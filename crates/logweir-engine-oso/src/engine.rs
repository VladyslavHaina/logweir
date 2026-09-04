//! `impl DataEngine for OsoCliEngine` — the point where the renderers (Task
//! 11), the subprocess runner (this task) and the vendored wire structs (Task
//! 8/8b) meet the trait boundary logweir-core defines.
use crate::{render_restore, subprocess, vendored};
use logweir_core::engine::*;
use std::path::PathBuf;

pub struct OsoCliEngine {
    binary: PathBuf,
    version: String,
    digest: String,
    workdir: PathBuf,
    /// Reads the archive: `list_backup_sets`, `describe` and `fingerprints`
    /// all go through this handle and only ever call `Store::get` /
    /// `list_manifests` / `segment_keys_for` — never `put_create_only`.
    /// Nothing in this file writes evidence, so the caller should construct
    /// this engine with `Store::read_only_from_url` over the OSO archive
    /// location: `Store::from_url`'s `LOGWEIR_ROOT` guard (Global Constraint
    /// 6) would otherwise refuse to build a handle over the archive prefix at
    /// all, since the archive is never under `logweir/`. See the module-level
    /// note below and the "cross-set merging" note on `fingerprints` for what
    /// this store's SCOPE means for that method.
    store: crate::storage::Store,
}

impl OsoCliEngine {
    pub fn new(
        binary: PathBuf,
        version: String,
        digest: String,
        workdir: PathBuf,
        store: crate::storage::Store,
    ) -> Self {
        Self {
            binary,
            version,
            digest,
            workdir,
            store,
        }
    }

    fn write(&self, name: &str, body: &str) -> Result<PathBuf, EngineError> {
        let p = self.workdir.join(name);
        std::fs::write(&p, body)
            .map_err(|e| EngineError::Operational(format!("{}: {e}", p.display())))?;
        Ok(p)
    }

    /// Rendered documents are the coupling surface; a key WE rendered that the
    /// engine dropped aborts the run (spec §7.2(a)). `doc` is the exact
    /// string handed to the engine as `--config`, so this check runs against
    /// what was actually asked for, not a reconstruction of it.
    fn assert_no_dropped_logweir_key(
        &self,
        doc: &str,
        warned: &[String],
    ) -> Result<(), EngineError> {
        for w in warned {
            let leaf = w.rsplit('.').next().unwrap_or(w);
            if doc.contains(&format!("{leaf}:")) {
                return Err(EngineError::Operational(format!(
                    "engine {} ignored the config key `{w}` that logweir rendered; \
                     this tag is below the declared floor — see docs/support-matrix.md",
                    self.version
                )));
            }
        }
        Ok(())
    }
}

impl DataEngine for OsoCliEngine {
    fn id(&self) -> EngineId {
        EngineId {
            id: "oso-cli".into(),
            version: self.version.clone(),
            digest: self.digest.clone(),
        }
    }

    fn list_backup_sets(&self, loc: &StorageUrl) -> Result<Vec<BackupSetRef>, EngineError> {
        // Read the bucket directly rather than shelling out to `list`: the CLI
        // prints human text, and object_store gives us the version id we
        // record as source.manifest_version_id. Global Constraint 3 also
        // forbids the `list` subcommand outright — see scripts/check-no-oso.sh.
        self.store.list_manifests(loc)
    }

    fn describe(&self, set: &BackupSetRef) -> Result<BackupSetFacts, EngineError> {
        let (bytes, version_id) = self.store.get(&set.manifest_key)?;
        let m: vendored::manifest::BackupManifest = serde_json::from_slice(&bytes)
            .map_err(|e| EngineError::Operational(format!("{}: {e}", set.manifest_key)))?;
        Ok(BackupSetFacts {
            backup_id: m.backup_id.clone(),
            created_at: chrono::DateTime::from_timestamp_millis(m.created_at).ok_or_else(|| {
                EngineError::Operational("manifest created_at out of range".into())
            })?,
            source_cluster_id: m.source_cluster_id.clone(),
            manifest_sha256: logweir_core::ids::sha256_prefixed(&bytes),
            manifest_version_id: version_id,
            // Spec §14 SP1b names consumer-groups-snapshot.json in scope and
            // §10/GT-10 treat it as part of the artifact set a reader must
            // handle. v0.1 records that it EXISTS and what it hashes to; it
            // never restores offsets (spec §2 non-goals), so nothing else is
            // read out of it. Absent is normal, not an error.
            consumer_group_snapshot_sha256: {
                let sibling = set
                    .manifest_key
                    .rsplit_once('/')
                    .map(|(dir, _)| format!("{dir}/consumer-groups-snapshot.json"))
                    .unwrap_or_else(|| "consumer-groups-snapshot.json".to_string());
                match self.store.get(&sibling) {
                    Ok((b, _)) => {
                        // Parse it so a reshaped object is a warning we can see
                        // rather than an opaque blob; the value is discarded.
                        let _: vendored::consumer_groups::ConsumerGroupsSnapshot =
                            serde_json::from_slice(&b)
                                .map_err(|e| EngineError::Operational(format!("{sibling}: {e}")))?;
                        Some(logweir_core::ids::sha256_prefixed(&b))
                    }
                    Err(_) => None,
                }
            },
            topics: m
                .topics
                .iter()
                .map(|t| TopicFacts {
                    name: t.name.clone(),
                    original_partition_count: t.original_partition_count,
                    source_replication_factor: t.source_replication_factor,
                    configurations: t.configurations.clone(),
                    partitions: t
                        .partitions
                        .iter()
                        .map(|p| PartitionFacts {
                            partition_id: p.partition_id,
                            segments: p
                                .segments
                                .iter()
                                .map(|s| SegmentFacts {
                                    key: s.key.clone(),
                                    start_offset: s.start_offset,
                                    end_offset: s.end_offset,
                                    start_timestamp: s.start_timestamp,
                                    end_timestamp: s.end_timestamp,
                                    record_count: s.record_count,
                                    sha256: s.sha256.clone(),
                                    uploaded_at: s.uploaded_at,
                                })
                                .collect(),
                            gaps: p
                                .gaps
                                .iter()
                                .map(|g| (g.start_offset, g.end_offset))
                                .collect(),
                            pruned: p
                                .pruned
                                .iter()
                                .map(|g| (g.start_offset, g.end_offset))
                                .collect(),
                        })
                        .collect(),
                })
                .collect(),
        })
    }

    fn preflight(&self, plan: &RestorePlan) -> Result<PreflightReport, EngineError> {
        let doc = render_restore::render(plan);
        let cfg = self.write("restore.yaml", &doc)?;
        let run = subprocess::run_engine(
            &self.binary,
            &[
                "validate-restore",
                "--config",
                cfg.to_str().ok_or_else(|| {
                    EngineError::Operational(format!("{}: not valid UTF-8", cfg.display()))
                })?,
                "--format",
                "json",
            ],
            &mut |_, _| {},
        )?;
        self.assert_no_dropped_logweir_key(&doc, &run.unknown_key_warnings)?;
        // validate-restore exits 1 on !valid || !errors.is_empty()
        // [VERIFIED U/kafka-backup/crates/kafka-backup-cli/src/commands/
        // validate_restore.rs:39-42], so exit 1 with parsable JSON is a
        // RESULT, not an operational failure. Only unparsable output is.
        // stdout is LOG LINES followed by the JSON object: the command logs
        // `info!("Validating restore configuration from: {}", ...)` (line 21)
        // and `RestoreEngine::new`/`dry_run()` log further, all through the
        // same stdout-defaulting fmt layer, before
        // `println!("{}", serde_json::to_string_pretty(&report)?)` (line 30)
        // [VERIFIED validate_restore.rs:5,21,30,39-42 and main.rs:553-556].
        // `RUST_LOG=warn` (set in `run_engine`) removes the info! lines at the
        // source; slicing from the first `{` is the belt to that brace,
        // because a WARN line would otherwise still break the parse.
        let json = run
            .stdout
            .find('{')
            .map(|i| &run.stdout[i..])
            .ok_or_else(|| {
                EngineError::Operational(format!(
                    "validate-restore exited {} and printed no JSON object\nstdout: {}\nstderr: {}",
                    run.exit_code, run.stdout, run.stderr
                ))
            })?;
        let r: vendored::manifest::DryRunReport = serde_json::from_str(json).map_err(|e| {
            EngineError::Operational(format!(
                "validate-restore exited {} and its stdout is not a DryRunReport: {e}\nstdout: {}",
                run.exit_code, run.stdout
            ))
        })?;

        // Readback (1) of spec §9.3 phase 5: an engine that IGNORES
        // `header_preflight` defaults to Auto (config.rs:972-980),
        // offset_recovery_requested is false (preflight.rs:196-198), so
        // scan_required is false and report.header_preflight stays None.
        let hp = r.header_preflight.as_ref();
        let honoured = hp
            .map(|h| h.scan_performed && h.mode == "full")
            .unwrap_or(false);

        let partitions = hp
            .map(|h| {
                h.partitions
                    .iter()
                    .map(|p| PartitionCoverage {
                        topic: p.topic.clone(),
                        partition: p.partition,
                        detail: p.detail(),
                        state: match &p.state {
                            vendored::preflight::PartitionCoverageState::Full => {
                                CoverageState::Full
                            }
                            vendored::preflight::PartitionCoverageState::Partial => {
                                CoverageState::Partial
                            }
                            vendored::preflight::PartitionCoverageState::Missing => {
                                CoverageState::Missing
                            }
                            vendored::preflight::PartitionCoverageState::Empty => {
                                CoverageState::Empty
                            }
                            vendored::preflight::PartitionCoverageState::DataMissing => {
                                CoverageState::DataMissing
                            }
                            vendored::preflight::PartitionCoverageState::Corrupt => {
                                CoverageState::Corrupt
                            }
                            vendored::preflight::PartitionCoverageState::Indeterminate => {
                                CoverageState::Indeterminate
                            }
                            vendored::preflight::PartitionCoverageState::Unknown(s) => {
                                CoverageState::Unknown(s.clone())
                            }
                        },
                    })
                    .collect()
            })
            .unwrap_or_default();

        Ok(PreflightReport {
            valid: r.valid,
            errors: r.errors,
            warnings: r.warnings,
            segments_to_process: r.segments_to_process,
            records_to_restore: r.records_to_restore,
            time_range: r.time_range,
            partitions,
            header_preflight_honoured: honoured,
            unknown_key_warnings: run.unknown_key_warnings,
        })
    }

    fn restore(
        &self,
        plan: &RestorePlan,
        obs: &mut dyn PhaseObserver,
    ) -> Result<RestoreFacts, EngineError> {
        let doc = render_restore::render(plan); // the SAME document phase 5 validated
        let cfg = self.write("restore.yaml", &doc)?;
        let started_at = chrono::Utc::now();
        let run = subprocess::run_engine(
            &self.binary,
            &[
                "restore",
                "--config",
                cfg.to_str().ok_or_else(|| {
                    EngineError::Operational(format!("{}: not valid UTF-8", cfg.display()))
                })?,
            ],
            &mut |stream, line| obs.engine_line(stream, line),
        )?;
        let finished_at = chrono::Utc::now();
        self.assert_no_dropped_logweir_key(&doc, &run.unknown_key_warnings)?;
        if run.exit_code != 0 {
            return Err(EngineError::Operational(format!(
                "kafka-backup restore exited {}: {}",
                run.exit_code, run.stderr
            )));
        }
        // `restore` has no --format and writes no report file; the exit code
        // is its only machine-readable signal, so every timing here is OURS.
        Ok(RestoreFacts {
            started_at,
            finished_at,
            exit_code: run.exit_code,
            unknown_key_warnings: run.unknown_key_warnings,
        })
    }

    fn fingerprints(&self, sel: &SampleSelection) -> Result<Vec<RecordFingerprint>, EngineError> {
        // KNOWN LIMITATION, carried forward rather than silently built on
        // (see task report "cross-set merging" section): `SampleSelection`
        // (logweir-core, Task 8a — not this task's file) carries no backup-set
        // id, and `Store::segment_keys_for` (Task 12b) resolves topic/partition
        // against EVERY manifest under this store's prefix. If two backup sets
        // share a topic/partition with overlapping windows, their segments
        // merge here with no way — at this trait boundary — to tell them
        // apart. Fixing it needs either a `set` field on `SampleSelection` or a
        // `Store` scoped to one backup set's own sub-prefix; both are changes
        // to code outside this task's Files list (logweir-core's trait, or the
        // as-yet-unwritten wiring that constructs this engine per drill run)
        // and are left for whichever later task owns that call site.
        let mut out = Vec::new();
        for key in self
            .store
            .segment_keys_for(&sel.topic, sel.partition, sel.window)?
        {
            let (bytes, _) = self.store.get(&key)?;
            for r in crate::kbak::decode_segment(&bytes)? {
                // Err(Unsupported) propagates
                if r.timestamp < sel.window.0 || r.timestamp > sel.window.1 {
                    continue;
                }
                out.push(RecordFingerprint {
                    topic: sel.topic.clone(),
                    partition: sel.partition,
                    offset: r.offset,
                    sha256: logweir_kafka::fingerprint::record_fingerprint(
                        r.key.as_deref(),
                        r.value.as_deref(),
                        &r.headers,
                        r.timestamp,
                    ),
                });
            }
        }
        out.sort_by_key(|f| f.offset);
        Ok(out)
    }
}
