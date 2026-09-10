//! `impl DataEngine for OsoCliEngine` — the point where the renderers (Task
//! 11), the subprocess runner (this task) and the vendored wire structs (Task
//! 8/8b) meet the trait boundary logweir-core defines.
use crate::{render_backup, render_restore, subprocess, vendored};
use logweir_core::engine::*;
use logweir_core::spec::Anchor;
use std::path::PathBuf;

/// How much of a failed subprocess's stream may travel into an `EngineError`.
///
/// This is not cosmetic. `crates/logweir/src/drill/mod.rs` writes an
/// `EngineError`'s `Display` into `PhaseRecord.outcome`, which phase 8 signs
/// and puts **create-only** into the evidence bucket: whatever lands there is
/// immutable and cannot be redacted afterwards. The capture was unbounded, and
/// the child inherits the parent environment — which is where
/// `AWS_SECRET_ACCESS_KEY` lives on the Kubernetes path
/// (`examples/cronjob-drill.yaml`). An engine that echoes its configuration on
/// failure could therefore write a credential into a write-once document.
///
/// A byte cap does not make that impossible and is not claimed to: it bounds
/// the blast radius and the document size. `RUST_LOG=warn` (set in
/// `run_engine`) remains the measure that keeps the engine quiet in the first
/// place, and it is a mitigation, not a boundary.
const MAX_CAPTURED_STREAM_BYTES: usize = 4096;

/// The TAIL of a captured stream, capped at `MAX_CAPTURED_STREAM_BYTES`, with
/// an explicit marker when anything was dropped — never a silent truncation,
/// which would let a reader mistake a cut-off message for the whole of it.
///
/// The tail rather than the head: a process's last output is what explains why
/// it stopped. The cut is moved to a `char` boundary so the result is always
/// valid UTF-8 (`String` requires it, and the engine's output is text).
fn captured(s: &str) -> String {
    if s.len() <= MAX_CAPTURED_STREAM_BYTES {
        return s.to_string();
    }
    let mut cut = s.len() - MAX_CAPTURED_STREAM_BYTES;
    while cut < s.len() && !s.is_char_boundary(cut) {
        cut += 1;
    }
    format!(
        "[{} earlier byte(s) dropped; a signed scorecard is immutable and this capture is \
         bounded at {} bytes]\n{}",
        cut,
        MAX_CAPTURED_STREAM_BYTES,
        &s[cut..]
    )
}

pub struct OsoCliEngine {
    binary: PathBuf,
    version: String,
    digest: String,
    workdir: PathBuf,
    /// Reads the archive: `list_backup_sets`, `describe` and `fingerprints`
    /// all go through this handle and only ever call `Store::get` /
    /// `list_manifests` / `segment_keys_for_set` — never `put_create_only`.
    /// Nothing in this file writes evidence, so the caller should construct
    /// this engine with `Store::read_only_from_url` over the OSO archive
    /// location: `Store::from_url`'s `LOGWEIR_ROOT` guard (Global Constraint
    /// 6) would otherwise refuse to build a handle over the archive prefix at
    /// all, since the archive is never under `logweir/`.
    store: crate::storage::Store,
    /// The digest of the `restore.yaml` phase 5 wrote, memoised so phase 6 can
    /// refuse a divergence. `Mutex`, not `RefCell`: `DataEngine`'s methods take
    /// `&self` and the orchestrator holds this behind a trait object whose auto
    /// traits must not narrow.
    phase5_render: std::sync::Mutex<Option<String>>,
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
            phase5_render: std::sync::Mutex::new(None),
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
            // Task 12 fix: `Err(_) => None` used to map EVERY read failure —
            // a 403, a timeout, a truncated read, genuine absence — onto the
            // same `None`, so `consumer_group_snapshot_sha256: None` then
            // flowed into the signed scorecard as a positive claim ("this
            // artefact was never uploaded") the store never actually
            // established. Only `StoreError::NotFound` means that; every
            // other error propagates as `Operational`, matching the
            // malformed-snapshot arm just below it, which already propagated.
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
                    Err(crate::storage::StoreError::NotFound(_)) => None,
                    Err(other) => return Err(other.into()),
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
                                    // QUALIFIED, not the manifest's own
                                    // relative key. A manifest body stores
                                    // `{backup_id}/topics/...` relative to the
                                    // configured storage prefix (see
                                    // `Store::qualify`), and `SegmentFacts.key`
                                    // is consumed by `phase7_verify::
                                    // segment_evidence` as an argument to
                                    // `Store::get`. Passing the relative key
                                    // through made every segment of a real
                                    // archive with a non-empty prefix 404 in
                                    // `get` — so the byte-fingerprint segment
                                    // lane could never verify anything against
                                    // a real bucket, only against a
                                    // `Filesystem` store (whose prefix is `""`)
                                    // and the in-memory doubles. Found by Task
                                    // 21c the first time phase 7 read segments
                                    // out of MinIO; `segments_in_manifest` in
                                    // storage.rs already carried the identical
                                    // fix and this call site was missed.
                                    key: self.store.qualify(&s.key),
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
        // G-GLOB: a topic (or a mapped target) carrying a glob
        // metacharacter is refused HERE, at phase 5, before the engine is
        // spawned. `EngineError::Operational` is exit 1 — the same mapping
        // ruling R-E gives the phase-5/phase-6 digest divergence, and for the
        // same reason: `preflight` is reached only after phase 0's guards have
        // run, so Global Constraint 11's exit 3 ("refused by a guard, before
        // anything runs") does not describe it.
        let (doc, rendered_restore_sha256) = render_restore::render_and_digest(plan)
            .map_err(|e| EngineError::Operational(e.to_string()))?;
        let cfg = self.write("restore.yaml", &doc)?;
        // T0-14. Memoised HERE, immediately after the write and before the
        // engine is spawned, because the field's meaning is "the digest of the
        // bytes that are now on disk as restore.yaml" — that is the document
        // phase 6 would silently truncate, and the moment those bytes exist is
        // the moment the claim becomes checkable. Memoising only on a
        // successful return would be no stronger in production (`record(..)?`
        // in `crates/logweir/src/drill/mod.rs` short-circuits, so phase 6 is
        // unreachable after a phase-5 error) and would make the memo depend on
        // the engine's behaviour rather than on our own write.
        *self.phase5_render.lock().expect("phase5_render mutex") =
            Some(rendered_restore_sha256.clone());
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
                    run.exit_code,
                    captured(&run.stdout),
                    captured(&run.stderr)
                ))
            })?;
        let r: vendored::manifest::DryRunReport = serde_json::from_str(json).map_err(|e| {
            EngineError::Operational(format!(
                "validate-restore exited {} and its stdout is not a DryRunReport: {e}\nstdout: {}",
                run.exit_code,
                captured(&run.stdout)
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
            rendered_restore_sha256,
        })
    }

    fn restore(
        &self,
        plan: &RestorePlan,
        obs: &mut dyn PhaseObserver,
    ) -> Result<RestoreFacts, EngineError> {
        // T0-14. Phases 5 and 6 each render `restore.yaml` and `self.write`
        // calls `std::fs::write`, which truncates — so phase 6 overwrites the
        // document phase 5 validated. Until this comparison existed, "the SAME
        // document" was true only because
        // `crates/logweir/src/drill/mod.rs:576` happened to build `plan` once
        // and hand the same value to both. That is a property of one call
        // site, not a guarantee; it dissolves the moment phase 5 and phase 6
        // are split.
        //
        // Ruling R-E: a divergence is `EngineError::Operational` and therefore
        // EXIT 1 (no artifact) via `DrillError::Engine` at drill/mod.rs:105 —
        // NOT exit 3. Global Constraint 11 reserves 3 for "plan refused by a
        // guard, before anything runs", and by phase 6 phases 0-5 have run,
        // including the engine's own `validate-restore`. The ADR that would
        // carry this ruling is gated on open question O2 and is deferred; see
        // docs/stability.md.
        let (doc, six) = render_restore::render_and_digest(plan)
            .map_err(|e| EngineError::Operational(e.to_string()))?;
        match self
            .phase5_render
            .lock()
            .expect("phase5_render mutex")
            .as_deref()
        {
            Some(five) if five != six => {
                return Err(EngineError::Operational(format!(
                    "rendered restore.yaml diverged between phase 5 and phase 6: \
                     phase 5 validated {five}, phase 6 would restore {six} — refusing"
                )));
            }
            Some(_) => {}
            None => {
                return Err(EngineError::Operational(
                    "restore() called before preflight(): no phase-5 render to compare against"
                        .into(),
                ))
            }
        }
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
        // Fix (post-review): exit code checked BEFORE the dropped-key check,
        // not after. With the order reversed, a run that both dropped a key
        // we rendered AND failed would return only the dropped-key error —
        // losing the exit code and stderr, which are the more actionable,
        // primary evidence for an outright failure. A run that dropped a key
        // but otherwise EXITED 0 still gets the dropped-key error, from the
        // `assert_no_dropped_logweir_key` call below.
        //
        // Fix (post-review): stdout is now included too, not just stderr —
        // by this crate's own finding (see subprocess.rs), the engine's log
        // lines (including a dropped-key warning, on the real binary) go to
        // stdout, so a failure message carrying only stderr can omit the very
        // line that explains the failure.
        if run.exit_code != 0 {
            return Err(EngineError::Operational(format!(
                "kafka-backup restore exited {}\nstdout: {}\nstderr: {}",
                run.exit_code,
                captured(&run.stdout),
                captured(&run.stderr)
            )));
        }
        self.assert_no_dropped_logweir_key(&doc, &run.unknown_key_warnings)?;
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
        // Fix (Task 16 fix round 1): `phase4_sample::run` (crates/logweir)
        // cannot populate `SampleSelection.set.manifest_key` — it only ever
        // sees `BackupSetFacts`, which does not carry one — so every
        // `Selection` it produces arrives with `manifest_key` empty until a
        // caller patches it (`Selection::bind_backup_set`) from the original
        // `BackupSetRef`. Refusing here, with a message naming exactly what
        // is wrong, turns a forgotten patch step into a loud, specific
        // `EngineError::Operational` rather than relying on
        // `Store::get("")`'s incidental "object not found" (harmless today,
        // but a guard that depends on a downstream error happening to be
        // legible is not a guard).
        if sel.set.manifest_key.is_empty() {
            return Err(EngineError::Operational(
                "SampleSelection.set.manifest_key is empty — phase 4 (phase4_sample::run) \
                 cannot populate it from BackupSetFacts alone; the caller must patch `sel.set` \
                 with the original BackupSetRef (Selection::bind_backup_set) before calling \
                 fingerprints"
                    .into(),
            ));
        }
        // Fix (post-review): scoped to `sel.set.manifest_key` via
        // `segment_keys_for_set`, NOT the broad `segment_keys_for` — the
        // latter resolves topic/partition against EVERY manifest under the
        // store's prefix, so two backup sets sharing a topic/partition with
        // an overlapping window would merge here, making the archive side a
        // strict superset of what a real restore populated. That reads as a
        // mismatch and fails a drill that actually succeeded — the worst
        // direction for a product whose deliverable is a signed attestation.
        // See `SampleSelection::set`'s doc comment (logweir-core).
        //
        // Fix (post-review, second round): `count == 0` returns before
        // touching storage at all — no listing, no reads, no decoding —
        // rather than building and discarding a full buffer, for every
        // anchor.
        if sel.count == 0 {
            return Ok(Vec::new());
        }
        let mut all = Vec::new();
        for key in self.store.segment_keys_for_set(
            &sel.set.manifest_key,
            &sel.topic,
            sel.partition,
            sel.window,
        )? {
            let (bytes, _) = self.store.get(&key)?;
            for r in crate::kbak::decode_segment(&bytes)? {
                // Err(Unsupported) propagates
                if r.timestamp < sel.window.0 || r.timestamp > sel.window.1 {
                    continue;
                }
                all.push(RecordFingerprint {
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
            // Fix (post-review, second round): `head` bounds the READ, not
            // only the output — `count`'s doc comment (logweir-core) used to
            // claim this for every anchor, when only `head` can actually do
            // it. `tail` and `random` both need the window's full extent
            // before they can choose (the latest `count` records, or a span
            // across all of them), so they must keep traversing regardless
            // of how much `all` already holds.
            //
            // Safe to stop here for `head` specifically because
            // `segment_keys_for_set` returns keys SORTED (lexicographically,
            // via `Vec::sort` in storage.rs) and the real key format
            // zero-pads the segment's start offset to a fixed width
            // (`segment-{offset:020}.bin{ext}`, storage.rs's `qualify` doc) —
            // for a FIXED topic/partition (already the case here; every key
            // in this loop shares that prefix), lexicographic order over
            // these keys IS ascending start-offset order. So once `all`
            // holds `count` in-window records after fully decoding a
            // segment, every segment left in the iterator has a start offset
            // at or above the one just processed and cannot contain a
            // record earlier than what has already been collected — reading
            // it could only add records `select_sample`'s `"head"` branch
            // would discard anyway.
            if sel.anchor == Anchor::Head && all.len() >= sel.count {
                break;
            }
        }
        all.sort_by_key(|f| f.offset);
        select_sample(all, sel.anchor, sel.count)
    }

    /// GC18's `--from-cluster` half, and the SECOND shipped `run_engine` call
    /// site (Task 1's `ENGINE_ARGV_ALLOWLIST` / interface **I32**).
    ///
    /// The engine's `backup` takes `--config` and nothing else
    /// [VERIFIED U:crates/kafka-backup-cli/src/main.rs:35-39,559-561] — no
    /// `--format`, no report file — so the WHOLE surface is the rendered
    /// document, and every fact about the run other than its exit code is
    /// ours to time and to read back off its streams. That is why
    /// `BackupFacts` looks like `RestoreFacts` and not like
    /// `PreflightReport`.
    ///
    /// `render_and_digest` rather than `render`: it is the entry point that
    /// carries GC18(c) rail 3 (`scan_rendered_document`, fail-closed) and
    /// **G-EXP**'s post-render sweep, both of which must run over the exact
    /// bytes about to be written. The digest is bound and dropped here
    /// because `BackupFacts` has no field for it (Task 2 froze that type);
    /// Task 5b's receipt can re-derive it from the same plan, since
    /// `render_backup::render` is a pure function of `BackupPlan`.
    ///
    /// An auth mode this build cannot render (SCRAM, until Task 6) reaches
    /// this method as `RenderError::UnsupportedAuthMode` — Task 3's TYPED
    /// refusal — and leaves it as `EngineError::Operational`, exit 1 by
    /// ruling R-E. It is never a panic and never a silent downgrade to an
    /// unauthenticated document.
    fn backup(
        &self,
        plan: &BackupPlan,
        obs: &mut dyn PhaseObserver,
    ) -> Result<BackupFacts, EngineError> {
        let (doc, _rendered_backup_sha256) = render_backup::render_and_digest(plan)
            .map_err(|e| EngineError::Operational(e.to_string()))?;
        let cfg = self.write("backup.yaml", &doc)?;
        let started_at = chrono::Utc::now();
        let run = subprocess::run_engine(
            &self.binary,
            &[
                "backup",
                "--config",
                cfg.to_str().ok_or_else(|| {
                    EngineError::Operational(format!("{}: not valid UTF-8", cfg.display()))
                })?,
            ],
            &mut |stream, line| obs.engine_line(stream, line),
        )?;
        let finished_at = chrono::Utc::now();
        // The exit code is checked BEFORE the dropped-key check, matching the
        // ordering fix recorded at `engine.rs:422-428` for `restore`: with the
        // order reversed, a run that both dropped a key we rendered AND failed
        // returns only the dropped-key error, losing the exit code and the
        // streams, which are the primary evidence for an outright failure. A
        // run that dropped a key but EXITED 0 still gets the dropped-key error,
        // from the `assert_no_dropped_logweir_key` call below.
        //
        // Both streams in the message, for the reason `restore` records: this
        // engine's log lines go to STDOUT, so a failure carrying only stderr
        // can omit the line that explains it.
        if run.exit_code != 0 {
            return Err(EngineError::Operational(format!(
                "kafka-backup backup exited {}\nstdout: {}\nstderr: {}",
                run.exit_code,
                captured(&run.stdout),
                captured(&run.stderr)
            )));
        }
        // Rendered documents are the coupling surface: a key WE rendered that
        // this engine tag dropped aborts the run (spec §7.2(a)), exactly as
        // `preflight` (engine.rs:270) and `restore` (engine.rs:443) already do.
        self.assert_no_dropped_logweir_key(&doc, &run.unknown_key_warnings)?;
        Ok(BackupFacts {
            started_at,
            finished_at,
            exit_code: run.exit_code,
            unknown_key_warnings: run.unknown_key_warnings,
        })
    }
}

/// Fix (post-review): `anchor` and `count` used to be read by nothing, so
/// `fingerprints()` returned every matching record in the window regardless
/// of what the caller (and the scorecard, which also records both fields —
/// see `SampleSpec` in the Task 14 drill spec) asked for. A scorecard stating
/// "head, 25 samples" while the comparison actually ran over every record in
/// the window is a signed document describing a sample nobody took; on a
/// real archive's volumes it is also an unbounded-memory read. `count` is a
/// CAP (fewer matching records than `count` is not an error), never a promise
/// of exactly that many.
///
/// `anchor` semantics chosen here (undocumented beyond "head | tail | random"
/// at the type's own definition, and the Task 14 brief's "rotating across
/// runs is the adopter's job" — which sets policy across MULTIPLE drill runs,
/// not what a single call does):
/// - `"head"`: the first `count` records by offset (lowest/earliest) in the
///   window — Kafka's own convention for "head of the log".
/// - `"tail"`: the last `count` records by offset (highest/latest).
/// - `"random"`: a DETERMINISTIC, evenly-spaced ("systematic") sample across
///   the full sorted set, not a seeded or unseeded RNG draw. Chosen over true
///   randomness because the scorecard's audit trail is the concrete list of
///   `RecordFingerprint`s it embeds, not a formula to regenerate them — an
///   auditor verifies the offsets actually recorded, never re-derives which
///   ones "should" have been picked — so genuine non-determinism buys nothing
///   an evenly-spaced deterministic sample does not, while costing a new `rand`
///   dependency and, more importantly, testability: this function's tests
///   assert exact selections, which a true RNG draw cannot do.
///
/// Any other value is a bug in the caller (this field is entirely
/// Logweir-internal, never engine-reported) and is reported as such rather
/// than silently defaulting — a scorecard cannot honestly claim an anchor
/// that was never actually applied.
fn select_sample(
    sorted: Vec<RecordFingerprint>,
    anchor: Anchor,
    count: usize,
) -> Result<Vec<RecordFingerprint>, EngineError> {
    // Backstop, not the primary path: `fingerprints()` already returns before
    // reading anything when `sel.count == 0`, so `sorted` is always empty
    // here in practice too. Kept in case a future caller reaches this
    // function some other way.
    if count == 0 {
        return Ok(Vec::new());
    }
    if sorted.len() <= count {
        return Ok(sorted);
    }
    match anchor {
        Anchor::Head => Ok(sorted.into_iter().take(count).collect()),
        Anchor::Tail => {
            let start = sorted.len() - count;
            Ok(sorted.into_iter().skip(start).collect())
        }
        Anchor::Random => {
            let n = sorted.len();
            let mut out: Vec<RecordFingerprint> = Vec::with_capacity(count);
            for i in 0..count {
                let idx = i * (n - 1) / (count - 1).max(1);
                out.push(sorted[idx].clone());
            }
            // Defensive, not load-bearing under the current n > count
            // guarantee (see the early return above): guards against a
            // repeated index if the stride between two adjacent `i` values
            // ever floors to the same integer, so `count` is honoured as a
            // cap even if that arithmetic assumption is ever violated.
            out.dedup_by_key(|f| f.offset);
            Ok(out)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{captured, MAX_CAPTURED_STREAM_BYTES};

    /// The whole point of the cap: an `EngineError`'s `Display` is written
    /// into `PhaseRecord.outcome`, which is signed and put create-only. An
    /// unbounded capture puts unbounded, unredactable subprocess output into
    /// an immutable document.
    #[test]
    fn a_captured_stream_is_bounded_and_says_so_when_it_was_cut() {
        let huge = "x".repeat(MAX_CAPTURED_STREAM_BYTES * 3);
        let out = captured(&huge);
        assert!(
            out.len() < MAX_CAPTURED_STREAM_BYTES + 200,
            "the capture is unbounded: {} bytes",
            out.len()
        );
        assert!(
            out.contains("earlier byte(s) dropped"),
            "a silent truncation lets a reader mistake a cut-off message for the whole of it"
        );
    }

    /// The TAIL survives: a process's last output is what explains why it
    /// stopped.
    #[test]
    fn a_captured_stream_keeps_its_end_not_its_beginning() {
        let s = format!("{}THE-REASON", "y".repeat(MAX_CAPTURED_STREAM_BYTES * 2));
        assert!(captured(&s).ends_with("THE-REASON"));
    }

    /// Anything that fits is passed through byte for byte — the normal case,
    /// and the one every existing test asserts against.
    #[test]
    fn a_short_stream_is_passed_through_unchanged() {
        assert_eq!(captured("exited 1: no such file"), "exited 1: no such file");
    }

    /// The cut lands on a `char` boundary, so a multi-byte character straddling
    /// it cannot panic the very error path that is trying to report a failure.
    #[test]
    fn a_multibyte_character_at_the_cut_does_not_panic() {
        let s = "é".repeat(MAX_CAPTURED_STREAM_BYTES); // 2 bytes each
        let out = captured(&s);
        assert!(out.contains("earlier byte(s) dropped"));
        assert!(out.ends_with('é'));
    }
}
