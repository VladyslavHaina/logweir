//! `impl DataEngine for OsoCliEngine`: where the renderers, the subprocess
//! runner and the vendored wire structs meet the trait boundary logweir-core
//! defines.
use crate::{render_backup, render_restore, subprocess, vendored};
use logweir_core::engine::*;
use logweir_core::spec::Anchor;
use std::path::PathBuf;

/// How much of a failed subprocess's stream may travel into an `EngineError`.
///
/// An `EngineError`'s `Display` is written into `PhaseRecord.outcome`, which
/// phase 8 signs and puts **create-only** into the evidence bucket, and the
/// child inherits the parent environment (`AWS_SECRET_ACCESS_KEY` on the
/// Kubernetes path): an engine that echoes its configuration on failure could
/// write a credential into a write-once document. The cap bounds the blast
/// radius and the document size; `RUST_LOG=warn` (set in `run_engine`) keeps
/// the engine quiet in the first place. Both are mitigations, not a boundary.
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

/// The engine's offset-mapping report at `path`, as phase 7 reads
/// it: the `(target topic, partition)` of every entry.
///
/// `Absent` when there is no file (the engine writes one only from a completed
/// restore, and its own write failure is a warning upstream), `Unreadable`
/// when there is one this build cannot parse. Neither is an error: the report
/// is evidence of what the engine LACKS, never of what it wrote, and phase 7
/// logs a report it could not check.
///
/// **Streamed, never buffered.** The report carries one
/// `detailed_mappings` pair per restored record — about 116 bytes of file per
/// record — so it is read through a `BufReader` into a shape that names only
/// `entries[].{topic, partition}`, and serde skips everything else as it
/// streams past. Memory is bounded by the number of partition entries, not by
/// the number of records (`tests/offset_report_memory.rs` measures it).
pub fn read_engine_report(path: &std::path::Path) -> EngineReport {
    let file = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return EngineReport::Absent,
        Err(e) => return EngineReport::Unreadable(format!("{}: {e}", path.display())),
    };
    match serde_json::from_reader::<_, vendored::offset_report::OffsetMappingReport>(
        std::io::BufReader::new(file),
    ) {
        Ok(r) => EngineReport::Read(
            r.entries
                .into_values()
                .map(|e| (e.topic, e.partition))
                .collect(),
        ),
        Err(e) => EngineReport::Unreadable(format!(
            "{}: not the engine's offset-mapping report: {e}",
            path.display()
        )),
    }
}

/// Whatever is at a report path before a restore is NOT that
/// restore's. The default path is per run, but `--offset-report-out` may name
/// a fixed one, and a report an earlier run left there would be read as this
/// run's (and uploaded by phase 8 as its evidence) whenever the engine writes
/// none. Removed first, so after the run the file is the engine's or absent.
fn remove_stale_report(path: &std::path::Path) -> Result<(), EngineError> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(EngineError::Operational(format!(
            "{}: an earlier offset report could not be removed before the restore, and it \
             would be read as this run's: {e}",
            path.display()
        ))),
    }
}

/// One `PreflightReport` for a plan restored by several engine
/// runs (one per distinct partition subset). The verdict-bearing fields can
/// only get worse by merging: `valid` and `header_preflight_honoured` hold
/// only when they hold for EVERY run, every run's errors, warnings, coverage
/// findings and dropped keys are kept, and the counts are summed. One run's
/// report is returned unchanged.
pub fn merge_preflight_reports(mut reports: Vec<PreflightReport>) -> PreflightReport {
    if reports.len() == 1 {
        return reports.pop().expect("one report");
    }
    let mut out = PreflightReport {
        valid: true,
        errors: Vec::new(),
        warnings: Vec::new(),
        segments_to_process: 0,
        records_to_restore: 0,
        time_range: None,
        partitions: Vec::new(),
        header_preflight_honoured: !reports.is_empty(),
        unknown_key_warnings: Vec::new(),
        rendered_restore_sha256: String::new(),
    };
    let mut digests = Vec::new();
    for r in reports {
        out.valid &= r.valid;
        out.errors.extend(r.errors);
        out.warnings.extend(r.warnings);
        out.segments_to_process += r.segments_to_process;
        out.records_to_restore += r.records_to_restore;
        out.time_range = match (out.time_range, r.time_range) {
            (Some((a, b)), Some((c, d))) => Some((a.min(c), b.max(d))),
            (x, None) | (None, x) => x,
        };
        out.partitions.extend(r.partitions);
        out.header_preflight_honoured &= r.header_preflight_honoured;
        for w in r.unknown_key_warnings {
            if !out.unknown_key_warnings.contains(&w) {
                out.unknown_key_warnings.push(w);
            }
        }
        digests.push(r.rendered_restore_sha256);
    }
    out.rendered_restore_sha256 = digests.join(",");
    out
}

/// The offset report of a restore made of several engine runs:
/// every run's report is checked only when every run's was READ — the
/// union of their entries, the runs restoring disjoint topics. Otherwise the
/// first run's report that could not be read (Unreadable before Absent) is the
/// answer, and phase 7 checks nothing from it, as for one run.
pub fn merge_engine_reports(reports: Vec<EngineReport>) -> EngineReport {
    let mut union = std::collections::BTreeSet::new();
    let mut absent = false;
    for r in reports {
        match r {
            EngineReport::Read(entries) => union.extend(entries),
            EngineReport::Unreadable(why) => return EngineReport::Unreadable(why),
            EngineReport::Absent => absent = true,
        }
    }
    if absent {
        EngineReport::Absent
    } else {
        EngineReport::Read(union)
    }
}

/// The evidence file a multi-run restore leaves at the plan's
/// `offset_report` path, which phase 8 uploads: a JSON ARRAY with one element
/// per engine run, in run order — that run's report, byte for byte, or `null`
/// when the run wrote none. The bytes are concatenated, never parsed, so the
/// per-record section of a large report is never held in memory. A one-run
/// restore keeps the engine's own report as before.
pub fn compose_offset_reports(runs: &[PathBuf], out: &std::path::Path) -> Result<(), EngineError> {
    use std::io::Write as _;
    let op = |e: std::io::Error| EngineError::Operational(format!("{}: {e}", out.display()));
    // A run's report IS the output: copying a file into itself
    // never ends. `RestoreRun::path` gives every run of a multi-run plan its
    // own `.run-<i>` path, so this is a guard against a future caller.
    if let Some(same) = runs.iter().find(|p| p.as_path() == out) {
        return Err(EngineError::Operational(format!(
            "{}: an engine run's offset report is the composed report's own path, so it would \
             be copied into itself; refusing",
            same.display()
        )));
    }
    let mut f = std::io::BufWriter::new(std::fs::File::create(out).map_err(op)?);
    f.write_all(b"[").map_err(op)?;
    for (i, path) in runs.iter().enumerate() {
        if i > 0 {
            f.write_all(b",\n").map_err(op)?;
        }
        match std::fs::File::open(path) {
            Ok(mut src) => {
                std::io::copy(&mut src, &mut f).map_err(op)?;
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                f.write_all(b"null").map_err(op)?;
            }
            Err(e) => return Err(EngineError::Operational(format!("{}: {e}", path.display()))),
        }
    }
    f.write_all(b"]\n").map_err(op)?;
    f.flush().map_err(op)
}

pub struct OsoCliEngine {
    binary: PathBuf,
    version: String,
    digest: String,
    workdir: PathBuf,
    /// Reads the archive (`list_backup_sets`, `describe`, `fingerprints`) and
    /// never writes: build it with `Store::read_only_from_url` over the OSO
    /// archive location, which `Store::from_url`'s `LOGWEIR_ROOT` guard (Global
    /// Constraint 6) would refuse.
    store: crate::storage::Store,
    /// The digest of the `restore.yaml` phase 5 wrote, memoised so phase 6 can
    /// refuse a divergence. `Mutex`, not `RefCell`: `DataEngine`'s methods take
    /// `&self` and the orchestrator holds this behind a trait object whose auto
    /// traits must not narrow.
    ///
    /// ONE DIGEST PER ENGINE RUN, in run order. A plan whose
    /// partition subsets differ between topics is several engine runs
    /// (`render_restore::runs`), each with its own document; phase 6 refuses
    /// unless it would restore exactly the documents phase 5 validated, in the
    /// same order. A plan with no subset is one run, one digest, as before.
    phase5_render: std::sync::Mutex<Option<Vec<String>>>,
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

    /// One engine run's `validate-restore` over the document already written
    /// at `cfg` (`preflight` calls it once per run).
    fn validate_one_run(
        &self,
        cfg: &std::path::Path,
        doc: &str,
        digest: &str,
    ) -> Result<PreflightReport, EngineError> {
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
        self.assert_no_dropped_logweir_key(doc, &run.unknown_key_warnings)?;
        // validate-restore exits 1 on !valid || !errors.is_empty() [VERIFIED
        // U/kafka-backup/crates/kafka-backup-cli/src/commands/validate_restore.rs:39-42],
        // so exit 1 with parsable JSON is a RESULT; only unparsable output is
        // an operational failure. stdout is log lines followed by the pretty
        // JSON object (validate_restore.rs:21,30): `RUST_LOG=warn` removes the
        // info! lines at the source, and slicing from the first `{` covers a
        // WARN line.
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
            rendered_restore_sha256: digest.to_string(),
        })
    }

    /// One engine run's `restore` over `doc` (`restore` calls it
    /// once per run, in order). `which` is `(index, count, the run's topic
    /// mapping)`, for the failure message.
    fn restore_one_run(
        &self,
        config_name: &str,
        doc: &str,
        report_path: &std::path::Path,
        obs: &mut dyn PhaseObserver,
        which: (usize, usize, &std::collections::BTreeMap<String, String>),
    ) -> Result<RestoreFacts, EngineError> {
        let cfg = self.write(config_name, doc)?;
        // Whatever is at the report path now is NOT this restore's: see
        // `remove_stale_report`.
        remove_stale_report(report_path)?;
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
        // The exit code is checked BEFORE the dropped-key check: a run that
        // both dropped a key and failed must report the exit code and the
        // streams, the primary evidence of the failure; one that dropped a key
        // but exited 0 still gets the dropped-key error below. stdout is
        // included because the engine's log lines (a dropped-key warning
        // included) go to stdout.
        if run.exit_code != 0 {
            let (index, count, mapping) = which;
            let of_runs = if count > 1 {
                format!(
                    " (engine run {} of {count}, topics {})",
                    index + 1,
                    mapping.keys().cloned().collect::<Vec<_>>().join(", ")
                )
            } else {
                String::new()
            };
            return Err(EngineError::Operational(format!(
                "kafka-backup restore exited {}{of_runs}\nstdout: {}\nstderr: {}",
                run.exit_code,
                captured(&run.stdout),
                captured(&run.stderr)
            )));
        }
        self.assert_no_dropped_logweir_key(doc, &run.unknown_key_warnings)?;
        // `restore` has no --format; the exit code is its only machine-readable
        // VERDICT, so every timing here is OURS. Its offset-mapping report is
        // read for one thing only: an engine a SIGTERM stopped between
        // topics exits 0 and writes a report without the topics it never
        // started, and phase 7 refuses a report that lacks a mapped partition
        // the manifest proves holds records in the window.
        Ok(RestoreFacts {
            started_at,
            finished_at,
            exit_code: run.exit_code,
            unknown_key_warnings: run.unknown_key_warnings,
            engine_report: read_engine_report(report_path),
        })
    }

    /// Rendered documents are the coupling surface; a key WE rendered that the
    /// engine dropped aborts the run. `doc` is the exact string handed to the
    /// engine as `--config`, so this check runs against what was actually asked
    /// for.
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

    /// The key and bytes of `set`'s consumer-groups snapshot, or `None` when
    /// the store says there is no such object. The ONE reader of the object:
    /// `describe` hashes what it returns and `consumer_group_snapshot` parses
    /// it, so the two can never look at different keys.
    ///
    /// Only `StoreError::NotFound` is absence. Any other read failure is an
    /// error: neither presence nor a digest can be stated then.
    fn consumer_group_snapshot_object(
        &self,
        set: &BackupSetRef,
    ) -> Result<Option<(String, Vec<u8>)>, EngineError> {
        let name = vendored::consumer_groups::OBJECT_NAME;
        let key = set
            .manifest_key
            .rsplit_once('/')
            .map(|(dir, _)| format!("{dir}/{name}"))
            .unwrap_or_else(|| name.to_string());
        // Under the engine-document cap. An object over it is an
        // error like any other read failure that leaves presence unknown.
        match self
            .store
            .get_capped(&key, crate::storage::caps::ENGINE_DOCUMENT)
        {
            Ok((bytes, _)) => Ok(Some((key, bytes))),
            Err(crate::storage::StoreError::NotFound(_)) => Ok(None),
            Err(other) => Err(other.into()),
        }
    }

    /// `set`'s consumer-groups snapshot, read the way the engine writes it
    /// (`vendored::consumer_groups`).
    ///
    /// **A snapshot that is not in the engine's shape is a VALUE here,
    /// [`ConsumerGroupSnapshotRead::Unreadable`], not an error**: the object
    /// exists, it hashes to this digest, and this build cannot read its
    /// content. It is never `EngineError::Operational` (a failure that says
    /// nothing about the archive), and a caller that needs positions must say
    /// what an unreadable snapshot means for it ("not captured", never zero
    /// groups). The only `Err` is a read failure that leaves presence unknown.
    ///
    /// `describe_with_notices` reads it on every `drill run` and `backup run`,
    /// which publish its digest and PRINT an `Unreadable` outcome as a warning,
    /// never act on it: Logweir restores no consumer offsets and signs nothing
    /// about the snapshot.
    pub fn consumer_group_snapshot(
        &self,
        set: &BackupSetRef,
    ) -> Result<ConsumerGroupSnapshotRead, EngineError> {
        let Some((key, bytes)) = self.consumer_group_snapshot_object(set)? else {
            return Ok(ConsumerGroupSnapshotRead::Absent);
        };
        let sha256 = logweir_core::ids::sha256_prefixed(&bytes);
        Ok(match vendored::consumer_groups::parse(&bytes) {
            Ok(snapshot) => ConsumerGroupSnapshotRead::Parsed {
                key,
                sha256,
                snapshot,
            },
            Err(e) => ConsumerGroupSnapshotRead::Unreadable {
                key,
                sha256,
                reason: e.to_string(),
            },
        })
    }
}

/// The `ArchiveNotice::kind` of an unreadable consumer-groups snapshot, and
/// the value of the structured log's `notice` field when one is printed.
pub const CONSUMER_GROUP_SNAPSHOT_UNREADABLE: &str = "consumer-groups-snapshot-unreadable";

/// What `OsoCliEngine::consumer_group_snapshot` found for one backup set.
#[derive(Debug, Clone, PartialEq)]
pub enum ConsumerGroupSnapshotRead {
    /// No object at `<backup_id>/consumer-groups-snapshot.json`. The engine
    /// writes none when its snapshot is off (upstream's default, and always so
    /// in Logweir's own backups), but also when it SKIPPED it — the manifest
    /// named no topic yet at the end of the pass
    /// (`U/…/backup/engine.rs:853-856`) — or when writing it FAILED, which
    /// the engine only logs (`:584-589`, "non-fatal"). So absence means "not
    /// captured", never "no group had committed".
    Absent,
    /// The object, in the shape the engine writes. `sha256` is over its bytes,
    /// the same value `describe` publishes.
    Parsed {
        key: String,
        sha256: String,
        snapshot: vendored::consumer_groups::ConsumerGroupsSnapshot,
    },
    /// The object exists and hashes to `sha256`, but is not in the shape the
    /// engine writes; `reason` says where it departs from it.
    Unreadable {
        key: String,
        sha256: String,
        reason: String,
    },
}

impl ConsumerGroupSnapshotRead {
    /// `sha256:<hex>` of the object's bytes, when there is an object: what
    /// `describe` publishes as `consumer_group_snapshot_sha256`.
    pub fn sha256(&self) -> Option<&str> {
        match self {
            Self::Absent => None,
            Self::Parsed { sha256, .. } | Self::Unreadable { sha256, .. } => Some(sha256),
        }
    }

    /// The notice `drill run` and `backup run` print for an UNREADABLE
    /// snapshot, and `None` for every other outcome.
    pub fn notice(&self) -> Option<ArchiveNotice> {
        match self {
            Self::Unreadable {
                key,
                sha256,
                reason,
            } => Some(ArchiveNotice {
                kind: CONSUMER_GROUP_SNAPSHOT_UNREADABLE.to_string(),
                key: key.clone(),
                sha256: sha256.clone(),
                message: "the consumer-groups snapshot is present but unreadable; Logweir \
                          reads no consumer offsets from it and signs nothing about it, so \
                          this run does not depend on it"
                    .to_string(),
                reason: reason.clone(),
            }),
            Self::Absent | Self::Parsed { .. } => None,
        }
    }
}

/// The group type every group imported from an engine
/// consumer-group snapshot carries: the engine records none (it keeps a streams
/// group's positions with no type at all), so an imported group is never shown
/// as a consumer group's, and its positions are never applied on the snapshot's
/// word.
pub const IMPORTED_GROUP_TYPE: &str = "unknown";

/// One group of an engine snapshot, imported.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportedGroup {
    /// The group id the engine recorded.
    pub group_id: String,
    /// Always [`IMPORTED_GROUP_TYPE`].
    pub group_type: &'static str,
    /// The committed offsets the engine recorded, by topic then partition:
    /// only those `>= 0` on topics the engine archived, which is the engine's
    /// own filter — a group or a partition missing here was not captured,
    /// never "no committed position".
    pub positions: Vec<vendored::consumer_groups::Position>,
}

/// An engine consumer-group snapshot, imported: the ONE way a
/// foreign archive's snapshot is read as positions. Logweir's own backups
/// never write one (the rendered document leaves the engine's
/// `consumer_group_snapshot` at its default, `false`); they record the
/// selected groups natively in the receipt (`consumer_positions`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportedSnapshot {
    /// The object's key.
    pub key: String,
    /// `sha256:<hex>` of its bytes.
    pub sha256: String,
    /// When the engine took it, epoch milliseconds, when it says.
    pub snapshot_time_ms: Option<i64>,
    /// Every group, in the engine's order.
    pub groups: Vec<ImportedGroup>,
}

impl ConsumerGroupSnapshotRead {
    /// The snapshot as an import source: `None` when there is no
    /// object, `Some(Err)` when it is unreadable or a group's positions are not
    /// the engine's shape — never a partial import, which would read as "no
    /// committed position" for what it dropped.
    #[must_use]
    pub fn imported(&self) -> Option<Result<ImportedSnapshot, String>> {
        match self {
            Self::Absent => None,
            Self::Unreadable { reason, .. } => Some(Err(reason.clone())),
            Self::Parsed {
                key,
                sha256,
                snapshot,
            } => Some(
                snapshot
                    .groups
                    .iter()
                    .map(|g| {
                        g.positions()
                            .map(|positions| ImportedGroup {
                                group_id: g.group_id.clone(),
                                group_type: IMPORTED_GROUP_TYPE,
                                positions,
                            })
                            .map_err(|e| e.to_string())
                    })
                    .collect::<Result<Vec<_>, _>>()
                    .map(|groups| ImportedSnapshot {
                        key: key.clone(),
                        sha256: sha256.clone(),
                        snapshot_time_ms: snapshot.snapshot_time,
                        groups,
                    }),
            ),
        }
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

    /// The facts alone. `describe_with_notices` is the one body, so the two
    /// can never read the archive differently.
    fn describe(&self, set: &BackupSetRef) -> Result<BackupSetFacts, EngineError> {
        self.describe_with_notices(set).map(|(facts, _)| facts)
    }

    fn describe_with_notices(
        &self,
        set: &BackupSetRef,
    ) -> Result<(BackupSetFacts, Vec<ArchiveNotice>), EngineError> {
        let (bytes, version_id) = self
            .store
            .get_capped(&set.manifest_key, crate::storage::caps::MANIFEST)?;
        let m: vendored::manifest::BackupManifest = serde_json::from_slice(&bytes)
            .map_err(|e| EngineError::Operational(format!("{}: {e}", set.manifest_key)))?;
        let created_at = chrono::DateTime::from_timestamp_millis(m.created_at)
            .ok_or_else(|| EngineError::Operational("manifest created_at out of range".into()))?;
        // `describe` publishes two facts about the snapshot: that it EXISTS and
        // what it HASHES to; absent is normal. Only `StoreError::NotFound` is
        // absence, and any other read error propagates, because then neither
        // fact can be stated. Its CONTENT is neither a published fact nor a
        // reason to refuse the archive: no drill (it restores with
        // `auto_consumer_groups: false`) and no receipt reads it. An UNREADABLE
        // snapshot comes back as an `ArchiveNotice` beside the facts, printed
        // by `drill run` and `backup run`; this is the ONE read of the object,
        // so the digest and the notice are of the same bytes.
        let snapshot = self.consumer_group_snapshot(set)?;
        let notices: Vec<ArchiveNotice> = snapshot.notice().into_iter().collect();
        let facts = BackupSetFacts {
            backup_id: m.backup_id.clone(),
            created_at,
            source_cluster_id: m.source_cluster_id.clone(),
            manifest_sha256: logweir_core::ids::sha256_prefixed(&bytes),
            manifest_version_id: version_id,
            consumer_group_snapshot_sha256: snapshot.sha256().map(str::to_string),
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
                                    // relative key: `SegmentFacts.key` is
                                    // passed to `Store::get_capped` by
                                    // `phase7_verify::segment_evidence`, and a
                                    // relative key 404s on any archive with a
                                    // non-empty prefix (see `Store::qualify`).
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
        };
        Ok((facts, notices))
    }

    fn preflight(&self, plan: &RestorePlan) -> Result<PreflightReport, EngineError> {
        // G-GLOB: a topic (or a mapped target) carrying a glob metacharacter is
        // refused HERE, at phase 5, before the engine is spawned, as
        // `EngineError::Operational` (exit 1): `preflight` runs after phase 0's
        // guards, so exit 3 ("refused by a guard, before anything runs") does
        // not describe it.
        //
        // Every engine run's document is rendered and written before any engine
        // is spawned (a plan with no subset is one run, `restore.yaml`).
        let runs = render_restore::render_all_and_digest(plan)
            .map_err(|e| EngineError::Operational(e.to_string()))?;
        let mut written = Vec::with_capacity(runs.len());
        for (run, doc, digest) in &runs {
            written.push((self.write(&run.config_file_name(), doc)?, doc, digest));
        }
        // Memoised HERE, immediately after the write and before the engine is
        // spawned: the field means "the digest of the bytes now on disk as
        // restore.yaml", so the memo depends on our own write, not on the
        // engine's behaviour.
        *self.phase5_render.lock().expect("phase5_render mutex") =
            Some(runs.iter().map(|(_, _, d)| d.clone()).collect());
        let mut reports = Vec::with_capacity(written.len());
        for (cfg, doc, digest) in written {
            reports.push(self.validate_one_run(&cfg, doc, digest)?);
        }
        Ok(merge_preflight_reports(reports))
    }

    fn restore(
        &self,
        plan: &RestorePlan,
        obs: &mut dyn PhaseObserver,
    ) -> Result<RestoreFacts, EngineError> {
        // Phases 5 and 6 each render `restore.yaml`, and the write truncates,
        // so phase 6 overwrites the document phase 5 validated; this comparison
        // makes "the SAME document" a guarantee rather than a property of one
        // call site. A divergence is `EngineError::Operational`, EXIT 1 with no
        // artifact, not exit 3: by phase 6 phases 0-5 have run, including the
        // engine's own `validate-restore` (`docs/stability.md`). The comparison
        // is over EVERY run's document, in order.
        let runs = render_restore::render_all_and_digest(plan)
            .map_err(|e| EngineError::Operational(e.to_string()))?;
        let six: Vec<String> = runs.iter().map(|(_, _, d)| d.clone()).collect();
        match self
            .phase5_render
            .lock()
            .expect("phase5_render mutex")
            .as_deref()
        {
            Some(five) if five != six.as_slice() => {
                return Err(EngineError::Operational(format!(
                    "rendered restore.yaml diverged between phase 5 and phase 6: \
                     phase 5 validated {}, phase 6 would restore {} — refusing",
                    five.join(","),
                    six.join(",")
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
        let count = runs.len();
        // With several runs, `plan.offset_report` is the composed
        // report Logweir writes after the last run; whatever an earlier run
        // left there is not this restore's (`remove_stale_report`, per path).
        if count > 1 {
            remove_stale_report(&plan.offset_report)?;
        }
        let mut started_at = None;
        let mut unknown_key_warnings: Vec<String> = Vec::new();
        let mut reports = Vec::with_capacity(count);
        let mut report_paths = Vec::with_capacity(count);
        for (run, doc, _) in &runs {
            let report_path = run.path(&plan.offset_report);
            let facts = self.restore_one_run(
                &run.config_file_name(),
                doc,
                &report_path,
                obs,
                (run.index, count, &run.topic_mapping),
            )?;
            started_at.get_or_insert(facts.started_at);
            for w in facts.unknown_key_warnings {
                if !unknown_key_warnings.contains(&w) {
                    unknown_key_warnings.push(w);
                }
            }
            reports.push(facts.engine_report);
            report_paths.push(report_path);
        }
        let finished_at = chrono::Utc::now();
        let engine_report = if count > 1 {
            compose_offset_reports(&report_paths, &plan.offset_report)?;
            merge_engine_reports(reports)
        } else {
            reports.pop().unwrap_or(EngineReport::Absent)
        };
        Ok(RestoreFacts {
            started_at: started_at.unwrap_or(finished_at),
            finished_at,
            exit_code: 0,
            unknown_key_warnings,
            engine_report,
        })
    }

    fn fingerprints(&self, sel: &SampleSelection) -> Result<Vec<RecordFingerprint>, EngineError> {
        // `phase4_sample::run` cannot populate
        // `SampleSelection.set.manifest_key` (it sees only `BackupSetFacts`),
        // so a caller must patch it (`Selection::bind_backup_set`). Refusing
        // here names a forgotten patch step instead of relying on
        // `Store::get_capped("", …)` happening to fail legibly.
        if sel.set.manifest_key.is_empty() {
            return Err(EngineError::Operational(
                "SampleSelection.set.manifest_key is empty — phase 4 (phase4_sample::run) \
                 cannot populate it from BackupSetFacts alone; the caller must patch `sel.set` \
                 with the original BackupSetRef (Selection::bind_backup_set) before calling \
                 fingerprints"
                    .into(),
            ));
        }
        // Scoped to `sel.set.manifest_key` via `segment_keys_for_set`, NOT
        // `segment_keys_for`: across every manifest under the prefix, two
        // backup sets sharing a topic/partition with an overlapping window
        // would merge, and the archive side would be a strict superset of what
        // a real restore populated, failing a drill that succeeded.
        //
        // `count == 0` returns before touching storage at all.
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
            // One segment, under the segment ceiling (the decoder has its own
            // caps).
            let (bytes, _) = self.store.get_capped(&key, crate::storage::caps::SEGMENT)?;
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
            // `head` bounds the READ, not only the output; `tail` and `random`
            // need the window's full extent before they can choose. Safe for
            // `head` because `segment_keys_for_set` returns keys SORTED and the
            // key format zero-pads the start offset
            // (`segment-{offset:020}.bin{ext}`), so for one topic/partition
            // lexicographic order is ascending start offset: once `all` holds
            // `count` in-window records after a whole segment, no later segment
            // can hold an earlier record.
            if sel.anchor == Anchor::Head && all.len() >= sel.count {
                break;
            }
        }
        all.sort_by_key(|f| f.offset);
        select_sample(all, sel.anchor, sel.count)
    }

    /// GC18's `--from-cluster` half, and the second `run_engine` call site
    /// (interface **I32**).
    ///
    /// The engine's `backup` takes `--config` and nothing else [VERIFIED
    /// U:crates/kafka-backup-cli/src/main.rs:35-39,559-561]: no `--format`, no
    /// report file, so every fact about the run other than its exit code is
    /// ours to time and to read back off its streams.
    ///
    /// `render_and_digest` rather than `render`: it carries GC18(c) rail 3
    /// (`scan_rendered_document`, fail-closed) and **G-EXP**'s post-render
    /// sweep over the exact bytes about to be written. The digest is dropped
    /// here (`BackupFacts` has no field for it); the receipt can re-derive it
    /// from the same plan. `RenderError::UnsupportedAuthMode` stays the rail
    /// for a mode `AuthRender` does not have: `EngineError::Operational`, exit
    /// 1, never a silent downgrade to an unauthenticated document.
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
        // The exit code is checked BEFORE the dropped-key check, and both
        // streams go in the message, for the reasons `restore` records.
        if run.exit_code != 0 {
            return Err(EngineError::Operational(format!(
                "kafka-backup backup exited {}\nstdout: {}\nstderr: {}",
                run.exit_code,
                captured(&run.stdout),
                captured(&run.stderr)
            )));
        }
        // A key WE rendered that this engine tag dropped aborts the run, as in
        // `preflight` and `restore`.
        self.assert_no_dropped_logweir_key(&doc, &run.unknown_key_warnings)?;
        Ok(BackupFacts {
            started_at,
            finished_at,
            exit_code: run.exit_code,
            unknown_key_warnings: run.unknown_key_warnings,
        })
    }
}

/// The sample the caller asked for: `count` is a CAP (fewer matching records is
/// not an error), and a scorecard must not state a sample nobody took.
///
/// - `"head"`: the first `count` records by offset in the window, Kafka's own
///   "head of the log".
/// - `"tail"`: the last `count` records by offset.
/// - `"random"`: a DETERMINISTIC, evenly-spaced sample across the sorted set.
///   The audit trail is the list of `RecordFingerprint`s the scorecard embeds,
///   not a formula to regenerate them, so an RNG would buy nothing and cost a
///   dependency and exact-selection tests.
///
/// Any other value is a caller bug (the field is Logweir-internal) and is
/// reported, never defaulted.
fn select_sample(
    sorted: Vec<RecordFingerprint>,
    anchor: Anchor,
    count: usize,
) -> Result<Vec<RecordFingerprint>, EngineError> {
    // Backstop: `fingerprints()` returns before reading anything when
    // `sel.count == 0`.
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
            // Defensive: a stride that floors two `i` values to one index must
            // not repeat a record.
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
