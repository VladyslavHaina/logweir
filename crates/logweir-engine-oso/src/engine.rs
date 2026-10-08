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

/// **FX-23.** The engine's offset-mapping report at `path`, as phase 7 reads
/// it: the `(target topic, partition)` of every entry.
///
/// `Absent` when there is no file (the engine writes one only from a completed
/// restore, and its own write failure is a warning upstream), `Unreadable`
/// when there is one this build cannot parse. Neither is an error: the report
/// is evidence of what the engine LACKS, never of what it wrote, and phase 7
/// logs a report it could not check.
///
/// **Streamed, never buffered (FX-23 review M1).** The report carries one
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

/// FX-23: whatever is at a report path before a restore is NOT that
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

/// **PROD-11.1.** One `PreflightReport` for a plan restored by several engine
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

/// **PROD-11.1.** The offset report of a restore made of several engine runs:
/// every run's report is checked (FX-23) only when every run's was READ — the
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

/// **PROD-11.1.** The evidence file a multi-run restore leaves at the plan's
/// `offset_report` path, which phase 8 uploads: a JSON ARRAY with one element
/// per engine run, in run order — that run's report, byte for byte, or `null`
/// when the run wrote none. The bytes are concatenated, never parsed, so the
/// per-record section of a large report is never held in memory (FX-23 review
/// M1). A one-run restore keeps the engine's own report as before.
pub fn compose_offset_reports(runs: &[PathBuf], out: &std::path::Path) -> Result<(), EngineError> {
    use std::io::Write as _;
    let op = |e: std::io::Error| EngineError::Operational(format!("{}: {e}", out.display()));
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
    ///
    /// PROD-11.1: ONE DIGEST PER ENGINE RUN, in run order. A plan whose
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
    /// at `cfg` (PROD-11.1: `preflight` calls it once per run).
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
            rendered_restore_sha256: digest.to_string(),
        })
    }

    /// One engine run's `restore` over `doc` (PROD-11.1: `restore` calls it
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
        // FX-23: whatever is at the report path now is NOT this restore's. The
        // default path is per run, but `--offset-report-out` may name a fixed
        // one, and a report an earlier run left there would be read below as
        // this run's (and uploaded by phase 8 as its evidence) whenever this
        // engine writes none. Removed first, so after the run the file is this
        // engine's or it is absent.
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
        // read for one thing only (FX-23): an engine a SIGTERM stopped between
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

    /// The key and bytes of `set`'s consumer-groups snapshot, or `None` when
    /// the store says there is no such object. The ONE reader of the object:
    /// `describe` hashes what it returns and `consumer_group_snapshot` parses
    /// it, so the two can never look at different keys.
    ///
    /// Only `StoreError::NotFound` is absence. Any other read failure is an
    /// error (Task 12): neither presence nor a digest can be stated then.
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
        match self.store.get(&key) {
            Ok((bytes, _)) => Ok(Some((key, bytes))),
            Err(crate::storage::StoreError::NotFound(_)) => Ok(None),
            Err(other) => Err(other.into()),
        }
    }

    /// FX-1. `set`'s consumer-groups snapshot, read the way the engine writes
    /// it (`vendored::consumer_groups`).
    ///
    /// **A snapshot that is not in the engine's shape is a VALUE here,
    /// [`ConsumerGroupSnapshotRead::Unreadable`], not an error.** It is a fact
    /// about the archive — the object exists, it hashes to this digest, and
    /// this build cannot read its content — so it is reported as one, with the
    /// reason. It is never `EngineError::Operational`, which the drill and the
    /// backup define as a failure that says nothing about the archive (exit
    /// 1, "fix the environment and re-run"), and it is not an `Err` a caller
    /// can propagate with `?` by reflex: a caller that needs the positions
    /// matches on the outcome and must say what an unreadable snapshot means
    /// for it (PROD-04.1: "not captured", never zero groups). The only `Err`
    /// is a read failure that leaves presence unknown.
    ///
    /// `describe_with_notices` calls it, so `drill run` and `backup run` read
    /// it on every run: they publish its digest, and they PRINT an
    /// `Unreadable` outcome as a warning (FX-1 fix round, M1) — never act on
    /// it, because Logweir restores no consumer offsets (spec §2 non-goals)
    /// and signs nothing about the snapshot. It is also the reader PROD-04.1's
    /// import of foreign archives builds on.
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
    /// captured", never "no group had committed" (PROD-04.1).
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
    /// snapshot, and `None` for every other outcome (FX-1 fix round, M1).
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
        let (bytes, version_id) = self.store.get(&set.manifest_key)?;
        let m: vendored::manifest::BackupManifest = serde_json::from_slice(&bytes)
            .map_err(|e| EngineError::Operational(format!("{}: {e}", set.manifest_key)))?;
        let created_at = chrono::DateTime::from_timestamp_millis(m.created_at)
            .ok_or_else(|| EngineError::Operational("manifest created_at out of range".into()))?;
        // Spec §14 SP1b names consumer-groups-snapshot.json in scope and
        // §10/GT-10 treat it as part of the artifact set a reader must handle.
        // `describe` publishes two facts about it: that it EXISTS and what it
        // HASHES to. Absent is normal, not an error.
        //
        // Task 12 fix: `Err(_) => None` used to map EVERY read failure — a
        // 403, a timeout, a truncated read, genuine absence — onto the same
        // `None`, a positive claim ("this artefact was never uploaded") the
        // store never established. Only `StoreError::NotFound` means that; any
        // other read error still propagates, because then neither fact can be
        // stated.
        //
        // FX-1: its CONTENT is not a fact `describe` publishes, and it is no
        // longer a reason to refuse the archive. The content used to be parsed
        // against an invented shape and a mismatch returned `Operational` —
        // "says nothing about the archive; fix the environment and re-run"
        // (exit 1, no artifact) — so every drill and every backup receipt of an
        // archive holding a real, non-empty snapshot failed, and no re-run
        // could ever succeed. Neither caller reads the content: a drill
        // restores data with `auto_consumer_groups: false`
        // (`render_restore.rs`) and signs no snapshot fact, and a receipt
        // carries none.
        //
        // FX-1 fix round (M1): an UNREADABLE snapshot is still told, not
        // swallowed. It comes back as an `ArchiveNotice` beside the facts,
        // which `drill run` and `backup run` print as a warning line and a
        // structured log event. The read below is the ONE read of the object:
        // the digest published and the notice printed are of the same bytes.
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
        };
        Ok((facts, notices))
    }

    fn preflight(&self, plan: &RestorePlan) -> Result<PreflightReport, EngineError> {
        // G-GLOB: a topic (or a mapped target) carrying a glob
        // metacharacter is refused HERE, at phase 5, before the engine is
        // spawned. `EngineError::Operational` is exit 1 — the same mapping
        // ruling R-E gives the phase-5/phase-6 digest divergence, and for the
        // same reason: `preflight` is reached only after phase 0's guards have
        // run, so Global Constraint 11's exit 3 ("refused by a guard, before
        // anything runs") does not describe it.
        //
        // PROD-11.1: every engine run's document, rendered and written before
        // any engine is spawned (a plan with no subset is one run and writes
        // `restore.yaml`, exactly as before).
        let runs = render_restore::render_all_and_digest(plan)
            .map_err(|e| EngineError::Operational(e.to_string()))?;
        let mut written = Vec::with_capacity(runs.len());
        for (run, doc, digest) in &runs {
            written.push((self.write(&run.config_file_name(), doc)?, doc, digest));
        }
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
        //
        // PROD-11.1: the comparison is over EVERY run's document, in order.
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
        // PROD-11.1: with several runs, `plan.offset_report` is the composed
        // report Logweir writes after the last run; whatever an earlier run
        // left there is not this restore's (FX-23's rule, below, per path).
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
    /// SASL/SCRAM-SHA-512 renders since Task 6, so nothing on this path
    /// refuses an auth mode any more. `RenderError::UnsupportedAuthMode` is
    /// kept as the rail for a mode `AuthRender` does not yet have (see the
    /// variant's own doc comment) and would still leave this method as
    /// `EngineError::Operational`, exit 1 by ruling R-E — never a panic and
    /// never a silent downgrade to an unauthenticated document.
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
