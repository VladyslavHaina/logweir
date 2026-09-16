use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct EngineId {
    pub id: String,      // "oso-cli"
    pub version: String, // "v0.21.0"
    pub digest: String,  // "sha256:…"
}

/// A storage location shaped EXACTLY like OSO's own `StorageBackendConfig`, so
/// the rendered YAML and our own reads cannot drift apart. Upstream types it as
/// an internally tagged enum whose variants have incompatible REQUIRED fields
/// [VERIFIED U/kafka-backup/crates/kafka-backup-core/src/storage/config.rs:14-105
/// and config.rs:24] — `filesystem` requires `path` and has no `bucket`;
/// `azure` requires `account_name` + `container_name` and has no `bucket`; `gcs`
/// accepts only `bucket`/`service_account_path`/`prefix`. A stringly-typed
/// struct would render `backend: filesystem` alongside a `bucket:` key, and
/// serde would fail the config load with a hard "missing field" error that the
/// unknown-key readback of Task 12 CANNOT catch — the same argument this plan
/// already makes for `time_window_start`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "backend", rename_all = "lowercase")]
pub enum StorageUrl {
    S3 {
        bucket: String,
        #[serde(default)]
        prefix: String,
        #[serde(default)]
        region: Option<String>,
        #[serde(default)]
        endpoint: Option<String>,
        #[serde(default)]
        path_style: bool,
        #[serde(default)]
        allow_http: bool,
    },
    Azure {
        account_name: String,
        container_name: String,
        #[serde(default)]
        prefix: String,
    },
    Gcs {
        bucket: String,
        #[serde(default)]
        prefix: String,
    },
    Filesystem {
        path: std::path::PathBuf,
    },
}

impl StorageUrl {
    /// The key prefix, for the object_store wrapper's own listing. `filesystem`
    /// has none: upstream's variant carries only `path`.
    pub fn prefix(&self) -> &str {
        match self {
            Self::S3 { prefix, .. } | Self::Azure { prefix, .. } | Self::Gcs { prefix, .. } => {
                prefix
            }
            Self::Filesystem { .. } => "",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupSetRef {
    pub backup_id: String,
    pub manifest_key: String,
}

#[derive(Debug, Clone)]
pub struct BackupSetFacts {
    pub backup_id: String,
    pub created_at: DateTime<Utc>,
    pub source_cluster_id: Option<String>,
    pub manifest_sha256: String,
    pub manifest_version_id: Option<String>,
    /// `sha256:<hex>` of the sibling `consumer-groups-snapshot.json` when the
    /// object is present, else `None`. v0.1 records presence and hash only —
    /// restoring consumer offsets is a spec §2 non-goal.
    pub consumer_group_snapshot_sha256: Option<String>,
    pub topics: Vec<TopicFacts>,
}

impl BackupSetFacts {
    pub fn consumer_group_snapshot_present(&self) -> bool {
        self.consumer_group_snapshot_sha256.is_some()
    }

    /// **Guard G-WIN's floor.** The archive set's EARLIEST COVERED TIMESTAMP
    /// as recorded in the manifest, over the topics the caller NAMES: the
    /// minimum `start_timestamp` over `topics[].partitions[].segments[]` for
    /// every topic in `named_topics`, and nothing else.
    ///
    /// `None` when the set records no segment for any named topic, which is
    /// not a number this function may invent — a caller that needs a floor
    /// must refuse instead (`logweir::drill::build_plan`).
    ///
    /// # Why `named_topics` and not the whole set
    ///
    /// This is the restore-side twin of the backup receipt's
    /// `covered.from_ms`, which is the minimum over the topics the BACKUP
    /// named (`logweir::backup::phase_run`, interface I22), and the two must
    /// report the same instant for the same archive and the same topics or
    /// "the archive's earliest covered timestamp" means two things in two
    /// signed documents (plan erratum **E7(b)**). An archive set may hold
    /// topics this restore does not name — a set is written per backup, a
    /// restore selects from it — and an unnamed topic's older segment would
    /// otherwise drag the floor below every instant the restore can reach.
    ///
    /// # Why it lives here, on the facts
    ///
    /// TWO callers must get the SAME answer from the SAME manifest: plan
    /// construction computes the floor it binds into
    /// `RestorePlan.time_window.0`, and phase 5 RE-DERIVES it from the
    /// manifest it already holds to check the rendered document against it.
    /// Two independent minimum-walks would be free to drift, and the whole
    /// point of the phase-5 check is that it is an independent reading of the
    /// same fact — not of the same code path's cached result. Both callers
    /// pass the keys of the SAME topic mapping for the same reason.
    ///
    /// The MINIMUM, never the maximum: a floor taken from the newest segment
    /// would exclude every record before it, which is precisely the silent
    /// loss G-WIN refuses. And the minimum over the whole walk, never the
    /// first segment it reaches: nothing orders a real manifest, so a later
    /// partition may hold the older segment.
    pub fn earliest_covered_timestamp_ms(
        &self,
        named_topics: &std::collections::BTreeSet<&str>,
    ) -> Option<i64> {
        self.topics
            .iter()
            .filter(|t| named_topics.contains(t.name.as_str()))
            .flat_map(|t| t.partitions.iter())
            .flat_map(|p| p.segments.iter())
            .map(|seg| seg.start_timestamp)
            .min()
    }
}

/// Returns (lower, upper): the count the manifest can PROVE over [floor_ms, pit_ms].
/// `lower` sums record_count for segments wholly inside the window; `upper` adds
/// every segment that straddles either bound. A restored count outside [lower, upper]
/// is an integrity failure; inside it, the sampled reconciliation is the finer check.
///
/// SegmentFacts::record_count is i64 (engine.rs:120) and non-negative by
/// construction; a negative value contributes 0 rather than wrapping.
///
/// # Both bounds are INCLUSIVE, because the plan's window is
///
/// `RestorePlan::time_window` is `(floor, point_in_time-or-window_end)` and its
/// END is inclusive — the engine's own restore filter is
/// `r.timestamp >= s && r.timestamp <= e`
/// [U:crates/kafka-backup-core/src/restore/helpers.rs:74-82], and
/// `docs/stability.md` records that the restore window's end is inclusive while
/// the backup receipt's `covered.to_ms` is exclusive. So "wholly inside" here is
/// `start_timestamp >= floor_ms && end_timestamp <= pit_ms`, and a segment
/// touching a bound exactly is inside it, not across it.
///
/// # Why a BOUND and not an equality
///
/// The manifest's finest granularity is the SEGMENT: `SegmentFacts` carries
/// `start_timestamp`/`end_timestamp`/`record_count` and there is no per-record
/// timestamp anywhere in `BackupSetFacts`. When `point_in_time` falls INSIDE a
/// segment — the normal case for a point-in-time restore — the number of records
/// at or before it is not derivable, so the manifest can only over-count (take
/// the whole segment) or under-count (drop it). An equality would therefore fail
/// a CORRECT implementation, and would then be "fixed" by weakening it, which is
/// what STANDING RULE 21 forbids. Hence: `lower` is what the manifest proves must
/// be there, `upper` is the most it could account for, and the boundary question
/// inside the bound belongs to G-PITR's payload set, never to a count.
///
/// # Segments outside the window contribute to NEITHER bound
///
/// A segment that ends before `floor_ms` or starts after `pit_ms` overlaps the
/// window nowhere; counting it into `upper` would let an arbitrarily large
/// archive make the upper bound unfalsifiable.
///
/// # The caller restricts `facts` to the topics it will count
///
/// This function sums over every topic in the `facts` it is handed, deliberately:
/// its signature carries no topic filter, so the ONE decision about which topics
/// are in scope stays at the call site, next to the count it is compared against.
/// `logweir::drill::phase7_verify::check_restored_count` therefore passes a
/// `BackupSetFacts` restricted to the source topics its `topic_mapping` names,
/// which is the same rule `earliest_covered_timestamp_ms` applies to the floor
/// (plan erratum **E7(b)**) — an archive may hold topics a restore does not
/// name, and their records are not records this restore was ever going to write.
pub fn expected_restored_count(facts: &BackupSetFacts, floor_ms: i64, pit_ms: i64) -> (u64, u64) {
    let mut lower = 0u64;
    let mut straddling = 0u64;
    for seg in facts
        .topics
        .iter()
        .flat_map(|t| t.partitions.iter())
        .flat_map(|p| p.segments.iter())
    {
        // `record_count` is i64 and non-negative by construction; a negative
        // value contributes 0 rather than wrapping a `u64` to ~1.8e19, which
        // would make the upper bound unfalsifiable.
        let n = seg.record_count.max(0) as u64;
        let wholly_inside = seg.start_timestamp >= floor_ms && seg.end_timestamp <= pit_ms;
        let overlaps = seg.end_timestamp >= floor_ms && seg.start_timestamp <= pit_ms;
        if wholly_inside {
            lower += n;
        } else if overlaps {
            straddling += n;
        }
    }
    (lower, lower + straddling)
}

/// Where `RestorePlan.time_window.0` came from. An enum, not a bool, so the
/// plan's claim about its own floor is a VALUE that can be checked against the
/// floor it describes — and the one place that check happens is plan
/// construction: `logweir::drill::build_plan_with_floor` refuses, exit 3, an
/// `ArchiveManifest` plan whose `time_window.0` is not the manifest floor it
/// was handed. In tag 1 that check is this enum's only reader.
///
/// **Phase 5 does not read it.**
/// `logweir::drill::phase5_preflight::check_rendered_window_floor` renders the
/// document, parses the `time_window_start` INTEGER back off the bytes and
/// compares it with a floor re-derived from the manifest, so it refuses an
/// `InheritedFromSpec` plan whose start is not the archive's exactly as it
/// refuses an `ArchiveManifest` one: by the integer, never by the claim. A
/// guard that read the claim could be talked out of refusing by the very plan
/// it is refusing.
///
/// `InheritedFromSpec` is constructed nowhere in tag 1 (plan erratum E7): it
/// is the negative arm of the plan-construction check, and the variant a later
/// restore mode may use (interface I33).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowFloorSource {
    ArchiveManifest,
    InheritedFromSpec,
}

#[derive(Debug, Clone)]
pub struct TopicFacts {
    pub name: String,
    pub original_partition_count: Option<i32>,
    pub source_replication_factor: Option<i16>,
    pub configurations: BTreeMap<String, String>,
    pub partitions: Vec<PartitionFacts>,
}

#[derive(Debug, Clone)]
pub struct PartitionFacts {
    pub partition_id: i32,
    pub segments: Vec<SegmentFacts>,
    /// Ranges the backup could NOT capture.
    pub gaps: Vec<(i64, i64)>,
    /// Ranges deliberately removed by retention.
    pub pruned: Vec<(i64, i64)>,
}

#[derive(Debug, Clone)]
pub struct SegmentFacts {
    pub key: String,
    pub start_offset: i64,
    pub end_offset: i64,
    pub start_timestamp: i64,
    pub end_timestamp: i64,
    pub record_count: i64,
    /// Empty for segments written before 0.21.
    pub sha256: String,
    /// 0 for segments written before 0.21.
    pub uploaded_at: i64,
}

#[derive(Debug, Clone)]
pub struct RestorePlan {
    pub set: BackupSetRef,
    pub storage: StorageUrl,
    pub target_bootstrap: Vec<String>,
    /// How the TARGET cluster's client is told to authenticate — the restore
    /// twin of `BackupPlan::source_auth`, and **guard G-ID's carrier**.
    ///
    /// It is a field of the PLAN, which is what `plan_hash` covers, precisely
    /// so the rendered `sasl_username` is a function of the approved bytes and
    /// not of a `KafkaCluster` object a controller could re-read after
    /// approval. Never a password — see `AuthRender`.
    pub target_auth: AuthRender,
    /// One explicit entry per selected topic: "<source>" -> "<prefix><source>".
    pub topic_mapping: BTreeMap<String, String>,
    pub time_window: (DateTime<Utc>, DateTime<Utc>),
    /// Where `time_window.0` came from — **guard G-WIN's carrier**, and the
    /// reason a `RestorePlan` cannot claim an archive-bound floor while
    /// holding a spec-supplied one: `logweir::drill::build_plan` ends with an
    /// explicit check (not a `debug_assert`, which is compiled out in
    /// release) that `ArchiveManifest` implies `time_window.0` equals the
    /// manifest floor it just computed, and refuses exit 3 otherwise.
    pub window_floor_source: WindowFloorSource,
    pub default_replication_factor: i16,
    pub checkpoint_state: std::path::PathBuf,
    pub checkpoint_interval_secs: u64,
    /// Where the engine writes its offset-mapping report
    /// [U:crates/kafka-backup-core/src/config.rs:857-858
    /// `pub offset_report: Option<std::path::PathBuf>`].
    ///
    /// A FIELD OF THE PLAN, like `checkpoint_state` beside it, and for the
    /// same reason: `plan_hash` covers the plan, so the path the engine writes
    /// to is a function of the approved bytes rather than of an environment
    /// variable read at render time. `--offset-report-out` sets it; absent
    /// that it is `offsets.json` in the same per-run workdir as
    /// `checkpoint_state`.
    ///
    /// **Pod-local, and that is why phase 8 uploads it.** The engine writes
    /// this file with `tokio::fs::write` inside the container
    /// [U:crates/kafka-backup-core/src/restore/engine.rs:417-427] and the pod
    /// is then deleted, so the only durable copy is the one
    /// `logweir::drill::phase8_score::run` puts at
    /// `logweir/drills/<run_id>.offsets.json` — recorded, with its digest, in
    /// the signed scorecard's `evidence.offset_report_key` /
    /// `evidence.offset_report_sha256`.
    ///
    /// Global Constraint 20 is UNCHANGED by its presence: tag 1 renders the
    /// report and applies nothing (constraint 35). The report is a
    /// description of what the offsets WOULD map to; committing it is tag 2's
    /// `Switchover`, behind its own approval and its own ADR.
    pub offset_report: std::path::PathBuf,
}

/// How a cluster's client is told to authenticate, as the renderer needs it —
/// the render-side twin of `crate::spec::AuthSpec`. Carried by
/// `BackupPlan::source_auth` and by `RestorePlan::target_auth`.
///
/// **It never carries a password.** `ScramSha512` names the username only;
/// the secret reaches the engine through its own `${VAR}` environment
/// expansion, which is the one thing `yaml_scalar` explicitly cannot defend
/// against and therefore the one thing that must never be interpolated by us.
/// `crate::spec::AuthSpec::to_render()` is the one mapping into this type
/// (interface **I1**).
///
/// The `Plaintext` arm renders **nothing at all** in all three documents: the
/// engine's own `SecurityProtocol` default is `PLAINTEXT`
/// [U:crates/kafka-backup-core/src/config.rs:261-269], so an explicit key
/// would be a fourth spelling to keep in step with upstream for no
/// behavioural gain — and it is what keeps every golden that predates SCRAM
/// byte-identical.
///
/// # `tls_ca_file` is a path, it is not in the plan hash, and it needs TLS
///
/// The CA certificate file a TLS connection's brokers are verified against,
/// when it is not in the engine's bundled roots — rendered as the engine's
/// `ssl_ca_location` (PLAT-07.1). It is a POD-LOCAL PATH to a file projected
/// from the saved connection, never certificate text, and it is attached by the
/// runner from `crate::connection::{SOURCE,TARGET}_TLS_CA_FILE_ENV` after the
/// plan is built, exactly as the password placeholder is: a trust anchor is a
/// deployment input that rotates, not an identity an approval binds.
/// [`AuthRender::with_tls_ca_file`] is the one way it is attached, and it
/// refuses a CA for a connection that is not TLS.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthRender {
    Plaintext,
    ScramSha512 {
        username: String,
        tls: bool,
        tls_ca_file: Option<String>,
    },
}

impl AuthRender {
    /// Attach the projected CA file, or refuse because the transport is not
    /// TLS. `None` returns `self` unchanged, so a connection with no private CA
    /// renders byte-identically to one built before the field existed.
    ///
    /// # Errors
    ///
    /// [`crate::connection::TlsCaWithoutTls`] when `ca_file` is `Some` and the
    /// auth is `Plaintext` or `ScramSha512 { tls: false, .. }`.
    pub fn with_tls_ca_file(
        self,
        ca_file: Option<String>,
    ) -> Result<AuthRender, crate::connection::TlsCaWithoutTls> {
        let Some(ca_file) = ca_file else {
            return Ok(self);
        };
        match self {
            AuthRender::ScramSha512 { username, tls, .. } if tls => Ok(AuthRender::ScramSha512 {
                username,
                tls,
                tls_ca_file: Some(ca_file),
            }),
            AuthRender::ScramSha512 { .. } => Err(crate::connection::TlsCaWithoutTls {
                mode: "scramSha512",
            }),
            AuthRender::Plaintext => Err(crate::connection::TlsCaWithoutTls { mode: "plaintext" }),
        }
    }
}

/// Everything `render_backup::render` needs to emit a `backup.yaml`, and
/// nothing else.
///
/// # There is deliberately no `source_cluster_id` field — do not look for one
///
/// GC18(c)'s fourth rail is "the source `cluster_id` recorded and re-asserted
/// `!= target`". That rail lands in **Task 4**, where the run happens, and NOT
/// as a field here, because phase 0 reads the cluster id FROM THE BROKER and
/// never from a spec (`crates/logweir/src/drill/phase0_admit.rs:94`). A
/// `source_cluster_id` on this struct would be an adopter-supplied string
/// standing where a measured fact belongs — the same class of defect as
/// letting a drill spec widen its own cluster allowlist, which is why
/// `AllowedClusters` is a separate file argument.
///
/// The other three rails ARE this struct's business and are enforced by the
/// renderer: `topics` is a named allowlist with no glob metacharacter
/// (**G-GLOB**, `crate::guard::reject_glob_metacharacters`); the type has no
/// field that could produce `reset_consumer_offsets`, `auto_consumer_groups`,
/// `create_topics` or any consumer-group key, so the rendered document is
/// read-only by construction; and `purge_topics`/`dry_run` are unrepresentable
/// here and re-scanned out of the rendered bytes by `render_and_digest`.
#[derive(Debug, Clone)]
pub struct BackupPlan {
    pub backup_id: String,
    pub source_bootstrap: Vec<String>,
    /// Never a password — see `AuthRender`.
    pub source_auth: AuthRender,
    /// Named topics. No glob metacharacter: one entry is one topic, because
    /// the engine's `TopicSelection` would otherwise read a name as a pattern.
    pub topics: Vec<String>,
    pub storage: StorageUrl,
    /// `"zstd"`.
    pub compression: String,
    pub segment_max_records: u64,
    pub segment_max_bytes: u64,
    pub max_concurrent_partitions: u32,
}

/// Logweir-measured, never engine-reported — the backup twin of
/// `RestoreFacts`, and for the same reason: `backup` has no `--format` and
/// writes no report file, so its start, finish and exit code are ours to
/// time and its unknown-key warnings are ours to read back off stderr.
#[derive(Debug, Clone)]
pub struct BackupFacts {
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    pub exit_code: i32,
    pub unknown_key_warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoverageState {
    Full,
    Partial,
    Missing,
    Empty,
    DataMissing,
    Corrupt,
    Indeterminate,
    Unknown(String),
}

#[derive(Debug, Clone)]
pub struct PartitionCoverage {
    pub topic: String,
    pub partition: i32,
    pub state: CoverageState,
    pub detail: String,
}

#[derive(Debug, Clone)]
pub struct PreflightReport {
    pub valid: bool,
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
    pub segments_to_process: u64,
    pub records_to_restore: u64,
    pub time_range: Option<(i64, i64)>,
    pub partitions: Vec<PartitionCoverage>,
    /// The per-run readback of spec §9.3 phase 5(1).
    pub header_preflight_honoured: bool,
    /// Paths the engine logged as `Ignoring unknown config key <path>`.
    pub unknown_key_warnings: Vec<String>,
    /// `sha256:<hex>` over the exact bytes written as `restore.yaml` at phase 5.
    /// Phase 6 re-renders, re-hashes and refuses on divergence (T0-14; ruling
    /// R-E: the refusal is exit 1, operational, no artifact — by phase 6 the
    /// guards have run and `validate-restore` has already executed, so GC11's
    /// exit 3 "refused before anything runs" does not describe it).
    pub rendered_restore_sha256: String,
}

/// Logweir-measured, never engine-reported: `restore` has no --format and
/// writes no report file [VERIFIED U/kafka-backup/crates/kafka-backup-cli/src/main.rs:47-51].
#[derive(Debug, Clone)]
pub struct RestoreFacts {
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    pub exit_code: i32,
    pub unknown_key_warnings: Vec<String>,
}

/// Task 19 glue: the engine's own `validation run --config validation.yaml
/// --triggered-by <s>` subcommand prints human text only and writes no
/// machine-readable report to stdout — its ONLY per-run signal reachable
/// through this trait is the exit code (see `DataEngine::validation_run`'s
/// doc comment for why phase 7 never reads more than that).
#[derive(Debug, Clone, Copy)]
pub struct EngineRun {
    pub exit_code: i32,
}

#[derive(Debug, Clone)]
pub struct SampleSelection {
    /// Which backup set's segments to sample. Without this, a `Store` whose
    /// prefix covers more than one backup set (the ordinary case for an
    /// incremental chain, a re-run, or a daily-plus-hourly archive) cannot
    /// tell `fingerprints()` which manifest's segments to read: two sets
    /// sharing a topic/partition with an overlapping window would otherwise
    /// merge silently, making the archive side a strict SUPERSET of what a
    /// real restore populated — extra fingerprints read as a mismatch, so a
    /// healthy restore fails a drill that actually succeeded. Added per
    /// controller authorization after Task 12's review (see
    /// task-12-fix-report.md): no drill phase consumed this type yet, so this
    /// was the cheapest point at which to add it.
    pub set: BackupSetRef,
    pub topic: String,
    pub partition: i32,
    /// A closed set since Task 21c — see `crate::spec::Anchor` for why, and
    /// for why v0.1 refuses everything but `Head` at phase 0.
    pub anchor: crate::spec::Anchor,
    /// A CAP on how many fingerprints `fingerprints()` returns, not a promise
    /// of exactly this many: fewer than `count` matching records in the
    /// window is not an error. What this bounds differs BY ANCHOR — an
    /// earlier version of this comment claimed a memory guarantee the code
    /// did not actually provide for every anchor, which is corrected here:
    /// - `"head"`: bounds the READ, not only the output.
    ///   `OsoCliEngine::fingerprints` stops decoding further segments once
    ///   `count` in-window records are in hand, because the earliest records
    ///   are already known once seen — no segment processed later (higher
    ///   start offset) can contain an earlier one.
    /// - `"tail"` and `"random"`: bound only the OUTPUT, not the read. Both
    ///   need the window's full extent before they can choose (the latest
    ///   `count` records, or an evenly-spaced span across all of them), so
    ///   every segment in the window is decoded and buffered regardless of
    ///   `count`; only what is RETURNED is trimmed.
    ///
    /// `count == 0` is bounded for every anchor: `fingerprints()` returns
    /// immediately, before reading anything.
    pub count: usize,
    pub window: (i64, i64),
}

/// sha256(key‖value‖headers‖timestamp), with headers sorted by key then value
/// and each field length-prefixed — see logweir_kafka::fingerprint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordFingerprint {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub sha256: String,
}

pub trait PhaseObserver {
    fn phase_started(&mut self, phase: i8, name: &str);
    fn phase_finished(&mut self, phase: i8, outcome: &str);
    fn engine_line(&mut self, stream: &str, line: &str);
}

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error("operational: {0}")]
    Operational(String),
    /// The KBAK decoder cannot read this segment. The drill records
    /// integrity.level "consume-only" rather than failing (spec §11).
    #[error("unsupported: {0}")]
    Unsupported(String),
}

pub trait DataEngine {
    fn id(&self) -> EngineId;
    fn list_backup_sets(&self, loc: &StorageUrl) -> Result<Vec<BackupSetRef>, EngineError>;
    fn describe(&self, set: &BackupSetRef) -> Result<BackupSetFacts, EngineError>;
    fn preflight(&self, plan: &RestorePlan) -> Result<PreflightReport, EngineError>;
    /// PRECONDITION: `preflight(plan)` must have been called on this same
    /// engine value first, and `restore` must be given the same `plan`.
    ///
    /// This is not merely a phase ordering the orchestrator happens to follow.
    /// Phase 5 records the digest of the exact bytes it wrote as `restore.yaml`
    /// (`PreflightReport::rendered_restore_sha256`), and phase 6 re-renders,
    /// re-hashes and REFUSES on divergence (T0-14) — so `restore` needs that
    /// digest to compare against, and an implementation that memoises it on the
    /// engine value has nothing to compare when `preflight` never ran. An
    /// implementor MUST fail with `EngineError::Operational` in that case
    /// (`OsoCliEngine::restore`: "restore() called before preflight(): no
    /// phase-5 render to compare against") and MUST NOT silently skip the
    /// comparison, which would restore bytes nothing validated.
    ///
    /// Calling `restore` first is therefore a caller bug, never a drill result:
    /// it is exit 1 with no artifact, not exit 2.
    fn restore(
        &self,
        plan: &RestorePlan,
        obs: &mut dyn PhaseObserver,
    ) -> Result<RestoreFacts, EngineError>;
    fn fingerprints(&self, sel: &SampleSelection) -> Result<Vec<RecordFingerprint>, EngineError>;

    /// Task 19 glue, not in this trait's original Task 8a contract. Phase 7's
    /// brief (`crates/logweir/src/drill/phase7_verify.rs`) calls
    /// `OsoCliEngine::validation_run(plan)` "through the concrete engine
    /// handle the orchestrator holds" — but phase 7's own signature only ever
    /// receives `engine: &dyn DataEngine`, and this trait had no method that
    /// could reach that subprocess through a trait object. Adding it here
    /// WITH A DEFAULT BODY is the minimal fix: every existing implementor
    /// (`OsoCliEngine`, and every test double across `logweir-core`,
    /// `logweir-engine-oso` and `crates/logweir`'s own test fixtures) keeps
    /// compiling unchanged, because none of them override it.
    ///
    /// The default returns `Operational` rather than a fabricated
    /// `EngineRun { exit_code: 0 }`: a silent fake pass here would let phase
    /// 7's `notify`/logging code report an engine validation that never ran.
    /// `OsoCliEngine`'s real override — spawning `validation run --config
    /// validation.yaml --triggered-by <s>` — is NOT added by this task: it
    /// needs the extracted `kafka-backup` binary to test against, and Docker
    /// (this environment's only path to that binary) is unusable here. See
    /// `phase7_verify::engine_validation_run`'s doc comment for the further,
    /// separate reason (a known upstream topic-rename mismatch) this run's
    /// CONTENT must never be trusted even once that override exists.
    fn validation_run(&self, _plan: &RestorePlan) -> Result<EngineRun, EngineError> {
        Err(EngineError::Operational(
            "validation_run is not implemented by this engine".into(),
        ))
    }

    /// PRECONDITION: none. Runs the engine's `backup` subcommand over the
    /// rendered document.
    ///
    /// The default body returns `EngineError::Operational` so every existing
    /// test double keeps compiling — exactly the `DataEngine::validation_run`
    /// pattern above, and for the same reason: this trait is implemented by
    /// `OsoCliEngine` and by a double in `logweir-core`,
    /// `logweir-engine-oso` and `crates/logweir`, none of which can be made
    /// to override a new REQUIRED method in the task that adds it.
    ///
    /// **Never a fabricated `Ok(BackupFacts { exit_code: 0, .. })`.** Phase
    /// −1's caller reads this result and then reads the ARCHIVE the run was
    /// supposed to produce; a default that claimed a clean exit would put a
    /// `records_per_topic` and a covered window from somebody else's archive
    /// behind a backup that never ran, which is the same class of defect as
    /// `validation_run`'s silent fake pass.
    /// `default_data_engine_backup_is_operational`
    /// (`logweir-core/tests/engine_trait.rs`) pins it.
    fn backup(
        &self,
        _plan: &BackupPlan,
        _obs: &mut dyn PhaseObserver,
    ) -> Result<BackupFacts, EngineError> {
        Err(EngineError::Operational(
            "backup is not implemented by this engine".into(),
        ))
    }
}
