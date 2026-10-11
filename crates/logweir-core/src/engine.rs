use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct EngineId {
    pub id: String,      // "oso-cli"
    pub version: String, // "v0.21.0"
    pub digest: String,  // "sha256:…"
}

/// A storage location shaped EXACTLY like OSO's own `StorageBackendConfig`
/// [VERIFIED
/// U/kafka-backup/crates/kafka-backup-core/src/storage/config.rs:14-105], so
/// the rendered YAML and our own reads cannot drift apart: an internally tagged
/// enum whose variants have incompatible REQUIRED fields (`filesystem` requires
/// `path`, `azure` `account_name` + `container_name`, `gcs` only
/// `bucket`/`service_account_path`/`prefix`). A stringly-typed struct would
/// render `backend: filesystem` beside a `bucket:` key, which the engine's
/// config load fails with "missing field".
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
    /// **C15**: an S3 location whose endpoint is plain `http://` while
    /// `allow_http` is false. From 0.22.0 on, including the 0.23.3 pin, the
    /// engine's YAML path ORs `allow_http` with the endpoint's scheme
    /// (`storage/config.rs:116-118`, `storage/mod.rs:58-73`) and dials the
    /// endpoint in the clear, and YAML cannot tell `allow_http: false` from an
    /// absent key, so the combination is refused before a document exists. The
    /// scheme is compared without regard to ASCII case, stricter than the
    /// engine's own test.
    pub fn plaintext_endpoint_without_allow_http(&self) -> bool {
        match self {
            Self::S3 {
                endpoint: Some(endpoint),
                allow_http: false,
                ..
            } => endpoint
                .trim_start()
                .get(..7)
                .is_some_and(|scheme| scheme.eq_ignore_ascii_case("http://")),
            _ => false,
        }
    }

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

    /// An S3 location whose `region` is not a region name
    /// ([`is_valid_s3_region`]). With no endpoint, `object_store` builds the
    /// host as `s3.<region>.amazonaws.com`, so a region such as
    /// `x@attacker.example/` would move the request, and the credential's
    /// signature, to another host. Every runner refuses such a location before
    /// any client exists, and `logweir-store` refuses to build one.
    #[must_use]
    pub fn has_invalid_region(&self) -> bool {
        matches!(self, Self::S3 { region: Some(region), .. } if !is_valid_s3_region(region))
    }
}

/// The one S3 region rule, `^[a-z0-9-]{1,32}$` — destination rule `region`
/// (`crate::destination::validate`), the `BackupDestination` CRD's pattern,
/// and the runner's and the store's refusal of any other spelling
/// ([`StorageUrl::has_invalid_region`]). Lower-case letters, digits and `-`
/// only: nothing that can end a host label, start a port, a path or a
/// userinfo.
pub const S3_REGION_PATTERN: &str = "^[a-z0-9-]{1,32}$";

/// Whether `region` matches [`S3_REGION_PATTERN`].
#[must_use]
pub fn is_valid_s3_region(region: &str) -> bool {
    !region.is_empty()
        && region.len() <= 32
        && region
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupSetRef {
    pub backup_id: String,
    pub manifest_key: String,
}

/// One finding about an archive that an operator must be told and that NO
/// signed field records: what [`DataEngine::describe_with_notices`] returns
/// beside the facts, for the caller to PRINT (a warning line and a structured
/// log event) and never to act on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveNotice {
    /// A stable, machine-readable name for the kind of finding, and the value
    /// of the structured log's `notice` field:
    /// `consumer-groups-snapshot-unreadable` is the one kind today.
    pub kind: String,
    /// The object the finding is about, as the store names it.
    pub key: String,
    /// `sha256:<hex>` of that object's bytes.
    pub sha256: String,
    /// What was found and what it means for the run, in one phrase.
    pub message: String,
    /// The detail behind it, e.g. the parser's own error.
    pub reason: String,
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
    /// restoring consumer offsets is a non-goal.
    pub consumer_group_snapshot_sha256: Option<String>,
    pub topics: Vec<TopicFacts>,
}

impl BackupSetFacts {
    pub fn consumer_group_snapshot_present(&self) -> bool {
        self.consumer_group_snapshot_sha256.is_some()
    }

    /// **Guard G-WIN's floor.** The archive set's EARLIEST COVERED TIMESTAMP as
    /// the manifest records it, over the topics the caller NAMES: the minimum
    /// `start_timestamp` over `topics[].partitions[].segments[]` for every
    /// topic in `named_topics`. `None` when the set records no segment for any
    /// named topic; a caller that needs a floor must refuse instead
    /// (`logweir::drill::build_plan`).
    ///
    /// Over `named_topics` because it is the restore-side twin of the backup
    /// receipt's `covered.from_ms` (the minimum over the topics the backup
    /// named, interface I22), and an unnamed topic's older segment would drag
    /// the floor below every instant the restore can reach. Here, on the facts,
    /// because plan construction and phase 5's independent re-derivation must
    /// get the same answer from the same manifest. The MINIMUM over the whole
    /// walk: a floor from the newest segment would exclude every record before
    /// it, and nothing orders a real manifest.
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

/// Returns (lower, upper): the count the manifest can PROVE over [floor_ms,
/// pit_ms]. `lower` sums `record_count` for segments wholly inside the window;
/// `upper` adds every segment that straddles either bound. A restored count
/// outside [lower, upper] is an integrity failure; inside it, the sampled
/// reconciliation is the finer check. A negative `record_count` contributes 0.
///
/// **Both bounds are INCLUSIVE**, as the plan's window is (the engine filters
/// `r.timestamp >= s && r.timestamp <= e`,
/// [U:crates/kafka-backup-core/src/restore/helpers.rs:74-82]), so a segment
/// touching a bound exactly is inside it.
///
/// **A BOUND, not an equality**: the manifest's finest granularity is the
/// SEGMENT, so when `point_in_time` falls inside one the count at or before it
/// is not derivable. The boundary question belongs to G-PITR's payload set,
/// never to a count. A segment outside the window contributes to neither bound,
/// or a large archive would make `upper` unfalsifiable.
///
/// The caller restricts `facts` to the topics it will count
/// (`logweir::drill::phase7_verify::check_restored_count` passes only its
/// mapping's source topics), the rule `earliest_covered_timestamp_ms` applies
/// to the floor.
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

/// The count bound of ONE partition over `[floor_ms, pit_ms]`,
/// both ends inclusive: `(lower, upper)`, read from that partition's own
/// segments.
///
/// It is [`expected_restored_count`]'s reading applied to one partition, with
/// one addition that only RAISES `lower`: a segment the window cuts across
/// still PROVES one record in the window when its first or its last record's
/// timestamp lies inside it. `SegmentFacts::start_timestamp` is the timestamp
/// of the segment's FIRST record and `end_timestamp` that of its LAST
/// (`kafka-backup-core/src/segment/writer.rs:233-241` in the pinned 0.23.3:
/// set from the first `add_record`, overwritten by every later one), and the
/// engine restores every record whose own timestamp is inside the window
/// (`restore/helpers.rs:66-84`). So such a segment adds 1 to `lower` (it
/// cannot add 2: a segment with BOTH ends inside is wholly inside) and its
/// whole `record_count` to `upper`, exactly as before.
///
/// # Why per partition
/// A sum over every mapped topic has slack: a restore an engine stopped early
/// (a SIGTERM between topics, then exit 0) leaves whole topics empty, and the
/// sum can stay inside its bound. One bound per partition has no sibling to
/// borrow from, and the straddler's 1 makes an empty partition the manifest
/// proves non-empty in the window fall outside its own bound.
///
/// A segment that overlaps the window with neither end inside it (it starts
/// before `floor_ms` and ends after `pit_ms`) proves nothing: its records may
/// all lie outside the window. Under the archive-manifest floor no segment of
/// a named topic starts before the floor, so the case needs a plan whose floor
/// is not the manifest's.
pub fn partition_count_bound(p: &PartitionFacts, floor_ms: i64, pit_ms: i64) -> (u64, u64) {
    let inside = |ts: i64| ts >= floor_ms && ts <= pit_ms;
    let mut lower = 0u64;
    let mut straddling = 0u64;
    for seg in &p.segments {
        let n = seg.record_count.max(0) as u64;
        let wholly_inside = seg.start_timestamp >= floor_ms && seg.end_timestamp <= pit_ms;
        let overlaps = seg.end_timestamp >= floor_ms && seg.start_timestamp <= pit_ms;
        if wholly_inside {
            lower += n;
        } else if overlaps {
            straddling += n;
            if n > 0 && (inside(seg.start_timestamp) || inside(seg.end_timestamp)) {
                lower += 1;
                straddling -= 1;
            }
        }
    }
    (lower, lower + straddling)
}

/// What the engine's own offset report says it restored, read by
/// the engine adapter after a restore that exited 0 and handed to phase 7.
///
/// The pinned engine writes the report only from the `Ok` arm of a completed
/// restore (`restore/engine.rs:426-437` in 0.23.3), and an engine stopped by a
/// SIGTERM returns `Ok` too: it finishes the topic it is on, sees the signal at
/// the top of its per-topic loop and exits 0 (`:905-909`). Its report then
/// exists WITHOUT the topics it never started, because an entry is added only
/// for a segment that produced records (`:1735-1739`, `:1794-1811`). That
/// absence is the one trace of the early stop Logweir can read, and phase 7
/// refuses on it. The report is used ONLY as that negative signal: an entry
/// never vouches for a record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EngineReport {
    /// The report was read. Every `(target topic, partition)` it names an
    /// entry for — the engine keys its entries by the TARGET topic.
    Read(std::collections::BTreeSet<(String, i32)>),
    /// There is no report at the path the plan named: the engine did not
    /// write one (its write failure is a warning upstream), or this engine
    /// writes none at all (every test double). Phase 7 cannot check it, and
    /// says so in its log.
    Absent,
    /// A file exists and this build cannot read it. Phase 7 cannot check it
    /// either; the reason is logged.
    Unreadable(String),
}

/// Where `RestorePlan.time_window.0` came from. An enum, not a bool, so the
/// plan's claim about its own floor is a VALUE plan construction checks:
/// `logweir::drill::build_plan_with_floor` refuses, exit 3, an
/// `ArchiveManifest` plan whose `time_window.0` is not the manifest floor.
///
/// **Phase 5 does not read it**: `check_rendered_window_floor` compares the
/// rendered `time_window_start` INTEGER with a floor re-derived from the spec
/// and the manifest, never from this claim: a guard that read the claim could
/// be talked out of refusing by the plan it refuses. `InheritedFromSpec` is
/// what a plan says when its spec states a window start
/// (`restore.point_in_time: "<start>/<end>"`); the complete lane selects its
/// expected output from it (`complete::window_of`).
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

impl TopicFacts {
    /// The newest record timestamp the manifest RECORDS for this
    /// topic: the maximum, over every segment of every partition, of the
    /// segment's first AND last record timestamp. `None` when the manifest
    /// records no segment for the topic.
    ///
    /// Both ends, because a segment's bounds are its first and last records'
    /// timestamps and not its minimum and maximum: with out-of-order timestamps
    /// the first record can be the later one.
    ///
    /// A LOWER bound on the newest archived timestamp, never the value itself:
    /// a record inside a segment can be later than both ends, which only
    /// decoding the segment shows. `crate::time_basis` reads it both ways, and
    /// only one is certain: a window END below it certainly excludes an
    /// archived record, so that window is a time selection; a window end at or
    /// after it excludes nothing THE MANIFEST SHOWS and is treated as no
    /// selection, though the engine can still drop a later out-of-order record
    /// inside a segment. The scorecard format and `docs/stability.md` state
    /// that residue where the empty lists are described.
    #[must_use]
    pub fn newest_recorded_timestamp_ms(&self) -> Option<i64> {
        self.partitions
            .iter()
            .flat_map(|p| p.segments.iter())
            .map(|seg| seg.start_timestamp.max(seg.end_timestamp))
            .max()
    }
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
    /// Per-topic source partition subsets, keyed by SOURCE
    /// topic, each list sorted and unique (`restore.partitions` in the spec,
    /// validated by `crate::replay_selection::ReplaySelection`). A mapped topic
    /// not named here restores every partition. EMPTY for every plan that
    /// states no subset, which renders the one document it always rendered.
    ///
    /// The engine's own `restore.source_partitions` applies to every topic of
    /// a run, so an engine adapter that cannot filter per topic runs once per
    /// distinct subset (`crate::replay_selection::ReplaySelection::engine_runs`).
    pub source_partitions: BTreeMap<String, Vec<i32>>,
    pub default_replication_factor: i16,
    pub checkpoint_state: std::path::PathBuf,
    pub checkpoint_interval_secs: u64,
    /// Where the engine writes its offset-mapping report
    /// [U:crates/kafka-backup-core/src/config.rs:857-858]. A FIELD OF THE PLAN,
    /// like `checkpoint_state`, so the path is a function of the approved
    /// bytes; `--offset-report-out` sets it, else `offsets.json` in the per-run
    /// workdir.
    ///
    /// **Pod-local, so phase 8 uploads it** to
    /// `logweir/drills/<run_id>.offsets.json`, recorded with its digest in the
    /// signed scorecard's `evidence.offset_report_*`. Rendering the report
    /// applies nothing (Global Constraint 20): committing offsets is a later,
    /// separately approved operation.
    pub offset_report: std::path::PathBuf,
}

/// How a cluster's client is told to authenticate, as the renderer needs it:
/// the render-side twin of `crate::spec::AuthSpec` (`AuthSpec::to_render()` is
/// the one mapping, interface **I1**). Carried by `BackupPlan::source_auth` and
/// `RestorePlan::target_auth`.
///
/// **It never carries a password.** The secret reaches the engine through its
/// own `${VAR}` environment expansion, which `yaml_scalar` cannot defend
/// against, so it is never interpolated by us. `Plaintext` renders **nothing at
/// all**: the engine's `SecurityProtocol` default is `PLAINTEXT`
/// [U:crates/kafka-backup-core/src/config.rs:261-269].
///
/// `tls_ca_file` (rendered as `ssl_ca_location`) and the `Mtls`
/// client-certificate pair are POD-LOCAL PATHS to files projected from the
/// saved connection, attached by the runner after the plan is built
/// ([`AuthRender::with_tls_ca_file`], [`AuthRender::with_client_certificate`]):
/// never key material, and not in the plan hash, because a trust anchor rotates
/// while an approval binds an identity. A CA without TLS, and `Plain` without
/// TLS (`RenderError::PlainWithoutTls`), are refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthRender {
    Plaintext,
    ScramSha512 {
        username: String,
        tls: bool,
        tls_ca_file: Option<String>,
    },
    ScramSha256 {
        username: String,
        tls: bool,
        tls_ca_file: Option<String>,
    },
    Plain {
        username: String,
        tls: bool,
        tls_ca_file: Option<String>,
    },
    Mtls {
        tls: bool,
        tls_ca_file: Option<String>,
        client_certificate: Option<crate::connection::ClientCertificateFiles>,
    },
}

impl AuthRender {
    /// The mode as the documents spell it — `crate::spec::AuthSpec::mode_str`'s
    /// twin, so a refusal can name the mode it refused.
    pub fn mode_str(&self) -> &'static str {
        use crate::connection as c;
        match self {
            AuthRender::Plaintext => c::AUTH_MODE_PLAINTEXT,
            AuthRender::ScramSha512 { .. } => c::AUTH_MODE_SCRAM_SHA_512,
            AuthRender::ScramSha256 { .. } => c::AUTH_MODE_SCRAM_SHA_256,
            AuthRender::Plain { .. } => c::AUTH_MODE_PLAIN,
            AuthRender::Mtls { .. } => c::AUTH_MODE_MTLS,
        }
    }

    /// The SASL username, for the arms that have one.
    pub fn username(&self) -> Option<&str> {
        match self {
            AuthRender::Plaintext | AuthRender::Mtls { .. } => None,
            AuthRender::ScramSha512 { username, .. }
            | AuthRender::ScramSha256 { username, .. }
            | AuthRender::Plain { username, .. } => Some(username),
        }
    }

    /// Attach the projected CA file, or refuse because the transport is not
    /// TLS. `None` returns `self` unchanged, so a connection with no private CA
    /// renders byte-identically to one built before the field existed.
    ///
    /// # Errors
    ///
    /// [`crate::connection::TlsCaWithoutTls`] when `ca_file` is `Some` and the
    /// transport is not TLS (`Plaintext`, or any arm with `tls: false`).
    pub fn with_tls_ca_file(
        self,
        ca_file: Option<String>,
    ) -> Result<AuthRender, crate::connection::TlsCaWithoutTls> {
        let Some(ca_file) = ca_file else {
            return Ok(self);
        };
        let refuse = |mode| Err(crate::connection::TlsCaWithoutTls { mode });
        match self {
            AuthRender::ScramSha512 { username, tls, .. } if tls => Ok(AuthRender::ScramSha512 {
                username,
                tls,
                tls_ca_file: Some(ca_file),
            }),
            AuthRender::ScramSha256 { username, tls, .. } if tls => Ok(AuthRender::ScramSha256 {
                username,
                tls,
                tls_ca_file: Some(ca_file),
            }),
            AuthRender::Plain { username, tls, .. } if tls => Ok(AuthRender::Plain {
                username,
                tls,
                tls_ca_file: Some(ca_file),
            }),
            AuthRender::Mtls {
                tls,
                client_certificate,
                ..
            } if tls => Ok(AuthRender::Mtls {
                tls,
                tls_ca_file: Some(ca_file),
                client_certificate,
            }),
            other => refuse(other.mode_str()),
        }
    }

    /// Attach the projected client-certificate pair of an `mtls` connection.
    ///
    /// `None` returns `self` unchanged for every mode but `Mtls`; for `Mtls` it
    /// is [`crate::connection::ClientCertificateRefusal::Missing`], because an
    /// mTLS dial with no certificate has no identity to present (the renderer
    /// refuses the same shape if a plan's fields were set directly).
    ///
    /// # Errors
    ///
    /// [`crate::connection::ClientCertificateRefusal`]: files for a mode that
    /// presents none (`NotMtls`), or none for `mtls` (`Missing`).
    pub fn with_client_certificate(
        self,
        files: Option<crate::connection::ClientCertificateFiles>,
    ) -> Result<AuthRender, crate::connection::ClientCertificateRefusal> {
        use crate::connection::ClientCertificateRefusal;
        match (self, files) {
            (
                AuthRender::Mtls {
                    tls, tls_ca_file, ..
                },
                Some(files),
            ) => Ok(AuthRender::Mtls {
                tls,
                tls_ca_file,
                client_certificate: Some(files),
            }),
            (AuthRender::Mtls { .. }, None) => Err(ClientCertificateRefusal::Missing),
            (other, Some(_)) => Err(ClientCertificateRefusal::NotMtls {
                mode: other.mode_str(),
            }),
            (other, None) => Ok(other),
        }
    }
}

/// Everything `render_backup::render` needs to emit a `backup.yaml`, and
/// nothing else.
///
/// No `source_cluster_id` field: phase 0 reads the cluster id FROM THE BROKER,
/// never from a spec, and an adopter-supplied string must not stand where a
/// measured fact belongs.
///
/// The renderer enforces the other rails: `topics` is a named allowlist with no
/// glob metacharacter (**G-GLOB**, `crate::guard::reject_glob_metacharacters`);
/// no field can produce `reset_consumer_offsets`, `auto_consumer_groups`,
/// `create_topics` or any consumer-group key, so the document is read-only by
/// construction; and `purge_topics`/`dry_run` are unrepresentable and
/// re-scanned out of the rendered bytes by `render_and_digest`.
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
    /// The per-run readback of phase 5's header preflight.
    pub header_preflight_honoured: bool,
    /// Paths the engine logged as `Ignoring unknown config key <path>`.
    pub unknown_key_warnings: Vec<String>,
    /// `sha256:<hex>` over the exact bytes written as `restore.yaml` at phase
    /// 5; for a plan restored by several engine runs (one per distinct
    /// partition subset), every run's digest in run order, joined by `,`. Phase
    /// 6 re-renders, re-hashes and refuses on divergence: exit 1, operational,
    /// no artifact (by phase 6 the guards have run and `validate-restore` has
    /// executed).
    pub rendered_restore_sha256: String,
}

/// Logweir-measured, never engine-reported: `restore` has no --format
/// [VERIFIED U/kafka-backup/crates/kafka-backup-cli/src/main.rs:47-51]. The one
/// engine artifact carried here, `engine_report`, is its offset-mapping file,
/// read only for what it LACKS.
#[derive(Debug, Clone)]
pub struct RestoreFacts {
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    pub exit_code: i32,
    pub unknown_key_warnings: Vec<String>,
    /// The engine's offset report as the adapter read it after the
    /// restore exited 0 ([`EngineReport`]). `Absent` from an engine that
    /// writes none.
    pub engine_report: EngineReport,
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
    /// healthy restore fails a drill that actually succeeded.
    pub set: BackupSetRef,
    pub topic: String,
    pub partition: i32,
    /// A closed set: see `crate::spec::Anchor` for why, and for why v0.1
    /// refuses everything but `Head` at phase 0.
    pub anchor: crate::spec::Anchor,
    /// A CAP on how many fingerprints `fingerprints()` returns, not a promise
    /// of exactly this many: fewer than `count` matching records in the
    /// window is not an error. What this bounds differs BY ANCHOR:
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
    /// integrity.level "consume-only" rather than failing.
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
    /// re-hashes and REFUSES on divergence — so `restore` needs that
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

    /// PRECONDITION: none. Runs the engine's `backup` subcommand over the
    /// rendered document.
    ///
    /// The default body returns `EngineError::Operational`, so a test double
    /// need not implement it.
    ///
    /// **Never a fabricated `Ok(BackupFacts { exit_code: 0, .. })`.** Phase
    /// −1's caller reads this result and then reads the ARCHIVE the run was
    /// supposed to produce; a default that claimed a clean exit would put a
    /// `records_per_topic` and a covered window from somebody else's archive
    /// behind a backup that never ran.
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

    /// `describe`, plus what the engine found about the
    /// set that no signed field carries and an operator must still be told.
    /// Today that is one thing: the set's consumer-groups snapshot is present
    /// but not in the shape the engine writes. A drill restores data and a
    /// receipt records data, so such a run proceeds and signs nothing about
    /// the snapshot; the notice is how the operator learns that the object
    /// beside the manifest cannot be read. `drill run` and `backup run` print
    /// every notice (`logweir::drill::surface_archive_notices`).
    ///
    /// The default is `describe` and no notice.
    /// `default_describe_with_notices_is_describe_with_no_notice`
    /// (`logweir-core/tests/engine_trait.rs`) pins it.
    fn describe_with_notices(
        &self,
        set: &BackupSetRef,
    ) -> Result<(BackupSetFacts, Vec<ArchiveNotice>), EngineError> {
        Ok((self.describe(set)?, Vec::new()))
    }
}
