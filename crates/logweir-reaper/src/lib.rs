#![forbid(unsafe_code)]
//! The one sanctioned deleter — decision D3 §6.5, ADR 0008 Amendment H.
//!
//! # What this crate is for
//!
//! Tag 1's statement was *no Logweir component holds any delete capability
//! against object storage*. Amendment H makes it version-scoped: it is true
//! wherever a `RetentionPolicy` is not in `mode: Enforce`, and where it is,
//! **this crate is the only place in the workspace that names an object-store
//! delete**. `weirkeeper`, `logweir-store`, `logweir` and `logweir-api` do not
//! link it; `scripts/check-no-archive-write.sh` check 3 proves the reaching set
//! over `cargo metadata` is exactly `{logweir-retention}`, and adding an edge
//! from any of the four is the mutant that gate exists to kill.
//!
//! # The shape, and why the deleting is behind a trait
//!
//! [`execute`] is the whole state machine — dry run, scope validation, per-point
//! ordering, bounded retry, caps, attribution — and it takes a [`Deleter`]. The
//! real one is [`ArchiveReaper`], which holds a credential-scoped
//! `object_store` handle; the tests drive the same state machine over a
//! recording fake, which is what makes "a dry run performs no delete", "an
//! `AccessDenied` is not retried" and "a key outside the scope exits 3 with
//! zero deletes" properties rather than hopes.
//!
//! # The order per point, and why it is this order
//!
//! Delete the **manifest first**, then the segments. A set whose manifest is
//! gone cannot be read as usable by anything — the catalog reports `Missing`
//! rather than `Partial` — so a run interrupted between the two leaves an
//! unmistakably dead set rather than a plausible-looking one. The leftover
//! segment keys are exactly what the next plan names, which is what makes
//! completion idempotent.
//!
//! # What this crate never touches
//!
//! Anything under `logweir/`. Receipts, sidecars, scorecards, catalog records,
//! tombstones and the enforcement records themselves live there, so the audit
//! trail of a deleted point outlives the point. The rule is enforced three
//! times over: by the CRD's K4 admission rule, by the plan writer
//! (`weirkeeper::retention_plan::validate_key`), and here, by
//! [`validate_plan`], before the first delete. It is deliberately not one
//! implementation shared between the writer and the worker — the worker's job
//! is to refuse a plan it was HANDED, and a validator it imported from the
//! writer would agree with the writer by construction.

use std::collections::BTreeSet;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub mod archive;

pub use archive::{ArchiveReaper, Credentials};

// ---------------------------------------------------------------------------
// The constants the plan and the record are written against
// ---------------------------------------------------------------------------

/// The plan document's media type — it must equal this exactly.
pub const PLAN_MEDIA_TYPE: &str = "application/vnd.logweir.retention-plan+json;version=1.0.0";

/// The enforcement record's media type.
pub const RECORD_MEDIA_TYPE: &str = "application/vnd.logweir.retention-record+json;version=1.0.0";

/// The record's `format_version`.
pub const RECORD_FORMAT_VERSION: &str = "1.0.0";

/// The evidence root nothing here may delete under.
pub const EVIDENCE_ROOT: &str = "logweir/";

/// The per-key attempt ceiling — D3 §6.5's bounded retry.
pub const MAX_ATTEMPTS: u32 = 3;

/// The waits BETWEEN attempts: 1 s, then 4 s.
///
/// **Two waits, because three attempts have two gaps** (review `d3w9` L1).
/// D3 §6.5 writes the backoff as "1 s/4 s/16 s", which reads as three numbers;
/// with [`MAX_ATTEMPTS`] at 3 the third would be a wait after the last attempt,
/// i.e. sixteen seconds of holding a Job open to learn nothing. The consistent
/// reading of §6.5 is the two gaps, and this is them.
pub const BACKOFF_SECONDS: [u64; 2] = [1, 4];

// ---------------------------------------------------------------------------
// The plan, as the worker reads it
// ---------------------------------------------------------------------------

/// One line of the plan — everything deleting one point means.
///
/// `deny_unknown_fields`: a plan written by a NEWER controller carrying a field
/// this build does not understand is refused, not partially obeyed. A deletion
/// worker that silently ignored half of an instruction is the failure mode the
/// whole two-step approval exists to prevent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct PlanLine {
    /// The point id. Every delete is attributable to exactly one of these.
    pub point_id: String,
    /// The archive set id, which bounds every key on this line.
    pub backup_id: String,
    /// Why the evaluation chose it.
    pub reason: String,
    /// Its capture start, epoch milliseconds.
    pub recovery_point_at_ms: i64,
    /// The manifest key — deleted first.
    pub manifest_key: String,
    /// The set's own key prefix, `<scope_prefix>/<backup_id>/`. It is the
    /// bound every key on this line must satisfy, and — when
    /// [`PlanLine::enumerate_set`] is set — the directory the worker lists.
    pub set_prefix: String,
    /// Whether the worker must enumerate the set directory for the keys the
    /// plan could not name.
    ///
    /// # Why this exists, and what it costs
    ///
    /// D3 §6.4 step 6 asks the plan to carry "the complete, explicit list of
    /// object keys to delete". The catalog view the evaluation reads carries a
    /// point's `manifestKey` and **not** its segment keys — D3 W8's contract
    /// says so in terms ("`topics` is deliberately ABSENT … the window is
    /// bounded by bytes") and the same bound excludes a segment list — and the
    /// controller holds no archive credential for the destination with which to
    /// go and look. So a plan written today names the manifest and the set
    /// prefix, and the worker enumerates the rest.
    ///
    /// That is exactly the second half of D3 §6.5's wrong-prefix rule, which
    /// requires every key to "have been listed from that point's own manifest
    /// or **set directory**". What an administrator approves is therefore the
    /// exact point set, the exact manifest keys and the exact key BOUND — not
    /// a per-key list — and [`validate_listed_key`] re-checks every enumerated
    /// key against that bound before it is deleted. When the view grows a
    /// segment-key field this flag goes to `false` and nothing else changes.
    #[serde(default)]
    pub enumerate_set: bool,
    /// Every key the plan could name, manifest first. Explicit: never a glob.
    pub object_keys: Vec<String>,
    /// Other points whose receipts name this SAME set and are removed with it
    /// (review M2). Two receipts over one set that are both due used to render
    /// two lines with one manifest key, which [`validate_plan`] refuses as
    /// `DuplicateKey` — the whole plan, on every run. One line per set, with
    /// every point it removes named here, keeps each delete attributable to one
    /// line and each point attributable in the record (one outcome and one
    /// pair of tombstones per point). Absent — and so byte-identical to a
    /// plan from before this field — when the set has one point.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub co_point_ids: Vec<String>,
}

/// The plan document, as the worker reads it.
///
/// **Field for field with `weirkeeper::retention_plan::PlanDocument`**, and
/// both carry `deny_unknown_fields`, so drift between them is total rather than
/// partial: one added field on the writer would make every plan
/// `Refusal::Unreadable` and every run exit 3, discovered at 04:17. The two
/// types cannot be one type — the crates must not link each other, which is the
/// whole point of `scripts/check-no-archive-write.sh` check 3 — so the
/// agreement is asserted instead, by
/// `crates/logweir/tests/retention_plan_wire.rs`, which is in a crate that may
/// read both files (review `d3w9` L6).
///
/// **It carries no instant and no generation.** See `PlanDocument`'s own note:
/// a digest that moves on its own can never be approved (review `d3w9` C1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct Plan {
    /// The media type.
    pub format: String,
    /// The document version.
    pub format_version: String,
    /// The policy's namespace.
    pub policy_namespace: String,
    /// The policy's name.
    pub policy_name: String,
    /// The policy's UID.
    pub policy_uid: String,
    /// The destination's location id.
    pub location_id: String,
    /// The immutable scope prefix. Every key is re-checked against it here.
    pub scope_prefix: String,
    /// `keepLast`, as applied.
    pub keep_last: Option<i64>,
    /// `keepDays`, as applied.
    pub keep_days: Option<i64>,
    /// `minUsablePoints`, as applied.
    pub min_usable_points: i64,
    /// The lines.
    pub lines: Vec<PlanLine>,
}

impl Plan {
    /// Every object key this plan names, in execution order.
    #[must_use]
    pub fn object_keys(&self) -> Vec<&str> {
        self.lines
            .iter()
            .flat_map(|l| l.object_keys.iter().map(String::as_str))
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Refusals — everything that happens BEFORE the first delete
// ---------------------------------------------------------------------------

/// Why a plan was refused. **Nothing was deleted when one of these is
/// returned**, which is what makes them exit 3 rather than exit 1.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Refusal {
    /// The bytes are not a plan document.
    #[error("the plan document did not parse: {0}")]
    Unreadable(String),
    /// `format` is not [`PLAN_MEDIA_TYPE`].
    #[error(
        "the document declares format `{0}`, and this worker executes only `{PLAN_MEDIA_TYPE}`"
    )]
    WrongFormat(String),
    /// The plan's digest is not the digest that was approved.
    #[error(
        "the plan on disk digests to {found}, and the approved digest is {approved}: an \
         administrator approved different bytes, so nothing is deleted"
    )]
    DigestMismatch {
        /// What the bytes actually digest to.
        found: String,
        /// What the Job was told to execute.
        approved: String,
    },
    /// The plan was written for another policy or another generation.
    #[error("the plan names {found} and this run is for {expected}; nothing is deleted")]
    PolicyMismatch {
        /// What the plan says.
        found: String,
        /// What the run was configured for.
        expected: String,
    },
    /// A key escaped `<scope_prefix>/<backup_id>/`.
    #[error(
        "RetentionScopeViolation: key `{key}` on point `{point_id}` is not under `{expected}`; \
         zero objects were deleted"
    )]
    ScopeViolation {
        /// The offending key.
        key: String,
        /// The point that named it.
        point_id: String,
        /// The prefix it had to start with.
        expected: String,
    },
    /// A key named the evidence root.
    #[error(
        "RetentionScopeViolation: key `{key}` on point `{point_id}` is under `{EVIDENCE_ROOT}`, \
         the evidence root no retention run may delete under; zero objects were deleted"
    )]
    EvidenceRoot {
        /// The offending key.
        key: String,
        /// The point that named it.
        point_id: String,
    },
    /// The plan's first key is not the manifest key its line declares.
    #[error(
        "point `{point_id}` lists `{first}` first and declares its manifest to be \
         `{manifest_key}`; the manifest is deleted first by construction, so a plan whose order \
         disagrees with its own declaration is refused"
    )]
    ManifestNotFirst {
        /// The point.
        point_id: String,
        /// What the line's key list starts with.
        first: String,
        /// What the line says the manifest is.
        manifest_key: String,
    },
    /// The plan would exceed a configured ceiling.
    #[error("the plan names {found} {what}, and this run's ceiling is {cap}; nothing is deleted")]
    OverCap {
        /// What was counted.
        what: &'static str,
        /// How many the plan names.
        found: i64,
        /// The ceiling.
        cap: i64,
    },
    /// A line names no key at all.
    #[error("point `{0}` names no object key, so it describes no deletion")]
    EmptyLine(String),
    /// A point is named twice — on two lines, or as a line's own point and a
    /// co-point. Its removal would then be recorded twice.
    #[error("point `{0}` is named more than once in the plan; each point is removed by one line")]
    DuplicatePoint(String),
    /// A key appears on two lines: one delete could then be attributed to two
    /// points, and D3 §6.5 requires every delete to be attributable to exactly
    /// one plan line.
    #[error(
        "key `{key}` appears on point `{first}` and on point `{second}`; a delete must be \
         attributable to exactly one plan line"
    )]
    DuplicateKey {
        /// The key.
        key: String,
        /// The first point naming it.
        first: String,
        /// The second.
        second: String,
    },
}

/// Parse and validate a plan document against the digest an administrator
/// approved and the identity this run was configured for.
///
/// **Every check here runs before a single delete is issued**, and the error
/// type says so: a [`Refusal`] is the exit-3 arm, and the caller has deleted
/// nothing when it sees one.
///
/// # Errors
///
/// [`Refusal`] — see its variants.
pub fn parse_plan(bytes: &[u8], approved_sha256: &str) -> Result<Plan, Refusal> {
    // THE DIGEST FIRST, over the exact bytes on disk, before the JSON is even
    // looked at. A plan whose bytes are not the approved bytes is not this
    // administrator's plan, whatever it happens to contain.
    let found = logweir_core::ids::sha256_prefixed(bytes);
    if found != approved_sha256 {
        return Err(Refusal::DigestMismatch {
            found,
            approved: approved_sha256.to_string(),
        });
    }
    let plan: Plan =
        serde_json::from_slice(bytes).map_err(|e| Refusal::Unreadable(e.to_string()))?;
    if plan.format != PLAN_MEDIA_TYPE {
        return Err(Refusal::WrongFormat(plan.format));
    }
    Ok(plan)
}

/// What a run was configured to execute.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunBinding {
    /// The policy UID the Job was created for.
    pub policy_uid: String,
    /// `spec.scope.prefix`, as the Job was told it, INDEPENDENTLY of the plan.
    ///
    /// A worker that took the scope from the plan alone would accept a plan
    /// that widened its own scope. The Job carries the prefix in its
    /// environment, the plan carries it in its bytes, and both must agree.
    pub scope_prefix: String,
    /// The per-run point ceiling.
    pub max_deletions_per_run: i64,
    /// The per-run object-key ceiling.
    pub max_objects_per_run: i64,
}

/// Every structural and scope check, in order, over a parsed plan.
///
/// # Errors
///
/// [`Refusal`] — nothing has been deleted when this returns one.
pub fn validate_plan(plan: &Plan, binding: &RunBinding) -> Result<(), Refusal> {
    // THE UID AND NOT A GENERATION. The generation is not in the plan bytes
    // (review `d3w9` C1) and could not be compared here if it were wanted: what
    // binds this plan to this run is the policy's identity, its scope and the
    // approved digest `parse_plan` already checked over the exact bytes.
    if plan.policy_uid != binding.policy_uid {
        return Err(Refusal::PolicyMismatch {
            found: plan.policy_uid.clone(),
            expected: binding.policy_uid.clone(),
        });
    }
    // THE SCOPE IS THE JOB'S, NOT THE PLAN'S. Compared after trimming a
    // trailing slash on both sides so `a/b` and `a/b/` are one prefix.
    let job_scope = binding.scope_prefix.trim_end_matches('/');
    if plan.scope_prefix.trim_end_matches('/') != job_scope {
        return Err(Refusal::PolicyMismatch {
            found: format!("scope `{}`", plan.scope_prefix),
            expected: format!("scope `{}`", binding.scope_prefix),
        });
    }
    // EVERY POINT, co-points included: `maxDeletionsPerRun` bounds points,
    // and a line that removes three receipts' set removes three points.
    let points = plan
        .lines
        .iter()
        .map(|l| 1 + l.co_point_ids.len())
        .fold(0usize, usize::saturating_add);
    let points = i64::try_from(points).unwrap_or(i64::MAX);
    let mut named_points: BTreeSet<&str> = BTreeSet::new();
    for line in &plan.lines {
        for point in line_points(line) {
            if !named_points.insert(point) {
                return Err(Refusal::DuplicatePoint(point.to_string()));
            }
        }
    }
    if points > binding.max_deletions_per_run {
        return Err(Refusal::OverCap {
            what: "points",
            found: points,
            cap: binding.max_deletions_per_run,
        });
    }
    let mut objects = 0i64;
    let mut seen: BTreeSet<&str> = BTreeSet::new();
    let mut owner: std::collections::BTreeMap<&str, &str> = std::collections::BTreeMap::new();
    for line in &plan.lines {
        let Some(first) = line.object_keys.first() else {
            return Err(Refusal::EmptyLine(line.point_id.clone()));
        };
        if first != &line.manifest_key {
            return Err(Refusal::ManifestNotFirst {
                point_id: line.point_id.clone(),
                first: first.clone(),
                manifest_key: line.manifest_key.clone(),
            });
        }
        // THE SET PREFIX IS RE-DERIVED FROM THE JOB'S SCOPE, never taken from
        // the plan: a line that carried a wider `set_prefix` than its own
        // `backup_id` justifies would otherwise widen the enumeration bound.
        let expected_set = format!("{job_scope}/{}/", line.backup_id);
        if line.set_prefix != expected_set {
            return Err(Refusal::ScopeViolation {
                key: line.set_prefix.clone(),
                point_id: line.point_id.clone(),
                expected: expected_set,
            });
        }
        for key in &line.object_keys {
            objects = objects.saturating_add(1);
            // THE EVIDENCE ROOT IS CHECKED FIRST AND ON ITS OWN, not derived
            // from the prefix comparison: a scope prefix that itself began
            // `logweir/` would otherwise let every key under it through.
            if key.starts_with(EVIDENCE_ROOT) {
                return Err(Refusal::EvidenceRoot {
                    key: key.clone(),
                    point_id: line.point_id.clone(),
                });
            }
            if !key.starts_with(&expected_set) {
                return Err(Refusal::ScopeViolation {
                    key: key.clone(),
                    point_id: line.point_id.clone(),
                    expected: expected_set.clone(),
                });
            }
            if !seen.insert(key.as_str()) {
                return Err(Refusal::DuplicateKey {
                    key: key.clone(),
                    first: owner
                        .get(key.as_str())
                        .copied()
                        .unwrap_or("<unknown>")
                        .to_string(),
                    second: line.point_id.clone(),
                });
            }
            owner.insert(key.as_str(), line.point_id.as_str());
        }
    }
    if objects > binding.max_objects_per_run {
        return Err(Refusal::OverCap {
            what: "object keys",
            found: objects,
            cap: binding.max_objects_per_run,
        });
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// The delete seam
// ---------------------------------------------------------------------------

/// How a delete failed, in a CLOSED vocabulary.
///
/// Never a raw object-store error body: those carry bucket names, request ids
/// and occasionally a principal ARN, and this value reaches a `RetentionPolicy`
/// status that anyone with read on the namespace can see.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteError {
    /// The principal may not delete this key. **Not retried** — a second
    /// attempt at a policy decision is three seconds of nothing.
    AccessDenied,
    /// A WORM lock or legal hold refused it. **Not retried**, and the point is
    /// recorded as `LegalHold` and excluded from the next plan until the
    /// reason clears.
    Locked,
    /// A conditional precondition failed. **Not retried.**
    PreconditionFailed,
    /// It was already gone. **Not an error**: retention is idempotent, and a
    /// key a previous interrupted run removed is a key this run wanted removed.
    NotFound,
    /// A 5xx. **Retried**, up to [`MAX_ATTEMPTS`].
    ServerError,
    /// A timeout or a transport failure. **Retried.**
    Timeout,
    /// The run reached its own `maxObjectsPerRun` ceiling with keys still to
    /// go. **Not a failure of anything** (review `d3w9` M2): it is the bound
    /// working, the leftovers are named, and the next run completes them.
    ///
    /// It has its own code so that
    /// `status.lastEnforcement.failed[].code` can be read as "this needs
    /// attention" everywhere else, and so the controller can decline to count
    /// it toward `consecutiveRunFailures` — three bounded runs on a large
    /// archive used to set `EnforcementDegraded` and stop scheduling for good.
    BudgetExhausted,
    /// The key's current version carries a provider version id: the bucket
    /// is versioned (every S3 Object Lock bucket is), so a DELETE by key would
    /// write a DELETE MARKER over the object and remove nothing — and a legal
    /// hold or retention period on the version is never consulted, because no
    /// version is being deleted. **Not retried, and nothing is deleted** (defect
    /// OBJECT-LOCK-DELETE-MARKER): recording `Deleted` for a key that only got a
    /// marker is the false record this code exists to prevent. See
    /// [`Deleter::probe_versioning`].
    VersionedBucket,
    /// The HEAD that establishes whether a delete would land as a marker was
    /// refused (typically `AccessDenied`: the credential lacks `s3:GetObject`
    /// on `<prefix>/*`, which D3 §6.5's documented scope includes). **Not
    /// retried, and nothing is deleted**: "could not tell" never authorises a
    /// delete.
    VersionProbeRefused,
    /// Anything else. **Not retried**: an unclassified failure repeated three
    /// times is still unclassified, and the run should stop and be looked at.
    Unclassified,
}

/// What a HEAD of one key says about how a delete of it BY KEY would land —
/// ONE of the two signals the worker reads, and not sufficient alone.
///
/// `object_store` 0.14 has no delete-by-version call and discards the DELETE
/// response's `x-amz-delete-marker` header, so the worker can neither delete a
/// specific version (where the provider would refuse a held one) nor tell,
/// after the fact, that it wrote a marker. What it CAN read is
/// `ObjectMeta::version`, which the S3 client fills from `x-amz-version-id` on
/// a HEAD. Measured on the lab MinIO (`claude/artifacts/ctl-batch-2/
/// minio-version-header-probe.txt`): present on a versioned bucket, a
/// `--with-lock` bucket, and an object written while versioning was enabled
/// in a now-suspended bucket; absent on a plain bucket and on an object written
/// while versioning was suspended.
///
/// **`Versioned` proves a delete by key would be a marker. `Unversioned` does
/// NOT prove it would remove the object** (review H1, measured on MinIO): an
/// object stored BEFORE versioning was enabled is a null version, its HEAD
/// carries no version id, and under Enabled versioning a delete by key still
/// writes a marker over it. That case is caught by the bucket-level signal —
/// the version id the provider returns for the run's own intent tombstone,
/// written into the same bucket ([`TombstoneSink::put_create_only`]). The HEAD
/// remains for what that signal cannot see: a key stored under Enabled
/// versioning in a bucket since Suspended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Versioning {
    /// No provider version id on this key's current version. With no version
    /// id on the run's intent tombstone either, a delete by key removes it.
    Unversioned,
    /// A provider version id: a delete by key would only hide the object.
    Versioned,
}

impl DeleteError {
    /// The wire spelling — what reaches `status.lastEnforcement.failed[].code`.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AccessDenied => "AccessDenied",
            Self::Locked => "Locked",
            Self::PreconditionFailed => "PreconditionFailed",
            Self::NotFound => "NotFound",
            Self::ServerError => "ServerError",
            Self::Timeout => "Timeout",
            Self::BudgetExhausted => "BudgetExhausted",
            Self::VersionedBucket => "VersionedBucket",
            Self::VersionProbeRefused => "VersionProbeRefused",
            Self::Unclassified => "Unclassified",
        }
    }

    /// Whether another attempt could plausibly succeed — D3 §6.5's "only on
    /// 5xx/timeouts".
    #[must_use]
    pub fn retryable(self) -> bool {
        matches!(self, Self::ServerError | Self::Timeout)
    }

    /// Whether this refusal means the point is HELD rather than merely failed.
    ///
    /// A `Locked` is a provider-authoritative legal hold. `object_store` 0.14
    /// exposes no WORM readback, so "legal hold respected" means exactly "a
    /// provider refusal is authoritative and recorded" — never "Logweir knows
    /// the hold exists" (D3 §16).
    #[must_use]
    pub fn is_hold(self) -> bool {
        matches!(self, Self::Locked)
    }
}

/// A key that was listed rather than named by the plan, re-checked.
///
/// Called for every enumerated key, immediately before it is deleted. It is a
/// SECOND implementation of the same rule [`validate_plan`] applies to the
/// named keys, deliberately: a listing comes from the bucket, not from the
/// approved document, and a bucket is exactly the thing this worker is not
/// allowed to trust.
///
/// # Errors
///
/// [`Refusal::EvidenceRoot`] or [`Refusal::ScopeViolation`].
pub fn validate_listed_key(key: &str, line: &PlanLine) -> Result<(), Refusal> {
    if key.starts_with(EVIDENCE_ROOT) {
        return Err(Refusal::EvidenceRoot {
            key: key.to_string(),
            point_id: line.point_id.clone(),
        });
    }
    if !key.starts_with(&line.set_prefix) {
        return Err(Refusal::ScopeViolation {
            key: key.to_string(),
            point_id: line.point_id.clone(),
            expected: line.set_prefix.clone(),
        });
    }
    Ok(())
}

/// Enumerating one set's directory.
///
/// Separate from [`Deleter`] so that a worker configured for a plan that names
/// every key needs no list permission at all, and so a test can drive the
/// enumeration path without a bucket.
pub trait Lister {
    /// Every key under this prefix, in any order.
    ///
    /// # Errors
    ///
    /// [`DeleteError`], classified the same way a delete failure is: a listing
    /// that was denied and a listing that timed out are the same two facts.
    fn list_exact(&self, prefix: &str) -> Result<Vec<String>, DeleteError>;
}

/// A lister that refuses — for a plan that names every key, and for a dry run.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoListing;

impl Lister for NoListing {
    fn list_exact(&self, _prefix: &str) -> Result<Vec<String>, DeleteError> {
        Err(DeleteError::Unclassified)
    }
}

/// The delete seam.
///
/// One method, one key, synchronous. A batch method would make "every delete is
/// attributable to one plan line" a property of the batching rather than of the
/// loop.
pub trait Deleter {
    /// Remove exactly this key. Idempotent: a key that is already gone is
    /// [`DeleteError::NotFound`], which [`execute`] treats as done.
    ///
    /// # Errors
    ///
    /// [`DeleteError`], classified.
    fn delete_exact(&self, key: &str) -> Result<(), DeleteError>;

    /// Whether a delete of this key BY KEY would remove it, or only write a
    /// delete marker over it — asked before EVERY delete [`execute`] issues.
    ///
    /// **Required, with no default**: a real deleter that forgot to answer
    /// must not be read as "unversioned", which is the answer that deletes.
    ///
    /// # Errors
    ///
    /// [`DeleteError`], classified; [`DeleteError::NotFound`] means the key is
    /// already gone, and no delete is issued for it.
    fn probe_versioning(&self, key: &str) -> Result<Versioning, DeleteError>;
}

/// How the executor waits between attempts. Injected so a bounded-retry test
/// does not take twenty-one seconds.
pub trait Sleeper {
    /// Wait.
    fn sleep(&self, d: Duration);
}

/// Why a tombstone could not be written.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct SinkError(pub String);

/// Where the per-point tombstones go — D3 §6.5's execution order.
///
/// **Create-only, under `logweir/`, which this run's delete credential cannot
/// reach.** The intent is written BEFORE the manifest delete and the completion
/// AFTER the segments, so the audit trail of a removal exists before the
/// removal does and survives it. A sink that refuses the intent stops that
/// point: an unattributable delete is not performed.
pub trait TombstoneSink {
    /// Write these bytes at this key, refusing if something is already there,
    /// and answer the provider's VERSION ID for the object written, if it gave
    /// one (`PutResult::version`, from `x-amz-version-id`).
    ///
    /// **The version id is load-bearing** (review H1): the sink writes into
    /// the same bucket the run deletes from, and a provider returns one exactly
    /// when versioning is Enabled on that bucket now — in which case a delete
    /// by key would only write a marker, whatever a key's own HEAD says.
    /// [`execute`] refuses the line `VersionedBucket` when the intent comes
    /// back versioned. A sink that cannot know answers `None`.
    ///
    /// # Errors
    ///
    /// [`SinkError`], carrying a message that names no credential.
    fn put_create_only(&self, key: &str, bytes: &[u8]) -> Result<Option<String>, SinkError>;
}

/// A sink that accepts nothing — for a dry run, where no tombstone is written
/// because nothing is removed.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoTombstones;

impl TombstoneSink for NoTombstones {
    fn put_create_only(&self, key: &str, _bytes: &[u8]) -> Result<Option<String>, SinkError> {
        Err(SinkError(format!(
            "this run writes no tombstone, so `{key}` was not written; a run that deletes must \
             be given a sink"
        )))
    }
}

/// One point's tombstone, at `intent` and at `completion`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct Tombstone {
    /// The media type.
    pub format: String,
    /// `intent` or `completion`.
    pub stage: String,
    /// The run that wrote it.
    pub run_id: String,
    /// The policy UID.
    pub policy_uid: String,
    /// The plan digest that authorised it.
    pub plan_sha256: String,
    /// The point.
    pub point_id: String,
    /// The archive set.
    pub backup_id: String,
    /// Every key the plan named for this point.
    pub object_keys: Vec<String>,
    /// How many were removed. `0` on an intent.
    pub objects_deleted: i64,
    /// What remains. The whole list on an intent.
    pub remaining_keys: Vec<String>,
    /// The state at this stage.
    pub state: String,
    /// The closed code that stopped it, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
}

/// The tombstone media type.
pub const TOMBSTONE_MEDIA_TYPE: &str =
    "application/vnd.logweir.retention-tombstone+json;version=1.0.0";

/// Where one point's tombstone goes.
#[must_use]
pub fn tombstone_key(policy_uid: &str, run_id: &str, point_id: &str, stage: &str) -> String {
    format!("{EVIDENCE_ROOT}retention/{policy_uid}/{run_id}/{point_id}.{stage}.json")
}

/// The real one.
#[derive(Debug, Clone, Copy, Default)]
pub struct ThreadSleeper;

impl Sleeper for ThreadSleeper {
    fn sleep(&self, d: Duration) {
        std::thread::sleep(d);
    }
}

// ---------------------------------------------------------------------------
// The outcome
// ---------------------------------------------------------------------------

/// What happened to one point.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PointState {
    /// Every key gone. The point is `Deleted`.
    Deleted,
    /// The manifest is gone and at least one segment is not. The set cannot be
    /// read as usable, and the next plan names exactly the leftovers.
    Orphaned,
    /// The manifest could not be deleted, so nothing about the set changed.
    Kept,
}

impl PointState {
    /// The wire spelling.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Deleted => "Deleted",
            Self::Orphaned => "Orphaned",
            Self::Kept => "Kept",
        }
    }
}

/// What one point's execution produced.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct PointOutcome {
    /// The point id.
    pub point_id: String,
    /// `Deleted`, `Orphaned` or `Kept`.
    pub state: String,
    /// How many keys were removed.
    pub objects_deleted: i64,
    /// The keys that remain, if any — exactly what the next plan must name.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub remaining_keys: Vec<String>,
    /// The closed code that stopped it, when something did.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
}

/// What one whole run produced.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Outcome {
    /// Per point, in plan order.
    pub points: Vec<PointOutcome>,
    /// How many object keys were removed in total.
    pub objects_deleted: i64,
    /// How many delete calls were issued, retries included. **Zero on a dry
    /// run**, which is the property `a_dry_run_issues_no_delete` asserts.
    pub attempts: u32,
    /// Whether this was a dry run.
    pub dry_run: bool,
}

impl Outcome {
    /// The point ids fully removed.
    #[must_use]
    pub fn deleted(&self) -> Vec<&str> {
        self.points
            .iter()
            .filter(|p| p.state == PointState::Deleted.as_str())
            .map(|p| p.point_id.as_str())
            .collect()
    }

    /// The points that did not complete, with their codes.
    #[must_use]
    pub fn failed(&self) -> Vec<(&str, &str)> {
        self.points
            .iter()
            .filter(|p| p.state != PointState::Deleted.as_str())
            .map(|p| (p.point_id.as_str(), p.code.as_deref().unwrap_or("Unknown")))
            .collect()
    }

    /// Whether every point completed.
    #[must_use]
    pub fn complete(&self) -> bool {
        self.points
            .iter()
            .all(|p| p.state == PointState::Deleted.as_str())
    }

    /// Whether the ONLY thing that stopped this run was its own object ceiling
    /// — review `d3w9` M2.
    ///
    /// A run like that exits 1 (work remains) and must **not** count toward
    /// `consecutiveRunFailures`: three bounded runs on a large archive would
    /// otherwise set `EnforcementDegraded` and stop retention for good.
    #[must_use]
    pub fn bounded_only(&self) -> bool {
        !self.complete()
            && self
                .points
                .iter()
                .filter(|p| p.state != PointState::Deleted.as_str())
                .all(|p| p.code.as_deref() == Some(DeleteError::BudgetExhausted.as_str()))
    }
}

/// How one run is bounded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// Nothing is deleted; every key is validated and reported.
    pub dry_run: bool,
    /// Stop issuing deletes once this many object keys have been removed.
    pub max_objects: i64,
}

/// Run the plan.
///
/// # Order, per point, per D3 §6.5
///
/// 1. The manifest key. If it does not go, the point is `Kept` and its segments
///    are not touched: deleting segments out from under a live manifest is the
///    one way to produce a set that LOOKS restorable and is not.
/// 2. Every segment key, in plan order.
/// 3. If any segment remains, the point is `Orphaned` and its leftovers are
///    reported so the next plan can name exactly them.
///
/// A failure on one point does not stop the run: the remaining points are
/// independent sets, and a partial failure that abandoned them would need a
/// second approval to finish work the administrator already approved.
///
/// # Bounded retry
///
/// Up to [`MAX_ATTEMPTS`] per key, 1 s and 4 s between them, **only** for
/// [`DeleteError::retryable`]. `AccessDenied`, `Locked` and
/// `PreconditionFailed` are answered once.
pub fn execute<D: Deleter, S: Sleeper, T: TombstoneSink, L: Lister>(
    plan: &Plan,
    deleter: &D,
    sleeper: &S,
    tombstones: &T,
    lister: &L,
    run: &RunAttribution,
    limits: Limits,
) -> Outcome {
    let mut out = Outcome {
        dry_run: limits.dry_run,
        ..Outcome::default()
    };
    for line in &plan.lines {
        if limits.dry_run {
            // THE PREVIEW ENUMERATES (review `d3w9` M1). A plan this build
            // writes names the manifest and the set's bound, so the plan's own
            // key list is 1 while the run removes everything under the bound.
            // A preview that printed 1 is how an administrator approves a
            // three-object removal and gets three thousand. Listing is a
            // separate IAM verb from deleting and the run needs it anyway, so
            // the dry run asks for it — and `NoListing` falls back to the
            // plan's own keys, which is the honest answer for a caller that
            // holds no list grant.
            let keys = resolve_keys(line, lister).unwrap_or_else(|_| line.object_keys.clone());
            // AND IT SAYS WHEN THE RUN WOULD REFUSE (review L9). A dry run
            // writes no tombstone, so the bucket-level signal the run reads
            // ([`TombstoneSink::put_create_only`]'s version id) does not exist
            // here; the manifest's own HEAD is what the preview can ask. A
            // manifest stored as a null version under versioning enabled later
            // answers no version id, so a preview can still look runnable
            // where the run refuses — the run's own signal is the authority.
            let code = match deleter.probe_versioning(&line.manifest_key) {
                Ok(Versioning::Versioned) => "DryRun:VersionedBucket",
                _ => "DryRun",
            };
            push_line(
                &mut out,
                line,
                PointState::Kept,
                0,
                keys,
                Some(code.to_string()),
            );
            continue;
        }
        if out.objects_deleted >= limits.max_objects {
            push_line(
                &mut out,
                line,
                PointState::Kept,
                0,
                line.object_keys.clone(),
                Some(DeleteError::BudgetExhausted.as_str().to_string()),
            );
            continue;
        }

        // 0a. THE KEY LIST. Either the plan named every key, or the plan named
        //     the set directory and this lists it — and re-validates every key
        //     it got back, because a listing comes from the bucket and the
        //     bucket is what this worker does not trust.
        let keys = match resolve_keys(line, lister) {
            Ok(k) => k,
            Err(code) => {
                push_line(
                    &mut out,
                    line,
                    PointState::Kept,
                    0,
                    line.object_keys.clone(),
                    Some(code),
                );
                continue;
            }
        };

        // 0b. THE INTENT, BEFORE ANY DELETE — one per point the line removes,
        //     co-points included. A point whose intent could not be written is
        //     not deleted: "every deletion is attributable" is a precondition,
        //     not a report.
        //
        //     AND THE INTENT IS THE BUCKET'S VERSIONING PROBE (review H1). The
        //     tombstone is PUT into the SAME bucket (one `DestinationLocation`
        //     holds the archive prefix and `logweir/`), and a provider answers
        //     a PUT with a version id exactly when versioning is ENABLED on the
        //     bucket now — measured on MinIO by the ctl-batch-2 review: a
        //     version id from an Enabled bucket, none from a plain or a
        //     Suspended one. Under Enabled versioning EVERY delete by key is a
        //     marker, including of an object stored before versioning was
        //     turned on — whose HEAD answers no version id, which is why the
        //     per-key HEAD alone was not enough. So: nothing is deleted, and
        //     the line is `VersionedBucket`.
        let mut intent_versioned = false;
        let mut refused: Option<String> = None;
        for point in line_points(line) {
            match write_tombstone(tombstones, run, line, point, "intent", 0, &keys, None) {
                Ok(version) => intent_versioned |= version.is_some_and(|v| !v.is_empty()),
                Err(e) => {
                    refused = Some(format!("TombstoneRefused:{e}"));
                    break;
                }
            }
        }
        if let Some(code) = refused {
            push_line(&mut out, line, PointState::Kept, 0, keys, Some(code));
            continue;
        }
        if intent_versioned {
            push_line(
                &mut out,
                line,
                PointState::Kept,
                0,
                keys,
                Some(DeleteError::VersionedBucket.as_str().to_string()),
            );
            continue;
        }

        // 1. The manifest.
        let (manifest_ok, manifest_code) = attempt(deleter, sleeper, &line.manifest_key, &mut out);
        if !manifest_ok {
            push_line(
                &mut out,
                line,
                PointState::Kept,
                0,
                keys,
                manifest_code.map(|c| c.as_str().to_string()),
            );
            continue;
        }
        let mut removed = 1i64;
        let mut code_text_override: Option<String> = None;
        out.objects_deleted = out.objects_deleted.saturating_add(1);

        // 2. The segments.
        let mut remaining: Vec<String> = Vec::new();
        let mut code: Option<DeleteError> = None;
        for key in keys.iter().skip(1) {
            if out.objects_deleted >= limits.max_objects {
                remaining.push(key.clone());
                // NAMED, not `Unclassified` (review `d3w9` M2). A run that
                // stopped on its own ceiling and a run that could not read the
                // bucket are different findings and are fixed in different
                // places.
                code.get_or_insert(DeleteError::BudgetExhausted);
                continue;
            }
            let (ok, why) = attempt(deleter, sleeper, key, &mut out);
            if ok {
                removed += 1;
                out.objects_deleted = out.objects_deleted.saturating_add(1);
            } else {
                remaining.push(key.clone());
                if code.is_none() {
                    code = why;
                }
            }
        }

        // 2b. THE BUCKET, AGAIN, AFTER THE DELETES (re-check RH1). The intent
        //     proved versioning was not Enabled when this line started; it
        //     proves nothing about the minutes the deletes took. Versioning
        //     switched on in between turns every later delete by key into a
        //     marker — and the per-key HEAD cannot see it for an object stored
        //     before (a null version answers no version id). So one more
        //     create-only object is PUT into the same bucket, and if THAT comes
        //     back versioned, nothing the deletes answered can be trusted: the
        //     line is not `Deleted`. It is `Orphaned` (the manifest DELETE was
        //     answered, so no reader that does not ask for versions sees a
        //     usable set) with `VersionedBucket`, every planned key named as
        //     possibly remaining, and no object counted as removed. A check
        //     that could not be written is the same "could not tell".
        let mut unverified: Option<String> = None;
        match write_check(tombstones, run, line) {
            Ok(Some(v)) if !v.is_empty() => {
                unverified = Some(DeleteError::VersionedBucket.as_str().to_string());
            }
            Ok(_) => {}
            Err(e) => unverified = Some(format!("VersionCheckRefused:{e}")),
        }
        if let Some(why) = unverified {
            out.objects_deleted = out.objects_deleted.saturating_sub(removed);
            removed = 0;
            remaining.clone_from(&keys);
            code_text_override = Some(why);
        }

        // 3. The verdict, and the completion tombstone beside it — one per
        //    point. A completion that could not be written does not un-delete
        //    anything, so it is recorded on the point and the run reports it;
        //    the intent is already there, so the deletion stays attributable.
        let state = if remaining.is_empty() {
            PointState::Deleted
        } else {
            PointState::Orphaned
        };
        let mut code_text = code_text_override
            .clone()
            .or_else(|| code.map(|c| c.as_str().to_string()));
        for point in line_points(line) {
            if let Err(e) = write_tombstone(
                tombstones,
                run,
                line,
                point,
                "completion",
                removed,
                &remaining,
                code_text_override
                    .as_deref()
                    .or_else(|| code.map(DeleteError::as_str)),
            ) {
                code_text = Some(format!("TombstoneIncomplete:{e}"));
            }
        }
        push_line(&mut out, line, state, removed, remaining, code_text);
    }
    out
}

/// Every point one line removes: its own, then its co-points
/// ([`PlanLine::co_point_ids`]), in plan order.
fn line_points(line: &PlanLine) -> impl Iterator<Item = &str> {
    std::iter::once(line.point_id.as_str()).chain(line.co_point_ids.iter().map(String::as_str))
}

/// One outcome per point the line removes. The objects are counted ONCE, on
/// the line's own point: a co-point shares the set, so its removal is the same
/// removal, and counting it twice would make `objects_deleted` a lie.
fn push_line(
    out: &mut Outcome,
    line: &PlanLine,
    state: PointState,
    objects_deleted: i64,
    remaining_keys: Vec<String>,
    code: Option<String>,
) {
    for (i, point) in line_points(line).enumerate() {
        out.points.push(PointOutcome {
            point_id: point.to_string(),
            state: state.as_str().to_string(),
            objects_deleted: if i == 0 { objects_deleted } else { 0 },
            remaining_keys: remaining_keys.clone(),
            code: code.clone(),
        });
    }
}

/// Every key one line means, manifest first.
///
/// For a plan that named them all this is the plan's own list. For a plan that
/// named the set directory this LISTS it and re-validates every key that comes
/// back — refusing the whole point, without deleting anything, on the first key
/// that escapes the bound.
fn resolve_keys<L: Lister>(line: &PlanLine, lister: &L) -> Result<Vec<String>, String> {
    if !line.enumerate_set {
        return Ok(line.object_keys.clone());
    }
    let listed = lister
        .list_exact(&line.set_prefix)
        .map_err(|e| format!("ListRefused:{}", e.as_str()))?;
    for key in &listed {
        validate_listed_key(key, line).map_err(|e| {
            // NAMED, AND THE POINT IS SKIPPED WITH ZERO DELETES. A listing that
            // returned something outside the bound is a listing this worker has
            // no reason to trust at all, so nothing from it is used.
            format!("RetentionScopeViolation:{e}")
        })?;
    }
    // MANIFEST FIRST, then everything else in a total order so a re-run
    // produces the same sequence. The manifest is included even when the
    // listing did not return it: a manifest that is already gone is a
    // `NotFound`, which the executor treats as done.
    let mut rest: Vec<String> = listed
        .into_iter()
        .filter(|k| k != &line.manifest_key)
        .collect();
    rest.sort();
    rest.dedup();
    let mut keys = Vec::with_capacity(rest.len() + 1);
    keys.push(line.manifest_key.clone());
    keys.extend(rest);
    Ok(keys)
}

/// Who a run is, for the tombstones it writes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunAttribution {
    /// The run id.
    pub run_id: String,
    /// The policy UID — the tombstone key's first segment.
    pub policy_uid: String,
    /// The plan digest that authorised the run.
    pub plan_sha256: String,
}

#[allow(clippy::too_many_arguments)]
fn write_tombstone<T: TombstoneSink>(
    sink: &T,
    run: &RunAttribution,
    line: &PlanLine,
    point_id: &str,
    stage: &str,
    objects_deleted: i64,
    remaining: &[String],
    code: Option<&str>,
) -> Result<Option<String>, SinkError> {
    let state = if stage == "intent" {
        PointState::Kept
    } else if remaining.is_empty() {
        PointState::Deleted
    } else {
        PointState::Orphaned
    };
    let doc = Tombstone {
        format: TOMBSTONE_MEDIA_TYPE.to_string(),
        stage: stage.to_string(),
        run_id: run.run_id.clone(),
        policy_uid: run.policy_uid.clone(),
        plan_sha256: run.plan_sha256.clone(),
        point_id: point_id.to_string(),
        backup_id: line.backup_id.clone(),
        object_keys: line.object_keys.clone(),
        objects_deleted,
        remaining_keys: remaining.to_vec(),
        state: state.as_str().to_string(),
        code: code.map(str::to_string),
    };
    let bytes = logweir_core::det_json::to_deterministic_json(&doc)
        .map_err(|e| SinkError(e.to_string()))?;
    sink.put_create_only(
        &tombstone_key(&run.policy_uid, &run.run_id, point_id, stage),
        &bytes,
    )
}

/// The post-delete versioning check's media type (re-check RH1).
pub const VERSIONING_CHECK_MEDIA_TYPE: &str =
    "application/vnd.logweir.retention-versioning-check+json;version=1.0.0";

/// The versioning check one line writes after its deletes — a create-only
/// object under `logweir/`, in the same bucket, whose only job is the version
/// id the provider answers it with.
#[derive(Serialize)]
struct VersioningCheck<'a> {
    format: &'a str,
    run_id: &'a str,
    policy_uid: &'a str,
    point_id: &'a str,
    purpose: &'a str,
}

fn write_check<T: TombstoneSink>(
    sink: &T,
    run: &RunAttribution,
    line: &PlanLine,
) -> Result<Option<String>, SinkError> {
    let doc = VersioningCheck {
        format: VERSIONING_CHECK_MEDIA_TYPE,
        run_id: &run.run_id,
        policy_uid: &run.policy_uid,
        point_id: &line.point_id,
        purpose: "a version id on this object means bucket versioning was Enabled when the \
                  point's deletes finished, so they may have written delete markers",
    };
    let bytes = logweir_core::det_json::to_deterministic_json(&doc)
        .map_err(|e| SinkError(e.to_string()))?;
    sink.put_create_only(
        &tombstone_key(&run.policy_uid, &run.run_id, &line.point_id, "check"),
        &bytes,
    )
}

/// One key, with the bounded retry. `true` when the key is gone.
///
/// # A delete marker is not a deletion (defect OBJECT-LOCK-DELETE-MARKER)
///
/// Every key is probed first ([`Deleter::probe_versioning`]). On a versioned
/// bucket — and every S3 Object Lock bucket is one — a DELETE with no version
/// id is ANSWERED 204 and removes nothing: the provider writes a delete marker,
/// the data survives as a noncurrent version, and a legal hold on it is never
/// consulted because no version was asked for. The first landing recorded such
/// a point `Deleted` (live: harness-rows-11 `object-lock`, a held point
/// `Deleted` with its data intact under markers). `object_store` 0.14 can
/// neither delete by version (where the provider would refuse a held one) nor
/// read the marker header off the response, so the worker REFUSES the
/// combination — PLAT-16.2's "reject unsupported combinations rather than
/// claiming" — and says so with [`DeleteError::VersionedBucket`], deleting
/// nothing for that key.
fn attempt<D: Deleter, S: Sleeper>(
    deleter: &D,
    sleeper: &S,
    key: &str,
    out: &mut Outcome,
) -> (bool, Option<DeleteError>) {
    match probe(deleter, sleeper, key) {
        Ok(Versioning::Unversioned) => {}
        Ok(Versioning::Versioned) => return (false, Some(DeleteError::VersionedBucket)),
        // ALREADY GONE IS DONE, and no delete is sent: on a versioned bucket a
        // DELETE of an absent key would itself write a marker.
        Err(DeleteError::NotFound) => return (true, None),
        // A transport failure that outlived its retries is named as itself.
        Err(e) if e.retryable() => return (false, Some(e)),
        Err(_) => return (false, Some(DeleteError::VersionProbeRefused)),
    }
    let mut last: Option<DeleteError> = None;
    for n in 0..MAX_ATTEMPTS {
        out.attempts = out.attempts.saturating_add(1);
        match deleter.delete_exact(key) {
            Ok(()) => return (true, None),
            // ALREADY GONE IS DONE. A key an interrupted run removed is a key
            // this run wanted removed; reporting it as a failure would make
            // idempotent completion impossible.
            Err(DeleteError::NotFound) => return (true, None),
            Err(e) => {
                last = Some(e);
                if !e.retryable() {
                    return (false, last);
                }
                if let Some(seconds) = BACKOFF_SECONDS.get(n as usize) {
                    sleeper.sleep(Duration::from_secs(*seconds));
                }
            }
        }
    }
    (false, last)
}

/// The versioning probe, with the same bounded retry a delete gets — and NOT
/// counted in [`Outcome::attempts`], which counts delete calls.
fn probe<D: Deleter, S: Sleeper>(
    deleter: &D,
    sleeper: &S,
    key: &str,
) -> Result<Versioning, DeleteError> {
    let mut last = DeleteError::Unclassified;
    for n in 0..MAX_ATTEMPTS {
        match deleter.probe_versioning(key) {
            Ok(v) => return Ok(v),
            Err(e) if e.retryable() => {
                last = e;
                if let Some(seconds) = BACKOFF_SECONDS.get(n as usize) {
                    sleeper.sleep(Duration::from_secs(*seconds));
                }
            }
            Err(e) => return Err(e),
        }
    }
    Err(last)
}

// ---------------------------------------------------------------------------
// The attributable record
// ---------------------------------------------------------------------------

/// The signed-record document — D3 §6.5's "attributable deletion".
///
/// Written under `logweir/retention/<policyUid>/<runId>.json`, create-only, to
/// a prefix this run's own credential cannot delete from.
///
/// **What it proves and what it does not.** It is written with
/// `PutMode::Create`, so it cannot be silently replaced by the retention
/// principal, and it names the plan digest, the approver reference, the policy
/// identity and generation, every point removed with its object count and every
/// failure with its closed code. It is **not** signed in this landing — see
/// `docs/stability.md`, "the retention record is create-only and unsigned in
/// v1" — so it is tamper-evident against the retention principal and not
/// against a principal with `s3:PutObject` under `logweir/`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct Record {
    /// The media type.
    pub format: String,
    /// The document version.
    pub format_version: String,
    /// The run id.
    pub run_id: String,
    /// The policy's namespace.
    pub policy_namespace: String,
    /// The policy's name.
    pub policy_name: String,
    /// The policy's UID.
    pub policy_uid: String,
    /// The generation.
    pub policy_generation: i64,
    /// The plan digest that authorised this run.
    pub plan_sha256: String,
    /// Who approved it — the API audit id or the patching subject recorded in
    /// an annotation. `"unattended"` when `requireApprovedPlan: false`.
    pub approver: String,
    /// The destination's location id.
    pub location_id: String,
    /// The scope prefix.
    pub scope_prefix: String,
    /// `keepLast`, as applied.
    pub keep_last: Option<i64>,
    /// `keepDays`, as applied.
    pub keep_days: Option<i64>,
    /// `minUsablePoints`, as applied.
    pub min_usable_points: i64,
    /// When the run started.
    pub started_at: DateTime<Utc>,
    /// When it finished.
    pub finished_at: DateTime<Utc>,
    /// Whether it was a dry run.
    pub dry_run: bool,
    /// Per point.
    pub points: Vec<PointOutcome>,
    /// How many object keys were removed.
    pub objects_deleted: i64,
    /// The process's exit code.
    pub exit_code: i32,
}

/// What the caller knows that the plan does not.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordContext {
    /// The run id.
    pub run_id: String,
    /// The policy generation this run was created at.
    ///
    /// It comes from the Job's environment and NOT from the plan: the plan
    /// carries no generation, because approving one bumps it (review `d3w9`
    /// C1). The record wants it anyway — it is a fact about the run, and an
    /// auditor asking "which revision of the policy authorised this" needs it.
    pub policy_generation: i64,
    /// The approver reference, or `"unattended"`.
    pub approver: String,
    /// The plan digest.
    pub plan_sha256: String,
    /// When the run started.
    pub started_at: DateTime<Utc>,
    /// When it finished.
    pub finished_at: DateTime<Utc>,
    /// The exit code the process will return.
    pub exit_code: i32,
}

/// Build the record for one run.
#[must_use]
pub fn record(plan: &Plan, outcome: &Outcome, ctx: &RecordContext) -> Record {
    Record {
        format: RECORD_MEDIA_TYPE.to_string(),
        format_version: RECORD_FORMAT_VERSION.to_string(),
        run_id: ctx.run_id.clone(),
        policy_namespace: plan.policy_namespace.clone(),
        policy_name: plan.policy_name.clone(),
        policy_uid: plan.policy_uid.clone(),
        policy_generation: ctx.policy_generation,
        plan_sha256: ctx.plan_sha256.clone(),
        approver: ctx.approver.clone(),
        location_id: plan.location_id.clone(),
        scope_prefix: plan.scope_prefix.clone(),
        keep_last: plan.keep_last,
        keep_days: plan.keep_days,
        min_usable_points: plan.min_usable_points,
        started_at: ctx.started_at,
        finished_at: ctx.finished_at,
        dry_run: outcome.dry_run,
        points: outcome.points.clone(),
        objects_deleted: outcome.objects_deleted,
        exit_code: ctx.exit_code,
    }
}

/// The record's canonical bytes and their digest.
///
/// # Errors
///
/// [`Refusal::Unreadable`] if the document does not serialise, which it always
/// does; named rather than unwrapped.
pub fn record_bytes(record: &Record) -> Result<(Vec<u8>, String), Refusal> {
    let bytes = logweir_core::det_json::to_deterministic_json(record)
        .map_err(|e| Refusal::Unreadable(e.to_string()))?;
    let digest = logweir_core::ids::sha256_prefixed(&bytes);
    Ok((bytes, digest))
}

/// Where one run's record goes.
#[must_use]
pub fn record_key(policy_uid: &str, run_id: &str) -> String {
    format!("{EVIDENCE_ROOT}retention/{policy_uid}/{run_id}.json")
}
