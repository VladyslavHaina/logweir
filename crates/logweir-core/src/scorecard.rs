use crate::outcome::{IntegrityLevel, IntegrityResult, LeverState, MatrixVerdict, Outcome};
use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Scorecard {
    /// Semver of the document format, and the field every reader checks
    /// FIRST: Global Constraint 12 — readers ignore unknown fields and refuse
    /// a higher major.
    ///
    /// The schema PINS the major with a pattern rather than leaving the field
    /// an unconstrained string. Without it, a document reading `"9.9.9"`
    /// validated cleanly against the file named
    /// `logweir-drill-scorecard-1.0.0.json`, so a schema-only validator — the
    /// one route that does not go through
    /// `Scorecard::refuse_unreadable_major` — accepted exactly the document
    /// GC12 exists to refuse. The pattern allows any `1.x.y`, because a MINOR
    /// bump adds optional fields only and a 1.0.0 reader must still read it.
    #[schemars(regex(pattern = r"^1\.[0-9]+\.[0-9]+$"))]
    pub format_version: String,
    pub run_id: String,
    pub outcome: Outcome,
    /// Highest phase INDEX completed. The DOMAIN is eleven phase slots, -1
    /// through 9 (Global Constraint 18, panel decision D1, 2026-09-03); -1 is
    /// the source-side `--from-cluster` capture phase, whose code lands in a
    /// follow-up, so v0.1.0 never emits it.
    ///
    /// WHAT A v0.1.0 SIGNED DOCUMENT CAN ACTUALLY CARRY IS 5, 6 or 7, and the
    /// reason is structural rather than incidental: `phase8_score::run` signs
    /// a frozen clone, so phase 8's own record — and phase 9's, which happens
    /// after the put — are pushed onto the in-memory document AFTER the bytes
    /// were signed. A drill that completed teardown therefore reads 7 here and
    /// is NOT a drill whose teardown was skipped; the teardown is attested in
    /// its own separately signed document. `drill run`'s console line quotes
    /// this same value so the two cannot disagree.
    ///
    /// A previous version of this comment added "a value below -1 is
    /// impossible because the phase-0 admission guard refusing yields exit
    /// code 3 with last_phase_completed = -1", which implied such a document
    /// exists. It does not: exit 3 writes NO scorecard at all. The lower bound
    /// is a domain rule, enforced by `validate_invariants`, not a description
    /// of anything v0.1 emits.
    pub last_phase_completed: i8,
    pub requested_at: DateTime<Utc>,
    #[serde(default)]
    pub approval_validated_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub triggered_by: Option<String>,
    pub engine: EngineInfo,
    pub source: SourceInfo,
    pub target: TargetInfo,
    pub approval: ApprovalInfo,
    pub phases: Vec<PhaseRecord>,
    pub measured: Measured,
    pub objectives: Objectives,
    pub sample: SampleInfo,
    pub target_diff: TargetDiffSummary,
    pub integrity: Integrity,
    pub topic_parity: TopicParity,
    #[serde(default)]
    pub engine_subreport: Option<EngineSubreport>,
    pub evidence: EvidenceInfo,
    #[serde(default)]
    pub redactions: Vec<Redaction>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct EngineInfo {
    pub id: String,        // "oso-cli"
    pub version: String,   // "v0.21.0"
    pub digest: String,    // "sha256:…" — the image digest the binary came from
    pub execution: String, // "subprocess" in v0.1; "k8s-job" only under weirkeeper (SP5)
    pub levers: Levers,
    pub matrix_verdict: MatrixVerdict,
    /// REQUIRED when `matrix_verdict` is `fail`; null otherwise. Enforced by
    /// `validate_invariants`.
    #[serde(default)]
    pub matrix_verdict_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Levers {
    pub header_preflight: LeverState,
    pub dry_run_check_segments: LeverState,
    /// Every path the engine logged as `Ignoring unknown config key <path>`.
    /// A match on a Logweir-rendered key aborts the run (spec §7.2(a)).
    #[serde(default)]
    pub unknown_key_warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SourceInfo {
    pub backup_id: String,
    pub manifest_sha256: String,
    #[serde(default)]
    pub manifest_version_id: Option<String>,
    /// true ONLY under `--from-cluster`. `--from-cluster` is in v0.1 scope
    /// (Global Constraint 18 / docs/adr/0007-from-cluster-in-v0.1.md); its
    /// execution path lands in a follow-up task, so nothing in the main task
    /// line yet sets this true. The invariant below is live regardless.
    pub captured_by_logweir: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct TargetInfo {
    pub cluster_id: String,
    /// The v0.1 segregation proof, verified over the logweir-kafka client:
    /// cluster_id ∈ allowedClusterIds AND this topic exists (spec §9.3 phase 0).
    pub marker_topic: String,
    pub topic_mapping_prefix: String,
    /// sha256 over the rendered restore.yaml topic_mapping block, so an
    /// auditor can re-derive exactly what was written.
    pub topic_mapping_sha256: String,
    pub topic_mapping_entries: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ApprovalInfo {
    pub approver: String,
    pub ticket: String,
    pub plan_hash: String,
    /// The human's OUT-OF-BAND timestamp. Explicitly NOT an input to any
    /// measured field in v0.1 (spec §9.3 phase 8).
    pub approved_at: DateTime<Utc>,
    pub key_id: String,
    /// true when the approval key equals the signing key. Labelled, never
    /// refused (spec §10).
    pub self_attested: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PhaseRecord {
    pub phase: i8,
    pub name: String,
    pub at: DateTime<Utc>,
    pub outcome: String,
    pub duration_ms: u64,
    /// Free-form evidence attached to this phase record. Two producers today,
    /// and the list is corrected here rather than left stale (Task 20 fix
    /// round 2):
    /// - phase 5 (`crate::drill::phase5_preflight`) carries each `Finding`'s
    ///   tag/detail here so a `preflight-failed` scorecard states *why* the
    ///   drill was refused, not just that it was — without this field the
    ///   adjudication's reasoning had nowhere in the signed document to land.
    ///   Note that phase 5 defines WHAT it contributes; the `PhaseRecord`
    ///   itself is not constructed until the orchestrator lands (Task 21a).
    /// - phase 8 (`crate::drill::phase8_score`) writes onto the phase-8 record
    ///   when the engine-validation prefix is empty, and when it holds more
    ///   than one object (naming the key retained and that others were
    ///   dropped). Both are warnings that must not be silent: an absent or
    ///   ambiguous engine sub-report is a fact about the evidence, not a
    ///   failure.
    ///
    /// `skip_serializing_if` (matching `TargetDiffSummary::absent`, Task 16 fix
    /// round 1) so a document signed before this field existed keeps
    /// round-tripping byte-for-byte when it has nothing to report.
    ///
    /// This reverses `task-3-addendum.md` A2 ("do not add a `notes` field to
    /// `PhaseRecord`"), on controller authority, in Task 17 fix round 1: that
    /// ruling predates `task-21a-brief.md:259`, which writes `p.notes = …`
    /// against this exact struct, so leaving the field out only delayed the
    /// same `E0560` to Task 21a.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Measured {
    #[serde(default)]
    pub rto_seconds: Option<u64>,
    #[serde(default)]
    pub rto_requested_to_verified_seconds: Option<u64>,
    #[serde(default)]
    pub rto_restore_only_seconds: Option<u64>,
    /// THE value compared against objectives.rto_seconds (spec §9.3 phase 8).
    #[serde(default)]
    pub rto_excluding_preflight_seconds: Option<u64>,
    /// ARCHIVE COVERAGE GAP at the requested recovery point — NOT
    /// source-relative data loss.
    ///
    /// `requested_point_in_time - newest_restored_record`, in that order, and
    /// **never negative**: it is how far SHORT of the requested recovery point
    /// the newest restored record falls. A record at or beyond the requested
    /// point means no gap there, which is 0. A negative gap is not a smaller
    /// gap, it is a meaningless one, and a reader meeting `-90` here would
    /// most likely take it for "no data loss". Enforced by
    /// `validate_invariants`, so the rule holds for every writer, not only for
    /// today's single call site in `crate::drill::phase8_score`.
    #[serde(default)]
    pub rpo_seconds: Option<i64>,
    /// Source-relative data loss: how far behind the live source the restored
    /// data is. Null in v0.1 (the source is never contacted); becomes a number
    /// only under `--from-cluster`. **Never negative**, for the same reason as
    /// `rpo_seconds` and enforced by the same `validate_invariants` rule — a
    /// rule deliberately put in place BEFORE the first writer exists, so the
    /// `--from-cluster` implementation inherits it rather than has to invent
    /// it.
    #[serde(default)]
    pub rpo_source_relative_seconds: Option<i64>,
    #[serde(default)]
    pub rpo_source_relative_unmeasured_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Objectives {
    #[serde(default)]
    pub rto_seconds: Option<u64>,
    /// The REQUESTED maximum archive-coverage gap, copied verbatim from the
    /// adopter's spec. **Never negative** — a negative allowed gap is not a
    /// stricter objective, it is an unsatisfiable one (`measured.rpo_seconds`
    /// is itself non-negative, so `got <= want` could never hold), and a
    /// signed document asserting it would be incoherent. Enforced by
    /// `validate_invariants`; nothing else validates this value, since it
    /// travels straight from YAML into the signed document.
    #[serde(default)]
    pub rpo_seconds: Option<i64>,
    #[serde(default)]
    pub pass_rate: Option<f64>,
    /// The aggregate verdict over whichever objectives above are non-null.
    ///
    /// - `true`  — every non-null objective was met.
    /// - `false` — at least one non-null objective was missed.
    /// - `null`  — a `pass_rate` objective WAS requested but could not be
    ///   measured (`integrity.pass_rate_measured` is null), so the aggregate
    ///   is unmeasurable rather than satisfied.
    ///
    /// The null condition is about the MEASURED rate, not this block's
    /// `pass_rate`. The earlier wording ("null when pass_rate is null") read
    /// the other way round and was wrong in the direction that matters: when
    /// `objectives.pass_rate` is null, no rate was asked for, nothing is
    /// unmeasurable, and `met` is a plain true/false over the remaining
    /// objectives. Decided in exactly one place,
    /// `crate::drill::phase8_score::decide`.
    #[serde(default)]
    pub met: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SampleInfo {
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub topics: u32,
    pub partitions: u32,
    /// THE CANARY SIZE: how many records this drill set out to reconcile —
    /// the sum of `sample.records_per_partition` over the partitions actually
    /// selected. `integrity.records_sampled` is measured against THIS figure.
    ///
    /// It is NOT `crate::drill::phase4_sample::Selection::records_expected`
    /// (in `crates/logweir`), which shares the name and answers a different
    /// question: how many records the manifest says the whole sampled window
    /// holds. The two differ by orders of magnitude on any real archive — 25
    /// against 500 on a single partition of this repository's own fixtures —
    /// and substituting one for the other would make a signed document
    /// overstate or understate what was verified. Both definition sites carry
    /// this note deliberately (Task 16's parked item, discharged in Task
    /// 21a); the field is not renamed because a scorecard field name is a
    /// Global Constraint 12 question and the format is frozen at 1.0.0.
    pub records_expected: u64,
    pub records_restored: u64,
    pub anchor: String, // head | tail | random
    pub coverage_note: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Integrity {
    pub level: IntegrityLevel,
    pub result: IntegrityResult,
    #[serde(default)]
    pub partial_reason: Option<String>,
    pub records_sampled: u64,
    pub records_sampled_matching: u64,
    pub mismatches: u64,
    /// MEASURED `records_sampled_matching / records_sampled`. The REQUESTED
    /// rate stays in `objectives.pass_rate`, so an auditor can read both the
    /// ask and the result off one document.
    ///
    /// Null in THREE cases, not one — a `byte-fingerprint` document with a
    /// null rate here is well-formed, not malformed:
    /// 1. `level` is not `byte-fingerprint` (also enforced by
    ///    `validate_invariants`).
    /// 2. Not every selection's record lane reached a conclusion. A ratio over
    ///    part of the sample, published as if it were the whole, is misleading
    ///    even when every figure in it is true.
    /// 3. `records_sampled` is 0 — a zero denominator is withheld rather than
    ///    published as NaN.
    ///
    /// Decided in one place, `crate::drill::phase7_verify::roll_up`.
    #[serde(default)]
    pub pass_rate_measured: Option<f64>,
    /// SP3 only; null, never false, when not attempted. The wire name is
    /// camelCase because spec §6.1 and §14 SP3 both cite the path
    /// `integrity.restoredPrincipalCouldConsume`, and `format_version` freezes
    /// at 1.0.0 in this task — renaming later would be a MAJOR bump.
    #[serde(default, rename = "restoredPrincipalCouldConsume")]
    #[schemars(rename = "restoredPrincipalCouldConsume")]
    pub restored_principal_could_consume: Option<bool>,
}

/// The published sink for phase 3 — the phase no shipped artifact performs.
/// Without this block the diff would be computed and discarded, and the §4
/// positioning claim would rest on a value that reaches no reader.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct TargetDiffSummary {
    /// Mapped target topics that already EXIST on the target — regardless of
    /// whether they currently hold any records. Existence alone is what
    /// makes writing into them a collision worth flagging (Task 16 fix
    /// round 1, MINOR-2): an existing-but-empty topic can still carry
    /// configuration that differs from what the restore expects, and
    /// creating a topic fresh is a different operational fact from reusing
    /// one someone else already created — even an empty one. The prior
    /// wording here ("already existed AND already held records") described
    /// a narrower rule than the code implements; the code is right (refusing
    /// to write into an existing empty topic costs at most a false alarm,
    /// while the narrower rule would let a restore silently write into a
    /// topic someone else created), so this text was corrected to match it.
    #[serde(default)]
    pub collisions: Vec<String>,
    /// Mapped target topics that do NOT exist on the target — the normal
    /// case on a scratch cluster. Every entry here has a corresponding
    /// `would_create` entry at the partition count the restore will build.
    /// `skip_serializing_if` (unlike its siblings here) so a document
    /// signed before this field existed keeps round-tripping byte-for-byte
    /// when it has nothing to report — added in Task 16 fix round 1, after
    /// `TargetDiff.absent` was found computed and never read.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub absent: Vec<String>,
    /// (mapped target topic, partition count the restore created).
    #[serde(default)]
    pub would_create: Vec<(String, i32)>,
    /// "full" in v0.1. Becomes "shallow" only if spec §15 cut 0d is ever taken.
    pub level: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct TopicParity {
    pub intentionally_deviated: Vec<String>,
    pub unexpected_divergence: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct EngineSubreport {
    pub retained_verbatim: bool,
    pub retrieved_from: String,
    pub caveat: String,
    /// The engine's evidence report as the EXACT bytes read from the bucket,
    /// base64 (RFC 4648 standard alphabet, padded).
    ///
    /// NOT a parsed `serde_json::Value`. OSO's envelope "covers the exact,
    /// complete stored report bytes. No canonicalization is performed at
    /// verification time" and "no JSON parsing, re-serialization, or
    /// canonicalization influences the digest or signature check"
    /// [VERIFIED U/kafka-backup/crates/kafka-backup-core/src/evidence/envelope.rs:5,254].
    /// Re-emitting a parsed value through `to_deterministic_json`'s two-space
    /// `PrettyFormatter` would change whitespace, escaping and number
    /// formatting, so the digest OSO signed would no longer match and the SP1c
    /// exit criterion ("the embedded engine sub-report round-trips through
    /// OSO's own `validation evidence-verify`") could never be ticked.
    pub body_b64: String,
    /// `sha256:<hex>` of the DECODED bytes, so an auditor can re-check the
    /// binding without base64-decoding anything.
    pub body_sha256: String,
}

/// WHAT THIS BLOCK IS NOT: it is not evidence about the upload, and it is not
/// a statement about the storage backend's capabilities.
///
/// All four fields describe the upload of THIS scorecard — an event that has
/// not happened when the scorecard is signed, and cannot happen before it,
/// because a signature covers bytes and the bytes must exist first. The
/// scorecard is never re-serialised afterwards (that would invalidate the
/// signature). So `crate::drill::phase8_score::run` ZEROES all four
/// immediately before serialising, whatever the caller supplied, and every
/// scorecard Logweir emits in v0.1 carries
/// `{version_id: null, retain_until: null, immutable: false,
/// create_only_enforced: false}`.
///
/// Read that as **"no proof was obtainable at signing time"** — never as a
/// finding about the object or the backend. The block deliberately
/// UNDER-claims; what it buys is the guarantee that a valid Logweir signature
/// can never cover an unsubstantiated WORM or create-only assertion.
///
/// The real post-upload readback IS published — in a second, separately signed
/// document written after the put: `logweir/drills/<run_id>.receipt.json` plus
/// its `.receipt.sig` sidecar (`crate::drill::phase8_score::PutReceipt`, Task
/// 21a, discharging Task 20's carried obligation). It carries the observed
/// `create_only_enforced`, `version_id`, `immutable` and `retain_until`, bound
/// to the scorecard by the sha256 of the exact SIGNED BYTES. Read the two
/// documents together: the scorecard is the measurement and under-claims about
/// storage; the receipt is the storage evidence. `docs/stability.md` carries
/// the adopter-facing version of this note.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct EvidenceInfo {
    /// Always null in v0.1: the store's version id for this object is only
    /// known after the put, and the put follows the signature. NOT a claim
    /// that the bucket is unversioned.
    #[serde(default)]
    pub version_id: Option<String>,
    /// Always null in v0.1: a retention date can only come from a provider
    /// readback performed after the put. NOT a claim that no retention
    /// applies.
    #[serde(default)]
    pub retain_until: Option<DateTime<Utc>>,
    /// Spec §6 C3 permits `true` ONLY after a provider readback. Always false
    /// in v0.1 — the readback happens after signing, and `object_store` 0.14
    /// exposes no Object Lock / WORM API on any backend it can build, so there
    /// is nothing to read back. `false` means "not proven here"; it is NOT a
    /// claim that the object is mutable or unprotected.
    pub immutable: bool,
    /// Always false in v0.1, on EVERY backend, including those whose
    /// conditional put was in fact used: whether the put was conditional is
    /// only known after it happens, and this document is signed first. `false`
    /// is NOT evidence that the backend lacks conditional put (spec §11) and
    /// NOT evidence that the object could have been overwritten — the two
    /// cases are indistinguishable in the signed document by design.
    pub create_only_enforced: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Redaction {
    /// A JSON pointer into THIS scorecard — never a missing field.
    pub path: String,
    pub reason: String,
    pub present: bool,
}

#[derive(Debug, thiserror::Error)]
#[error("scorecard invariant violated: {0}")]
pub struct InvariantError(pub String);

/// The leading dot-separated component of a semver string, parsed as an
/// integer. Returns `None` for anything that doesn't start with an integer
/// (so a malformed `format_version` is refused rather than silently treated
/// as major 0).
fn major_version(v: &str) -> Option<u64> {
    v.split('.').next()?.parse().ok()
}

impl Scorecard {
    /// GLOBAL CONSTRAINT 12, IN ONE PLACE: a reader must refuse a
    /// `format_version` whose major is newer than the one this binary
    /// understands, and must ignore unknown fields.
    ///
    /// Public, and separate from `validate_invariants`, because Logweir ships
    /// THREE readers and the rule belongs to all of them. It used to be an
    /// inlined first block of `validate_invariants`, which meant `drill
    /// verify` honoured it (it calls that function) and `drill show` did not
    /// (it never did) — a `format_version: "2.0.0"` document rendered its full
    /// table, header reading `v2.0.0`, `outcome Pass`, and exited 0. A rule the
    /// README states and the project keeps in one place out of three is not a
    /// rule. `docs/verify_scorecard.py` is the third reader and implements the
    /// same comparison against its own `FORMAT_VERSION` constant.
    ///
    /// The comparison is against `crate::FORMAT_VERSION` by string, never by
    /// re-deriving what "this build understands" some other way.
    pub fn refuse_unreadable_major(&self) -> Result<(), InvariantError> {
        let doc_major = major_version(&self.format_version).ok_or_else(|| {
            InvariantError(format!(
                "format_version {:?} is not a parseable semver",
                self.format_version
            ))
        })?;
        let known_major =
            major_version(crate::FORMAT_VERSION).expect("FORMAT_VERSION is a valid semver");
        if doc_major > known_major {
            return Err(InvariantError(format!(
                "format_version {} has a major version newer than this reader understands \
                 (this build knows {})",
                self.format_version,
                crate::FORMAT_VERSION
            )));
        }
        Ok(())
    }

    /// The invariants spec §6.1 and §9.3 phase 8 state in prose. Called before
    /// signing (Task 20) and by `drill verify` (Task 6), so no signed document
    /// can carry a self-contradicting claim.
    pub fn validate_invariants(&self) -> Result<(), InvariantError> {
        // Checked FIRST, so a document from a future major bump is rejected
        // before any other invariant is evaluated against fields that build
        // may have changed the meaning of.
        self.refuse_unreadable_major()?;
        if self.integrity.result == IntegrityResult::Partial
            && self.integrity.partial_reason.is_none()
        {
            return Err(InvariantError(
                "integrity.result is 'partial' but partial_reason is null".into(),
            ));
        }
        // The format's only two float fields. `serde_json::to_value` turns a
        // non-finite f64 into `Value::Null` before `det_json`'s own walk ever
        // sees it (see `det_json.rs`'s module doc comment), so finiteness has
        // to be enforced here, on the typed field, where the error can still
        // name which field was bad.
        if let Some(pass_rate) = self.objectives.pass_rate {
            if !pass_rate.is_finite() {
                return Err(InvariantError(
                    "objectives.pass_rate is not finite (NaN or +/-Inf)".into(),
                ));
            }
        }
        if let Some(pass_rate_measured) = self.integrity.pass_rate_measured {
            if !pass_rate_measured.is_finite() {
                return Err(InvariantError(
                    "integrity.pass_rate_measured is not finite (NaN or +/-Inf)".into(),
                ));
            }
        }
        // Global Constraint 18(a): captured_by_logweir is a biconditional.
        // true  => last_phase_completed >= -1, a measured source-relative RPO,
        //          and NO unmeasured reason.
        // false => no source-relative RPO, and a reason saying why not.
        if self.source.captured_by_logweir {
            if self.last_phase_completed < -1 {
                return Err(InvariantError(
                    "source.captured_by_logweir is true but last_phase_completed is below -1"
                        .into(),
                ));
            }
            if self.measured.rpo_source_relative_seconds.is_none() {
                return Err(InvariantError(
                    "source.captured_by_logweir is true but rpo_source_relative_seconds is null"
                        .into(),
                ));
            }
            if self
                .measured
                .rpo_source_relative_unmeasured_reason
                .is_some()
            {
                return Err(InvariantError(
                    "source.captured_by_logweir is true but an unmeasured reason is present".into(),
                ));
            }
        } else {
            if self.measured.rpo_source_relative_seconds.is_some() {
                return Err(InvariantError(
                    "rpo_source_relative_seconds is set but the source was never contacted".into(),
                ));
            }
            if self
                .measured
                .rpo_source_relative_unmeasured_reason
                .is_none()
            {
                return Err(InvariantError(
                    "source.captured_by_logweir is false but rpo_source_relative_unmeasured_reason is null".into(),
                ));
            }
        }
        if self.integrity.level != IntegrityLevel::ByteFingerprint
            && self.objectives.pass_rate.is_some()
            && self.objectives.met == Some(true)
        {
            return Err(InvariantError(
                "objectives.met must be null when pass_rate is not measurable".into(),
            ));
        }
        // Every seconds-valued gap in the document is non-negative. These are
        // the format's only signed integers, and each has a direction that,
        // until now, only one call site enforced: `measured.rpo_seconds` by
        // the clamp in `crate::drill::phase8_score::compute_measured`,
        // `objectives.rpo_seconds` by nothing at all (it travels straight from
        // adopter YAML into the signed document), and
        // `measured.rpo_source_relative_seconds` by a writer that does not
        // exist yet (`--from-cluster`). Stating the rule here, in the
        // scorecard's own validator, converts a property guaranteed by one
        // call site into one guaranteed by the type — including for phases not
        // yet written. Checked field by field so the message names the
        // offender. The four RTO figures need no equivalent: they are `u64`,
        // so negative is already unrepresentable.
        for (name, value) in [
            ("measured.rpo_seconds", self.measured.rpo_seconds),
            (
                "measured.rpo_source_relative_seconds",
                self.measured.rpo_source_relative_seconds,
            ),
            ("objectives.rpo_seconds", self.objectives.rpo_seconds),
        ] {
            if let Some(v) = value {
                if v < 0 {
                    return Err(InvariantError(format!(
                        "{name} is negative ({v}); a recovery-point gap of less than zero \
                         is not a smaller gap, it is a meaningless one"
                    )));
                }
            }
        }
        if self.integrity.records_sampled_matching > self.integrity.records_sampled {
            return Err(InvariantError(
                "records_sampled_matching exceeds records_sampled".into(),
            ));
        }
        if self.engine.matrix_verdict == MatrixVerdict::Fail
            && self.engine.matrix_verdict_reason.is_none()
        {
            return Err(InvariantError(
                "engine.matrix_verdict is 'fail' but matrix_verdict_reason is null".into(),
            ));
        }
        if self.integrity.level != IntegrityLevel::ByteFingerprint
            && self.integrity.pass_rate_measured.is_some()
        {
            return Err(InvariantError(
                "integrity.pass_rate_measured is set but the level is not byte-fingerprint".into(),
            ));
        }
        if !(-1..=9).contains(&self.last_phase_completed) {
            return Err(InvariantError("last_phase_completed outside -1..=9".into()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    //! Direct coverage of `Scorecard::validate_invariants`'s per-arm behaviour.
    //! `crates/logweir-core/tests/scorecard_golden.rs` is frozen at exactly
    //! four tests (addendum ruling A1), so this coverage lives here instead.
    //! Every test asserts the SPECIFIC error message, not a bare `is_err()`,
    //! so deleting or merging an arm makes exactly one test fail.
    use super::*;

    /// A scorecard that satisfies every invariant `validate_invariants` checks.
    /// `captured_by_logweir` is false (the false-branch of Global Constraint
    /// 18(a)), matching what v0.1 ever actually produces. Each test below
    /// clones this and overrides only the field(s) needed to trip one arm.
    fn valid_scorecard() -> Scorecard {
        let t = |s: &str| DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc);
        Scorecard {
            format_version: crate::FORMAT_VERSION.to_string(),
            run_id: "01J9X2QK7C4V0R8YB3ZP6MTS5A".into(),
            outcome: Outcome::Pass,
            last_phase_completed: 9,
            requested_at: t("2026-09-03T09:00:00Z"),
            approval_validated_at: None,
            triggered_by: None,
            engine: EngineInfo {
                id: "oso-cli".into(),
                version: "v0.21.0".into(),
                digest: "sha256:0".into(),
                execution: "subprocess".into(),
                levers: Levers {
                    header_preflight: LeverState::Honoured,
                    dry_run_check_segments: LeverState::UnknownNotObservable,
                    unknown_key_warnings: vec![],
                },
                matrix_verdict: MatrixVerdict::Pass,
                matrix_verdict_reason: None,
            },
            source: SourceInfo {
                backup_id: "backup-1".into(),
                manifest_sha256: "sha256:0".into(),
                manifest_version_id: None,
                captured_by_logweir: false,
            },
            target: TargetInfo {
                cluster_id: "cluster-1".into(),
                marker_topic: "logweir.scratch".into(),
                topic_mapping_prefix: "drill-".into(),
                topic_mapping_sha256: "sha256:0".into(),
                topic_mapping_entries: 1,
            },
            approval: ApprovalInfo {
                approver: "sre-oncall@example.com".into(),
                ticket: "CHG-1".into(),
                plan_hash: "sha256:0".into(),
                approved_at: t("2026-09-02T17:40:00Z"),
                key_id: "a".repeat(64),
                self_attested: false,
            },
            phases: vec![],
            measured: Measured {
                rto_seconds: None,
                rto_requested_to_verified_seconds: None,
                rto_restore_only_seconds: None,
                rto_excluding_preflight_seconds: None,
                rpo_seconds: None,
                rpo_source_relative_seconds: None,
                rpo_source_relative_unmeasured_reason: Some(
                    "source cluster never contacted".into(),
                ),
            },
            objectives: Objectives {
                rto_seconds: None,
                rpo_seconds: None,
                pass_rate: None,
                met: None,
            },
            sample: SampleInfo {
                window_start: t("2026-08-29T00:00:00Z"),
                window_end: t("2026-08-30T02:00:00Z"),
                topics: 1,
                partitions: 1,
                records_expected: 0,
                records_restored: 0,
                anchor: "head".into(),
                coverage_note: "no capture gap overlaps the sampled window".into(),
            },
            target_diff: TargetDiffSummary {
                collisions: vec![],
                absent: vec![],
                would_create: vec![],
                level: "full".into(),
            },
            integrity: Integrity {
                level: IntegrityLevel::ByteFingerprint,
                result: IntegrityResult::Pass,
                partial_reason: None,
                records_sampled: 0,
                records_sampled_matching: 0,
                mismatches: 0,
                pass_rate_measured: None,
                restored_principal_could_consume: None,
            },
            topic_parity: TopicParity {
                intentionally_deviated: vec![],
                unexpected_divergence: vec![],
            },
            engine_subreport: None,
            evidence: EvidenceInfo {
                version_id: None,
                retain_until: None,
                immutable: false,
                create_only_enforced: true,
            },
            redactions: vec![],
        }
    }

    #[test]
    fn baseline_is_valid() {
        valid_scorecard()
            .validate_invariants()
            .expect("the test baseline itself must satisfy every invariant");
    }

    // --- Global Constraint 18(a), true branch -----------------------------

    #[test]
    fn captured_by_logweir_true_rejects_last_phase_completed_below_neg1() {
        let mut sc = valid_scorecard();
        sc.source.captured_by_logweir = true;
        sc.last_phase_completed = -2;
        // Needed so the true-branch's OTHER checks would pass if reached —
        // isolates the failure to the last_phase_completed arm specifically.
        sc.measured.rpo_source_relative_seconds = Some(0);
        sc.measured.rpo_source_relative_unmeasured_reason = None;
        let err = sc
            .validate_invariants()
            .expect_err("last_phase_completed below -1 must be rejected");
        assert_eq!(
            err.0,
            "source.captured_by_logweir is true but last_phase_completed is below -1"
        );
    }

    #[test]
    fn captured_by_logweir_true_requires_measured_source_relative_rpo() {
        let mut sc = valid_scorecard();
        sc.source.captured_by_logweir = true;
        sc.last_phase_completed = 9;
        sc.measured.rpo_source_relative_seconds = None;
        sc.measured.rpo_source_relative_unmeasured_reason = None;
        let err = sc.validate_invariants().expect_err(
            "a null rpo_source_relative_seconds must be rejected when captured_by_logweir is true",
        );
        assert_eq!(
            err.0,
            "source.captured_by_logweir is true but rpo_source_relative_seconds is null"
        );
    }

    #[test]
    fn captured_by_logweir_true_rejects_a_lingering_unmeasured_reason() {
        let mut sc = valid_scorecard();
        sc.source.captured_by_logweir = true;
        sc.last_phase_completed = 9;
        sc.measured.rpo_source_relative_seconds = Some(0);
        sc.measured.rpo_source_relative_unmeasured_reason = Some("stale reason".into());
        let err = sc.validate_invariants().expect_err(
            "a non-null unmeasured reason must be rejected when captured_by_logweir is true",
        );
        assert_eq!(
            err.0,
            "source.captured_by_logweir is true but an unmeasured reason is present"
        );
    }

    // --- Global Constraint 18(a), false branch ------------------------------

    #[test]
    fn captured_by_logweir_false_rejects_a_source_relative_rpo() {
        let mut sc = valid_scorecard();
        sc.source.captured_by_logweir = false;
        sc.measured.rpo_source_relative_seconds = Some(0);
        // Left as Some(...) so, if the seconds arm were deleted, the reason
        // arm below would not incidentally catch this case too.
        sc.measured.rpo_source_relative_unmeasured_reason =
            Some("source cluster never contacted".into());
        let err = sc
            .validate_invariants()
            .expect_err("a source-relative RPO with the source never contacted must be rejected");
        assert_eq!(
            err.0,
            "rpo_source_relative_seconds is set but the source was never contacted"
        );
    }

    #[test]
    fn captured_by_logweir_false_requires_an_unmeasured_reason() {
        let mut sc = valid_scorecard();
        sc.source.captured_by_logweir = false;
        sc.measured.rpo_source_relative_seconds = None;
        sc.measured.rpo_source_relative_unmeasured_reason = None;
        let err = sc.validate_invariants().expect_err(
            "a null unmeasured reason must be rejected when captured_by_logweir is false",
        );
        assert_eq!(
            err.0,
            "source.captured_by_logweir is false but rpo_source_relative_unmeasured_reason is null"
        );
    }

    // --- Non-negativity of the format's three signed integers ---------------
    //
    // One test per field, each asserting the SPECIFIC message, so collapsing
    // the loop to cover fewer fields makes exactly the dropped field's test
    // fail.

    #[test]
    fn measured_rpo_seconds_may_not_be_negative() {
        let mut sc = valid_scorecard();
        sc.measured.rpo_seconds = Some(-90);
        let err = sc
            .validate_invariants()
            .expect_err("a negative archive-coverage gap must be refused");
        assert_eq!(
            err.0,
            "measured.rpo_seconds is negative (-90); a recovery-point gap of less than zero \
             is not a smaller gap, it is a meaningless one"
        );
    }

    /// Null in v0.1 and written by no code yet — which is exactly why the rule
    /// is here rather than at a call site: `--from-cluster` inherits it.
    #[test]
    fn measured_rpo_source_relative_seconds_may_not_be_negative() {
        let mut sc = valid_scorecard();
        // The Global Constraint 18(a) true-branch, so this test trips the
        // non-negativity arm and not the captured_by_logweir arms.
        sc.source.captured_by_logweir = true;
        sc.measured.rpo_source_relative_seconds = Some(-1);
        sc.measured.rpo_source_relative_unmeasured_reason = None;
        let err = sc
            .validate_invariants()
            .expect_err("a negative source-relative RPO must be refused");
        assert_eq!(
            err.0,
            "measured.rpo_source_relative_seconds is negative (-1); a recovery-point gap of \
             less than zero is not a smaller gap, it is a meaningless one"
        );
    }

    /// This one is adopter input travelling straight from YAML into a signed
    /// document, validated by nothing else anywhere.
    #[test]
    fn objectives_rpo_seconds_may_not_be_negative() {
        let mut sc = valid_scorecard();
        sc.objectives.rpo_seconds = Some(-300);
        let err = sc
            .validate_invariants()
            .expect_err("a negative REQUESTED rpo objective must be refused");
        assert_eq!(
            err.0,
            "objectives.rpo_seconds is negative (-300); a recovery-point gap of less than zero \
             is not a smaller gap, it is a meaningless one"
        );
    }

    #[test]
    fn a_zero_gap_is_valid_the_rule_refuses_only_negatives() {
        let mut sc = valid_scorecard();
        sc.measured.rpo_seconds = Some(0);
        sc.objectives.rpo_seconds = Some(0);
        sc.validate_invariants()
            .expect("zero is a legitimate gap — the archive covers the requested point");
    }

    // --- Finiteness of the format's two float fields ------------------------

    #[test]
    fn objectives_pass_rate_must_be_finite() {
        let mut sc = valid_scorecard();
        sc.objectives.pass_rate = Some(f64::NAN);
        let err = sc.validate_invariants().expect_err(
            "a NaN objectives.pass_rate must be rejected before it can be signed as a bare null",
        );
        assert_eq!(err.0, "objectives.pass_rate is not finite (NaN or +/-Inf)");
    }

    #[test]
    fn integrity_pass_rate_measured_must_be_finite() {
        let mut sc = valid_scorecard();
        // records_sampled_matching / records_sampled with records_sampled == 0
        // is exactly the reachable trigger: a drill that samples zero records.
        sc.integrity.pass_rate_measured = Some(f64::NAN);
        let err = sc.validate_invariants().expect_err(
            "a NaN integrity.pass_rate_measured must be rejected before it can be signed as a bare null",
        );
        assert_eq!(
            err.0,
            "integrity.pass_rate_measured is not finite (NaN or +/-Inf)"
        );
    }

    // --- Global Constraint 12: refuse a higher-major format_version --------

    #[test]
    fn format_version_with_a_higher_major_is_refused() {
        let mut sc = valid_scorecard();
        sc.format_version = "9.9.9".into();
        let err = sc
            .validate_invariants()
            .expect_err("a format_version from a future major must be refused");
        assert_eq!(
            err.0,
            "format_version 9.9.9 has a major version newer than this reader understands \
             (this build knows 1.0.0)"
        );
    }

    #[test]
    fn format_version_with_a_lower_or_equal_major_is_accepted() {
        let mut sc = valid_scorecard();
        sc.format_version = "1.9.9".into();
        sc.validate_invariants()
            .expect("a same-major minor/patch bump must not be refused");
        sc.format_version = "0.9.9".into();
        sc.validate_invariants()
            .expect("an older major must not be refused by this check");
    }

    #[test]
    fn format_version_that_does_not_parse_is_refused() {
        let mut sc = valid_scorecard();
        sc.format_version = "not-a-semver".into();
        let err = sc
            .validate_invariants()
            .expect_err("an unparseable format_version must be refused, not treated as major 0");
        assert_eq!(
            err.0,
            "format_version \"not-a-semver\" is not a parseable semver"
        );
    }
}
