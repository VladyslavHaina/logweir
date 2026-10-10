//! `RetentionPolicy` — what may be removed from one destination, and under
//! whose authority.
//!
//! # Why deletion is its own kind (ADR 0008 Amendment G, and Amendment H)
//!
//! "Who may configure storage" and "who may delete data" must be separable
//! grants. Putting retention on a `BackupDestination` would make them the same
//! `update`. So this is its own namespaced kind, with its own RBAC, its own
//! approved-plan state and its own conditions.
//!
//! # `mode` is the boundary, and `Report` is the default
//!
//! Tag 1's statement — *no Logweir component holds any delete capability
//! against object storage* — becomes version-scoped by Amendment H: it is true
//! wherever `mode != Enforce`. In `Report` (the default) and
//! `ExternalLifecycle` nothing is deleted by Logweir at all; `Enforce` opts in
//! to a separately linked, separately credentialed worker that may delete under
//! an explicitly configured archive prefix, **never under `logweir/`**, only
//! from an administrator-approved plan, and only with an attributable record —
//! create-only under `logweir/`, written by a credential that cannot delete
//! there, and **unsigned in this build** (`docs/stability.md`).
//! `logweir-store` stays delete-free and the control plane stays delete-free
//! either way.
//!
//! `ExternalLifecycle` is a **declaration**, not an enforcement: it records
//! that a bucket lifecycle rule exists so the console can stop claiming
//! retention is unenforced, and `status.guarantees` marks it
//! `ProviderEnforcedUnverified` rather than `LogweirEnforced`. Logweir does not
//! read the provider's rule and does not claim it is in force.
//!
//! # What is immutable, and why those three
//!
//! `destinationRef`, `catalogRef` and `scope` are sealed. A policy that could
//! be re-pointed is a policy whose approved plan — a list of point ids with a
//! digest — could be applied to a different bucket. The mutable half is the
//! part an administrator is expected to change: the rules, the holds, the mode
//! and the approved plan digest.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{Condition, LocalRef, SpecRule, Time};

/// A DNS-1123 subdomain — a Secret name.
pub const OBJECT_NAME_PATTERN: &str = super::kafka_cluster::OBJECT_NAME_PATTERN;
/// `sha256:<64 lowercase hex>`.
pub const SHA256_PATTERN: &str = super::preflight::SHA256_PATTERN;

/// K1 — the three fields that decide WHERE deletion could happen are
/// immutable.
pub const IMMUTABLE_TARGET_RULE: &str = "has(self.destinationRef) == has(oldSelf.destinationRef) && (!has(self.destinationRef) || self.destinationRef == oldSelf.destinationRef) && has(self.catalogRef) == has(oldSelf.catalogRef) && (!has(self.catalogRef) || self.catalogRef == oldSelf.catalogRef) && has(self.scope) == has(oldSelf.scope) && (!has(self.scope) || self.scope == oldSelf.scope)";

/// The message [`IMMUTABLE_TARGET_RULE`] travels with.
pub const IMMUTABLE_TARGET_MESSAGE: &str = "spec.destinationRef, spec.catalogRef and spec.scope are immutable: an approved deletion plan names point ids, and a policy that could be re-pointed would apply that plan to a different location";

/// K2 — `Enforce` needs its enforcement block, and nothing else may carry one.
pub const K2_ENFORCEMENT_IFF_ENFORCE_RULE: &str =
    "has(self.enforcement) == (self.mode == 'Enforce')";
/// K2's message.
pub const K2_ENFORCEMENT_IFF_ENFORCE_MESSAGE: &str =
    "spec.enforcement is required for mode Enforce and forbidden otherwise";

/// K3 — `ExternalLifecycle` needs its declaration, and nothing else may carry
/// one.
pub const K3_EXTERNAL_IFF_EXTERNAL_RULE: &str =
    "has(self.externalLifecycle) == (self.mode == 'ExternalLifecycle')";
/// K3's message.
pub const K3_EXTERNAL_IFF_EXTERNAL_MESSAGE: &str =
    "spec.externalLifecycle is required for mode ExternalLifecycle and forbidden otherwise";

/// K4 — the enforcement scope may never be Logweir's own evidence root.
///
/// The LAST rail, not the only one: the runner refuses it too, and
/// `logweir-store` cannot delete at all. It is stated here so the refusal
/// happens at admission, where an operator sees it, rather than at 04:17 in a
/// Job log.
pub const K4_SCOPE_IS_NOT_EVIDENCE_RULE: &str =
    "!self.prefix.matches('(^|/)logweir(/|$)') && self.prefix != ''";
/// K4's message.
pub const K4_SCOPE_IS_NOT_EVIDENCE_MESSAGE: &str = "spec.scope.prefix must be a non-empty archive prefix and may never name logweir/, which is the evidence root no retention run may delete under";

/// The rules on `.spec`.
pub const SPEC_RULES: [SpecRule; 3] = [
    SpecRule::new(IMMUTABLE_TARGET_RULE, IMMUTABLE_TARGET_MESSAGE),
    SpecRule::new(
        K2_ENFORCEMENT_IFF_ENFORCE_RULE,
        K2_ENFORCEMENT_IFF_ENFORCE_MESSAGE,
    ),
    SpecRule::new(
        K3_EXTERNAL_IFF_EXTERNAL_RULE,
        K3_EXTERNAL_IFF_EXTERNAL_MESSAGE,
    ),
];

/// The rules attached below `.spec`.
pub const NESTED_RULES: [(&[&str], &str, &str); 1] = [(
    &["scope"],
    K4_SCOPE_IS_NOT_EVIDENCE_RULE,
    K4_SCOPE_IS_NOT_EVIDENCE_MESSAGE,
)];

fn default_min_usable_points() -> i32 {
    3
}
fn default_true() -> bool {
    true
}
fn default_plan_max_age_seconds() -> i32 {
    3600
}
fn default_max_deletions_per_run() -> i32 {
    50
}
fn default_max_objects_per_run() -> i32 {
    20_000
}
fn default_deadline_seconds() -> i32 {
    1800
}

/// What this policy is allowed to do.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, Default, JsonSchema, PartialEq, Eq)]
pub enum RetentionMode {
    /// Evaluate and REPORT. Logweir deletes nothing. The default, and the only
    /// mode in which the tag-1 statement "no Logweir component holds any
    /// delete capability against object storage" is unqualified.
    #[default]
    Report,
    /// Opt in to the retention worker (Amendment H).
    Enforce,
    /// Declare that the bucket's own lifecycle rule does the deleting. Logweir
    /// neither reads nor verifies it, and says so in `status.guarantees`.
    ExternalLifecycle,
}

/// The object-store provider whose lifecycle rule is being declared.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
#[schemars(rename_all = "lowercase")]
pub enum LifecycleProvider {
    /// Amazon S3 or an S3-compatible store.
    S3,
    /// Google Cloud Storage.
    Gcs,
    /// Azure Blob Storage.
    Azure,
}

/// The prefix deletion may ever touch. Immutable (K1), and never `logweir/`
/// (K4).
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RetentionScope {
    /// The archive prefix, matching the destination's bucket prefix.
    #[schemars(length(min = 1, max = 512))]
    pub prefix: String,
}

/// What to keep.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RetentionRules {
    /// Keep the most recent N points.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1, max = 100000))]
    pub keep_last: Option<i32>,
    /// Keep points newer than N days.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1, max = 36500))]
    pub keep_days: Option<i32>,
    /// The floor that survives every other rule: this many USABLE points are
    /// kept whatever `keepLast` and `keepDays` say. A retention policy that can
    /// empty an archive is a retention policy that will.
    #[serde(default = "default_min_usable_points")]
    #[schemars(range(min = 1, max = 1000))]
    pub min_usable_points: i32,
}

/// A point that may not be removed, whatever the rules say.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LegalHold {
    /// The point id.
    #[schemars(length(min = 1, max = 253))]
    pub point_id: String,
    /// Why it is held — a case reference, an auditor, a regulation.
    #[schemars(length(min = 1, max = 253))]
    pub reason: String,
    /// When the hold lapses. Absent means it does not.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub until: Option<Time>,
}

/// A declared, unverified provider lifecycle rule.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ExternalLifecycle {
    /// Whose rule it is.
    pub provider: LifecycleProvider,
    /// The rule's own id in that provider's console, so an auditor can find it.
    #[schemars(length(min = 1, max = 253))]
    pub rule_id: String,
    /// The expiry the rule declares.
    #[schemars(range(min = 1, max = 36500))]
    pub expiration_days: i32,
    /// The prefix the rule covers.
    #[schemars(length(min = 1, max = 512))]
    pub prefix: String,
}

/// The settings that make `Enforce` a bounded, attributable operation.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Enforcement {
    /// The delete-capable credential, mounted ONLY into retention Jobs. It is
    /// a separate Secret from every other grant on purpose: the blast radius of
    /// this key is exactly one prefix in one bucket.
    pub credential_secret_ref: LocalRef,
    /// A five-field UTC cron expression for the evaluation and enforcement
    /// cadence.
    #[schemars(length(min = 1, max = 128))]
    pub schedule: String,
    /// Whether an administrator must approve the plan before anything is
    /// deleted. Default `true`.
    #[serde(default = "default_true")]
    pub require_approved_plan: bool,
    /// The digest of the plan an administrator approved after reviewing the
    /// preview. Mutable — approving a new plan is the routine — and the ONLY
    /// thing that authorizes a deletion run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(regex(path = "SHA256_PATTERN"))]
    pub approved_plan_sha256: Option<String>,
    /// How long an approved plan stays usable. A plan older than this is
    /// re-previewed rather than executed, because the archive has moved on.
    #[serde(default = "default_plan_max_age_seconds")]
    #[schemars(range(min = 300, max = 86400))]
    pub plan_max_age_seconds: i32,
    /// The per-run ceiling on points.
    #[serde(default = "default_max_deletions_per_run")]
    #[schemars(range(min = 1, max = 500))]
    pub max_deletions_per_run: i32,
    /// The per-run ceiling on object keys.
    #[serde(default = "default_max_objects_per_run")]
    #[schemars(range(min = 1, max = 200000))]
    pub max_objects_per_run: i32,
    /// The Job's `activeDeadlineSeconds`.
    #[serde(default = "default_deadline_seconds")]
    #[schemars(range(min = 60, max = 21600))]
    pub deadline_seconds: i32,
}

/// `RetentionPolicy.spec`.
#[derive(kube::CustomResource, Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "logweir.dev",
    version = "v1alpha1",
    kind = "RetentionPolicy",
    doc = "What may be removed from one destination, and under whose authority (ADR 0008 Amendments G and H). `spec.destinationRef`, `spec.catalogRef` and `spec.scope` are immutable, because an approved deletion plan names point ids and a re-pointable policy would apply it elsewhere. `mode: Report` is the default and deletes nothing; `Enforce` opts in to a separately credentialed worker bounded to an explicit archive prefix, never `logweir/`, only from an approved plan, and only with an attributable record written create-only under `logweir/` by a credential that cannot delete. That record is UNSIGNED in this build: it is tamper-evident against the retention principal and against anyone who can only delete, and not against a principal that can write under `logweir/` (docs/stability.md).",
    plural = "retentionpolicies",
    singular = "retentionpolicy",
    namespaced,
    status = "RetentionPolicyStatus",
    printcolumn = r#"{"name":"DESTINATION","type":"string","jsonPath":".spec.destinationRef.name"}"#,
    printcolumn = r#"{"name":"MODE","type":"string","jsonPath":".spec.mode"}"#,
    printcolumn = r#"{"name":"ENFORCEMENT","type":"string","jsonPath":".status.enforcement"}"#,
    printcolumn = r#"{"name":"CANDIDATES","type":"integer","jsonPath":".status.lastEvaluation.candidateCount"}"#,
    printcolumn = r#"{"name":"HELD-BACK","type":"integer","jsonPath":".status.lastEvaluation.truncatedByCap"}"#,
    printcolumn = r#"{"name":"EVALUATED","type":"date","jsonPath":".status.lastEvaluation.at"}"#,
    printcolumn = r#"{"name":"AGE","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct RetentionPolicySpec {
    /// The destination this policy covers. Immutable (K1).
    pub destination_ref: LocalRef,
    /// The catalog the evaluation reads. Immutable (K1). Evaluation reads the
    /// catalog VIEW, never a second bucket walk, so a schedule writing
    /// elsewhere is never reported against the wrong location.
    pub catalog_ref: LocalRef,
    /// The prefix deletion may ever touch. Immutable (K1).
    pub scope: RetentionScope,
    /// What to keep.
    pub rules: RetentionRules,
    /// Points that may not be removed. Mutable — a legal hold arrives on a
    /// Tuesday.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 256))]
    pub holds: Option<Vec<LegalHold>>,
    /// What this policy is allowed to do. `Report` by default.
    #[serde(default)]
    pub mode: RetentionMode,
    /// The declared provider lifecycle rule, for `ExternalLifecycle` (K3).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_lifecycle: Option<ExternalLifecycle>,
    /// The enforcement settings, for `Enforce` (K2).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enforcement: Option<Enforcement>,
}

/// One point the evaluation would remove.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RetentionCandidate {
    /// The point id.
    pub point_id: String,
    /// Why it qualifies — `OlderThanKeepDays` or `BeyondKeepLast`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Its capture start.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery_point_at: Option<Time>,
    /// How many object keys it owns.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub objects: Option<i64>,
    /// How many bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bytes: Option<i64>,
}

/// One point the rules selected and something else protected.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ProtectedPoint {
    /// The point id.
    pub point_id: String,
    /// `ActiveRestore`, `MinUsablePoints`, `LegalHold`, `SharedSegment`,
    /// `Hold` or `Unknown`. **`Unknown` protects**: a point the evaluation
    /// could not classify is never a deletion candidate.
    pub reason: String,
}

/// One point or key the evaluation could not classify.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SkippedEntry {
    /// The point id, when there was one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub point_id: Option<String>,
    /// The object key, when there was not.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    /// `Unreadable`, `UnsupportedFormat` or `Conflict`.
    pub reason: String,
}

/// What the last evaluation found. **Nothing here was deleted.**
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RetentionEvaluation {
    /// When an evaluation first reached the findings below. Later evaluations
    /// that find the same keep this instant, so it is when the findings last
    /// changed, not when the controller last looked.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub at: Option<Time>,
    /// How many points were considered: every point of this destination in
    /// the catalog view the evaluation read. Each is counted once, in
    /// `keptCount`, `candidateCount`, `truncatedByCap` or `skippedCount`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub points_evaluated: Option<i64>,
    /// How many points stay: the rules keep them, or something protects them.
    /// Never a point the per-run ceiling held back. Absent on an evaluation
    /// written by a controller that did not record it; `kept` may then also
    /// list points the ceiling held back.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kept_count: Option<i64>,
    /// How many points THIS plan would remove: at most the per-run ceiling,
    /// and not every point the rules would remove when `truncatedByCap` is
    /// above 0.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_count: Option<i64>,
    /// How many more points the rules would remove, that nothing protects and
    /// that the per-run ceiling (`maxDeletionsPerRun`) left out of this plan.
    /// They are due, not kept: no run deletes them until a later plan names
    /// them, and a backup set that more due points name than the ceiling is
    /// named by no plan until the ceiling is raised. `0` means this plan is
    /// everything the rules would remove. Absent means not recorded (an older
    /// controller), never 0.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub truncated_by_cap: Option<i64>,
    /// The per-run ceiling this evaluation applied:
    /// `spec.enforcement.maxDeletionsPerRun`, or 50 for a policy with no
    /// `spec.enforcement` (a preview is bounded as a run would be).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_deletions_per_run: Option<i64>,
    /// Whether the catalog said the view this evaluation read does not hold
    /// every point of the archive: the view is a window
    /// (`RecoveryCatalog.status.truncated`), or the catalog's walk had not
    /// finished (`status.cursor.complete: false`). The points outside the
    /// view were not evaluated, are in none of the counts above, and are
    /// never candidates while they stay outside it. `false` means the catalog
    /// said its walk finished and its view holds every point it counted.
    /// Absent means the catalog did not say, never `false`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub view_incomplete: Option<bool>,
    /// How many points something protected: the length of `protected`
    /// before it was cut at 500. Absent on an evaluation written by a
    /// controller that did not record it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protected_count: Option<i64>,
    /// How many points could not be classified: the length of `skipped`
    /// before it was cut at 500. Absent on an evaluation written by a
    /// controller that did not record it; `skipped` is then whole.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skipped_count: Option<i64>,
    /// The point ids that stay, the first 500. `keptCount` is their number.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 500))]
    pub kept: Option<Vec<String>>,
    /// The points this plan would remove, the first 500.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 500))]
    pub candidates: Option<Vec<RetentionCandidate>>,
    /// The points something protected, the first 500.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 500))]
    pub protected: Option<Vec<ProtectedPoint>>,
    /// What could not be classified, the first 500.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 500))]
    pub skipped: Option<Vec<SkippedEntry>>,
    /// The `ConfigMap` holding the full plan.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_ref: Option<LocalRef>,
    /// The plan's digest — what an administrator approves by copying into
    /// `spec.enforcement.approvedPlanSha256`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_sha256: Option<String>,
    /// When the plan stops being usable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_expires_at: Option<Time>,
}

/// The closed accounting of one evaluation — [`RetentionEvaluation::accounting`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetentionAccounting {
    /// `pointsEvaluated`.
    pub points_evaluated: i64,
    /// `keptCount`: the points that stay.
    pub kept: i64,
    /// `candidateCount`: the points this plan would remove.
    pub candidates: i64,
    /// `truncatedByCap`: due, and held back by the per-run ceiling.
    pub held_back: i64,
    /// `skippedCount`, or the length of `skipped` where an older controller
    /// wrote no count.
    pub skipped: i64,
}

/// The CRD's `maxItems` on each `status.lastEvaluation` list (FX-39). The
/// controller writes the first this many entries of each list and the full
/// number beside it, so the API server never refuses the status write.
pub const EVALUATION_LIST_BOUND: usize = 500;

impl RetentionEvaluation {
    /// The four counts every surface reads (FX-22), **or `None` when this
    /// block does not carry an accounting that closes.**
    ///
    /// `None` when `pointsEvaluated`, `keptCount`, `candidateCount` or
    /// `truncatedByCap` is absent — an evaluation written before the
    /// controller recorded what the per-run ceiling held back, whose `kept`
    /// list may include such points — and `None` when
    ///
    /// ```text
    /// pointsEvaluated = keptCount + candidateCount + truncatedByCap + skippedCount
    /// ```
    ///
    /// (`skippedCount` absent: the length of `skipped`, which a controller
    /// that wrote no count wrote whole, or the API server refused the write).
    ///
    /// does not hold. Every evaluation this controller writes satisfies it, so
    /// one that does not is two writers' numbers in one block: a merge patch
    /// from an OLDER controller (after a rollback of the image alone) rewrites
    /// `pointsEvaluated`, `candidateCount` and the lists and cannot remove the
    /// two counts it does not know, which then describe an earlier archive.
    /// Absent means not recorded, and a stale count is not a count: a reader
    /// that gets `None` says so, and never derives "kept" from the list.
    ///
    /// **And `None` when the `kept` list is not as long as `keptCount`**
    /// (FX-22 review M2). The sum alone does not catch that rollback while
    /// the archive stands still: the older controller writes its own `kept`
    /// list — the kept points AND the ones the ceiling held back, 321 ids for
    /// `keepLast: 10` over 371 points — beside `keptCount: 10` and
    /// `truncatedByCap: 311`, which it cannot remove and which still add up
    /// to an unchanged `pointsEvaluated`. A reader that trusted the sum
    /// published 311 due points under the name `kept` until the next point
    /// landed or left. This controller writes the list whole and the count
    /// from the same vector in one patch, so a list of another length is two
    /// writers', whatever the sum says.
    ///
    /// THE LISTS ARE CUT AT [`EVALUATION_LIST_BOUND`] (FX-39), so the sum
    /// reads the counts, and the `kept` list is held to
    /// `min(keptCount, 500)`.
    #[must_use]
    pub fn accounting(&self) -> Option<RetentionAccounting> {
        let list_len = |len: usize| i64::try_from(len).ok();
        let accounting = RetentionAccounting {
            points_evaluated: self.points_evaluated?,
            kept: self.kept_count?,
            candidates: self.candidate_count?,
            held_back: self.truncated_by_cap?,
            skipped: match self.skipped_count {
                Some(count) => count,
                None => list_len(self.skipped.as_ref().map_or(0, Vec::len))?,
            },
        };
        let parts = [
            accounting.kept,
            accounting.candidates,
            accounting.held_back,
            accounting.skipped,
        ];
        if parts.iter().any(|n| *n < 0) {
            return None;
        }
        // THE LIST THAT IS CALLED `kept` IS THE RECORDED COUNT LONG, UP TO THE
        // BOUND (review M2) — see the doc comment for the rollback this
        // refuses.
        let listed = list_len(self.kept.as_ref().map_or(0, Vec::len))?;
        if listed != accounting.kept.min(list_len(EVALUATION_LIST_BOUND)?) {
            return None;
        }
        let sum = parts.iter().try_fold(0i64, |acc, n| acc.checked_add(*n))?;
        (sum == accounting.points_evaluated).then_some(accounting)
    }
}

/// One point a run could not delete.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct FailedDeletion {
    /// The point id.
    pub point_id: String,
    /// The closed-vocabulary code. Never a raw object-store error body.
    pub code: String,
}

/// What the last enforcement run actually did.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RetentionEnforcementRun {
    /// The run id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    /// When it started.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<Time>,
    /// When it finished.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<Time>,
    /// The plan digest it executed — the one an administrator approved.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_sha256: Option<String>,
    /// The points it removed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 500))]
    pub deleted: Option<Vec<String>>,
    /// The points it could not.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 500))]
    pub failed: Option<Vec<FailedDeletion>>,
    /// How many object keys were removed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub objects_deleted: Option<i64>,
    /// The signed, attributable record of the deletion. **Written under
    /// `logweir/`, which this run may never delete from.**
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub record_key: Option<String>,
    /// That record's digest.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub record_sha256: Option<String>,
    /// The run's exit code.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
}

/// The points one run has claimed, so a second run cannot claim them too.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RetentionLease {
    /// The run holding it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    /// The points it covers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 500))]
    pub point_ids: Option<Vec<String>>,
    /// When it was taken.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acquired_at: Option<Time>,
    /// When it lapses.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<Time>,
}

/// Which guarantees are actually in force, and by whom.
///
/// EACH ONE IS `LogweirEnforced`, `ProviderEnforcedUnverified` OR
/// `NotEnforced`. The middle value is the honest one for a declared bucket
/// lifecycle rule: Logweir did not read it, did not verify it and will not
/// claim it works.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RetentionGuarantees {
    /// That points older than the limit actually go.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub age_expiry: Option<String>,
    /// That the usable floor is respected.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_usable_points: Option<String>,
    /// That a point being restored right now is not removed under it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_restore_protection: Option<String>,
    /// That a segment two points share is not removed with one of them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shared_segments: Option<String>,
    /// That a legal hold is honoured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub legal_hold: Option<String>,
}

/// `RetentionPolicy.status`.
#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RetentionPolicyStatus {
    /// The `metadata.generation` this status was computed from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    /// `RecommendationOnly`, `LogweirWorker` or `ExternalLifecycleDeclared` —
    /// what is actually happening, as opposed to what `spec.mode` asks for.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enforcement: Option<String>,
    /// Which guarantees are in force, and by whom.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub guarantees: Option<RetentionGuarantees>,
    /// What the last evaluation found. Nothing in it was deleted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_evaluation: Option<RetentionEvaluation>,
    /// What the last enforcement run did.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_enforcement: Option<RetentionEnforcementRun>,
    /// The points a run has claimed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease: Option<RetentionLease>,
    /// FX-20: the binding `spec.enforcement.credentialSecretRef`'s Secret must
    /// carry under its `logweir-binding` key —
    /// `v1:<policy uid>:sha256:<digest of the destination's route and the
    /// scope>` — for the worker to use the delete-capable key at all. Public
    /// (a UID and a digest, never a credential). A Secret without it, or with
    /// another policy's, or bound before the destination of this name was
    /// re-created at another route, is refused before any handle is built
    /// (`CredentialBindingMismatch`). Absent without `spec.enforcement`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_binding: Option<String>,
    /// How many runs have failed in a row.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consecutive_run_failures: Option<i64>,
    /// The condition set: `Ready`, `Evaluated`, `Enforced`,
    /// `ExternalLifecycleConflict`, `EnforcementDegraded`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<Condition>>,
}
