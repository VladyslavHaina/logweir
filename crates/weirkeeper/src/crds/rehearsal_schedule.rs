//! `RehearsalSchedule` — a recurring proof that the archive can actually be
//! restored, on a cron, without a human in the loop.
//!
//! # Sealed except `suspend`, and for a sharper reason than a schedule's
//!
//! `BackupSchedule` seals its spec so an approved policy cannot drift. This one
//! seals it because the **standing authorization binds a digest of this spec**:
//! `templateDigest` is the sha256 of the canonical JSON of `spec` minus
//! `suspend`, recomputed by the controller every slot. A template that could be
//! edited after approval would authorize work nobody approved — change the
//! target cluster, change the topic prefix, and the same signed document now
//! covers a different operation.
//!
//! Because the spec cannot change, the digest cannot drift. A NEW schedule
//! needs a NEW authorization, which is exactly the property a standing
//! approval has to have to be safe.
//!
//! The seal is one object-level rule enumerating every field but `suspend`, for
//! the reason [`super::backup_schedule::SUSPEND_ONLY_RULE`] is — the map form
//! does not compile, and the measurement is recorded in that module's header.
//!
//! # Every bound is in the spec, so the Job is a function of the object
//!
//! `bounds.runnerResources` is here rather than on an annotation because
//! PLAT-06.1's rule is that a Job's shape is a function of the object. That is
//! also what keeps
//! `scratch_mode_and_new_topic_mode_produce_the_same_job_shape` true: resources
//! come from the spec, not from the target mode.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{Condition, LocalRef, SpecRule, Time};

/// What `spec.target.topicPrefix` may be.
///
/// # This is NOT the pattern D3 §4.1 quotes, and the difference is measured
///
/// The decision states the grammar `^rehearsal-[a-z0-9-]*-$` **"after
/// rendering"** (§4.4): the controller appends `<schedule-uid-first-8>-`, so
/// the RENDERED prefix is `rehearsal-3f2a91c7-` and that is what has to end in
/// a hyphen. Putting the rendered pattern on the spec field refuses the
/// decision's own example — a live `kubectl apply` on 2026-09-16 rejected
/// `topicPrefix: "rehearsal-"` with
/// *should match '^rehearsal-[a-z0-9-]*-$'*, because RE2 needs one more
/// character before the trailing `-$` after the literal has been consumed.
///
/// So the spec field carries [`TOPIC_PREFIX_PATTERN`] and the rendered value
/// carries [`RENDERED_TOPIC_PREFIX_PATTERN`]; the rendering step is what turns
/// one into the other, and
/// `the_rendered_rehearsal_prefix_satisfies_the_decisions_grammar` asserts it
/// rather than trusting it.
pub const TOPIC_PREFIX_PATTERN: &str = "^rehearsal-[a-z0-9-]*$";

/// The grammar the RENDERED prefix satisfies — D3 §4.1's text, verbatim.
///
/// It is what keeps teardown inside the runner's `with_scratch_prefix`
/// deletion guard, and what makes two schedules' topic names unmixable: the
/// UID fragment is unique per schedule object.
pub const RENDERED_TOPIC_PREFIX_PATTERN: &str = "^rehearsal-[a-z0-9-]*-$";

/// What the controller appends to [`RehearsalTarget::topic_prefix`] to render
/// the per-schedule prefix: the first eight characters of this object's UID,
/// then a hyphen.
pub const RENDERED_PREFIX_SUFFIX_LEN: usize = 9;

/// The Kafka topic-name grammar.
pub const TOPIC_NAME_PATTERN: &str = super::topic_discovery::TOPIC_NAME_PATTERN;

/// A Kubernetes quantity, in **the grammar `apimachinery` actually accepts**:
/// a decimal number with an optional decimal SI suffix (`n`, `u`, `m`, none,
/// `k`, `M`, `G`, `T`, `P`, `E`), an optional binary SI suffix (`Ki`, `Mi`,
/// `Gi`, `Ti`, `Pi`, `Ei`) or an optional decimal exponent (`e9`, `E-3`).
///
/// # Two corrections, both from review finding F6
///
/// The first form of this pattern was `(m|Ki|Mi|Gi|K|M|G)?`. It **refused legal
/// quantities** an operator would reasonably write — `2Ti` for a scratch
/// restore, `100n`, `1e9` — with a message that says only "should match", and
/// it **accepted `K`**, which Kubernetes does not: the decimal kilo suffix is
/// lowercase `k`, and `Ki` is the binary one.
///
/// # What a pattern still cannot say
///
/// It cannot tell a cpu quantity from a memory one, so `cpu: 5Gi` and
/// `memory: 100m` are both well-formed here and both nonsense. Nor can it
/// express the numeric ceilings D3 §4.1 states (memory limit 8Gi, cpu limit 4):
/// comparing quantities in CEL needs the `quantity` library, whose presence
/// cannot be proved on the 1.29 floor Global Constraint 25 fixes — the same
/// reason D2 R5 uses a regex instead of the CEL URL library. **Both are the
/// rehearsal controller's**, which must refuse an oversized or wrong-unit
/// request with `AuthorizationInvalid` rather than creating the Job.
pub const QUANTITY_PATTERN: &str =
    r"^[0-9]+(\.[0-9]+)?(([KMGTPE]i)|[numkMGTPE]|([eE][-+]?[0-9]+))?$";

/// I1 — the spec is sealed except `suspend`.
///
/// The clause order is the field order of [`RehearsalScheduleSpec`], minus
/// `suspend`.
pub const SUSPEND_ONLY_RULE: &str = "has(self.schedule) == has(oldSelf.schedule) && (!has(self.schedule) || self.schedule == oldSelf.schedule) && has(self.protectionPolicyRef) == has(oldSelf.protectionPolicyRef) && (!has(self.protectionPolicyRef) || self.protectionPolicyRef == oldSelf.protectionPolicyRef) && has(self.point) == has(oldSelf.point) && (!has(self.point) || self.point == oldSelf.point) && has(self.target) == has(oldSelf.target) && (!has(self.target) || self.target == oldSelf.target) && has(self.bounds) == has(oldSelf.bounds) && (!has(self.bounds) || self.bounds == oldSelf.bounds) && has(self.objectives) == has(oldSelf.objectives) && (!has(self.objectives) || self.objectives == oldSelf.objectives) && has(self.authorization) == has(oldSelf.authorization) && (!has(self.authorization) || self.authorization == oldSelf.authorization)";

/// The message [`SUSPEND_ONLY_RULE`] travels with.
pub const SUSPEND_ONLY_MESSAGE: &str = "only spec.suspend is mutable; the standing authorization binds a digest of this spec, so an edit would authorize work nobody approved — create a new RehearsalSchedule and a new authorization";

/// I2 — a rehearsal that accepted unverified evidence would prove the archive
/// is readable and nothing about whether it is trustworthy.
pub const I2_REQUIRE_VERIFIED_EVIDENCE_RULE: &str = "self.point.requireVerifiedEvidence == true";
/// I2's message.
pub const I2_REQUIRE_VERIFIED_EVIDENCE_MESSAGE: &str =
    "spec.point.requireVerifiedEvidence must be true in v1";

/// I3 — a rehearsal needs somewhere to take candidate points from.
pub const I3_POINT_SOURCE_RULE: &str = "has(self.scheduleRefs) || has(self.catalogRef)";
/// I3's message.
pub const I3_POINT_SOURCE_MESSAGE: &str =
    "spec.point names scheduleRefs, catalogRef or both; otherwise there are no candidate points";

/// The rules on `.spec`.
pub const SPEC_RULES: [SpecRule; 2] = [
    SpecRule::new(SUSPEND_ONLY_RULE, SUSPEND_ONLY_MESSAGE),
    SpecRule::new(
        I2_REQUIRE_VERIFIED_EVIDENCE_RULE,
        I2_REQUIRE_VERIFIED_EVIDENCE_MESSAGE,
    ),
];

/// The rules attached below `.spec`.
pub const NESTED_RULES: [(&[&str], &str, &str); 1] =
    [(&["point"], I3_POINT_SOURCE_RULE, I3_POINT_SOURCE_MESSAGE)];

fn default_true() -> bool {
    true
}
fn default_min_age_seconds() -> i32 {
    0
}
fn default_deadline_seconds() -> i32 {
    3600
}
fn default_starting_deadline_seconds() -> i32 {
    3600
}
fn default_records_per_partition() -> i32 {
    25
}
fn default_max_partitions() -> i32 {
    200
}
fn default_replication_factor() -> i32 {
    1
}

/// How a qualifying point is chosen.
///
/// ONE VALUE IN v1, AS AN ENUM RATHER THAN A CEL RULE. A single-variant enum
/// says the same thing the decision's `selection: NewestAvailable # only value
/// in v1 (CEL)` says, at no CEL cost and with a better admission message; a
/// second strategy is then a schema change, which is a reviewable diff.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, Default, JsonSchema, PartialEq, Eq)]
pub enum PointSelection {
    /// The newest point that is available, verified and old enough.
    #[default]
    NewestAvailable,
}

/// Whether a due slot may start while an earlier rehearsal is unfinished.
///
/// ONE VALUE IN v1, for the same reason [`PointSelection`] has one: two
/// concurrent rehearsals against one target cluster would race over the mapped
/// topic names, and the reservation protocol that makes `Forbid` correct is not
/// an optimisation that also happens to make `Allow` safe.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, Default, JsonSchema, PartialEq, Eq)]
pub enum RehearsalConcurrencyPolicy {
    /// Never two at once.
    #[default]
    Forbid,
}

/// Which point to rehearse.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PointSelector {
    /// Candidates from these schedules' `Backup`s.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 16))]
    pub schedule_refs: Option<Vec<LocalRef>>,
    /// Candidates from this `RecoveryCatalog`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog_ref: Option<LocalRef>,
    /// How to choose among them.
    #[serde(default)]
    pub selection: PointSelection,
    /// How old a point must be before it qualifies — a guard against
    /// rehearsing a run that is still being written.
    #[serde(default = "default_min_age_seconds")]
    #[schemars(range(min = 0, max = 2592000))]
    pub min_age_seconds: i32,
    /// The topics to restore. Must be a subset of the chosen point's.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 32), inner(regex(path = "TOPIC_NAME_PATTERN")))]
    pub topics: Option<Vec<String>>,
    /// Whether the point's evidence must verify. `true` in v1 (I2).
    #[serde(default = "default_true")]
    pub require_verified_evidence: bool,
}

/// Where a rehearsal writes.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RehearsalTarget {
    /// The `KafkaCluster` to restore into. Its observed cluster id must be in
    /// the bound `TrustPolicy`'s `allowedTargetClusterIds` — `spec.role` is a
    /// label an adopter picks and is never authority.
    pub cluster_ref: LocalRef,
    /// The topic-name prefix. Rendered as `<prefix><uid-first-8>-`, so it is
    /// unique per schedule object and inside the runner's deletion guard.
    // TEN IS THE SHORTEST: `rehearsal-` itself, which is D3 §4.1's example and
    // was refused by a live API server while this field carried the RENDERED
    // grammar. See `TOPIC_PREFIX_PATTERN` for the measurement.
    #[schemars(regex(path = "TOPIC_PREFIX_PATTERN"), length(min = 10, max = 40))]
    pub topic_prefix: String,
    /// The marker topic that proves this is a scratch cluster.
    #[schemars(regex(path = "TOPIC_NAME_PATTERN"))]
    pub marker_topic: String,
    /// The replication factor for the topics the rehearsal creates.
    #[serde(default = "default_replication_factor")]
    #[schemars(range(min = 1, max = 8))]
    pub replication_factor: i32,
}

/// A cpu/memory pair.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ResourceQuantities {
    /// CPU, as a Kubernetes quantity — `200m`, `2`, `1500m`. The **ceiling**
    /// (4) is the rehearsal controller's, not this pattern's.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(regex(path = "QUANTITY_PATTERN"), length(max = 20))]
    pub cpu: Option<String>,
    /// Memory, as a Kubernetes quantity — `512Mi`, `2Gi`. The **ceiling**
    /// (8Gi) is the rehearsal controller's, not this pattern's.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(regex(path = "QUANTITY_PATTERN"), length(max = 20))]
    pub memory: Option<String>,
}

/// What the runner pod asks for and is capped at.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RunnerResources {
    /// Requests.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requests: Option<ResourceQuantities>,
    /// Limits.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limits: Option<ResourceQuantities>,
}

/// The bounds a rehearsal runs inside.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RehearsalBounds {
    /// Whether two may run at once. `Forbid` in v1.
    #[serde(default)]
    pub concurrency_policy: RehearsalConcurrencyPolicy,
    /// The Job's `activeDeadlineSeconds`.
    #[serde(default = "default_deadline_seconds")]
    #[schemars(range(min = 300, max = 21600))]
    pub deadline_seconds: i32,
    /// How late a missed slot may still start.
    #[serde(default = "default_starting_deadline_seconds")]
    #[schemars(range(min = 60, max = 86400))]
    pub starting_deadline_seconds: i32,
    /// How many records per partition the canary restores.
    #[serde(default = "default_records_per_partition")]
    #[schemars(range(min = 1, max = 1000))]
    pub records_per_partition: i32,
    /// The partition ceiling, enforced when the catalog knows the count.
    #[serde(default = "default_max_partitions")]
    #[schemars(range(min = 1, max = 2000))]
    pub max_partitions: i32,
    /// The runner pod's resources, projected onto the `Restore` it creates.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runner_resources: Option<RunnerResources>,
}

/// What a passing rehearsal has to achieve.
// `Eq` is deliberately absent: `passRate` is an `f64` and `f64` is not `Eq`.
// Modelling it as an integer percentage instead would make `1.0` and the
// scorecard's own fractional pass rate two different spellings of one number,
// which is how a comparison comes to be made in the wrong unit.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RehearsalObjectives {
    /// The recovery-time objective, in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1, max = 86400))]
    pub rto_seconds: Option<i64>,
    /// The share of sampled records that must match, `0.0`..`1.0`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 0.0, max = 1.0))]
    pub pass_rate: Option<f64>,
}

/// How an unattended rehearsal is authorized.
///
/// THE CONTROLLER NEVER MINTS ITS OWN. A reconciler that could authorize its
/// own restores would be the bypass PLAT-19.2 exists to prevent; per-slot human
/// approval would defeat an unattended rehearsal. So there is one signed
/// standing document, carried by an ordinary immutable `Approval`, and it is
/// checked twice — once by the controller before it creates the `Restore`, and
/// once by the runner against the mounted bundle before any client is
/// constructed.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RehearsalAuthorization {
    /// The approval policy (PLAT-19.2). Absent means the legacy governed
    /// policy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_policy_ref: Option<LocalRef>,
    /// The `Approval` carrying the signed standing scope. Its
    /// `spec.subjectRef.kind` is `RehearsalSchedule` and its `spec.planHash` is
    /// this spec's `templateDigest`.
    pub standing_approval_ref: LocalRef,
}

/// `RehearsalSchedule.spec`.
#[derive(kube::CustomResource, Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "logweir.dev",
    version = "v1alpha1",
    kind = "RehearsalSchedule",
    doc = "A recurring recovery rehearsal (ADR 0008 Amendment G): on a cron, restore a qualifying recovery point into an isolated scratch target and score it. `spec.suspend` is the only mutable field, because the standing authorization binds a sha256 of this spec minus `suspend` — an editable template would authorize work nobody approved. The controller deletes no topic ever; teardown is the runner's phase 9, inside a prefix guard.",
    plural = "rehearsalschedules",
    singular = "rehearsalschedule",
    namespaced,
    status = "RehearsalScheduleStatus",
    printcolumn = r#"{"name":"SCHEDULE","type":"string","jsonPath":".spec.schedule"}"#,
    printcolumn = r#"{"name":"SUSPEND","type":"boolean","jsonPath":".spec.suspend"}"#,
    printcolumn = r#"{"name":"TARGET","type":"string","jsonPath":".spec.target.clusterRef.name"}"#,
    printcolumn = r#"{"name":"LAST-OK","type":"date","jsonPath":".status.lastSucceeded.at"}"#,
    printcolumn = r#"{"name":"NEXT","type":"date","jsonPath":".status.nextFireTime"}"#,
    printcolumn = r#"{"name":"AUTHORIZED","type":"string","jsonPath":".status.conditions[?(@.type==\"Authorized\")].status"}"#,
    printcolumn = r#"{"name":"AGE","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct RehearsalScheduleSpec {
    /// A five-field UTC cron expression, the same grammar `BackupSchedule`
    /// uses.
    #[schemars(length(min = 1, max = 128))]
    pub schedule: String,
    /// Stop firing new rehearsals. **The only mutable field.**
    #[serde(default)]
    pub suspend: bool,
    /// The `ProtectionPolicy` whose health these results feed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protection_policy_ref: Option<LocalRef>,
    /// Which point to rehearse.
    pub point: PointSelector,
    /// Where to restore it.
    pub target: RehearsalTarget,
    /// The bounds it runs inside.
    pub bounds: RehearsalBounds,
    /// What a pass means.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub objectives: Option<RehearsalObjectives>,
    /// The standing authorization.
    pub authorization: RehearsalAuthorization,
}

/// The last rehearsal that passed.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RehearsalSuccess {
    /// The `Restore` it ran as.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restore_ref: Option<LocalRef>,
    /// When.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub at: Option<Time>,
    /// Which point.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub point_id: Option<String>,
    /// The evidence verdict for that run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence: Option<String>,
    /// The measured recovery time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rto_seconds: Option<i64>,
}

/// The last rehearsal that failed.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RehearsalFailure {
    /// The `Restore` it ran as.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restore_ref: Option<LocalRef>,
    /// When.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub at: Option<Time>,
    /// Why.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// A slot that did not run, and why.
///
/// A SKIP IS RECORDED, NEVER A SILENT NO-OP. `AuthorizationExpired` and
/// `LeftoverTopics` are the two that matter most: the first is a rehearsal
/// quietly stopping because a signature aged out, and the second is the run
/// that would otherwise adopt topics it did not create.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SkippedSlot {
    /// The DUE slot that was refused, `yyyymmdd-hhmmss` in UTC — never the
    /// instant the controller looked.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slot: Option<String>,
    /// `NoQualifyingPoint`, `TargetUnavailable`, `AuthorizationInvalid`,
    /// `AuthorizationExpired`, `ConcurrencyBlocked`, `TargetBusy`,
    /// `LeftoverTopics` or `PointRetentionInProgress`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// Topics a previous teardown could not remove.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CleanupState {
    /// The names still present, read from the signed teardown attestation.
    /// While this is non-empty the next slot is SKIPPED: the controller
    /// deletes no topic, and a run that adopted them would be operating on
    /// data it did not create.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 256))]
    pub pending_topics: Option<Vec<String>>,
    /// Since when.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub since: Option<Time>,
}

/// `RehearsalSchedule.status`.
#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RehearsalScheduleStatus {
    /// The `metadata.generation` this status was computed from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    /// The last slot this schedule decided: fired, or skipped with
    /// `lastSkipped.slot` naming the same slot. A slot recorded here is never
    /// fired again, so a skipped slot is not run late.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_scheduled_slot: Option<String>,
    /// When the next one is due.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_fire_time: Option<Time>,
    /// The rehearsal running now.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_restore_ref: Option<LocalRef>,
    /// A slot admitted in status whose `Restore` creation is not yet
    /// confirmed — the reservation half of the PLAT-04.1 protocol.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_restore_ref: Option<LocalRef>,
    /// The last pass.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_succeeded: Option<RehearsalSuccess>,
    /// The last failure.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_failed: Option<RehearsalFailure>,
    /// The last skip.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_skipped: Option<SkippedSlot>,
    /// Topics a teardown left behind.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cleanup: Option<CleanupState>,
    /// The digest the standing authorization is checked against, recomputed
    /// every slot from this object's own sealed spec.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub template_digest: Option<String>,
    /// The condition set: `Ready`, `Authorized`, `RehearsalHealthy`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<Condition>>,
}
