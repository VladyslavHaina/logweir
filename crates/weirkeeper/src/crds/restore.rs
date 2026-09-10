//! `Restore` — one restore run, and the only kind a drill is.
//!
//! `Drill` IS NOT A KIND. A drill is a `Restore` whose `spec.target.mode` is
//! `scratch` (Global Constraint 34). A `Restore` only ever writes a *new*
//! topic, so restoring into production is non-destructive by construction, and
//! the drill's only distinguishing behaviour is a scratch target plus phase-9
//! teardown. Two kinds would duplicate the whole exit-code-to-condition table
//! for one boolean.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{ArchiveRef, Condition, EvidenceVerification, LocalRef, Time};

/// `target.mode`'s enum, fixed byte for byte at this task.
///
/// THE SPELLINGS ARE THE CONTRACT. They are exactly `["scratch", "newTopic"]`,
/// in that order, and Task 9b's late-binding agreement test
/// `the_crd_mode_enum_and_target_mode_agree` (which lives in
/// `tests/crd_shape.rs` and is owned by that task) asserts BYTE equality
/// between this enum and the Rust `TargetMode` that lands in a later slot.
/// This task consumes nothing from Task 9b (interface **I33**).
#[derive(Deserialize, Serialize, Clone, Copy, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
#[schemars(rename_all = "camelCase")]
pub enum TargetMode {
    /// A drill. The target is proved to be a scratch cluster and phase 9 tears
    /// down the topics this run created — the only deletion tag 1 performs
    /// anywhere (Global Constraint 19).
    Scratch,
    /// A real restore into a new topic beside the existing one. Nothing is
    /// overwritten and nothing is deleted; repointing consumers is tag 2's
    /// `Switchover` and is not in this group.
    NewTopic,
}

/// How restored topics are named.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TopicNaming {
    /// Prepended to each source topic name to form the new topic's name. The
    /// result is what `status.newTopics` records, and — for `mode: scratch` —
    /// what phase 9 tears down.
    pub prefix: String,
}

/// Where the restore writes, and in which mode.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RestoreTarget {
    /// The `KafkaCluster` to write to, in this namespace.
    pub cluster_ref: LocalRef,
    /// `scratch` for a drill, `newTopic` for a real restore.
    pub mode: TargetMode,
    /// How the new topics are named.
    pub topic_naming: TopicNaming,
}

/// The requested objectives, lifted from the scorecard.
///
/// INTERFACE **I34**, first half. Produced by Task 20 from the scorecard's own
/// `objectives` block (`crates/logweir-core/src/scorecard.rs`), camelCased by
/// the CRD derive. Spec §8 requires the UI to render both this block and
/// [`Integrity::partial_reason`], and no other status field carries them —
/// `measured` is what the run achieved, this is what was asked for.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Objectives {
    /// The requested maximum recovery time, seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rto_seconds: Option<i64>,
    /// The requested maximum archive-coverage gap, seconds. Never negative: a
    /// negative allowed gap is not a stricter objective, it is an
    /// unsatisfiable one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rpo_seconds: Option<i64>,
    /// The requested record match rate, 0.0 to 1.0.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pass_rate: Option<f64>,
    /// The aggregate verdict over whichever objectives above are non-null.
    /// `true` — every non-null objective was met. `false` — at least one was
    /// missed. Absent — a `passRate` objective was requested but the MEASURED
    /// rate could not be computed, so the aggregate is unmeasurable rather
    /// than satisfied.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub met: Option<bool>,
}

/// What the run actually achieved.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Measured {
    /// Wall-clock seconds from the start of the restore to a verified target.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rto_seconds: Option<i64>,
    /// The measured archive-coverage gap, seconds. Non-negative.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rpo_seconds: Option<i64>,
}

/// How thoroughly the restore was checked, and what that check found.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Integrity {
    /// `byte-fingerprint`, `consume-only` or `not-attempted`. The scorecard's
    /// own kebab-case spellings, carried through unchanged so the badge the UI
    /// renders and the document an auditor reads say the same word.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub level: Option<String>,
    /// `pass`, `partial` or `fail`. `partial` is a selection whose evidence
    /// was inconclusive — something in the sample was not examined — and is
    /// NOT a pass.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<String>,
    /// INTERFACE **I34**, second half. Why the integrity result is `partial`,
    /// naming itself: an archive returning zero or short fingerprints for a
    /// selection; a (topic, partition, window) the manifest claims exists but
    /// no segment matches; a pre-0.21 segment carrying no sha256; a
    /// consume-only selection whose target partition gave back less than the
    /// manifest claims. Produced by Task 20 from the scorecard's
    /// `integrity.partial_reason`. Spec §8 requires the UI to render it — a
    /// `partial` with no reason is a badge an auditor cannot act on.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partial_reason: Option<String>,
}

/// What phase 0 found out about the target topics before writing anything.
///
/// Guard **G-TS**. Not a scorecard field: it is returned by phase 0 in
/// `RestoreOutcome`.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TopicPreflight {
    /// The target topic's `message.timestamp.type` — `CreateTime` or
    /// `LogAppendTime`. `LogAppendTime` rewrites every restored record's
    /// timestamp to the moment of the restore, which makes a point-in-time
    /// claim unverifiable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp_type: Option<String>,
    /// The target topic's `retention.ms`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention_ms: Option<i64>,
    /// The oldest timestamp the target will accept without rejecting or
    /// rewriting the record, derived from `retentionMs`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp_bound: Option<i64>,
}

/// Where the signed evidence is, and what the controller made of it.
///
/// KEYS AND DIGESTS ONLY, NEVER CONTENT — as on `Backup`.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RestoreEvidence {
    /// The object key of the signed scorecard.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scorecard_key: Option<String>,
    /// The sha256 of the scorecard at `scorecardKey`, as
    /// `sha256:<lowercase hex>` — the one digest spelling this corpus uses
    /// everywhere (`logweir_core::ids::sha256_prefixed`), so a value read off
    /// this field and a value read out of a signed document compare as
    /// strings. **COMPUTED by the controller over the bytes it fetched**, not
    /// copied: a document cannot carry its own digest.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scorecard_sha256: Option<String>,
    /// The object key of the scorecard's detached DSSE sidecar.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sidecar_key: Option<String>,
    /// The object key of the offset report. Tag 1 renders the report and
    /// applies nothing (Global Constraint 35).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset_report_key: Option<String>,
    /// The sha256 of the offset report, as `sha256:<lowercase hex>`.
    /// **COPIED from the signed scorecard's own
    /// `evidence.offset_report_sha256`**, never recomputed: the report's
    /// digest is inside the bytes the signature covers, so recomputing it
    /// would fetch a second object to answer a question the first one already
    /// answers — and would report a mismatch as agreement.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset_report_sha256: Option<String>,
    /// What `weirkeeper` recorded when it verified the scorecard.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verification: Option<EvidenceVerification>,
}

/// `Restore.spec`.
#[derive(kube::CustomResource, Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "logweir.dev",
    version = "v1alpha1",
    kind = "Restore",
    doc = "One restore run, executed as a Job. A drill is a Restore with `spec.target.mode: scratch`; there is no separate Drill kind. A Restore only ever writes a NEW topic, so it is non-destructive by construction. `spec` is immutable — the approval binds these exact bytes.",
    plural = "restores",
    singular = "restore",
    namespaced,
    status = "RestoreStatus",
    printcolumn = r#"{"name":"MODE","type":"string","jsonPath":".spec.target.mode","description":"scratch is a drill"}"#,
    printcolumn = r#"{"name":"PHASE","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"EXIT","type":"integer","jsonPath":".status.exitCode","description":"0 pass, 1 operational, 2 not-a-pass, 3 refused, 4 signing failed"}"#,
    printcolumn = r#"{"name":"REASON","type":"string","jsonPath":".status.reason","description":"the terminal or current condition reason - ApprovalNotVerified, PlanHashMismatch, GuardRefused, Ok, ...; NOT exitReason, which is `operational` for every admission refusal"}"#,
    printcolumn = r#"{"name":"OUTCOME","type":"string","jsonPath":".status.outcome"}"#,
    printcolumn = r#"{"name":"INTEGRITY","type":"string","jsonPath":".status.integrity.result"}"#,
    printcolumn = r#"{"name":"RTO","type":"integer","jsonPath":".status.measured.rtoSeconds"}"#,
    printcolumn = r#"{"name":"SIGNED","type":"string","jsonPath":".status.evidence.verification.result","description":"green needs this Valid AND outcome pass"}"#,
    printcolumn = r#"{"name":"AGE","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct RestoreSpec {
    /// The restore plan, as the exact bytes the approver signed.
    ///
    /// AN OPAQUE STRING, NEVER A MODELLED SCHEMA. `planHash` binds exact spec
    /// bytes, the API server normalises YAML, and a typed round-trip would
    /// silently invalidate every approval. The UI produces these bytes
    /// client-side and shows their sha256 before submitting; the controller
    /// hashes exactly what is here.
    pub plan_bytes: String,
    /// The `Approval` that authorises this restore, in this namespace. The
    /// approval's `planHash` must equal the sha256 of `planBytes` above, and
    /// its `subjectRef` must name this object.
    pub approval_ref: LocalRef,
    /// The archive to restore from.
    pub source_archive: ArchiveRef,
    /// The backup set inside `sourceArchive` — a `backupId`.
    pub backup_set_ref: String,
    /// The point in time to restore to. Records after it are not restored.
    pub point_in_time: Time,
    /// Where the restore writes, and in which mode.
    pub target: RestoreTarget,
    /// The Job's `activeDeadlineSeconds`.
    pub deadline_seconds: i64,
}

/// `Restore.status`.
#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RestoreStatus {
    /// Where the run is: `Pending`, `Running`, `Succeeded`, `Failed`,
    /// `Refused`. Task 20 owns the phase vocabulary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
    /// The runner's exit code (Global Constraint 11). `2` is the most valuable
    /// result the tool produces: the drill ran, it was measured, and the
    /// backup did not meet its objective — a scorecard WAS written and
    /// signed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    /// The terminal state that refused the run, for exit 3 — `Expired`,
    /// `WindowNotCovered`, `ApprovalNotReceived`, `OrphanedScorecard`,
    /// `DisruptedMidDrill`, `PodUnschedulable`, `TargetTopicConfigRefused`,
    /// `CredentialNotRenderable`. Read off the pod log's final line, which
    /// carries `refusal-reason=<TerminalState>`: the pod log API has no stream
    /// selector, so nothing on stderr is distinguishable by a controller.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_reason: Option<String>,
    /// The reason of the condition that describes this run RIGHT NOW —
    /// `ApprovalNotVerified`, `ApprovalNotReceived`, `PlanHashMismatch`,
    /// `ClusterNotReachable`, `NameTooLong`, `JobCreated`, `Ok`,
    /// `GuardRefused`, `DrillNotPass`, `SigningOrLock`, `Operational`, … —
    /// and the field the `REASON` printer column reads.
    ///
    /// # Why this exists beside `exitReason`, which looks like it says the
    /// same thing
    ///
    /// IT DOES NOT SAY THE SAME THING, AND FOR THE FOUR STATES AN OPERATOR
    /// MOST NEEDS IT SAID NOTHING AT ALL. `exitReason` is Global Constraint
    /// 11's vocabulary about a RUN: it is written from an exit code, and a
    /// refusal this controller makes ITSELF — before any `POST`, so with no
    /// run and no code — can only spell it `operational`, which is GC11's
    /// "could not be attempted". All four admission refusals
    /// (`ApprovalNotReceived`, `ApprovalNotVerified`, `PlanHashMismatch`,
    /// `ClusterNotReachable`) and `NameTooLong` therefore printed the SAME
    /// `operational` in the `REASON` column, and the specific state existed
    /// only inside `status.conditions` — measured live at the Task 20 review
    /// (finding M2) on two objects that both printed `operational`.
    ///
    /// So this field is the CONDITION's answer, promoted to a scalar. It is
    /// not a third vocabulary: it is always **verbatim** the `reason` of the
    /// condition this patch writes about the run's current or terminal state,
    /// which makes it CamelCase everywhere by errata **E5b**, and
    /// `every_status_write_sets_the_scalar_reason` asserts the equality for
    /// every patch builder plus, by source scan, that a patch which writes
    /// `conditions` and no `reason` cannot be added.
    ///
    /// `exitReason` is UNCHANGED and still the honest home of GC11's wire
    /// string and of the runner's own `refusal-reason=` terminal state, which
    /// is the more specific answer whenever a pod actually ran. Two fields,
    /// two questions: *what did the run exit with* and *what state is this
    /// object in*.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// The last phase slot that completed, `-1` through `9`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_phase_completed: Option<i32>,
    /// `pass`, `fail-objective`, `fail-integrity` or `preflight-failed`. Spec
    /// §8's green badge for a `Restore` needs
    /// `evidence.verification.result == Valid` **and** `outcome == pass`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<String>,
    /// How thoroughly the restore was checked, what that found, and — when the
    /// result is `partial` — why (interface **I34**).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub integrity: Option<Integrity>,
    /// What the run achieved.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub measured: Option<Measured>,
    /// What was asked for, and whether it was met (interface **I34**).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub objectives: Option<Objectives>,
    /// What phase 0 found out about the target topics (guard **G-TS**).
    /// **Always absent in tag 1: it has no producer, and this field says so
    /// rather than being fabricated.** The observation is returned by phase 0
    /// inside the runner (`RestoreOutcome::topic_preflight`) and, by Global
    /// Constraint 12 as amended, is deliberately NOT a scorecard field — so
    /// nothing carries it out of the pod. Interface I8 fixes three stdout key
    /// lines and none of them is a preflight, and the controller reads the
    /// pod's log and the signed scorecard and nothing else. Closing the gap
    /// means a fourth machine-read stdout line on the runner's side, which is
    /// the interface owner's change and not the operator's; an absent field is
    /// truthful and a guessed one is not.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic_preflight: Option<TopicPreflight>,
    /// The signed scorecard, the offset report, and the controller's
    /// verification.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence: Option<RestoreEvidence>,
    /// The topics this run CREATED. For `mode: scratch`, exactly what phase 9
    /// tears down.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub new_topics: Option<Vec<String>>,
    /// The source topics the new ones were restored from. Nothing here was
    /// written to, in any tag: retiring an old topic is tag 2's `Switchover`
    /// and is validated against this list.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub old_topics: Option<Vec<String>>,
    /// The Job that ran, or is running, this restore.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job_ref: Option<LocalRef>,
    /// The condition set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<Condition>>,
}
