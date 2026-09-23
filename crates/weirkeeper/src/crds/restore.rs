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

use super::{ArchiveRef, Condition, EvidenceVerification, LocalRef, RunProgress, SpecRule, Time};

/// The CEL rule that refuses half a destination-backed restore.
///
/// A restore reads an archive and WRITES evidence, and those are two grants on
/// two destinations. Allowing one of the pair to be set would let a run read
/// from a saved destination and write its scorecard wherever the inline
/// archive pointed — two locations nobody chose together.
pub const DESTINATIONS_TOGETHER_RULE: &str =
    "has(self.sourceDestinationRef) == has(self.evidenceDestinationRef)";

/// The message [`DESTINATIONS_TOGETHER_RULE`] travels with.
pub const DESTINATIONS_TOGETHER_MESSAGE: &str =
    "sourceDestinationRef and evidenceDestinationRef are set together";

/// The CEL rule that ties `spec.sourceDestinationRef` to the sentinel in
/// `spec.sourceArchive.url`.
///
/// # What an older controller does with a sentinel Restore
///
/// It ignores `sourceDestinationRef`, projects no archive credential (the
/// sentinel carries no `secretRef`) and its runner fails reading the
/// plan-pinned location. **The approved plan bytes pin the location**, so
/// nothing is misrouted: the failure is an unreadable archive, never a write
/// somewhere else.
pub const DESTINATION_SENTINEL_RULE: &str = "has(self.sourceDestinationRef) ? (self.sourceArchive.url == 'logweir-destination://' + self.sourceDestinationRef.name && !has(self.sourceArchive.secretRef)) : !self.sourceArchive.url.startsWith('logweir-destination://')";

/// The message [`DESTINATION_SENTINEL_RULE`] travels with.
pub const DESTINATION_SENTINEL_MESSAGE: &str = "with sourceDestinationRef, sourceArchive.url is exactly logweir-destination://<sourceDestinationRef.name> and sourceArchive.secretRef is absent; the logweir-destination scheme is otherwise reserved";

/// The CEL rule that makes an unauthorised `Restore` unrepresentable.
///
/// `approvalRef` used to be required, so "no authorisation" was refused by the
/// structural schema. Making it optional to admit a standing authorisation
/// would have opened exactly that hole; this rule closes it, and closes the
/// other one too — carrying BOTH, where a per-run approval and a standing
/// scope could disagree about what was authorised.
pub const EXACTLY_ONE_AUTHORIZATION_RULE: &str = "has(self.approvalRef) != has(self.authorization)";

/// The message [`EXACTLY_ONE_AUTHORIZATION_RULE`] travels with.
pub const EXACTLY_ONE_AUTHORIZATION_MESSAGE: &str =
    "set exactly one of spec.approvalRef (a per-run Approval) or spec.authorization (a standing authorization); a Restore is never unauthorized";

/// The rules on `Restore`'s `.spec`.
pub const SPEC_RULES: [SpecRule; 4] = [
    SpecRule::new(super::SPEC_IMMUTABLE_RULE, super::SPEC_IMMUTABLE_MESSAGE),
    SpecRule::new(DESTINATIONS_TOGETHER_RULE, DESTINATIONS_TOGETHER_MESSAGE),
    SpecRule::new(DESTINATION_SENTINEL_RULE, DESTINATION_SENTINEL_MESSAGE),
    SpecRule::new(
        EXACTLY_ONE_AUTHORIZATION_RULE,
        EXACTLY_ONE_AUTHORIZATION_MESSAGE,
    ),
];

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
    /// How the evidence is being read when an evidence-fetch Job reads it —
    /// D2 §3.9 step 3. Absent for every other evidence path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observation: Option<super::EvidenceObservation>,
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
    ///
    /// # Why this became optional, and why that is not a weakening
    ///
    /// A rehearsal is authorised by a STANDING document instead
    /// ([`RestoreAuthorization`]), so exactly one of this and `authorization`
    /// is set and CEL refuses both and neither. An OLDER controller reading a
    /// standing-authorised `Restore` sees no `approvalRef`, resolves the empty
    /// name to nothing and refuses terminally with `ApprovalNotReceived` —
    /// fail closed, which is the required rollback behaviour. An unauthorised
    /// restore has never been reachable through either field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_ref: Option<LocalRef>,
    /// A standing authorisation, for an unattended run (D3 §4.3). Exactly one
    /// of this and `approvalRef`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authorization: Option<RestoreAuthorization>,
    /// The archive to restore from.
    ///
    /// With `sourceDestinationRef` set this is the sentinel
    /// `logweir-destination://<name>` and carries no `secretRef`; see
    /// [`DESTINATION_SENTINEL_RULE`].
    pub source_archive: ArchiveRef,
    /// The saved `BackupDestination` the archive is read from, in this
    /// namespace. Set together with `evidenceDestinationRef`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_destination_ref: Option<LocalRef>,
    /// The saved `BackupDestination` this run's evidence is written to.
    ///
    /// A SECOND DESTINATION, ON PURPOSE. Evidence is written under `logweir/`
    /// with its own grant, and an installation that keeps evidence in a
    /// different bucket from the archive is the case this pair exists for.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence_destination_ref: Option<LocalRef>,
    /// The backup set inside `sourceArchive` — a `backupId`.
    pub backup_set_ref: String,
    /// The point in time to restore to. Records after it are not restored.
    pub point_in_time: Time,
    /// Where the restore writes, and in which mode.
    pub target: RestoreTarget,
    /// The Job's `activeDeadlineSeconds`.
    pub deadline_seconds: i64,
    /// What the runner pod asks for and is capped at.
    ///
    /// ON THE SPEC, NOT ON AN ANNOTATION, because PLAT-06.1's rule is that a
    /// Job's shape is a function of the object. It is also what keeps
    /// `scratch_mode_and_new_topic_mode_produce_the_same_job_shape` true:
    /// resources come from here and never from the target mode.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runner_resources: Option<super::rehearsal_schedule::RunnerResources>,
}

impl RestoreSpec {
    /// The per-run `Approval` this restore names, or `""`.
    ///
    /// `""` IS THE SHAPE EVERY CALLER ALREADY HANDLED. Before `approvalRef`
    /// became optional, an empty NAME was the "no authorisation" case and each
    /// call site refused it terminally with `ApprovalNotReceived`. Collapsing
    /// absent to `""` keeps those three refusals byte-identical — which is
    /// also exactly what an older controller does with a standing-authorised
    /// `Restore`, so the rollback behaviour and the current behaviour are the
    /// same code path rather than two that have to be kept in step.
    #[must_use]
    pub fn approval_ref_name(&self) -> &str {
        self.approval_ref.as_ref().map_or("", |r| r.name.as_str())
    }
}

/// How an unattended `Restore` is authorised.
///
/// THE CONTROLLER DOES NOT MINT THIS. `approvalRef` names the `Approval`
/// carrying the signed standing scope, and `rehearsalScheduleRef` names the
/// schedule whose sealed spec the scope's `templateDigest` is over. The
/// controller re-verifies both every slot, and the runner re-proves
/// `plan ∈ scope` against the mounted bundle before any client is constructed.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RestoreAuthorization {
    /// `Standing` — the only kind in v1.
    pub kind: AuthorizationKind,
    /// The `Approval` carrying the signed standing scope.
    pub approval_ref: LocalRef,
    /// The `RehearsalSchedule` the scope is bound to.
    pub rehearsal_schedule_ref: LocalRef,
}

/// The kinds of authorisation a `Restore` can carry besides a per-run
/// `Approval`.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, Default, JsonSchema, PartialEq, Eq)]
pub enum AuthorizationKind {
    /// One signed document covering every slot of one sealed schedule, checked
    /// again each slot and again by the runner.
    #[default]
    Standing,
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
    ///
    /// **IT HAS A PRODUCER SINCE TASK 24, AND THE GAP THAT MADE IT ABSENT IS
    /// CLOSED** (plan erratum **E10(c)**). The observation is returned by
    /// phase 0 inside the runner (`RestoreOutcome::topic_preflight`) and, by
    /// Global Constraint 12 as amended, is deliberately NOT a scorecard field
    /// — so for two slots nothing carried it out of the pod and this field
    /// declared its own absence rather than being fabricated. Closing it took
    /// exactly what that note said it would: a fourth machine-read stdout line.
    /// `logweir restore run` now prints
    /// `topic-preflight=<one-line JSON object>`
    /// (`logweir::drill::phase0_admit::TOPIC_PREFLIGHT_KEY_PREFIX`) on a
    /// successful run, and
    /// `weirkeeper::controllers::restore::topic_preflight` scans it out of the
    /// same bounded tail as the evidence keys, BY KEY NAME (erratum E4).
    ///
    /// **STILL ABSENT ON EVERY RUN THAT DID NOT COMPLETE PHASE 0**, and that
    /// absence is still the truthful answer rather than a zero: the line is
    /// printed only at exit 0, because `RestoreOutcome` is what carries the
    /// observation and every other path returns an error instead. A run
    /// refused at phase 0 never read the target's config, so there is nothing
    /// to report about it.
    ///
    /// THE THREE FIELDS ARE THE CONTRACT. The runner emits exactly
    /// `timestampType`, `retentionMs` and `timestampBound` — this struct's
    /// own camelCase spellings — and the controller filters the line to those
    /// three, so a fourth key on either side is dropped rather than misfiled
    /// into a property the structural schema then prunes. `retentionMs` is a
    /// STRING on the runner's side (that is what DescribeConfigs returns) and
    /// an `i64` here, and a value that will not parse is OMITTED from the line
    /// rather than flattened to `0`.
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
    /// What this run is doing right now. Absent on a `Restore` an older
    /// controller reconciled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub progress: Option<RunProgress>,
    /// What the run actually restored, copied by JSON pointer from the SIGNED
    /// scorecard. Written ONLY beside a `Valid` evidence verdict (basis
    /// `Current` or `Historical`) over that same scorecard, in the write that
    /// records the verdict: it is absent while verification is pending or
    /// `NotAttempted`, and it is never written from a scorecard whose verdict
    /// is `Invalid` or `Untrusted`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completion: Option<RestoreCompletion>,
    /// What phase 9 removed, and what it could not.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub teardown: Option<Teardown>,
    /// The condition set. `RunnerReady` joins `Verified` as a condition every
    /// terminal builder carries forward, because a JSON merge patch replaces
    /// the whole list.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<Condition>>,
}

/// One topic the run created, with its partition count.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CreatedTopic {
    /// The new topic's name.
    pub name: String,
    /// How many partitions it was created with.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partitions: Option<i64>,
}

/// The window the integrity sample covered.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SampleWindow {
    /// Inclusive start.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start: Option<Time>,
    /// Inclusive end.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end: Option<Time>,
}

/// The incident-facing summary of a recovery.
///
/// COPIED FROM THE SIGNED SCORECARD, NEVER RECOMPUTED. Every number here has a
/// JSON pointer into a document that was signed; a controller that computed
/// its own would be asserting an outcome nobody attested.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RestoreCompletion {
    /// The topics created, from `target_diff.would_create`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 256))]
    pub new_topics: Option<Vec<CreatedTopic>>,
    /// The canary size, from `sample.records_expected`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub records_expected: Option<i64>,
    /// From `sample.records_restored`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub records_restored: Option<i64>,
    /// From `integrity.records_sampled`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub records_sampled: Option<i64>,
    /// How many of those matched.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub records_sampled_matching: Option<i64>,
    /// `byte-fingerprint`, `consume-only` or `not-attempted` — HOW the check
    /// was made, beside its result, because "sampled 100, matched 100" means
    /// two different things under the first two.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub integrity_level: Option<String>,
    /// The window the sample covered.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sample_window: Option<SampleWindow>,
}

/// One topic teardown could not remove.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TeardownFailure {
    /// The topic.
    pub topic: String,
    /// Why, redacted and bounded.
    #[schemars(length(max = 256))]
    pub error: String,
}

/// What phase 9 removed, read from the SIGNED teardown attestation.
///
/// THE CONTROLLER DELETES NO TOPIC, EVER. The runner's phase 9 deletes the
/// exact names it created, through a deleter that refuses any name outside the
/// run's prefix; this block is the controller reading what that attested. A
/// non-empty `failed` is what makes the next rehearsal slot SKIP rather than
/// adopt topics it did not create.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Teardown {
    /// The object key of the signed teardown attestation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attestation_key: Option<String>,
    /// The topics it removed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 256))]
    pub deleted: Option<Vec<String>>,
    /// The topics it could not.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 256))]
    pub failed: Option<Vec<TeardownFailure>>,
}
