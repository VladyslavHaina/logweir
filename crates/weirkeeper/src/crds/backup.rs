//! `Backup` — one archive run, as an object.
//!
//! Object identity is a pure function of the trigger (guard **G-SLOT**). A
//! scheduled `Backup` is named `<schedule>-<slot as yyyymmdd-hhmmss, UTC,
//! lowercase>` — a DNS-1123 subdomain, because uppercase `T`/`Z` are rejected
//! as Kubernetes object names; the `YYYYmmddTHHMMSSZ` form is kept only for
//! Kafka topic names. `status.backupId` derives from the object UID plus that
//! slot, and the cron reconciler's only write for a due slot is a `create`, so
//! an `AlreadyExists` after a crash **is** the idempotence key.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::selection::{AllUserTopics, SelectionStatus, TOPIC_NAME_PATTERN};
use super::{ArchiveRef, Condition, EvidenceVerification, LocalRef, RunProgress, SpecRule, Time};

/// The `spec.trigger.kind` values, and what each one means for identity.
///
/// # Why the trigger is a field and not an inference
///
/// `triggeredBy` already said `schedule` or `manual`, and that was enough
/// while every scheduled run was attempt 0 of a slot that fired on time. It is
/// not enough once a slot can be started LATE (a catch-up) or started AGAIN (a
/// retry): all three are `schedule`, all three produce a different execution
/// id, and a controller that had to infer which one it was looking at would be
/// inferring it from a status field or a clock — the two inputs D1 §3.1 rule 7
/// keeps out of the naming function.
///
/// `triggeredBy` is UNCHANGED and still what the signed receipt carries.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, JsonSchema, PartialEq, Eq)]
pub enum TriggerKind {
    /// A slot that fired at its own instant. `attempt` is 0.
    Scheduled,
    /// Slot S, started late because the controller was not running when it came
    /// due. Its name and execution id are Scheduled's — **it is the same slot**,
    /// and giving a catch-up an identity of its own would let a restart produce
    /// a second archive of one window.
    CatchUp,
    /// Attempt `k` of slot S, `1..=3`. A NEW execution id, never a second write
    /// under the old one: a failed attempt may have written part of an archive,
    /// and reusing its `backup_id` would append into that partial prefix.
    Retry,
    /// Created by a person, the API or the console. No slot, and the execution
    /// id is this object's own UID.
    Manual,
}

/// What caused this run, in the form identity is derived from.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Trigger {
    /// Which kind of run this is.
    pub kind: TriggerKind,
    /// `0` for `Scheduled` and `CatchUp`, `1..=3` for `Retry`, `0` for
    /// `Manual`.
    #[serde(default)]
    #[schemars(range(min = 0, max = 3))]
    pub attempt: i32,
    /// The attempt this one retries — `name(S, attempt - 1)`, checked rather
    /// than trusted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_of: Option<LocalRef>,
    /// The IANA zone the slot was computed in.
    ///
    /// INFORMATIONAL, AND THAT IS THE POINT. The slot itself is UTC; this
    /// records the zone it was computed in so a history row keeps its local
    /// time after somebody edits the schedule's `timeZone`. Nothing resolves a
    /// run from it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 64))]
    pub time_zone: Option<String>,
}

/// The `BackupSchedule` this run belongs to, and the revision of its policy.
///
/// # Why this grew four fields
///
/// It was a bare name, which was enough while a schedule's spec was immutable.
/// PLAT-05.1 makes the policy editable, so "which schedule" stops answering
/// "under which policy": `uid` distinguishes a same-named replacement,
/// `generation` names the revision, and `runPolicySha256` is the digest of the
/// fields that decide WHAT a run does — so a `suspend` flip visibly leaves it
/// unchanged while a topic-list edit visibly does not.
///
/// Every field but `name` is optional, because a `Backup` created before they
/// existed has none of them and must keep resolving exactly as it did.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ScheduleRef {
    /// The `BackupSchedule` name, in this namespace.
    pub name: String,
    /// Its UID. A schedule deleted and recreated under the same name is a
    /// different schedule and must not adopt this run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uid: Option<String>,
    /// Its `metadata.generation` when this run was admitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generation: Option<i64>,
    /// `sha256:<lowercase hex>` over the run policy this run copied.
    ///
    /// AN INTEGRITY CHECK AGAINST BUGS, NOT A SECURITY BOUNDARY (D1 §8.7).
    /// `Backup.spec` is CEL-immutable and the controller recomputes this from
    /// the object's own fields; a mismatch means the copy and the fields
    /// disagree, which is a control-plane defect and terminal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_policy_sha256: Option<String>,
}

/// The CEL rule that ties `spec.destinationRef` to the sentinel in
/// `spec.archive.url`, on every create as well as every update.
///
/// # Why a sentinel and not an absent `archive`
///
/// An older controller deserializing a `Backup` with no `archive` would fail
/// the whole list or watch with a reflector decode error, and EVERY `Backup`
/// reconcile would stall after a rollback — one bad object taking out the
/// kind. With the sentinel the old object decodes, the old `storage_url_for`
/// falls into its unknown-scheme arm, and the old controller writes the
/// terminal `ArchiveUrlUnreadable` **before any POST**. Fail closed, visibly,
/// on one object.
///
/// The second half — `!has(self.archive.secretRef)` — is what stops a
/// destination-backed `Backup` carrying a credential reference that nothing
/// would read. The `else` arm reserves the scheme: without it, an object could
/// name `logweir-destination://something` with no `destinationRef` and mean
/// nothing at all.
pub const DESTINATION_SENTINEL_RULE: &str = "has(self.destinationRef) ? (self.archive.url == 'logweir-destination://' + self.destinationRef.name && !has(self.archive.secretRef)) : !self.archive.url.startsWith('logweir-destination://')";

/// The message [`DESTINATION_SENTINEL_RULE`] travels with.
pub const DESTINATION_SENTINEL_MESSAGE: &str = "with destinationRef, archive.url is exactly logweir-destination://<destinationRef.name> and archive.secretRef is absent; the logweir-destination scheme is otherwise reserved";

/// The rules on `Backup`'s `.spec`.
///
/// The whole spec stays sealed — a run's inputs are the run — and the sentinel
/// rule sits beside it because a transition rule is NOT evaluated on create
/// and the sentinel has to be.
pub const SPEC_RULES: [SpecRule; 2] = [
    SpecRule::new(super::SPEC_IMMUTABLE_RULE, super::SPEC_IMMUTABLE_MESSAGE),
    SpecRule::new(DESTINATION_SENTINEL_RULE, DESTINATION_SENTINEL_MESSAGE),
];

/// The archive window this run covers, in **epoch milliseconds**.
///
/// INTERFACE **I22**. The same shape as `BackupReceipt.covered{from_ms,
/// to_ms}` (Task 5), camelCased by the CRD derive, and **not** two RFC 3339
/// strings: the receipt these two fields mirror carries integers, and a
/// controller that had to convert between the two representations is a
/// controller that can round a window boundary. Task 17 produces the value;
/// this task declares the shape so no consumer has to guess.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct WindowCovered {
    /// Inclusive start of the covered window, epoch milliseconds.
    pub from_ms: i64,
    /// Exclusive end of the covered window, epoch milliseconds.
    pub to_ms: i64,
}

/// The auth identity the run actually used, as an observation.
///
/// `mode` and `username` only. There is no password field anywhere in this
/// group, and a status block is the last place one could be justified.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ObservedAuth {
    /// `plaintext` or `scramSha512`, as resolved from the source
    /// `KafkaCluster`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
    /// The SASL principal the run authenticated as, when there was one.
    /// Logweir cannot observe the principal the broker authenticated — Kafka
    /// exposes no such call — so this is the username Logweir PRESENTED.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
}

/// Where the signed backup receipt is, and what the controller made of it.
///
/// KEYS AND DIGESTS ONLY, NEVER CONTENT. An evidence block names the object; a
/// reader fetches it. Putting the document in the status would make the API
/// server the evidence store.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct BackupEvidence {
    /// The object key of the signed backup receipt.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receipt_key: Option<String>,
    /// The sha256 of the receipt at `receiptKey`, as `sha256:<lowercase hex>`
    /// — the one digest spelling this corpus uses everywhere
    /// (`logweir_core::ids::sha256_prefixed`). **COMPUTED by the controller
    /// over the bytes it fetched**, not copied: a document cannot carry its
    /// own digest.
    ///
    /// WHY IT IS HERE AT ALL, AND IT IS NOT DECORATION (Task 24). It is the
    /// value `verification` is checked against on a LATER pass: the controller
    /// re-fetches the receipt with its read-only evidence credential and
    /// compares this recorded digest against the bytes in the bucket right
    /// now. Without it, `verify_evidence` could only check the signature —
    /// and a genuinely-signed OLDER receipt put in this one's place would
    /// verify. `Restore` has carried the same field for the scorecard since
    /// Task 20 (`restore::RestoreEvidence::scorecard_sha256`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receipt_sha256: Option<String>,
    /// The object key of the receipt's detached DSSE sidecar.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sidecar_key: Option<String>,
    /// What `weirkeeper` recorded when it verified the pair above.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verification: Option<EvidenceVerification>,
}

/// The server-derived identity of this run and the immutable inputs it
/// executes.
///
/// Written once, before the runner Job is created, and never rewritten. The
/// referenced ConfigMap is create-only, `immutable: true` and owned by exactly
/// this `Backup`; it holds the canonical typed input snapshot and the two
/// runner documents rendered from it. No annotation contributes to either.
/// Broker addresses and Secret names stay in that ConfigMap: this projection
/// carries an identity, a reference and a digest only.
// PLAT-06.1. Kept as three required fields so a consumer (the UI, a later
// run-snapshot or topic-resolution task) never has to guess which half of a
// partially written block it is reading.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct BackupExecution {
    /// The run identity, generated by the control plane and never read from an
    /// annotation: `<BackupSchedule uid>-<slot>` for a scheduled `Backup`, this
    /// object's own UID for a manual one. It is the archive `backup_id` and the
    /// runner's backup id override.
    pub id: String,
    /// The immutable ConfigMap, in this namespace, holding
    /// `execution-inputs.json` and the runner documents derived from it.
    pub inputs_ref: LocalRef,
    /// `sha256:<lowercase hex>` over the exact `execution-inputs.json` bytes
    /// in `inputsRef`. The Job created from those inputs carries the same
    /// value in its `logweir.dev/execution-inputs-sha256` annotation.
    pub inputs_sha256: String,
}

/// `Backup.spec`.
#[derive(kube::CustomResource, Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "logweir.dev",
    version = "v1alpha1",
    kind = "Backup",
    doc = "One archive run, executed as a Job. Its name and `status.backupId` are a pure function of the trigger, so a duplicate reconcile gets AlreadyExists rather than a second partial archive. `spec` is immutable.",
    plural = "backups",
    singular = "backup",
    namespaced,
    status = "BackupStatus",
    printcolumn = r#"{"name":"PHASE","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"EXIT","type":"integer","jsonPath":".status.exitCode","description":"0 pass, 1 operational, 2 not-a-pass, 3 refused, 4 signing failed"}"#,
    printcolumn = r#"{"name":"RECORDS","type":"integer","jsonPath":".status.records"}"#,
    printcolumn = r#"{"name":"SIGNED","type":"string","jsonPath":".status.evidence.verification.result","description":"green needs this Valid AND exitCode 0"}"#,
    printcolumn = r#"{"name":"AGE","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct BackupSpec {
    /// The `KafkaCluster` to read from, in this namespace.
    pub source_ref: LocalRef,
    /// NAMED topics, never patterns — guard **G-GLOB**, as on
    /// `BackupSchedule`.
    ///
    /// `[]` WITH `allUserTopics` SET IS DYNAMIC MODE, and the field stays
    /// required in both: an older controller reading a dynamic object
    /// deserializes it, renders an empty list, and the runner refuses before
    /// it contacts the engine. Making this optional would have made a rollback
    /// a reflector decode error across the whole kind.
    #[schemars(inner(regex(path = "TOPIC_NAME_PATTERN")))]
    pub topics: Vec<String>,
    /// Dynamic selection: every user topic this run's principal can see, minus
    /// the exclusions. Set with `topics: []`, and never beside a non-empty
    /// `topics`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub all_user_topics: Option<AllUserTopics>,
    /// Where the archive is written.
    ///
    /// With `destinationRef` set this is the sentinel
    /// `logweir-destination://<name>` and carries no `secretRef`; see
    /// [`DESTINATION_SENTINEL_RULE`] for why the field stays REQUIRED rather
    /// than becoming optional.
    pub archive: ArchiveRef,
    /// The saved `BackupDestination` this run writes to, in this namespace.
    ///
    /// Optional and additive: absent means `archive` carries the location
    /// inline, exactly as it did before saved destinations existed. An older
    /// controller ignores this field — and then refuses the sentinel URL
    /// terminally, which is the intended rollback behaviour.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination_ref: Option<LocalRef>,
    /// The `BackupSchedule` that created this object, when one did, and the
    /// revision of the policy it copied.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule_ref: Option<ScheduleRef>,
    /// The schedule slot this run is for, `yyyymmdd-hhmmss` in UTC. Part of
    /// this object's name and of `status.backupId`; guard **G-SLOT**.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slot: Option<String>,
    /// What caused this run — `schedule` or `manual`. Recorded rather than
    /// inferred from the presence of `scheduleRef`, because the receipt
    /// carries it and an auditor reads the receipt.
    ///
    /// UNCHANGED, AND STILL THE RECEIPT'S. `trigger` beside it is finer —
    /// `Scheduled`, `CatchUp`, `Retry`, `Manual` — and is what run IDENTITY is
    /// derived from; this stays the two-value vocabulary the signed document
    /// carries, so no existing receipt or fixture changes meaning.
    pub triggered_by: String,
    /// The finer trigger, and the attempt inside its slot.
    ///
    /// Optional, because a `Backup` created before it existed has none: such a
    /// run is read as `Scheduled`/attempt 0 when `triggeredBy` is `schedule`
    /// and `Manual` otherwise (D1 §3.1 rule 4), which is exactly what the
    /// earlier controller did with it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trigger: Option<Trigger>,
    /// The Job's `activeDeadlineSeconds`.
    pub deadline_seconds: i64,
}

/// `Backup.status`.
#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BackupStatus {
    /// Where the run is: `Pending`, `Running`, `Succeeded`, `Failed`,
    /// `Refused`. A free-form string rather than an enum, because Task 17
    /// owns the phase vocabulary and no agreement test in this plan binds it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
    /// The runner's exit code, lifted from
    /// `pod.status.containerStatuses[].state.terminated.exitCode` — the one
    /// path in Kubernetes that carries it. `0` pass · `1` operational, no
    /// artifact · `2` a result that is not a pass, document written and
    /// signed · `3` refused by a guard · `4` signing or lock proof failed
    /// (Global Constraint 11). Spec §8's green badge for a `Backup` requires
    /// `evidence.verification.result == Valid` AND `exitCode == 0` — green
    /// requires verification Valid and exitCode 0, and a `Backup` carries no
    /// `outcome` for the badge to read instead. ABSENT is a real value: a Job
    /// that finished with no terminated state for the `runner` container has
    /// no recoverable code, and the controller records the absence rather
    /// than fabricating a `0` or a `1`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    /// Why the run ended, as one of Global Constraint 11's wire reasons —
    /// `ok`, `operational`, `drill-not-pass`, `guard-refused`,
    /// `signing-or-lock` — or, when the run reached a **terminal state** more
    /// specific than its code, that state: the `refusal-reason=` line the
    /// runner printed for exit 3, or `OrphanedScorecard` for an exit-4 run
    /// whose payload exists without its sidecar.
    ///
    /// `operational` for the crashed-Job case, where `exitCode` is absent: a
    /// run whose code is unrecoverable produced no artifact either, and the
    /// SUB-CASE (`DisruptedMidDrill`, `PodUnschedulable`, `NoExitCode`) is the
    /// condition's `reason`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_reason: Option<String>,
    /// The archive's backup id, derived from this object's UID and
    /// `spec.slot`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backup_id: Option<String>,
    /// The run identity and its frozen inputs, recorded before the runner Job
    /// exists. Absent on a `Backup` whose Job was created by a controller that
    /// predates frozen execution inputs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution: Option<BackupExecution>,
    /// The object key of the manifest this run wrote.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manifest_key: Option<String>,
    /// The sha256 of the manifest at `manifestKey`, lowercase hex.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manifest_sha256: Option<String>,
    /// How many records the run archived.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub records: Option<i64>,
    /// The window the archive covers, in epoch milliseconds — interface
    /// **I22**.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window_covered: Option<WindowCovered>,
    /// The identity the run presented. `mode` and `username`, never a
    /// password.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth: Option<ObservedAuth>,
    /// The signed receipt, and the controller's verification of it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence: Option<BackupEvidence>,
    /// The Job that ran, or is running, this backup.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job_ref: Option<LocalRef>,
    /// What the run's topic resolution found, and what it may claim to have
    /// covered. Absent on a run frozen before dynamic selection existed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selection: Option<SelectionStatus>,
    /// What this run is doing right now, and why it is taking as long as it
    /// is. **Absent on a `Backup` an older controller reconciled**, which is
    /// the documented absent-field behaviour and not a degraded state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub progress: Option<RunProgress>,
    /// When the capture itself started and finished, copied VERBATIM from the
    /// verified receipt.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capture: Option<CaptureWindow>,
    /// The condition set. Spec §8's green badge for a `Backup` needs
    /// `evidence.verification.result == Valid` **and** `exitCode == 0` — a
    /// `Backup` carries no `outcome`, so the badge rule here is not the
    /// `Restore` rule. `RunnerReady` joins `Verified` as a condition every
    /// terminal builder carries forward: a JSON merge patch REPLACES
    /// `status.conditions`, so a builder that emits one and not the other
    /// deletes the one it left out.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<Condition>>,
}

/// When the capture actually happened.
///
/// # Why `startedAt` and not `finishedAt` is what protection is measured from
///
/// A four-hour backup that STARTED at 02:00 protects you to 02:00. Dating the
/// recovery point at 06:00 would overstate the protection by the length of the
/// run — which is exactly backwards, since a long run is usually a big or a
/// struggling one. `ProtectionPolicy` reads `startedAt`; both are recorded so
/// nobody has to infer the other.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CaptureWindow {
    /// `BackupReceipt.started_at`, copied from the verified receipt.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<Time>,
    /// `BackupReceipt.finished_at`, copied from the verified receipt.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<Time>,
}
