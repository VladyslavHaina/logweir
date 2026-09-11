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

use super::{ArchiveRef, Condition, EvidenceVerification, LocalRef};

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
    pub topics: Vec<String>,
    /// Where the archive is written.
    pub archive: ArchiveRef,
    /// The `BackupSchedule` that created this object, when one did.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule_ref: Option<LocalRef>,
    /// The schedule slot this run is for, `yyyymmdd-hhmmss` in UTC. Part of
    /// this object's name and of `status.backupId`; guard **G-SLOT**.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slot: Option<String>,
    /// What caused this run — `schedule` or `manual`. Recorded rather than
    /// inferred from the presence of `scheduleRef`, because the receipt
    /// carries it and an auditor reads the receipt.
    pub triggered_by: String,
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
    /// The condition set. Spec §8's green badge for a `Backup` needs
    /// `evidence.verification.result == Valid` **and** `exitCode == 0` — a
    /// `Backup` carries no `outcome`, so the badge rule here is not the
    /// `Restore` rule.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<Condition>>,
}
