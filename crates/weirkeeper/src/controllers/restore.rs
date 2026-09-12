//! The `Restore` reconciler — one Job, the plan hash recomputed at
//! Job-creation time, and `mode: scratch` is a field.
//!
//! # Why the admission happens HERE and not in the pod
//!
//! Exit 3 from a runner pod is ambiguous today, and the corpus says why: phase
//! 0 dials before phase 1 runs, so a `plan_hash` mismatch or a bad approval
//! signature only becomes exit 3 **when the target answers**. With a dead
//! broker the same spec exits `1`, and a UI that maps exit 3 to *"your
//! approval does not match this spec"* mislabels that case every time the
//! scratch cluster is down. This reconciler removes the ambiguity **at the
//! source**: [`admit`] verifies the approval and recomputes the plan hash
//! **before any pod exists**, so a mismatch is a terminal status with no run
//! created, and **exit 3 from a pod therefore means only "a phase-0 admission
//! guard refused"**.
//!
//! Recomputing at Job-creation time rather than trusting a `status` matters
//! because a spec schema change invalidates every approval
//! (`02-k8s-transition-plan.md:2642`) and a status is a cache. Interface
//! **I18**: the hash is over `Approval.spec.approvalBytes`' own `plan_hash`,
//! parsed from the **UTF-8 document text, verbatim, never base64**, and never
//! from `Approval.status.matchedKeyId` or any other status value.
//!
//! # `Drill` is a field
//!
//! The same reconciler, the same Job shape, the same status block.
//! `spec.target.mode: scratch` adds `--allowed-clusters` enforcement and
//! phase-9 teardown **inside the runner**; `newTopic` does not. Two kinds
//! would duplicate the whole exit-code-to-condition table for one boolean, so
//! `scratch_mode_and_new_topic_mode_produce_the_same_job_shape` asserts field
//! by field that the two Jobs differ **only** in the plan ConfigMap's
//! contents.
//!
//! # THE CREDENTIAL IS VALIDATED BY THE RUNNER. THIS RECONCILER PERFORMS NO
//! CHECK OF ITS OWN
//!
//! Interface **I11**, spec §7 and §10's amended G-EXP. `weirkeeper` holds
//! **no `get` on Secrets anywhere** (spec §9), so it never sees the projected
//! value and has nothing to validate. The check runs in the **runner**, at the
//! moment it reads `LOGWEIR_SOURCE_PASSWORD` / [`TARGET_PASSWORD_ENV`], and
//! exits **3** with `CredentialNotRenderable`: the predicate lives in
//! `logweir-core`'s guard module, where Task 3 owns it and Task 6 owns the
//! call site. This module MAPS that refusal onto the terminal state of the
//! same name and does nothing else with it. The division of labour is written
//! here so a reader does not go looking for a controller-side check that "no
//! `get` on Secrets" forbids.
//!
//! Two tests hold the property from opposite ends.
//! `the_controller_performs_no_credential_check` reads this file's source and
//! asserts it names neither that predicate nor a namespaced Secret API — and
//! it spells both tokens in the TEST rather than here, because
//! `tests/linkage.rs::the_controller_never_reads_a_secret` scans every line of
//! `src/` including this one, comments included, and a guard that its own
//! subject can talk its way past is not a guard.
//!
//! # THE EXIT-3 DISCRIMINATOR IS A KEY NAME IN A BOUNDED TAIL, NOT A POSITION
//!
//! Global Constraint 11 puts `refusal-reason=<TerminalState>` on the runner's
//! final **stdout** line, and plan erratum **E4** measured what that becomes
//! in a pod log: stdout and stderr **merged in nondeterministic order** — the
//! same refusal came back second-to-last in one run on docker-desktop v1.34.1
//! and last in another. **The pod log API has no stream selector**, so nothing
//! written to stderr is distinguishable by a controller at all. Every reader
//! here therefore scans the final [`super::backup::KEY_SCAN_TAIL_LINES`]
//! non-empty lines and matches **by key name**:
//! [`super::backup::refusal_state`] for the refusal and
//! [`restore_evidence_keys`] for interface **I8**'s three keys. A log body
//! with no refusal line at exit 3 yields
//! [`TERMINAL_STATE_GUARD_REFUSED_UNKNOWN_REASON`], never a guess.
//!
//! # What this module never does
//!
//! **It never parses a scorecard into a typed struct and re-emits it.**
//! (The type is named in `logweir-core`'s scorecard module and deliberately
//! nowhere in this file: `the_controller_never_types_a_scorecard` reads this
//! source, comments included, so a paragraph that spelled the name would
//! defeat the guard that argues for it.)
//! That type has no `deny_unknown_fields` and every
//! field is `#[serde(default)]`, so a field the reader's struct does not
//! declare is silently dropped on parse — and a controller that re-emitted the
//! result would publish a status block that quietly disagrees with the signed
//! document. [`observe_scorecard`] reads the specific values it needs out of a
//! `serde_json::Value` and copies them verbatim;
//! `the_controller_never_types_a_scorecard` asserts the type name appears
//! nowhere in this file.
//!
//! **It performs no signature verification.** Task 24 does, through
//! `logweir-verify` (Global Constraint 27 as narrowed: this crate links the
//! verifying half and never the signer). This reconciler records the evidence
//! KEYS and leaves `status.evidence.verification` untouched.
//!
//! **It deletes nothing.** Not the Job, not the pod, not an object in the
//! archive. Global Constraint 6 gives no Logweir component a delete capability
//! over the archive in tag 1, and the only deletion tag 1 performs anywhere is
//! phase 9's teardown of the scratch topics a `mode: scratch` run created —
//! which happens **inside the runner**, over topics, never here.

use std::fmt;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use futures::future::BoxFuture;
use futures::StreamExt as _;
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::{ConfigMap, Pod};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference};
use kube::api::{Api, ListParams, LogParams, Patch, PatchParams, PostParams};
use kube::runtime::controller::Action;
use kube::runtime::{watcher, Controller};
use kube::{Resource, ResourceExt as _};
use serde_json::{json, Value};
use tracing::{debug, info, warn};

use super::backup::{
    self, ARCHIVE_ACCESS_KEY, ARCHIVE_ACCESS_KEY_ENV, ARCHIVE_SECRET_KEY, ARCHIVE_SECRET_KEY_ENV,
    RUNNER_SERVICE_ACCOUNT, SIGNING_KEY_FILE, SIGNING_KEY_SECRET, SIGNING_KEY_SECRET_KEY,
    SIGNING_MOUNT_PATH, SIGNING_VOLUME, TTL_SECONDS_AFTER_FINISHED,
};
use super::Context;
use crate::conditions::{
    current_condition, merge_condition, reason_for_exit, status_unchanged, wire_reason_for_exit,
    CONDITION_ADMITTED, CONDITION_COMPLETE, CONDITION_EVIDENCE_RECORDED, CONDITION_FAILED,
    CONDITION_JOB_CREATED, PHASE_FAILED, PHASE_PENDING, PHASE_RUNNING, PHASE_SUCCEEDED,
    REASON_ADMITTED, REASON_APPROVAL_NOT_VERIFIED, REASON_EVIDENCE_KEYS_RECORDED,
    REASON_EVIDENCE_KEYS_UNREADABLE, REASON_OPERATIONAL, TERMINAL_STATE_APPROVAL_NOT_RECEIVED,
    TERMINAL_STATE_CLUSTER_NOT_REACHABLE, TERMINAL_STATE_GUARD_REFUSED_UNKNOWN_REASON,
    TERMINAL_STATE_NAME_TOO_LONG, TERMINAL_STATE_PLAN_CONFIG_MAP_CONFLICT,
    TERMINAL_STATE_PLAN_HASH_MISMATCH, TERMINAL_STATE_WINDOW_NOT_COVERED,
};
use crate::crds::approval::Approval;
use crate::crds::kafka_cluster::{AuthMode, KafkaCluster};
use crate::crds::restore::Restore;
use crate::crds::trust_roster::TrustRoster;
use crate::job::{
    self, EnvFromSecret, RunnerJobSpec, RunnerOwner, SecretMount, APPROVAL_MOUNT_PATH,
    APPROVAL_VOLUME, CONTAINER_NAME, PLAN_MOUNT_PATH,
};
use crate::verification::{
    conditions_in, restore_badge, second_patch, stored_verification, verified_condition,
    EvidenceRef, VerifyOracle,
};
use logweir_core::ids::sha256_prefixed;
use logweir_store::Store;

// ---------------------------------------------------------------------------
// The plan ConfigMap, and the one key in it
// ---------------------------------------------------------------------------

/// The key in the plan ConfigMap carrying `spec.planBytes`, and the file name
/// [`runner_argv`] points `--spec` at.
///
/// `restore.yaml` AND THE BYTES ARE VERBATIM. Interface **I20**: this document
/// has ONE grammar and it is the runner's own — `logweir_core::spec::RestoreSpec`,
/// which is what `serde_yaml::from_str` deserialises `--spec` with and what
/// the shipped `examples/restore.yaml` is written in. `planBytes` stays an
/// OPAQUE STRING on the way through, precisely because the API server
/// normalises YAML and a typed round-trip silently invalidates every approval:
/// `plan_hash` binds exact bytes, so a renderer that parsed and re-serialised
/// them would produce a document nobody signed while every gate reported
/// green. `the_plan_configmap_carries_the_spec_bytes_verbatim` asserts byte
/// equality including trailing whitespace, and
/// `the_plan_bytes_fixture_is_a_document_the_runner_parses` asserts the fixture
/// really is a document `logweir restore run --spec` accepts.
pub const PLAN_SPEC_KEY: &str = "restore.yaml";

/// The plan ConfigMap a `Restore`'s runner reads `spec.planBytes` from.
///
/// The same `<name>-plan` shape the `Backup` path uses
/// ([`super::backup::plan_config_map_name`]), for the same reason: the mount
/// and the renderer must not choose separately.
#[must_use]
pub fn plan_config_map_name(restore_name: &str) -> String {
    format!("{restore_name}-plan")
}

// ---------------------------------------------------------------------------
// The approval bundle, the signing key, and the credential that is NOT ours
// ---------------------------------------------------------------------------

/// The Secret carrying the approval bundle — Task 22's, projected at
/// [`APPROVAL_MOUNT_PATH`].
///
/// A SECRET AND NOT A ConfigMap, AND THE DIRECTION IS WHY. `allowed-clusters.json`
/// on this path authorises a restore **TARGET**: any subject with `patch
/// configmaps` who could replace it would WIDEN the set of clusters a restore
/// may write into. On the `Backup` path the same file is a consistency rail
/// whose only possible effect is to make a run refuse (errata **E5a**;
/// `docs/kubernetes.md` §10 carries that reason), so it stays a ConfigMap
/// there. Same file name, opposite blast radius, two homes.
pub const APPROVAL_BUNDLE_SECRET: &str = "logweir-approval-bundle";

/// The signed approval document, at `/approval/approval.json`.
pub const APPROVAL_DOC_FILE: &str = "approval.json";
/// Its detached DSSE sidecar, at `/approval/approval.sig`.
pub const APPROVAL_SIG_FILE: &str = "approval.sig";
/// The approver's **public** key, at `/approval/approver.pub.pem`. No private
/// key reaches a runner pod on this path or any other.
pub const APPROVER_KEY_FILE: &str = "approver.pub.pem";
/// The cluster allowlist, at `/approval/allowed-clusters.json`.
pub const ALLOWED_CLUSTERS_FILE: &str = "allowed-clusters.json";

/// The environment variable the runner reads the TARGET SASL password from,
/// and from nowhere else.
///
/// MIRRORS `logweir::drill::TARGET_PASSWORD_VAR` AND IS NOT LINKED TO IT.
/// `weirkeeper` links `logweir-core` and `logweir-verify` and never the
/// `logweir` binary crate, so the string is restated here — the same shape
/// [`crate::job::ENGINE_VERSION`] uses for the two engine variables, and for
/// the same reason: the Job template is where the value has to be, and a test
/// is what keeps the statement true.
///
/// **The controller never reads it.** It projects a `secretKeyRef` and that is
/// all; see this module's header for interface **I11**.
pub const TARGET_PASSWORD_ENV: &str = "LOGWEIR_TARGET_PASSWORD";

/// The key within `KafkaCluster.spec.auth.secretRef` holding the SASL
/// password.
///
/// One spelling, stated once, because an adopter's Secret has to carry it and
/// a controller that guessed two spellings would produce a pod that starts and
/// then authenticates as nobody. `CredentialNotRenderable` — the runner's
/// refusal at exit 3 — is what an absent or unrenderable value becomes, and it
/// is the runner that decides it.
pub const TARGET_PASSWORD_SECRET_KEY: &str = "password";

// ---------------------------------------------------------------------------
// Where the runner writes, inside the pod
// ---------------------------------------------------------------------------

/// `--out`: the scorecard's pod-local path, on the writable `/work` volume.
///
/// UNDER [`crate::job::WORK_MOUNT_PATH`] BECAUSE THE ROOT FILESYSTEM IS
/// READ-ONLY. The
/// container runs with `readOnlyRootFilesystem: true`, so a default `--out` of
/// `./logweir-<run_id>.json` fails at the first write with an error that looks
/// nothing like the contract it breaks. The signed bytes also go to the
/// evidence bucket; this is the local copy.
pub const SCORECARD_OUT_PATH: &str = "/work/scorecard.json";

/// `--offset-report-out`: where the ENGINE writes its offset-mapping report.
///
/// The run uploads it beside the scorecard and records the key and its sha256
/// in the signed document. **Tag 1 renders the report and applies nothing** —
/// Global Constraint 35, and no consumer-group offset is committed on any
/// cluster.
pub const OFFSET_REPORT_OUT_PATH: &str = "/work/offsets.json";

// ---------------------------------------------------------------------------
// Interface I8 — the three stdout key lines
// ---------------------------------------------------------------------------

/// `scorecard-key=` — interface **I8**'s first line.
pub const SCORECARD_KEY_PREFIX: &str = "scorecard-key=";
/// `sidecar-key=` — interface **I8**'s second line. The same prefix the
/// `Backup` path's interface I7 uses, and deliberately the same constant.
pub const SIDECAR_KEY_PREFIX: &str = backup::SIDECAR_KEY_PREFIX;
/// `offset-report-key=` — interface **I8**'s third line, **conditional**.
///
/// Printed exactly when the engine wrote an offset report
/// (`docs/stability.md`; `logweir::drill::phase8_score::Signed::offset_report_key`).
/// Its ABSENCE at exit 0 is therefore a truthful answer about this run and not
/// an unreadable log — see [`RestoreEvidenceKeys::mandatory_complete`].
pub const OFFSET_REPORT_KEY_PREFIX: &str = "offset-report-key=";

/// **Interface I8's FOURTH key line**, and plan erratum **E10(c)**'s controller
/// half — guard **G-TS**.
///
/// The runner prints it as `topic-preflight=<one-line JSON object>` on a
/// successful restore
/// (`logweir::drill::phase0_admit::TOPIC_PREFLIGHT_KEY_PREFIX`), and this
/// reconciler scans it out of the same bounded tail as the evidence keys, BY
/// NAME (erratum E4). Until Task 24 it had no producer at all, and
/// `Restore.status.topicPreflight` rendered a declared absence in both places.
///
/// SCANNED, NEVER DERIVED. There is no way to compute a target topic's
/// `message.timestamp.type` from anything else on the object — the controller
/// never dials a broker (spec §9) — so an absent or unparseable line leaves
/// the field absent, which is the truthful answer for a run that reported
/// none.
pub const TOPIC_PREFLIGHT_KEY_PREFIX: &str = "topic-preflight=";

/// How long before an unfinished Job is looked at again. Fifteen seconds, as
/// on the `Backup` path: a Job's own events wake this controller, so the
/// requeue exists for the one transition no watch delivers.
pub const REQUEUE_SECS: u64 = 15;

/// **Interface I19.** How long before an `Approval` that is not yet
/// `Verified=True` is looked at again — **thirty seconds**.
///
/// THIS IS WHAT MAKES THE WIZARD'S ORDERING WORKABLE. Both `metadata.name`s
/// are minted from the plan bytes by the producer (Phase C's Task 27), so a
/// `Restore` can legitimately exist for a few seconds before the `Approval`
/// that authorises it does — and an approver may take an afternoon over it. A
/// dangling `spec.approvalRef` is therefore a HOLD and not a verdict: the
/// object sits at `phase: Pending` with one `Admitted=False` condition naming
/// [`REASON_APPROVAL_NOT_VERIFIED`], and is released the moment the `Approval`
/// arrives and the `Approval` reconciler verifies it. Thirty seconds rather
/// than the fifteen a running Job gets, because nothing is in flight; rather
/// than the five minutes the `Approval` controller uses, because the human on
/// the other end is waiting.
pub const ADMISSION_REQUEUE_SECS: u64 = 30;

/// The scorecard `outcome` that means the archive did not cover the requested
/// window.
///
/// **IT IS NOT IN THE FROZEN 1.0.0 ENUM, AND THAT IS RECORDED RATHER THAN
/// PAPERED OVER.** `schemas/logweir-drill-scorecard-1.0.0.json`'s `Outcome` is
/// exactly `["pass", "fail-objective", "fail-integrity", "preflight-failed"]`,
/// so no document this build's runner signs can carry `fail-coverage`. The
/// mapping exists anyway, in one place and with a test, because this
/// controller reads the scorecard as a `serde_json::Value` and copies
/// `outcome` VERBATIM (see this module's header): it validates nothing against
/// that enum, so a document carrying the value is a document this reconciler
/// will classify — and `WindowNotCovered` is a member of
/// [`crate::conditions::TERMINAL_STATES`] that would otherwise have no
/// producer at all. Recorded in the task report as a brief-versus-schema
/// discrepancy for the controller to route.
pub const OUTCOME_FAIL_COVERAGE: &str = "fail-coverage";

// ---------------------------------------------------------------------------
// Admission — a pure function, run before any Job exists
// ---------------------------------------------------------------------------

/// What [`admit`] decided.
///
/// FIVE VARIANTS AND THE SET IS CLOSED. Tasks 26 and 27 render the `reason`
/// this produces, so a variant is an interface and not an implementation
/// detail; [`RestoreAdmission::reason`] is a `match` with no wildcard so a
/// sixth variant fails to compile until someone names its reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RestoreAdmission {
    /// Every check passed. A Job may be created.
    Ok,
    /// The `Approval` `spec.approvalRef` names does not exist yet, or exists
    /// and is not `Verified=True`.
    ///
    /// **REQUEUED, NEVER TERMINAL** — interface **I19**, and
    /// [`ADMISSION_REQUEUE_SECS`] carries the reason.
    ApprovalNotVerified {
        /// The name `spec.approvalRef` gave.
        approval: String,
    },
    /// The ref names nothing: `spec.approvalRef.name` is empty.
    ///
    /// TERMINAL. `spec` is sealed by a CEL rule, so a `Restore` that names no
    /// approval names no approval forever, and no `Approval` anyone creates
    /// can bind to it. It is a different fact from
    /// [`Self::ApprovalNotVerified`] — "you did not ask for authorisation"
    /// rather than "your authorisation has not arrived" — and reporting the
    /// two under one name is how an operator comes to wait for an approval
    /// that will never be looked for.
    ApprovalNotReceived {
        /// Empty, and named in the message as such.
        approval: String,
    },
    /// `sha256_prefixed(spec.planBytes)` is not the `plan_hash` inside
    /// `Approval.spec.approvalBytes`.
    ///
    /// TERMINAL, and both hashes are named. The approval binds bytes; these
    /// are different bytes; and since `spec` is immutable on both objects,
    /// re-approving the exact plan is the only way forward.
    PlanHashMismatch {
        /// `sha256_prefixed(spec.planBytes.as_bytes())`, computed here.
        recomputed: String,
        /// The `plan_hash` the approval DOCUMENT names, parsed from
        /// `spec.approvalBytes`. Never `Approval.status`.
        approval_says: String,
    },
    /// `spec.target.clusterRef` resolves to nothing, or to a `KafkaCluster`
    /// whose `status.reachable` is not `Some(true)`.
    ClusterNotReachable {
        /// The name `spec.target.clusterRef` gave.
        cluster: String,
    },
}

impl RestoreAdmission {
    /// The condition `reason` this admission writes.
    ///
    /// NO WILDCARD ARM, DELIBERATELY — see the type's own note.
    #[must_use]
    pub const fn reason(&self) -> &'static str {
        match self {
            Self::Ok => REASON_ADMITTED,
            Self::ApprovalNotVerified { .. } => REASON_APPROVAL_NOT_VERIFIED,
            Self::ApprovalNotReceived { .. } => TERMINAL_STATE_APPROVAL_NOT_RECEIVED,
            Self::PlanHashMismatch { .. } => TERMINAL_STATE_PLAN_HASH_MISMATCH,
            Self::ClusterNotReachable { .. } => TERMINAL_STATE_CLUSTER_NOT_REACHABLE,
        }
    }

    /// Whether this admission ends the object's life.
    ///
    /// # The split, and the one member that is NOT terminal
    ///
    /// [`Self::ApprovalNotVerified`] is a HOLD: the fact it reports can change
    /// without anybody touching this object, so it requeues at
    /// [`ADMISSION_REQUEUE_SECS`] (interface **I19**). The other three cannot:
    /// `Restore.spec` and `Approval.spec` are both sealed by CEL rules, so an
    /// empty `approvalRef` stays empty and a plan hash that does not match
    /// never will. `ClusterNotReachable` is terminal by controller ruling
    /// (dispatch ruling 3) even though `KafkaCluster.status.reachable` is a
    /// value Task 15c refreshes — the reasoning being that an approval binds a
    /// plan to a target and a target that was down at admission time is a run
    /// an operator should re-authorise rather than one that starts by itself
    /// hours later. The tension is real and is recorded in the task report;
    /// what is NOT acceptable either way is an object with no status at all,
    /// which is what a bare requeue over an immutable spec produces (review
    /// finding MEDIUM-1, errata **E5d**).
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        match self {
            Self::Ok | Self::ApprovalNotVerified { .. } => false,
            Self::ApprovalNotReceived { .. }
            | Self::PlanHashMismatch { .. }
            | Self::ClusterNotReachable { .. } => true,
        }
    }
}

impl fmt::Display for RestoreAdmission {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ok => write!(
                f,
                "the approval is Verified=True, the recomputed plan hash matches the hash inside \
                 its own signed bytes, and the target cluster reports reachable"
            ),
            Self::ApprovalNotVerified { approval } => write!(
                f,
                "spec.approvalRef names the Approval `{approval}`, which does not exist yet or is \
                 not Verified=True; no Job is created until it is, and this object is looked at \
                 again in {ADMISSION_REQUEUE_SECS}s (interface I19)"
            ),
            Self::ApprovalNotReceived { approval } => write!(
                f,
                "spec.approvalRef.name is `{approval}` — it names nothing, so no Approval can \
                 ever bind to this Restore; spec is immutable, so create a new Restore that names \
                 one"
            ),
            Self::PlanHashMismatch {
                recomputed,
                approval_says,
            } => write!(
                f,
                "spec.planBytes hash to {recomputed} and the approval document's own plan_hash is \
                 {approval_says}; the approval authorises different bytes. The hash is recomputed \
                 at Job-creation time from spec.planBytes and read from inside \
                 Approval.spec.approvalBytes, never from either status"
            ),
            Self::ClusterNotReachable { cluster } => write!(
                f,
                "spec.target.clusterRef names the KafkaCluster `{cluster}`, which does not exist \
                 in this namespace or whose status.reachable is not true; a restore is not \
                 started against a target the control plane cannot see"
            ),
        }
    }
}

/// The `plan_hash` inside an approval's own signed bytes.
///
/// **INTERFACE I18: `approvalBytes` IS DOCUMENT TEXT, NEVER BASE64.** The
/// bytes are handed to `serde_json` with no decode step, exactly as
/// [`super::approval::evaluate`] does — a base64 layer between the approver's
/// file and the verified bytes is the class of transformation `planBytes`
/// exists to forbid, and a reader that decoded first would find no JSON
/// document, no `plan_hash`, and would report every approval as a mismatch.
///
/// `None` when the bytes are not a JSON object or carry no string
/// `plan_hash` — which is a mismatch against any real hash and is reported as
/// one, naming what the document did and did not carry.
#[must_use]
pub fn approval_plan_hash(approval: &Approval) -> Option<String> {
    serde_json::from_str::<Value>(&approval.spec.approval_bytes)
        .ok()?
        .get("plan_hash")?
        .as_str()
        .map(str::to_string)
}

/// The plan hash, **recomputed from the spec bytes**.
///
/// `sha256_prefixed(spec.planBytes.as_bytes())` — the same function
/// [`super::approval::evaluate`]'s check 7 uses, so the controller's two
/// halves cannot derive one plan's identity two ways.
#[must_use]
pub fn recomputed_plan_hash(restore: &Restore) -> String {
    sha256_prefixed(restore.spec.plan_bytes.as_bytes())
}

/// Decide whether this `Restore` may have a Job, **before one exists**.
///
/// Recomputes `sha256_prefixed(spec.planBytes)` and compares it against the
/// `Approval`'s own `plan_hash`, parsed from `spec.approvalBytes`, at
/// Job-creation time. **NEVER reads `Approval.status.matchedKeyId` or any
/// other status value as evidence** (interface **I18**);
/// `the_plan_hash_is_recomputed_from_the_spec_bytes_at_job_creation`'s second
/// arm sets `Approval.status` to carry the CORRECT hash and asserts it changes
/// nothing.
///
/// # The order, and why each step is where it is
///
/// 1. `spec.approvalRef` must NAME something → else
///    [`RestoreAdmission::ApprovalNotReceived`], terminal. First, because a
///    `Restore` that asks for no authorisation is not a `Restore` whose plan
///    hash is worth computing.
/// 2. That `Approval` must exist and be `status.verified == Some(true)` →
///    else [`RestoreAdmission::ApprovalNotVerified`], **requeued at
///    [`ADMISSION_REQUEUE_SECS`]** (interface **I19**). Before the hash,
///    because the hash inside unverified bytes is a claim nobody signed for.
/// 3. The recomputed hash must equal the approval document's own → else
///    [`RestoreAdmission::PlanHashMismatch`], terminal, naming both.
/// 4. `spec.target.clusterRef` must resolve to a `KafkaCluster` with
///    `status.reachable == Some(true)` → else
///    [`RestoreAdmission::ClusterNotReachable`]. Last, because it is the only
///    check whose answer is about the world rather than about the documents.
///
/// **PURE, AND THAT IS WHAT MAKES `no_job_exists_until_the_approval_is_verified`
/// AN ASSERTION ABOUT A VERDICT RATHER THAN ABOUT A ROUTE TABLE.** It reads no
/// clock, holds no client and creates nothing. Global Constraint 6's operator
/// half — an unapproved plan creates NOTHING — is enforced by the caller
/// running this before its first `POST`, and asserted as a zero count over a
/// route table that HAS the `POST` routes present.
#[must_use]
pub fn admit(
    restore: &Restore,
    approval: Option<&Approval>,
    cluster: Option<&KafkaCluster>,
) -> RestoreAdmission {
    // ---- 1. the ref must name something ---------------------------------
    let referent = restore.spec.approval_ref.name.trim().to_string();
    if referent.is_empty() {
        return RestoreAdmission::ApprovalNotReceived {
            approval: restore.spec.approval_ref.name.clone(),
        };
    }

    // ---- 2. and that Approval must be Verified=True ----------------------
    let Some(approval) = approval else {
        return RestoreAdmission::ApprovalNotVerified { approval: referent };
    };
    if approval.status.as_ref().and_then(|s| s.verified) != Some(true) {
        return RestoreAdmission::ApprovalNotVerified { approval: referent };
    }

    // ---- 3. the plan hash, RECOMPUTED, from inside the signed bytes ------
    let recomputed = recomputed_plan_hash(restore);
    let approval_says = approval_plan_hash(approval).unwrap_or_default();
    if recomputed != approval_says {
        return RestoreAdmission::PlanHashMismatch {
            recomputed,
            approval_says,
        };
    }

    // ---- 4. the target must report reachable -----------------------------
    let cluster_name = restore.spec.target.cluster_ref.name.clone();
    let reachable = cluster
        .and_then(|c| c.status.as_ref())
        .and_then(|s| s.reachable)
        == Some(true);
    if !reachable {
        return RestoreAdmission::ClusterNotReachable {
            cluster: cluster_name,
        };
    }

    RestoreAdmission::Ok
}

// ---------------------------------------------------------------------------
// The plan ConfigMap object
// ---------------------------------------------------------------------------

/// The plan ConfigMap, with exactly one key, whose value is `spec.planBytes`
/// **byte for byte**.
///
/// PURE, so the object a test builds is byte-identical to the one the
/// reconciler `POST`s.
///
/// NO ROUND TRIP THROUGH A TYPED STRUCT, and no `trim`, no re-indent and no
/// re-serialise. See [`PLAN_SPEC_KEY`] for the whole argument; the short form
/// is that `plan_hash` binds these exact bytes and the runner's `--spec`
/// parses them, so any transformation between the approver's file and the
/// mounted file produces a run nobody authorised.
///
/// `ownerReferences` WITH `controller: true` AND `blockOwnerDeletion: true`:
/// deleting the `Restore` garbage-collects its plan, and it is what makes the
/// 409 case decidable (see
/// [`crate::conditions::TERMINAL_STATE_PLAN_CONFIG_MAP_CONFLICT`]).
///
/// # Errors
///
/// [`RestoreError`] for an object with no namespace or UID — both unreachable
/// from the API server, both named rather than unwrapped.
pub fn plan_config_map(restore: &Restore) -> Result<ConfigMap, RestoreError> {
    let name = restore.name_any();
    let namespace = restore
        .namespace()
        .ok_or_else(|| RestoreError::NoNamespace(name.clone()))?;
    let uid = restore
        .uid()
        .ok_or_else(|| RestoreError::NoUid(name.clone()))?;
    Ok(ConfigMap {
        metadata: ObjectMeta {
            name: Some(plan_config_map_name(&name)),
            namespace: Some(namespace),
            owner_references: Some(vec![OwnerReference {
                api_version: Restore::api_version(&()).to_string(),
                kind: Restore::kind(&()).to_string(),
                name,
                uid,
                controller: Some(true),
                block_owner_deletion: Some(true),
            }]),
            ..ObjectMeta::default()
        },
        data: Some(
            [(
                PLAN_SPEC_KEY.to_string(),
                // VERBATIM. `.clone()` and nothing else.
                restore.spec.plan_bytes.clone(),
            )]
            .into_iter()
            .collect(),
        ),
        ..ConfigMap::default()
    })
}

// ---------------------------------------------------------------------------
// The runner argv
// ---------------------------------------------------------------------------

/// The unexpired approver key ids the roster names — interface **I16**.
///
/// `roster.spec.approverKeys[].keyId` minus everything in
/// `roster.status.expiredKeyIds`, in roster order. Expiry is read from the
/// STATUS the `TrustRoster` reconciler wrote (Task 16) rather than recomputed
/// from `notAfter` here, and that is deliberate: the roster reconciler holds
/// the one clock this decision is made against, and a second clock read in a
/// second controller is how one component comes to accept a key the other
/// rejects. `TrustRosterStatus::expired_key_ids`' own doc comment says so —
/// *"declared here so no consumer has to derive expiry itself from a clock it
/// does not share with the controller"*.
///
/// `None` roster — the install skipped step 1 — yields an EMPTY list rather
/// than an error, because the flag list is one part of an argv and the
/// `Approval` reconciler has already refused everything on that roster's
/// behalf with `RosterNotFound`. A `Restore` cannot be admitted without a
/// `Verified=True` approval, and no approval verifies without a roster.
///
/// **Task 22 lands the runner-side `--approver-key-ids` flag** and owns
/// `the_restore_job_projects_every_unexpired_roster_key_id`, because it owns
/// both sides; this function builds the argv that carries it.
#[must_use]
pub fn approver_key_ids(roster: Option<&TrustRoster>) -> Vec<String> {
    let Some(roster) = roster else {
        return Vec::new();
    };
    let expired: Vec<&str> = roster
        .status
        .as_ref()
        .and_then(|s| s.expired_key_ids.as_deref())
        .unwrap_or_default()
        .iter()
        .map(String::as_str)
        .collect();
    roster
        .spec
        .approver_keys
        .iter()
        .map(|e| e.key_id.clone())
        .filter(|id| !expired.contains(&id.as_str()))
        .collect()
}

/// `--triggered-by`'s value: `approval/<the Approval that authorised this run>`.
///
/// # THE FIELD THE BRIEF NAMES DOES NOT EXIST, AND INVENTING IT WOULD COST MORE
///
/// The dispatched argv reads `--triggered-by <spec.triggeredBy>`. **`Restore.spec`
/// has no `triggeredBy` field**: the shipped schema's properties are exactly
/// `approvalRef`, `backupSetRef`, `deadlineSeconds`, `planBytes`,
/// `pointInTime`, `sourceArchive` and `target` (`config/crd/restores.yaml`,
/// Task 15b), all seven required, and `.spec` is sealed by a CEL immutability
/// rule. Adding an eighth at this slot is a CRD SCHEMA change on a sealed spec
/// — it would invalidate every checked-in `Restore` fixture and every
/// `Approval` bound to one — while this task's Files block admits only field
/// DESCRIPTIONS. So the flag stays in the argv, exactly as the brief fixes it,
/// and its VALUE is derived by this function.
///
/// # Why the approval, and not a literal
///
/// `--triggered-by` is free text copied verbatim into the signed scorecard's
/// `triggered_by`, which is what an auditor reads to find out *why this run
/// happened*. The `Backup` path's answer is `schedule`
/// ([`super::backup_schedule::TRIGGERED_BY_SCHEDULE`]) because a cron trigger
/// really is the whole reason. A restore's reason is the authorisation: naming
/// the `Approval` puts the approver, the ticket and the matched key id one
/// `kubectl get approval` away from the signed document, and it is bounded
/// (one value per object) rather than free-form. A bare `restore` literal
/// would say nothing the kind does not already say.
#[must_use]
pub fn triggered_by(restore: &Restore) -> String {
    let referent = restore.spec.approval_ref.name.trim();
    if referent.is_empty() {
        // Unreachable through `reconcile_restore` — `admit` refuses an empty
        // ref terminally before any argv is built — and named rather than
        // unwrapped so a future caller cannot produce a bare `approval/`.
        return "approval/<none>".to_string();
    }
    format!("approval/{referent}")
}

/// The runner argv for `restore`, built here and consumed by nothing else.
///
/// # Every path is a MOUNT PATH, and none of them is a default
///
/// `--spec` is [`PLAN_MOUNT_PATH`]/[`PLAN_SPEC_KEY`]; the four approval-bundle
/// files are under [`APPROVAL_MOUNT_PATH`]; the signing key is
/// [`SIGNING_MOUNT_PATH`]/[`SIGNING_KEY_FILE`]; `--out` and
/// `--offset-report-out` are under [`crate::job::WORK_MOUNT_PATH`], the only
/// writable volume in a pod whose root filesystem is read-only.
///
/// # `--approver-key-ids` is repeated, one flag per id
///
/// Interface **I16**: `roster.spec.approverKeys[].keyId` minus
/// `status.expiredKeyIds`, in roster order. A roster with no unexpired
/// approver key contributes no flags at all rather than an empty value — an
/// empty string is a key id nothing matches, and passing one would turn a
/// missing roster into a signature refusal.
///
/// **Task 22 lands the flag on the runner side** (slot 15, after this one), so
/// an argv built here is not yet parseable by the shipped `logweir restore
/// run`. That ordering is the plan's and is stated in the brief; the measured
/// consequence for a live Job before Task 22 lands is in this task's report.
#[must_use]
pub fn runner_argv(restore: &Restore, approver_key_ids: &[String]) -> Vec<String> {
    let mut argv: Vec<String> = vec![
        "restore".to_string(),
        "run".to_string(),
        "--spec".to_string(),
        format!("{PLAN_MOUNT_PATH}/{PLAN_SPEC_KEY}"),
        "--approval".to_string(),
        format!("{APPROVAL_MOUNT_PATH}/{APPROVAL_DOC_FILE}"),
        "--approver-key".to_string(),
        format!("{APPROVAL_MOUNT_PATH}/{APPROVER_KEY_FILE}"),
    ];
    for id in approver_key_ids {
        argv.push("--approver-key-ids".to_string());
        argv.push(id.clone());
    }
    argv.extend([
        "--allowed-clusters".to_string(),
        format!("{APPROVAL_MOUNT_PATH}/{ALLOWED_CLUSTERS_FILE}"),
        "--signing-key".to_string(),
        format!("{SIGNING_MOUNT_PATH}/{SIGNING_KEY_FILE}"),
        "--out".to_string(),
        SCORECARD_OUT_PATH.to_string(),
        "--offset-report-out".to_string(),
        OFFSET_REPORT_OUT_PATH.to_string(),
        "--triggered-by".to_string(),
        triggered_by(restore),
    ]);
    argv
}

/// The [`RunnerJobSpec`] one `Restore` produces.
///
/// PURE, so the Job a test builds is byte-identical to the one the reconciler
/// `POST`s and an assertion over this function is an assertion over the
/// request.
///
/// # THE JOB SHAPE DOES NOT DEPEND ON `spec.target.mode`
///
/// `scratch_mode_and_new_topic_mode_produce_the_same_job_shape` asserts it
/// field by field. The mode's four differences — a marker topic, an
/// allowlisted cluster, a source-equals-target check and phase-9 teardown —
/// are all decisions the RUNNER makes from the plan document it parses. A
/// controller that gave `scratch` a different Job (a second container, another
/// mount, a different failure policy) would put half the mode's meaning in a
/// place the approval does not cover, since `planBytes` is what the approver
/// signed and the Job template is not.
///
/// # Errors
///
/// [`RestoreError`] when the object carries no namespace or UID.
pub fn runner_job_spec(
    restore: &Restore,
    cluster: &KafkaCluster,
    approver_key_ids: &[String],
) -> Result<RunnerJobSpec, RestoreError> {
    let name = restore.name_any();
    let namespace = restore
        .namespace()
        .ok_or_else(|| RestoreError::NoNamespace(name.clone()))?;
    let uid = restore
        .uid()
        .ok_or_else(|| RestoreError::NoUid(name.clone()))?;

    // Two Secret volumes, sorted by volume name so the rendered spec is a
    // function of the inputs and not of the order this function happens to
    // push them in. Both are FILES the runner opens by path: a key or a
    // signed document in an env var would appear in `kubectl describe pod`
    // for anyone with pod read.
    let mut secret_mounts = vec![
        SecretMount {
            volume: APPROVAL_VOLUME.to_string(),
            secret_name: APPROVAL_BUNDLE_SECRET.to_string(),
            mount_path: APPROVAL_MOUNT_PATH.to_string(),
            items: vec![
                (APPROVAL_DOC_FILE.to_string(), APPROVAL_DOC_FILE.to_string()),
                (APPROVAL_SIG_FILE.to_string(), APPROVAL_SIG_FILE.to_string()),
                (APPROVER_KEY_FILE.to_string(), APPROVER_KEY_FILE.to_string()),
                (
                    ALLOWED_CLUSTERS_FILE.to_string(),
                    ALLOWED_CLUSTERS_FILE.to_string(),
                ),
            ],
        },
        SecretMount {
            volume: SIGNING_VOLUME.to_string(),
            secret_name: SIGNING_KEY_SECRET.to_string(),
            mount_path: SIGNING_MOUNT_PATH.to_string(),
            items: vec![(
                SIGNING_KEY_SECRET_KEY.to_string(),
                SIGNING_KEY_FILE.to_string(),
            )],
        },
    ];
    secret_mounts.sort_by(|a, b| a.volume.cmp(&b.volume));

    let mut env_from_secret = Vec::new();
    if let Some(secret) = restore.spec.source_archive.secret_ref.as_ref() {
        env_from_secret.push(EnvFromSecret {
            name: ARCHIVE_ACCESS_KEY_ENV.to_string(),
            secret_name: secret.name.clone(),
            key: ARCHIVE_ACCESS_KEY.to_string(),
        });
        env_from_secret.push(EnvFromSecret {
            name: ARCHIVE_SECRET_KEY_ENV.to_string(),
            secret_name: secret.name.clone(),
            key: ARCHIVE_SECRET_KEY.to_string(),
        });
    }
    // THE TARGET'S SASL PASSWORD, PROJECTED AND NEVER READ. `secretKeyRef`
    // only: the controller holds no `get` on Secrets, so this is a reference
    // it writes into a pod spec and a value it cannot see. A `scramSha512`
    // cluster with no `secretRef` gets NO variable — and the runner then
    // refuses at exit 3 with `CredentialNotRenderable`, which is interface
    // I11's division of labour and not a gap on this side.
    if matches!(cluster.spec.auth.mode, AuthMode::ScramSha512) {
        if let Some(secret) = cluster.spec.auth.secret_ref.as_ref() {
            env_from_secret.push(EnvFromSecret {
                name: TARGET_PASSWORD_ENV.to_string(),
                secret_name: secret.name.clone(),
                key: TARGET_PASSWORD_SECRET_KEY.to_string(),
            });
        }
    }

    Ok(RunnerJobSpec {
        // THE JOB IS NAMED AFTER THE CR, VERBATIM — see `RunnerJobSpec::name`
        // for the 63-character argument, and step 0 of `reconcile_restore` for
        // what happens when the CR's own name is longer than that.
        name: name.clone(),
        namespace,
        owner: RunnerOwner {
            api_version: Restore::api_version(&()).to_string(),
            kind: Restore::kind(&()).to_string(),
            name,
            uid,
        },
        args: runner_argv(restore, approver_key_ids),
        deadline_seconds: restore.spec.deadline_seconds,
        service_account_name: RUNNER_SERVICE_ACCOUNT.to_string(),
        secret_mounts,
        env_from_secret,
        // `RUST_LOG` is pinned rather than inherited: below `info` the run id
        // and the exit-code meaning line are lost, and those two are how a pod
        // is correlated with the archive it read.
        //
        // …AND THE OBJECT-STORE ADDRESSING THE CONTROLLER WAS GIVEN (Task 24,
        // critique B H20). The `Restore` path's plan is `spec.planBytes` —
        // bytes an approver signed — so unlike the `Backup` path it CAN carry
        // an endpoint in its own `storage` block; forwarding the same four
        // variables here means a demo or an adopter configures the endpoint
        // ONCE, on the controller, and both kinds of runner agree. Nothing is
        // forwarded that is not set, so the default install's runner env is
        // unchanged. See `backup::ARCHIVE_ADDRESSING_ENV`.
        env_literal: {
            let mut env = vec![("RUST_LOG".to_string(), "info".to_string())];
            env.extend(backup::archive_addressing_env());
            env
        },
        plan_config_map: Some(plan_config_map_name(&restore.name_any())),
        // THE SHIPPED PIN, AND THE RECONCILER OVERWRITES IT IF THIS PROCESS
        // WAS HANDED ANOTHER IMAGE (Task 33, `job::RUNNER_IMAGE_ENV`). This
        // function is a pure function of the custom resource and stays one:
        // the override is a property of the PROCESS, read once in `main`.
        image: None,
        // AND THE COMPILED-IN `job::IMAGE_PULL_POLICY`, OVERWRITTEN THE SAME
        // WAY IF THIS PROCESS WAS HANDED ANOTHER POLICY (Task 37,
        // `job::RUNNER_PULL_POLICY_ENV`). Same argument, same one line.
        image_pull_policy: None,
    })
}

// ---------------------------------------------------------------------------
// Interface I8 — the three keys, read BY NAME from a bounded tail
// ---------------------------------------------------------------------------

/// Interface **I8**'s three evidence keys, as read off a pod log.
///
/// Three `Option`s, INDEPENDENTLY: a log carrying two of the three lines
/// yields two keys and one absence, and no key is ever derived from another.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RestoreEvidenceKeys {
    /// `scorecard-key=<key>`'s value. **Mandatory at exit 0.**
    pub scorecard: Option<String>,
    /// `sidecar-key=<key>`'s value. **Mandatory at exit 0.**
    pub sidecar: Option<String>,
    /// `offset-report-key=<key>`'s value. **Conditional**, and its absence at
    /// exit 0 is a fact about the run rather than about the log — see
    /// [`OFFSET_REPORT_KEY_PREFIX`].
    pub offset_report: Option<String>,
}

impl RestoreEvidenceKeys {
    /// Whether both MANDATORY lines were present.
    ///
    /// **TWO, NOT THREE, AND THE THIRD IS NOT A FAILURE.** Interface **I8**'s
    /// third line is printed exactly when the engine wrote an offset report
    /// (Task 9b; `docs/stability.md`), so a two-line tail at exit 0 is a
    /// complete, truthful answer. A `complete()` that required all three would
    /// raise `EvidenceRecorded=False` / `EvidenceKeysUnreadable` on every
    /// perfectly good run that had no offset report — a condition that is
    /// false about the log and about the run.
    #[must_use]
    pub fn mandatory_complete(&self) -> bool {
        self.scorecard.is_some() && self.sidecar.is_some()
    }
}

/// Read interface **I8**'s three keys off a pod log — **by key name, never by
/// position**.
///
/// # Why by name, and why a bounded tail
///
/// Plan erratum **E4**, measured: a pod log is stdout and stderr merged in
/// nondeterministic order, so "the last three lines" is not a rule in either
/// direction. This scans the final [`super::backup::KEY_SCAN_TAIL_LINES`]
/// non-empty lines through [`super::backup::tail_lines`] — the SAME tail the
/// `Backup` path uses, shared rather than reimplemented — and matches each
/// line's own prefix. The second arm of
/// `the_three_evidence_keys_are_read_from_the_final_three_stdout_lines`
/// reorders the lines and asserts each key still lands in its own field.
///
/// # Why no key is ever guessed
///
/// A scorecard key is derivable — `logweir/drills/<run_id>.json` — and a
/// controller that derived one on an unreadable log would point
/// `status.evidence.scorecardKey` at an object that may not exist. Task 24's
/// verifier would then report `Invalid` for a run whose evidence was merely
/// unread. Absent is the truthful value.
///
/// The LAST occurrence of each prefix within the tail wins: a runner that
/// logged an earlier draft of a key would have the final one be the one it
/// wrote.
#[must_use]
pub fn restore_evidence_keys(log: &str) -> RestoreEvidenceKeys {
    let mut keys = RestoreEvidenceKeys::default();
    for line in backup::tail_lines(log) {
        if let Some(v) = line.strip_prefix(SCORECARD_KEY_PREFIX) {
            keys.scorecard = Some(v.to_string());
        }
        if let Some(v) = line.strip_prefix(SIDECAR_KEY_PREFIX) {
            keys.sidecar = Some(v.to_string());
        }
        if let Some(v) = line.strip_prefix(OFFSET_REPORT_KEY_PREFIX) {
            keys.offset_report = Some(v.to_string());
        }
    }
    keys
}

// ---------------------------------------------------------------------------
// The scorecard, read as a `Value` and copied — never typed
// ---------------------------------------------------------------------------

/// The values this reconciler copies out of a signed scorecard.
///
/// **EVERY FIELD IS A COPY, AND NONE OF THEM IS A DECISION.** The document is
/// read as a `serde_json::Value` and each value is lifted out by pointer;
/// `outcome` is the scorecard's own string, `integrity.result` its own
/// kebab-case spelling, `objectives.met` its own tri-state. See this module's
/// header for why the document is never parsed into
/// the scorecard's own Rust type and re-emitted.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ScorecardObservation {
    /// `outcome` — `pass`, `fail-objective`, `fail-integrity` or
    /// `preflight-failed` in the frozen 1.0.0 enum. Interface **I21** reads
    /// this on `Restore.status.outcome`.
    pub outcome: Option<String>,
    /// `last_phase_completed`, `-1` through `9`.
    pub last_phase_completed: Option<i64>,
    /// `objectives.rto_seconds`.
    pub objective_rto_seconds: Option<i64>,
    /// `objectives.rpo_seconds`.
    pub objective_rpo_seconds: Option<i64>,
    /// `objectives.pass_rate`. `None` where the scorecard's is null.
    pub objective_pass_rate: Option<f64>,
    /// `objectives.met`. `None` where the scorecard's is null — a `passRate`
    /// was asked for and the measured rate could not be computed, so the
    /// aggregate is unmeasurable rather than satisfied (interface **I34**).
    pub objective_met: Option<bool>,
    /// `integrity.level`.
    pub integrity_level: Option<String>,
    /// `integrity.result`.
    pub integrity_result: Option<String>,
    /// `integrity.partial_reason` — interface **I34**'s second half.
    pub integrity_partial_reason: Option<String>,
    /// `measured.rto_seconds`.
    pub measured_rto_seconds: Option<i64>,
    /// `measured.rpo_seconds`.
    pub measured_rpo_seconds: Option<i64>,
    /// `evidence.offset_report_sha256`, copied from the signed document. **The
    /// controller does not compute this one**: the report's digest is inside
    /// the bytes an approver's chain of custody covers, and recomputing it
    /// would require fetching a second object to answer a question the first
    /// one already answers.
    pub offset_report_sha256: Option<String>,
    /// `sha256_prefixed` of the scorecard bytes the controller actually
    /// fetched. **Computed here and not copied**, because a document cannot
    /// carry its own digest.
    pub scorecard_sha256: Option<String>,
}

/// Guard **G-TS**'s observation, read off the pod log by key name.
///
/// Returns the object the runner printed, **filtered to the three fields
/// `crate::crds::restore::TopicPreflight` declares and no others**: a line
/// carrying a fourth key would otherwise write a property the CRD's structural
/// schema prunes, and a client reading the status back would see a field that
/// was never stored. The LAST occurrence in the tail wins, as for every other
/// key here.
///
/// `None` when no line was printed, when its value is not a JSON object, or
/// when the object carries none of the three fields — each of which leaves
/// `status.topicPreflight` absent rather than writing an empty block.
#[must_use]
pub fn topic_preflight(log: &str) -> Option<Value> {
    let mut raw: Option<&str> = None;
    for line in backup::tail_lines(log) {
        if let Some(v) = line.strip_prefix(TOPIC_PREFLIGHT_KEY_PREFIX) {
            raw = Some(v);
        }
    }
    let doc: Value = serde_json::from_str(raw?).ok()?;
    let doc = doc.as_object()?;
    let mut out = serde_json::Map::new();
    for key in ["timestampType", "retentionMs", "timestampBound"] {
        if let Some(v) = doc.get(key) {
            out.insert(key.to_string(), v.clone());
        }
    }
    if out.is_empty() {
        return None;
    }
    Some(Value::Object(out))
}

/// Read the values [`ScorecardObservation`] names out of a scorecard
/// document.
///
/// PURE, over bytes, so every status assertion in this module's tests is made
/// without a socket. `None` when the bytes are not a JSON object at all —
/// which is NOT OBSERVED, and not "the run had no outcome".
#[must_use]
pub fn scorecard_observation(bytes: &[u8]) -> Option<ScorecardObservation> {
    let doc: Value = serde_json::from_slice(bytes).ok()?;
    if !doc.is_object() {
        return None;
    }
    let s = |ptr: &str| doc.pointer(ptr).and_then(Value::as_str).map(str::to_string);
    let i = |ptr: &str| doc.pointer(ptr).and_then(Value::as_i64);
    Some(ScorecardObservation {
        outcome: s("/outcome"),
        last_phase_completed: i("/last_phase_completed"),
        objective_rto_seconds: i("/objectives/rto_seconds"),
        objective_rpo_seconds: i("/objectives/rpo_seconds"),
        objective_pass_rate: doc.pointer("/objectives/pass_rate").and_then(Value::as_f64),
        objective_met: doc.pointer("/objectives/met").and_then(Value::as_bool),
        integrity_level: s("/integrity/level"),
        integrity_result: s("/integrity/result"),
        integrity_partial_reason: s("/integrity/partial_reason"),
        measured_rto_seconds: i("/measured/rto_seconds"),
        measured_rpo_seconds: i("/measured/rpo_seconds"),
        offset_report_sha256: s("/evidence/offset_report_sha256"),
        scorecard_sha256: Some(sha256_prefixed(bytes)),
    })
}

/// What the archive-facing half of this reconciler is handed, and the reason it
/// is a parameter.
///
/// # Why an injected observation and not a `Store` handle
///
/// The scorecard read needs the controller's read-only archive handle, and
/// that handle is **interface I13** — the shared `Option<Arc<Store>>` on
/// [`super::Context`], built once in `main` before the tokio runtime exists,
/// with no write capability anywhere (Global Constraint 6). So the read is
/// written here as a pure decision over an INJECTED observation, which is what
/// lets `the_restore_status_carries_objectives_and_partial_reason` assert the
/// status this reconciler patches without a socket.
///
/// # WHY IT IS ASYNC, AND WHY THAT IS NOT DECORATION
///
/// `Store::get` is a **blocking** method that drives its own current-thread
/// runtime, and `kube` drives every reconciler ON a runtime: a direct call
/// COMPILES CLEANLY and panics with *Cannot start a runtime from within a
/// runtime* at the first reconcile. So every `Store` call in this crate goes
/// through `tokio::task::spawn_blocking(move || …).await` — interface **I13**,
/// enforced over this file's source text by
/// `tests/retention.rs::no_store_call_is_made_outside_spawn_blocking`, whose
/// `I13_FILES` names it. A synchronous `Fn` cannot `.await` anything, so the
/// oracle's TYPE has to be the async one.
///
/// `BoxFuture<'static, …>` AND NOT `BoxFuture<'a, …>`: an `Fn`'s `Output` is
/// an associated type matched exactly, so a future borrowed for `'a` makes
/// `'a` the lifetime of both the future and the `&'a dyn` — and the oracle is
/// a local, which the borrow checker then wants to be `'static`. The future
/// owns everything it reads (an `Arc<Store>` clone and the key), so `'static`
/// is what it actually is.
///
/// # `None` means NOT OBSERVED, never "the run said nothing"
///
/// A controller with no archive handle — no `LOGWEIR_ARCHIVE_URL` — must not
/// write `outcome: null` over a real outcome, and must certainly not write an
/// `objectives` block of nothing onto a run that met its objectives. A `None`
/// observation OMITS every one of those keys from the merge patch, which means
/// "leave it alone".
pub type ScorecardOracle<'a> =
    &'a (dyn Fn(String) -> BoxFuture<'static, Option<ScorecardObservation>> + Send + Sync + 'a);

/// The oracle for a controller that holds no archive handle: it observes
/// nothing.
///
/// See [`ScorecardOracle`]. This is what this module's `reconcile` entry point
/// uses when [`super::Context::archive`] is `None`, and what every unit test
/// that is not about the archive passes.
#[must_use]
pub fn unobserved_scorecard(_key: String) -> BoxFuture<'static, Option<ScorecardObservation>> {
    Box::pin(async { None })
}

/// The ONE `get` the real oracle makes, as a single blocking function.
///
/// **THE ONLY PLACE IN THIS FILE THAT TOUCHES A `Store`, AND IT IS NOT
/// `async`.** See [`ScorecardOracle`]. An unreadable object is `None` — NOT
/// OBSERVED — and never an error the reconcile returns: a run whose scorecard
/// cannot be fetched still has an exit code, and that exit code is the fact
/// the status exists to record.
#[must_use]
pub fn observe_scorecard(store: &Store, key: &str) -> Option<ScorecardObservation> {
    if key.trim().is_empty() {
        return None;
    }
    let (bytes, _version) = store.get(key).ok()?;
    scorecard_observation(&bytes)
}

// ---------------------------------------------------------------------------
// The topics, derived from the APPROVED bytes and from nothing else
// ---------------------------------------------------------------------------

/// `(oldTopics, newTopics)` — the source names and the mapped target names.
///
/// # Where these come from, and why not from the scorecard
///
/// The signed scorecard carries `target.topic_mapping_prefix`,
/// `topic_mapping_sha256` and `topic_mapping_entries` — a digest and a count,
/// never the names (Global Constraint 12 freezes the document's shape, and a
/// per-topic list would be a new property). So the names are derived from
/// `spec.planBytes`: `source.topics` verbatim for the old ones, and each of
/// them behind [`logweir_core::spec::target_topic_prefix`] for the new ones.
///
/// **THAT IS THE APPROVED BYTES AND NOTHING ELSE.** `target_topic_prefix` is
/// the ONE place the mapping rule lives — `scratch` takes
/// `target.topic_mapping_prefix`, `newTopic` takes `target.topic_naming.prefix`
/// or `default_topic_prefix` of the recovery point — so the renderer, the
/// runner's own refusal messages and this list cannot each derive it
/// differently. It reads no clock and no environment, so the answer is a pure
/// function of the document the approver signed.
///
/// # Parsing `planBytes` here is not the round trip `planBytes` forbids
///
/// What is forbidden is WRITING transformed bytes: the ConfigMap gets
/// `spec.planBytes` verbatim (see [`plan_config_map`]) and nothing this
/// function does can change that. Reading the approved document to answer a
/// question about it is what the runner does too.
///
/// `None` when `planBytes` is not a `RestoreSpec` — in which case the runner
/// will refuse the same bytes, and a fabricated topic list would be a status
/// field naming topics no run touched. **Tag 1 writes these two solely so tag
/// 2's `Switchover` retirement has a list to validate against**; nothing in
/// tag 1 reads them, and nothing in any tag writes to `oldTopics`' topics.
#[must_use]
pub fn topic_mapping(restore: &Restore) -> Option<(Vec<String>, Vec<String>)> {
    let plan: logweir_core::spec::RestoreSpec =
        serde_yaml::from_str(&restore.spec.plan_bytes).ok()?;
    let prefix = logweir_core::spec::target_topic_prefix(&plan);
    let old: Vec<String> = plan.source.topics.clone();
    let new: Vec<String> = old.iter().map(|t| format!("{prefix}{t}")).collect();
    Some((old, new))
}

/// Whether a run's exit code and scorecard `outcome` mean the archive did not
/// cover the requested window.
///
/// Exit **2** — a result that is not a pass, with a document written AND
/// signed — whose `outcome` is [`OUTCOME_FAIL_COVERAGE`] is
/// [`TERMINAL_STATE_WINDOW_NOT_COVERED`]. Every other pairing is `None` and
/// keeps Global Constraint 11's wire reason. See [`OUTCOME_FAIL_COVERAGE`] for
/// why the value is not in the frozen enum and why the mapping exists anyway.
#[must_use]
pub fn window_not_covered(exit_code: i32, outcome: Option<&str>) -> Option<&'static str> {
    if exit_code == 2 && outcome == Some(OUTCOME_FAIL_COVERAGE) {
        return Some(TERMINAL_STATE_WINDOW_NOT_COVERED);
    }
    None
}

// ---------------------------------------------------------------------------
// Status patches
// ---------------------------------------------------------------------------

/// One condition, as a merge-patch fragment.
///
/// `lastTransitionTime` moves only when the condition actually transitions,
/// and the comparison that decides it is
/// [`crate::conditions::merge_condition`] — ONE implementation for all six
/// reconcilers (plan erratum E11(d)); this file used to carry a private copy.
/// Built by serialising [`crate::crds::Condition`] so the element compares
/// byte for byte against the stored one — see `backup::condition` for why a
/// hand-built element re-opens the loop.
fn condition(
    restore: &Restore,
    r#type: &str,
    status: &str,
    reason: &str,
    message: &str,
    now: DateTime<Utc>,
) -> Value {
    json!(merge_condition(
        current_condition(
            restore.status.as_ref().and_then(|s| s.conditions.as_ref()),
            r#type,
        ),
        crate::crds::Condition {
            r#type: r#type.to_string(),
            status: status.to_string(),
            observed_generation: restore.meta().generation,
            last_transition_time: Some(now),
            reason: Some(reason.to_string()),
            message: Some(message.to_string()),
        },
    ))
}

/// Patch `/status` — unless the patch would change nothing.
///
/// The decision is [`crate::conditions::status_unchanged`]'s; this exists so
/// this reconciler's six patch sites read as one line each and the skip cannot
/// be applied at five of them and forgotten at the sixth.
async fn patch_status_if_changed(
    api: &Api<Restore>,
    restore: &Restore,
    name: &str,
    patch: Value,
) -> Result<(), RestoreError> {
    if status_unchanged(
        restore
            .status
            .as_ref()
            .and_then(|s| serde_json::to_value(s).ok())
            .as_ref(),
        &patch,
    ) {
        debug!(
            restore = %name,
            "the computed status equals the one on the object; no patch is sent"
        );
        return Ok(());
    }
    api.patch_status(name, &PatchParams::default(), &Patch::Merge(patch))
        .await
        .map_err(RestoreError::Api)?;
    Ok(())
}

/// The `/status` merge patch for an admission that is a HOLD rather than a
/// verdict — interface **I19**.
///
/// `phase: Pending` and EXACTLY ONE condition, `Admitted=False`. Not a `Failed`
/// condition: nothing failed, and errata **E5c** is about exactly this — a
/// condition array is a map keyed by `type`, so a `Failed=False` beside a
/// later `Failed=True` is a malformed status whatever the messages say. No
/// `exitCode` and no `exitReason`, because no run was attempted.
#[must_use]
pub fn admission_hold_patch(
    restore: &Restore,
    admission: &RestoreAdmission,
    now: DateTime<Utc>,
) -> Value {
    json!({
        "status": {
            "phase": PHASE_PENDING,
            // The scalar the `REASON` column reads — review finding M2. The
            // hold's own state, VERBATIM the condition's `reason` below.
            "reason": admission.reason(),
            "conditions": [condition(
                restore,
                CONDITION_ADMITTED,
                "False",
                admission.reason(),
                &admission.to_string(),
                now,
            )],
        }
    })
}

/// The `/status` merge patch for a run this CONTROLLER refused terminally,
/// before any `POST`.
///
/// TERMINAL, WITH NO `exitCode`, AND NEVER A REQUEUE. Nothing ran, so there is
/// no code to lift and none is invented; `exitReason` is
/// [`REASON_OPERATIONAL`], which is what Global Constraint 11's code 1 means —
/// the run could not be attempted and no artifact was written — and the
/// SUB-CASE is the condition's `reason`.
///
/// WHY A STATUS AND NOT AN ERROR. A refusal that reaches `error_policy`
/// becomes a 15-second requeue, which for a refusal over a CEL-immutable spec
/// is an infinite loop with an EMPTY status: no phase, no condition, an empty
/// `PHASE` column, and nothing for `kubectl describe restore` to say. That is
/// review finding MEDIUM-1 and errata **E5d**, measured on a 64-character
/// `Backup`.
///
/// Its condition array goes through [`crate::verification::carry_verified`]
/// for [`crashed_status_patch`]'s reason.
#[must_use]
pub fn refused_status_patch(
    restore: &Restore,
    reason: &str,
    message: &str,
    now: DateTime<Utc>,
) -> Value {
    let conditions = crate::verification::carry_verified(
        restore.status.as_ref().and_then(|s| s.conditions.as_ref()),
        vec![condition(
            restore,
            CONDITION_FAILED,
            "True",
            reason,
            message,
            now,
        )],
    );
    json!({
        "status": {
            "phase": PHASE_FAILED,
            "exitReason": REASON_OPERATIONAL,
            // THIS IS THE PATCH REVIEW FINDING M2 IS ABOUT. `exitReason` above
            // is `operational` for every self-decided refusal, because GC11
            // has no code for "refused before anything ran" — so the `REASON`
            // column printed `operational` for `ApprovalNotReceived`,
            // `PlanHashMismatch`, `ClusterNotReachable` and `NameTooLong`
            // alike. `reason` is the SUB-CASE, verbatim the condition's own.
            "reason": reason,
            "conditions": conditions,
        }
    })
}

/// The `/status` merge patch for a Job that exists and has not finished.
///
/// BUILT AS JSON AND NOT BY SERIALISING `RestoreStatus`, for merge-patch
/// semantics: an absent key means "leave it alone", which is exactly what a
/// running reconcile wants for the fields a finished one will write.
#[must_use]
pub fn running_status_patch(
    restore: &Restore,
    job_name: &str,
    admitted: bool,
    now: DateTime<Utc>,
) -> Value {
    let mut conditions = Vec::new();
    // THE ADMISSION CONDITION IS WRITTEN ON THE CREATING PASS ONLY. A later
    // pass over a running Job re-asserts nothing about an approval it did not
    // re-read; the condition it wrote is still on the object, and a merge
    // patch that omitted it would leave it alone anyway — the `conditions`
    // array is replaced wholesale by a merge patch, so re-sending it on every
    // pass is what keeps it from being dropped.
    if admitted {
        conditions.push(condition(
            restore,
            CONDITION_ADMITTED,
            "True",
            REASON_ADMITTED,
            &RestoreAdmission::Ok.to_string(),
            now,
        ));
    }
    conditions.push(condition(
        restore,
        CONDITION_JOB_CREATED,
        "True",
        CONDITION_JOB_CREATED,
        &format!("the runner Job {job_name} exists and has not finished"),
        now,
    ));
    json!({
        "status": {
            "phase": PHASE_RUNNING,
            "jobRef": { "name": job_name },
            // THE *CURRENT* CONDITION, WHICH IS `JobCreated` ON BOTH PASSES —
            // not `Admitted`, which is the FIRST element of `conditions` on
            // the creating pass only. A scalar that read the array's head
            // would print `Admitted` once and `JobCreated` for every later
            // reconcile of an unchanged running object; the run's state is
            // "the Job exists and has not finished" throughout. Verbatim the
            // `JobCreated` condition's own `reason` (review finding M2).
            "reason": CONDITION_JOB_CREATED,
            "conditions": conditions,
        }
    })
}

/// The `/status` merge patch for a finished Job whose `runner` container
/// terminated.
///
/// # What is copied, and what is decided
///
/// `exitCode` and the condition are DECIDED from the pod. Everything else —
/// `outcome`, `integrity`, `measured`, `objectives`, `lastPhaseCompleted` and
/// the evidence digests — is COPIED VERBATIM out of the scorecard the runner
/// signed, through [`ScorecardObservation`], and every one of them is OMITTED
/// when the archive was not observed. A merge patch with no key means "leave
/// it alone", which is the only honest thing to write about a document that
/// could not be fetched.
///
/// # `status.topicPreflight` IS NOT WRITTEN, AND THERE IS NO PRODUCER FOR IT
///
/// Guard **G-TS**'s observation is returned by phase 0 in
/// `logweir::drill::RestoreOutcome::topic_preflight` and, by Global Constraint
/// 12 as amended, is deliberately NOT a scorecard field. Nothing carries it
/// out of the pod: interface **I8** fixes three stdout key lines and none of
/// them is a preflight, and the controller reads the pod's log and the archive
/// and nothing else. So the field stays ABSENT rather than fabricated — the
/// same rule the evidence keys follow — and the gap is recorded in the CRD
/// field's own description and in the task report.
///
/// # `EvidenceRecorded` exists only at exit 0, and only over the two mandatory
/// keys
///
/// Errata **E5c**. Global Constraint 11 says exits 1, 3 and 4 write no
/// artifact, so "no evidence key lines" is the expected shape there and a
/// condition about it would be a condition about nothing.
/// [`RestoreEvidenceKeys::mandatory_complete`] is the predicate, and the third
/// key's absence is recorded as an absence rather than as a failure.
/// `#[allow(clippy::too_many_arguments)]`, AND THE REASON IS THE FUNCTION'S
/// WHOLE POINT. This is a PURE patch builder: every parameter is one
/// independent OBSERVATION the reconcile made, and the value of the function
/// is that a test can construct any combination of them and assert the exact
/// bytes that reach `/status`. Bundling them into a struct to satisfy the
/// seven-argument lint would move the combinations into a constructor and
/// change nothing about how many facts the status carries — while making every
/// existing assertion in `tests/{backup,restore}_controller.rs` read one level
/// further from the patch it is about. Task 24 took the eighth argument (the
/// receipt digest on the `Backup` path, the topic-preflight observation on the
/// `Restore` path) and this allow with it.
#[allow(clippy::too_many_arguments)]
#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn finished_status_patch(
    restore: &Restore,
    exit_code: i32,
    keys: &RestoreEvidenceKeys,
    refusal: Option<&str>,
    observed: Option<&ScorecardObservation>,
    topics: Option<&(Vec<String>, Vec<String>)>,
    preflight: Option<&Value>,
    now: DateTime<Utc>,
) -> Value {
    // TWO VOCABULARIES, TWO FIELDS (errata E5b). The CONDITION's `reason` is
    // CamelCase, because that is what a `metav1.Condition`'s own validation
    // pattern permits; `exitReason` keeps GC11's wire string, which is what the
    // CRD's field description and `docs/kubernetes.md`'s table already say.
    let wire_reason = wire_reason_for_exit(exit_code);
    let cond_reason = reason_for_exit(exit_code);
    let (phase, cond_type) = if exit_code == 0 {
        (PHASE_SUCCEEDED, CONDITION_COMPLETE)
    } else {
        (PHASE_FAILED, CONDITION_FAILED)
    };

    let mut conditions = vec![condition(
        restore,
        cond_type,
        "True",
        cond_reason,
        &format!(
            "the runner exited {exit_code} ({wire_reason}); the code was read from \
             status.containerStatuses[name={CONTAINER_NAME}].state.terminated.exitCode"
        ),
        now,
    )];
    if exit_code == 0 {
        let (status, reason, message) = if keys.mandatory_complete() {
            (
                "True",
                REASON_EVIDENCE_KEYS_RECORDED,
                "both `scorecard-key=` and `sidecar-key=` were read off the pod log by name and \
                 are on status.evidence; `offset-report-key=` is printed only when the engine \
                 wrote an offset report, so its absence is a fact about the run",
            )
        } else {
            (
                "False",
                REASON_EVIDENCE_KEYS_UNREADABLE,
                "the pod log did not carry both `scorecard-key=` and `sidecar-key=`; neither \
                 mandatory evidence key is set, and none was guessed from the run id",
            )
        };
        conditions.push(condition(
            restore,
            CONDITION_EVIDENCE_RECORDED,
            status,
            reason,
            message,
            now,
        ));
    }

    // The terminal state, when there is one, is the most specific thing known
    // about the run: the archive-coverage verdict on exit 2, or the guard's
    // own state on exit 3. Otherwise GC11's wire reason.
    let coverage = window_not_covered(exit_code, observed.and_then(|o| o.outcome.as_deref()));
    let exit_reason = coverage.or(refusal).unwrap_or(wire_reason);

    let mut status = serde_json::Map::new();
    status.insert("phase".to_string(), json!(phase));
    status.insert("exitCode".to_string(), json!(exit_code));
    status.insert("exitReason".to_string(), json!(exit_reason));
    // The scalar the `REASON` column reads (review finding M2): the reason of
    // the TERMINAL condition — `conditions[0]`, the `Complete`/`Failed` one —
    // and never the `EvidenceRecorded` condition appended after it. On this
    // path `exitReason` is genuinely informative (it carries the runner's own
    // `refusal-reason=` state), so the two fields agree in spirit and differ
    // in vocabulary exactly as errata E5b requires.
    status.insert("reason".to_string(), json!(cond_reason));
    // THE EXISTING `Verified` CONDITION IS CARRIED FORWARD, and without this
    // line the controller hot-loops: a merge patch REPLACES arrays, so this
    // one would delete the condition the SECOND patch adds, which would re-add
    // it, which would wake this reconciler again. Measured at 20 reconciles
    // per second on the Phase B run. See `verification::carry_verified`.
    let conditions = crate::verification::carry_verified(
        restore.status.as_ref().and_then(|s| s.conditions.as_ref()),
        conditions,
    );
    status.insert("conditions".to_string(), json!(conditions));

    let mut evidence = serde_json::Map::new();
    if let Some(k) = keys.scorecard.as_ref() {
        evidence.insert("scorecardKey".to_string(), json!(k));
    }
    if let Some(k) = keys.sidecar.as_ref() {
        evidence.insert("sidecarKey".to_string(), json!(k));
    }
    if let Some(k) = keys.offset_report.as_ref() {
        evidence.insert("offsetReportKey".to_string(), json!(k));
    }
    if let Some(o) = observed {
        if let Some(d) = o.scorecard_sha256.as_ref() {
            evidence.insert("scorecardSha256".to_string(), json!(d));
        }
        if let Some(d) = o.offset_report_sha256.as_ref() {
            evidence.insert("offsetReportSha256".to_string(), json!(d));
        }
    }
    if !evidence.is_empty() {
        status.insert("evidence".to_string(), Value::Object(evidence));
    }

    if let Some(o) = observed {
        if let Some(v) = o.outcome.as_ref() {
            status.insert("outcome".to_string(), json!(v));
        }
        if let Some(v) = o.last_phase_completed {
            status.insert("lastPhaseCompleted".to_string(), json!(v));
        }
        let objectives = objectives_block(o);
        if !objectives.is_empty() {
            status.insert("objectives".to_string(), Value::Object(objectives));
        }
        let integrity = integrity_block(o);
        if !integrity.is_empty() {
            status.insert("integrity".to_string(), Value::Object(integrity));
        }
        let measured = measured_block(o);
        if !measured.is_empty() {
            status.insert("measured".to_string(), Value::Object(measured));
        }
    }

    if let Some((old, new)) = topics {
        status.insert("oldTopics".to_string(), json!(old));
        status.insert("newTopics".to_string(), json!(new));
    }

    // GUARD **G-TS**, erratum **E10(c)**. Omitted when the runner printed no
    // preflight line: a merge patch with no key means "leave it alone", and an
    // absent observation must not overwrite one a previous pass recorded.
    if let Some(p) = preflight {
        status.insert("topicPreflight".to_string(), p.clone());
    }

    json!({ "status": Value::Object(status) })
}

/// `status.objectives` — interface **I34**, first half.
///
/// The scorecard's four values, camelCased field names, **verbatim values**;
/// `passRate` and `met` are absent where the scorecard's are null. Absent and
/// not `null`: `met: null` on a merge patch means "leave the field alone",
/// which is the same wire shape as omitting it, and writing an explicit null
/// would require a JSON-patch this reconciler does not use.
#[must_use]
pub fn objectives_block(o: &ScorecardObservation) -> serde_json::Map<String, Value> {
    let mut m = serde_json::Map::new();
    if let Some(v) = o.objective_rto_seconds {
        m.insert("rtoSeconds".to_string(), json!(v));
    }
    if let Some(v) = o.objective_rpo_seconds {
        m.insert("rpoSeconds".to_string(), json!(v));
    }
    if let Some(v) = o.objective_pass_rate {
        m.insert("passRate".to_string(), json!(v));
    }
    if let Some(v) = o.objective_met {
        m.insert("met".to_string(), json!(v));
    }
    m
}

/// `status.integrity` — including interface **I34**'s `partialReason`.
///
/// `partialReason` IS THE POINT OF THE BLOCK. A `partial` integrity result
/// with no reason is a badge an auditor cannot act on, and spec §8 requires
/// the UI to render it; the scorecard's own invariant check refuses a
/// `partial` document whose `partial_reason` is blank, so a document that
/// reaches here with one is a document that names it.
#[must_use]
pub fn integrity_block(o: &ScorecardObservation) -> serde_json::Map<String, Value> {
    let mut m = serde_json::Map::new();
    if let Some(v) = o.integrity_level.as_ref() {
        m.insert("level".to_string(), json!(v));
    }
    if let Some(v) = o.integrity_result.as_ref() {
        m.insert("result".to_string(), json!(v));
    }
    if let Some(v) = o.integrity_partial_reason.as_ref() {
        m.insert("partialReason".to_string(), json!(v));
    }
    m
}

/// `status.measured` — what the run achieved, as against what was asked for.
#[must_use]
pub fn measured_block(o: &ScorecardObservation) -> serde_json::Map<String, Value> {
    let mut m = serde_json::Map::new();
    if let Some(v) = o.measured_rto_seconds {
        m.insert("rtoSeconds".to_string(), json!(v));
    }
    if let Some(v) = o.measured_rpo_seconds {
        m.insert("rpoSeconds".to_string(), json!(v));
    }
    m
}

/// The `/status` merge patch for the crashed-Job case — a Job that finished
/// with **no terminated state for `runner`**.
///
/// `exitCode` IS ABSENT, AND ITS ABSENCE IS THE ASSERTION. The status says
/// "this run has no exit code" rather than fabricating a `1` (indistinguishable
/// from a real operational failure) or a `0` (a green badge for a run that
/// never reported).
///
/// THE CONDITION ARRAY GOES THROUGH [`crate::verification::carry_verified`],
/// as [`finished_status_patch`]'s does — belt and braces beside the
/// already-terminal guard in `reconcile_restore`, and for the reason the
/// `Backup` twin states: a merge patch REPLACES arrays, so a builder that owns
/// the array owes the parts of it that are not its own.
#[must_use]
pub fn crashed_status_patch(
    restore: &Restore,
    terminal_state: &str,
    job_name: &str,
    now: DateTime<Utc>,
) -> Value {
    let conditions = crate::verification::carry_verified(
        restore.status.as_ref().and_then(|s| s.conditions.as_ref()),
        vec![condition(
            restore,
            CONDITION_FAILED,
            "True",
            terminal_state,
            "the Job finished but no container named runner reported a terminated state; \
             the exit code is unrecoverable",
            now,
        )],
    );
    json!({
        "status": {
            "phase": PHASE_FAILED,
            "exitReason": REASON_OPERATIONAL,
            "jobRef": { "name": job_name },
            // Review finding M2 again: with no exit code there is no wire
            // reason but `operational`, so `DisruptedMidDrill`,
            // `PodUnschedulable` and `NoExitCode` were indistinguishable in
            // the `REASON` column. Verbatim the condition's own `reason`.
            "reason": terminal_state,
            "conditions": conditions,
        }
    })
}

// ---------------------------------------------------------------------------
// Outcome, error, and the reconcile itself
// ---------------------------------------------------------------------------

/// When this object should be looked at again.
///
/// A NAMED THREE-STATE VALUE RATHER THAN A `kube::runtime::Action`, because
/// `Action` implements neither `PartialEq` nor a readable `Debug` a test can
/// assert an interval off. Interface **I19**'s whole property is *"the
/// returned `Action` is a requeue of 30 s"*, and
/// `an_approval_that_does_not_exist_yet_requeues_at_thirty_seconds` asserts it
/// on this value; [`action_for`] is the one place it becomes an `Action`, and
/// `the_requeue_maps_onto_the_action_the_runtime_gets` asserts that mapping.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Requeue {
    /// Nothing will change until something edits this object or its Job:
    /// `Action::await_change()`. **Every terminal refusal is this**, and a
    /// terminal refusal that requeued instead would spin forever over a
    /// CEL-immutable spec.
    AwaitChange,
    /// Look again after this many seconds.
    After(u64),
}

/// What one reconcile did.
#[derive(Clone, Debug, PartialEq)]
pub struct RestoreOutcome {
    /// The Job's name — always the `Restore`'s own name.
    pub job_name: String,
    /// Whether this reconcile created the Job.
    pub created: bool,
    /// The admission verdict, when one was reached this pass. `None` on a pass
    /// that never got to admission — a Job already existed.
    pub admission: Option<RestoreAdmission>,
    /// The exit code that reached `status.exitCode`, when there was one.
    pub exit_code: Option<i32>,
    /// The terminal state that reached the condition's `reason`, when a
    /// terminal path was taken.
    pub terminal_state: Option<String>,
    /// Interface **I8**'s three keys, as read.
    pub keys: RestoreEvidenceKeys,
    /// Whether the Job was patched with `ttlSecondsAfterFinished`.
    pub ttl_patched: bool,
    /// When to look again — see [`Requeue`].
    pub requeue: Requeue,
}

/// The `kube::runtime::Action` one outcome asks for.
///
/// The ONE place [`Requeue`] becomes an `Action`, so the interval a test
/// asserts and the interval the runtime gets cannot drift apart.
#[must_use]
pub fn action_for(outcome: &RestoreOutcome) -> Action {
    match outcome.requeue {
        Requeue::AwaitChange => Action::await_change(),
        Requeue::After(secs) => Action::requeue(std::time::Duration::from_secs(secs)),
    }
}

/// Anything that is not an outcome. Requeues; writes nothing.
#[derive(Debug)]
pub enum RestoreError {
    /// No `metadata.namespace`. Unreachable from the API server; named rather
    /// than unwrapped.
    NoNamespace(String),
    /// No `metadata.uid`, so no owner reference can be built.
    NoUid(String),
    /// The API server could not be talked to. Requeue.
    Api(kube::Error),
    /// A TERMINAL REFUSAL THIS CONTROLLER DECIDED BY ITSELF, carrying the
    /// terminal state and the message its condition names.
    ///
    /// NOT A REQUEUE, and that is the whole reason the variant exists — see
    /// [`refused_status_patch`]. `reconcile_restore` converts this into a
    /// status patch and returns an OUTCOME; it never reaches `error_policy`.
    Refused(&'static str, String),
}

impl fmt::Display for RestoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoNamespace(name) => {
                write!(f, "the object {name} carries no metadata.namespace")
            }
            Self::NoUid(name) => write!(f, "the object {name} carries no metadata.uid"),
            Self::Api(e) => write!(f, "kubernetes API error: {e}"),
            Self::Refused(state, message) => write!(f, "{state}: {message}"),
        }
    }
}

impl std::error::Error for RestoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::NoNamespace(_) | Self::NoUid(_) | Self::Refused(..) => None,
            Self::Api(e) => Some(e),
        }
    }
}

impl From<kube::Error> for RestoreError {
    fn from(e: kube::Error) -> Self {
        Self::Api(e)
    }
}

/// Whether `status` is already terminal, so a finished run is not re-run.
///
/// Reading `phase` and not `exitCode`: the crashed-Job case and every
/// controller-side refusal write a terminal phase with NO exit code on
/// purpose, and a check that keyed on the code would re-create the Job for
/// exactly the runs whose code is unrecoverable or never existed.
#[must_use]
pub fn status_is_terminal(restore: &Restore) -> bool {
    matches!(
        restore.status.as_ref().and_then(|s| s.phase.as_deref()),
        Some(PHASE_SUCCEEDED | PHASE_FAILED)
    )
}

/// `GET` the `Approval` `spec.approvalRef` names, if it names one.
///
/// A 404 IS `None` AND IS NOT AN ERROR — interface **I19**. It is the shape a
/// `Restore` created before its `Approval` is in, and [`admit`] turns it into
/// a 30-second hold. A transport failure IS an error and IS a requeue: it is
/// not a verdict about anybody's approval.
///
/// # Errors
///
/// [`RestoreError::Api`] for any API error that is not a 404.
async fn get_approval(
    restore: &Restore,
    client: &kube::Client,
    namespace: &str,
) -> Result<Option<Approval>, RestoreError> {
    let referent = restore.spec.approval_ref.name.trim().to_string();
    if referent.is_empty() {
        return Ok(None);
    }
    let api: Api<Approval> = Api::namespaced(client.clone(), namespace);
    api.get_opt(&referent).await.map_err(RestoreError::Api)
}

/// `GET` the target `KafkaCluster`.
///
/// `None` for a 404 — [`admit`] reports it as
/// [`RestoreAdmission::ClusterNotReachable`], which names the same fact as a
/// cluster whose probe failed: the control plane cannot see the target.
///
/// # Errors
///
/// [`RestoreError::Api`] for any API error that is not a 404.
async fn get_target_cluster(
    restore: &Restore,
    client: &kube::Client,
    namespace: &str,
) -> Result<Option<KafkaCluster>, RestoreError> {
    let referent = restore.spec.target.cluster_ref.name.clone();
    let api: Api<KafkaCluster> = Api::namespaced(client.clone(), namespace);
    api.get_opt(&referent).await.map_err(RestoreError::Api)
}

/// `GET` the one cluster-scoped `TrustRoster`, for the argv's key ids.
///
/// **AFTER the admission, deliberately.** An unapproved plan creates nothing
/// (Global Constraint 6) and it also READS nothing it does not need: the
/// roster is an argv input, so it is fetched only once a Job is going to
/// exist. `RosterLoad::NotFound` yields `None` — see [`approver_key_ids`].
///
/// # Errors
///
/// [`RestoreError::Api`] for a transport failure, which is a requeue.
async fn get_roster(client: &kube::Client) -> Result<Option<TrustRoster>, RestoreError> {
    match super::approval::load_roster(client)
        .await
        .map_err(RestoreError::Api)?
    {
        super::approval::RosterLoad::Found(roster) => Ok(Some(*roster)),
        super::approval::RosterLoad::NotFound => Ok(None),
    }
}

/// `POST` the plan ConfigMap, and decide the 409.
///
/// A 409 IS SUCCESS ONLY IF THE EXISTING OBJECT IS OURS. The ordinary 409 is
/// this same reconcile's previous pass: the object is byte-identical, because
/// its one key is `spec.planBytes` and `spec` is immutable. A 409 on an object
/// owned by something else is a plan document a stranger wrote, at the mount
/// path of a pod that holds this `Restore`'s signing key, and it is terminal.
///
/// THE CONFIGMAP WRITE IS A KUBERNETES API WRITE, NOT AN ARCHIVE WRITE.
/// `scripts/check-no-archive-write.sh` is unaffected: its control-plane token
/// list forbids the writable `Store` constructor and the put family.
///
/// # Errors
///
/// Whatever [`plan_config_map`] refuses;
/// [`TERMINAL_STATE_PLAN_CONFIG_MAP_CONFLICT`] for a foreign existing object;
/// [`RestoreError::Api`] for anything transient.
async fn write_plan_config_map(
    restore: &Restore,
    client: &kube::Client,
    namespace: &str,
) -> Result<(), RestoreError> {
    let name = restore.name_any();
    let cm_name = plan_config_map_name(&name);
    let uid = restore
        .uid()
        .ok_or_else(|| RestoreError::NoUid(name.clone()))?;
    let desired = plan_config_map(restore)?;
    let maps: Api<ConfigMap> = Api::namespaced(client.clone(), namespace);

    match maps.create(&PostParams::default(), &desired).await {
        Ok(_) => {
            info!(
                restore = %name,
                namespace = %namespace,
                config_map = %cm_name,
                key = PLAN_SPEC_KEY,
                "wrote spec.planBytes verbatim into the plan ConfigMap the runner Job mounts at \
                 /plan"
            );
            Ok(())
        }
        Err(kube::Error::Api(e)) if e.code == 409 => {
            let existing = maps
                .get_opt(&cm_name)
                .await
                .map_err(RestoreError::Api)?
                .ok_or_else(|| {
                    // A 409 followed by a 404 is a race with a deletion, and a
                    // race IS transient — but it is reported as the conflict
                    // it looked like rather than silently retried, because a
                    // second `POST` in the same pass would race the same way.
                    RestoreError::Refused(
                        TERMINAL_STATE_PLAN_CONFIG_MAP_CONFLICT,
                        format!(
                            "the ConfigMap {cm_name} answered 409 to a create and 404 to the \
                             read that followed it"
                        ),
                    )
                })?;
            if backup::is_owned_by(&existing, &uid) {
                debug!(
                    restore = %name,
                    namespace = %namespace,
                    config_map = %cm_name,
                    "the plan ConfigMap already exists and is owned by this Restore; its one key \
                     is spec.planBytes and spec is immutable, so it is the same bytes"
                );
                Ok(())
            } else {
                Err(RestoreError::Refused(
                    TERMINAL_STATE_PLAN_CONFIG_MAP_CONFLICT,
                    format!(
                        "the ConfigMap {cm_name} already exists and carries no controller owner \
                         reference with this Restore's UID; the runner Job would mount a plan \
                         document this object did not render, in the pod that holds the signing \
                         key"
                    ),
                ))
            }
        }
        Err(e) => Err(RestoreError::Api(e)),
    }
}

/// Find the pod the exit code is read from.
///
/// The prefixed selector first, the legacy one as a fallback — the same two,
/// in the same order, as the `Backup` path, through
/// [`super::backup::pod_selectors`] so the two reconcilers cannot disagree
/// about which label the job controller sets.
async fn find_pod(
    client: &kube::Client,
    namespace: &str,
    job_name: &str,
) -> Result<Option<Pod>, RestoreError> {
    let api: Api<Pod> = Api::namespaced(client.clone(), namespace);
    for selector in backup::pod_selectors(job_name) {
        let list = api
            .list(&ListParams::default().labels(&selector))
            .await
            .map_err(RestoreError::Api)?;
        if let Some(pod) = list.items.into_iter().next() {
            return Ok(Some(pod));
        }
        debug!(
            job = %job_name,
            selector = %selector,
            "no pod matched this selector; trying the next"
        );
    }
    Ok(None)
}

/// Reconcile one `Restore` at the instant `now`.
///
/// # The state machine, one pass per event
///
/// 0. **The object's own name is longer than [`crate::slot::NAME_LIMIT`]** → a
///    TERMINAL status, and nothing is created. Before any `POST`, because the
///    Job the API server would refuse is a Job whose pods could never be
///    labelled, and a requeue on a refusal that can never succeed leaves the
///    CR with NO STATUS AT ALL (errata **E5d**).
/// 1. **No Job and no terminal status** → `GET` the `Approval` and the target
///    `KafkaCluster`, run [`admit`], and **only then**:
///    * `Ok` → `GET` the roster, `POST` the plan ConfigMap, `POST` the Job,
///      patch `phase: Running`;
///    * a HOLD → patch `phase: Pending` with one `Admitted=False` condition
///      and requeue at [`ADMISSION_REQUEUE_SECS`] (interface **I19**);
///    * terminal → patch the refusal and await a change.
///
///    **Zero `POST`s on either refusal** — Global Constraint 6, asserted as a
///    zero count over a route table that has the `POST` routes present.
/// 2. **Job exists, not finished** → `phase: Running`, `jobRef` set.
/// 3. **Job finished, `runner` terminated** → read `exitCode` from that
///    container's `state.terminated.exitCode`; `get` the pod's log through the
///    `pods/log` subresource, scan the bounded tail BY KEY NAME for the
///    refusal reason and interface **I8**'s three keys; fetch the scorecard
///    through the archive oracle; patch the status; **and only after that
///    returns 200**, `PATCH` the Job with `ttlSecondsAfterFinished`.
/// 4. **Job finished, no terminated state for `runner`** → the crashed-Job
///    case: a TERMINAL status naming the sub-case, with `exitCode` absent.
///
/// # No clock read in this function
///
/// `now` is an argument: the one clock read is in the `kube::runtime` wrapper,
/// before anything is decided, so every assertion below is over a value.
///
/// # Errors
///
/// [`RestoreError`] for anything that is not an outcome.
pub async fn reconcile_restore(
    restore: &Restore,
    client: &kube::Client,
    scorecard: ScorecardOracle<'_>,
    verify: VerifyOracle<'_>,
    now: DateTime<Utc>,
) -> Result<RestoreOutcome, RestoreError> {
    reconcile_restore_with_runner_image(
        restore,
        client,
        scorecard,
        verify,
        now,
        &job::RunnerImage::default(),
    )
    .await
}

/// [`reconcile_restore`], with the runner image and pull policy this controller
/// process was handed — Task 33, and Task 37's policy beside it.
///
/// `runner.image` is `None` for the shipped pin `job::RUNNER_IMAGE` and
/// `Some(reference)` for the value `main` read out of `job::RUNNER_IMAGE_ENV`;
/// `runner.image_pull_policy` is `None` for the compiled-in
/// `job::IMAGE_PULL_POLICY` and `Some(policy)` for `job::RUNNER_PULL_POLICY_ENV`'s.
/// The pair becomes `job::RunnerJobSpec::{image, image_pull_policy}` and changes
/// nothing else about the Job. It is a second function
/// rather than a sixth parameter for the reason
/// [`super::backup::reconcile_backup_with_runner_image`] gives: the thirty-two
/// rows in `tests/restore_controller.rs` are not about the image.
///
/// # Errors
///
/// [`RestoreError`] for anything that is not an outcome.
pub async fn reconcile_restore_with_runner_image(
    restore: &Restore,
    client: &kube::Client,
    scorecard: ScorecardOracle<'_>,
    verify: VerifyOracle<'_>,
    now: DateTime<Utc>,
    runner: &job::RunnerImage,
) -> Result<RestoreOutcome, RestoreError> {
    match reconcile_restore_inner(restore, client, scorecard, verify, now, runner).await {
        Err(RestoreError::Refused(state, message)) => {
            // THE ONE PLACE A SELF-DECIDED REFUSAL IS WRITTEN. Every refusal
            // inside the reconcile is a `?` on `RestoreError::Refused`, so the
            // status write cannot be forgotten at one of them.
            let name = restore.name_any();
            let namespace = restore
                .namespace()
                .ok_or_else(|| RestoreError::NoNamespace(name.clone()))?;
            warn!(
                restore = %name,
                namespace = %namespace,
                terminal_state = state,
                reason = %message,
                "refusing this Restore terminally: nothing was created, and a requeue over an \
                 immutable spec would never succeed"
            );
            if !status_is_terminal(restore) {
                let restores: Api<Restore> = Api::namespaced(client.clone(), &namespace);
                patch_status_if_changed(
                    &restores,
                    restore,
                    &name,
                    refused_status_patch(restore, state, &message, now),
                )
                .await?;
            }
            Ok(RestoreOutcome {
                job_name: name,
                created: false,
                admission: None,
                exit_code: None,
                terminal_state: Some(state.to_string()),
                keys: RestoreEvidenceKeys::default(),
                ttl_patched: false,
                requeue: Requeue::AwaitChange,
            })
        }
        other => other,
    }
}

/// [`reconcile_restore`]'s body. See that function for the state machine; the
/// split exists so a `RestoreError::Refused` raised anywhere below reaches
/// exactly one status write.
async fn reconcile_restore_inner(
    restore: &Restore,
    client: &kube::Client,
    scorecard: ScorecardOracle<'_>,
    verify: VerifyOracle<'_>,
    now: DateTime<Utc>,
    runner: &job::RunnerImage,
) -> Result<RestoreOutcome, RestoreError> {
    let name = restore.name_any();
    let namespace = restore
        .namespace()
        .ok_or_else(|| RestoreError::NoNamespace(name.clone()))?;
    // The Job is named after the CR, VERBATIM.
    let job_name = name.clone();
    let jobs: Api<Job> = Api::namespaced(client.clone(), &namespace);
    let restores: Api<Restore> = Api::namespaced(client.clone(), &namespace);

    // STEP 0. THE OBJECT'S OWN NAME, BEFORE ANY `POST` — errata E5d.
    if name.len() > crate::slot::NAME_LIMIT {
        return Err(RestoreError::Refused(
            TERMINAL_STATE_NAME_TOO_LONG,
            format!(
                "the object name is {} characters and the pod label `{}` may carry at most {}; \
                 the Job's pods could not be labelled, so their exit code could never be read",
                name.len(),
                backup::JOB_NAME_LABEL,
                crate::slot::NAME_LIMIT
            ),
        ));
    }

    let existing = jobs.get_opt(&job_name).await.map_err(RestoreError::Api)?;

    // STEP 1. Nothing running and nothing terminal: admit, then create.
    let Some(job) = existing else {
        if status_is_terminal(restore) {
            // A finished run whose Job has been garbage-collected.
            // Re-creating it would re-run a restore whose scorecard is already
            // signed and already in the bucket.
            debug!(
                restore = %name,
                namespace = %namespace,
                "the Job is gone and the status is terminal; nothing to do"
            );
            return Ok(RestoreOutcome {
                job_name,
                created: false,
                admission: None,
                exit_code: restore.status.as_ref().and_then(|s| s.exit_code),
                terminal_state: None,
                keys: RestoreEvidenceKeys::default(),
                ttl_patched: false,
                requeue: Requeue::AwaitChange,
            });
        }

        // THE ADMISSION, BEFORE THE FIRST `POST`. Two `GET`s and a pure
        // function; Global Constraint 6's operator half is that an unapproved
        // plan creates NOTHING, and the zero-`POST` assertions in
        // `tests/restore_controller.rs` are made over route tables that HAVE
        // the `POST` routes present, so it is the count that refuses.
        let approval = get_approval(restore, client, &namespace).await?;
        let cluster = get_target_cluster(restore, client, &namespace).await?;
        let admission = admit(restore, approval.as_ref(), cluster.as_ref());
        match &admission {
            RestoreAdmission::Ok => {}
            a if a.is_terminal() => {
                return Err(RestoreError::Refused(a.reason(), a.to_string()));
            }
            a => {
                // A HOLD — interface I19. `phase: Pending`, one condition, and
                // a 30-second requeue so the object is released the moment
                // the `Approval` is verified.
                //
                // ROUTED EXPLICITLY WHEN THE APPROVAL'S OWN PROBLEM IS THAT IT
                // CANNOT FIND US. `super::approval::ReferentProblem::ReferentNotFound`
                // reaches this object as `verified: false` with that reason on
                // the `Approval`'s `Verified` condition, and it is the ONE
                // unverified state that is about a race rather than about a
                // signature: the `Approval` was reconciled before this
                // `Restore` existed. The verdict is unchanged — a 30-second
                // hold — but the log line says which of the two it is, so an
                // operator does not go looking at a key.
                let approval_reason = approval
                    .as_ref()
                    .and_then(|a| a.status.as_ref())
                    .and_then(|s| s.conditions.as_ref())
                    .and_then(|cs| {
                        cs.iter()
                            .find(|c| c.r#type == super::approval::CONDITION_VERIFIED)
                    })
                    .and_then(|c| c.reason.clone());
                if approval_reason.as_deref() == Some(REFERENT_NOT_FOUND_REASON) {
                    info!(
                        restore = %name,
                        namespace = %namespace,
                        approval_reason = REFERENT_NOT_FOUND_REASON,
                        "the Approval that authorises this Restore was reconciled before this \
                         object existed and reports it cannot find its own subject; holding for \
                         {ADMISSION_REQUEUE_SECS}s rather than refusing, because the Approval \
                         controller will look again"
                    );
                } else {
                    info!(
                        restore = %name,
                        namespace = %namespace,
                        reason = a.reason(),
                        approval_reason = approval_reason.as_deref().unwrap_or("<none>"),
                        "no Job exists until the referenced approval is Verified=True; holding \
                         for {ADMISSION_REQUEUE_SECS}s (interface I19)"
                    );
                }
                patch_status_if_changed(
                    &restores,
                    restore,
                    &name,
                    admission_hold_patch(restore, a, now),
                )
                .await?;
                return Ok(RestoreOutcome {
                    job_name,
                    created: false,
                    admission: Some(admission),
                    exit_code: None,
                    terminal_state: None,
                    keys: RestoreEvidenceKeys::default(),
                    ttl_patched: false,
                    requeue: Requeue::After(ADMISSION_REQUEUE_SECS),
                });
            }
        }

        // ADMITTED. The cluster is `Some` — `admit` refused a `None` — and
        // `expect` is not used: the branch names the impossibility.
        let Some(cluster) = cluster else {
            return Err(RestoreError::Refused(
                TERMINAL_STATE_CLUSTER_NOT_REACHABLE,
                format!(
                    "spec.target.clusterRef names `{}`, which was admitted and then found \
                     absent; nothing was created",
                    restore.spec.target.cluster_ref.name
                ),
            ));
        };
        let key_ids = approver_key_ids(get_roster(client).await?.as_ref());
        let mut spec = runner_job_spec(restore, &cluster, &key_ids)?;
        // THE TWO LINES THE OVERRIDES ARE (Task 33's image, Task 37's pull
        // policy). `None` in either leaves the compiled-in constant in place,
        // which is what every test that does not pass one sees.
        spec.image = runner.image.clone();
        spec.image_pull_policy = runner.image_pull_policy.clone();

        // THE PLAN CONFIGMAP, IN THIS SAME PASS AND BEFORE THE JOB `POST`
        // (errata E5a). The Job mounts `<name>-plan` at `/plan`, so a Job
        // created first is a pod that stalls in `ContainerCreating` until its
        // deadline fires — measured live on the `Backup` path, and the reason
        // every scheduled backup was failing as an unexplained `NoExitCode`.
        write_plan_config_map(restore, client, &namespace).await?;

        jobs.create(&PostParams::default(), &job::build(&spec))
            .await
            .map_err(RestoreError::Api)?;
        info!(
            restore = %name,
            namespace = %namespace,
            job = %job_name,
            mode = %format!("{:?}", restore.spec.target.mode),
            approver_key_ids = key_ids.len(),
            "created the runner Job; the plan hash was recomputed from spec.planBytes and \
             matched the hash inside the approval's own signed bytes"
        );
        patch_status_if_changed(
            &restores,
            restore,
            &name,
            running_status_patch(restore, &job_name, true, now),
        )
        .await?;
        return Ok(RestoreOutcome {
            job_name,
            created: true,
            admission: Some(RestoreAdmission::Ok),
            exit_code: None,
            terminal_state: None,
            keys: RestoreEvidenceKeys::default(),
            ttl_patched: false,
            requeue: Requeue::After(REQUEUE_SECS),
        });
    };

    // STEP 2. Running.
    if !backup::job_finished(&job) {
        patch_status_if_changed(
            &restores,
            restore,
            &name,
            running_status_patch(restore, &job_name, false, now),
        )
        .await?;
        return Ok(RestoreOutcome {
            job_name,
            created: false,
            admission: None,
            exit_code: None,
            terminal_state: None,
            keys: RestoreEvidenceKeys::default(),
            ttl_patched: false,
            requeue: Requeue::After(REQUEUE_SECS),
        });
    }

    // STEP 2b. ALREADY TERMINAL: READ NOTHING AND WRITE NOTHING.
    //
    // The `Backup` twin (`controllers::backup.rs`, same position, same guard)
    // carries the measurement this is written from. The `Restore` half was
    // proven live on its own: a terminal, verified `Restore` at `Succeeded` /
    // `0` / `outcome: pass` / `Valid` became `phase: Failed` with its
    // `[Complete, EvidenceRecorded, Verified]` conditions replaced by one
    // `Failed/NoExitCode` after a plain `kubectl rollout restart
    // deploy/weirkeeper` — an upgrade, a node reboot, an eviction or an OOM,
    // any of which re-lists a finished Job whose pod is gone. A terminal
    // `Restore` requeues `AwaitChange`, so it does not even need the 15 s
    // timer to get there.
    //
    // The trade is the `Backup` file's, written out there: a TTL or
    // verification patch that failed on the pass that made the object terminal
    // is not retried. Neither leaves a false statement on the object; the
    // re-derivation did.
    if status_is_terminal(restore) {
        debug!(
            restore = %name,
            namespace = %namespace,
            job = %job_name,
            "the status is already terminal; the runner's pod is not read again and no patch is \
             sent"
        );
        return Ok(RestoreOutcome {
            job_name,
            created: false,
            admission: None,
            exit_code: restore.status.as_ref().and_then(|s| s.exit_code),
            terminal_state: None,
            keys: RestoreEvidenceKeys::default(),
            ttl_patched: false,
            requeue: Requeue::AwaitChange,
        });
    }

    let pod = find_pod(client, &namespace, &job_name).await?;
    let exit_code = pod.as_ref().and_then(backup::terminated_exit_code);

    // STEP 4. The crashed-Job case, before the happy path, because the happy
    // path needs a code and this branch is "there is none".
    let Some(exit_code) = exit_code else {
        let terminal_state = backup::crash_terminal_state(pod.as_ref());
        warn!(
            restore = %name,
            namespace = %namespace,
            job = %job_name,
            terminal_state,
            "the Job finished with no terminated state for the runner container; writing a \
             terminal status rather than watching forever, and inventing no exit code"
        );
        patch_status_if_changed(
            &restores,
            restore,
            &name,
            crashed_status_patch(restore, terminal_state, &job_name, now),
        )
        .await?;
        return Ok(RestoreOutcome {
            job_name,
            created: false,
            admission: None,
            exit_code: None,
            terminal_state: Some(terminal_state.to_string()),
            keys: RestoreEvidenceKeys::default(),
            ttl_patched: false,
            requeue: Requeue::AwaitChange,
        });
    };

    // STEP 3. The code is known. Read the log through the `pods/log`
    // subresource — the only route to a runner's stdout, and the RBAC rule
    // that grants it is Task 21's (interface I28, a declared late binding).
    let pod_name = pod
        .as_ref()
        .map(kube::ResourceExt::name_any)
        .unwrap_or_default();
    let pods: Api<Pod> = Api::namespaced(client.clone(), &namespace);
    let log = pods
        .logs(&pod_name, &LogParams::default())
        .await
        .map_err(RestoreError::Api)?;
    let keys = restore_evidence_keys(&log);
    let refusal = if exit_code == 3 {
        Some(
            backup::refusal_state(&log)
                .unwrap_or_else(|| TERMINAL_STATE_GUARD_REFUSED_UNKNOWN_REASON.to_string()),
        )
    } else {
        None
    };
    // THE SCORECARD, THROUGH THE READ-ONLY ARCHIVE HANDLE. AWAITED: the real
    // oracle's `Store` read happens inside one `spawn_blocking` (interface
    // I13, see `ScorecardOracle`), so this is the one point in the reconcile
    // that yields to the runtime for the archive.
    let observed = match keys.scorecard.as_ref() {
        Some(key) => scorecard(key.clone()).await,
        None => None,
    };
    let topics = topic_mapping(restore);
    // GUARD **G-TS**, erratum **E10(c)**'s controller half: scanned by NAME
    // out of the same bounded tail as the evidence keys.
    let preflight = topic_preflight(&log);

    if !keys.mandatory_complete() {
        if exit_code == 0 {
            warn!(
                restore = %name,
                namespace = %namespace,
                pod = %pod_name,
                "the runner exited 0 and the pod log did not carry both mandatory evidence key \
                 lines; neither key is set and none is guessed"
            );
        } else {
            debug!(
                restore = %name,
                namespace = %namespace,
                pod = %pod_name,
                exit_code,
                "no evidence key lines, which is what Global Constraint 11 says a non-zero exit \
                 writes; no evidence condition is raised"
            );
        }
    }

    // BUILT ONCE AND HELD, because the SECOND patch needs the condition array
    // this one carries: a JSON merge patch REPLACES arrays, and after this
    // PATCH returns the in-memory `restore` is stale and no longer says what
    // the object says. See `verification::second_patch`.
    let terminal = finished_status_patch(
        restore,
        exit_code,
        &keys,
        refusal.as_deref(),
        observed.as_ref(),
        topics.as_ref(),
        preflight.as_ref(),
        now,
    );
    patch_status_if_changed(&restores, restore, &name, terminal.clone()).await?;

    // ONLY NOW. The `?` above is what makes this ordering a guarantee rather
    // than a comment: a status patch that did not return 200 leaves this
    // function before any TTL exists, so pod GC cannot start on a run whose
    // code was never recorded.
    jobs.patch(
        &job_name,
        &PatchParams::default(),
        &Patch::Merge(json!({
            "spec": { "ttlSecondsAfterFinished": TTL_SECONDS_AFTER_FINISHED }
        })),
    )
    .await
    .map_err(RestoreError::Api)?;

    // ===================================================================
    // THE SECOND PATCH — Task 24, interface I21's `Restore` half.
    // ===================================================================
    //
    // AFTER the terminal status write and after the TTL, in a patch of its
    // own, so a verification that fails — or a 500 on this very PATCH — can
    // never prevent the exit code from being recorded. The `?` on the terminal
    // patch above is what makes that an ordering guarantee rather than a
    // comment.
    //
    // ATTEMPTED ONLY WHEN THERE IS SOMETHING TO VERIFY: both mandatory keys
    // and the digest the controller computed over the bytes it fetched. At
    // exits 1, 3 and 4 the contract says no artifact was written (GC11), so
    // there is no document to have an opinion about.
    if let (Some(payload_key), Some(sidecar_key), Some(digest)) = (
        keys.scorecard.as_deref(),
        keys.sidecar.as_deref(),
        observed
            .as_ref()
            .and_then(|o| o.scorecard_sha256.as_deref()),
    ) {
        let result = verify(EvidenceRef {
            payload_key: payload_key.to_string(),
            payload_sha256: digest.to_string(),
            sidecar_key: sidecar_key.to_string(),
            payload_type: logweir_verify::PAYLOAD_TYPE_SCORECARD,
        })
        .await;
        let current = restore
            .status
            .as_ref()
            .and_then(|s| serde_json::to_value(s).ok());
        let block = result.to_status_value(stored_verification(current.as_ref()));
        // THE BADGE IS COMPUTED OVER THE STATUS THAT WILL EXIST, not the one
        // that did: the terminal patch has landed, so `outcome` is the value
        // it copied out of the signed scorecard. Interface I21's `Restore`
        // rule reads `outcome`; the `Backup` rule reads `exitCode`, and the
        // two are not one rule.
        let mut projected = terminal
            .pointer("/status")
            .cloned()
            .unwrap_or_else(|| json!({}));
        projected["evidence"] = json!({ "verification": block.clone() });
        let badge = restore_badge(&projected);
        let verified = verified_condition(
            &badge,
            current_condition(
                restore.status.as_ref().and_then(|s| s.conditions.as_ref()),
                crate::conditions::CONDITION_VERIFIED,
            ),
            restore.meta().generation,
            now,
        );
        info!(
            restore = %name,
            namespace = %namespace,
            verification = %result.result,
            matched_key_id = result.matched_key_id.as_deref().unwrap_or("<none>"),
            green = badge.green,
            badge = %badge.label,
            "weirkeeper verified this Restore's signed scorecard with its read-only evidence \
             credential"
        );
        patch_status_if_changed(
            &restores,
            restore,
            &name,
            second_patch(&conditions_in(&terminal), verified, block),
        )
        .await?;
    }

    let coverage = window_not_covered(
        exit_code,
        observed.as_ref().and_then(|o| o.outcome.as_deref()),
    );
    info!(
        restore = %name,
        namespace = %namespace,
        job = %job_name,
        exit_code,
        exit_reason = wire_reason_for_exit(exit_code),
        outcome = observed.as_ref().and_then(|o| o.outcome.as_deref()).unwrap_or("<not observed>"),
        scorecard_key = keys.scorecard.as_deref().unwrap_or("<unread>"),
        sidecar_key = keys.sidecar.as_deref().unwrap_or("<unread>"),
        offset_report_key = keys.offset_report.as_deref().unwrap_or("<none>"),
        "the runner finished; the exit code, the evidence keys and the scorecard's own values \
         are on the status and the Job now has a TTL"
    );

    Ok(RestoreOutcome {
        job_name,
        created: false,
        admission: None,
        exit_code: Some(exit_code),
        terminal_state: coverage.map(str::to_string).or(refusal),
        keys,
        ttl_patched: true,
        requeue: Requeue::AwaitChange,
    })
}

/// `ReferentNotFound` — [`super::approval::ReferentProblem`]'s own `reason`.
///
/// RESTATED AS A CONSTANT RATHER THAN CONSTRUCTED, because
/// `ReferentProblem::reason` is a method on a value and this code has only the
/// STRING the `Approval`'s condition carries.
/// `the_referent_not_found_reason_is_the_approval_modules_own` asserts the two
/// agree, so the pair cannot drift.
pub const REFERENT_NOT_FOUND_REASON: &str = "ReferentNotFound";

/// The `kube::runtime` reconcile entry point.
///
/// THE ONE CLOCK READ IN THIS FILE IS HERE.
///
/// # The archive oracle, built per reconcile over a handle built once
///
/// [`super::Context::archive`] is the controller's ONE read-only `Arc<Store>`,
/// constructed in `main` before the tokio runtime exists — interface **I13**.
/// The closure below is cheap (an `Arc` clone) and the HANDLE is not rebuilt:
/// a `Store` constructor drives its own runtime, so building one here would
/// both panic and discard the connection pool on every reconcile. `None` — a
/// controller with no `LOGWEIR_ARCHIVE_URL` — takes
/// [`unobserved_scorecard`]'s answer through the same shape: the `?` inside
/// the future is what turns "no handle" into NOT OBSERVED, so every scorecard
/// field is omitted from the status rather than written as nothing.
async fn reconcile(restore: Arc<Restore>, ctx: Arc<Context>) -> Result<Action, RestoreError> {
    let archive = ctx.archive.clone();
    let oracle = move |key: String| -> BoxFuture<'static, Option<ScorecardObservation>> {
        let handle = archive.clone();
        Box::pin(async move {
            let handle = handle?;
            // ONE `spawn_blocking`, ONE `get` — interface I13. `Store` drives
            // its own current-thread runtime and this task is already on one.
            tokio::task::spawn_blocking(move || observe_scorecard(&handle, &key))
                .await
                .ok()
                .flatten()
        })
    };
    let verify = crate::verification::verify_oracle(ctx.archive.clone(), ctx.client.clone());
    let outcome = reconcile_restore_with_runner_image(
        &restore,
        &ctx.client,
        &oracle,
        &verify,
        Utc::now(),
        &ctx.runner_image,
    )
    .await?;
    Ok(action_for(&outcome))
}

/// Requeue on an error, naming it. Never a panic and never a drop.
fn error_policy(restore: Arc<Restore>, err: &RestoreError, _ctx: Arc<Context>) -> Action {
    warn!(
        restore = %restore.name_any(),
        error = %err,
        "restore reconcile failed; requeueing"
    );
    Action::requeue(std::time::Duration::from_secs(REQUEUE_SECS))
}

/// Run the `Restore` controller until the process ends.
///
/// `Api::all`, and it `owns` the Jobs it creates so a pod terminating wakes
/// this reconciler through its Job rather than only on the requeue timer.
///
/// `archive` is the controller's ONE read-only archive handle, built once in
/// `main` before the tokio runtime exists and shared as `Arc<Store>` —
/// interface **I13**. `None` is a controller with no archive configured: it
/// records the exit code and the evidence keys and writes no scorecard-derived
/// field, which is the truthful answer and not a degraded one.
///
/// `runner_image` is Task 33's runtime override and Task 37's beside it, read
/// once each in `main`: an unset field is the compiled-in constant, a set one
/// the image this cluster's nodes hold and the pull policy that makes it
/// resolvable.
pub fn controller(
    client: kube::Client,
    archive: Option<Arc<Store>>,
    runner_image: job::RunnerImage,
) -> impl std::future::Future<Output = ()> + Send {
    let api: Api<Restore> = Api::all(client.clone());
    let jobs: Api<Job> = Api::all(client.clone());
    let ctx = Arc::new(Context {
        client,
        archive,
        runner_image,
    });
    async move {
        Controller::new(api, watcher::Config::default())
            .owns(jobs, watcher::Config::default())
            .run(reconcile, error_policy, ctx)
            .for_each(|_| std::future::ready(()))
            .await;
    }
}
