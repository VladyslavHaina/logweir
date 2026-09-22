//! The `Restore` reconciler — one Job, the plan hash recomputed at
//! Job-creation time, and `mode: scratch` is a field.
//!
//! # Why the admission happens HERE and not in the pod
//!
//! Approval identity and plan hash are checked here before any pod exists.
//! The runner then independently captures and validates the controller-pinned
//! plan, approval, sidecar, public key, and allowlist before constructing a
//! Kafka or object-store client. A bundle replacement therefore refuses
//! before phase 0 rather than being masked by target reachability.
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

use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use futures::future::BoxFuture;
use futures::StreamExt as _;
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::{ConfigMap, Pod};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference};
use kube::api::{Api, LogParams, Patch, PatchParams, PostParams};
use kube::runtime::controller::Action;
use kube::runtime::reflector::{self, ObjectRef};
use kube::runtime::{watcher, Controller};
use kube::{Resource, ResourceExt as _};
use serde_json::{json, Value};
use tracing::{debug, info, warn};

use super::backup::{
    self, ARCHIVE_ACCESS_KEY, ARCHIVE_ACCESS_KEY_ENV, ARCHIVE_SECRET_KEY, ARCHIVE_SECRET_KEY_ENV,
    SIGNING_KEY_FILE, SIGNING_KEY_SECRET, SIGNING_KEY_SECRET_KEY, SIGNING_MOUNT_PATH,
    SIGNING_VOLUME,
};
use super::Context;
use crate::check;
use crate::conditions::{
    current_condition, merge_condition, reason_for_exit, wire_reason_for_exit, StatusVersion,
    CONDITION_ADMITTED, CONDITION_COMPLETE, CONDITION_EVIDENCE_RECORDED, CONDITION_FAILED,
    CONDITION_JOB_CREATED, PHASE_FAILED, PHASE_PENDING, PHASE_RUNNING, PHASE_SUCCEEDED,
    REASON_ADMITTED, REASON_APPROVAL_BUNDLE_MATERIALIZATION_FAILED, REASON_APPROVAL_NOT_VERIFIED,
    REASON_EVIDENCE_KEYS_RECORDED, REASON_EVIDENCE_KEYS_UNREADABLE, REASON_OPERATIONAL,
    TERMINAL_STATE_APPROVAL_BUNDLE_CONFLICT, TERMINAL_STATE_APPROVAL_NOT_RECEIVED,
    TERMINAL_STATE_APPROVAL_POLICY_MISMATCH, TERMINAL_STATE_APPROVAL_SUBJECT_MISMATCH,
    TERMINAL_STATE_AUTHORIZATION_EXPIRED, TERMINAL_STATE_CLUSTER_NOT_REACHABLE,
    TERMINAL_STATE_GUARD_REFUSED_UNKNOWN_REASON, TERMINAL_STATE_JOB_NAME_CONFLICT,
    TERMINAL_STATE_NAME_TOO_LONG, TERMINAL_STATE_PLAN_CONFIG_MAP_CONFLICT,
    TERMINAL_STATE_PLAN_HASH_MISMATCH, TERMINAL_STATE_POD_OWNERSHIP_CONTESTED,
    TERMINAL_STATE_WINDOW_NOT_COVERED,
};
use crate::crds::approval::Approval;
use crate::crds::kafka_cluster::KafkaCluster;
use crate::crds::restore::Restore;
use crate::crds::trust_roster::TrustRoster;
use crate::destination::{self, DestinationRole, ResolveError, ResolvedDestination};
use crate::diagnostics;
use crate::job::{
    self, ConfigMapMount, EnvFromSecret, RunnerJobSpec, RunnerOwner, SecretMount,
    APPROVAL_MOUNT_PATH, APPROVAL_VOLUME, CONTAINER_NAME, PLAN_MOUNT_PATH,
};
use crate::verification::{
    conditions_in, restore_badge, second_patch, stored_verification, verified_condition,
    EvidenceRef, VerifyOracle,
};
use logweir_core::approval_policy::{
    self as approval_policy, ApprovalMode, ApprovalPolicySet, AuthorizationRefusal,
    EffectivePolicy, ExpectedSubject, RestoreAuthorization, PAYLOAD_TYPE_RESTORE_AUTHORIZATION,
};
use logweir_core::check_contract::CheckCode;
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

/// The namespace-wide Secret mounted by Jobs created before PLAT-01.
///
/// Kept as a named compatibility contract: an existing Job that references
/// this Secret is legacy and is observed without rewriting its pod template.
pub const APPROVAL_BUNDLE_SECRET: &str = "logweir-approval-bundle";

/// Suffix for the immutable, public bundle created for one Restore.
pub const APPROVAL_BUNDLE_SUFFIX: &str = "-approval-bundle";

/// Annotation carrying the Restore UID bound to the bundle.
pub const BUNDLE_RESTORE_UID_ANNOTATION: &str = "logweir.dev/restore-uid";
/// Annotation carrying `sha256(spec.planBytes)`.
pub const BUNDLE_PLAN_HASH_ANNOTATION: &str = "logweir.dev/plan-hash";
/// Annotation carrying the referenced Approval name.
pub const BUNDLE_APPROVAL_NAME_ANNOTATION: &str = "logweir.dev/approval-name";
/// Annotation carrying the referenced Approval UID.
pub const BUNDLE_APPROVAL_UID_ANNOTATION: &str = "logweir.dev/approval-uid";

/// Condition type showing whether the per-Restore public inputs are ready.
pub const CONDITION_APPROVAL_BUNDLE_READY: &str = "ApprovalBundleReady";

/// The signed approval document, at `/approval/approval.json`.
pub const APPROVAL_DOC_FILE: &str = "approval.json";
/// Its detached DSSE sidecar, at `/approval/approval.sig`.
pub const APPROVAL_SIG_FILE: &str = "approval.sig";
/// The approver's **public** key, at `/approval/approver.pub.pem`. No private
/// key reaches a runner pod on this path or any other.
pub const APPROVER_KEY_FILE: &str = "approver.pub.pem";
/// The cluster allowlist, at `/approval/allowed-clusters.json`.
pub const ALLOWED_CLUSTERS_FILE: &str = "allowed-clusters.json";
/// PLAT-19.2: the frozen approval-policy snapshot, at
/// `/approval/approval-policy.json` — exactly
/// `logweir_core::approval_policy::ApprovalPolicy::snapshot_bytes`, whose
/// digest the signed authorization document v2 names. Present only in the
/// bundle of a Restore authorised by a v2 document.
pub const APPROVAL_POLICY_FILE: &str = "approval-policy.json";
/// PLAT-19.2: the console's `ConsoleConfirmation` public key, the second of
/// D0's "both required public keys". Present only with
/// [`APPROVAL_POLICY_FILE`].
pub const CONFIRMATION_KEY_FILE: &str = "confirmation.pub.pem";
/// PLAT-19.2: the policy digest the bundle was rendered under, as an
/// annotation beside the other binding annotations.
pub const BUNDLE_APPROVAL_POLICY_DIGEST_ANNOTATION: &str = "logweir.dev/approval-policy-digest";
/// The runner flag naming [`APPROVAL_POLICY_FILE`].
pub const APPROVAL_POLICY_ARG: &str = "--approval-policy";
/// The runner flag naming [`CONFIRMATION_KEY_FILE`].
pub const CONFIRMATION_KEY_ARG: &str = "--confirmation-key";

/// The per-Restore immutable ConfigMap mounted at [`APPROVAL_MOUNT_PATH`].
#[must_use]
pub fn approval_bundle_config_map_name(restore_name: &str) -> String {
    format!("{restore_name}{APPROVAL_BUNDLE_SUFFIX}")
}

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
///
/// **PLAT-07.1 MADE IT THE DEFAULT RATHER THAN THE ONLY KEY.** A connection may
/// name `auth.secretRef.passwordKey`; absent, the resolver projects this key, so
/// every Secret written before that field existed still resolves. The constant
/// stays here because the chart's own refusal quotes this line
/// (`crates/logweir/tests/chart_lint.rs`), and
/// `crates/weirkeeper/tests/connection.rs` asserts it equals
/// `crds::kafka_cluster::DEFAULT_PASSWORD_KEY`.
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
/// SIX VARIANTS AND THE SET IS CLOSED. Tasks 26 and 27 render the `reason`
/// this produces, so a variant is an interface and not an implementation
/// detail; [`RestoreAdmission::reason`] is a `match` with no wildcard so a
/// seventh variant fails to compile until someone names its reason.
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
    /// The named Approval is verified, but for a different Kubernetes
    /// subject identity. Both specs are immutable, so this is terminal.
    ApprovalSubjectMismatch {
        /// The referenced Approval name.
        approval: String,
        /// Which non-secret identity component failed.
        detail: String,
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
    /// `spec.authorization`'s standing document does not admit this run —
    /// PLAT-14.3b, D3 §4.3.
    ///
    /// TERMINAL. Expired, a revoked or wrong-usage key, a subject that is not
    /// this schedule, or a plan outside the signed scope: every one of them is
    /// a property of two sealed specs, so the slot cannot be rescued. The
    /// schedule's NEXT slot renders a new plan and re-checks everything, which
    /// is why a refusal here is recorded rather than held.
    StandingAuthorizationRefused {
        /// The standing `Approval` `spec.authorization.approvalRef` names.
        approval: String,
        /// Which check refused, in the same words the `RehearsalSchedule`
        /// reconciler uses for the same check before the `Restore` exists.
        detail: String,
    },
    /// PLAT-19.2: the Approval's authorization does not match the namespace's
    /// CURRENT approval-policy binding. TERMINAL.
    AuthorizationPolicyMismatch {
        /// The Approval `spec.approvalRef` names.
        approval: String,
        /// Both policies, or the format the binding refuses.
        detail: String,
    },
    /// PLAT-19.2: the authorization document v2 expired before admission.
    /// TERMINAL.
    AuthorizationExpired {
        /// The Approval `spec.approvalRef` names.
        approval: String,
        /// The instant and the clock.
        detail: String,
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
            Self::ApprovalSubjectMismatch { .. } => TERMINAL_STATE_APPROVAL_SUBJECT_MISMATCH,
            Self::PlanHashMismatch { .. } => TERMINAL_STATE_PLAN_HASH_MISMATCH,
            Self::ClusterNotReachable { .. } => TERMINAL_STATE_CLUSTER_NOT_REACHABLE,
            Self::StandingAuthorizationRefused { .. } => {
                crate::conditions::TERMINAL_STATE_STANDING_AUTHORIZATION_REFUSED
            }
            Self::AuthorizationPolicyMismatch { .. } => TERMINAL_STATE_APPROVAL_POLICY_MISMATCH,
            Self::AuthorizationExpired { .. } => TERMINAL_STATE_AUTHORIZATION_EXPIRED,
        }
    }

    /// Whether this admission ends the object's life.
    ///
    /// # The split, and the one member that is NOT terminal
    ///
    /// [`Self::ApprovalNotVerified`] is a HOLD: the fact it reports can change
    /// without anybody touching this object, so it requeues at
    /// [`ADMISSION_REQUEUE_SECS`] (interface **I19**). The other four cannot:
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
            | Self::ApprovalSubjectMismatch { .. }
            | Self::PlanHashMismatch { .. }
            | Self::ClusterNotReachable { .. }
            | Self::StandingAuthorizationRefused { .. }
            | Self::AuthorizationPolicyMismatch { .. }
            | Self::AuthorizationExpired { .. } => true,
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
            Self::ApprovalSubjectMismatch { approval, detail } => write!(
                f,
                "spec.approvalRef names Approval `{approval}`, but its verified subject binding \
                 does not identify this Restore ({detail}); create a new Approval for this exact \
                 Restore name, namespace, and UID"
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
            Self::StandingAuthorizationRefused { approval, detail } => write!(
                f,
                "spec.authorization names the standing Approval `{approval}`, which does not \
                 authorise this rehearsal ({detail}); no Job is created. Both specs are sealed, \
                 so this slot cannot be rescued — the schedule's next slot renders a new plan \
                 and is checked again"
            ),
            Self::AuthorizationPolicyMismatch { approval, detail } => write!(
                f,
                "spec.approvalRef names Approval `{approval}`, whose authorization does not match \
                 this namespace's approval policy ({detail}); no Job is created. Both specs are \
                 immutable: create a new Restore, which is confirmed or approved under the policy \
                 bound now"
            ),
            Self::AuthorizationExpired { approval, detail } => write!(
                f,
                "spec.approvalRef names Approval `{approval}`, whose authorization expired before \
                 this Restore was admitted ({detail}); no Job is created. Create a new Restore"
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
///
/// PLAT-19.2: an authorization document v2 spells it `planHash`. WHICH
/// spelling is read is decided by the SIDECAR's payload type — the value the
/// signature covers — and never by trying both, so a v1 document that also
/// carried a `planHash` key cannot present a second hash.
#[must_use]
pub fn approval_plan_hash(approval: &Approval) -> Option<String> {
    let field = if is_authorization_v2(approval) {
        "planHash"
    } else {
        "plan_hash"
    };
    serde_json::from_str::<Value>(&approval.spec.approval_bytes)
        .ok()?
        .get(field)?
        .as_str()
        .map(str::to_string)
}

/// Whether this Approval carries an authorization document v2 — its sidecar's
/// `payloadType` is [`PAYLOAD_TYPE_RESTORE_AUTHORIZATION`].
#[must_use]
pub fn is_authorization_v2(approval: &Approval) -> bool {
    serde_json::from_str::<Value>(&approval.spec.sidecar_bytes)
        .ok()
        .and_then(|v| {
            v.get("payloadType")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .as_deref()
        == Some(PAYLOAD_TYPE_RESTORE_AUTHORIZATION)
}

/// What [`admit_with_policy`] needs to judge an approval POLICY — PLAT-19.2.
pub struct PolicyAdmission<'a> {
    /// What the Restore's namespace is bound to NOW.
    pub policy: &'a EffectivePolicy,
    /// The clock, passed in — Global Constraint 1.
    pub now: DateTime<Utc>,
}

/// The Approval's own `Verified` reason, when it is one of the two PERMANENT
/// v2 refusals that must end the Restore rather than hold it for ever.
fn permanent_approval_refusal(approval: &Approval, referent: &str) -> Option<RestoreAdmission> {
    let condition = approval
        .status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .and_then(|cs| {
            cs.iter()
                .find(|c| c.r#type == super::approval::CONDITION_VERIFIED && c.status == "False")
        })?;
    let detail = condition.message.clone().unwrap_or_default();
    match condition.reason.as_deref() {
        Some(TERMINAL_STATE_AUTHORIZATION_EXPIRED) => {
            Some(RestoreAdmission::AuthorizationExpired {
                approval: referent.to_string(),
                detail,
            })
        }
        Some(TERMINAL_STATE_APPROVAL_POLICY_MISMATCH) => {
            Some(RestoreAdmission::AuthorizationPolicyMismatch {
                approval: referent.to_string(),
                detail,
            })
        }
        _ => None,
    }
}

/// **PLAT-19.2's admission half: the approval POLICY, re-checked against the
/// namespace's CURRENT binding immediately before any Job exists** (D0: "The
/// Restore controller repeats the authorization verdict immediately before
/// materializing the bundle and before Job creation").
///
/// The signatures are the Approval controller's (and, again, the runner's);
/// what this re-derives from the SIGNED BYTES is everything that depends on
/// the namespace's binding and the clock — which a policy rollout or the
/// passage of time can change after the Approval was verified.
fn policy_refusal(
    restore: &Restore,
    approval: &Approval,
    referent: &str,
    admission: &PolicyAdmission<'_>,
) -> Option<RestoreAdmission> {
    let mismatch = |refusal: AuthorizationRefusal| RestoreAdmission::AuthorizationPolicyMismatch {
        approval: referent.to_string(),
        detail: refusal.to_string(),
    };
    let namespace = restore.namespace().unwrap_or_default();
    let bound = match (admission.policy, is_authorization_v2(approval)) {
        (EffectivePolicy::Legacy, false) => return None,
        (EffectivePolicy::Legacy, true) => {
            return Some(mismatch(approval_policy::unbound_namespace_refusal(
                &namespace,
            )))
        }
        (EffectivePolicy::Bound(bound), false) => {
            return Some(mismatch(approval_policy::v1_under_bound_policy_refusal(
                bound,
            )))
        }
        (EffectivePolicy::Bound(bound), true) => bound,
    };
    let doc = match RestoreAuthorization::from_bytes(approval.spec.approval_bytes.as_bytes()) {
        Ok(doc) => doc,
        Err(refusal) => return Some(mismatch(refusal)),
    };
    let expected = ExpectedSubject {
        namespace,
        name: restore.name_any(),
        uid: restore.uid().unwrap_or_default(),
        plan_hash: recomputed_plan_hash(restore),
    };
    if let Err(refusal) =
        approval_policy::check_restore_authorization(&doc, &expected, bound, admission.now)
    {
        return Some(match refusal {
            AuthorizationRefusal::Expired(_) => RestoreAdmission::AuthorizationExpired {
                approval: referent.to_string(),
                detail: refusal.to_string(),
            },
            AuthorizationRefusal::PlanHashMismatch { got, want } => {
                RestoreAdmission::PlanHashMismatch {
                    recomputed: want,
                    approval_says: got,
                }
            }
            AuthorizationRefusal::SubjectMismatch(detail) => {
                RestoreAdmission::ApprovalSubjectMismatch {
                    approval: referent.to_string(),
                    detail,
                }
            }
            other => mismatch(other),
        });
    }
    // THE VERDICT WAS MADE UNDER THIS POLICY, TOO. The signed bytes agree with
    // the binding; the controller-written provenance must say the Approval
    // controller agreed as well, so a verdict left over from before a policy
    // rollout is never the one admitted.
    let provenance = approval
        .status
        .as_ref()
        .and_then(|s| s.authorization.as_ref());
    let agrees = provenance.is_some_and(|p| {
        p.policy_name == bound.name
            && p.policy_digest == bound.digest()
            && p.mode == bound.mode.as_str()
    });
    if !agrees {
        return Some(RestoreAdmission::AuthorizationPolicyMismatch {
            approval: referent.to_string(),
            detail: format!(
                "status.authorization records {} and this namespace is bound to policy {} ({}, \
                 {}); the Approval controller has not verified it under the current binding",
                provenance.map_or_else(
                    || "no v2 verdict".to_string(),
                    |p| format!("policy {} ({}, {})", p.policy_name, p.mode, p.policy_digest)
                ),
                bound.name,
                bound.mode,
                bound.digest()
            ),
        });
    }
    None
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
/// Job-creation time. **NEVER reads `Approval.status.matchedKeyId` as proof of
/// the document hash** (interface **I18**); status is used only for the
/// controller-produced verified flag and exact subject provenance.
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
/// 2. That `Approval` must exist and its immutable subject plus sticky verified
///    provenance must identify this exact Restore name, namespace and UID →
///    else [`RestoreAdmission::ApprovalSubjectMismatch`], terminal.
/// 3. That exact Approval must be `status.verified == Some(true)` → else
///    [`RestoreAdmission::ApprovalNotVerified`], **requeued at
///    [`ADMISSION_REQUEUE_SECS`]** (interface **I19**). Before the hash,
///    because the hash inside unverified bytes is a claim nobody signed for.
/// 4. The recomputed hash must equal the approval document's own → else
///    [`RestoreAdmission::PlanHashMismatch`], terminal, naming both.
/// 5. `spec.target.clusterRef` must resolve to a `KafkaCluster` with
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
    standing: Option<&StandingAdmission<'_>>,
) -> RestoreAdmission {
    admit_with_policy(restore, approval, cluster, standing, None)
}

/// [`admit`], under the namespace's approval-policy binding — PLAT-19.2.
///
/// `policy: None` is `legacy-governed-v1` with no clock, which is exactly
/// [`admit`]: the legacy arm reads neither. With a policy, step 3b below runs
/// between "the exact Approval is Verified=True" and the plan hash.
#[must_use]
pub fn admit_with_policy(
    restore: &Restore,
    approval: Option<&Approval>,
    cluster: Option<&KafkaCluster>,
    standing: Option<&StandingAdmission<'_>>,
    policy: Option<&PolicyAdmission<'_>>,
) -> RestoreAdmission {
    // **THE TWO AUTHORIZATIONS, DISPATCHED ON THE OBJECT AND NEVER ON WHAT
    // HAPPENS TO EXIST** — PLAT-14.3b.
    //
    // `spec.authorization` is what makes a `Restore` a rehearsal, and the CEL
    // rule `has(self.approvalRef) != has(self.authorization)` makes the two
    // mutually exclusive on a sealed spec. Dispatching on the presence of a
    // standing `Approval` in the namespace instead would let a standing
    // document admit an ORDINARY `Restore` — the exact widening this branch is
    // shaped to prevent, and a planted mutant of it is in
    // `claude/plat14-3b-mutants.log`.
    if restore.spec.authorization.is_some() {
        return admit_standing(restore, approval, cluster, standing);
    }

    // ---- 1. the ref must name something ---------------------------------
    let referent = restore.spec.approval_ref_name().trim().to_string();
    if referent.is_empty() {
        return RestoreAdmission::ApprovalNotReceived {
            approval: restore.spec.approval_ref_name().to_string(),
        };
    }

    // ---- 2. the referenced object must be this exact subject -------------
    let Some(approval) = approval else {
        return RestoreAdmission::ApprovalNotVerified { approval: referent };
    };

    // A plan hash is not a subject identity.  The Approval controller records
    // the exact referent UID it actually read, and this side checks the whole
    // binding before accepting its cached verdict. Check immutable subject
    // mismatches even after Verified was revoked so a recreated UID produces
    // an actionable terminal Restore status instead of a permanent generic
    // hold. A legacy status without provenance is still held for refresh.
    let status = approval.status.as_ref();
    let bound = status.and_then(|status| status.verified_subject_ref.as_ref());
    let restore_name = restore.name_any();
    let restore_namespace = restore.namespace().unwrap_or_default();
    let restore_uid = restore.uid().unwrap_or_default();
    let approval_namespace = approval.namespace().unwrap_or_default();
    let subject = &approval.spec.subject_ref;
    let expected_api_version = Restore::api_version(&()).to_string();
    let mismatch = if approval.name_any() != referent {
        Some("the API response name differs from spec.approvalRef".to_string())
    } else if approval_namespace != restore_namespace {
        Some("the Approval is from a different namespace".to_string())
    } else if subject.kind != crate::crds::approval::SubjectKind::Restore {
        Some(format!("spec.subjectRef.kind is {}", subject.kind))
    } else if subject.name != restore_name {
        Some(format!("spec.subjectRef.name is `{}`", subject.name))
    } else if let Some(bound) = bound {
        if bound.api_version != expected_api_version
            || bound.kind != crate::crds::approval::SubjectKind::Restore
            || bound.name != restore_name
            || bound.namespace != restore_namespace
        {
            Some("status.verifiedSubjectRef names a different object".to_string())
        } else if bound.uid != restore_uid {
            Some(format!(
                "status.verifiedSubjectRef.uid is `{}`, current metadata.uid is `{restore_uid}`",
                bound.uid
            ))
        } else {
            None
        }
    } else {
        None
    };
    if let Some(detail) = mismatch {
        return RestoreAdmission::ApprovalSubjectMismatch {
            approval: referent,
            detail,
        };
    }

    // ---- 3. and that exact Approval must currently be Verified=True -------
    //
    // PLAT-19.2: two of the Approval's own refusals are PERMANENT — its
    // document expired, or it was issued under another policy — and both specs
    // are immutable, so holding for ever would be a Restore nobody is told is
    // dead. They end it instead, with the Approval's own words.
    if status.and_then(|status| status.verified) != Some(true) || bound.is_none() {
        if policy.is_some() {
            if let Some(permanent) = permanent_approval_refusal(approval, &referent) {
                return permanent;
            }
        }
        return RestoreAdmission::ApprovalNotVerified { approval: referent };
    }

    // ---- 3b. the approval POLICY, re-checked now (PLAT-19.2) --------------
    let legacy = PolicyAdmission {
        policy: &EffectivePolicy::Legacy,
        now: DateTime::<Utc>::MIN_UTC,
    };
    if let Some(refusal) = policy_refusal(restore, approval, &referent, policy.unwrap_or(&legacy)) {
        return refusal;
    }

    // ---- 4. the plan hash, RECOMPUTED, from inside the signed bytes ------
    let recomputed = recomputed_plan_hash(restore);
    let approval_says = approval_plan_hash(approval).unwrap_or_default();
    if recomputed != approval_says {
        return RestoreAdmission::PlanHashMismatch {
            recomputed,
            approval_says,
        };
    }

    // ---- 5. the target must report reachable -----------------------------
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

/// What [`admit`] needs to judge a STANDING authorization that it cannot read
/// off the two objects — PLAT-14.3b.
///
/// # Why trust reaches the admission at all
///
/// For an ordinary `Restore` the trust is resolved AFTER admission, because an
/// unapproved plan reads nothing it does not need. A standing authorization
/// cannot be judged that way: "the key that signed it may no longer authorise
/// anything new" and "that key's usage is not an approver's" are facts about
/// the namespace's resolved trust, and they are two of the four refusals D3
/// §4.3(c) requires at EVERY slot. Resolving trust before admission for a
/// rehearsal — and only for a rehearsal — is the narrow ordering change that
/// buys them.
pub struct StandingAdmission<'a> {
    /// This namespace's resolved trust.
    pub trust: &'a crate::trust::ResolvedTrust,
    /// The clock, passed in — Global Constraint 1.
    pub now: DateTime<Utc>,
}

/// [`admit`] for a `Restore` carrying `spec.authorization` — D3 §4.3(c), the
/// controller's half of "checked twice" at the RESTORE reconciler.
///
/// # The same refusals the schedule already made, made again
///
/// `RehearsalSchedule`'s `authorize` runs this chain before the `Restore`
/// exists. Running it again here is not redundancy for its own sake: the
/// `Restore` is a separate object with its own lifetime, a key can be
/// withdrawn between the slot firing and this reconcile, and a `Restore`
/// carrying `spec.authorization` can be created by anything with RBAC on the
/// kind — including a hand-written one that no schedule ever rendered. This
/// function is what makes the standing document, and not the creator, the
/// thing that authorises the Job.
///
/// # The order
///
/// The ref names something. The `Approval` exists and is `Verified=True` — a
/// HOLD, because it can become true without anyone touching this object. It is
/// a `RehearsalSchedule` approval bound to the schedule
/// `spec.authorization.rehearsalScheduleRef` names. The key it verified under
/// is one the namespace's trust still lets authorise, carrying an approver's
/// usage. The SIGNED bytes are admissible — kind, subject, the UID binding,
/// the validity window and D3 §4.3's 90-day cap. And `plan ∈ scope`. The
/// target-reachable check is last, exactly as on the ordinary path, so an
/// authorization problem is never reported as a broker problem.
///
/// # The two scope fields `plan_within_scope` cannot see
///
/// `deadlineSeconds` is the Job's `activeDeadlineSeconds` and not a plan
/// field, so it is compared here, over the signed number — for a hand-written
/// `Restore` this is the only enforcement of it anywhere in the product.
/// `templateDigest` is the other, and it is STRUCTURALLY unreachable from a
/// `Restore`: it is a digest of the `RehearsalSchedule`'s sealed spec, which
/// this reconciler does not hold and must not fetch to decide an admission.
/// It stays the schedule reconciler's, checked every slot in
/// `rehearsal::scope_agrees` before any `Restore` exists, and is named here so
/// its absence reads as a decision rather than as an oversight.
fn admit_standing(
    restore: &Restore,
    approval: Option<&Approval>,
    cluster: Option<&KafkaCluster>,
    standing: Option<&StandingAdmission<'_>>,
) -> RestoreAdmission {
    use logweir_core::execution_contract as wire;

    // Unreachable — the caller resolves trust before admitting a rehearsal —
    // and a REFUSAL rather than an admission, because "we could not judge the
    // authorization" must never read as "the authorization was fine".
    let Some(standing_inputs) = standing else {
        return RestoreAdmission::StandingAuthorizationRefused {
            approval: restore
                .spec
                .authorization
                .as_ref()
                .map(|a| a.approval_ref.name.clone())
                .unwrap_or_default(),
            detail: "this build could not resolve the namespace's trust at admission time, so \
                     the standing authorization's signing key could not be judged"
                .to_string(),
        };
    };
    // Unreachable: `admit` dispatched on exactly this field being present.
    let Some(authorization) = restore.spec.authorization.as_ref() else {
        return RestoreAdmission::ApprovalNotReceived {
            approval: String::new(),
        };
    };
    let wanted = authorization.approval_ref.name.trim().to_string();
    if wanted.is_empty() {
        return RestoreAdmission::ApprovalNotReceived { approval: wanted };
    }
    let refused = |detail: String| RestoreAdmission::StandingAuthorizationRefused {
        approval: wanted.clone(),
        detail,
    };

    // ---- 2. the Approval exists and is verified (a HOLD) -----------------
    let Some(approval) = approval else {
        return RestoreAdmission::ApprovalNotVerified { approval: wanted };
    };
    let status = approval.status.as_ref();
    if status.and_then(|s| s.verified) != Some(true) {
        return RestoreAdmission::ApprovalNotVerified { approval: wanted };
    }

    // ---- 3. a RehearsalSchedule approval, bound to THIS schedule ---------
    //
    // **THE KIND IS WHAT SEPARATES THE TWO AUTHORIZATIONS.** A per-run
    // `Approval` names `subjectRef.kind: Restore`; accepting one here would
    // let an approval a human signed for one restore authorise a rehearsal it
    // says nothing about.
    if approval.spec.subject_ref.kind != crate::crds::approval::SubjectKind::RehearsalSchedule {
        return RestoreAdmission::ApprovalSubjectMismatch {
            approval: wanted,
            detail: format!(
                "spec.subjectRef.kind is {} and a standing rehearsal authorization names \
                 RehearsalSchedule",
                approval.spec.subject_ref.kind
            ),
        };
    }
    if approval.namespace().unwrap_or_default() != restore.namespace().unwrap_or_default() {
        return RestoreAdmission::ApprovalSubjectMismatch {
            approval: wanted,
            detail: "the Approval is from a different namespace".to_string(),
        };
    }
    let schedule = authorization.rehearsal_schedule_ref.name.clone();
    // THE UID THE APPROVAL CONTROLLER ACTUALLY VERIFIED AGAINST. It is also
    // the only RehearsalSchedule UID reachable from here — a `Restore` carries
    // the schedule's NAME — and it is what makes the signed document's own
    // `subjectRef.uid` check below a binding rather than a tautology.
    let Some(bound) = status.and_then(|s| s.verified_subject_ref.as_ref()) else {
        return RestoreAdmission::ApprovalSubjectMismatch {
            approval: wanted,
            detail: "status.verifiedSubjectRef is absent, so nothing says WHICH object this \
                     Approval was verified against"
                .to_string(),
        };
    };
    if bound.kind != crate::crds::approval::SubjectKind::RehearsalSchedule || bound.name != schedule
    {
        return RestoreAdmission::ApprovalSubjectMismatch {
            approval: wanted,
            detail: format!(
                "status.verifiedSubjectRef names {}/{} and spec.authorization.rehearsalScheduleRef \
                 names RehearsalSchedule `{schedule}`",
                bound.kind, bound.name
            ),
        };
    }

    // ---- 3b. the OBJECT, and not merely the name -------------------------
    //
    // **The deviation this closes.** `spec.authorization.approvalRef` is a
    // `LocalRef` and carries only a name (D3 W0's CRD), so `get_approval`
    // resolves the standing `Approval` by name and a DIFFERENT object that
    // later took that name would be resolved instead. The
    // `RehearsalSchedule` reconciler writes the UID of the object the slot
    // actually authorised against onto the child, and it is required to match
    // here.
    //
    // ABSENT IS A REFUSAL, not a skipped check. Every standing `Restore` this
    // build's schedule creates carries it, and a standing `Restore` that
    // predates it never executed — `admit` refused every one of them with
    // `ApprovalNotReceived` before PLAT-14.3b — so there is no object in the
    // world this costs, and "we could not tell which Approval this was" must
    // never read as "it was the right one".
    let pinned_uid = restore
        .annotations()
        .get(BUNDLE_APPROVAL_UID_ANNOTATION)
        .map(|uid| uid.trim())
        .filter(|uid| !uid.is_empty());
    let Some(pinned_uid) = pinned_uid else {
        return refused(format!(
            "it carries no {BUNDLE_APPROVAL_UID_ANNOTATION} annotation, so the Approval `{wanted}` \
             resolved by NAME cannot be pinned to the object this slot was authorised against"
        ));
    };
    let resolved_uid = approval.uid().unwrap_or_default();
    if resolved_uid != pinned_uid {
        return refused(format!(
            "the Approval `{wanted}` resolved by name has uid `{resolved_uid}` and this rehearsal \
             was authorised against uid `{pinned_uid}`; an Approval deleted and recreated under \
             the same name is a different authorisation"
        ));
    }

    // ---- 4. the key: it exists, it may authorise, it is not withdrawn ----
    let Some(key_id) = status.and_then(|s| s.matched_key_id.clone()) else {
        return refused("the Approval records no status.matchedKeyId".to_string());
    };
    let Some(key) = standing_inputs.trust.key(&key_id) else {
        return refused(format!(
            "it verified under key {key_id}, which the trust this namespace resolves to does not \
             carry"
        ));
    };
    let usage = logweir_core::trust::KeyUsage::GovernedApproval;
    if !key.trust.has_usage(usage) {
        // D3 §7.3's key-usage separation: the installation's own evidence
        // identity must never authorise its own rehearsals.
        return refused(format!(
            "it verified under key {key_id}, whose usages are [{}]; a rehearsal in the current \
             format is authorised only by GovernedApproval; ConsoleConfirmation requires \
             PLAT-19.2's immutable policy-mode binding and EvidenceSigning never authorises",
            key.trust
                .usages
                .iter()
                .map(|u| format!("{u:?}"))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    };
    if let Err(refusal) =
        standing_inputs
            .trust
            .may_sign_new_for(&key_id, usage, standing_inputs.now)
    {
        return refused(format!(
            "it verified under key {key_id}, which may no longer authorise anything new ({}); a \
             slot does not run under a withdrawn key",
            refusal.as_str()
        ));
    }

    // ---- 5. the SIGNED bytes, parsed only now ----------------------------
    let doc: wire::StandingAuthorization = match serde_json::from_str(&approval.spec.approval_bytes)
    {
        Ok(doc) => doc,
        Err(error) => {
            return refused(format!(
                "it verified, but its bytes are not a standing rehearsal authorization: \
                     {error}"
            ))
        }
    };
    if let Err(refusal) =
        wire::admit_standing_authorization(&doc, Some(&bound.uid), standing_inputs.now)
    {
        return refused(refusal.detail);
    }
    if doc.subject_ref.namespace != restore.namespace().unwrap_or_default() {
        return refused(format!(
            "the standing authorization names namespace `{}` and this Restore is in `{}`",
            doc.subject_ref.namespace,
            restore.namespace().unwrap_or_default()
        ));
    }
    // The SIGNED name, against the one this object claims. The runner catches
    // a mismatch late, through `--triggered-by`'s binding to
    // `subjectRef.name`; catching it here means no Job is created at all, and
    // the message names the schedule rather than the trigger.
    if doc.subject_ref.name != schedule {
        return refused(format!(
            "the standing authorization was signed for RehearsalSchedule `{}` and \
             spec.authorization.rehearsalScheduleRef names `{schedule}`",
            doc.subject_ref.name
        ));
    }

    // ---- 6. plan ∈ scope, over the bytes this Restore froze --------------
    //
    // The allowlist is EXACTLY the signed target cluster id — W5's R1.3
    // obligation 1, and the same projection `rehearsal_schedule` uses, so the
    // two halves of "checked twice" cannot disagree by computing different
    // facts from the same plan.
    let plan: logweir_core::spec::DrillSpec = match serde_yaml::from_str(&restore.spec.plan_bytes) {
        Ok(plan) => plan,
        Err(error) => {
            return refused(format!(
                "spec.planBytes does not parse as a restore plan, so it cannot be proved \
                     inside the signed scope: {error}"
            ))
        }
    };
    let allowed = logweir_core::spec::AllowedClusters {
        allowed_cluster_ids: vec![doc.scope.target_cluster_id.clone()],
        source_cluster_id: None,
    };
    let facts = wire::plan_scope_facts(&plan, &allowed);
    if let Err(refusal) = wire::plan_within_scope(&facts, &doc.scope) {
        return refused(format!(
            "the rendered plan is outside the signed scope: {refusal}"
        ));
    }

    // **D3 §4.3(d)'s CONTROLLER-ONLY half, and for a hand-written `Restore`
    // this is the only place in the product that enforces it.**
    //
    // `plan_within_scope` cannot see `deadlineSeconds`: it is the Job's
    // `activeDeadlineSeconds`, a field of the `Restore`, not of the plan — and
    // `logweir_core::execution_contract` says so where the predicate is
    // defined. The `RehearsalSchedule` reconciler checks it every slot in
    // `rehearsal::scope_agrees`, but only for the `Restore`s IT rendered. A
    // `Restore` carrying `spec.authorization` can be created by anything with
    // RBAC on the kind, and that object is the whole reason this function
    // exists, so the bound is re-made here over the signed number.
    if restore.spec.deadline_seconds > i64::from(doc.scope.deadline_seconds) {
        return refused(format!(
            "the Restore's deadlineSeconds is {} and the signed scope permits at most {}",
            restore.spec.deadline_seconds, doc.scope.deadline_seconds
        ));
    }
    // A NEGATIVE OR ZERO DEADLINE IS NOT A SMALLER ONE. `activeDeadlineSeconds`
    // must be positive for the Job to be admissible at all, and a run with no
    // wall-clock bound is exactly what the signed field exists to prevent.
    if restore.spec.deadline_seconds <= 0 {
        return refused(format!(
            "the Restore's deadlineSeconds is {}; a rehearsal runs under a positive wall-clock \
             bound and the signed scope permits at most {}",
            restore.spec.deadline_seconds, doc.scope.deadline_seconds
        ));
    }

    // ---- 7. the target must report reachable -----------------------------
    let cluster_name = restore.spec.target.cluster_ref.name.clone();
    if cluster
        .and_then(|c| c.status.as_ref())
        .and_then(|s| s.reachable)
        != Some(true)
    {
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
    plan_config_map_with_destinations(restore, None)
}

/// [`plan_config_map`], with the two destinations' CA bundles beside the plan
/// — D2 §3.5's "CA files" paragraph.
///
/// # THE BYTES ARE FROZEN HERE AND NEVER RE-READ AT JOB TIME
///
/// `LOGWEIR_ARCHIVE_CA_FILE` and `LOGWEIR_EVIDENCE_CA_FILE` point at keys in
/// THIS immutable `ConfigMap`, not at the destination's own `caBundle`
/// `ConfigMap`. Mounting the live one would mean a CA rotated while a restore
/// runs changes what that run trusts, mid-run, with an approved plan that says
/// nothing about it. Copying the bytes into the run's own immutable object is
/// what makes "this run trusts this root" a property of the run.
///
/// **THE PLAN BYTES STAY VERBATIM.** `restore.yaml` is `spec.planBytes`
/// `.clone()` and nothing else, at both arities: they are the bytes an
/// approver signed, and a re-serialisation would change the hash the runner
/// pins.
///
/// # Errors
///
/// [`RestoreError`] for an object with no namespace or UID, or a CA bundle
/// that is not UTF-8 — a `ConfigMap`'s `data` values are strings, and
/// replacement characters in a trust root are not a trust root.
pub fn plan_config_map_with_destinations(
    restore: &Restore,
    destinations: Option<&RestoreDestinations>,
) -> Result<ConfigMap, RestoreError> {
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
            annotations: Some(
                [
                    (BUNDLE_RESTORE_UID_ANNOTATION.to_string(), uid.clone()),
                    (
                        BUNDLE_PLAN_HASH_ANNOTATION.to_string(),
                        recomputed_plan_hash(restore),
                    ),
                ]
                .into_iter()
                .collect(),
            ),
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
        data: Some({
            let mut data: std::collections::BTreeMap<String, String> = [(
                PLAN_SPEC_KEY.to_string(),
                // VERBATIM. `.clone()` and nothing else.
                restore.spec.plan_bytes.clone(),
            )]
            .into_iter()
            .collect();
            if let Some(pair) = destinations {
                for (key, resolved) in [
                    (destination::ARCHIVE_CA_PLAN_KEY, &pair.source),
                    (destination::EVIDENCE_CA_PLAN_KEY, &pair.evidence),
                ] {
                    if let Some(pem) = resolved.ca_pem.as_ref() {
                        data.insert(key.to_string(), ca_text(resolved, pem)?);
                    }
                }
            }
            data
        }),
        immutable: Some(true),
        ..ConfigMap::default()
    })
}

/// One destination's CA bundle as `ConfigMap` text.
///
/// NOT `from_utf8_lossy`. A `ConfigMap`'s `data` values are strings, so bytes
/// that are not UTF-8 would be replacement-charactered on the way in and the
/// runner would build its store against a mangled trust root — silently, and
/// only failing at the first TLS handshake. `destination::check_ca_bundle` has
/// already refused anything that is not a PEM bundle; this is the second rail,
/// and it names the object rather than the byte count.
fn ca_text(resolved: &ResolvedDestination, pem: &[u8]) -> Result<String, RestoreError> {
    String::from_utf8(pem.to_vec()).map_err(|_| {
        RestoreError::Refused(
            TERMINAL_STATE_PLAN_CONFIG_MAP_CONFLICT,
            format!(
                "the CA bundle of BackupDestination {}/{} is not UTF-8, so it cannot be written \
                 into the immutable plan ConfigMap the runner mounts",
                resolved.namespace, resolved.name
            ),
        )
    })
}

// ---------------------------------------------------------------------------
// Destination admission — D2 §3.6 checks 5-9
// ---------------------------------------------------------------------------

/// The two destinations a destination-backed `Restore` resolves.
///
/// # TWO, BECAUSE A RESTORE READS ONE STORE AND WRITES ANOTHER
///
/// The archive it reads is the source destination's, under `archiveRead`; the
/// scorecard and offset report it writes are the evidence destination's, under
/// `evidenceWrite`. They may be the same `BackupDestination` — that is the
/// common case — and the run then carries one credential and
/// `LOGWEIR_EVIDENCE_CREDENTIALS=archive`. They may be two, which is exactly
/// what defect **UI-HTTPDOWNGRADE**'s second half is about: one endpoint,
/// region and addressing applied to both stores because the wizard had only one
/// set of fields. Here they are two independent resolutions and neither field
/// of one reaches the other.
#[derive(Debug)]
pub struct RestoreDestinations {
    /// `spec.sourceDestinationRef`, resolved for
    /// [`DestinationRole::ArchiveRead`].
    pub source: ResolvedDestination,
    /// `spec.evidenceDestinationRef`, resolved for
    /// [`DestinationRole::EvidenceWrite`].
    pub evidence: ResolvedDestination,
}

/// What destination admission decided for one `Restore` — D2 §3.6 checks 5-9.
#[derive(Debug)]
pub enum RestoreDestinationAdmission {
    /// The `Restore` names neither ref: a legacy inline-`sourceArchive` run,
    /// unchanged in every respect.
    NotRequested,
    /// Both destinations resolved, and the approved plan bytes name exactly
    /// their locations.
    Resolved(Box<RestoreDestinations>),
    /// One of them is absent or not yet `Valid`. HELD, never terminal: D2 §3.6
    /// puts this with `ApprovalNotVerified`, because an operator who is still
    /// creating the destination is in the same position as an approver who has
    /// not signed yet, and a `Restore.spec` is CEL-immutable, so a terminal
    /// refusal could never be repaired in place.
    Holding {
        /// The resolver's `CheckCode`, as a condition `reason`.
        reason: &'static str,
        /// What is wrong and what to do. Names objects, fields and locations —
        /// never a credential.
        message: String,
    },
}

/// Resolve the two `BackupDestination`s a `Restore` names and check the
/// approved plan against them — D2 §3.6 checks 5 through 9.
///
/// # THE ORDER, AND WHY THE PLAN IS CHECKED AFTER THE DESTINATIONS
///
/// 5. Both refs resolve and are `Valid` — a HOLD while they are not.
/// 6. `spec.planBytes` parses as a `logweir_core::spec::RestoreSpec`. READ
///    ONLY, and never re-emitted: those bytes are what an approver signed, and
///    the plan `ConfigMap` still carries them verbatim.
/// 7. `plan.source.storage` is the source destination's archive `StorageUrl`,
///    exact on all six fields.
/// 8. `plan.evidence` is the evidence destination's evidence `StorageUrl`.
/// 9. The `archiveRead` and `evidenceWrite` grants resolve, and the two can be
///    satisfied by ONE pod — one pod has one ServiceAccount.
///
/// Checks 7 and 8 are the ones that make a saved destination mean anything on
/// this path. The runner reads the location out of the PLAN, not out of the
/// destination, so a destination whose location differs from the approved plan
/// would otherwise contribute its CREDENTIALS to a run that reads somewhere
/// else — a credential pointed at a location nobody approved. Refusing here is
/// terminal, and the message names fields rather than credentials.
///
/// **A DESTINATION EDIT NEVER INVALIDATES AN APPROVAL.** Location and transport
/// are immutable on a `BackupDestination`, so the plan bytes an approver signed
/// cannot go stale through an edit; only preflights do (D2 §6.6).
///
/// # Errors
///
/// [`RestoreError::Api`] for a failed read, or [`RestoreError::Refused`] with
/// the terminal state named by checks 6 through 9.
pub async fn admit_restore_destinations(
    restore: &Restore,
    client: &kube::Client,
    namespace: &str,
    now: DateTime<Utc>,
) -> Result<RestoreDestinationAdmission, RestoreError> {
    let source_ref = restore.spec.source_destination_ref.as_ref();
    let evidence_ref = restore.spec.evidence_destination_ref.as_ref();
    let (source_ref, evidence_ref) = match (source_ref, evidence_ref) {
        (None, None) => return Ok(RestoreDestinationAdmission::NotRequested),
        (Some(s), Some(e)) => (s, e),
        // THE CEL RULE REFUSES THIS PAIR ON ADMISSION
        // (`crds::restore::DESTINATION_PAIR_RULE`), and it is refused again
        // here for the reason every re-validated rule in this crate exists: an
        // object admitted by an OLDER CRD revision reaches this controller
        // unchecked. Half a destination-backed restore would read from a saved
        // destination and write its scorecard wherever the inline block says.
        (present, _) => {
            return Err(RestoreError::Refused(
                CheckCode::DestinationRoleNotConfigured.as_str(),
                format!(
                    "spec.{} is set and its partner is not; a Restore reads its archive from one \
                     destination and writes its evidence to another, so the two are set together \
                     or neither is",
                    if present.is_some() {
                        "sourceDestinationRef"
                    } else {
                        "evidenceDestinationRef"
                    }
                ),
            ))
        }
    };

    let load = crate::check::policy::load(
        client,
        super::topic_discovery::configured_policy_ref().as_ref(),
        &crate::check::policy::PolicyCache::new(),
        now,
    )
    .await
    .map_err(RestoreError::Api)?;
    let policy = load.policy();

    // CHECK 5.
    let source = match destination::resolve_ref(
        client,
        namespace,
        &source_ref.name,
        DestinationRole::ArchiveRead,
        policy,
    )
    .await
    {
        Ok(resolved) => resolved,
        Err(ResolveError::Api(e)) => return Err(RestoreError::Api(e)),
        Err(ResolveError::Refused(refusal)) => {
            return destination_verdict(&refusal);
        }
    };
    let evidence = match destination::resolve_ref(
        client,
        namespace,
        &evidence_ref.name,
        DestinationRole::EvidenceWrite,
        policy,
    )
    .await
    {
        Ok(resolved) => resolved,
        Err(ResolveError::Api(e)) => return Err(RestoreError::Api(e)),
        Err(ResolveError::Refused(refusal)) => {
            return destination_verdict(&refusal);
        }
    };
    // The Job runs where both sets of references resolve, and nowhere else.
    for resolved in [&source, &evidence] {
        if let Err(refusal) = resolved.check_job_namespace(namespace) {
            return Err(RestoreError::Refused(refusal.reason(), refusal.message));
        }
        // D2 §3.5's U1 gate, on BOTH destinations: a restore drives the engine
        // over the archive AND writes its scorecard, and the engine child is in
        // the same pod either way. See `backup::ENGINE_CUSTOM_CA_VERIFIED`.
        if resolved.ca_bundle.is_some() && !backup::engine_custom_ca_allowed(policy) {
            return Err(RestoreError::Refused(
                CheckCode::CaBundleUnsupportedByEngine.as_str(),
                backup::engine_custom_ca_refusal(&resolved.namespace, &resolved.name),
            ));
        }
    }

    // CHECK 6. READ ONLY. These bytes are never re-emitted: `plan_config_map`
    // still writes `spec.planBytes` verbatim, because they are the bytes an
    // approver signed and a re-serialisation would change the hash.
    let plan: logweir_core::spec::RestoreSpec = serde_yaml::from_str(&restore.spec.plan_bytes)
        .map_err(|e| {
            RestoreError::Refused(
                CheckCode::PlanUnparseable.as_str(),
                format!(
                    "spec.planBytes is not a readable restore plan ({e}), so the destinations this \
                     Restore names cannot be compared with the location it was approved to read"
                ),
            )
        })?;

    // CHECK 7. EXACT, ON ALL SIX FIELDS. The runner reads the location out of
    // the PLAN, so a destination whose location differs would contribute its
    // credential to a run that reads somewhere else.
    let expected_source = source.plan_storage();
    if plan.source.storage != expected_source {
        return Err(RestoreError::Refused(
            CheckCode::PlanDestinationMismatch.as_str(),
            format!(
                "the approved plan reads {} and BackupDestination {}/{} is {}; the runner reads \
                 the location the plan names, so this destination's credential would be presented \
                 at a location nobody approved. Build a plan from this destination and approve it",
                describe_storage(&plan.source.storage),
                source.namespace,
                source.name,
                describe_storage(&expected_source)
            ),
        ));
    }

    // CHECK 8. The evidence root is Global Constraint 6's `logweir/` and the
    // resolver imposes it, so this compares the BUCKET the scorecard lands in.
    let expected_evidence = evidence.evidence_storage();
    if plan.evidence != expected_evidence {
        return Err(RestoreError::Refused(
            CheckCode::PlanEvidenceDestinationMismatch.as_str(),
            format!(
                "the approved plan writes its scorecard to {} and BackupDestination {}/{} is {}; \
                 the signed document would land outside the destination this Restore names",
                describe_storage(&plan.evidence),
                evidence.namespace,
                evidence.name,
                describe_storage(&expected_evidence)
            ),
        ));
    }

    // CHECK 9. ONE POD, ONE ServiceAccount. `evidence_env` is the one place
    // that decides how two grants share a pod, and it refuses two different
    // workload identities as `ExecutionContextConflict` — a fact about
    // Kubernetes, not a policy.
    if let Err(refusal) = evidence.evidence_env(&source) {
        return Err(RestoreError::Refused(refusal.reason(), refusal.message));
    }

    Ok(RestoreDestinationAdmission::Resolved(Box::new(
        RestoreDestinations { source, evidence },
    )))
}

/// A resolver refusal as D2 §3.6's split: a hold for the two codes an operator
/// can still fix by creating or repairing an object, a terminal refusal for
/// every decision.
///
/// `DestinationNotFound` and `DestinationNotValid` are races — the ref and the
/// object are two applies, and a `BackupDestination`'s own reconciler has to
/// run before it carries a `Valid=True`. Everything else is a decision, and a
/// decision retried forever is a decision nobody sees.
fn destination_verdict(
    refusal: &destination::DestinationRefusal,
) -> Result<RestoreDestinationAdmission, RestoreError> {
    if refusal.is_hold() {
        Ok(RestoreDestinationAdmission::Holding {
            reason: refusal.reason(),
            message: refusal.message.clone(),
        })
    } else {
        Err(RestoreError::Refused(
            refusal.reason(),
            refusal.message.clone(),
        ))
    }
}

/// A `StorageUrl` as an operator-readable location, with no credential in it.
///
/// `{:?}` WOULD HAVE DONE, AND IT IS THE WRONG SHAPE. A `PlanDestinationMismatch`
/// message is read by somebody comparing two locations field by field, and
/// `S3 { bucket: "a", prefix: "b", region: None, … }` twice on one line is a
/// diff nobody can perform in their head. `StorageUrl` carries no credential at
/// any variant, so nothing here needs redaction — the endpoint, bucket and
/// prefix are exactly what the operator has to compare.
fn describe_storage(url: &logweir_core::engine::StorageUrl) -> String {
    use logweir_core::engine::StorageUrl as U;
    match url {
        U::S3 {
            bucket,
            prefix,
            region,
            endpoint,
            path_style,
            allow_http,
        } => format!(
            "s3://{bucket}/{prefix} (region {}, endpoint {}, {}, {})",
            region.as_deref().unwrap_or("<unset>"),
            endpoint.as_deref().unwrap_or("<default>"),
            if *path_style {
                "path-style"
            } else {
                "virtual-hosted"
            },
            if *allow_http { "http allowed" } else { "https" }
        ),
        U::Gcs { bucket, prefix } => format!("gs://{bucket}/{prefix}"),
        U::Azure {
            account_name,
            container_name,
            prefix,
        } => format!("az://{account_name}/{container_name}/{prefix}"),
        U::Filesystem { path } => format!("file://{}", path.display()),
    }
}

/// Exact compatibility for a create-only plan ConfigMap retry.
#[must_use]
pub fn compatible_plan_config_map(existing: &ConfigMap, desired: &ConfigMap, uid: &str) -> bool {
    complete_restore_owner(&existing.metadata, desired, uid)
        && existing.immutable == Some(true)
        && existing.data == desired.data
        && desired_annotations_match(existing, desired)
}

/// Compatibility for a controller-crash transition where the previous
/// release created the mutable plan ConfigMap but had not created its Job.
/// The new Job pins the exact plan digest, so a later replacement is rejected
/// by the runner before any client is constructed.
#[must_use]
pub fn compatible_legacy_plan_config_map(
    existing: &ConfigMap,
    desired: &ConfigMap,
    uid: &str,
) -> bool {
    complete_restore_owner(&existing.metadata, desired, uid)
        && existing.immutable != Some(true)
        && existing.data == desired.data
        && existing
            .metadata
            .annotations
            .as_ref()
            .is_none_or(|annotations| {
                !annotations.contains_key(BUNDLE_RESTORE_UID_ANNOTATION)
                    && !annotations.contains_key(BUNDLE_PLAN_HASH_ANNOTATION)
            })
}

/// The object an operator would edit to change a bundle refusal — the roster
/// when this namespace resolves to the synthesised `legacy-roster-v1`, the
/// policy otherwise.
///
/// THE REFUSAL NAMES THE THING TO EDIT. "absent from the TrustRoster" is the
/// wrong sentence for a namespace governed by `org-default`, and it is the
/// sentence that made F1 a permanent hold with no way out of it.
fn trust_source_phrase(trust: &crate::trust::ResolvedTrust) -> String {
    if trust.source.is_legacy() {
        format!("the TrustRoster '{}'", crate::ROSTER_NAME)
    } else {
        format!("the TrustPolicy '{}'", trust.source.name())
    }
}

/// Build the immutable public approval bundle for exactly one Restore.
///
/// The bytes copied from `Approval.spec` are never parsed and re-emitted. The
/// public key is the entry the verified status named, **taken from the trust
/// this namespace resolves to**, and the allowlist is that same resolution's
/// `allowedTargetClusterIds`. Owner UID plus the binding annotations make the
/// Kubernetes object specific to this Restore; the runner still independently
/// verifies the signature and plan hash from the mounted files.
///
/// # Why this reads the RESOLVED trust and not `TrustRoster/default` (PLAT-19.1)
///
/// Because admission does, and the two must not be able to disagree. Before
/// PLAT-19.1 both `approval::decide` and this function read the one roster, so
/// a key that verified an `Approval` was by construction a key this bundle
/// could render. Once admission resolves a `TrustPolicy` (D3 §7.1) and this
/// does not, a `GovernedApproval` key that lives only in the policy — which is
/// exactly what §7.6 step 1 stages, and what §15's L7 namespace B has, with no
/// roster at all — writes `Verified=True` on the `Approval` and then leaves
/// the `Restore` in a **non-terminal** `ApprovalBundleMaterializationFailed`
/// hold for ever, pointing the operator at an object D3 §7.2 says no longer
/// decides anything for their namespace. It is fail-closed, and it is a
/// permanent restore hold on the one procedure the decision documents.
///
/// # The expiry re-check is `may_sign_new`, not `status.expiredKeyIds`
///
/// Same reason, one level down. The roster's `expiredKeyIds` is a list its own
/// reconciler derives from `notAfter` alone; the question this site asks is D3
/// §7.4's first one — *may this key authorise something new* — which is also
/// false for a `Retired` or `Revoked` key whose `notAfter` has not arrived. A
/// bundle is the last thing written before a runner executes under that key,
/// so asking the weaker question here would mount the public half of a key an
/// administrator withdrew between admission and materialization. `now` is an
/// argument: this function reads no clock.
pub fn approval_bundle_config_map(
    restore: &Restore,
    approval: &Approval,
    trust: &crate::trust::ResolvedTrust,
    now: DateTime<Utc>,
) -> Result<ConfigMap, RestoreError> {
    approval_bundle_config_map_with_policy(restore, approval, trust, &EffectivePolicy::Legacy, now)
}

/// [`approval_bundle_config_map`], under the namespace's approval-policy
/// binding — PLAT-19.2, D0's "bundle contract v2".
///
/// # What a v2 authorization adds, and what it re-checks
///
/// For a Restore authorised by authorization document v2 the bundle carries
/// two more PUBLIC members: [`CONFIRMATION_KEY_FILE`], the console key that
/// attested the requester, and [`APPROVAL_POLICY_FILE`], the frozen policy
/// snapshot whose digest the signed document names. `approver.pub.pem` holds
/// the key that AUTHORISED the run — the governed approver's under
/// `Governed`, the console's own under `Ordinary` — and each key must still
/// be one this namespace's trust lets sign something new, under ITS usage. A
/// key an administrator withdrew between verification and materialization is
/// never mounted.
///
/// # Errors
///
/// [`RestoreError::Materialization`] when a key is gone or withdrawn, and
/// [`RestoreError::Refused`] on a plan-hash mismatch.
pub fn approval_bundle_config_map_with_policy(
    restore: &Restore,
    approval: &Approval,
    trust: &crate::trust::ResolvedTrust,
    policy: &EffectivePolicy,
    now: DateTime<Utc>,
) -> Result<ConfigMap, RestoreError> {
    let name = restore.name_any();
    let namespace = restore
        .namespace()
        .ok_or_else(|| RestoreError::NoNamespace(name.clone()))?;
    let restore_uid = restore
        .uid()
        .ok_or_else(|| RestoreError::NoUid(name.clone()))?;
    let approval_uid = approval.uid().ok_or_else(|| {
        RestoreError::Materialization(format!(
            "the verified Approval {} carries no metadata.uid",
            approval.name_any()
        ))
    })?;
    let matched_key_id = approval
        .status
        .as_ref()
        .filter(|status| status.verified == Some(true))
        .and_then(|status| status.matched_key_id.as_deref())
        .ok_or_else(|| {
            RestoreError::Materialization(format!(
                "the Approval {} is not Verified=True with a matchedKeyId",
                approval.name_any()
            ))
        })?;
    let key = trust.key(matched_key_id).ok_or_else(|| {
        RestoreError::Materialization(format!(
            "the Approval {} verified under key {matched_key_id}, which {} does not carry; add \
             the approver's public half there, not to an object this namespace no longer resolves \
             to",
            approval.name_any(),
            trust_source_phrase(trust)
        ))
    })?;
    // THE USAGE THE AUTHORISING KEY MUST STILL HOLD is the bound policy's:
    // a governed approver's, or — under `Ordinary` only — the console's.
    let bound = policy.bound().filter(|_| is_authorization_v2(approval));
    let authorising_usage = match bound.map(|p| p.mode) {
        Some(ApprovalMode::Ordinary) => logweir_core::trust::KeyUsage::ConsoleConfirmation,
        Some(ApprovalMode::Governed) | None => logweir_core::trust::KeyUsage::GovernedApproval,
    };
    if let Err(refusal) = trust.may_sign_new_for(matched_key_id, authorising_usage, now) {
        return Err(RestoreError::Materialization(format!(
            "the Approval {} verified under key {matched_key_id}, which {} no longer accepts for \
             a new authorisation ({}); no bundle is written and nothing executes under it",
            approval.name_any(),
            trust_source_phrase(trust),
            refusal.as_str()
        )));
    }
    // AND THE CONSOLE KEY, UNDER v2: the second of D0's "both required public
    // keys", re-checked here for the same reason as the first.
    let confirmation = match bound {
        None => None,
        Some(bound) => {
            let confirmation_key_id = approval
                .status
                .as_ref()
                .and_then(|s| s.authorization.as_ref())
                .map(|a| a.confirmation_key_id.clone())
                .ok_or_else(|| {
                    RestoreError::Materialization(format!(
                        "the Approval {} carries an authorization document v2 and no \
                         status.authorization.confirmationKeyId",
                        approval.name_any()
                    ))
                })?;
            let confirmation_key = trust.key(&confirmation_key_id).ok_or_else(|| {
                RestoreError::Materialization(format!(
                    "the Approval {} was confirmed under console key {confirmation_key_id}, which \
                     {} does not carry",
                    approval.name_any(),
                    trust_source_phrase(trust)
                ))
            })?;
            if let Err(refusal) = trust.may_sign_new_for(
                &confirmation_key_id,
                logweir_core::trust::KeyUsage::ConsoleConfirmation,
                now,
            ) {
                return Err(RestoreError::Materialization(format!(
                    "the Approval {} was confirmed under console key {confirmation_key_id}, which \
                     {} no longer accepts ({}); no bundle is written",
                    approval.name_any(),
                    trust_source_phrase(trust),
                    refusal.as_str()
                )));
            }
            Some((confirmation_key.spki_pem.clone(), bound))
        }
    };
    let plan_hash = recomputed_plan_hash(restore);
    let approval_hash = approval_plan_hash(approval).unwrap_or_default();
    if approval_hash != plan_hash {
        return Err(RestoreError::Refused(
            TERMINAL_STATE_PLAN_HASH_MISMATCH,
            format!(
                "spec.planBytes hash to {plan_hash} and the verified Approval document names {approval_hash}"
            ),
        ));
    }
    // `allowedTargetClusterIds` (D3 §7.2), which the synthesis carries verbatim
    // from the roster's `allowedClusterIds` — so an unmigrated cluster renders
    // the same bytes it always did.
    let allowed = logweir_core::spec::AllowedClusters {
        allowed_cluster_ids: trust.allowed_target_cluster_ids.clone(),
        source_cluster_id: None,
    };
    let allowed_bytes = serde_json::to_string_pretty(&allowed).map_err(|error| {
        RestoreError::Materialization(format!(
            "the target-cluster allowlist could not be rendered: {error}"
        ))
    })?;
    let mut annotations: BTreeMap<String, String> = [
        (
            BUNDLE_RESTORE_UID_ANNOTATION.to_string(),
            restore_uid.clone(),
        ),
        (BUNDLE_PLAN_HASH_ANNOTATION.to_string(), plan_hash),
        (
            BUNDLE_APPROVAL_NAME_ANNOTATION.to_string(),
            approval.name_any(),
        ),
        (BUNDLE_APPROVAL_UID_ANNOTATION.to_string(), approval_uid),
    ]
    .into_iter()
    .collect();
    let mut data: BTreeMap<String, String> = [
        (
            APPROVAL_DOC_FILE.to_string(),
            approval.spec.approval_bytes.clone(),
        ),
        (
            APPROVAL_SIG_FILE.to_string(),
            approval.spec.sidecar_bytes.clone(),
        ),
        (APPROVER_KEY_FILE.to_string(), key.spki_pem.clone()), // public SPKI only
        (ALLOWED_CLUSTERS_FILE.to_string(), allowed_bytes),
    ]
    .into_iter()
    .collect();
    if let Some((confirmation_pem, bound)) = confirmation {
        let snapshot = String::from_utf8(bound.snapshot_bytes()).map_err(|e| {
            RestoreError::Materialization(format!("the approval-policy snapshot is not UTF-8: {e}"))
        })?;
        annotations.insert(
            BUNDLE_APPROVAL_POLICY_DIGEST_ANNOTATION.to_string(),
            bound.digest(),
        );
        data.insert(CONFIRMATION_KEY_FILE.to_string(), confirmation_pem); // public SPKI only
        data.insert(APPROVAL_POLICY_FILE.to_string(), snapshot);
    }

    // PLAT-15.2: the evidence keyring, exactly when the plan binds a point.
    data.extend(evidence_member(restore, trust)?);

    Ok(bundle_object(
        name,
        namespace,
        restore_uid,
        annotations,
        data,
    ))
}

/// The [`EVIDENCE_KEYS_FILE`] member, when this plan binds a point.
fn evidence_member(
    restore: &Restore,
    trust: &crate::trust::ResolvedTrust,
) -> Result<Option<(String, String)>, RestoreError> {
    if !plan_binds_point(restore) {
        return Ok(None);
    }
    Ok(Some((
        EVIDENCE_KEYS_FILE.to_string(),
        evidence_keyring_bytes(trust)?,
    )))
}

// --------------------------------------------------------------------------
// THE STANDING-AUTHORIZATION ARM OF THE APPROVAL BUNDLE (D3 W7, PLAT-14.3)
//
// One function, two arms, one object shape. `bundle_object` below is the shape
// both arms produce, factored out so that the immutability, the single
// controller owner and the name can never differ between "a human approved this
// run" and "a human approved this schedule". A second renderer would be a second
// answer to *what did the controller commit this run to*, and the digests in the
// Job template are computed from whichever one ran.
// --------------------------------------------------------------------------

/// The ConfigMap shape every approval bundle has, whichever arm rendered it.
fn bundle_object(
    name: String,
    namespace: String,
    restore_uid: String,
    annotations: BTreeMap<String, String>,
    data: BTreeMap<String, String>,
) -> ConfigMap {
    ConfigMap {
        metadata: ObjectMeta {
            name: Some(approval_bundle_config_map_name(&name)),
            namespace: Some(namespace),
            annotations: Some(annotations),
            owner_references: Some(vec![OwnerReference {
                api_version: Restore::api_version(&()).to_string(),
                kind: Restore::kind(&()).to_string(),
                name,
                uid: restore_uid,
                controller: Some(true),
                block_owner_deletion: Some(true),
            }]),
            ..ObjectMeta::default()
        },
        immutable: Some(true),
        data: Some(data),
        ..ConfigMap::default()
    }
}

/// The trusted-public-keys member, standing-only — D3 §4.3(e).
pub const AUTHORIZATION_KEYS_FILE: &str = "authorization-keys.json";

/// The evidence-signing keyring member — D3 §5.5 step 6
/// (RUNNER-POINT-BINDING-SKIPS-SIGNATURE). Present in either arm's bundle
/// exactly when the plan binds a recovery point (`source.point`); the runner
/// verifies that point's receipt signature against it before any client is
/// constructed, and refuses a point-bound plan without it.
pub const EVIDENCE_KEYS_FILE: &str = "evidence-keys.json";

/// Whether this `Restore`'s plan binds a recovery point (`source.point`).
///
/// ONE PREDICATE decides the bundle member, its pinned digest, the runner
/// flag and the projected file, so the four cannot disagree. A plan that does
/// not parse binds nothing here; the runner refuses it on its own terms.
#[must_use]
pub fn plan_binds_point(restore: &Restore) -> bool {
    serde_yaml::from_str::<logweir_core::spec::DrillSpec>(&restore.spec.plan_bytes)
        .is_ok_and(|plan| plan.source.point.is_some())
}

/// The [`EVIDENCE_KEYS_FILE`] bytes: every key of the namespace's resolved
/// trust whose public half parses and whose declared id is its own, with its
/// whole lifecycle record. The runner, not this function, judges each key --
/// with `logweir_core::trust::decide` for `EvidenceSigning` against the
/// receipt's own claimed signing time -- so a revoked key is RENDERED (the
/// refusal can then name it) and never accepted.
///
/// # Errors
///
/// [`RestoreError::Materialization`] for a keyring that will not serialise.
pub fn evidence_keyring_bytes(trust: &crate::trust::ResolvedTrust) -> Result<String, RestoreError> {
    use logweir_core::execution_contract as wire;
    let keyring = wire::EvidenceKeyring {
        format_version: wire::EVIDENCE_KEYRING_FORMAT_VERSION.to_string(),
        keys: trust
            .keys
            .iter()
            .filter(|k| k.is_usable())
            .map(|k| wire::EvidenceKey {
                public_key_pem: k.spki_pem.clone(),
                trust: k.trust.clone(),
            })
            .collect(),
    };
    serde_json::to_string_pretty(&keyring).map_err(|error| {
        RestoreError::Materialization(format!(
            "the evidence-signing keyring could not be rendered: {error}"
        ))
    })
}

/// The SIGNED standing rehearsal authorization, at
/// `/approval/standing-authorization.json`.
///
/// ITS OWN NAME, AND NOT `approval.json`. The runner verifies `approval.json`
/// under `PAYLOAD_TYPE_APPROVAL`; a standing document is signed under its own
/// payload type, so putting it in the per-run slot makes a correctly signed
/// rehearsal look like a substituted approval. This is the path D3 W5's landed
/// fixture mounts and the one `--standing-authorization` points at.
pub const STANDING_AUTHORIZATION_FILE: &str = "standing-authorization.json";

/// Its detached DSSE sidecar, at `/approval/standing-authorization.sig`.
///
/// The runner DERIVES this path from [`STANDING_AUTHORIZATION_FILE`] by
/// replacing the extension — one flag fewer to get wrong — so the two names
/// must differ only in that extension.
pub const STANDING_AUTHORIZATION_SIG_FILE: &str = "standing-authorization.sig";

/// D3 §4.3(e)'s bundle, for a `Restore` authorised by a STANDING document.
///
/// # Five members, and the standing document has its OWN name
///
/// | file | bytes |
/// |---|---|
/// | `standing-authorization.json` | the SIGNED standing envelope, copied verbatim from `Approval.spec.approvalBytes` |
/// | `standing-authorization.sig` | its DSSE sidecar, copied verbatim — the runner DERIVES this path from the one above |
/// | `authorization-keys.json` | every key this namespace's trust currently lets authorise |
/// | `allowed-clusters.json` | **exactly** `[scope.targetClusterId]` |
/// | `approver.pub.pem` | the public SPKI of the key the `Approval` verified under |
///
/// **The standing document is NOT written as `approval.json`, and an earlier
/// revision of this function got that wrong.** The runner reads `approval.json`
/// into `bundle.approval` and hands it to
/// `phase1_approval::verify_bytes`, which verifies under
/// `PAYLOAD_TYPE_APPROVAL`; a standing sidecar carries
/// `PAYLOAD_TYPE_STANDING_AUTHORIZATION`, so `verify_detached` returns
/// `Error::Verify("payload_type mismatch…")` — a variant whose own doc comment
/// calls it *evidence of substitution*. A correctly signed, correctly scoped
/// rehearsal would have been reported to the operator as a TAMPERED APPROVAL.
/// D3 W5's landed fixture (`crates/logweir/tests/execution_contract_v2.rs`)
/// mounts the standing document at `standing-authorization.json` with its
/// sidecar derived at `.sig`, BESIDE a genuine per-run approval, and that is the
/// layout written here.
///
/// # There is NO `approval.json` slot — PLAT-14.3b removed it
///
/// D3 W7 wrote the standing envelope into `approval.json` as a PLACEHOLDER,
/// because `--approval` was mandatory and `load_startup_inputs` called
/// `phase1_approval::verify_bytes` unconditionally. That was the reason no
/// rehearsal could execute: a per-run approval must bind `sha256(plan bytes)`,
/// which only a human with a signing key can produce and `weirkeeper` links no
/// signer (Global Constraint 27) — and the placeholder was signed under
/// `PAYLOAD_TYPE_STANDING_AUTHORIZATION`, so the runner reported a correctly
/// signed rehearsal as a TAMPERED APPROVAL.
///
/// PLAT-14.3b made the standing document REPLACE the per-run approval instead
/// of sitting beside it. `--approval` is now refused under
/// `AUTHORIZATION_KIND=standing`, the contract emits no
/// `LOGWEIR_EXECUTION_APPROVAL_SHA256` / `…_SIDECAR_SHA256` for a rehearsal
/// (see [`standing_execution_contract_env`]), and this bundle carries five
/// members. `admit` admits a standing `Restore` on `spec.authorization` and a
/// Job runs under it.
///
/// # The allowlist is EQUALITY, not membership
///
/// `[scope.targetClusterId]` and nothing else, which is W5's R1.3 obligation 1
/// and is what `plan_within_scope` then requires. Narrowing the allowlist is
/// what turns "the signed scope names cluster X" into "this run cannot reach
/// anything but X" without a second broker round trip: phase 0 refuses any
/// observed cluster id outside the mounted set, and the mounted set is one id.
///
/// # No private material, and no key lifecycle
///
/// `publicKeyPem` is public SPKI. Lifecycle — `state`, `notBefore`/`notAfter`,
/// revocation — is `trust::decide`'s and was evaluated by the caller BEFORE
/// these bytes were rendered; the runner re-checks what a credential-less
/// process can, which is pinning and usage.
///
/// # Errors
///
/// [`RestoreError`] for an object with no namespace or UID, for a BLANK
/// `approval_uid` (the runner treats a present-and-blank environment value as
/// missing and refuses the whole contract), for a key id the resolved trust does
/// not carry, and for a keyring that will not serialise.
/// Everything about a VERIFIED standing authorization that
/// [`standing_bundle_config_map`] writes into the bundle.
///
/// A struct rather than six more parameters, and not only for clippy's sake:
/// these six values are ONE decision — "this document, signed by this key, for
/// this cluster, under this `Approval`" — and a caller that could pass five of
/// them from one authorization and the sixth from another would be writing a
/// bundle that binds two different grants.
#[derive(Debug, Clone, Copy)]
pub struct StandingInputs<'a> {
    /// The signed envelope, verbatim from `Approval.spec.approvalBytes`.
    pub envelope: &'a str,
    /// Its DSSE sidecar, verbatim from `Approval.spec.sidecarBytes`.
    pub sidecar: &'a str,
    /// The `Approval` object's `metadata.uid`. **Never blank** — see the
    /// function's `# Errors`.
    pub approval_uid: &'a str,
    /// The key id the `Approval` verified under.
    pub key_id: &'a str,
    /// D3 §4.3(e)'s trusted public keys.
    pub keyring: &'a logweir_core::execution_contract::AuthorizationKeyring,
    /// The cluster id inside the SIGNED scope — the whole allowlist.
    pub target_cluster_id: &'a str,
}

pub fn standing_bundle_config_map(
    restore: &Restore,
    signed: &StandingInputs<'_>,
    trust: &crate::trust::ResolvedTrust,
) -> Result<ConfigMap, RestoreError> {
    let StandingInputs {
        envelope,
        sidecar,
        approval_uid,
        key_id,
        keyring,
        target_cluster_id,
    } = *signed;
    let name = restore.name_any();
    let namespace = restore
        .namespace()
        .ok_or_else(|| RestoreError::NoNamespace(name.clone()))?;
    let restore_uid = restore
        .uid()
        .ok_or_else(|| RestoreError::NoUid(name.clone()))?;
    let authorization = restore.spec.authorization.as_ref().ok_or_else(|| {
        RestoreError::Materialization(format!(
            "the Restore {name} carries no spec.authorization, so it is not standing-authorised"
        ))
    })?;
    // A BLANK UID IS NOT A UID. `LOGWEIR_EXECUTION_APPROVAL_UID` is one of the
    // thirteen mandatory contract values, and the runner's own `required()`
    // treats a present-and-blank value as missing and refuses the entire
    // contract before phase 0. Catching it here, where the bundle is written,
    // means the operator reads a controller message naming the `Approval`
    // rather than a Job that aborts.
    if approval_uid.trim().is_empty() {
        return Err(RestoreError::Materialization(format!(
            "the Approval {} carries no metadata.uid, and the execution contract's \
             APPROVAL_UID may not be blank; no bundle is written",
            authorization.approval_ref.name
        )));
    }
    let key = trust.key(key_id).ok_or_else(|| {
        RestoreError::Materialization(format!(
            "the standing authorization verified under key {key_id}, which {} does not carry",
            trust_source_phrase(trust)
        ))
    })?;
    if keyring.keys.is_empty() {
        return Err(RestoreError::Materialization(format!(
            "{} currently lets no key authorise a rehearsal; an empty keyring is refused by the \
             runner and is not written",
            trust_source_phrase(trust)
        )));
    }
    let keyring_bytes = serde_json::to_string_pretty(keyring).map_err(|error| {
        RestoreError::Materialization(format!(
            "the authorization keyring could not be rendered: {error}"
        ))
    })?;
    let allowed = logweir_core::spec::AllowedClusters {
        allowed_cluster_ids: vec![target_cluster_id.to_string()],
        source_cluster_id: None,
    };
    let allowed_bytes = serde_json::to_string_pretty(&allowed).map_err(|error| {
        RestoreError::Materialization(format!(
            "the target-cluster allowlist could not be rendered: {error}"
        ))
    })?;
    let annotations: BTreeMap<String, String> = [
        (
            BUNDLE_RESTORE_UID_ANNOTATION.to_string(),
            restore_uid.clone(),
        ),
        (
            BUNDLE_PLAN_HASH_ANNOTATION.to_string(),
            recomputed_plan_hash(restore),
        ),
        (
            BUNDLE_APPROVAL_NAME_ANNOTATION.to_string(),
            authorization.approval_ref.name.clone(),
        ),
        (
            BUNDLE_APPROVAL_UID_ANNOTATION.to_string(),
            approval_uid.to_string(),
        ),
    ]
    .into_iter()
    .collect();
    Ok(bundle_object(
        name,
        namespace,
        restore_uid,
        annotations,
        [
            (
                STANDING_AUTHORIZATION_FILE.to_string(),
                envelope.to_string(),
            ),
            (
                STANDING_AUTHORIZATION_SIG_FILE.to_string(),
                sidecar.to_string(),
            ),
            (AUTHORIZATION_KEYS_FILE.to_string(), keyring_bytes),
            (ALLOWED_CLUSTERS_FILE.to_string(), allowed_bytes),
            (APPROVER_KEY_FILE.to_string(), key.spki_pem.clone()),
            // **NO `approval.json` SLOT — PLAT-14.3b closed this.** D3 W7 had
            // to write the standing envelope here as a placeholder because
            // `--approval` was mandatory and `load_startup_inputs` verified it
            // under `PAYLOAD_TYPE_APPROVAL`, which is exactly why no rehearsal
            // could execute: a correctly signed one was reported as a
            // TAMPERED APPROVAL. The standing document now REPLACES the
            // per-run approval, so a rehearsal bundle has five members and the
            // contract pins no approval digest for it.
        ]
        .into_iter()
        // A rehearsal's plan ALWAYS binds its point, so it always carries the
        // evidence keyring too (D3 §5.5 step 6).
        .chain(evidence_member(restore, trust)?)
        .collect(),
    ))
}

/// [`standing_bundle_config_map`] for a `Restore` the RESTORE reconciler is
/// looking at — PLAT-14.3b.
///
/// The `RehearsalSchedule` reconciler already renders this object from the
/// same function, from `Authorization` values it has in hand. This is the same
/// render from the two objects the `Restore` reconciler has: the standing
/// `Approval` and the namespace's resolved trust. It stays one renderer —
/// [`standing_bundle_config_map`] — because the Job template's digests are
/// computed from whichever call produced the bytes, and two renderers would be
/// two answers to what the controller committed this run to.
///
/// # Errors
///
/// [`RestoreError::Materialization`] for an `Approval` with no UID or no
/// `matchedKeyId`, or whose signed bytes do not parse as a standing
/// authorization; whatever [`standing_bundle_config_map`] refuses.
fn standing_bundle_for(
    restore: &Restore,
    approval: &Approval,
    trust: &crate::trust::ResolvedTrust,
    now: DateTime<Utc>,
) -> Result<ConfigMap, RestoreError> {
    let approval_uid = approval
        .uid()
        .filter(|uid| !uid.trim().is_empty())
        .ok_or_else(|| {
            RestoreError::Materialization(format!(
                "the standing Approval {} carries no metadata.uid",
                approval.name_any()
            ))
        })?;
    let key_id = approval
        .status
        .as_ref()
        .and_then(|s| s.matched_key_id.as_deref())
        .ok_or_else(|| {
            RestoreError::Materialization(format!(
                "the standing Approval {} records no status.matchedKeyId",
                approval.name_any()
            ))
        })?;
    // The signed bytes are parsed only for the ALLOWLIST, which is exactly
    // `[scope.targetClusterId]` — W5's R1.3 obligation 1. `admit_standing`
    // has already proved the signature, the key and `plan ∈ scope` before
    // anything reaches here.
    let doc: logweir_core::execution_contract::StandingAuthorization =
        serde_json::from_str(&approval.spec.approval_bytes).map_err(|error| {
            RestoreError::Materialization(format!(
                "the standing Approval {}'s bytes are not a standing rehearsal authorization: \
                 {error}",
                approval.name_any()
            ))
        })?;
    let keyring = super::rehearsal_schedule::keyring(trust, now);
    standing_bundle_config_map(
        restore,
        &StandingInputs {
            envelope: &approval.spec.approval_bytes,
            sidecar: &approval.spec.sidecar_bytes,
            approval_uid: &approval_uid,
            key_id,
            keyring: &keyring,
            target_cluster_id: &doc.scope.target_cluster_id,
        },
        trust,
    )
}

/// The `RehearsalSchedule` UID a standing `Approval` was verified against —
/// PLAT-14.3b.
///
/// **`status.verifiedSubjectRef.uid`, AND NOT A NAME.** It is the only
/// RehearsalSchedule UID reachable from a `Restore` (which carries the
/// schedule's name), it is the value the `Approval` controller recorded for
/// the object it actually read, and it becomes
/// `LOGWEIR_EXECUTION_REHEARSAL_SCHEDULE_UID` — which the runner then requires
/// to equal the SIGNED document's own `subjectRef.uid`. A schedule deleted and
/// recreated under the same name has a new UID and a standing document that no
/// longer binds to it, which is the whole point of the field.
///
/// # Errors
///
/// [`RestoreError::Materialization`] for an absent or blank UID — the runner
/// treats a present-and-empty value as MISSING and refuses the whole contract,
/// so a default here would render a bundle every Job aborts on.
fn standing_schedule_uid(approval: &Approval) -> Result<String, RestoreError> {
    approval
        .status
        .as_ref()
        .and_then(|s| s.verified_subject_ref.as_ref())
        .map(|bound| bound.uid.trim().to_string())
        .filter(|uid| !uid.is_empty())
        .ok_or_else(|| {
            RestoreError::Materialization(format!(
                "the standing Approval {} carries no non-blank \
                 status.verifiedSubjectRef.uid, so the RehearsalSchedule this rehearsal claims \
                 to be cannot be bound to the signed document",
                approval.name_any()
            ))
        })
}

/// The execution contract v2 environment a STANDING-authorised Restore Job
/// carries — the thirteen mandatory names plus D3 §4.3's four.
///
/// # Every value is NON-BLANK, and that is the contract and not tidiness
///
/// The runner's own `required()` filters on `!value.trim().is_empty()` before
/// it decides a mandatory name is present, so a variable emitted
/// PRESENT-AND-BLANK is a variable the runner reports as MISSING, and the whole
/// contract is refused with `GuardRefusal: incomplete Restore execution
/// contract` before phase 0. An earlier revision of this function read
/// `APPROVAL_UID` out of the bundle's annotations with `.unwrap_or_default()`
/// while the standing bundle did not write that annotation at all: every
/// standing rehearsal Job would have aborted, and the twenty mutants planted
/// against this file did not catch it because the test asserted only that the
/// KEY was present. Both halves are now errors, at the bundle and here, and
/// `the_rendered_bundle_is_what_the_runner_loads` asserts non-blankness by the
/// same rule `required()` applies.
///
/// # The per-run approval digests are NOT emitted — PLAT-14.3b
///
/// A rehearsal bundle has no `approval.json` slot, so there is nothing for
/// `LOGWEIR_EXECUTION_APPROVAL_SHA256` / `…_SIDECAR_SHA256` to pin, and the
/// runner REFUSES a `standing` contract that sets either (a digest over a
/// member nothing verifies is worse than absence in both directions). The
/// mandatory set here is therefore
/// [`logweir_core::execution_contract::STANDING_MANDATORY_ENV`]'s eleven, not
/// `ALL_ENV`'s thirteen; `APPROVAL_NAME` and `APPROVAL_UID` stay, because they
/// name the STANDING `Approval`, which is a real object a human signed.
///
/// # `AUTHORIZATION_*_SHA256` are over the standing document's OWN members
///
/// `standing-authorization.json` and `standing-authorization.sig`, not
/// `approval.json`/`approval.sig` — see [`standing_bundle_config_map`] for why
/// the standing document has its own name. There is no per-run slot to pin:
/// see the section above.
///
/// `POLICY_SNAPSHOT_SHA256` and `CONFIRMATION_KEY_SHA256` are deliberately NOT
/// set: PLAT-19.2 owns both halves, and the runner refuses a mounted member
/// with no pinned digest AND a pinned digest with nothing mounted, so emitting
/// either without the file would refuse every rehearsal.
///
/// # Errors
///
/// As [`standing_bundle_config_map`], plus a missing bundle member and a bundle
/// that carries no `APPROVAL_UID` annotation.
pub fn standing_execution_contract_env(
    restore: &Restore,
    bundle: &ConfigMap,
    schedule_uid: &str,
) -> Result<Vec<(String, String)>, RestoreError> {
    use logweir_core::execution_contract as contract;

    let data = bundle.data.as_ref().ok_or_else(|| {
        RestoreError::Materialization("the rendered approval bundle has no data".to_string())
    })?;
    let get = |key: &str| {
        data.get(key).ok_or_else(|| {
            RestoreError::Materialization(format!(
                "the rendered approval bundle is missing public member {key}"
            ))
        })
    };
    let name = restore.name_any();
    let namespace = restore
        .namespace()
        .ok_or_else(|| RestoreError::NoNamespace(name.clone()))?;
    let restore_uid = restore
        .uid()
        .ok_or_else(|| RestoreError::NoUid(name.clone()))?;
    let authorization = restore.spec.authorization.as_ref().ok_or_else(|| {
        RestoreError::Materialization(format!(
            "the Restore {name} carries no spec.authorization, so it is not standing-authorised"
        ))
    })?;
    // THE UID THE BUNDLE COMMITTED TO, and an error rather than a default: see
    // this function's header for what a blank one costs.
    let approval_uid = bundle
        .metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get(BUNDLE_APPROVAL_UID_ANNOTATION))
        .map(String::as_str)
        .filter(|uid| !uid.trim().is_empty())
        .ok_or_else(|| {
            RestoreError::Materialization(format!(
                "the rendered approval bundle carries no non-blank \
                 {BUNDLE_APPROVAL_UID_ANNOTATION} annotation, and the runner treats a blank \
                 {} as missing",
                contract::APPROVAL_UID_ENV
            ))
        })?
        .to_string();
    let digest = |bytes: &[u8]| sha256_prefixed(bytes);
    let standing = digest(get(STANDING_AUTHORIZATION_FILE)?.as_bytes());
    let standing_sidecar = digest(get(STANDING_AUTHORIZATION_SIG_FILE)?.as_bytes());

    Ok(vec![
        (
            contract::VERSION_ENV.to_string(),
            contract::VERSION.to_string(),
        ),
        (
            contract::SUBJECT_API_VERSION_ENV.to_string(),
            Restore::api_version(&()).to_string(),
        ),
        (
            contract::SUBJECT_KIND_ENV.to_string(),
            Restore::kind(&()).to_string(),
        ),
        (contract::SUBJECT_NAME_ENV.to_string(), name),
        (contract::SUBJECT_NAMESPACE_ENV.to_string(), namespace),
        (contract::SUBJECT_UID_ENV.to_string(), restore_uid),
        (
            contract::APPROVAL_NAME_ENV.to_string(),
            authorization.approval_ref.name.clone(),
        ),
        (contract::APPROVAL_UID_ENV.to_string(), approval_uid),
        (
            contract::PLAN_SHA256_ENV.to_string(),
            digest(restore.spec.plan_bytes.as_bytes()),
        ),
        // **`APPROVAL_SHA256` / `APPROVAL_SIDECAR_SHA256` ARE NOT EMITTED** —
        // PLAT-14.3b. The runner refuses a contract that pins them while
        // `AUTHORIZATION_KIND` is `standing`, because a rehearsal bundle has
        // no approval slot and a digest over a member nothing verifies is
        // worse than either presence or absence. `APPROVAL_NAME`/`APPROVAL_UID`
        // stay: they name the STANDING Approval, which is a real object a
        // human signed and the one an auditor looks up.
        (
            contract::APPROVER_KEY_SHA256_ENV.to_string(),
            digest(get(APPROVER_KEY_FILE)?.as_bytes()),
        ),
        (
            contract::ALLOWED_CLUSTERS_SHA256_ENV.to_string(),
            digest(get(ALLOWED_CLUSTERS_FILE)?.as_bytes()),
        ),
        (
            contract::AUTHORIZATION_KIND_ENV.to_string(),
            contract::AUTHORIZATION_KIND_STANDING.to_string(),
        ),
        (contract::AUTHORIZATION_SHA256_ENV.to_string(), standing),
        (
            contract::AUTHORIZATION_SIDECAR_SHA256_ENV.to_string(),
            standing_sidecar,
        ),
        (
            contract::AUTHORIZATION_KEYS_SHA256_ENV.to_string(),
            digest(get(AUTHORIZATION_KEYS_FILE)?.as_bytes()),
        ),
        (
            contract::REHEARSAL_SCHEDULE_UID_ENV.to_string(),
            schedule_uid.to_string(),
        ),
    ]
    .into_iter()
    .chain(evidence_keys_env(data))
    .collect())
}

/// `LOGWEIR_EXECUTION_EVIDENCE_KEYS_SHA256`, exactly when the rendered bundle
/// carries [`EVIDENCE_KEYS_FILE`] -- computed from the bytes that were
/// rendered, like every other pinned member.
fn evidence_keys_env(data: &BTreeMap<String, String>) -> Option<(String, String)> {
    data.get(EVIDENCE_KEYS_FILE).map(|bytes| {
        (
            logweir_core::execution_contract::EVIDENCE_KEYS_SHA256_ENV.to_string(),
            sha256_prefixed(bytes.as_bytes()),
        )
    })
}

/// Environment contract pinned into every newly rendered Restore Job.
/// Projected ConfigMaps are name-bound and may be delete/recreated; these
/// values live in the immutable Job template and let the runner reject any
/// replacement before it constructs Kafka, store, or engine clients.
pub fn execution_contract_env(
    restore: &Restore,
    approval: &Approval,
    trust: &crate::trust::ResolvedTrust,
    now: DateTime<Utc>,
) -> Result<Vec<(String, String)>, RestoreError> {
    execution_contract_env_with_policy(restore, approval, trust, &EffectivePolicy::Legacy, now)
}

/// [`execution_contract_env`], under the namespace's approval-policy binding —
/// PLAT-19.2. A v2 bundle's two extra members are pinned too:
/// `POLICY_SNAPSHOT_SHA256_ENV` and `CONFIRMATION_KEY_SHA256_ENV`, the two
/// variables execution contract v2 reserved for exactly this.
///
/// # Errors
///
/// As [`execution_contract_env`].
pub fn execution_contract_env_with_policy(
    restore: &Restore,
    approval: &Approval,
    trust: &crate::trust::ResolvedTrust,
    policy: &EffectivePolicy,
    now: DateTime<Utc>,
) -> Result<Vec<(String, String)>, RestoreError> {
    use logweir_core::execution_contract as contract;

    // **ONE ENTRY POINT, TWO ARMS** — PLAT-14.3b. The Job template's digests
    // must be computed from whichever bundle was actually rendered, so the
    // choice is made here rather than at the three call sites that would each
    // have had to remember it.
    if restore.spec.authorization.is_some() {
        let schedule_uid = standing_schedule_uid(approval)?;
        let bundle = standing_bundle_for(restore, approval, trust, now)?;
        return standing_execution_contract_env(restore, &bundle, &schedule_uid);
    }

    let bundle = approval_bundle_config_map_with_policy(restore, approval, trust, policy, now)?;
    let data = bundle.data.as_ref().ok_or_else(|| {
        RestoreError::Materialization("the rendered approval bundle has no data".to_string())
    })?;
    let get = |key: &str| {
        data.get(key).ok_or_else(|| {
            RestoreError::Materialization(format!(
                "the rendered approval bundle is missing public member {key}"
            ))
        })
    };
    let namespace = restore
        .namespace()
        .ok_or_else(|| RestoreError::NoNamespace(restore.name_any()))?;
    let restore_uid = restore
        .uid()
        .ok_or_else(|| RestoreError::NoUid(restore.name_any()))?;
    let approval_uid = approval.uid().ok_or_else(|| {
        RestoreError::Materialization(format!(
            "the verified Approval {} carries no metadata.uid",
            approval.name_any()
        ))
    })?;
    let digest = |bytes: &[u8]| sha256_prefixed(bytes);

    Ok(vec![
        (
            contract::VERSION_ENV.to_string(),
            contract::VERSION.to_string(),
        ),
        (
            contract::SUBJECT_API_VERSION_ENV.to_string(),
            Restore::api_version(&()).to_string(),
        ),
        (
            contract::SUBJECT_KIND_ENV.to_string(),
            Restore::kind(&()).to_string(),
        ),
        (contract::SUBJECT_NAME_ENV.to_string(), restore.name_any()),
        (contract::SUBJECT_NAMESPACE_ENV.to_string(), namespace),
        (contract::SUBJECT_UID_ENV.to_string(), restore_uid),
        (contract::APPROVAL_NAME_ENV.to_string(), approval.name_any()),
        (contract::APPROVAL_UID_ENV.to_string(), approval_uid),
        (
            contract::PLAN_SHA256_ENV.to_string(),
            digest(restore.spec.plan_bytes.as_bytes()),
        ),
        (
            contract::APPROVAL_SHA256_ENV.to_string(),
            digest(get(APPROVAL_DOC_FILE)?.as_bytes()),
        ),
        (
            contract::APPROVAL_SIDECAR_SHA256_ENV.to_string(),
            digest(get(APPROVAL_SIG_FILE)?.as_bytes()),
        ),
        (
            contract::APPROVER_KEY_SHA256_ENV.to_string(),
            digest(get(APPROVER_KEY_FILE)?.as_bytes()),
        ),
        (
            contract::ALLOWED_CLUSTERS_SHA256_ENV.to_string(),
            digest(get(ALLOWED_CLUSTERS_FILE)?.as_bytes()),
        ),
    ]
    .into_iter()
    .chain(evidence_keys_env(data))
    .chain(
        // PLAT-19.2: PRESENT EXACTLY WHEN THE BUNDLE CARRIES THE MEMBERS, so a
        // v1 bundle's environment is byte-for-byte what it was.
        [
            (contract::POLICY_SNAPSHOT_SHA256_ENV, APPROVAL_POLICY_FILE),
            (contract::CONFIRMATION_KEY_SHA256_ENV, CONFIRMATION_KEY_FILE),
        ]
        .into_iter()
        .filter_map(|(env, file)| {
            data.get(file)
                .map(|bytes| (env.to_string(), digest(bytes.as_bytes())))
        }),
    )
    .collect())
}

/// Whether an `AlreadyExists` object is the exact bundle this reconcile
/// intended. Ownership without byte equality is never accepted.
#[must_use]
pub fn compatible_approval_bundle(existing: &ConfigMap, desired: &ConfigMap, uid: &str) -> bool {
    complete_restore_owner(&existing.metadata, desired, uid)
        && existing.immutable == Some(true)
        && existing.data == desired.data
        && desired_annotations_match(existing, desired)
}

fn desired_annotations_match(existing: &ConfigMap, desired: &ConfigMap) -> bool {
    let existing = existing.metadata.annotations.as_ref();
    desired.metadata.annotations.as_ref().is_some_and(|wanted| {
        wanted.iter().all(|(key, value)| {
            existing.and_then(|annotations| annotations.get(key)) == Some(value)
        })
    })
}

/// The exact single controller owner contract for a Restore-owned object.
/// A second owner can retain the object after Restore deletion, so even a
/// complete expected owner is insufficient unless it is the only owner.
#[must_use]
pub fn has_complete_restore_owner(metadata: &ObjectMeta, restore: &Restore) -> bool {
    let Some(uid) = restore.uid() else {
        return false;
    };
    has_exact_restore_owner_set(metadata, &restore.name_any(), &uid)
}

fn has_exact_restore_owner_set(metadata: &ObjectMeta, restore_name: &str, uid: &str) -> bool {
    let owners = metadata.owner_references.as_deref().unwrap_or_default();
    owners.len() == 1
        && owners.first().is_some_and(|owner| {
            owner.api_version == Restore::api_version(&())
                && owner.kind == Restore::kind(&())
                && owner.name == restore_name
                && owner.uid == uid
                && owner.controller == Some(true)
                && owner.block_owner_deletion == Some(true)
        })
}

/// A same-named Job is observable only when it is controlled by this exact
/// Restore incarnation. Its volume shape may be legacy, but its identity may
/// never be inferred from the name alone.
#[must_use]
pub fn compatible_restore_job(job: &Job, restore: &Restore) -> bool {
    job.metadata.name.as_deref() == Some(restore.name_any().as_str())
        && job.metadata.namespace == restore.namespace()
        && has_complete_restore_owner(&job.metadata, restore)
}

fn complete_restore_owner(metadata: &ObjectMeta, desired: &ConfigMap, uid: &str) -> bool {
    let Some(restore_name) = desired
        .metadata
        .owner_references
        .as_deref()
        .and_then(|owners| owners.first())
        .map(|owner| owner.name.as_str())
    else {
        return false;
    };
    metadata.name == desired.metadata.name
        && metadata.namespace == desired.metadata.namespace
        && has_exact_restore_owner_set(metadata, restore_name, uid)
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
    // **A REHEARSAL'S REASON IS A SLOT, NOT AN APPROVAL** — PLAT-14.3b. One
    // standing document covers every slot of one schedule, so
    // `approval/<standing approval>` would read identically on every run the
    // schedule ever makes and neither the schedule nor the slot would appear
    // anywhere in the signed evidence. The runner re-derives the same shape
    // and binds the `<schedule>` segment to the SIGNED `subjectRef.name`, so a
    // value invented here cannot survive the run.
    if restore.spec.authorization.is_some() {
        return match rehearsal_schedule_and_slot(restore) {
            Some((schedule, slot)) => {
                logweir_core::execution_contract::rehearsal_triggered_by(&schedule, &slot)
            }
            // Unreachable through `reconcile_restore` — `admit` refuses a
            // rehearsal naming no schedule before any argv is built — and
            // named rather than defaulted, because a `rehearsal//` value would
            // be refused by the runner with a message about a malformed
            // trigger instead of about the object that is actually wrong.
            None => logweir_core::execution_contract::rehearsal_triggered_by("<none>", "<none>"),
        };
    }
    let referent = restore.spec.approval_ref_name().trim();
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
    let standing = restore.spec.authorization.is_some();
    let mut argv: Vec<String> = vec![
        "restore".to_string(),
        "run".to_string(),
        logweir_core::execution_contract::VERSION_ARG.to_string(),
        logweir_core::execution_contract::VERSION.to_string(),
        "--spec".to_string(),
        format!("{PLAN_MOUNT_PATH}/{PLAN_SPEC_KEY}"),
    ];
    // **THE STANDING DOCUMENT REPLACES `--approval`** — PLAT-14.3b. Passing
    // both would be the pre-14.3b "sits beside" shape the runner now refuses
    // by name, and there is no per-run approval in a rehearsal bundle to point
    // at in any case. The sidecar is NOT a flag: the runner derives it from
    // this path by replacing the extension, exactly as it does for
    // `--approval`, so the two files cannot be mismatched.
    if standing {
        argv.extend([
            "--standing-authorization".to_string(),
            format!("{APPROVAL_MOUNT_PATH}/{STANDING_AUTHORIZATION_FILE}"),
            "--authorization-keys".to_string(),
            format!("{APPROVAL_MOUNT_PATH}/{AUTHORIZATION_KEYS_FILE}"),
        ]);
    } else {
        argv.extend([
            "--approval".to_string(),
            format!("{APPROVAL_MOUNT_PATH}/{APPROVAL_DOC_FILE}"),
        ]);
    }
    argv.extend([
        "--approver-key".to_string(),
        format!("{APPROVAL_MOUNT_PATH}/{APPROVER_KEY_FILE}"),
    ]);
    for id in approver_key_ids {
        argv.push("--approver-key-ids".to_string());
        argv.push(id.clone());
    }
    // D3 §5.5 step 6: a point-bound plan's receipt signature is verified
    // against the mounted evidence keyring before any client exists.
    if plan_binds_point(restore) {
        argv.extend([
            "--evidence-keys".to_string(),
            format!("{APPROVAL_MOUNT_PATH}/{EVIDENCE_KEYS_FILE}"),
        ]);
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
/// [`RestoreError`] when the object carries no namespace or UID, and
/// [`RestoreError::Refused`] when the target connection does not resolve or the
/// approved plan names a different target than it (PLAT-07.1,
/// [`crate::connection`]) — before any ConfigMap or Job exists.
pub fn runner_job_spec(
    restore: &Restore,
    cluster: &KafkaCluster,
    approver_key_ids: &[String],
    approval: &Approval,
    trust: &crate::trust::ResolvedTrust,
    now: DateTime<Utc>,
) -> Result<RunnerJobSpec, RestoreError> {
    runner_job_spec_with_destinations(
        restore,
        cluster,
        approver_key_ids,
        approval,
        trust,
        now,
        None,
    )
}

/// [`runner_job_spec`] for a destination-backed `Restore` — D2 §3.5's Restore
/// paragraph.
///
/// # THE TWO STORES, AND WHY NEITHER SHADOWS THE OTHER
///
/// The engine reads the archive through `AWS_*`; that is the SOURCE
/// destination's `archiveRead` grant, rendered by
/// [`ResolvedDestination::job_env`] exactly as a backup's is. The scorecard is
/// written through the EVIDENCE destination's `evidenceWrite` grant, and when
/// that is a different Secret it arrives under `LOGWEIR_EVIDENCE_AWS_*` —
/// separately named on purpose, because one `AWS_ACCESS_KEY_ID` in a pod is one
/// credential and a restore may legitimately need two.
/// [`ResolvedDestination::evidence_env`] owns the three cases and the one
/// refusal; nothing here re-decides them.
///
/// # `archive_addressing_env` IS NOT FORWARDED HERE
///
/// Defect **SEC-ENVHTTP**: the controller's own `AWS_ENDPOINT_URL`,
/// `AWS_REGION`, `AWS_ALLOW_HTTP` and `AWS_VIRTUAL_HOSTED_STYLE_REQUEST` reach
/// every legacy runner Job, and the engine's `from_env()` honours them over the
/// approved plan. On this path the complete explicit set comes from the two
/// destinations and the process contributes nothing — a planted
/// `AWS_ALLOW_HTTP=true` cannot enable plaintext transport for a run whose
/// destination declares TLS. The legacy arm keeps forwarding, deliberately:
/// see [`desired_execution_inputs_for_destination`]'s note on the upgrade.
///
/// # Errors
///
/// Whatever [`runner_job_spec`] refuses, plus
/// [`logweir_core::check_contract::CheckCode::ExecutionContextConflict`] when
/// the two grants cannot share one pod.
#[allow(clippy::too_many_arguments)]
pub fn runner_job_spec_with_destinations(
    restore: &Restore,
    cluster: &KafkaCluster,
    approver_key_ids: &[String],
    approval: &Approval,
    trust: &crate::trust::ResolvedTrust,
    now: DateTime<Utc>,
    destinations: Option<&RestoreDestinations>,
) -> Result<RunnerJobSpec, RestoreError> {
    runner_job_spec_with_policy(
        restore,
        cluster,
        approver_key_ids,
        approval,
        trust,
        &EffectivePolicy::Legacy,
        now,
        destinations,
    )
}

/// Whether the Job this Restore gets carries the v2 authorization members —
/// an authorization document v2 under an explicit binding. The ONE predicate
/// the argv, the mount table and the environment all read, so the three can
/// never disagree about which bundle the runner was handed.
#[must_use]
pub fn carries_authorization_v2(
    restore: &Restore,
    approval: &Approval,
    policy: &EffectivePolicy,
) -> bool {
    restore.spec.authorization.is_none()
        && policy.bound().is_some()
        && is_authorization_v2(approval)
}

/// [`runner_job_spec_with_destinations`], under the namespace's approval-policy
/// binding — PLAT-19.2. A v2-authorised Restore's Job mounts the two extra
/// bundle members and passes [`APPROVAL_POLICY_ARG`] and
/// [`CONFIRMATION_KEY_ARG`], and its environment pins both digests; nothing
/// else about the Job changes.
///
/// # Errors
///
/// As [`runner_job_spec_with_destinations`].
#[allow(clippy::too_many_arguments)]
pub fn runner_job_spec_with_policy(
    restore: &Restore,
    cluster: &KafkaCluster,
    approver_key_ids: &[String],
    approval: &Approval,
    trust: &crate::trust::ResolvedTrust,
    policy: &EffectivePolicy,
    now: DateTime<Utc>,
    destinations: Option<&RestoreDestinations>,
) -> Result<RunnerJobSpec, RestoreError> {
    let v2 = carries_authorization_v2(restore, approval, policy);
    let name = restore.name_any();
    let namespace = restore
        .namespace()
        .ok_or_else(|| RestoreError::NoNamespace(name.clone()))?;
    let uid = restore
        .uid()
        .ok_or_else(|| RestoreError::NoUid(name.clone()))?;

    // THE TARGET CONNECTION, FROM THE ONE RESOLVER THE PROBE AND THE BACKUP USE
    // (PLAT-07.1). The runner dials `planBytes`' target with THIS connection's
    // password and CA, so the approved plan must name the same target; a
    // mismatch is refused here, which is before `write_plan_config_map` and the
    // Job `POST` in `reconcile_restore`.
    let connection =
        crate::connection::resolve(cluster, crate::connection::ConnectionUse::RestoreTarget)?;
    connection.check_job_namespace(&namespace)?;
    connection.check_restore_plan(&restore.spec.plan_bytes)?;
    let projection = connection.project();

    // The private signing key remains the only file-backed Secret. The
    // approval inputs are public and come from the immutable ConfigMap this
    // Restore owns.
    //
    // A key is a FILE the runner opens by path: putting it in an env var would
    // expose it through `kubectl describe pod` to anyone with pod read.
    let mut secret_mounts = vec![SecretMount {
        volume: SIGNING_VOLUME.to_string(),
        secret_name: SIGNING_KEY_SECRET.to_string(),
        mount_path: SIGNING_MOUNT_PATH.to_string(),
        items: vec![(
            SIGNING_KEY_SECRET_KEY.to_string(),
            SIGNING_KEY_FILE.to_string(),
        )],
    }];
    secret_mounts.extend(projection.secret_mounts);
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
    // it writes into a pod spec and a value it cannot see. The VALUE is still
    // validated by the RUNNER (exit 3, `CredentialNotRenderable`), interface
    // I11's division of labour; what the resolver refuses up front is a
    // `scramSha512` connection with no reference at all.
    env_from_secret.extend(projection.env_from_secret);

    // === THE TWO DESTINATIONS' CONTRIBUTION (D2 §3.5) ===
    //
    // `evidence_env` is called with the SOURCE resolution beside it, because
    // whether the evidence grant needs its own projected credential at all is a
    // question about the PAIR: the same Secret and keys means
    // `LOGWEIR_EVIDENCE_CREDENTIALS=archive` and nothing further is projected.
    let (archive_env, evidence_env) = match destinations {
        None => (None, None),
        Some(pair) => {
            let evidence = pair
                .evidence
                .evidence_env(&pair.source)
                .map_err(|refusal| RestoreError::Refused(refusal.reason(), refusal.message))?;
            (Some(pair.source.job_env()), Some(evidence))
        }
    };
    for env in [archive_env.as_ref(), evidence_env.as_ref()]
        .into_iter()
        .flatten()
    {
        env_from_secret.extend(env.from_secret.iter().cloned());
    }
    // ONE POD, ONE ServiceAccount. A workload-identity grant REPLACES the
    // connection's runner ServiceAccount — the object store authenticates the
    // pod's identity, and a pod has exactly one. `evidence_env` has already
    // refused two DIFFERENT workload identities, so the two values here agree
    // whenever both are present.
    let service_account_name = archive_env
        .as_ref()
        .and_then(|env| env.service_account_name.clone())
        .or_else(|| {
            evidence_env
                .as_ref()
                .and_then(|env| env.service_account_name.clone())
        })
        .unwrap_or_else(|| connection.execution.service_account_name.clone());

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
        args: {
            let mut argv = runner_argv(restore, approver_key_ids);
            // PLAT-19.2: THE TWO v2 MEMBERS, BY FLAG. An old runner image
            // handed these fails to parse them and exits before it dispatches
            // — the version-skew refusal the destination flag below relies on
            // too — rather than verifying a v2 document as if it were v1.
            if v2 {
                argv.extend([
                    APPROVAL_POLICY_ARG.to_string(),
                    format!("{APPROVAL_MOUNT_PATH}/{APPROVAL_POLICY_FILE}"),
                    CONFIRMATION_KEY_ARG.to_string(),
                    format!("{APPROVAL_MOUNT_PATH}/{CONFIRMATION_KEY_FILE}"),
                ]);
            }
            // THE VERSION-SKEW HANDSHAKE (D2 §3.5). An old runner image handed
            // a destination-backed Job fails to parse this flag and exits
            // before it dispatches, rather than building its stores out of
            // whatever `AWS_*` happens to be in the pod.
            if destinations.is_some() {
                argv.push(destination::STORE_CONTRACT_VERSION_ARG.to_string());
                argv.push(destination::STORE_CONTRACT_VERSION.to_string());
            }
            argv
        },
        deadline_seconds: restore.spec.deadline_seconds,
        service_account_name,
        secret_mounts,
        config_map_mounts: {
            let mut mounts = vec![ConfigMapMount {
                volume: APPROVAL_VOLUME.to_string(),
                config_map_name: approval_bundle_config_map_name(&restore.name_any()),
                mount_path: APPROVAL_MOUNT_PATH.to_string(),
                // **THE FILE TABLE, AND THE STANDING DOCUMENT KEEPS ITS OWN
                // NAME** — PLAT-14.3b, and see `standing_bundle_config_map`
                // for why. `standing-authorization.sig` is projected under its
                // own name and NEVER under `approval.sig`: the runner verifies
                // `approval.json` under `PAYLOAD_TYPE_APPROVAL`, so a standing
                // sidecar in that slot makes a correctly signed rehearsal look
                // like a substituted approval. A rehearsal bundle carries no
                // approval slot at all, so neither per-run member is projected.
                items: {
                    let mut items = if restore.spec.authorization.is_some() {
                        vec![
                            (
                                STANDING_AUTHORIZATION_FILE.to_string(),
                                STANDING_AUTHORIZATION_FILE.to_string(),
                            ),
                            (
                                STANDING_AUTHORIZATION_SIG_FILE.to_string(),
                                STANDING_AUTHORIZATION_SIG_FILE.to_string(),
                            ),
                            (
                                AUTHORIZATION_KEYS_FILE.to_string(),
                                AUTHORIZATION_KEYS_FILE.to_string(),
                            ),
                            (APPROVER_KEY_FILE.to_string(), APPROVER_KEY_FILE.to_string()),
                            (
                                ALLOWED_CLUSTERS_FILE.to_string(),
                                ALLOWED_CLUSTERS_FILE.to_string(),
                            ),
                        ]
                    } else {
                        let mut items = vec![
                            (APPROVAL_DOC_FILE.to_string(), APPROVAL_DOC_FILE.to_string()),
                            (APPROVAL_SIG_FILE.to_string(), APPROVAL_SIG_FILE.to_string()),
                            (APPROVER_KEY_FILE.to_string(), APPROVER_KEY_FILE.to_string()),
                            (
                                ALLOWED_CLUSTERS_FILE.to_string(),
                                ALLOWED_CLUSTERS_FILE.to_string(),
                            ),
                        ];
                        // PLAT-19.2: the frozen policy snapshot and the
                        // console key, exactly when the bundle carries them.
                        if v2 {
                            items.push((
                                APPROVAL_POLICY_FILE.to_string(),
                                APPROVAL_POLICY_FILE.to_string(),
                            ));
                            items.push((
                                CONFIRMATION_KEY_FILE.to_string(),
                                CONFIRMATION_KEY_FILE.to_string(),
                            ));
                        }
                        items
                    };
                    // PLAT-15.2: the evidence keyring, exactly when the plan
                    // binds a recovery point.
                    if plan_binds_point(restore) {
                        items.push((
                            EVIDENCE_KEYS_FILE.to_string(),
                            EVIDENCE_KEYS_FILE.to_string(),
                        ));
                    }
                    items
                },
            }];
            mounts.extend(projection.config_map_mounts);
            mounts
        },
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
            match (archive_env.as_ref(), evidence_env.as_ref()) {
                // THE COMPLETE EXPLICIT SET, AND NOTHING FROM THIS PROCESS.
                (Some(archive), Some(evidence)) => {
                    env.extend(archive.literals.iter().cloned());
                    env.extend(evidence.literals.iter().cloned());
                }
                // The legacy inline-`sourceArchive` path, byte for byte as it
                // was: the four forwarded variables, or none of them on a
                // default install that sets none.
                _ => env.extend(backup::archive_addressing_env()),
            }
            env.extend(execution_contract_env_with_policy(
                restore, approval, trust, policy, now,
            )?);
            env.extend(projection.env_literal);
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

/// Patch `/status` — under seam **S7**'s `metadata.resourceVersion`
/// precondition, and not at all when the patch would change nothing.
///
/// Both decisions are [`crate::conditions::patch_status_preconditioned`]'s;
/// this exists so this reconciler's nine patch sites read as one line each and
/// neither rule can be applied at eight of them and forgotten at the ninth.
/// Defect STATUS-PATCH-NO-RV is what the forgotten one looks like: every write
/// here was unconditional while the chart's own README said all of them were
/// preconditioned.
///
/// THE PRECONDITION IS THE OBJECT THIS PASS OBSERVED. A `Restore` whose
/// terminal patch is followed by the verification patch writes twice in one
/// pass, and the second write is where the observed version is already stale —
/// [`patch_status_at`] is that caller's opt-in, by name.
async fn patch_status_if_changed(
    api: &Api<Restore>,
    restore: &Restore,
    name: &str,
    patch: Value,
) -> Result<StatusVersion, RestoreError> {
    patch_status_at(
        api,
        restore,
        name,
        &StatusVersion::observed(restore.meta()),
        patch,
    )
    .await
}

/// [`patch_status_if_changed`] for a write that is NOT the first of its pass.
///
/// `at` is where the previous write of this pass left the object, so the
/// compare-and-set is against what this controller itself stored a moment ago
/// rather than against the version the watch delivered — which the first write
/// has already superseded. Without it the second patch of every terminal pass
/// would be refused with a `409` the reconciler cannot retry: the object is
/// terminal by then, `AwaitChange` is its requeue, and STEP 2b reads and
/// writes nothing, so the evidence verdict would be lost for good.
async fn patch_status_at(
    api: &Api<Restore>,
    restore: &Restore,
    name: &str,
    at: &StatusVersion,
    patch: Value,
) -> Result<StatusVersion, RestoreError> {
    crate::conditions::patch_status_preconditioned(
        api,
        "Restore",
        name,
        at,
        restore
            .status
            .as_ref()
            .and_then(|s| serde_json::to_value(s).ok())
            .as_ref(),
        patch,
    )
    .await
    .map_err(RestoreError::Api)
}

/// `restore` as it stands AFTER `patch` was stored at `at` — the projection a
/// second write of one pass builds from and preconditions on (seam **S7**).
fn with_status_written(restore: &Restore, patch: &Value, at: &StatusVersion) -> Restore {
    let mut next = restore.clone();
    if let Some(fragment) = patch.get("status") {
        let mut status = restore
            .status
            .as_ref()
            .and_then(|s| serde_json::to_value(s).ok())
            .unwrap_or(Value::Null);
        crate::conditions::apply_merge_patch(&mut status, fragment);
        if let Ok(projected) = serde_json::from_value(status) {
            next.status = Some(projected);
        }
    }
    if let Some(version) = at.get() {
        next.metadata.resource_version = Some(version.to_string());
    }
    next
}

// ---------------------------------------------------------------------------
// The evidence-fetch Job's half — D2 §3.9's Restore paragraph
// ---------------------------------------------------------------------------

/// The `Restore` this Job and plan belong to, as an owner reference.
fn restore_owner(restore: &Restore) -> Option<RunnerOwner> {
    Some(RunnerOwner {
        api_version: Restore::api_version(&()).to_string(),
        kind: Restore::kind(&()).to_string(),
        name: restore.name_any(),
        uid: restore.uid()?,
    })
}

/// Write one evidence-fetch verdict for a `Restore`; see
/// [`crate::evidence_fetch::verdict_patch`].
#[allow(clippy::too_many_arguments)]
async fn write_fetch_verdict(
    restores: &Api<Restore>,
    restore: &Restore,
    name: &str,
    at: &StatusVersion,
    result: &crate::verification::VerificationResult,
    observation: Value,
    evidence_facts: serde_json::Map<String, Value>,
    facts: serde_json::Map<String, Value>,
    now: DateTime<Utc>,
) -> Result<StatusVersion, RestoreError> {
    let current = restore
        .status
        .as_ref()
        .and_then(|s| serde_json::to_value(s).ok());
    let (patch, badge) = crate::evidence_fetch::verdict_patch(
        current.as_ref(),
        restore
            .status
            .as_ref()
            .and_then(|s| s.conditions.as_deref())
            .unwrap_or_default(),
        restore.meta().generation,
        result,
        observation,
        evidence_facts,
        facts,
        restore_badge,
        now,
    );
    info!(
        restore = %name,
        verification = %result.result,
        matched_key_id = result.matched_key_id.as_deref().unwrap_or("<none>"),
        green = badge.green,
        "weirkeeper recorded this Restore's evidence verdict"
    );
    patch_status_at(restores, restore, name, at, patch).await
}

/// One pass over this run's evidence-fetch Job — the `Restore` twin of
/// `controllers::backup`'s, on `scorecard-key` / `sidecar-key` and the
/// EVIDENCE destination's `evidenceRead` grant (D2 §3.9, Restore paragraph).
///
/// # What differs from the `Backup` half, and why
///
/// * **The digest.** A restore runner reports no scorecard digest of its own,
///   so — exactly as the controller's own-handle path does — the digest is
///   computed HERE over the relayed bytes and is what the signature is
///   checked against; `scorecardSha256` records it.
/// * **The binding.** The runner writes its scorecard at
///   `logweir/drills/<run_id>.json` and prints that key; the relayed
///   document's own `run_id` must be the one the key names. Another run's
///   scorecard copied to this key — however validly signed — is `Invalid` and
///   projects nothing.
/// * **The facts.** `outcome`, `lastPhaseCompleted`, `objectives`,
///   `integrity`, `measured` and the offset report's digest are copied out of
///   the relayed bytes by JSON pointer (`scorecard_observation`, the one
///   reader) — the same facts, the same rule the own-handle path applies on
///   its terminal patch: whenever the document was read and is bound to this
///   run, whatever the signature verdict. The badge is still green only on
///   `Valid` with `outcome: pass`.
#[allow(clippy::too_many_arguments)]
async fn evidence_fetch_pass(
    restores: &Api<Restore>,
    restore: &Restore,
    at: &StatusVersion,
    client: &kube::Client,
    namespace: &str,
    name: &str,
    attempt: u32,
    source: &backup::EvidenceSource,
    now: DateTime<Utc>,
    runner: &job::RunnerImage,
) -> Result<(), RestoreError> {
    use crate::evidence_fetch::{observation_patch, Relayed, Step};
    use crate::verification::VerificationResult;
    let payload_type = logweir_verify::PAYLOAD_TYPE_SCORECARD;
    let status = restore.status.as_ref();
    let evidence = status.and_then(|s| s.evidence.as_ref());
    let (Some(payload_key), Some(sidecar_key)) = (
        evidence.and_then(|e| e.scorecard_key.clone()),
        evidence.and_then(|e| e.sidecar_key.clone()),
    ) else {
        return Ok(());
    };
    let Some(owner) = restore_owner(restore) else {
        return Ok(());
    };
    let stored_mode = evidence
        .and_then(|e| e.observation.as_ref())
        .and_then(|o| o.mode.clone());
    let request = crate::evidence_fetch::Request {
        payload_key: payload_key.clone(),
        sidecar_key: sidecar_key.clone(),
    };
    let (destination, checks, policy_digest, unresolved) = source.fetch_inputs();
    let mode = destination
        .and_then(|d| crate::evidence_fetch::grant_mode(&d.grant))
        .map(str::to_string)
        .or(stored_mode);
    let step = crate::evidence_fetch::advance(&crate::evidence_fetch::Inputs {
        client,
        namespace,
        owner: &owner,
        attempt,
        request: &request,
        destination,
        unresolved: unresolved.as_deref(),
        checks: &checks,
        policy_digest,
        image: runner,
        now,
    })
    .await
    .map_err(RestoreError::Api)?;

    let none = serde_json::Map::new;
    match step {
        Step::Queued { detail } => {
            write_fetch_verdict(
                restores,
                restore,
                name,
                at,
                &VerificationResult::pending(payload_type, detail),
                observation_patch(mode.as_deref(), None, attempt, None, None),
                none(),
                none(),
                now,
            )
            .await?;
        }
        Step::Running { job_ref, detail } => {
            write_fetch_verdict(
                restores,
                restore,
                name,
                at,
                &VerificationResult::pending(payload_type, detail),
                observation_patch(mode.as_deref(), Some(&job_ref), attempt, None, None),
                none(),
                none(),
                now,
            )
            .await?;
        }
        Step::Refused { detail } => {
            write_fetch_verdict(
                restores,
                restore,
                name,
                at,
                &VerificationResult::not_attempted(payload_type, detail),
                observation_patch(mode.as_deref(), None, attempt, None, None),
                none(),
                none(),
                now,
            )
            .await?;
        }
        Step::Failed {
            job_name,
            job_uid,
            detail,
        } => {
            let retry = crate::evidence_fetch::retry_after(attempt, now);
            let job_ref = crate::crds::ObservedJobRef {
                name: Some(job_name.clone()),
                uid: job_uid,
            };
            write_fetch_verdict(
                restores,
                restore,
                name,
                at,
                &VerificationResult::not_attempted(
                    payload_type,
                    crate::evidence_fetch::failed_detail(&detail, attempt, retry),
                ),
                observation_patch(mode.as_deref(), Some(&job_ref), attempt, None, retry),
                none(),
                none(),
                now,
            )
            .await?;
            check::set_ttl(client, namespace, &job_name)
                .await
                .map_err(RestoreError::Api)?;
        }
        Step::Relayed {
            job_name,
            job_uid,
            presence,
            relayed,
        } => {
            let job_ref = crate::crds::ObservedJobRef {
                name: Some(job_name.clone()),
                uid: job_uid,
            };
            let mut evidence_facts = serde_json::Map::new();
            let mut facts = serde_json::Map::new();
            let result = match relayed {
                Relayed::Unread { detail } => {
                    VerificationResult::not_attempted(payload_type, detail)
                }
                Relayed::Both { payload, sidecar } => {
                    let fetched = sha256_prefixed(&payload);
                    let claimed_run = serde_json::from_slice::<Value>(&payload)
                        .ok()
                        .and_then(|d| d.get("run_id").and_then(Value::as_str).map(str::to_string));
                    match claimed_run {
                        Some(run) if payload_key == format!("logweir/drills/{run}.json") => {
                            let reference = EvidenceRef {
                                namespace: namespace.to_string(),
                                payload_key: payload_key.clone(),
                                payload_sha256: fetched.clone(),
                                sidecar_key: sidecar_key.clone(),
                                payload_type,
                            };
                            let result = crate::verification::verify_relayed(
                                client, &reference, &payload, &sidecar,
                            )
                            .await;
                            if let Some(o) = scorecard_observation(&payload) {
                                evidence_facts
                                    .insert("scorecardSha256".to_string(), json!(fetched));
                                if let Some(d) = o.offset_report_sha256.as_ref() {
                                    evidence_facts
                                        .insert("offsetReportSha256".to_string(), json!(d));
                                }
                                if let Some(v) = o.outcome.as_ref() {
                                    facts.insert("outcome".to_string(), json!(v));
                                }
                                if let Some(v) = o.last_phase_completed {
                                    facts.insert("lastPhaseCompleted".to_string(), json!(v));
                                }
                                for (key, block) in [
                                    ("objectives", objectives_block(&o)),
                                    ("integrity", integrity_block(&o)),
                                    ("measured", measured_block(&o)),
                                ] {
                                    if !block.is_empty() {
                                        facts.insert(key.to_string(), Value::Object(block));
                                    }
                                }
                                if let Some(state) = window_not_covered(
                                    status.and_then(|s| s.exit_code).unwrap_or(0),
                                    o.outcome.as_deref(),
                                ) {
                                    facts.insert("exitReason".to_string(), json!(state));
                                }
                            }
                            result
                        }
                        // BOUND TO THIS RUN OR NOT READ AS IT. The runner
                        // writes a scorecard at `logweir/drills/<run_id>.json`;
                        // a document whose own `run_id` is not the one its key
                        // names is another run's scorecard, however validly
                        // signed, and nothing is copied out of it.
                        claimed => VerificationResult::invalid(
                            payload_type,
                            format!(
                                "the relayed scorecard at {payload_key} names run_id {}, which \
                                 is not the run its key names; it is not this run's scorecard",
                                claimed.as_deref().unwrap_or("<absent>")
                            ),
                        ),
                    }
                }
            };
            write_fetch_verdict(
                restores,
                restore,
                name,
                at,
                &result,
                observation_patch(
                    mode.as_deref(),
                    Some(&job_ref),
                    attempt,
                    Some(presence),
                    None,
                ),
                evidence_facts,
                facts,
                now,
            )
            .await?;
            check::set_ttl(client, namespace, &job_name)
                .await
                .map_err(RestoreError::Api)?;
        }
    }
    Ok(())
}

/// The fetch a TERMINAL `Restore` still owes, and when to look again.
///
/// Returns the requeue a terminal pass should use: `After` while a fetch is
/// pending or a retry is scheduled (a terminal `Restore` otherwise waits for a
/// change, and a queued fetch or a `+5 m` retry is not one), `AwaitChange`
/// once the verdict is reached.
async fn continue_evidence_fetch(
    restore: &Restore,
    client: &kube::Client,
    namespace: &str,
    now: DateTime<Utc>,
    runner: &job::RunnerImage,
) -> Result<Requeue, RestoreError> {
    let evidence = restore.status.as_ref().and_then(|s| s.evidence.as_ref());
    let result = evidence
        .and_then(|e| e.verification.as_ref())
        .and_then(|v| v.result.as_deref());
    let observation = evidence.and_then(|e| e.observation.as_ref());
    // THE CRASH WINDOW — see the `Backup` twin: a terminal pass that stopped
    // between the terminal patch and the recorded fetch.
    let unrecorded = result.is_none()
        && observation.is_none()
        && restore.spec.evidence_destination_ref.is_some()
        && evidence.is_some_and(|e| e.scorecard_key.is_some() && e.sidecar_key.is_some());
    let owed = crate::evidence_fetch::owed_attempt(result, observation, now);
    let waiting = match crate::evidence_fetch::retry_due_in(result, observation, now) {
        Some(secs) => Requeue::After(secs),
        None => Requeue::AwaitChange,
    };
    if owed.is_none() && !unrecorded {
        // THE VERDICT IS COMMITTED; ITS JOB'S TTL MAY NOT BE (review LOW-4).
        // A failed `set_ttl` on the committing pass returned an error, and the
        // error policy's requeue lands here.
        if let Some(owner) = restore_owner(restore) {
            crate::evidence_fetch::repair_ttl(
                client,
                namespace,
                &owner,
                result,
                evidence
                    .and_then(|e| e.verification.as_ref())
                    .and_then(|v| v.verified_at),
                observation,
                now,
            )
            .await
            .map_err(RestoreError::Api)?;
        }
        return Ok(waiting);
    }
    let source = backup::evidence_source_for(
        restore.spec.evidence_destination_ref.as_ref(),
        client,
        namespace,
        now,
    )
    .await
    .map_err(RestoreError::Api)?;
    let attempt = match (owed, &source) {
        (Some(attempt), _) => attempt,
        (None, backup::EvidenceSource::FetchJob { .. }) => 1,
        (None, _) => return Ok(waiting),
    };
    let restores: Api<Restore> = Api::namespaced(client.clone(), namespace);
    evidence_fetch_pass(
        &restores,
        restore,
        &StatusVersion::observed(restore.meta()),
        client,
        namespace,
        &restore.name_any(),
        attempt,
        &source,
        now,
        runner,
    )
    .await?;
    Ok(Requeue::After(REQUEUE_SECS))
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

/// [`admission_hold_patch`] for a destination that is absent or not yet
/// `Valid` — D2 §3.6 check 5.
///
/// The SAME SHAPE as the approval hold, on purpose: `phase: Pending`, the
/// scalar `reason` the `REASON` column renders, and exactly one
/// `Admitted=False` condition. Two hold shapes for two holds would make a
/// console render the same situation two ways, and the `reason` is the
/// resolver's own `CheckCode` — the same string the `BackupDestination`'s own
/// `Valid` condition carries, so the two objects agree about the fault.
#[must_use]
pub fn destination_hold_patch(
    restore: &Restore,
    reason: &str,
    message: &str,
    now: DateTime<Utc>,
) -> Value {
    json!({
        "status": {
            "phase": PHASE_PENDING,
            "reason": reason,
            "conditions": [condition(
                restore,
                CONDITION_ADMITTED,
                "False",
                reason,
                message,
                now,
            )],
        }
    })
}

/// A retryable status for failure to materialize controller-pinned execution
/// inputs. The detailed condition message names the plan, approval bundle, or
/// raced Job operation that will be retried.
#[must_use]
pub fn approval_bundle_hold_patch(restore: &Restore, message: &str, now: DateTime<Utc>) -> Value {
    json!({
        "status": {
            "phase": PHASE_PENDING,
            "reason": REASON_APPROVAL_BUNDLE_MATERIALIZATION_FAILED,
            "conditions": [condition(
                restore,
                CONDITION_APPROVAL_BUNDLE_READY,
                "False",
                REASON_APPROVAL_BUNDLE_MATERIALIZATION_FAILED,
                message,
                now,
            )],
        }
    })
}

/// The `/status` merge patch for a run this CONTROLLER refused terminally,
/// before any Job was created.
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
    let conditions = crate::conditions::carry_remaining(
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
    // THE ADMISSION CONDITION IS ASSERTED ON THE CREATING PASS ONLY, AND
    // CARRIED ON EVERY LATER ONE. A later pass over a running Job re-asserts
    // nothing about an approval it did not re-read — but a merge patch
    // REPLACES `status.conditions`, so leaving the type out of this array does
    // not leave the stored condition alone, it DELETES it.
    //
    // This comment used to say both things at once ("a merge patch that
    // omitted it would leave it alone anyway" beside "the array is replaced
    // wholesale"), and the code did the second: defect
    // RESTORE-ADMITTED-DROPPED, `Admitted=True` present after the pass that
    // created the Job and gone on the very next reconcile of an unchanged
    // running `Restore`. The carry is `diagnostics::apply`'s
    // `upsert_conditions`, which keeps every stored condition this patch is
    // not about — including this one, with its ORIGINAL `lastTransitionTime`,
    // because the stored element is carried verbatim.
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
    // AND EVERY STORED CONDITION THIS PATCH IS NOT ABOUT, HERE TOO — review
    // finding LOW-2. `diagnostics::apply` carries them for the RUNNING pass,
    // but the CREATING pass does not go through it (`restore.rs`'s step 1
    // writes this patch directly), and its `409 AlreadyExists`-adopt arm calls
    // this builder with `admitted: false` — an array of `[JobCreated]` alone,
    // which would delete a stored `Admitted`. Stored order, for
    // `upsert_conditions`' own reason: a running object is reconciled again.
    let conditions = crate::conditions::upsert_conditions(
        restore.status.as_ref().and_then(|s| s.conditions.as_ref()),
        conditions,
    );
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
    let conditions = crate::conditions::carry_remaining(
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
    let conditions = crate::conditions::carry_remaining(
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
    /// The verified public inputs could not be materialized. This is exposed
    /// as a Pending status and retried; no Job is created.
    Materialization(String),
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
            Self::Materialization(message) => write!(f, "{message}"),
            Self::Refused(state, message) => write!(f, "{state}: {message}"),
        }
    }
}

impl std::error::Error for RestoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::NoNamespace(_)
            | Self::NoUid(_)
            | Self::Materialization(_)
            | Self::Refused(..) => None,
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
    // **ONE `Approval` GET, and `spec.authorization` names it for a
    // rehearsal** — PLAT-14.3b. The CEL rule makes the two fields mutually
    // exclusive, so this is a choice between them and never a fallback: an
    // ordinary `Restore` whose `approvalRef` resolves to nothing must stay
    // unauthorized rather than pick up a standing document that happens to
    // exist in the namespace.
    let referent = standing_approval_name(restore)
        .unwrap_or_else(|| restore.spec.approval_ref_name().trim().to_string());
    if referent.is_empty() {
        return Ok(None);
    }
    let api: Api<Approval> = Api::namespaced(client.clone(), namespace);
    api.get_opt(&referent).await.map_err(RestoreError::Api)
}

/// The standing `Approval` this `Restore` names, or `None` for an ordinary
/// one — PLAT-14.3b.
///
/// One accessor, so the five functions that had to learn about
/// `spec.authorization` cannot each spell the same field access slightly
/// differently. A blank name is `Some("")` and not `None`: it is a rehearsal
/// naming nothing, which `admit` refuses terminally, and collapsing it to
/// `None` would send it down the ordinary approval path instead.
#[must_use]
pub fn standing_approval_name(restore: &Restore) -> Option<String> {
    restore
        .spec
        .authorization
        .as_ref()
        .map(|a| a.approval_ref.name.trim().to_string())
}

/// The `(schedule, slot)` a rehearsal `Restore` was created for, from the two
/// labels `RehearsalSchedule`'s `child_restore` sets — PLAT-14.3b.
///
/// **LABELS, BECAUSE THE SLOT IS NOWHERE ELSE.** `spec.authorization` names
/// the schedule but not the slot, and the slot is what distinguishes one run
/// of a schedule from the next in the signed scorecard's `triggered_by`. The
/// schedule is taken from `spec.authorization` rather than from its label,
/// because the spec is sealed by CEL and a label is not.
#[must_use]
fn rehearsal_schedule_and_slot(restore: &Restore) -> Option<(String, String)> {
    let schedule = restore
        .spec
        .authorization
        .as_ref()
        .map(|a| a.rehearsal_schedule_ref.name.clone())
        .filter(|name| !name.trim().is_empty())?;
    let slot = restore
        .labels()
        .get(crate::rehearsal::SLOT_LABEL)
        .map(|slot| slot.trim().to_string())
        .filter(|slot| !slot.is_empty())?;
    Some((schedule, slot))
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

/// Resolve **this namespace's** trust, for the approval bundle and the argv —
/// PLAT-19.1, D3 §7.1.
///
/// **AFTER the admission, deliberately.** An unapproved plan creates nothing
/// (Global Constraint 6) and it also READS nothing it does not need: this is a
/// materialization input, so it is fetched only once a Job is going to exist.
///
/// The two non-`Trust` resolutions are **holds, not terminal refusals**, and
/// the distinction is the point: a contested namespace and a cluster with no
/// trust material at all are both administrator faults an administrator can
/// fix, and the `Restore` must still be materializable afterwards. A terminal
/// refusal would make the operator mint a new plan and a new approval to
/// recover from someone else's typo.
///
/// # Errors
///
/// [`RestoreError::Api`] for a transport failure, which is a requeue;
/// [`RestoreError::Materialization`] for a conflict or an unconfigured
/// cluster, which is the non-terminal hold.
async fn resolve_trust(
    client: &kube::Client,
    namespace: &str,
) -> Result<crate::trust::ResolvedTrust, RestoreError> {
    match crate::trust::resolve(client, namespace)
        .await
        .map_err(RestoreError::Api)?
    {
        crate::trust::Resolution::Trust(trust) => Ok(*trust),
        crate::trust::Resolution::Conflict {
            namespace,
            policies,
        } => Err(RestoreError::Materialization(format!(
            "{}: the namespace {namespace} is claimed by more than one TrustPolicy ({}), so it \
             resolves to no trust at all and the verified Approval bundle cannot be \
             materialized; remove the namespace from all but one policy",
            crate::trust::REASON_TRUST_POLICY_CONFLICT,
            policies.join(", ")
        ))),
        crate::trust::Resolution::Unconfigured => Err(RestoreError::Materialization(
            "no TrustPolicy governs this namespace and there is no cluster-scoped TrustRoster \
             named 'default', so the verified Approval bundle cannot be materialized"
                .to_string(),
        )),
    }
}

/// `POST` the plan ConfigMap, and decide the 409.
///
/// A 409 IS SUCCESS ONLY IF THE EXISTING OBJECT IS OURS, immutable, has the
/// exact binding annotations, and carries byte-identical `spec.planBytes`.
/// Ownership alone is insufficient: a same-UID stale or substituted object
/// must never become the plan paired with a verified approval bundle.
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
    destinations: Option<&RestoreDestinations>,
) -> Result<(), RestoreError> {
    let name = restore.name_any();
    let cm_name = plan_config_map_name(&name);
    let uid = restore
        .uid()
        .ok_or_else(|| RestoreError::NoUid(name.clone()))?;
    let desired = plan_config_map_with_destinations(restore, destinations)?;
    let maps: Api<ConfigMap> = Api::namespaced(client.clone(), namespace);

    match maps.create(&PostParams::default(), &desired).await {
        Ok(created) if has_exact_restore_owner_set(&created.metadata, &name, &uid) => {
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
        Ok(_) => Err(RestoreError::Refused(
            TERMINAL_STATE_PLAN_CONFIG_MAP_CONFLICT,
            format!(
                "the API create response for ConfigMap {cm_name} did not retain the exact single \
                 owner reference for Restore {name} UID {uid}"
            ),
        )),
        Err(kube::Error::Api(e)) if e.code == 409 => {
            let existing = maps
                .get_opt(&cm_name)
                .await
                .map_err(RestoreError::Api)?
                .ok_or_else(|| {
                    RestoreError::Materialization(format!(
                        "the plan ConfigMap {cm_name} answered 409 to create and 404 to the \
                         following read; it was deleted concurrently and will be retried"
                    ))
                })?;
            if compatible_plan_config_map(&existing, &desired, &uid) {
                debug!(
                    restore = %name,
                    namespace = %namespace,
                    config_map = %cm_name,
                    "the existing immutable plan ConfigMap exactly matches this Restore's bytes and bindings"
                );
                Ok(())
            } else if compatible_legacy_plan_config_map(&existing, &desired, &uid) {
                info!(
                    restore = %name,
                    namespace = %namespace,
                    config_map = %cm_name,
                    "adopting the exact mutable plan left by a pre-PLAT-01 controller crash; \
                     the new Job independently pins and verifies these bytes before data access"
                );
                Ok(())
            } else {
                Err(RestoreError::Refused(
                    TERMINAL_STATE_PLAN_CONFIG_MAP_CONFLICT,
                    format!(
                        "the ConfigMap {cm_name} already exists but its owner UID, immutable bit, \
                         binding annotations, or plan bytes differ; the runner Job would mount a \
                         plan document this object did not render"
                    ),
                ))
            }
        }
        Err(e) => Err(RestoreError::Api(e)),
    }
}

/// Create the immutable per-Restore approval bundle, accepting a 409 only
/// when the existing object is byte-for-byte identical and owned by this
/// Restore UID.
async fn write_approval_bundle_config_map(
    restore: &Restore,
    approval: &Approval,
    trust: &crate::trust::ResolvedTrust,
    policy: &EffectivePolicy,
    now: DateTime<Utc>,
    client: &kube::Client,
    namespace: &str,
) -> Result<(), RestoreError> {
    let restore_name = restore.name_any();
    let uid = restore
        .uid()
        .ok_or_else(|| RestoreError::NoUid(restore_name.clone()))?;
    // PLAT-14.3b: a rehearsal's bundle is the standing one. The
    // `RehearsalSchedule` reconciler renders the same object from the same
    // function before it creates the `Restore`, so the create below meets a
    // 409 whose existing bytes are byte-identical — which
    // `compatible_approval_bundle` already treats as success.
    let desired = if restore.spec.authorization.is_some() {
        standing_bundle_for(restore, approval, trust, now)?
    } else {
        approval_bundle_config_map_with_policy(restore, approval, trust, policy, now)?
    };
    let bundle_name = approval_bundle_config_map_name(&restore_name);
    let maps: Api<ConfigMap> = Api::namespaced(client.clone(), namespace);

    match maps.create(&PostParams::default(), &desired).await {
        Ok(created) if has_exact_restore_owner_set(&created.metadata, &restore_name, &uid) => {
            info!(
                restore = %restore_name,
                namespace = %namespace,
                config_map = %bundle_name,
                "materialized the immutable per-Restore approval bundle"
            );
            Ok(())
        }
        Ok(_) => Err(RestoreError::Refused(
            TERMINAL_STATE_APPROVAL_BUNDLE_CONFLICT,
            format!(
                "the API create response for ConfigMap {bundle_name} did not retain the exact \
                 single owner reference for Restore {restore_name} UID {uid}"
            ),
        )),
        Err(kube::Error::Api(error)) if error.code == 409 => {
            let existing = maps
                .get_opt(&bundle_name)
                .await
                .map_err(|error| RestoreError::Materialization(format!(
                    "could not inspect the existing approval bundle {bundle_name}: {error}"
                )))?
                .ok_or_else(|| RestoreError::Materialization(format!(
                    "the approval bundle {bundle_name} answered 409 to create and 404 to the following read"
                )))?;
            if compatible_approval_bundle(&existing, &desired, &uid) {
                debug!(
                    restore = %restore_name,
                    namespace = %namespace,
                    config_map = %bundle_name,
                    "the existing immutable approval bundle exactly matches the desired bytes and bindings"
                );
                Ok(())
            } else {
                // **LOW-1: name the roster change when that is what it is.**
                //
                // `authorization-keys.json` is a function of the namespace's
                // resolved trust AND the clock (`keyring(trust, now)` renders
                // only keys that may authorise TODAY). The schedule writes the
                // bundle at one instant and this reconciler re-renders at
                // another, so a key added, retired or crossing `notAfter`
                // between them makes the two renders differ. That is
                // fail-closed and correct, but reported as a bare
                // `ApprovalBundleConflict` it sends an operator to look at a
                // ConfigMap when the fact is "the trust roster changed".
                // Same terminal state, honest message.
                let differing = differing_bundle_members(&existing, &desired);
                let detail = if differing == [AUTHORIZATION_KEYS_FILE] {
                    format!(
                        "; the ONLY member that differs is {AUTHORIZATION_KEYS_FILE}, which is \
                         rendered from this namespace's resolved trust as of the moment it is \
                         written — a key added, retired or expiring between the slot firing and \
                         this reconcile changes it. The rehearsal is refused rather than run \
                         under a keyring nothing committed to; the schedule's next slot renders \
                         a fresh bundle"
                    )
                } else if differing.is_empty() {
                    String::new()
                } else {
                    format!("; the differing member(s) are {}", differing.join(", "))
                };
                Err(RestoreError::Refused(
                    TERMINAL_STATE_APPROVAL_BUNDLE_CONFLICT,
                    format!(
                        "the ConfigMap {bundle_name} already exists but its owner UID, immutable bit, binding annotations, or public artifact bytes differ; it cannot substitute for this Restore's verified approval{detail}"
                    ),
                ))
            }
        }
        Err(error) => Err(RestoreError::Materialization(format!(
            "could not create approval bundle {bundle_name}: {error}"
        ))),
    }
}

/// Which `data` keys differ between an existing bundle and the desired one.
///
/// Diagnostic ONLY — the admission decision is
/// [`compatible_approval_bundle`]'s, which compares owner UID, the immutable
/// bit, the binding annotations and the bytes. This exists so the refusal can
/// say WHICH member moved, and it never widens what is accepted.
///
/// Sorted, so the message is stable, and it names keys and never bytes: a
/// bundle member can be a public key or a signed envelope, and neither belongs
/// in a status message.
fn differing_bundle_members(existing: &ConfigMap, desired: &ConfigMap) -> Vec<String> {
    let empty = BTreeMap::new();
    let a = existing.data.as_ref().unwrap_or(&empty);
    let b = desired.data.as_ref().unwrap_or(&empty);
    let mut keys: Vec<String> = a.keys().chain(b.keys()).cloned().collect();
    keys.sort();
    keys.dedup();
    keys.into_iter().filter(|k| a.get(k) != b.get(k)).collect()
}

/// Find the pod the exit code is read from — D-SEAMS **S6**, `SEC-PODLOG`.
///
/// The prefixed selector first, the legacy one as a fallback — the same two,
/// in the same order, as the `Backup` path, through
/// [`super::backup::pod_selectors`] so the two reconcilers cannot disagree
/// about which label the job controller sets — and then the same ONE selection
/// rule, [`check::pod::find_owned_pod_by_selectors`]: a candidate is read only
/// when its **controller** `ownerReference` is a `Job` carrying this Job's
/// `metadata.uid`.
///
/// # What this used to do, and why it is gone
///
/// `list.items.into_iter().next()` — the first pod the label selector
/// returned, with no owner check at all. `batch.kubernetes.io/job-name` is a
/// plain label that anything able to create a pod in the namespace can set, so
/// a planted pod's `exitCode` and its three interface **I8** evidence keys
/// became this `Restore`'s recorded outcome. A `Restore` is the path that
/// writes data back into a cluster and the object whose status an approver
/// reads afterwards, which is why a stranger's exit code on it is worse than a
/// missing one. Zero owned pods is now "no pod yet" and goes to the
/// crashed-Job branch, which writes a terminal `NoExitCode` rather than a
/// borrowed success.
async fn find_pod(
    client: &kube::Client,
    namespace: &str,
    job_name: &str,
    job_uid: Option<&str>,
) -> Result<check::pod::FoundPod, RestoreError> {
    check::pod::find_owned_pod_by_selectors(
        client,
        namespace,
        job_name,
        job_uid,
        &backup::pod_selectors(job_name),
    )
    .await
    .map_err(RestoreError::Api)
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
    reconcile_restore_with_policy(
        restore,
        client,
        scorecard,
        verify,
        now,
        runner,
        &ApprovalPolicySet::default(),
    )
    .await
}

/// [`reconcile_restore_with_runner_image`], under the installation's approval
/// policies — PLAT-19.2. A seventh parameter on a new function rather than on
/// the old one, for the reason that function gives for its own sixth.
///
/// # Errors
///
/// [`RestoreError`] for anything that is not an outcome.
pub async fn reconcile_restore_with_policy(
    restore: &Restore,
    client: &kube::Client,
    scorecard: ScorecardOracle<'_>,
    verify: VerifyOracle<'_>,
    now: DateTime<Utc>,
    runner: &job::RunnerImage,
    policies: &ApprovalPolicySet,
) -> Result<RestoreOutcome, RestoreError> {
    match reconcile_restore_inner(restore, client, scorecard, verify, now, runner, policies).await {
        Err(RestoreError::Materialization(message)) => {
            let name = restore.name_any();
            let namespace = restore
                .namespace()
                .ok_or_else(|| RestoreError::NoNamespace(name.clone()))?;
            warn!(
                restore = %name,
                namespace = %namespace,
                reason = REASON_APPROVAL_BUNDLE_MATERIALIZATION_FAILED,
                error = %message,
                "Restore execution-input materialization failed; no Job was created"
            );
            let restores: Api<Restore> = Api::namespaced(client.clone(), &namespace);
            patch_status_if_changed(
                &restores,
                restore,
                &name,
                approval_bundle_hold_patch(restore, &message, now),
            )
            .await?;
            Ok(RestoreOutcome {
                job_name: name,
                created: false,
                admission: None,
                exit_code: None,
                terminal_state: None,
                keys: RestoreEvidenceKeys::default(),
                ttl_patched: false,
                requeue: Requeue::After(ADMISSION_REQUEUE_SECS),
            })
        }
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
                "refusing this Restore terminally: no Job was created and no unverified inputs \
                 were used; a requeue over an immutable spec would never succeed"
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
    policies: &ApprovalPolicySet,
) -> Result<RestoreOutcome, RestoreError> {
    let name = restore.name_any();
    let namespace = restore
        .namespace()
        .ok_or_else(|| RestoreError::NoNamespace(name.clone()))?;
    // PLAT-19.2: what this namespace is bound to NOW, resolved once per pass
    // and used for admission, the bundle, the argv and the environment alike.
    let effective_policy = policies.resolve(&namespace);
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
    if let Some(job) = existing.as_ref() {
        if !compatible_restore_job(job, restore) {
            return Err(RestoreError::Refused(
                TERMINAL_STATE_JOB_NAME_CONFLICT,
                format!(
                    "Job {namespace}/{job_name} already exists but is not controlled by Restore \
                     {namespace}/{name} with UID {}; remove the foreign or stale Job before \
                     retrying; the controller did not observe or adopt it",
                    restore.uid().unwrap_or_default()
                ),
            ));
        }
    }

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
            // …except an evidence fetch it may still owe (D2 §3.9).
            let requeue = continue_evidence_fetch(restore, client, &namespace, now, runner).await?;
            return Ok(RestoreOutcome {
                job_name,
                created: false,
                admission: None,
                exit_code: restore.status.as_ref().and_then(|s| s.exit_code),
                terminal_state: None,
                keys: RestoreEvidenceKeys::default(),
                ttl_patched: false,
                requeue,
            });
        }

        // THE ADMISSION, BEFORE THE FIRST `POST`. Two `GET`s and a pure
        // function; Global Constraint 6's operator half is that an unapproved
        // plan creates NOTHING, and the zero-`POST` assertions in
        // `tests/restore_controller.rs` are made over route tables that HAVE
        // the `POST` routes present, so it is the count that refuses.
        let approval = get_approval(restore, client, &namespace).await?;
        let cluster = get_target_cluster(restore, client, &namespace).await?;
        // **A REHEARSAL RESOLVES TRUST BEFORE IT IS ADMITTED** — PLAT-14.3b.
        //
        // For an ordinary Restore trust stays a MATERIALIZATION input, fetched
        // only once a Job is going to exist, and this `None` keeps that path
        // byte-for-byte as it was. A standing authorization cannot be judged
        // that way: "the key may no longer authorise anything new" and "that
        // key's usage is not an approver's" are facts about the namespace's
        // resolved trust and are two of D3 §4.3(c)'s four each-slot refusals.
        // One extra read, for the one kind that needs it.
        let standing_trust = if restore.spec.authorization.is_some() {
            Some(resolve_trust(client, &namespace).await?)
        } else {
            None
        };
        let standing_inputs = standing_trust
            .as_ref()
            .map(|trust| StandingAdmission { trust, now });
        let admission = admit_with_policy(
            restore,
            approval.as_ref(),
            cluster.as_ref(),
            standing_inputs.as_ref(),
            Some(&PolicyAdmission {
                policy: &effective_policy,
                now,
            }),
        );
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
        let approval = approval.ok_or_else(|| {
            RestoreError::Materialization(
                "the admitted Approval disappeared before its bundle was materialized".to_string(),
            )
        })?;
        // === THE DESTINATIONS, AFTER CHECKS 0-4 AND BEFORE THE FIRST POST ===
        //
        // AFTER, so today's reason precedence is unchanged: an unapproved plan
        // is still `ApprovalNotVerified` and not a destination complaint. The
        // hold here is the same 30-second hold the approval gets, for the same
        // reason — an operator who is still creating the destination is in the
        // position of an approver who has not signed yet — and it is not capped:
        // a `Restore` waits for a human either way.
        let destinations =
            match admit_restore_destinations(restore, client, &namespace, now).await? {
                RestoreDestinationAdmission::NotRequested => None,
                RestoreDestinationAdmission::Resolved(pair) => Some(pair),
                RestoreDestinationAdmission::Holding { reason, message } => {
                    info!(
                        restore = %name,
                        namespace = %namespace,
                        reason,
                        detail = %message,
                        "no Job exists until both of this Restore's BackupDestinations resolve and \
                         report Valid=True; holding for {ADMISSION_REQUEUE_SECS}s"
                    );
                    patch_status_if_changed(
                        &restores,
                        restore,
                        &name,
                        destination_hold_patch(restore, reason, &message, now),
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
                        requeue: Requeue::After(ADMISSION_REQUEUE_SECS),
                    });
                }
            };

        let trust = resolve_trust(client, &namespace).await?;
        let matched_key_id = approval
            .status
            .as_ref()
            .and_then(|status| status.matched_key_id.clone())
            .ok_or_else(|| {
                RestoreError::Materialization(
                    "the admitted Approval carries no status.matchedKeyId".to_string(),
                )
            })?;
        // One exact verified key, not every key the roster happens to contain.
        // The runner pins this id and verifies the detached signature again.
        let key_ids = vec![matched_key_id];
        let mut spec = runner_job_spec_with_policy(
            restore,
            &cluster,
            &key_ids,
            &approval,
            &trust,
            &effective_policy,
            now,
            destinations.as_deref(),
        )?;
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
        write_plan_config_map(restore, client, &namespace, destinations.as_deref()).await?;
        write_approval_bundle_config_map(
            restore,
            &approval,
            &trust,
            &effective_policy,
            now,
            client,
            &namespace,
        )
        .await?;

        let created = match jobs
            .create(&PostParams::default(), &job::build(&spec))
            .await
        {
            Ok(created) if compatible_restore_job(&created, restore) => true,
            Ok(_) => {
                return Err(RestoreError::Refused(
                    TERMINAL_STATE_JOB_NAME_CONFLICT,
                    format!(
                        "the API create response for Job {namespace}/{job_name} did not retain \
                         the exact single owner reference for Restore {namespace}/{name} UID {}",
                        restore.uid().unwrap_or_default()
                    ),
                ))
            }
            Err(kube::Error::Api(error)) if error.code == 409 => {
                let raced = jobs
                    .get_opt(&job_name)
                    .await
                    .map_err(RestoreError::Api)?
                    .ok_or_else(|| {
                        RestoreError::Materialization(format!(
                            "Job {namespace}/{job_name} answered 409 to create and 404 to the \
                             following read; it was deleted concurrently and will be retried"
                        ))
                    })?;
                if !compatible_restore_job(&raced, restore) {
                    return Err(RestoreError::Refused(
                        TERMINAL_STATE_JOB_NAME_CONFLICT,
                        format!(
                            "Job {namespace}/{job_name} won a concurrent create but is not \
                             controlled by this Restore UID; it was not adopted"
                        ),
                    ));
                }
                false
            }
            Err(error) => return Err(RestoreError::Api(error)),
        };
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
            running_status_patch(restore, &job_name, created, now),
        )
        .await?;
        return Ok(RestoreOutcome {
            job_name,
            created,
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
        // D3 §2.3 / §2.4 — PLAT-14.1, the same ONE derivation the `Backup`
        // twin calls. A `Restore` whose runner cannot mount its approval
        // bundle used to sit at `phase: Running` until its deadline with
        // nothing on the object to say so.
        let stored = restore.status.as_ref().and_then(|s| s.progress.as_ref());
        let run = diagnostics::observe(
            client,
            &namespace,
            &job,
            &backup::pod_selectors(&job_name),
            stored,
            now,
        )
        .await
        .map_err(RestoreError::Api)?;
        patch_status_if_changed(
            &restores,
            restore,
            &name,
            diagnostics::apply(
                running_status_patch(restore, &job_name, false, now),
                &diagnostics::Write {
                    derived: &run.derived,
                    progress: &run.progress,
                    stored,
                    conditions: restore.status.as_ref().and_then(|s| s.conditions.as_ref()),
                    generation: restore.meta().generation,
                    // D3 §2.2: `Restore.status.reason` is the `RunnerReady`
                    // reason WHILE IT IS FALSE, and the base builder's
                    // `JobCreated` otherwise. Review finding M2's column keeps
                    // its meaning — "the reason of the condition this patch
                    // writes about current state" — and now answers it for the
                    // four minutes a pod spends in `ImagePullBackOff`.
                    scalar_reason: true,
                    now,
                },
            ),
        )
        .await?;
        let failed_fast = diagnostics::fail_fast(
            client,
            &namespace,
            &job,
            &restore.uid().unwrap_or_default(),
            &run,
            stored,
            now,
        )
        .await
        .map_err(RestoreError::Api)?;
        return Ok(RestoreOutcome {
            job_name,
            created: false,
            admission: None,
            exit_code: None,
            terminal_state: failed_fast.map(str::to_string),
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
        // D3 §2.7's REPAIR — the `Backup` twin carries the reasoning. A
        // terminal `Restore` requeues `AwaitChange`, so this is the ONE pass
        // that will ever look at the Job again; a TTL patch that failed on the
        // pass that made the object terminal is repaired here or never.
        let ttl_patched =
            diagnostics::repair_ttl(client, &namespace, &job, &restore.uid().unwrap_or_default())
                .await
                .map_err(RestoreError::Api)?;
        // The other thing a terminal `Restore` may still owe: its evidence
        // verdict, when an evidence-fetch Job reads it (D2 §3.9). The run's
        // own pod is still not read.
        let requeue = continue_evidence_fetch(restore, client, &namespace, now, runner).await?;
        return Ok(RestoreOutcome {
            job_name,
            created: false,
            admission: None,
            exit_code: restore.status.as_ref().and_then(|s| s.exit_code),
            terminal_state: None,
            keys: RestoreEvidenceKeys::default(),
            ttl_patched,
            requeue,
        });
    }

    let found = find_pod(client, &namespace, &job_name, job.uid().as_deref()).await?;
    // The pod and its code travel together; see the `Backup` twin for why the
    // `pods/log` call below must hold a `&Pod` and not an `Option` (review
    // finding R6).
    let terminated = found
        .pod
        .as_ref()
        .and_then(|p| backup::terminated_exit_code(p).map(|code| (p, code)));

    // STEP 4. The crashed-Job case, before the happy path, because the happy
    // path needs a code and this branch is "there is none".
    let Some((pod, exit_code)) = terminated else {
        let terminal_state = if found.contested.is_empty() {
            // D3 §2.2, the `Backup` twin's rule verbatim: the four new states
            // replace `NoExitCode` only when a matching diagnostic was
            // recorded before the Job ended.
            let from_pod = backup::crash_terminal_state(found.pod.as_ref());
            if from_pod == crate::conditions::TERMINAL_STATE_NO_EXIT_CODE {
                diagnostics::recorded_terminal_state(
                    restore.status.as_ref().and_then(|s| s.progress.as_ref()),
                )
                .unwrap_or(from_pod)
            } else {
                from_pod
            }
        } else {
            TERMINAL_STATE_POD_OWNERSHIP_CONTESTED
        };
        warn!(
            restore = %name,
            namespace = %namespace,
            job = %job_name,
            terminal_state,
            contested = %found.contested.join(","),
            "no exit code could be read for this run: either the Job finished with no terminated \
             state for the runner container, or more than one pod claimed the Job and none was \
             read. A terminal status rather than watching forever, and no invented exit code"
        );
        patch_status_if_changed(
            &restores,
            restore,
            &name,
            diagnostics::apply_finished(
                crashed_status_patch(restore, terminal_state, &job_name, now),
                restore.status.as_ref().and_then(|s| s.progress.as_ref()),
                now,
            ),
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
    let pod_name = pod.name_any();
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
    //
    // WHICH HANDLE, FIRST — D2 §3.9's Restore paragraph, §3.10. A legacy object
    // takes the oracle it has always taken; a destination-backed one takes the
    // EVIDENCE destination's own read-only handle, and never the controller's
    // global one: that handle holds a different principal over a different
    // bucket, and an answer from it would read as "no scorecard" rather than
    // "wrong bucket" (grounding G2).
    //
    // THE EVIDENCE DESTINATION AND NOT THE SOURCE ONE. The scorecard was
    // written under `evidenceWrite` of the evidence destination; reading it
    // back from the source destination's bucket is the two-destination version
    // of the same defect.
    let evidence_from = backup::evidence_source_for(
        restore.spec.evidence_destination_ref.as_ref(),
        client,
        &namespace,
        now,
    )
    .await
    .map_err(RestoreError::Api)?;
    let observed = match (keys.scorecard.as_ref(), &evidence_from) {
        (None, _) => None,
        (Some(key), backup::EvidenceSource::GlobalHandle) => scorecard(key.clone()).await,
        (Some(key), backup::EvidenceSource::Destination(store)) => {
            // THE SAME `observe_scorecard`, ON A DIFFERENT HANDLE. One reader
            // of a scorecard, one JSON-pointer extraction; a second one for
            // destination-backed runs would be a second answer to "what does
            // this document say".
            let handle = Arc::clone(store);
            let key = key.clone();
            tokio::task::spawn_blocking(move || observe_scorecard(&handle, &key))
                .await
                .ok()
                .flatten()
        }
        // NOTHING WAS READ AND NOTHING IS GUESSED: no `outcome`, no
        // `objectives`, no `measured` block copied out of a document nobody
        // fetched. `FetchJob` ON THIS PASS too: the Job's relay supplies
        // them, on a later write.
        (
            Some(_),
            backup::EvidenceSource::NotAttempted { .. } | backup::EvidenceSource::FetchJob { .. },
        ) => None,
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
    let terminal = diagnostics::apply_finished(
        finished_status_patch(
            restore,
            exit_code,
            &keys,
            refusal.as_deref(),
            observed.as_ref(),
            topics.as_ref(),
            preflight.as_ref(),
            now,
        ),
        restore.status.as_ref().and_then(|s| s.progress.as_ref()),
        now,
    );
    // WHERE THE OBJECT NOW STANDS. The verification patch below is the SECOND
    // write of this pass, and seam S7's precondition makes that fact load-bearing:
    // preconditioned on the version the watch delivered it would be refused, and a
    // terminal `Restore` is never reconciled again.
    let at = patch_status_if_changed(&restores, restore, &name, terminal.clone()).await?;

    // ONLY NOW. The `?` above is what makes this ordering a guarantee rather
    // than a comment: a status patch that did not return 200 leaves this
    // function before any TTL exists, so pod GC cannot start on a run whose
    // code was never recorded.
    jobs.patch(
        &job_name,
        &PatchParams::default(),
        &Patch::Merge(json!({
            // D3 §2.7, chart value `controller.jobTtlSeconds`; the default
            // is `TTL_SECONDS_AFTER_FINISHED`.
            "spec": { "ttlSecondsAfterFinished": diagnostics::job_ttl_seconds() }
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
    // ATTEMPTED ONLY WHEN AN ARTIFACT WAS WRITTEN, AND **THE TWO MANDATORY
    // KEYS ARE WHAT SAY SO** — D2 §3.9's Restore paragraph, which runs "the
    // same flow" as the `Backup` half on `scorecard-key`/`sidecar-key`, and
    // whose step 2 chooses the mode once both keys are present. At exits 1, 3
    // and 4 the contract says no artifact was written (GC11), the runner
    // prints no key lines, and there is no document to have an opinion about:
    // no verification block is written at all. That absence is the GC11
    // distinction and it is DELIBERATELY kept.
    //
    // THE DIGEST IS REQUIRED BY THE PATHS THAT VERIFY BYTES, AND BY THEM ONLY.
    // A `NotAttempted` source fetched nothing, so `scorecard_sha256` is `None`
    // BY CONSTRUCTION (`observed` is `None` for it, above) — and while the
    // digest was demanded of every source this whole block was skipped for it,
    // so an operator whose EVIDENCE destination reads with a grant only a pod
    // may hold (`SecretKeys`, `WorkloadIdentity`) saw NO
    // `status.evidence.verification` at all rather than the honest
    // `NotAttempted` and the sentence naming why. That is the `Restore` twin
    // of defect D2-EVIDENCE-NOTATTEMPTED-UNWRITTEN, and it is the same fix as
    // `controllers::backup`'s — one flow, one answer. NOTHING IS INVENTED
    // HERE: the only verdict writable without bytes is `NotAttempted`, and
    // `Valid`/`Invalid`/`Untrusted` still come only from a verifier that read
    // the document.
    let reference = match (
        keys.scorecard.as_deref(),
        keys.sidecar.as_deref(),
        observed
            .as_ref()
            .and_then(|o| o.scorecard_sha256.as_deref()),
    ) {
        (Some(payload_key), Some(sidecar_key), Some(digest)) => Some(EvidenceRef {
            // PLAT-19.1: trust is resolved PER NAMESPACE, and this is
            // `metadata.namespace` read off the object being reconciled —
            // never a name the subject supplied.
            namespace: namespace.to_string(),
            payload_key: payload_key.to_string(),
            payload_sha256: digest.to_string(),
            sidecar_key: sidecar_key.to_string(),
            payload_type: logweir_verify::PAYLOAD_TYPE_SCORECARD,
        }),
        _ => None,
    };
    let verdict = match (&evidence_from, reference) {
        // THE DECISION IS ALREADY MADE AND IT IS RECORDED. `evidence_source_for`
        // answered with the reason there is no reader for this run's evidence;
        // the guard is `keys.mandatory_complete()` and not the digest, because
        // the question this arm answers is "was a scorecard written", which the
        // keys say and the digest — which only a fetch produces — cannot.
        (backup::EvidenceSource::NotAttempted { detail }, _) if keys.mandatory_complete() => {
            Some(crate::verification::VerificationResult::not_attempted(
                logweir_verify::PAYLOAD_TYPE_SCORECARD,
                detail.clone(),
            ))
        }
        // THE JOB'S VERDICT IS WRITTEN BY THE JOB'S PASS, below.
        (backup::EvidenceSource::FetchJob { .. }, _) => None,
        (backup::EvidenceSource::GlobalHandle, Some(reference)) => Some(verify(reference).await),
        // THE SAME VERIFIER, ON THE EVIDENCE DESTINATION'S HANDLE — D2
        // §3.9's "no second verification path".
        (backup::EvidenceSource::Destination(store), Some(reference)) => Some(
            crate::verification::verify_oracle(Some(Arc::clone(store)), client.clone())(reference)
                .await,
        ),
        // GC11's no-artifact case, and a fetch that returned no digest: no
        // document, no opinion, no block.
        _ => None,
    };
    if let Some(result) = verdict {
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
            // R2's sibling: the verb every arm earns. It used to read
            // "weirkeeper verified this Restore's signed scorecard with its
            // read-only evidence credential", which is false on the
            // `NotAttempted` arm — nothing was fetched, no credential was used
            // and no signature was checked. `verification` carries which
            // verdict it was.
            "weirkeeper recorded this Restore's evidence verdict"
        );
        patch_status_at(
            &restores,
            restore,
            &name,
            &at,
            // EXPLICIT NULLS FOR THE FIELDS THIS VERDICT DOES NOT HOLD — see
            // `verification::verification_patch_value`. One rule, one helper,
            // both reconcilers and the re-trust patch.
            second_patch(
                &conditions_in(&terminal),
                verified,
                crate::verification::verification_patch_value(block),
            ),
        )
        .await?;
    }

    // D2 §3.9, THE DEFAULT ARM: an evidence-fetch Job reads the scorecard
    // with the EVIDENCE destination's `evidenceRead` grant, after the terminal
    // patch and the runner Job's TTL. A terminal `Restore` otherwise waits for
    // a change, so while the fetch is owed it is looked at again on a timer.
    let mut requeue = Requeue::AwaitChange;
    if matches!(evidence_from, backup::EvidenceSource::FetchJob { .. }) && keys.mandatory_complete()
    {
        let stored = with_status_written(restore, &terminal, &at);
        evidence_fetch_pass(
            &restores,
            &stored,
            &at,
            client,
            &namespace,
            &name,
            1,
            &evidence_from,
            now,
            runner,
        )
        .await?;
        requeue = Requeue::After(REQUEUE_SECS);
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
        requeue,
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
async fn reconcile(
    restore: Arc<Restore>,
    ctx: Arc<Context>,
    approval_policies: Arc<ApprovalPolicySet>,
) -> Result<Action, RestoreError> {
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
    let outcome = reconcile_restore_with_policy(
        &restore,
        &ctx.client,
        &oracle,
        &verify,
        Utc::now(),
        &ctx.runner_image,
        &approval_policies,
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

// ---------------------------------------------------------------------------
// The re-trust trigger — PLAT-19.1, D3 §7.4 "re-evaluation without re-fetching"
// ---------------------------------------------------------------------------

/// Every Restore a `TrustPolicy` event could change the verdict of.
///
/// # No LIST, and that is the whole design
///
/// The obvious shape is "one paginated LIST per bound namespace on every
/// policy event". This controller already RUNS a watch over every Restore in
/// the cluster — that is what `Controller::new` is — so `Controller::store()`
/// is the same index, already paid for, already warm, and cannot be staler
/// than the event being mapped. The trigger therefore costs **zero** API
/// calls and is bounded by the objects this controller holds rather than by
/// the cluster.
///
/// # It over-approximates on purpose
///
/// [`crate::trust::may_govern`] treats a `default: true` policy as claiming
/// every namespace, although resolution would hand an explicitly-named
/// namespace to its own policy instead. Enqueuing an object the policy does
/// not govern costs one re-derivation that writes nothing
/// (`verification::retrust` returns `None` for an unchanged block, erratum
/// **E11(d)**); failing to enqueue one leaves a revoked key green until
/// something else happens to reconcile it. The two errors are not symmetric.
fn policy_targets(
    objects: &reflector::Store<Restore>,
    scopes: &crate::trust::PolicyScopeMemory,
    policy: &crate::crds::trust_policy::TrustPolicy,
) -> Vec<ObjectRef<Restore>> {
    // THE UNION OF BEFORE AND AFTER. A `watches` mapper is handed only the
    // NEW object, so an edit that NARROWS — a namespace removed, `default`
    // cleared — would otherwise enqueue nothing in the namespace it just
    // stopped governing, which is the one edit that certainly changed that
    // namespace's resolution. See `trust::PolicyScopeMemory`.
    let scope = scopes.observe(policy);
    crate::verification::targets_in_scope(objects.state(), &scope)
}

/// [`reconcile`], with the re-trust pass in front of it.
///
/// # Why this is in FRONT and not inside
///
/// Because a terminal object short-circuits: [`reconcile_restore_inner`]
/// returns long before the verification block is written, which is what keeps
/// a finished run quiet (D3 §1) and is also why "force a reconcile" was never
/// a way to re-apply a revocation. The re-trust pass is the other half of that
/// rule — it re-derives the verdict from what is already ON the status, with
/// no storage read and no signature check — so it belongs where a terminal
/// object still reaches it.
///
/// It runs only when the object already carries a `matchedKeyId`
/// ([`crate::verification::has_trust_verdict`]) and the policy reflector has
/// synced. Both guards are about cost and correctness at once: an object with
/// no verdict has nothing to re-decide, and an unsynced store looks like a
/// cluster with no `TrustPolicy` at all, which would resolve every namespace
/// to the legacy roster and write a verdict nobody asked for.
async fn reconcile_with_trust(
    restore: Arc<Restore>,
    ctx: Arc<Context>,
    policies: reflector::Store<crate::crds::trust_policy::TrustPolicy>,
    synced: Arc<AtomicBool>,
    approval_policies: Arc<ApprovalPolicySet>,
) -> Result<Action, RestoreError> {
    if synced.load(Ordering::Relaxed) {
        let value = serde_json::to_value(&*restore).ok();
        let status = value.as_ref().and_then(|v| v.get("status"));
        if crate::verification::has_trust_verdict(status) {
            if let Some(namespace) = restore.namespace() {
                let snapshot: Vec<crate::crds::trust_policy::TrustPolicy> =
                    policies.state().into_iter().map(|p| (*p).clone()).collect();
                let resolution =
                    crate::trust::resolve_with(&snapshot, &ctx.client, &namespace).await?;
                let api: Api<Restore> = Api::namespaced(ctx.client.clone(), &namespace);
                // ONE BOUNDED RE-READ FOR A STATUS THAT PREDATES `signedAt`
                // (TRUST-UPGRADE-SIGNEDAT). It happens only for a block that
                // carries a matched key, no signing time and no `trust` at all
                // — a pre-PLAT-19.1 write — and it goes through THE SAME
                // evidence path the original verdict came from, so a
                // destination-backed run reads its own bucket and one whose
                // grant only a pod may hold reads nothing and says so.
                let signing_time = match crate::verification::signing_time_need(status, Utc::now())
                {
                    crate::verification::ReadPlan::None => {
                        crate::verification::SigningTime::NotNeeded
                    }
                    // THE BACKOFF SHORT-CIRCUITS BEFORE `evidence_source`, so a
                    // deferred pass costs neither a `Store::get` NOR the
                    // destination read that resolving the handle would need.
                    crate::verification::ReadPlan::Deferred => {
                        crate::verification::SigningTime::Deferred
                    }
                    crate::verification::ReadPlan::Read(need) => {
                        // A FAILED READ IS NOT A FAILED RECONCILE — review
                        // finding **F6**; see the same hunk in `backup.rs`.
                        let (handle, unread) = match backup::evidence_source_for(
                            restore.spec.evidence_destination_ref.as_ref(),
                            &ctx.client,
                            &namespace,
                            Utc::now(),
                        )
                        .await
                        {
                            Ok(backup::EvidenceSource::GlobalHandle) => (ctx.archive.clone(), None),
                            Ok(backup::EvidenceSource::Destination(store)) => (Some(store), None),
                            Ok(backup::EvidenceSource::NotAttempted { detail }) => {
                                (None, Some(detail))
                            }
                            // A pod-only grant is not re-read by the re-trust
                            // hook: there is no Job to wait for here.
                            Ok(backup::EvidenceSource::FetchJob { destination, .. }) => (
                                None,
                                Some(format!(
                                    "BackupDestination {}/{} reads evidence with a grant only a \
                                     pod may hold; the signing time is not re-read by the \
                                     re-trust pass",
                                    destination.namespace, destination.name
                                )),
                            ),
                            Err(e) => (
                                None,
                                Some(crate::verification::evidence_path_unreadable(&e)),
                            ),
                        };
                        crate::verification::recover_signing_time(handle, unread, need).await
                    }
                };
                crate::verification::apply_retrust(
                    &api,
                    &*restore,
                    &resolution,
                    crate::verification::restore_badge,
                    Utc::now(),
                    &signing_time,
                )
                .await?;
            }
        }
    }
    reconcile(restore, ctx, approval_policies).await
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
///
/// `approval_policies` is the installation's approval-policy document, read
/// once by `main` (PLAT-19.2), and shared by every per-namespace copy.
pub async fn controller(
    client: kube::Client,
    archive: Option<Arc<Store>>,
    runner_image: job::RunnerImage,
    approval_policies: Arc<ApprovalPolicySet>,
) {
    // D0 STAGE 5: ONE WATCH PER WATCHED NAMESPACE. `crate::scope` is the whole
    // cluster unless `LOGWEIR_WATCH_NAMESPACES` names the execution
    // namespaces, and then this reconciler runs once per namespace with an
    // `Api::namespaced` watch — the only shape the scoped chart's RoleBindings
    // permit.
    //
    // The `TrustPolicy` store is built ONCE here and shared by every copy
    // (review L3): the kind is cluster-scoped, so a reflector per namespace
    // would be N identical cluster-wide watches.
    let policies = crate::trust::spawn_policy_reflector(&client);
    crate::scope::run_everywhere(move |namespace| {
        controller_in(
            client.clone(),
            archive.clone(),
            runner_image.clone(),
            policies.clone(),
            Arc::clone(&approval_policies),
            namespace,
        )
    })
    .await;
}

/// One watch of [`controller`], over `namespace` (`None` is the whole
/// cluster, the behaviour before D0 stage 5).
fn controller_in(
    client: kube::Client,
    archive: Option<Arc<Store>>,
    runner_image: job::RunnerImage,
    shared: crate::trust::SharedPolicies,
    approval_policies: Arc<ApprovalPolicySet>,
    namespace: Option<String>,
) -> impl std::future::Future<Output = ()> + Send {
    let api: Api<Restore> = crate::scope::api(&client, namespace.as_deref());
    let jobs: Api<Job> = crate::scope::api(&client, namespace.as_deref());
    let client_for_watch = client.clone();
    let ctx = Arc::new(Context {
        client,
        archive,
        runner_image,
    });
    // The ONE `TrustPolicy` store, shared by the trigger's resolutions and by
    // every copy of this reconciler (`crate::trust::spawn_policy_reflector`).
    // The trigger itself is a watch per copy: it maps a policy event to the
    // objects in THIS copy's store.
    let policies = shared.store;
    let synced = shared.synced;
    let policy_api: Api<crate::crds::trust_policy::TrustPolicy> = Api::all(client_for_watch);
    // ONE memory of what each policy bound last, owned by the mapper.
    let scopes = Arc::new(crate::trust::PolicyScopeMemory::default());
    async move {
        let controller = Controller::new(api, watcher::Config::default());
        let objects = controller.store();
        controller
            .owns(jobs, watcher::Config::default())
            // THE RE-TRUST TRIGGER. A `TrustPolicy` event maps to the objects
            // this controller already holds in the namespaces that policy could
            // govern — see `policy_targets`.
            .watches(policy_api, watcher::Config::default(), move |policy| {
                policy_targets(&objects, &scopes, &policy)
            })
            .run(
                move |object, context| {
                    reconcile_with_trust(
                        object,
                        context,
                        policies.clone(),
                        Arc::clone(&synced),
                        Arc::clone(&approval_policies),
                    )
                },
                error_policy,
                ctx,
            )
            .for_each(|_| std::future::ready(()))
            .await;
    }
}
