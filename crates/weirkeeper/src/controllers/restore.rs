//! The `Restore` reconciler: one Job, the plan hash recomputed at
//! Job-creation time, and `mode: scratch` is a field.
//!
//! # Admission happens here, not in the pod
//!
//! Approval identity and plan hash are checked here before any pod exists.
//! The runner then independently captures and validates the controller-pinned
//! plan, approval, sidecar, public key, and allowlist before constructing a
//! Kafka or object-store client. A bundle replacement therefore refuses
//! before phase 0 rather than being masked by target reachability.
//!
//! The hash is recomputed at Job-creation time rather than read from a
//! `status`, because a spec schema change invalidates every approval and a
//! status is a cache. Interface **I18**: the hash is over
//! `Approval.spec.approvalBytes`' own `plan_hash`, parsed from the **UTF-8
//! document text, verbatim, never base64**, and never from
//! `Approval.status.matchedKeyId` or any other status value.
//!
//! # `Drill` is a field
//!
//! `spec.target.mode: scratch` adds `--allowed-clusters` enforcement and
//! phase-9 teardown **inside the runner**; `newTopic` does not. Two kinds
//! would duplicate the exit-code-to-condition table for one boolean;
//! `scratch_mode_and_new_topic_mode_produce_the_same_job_shape` asserts the
//! two Jobs differ **only** in the plan ConfigMap's contents.
//!
//! # The credential is VALIDATED BY THE RUNNER, not by this reconciler
//!
//! Interface **I11**. `weirkeeper` holds **no `get` on Secrets anywhere**, so
//! it never sees the projected value. The runner checks it when it reads
//! `LOGWEIR_SOURCE_PASSWORD` / [`TARGET_PASSWORD_ENV`] and exits **3** with
//! `CredentialNotRenderable`; this module maps that refusal onto the terminal
//! state of the same name and does nothing else with it.
//!
//! `the_controller_performs_no_credential_check` asserts this file names
//! neither that predicate nor a namespaced Secret API. The test spells both
//! tokens itself, because `tests/linkage.rs::the_controller_never_reads_a_secret`
//! scans every line of `src/`, comments included.
//!
//! # The exit-3 discriminator is a key name in a bounded tail, not a position
//!
//! The runner puts `refusal-reason=<TerminalState>` on its final stdout line,
//! but a pod log merges stdout and stderr in nondeterministic order and the
//! pod log API has no stream selector. Every reader here therefore scans the
//! final [`super::backup::KEY_SCAN_TAIL_LINES`] non-empty lines and matches
//! **by key name**: [`super::backup::refusal_state`] for the refusal and
//! [`restore_evidence_keys`] for interface **I8**'s keys. A log body with no
//! refusal line at exit 3 yields
//! [`TERMINAL_STATE_GUARD_REFUSED_UNKNOWN_REASON`], never a guess.
//!
//! # What this module never does
//!
//! **It never parses a scorecard into a typed struct and re-emits it.** That
//! type silently drops fields it does not declare, so a re-emitted status
//! would disagree with the signed document. [`observe_scorecard`] copies the
//! values it needs verbatim out of a `serde_json::Value`, and
//! `the_controller_never_types_a_scorecard` asserts the type name appears
//! nowhere in this file, comments included.
//!
//! **It performs no signature verification.** `crate::verification` does; this
//! reconciler records the evidence keys and leaves
//! `status.evidence.verification` untouched.
//!
//! **It deletes nothing**: not the Job, not the pod, not an archive object. No
//! Logweir component holds a delete capability over the archive; the only
//! deletion a restore performs is phase 9's teardown of the scratch topics a
//! `mode: scratch` run created, inside the runner.

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
use kube::api::{Api, PostParams};
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

/// PROD-11.1a: a TTL clone's lifetime, cleanup Job and finalizer
/// (`controllers/restore_clone.rs`).
#[path = "restore_clone.rs"]
pub mod restore_clone;
#[path = "restore_interruption.rs"]
pub mod restore_interruption;
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
use crate::runner_log;
use crate::verification::{
    conditions_in, restore_badge, second_patch, stored_verification, verified_condition,
    EvidenceRef, VerifyOracle,
};
use logweir_core::approval_policy::{
    self as approval_policy, ApprovalPolicySet, AuthorizationRefusal, EffectivePolicy,
    ExpectedSubject, RestoreAuthorization, PAYLOAD_TYPE_RESTORE_AUTHORIZATION,
};
use logweir_core::check_contract::CheckCode;
use logweir_core::ids::sha256_prefixed;
use logweir_core::spec::RestoreSpec;
use logweir_store::{caps, Store};

/// The key in the plan ConfigMap carrying `spec.planBytes`, and the file name
/// [`runner_argv`] points `--spec` at.
///
/// The bytes are verbatim (interface **I20**): `plan_hash` binds exact bytes
/// and the API server normalises YAML, so a typed round-trip would produce a
/// document nobody signed. `the_plan_configmap_carries_the_spec_bytes_verbatim`
/// asserts byte equality.
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
/// The frozen approval-policy snapshot, at `/approval/approval-policy.json`:
/// exactly `logweir_core::approval_policy::ApprovalPolicy::snapshot_bytes`,
/// whose digest the signed authorization document v2 names. Present only in
/// the bundle of a Restore authorised by a v2 document.
pub const APPROVAL_POLICY_FILE: &str = "approval-policy.json";
/// The console's `ConsoleConfirmation` public key. Present only with
/// [`APPROVAL_POLICY_FILE`].
pub const CONFIRMATION_KEY_FILE: &str = "confirmation.pub.pem";
/// The policy digest the bundle was rendered under, beside the other binding
/// annotations.
pub const BUNDLE_APPROVAL_POLICY_DIGEST_ANNOTATION: &str = "logweir.dev/approval-policy-digest";
/// The runner flag naming [`APPROVAL_POLICY_FILE`] — the flag `logweir
/// restore run` reserved for the bundle-v2 policy snapshot.
pub const APPROVAL_POLICY_ARG: &str = "--policy-snapshot";
/// The runner flag naming [`CONFIRMATION_KEY_FILE`] — likewise reserved.
pub const CONFIRMATION_KEY_ARG: &str = "--confirmation-key";

/// The per-Restore immutable ConfigMap mounted at [`APPROVAL_MOUNT_PATH`].
#[must_use]
pub fn approval_bundle_config_map_name(restore_name: &str) -> String {
    format!("{restore_name}{APPROVAL_BUNDLE_SUFFIX}")
}

/// The environment variable the runner reads the TARGET SASL password from.
///
/// Mirrors `logweir::drill::TARGET_PASSWORD_VAR`, which this crate does not
/// link; a test keeps the two equal. The controller never reads it: it
/// projects a `secretKeyRef` (interface **I11**).
pub const TARGET_PASSWORD_ENV: &str = "LOGWEIR_TARGET_PASSWORD";

/// The default key within `KafkaCluster.spec.auth.secretRef` holding the SASL
/// password, projected when the connection names no
/// `auth.secretRef.passwordKey`.
///
/// The chart's own refusal quotes this line
/// (`crates/logweir/tests/chart_lint.rs`), and
/// `crates/weirkeeper/tests/connection.rs` asserts it equals
/// `crds::kafka_cluster::DEFAULT_PASSWORD_KEY`.
pub const TARGET_PASSWORD_SECRET_KEY: &str = "password";

/// `--out`: the scorecard's pod-local path, under
/// [`crate::job::WORK_MOUNT_PATH`] because the root filesystem is read-only.
/// The signed bytes also go to the evidence bucket; this is the local copy.
pub const SCORECARD_OUT_PATH: &str = "/work/scorecard.json";

/// `--offset-report-out`: where the engine writes its offset-mapping report.
/// The run uploads it beside the scorecard. Nothing applies it: no
/// consumer-group offset is committed on any cluster.
pub const OFFSET_REPORT_OUT_PATH: &str = "/work/offsets.json";

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

/// Interface **I8**'s fourth key line (guard **G-TS**). The runner prints
/// `topic-preflight=<one-line JSON object>` on a successful restore
/// (`logweir::drill::phase0_admit::TOPIC_PREFLIGHT_KEY_PREFIX`), and this
/// reconciler scans it by name out of the same bounded tail as the evidence
/// keys.
///
/// Scanned, never derived: the controller never dials a broker, so an absent
/// or unparseable line leaves `status.topicPreflight` absent.
pub const TOPIC_PREFLIGHT_KEY_PREFIX: &str = "topic-preflight=";

/// The runner's one line about a creation step that stopped
/// (`logweir::drill::phase0_admit::TARGET_TOPICS_APPEARED_KEY_PREFIX`),
/// printed with `failure-reason=TargetTopicAppeared` or `CreatedTopicsLeft` on
/// exit 1. One definition for the writer and this reader:
/// `logweir_core::creation_stop`.
pub use logweir_core::creation_stop::LINE_PREFIX as TARGET_TOPICS_APPEARED_KEY_PREFIX;

/// The most names one list of `status.targetTopicsAppeared` carries.
pub use logweir_core::creation_stop::MAX_NAMES as TARGET_TOPICS_APPEARED_MAX_NAMES;

/// `tailLines` on the terminal `pods/log` read: THE one terminal bound
/// ([`runner_log::TERMINAL`], FX-43), the same at every exit code. The one
/// line parsed as JSON is refused over
/// `logweir_core::creation_stop::MAX_VALUE_BYTES` before it is parsed.
pub use crate::runner_log::TERMINAL_LOG_TAIL_LINES;

/// What every surface says about a topic a stopped creation step created and
/// left — the runner's own sentence, from the one place both read.
pub use logweir_core::guard::LEFT_TOPIC_SENTENCE;

/// How long before an unfinished Job is looked at again. Fifteen seconds, as
/// on the `Backup` path: a Job's own events wake this controller, so the
/// requeue exists for the one transition no watch delivers.
pub const REQUEUE_SECS: u64 = 15;

/// **Interface I19.** How long before an `Approval` that is not yet
/// `Verified=True` is looked at again.
///
/// A `Restore` can exist before the `Approval` that authorises it, and an
/// approver may take an afternoon, so a dangling `spec.approvalRef` is a hold,
/// not a verdict: `phase: Pending` with one `Admitted=False` condition naming
/// [`REASON_APPROVAL_NOT_VERIFIED`]. Longer than a running Job's requeue
/// because nothing is in flight; shorter than the `Approval` controller's
/// because a human is waiting.
pub const ADMISSION_REQUEUE_SECS: u64 = 30;

/// The scorecard `outcome` that means the archive did not cover the requested
/// window.
///
/// It is not in the frozen 1.0.0 `Outcome` enum, so no document this build's
/// runner signs carries it. It is mapped anyway because this controller copies
/// `outcome` verbatim without checking it against that enum, and
/// `WindowNotCovered` in [`crate::conditions::TERMINAL_STATES`] would otherwise
/// have no producer.
pub const OUTCOME_FAIL_COVERAGE: &str = "fail-coverage";

/// What [`admit`] decided.
///
/// The set is closed: the API and console render the `reason` this produces,
/// and [`RestoreAdmission::reason`] is a `match` with no wildcard, so a new
/// variant fails to compile until someone names its reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RestoreAdmission {
    /// Every check passed. A Job may be created.
    Ok,
    /// The `Approval` `spec.approvalRef` names does not exist yet, or exists
    /// and is not `Verified=True`. Requeued, never terminal (interface
    /// **I19**, [`ADMISSION_REQUEUE_SECS`]).
    ApprovalNotVerified {
        /// The name `spec.approvalRef` gave.
        approval: String,
    },
    /// The ref names nothing: `spec.approvalRef.name` is empty.
    ///
    /// Terminal, because `spec` is sealed. It is a different fact from
    /// [`Self::ApprovalNotVerified`] ("you did not ask for authorisation", not
    /// "it has not arrived"), and one name for both would leave an operator
    /// waiting for an approval that will never be looked for.
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
    /// The Approval binds this Restore, but its signed approval subject is not
    /// the one the plan needs: an ordinary approval for a restore under the
    /// original topic names (which needs `originalName`), the reverse, or a
    /// subject this build does not know.
    ///
    /// Terminal, under the reason `ApprovalSubjectMismatch` with its own
    /// message: both specs are sealed, so only a new Approval and a new
    /// Restore can proceed.
    ApprovalSubjectNotThePlans {
        /// The referenced Approval name.
        approval: String,
        /// Both subjects, from `logweir_core::original_name::check_approval_subject`.
        detail: String,
    },
    /// `sha256_prefixed(spec.planBytes)` is not the `plan_hash` inside
    /// `Approval.spec.approvalBytes`.
    ///
    /// Terminal, naming both hashes: the approval binds bytes, and both specs
    /// are immutable, so re-approving the exact plan is the only way forward.
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
    /// The `Restore` carries `spec.authorization`, a standing (rehearsal)
    /// authorization. Those are withdrawn in v1, so such a `Restore` is refused
    /// by name before any `Approval` is read and before anything is created.
    ///
    /// Terminal: `spec` is sealed. The field stays in the schema so objects
    /// written before v1 still parse.
    StandingAuthorizationRefused {
        /// The `Approval` `spec.authorization.approvalRef` names.
        approval: String,
    },
    /// The Approval's authorization does not match the namespace's current
    /// approval-policy binding. Terminal.
    AuthorizationPolicyMismatch {
        /// The Approval `spec.approvalRef` names.
        approval: String,
        /// Both policies, or the format the binding refuses.
        detail: String,
    },
    /// The authorization document v2 expired before admission. Terminal.
    AuthorizationExpired {
        /// The Approval `spec.approvalRef` names.
        approval: String,
        /// The instant and the clock.
        detail: String,
    },
}

impl RestoreAdmission {
    /// The condition `reason` this admission writes. No wildcard arm; see the
    /// type's own note.
    #[must_use]
    pub const fn reason(&self) -> &'static str {
        match self {
            Self::Ok => REASON_ADMITTED,
            Self::ApprovalNotVerified { .. } => REASON_APPROVAL_NOT_VERIFIED,
            Self::ApprovalNotReceived { .. } => TERMINAL_STATE_APPROVAL_NOT_RECEIVED,
            Self::ApprovalSubjectMismatch { .. } | Self::ApprovalSubjectNotThePlans { .. } => {
                TERMINAL_STATE_APPROVAL_SUBJECT_MISMATCH
            }
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
    /// Only [`Self::ApprovalNotVerified`] is a hold: its fact can change without
    /// anybody touching this object, so it requeues at
    /// [`ADMISSION_REQUEUE_SECS`] (interface **I19**). The others cannot change,
    /// because `Restore.spec` and `Approval.spec` are sealed.
    /// `ClusterNotReachable` is terminal by ruling even though
    /// `status.reachable` is refreshed: an approval binds a plan to a target,
    /// and a target that was down at admission should be re-authorised, not
    /// started by itself hours later. Either way the object gets a status; a
    /// bare requeue over an immutable spec would leave it with none.
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        match self {
            Self::Ok | Self::ApprovalNotVerified { .. } => false,
            Self::ApprovalNotReceived { .. }
            | Self::ApprovalSubjectMismatch { .. }
            | Self::ApprovalSubjectNotThePlans { .. }
            | Self::PlanHashMismatch { .. }
            | Self::ClusterNotReachable { .. }
            | Self::StandingAuthorizationRefused { .. }
            | Self::AuthorizationPolicyMismatch { .. }
            | Self::AuthorizationExpired { .. } => true,
        }
    }
}

/// The field an ordinary `Restore` names its `Approval` by.
pub const APPROVAL_REF_FIELD: &str = "spec.approvalRef";

impl RestoreAdmission {
    /// The sentences, with the approval field as a parameter.
    fn write_sentence(&self, f: &mut fmt::Formatter<'_>, field: &str) -> fmt::Result {
        match self {
            Self::Ok => write!(
                f,
                "the approval is Verified=True, the recomputed plan hash matches the hash inside \
                 its own signed bytes, and the target cluster reports reachable"
            ),
            Self::ApprovalNotVerified { approval } => write!(
                f,
                "{field} names the Approval `{approval}`, which does not exist yet or is \
                 not Verified=True; no Job is created until it is, and this object is looked at \
                 again in {ADMISSION_REQUEUE_SECS}s (interface I19)"
            ),
            Self::ApprovalNotReceived { approval } => write!(
                f,
                "{field}.name is `{approval}` — it names nothing, so no Approval can \
                 ever bind to this Restore; spec is immutable, so create a new Restore that names \
                 one"
            ),
            Self::ApprovalSubjectMismatch { approval, detail } => write!(
                f,
                "{field} names Approval `{approval}`, but its verified subject binding \
                 does not identify this Restore ({detail}); create a new Approval for this exact \
                 Restore name, namespace, and UID"
            ),
            Self::ApprovalSubjectNotThePlans { approval, detail } => write!(
                f,
                "{field} names Approval `{approval}`, whose signed approval subject is \
                 not the one this Restore's plan needs ({detail}); no Job was created. spec is \
                 immutable: create a new Restore and an Approval signed for its subject"
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
            Self::StandingAuthorizationRefused { approval } => write!(
                f,
                "spec.authorization names the standing Approval `{approval}`, and standing \
                 authorizations are withdrawn in v1 (scheduled rehearsals with them), so this \
                 Restore is refused and no Job is created. spec is immutable: create a new \
                 Restore authorised by a per-run Approval"
            ),
            Self::AuthorizationPolicyMismatch { approval, detail } => write!(
                f,
                "{field} names Approval `{approval}`, whose authorization does not match \
                 this namespace's approval policy ({detail}); no Job is created. Both specs are \
                 immutable: create a new Restore, which is confirmed or approved under the policy \
                 bound now"
            ),
            Self::AuthorizationExpired { approval, detail } => write!(
                f,
                "{field} names Approval `{approval}`, whose authorization expired before \
                 this Restore was admitted ({detail}); no Job is created. Create a new Restore"
            ),
        }
    }
}

/// The sentence a status carries; it names [`APPROVAL_REF_FIELD`].
impl fmt::Display for RestoreAdmission {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.write_sentence(f, APPROVAL_REF_FIELD)
    }
}

/// The `plan_hash` inside an approval's own signed bytes.
///
///
/// Interface **I18**: `approvalBytes` is document text, never base64. The
/// bytes go to `serde_json` with no decode step, exactly as
/// [`super::approval::evaluate`] reads them.
///
/// `None` when the bytes are not a JSON object or carry no string
/// `plan_hash`, which is reported as a mismatch naming what the document
/// carried.
///
/// An authorization document v2 spells it `planHash`. The spelling is decided
/// by the sidecar's payload type, which the signature covers, and never by
/// trying both, so a v1 document that also carried `planHash` cannot present
/// a second hash.
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

/// The approval subject the signed document carries: v1's
/// `approval_subject`, v2's `approvalSubject`, the spelling decided by the
/// sidecar's payload type as in [`approval_plan_hash`]. An absent key is an
/// ordinary approval.
///
/// # Errors
///
/// A document that does not parse as JSON, or a subject this build does not
/// know ([`ApprovalSubject::from_wire`]).
pub fn approval_subject_of(
    approval: &Approval,
) -> Result<logweir_core::original_name::ApprovalSubject, String> {
    let field = if is_authorization_v2(approval) {
        "approvalSubject"
    } else {
        "approval_subject"
    };
    let doc = serde_json::from_str::<Value>(&approval.spec.approval_bytes)
        .map_err(|e| format!("the approval document is not JSON: {e}"))?;
    match doc.get(field) {
        None | Some(Value::Null) => Ok(logweir_core::original_name::ApprovalSubject::Ordinary),
        Some(Value::String(s)) => {
            logweir_core::original_name::ApprovalSubject::from_wire(Some(s.as_str()))
        }
        Some(other) => Err(format!(
            "the approval document's {field} is {other}, not a string"
        )),
    }
}

/// Whether an Approval is a one-person confirmation (an authorization
/// document v2 signed under an `Ordinary` policy), and the typed topic names
/// its signed bytes carry. A v1 approval and a `Governed` document have a
/// second person and carry none.
///
/// # Errors
///
/// A v2 document that does not parse (the admission's signature and binding
/// checks refuse it too).
pub fn typed_confirmation_of(
    approval: &Approval,
) -> Result<
    (
        bool,
        Option<logweir_core::original_name::OriginalNameConfirmation>,
    ),
    String,
> {
    if !is_authorization_v2(approval) {
        return Ok((false, None));
    }
    let doc = logweir_core::approval_policy::RestoreAuthorization::from_bytes(
        approval.spec.approval_bytes.as_bytes(),
    )
    .map_err(|e| e.to_string())?;
    Ok((
        doc.authorization_mode == logweir_core::approval_policy::ApprovalMode::Ordinary,
        doc.original_name_confirmation,
    ))
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

/// What [`admit_with_policy`] needs to judge an approval policy.
pub struct PolicyAdmission<'a> {
    /// What the Restore's namespace is bound to NOW.
    pub policy: &'a EffectivePolicy,
    /// The clock, passed in.
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

/// The approval policy, re-checked against the namespace's current binding
/// immediately before any Job exists.
///
/// The signatures are the Approval controller's (and the runner's). This
/// re-derives from the signed bytes what depends on the binding and the
/// clock, which a policy rollout or the passage of time can change after the
/// Approval was verified.
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
        kind: logweir_core::approval_policy::SUBJECT_KIND_RESTORE.to_string(),
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
    // The controller-written provenance must say the Approval controller
    // verified under this policy too, so a verdict from before a policy
    // rollout is never admitted.
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
/// The same function [`super::approval::evaluate`] uses, so the two halves
/// cannot derive one plan's identity two ways.
#[must_use]
pub fn recomputed_plan_hash(restore: &Restore) -> String {
    sha256_prefixed(restore.spec.plan_bytes.as_bytes())
}

/// Decide whether this `Restore` may have a Job, **before one exists**.
///
/// Recomputes `sha256_prefixed(spec.planBytes)` and compares it against the
/// `Approval`'s own `plan_hash`, parsed from `spec.approvalBytes`. **Never
/// reads `Approval.status.matchedKeyId` as proof of the document hash**
/// (interface **I18**); status is used only for the controller-produced
/// verified flag and exact subject provenance.
///
/// # The order
///
/// 0. A `Restore` carrying `spec.authorization` (withdrawn in v1) →
///    [`RestoreAdmission::StandingAuthorizationRefused`], terminal.
/// 1. `spec.approvalRef` must name something → else
///    [`RestoreAdmission::ApprovalNotReceived`], terminal.
/// 2. That `Approval` must exist and its immutable subject plus sticky verified
///    provenance must identify this exact Restore name, namespace and UID →
///    else [`RestoreAdmission::ApprovalSubjectMismatch`], terminal.
/// 3. That exact Approval must be `status.verified == Some(true)` → else
///    [`RestoreAdmission::ApprovalNotVerified`], requeued at
///    [`ADMISSION_REQUEUE_SECS`]. Before the hash, because the hash inside
///    unverified bytes is a claim nobody signed for.
/// 4. The recomputed hash must equal the approval document's own → else
///    [`RestoreAdmission::PlanHashMismatch`], terminal, naming both.
/// 5. `spec.target.clusterRef` must resolve to a `KafkaCluster` with
///    `status.reachable == Some(true)` → else
///    [`RestoreAdmission::ClusterNotReachable`]. Last, because it is the only
///    check about the world rather than the documents.
///
/// Pure: no clock, no client, creates nothing. An unapproved plan creates
/// nothing because the caller runs this before its first `POST`.
#[must_use]
pub fn admit(
    restore: &Restore,
    approval: Option<&Approval>,
    cluster: Option<&KafkaCluster>,
) -> RestoreAdmission {
    admit_with_policy(
        restore,
        parse_plan(restore).ok().as_ref(),
        approval,
        cluster,
        None,
    )
}

/// [`admit`], under the namespace's approval-policy binding.
///
/// `policy: None` is `legacy-governed-v1` with no clock, which is exactly
/// [`admit`]. With a policy, step 3b runs between "the exact Approval is
/// Verified=True" and the plan hash. `plan`: [`parse_plan`]'s value; `None`
/// (bytes that do not parse, which the reconcile has refused) is an ordinary
/// subject naming no topics.
#[must_use]
pub fn admit_with_policy(
    restore: &Restore,
    plan: Option<&RestoreSpec>,
    approval: Option<&Approval>,
    cluster: Option<&KafkaCluster>,
    policy: Option<&PolicyAdmission<'_>>,
) -> RestoreAdmission {
    // 0. A standing authorization is refused by name, first. The CEL rule
    // `has(self.approvalRef) != has(self.authorization)` means such an object
    // has no `spec.approvalRef`, so it would otherwise fall to step 1 as
    // `ApprovalNotReceived`.
    if let Some(authorization) = restore.spec.authorization.as_ref() {
        return RestoreAdmission::StandingAuthorizationRefused {
            approval: authorization.approval_ref.name.clone(),
        };
    }

    // 1. The ref must name something.
    let referent = restore.spec.approval_ref_name().trim().to_string();
    if referent.is_empty() {
        return RestoreAdmission::ApprovalNotReceived {
            approval: restore.spec.approval_ref_name().to_string(),
        };
    }

    // 2. The referenced object must be this exact subject.
    let Some(approval) = approval else {
        return RestoreAdmission::ApprovalNotVerified { approval: referent };
    };

    // A plan hash is not a subject identity. The Approval controller records
    // the exact referent UID it read, and this side checks the whole binding
    // before accepting its cached verdict. Immutable subject mismatches are
    // checked even after Verified was revoked, so a recreated UID gets a
    // terminal status instead of a permanent generic hold. A legacy status
    // without provenance is still held for refresh.
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

    // 3. That exact Approval must be Verified=True. Two of its refusals are
    // permanent (the document expired, or was issued under another policy),
    // and both specs are immutable, so they end the Restore instead of
    // holding it for ever.
    if status.and_then(|status| status.verified) != Some(true) || bound.is_none() {
        if policy.is_some() {
            if let Some(permanent) = permanent_approval_refusal(approval, &referent) {
                return permanent;
            }
        }
        return RestoreAdmission::ApprovalNotVerified { approval: referent };
    }

    // 3b. The approval policy, re-checked now.
    let legacy = PolicyAdmission {
        policy: &EffectivePolicy::Legacy,
        now: DateTime::<Utc>::MIN_UTC,
    };
    if let Some(refusal) = policy_refusal(restore, approval, &referent, policy.unwrap_or(&legacy)) {
        return refusal;
    }

    // 4. The plan hash, recomputed, from inside the signed bytes.
    let recomputed = recomputed_plan_hash(restore);
    let approval_says = approval_plan_hash(approval).unwrap_or_default();
    if recomputed != approval_says {
        return RestoreAdmission::PlanHashMismatch {
            recomputed,
            approval_says,
        };
    }

    // 4b. The approval subject, from inside the signed bytes. A restore under
    // the original topic names needs an `originalName` approval, which
    // authorises nothing else. After the plan hash, so the subject is inside
    // bytes that bind this plan; before the target, because it never changes.
    // The runner checks it again. Read from the plan, which
    // `original_name_agrees` has already held the object's declaration to.
    let subject_refusal = approval_subject_of(approval).and_then(|approved| {
        logweir_core::original_name::check_approval_subject(
            plan.map_or(
                logweir_core::original_name::ApprovalSubject::Ordinary,
                logweir_core::original_name::ApprovalSubject::of_plan,
            ),
            approved,
        )?;
        // A one-person confirmation of an original-name plan carries every
        // original topic name re-typed, exactly, in its signed bytes.
        let (one_person, confirmation) = typed_confirmation_of(approval)?;
        logweir_core::original_name::check_typed_confirmation(
            plan.map_or(&[][..], |plan| plan.source.topics.as_slice()),
            approved,
            one_person,
            confirmation.as_ref(),
        )
    });
    if let Err(detail) = subject_refusal {
        return RestoreAdmission::ApprovalSubjectNotThePlans {
            approval: referent,
            detail,
        };
    }

    // 5. The target must report reachable.
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

/// The plan ConfigMap, with exactly one key, whose value is `spec.planBytes`
/// **byte for byte**.
///
/// Pure, so the object a test builds is byte-identical to the one the
/// reconciler `POST`s. No typed round trip, `trim` or re-indent: `plan_hash`
/// binds these exact bytes (see [`PLAN_SPEC_KEY`]).
///
/// `ownerReferences` with `controller: true` and `blockOwnerDeletion: true`:
/// deleting the `Restore` garbage-collects its plan, and it makes the 409 case
/// decidable (see [`crate::conditions::TERMINAL_STATE_PLAN_CONFIG_MAP_CONFLICT`]).
///
/// # Errors
///
/// [`RestoreError`] for an object with no namespace or UID.
pub fn plan_config_map(restore: &Restore) -> Result<ConfigMap, RestoreError> {
    plan_config_map_with_destinations(restore, None)
}

/// [`plan_config_map`], with the two destinations' CA bundles beside the plan.
///
/// The CA bytes are frozen here and never re-read at Job time:
/// `LOGWEIR_ARCHIVE_CA_FILE` and `LOGWEIR_EVIDENCE_CA_FILE` point at keys in
/// this immutable `ConfigMap`, not at the destination's live `caBundle`, so a
/// CA rotated mid-run cannot change what that run trusts. The plan bytes stay
/// verbatim at both arities.
///
/// # Errors
///
/// [`RestoreError`] for an object with no namespace or UID, or a CA bundle
/// that is not UTF-8.
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
/// Not `from_utf8_lossy`: a mangled trust root would fail only at the first
/// TLS handshake. `destination::check_ca_bundle` has already refused anything
/// that is not PEM; this second rail names the object.
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

/// The two destinations a destination-backed `Restore` resolves.
///
/// A restore reads the source destination's archive under `archiveRead` and
/// writes its scorecard and offset report to the evidence destination under
/// `evidenceWrite`. They are often the same `BackupDestination` (one
/// credential, `LOGWEIR_EVIDENCE_CREDENTIALS=archive`); when they are two, they
/// are resolved independently and no field of one reaches the other.
#[derive(Debug)]
pub struct RestoreDestinations {
    /// `spec.sourceDestinationRef`, resolved for
    /// [`DestinationRole::ArchiveRead`].
    pub source: ResolvedDestination,
    /// `spec.evidenceDestinationRef`, resolved for
    /// [`DestinationRole::EvidenceWrite`].
    pub evidence: ResolvedDestination,
}

/// What destination admission decided for one `Restore`.
#[derive(Debug)]
pub enum RestoreDestinationAdmission {
    /// The `Restore` names neither ref: a legacy inline-`sourceArchive` run,
    /// unchanged in every respect.
    NotRequested,
    /// Both destinations resolved, and the approved plan bytes name exactly
    /// their locations.
    Resolved(Box<RestoreDestinations>),
    /// One of them is absent or not yet `Valid`. Held, never terminal, like
    /// `ApprovalNotVerified`: the operator may still be creating it, and a
    /// sealed `Restore.spec` could never repair a terminal refusal in place.
    Holding {
        /// The resolver's `CheckCode`, as a condition `reason`.
        reason: &'static str,
        /// What is wrong and what to do. Names objects, fields and locations —
        /// never a credential.
        message: String,
    },
}

/// Resolve the two `BackupDestination`s a `Restore` names and check the
/// approved plan against them.
///
/// 5. Both refs resolve and are `Valid`; a hold while they are not.
/// 6. `spec.planBytes` parses as a `RestoreSpec`, read only and never
///    re-emitted.
/// 7. `plan.source.storage` is the source destination's archive `StorageUrl`,
///    exact on all six fields.
/// 8. `plan.evidence` is the evidence destination's evidence `StorageUrl`.
/// 9. The `archiveRead` and `evidenceWrite` grants resolve, and one pod (one
///    ServiceAccount) can satisfy both.
///
/// Checks 7 and 8 matter because the runner reads the location from the plan,
/// so a destination whose location differs would lend its credentials to a
/// location nobody approved. Refusing is terminal. A destination edit never
/// invalidates an approval: location and transport are immutable on a
/// `BackupDestination`.
///
/// # Errors
///
/// [`RestoreError::Api`] for a failed read, or [`RestoreError::Refused`] with
/// the terminal state named by checks 6 through 9.
pub async fn admit_restore_destinations(
    restore: &Restore,
    plan: Option<&RestoreSpec>,
    client: &kube::Client,
    namespace: &str,
    now: DateTime<Utc>,
) -> Result<RestoreDestinationAdmission, RestoreError> {
    let source_ref = restore.spec.source_destination_ref.as_ref();
    let evidence_ref = restore.spec.evidence_destination_ref.as_ref();
    let (source_ref, evidence_ref) = match (source_ref, evidence_ref) {
        (None, None) => return Ok(RestoreDestinationAdmission::NotRequested),
        (Some(s), Some(e)) => (s, e),
        // The CEL rule (`crds::restore::DESTINATION_PAIR_RULE`) refuses this
        // pair on admission; an object admitted under an older CRD revision
        // reaches this controller unchecked.
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
        // Both destinations: the engine child reads the archive and the same
        // pod writes the scorecard. See `backup::ENGINE_CUSTOM_CA_VERIFIED`.
        if resolved.ca_bundle.is_some() && !backup::engine_custom_ca_allowed(policy) {
            return Err(RestoreError::Refused(
                CheckCode::CaBundleUnsupportedByEngine.as_str(),
                backup::engine_custom_ca_refusal(&resolved.namespace, &resolved.name),
            ));
        }
    }

    // CHECK 6. The reconcile's read of the plan, or one made here.
    let parsed;
    let plan = match plan {
        Some(plan) => plan,
        None => {
            parsed = parse_plan(restore)?;
            &parsed
        }
    };

    // The withdrawn `notifications` block: a plan that names a sink is refused
    // by name before any Job exists, in the runner's own words. The runner
    // refuses it again at phase 0, which is where an inline Restore meets it.
    if let Err(refusal) = logweir_core::guard::reject_withdrawn_notifications(&plan.notifications) {
        return Err(RestoreError::Refused(
            logweir_core::guard::TERMINAL_STATE_GUARD_REFUSED,
            refusal.0,
        ));
    }

    // CHECK 7, exact on all six fields.
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

    // CHECK 8. The resolver imposes the `logweir/` evidence root, so this
    // compares the bucket the scorecard lands in.
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

    // CHECK 9. `evidence_env` decides how two grants share a pod, and refuses
    // two different workload identities as `ExecutionContextConflict`.
    if let Err(refusal) = evidence.evidence_env(&source) {
        return Err(RestoreError::Refused(refusal.reason(), refusal.message));
    }

    Ok(RestoreDestinationAdmission::Resolved(Box::new(
        RestoreDestinations { source, evidence },
    )))
}

/// A resolver refusal, split: a hold for `DestinationNotFound` and
/// `DestinationNotValid`, which are races between two applies and the
/// destination's own reconcile, and a terminal refusal for every decision,
/// because a decision retried forever is a decision nobody sees.
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
/// Not `{:?}`: a `PlanDestinationMismatch` message is read by someone
/// comparing two locations field by field. `StorageUrl` carries no credential
/// at any variant, so nothing needs redaction.
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
/// The refusal names the thing to edit: "absent from the TrustRoster" is the
/// wrong sentence for a namespace governed by `org-default`, and left the
/// operator with no way out of the hold.
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
/// # It reads the resolved trust, as admission does
///
/// The two must not disagree. A `GovernedApproval` key that lives only in a
/// `TrustPolicy` writes `Verified=True` on the `Approval`; a bundle that read
/// only `TrustRoster/default` would then leave the `Restore` in a
/// non-terminal `ApprovalBundleMaterializationFailed` hold for ever.
///
/// # The expiry re-check is `may_sign_new`, not `status.expiredKeyIds`
///
/// `expiredKeyIds` derives from `notAfter` alone; the question here is *may
/// this key authorise something new*, which is also false for a `Retired` or
/// `Revoked` key. A bundle is the last thing written before a runner executes
/// under that key, so the weaker question would mount a key an administrator
/// withdrew between admission and materialization. `now` is an argument: this
/// function reads no clock.
pub fn approval_bundle_config_map(
    restore: &Restore,
    approval: &Approval,
    trust: &crate::trust::ResolvedTrust,
    now: DateTime<Utc>,
) -> Result<ConfigMap, RestoreError> {
    approval_bundle_config_map_with_policy(
        restore,
        &parse_plan(restore)?,
        approval,
        trust,
        &EffectivePolicy::Legacy,
        now,
    )
}

/// [`approval_bundle_config_map`], under the namespace's approval-policy
/// binding (bundle contract v2).
///
/// For a Restore authorised by authorization document v2 the bundle carries
/// two more public members: [`CONFIRMATION_KEY_FILE`], the console key that
/// attested the requester, and [`APPROVAL_POLICY_FILE`], the frozen policy
/// snapshot whose digest the signed document names. `approver.pub.pem` holds
/// the key that authorised the run (the governed approver's under `Governed`,
/// the console's under `Ordinary`). Each key must still be one this
/// namespace's trust lets sign something new under its usage, so a key
/// withdrawn between verification and materialization is never mounted.
///
/// # Errors
///
/// [`RestoreError::Materialization`] when a key is gone or withdrawn, and
/// [`RestoreError::Refused`] on a plan-hash mismatch.
pub fn approval_bundle_config_map_with_policy(
    restore: &Restore,
    plan: &RestoreSpec,
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
    // The usage the authorising key must still hold is the bound policy's: a
    // governed approver's, or the console's (under `Ordinary`, and under a
    // `Governed` policy whose approval the console signs).
    let bound = policy.bound().filter(|_| is_authorization_v2(approval));
    // The bound policy's row says which usage authorises; a pair that is not a
    // row materialises nothing.
    let route = bound
        .map(logweir_core::approval_policy::ApprovalPolicy::route)
        .transpose()
        .map_err(|refusal| {
            RestoreError::Materialization(format!(
                "the Approval {} is under a policy this build runs nothing under ({refusal}); no \
                 bundle is written",
                approval.name_any()
            ))
        })?;
    let console_approved =
        route == Some(logweir_core::approval_policy::ApprovalRoute::SecondPersonInConsole);
    let authorising_usage = route.map_or(
        logweir_core::trust::KeyUsage::GovernedApproval,
        logweir_core::approval_policy::ApprovalRoute::authorising_usage,
    );
    if let Err(refusal) = trust.may_sign_new_for(matched_key_id, authorising_usage, now) {
        return Err(RestoreError::Materialization(format!(
            "the Approval {} verified under key {matched_key_id}, which {} no longer accepts for \
             a new authorisation ({}); no bundle is written and nothing executes under it",
            approval.name_any(),
            trust_source_phrase(trust),
            refusal.as_str()
        )));
    }
    // The console key, under v2, re-checked for the same reason.
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
            // Under a console-approved policy the console's signature is the
            // approval, so it is verified again here, the last thing before a
            // runner is given anything: against the console key this
            // namespace's trust carries now, over the exact bytes the bundle
            // mounts. A personal-key policy's second signature is verified by
            // the Approval controller and the runner.
            if console_approved {
                if matched_key_id != confirmation_key_id {
                    return Err(RestoreError::Materialization(format!(
                        "the Approval {} is under policy {}, which takes its approval from the \
                         console, and it verified under key {matched_key_id}, which is not the \
                         console key {confirmation_key_id} that confirmed it; no bundle is written",
                        approval.name_any(),
                        bound.name
                    )));
                }
                let verified =
                    serde_json::from_str::<logweir_verify::Sidecar>(&approval.spec.sidecar_bytes)
                        .map_err(|e| e.to_string())
                        .and_then(|sidecar| {
                            let key = logweir_verify::VerifyingKey::from_pem_str(
                                &confirmation_key.spki_pem,
                            )
                            .map_err(|e| e.to_string())?;
                            logweir_verify::verify_detached(
                                &key,
                                PAYLOAD_TYPE_RESTORE_AUTHORIZATION,
                                approval.spec.approval_bytes.as_bytes(),
                                &sidecar,
                            )
                            .map_err(|e| e.to_string())
                        });
                if let Err(reason) = verified {
                    return Err(RestoreError::Materialization(format!(
                        "the Approval {} is under policy {}, which takes its approval from the \
                         console, and the console key {confirmation_key_id} does not verify its \
                         signature over the approval's bytes ({reason}); no bundle is written and \
                         nothing executes under it",
                        approval.name_any(),
                        bound.name
                    )));
                }
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
    // `allowedTargetClusterIds`, which the legacy synthesis carries verbatim
    // from the roster's `allowedClusterIds`.
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

    // The evidence keyring, exactly when the plan binds a point.
    data.extend(evidence_member(plan, trust)?);

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
    plan: &RestoreSpec,
    trust: &crate::trust::ResolvedTrust,
) -> Result<Option<(String, String)>, RestoreError> {
    if !plan_binds_point(plan) {
        return Ok(None);
    }
    Ok(Some((
        EVIDENCE_KEYS_FILE.to_string(),
        evidence_keyring_bytes(trust)?,
    )))
}

/// The ConfigMap shape of an approval bundle.
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

/// The evidence-signing keyring member. Present exactly when the plan binds a
/// recovery point (`source.point`); the runner verifies that point's receipt
/// signature against it before any client is constructed, and refuses a
/// point-bound plan without it.
pub const EVIDENCE_KEYS_FILE: &str = "evidence-keys.json";

/// Whether this `Restore`'s plan binds a recovery point (`source.point`).
///
/// One predicate decides the bundle member, its pinned digest, the runner flag
/// and the projected file, so the four cannot disagree.
#[must_use]
pub fn plan_binds_point(plan: &RestoreSpec) -> bool {
    plan.source.point.is_some()
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
    execution_contract_env_with_policy(
        restore,
        &parse_plan(restore)?,
        approval,
        trust,
        &EffectivePolicy::Legacy,
        now,
    )
}

/// [`execution_contract_env`], under the namespace's approval-policy binding.
/// A v2 bundle's two extra members are pinned too, as
/// `POLICY_SNAPSHOT_SHA256_ENV` and `CONFIRMATION_KEY_SHA256_ENV`.
///
/// # Errors
///
/// As [`execution_contract_env`].
pub fn execution_contract_env_with_policy(
    restore: &Restore,
    plan: &RestoreSpec,
    approval: &Approval,
    trust: &crate::trust::ResolvedTrust,
    policy: &EffectivePolicy,
    now: DateTime<Utc>,
) -> Result<Vec<(String, String)>, RestoreError> {
    use logweir_core::execution_contract as contract;

    let bundle =
        approval_bundle_config_map_with_policy(restore, plan, approval, trust, policy, now)?;
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
        // Present exactly when the bundle carries the members, so a v1
        // bundle's environment is unchanged.
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

/// The unexpired approver key ids the roster names (interface **I16**).
///
/// `roster.spec.approverKeys[].keyId` minus `roster.status.expiredKeyIds`, in
/// roster order. Expiry is read from the status the `TrustRoster` reconciler
/// wrote rather than recomputed here: that reconciler holds the one clock
/// this decision is made against, and a second clock is how one component
/// comes to accept a key the other rejects.
///
/// A `None` roster yields an empty list rather than an error: the `Approval`
/// reconciler has already refused everything with `RosterNotFound`, so no
/// `Restore` is admitted without one.
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
/// `Restore.spec` has no `triggeredBy` field and `.spec` is sealed, so the
/// value is derived here. `--triggered-by` is copied verbatim into the signed
/// scorecard's `triggered_by`, which an auditor reads to learn why the run
/// happened. A restore's reason is its authorisation: naming the `Approval`
/// puts the approver, the ticket and the matched key id one
/// `kubectl get approval` away from the signed document.
#[must_use]
pub fn triggered_by(restore: &Restore) -> String {
    let referent = restore.spec.approval_ref_name().trim();
    if referent.is_empty() {
        // Unreachable through `reconcile_restore` (`admit` refuses an empty
        // ref first); named so a future caller cannot produce `approval/`.
        return "approval/<none>".to_string();
    }
    format!("approval/{referent}")
}

/// The runner argv for `restore`, built here and consumed by nothing else.
///
/// Every path is a mount path, never a default: `--spec` is
/// [`PLAN_MOUNT_PATH`]/[`PLAN_SPEC_KEY`]; the approval-bundle files are under
/// [`APPROVAL_MOUNT_PATH`]; the signing key is
/// [`SIGNING_MOUNT_PATH`]/[`SIGNING_KEY_FILE`]; `--out` and
/// `--offset-report-out` are under [`crate::job::WORK_MOUNT_PATH`], the only
/// writable volume.
///
/// `--approver-key-ids` is repeated, one flag per id (interface **I16**). A
/// roster with no unexpired approver key contributes no flags rather than an
/// empty value, which would be a key id nothing matches.
#[must_use]
pub fn runner_argv(
    restore: &Restore,
    plan: &RestoreSpec,
    approver_key_ids: &[String],
) -> Vec<String> {
    let mut argv: Vec<String> = vec![
        "restore".to_string(),
        "run".to_string(),
        logweir_core::execution_contract::VERSION_ARG.to_string(),
        logweir_core::execution_contract::VERSION.to_string(),
        "--spec".to_string(),
        format!("{PLAN_MOUNT_PATH}/{PLAN_SPEC_KEY}"),
        "--approval".to_string(),
        format!("{APPROVAL_MOUNT_PATH}/{APPROVAL_DOC_FILE}"),
    ];
    argv.extend([
        "--approver-key".to_string(),
        format!("{APPROVAL_MOUNT_PATH}/{APPROVER_KEY_FILE}"),
    ]);
    for id in approver_key_ids {
        argv.push("--approver-key-ids".to_string());
        argv.push(id.clone());
    }
    // A point-bound plan's receipt signature is verified against the mounted
    // evidence keyring before any client exists.
    if plan_binds_point(plan) {
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

/// The binding pairs an inline `sourceArchive.secretRef` contributes to a
/// `Restore` Job, over the approved plan's `source.storage` and `evidence:`
/// locations as the runner will dial them
/// ([`crate::destination::inline_archive_binding_env`],
/// [`crate::destination::effective_inline_storage`]).
#[must_use]
pub fn inline_restore_binding_env(
    secret: &str,
    plan: &RestoreSpec,
    addressing: &[(String, String)],
) -> crate::destination::DestinationEnv {
    crate::destination::inline_archive_binding_env(
        secret,
        &crate::destination::effective_inline_storage(&plan.source.storage, addressing),
        Some(&crate::destination::effective_inline_storage(
            &plan.evidence,
            addressing,
        )),
    )
}

/// The [`RunnerJobSpec`] one `Restore` produces.
///
/// Pure, so the Job a test builds is byte-identical to the one the reconciler
/// `POST`s.
///
/// The Job shape does not depend on `spec.target.mode`
/// (`scratch_mode_and_new_topic_mode_produce_the_same_job_shape`). The mode's
/// differences are all decisions the runner makes from the plan, which the
/// approver signed; a different Job for `scratch` would put part of the mode's
/// meaning where the approval does not cover it.
///
/// # Errors
///
/// [`RestoreError`] when the object carries no namespace or UID, and
/// [`RestoreError::Refused`] when the target connection does not resolve or the
/// approved plan names a different target than it ([`crate::connection`]),
/// before any ConfigMap or Job exists.
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

/// [`runner_job_spec`] for a destination-backed `Restore`.
///
/// The engine reads the archive through `AWS_*`, the source destination's
/// `archiveRead` grant ([`ResolvedDestination::job_env`]). The scorecard is
/// written through the evidence destination's `evidenceWrite` grant; when that
/// is a different Secret it arrives under `LOGWEIR_EVIDENCE_AWS_*`, because one
/// `AWS_ACCESS_KEY_ID` in a pod is one credential.
/// [`ResolvedDestination::evidence_env`] owns the cases and the refusal.
///
/// `archive_addressing_env` is not forwarded on this path: the engine's
/// `from_env()` honours the controller's `AWS_ENDPOINT_URL`, `AWS_REGION`,
/// `AWS_ALLOW_HTTP` and `AWS_VIRTUAL_HOSTED_STYLE_REQUEST` over the approved
/// plan, so a planted `AWS_ALLOW_HTTP=true` could enable plaintext for a
/// destination that declares TLS. The complete set comes from the two
/// destinations. The legacy arm keeps forwarding (see
/// [`desired_execution_inputs_for_destination`]).
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
        &parse_plan(restore)?,
        cluster,
        approver_key_ids,
        approval,
        trust,
        &EffectivePolicy::Legacy,
        now,
        destinations,
    )
}

/// Whether the Job this Restore gets carries the v2 authorization members (an
/// authorization document v2 under an explicit binding). The one predicate the
/// argv, the mount table and the environment all read.
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
/// binding. A v2-authorised Restore's Job mounts the two extra bundle members,
/// passes [`APPROVAL_POLICY_ARG`] and [`CONFIRMATION_KEY_ARG`], and pins both
/// digests; nothing else changes. `plan` is [`parse_plan`]'s value.
///
/// # Errors
///
/// As [`runner_job_spec_with_destinations`].
#[allow(clippy::too_many_arguments)]
pub fn runner_job_spec_with_policy(
    restore: &Restore,
    plan: &RestoreSpec,
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

    // Validated by the one function the reconcile's early refusal also calls,
    // so the Job never carries a value the object would be refused for.
    let resources = runner_resources_of(restore)?;
    // By the same one function as the early refusal.
    coverage_agrees(restore, plan)?;
    // Likewise.
    original_name_agrees(restore, plan)?;

    // The target connection, from the one resolver the probe and the backup
    // use. The runner dials `planBytes`' target with this connection's
    // password and CA, so the approved plan must name the same target; a
    // mismatch is refused before any ConfigMap or Job exists.
    let connection =
        crate::connection::resolve(cluster, crate::connection::ConnectionUse::RestoreTarget)?;
    connection.check_job_namespace(&namespace)?;
    connection.check_restore_target(&plan.target.bootstrap_servers, &plan.target.auth)?;
    let projection = connection.project();

    // The private signing key is the only file-backed Secret; the approval
    // inputs are public and come from this Restore's immutable ConfigMap. A
    // key is a file, because an env var is visible through
    // `kubectl describe pod`.
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
    // An inline archive's Secret is bound to the locations the runner dials
    // with it: the plan's source archive and evidence store, both reached with
    // this one credential, with an omitted endpoint filled from the forwarded
    // `AWS_ENDPOINT_URL`.
    let inline_binding = match (
        restore.spec.source_archive.secret_ref.as_ref(),
        destinations,
    ) {
        (Some(secret), None) => Some(inline_restore_binding_env(
            &secret.name,
            plan,
            &backup::archive_addressing_env(),
        )),
        _ => None,
    };
    if let Some(secret) = restore.spec.source_archive.secret_ref.as_ref() {
        env_from_secret.push(EnvFromSecret {
            name: ARCHIVE_ACCESS_KEY_ENV.to_string(),
            secret_name: secret.name.clone(),
            optional: false,
            key: ARCHIVE_ACCESS_KEY.to_string(),
        });
        env_from_secret.push(EnvFromSecret {
            name: ARCHIVE_SECRET_KEY_ENV.to_string(),
            secret_name: secret.name.clone(),
            optional: false,
            key: ARCHIVE_SECRET_KEY.to_string(),
        });
    }
    if let Some(binding) = inline_binding.as_ref() {
        env_from_secret.extend(binding.from_secret.iter().cloned());
    }
    // The target's SASL password, projected by `secretKeyRef` and never read:
    // the controller holds no `get` on Secrets. The runner validates the value
    // (exit 3, `CredentialNotRenderable`); the resolver refuses up front a
    // `scramSha512` connection with no reference at all.
    env_from_secret.extend(projection.env_from_secret);

    // Whether the evidence grant needs its own credential is a question about
    // the pair: the same Secret and keys means
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
    // One pod, one ServiceAccount: a workload-identity grant replaces the
    // connection's runner ServiceAccount. `evidence_env` has already refused
    // two different workload identities.
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
        // Named after the CR, verbatim; see `RunnerJobSpec::name` and step 0
        // of `reconcile_restore` for names over 63 characters.
        name: name.clone(),
        namespace,
        owner: RunnerOwner {
            api_version: Restore::api_version(&()).to_string(),
            kind: Restore::kind(&()).to_string(),
            name,
            uid,
        },
        args: {
            let mut argv = runner_argv(restore, plan, approver_key_ids);
            // An old runner image fails to parse these flags and exits before
            // it dispatches, rather than verifying a v2 document as v1.
            if v2 {
                argv.extend([
                    APPROVAL_POLICY_ARG.to_string(),
                    format!("{APPROVAL_MOUNT_PATH}/{APPROVAL_POLICY_FILE}"),
                    CONFIRMATION_KEY_ARG.to_string(),
                    format!("{APPROVAL_MOUNT_PATH}/{CONFIRMATION_KEY_FILE}"),
                ]);
            }
            // The version-skew handshake: an old runner image fails to parse
            // this flag and exits before it dispatches, rather than building
            // its stores out of whatever `AWS_*` is in the pod.
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
                items: {
                    let mut items = vec![
                        (APPROVAL_DOC_FILE.to_string(), APPROVAL_DOC_FILE.to_string()),
                        (APPROVAL_SIG_FILE.to_string(), APPROVAL_SIG_FILE.to_string()),
                        (APPROVER_KEY_FILE.to_string(), APPROVER_KEY_FILE.to_string()),
                        (
                            ALLOWED_CLUSTERS_FILE.to_string(),
                            ALLOWED_CLUSTERS_FILE.to_string(),
                        ),
                    ];
                    // The frozen policy snapshot and the console key, exactly
                    // when the bundle carries them.
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
                    // The evidence keyring, exactly when the plan binds a
                    // recovery point.
                    if plan_binds_point(plan) {
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
        // `RUST_LOG` is pinned: below `info` the run id and the exit-code
        // meaning line are lost, and they correlate a pod with its archive.
        //
        // The legacy path also forwards the object-store addressing the
        // controller was given, so the endpoint is configured once, on the
        // controller, for both kinds of runner. Nothing unset is forwarded.
        // See `backup::ARCHIVE_ADDRESSING_ENV`.
        env_literal: {
            let mut env = vec![("RUST_LOG".to_string(), "info".to_string())];
            match (archive_env.as_ref(), evidence_env.as_ref()) {
                // The complete explicit set, and nothing from this process.
                (Some(archive), Some(evidence)) => {
                    env.extend(archive.literals.iter().cloned());
                    env.extend(evidence.literals.iter().cloned());
                }
                // The legacy inline-`sourceArchive` path: the four forwarded
                // variables, or none on a default install.
                _ => env.extend(backup::archive_addressing_env()),
            }
            // The inline archive Secret's expected location bindings.
            if let Some(binding) = inline_binding.as_ref() {
                env.extend(binding.literals.iter().cloned());
            }
            env.extend(execution_contract_env_with_policy(
                restore, plan, approval, trust, policy, now,
            )?);
            env.extend(projection.env_literal);
            env
        },
        plan_config_map: Some(plan_config_map_name(&restore.name_any())),
        // The shipped pin; the reconciler overwrites it when this process was
        // handed another image (`job::RUNNER_IMAGE_ENV`), so this stays a
        // pure function of the custom resource.
        image: None,
        // Likewise for `job::RUNNER_PULL_POLICY_ENV`.
        image_pull_policy: None,
        // `spec.runnerResources`, validated above, or no key at all.
        resources,
    })
}

/// `spec.runnerResources` as the runner container carries it. `Ok(None)` when
/// the object asks for nothing, which renders no `resources` key.
///
/// # Errors
///
/// [`RestoreError::Refused`] with
/// [`crate::conditions::TERMINAL_STATE_EXECUTION_SPEC_INVALID`], naming every
/// field [`crate::runner_resources::validate`] refused. Terminal, because
/// `spec` is immutable.
pub fn runner_resources_of(
    restore: &Restore,
) -> Result<Option<job::ContainerResources>, RestoreError> {
    crate::runner_resources::validate(
        restore.spec.runner_resources.as_ref(),
        crate::runner_resources::RESTORE_PATH,
    )
    .map_err(|refused| {
        RestoreError::Refused(
            crate::conditions::TERMINAL_STATE_EXECUTION_SPEC_INVALID,
            format!(
                "{refused}. The runner's requests and limits are applied exactly as written or \
                 not at all, so no Job was created; spec is immutable — create a new Restore \
                 whose spec.runnerResources is inside the bounds, or leave it out"
            ),
        )
    })
}

/// The one read of `spec.planBytes` a reconcile makes; every check, the
/// admission, the bundle and the Job are handed its value. A plan that does
/// not parse is refused here, before an approval is read, rather than by the
/// runner at phase 0. A declaration over such a plan is refused under the
/// declaration's name ([`coverage_agrees`], [`original_name_agrees`]).
///
/// # Errors
///
/// [`RestoreError::Refused`]: `ExecutionSpecInvalid` when the coverage or the
/// original names are declared, [`CheckCode::PlanUnparseable`] otherwise.
pub fn parse_plan(restore: &Restore) -> Result<RestoreSpec, RestoreError> {
    serde_yaml::from_str(&restore.spec.plan_bytes).map_err(|error| {
        if restore.spec.coverage.is_some() || restore.spec.complete_max_records.is_some() {
            coverage_refused(format!(
                "spec.coverage is declared and spec.planBytes does not parse as a restore plan, \
                 so the two cannot be compared: {error}"
            ))
        } else if restore.spec.target.topic_naming.is_original_name() {
            original_name_refused(format!(
                "spec.target.topicNaming.originalName is declared and spec.planBytes does not \
                 parse as a restore plan, so the two cannot be compared: {error}"
            ))
        } else {
            RestoreError::Refused(
                CheckCode::PlanUnparseable.as_str(),
                format!(
                    "spec.planBytes is not a readable restore plan ({error}); the runner would \
                     refuse it, so no Job was created. spec is immutable and the approval binds \
                     the plan bytes — create a new Restore with a plan the runner accepts"
                ),
            )
        }
    })
}

fn coverage_refused(detail: String) -> RestoreError {
    RestoreError::Refused(
        crate::conditions::TERMINAL_STATE_EXECUTION_SPEC_INVALID,
        format!(
            "{detail}. spec.coverage and spec.completeMaxRecords state what the plan's \
             sample.coverage and sample.complete_max_records say (absent: sampled, no \
             bound), so a list and the console show the coverage that will run; no Job was \
             created. spec is immutable and the approval binds the plan bytes — create a \
             new Restore whose two fields match its plan"
        ),
    )
}

fn original_name_refused(detail: String) -> RestoreError {
    RestoreError::Refused(
        crate::conditions::TERMINAL_STATE_EXECUTION_SPEC_INVALID,
        format!(
            "{detail}. spec.target.topicNaming.originalName states what the plan's \
             target.topic_naming.original_name says, so the console and a list show the \
             approval subject the restore needs; no Job was created. spec is immutable and the \
             approval binds the plan bytes — create a new Restore whose declaration matches \
             its plan"
        ),
    )
}

/// `spec.coverage` and `spec.completeMaxRecords` against the plan's own
/// `sample.coverage` and `sample.complete_max_records`.
///
/// The spec fields declare what the plan says, so a list, a printer column and
/// the console can read the coverage without parsing the plan; a declaration
/// that disagrees would show one coverage and run another. Absent means
/// `sampled` with no bound, so older `Restore`s (all sampled) agree.
///
/// # Errors
///
/// [`RestoreError::Refused`] with
/// [`crate::conditions::TERMINAL_STATE_EXECUTION_SPEC_INVALID`], naming both
/// values. Terminal: `spec` is immutable and the approval binds the plan.
pub fn coverage_agrees(restore: &Restore, plan: &RestoreSpec) -> Result<(), RestoreError> {
    let declared = crate::crds::restore::VerificationCoverage::of(restore.spec.coverage);
    let declared_bound = restore.spec.complete_max_records;
    let plan_bound = plan
        .sample
        .complete_max_records
        .map(|n| i64::try_from(n).unwrap_or(i64::MAX));
    if plan.sample.coverage != declared || plan_bound != declared_bound {
        let bound = |b: Option<i64>| b.map_or_else(|| "no bound".to_string(), |n| n.to_string());
        return Err(coverage_refused(format!(
            "the plan asks for sample.coverage `{}` ({}) and spec declares coverage `{}` ({})",
            plan.sample.coverage.as_str(),
            bound(plan_bound),
            declared.as_str(),
            bound(declared_bound)
        )));
    }
    Ok(())
}

/// The original-name declaration holds to the plan in both directions, for
/// the same reason as [`coverage_agrees`].
///
/// `spec.target.topicNaming.originalName` declares the plan's
/// `target.topic_naming.original_name`, so the API and console can show a
/// restore under the original topic names, and its separate approval subject,
/// without parsing the plan. A disagreeing declaration could let an approver
/// shown "ordinary" sign the approval that authorises a write under the
/// production names.
///
/// # Errors
///
/// [`RestoreError::Refused`] with
/// [`crate::conditions::TERMINAL_STATE_EXECUTION_SPEC_INVALID`], naming both.
pub fn original_name_agrees(restore: &Restore, plan: &RestoreSpec) -> Result<(), RestoreError> {
    let declared = restore.spec.target.topic_naming.is_original_name();
    let in_plan = logweir_core::original_name::ApprovalSubject::of_plan(plan)
        == logweir_core::original_name::ApprovalSubject::OriginalName;
    if in_plan != declared {
        return Err(original_name_refused(format!(
            "the plan {} under the original topic names and spec.target.topicNaming.originalName \
             is {}",
            if in_plan {
                "restores"
            } else {
                "does not restore"
            },
            if declared { "true" } else { "absent or false" }
        )));
    }
    // An original-name plan in a shape the runner refuses at phase 0 is
    // refused here, in the runner's words, before any approval or Job. A
    // sampled plan (`OriginalNameNeedsCompleteCoverage`) also has a CEL rule
    // (`crds::restore::ORIGINAL_NAME_COVERAGE_RULE`); this covers an older CRD
    // and the plan bytes. A partition subset (`OriginalNameNeedsWholeTopics`)
    // has no CEL rule, because the CRD declares no partitions, so this check
    // is the Kubernetes boundary for it.
    if in_plan {
        if let Some(why) = logweir_core::original_name::refuse_shape(plan) {
            return Err(RestoreError::Refused(
                crate::conditions::TERMINAL_STATE_EXECUTION_SPEC_INVALID,
                format!(
                    "{why}; no Job was created. spec is immutable and the approval binds the \
                     plan bytes — create a new Restore with a plan the runner accepts"
                ),
            ));
        }
    }
    Ok(())
}

/// Interface **I8**'s three evidence keys, as read off a pod log.
///
/// Three independent `Option`s: no key is ever derived from another.
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
    /// Whether both mandatory lines were present. The third line is printed
    /// only when the engine wrote an offset report, so a two-line tail at
    /// exit 0 is complete; requiring all three would raise
    /// `EvidenceKeysUnreadable` on every good run without a report.
    #[must_use]
    pub fn mandatory_complete(&self) -> bool {
        self.scorecard.is_some() && self.sidecar.is_some()
    }
}

/// Read interface **I8**'s three keys off a pod log, by key name, never by
/// position.
///
/// A pod log is stdout and stderr merged in nondeterministic order, so this
/// scans the final [`super::backup::KEY_SCAN_TAIL_LINES`] non-empty lines
/// through [`runner_log::key_values`], the scan the `Backup` path uses, and
/// matches each line's prefix. The last occurrence of each prefix wins.
///
/// No key is ever guessed: a scorecard key is derivable, but deriving one on
/// an unreadable log would point `status.evidence.scorecardKey` at an object
/// that may not exist, and verification would report `Invalid` for evidence
/// that was merely unread.
#[must_use]
pub fn restore_evidence_keys(log: &str) -> RestoreEvidenceKeys {
    let last = |key| runner_log::key_values(log, key).pop().map(str::to_string);
    RestoreEvidenceKeys {
        scorecard: last(SCORECARD_KEY_PREFIX),
        sidecar: last(SIDECAR_KEY_PREFIX),
        offset_report: last(OFFSET_REPORT_KEY_PREFIX),
    }
}

/// The values this reconciler copies out of a signed scorecard.
///
/// Every field is a copy, none a decision: the document is read as a
/// `serde_json::Value` and each value lifted out by pointer (see the module
/// header for why it is never typed).
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
    /// `evidence.offset_report_sha256`, copied from the signed document, not
    /// computed: that would mean fetching a second object to answer what the
    /// first already answers.
    pub offset_report_sha256: Option<String>,
    /// `sha256_prefixed` of the scorecard bytes the controller fetched.
    /// Computed here, because a document cannot carry its own digest.
    pub scorecard_sha256: Option<String>,
    /// `target_diff.would_create`, as `(topic, partitions)` pairs: the source
    /// of `status.completion.newTopics`. `None` when the document has no such
    /// list, when any entry is not a `[string, integer]` pair, or past
    /// [`COMPLETION_NEW_TOPICS_MAX`] entries: a partial list would be a claim
    /// the document does not make, and one past the CRD's `maxItems` would make
    /// the API server refuse the whole status patch.
    pub would_create: Option<Vec<(String, i64)>>,
    /// `sample.records_expected` — the canary size.
    pub sample_records_expected: Option<i64>,
    /// `sample.records_restored` — records consumed back from the target in
    /// the sampled window (NOT the whole restore's record count).
    pub sample_records_restored: Option<i64>,
    /// `integrity.records_sampled`.
    pub integrity_records_sampled: Option<i64>,
    /// `integrity.records_sampled_matching`.
    pub integrity_records_sampled_matching: Option<i64>,
    /// `sample.window_start`, verbatim, only when it is an RFC 3339 instant:
    /// the CRD types it `date-time`, and a value that does not parse back
    /// would make the stored object unreadable to typed clients.
    pub sample_window_start: Option<String>,
    /// `sample.window_end`, under the same rule.
    pub sample_window_end: Option<String>,
    /// `source.time_basis` (format 1.3.0), the source of `status.timeBasis`.
    /// `None` when the document carries no such block, when it is not an
    /// object of an optional string `plan` and two arrays of strings, or when
    /// a list holds more than [`TIME_BASIS_TOPICS_MAX`] entries.
    pub time_basis: Option<crate::crds::restore::RestoreTimeBasis>,
    /// `integrity.verification.coverage` (format 1.4.0), `sampled` or
    /// `complete`; any other value is not copied. `None` is not recorded (a
    /// document before 1.4.0), never "complete".
    pub integrity_coverage: Option<String>,
    /// `integrity.verification.complete`, read as
    /// [`crate::crds::restore::CompleteCoverage`]: all of it or nothing
    /// ([`complete_coverage`]), and only beside `coverage: complete`.
    pub integrity_complete: Option<crate::crds::restore::CompleteCoverage>,
    /// `sample.unsampled_topics` (format 1.6.0): all of it, or nothing when
    /// malformed or past [`crate::crds::restore::UNSAMPLED_TOPICS_MAX`].
    pub unsampled_topics: Option<Vec<String>>,
    /// `source.selection` (format 1.7.0 / 2.0.0): present exactly when the
    /// document carries the block. A block this controller cannot read is
    /// still a selection, copied empty, so a narrowed restore is never shown
    /// as a full one ([`restore_selection`]).
    pub selection: Option<crate::crds::restore::RestoreSelection>,
    /// `integrity.verification.positions_mapping` (format 1.12.0 / 2.4.0),
    /// read by [`positions_mapping_record`]: `Some` on every document read,
    /// `bound: false` when it binds none (DUI-50).
    pub positions_mapping: Option<crate::crds::restore::PositionsMappingRecord>,
    /// `Some` when the scorecard was not read because it exceeds the
    /// controller's read cap: the `NotAttempted` detail naming the cap
    /// ([`crate::verification::over_read_cap_detail`]). Every other field is
    /// `None` then, and [`split_refused`] turns it into not observed.
    pub read_refused: Option<String>,
}

/// A signed `source.selection` read as the status'
/// [`crate::crds::restore::RestoreSelection`]: the window's ends, how many
/// topics it narrows to a partition subset, and those topics' partitions.
/// The rows are all or none (none past
/// [`crate::crds::restore::SELECTION_TOPICS_MAX`] topics or
/// [`crate::crds::restore::SELECTION_PARTITIONS_MAX`] partitions in one, or
/// when a row is malformed); the count is kept either way. Fail safe: any
/// value under the key is a selection, because a reader must never conclude
/// that a narrowed restore restored everything.
#[must_use]
pub fn restore_selection(block: &Value) -> crate::crds::restore::RestoreSelection {
    use crate::crds::restore::{
        RestoreSelection, SelectedPartitions, SELECTION_PARTITIONS_MAX, SELECTION_SCOPE_PARTIAL,
        SELECTION_TOPICS_MAX,
    };
    let scope = Some(SELECTION_SCOPE_PARTIAL.to_string());
    let Some(o) = block.as_object() else {
        return RestoreSelection {
            scope,
            ..RestoreSelection::default()
        };
    };
    let subsets = o.get("partitions").and_then(Value::as_array);
    let rows = subsets.and_then(|list| {
        if list.len() > SELECTION_TOPICS_MAX {
            return None;
        }
        list.iter()
            .map(|e| {
                let topic = e.get("topic")?.as_str().filter(|t| t.len() <= 249)?;
                let parts = e.get("partitions")?.as_array()?;
                if parts.len() > SELECTION_PARTITIONS_MAX {
                    return None;
                }
                let partitions = parts
                    .iter()
                    .map(|p| p.as_i64().and_then(|p| i32::try_from(p).ok()))
                    .collect::<Option<Vec<i32>>>()?;
                Some(SelectedPartitions {
                    topic: topic.to_string(),
                    partitions,
                })
            })
            .collect::<Option<Vec<_>>>()
    });
    RestoreSelection {
        scope,
        window_start_ms: o.get("window_start_ms").and_then(Value::as_i64),
        window_end_ms: o.get("window_end_ms").and_then(Value::as_i64),
        // A key holding anything but an empty list narrows: a malformed one
        // is counted as one topic rather than read as none.
        narrowed_topics: o.get("partitions").and_then(|v| match v.as_array() {
            Some(list) if list.is_empty() => None,
            Some(list) => i64::try_from(list.len()).ok(),
            None if v.is_null() => None,
            None => Some(1),
        }),
        partitions: rows.filter(|r| !r.is_empty()),
        engine_runs: o.get("engine_runs").and_then(Value::as_i64),
    }
}

/// An observation that is only a refusal is not observed for every fact a
/// pass copies, like an unread scorecard; its sentence is returned beside it
/// as the verdict's `NotAttempted` detail.
#[must_use]
pub fn split_refused(
    observed: Option<ScorecardObservation>,
) -> (Option<ScorecardObservation>, Option<String>) {
    match observed {
        Some(ScorecardObservation {
            read_refused: Some(detail),
            ..
        }) => (None, Some(detail)),
        other => (other, None),
    }
}

/// A signed `integrity.verification.complete` block read as the status'
/// [`crate::crds::restore::CompleteCoverage`]: copied, never recomputed, and
/// all or nothing, so the status never carries numbers the document does not
/// hold. Partition rows are copied only up to
/// [`crate::crds::restore::COMPLETE_PARTITIONS_MAX`] (`partitionCount` says
/// how many either way); failed and unverified segment keys are counted, not
/// copied. `incomplete_reason` is cut at a char boundary to the CRD's 1024
/// bytes, marked.
#[must_use]
pub fn complete_coverage(block: &Value) -> Option<crate::crds::restore::CompleteCoverage> {
    use crate::crds::restore::{
        CompleteArchive, CompleteCoverage, PartitionCoverage, ReplayCounts, COMPLETE_PARTITIONS_MAX,
    };
    let count = |v: &Value, key: &str| -> Option<i64> { i64::try_from(v.get(key)?.as_u64()?).ok() };
    let replay = |v: &Value| -> Option<ReplayCounts> {
        Some(ReplayCounts {
            expected: count(v, "expected")?,
            restored: count(v, "restored")?,
            matching: count(v, "matching")?,
            missing: count(v, "missing")?,
            unexpected: count(v, "unexpected")?,
            duplicates: count(v, "duplicates")?,
            out_of_order: count(v, "out_of_order")?,
            mismatched: count(v, "mismatched")?,
        })
    };
    let block = block.as_object().map(|_| block)?;
    let incomplete_reason = match block.get("incomplete_reason") {
        None | Some(Value::Null) => None,
        Some(Value::String(reason)) => Some(bounded_reason(reason)),
        Some(_) => return None,
    };
    let max_records = match block.get("max_records") {
        None | Some(Value::Null) => None,
        Some(v) => Some(i64::try_from(v.as_u64()?).ok()?),
    };
    let archive = block.get("archive")?;
    let list_len =
        |v: &Value, key: &str| -> Option<i64> { i64::try_from(v.get(key)?.as_array()?.len()).ok() };
    let rows = block.get("partitions")?.as_array()?;
    let partitions = if rows.len() <= COMPLETE_PARTITIONS_MAX {
        Some(
            rows.iter()
                .map(|row| {
                    let topic = row.get("topic")?.as_str()?;
                    let target_topic = match row.get("target_topic") {
                        None | Some(Value::Null) => None,
                        Some(Value::String(t)) if t.len() <= 249 => Some(t.clone()),
                        Some(_) => return None,
                    };
                    (topic.len() <= 249).then_some(())?;
                    Some(PartitionCoverage {
                        topic: topic.to_string(),
                        partition: i32::try_from(row.get("partition")?.as_i64()?).ok()?,
                        target_topic,
                        compared: row.get("compared")?.as_bool()?,
                        replay: replay(row.get("replay")?)?,
                    })
                })
                .collect::<Option<Vec<_>>>()?,
        )
    } else {
        None
    };
    Some(CompleteCoverage {
        covered: block.get("covered")?.as_bool()?,
        incomplete_reason,
        max_records,
        archive: CompleteArchive {
            segments: count(archive, "segments")?,
            segments_verified: count(archive, "segments_verified")?,
            segments_failed_count: list_len(archive, "segments_failed")?,
            segments_unverified_count: list_len(archive, "segments_unverified")?,
            records_decoded: count(archive, "records_decoded")?,
            offset_holes: count(archive, "offset_holes")?,
        },
        replay: replay(block.get("replay")?)?,
        partition_count: i64::try_from(rows.len()).ok()?,
        partitions,
    })
}

/// A free-text reason cut to `status.integrity.complete.incompleteReason`'s
/// 1024 bytes at a char boundary, marked when cut.
fn bounded_reason(reason: &str) -> String {
    const MAX: usize = 1024;
    const MARK: &str = " [cut; the signed scorecard holds the whole reason]";
    if reason.len() <= MAX {
        return reason.to_string();
    }
    let mut end = MAX - MARK.len();
    while !reason.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}{MARK}", &reason[..end])
}

/// `sample.unsampled_topics`, all of it or nothing.
fn unsampled_topics(list: &Value) -> Option<Vec<String>> {
    let items = list.as_array()?;
    if items.len() > crate::crds::restore::UNSAMPLED_TOPICS_MAX {
        return None;
    }
    items
        .iter()
        .map(|t| t.as_str().filter(|t| t.len() <= 249).map(str::to_string))
        .collect()
}

/// `status.timeBasis` from a read scorecard: written whenever the document
/// carried the block (two empty lists are a claim), never invented. One
/// helper for both writers of scorecard facts (the terminal patch and the
/// evidence-fetch relay), so neither can drop it alone.
#[must_use]
pub fn time_basis_value(o: &ScorecardObservation) -> Option<Value> {
    o.time_basis
        .as_ref()
        .and_then(|tb| serde_json::to_value(tb).ok())
}

/// `status.timeBasis`' per-list `maxItems` (`crds/restore.rs`).
pub const TIME_BASIS_TOPICS_MAX: usize = 256;

/// `source.time_basis` read as [`crate::crds::restore::RestoreTimeBasis`] —
/// all of it, or nothing (see [`ScorecardObservation::time_basis`]).
fn time_basis_block(block: &Value) -> Option<crate::crds::restore::RestoreTimeBasis> {
    let block = block.as_object()?;
    let list = |name: &str| -> Option<Vec<String>> {
        let items = block.get(name)?.as_array()?;
        if items.len() > TIME_BASIS_TOPICS_MAX {
            return None;
        }
        items
            .iter()
            .map(|t| t.as_str().map(str::to_string))
            .collect()
    };
    let plan = match block.get("plan") {
        None | Some(Value::Null) => None,
        Some(Value::String(p)) => Some(p.clone()),
        Some(_) => return None,
    };
    Some(crate::crds::restore::RestoreTimeBasis {
        plan,
        producer_time: list("producer_time")?,
        not_recorded: list("not_recorded")?,
    })
}

/// `Restore.status.completion.newTopics`'s `maxItems` (`crds/restore.rs`).
pub const COMPLETION_NEW_TOPICS_MAX: usize = 256;

/// `status.teardown.failed` from the runner's last `teardown-failed=` line,
/// held to this Restore's mapped target names: a topic its plan does not map
/// is not this run's to name. `None` when there is no line or nothing in it
/// survives. `failedCount` from its `teardown-failed-count=` line (FX-38): the
/// list carries at most 256 names, and the count says how many were left,
/// held between the names kept and the names the plan maps.
#[must_use]
pub fn teardown_failed(log: &str, mapped: &[String]) -> Option<Value> {
    use logweir_core::execution_contract::{
        parse_teardown_failed, TEARDOWN_FAILED_COUNT_PREFIX, TEARDOWN_FAILED_PREFIX,
    };
    let raw = runner_log::key_values(log, TEARDOWN_FAILED_PREFIX).pop()?;
    let kept: Vec<Value> = parse_teardown_failed(raw)?
        .into_iter()
        .filter(|f| mapped.iter().any(|m| m == &f.topic))
        .map(|f| json!({"topic": f.topic, "error": f.error}))
        .collect();
    if kept.is_empty() {
        return None;
    }
    let mut teardown = json!({ "failed": kept });
    if let Some(count) = runner_log::key_values(log, TEARDOWN_FAILED_COUNT_PREFIX)
        .pop()
        .and_then(|v| v.trim().parse::<usize>().ok())
    {
        let count = count.min(mapped.len()).max(kept.len());
        teardown["failedCount"] = json!(count);
    }
    Some(teardown)
}

/// The longest `teardown-key=` value read; a genuine key is
/// `logweir/drills/<run id>.teardown.json`.
const TEARDOWN_KEY_MAX_BYTES: usize = 512;

/// `status.teardown` for a finished run (F2): [`teardown_failed`], and what
/// an attested phase 9 removed, from the runner's SEALED lines only
/// (`sealed_only`, FX-43): the attestation's key from `teardown-key=`, and
/// its `teardown-deleted=` list and `teardown-deleted-count=`, held to this
/// Restore's mapped target names as `failed` is. A Job an older controller
/// built (its whole log is text anyone could have put there) gets only what
/// [`teardown_failed`] reads. `None` when there is nothing to record.
#[must_use]
pub fn teardown_status(log: &str, sealed_only: bool, mapped: &[String]) -> Option<Value> {
    use logweir_core::execution_contract::{
        parse_teardown_deleted, TEARDOWN_DELETED_COUNT_PREFIX, TEARDOWN_DELETED_PREFIX,
        TEARDOWN_KEY_PREFIX,
    };
    let mut teardown = teardown_failed(log, mapped).unwrap_or_else(|| json!({}));
    let key = sealed_only
        .then(|| runner_log::key_values(log, TEARDOWN_KEY_PREFIX).pop())
        .flatten()
        .filter(|k| {
            k.len() <= TEARDOWN_KEY_MAX_BYTES
                && k.strip_prefix("logweir/drills/")
                    .and_then(|r| r.strip_suffix(".teardown.json"))
                    .is_some_and(|id| {
                        !id.is_empty() && !id.chars().any(|c| c.is_whitespace() || c.is_control())
                    })
        });
    if let Some(key) = key {
        teardown["attestationKey"] = json!(key);
        if let Some(names) = runner_log::key_values(log, TEARDOWN_DELETED_PREFIX)
            .pop()
            .and_then(parse_teardown_deleted)
        {
            let kept: Vec<String> = names.into_iter().filter(|t| mapped.contains(t)).collect();
            let count = runner_log::key_values(log, TEARDOWN_DELETED_COUNT_PREFIX)
                .pop()
                .and_then(|v| v.trim().parse::<usize>().ok())
                .map_or(kept.len(), |c| c.min(mapped.len()).max(kept.len()));
            teardown["deleted"] = json!(kept);
            teardown["deletedCount"] = json!(count);
        }
    }
    teardown
        .as_object()
        .is_some_and(|m| !m.is_empty())
        .then_some(teardown)
}

/// **PROD-05.2.** `status.topicConfiguration` from the runner's last
/// `topic-configuration=` line, held to this Restore's mapped target names and
/// re-shaped into exactly the CRD's fields (a typed round trip): a topic the
/// plan does not map is not this run's to name. `None` when there is no line
/// or it does not parse.
#[must_use]
pub fn topic_configuration(log: &str, mapped: &[String]) -> Option<Value> {
    use crate::crds::restore::RestoreTopicConfiguration;
    let raw = runner_log::key_values(
        log,
        logweir_core::topic_transition::TOPIC_CONFIGURATION_KEY_PREFIX,
    )
    .pop()?;
    let mut doc: RestoreTopicConfiguration = serde_json::from_str(raw).ok()?;
    let ours = |t: &String| mapped.iter().any(|m| m == t);
    if let Some(list) = doc.applied.as_mut() {
        list.retain(ours);
    }
    if let Some(list) = doc.exported.as_mut() {
        list.retain(ours);
    }
    if let Some(list) = doc.failed.as_mut() {
        list.retain(|f| ours(&f.topic));
        for f in list.iter_mut() {
            f.error = f.error.chars().take(256).collect();
        }
    }
    if let Some(list) = doc.topics.as_mut() {
        list.retain(|t| ours(&t.topic));
    }
    doc.transition = doc.transition.map(|t| t.chars().take(64).collect());
    serde_json::to_value(doc).ok()
}

/// Guard **G-TS**'s observation, read off the pod log by key name.
///
/// Returns the printed object filtered to the three fields
/// `crate::crds::restore::TopicPreflight` declares: a fourth key would be
/// pruned by the structural schema, and a client reading the status back would
/// see a field that was never stored. The last occurrence in the tail wins.
///
/// `None` when no line was printed, its value is not a JSON object, or it
/// carries none of the three fields; `status.topicPreflight` then stays
/// absent.
#[must_use]
pub fn topic_preflight(log: &str) -> Option<Value> {
    let raw = runner_log::key_values(log, TOPIC_PREFLIGHT_KEY_PREFIX).pop();
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

/// The bound on every parse of a scorecard into a tree: `Some` with the bytes
/// when they are within `caps::CONTROLLER_DOCUMENT` (1 MiB), `None` when over.
///
/// A `Value` costs up to 37 times the document, so the cap is enforced at the
/// two places a scorecard becomes a tree ([`scorecard_observation`] and the
/// relayed scorecard's `run_id` read), not only where the bytes were read.
/// Relayed bytes come out of a pod log in the subject's namespace.
#[must_use]
pub fn within_scorecard_cap(bytes: &[u8]) -> Option<&[u8]> {
    (bytes.len() as u64 <= caps::CONTROLLER_DOCUMENT).then_some(bytes)
}

/// Read the values [`ScorecardObservation`] names out of a scorecard
/// document.
///
/// Pure, over bytes. `None` when the bytes are not a JSON object, which is
/// not observed, not "the run had no outcome". Bytes over
/// `caps::CONTROLLER_DOCUMENT` are never parsed, whoever handed them over
/// ([`within_scorecard_cap`]).
#[must_use]
pub fn scorecard_observation(bytes: &[u8]) -> Option<ScorecardObservation> {
    let bytes = within_scorecard_cap(bytes)?;
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
        would_create: doc
            .pointer("/target_diff/would_create")
            .and_then(would_create_pairs),
        sample_records_expected: i("/sample/records_expected"),
        sample_records_restored: i("/sample/records_restored"),
        integrity_records_sampled: i("/integrity/records_sampled"),
        integrity_records_sampled_matching: i("/integrity/records_sampled_matching"),
        sample_window_start: s("/sample/window_start").filter(|t| is_rfc3339(t)),
        sample_window_end: s("/sample/window_end").filter(|t| is_rfc3339(t)),
        time_basis: doc.pointer("/source/time_basis").and_then(time_basis_block),
        integrity_coverage: s("/integrity/verification/coverage").filter(|c| {
            c == logweir_core::scorecard::COVERAGE_SAMPLED
                || c == logweir_core::scorecard::COVERAGE_COMPLETE
        }),
        // Only beside `coverage: complete`; a document that broke that is not
        // believed half-way.
        integrity_complete: doc
            .pointer("/integrity/verification/coverage")
            .and_then(Value::as_str)
            .filter(|c| *c == logweir_core::scorecard::COVERAGE_COMPLETE)
            .and_then(|_| doc.pointer("/integrity/verification/complete"))
            .and_then(complete_coverage),
        unsampled_topics: doc
            .pointer("/sample/unsampled_topics")
            .and_then(unsampled_topics),
        // `null` is absent, as the scorecard's own `Option` reads it; any
        // other value is a selection.
        selection: doc
            .pointer("/source/selection")
            .filter(|v| !v.is_null())
            .map(restore_selection),
        positions_mapping: Some(positions_mapping_record(
            doc.pointer("/integrity/verification/positions_mapping"),
        )),
        read_refused: None,
    })
}

/// A signed `integrity.verification.positions_mapping` block read as the
/// status' [`crate::crds::restore::PositionsMappingRecord`] (DUI-50).
///
/// `bound` is `true` exactly when the block names a document whose `key` and
/// `sha256` are strings: the condition a switchover's review requires before
/// it reads the mapping. No block, a `null` one, or one that names only why
/// nothing was written, is `bound: false`. `omitted` is copied only as one of
/// the scorecard's own reasons, and `groups` only as a `u32`.
#[must_use]
pub fn positions_mapping_record(
    block: Option<&Value>,
) -> crate::crds::restore::PositionsMappingRecord {
    let block = block.filter(|b| b.is_object());
    let document = block.and_then(|b| b.get("document")).filter(|d| {
        d.get("key").is_some_and(Value::is_string) && d.get("sha256").is_some_and(Value::is_string)
    });
    crate::crds::restore::PositionsMappingRecord {
        bound: document.is_some(),
        groups: block
            .and_then(|b| b.get("groups"))
            .and_then(Value::as_u64)
            .and_then(|g| u32::try_from(g).ok()),
        omitted: block
            .and_then(|b| b.get("omitted"))
            .and_then(Value::as_str)
            .filter(|o| logweir_core::positions_mapping::OMITTED_REASONS.contains(o))
            .map(str::to_string),
    }
}

/// `target_diff.would_create` read as `(topic, partitions)` pairs — all of
/// them, or none (see [`ScorecardObservation::would_create`]).
fn would_create_pairs(list: &Value) -> Option<Vec<(String, i64)>> {
    let entries = list.as_array()?;
    if entries.len() > COMPLETION_NEW_TOPICS_MAX {
        return None;
    }
    entries
        .iter()
        .map(|entry| match entry.as_array().map(Vec::as_slice) {
            Some([name, partitions]) => Some((name.as_str()?.to_string(), partitions.as_i64()?)),
            _ => None,
        })
        .collect()
}

fn is_rfc3339(value: &str) -> bool {
    DateTime::parse_from_rfc3339(value).is_ok()
}

/// What the archive-facing half of this reconciler is handed.
///
/// An injected observation, not a `Store` handle, so the status this
/// reconciler patches is asserted without a socket. The real oracle reads
/// through the shared read-only `Option<Arc<Store>>` on [`super::Context`]
/// (interface **I13**).
///
/// It is async because `Store::get_capped` blocks on its own current-thread
/// runtime, and calling it from a reconciler compiles but panics with
/// *Cannot start a runtime from within a runtime*. Every `Store` call goes
/// through `tokio::task::spawn_blocking(move || …).await`, which
/// `tests/retention.rs::no_store_call_is_made_outside_spawn_blocking` enforces
/// over this file.
///
/// `BoxFuture<'static, …>`, not `'a`: an `Fn`'s `Output` is matched exactly,
/// so a future borrowed for `'a` would force the local oracle to be
/// `'static`. The future owns what it reads (an `Arc<Store>` clone and the
/// key).
///
/// `None` means not observed, never "the run said nothing": it omits every
/// scorecard key from the merge patch, so a controller with no archive handle
/// never writes `outcome: null` over a real outcome.
pub type ScorecardOracle<'a> =
    &'a (dyn Fn(String) -> BoxFuture<'static, Option<ScorecardObservation>> + Send + Sync + 'a);

/// The oracle for a controller that holds no archive handle: it observes
/// nothing. Used when [`super::Context::archive`] is `None`, and by unit tests
/// that are not about the archive.
#[must_use]
pub fn unobserved_scorecard(_key: String) -> BoxFuture<'static, Option<ScorecardObservation>> {
    Box::pin(async { None })
}

/// The one `get` the real oracle makes, as a single blocking function; the
/// only place in this file that touches a `Store` (see [`ScorecardOracle`]).
/// An unreadable object is `None`, never an error the reconcile returns: the
/// run still has an exit code, which is the fact the status records.
///
/// The read is capped at `caps::CONTROLLER_DOCUMENT`, which also bounds the
/// `serde_json::Value` parse (a `Restore` has no runner digest to check the
/// bytes against first). A scorecard over it is an observation carrying only
/// [`ScorecardObservation::read_refused`]. The bytes and the parsed `Value`
/// are held under one [`crate::read_budget::DOCUMENT_READ_COST_BYTES`]
/// reservation of the controller's read budget.
#[must_use]
pub fn observe_scorecard(store: &Store, key: &str) -> Option<ScorecardObservation> {
    observe_scorecard_within(crate::read_budget::ReadBudget::controller(), store, key)
}

/// [`observe_scorecard`] under `budget` rather than the controller's own,
/// for a row that compares the two.
#[must_use]
pub fn observe_scorecard_within(
    budget: &crate::read_budget::ReadBudget,
    store: &Store,
    key: &str,
) -> Option<ScorecardObservation> {
    if key.trim().is_empty() {
        return None;
    }
    let _reservation = budget.reserve(crate::read_budget::DOCUMENT_READ_COST_BYTES);
    let (bytes, _version) = match store.get_capped(key, caps::CONTROLLER_DOCUMENT) {
        Ok(read) => read,
        Err(e @ logweir_store::StoreError::TooLarge { .. }) => {
            return Some(ScorecardObservation {
                read_refused: Some(crate::verification::store_detail(&e)),
                ..ScorecardObservation::default()
            })
        }
        Err(_) => return None,
    };
    scorecard_observation(&bytes)
}

/// [`backup::evidence_source_for`]'s answer, with the global handle confined to
/// the bucket it can actually see. Pure.
///
/// A `Restore` with no `evidenceDestinationRef` is an inline-archive run: its
/// runner writes the signed scorecard to the plan's `evidence:` block with the
/// archive credential, and the controller reads it through its one global
/// handle (built from `LOGWEIR_ARCHIVE_URL`). So such a run is verified exactly
/// when its plan writes the evidence to that handle's bucket, which the
/// console's wizard defaults to. Any other plan is not read and publishes
/// `NotAttempted` naming both buckets
/// ([`crate::destination::LegacyEvidenceScope`]); reading it there would look
/// the key up where the document never was.
///
/// A destination-backed answer is returned unchanged. A plan that does not
/// parse (`None`), which admission and the runner both refuse, keeps the
/// answer it had: a run already started is observed, never refused.
#[must_use]
pub fn legacy_evidence_source(
    from: backup::EvidenceSource,
    plan: Option<&RestoreSpec>,
    archive_url: Option<&str>,
) -> backup::EvidenceSource {
    if !matches!(from, backup::EvidenceSource::GlobalHandle) {
        return from;
    }
    let Some(plan) = plan else {
        return from;
    };
    match crate::destination::legacy_evidence_scope(&plan.evidence, archive_url)
        .not_attempted_detail("signed scorecard")
    {
        Some(detail) => backup::EvidenceSource::NotAttempted { detail },
        None => from,
    }
}

/// The `NotAttempted` detail for an INLINE-ARCHIVE run whose runner printed
/// both evidence keys and whose read through the controller's archive handle
/// produced no document.
///
/// It names the key, the handle by role
/// ([`crate::destination::ARCHIVE_HANDLE_LABEL`]) and the three causes (the
/// handle is not configured, the object is not in its bucket, or the
/// credential cannot read it), because the oracle collapses them into one
/// "nothing". The handle's URL is installation configuration and is not in
/// it: this detail lands in a status every operator of the namespace can read.
///
/// It begins with [`crate::verification::UNREAD_SCORECARD_PREFIX`], which
/// makes it a transient `NotAttempted`: the read is retried, because the
/// cause may be a credential that arrives later.
#[must_use]
pub fn unread_scorecard_detail(key: &str) -> String {
    format!(
        "{}{key}` and weirkeeper read no such document through {}: the handle is not \
         configured, the object is not in the bucket it reads, or the controller's credential \
         cannot read it. Nothing was verified and no completion is written; run the printed \
         logweir drill verify command",
        crate::verification::UNREAD_SCORECARD_PREFIX,
        crate::destination::ARCHIVE_HANDLE_LABEL
    )
}

/// [`unread_scorecard_detail`]'s twin for a run whose EVIDENCE destination
/// reads with `ControllerIdentity`: the read went through that destination's
/// own handle, so the sentence names the destination (a namespaced object the
/// reader can open) and not the installation's archive handle.
#[must_use]
pub fn unread_destination_scorecard_detail(key: &str, destination: &str) -> String {
    format!(
        "{}{key}` and weirkeeper read no such document through BackupDestination \
         {destination}'s evidenceRead handle: the object is not in its bucket, or the \
         controller's credential cannot read it. Nothing was verified and no completion is \
         written; run the printed logweir drill verify command",
        crate::verification::UNREAD_SCORECARD_PREFIX,
    )
}

/// What the terminal pass does about a finished `Restore`'s evidence. Pure.
#[derive(Debug)]
pub enum TerminalEvidence {
    /// Record this verdict; there is nothing to read. Boxed: the verdict is
    /// several times the size of the other arms (clippy `large_enum_variant`).
    Record(Box<crate::verification::VerificationResult>),
    /// Verify this reference through the source's own handle.
    Verify(EvidenceRef),
    /// Write nothing on this pass: an evidence-fetch Job's pass writes the
    /// verdict, or no scorecard was written at all.
    Nothing,
}

/// The terminal pass's evidence choice, from where the evidence is read and
/// what the controller's own read of the scorecard produced. Pure, so every
/// source is a table row.
///
/// * a source that refused (`NotAttempted`), when a scorecard was written:
///   record the refusal's own sentence;
/// * an evidence-fetch Job's source: nothing here; the Job's pass writes it;
/// * a source the controller reads (its archive handle, or a
///   `ControllerIdentity` destination) whose read produced a digest: verify it
///   through the same handle, with no second verification path;
/// * the same with no digest, when a scorecard was written: record the unread
///   sentence ([`unread_scorecard_verdict`]), so it can be read again.
///   `NotAttempted` is the only verdict writable without bytes;
/// * no keys: nothing.
#[must_use]
pub fn terminal_evidence(
    from: &backup::EvidenceSource,
    reference: Option<EvidenceRef>,
    keys_complete: bool,
    scorecard_key: &str,
    destination: Option<&str>,
) -> TerminalEvidence {
    match (from, reference) {
        (backup::EvidenceSource::NotAttempted { detail }, _) if keys_complete => {
            TerminalEvidence::Record(Box::new(
                crate::verification::VerificationResult::not_attempted(
                    logweir_verify::PAYLOAD_TYPE_SCORECARD,
                    detail.clone(),
                ),
            ))
        }
        (backup::EvidenceSource::FetchJob { .. }, _) => TerminalEvidence::Nothing,
        (
            backup::EvidenceSource::GlobalHandle | backup::EvidenceSource::Destination(_),
            Some(reference),
        ) => TerminalEvidence::Verify(reference),
        (
            from @ (backup::EvidenceSource::GlobalHandle | backup::EvidenceSource::Destination(_)),
            None,
        ) => unread_scorecard_verdict(from, keys_complete, scorecard_key, destination)
            .map_or(TerminalEvidence::Nothing, |r| {
                TerminalEvidence::Record(Box::new(r))
            }),
        _ => TerminalEvidence::Nothing,
    }
}

/// The verdict for a finished run whose runner printed both scorecard keys and
/// whose controller-side read produced no digest: `Some` for a run the
/// controller reads (its archive handle, or a `ControllerIdentity` evidence
/// destination). Pure.
///
/// A failed read there is transient, and the run can only be read again if
/// the attempt is recorded; otherwise a `Succeeded` restore would carry no
/// verification block at all. A run with no keys, and a run whose evidence a
/// Job or nothing reads, get no verdict here.
#[must_use]
pub fn unread_scorecard_verdict(
    from: &backup::EvidenceSource,
    keys_complete: bool,
    scorecard_key: &str,
    destination: Option<&str>,
) -> Option<crate::verification::VerificationResult> {
    if !keys_complete {
        return None;
    }
    let detail = match from {
        backup::EvidenceSource::GlobalHandle => unread_scorecard_detail(scorecard_key),
        backup::EvidenceSource::Destination(_) => {
            unread_destination_scorecard_detail(scorecard_key, destination.unwrap_or_default())
        }
        backup::EvidenceSource::FetchJob { .. } | backup::EvidenceSource::NotAttempted { .. } => {
            return None
        }
    };
    Some(crate::verification::VerificationResult::not_attempted(
        logweir_verify::PAYLOAD_TYPE_SCORECARD,
        detail,
    ))
}

/// `(oldTopics, newTopics)` — the source names and the mapped target names.
///
/// Derived from `spec.planBytes`, not the scorecard, which carries only a
/// digest and a count of the mapping: `source.topics` verbatim for the old
/// names, and each behind [`logweir_core::spec::target_topic_prefix`] for the
/// new. That function is the one place the mapping rule lives, so the
/// renderer, the runner's refusals and this list cannot derive it differently.
/// It reads no clock or environment.
///
/// Parsing `planBytes` to answer a question is not the round trip it forbids;
/// the ConfigMap still gets the bytes verbatim ([`plan_config_map`]). When the
/// bytes are not a `RestoreSpec` the caller has none, rather than a fabricated
/// list naming topics no run touched. Nothing in v1 reads the two lists, and
/// nothing writes to `oldTopics`' topics.
#[must_use]
pub fn topic_mapping(plan: &RestoreSpec) -> (Vec<String>, Vec<String>) {
    let prefix = logweir_core::spec::target_topic_prefix(plan);
    let old: Vec<String> = plan.source.topics.clone();
    let new: Vec<String> = old.iter().map(|t| format!("{prefix}{t}")).collect();
    (old, new)
}

/// Whether a run's exit code and scorecard `outcome` mean the archive did not
/// cover the requested window.
///
/// Exit **2** (a non-pass result, signed) whose `outcome` is
/// [`OUTCOME_FAIL_COVERAGE`] is [`TERMINAL_STATE_WINDOW_NOT_COVERED`]. Every
/// other pairing is `None` and keeps the runner's wire reason.
#[must_use]
pub fn window_not_covered(exit_code: i32, outcome: Option<&str>) -> Option<&'static str> {
    if exit_code == 2 && outcome == Some(OUTCOME_FAIL_COVERAGE) {
        return Some(TERMINAL_STATE_WINDOW_NOT_COVERED);
    }
    None
}

/// One condition, as a merge-patch fragment.
///
/// `lastTransitionTime` moves only on a real transition, decided by
/// [`crate::conditions::merge_condition`], the one implementation for every
/// reconciler. Built by serialising [`crate::crds::Condition`] so the element
/// compares byte for byte against the stored one; see `backup::condition` for
/// why a hand-built element re-opens the loop.
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

/// Patch `/status` under seam **S7**'s `metadata.resourceVersion`
/// precondition, and not at all when the patch would change nothing.
///
/// Both decisions are [`crate::conditions::patch_status_preconditioned`]'s;
/// this keeps every patch site in this reconciler one line, so neither rule
/// can be forgotten at one of them. The precondition is the object this pass
/// observed; a second write in the same pass uses [`patch_status_at`].
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
/// compare-and-set is against what this controller stored a moment ago, not
/// the version the watch delivered. Otherwise the second patch of every
/// terminal pass would get a `409` the reconciler cannot retry (the object is
/// terminal and step 2b writes nothing), and the evidence verdict would be
/// lost.
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

/// `restore` as it stands after `patch` was stored at `at`: what a second
/// write of one pass builds from and preconditions on (seam **S7**).
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

/// One pass over this run's evidence-fetch Job: the `Restore` twin of
/// `controllers::backup`'s, on `scorecard-key` / `sidecar-key` and the
/// evidence destination's `evidenceRead` grant. It differs from the `Backup`
/// half in three ways:
///
/// * **The digest.** A restore runner reports no scorecard digest, so the
///   digest is computed here over the relayed bytes and the signature is
///   checked against it; `scorecardSha256` records it.
/// * **The binding.** The relayed document's own `run_id` must be the one the
///   key `logweir/drills/<run_id>.json` names. Another run's scorecard copied
///   to this key, however validly signed, is `Invalid` and projects nothing.
/// * **The facts.** `outcome`, `lastPhaseCompleted`, `objectives`,
///   `integrity`, `measured` and the offset report's digest are copied by
///   `scorecard_observation`, as on the own-handle path, whenever the document
///   was read and is bound to this run, whatever the verdict. The badge is
///   still green only on `Valid` with `outcome: pass`; `completion` is copied
///   only on `Valid` ([`completion_patch_value`]).
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
        // What this fetch is for decides how much of it the controller holds
        // (`Request::payload_cap`).
        payload_type,
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
                    // Parsed only within a scorecard's cap, here too.
                    let claimed_run = within_scorecard_cap(&payload)
                        .and_then(|bytes| serde_json::from_slice::<Value>(bytes).ok())
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
                                client, &reference, &payload, &sidecar, now,
                            )
                            .await;
                            if let Some(o) = scorecard_observation(&payload) {
                                (evidence_facts, facts) = scorecard_facts(&o, &result, status);
                            }
                            result
                        }
                        // A document whose own `run_id` is not the one its key
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
            // A transient relay failure is retried with the next Job.
            let (result, retry) = crate::evidence_fetch::relayed_schedule(result, attempt, now);
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
                    retry,
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

/// The facts a read scorecard puts on a terminal `Restore`'s status beside its
/// verdict: the one rule for the evidence-fetch relay and the controller
/// read's retry (the terminal pass writes the same on its terminal patch).
/// The digest and the offset report's digest under `status.evidence`; and
/// `outcome`, `lastPhaseCompleted`, `objectives`, `integrity`, `measured` and
/// the archive-coverage `exitReason` whenever the document was read;
/// `completion` only beside a `Valid` verdict on a run that passed
/// ([`completion_patch_value`]).
///
/// Returns `(evidence_facts, facts)`. Public so the relay path's facts can be
/// asserted without a relay.
#[must_use]
pub fn scorecard_facts(
    o: &ScorecardObservation,
    result: &crate::verification::VerificationResult,
    status: Option<&crate::crds::restore::RestoreStatus>,
) -> (
    serde_json::Map<String, Value>,
    serde_json::Map<String, Value>,
) {
    let mut evidence_facts = serde_json::Map::new();
    let mut facts = serde_json::Map::new();
    if let Some(d) = o.scorecard_sha256.as_ref() {
        evidence_facts.insert("scorecardSha256".to_string(), json!(d));
    }
    if let Some(d) = o.offset_report_sha256.as_ref() {
        evidence_facts.insert("offsetReportSha256".to_string(), json!(d));
    }
    if let Some(v) = o.outcome.as_ref() {
        facts.insert("outcome".to_string(), json!(v));
    }
    if let Some(v) = o.last_phase_completed {
        facts.insert("lastPhaseCompleted".to_string(), json!(v));
    }
    // Only beside a valid verdict on a run that passed.
    if let Some(c) = completion_patch_value(
        o,
        result,
        status.and_then(|s| serde_json::to_value(s).ok()).as_ref(),
        status.and_then(|s| s.exit_code),
    ) {
        facts.insert("completion".to_string(), c);
    }
    for (key, block) in [
        ("objectives", objectives_block(o)),
        ("integrity", integrity_block(o)),
        ("measured", measured_block(o)),
    ] {
        if !block.is_empty() {
            facts.insert(key.to_string(), Value::Object(block));
        }
    }
    // The signed time basis, so the console can show what the run selected by
    // producer time and what it selected with an unrecorded type.
    if let Some(v) = time_basis_value(o) {
        facts.insert("timeBasis".to_string(), v);
    }
    if let Some(state) = window_not_covered(
        status.and_then(|s| s.exit_code).unwrap_or(0),
        o.outcome.as_deref(),
    ) {
        facts.insert("exitReason".to_string(), json!(state));
    }
    (evidence_facts, facts)
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
    reads: &Reread<'_>,
) -> Result<Requeue, RestoreError> {
    // First, a read the controller owes this run (see the `Backup` twin),
    // decided from the stored status alone. A terminal `Restore` otherwise
    // waits for a change, so a scheduled attempt needs a requeue, but only
    // while the read is still possible: a schedule whose run can no longer be
    // read (the archive handle was removed or re-pointed) is settled on this
    // pass, never waited for.
    let mut controller_wait = None;
    match restore_read_plan(restore, now, reads) {
        crate::evidence_fetch::ControllerRead::Due(attempt) => {
            controller_read_pass(restore, client, namespace, now, attempt, reads).await?;
            return Ok(Requeue::After(REQUEUE_SECS));
        }
        crate::evidence_fetch::ControllerRead::Settle(why) => {
            settle_controller_read(restore, client, namespace, now, why).await?;
            return Ok(Requeue::AwaitChange);
        }
        crate::evidence_fetch::ControllerRead::Scheduled(at) => {
            controller_wait = Some(Requeue::After(crate::evidence_fetch::seconds_until(
                at, now,
            )));
        }
        crate::evidence_fetch::ControllerRead::Nothing => {}
    }
    let evidence = restore.status.as_ref().and_then(|s| s.evidence.as_ref());
    let result = evidence
        .and_then(|e| e.verification.as_ref())
        .and_then(|v| v.result.as_deref());
    // Only an evidence-fetch Job's observation may make `owed_attempt` start a
    // Job or `retry_due_in` time one; a controller read's is planned above.
    let observation = evidence
        .and_then(|e| e.observation.as_ref())
        .filter(|o| !crate::evidence_fetch::is_controller_read(o.mode.as_deref()));
    // The crash window (see the `Backup` twin): a terminal pass that stopped
    // between the terminal patch and the recorded fetch.
    let unrecorded = result.is_none()
        && observation.is_none()
        && restore.spec.evidence_destination_ref.is_some()
        && evidence.is_some_and(|e| e.scorecard_key.is_some() && e.sidecar_key.is_some());
    let owed = crate::evidence_fetch::owed_attempt(result, observation, now);
    let waiting = match crate::evidence_fetch::retry_due_in(result, observation, now) {
        Some(secs) => Requeue::After(secs),
        None => controller_wait.unwrap_or(Requeue::AwaitChange),
    };
    if owed.is_none() && !unrecorded {
        // The verdict is committed, but its Job's TTL may not be: a failed
        // `set_ttl` returned an error, and the error policy's requeue lands
        // here.
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
    let restores: Api<Restore> = Api::namespaced(client.clone(), namespace);
    let attempt = match (owed, &source) {
        (Some(attempt), _) => attempt,
        (None, backup::EvidenceSource::FetchJob { .. }) => 1,
        // DUI-38 review L1: the crash window, and the source is no longer a Job's
        // (the destination was edited or deleted before the requeue). Record the
        // verdict the terminal pass records for that source (`terminal_evidence`),
        // so the run is never `pending` for ever: the API reads an owed Job read
        // as pending, and nothing else would write one.
        (None, from) if unrecorded => {
            let Some((result, observation)) = crash_window_verdict(
                from,
                evidence
                    .and_then(|e| e.scorecard_key.as_deref())
                    .unwrap_or_default(),
                restore
                    .spec
                    .evidence_destination_ref
                    .as_ref()
                    .map(|r| r.name.as_str()),
                now,
            ) else {
                return Ok(waiting);
            };
            write_fetch_verdict(
                &restores,
                restore,
                &restore.name_any(),
                &StatusVersion::observed(restore.meta()),
                &result,
                observation,
                serde_json::Map::new(),
                serde_json::Map::new(),
                now,
            )
            .await?;
            return Ok(Requeue::After(REQUEUE_SECS));
        }
        (None, _) => return Ok(waiting),
    };
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

/// What the crash-window arm of `continue_evidence_fetch` records for a run
/// whose owed Job read can no longer be made, because its source is no longer
/// a Job's (DUI-38 review L1): the verdict the terminal pass records for that
/// source with no reference ([`terminal_evidence`]), and, for a controller
/// read, attempt 1 of its read schedule as the terminal pass writes it (the
/// observation `{mode, attempt: 1, retryAfter}`), so the read is tried again.
/// `None` for a source that records nothing here (a Job's, or none). Pure.
#[must_use]
pub fn crash_window_verdict(
    from: &backup::EvidenceSource,
    scorecard_key: &str,
    destination: Option<&str>,
    now: DateTime<Utc>,
) -> Option<(crate::verification::VerificationResult, Value)> {
    let TerminalEvidence::Record(result) =
        terminal_evidence(from, None, true, scorecard_key, destination)
    else {
        return None;
    };
    Some(match backup::controller_read_mode(from) {
        Some(mode) => {
            let (result, retry) = crate::evidence_fetch::scheduled(*result, 1, now);
            let observation = retry.map_or(Value::Null, |retry| {
                crate::evidence_fetch::observation_patch(Some(mode), None, 1, None, Some(retry))
            });
            (result, observation)
        }
        None => (*result, Value::Null),
    })
}

/// What a terminal pass needs to read a run's scorecard AGAIN — the `Backup`
/// twin's [`backup::Reread`], over the scorecard oracle.
#[derive(Clone, Copy)]
pub struct Reread<'a> {
    /// The terminal pass's scorecard oracle.
    pub scorecard: ScorecardOracle<'a>,
    /// The terminal pass's verification oracle.
    pub verify: VerifyOracle<'a>,
    /// `LOGWEIR_ARCHIVE_URL`, the global handle's location.
    pub archive_url: Option<&'a str>,
    /// Which runs this process has read (`None`: continue a scheduled retry,
    /// never start a new schedule).
    pub memory: Option<&'a crate::evidence_fetch::ReadMemory>,
}

/// One verification through the handle `source` names (the `Backup` twin's
/// `read_verdict`): the injected oracle for the global handle, and the same
/// `verify_oracle` over the evidence destination's own handle for
/// `ControllerIdentity`, so there is no second verification path.
async fn read_verdict(
    source: &backup::EvidenceSource,
    reference: EvidenceRef,
    verify: VerifyOracle<'_>,
    client: &kube::Client,
    now: DateTime<Utc>,
) -> crate::verification::VerificationResult {
    match source {
        backup::EvidenceSource::Destination(store) => {
            crate::verification::verify_oracle(Some(Arc::clone(store)), client.clone(), now)(
                reference,
            )
            .await
        }
        _ => verify(reference).await,
    }
}

/// What this terminal `Restore`'s controller read calls for now
/// ([`crate::evidence_fetch::plan_controller_read`]), after the checks that
/// make a read possible: a `NotAttempted` verdict, both mandatory evidence keys
/// on the status, and, for an inline-archive run, a configured archive handle
/// whose bucket the approved plan writes its evidence to. A run that fails the
/// last check while an attempt is scheduled settles on that sentence rather
/// than being requeued for an attempt that can never be made. Pure.
#[must_use]
pub fn restore_read_plan(
    restore: &Restore,
    now: DateTime<Utc>,
    reads: &Reread<'_>,
) -> crate::evidence_fetch::ControllerRead {
    use crate::evidence_fetch::ControllerRead;
    let Some(evidence) = restore.status.as_ref().and_then(|s| s.evidence.as_ref()) else {
        return ControllerRead::Nothing;
    };
    let Some(verification) = evidence.verification.as_ref() else {
        return ControllerRead::Nothing;
    };
    if evidence.scorecard_key.is_none() || evidence.sidecar_key.is_none() {
        return ControllerRead::Nothing;
    }
    let inline = restore.spec.evidence_destination_ref.is_none();
    let readable = if inline {
        match reads.archive_url {
            None => Err(crate::evidence_fetch::archive_handle_gone_detail(
                "signed scorecard",
            )),
            Some(handle) => match legacy_evidence_source(
                backup::EvidenceSource::GlobalHandle,
                parse_plan(restore).ok().as_ref(),
                Some(handle),
            ) {
                backup::EvidenceSource::NotAttempted { detail } => Err(detail),
                _ => Ok(()),
            },
        }
    } else {
        Ok(())
    };
    let uid = restore.uid();
    crate::evidence_fetch::plan_controller_read(
        verification.result.as_deref(),
        verification.detail.as_deref(),
        evidence.observation.as_ref(),
        inline,
        readable,
        now,
        reads.memory.zip(uid.as_deref()),
    )
}

/// [`restore_read_plan`] as the attempt this `Restore` owes now, if any.
#[must_use]
pub fn owed_restore_read(restore: &Restore, now: DateTime<Utc>, reads: &Reread<'_>) -> Option<u32> {
    match restore_read_plan(restore, now, reads) {
        crate::evidence_fetch::ControllerRead::Due(attempt) => Some(attempt),
        _ => None,
    }
}

/// Record a scheduled controller read that can no longer happen (the `Backup`
/// twin's `settle_controller_read`).
async fn settle_controller_read(
    restore: &Restore,
    client: &kube::Client,
    namespace: &str,
    now: DateTime<Utc>,
    why: String,
) -> Result<(), RestoreError> {
    let observation = restore
        .status
        .as_ref()
        .and_then(|s| s.evidence.as_ref())
        .and_then(|e| e.observation.as_ref());
    let mode = observation
        .and_then(|o| o.mode.clone())
        .unwrap_or_else(|| crate::evidence_fetch::MODE_ARCHIVE_HANDLE.to_string());
    let attempt = observation
        .and_then(|o| o.attempt)
        .and_then(|a| u32::try_from(a).ok())
        .unwrap_or(1);
    let name = restore.name_any();
    info!(
        restore = %name,
        namespace = %namespace,
        attempt,
        "this finished Restore's scheduled scorecard read can no longer be made; recorded, and \
         no further attempt is scheduled"
    );
    let restores: Api<Restore> = Api::namespaced(client.clone(), namespace);
    write_fetch_verdict(
        &restores,
        restore,
        &name,
        &StatusVersion::observed(restore.meta()),
        &crate::verification::VerificationResult::not_attempted(
            logweir_verify::PAYLOAD_TYPE_SCORECARD,
            why,
        ),
        crate::evidence_fetch::observation_patch(Some(&mode), None, attempt, None, None),
        serde_json::Map::new(),
        serde_json::Map::new(),
        now,
    )
    .await?;
    Ok(())
}

/// One controller read of a terminal `Restore`'s scorecard, attempt
/// `attempt`, and the one status write it implies: the terminal pass's
/// evidence half run again (same source resolution, reader and verifier), with
/// the facts the terminal pass would have written ([`scorecard_facts`];
/// `completion` only beside `Valid`), plus the observation recording this
/// attempt and, for a transient failure, the next.
async fn controller_read_pass(
    restore: &Restore,
    client: &kube::Client,
    namespace: &str,
    now: DateTime<Utc>,
    attempt: u32,
    reads: &Reread<'_>,
) -> Result<(), RestoreError> {
    // At most `CONTROLLER_READ_CONCURRENCY` of these in flight per process.
    // The semaphore is never closed, so `acquire` cannot fail; a failure would
    // only mean the read is not throttled, never that it is skipped.
    let _permit = crate::evidence_fetch::controller_read_permits()
        .acquire()
        .await
        .ok();
    use crate::verification::VerificationResult;
    let payload_type = logweir_verify::PAYLOAD_TYPE_SCORECARD;
    let name = restore.name_any();
    let status = restore.status.as_ref();
    let evidence = status.and_then(|s| s.evidence.as_ref());
    let (Some(payload_key), Some(sidecar_key)) = (
        evidence.and_then(|e| e.scorecard_key.clone()),
        evidence.and_then(|e| e.sidecar_key.clone()),
    ) else {
        return Ok(());
    };
    let routed = backup::evidence_source_for(
        restore.spec.evidence_destination_ref.as_ref(),
        client,
        namespace,
        now,
    )
    .await
    .map_err(RestoreError::Api)?;
    let source =
        legacy_evidence_source(routed, parse_plan(restore).ok().as_ref(), reads.archive_url);
    let destination = restore
        .spec
        .evidence_destination_ref
        .as_ref()
        .map(|r| r.name.as_str());
    let mode = backup::controller_read_mode(&source).unwrap_or(if destination.is_some() {
        crate::evidence_fetch::MODE_CONTROLLER_IDENTITY
    } else {
        crate::evidence_fetch::MODE_ARCHIVE_HANDLE
    });
    let observed = match &source {
        backup::EvidenceSource::GlobalHandle => (reads.scorecard)(payload_key.clone()).await,
        backup::EvidenceSource::Destination(store) => {
            let handle = Arc::clone(store);
            let key = payload_key.clone();
            tokio::task::spawn_blocking(move || observe_scorecard(&handle, &key))
                .await
                .ok()
                .flatten()
        }
        backup::EvidenceSource::FetchJob { .. } | backup::EvidenceSource::NotAttempted { .. } => {
            None
        }
    };
    // A scorecard over the read cap is not observed, and its sentence is the
    // verdict.
    let (observed, refused) = split_refused(observed);
    let digest = observed.as_ref().and_then(|o| o.scorecard_sha256.clone());
    let result = match (&source, digest) {
        (backup::EvidenceSource::NotAttempted { detail }, _) => {
            VerificationResult::not_attempted(payload_type, detail.clone())
        }
        (backup::EvidenceSource::FetchJob { destination, .. }, _) => {
            VerificationResult::not_attempted(
                payload_type,
                format!(
                    "BackupDestination {}/{} now reads evidence with a grant only a pod may \
                     hold; this run's scorecard was read by the controller and is not read again \
                     here — run the printed logweir drill verify command",
                    destination.namespace, destination.name
                ),
            )
        }
        (source, Some(digest)) => {
            read_verdict(
                source,
                EvidenceRef {
                    namespace: namespace.to_string(),
                    payload_key: payload_key.clone(),
                    payload_sha256: digest,
                    sidecar_key,
                    payload_type,
                },
                reads.verify,
                client,
                now,
            )
            .await
        }
        (_, None) if refused.is_some() => {
            VerificationResult::not_attempted(payload_type, refused.unwrap_or_default())
        }
        (source, None) => unread_scorecard_verdict(source, true, &payload_key, destination)
            .unwrap_or_else(|| {
                VerificationResult::not_attempted(
                    payload_type,
                    unread_scorecard_detail(&payload_key),
                )
            }),
    };
    let (result, retry) = crate::evidence_fetch::scheduled(result, attempt, now);
    let (evidence_facts, facts) = match observed.as_ref() {
        Some(o) => scorecard_facts(o, &result, status),
        None => (serde_json::Map::new(), serde_json::Map::new()),
    };
    info!(
        restore = %name,
        namespace = %namespace,
        attempt,
        mode,
        verification = %result.result,
        retry_after = ?retry,
        "weirkeeper read this finished Restore's scorecard again"
    );
    let restores: Api<Restore> = Api::namespaced(client.clone(), namespace);
    write_fetch_verdict(
        &restores,
        restore,
        &name,
        &StatusVersion::observed(restore.meta()),
        &result,
        crate::evidence_fetch::observation_patch(Some(mode), None, attempt, None, retry),
        evidence_facts,
        facts,
        now,
    )
    .await?;
    if let (Some(memory), Some(uid)) = (reads.memory, restore.uid()) {
        memory.mark(&uid);
    }
    Ok(())
}

/// The `/status` merge patch for an admission that is a hold rather than a
/// verdict (interface **I19**).
///
/// `phase: Pending` and exactly one condition, `Admitted=False`. Not a
/// `Failed` condition: nothing failed, and a condition array is a map keyed by
/// `type`, so `Failed=False` beside a later `Failed=True` is malformed. No
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
            // The scalar the `REASON` column reads: the condition's `reason`
            // below, verbatim.
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
/// `Valid`.
///
/// The same shape as the approval hold, so a console renders one situation
/// one way. The `reason` is the resolver's own `CheckCode`, the string the
/// `BackupDestination`'s `Valid` condition carries, so the two objects agree
/// about the fault.
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

/// The `/status` patch a manual `Restore` carries while it waits, admitted and
/// its approval verified, for a slot in its namespace's manual-restore pool
/// ([`crate::run_pool`]).
///
/// `phase: Queued`, the scalar `reason`, and `Admitted=False` with reason
/// [`crate::conditions::REASON_CONCURRENCY_LIMITED`]: the shape of the two
/// admission holds, carried rather than replaced so no stored condition is
/// dropped. No plan `ConfigMap`, bundle or Job exists for a queued restore.
/// Stable for a given ceiling and approval, so it is written once.
///
/// The queue does not extend an approval's maximum age: every pass runs the
/// admission first, so an authorization that expires while the run waits
/// refuses it `AuthorizationExpired`. The instant is stated where the operator
/// looks (`status.queue.authorizationExpiresAt` and the condition's message).
/// `None` for an approval that states no expiry (a legacy governed v1
/// approval).
#[must_use]
pub fn queued_status_patch(
    restore: &Restore,
    limit: u32,
    authorization_expires_at: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
) -> Value {
    let namespace = restore.namespace().unwrap_or_default();
    let deadline = match authorization_expires_at {
        Some(at) => format!(
            ". Its approval expires at {}: the queue does not extend an approval's maximum age, \
             so if this Restore is still queued then it is refused `AuthorizationExpired` and \
             must be confirmed again",
            at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
        ),
        None => ". Its approval states no expiry".to_string(),
    };
    let conditions = crate::conditions::carry_remaining(
        restore.status.as_ref().and_then(|s| s.conditions.as_ref()),
        vec![condition(
            restore,
            CONDITION_ADMITTED,
            "False",
            crate::conditions::REASON_CONCURRENCY_LIMITED,
            &format!(
                "{}{deadline}",
                crate::run_pool::queued_message(
                    "Restore",
                    &namespace,
                    limit,
                    crate::run_pool::RESTORE_LIMIT_FIELD,
                )
            ),
            now,
        )],
    );
    let mut queue = json!({ "limit": limit });
    if let Some(at) = authorization_expires_at {
        queue["authorizationExpiresAt"] =
            json!(at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true));
    }
    json!({
        "status": {
            "phase": crate::conditions::PHASE_QUEUED,
            "reason": crate::conditions::REASON_CONCURRENCY_LIMITED,
            "queue": queue,
            "conditions": conditions,
        }
    })
}

/// The admission record for a manual `Restore`: the one write every pass that
/// admits one makes before its trust read, plan, bundle or Job. The `Backup`
/// twin (`controllers::backup::admitted_status_patch`) carries the reasoning.
/// `Admitted=True` is durable: the run is `Occupying` in every process and
/// after every restart.
#[must_use]
pub fn admitted_status_patch(restore: &Restore, limit: u32, now: DateTime<Utc>) -> Value {
    let namespace = restore.namespace().unwrap_or_default();
    let conditions = crate::conditions::upsert_conditions(
        restore.status.as_ref().and_then(|s| s.conditions.as_ref()),
        vec![condition(
            restore,
            CONDITION_ADMITTED,
            "True",
            REASON_ADMITTED,
            &crate::run_pool::admitted_message(
                "Restore",
                &namespace,
                limit,
                crate::run_pool::RESTORE_LIMIT_FIELD,
            ),
            now,
        )],
    );
    json!({
        "status": {
            "queue": Value::Null,
            "reason": REASON_ADMITTED,
            "conditions": conditions,
        }
    })
}

/// When an approval stops authorising anything new: the signed
/// `expires_at` of a v2 authorization document, or `None` for a document that
/// states none (a legacy governed v1 approval) or that does not parse (the
/// admission refuses that one by itself).
#[must_use]
pub fn authorization_deadline(approval: &Approval) -> Option<DateTime<Utc>> {
    if !is_authorization_v2(approval) {
        return None;
    }
    RestoreAuthorization::from_bytes(approval.spec.approval_bytes.as_bytes())
        .ok()
        .map(|doc| doc.expires_at)
}

/// The refusal a restore that waited in the manual-restore pool gets when its
/// approval expired meanwhile: the admission's own sentence, plus how long the
/// line was and that the queue does not extend an approval.
#[must_use]
pub fn expired_while_queued_message(expired: &str, behind: u32, limit: Option<i64>) -> String {
    let ceiling = limit.map_or_else(String::new, |l| format!(" = {l}"));
    format!(
        "{expired}. It expired while this Restore was queued behind {behind} manual restore(s) \
         for a slot (`{}`{ceiling}): the queue does not extend an approval's maximum age. Confirm \
         (or approve) again and create a new Restore",
        crate::run_pool::RESTORE_LIMIT_FIELD
    )
}

/// What the manual-restore gate decided for one pass.
enum RestoreGate {
    /// Go on over this object: the admission record moved its status and its
    /// `resourceVersion` (seam S7).
    Admitted(Box<Restore>),
    /// Stop: queued, or the pool's snapshot is not synced yet. Nothing was
    /// created.
    Wait,
}

/// The manual-restore gate; `controllers::backup`'s `manual_run_gate` is the
/// twin and carries the reasoning. `approval` is the verified approval the
/// admission just passed, read for its deadline.
///
/// # Errors
///
/// [`RestoreError::Api`] for the policy read or a status write.
async fn manual_restore_gate(
    restore: &Restore,
    approval: &Approval,
    client: &kube::Client,
    namespace: &str,
    pool: &crate::run_pool::Pool<'_, Restore>,
    now: DateTime<Utc>,
) -> Result<RestoreGate, RestoreError> {
    let name = restore.name_any();
    let limit = match pool.limit {
        Some(limit) => limit,
        None => crate::run_pool::ceiling(
            crate::run_pool::PoolKind::Restore,
            crate::check::policy::load(
                client,
                crate::run_pool::policy_ref().as_ref(),
                crate::run_pool::policy_cache(),
                now,
            )
            .await
            .map_err(RestoreError::Api)?
            .policy(),
        ),
    };
    let decision = pool.reservations.decide(
        crate::run_pool::PoolKind::Restore,
        restore,
        pool.peers,
        limit,
        crate::run_pool::restore_standing,
        now,
    );
    let restores: Api<Restore> = Api::namespaced(client.clone(), namespace);
    match decision {
        crate::run_pool::Decision::Unsynced => {
            debug!(
                restore = %name,
                namespace = %namespace,
                "the Restore watch has not finished its first list, so the manual-restore pool \
                 cannot be counted; nothing is created for this run until it has"
            );
            Ok(RestoreGate::Wait)
        }
        crate::run_pool::Decision::Queued {
            active,
            ahead,
            limit,
        } => {
            let deadline = authorization_deadline(approval);
            let patch = queued_status_patch(restore, limit, deadline, now);
            let first = !crate::conditions::status_unchanged(
                restore
                    .status
                    .as_ref()
                    .and_then(|s| serde_json::to_value(s).ok())
                    .as_ref(),
                &patch,
            );
            patch_status_if_changed(&restores, restore, &name, patch).await?;
            if first {
                info!(
                    restore = %name,
                    namespace = %namespace,
                    active,
                    ahead,
                    limit,
                    authorization_expires_at = ?deadline,
                    field = crate::run_pool::RESTORE_LIMIT_FIELD,
                    "queued this admitted manual Restore: the namespace's manual-restore pool is \
                     full; nothing is created for it until a slot frees"
                );
            } else {
                debug!(restore = %name, namespace = %namespace, active, ahead, limit, "still queued");
            }
            Ok(RestoreGate::Wait)
        }
        crate::run_pool::Decision::Admit {
            active,
            ahead,
            limit,
        } => {
            let patch = admitted_status_patch(restore, limit, now);
            let at = patch_status_if_changed(&restores, restore, &name, patch.clone()).await?;
            info!(
                restore = %name,
                namespace = %namespace,
                active,
                ahead,
                limit,
                "admitted this manual Restore to its namespace's manual-restore pool; it holds a \
                 slot until it finishes"
            );
            Ok(RestoreGate::Admitted(Box::new(with_status_written(
                restore, &patch, &at,
            ))))
        }
    }
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

/// The `/status` merge patch for a run this controller refused terminally,
/// before any Job was created.
///
/// Terminal, with no `exitCode` (nothing ran) and never a requeue.
/// `exitReason` is [`REASON_OPERATIONAL`] (the run could not be attempted and
/// no artifact was written), and the sub-case is the condition's `reason`.
///
/// A status, not an error: a refusal that reached `error_policy` would requeue
/// every 15 seconds over a CEL-immutable spec, for ever, with an empty status
/// and nothing for `kubectl describe restore` to say.
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
            // `exitReason` above is `operational` for every self-decided
            // refusal, so `reason` carries the sub-case, verbatim the
            // condition's own, for the `REASON` column.
            "reason": reason,
            "conditions": conditions,
        }
    })
}

/// The `/status` merge patch for a Job that exists and has not finished.
///
/// Built as JSON, not by serialising `RestoreStatus`, for merge-patch
/// semantics: an absent key means "leave it alone", which a running reconcile
/// wants for the fields a finished one will write.
#[must_use]
pub fn running_status_patch(
    restore: &Restore,
    job_name: &str,
    admitted: bool,
    now: DateTime<Utc>,
) -> Value {
    let mut conditions = Vec::new();
    // The admission condition is asserted on the creating pass only and
    // carried on every later one. A merge patch replaces `status.conditions`,
    // so leaving the type out of this array would delete the stored
    // condition; `upsert_conditions` keeps every stored condition this patch
    // is not about, verbatim, with its original `lastTransitionTime`.
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
    // Every stored condition this patch is not about is carried here too:
    // the creating pass writes this patch directly, not through
    // `diagnostics::apply`, and its `409 AlreadyExists`-adopt arm passes
    // `admitted: false`, which would otherwise delete a stored `Admitted`.
    // Stored order, because a running object is reconciled again.
    let conditions = crate::conditions::upsert_conditions(
        restore.status.as_ref().and_then(|s| s.conditions.as_ref()),
        conditions,
    );
    json!({
        "status": {
            "phase": PHASE_RUNNING,
            "jobRef": { "name": job_name },
            // The current condition, `JobCreated` on both passes, not the
            // array's head (`Admitted` on the creating pass only): the run's
            // state is "the Job exists and has not finished" throughout.
            "reason": CONDITION_JOB_CREATED,
            "conditions": conditions,
        }
    })
}

/// The `/status` merge patch for a finished Job whose `runner` container
/// terminated.
///
/// `exitCode` and the condition are decided from the pod. Everything else
/// (`outcome`, `integrity`, `measured`, `objectives`, `lastPhaseCompleted` and
/// the evidence digests) is copied verbatim from the signed scorecard through
/// [`ScorecardObservation`], and omitted when the archive was not observed:
/// an absent key means "leave it alone".
///
/// `status.topicPreflight` is written only from the runner's
/// `topic-preflight=` line; absent, the field stays absent rather than
/// fabricated.
///
/// `EvidenceRecorded` exists at exit 0, and at exit 2 only when the keys were
/// read, over the two mandatory keys. Exits 1, 3 and 4 write no artifact, so
/// no key lines is the expected shape there. At exit 2 only the positive arm
/// is raised: a runner that predates signed exit-2 evidence prints no keys,
/// and its log is not "unreadable".
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
    finished_status_patch_with_reason(
        restore, exit_code, keys, refusal, None, observed, topics, preflight, now,
    )
}

/// [`finished_status_patch`] with what an exit-3 run's pod log said about why
/// ([`crate::refusal::RunnerReason`]).
///
/// The reason is appended to the terminal condition's message, so
/// `kubectl get restore -o yaml` and the console say why the plan was refused
/// once the pod and its log are gone; `status.progress.message` follows via
/// [`diagnostics::apply_finished`]. `None` (any other exit code, or no reason
/// read) and `Some(NotStated)` (an older runner's log) leave the message
/// unchanged. `exitReason` stays the closed state off `refusal-reason=`.
#[allow(clippy::too_many_arguments)]
#[must_use]
pub fn finished_status_patch_with_reason(
    restore: &Restore,
    exit_code: i32,
    keys: &RestoreEvidenceKeys,
    refusal: Option<&str>,
    runner_reason: Option<&crate::refusal::RunnerReason>,
    observed: Option<&ScorecardObservation>,
    topics: Option<&(Vec<String>, Vec<String>)>,
    preflight: Option<&Value>,
    now: DateTime<Utc>,
) -> Value {
    // Two vocabularies, two fields: the condition's `reason` is CamelCase, as
    // `metav1.Condition` validation requires; `exitReason` keeps the runner's
    // wire string, which the CRD's field description documents.
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
             status.containerStatuses[name={CONTAINER_NAME}].state.terminated.exitCode{}",
            runner_reason.map_or_else(String::new, |r| r.message_suffix())
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
    } else if exit_code == 2 && keys.mandatory_complete() {
        // A signed failure, named: exit 2 means a drill ran, did not pass, and
        // wrote a signed scorecard, whose keys are recorded above like exit
        // 0's. Only the positive arm: a runner that predates this prints no
        // keys at exit 2, and "unreadable" would be false about its log.
        // `EvidenceRecorded=True` says the evidence is named; whether the run
        // passed is still `exitCode` (see `verification::restore_badge`).
        conditions.push(condition(
            restore,
            CONDITION_EVIDENCE_RECORDED,
            "True",
            REASON_EVIDENCE_KEYS_RECORDED,
            "the drill did not pass (exit 2) and signed its result; both `scorecard-key=` and \
             `sidecar-key=` were read off the pod log by name and are on status.evidence, so the \
             signed failure is verified like any other scorecard",
            now,
        ));
    }

    // The terminal state, when there is one, is the most specific thing known
    // about the run: the archive-coverage verdict on exit 2, or the guard's
    // own state on exit 3. Otherwise the runner's wire reason.
    let coverage = window_not_covered(exit_code, observed.and_then(|o| o.outcome.as_deref()));
    let exit_reason = coverage.or(refusal).unwrap_or(wire_reason);

    let mut status = serde_json::Map::new();
    status.insert("phase".to_string(), json!(phase));
    status.insert("exitCode".to_string(), json!(exit_code));
    status.insert("exitReason".to_string(), json!(exit_reason));
    // The scalar the `REASON` column reads: the reason of the terminal
    // condition (`conditions[0]`), never the `EvidenceRecorded` one appended
    // after it.
    status.insert("reason".to_string(), json!(cond_reason));
    // The existing `Verified` condition is carried forward. A merge patch
    // replaces arrays, so this one would delete the condition the second
    // patch adds, which would re-add it and wake this reconciler again: a
    // hot loop. See `verification::carry_verified`.
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
        // The signed time basis, by the helper the evidence-fetch relay uses.
        if let Some(v) = time_basis_value(o) {
            status.insert("timeBasis".to_string(), v);
        }
        // `completion` is not here: it is published only beside a `Valid`
        // verdict (`completion_patch_value`), and this write lands before any
        // verification has run.
    }

    if let Some((old, new)) = topics {
        status.insert("oldTopics".to_string(), json!(old));
        status.insert("newTopics".to_string(), json!(new));
    }

    // Guard **G-TS**. Omitted when the runner printed no preflight line, so an
    // absent observation never overwrites one a previous pass recorded.
    if let Some(p) = preflight {
        status.insert("topicPreflight".to_string(), p.clone());
    }

    json!({ "status": Value::Object(status) })
}

/// The closed state of a stopped creation step, read off a Restore runner's
/// log: `TargetTopicAppeared` or `CreatedTopicsLeft`, beside exit 1.
///
/// Three rules, each to the safer side:
///
/// - **per kind**: only a restore's two states
///   (`logweir_core::guard::RESTORE_FAILURE_REASONS`);
/// - **paired with the code**: exit 1 only;
/// - **held to its place**: `failure-reason=` must be the last non-empty line
///   of the log, where `logweir::drill::print_creation_stop_to` prints it.
///
/// The place rule cannot tell the runner's own last line from text the runner
/// was made to print last: stdout and stderr share one unordered stream, and
/// a runner built before `logweir::exit::one_line` prints an error's text raw,
/// so an error ending with a plan's string could forge the pair. This build's
/// runner escapes every line break it prints; for an older runner image the
/// names are shown as its log gives them.
#[must_use]
pub fn creation_stop_state(exit_code: i32, log: &str) -> Option<&'static str> {
    let tail = runner_log::tail_lines(log);
    let value = tail
        .last()?
        .strip_prefix(logweir_core::guard::FAILURE_REASON_PREFIX)?;
    logweir_core::guard::failure_reason_for_exit(
        logweir_core::refusal_detail::RefusingRun::Restore,
        exit_code,
        value.trim(),
    )
}

/// The three lists of a stopped creation step, as this Restore may show them.
/// `None` unless every one of these holds:
///
/// - the run's last non-empty log line is its closed state
///   ([`creation_stop_state`]) and the line before it is
///   `target-topics-appeared=`, in the order and place the runner prints them;
/// - the line's value is no longer than a genuine one and is a JSON object
///   (`logweir_core::creation_stop::CreationStopLists::parse_value`, which
///   refuses a longer value before parsing and keeps only the three lists,
///   only names a broker accepts, at most 100 each, with counts);
/// - each name is a mapped target of this Restore's plan (`mapped`,
///   [`topic_mapping`]'s new names); any other name is not this run's to speak
///   of and is dropped, and a count is held to the number the plan maps;
/// - something remains. A block with no name left is dropped whole.
#[must_use]
pub fn creation_stop_lists(
    exit_code: i32,
    log: &str,
    mapped: &[String],
) -> Option<logweir_core::creation_stop::CreationStopLists> {
    use logweir_core::creation_stop::CreationStopLists;
    creation_stop_state(exit_code, log)?;
    let tail = runner_log::tail_lines(log);
    let line = tail.len().checked_sub(2).and_then(|i| tail.get(i))?;
    let lists =
        CreationStopLists::parse_value(line.strip_prefix(TARGET_TOPICS_APPEARED_KEY_PREFIX)?)?;
    let keep = |name: &str| mapped.iter().any(|m| m == name);
    let held = CreationStopLists {
        appeared: lists.appeared.held_to(keep, mapped.len()),
        left: lists.left.held_to(keep, mapped.len()),
        unconfirmed: lists.unconfirmed.held_to(keep, mapped.len()),
        unconfirmed_seen: lists.unconfirmed_seen,
    };
    (!held.is_empty()).then_some(held)
}

/// `status.targetTopicsAppeared` for `lists`: the three lists, a count beside
/// each, and `unconfirmedSeen` only beside an unconfirmed name.
#[must_use]
pub fn creation_stop_status(lists: &logweir_core::creation_stop::CreationStopLists) -> Value {
    let mut out = serde_json::Map::new();
    out.insert("appeared".into(), json!(lists.appeared.names));
    out.insert("left".into(), json!(lists.left.names));
    out.insert("unconfirmed".into(), json!(lists.unconfirmed.names));
    out.insert("appearedCount".into(), json!(lists.appeared.count));
    out.insert("leftCount".into(), json!(lists.left.count));
    out.insert("unconfirmedCount".into(), json!(lists.unconfirmed.count));
    if !lists.unconfirmed.is_empty() {
        out.insert("unconfirmedSeen".into(), json!(lists.unconfirmed_seen));
    }
    Value::Object(out)
}

/// The terminal patch of a run whose creation step stopped:
/// `status.targetTopicsAppeared` carries the three lists with their counts,
/// and the `Failed` condition's message says which names appeared, which
/// topics this run created and left, and which it asked for and cannot
/// account for, so the operator reads it on the object rather than in a
/// garbage-collected pod log. Nothing was deleted, and the message says what
/// to do with each.
///
/// `status.newTopics` becomes exactly `left`: the topics this run's own
/// `CreateTopics` answers say it created.
#[must_use]
pub fn with_target_topics_appeared(
    mut patch: Value,
    lists: &logweir_core::creation_stop::CreationStopLists,
) -> Value {
    use logweir_core::creation_stop::{and_more, NameList};
    let named = |list: &NameList| -> String {
        let names = list
            .names
            .iter()
            .map(|n| format!("`{n}`"))
            .collect::<Vec<_>>()
            .join(", ");
        format!("{names}{}", and_more(list.more()))
    };
    // The closed state the runner named, as `status.exitReason` carries it,
    // opens the words so a reader of the condition alone can look it up.
    let state = patch
        .pointer("/status/exitReason")
        .and_then(Value::as_str)
        .unwrap_or(logweir_core::guard::TERMINAL_STATE_CREATED_TOPICS_LEFT)
        .to_string();
    let mut words = format!("; the creation step stopped ({state})");
    if !lists.appeared.is_empty() {
        words.push_str(&format!(
            ": mapped target topic(s) {} were created by someone else after phase 0 proved \
             them absent, and nothing was written into them",
            named(&lists.appeared)
        ));
    }
    if !lists.left.is_empty() {
        words.push_str(&format!(
            "; {}: {LEFT_TOPIC_SENTENCE} (Logweir never deletes a topic under a name it may \
             not own)",
            named(&lists.left)
        ));
    } else if lists.unconfirmed.is_empty() {
        words.push_str("; this restore created no topic");
    } else {
        words.push_str("; no CreateTopics answer says this restore created a topic");
    }
    if !lists.unconfirmed.is_empty() {
        words.push_str(&format!(
            "; {}: {} (Logweir deletes none of them)",
            named(&lists.unconfirmed),
            lists.unconfirmed_sentence()
        ));
    }
    if lists.appeared.more() + lists.left.more() + lists.unconfirmed.more() > 0 {
        words.push_str(&format!(
            "; a list shows its first {TARGET_TOPICS_APPEARED_MAX_NAMES} names, each name is one \
             of this restore's mapped target topics, and the runner's log names every one"
        ));
    }
    if let Some(status) = patch.get_mut("status").and_then(Value::as_object_mut) {
        status.insert(
            "targetTopicsAppeared".to_string(),
            creation_stop_status(lists),
        );
        // What this run created is `left`, not every mapped name.
        status.insert("newTopics".to_string(), json!(lists.left.names));
        // The `REASON` scalar stays the terminal condition's; this only adds
        // words to its message.
        let reason = status
            .get("reason")
            .cloned()
            .unwrap_or_else(|| json!(reason_for_exit(1)));
        status.insert("reason".to_string(), reason);
        if let Some(message) = status
            .get_mut("conditions")
            .and_then(Value::as_array_mut)
            .and_then(|c| c.first_mut())
            .and_then(|c| c.get_mut("message"))
        {
            if let Some(text) = message.as_str() {
                *message = json!(format!("{text}{words}"));
            }
        }
    }
    patch
}

/// The terminal patch of a run that named a stopped creation step
/// (`status.exitReason`) but whose lists this Restore cannot show: the line
/// before the reason was absent, too long, not an object, or named nothing
/// this plan maps. The condition says so and what to do, and
/// `status.newTopics` is omitted: which mapped names the run created is not
/// known, and the plan's names are not an answer.
#[must_use]
pub fn with_unread_creation_stop(mut patch: Value) -> Value {
    let state = patch
        .pointer("/status/exitReason")
        .and_then(Value::as_str)
        .unwrap_or(logweir_core::guard::TERMINAL_STATE_CREATED_TOPICS_LEFT)
        .to_string();
    let words = format!(
        "; the creation step stopped ({state}) and the runner's list of topics could not be \
         read, so this restore may have left empty topics under its mapped target names: list \
         the target cluster for them, and check what each holds and who writes to it before \
         you remove it (Logweir never deletes a topic under a name it may not own)"
    );
    if let Some(status) = patch.get_mut("status").and_then(Value::as_object_mut) {
        status.remove("newTopics");
        // The `REASON` scalar stays the terminal condition's; this only adds
        // words to its message.
        let reason = status
            .get("reason")
            .cloned()
            .unwrap_or_else(|| json!(reason_for_exit(1)));
        status.insert("reason".to_string(), reason);
        if let Some(message) = status
            .get_mut("conditions")
            .and_then(Value::as_array_mut)
            .and_then(|c| c.first_mut())
            .and_then(|c| c.get_mut("message"))
        {
            if let Some(text) = message.as_str() {
                *message = json!(format!("{text}{words}"));
            }
        }
    }
    patch
}

/// `status.objectives` (interface **I34**, first half): the scorecard's four
/// values under camelCased names, verbatim. `passRate` and `met` are absent
/// where the scorecard's are null: on a merge patch `null` would mean "leave
/// it alone", the same as omitting it.
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

/// `status.integrity`, including interface **I34**'s `partialReason`: a
/// `partial` result with no reason is a badge an auditor cannot act on. The
/// scorecard's own invariant check refuses a `partial` document with a blank
/// `partial_reason`.
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
    // What the run signed about its coverage, a complete verification's exact
    // counts, and the topics a sampled check left unsampled. Each absent when
    // the document carries none.
    if let Some(v) = o.integrity_coverage.as_ref() {
        m.insert("coverage".to_string(), json!(v));
    }
    if let Some(v) = o
        .integrity_complete
        .as_ref()
        .and_then(|c| serde_json::to_value(c).ok())
    {
        m.insert("complete".to_string(), v);
    }
    if let Some(v) = o.unsampled_topics.as_ref() {
        m.insert("unsampledTopics".to_string(), json!(v));
    }
    // The restore's selection, beside the verdict it qualifies; absent for a
    // restore of everything.
    if let Some(v) = o
        .selection
        .as_ref()
        .and_then(|sel| serde_json::to_value(sel).ok())
    {
        m.insert("selection".to_string(), v);
    }
    // DUI-50: whether the signed scorecard binds a positions mapping.
    if let Some(v) = o
        .positions_mapping
        .as_ref()
        .and_then(|pm| serde_json::to_value(pm).ok())
    {
        m.insert("positionsMapping".to_string(), v);
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

/// `status.completion`, the durable completion report.
///
/// Copied by JSON pointer from the signed scorecard, never recomputed, and
/// every key is absent when the document does not carry it: `newTopics` from
/// `target_diff.would_create`, `recordsExpected`/`recordsRestored` from
/// `sample`, `recordsSampled`/`recordsSampledMatching`/`integrityLevel` from
/// `integrity`, `sampleWindow` from `sample.window_start`/`window_end`. An
/// empty map means the caller writes no `completion`, so a run whose
/// scorecard was never read keeps the field absent rather than reporting
/// zeros nobody signed. Writers go through [`completion_patch_value`], which
/// also requires a `Valid` verdict over the same bytes.
///
/// `newTopics` is a whole-value copy, which a merge patch's array replacement
/// allows only because the array has one source and one writer.
#[must_use]
pub fn completion_block(o: &ScorecardObservation) -> serde_json::Map<String, Value> {
    let mut m = serde_json::Map::new();
    if let Some(topics) = o.would_create.as_ref() {
        m.insert(
            "newTopics".to_string(),
            Value::Array(
                topics
                    .iter()
                    .map(|(name, partitions)| json!({"name": name, "partitions": partitions}))
                    .collect(),
            ),
        );
    }
    for (key, value) in [
        ("recordsExpected", o.sample_records_expected),
        ("recordsRestored", o.sample_records_restored),
        ("recordsSampled", o.integrity_records_sampled),
        (
            "recordsSampledMatching",
            o.integrity_records_sampled_matching,
        ),
    ] {
        if let Some(v) = value {
            m.insert(key.to_string(), json!(v));
        }
    }
    if let Some(v) = o.integrity_level.as_ref() {
        m.insert("integrityLevel".to_string(), json!(v));
    }
    let mut window = serde_json::Map::new();
    if let Some(v) = o.sample_window_start.as_ref() {
        window.insert("start".to_string(), json!(v));
    }
    if let Some(v) = o.sample_window_end.as_ref() {
        window.insert("end".to_string(), json!(v));
    }
    if !window.is_empty() {
        m.insert("sampleWindow".to_string(), Value::Object(window));
    }
    m
}

/// `status.completion` for `o`, or `None` when it must not be written: the
/// run did not pass by the `Restore` badge rule (the verdict over the same
/// bytes is not `Valid` on an accepted basis
/// ([`crate::verification::verification_is_valid`]), `outcome` is not `pass`,
/// or `exit_code` is not `0`), or the document carries nothing to copy.
///
/// The console's completion panel renders topic names and counts with no
/// trust caption of its own, so a document whose signature is `Invalid` or
/// `Untrusted` (counts rewritten under the right `run_id` by someone with
/// write access to the bucket) must not reach it: never on the terminal write
/// (no verdict yet), never beside `NotAttempted`, `Pending`, `Invalid` or
/// `Untrusted`. `current` is the stored status the verdict block is built
/// over, so the basis read here is the one the write will store.
///
/// Only for a run that passed: an exit-2 run's signed failure verifies `Valid`
/// too, and the panel's cutover guidance must never sit on a restore whose
/// data did not reconcile. The status that will exist must be green under
/// [`crate::verification::restore_badge`], and `exit_code` must be `Some(0)`,
/// because the badge alone would judge an absent code on `outcome`.
#[must_use]
pub fn completion_patch_value(
    o: &ScorecardObservation,
    result: &crate::verification::VerificationResult,
    current: Option<&Value>,
    exit_code: Option<i32>,
) -> Option<Value> {
    if exit_code != Some(0) {
        return None;
    }
    let block = result.to_status_value(stored_verification(current));
    let mut projected = current.cloned().unwrap_or_else(|| json!({}));
    if !projected.is_object() {
        projected = json!({});
    }
    projected["exitCode"] = json!(0);
    projected["outcome"] = o.outcome.as_ref().map_or(Value::Null, |v| json!(v));
    projected["evidence"] = json!({ "verification": block });
    if !restore_badge(&projected).green {
        return None;
    }
    let completion = completion_block(o);
    (!completion.is_empty()).then_some(Value::Object(completion))
}

/// `patch` with `status.evidence.observation` set, when a controller read
/// scheduled a retry.
fn with_observation(mut patch: Value, observation: Option<Value>) -> Value {
    if let (Some(o), Some(evidence)) = (
        observation,
        patch
            .pointer_mut("/status/evidence")
            .and_then(Value::as_object_mut),
    ) {
        evidence.insert("observation".to_string(), o);
    }
    patch
}

/// `patch` with `status.completion` set, when there is one to set.
fn with_completion(mut patch: Value, completion: Option<Value>) -> Value {
    if let (Some(c), Some(status)) = (
        completion,
        patch.get_mut("status").and_then(Value::as_object_mut),
    ) {
        status.insert("completion".to_string(), c);
    }
    patch
}

/// The `/status` merge patch for the crashed-Job case — a Job that finished
/// with **no terminated state for `runner`**.
///
/// `exitCode` is absent, and its absence is the assertion: the status says
/// "this run has no exit code" rather than fabricating a `1` (a real
/// operational failure) or a `0` (a green badge for a run that never
/// reported).
///
/// The condition array goes through [`crate::verification::carry_verified`],
/// as [`finished_status_patch`]'s does: a merge patch replaces arrays, so a
/// builder that owns the array owes the parts that are not its own.
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
            // With no exit code the wire reason is always `operational`, so
            // `reason` carries the sub-case (`DisruptedMidDrill`,
            // `PodUnschedulable`, `NoExitCode`), verbatim the condition's own.
            "reason": terminal_state,
            "conditions": conditions,
        }
    })
}

/// When this object should be looked at again.
///
/// A named value rather than a `kube::runtime::Action`, which has no
/// `PartialEq` or readable `Debug` for a test to assert an interval off.
/// [`action_for`] is the one place it becomes an `Action`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Requeue {
    /// Nothing will change until something edits this object or its Job:
    /// `Action::await_change()`. Every terminal refusal is this; a requeue
    /// would spin forever over a CEL-immutable spec.
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
/// The one place [`Requeue`] becomes an `Action`, so the interval a test
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
    /// A terminal refusal this controller decided by itself, carrying the
    /// terminal state and the message its condition names. Not a requeue:
    /// `reconcile_restore` turns it into a status patch
    /// ([`refused_status_patch`]) and it never reaches `error_policy`.
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
/// Reads `phase`, not `exitCode`: the crashed-Job case and every
/// controller-side refusal write a terminal phase with no exit code, and
/// keying on the code would re-create the Job for exactly those runs.
#[must_use]
pub fn status_is_terminal(restore: &Restore) -> bool {
    matches!(
        restore.status.as_ref().and_then(|s| s.phase.as_deref()),
        Some(PHASE_SUCCEEDED | PHASE_FAILED)
    )
}

/// `GET` the `Approval` `spec.approvalRef` names, if it names one.
///
/// A 404 is `None`, not an error (interface **I19**): a `Restore` created
/// before its `Approval`, which [`admit`] turns into a 30-second hold. A
/// transport failure is an error and a requeue.
///
/// # Errors
///
/// [`RestoreError::Api`] for any API error that is not a 404.
async fn get_approval(
    restore: &Restore,
    client: &kube::Client,
    namespace: &str,
) -> Result<Option<Approval>, RestoreError> {
    // `spec.approvalRef` only: a `Restore` carrying `spec.authorization` is
    // refused by `admit` without its `Approval` being read.
    let referent = restore.spec.approval_ref_name().trim().to_string();
    if referent.is_empty() {
        return Ok(None);
    }
    let api: Api<Approval> = Api::namespaced(client.clone(), namespace);
    api.get_opt(&referent).await.map_err(RestoreError::Api)
}

/// `GET` the target `KafkaCluster`.
///
/// `None` for a 404, which [`admit`] reports as
/// [`RestoreAdmission::ClusterNotReachable`]: the control plane cannot see the
/// target.
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

/// Resolve this namespace's trust, for the approval bundle and the argv.
///
/// After the admission, deliberately: an unapproved plan reads nothing it
/// does not need. The two non-`Trust` resolutions (a contested namespace, no
/// trust material at all) are holds, not terminal refusals: they are
/// administrator faults an administrator can fix, and the `Restore` must
/// still be materializable afterwards without a new plan and approval.
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
/// A 409 is success only if the existing object is ours, immutable, has the
/// exact binding annotations, and carries byte-identical `spec.planBytes`.
/// Ownership alone is insufficient: a same-UID stale or substituted object
/// must never become the plan paired with a verified approval bundle.
///
/// This is a Kubernetes API write, not an archive write.
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
                    "adopting the exact mutable plan an earlier controller release left when it crashed; \
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
#[allow(clippy::too_many_arguments)]
async fn write_approval_bundle_config_map(
    restore: &Restore,
    plan: &RestoreSpec,
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
    let desired =
        approval_bundle_config_map_with_policy(restore, plan, approval, trust, policy, now)?;
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
                // The message names the members that differ.
                let differing = differing_bundle_members(&existing, &desired);
                let detail = if differing.is_empty() {
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
/// Diagnostic only: the decision is [`compatible_approval_bundle`]'s, and
/// this never widens what is accepted. Sorted, so the message is stable, and
/// it names keys, never bytes (a member can be a public key or a signed
/// envelope).
fn differing_bundle_members(existing: &ConfigMap, desired: &ConfigMap) -> Vec<String> {
    let empty = BTreeMap::new();
    let a = existing.data.as_ref().unwrap_or(&empty);
    let b = desired.data.as_ref().unwrap_or(&empty);
    let mut keys: Vec<String> = a.keys().chain(b.keys()).cloned().collect();
    keys.sort();
    keys.dedup();
    keys.into_iter().filter(|k| a.get(k) != b.get(k)).collect()
}

/// Find the pod the exit code is read from (seam **S6**).
///
/// The prefixed selector first, the legacy one as a fallback, through
/// [`super::backup::pod_selectors`] so the two reconcilers cannot disagree,
/// and then the one selection rule,
/// [`check::pod::find_owned_pod_by_selectors`]: a candidate is read only when
/// its controller `ownerReference` is a `Job` carrying this Job's
/// `metadata.uid`. `batch.kubernetes.io/job-name` is a plain label anyone who
/// can create a pod can set, so without the owner check a planted pod's exit
/// code and evidence keys would become this `Restore`'s outcome. Zero owned
/// pods is "no pod yet" and goes to the crashed-Job branch, which writes a
/// terminal `NoExitCode` rather than a borrowed success.
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
///    terminal status, and nothing is created: its Job's pods could never be
///    labelled, and a requeue would leave the CR with no status at all.
/// 1. **No Job and no terminal status** → `GET` the `Approval` and the target
///    `KafkaCluster`, run [`admit`], and only then:
///    * `Ok` → `GET` the roster, `POST` the plan ConfigMap, `POST` the Job,
///      patch `phase: Running`;
///    * a hold → patch `phase: Pending` with one `Admitted=False` condition
///      and requeue at [`ADMISSION_REQUEUE_SECS`] (interface **I19**);
///    * terminal → patch the refusal and await a change.
///
///    Zero `POST`s on either refusal.
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
/// process was handed.
///
/// `runner.image` is `None` for the shipped pin `job::RUNNER_IMAGE` and
/// `Some(reference)` for the value `main` read out of `job::RUNNER_IMAGE_ENV`;
/// `runner.image_pull_policy` is `None` for the compiled-in
/// `job::IMAGE_PULL_POLICY` and `Some(policy)` for
/// `job::RUNNER_PULL_POLICY_ENV`'s. The pair becomes
/// `job::RunnerJobSpec::{image, image_pull_policy}` and changes nothing else
/// about the Job.
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
/// policies.
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
    reconcile_restore_at(
        restore, client, scorecard, verify, now, runner, policies, None,
    )
    .await
}

/// [`reconcile_restore_with_policy`], with the location (`LOGWEIR_ARCHIVE_URL`)
/// the controller's one global archive handle was built from. `Store` exposes
/// no accessor for it, so the one read lives in [`reconcile`] and a test
/// states the location instead of setting process-global state.
///
/// `None` is "not stated"
/// ([`crate::destination::LegacyEvidenceScope::HandleLocationUnstated`]);
/// `Some` confines an inline-archive run's evidence read to the handle's own
/// bucket.
///
/// # Errors
///
/// [`RestoreError`] for anything that is not an outcome.
#[allow(clippy::too_many_arguments)]
pub async fn reconcile_restore_at(
    restore: &Restore,
    client: &kube::Client,
    scorecard: ScorecardOracle<'_>,
    verify: VerifyOracle<'_>,
    now: DateTime<Utc>,
    runner: &job::RunnerImage,
    policies: &ApprovalPolicySet,
    archive_url: Option<&str>,
) -> Result<RestoreOutcome, RestoreError> {
    reconcile_restore_run(
        restore,
        client,
        scorecard,
        verify,
        now,
        runner,
        policies,
        archive_url,
        None,
        None,
    )
    .await
}

/// [`reconcile_restore_at`], with this process's memory of which finished runs
/// it has read (the `Backup` twin's `reconcile_backup_rereading`). The running
/// controller passes [`crate::evidence_fetch::ReadMemory::global`]; a row
/// passes a fresh one.
///
/// # Errors
///
/// [`RestoreError`] for anything that is not an outcome.
#[allow(clippy::too_many_arguments)]
pub async fn reconcile_restore_rereading(
    restore: &Restore,
    client: &kube::Client,
    scorecard: ScorecardOracle<'_>,
    verify: VerifyOracle<'_>,
    now: DateTime<Utc>,
    runner: &job::RunnerImage,
    policies: &ApprovalPolicySet,
    archive_url: Option<&str>,
    memory: &crate::evidence_fetch::ReadMemory,
) -> Result<RestoreOutcome, RestoreError> {
    reconcile_restore_run(
        restore,
        client,
        scorecard,
        verify,
        now,
        runner,
        policies,
        archive_url,
        None,
        Some(memory),
    )
    .await
}

/// [`reconcile_restore_at`], through the manual-restore pool.
///
/// The entry point the running controller uses
/// ([`reconcile_in_context_pooled`]). An admitted manual `Restore` that does
/// not already hold a slot passes `manual_restore_gate` before its trust read,
/// plan, bundle and Job; a full pool leaves it `phase: Queued` with nothing
/// created. [`reconcile_restore`] and its siblings do not consult the pool,
/// for the reason `controllers::backup::reconcile_backup_pooled` gives.
///
/// # Errors
///
/// [`RestoreError`] for anything that is not an outcome.
#[allow(clippy::too_many_arguments)]
pub async fn reconcile_restore_pooled(
    restore: &Restore,
    client: &kube::Client,
    scorecard: ScorecardOracle<'_>,
    verify: VerifyOracle<'_>,
    now: DateTime<Utc>,
    runner: &job::RunnerImage,
    policies: &ApprovalPolicySet,
    archive_url: Option<&str>,
    pool: &crate::run_pool::Pool<'_, Restore>,
) -> Result<RestoreOutcome, RestoreError> {
    reconcile_restore_run(
        restore,
        client,
        scorecard,
        verify,
        now,
        runner,
        policies,
        archive_url,
        Some(pool),
        None,
    )
    .await
}

/// The entry points' one body.
#[allow(clippy::too_many_arguments)]
async fn reconcile_restore_run(
    restore: &Restore,
    client: &kube::Client,
    scorecard: ScorecardOracle<'_>,
    verify: VerifyOracle<'_>,
    now: DateTime<Utc>,
    runner: &job::RunnerImage,
    policies: &ApprovalPolicySet,
    archive_url: Option<&str>,
    pool: Option<&crate::run_pool::Pool<'_, Restore>>,
    memory: Option<&crate::evidence_fetch::ReadMemory>,
) -> Result<RestoreOutcome, RestoreError> {
    // Where the object stands when a hold or refusal is written: the pool's
    // admission record is this pass's first write, and seam S7 preconditions
    // every later write on where it left the object.
    let mut written: Option<Restore> = None;
    // PROD-11.1a: a TTL clone's admission, lifetime and cleanup wrap the
    // reconcile (`restore_clone::around`); every other `Restore` is the
    // reconcile, unchanged.
    let result = restore_clone::around(
        restore,
        client,
        now,
        runner.image.as_deref(),
        // Boxed: the reconcile's future is large, and holding it by value in
        // the wrapper's would double what one poll puts on the stack.
        Box::pin(reconcile_restore_inner(
            restore,
            client,
            scorecard,
            verify,
            now,
            runner,
            policies,
            archive_url,
            pool,
            memory,
            &mut written,
        )),
    )
    .await;
    let restore = written.as_ref().unwrap_or(restore);
    match result {
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
            // The one place a self-decided refusal is written: every refusal
            // inside the reconcile is a `?` on `RestoreError::Refused`.
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
#[allow(clippy::too_many_arguments)]
async fn reconcile_restore_inner(
    restore: &Restore,
    client: &kube::Client,
    scorecard: ScorecardOracle<'_>,
    verify: VerifyOracle<'_>,
    now: DateTime<Utc>,
    runner: &job::RunnerImage,
    policies: &ApprovalPolicySet,
    archive_url: Option<&str>,
    pool: Option<&crate::run_pool::Pool<'_, Restore>>,
    memory: Option<&crate::evidence_fetch::ReadMemory>,
    written: &mut Option<Restore>,
) -> Result<RestoreOutcome, RestoreError> {
    // What a terminal pass reads a run's scorecard again with.
    let reads = Reread {
        scorecard,
        verify,
        archive_url,
        memory,
    };
    let name = restore.name_any();
    let namespace = restore
        .namespace()
        .ok_or_else(|| RestoreError::NoNamespace(name.clone()))?;
    // What this namespace is bound to now, resolved once per pass for
    // admission, the bundle, the argv and the environment alike.
    let effective_policy = policies.resolve(&namespace);
    // The Job is named after the CR, verbatim.
    let job_name = name.clone();
    let jobs: Api<Job> = Api::namespaced(client.clone(), &namespace);
    let restores: Api<Restore> = Api::namespaced(client.clone(), &namespace);

    // PROD-07.2: an `Interrupted` restore's reconcile and cleanup, and a
    // cancel.
    if let Some(outcome) =
        restore_interruption::before(restore, client, &namespace, now, runner, &reads).await?
    {
        return Ok(outcome);
    }

    // STEP 0. The object's own name, before any `POST`.
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
            // ...except an evidence fetch it may still owe.
            let requeue =
                continue_evidence_fetch(restore, client, &namespace, now, runner, &reads).await?;
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

        // The runner's resources, before the admission reads anything: a
        // `spec.runnerResources` this controller will never apply is a fact
        // about the sealed spec, so it is refused before an approval is waited
        // for, a pool slot taken or anything created; otherwise an approver
        // could sign a run that could never start. Only a Restore with no Job
        // reaches this line, so a run already started is never refused
        // mid-flight.
        runner_resources_of(restore)?;
        // The plan, read once for everything below.
        let plan = parse_plan(restore)?;
        // The declared coverage against the plan's own, for the same reason:
        // a fact about the sealed spec.
        coverage_agrees(restore, &plan)?;
        // The original-name declaration against the plan, before an approver
        // is shown the wrong subject.
        original_name_agrees(restore, &plan)?;

        // The admission, before the first `POST`: two `GET`s and a pure
        // function. An unapproved plan creates nothing.
        let approval = get_approval(restore, client, &namespace).await?;
        let cluster = get_target_cluster(restore, client, &namespace).await?;
        let admission = admit_with_policy(
            restore,
            Some(&plan),
            approval.as_ref(),
            cluster.as_ref(),
            Some(&PolicyAdmission {
                policy: &effective_policy,
                now,
            }),
        );
        match &admission {
            RestoreAdmission::Ok => {}
            // An approval that expired while this Restore waited in the
            // manual-restore pool says so: the queue does not extend an
            // approval's maximum age, and the sentence adds how long the line
            // was.
            a @ RestoreAdmission::AuthorizationExpired { .. }
                if restore.status.as_ref().is_some_and(|s| {
                    crate::run_pool::was_queued(
                        s.phase.as_deref(),
                        s.queue.is_some(),
                        current_condition(s.conditions.as_ref(), CONDITION_ADMITTED)
                            .and_then(|c| c.reason.as_deref()),
                    )
                }) =>
            {
                let behind = pool.map_or(0, |p| {
                    (p.peers)().map_or(0, |peers| {
                        p.reservations.behind(
                            crate::run_pool::PoolKind::Restore,
                            restore,
                            &peers,
                            crate::run_pool::restore_standing,
                        )
                    })
                });
                let limit = restore
                    .status
                    .as_ref()
                    .and_then(|s| s.queue.as_ref())
                    .map(|q| q.limit);
                return Err(RestoreError::Refused(
                    a.reason(),
                    expired_while_queued_message(&a.to_string(), behind, limit),
                ));
            }
            a if a.is_terminal() => {
                return Err(RestoreError::Refused(a.reason(), a.to_string()));
            }
            a => {
                // A hold (interface I19): `phase: Pending`, one condition, and
                // a 30-second requeue so the object is released the moment
                // the `Approval` is verified.
                //
                // `ReferentNotFound` on the Approval's `Verified` condition is
                // the one unverified state that is a race (the `Approval` was
                // reconciled before this `Restore` existed), not a signature
                // problem. The verdict is the same hold; the log line says
                // which it is, so an operator does not go looking at a key.
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

        // Admitted, so the cluster is `Some`; the branch names the
        // impossibility instead of using `expect`.
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
        // The destinations, after checks 0-4 and before the first POST, so an
        // unapproved plan is still `ApprovalNotVerified` rather than a
        // destination complaint. The hold is the approval's 30-second hold,
        // uncapped: a `Restore` waits for a human either way.
        let destinations = match admit_restore_destinations(
            restore,
            Some(&plan),
            client,
            &namespace,
            now,
        )
        .await?
        {
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

        // The manual-restore pool, the last check before anything is created:
        // after the admission, so a restore that is not approved or whose
        // destination is missing holds under that reason and never takes a
        // place in line; before the trust read, the plan `ConfigMap`, the
        // approval bundle and the Job.
        let released: Box<Restore>;
        let restore: &Restore = match pool {
            Some(pool) if crate::run_pool::restore_gated(restore) => {
                match manual_restore_gate(restore, &approval, client, &namespace, pool, now).await?
                {
                    RestoreGate::Admitted(object) => {
                        released = object;
                        // A hold or refusal from here on is written over the
                        // admitted object.
                        *written = Some((*released).clone());
                        &released
                    }
                    RestoreGate::Wait => {
                        return Ok(RestoreOutcome {
                            job_name,
                            created: false,
                            admission: None,
                            exit_code: None,
                            terminal_state: None,
                            keys: RestoreEvidenceKeys::default(),
                            ttl_patched: false,
                            requeue: Requeue::After(REQUEUE_SECS),
                        })
                    }
                }
            }
            _ => restore,
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
            &plan,
            &cluster,
            &key_ids,
            &approval,
            &trust,
            &effective_policy,
            now,
            destinations.as_deref(),
        )?;
        // The image and pull-policy overrides. `None` leaves the compiled-in
        // constant.
        spec.image = runner.image.clone();
        spec.image_pull_policy = runner.image_pull_policy.clone();

        // The plan ConfigMap, in this same pass and before the Job `POST`: the
        // Job mounts `<name>-plan`, so a Job created first stalls in
        // `ContainerCreating` until its deadline and ends as `NoExitCode`.
        write_plan_config_map(restore, client, &namespace, destinations.as_deref()).await?;
        write_approval_bundle_config_map(
            restore,
            &plan,
            &approval,
            &trust,
            &effective_policy,
            now,
            client,
            &namespace,
        )
        .await?;

        let mut desired_job = job::build(&spec);
        // PROD-07.2: the run's id, named here so its targets record can be
        // found after its pod is gone, and a grace for its stop.
        restore_interruption::prepare_run_job(
            &mut desired_job,
            restore_interruption::new_run_id(now).as_deref(),
        );
        // The Job's line token, made now and kept nowhere but in the Job; a
        // re-created Job gets another. Never logged (see
        // `crate::job::new_line_token`).
        if !job::add_fresh_line_token(&mut desired_job) {
            warn!(
                restore = %name,
                namespace = %namespace,
                job = %job_name,
                "the operating system's random source gave no line token; the Job is created \
                 without one, and if its runner refuses the plan the Restore will not say why"
            );
        }
        let created = match jobs.create(&PostParams::default(), &desired_job).await {
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

    // STEP 2. Running. (A `Restore` an interruption ended may keep a
    // suspended run Job: it is terminal, below.)
    if !backup::job_finished(&job) && !status_is_terminal(restore) {
        // The same derivation the `Backup` twin calls, so a runner that cannot
        // mount its approval bundle is visible before its deadline.
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
                    // `Restore.status.reason` is the `RunnerReady` reason while
                    // it is false, and the base builder's `JobCreated`
                    // otherwise.
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

    // STEP 2b. Already terminal: read nothing and write nothing. Re-deriving
    // here turned a verified `Succeeded` restore into `Failed/NoExitCode`
    // after a controller restart re-listed a finished Job whose pod was gone.
    // The trade (see the `Backup` twin): a TTL or verification patch that
    // failed on the terminal pass is not retried, which leaves no false
    // statement on the object.
    if status_is_terminal(restore) {
        debug!(
            restore = %name,
            namespace = %namespace,
            job = %job_name,
            "the status is already terminal; the runner's pod is not read again and no patch is \
             sent"
        );
        // A terminal `Restore` requeues `AwaitChange`, so this is the one pass
        // that will look at the Job again; a TTL patch that failed on the
        // terminal pass is repaired here or never.
        let ttl_patched =
            diagnostics::repair_ttl(client, &namespace, &job, &restore.uid().unwrap_or_default())
                .await
                .map_err(RestoreError::Api)?;
        // The other thing a terminal `Restore` may still owe: its evidence
        // verdict, when an evidence-fetch Job reads it. The run's own pod is
        // still not read.
        let requeue =
            continue_evidence_fetch(restore, client, &namespace, now, runner, &reads).await?;
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
    // `pods/log` call below must hold a `&Pod` and not an `Option`.
    let terminated = found
        .pod
        .as_ref()
        .and_then(|p| backup::terminated_exit_code(p).map(|code| (p, code)));

    // STEP 4. The crashed-Job case, before the happy path, because the happy
    // path needs a code and this branch is "there is none".
    let Some((pod, exit_code)) = terminated else {
        let terminal_state = if found.contested.is_empty() {
            // The `Backup` twin's rule: the four diagnostic states replace
            // `NoExitCode` only when a matching diagnostic was recorded before
            // the Job ended.
            let from_pod = backup::crash_terminal_state(found.pod.as_ref());
            if from_pod == crate::conditions::TERMINAL_STATE_NO_EXIT_CODE {
                diagnostics::recorded_terminal_state(
                    restore.status.as_ref().and_then(|s| s.progress.as_ref()),
                    diagnostics::job_ended_at(&job).unwrap_or(now),
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
        let crashed = diagnostics::apply_finished(
            crashed_status_patch(restore, terminal_state, &job_name, now),
            restore.status.as_ref().and_then(|s| s.progress.as_ref()),
            now,
        );
        // PROD-07.2: a run that may have started is Interrupted, and the
        // reconcile decides whether it created anything.
        if let Some(patch) =
            restore_interruption::after_crash(restore, &job, terminal_state, crashed.clone(), now)
        {
            patch_status_if_changed(&restores, restore, &name, patch).await?;
            return Ok(RestoreOutcome {
                job_name,
                created: false,
                admission: None,
                exit_code: None,
                terminal_state: Some(restore_interruption::PHASE_INTERRUPTED.to_string()),
                keys: RestoreEvidenceKeys::default(),
                ttl_patched: false,
                requeue: Requeue::After(1),
            });
        }
        patch_status_if_changed(&restores, restore, &name, crashed).await?;
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

    finish_job(
        restore,
        client,
        &namespace,
        &job,
        pod,
        exit_code,
        &reads,
        now,
        runner,
        Finish::Run,
    )
    .await
}

/// Which Job's end [`finish_job`] reads (PROD-07.2).
enum Finish<'a> {
    /// The run Job's: an exit that signed nothing after the run created its
    /// targets is `Interrupted` (`restore_interruption::after_exit`).
    Run,
    /// An interrupted run's reconcile Job's
    /// (`restore_interruption::ReconcileEnd::adjust`).
    Reconcile(&'a restore_interruption::ReconcileEnd),
}

/// **STEP 3: a Job whose runner's exit code is known** — the run Job, or a
/// reconcile Job (PROD-07.2). Read the log, the evidence keys and the
/// scorecard; write the terminal status, the Job's TTL and the verdict.
#[allow(clippy::too_many_arguments)]
async fn finish_job(
    restore: &Restore,
    client: &kube::Client,
    namespace: &str,
    job: &Job,
    pod: &Pod,
    exit_code: i32,
    reads: &Reread<'_>,
    now: DateTime<Utc>,
    runner: &job::RunnerImage,
    finish: Finish<'_>,
) -> Result<RestoreOutcome, RestoreError> {
    let Reread {
        scorecard,
        verify,
        archive_url,
        memory,
    } = *reads;
    let name = restore.name_any();
    let job_name = job.name_any();
    let jobs: Api<Job> = Api::namespaced(client.clone(), namespace);
    let restores: Api<Restore> = Api::namespaced(client.clone(), namespace);
    // STEP 3. The code is known. Read the log through the `pods/log`
    // subresource, the only route to a runner's stdout (interface I28), with
    // THE terminal bound (`runner_log::TERMINAL`) at every exit code.
    let pod_name = pod.name_any();
    let pods: Api<Pod> = Api::namespaced(client.clone(), &namespace);
    // The Job's own line token, and by which flag (FX-43): whose lines are
    // read at all.
    let job_token = job::job_token(&job);
    // A refused run's read failure is an answer (see the `Backup` twin). This
    // `if` is the only gate: every other exit code's failure is still a
    // reconcile error.
    let (raw, mut runner_reason) = if exit_code == 3 {
        let read = crate::refusal::read(
            &pods,
            &namespace,
            &pod_name,
            logweir_core::refusal_detail::RefusingRun::Restore,
            &job_token,
        )
        .await;
        (read.body, Some(read.reason))
    } else {
        match runner_log::read_terminal(&pods, &pod_name)
            .await
            .map_err(RestoreError::Api)?
        {
            runner_log::TerminalRead::Read(body) => (body, None),
            // Cut before its end: nothing is taken from it, and nothing is
            // said about the runner either.
            runner_log::TerminalRead::OverBound => (String::new(), None),
        }
    };
    // THE one answer to "whose line is it": only the lines the runner sealed
    // with this Job's token (a Job an older controller built is read as that
    // controller read it). Every reader below scans `log`.
    let lines = runner_log::own_lines(&job_token, &raw);
    if runner_reason.is_none()
        && !raw.is_empty()
        && runner_log::vouches_for_exit(exit_code)
        && lines.runner_is_older()
    {
        runner_reason = Some(crate::refusal::RunnerReason::RunnerOlderThanController);
    }
    let older = runner_reason == Some(crate::refusal::RunnerReason::RunnerOlderThanController);
    // DUI-5: why an exit-1 run failed, from its sealed `failure-detail=`.
    if runner_reason.is_none() {
        runner_reason = crate::refusal::failure_detail(exit_code, &lines);
    }
    // poc-dui3 O2: no sealed end and no sign of an older runner is said as
    // observed, never as an older image.
    if runner_reason.is_none()
        && !raw.is_empty()
        && runner_log::vouches_for_exit(exit_code)
        && lines.no_sealed_end()
    {
        runner_reason = Some(crate::refusal::RunnerReason::NoSealedEnd(exit_code));
    }
    let log = lines.text();
    let keys = restore_evidence_keys(log);
    // An exit-1 run whose last line names a state on the Restore's closed
    // `failure-reason=` list (a stopped creation step) says so on
    // `status.exitReason`; every other exit 1 keeps the wire reason.
    let creation_stop = creation_stop_state(exit_code, log);
    let refusal = if older {
        // The mixed pair, by name, at whatever code the older runner stopped
        // with: never a generic `operational` a schedule retries.
        Some(logweir_core::runner_line::RUNNER_OLDER_THAN_CONTROLLER.to_string())
    } else if exit_code == 3 {
        Some(
            backup::refusal_state(log)
                .unwrap_or_else(|| TERMINAL_STATE_GUARD_REFUSED_UNKNOWN_REASON.to_string()),
        )
    } else {
        creation_stop.map(str::to_string)
    };
    // The scorecard, through a read-only handle, awaited: the real oracle's
    // `Store` read runs inside `spawn_blocking` (interface I13).
    //
    // A legacy object takes the oracle it always has; a destination-backed one
    // takes the evidence destination's own read-only handle, never the
    // controller's global one (a different principal over a different bucket,
    // whose answer would read as "no scorecard" rather than "wrong bucket"),
    // and never the source destination's, which is the two-destination
    // version of the same mistake.
    let routed = backup::evidence_source_for(
        restore.spec.evidence_destination_ref.as_ref(),
        client,
        &namespace,
        now,
    )
    .await
    .map_err(RestoreError::Api)?;
    let was_global = matches!(routed, backup::EvidenceSource::GlobalHandle);
    // The plan, read once for this pass; never refused here, because a run
    // that has a Job is observed.
    let plan = parse_plan(restore).ok();
    let evidence_from = legacy_evidence_source(routed, plan.as_ref(), archive_url);
    if was_global && !matches!(evidence_from, backup::EvidenceSource::GlobalHandle) {
        // The handle's location is logged here and nowhere a tenant reads; the
        // status detail names it by role (`destination::ARCHIVE_HANDLE_LABEL`).
        info!(
            restore = %name,
            namespace = %namespace,
            archive_handle = archive_url.unwrap_or_default(),
            "this inline-archive run's plan writes its evidence outside the archive handle's \
             bucket; the scorecard is not read and the verdict is NotAttempted"
        );
    }
    let observed = match (keys.scorecard.as_ref(), &evidence_from) {
        (None, _) => None,
        (Some(key), backup::EvidenceSource::GlobalHandle) => scorecard(key.clone()).await,
        (Some(key), backup::EvidenceSource::Destination(store)) => {
            // The same `observe_scorecard`, on a different handle: one reader
            // of a scorecard, one answer to "what does this document say".
            let handle = Arc::clone(store);
            let key = key.clone();
            tokio::task::spawn_blocking(move || observe_scorecard(&handle, &key))
                .await
                .ok()
                .flatten()
        }
        // Nothing was read and nothing is guessed: no fact is copied out of a
        // document nobody fetched. For `FetchJob` the Job's relay supplies
        // them on a later write.
        (
            Some(_),
            backup::EvidenceSource::NotAttempted { .. } | backup::EvidenceSource::FetchJob { .. },
        ) => None,
    };
    // A scorecard over the read cap is not observed for every fact below, and
    // its sentence is the verdict.
    let (observed, refused) = split_refused(observed);
    let topics = plan.as_ref().map(topic_mapping);
    // The stopped creation step's lists, held to this Restore's mapped target
    // names: a name the plan does not map is not this run's to speak of.
    let race = creation_stop.and_then(|_| {
        creation_stop_lists(
            exit_code,
            log,
            topics.as_ref().map_or(&[][..], |(_, new)| new.as_slice()),
        )
    });
    // Guard **G-TS**: scanned by name out of the same bounded tail as the
    // evidence keys.
    let preflight = topic_preflight(log);

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

    // Built once and held, because the second patch needs the condition array
    // this one carries: a merge patch replaces arrays, and after this PATCH
    // the in-memory `restore` is stale. See `verification::second_patch`.
    //
    // The patch carries both the runner's own reason for an exit 3 (appended
    // by the builder) and a stopped creation step's lists (added by the
    // `match` below); skipping either drops one of them.
    let finished = finished_status_patch_with_reason(
        restore,
        exit_code,
        &keys,
        refusal.as_deref(),
        runner_reason.as_ref(),
        observed.as_ref(),
        topics.as_ref(),
        preflight.as_ref(),
        now,
    );
    let mut finished = match (creation_stop, race.as_ref()) {
        (_, Some(lists)) => with_target_topics_appeared(finished, lists),
        // The state is named and no list can be shown: say so, and do not call
        // the plan's names "created".
        (Some(_), None) => with_unread_creation_stop(finished),
        (None, None) => finished,
    };
    // What the run's teardown removed and what it left, on any exit code.
    if let Some(teardown) = teardown_status(
        log,
        lines.sealed_only(),
        topics.as_ref().map_or(&[][..], |(_, new)| new.as_slice()),
    ) {
        if let Some(status) = finished.get_mut("status").and_then(Value::as_object_mut) {
            status.insert("teardown".to_string(), teardown);
        }
    }
    // PROD-05.2: what the configuration transition did, by the same rule.
    if let Some(configured) = topic_configuration(
        log,
        topics.as_ref().map_or(&[][..], |(_, new)| new.as_slice()),
    ) {
        if let Some(status) = finished.get_mut("status").and_then(Value::as_object_mut) {
            status.insert("topicConfiguration".to_string(), configured);
        }
    }
    let terminal = diagnostics::apply_finished(
        finished,
        restore.status.as_ref().and_then(|s| s.progress.as_ref()),
        now,
    );
    // PROD-07.2: a run that signed nothing after it created its targets is
    // `Interrupted` (its Job keeps no TTL: the reconcile copies it); a
    // reconcile's end is written as the reconcile's.
    let terminal = match finish {
        Finish::Run => {
            if let Some(patch) = restore_interruption::after_exit(
                restore,
                job,
                exit_code,
                log,
                terminal.clone(),
                now,
            ) {
                warn!(
                    restore = %name,
                    namespace = %namespace,
                    job = %job_name,
                    exit_code,
                    "the run ended without a signed scorecard after it created its target \
                     topics; the Restore is Interrupted and a reconcile Job verifies what it left"
                );
                patch_status_if_changed(&restores, restore, &name, patch).await?;
                return Ok(RestoreOutcome {
                    job_name,
                    created: false,
                    admission: None,
                    exit_code: Some(exit_code),
                    terminal_state: Some(restore_interruption::PHASE_INTERRUPTED.to_string()),
                    keys,
                    ttl_patched: false,
                    requeue: Requeue::After(1),
                });
            }
            terminal
        }
        Finish::Reconcile(end) => end.adjust(restore, terminal, &keys),
    };
    // Where the object now stands. The verification patch below is this
    // pass's second write; preconditioned on the version the watch delivered
    // it would be refused, and a terminal `Restore` is never reconciled again.
    let at = patch_status_if_changed(&restores, restore, &name, terminal.clone()).await?;

    // Only now: the `?` above means a status patch that did not return 200
    // leaves before any TTL exists, so pod GC cannot start on a run whose code
    // was never recorded. Chart value `controller.jobTtlSeconds`; the default
    // is `TTL_SECONDS_AFTER_FINISHED`.
    runner_log::set_ttl(&jobs, &job_name, diagnostics::job_ttl_seconds(), None)
        .await
        .map_err(RestoreError::Api)?;

    // The second patch (interface I21's `Restore` half), after the terminal
    // status write and the TTL, so a failed verification, or a 500 on this
    // PATCH, can never prevent the exit code from being recorded.
    //
    // Attempted only when an artifact was written, which the two mandatory
    // keys say. At exits 1, 3 and 4 no artifact exists, and no verification
    // block is written at all: that absence is deliberate.
    //
    // The digest is required only by the paths that verify bytes. A
    // `NotAttempted` source fetched nothing, so `scorecard_sha256` is `None` by
    // construction; it still records the honest `NotAttempted` and the
    // sentence naming why. Nothing is invented: `Valid`, `Invalid` and
    // `Untrusted` come only from a verifier that read the document.
    let reference = match (
        keys.scorecard.as_deref(),
        keys.sidecar.as_deref(),
        observed
            .as_ref()
            .and_then(|o| o.scorecard_sha256.as_deref()),
    ) {
        (Some(payload_key), Some(sidecar_key), Some(digest)) => Some(EvidenceRef {
            // Trust is resolved per namespace, from `metadata.namespace` of
            // the object being reconciled, never a name the subject supplied.
            namespace: namespace.to_string(),
            payload_key: payload_key.to_string(),
            payload_sha256: digest.to_string(),
            sidecar_key: sidecar_key.to_string(),
            payload_type: logweir_verify::PAYLOAD_TYPE_SCORECARD,
        }),
        _ => None,
    };
    // The choice is pure and tabled (`terminal_evidence`): which verdict is
    // recorded without a read, which reference is verified, and which pass
    // writes nothing.
    let verdict = match (
        refused,
        terminal_evidence(
            &evidence_from,
            reference,
            keys.mandatory_complete(),
            keys.scorecard.as_deref().unwrap_or_default(),
            restore
                .spec
                .evidence_destination_ref
                .as_ref()
                .map(|r| r.name.as_str()),
        ),
    ) {
        // The table recorded "read no such document" for a read that produced
        // no digest; when the cap is why, the sentence naming the cap is
        // recorded instead, final, so the object is not read again.
        (Some(detail), TerminalEvidence::Record(_)) => {
            Some(crate::verification::VerificationResult::not_attempted(
                logweir_verify::PAYLOAD_TYPE_SCORECARD,
                detail,
            ))
        }
        (_, TerminalEvidence::Record(result)) => Some(*result),
        (_, TerminalEvidence::Verify(reference)) => {
            Some(read_verdict(&evidence_from, reference, verify, client, now).await)
        }
        (_, TerminalEvidence::Nothing) => None,
    };
    // The controller's own read is attempt 1 of a schedule (the `Backup`
    // twin's rule). A transient failure records
    // `observation.{mode, attempt: 1, retryAfter}`; the terminal branches read
    // it again (`continue_evidence_fetch`), and a terminal `Restore` waiting
    // for that attempt requeues for it.
    let mut read_observation: Option<Value> = None;
    let verdict = match (verdict, backup::controller_read_mode(&evidence_from)) {
        (Some(result), Some(mode)) => {
            let (result, retry) = crate::evidence_fetch::scheduled(result, 1, now);
            if retry.is_some() {
                read_observation = Some(crate::evidence_fetch::observation_patch(
                    Some(mode),
                    None,
                    1,
                    None,
                    retry,
                ));
            }
            Some(result)
        }
        (verdict, _) => verdict,
    };
    let read_retry = read_observation.is_some();
    if let Some(result) = verdict {
        let current = restore
            .status
            .as_ref()
            .and_then(|s| serde_json::to_value(s).ok());
        let block = result.to_status_value(stored_verification(current.as_ref()));
        // The badge is computed over the status that will exist: the terminal
        // patch has landed, so `outcome` is the value copied from the signed
        // scorecard. The `Restore` rule reads `outcome`; the `Backup` rule
        // reads `exitCode`.
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
            // The verb every arm earns: on the `NotAttempted` arm nothing was
            // fetched or checked. `verification` carries which verdict it was.
            "weirkeeper recorded this Restore's evidence verdict"
        );
        patch_status_at(
            &restores,
            restore,
            &name,
            &at,
            // Explicit nulls for the fields this verdict does not hold (see
            // `verification::verification_patch_value`).
            with_observation(
                with_completion(
                    second_patch(
                        &conditions_in(&terminal),
                        verified,
                        crate::verification::verification_patch_value(block),
                    ),
                    observed.as_ref().and_then(|o| {
                        completion_patch_value(o, &result, current.as_ref(), Some(exit_code))
                    }),
                ),
                read_observation,
            ),
        )
        .await?;
        // This process has read it (`evidence_fetch::ReadMemory`).
        if backup::controller_read_mode(&evidence_from).is_some() {
            if let (Some(memory), Some(uid)) = (memory, restore.uid()) {
                memory.mark(&uid);
            }
        }
    }

    // The default arm: an evidence-fetch Job reads the scorecard with the
    // evidence destination's `evidenceRead` grant, after the terminal patch
    // and the runner Job's TTL. A terminal `Restore` otherwise waits for a
    // change, so while the fetch is owed, or a controller read's retry is
    // scheduled, it is looked at again on a timer.
    let mut requeue = if read_retry {
        Requeue::After(crate::evidence_fetch::RETRY_DELAYS_SECONDS[0].unsigned_abs())
    } else {
        Requeue::AwaitChange
    };
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
/// A constant, because this code has only the string the `Approval`'s
/// condition carries. `the_referent_not_found_reason_is_the_approval_modules_own`
/// asserts the two agree.
pub const REFERENT_NOT_FOUND_REASON: &str = "ReferentNotFound";

/// The `kube::runtime` reconcile entry point.
///
/// The one clock read in this file is here.
///
/// [`super::Context::archive`] is the controller's one read-only
/// `Arc<Store>`, built once in `main` (interface **I13**); the closure is an
/// `Arc` clone per reconcile, because a `Store` constructor drives its own
/// runtime and would panic here. `None` (no `LOGWEIR_ARCHIVE_URL`) takes
/// [`unobserved_scorecard`]'s answer: every scorecard field is omitted from
/// the status rather than written as nothing.
async fn reconcile(
    restore: Arc<Restore>,
    ctx: Arc<Context>,
    approval_policies: crate::approval_policy::PolicySource,
    objects: reflector::Store<Restore>,
) -> Result<Action, RestoreError> {
    // The document with the fresh-install marker as last read.
    let approval_policies = approval_policies.effective();
    // The manual-restore pool counts from the watch this controller already
    // runs (`run_pool::store_snapshot`: `None` until it has synced), with the
    // process's one reservation registry.
    let peers = move || crate::run_pool::store_snapshot(&objects);
    let pool = crate::run_pool::Pool {
        peers: &peers,
        limit: None,
        reservations: crate::run_pool::Reservations::global(),
    };
    // The process's one memory of which finished runs it has read (see the
    // `Backup` reconciler's `reconcile`).
    let outcome = reconcile_in_context_pooled(
        &restore,
        &ctx,
        &approval_policies,
        Utc::now(),
        &|name: &str| std::env::var(name),
        Some(&pool),
        Some(crate::evidence_fetch::ReadMemory::global()),
    )
    .await?;
    Ok(action_for(&outcome))
}

/// How the shipped reconcile reads its process environment: a parameter so a
/// test can state it, since the handle location's wiring is otherwise
/// reachable from no row.
pub type EnvReader<'a> = &'a (dyn Fn(&str) -> Result<String, std::env::VarError> + Send + Sync);

/// [`reconcile`]'s body, with the archive oracles and their location read
/// from `env`. Errors as [`reconcile_restore_at`].
pub async fn reconcile_in_context(
    restore: &Restore,
    ctx: &Context,
    approval_policies: &ApprovalPolicySet,
    now: DateTime<Utc>,
    env: EnvReader<'_>,
) -> Result<RestoreOutcome, RestoreError> {
    reconcile_in_context_pooled(restore, ctx, approval_policies, now, env, None, None).await
}

/// [`reconcile_in_context`], through the manual-restore pool when one is given:
/// what [`reconcile`] runs.
///
/// # Errors
///
/// As [`reconcile_restore_at`].
pub async fn reconcile_in_context_pooled(
    restore: &Restore,
    ctx: &Context,
    approval_policies: &ApprovalPolicySet,
    now: DateTime<Utc>,
    env: EnvReader<'_>,
    pool: Option<&crate::run_pool::Pool<'_, Restore>>,
    memory: Option<&crate::evidence_fetch::ReadMemory>,
) -> Result<RestoreOutcome, RestoreError> {
    let archive = ctx.archive.clone();
    let oracle = move |key: String| -> BoxFuture<'static, Option<ScorecardObservation>> {
        let handle = archive.clone();
        Box::pin(async move {
            let handle = handle?;
            // One `spawn_blocking`, one `get` (interface I13).
            tokio::task::spawn_blocking(move || observe_scorecard(&handle, &key))
                .await
                .ok()
                .flatten()
        })
    };
    let verify = crate::verification::verify_oracle(ctx.archive.clone(), ctx.client.clone(), now);
    // The handle's own location, read beside the handle: `Store` exposes no
    // accessor for the URL it was built from.
    let archive_url =
        crate::retention::configured_archive_url(env(crate::retention::ARCHIVE_URL_ENV));
    reconcile_restore_run(
        restore,
        &ctx.client,
        &oracle,
        &verify,
        now,
        &ctx.runner_image,
        approval_policies,
        archive_url.as_deref(),
        pool,
        memory,
    )
    .await
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

/// Every Restore a `TrustPolicy` event could change the verdict of.
///
/// No LIST: the controller's own watch store over every Restore is the same
/// index, already warm and no staler than the event, so the trigger costs no
/// API calls.
///
/// It over-approximates on purpose: [`crate::trust::may_govern`] treats a
/// `default: true` policy as claiming every namespace. An extra enqueue costs
/// one re-derivation that writes nothing; a missed one leaves a revoked key
/// green until something else reconciles the object.
fn policy_targets(
    objects: &reflector::Store<Restore>,
    scopes: &crate::trust::PolicyScopeMemory,
    policy: &crate::crds::trust_policy::TrustPolicy,
) -> Vec<ObjectRef<Restore>> {
    // The union of before and after: a `watches` mapper sees only the new
    // object, so an edit that narrows the scope would otherwise enqueue
    // nothing in the namespace it stopped governing. See
    // `trust::PolicyScopeMemory`.
    let scope = scopes.observe(policy);
    crate::verification::targets_in_scope(objects.state(), &scope)
}

/// [`reconcile`], with the re-trust pass in front of it.
///
/// The re-trust pass runs in front because a terminal object short-circuits:
/// [`reconcile_restore_inner`] returns before the verification block is
/// written, which keeps a finished run quiet. The re-trust pass re-derives the
/// verdict from what is already on the status, with no storage read and no
/// signature check, so it must sit where a terminal object still reaches it.
///
/// It runs only when the object already carries a `matchedKeyId`
/// ([`crate::verification::has_trust_verdict`]) and the policy reflector has
/// synced: an unsynced store looks like a cluster with no `TrustPolicy`, which
/// would resolve every namespace to the legacy roster.
async fn reconcile_with_trust(
    restore: Arc<Restore>,
    ctx: Arc<Context>,
    policies: reflector::Store<crate::crds::trust_policy::TrustPolicy>,
    synced: Arc<AtomicBool>,
    approval_policies: crate::approval_policy::PolicySource,
    objects: reflector::Store<Restore>,
) -> Result<Action, RestoreError> {
    if synced.load(Ordering::Relaxed) {
        let value = serde_json::to_value(&*restore).ok();
        let status = value.as_ref().and_then(|v| v.get("status"));
        if crate::verification::has_trust_verdict(status) {
            if let Some(namespace) = restore.namespace() {
                // The hook's one clock read: the signing-time plan, the
                // destination read and the re-derived verdict are judged at
                // one instant (FX-12).
                let now = Utc::now();
                let snapshot: Vec<crate::crds::trust_policy::TrustPolicy> =
                    policies.state().into_iter().map(|p| (*p).clone()).collect();
                let resolution =
                    crate::trust::resolve_with(&snapshot, &ctx.client, &namespace).await?;
                let api: Api<Restore> = Api::namespaced(ctx.client.clone(), &namespace);
                // One bounded re-read for a status that predates `signedAt`: a
                // block with a matched key, no signing time and no `trust`. It
                // goes through the same evidence path as the original verdict,
                // so a destination-backed run reads its own bucket, and one
                // whose grant only a pod may hold reads nothing and says so.
                let signing_time = match crate::verification::signing_time_need(status, now) {
                    crate::verification::ReadPlan::None => {
                        crate::verification::SigningTime::NotNeeded
                    }
                    // The backoff short-circuits before `evidence_source`, so a
                    // deferred pass costs neither a store read nor a destination
                    // read.
                    crate::verification::ReadPlan::Deferred => {
                        crate::verification::SigningTime::Deferred
                    }
                    crate::verification::ReadPlan::Read(need) => {
                        // A failed read is not a failed reconcile; see the same
                        // hunk in `backup.rs`.
                        let (handle, unread) = match backup::evidence_source_for(
                            restore.spec.evidence_destination_ref.as_ref(),
                            &ctx.client,
                            &namespace,
                            now,
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
                    now,
                    &signing_time,
                )
                .await?;
            }
        }
    }
    reconcile(restore, ctx, approval_policies, objects).await
}

/// Run the `Restore` controller until the process ends.
///
/// `Api::all`, and it `owns` the Jobs it creates so a pod terminating wakes
/// this reconciler through its Job rather than only on the requeue timer.
///
/// `archive` is the controller's one read-only archive handle, built once in
/// `main` and shared as `Arc<Store>` (interface **I13**). `None` is a
/// controller with no archive configured: it records the exit code and the
/// evidence keys and writes no scorecard-derived field.
///
/// `runner_image` is the runtime image and pull-policy override, read once in
/// `main`: an unset field is the compiled-in constant.
///
/// `approval_policies` is the installation's approval-policy document, read
/// once by `main` with the fresh-install marker as last read, and shared by
/// every per-namespace copy.
pub async fn controller(
    client: kube::Client,
    archive: Option<Arc<Store>>,
    runner_image: job::RunnerImage,
    approval_policies: crate::approval_policy::PolicySource,
) {
    // One watch per watched namespace: `crate::scope` is the whole cluster
    // unless `LOGWEIR_WATCH_NAMESPACES` names the execution namespaces, and
    // then this reconciler runs once per namespace with an `Api::namespaced`
    // watch, the only shape the scoped chart's RoleBindings permit.
    //
    // The `TrustPolicy` store is built once and shared by every copy: the kind
    // is cluster-scoped, so a reflector per namespace would be N identical
    // cluster-wide watches.
    let policies = crate::trust::spawn_policy_reflector(&client);
    crate::scope::run_everywhere(move |namespace| {
        controller_in(
            client.clone(),
            archive.clone(),
            runner_image.clone(),
            policies.clone(),
            approval_policies.clone(),
            namespace,
        )
    })
    .await;
}

/// One watch of [`controller`], over `namespace` (`None` is the whole
/// cluster).
fn controller_in(
    client: kube::Client,
    archive: Option<Arc<Store>>,
    runner_image: job::RunnerImage,
    shared: crate::trust::SharedPolicies,
    approval_policies: crate::approval_policy::PolicySource,
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
    // The one `TrustPolicy` store, shared by the trigger's resolutions and by
    // every copy of this reconciler. The trigger itself is a watch per copy: it
    // maps a policy event to the objects in this copy's store.
    let policies = shared.store;
    let synced = shared.synced;
    let policy_api: Api<crate::crds::trust_policy::TrustPolicy> = Api::all(client_for_watch);
    // One memory of what each policy bound last, owned by the mapper.
    let scopes = Arc::new(crate::trust::PolicyScopeMemory::default());
    async move {
        let controller = Controller::new(api, watcher::Config::default());
        let objects = controller.store();
        // The same store, for the manual-restore pool (`run_pool`).
        let pool_objects = controller.store();
        controller
            .owns(jobs, watcher::Config::default())
            // The re-trust trigger: a `TrustPolicy` event maps to the objects
            // this controller holds in the namespaces that policy could govern.
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
                        approval_policies.clone(),
                        pool_objects.clone(),
                    )
                },
                error_policy,
                ctx,
            )
            .for_each(|_| std::future::ready(()))
            .await;
    }
}
