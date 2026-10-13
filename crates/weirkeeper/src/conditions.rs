//! The runner's five exit codes as wire strings and condition reasons, and
//! the terminal states that are not an exit code.
//!
//! The contract: `0` pass · `1` operational, no artifact · `2` a result that is
//! not a pass, document written and signed · `3` refused by a guard, before
//! anything ran · `4` signing or lock proof failed, nothing uploaded. It is
//! implemented once here, so two reconcilers cannot spell one code two ways.
//!
//! # Two vocabularies, two functions
//!
//! [`wire_reason_for_exit`] returns the wire strings (`ok`, `operational`,
//! `drill-not-pass`, `guard-refused`, `signing-or-lock`), which land on
//! `status.exitReason` and are the spellings `logweir drill`'s own outcome
//! strings use.
//!
//! [`reason_for_exit`] returns the condition `reason`, in CamelCase (`Ok`,
//! `Operational`, `DrillNotPass`, `GuardRefused`, `SigningOrLock`): a
//! `metav1.Condition` `reason` must match
//! `^[A-Za-z]([A-Za-z0-9_,:]*[A-Za-z0-9_])?$`, which forbids `-`.
//! `the_condition_reasons_are_valid_metav1_reasons` checks every constant here
//! against that regex, and `the_two_reason_vocabularies_never_overlap` keeps
//! the two sets apart: neither vocabulary is used in the other's field.
//!
//! # The catch-all is the contract
//!
//! A container can exit with any of 256 codes (`137` for SIGKILL, `139` for a
//! segfault). Only `0..=4` are Logweir's contract; every other code is the
//! runner failing in a way the contract does not describe, which is
//! [`REASON_OPERATIONAL`], never a guess at one of the other four
//! (`every_exit_code_maps_to_its_wire_reason`).

/// Exit **0**: a pass. The one code the green badge accepts.
pub const REASON_OK: &str = "ok";

/// Exit **1**: an operational error. No artifact was written.
///
/// Also the reason for a Job that finished with no exit code at all: a run
/// whose code is unrecoverable produced no artifact either, and inventing a
/// code would be worse than classifying the absence.
pub const REASON_OPERATIONAL: &str = "operational";

/// Exit **2**: a result that is not a pass. A document was written and
/// signed, which makes this the most valuable result the tool produces rather
/// than a failure.
pub const REASON_DRILL_NOT_PASS: &str = "drill-not-pass";

/// Exit **3**: refused by a guard, before anything ran. The runner prints
/// `refusal-reason=<TerminalState>` as its final stdout line, carrying one of
/// [`TERMINAL_STATES`].
pub const REASON_GUARD_REFUSED: &str = "guard-refused";

/// Exit **4**: signing or the lock proof failed, and nothing was uploaded.
pub const REASON_SIGNING_OR_LOCK: &str = "signing-or-lock";

/// The `exitReason` wire string for `code`: exhaustive over `0..=4`, anything
/// else [`REASON_OPERATIONAL`]. Not the condition `reason`, which is
/// [`reason_for_exit`].
#[must_use]
pub fn wire_reason_for_exit(code: i32) -> &'static str {
    match code {
        0 => REASON_OK,
        1 => REASON_OPERATIONAL,
        2 => REASON_DRILL_NOT_PASS,
        3 => REASON_GUARD_REFUSED,
        4 => REASON_SIGNING_OR_LOCK,
        _ => REASON_OPERATIONAL,
    }
}

/// Exit **0**'s condition `reason`.
pub const CONDITION_REASON_OK: &str = "Ok";
/// Exit **0**'s `Complete` reason for an empty capture, set only beside a
/// passing verification of a receipt whose arms hold and whose window is empty
/// (`controllers::backup::with_empty_capture`): the source held no record in
/// any selected partition, so the run is not a recovery point. Never an
/// operational failure.
pub const CONDITION_REASON_EMPTY_CAPTURE: &str = "EmptyCapture";
/// Exit **1**'s condition `reason`. The crashed-Job case's `exitReason` says
/// the same thing in the other vocabulary ([`REASON_OPERATIONAL`]).
pub const CONDITION_REASON_OPERATIONAL: &str = "Operational";
/// Exit **2**'s condition `reason`.
pub const CONDITION_REASON_DRILL_NOT_PASS: &str = "DrillNotPass";
/// Exit **3**'s condition `reason`.
///
/// The condition says only "a guard refused"; which guard is
/// `status.exitReason`, off the runner's `refusal-reason=` line (or
/// [`TERMINAL_STATE_GUARD_REFUSED_UNKNOWN_REASON`] when it was absent). Also
/// the reason for a refusal this controller makes itself before any `POST`,
/// such as a topic pattern on the no-wildcard rail.
pub const CONDITION_REASON_GUARD_REFUSED: &str = "GuardRefused";
/// Exit **4**'s condition `reason`.
pub const CONDITION_REASON_SIGNING_OR_LOCK: &str = "SigningOrLock";

/// The condition `reason` for `code`, in CamelCase: the same partition as
/// [`wire_reason_for_exit`] in the other vocabulary, everything outside
/// `0..=4` [`CONDITION_REASON_OPERATIONAL`].
#[must_use]
pub fn reason_for_exit(code: i32) -> &'static str {
    match code {
        0 => CONDITION_REASON_OK,
        1 => CONDITION_REASON_OPERATIONAL,
        2 => CONDITION_REASON_DRILL_NOT_PASS,
        3 => CONDITION_REASON_GUARD_REFUSED,
        4 => CONDITION_REASON_SIGNING_OR_LOCK,
        _ => CONDITION_REASON_OPERATIONAL,
    }
}

/// The terminal states that are **not** an exit code.
///
/// Two producers, one list. Some are printed by the runner on its
/// `refusal-reason=` line and declared in `logweir_core::guard::TERMINAL_STATES`;
/// the runner's default `GuardRefused` is spelled
/// [`TERMINAL_STATE_GUARD_REFUSED_UNKNOWN_REASON`] here, because the
/// controller reaches it for a different observation (the line was absent or
/// unparseable). The rest are the controller's own: a pod that was never
/// scheduled wrote no log to say `PodUnschedulable`.
///
/// Not in `logweir-core`, which is the pure layer and knows nothing of pods or
/// Jobs. Neither list is derived from the other: one names what a refusal
/// message can say, this one what a status can say.
pub const TERMINAL_STATES: &[&str] = &[
    "Expired",
    "WindowNotCovered",
    "ApprovalNotReceived",
    "OrphanedScorecard",
    "DisruptedMidDrill",
    "PodUnschedulable",
    "TargetTopicConfigRefused",
    "CredentialNotRenderable",
    // The runner's refusal of a time selection by producer time over a
    // `LogAppendTime` source, read off `refusal-reason=`.
    "PointInTimeByProducerTime",
    // SASL/PLAIN without TLS: the resolver's refusal of such a `KafkaCluster`,
    // and the runner's `refusal-reason=` for such a plan.
    TERMINAL_STATE_PLAIN_WITHOUT_TLS,
    // A projected credential whose binding does not name the connection the
    // Job was built for, read off `refusal-reason=`.
    TERMINAL_STATE_CREDENTIAL_BINDING_MISMATCH,
    // An archive holding transaction markers under a plan that does not allow
    // them, read off `refusal-reason=`.
    logweir_core::guard::TERMINAL_STATE_TRANSACTION_MARKERS_IN_ARCHIVE,
    // A restore window that selects records in a segment the engine skips
    // (out-of-order timestamps), read off `refusal-reason=`.
    logweir_core::guard::TERMINAL_STATE_POINT_IN_TIME_SELECTION_INCOMPLETE,
    "NoExitCode",
    TERMINAL_STATE_POD_OWNERSHIP_CONTESTED,
    "GuardRefusedUnknownReason",
    "NameTooLong",
    "ReferentNotFound",
    "PlanConfigMapConflict",
    TERMINAL_STATE_APPROVAL_BUNDLE_CONFLICT,
    TERMINAL_STATE_APPROVAL_SUBJECT_MISMATCH,
    TERMINAL_STATE_STANDING_AUTHORIZATION_REFUSED,
    TERMINAL_STATE_JOB_NAME_CONFLICT,
    "ArchiveUrlUnreadable",
    "PlanHashMismatch",
    "ClusterNotReachable",
    TERMINAL_STATE_EXECUTION_SPEC_INVALID,
    TERMINAL_STATE_CONNECTION_CONFIG_INVALID,
    TERMINAL_STATE_CONNECTION_REFERENCE_INVALID,
    TERMINAL_STATE_CONNECTION_FIELD_UNSUPPORTED,
    TERMINAL_STATE_CONNECTION_PLAN_MISMATCH,
    TERMINAL_STATE_SCHEDULED_IDENTITY_MISMATCH,
    TERMINAL_STATE_SCHEDULE_NOT_FOUND,
    TERMINAL_STATE_RUN_POLICY_DIGEST_MISMATCH,
    TERMINAL_STATE_INVALID_TOPIC_SELECTION,
    TERMINAL_STATE_DISCOVERY_FAILED,
    TERMINAL_STATE_DISCOVERY_INCOMPLETE,
    TERMINAL_STATE_DISCOVERY_RESULT_UNREADABLE,
    TERMINAL_STATE_SELECTION_EMPTY,
    TERMINAL_STATE_SELECTION_TOO_LARGE,
    TERMINAL_STATE_SOURCE_CHANGED_DURING_RESOLUTION,
    // The four states a run reaches without an exit code because its runner
    // container never started: the `RunnerReady=False` reasons that cannot get
    // better on their own, written only when the matching diagnostic was
    // recorded before the Job ended (otherwise `NoExitCode`). `exitCode` stays
    // absent in all four.
    TERMINAL_STATE_VOLUME_MOUNT_FAILED,
    TERMINAL_STATE_CREDENTIAL_REFERENCE_MISSING,
    TERMINAL_STATE_RUNNER_IMAGE_UNAVAILABLE,
    TERMINAL_STATE_POD_CREATION_FORBIDDEN,
    // Declared and not yet produced (see the constant's note).
    TERMINAL_STATE_POINT_BINDING_MISMATCH,
    // The two admission refusals an approval policy adds. Both are properties
    // of two immutable specs and the installation's binding, so a requeue
    // could never rescue them.
    TERMINAL_STATE_APPROVAL_POLICY_MISMATCH,
    TERMINAL_STATE_AUTHORIZATION_EXPIRED,
    // PROD-05.1a: a backup of a cluster whose `spec.strimzi` is set could not
    // list its `KafkaTopic` resources.
    TERMINAL_STATE_OWNER_DETECTION_FAILED,
];

/// **PROD-05.1a.** A `Backup` of a `KafkaCluster` whose `spec.strimzi` is set,
/// and the controller could not list that Strimzi cluster's `KafkaTopic`
/// resources: the list was refused (the chart value
/// `strimzi.readKafkaTopics` grants it) or the API server does not serve the
/// resource. No runner Job is created: a receipt that said "looked, no owner"
/// would send a restore around an owner nobody checked for.
pub const TERMINAL_STATE_OWNER_DETECTION_FAILED: &str = "OwnerDetectionFailed";

/// The Approval a `Restore` names was issued under a different approval
/// policy, digest or mode than the namespace is bound to now: a v1 approval
/// under an explicit binding, a v2 document in an unbound namespace, or a
/// document whose policy has since been replaced. The downgrade refusal: an
/// ordinary confirmation presented in a governed namespace ends here with no
/// Job.
pub const TERMINAL_STATE_APPROVAL_POLICY_MISMATCH: &str = "ApprovalPolicyMismatch";

/// The authorization document v2 an Approval carries expired before this
/// Restore was admitted. Both specs are immutable, so the only way forward is a
/// new Restore and a new confirmation.
pub const TERMINAL_STATE_AUTHORIZATION_EXPIRED: &str = "AuthorizationExpired";

/// The archive holds a recovery point whose receipt is not the one the
/// approver signed a binding to.
///
/// Execution contract v2 binds a `Restore` to a recovery point
/// (`source.point{point_id, receipt_key, receipt_sha256, manifest_sha256}`)
/// inside the approved plan bytes; the runner re-verifies it against the bucket
/// before any data-plane work and refuses with exit 3. So this state means the
/// archive changed under an approved restore: a tampering signal, not a
/// configuration mistake, which an operator must not have to find in a pod log.
///
/// Declared and not yet produced: the runner reports the refusal as
/// `GuardRefused` until `PointBindingMismatch` joins
/// `logweir_core::guard::TERMINAL_STATES` and the runner's binding refusals
/// open with `<State>: ` (what `guard::terminal_state` matches). Declaring it
/// keeps the `metav1` reason vocabulary in one place.
pub const TERMINAL_STATE_POINT_BINDING_MISMATCH: &str = "PointBindingMismatch";

/// The condition carrying whether the one runner pod's `runner` container has
/// started.
///
/// `False` while the container cannot start, `True` once
/// `containerStatuses[name=runner].state.running|terminated` has been seen: the
/// answer to "why has nothing happened for four minutes" that `phase: Running`
/// cannot give (a pod in `ImagePullBackOff` is `Running` by every other
/// field). Every terminal builder carries it forward beside
/// [`CONDITION_VERIFIED`], because a merge patch replaces `status.conditions`.
pub const CONDITION_RUNNER_READY: &str = "RunnerReady";

/// [`CONDITION_RUNNER_READY`] `True`: the container has been seen running or
/// terminated.
pub const REASON_RUNNER_STARTED: &str = "RunnerStarted";

/// [`CONDITION_RUNNER_READY`] `False`: there is no pod yet, or there is one
/// and nothing has said why its container has not started.
///
/// Not a terminal state: "nothing has happened yet" is never a verdict about a
/// run, and a fail-fast on it would cancel every Job in a busy cluster.
pub const REASON_WAITING_FOR_POD: &str = "WaitingForPod";

/// A volume the runner pod declares did not mount.
///
/// Both a [`CONDITION_RUNNER_READY`] reason and, after `failFastSeconds`, a
/// terminal state. The diagnostic keeps the more specific code
/// (`weirkeeper::diagnostics::Code::SigningKeyMissing` for the signing volume)
/// and names the volume; a condition reason is a closed label other software
/// matches on, so parameters travel in the diagnostic, never in the reason.
pub const TERMINAL_STATE_VOLUME_MOUNT_FAILED: &str = "VolumeMountFailed";

/// A Secret or ConfigMap the pod's configuration names is not there. The
/// diagnostic distinguishes "no Secret", "no key in the Secret" and "no trust
/// bundle"; the condition reason is their class.
///
/// Not [`TERMINAL_STATE_CREDENTIAL_NOT_RENDERABLE`], which is the controller
/// refusing to render a credential into a Job it has not created; this is the
/// kubelet refusing to start a container in a Job that exists.
pub const TERMINAL_STATE_CREDENTIAL_REFERENCE_MISSING: &str = "CredentialReferenceMissing";

/// The runner image could not be pulled, is not present under a `Never` pull
/// policy, or is not a valid reference.
pub const TERMINAL_STATE_RUNNER_IMAGE_UNAVAILABLE: &str = "RunnerImageUnavailable";

/// The Job controller could not create the pod at all: a missing
/// ServiceAccount, a `ResourceQuota`, a PodSecurity level or an admission
/// webhook.
pub const TERMINAL_STATE_POD_CREATION_FORBIDDEN: &str = "PodCreationForbidden";

/// The typed `Backup.spec` cannot produce a runner execution: `triggeredBy` is
/// not `manual` or `schedule`, a `schedule` trigger lacks the complete
/// `BackupSchedule` identity (`scheduleRef`, `slot` and the deterministic object
/// name), a `manual` Backup names a schedule `slot`, or `deadlineSeconds` is
/// not positive. For a `Restore`: `spec.runnerResources` asks for requests or
/// limits the controller will not apply ([`crate::runner_resources::validate`]).
///
/// Terminal, because `spec` is CEL-immutable and the run identity is derived
/// from it and server metadata, never an annotation. Refused before any read or
/// `POST`; the message names every field.
pub const TERMINAL_STATE_EXECUTION_SPEC_INVALID: &str = "ExecutionSpecInvalid";

/// A `KafkaCluster`'s settings contradict each other or name an unusable value
/// ([`crate::connection::resolve`]): `plaintext` with `tls: true`,
/// `auth.tlsCa` with `tls: false`, a CA naming both or neither source, or a
/// bootstrap entry no client can dial. Refused before any Job, ConfigMap or
/// plan exists. Terminal for a `Backup` or `Restore` (immutable specs); on the
/// `KafkaCluster` itself it is re-evaluated every reconcile, so a controller
/// upgrade that understands the object clears it without an edit.
pub const TERMINAL_STATE_CONNECTION_CONFIG_INVALID: &str = "ConnectionConfigInvalid";

/// A credential or CA reference is missing a required part or names something
/// the kubelet could never resolve: an empty or non-DNS-1123 object name, or a
/// data key outside `[-._a-zA-Z0-9]+`.
///
/// It cannot say that the named Secret or ConfigMap exists or holds the key:
/// the controller reads neither kind, so a well-formed reference to a missing
/// object is still a pod that cannot start; the readiness check names that.
pub const TERMINAL_STATE_CONNECTION_REFERENCE_INVALID: &str = "ConnectionReferenceInvalid";

/// The object carries a connection field this controller does not implement:
/// the installed CRD is newer than the controller. Refused rather than
/// resolved without the field, which would be a silent downgrade. The message
/// names every such field.
pub const TERMINAL_STATE_CONNECTION_FIELD_UNSUPPORTED: &str = "ConnectionFieldUnsupported";

/// A `Restore`'s approved plan names a different target than the saved
/// connection its `spec.target.clusterRef` resolves to: bootstrap servers,
/// auth mode, username or TLS.
///
/// The runner dials the plan's address with the connection's credential and
/// CA, so a mismatch is a credential sent somewhere the connection does not
/// name, or TLS dialled without TLS. Terminal: the fix is a new plan from the
/// saved connection, and a new approval.
pub const TERMINAL_STATE_CONNECTION_PLAN_MISMATCH: &str = "ConnectionPlanMismatch";

/// A `KafkaCluster` (or plan) whose auth mode is `plain` without `tls: true`:
/// PLAIN sends the password itself, so it is refused before any Job exists.
/// The same string as the runner's own list
/// (`logweir_core::guard::TERMINAL_STATE_PLAIN_WITHOUT_TLS`). The CRD refuses
/// the same shape first; this is what an object admitted by an older CRD
/// meets.
pub const TERMINAL_STATE_PLAIN_WITHOUT_TLS: &str = logweir_core::connection::PLAIN_WITHOUT_TLS;

/// The runner refused the projected credential because the Secret's
/// `logweir-binding` is absent or names another connection or endpoint
/// (`logweir_core::connection::CredentialBindingRefusal`): a `KafkaCluster`
/// naming another connection's Secret cannot make Logweir present it to a
/// broker of its author's choosing. Nothing was dialled. The same string as
/// the runner's own list.
pub const TERMINAL_STATE_CREDENTIAL_BINDING_MISMATCH: &str =
    logweir_core::connection::CREDENTIAL_BINDING_MISMATCH;

/// A `scramSha512` `KafkaCluster` carries no `auth.username`, so the plan
/// document cannot name the identity the run will present.
///
/// The same state as the runner's own list, reached one step earlier: the
/// approval binds the identity, not just the address, and rendering
/// `sasl_username: ""` would produce a run that authenticates as nobody.
pub const TERMINAL_STATE_CREDENTIAL_NOT_RENDERABLE: &str = "CredentialNotRenderable";

/// `spec.approvalRef` names nothing at all.
///
/// Not [`REASON_APPROVAL_NOT_VERIFIED`]: "you did not ask for authorisation".
/// The ref's name is empty and `spec` is sealed, so no `Approval` can ever bind
/// and it is terminal; an `Approval` not yet arrived or not yet verified is a
/// 30-second hold (interface **I19**). One name for both would leave an
/// operator waiting for an approval that will never be looked for.
pub const TERMINAL_STATE_APPROVAL_NOT_RECEIVED: &str = "ApprovalNotReceived";

/// `sha256_prefixed(Restore.spec.planBytes)` is not the `plan_hash` inside the
/// approval's own signed bytes (interface **I18**).
///
/// Terminal: the approval authorises other bytes and both specs are immutable,
/// so re-approving the exact plan is the only way forward. The hash is read
/// from inside `Approval.spec.approvalBytes`, never from a `status`. The same
/// spelling as [`crate::controllers::approval::ApprovalRefusal`]'s own, because
/// it is one fact reached from two sides.
pub const TERMINAL_STATE_PLAN_HASH_MISMATCH: &str = "PlanHashMismatch";

/// `Restore.spec.target.clusterRef` resolves to nothing, or to a `KafkaCluster`
/// whose `status.reachable` is not `Some(true)`.
///
/// Terminal by ruling: an approval binds a plan to a target, and a target not
/// visible at admission should be re-authorised rather than start by itself
/// hours later. `status.reachable` is the connection probe's last look, not a
/// dial this reconciler made.
pub const TERMINAL_STATE_CLUSTER_NOT_REACHABLE: &str = "ClusterNotReachable";

/// Exit **2** whose scorecard `outcome` names an archive-coverage failure: a
/// signed scorecard says the archive did not cover the requested window. See
/// [`crate::controllers::restore::OUTCOME_FAIL_COVERAGE`] for which `outcome`
/// values the frozen 1.0.0 schema admits.
pub const TERMINAL_STATE_WINDOW_NOT_COVERED: &str = "WindowNotCovered";

/// The target topic's own configuration refused the restore (guard **G-TS**),
/// printed by the runner on its `refusal-reason=` line. The controller maps it
/// and produces no part of it (interface **I9**).
pub const TERMINAL_STATE_TARGET_TOPIC_CONFIG_REFUSED: &str = "TargetTopicConfigRefused";

/// The archive exists without its detached sidecar. The controller records
/// it; it does not delete, repair, or render the orphan as a result.
pub const TERMINAL_STATE_ORPHANED_SCORECARD: &str = "OrphanedScorecard";

/// The pod carried a `DisruptionTarget` condition: the node went away
/// mid-run.
pub const TERMINAL_STATE_DISRUPTED_MID_DRILL: &str = "DisruptedMidDrill";

/// The pod never scheduled — `PodScheduled=False` with reason
/// `Unschedulable`. It wrote no log, so there is nothing to read.
pub const TERMINAL_STATE_POD_UNSCHEDULABLE: &str = "PodUnschedulable";

/// The Job finished and no container named `runner` reported a terminated
/// state, so the exit code is unrecoverable.
///
/// A finished Job with zero pods is this state, not a crash: the pod was
/// garbage-collected (or never created) and the code went with it.
pub const TERMINAL_STATE_NO_EXIT_CODE: &str = "NoExitCode";

/// More than one pod claimed this run's Job as its controller owner, so none
/// was read (seam **S6**).
///
/// Not a sub-case of [`TERMINAL_STATE_NO_EXIT_CODE`]: a Job with
/// `backoffLimit: 0` and `restartPolicy: Never` cannot produce two pods, and an
/// `ownerReferences` entry is ordinary metadata, so a principal that read the
/// Job's UID minted at least one claimant. The controller reads none and says
/// so by name; `NoExitCode` would send an operator looking for a deleted pod.
///
/// Not in [`crate::cadence::RETRYABLE_TERMINAL_STATES`]: a retry would meet the
/// same planted claimant.
pub const TERMINAL_STATE_POD_OWNERSHIP_CONTESTED: &str = "PodOwnershipContested";

/// Exit 3, and the pod log carried no parseable `refusal-reason=` line.
pub const TERMINAL_STATE_GUARD_REFUSED_UNKNOWN_REASON: &str = "GuardRefusedUnknownReason";

/// The object's own name is longer than [`crate::slot::NAME_LIMIT`], so no Job
/// it created could ever be found again.
///
/// The API server refuses such a Job (pod labels are at most 63 characters),
/// and a requeue would leave the object with no status at all, for ever. The
/// scheduled path refuses at name-minting time with the same spelling
/// ([`crate::slot::SlotError::NameTooLong`]): one refusal, one name.
pub const TERMINAL_STATE_NAME_TOO_LONG: &str = "NameTooLong";

/// `spec.sourceRef` names a `KafkaCluster` that does not exist in this
/// namespace.
///
/// Terminal, not a requeue: `spec` is CEL-immutable, so a `sourceRef` that
/// resolves to nothing stays that way, and a requeue would be an object with an
/// empty status and no explanation. Created later, the referent does not revive
/// the `Backup`: the plan is rendered once, from a spec that cannot change.
pub const TERMINAL_STATE_REFERENT_NOT_FOUND: &str = "ReferentNotFound";

/// The plan ConfigMap already exists and is **not owned by this `Backup`**.
///
/// A 409 on the ConfigMap `POST` is ordinarily success (an earlier pass created
/// the identical object). Not when the existing object belongs to something
/// else: the Job would mount a plan a stranger wrote for a run carrying this
/// `Backup`'s signing key. The owner reference's UID is checked, and a mismatch
/// is terminal.
pub const TERMINAL_STATE_PLAN_CONFIG_MAP_CONFLICT: &str = "PlanConfigMapConflict";

/// A per-Restore approval-bundle ConfigMap exists under the desired name but
/// does not exactly match the owner UID, immutable bit, binding annotations,
/// or public artifact bytes the controller intended.
pub const TERMINAL_STATE_APPROVAL_BUNDLE_CONFLICT: &str = "ApprovalBundleConflict";

/// The referenced Approval was verified for a different kind, name,
/// namespace, or Kubernetes UID than the Restore being reconciled.
pub const TERMINAL_STATE_APPROVAL_SUBJECT_MISMATCH: &str = "ApprovalSubjectMismatch";

/// The `Restore` carries `spec.authorization`, a standing rehearsal
/// authorization, withdrawn in v1. Terminal: `spec` is sealed. The `Approval`
/// controller refuses an `Approval` naming a `RehearsalSchedule` under the same
/// reason.
pub const TERMINAL_STATE_STANDING_AUTHORIZATION_REFUSED: &str = "StandingAuthorizationRefused";

/// A Job already occupies the Restore's name but is not controlled by this
/// exact Restore UID with the complete garbage-collection owner contract.
pub const TERMINAL_STATE_JOB_NAME_CONFLICT: &str = "JobNameConflict";

/// `spec.archive.url` is not readable as an object-store location.
///
/// The rendered `backup.yaml` carries a typed `storage` block whose variants
/// have incompatible required fields, so a URL with no scheme or an unsupported
/// one cannot be rendered. Terminal for the same reason as
/// [`TERMINAL_STATE_REFERENT_NOT_FOUND`].
pub const TERMINAL_STATE_ARCHIVE_URL_UNREADABLE: &str = "ArchiveUrlUnreadable";

/// The `Complete` condition type, for a run that exited **0**.
pub const CONDITION_COMPLETE: &str = "Complete";

/// The `Failed` condition type, for every other terminal outcome.
pub const CONDITION_FAILED: &str = "Failed";

/// The condition type a `Backup` carries while its Job exists and has not
/// finished.
pub const CONDITION_JOB_CREATED: &str = "JobCreated";

/// Whether a `Restore`'s admission passed: its own condition type.
///
/// Not a `Failed` condition: an `Approval` that has not arrived is a hold, and
/// a condition array is a map keyed by `type`, so a `Failed=False` at
/// admission and a `Failed=True` later would be one field with two values,
/// rejected the day the array gets `x-kubernetes-list-type: map`. So `True`
/// with [`REASON_ADMITTED`] on the pass that creates the Job, `False` with
/// [`REASON_APPROVAL_NOT_VERIFIED`] while it waits, and a terminal admission
/// refusal writes `Failed`, because that one ends the object's life.
pub const CONDITION_ADMITTED: &str = "Admitted";

/// The `reason` on an [`CONDITION_ADMITTED`] condition that passed.
pub const REASON_ADMITTED: &str = "Admitted";

/// The `reason` on an `Admitted=False` condition while a manual run waits for a
/// slot in its namespace's manual-run pool ([`crate::run_pool`]).
///
/// The same spelling as
/// [`logweir_core::check_contract::CheckCode::ConcurrencyLimited`], which a
/// queued `Preflight` and `TopicDiscovery` carry: one fact, one word, one
/// remedy. A hold, not in [`TERMINAL_STATES`]; it clears when a slot frees.
pub const REASON_CONCURRENCY_LIMITED: &str = "ConcurrencyLimited";

/// `phase` for a manual run waiting in its namespace's manual-run pool.
///
/// Nonterminal, and safe to add: every terminal predicate treats an unknown
/// phase as active, so an older controller simply runs a `Queued` object. No
/// plan `ConfigMap`, Job or execution claim exists for a queued run, which
/// separates it from `Pending` (a hold on a missing destination or approval)
/// and `Running`.
pub const PHASE_QUEUED: &str = "Queued";

/// The `reason` while a `Restore` waits for its `Approval` (interface
/// **I19**): a hold, not a verdict, and not in [`TERMINAL_STATES`]. It can
/// change without anybody touching the object, so it requeues at
/// [`crate::controllers::restore::ADMISSION_REQUEUE_SECS`]. Its terminal
/// sibling is [`TERMINAL_STATE_APPROVAL_NOT_RECEIVED`].
pub const REASON_APPROVAL_NOT_VERIFIED: &str = "ApprovalNotVerified";

/// The verified inputs could not be materialized into the immutable public
/// bundle, so no Job was created and the controller will retry.
pub const REASON_APPROVAL_BUNDLE_MATERIALIZATION_FAILED: &str =
    "ApprovalBundleMaterializationFailed";

/// Whether the run's evidence keys were recorded: its own condition type,
/// raised at exit 0 (and, on a `Restore`, at exit 2 when keys were read).
///
/// Not a second `Failed` condition: exits 1, 3 and 4 write no artifact by
/// contract, and a `Failed/False/EvidenceKeysUnreadable` beside
/// `Failed/True/GuardRefused` is two values in one map key (a
/// `FindStatusCondition` reader sees whichever comes first, and a
/// `listMapKey: [type]` schema would reject the patch), with an untrue message
/// besides.
///
/// So it is raised only where the contract promised an artifact: `True` with
/// [`REASON_EVIDENCE_KEYS_RECORDED`] when the keys were read, `False` with
/// [`REASON_EVIDENCE_KEYS_UNREADABLE`] when they were not, nothing at exits 1,
/// 3 and 4. A `Restore` at exit 2 raises `True` when its runner named the
/// signed failure's keys, and nothing when an older runner printed none.
pub const CONDITION_EVIDENCE_RECORDED: &str = "EvidenceRecorded";

/// The condition reason for a log body that did not carry both key lines.
///
/// No key is guessed: a receipt key is derivable from a backup id, but deriving
/// one would point a status field at an object that may not exist, which
/// verification would report `Invalid` rather than unread.
pub const REASON_EVIDENCE_KEYS_UNREADABLE: &str = "EvidenceKeysUnreadable";

/// The condition reason for an exit-0 run whose two key lines WERE read.
///
/// The positive arm exists because "the keys are recorded" and "nobody has
/// looked" are different answers a reader of `status.evidence` alone cannot
/// tell apart while the status patch is in flight.
pub const REASON_EVIDENCE_KEYS_RECORDED: &str = "EvidenceKeysRecorded";

/// The condition that says whether the UI may render a green badge (interface
/// **I21**).
///
/// One type, two rules: a `Backup` is green on `Valid` + `exitCode == 0`, a
/// `Restore` on `Valid` + `outcome == pass`
/// (`crate::verification::{backup_badge, restore_badge}`). The message is the
/// badge label: "verified by weirkeeper at `<verifiedAt>` against key
/// `<matchedKeyId>`" when green, the literal word `unverified` otherwise, never
/// `pass` and never "verified in your browser".
pub const CONDITION_VERIFIED: &str = "Verified";

/// [`CONDITION_VERIFIED`]'s reason when the badge is green.
pub const REASON_VERIFIED: &str = "Verified";

/// [`CONDITION_VERIFIED`]'s reason when the controller verified the evidence
/// and the answer was no. A claim about the document.
pub const REASON_VERIFICATION_INVALID: &str = "VerificationInvalid";

/// [`CONDITION_VERIFIED`]'s reason when no verification happened at all (no
/// evidence credential, an unreadable object, no signing key material). A
/// claim about the controller.
pub const REASON_VERIFICATION_NOT_ATTEMPTED: &str = "VerificationNotAttempted";

/// [`CONDITION_VERIFIED`]'s reason when the signature verified and the signer
/// is one this installation will not accept.
///
/// A claim about the key: the document is exactly what it says, and the key
/// that made it is revoked, unknown to the resolved `TrustPolicy`, declared for
/// another usage, or used outside its trusted window. The operator's next step
/// is their trust policy, not their archive.
pub const REASON_VERIFICATION_UNTRUSTED: &str = "VerificationUntrusted";

/// [`CONDITION_VERIFIED`]'s reason when a `Backup`'s evidence verified and the
/// run did not exit 0. The `Backup` half of interface **I21**: there is no
/// `outcome` on that path.
pub const REASON_EXIT_CODE_NOT_ZERO: &str = "ExitCodeNotZero";

/// [`CONDITION_VERIFIED`]'s reason when a `Restore`'s evidence verified and
/// its `outcome` is not `pass`. The `Restore` half of interface **I21**.
pub const REASON_OUTCOME_NOT_PASS: &str = "OutcomeNotPass";

/// [`CONDITION_VERIFIED`]'s reason when a `Restore`'s evidence verified and
/// its signed complete verification says `covered: false`.
///
/// A real one signs `fail-integrity` and exits 2; this reason is judged before
/// `outcome`, so an operator can alert on it. It is also the reason of a status
/// saying `pass` beside `covered: false`, a combination no accepted document
/// carries, read anyway because a badge rule trusting one field to imply
/// another would be one runner defect away from green.
pub const REASON_COMPLETE_NOT_COVERED: &str = "CompleteNotCovered";

/// A `Backup` whose runner Job is not known to execute this object's frozen
/// execution inputs. `True` is the problem being present; a Job created from
/// frozen inputs raises no condition, because `status.execution` is the
/// positive record.
///
/// The Job is observed to completion and never changed, deleted or re-derived:
/// a Job that already exists is the compatibility boundary.
pub const CONDITION_EXECUTION_INPUTS_UNVERIFIED: &str = "ExecutionInputsUnverified";

/// [`CONDITION_EXECUTION_INPUTS_UNVERIFIED`]'s reason for a Job created by a
/// controller that predates frozen execution inputs: the Job carries no inputs
/// digest and the `Backup` has no `status.execution`. Its argv may have come
/// from the legacy `logweir.dev/runner-argv` annotation.
pub const REASON_LEGACY_EXECUTION: &str = "LegacyExecution";

/// [`CONDITION_EXECUTION_INPUTS_UNVERIFIED`]'s reason for a Job whose inputs
/// digest annotation does not equal `status.execution.inputsSha256` (either
/// side may be absent) — for example a Job an older controller created after a
/// rollback, from a `Backup` a newer controller had already frozen.
pub const REASON_JOB_INPUTS_MISMATCH: &str = "JobInputsMismatch";

/// A `Backup` carrying the legacy `logweir.dev/runner-argv` annotation, which
/// this controller never executes. `True` while the annotation is present on
/// an object whose run this controller derives or refuses. The message names
/// the annotation's size and digest, never its content.
pub const CONDITION_RUNNER_ARGV_ANNOTATION_IGNORED: &str = "RunnerArgvAnnotationIgnored";

/// [`CONDITION_RUNNER_ARGV_ANNOTATION_IGNORED`]'s reason for an annotation that
/// is a JSON array of strings. Whether it equals the derived argv is stated in
/// the message; it is not executed either way.
pub const REASON_RUNNER_ARGV_ANNOTATION_IGNORED: &str = "AnnotationIgnored";

/// [`CONDITION_RUNNER_ARGV_ANNOTATION_IGNORED`]'s reason for an annotation that
/// is not a JSON array of strings.
pub const REASON_RUNNER_ARGV_ANNOTATION_MALFORMED: &str = "AnnotationMalformed";

/// Every condition `reason` this crate writes that is not one of
/// [`TERMINAL_STATES`], in one list, so
/// `the_condition_reasons_are_valid_metav1_reasons` iterates this and
/// [`TERMINAL_STATES`] and cannot go stale, and
/// `the_two_reason_vocabularies_never_overlap` asserts no member is a
/// [`wire_reason_for_exit`] value.
pub const CONDITION_REASONS: &[&str] = &[
    CONDITION_REASON_OK,
    CONDITION_REASON_EMPTY_CAPTURE,
    CONDITION_REASON_OPERATIONAL,
    CONDITION_REASON_DRILL_NOT_PASS,
    CONDITION_REASON_GUARD_REFUSED,
    CONDITION_REASON_SIGNING_OR_LOCK,
    REASON_EVIDENCE_KEYS_UNREADABLE,
    REASON_EVIDENCE_KEYS_RECORDED,
    CONDITION_JOB_CREATED,
    REASON_ADMITTED,
    REASON_APPROVAL_NOT_VERIFIED,
    REASON_CONCURRENCY_LIMITED,
    REASON_APPROVAL_BUNDLE_MATERIALIZATION_FAILED,
    REASON_VERIFIED,
    REASON_VERIFICATION_INVALID,
    REASON_VERIFICATION_NOT_ATTEMPTED,
    REASON_VERIFICATION_UNTRUSTED,
    REASON_EXIT_CODE_NOT_ZERO,
    REASON_OUTCOME_NOT_PASS,
    REASON_COMPLETE_NOT_COVERED,
    REASON_LEGACY_EXECUTION,
    REASON_JOB_INPUTS_MISMATCH,
    REASON_RUNNER_ARGV_ANNOTATION_IGNORED,
    REASON_RUNNER_ARGV_ANNOTATION_MALFORMED,
    REASON_DISCOVERY_RUNNING,
    REASON_RESOLVED,
    REASON_RETAINED,
    REASON_HISTORY_LARGE,
    REASON_LEGACY_OWNER_REFERENCES_REMAIN,
    REASON_ACTIVE_LEGACY_RUNS_OWNED,
    REASON_MIGRATION_BLOCKED,
    REASON_USE_RETENTION_POLICY,
    REASON_CAUGHT_UP,
    REASON_CATCH_UP_BLOCKED,
    REASON_RETRY_SCHEDULED,
    REASON_RETRY_PENDING,
    REASON_RETRY_BLOCKED,
    REASON_RETRY_EXHAUSTED,
    REASON_RUN_FAILED,
    REASON_SLOT_NAME_UNAVAILABLE,
    REASON_ACTIVE_RUN_LIMIT,
    REASON_UNKNOWN_TIME_ZONE,
    REASON_INVALID_TOPIC_SELECTION,
    REASON_INVALID_RUN_POLICY,
    REASON_CRD_OUTDATED,
    // The other four `RunnerReady=False` reasons are `TERMINAL_STATES`
    // members, which the regex test iterates too.
    REASON_RUNNER_STARTED,
    REASON_WAITING_FOR_POD,
];

/// The identity a scheduled `Backup` claims is not the one its own fields
/// compose. Terminal, and never re-read as a manual run: a manual run executes
/// under its own UID, and would write a second archive of a window a
/// scheduled run already owns.
pub const TERMINAL_STATE_SCHEDULED_IDENTITY_MISMATCH: &str = "ScheduledIdentityMismatch";

/// A scheduled `Backup` whose `spec.scheduleRef` names a schedule this
/// namespace does not have. Deleting a schedule stops future work; checked only
/// before the freeze, so it never kills a run already executing.
pub const TERMINAL_STATE_SCHEDULE_NOT_FOUND: &str = "ScheduleNotFound";

/// `spec.scheduleRef.runPolicySha256` disagrees with the digest recomputed
/// from this object's own policy fields.
pub const TERMINAL_STATE_RUN_POLICY_DIGEST_MISMATCH: &str = "RunPolicyDigestMismatch";

/// `spec.topics` and `spec.allUserTopics` do not form one of the two legal
/// selection shapes, or a name in either is not Kafka-legal.
pub const TERMINAL_STATE_INVALID_TOPIC_SELECTION: &str = "InvalidTopicSelection";

/// The discovery Job did not produce a usable result: unreachable broker,
/// timeout, authentication failure, a non-zero exit. Retryable
/// ([`crate::cadence::RETRYABLE_TERMINAL_STATES`]): a broker that was down can
/// be up in five minutes, and nothing was decided.
pub const TERMINAL_STATE_DISCOVERY_FAILED: &str = "DiscoveryFailed";

/// Discovery could not prove it saw every topic and the policy says `Refuse`.
///
/// Not retryable: the same principal would get the same partial answer; an
/// ACL or an administrator attestation changes it.
pub const TERMINAL_STATE_DISCOVERY_INCOMPLETE: &str = "DiscoveryIncomplete";

/// The discovery output was malformed: a missing or duplicated summary, a
/// count or digest that does not match the lines, a name no broker could have
/// produced. Not retryable: malformed output is a defect, not a blip.
pub const TERMINAL_STATE_DISCOVERY_RESULT_UNREADABLE: &str = "DiscoveryResultUnreadable";

/// Dynamic selection resolved to nothing: every user topic excluded, or an
/// empty cluster. The runner is never started: a backup of no topics is not a
/// backup.
pub const TERMINAL_STATE_SELECTION_EMPTY: &str = "SelectionEmpty";

/// Dynamic selection resolved to more than the run may carry.
pub const TERMINAL_STATE_SELECTION_TOO_LARGE: &str = "SelectionTooLarge";

/// The source connection resolved to something different between discovery and
/// the freeze: a `KafkaCluster` edited or replaced mid-resolution.
pub const TERMINAL_STATE_SOURCE_CHANGED_DURING_RESOLUTION: &str = "SourceChangedDuringResolution";

/// `phase` while a dynamic run is resolving its topic list.
///
/// Nonterminal, and safe to add: `backup_is_terminal` treats an unknown phase
/// as active, so an older controller does not mistake it for finished.
pub const PHASE_RESOLVING: &str = "Resolving";

/// The condition type carrying a dynamic run's topic resolution.
pub const CONDITION_TOPICS_RESOLVED: &str = "TopicsResolved";

/// `TopicsResolved=False` while the discovery Job is running.
pub const REASON_DISCOVERY_RUNNING: &str = "DiscoveryRunning";

/// `TopicsResolved=True` once the list is frozen.
pub const REASON_RESOLVED: &str = "Resolved";

/// The condition type carrying whether a schedule's run history is retained
/// independently of the schedule object.
pub const CONDITION_HISTORY_RETAINED: &str = "HistoryRetained";

/// `HistoryRetained=True`: no run is owned by the schedule any more.
pub const REASON_RETAINED: &str = "Retained";

/// `HistoryRetained=True` with a warning: the retained history is large enough
/// to be worth an operator's attention.
pub const REASON_HISTORY_LARGE: &str = "HistoryLarge";

/// `HistoryRetained=False`: legacy controller ownerReferences remain.
pub const REASON_LEGACY_OWNER_REFERENCES_REMAIN: &str = "LegacyOwnerReferencesRemain";

/// `HistoryRetained=False`: a run the schedule still owns is active, so its
/// ownerReference is not removed yet.
pub const REASON_ACTIVE_LEGACY_RUNS_OWNED: &str = "ActiveLegacyRunsOwned";

/// `HistoryRetained=False`: the migration cannot proceed.
pub const REASON_MIGRATION_BLOCKED: &str = "MigrationBlocked";

/// The condition a `BackupSchedule` carries while it still sets the withdrawn
/// `spec.retention`. `True` names the field; the schedule backs up as before.
pub const CONDITION_RETENTION_REPORT_WITHDRAWN: &str = "RetentionReportWithdrawn";

/// [`CONDITION_RETENTION_REPORT_WITHDRAWN`]'s reason.
pub const REASON_USE_RETENTION_POLICY: &str = "UseRetentionPolicy";

/// [`CONDITION_RETENTION_REPORT_WITHDRAWN`]'s message.
pub const RETENTION_REPORT_WITHDRAWN_MESSAGE: &str =
    "spec.retention no longer reports; use a RetentionPolicy";

/// `BackupSchedule` `Ready` reason: every missed slot has been caught up.
pub const REASON_CAUGHT_UP: &str = "CaughtUp";
/// `Ready` reason: a catch-up is due but something blocks it.
pub const REASON_CATCH_UP_BLOCKED: &str = "CatchUpBlocked";
/// `Ready` reason: a retry has been admitted.
pub const REASON_RETRY_SCHEDULED: &str = "RetryScheduled";
/// `Ready` reason: a retry is waiting out its delay.
pub const REASON_RETRY_PENDING: &str = "RetryPending";
/// `Ready` reason: a retry is due but concurrency or a newer slot blocks it.
pub const REASON_RETRY_BLOCKED: &str = "RetryBlocked";
/// `Ready` reason: the chain reached `maxRetries` without succeeding.
pub const REASON_RETRY_EXHAUSTED: &str = "RetryExhausted";
/// `Ready` reason: the last run failed and the policy does not retry it.
pub const REASON_RUN_FAILED: &str = "RunFailed";
/// `Ready` reason: the deterministic name for a due slot is already taken by
/// an object this schedule does not own.
pub const REASON_SLOT_NAME_UNAVAILABLE: &str = "SlotNameUnavailable";
/// `Ready` reason: the per-schedule active-run ceiling is reached.
pub const REASON_ACTIVE_RUN_LIMIT: &str = "ActiveRunLimit";
/// `Ready=False`: `spec.timeZone` names a zone this build's tzdb does not
/// have. Fail closed: never silently UTC.
pub const REASON_UNKNOWN_TIME_ZONE: &str = "UnknownTimeZone";
/// `Ready=False`: the topic selection is not one of the two legal shapes.
pub const REASON_INVALID_TOPIC_SELECTION: &str = "InvalidTopicSelection";
/// `Ready=False`: the run policy has a field-level problem.
pub const REASON_INVALID_RUN_POLICY: &str = "InvalidRunPolicy";
/// `Ready=False`: the object uses a field the installed CRD declares and this
/// controller does not understand: a CRD applied ahead of its controller.
pub const REASON_CRD_OUTDATED: &str = "CrdOutdated";

/// The condition types a `Backup` can carry, in one list. A condition array is
/// a map keyed by `type`, so two conditions sharing a `type` is a malformed
/// status; `no_two_conditions_share_a_type` measures written statuses against
/// this.
pub const CONDITION_TYPES: &[&str] = &[
    CONDITION_COMPLETE,
    CONDITION_FAILED,
    CONDITION_JOB_CREATED,
    CONDITION_EVIDENCE_RECORDED,
    CONDITION_ADMITTED,
    CONDITION_VERIFIED,
    CONDITION_EXECUTION_INPUTS_UNVERIFIED,
    CONDITION_RUNNER_ARGV_ANNOTATION_IGNORED,
    CONDITION_TOPICS_RESOLVED,
    CONDITION_HISTORY_RETAINED,
    CONDITION_RETENTION_REPORT_WITHDRAWN,
    CONDITION_RUNNER_READY,
];

/// `phase` for a `Restore` whose admission has not passed yet (interface
/// **I19**).
///
/// Not `Failed`: a `Restore` created before its `Approval` is verified sits
/// here with one `Admitted=False` condition and is released the moment the
/// approval verifies, which makes a dangling `approvalRef` workable rather
/// than fatal. A `Backup`'s Job is created on the first pass, so it has no
/// such state.
pub const PHASE_PENDING: &str = "Pending";

/// `phase` while the Job exists and has not finished.
pub const PHASE_RUNNING: &str = "Running";

/// `phase` for a run that exited **0**.
pub const PHASE_SUCCEEDED: &str = "Succeeded";

/// `phase` for every other terminal outcome, the crashed-Job case included.
pub const PHASE_FAILED: &str = "Failed";

// The status-write contract: one implementation for every reconciler.
//
// A reconciler's own status patch wakes it: a changed byte bumps
// `resourceVersion`, the watch fires, and the next reconcile writes another
// changing byte (12,107 reconciles in 91 s on an untouched object, live). The
// invariant: a status patch is a pure function of the object and its
// children, never of the clock. Three rules implement it, here:
//
// 1. A condition's `lastTransitionTime` moves only when its `status` or
//    `reason` moves ([`merge_condition`], the `metav1.Condition` contract).
//    `message` is not compared: it carries instants that move by design.
// 2. A "when computed" field (`lastSlot.decidedAt`, `lastEvaluation.at`) is
//    written only when the thing it timestamps changed
//    ([`keep_instant_unless_changed`]), compared on the JSON the API server
//    will store, never a typed struct, which can see a difference the merge
//    patch cannot carry and stamp a fresh instant for ever. A struct block goes
//    through [`replacing`] first, so a field that became `None` is cleared.
// 3. If the patch would not change the object, no patch is sent
//    ([`status_unchanged`]): the backstop that makes rules 1 and 2 observable
//    in a test (zero PATCHes) and turns a redundant write into none.
//
// The comparison is RFC 7386, not `==`: every reconciler patches with
// `Patch::Merge`, where an omitted key means "leave it alone" and `null` means
// "delete it", so the question is whether applying the patch changes anything.
// [`apply_merge_patch`] is the API server's half, written out. Arrays are
// replaced, never merged, which is why every condition array is built from
// `Condition` and serialised by `serde`: a hand-built element spelling an
// optional field differently would compare unequal for ever.

/// The object's current condition of this `type`, if it carries one.
///
/// The lookup half of [`merge_condition`], separate only because each kind's
/// status struct has its own path to the vector.
#[must_use]
pub fn current_condition<'a>(
    conditions: Option<&'a Vec<crate::crds::Condition>>,
    r#type: &str,
) -> Option<&'a crate::crds::Condition> {
    conditions?.iter().find(|c| c.r#type == r#type)
}

/// `next`, carrying the `lastTransitionTime` the `metav1.Condition` contract
/// says it should: the one it ALREADY has when neither `status` nor `reason`
/// changed, and `next`'s own otherwise.
///
/// The one comparison: every reconciler calls this, and none compares
/// timestamps itself, so the rule cannot hold on some kinds and not others.
///
/// `existing` is `None` for a first write, and then `next` is returned
/// unchanged: the first time a condition appears IS a transition.
#[must_use]
pub fn merge_condition(
    existing: Option<&crate::crds::Condition>,
    next: crate::crds::Condition,
) -> crate::crds::Condition {
    match existing {
        Some(c) if c.status == next.status && c.reason == next.reason => crate::crds::Condition {
            last_transition_time: c.last_transition_time,
            ..next
        },
        _ => next,
    }
}

/// `owned` upserted into the conditions the object already carries.
///
/// A merge patch replaces arrays, so the builder that writes
/// `status.conditions` deletes every type it does not name. A builder
/// therefore owes the conditions it does not own, which is everything the
/// object carries that this patch is not about, not a list someone must
/// remember to extend ([`crate::verification::carry_conditions`]' allow-list
/// is the narrower, older form).
///
/// Stored order is kept and new types are appended in `owned`'s order:
/// `status_unchanged` compares arrays element by element, so a reshuffle would
/// make every pass a write and a wake-up. An element of `owned` wins over the
/// stored one of the same `type`. `Vec<Value>`, not `Vec<Condition>`, for
/// [`crate::verification::carry_conditions`]' reason.
#[must_use]
pub fn upsert_conditions(
    stored: Option<&Vec<crate::crds::Condition>>,
    owned: Vec<serde_json::Value>,
) -> Vec<serde_json::Value> {
    let mut out: Vec<serde_json::Value> = stored
        .map(|cs| cs.iter().map(|c| serde_json::json!(c)).collect())
        .unwrap_or_default();
    for condition in owned {
        let r#type = condition.get("type").cloned();
        match out.iter().position(|c| c.get("type").cloned() == r#type) {
            Some(at) => out[at] = condition,
            None => out.push(condition),
        }
    }
    out
}

/// `owned` first, then every condition the object carries whose type `owned`
/// does not name: the terminal builders' form of [`upsert_conditions`].
///
/// A terminal object is reconciled once more and then never, so stored order
/// buys nothing, and the terminal builders' contract is that `conditions[0]`
/// is the `Complete`/`Failed` one the scalar `status.reason` quotes.
///
/// The rule is still "carry everything you do not own": a terminal patch that
/// dropped `Admitted` would take the statement that the run was approved off
/// the object an auditor inspects. A condition a builder owns may transition;
/// it may not disappear.
#[must_use]
pub fn carry_remaining(
    stored: Option<&Vec<crate::crds::Condition>>,
    mut owned: Vec<serde_json::Value>,
) -> Vec<serde_json::Value> {
    for condition in stored.map(Vec::as_slice).unwrap_or_default() {
        let r#type = serde_json::json!(condition.r#type);
        if owned.iter().any(|c| c.get("type") == Some(&r#type)) {
            continue;
        }
        owned.push(serde_json::json!(condition));
    }
    owned
}

/// Apply an RFC 7386 JSON merge patch to `target`, exactly as the API server
/// would.
///
/// `null` deletes the key; an object merges recursively; anything else,
/// arrays included, replaces. A non-object `target` under an object patch
/// becomes an empty object first (the RFC's own rule), so a first write onto an
/// absent status comes out right.
pub fn apply_merge_patch(target: &mut serde_json::Value, patch: &serde_json::Value) {
    let Some(fields) = patch.as_object() else {
        *target = patch.clone();
        return;
    };
    if !target.is_object() {
        *target = serde_json::Value::Object(serde_json::Map::new());
    }
    let map = target
        .as_object_mut()
        .expect("target was just made an object");
    for (k, v) in fields {
        if v.is_null() {
            map.remove(k);
        } else {
            apply_merge_patch(map.entry(k.clone()).or_insert(serde_json::Value::Null), v);
        }
    }
}

/// Whether sending `next` would leave `current` byte-for-byte as it is, in
/// which case the reconciler sends nothing.
///
/// `current` is the object's status (`None` for none yet); `next` is the whole
/// patch body, `{"status": {…}}`. A body with no `status` key changes no status
/// and is answered as unchanged rather than asserted, because a panic here
/// would be on a path whose purpose is to avoid a write.
#[must_use]
pub fn status_unchanged(current: Option<&serde_json::Value>, next: &serde_json::Value) -> bool {
    let Some(status) = next.get("status") else {
        return true;
    };
    let current = current.cloned().unwrap_or(serde_json::Value::Null);
    let mut merged = current.clone();
    apply_merge_patch(&mut merged, status);
    merged == current
}

/// `next`, a struct block serialised for a merge patch, made to replace the
/// block `stored` under the same key instead of merging into it.
///
/// A status field that has become `None` serialises as an absent key, which a
/// merge patch reads as "leave it alone": a slot record could keep the previous
/// slot's `backupRef`, and a typed "same decision?" comparison would say
/// "changed" on every pass while no write could make them equal. So every key
/// `stored` carries and `next` does not is sent as an explicit `null`,
/// recursively through nested objects. Arrays are left alone (a merge patch
/// replaces them). A non-object `next`, or an absent or non-object `stored`,
/// is returned unchanged.
#[must_use]
pub fn replacing(next: serde_json::Value, stored: Option<&serde_json::Value>) -> serde_json::Value {
    let serde_json::Value::Object(mut fields) = next else {
        return next;
    };
    let Some(serde_json::Value::Object(old)) = stored else {
        return serde_json::Value::Object(fields);
    };
    for (key, old_value) in old {
        match fields.get_mut(key) {
            None => {
                fields.insert(key.clone(), serde_json::Value::Null);
            }
            Some(new_value) => {
                let taken = std::mem::take(new_value);
                *new_value = replacing(taken, Some(old_value));
            }
        }
    }
    serde_json::Value::Object(fields)
}

/// Put `stored[instant]` back into `next` when merging `next` onto `stored`
/// would change nothing else, so a "when computed" instant moves only when the
/// thing it timestamps moves. Returns whether it did.
///
/// `stored` and `next` are one status block (`status.lastSlot`,
/// `status.lastEvaluation`), `next` as it will be sent. The comparison is
/// [`status_unchanged`]'s, made on the merged JSON the API server will store,
/// with `instant` left out of both sides. When the block did change, `next`
/// keeps the instant it was built with, which is the caller's `now`.
///
/// A `stored` block with no `instant` keeps nothing: there is no earlier
/// instant to keep, and the first one written is this one.
pub fn keep_instant_unless_changed(
    stored: Option<&serde_json::Value>,
    next: &mut serde_json::Value,
    instant: &str,
) -> bool {
    let Some(stored) = stored.filter(|s| s.is_object()) else {
        return false;
    };
    let Some(kept) = stored.get(instant).filter(|v| !v.is_null()).cloned() else {
        return false;
    };
    let without = |value: &serde_json::Value| {
        let mut value = value.clone();
        if let Some(fields) = value.as_object_mut() {
            fields.remove(instant);
        }
        value
    };
    let mut merged = stored.clone();
    apply_merge_patch(&mut merged, next);
    if without(&merged) != without(stored) {
        return false;
    }
    if let Some(fields) = next.as_object_mut() {
        fields.insert(instant.to_string(), kept);
        return true;
    }
    false
}

// The fourth rule, seam **S7**: every `/status` write carries
// `metadata.resourceVersion` as a precondition. The API server applies a
// `resourceVersion` in a merge-patch body as an update precondition and
// answers `409 Conflict` on a mismatch, which gives a merge PATCH a
// compare-and-set without the `update` verb. Without it, a pass computing from
// a stale watch-cache copy overwrites what a concurrent writer stored, and a
// `Backup` has four writers (its reconciler, `backup_selection`,
// `backup_schedule`'s reservation and `schedule_history`).
// [`patch_status_preconditioned`] is the one implementation.
//
// A 409 is not swallowed or retried here: it reaches the reconciler, whose
// `error_policy` requeues, and the next pass recomputes from what the other
// writer stored. A retry inside this helper would write a status computed from
// inputs nobody re-observed. A caller that needs a later write within the
// pass opts in by name through [`StatusVersion`], passing the version the
// previous write left (`Backup`'s freeze → resolve → running sequence, and
// both terminal → verification sequences).

/// The `metadata.resourceVersion` the next `/status` write of this pass must
/// precondition on.
///
/// After a pass's first PATCH the object it was handed is stale by one
/// version, and a second write preconditioned on it would be refused, so every
/// write returns where the object now stands and multi-write sequences thread
/// it forward. Not `#[must_use]`: discarding it after a single write is correct
/// and common.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StatusVersion(Option<String>);

impl StatusVersion {
    /// Where the object a pass was handed stands: the precondition for that
    /// pass's first write. An empty string (a hand-built fixture) is treated
    /// as absent.
    #[must_use]
    pub fn observed(meta: &kube::core::ObjectMeta) -> Self {
        Self(meta.resource_version.clone().filter(|v| !v.is_empty()))
    }

    /// The version itself, or `None` for an object that carries none.
    #[must_use]
    pub fn get(&self) -> Option<&str> {
        self.0.as_deref()
    }
}

/// Write `/status` under seam **S7**'s precondition, unless the patch would
/// change nothing.
///
/// `at` is where the object stands ([`StatusVersion`]); `current` is the
/// status this pass believes is stored, for [`status_unchanged`]. Returns
/// where the object stands after the call: the response's version when a patch
/// was sent, `at` unchanged when none was.
///
/// # Errors
///
/// * [`kube::Error::Discovery`], shaped as the API server's "no
///   resourceVersion" answer, for an object that carries none: unreachable for
///   anything from a watch or a `get`, and never written without its
///   precondition.
/// * The API error for anything else, `409 Conflict` included (see this
///   section's header).
pub async fn patch_status_preconditioned<K>(
    api: &kube::Api<K>,
    kind: &str,
    name: &str,
    at: &StatusVersion,
    current: Option<&serde_json::Value>,
    patch: serde_json::Value,
) -> Result<StatusVersion, kube::Error>
where
    K: kube::Resource + Clone + std::fmt::Debug + serde::de::DeserializeOwned,
{
    if status_unchanged(current, &patch) {
        tracing::debug!(
            kind,
            object = name,
            "the computed status equals the one on the object; no patch is sent"
        );
        return Ok(at.clone());
    }
    let Some(resource_version) = at.get() else {
        return Err(kube::Error::Discovery(
            kube::error::DiscoveryError::MissingResource(format!(
                "{kind} {name} carries no metadata.resourceVersion, which a /status \
                 compare-and-set needs (D-SEAMS S7)"
            )),
        ));
    };
    let mut body = patch;
    body.as_object_mut()
        .expect("a status patch is always a JSON object")
        .insert(
            "metadata".to_string(),
            serde_json::json!({ "name": name, "resourceVersion": resource_version }),
        );
    let applied = api
        .patch_status(
            name,
            &kube::api::PatchParams::default(),
            &kube::api::Patch::Merge(body),
        )
        .await?;
    Ok(StatusVersion::observed(applied.meta()))
}
