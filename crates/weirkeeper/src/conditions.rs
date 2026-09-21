//! Global Constraint 11's five exit codes as **wire strings**, and the
//! terminal states that are not an exit code.
//!
//! # Why this module exists at all
//!
//! GC11's contract — `0` pass · `1` operational, no artifact · `2` a result
//! that is not a pass, document written AND signed · `3` refused by a guard,
//! before anything ran · `4` signing or lock proof failed, nothing uploaded —
//! is written out in four places in this corpus (`docs/stability.md`,
//! `docs/kubernetes.md` §1, `crates/logweir/src/exit.rs`, the `EXIT` printer
//! column's description) and, until this file, implemented in none of them on
//! the controller side. A whole exit-code-to-condition table in prose with no
//! function behind it is how two reconcilers come to spell the same code two
//! ways.
//!
//! # TWO VOCABULARIES, TWO FUNCTIONS, AND THE FIELD EACH ONE LANDS ON
//!
//! [`wire_reason_for_exit`] returns GC11's **wire strings** — `ok`,
//! `operational`, `drill-not-pass`, `guard-refused`, `signing-or-lock`. They
//! land on `Backup.status.exitReason`, they are read by the UI (Task 26) and
//! by `docs/kubernetes.md`'s exit-code table, and they are the same spellings
//! `logweir drill`'s own outcome strings use
//! (`crates/logweir/src/drill/mod.rs:61`'s `drill-not-pass: {0}`) — so a
//! consumer never has to accept two spellings for one fact.
//!
//! [`reason_for_exit`] returns the **condition `reason`**, and it is
//! CamelCase: `Ok`, `Operational`, `DrillNotPass`, `GuardRefused`,
//! `SigningOrLock`. A `metav1.Condition`'s `reason` is validated upstream
//! against `^[A-Za-z]([A-Za-z0-9_,:]*[A-Za-z0-9_])?$`, **which forbids `-`**,
//! so a hyphenated reason is a value the API server rejects the day anything
//! gives this CRD's hand-rolled condition schema the real pattern.
//! `the_condition_reasons_are_valid_metav1_reasons` iterates every condition
//! reason constant in this module against that regex.
//!
//! THE ONE FUNCTION WAS THE DEFECT. Task 17's brief mandated
//! `reason_for_exit(code)` as the condition `reason`, which put a wire string
//! in a field whose vocabulary is CamelCase — review finding LOW-2, plan
//! errata **E5b**. The fix is not to re-spell the wire strings: that would put
//! `DrillNotPass` on `exitReason` and make it disagree with the drill path and
//! with the shipped CRD's own field description, which is why the errata says
//! `exitReason` keeps its own vocabulary. It is to stop using one vocabulary
//! in the other's field. The two sets never overlap, and
//! `the_two_reason_vocabularies_never_overlap` asserts it.
//!
//! # Why the exit-code functions have a catch-all and why that is not a hole
//!
//! A container can terminate with any of 256 codes: `137` for a SIGKILL,
//! `139` for a segfault, `2` from a shell that could not find the binary. Only
//! `0..=4` are Logweir's contract, and every other code is, by definition, the
//! runner failing in a way the contract does not describe — which is
//! [`REASON_OPERATIONAL`] and never a guess at one of the other four. The
//! `match` is exhaustive over the contract and the catch-all is reached only
//! outside it; `every_exit_code_maps_to_its_wire_reason` asserts both halves.

/// Exit **0** — a pass. The one code the green badge accepts.
pub const REASON_OK: &str = "ok";

/// Exit **1** — an operational error. **No artifact was written.**
///
/// Also the reason for a Job that finished with **no exit code at all** (the
/// crashed-Job case, `controllers::backup::reconcile_backup` step 4): a run
/// whose code is unrecoverable produced no artifact either, and inventing a
/// code to classify it would be worse than classifying the absence.
pub const REASON_OPERATIONAL: &str = "operational";

/// Exit **2** — a result that is not a pass. **A document WAS written and
/// signed**, which is what makes this the most valuable result the tool
/// produces rather than a failure.
pub const REASON_DRILL_NOT_PASS: &str = "drill-not-pass";

/// Exit **3** — refused by a guard, before anything ran. The runner prints
/// `refusal-reason=<TerminalState>` as its final stdout line for this code
/// (GC11); one of [`TERMINAL_STATES`] is what that line carries.
pub const REASON_GUARD_REFUSED: &str = "guard-refused";

/// Exit **4** — signing or the lock proof failed, and **nothing was
/// uploaded**.
pub const REASON_SIGNING_OR_LOCK: &str = "signing-or-lock";

/// The `exitReason` wire string for `code` — Global Constraint 11's own
/// vocabulary.
///
/// Exhaustive over `0..=4`; anything else is [`REASON_OPERATIONAL`]. See the
/// module note for why the catch-all is the contract and not a gap, and for
/// why this is NOT the condition `reason` — that is [`reason_for_exit`].
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
/// Exit **1**'s condition `reason`. The crashed-Job case's `exitReason` says
/// the same thing in the other vocabulary ([`REASON_OPERATIONAL`]).
pub const CONDITION_REASON_OPERATIONAL: &str = "Operational";
/// Exit **2**'s condition `reason`.
pub const CONDITION_REASON_DRILL_NOT_PASS: &str = "DrillNotPass";
/// Exit **3**'s condition `reason`.
///
/// The CONDITION says only "a guard refused"; WHICH guard is
/// `status.exitReason`, off the runner's own `refusal-reason=` line (or
/// [`TERMINAL_STATE_GUARD_REFUSED_UNKNOWN_REASON`] when that line was absent).
/// It is also the reason for a refusal this controller makes ITSELF, before any
/// `POST` — a topic pattern behind Global Constraint 18(c)'s no-wildcard rail.
pub const CONDITION_REASON_GUARD_REFUSED: &str = "GuardRefused";
/// Exit **4**'s condition `reason`.
pub const CONDITION_REASON_SIGNING_OR_LOCK: &str = "SigningOrLock";

/// The condition `reason` for `code` — **CamelCase**, per errata E5b.
///
/// The same partition as [`wire_reason_for_exit`], in the other vocabulary:
/// exhaustive over `0..=4`, with everything else
/// [`CONDITION_REASON_OPERATIONAL`], because a container that terminated with
/// `137` or `139` failed in a way GC11's contract does not describe.
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
/// TWO PRODUCERS, ONE LIST. Three of these are printed by the RUNNER on its
/// `refusal-reason=` line and are declared in
/// `logweir_core::guard::TERMINAL_STATES` — `TargetTopicConfigRefused`,
/// `CredentialNotRenderable`, and the default `GuardRefused` which this list
/// spells [`TERMINAL_STATE_GUARD_REFUSED_UNKNOWN_REASON`] on the controller
/// side because the controller reaches it for a DIFFERENT observation (the
/// line was absent or unparseable, not "the message named no state"). The rest
/// are the controller's own: nothing in a pod log says `PodUnschedulable`,
/// because a pod that was never scheduled wrote no log at all.
///
/// WHY THE LIST IS HERE AND NOT IN `logweir-core`. `logweir-core` is inside
/// the pure layer (Global Constraint 1) and knows nothing about pods, Jobs or
/// scorecards; half of these states are observations only a Kubernetes client
/// can make. The two lists are not merged and neither is derived from the
/// other: one names what a refusal MESSAGE can say, this one names what a
/// `Backup`'s status can say.
pub const TERMINAL_STATES: &[&str] = &[
    "Expired",
    "WindowNotCovered",
    "ApprovalNotReceived",
    "OrphanedScorecard",
    "DisruptedMidDrill",
    "PodUnschedulable",
    "TargetTopicConfigRefused",
    "CredentialNotRenderable",
    "NoExitCode",
    TERMINAL_STATE_POD_OWNERSHIP_CONTESTED,
    "GuardRefusedUnknownReason",
    "NameTooLong",
    "ReferentNotFound",
    "PlanConfigMapConflict",
    TERMINAL_STATE_APPROVAL_BUNDLE_CONFLICT,
    TERMINAL_STATE_APPROVAL_SUBJECT_MISMATCH,
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
    // D3 §2.2 — the four states a run can reach WITHOUT AN EXIT CODE because
    // its runner container never started. They are the `RunnerReady=False`
    // reasons that cannot get better on their own, and each one is written
    // only when the matching diagnostic was recorded BEFORE the Job ended;
    // otherwise `crash_terminal_state`'s existing table is unchanged and the
    // answer is still `NoExitCode`. `exitCode` stays absent in all four.
    TERMINAL_STATE_VOLUME_MOUNT_FAILED,
    TERMINAL_STATE_CREDENTIAL_REFERENCE_MISSING,
    TERMINAL_STATE_RUNNER_IMAGE_UNAVAILABLE,
    TERMINAL_STATE_POD_CREATION_FORBIDDEN,
    // D3 §5.5 step 6, W5's hand-off — DECLARED AND NOT YET PRODUCED. See the
    // constant's own note for the two runner-side edits that make it
    // reachable; it is here so the controller does not have to grow a second
    // reason vocabulary when they land.
    TERMINAL_STATE_POINT_BINDING_MISMATCH,
];

// ===========================================================================
// D3 §2.2 — the `RunnerReady` condition, and the states a run reaches when
// its runner container never starts
// ===========================================================================

/// D3 §5.5 step 6: the archive holds a recovery point whose receipt is not the
/// one the approver signed a binding to — **W5's hand-off**, review finding F6.
///
/// # What it means
///
/// Execution contract v2 binds a `Restore` to a specific recovery point
/// (`source.point{point_id, receipt_key, receipt_sha256, manifest_sha256}`)
/// inside the plan bytes the approval's signature covers. The runner
/// re-verifies that binding against the bytes actually in the bucket before
/// any data-plane work and refuses with exit 3 when they disagree. So this
/// state means the archive changed under an approved restore — the one shape
/// that is a tampering signal and not a configuration mistake, and the one an
/// operator must never have to tell apart from an ordinary guard refusal by
/// reading a pod log.
///
/// # It is declared here and NOT YET PRODUCED, and that is the hand-off
///
/// `crates/logweir/src/drill/binding.rs` says in so many words that
/// `logweir_core::guard::TERMINAL_STATES` "is a closed three-element list owned
/// elsewhere … until that list grows (it is the status worker's to extend) the
/// refusal classifies as the general `GuardRefused`". `logweir-core` is not in
/// this worker's D3 §14 ownership row, so the controller half lands here and
/// the runner half is recorded as an explicit hand-off rather than reached for:
///
/// 1. add `PointBindingMismatch` (and D3 §4.3's `RehearsalScopeViolation`) to
///    `logweir_core::guard::TERMINAL_STATES`;
/// 2. prefix `binding.rs`'s two refusal messages with `<State>: `, which is
///    what `guard::terminal_state` matches on.
///
/// Until both land, `refusal_state` reads `GuardRefused` off the log and this
/// constant is never written. Declaring it now is what makes the controller
/// side ready and keeps the `metav1` reason vocabulary in one place; it is not
/// a claim that the state is reachable today.
pub const TERMINAL_STATE_POINT_BINDING_MISMATCH: &str = "PointBindingMismatch";

/// The condition carrying whether the one runner pod's `runner` container has
/// started — D3 §2.2, PLAT-14.1.
///
/// `False` WHILE THE CONTAINER CANNOT START, `True` once
/// `containerStatuses[name=runner].state.running|terminated` has been seen.
/// It is the answer to "why has nothing happened for four minutes", which
/// `phase: Running` could never give: a `Backup` whose pod sits in
/// `ImagePullBackOff` is `Running` by every existing field on the object.
///
/// EVERY TERMINAL BUILDER CARRIES IT FORWARD, beside
/// [`CONDITION_VERIFIED`]: a JSON merge patch REPLACES `status.conditions`, so
/// a builder that emits one and not the other deletes the one it left out.
pub const CONDITION_RUNNER_READY: &str = "RunnerReady";

/// [`CONDITION_RUNNER_READY`] `True`: the container has been seen running or
/// terminated.
pub const REASON_RUNNER_STARTED: &str = "RunnerStarted";

/// [`CONDITION_RUNNER_READY`] `False`: there is no pod yet, or there is one
/// and nothing has said why its container has not started.
///
/// **NOT A TERMINAL STATE, AND THAT IS THE ASSERTION.** "Nothing has happened
/// yet" is the one answer that can never be a verdict about a run; a
/// fail-fast on it would cancel every Job created in a busy cluster.
pub const REASON_WAITING_FOR_POD: &str = "WaitingForPod";

/// A volume the runner pod declares did not mount — D3 §2.2.
///
/// Both a [`CONDITION_RUNNER_READY`] reason and, after `failFastSeconds` of
/// it, a terminal state. The DIAGNOSTIC keeps the more specific code
/// (`weirkeeper::diagnostics::Code::SigningKeyMissing` for the signing volume,
/// `VolumeMountFailed` for any other) and names the volume in its message; a
/// `metav1` condition reason is a closed label other software matches on, so
/// the parameters travel in the diagnostic and never in the reason.
pub const TERMINAL_STATE_VOLUME_MOUNT_FAILED: &str = "VolumeMountFailed";

/// A Secret or ConfigMap the pod's configuration names is not there — D3
/// §2.2. The diagnostic distinguishes "no Secret", "no key in the Secret" and
/// "no trust bundle"; the condition reason is their class.
///
/// **NOT [`TERMINAL_STATE_CREDENTIAL_NOT_RENDERABLE`]**, which is the
/// controller refusing to RENDER a credential into a Job it has not created
/// yet. This one is the kubelet refusing to start a container in a Job that
/// exists.
pub const TERMINAL_STATE_CREDENTIAL_REFERENCE_MISSING: &str = "CredentialReferenceMissing";

/// The runner image could not be pulled, is not present under a `Never` pull
/// policy, or is not a valid reference — D3 §2.2.
pub const TERMINAL_STATE_RUNNER_IMAGE_UNAVAILABLE: &str = "RunnerImageUnavailable";

/// The Job controller could not create the pod at all: a missing
/// ServiceAccount, a `ResourceQuota`, a PodSecurity level or an admission
/// webhook — D3 §2.2.
pub const TERMINAL_STATE_POD_CREATION_FORBIDDEN: &str = "PodCreationForbidden";

/// The typed `Backup.spec` cannot produce a runner execution: `triggeredBy` is
/// not `manual` or `schedule`, a `schedule` trigger lacks the complete
/// `BackupSchedule` controller identity (owner reference, `scheduleRef`,
/// `slot` and the deterministic object name), a `manual` Backup names a
/// schedule `slot`, or `deadlineSeconds` is not positive — PLAT-06.1.
///
/// TERMINAL, because `spec` is CEL-immutable and the run identity is derived
/// from it and from server-generated metadata, never from an annotation. The
/// condition message names the field.
pub const TERMINAL_STATE_EXECUTION_SPEC_INVALID: &str = "ExecutionSpecInvalid";

/// A `KafkaCluster`'s settings contradict each other or name an unusable
/// value — PLAT-07.1, [`crate::connection::resolve`].
///
/// `plaintext` with `tls: true` (TLS without SASL, which contract v1 does not
/// dial rather than dial without TLS), `auth.tlsCa` with `tls: false`, a CA
/// naming both or neither source, or a bootstrap entry no client can dial.
/// Refused before any Job, ConfigMap or plan exists. Terminal for a `Backup` or
/// `Restore` (their `spec` and the referent's are immutable); on the
/// `KafkaCluster` itself it is re-evaluated on every reconcile, so a controller
/// upgrade that understands the object clears it without an edit.
pub const TERMINAL_STATE_CONNECTION_CONFIG_INVALID: &str = "ConnectionConfigInvalid";

/// A credential or CA reference is missing a required part or names something
/// the kubelet could never resolve — an empty or non-DNS-1123 object name, or a
/// data key outside `[-._a-zA-Z0-9]+`. PLAT-07.1.
///
/// WHAT IT CANNOT SAY: that the named Secret or ConfigMap EXISTS, or holds the
/// key. The controller holds no read on Secrets (spec §9) and checks neither
/// kind, so a well-formed reference to a missing object is still a pod that
/// cannot start; PLAT-03.1's readiness check is where that becomes a named
/// prerequisite.
pub const TERMINAL_STATE_CONNECTION_REFERENCE_INVALID: &str = "ConnectionReferenceInvalid";

/// The object carries a connection field this controller does not implement —
/// the CRD installed is newer than the controller (a rollback, or a CRD
/// applied ahead of its controller). PLAT-07.1.
///
/// Refused rather than resolved without the field, because a connection built
/// without a setting its author asked for is exactly the silent downgrade a
/// rollback must not perform. The message names every such field.
pub const TERMINAL_STATE_CONNECTION_FIELD_UNSUPPORTED: &str = "ConnectionFieldUnsupported";

/// A `Restore`'s approved plan names a different target than the saved
/// connection its `spec.target.clusterRef` resolves to — bootstrap servers,
/// auth mode, username or TLS. PLAT-07.1.
///
/// The runner dials the PLAN's address with the CONNECTION's credential and CA,
/// so a mismatch is a credential sent somewhere the saved connection does not
/// name, or a TLS connection dialled without TLS. Terminal: both `spec`s are
/// immutable and the plan is approved bytes, so the fix is a new plan built
/// from the saved connection, and a new approval.
pub const TERMINAL_STATE_CONNECTION_PLAN_MISMATCH: &str = "ConnectionPlanMismatch";

/// A `scramSha512` `KafkaCluster` carries no `auth.username`, so the plan
/// document cannot name the identity the run will present.
///
/// SHARED WITH THE RUNNER'S OWN LIST ON PURPOSE. `logweir_core::guard`'s
/// `TERMINAL_STATES` declares this state for the runner's
/// `refusal-reason=` line; the controller reaches the same FACT one step
/// earlier — spec §4's "the approval binds the identity, not just the
/// address" is unsatisfiable with no identity to bind, and rendering
/// `sasl_username: ""` would produce a run that authenticates as nobody and a
/// receipt that says so.
pub const TERMINAL_STATE_CREDENTIAL_NOT_RENDERABLE: &str = "CredentialNotRenderable";

/// `spec.approvalRef` names nothing at all — the `Restore` half, Task 20.
///
/// A DIFFERENT FACT FROM [`REASON_APPROVAL_NOT_VERIFIED`], AND THE SPLIT IS
/// THE WHOLE POINT. This one is "you did not ask for authorisation": the ref's
/// name is empty, `spec` is sealed by a CEL rule, and no `Approval` anyone
/// creates can ever bind to this object — so it is TERMINAL. An `Approval`
/// that has simply not arrived yet, or has arrived and is not `Verified=True`,
/// is the other one and is a 30-second HOLD (interface **I19**). Reporting
/// both under one name is how an operator comes to wait for an approval that
/// will never be looked for.
pub const TERMINAL_STATE_APPROVAL_NOT_RECEIVED: &str = "ApprovalNotReceived";

/// `sha256_prefixed(Restore.spec.planBytes)` is not the `plan_hash` inside the
/// approval's own signed bytes — Task 20, interface **I18**.
///
/// TERMINAL. The approval authorises bytes; these are different bytes; and
/// both `spec`s are immutable, so re-approving the exact plan is the only way
/// forward. The hash is recomputed at Job-creation time and read from inside
/// `Approval.spec.approvalBytes` — never from either object's `status`, which
/// is a controller-written cache nobody signed.
///
/// THE SAME SPELLING AS [`crate::controllers::approval::ApprovalRefusal`]'s
/// OWN `PlanHashMismatch`, ON PURPOSE. The `Approval` reconciler reaches the
/// same fact from the other side — it hashes the referent's bytes while
/// verifying the signature — and two halves of one refusal reported under two
/// names is how an operator comes to think they are two problems.
pub const TERMINAL_STATE_PLAN_HASH_MISMATCH: &str = "PlanHashMismatch";

/// `Restore.spec.target.clusterRef` resolves to nothing, or to a
/// `KafkaCluster` whose `status.reachable` is not `Some(true)` — Task 20.
///
/// TERMINAL BY CONTROLLER RULING, and the reasoning is that an approval binds
/// a plan to a target: a target that was not visible at admission time is a
/// run an operator should re-authorise rather than one that starts by itself
/// hours later. `status.reachable` is written by Task 15c's probe, so the
/// value this reads is the control plane's own last look and not a dial this
/// reconciler made.
pub const TERMINAL_STATE_CLUSTER_NOT_REACHABLE: &str = "ClusterNotReachable";

/// Exit **2** whose scorecard `outcome` names an archive-coverage failure —
/// Task 20.
///
/// The run happened, was measured, and a scorecard WAS written and signed
/// (Global Constraint 11's code 2); what it says is that the archive did not
/// cover the window the plan asked for.
/// [`crate::controllers::restore::OUTCOME_FAIL_COVERAGE`] carries the
/// measurement about which `outcome` values the frozen 1.0.0 schema actually
/// admits.
pub const TERMINAL_STATE_WINDOW_NOT_COVERED: &str = "WindowNotCovered";

/// The target topic's own configuration refused the restore — guard **G-TS**,
/// printed by the RUNNER on its `refusal-reason=` line.
///
/// The controller MAPS it and produces no part of it (interface **I9**): Task
/// 3 owns `guard.rs`'s refusal printer and Task 8 the target-topic preflight.
/// Declared here because `Restore.status.exitReason` is where it lands.
pub const TERMINAL_STATE_TARGET_TOPIC_CONFIG_REFUSED: &str = "TargetTopicConfigRefused";

/// The archive exists without its detached sidecar — spec §3.2.
///
/// The controller records it. It does **not** delete, does **not** repair and
/// does **not** render the orphan as a result
/// (`design-operator.md:527-535`).
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
/// **A finished Job with zero pods is this state**, not a crash: the pod was
/// garbage-collected (or never created) and the code went with it.
pub const TERMINAL_STATE_NO_EXIT_CODE: &str = "NoExitCode";

/// More than one pod claimed this run's Job as its controller owner, so none
/// of them was read — D-SEAMS **S6**, defect `SEC-PODLOG`, review finding R1.
///
/// **NOT A SUB-CASE OF [`TERMINAL_STATE_NO_EXIT_CODE`].** "No pod reported"
/// is an accident — garbage collection, an eviction, a node that went away.
/// This is a namespace in which two objects claim one identity, which a Job
/// pinned to `backoffLimit: 0` and `restartPolicy: Never` cannot produce, and
/// an `ownerReferences` entry is ordinary metadata written by whoever created
/// the pod. So at least one claimant was minted by a principal that read the
/// Job's UID, and the controller cannot tell which. It reads none of them and
/// says so under its own name, because an operator seeing `NoExitCode` would
/// go looking for a deleted pod and find two.
///
/// Deliberately **not** in [`crate::cadence::RETRYABLE_TERMINAL_STATES`]: a
/// retry re-runs the same Job into the same namespace and the same principal
/// plants the same second claimant.
pub const TERMINAL_STATE_POD_OWNERSHIP_CONTESTED: &str = "PodOwnershipContested";

/// Exit 3, and the pod log carried no parseable `refusal-reason=` line.
pub const TERMINAL_STATE_GUARD_REFUSED_UNKNOWN_REASON: &str = "GuardRefusedUnknownReason";

/// The object's own name is longer than [`crate::slot::NAME_LIMIT`], so no
/// Job it created could ever be found again.
///
/// REVIEW FINDING **MEDIUM-1**, plan errata **E5d**. Measured: the API server
/// refuses the Job (`spec.template.labels: Invalid value: … must be no more
/// than 63 characters`), the reconciler turned that into a requeue, and the
/// `Backup` sat with `status: null` — no phase, no condition, an empty `PHASE`
/// column — **forever**. Task 18 guards the SCHEDULED path at name-minting
/// time with this same spelling ([`crate::slot::SlotError::NameTooLong`]);
/// Task 26's create page and any hand-written `Backup` reach the reconciler
/// directly, and the brief's own principle — never leave the CR in `Running`
/// forever, never invent a code — had no counterpart for "never leave the CR
/// with no status at all".
///
/// THE SAME SPELLING ON PURPOSE. Two halves of one refusal reported under two
/// names is how an operator comes to think they are two problems.
pub const TERMINAL_STATE_NAME_TOO_LONG: &str = "NameTooLong";

/// `spec.sourceRef` names a `KafkaCluster` that does not exist in this
/// namespace.
///
/// A TERMINAL REFUSAL AND NOT A REQUEUE (errata **E5a**). `spec` is
/// CEL-immutable, so a `sourceRef` that resolves to nothing today resolves to
/// nothing forever unless somebody creates that object — and a requeue in the
/// meantime is an object with an empty status and no explanation, which is the
/// shape review finding MEDIUM-1 was about. If the referent is created later,
/// the `Backup` is still terminal and a new one is the answer: the plan
/// document is rendered ONCE, at Job-creation time, from a spec that cannot
/// change.
pub const TERMINAL_STATE_REFERENT_NOT_FOUND: &str = "ReferentNotFound";

/// The plan ConfigMap already exists and is **not owned by this `Backup`**.
///
/// A 409 on the ConfigMap `POST` is ordinarily success — the previous pass of
/// this same reconcile created it, and the object is byte-identical because it
/// is rendered from an immutable spec. It is NOT success when the existing
/// object belongs to something else: writing the Job then would mount a plan
/// document a stranger wrote, over the same mount path, for a run that carries
/// this `Backup`'s signing key. So the owner reference's UID is checked, and a
/// mismatch is terminal.
pub const TERMINAL_STATE_PLAN_CONFIG_MAP_CONFLICT: &str = "PlanConfigMapConflict";

/// A per-Restore approval-bundle ConfigMap exists under the desired name but
/// does not exactly match the owner UID, immutable bit, binding annotations,
/// or public artifact bytes the controller intended.
pub const TERMINAL_STATE_APPROVAL_BUNDLE_CONFLICT: &str = "ApprovalBundleConflict";

/// The referenced Approval was verified for a different kind, name,
/// namespace, or Kubernetes UID than the Restore being reconciled.
pub const TERMINAL_STATE_APPROVAL_SUBJECT_MISMATCH: &str = "ApprovalSubjectMismatch";

/// A Job already occupies the Restore's name but is not controlled by this
/// exact Restore UID with the complete garbage-collection owner contract.
pub const TERMINAL_STATE_JOB_NAME_CONFLICT: &str = "JobNameConflict";

/// `spec.archive.url` is not readable as an object-store location.
///
/// The rendered `backup.yaml` carries a TYPED `storage` block
/// (`logweir_core::engine::StorageUrl`, an internally tagged enum whose
/// variants have incompatible required fields), so a URL with no scheme or an
/// unsupported one cannot be rendered at all. Terminal for the same reason as
/// [`TERMINAL_STATE_REFERENT_NOT_FOUND`]: `spec` is immutable, so the next
/// pass reads the same unusable string.
pub const TERMINAL_STATE_ARCHIVE_URL_UNREADABLE: &str = "ArchiveUrlUnreadable";

/// The `Complete` condition type, for a run that exited **0**.
pub const CONDITION_COMPLETE: &str = "Complete";

/// The `Failed` condition type, for every other terminal outcome.
pub const CONDITION_FAILED: &str = "Failed";

/// The condition type a `Backup` carries while its Job exists and has not
/// finished.
pub const CONDITION_JOB_CREATED: &str = "JobCreated";

/// Whether a `Restore`'s admission passed — **its own condition type**, Task
/// 20.
///
/// WHY NOT A `Failed` CONDITION. An `Approval` that has not arrived yet is not
/// a failure: it is a HOLD, and interface **I19** requires the object to be
/// released the moment the approval is verified. Errata **E5c** is exactly
/// about the shape a `Failed=False` would take — a condition array is a MAP
/// KEYED BY `type`, so a `Failed=False` written at admission time and a
/// `Failed=True` written when the run later fails are one field with two
/// values, and the day any task gives the array the standard
/// `x-kubernetes-list-type: map` the API server rejects the patch. So the
/// admission gets its own type: `True` with reason [`REASON_ADMITTED`] on the
/// pass that creates the Job, `False` with reason
/// [`REASON_APPROVAL_NOT_VERIFIED`] while it waits, and a TERMINAL admission
/// refusal writes a `Failed` condition instead, because that one really does
/// end the object's life.
pub const CONDITION_ADMITTED: &str = "Admitted";

/// The `reason` on an [`CONDITION_ADMITTED`] condition that passed.
pub const REASON_ADMITTED: &str = "Admitted";

/// The `reason` while a `Restore` waits for its `Approval` — **interface
/// I19**, and it is a HOLD and not a verdict.
///
/// NOT IN [`TERMINAL_STATES`], AND THAT IS THE ASSERTION. It is the one
/// admission outcome that can change without anybody touching the object, so
/// it requeues at
/// [`crate::controllers::restore::ADMISSION_REQUEUE_SECS`] — thirty seconds —
/// which is what makes the restore wizard's minted-both-names-first ordering
/// workable. Its terminal sibling is
/// [`TERMINAL_STATE_APPROVAL_NOT_RECEIVED`].
pub const REASON_APPROVAL_NOT_VERIFIED: &str = "ApprovalNotVerified";

/// The verified inputs could not be materialized into the immutable public
/// bundle, so no Job was created and the controller will retry.
pub const REASON_APPROVAL_BUNDLE_MATERIALIZATION_FAILED: &str =
    "ApprovalBundleMaterializationFailed";

/// Whether the run's two evidence keys were recorded — **its own condition
/// type, and raised only at exit 0**.
///
/// REVIEW FINDING HIGH-2, plan errata **E5c**. This used to be a second
/// `Failed` condition with `status: "False"` and reason
/// [`REASON_EVIDENCE_KEYS_UNREADABLE`], appended whenever the two key lines
/// were absent — which is ALWAYS for exits 1, 3 and 4, because Global
/// Constraint 11 says those runs write no artifact. Measured live, every
/// refused `Backup` came back carrying
/// `Failed/True/GuardRefused` **and** `Failed/False/EvidenceKeysUnreadable`.
///
/// THAT IS A MALFORMED STATUS, NOT A COSMETIC ONE. A condition array is a MAP
/// KEYED BY `type` — the CRD's own item description says "a
/// `metav1.Condition`" — so a standard `FindStatusCondition`-style reader sees
/// whichever comes first, `kubectl wait --for=condition=Failed` matched the
/// `True` one only by array order, and the day any task gives the array the
/// standard `x-kubernetes-list-type: map` + `listMapKey: [type]` the API
/// server would REJECT every failed `Backup`'s status patch. The message was
/// also untrue: a guard-refused run produced no evidence BY CONTRACT, so
/// nothing was "unreadable".
///
/// So the fact gets its own type, and it is raised only where the contract
/// promised an artifact: `status: "True"` with reason
/// [`REASON_EVIDENCE_KEYS_RECORDED`] when both keys were read, `status:
/// "False"` with reason [`REASON_EVIDENCE_KEYS_UNREADABLE`] when they were
/// not, and **no evidence condition at all** at exits 1, 3 and 4.
pub const CONDITION_EVIDENCE_RECORDED: &str = "EvidenceRecorded";

/// The condition reason for a log body that did not carry both key lines.
///
/// THE POINT OF A NAMED REASON IS THAT NO KEY IS GUESSED. A receipt key is
/// derivable from a backup id — `logweir/backups/<id>/<run>.receipt.json` —
/// and a controller that derived one would write a status field pointing at an
/// object that may not exist, which a verifier (Task 24) would then report as
/// `Invalid` rather than as unread. Absent is the truthful value.
pub const REASON_EVIDENCE_KEYS_UNREADABLE: &str = "EvidenceKeysUnreadable";

/// The condition reason for an exit-0 run whose two key lines WERE read.
///
/// The positive arm exists rather than being left absent, because "the keys
/// are recorded" and "nobody has looked" are different answers and a consumer
/// reading `status.evidence` alone cannot tell them apart on a run whose
/// status patch is still in flight.
pub const REASON_EVIDENCE_KEYS_RECORDED: &str = "EvidenceKeysRecorded";

/// The condition that says whether the UI may render a GREEN badge for this
/// object — Task 24, interface **I21**.
///
/// **ONE TYPE, TWO RULES.** The `type` is `Verified` on both kinds, and the
/// rule behind it is not: a `Backup` is green on `Valid` + `exitCode == 0`,
/// a `Restore` on `Valid` + `outcome == pass`
/// (`crate::verification::{backup_badge, restore_badge}`). The condition's
/// `message` is the badge label, which is "verified by weirkeeper at
/// `<verifiedAt>` against key `<matchedKeyId>`" when green and the literal
/// word `unverified` otherwise — **never** `pass`, and never "verified in your
/// browser".
pub const CONDITION_VERIFIED: &str = "Verified";

/// [`CONDITION_VERIFIED`]'s reason when the badge is green.
pub const REASON_VERIFIED: &str = "Verified";

/// [`CONDITION_VERIFIED`]'s reason when the controller verified the evidence
/// and the answer was no. A claim about the DOCUMENT.
pub const REASON_VERIFICATION_INVALID: &str = "VerificationInvalid";

/// [`CONDITION_VERIFIED`]'s reason when no verification happened at all — no
/// evidence credential, an unreadable object, or a `TrustRoster` with no
/// signing key material. A claim about the CONTROLLER, and the reason
/// `NotAttempted` exists as a verdict distinct from `Invalid`.
pub const REASON_VERIFICATION_NOT_ATTEMPTED: &str = "VerificationNotAttempted";

/// [`CONDITION_VERIFIED`]'s reason when the signature verified and the SIGNER
/// is one this installation will not accept — PLAT-19.1, decision D3 §7.4.
///
/// A CLAIM ABOUT THE KEY, and the third thing that is not
/// [`REASON_VERIFICATION_INVALID`]: the document is exactly what it says it is,
/// and the key that made it is revoked, unknown to the resolved `TrustPolicy`,
/// declared for another usage, or was used outside the window it was trusted
/// in. The operator's next step is their trust policy, not their archive.
pub const REASON_VERIFICATION_UNTRUSTED: &str = "VerificationUntrusted";

/// [`CONDITION_VERIFIED`]'s reason when a `Backup`'s evidence verified and the
/// run did not exit 0. The `Backup` half of interface **I21** — there is no
/// `outcome` on that path to read instead.
pub const REASON_EXIT_CODE_NOT_ZERO: &str = "ExitCodeNotZero";

/// [`CONDITION_VERIFIED`]'s reason when a `Restore`'s evidence verified and
/// its `outcome` is not `pass`. The `Restore` half of interface **I21**.
pub const REASON_OUTCOME_NOT_PASS: &str = "OutcomeNotPass";

/// A `Backup` whose runner Job is NOT known to execute this object's frozen
/// execution inputs — PLAT-06.1. `True` is the problem being present, as with
/// [`CONDITION_FAILED`]; a Job created from frozen inputs raises no condition,
/// because `status.execution` is the positive record.
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
/// this controller never executes — PLAT-06.1. `True` while the annotation is
/// present on an object whose run this controller derives or refuses; absent
/// when there is no annotation. The message names the annotation's size and
/// digest and never its content.
pub const CONDITION_RUNNER_ARGV_ANNOTATION_IGNORED: &str = "RunnerArgvAnnotationIgnored";

/// [`CONDITION_RUNNER_ARGV_ANNOTATION_IGNORED`]'s reason for an annotation that
/// is a JSON array of strings. Whether it equals the derived argv is stated in
/// the message; it is not executed either way.
pub const REASON_RUNNER_ARGV_ANNOTATION_IGNORED: &str = "AnnotationIgnored";

/// [`CONDITION_RUNNER_ARGV_ANNOTATION_IGNORED`]'s reason for an annotation that
/// is not a JSON array of strings.
pub const REASON_RUNNER_ARGV_ANNOTATION_MALFORMED: &str = "AnnotationMalformed";

/// Every condition `reason` this crate writes that is NOT one of
/// [`TERMINAL_STATES`], in one list.
///
/// ONE LIST SO THE REGEX TEST CANNOT MISS ONE. A test that named the reasons
/// itself would be a test that goes stale the first time a reason is added;
/// `the_condition_reasons_are_valid_metav1_reasons` iterates THIS and
/// [`TERMINAL_STATES`], and `the_two_reason_vocabularies_never_overlap`
/// asserts no member of either is a [`wire_reason_for_exit`] value.
pub const CONDITION_REASONS: &[&str] = &[
    CONDITION_REASON_OK,
    CONDITION_REASON_OPERATIONAL,
    CONDITION_REASON_DRILL_NOT_PASS,
    CONDITION_REASON_GUARD_REFUSED,
    CONDITION_REASON_SIGNING_OR_LOCK,
    REASON_EVIDENCE_KEYS_UNREADABLE,
    REASON_EVIDENCE_KEYS_RECORDED,
    CONDITION_JOB_CREATED,
    REASON_ADMITTED,
    REASON_APPROVAL_NOT_VERIFIED,
    REASON_APPROVAL_BUNDLE_MATERIALIZATION_FAILED,
    REASON_VERIFIED,
    REASON_VERIFICATION_INVALID,
    REASON_VERIFICATION_NOT_ATTEMPTED,
    REASON_VERIFICATION_UNTRUSTED,
    REASON_EXIT_CODE_NOT_ZERO,
    REASON_OUTCOME_NOT_PASS,
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
    // D3 §2.2. The other four `RunnerReady=False` reasons are NOT here: they
    // are `TERMINAL_STATES` members, the regex test iterates both lists, and a
    // reason in both would be counted twice and asserted twice about nothing.
    REASON_RUNNER_STARTED,
    REASON_WAITING_FOR_POD,
];

// ===========================================================================
// D1 §3.4 — the cadence, selection and identity vocabulary
// ===========================================================================

/// The identity a scheduled `Backup` claims is not the one its own fields
/// compose (D1 §3.1 rules 1 and 3).
///
/// TERMINAL, AND NEVER RE-READ AS A MANUAL RUN. A manual run executes under
/// its own UID, so quietly accepting a misnamed scheduled object would let it
/// write a second archive of a window a scheduled run already owns.
pub const TERMINAL_STATE_SCHEDULED_IDENTITY_MISMATCH: &str = "ScheduledIdentityMismatch";

/// A scheduled `Backup` whose `spec.scheduleRef` names a schedule this
/// namespace does not have (D1 §3.1 rule 2).
///
/// **Deleting a schedule stops future work.** Checked only BEFORE the freeze:
/// once a run's inputs exist, nothing is re-checked, so deleting a schedule
/// never kills a run that is already executing.
pub const TERMINAL_STATE_SCHEDULE_NOT_FOUND: &str = "ScheduleNotFound";

/// `spec.scheduleRef.runPolicySha256` disagrees with the digest recomputed
/// from this object's own policy fields (D1 §3.1 rule 5).
pub const TERMINAL_STATE_RUN_POLICY_DIGEST_MISMATCH: &str = "RunPolicyDigestMismatch";

/// `spec.topics` and `spec.allUserTopics` do not form one of the two legal
/// selection shapes, or a name in either is not Kafka-legal.
pub const TERMINAL_STATE_INVALID_TOPIC_SELECTION: &str = "InvalidTopicSelection";

/// The discovery Job did not produce a usable result — unreachable broker,
/// timeout, authentication failure, a non-zero exit.
///
/// **RETRYABLE**, and the one member of [`crate::cadence::RETRYABLE_TERMINAL_STATES`]
/// this task adds: a broker that was down can be up in five minutes, and
/// nothing was decided.
pub const TERMINAL_STATE_DISCOVERY_FAILED: &str = "DiscoveryFailed";

/// Discovery could not prove it saw every topic and the policy says `Refuse`.
///
/// NOT RETRYABLE. Re-running would ask the same principal the same question
/// and get the same partial answer; what changes it is an ACL or an
/// administrator attestation.
pub const TERMINAL_STATE_DISCOVERY_INCOMPLETE: &str = "DiscoveryIncomplete";

/// The discovery output was malformed: a missing or duplicated summary, a
/// count or digest that does not match the lines, a name no broker could have
/// produced. NOT retryable — malformed output is a defect, not a blip.
pub const TERMINAL_STATE_DISCOVERY_RESULT_UNREADABLE: &str = "DiscoveryResultUnreadable";

/// Dynamic selection resolved to nothing: every user topic excluded, or an
/// empty cluster. **The runner is never started**, because a backup of no
/// topics is not a backup.
pub const TERMINAL_STATE_SELECTION_EMPTY: &str = "SelectionEmpty";

/// Dynamic selection resolved to more than the run may carry.
pub const TERMINAL_STATE_SELECTION_TOO_LARGE: &str = "SelectionTooLarge";

/// The source connection resolved to something different between discovery and
/// the freeze — a `KafkaCluster` edited or replaced mid-resolution.
pub const TERMINAL_STATE_SOURCE_CHANGED_DURING_RESOLUTION: &str = "SourceChangedDuringResolution";

/// `phase` while a dynamic run is resolving its topic list.
///
/// NONTERMINAL, and safe to add: `backup_is_terminal` already treats a phase
/// it does not recognise as active, so an older controller reading a
/// `Resolving` Backup does not mistake it for a finished one.
pub const PHASE_RESOLVING: &str = "Resolving";

/// The condition type carrying a dynamic run's topic resolution.
pub const CONDITION_TOPICS_RESOLVED: &str = "TopicsResolved";

/// `TopicsResolved=False` while the discovery Job is running.
pub const REASON_DISCOVERY_RUNNING: &str = "DiscoveryRunning";

/// `TopicsResolved=True` once the list is frozen.
pub const REASON_RESOLVED: &str = "Resolved";

/// The condition type carrying whether a schedule's run history is retained
/// independently of the schedule object (PLAT-05.2).
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
/// have. **Fail closed**: a zone nobody can resolve is not silently UTC.
pub const REASON_UNKNOWN_TIME_ZONE: &str = "UnknownTimeZone";
/// `Ready=False`: the topic selection is not one of the two legal shapes.
pub const REASON_INVALID_TOPIC_SELECTION: &str = "InvalidTopicSelection";
/// `Ready=False`: the run policy has a field-level problem.
pub const REASON_INVALID_RUN_POLICY: &str = "InvalidRunPolicy";
/// `Ready=False`: the object uses a field the installed CRD declares and this
/// controller does not understand — a CRD applied ahead of its controller.
pub const REASON_CRD_OUTDATED: &str = "CrdOutdated";

/// The condition TYPES a `Backup` can carry, in one list.
///
/// A condition array is a MAP KEYED BY `type` (the CRD's item description says
/// "a `metav1.Condition`"), so two conditions sharing a `type` is a malformed
/// status whatever their statuses say — review finding HIGH-2. This list is
/// what `no_two_conditions_share_a_type` measures the written status against.
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
    CONDITION_RUNNER_READY,
];

/// `phase` for a `Restore` whose admission has not passed yet — **interface
/// I19**, Task 20.
///
/// `Pending` IS NOT `Failed`, AND THE DIFFERENCE IS THE WHOLE OF I19. A
/// `Restore` created before its `Approval` is verified sits here with one
/// `Admitted=False` condition and is released the moment the approval
/// verifies. `Restore.status.phase`'s vocabulary is Task 20's
/// (`crds/restore.rs` says so), and this is the value that makes a dangling
/// `approvalRef` workable rather than fatal. `Backup` has no such state: its
/// Job is created on the first pass.
pub const PHASE_PENDING: &str = "Pending";

/// `phase` while the Job exists and has not finished.
pub const PHASE_RUNNING: &str = "Running";

/// `phase` for a run that exited **0**.
pub const PHASE_SUCCEEDED: &str = "Succeeded";

/// `phase` for every other terminal outcome, the crashed-Job case included.
pub const PHASE_FAILED: &str = "Failed";

// ===========================================================================
// THE STATUS-WRITE CONTRACT — ONE IMPLEMENTATION, SIX RECONCILERS
// ===========================================================================
//
// Plan erratum **E11(d)**, from Task 15c's review (findings H-1, H-2, M-1).
//
// # The bug these four functions close
//
// A reconciler's OWN status patch is what wakes it: a patch that changes any
// byte bumps `resourceVersion`, the primary-resource watch fires, and the next
// reconcile writes another changing byte. `owns` is not required and no child
// object is needed — two of the three reconcilers caught by this were watching
// nothing at all. Measured live on docker-desktop at `376a09e`, on objects
// nobody touched: `trust_roster` **12,107 reconciles in 91.2 s**, `approval`
// **7,114 in 90.4 s**, and `backup_schedule` writing a fresh
// `retentionReport.evaluatedAt` on every pass.
//
// The invariant, stated once: **a status patch is a pure function of the
// object and its children, never of the clock.** Three rules implement it, and
// all three live here rather than in any reconciler:
//
// 1. A condition's `lastTransitionTime` moves only when that condition's
//    `status` or `reason` moves — [`merge_condition`], the `metav1.Condition`
//    contract. `message` is deliberately NOT compared: it carries instants
//    (the next firing, the slot) that move by design, and comparing it would
//    put the bug straight back.
// 2. A "when computed" field — `retentionReport.evaluatedAt` — is written only
//    when the thing it timestamps changed, and kept otherwise. That
//    comparison is the report's own
//    (`crate::retention::RetentionReport::same_findings_as`), and it feeds
//    this module's rule 3.
// 3. **If the patch would not change the object, no patch is sent at all** —
//    [`status_unchanged`]. This is the backstop that makes rules 1 and 2
//    OBSERVABLE in a test (a route table with zero PATCHes) instead of merely
//    invisible on the wire, and it is what turns a redundant API write into no
//    API write.
//
// # Why the comparison is RFC 7386 and not `==`
//
// Every reconciler here patches with `Patch::Merge`, which is RFC 7386. A
// merge patch that omits a key means "leave it alone", and one that carries
// `null` means "delete it" — so "the computed status equals the current
// status" is not a comparison of two objects, it is the question *would
// applying this patch change anything*. [`apply_merge_patch`] is the API
// server's half of that question, written out so the answer is exact: a patch
// that omits five keys and repeats a sixth is correctly read as a no-op, and a
// patch that sets a key to `null` on an object that never had it is too.
// Arrays are REPLACED and never merged, which is why every condition array
// this crate writes is built from `Condition` and serialised by `serde` —
// a hand-built element that spells one optional field differently from the
// stored one would compare unequal forever and re-open the loop.

/// The object's current condition of this `type`, if it carries one.
///
/// The lookup half of [`merge_condition`], separate only because each kind has
/// its own status struct and therefore its own path to the vector; the
/// COMPARISON is in one place and this is not it.
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
/// THE ONE COMPARISON. Before this function there were four byte-identical
/// private copies of it (`backup.rs`, `restore.rs`, `kafka_cluster.rs`,
/// `backup_schedule.rs`) and two reconcilers with none at all
/// (`trust_roster.rs`, `approval.rs`) — which is exactly how a rule comes to
/// hold on four kinds and not on the other two. Every reconciler in this crate
/// calls this; none of them compares timestamps itself.
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

/// Apply an RFC 7386 JSON merge patch to `target`, exactly as the API server
/// would.
///
/// `null` deletes the key; an object merges recursively; anything else —
/// arrays included — replaces. A non-object `target` under an object patch
/// becomes an empty object first, which is the RFC's own rule and is what
/// makes a first write onto an absent status come out right.
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

/// Whether sending `next` would leave `current` byte-for-byte as it is — in
/// which case the reconciler sends nothing.
///
/// `current` is the object's status as it stands (`None` for an object with no
/// status yet); `next` is the WHOLE patch body the reconciler is about to
/// send, `{"status": {…}}`, so a call site passes the value it already built
/// and does no indexing of its own.
///
/// A body with no `status` key changes no status and is therefore unchanged;
/// that shape is not written anywhere in this crate and is answered rather
/// than asserted, because a `debug_assert` here would be a panic on a path
/// whose whole purpose is to avoid a write.
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

// ---------------------------------------------------------------------------
// THE FOURTH RULE OF THE STATUS-WRITE CONTRACT — D-SEAMS **S7**
// ---------------------------------------------------------------------------
//
// Every `/status` write carries `metadata.resourceVersion` as a PRECONDITION.
// The API server applies a `resourceVersion` carried in a merge-patch BODY as
// an update precondition and answers `409 Conflict` on a mismatch, which is
// how a merge PATCH gets a compare-and-set without the `update` verb —
// `charts/logweir/README.md:53` and the D3 W2 record both state the rule for
// every status write in this crate, and six of the seven kinds that have it
// spell it out for themselves.
//
// Defect STATUS-PATCH-NO-RV is the seventh: `controllers::{backup, restore,
// kafka_cluster}::patch_status_if_changed` — twenty-three call sites between
// them — sent an UNCONDITIONAL merge patch, so a pass computing from a stale
// watch-cache copy overwrote whatever a concurrent writer had stored. The
// `Backup` kind has four writers (this reconciler, `backup_selection`,
// `backup_schedule`'s reservation and `schedule_history`), which is exactly
// the shape the precondition exists for.
//
// [`patch_status_preconditioned`] is the one implementation, so the rule
// cannot hold at twenty-two sites and not at the twenty-third.
//
// # A 409 is not swallowed here
//
// It is returned as the API error it is and reaches the reconciler, whose
// `error_policy` requeues; the next pass reads the object the other writer
// stored and recomputes. A re-read-and-retry INSIDE this helper would be a
// second write of a status computed from inputs nobody re-observed, which is
// the defect with an extra round trip. A caller that needs the write to land
// within the pass opts in BY NAME through [`StatusVersion`]: it passes the
// version the previous write of the same pass left behind, which is what
// `Backup`'s freeze → resolve → running sequence and both terminal →
// verification sequences do.

/// The `metadata.resourceVersion` the NEXT `/status` write of this pass must
/// precondition on.
///
/// A pass that writes once takes it from the object the watch delivered and
/// discards the result. A pass that writes more than once CANNOT: after the
/// first PATCH returns, the object it was handed is stale by exactly one
/// version, and a second write preconditioned on the old one is refused
/// forever. So every write returns where the object now stands, and the
/// sequences that need it thread it forward.
///
/// Deliberately NOT `#[must_use]`: discarding it is correct and common (one
/// write, then a `return`), and a lint on every such site would train the
/// reader to ignore it at the two sites where it matters.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StatusVersion(Option<String>);

impl StatusVersion {
    /// Where the object a pass was handed stands — the precondition for that
    /// pass's FIRST write.
    ///
    /// An empty string is treated as absent: that is what a hand-built fixture
    /// carries, and a precondition of `""` is not one.
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

/// Write `/status` under seam **S7**'s precondition — unless the patch would
/// change nothing.
///
/// `at` is where the object stands ([`StatusVersion`]); `current` is the
/// status this pass believes is stored, for [`status_unchanged`]'s decision.
/// The returned [`StatusVersion`] is where the object stands AFTER this call:
/// the response's own version when a patch was sent, and `at` unchanged when
/// none was — a skipped write moves nothing, so the caller's precondition
/// still holds.
///
/// # Errors
///
/// * [`kube::Error::Discovery`], shaped as the API server's own "no
///   resourceVersion" answer, for an object that carries none. Unreachable for
///   anything that came from a watch or a `get`; named rather than written
///   without its precondition, because writing it unconditionally is the
///   defect this function exists to close.
/// * The API error for anything else, `409 Conflict` INCLUDED — see this
///   section's header for why it is not retried here.
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
