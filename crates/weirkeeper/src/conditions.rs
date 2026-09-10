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
    "GuardRefusedUnknownReason",
    "NameTooLong",
    "ReferentNotFound",
    "PlanConfigMapConflict",
    "ArchiveUrlUnreadable",
    "PlanHashMismatch",
    "ClusterNotReachable",
];

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
];

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
