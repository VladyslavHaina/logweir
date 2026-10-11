# Reference: exit codes, runner output and status reasons

The lookup tables behind [the operator guide](kubernetes.md): what every exit
code, runner output line and status reason means. The guide says what to do;
this page says what a word on an object means. Signed document fields are in
[formats/](formats/), and what is promised to stay stable is in
[stability.md](stability.md).

## Exit codes

One contract for every runner that signs or uploads (`logweir backup run`,
`logweir restore run` and its `drill run` alias):

| Code | Meaning |
|---|---|
| 0 | Pass. |
| 1 | Operational failure; no artifact was written. Retryable as a new run. |
| 2 | A measured result that did not pass; a scorecard was written and signed. |
| 3 | A guard refused the run before anything ran. |
| 4 | Signing or the lock proof failed; nothing was uploaded. |

Exit 3 has one documented exception: on a target broker whose
`log.message.timestamp.type` is `LogAppendTime`, phase 0 creates and deletes one
probe topic inside the scratch prefix to learn whether the broker honours a
per-topic `CreateTime` override. A phase-5 / phase-6 `restore.yaml` divergence is
exit 1, not 3.

`logweir check run` returns 0 (a result was relayed, whatever the rows say), 1
(an operational failure before a result existed) or 3 (a contract refusal,
`refusal-reason=CheckContractMismatch`, no frame printed). `logweir-retention`
returns 0 (every planned point completed, or a dry run), 1 (a point did not
complete) or 3 (the plan was refused before anything ran; nothing deleted).
Neither returns 2 or 4.

### Backup: exit code to status

| Exit | `status.phase` | `status.exitReason` | Condition | Meaning |
|---|---|---|---|---|
| 0 | `Succeeded` | `ok` | `Complete=True`, `Ok` | Archive captured, receipt signed |
| 0 | `Succeeded` | `ok` | `Complete=True`, `EmptyCapture` | Every selected partition was empty; no `windowCovered`, not a recovery point |
| 1 | `Failed` | `operational`, or `ExecutionAlreadyClaimed` off the runner's `failure-reason=` line | `Failed=True`, `Operational` | Could not run or continue; no artifact. `ExecutionAlreadyClaimed`: an earlier run of this execution reached the engine, or its set already exists |
| 2 | `Failed` | `drill-not-pass` | `Failed=True`, `DrillNotPass` | Not produced by `backup run`; one vocabulary across both paths |
| 3 | `Failed` | the state on `refusal-reason=`, or `GuardRefusedUnknownReason` | `Failed=True`, `GuardRefused` | A guard refused; the message ends with the runner's reason |
| 4 | `Failed` | `signing-or-lock`, `OrphanedScorecard`, or `ExecutionClaimUnproven` | `Failed=True`, `SigningOrLock` | Nothing uploaded. `ExecutionClaimUnproven`: the store refused the claim, does not enforce conditional create, or the archive could not be read for a reason no retry changes |
| absent | `Failed` | `operational` | `Failed=True`, `DisruptedMidDrill` / `PodUnschedulable` / `NoExitCode` / `VolumeMountFailed` / `CredentialReferenceMissing` / `RunnerImageUnavailable` / `PodCreationForbidden` / `PodOwnershipContested` | The runner reported no terminated state |
| absent | `Failed` | `operational` | `Failed=True`, a refusal before any Job: `NameTooLong`, `ExecutionSpecInvalid`, `ScheduledIdentityMismatch`, `ScheduleNotFound`, `RunPolicyDigestMismatch`, `InvalidTopicSelection`, `GuardRefused` (a glob), `ReferentNotFound`, `CredentialNotRenderable`, `ConnectionConfigInvalid`, `ConnectionReferenceInvalid`, `ConnectionFieldUnsupported`, `PlainWithoutTls`, `ArchiveUrlUnreadable`, `PlanConfigMapConflict`, `JobNameConflict`, `SelectionTooLarge`, and the dynamic-selection reasons | Nothing was created |

`status.exitReason` carries the five lowercase wire strings (`ok`,
`operational`, `drill-not-pass`, `guard-refused`, `signing-or-lock`) or a more
specific CamelCase terminal state. A condition `reason` is always CamelCase.
The two vocabularies never overlap.

Other `Backup` conditions: `EvidenceRecorded` (`EvidenceKeysRecorded` /
`EvidenceKeysUnreadable`, exit 0 only), `RunnerReady`, `TopicsResolved`
(`DiscoveryRunning`, `Resolved`, or the dynamic-selection failure),
`Verified` (see [verdicts](#verification-verdicts)), `RunnerArgvAnnotationIgnored`
(`AnnotationIgnored`, `AnnotationMalformed`) and `ExecutionInputsUnverified`
(`LegacyExecution`, `JobInputsMismatch`).

### Restore: reasons

A `Restore` uses the same exit table, with `exitReason` also carrying
`TargetTopicAppeared` and `CreatedTopicsLeft` off the runner's `failure-reason=`
line (exit 1). `status.reason` (the `REASON` column) is always the current
condition's reason.

| Reason | When |
|---|---|
| `NameTooLong` | The name is over 63 characters |
| `ExecutionSpecInvalid` | `spec.runnerResources` out of bounds; a coverage or original-name declaration the plan does not match |
| `PlanUnparseable` | `spec.planBytes` is not a restore plan |
| `StandingAuthorizationRefused` | `spec.authorization` names a standing authorization (withdrawn in v1) |
| `ApprovalNotReceived` | `spec.approvalRef` is empty |
| `ApprovalNotVerified` | Held: the `Approval` does not exist or is not `Verified=True` yet |
| `PlanHashMismatch` | The plan's hash is not the approval's |
| `ApprovalSubjectMismatch` | An ordinary approval for an original-name restore, or the reverse |
| `ApprovalPolicyMismatch`, `AuthorizationExpired` | A v2 approval under another policy, digest or mode, or past its window |
| `ClusterNotReachable` | The target `KafkaCluster` is not `reachable: true` |
| `PlanDestinationMismatch`, `PlanEvidenceDestinationMismatch` | The plan's locations are not the destinations' |
| `DestinationRoleNotConfigured`, `ExecutionContextConflict` | A grant does not resolve, or two workload identities in one pod |
| `ConnectionPlanMismatch` | The plan's `target` is not the `clusterRef` connection |
| `ApprovalBundleConflict` | The approval bundle was made by another controller; delete and re-create |
| `ApprovalBundleMaterializationFailed` | Held: a bundle key cannot sign anything new under its usage |
| `WindowNotCovered` | Exit 2: the archive did not cover the requested window |
| `TargetTopicConfigRefused`, `CredentialNotRenderable`, `PointInTimeByProducerTime`, `TransactionMarkersInArchive`, `PlainWithoutTls`, `CredentialBindingMismatch` | Exit 3 runner refusals, by `refusal-reason=` |
| `PointBindingMismatch`, `PointUntrusted`, `PointBindingSetMismatch` | Exit 3, read as `GuardRefused`; the name opens the message. `logweir_core::guard::TERMINAL_STATES` is a closed list, so these (and `AuthorizationInvalid`, `AuthorizationExpired`, `RehearsalScopeViolation`) are carried as the message's first token, not as declared states. A bound point whose receipt or manifest is missing or unreadable is exit 1 |

Plus the no-exit-code states of the `Backup` table, `PodOwnershipContested`,
`JobNameConflict` and `PlanConfigMapConflict`. `Admitted` is its own condition
(`ApprovalNotVerified` while held).

## Runner output lines

The machine contract is a set of `key=value` lines on stdout. A pod log merges
stdout and stderr in no promised order, so every reader scans a bounded tail
and matches **by key name, never by position**; the controller reads the final
sixteen non-empty lines (`KEY_SCAN_TAIL_LINES = 16`). Optional lines go before
the mandatory ones. Stderr carries JSON tracing and nothing a controller parses.

### `logweir backup run`

| Line | When | Read into |
|---|---|---|
| `progress-contract=2` then `progress-phase=-1:<admit\|engine\|readback\|sign\|upload>` | While running; optional, each at most 96 bytes | `status.progress.runnerPhase` |
| `catalog-key=<key>` | Exit 0, when the catalog record was written | — |
| `receipt-sha256=sha256:<64 hex>` | Exit 0 | `status.evidence.receiptSha256` |
| `receipt-key=<key>` then `sidecar-key=<key>` | Exit 0, the final two lines (interface I7) | `status.evidence.receiptKey`, `sidecarKey` |
| `failure-reason=ExecutionAlreadyClaimed` / `ExecutionClaimUnproven` | Exit 1 / exit 4, the final line | `status.exitReason` |
| `refusal-detail={…}` then `refusal-reason=<State>` | Exit 3, the last two lines | The `Failed` message; `status.exitReason` |

### `logweir restore run` / `drill run`

| Line | When | Read into |
|---|---|---|
| `progress-contract=<n>` then `progress-phase=<0..9>:<name>` | While running; optional | `status.progress.runnerPhase` |
| `topic-preflight=<json>` | Phase 0's observation of the target: `timestampType`, `retentionMs`, `timestampBound` | `status.topicPreflight` |
| `teardown-failed=[{"topic":…,"error":…}]` | When a teardown left a topic; at most 256 entries | `status.teardown.failed` |
| `teardown-key=logweir/drills/<run_id>.teardown.json` | When phase 9's signed teardown attestation was written | `status.teardown` |
| `scorecard-key=logweir/drills/<run_id>.json` | Exit 0 and exit 2 | `status.evidence` |
| `sidecar-key=logweir/drills/<run_id>.sig` | Exit 0 and exit 2 | `status.evidence` |
| `offset-report-key=logweir/drills/<run_id>.offsets.json` | Exit 0 and 2, only when the engine wrote an offset report | `status.evidence` |
| `target-topics-appeared={…}` then `failure-reason=TargetTopicAppeared` / `CreatedTopicsLeft` | Exit 1 from a stopped creation step, the last two lines; at most 76 KB | `status.targetTopicsAppeared`, `status.exitReason` |
| `refusal-detail={…}` then `refusal-reason=<State>` | Exit 3, the last two lines | The `Failed` message; `status.exitReason` |

The evidence keys (interface I8) are the final lines of a run that signed a
scorecard, in that order, with nothing after them; `teardown-key=` comes before
them, as `catalog-key=` comes before interface I7's two. A run that signed
nothing prints no key line. Exit 2 prints the keys of its signed failure: read
the exit code for success, the keys only to find the document
([the amendment](stability.md#interface-i8-is-amended-exit-2-prints-the-signed-failures-keys-too)).

- **`offset-report-key=` is conditional.** The engine writes its offset report
  only from a completed restore, and its own write failure is a warning, so a
  finished restore can leave nothing at the path: the run then records no
  offset-report key or digest in the scorecard, uploads nothing, logs a
  warning and still exits 0 with two key lines. A scorecard without the
  `evidence.offset_report_*` pair (present or absent together) is well-formed.
  Scan for a key by NAME and treat the third as optional.
- **`teardown-key=` is conditional in the same way**: printed when, and only
  when, phase 9's create-only put of the signed teardown attestation
  succeeded, at exit 0 or exit 2. It rides on the phase-9 record, pushed after
  phase 8 signed, so nothing that delivers it reaches the signed bytes.
- **`teardown-failed=`** is printed once, by phase 9 or by the teardown of a run
  that failed after its creation step, whenever a teardown left a topic:
  Kafka-legal names only, each `error` at most 256 characters on one line,
  before the summary line. The controller copies the last one in its tail, held
  to the Restore's mapped target names; an older controller ignores it.
- **The progress channel is optional and bounded.** `progress-contract=<n>` is
  printed once, before the first `progress-phase=` line: the execution contract
  version the controller stamped (a v1 invocation prints `1`), else the version
  the binary implements (`logweir_core::execution_contract::VERSION`, `"2"`), as
  for `backup run`, whose Job carries no contract. Lines come from a closed
  vocabulary (the ten restore phase names, the five backup step names), and
  anything else is silence, never a sanitised half-truth. A runner that
  predates the channel prints nothing, which is not an error.
- **`failure-reason=` states are closed per kind**
  (`logweir_core::guard::RESTORE_FAILURE_REASONS`: `TargetTopicAppeared` and
  `CreatedTopicsLeft`, exit 1; `BACKUP_FAILURE_REASONS`:
  `ExecutionAlreadyClaimed`, exit 1, and `ExecutionClaimUnproven`, exit 4), so
  one kind's log never lifts the other kind's state, and only beside its own
  exit code. The controller lifts `target-topics-appeared=` and
  `failure-reason=` only from the log's last two non-empty lines, in that
  order, reading the last 256 lines; it refuses a value over 76 KB before
  parsing it and keeps only the names (and counts) its own plan maps. An older
  controller reads `appeared` and `left` only; one older still reports a plain
  exit 1. The place rule cannot tell the runner's own last lines from text an
  OLDER runner image was made to print last, so with such an image check the
  names against the cluster before acting.
- **Every error text the runner prints is on one line**
  (`logweir::exit::one_line` escapes every line break), so no plan- or
  broker-chosen string can start a line a controller reads by key. The Kafka
  client's own stderr logging is not escaped.

### Refusal detail

At exit 3 both runners print `refusal-detail=` immediately before
`refusal-reason=`, as the last two lines, in one write. It is one JSON object of
at most 2048 bytes:

| Member | What it is |
|---|---|
| `token` | The `--line-token` value (32 to 128 lower-case hex digits) when the run was given one; written first. The controller reads only the last line carrying its own Job's token |
| `code` | A member of the run kind's closed set (below), or `GuardRefused` |
| `message` | The refusal's sentence, cleaned: printable ASCII and `§ – — … →`; whitespace and controls become one space, other runs one `U+FFFD`; URLs lose query and userinfo; credential shapes read `[redacted]`; at most 760 bytes, a cut one ends `…` |

The state on the following `refusal-reason=` line is the detail's code (when
the code is one of the terminal states and the sentence named it with `: `) or
`GuardRefused`, and must agree with it. The `code` is the name the refusal's
sentence opens with (`<Code>: ` or `<Code>. `) when that name is in the kind's
set, else `GuardRefused`: ASCII letters and digits, a letter first, at most 64
bytes. A run with nothing to say prints `the refusal carried no sentence`. At
2048 bytes, CRI's 16 KiB line split never divides the line.

**The line token.** `--line-token <hex>` is a flag of `restore run`, `drill
run` and `backup run`: 32 to 128 lower-case hex digits; a malformed value or
the flag given twice is a usage error (exit 1). `weirkeeper` writes 40 digits
(160 bits) from the operating system's random source, fresh for each Job, as
the runner container's last two arguments. It is not a credential, but it is
treated as one in output: it appears in the pod template and the runner's one
line and nowhere else. A reader honours only the LAST `refusal-detail=` line
whose `token` equals the Job's (compared in constant time) and ignores every
other; a Job with no token has no line read. The line it takes must be at most
2048 bytes, carry a code of the kind's set and agree with `refusal-reason=`,
and its sentence is cleaned again; a line that carries the token and fails one
of those reads as no readable reason, and no other line is used in its place.
`logweir_core::refusal_detail` implements both sides. A code added to a set is
additive: a reader that predates it shows that the runner gave no readable
reason. `logweir check run` and `logweir-retention` print no `refusal-detail=`
and take no token.

| Run | Codes |
|---|---|
| `restore run` | `GuardRefused`, `CredentialNotRenderable`, `TargetTopicConfigRefused`, `PointInTimeByProducerTime`, `PlainWithoutTls`, `CredentialBindingMismatch`, `StorageRegionInvalid`, `AuthorizationInvalid`, `AuthorizationExpired`, `PointBindingMismatch`, `PointBindingSetMismatch`, `PointUntrusted`, `RehearsalScopeViolation`, `TransactionMarkersInArchive` |
| `backup run` | `GuardRefused`, `CredentialNotRenderable`, `PlainWithoutTls`, `CredentialBindingMismatch`, `StorageRegionInvalid`, `ConsumerGroupSelectionTooLarge`, `ConsumerGroupIdInvalid`, `ConsumerGroupSelectedTwice`, `WorkloadIdentityNotInjected` |

When the controller shows no reason, the message ends with nothing after
`…exitCode` (no line carries this Job's token), with `the runner gave no
readable reason`, with `the pod is gone`, with `the pod log read answered HTTP
403` (or another status), or with `the last 32 lines of the pod log are over the
512 KiB this controller reads`.

### `logweir check run`

```
argv: check run --plan /check/check-plan.json --check-contract-version 1
env:  LOGWEIR_CHECK_CONTRACT_VERSION=1
      LOGWEIR_CHECK_PLAN_SHA256=sha256:<64 lowercase hex>
      LOGWEIR_CHECK_SUBJECT_UID=<uid>
      RUST_LOG=warn        TMPDIR=/work
```

The **one** check runner (decision D2 §4.2): six plan kinds share it,
`topicInventory`, `operationReadiness`, `restorePreflight`,
`destinationAccess`, `evidenceFetch` and `catalogSync`, and only the controller
writes the invocation. The plan's SHA-256 and subject UID are checked, and the
plan parsed strictly (`deny_unknown_fields`), before any client exists, so a
plan swapped under a running Job, or written by a newer controller with a field
this runner does not know, is refused. The version handshake is in argv **and**
the environment. A `catalogSync` may ask for up to 1800 s (the `RecoveryCatalog`
controller asks for 900), the others 600 s. Stdout carries frames only:
`logweir-check-topic=`, `logweir-check-part=` and one final
`logweir-check-end=` declaring each stream's part count and SHA-256 and the
topic-line count and digest, each line at most 4,096 bytes. A relay that does
not verify is `ResultUnreadable`, and its content is never published. A runner
without the `check` subcommand exits 1 with a usage error, read as
`RunnerContractUnsupported`. **Do not read a bare exit 0 as "everything
passed"**: read the relayed result.

What a check may do:

- It **never invokes the engine** (`scripts/check-no-oso.sh`), and **never
  creates, alters or deletes a topic**: the restore preflight's collision
  answer is targeted metadata plus a `CreateTopics` with `validate_only =
  true`.
- It **never writes**, except the optional create-only readiness marker
  `logweir/readiness/<destinationUid>.json`. "Already exists" counts as
  write-authorised (S3 and MinIO authorise a `PUT` before the `If-None-Match`
  precondition) and is reported `MarkerAlreadyPresent`, not `MarkerWritten`.
  The marker is written as the destination's `evidenceWrite` grant, from
  `LOGWEIR_EVIDENCE_AWS_*` or the injected identity only; a missing projected
  variable is `CredentialSecretKeyMissing`, never a fall-back. The evidence-read
  probe follows the same rule (`LOGWEIR_EVIDENCE_READ_AWS_*`; a
  `controllerIdentity` or `notConfigured` grant is `unknown`,
  `EvidenceReadNotConfigured`, with no request). A plan lists the `SecretKeys`
  grants whose binding it compares as `grantBindings: [{role, secretName}]`
  (FX-20c), absent when empty; each arrives as
  `LOGWEIR_<ROLE>_GRANT_BINDING[_EXPECTED]` with no credential beside it. An
  older runner refuses a plan carrying `evidenceWrite` or `grantBindings`
  (exit 3).
- An `evidenceFetch` object may name only `evidence.payload` or
  `evidence.sidecar`, and no two objects the same one (a named refusal, exit 3).
  `catalogSync` writes nothing at all, reports a signature verdict and a signer
  key id but never a trust decision, and relays **no body** when its walk could
  not start.
- It **never prints a credential**: every message passes
  `logweir_core::check_contract::redact` and a 512-character cap, and no broker
  or object-store error string is interpolated; a key path gets the shape rules
  over the whole string and the long-run rule per `/` segment. Exactly one field
  is written verbatim, an `evidenceFetch` result's `key`.
- It **never dials TLS without SASL**: `auth.mode: plaintext` with `tls: true`
  is refused, as `logweir cluster-probe` and the controller refuse it.
- **Every network call is bounded** by the plan's `timeoutSeconds`.
- **An empty inventory and a failed one differ**: a `topicInventory` against a
  cluster showing no topic declares `topicLines: {"count": 0, …}`; one whose
  broker never answered declares no `topicLines` block and carries a
  `connection.authenticated` row with the classified code. A Kafka listing never
  proves full visibility: the broker omits topics the principal cannot
  `DESCRIBE`.

A `catalogSync` body, inside the `details` stream:

```
catalog-format=1                                     # required; a higher major is refused
catalog-page=<i>/<n> count=<c> sha256=<64 hex>       # 1..=n in order, n <= 8
catalog-entry=<compact json>                         # exactly <c> per page
catalog-counts=<json>   catalog-cursor=<json>   catalog-signers=<json>
```

At most 5 MB of `details`, `viewLimit` entries, 64 signer rows and 16 locations
per point; a summary line that arrives twice is an error.

### `logweir-retention`

```
command: logweir-retention
argv:    run --plan /retention/plan.json --retention-contract-version 1 [--dry-run]
env:     LOGWEIR_RETENTION_PLAN_SHA256, _POLICY_UID, _POLICY_GENERATION,
         _SCOPE_PREFIX, _RUN_ID, _APPROVER (absent: unattended),
         _MAX_DELETIONS, _MAX_OBJECTS, _LOCATION
         AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY                  (the delete grant)
         LOGWEIR_EVIDENCE_AWS_ACCESS_KEY_ID / …_SECRET_ACCESS_KEY   (the record grant)
```

Every variable but the approver is required (exit 3 by name otherwise; an
unreadable ceiling is `CapUnreadable`), and the scope in the environment must
equal the plan's. A dry run needs no credential.

```
retention-plan=sha256:… points=3 objects=19
retention-point=lwp1-… state=Deleted objects=7
retention-point=lwp1-… state=Orphaned objects=3 code=AccessDenied
retention-record=logweir/retention/<policyUid>/<runId>.json sha256=sha256:…
retention-result=deleted=2 failed=1 objects=10
```

The contract version is checked before the plan is read; the scope arrives
twice (environment and plan) and a disagreement is a refusal. Exit 3 means
**zero objects deleted**: a digest that is not the approved one, an unknown
field, another media type, policy or generation, a widened scope, a key outside
`<scope>/<backupId>/` or under `logweir/`, a plan over a ceiling, or no record
sink. Per point it writes the intent tombstone, then deletes the **manifest**
(so an interrupted run leaves a set the catalog reports `Missing`, never a
plausible `Partial`), then the segments, then writes the completion tombstone.

`retention-refusal=<code>` names the one refusal the controller reads
(`CredentialBindingMismatch`). A key is tried three times, waiting 1 s and 4 s,
only for a 5xx or a timeout; `AccessDenied`, `Locked` and `PreconditionFailed`
are answered once, and `NotFound` is success. Per point the state is `Deleted`,
`Kept` or `Orphaned`, with a closed code such as `AccessDenied`, `Locked`,
`VersionedBucket`, `VersionProbeRefused`, `VersionCheckRefused:…`,
`TombstoneRefused`, `ListRefused:AccessDenied` or `DryRun`. The record is
create-only and **unsigned in this build** ([stability.md](stability.md)).

### Logs

Every terminal path of `drill run` logs `drill finished` with
`fields.exit_code` and `fields.meaning`, and every Logweir event carries
`fields.run_id` or `span.run_id`.

## Status reasons by kind

### Verification verdicts

`status.evidence.verification.result` on a `Backup` or `Restore`: `Valid`,
`Invalid`, `NotAttempted`, `Untrusted` or `Pending`. An `Untrusted` `detail`
names `UntrustedSigner`, `KeyUsageMismatch`, `SignedOutsideValidity`, `Revoked` or
`RecordedBeforeRevocation`. `trust.basis` is `Current`, `Historical`,
`RecordedBeforeRevocation`, `Unverified` or `None`; `trust.keyState` is
`Active`, `Retired`, `Expired`, `Revoked` or `Unknown`; `trust.signingTimeRead`
is `absent` or `overCap`. The `Verified` condition's reason is `Verified`,
`VerificationInvalid`, `VerificationNotAttempted`, `VerificationUntrusted`,
`ExitCodeNotZero`, `OutcomeNotPass` or `CompleteNotCovered`.
`status.evidence.observation.mode` is `ControllerIdentity`, `ArchiveHandle` or
an evidence-fetch Job's.

### `BackupSchedule`

`Ready` and `status.lastSlot.disposition`, evaluated top to bottom; the first
matching row decides. "Blocked" means `Forbid` with any nonterminal
schedule-created run, or `Allow` with ten.

| # | Observed state | Creates | `Ready` reason / disposition |
|---|---|---|---|
| 1 | Accepted reservation, child absent, policy valid | The reserved name | `Scheduled` / `CaughtUp` / `RetryScheduled` |
| 2 | Accepted reservation, child absent, policy invalid | Nothing; the reservation is released | `InvalidRunPolicy` / `Released` |
| 3 | `suspend: true` | Nothing; `nextRuns` empty | `Suspended` (False) |
| 4 | Cron, zone, selection or run policy invalid | Nothing; running work continues | `UnparseableSchedule` / `UnknownTimeZone` / `InvalidTopicSelection` / `InvalidRunPolicy` (False) |
| 5 | No due slot inside the walk bound | Nothing | `NoDueSlot` (False) |
| 5a | The latest due slot is before the schedule's `creationTimestamp` | Nothing; not counted | `Scheduled` (True) |
| 6 | The slot's name is held by an object that is not this schedule's scheduled run | Nothing | `SlotNameUnavailable` / `NameUnavailable` |
| 7 | Some attempt of S succeeded | Nothing | `Scheduled` / `Admitted` |
| 8 | The highest attempt of S is nonterminal | Nothing | `Scheduled` |
| 9 | The highest attempt failed, not retryable | Nothing | `RunFailed` / `Failed` |
| 10 | Failed retryably, attempt ≥ `maxRetries` | Nothing | `RetryExhausted` / `Exhausted` (`RunFailed` without `spec.retry`) |
| 11 | Failed retryably, the delay has not elapsed | Nothing | `RetryPending` |
| 12 | Failed retryably, delay elapsed, blocked | Nothing | `RetryBlocked` |
| 13 | Failed retryably, delay elapsed, not blocked | `…-r<k+1>`, kind `Retry` | `RetryScheduled` / `Retried` |
| 14 | No attempt, `S ≤ status.lastFireTime` (history pruned) | Nothing | `Scheduled` (`AlreadyFired`) |
| 15 | No attempt, the name would not fit | Nothing | `NameTooLong` (False) |
| 16 | No attempt, inside `startingDeadlineSeconds`, blocked | Nothing | `ConcurrencyBlocked` / `Blocked` (`ActiveRunLimit` under `Allow`) |
| 17 | No attempt, inside the deadline, not blocked | `name(S,0)`, kind `Scheduled` | `Scheduled` / `Admitted` |
| 18 | No attempt, past the deadline, `catchUpPolicy: None` | Nothing; counted in `missedSlots` | `SlotMissed` / `Missed` |
| 19 | No attempt, past the deadline, `Latest`, S before `status.policy.effectiveSince` | Nothing | `SlotMissed`, missed reason `BeforeRevision` |
| 20 | No attempt, past the deadline, `Latest`, blocked | Nothing | `CatchUpBlocked` / `Blocked` |
| 21 | No attempt, past the deadline, `Latest`, not blocked | `name(S,0)`, kind `CatchUp` | `CaughtUp` / `CaughtUp` |
| 22 | A newer slot comes due while 11, 12, 16 or 20 wait | Per the new slot | Per the new slot; the old one is counted once |

Other conditions: `HistoryRetained` (`Retained`, `HistoryLarge`,
`LegacyOwnerReferencesRemain`, `ActiveLegacyRunsOwned`, `MigrationBlocked`, with
`status.history.migrationBlocked[].reason` `ApiForbidden`, `ApiInvalid` or
`NoScheduleReference`) and `RetentionReportWithdrawn=True`, `UseRetentionPolicy`.
`status.nextRuns` markers: `RepeatedLocalTimeFirst`, `RepeatedLocalTimeSecond`,
`NonexistentLocalTimeShifted`.

### `Approval`

`Verified`'s reason, on a refusal:

| Reason | Meaning |
|---|---|
| `PayloadTypeMismatch` | The sidecar's payload type is not the approval's (checked before any key) |
| `SignatureInvalid` | A trusted key does not parse, or the signature does not verify under the matched key |
| `KeyIdNotInRoster` | No signature is by a trusted key, or a declared `keyId` is not its key's SHA-256 |
| `KeyIdExpired`, `KeyRetired`, `KeyRevoked`, `KeyNotYetValid` | The matched key may not authorise anything now |
| `TrustPolicyConflict` | Two policies claim the namespace |
| `RosterNotFound` | No trust at all: `no cluster-scoped TrustRoster named 'default'; see docs/kubernetes.md install step 1` |
| `PlanHashMismatch` | The signed `plan_hash`, or `spec.planHash`, is not the referent's recomputed hash |
| `SubjectKindMismatch` | The signed `subject_kind` is not the referent's kind |
| `ReferentNotFound`, `ReferentHasNoPlanBytes`, `ReferentUidChanged` | The `spec.subjectRef` object is missing, is a kind with no plan (`Backup`), or was re-created |
| `StandingAuthorizationRefused` | It names a `RehearsalSchedule` (withdrawn in v1) |
| `AuthorizationDocumentInvalid` | A v2 document that does not parse, lacks a required `ticket`, or carries a field its `formatVersion` does not define |
| `AuthorizationSubjectMismatch` | The v2 subject is not this `Restore` and UID |
| `ApprovalPolicyMismatch` | The document's policy, digest or mode is not the namespace's binding; or a v1 document in a bound namespace, or v2 in an unbound one |
| `AuthorizationWindowInvalid`, `AuthorizationExpired` | The window is longer than `maxAgeSeconds`, issued more than 60 s ahead, or over |
| `GovernedApprovalRequired` | Governed and only the console's signature so far: the pending state |
| `SelfApprovalRefused` | The approver is the requester, or its `principal.id` is not `<issuer>#<subject>` |
| `RecordedBeforeRevocation` | A consumed record whose key was revoked for compromise after admission |

The v1 checks run in this order: payload type; every key parses; each `keyId`
is its key's SHA-256; a signature's key is trusted; it verifies; the key is in
its window; the signed `plan_hash`; `subject_kind`; `spec.planHash`. A consumed
record carries `Consumed=True`, `RestoreAdmitted`.

### `TrustPolicy` and `TrustRoster`

`Loaded` (`Loaded`, `UnparseableKey`, `KeyIdMismatch`, `AlgorithmMismatch`),
`Bound` (`Bound`, `DefaultPolicy`, `NotBound`), `ExpiringSoon` (`ExpiringSoon`
within 30 days of an `Active` key's `notAfter`, `NotExpiring`), `CompromiseGuard`
(`CompromiseRecorded`, `CompromiseInherited`, `DeletionBlocked`,
`NoCompromiseRecorded`). `status.conflicts` names contested namespaces (`*` for
two defaults). A `TrustRoster` reports `Loaded` (`Loaded`, `UnparseableKey`),
`status.expiredKeyIds` and `Superseded` (`SupersededByTrustPolicy`,
`RosterStillConsulted`).

### `BackupDestination`

`status.reason` (the `VALID` column), in the order checked:

| Reason | What was wrong |
|---|---|
| `Valid` | Every check passed |
| `DestinationNotValid` | The stored object breaks a location rule (re-checked here for objects an older CRD admitted); the message names the field |
| `AddressingUnsupportedByEngine` | `storage.addressing: VirtualHosted` with a `storage.endpoint` |
| `CaBundleNotFound` | `transport.caBundle` names a `ConfigMap` this namespace does not have |
| `CaBundleKeyMissing` | The `ConfigMap` has no such key |
| `CaBundleTooLarge` | Over 64 KiB |
| `CaBundleInvalid` | No parseable `-----BEGIN CERTIFICATE-----` block |

A run or check over a destination can also meet `DestinationRoleNotConfigured`
(a grant spelled `<namespace>/<name>`, or unresolvable),
`ExecutionContextConflict`, `CaBundleUnsupportedByEngine`,
`ControllerIdentityNotAllowlisted` and `CredentialBindingMismatch`.

### `KafkaCluster`

`Reachable`: `Reachable` (True); `ProbeReportedUnreachable` (False);
`ProbeOutputUnreadable`, `ProbeRunning`, `ProbeStale`, `PodCreationForbidden`,
`NoExitCode` and the crash states (Unknown); and the configuration refusals
`ConnectionConfigInvalid`, `CredentialNotRenderable`, `PlainWithoutTls`,
`ConnectionReferenceInvalid`, `ConnectionFieldUnsupported`,
`CredentialBindingMismatch`. Every reason but `Reachable` clears
`status.reachable`.

### `RecoveryCatalog`

| Condition | Reasons |
|---|---|
| `Ready` | `ViewReady`; `NeverSynced`, `SyncInProgress` (Unknown); `ViewExpired`, `DestinationUnusable`, `LegacyArchiveUnsupported`, `PageConflict`, `JobNameConflict`, `DuplicateCatalog` (False) |
| `Synced` | `Succeeded`; `SyncInProgress`, a pod-waiting code (Unknown); `PartialScan`, `ScanIncomplete`, `ResultUnreadable`, `PodCreateRejected` or another check code (False) |
| `Stale` | `ViewStale`, `ViewFresh` |
| `TrustAvailable` | `TrustMaterialPresent`, `NoTrustMaterial`, `TrustPolicyConflict` |

A walk stopped by a budget names `catalogStoppedFor: objectBudget` or
`shardBudget`; `deepCheck: SegmentSample` reports
`catalogSegmentSample: notImplemented`.

### `RetentionPolicy`

| Condition | Reasons |
|---|---|
| `Ready` | `PolicyReady`; `Conflict`, `DestinationUnusable`, `CatalogUnusable`, `BackupHistoryTooLarge`, `BackupVerdictsUnreadable`, `UnsupportedCombination` (False) |
| `Evaluated` | `EvaluationComplete`; `NeverEvaluated` (Unknown); `ViewUnreadable`, `BackupVerdictsIncomplete`, `PlanRefused` (False) |
| `Enforced` | `RunInProgress`, `RunComplete`, `UnattendedDeletionEnabled` (True); `RecommendationOnly`, `AwaitingApproval`, `PlanSuperseded`, `PlanExpired`, `ActiveRestore`, `UnsupportedSchedule`, `LeaseNotHeld`, `PlanConfigMapConflict`, `RunNotRecorded`, `RunFailed`, `NothingToDo`, `NothingFitsCeiling`, `ViewIncomplete`, `ViewExpired`, `JobNameConflict`, `EvidenceGrantUnusable`, `CredentialBindingMismatch`, `PodCreationForbidden` (False) |
| `ExternalLifecycleConflict` | `DeclaredExpiryConflicts`, `NoConflict` |
| `EnforcementDegraded` | `ConsecutiveRunFailures`, `Healthy` |

`status.enforcement`: `RecommendationOnly`, `LogweirWorker`,
`ExternalLifecycleDeclared`. Each of `status.guarantees` (`ageExpiry`,
`minUsablePoints`, `activeRestoreProtection`, `sharedSegments`, `legalHold`):
`LogweirEnforced`, `ProviderEnforcedUnverified`, `NotEnforced`. A candidate names
`OlderThanKeepDays` or `BeyondKeepLast`; a protected point `ActiveRestore`,
`MinUsablePoints`, `LegalHold`, `SharedSegment`, `Hold` or `Unknown`; a skipped
point `Unreadable`, `UnsupportedFormat`, `Conflict` or `AlreadyDeleted`.

### `ProtectionPolicy`

`status.health`: `Healthy`, `AtRisk`, `Stale`, `Unprotected`, `Unknown`.
`Protected`'s reason: `WithinObjective`, `PointOlderThanObjective`,
`NoAvailablePoint`, `CatalogStale`, `CatalogUnreadable`, `SourceMissing`,
`DestinationMissing`, `ScheduleMissing`, `PointFactsUnread`.
`status.availabilityBasis`: `KubernetesStatus`, `Catalog`, `CatalogStale`.
`Ready`: `Evaluated`, `NotEvaluated`. `NotificationsDelivered`:
`NotificationsWithdrawn`. Ledger kinds include `Staleness`, `ArchiveUnavailable`
and `RecoveryCompleted`.

### `TopicDiscovery`

`status.phase`: `Pending`, `Queued` (`ConcurrencyLimited`), `Running`,
`Succeeded`, `Failed`, `Cancelled`. A `Failed` discovery carries the relayed
check's own code, or `ConnectionNotFound`, `ConnectionInvalid`,
`DeadlineExceeded`, `ResultUnreadable`, `CheckPlanConflict`,
`ResultStorageConflict`, `Stalled`, `RunnerContractUnsupported`,
`PodCreateRejected` or a pod-waiting code. `status.result.visibility.state`:
`unknown`, `limited`, `attestedComplete`; `visibility.basis` includes
`attestationPrincipalMismatch` and `attestationClusterIdMismatch`;
`truncationReason: MaxTopics`.

### Preflight rows

`status.result.state` is `ready`, `notReady` or `unknown`; a row's `state` is
also `skipped`. `authority` is `controller`, `checkJob` or `podStatus`.

| Row | Gating | Asks |
|---|---|---|
| `connection.resolved` | blocking | The `KafkaCluster` resolves to a dialable connection |
| `connection.credentialProjected` | blocking | The credential Secret and key exist (the kubelet's answer) |
| `connection.authenticated` | blocking | The broker accepted the credential |
| `connection.clusterIdentity` | blocking | The broker's cluster id is the recorded one; a backup source is not an allowlisted target |
| `connection.topicsDescribable` | blocking | Every named topic exists and may be described |
| `connection.topicsReadable` | execution-only | Proved only by the run |
| `connection.engineProtocol` | blocking | The source serves the request versions the engine sends |
| `connection.topicConfigsReadable` | advisory | DescribeConfigs on the named topics |
| `connection.groupTypes` | advisory | The group listing names group types (ListGroups v5) |
| `destination.resolved` | blocking | The destination and its grants resolve for one pod |
| `destination.credentialProjected` | blocking | The grant's Secret and keys exist |
| `destination.archiveListable` | blocking | The archive prefix can be listed |
| `destination.evidenceWritable` | blocking | The readiness marker can be created, and conditional create is enforced (only with `writeProbe`) |
| `destination.archivePrefixWritable` | execution-only | Proved only by the run |
| `destination.evidenceReadable` | advisory | The `evidenceRead` grant can read under `logweir/` |
| `destination.credentialBound` | blocking | Every listed `SecretKeys` grant carries this destination's binding |
| `signer.privateKeyUsable` | blocking | The signing key mounts and parses |
| `signer.rostered` | blocking (backup); execution-only (restore) | The signer is a usable `EvidenceSigning` key of the namespace's trust |
| `runner.image`, `runner.pod`, `runner.contract` | blocking | The runner image pulls, the pod starts, the runner speaks the check contract |
| `configuration.policy` | advisory | The installation policy parsed |
| `configuration.egress` | execution-only | A NetworkPolicy no check can observe |
| `target.resolved`, `target.credentialProjected`, `target.authenticated`, `target.clusterIdentity` | blocking | As for the source, on the target; the target is allowlisted and not the source |
| `target.scratchMarker` | blocking | A scratch target carries its marker topic |
| `target.mappedTopics` | blocking | No mapped target topic exists |
| `target.topicCreate` | blocking | A validate-only create of every mapped topic succeeds |
| `target.timestampBound` | blocking | The broker's record-timestamp bound admits the archive |
| `target.logAppendTime` | execution-only | Phase 0's override probe |
| `target.engineProtocol` | blocking | The target serves the engine's request versions |
| `plan.parse` | blocking | The plan parses |
| `plan.bindings` | blocking | The plan names the request's referents and locations |
| `plan.names` | blocking | Every mapped name is legal and not the identity mapping (unless original names were asked for) |
| `recoveryPoint.state` | blocking | The point exists, succeeded, and is in the source destination (or the catalog row is selectable) |
| `archive.backupSet` | blocking | The manifest (and a bound point's receipt and pin) read as the runner will read them |
| `archive.coverage` | blocking | The archive covers the point in time |
| `archive.segments` | blocking | The sampled segments exist |
| `approval.state` | blocking | The `Approval`'s own verdict |
| `approval.keyValidity` | advisory | The approver key outlives the restore's deadline |

A row expires after 15 minutes, except `connection.topicsDescribable` and
`connection.topicConfigsReadable` (10 minutes), the `archive.*` rows (30
minutes), `target.mappedTopics` and `target.topicCreate` (5 minutes), the two
`approval.*` rows (`min(10 m, notAfter)`), and `plan.parse`, `plan.names` and
every execution-only row (never).

Row codes. **Ready:** `Resolved`, `Projected`, `Authenticated`,
`ClusterIdentityMatches`, `TopicsDescribable`, `DestinationValid`,
`ArchiveListable`, `MarkerWritten`, `MarkerAlreadyPresent`, `EvidenceReadable`,
`SignerUsable`, `SignerRostered`, `ImageAvailable`, `PodStarted`,
`ContractSupported`, `PolicyLoaded`, `PlanParsed`, `PlanMatchesReferences`,
`MappedNamesLegal`, `RecoveryPointSucceeded`, `CatalogPointSelectable`,
`ManifestReadable`, `PointInTimeCovered`, `SegmentsPresent`, `TargetAllowed`,
`MarkerHealthy`, `MappedTopicsAbsent`, `TopicCreateValidated`,
`TimestampWithinBound`, `ApprovalVerified`, `ApproverKeyValid`, `Valid`,
`CredentialBound`, `EngineProtocolSupported`, `TopicConfigsReadable`,
`GroupTypesListed`, `Succeeded`.

**Store and broker failures:** `AccessDenied`, `InvalidCredentials`,
`BucketNotFound`, `ObjectNotFound`, `EndpointUnreachable`, `TlsTrustFailed`,
`RegionMismatch`, `Timeout`, `StoreErrorUnclassified`, `BrokerUnreachable`,
`AuthenticationFailed`, `TlsHandshakeFailed`, `MetadataTimeout`,
`ClusterAuthorizationFailed`, `TopicAuthorizationFailed`,
`UnknownTopicOrPartition`.

**Pod waiting:** `CredentialSecretNotFound`, `CredentialSecretKeyMissing`,
`TrustBundleNotFound`, `RunnerImagePullFailed`, `RunnerImageNotPresent`,
`RunnerImageInvalid`, `PodUnschedulable`, `SigningKeyMissing`,
`VolumeMountFailed`, `RunnerServiceAccountMissing`, `PodCreateRejected`,
`DisruptedMidCheck`, `ForeignPodIgnored`.

**Not ready:** `ConnectionNotFound`, `ConnectionInvalid`,
`CredentialReferenceMissing`, `CredentialBindingMismatch`,
`ClusterIdentityChanged`, `SourceIsAllowlistedTarget`, `TopicNotFound`,
`TopicNotAuthorized`, `DestinationNotFound`, `DestinationNotValid`,
`DestinationRoleNotConfigured`, `ExecutionContextConflict`,
`CaBundleUnsupportedByEngine`, `CaBundleNotFound`, `CaBundleKeyMissing`,
`CaBundleTooLarge`, `CaBundleInvalid`, `AddressingUnsupportedByEngine`,
`ControllerIdentityNotAllowlisted`, `WorkloadIdentityNotInjected`,
`SigningKeyUnreadable`, `SigningKeyInvalid`, `TrustRosterNotFound`,
`TrustRosterNotLoaded`, `SignerNotRostered`, `SignerKeyExpired`,
`RunnerContractUnsupported`, `PolicyUnreadable`, `PlanUnparseable`,
`PlanHashMismatch`, `PlanDestinationMismatch`, `PlanEvidenceDestinationMismatch`,
`PlanTargetMismatch`, `PlanTopicsNotInRecoveryPoint`, `MappedTopicNameIllegal`,
`TopicMappingIdentity`, `GlobInTopic`, `ExpansionInTopic`,
`RecoveryPointNotFound`, `RecoveryPointNotSucceeded`, `RecoveryPointUidChanged`,
`RecoveryPointLocationMismatch`, `CatalogPointNotSelectable`,
`CatalogPointRefusedByController`, `CatalogPointBindingMismatch`,
`CatalogPointSignerUntrusted`, `BackupSetNotFound`, `ManifestUnreadable`,
`ManifestSuperseded`, `PointBindingMismatch`, `PointInTimeBeforeCoverage`,
`PointInTimeAfterCoverage`, `TopicNotInBackupSet`, `SelectionInvalid`,
`WindowStartBeforeCoverage`, `PartitionNotInBackupSet`, `SelectionEmpty`,
`SegmentMissing`, `TargetNotAllowlisted`, `TargetEqualsSource`,
`MarkerTopicMissing`, `MarkerTopicErrored`, `MappedTopicExists`,
`TopicCreateNotAuthorized`, `TopicConfigRejected`,
`ReplicationFactorExceedsBrokers`, `TimestampBoundExceeded`,
`ApprovalNotVerified`, `ApprovalExpired`, `ApprovalPlanMismatch`,
`ApprovalSubjectMismatch`, `ApproverKeyExpiresBeforeDeadline`,
`ArchiveUrlUnreadable`, `ConditionalCreateUnsupported`,
`EngineProtocolUnsupported`, `TopicConfigsNotReadable`, `GroupTypesNotListed`,
`NotReady`.

**Unknown:** `PodNotStarted`, `BlockedByPrerequisite`,
`ClusterIdentityNotObserved`, `TopicVisibilityUnknown`, `WriteNotProbed`,
`EvidenceReadNotConfigured`, `SignerKeyIdNotObserved`,
`ReadVerifiedOnlyAtExecution`, `ArchivePrefixWriteVerifiedOnlyAtExecution`,
`NetworkPolicyEnforcementNotObservable`, `SegmentListTooLarge`,
`MappedTopicVisibilityUnknown`, `TopicCreateValidationUnsupported`,
`BrokerConfigsNotReadable`, `LogAppendTimeOverrideVerifiedOnlyAtExecution`,
`ApprovalPending`, `ApproverKeyWindowUnknown`, `SubjectNotCreated`,
`RecoveryPointLocationUnknown`, `CatalogPointViewUnavailable`,
`CatalogPointSignerUnknown`, `TrustUnknown`, `ApiVersionsNotObserved`,
`TimestampBoundNotReported`.

**Lifecycle (a `Preflight` or `TopicDiscovery` that produced no result):**
`CheckContractMismatch`, `ResultUnreadable`, `DeadlineExceeded`,
`CancelRequested`, `Stalled`, `ConcurrencyLimited`, `CheckPlanConflict`,
`ResultStorageConflict`.

### A pod refused at creation

Every Job-owning controller reads its Job's `FailedCreate` event (a
`ResourceQuota`, a `LimitRange`, an admission webhook, a missing ServiceAccount)
and reports it in the admission's own words:

| The Job | What the object reports |
|---|---|
| A `Backup`'s or `Restore`'s runner | `RunnerReady=False/PodCreationForbidden`, a `PodCreateRejected` diagnostic, then terminal `PodCreationForbidden` after `failFastSeconds` |
| A `Preflight`, an evidence fetch | `PodCreateRejected` after 30 s with no pod; the Job is cancelled |
| A `TopicDiscovery` | `Failed/PodCreateRejected` after 30 s; the Job is cancelled |
| A `RecoveryCatalog` sync | `Synced=False/PodCreateRejected`, `lastSyncJob.refusalReason: PodCreateRejected`; the view is kept |
| A dynamic `Backup`'s discovery | `Failed/PodCreationForbidden` (also on `TopicsResolved`); no runner Job |
| A `KafkaCluster` probe | `Reachable=Unknown/PodCreationForbidden`; `reachable` cleared; the next probe runs on the cadence |
| A `RetentionPolicy` run | `Enforced=False/PodCreationForbidden`, "nothing was deleted"; counts as a failed run |

The read is one `list` of `events` by `involvedObject.uid`, `limit=20`, made only
when no pod appeared after 30 s or a volume is still mounting after 60 s. A
failed list leaves the object on its previous path.

## Object-storage grants

The minimal actions per role are in [kubernetes.md](kubernetes.md#the-grants-and-the-object-storage-actions-each-one-needs).
This is what the product answers when one required action is missing, as
measured on docker-desktop MinIO (`e2e/k8s/d2/d2_live.py`, `U6` phases; re-run
with `u6setup u6point u6b u6f u6a u6c u6d u6e`, then `u6table`). Every other
action tried (`s3:GetBucketLocation`, `s3:AbortMultipartUpload`,
`s3:DeleteObject` outside the enforcer, the second `s3:prefix` leg, reads under
the other root) was removed with no effect.

| Role | Action removed | The product's answer |
|---|---|---|
| `archiveWrite` | `s3:ListBucket` (`<prefix>/*`) | exit `1`, `operational` |
| `archiveWrite` | `s3:GetObject` on `<prefix>/*` | exit `1`, `operational` |
| `archiveWrite` | `s3:PutObject` on `<prefix>/*` | exit `1`, `operational` |
| `archiveWrite` | `s3:PutObject` on `logweir/*` | exit `4`, `signing-or-lock` (`ExecutionClaimUnproven`, before the engine starts) |
| `archiveRead` | `s3:ListBucket` (`<prefix>/*`) | `archive.segments` and `destination.archiveListable` → `AccessDenied` |
| `archiveRead` | `s3:GetObject` on `<prefix>/*` | `archive.backupSet` → `AccessDenied`; `archive.segments` → `BlockedByPrerequisite` |
| `evidenceWrite` | `s3:PutObject` on `logweir/*` | exit `4`, `signing-or-lock` (`ExecutionClaimUnproven`) |
| `evidenceRead` | `s3:GetObject` on `logweir/*` | `destination.evidenceReadable` → `AccessDenied` |
| write probe | `s3:PutObject` on `logweir/readiness/*` | `destination.evidenceWritable` → `AccessDenied` |
| `catalogSync` reader | `s3:ListBucket` (`logweir/*`) | `ResultUnreadable` |
| `catalogSync` reader | `s3:GetObject` on `<prefix>/*` or `logweir/*` | `PartialScan` |
| retention enforcer | `s3:ListBucket` (`<prefix>/*`) | `state=Kept`, `code=ListRefused:AccessDenied`, `retention-result=deleted=0 failed=1 objects=0` |
| retention enforcer | `s3:GetObject` on `<prefix>/*` | every key `Kept`, `code=VersionProbeRefused`, nothing deleted |
| retention enforcer | `s3:DeleteObject` on `<prefix>/*` | `state=Kept`, `code=AccessDenied`, `retention-result=deleted=0 failed=1 objects=0` |

## Destination-backed Job environment

A runner Job for a `Backup` or `Restore` that names a `BackupDestination`
carries this set, computed from the destination alone. Nothing comes from the
controller's environment.

| Variable | Value |
|---|---|
| `LOGWEIR_STORE_CONTRACT_VERSION` | `1`, with `--store-contract-version 1` in argv. A runner that does not know the contract rejects the flag before dispatch. |
| `AWS_ALLOW_HTTP` | `true` only for `transport.security: InsecureHTTP` (which needs an `http://` endpoint). Always rendered, `false` included. |
| `AWS_VIRTUAL_HOSTED_STYLE_REQUEST` | From `storage.addressing` only. |
| `AWS_METADATA_ENDPOINT` | `http://127.0.0.1:1`, a dead loopback. |
| `AWS_REGION` | Only when `storage.region` is set. |
| `LOGWEIR_ARCHIVE_CREDENTIALS` | `static` or `workloadIdentity`. |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` | `secretKeyRef` into the grant's Secret and keys. The session token only when the grant names one. |
| `LOGWEIR_ARCHIVE_CA_FILE`, `LOGWEIR_EVIDENCE_CA_FILE` | Only with a `transport.caBundle`; the bytes are frozen into the run's plan `ConfigMap`. |
| `LOGWEIR_EVIDENCE_CREDENTIALS` | `archive` when `evidenceWrite` resolves to the `archiveWrite` grant (nothing second is projected); `static` or `workloadIdentity` when separated, with `LOGWEIR_EVIDENCE_AWS_*`. The runner refuses a run that does not name it. |

`AWS_ENDPOINT_URL` is absent by construction: the endpoint is in the plan's
`storage` block.

## Read caps

Every object-store read names a cap. The size the store reports is checked
first, then a running cap holds over the stream. An existence test reads no body.

| Document | Read by | Cap |
|---|---|---|
| Backup receipt | the controller, an evidence relay, the catalog walk | 5,131,072 bytes |
| Scorecard and every other signed document | the controller, an evidence relay | 1 MiB |
| DSSE sidecar | everyone | 64 KiB |
| Catalog record | the catalog walk | 6,131,072 bytes |
| Receipt, scorecard, catalog record | runner, CLI | 64 MiB |
| Engine manifest | runner, CLI, check Jobs | 256 MiB |
| Archived segment | runner, CLI | 1 GiB |
| Consumer-groups snapshot, engine report | runner, CLI | 64 MiB |

The controller's concurrent reads share one 128 MiB budget (a receipt or
scorecard reserves 40 MiB); a read that does not fit waits. A `catalogSync`
check Job needs about 64 MiB above its baseline for the largest point, plus its
largest manifest (about 540 bytes a segment); a check Job states no memory
limit, so a namespace `LimitRange` applies.

An object over its cap is never a crash and never a pass:

| Where | What it says |
|---|---|
| `Backup` and `Restore` `status.evidence.verification` | `NotAttempted`: `<key> is larger than the <cap>-byte cap weirkeeper reads (the store reports <n> bytes); nothing was verified`. Final; not retried. |
| An evidence relay | The Job reports the object `present` and `truncated` and relays no bytes: `<key> is larger than the <cap>-byte cap an evidence fetch relays; nothing was verified`. Final. |
| `Preflight` `archive.backupSet` | Not ready: `…could not be read: <code>: it is larger than the 268435456-byte read cap for a manifest`. |
| `Preflight` `archive.backupSet`, a point-bound plan's receipt | `PointBindingMismatch`: `…it is absent, its bytes do not hash to the bound digest, or it is larger than the 67108864-byte read cap for a receipt`. |
| Drill, `backup run`, `catalog sync` | exit 1, or an `Unreadable` point, naming the cap. |

Documentation is licensed [CC-BY-4.0](LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
