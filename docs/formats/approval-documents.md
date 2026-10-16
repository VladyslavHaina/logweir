# Approval documents: what authorises a restore, and their versions

Three signed documents have authorised a restore. This page holds their formats
and versions; what the controller checks before it admits a run is in
[the operator guide](../kubernetes.md#what-the-controller-checks), and how a
scorecard records the approval is in
[the scorecard format](drill-scorecard.md#approval).

| Document | Payload type | Written by | Versions |
|---|---|---|---|
| Per-run approval document v1 | `application/vnd.logweir.drill-approval+json;version=1.0.0` | `logweir drill approve` | no version field |
| Authorization document v2 | `application/vnd.logweir.restore-authorization+json;version=2.0.0` | the console | `2.0.0` to `2.3.0`; this build writes `2.3.0` and refuses a document without `subject.workspace` |
| Standing rehearsal authorization | `application/vnd.logweir.standing-rehearsal-authorization+json;version=1.0.0` | nothing since v1 (withdrawn, release-notes item 73) | `1.0.0`, `1.1.0` |

## The per-run approval document v1

`"subject_kind": "Restore"` is inside the signed bytes, so an approval cannot be
retargeted at another kind of object. The document has no version field.

**`approval_subject` (PROD-15.1), optional.** Only `originalName` is written;
absent is an ordinary approval, read exactly as before. An original-name plan
needs the subject, and the subject authorises nothing else: in the runner (both
directions, exit 3) and in the controller's admission
(`ApprovalSubjectMismatch`, terminal). `logweir drill approve
--approval-subject original-name` mints one. An older v1 reader ignores the
optional key, and the ORIGINAL-NAME PLAN is what that older runner refuses
([the plan field](drill-spec.md#targettopic_namingoriginal_name-prod-151)), so
an ignored subject authorises nothing.

## The authorization document v2

PLAT-19.2's per-run authorization under a workspace's approval policy, signed
by the console. The DSSE payload type names the v2 family and is unchanged by
every minor. Every document this build writes is 2.3.0:

| Version | Task | Adds | Written for |
|---|---|---|---|
| 2.0.0 | PLAT-19.2 | the format | every document that carries none of the fields below, byte for byte what it was |
| 2.1.0 | PROD-15.1 | `approvalSubject` (only `originalName` is written) and, under an `Ordinary` policy (a one-person confirmation) of an original-name restore, `originalNameConfirmation.typedTopics`: the original topic names the requester re-typed, held to the plan's topics at every boundary (the owner's decision OD-10) | a document that carries either; `originalNameConfirmation` is absent on every other document and refused there |
| 2.2.0 | PROD-16.2 | `approver {issuer, subject}` and `approvedAt`, both or neither | a run a second person approved in the console (two-person) |
| 2.3.0 | PROD-19.1 | `subject.workspace`: the subject's `logweir.dev/workspace` label; `subject.namespace` is the install's one namespace | every document |

**This build's readers** (the Approval controller, Restore and Switchover
admission and the runner, through one function) REFUSE a field under a version
that predates it — `AuthorizationDocumentInvalid`, for instance "defined from
formatVersion 2.1.0" — so a 2.0.0 document can never name the original-name
subject, and a 2.1.0 one can never claim a second person approved. They
REQUIRE `subject.workspace` and compare it with the subject's label, beside
name and UID: a document that names another workspace, or none (any minor
before 2.3.0), is `AuthorizationSubjectMismatch`. So an approval made in `dev`
never authorises a `prd` run, even when both workspaces share a policy and
someone relabels the object. The runner gets the label as
`LOGWEIR_EXECUTION_SUBJECT_WORKSPACE` (restore) or `--workspace` (switchover).

**A reader built before the fields** reads major 2 and refuses a document that
carries one as an unknown field (`AuthorizationDocumentInvalid`, exit 3 in a
runner): the safer verdict, OD-7's third case. It never reads one as an
ordinary authorization. Every 2.1.0, 2.2.0 and 2.3.0 document carries at least
one of its fields, so every one of them is refused there: a reader built before
PROD-19.1 refuses every document this build writes. `logweir-core`'s
`approval_policy::tests::an_older_document_reader_refuses_the_workspace` holds
that for 2.3.0 on every build: a subject read with the pre-2.3.0 shape
(`deny_unknown_fields`) refuses ``unknown field `workspace` ``, and the same
subject without the field is read.

**The approval policy snapshot** gains `approverSignature: Console` as its
last key, only when set (PROD-16.2): every existing policy's bytes and digest
are unchanged. An older controller, console or runner refuses the key
(`deny_unknown_fields`).

## The standing rehearsal authorization (format 1.0.0 and 1.1.0)

**Withdrawn (PROD-17.1, release-notes item 73).** The runner no longer reads a
standing authorization: it refuses `--standing-authorization` and
`--authorization-keys` by name (exit 1) and a contract naming the `standing`
kind (exit 3, `GuardRefused`, the message saying it is withdrawn).
`logweir-core` still reads the documents already signed, and the verifiers
still verify the scorecards they authorised. The rest of this section is the
format as it was signed.

The document is canonical JSON with the DSSE payload type
`application/vnd.logweir.standing-rehearsal-authorization+json;version=1.0.0`:

```json
{
  "formatVersion": "1.0.0",
  "kind": "StandingRehearsalAuthorization",
  "subjectRef": {"apiVersion": "logweir.dev/v1alpha1", "kind": "RehearsalSchedule",
                 "namespace": "team-a", "name": "weekly-orders", "uid": "…"},
  "scope": {"templateDigest": "sha256:…", "targetClusterId": "…",
            "topicPrefix": "rehearsal-3f2a91c7-", "topics": ["orders"],
            "maxPartitions": 200, "recordsPerPartition": 25,
            "deadlineSeconds": 3600, "modes": ["scratch"]},
  "issuedAt": "2026-06-01T00:00:00Z",
  "expiresAt": "2026-07-01T00:00:00Z"
}
```

`subjectRef.uid` names the one `RehearsalSchedule` a human authorised; the
runner required the environment's `…_REHEARSAL_SCHEDULE_UID` to equal it.
`issuedAt` must not be in the future, `expiresAt` must not have passed, and
`expiresAt - issuedAt` must not exceed 90 days (decision D3 §4.3), checked
against the node's clock as a second line of defence behind the controller's
each-slot check: an expired document refuses as `AuthorizationExpired`, every
other authorization fault as `AuthorizationInvalid`.

The scope is D3 §4.3's `RehearsalScope` (`logweir_core::rehearsal_scope`). Each
field bounds the plan a slot may run:

| Field | What it bounds |
|---|---|
| `templateDigest` | `sha256` over the canonical JSON of the schedule's `spec` minus `suspend`: the binding to one schedule template. The controller recomputed it each slot; the runner never checked it, holding no cluster credential to read the spec. |
| `targetClusterId` | The one cluster a rehearsal may restore into, compared with the id the cluster itself reports. |
| `topicPrefix` | The prefix every restored topic name carries, rendered per schedule as `<prefix><schedule-uid-first-8>-`. |
| `topics` | The source topics the authorization covers, by exact name. |
| `maxPartitions` | The partition ceiling: each slot's sampled plan carries the schedule's own bound as `sample.max_partitions`, which may not exceed it. **A scope that authorises `coverage: complete` signs exactly `0`** (format 1.1.0, below): a complete plan states no bound, so it is never read for one, and 0 is what makes every older reader refuse every plan under the scope. The schedule's own `spec.bounds.maxPartitions` still bounds the point a slot selects, inside `templateDigest`. |
| `recordsPerPartition` | The most records per partition the plan may sample and verify. It does not bound how many records the restore writes, which is every archived record in the window. |
| `deadlineSeconds` | The wall-clock bound on one run, the Job's `activeDeadlineSeconds`. The controller checks it; the runner does not. |
| `modes` | The target modes permitted: `["scratch"]`, and nothing else. A scope naming any other mode is refused. |
| `coverage` (format 1.1.0, optional) | The verification coverage the signer authorises; the plan's `sample.coverage` must be exactly it (PROD-08.1a). **Absent means `sampled`** — every 1.0.0 scope — so a scope signed before the field authorises sampled rehearsals only, and a complete plan under it is refused by name. `complete` (with `maxPartitions: 0`) admits a complete plan (which states no `max_partitions`) and refuses a sampled one. |
| `completeMaxRecords` (format 1.1.0, optional) | Only beside `coverage: complete`: the most archived records one complete verification may decode. The plan's `sample.complete_max_records` must be present and no larger. Absent: the work is bounded by `deadlineSeconds` alone. |

A standing scope admits no narrowing nobody approved: a plan under one that
states a window start, a partition subset or `restore.allow_transaction_markers`
is refused as outside the signed scope (`plan_within_scope`), because a
rehearsal restores every partition from the archive's floor.

**Format 1.1.0 (PROD-08.1a).** `coverage` and `completeMaxRecords` are defined
from `formatVersion` 1.1.0, minted exactly when the scope carried one of them,
and 1.0.0 — byte for byte as before — otherwise; the payload type is unchanged.
A reader refuses either field under a document whose minor is below 1 or not a
number, `completeMaxRecords` beside a scope that does not authorise complete
coverage, a bound of 0, and **a scope that authorises `coverage: complete` with
any `maxPartitions` but 0**. The 0 is what keeps the format MINOR (OD-7: only to
the safer side), because every reader of every version is on that side:

- **A scope that states no `coverage`, or states `sampled`:** a reader of 1.1.0
  admits exactly the sampled plans a 1.0.0 reader admitted, and refuses every
  complete plan by name.
- **A 1.1.0 scope that states `complete` (and `maxPartitions: 0`):** a reader of
  1.1.0 admits a complete plan with no `max_partitions` (and, when the scope
  signs `completeMaxRecords`, only one whose `complete_max_records` is present
  and no larger), and refuses every sampled plan by its coverage. Only a
  document that signs it grants this authority.
- **A runner or controller built before 1.1.0, the same complete-only
  document:** its admission checks the major only and its scope type ignores
  unknown fields, so it reads a SAMPLED scope with a partition bound of 0 and
  runs nothing under it. Its `Approval` controller refuses the scope
  (`ScopeInvalid`), and its plan-in-scope check refuses an ordinary sampled
  plan (its bound is above 0) and a complete plan (no bound: unbounded). A plan
  stating `max_partitions: 0` samples no partition, and every build's phase 7
  refuses an empty selection before any verdict (exit 1, no scorecard).
  Measured against `main` at `1c3a1b5e`
  (`execution_contract_v2.rs::an_older_runner_refuses_every_plan_under_a_complete_only_scope_this_build_mints`).
- **An older minter** refuses a scope with `maxPartitions: 0` before signing
  anything, so it cannot mint a complete-only authorization at all.

**Unknown fields are ignored on read, and a higher `formatVersion` major is
refused.** An unknown field cannot widen authority: the format carries no
approval-policy mode, so its readers require `GovernedApproval` whatever a
namespace is bound to. A field that NARROWS what a scope authorises ships beside
a value of an existing field that an older reader already refuses everything
under (1.1.0's `maxPartitions: 0`).

**How a signature was judged.** The signature is checked over the envelope
bytes *before* they are parsed. The key that verified is then judged on its
**usage**: `GovernedApproval` only, under every approval policy. A
`ConsoleConfirmation` key, and a key carrying only `EvidenceSigning` (the
installation's own evidence identity must never authorise its own rehearsals,
D3 §7.3), are refused as `KeyUsageMismatch`, never as a bad signature. The
mounted keyring carries `{keyId, publicKeyPem, usages}` and no lifecycle: key
state, `notBefore`/`notAfter` and revocation were the controller's to evaluate
(`logweir_core::trust::decide`) before it wrote the keyring.

---

Documentation is licensed [CC-BY-4.0](../LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
