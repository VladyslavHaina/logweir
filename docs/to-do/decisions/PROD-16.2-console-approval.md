# PROD-16.2 — Two-person approval in the console

- Row: PROD-16.2 (impl, Tier A security), [product-expansion tracker](../product-expansion.md). Owner decisions: OD-7 (signed formats: MINOR, only safer), OD-8 (no approver key by default), OD-10 (typed names are the one-person mode's), OD-11 (v1 cut: one team per install, the smallest thing that meets the row).
- Date: 2026-10-10. Branch `claude/prod-16-2`. Amends [D0](D0-product-api-and-identity.md). Operator account: [kubernetes.md](../../kubernetes.md#two-person-approval-in-the-console-prod-162) §8; API: [api.md](../../api.md#two-person-approval-in-the-console-prod-162); residual: [SECURITY.md](../../../SECURITY.md).

## 1. The protocol

A namespace bound to `mode: two-person` (internal `Governed` + `approverSignature: Console`, inside the policy snapshot and its digest) runs a restore after a second signed-in person clicks Approve. No personal key exists; the console's existing `ConsoleConfirmation` key signs both documents.

| Step | Signed bytes | Stored as |
| --- | --- | --- |
| **Request** (an Operator, shared console) | authorization document v2, exactly as a strict namespace's: Restore namespace/name/**UID**, `planHash`, `requester {issuer, subject}`, policy name + digest, `issuedAt`, `expiresAt`, `ticket` (2.0.0; 2.1.0 with `approvalSubject: originalName`) | `Approval/<approvalRef>-confirmation`, authorises nothing |
| **View** | none. The console verifies its own signature over the stored bytes, holds them to the Restore, its plan and the current policy, then shows the request and its whole scope (§2) | — |
| **Click** (an Approver, not the requester) | the SAME document plus `approver {issuer, subject}` and `approvedAt`, as **2.2.0**. Every other field is copied from the verified bytes; the body carries only `confirmationSha256`, compared and never copied | `Approval/<approvalRef>`, create-only |

The click checks, in order: Approver role (from the binding table, per request; an Administrator is not an Approver); `Origin`, CSRF token, JSON; shared console; two-person policy now; **its own signature on the stored request**; binding to Restore/UID/plan/policy; not expired; the hash; the scope is complete (§2); **requester ≠ approver** (§3); `approvedAt` in the window (§4); the readers' own check over what it is about to sign.

The **controller** (Approval verdict, then Restore admission, then the bundle writer) and the **runner** re-check from the signed bytes: the console's signature, subject/UID, plan, policy digest and setting, both new fields, §3 and §4. The runner accepts the console key as the approver's for this one row of the table (§5) and no other.

## 2. What the approver is shown (the coordinator's addition 6)

What is approved is what was shown, and the server enforces it. `logweir_core::approval_scope::approval_scope` is a pure function of the verified request and the plan it names by hash (it checks the hash first; nothing is read from the Restore object's other fields): requester, Restore and UID, plan hash, subject, policy, ticket, expiry, **source archive and backup set, recovery point, target cluster (bootstrap servers, auth, mode, prefix), every topic and the name it is restored under (original names marked, partition subsets listed), verification coverage and window, evidence location**.

- **Complete or nothing.** At most 1024 topics and 8192 listed partitions; every topic a Kafka name before and after mapping; every other value printable ASCII of at most 1024 characters. Otherwise the scope is incomplete, with a stable reason and a sentence saying what to do.
- The view carries `scopeComplete` and the whole scope; an incomplete one is offered to nobody. The **click re-derives it** and refuses an incomplete one (`409`, `scope_incomplete`), whatever a page did. The create refuses a two-person request whose plan could not be shown in full (`422`, `planBytes`, `scope_incomplete`), so the largest restore this mode accepts is 1024 topics: a larger one is split, or bound `strict`.
- The page renders every topic (a list that scrolls and wraps, never sliced) and draws no button beside a scope that is not complete and whole.
- The approval's audit record notes `scope: complete`, the topics shown and the topics in the plan.

## 3. Who is a second person

One function (`approval_policy::separation_fault`), used by the console, the controller twice, the runner and both scorecard readers. It compares issuer and subject only: each visible ASCII, issuer without `#`, each at most 255 characters; neither `urn:logweir:local-admin` nor a `system:` subject; same issuer after lower-casing and trimming a trailing `/`; different subject after lower-casing. Every arm only refuses. It does not detect one human with two accounts (§7).

## 4. Time

`issuedAt <= approvedAt < expiresAt`, exactly, at every reader; the controller also refuses `approvedAt` more than 60 s ahead of its clock and an expired request; the runner reads no clock. **No reader substitutes a time**: an approver without `approvedAt` is refused by name (parser, controller, runner, both verifiers). Strict and confirm keep recording `issuedAt`, as before.

## 5. One table, every layer

`ApprovalPolicy::route`: (`Ordinary`, —) → requester confirms (`confirm`, scorecard `ordinary`); (`Governed`, `Console`) → second person in the console (`two-person`, `consoleApproval`; shared console only); (`Governed`, —) → personal key (`strict`, `governed`); (`Ordinary`, `Console`) → **not a row**: refused by the policy parser (controller and console do not start), by the snapshot parser (runner exit 3), and by the verdict, admission, bundle and API if built in memory. The chart refuses `two-person` outside `api.console.mode=shared`.

## 6. Versions

| Signed thing | Change | Older readers |
| --- | --- | --- |
| policy snapshot | `approverSignature` last key, only when set; existing bytes and digests unchanged | refuse the key: runner exit 3, controller/console refuse to start |
| authorization document | **2.2.0**: `approver`, `approvedAt`; 2.0.0/2.1.0 bytes unchanged; payload type unchanged | refuse the fields |
| scorecard | **1.9.0** and **2.1.0**: optional `approval.console`; `approval_mode` gains `consoleApproval` from 1.9.0 (ON-5 split by version); arms CA-1 to CA-8 | accept and ignore the block; a 1.28.0 reader refuses an original-name document naming the new mode (safer) |
| `verify_scorecard.py` | 1.29.0 | — |
| product API | two operations, the scope schemas | — |

No MAJOR. A shipped reader cannot be made to refuse a console-approved scorecard by its version without one (measured in the row's report).

## 7. Residual

Two identities of one identity provider, attested and signed by one console. Whoever controls the console pod, its key, the identity provider (or two accounts there), the role bindings or the policy document can produce both halves. `strict` is the mode where the console is not enough.

## 8. PoC rows (not run by this row)

Shared-mode install, Dex users `alice` (Operator) and `bob` (Approver) in a namespace bound `two-person`:

1. `alice` submits a **three-topic** restore; `bob` sees every topic and the name it is restored under, the target cluster and the source, clicks Approve; the Restore runs; both verifiers print the `console approval:` line naming `alice` and `bob`.
2. `alice` (also bound Approver) opens her own request: no button; a direct POST is 403 `self_approval_forbidden`.
3. A hand-applied `<approvalRef>-confirmation` not signed by the console: "not confirmed by this console", no fields; a POST is 409.
4. An Administrator-only user: no button; POST 403.
5. Past `maxAgeSeconds`: expired; POST 409.
6. `helm template` with `localAdmin` and a two-person policy fails by name.
7. A `strict` namespace on the same install: countersign flow and 2.0.0 bytes unchanged.
8. An original-name restore: `bob` sees the names marked original; scorecard 1.9.0 with `consoleApproval`.
9. A 1025-topic plan in the two-person namespace: the create is refused `scope_incomplete`, nothing created.
10. Rollback: an older controller refuses to start on a document carrying `approverSignature`; removing the policy lets it start.

Documentation is licensed [CC-BY-4.0](../../LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
