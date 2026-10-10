# PROD-15.1 — Restore under the original topic name, into an absent topic

- Row: PROD-15.1 (impl, Tier A), [product-expansion tracker](../product-expansion.md). Owner decisions: OD-2 (2026-10-05) and OD-10 (2026-10-09).
- Date: 2026-10-09. Branch `claude/prod-15-1`. The first round was reviewed in `claude/prod-15-1.review.md` (ACCEPT-WITH-FIXES, no HIGH). This record states the contract after the fix round.
- Operator account: [kubernetes.md](../../kubernetes.md#restoring-under-the-original-topic-names-prod-151) §12. Plan field: [drill-spec.md](../../formats/drill-spec.md#targettopic_namingoriginal_name-prod-151). Evidence: [drill-scorecard.md](../../formats/drill-scorecard.md#targetoriginal_name-format-180).

## 0. Owner decisions this row implements

These are written in the tracker's *Owner decisions* style. The tracker holds the canonical rows; this table repeats what was decided.

| ID | Decision | Options | Decided | Implemented here as |
| --- | --- | --- | --- | --- |
| OD-2 | `docs/stability.md` Never #1 | keep, narrow or reverse | **Narrowed 2026-10-05**: a restore into a LIVE topic stays refused; PROD-15's original-name restore into an ABSENT topic is allowed behind its own approval | Never #1 narrowed, with its doc_lint guard, in one commit (`41aa5258`); the separate approval subject `originalName` |
| OD-10 | How an original-name restore is approved on an install using one-person confirmation (OD-8) | (a) typed confirmation: the requester re-types the original topic names in the console, and the evidence records it; two-person or strict namespaces still need a second person; (b) always two people; (c) the namespace's normal mode, with no extra step | **(a), 2026-10-09** | §2 |

## 1. The conditions, and where each is held

| # | Condition | Held by | Refusal |
| --- | --- | --- | --- |
| 1 | The plan opts in: `newTopic`, `topic_naming: {prefix: "", original_name: {…}}` | runner phase 0; controller (CEL rule, declaration held to the plan) | `OriginalNameNotNewTopic`, `OriginalNamePrefixNotEmpty`; an empty prefix without the block is refused "onto itself" |
| 2 | Every restored name is absent | runner phase 0 | "already exists" |
| 3 | The target is not the source cluster, or every metadata-listed broker reports `auto.create.topics.enable=false` | runner phase 0; source id from the bound point's VERIFIED receipt only (review L3) | `OriginalNameAutoCreateEnabled`, `OriginalNameAutoCreateUnknown` |
| 4 | An owner was looked for and none found, unless the plan chose the owner path; nothing in a `KafkaTopic` resources file is dropped (review M2, L2) | runner startup and phase 0 | `OriginalNameOwnerNotChecked`, `OriginalNameOwnerPresent`, `OriginalNameOwnersInvalid`, `OriginalNameOwnerUnreadable` |
| 5 | The approval's signed subject is the plan's: `originalName` | `drill approve`, product API, controller admission step 4b, runner (startup, before phase 0, after phase 1) | `ApprovalSubjectMismatch` |
| 5b | On a one-person confirmation, every original topic name re-typed, exactly (OD-10) | console, product API, controller admission step 4b, runner | `OriginalNameConfirmationMissing`, `…Mismatch`, `…NotAccepted`; API codes `typed_topics_required`, `typed_topics_mismatch`, `not_accepted` |
| 6 | Creation is exclusive; a race is lost by name and surfaced on the Restore (review M4) | the creation step; controller | `TargetTopicAppeared` (exit 1, `failure-reason=`) |
| 7 | The probe and teardown never touch an original name | phase 0 probe name; phase 9 rail; the deleter's protected names | — |

## 2. OD-10: the typed confirmation

1. **Who must type.** Only a one-person confirmation needs typed names: an authorization document v2 signed under an `Ordinary` policy (`confirm`). A v1 approval (an approver's personal key) and a `Governed` document (`strict`: the console's confirmation plus an approver's countersignature) have a second person and carry no typed names. Two-person (PROD-16.2) is not served by this build. When it is, it is a second person too.
2. **What is typed.** Every name in the plan's `source.topics`, once each, with nothing else, matched byte for byte. The console splits on lines and commas and trims the spaces around each name. It never folds case.
3. **Where it is signed.** In the authorization document v2's `originalNameConfirmation: {typedTopics: [...]}`, beside `approvalSubject`, so it is inside the bytes the console key signs. `formatVersion` stays `2`, for `approvalSubject`'s reason: a reader that predates the field refuses the document.
4. **Where it is held to the plan.** The product API refuses before anything is created. The controller refuses at admission step 4b, terminally (reason `ApprovalSubjectMismatch`, the message naming the token). The runner refuses at startup and before phase 0 (exit 3). A document that carries typed names in any other case is refused everywhere (`OriginalNameConfirmationNotAccepted`).
5. **What the evidence says.** `target.original_name.confirmation: "typedTopicNames"` is written exactly when `approval_mode` is `ordinary` (arm ON-11), and both readers print "approved by ordinary (the requester re-typed every original topic name)". The approvals page shows the typed names from the signed bytes.

## 3. The fix round's other decisions

| Finding | Decision |
| --- | --- |
| M2: an owner reference over 256 characters was dropped | The restore's scan (`topic_configuration::strimzi_owner_scan`) reports every `KafkaTopic` it cannot record or read. The runner refuses `OriginalNameOwnerUnreadable` and never drops one. |
| M2's sweep: the receipt path | A backup records a `KafkaTopic` whose reference it cannot record as NO owner (`crates/logweir/src/backup/mod.rs`, PROD-05.1 warns and goes on). The receipt therefore ADDS owners but never stands in for looking: "none found" needs the plan's statement or the resources file. Changing what a backup records is PROD-05.1's contract and is owed as a class-sweep row. |
| L2: any parseable file counted as "looked" | A file with no `KafkaTopic` is refused unless it is the explicit empty `List`. The file's `sha256` is signed as `kafka_topic_resources_sha256` (arm ON-12). |
| L3: the allowlist's `source_cluster_id` counted | Only the verified receipt's measured id counts. Without a bound point the source is unknown, and auto-creation must be proven disabled. |
| M3: removing the runner's subject checks survived CI | Each call site has a CI-run row that fails without it: startup (`original_name_cli.rs`, the binary), before phase 0 and after phase 1 (`original_name_runner.rs`, the orchestrator fixture). `drill approve` has its row too. |
| M4: a lost race was a generic exit 1 and left empty topics | The runner prints `target-topics-appeared={appeared, removed, left}` and, last, `failure-reason=TargetTopicAppeared`. The controller lifts these onto `status.exitReason`, `status.targetTopicsAppeared` and the `Failed` message. **Cleanup:** a topic this execution created in the same request is removed only when three things hold: its own `CreateTopics` answer said it was created, the cluster still lists it with this run's partition count and pinned configuration, and the deleter re-reads every partition's end offset as 0 immediately before the delete. Anything else is left and named with the reason. A name that appeared is never touched. **Residual:** Kafka has no conditional delete, so a record written between that last read and the delete is lost with the topic. The window is one round trip, on a topic this run created moments earlier, before any restore. |
| M5: a producer writing during the restore | **Detect and fail.** Phase 7's count bound finds more records than the manifest bounds the window to, and the run signs `fail-integrity` (exit 2). The topic then holds both writers' records. Refusing in advance is not possible, because the name does not exist until the restore creates it. The runbook says to stop every producer first. Live row: 36 records against a bound of 30. |
| L1: a short `CreateTopics` answer | Exactly one answer per name asked, or exit 1 naming both lists. |
| L4: an exact version pin in a live row | `harness::assert_format_at_least`. |
| L5: three unguarded call sites | Rows for the deleter's protected names (`scoped_target_reader`), both `original_name_agrees` call sites, and the signed block being written. |
| L6: readiness did not evaluate the conditions | The restore readiness check refuses an original-name plan whose shape or owner statement the runner would refuse (`check/kinds/restore.rs`). |
| L7: no ADR text | `docs/architecture.md` Amendment F. |
| L8: v2 fields without a new `formatVersion` | Kept. Older readers refuse such a document (`deny_unknown_fields`), which is OD-7's safer direction. This is stated for the orchestrator to confirm. |
| L9: known limits | Documented: auto-creation is read from the brokers the metadata lists at phase 0. The CEL rule is proven at the PoC upgrade (K-rows, `originalName: null` included). |

## 4. What stays refused

These stay refused: a name that exists, in any mode; an identity mapping in a scratch drill; a target that may be the source cluster unless auto-creation is proven disabled; an owner found off the owner path; an owner looked for nowhere; an unreadable owner; an ordinary approval or a standing authorization; and a one-person confirmation without the typed names. Logweir never writes into a topic it did not create in this run.

Documentation is licensed [CC-BY-4.0](../../LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
