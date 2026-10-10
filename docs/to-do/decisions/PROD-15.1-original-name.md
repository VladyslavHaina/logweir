# PROD-15.1 — Restore under the original topic name, into an absent topic

- Row: PROD-15.1 (impl, Tier A), [product-expansion tracker](../product-expansion.md). Owner decisions: OD-2 (2026-10-05) and OD-10 (2026-10-09).
- Date: 2026-10-09. Branch `claude/prod-15-1`. The first round was reviewed in `claude/prod-15-1.review.md` (ACCEPT-WITH-FIXES, no HIGH). This record states the contract after the fix round and the orchestrator's three decisions of 2026-10-09 (§3a), which replace the fix round's answers to M4, M5 and L8.
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
| 1b | The plan asks for COMPLETE verification, `sample.coverage: complete` (§3a, decision 3) | runner startup and phase 0; `drill approve`; runner and controller readiness; controller admission and a CEL rule; product API; console | `OriginalNameNeedsCompleteCoverage`; API code `original_name_requires_complete`; scorecard arm ON-13 |
| 1c | The plan restores WHOLE topics: no `restore.partitions` (§3b, the whole-topics rule). A stated window start or end is allowed | runner startup and phase 0; `drill approve`; runner and controller readiness; controller admission and reconcile. No CEL rule: the `Restore` CRD declares no partitions | `OriginalNameNeedsWholeTopics`; scorecard arm ON-14 in both readers |
| 2 | Every restored name is absent | runner phase 0 | "already exists" |
| 3 | The target is not the source cluster, or every metadata-listed broker reports `auto.create.topics.enable=false` | runner phase 0; source id from the bound point's VERIFIED receipt only (review L3) | `OriginalNameAutoCreateEnabled`, `OriginalNameAutoCreateUnknown` |
| 4 | An owner was looked for and none found, unless the plan chose the owner path; nothing in a `KafkaTopic` resources file is dropped (review M2, L2) | runner startup and phase 0 | `OriginalNameOwnerNotChecked`, `OriginalNameOwnerPresent`, `OriginalNameOwnersInvalid`, `OriginalNameOwnerUnreadable` |
| 5 | The approval's signed subject is the plan's: `originalName` | `drill approve`, product API, controller admission step 4b, runner (startup, before phase 0, after phase 1) | `ApprovalSubjectMismatch` |
| 5b | On a one-person confirmation, every original topic name re-typed, exactly (OD-10) | console, product API, controller admission step 4b, runner | `OriginalNameConfirmationMissing`, `…Mismatch`, `…NotAccepted`; API codes `typed_topics_required`, `typed_topics_mismatch`, `not_accepted` |
| 6 | Creation is exclusive; a race is lost by name and surfaced on the Restore (review M4) | the creation step; controller | `TargetTopicAppeared` (exit 1, `failure-reason=`) |
| 7 | The probe and teardown never touch an original name, and NO code path deletes a topic under one: a topic the run created before its creation step stopped is left, empty, and named (§3a, decision 1) | phase 0 probe name; the creation step, which holds no deleter; phase 9 rail; the deleter's protected names | `CreatedTopicsLeft` (exit 1, `failure-reason=`) when creation stopped for a reason other than a race after a topic was created |

## 2. OD-10: the typed confirmation

1. **Who must type.** Only a one-person confirmation needs typed names: an authorization document v2 signed under an `Ordinary` policy (`confirm`). A v1 approval (an approver's personal key) and a `Governed` document (`strict`: the console's confirmation plus an approver's countersignature) have a second person and carry no typed names. Two-person (PROD-16.2) is not served by this build. When it is, it is a second person too.
2. **What is typed.** Every name in the plan's `source.topics`, once each, with nothing else, matched byte for byte. The console splits on lines and commas and trims the spaces around each name. It never folds case.
3. **Where it is signed.** In the authorization document v2's `originalNameConfirmation: {typedTopics: [...]}`, beside `approvalSubject`, so it is inside the bytes the console key signs. A document that carries either is format **2.1.0** (§3a, decision 2).
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
| M4: a lost race was a generic exit 1 and left empty topics | The runner prints `target-topics-appeared={appeared, left}` and, last, `failure-reason=TargetTopicAppeared`. The controller lifts these onto `status.exitReason`, `status.targetTopicsAppeared` and the `Failed` message. The fix round also REMOVED a topic it could prove was its own and empty; that cleanup is withdrawn (§3a, decision 1). |
| M5: a producer writing during the restore | The fix round's answer was detect-and-fail under a sampled check (the count bound). It left a residual: foreign records inside a loose bound could pass. Replaced by §3a, decision 3: complete verification is required, and it names each foreign record. Refusing in advance is not possible, because the name does not exist until the restore creates it. The runbook still says to stop every producer first. |
| L1: a short `CreateTopics` answer | Exactly one answer per name asked, or exit 1 naming both lists. |
| L4: an exact version pin in a live row | `harness::assert_format_at_least`. |
| L5: three unguarded call sites | Rows for the deleter's protected names (`scoped_target_reader`), both `original_name_agrees` call sites, and the signed block being written. |
| L6: readiness did not evaluate the conditions | The restore readiness check refuses an original-name plan whose shape or owner statement the runner would refuse (`check/kinds/restore.rs`). |
| L7: no ADR text | `docs/architecture.md` Amendment J. |
| L8: v2 fields without a new `formatVersion` | Replaced by §3a, decision 2: the document is versioned 2.1.0. |
| L9: known limits | Documented: auto-creation is read from the brokers the metadata lists at phase 0. The CEL rule is proven at the PoC upgrade (K-rows, `originalName: null` included). |

## 3a. The orchestrator's decisions of 2026-10-09

Each was decided to the safer side after the fix round was read.

| # | Decision | Implemented as |
| --- | --- | --- |
| 1 | **Never delete a topic under an original name.** The fix round's lost-race cleanup is removed: Kafka has no conditional delete, so a record a producer writes between the last end-offset read and the delete would be LOST under a production name. | The creation step takes no deleter (`phase0_admit::create_target_topics`), and `TopicDeleter` has no cleanup method. Every stop of the creation step after a topic was created names what it created and left: `target-topics-appeared={"appeared":[…],"left":[…]}`, then `failure-reason=TargetTopicAppeared` (a lost race) or `CreatedTopicsLeft` (any other stop). The controller copies the lists to `status.targetTopicsAppeared` and into the `Failed` message; the product API's Restore view carries `targetTopicsAppeared` with `leftInstruction`; the console shows it first on the Restore's page. One sentence everywhere: "created by this restore and left empty; remove it yourself once you have checked nothing writes to it". A source-text row fails if any delete enters the creation step or the path to phase 6. |
| 2 | **Version the document (review L8).** An authorization document v2 carrying `approvalSubject` or `originalNameConfirmation` is format **2.1.0**; every other document stays 2.0.0, byte identical. The standing authorization's 1.1.0 (PROD-08.1a) is the pattern. | `approval_policy::restore_authorization_format_version_for` is the one place the writer takes the version from. Both readers (controller and runner, through `RestoreAuthorization::from_bytes` and `check_binding`) accept 2.1.0 and refuse either field under a version that predates it (`AuthorizationDocumentInvalid`, "defined from formatVersion 2.1.0"). A reader built before the fields reads major 2 and refuses the document for its unknown field; measured with a binary built from main. The v1 approval document has no version field: an older v1 reader ignores `approval_subject`, and the original-name PLAN is what that older runner refuses, so the ignored subject authorises nothing. |
| 3 | **An original-name restore REQUIRES complete verification (review M5).** A sampled plan with the identity mapping is refused by name. | `original_name::refuse_shape` (`OriginalNameNeedsCompleteCoverage`), which the runner calls at startup (before anything is dialled) and at phase 0, `drill approve` before it signs, both readiness checks, and the controller's admission. A CEL rule ties `topicNaming.originalName` to `coverage: complete`. The product API refuses the request (`coverage`, `original_name_requires_complete`). The console selects complete coverage when the original names are chosen, locks the box and says why, and never renders a sampled original-name plan. Scorecard arm ON-13 refuses the block beside a sampled verification, or beside a pass that records none. Complete verification names an interleaved foreign record by its target offset ("carries no x-original-offset") and the run signs `fail-integrity`. |

## 4. What stays refused

These stay refused: a name that exists, in any mode; an identity mapping in a scratch drill; a target that may be the source cluster unless auto-creation is proven disabled; an owner found off the owner path; an owner looked for nowhere; an unreadable owner; an ordinary approval or a standing authorization; a one-person confirmation without the typed names; a sampled verification; and the approval subject in a document older than format 2.1.0. Logweir never writes into a topic it did not create in this run, and never deletes a topic under an original name.

Documentation is licensed [CC-BY-4.0](../../LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
