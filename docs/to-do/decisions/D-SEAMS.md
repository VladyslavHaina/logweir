# Cross-decision seam rulings (orchestrator)

These rulings bind every worker implementing D1, D2, D3 or the adopted PLAT-17
decision. Where a decision document disagrees with a ruling here, this file
wins; the decision documents are otherwise unchanged and remain the detail.

## S1 — One check runner, not two (D1 §7.3 superseded by D2 §4.1/§4.2)

D1 proposed a fallback `logweir topics discover` subcommand with
`discovery-topic=` / `discovery-summary=` stdout lines. D2 independently
specified one shared check runner, `logweir check run --plan <file>
--check-contract-version 1`, with plan kinds `topicInventory`,
`operationReadiness`, `restorePreflight`, `destinationAccess` and
`evidenceFetch`, a strict pre-network startup order and a closed error-code
vocabulary.

Ruling: the D2 check runner is canonical. No `logweir topics discover`
subcommand is added. PLAT-09.2's per-run discovery invokes `check run` with
kind `topicInventory` from a Backup-owned Job, and reuses the same result
frames, digest and error codes. This keeps one runner contract, one argv
allowlist surface and one classification table.

## S2 — Discovery results are never execution inputs

A `TopicDiscovery` object (D2 §5) serves interactive API/UI discovery. A
dynamic `allUserTopics` Backup (D1 §7) must discover afresh in its own owned
Job and freeze the exact sorted names plus the discovery summary and result
digest into `execution-inputs.json` (D1 §3.3). A worker must never read a
`TopicDiscovery` result as the input to a run.

## S3 — Completeness vocabulary is D2 §5.4 everywhere

`unknown | limited | attestedComplete`, where a successful Kafka listing alone
is `unknown` and `limited` requires an observed authorization failure. D1's
coverage labels (`NamedTopics`, `AllUserTopicsAttested`,
`VisibleUserTopicsOnly`) are the run-level rendering of that vocabulary and
must be derived from it, not computed separately. No UI, API, status or
receipt text claims "all topics" unless coverage is `AllUserTopicsAttested`.

## S4 — Execution inputs grammar is owned by PLAT-06.1, extended additively

`execution-inputs.json` version `v2` (D1 §3.3) is the single frozen grammar.
D2's resolved destination snapshot (D2 §3.7) is a block inside that same
document and the same immutable ConfigMap — not a second freeze, not a second
ConfigMap. Any worker adding a block adds it to that grammar, keeps `v1`
loadable, and extends PLAT-06.1's validation rather than adding a parallel
check.

## S5 — Transport security is never derived

Neither addressing style, endpoint shape nor any global environment value may
enable plaintext transport. `AWS_ALLOW_HTTP` and friends must not reach a
runner Job from controller environment (defect SEC-ENVHTTP); a destination-
backed Job carries its complete, explicit `AWS_*` set. The UI must not derive
`allowHttp` from the path-style control (defect UI-HTTPDOWNGRADE).

## S6 — Pod identity is verified by owner UID

Every place that reads a pod log or exit code for a run (controllers
`backup.rs`, `restore.rs`, `kafka_cluster.rs`, and any new check controller)
must verify the pod's controller owner reference UID equals the intended Job's
UID before trusting its output (defect SEC-PODLOG). New check code must not
copy the existing label-only pattern.

## S7 — RBAC: status writes use conditional merge PATCH

No controller path may use PUT (`update`) on a custom resource or its status
subresource. Status writes are merge patches carrying
`metadata.resourceVersion` as the precondition (defect P0-RESERVE). Any new
call must be covered by the reverse "every call has a grant" lint that
w0-reservation adds.

## S8 — New kinds

D2 adds `BackupDestination`, `TopicDiscovery` and `Preflight` under a new ADR
0008 Amendment F; D1 adds no kind. Any further kind needs its own recorded
amendment in `docs/architecture.md` before implementation, per Amendment A.

---

Documentation is licensed [CC-BY-4.0](../../LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
