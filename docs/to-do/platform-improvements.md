# Platform improvements tracker

Status: **implementation in progress**. Only tasks with explicit completion
evidence are Done; remaining tasks retain their proposed or recorded state.
Reviewed against main commit `92e02097540c39ff8565283a38ee592499b95020` on
2026-09-14. This tracker consolidates the operator, product, UI and architecture
review into 20 implementation spikes and 41 independently assignable tasks.
Recheck the current repository before starting: later changes may satisfy part
of a task. Preserve these IDs when updating status or splitting work.

## Starting point and boundaries

Already delivered: documentation consolidation, SCRAM connection-configuration
and Secret-reference reuse between probe and backup, and the simplified main
CI/publication pipeline. Main CI passed and published the reviewed commit's
runner, controller and UI images. These are foundations to preserve, not new
work to claim through this tracker.

Keep the Rust execution core, Kubernetes reconciliation, isolated runner Jobs,
static asset packaging and signed execution evidence. Reuse saved connection
settings and credentials; independent pods cannot share a live TCP socket.
Keep Kubernetes as execution-state storage and object storage as durable
recovery metadata. Introduce another database only after measured requirements
justify it. A frontend framework change alone cannot fix missing product APIs
or recovery semantics.

Use **All user topics** for dynamic topic-data protection. Current restores
replay records into newly named topics using manifest partition counts and a
configured target replication factor. They do not promise automatic consumer
cutover, complete source configuration reconstruction, continuous recovery or
full Kafka platform recovery. Record-byte verification is sampled, alongside
count and configuration checks; some archives have weaker evidence. Signed
reports authenticate those claims, not an exhaustive comparison of every byte.
Current restore checkpoints are pod-local and do not establish crash resume.
Future metadata recovery, continuous protection and advanced recovery modes
belong in [product expansion](product-expansion.md), not this baseline tracker.

## Execution rules and completion contract

- Orchestration (updated 2026-09-15): the lead orchestrator dispatches bounded
  workers and does not self-certify their output. The 2026-09-14 batch ran as
  Codex workers (Astra orchestrating, GPT-5.6 Sol/Terra implementing) and
  stopped at that service's usage limit on 2026-09-15; work continues with
  Claude Code subagents under the same ownership, independent-review and
  evidence rules. Complete this platform tracker before beginning
  implementation of product expansion.
- User disabled custom and third-party skills, including gstack and graphify.
  Use direct reasoning and standard tools; do not apply those skill workflows.
- Work on a single task or a named dependency group. Record its owner, state,
  starting commit, decisions, affected contracts and next action in its handoff.
  States are Proposed, In progress, Blocked and Done. A spike investigation
  must end in an implementation decision and runnable acceptance criteria,
  not only a design discussion.
- Confirm current behavior first. Do not replace earlier user changes or claim
  a proposal is deployed. Keep per-run plans immutable, references scoped to
  the correct namespace, credentials out of browser responses/logs/downloads,
  and old signed archives verifiable. Make changes backward compatible where
  possible; document conversion, rollout and rollback when they are not.
- Every local Kubernetes deployment and E2E operation must explicitly target
  **docker-desktop**. Do not modify a company EKS cluster or another context.
  Use isolated test resources and clean up only resources owned by the test.
- Use the task-specific tests below plus the relevant existing checks. Preserve
  the simplified CI pipeline; add behavior tests to existing suites rather
  than another mandatory workflow or duplicated full-suite gate. Use a code
  reviewer for code changes and a Rust reviewer for Rust changes.
- A task is Done only when its acceptance criteria pass, relevant documentation
  is updated, migration/rollback implications are recorded, reviews are
  resolved, and the handoff names the tested commit and evidence. Report
  unsupported cases and unrun tests explicitly. A mock-only result is not a
  passed Kubernetes or browser journey.
- Per-task dependencies below indicate prerequisites for final integration.
  Contract design and tests may proceed independently against an agreed fake
  boundary. Handoffs must name the remaining dependency rather than marking a
  partially integrated feature Done.

## Delivery order

1. Correctness first: PLAT-01 through PLAT-06 and PLAT-13. Begin API and trust
   design in PLAT-17/19 alongside these changes.
2. Everyday use: saved connections/destinations, discovery, schedule detail and
   restore flows in PLAT-07 through PLAT-12, using PLAT-18 contracts.
3. Operational recovery: status, catalog, retention and trust integration in
   PLAT-14 through PLAT-16 and PLAT-19.
4. Shared-console release and scale: complete PLAT-17/18, then PLAT-20.
   PLAT-20's regression work accompanies earlier tasks rather than waiting
   until the end.

## Active execution ledger

Current state (2026-09-15). Source candidate `a2bf5220e92d7c712ff77bf44c47d6fbabd1e9bc`
failed CI run [34936909533](https://github.com/VladyslavHaina/logweir/actions/runs/34936909533)
(label gate and a stale invalid-signer guard). Commits `44f1c2d` through
`4956785ba0f6` committed the backend live harness and fixed those failures;
CI run [35019727967](https://github.com/VladyslavHaina/logweir/actions/runs/35019727967)
passed check, e2e and publish for `4956785`. The Codex batch below stopped
at its usage limit: backend-live-close and plat06-implementation ended without
terminal reports, and backend-live-close left namespace
`logweir-backend-close-20260915` behind. Its interrupted PLAT-06.1 worktree
`/tmp/logweir-plat06-worktree` is preserved, unmerged and unreviewed.

Shared lab fixture (2026-09-17, `lab-refresh`): `weirkeeper:scram-reviewed` and
`logweir:scram-local` were rebuilt from main `c1d3411` with the unmodified
Dockerfiles (revision labels now present; the previous, label-less images from
2026-09-14 are kept as `weirkeeper:scram-reviewed-20260914` and
`logweir:scram-local-20260914` for a documented tag-and-restart rollback); all
fourteen CRDs installed, every UID and generation unchanged; 17 of 18 lab objects
kept their phase and the 18th changed only as PLAT-07.1 predicts
(`missing-reference` → `CredentialNotRenderable`); a manual Backup ran to a
`Valid` receipt on the new runner. Both tags are mutable under `imagePullPolicy:
Never`, so a worker rebuilding them changes the lab without a Deployment diff —
the revision label is how drift is detected until the fixture is pinned.

Shared lab fixture (2026-09-18, `lab-refresh-2`): both images rebuilt from main
`e7d0e79` (`weirkeeper:scram-reviewed` `sha256:dcb63a07…`, `logweir:scram-local`
`sha256:930c304e…`; the `c1d3411` pair kept as `-c1d3411` tags), the CRDs applied from
`config/crd` (14 kinds; exactly the five changed files moved a generation — `backups`,
`backupschedules`, `recoverycatalogs`, `retentionpolicies`, `trustpolicies` — every UID
unchanged), the `weirkeeper` ClusterRole 7 → 26 rules and `logweir-trust-admin` created
(every grant verified with `auth can-i` plus negative controls), the `weirkeeper-policy`
ConfigMap applied with its `LOGWEIR_POLICY_CONFIGMAP`/`LOGWEIR_INSTALLATION_NAMESPACE`
wiring patched into the lab Deployment (the chart renders both; the lab Deployment predated
them), the VAP not applied (values off, no console ServiceAccount); one pod,
`"controllers":14`, no `Forbidden` in two minutes; `test-plat06-live.py` 7/7,
`test-plat07-live.py` 6/6, the forged-subject journey 20/20. Five 2026-09-14 objects went
`Valid` → `Untrusted` (TRUST-UPGRADE-SIGNEDAT). Report `claude/lab-refresh-2.result.md`.
The three live waves then ran against it: D1 W8 (`b500190`, `scripts/live/d1/`), D2 W14
(`d3cc637`, `e2e/k8s/d2/`), D3 W14 (`1ab43cd`, `e2e/k8s/d3/`), each with an independent review
that re-ran part of the evidence; no task reached Done — every unmet acceptance item and
every defect is recorded under its task and in the defect table.

Shared lab fixture (2026-09-18, `lab-refresh-3`): both images rebuilt from main `c6422a7`
(controller `36bb4d0b…`, runner `6c88521f…` — the runner now carries `logweir-retention`,
proved by `check-image.sh` check 7; the `e7d0e79` pair kept as tags, four pairs recoverable),
the CRDs applied (only `backups` 5 → 6 and `restores` 3 → 4 moved; every UID unchanged), the
chart's roles, binding and policy ConfigMap re-rendered and diffed — unchanged — and the
insecure-sink hatch enabled on the Deployment for the echo-sink row; `"controllers":14`, no
`Forbidden` in two minutes; `test-plat06-live.py` 7/7, `test-plat07-live.py` 6/6 (after
regenerating the lab's expired 2026-09-16 certificates), the journey 20/20. The committed
harnesses then re-ran every row the wave-9 fixes were made for: ten of the eleven defect
rows CLOSED live (the defect table carries each row's evidence); TRUST-UPGRADE-SIGNEDAT did
not — the lab's five objects carry an intermediate-build `trust` block with `basis: "None"`
and no `signedAt`, a third shape the repair's discriminator missed, being fixed on
`claude/fix-trust-upgrade-2`. Rows that asserted the pre-fix behaviour now fail and are being
realigned (`claude/harness-refresh`). Reports `claude/lab-refresh-3.result.md`,
`claude/lab-refresh-3.review.md`.

Shared lab fixture (2026-09-18, `lab-refresh-4`): both images rebuilt from main `7b4fae9`,
the CRDs applied (`backups` 6 → 7, `preflights` 1 → 2, `restores` 4 → 5 — the frozen
destination block, the `SourceConnection` operation and the trust backoff fields; every UID
unchanged), roles and policy re-rendered and unchanged, `"controllers":14`, 7/7, 6/6, 20/20.
Every wave-10 item proved live and independently re-verified: the trust self-heal on the lab's
own five objects and a planted copy (TRUST-UPGRADE-SIGNEDAT closed), the frozen destination
block and a reachable `RecoveryPointLocationMismatch`, the `SourceConnection` kind on the lab
controller and the console's dialling control, the realigned harness rows (D3 27/0, D2 3/3,
D1 3/3), S14e through a fenced controller and S18 in an isolated `TrustPolicy`. The review's
Done verdicts: PLAT-03.1 and PLAT-07.2 yes; 03.2, 05.1, 16.1, 16.2 and 19.1 not yet — rows
proven only on `e7d0e79` under code rewritten since, a vacuous rollback clause, and tests with
no row (see each task's record). New defect: TRUST-EXPIRY-LAG. Reports
`claude/lab-refresh-4.result.md`, `claude/lab-refresh-4.review.md`.

Shared lab fixture (2026-09-18, `lab-refresh-5`): both images rebuilt from main `d387f87`
(no CRD, role or policy file moved since `7b4fae9`, so nothing was re-applied), `"controllers":14`,
7/7, 6/6, 20/20. The re-proof run: PLAT-03.2's five rows that had been proven only under
rewritten code all PASS on this build with S16 and E4; PLAT-05.1's rollback is a real
controller swap and the concurrent edit/fire runs both orderings, fenced with the drift check
enforced; PLAT-16.1's six rows PASS; PLAT-16.2 seven PASS; PLAT-19.1's overlap, retirement,
revocation and CEL rows plus two rows no harness had (`ConsoleConfirmation` usage,
self-approval denial); TRUST-EXPIRY-LAG closed (refusal 4 ms after `notAfter`; the review's own
probe 9 ms, 0 of 56 post-boundary samples `True`). Four honest FAILs: the bounded-retry row
(product — the counter reaches 3 and scheduling stops, but a merge patch of the conditions
array wiped `EnforcementDegraded`; fixed on `claude/fix-retention-degraded-2`), and three
harness rows (an inverted guard that outlived its fix, a non-idempotent re-seed, a bulk
fixture's convergence race; fixed on `claude/harness-rows-3`). The review's Done verdicts:
PLAT-16.1 yes; 03.2 no (the expired-approval test has no row on any build), 05.1 no
(L-05.1-1 and D1's negative control not run on this build), 16.2 no (bounded retry), 19.1 no
(unauthorized update as an RBAC result, old archive, multiple namespaces, the keys view).
Reports `claude/lab-refresh-5.result.md`, `claude/lab-refresh-5.review.md`.

Shared lab fixture (2026-09-19, `lab-refresh-6`): both images rebuilt from main `af64073`
(no CRD, role or policy file moved; the apply touched only last-applied annotations),
`"controllers":14`, 7/7, 6/6, 20/20. Re-proved on this build and independently re-verified:
PLAT-03.2's seven rows (S19, S15, S15b, S17, S21, S16, E4) with harness-rows-6's real
expired-approval S18 gated on the image carrying the preflight fix; PLAT-19.1's overlap,
retirement, revocation, CEL, RBAC unauthorized update, old archive (verify half, `Historical`),
the expiry boundary (refusal 4 ms after `notAfter`, 0 of 43 post-boundary `True`),
`ConsoleConfirmation` usage, self-approval denial and multiple namespaces; PLAT-16.2's rows
from the published runner image except bounded retry — `EnforcementDegraded` is now published
with its words and a spec change releases the budget, but the count reached 3 after two Jobs
(RET-COUNT-EARLY: a second harvest of one Job through `start_run`'s 409 arm and a plan digest
oscillating through `previously_refused()`). The review's Done verdicts: PLAT-03.2 yes (with
three stated residues); 16.2 no (bounded retry); 19.1 no — the keys view renders absent expiry
information as `valid` (KEYSVIEW-ABSENT-VALID), which the acceptance forbids, until the D3 W12
console lands. Reports `claude/lab-refresh-6.result.md`, `claude/lab-refresh-6.review.md`.

Claude orchestration recovery: worker rules, reports and artifacts are under
`/tmp/logweir-roadmap-run/claude/`; workers use isolated worktrees under
`/tmp/logweir-roadmap-run/wt/` and serialize docker-desktop operations that
change shared or cluster-scoped state through `claude/k8s-lock.sh`. This table
records dispatch and next evidence, not completion.

Resumption 2026-09-16 (Claude Fable 5.1 orchestrating): the 2026-09-15 session
ended without terminal reports for plat07, plat17-api, plat01-02-live and
d1w1-cadence, left a 13-hour hung mutation test in `wt/plat17-api` (killed,
source restored), a stale cluster lock (released) and an un-reverted mutant in
`wt/w0-reservation` (discarded; the branch is merged into `claude/plat06`).
Wave 2 resumes every branch in place with the prompts under
`/tmp/logweir-roadmap-run/claude/prompts/`; the table names the current worker.

| Tasks | State | Worker | Boundary and next evidence |
| --- | --- | --- | --- |
| PLAT-01.1 / 01.2 / 02.1 / 02.2 | Done | plat01-02-live-finish, plat01-02-live-review | Completion records under PLAT-01 and PLAT-02. Integrated into main as `4b7a1f6` (chart bootstrap digest pin) and `fbd124e` (the two live harnesses). |
| PLAT-13.1 | Done | closure-0413 | Completion record under PLAT-13.1. |
| PLAT-04.1 | Done | closure-0413, w0-reservation, plat06-live, plat06-review | Completion record under PLAT-04.1. The two gaps closed live in `10f6c28`'s run: the deleted-Job case (e) and the reservation under the unmodified shipped role (c). |
| PLAT-04.1 defect (P0) | Fixed (`bdd26dc`, live-proved) | w0-reservation → plat06-live | `backup_schedule.rs:1359` reserves a Forbid slot with `replace_status`, which RBAC authorizes as `update` on `backupschedules/status`; the shipped role grants only `patch` (`config/rbac/role.yaml:118`, `charts/logweir/templates/clusterrole.yaml:29`). Confirmed on docker-desktop: the lab ServiceAccount has `update` no, `patch` yes, so with the default `Forbid` policy no scheduled Backup is created on a shipped install. PLAT-04.1's live run used custom namespace Roles and never exercised this. Fix: resourceVersion-conditional merge PATCH, an audit of every other call against the shipped role, a reverse "every call has a grant" lint with mutant evidence, and live proof under shipped RBAC. |
| PLAT-06.1 | Done | plat06-live, plat06-review | Completion record under PLAT-06.1. Integrated into main as `8e362f9..10f6c28`. |
| PLAT-07.1 | Done | plat07-finish, plat07-integrate, plat07-review, plat07-live | Completion record under PLAT-07.1. Integrated into main as `6c534b2..199020a` plus the live harness `50e641f`. | Versioned connection contract, one shared resolver for probe/backup/restore Jobs, TLS private CA, rotation, redaction, write-only credential builder; live SCRAM rotation and TLS cases. |
| PLAT-07.2 | In progress | ui072, ui072-review | Partial record under PLAT-07.2. Integrated into main as `8bbe4d1..b65f23f`. "Test connection" cannot yet force a re-probe (D2 W13). | Saved-cluster selector by UID, probe vocabulary, freshness budget; live 20/20 |
| PLAT-13.2 | Done | ui-correct, ui-correct-review, ui-correct-fix | Completion record under PLAT-13.2. Integrated into main as `2a34abd..8020876`. |
| PLAT-11.1 | Done | ui-restore-selection, ui-restore-selection-review | Completion record under PLAT-11.1. Integrated into main as `6c2c95e..02426c8`; the same branch fixes the `allowHttp` half of UI-HTTPDOWNGRADE (D2 W13a). |
| PLAT-18.1 | Done | ui-typed-client, ui-typed-client-review | Completion record under PLAT-18.1; D0 stage 6 (static client migration) done for its own scope. Integrated into main as `fa73824..48d5ec0`. |
| PLAT-12.1 (immediate slice), PLAT-12.2 (subject slice) | In progress (slices landed) | ui-correct | The guided submit, idempotent durable Restore and subject binding landed with PLAT-13.2 (records under each task); remaining: PLAT-11.2/13.2-backed selection flow and PLAT-19.2 policy routing for 12.1, retry identity for 12.2. |
| PLAT-17.2 (stage 2) | In progress (stage landed) | plat17-2-authz, plat17-2-authz-review | Partial record under PLAT-17.2. Integrated into main as `24752f4..90ecd0c`. Shared mode is implemented and live-verified locally but not deployable or declarable secure until D0 stages 5 and 7. |
| PLAT-17.1 (stages 1 and 3) | In progress (stages landed) | plat17-api-finish, plat17-api-review | Partial record under PLAT-17.1. Integrated into main as `4b571d1..de0207c`. Remaining for Done: console image and chart with the API's own RBAC (D0 stage 7), transient-check cancellation once PLAT-03/09.1 exist, `POST …/backups` (PLAT-06.2), SSE (PLAT-14.1), a browser journey through the API, and PLAT-17.2. |
| PLAT-05.2 | Done | d1w4, d1w8, d1-fence (+ reviews) | Completion record under PLAT-05.2; live on docker-desktop 2026-09-18 behind D1 §13.1's fence. |
| PLAT-04.2 | Done | d1w2, d1w6, d1w8, d1-fence, d1w7 (+ reviews) | Completion record under PLAT-04.2; live on docker-desktop 2026-09-18 incl. the console journey. |
| PLAT-09.1 | Done | d2w4, d2w8, d2w12, d2w13, d2w14, fix-discovery-results, lab-refresh-3 (+ reviews) | Completion record under PLAT-09.1; live on docker-desktop 2026-09-18. |
| PLAT-03.1 | Done | d2w4, d2w9, d2w12, d2w13, d2w14, fix-runner-checks, d2-source-check, lab-refresh-3/4 (+ reviews) | Completion record under PLAT-03.1; live on docker-desktop 2026-09-18. |
| PLAT-07.2 | Done | ui072, d2w13, d2w14, d2-source-check, ui-conn-followups, lab-refresh-4 (+ reviews) | Completion record under PLAT-07.2; live on docker-desktop 2026-09-18 incl. the dialling control. |
| PLAT-16.1 | Done | d3w9, d3w14, fix-retention, harness-refresh, harness-rows-2, lab-refresh-3/4/5 (+ reviews) | Completion record under PLAT-16.1; live on docker-desktop 2026-09-18. |
| PLAT-05.1 | Done | d1w2, d1w6, d1w7, d1w8, d1-fence, harness-refresh, harness-rows-2/4, fix-trust-upgrade(-2), lab-refresh-4/5 (+ reviews) | Completion record under PLAT-05.1; live on docker-desktop 2026-09-18/19 incl. a real rollback. |
| PLAT-03.2 | Done | d2w4, d2w9, d2w13, d2w14, fix-runner-checks, d2-status-destination, fix-preflight-approval, harness-rows-6, lab-refresh-3/4/5/6 (+ reviews) | Completion record under PLAT-03.2; live on docker-desktop 2026-09-19. |
| PLAT-06.2, 09.2 | In progress (every D1 wave landed; PLAT-04.2, 05.1 and 05.2 Done; 06.2 waits on the CLI/API/UI demonstration and the legacy grant, 09.2 on L-09-3a/3b through the fence and the authorizer broker for L-09-5) | [D1](decisions/D1-backup-scheduling.md) | Cadence/time zone, editable policy with per-run snapshots, retained history, dynamic selection, manual runs; nine worker tasks. W1 (the pure cadence engine) landed in main as `6eedc0a..4b54a5c` after review; see the PLAT-04.2 partial record. W3a (the `Backup` run contract: `spec.trigger`, `spec.scheduleRef` with generation and `runPolicySha256`, the selection type with `allUserTopics` requiring `incompleteDiscovery`, `status.selection`, `src/identity.rs`, `src/policy.rs`, the §3.4 vocabulary) landed in `696c81a`/`b334a98` inside `crds-shapes`; a `Backup` naming both `topics` and `allUserTopics` is refused by CEL and, terminally, by admission. W3b (the reconciler consumes the contract; grammar `v2`) landed at `397e37d`; see the PLAT-04.2 partial record. W2 (editable policy, the §4.5 scheduler; `destinationRef` editable) landed at `98257f8`; see the PLAT-05.1/04.2 partial record. W5 (dynamic selection per run through the check runner) landed at `7072b9d`; see the PLAT-09.2 partial record. W6 (cadence previews, `PUT schedules`, `POST backups`) landed at `72751ab`; see the PLAT-06.2 partial record. W4 (retained history; runs detached from the schedule's ownerReference) landed at `6845ffb`; see the PLAT-05.2 partial record. W7, W8 remain. |
| PLAT-08.x | In progress (every D2 wave landed; PLAT-03.1, 03.2, 07.2 and 09.1 Done; 08.1 waits on U6's minimal-permission table, 08.2 on its inheritance/validation rows) | [D2](decisions/D2-destinations-discovery-readiness.md) | `BackupDestination`, `TopicDiscovery`, `Preflight`, one shared check runner; sixteen worker tasks. W1 (pure check contract and destination model) and W2 (explicit store options) landed as `c13b0cc..56bd074` after review (ACCEPT after two high and three medium fixes: JSON-form redaction bypass, ambient credentials inheriting the environment). W3 (`logweir_kafka::inventory`: bounded targeted describe, broker count, validate-only `CreateTopics`, error classification where an observed authorization failure makes visibility `limited` and anything unknown is failure, with a real admin-client fault capture because rdkafka 0.36 never invokes `ClientContext::error` for a metadata-only workflow — D2 §4.2 `[VERIFY U5]` corrected) and W5 (`weirkeeper::check`: check Jobs mirroring the execution pod, pod selection by controller owner UID only, framed-stdout relay through the W1 decoder, the full waiting-code table, TTL, plan/chunk/limit modules, the installation policy loader failing closed) landed as `23cec50..b8e62d1` after review (ACCEPT after one high and three medium fixes; 19 mutants killed; the rebase over PLAT-07.1 then routed the inventory client through the reader's `client_config`, removing a drifted copy that could upgrade plaintext to TLS when a CA was present — re-checked ACCEPT; weirkeeper 435, kafka 57). RBAC still owed by W11: `events: list` plus its `manifest_lint` row, the three new kinds' verbs, and a decision on `gc.rs`'s deletes. The reviewers' SEC-PODLOG finding against `controllers::backup::select_job_pod` is closed by `secpodlog` (see the defects table). W6a and W6b (the three Amendment F kinds and the destination sentinel on existing kinds) landed in `46880a3`/`88232f5`/`b334a98` inside `crds-shapes`; W7 (destination resolver, controller, evidence store cache) landed as `27fb924..0b25e95` (see the PLAT-08.1 partial record); W4 (runner `logweir check run`) landed at `537657d` (see the PLAT-03 partial record); W8 (`TopicDiscovery` controller) and W12 (API routes) are in review or in progress. W9, W10, W11, W13, W14 remain. |
| PLAT-14.x, 15.x, 16.2, 19.1 | In progress (W0–W10 landed; W14 live runs 2026-09-18 and the wave-9/10/11 fixes; PLAT-16.1 Done; 16.2 waits on the degraded condition's live proof, 19.1 on RBAC/old-archive/multi-namespace rows and the keys view (W12), 15.1 on paging/partial access, 14.1/14.2/14.3 on the console waves W11/W12) | [D3](decisions/D3-status-catalog-retention-trust.md) | Operation states, protection freshness, rehearsals, durable catalog, retention enforcement boundary, trust lifecycle; fifteen worker tasks. W4 (`d3-notify`: the shared notification module and `logweir notify deliver`) landed after review (ACCEPT after two high fixes); W3 (`d3-catalog-writer`: signed catalog point records, `list_page`, `logweir catalog sync|list`) landed after review (see the PLAT-15.1 partial record); W0 (the five Amendment G kinds, additive run status, `Restore.spec` additions, the `Approval` enum) landed in `496451a`/`88232f5`/`b334a98` inside `crds-shapes`; W1 (trust lifecycle core, `TrustPolicy` controller, `trust export|migrate-roster`, G8) landed as `64fcd38..5fc1a72` (see the PLAT-19.1 partial record); W8 (`RecoveryCatalog` controller) in progress. W2, W5, W6, W7, W9, W10, W11, W12, W13, W14 remain. |

### Decision records

Spike outcomes are recorded under [decisions/](decisions/) and are binding on
implementation: [D0](decisions/D0-product-api-and-identity.md) (product API,
identity and the ordinary-versus-governed approval seam),
[D1](decisions/D1-backup-scheduling.md), [D2](decisions/D2-destinations-discovery-readiness.md),
[D3](decisions/D3-status-catalog-retention-trust.md), and
[D-SEAMS](decisions/D-SEAMS.md), which resolves conflicts between them: one
check runner rather than two, discovery results are never execution inputs, one
completeness vocabulary, one frozen execution-inputs grammar, transport
security is never derived, pod identity is verified by owner UID, status writes
use conditional merge PATCH, and a new kind needs a recorded amendment.

Amendments recorded at integration (binding on later workers, detail in
`/tmp/logweir-roadmap-run/claude/d2-core.result.md` §7 and its review): D2 W1/W2
landed in main as `c13b0cc..56bd074` with these deviations from D2's text —
`DestinationRole` lives in `logweir_core::destination` (W7 re-exports it);
`validate_ca_bundle` is a separate function from `validate`; `StaleReason` gains
a sixth, additive `inputsDigestChanged` (§6.6's list is no longer closed at five);
`CheckRequest` is externally tagged on the wire (W4 and W5 must serialise plans
that way); `StoreOptions` carries `request_timeout` and `max_retries`;
`Store::from_url` is byte-compatible for legacy callers and no credential source
inherits endpoint, region, addressing or transport from the environment (closing
SEC-ENVHTTP at the store layer once controllers render explicit options).

D2 and D3 add eight kinds (`BackupDestination`, `TopicDiscovery`, `Preflight`,
`TrustPolicy`, `ProtectionPolicy`, `RehearsalSchedule`, `RecoveryCatalog`,
`RetentionPolicy`) under new ADR 0008 amendments, each justified by an
authorization or lifetime boundary rather than by convenience. A kind ships
only together with its controller, RBAC and documentation; an unserved schema
is not a delivery.

Schemas landed (2026-09-16, `crds-shapes`, main `46880a3..b334a98` after review):
all eight kinds with their CEL rules, ADR 0008 Amendments F, G, H and I in
`docs/architecture.md`, the destination references and sentinel on the three
existing kinds (D2 W6b), the additive run status blocks, `Restore.spec.
{authorization,runnerResources}` and the `Approval` subject enum (D3 W0), and the
D1 run contract on `Backup` with `crds/selection.rs`, `src/identity.rs` and
`src/policy.rs` (D1 W3a). Every named CEL rule is pinned by presence in the
generated bytes (`crd_shape::every_named_cel_rule_ships_in_its_crd`, 48 rules)
and was probed live on docker-desktop (59/59 cases; two rule shapes the API
server refused were fixed: `TrustPolicy`'s per-key checks moved to per-item
transition rules under the CEL cost budget, and `RehearsalSchedule`'s
`topicPrefix` grammar). The kinds are served by no controller yet, so by the
rule above they are not deliveries: W7/W8/W9 (D2), W6/W7/W8/W9/W10 (D3) and W2
(D1) remain. Deviations recorded in `claude/crds-shapes.result.md` §7 (eight:
`Preflight` per-check `detail` omitted, the two live-forced reshapes,
single-variant enums for `selection`/`concurrencyPolicy`, `runnerResources`
ceilings pattern-only until the rehearsal controller enforces them, the
emitter's numeric-bound normalisation, `BackupSchedule` rules left to D1 W2).
Rollback note: widening `Approval.spec.subjectRef.kind` with `RehearsalSchedule`
is the one change an older controller cannot decode; `docs/kubernetes.md`
records the order and check. The per-item transition rules were proved on the
lab's 1.34 API server, not on the 1.29 floor — a floor check is owed before a
release.

### Defects found by the 2026-09-15 contract spikes

Each was confirmed against current source (and, where marked, against the live
cluster). They are recorded here so no finding depends on a worker report
surviving. None is fixed yet except where a worker is named.

| Id | Defect | Owner |
| --- | --- | --- |
| P0-RESERVE | `backup_schedule.rs:1359` reserves a slot with `replace_status` (PUT, verb `update`) while the shipped role grants only `patch` on `backupschedules/status`; default `Forbid` schedules therefore never create a Backup on a shipped install. Confirmed live (`auth can-i`: update no, patch yes). **Fixed** in `bdd26dc` (resourceVersion-conditional merge PATCH) with the reverse "every call has a grant" lint (`40fd7cb`); live-proved under the unmodified shipped role in the PLAT-06.1 run (reservation set at rv 2332382, cleared at 2332386). | w0-reservation, plat06-live (done) |
| SEC-ENVHTTP | The controller forwards its own `AWS_ENDPOINT_URL`, `AWS_REGION`, `AWS_ALLOW_HTTP` and `AWS_VIRTUAL_HOSTED_STYLE_REQUEST` into every runner Job (`controllers/backup.rs:192`, `restore.rs:1297`); the engine's `from_env()` then honours them, so a forwarded `AWS_ALLOW_HTTP=true` enables plaintext transport even when the approved plan says `allow_http: false`. A global setting overrides approved execution inputs. Store-layer half closed by D2 W2 (`e86ea4a`: no credential source inherits endpoint, region, addressing or transport from the environment); the controller still forwards the variables and PLAT-06.1 now freezes them into the snapshot, so the controller/runner half stays open. **Closed for destination-backed runs** by D2 W10 at `f4ab8a3` (the resolver's explicit set, never the controller's environment, pinned by a planted-env row); **open by design for legacy inline-archive runs**, which keep the forwarding so upgrades do not break — the remaining surface is documented in `docs/kubernetes.md` §20–22. | PLAT-08.1 destination resolver (D2 W7/W10) |
| SEC-PODLOG | Pod lookup for exit codes and evidence keys matches on labels alone and takes the first result (`controllers/backup.rs:1683`, `restore.rs:2519`, `kafka_cluster.rs:832`); a tenant able to create a pod with `batch.kubernetes.io/job-name=<job>` can have its log read as the run's outcome. The pod's controller owner UID is never checked. **Fixed** (`secpodlog`, main after `93d403c`): every pod read in the three execution controllers goes through one `check::pod::find_owned_pod_by_selectors` — an owner reference with kind `Job`, `apiVersion batch/v1`, the Job's UID and `controller: true` is required, no label or ownerless fallback exists, no Job UID means no listing, and because owner references are author-written, more than one claimant is fail-closed as the terminal, non-retryable `PodOwnershipContested` rather than resolved by age; eleven mutants killed, review ACCEPT after fixes. Owner-reference shape is asserted by fixtures; the next live controller leg should assert the real pod's `ownerReferences` and plant a forged claimant (D2 S20 plants only a bare pod). | fixed; live arm owed to the next controller leg |
| UI-HTTPDOWNGRADE | The restore wizard sets `allowHttp` from the path-style checkbox (`ui/pages/restore-wizard.js:1142`) and applies one endpoint/region/addressing to both the source archive and evidence store (`:1135`). **First half fixed** in `6c2c95e` (D2 W13a): path-style never sets `allowHttp`; an explicit, separate "allow insecure HTTP" control defaulting off is the only source of `allow_http: true`, guarded by a behaviour row and a live journey that reads the plan bytes the API server holds. The one-endpoint-for-archive-and-evidence half remains PLAT-08.2. | PLAT-08.2 UI slice |
| UI-FAKEPREFLIGHT | Wizard step 5 "Target-topic preflight" shows only the target cluster's cached `status.reachable` (`ui/pages/restore-wizard.js:424`), and restore admission gates on the same cached value (`controllers/restore.rs:638`). **Fixed by D2 W13 (`claude/d2w13.result.md` §3.6): wizard step 5 creates a real `Preflight` bound to the plan hash on screen and renders its rows, and restore admission gates on the Preflight's verdict; proven live in D2 W14's wizard journeys and on lab-refresh-6 (S15, S15b: a new-target conflict after a green preview ends `GuardRefused`, `exitCode 3`). Row closed 2026-09-19 with PLAT-03.2's completion record.** | PLAT-03.2 |
| RET-WRONGBUCKET | Retention lists manifests through the controller's single global store while rendering commands for the schedule's own URL (`backup_schedule.rs:1417`), so a schedule on another bucket is reported against the wrong catalog. Since D1 W2 (2026-09-17) `destinationRef` is editable, so the wrong-bucket case is also reachable by an edit between runs; retention must evaluate per frozen run, not the schedule's current URL. **Fixed** by D3 W9 at `a3af420`: membership is a private newtype keyed on the frozen run's destination, the legacy report is replaced by a note with empty set lists, and every evaluation names the destination it describes. | PLAT-16.1 |
| FLAKE-APISHUTDOWN | `a_held_connection_does_not_block_shutdown_past_the_grace_period` (`crates/logweir-api/tests/local_admin.rs`) fails with `ConnectionReset` when several cargo test runs share the host and passes in isolation (2/2 on 2026-09-17 at the D2 W12 rebase); the held connection's read `unwrap`s, so a reset past the grace deadline — the behaviour under test — is reported as an error. Also seen under the same load: `the_binary_serves_loopback_and_stops_on_sigterm` ("answers within five seconds: ConnectionReset", the d2w9 integration gates, 2412/1) and once `a_non_loopback_listener_is_refused_before_anything_is_bound`; the suite is 12/12 alone. Fix: **the diagnosis above is wrong and is corrected here.** Reproduced 2026-09-17 by running five copies of the test binary concurrently: 5 failures in 15 runs across **four** tests (`the_binary_serves_loopback_and_stops_on_sigterm`, `an_oversized_request_head_is_refused`, `the_connection_ceiling_holds_and_then_releases`, `a_non_loopback_listener_is_refused_before_anything_is_bound`), every one a socket belonging to another run and every one returning in under 13 s — so no failure was a timeout, and neither a longer `SERVE_LIMIT` nor accepting a reset would have fixed any of them; the test this row is named after did not fail at all. One cause: `free_port()` reports a port nothing is listening on and cannot reserve it, so a concurrent process takes it before the child's `bind`, and a successful connect proves only that *someone* is listening. **Fixed** (`flake-localadmin`, `e0d729f`, branch `claude/flake-localadmin`): no test infers a server from a port — `start_server` waits for the child's own `logweir-api started` line naming that exact port and restarts on a fresh port when the child reports `cannot bind the listener`; `http_get` retries a transport failure until `SERVE_LIMIT`; the held-connection test retries its keep-alive setup instead of `unwrap`ing it and asserts the post-exit reset/EOF as the pass, a reset before the signal as the failure; the non-loopback test asserts "nothing was bound" from the child's stdout, which also catches a bind made and closed again (its old probe ran after the child was dead, so it could only ever have seen a foreign listener). `SERVE_LIMIT` stays 40 s. Evidence at load average 20–26: 15/15 under the concurrency that failed 5/15, 10/10 under `cargo build -p weirkeeper` plus four concurrent suites (44 further runs, 0 failures), 3/3 alone. Test file only; no production change. Review ACCEPT (`flake-localadmin-review`, 0 critical/high/medium, 4 low follow-ups): independently re-ran 34 suite executions under the same concurrency with 0 failures, and re-proved the properties with four mutants — SIGSTOP before SIGTERM makes the shutdown genuinely hang and the held-connection test still FAILS in 26 s; a reset before the signal FAILS in 42 s with the directional message; the new post-exit reset/EOF assertion FAILS in 8 s when the connection neither ends nor resets; and removing `config::is_loopback` still FAILS the non-loopback test in 60 s. Fixed in `cd16ce6` (`crates/logweir-api/tests/local_admin.rs` only; review `claude/flake-localadmin.review.md` ACCEPT with four follow-up lows). | PLAT-17.1 |
| FLAKE-APICOOKIE | `the_sealed_cookie_contains_no_provider_material` (`crates/logweir-api`) fails about one run in eight hundred: `seal` uses a random nonce and base64url, so the literal `u-1` the test forbids can appear by chance inside the ciphertext. Seen once in the D3 W9 integration run on 2026-09-17 (60/60 in isolation, green on re-run). Fix (PLAT-17.2's owner): assert the provider material is absent by decoding the envelope, not by a substring over random bytes, or use a fixed nonce in the test. Recorded, not fixed. Second occurrence 2026-09-19 in the fix-retention-count merge gates (3185 passed, this one failed; 3/3 green on re-run) — the gate was accepted on the re-run and a structural fix is now assigned (`claude/fix-flake-cookie`). | PLAT-17.2 |
| ENGINE-PATHSTYLE | The pinned engine ignores `path_style` and forces path-style addressing whenever an endpoint is set (`kafka-backup-core storage/s3.rs:66`), so virtual-hosted addressing with a custom endpoint cannot be honoured and must be refused rather than advertised. | PLAT-08.1 (documented refusal) |
| STATUS-RECORDS | `Backup.status.records` is declared in `config/crd/backups.yaml` with a `RECORDS` printer column and is never written by any controller path; it is blank on every Backup the PLAT-06.1 and PLAT-07.1 live runs produced and on the lab's own scheduled Backup, while the counts exist in the signed receipt. Found by plat07-live. Either write it from the verified receipt or drop the field and column. **Fixed** by D3 W2 at `d69ca1a`: written from the verified receipt only, absent otherwise; `status.completion`/`status.teardown` remain declared-and-unwritten (owner needed). | PLAT-14.1 (D3 W2 status/progress) |
| RECEIPT-DUP | A Backup Job re-created from its frozen inputs (PLAT-06.1 case e) writes a second run-id receipt under the same execution id while overwriting the manifest at the same key; if the topic advanced between the runs, the first signed receipt's digests no longer match and a verifier reports it Invalid. Found by plat06-review (M1); run identity is idempotent, signed evidence is not. | PLAT-15.1 catalog / D3 W3 (point identity is content-derived from the receipt) |
| LINT-INLINE-CALL | The reverse RBAC lint (`manifest_lint::every_call_site_has_a_grant`) and its forward twin do not see an inline `Api::<T>::namespaced(…).delete(…)` call shape (plat06-review L1), so a future controller call written that way would escape both. | PLAT-20.1 regression set; fix alongside the next controller task that adds a call |
| D1-DISCOVERY-IMAGE | A dynamic `Backup`'s discovery Job names the compile-time image pin: `backup_selection::resolve` (`crates/weirkeeper/src/controllers/backup_selection.rs:1022`) never receives the process `RunnerImage`, the builder at `:1307` leaves `image: None` and `:1310` posts the Job directly, so the Job carries `job.rs:62`'s `RUNNER_IMAGE` digest with `imagePullPolicy: Never` (`job.rs:261`) — `ErrImageNeverPull`, then `DeadlineExceeded` → `TopicsResolved=False/DiscoveryFailed`. Every other Job builder is overwritten by its reconciler with `ctx.runner_image` (`kafka_cluster.rs:1123`, `topic_discovery.rs:1562`, `recovery_catalog.rs:635`, `retention_policy.rs:1784`); the runner Job of the same namespace and minute got `logweir:scram-local` and completed in 4 s. On a default chart install the discovery Job would name an unpublished digest under `Never` while `LOGWEIR_RUNNER_IMAGE`/`LOGWEIR_RUNNER_PULL_POLICY` reach only the runner Job, so PLAT-09.2's dynamic half is unusable on every installation. Evidence: `claude/d1w8.result.md` §6, `artifacts/d1-live/20260918t0330z/objects/L-09-1/`. Fixed 2026-09-18 in `86a18b1`: the process `RunnerImage` is threaded through `backup_selection::resolve → resolve_inner → start`, so the discovery Job carries the configured image and pull policy exactly as the runner Job does; a regression row asserts equality and a mutant reverts to the pin. The same branch (`91e455d`) also fixes the dynamic-Backup twin of D2-RESULTUNREADABLE at `backup_selection.rs:1425` (the relayed `CheckCode` projects through `discovery_failure_state`: an unreachable broker is `DiscoveryFailed`, retryable, not `DiscoveryResultUnreadable`). Review `claude/fix-backup-ctl.review.md`. Live proof: CLOSED 2026-09-18 on the lab at `c6422a7` — the discovery Job ran the configured image; L-09-1/2/4's product half (`claude/lab-refresh-3.result.md` §8, independently re-verified in `claude/lab-refresh-3.review.md`). | PLAT-09.2 (D1 W5) |
| D2-RESULTUNREADABLE | A `TopicDiscovery` (and the API's timeout path) whose check Job relays a classified `notReady` result — `connection.authenticated` `BrokerUnreachable` for an unreachable bootstrap, `AuthenticationFailed` after a broker-side password change — ends `Failed` with `status.reason: ResultUnreadable`: the `CheckPhase::Succeeded → commit()` path in `crates/weirkeeper/src/controllers/topic_discovery.rs` treats "no inventory" as unreadable before it reads the relayed check codes. The pod's own frame carries the right code, message and remedy (`d2-live/<ts>/objects/s11/td-timeout-relayed.json`). Consequence: the console cannot tell an unreachable broker from a rejected password (PLAT-09.1 timeout and rotation tests FAIL; PLAT-17.1's timeout half PARTIAL). Evidence: `claude/d2w14.result.md` §5.1, S11/S12/api T2. Fixed 2026-09-18 in `1da8faf` (+ rows `1fa05cf`/`e3f7466`, docs `8f53bd1`/`0c4812b`): `commit()` binds the whole relayed document and projects the first blocking non-`ready` check through `check_contract::aggregate`'s precedence (`notReady` before `unknown`; advisory and `executionOnly` rows excluded) as `status.reason` with message and remedy through the existing redaction and 512-byte cap; `ResultUnreadable` only when no readable frame was relayed. Review `claude/fix-discovery-results.review.md` ACCEPT-WITH-FIXES (low) then closed; 14/14 mutants. The dynamic-Backup twin at `backup_selection.rs:1425` is fixed under D1-DISCOVERY-IMAGE's branch. Live proof: CLOSED 2026-09-18 on the lab at `c6422a7` — S11 `BrokerUnreachable`, S12 `AuthenticationFailed`, api T2 (`claude/lab-refresh-3.result.md` §8, independently re-verified in `claude/lab-refresh-3.review.md`). | PLAT-09.1, PLAT-17.1 |
| D2-PREFLIGHT-PREFIX | The restore preflight builds `archive.backupSet`'s `manifestKey` as `<id>/manifest.json` and ignores the destination's `storage.prefix`, so a destination with `prefix: team/prod` answers `notReady/AccessDenied` for a manifest that `mc` reads as the same principal at `<bucket>/team/prod/<id>/manifest.json`, and a prefix-less destination answers `ready/ManifestReadable` for the same set. No restore preflight can be green for any prefixed destination; a green preview is only reachable prefix-less (E4/E6). Evidence: §5.2, `objects/s16/preflight-pf-flat.json` and the check plan beside it. Fixed 2026-09-18 in `e7f8c65`: the restore preflight joins the destination's `storage.prefix` into the manifest key and the segment listing exactly as the runner writes them (matches `Store::qualify`), with a prefixed-destination row and a mutant; the review confirmed the join against the writer. Live proof: CLOSED 2026-09-18 on the lab at `c6422a7` — E4 — a prefixed destination's manifest readable (`claude/lab-refresh-3.result.md` §8, independently re-verified in `claude/lab-refresh-3.review.md`). | PLAT-03.2 |
| D2-SIGNERID-REDACTED | The runner redacts the `signerKeyId` fact inside the relayed frame (`facts: {"signerKeyId": "[redacted]"}`) and the controller then compares the literal `[redacted]` with the `TrustRoster`'s key ids, so `signer.rostered` answers `notReady/SignerNotRostered` for a key whose SPKI sha256 equals the roster entry, while `signer.privateKeyUsable` on the same Job is `ready`. Every Backup preflight is blocked by a permanent false negative. A key id is public material. Evidence: §5.3, `results.json#E5`. Fixed 2026-09-18 in `83b8927` (redaction by key name and secret shape) and `902f4a1` (`signer.rostered` compares a key id the roster can answer and refuses a non-key-id with a distinct reason; every notReady row carries its authority's `scope`); a rostered key answers `ready` in a row with a mutant. Security-first review `claude/fix-runner-checks.review.md`: three rounds — a CRITICAL leak (an AWS secret key sharing a token run with a UUID survived the first rewrite) and a pre-existing `redact_path` leak were found by the reviewer's probes and closed (`f4ff0a3`, `4c38575`, `90edf23`); final ACCEPT with probes KEEP 30/30, DIE 57/57 and an independent 400k-sample bound (a random key misread as an object key 2.28% → ~1e-5). Live proof: CLOSED 2026-09-18 on the lab at `c6422a7` — E5 — `signer.rostered = ready/SignerRostered` with the key id printed; E3 — every notReady row scoped (`claude/lab-refresh-3.result.md` §8, independently re-verified in `claude/lab-refresh-3.review.md`). | PLAT-03.1 |
| D2-REDACT-OVERBROAD | The redaction rule also blanks the missing segment's name in `archive.segments` and in the `detailsRef` ConfigMap that exists to carry it (`{"missingSegment":"[redacted].bin.zst"}`), `runner.image`'s `imageID` and the manifest key in `archive.backupSet`'s message, so the printed remedy "do not restore until the objects are recovered" cannot be acted on. Evidence: §5.4, `objects/s16/details.jsonl`. Fixed 2026-09-18 in `83b8927`/`f4ff0a3`/`90edf23` (redaction by key name and secret shape; anchored archive paths `manifest`/`topics`/`partition=N`/`segment-N`/`logweir`; a 24-char free-component cap): segment paths, image IDs, manifest keys, digests, key ids and Secret/key NAMES survive into messages and `detailsRef`, secret VALUES never do (probes in both directions are rows). Residual, documented: a path carrying two free components (a timestamp set id and a hyphenated topic) is still blanked (review F6) — narrow, kept over the alternative of admitting credentials. Live proof: CLOSED 2026-09-18 on the lab at `c6422a7` — S16 — the segment path in `detailsRef`; S14b — the Secret and key named (`claude/lab-refresh-3.result.md` §8, independently re-verified in `claude/lab-refresh-3.review.md`). | PLAT-03.2 |
| D2-EVIDENCE-NOTATTEMPTED-UNWRITTEN | For a destination-backed Backup whose `evidenceRead` grant is `SecretKeys`, `controllers/backup.rs:1302` answers `EvidenceSource::NotAttempted` with its sentence, `receipt_sha256` is `None`, and the `if let` guard at `backup.rs:3966` skips the verification patch — so the operator sees no `status.evidence.verification` at all instead of the honest `NotAttempted`. The receipts are sound (`logweir drill verify` exit 0 on `bk-a`/`bk-b` receipts fetched from both buckets). Evidence: §5.5, S1.statusVerification. Fixed 2026-09-18 in `e90af89` (Backup) and `84c0aee` (the Restore twin the review found at `restore.rs:4321`): the second evidence patch is no longer fenced on `receipt_sha256`, so a pod-only `evidenceRead` grant publishes its honest `NotAttempted` verdict with its sentence; `NotAttempted` is the only verdict reachable without a fetched digest and carries no `matchedKeyId`, so `retrust` cannot re-derive it; the GC11 no-artifact case still writes nothing (`keys.complete()` is the guard); `verification_patch_value` (`b8322a0`, moved into `verification.rs` and applied to `Retrust::patch` too in `33eba99`) writes explicit nulls. Review ACCEPT-WITH-FIXES then closed. Live proof: CLOSED 2026-09-18 on the lab at `c6422a7` — S1.statusVerification — `NotAttempted` present with its sentence (`claude/lab-refresh-3.result.md` §8, independently re-verified in `claude/lab-refresh-3.review.md`).statusVerification). | PLAT-08.1 (D2 W10) |
| RET-DIGEST-PREFIX | No retention report renders on this build: `catalog_view.rs:966`/`:1616` `page_digest` returns bare hex while `RecoveryCatalog.status.pages[].sha256` is written `sha256:`-prefixed, and `retention_policy.rs:1345` compares them with `!=` and answers `ViewUnreadable` — the controller's own condition message prints both values, equal apart from the prefix. Every PLAT-16.1 test that needs the view (two destinations, unreadable manifest, overlapping keep rules, evaluation failure as a distinguishable state) and every PLAT-16.2 controller-side guard (active restore, legal hold, last usable point, shared segments) waits on it; the two tests that passed live (declared external lifecycle, continued scheduled backup 25/25) do not need the view. Evidence: `claude/d3w14.result.md` §5 PLAT-16.1. Fixed 2026-09-18 in `f5a6876` (+ `31259ec`): `bare_hex` normalises the published `sha256:` spelling on the retention side (prefixed and bare accepted; upper-case, wrong-length, truncated, double-prefix and non-hex refused as `ViewUnreadable`), the fixture publishes production's spelling, reverting the one line fails 30 rows. Review `claude/fix-retention.review.md` ACCEPT; 10/10 + 4/4 mutants. Live proof: CLOSED 2026-09-18 on the lab at `c6422a7` — the evaluation exists: two destinations and the evaluation-failure rows PASS (the two remaining rows are row-side key expectations being realigned) (`claude/lab-refresh-3.result.md` §8, independently re-verified in `claude/lab-refresh-3.review.md`). | PLAT-16.1, PLAT-16.2 (D3 W9) |
| RET-NOIMAGE | `retention_policy.rs` renders every enforcement Job's command as `logweir-retention` from `LOGWEIR_RUNNER_IMAGE`, but `Dockerfile` (`:158`, `:185`) builds only `-p logweir` and copies one binary; there is no `Dockerfile.retention` and no retention image value in the chart. Live: exit `127` (executable not found) from the image the shared controller names. The wave proved the enforcer's worker half (dry preview with real counts, one Enforce pass removing exactly the plan's 9 objects under `archive/`, the record verified by digest, four refusals) from an image built from `e7d0e79` with that one binary added, run as the operator Job D3 §7f prescribes; the shared controller's runner image was never changed. Evidence: §5 PLAT-16.2. Fixed 2026-09-18 in `457f651` (+ `241e6d7`, docs `90b604c`/`6adf6b8`): one `Dockerfile` builds `-p logweir -p logweir-retention` and the runner image carries both binaries (+11.5 MB); the enforcement Job's image is `runnerImage`; `scripts/check-image.sh` check 7 runs `logweir-retention` by bare name and asserts each binary names itself with equal versions (a mis-copied binary fails); `render-install --check` refuses a broken chain; `images.yml` carries the fast-fail copy; the one-signer and reaper reach-set gates stay green. Live proof: CLOSED 2026-09-18 on the lab at `c6422a7` — an Enforce pass from the PUBLISHED runner image; `scripts/check-image.sh` check 7 (`claude/lab-refresh-3.result.md` §8, independently re-verified in `claude/lab-refresh-3.review.md`). | PLAT-16.2 (D3 W9/W13) |
| CATALOG-RESYNC-NOT-HARVESTED | `RecoveryCatalog.spec.syncRequest` is documented as "change it to ask for a sync now"; on the second request the sync Job runs to `Complete` (19 of 19 Jobs) and the controller never harvests it — after 480 s the object still reads `Synced=Unknown/PodNotStarted` with the previous view published; an `intervalSeconds: 300` catalog behaved the same past its interval; a published view is also routinely followed by a write that puts `Synced` back to `Unknown/PodNotStarted`, so that condition is unusable as a completion signal (the harness watched `status.syncedAt` instead). Operators can only refresh a view by deleting and re-creating the catalog. Evidence: §5 PLAT-15.1 "New blocker". Fixed 2026-09-18 in `861e97d` (+ `3015472` self-heal, `5000da0` nulls, rows `919c202`/`767ed0d`/`8e379fe`, docs `2c0d556`/`d90958d`): `start` wrote `lastSyncJob: {name}` through an RFC 7386 merge patch, so the previous Job's `finishedAt` survived onto the new record and the harvest arm saw every later sync as already read; the write that flipped `Synced` back was `report_running`'s status patch carrying `PodNotStarted`, reached one reconcile after each publish because a completed request-sync did not serve its interval slot. Now the record is replaced with explicit nulls, `slot_already_served` and `harvested_record` (self-heals already-stuck catalogs), a failed sync spends its slot, and the harvest trusts a Job only by controller owner UID. Review `claude/fix-catalog-resync.review.md`. Live proof: CLOSED 2026-09-18 on the lab at `c6422a7` — the second `syncRequest` harvested (10.2 s in the review's re-run) and the interval re-sync (`claude/lab-refresh-3.result.md` §8, independently re-verified in `claude/lab-refresh-3.review.md`). | PLAT-15.1 (D3 W8) |
| NOTIFY-INSECURE-SINK-UNEXPOSED | `logweir notify deliver` refuses a non-`https://` webhook before it dials, and `NOTIFY_ALLOW_INSECURE_SINKS` — the documented local-development escape hatch — is set by nothing in the `ProtectionPolicy` spec or `delivery_job_spec`, so D3 L5's "an in-cluster echo sink records exactly 1 POST" is unprovable as written on docker-desktop: the sink received 0 POSTs and the alert reads `webhook:failed` (the bounded retry, `attempts=2` of 3, and "notification failure does not rewrite the backup result" — 33 Backups keeping their `resourceVersion` — were proven). Evidence: §5 PLAT-14.2. Decided and landed 2026-09-18 (`75750b9`, `41b8310`, tests `2d74b58`, docs `af779d1`): the escape hatch is an INSTALLATION setting — chart value `notify.allowInsecureSinks` (default `false`, boolean-only in `values.schema.json`) → `LOGWEIR_NOTIFY_ALLOW_INSECURE_SINKS=1` on the controller Deployment → the literal `NOTIFY_ALLOW_INSECURE_SINKS=1` in every delivery Job only when set; unset renders byte-identically to before; no `ProtectionPolicy` field, annotation, label or sink URL can enable it (probe row over nine surfaces); the Job's args never carry the URL. Review `claude/fix-notify-sink.review.md` ACCEPT; 9/9 + 4/4 mutants. D3 L5 stands as written for a lab that sets the value; production leaves it false. Live proof: CLOSED 2026-09-18 on the lab at `c6422a7` — the echo sink received exactly one POST, `Delivered` (`claude/lab-refresh-3.result.md` §8, independently re-verified in `claude/lab-refresh-3.review.md`). | PLAT-14.2 (D3 W4/W13) |
| TRUST-UPGRADE-SIGNEDAT | On the 2026-09-18 lab refresh to `e7d0e79`, five fixture objects written on 2026-09-14 (three `Backup`s, two `Restore`s) were re-derived from `Valid` to `Untrusted` (`SignedOutsideValidity`, "the document carries no signing-time field") while their phase, exit code and reason were unchanged: `verification.rs::stored_claim` reads the claimed signing time from `status.evidence.verification.signedAt`, a field the pre-D3 W10 controller never wrote, and `logweir_core::trust::decide` fails closed on `ClaimAbsence::FieldAbsent` — the same code path as a document that genuinely claims no signing time. The archive is never read and no signature is re-checked, so nothing about these receipts is in doubt; every fresh run records `signedAt` and verifies `Valid` (ten of ten in the refresh's plat06 run). Evidence: `lab-refresh-2.result.md` §8; `claude/d3w14.result.md` §4 measured it again — a fresh run is `Valid` with `signedAt`; stripping the field re-derives `Untrusted`; clearing the whole verification block triggers no archive re-read, so a pre-`signedAt` object cannot be repaired in place (`verification.rs:1554`; `:1450` returns `None` for an absent block, so clearing it forces no re-read — review confirmed, high). Smallest correct repair per review: an absent verification block schedules one evidence-fetch Job; otherwise document the upgrade consequence. Fixed 2026-09-18 in `6663ccb` (trust core), `03c2a85` (the bounded re-read), `e247cf9` (the undecided arm writes `NotAttempted`; `basis: Unverified` is its own retry mark), docs `84c8fe4`/`5c83ea6`/`bfbabcc`, `d8d3479` (regenerated `logweir.yaml`): `ClaimAbsence::NotRecorded` (a status written before `signedAt` existed, discriminated by the absent `trust` block and read back from `basis: Unverified`) is a different fact from `FieldAbsent` (the document claims no signing time) — `decide` returns an undecided verdict for it (`TrustBasis::Unverified`, `result: NotAttempted`, so the console badges, the API projection and the `SIGNED` column show nothing green), and both reconcilers do ONE digest-checked re-read of the receipt/scorecard through the evidence path that produced the original verdict, derive `signedAt` from the receipt's `finished_at` as a fresh run does, then re-derive; an unreachable archive keeps `Unverified` and is retried on the next policy event, bounded; objects that carry `signedAt` are never re-read; `KeyCompromise`, an unlisted signer and a usage refusal still flip immediately. Review `claude/fix-trust-upgrade.review.md` (fail-closed lens): a first round found the undecided arm still rendering green (F1 critical) and destroying its own discriminator (F2 high) — both closed at the root; final ACCEPT; 21/21 + 8/8 mutants. Live proof on lab-refresh-3: the lab's five objects did NOT heal — they carry an intermediate-build `trust` block with `basis: "None"` (`claude/lab-refresh-3.result.md` §9). Second fix 2026-09-18 in `0e68dfb`, `2d22f81`, `9fef8f6` after lab-refresh-3 §9 found the lab's five objects untouched: the discriminator is an allow-list on the basis — only `Current`/`Historical` mean a claim was compared; no `trust` block, no `basis`, `basis: None`, `Unverified` or an unknown value are `NotRecorded` and get the one bounded re-read; a `basis: Current` block without `signedAt` stays `FieldAbsent`; a document that genuinely carries no `finished_at` settles as `trust.signingTimeRead: absent` (never on a transient error); a fruitless attempt records `trust.retryAfter` (900 s window, a value outside it honoured as no backoff, cleared by an operator or the next policy event) instead of one get per reconcile; `docs/kubernetes.md` §15.2c is the allow-list. Review: fail-closed lens, 12 probed no-claim rows stay `Untrusted`, 23/23 + 27/27 mutants, ACCEPT. Live proof: CLOSED 2026-09-18 on the lab at `7b4fae9` — the five 2026-09-14 objects healed in their first reconcile with one read each and sat unmoved for 56 minutes across three controller restarts; a planted `basis: "None"` copy healed in 5 s with one store get (`claude/lab-refresh-4.result.md` §6, re-verified in `claude/lab-refresh-4.review.md`).1-3 already PASSES on genuinely pre-PLAT-19.1 objects behind the fence).1-3). | PLAT-19.1 (upgrade; D3 W10) |
| STATUS-PATCH-NO-RV | `patch_status_if_changed` (weirkeeper) sends its merge patch without a `metadata.resourceVersion` precondition, while `charts/logweir/README.md:53` and the D3 W2 record state that every status write is resourceVersion-preconditioned; found pre-existing by the d2-status-destination review (F1, low) — the frozen destination block itself is written under the precondition, but any status write that goes through this helper can overwrite a concurrent controller write. Fix (not started): carry the observed `resourceVersion` into the helper's patch and add the row the D3 W2 record implies; grep every caller. | PLAT-14.1 (D3 W2 seam S7) |
| TRUST-EXPIRY-LAG | On lab-refresh-4's S18 (an isolated `TrustPolicy` whose approver key has a `notAfter`), the approval's `Verified` condition read `True` for 2 m 35 s after `notAfter` passed before the controller re-derived it to `Verified=False, KeyIdExpired` — an expired key shown as valid for the length of a reconcile interval, against PLAT-19.1's acceptance ("treat unevaluated or stale expiry information as unknown, not valid"). Found by the independent review of lab-refresh-4 from the row's own samples (`claude/lab-refresh-4.review.md`). Fixed 2026-09-18 in `532740d` (+ docs `efa16b2`): the `Verified` condition is the verdict, so the boundary is made exact by the timer — an Approval requeues at its matched key's `notAfter` and a TrustPolicy at its next `notBefore`/`notAfter`/`ExpiringSoon` horizon, the heartbeat as ceiling, bounded in [1 s, 300 s] (a past deadline rests at the heartbeat, 500 keys arm one wakeup, a restart re-arms from the object); `may_sign_new` and `effective_state` flip on the same `now >= notAfter`, so `Verified=True` is never written at or after the boundary. What remains is stated in `docs/keys.md`: the requeue floor plus reconcile latency, an outage across the boundary and clock skew — a reader that must never act on a stale `Verified=True` treats a closed key window as unknown. Review (fail-closed lens) ACCEPT; 16/16 mutants. Live proof: CLOSED 2026-09-18 on the lab at `d387f87` — the refusal landed 4 ms after `notAfter`, 0 of 44 post-boundary samples read `True` (was 2 m 35 s); `claude/lab-refresh-5.result.md` §9.2, re-verified in `claude/lab-refresh-5.review.md`. | PLAT-19.1 (D3 W10) |
| RET-DEGRADED-UNREACHABLE | `RetentionPolicy.status.consecutiveRunFailures` never rises past 1, so D3 §6.5's `EnforcementDegraded` (three consecutive failed runs stop scheduling until the spec changes) is unreachable: after a failed enforcement run the policy controller's status patch is refused by the write guard ("carries no metadata.resourceVersion … no patch is sent", ~11 times a second in the controller log) because that write path builds its patch without the observed `resourceVersion` — the STATUS-PATCH-NO-RV class in one more place; the runs themselves keep being scheduled (five Jobs in 140 s), so the bound is not applied. Found by the harness-rows-2 review (`claude/harness-refresh.review.md`, H-1) on the lab at `7b4fae9`. Cause corrected at the fix's review: every status write carries its resourceVersion — the WARN was the policy's own guard after a 409 cleared the cursor mid-pass, a symptom; the defect is `start_run`'s `lastEnforcement` merge PATCH leaving the previous run's `finishedAt` behind, so `tracked_run` never tracked any run after the first (the CATALOG-RESYNC-NOT-HARVESTED class in one more place) and the counter froze at 1 while runs kept being scheduled. Fixed 2026-09-18 in `2d480a4`: all seven terminal fields of the record are nulled explicitly at start; a harvest whose write did not land no longer sets the Job TTL (a second defect, fixed); sequence rows prove degrade at three, no fourth Job, release on a generation bump, reset on success; review `claude/fix-retention-degraded.review.md` ACCEPT, 10/10 mutants. Live proof on lab-refresh-5 at `d387f87`: the counter half is CLOSED — `consecutiveRunFailures` reaches 3 and no further Job is created (`claude/lab-refresh-5.result.md` §8.1); the condition half is NOT — `EnforcementDegraded` is never written and the counter is not cleared by the generation bump that releases the stop; The condition half: fixed 2026-09-18 in `1ccf868`/`97a2563`/`e308ab3` — the real cause was a merge PATCH of the `status.conditions` ARRAY, which replaced it whole so every write deleted the conditions it did not name (the harvest published `EnforcementDegraded`, the next evaluation wiped it; `Ready`/`Evaluated` were dropped on refusals the same way); `conditions()` now upserts into the observed list, `EnforcementDegraded=True/ConsecutiveFailures` carries the count, exit code, per-point codes and record key, a generation bump clears it and resets the counter through one `adopt_generation()` helper used by all seven `observedGeneration` writers (a degraded policy with an unreadable view releases its budget on a spec edit), a success resets. The review swept every other controller for the conditions-array class: retention was the only instance. Review `claude/fix-retention-degraded.review.md` ACCEPT; 10/10 + 3/4 mutants. Live proof: pending lab-refresh-6 (the bounded-retry row).. | PLAT-16.2 (D3 W9) |
| RET-STALE-PLANREF | `RetentionPolicy.status.lastEvaluation.planRef` is written only by `start_run`, so after an evaluation that renders a new plan without starting a run the status still names the previous plan (found by the fix-retention-degraded review, low). Fix (not started): write `planRef` where the evaluation publishes its plan, with a row that a re-evaluation moves it. | PLAT-16.2 (D3 W9) |
| RET-STARTRUN-PATCH-OUTCOME | `start_run` discards the outcome of its status patch, so a patch refused after the enforcement Job was created leaves an orphan deletion Job the status never tracks (found by the fix-retention-degraded review, low). Fix (not started): create the Job only after the record write lands, or delete the Job when the write is refused; a row for the refused-patch path. | PLAT-16.2 (D3 W9) |
| PREFLIGHT-APPROVAL-ROSTER | The restore preflight's `approval.state` row (`controllers/preflight.rs::approval_verdict`, fed by `super::approval::load_roster` at ~:3441) resolves the approver key and its `notAfter` from `TrustRoster/default` directly, while the Approval controller itself resolves through the trust policy (D3 W10: an isolated `TrustPolicy` with an expiring approver key moved the Approval to `Verified=False, KeyIdExpired` on lab-refresh-4 §11.2). The two can disagree: an approval the controller has already expired under its policy can still read `ready` on the preflight, and the tracker's PLAT-03.2 test "expired approval" cannot be run without replacing the shared, immutable roster (harness-rows-5 §). Fixed 2026-09-19 in `e48da61` (+ `c694fcd`): the preflight's `approval.state` row relays the Approval's own verdict — condition, reason token, message, matched key — and `approval_rows` is no longer given `RosterFacts`, so the roster is unreachable by signature; `KeyIdExpired` → `notReady/ApprovalExpired` (blocking), retired/revoked stay `ApprovalNotVerified`, the plan-hash → expiry → verified ordering unchanged; the Approval is read in the Preflight's own namespace (a cross-namespace substitution is refused, pinned by a row). Review `claude/fix-preflight-approval.review.md` ACCEPT — a stale Approval status cannot durably fool the relay (immutable spec, the binding carries the Approval's resourceVersion, `restore.rs` re-checks `Verified=True` within 30 s); 15/15 mutants. Deliberate regression recorded as APPROVAL-KEY-WINDOW-UNPUBLISHED. Live proof: pending lab-refresh-6 (the real S18 row, harness-rows-6). | PLAT-03.2 (D2 W9 / D3 W10 seam) |
| APPROVAL-KEY-WINDOW-UNPUBLISHED | After PREFLIGHT-APPROVAL-ROSTER's fix the restore preflight's approval rows relay the Approval's own verdict and no longer read the roster, so two D2 §6.3 behaviours are unreachable until the Approval publishes its matched approver key's window: `ApproverKeyExpiresBeforeDeadline` (an approval whose key expires before the restore's deadline warned ahead of time) and the `min(10 m, notAfter)` re-check cap on both approval rows (found by the fix's review, `claude/fix-preflight-approval.review.md`; a deliberate regression, recorded rather than hidden). Fix (not started): the Approval controller publishes the matched key's `notBefore`/`notAfter` on its status (D3 W10's trust projection already has the key), the preflight reads it back, and the two behaviours return with rows. | PLAT-03.2 / PLAT-19.1 (D2 §6.3 amendment) |
| RET-COUNT-EARLY | On lab-refresh-6 at `af64073`, `RetentionPolicy.status.consecutiveRunFailures` reached 3 and `EnforcementDegraded` fired after only TWO enforcement Jobs had been created (the controller's own log shows two "created the retention Job" lines before the condition; the harness's owner census saw both Jobs with a 600 s TTL untouched, so the earlier TTL explanation does not hold) — the retry budget is spent one run early, so the bound D3 §6.5 promises (three failed runs) is applied after two. Evidence: `claude/lab-refresh-6.result.md` §8.2, `d3-live/lr620260919t0446z/state.json#retention-bounded-retry`. Fixed 2026-09-19 in `6d17baf`/`a7ead32`: the extra count was a second harvest of one Job — `start_run`'s 409 `AlreadyExists` arm fell through and nulled `finishedAt`, resurrecting an already-counted run, and a run already harvested in its slot could be started again; the 409 arm now returns without rewriting `lastEnforcement` and a harvested run is not restarted in its slot. `previously_refused()` is deliberately unchanged: the plan digest returning to an earlier run's id is D3 §6.5's "until the reason clears", costs nothing in-slot and is bounded by the budget (review: no CRD field needed). Review `claude/fix-retention-degraded.review.md` ("fix-retention-count") ACCEPT — two Jobs plus an extra same-slot pass count 2, three distinct Jobs count 3; follow-up I1 (an orphan Job after a crash between create and record) recorded under RET-STARTRUN-PATCH-OUTCOME. Live proof: pending lab-refresh-7 (the bounded-retry row). | PLAT-16.2 (D3 W9) |
| KEYSVIEW-ABSENT-VALID | `ui/pages/keys.js:67-79` renders every key `valid` when `status.expiredKeyIds` is ABSENT — the exact value PLAT-19.1's acceptance forbids ("the keys view labels unevaluated or stale expiry/trust information as unknown, not valid"); found by the lab-refresh-6 review (F2). The D3 W12 console wave (`claude/d3w12`, in review) rewrites the keys view with a §7.7 `unknown` evaluation column — its pass-2 review must prove this exact case (absent `expiredKeyIds` → `unknown`, never `valid`) with a fixture and a mutant before PLAT-19.1 can be Done. | PLAT-19.1 (D3 W12) |

### Codex batch history (2026-09-14, superseded by the table above)

Source candidate `a2bf5220e92d7c712ff77bf44c47d6fbabd1e9bc` was pushed to main.
Its CI run is [34936909533](https://github.com/VladyslavHaina/logweir/actions/runs/34936909533),
failed: the label gate rejects the new multiline NetworkPolicy caveat, and
one E2E guard expects the old invalid-signer exit code. Publication was skipped;
ci-candidate-fix owns scoped corrections. The standard shipping runner build
and exact-image smoke check passed. Bootstrap digest pinning/full-chart acceptance and backend live
closure remain open; candidate publication is not yet claimed.

Batch started 2026-09-14 from main revision
92e02097540c39ff8565283a38ee592499b95020. The built-in conversation agent limit
was reached, so this batch uses Codex worker processes with explicit model
selection. Workers share the checkout with separate ownership; implementation
must receive independent smaller-model review before acceptance.

| Tasks | State | Worker/model | Current boundary and next evidence |
| --- | --- | --- | --- |
| PLAT-01.1 / PLAT-01.2 | In progress | plat01 and plat01-review / GPT-5.6 Sol | Independent review found approval subject/UID replay, foreign Job adoption, late runner validation and unsigned allowlist replacement, incomplete owner checks and terminal deletion race. plat01-fix implemented all five findings and migration guidance; approval33, restore51, backup48, approval-runtime13, startup-contract5, docs12 and manifest28 focused tests passed. Combined rereview found remaining old-runner downgrade, untrusted/late-substituted notification, extra-owner adoption and CRD-rollout gaps; backend-close implemented all six: mandatory argv+environment handshake (actual old binary rejects new arg), retained authenticated notification config with TCP substitution controls, required redacted failure metrics, exact owner set, ordered CRD rollout and exact-plan CLI fixtures. Focused approval13/startup8/CLI14/orchestrator35/signing2/restore52/docs12 plus old-binary1 tests, strict clippy/fmt passed. Independent closure review accepted all six backend fixes and actual old-binary/transport evidence; backend-live now owns source-matched docker-desktop runtime matrix; interim harness records20 passed cases including distinct simultaneous restores, replay/owner collisions, substitution, restart, RBAC, retained evidence/GC and actual old runner refusal; legacy mutable pre-Job transition failed with PlanConfigMapConflict. Owned namespace cleaned. Terminal report20 passed/1 failed/7 unrun records exact200-record backup, independent destination content/offset and signature checks, and restored baseline deployments/cleanup. Independent review accepted exact record/signature/concurrent execution proof; legacy failure is an invalid mixed-format fixture, not product defect. backend-live-close assigned legitimate legacy case, true permission-denied file, retained collision evidence, strict absence/aggregate exits, provenance and remaining runtime observations |
| PLAT-02.1 | In progress | plat02-bootstrap / GPT-5.6 Sol | Implemented short-lived bootstrap CLI/Helm hook with retained private/public identity, resource-version initialization, external adoption and lost-key/rotation refusal; no controller/UI Secret authority. Identity10, chart26, docs12, CLI14, provenance5 tests and chart gate pass. Independent review found release image compatibility/pinning, bootstrap egress/authority, cross-namespace distribution, fake-concurrency evidence, stale entrypoint docs and rollback claim gaps. plat02-bootstrap-fix implemented protected same-key namespace distribution, singleton retention, digest-required bootstrap, API-only network policy, actual HTTP patch boundary tests, rollback hooks and corrected docs. Identity15/CLI15/chart27/docs12/manifest28/evidence21/publication9 tests plus strict/shell checks pass. Rereview found JSONPatch422 contention, Helm3 rollback omission, mutable development-image pull policy, offline/API-port docs, persistent-RBAC wording and exact-public-key-set gaps. plat02-close reports all six fixed: actual two-client Kubernetes422 convergence; exact identity-template Helm3/4 fresh/upgrade/rollback and Helm3 manual adoption retain key/resource identity; identity19/chart27/manifest28/docs12 and strict checks pass. Independent source/security/evidence review ACCEPTED all six with no material findings and credible retained scoped evidence. batch01-source-release owns standard shipping checks and candidate readiness. Default image remains unset until compatible digest publication/pinning; full-chart install/distribution/reinstall/lost-key acceptance remains |
| PLAT-02.2 | In progress | plat02, plat02-restore and plat02-review / GPT-5.6 Sol | Backup/restore/drill now retain one validated signer. Combined focused signing checks: 132 passed, 2 intentionally ignored; clippy/fmt/signer check passed. Independent review found signer lifecycle sound but requested valid CLI fixture, current help text, binary startup-side-effect proof and backup Ed25519 coverage. plat02-fix reports 132 passed, including 9 binary subprocess cases, and the valid-key CLI fixture now passes. Combined review requested retained authenticated notifications, mandatory safe failure metrics and exact-plan CLI fixtures; backend-close reports retained authenticated notification configuration, startup zero-network guards and mandatory safe local metrics; approval-bundle startup8, CLI14 and signing2 suites pass. Independent closure review accepted signer/runtime reporting changes. Live evidence review accepted exact200-record backup, simultaneous restores, independent destination reads and signatures. backend-live-close owns true permission-denied signer and notification/rotation observations plus harness/provenance corrections; full completion remains unproven |
| PLAT-13.1 | In progress | plat13 / Terra; plat13-review / Sol; plat13-fix / Terra | Four review findings fixed; current Chromium regression 5/5, lifecycle 7/7, default handoff 1/1, chart checks and 16-asset offline checks passed. Independent rereview found no remaining functional/security issue; stale image metadata correction remains. Real docker-desktop UI-template Helm install/upgrade/rollback, asset matching, namespace RBAC and durable API/browser cases passed; full-stack chart was not installed alongside existing singleton controller. Evidence: /tmp/plat13-e2e-authoritative.md. Lint correction passed ui_lint25/25 and behavior61/61, with a signed-byte trim mutant regression. Production assets unchanged. Independent audit accepted source/lint changes and most historical deployment evidence, but found live-harness false positives: same-heading namespace check, unchecked POST acceptance, incomplete core-API request capture and tee masking exit status. plat13-harness-fix reports real single/multi API/browser runs passed with distinct A/B UIDs/content, original POST201 and independent persistence, core API capture, nonzero failure exits and precise cleanup. Stale-content, POST403, core Namespace request, unreachable endpoint and unsafe-default negative controls passed; 61 behavior/7 lifecycle checks passed. Independent closure review accepts real UI behavior, all production hashes and three core harness fixes; two harness-only findings remain: cleanup exit1 ambiguity and documented port-forward rollout/reaping. ui-close-scheduler-review implemented narrow fixes and exact documented single/upgrade/multi passed, Forbidden cleanup control exits1, owned resources cleaned. Final review accepts harness code and live UI evidence; Bootstrap-fix repaired README-only readiness/Bash3.2 cleanup findings; actual dead-forward/unrelated200, early-exit37 cleanup, cleanup-only failure and child-reaping controls pass. Independent final UI shell review accepted; all UI source/harness/shell findings closed. Production UI unchanged; source committed as a2bf522, candidate CI/publication pending before Done |
| PLAT-04.1 | In progress | plat04 / GPT-5.6 Sol | Forbid/Allow policy, real owned-run state and resource-version pending reservation implemented;37 scheduler tests and strict clippy passed. Independent review confirmed central CAS design/tests but found older final status clearing newer reservation, unsafe replacement/history guidance and Allow409 foreign adoption. plat04-fix completed all three with stateful A-finalizer/B-reservation conflict+restart proof, ownership/terminal/migration cases, and regenerated both CRD copies/root install. Scheduler44 and CRD24 tests, strict clippy/fmt and chart/render/drift checks pass. Independent closure review accepted all three fixes/schema and independently passed44 scheduler/24 CRD tests. plat04-live recovered initial harness/watch-stream issues. Final0524z live run passed all requested scheduler cases, including both restart windows, stale-finalizer409, owned/foreign collisions and safe replacement; old-schema CEL proof linked to prior run. Owned cleanup passed and baseline preserved. Independent Python/proxy/evidence review accepted scheduler behavior, source matching and actual cleanup. Four harness issues remain: deterministic negative completion, admission-object deletion polling, unsupported reported HTTP403, and retained stale-finalizer capture. plat04-live-close fixed all four; authoritative060520z-harnessfix3 focused run exited0, retained stale conflict/state, exact rejection before resourceVersion-conditional suspension, honest conservative404 classification and all3ownedresource NotFound cleanup. Independent final Python/code/evidence review ACCEPTED all four, current hashes/artifacts and cleanup. Scheduler source and live acceptance are closed; source/harness committed as a2bf522, candidate CI/publication pending before Done |
| PLAT-06.1 | In progress | plat06-implementation / GPT-5.6 Sol | Isolated worktree /tmp/logweir-plat06-worktree from a2bf522; implement typed manual Backup execution, immutable settings and server-derived run identity without arbitrary runner annotations. No main candidate edits; review/live validation required |
| PLAT-17.1 / PLAT-17.2 | In progress (contract design) | plat17-contract / GPT-5.6 Sol | Read-only decision and implementation brief for bounded product API, identity/authorization and PLAT-19.2 ordinary/governed approval interface. Existing source release candidate preserved; research intentionally paused to prioritize verified candidate CI failures; retained draft/log to resume, no implementation or acceptance claimed |

Coordinator recovery state for the current batch: worker prompts, logs and
final reports are in /tmp/logweir-roadmap-run. Check live processes and reports
before dispatching replacements; do not infer success from process exit alone.
The continuation heartbeat is named Continue Logweir platform roadmap. This
ledger records dispatch, not completion or deployment.

Signing acceptance clarification: invalid signers must prevent data-plane
work, not suppress correctly redacted diagnostic failure metrics. Binary
sentinels distinguish diagnostic output from engine, Kafka, storage, archive
and work-directory side effects.

Resolved first-batch integration issue: the runner-argument test now copies
a valid PKCS#8 fixture and still proves the expected missing-password exit 1;
it does not accept the earlier prerequisite exit 4 as a substitute. Remaining
combined CLI tests must be rechecked after approval startup changes settle.

## PLAT-01 — Make every restore carry its own approved inputs

**Priority P0 · Done (2026-09-16).** Admission currently verifies a particular Approval,
while runner Jobs mount one namespace-wide approval bundle. Remove this
concurrency and deployment coupling without weakening signature validation.
Scope: approval transport and provenance; approval-policy changes are PLAT-19.

### PLAT-01.1 — Materialize an immutable execution bundle

**Problem/implementation:** Two restores can require different approved bytes
but share a mount. Build an immutable bundle from the referenced verified
Approval, signature, public verification key and permitted target identities;
bind it to the Restore UID and exact plan hash. Use a ConfigMap for public
artifacts where appropriate; never place private signing material there.

**Acceptance:** Concurrent restores mount only their own verified artifacts;
existing-name collisions cannot substitute content. **Tests:** Distinct
approvals, hash mismatch, collision, controller restart and concurrent
reconciliation. **Dependencies:** None; agree the runner input contract first.

### PLAT-01.2 — Integrate bundle lifetime and runner revalidation

**Problem/implementation:** Admission alone does not ensure that mounted
inputs are the approved ones. Make the runner revalidate the immutable bundle,
expose materialization failures in status, and retain the bundle through
execution/debugging before owned cleanup. Support a documented transition for
already-created restores that reference legacy transport.

**Acceptance:** No runner starts with missing/substituted artifacts; an in-flight
legacy restore is not silently changed. **Tests:** Two live simultaneous
restores, tampering, lost bundle, upgrade during execution and owned cleanup.
**Dependencies:** PLAT-01.1.

**Migration/safety and done evidence:** Record legacy-object treatment and
bundle retention; attach concurrent-run evidence proving distinct mounted
inputs and successful independent verification.

**Completion record — Done (2026-09-16), PLAT-01.1 and PLAT-01.2.** Source
`4956785` (backend unchanged since; CI run 35019727967 published
`vladyslavhaina/weirkeeper` `sha256:bdaaf374…` and `vladyslavhaina/logweir`
`sha256:2d20f4bd…`). Live docker-desktop matrix with source-matched images
built from `4956785` and from the pre-PLAT-01 commit `92e0209` (old controller
and old runner): 31/31 required cases passed, including two simultaneous
restores mounting only their own bundles (distinct plan/approval/sidecar
digests, all five `LOGWEIR_EXECUTION_*` digests differing), the Job and
ConfigMap collision matrix (ownerless, foreign-owner, old-UID, secondary-owner
all refused with the pre-reconcile object retained), tampered bundle member
(runner exit 3 on digest, zero notification posts, target topics and archive
listing unchanged), missing member (exit 1 before any client), controller
restart in flight (Job UIDs retained), upgrade/drain/rollback during execution
(seven controller transitions, Job UID unchanged, result `Succeeded`,
verification `Valid`), legacy in-flight Job observed unchanged, legacy pre-Job
plan retained at the same UID, owned garbage collection after Restore
deletion, and independent receipt verification. Harnesses committed as
`fbd124e` (`scripts/test-plat01-02-live.py`); evidence
`/tmp/logweir-roadmap-run/claude/artifacts/plat01-02-live/matrix/` (report,
state, namespace dump with Secret values as digests only); the harness version
that produced it was established from four independent fingerprints
(`claude/plat01-02-live.result.md` §2). Independent review
`claude/plat01-02-live.review.md`: ACCEPT, eight low findings, none blocking.
Migration: `docs/kubernetes.md` ordered Approval CRD rollout, legacy Job
adoption rules, rollback fence and bundle retention (each proved by a named
case). Classified unsupported live, not passed: post-parse software signing
failure (source-matched unit seam), mutating-admission rewrite (no webhook in
the lab; no cluster-admin claim), NetworkPolicy enforcement (Docker Desktop
does not enforce it).

## PLAT-02 — Bootstrap persistent signing and fail before data work

**Priority P0 · Done (2026-09-16).** Routine signing should be automatic, not removed.
Scope: installation identity and signing readiness; organization approval and
rotation are PLAT-19.

### PLAT-02.1 — Provision or adopt an installation identity

**Problem/implementation:** First-time users manually generate keys and create
Secrets. Add an idempotent bootstrap operation that creates a persistent
identity and publishes its public verification material, or adopts an
explicitly configured external key. Preserve identity across upgrades and
restarts; document backup/recovery of the key and public trust material.

**Acceptance:** A clean supported install needs no local OpenSSL ceremony;
reinstall/upgrade does not unexpectedly rotate the signer. **Tests:** Fresh
install, existing-key adoption, concurrent bootstrap, upgrade, denied Secret
write and external-key unavailability. **Dependencies:** None; coordinate the
trust-reference contract with PLAT-19.1.

### PLAT-02.2 — Validate signing before executing backup or restore

**Problem/implementation:** Invalid signing material may fail after backup
data work. Validate key availability, format and signing capability before
the engine starts; reuse the validated signer within the execution. Surface
an actionable prerequisite failure without exposing key contents.

**Acceptance:** Invalid/missing keys trigger no engine data operation; valid
work still produces independently verifiable evidence. **Tests:** Missing,
malformed, unreadable and rotated files; signing failure; assertion that the
engine was never invoked; successful verification. **Dependencies:** None for
early validation; integrate managed identities after PLAT-02.1.

**Migration/safety and done evidence:** Never mint keys on every chart render
or discard old verification material. Demonstrate old-archive verification and
identity retention after upgrade.

**Completion record — Done (2026-09-16), PLAT-02.1 and PLAT-02.2.** PLAT-02.1:
the chart's identity bootstrap is pinned to the reviewed runner digest
`docker.io/vladyslavhaina/logweir@sha256:2d20f4bd…` (rev `4956785`, amd64-only)
in `4b7a1f6`, with `scripts/check-chart.sh` requiring the tree's runner
repository by digest and four chart-gate mutants failing as required. Full-chart
acceptance on docker-desktop (`scripts/test-plat02-chart-live.py`, evidence
`artifacts/plat01-02-live/chart/`): 18/18 cases — fresh default install
bootstraps an identity with no OpenSSL ceremony, resource-scoped bootstrap RBAC,
no private material in Helm state, upgrade / controller restart / rollback /
uninstall-reinstall / restore-first recovery all retain the same key id and
private-key digest, external key adoption, external mismatch never rotates,
existing managed Secret adopted, external key unavailable with no fallback,
denied Secret write fails closed (HTTP 403, no key written) and recovers once
allowed, concurrent bootstrap converges on one identity (real HTTP 422
contention), lost private key refused without rotation, owned cleanup and exact
lab restore. PLAT-02.2: missing, malformed, unreadable (a genuine
permission-denied regular file) and wrong-type signers exit 4 with only
`metrics.prom` written and the engine never invoked (Kafka topic set and
archive listing unchanged); mid-run Secret rotation kept the retained parsed
signer and the rotated public key fails to verify its output; old archives
re-verify; successful evidence verified by the independent Python verifier.
Independent review ACCEPT. Docs: `docs/install.md` (re-pin procedure, amd64
`nodeSelector` remedy, rollback by re-pin, `identity.enabled: false`, or the
`Never`-only development override), `charts/logweir/README.md`. Residuals
carried as release-readiness notes, not open work: the chart harness file's
mtime falls inside its run window (the reviewer re-applied 24 committed
assertions to the recorded artifacts); the cold kubelet pull of the exact
digest is shown by composition (registry pull, sibling-digest cold pull, hook
Pod ran the exact digest); an emptied pin is caught by `chart_lint`, not by the
shell gate alone; the `sign_probe` error branch has no direct unit test.

## PLAT-03 — Report actual prerequisites and reusable preflight

**Priority P0 · Proposed.** A reachable broker or valid cron is not proof that
the requested backup/restore can run. Scope: bounded checks and honest status;
execution-time guards remain authoritative.

### PLAT-03.1 — Define operation readiness with actionable failures

**Problem/implementation:** Missing credentials, mounts and permissions become
late pod failures. Add a bounded readiness operation for referenced connection,
destination, signer, runner availability and required configuration. Choose
explicitly between narrowly scoped Secret access and a credential-consuming
check Job; do not grant broad Secret reads merely to improve error text.

**Acceptance:** The UI names each failed prerequisite and its remedy, with
check time and scope. **Tests:** Missing Secret/key, wrong credentials, storage
denial, image failure, timeout and redaction. **Dependencies:** PLAT-02.2;
PLAT-07.1 and PLAT-08.1 contracts for saved-reference integration; their UI
tasks are not prerequisites for this readiness contract.

**Live validation record (2026-09-18) — PLAT-03.1: six of the listed tests PASS live on
`e7d0e79`; the acceptance sentence FAILS on scope and naming; one blocking defect; stays In
progress.** PASS: missing Secret (S14a — `CredentialSecretNotFound` naming
`missing-secret` in 10 s), missing key (S14b — `CredentialSecretKeyMissing`, but the
message names neither Secret nor key after redaction, the reviewer's finding), wrong
credentials (S14c — `AuthenticationFailed`), storage denial (S3), timeout (S14f —
`BrokerUnreachable` on the blocking row, dependents `BlockedByPrerequisite`), redaction
(18 markers × {plain, base64} across every Preflight and 60 minutes of log: 0 hits).
NOT-RUN: image failure (S14e needs the shared controller's `LOGWEIR_RUNNER_IMAGE` changed).
FAIL: "with check time and scope" — six `notReady` rows carry no `scope` (E3: the
`podStatus`-, `checkJob`- and two `controller`-authority rows; ready rows do); and
`signer.rostered` refuses a rostered key on every Backup preflight because the runner
redacts `signerKeyId` inside the relayed frame and the controller compares the literal
`[redacted]` (E5; D2-SIGNERID-REDACTED, `readiness.rs:337` / `preflight.rs:947`). Also
owed: the source-connectivity check kind PLAT-07.2's "Test connection" consumes. Evidence
`claude/artifacts/d2-live/20260918T030840Z/objects/s14/`, `results.json#E3,#E5`.

**Partial record (2026-09-18) — PLAT-03.1: the source-connectivity check kind landed
(`5bf6d25`…`33ff2eb`), the rostered-key refusal and the scopeless rows are fixed and
closed live (D2-SIGNERID-REDACTED, lab-refresh-3), and S14b names the Secret and key;
stays In progress on two live items.** Every listed test but one is now proven live:
missing Secret, missing key (named), wrong credentials, storage denial, timeout, redaction;
the acceptance's "with check time and scope" holds — every notReady row carries its
authority's `scope`. Remaining: the image-failure case (S14e, needs a fenced controller
whose runner image cannot pull — queued for the next refresh) and the new kind's live
proof on the lab controller.

**Completion record — Done (2026-09-18), PLAT-03.1.** Source landed on main as D2 W4 (the
check runner and its readiness rows), W9 (the `Preflight` controller: every row with
state, code, message, remedy, authority, `scope` and check time; a blocking row's
dependents `BlockedByPrerequisite`), W12/W13 (routes and console), the wave-9 fixes
`83b8927`/`902f4a1` (redaction by key name and secret shape; `signer.rostered` comparing a
key id the roster can answer; `scope` on every notReady row) and `d2-source-check`
(`5bf6d25`…`33ff2eb`, the source-connectivity check kind this task owed PLAT-07.2). Proved
live on docker-desktop: D2 W14 at `e7d0e79` (missing Secret, wrong credentials, storage
denial, timeout with dependents blocked, redaction — 18 markers × {plain, base64} across
every Preflight and an hour of controller log, 0 hits), lab-refresh-3 at `c6422a7` (E5 a
rostered key `ready/SignerRostered`; E3 every notReady row scoped; S14b the missing key
naming its Secret and key) and lab-refresh-4 at `7b4fae9` (<LR4-03.1>: the image-failure
case through the fence; the `SourceConnection` rows on the lab controller). Independent
reviews: `claude/d2w14.review.md`, `claude/fix-runner-checks.review.md` (three adversarial
rounds on the redaction rule), `claude/lab-refresh-3.review.md`,
`claude/d2-source-check.review.md`, `claude/lab-refresh-4.review.md`. Acceptance ("the UI
names each failed prerequisite and its remedy, with check time and scope") and all six
listed tests (missing Secret/key, wrong credentials, storage denial, image failure, timeout,
redaction) are satisfied. Migration: none for existing objects; a Preflight from an older
controller keeps its stored rows.

### PLAT-03.2 — Add restore-specific preflight and invalidation

**Problem/implementation:** Cached reachability currently looks like completed
preflight. Check archive availability/coverage, target identity/access, mapped
topic collisions and approval/signing readiness against a specific plan hash.
Invalidate the displayed result whenever relevant input changes and repeat
authoritative checks at execution.

**Acceptance:** Editing a target, recovery point or topic mapping makes prior
checks stale; a green preview cannot bypass a later collision. **Tests:**
Stale inventory, plan edits, new target conflict after preview, missing segment,
denied access and expired approval. **Dependencies:** PLAT-03.1, PLAT-11.1.

**Migration/safety and done evidence:** Distinguish connection health from
operation readiness; retain existing runner guards. Capture both early error
messages and a race rejected at execution.

**Partial record (2026-09-17) — PLAT-03.1/03.2 and PLAT-09.1 runner half: the one
check runner `logweir check run` (D2 W4) landed; the tasks stay In progress until
the `Preflight` controller (W9), the API (W12) and the UI (W13) exist and W14
proves them live.** Landed in main as `claude/d2w4` (tip `537657d`: the runner,
its contract tests, docs, the redaction fixes and the closed startup rule).
Contract (`docs/stability.md`, D2 §4.2): argv `check run --plan <path>
--check-contract-version 1` with `LOGWEIR_CHECK_{CONTRACT_VERSION,PLAN_SHA256,
SUBJECT_UID}`; steps 1–4 (argv, plan bytes, sha, strict parse with subject UID)
open nothing and refuse with exit 3 and a lone `refusal-reason=CheckContractMismatch`
line, enforced by a module rule that every function reachable before step 5 names
no client constructor (the reviewer's helper-dial mutants die); all five plan
kinds (`topicInventory`, `operationReadiness`, `restorePreflight`,
`destinationAccess`, `evidenceFetch`) run over the landed pure contract, the Kafka
inventory probes and the store options, emit `check_contract` frames with the
relay budget respected and one end line last; exit 0 when an end line was printed
whatever the per-check states, 1 when it could not be, 2 and 4 structurally
unreachable; the engine is never invoked; the only write is the optional
create-only readiness marker (`AlreadyExists` counts as write-authorised); every
message, scope name, detail sample and stream passes `redact` (an AWS key planted in
a connection, a store error or the details stream is absent from the decoded
relay and from stderr); `plaintext` with `tls: true` is refused, transport comes
only from `plan.tls` and the connection CA is a projected in-pod path handed to
`ssl.ca.location` unopened; `EndpointUnreachable` is distinguished from `Timeout`;
`result`/`details` stream names are refused by name. Verified at `537657d`: logweir
904/904 (`check_cli` 91), the whole workspace, strict clippy (default and `e2e`),
fmt, `no-oso`, `no-archive-write`, `one-signer`, `pure-core`, `just lint`; four live
Compose rows (SASL inventory, a refused SCRAM credential classified
`AuthenticationFailed`, a MinIO denial versus not-found, an `evidenceFetch` of a
receipt a real `backup run` wrote; stack torn down); thirteen planted mutants
killed across three rounds. Independent review `claude/d2w4.review.md`:
ACCEPT-WITH-FIXES (two high: a plaintext dial under `tls: true`; a startup guard
that missed helpers) then ACCEPT, with two test-only guard gaps closed in a final
round. Migration: a new subcommand only; nothing invokes it until W8's discovery
Jobs and W9's preflight Jobs run with a runner image built from this source.
Limitations: a true authenticated-but-unauthorized store denial and the
private-CA broker are not proven live (W14); the restore preflight's
`writeProbe` needs a field in the pure contract (W1 amendment for W9).

**Partial record (2026-09-17) — PLAT-03.1/03.2 controller half: the `Preflight`
reconciler (D2 W9) landed as the eleventh controller; the tasks stay In progress
until W10 wires execution to consult a green preflight, W13 shows one, and W14
proves S15b live.** Landed in main as `` (the controller, its catalogue half
and its binding), `4c4d2ed` (the three grants it calls and no more), `b8c3bab`/
`ccff054` (tests), `388b18c`/`5694ad5` (docs) and `0de5e10` (review fixes).
Contract for W12/W13: `phase ∈ {Pending, Queued, Running, Completed, Failed,
Cancelled}` with `reason` always a `CheckCode`; `Failed` is narrower than "the pod
failed" — it means no result could be produced (an unattributed waiting code,
`ResultUnreadable`, `CheckPlanConflict`, `ArchiveUrlUnreadable`), while a missing
Secret is `Completed`/`notReady` with a remedy; `result.state` is `aggregate()`
verbatim (an empty set is `unknown`), `result.expiresAt` the minimum over
non-skipped rows, `checks[]` carry id/category/scope/state/gating/authority/code/
message/remedy/observedAt/expiresAt with facts and detail folded into `message`
and an offending name carried on `scope`; `Ready` is `Unknown`, never `False`, for
an `unknown` verdict. The expected rows are derived from the RENDERED
`CheckRequest` — never a static list per operation — and a cross-crate guard reads
the runner's pinned `want` literals so the two sides cannot drift (the review found
that the static list made every preflight `unknown`, so none could ever be
`ready`; both happy paths now build the relay from the runner's own emission and
assert `ready`); the backup plan requests `ArchiveRead`/`EvidenceWrite` (and
`EvidenceRead` when configured); `readiness.writeProbe` is threaded through
`weirkeeper::destination` into the Job so a requested `CreateOnlyMarker` is
honoured, never silently `false` (UI-FAKEPREFLIGHT's defect class). `binding`
(recomputed `planHash`, `inputsDigest`, referents with UID and generation,
`policyDigest`) is written BEFORE the Job — asserted by route order — and W12
decides applicability with `check_contract::stale_reasons`; the API's stale-reason
vocabulary was reconciled by the `d2w12b` follow-up (landed as `9b45a39..8f3165c`):
the API never parses `status.message` — a source-scan test bans it — but
recomputes staleness with `check_contract::stale_reasons` over `binding.referents[]`
read back through the sealed adapter, closes its enum over exactly what core
renders plus one API-side `unverifiable` (a referent it cannot read, a kind
outside the sealed set, a missing binding — always `applicable: false`, never
fail-open), and states the policy-digest comparison as a `staleBasis` line
naming the controller; `cancelRequested` is dropped because the controller
never emits it. Follow-up owed to the controller: `status.observedInputsDigest`
+ `observedAt` on `Preflight`, so `inputsDigestChanged` has a real producer and
the policy comparison stops overclaiming for non-ready verdicts. Seams: S1 (the plan through `check::plan::build`
and `logweir check run`), S2 (`no_execution_path_reads_preflight_or_discovery`
now globs both `src` trees and catches `as` aliases), S6 (pod by owner UID), S7
(every status write resourceVersion-preconditioned). RBAC: `preflights: [list,
watch]` (no `get` — nothing looks one up), `preflights/status: [patch]`,
`events: [list]` (D2 §7.1: without it three of the runner pod's four refusals and
`SigningKeyMissing` are unreachable), each with a note for W11; no `secrets`, no
`delete`. Verified at `fdbe553` on main: weirkeeper 795/795 (`preflight_controller`
78), `check_cli` 91, `manifest_lint` 29, `doc_lint` 12, `chart_lint` 28,
`crds-check`, `chart-check`, `render-install --check`, `check-no-archive-write`,
strict clippy and fmt; nineteen planted mutants killed (five original, fourteen
from the fix round, including the review's two survivors). Independent review
`claude/d2w9.review.md`: REJECT (one critical: no preflight could ever be `ready`;
one high: two surviving mutants; four medium; four low; two questions) then
ACCEPT. Hand-offs recorded: D2 W1 amendment — `BindingInputs` should gain the
credential Secret's `resourceVersion` and the recovery point's topic digest (§21.4
now says the list is not complete); W11 — the namespace-scoped `Role` question
and the three grant notes. Gaps: an inline `legacyArchive` is reported
`Failed`/`ArchiveUrlUnreadable` rather than checked (needs the resolver's
legacy-addressing block); `RecoveryPointLocationMismatch` is inert until W10 puts
the frozen snapshot on `Backup.status`; no `gc.rs`. Live: none here — W14 owes
S15b (a green preflight, then a collision, then `exitCode=3`). Migration: three
additive grants on a kind nothing had created yet.

**Live validation record (2026-09-18) — PLAT-03.2: five listed tests and the staleness
half of the acceptance PASS live on `e7d0e79`; one FAIL, one NOT-RUN, and a green preview
is reachable only for a prefix-less destination; stays In progress.** PASS: stale inventory
(S19 — the Backup preflight answers `TopicNotFound` from the broker, not the inventory),
plan edits (S15 — two plans, two hashes, two `inputsDigest`s; the bound `planHash`
immutable in place), new target conflict after preview (S15b — `MappedTopicsAbsent`, then
the collision created on the target and the Restore `exitCode 3`/`GuardRefused` "already
exists"), denied access (S17 — `archive.backupSet/AccessDenied` for a principal with no
policy at a location a working destination proves readable), a destination edit making
prior checks stale without moving the plan (S21 — generation diverges, `planHash` and
`locationDigest` unchanged). FAIL: missing segment (S16 — `SegmentMissing`, `count: 1`
correct; the sample and the `detailsRef` document both `[redacted]`; D2-REDACT-OVERBROAD,
`check_contract.rs:2418` rule `long-base64-or-hex-run`). NOT-RUN: expired approval (S18
needs the cluster-scoped `TrustRoster` edited). Defect: the restore preflight ignores
`storage.prefix` (E4/E6; D2-PREFLIGHT-PREFIX, `restore.rs:270`/`:480`), so no preview can
be green for a prefixed destination. Evidence `claude/artifacts/d2-live/20260918T030840Z/objects/s1[5-9]/`, `objects/s21/`.

**Completion record — Done (2026-09-18), PLAT-03.2.** Source landed on main as D2 W4/W9
(the restore preflight: plan hash and `inputsDigest` binding, staleness on a destination
or target edit, the segment and coverage rows), D3 W5 (point binding and the scope check
on the runner), the wave-9 fixes `e7f8c65` (the destination's `storage.prefix` joined into
the manifest and segment keys) and `83b8927` (the segment path surviving redaction into
`detailsRef`), and `d2-status-destination` (`52c1f00`…`85adc2e`: the frozen destination
block and a reachable `RecoveryPointLocationMismatch`). Proved live on docker-desktop: D2
W14 at `e7d0e79` (stale inventory, plan edits with immutable `planHash`, a new target
conflict after preview ending `GuardRefused`, denied access, a destination edit making
prior checks stale without moving the plan), lab-refresh-3 at `c6422a7` (S16 the missing
segment named in `detailsRef`; E4 a prefixed destination readable) and lab-refresh-4 at
`7b4fae9` (`claude/lab-refresh-4.result.md` §7 and §11.2: the location mismatch with both
digests printed, the pre-block `RecoveryPointLocationUnknown`, and the expired approval in an
isolated `TrustPolicy` — `Verified/True` → `Verified/False, KeyIdExpired` when the key's
`notAfter` passed) and lab-refresh-5 at `d387f87` (`claude/lab-refresh-5.result.md` §7.1 and
§9.2: all six listed tests re-run on the current build after the code paths under them were
rewritten — S19, S15, S15b, S17, S21, S16 with the segment key in `detailsRef`, E4 — and the
expiry boundary sampled with no post-boundary `True`), and harness-rows-6 on the `af64073` lab
(`claude/harness-rows-6.result.md`: the expired-approval test as the tracker means it — a
namespace-scoped `TrustPolicy` expires the approver key, the Approval goes
`Verified=False/KeyIdExpired`, the previously green preview comes back
`notReady/ApprovalExpired`, and the Restore holds un-admitted with no Job; the row gates on
the running image carrying the PREFLIGHT-APPROVAL-ROSTER fix `e48da61`, which made the
preflight's approval row follow the Approval's own verdict). Independent reviews: `claude/d2w14.review.md`,
`claude/fix-runner-checks.review.md`, `claude/d2-status-destination.review.md`,
`claude/lab-refresh-4.review.md`, `claude/lab-refresh-5.review.md`, `claude/harness-refresh.review.md`
(harness-rows-6), `claude/fix-preflight-approval.review.md`. Acceptance ("editing a target, recovery point or topic
mapping makes prior checks stale; a green preview cannot bypass a later collision") and
all six listed tests (stale inventory, plan edits, new target conflict after preview,
missing segment, denied access, expired approval) are satisfied. Stated residues (from `claude/lab-refresh-6.review.md`): staleness is proven at the binding
level — a referent's generation diverging from the frozen one with `planHash` and
`locationDigest` unmoved — and the API's `stale_reasons` projection is unit-proven, not
rendered in a live journey; APPROVAL-KEY-WINDOW-UNPUBLISHED (the deliberate regression the
preflight fix created — `ApproverKeyExpiresBeforeDeadline` and the `min(10 m, notAfter)`
re-check cap suspended until the Approval publishes its key window) fails no listed test and
stays open in the defect table; UI-FAKEPREFLIGHT, which pointed at this task, is resolved by
D2 W13's real Preflight at wizard step 5. Migration: a recovery point written before
`Backup.status.destination` answers `RecoveryPointLocationUnknown`, never `ready`; a preview
against it needs an operator's explicit destination choice.

## PLAT-04 — Make scheduling policy predictable

**Priority P1 · In progress.** Due runs currently do not reliably represent
active-run exclusion, and stale active references mislead users. Scope:
scheduling semantics; continuous capture is product expansion.

### PLAT-04.1 — Enforce concurrency using actual run state

**Problem/implementation:** A new slot can create work while the previous run
is active. Add an explicit concurrency policy with Forbid as the recommended
default, reconcile owned active executions, and clear completed active
references. Preserve deterministic slot identities across controller replicas.

**Acceptance:** Forbid prevents overlapping runs and explains the affected
slot; Allow is an explicit choice. **Tests:** Long-running backup, two
controllers, restart after creation, deleted Job, terminal Backup and stale
active reference. **Dependencies:** None.

### PLAT-04.2 — Expose cadence, missed-slot and retry policy

**Problem/implementation:** Raw UTC cron, fixed deadlines and implicit missed
slots obscure protection behavior. Add interval presets, next-run previews and
clearly defined timezone handling, deadline, catch-up and bounded retry policy.
Keep advanced cron available; record whether a run was scheduled, caught up
or retried. Retries get explicit identities without replaying completed work.

**Acceptance:** Users can predict the next runs and the outcome of downtime;
policy never creates an unbounded backlog. **Tests:** Timezone/DST boundaries,
long downtime, invalid cron, retry exhaustion and duplicate reconciliation.
**Dependencies:** PLAT-04.1, PLAT-06.1 for execution identities.

**Migration/safety and done evidence:** Preserve existing UTC behavior unless
the user changes policy; document defaults applied to old schedules. Publish
a policy truth table and live overlap/restart results.

**Partial record (2026-09-16) — PLAT-04.2: D1 W1, the pure cadence engine,
landed; the task stays In progress until W2 (schedule controller), W6 (API
previews) and W7 (UI) consume it.** `weirkeeper::cadence` (`15ce4b6`) is the
one clock-free evaluator for time zones, presets and previews: an absent
`timeZone` delegates to the legacy UTC `Cron` walk (asserted structurally and
by a zoned-versus-legacy property comparison), IANA zones come from a compiled-in
`chrono-tz` database (`6eedc0a`; four new packages, notices regenerated, `cargo
deny` clean, kept out of the pure layer), the D1 §4.3 rule holds (a nonexistent
local time fires once at the transition, a repeated hour fires both occurrences,
interval schedules keep cadence, slot identity stays the UTC instant), presets
compile to canonical cron and round-trip through one alias table, previews carry
one adjustment marker chosen by one rule (`marker_supersedes`), and the policy
inputs (`startingDeadlineSeconds`, `catchUpPolicy`, bounded retries with `-r<k>`
names that fit the DNS limit, the closed retryable classification where unknown
is not retryable) are validated pure functions with a deadline table
(`validate_deadlines`). `ui/tests/fixtures/cadence-presets.json` is generated
from the Rust table and checked against it. Verified at `4b54a5c`: cadence
41/41, weirkeeper 363/363, linkage 17/17, strict clippy and fmt, `pure-core`;
eleven planted mutants killed including the reviewer's three medium findings
(marker disagreement between walk paths, the load-bearing `EDGE_DAYS` slack now
pinned by the 1867 America/Sitka day, the tautological delegation test) and
the earlier UTC preview-horizon bug the worker found and fixed. Independent
review `claude/d1w1-cadence.review.md`: ACCEPT-WITH-FIXES then ACCEPT. No CRD
or controller change yet; migration is none until W2 adds the fields.

**Partial record (2026-09-17) — PLAT-04.2 and PLAT-06.2: D1 W3b, the `Backup`
reconciler consumes the run contract and freezes inputs grammar `v2`; both
tasks stay In progress until W2 (scheduler cadence), W5 (dynamic selection), W6
(API) and W8 (live) land.** Landed in main as `88ecec5` (the four trigger kinds,
grammar `v2`, the frozen schedule revision and `status.selection`),
`1af58b1`/`3e062ba` (tests), `ff3ab94`/`397e37d` (docs) and `369a6e0` (review
fixes) on top of W3a's types. Contract: every run's identity is derived through
`identity::run_identity` for `Scheduled`, `CatchUp`, `Retry` and `Manual` — the
composed name `logweir-backup-<schedule>-<slot>[-r<k>]` and the archive prefix
`<scheduleUID>-<slot>[-r<k>]` for scheduled kinds, the object's own API-server
UID for a manual run — and never from a client-supplied field (an invented
`scheduleRef.uid` is `ScheduleNotFound` with no POST; a name the trigger does
not compose is `ScheduledIdentityMismatch` and is never re-read as manual;
`triggeredBy` and `trigger.kind` must agree); the calendar-slot check (a real
UTC instant, so month 13 is refused) and the legacy owner-NAME equality check
live in `identity.rs` with rows in `tests/run_identity.rs` (15) — the review
found both had been compensated by hand in the reconciler without a killing
test, and the fix moved them where D1 §3.1 says they belong. `execution-inputs.json`
is grammar `logweir.dev/backup-execution-inputs/v2` = `v1` plus five optional
blocks (`trigger`, `scheduleRef`, `runPolicySha256`, `selection`, `destination`
— the last reserved for D2 W10) inserted in declaration order so a `v1` document
re-encodes to its own bytes (S4; the pre-contract fixture captured at `7cf50c1`
is untouched and still admitted end to end); a stored `v1` plan is compared
through the `v1` view, a stored `v2` plan whole with each block refusing by
name. The schedule's revision and policy digest are copied at the freeze, never
re-resolved; a manual run never reads a `BackupSchedule` (the route table proves
no `/backupschedules/` call), so a suspended, busy or deleted schedule cannot
touch it (§8.1/§8.3); `status.selection` is written in the same merge patch as
`status.execution` before the Job POST; `allUserTopics` is refused terminally as
`InvalidTopicSelection` naming PLAT-09.2/W5 until dynamic selection lands, and
the two freeze-boundary rails (never empty, never a glob) sit in `resolve_inputs`
so W5's runner-derived list meets them too; the W5 seam is gated on
`status.execution` so a frozen run re-reads its stored selection and never
re-runs discovery (`a_frozen_dynamic_backup_never_reruns_discovery`). PLAT-06.1's
plan ordering, ownership and SEC-PODLOG pod selection are untouched. Verified at
`397e37d` on main: weirkeeper 568/568 (`backup_controller` 85, `run_identity` 15),
`manifest_lint` 29, `doc_lint` 12, strict clippy and fmt; twelve planted mutants
killed (nine original, three from the fix round — including the reviewer's two
survivors and the seam gate). Independent review `claude/d1w3b.review.md`:
ACCEPT-WITH-FIXES (two medium: the unguarded compensations; the seam that would
have re-run discovery after the freeze; six low) then ACCEPT. Decision recorded:
D1 §3.3 amended at this integration to the landed grammar (`trigger`/`scheduleRef`
blocks, the single top-level `topics`, no top-level `executionId`/`triggeredBy`/
`deadlineSeconds`). Notes for W2: `concurrencyPolicy` accounting must filter on
`is_run_of_schedule && declared_trigger ∈ {Scheduled, CatchUp, Retry}` (a manual
run carrying `scheduleRef` is a member for history but never occupies a Forbid
slot), and a manual object squatting a deterministic scheduled name is a foreign
occupant of that slot. Gaps: `config/samples/backup-manual.yaml` (D1 §8.1) is
not shipped yet (W6); live proof is W8's. Migration: none — every `Backup`
created under PLAT-06.1 keeps running unchanged, and a `Backup` refused for
dynamic selection before W5 must be recreated after it (docs §10).

**Partial record (2026-09-17) — PLAT-05.1 and PLAT-04.2: D1 W2, the editable
`BackupSchedule` policy with immutable run snapshots and the §4.5 scheduler
(zones, deadlines, catch-up, retries) landed; both stay In progress until W4
(history), W6 (API previews and `PUT schedules`), W7 (UI) and W8 (live) land.**
Landed in main as `` (editable policy with immutable run snapshots), `c00da24`
(the §4.5 scheduler), `123fcd9` (a manual run is history, not concurrency), `74bca8d`/
`aaf1c7f` (docs), `55099d5`/`80be529` (tests) and `77de0b9` (review fixes). PLAT-05.1:
every `BackupSchedule.spec` field is editable except `sourceRef` — including
`destinationRef`, ratified at review because `archive.url` and `destinationRef`
are two spellings of one location and the task's own text says a changed
destination must not require a new schedule (D1 §5.1 amended; D2 §3.6's W6b
sealing superseded; RET-WRONGBUCKET's wrong-bucket case is now reachable by an
edit between runs, never mid-run); CEL R1/R2 on `.spec` and R3 on the schema
root ship with D1 §5.2's exact text; every created `Backup` records
`spec.scheduleRef {uid, generation, runPolicySha256}` from the same in-memory
object the reservation was made against (§5.4 by construction); §5.5 fails
closed with the reservation released and running Backups untouched; the
operator role gains `patch` on `backupschedules`. PLAT-04.2: `reconcile_schedule_with_archive`
is D1 §4.5 — zones through `cadence::Cadence`, `startingDeadlineSeconds`,
catch-up bounded and capped, retries 1–3 as `-r<k>` attempts with `retryOf`, the
uniform resourceVersion-preconditioned reservation for `Forbid` AND `Allow` (a
merge PATCH, never `replace_status` — P0-RESERVE stays closed), attempt chains
discovered by GET of deterministic names and never by listing (a foreign object
on a chain name stops the walk and `SlotNameUnavailable` is raised only when it
holds the name the next admission would use, so a succeeded attempt 0 beside a
foreign `-r1` is `AlreadyFired`), the §4.9 CRD-outdated guard reading only the
controller's own write responses, and `missedSlots` accounting that counts a
slot once (a slot that already has a final disposition is never re-counted on a
later pass, and a `ControllerUnavailable` gap records one boundary entry only
when the gap count moved); an unparseable `-r<k>` reservation is cleared (chrono
already refuses every impossible calendar slot of this format, measured and
recorded, so no separate round-trip check exists). Concurrency accounting filters on `is_run_of_schedule &&
declared_trigger ∈ {Scheduled, CatchUp, Retry}` — a manual run created from a
schedule is a member for history and never occupies a `Forbid` slot; `CatchUp`
and `Retry` runs do count, each pinned by a row. Status contract for W6/W7:
`status.policy {generation, runPolicySha256, timeZone, tzdb, effectiveSince,
evaluatedAt}`, `nextRuns[≤5] {at, localTime, adjustment?}`, `lastSlot {slot,
dueAt, attempt, disposition, backupRef?, reason, decidedAt}`, `missedSlots
{count, countCapped, lastEvaluatedSlot?, recent[≤10]}`, `pendingRun`,
`activeRuns[≤10]`, `observedGeneration` — `evaluatedAt` is the instant the
status last moved, not a liveness probe (staleness is `nextRuns[0].at` in the
past), `activeRuns` absent is "not computed", `Superseded` is not emitted, and
`PastStartingDeadline`/`BeforeRevision`/`NameUnavailable` are recorded reasons
(D1 §4.5 step 5, §4.8 and §4.9 amended to say so). Verified at `98257f8` on
main: weirkeeper 717/717 (`schedule_controller` 74, every PLAT-04.1 row preserved
with nine fixtures made stricter), 89 logweir lint tests, `crds-check`,
`chart-check`, `schema-check`, `render-install --check`, strict clippy and fmt;
26 planted mutants killed, including the review's surviving one (the
concurrency filter narrowed to `Scheduled`). Independent review
`claude/d1w2.review.md`: ACCEPT-WITH-FIXES (one high: the same missed slot
re-counted every pass; three medium: a foreign `-r1` discarding a succeeded
attempt 0, the unguarded `CatchUp`/`Retry` half of the concurrency filter, and
three deviations recorded only in the report) then ACCEPT. Live: none here —
W8 owes L-05.1-4 (R1–R3 on a real API server, with a `destinationRef` edit),
L-04-1 (zoned slot), L-04-2 (both downtime arms), L-04-4 (a real retry chain),
L-05.1-2 (the reservation 409), the CRD-outdated guard against the old CRD, and
a manual run beside a scheduled one. Handoffs: W4 replaces the bootstrap
namespace LIST (taken only when `activeRuns` is absent) with D1 §6.7's paginated
label-selected inventory; W10 may fold `destinationRef.name` into
`RunPolicyV1`. Migration: a schedule reconciled by an older controller has no
`activeRuns` block and is bootstrapped once; rollback per D1 §5.7 (never
downgrade the CRD).

**Completion record — Done (2026-09-16), PLAT-04.1.** Scheduler source and
live acceptance were closed at `4956785` (closure verification
`claude/closure-0413.result.md`: overlap under Forbid, explicit Allow with the
owned/foreign/ownerless/old-UID 409 winners, two controllers admitting from one
resourceVersion, restart after reservation and after child creation, stale
finalizer 409, safe replacement; schedule_controller 44/44, crd_shape 24/24,
retention 48/48; published controller digest `sha256:bdaaf374…`). The two
remaining gaps closed with PLAT-06.1's live run on `10f6c28`: the deleted-Job
case (a scheduled Backup's Job deleted under Forbid keeps the Backup
conservatively active, the schedule admits no new slot until it is terminal,
and the Job is re-created from the identical frozen inputs) and the slot
reservation under the unmodified shipped ClusterRole (`auth can-i
--subresource=status`: `patch` yes, `update` no on all five status
subresources; `pendingBackupRef` set at rv 2332382 and cleared at 2332386 with
zero namespace-local grants). The P0-RESERVE defect is fixed in `bdd26dc`
(resourceVersion-conditional merge PATCH, D-SEAMS S7) and guarded by
`manifest_lint::every_call_site_has_a_grant` (`40fd7cb`), whose
`replace_status` mutant fails by name. Independent review
`claude/plat06.review.md`: ACCEPT. Migration: none beyond PLAT-06.1's additive
CRD field; `docs/kubernetes.md` §9 records that status writes are
preconditioned merge patches. Earlier evidence used custom namespace Roles
(`/tmp/logweir-roadmap-run/plat04-live*.result.md`); the
`artifacts/w0-reservation/can-i-matrix-before.txt` capture is superseded and
annotated (it queried `status` as an object name).

**Live validation record (2026-09-18) — PLAT-04.2: D1 W8 measured the cadence contract on
docker-desktop against images built from `e7d0e79`; three scenarios PASS, one PARTIAL, two
NOT-RUN; stays In progress.** Harness `scripts/live/d1/{run,fixture}.py` (landed as
`b500190`; evidence `claude/artifacts/d1-live/20260918t0330z/`, report
`claude/d1w8.result.md`, review `claude/d1w8.review.md`). Proven live: time-zone
evaluation with its UTC negative control and `status.nextRuns` (L-04-1); the cadence preview
API through a real `logweir-api` in localAdmin mode (L-04-preview); `missedSlots.countCapped`
(L-04-cap); an unparseable cron refusing, admitting nothing for two periods, then recovering
(L-04-3); the full retry chain — attempt 0 `Failed` exit 1, `-r1` 60 s later with
`trigger {kind: Retry, attempt: 1, retryOf}`, `-r2`, no `-r3`, `lastSlot {disposition:
Exhausted, reason: RetryExhausted}`, and a second slot whose `-r1` succeeded once the store
came back with a receipt read from the archive and no `-r2` (L-04-4); two of L-04-6's three
clauses. Unmet: L-04-2 (long downtime) and L-04-5 (duplicate reconciliation across two
replicas) need D1 §13.1's fenced source-matched controller plus the namespace-rewriting
proxy, which was not deployed because the shared controller was serving two other live
waves (stopping or doubling it is not a test); L-04-6's Allow-cap clause (`Ready` reason
`ActiveRunLimit` not observed — holding ten runs open needs a large seeded topic; a harness
gap, as is L-04-2's catch-up half, which the reviewer found reachable by backdating
`status.policy.effectiveSince` the way L-04-cap already does). Nothing measured
contradicts the contract.

**Live validation record (2026-09-18) — PLAT-04.2: every listed test now PASSES live; the task stays In progress for its console slice only.** Source landed on main as D1 W1
(the pure cadence engine), W2 (cadence, deadline, catch-up and retry in the schedule
controller), W3a/W3b (the run contract the controller consumes) and W6 (the cadence
preview routes) — see the partial records above — and was proved live on docker-desktop
against images built from `e7d0e79` in two runs. D1 W8 (`claude/artifacts/d1-live/
20260918t0330z/`): time-zone evaluation with its UTC negative control and
`status.nextRuns` (L-04-1); the cadence preview through a real `logweir-api`
(L-04-preview); `missedSlots.countCapped` (L-04-cap); an unparseable cron refusing,
admitting nothing for two periods, then recovering (L-04-3); the full retry chain —
attempt 0 `Failed`, `-r1` 60 s later with `trigger {kind: Retry, attempt: 1, retryOf}`,
`-r2`, no `-r3`, `lastSlot {disposition: Exhausted, reason: RetryExhausted}`, and a second
slot whose `-r1` succeeded once the store came back, with the receipt read from the
archive (L-04-4); two of L-04-6's three clauses. The fenced-controller run
(`20260918t1209z/`, harness `scripts/live/d1/` with `fence/`, landed as `8e8df63`): L-04-2 long downtime —
a 503 s outage, restart +79 s, the missed slots recorded and the durable `lastSlot`
`Admitted/Scheduled` (the `CaughtUp` disposition is transient) — and L-04-5, two replicas reconciling one schedule without a
duplicate run. Independent reviews `claude/d1w8.review.md` and `claude/d1-fence.review.md`
reproduced the fence and re-ran rows. Acceptance ("users can predict the next runs and the
outcome of downtime; every slot is accounted for") and all five listed tests (time zones,
downtime, retries, invalid cron, concurrency cap) are satisfied live. Deviation: L-04-6's Allow-cap clause (`Ready` reason `ActiveRunLimit`) is proved by its unit truth
table, not live — holding ten runs open needs a large seeded topic; the orchestrator
accepted the unit proof and records it here. The policy truth table is published
(`docs/kubernetes.md`, "The policy truth table"). Residue before Done: the task's own text
asks for interval presets and next-run previews in the product, and the console does not
yet expose the W6 preview or `preset` (`ui/contract.js` declares neither) — D1 W7's schedule
slice (presets, next runs, time zone on the schedule form) is the one remaining item.
Migration: a schedule without `activeRuns` is bootstrapped once; never downgrade the CRD
(D1 §5.7).

**Completion record — Done (2026-09-18), PLAT-04.2.** Source landed on main as D1 W1 (the
pure cadence engine), W2 (cadence, deadline, catch-up and retry in the schedule controller),
W3a/W3b (the run contract), W6 (the cadence preview and PUT routes) and W7 (the console
slice, `a2ce381`…`700916e`: interval presets that compile to canonical cron through the W6
preview route, a next-run preview rendered from the route's zoned times with both firings
of a DST repeated hour, an explicit time-zone field, deadline / catch-up / retry fields with
their defaults printed, and an edit path under `expectedGeneration` that shows the revision
in force beside each running run's frozen revision); the console re-reads after a save and
renders a `policy_changed` 409 with the revision in force. Proved live on docker-desktop
against images built from `e7d0e79`: D1 W8 (`claude/artifacts/d1-live/20260918t0330z/`
— time zones with the UTC negative control and `status.nextRuns`, the preview route,
`missedSlots.countCapped`, an unparseable cron refusing then recovering, the full retry
chain `-r1`/`-r2`/no `-r3` with `RetryExhausted` and a later slot succeeding), the §13.1
fenced run (`20260918t1209z/`, harness `scripts/live/d1/` + `fence/` at `8e8df63` — long
downtime with a 503 s outage and the durable `lastSlot`, two replicas without a duplicate
run) and the console journey in the d1w7 review (`claude/d1w7.review.md`: 8/8 — a preset
with `Asia/Kathmandu` compiled to `30 2 * * *`, the PUT moving g1→g2 and the form showing
the stored generation, a run frozen at g3 under schedule g4). Independent reviews:
`claude/d1w8.review.md`, `claude/d1-fence.review.md`, `claude/d1w7.review.md` (the last with
three major findings — stale form after save, no path to a deliberate second backup, an
idempotency key colliding across reloads — all closed and re-journeyed). Acceptance ("users
can predict the next runs and the outcome of downtime; policy never creates an unbounded
backlog") and all five listed tests (time zones/DST, long downtime, invalid cron, retry
exhaustion, duplicate reconciliation) are satisfied; the policy truth table is published in
`docs/kubernetes.md`. Stated deviation: L-04-6's Allow-cap clause (`ActiveRunLimit`) is
proved by its unit truth table, not live — holding ten runs open needs a large seeded topic.
Migration: existing UTC schedules keep their behaviour; a schedule without `activeRuns` is
bootstrapped once; never downgrade the CRD (D1 §5.7).

## PLAT-05 — Edit future protection without losing recovery history

**Priority P1 · Proposed.** Immutable schedules force replacement, and their
owner relationships can remove execution history. Scope: schedule revisions
and Kubernetes resource lifetime; durable disaster catalog is PLAT-15.

### PLAT-05.1 — Introduce editable policy and immutable run snapshots

**Problem/implementation:** Changing cadence, selection or destination requires
a new schedule. Permit validated edits for future runs, version the policy,
and freeze the resolved settings and revision on every created Backup. Define
the atomic boundary when an edit races a scheduled slot.

**Acceptance:** Existing runs retain their original settings and new runs
identify the applied revision. **Tests:** Edit during execution, concurrent
edit/fire, conversion of existing resources, invalid edit and rollback.
**Dependencies:** PLAT-04.1. Agree the future selection fields with PLAT-09.1's
discovery contract; do not wait for PLAT-09.2 implementation, which consumes
this task's immutable snapshot contract.

**Live validation record (2026-09-18) — PLAT-05.1: the acceptance sentence PASSES live on
`e7d0e79`; three of the listed tests are NOT-RUN for want of a fenced controller; stays In
progress.** Proven: existing runs retain their original settings and new runs identify the
applied revision through `spec.scheduleRef.generation` and `runPolicySha256` (L-05.1-1,
re-run by the independent reviewer); all three CEL rules refusing on the real API server
with their decided text, including the `destinationRef` edit, and `Mars/Olympus` accepted by
the schema and refused by the controller with no admissions (L-05.1-4). Unmet: L-05.1-2
(concurrent edit and fire — needs the proxy to hold one identified controller request),
L-05.1-3 (conversion from main@4956785 — needs a swappable controller image) and L-05.1-5
(rollback) — all D1 §13.1 isolation the shared lab could not provide while other waves ran.
Evidence `claude/artifacts/d1-live/20260918t0330z/`.

**Live validation record (2026-09-18, fenced run) — PLAT-05.1: the three rows the first
wave could not reach were run behind D1 §13.1's fence; one PASS, one PARTIAL, one FAIL on
a product defect; stays In progress.** Harness `scripts/live/d1/` with `fence/` (landed as
`8e8df63`): a source-matched controller in an isolated namespace behind the
namespace-rewriting proxy, the shared controller fenced out by a ValidatingAdmissionPolicy
(its own log carries the denials; `unknownResources: {}`), the negative control NEG-1
failing as required. Evidence `claude/artifacts/d1-live/20260918t1209z/`; review
`claude/d1-fence.review.md` (fence reproduced; L-05.1-2 re-run PASS 192.7 s). L-05.1-2
(concurrent edit and fire — the held reservation 409s, the fired run carries the
generation it reserved) PASS; L-05.1-5 (rollback) PARTIAL — its legacy-owner clause is
vacuous by construction and is re-attributed to L-05.1-3's dumps; L-05.1-3 (conversion
from main@4956785) FAIL: the upgrade flips a pre-upgrade receipt `Valid` → `Untrusted`
because its status predates `signedAt` (TRUST-UPGRADE-SIGNEDAT, owned by PLAT-19.1 and
being fixed on `claude/fix-trust-upgrade`); the same run showed 4956785's own controller
`update`s `backupschedules/status` under a role that grants `patch` — the P0-RESERVE
defect already fixed on main, restated as an upgrade consequence. Residue before Done:
re-run L-05.1-3 after the trust-upgrade fix, and D1 W7's schedule slice (edit path showing
the applied revision on the form).

**Partial record (2026-09-18) — PLAT-05.1 console half: D1 W7 landed.** The schedule form
edits the whole future policy under `expectedGeneration` (PUT), shows the revision in force
and each running run's frozen revision (`spec.scheduleRef.generation` /
`runPolicySha256`), and re-reads after a save; a stale edit renders the `policy_changed` 409
with the revision in force (journey step verified live, `claude/d1w7.review.md`). Residue
before Done is unchanged: L-05.1-3 after TRUST-UPGRADE-SIGNEDAT's fix lands and is
re-run live.

**Completion record — Done (2026-09-18), PLAT-05.1.** Source landed on main as D1 W2 (the
editable future policy with a per-run snapshot: `spec.scheduleRef.generation` and
`runPolicySha256` frozen on every created Backup, the three CEL rules), W6 (PUT under
`expectedGeneration`), W7 (the console's policy form showing the revision in force beside
each running run's frozen revision, re-reading after a save, rendering a `policy_changed`
409 with the revision in force) and the trust repairs (`6663ccb`…`d8d3479`, `0e68dfb`…
`9fef8f6`) that made the conversion row honest. Proved live on docker-desktop: D1 W8
(`claude/artifacts/d1-live/20260918t0330z/` — L-05.1-1 existing runs keep their original
settings and new runs identify the applied revision; L-05.1-4 all three CEL rules refusing
on the real API server incl. the `destinationRef` edit, `Mars/Olympus` refused by the
controller with no admissions), the §13.1 fenced runs (`20260918t1209z/`,
`hr-d1f-20260918t192707z` — L-05.1-2 concurrent edit and fire: the held reservation 409s
and the fired run carries the generation it reserved; L-05.1-3 conversion from
main@4956785: every status field byte-identical across the upgrade except the trust
block's additive `signedAt`/`trust`, exactly D1 §6.2's carve-out; L-05.1-5 rollback — first
PARTIAL, then on lab-refresh-5 at `d387f87` a REAL rollback with the schedule still running:
the fenced controller swapped back to `main@4956785`, three attributable runs by the old
controller, all migrated, then forward again; and L-05.1-2 re-run on the same build in both
orderings, `driftCheck: enforced` with no flag — `claude/lab-refresh-5.result.md` §6), the
console journey (`claude/d1w7.review.md`, 8/8), lab-refresh-4 (`claude/lab-refresh-4.result.md`
§9.1: L-05.1-3 on the refreshed lab with the §6.2 carve-out) and harness-rows-4 on the
`d387f87` lab (`claude/harness-rows-4.result.md`: L-05.1-1 — edit during execution, the running
run keeps its frozen settings and the next run carries the new revision — PASS, and D1's
negative control NEG-1 fires on a deliberately false assertion; both re-run by the review). Independent reviews: `claude/d1w8.review.md`, `claude/d1-fence.review.md`,
`claude/harness-refresh.review.md` (incl. harness-rows-2 and harness-rows-4), `claude/d1w7.review.md`,
`claude/lab-refresh-5.review.md` (which named L-05.1-1 and NEG-1 as the residue now closed). Acceptance ("existing runs
retain their original settings and new runs identify the applied revision") and the five
listed tests (edit during execution, concurrent edit/fire, conversion, invalid edit,
rollback) are satisfied. Migration: a schedule without `activeRuns` is bootstrapped once;
rollback per D1 §5.7; an upgrade may add `signedAt`/`trust` to a finished run's
verification block by one digest-checked re-read and nothing else.

### PLAT-05.2 — Decouple schedule deletion from retained history

**Problem/implementation:** Deleting the schedule can garbage-collect Backup
objects. Separate future scheduling ownership from retained execution and
recovery-point lifetime; implement a migration for existing owner references
and explicit history cleanup rules.

**Acceptance:** Deleting/recreating a schedule stops future work without
silently removing recovery history or archive data. **Tests:** Delete with
active/completed runs, API garbage collection, migration interruption, same-name
schedule recreation and unrelated-resource preservation. **Dependencies:**
PLAT-05.1; PLAT-15.1 supplies durable catalog integration later.

**Migration/safety and done evidence:** CRD validation changes require an
upgrade/rollback plan. Demonstrate history surviving deletion and identify
any resource-retention cost.

**Partial record (2026-09-17) — PLAT-05.2: retained history decoupled from schedule
deletion (D1 W4) landed; the task stays In progress until W7 shows history and W8
proves L-05.2-1…6 live.** Landed in main as `58d68f6` (`controllers/schedule_history.rs`:
migration, inventory, `HistoryRetained`), `ee842cd` (`patch` on `backups` — one caller,
the ownerReference detach), `321d18e`/`faf19a0` (tests), `58ee9d1`/`a01b188`
(docs §9 rewritten, §13, the chart README), `3738a37`/`3f7657e` (fixes) and
`f35c7cf` (the callerless `is_owned_by_schedule` deleted). Contract
(`docs/kubernetes.md` §9): scheduled runs no longer carry the schedule's controller
ownerReference, so deleting a schedule cascades to nothing — every run, plan
ConfigMap and evidence stays; membership is `identity::is_run_of_schedule` (a manual
run of a schedule IS history); same-name recreation is a new UID whose history does
not inherit; Logweir still deletes nothing (no `delete` verb; `manifest_lint`'s two
delete assertions untouched). The §6.2 migration detaches legacy ownerReferences
idempotently and resumably INSIDE the paginated inventory's page loop (`limit=500`,
twenty pages, budget = page size — measured by a stateful double at 229 LISTs and
about 114 500 object reads to migrate 10 000 runs, closed form `P(P+1)/2 + P − 1`,
against roughly 500 000 before the review; the table is in §9), never faster than
the inventory interval; a `409` is re-read next
pass; `422`/`403` and a legacy run whose `spec.scheduleRef` does not name this
schedule (it cannot be detached without orphaning it, and `Backup.spec` is
CEL-sealed) are recorded as `status.history.migrationBlocked[]`
with one vocabulary (`ApiForbidden` / `ApiInvalid` / `NoScheduleReference`), a sorted
bounded sample, and — for the last — a message naming the only two remedies
(`--cascade=orphan`, or delete the run); D1 §6.2 step 4 amended at this integration.
`HistoryRetained` is COMPUTED from the recorded block, never copied; a capped
namespace-wide walk sets `status.history.ownershipScanComplete: false`, never unlocks
the UID label selector and never claims `True` (the review's high finding: D1 §6.6's
own 35 040-run worst case would otherwise have hidden legacy runs forever) — an
unfinished walk reports `False MigrationBlocked` naming `--cascade=orphan`, as a
suffix that never shadows `LegacyOwnerReferencesRemain`. The
inventory replaces W2's bootstrap namespace LIST (D1 §6.7); the hourly inventory
writes status once an hour, measured and guarded; every write is
resourceVersion-preconditioned. Verified at `6845ffb` on main: weirkeeper 902/902 at the reviewed tip
(`schedule_history` 23+, every PLAT-04.1/04.2 row preserved), `manifest_lint` 29,
`doc_lint` 12, `chart_lint` 28, `crds-check`, `chart-check`, `render-install --check`,
`check-no-archive-write`, strict clippy and fmt; thirty-nine planted mutants killed
(including the review's survivor on the capped walk; two genuine counting defects
found and fixed). Independent review `claude/d1w4.review.md`: ACCEPT-WITH-FIXES (one
high: the capped walk latched the label selector; two medium: a namespace re-list
every 30 s during migration, an un-clearable `MigrationBlocked`; four low; one
question) then ACCEPT. Deviations: no process-start inventory clause; three extra
`status.history` fields; a 200-patch per-pass cap replaced by the page budget;
`estimatedBytes` floors until the catalog supplies sizes. Live: none here — W8 owes
L-05.2-1…6 (delete with active and completed runs, API garbage collection, the
migration in place, same-name recreation). Migration: one additive grant; existing
runs are detached on first reconcile and the schedule's `HistoryRetained` reports
the walk; rollback per D1 §6.9.

**Live validation record (2026-09-18) — PLAT-05.2: four scenarios PASS live on `e7d0e79`,
three NOT-RUN and two sub-clauses unverified; stays In progress.** Proven: migration
detaches legacy owners while preserving a foreign owner, a label and an annotation
byte-for-byte and writing the UID label and marker annotation (L-05.2-1); deletion under all
three cascade modes leaves every run, plan ConfigMap and Job alive with its own UID while
active runs finish and verify (L-05.2-2, re-run by the reviewer); a same-name recreation is
a different schedule down to the `NameUnavailable` disposition (L-05.2-4); unrelated
resources do not move (L-05.2-5); `ownershipScanComplete` observed `true` on a small
history. Unmet: L-05.2-3 (migration interruption with an injected 409), L-05.2-6 (read
cost by request counting) and the >10 000-`Backup` capped walk (`MAX_INVENTORY_PAGES` 20 ×
`INVENTORY_PAGE_SIZE` 500) — each needs D1 §13.1's fenced controller and proxy, and the
walk must not be seeded against a controller other waves use; within L-05.2-1 the "every
migration PATCH carries `metadata.resourceVersion`" clause and within L-05.2-2 the
"unfrozen" sub-case are unverified. Evidence `claude/artifacts/d1-live/20260918t0330z/`.

**Completion record — Done (2026-09-18), PLAT-05.2.** Source landed on main as D1 W4
(`58d68f6`, `ee842cd`, `321d18e`/`faf19a0`, `58ee9d1`/`a01b188`, `3738a37`/`3f7657e`, `f35c7cf` — see the partial record above: retained history decoupled from schedule deletion — legacy
owner references migrated once with a resourceVersion precondition, the UID label and
marker annotation, all three cascade modes leaving every run, plan ConfigMap and Job
alive, a same-name recreation a different schedule down to `NameUnavailable`, the
bounded ownership walk) and was proved live on docker-desktop against images built from
`e7d0e79` in two runs: D1 W8 (`claude/artifacts/d1-live/20260918t0330z/`: L-05.2-1
migration preserving a foreign owner, a label and an annotation byte-for-byte; L-05.2-2
deletion under `background`, `foreground` and `orphan` with active runs finishing and
verifying; L-05.2-4 same-name recreation; L-05.2-5 unrelated resources unmoved;
`ownershipScanComplete` true on a small history) and the fenced-controller run
(`20260918t1209z/`, harness `scripts/live/d1/` with `fence/`, landed as `8e8df63` — the shared controller
fenced out of the namespace by a ValidatingAdmissionPolicy, proved by its own log's
denials, and a source-matched controller behind the namespace-rewriting proxy: L-05.2-3
migration interrupted by an injected 409 and resumed without a duplicate write, L-05.2-6
read cost — zero `LIST backups`, 2.55 `GET`s per reconcile — L-05.2-1's "every migration
PATCH carries `metadata.resourceVersion`" clause (22/22 independently), and L-05.2-2's
unfrozen sub-case). Independent reviews `claude/d1w8.review.md` and
`claude/d1-fence.review.md` re-ran the deletion journey, L-05.2-3 and the resourceVersion
count and reproduced the worker's figures. Acceptance ("deleting a schedule never deletes
history; history survives and stays attributable") and every listed test are satisfied.
Stated deviation: the >10 000-`Backup` capped walk (`MAX_INVENTORY_PAGES` ×
`INVENTORY_PAGE_SIZE`) is proved by its unit truth table (`schedule_history.rs:1801`),
not live — seeding ten thousand objects against a shared controller is a denial of
service, not a test; the reviewer judged the unit proof sufficient. Migration: a schedule
reconciled by an older controller has no marker and is migrated once on first reconcile;
rollback per D1 §5.7. The console's schedule-history view (D1 W7) is not part of this task's acceptance; it belongs to PLAT-10.2.

## PLAT-06 — Make manual backup a normal operation

**Priority P1 · In progress.** A manual Backup should not require scheduler-owned
CLI annotations. Scope: typed execution creation, not a permanent backup worker.

### PLAT-06.1 — Derive runner inputs from the Backup contract

**Problem/implementation:** Missing runner-argv annotations prevent ordinary
manual resources from running. Generate arguments from validated typed spec,
immutable resolved inputs and server-generated run identity. Preserve scheduled
idempotence and temporarily interpret legacy objects through a documented
compatibility path where necessary.

**Acceptance:** A valid manual Backup needs no internal annotation; arbitrary
annotation arguments cannot change the executed operation. **Tests:** Manual
and scheduled runs, missing/hostile legacy annotation, duplicate creation,
restart and configuration snapshot equality. **Dependencies:** None.

### PLAT-06.2 — Expose Back up now and first-run execution

**Problem/implementation:** Users wait for a cron slot to verify setup. Add
Back up now from a cluster or schedule and Run first backup now after schedule
creation. Use an idempotent submission token and show the durable resulting
run, including the copied schedule revision where applicable.

**Acceptance:** Repeated clicks create one requested run; a deliberate later
backup creates another. **Tests:** Double click, lost HTTP response, refresh,
paused schedule, failed preflight and successful scheduled-policy copy.
**Dependencies:** PLAT-06.1, PLAT-03.1; PLAT-17.1 for shared API integration.

**Migration/safety and done evidence:** Do not mutate an existing execution to
retry it. Demonstrate the same manual CR path through CLI/API and UI.

**Completion record — Done (2026-09-16), PLAT-06.1.** Landed in main as
`8e362f9` (typed runner inputs), `3c7e629` (contract docs), `bdd26dc`,
`40fd7cb`, `ae39b35` (the P0-RESERVE fix, reverse RBAC lint and its doc) and
`10f6c28` (live harness `scripts/test-plat06-live.py`). Contract
(`docs/kubernetes.md` §10, `config/samples/backup.yaml`): a `Backup` no longer
executes the `logweir.dev/runner-argv` annotation; run identity is server-derived
(`manual` → the object UID, `schedule` → `backup_id_for(schedule UID, slot)` with
the deterministic name and owner reference required); the controller freezes the
whole resolved input set in a create-only `immutable: true` ConfigMap
`<backup>-plan` (`execution-inputs.json` grammar
`logweir.dev/backup-execution-inputs/v1` plus the documents rendered from it),
records `status.execution {id, inputsRef, inputsSha256}` before any Job exists,
and admits an existing plan only after re-resolving and comparing every
executable field (`PlanConfigMapConflict` otherwise, nothing patched, replaced,
deleted or adopted); a Job not controlled by exactly this Backup is
`JobNameConflict`; a vanished Job is re-created from the frozen inputs; the pod
carrying the exit code is selected by job-name label and owner Job UID; an
annotation is surfaced by size and digest only (`RunnerArgvAnnotationIgnored`)
and a Job an older controller created is observed unchanged
(`ExecutionInputsUnverified`). No Secret name or credential appears in the
snapshot or status. Verified at `10f6c28`: weirkeeper 321/321 (backup_controller
63, schedule_controller 44), the nine lint suites 105/105, strict clippy and fmt,
`crds-check`/`chart-check`/`schema-check`/`render-install --check`, fourteen
planted mutants killed. Live docker-desktop run (namespace
`lw-plat06-20260916t131700z`, controller and runner images built from the
branch, lab restored byte-equal, lock 13:02:51–13:36:09Z; evidence
`artifacts/plat06/`, 106 files): 7/7 cases — manual Backup with no annotation to
a signed receipt independently verified by `docs/verify_scorecard.py`; hostile
annotation ignored (argv unchanged, attacker prefix absent from the bucket);
scheduled Backup under the shipped Forbid role (the P0-RESERVE proof); controller
restart mid-run keeps the Job and inputs; deleted Job re-created from identical
inputs under Forbid; two Backups with identical inputs freeze byte-identical
snapshots apart from identity; a duplicate name is the API server's
`AlreadyExists`. Independent review `claude/plat06.review.md`: ACCEPT (two
medium, five low, none blocking). Migration (`docs/kubernetes.md` §10): apply
`config/crd/backups.yaml` before the controller (the additive `status.execution`
field is otherwise pruned and every run reports `ExecutionInputsUnverified`);
in-flight Jobs finish unchanged; a Backup caught between an older controller's
plan write and its Job create becomes `PlanConfigMapConflict` and is re-created;
rollback fails safe (an older controller creates nothing for an annotation-less
Backup and refuses a frozen three-key plan); leave the CRD in place. Limitations:
no live failure path ran (`ExecutionSpecInvalid`, `PlanConfigMapConflict`,
`JobNameConflict`, legacy observation and the rollback leg rest on
controller-double coverage; the reviewer verified both code halves and asks for
one live legacy/upgrade/rollback leg before a release branch — carried to
PLAT-20.2); the SEC-PODLOG ownerless-pod fallback in `select_job_pod` and the
SEC-ENVHTTP forwarded `AWS_ALLOW_HTTP` remain open under their own rows; the
RECEIPT-DUP finding is recorded in the defects table for D3.

**Partial record (2026-09-18) — PLAT-06.2 console half: D1 W7 landed; the task stays In
progress until the manual CR path is shown through CLI/API and UI end to end and the
legacy-mode grant lands.** `claude/d1w7` (landed as `a2ce381`…`700916e`): "Back up now" on a schedule
and "Run first backup now" after creation go through `POST /api/v1/backups` with one
idempotency intent per draft (32 random hex, persisted with the draft, ended by a durable
run or a reload), so a double click, a lost response and a refresh produce ONE run — the
review's journey measured three clicks + reload = one run, a reload-click = a second, "Back
up again" = a third, four distinct intents; both 409s render (`policy_changed` with the
revision in force, `idempotency_conflict` naming the spent intent); the disabled state
carries the controller's own reason; the resulting run shows its copied schedule revision.
Review `claude/d1w7.review.md` ACCEPT after a fix round. Unmet: legacy mode refuses "Back
up now" by name (a derived name and a missing chart grant), the same manual CR path
through the CLI is not demonstrated beside the UI, and the failed-preflight case is not
journeyed.

## PLAT-07 — Reuse saved cluster connections everywhere

**Priority P1 · Proposed.** SCRAM reference reuse is already fixed; extend the
same model to registration, discovery, preflight and restoration without
reintroducing separate credentials.

**Partial record (2026-09-17) — PLAT-06.2 API half, with PLAT-04.2's previews and
PLAT-05.1's policy edit: D1 W6 landed; the tasks stay In progress until W7 (UI) and
W8 (live) land.** Landed in main as `` (three route families), `89bba9e`/
`c101b54` (tests), `da9a757`/`b55c269` (docs) and `3968ecf` (review fixes) on the
PLAT-17.1 skeleton. Contract for W7 (`docs/api.md`): `GET /api/v1/cadence-previews`
(D1 §4.4's path) compiles a preset or cron in a zone through `weirkeeper::cadence`
with no cluster read — next runs with local time and DST adjustment, an invalid cron or
zone refused as `schedule_invalid` naming the field, the same code the other routes
use for the same condition; `PUT …/schedules/{name}` edits FUTURE policy as a typed
merge PATCH under `expectedGeneration` (412 on mismatch; `sourceRef` immutable before
the call; the sentinel rule reported as `destination_sentinel_mismatch`; CEL refusals
from the API server mapped by rule; running runs keep their frozen snapshot) — the
verb stays `patch`, so no new console-ServiceAccount grant; `POST …/backups` creates
the canonical manual `Backup` (D1 §8.1 — `spec.trigger.kind: Manual`, `triggeredBy:
manual`, the deterministic `logweir-manual-<26 base32>` name from the request scope,
`scheduleRef` and the policy snapshot copied from one read when created from a
schedule) for "Back up now" and "Run first backup now", with same key + same body ⇒ 200
and the existing uid, same key + different body ⇒ 409 `idempotency_conflict`, a lost
response replayed safely, a suspended or busy schedule never blocking a manual run
(§8.3), and the §8.4 preflight seam stated honestly; `config/samples/backup-manual.yaml`
ships and a test creates through the route and compares labels and spec byte for
byte. Authorisation: every route probed across the four roles — an Operator may edit
a schedule's future policy with the authority it has to create one (D0 matrix amended
at this integration), the cadence preview needs permission to read schedules
somewhere (the review's untested guard now has a killing row: an approver bound
without a schedule surface is refused), every mutation audited. Verified at `72751ab`
on main: logweir-api 308/308, strict clippy and fmt, `schema-check`, one-signer,
no-oso, the five repo lint suites, `node --test ui/tests/contract.spec.js` 18/18 with
`ui/` byte-unchanged; twelve planted mutants killed (two real defects — nested
merge-patch nulls, the sample digest — found during development, plus the review's
surviving authorisation mutant). Live on docker-desktop (isolated labelled namespaces,
deleted after owner-label and UID checks): both DST previews, a manual run from a
suspended schedule, replay with the same UID, an idempotency conflict, `409
policy_changed`, `412` on a stale generation; a SUCCESSFUL edit could not be observed
because the lab's `backupschedules` CRD predates D1 W2 and the API fails closed against
it (422, the message logged, not returned) — the lab refresh and W8 owe that row.
Independent review `claude/d1w6.review.md`: ACCEPT-WITH-FIXES (one medium: the
preview authorisation guard had no killing test; eight low) then ACCEPT. Deviations:
merge PATCH + `expectedGeneration` instead of §5.6's `replace` (keeps the verb at
`patch`); `Schedule.generation` optional in the schema — W7 tightens it. Gaps: W7's UI,
W8's live rows, the `omitted ⇒ removed` semantics under a CRD-ahead window
(documented). Migration: none — additive routes; the manual-run sample is new.

### PLAT-07.1 — Complete the saved-connection contract

**Problem/implementation:** Public connection settings and credential handling
are scattered across forms and execution setup. Define one saved connection
contract for bootstrap servers, authentication, TLS, credential/workload
identity references and network execution context. Support write-only new
credential entry or an existing Secret reference, never secret readback.

**Acceptance:** Probe, discovery, backup and restore resolve compatible settings;
responses/downloads contain references, not credential values. **Tests:**
Plaintext/SCRAM, TLS configuration, credential rotation, namespace isolation,
redaction and conflicting configuration. **Dependencies:** None; retain the
delivered shared source-credential helper behavior.

### PLAT-07.2 — Add saved-cluster selection and health freshness

**Problem/implementation:** Free-text names cause errors and cached health can
look current. Use a searchable saved-cluster selector, explicit connection
test/refresh, observed timestamp and source/target capability information.
Preserve selection by identity while names/status update.

**Acceptance:** Forms reuse the chosen connection and show stale or failed
checks honestly. **Tests:** Deleted/recreated cluster, delayed refresh, credential
rotation, role/capability changes and namespace navigation. **Dependencies:**
PLAT-07.1, PLAT-13.1.

**Migration/safety and done evidence:** Existing KafkaCluster references remain
usable. Do not pool runner sockets across pods or infer EKS compatibility solely
from a broker metadata success; capture settings used by each tested path.

**Partial record (2026-09-17) — PLAT-07.2: the saved-cluster selector, identity
by UID, the probe vocabulary and the freshness budget landed; the task stays In
progress until "Test connection" can start a real source check (D2 W13 wires it
to a `Preflight` through the product API) and the console API can create a
contract v1 connection.** Landed in main as `8bbe4d1` (choose a saved cluster by
UID, and call a probe a probe), `c65db2d`/`baa6929` (the selector's identity
rules, freshness, both client modes and the wizard guards), `35e3dce` (read the
connections again before creating against one), `ac48919` (four live browser
journeys), `1cdefdf`/`b65f23f` (the selection contract, the probe vocabulary and
the freshness budget), `13fab98` and `e6b88a8` (review fixes). Contract
(`ui/select.js`, for PLAT-10.1/11.2): `selection = {uid, name}`;
`resolveClusterSelection(clusters, selection)` yields `none | selected |
recreated | missing` — the UID wins, a rename keeps the selection and the
request body spells the CURRENT name, a delete-and-recreate is a refusal naming
both UIDs with zero POSTs (the refusal states render an explicit empty selected
option and the hidden pair is the refused identity), a name-only reference (an
existing `sourceRef.name`) resolves and is pinned as the migration clause; every
role is offered and shown; a namespace change clears the selection; both submits
re-read the connections first and a failed re-read is a refusal that keeps the
draft (the wizard's `confirmClusters` is now guarded by rows that assert `list`
precedes `create` and that a mid-wizard recreation creates nothing). Probe
vocabulary: `reachable | not reachable | refused | probing | unknown | never
probed`, never "ready"; a connection with no observation is `never observed`
and is never `stale`; the `stale` badge starts at 630 s (twice the controller's
315 s `RE_PROBE_SECS`); the controller's reason is rendered verbatim through
text nodes; "Test connection" is a re-read and its own sentence says so. Asset
set 20 → 21 files in `Dockerfile.ui`, `check-image-ui.sh`, `chart_lint`, the
API `boundary` suite and the chart prose. Verified at `b65f23f` on main: node
197/197 (+29 rows), `check-ui-behaviour.sh` 197 with the plan golden
byte-identical, `check-ui-offline.sh` 21 files, `ui_lint` 26, `chart_lint` 28,
`doc_lint` 12, `gate_lint` 13, `boundary` 13, `chart-check`, `links`, fmt; nine
planted mutants killed (selection by name, stale as valid, a password field in a
draft, a recreated cluster silently re-selected, "ready" wording, and the
review's four). Live on docker-desktop against the lab images: 20/20 journeys
(`claude/artifacts/ui072/live-result-20260917T054314Z.json`; the reviewer
reproduced 19/19 before the fix round and re-ran after it) — selection by UID
surviving a rename across a route change, a recreated cluster refused with 0
POSTs and the draft kept, two real controller refusals
(`ConnectionConfigInvalid`, `CredentialNotRenderable`) rendered verbatim with
zero probe Jobs, a real credential rotation under contract v1 re-probed by the
controller with `spec` byte-identical, and the mid-wizard recreation creating
nothing; every namespace deleted after an owner-label and UID check. Independent
review `claude/ui072.review.md`: ACCEPT-WITH-FIXES (three medium: the refusal
states left a third cluster selected; a contradictory stale text; the unguarded
wizard re-read) then ACCEPT. Gaps: the browser cannot force a re-probe (needs a
controller annotation trigger or a check through the API — D2 W13); console
mode cannot create a contract v1 connection because `ConnectionAuthView` and
`CreateConnectionRequest` carry neither field (the page refuses by name,
`NoConsoleRoute`) — owed to the API's connection routes; the freshness budget is
a constant, not an installation setting; the rename journey is fault-injected
because object names are immutable. Migration: none — existing `KafkaCluster`
references remain usable and a name-only reference resolves.

**Completion record — Done (2026-09-16), PLAT-07.1.** Landed in main as
`6c534b2` (a projected private CA trusted by both of the runner's TLS clients),
`b53afb6` (one resolver for every runner Job), `a0725e8` (the contract docs),
`7cf50c1`/`efa8906`/`9973f5c`/`b999b74`/`199020a` (rebase proof and review fixes)
and `50e641f` (live harness `scripts/test-plat07-live.py`). Contract v1
(`docs/kubernetes.md` §20, `logweir_core::connection::CONTRACT_VERSION`):
`KafkaCluster.spec.auth` carries the mode, the username, `secretRef {name,
passwordKey}` (absent key means `password`, byte-for-byte legacy), the `tls`
switch and `tlsCa` (exactly one Secret or ConfigMap key in the same namespace,
CEL-required to sit beside `tls: true`); `weirkeeper::connection::resolve(cluster,
ConnectionUse)` is the one resolver for the probe, backup, restore (source and
target), discovery and preflight Jobs and `ResolvedConnection::project()` is the
only place that renders the password reference (a `secretKeyRef` the kubelet
resolves, never a value) and the CA projection (one variable read by both
librdkafka and the engine); conflicting shapes (`tlsCa` without `tls`, both CA
sources, `plaintext`+`tls`, `scramSha512` without a credential, malformed
bootstrap entries or Secret names) are refused by CEL or by the resolver before
any Job with a named condition, never dialled; the frozen execution inputs of
PLAT-06.1 now record the resolved CA reference (the `v1` grammar stays
byte-identical for CA-less objects) so a changed connection is
`PlanConfigMapConflict`. Verified at `199020a`: weirkeeper 395/395 (`connection`
27), runner crates and API 1175/1175, strict clippy and fmt, `just lint`,
`crds-check`/`chart-check`/`schema-check`/`render-install --check`, the TLS
hostname-verification and CA-to-both-clients pins guarded by tests whose mutants
die. Live docker-desktop (`artifacts/plat07/`, controller and runner built from
the branch, lab restored, lock 13:36:33–13:54:49Z): SCRAM with a non-default
`passwordKey`, plaintext against an in-namespace broker, TLS with a private CA
succeeding and failing at the handshake without or with the wrong CA (never a
plaintext dial), credential rotation without editing the `KafkaCluster`, a
redaction sweep over 1.7 MB of CRs, ConfigMaps, Jobs, logs and events with zero
hits, and a foreign-namespace Secret refused. Combined live leg over the merged
frozen-inputs path (`artifacts/plat07-live/`, images from `64e4bb8`, lock
14:48:08–14:59:56Z, lab restored byte-equal): 6/6 — a snapshot carrying
`source.tlsCa`, the joint conflict (a recreated cluster with a different CA is a
terminal `PlanConfigMapConflict`, zero Jobs even after the holding quota was
lifted, plan resourceVersion unchanged), rotation with `inputsSha256`
byte-identical, every receipt independently verified by the Python verifier.
Independent review `claude/plat07.review.md`: ACCEPT-WITH-FIXES (two medium: the
TLS pins were untested) then ACCEPT, PLAT-07.1 Done unconditional. Migration
(`docs/kubernetes.md` §20.5-§20.7): apply the `kafkaclusters` CRD before the
controller; legacy objects render the same Job and plan; four previously
accepted shapes are now refused with the listed reasons and are fixed by
delete-and-recreate; a CA in a Secret puts that Secret's name (never its value)
in the plan ConfigMap, a documented trade `configMapKeyRef` avoids; an older
controller ignores `passwordKey`/`tlsCa` and would dial without the CA, so
downgrade only after removing CA-bearing objects. Limitations carried forward:
no live restore run through the new resolver (D2 W10's combined leg); the
`credential` write-only builder has no caller until PLAT-17's routes; no node
placement in the execution context (contract v1 has no such setting).

**Live validation record (2026-09-18) — PLAT-07.2: the acceptance sentence and all five
listed tests PASS live on `e7d0e79`; the task stays In progress on one clause of its own
implementation text.** All 20 journeys of `scripts/plat12-13-ui-e2e.mjs` pass against the
refreshed lab (deleted/recreated cluster, delayed refresh, credential rotation, the refused
probe carrying the controller's own reason, mid-wizard target recreation, the namespace
change that clears the selection), re-run 20/20 by the independent reviewer; `ui_live.mjs`
J6 measures a second delete-and-recreate on a discovery's binding (`stale:
connectionReplaced`, both UIDs recorded); the console API creates a contract-v1 connection
(`api_live.py` C1, 201, validated field for field, the object present with the returned
UID). Evidence `claude/artifacts/d2-live/20260918T030840Z/{selector,ui,api}/`. Unmet:
"explicit connection test/refresh" — "Test connection" re-reads what the controller
recorded and its own sentence says so (`ui/select.js:538`); nothing dials until PLAT-03.1
delivers a source-connectivity check kind (only "Discover topics" starts a real check Job).

**Partial record (2026-09-18) — PLAT-07.2: "Test connection" now dials (`5bf6d25`…`33ff2eb`,
the source-connectivity check kind); the task stays In progress until that path is proved
on the lab controller.** A `Preflight` with `operation: SourceConnection` and
`sourceConnection.connectionRef` runs the `connection.*` rows through the existing check
runner (no destination, no signer, no plan, no topic; `connection.topicsDescribable`
deliberately not reported; D2 §4.2/§4.3/§6.2/§6.3/§8.3/§9 amended verbatim from the
worker's proposal); the API accepts it with contract validation; the console's detail
control creates one Preflight per click under a per-load random nonce composed with the
click ordinal (two page loads → different keys; four clicks in one in-flight create → one
Preflight; a later deliberate test → a new key — proved live by the review's API/UI
journey), renders each row's state, code, remedy, scope and check time from the real
object, and the list's per-row control is relabelled "Re-read probe" so one label no
longer means two things. Review `claude/d2-source-check.review.md` ACCEPT after a fix
round (a page-load ordinal had been reused as the key; a guard gap on the emitted row
set); 20/20 mutants; 2797 tests. Remaining: the controller/runner half live on the lab
(the lab's controller predates the operation until the next refresh), then Done.

**Completion record — Done (2026-09-18), PLAT-07.2.** Source landed on main as `ui072`
(the saved-cluster selector, identity by UID, the probe vocabulary and the freshness
budget — see the 2026-09-17 partial record), D2 W13 (the console's destination, discovery
and preflight pages) `d2-source-check` (`5bf6d25`…`33ff2eb`: the `SourceConnection`
Preflight operation, its API route, and a "Test connection" control that dials — one
Preflight per click under a per-load random nonce, the rows' state, code, remedy, scope
and check time rendered from the real object; the list's per-row control relabelled
"Re-read probe") and `ui-conn-followups` (`e20298f`…`b650885`: the check labelled by the connection it dialled so a reload finds the last one for that cluster and a recreated cluster inherits none, the uid rendered, the journey assertion pinned to the real control by a lint). Proved live on docker-desktop: the 20-journey selector run (deleted and
recreated cluster, delayed refresh, credential rotation, the refused probe carrying the
controller's own reason, mid-wizard target recreation, the namespace change clearing the
selection — 20/20 in D2 W14 and again in its review), a second delete-and-recreate measured
on a discovery's binding (`stale: connectionReplaced`), the console API creating a
contract-v1 connection, and — on lab-refresh-4 at `7b4fae9` — the dialling control on the
lab controller (`claude/lab-refresh-4.result.md` §8: one real `SourceConnection` Preflight
per deliberate click, a double click makes one, a reload and a click make a second under a
different key, the panel rendering the object's own rows) and, in the follow-up's review, a
5/5 journey — a real dial ending `AuthenticationFailed` in a Job owned by the cluster's UID
with no signer volume, nine rows all scoped, zero redaction hits, the last test shown after a
reload, none inherited after recreate — beside the 20-journey selector run at 20/20. Independent reviews: `claude/d2w14.review.md`,
`claude/d2-source-check.review.md` (a page-load ordinal reused as the idempotency key was
found live and closed; the emitted row set pinned), `claude/lab-refresh-4.review.md`.
Acceptance ("forms reuse the chosen connection and show stale or failed checks honestly")
and all five listed tests are satisfied; the implementation text's "explicit connection
test/refresh" is now a real test. Migration: existing `KafkaCluster` references remain
usable; a name-only reference resolves; the new Preflight operation is a closed-enum
addition — connectivity checks must be deleted before rolling back to a controller that
predates it (documented).

## PLAT-08 — Save backup destinations instead of rebuilding storage inputs

**Priority P1 · Proposed.** Recovery currently exposes endpoint/region/Secret
details that should have been saved before the incident.

### PLAT-08.1 — Model destination settings and access independently

**Problem/implementation:** Archive settings are duplicated and the controller
has one global read-only store. Introduce a named destination containing storage
location, endpoint, region, addressing, TLS and credential/workload identity
references; resolve the correct destination for each operation. Keep source
archive access separate from evidence storage access.

**Acceptance:** Two destinations with different settings work without global
configuration leakage; secret values are not returned. **Tests:** Distinct
endpoints/credentials, denied location, malformed URL, namespace separation
and evidence/archive destination differences. **Dependencies:** PLAT-07.1 for
shared credential conventions.

**Live validation record (2026-09-18) — PLAT-08.1: D2 W14 ran the destination matrix on
docker-desktop against images built from `e7d0e79`; the acceptance sentence and every
listed test PASS live; the task stays In progress for three named items.** Harness
`e2e/k8s/d2/` (landed as `aaf48ed`…`d3cc637`; evidence
`claude/artifacts/d2-live/20260918T030840Z/`, report `claude/d2w14.result.md`, review
`claude/d2w14.review.md` ACCEPT with an independent re-run of the two-destination Backup and
the selector). Proven with real objects, Jobs and buckets: two MinIO destinations with
disjoint buckets and three credentials each — the Job env, the frozen plan storage block
and the bucket contents disjoint (S1); denied location → `Preflight DestinationAccess`
`archiveListable/AccessDenied`, the Backup `Failed` with `exitCode 1` (S3); malformed and
refused inputs — transport/scheme, malformed endpoints, the reserved `logweir/` prefix,
`VirtualHosted`+endpoint → `AddressingUnsupportedByEngine`, both immutability refusals (S2);
namespace separation (S4); a Restore reading `lw-a` with `a-reader` and writing its
evidence to `lw-b` with `b-writer`, the scorecard verified from `lw-b`'s bytes and nothing
under `lw-a/logweir/drills/` (S5); no global-configuration leakage (S6); no secret value in
any object, API body, browser page or 60 minutes of controller log (S14.redaction and two
independent sweeps). Receipts for both destination-backed Backups fetched from their own
buckets verify with `logweir drill verify` against the roster key. Not Done because:
(1) `Backup.status.destination` (`name`, `uid`, `generation`, `locationDigest`) is absent
from the live CRD, so a recovery point publishes no frozen location and the Preflight's
`RecoveryPointLocationMismatch` is inert (NOT-RUN); (2) D2-EVIDENCE-NOTATTEMPTED-UNWRITTEN
— a `SecretKeys` grant's honest `NotAttempted` verdict never reaches `status` (defect
table); (3) D2 §15 U6, the per-role minimal-permission table, is still unmeasured (the
MinIO policies that sufficed are recorded, not bisected). `ControllerIdentity` evidence
reads and U1 (private CA through the engine) are NOT-RUN and belong to PLAT-08.2/U1.

**Partial record (2026-09-18) — PLAT-08.1: the frozen destination block landed
(`52c1f00`…`85adc2e`); the task stays In progress until the block is proved live and U6 is
measured.** `Backup.status.destination {name, uid, generation, locationDigest}` is written
once at the freeze from the same `ResolvedDestinationSnapshot` the plan's `v2` block was
rendered from (status and plan cannot disagree; a later pass skips resolution; a missing
plan with a changed digest is refused terminally), under the resourceVersion precondition,
omitted (never `null`) for legacy inline-archive runs; the CRD requires all four fields when
present; `archiveStorage` is not published (the `StorageUrl` enum has no structural-schema
form — stated in the docs). The restore preflight's `recoveryPoint.state` now compares the
point's frozen `locationDigest` with the resolved destination's — `RecoveryPointLocationMismatch`
is reachable (both public digests in the message, surviving redaction), and a point that
predates the block answers `unknown/RecoveryPointLocationUnknown`, never `ready` (D2 §6.3
amended). Review `claude/d2-status-destination.review.md` ACCEPT; 9/9 + 3/3 mutants; 1781
tests; the review also found STATUS-PATCH-NO-RV (defect table, pre-existing). Remaining:
live proof of the block and the mismatch row on the next refresh; D2 §15 U6 (the per-role
minimal permission table) still unmeasured.

### PLAT-08.2 — Inherit destination defaults and validate storage choices

**Problem/implementation:** Restore reconstructs storage settings and path-style
addressing can accidentally enable HTTP. Add destination selection and inherited
defaults to schedule/restore forms; make transport security and path-style
addressing independent explicit controls with operation-specific checks.

**Acceptance:** Selecting a recovery point restores the correct saved settings;
changing addressing never silently downgrades transport. **Tests:** HTTPS with
path-style, explicitly configured local HTTP, destination edit during a draft,
custom endpoint and archive/evidence separation. **Dependencies:** PLAT-08.1,
PLAT-03.1, PLAT-13.2.

**Migration/safety and done evidence:** Convert inline archive configuration
without changing in-flight plans or archive location. Document required object
permissions and prove two-destination operation.

**Partial record (2026-09-17) — PLAT-08.1: the destination model, resolver,
`BackupDestination` controller and controller-identity evidence store cache
(D2 W7) landed; the task stays In progress until D2 W10 wires destination-backed
execution and W14 proves two-destination operation live.** Landed in main as
`27fb924` (resolver, controller, cache), `d555cdc` (the two `backupdestinations`
RBAC rows), `65bda96`/`9727bc5` (tests), `84a20d5`/`0b25e95` (docs), `3377c98`
(review fixes) on top of the `BackupDestination` schema from `crds-shapes` and the
pure model from D2 W1. Contract: `weirkeeper::destination::resolve(destination,
role, policy)` returns, per operation role (`archiveWrite`, `archiveRead`,
`evidenceWrite`, `evidenceRead`), a complete and explicit `AWS_*` set and plan
storage blocks — never inherited from the controller's process environment
(planted `AWS_ALLOW_HTTP`/`AWS_ENDPOINT_URL` cannot reach a rendered set, which
closes SEC-ENVHTTP's controller half for destination-backed runs once W10 uses
it), `allow_http` only from the declared transport (S5), virtual-hosted addressing
with a custom endpoint refused (ENGINE-PATHSTYLE), grant Secrets referenced by
name in the same namespace and never read (a cross-namespace reference is refused
by name; a genuinely absent Secret surfaces from the pod as
`CredentialSecretNotFound`); the controller validates location, transport versus
endpoint scheme, addressing and the CA reference, writes `status` conditions and
`locationDigest` with a resourceVersion-preconditioned merge PATCH (S7); the
resolved-destination snapshot has canonical `det_json` bytes in camelCase pinned by
a golden test so W10 can add it to the frozen inputs without forking the `v1`
grammar (S4); `evidence_store::StoreCache` is an LRU of at most 32 read-only
stores keyed by destination UID, generation and CA digest, built and evicted inside
`spawn_blocking` (the I13 amendment names it as the one sanctioned construction
site) and gated by the policy allowlist; `retention_scope` is the G14 guard that
keeps a retention report from describing another destination's catalog (unwired
until W10). Verified at `0b25e95`: weirkeeper 505/505 (`destination_controller`
35), the whole workspace 1916/1916, strict clippy and fmt, `manifest_lint`,
`chart_lint`, `chart-check`, `render-install --check`; nine planted mutants
killed and a tenth defect (dropping an evicted store on an async thread) found
by the tests and fixed. Independent review `claude/d2w7.review.md`:
ACCEPT-WITH-FIXES (one high: the unit no-network gate; two medium: the missing
precondition and a snake_case leak into the snapshot) then ACCEPT. Migration: two
additive RBAC grants; the kind is served but nothing executes against it yet, so
`docs/kubernetes.md` marks execution-time behaviour as not in this build and the
per-role permission table in `docs/install.md` as unmeasured until W14.

**Partial record (2026-09-17) — PLAT-08.1 execution half: destination-backed Backup,
Schedule and Restore (D2 W10) landed; the task stays In progress until the
`Backup.status.destination` fields land (a CRD-shape follow-up) and W14 proves
two-destination operation live.** Landed in main as `c1282e8` (the Backup freeze and
Job render), `139944c` (retention scope), `53b3073` (Restore checks 5–9), `784f499` (evidence
via the destination), `cf317e5` (the runner store contract), `3cdd0e9`/`45f6e0c`/`f9fc61a`/`e98eb8a` (tests),
`f01c352`/`26dc6f2` (docs) and `7ea3513`/`379ace7` (review fixes; `f4ab8a3` labels the three U1 marks for the label gate). Contract: a run naming a
`destinationRef` resolves through `destination::resolve` per operation role,
renders the complete explicit `AWS_*` set and plan storage blocks into the Job —
never the controller's environment (a planted `AWS_ALLOW_HTTP`/`AWS_ENDPOINT_URL`
cannot reach a destination-backed Job) — carries the `evidenceWrite` credential
the runner requires (the review found the first render omitted it, so every such run
would have exited before archiving; the rendered env is now driven through the
runner's own startup loader in a test), and freezes the `ResolvedDestinationSnapshot`
into the `v2` `destination` block at the freeze (S4: the `v1`/`v2` byte
fixtures untouched; a stored plan compares the block whole; a destination edited
after the freeze changes nothing for a running run and a Job re-created from the
stored plan compares against the FROZEN block). Evidence for destination-backed runs
is read only through the controller-identity `StoreCache` inside `spawn_blocking`
(I13), allowlisted, with verdicts through D3 W10's trust projection — a
`SecretKeys`/`WorkloadIdentity` grant is an honest `NotAttempted`, never the
global handle. Legacy inline-`archive` runs render byte-identical Jobs (goldens
untouched) and deliberately keep the controller's environment forwarding, because
removing it would break every upgrade at the upgrade — pinned by a row asserting both
halves. `retention_scope` is wired so a legacy retention report never describes
another destination's catalog; ENGINE-PATHSTYLE's refusal is now reachable; the
engine's custom CA stays refused until U1 is measured; STATUS-RECORDS was left to D3
W2; RECEIPT-DUP untouched. Verified at `f4ab8a3` on main: workspace 3019/3019 at `e98eb8a`,
strict clippy and fmt, the four script gates, the four logweir lints,
`crds-check`, `chart-check`, `render-install --check`; twenty planted
mutants killed. Independent review `claude/d2w10.review.md`: ACCEPT-WITH-FIXES
(one critical: the missing evidence credential; two high: the untested evidence
source with a surviving global-handle mutant, and the chart's missing policy
ConfigMap — landed by D2 W11; three medium; six low; two questions) then the verdict
in its re-verification. STOPPED, by design, on the `Backup.status.destination`
fields (`name`, `uid`, `generation`, `locationDigest`,
`archiveStorage?`) — CRD shapes are sequenced through one worker — so the
Preflight's `RecoveryPointLocationMismatch` stays inert until that follow-up; W11's
chart carries the keys this half reads (`engine.allowUnverifiedCustomCa`,
`controllerIdentityLocations`) and the controller fails closed with a named
condition when they are absent. Live: none — W14 owes §3.13's two-destination
scenarios. Migration: nothing changes for existing objects; a destination-backed run
needs the resolver's grants.

**Partial record (2026-09-17) — D2 W12: the product API's three route families
for destinations, topic discoveries and preflights landed (PLAT-08.x, 09.1 and
03.x API halves; PLAT-17.1 stage for these kinds); the tasks stay In progress
until W13 (UI) and W14 (live) consume them and W11 ships the admission policy
the Secret-create grant needs.** Landed in main as `2a4c65b` (seventeen routes,
DTOs, problem codes, sealed-adapter extensions, role-matrix rows, the
regenerated OpenAPI document), `e176039`/`19dbb9c` (tests), `dbf3b8f`/`e432383`
(docs) and `67c4389`/`9f52249` (review fixes, two rounds) on the PLAT-17.1 skeleton. Contract for W13:
destinations list/get/create/`:update-access`/`:test`/`:from-legacy`/`/usage`; per-
connection discoveries list (`?latest=true` → `{latestAttempt, lastSuccessful}`),
create, get, `/topics` (`limit, cursor, q, prefix, internal, errored`; `page.snapshot`
= `<uid>@<topicsSha256>`; `scan.complete: false` means keep following the
cursor), `:cancel`; preflights create, get (`?planHash=`), `/details`, `:cancel`;
`operations/{kind}` grows `discovery|preflight`; the four item-level commands
are published as explicit `:command` paths like `:set-suspension`. A
destination's Kubernetes name is its `name`; `inheritsArchiveWrite`/`notConfigured`
are response-only grant modes; `writeProbe` defaults to `createOnlyMarker` in
the API and `Disabled` in the CRD; `:update-access` and both `:cancel` routes
refuse `Idempotency-Key`; `expectedGeneration` 412; the idempotency request hash
mixes the key-derived `scope_digest`, so it cannot be reconstructed from a
published object; `visibility.state` is never better than `unknown` without an
attestation (an `attestedComplete` without one is downgraded and the downgrade
recorded in `basis`); checks answer `CheckOperationResponse` with no
`verification`/`result`/`evidence`; `lastTest` selects on a
`logweir.dev/destination-test` label only `:test` sets, pages to exhaustion and
reports `truncated`; the capability flags `destinations`/`topicDiscovery`/
`preflight` mean the domain's READ floor — start/cancel comes from
`namespaces[].roles` (splitting them into read/start pairs is one commit that
must touch `ui/contract.js` too). Credentials: the console ServiceAccount gains
`get/list/create/patch` on the three kinds, `get` on `configmaps` and `create`
on `secrets` — no `watch`, no Secret read/update/delete, no `delete` anywhere,
pinned as `(verb, resource)` pairs in `linkage.rs`; a credential Secret is
created owned by its destination and the entered value appears in no response,
list, stored object or log line (the API-server echo is dropped by the parser,
now proven by a unit test that fails when `skip_deserializing` is removed); a
second `secret.new` for a role whose Secret already exists is `409
state_conflict` naming the Secret and saying the value was NOT written (this
service has `create` and nothing else) — a live replay confirmed 409 with both
`resourceVersion`s unchanged and the first value still in place; and because the
destination used to be written before its credentials, every credential name a
request would write is now probed with a `dryRun: All` create (same verb, empty
`data`) BEFORE anything is written, all taken names are reported, and a replay is
recognised only from this service's idempotency record, never from a Secret's
existence — a planted Secret is 409 on the first attempt and on the prescribed
retry, with zero destinations created. Decisions
recorded at integration: `:from-legacy` takes D2 §3.12 branch (c) — refuse
`legacy_location_unknown` — until W11 publishes the installation's legacy
addressing (a hard-coded AWS/TLS guess labelled `installationConfig` was removed);
an Administrator may cancel any check in an administered namespace, an Operator
only their own (D0 matrix amended). Verified at `9b0a599` on main: logweir-api
257/257 (+107), strict clippy and fmt, `schema-check`, one-signer, no-oso, the
five repo lint suites (108), `node --test ui/tests/contract.spec.js` 18/18 with
`ui/contract.js` unchanged; nine planted mutants killed including the review's
surviving one and its two fail-closed probes. Live on docker-desktop (isolated labelled namespaces, deleted
after owner-label and UID checks; no controller for these kinds in the lab yet):
create with idempotent replay and conflict, `expectedGeneration` 412 then 200,
the CRD's CEL refusing a `spec.storage` patch, a discovery created and cancelled
(`cancelRequested false→true`, a second cancel writing nothing), `:test`
starting a labelled `DestinationAccess` preflight, and the H1 replay above.
Independent review `claude/d2w12.review.md`: ACCEPT-WITH-FIXES (two high: the
silent no-write on rotation; the AWS guess mislabelled as installation config;
six medium; five low; one question), a second round (one high: a planted
Secret adopted on the documented retry; one medium: partial role writes) then
ACCEPT. Gaps: the
ValidatingAdmissionPolicy restricting the Secret-create grant to the two
Logweir credential `type`s (W11 — without it the grant is wider than the route
using it); `/usage` and `lastTest` only see objects the API labels;
`legacy_location_mismatch` is published but reserved; the check rate limiter is
per process; the field mapping for `visibility.state`, chunk `firstName`/
`lastName` and the `gating` spellings should be re-checked against W8/W9's
controllers now that they exist; one timing-sensitive process test flaked once
under concurrent load (reported, not a merge blocker). Migration: five additive
console-ServiceAccount grants; nothing changes for existing objects.

**Partial record (2026-09-17) — PLAT-08.2, with the UI halves of PLAT-09.1 and
PLAT-03.x: D2 W13, the console pages for destinations, topic discovery and
preflight landed; the tasks stay In progress until the API publishes the four
fields below, D1 W7 wires schedule editing, and W14 proves the journeys against
controllers that reconcile these kinds.** Landed in main as `f675706` (the typed
client and `ui/pages/destinations.js`), `9a86ebb` (the discovery panel, the
destination selector by identity, real readiness in schedules and wizard step 5),
`3ca75cb`/`14bf256` (specs), `aa53ce2` (the live harness), `e20291f` (docs) and
`6f23f5b` (review fixes). Contract (`ui/README.md`): every readiness or verdict
string is rendered from a `CheckOperationResponse` or a status field — nothing is
computed client-side (UI-FAKEPREFLIGHT); with no controller reconciling a kind the
page says pending/unknown, never ready or failed; `applicable: false` and every
stale reason including `unverifiable` are shown; `visibility.state` renders
`unknown` as the healthy default, `limited` with its count, and
`attestedComplete` only beside "attested by X at T; not verified by Logweir";
discovery follows the cursor until `scan.complete` and keeps `latestAttempt`
apart from `lastSuccessful`; cancel is rendered from roles and the server's
refusal verbatim (exact-owner cancel is enforced by the API; an `ownedByCaller`
flag is owed if the button should be disabled up front). A destination credential
is write-only: never in a draft, storage, the DOM after submit, a URL, a log or an
artifact (`sessionToken` included); the rotate form validates before submit, says
that credential inputs are always cleared, and renders the API's 409 verbatim;
`:from-legacy`'s `legacy_location_unknown` is shown as what it is. Capability
flags untouched (`ui/contract.js` byte-unchanged; start/cancel come from roles).
Verified at `246e158` on main: node 255/255, `check-ui-behaviour.sh` with the
plan golden byte-identical, `check-ui-offline.sh`, `ui_lint` 26,
`chart_lint` 28, `doc_lint` 12, `gate_lint` 13, the API `boundary`
suite 14, `chart-check`, `links`, fmt; nineteen planted mutants killed, including the review's two on the schedule
projection (the mutant script now keys on the exit status and the TAP summary — the first version
could report green for a mutant that died). Live on docker-desktop, console mode
against a loopback `logweir-api`: nine browser journeys 9/9 with the negative
control 0/9 preserved as distinct timestamped files (the review found the first
positive result overwritten by the control), reproduced independently by the
reviewer; every namespace deleted after owner-label and UID checks, including on
the harness's failure path; zero credential hits across the artifacts. Independent
review `claude/d2w13.review.md`: ACCEPT-WITH-FIXES (one high: the overwritten
evidence; two medium: on-screen text claiming landed fields did not exist, a rotate
form that skipped validation; four low; one question) then ACCEPT. Owed by the API
before these pages are complete: `CreateScheduleRequest.destinationRef`
(PLAT-06.2); the frozen destination on the `Backup` projection (PLAT-08.2, after
D2 W10); schedule coverage from `status.selection` (PLAT-09.2); a
source-connectivity check kind so "Test connection" starts a real check (PLAT-03.1,
consumed by PLAT-07.2). Schedule EDITING is deliberately not wired here — the
`PUT …/schedules` path removes omitted fields and belongs to D1 W7, and both pages
say so on screen. Migration: none — additive pages; the legacy in-cluster UI mode is
unchanged.

**Partial record (2026-09-17) — D2 W11: RBAC, the chart, the policy `ConfigMap`,
the admission policy and the delete decision landed (PLAT-03.x, 08.x, 09.1 chart and
grant halves; closes owed items from D2 W8, W9 and W12); the tasks stay In progress
until W14 proves eight named grants live and D0 stage 7 creates the console
ServiceAccount the admission policy names.** Landed in main as `a319a19` (roles),
`bc65236`/`0f9ec45` (chart), `2aefaf3`/`25a963f`/`4796348` (tests) and `b7777b2`/`9b9471c`
(docs). Decisions recorded: `delete` is granted on EXACTLY `topicdiscoveries`
and `preflights` — transient check kinds (D2 §4.3/§5.8: 24 h retention, keep-last-five
per connection) — and on nothing else; `manifest_lint`'s two delete assertions are
narrowed to "only from `gc.rs`, only these two kinds" with a crate-wide source scan
that fails on a planted `Api::delete` anywhere else (the review planted one in the
Preflight reconciler and it survived until the scan was made symmetric); `gc.rs` is
wired into both reconcilers with a bounded per-pass cap and never touches a
non-terminal object; `check-no-archive-write` stays green because `gc.rs` deletes
Kubernetes objects, never archive bytes. The `weirkeeper-policy` `ConfigMap` is
rendered by `charts/logweir/templates/policy.yaml` from values under a schema whose
bounds are pinned to the code's constants by a test that reads both (`hardMaxTopics`
≤ `MAX_TOPICS_CEILING`; the two cross-field rules refused by named template
`fail`s), lives in the release namespace only, and carries the attestation entries
that make `attestedComplete` reachable (principal + cluster id + an unexpired entry;
a blank field fails closed), the legacy addressing block D2 §3.12 (b) needs, and the
check pool limits; a policy the controller refuses is now WARNED, not silently
dropped (the review found an install that succeeded while every attestation vanished).
A `ValidatingAdmissionPolicy` + binding lets the console ServiceAccount `create` a
Secret only when its `type` is one of the two Logweir credential types and it carries
the API's owner label, fail-closed; its CEL compiles on docker-desktop
(`--dry-run=server`, nothing persisted); unset `consoleServiceAccountName` is
refused by the schema rather than rendering a binding that matches nobody; "no
`update`/`patch`/`delete` on `secrets` for any Logweir ServiceAccount" is
pinned by `manifest_lint`. Human roles gain READ on the eight new kinds; the
trust-admin role is administrator-only and cluster-scoped for `TrustPolicy` alone;
the legacy UI proxy gains its reads; `topicdiscoveries: get` is NOT granted (no
caller). Verified at `e15ddd2` on main: weirkeeper + logweir 1971/1971,
`manifest_lint` 31, `chart_policy` 8, `preflight_controller` 83, strict clippy
and fmt, `check-no-archive-write`, `crds-check`, `chart-check`,
`schema-check`, `render-install --check`, `links`, `just lint`;
twenty-five planted mutants killed. Independent review `claude/d2w11.review.md`:
ACCEPT-WITH-FIXES (two medium: the values schema looser than `Policy::validate`;
the delete narrowing enforced for one kind only; four low) then ACCEPT. Not closed,
with reasons: `Api<Event>` wiring stays the discovery controller's; the demo
destination values (D2 §10) and `logweir-retention-admin` are D3 W13's; the console
ServiceAccount the policy names arrives with D0 stage 7, so the admission policy is
inert until then (documented); keep-last-five is keyed on the connection, not the
creator, for the reason recorded in docs. Live: none — W14 owes the eight grant proofs.
Migration: additive grants and one new ConfigMap rendered from values; an existing
install without the values keeps today's behaviour.

## PLAT-09 — Discover topics and resolve all-user-topic policies

**Priority P1 · Proposed.** Scope: dynamic topic-data selection, not complete
Kafka metadata recovery or continuous capture.

### PLAT-09.1 — Provide bounded, honest topic inventory

**Problem/implementation:** Topic names are typed manually although the Kafka
reader already supports listing metadata. Add a bounded discovery operation
using the saved execution connection, returning searchable/paginated topics,
partition counts, timestamp and visibility/completeness information. Exclude
internal topics by default; avoid an unbounded Kubernetes status object. Kafka
may silently omit topics a principal cannot describe: successful listing alone
cannot prove full visibility. Define an explicit completeness policy and show
unknown or visibility-limited coverage when it cannot be established.

**Acceptance:** Users select visible topics and can distinguish empty, failed,
stale and permission-limited discovery. **Tests:** Large catalog, empty cluster,
ACL-limited principal, timeout, internal topics, refresh and credential rotation.
**Dependencies:** PLAT-07.1; PLAT-17.1 for shared-console endpoint delivery.

**Partial record (2026-09-17) — PLAT-09.1 controller half: the `TopicDiscovery`
reconciler (D2 W8) landed; the task stays In progress until D2 W11 (the three
grants and the delete decision), W12 (the API), W13 (the UI) and W14 (live
scenarios S7–S13) close it.** Landed in main as `8a2ad96` (the controller, its
owned chunks and its completeness verdict), `02ab722` (the two `topicdiscoveries`
RBAC rows), `3e79d41`/`35aaa1e` (tests), `e1683fc` (docs) and `51f6405` (review
fixes) on top of the `TopicDiscovery` schema from `crds-shapes`, the shared check
framework (D2 W5) and the check runner (D2 W4). Contract: one bounded discovery
request becomes one isolated check Job — a `topicInventory` plan rendered from
the saved `KafkaCluster` connection, the CA projected as a mount (never inlined,
because the CA may live in a Secret this controller holds no verb on) with the
same `LOGWEIR_SOURCE_TLS_CA_FILE` an execution Job gets; the pod is read only
through the Job's owner UID (S6) and the plan digest is read back from the
immutable plan ConfigMap rather than re-rendered under whatever policy is in
force now; the inventory is stored as owned immutable ConfigMap chunks with an
index bounded to the CRD's `maxItems: 64` (entries cut to `min(maxTopics,
64×2500)` before any write and reported `truncated`/`MaxTopics`; a result that
would still exceed 64 chunks refuses terminally); `status.result.visibility.state`
is `unknown` unless an administrator-governed attestation names the principal
and the cluster id (a blank principal or cluster id fails closed), `limited` only
on an observed authorization failure, and `attestedComplete` is unreachable until
W11 wires the policy environment; empty, failed, stale and permission-limited
outcomes are distinguishable from `status` alone (phase, reason, `freshUntil`,
`binding` written once and `binding.policyDigest` updated at commit); every
status write is a resourceVersion-preconditioned merge PATCH (S7) and the Job TTL
is set only after a status that landed; cancellation is `spec.cancelRequested`
on an otherwise CEL-sealed spec; a stall guard marks `Failed/Stalled` when a
non-terminal object's Job has vanished. RBAC: `topicdiscoveries: [list, watch]`
and `/status: [patch]` only — no `get`, no `events`, no `delete`. Verified at
`35aaa1e` on main: weirkeeper 601/601 (`topic_discovery_controller` 49),
`manifest_lint` 29, `chart_lint` 28, `doc_lint` 12, `crds-check`, `chart-check`,
`render-install --check`, strict clippy and fmt; nine planted mutants killed (six
original, three from the fix round). Independent review `claude/d2w8.review.md`:
ACCEPT-WITH-FIXES (two medium — the chunk index could exceed the CRD's
`maxItems`; the TTL was patched after a refused terminal commit — and six low)
then ACCEPT with nothing outstanding. Live: none here — W14 owes D2 §14 S7–S13
(large catalog with a spoofed pod, internal topics, ACL-limited principal, empty
cluster, timeout, refresh with credential rotation, cancel) and the `[VERIFY
U3]`/`[VERIFY U4]` measurements. Deviations recorded: `gc.rs` (24 h retention,
keep-last-five per connection) is pure and tested but unwired because the
control plane grants `delete` on nothing but the two transient check kinds (`topicdiscoveries`, `preflights`) since D2 W11 and `manifest_lint` asserts that twice —
narrowing it is W11's RBAC decision, so terminal objects accumulate until then;
W12 computes `stale` from `freshUntil` plus binding drift plus supersession and
serves `latestAttempt` and `lastSuccessful` separately; W13 renders `unknown` as
the healthy default and labels an attestation "attested by X at T; not verified
by Logweir". Migration: two additive RBAC grants; the kind is served and
reconciled; nothing else executes against it yet.

**Live validation record (2026-09-18) — PLAT-09.1: seven of nine acceptance/test items
PASS live on `e7d0e79`; two FAIL on one controller defect; stays In progress.** PASS: large
catalog (S7 — 5,003 topics in three immutable owned chunks, every chunk's sha256 equal to
its annotation and index entry, the union exactly the broker's user-topic set), empty
cluster (S10), ACL-limited principal (S9 — `limited`, basis `expectedTopicNotAuthorized`,
exactly `orders` visible; with nothing expected `unknown`/`listingOnly`), internal topics
(S8), refresh and GC (S12 baseline→failure→refresh, five kept per connection), selecting
visible topics from the stored inventory on screen (`ui_live.mjs` J1/J2), and the four
states empty/failed/stale/permission-limited rendered distinctly from real objects
(J3–J6; the reviewer notes J4's recorded reason is a cancellation, so "failed with the
reason" rests on S11/S12). FAIL: timeout (S11) and credential rotation (S12) — the object
says `ResultUnreadable` while the runner's relayed frame says `BrokerUnreachable` /
`AuthenticationFailed` (D2-RESULTUNREADABLE, `topic_discovery.rs:1821`); the rotation
itself is proven end to end (Secret updated → `Succeeded`, the earlier success readable
throughout). Not proven: `S9.attested` (`attestedComplete` needs an entry in the shared
installation policy). Evidence `claude/artifacts/d2-live/20260918T030840Z/`.

**Completion record — Done (2026-09-18), PLAT-09.1.** Source landed on main as D2 W4 (the
check runner), W8 (`TopicDiscovery` controller: bounded, chunked, owner-labelled
inventory with a completeness basis), W12 (the discovery routes), W13 (the console's
discovery page and the four distinguishable states) and the wave-9 fix `1da8faf`
(D2-RESULTUNREADABLE: a relayed `notReady` result projects its own code as
`status.reason`). Proved live on docker-desktop: D2 W14 against `e7d0e79`
(`claude/artifacts/d2-live/20260918T030840Z/`, review `claude/d2w14.review.md`) — large
catalog (5,003 topics in three immutable owned chunks, every chunk digest equal to its
annotation and index entry, the union exactly the broker's user-topic set), empty cluster,
ACL-limited principal (`limited`, basis `expectedTopicNotAuthorized`, exactly the visible
topic; with nothing expected `unknown`/`listingOnly`), internal topics, refresh with GC (five
kept per connection), selecting visible topics from the stored inventory on screen and the
empty / failed / stale / permission-limited states rendered from real objects; and
lab-refresh-3 against `c6422a7` (`claude/lab-refresh-3.result.md` §8.4, review
`claude/lab-refresh-3.review.md` CLOSED-CONFIRMED) — timeout (S11: `BrokerUnreachable` with
message and remedy on the object), credential rotation (S12: `AuthenticationFailed`, then
`Succeeded` with the same 5,003 topics once the Secret was updated) and the API's timeout
projection (T2). Acceptance ("users select visible topics and can distinguish empty,
failed, stale and permission-limited discovery") and all seven listed tests are satisfied.
Stated deviations: D2 §14.4 S11's "Job condition `Failed`" criterion is amended — the runner
exits 0 after relaying a `notReady` result, so the Job is `Complete` and the failure is
carried in the projected reason; D2's `attestedComplete` verdict (`S9.attested`) needs an
entry in the installation policy and is proved by its unit rows only. Migration: none —
discoveries are new objects; a `Failed` discovery from an older controller keeps its stored
reason.

### PLAT-09.2 — Resolve explicit and dynamic selection per run

**Problem/implementation:** An all-topics promise cannot be a one-time list or
an ambiguous empty engine selector. Add selected-topics and all-user-topics
policy modes with explicit exclusions. Discover afresh for dynamic runs and
freeze the exact names and discovery result in each Backup; define behavior
when completeness cannot be established.

**Acceptance:** A newly created user topic enters the next dynamic backup;
failed/partial discovery never silently claims whole-cluster coverage. **Tests:**
Topic creation/deletion between runs, excluded/internal topic, empty resolution,
ACL limitation, discovery/execution race and immutable snapshot. **Dependencies:**
PLAT-09.1, PLAT-05.1, PLAT-06.1.

**Migration/safety and done evidence:** Preserve existing named allowlists.
Do not pass wildcard/omitted topics to the engine. Record selection semantics
and a live new-topic-in-next-run demonstration.

**Partial record (2026-09-17) — PLAT-09.2: dynamic selection per run (D1 W5)
landed; the task stays In progress until W8 proves L-09-1…6 live and W11 wires the
attestation environment.** Landed in main as `f22ab51` (the `AllUserTopics` arm of the
W3b seam, `controllers/backup_selection.rs`), `3a949dd`/`58a6235`/`00abb3f`/`7072b9d`
(tests), `bd19729`/`6cf44bf`/`ab8d01d` (docs) and `242018d`/`4c8a8d5` (review fixes). Contract
(`docs/kubernetes.md` §10 "selection"; D1 §7.3 amended): a dynamic `Backup`
discovers afresh, per run, BEFORE the freeze, through D2's one check runner —
`logweir check run` with plan kind `topicInventory` rendered by `check::plan::build`
from the PLAT-07.1 resolver's connection (the CA projected as a mount) into a plan
ConfigMap `lwd-<backup-uid>-plan` and a Job `lwd-<backup-uid>` both owned by the
`Backup`, labelled `logweir.dev/purpose=topic-discovery` (never
`component=check`, which would spend the console's pool); the pod is read only
through the Job's owner UID (S6) and the full log through `check::relay` within
the framework's budget; a `TopicDiscovery` object's result is never an input (S2).
Resolution (D1 §7.2 R5): internal (`internal` flag or a `__` prefix), limited
(authorization-failed entries) and rule-excluded (literal exact or prefix) names
are removed, the rest byte-sorted and deduplicated; every resolved name must be
Kafka-legal (`^[a-zA-Z0-9._-]{1,249}$`, `logweir_core::guard`) — refused as
`DiscoveryResultUnreadable` at resolution and again at the freeze rail beside the
empty and glob rails, so every producer meets them. Visibility is the runner's
`unknown|limited`, upgraded to `attestedComplete` only through the same
`check::policy` attestation predicate D2 W8 uses (unreachable until W11 wires the
policy environment, and said so); `incompleteDiscovery: Refuse` ends
`DiscoveryIncomplete`, `BackUpVisibleTopics` runs with coverage
`VisibleUserTopicsOnly`; an empty resolution is `SelectionEmpty` with no runner Job
and no retry; more than 5 000 names or 256 KiB — or a TRUNCATED listing, which
cannot prove the set — is `SelectionTooLarge` (the two share a reason and are told
apart by the message). The source is resolved once and compared at the freeze
(`SourceChangedDuringResolution` on a changed resolver digest or reported cluster
id — `status.clusterId`, which the probe clears on any unreadable pass, is NOT in
the digest, so probe churn cannot kill a good run); a `spec.deadlineSeconds` under
120 s is refused up front rather than dispatching a Job with a one-second budget; a
finished Job with no recorded finish instant is refused rather than stamped `now`.
The freeze fills the amended §3.3 `selection` block (mode, coverage, counts,
exclusions, `incompleteDiscovery`, the bounded `discovery` summary with its
`resultSha256`) beside the single top-level `topics`, `status.selection` goes in
the same patch as `status.execution`, the discovery Job's TTL is patched only
after that patch — or the terminal refusal — landed (S7 ordering), and a frozen run
re-reads its stored selection and never re-runs discovery. Every status write on
this path is resourceVersion-preconditioned (S7). No RBAC change. Verified at
`7072b9d` on main: weirkeeper + `logweir-core` 1063/1063 (`backup_selection` 30,
`backup_controller` 85, `topic_discovery_controller` untouched), `manifest_lint`
29, `doc_lint` 12, `chart_lint` 28, `check-no-archive-write`, `check-pure-core`,
`crds-check`, `chart-check`, `render-install --check`, strict clippy and fmt;
thirty-six planted mutants killed. Independent review `claude/d1w5.review.md`:
ACCEPT-WITH-FIXES (one high: the probe-cleared cluster id in the source digest;
three medium: the missing Kafka-legal rail, run discoveries invisible to the
per-connection ceiling, the deadline floor; seven low; one question) then ACCEPT.
Deviations recorded: D1 §7.3's `logweir topics discover` superseded by S1 (amended,
with §7.2 R2's plan-ConfigMap and relay-budget clauses); the Job name is
`lwd-<uid>`, not the framework's; the plan budget is the ceiling minus a 90 s
margin. The run-discovery Job also carries
`app.kubernetes.io/component=run-discovery`, and the interactive controller's one
listing selects `component in (check, run-discovery)`, so run discoveries count
toward the connection's ceiling while the console pool's admission still selects
only `check`; the provenance name lists in the frozen `discovery` block are
bounded in bytes as well as entries. Gaps: live L-09-1…6 are W8's (L-09-1's `selection.topics` criterion must be read as the
top-level `topics`); the runner half never ran live here. Migration: none —
named allowlists are untouched and every `v1` plan keeps running; a `Backup`
refused for dynamic selection before this landed must be recreated.

**Live validation record (2026-09-18) — PLAT-09.2: the dynamic half FAILS live on
`e7d0e79` on one controller defect; only the named allowlist PASSES; stays In progress.**
Proven: L-09-6 — a named allowlist runs with no discovery Job, `selection {mode:
SelectedTopics, coverage: NamedTopics}`, frozen `topics: ["t1"]`. FAIL: L-09-1, L-09-2 and
L-09-4 — every dynamic `Backup` ends `Resolving → Failed`, `TopicsResolved=False /
DiscoveryFailed`, because its discovery Job names the compile-time image pin under
`imagePullPolicy: Never` (`ErrImageNeverPull`, then the check deadline) while the runner Job
of the same controller and minute runs the configured image (D1-DISCOVERY-IMAGE,
`backup_selection.rs:1022`/`:1307`; defect table). NOT-RUN: L-09-3a/3b (need the proxy and
the fix), L-09-5 (needs a KRaft broker with `StandardAuthorizer` and a SCRAM principal
denied `Describe` on one topic — credentials are set at storage-format time, so a build of
its own). The acceptance sentence ("a newly created user topic enters the next dynamic
backup; failed or partial discovery never claims whole-cluster coverage") is not
demonstrated in either half; nothing measured disproves the resolution, exclusion, coverage
or `SelectionEmpty` rules — they are untested behind the one located defect. Evidence
`claude/artifacts/d1-live/20260918t0330z/objects/L-09-*/`.

**Partial record (2026-09-18) — PLAT-09.2 console half: D1 W7 landed.** The run list
renders coverage from `status.selection {mode, coverage}` alone ("named topics", "all user
topics — complete" / "— incomplete discovery") and the discovery failure reason when
`TopicsResolved=False`, plus `trigger.kind` (Scheduled / CaughtUp / Retry n of m / Manual)
from `spec.trigger`; nothing is inferred from a topic count (mutant-pinned). Residue before
Done: the dynamic half's live proof after D1-DISCOVERY-IMAGE's fix (L-09-1/2/4), then
L-09-3a/3b and L-09-5.

## PLAT-10 — Make schedules the everyday protection workspace

**Priority P1 · Proposed.** Scope: an integrated schedule experience backed by
the earlier operator contracts, not duplicate orchestration in the browser.

### PLAT-10.1 — Guide schedule creation and editing

**Problem/implementation:** Raw cron and free-text infrastructure fields make
the first run difficult. Compose saved cluster, discovered coverage, destination,
cadence/next-run preview and readiness into a short form with advanced options
collapsed. Reuse it for future-policy editing and first-backup submission.

**Acceptance:** The standard path requires no YAML, raw endpoint reconstruction
or manual signing step; invalid fields retain the draft. **Tests:** Selected and
all-user-topic creation, edit, invalid cron, readiness failure, keyboard use and
first-run redirect. **Dependencies:** PLAT-04.2, PLAT-05.1, PLAT-06.2,
PLAT-07.2, PLAT-08.2, PLAT-09.2.

### PLAT-10.2 — Add schedule detail and recovery-point history

**Problem/implementation:** Schedule cards omit practical recovery actions.
Show source, destination, policy, last successful point and age, next run,
active work, failures and filtered history. Add Back up now, Restore,
Pause/Resume and Edit; give each recovery-point row its own Restore action.

**Acceptance:** Actions retain schedule and recovery-point context; unavailable
archives and incomplete evidence are distinguishable from healthy points.
**Tests:** Empty history, running/failed/verified runs, paused schedule, archived
schedule and navigation to an older backup. **Dependencies:** PLAT-06.2,
PLAT-11.1, PLAT-14.1; PLAT-15.1 adds durable catalog-backed history.

**Migration/safety and done evidence:** Keep existing deep links working or
redirect them explicitly. Demonstrate create → backup → schedule detail →
restore without reconstructing configuration.

## PLAT-11 — Make restore selection explicit and stable

**Priority P1 · Proposed.** Scope: recovery-point and target selection; advanced
in-place recovery and crash resume remain product expansion.

### PLAT-11.1 — Bind the wizard to a selected recovery point

**Problem/implementation:** Initialization selects the newest successful Backup
across a namespace rather than an explicit user choice. Accept a stable point
identity from schedule/backup links, add a real selector, and display coverage,
topics, source and availability. Keep the choice stable while newer backups
arrive; constrain the requested timestamp to disclosed archive coverage.

**Acceptance:** An older selected backup remains selected throughout review and
submission. **Tests:** New completion during the wizard, missing selected point,
gapped/unavailable coverage, inclusive boundary, different schedules and empty
catalog. **Dependencies:** None for existing Backup history; PLAT-15.1 for
imported points.

**Completion record — Done (2026-09-16), PLAT-11.1 (imported points deferred to
PLAT-15.1 by the dependency above).** Landed in main as `5a6b1a8` (the wizard bound
to a chosen recovery point), `a447791` (live journeys), `3fd6985` and `02426c8`
(review fixes), alongside `6c2c95e` (D2 W13a). Contract (`ui/README.md`): the route
is `#/restore?ns=<ns>&backup=<name>&uid=<uid>` — the Backup UID is the identity, the
name is for display and refusals, a name-only link pins the UID it resolved; history
rows and schedule cards carry a "Restore this point" link built by one helper; with
no identity the wizard shows a searchable newest-first selector of `Succeeded`
Backups (schedule, slot, topics, records, signed verdict, availability as the run
recorded it) or an empty state; the selection never changes while newer Backups
arrive, across re-reads, route changes and reloads; the requested timestamp is
constrained to the point's disclosed window with an inclusive boundary and an
out-of-window value is a field error that keeps the draft; a missing or
non-`Succeeded` point is a refusal naming it with no plan, no hash, no submit and
never a substitution; the reviewed plan hash changes with the point. Verified at
`02426c8`: node rows 104/104 (ten new), `check-ui-behaviour.sh` 104 with the plan
golden byte-identical, `check-ui-offline.sh` sixteen files, `ui_lint` 26, `chart_lint`
28, the three demo lints, `chart-check`; thirteen mutants killed including two
planted on the reachable re-read path after review. Live docker-desktop browser
journeys (own namespace, lab controller reconciling, no lock): 14/15 twice
(`artifacts/ui-restore-selection/live-result-20260916T142437Z.json` and
`…T145850Z`): selector with zero POSTs and no plan; a history link pre-selects by
UID; an older point survives a newer Backup completing mid-wizard across a route
change and a reload with the same plan hash three times; path-style leaves
`allow_http: false` in the plan bytes the API server holds; a deleted point is
refused without substitution; a failing negative control against the pre-change
UI. The 15th journey (a forged subject binding refused terminally by the
controller) failed only because the shared lab controller image predated the
`ApprovalSubjectMismatch` check; after the lab images were rebuilt from main
`c1d3411` (`claude/lab-refresh.result.md`) the full harness passed 15/15
(`artifacts/lab-refresh/`, run `20260917T024927Z`, `Failed/ApprovalSubjectMismatch`
with zero Jobs). Independent review `claude/ui-restore-selection.review.md`:
ACCEPT-WITH-FIXES (seven low) then ACCEPT. Migration: none (static assets, sixteen
files; deep links to `#/restore?ns=` still work and land on the selector; the
`plat13` harness and the demo steps were updated to enter the wizard on a point).
Limitations: "archive availability" reflects what the run recorded, not the bucket
(PLAT-15.1's catalog is the fix); intra-window coverage gaps are not
representable until PLAT-15.1.

### PLAT-11.2 — Preview target topic subset, mapping and recovery limits

**Problem/implementation:** Users cannot readily choose a topic subset or see
what recovery changes. Add subset selection from the point's frozen topic list,
saved target selection and exact new-name mapping preview. Display target
replication/recovery settings, sampled verification scope and consumer cutover
limitations; offer a fresh-target retry after failure.

**Acceptance:** Submitted topics/mapping equal the preview, and no existing
target topic is overwritten by the ordinary path. **Tests:** Subset restore,
duplicate mapping, collision, invalid prefix, target change, stale preflight,
failed retry and timestamp boundary. **Dependencies:** PLAT-11.1, PLAT-07.2,
PLAT-03.2.

**Migration/safety and done evidence:** Preserve approved byte identity after
review; any material edit requires new approval where configured. Demonstrate
an older-point subset restore and clearly identify unimplemented resume.

## PLAT-12 — Unify restore creation, approval and retry

**Priority P1 · Proposed.** Scope: a complete submission journey; trust policy
is PLAT-19 and approved artifact transport is PLAT-01.

### PLAT-12.1 — Make one submission produce visible durable progress

**Problem/implementation:** Create discards its success route while Request
approval navigates independently. Use one guided action that validates the
reviewed plan, creates an idempotent durable request, then routes to execution
or Awaiting approval according to policy. Disable duplicate submission while
pending and recover after a lost response.

**Acceptance:** Every accepted click ends on the correct durable operation;
refresh/reconnect does not create another restore. **Tests:** Success redirect,
double click, lost response, browser refresh, API rejection and approval-required
submission. **Dependencies:** Existing helper and approval contracts suffice
for the immediate success-transition and create-before-approval fixes; land
those regressions first. PLAT-11.2 and PLAT-13.2 supply the complete selection
and mutation flow. PLAT-19.2 is required only for integrating configurable
approval policy, not for correcting today's button behavior.

### PLAT-12.2 — Correct approval subject handling and explicit retry

**Problem/implementation:** The approval form can display an editable subject
but submit the route subject. Make subject and plan identity explicit and
consistent, enforce the intended binding server-side, and show pending/denied/
expired decisions. A standalone Approvals visit must offer selection or a clear
empty state instead of assuming a subject exists in the route. Retrying failed
work creates a new execution with deliberate
fresh-target/approval handling rather than colliding with the old name.

**Acceptance:** The displayed subject is exactly the authorized subject; a
standalone visit is usable, and a retry never silently reuses an approval bound
to another execution. **Tests:** Empty/standalone route, edited/forged subject,
route mismatch, expiry, denial, retry identity and approval
scope rejection. **Dependencies:** Subject consistency and standalone-route
corrections can land under existing approval semantics immediately. The complete
retry path uses PLAT-01.2 and PLAT-06.1 identity conventions; configurable
policy integration uses PLAT-19.2. Record partial completion as such until
each slice's acceptance evidence is present.

**Migration/safety and done evidence:** Do not reuse signatures when their
subject binding changes. Capture an authorized flow, a pending-approval flow
and a rejected subject mismatch.

**Partial record (2026-09-16) — PLAT-12.1 immediate slice and PLAT-12.2 subject
slice landed with PLAT-13.2 (`3cca821`, `6b34af9`, `8020876`); both tasks stay
In progress.** 12.1: `Create the Restore` is the one guided action (validate →
reviewed-plan hash check → idempotent create → route to the operation view when
the referenced Approval already authorises exactly this Restore, else to Awaiting
approval); double click, lost response, refresh and API rejection are covered by
node rows and the live journeys above. Not done: the complete selection flow
(PLAT-11.2) and configurable policy routing (PLAT-19.2). 12.2: the approvals page
derives kind/name/UID/plan hash from the Restore, refuses an edited or forged
subject (0 POSTs, live), refuses a route that disagrees with its Restore, offers a
standalone selection or an empty state even when the approvals list is unreadable,
renders absent/awaiting/verified/refused/expired and the never-reused
foreign-subject/foreign-execution/plan-mismatch bindings, and the controller now
refuses an Approval whose `spec.planHash` names another plan (`PlanHashMismatch`,
live: a forged binding written directly to the API left the Restore
`Failed/ApprovalSubjectMismatch` with no Job). Not done: retry identity (a
fresh-target/new-approval retry) and the verified-approval live route (the lab
approver key is not available; covered by node rows over a fake cluster).

## PLAT-13 — Prevent stale UI actions and preserve user work

**Priority P1 · Done (2026-09-16).** These correctness fixes can land before the new
API or a frontend framework change.

### PLAT-13.1 — Isolate navigation and namespace request lifetimes

**Problem/implementation:** Delayed mounts can overwrite a newer namespace
view and attach actions using an old namespace. Cancel superseded reads, check
navigation generation before rendering/binding, dispose subscriptions on exit,
and verify action context at submission. Initialize namespace from an explicit
installation/user selection rather than an unrelated hardcoded default; offer
only authorized namespaces without requiring cluster-wide namespace listing.

**Acceptance:** No response or action from an old view can affect the current
namespace. **Tests:** Slow A response after switching to B, rapid routes,
back/forward navigation, unmount, namespace deletion, restricted namespace
permissions and stale submit callback.
**Dependencies:** None.

**Completion record — Done (2026-09-15).** Implementation landed in
`a2bf522`; no PLAT-13.1 production or harness file changed through
`4956785d00d7` except the later README port-forward/Bash 3.2 hardening, which
`ui_lint::chart_readme_ui_harness_binds_readiness_and_cleans_up_on_bash_3_2`
now guards. Verified at `4956785`: `ui/tests/lifecycle.spec.js` 7/7 (delayed A
after B, rapid routes and back/forward, hash ownership, route-exit disposal,
stale form callback, restore preparation after exit, explicit namespace
source), `scripts/check-ui-behaviour.sh` 61/61, `scripts/check-ui-offline.sh`
16 files clean, `ui_lint` 26/26, `chart_lint` 27/27. Live docker-desktop
evidence against the same source: `/tmp/plat13-e2e-authoritative.md` and
`/tmp/plat13-harness-fix-report.md` (delayed live GET, namespace deletion 403,
restricted RBAC with `kubectl auth can-i`, delayed POST and restore
preparation after navigation, cleanup), accepted by the final UI/harness
reviews in `/tmp/logweir-roadmap-run/`. CI run 35019727967 passed and
published `vladyslavhaina/logweir-ui:sha-4956785d00d74fe960c84d396d2eff852c68ebd8`
(`sha256:fb2b9467285b190f1bb872959c474a0001ce284f1f525a38c4d2ec6f8f83dfbe`,
revision label matches). Closure verification:
`/tmp/logweir-roadmap-run/claude/closure-0413.result.md`. Migration: none;
production assets remain static and same-origin. Limitations: the literal
`ns=default` approval handoff is covered by a behaviour test only; the full
multi-role chart was not installed beside the lab singleton controller (the
UI template was installed through a disposable wrapper).

### PLAT-13.2 — Preserve drafts and standardize mutation state

**Problem/implementation:** Error rendering discards inputs and permits unclear
retries. Preserve non-secret drafts through validation/network errors, show
field-level errors, track pending/success/failure consistently and use submission
idempotency. Define what survives navigation/refresh without persisting passwords
or secret-bearing form content in browser storage.

**Acceptance:** Recoverable failures retain safe input; repeated requests do
not duplicate operations or expose credentials. **Tests:** Invalid field,
timeout, conflict, retry, refresh, secret-entry failure and double click.
**Dependencies:** None for local form behavior; PLAT-17.1 for shared mutation
idempotency.

**Migration/safety and done evidence:** Keep read cancellation separate from
durable operation cancellation. Attach a delayed-response namespace regression
and error/retry browser journey.

**Completion record — Done (2026-09-16), PLAT-13.2.** Landed in main as
`2a34abd` (drafts and one mutation state), `3cca821` (guided submit, approvals
page bound to its subject), `9018782` (live browser harness
`scripts/plat12-13-ui-e2e.mjs`), `6b34af9` (controller: `Approval.spec.planHash`
is compared) and `8020876` (review fixes). Contract (`ui/README.md`): drafts live
in page memory keyed by namespace and form, survive validation/API/network
failures and route changes, never a reload, never an unlisted field and never key
material (`localStorage`/`sessionStorage`/cookies stay forbidden by
`check-ui-offline.sh` and `ui_lint`); one `createMutation()` state machine per
form with `invalid | conflict | rejected | refused | unknown` failure kinds; the
pending guard, not the disabled button, prevents duplicate submission; creates
are idempotent by typed or plan-derived names and a `409 AlreadyExists` is
resolved by spec comparison; a timeout reports an unknown outcome and a late
answer still settles the record with the durable link; read cancellation stays
separate from durable acknowledgement. Verified at `8020876`:
`ui/tests/*.spec.js` 94/94 (27 mutation rows added), `check-ui-behaviour.sh`
94/94 with the plan golden byte-identical, `check-ui-offline.sh` 16 files,
`ui_lint` 26/26, `chart_lint` 27/27, `approval_controller` 34/34, strict clippy
and fmt clean; twelve mutants killed across the two review rounds. Live
docker-desktop browser journeys (Chromium via `kubectl --context docker-desktop
proxy`, own namespace, lab controller reconciling): 10/10 before the review
fixes (`artifacts/ui-correct/live-result-20260915T231433Z.json`) and 10/10 after
them (`live-result-20260916T133923Z.json`, namespace UID `48492aa0…`, deleted
after an owner-label and UID check), with a failing negative control against the
pre-change UI; journeys cover invalid field keeping the draft, double click
creating one object, a real lost 201 resolved by retry to the same UID,
navigation neither cancelling an accepted create nor letting a left form write,
and the wizard resubmission after reload creating nothing. Independent review
`claude/ui-correct.review.md`: ACCEPT-WITH-FIXES (six low) then ACCEPT after
`8020876`. Migration: none for the UI (static assets, sixteen files unchanged);
for the controller the `planHash` check is fail-closed and needs no conversion
(a previously verified approval whose recorded hash disagrees flips to
`Verified=False`; rollback restores the old behaviour). Limitations: drafts do
not survive a reload by design; a headerless key blob (DER/PKCS#12) cannot be
recognised by the text guard and the README says so; the 30 s timeout is not
configurable; the wizard's mount-time draft drop after a late success while
away is a documented edge.

## PLAT-14 — Show protection health and durable execution progress

**Priority P1 · Proposed.** Scope: truthful status and actionable operations;
exhaustive data verification is a separate capability decision.

### PLAT-14.1 — Define and deliver user-facing operation states

**Problem/implementation:** Cron readiness, process success and evidence validity
are easily conflated. Expose configuration/readiness, queued/preparing/running/
verifying/terminal states, current reason, last update and bounded diagnostics.
Use reconnectable polling or streaming, stop it on navigation, and surface pod
mount/scheduling failures as useful resource-scoped errors.

**Acceptance:** Refresh resumes the same operation; success, verified evidence
and incomplete verification are separately legible. **Tests:** Mount failure,
unschedulable pod, engine crash, verification downgrade, stream disconnect,
refresh and completed Job cleanup. **Dependencies:** PLAT-03.1, PLAT-13.1;
PLAT-17.1 for shared status endpoints.

**Partial record (2026-09-17) — PLAT-14.1, PLAT-14.3 and PLAT-19.2 runner half: D3
W5, execution contract v2 with the progress channel, the recovery-point binding and
the standing-authorization scope check; the tasks stay In progress until D3 W2
(controller-side progress and diagnostics), W7 (the rehearsal controller that
PRODUCES the signed authorization document) and W14 (live) land.** Landed in main
as ``/`f5e2e66` (`logweir_core::execution_contract` v2: `ContractVersion {V1, V2}`
explicit in the type, point binding, the scope predicate, the progress grammar),
`51f5b70`/`cbab961` (the runner half), `b4bcbcd`/`057d9b0` (tests) and `c3f7ddf`/`7e13fdc` (docs).
Contract (`docs/stability.md`, `docs/formats/`): `VERSION = "2"`; a `v1` invocation
is accepted only when it carries NO v2 material (five optional environment items and
`source.point` are each refused by name under `v1`), the only test a credential-less
runner can perform, and a `v1` document still loads and verifies byte for byte (S4);
the three bundle members are digest-pinned and verified before any data-plane work.
Stdout grammar: `progress-contract=2` once (ratified — on the restore path it is the
execution contract version, otherwise the binary's own; D3 §2.4 amended), then
`progress-phase=<n>:<name>` from a closed vocabulary (restore 0–9; backup
`-1:admit|engine|readback|sign|upload`) within a 96-byte bound; `teardown-key=` only
when phase 9 attested, before I8's trio, also on exit 2, and never after a
`refusal-reason=` line (I9's "final line for exit 3" holds). Point binding: the plan's
bound recovery-point id and receipt digest must match what the archive holds — a
manifest that hashes to something else is `GuardRefused` "hashes to", a manifest the
archive does not hold is refused — before any data-plane work. Standing authorization
(PLAT-19.2, D3 §4.3): the bundle member is the SIGNED authorization document — an
envelope, its DSSE sidecar and the trusted public keys, each digest-pinned — and the
runner verifies the signature over the envelope BEFORE parsing it, judges the
verifying key's usage (an `EvidenceSigning` key is `KeyUsageMismatch`, not a bad
signature), checks kind, subject UID, window and a 90-day cap, then derives the scope
and refuses a rendered plan outside it (`execution_contract::plan_within_scope(&PlanScopeFacts,
&RehearsalScope)` over prefix, target cluster id — exactly the signed id, no
prefix or glob — topics, mode, `max_partitions` (absent ⇒ unbounded ⇒ mismatch),
records per partition; D3 §4.3(d) amended to the landed symbol); the schedule UID is
a verified binding, `templateDigest` stays the controller's check. Verified at `63fe262`
on main: `logweir-core` + `logweir` 1200/1200 (+80), strict clippy and fmt,
`check-pure-core`, `check-one-signer` (verification only, signing crates
unchanged), `check-verifier-parity`, `verify-py` 106, `doc_lint`/`gate_lint`/
`withdrawn_claim`; fourteen planted mutants killed (five original, eight from the fix
round including the review's surviving manifest-digest one, and the review's
confused-deputy mutant — the usage judged is the VERIFYING key's, proven by a
two-key keyring row in both directions; two real defects found by the original five). Independent review `claude/d3w5.review.md`: ACCEPT-WITH-FIXES
(one high: the standing scope was not signature-verified at the runner — fixed by
landing the signed document here rather than deferring to W7; four medium; five
low) then ACCEPT after one test-only follow-up. Hand-offs recorded: D3 W2 must raise or budget weirkeeper's
`KEY_SCAN_TAIL_LINES` (a passing restore now prints seven of the eight trailing
lines — a runner-side row pins the count); W7/PLAT-19.2 must produce the signed
document exactly as `claude/d3w5.result.md` §R1.3 states, and should say in signed
material that a run is a rehearsal (review F8). Live: none here — W14 owes an old
runner refusing a v2 invocation and a rehearsal refused out of scope against a real
archive. Gaps: no declared terminal state for the new refusals (`TERMINAL_STATES` is
W2's); the receipt signature is not yet judged against the trust bundle (D3 W10).
Migration: additive; every `v1` plan keeps running.

**Partial record (2026-09-17) — PLAT-14.1 controller half: operation state,
diagnostics and progress in both Job-backed reconcilers (D3 W2) landed; the task
stays In progress until W11 normalises and streams it, W12 shows it, and W14 proves
it live.** Landed in main as `8290d06` (`weirkeeper::diagnostics` — ONE pure
derivation from a Job, its owner-UID pod, container states, events and the bounded
tail to a closed diagnosis), `7f83000` (both reconcilers), `80991b8`/`e09739b`/`fa452bc`
(tests, including the rows that fixed three mutant survivors — two guards that could
not fail and a fixture failing two conditions at once) and `8be3c57` (docs §10/§11).
Contract for W11/W12: additive, bounded status — stage, progress (`phase` from
the ratified `progress-contract=2` grammar within its 96-byte bound; an unknown
phase, a malformed or out-of-order line and an absent contract line are recorded
diagnostics, never failures; an old runner yields no progress and no error), the
closed diagnosis vocabulary (image pull, unschedulable, OOM, deadline, evicted, a
refusal with its `refusal-reason=`, signing/lock, crashed) with events listed by
the Job's UID and `count` meaning heartbeats; fail-fast surfaces a Job that cannot
start within one requeue; a finished Job missing its TTL gets it patched only after
the terminal status landed; `KEY_SCAN_TAIL_LINES` budgeted against the runner's
own trailing-line count; `TERMINAL_STATES` declares contract v2's refusals; every
write a resourceVersion-preconditioned merge PATCH with explicit `null` for a
field that no longer holds. STATUS-RECORDS: `Backup.status.records` is written from
the VERIFIED receipt and absent otherwise. Verified at `d69ca1a` on main: weirkeeper
+ `logweir-core` 1385/1385 (`diagnostics` 31 rows, `backup_controller` +11,
`restore_controller` +3), strict clippy and fmt, no-archive-write,
manifest_lint/doc_lint, `crds-check`, `render-install --check`; fifty-one planted
mutants killed at their named rows. Independent review `claude/d3w2.review.md`: ACCEPT-WITH-FIXES
(two high: every terminal builder dropped `RunnerReady`; the progress contract was
read from a bounded tail with nothing stored, so `runnerPhase` blanked mid-run —
the fix also found a second retraction, `read_progress` returning nothing whenever
its window held no phase line; six medium; three low) then its re-verification;
`failFastSeconds: 0` means never and is read before the floor. Deviations: the parsed progress-contract version is a
gate but unpublished (no `RunnerPhase.contract` status field — a crds-shapes
follow-up); events by UID, not name. Gaps: `status.completion` and
`status.teardown` are still declared and unwritten (an owner is needed — the same
defect class as STATUS-RECORDS); chart values for the diagnostics are W13's (the
fail-fast lever is an environment variable until then). Live: none — W14 owes a real image-pull failure, a deadline and a
refusal rendered through this path.

### PLAT-14.2 — Add protection freshness and useful notifications

**Problem/implementation:** An enabled schedule can have no recent recoverable
backup. Track last successful available point, its age, missed/failed runs and
stale protection against configured objectives. Show recovery completion topic
names, measured counts/verification limitations and application cutover guidance;
support deduplicated failure/staleness/recovery notifications.

**Acceptance:** Users can distinguish healthy scheduling from healthy protection;
notification failure does not rewrite the backup result. **Tests:** Stale point,
repeated failure deduplication, recovery notification, unavailable archive,
sampled-versus-complete labeling and notification transport failure.
**Dependencies:** PLAT-14.1, PLAT-15.1 availability integration.

**Live validation record (2026-09-18) — PLAT-14.2 (evidence only, stays In progress).**
D3 W14 on `e7d0e79` contributed: staleness detection reaching an alert; exactly one open
alert across three evaluation intervals with every delivery Job belonging to one transition
(two Jobs = the bounded retry, `attempts=2` of at most 3); a transport failure recorded on
`ProtectionPolicy.status` and nowhere else; and "notification failure does not rewrite the
backup result" — all 33 Backups kept their `resourceVersion`. Unprovable on this build: D3
L5's "an in-cluster echo sink records exactly 1 POST" — `logweir notify deliver` refuses a
non-`https://` sink before dialling and `NOTIFY_ALLOW_INSECURE_SINKS` is set by nothing in
the spec or the delivery Job (NOTIFY-INSECURE-SINK-UNEXPOSED; a decision for W13). Still
missing: recovery notification (needs a fresh point through the catalog view), unavailable
archive, sampled-versus-complete labelling. Evidence `claude/artifacts/d3-live/20260918t0316z/`.

### PLAT-14.3 — Schedule basic recovery rehearsals

**Problem/implementation:** Successful backups alone do not prove the supported
restore journey still works. Add opt-in recurring drills selecting a qualifying
recovery point and an explicitly approved isolated target, reusing the normal
restore/preflight/evidence path. Bound concurrency, runtime, resource use and
owned-topic cleanup; surface last successful rehearsal and failures. Advanced
exhaustive verification and application assertions remain product expansion.

**Acceptance:** A configured drill restores and verifies through the supported
path on schedule; unrelated target topics are never cleaned up. **Tests:**
Missing recovery point, unavailable target, approval policy, overlapping drill,
failed verification, cleanup failure and successful evidence retention.
**Dependencies:** PLAT-04.1, PLAT-11.2, PLAT-14.1, PLAT-19.2.

**Migration/safety and done evidence:** Do not label a signature as exhaustive
data verification. Record state mappings, notification deduplication behavior
and the incident-facing completion screen.

**Partial record (2026-09-16) — PLAT-14.2: the delivery path (D3 W4) landed;
the task stays In progress until the `ProtectionPolicy` kind and controller (D3
W0, W6) exist.** `crates/logweir/src/notify.rs` now holds the notification half
moved verbatim out of the drill (re-exported from its old path; the structural
guards in `tests/notify.rs` unchanged), the protection event document
(`application/vnd.logweir.protection-event+json;version=1.0.0`, strict parsing,
`verification_scope` limited to `sampled | degraded | none`, `docs/formats/protection-event.md`),
the pure dedup key builders W6 will call, and the subcommand `logweir notify
deliver --event <path>`: one PagerDuty event (`trigger`/`resolve` under the alert
key), one webhook and one Slack POST as configured by `PAGERDUTY_ROUTING_KEY`,
`NOTIFY_WEBHOOK_URL`, `NOTIFY_SLACK_WEBHOOK_URL` (https required unless
`NOTIFY_ALLOW_INSECURE_SINKS`), `notify-result=<sink>:<ok|failed>` as the final
stdout lines, exit 0 only when every configured sink accepted, exit 1 when one
refused or none is configured (`notify-result=none:unconfigured`), exit 3 when the
document is unreadable and nothing was posted (`docs/stability.md`). A summary
that claims exhaustive verification is sanitised to `[claim removed]` and still
delivered; operator identifiers are never scanned. No routing key, URL or Secret
value reaches stdout, stderr or tracing (a captured-tracing fold, a real-key
process row and a structural scan of every `tracing!` call guard it). Verified at
`d994e4c`: logweir 746/746 (`notify_deliver` 41), strict clippy and fmt, `just lint`.
Independent review `claude/d3-notify.review.md`: ACCEPT-WITH-FIXES (two high: the
claim gate scanned policy names; a routing key could reach stderr) then ACCEPT.
Note for W6: a `RecoveryCompleted` event with PagerDuty-only routes exits 1 by
design (it is webhook/Slack only) and must be treated as a no-op, not retried.
A pre-existing defect is recorded, not fixed: the drill path posts raw JSON to
Slack, which Slack rejects. Migration: none (an unreleased subcommand; no
controller uses it yet).

**Partial record (2026-09-17) — PLAT-14.2 controller half: the `ProtectionPolicy`
reconciler (D3 W6) landed as the twelfth controller; the task stays In progress
until W7 supplies `RehearsalFailure`, W11/W12 surface the status, W13 settles the
notifier's ServiceAccount and NetworkPolicy, and W14 proves delivery live.** Landed
in main as `a018b5c` (`protection.rs` — pure freshness, ledger and event — and the
controller), `8281270` (two `protectionpolicies` rules; `get` folded into the
`recoverycatalogs` rule), `ac16460`/`cf163d5` (tests), `6ebd085`/`3855c96` (docs
§7e) and `0306e5b` (review fixes). Contract for W11/W12 (`docs/kubernetes.md` §7e):
freshness is a pure function over the policy, its schedules' runs (membership by
`identity::is_run_of_schedule` over a bounded list — a manual run of a schedule is
history) and the catalog view's entries selected on BOTH axes (`selectable`, never
availability alone; a blank axis reads `CatalogUnreadable`, never "not
available"), yielding `Healthy | AtRisk | Stale | Unprotected | Unknown` with §3.3's
reason vocabulary — `unknown` is neither healthy nor failed; `ageSeconds`,
`evaluatedAt` and `sinceLastFire` settle together so a steady policy issues no
patch (E11(d)), conditions carry no humanized age, and a missed slot is judged
against `lastFireTime`, never pinned. Alerts: §3.3's kinds deduplicated by D3 W4's
helpers; `Unprotected` (no recoverable point at all) opens `Staleness`; an open
alert is RESOLVED only when health returns to `Healthy`/`AtRisk` — protection
getting worse (`Stale → Unprotected/Unknown`) never sends a `resolve`, and under
`Unknown` nothing was measured so nothing moves; `ArchiveUnavailable` opens when the
chosen point's own catalog entry is degraded on either axis. Delivery is one Job per
transition running `logweir notify deliver` exactly as W4 specifies, under the
`logweir-runner` ServiceAccount (a notifier-scoped SA and NetworkPolicy are W13's
question), the sink credential only by `secretKeyRef` and never read by the
controller (a fake API server echoing the Secret proves absence everywhere), three
bounded attempts (`[60, 300]` s), the first route per sink kind, the event
ConfigMap owned by the FIRST delivery Job for that transition so Job TTL collects
it, the Job owned by the policy with `blockOwnerDeletion: false`, the TTL patched
only after the status recording the delivery landed (S7 ordering), the pod read
only through the Job's owner UID (S6). Status writes carry explicit `null` for the
seven clearable fields (`staleSince`, `lastAvailablePoint`, …) so a merge PATCH can
clear them, and every write is resourceVersion-preconditioned. Verified at `3855c96`
on main: weirkeeper 872/872 (`protection_controller` 45), `manifest_lint` 29,
`doc_lint` 12, `chart_lint` 28, `crds-check`, `chart-check`, `render-install
--check`, `check-no-archive-write`, strict clippy and fmt; twenty-three planted
mutants killed (one first survived a vacuous fixture and was re-planted). Independent
review `claude/d3w6.review.md`: ACCEPT-WITH-FIXES (three high: worsening protection
resolved the open alert, `Unprotected` opened nothing, the status PATCH could never
clear a field; four medium; five low) then ACCEPT. Live: none here — W14 owes §15's
14.2 scenarios (a stale schedule alerting exactly once, recovery resolving it,
delivery failure retried and recorded). Gaps: `RehearsalFailure` awaits W7's inputs;
the catalog entry reader has no shared fixture binding it to `catalog_view::ViewEntry`
(W8/W13 add the line). Migration: two additive RBAC rules on a kind nothing had
created yet.

**Partial record (2026-09-17) — PLAT-14.3 controller half: the `RehearsalSchedule`
reconciler (D3 W7) landed as the thirteenth controller; the task stays In progress
until PLAT-14.3b makes a rehearsal executable and W14 proves L6 live.** Landed in
main as `b31ba35` (`rehearsal.rs` — template digest, the §4.2 filter chain, the plan
render — and the controller, plus the standing arm of `restore.rs`'s approval-bundle
function and the `RehearsalSchedule` referent arm in `approval.rs`), `43f5869`,
`5178d0e`/`838f05a` (tests), `974b141`/`3b0e964` (docs §7g) and `086f588`/`40b6bca` (review
fixes). Contract: the spec is sealed except `suspend`; slots and attempts have
deterministic names discovered by GET, one active rehearsal per schedule through the
reservation pattern (merge PATCH, never `replace_status`); a point is selected only
among the catalog view's `selectable` entries of the schedule's own source and
destination — no point is a recorded skip, never a fabricated run. Authorisation is
one standing `Approval` with `spec.subjectRef.kind: RehearsalSchedule`, checked
twice: the controller checks the rendered plan against the signed scope with
`execution_contract::plan_within_scope(plan_scope_facts(..), scope)` BEFORE creating
anything, then materialises the bundle — the Approval's signed envelope and sidecar
copied verbatim (never re-signed) at `standing-authorization.json`/`.sig`, the
trusted public keys from the namespace's resolved trust, each digest-pinned over its
own bytes, the approval UID threaded and a blank refused at three points — so the
runner checks it again; an expired, revoked-key, wrong-usage, wrong-subject or
out-of-scope authorisation is a recorded refusal with no Restore created. The
rehearsal cannot yet EXECUTE: the runner's standing path sits beside the per-run
approval (`load_startup_inputs` verifies the approval payload type unconditionally)
and five `restore.rs` functions (`admit`, `get_approval`, `triggered_by`,
`runner_argv`, `runner_job_spec`) do not read `spec.authorization`, so a
rehearsal Restore holds — recorded on the schedule as
`RehearsalHealthy=False/StandingAuthorizationNotAdmitted` naming PLAT-14.3b, never
silently. Verified at `3b0e964` on main: weirkeeper + `logweir-core` 1217/1217
(`rehearsal_controller` 32, `approval_controller` 41 and `restore_controller` 58
unchanged), strict clippy and fmt, one-signer, no-archive-write, manifest_lint/
doc_lint/chart_lint, `crds-check`, `chart-check`, `render-install --check`;
thirty planted mutants killed with module-qualified names (the first pass's bare
`--exact` names ran zero tests and were re-run). Independent review
`claude/d3w7.review.md`: ACCEPT-WITH-FIXES (three high: a blank approval UID
aborting every Job, the standing envelope mounted under the approval's name, the
walk that dropped the newest rehearsal; three medium; three low) then ACCEPT for
the controller half. Deviations: `sizeBasis` is an annotation (sealed status);
catalog-only points are admitted only when the template requires no topic subset;
a per-slot scratch prefix is deferred because it would change the signed scope.
Follow-up PLAT-14.3b (queued): the runner's standing path REPLACES the per-run
approval and the five `restore.rs` functions read `spec.authorization` — about
150 lines, after D2 W10 and D3 W2 land in the same file. Live: none — W14 owes L6.
Migration: additive RBAC on a kind nothing had created yet.

## PLAT-15 — Recover from archives without the original Kubernetes objects

**Priority P1 · Proposed.** Scope: a durable recovery catalog and import path;
no new database unless measured catalog requirements justify it.

### PLAT-15.1 — Define and index durable recovery-point metadata

**Problem/implementation:** Backup CR lifetime is currently entangled with
discoverability. Store/index stable point identity, archive location, covered
windows, resolved topics, source identity, execution/evidence references and
verification material in durable storage. Distinguish missing, unreadable and
unverified points; paginate catalog reads and bound rescans.

**Acceptance:** History can be reconstructed from storage after CR loss without
trusting unsigned metadata as verified evidence. **Tests:** Missing/corrupt
manifest, duplicate identity, large catalog, partial access, stale index and
schema-version compatibility. **Dependencies:** PLAT-08.1, PLAT-02.1 public
verification material.

**Live validation record (2026-09-18) — PLAT-15.1: D3 W14 proved the acceptance sentence
live on `e7d0e79` — history reconstructed from storage after CR loss without trusting
unsigned metadata as verified evidence — and found the listed tests partly unreachable and
one blocker; stays In progress.** Harness `e2e/k8s/d3/` (landed as `1ab43cd`; evidence
`claude/artifacts/d3-live/20260918t0316z/`, report `claude/d3w14.result.md`, review
`claude/d3w14.review.md` ACCEPT-WITH-FIXES on the harness, which were applied). Proven:
three recovery points rebuilt into the `RecoveryCatalog` view with zero `Backup` CRs in the
namespace (the reviewer reproduced it with two), the two axes kept separate — the
`catalogSync` Job reports only whether a signature verified and under which key id, the
controller decides trust (`status.signers[].trusted`, `TrustAvailable=True`, "1 signing
key(s) from `TrustRoster/default`"); `Missing` for a removed manifest; schema-version
mismatch never fatal and never offered. Unmet from the test list: large catalog/paging
(`viewLimit` floor 100 and `maxObjectsPerRun` floor 1000 — truncation needs more than 100
real signed points; no bound was faked by editing the CRD), partial access (an `Unreadable`
row needs a key-scoped credential on a second destination), corrupt manifest ("could not
tell" as distinct from `Missing`), and the `UnsupportedFormat` classification (a record both
validly signed and of a future major needs the installation's private key). Blocker:
CATALOG-RESYNC-NOT-HARVESTED — a second `spec.syncRequest` runs a Job to `Complete` that
the controller never harvests, and `Synced` flips back to `Unknown/PodNotStarted` after a
publish, so a view can only be refreshed by re-creating the catalog (defect table).

### PLAT-15.2 — Add Connect existing archive and disaster restore

**Problem/implementation:** The wizard requires Backup objects that may have
been lost in the incident. Let an authorized user connect a destination,
discover existing points, establish verification trust explicitly and restore
without a live source or its old CRs. Reuse normal point selection and preflight.

**Acceptance:** A fresh supported installation restores a selected known archive
while the source and original Backup CRs are unavailable. **Tests:** Source
offline, CR loss, old signer, untrusted signer, storage denial, incomplete point
and repeated import. **Dependencies:** PLAT-15.1, PLAT-11.1, PLAT-03.2,
PLAT-19.1.

**Migration/safety and done evidence:** Never automatically trust a public key
solely because it arrived beside an archive. Supply catalog compatibility rules
and a complete source-offline/CR-loss recovery record.

**Partial record (2026-09-16) — PLAT-15.1: the runner half (D3 W3) landed; the
task stays In progress until the `RecoveryCatalog` kind, its controller and the
bounded Kubernetes view (D3 W0/W8) exist.** Landed in main as `49c2a63`
(`Store::list_page`: one bounded page of keys, no write, no delete), `75253ac`
(the backup runner writes a durable, signed recovery-point record beside every
receipt and `logweir catalog sync|list`), `389ab9c` (mutant guards and the
Python verifier's `--payload-type catalog-point`), `be33766`
(`docs/formats/catalog-point.md`), `1794275`, `398b403`, `c5d57ce` (review
fixes). Contract: `pointId = "lwp1-" + hex(sha256(receipt bytes))[0..32]` with
the full `receipt.sha256` as the binding, so two receipts under one
`backup_id` are two points (the RECEIPT-DUP defect's answer) and a copied
archive yields the same point; create-only, never-rewritten keys
`logweir/catalog/v1/points/<pointId>/record.json|.sig` and the day-sharded
index `logweir/catalog/v1/log/<yyyy>/<mm>/<dd>/<ms:013>-<pointId>.json` under
the existing `LOGWEIR_ROOT` write assertion; the record
(`application/vnd.logweir.catalog-point+json;version=1.0.0`,
`schemas/logweir-catalog-point-1.0.0.json` with an `enum` for
`signing.algorithm`) signed with the one run signer as a DSSE sidecar; reading
rules — a higher major is `UnsupportedFormat` per entry, unknown fields are
ignored within major 1, absent optional fields mean unknown never zero, the
receipt-derived facts are recomputed from the verified receipt and a
disagreeing copy is `RecordMismatch`, a malformed or inconsistent index entry
is skipped and counted, never fatal; the runner prints a conditional
`catalog-key=` line before the receipt lines and a failed catalog write is a
warning that can change neither the exit code nor the receipt; `logweir
catalog sync` backfills records only for receipts that verify against a
supplied public key and reports a store refusal as exit 1 with the truth, never
as the signing exit 4; `logweir catalog list` walks day shards newest-first
within `--max`/`--days`. Verified at `c5d57ce`: catalog 42, `list_page` 8,
private-MinIO rows 5/5 (twice; container and network removed), `just verify-py`
106, parity corpus extended, `check-no-archive-write`, `pure-core`, strict
clippy and fmt; eight planted mutants killed plus the reviewer's three.
Independent review `claude/d3-catalog-writer.review.md`: ACCEPT-WITH-FIXES
(three medium: a store failure reported as a signing failure, the schema's
old `p256` spelling, a fatal index read) then ACCEPT. Decision amendments
recorded in D3 at integration: §17 S1 now lists `logweir catalog sync|list` as
the third deliberate exception (an operator command over the same create-only
key family, not a check runner; the `catalogSync` plan kind remains what the
controller runs) and §5.2's example names `ecdsa-p256-sha256`. Migration: an
additive key family under `logweir/`; old archives without records are
backfilled by `sync`; no reader of existing evidence changes. Limitations: the
`execution` block is absent because a Backup Job's argv passes the runner no
execution identity today; availability, verification state, tombstones,
retention and disaster import are W8/W9/PLAT-15.2.

**Partial record (2026-09-17) — PLAT-15.1 controller half: the `RecoveryCatalog`
reconciler and the bounded Kubernetes view (D3 W8) landed; the task stays In
progress until D2 lands the `catalogSync` plan kind in the runner, W11/W12
expose the view, and W14 proves a sync live.** Landed in main as `3bcec20` (the
controller, and a view that expires instead of lying), `857a2f5`/`48d96f4`
(tests), `d35e6e6`/`4a296b7` (docs) and `16191c8` (review fixes) as the
tenth controller. Contract (`docs/kubernetes.md` §7d; `catalog_view.rs`): a
sync is one check Job per slot running D2's check runner with plan kind
`catalogSync` (D-SEAMS S1 — the kind is D2's to add in `logweir-core`, and a
local test flips the day it lands); the Job owns its plan ConfigMap and every
page and index ConfigMap it materialises (`blockOwnerDeletion: false`), so Job
TTL — `max(3 × intervalSeconds, 3600)`, at most twelve live generations at D3's
300 s floor, which the CRD now enforces with CEL J3 (`intervalSeconds == 0 ||
>= 300`, 0 meaning manual only) — is the only collector and the controller holds
no `delete` verb (RBAC `recoverycatalogs: [list, watch]` + `/status: [patch]`,
no `get`); the archive is read through the destination's `archiveRead` grant by
`secretKeyRef` and nothing else; the pod is read only through the Job's owner
UID (S6); every status write is a resourceVersion-preconditioned merge PATCH
(S7). The view keeps availability and verification as two axes and materialises
`selectable = Available ∧ (Verified | VerifiedHistorical)` once; `Unreadable ≠
Missing`; a `RecordMismatch` is a `Conflict`; a duplicate identity under one
`backup_id` is two points; an unsupported record major is per entry, never
fatal; one point seen in several locations is one entry whose `locations[]`
carry their own availability — the entry's availability is the BEST of them
with degraded copies named in `remedy`, and its verification the WORST of them
with the signer key id following the worse verdict (D3 §5.4 amended at this
integration); `status.signers[].trusted` means accepted, so a `Revoked` key is
`trusted: false` and `Revoked`/`VerifiedHistorical` points are named in the
`Synced` message beside `Partial`. A failed sync keeps the previous view; a view
whose Job has aged out is cleared (`pages`, `indexConfigMap`, `truncated` to
`null`) on every status-writing path and reported `ViewExpired` without
claiming the archive changed; `Ready` stays `True` across a sync and
`SyncInProgress` lives on `Synced`; the fence pointer excludes and never proves.
Runner-side result grammar the `catalogSync` kind must meet: a `catalog-format=1`
line first (a higher major refused by name), a byte budget of 5 MB and at most
`viewLimit` entries, `catalog-signers` capped at 64, `locations[]` capped at 16,
a repeated `catalog-counts=`/`catalog-cursor=` line an error, the summary lines
covered by the frame-stream digest, an oversized entry refused into
`dropped_oversized` and named. Verified at `4a296b7` on main: weirkeeper 687/687
(`catalog_controller` 70, `crd_shape` 41 with J3 in the shipped CRD), `linkage`
17, `manifest_lint` 29, `chart_lint` 28, `doc_lint` 12, `crds-check`,
`chart-check`, `render-install --check`, `check-no-archive-write` (36 055
weirkeeper lines naming no write or delete), strict clippy and fmt; thirty-nine
planted mutants killed (sixteen original, twenty-three from the fix round,
including the review's surviving signature-merge mutant). Independent review
`claude/d3w8.review.md`: ACCEPT-WITH-FIXES (three high — a plan ConfigMap per
slot leaked forever, aged-out pages left listed on refusal paths, unbounded
coexisting generations; four medium; six low) then ACCEPT. Live: none here —
W14 owes a real sync against MinIO (a catalog larger than the view limit paging
and saying so; a tampered relay writing no page; an impostor pod never read;
expiry after the Job is collected). Gaps: `legacyArchive` sources are
`Ready=False/LegacyArchiveUnsupported`; view ConfigMap names derive from
`sha256(catalog UID)`; `counts` do not sum to `total` by design (documented).
Migration: two additive RBAC grants and one additive CEL rule on a kind nothing
had created yet; no existing object is affected.

**Addendum (2026-09-17) — the `catalogSync` plan kind landed in the one check
runner (D-SEAMS S1), closing the gap the W8 record named.** Landed in main as
`9293402` (`CheckPlanKind::CatalogSync` and its request shape, additive in
`logweir_core::check_contract`), `0c05ed2` (the runner kind), `6addd01` (the
controller's local constants replaced by the core variant; its "not yet in the
vocabulary" alarm retired), `80b708c`/`aa864dd` (tests), `cc69b7a` (docs) and `528ffb3`
(review fixes). The kind reads the destination through the read-only
`archiveRead` grant with explicit options (S5), walks the durable catalog records
from the cursor within the byte and entry budgets, reports each record's signature
verdict and signer key id without judging trust, availability per location, and
emits the §7d grammar — `catalog-format=1` first, ≤ 64 signers, ≤ 16 locations per
entry, no repeated summary line, the fence cursor last, `complete` meaning every
point was examined (the review found a full rescan stopping at the view limit while
claiming completion, and an index walk advancing one day per sync so an old archive
could never complete); an unreadable record is `Unreadable`, never `Missing`,
and budget exhaustion is its own outcome with the cursor, never a transport failure.
The wire is guarded both ways with no dependency edge: the runner reads the
controller's grammar constants and the controller parses the runner's pinned body and
round-trips its plan document through `parse_and_verify`. The `catalogSync`
ceiling is 1800 s (the framework's 600 s refused every 900 s sync plan). Verified at
`5205090` on main: the three-crate suite 2238/2238, strict clippy and fmt, pure-core,
no-archive-write, no-oso, one-signer, verifier-parity, one compose-MinIO end-to-end
row; fifty-six planted mutants killed. Independent review `claude/d3w8b.review.md`:
ACCEPT-WITH-FIXES (three high on completeness reporting; two medium; five low) then
the verdict in its re-verification. Live: none — W14 owes a real sync.

## PLAT-16 — Make retention promises and destination scope accurate

**Priority P2 · Proposed.** Current keep-count/day fields report recommendations;
they do not delete data. Preserve that fact through the transition.

### PLAT-16.1 — Separate recommendations from enforced lifecycle

**Problem/implementation:** Retention settings can sound like automatic cleanup,
and one global store cannot accurately represent every destination. Evaluate
the selected destination and label recommendation-only versus external bucket
lifecycle enforcement. Show last evaluation, skipped/unreadable points and
policy conflicts without blocking unrelated backup execution.

**Acceptance:** Users can tell what will actually delete data and where; reports
never describe another destination's catalog. **Tests:** Two destinations,
missing lifecycle permissions, unreadable manifest, overlapping keep rules,
evaluation failure and continued scheduled backup. **Dependencies:** PLAT-08.1,
PLAT-15.1.

**Partial record (2026-09-17) — PLAT-16.1 and PLAT-16.2: retention recommendation
scoped to the right destination, and the one supported enforcer (D3 W9) landed as
the fourteenth controller; both stay In progress until the restore-side hold lands,
W11/W12 surface the report, and W14 proves an enforcement live.** Landed in main as
`582ba14` (`weirkeeper::retention_plan` — pure `evaluate` and the plan document
— `crates/logweir-reaper` — the ONE crate that names an object-store delete — and
`crates/logweir-retention`, the separate binary), `a6bca2a`, `5937a3c`/`db6ff6a`
(tests), `b118945`/`f8f7ced` (docs §7f, the binary's exit contract in
`docs/stability.md`) and `a880da5`/`bbdd0be` (fixes). Contract: PLAT-16.1's report
names what WOULD be removed per destination, evaluated over the catalog view's
selectable points and each frozen run's own destination — never a global store,
never the schedule's current URL; membership is a private newtype the compiler
enforces, so a run frozen against destination A is never counted against B, and
the legacy retention report is replaced by a note with empty set lists
(RET-WRONGBUCKET closes both ways). PLAT-16.2's enforcement is opt-in per policy: the
controller renders an immutable, digest-pinned plan whose `planSha256` carries no
clock, generation or counter (the review found the first digest embedded the
evaluation instant, so an approved plan could never match), refuses to start while
any non-terminal Restore or rehearsal in the namespace touches the destination
(identity by `sourceDestinationRef` name and namespace, URL equality only when
absent — a substring match had made destination-backed Restores invisible), writes a
lease to its own status with the threaded `resourceVersion` so a 409 aborts the
pass before any Job (a swallowed 409 had let the Job delete leaseless), and only then
creates a Job running `logweir-retention` with the delete-capable credential by
`secretKeyRef` alone (S5), dry-run by default, the enforcement record written
BEFORE any delete — create-only and UNSIGNED in this build, said so on every surface
(signing would widen the one-signer allowlist) — per-run and per-window caps, plan
lines naming a set prefix whose boundary cannot match another backup's objects, an
expired plan never started, the harvest of the Job's key lines populating
`lastEnforcement.{deleted, failed, recordKey, recordSha256}` so the `LegalHold`
exclusion has an input, and the preview reporting the real per-candidate object
count. The everyday `logweir` binary and `weirkeeper` link no delete path:
`check-no-archive-write.sh` gained a `cargo metadata` walk over every dependency
kind (the reviewer tried six evasions). Verified at `a3af420` on main: workspace
2912/2912 (the binary itself now has a lib target and rows), strict clippy and fmt,
every script gate, `verify-py` 106, `crds-check`, `chart-check`,
`render-install --check`, `links`; fifty-two planted mutants killed from a
clean tree including the review's flipped `--dry-run` and the gate mutant.
Independent security review `claude/d3w9.review.md`: REJECT (three critical, six
high, nine medium, seven low) then, after a full round with a killing test per
finding, ACCEPT-WITH-FIXES on two documentation residuals closed before the merge —
the digest no longer takes a clock at all, so the defect is unwritable, and
`check-withdrawn-claim.sh` now matches a phrase across a line wrap (its first
wrap-insensitive run found three more instances, all corrected). Deviations recorded: the record
is unsigned; the restore-side hold on a lease is a hand-off to the owner of
`restore.rs` (enforcement fails closed meanwhile); plan lines name a set prefix
plus `enumerate_set` because the view carries no segment keys
(`sharedSegments: NotEnforced` with its reason until it does);
`acknowledgedUnenforceable` and `retentionReport.enforcement`/`supersededBy`
need W0 fields. Live: none — W14 owes a dry-run and a real enforcement against
MinIO with the record verified. Migration: additive RBAC rules and a new binary
image; nothing enforces until a policy opts in.

**Live validation record (2026-09-18) — PLAT-16.1: blocked live on `e7d0e79` by one
digest-spelling defect; two of six tests PASS; stays In progress.** No retention report
renders for any destination: `catalog_view.rs:966` publishes `page_digest` as bare hex,
`status.pages[].sha256` carries the `sha256:` prefix, and `retention_policy.rs:1345`
compares them with `!=` and answers `ViewUnreadable` — the controller's own condition
message prints both values equal apart from the prefix, and the controller test fixture
(`retention_policy_controller.rs:1226`) publishes the bare spelling, which is why every unit
row passed (RET-DIGEST-PREFIX, confirmed critical and reproduced by the reviewer). PASS
without the view: declared external lifecycle (`ExternalLifecycleConflict=True /
DeclaredExpiryConflicts`, the bucket wins, `minUsablePoints` / `activeRestoreProtection`
`NotEnforced`) and continued scheduled backup (25/25 `Succeeded` through the window, a
refused retention verdict blocking no backup). Waiting on the fix: two destinations,
unreadable manifest, overlapping keep rules, evaluation failure as a distinguishable state,
and the acceptance sentence itself. Evidence `claude/artifacts/d3-live/20260918t0316z/`.

**Completion record — Done (2026-09-18), PLAT-16.1.** Source landed on main as D3 W9
(`retention_scope` — a report never describes another destination's catalog; the
recommendation-only versus enforced lifecycle labelling; `ExternalLifecycleConflict`),
W8/W8b (the catalog view it reads) and the wave-9 fix `f5a6876` (RET-DIGEST-PREFIX: the
evaluation had never rendered because of one digest spelling). Proved live on
docker-desktop: D3 W14 at `e7d0e79` (declared external lifecycle — `ExternalLifecycleConflict=True /
DeclaredExpiryConflicts`, the bucket wins; continued scheduled backup 25/25 through the
window with a refused retention verdict blocking no backup), lab-refresh-3 at `c6422a7`
(two destinations with real point ids per destination; evaluation failure as a
distinguishable state — `sharedSegments: NotEnforced`, `legalHold != LogweirEnforced`)
lab-refresh-4 at `7b4fae9` (`claude/lab-refresh-4.result.md` §9.3: unreadable manifest as a
skipped point never a candidate, overlapping keep rules with the arithmetic spelled out, two
policies on one destination) and lab-refresh-5 at `d387f87` (`claude/lab-refresh-5.result.md`
§8: the two rows the reviews found missing — missing lifecycle permissions, where a
credential without the bucket-lifecycle read makes the report say `unknown`/unenforced with
no evaluation block while unrelated backups continue, and evaluation failure as its own
`Ready=False` reason with no candidate list or count — beside all four earlier rows, six of
six on the current build). Independent reviews:
`claude/d3w14.review.md`, `claude/lab-refresh-3.review.md` (the two-destination row
re-run naming 4 and 6 real point ids), `claude/harness-refresh.review.md`,
`claude/lab-refresh-4.review.md`, `claude/lab-refresh-5.review.md`. Acceptance ("users can tell what will actually delete
data and where; reports never describe another destination's catalog") and all six
listed tests are satisfied. Stated deviations (from the lab-refresh-5 review): the
"unreadable manifest" test is proven as an unreadable POINT that is never a candidate (the
manifest itself is what the point's reader could not open); the keep-rule row's second
recorded FAIL on lab-refresh-5 was the harness re-seeding on top of existing points, fixed
on `claude/harness-rows-3` — the first, clean measurement PASSED. Migration: none; a policy
evaluated by an older controller re-evaluates on its next interval.

### PLAT-16.2 — Decide and implement the supported enforcement boundary

**Problem/implementation:** A mature retention control needs an accountable
enforcer. Choose a documented lifecycle integration or an isolated optional
Logweir retention worker. Implement preview, scoped authorization, active-restore
protection and observable results for the chosen mode; do not grant deletion
authority to the existing read-only controller by accident. Respect legal holds,
storage locks, shared-segment references and a configurable minimum number of
usable recovery points. For external lifecycle enforcement, identify which of
these guarantees the provider policy can actually enforce; reject unsupported
combinations rather than claiming a Logweir pin can override bucket deletion.

**Acceptance:** The supported mode demonstrably enforces its advertised policy;
protected/unknown points are retained and every deletion is attributable.
**Tests:** Dry preview, denied deletion, active restore, legal hold/lock,
last usable point, shared segment, partial failure, policy change, wrong-prefix
rejection and bounded retry. **Dependencies:** PLAT-16.1,
PLAT-15.1, PLAT-17.2 authorization.

**Migration/safety and done evidence:** Existing reporting-only users remain
reporting-only until an explicit enforcement choice. Record irreversible-action
boundaries, least-privilege permissions and a safe isolated enforcement test.

**Live validation record (2026-09-18) — PLAT-16.2: the enforcer's worker half is proven
live on `e7d0e79` from an image built for the wave; the controller half is NOT-RUN and the
binary ships in no image; stays In progress.** Proven with the `logweir-retention` binary
added to an image built from this commit (`e2e/k8s/d3/Dockerfile.retention`; the shared
controller's `LOGWEIR_RUNNER_IMAGE` never changed), run as the operator Job D3 §7f
prescribes: a dry preview with real per-candidate object counts deleting nothing; one
enforced pass removing exactly the nine objects the plan's set bounds named, every one under
`archive/`, every `logweir/` object still present (the reviewer re-verified: 9 gone, 0
`logweir/` removed, 7 tombstones plus the record added); the record verified by digest —
unsigned, as D3 §6.5/L9 and `docs/stability.md` prescribe (one contradicting line at
D3 §1084 is queued); refusals for a wrong prefix (exit 3), a stale approved digest (exit
3), no `evidenceWrite` credential (exit 3) and a denied deletion (exit 1, `Kept` /
`AccessDenied`, nothing removed). Not proven: every controller-side guard (active restore,
legal hold, last usable point, shared segments — no evaluation exists while
RET-DIGEST-PREFIX stands; the enforced plan was hand-built in the product's own
`deny_unknown_fields` format and every affected row says so), partial failure, bounded
retry, and the record's create-only put (MinIO via `mc` cannot issue `If-None-Match`).
Packaging: RET-NOIMAGE — `Dockerfile` builds only `-p logweir`; the enforcement Job's
command `logweir-retention` exits 127 from the image the shared controller names (defect
table). Evidence `claude/artifacts/d3-live/20260918t0316z/`.

## PLAT-17 — Establish a shared-console product API and identity boundary

**Priority P1 · In progress (contract design).** Static UI plus administrator port-forward is a
useful local mode. A shared ServiceAccount proxy is not individual application
authorization. Scope: a small product API, not an unrestricted Kubernetes proxy.

### PLAT-17.1 — Introduce bounded product endpoints

**Problem/implementation:** Direct CR manipulation exposes infrastructure details
and leaves client logic responsible for orchestration. Define typed endpoints
for saved references, discovery, preflight, durable submissions and status;
reuse Rust domain/plan libraries and Kubernetes execution state. Serve static
assets through the same supported boundary where practical; include request
idempotency, pagination and bounded cancellation for transient checks.

**Acceptance:** The API submits durable CRs while the controller remains the
execution authority; arbitrary Kubernetes paths are unavailable. **Tests:**
Contract validation, malformed input, duplicate request, timeout/cancellation,
pagination and restart with existing operations. **Dependencies:** None for
the boundary; integrate domain contracts from PLAT-06/07/08/09/14 incrementally.

**Partial record (2026-09-16) — D0 stages 1 and 3 landed; PLAT-17.1 stays In
progress.** `crates/logweir-api` (`4b571d1` service, `077ac48` bounded test
children, `1a9b631` `docs/api.md`, `586030c`/`c7f13af`/`972415f`/`de0207c`
review fixes) serves the sixteen static UI files and `/api/v1` on one origin in
`localAdmin` mode only (loopback-only listener refused before binding
otherwise), reads and creates the existing kinds through one sealed Kubernetes
adapter whose verb set is pinned by an allowlist test (`create`, `get`, `list`,
`patch`; the one update is `BackupSchedule.spec.suspend` under a resourceVersion
precondition, D-SEAMS S7), refuses `Impersonate-*`, exposes no generic path,
Secret, Pod, log, exec, Job or delete, renders every rejection as problem+json
with field paths, names creates deterministically with request-hash replay
comparison (`idempotency_conflict` on different content), authenticates list
cursors with an HMAC, normalises Backup/Restore status, checks the third
checked-in schema `schemas/logweir-api-v1.openapi.json` for drift, and links
only the verifying half of the signing API (`tests/linkage.rs`,
`scripts/check-one-signer.sh`). The one dependency decision is axum 0.8 (three
new packages; `THIRD_PARTY_NOTICES.md` regenerated, `cargo deny` clean).
Verified at `de0207c`: 106 crate tests, eight planted mutants killed within
bounded time (including the reviewer's PUT-on-a-renamed-handle and turbofish
dodges), strict clippy and fmt, `schema-check`, `one-signer`, `pure-core`,
`no-oso`. Live smoke on docker-desktop (`artifacts/plat17-api/live/`, namespace
`lw-plat17-20260916131321`, deleted after an owner-label check, no cluster
lock): 67/67 steps — the shared lab controller reconciled the API-created
`KafkaCluster` and the API projected its real status while creating no Job,
Pod or Secret; three POSTs with one key left one object and a restarted process
replayed the key to the same UID; 56-object pagination with tampered and
route-replayed cursors rejected; every Kubernetes path 404; traversal refused;
server logs carry no query strings or raw idempotency keys. Independent review
`claude/plat17-api.review.md`: ACCEPT-WITH-FIXES (one medium: the S7 lint could
not fail) then ACCEPT after the fixes. Migration/rollback: none — nothing
packages or deploys the crate yet (`publish = false`; `docs/api.md` says so).
Not done: console image and chart with the API's own RBAC (so the closed
adapter is a source-level bound and `inCluster` is untested), transient-check
timeout/cancellation (needs PLAT-03/09.1 kinds), `POST …/backups` (PLAT-06.2),
destinations/credential-write/approval-submit routes, SSE, a browser journey,
the readiness-cache mutex question (Q1, deferred to stage 7), and all of
PLAT-17.2.

**Live validation record (2026-09-18) — PLAT-17.1: both acceptance clauses and five of six
listed tests PASS live on `e7d0e79` through the product API (`api_live.py` 41/44); stays In
progress on the timeout half.** PASS: contract validation (every write response and list
envelope validated field for field against `schemas/logweir-api-v1.openapi.json`, zero
drift), malformed input (undeclared field, wrong type, missing required → 422
`problem+json` with a field path, nothing created; `PUT`/`DELETE` refused), duplicate
request (one key, three POSTs, one UID; different content → `idempotency_conflict`; the
three key-refusing routes answer 400), pagination (56 objects at `limit=10` →
`[10,10,10,10,10,6]`, set equality with `kubectl`, tampered and route-replayed cursors
refused), restart with existing operations (a real kill, a 16.3 s measured outage, the
controller moving an operation while the API was down, the same UIDs and a replayed
pre-restart key afterwards); "the controller remains the execution authority" (the API
created no Job, Pod or Secret) and "arbitrary Kubernetes paths unavailable" (Secrets, Pods,
logs, exec, Jobs, `/apis/...`, two raw-socket traversals → 404; another namespace 403;
`Impersonate-*` 400). PARTIAL: timeout/cancellation — cancellation proven for both
transient kinds (`:cancel` on a `Running` check → `Cancelled` ~1 s later, kubectl-confirmed);
the timeout path terminates but projects `ResultUnreadable` (D2-RESULTUNREADABLE). The
2026-09-16 record's other open items (console image and chart with the API's own RBAC,
`inCluster` mode, SSE) are untouched. Evidence `claude/artifacts/d2-live/20260918T030840Z/api/`.

**Live validation record (2026-09-18, lab-refresh-3) — PLAT-17.1: the timeout half now
PASSES (api T2 projects `BrokerUnreachable` / `MetadataTimeout` after the
D2-RESULTUNREADABLE fix), so every listed test and both acceptance clauses are proven live
through the product API; the task stays In progress on its packaging stage only.** The
console journeys of D2 W14 and D1 W7 went through this API (`POST …/backups` with an
idempotency intent, the preview and PUT routes, the discovery and preflight routes).
Residue before Done: D0 stage 7 — the console image and chart deployment with the API's
own RBAC (the lab still runs the API out of cluster in localAdmin mode; no console
ServiceAccount exists in the release). SSE belongs to PLAT-14.1 and PLAT-17.2 is its own
task.

### PLAT-17.2 — Enforce user identity, roles and audit attribution

**Problem/implementation:** Everyone reaching the shared proxy acts through one
account. Add verified SSO identity and namespace/resource-scoped viewer,
operator, approver and administrator authorization. Decide explicitly between API-managed
authorization and permitted Kubernetes impersonation; record initiating and
approving identities with operation/audit correlation. Require TLS on the
shared entry point, protect API and status streams, define trusted proxy-header
and session/token handling, and remove or isolate the old direct proxy route.
Add UI/API ingress NetworkPolicy and service-exposure configuration appropriate
to the supported ingress controller. Keep localhost administrator access an
explicit deployment mode, not an unauthenticated shared-console bypass.

**Acceptance:** A viewer cannot mutate and an operator cannot assume approver
rights; cross-namespace access and forged identity headers are rejected.
**Tests:** Role matrix, expired session, forged header, unauthorized namespace,
CSRF/session controls where applicable, unauthenticated API/stream requests,
direct legacy-proxy reachability, TLS ingress, allowed/denied network paths,
audit attribution and denied mutation.
**Dependencies:** PLAT-17.1. Agree the policy contract with PLAT-19.2; base
identity/role enforcement can ship before its approval-policy integration.

**Migration/safety and done evidence:** Keep local administrator access explicit;
do not imply that adding a login in front of a shared proxy creates per-user
authorization. Supply threat-boundary, RBAC and deployment migration decisions.

**Partial record (2026-09-16) — D0 stage 2 landed; PLAT-17.2 stays In progress
and shared mode is not declared secure.** Landed in main as `24752f4` (OIDC
identity, sessions, CSRF, roles and audit for `mode: shared`), `8bfe3d3`
(the OIDC, session, CSRF, role-matrix and audit suites over an in-process
provider signing real RS256/ES256), `8d35fd0` (the shared-mode contract in
`docs/api.md`, whose configuration example a test parses and starts),
`bd853bb`, `af5e083` and `90ecd0c` (review fixes). Contract (`docs/api.md`
§Shared mode): Authorization Code with PKCE S256, `state` and `nonce` in a
sealed ten-minute login cookie, exact issuer, exact audience with `azp`
checked whenever present, `alg` allowlist, JWKS by exact `kid` with bounded
refetch and a fail-closed outage window, `exp`/`iat` with bounded skew; the
browser never receives a provider token; a ≤ 15-minute
ChaCha20-Poly1305-sealed `__Host-logweir_session` cookie (`Secure`, `HttpOnly`,
`SameSite=Lax`, `Path=/`, no `Domain`, key version in the AAD, refused on an
unexpected key rotation, refused by name as `session_too_large` rather than
truncated) carrying only bindable group claims; a synchroniser CSRF token
derived by HMAC from the session id, required with `application/json` and the
exact `Origin` on every unsafe method, no CORS anywhere; `X-Remote-User`,
`X-Forwarded-*`, `X-Auth-Request-*` ignored and stripped from logs and
`Impersonate-*` refused; four roles over exact group or `issuer#sub` bindings,
re-derived per request from the carried claims, `Role::allows` an exhaustive
match with Administrator absent from approval submission and separation of
duties by `(issuer, sub)`; an ungranted namespace answered byte-identically
to a missing object before any Kubernetes call; one deny-by-default audit
record per request with the D0 fields and the real reason behind the
enumeration-resistant 404, never a cookie, token, code, CSRF token, client
secret, Secret value, plan bytes or URL userinfo (a captured-tracing guard
and a real-key process row enforce it); shared mode refuses at startup, by
field, a non-HTTPS `publicBaseUrl`, a missing or rotated key file, wildcard
bindings and every other malformed setting the doc example is mutated into.
Verified at `90ecd0c`: logweir-api 185/185 (twelve targets), 22 planted
mutants killed plus the reviewer's five, strict clippy and fmt, `just lint`,
`schema-check`, `one-signer`, `cargo deny`, notices unchanged (five dependency
edges, zero new packages). Live docker-desktop (`artifacts/plat17-2-authz/live/`,
namespaces `lw-p172-a/b-20260916152433` deleted after owner-label checks, no
lock, a local TLS terminator and a local ES256 mock provider): 71/71 — four
real code+PKCE sign-ins, the operator's API-created `KafkaCluster` and
`BackupSchedule` reconciled by the shared lab controller while the API created
no Secret, Job or Pod, viewer cannot mutate, approver cannot create, operator
and admin are not offered approval submission, the operator's reach into the
other namespace is byte-identical to a missing object, forged identity
headers change nothing, missing CSRF or `Origin` refused, foreign `Host` 421,
no `Access-Control-*` header anywhere, 43 audit records with no secret
material, session expiry after the configured lifetime. Mandatory independent
security review `claude/plat17-2-authz.review.md`: ACCEPT-WITH-FIXES (two
medium: transport refusals lacked audit codes; an unbounded session cookie)
then ACCEPT. Migration/rollback: none — nothing deploys shared mode yet; the
`localAdmin` mode and the legacy `kubectl proxy` UI are unchanged. Not done,
in D0's own terms: the console image, chart, ingress and per-namespace
RoleBindings (stage 7), so the API has no Kubernetes identity of its own and
the `auth can-i` negatives cannot run; the console keys still sit inside
`weirkeeper`'s Job-create authority (stage 5) — D0 says shared mode must not be
declared secure until that is scoped; no TLS-ingress or NetworkPolicy
evidence; no browser journey (stage 8) and the `ui/` client's console mode
uses a provisional CSRF header name; no governed-approval route (PLAT-19.2);
no event stream (PLAT-14.1); the legacy direct proxy is neither removed nor
isolated; revocation is bounded by the ≤ 15-minute expiry; the session key
bytes and CSRF subkey are not zeroised (only the client secret is).

## PLAT-18 — Strengthen UI structure without a speculative rewrite

**Priority P2 · In progress (18.1 Done).** Scope: contracts, state, reusable interactions and
accessibility. Keep static packaging; choose a framework only against concrete
maintenance and interaction requirements.

### PLAT-18.1 — Introduce typed clients and explicit workflow state

**Problem/implementation:** Hand-built requests, forms and wizard state drift
from backend behavior. Add a typed client contract and explicit wizard/mutation
state transitions, shared validation and reusable selectors/forms. Centralize
plan production/canonical bytes through a stable tested contract; do not move
execution authorization into the browser.

**Acceptance:** UI/backend changes produce contract failures instead of silent
field loss; plan review and submission use the same bytes. **Tests:** Contract
fixtures, optional fields, state transition errors, plan/hash round trip,
navigation disposal and server validation disagreement. **Dependencies:**
PLAT-17.1 contract; can begin current-API typing before API deployment.

**Completion record — Done (2026-09-16), PLAT-18.1, with D0 stage 6 (the static
UI client migration) done for its own scope.** Landed in main as `fa73824`
(typed client contract, one mode decided at boot, named transitions), `2ad4356`
(contract-against-schema tests in both modes), `f3c20a1` (docs), `715e183` and
`48d5ec0` (review fixes). Contract (`ui/README.md`): four new modules —
`ui/contract.js` (JSDoc types and strict decoders for every DTO in the legacy
direct-CR mode and the console `/api/v1` mode: a missing required field or an
undeclared enum member is a rendered `contractFailure` naming DTO and JSON path,
an unknown field is tolerated and recorded, legacy decoders return the same
object so nothing is dropped in transit; a node test compares enum sets,
envelopes and request shapes against `schemas/logweir-api-v1.openapi.json`),
`ui/client.js` (one `GET /api/v1/session` at boot decides the mode once and
synchronously; any failure is legacy; namespaces from `/session` in console
mode; the synchroniser token lives only in module memory and rides
`X-CSRF-Token`; console lists follow the cursor to the end and render
`ListTooLarge` rather than truncate), `ui/validate.js` (one check set and one
field-path vocabulary so problem+json field errors land beside the right input
in both modes) and `ui/workflow.js` (declared machines with named transitions;
`pending` has no `start`, which is the duplicate-submit guard; `unanswered` is
its own state that a late answer settles or `abandon` closes); `ui/plan.js`
returns one frozen plan document so review and submission are one object and
the plan golden is byte-identical. Pages changed only at their adapter call
site. The shipped asset set is now twenty files, moved consistently in
`Dockerfile.ui`, `scripts/check-image-ui.sh`, `chart_lint`, `ui_lint` and the
API's `boundary` suite. Verified at `48d5ec0`: `check-ui-behaviour.sh` 166/166
(contract 14, client 22, workflow 15 and the review's added rows), offline gate
twenty files, `ui_lint` 26, `chart_lint` 28, `doc_lint` 12, `gate_lint` 13,
logweir-api 106 across twelve targets, `chart-check`, the image gate; eleven
mutants killed (one equivalent mutant recorded as such; one — a mount calling a
bare import — was not hypothetical and is now guarded). Live docker-desktop:
legacy mode 14/15 of the existing Playwright journeys (the 15th is the shared
lab controller image, and a control run of `main`'s own `ui/` fails it
identically) and console mode 3/3 against a locally run `logweir-api` in
localAdmin mode with exactly one `/api/v1/session` request and zero direct
`/apis/logweir.dev/` requests, a problem+json field error rendered beside the
field with the draft kept, and the corrected create read back by UID; both
namespaces deleted after owner-label and UID checks. Independent review
`claude/ui-typed-client.review.md`: ACCEPT-WITH-FIXES (one high: the API's
boundary suite still asserted sixteen files) then ACCEPT. Migration: none for
operators (static assets; deep links unchanged; the legacy `kubectl proxy` mode
is untouched and remains the default when no `/api/v1/session` answers).
Limitations: the wizard machine is a test-time invariant, not enforced at
runtime (PLAT-18.2 owns the stepper); the CSRF header name is provisional
until PLAT-17.2 lands; no page surfaces cursor paging controls (PLAT-18.2);
`approvals`/`backups` creates are refused by name in console mode until their
routes exist.

### PLAT-18.2 — Make dense workflows accessible and scalable

**Problem/implementation:** Large topic/history lists and complex forms can
become unusable during incidents. Add consistent search/filter/pagination,
loading/empty/error states, keyboard navigation, labels, focus restoration,
responsive layouts and accessible progress announcements. Measure representative
large datasets before choosing virtualization or a framework migration.

**Acceptance:** Primary configure/restore journeys work by keyboard and remain
usable on supported small screens and large catalogs. **Tests:** Accessibility
checks plus manual keyboard journey, slow network, empty/error state, large
inventory/history and responsive screenshots. **Dependencies:** PLAT-18.1,
PLAT-09.1, PLAT-10.2.

**Migration/safety and done evidence:** Preserve deep links and asset deployment
contracts. Record any framework decision with measured benefit, migration cost
and regression coverage; a rewrite is not itself acceptance.

## PLAT-19 — Make trust rotation and approval policy operable

**Priority P1 · Proposed.** Automate routine evidence signing while retaining
verification. Scope: managed trust and configurable authorization policy;
transport correctness is PLAT-01.

### PLAT-19.1 — Replace implicit global trust with explicit lifecycle

**Problem/implementation:** The immutable, globally named roster makes target
changes and rotation require replacement. Add explicit trust-policy references,
authorized updates and overlapping key validity/retirement handling. Preserve
public material needed for old archives and define verification behavior for
revocation versus routine retirement.

**Acceptance:** Rotation permits new evidence and continued policy-correct
verification of old evidence without a blanket trust gap. The keys view labels
unevaluated or stale expiry/trust information as unknown, not valid. **Tests:**
Overlap, retirement, revocation, unknown/stale expiry, unauthorized update,
old archive, multiple namespaces
and upgrade from the default roster. **Dependencies:** PLAT-02.1; PLAT-17.2
for shared-console administration.

**Live validation record (2026-09-18) — PLAT-19.1 (evidence only, stays In progress).**
D3 W14 on `e7d0e79`, all PASS: an explicit `TrustPolicy` taking over from the synthesised
`legacy-roster-v1`; retirement keeping old evidence `Valid` with `trust.basis: Historical`;
`KeyCompromise` revocation flipping a terminal Backup's badge to `Untrusted` on the next
policy event with `phase`, `exitCode` and `conditions[Complete]` untouched; the CEL
lifecycle refusing a backwards edit with its own message. Upgrade case FAILS:
TRUST-UPGRADE-SIGNEDAT — status written before D3 W10 carries no
`evidence.verification.signedAt`, the re-derivation reads only that field
(`verification.rs:1554`) and fails closed to `Untrusted`, and clearing the block schedules
no archive re-read (`:1450`), so five 2026-09-14 lab objects cannot be repaired in place
while every fresh run is `Valid` (defect table; the lab refresh report §8 and
`claude/d3w14.result.md` §4). Still missing: a second signer's archive, multiple
namespaces, the keys view's `unknown` rendering (UI), and "unauthorized update" as an RBAC
result (the refusal proven here is CEL's under a cluster-admin subject). Evidence
`claude/artifacts/d3-live/20260918t0316z/`.

### PLAT-19.2 — Support ordinary confirmation and governed approval

**Problem/implementation:** Every restore currently carries the same external
approval ceremony. Define explicit installation/namespace policy for authorized
operator confirmation versus separately approved recovery, including separation
of duties, expiry and subject/plan binding. Materialize the selected policy's
authorized execution inputs server-side and retain auditable signed evidence.

**Acceptance:** The ordinary path is simple and the governed path cannot be
bypassed through direct API/CR submission or self-approval where prohibited.
**Tests:** Both policies, self-approval denial, expiry, changed plan, unauthorized
policy downgrade and direct-resource admission behavior. **Dependencies:**
PLAT-19.1, PLAT-17.2, PLAT-01.1.

**Migration/safety and done evidence:** Existing installations retain their
approval requirement until explicitly changed. Record enforcement points,
signer/approver authority separation and old-artifact compatibility.

**Partial record (2026-09-17) — PLAT-19.1: the trust lifecycle core, the
`TrustPolicy` controller and the migration/export commands (D3 W1) landed; the task
stays In progress until D3 W10 wires the verdict into evidence verification and
approval admission, W13 ships the `logweir-trust-admin` role and chart values, and
W12 replaces the keys view.** Landed in main as `64fcd38` (`logweir_core::trust`:
D3 §7.4's whole table as one pure `decide` with `now` an argument, `may_sign_new`,
`claimed_signing_time`, and the rehearsal scope types), `b02e983` (per-namespace
resolution — explicit binding, then the single `default: true` policy, then the
synthesized `legacy-roster-v1`; a namespace claimed by two policies resolves to
nothing; the reconciler writes `status.keys[].effectiveState/usable*`,
`boundNamespaces`, `conflicts`, `Loaded`/`Bound`/`ExpiringSoon`, marks the roster
`Superseded` only while a single default policy exists and clears it otherwise),
`7a95262` (`logweir trust export` writes public material only; `logweir trust
migrate-roster` emits a reviewable policy and refuses a roster key on both lists),
`6bd34ec` (G8: a `TrustPolicy` key declares exactly one usage, so the append-only
list can never make a dual-usage key permanent), `c9c7228`/`eb5bc32`/`488197a`/
`e3e4062`/`80d6d9d`/`5fc1a72` (review fixes: a not-yet-valid key verifies nothing,
a claimed signing time cannot lie in the future, both status writes carry S7's
precondition, the PEM guard is a substring, `EmptyPhases` named) and
`fdd1080` (docs). Verified at `5fc1a72`: the whole workspace 2006/2006 (trust core
24, `trust_policy_controller` 35, CLI 15), strict clippy and fmt, `pure-core`,
`one-signer`, `crds-check`/`chart-check`/`schema-check`/`render-install --check`;
eleven planted mutants killed plus the reviewer's; sixteen live CEL transition
probes on docker-desktop (`artifacts/d3w1/`) and a server dry-run of the
migrate-roster output. Mandatory security review `claude/d3w1.review.md`:
ACCEPT-WITH-FIXES (one high: the docs described policy-driven trust as live) then
ACCEPT. Decision amendments recorded in D3 at integration: the usage is spelled
`ConsoleConfirmation` (the landed CRD's immutable enum value; `ConfirmationIssuer`
retired), and the per-key usage-exclusion rule G8 is part of §7.1. Migration: the
roster remains the only consulted trust source until W10 — `docs/keys.md` and
`docs/kubernetes.md` say so and state that a revocation recorded on a
`TrustPolicy` today is not applied; rollback deletes nothing (public material is
never removed in either direction). Limitations: the lab cluster's installed
`trustpolicies` CRD predates G8 (the runtime refusal of a dual-usage key is owed
to the next lock holder, W14); the deliberate tightening that evidence claiming a
signing time after an expired legacy key's `notAfter` is `Untrusted` is
documented; `export` reads a policy from stdin or a file rather than dialling
the cluster.

**Partial record (2026-09-17) — PLAT-19.1: trust resolution wired into evidence
verification and approval admission (D3 W10) landed; the task stays In progress
until W11/W12 surface `Untrusted` and W14 proves rotation and revocation live.**
Landed in main as `b3aed6f` (verification through the namespace's resolved trust,
the fourth result, the four new refusal reasons), `0228333`/`2bb33ca`/`7da9bca` (tests),
`3fd4f6b`/`c5c4795` (docs) and `bc031de`/`3eb7897` (review fixes) on top of D3 W1's
`logweir_core::trust::decide` and `weirkeeper::trust`. Contract
(`docs/kubernetes.md` §15.3, §8): every evidence-verification site and the approval
admission resolve the object's OWN namespace through `weirkeeper::trust` (one
shared reflector per controller via `trust::resolve_with`), offer only
`EvidenceSigning` keys, and record `Untrusted` with `signedAt` and
`trust.{basis, keyState, policy}` beside `Valid`/`NotAttempted`/`Invalid`; a retired
key verifies historically only inside its window, a revoked key never, an unlisted
key is `UntrustedSigner`, a window not yet open verifies nothing, a
`ConsoleConfirmation` key is never accepted as an evidence signer and vice versa; a
contested namespace (two policies claim it) or an unconfigured one is a HOLD
(`NotAttempted` with the reason named), never a terminal verdict. Approvals ask
`may_sign_new` and refuse with `KeyRetired`, `KeyRevoked`, `KeyNotYetValid` or
`TrustPolicyConflict`; the restore path's approval bundle is built from the
resolved trust too — key material from `ResolvedTrust::key`, the allowlist from
`allowedTargetClusterIds`, expiry from `may_sign_new_for` — so a key that lives
only in a `TrustPolicy` (§7.6 rotation) verifies the Approval AND materialises the
bundle (the review found the old roster read there, looping forever). The re-trust
pass is real: both the Backup and Restore controllers watch `TrustPolicy` and map a
changed policy to the objects in its bound namespaces from the controller's own
store (zero API calls), re-deriving a terminal object's verdict when the resolved
policy digest differs and carrying conditions (`carry_conditions`) with a recorded
reason — a key revoked after a run went terminal flips its badge on the next policy
event, and an edit that NARROWS a policy (a namespace or key removed, or `default`
cleared) enqueues the union of the policy's scope before and after the event
(`trust::PolicyScopeMemory`), so nothing waits forever in `await_change` (the review's fail-open residual, closed
before the merge with direct rows for the mapper and `apply_retrust`'s S7
precondition). No policy bound ⇒ `legacy-roster-v1` synthesis (§7.5), asserted field by
field against the pre-change verdicts including `notAfter` in both directions. Every
status write is resourceVersion-preconditioned; no private key is read anywhere
(`check-one-signer` green). Verified at `7da9bca` on main: weirkeeper + `logweir-core`
1142/1142 (`verification` 34+, `approval_controller` 41+), strict clippy and fmt,
`check-one-signer`, `check-pure-core`, `manifest_lint` 29, `doc_lint` 12, no
RBAC/CRD/chart change (`trustpolicies` list/watch was already granted); thirty-nine
planted mutants killed from a clean build (the worker's first matrix was
untrustworthy — an mtime restore left a mutation compiled in — and was re-run).
Independent review `claude/d3w10.review.md`: ACCEPT-WITH-FIXES (one high: the
restore approval bundle still read the roster; three medium: nothing called the
re-trust pass, the §7.5 fixture lacked a `notAfter` row, and a catalog-vocabulary
flattening; six low; one question) then ACCEPT. Hand-offs: W11/W12 must surface
`Untrusted` (the API maps it to `Unknown` today, pinned by a guard row) and carry
`UntrustReason` beside the catalog's flattened word; `approval::decide` still LISTs
per reconcile (a `Context` field across twelve live sites — a follow-up, not a
correctness defect); a namespace a policy governs reads no roster at all. Live: none here — W14 owes §15's rotation, revocation and
re-trust scenarios. Migration: none — an installation without a `TrustPolicy`
verifies exactly as before through the synthesised legacy roster.

## PLAT-20 — Prove complete journeys and ship without gate sprawl

**Priority P1 · Proposed.** Scope: behavioral validation, operational handoff
and proportionate release checks. CI simplification is already delivered.

### PLAT-20.1 — Build a focused cross-layer regression journey set

**Problem/implementation:** Unit fixtures do not prove an incident workflow.
Add deterministic browser/API/controller journeys covering registration,
discovery, scheduled/manual backup, selected-point restore and durable progress;
include the critical failure/race cases from this tracker. Reuse existing suites
and isolated docker-desktop fixtures instead of duplicating every check.

**Acceptance:** Journeys verify archive data/evidence and durable resources,
not only rendered text; failures leave redacted diagnostics. **Tests:** SCRAM
rotation, new topic in dynamic policy, overlap, two approvals, source offline
and CR loss, stale namespace request, duplicate submit and old-point selection.
**Dependencies:** Land coverage with each corresponding spike; full integration
requires PLAT-01 through PLAT-19's implemented surfaces.

### PLAT-20.2 — Publish supported behavior, upgrade proof and performance limits

**Problem/implementation:** Feature labels can exceed tested behavior and
documentation can re-fragment. Update existing operator/install/recovery guides,
UI terminology and release notes with supported paths, verification scope,
retention authority and migration/rollback. Measure discovery, history queries
and status load; add a performance regression check only for a demonstrated
budget, within the existing pipeline when possible.

**Acceptance:** A new user follows one supported setup/recovery guide; upgrading
retains identities, schedules and archive readability. Main publication stays
the tested existing flow without redundant mandatory workflows. **Tests:**
Clean install and upgrade on docker-desktop, documented recovery rehearsal,
large-catalog measurements, relevant link/contract checks and the existing CI.
**Dependencies:** The features included in the release and PLAT-20.1 evidence;
unfinished spikes remain explicitly proposed.

**Migration/safety and done evidence:** The AI handoff must list shipped task
IDs, commit/image identities, tested environments, results, limitations and
rollback instructions. Update this tracker per task rather than claiming the
entire roadmap complete after one release.

---

Documentation is licensed [CC-BY-4.0](../LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
