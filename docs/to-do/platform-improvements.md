/# Platform improvements tracker

Status: **all 41 tasks Done (2026-09-25)**, each with its completion record.
Follow-up rows stay open in the defect ledger: POC-P15 (fix in flight),
CONSOLE-MCP-ROUND3's low rows and REPLACE-MINIO. Product expansion has not
started, by the user's decision. Only tasks with explicit completion evidence
are Done.
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

Shared lab fixture (2026-09-22/23, `lab-refresh-8`): both images rebuilt from main `f49849d` (controller `sha256:01338ec0…`, runner `sha256:ad705ba2…`; the `1a9aca6` pair kept as tags), the CRDs applied (`backups` 7→8, `restores` 5→6, `rehearsalschedules` 4→5; every UID unchanged), roles and policy re-rendered with the lab's own values and unchanged, `"controllers":14`, no ERROR or Forbidden; plat07 14/14, plat06 case-a…g (case-c no longer racy), console journey 20/20. The lab approver key had been rotated the same day by the owner's decision (`TrustRoster/default` recreated with `approverKeys = [b7a5ac87… (new), a1616991… (old)]`, key material under `$HOME/.logweir-lab/scram-e2e`) and the seed topics `orders`/`payments`, which the broker's default 7-day retention had emptied, were re-seeded identically with `retention.ms=-1` (LAB-SEED-TOPIC-RETENTION). Closed live: EVIDENCE-FETCH-JOB-UNBUILT, APPROVAL-KEY-WINDOW-UNPUBLISHED, CATALOG-RECEIPTKEY-REDACTED, REHEARSAL-STANDING-APPROVAL-NEVER-VERIFIES, REHEARSAL-NO-CANDIDATE-WITHOUT-DIGEST and RESTORE-ADMITTED-DROPPED's Restore half. New: REHEARSAL-PLAN-AUTH-PLAINTEXT, CATALOG-TRUST-ROSTER-ONLY, CONSOLE-APPROVAL-VERIFIED-SUBJECT-UNMAPPED, RESTORE-COMPLETION-UNWRITTEN, SCHEDULE-FIRES-SLOT-BEFORE-CREATION. PLAT-10.1 and 10.2 reached Done. Report `claude/lab-refresh-8.result.md`; harness fixes on `claude/lab-refresh-8` (13 commits).

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

Resumption 2026-09-22 (Claude Opus 5.5 orchestrating, after Codex's usage limit): Codex had pushed
`1947864` and integrated `claude/fix-backup-projection` locally; the combined gate was re-run
(`scripts/ci-check.sh` exit 0, 3,284 tests) and `454cd6b` pushed (CI 35758414988 green incl. publish).
`claude/fix-standing-verify` then took a final Rust+security review (REJECT on one MEDIUM, closed in
`dffe118`/`67aad4f`/`95c2279`), landed as `0237e9c..95c2279` (gate exit 0, 3,300 tests), and closes
REHEARSAL-STANDING-APPROVAL-NEVER-VERIFIES and REHEARSAL-NO-CANDIDATE-WITHOUT-DIGEST pending live proof.
`claude/harness-rows-9` (D3 §15 L6 as ten rows) was rejected by its Tier-B review (step 7 could not
fail; the four arms shared one target) and fixed together with every harness defect lab-refresh-7
found (the shadowed `patch_policy`, the retention schedule suspended at birth, plat06 case-c's single
read, the history journey's locator, the ceiling journey's early read, the POST-count window race),
plus an AST guard against duplicate harness definitions; landed with lab-refresh-7's committed
`scripts/d3-ui-e2e.mjs`. The lab approver private key was lost to macOS `/tmp` cleanup; by the
owner's decision a new lab approver keypair was minted (stored under `$HOME/.logweir-lab/scram-e2e`,
`/tmp/logweir-scram-e2e` now a symlink) and `TrustRoster/default` replaced per `docs/install.md`'s
roster-only rotation with `approverKeys = [b7a5ac87… (new), a1616991… (old)]`, no lab object changed
phase or verdict (`claude/artifacts/approver-rotation-20260922/`). Next: lab-refresh-8 (controller +
runner from main with the standing fix, `claude/verdict-precedence`, `claude/evidence-fetch`) → L6,
16.2, 14.1 restore half, S22/S23, old-archive, PLAT-10.2's blocked rows.

| Tasks | State | Worker | Boundary and next evidence |
| --- | --- | --- | --- |
| PLAT-01.1 / 01.2 / 02.1 / 02.2 | Done | plat01-02-live-finish, plat01-02-live-review | Completion records under PLAT-01 and PLAT-02. Integrated into main as `4b7a1f6` (chart bootstrap digest pin) and `fbd124e` (the two live harnesses). |
| PLAT-13.1 | Done | closure-0413 | Completion record under PLAT-13.1. |
| PLAT-04.1 | Done | closure-0413, w0-reservation, plat06-live, plat06-review | Completion record under PLAT-04.1. The two gaps closed live in `10f6c28`'s run: the deleted-Job case (e) and the reservation under the unmodified shipped role (c). |
| PLAT-04.1 defect (P0) | Fixed (`bdd26dc`, live-proved) | w0-reservation → plat06-live | `backup_schedule.rs:1359` reserves a Forbid slot with `replace_status`, which RBAC authorizes as `update` on `backupschedules/status`; the shipped role grants only `patch` (`config/rbac/role.yaml:118`, `charts/logweir/templates/clusterrole.yaml:29`). Confirmed on docker-desktop: the lab ServiceAccount has `update` no, `patch` yes, so with the default `Forbid` policy no scheduled Backup is created on a shipped install. PLAT-04.1's live run used custom namespace Roles and never exercised this. Fix: resourceVersion-conditional merge PATCH, an audit of every other call against the shipped role, a reverse "every call has a grant" lint with mutant evidence, and live proof under shipped RBAC. |
| PLAT-06.1 | Done | plat06-live, plat06-review | Completion record under PLAT-06.1. Integrated into main as `8e362f9..10f6c28`. |
| PLAT-07.1 | Done | plat07-finish, plat07-integrate, plat07-review, plat07-live | Completion record under PLAT-07.1. Integrated into main as `6c534b2..199020a` plus the live harness `50e641f`. | Versioned connection contract, one shared resolver for probe/backup/restore Jobs, TLS private CA, rotation, redaction, write-only credential builder; live SCRAM rotation and TLS cases. |
| PLAT-10.1 / 10.2 | Done | plat10, plat10-finish, lab-refresh-8 (+ two reviews) | Completion record under PLAT-10.2; live on docker-desktop 2026-09-23 incl. create → backup → detail → restore. |
| PLAT-08.2, 12.1, 12.2, 14.1, 14.2, 15.1, 15.2, 16.2, 18.2, 19.2, 20.1 | Done | (see each record) | Completion records under each task; live on docker-desktop 2026-09-23 (lab-refresh-9 and harness-rows-12, main `306cebf`). |
| PLAT-14.3 | Done | rehearsal-catalog-trust, rehearsal-fix, reserve-commit, lab-refresh-9/10/11 (+ reviews) | Completion record under PLAT-14.3; live on docker-desktop 2026-09-24 (lab-refresh-11, main `86a554e`), every tracker test. |
| PLAT-20.2 | Done 2026-09-25 | plat20-2 (offline half), poc-install, poc-upgrade-1/2/3, poc-fixes-1…4, release-docs-final (+ reviews); MCP console rounds 1–3 (orchestrator) | Completion record under PLAT-20.2. Live on docker-desktop 2026-09-24/25, on the PoC profile, from the published chart and images: a clean install, rehearsals R1/R2 with rollbacks, and three in-place upgrades to `a54fb823` (328/328 receipts VALID). README §10 is clean for a new user. Release notes and handoff are true at `4e58d330`. Residue: 258 of 1,000 points, POC-P15 and REPLACE-MINIO. |
| PLAT-17.2 (PLAT-08.2, 12.1, 15.2, 19.2, 20.1 Done 2026-09-23) | Done 2026-09-24 (see its record; PoC round on the real Traefik + Dex ingress); was: In progress (source on main since `ac00819`, 2026-09-23: seven accepted branches integrated in `claude/integration-2`, gate 3,545/0; each passed its own Tier-A or Tier-B review and re-check; live evidence so far is each worker's own journey; Done records wait for lab-refresh-9 on a build that carries them) | plat08-2, plat15-2, plat17-2, plat19-2, plat20-1, integration-2 (+ reviews) | Worker reports `claude/<branch>.result.md` and reviews `claude/<branch>.review.md`. Decisions at integration: the `values.yaml` lint ceiling rises 230→232 (two reviewed option sets); a catalog-point restore writes evidence to the point's own destination. |
| PLAT-11.2 | Done | ui-restore-selection, d2w13, d3w12, plat11-2 (+ two reviews) | Completion record under PLAT-11.2; live on docker-desktop 2026-09-22, all eight tests through the console. |
| PLAT-07.2 | Superseded — Done (see the PLAT-07.2 Done row below) | ui072, ui072-review | Partial record under PLAT-07.2. Integrated into main as `8bbe4d1..b65f23f`. "Test connection" cannot yet force a re-probe (D2 W13). | Saved-cluster selector by UID, probe vocabulary, freshness budget; live 20/20 |
| PLAT-13.2 | Done | ui-correct, ui-correct-review, ui-correct-fix | Completion record under PLAT-13.2. Integrated into main as `2a34abd..8020876`. |
| PLAT-11.1 | Done | ui-restore-selection, ui-restore-selection-review | Completion record under PLAT-11.1. Integrated into main as `6c2c95e..02426c8`; the same branch fixes the `allowHttp` half of UI-HTTPDOWNGRADE (D2 W13a). |
| PLAT-18.1 | Done | ui-typed-client, ui-typed-client-review | Completion record under PLAT-18.1; D0 stage 6 (static client migration) done for its own scope. Integrated into main as `fa73824..48d5ec0`. |
| PLAT-12.1 (immediate slice), PLAT-12.2 (subject + retry slices) | Superseded — both Done 2026-09-23; was: In progress (slices landed; 12.2's retry identity closed by `claude/plat11-2` 2026-09-22 — a retry never silently reuses an approval bound to another execution, fresh target + new approval; open for 12.2: the verified-approval live route (an approver key the lab lacks); open for 12.1: PLAT-19.2 policy routing, the PLAT-11.2-backed selection flow having landed) | ui-correct, plat11-2 | The guided submit, idempotent durable Restore, subject binding and the fresh-target retry landed (records under each task). |
| PLAT-17.2 (stage 2) | Superseded by the PLAT-17.2 row above; was: In progress (stage landed) | plat17-2-authz, plat17-2-authz-review | Partial record under PLAT-17.2. Integrated into main as `24752f4..90ecd0c`. Shared mode is implemented and live-verified locally; deployable since D0 stage 7 (PLAT-17.1 Done 2026-09-21) but not declarable secure until D0 stage 5. |
| PLAT-05.2 | Done | d1w4, d1w8, d1-fence (+ reviews) | Completion record under PLAT-05.2; live on docker-desktop 2026-09-18 behind D1 §13.1's fence. |
| PLAT-04.2 | Done | d1w2, d1w6, d1w8, d1-fence, d1w7 (+ reviews) | Completion record under PLAT-04.2; live on docker-desktop 2026-09-18 incl. the console journey. |
| PLAT-09.1 | Done | d2w4, d2w8, d2w12, d2w13, d2w14, fix-discovery-results, lab-refresh-3 (+ reviews) | Completion record under PLAT-09.1; live on docker-desktop 2026-09-18. |
| PLAT-03.1 | Done | d2w4, d2w9, d2w12, d2w13, d2w14, fix-runner-checks, d2-source-check, lab-refresh-3/4 (+ reviews) | Completion record under PLAT-03.1; live on docker-desktop 2026-09-18. |
| PLAT-07.2 | Done | ui072, d2w13, d2w14, d2-source-check, ui-conn-followups, lab-refresh-4 (+ reviews) | Completion record under PLAT-07.2; live on docker-desktop 2026-09-18 incl. the dialling control. |
| PLAT-16.1 | Done | d3w9, d3w14, fix-retention, harness-refresh, harness-rows-2, lab-refresh-3/4/5 (+ reviews) | Completion record under PLAT-16.1; live on docker-desktop 2026-09-18. |
| PLAT-05.1 | Done | d1w2, d1w6, d1w7, d1w8, d1-fence, harness-refresh, harness-rows-2/4, fix-trust-upgrade(-2), lab-refresh-4/5 (+ reviews) | Completion record under PLAT-05.1; live on docker-desktop 2026-09-18/19 incl. a real rollback. |
| PLAT-03.2 | Done | d2w4, d2w9, d2w13, d2w14, fix-runner-checks, d2-status-destination, fix-preflight-approval, harness-rows-6, lab-refresh-3/4/5/6 (+ reviews) | Completion record under PLAT-03.2; live on docker-desktop 2026-09-19. |
| PLAT-09.2 | Done | d1w5, d1w7, d1w8, fix-discovery-results, lab-refresh-3, plat09-2-rows (+ review) | Completion record under PLAT-09.2; live on docker-desktop 2026-09-21, all seven L-09 rows on one build. |
| PLAT-17.1 | Done | plat17-api-finish, plat17-api-review, d1w6, d2w12, d3w11, d3w13, lab-refresh-3, plat17-1-stage7 (+ review) | Completion record under PLAT-17.1; D0 stage 7 landed 2026-09-21 (console image, chart deployment under the API's own RBAC, live can-i matrix); residue named in the record. |
| PLAT-19.1 | Done | d3w1, d3w10, d3w14, fix-trust-upgrade, fix-trust-expiry, harness-rows-4/5/6/7, lab-refresh-4/5/6, d3w11, d3w12 (+ reviews) | Completion record under PLAT-19.1; live on docker-desktop 2026-09-18..21 incl. the keys view's unknown rendering. |
| PLAT-08.1 | Done | d2w1, d2w2, d2w7, d2w10, d2w11, d2w13, d2w14, d2-status-destination, lab-refresh-3/4, plat08-u6 (+ reviews) | Completion record under PLAT-08.1; live on docker-desktop 2026-09-18..21 incl. the measured per-role permission table. |
| PLAT-06.2 | Done | d1w6, d1w7, d1w8, plat06-2-finish (+ reviews) | Completion record under PLAT-06.2; live on docker-desktop 2026-09-21 incl. the CLI path beside the UI and the failed-preflight journey. |
| PLAT-08.2 | Superseded — Done 2026-09-23 (see the PLAT-08.2 record); was: In progress (every D2 wave landed; PLAT-03.1, 03.2, 07.2, 08.1 and 09.1 Done; 08.2's "document required object permissions" clause is met by the measured table; it waits on its console rows — destination edit during a draft, a recovery point inheriting its saved settings, addressing and transport as independent controls) | [D2](decisions/D2-destinations-discovery-readiness.md) | `BackupDestination`, `TopicDiscovery`, `Preflight`, one shared check runner; sixteen worker tasks. W1 (pure check contract and destination model) and W2 (explicit store options) landed as `c13b0cc..56bd074` after review (ACCEPT after two high and three medium fixes: JSON-form redaction bypass, ambient credentials inheriting the environment). W3 (`logweir_kafka::inventory`: bounded targeted describe, broker count, validate-only `CreateTopics`, error classification where an observed authorization failure makes visibility `limited` and anything unknown is failure, with a real admin-client fault capture because rdkafka 0.36 never invokes `ClientContext::error` for a metadata-only workflow — D2 §4.2 `[VERIFY U5]` corrected) and W5 (`weirkeeper::check`: check Jobs mirroring the execution pod, pod selection by controller owner UID only, framed-stdout relay through the W1 decoder, the full waiting-code table, TTL, plan/chunk/limit modules, the installation policy loader failing closed) landed as `23cec50..b8e62d1` after review (ACCEPT after one high and three medium fixes; 19 mutants killed; the rebase over PLAT-07.1 then routed the inventory client through the reader's `client_config`, removing a drifted copy that could upgrade plaintext to TLS when a CA was present — re-checked ACCEPT; weirkeeper 435, kafka 57). RBAC still owed by W11: `events: list` plus its `manifest_lint` row, the three new kinds' verbs, and a decision on `gc.rs`'s deletes. The reviewers' SEC-PODLOG finding against `controllers::backup::select_job_pod` is closed by `secpodlog` (see the defects table). W6a and W6b (the three Amendment F kinds and the destination sentinel on existing kinds) landed in `46880a3`/`88232f5`/`b334a98` inside `crds-shapes`; W7 (destination resolver, controller, evidence store cache) landed as `27fb924..0b25e95` (see the PLAT-08.1 partial record); W4 (runner `logweir check run`) landed at `537657d` (see the PLAT-03 partial record); W8 (`TopicDiscovery` controller) and W12 (API routes) are in review or in progress. W9, W10, W11, W13, W14 remain. |
| PLAT-14.x, 15.x, 16.2 | Superseded — 14.1, 14.2, 15.1, 15.2 and 16.2 Done 2026-09-23; 14.3 Done 2026-09-24 (see its record); was: In progress (W0–W13 and the console wave W12 landed; PLAT-16.1 and 19.1 Done; 16.2 waits on the batch lab refresh for the degraded condition and the retention-count/status-sweep rows, 15.1 on CATALOG-RECEIPTKEY-REDACTED, 14.2 on PROTECTION-SECRETKEYS-UNPROTECTED, 14.3 on the standing-authorization runner path (14.3b), 14.1 on the refresh's operation-state rows) | [D3](decisions/D3-status-catalog-retention-trust.md) | Operation states, protection freshness, rehearsals, durable catalog, retention enforcement boundary, trust lifecycle; fifteen worker tasks. W4 (`d3-notify`: the shared notification module and `logweir notify deliver`) landed after review (ACCEPT after two high fixes); W3 (`d3-catalog-writer`: signed catalog point records, `list_page`, `logweir catalog sync|list`) landed after review (see the PLAT-15.1 partial record); W0 (the five Amendment G kinds, additive run status, `Restore.spec` additions, the `Approval` enum) landed in `496451a`/`88232f5`/`b334a98` inside `crds-shapes`; W1 (trust lifecycle core, `TrustPolicy` controller, `trust export|migrate-roster`, G8) landed as `64fcd38..5fc1a72` (see the PLAT-19.1 partial record); W8 (`RecoveryCatalog` controller) in progress. W2, W5, W6, W7, W9, W10, W11, W12, W13, W14 remain. |

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
| UI-HTTPDOWNGRADE | **CLOSED (audit 2026-09-23): fixed by first half `6c2c95e`, second half `c761a46`; archive/evidence separation row on lab-refresh-9.** The restore wizard sets `allowHttp` from the path-style checkbox (`ui/pages/restore-wizard.js:1142`) and applies one endpoint/region/addressing to both the source archive and evidence store (`:1135`). **First half fixed** in `6c2c95e` (D2 W13a): path-style never sets `allowHttp`; an explicit, separate "allow insecure HTTP" control defaulting off is the only source of `allow_http: true`, guarded by a behaviour row and a live journey that reads the plan bytes the API server holds. The one-endpoint-for-archive-and-evidence half remains PLAT-08.2. | PLAT-08.2 UI slice |
| UI-FAKEPREFLIGHT | Wizard step 5 "Target-topic preflight" shows only the target cluster's cached `status.reachable` (`ui/pages/restore-wizard.js:424`), and restore admission gates on the same cached value (`controllers/restore.rs:638`). **Fixed by D2 W13 (`claude/d2w13.result.md` §3.6): wizard step 5 creates a real `Preflight` bound to the plan hash on screen and renders its rows, and restore admission gates on the Preflight's verdict; proven live in D2 W14's wizard journeys and on lab-refresh-6 (S15, S15b: a new-target conflict after a green preview ends `GuardRefused`, `exitCode 3`). Row closed 2026-09-19 with PLAT-03.2's completion record.** | PLAT-03.2 |
| RET-WRONGBUCKET | Retention lists manifests through the controller's single global store while rendering commands for the schedule's own URL (`backup_schedule.rs:1417`), so a schedule on another bucket is reported against the wrong catalog. Since D1 W2 (2026-09-17) `destinationRef` is editable, so the wrong-bucket case is also reachable by an edit between runs; retention must evaluate per frozen run, not the schedule's current URL. **Fixed** by D3 W9 at `a3af420`: membership is a private newtype keyed on the frozen run's destination, the legacy report is replaced by a note with empty set lists, and every evaluation names the destination it describes. | PLAT-16.1 |
| FLAKE-APISHUTDOWN | `a_held_connection_does_not_block_shutdown_past_the_grace_period` (`crates/logweir-api/tests/local_admin.rs`) fails with `ConnectionReset` when several cargo test runs share the host and passes in isolation (2/2 on 2026-09-17 at the D2 W12 rebase); the held connection's read `unwrap`s, so a reset past the grace deadline — the behaviour under test — is reported as an error. Also seen under the same load: `the_binary_serves_loopback_and_stops_on_sigterm` ("answers within five seconds: ConnectionReset", the d2w9 integration gates, 2412/1) and once `a_non_loopback_listener_is_refused_before_anything_is_bound`; the suite is 12/12 alone. Fix: **the diagnosis above is wrong and is corrected here.** Reproduced 2026-09-17 by running five copies of the test binary concurrently: 5 failures in 15 runs across **four** tests (`the_binary_serves_loopback_and_stops_on_sigterm`, `an_oversized_request_head_is_refused`, `the_connection_ceiling_holds_and_then_releases`, `a_non_loopback_listener_is_refused_before_anything_is_bound`), every one a socket belonging to another run and every one returning in under 13 s — so no failure was a timeout, and neither a longer `SERVE_LIMIT` nor accepting a reset would have fixed any of them; the test this row is named after did not fail at all. One cause: `free_port()` reports a port nothing is listening on and cannot reserve it, so a concurrent process takes it before the child's `bind`, and a successful connect proves only that *someone* is listening. **Fixed** (`flake-localadmin`, `e0d729f`, branch `claude/flake-localadmin`): no test infers a server from a port — `start_server` waits for the child's own `logweir-api started` line naming that exact port and restarts on a fresh port when the child reports `cannot bind the listener`; `http_get` retries a transport failure until `SERVE_LIMIT`; the held-connection test retries its keep-alive setup instead of `unwrap`ing it and asserts the post-exit reset/EOF as the pass, a reset before the signal as the failure; the non-loopback test asserts "nothing was bound" from the child's stdout, which also catches a bind made and closed again (its old probe ran after the child was dead, so it could only ever have seen a foreign listener). `SERVE_LIMIT` stays 40 s. Evidence at load average 20–26: 15/15 under the concurrency that failed 5/15, 10/10 under `cargo build -p weirkeeper` plus four concurrent suites (44 further runs, 0 failures), 3/3 alone. Test file only; no production change. Review ACCEPT (`flake-localadmin-review`, 0 critical/high/medium, 4 low follow-ups): independently re-ran 34 suite executions under the same concurrency with 0 failures, and re-proved the properties with four mutants — SIGSTOP before SIGTERM makes the shutdown genuinely hang and the held-connection test still FAILS in 26 s; a reset before the signal FAILS in 42 s with the directional message; the new post-exit reset/EOF assertion FAILS in 8 s when the connection neither ends nor resets; and removing `config::is_loopback` still FAILS the non-loopback test in 60 s. Fixed in `cd16ce6` (`crates/logweir-api/tests/local_admin.rs` only; review `claude/flake-localadmin.review.md` ACCEPT with four follow-up lows). | PLAT-17.1 |
| FLAKE-LOCALADMIN-LOGLEVEL | The local-admin harness identifies its child and exact listener from the API's INFO startup record, but an ambient `RUST_LOG=warn` suppresses that record and made a healthy listener look like a 40-second startup timeout. **Fixed** on main at `580cb5c`: `Watched::spawn` sets `RUST_LOG=info` for the child only, without changing the test process or production default. Evidence: 12/12 under ambient `RUST_LOG=warn`; fresh isolated-target workspace check, strict Clippy, fmt and full `logweir-api --all-targets` passed; independent code and Rust review found no issue. The integrated source `580cb5c54e38a495b6a1ee7a205618f3820ec3c0` passed `env -u RUST_LOG LOGWEIR_PYTHON=/tmp/logweir-roadmap-run/venv/bin/python3 bash scripts/ci-check.sh` (exit 0, including full workspace, UI, image fixture, schema, chart, links and advisory/license gates). GitHub run 35699688761 independently passed `check` and `e2e`; publication alone failed on the stale console-image file-count gate described below. Replacement run 35703652337 at `1aa1c72` passed `check`, `e2e`, both architecture builds and promotion. | PLAT-17.1 test reliability |
| API-STREAM-END-CAPACITY | The operations SSE producer could fill its bounded channel with progress frames while the client was not reading, leaving no capacity for the required terminal `end` frame. A test that merely saw some frames did not prove the stream contract. **Fixed** on main at `27173bc`: the producer owns one `OwnedPermit` for the terminal frame and non-terminal frames can consume only the other seven slots. The regression intentionally leaves the body unread, then drains and requires the final event to be `end` with `reason=maxDuration`; it passed 20 consecutive stress runs, while removing the permit fails deterministically (`/tmp/logweir-roadmap-run/stream-end-mutant.log`). Full `logweir-api`, fmt and strict Clippy passed; independent code and Rust reviews approved. Included in the integrated `580cb5c` full gate, the successful functional jobs in run 35699688761, and the fully green replacement run 35703652337. | PLAT-17.1 / PLAT-18.2 stream reliability |
| CI-CONSOLE-IMAGE-FIXTURE | Image publication had added the console platform products, but `scripts/test-ci-images.py` still supplied only runner/controller fixture digests, so main CI run 35691073351 failed four promotion tests with missing `image-digests/logweir-console-amd64.json` even though its E2E job passed. **Fixture fixed** on main at `ea6b18a`: it covers console amd64/arm64, asserts `console_digest`, and derives product/write counts rather than freezing the old two-image total; promotion suite 9/9 and independent review passed. Run 35699688761 then exposed a second publication-only drift after both functional jobs passed: both architectures built the correct 26-file console image, while `scripts/check-image-api.sh` still pinned the pre-operations count of 22. **Gate fixed** at `1aa1c72`: the console gate pins 26, and a Rust regression requires both UI-bearing image scripts' tree/image guards to equal `shipped_ui_files()` and contain no stale `twenty-two` claim. Focused chart lint 42/42, strict focused Clippy, fmt, Bash parse and independent review passed; a fresh arm64 `Dockerfile.console` build passed all six `check-image-api.sh` checks. Replacement GitHub run 35703652337 passed the full matrix: `check`, `e2e`, arm64 build, amd64 build and promotion. | PLAT-20.1 CI regression set |
| FLAKE-APICOOKIE | `the_sealed_cookie_contains_no_provider_material` (`crates/logweir-api`) fails about one run in eight hundred: `seal` uses a random nonce and base64url, so the literal `u-1` the test forbids can appear by chance inside the ciphertext. Seen once in the D3 W9 integration run on 2026-09-17 (60/60 in isolation, green on re-run). Fix (PLAT-17.2's owner): assert the provider material is absent by decoding the envelope, not by a substring over random bytes, or use a fixed nonce in the test. Recorded, not fixed. Second occurrence 2026-09-19 in the fix-retention-count merge gates (3185 passed, this one failed; 3/3 green on re-run) — the gate was accepted on the re-run. Fixed 2026-09-21 in `83d1a01`: the guard opens the sealed envelope with the test's key and reads the DECODED payload — the field set is pinned to the documented contract (`sid, iss, sub, name, groups, iat, exp, auth, kv`; an undeclared field is provider material until declared), every string is scanned for provider names and for a JWT shape, 128 fresh seals per run, the wire form asserted opaque (ciphertext ≠ plaintext, another key gets `NotAuthentic`); a four-row mutant seals forbidden material through the same path and is caught by every shape. 2500/2500 loop runs green (the old assertion measured 349 false failures in 200000). Test-only; gates only per the lean loop. | PLAT-17.2 |
| ENGINE-PATHSTYLE | The pinned engine ignores `path_style` and forces path-style addressing whenever an endpoint is set (`kafka-backup-core storage/s3.rs:66`), so virtual-hosted addressing with a custom endpoint cannot be honoured and must be refused rather than advertised. | PLAT-08.1 (documented refusal) |
| STATUS-RECORDS | `Backup.status.records` is declared in `config/crd/backups.yaml` with a `RECORDS` printer column and is never written by any controller path; it is blank on every Backup the PLAT-06.1 and PLAT-07.1 live runs produced and on the lab's own scheduled Backup, while the counts exist in the signed receipt. Found by plat07-live. Either write it from the verified receipt or drop the field and column. **Fixed** by D3 W2 at `d69ca1a`: written from the verified receipt only, absent otherwise; `status.completion`/`status.teardown` remain declared-and-unwritten (owner needed). | PLAT-14.1 (D3 W2 status/progress) |
| RECEIPT-DUP | **CLOSED-LIVE (2026-09-24, lab-refresh-10: plat06 case-e both arms, receipt-dup rows 2–5).** **FIXED (2026-09-23): `claude/receipt-dup` merged (execution claim; D3 amendment of this date); live case-e rows at lab-refresh-10.** A Backup Job re-created from its frozen inputs (PLAT-06.1 case e) writes a second run-id receipt under the same execution id while overwriting the manifest at the same key; if the topic advanced between the runs, the first signed receipt's digests no longer match and a verifier reports it Invalid. Found by plat06-review (M1); run identity is idempotent, signed evidence is not. | PLAT-15.1 catalog / D3 W3 (point identity is content-derived from the receipt) |
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
| STATUS-PATCH-NO-RV | **CLOSED (audit 2026-09-23): fixed by `04020b0` (status-sweep).** `patch_status_if_changed` (weirkeeper) sends its merge patch without a `metadata.resourceVersion` precondition, while `charts/logweir/README.md:53` and the D3 W2 record state that every status write is resourceVersion-preconditioned; found pre-existing by the d2-status-destination review (F1, low) — the frozen destination block itself is written under the precondition, but any status write that goes through this helper can overwrite a concurrent controller write. Fix (not started): carry the observed `resourceVersion` into the helper's patch and add the row the D3 W2 record implies; grep every caller. | PLAT-14.1 (D3 W2 seam S7) |
| TRUST-EXPIRY-LAG | On lab-refresh-4's S18 (an isolated `TrustPolicy` whose approver key has a `notAfter`), the approval's `Verified` condition read `True` for 2 m 35 s after `notAfter` passed before the controller re-derived it to `Verified=False, KeyIdExpired` — an expired key shown as valid for the length of a reconcile interval, against PLAT-19.1's acceptance ("treat unevaluated or stale expiry information as unknown, not valid"). Found by the independent review of lab-refresh-4 from the row's own samples (`claude/lab-refresh-4.review.md`). Fixed 2026-09-18 in `532740d` (+ docs `efa16b2`): the `Verified` condition is the verdict, so the boundary is made exact by the timer — an Approval requeues at its matched key's `notAfter` and a TrustPolicy at its next `notBefore`/`notAfter`/`ExpiringSoon` horizon, the heartbeat as ceiling, bounded in [1 s, 300 s] (a past deadline rests at the heartbeat, 500 keys arm one wakeup, a restart re-arms from the object); `may_sign_new` and `effective_state` flip on the same `now >= notAfter`, so `Verified=True` is never written at or after the boundary. What remains is stated in `docs/keys.md`: the requeue floor plus reconcile latency, an outage across the boundary and clock skew — a reader that must never act on a stale `Verified=True` treats a closed key window as unknown. Review (fail-closed lens) ACCEPT; 16/16 mutants. Live proof: CLOSED 2026-09-18 on the lab at `d387f87` — the refusal landed 4 ms after `notAfter`, 0 of 44 post-boundary samples read `True` (was 2 m 35 s); `claude/lab-refresh-5.result.md` §9.2, re-verified in `claude/lab-refresh-5.review.md`. | PLAT-19.1 (D3 W10) |
| RET-DEGRADED-UNREACHABLE | **CLOSED (audit 2026-09-23): fixed by the status-sweep class fix; bounded retry PASS on lab-refresh-8 and -9.** `RetentionPolicy.status.consecutiveRunFailures` never rises past 1, so D3 §6.5's `EnforcementDegraded` (three consecutive failed runs stop scheduling until the spec changes) is unreachable: after a failed enforcement run the policy controller's status patch is refused by the write guard ("carries no metadata.resourceVersion … no patch is sent", ~11 times a second in the controller log) because that write path builds its patch without the observed `resourceVersion` — the STATUS-PATCH-NO-RV class in one more place; the runs themselves keep being scheduled (five Jobs in 140 s), so the bound is not applied. Found by the harness-rows-2 review (`claude/harness-refresh.review.md`, H-1) on the lab at `7b4fae9`. Cause corrected at the fix's review: every status write carries its resourceVersion — the WARN was the policy's own guard after a 409 cleared the cursor mid-pass, a symptom; the defect is `start_run`'s `lastEnforcement` merge PATCH leaving the previous run's `finishedAt` behind, so `tracked_run` never tracked any run after the first (the CATALOG-RESYNC-NOT-HARVESTED class in one more place) and the counter froze at 1 while runs kept being scheduled. Fixed 2026-09-18 in `2d480a4`: all seven terminal fields of the record are nulled explicitly at start; a harvest whose write did not land no longer sets the Job TTL (a second defect, fixed); sequence rows prove degrade at three, no fourth Job, release on a generation bump, reset on success; review `claude/fix-retention-degraded.review.md` ACCEPT, 10/10 mutants. Live proof on lab-refresh-5 at `d387f87`: the counter half is CLOSED — `consecutiveRunFailures` reaches 3 and no further Job is created (`claude/lab-refresh-5.result.md` §8.1); the condition half is NOT — `EnforcementDegraded` is never written and the counter is not cleared by the generation bump that releases the stop; The condition half: fixed 2026-09-18 in `1ccf868`/`97a2563`/`e308ab3` — the real cause was a merge PATCH of the `status.conditions` ARRAY, which replaced it whole so every write deleted the conditions it did not name (the harvest published `EnforcementDegraded`, the next evaluation wiped it; `Ready`/`Evaluated` were dropped on refusals the same way); `conditions()` now upserts into the observed list, `EnforcementDegraded=True/ConsecutiveFailures` carries the count, exit code, per-point codes and record key, a generation bump clears it and resets the counter through one `adopt_generation()` helper used by all seven `observedGeneration` writers (a degraded policy with an unreadable view releases its budget on a spec edit), a success resets. The review swept every other controller for the conditions-array class: retention was the only instance. Review `claude/fix-retention-degraded.review.md` ACCEPT; 10/10 + 3/4 mutants. Live proof: pending lab-refresh-6 (the bounded-retry row).. | PLAT-16.2 (D3 W9) |
| RET-STALE-PLANREF | **CLOSED (audit 2026-09-23): fixed by the status-sweep class fix.** `RetentionPolicy.status.lastEvaluation.planRef` is written only by `start_run`, so after an evaluation that renders a new plan without starting a run the status still names the previous plan (found by the fix-retention-degraded review, low). Fix (not started): write `planRef` where the evaluation publishes its plan, with a row that a re-evaluation moves it. | PLAT-16.2 (D3 W9) |
| RET-STARTRUN-PATCH-OUTCOME | **CLOSED (audit 2026-09-23): fixed by the status-sweep class fix.** `start_run` discards the outcome of its status patch, so a patch refused after the enforcement Job was created leaves an orphan deletion Job the status never tracks (found by the fix-retention-degraded review, low). Fix (not started): create the Job only after the record write lands, or delete the Job when the write is refused; a row for the refused-patch path. | PLAT-16.2 (D3 W9) |
| PREFLIGHT-APPROVAL-ROSTER | **CLOSED (audit 2026-09-23): fixed by `fix-preflight-approval`; S18 PASS on lab-refresh-6.** The restore preflight's `approval.state` row (`controllers/preflight.rs::approval_verdict`, fed by `super::approval::load_roster` at ~:3441) resolves the approver key and its `notAfter` from `TrustRoster/default` directly, while the Approval controller itself resolves through the trust policy (D3 W10: an isolated `TrustPolicy` with an expiring approver key moved the Approval to `Verified=False, KeyIdExpired` on lab-refresh-4 §11.2). The two can disagree: an approval the controller has already expired under its policy can still read `ready` on the preflight, and the tracker's PLAT-03.2 test "expired approval" cannot be run without replacing the shared, immutable roster (harness-rows-5 §). Fixed 2026-09-19 in `e48da61` (+ `c694fcd`): the preflight's `approval.state` row relays the Approval's own verdict — condition, reason token, message, matched key — and `approval_rows` is no longer given `RosterFacts`, so the roster is unreachable by signature; `KeyIdExpired` → `notReady/ApprovalExpired` (blocking), retired/revoked stay `ApprovalNotVerified`, the plan-hash → expiry → verified ordering unchanged; the Approval is read in the Preflight's own namespace (a cross-namespace substitution is refused, pinned by a row). Review `claude/fix-preflight-approval.review.md` ACCEPT — a stale Approval status cannot durably fool the relay (immutable spec, the binding carries the Approval's resourceVersion, `restore.rs` re-checks `Verified=True` within 30 s); 15/15 mutants. Deliberate regression recorded as APPROVAL-KEY-WINDOW-UNPUBLISHED. Live proof: pending lab-refresh-6 (the real S18 row, harness-rows-6). | PLAT-03.2 (D2 W9 / D3 W10 seam) |
| APPROVAL-KEY-WINDOW-UNPUBLISHED | After PREFLIGHT-APPROVAL-ROSTER's fix the restore preflight's approval rows relay the Approval's own verdict and no longer read the roster, so two D2 §6.3 behaviours are unreachable until the Approval publishes its matched approver key's window: `ApproverKeyExpiresBeforeDeadline` (an approval whose key expires before the restore's deadline warned ahead of time) and the `min(10 m, notAfter)` re-check cap on both approval rows (found by the fix's review, `claude/fix-preflight-approval.review.md`; a deliberate regression, recorded rather than hidden). Fix (not started): the Approval controller publishes the matched key's `notBefore`/`notAfter` on its status (D3 W10's trust projection already has the key), the preflight reads it back, and the two behaviours return with rows.  **Live proof: CLOSED-LIVE 2026-09-23 on lab-refresh-8 (lab at main `f49849d`, controller `sha256:01338ec0…`, runner `sha256:ad705ba2…`)** — d2 S22 and S23 PASS (after harness fix `eb3a2ef`). | PLAT-03.2 / PLAT-19.1 (D2 §6.3 amendment) |
| RET-COUNT-EARLY | **CLOSED (audit 2026-09-23): fixed by the status-sweep class fix; bounded retry PASS on lab-refresh-8 and -9.** On lab-refresh-6 at `af64073`, `RetentionPolicy.status.consecutiveRunFailures` reached 3 and `EnforcementDegraded` fired after only TWO enforcement Jobs had been created (the controller's own log shows two "created the retention Job" lines before the condition; the harness's owner census saw both Jobs with a 600 s TTL untouched, so the earlier TTL explanation does not hold) — the retry budget is spent one run early, so the bound D3 §6.5 promises (three failed runs) is applied after two. Evidence: `claude/lab-refresh-6.result.md` §8.2, `d3-live/lr620260919t0446z/state.json#retention-bounded-retry`. Fixed 2026-09-19 in `6d17baf`/`a7ead32`: the extra count was a second harvest of one Job — `start_run`'s 409 `AlreadyExists` arm fell through and nulled `finishedAt`, resurrecting an already-counted run, and a run already harvested in its slot could be started again; the 409 arm now returns without rewriting `lastEnforcement` and a harvested run is not restarted in its slot. `previously_refused()` is deliberately unchanged: the plan digest returning to an earlier run's id is D3 §6.5's "until the reason clears", costs nothing in-slot and is bounded by the budget (review: no CRD field needed). Review `claude/fix-retention-degraded.review.md` ("fix-retention-count") ACCEPT — two Jobs plus an extra same-slot pass count 2, three distinct Jobs count 3; follow-up I1 (an orphan Job after a crash between create and record) recorded under RET-STARTRUN-PATCH-OUTCOME. Live proof: pending lab-refresh-7 (the bounded-retry row). | PLAT-16.2 (D3 W9) |
| KEYSVIEW-ABSENT-VALID | **CLOSED (audit 2026-09-23): fixed by `d3w12-finish` (PLAT-19.1 record).** `ui/pages/keys.js:67-79` renders every key `valid` when `status.expiredKeyIds` is ABSENT — the exact value PLAT-19.1's acceptance forbids ("the keys view labels unevaluated or stale expiry/trust information as unknown, not valid"); found by the lab-refresh-6 review (F2). The D3 W12 console wave (`claude/d3w12`, in review) rewrites the keys view with a §7.7 `unknown` evaluation column — its pass-2 review must prove this exact case (absent `expiredKeyIds` → `unknown`, never `valid`) with a fixture and a mutant before PLAT-19.1 can be Done. | PLAT-19.1 (D3 W12) |
| RESTORE-ADMITTED-DROPPED | **CLOSED (audit 2026-09-23): fixed by the Restore half closed live; the Backup half by the status-sweep class fix.** `controllers/restore.rs:3997` writes `Admitted=True` on the creating pass (`running_status_patch(.., created, ..)`), but every later pass over the running Job calls it with `admitted=false` (`:4034`) and `diagnostics::apply` (`src/diagnostics.rs:1299`) takes `conditions` from that base — only `RunnerReady` is merged from the stored array — so the merge PATCH replaces `status.conditions` without `Admitted` and the admission condition disappears on the second reconcile of an unchanged running Restore; the comment at `restore.rs:2793-2798` claims the opposite. The conditions-array class the retention sweep (`e308ab3`) fixed, in the restore/backup running path. Found by the harness-rows-7 worker, whose old-archive row therefore asserts "no `Admitted=False` hold and the Job exists" rather than `Admitted=True` (`claude/harness-rows-7.result.md`, R2.1). Fix (in progress, `claude/status-sweep`): the base-plus-apply path carries every stored condition it does not own, for every `diagnostics::apply` caller; a row that `Admitted=True` keeps its original `lastTransitionTime` across a running pass, mutant-pinned. **Restore half CLOSED-LIVE 2026-09-23 on lab-refresh-8** — `trust-old-archive-still-restores` (twice) and the PLAT-10 done-evidence Restore kept the `Admitted` instant to terminal. | PLAT-14.1 (D3 W2 seam) |
| SECRET-VOLUME-MODE-SWEEP | The console's key Secret volume was unreadable by the non-root container until `7d3698d` set the mount mode/`fsGroup`, and no test could see it; the same check is owed as a class sweep over every other Secret volume mounted into a non-root container — `charts/logweir/templates/identity.yaml`, `config/manager/deployment.yaml` and any runner Job that projects a credential (found by the plat17-1-stage7 review, residue 9; low — those mounts work live today, the sweep is consistency and a guard). Fix (not started): one `chart_lint`/`manifest_lint` row asserting mode/`fsGroup` on every Secret volume of a non-root pod, with a planted mutant. | PLAT-20.2 |
| CATALOG-RECEIPTKEY-REDACTED | A catalog point's `receiptKey` reaches the wire as `"[redacted].receipt.json"`: `check_contract.rs:2604` caps a free path component at `FREE_COMPONENT_MAX = 24` and the run id is a 26-character ULID, so `catalog_sync.rs:1404`'s redaction rewrites the whole run component of a key the console needs as a plan binding (found by the d3w12 finisher, pinned to the line by its review `claude/d3w12-finish.review.md` M-3). Degrades PLAT-15.1's point row and would block PLAT-15.2's wizard step; the console renders the redacted key as what it is rather than working around it. Fix (not started, controller/runner lane): treat a ULID-shaped run component as identity, not free text — either raise the free-component budget to cover a ULID with a row that a 26-character ULID survives and a 27-character free component is still redacted, or exempt the run-id position of the receipt key by shape; a mutant that redacts the ULID again.  **Live proof: CLOSED-LIVE 2026-09-23 on lab-refresh-8 (lab at main `f49849d`, controller `sha256:01338ec0…`, runner `sha256:ad705ba2…`)** — `catalog-reconstruction-after-cr-loss`: every `receiptKey` complete, the ULID clause True. | PLAT-15.1 (D3 W3/W8) |
| PROTECTION-SECRETKEYS-UNPROTECTED | **CLOSED (audit 2026-09-23): fixed by `dd50159` (fix-protection); rows PASS on lab-refresh-8 and -9.** With a `SecretKeys` `evidenceRead` grant the controller never fetches the receipt itself (D2's grant model — the pod reads it), so the Backup publishes `evidence.verification: NotAttempted` and no `capture`/`receiptSha256`; the `ProtectionPolicy` evaluation then degrades every such point to `health: Unprotected` (`protection.rs:1071`) although D3 §3.2 reserves `Unknown` for a point whose verification was not attempted, joins on a fact that is absent (`:992`) instead of the catalog entry's `backupId`, leaves `recoveryPointAt` unfilled, and `requireVerifiedEvidence` (`:976`) treats `NotAttempted` as fatal — so a legitimately protected schedule is reported unprotected and its staleness alert pages on it, and PLAT-14.2's recovery-notification and unavailable-archive rows FAIL on this build (found by the d3-rows worker; mechanism and the third clause pinned by `claude/d3-rows.review.md`). Every missing fact is already on the catalog entry the policy reads (`backupId`, `recoveryPointAtMs`, `verification`). Fix (not started, controller lane): `:1071` → `Unknown` for a `NotAttempted` point; `:992` join on `backup_id`; backfill `recovery_point_at` from the catalog entry; `:976` defer the verification requirement to the catalog's verdict; rows for each and the two 14.2 live rows re-run at the batch refresh. | PLAT-14.2 (D3 W6) |
| RET-EVIDENCE-GRANT-IS-ARCHIVEREAD | **CLOSED (audit 2026-09-23): fixed by `6e8b1f8`; U6 9/9 on lab-refresh-9.** `controllers/retention_policy.rs:1188` resolves the destination under `DestinationRole::ArchiveRead` (for the location) and `:2073` projects that same `resolved.grant` as the enforcement Job's `LOGWEIR_EVIDENCE_AWS_*` under a comment that says `evidenceWrite`; D3 §6.5 and `docs/kubernetes.md` §7f promise `evidenceWrite`. Live (plat08-u6, `claude/artifacts/d2-live/u620260921t140000z/u6/retention-enforcer.json#evidenceCredentialDefect`): on a destination separating the four principals the first intent tombstone is refused `403 AccessDenied`, `state=Kept code=TombstoneRefused`, `deleted=0 failed=1` — an installation whose `archiveRead` is the read-only grant the docs recommend cannot enforce retention at all, and one whose `archiveRead` can write under `logweir/*` attributes its deletions to a credential the design says must not write there. Fix (not started, controller lane): resolve `evidenceWrite` for the tombstone/record credential (a second resolution or a two-role resolve), project it as `LOGWEIR_EVIDENCE_AWS_*`, keep `archiveRead` for the location and the deletes' own grant per D3 §7f; a row that the enforcement Job's evidence variables name the `evidenceWrite` Secret and a mutant that projects `archiveRead` again; the U6 retention row re-run at the batch refresh. | PLAT-16.2 (D3 W9) |
| PREFLIGHT-EVIDENCEWRITABLE-WRONG-PRINCIPAL | **CLOSED-LIVE (2026-09-24, lab-refresh-10: RP-L1…L15).** **FIXED (2026-09-23): `claude/readiness-principal` merged, with the `evidenceRead` class sweep (D2 amendment of this date); live rows RP-L* at lab-refresh-10.** `crates/logweir/src/check/store.rs::open_evidence_write`, driven by `preflight.rs`'s one-credential check plan, answers `destination.evidenceWritable` with the ARCHIVE credential, so on a destination that separates `evidenceWrite` the row's sentence is about the wrong principal (found by plat08-u6's U6 measurement; awaiting the review's confirmation of severity). Fix (not started): the readiness plan carries the `evidenceWrite` grant for that row and the runner opens the evidence store with it; a row per principal. | PLAT-03.2 / PLAT-08.2 (D2 W5/W9) |
| DESTINATIONACCESS-IGNORES-WRITEPROBE | **CLOSED-LIVE (2026-09-24, lab-refresh-10: RP rows).** **FIXED (2026-09-23): decided a defect; `claude/readiness-principal` merged (D2 amendment of this date); archive write stays execution-only and never green.** `controllers/preflight.rs:3692` derives `writeProbe: CreateOnlyMarker` only for a Backup/Restore readiness plan, so a `DestinationAccess` check requesting `EvidenceWrite` always answers `WriteNotProbed` (found by plat08-u6; the comment suggests it may be intended — the review decides whether this is a defect or a documented limit; if a limit, D2 §6.3's row must say so). | PLAT-08.1 (D2 W9) |
| RET-RECORD-WITHOUT-JOB-COUNTS | A controller that dies between `start_run`'s record write and its `jobs.create` leaves a record with no Job; the next pass harvests it as a failed run and spends a retry-budget slot (found by the status-sweep worker after its MED-1 fix, which withdraws the record on a refused create and covers every non-crash case; low). Fix (not started): carry the Job the record is waiting for on `status.lastEnforcement` (a CRD field) and give `tracked_run`'s absent-Job arm a "the record names no Job ⇒ the run never started, supersede it" branch, with a row. Also owed: `crds/retention_policy.rs:373`'s `planRef` description is imprecise in `Report` mode. | PLAT-16.2 (D3 W9) |
| REHEARSAL-NO-STANDING-SIGNER | Nothing in the product mints a signed standing rehearsal authorization: `logweir approve` (`crates/logweir/src/cli.rs:385-418` → `approve.rs:106`) signs only `PAYLOAD_TYPE_APPROVAL`, while the `Approval` controller requires `PAYLOAD_TYPE_STANDING_AUTHORIZATION` for a `RehearsalSchedule` referent (`controllers/approval.rs:483-499`); the only producers are Rust test fixtures, so no operator can use PLAT-14.3 at all and D3 §15 L6 cannot run (found by the plat14-3b review, `claude/plat14-3b.review.md` §4, P0). Fixed 2026-09-22 in `claude/plat14-3b` (`..84f543b`): `logweir drill approve --standing` mints the D3 W5 §R1.3 document (kind, `subjectRef` with the schedule's UID, usage, an explicit scope incl. `deadlineSeconds`, a window refused by name above 90 days) as a DSSE envelope + sidecar under `PAYLOAD_TYPE_STANDING_AUTHORIZATION`, through the one signing path (`check-one-signer` green); a subprocess row over the built binary decodes the envelope and asserts every scope field; `--standing` with a `Restore` subject is refused; the per-run signer's `--approver`/`--ticket` stay required (`required_unless_present = "standing"`). Focused signer review `claude/plat14-3b.review-2.md` ACCEPT-WITH-FIXES, all applied. Live proof: the L6 row (per `claude/plat14-3b.review.md` §4's ten steps) is still unwritten and needs a lab build carrying this commit — lab-refresh-8. | PLAT-14.3 (D3 W7/14.3b) |
| REHEARSAL-APPROVALREF-BY-NAME | `RestoreAuthorization.approvalRef` is a `LocalRef` carrying only `name` (`crds/restore.rs:348-355`, `crds/mod.rs:126-129`), so the standing `Approval` is resolved by name (`restore.rs:3798-3814`) and a same-named replacement is refused only by the chain the review verified — `Verified=True`, the `RehearsalSchedule` subject with `status.verifiedSubjectRef.uid`, a trusted approver key in window, `plan_within_scope`, and after the first bundle write the immutable `logweir.dev/approval-uid` annotation (`ApprovalBundleConflict`); the only window is between the schedule's authorize read and the first bundle write (found by the plat14-3b review, MEDIUM-3; low). **Decided 2026-09-21:** the chain is adequate for this release; `claude/plat14-3b` (`..84f543b`) additionally stamps the standing Approval's UID on the child Restore and `admit_standing` requires it (a same-named replacement is refused by name, mutant-pinned), so the only remaining gap is that the pin lives in mutable metadata beside a CEL-immutable `spec`; the follow-up adding an optional `approvalRef.uid` to the CRD (additive) stays queued. | PLAT-14.3 (D3 W0/W7) |
| DESTINATION-CA-DIGEST-STRANDED | `controllers/backup_destination.rs:172` builds its status merge patch with `ca_bundle_sha256` as an `Option` that is OMITTED when `None`, so a destination whose CA bundle is removed keeps the previous bundle's digest on `status` (the merge-patch-omission class the retention and approval controllers already fixed with explicit `null`s); found by the fix-approval-window review's sweep routing of seven whole-status patch sites — the other six are correct (unconditional sets, explicit nulls, or merge-preserve by design). Low: the digest is informational. Fix (not started): send an explicit `null` for a cleared CA digest, with a row that Some→None clears it and a mutant that omits it. | PLAT-08.1 (D2 W7) |
| BACKUP-PROJECTION-NO-DESTINATION | **CLOSED (audit 2026-09-23): fixed by `c7ea1f9` via `454cd6b`; PLAT-08.2 rows on lab-refresh-9.** `crates/logweir-api/src/projection.rs:302-351` (`backup`) publishes `archive` but neither `destinationRef` nor `locationDigest`, although `Backup.spec.destinationRef` exists (`crds/backup.rs:373`) and the Schedule projection publishes it (`projection.rs:160`); the restore wizard therefore has nothing to name and sends `restore.legacySourceArchive` (`ui/pages/restore-wizard.js:~3125`, `SOURCE_DESTINATION_NOT_PUBLISHED`), `routes/preflights.rs:866-872` accepts it, and `controllers/preflight.rs:4255-4278` fails the whole `Preflight` terminally with `ArchiveUrlUnreadable` — so every console-initiated restore readiness check on this build ends `Failed` and never reaches `target.mappedTopics`, leaving PLAT-11.2's submit gate inert on the ordinary journey (found by the plat11-2 review F10, reproduced by its second pass on `8ce3ffa`; medium — the CLI/API path with an explicit destination is unaffected). Fix (not started, API lane, after `claude/plat11-2` merges): the Backup projection publishes `destinationRef` and the frozen `locationDigest` from `status.destination`; the wizard names them (one line, `SOURCE_DESTINATION_NOT_PUBLISHED` retired); and independently the preflight route refuses `legacySourceArchive` up front with a 422 naming `destinationRef` — a refusal per request is better operator feedback than a terminal object per click; rows for both, a mutant that drops the field, and the plat11-2 journey's step 5 re-run to a verified scorecard. | PLAT-08.2 / PLAT-03.2 (D2 W12/W13) |
| REHEARSAL-STANDING-APPROVAL-NEVER-VERIFIES | The `Approval` controller's checks 7/8 (`controllers/approval.rs:420-429`, `:633-654`) parse `spec.approvalBytes` as the per-run `ApprovalDocument` with every field defaulted and compare `doc.plan_hash` and `doc.subject_kind`, while the standing document `logweir drill approve --standing` mints (`execution_contract.rs:463-471`: `formatVersion`, `kind`, `subjectRef`, `scope` with `templateDigest`, `issuedAt`, `expiresAt`) carries neither field — so every standing `Approval` for a `RehearsalSchedule` ends `Verified=False/PlanHashMismatch` ("the approval names plan hash  but…"), and no rehearsal can ever be authorised on this build; found live by the L6 row on the lab at `1a9aca6` (`claude/harness-rows-9.result.md` §F1, `claude/artifacts/d3-live/20260922t0449z/rehearsal/01-approval.json`). The signer's own end-to-end row exercised the library admission, not the controller's verification. Fixed on main (`0237e9c..95c2279`, `claude/fix-standing-verify`, 2026-09-22): a `RehearsalSchedule` referent's Approval is parsed as the standing document and checked for the canonical template digest, full subject identity incl. UID, an active window of at most 90 days and a scratch-only scope, under `TemplateDigestMismatch`/`SubjectMismatch`/`WindowInvalid`/`ScopeInvalid`/`StandingDocumentInvalid`; the Restore path's per-run checks are unchanged; `GovernedApproval` only at all five boundaries (`ConsoleConfirmation` fails closed until PLAT-19.2 carries the policy mode). Three review rounds (final Rust+security review `claude/fix-standing-verify.review-final.md`, its MEDIUM closed in `dffe118`), 27 mutants killed (`claude/fix-standing-verify-mutants.log`), `cargo test -p weirkeeper` 1,333/0. **Live proof: pending lab-refresh-8** (L6 steps 1–10 plus rows 11–12, `claude/fix-standing-verify.result.md`).  **Live proof: CLOSED-LIVE 2026-09-23 on lab-refresh-8 (lab at main `f49849d`, controller `sha256:01338ec0…`, runner `sha256:ad705ba2…`)** — `rehearsal-1-setup-standing-approval-verifies`: Verified=True, matchedKeyId recorded, `verifiedSubjectRef.uid` = the schedule's uid (`claude/lab-refresh-8.result.md` §6). | PLAT-14.3 (D3 W7/14.3b) |
| REHEARSAL-NO-CANDIDATE-WITHOUT-DIGEST | `rehearsal_schedule.rs:1481` (`candidate_from_backup`) requires `status.evidence.receiptSha256`, which a destination-backed run whose verification is `NotAttempted` never carries (`backup.rs:~4088`: "fetched nothing, so `receipt_sha256` is `None` BY CONSTRUCTION"), and a catalog entry records no topic list (`:1562-1565`), so a `RehearsalSchedule` with a non-empty `spec.point.topics` can never select a point and skips every slot `NoQualifyingPoint` — while the signer refuses an empty `scope.topics`, closing the only escape; found live by the L6 row (`claude/harness-rows-9.result.md` §F2). **Decided 2026-09-22:** write `receiptSha256` on a `NotAttempted` run from the digest the runner reported — a `capture` fact per D3 §2.5, not a verification claim; the verification axis and the protection health derivation (`Evidence::was_reached()`) unchanged, mutant-pinned; and the candidate carries the Backup's frozen topic list. Fixed on main (`0237e9c..95c2279`, 2026-09-22): `logweir backup run` emits the canonical `sha256:` receipt digest once before the final key pair, the controller publishes it as a capture claim on a `NotAttempted` run (verification and protection health unchanged), the candidate keeps `Backup.spec.topics`, and a catalog row enriches it only on full-digest equality and never overrules a reached refusal. **Live proof: pending lab-refresh-8** (needs the rebuilt runner image).  **Live proof: CLOSED-LIVE 2026-09-23 on lab-refresh-8 (lab at main `f49849d`, controller `sha256:01338ec0…`, runner `sha256:ad705ba2…`)** — the controller selected point `lwp1-0193fe6a…` and rendered a plan carrying `source.point.receipt_sha256` (`rehearsal/02-restore.json`). | PLAT-14.3 (D3 W7) |
| RETENTION-PLAN-IGNORES-REFUSED-VERDICT | `crates/weirkeeper/src/retention_plan.rs:609` counts a point as usable from its catalog row without reading the `Backup`'s own reached verification verdict, so a point the controller refused (`Invalid`/`Untrusted`) under a stale catalog row (served until `viewExpiresAt`) can count toward `keepLast`, letting an older good point become a deletion candidate. Same rule as the rehearsal/protection joins (`dffe118`, `95c2279`): the catalog decides only where the controller could not look. Found by the fix-standing-verify class sweep 2026-09-22; not yet fixed.  **lab-refresh-8 (2026-09-23): NOT closed live** — row `retention-refused-newest-point-is-skipped-unreadable` NOT-REACHED behind CATALOG-TRUST-ROSTER-ONLY (the refused-point fixture's policy-signed point is NotAttempted in the view). Code on main (`bc1e161`, `e0f73f6`).  **CLOSED-LIVE 2026-09-23 on lab-refresh-9 (`306cebf`).** | PLAT-16.2 (D3 W9) |
| CATALOG-LIST-IGNORES-REFUSED-VERDICT | `crates/logweir-api/src/routes/catalogs.rs:996` has the same exposure in the console's catalog listing: a stale selectable row is shown for a point whose `Backup` verdict is a reached refusal. Found by the same sweep 2026-09-22; not yet fixed.  **lab-refresh-8 (2026-09-23): NOT closed live** — row `catalog-points-refused-point-is-not-selectable` NOT-REACHED behind CATALOG-TRUST-ROSTER-ONLY. Code on main (`4840c6e`, `e0f73f6`).  **CLOSED-LIVE 2026-09-23 on lab-refresh-9 (`306cebf`).** | PLAT-15.1 (D3 W11) |
| CONSOLE-RESTORE-IGNORES-CATALOG-WINDOW | `ui/pages/restore-wizard.js:423` `isRecoveryPoint` (and PLAT-10's schedule detail, which follows it) decides restorability only from the controller-written `Backup.status.windowCovered`. A destination-backed run whose own verification is `NotAttempted` (no evidence grant, or `evidenceRead: ControllerIdentity` on a location the administrator did not allowlist — `ControllerIdentityNotAllowlisted`) never gets a window, so the console offers no Restore even when the durable catalog lists the point `Available/Verified` with its window. Found live by the PLAT-10 finisher 2026-09-22 (`claude/plat10.result.md` §Final, Class sweep owed). Design question for PLAT-15.2 (catalog-backed restore): whether a catalog-verified point may be offered from the catalog's window; the controller-verdict precedence rule (`dffe118`) must hold. Not yet fixed. Fixed on `claude/plat15-2` (accepted after two review rounds, merging in `claude/integration-2`): a windowless run is offered a catalog-window restore only when its own verdict is absent/`NotAttempted`, exactly one catalog row matches its receipt and that row is offerable, and the catalog reads the run's own destination; the Restore binds `source.point` and the runner re-verifies the receipt digest AND signature before any data moves. | PLAT-15.2 / PLAT-11.1 |
| REHEARSAL-SKIP-DEFERS-SLOT | `crates/weirkeeper/src/controllers/rehearsal_schedule.rs:1745-1752`: a skip writes `lastSkipped.slot = slot_name(now)` (the evaluation instant, not the due slot it refused) and does not advance `lastScheduledSlot`, so a slot skipped `ConcurrencyBlocked` fires LATE the moment the blocker finishes (within `startingDeadlineSeconds`). D3 §5 (`lastSkipped: {slot, reason}`, line 604) treats `ConcurrencyBlocked` as a skip of that slot. Found by the harness-rows-9 fix round 2026-09-22 (`claude/harness-rows-9.result.md` Class sweep owed). Not yet fixed.  **lab-refresh-8 (2026-09-23): NOT closed live** — row 7b NOT-REACHED behind REHEARSAL-PLAN-AUTH-PLAINTEXT; partial: the refused arm's skip named the due slot `20260922-233000`. Code on main (`b58341d`).  **CLOSED-LIVE 2026-09-23 on lab-refresh-9 (`306cebf`).** | PLAT-14.3 (D3 W7) |
| EVIDENCE-FETCH-JOB-UNBUILT | D2 §1.5 and §3.9 make the evidence-fetch check Job the DEFAULT evidence path (`SecretKeys`/`WorkloadIdentity` `evidenceRead`, including `ArchiveReadGrant`), with `ControllerIdentity` opt-in and administrator-allowlisted. The contract (`logweir-core::check_contract::EvidenceFetchRequest`, discriminator `ev`) and the runner kind (`crates/logweir/src/check/kinds/evidence.rs`) exist, but the controller never creates the Job: `controllers/backup.rs:1298-1311` answers `NotAttempted` ("this build does not create that Job"). Consequence, found live by the PLAT-10 finisher 2026-09-22 (`claude/plat10.result.md` §Final 2, artifact `claude/artifacts/plat10-ui/lw-p10-20260922t174132z/live.json`): on an install without a `controllerIdentityLocations` allowlist (the chart default is `[]`) no destination-backed run ever gets `windowCovered`, so the console offers no Restore for any scheduled backup — PLAT-10.2's per-point Restore and older-backup navigation and the create→backup→detail→restore journey are BLOCKED. D2-EVIDENCE-NOTATTEMPTED-UNWRITTEN's fix made that verdict honest; it did not fetch. `docs/install.md` §3 presents `ArchiveReadGrant` as working. Fixed on main (`4cd3e55..4f2c93e`, `claude/evidence-fetch`, 2026-09-22): the controller creates `lwc-ev-<20 hex of sha256(uid:attempt)>` owned by the Backup/Restore with only the `evidenceRead` grant (no signing key, no write grant, no token automount), shows `Pending` with `evidence.observation` while it runs, and on relay checks the runner-reported digest, the `backup_id`/`run_id` binding, the controller-side size caps and DSSE under resolved trust through the same `verify_fetched` path `ControllerIdentity` uses; failures are `NotAttempted` naming the cause with bounded retries (+1/+5/+15 min, 4 attempts); no RBAC change (the controller still holds no verb on Secrets). Tier-A review ACCEPT (`claude/evidence-fetch.review.md`, MEDIUM-1 cap enforcement and four LOWs closed in the fix round), 27 mutants killed, gate `scripts/ci-check.sh` exit 0 (3,330 tests). **Live proof: pending lab-refresh-8** (six rows in `claude/evidence-fetch.result.md` §5).  **Live proof: CLOSED-LIVE 2026-09-23 on lab-refresh-8 (lab at main `f49849d`, controller `sha256:01338ec0…`, runner `sha256:ad705ba2…`)** — d2 EVF-1, EVF-2, EVF-1.ttl, EVF-4, EVF-5 (controller scaled to 0 mid-fetch: one Job per attempt), EVF-6 and S1.statusVerification PASS; PLAT-10's per-point Restore now offered from the controller's own window. | PLAT-08.1 (D2 W10) → blocks PLAT-10.2, PLAT-11.1 live |
| REHEARSAL-PLAN-AUTH-PLAINTEXT | `crates/weirkeeper/src/rehearsal.rs:808` `render_plan` hard-coded the target `auth: AuthSpec::default()` (plaintext), so a rehearsal Restore against a SCRAM/TLS target is refused `ConnectionPlanMismatch`; L6 steps 2–9 and 7b cannot run. Found live by lab-refresh-8 (`rehearsal/02-restore.json`). Fixed on `claude/rehearsal-catalog-trust` (`99fda80`: the plan's bootstrap and auth come from `connection::resolve(cluster, RestoreTarget)`, the call admission makes; a refused connection is a `TargetUnavailable` skip; standing Approvals unaffected). Tier-A review and live proof pending. **Landed on main `2cb04c7`** after the review fix round (Tier-A ACCEPT; gate 3,570/0). Live proof owed at lab-refresh-9.  **CLOSED-LIVE 2026-09-23 on lab-refresh-9 (`306cebf`).** | PLAT-14.3 (D3 W7) |
| CATALOG-TRUST-ROSTER-ONLY | `crates/weirkeeper/src/controllers/recovery_catalog.rs:342-353` `trust_view` read only `TrustRoster/default` (the D3 W1/W10 seam never replaced), so the catalog ignored `TrustPolicy`: a policy-signed point is `NotAttempted` in the view and a policy-retired/revoked key is not reflected. Found live by lab-refresh-8 (`refused-point/00-fixture.json`). Fixed on `claude/rehearsal-catalog-trust` (`441bbc8`: `trust::resolve` per namespace, keys judged as `logweir_core::trust::decide` does, roster fallback unchanged, `TrustPolicyConflict`), with a class sweep of `preflight.rs` `signer.rostered` in progress. Review and live proof pending. **Landed on main `2cb04c7`:** the catalog judges policy keys with `logweir_core::trust::decide` itself (a 100-case parity grid), and the class sweep moved Preflight `signer.rostered` and the restore allowlist to `trust::resolve`, with `TrustUnknown` when policy trust is unreadable. Live proof owed at lab-refresh-9.  **CLOSED-LIVE 2026-09-23 on lab-refresh-9 (`306cebf`).** | PLAT-15.1 / PLAT-19.1 (D3 W1/W10) |
| CONSOLE-APPROVAL-VERIFIED-SUBJECT-UNMAPPED | `ui/client.js:854-860` projected `verifiedSubject` while `ui/pages/approvals.js:295,404` read `verifiedSubjectRef`, so a controller-verified Approval always showed "awaiting verification" in console mode (and a leftover Approval from a deleted same-name Restore was never flagged). Found live by lab-refresh-8 (PLAT-10 `failed[0]`, the owed PLAT-12.2 verified-approval route). Fixed on `claude/plat19-2` (accepted, merging in `claude/integration-2`); live re-run owed. Landed on main `ac00819`. Live re-run owed at lab-refresh-9.  **CLOSED-LIVE 2026-09-23 on lab-refresh-9 (`306cebf`).** | PLAT-12.2 |
| RESTORE-COMPLETION-UNWRITTEN | `Restore.status.completion` (recordsRestored, recordsSampled, newTopics) is declared by the CRD and never written by `controllers/restore.rs`; measured live by lab-refresh-8 (PLAT-10 `failed[1]`, a row the harness labels PLAT-10.2 but whose requirement is PLAT-14.1's durable completion reporting). Not yet fixed. Fixed on `claude/ctl-batch-1` (Tier-A ACCEPT). Decision: `status.completion` is written ONLY from a scorecard whose verification is `Valid`, never from an unverified one (fix round in progress). `recordsRestored` is the sampled-window count (CRD "From `sample.records_restored`", D3 §3.5), so the PLAT-10 harness clause is being corrected. **Landed on main `fa3384e`.** Written only from a Valid scorecard. Live proof owed at lab-refresh-9.  **CLOSED-LIVE 2026-09-23 on lab-refresh-9 (`306cebf`).** | PLAT-14.1 |
| SCHEDULE-FIRES-SLOT-BEFORE-CREATION | `controllers/backup_schedule.rs` `MISSED_SLOT_HORIZON` catch-up is not bounded by the schedule's creation time, so a schedule created at 00:53:11Z fired its 00:30:00Z slot (lab-refresh-8 merged-tree run `lr8merged-20260923t0052z`); `scripts/plat10-ui-e2e.mjs:977-979` "empty history" is time-of-day sensitive until decided. PRODUCT DECISION owed (D1 §4 missed-slot semantics): a slot due before `metadata.creationTimestamp` should not fire. **Decided 2026-09-23:** a slot due before `metadata.creationTimestamp` never fires (CronJob semantics); D1 §4.7 is amended with row 5a. Fixed on `claude/ctl-batch-1` for BackupSchedule and RehearsalSchedule (Tier-A ACCEPT; fix round and rebase in progress). **Landed on main `fa3384e`**, for both BackupSchedule and RehearsalSchedule. Live proof owed at lab-refresh-9.  **CLOSED-LIVE 2026-09-23 on lab-refresh-9 (`306cebf`).** | PLAT-04.2 (D1) |
| PREFLIGHT-TRUSTROSTER-STALE | Since `4c4d2ed` (2026-09-17) the controller records `TrustRoster/default` as a Preflight referent whenever it exists, but the API's preflights route cannot read that kind, so every console readiness result is served stale and the wizard refuses it (pinned as deliberate at `crates/logweir-api/tests/preflights.rs:1188`). Confirmed by the PLAT-19.2 review (item 7). Fix owed after `claude/integration-2`: a narrow API read of `trustrosters/default` beside PLAT-17.2's `logweir-api-trustpolicies` grant, or not re-reading cluster trust referents. **Fixed on main `e109a77` (`claude/trust-stale`, Tier-A review ACCEPT, gate 3,582/0):** the API compares `TrustRoster/default` and the governing `TrustPolicy` by uid and generation; a refused or failed read stays unverifiable, so the result stays stale; a new chart ClusterRole `<release>-api-trustroster` grants only `get` on `trustrosters` with `resourceNames: [default]`. Live on the lab at `f49849d`: a readiness verdict served fresh, and the wizard created the draft Restore; with the roster binding removed as a control, the same verdict went stale and the wizard refused. The TrustPolicy half is proven only by tests until lab-refresh-9.  **CLOSED-LIVE 2026-09-23 on lab-refresh-9 (`306cebf`).** | PLAT-03.2 / PLAT-17.2 |
| LAB-SEED-TOPIC-RETENTION | `scripts/test-k8s-scram.py:309-316` creates the shared lab's seed topics with the broker's default 7-day retention, so `orders`/`payments` emptied a week after the lab build (2026-09-22 ~23:29Z) and every Backup of them captured nothing. Lab fixture re-seeded identically with `retention.ms=-1` by lab-refresh-8 (owner-authorized); the script fix is owed. Script fixed on `claude/ctl-batch-1` (seed topics created with `retention.ms=-1`, plus an offline check). Script fix landed on main `fa3384e`; the offline check runs inside `just lint`. | lab fixture |
| DRAFT-PREFLIGHT-NEVER-READY | The wizard's `readinessRefusal` submitted only on overall `ready`, but a draft restore's blocking `approval.state` is always `Skipped/SubjectNotCreated` (`controllers/preflight.rs` `approval_rows`), which `check_contract::aggregate` makes `unknown` by design — so after any readiness check no console restore could be created, on any cluster. Found live by the PLAT-08.2 worker. Fixed on `claude/plat19-2` in the console (the aggregate unchanged): a draft submits only when every blocking check is `ready` except `approval.state` in exactly that shape; three widening mutants killed; merging in `claude/integration-2`.  **CLOSED-LIVE 2026-09-23 on lab-refresh-9 (`306cebf`).** | PLAT-12.1 / PLAT-11.2 |
| WIZARD-DEFAULT-PIT-EXCLUSIVE | The wizard defaulted the point in time to the covered window's end, which the runner treats as exclusive and refuses. Found live by the PLAT-08.2 worker. Fixed on `claude/plat15-2`: points accept `[fromMs + 1 ms, toMs − 1 ms]` and default to its end (the start is exclusive too: `restore.rs:435`); proved live on a verified Backup.  **CLOSED-LIVE 2026-09-23 on lab-refresh-9 (`306cebf`).** | PLAT-11.1 |
| TRUST-REFERENT-RESOLUTION-UNRECORDED | The controller records only the governing `TrustPolicy` (and `TrustRoster/default`) as Preflight referents. A new second policy claiming the namespace, or a new default policy, does not make an existing readiness verdict stale. This affects only the console gate: verdicts expire after 15 minutes, and the Restore and Approval controllers re-resolve trust before anything runs. Follow-up: record a digest of how the namespace's trust was resolved. Found by the trust-stale and rehearsal-catalog-trust reviews (LOW-3/LOW-4). | PLAT-19.1 / PLAT-03.2 |
| SHARED-SET-RETENTION | A re-created runner Job writes a second signed receipt over ONE backup set, and `keepLast 1` then planned deletion of the older point, which is the kept point's own set. This is data loss. Found live by harness-rows-11 (`claude/harness-rows-11.result.md`). **Fixed on main `b57753b` (`claude/ctl-batch-2`)**: points sharing a backup set, manifest key or segment key (transitively) are grouped; a kept, skipped or location-less member protects every candidate in the group (`SharedSegment`); the deletion ceiling takes groups whole; the plan writer refuses a line whose set a kept point names; and a fully due set is removed by one plan line naming the others in `co_point_ids`. Tier-A review with two fix rounds. `sharedSegments` still honestly reads `NotEnforced`, because a manifest pointing into another set's directory is not visible. Live proof owed at lab-refresh-9 (`shared-set`).  **CLOSED-LIVE 2026-09-23 on lab-refresh-9 (`306cebf`).** | PLAT-16.2 (D3 W9) |
| OBJECT-LOCK-DELETE-MARKER | The enforcer deleted by key without a version id. On a versioned or Object Lock bucket, S3 then writes a delete marker, and the point was recorded `Deleted` while its data survived. A provider refusal could never occur. Found live by harness-rows-11 on the lab MinIO. **Fixed on main `b57753b`**: `object_store` 0.14 cannot delete by version, so the enforcer refuses a versioned bucket with `VersionedBucket` and deletes nothing. It detects versioning three ways: a versioned intent tombstone, a per-key HEAD, and a post-delete check object that catches versioning enabled mid-point. The delete credential now needs `s3:GetObject`. A policy degraded only on a bucket-level refusal re-probes after 24 h. Two review rounds (H1 pre-versioning object, RH1 enabled mid-point) closed. Live proof owed at lab-refresh-9 (`object-lock` flipped: nothing deleted).  **CLOSED-LIVE 2026-09-23 on lab-refresh-9 (`306cebf`).** | PLAT-16.2 (D3 §16) |
| RET-VERSIONED-SUSPENDED-REWRITE | **Accepted residual (orchestrator decision 2026-09-23).** On a bucket whose versioning was once Enabled and later Suspended, where the same keys are rewritten (re-run backups do rewrite them), a delete removes only the current null version and the point is recorded `Deleted` while Enabled-era versions survive. Nothing the enforcer can call with `object_store` 0.14 can see this. `Deleted` now means the current object at each key was removed; noncurrent versions are the bucket's lifecycle responsibility. This is documented in `docs/kubernetes.md` §7f and `docs/install.md`, with the operator rule (do not change versioning mid-run; do not enforce on a bucket that was ever Enabled then Suspended; use `mode: ExternalLifecycle` or a noncurrent-version lifecycle rule). Fix owed: a version-aware store client (ListObjectVersions / GetBucketVersioning). | PLAT-16.2 (follow-up) |
| WARNING-DIAGNOSTICS-NOEXITCODE | A projected-ConfigMap mount failure or an unschedulable pod ended with the generic `NoExitCode`, because `recorded_terminal_state` kept only Error-class diagnostics. Found live by harness-rows-11. **Fixed on main `b57753b`**: a warning-class diagnostic is the terminal reason when the runner never started and the warning was still seen within 180 s of the Job's end. Error-class precedence and the recorded started state are kept, including a crash-looping runner's start from `lastState`. Live proof owed at lab-refresh-9 (`operation-states`).  **CLOSED-LIVE 2026-09-23 on lab-refresh-9 (`306cebf`).** | PLAT-14.1 |
| TRUST-STATE-RBR-VERIFIED | The API mapped `Valid` + `RecordedBeforeRevocation` to `trust.state: verified`, which D3 §7.4 forbids. **Fixed on main `178cc1c` (`claude/api-trust-state`)**: that pairing and any other non-pass basis now read `untrusted`, a recorded `Untrusted` reads `untrusted`, and `NotAttempted` + `Unverified` reads `notAttempted`. Tier-A ACCEPT. The console detail agrees in both modes. | PLAT-19.1 / PLAT-17.1 |
| TRUST-VALID-BASIS-CLASS | Eight controller, API and runner sites treated `verification.result: Valid` as a pass without reading the trust basis. None misfired on current writes, but lab objects from `03c2a85..e247cf9` carry `Valid` + `Unverified`. **Fixed on main `7b0277b` (`claude/trust-basis-class`)**: one rule (`weirkeeper::verification::ValidBasis`, and `Verdict::may_render_green` in `logweir_core` for the runner) — pass only with no trust block, `trust: null`, or a `Current`/`Historical` basis — used by protection, the catalog, rehearsal selection, readiness, backup records gating, the API `trust_state` and the console legacy mode. Tier-A review ACCEPT after a docs round. | PLAT-19.1 |
| CONSOLE-HISTORY-VALID-SHOWN-UNVERIFIED | Since `43576bf` (2026-09-16), console-mode list rows dropped the API's `verificationState`/`verifiedSuccess`, so every Backup read "no verification was recorded", even when Valid. **Fixed on main `9d638cc` (PLAT-18.2 fix round)**: list rows carry the API's summary, and the fields they lack are declared absent. | PLAT-11.1 / PLAT-14.1 |
| SCORECARD-FACTS-UNVERIFIED-SHOWN | A Restore's `outcome`, `integrity`, `objectives.met` and `measured` are copied from the run's scorecard whatever its verdict (`completion` is gated to Valid). **Console side fixed on main `9d638cc`**: they are captioned "unverified scorecard claim" unless the verdict is green, on the detail, history and operation views. | PLAT-14.1 / PLAT-14.3 |
| CONSOLE-DETAIL-TRUST-BASIS-DROPPED | The console detail and operation views dropped the verdict's trust basis, so `Valid` + `RecordedBeforeRevocation` rendered green. **Fixed on main `8485665` (`claude/ui-followup-1`)**. | PLAT-19.1 |
| REHEARSAL-PASS-RECORDED-AS-FAILED | **CLOSED-LIVE (2026-09-24, lab-refresh-10 at `b426096`: L6 step 5 PASS twice, `lastSucceeded` after the verdict, `RehearsalHealthy=True`).** `crates/weirkeeper/src/controllers/rehearsal_schedule.rs:1777-1786` `observe` decides pass/fail the instant the Restore turns terminal, but since evidence fetch the `outcome` and verdict arrive with the later evidence-fetch pass, so a passing destination-backed rehearsal is recorded failed. Found live by lab-refresh-9 (L6 step 5 ×3). In progress on `claude/rehearsal-fix`. | PLAT-14.3 |
| FAILED-DRILL-EVIDENCE-UNPUBLISHED | **CLOSED-LIVE (2026-09-24, lab-refresh-10: exit 2, `fail-integrity`, three evidence keys, verification `Valid`, schedule `lastFailed`).** `crates/logweir/src/drill/mod.rs:1333-1336,1394-1400`: an exit-2 (failed verification) drill signs its scorecard but prints no evidence keys, so the Restore records no evidence, verification or outcome, and a failed rehearsal's signed evidence is lost. Interface I8 said "only on exit 0"; **decided 2026-09-23: follow D3 §2.5 and PLAT-14.3** — a failed drill publishes its signed evidence; the exit code stays authoritative for success; I8 amended. Found live by lab-refresh-9. In progress on `claude/rehearsal-fix`. | PLAT-14.3 |
| REHEARSAL-LASTSUCCEEDED-POINTID-UNWRITTEN | `RehearsalSchedule.status.lastSucceeded.pointId` is declared (D3 §4.4, CRD) but the controller never writes it, so the recovery point a passing rehearsal proved is only reachable through its Restore. Found by the `claude/rehearsal-fix` Tier-A review. Open; outside PLAT-14.3's acceptance (D3 §4.4 amendment 2026-09-23). | PLAT-14.3 (follow-up) |
| CONSOLE-APPROVALS-EXPIRED-SHOWN-REFUSED | LOW. The console's approvals page labels an approval document that has simply expired as "refused". Found live by lab-refresh-9. Open. | PLAT-19.2 (follow-up) |
| PYTEST-RUN-MODULE-COLLISION | LOW, test harness only. Collecting several `test/live/*` directories in one pytest session fails on the duplicate module name `run`; each directory passes alone and `just lint` passes. Open. | — |
| CONSOLE-COMPLETION-ON-FAILED-RESTORE | **CLOSED-LIVE for the status half (2026-09-24, lab-refresh-10: no `status.completion` on the failed Restore; the passing Restore's panel present). The console still shows the empty heading — CONSOLE-COMPLETION-HEADING-ON-FAILED.** Found by the `claude/rehearsal-fix` Tier-A review (MEDIUM-1): once exit 2 publishes its evidence keys, `completion_patch_value` (`restore.rs`) would write `status.completion` for a failed Restore whose signed failure verifies `Valid`, and the console would show "What this restore produced" on a failed restore. Fix round on `claude/rehearsal-fix` (require exit 0 and outcome `pass`). | PLAT-14.3 |
| CATALOG-POINT-READINESS-ALWAYS-STALE | **CLOSED-LIVE (2026-09-24, lab-refresh-10: plat15-2 row 5b, its control and journey 6 PASS).** `crates/logweir-api/src/routes/preflights.rs` `read_one_referent` has no `RecoveryCatalog` arm, but the controller records `RecoveryCatalog` in every catalog-point binding, so the console's submit-time re-read (PLAT-08.2) always sees a catalog-point restore check as stale and refuses Create. Seen live on lab-refresh-9 (`artifacts/lab-refresh-9/ui/plat15-2/20260923t162244z/dr/gate-after-check.txt`), masked by a harness workaround. Found by the PLAT-08.2 Tier-A review (M1). **Fixed** on `claude/catalog-referent` (Tier-A review ACCEPT-with-LOWs), merged 2026-09-23; live row in lab-refresh-10. | PLAT-08.2 / PLAT-15.2 |
| REHEARSAL-FIRE-PASS-STATUS-LOST | **CLOSED-LIVE (2026-09-24, lab-refresh-11 at `86a554e`: `rehearsal-deleted` ×2 recorded `RestoreDeleted`; `activeRestoreRef` written while running; `Authorized=True`; the L1 row and both class-sweep rows PASS).** Found live by lab-refresh-10 (row e, `d3-live/lr10r320260924t0216z/rehearsal-deleted/01-attempts.json`). `fire()` reserved the slot with a resourceVersion-preconditioned patch and then committed with the PRE-reservation object, so every commit (`activeRestoreRef`, `pendingRestoreRef: null`, the pass's verdict and conditions) was refused 409 and dropped: `activeRestoreRef` was never written, a rehearsal deleted while its verdict was owed was forgotten and the next slot fired over it, and `Authorized` stayed `NoResult`. Class sweep: preflight `CheckPlanConflict` and backup `JobNameConflict` had the same shape. Fix on `claude/reserve-commit` (Tier-A review ACCEPT-with-LOWs; fix round running); live proof at lab-refresh-11. | PLAT-14.3 / PLAT-04.1 |
| CONSOLE-COMPLETION-HEADING-ON-FAILED | LOW (lab-refresh-10 row c). A failed Restore's detail no longer shows counts or guidance, but still shows the heading "What this restore produced" with "No completion was recorded". On a failed run the section should not appear. Fix with the console visual pass. Open. | PLAT-14.1 / PLAT-10.x |
| RESTORE-STALE-CACHE-409-WARN | LOW (lab-refresh-11 Class sweep). `controllers/restore.rs:5965`: the `running_status_patch` written right after "created the runner Job" is refused 409 on a stale watch-cache object and logged `WARN restore reconcile failed; requeueing` (33 times in one lab run, rehearsal and console Restores alike). Self-healing: the next pass observes the Job and every run completed correctly. Owed: find what writes the Restore between the watch event and the pass and thread that version, or lower the log. Open. | PLAT-14.1 (follow-up) |
| REHEARSAL-RECOVERY-LOG-NOISE | INFO (lab-refresh-11). `rehearsal_schedule.rs` `recover_reservation`/`commit`: a pass handed a cache object that predates the fire pass's landed commit runs the recovery arm and logs a 409; nothing is written and the landed commit stands. Compare the handed resourceVersion with `activeRestoreRef` first, or log at debug. Open. | PLAT-14.3 (follow-up) |
| BACKUP-UNSCHEDULABLE-SAYS-CHECK-POD | LOW (lab-refresh-10/11). `check/waiting.rs:180`: a Backup's unschedulable message says "the check pod". Open. | PLAT-14.1 (follow-up) |
| MINIO-IMAGES-WITHDRAWN | External. MinIO withdrew its public images: Docker Hub deleted `minio/minio` and `minio/mc` on 2026-09-11, and `quay.io/minio/*` has answered anonymous pulls 401 since ~2026-09-24 12:55 UTC. CI's `e2e` job (`just e2e-up`) went red on `5b019ec` and `3b09059`, which blocked publication; the chart's demo MinIO and the PoC demo archive pin the same images. User decision (2026-09-24): mirror now, replace later. Because only the arm64 halves were cached on the host, the same releases (MinIO `RELEASE.2025-09-07T16-13-09Z` @ commit `07c3a429` (tag object `01ce918d`), mc `RELEASE.2025-08-13T08-35-41Z` @ `7394ce0d`) are rebuilt from the archived upstream source for amd64 and arm64 and published as `docker.io/vladyslavhaina/minio-mirror` and `mc-mirror` (AGPL-3.0, with source labels). **Fixed, merged `e1700b7` (2026-09-24):**
  - **Mirrors:** `minio-mirror@sha256:b4c3dc9fb0a8…` and `mc-mirror@sha256:9c7cbc3f47b0…`, amd64+arm64, public; anonymous pulls verified independently.
  - **Rebuild vs upstream:** mc is byte-identical; minio differs only in its build ids.
  - **Behaviour:** smoke test 48/48 identical to upstream; e2e 3869/0 on the mirror.
  - **Licence:** the image label reads `AGPL-3.0-only`, while upstream's headers grant version 3 "or any later version"; `THIRD_PARTY_NOTICES.md` says so. The next rebuild labels `AGPL-3.0-or-later`. | CI / PLAT-20.2 |
| REPLACE-MINIO | Follow-up task (user decision 2026-09-24). Replace MinIO in the e2e stack, the demo chart and the PoC with a maintained, permissively licensed S3 server. First re-validate everything Logweir relies on: `mc admin` users and policies (least privilege), versioning and Object Lock (retention `VersionedBucket`), conditional create (`If-None-Match`, the execution claim), SlowDown behaviour. Open; not started. | CI / PLAT-20.2 |
| POC-CONSOLE-SHARED-P1-P2-P4 | **CLOSED-LIVE (2026-09-24, PoC upgrade round `claude/poc-upgrade-1`, helm rev 5 to the published `sha-02dc44b6`; report `claude/poc-upgrade-1.result.md`).** Found by the PoC round (2026-09-24, `claude/poc-install.result.md` §5). P4 (high for disaster restore): Catalog → Connect sent no `X-CSRF-Token`, so it was refused 403. P2: the wizard read CR fields the API projection lacks, so every point read "unverified". P1: legacy masthead in the shared console. **Fixed on main `3b09059`** (`claude/console-shared-fix`, plus four more items from the class sweep); live re-proof owed on the next publication. | PLAT-17.2 / PLAT-15.2 / PLAT-18.2 |
| POC-LEGACY-POINT-P3-P5-P6 | **CLOSED-LIVE for P3/P5 on a legacy point written after the upgrade and for P6's copy (2026-09-24, PoC upgrade round `claude/poc-upgrade-1`, helm rev 5 to the published `sha-02dc44b6`; report `claude/poc-upgrade-1.result.md`); pre-upgrade legacy points stayed unverified — see POC-P11-P12 (P12).** PoC round. For points written before destinations (v0.1.5): P3, readiness refuses them (`ArchiveUrlUnreadable`) and then disables Create; P5, the wizard hard-codes the evidence bucket, so the restore gets no verdict and no completion; P6, the catalog `Full` sync doesn't see pre-catalog archives, and the help text says it does. Fix on `claude/legacy-point-restore` (Tier-A review running). | PLAT-20.2 / PLAT-15.1 |
| POC-P7-P8-P9 | **CLOSED-LIVE (2026-09-24, PoC upgrade round `claude/poc-upgrade-1`, helm rev 5 to the published `sha-02dc44b6`; report `claude/poc-upgrade-1.result.md`; R9.5's compromise row not run by design).** PoC round. P7: in shared mode, typed connection and schedule names are discarded (the API mints them); D11, README §10 names can't be produced. P8: *Test access* never shows its ready result. P9 (high): after `maxAgeSeconds` the controller re-judges the Approval of an admitted, finished Restore as `AuthorizationExpired`, drops its recorded authorization and hot-loops (~42 WARN/s, apiserver 100%+). Fix on `claude/poc-fixes-2` (running). | PLAT-19.2 / PLAT-12.x / PLAT-07.x |
| MANUAL-RUN-UNBOUNDED | **CLOSED-LIVE (2026-09-24, PoC upgrade round `claude/poc-upgrade-1`, helm rev 5 to the published `sha-02dc44b6`; report `claude/poc-upgrade-1.result.md`): 20 manual runs → at most 4 active, the rest Queued and finishing oldest-first; restores at ceiling 2; per-person 429 + Retry-After.** P10, PoC round. Nothing bounds manual *Back up now*: 100 accepted runs became 100 runner pods, docker-desktop hit its 110-pod limit, the node went NotReady, and MinIO answered SlowDown. Evidence fetches ARE bounded (4 per namespace). Fix on `claude/manual-run-bound` (running): a per-namespace active-Job bound with a visible queue, plus a per-principal API rate limit. | PLAT-04.x / PLAT-17.x |
| TRUSTPOLICY-DELETE-DROPS-REVOCATION | **CLOSED-LIVE (2026-09-25, PoC upgrade round 2 `claude/poc-upgrade-2`, helm rev 7 to the published `sha-b748fd5f`; report `claude/poc-upgrade-2.result.md`), minted key only: revocation reached the other policy within 2 s, the recording policy's delete was held, G9 refused the reason change; L9 not stageable on the single-namespace PoC without its own keys.** **FIXED (2026-09-24, HIGH): `claude/trust-revocation-durable` merged `da83834`.** A KeyCompromise record is installation-wide, reaches recorded verdicts in every namespace promptly (roster namespaces included), holds its policy from deletion until recorded elsewhere, and CEL rule G9 makes the reason permanent. Tier-A review REJECT (narrow), fix round, re-check RESOLVED. Live rows L9/L10 owed at the next PoC round. Observation from the PoC round (R2), to be assessed: deleting a TrustPolicy drops a compromise revocation it recorded while `TrustRoster/default` still lists the key, so the key's evidence may read trusted again under the legacy roster. Security-relevant; severity not yet decided. Open. | PLAT-19.1 / PLAT-08.x |
| POC-DOC-D1-D11 | **CLOSED-LIVE for D1–D5 and D11 (2026-09-24, PoC upgrade round `claude/poc-upgrade-1`, helm rev 5 to the published `sha-02dc44b6`; report `claude/poc-upgrade-1.result.md`); D6–D10 need a rollback, uninstall or the R2 baseline and stand on round 1.** PoC round doc and profile defects, D1–D10 **fixed on main `7beb7c8`** (`claude/poc-install`):
| POC-P11-P12 | **CLOSED-LIVE (2026-09-25, PoC upgrade round 2 `claude/poc-upgrade-2`, helm rev 7 to the published `sha-b748fd5f`; report `claude/poc-upgrade-2.result.md`): the three stuck pre-upgrade legacy points verified 3 s after the controller restart and one restored Valid 150/150 through the console; duplicate catalogs refused, takeover 28 s.** **FIXED (2026-09-24): `claude/poc-fixes-3` merged `56205b1`** (P12: bounded retries for transient read failures, throttled restart re-read, no windowless Valid; P11: `DuplicateCatalog`). Live re-proof owed at the next PoC round. Found by the PoC upgrade round (2026-09-24).
| POC-P13-P14 | **CLOSED-LIVE (2026-09-25, PoC upgrade round 3 `claude/poc-upgrade-3`, helm rev 9 to the published `sha-a54fb823`; report `claude/poc-upgrade-3.result.md` §5).**
  - **P13:** the topics were typed first, then a source (found by search) and a non-default destination were chosen. All were kept, and the POST carried them (202). An edit made while the panel was followed survived. The class is proven on the connection detail, the destination's Rotate access and the wizard's deferred paint.
  - **P14:** proven on the schedule form and on the list panel (PANEL14, PANEL14B):
    - a double click is one check;
    - a retry inside the window replays the same Preflight;
    - after expiry, one click gives a 200 replay naming `expired`, then a 202 under a new key that applies;
    - Cancel then Check is a new check.
  - **Residue:** Discover-topics-after-stale cannot be staged inside the 900 s shared-mode session. The offline rows in `ui/tests/check-intent.spec.js` cover it.

  **FIXED:** `claude/poc-fixes-4` merged as `a54fb823`.

  Found by the PoC upgrade round 2 (2026-09-25) on the README §10 re-run.
  - **P13:** the Schedules list's Backup readiness panel wipes typed topics when a discovery read repaints it, and the check is then refused 422.
  - **P14:** the schedule form's Check readiness replays an EXPIRED Preflight (content-derived Idempotency-Key), so no fresh verdict is possible for unchanged inputs.
  - **H5:** a plat15-2 harness guard already fails on main.

  Fix on `claude/poc-fixes-4` (running). | PLAT-20.2 / PLAT-08.2 / PLAT-05.x |
  - **P12** (medium; blocks PLAT-20.2's archive readability across an upgrade): a legacy inline-archive Backup whose evidence read failed stays unverified forever, with no retry. On the PoC the read failed because the controller's evidence credential was missing (D12), so three pre-upgrade legacy points are never offered for restore.
  - **P11**: several RecoveryCatalogs over one destination are all accepted, although the design specifies a refusal.

  Fix on `claude/poc-fixes-2`'s successor `claude/poc-fixes-3` (running). D12 (the PoC profile never created `logweir-evidence-ro`) and D13 (`poc-secrets/` not gitignored) are **fixed on main `446fbaf`**. | PLAT-20.2 / PLAT-15.1 |
| RATE-LIMIT-PER-CONSOLE-PROCESS | LOW. The per-person manual-run rate limit (P10) is counted per console process, so on the 2-replica PoC profile it is effectively ×2. Documented. Open (a shared counter or a per-replica share would close it). | PLAT-17.x |
| TRUST-HELD-POLICY-HEARTBEAT-FANOUT | LOW (trust re-check). A TrustPolicy held from deletion fans the compromise re-evaluation out again on every 300 s status heartbeat (`trust.rs:~1292-1295`). Bounded, not a storm, but wasted work. Open. | PLAT-19.1 |
| TRUST-LOOKUP-CLUSTER-LIST-PER-READ | LOW (poc-fixes-3). The trust lookup does one cluster-wide list per evidence re-read. Owed: a shared cache. Open. | PLAT-19.1 |
| HARNESS-CSRF-TOKEN-RECORDED | Incident (2026-09-24, PoC upgrade round): one harness row printed a viewer's CSRF synchronizer token into a local artifact. It was redacted in place, and a guard now refuses harness rows that keep token values. No password, key or bearer token was exposed. Closed. | PLAT-20.1 |
| CONSOLE-MCP-ROUND1 | **Functional half CLOSED-LIVE (2026-09-25, PoC upgrade round 2 `claude/poc-upgrade-2`, helm rev 7 to the published `sha-b748fd5f`; report `claude/poc-upgrade-2.result.md`); the human-like round 2 is the orchestrator's.** **FIXED (2026-09-24): `claude/console-ux-1` merged `ffe7772`,** all 34 findings; review REJECT (narrow), fix round accepted. Round 2 of the human-like pass owed on the next publication. The human-like console pass through the Playwright MCP (2026-09-24, on the PoC at `86a554e`; report `claude/mcp-ui-test.result.md`, 25 screenshots in `claude/artifacts/mcp-ui-test/shots/`) found 34 layout, flow and copy defects.
| CONSOLE-MCP-ROUND2 | **VERIFIED LIVE (2026-09-25, the orchestrator's MCP round 3 on the published `sha-a54fb823`, helm rev 9; report `claude/mcp-ui-test-round3.result.md`).**
  - **Fixed:** all six medium rows (R2-3, R2-11, R2-12, R2-14, R2-14b, R2-16) and the low rows R2-1, R2-2, R2-4, R2-6, R2-8 and R2-13.
  - **Mitigated:** R2-17, now with a scroll shadow; the action column is always visible.
  - **Still open:** R2-10, carried to CONSOLE-MCP-ROUND3.
  - **Not re-checked:** R2-5 and R2-15.

  **FIXED:** `claude/poc-fixes-4` merged as `a54fb823`. The poc-upgrade-3 harness rows passed the same fixes in the browser (13 PASS, 5 NOT RUN).

  The human-like console pass, round 2 (2026-09-25, on the PoC at the published `sha-b748fd5f`; report `claude/mcp-ui-test-round2.result.md`, 20 screenshots in `claude/artifacts/mcp-ui-test/round2/shots/`).
  - **Round 1 verified fixed:** the 5 HIGH findings and the rest of round 1's findings; the wizard is usable in 1.3 s (was 12.4 s).
  - **Open, medium (6):**
    - R2-3: the Clusters table overflows its card;
    - R2-12: the readiness headline reads "unknown" when only execution-only rows are unknown;
    - R2-14: a viewer or norole user reaching the restore route sees the actionable wizard;
    - R2-14b: the return address survives Sign out;
    - R2-16: norole gets no landing explanation;
    - R2-11: a pending check is labelled "does not apply".
  - **Open, low:** 10 findings.

  Fix on `claude/poc-fixes-4`, together with P13/P14 (running). | PLAT-18.2 / PLAT-17.2 |
  - **High (5):**
    - MCP-1: no Sign in affordance when signed out, and the console falls back to legacy mode;
    - MCP-4: raw problem-JSON shown when signed out;
    - MCP-5: no signed-in identity and no Sign out;
    - MCP-25: the restore wizard's primary action "Restore this point" is in a clipped, overflowing table column;
    - MCP-29: the six-step wizard is one 22,686 px page.
  - **Medium (11)**, including:
    - MCP-16: per-row SIGNED note triples row height, 8 lines at 1024 px;
    - MCP-19: Operations nav dead end;
    - MCP-27: stepper shows readiness DONE before it ran;
    - MCP-30: `--context docker-desktop` hard-coded in product copy;
    - MCP-32: a 403 on Keys shown as "no trust exists";
    - MCP-26: the wizard takes 12.4 s to become usable at 258 points.
  - **Low (18)**, including: timestamps wrapping mid-value, nanosecond precision, raw booleans and condition syntax, internal `PLAT-` IDs in UI text, and a role-unaware nav.

  Open. A UI fix batch is queued after `claude/poc-fixes-2`, then round 2 on the next publication. | PLAT-18.2 / PLAT-17.2 / PLAT-11.x |
  - D1: Dex needs a writable `/tmp`.
  - D2: CRD apply needs `--force-conflicts`, now with a `doc_lint` guard.
  - D3: stale NOTES.txt.
  - D4: hook logs.
  - D5: `trustpolicy.sh` signer window.
  - D6: rollback deletes adopted objects.
  - D7: uninstall deletes the MinIO PVC.
  - D8: R2 console "Ready but sign-in 503".
  - D9: `revokedAt` in `docs/keys.md`.
  - D10: uninstall "what remains".

  D11 (README §10 names) depends on P7. | PLAT-20.2 |
| POC-P15 | LOW–MEDIUM, found by the PoC upgrade round 3 (2026-09-25; `claude/poc-upgrade-3.result.md` §6, `defects/P15-follow-budget.txt`).
  - **The defect:** a readiness check that runs longer than the page's follow is left "The check has not finished … this page reads it again until then", and the page never reads it again.
  - **Live instance:** on the Schedules list's Backup readiness panel, a renewed check took 64 s. The panel's follow is `READINESS_POLLS` 20 × 2 s = 40 s, so it read the check 20 times and stopped.
  - **Class:** every console follow is shorter than a Preflight's own `timeoutSeconds` 120 (`DEFAULT_TIMEOUT_SECONDS`):
    - Schedules: 40 s;
    - Test connection: 30 s;
    - Test access: 60 s;
    - restore step 5: 90 s.
  - **Recovery:** click again, which replays the finished check, or reload.

  Fix on `claude/poc-fixes-5` (running): all four follows, plus a guard that ties every budget to the check's timeout. | PLAT-20.2 / PLAT-03.x / PLAT-10.x console |
| HARNESS-CHECK-TABLE-READERS | **FIXED (2026-09-25, H7; `claude/poc-upgrade-3` `93140122`, merged as `4e58d330`).**
  - **The defect:** R2-3 prints a check's fields in four cells, but every reader in `scripts/live/poc` took a row from innerText's tab-joined `id verdict gating code`. So every readiness row would have waited out its budget and failed: J2–J4, J6, README10 R8.3/R8.5, restore step 5 and P8.
  - **Fix:** `checkRowsIn`/`settledRows` read cells and `data-field` spans, and settle on rows plus no "checking…".
  - **Guard:** in `test_poc_harness.py`, with planted twins. Its negative control flags 8 lines of the `a54fb823` harness, and on the real `checkTable` the old readers find 0 rows.
  - **Class:** the lab harnesses read the API's JSON, so nothing is owed there. | PLAT-20.2 harness |
| CONSOLE-MCP-ROUND3 | The human-like console pass, round 3 (2026-09-25, by the orchestrator, on the PoC at the published `sha-a54fb823`, helm rev 9; report `claude/mcp-ui-test-round3.result.md`).
  - **Evidence:** 17 screenshots in `claude/artifacts/mcp-ui-test/round3/shots/`. A secret scan of 41 artifacts found 0 values.
  - **Verified:** round 2's fixes (see CONSOLE-MCP-ROUND2).
  - **Open:**
    - R2-10 (low): raw Markdown backticks in step-5 copy;
    - R3-1 (low–medium): at 390 px, after Check readiness, focus lands on the status line behind the sticky Back/Next bar, so the result is hidden until the user scrolls;
    - R3-2 (low): a viewer is offered Restore this point, and the route then refuses correctly;
    - R3-3 (low): norole's header subline ("choose a namespace to see your role") contradicts the card;
    - R3-4 (low): internal vocabulary in the applicability chip;
    - R3-5 (low): cluster IDs wrap at 1024 px.

  R2-10 and R3-1 to R3-3 are on `claude/poc-fixes-5` as a separate commit (running). R3-4 and R3-5 are open. | PLAT-18.2 / PLAT-17.2 console |
| CATALOG-POINT-STATE-NOT-IN-CHECK-INPUTS | LOW, pre-existing (catalog-referent review). A restore check binds its `RecoveryCatalog` by UID, and the catalog's spec is immutable except `syncRequest`, but the chosen point's catalogued state is not among the check's recorded inputs, so a re-sync that changes that point (e.g. its trust or its receipt) does not mark the check stale. Bounded: the runner re-verifies the point's signed receipt and signer at restore time and refuses an untrusted point (exit 3 `PointUntrusted`). Open. | PLAT-08.2 / PLAT-15.2 (follow-up) |
| TEST-APPROVAL-UNPINNED-TIMING | **FIXED (2026-09-24):** the bound now subtracts a same-moment `--version` start-up baseline (985 ms vs the command's 8.6 ms warm); a 0 s bound fails as a control. LOW, test only. `weirkeeper` `approval.rs::an_unpinned_approver…` failed at 15–55 s against its 15 s bound on a loaded host (four agents compiling; seen by the `claude/readiness-principal` worker); it passes on re-run and at base, and no product path changed. Fix: a bound that measures the controller's own work rather than wall time, or a documented larger budget. Open. | — |
| RECEIPT-DUP-UPGRADE-WINDOW | An execution whose first run was made by a runner without the execution claim, re-created after the upgrade (the runner image is not frozen in the execution inputs), is claimed successfully by the new runner and the engine overwrites the old run's manifest. Mitigation: release notes, "let in-flight Backups finish before upgrading". Fix (follow-up): a pre-engine manifest-exists refusal, after the engine test doubles write the manifest. Open. | PLAT-06.1 / PLAT-20.2 |
| CASE-E-PESSIMISTIC-STATUS | LOW. When a lost Job's pod reached the execution claim and went on to sign a valid receipt (e.g. an orphan-deleted Job), the re-created Job ends `Failed`/`ExecutionAlreadyClaimed` although the evidence is intact and catalogued. A controller adopting the committed receipt (it holds `evidenceRead`) would close it. Open. | PLAT-06.1 |

### Evidence audit (2026-09-23)

A read-only audit of the 38 Done tasks (`claude/done-audit.result.md`) found that the macOS `/tmp` cleanup, before the run directory moved to `$HOME/.logweir-roadmap-run`, emptied or deleted 21 cited result/review files and several artifact directories. Of the Done tasks, 32 are confirmed against their acceptance. Where a record cites lost evidence, read it together with the later re-proof:

| Task | Lost | Re-proof on a later build |
|---|---|---|
| PLAT-04.1 | closure-0413, plat06 review | plat06 case-c/case-e artifacts; PLAT-20.1 composed case-e and the full journey on lab-refresh-9 (`lab-refresh-9/journeys/full-20260923t1722z`); unit rows `schedule_controller.rs:2968,3039` |
| PLAT-06.1 | plat06 review (ACCEPT, two MEDIUM; M1 is RECEIPT-DUP) | `artifacts/plat06/` (106 files); plat06 7/7 on lab-refresh-8 |
| PLAT-07.1 | `artifacts/plat07/`, plat07 review | `artifacts/plat07-live/`; plat07 14/14 on lab-refresh-8 |
| PLAT-11.1 | ui-restore-selection artifacts and review | plat12-13 20/20 on lab-refresh-9; unit rows `ui/tests/pages.spec.js:2061,2126,2144` |
| PLAT-13.1 | `/tmp/plat13-*.md`, closure-0413 | `artifacts/ui-harness-drift/plat13/` (2026-09-23); the native stale-namespace journey on lab-refresh-9 |
| PLAT-13.2 | ui-correct artifacts and review | plat12-13 20/20 on lab-refresh-9 |
| PLAT-18.1 | ui-typed-client review | the live screenshots and run log in `artifacts/ui-typed-client/after-review-fixes/` |
| PLAT-01.1, 01.2, 02.1, 02.2 | `artifacts/plat01-02-live/` and its result and review | **Re-proved in full.** 01.x by lab-refresh-10 at `b426096`: `scripts/test-plat01-02-live.py` 31/31 `accepted` (`artifacts/plat01-02-live-rerun/`). 02.x on 2026-09-24 at main `71edaaa`: `scripts/test-plat02-chart-live.py` 18/18 `accepted`, run 3 at harness `b73aed0`, beside the old lab, which it scaled down and restored exactly (`artifacts/plat01-02-live-rerun/chart-20260924t1224z/`; report `claude/plat02-chart.result.md`). Three harness drifts were fixed with negative controls. |

Also owed from the audit: a Tier-A review of PLAT-08.2 (done: `claude/plat08-2.review.md`, ACCEPT-with-LOWs plus one integration MEDIUM, fixed on `claude/catalog-referent`), a Tier-B review of PLAT-20.1 and of the evidence harnesses, and a closing re-check of PLAT-16.2's last REJECT (in progress).

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
naming its Secret and key) and lab-refresh-4 at `7b4fae9` (its PLAT-03.1 rows — the lab-refresh-4 report itself was lost to the macOS `/tmp` cleanup before the run directory moved to `$HOME`: the image-failure
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
proves S15b live.** Landed in main as `` [audit 2026-09-23: the first hash was lost when this record was written and the hashes after it are shifted one place; by commit subject the controller is `4c4d2ed` and the three grants `b8c3bab`] (the controller, its catalogue half
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
Landed in main as `` [audit 2026-09-23: the first hash was lost when this record was written; by commit subject the editable policy is `c00da24`] (editable policy with immutable run snapshots), `c00da24`
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

**Amended 2026-09-23 (RECEIPT-DUP).** The case-e evidence "deleted Job re-created from identical inputs" is superseded for the runner half.
- **Pod reached the claim:** since RECEIPT-DUP, a re-created Job whose predecessor's pod reached the execution claim exits 1 `ExecutionAlreadyClaimed` without starting the engine. The Backup ends `Failed` with that `status.exitReason`. The execution keeps exactly the one receipt the first pod signed, which still verifies, and its manifest still hashes to its attested digest.
- **Pod never existed:** a Job deleted before its pod existed is still re-created and Succeeds.
- **Controller half, unchanged:** the Job is re-created from the frozen inputs (same ConfigMap UID, args, volumes, annotations). Forbid blocks the next slot while the Backup is nonterminal and admits it after.
- **Tests:** `scripts/test-plat06-live.py case-e` runs both arms (claimed via `--cascade=orphan`, unclaimed via a pre-fire `pods: 0` quota), with pure judges and offline negative controls (`scripts/test_plat06_case_e_rows.py`). The live re-run is owed at lab-refresh-10.
- **Retries:** a scheduled Backup that fails this way is retried under `<uid>-<slot>-r<k>` only when the schedule has `spec.retry`; otherwise the slot is `RunFailed`.

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

**Amended 2026-09-23 (RECEIPT-DUP).** The case-e evidence "deleted Job re-created from identical inputs" is superseded for the runner half.
- **Pod reached the claim:** since RECEIPT-DUP, a re-created Job whose predecessor's pod reached the execution claim exits 1 `ExecutionAlreadyClaimed` without starting the engine. The Backup ends `Failed` with that `status.exitReason`. The execution keeps exactly the one receipt the first pod signed, which still verifies, and its manifest still hashes to its attested digest.
- **Pod never existed:** a Job deleted before its pod existed is still re-created and Succeeds.
- **Controller half, unchanged:** the Job is re-created from the frozen inputs (same ConfigMap UID, args, volumes, annotations). Forbid blocks the next slot while the Backup is nonterminal and admits it after.
- **Tests:** `scripts/test-plat06-live.py case-e` runs both arms (claimed via `--cascade=orphan`, unclaimed via a pre-fire `pods: 0` quota), with pure judges and offline negative controls (`scripts/test_plat06_case_e_rows.py`). The live re-run is owed at lab-refresh-10.
- **Retries:** a scheduled Backup that fails this way is retried under `<uid>-<slot>-r<k>` only when the schedule has `spec.retry`; otherwise the slot is `RunFailed`.

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

**Completion record — Done (2026-09-21), PLAT-06.2.** Source landed on main as D1 W6 (the
API half, `72751ab`: `POST …/backups` creating the canonical manual `Backup` of D1 §8.1 —
`spec.trigger.kind: Manual`, the deterministic `logweir-manual-<26 base32>` name from the
request scope, `scheduleRef` and the policy snapshot copied from one read — with the
idempotency contract: same key + same body ⇒ 200 and the existing uid, same key + different
body ⇒ 409 `idempotency_conflict`, a suspended or busy schedule never blocking a manual run),
D1 W7 (the console half, `a2ce381`..`700916e`: "Back up now" and "Run first backup now"
through the API with one idempotency intent per draft, the resulting run showing its
copied schedule revision), and `claude/plat06-2-finish` (`..cc05b34`, review
`claude/plat06-2-finish.review.md` ACCEPT-WITH-FIXES then the six fixes applied): the legacy
`kubectl proxy` page gains exactly `create` on `backups` in the chart's UI ClusterRole (the
`chart_lint` row pins the verb set; a planted extra verb fails) and derives the same
deterministic name the API derives — one rule, D1 §8.2, pinned by one fixture
(`ui/tests/fixtures/manual-backup-names.json`) that both a node row and a Rust row in
`crates/logweir-api` drive; the manual CR path is demonstrated through the CLI beside the UI
(`L-06-2-cli`: `kubectl --context docker-desktop apply -f config/samples/backup-manual.yaml`
and the API-created run for the same scope compared field by field — name equal,
`specDifferences: []`, labels equal, both reconciled; the negative control changes one
snapshot field and is refused `RunPolicyDigestMismatch`); and the failed-preflight case is
journeyed. Every listed test PASSES live on docker-desktop against the lab at `af64073`
(`scripts/plat06-2-ui-e2e.mjs`, 9/9, localAdmin console mode; evidence
`claude/artifacts/plat06-2-ui/lw-p062-ui20260921t205953z/` and
`claude/artifacts/d1-live/20260921t203643z/`; the reviewer re-ran `L-06-2-cli` in its own
namespace with the same result): **double click** — two POSTs, one run (and the legacy
node row, with the adopt-anything mutant killed); **lost HTTP response** — a real
`route.abort` after a real `route.fetch`, the resend answered 201 with the same uid, one run;
**refresh** — the run listed after a real reload, the next click a second run; **paused
schedule** — a run created with `suspend` untouched and the generation unchanged (D1
§8.3); **failed preflight** — a schedule whose source is unreachable: D1 §8.4's own rule
on screen (the readiness label `unknown`, the button enabled, no client-side guess), the
run created and `Failed`/`operational` with the preflight's reason on the run view, the
healthy control `Succeeded`; **successful scheduled-policy copy** — the copied policy
digest equals the published digest at generation 1, through the console and the CLI
alike (`specDifferencesConsoleVsApi: []`). Acceptance: repeated clicks create one requested
run and a deliberate later backup creates another — the double-click, lost-response and
refresh journeys read together. Named residue, not this task's acceptance: D1 §8.4's
`notReady` branch (a preflight that has already answered not-ready before the click) was
not journeyed — the unreachable-source case exercises the `unknown` branch; legacy mode's
"Back up now" is proved by the chart grant, the shared name fixture and node rows, not by
a live journey through the `kubectl proxy` page. Migration: the chart grant is additive;
`config/samples/backup-manual.yaml` is the CLI path; nothing mutates an existing run.

## PLAT-07 — Reuse saved cluster connections everywhere

**Priority P1 · Proposed.** SCRAM reference reuse is already fixed; extend the
same model to registration, discovery, preflight and restoration without
reintroducing separate credentials.

**Partial record (2026-09-17) — PLAT-06.2 API half, with PLAT-04.2's previews and
PLAT-05.1's policy edit: D1 W6 landed; the tasks stay In progress until W7 (UI) and
W8 (live) land.** Landed in main as `` [audit 2026-09-23: the first hash was lost when this record was written; by commit subject the route families are `89bba9e`] (three route families), `89bba9e`/
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

**Completion record — Done (2026-09-21), PLAT-08.1.** Source landed on main as D2 W1/W2
(the pure destination model and explicit store options, `c13b0cc`..`56bd074`), W6a/W6b (the
`BackupDestination` kind and the destination sentinel on existing kinds, inside
`crds-shapes`), W7 (`27fb924`..`0b25e95`: `weirkeeper::destination::resolve` returning, per
operation role — `archiveWrite`, `archiveRead`, `evidenceWrite`, `evidenceRead` — a complete
and explicit credential set never inherited from the controller's environment, the
`BackupDestination` controller and the controller-identity evidence cache), W10
(destination-backed Backup, Schedule and Restore execution, `e98eb8a`), W11 (RBAC, the
policy ConfigMap and the admission policy), D2 W13's console destinations page, and the
frozen `Backup.status.destination {name, uid, generation, locationDigest}` block with the
restore preflight's `RecoveryPointLocationMismatch`/`RecoveryPointLocationUnknown`
(`52c1f00`..`85adc2e`, D2 §6.3 amended). Every listed test is proven live on docker-desktop
from `e2e/k8s/d2` (D2 W14 on `e7d0e79`, `claude/d2w14.result.md`/`.review.md` ACCEPT;
re-proved on lab-refresh-3/4): **distinct endpoints/credentials** — S1, two MinIO
destinations with disjoint buckets and three credentials each, the Job env, the frozen plan
storage block and the bucket contents disjoint; **denied location** — S3, `Preflight
DestinationAccess archiveListable/AccessDenied`, the Backup `Failed` with `exitCode 1`;
**malformed URL** — S2, transport/scheme, malformed endpoints, the reserved `logweir/`
prefix, `VirtualHosted` with a custom endpoint refused as `AddressingUnsupportedByEngine`,
both immutability refusals; **namespace separation** — S4; **evidence/archive destination
differences** — S5, a Restore reading `lw-a` with `a-reader` and writing its evidence to
`lw-b` with `b-writer`, the scorecard verified from `lw-b`'s bytes and nothing under
`lw-a/logweir/drills/`; no global-configuration leakage (S6); **secret values not returned**
— S14 and two independent sweeps over every object, API body, browser page and sixty
minutes of controller log. The three items the 2026-09-18 records left open are closed:
the frozen destination block and the mismatch row proved on lab-refresh-4 (its report
§605-608), D2-EVIDENCE-NOTATTEMPTED-UNWRITTEN closed on lab-refresh-3, and D2 §15 U6 — the
per-role minimal object-storage permission table — now MEASURED rather than recorded
(`claude/plat08-u6` `..0452439`, review `claude/plat08-u6.review.md` ACCEPT-WITH-FIXES then
the seven fixes applied): seven roles, thirty-five bisection rows on the worker's own
MinIO, fourteen required units each proved by withdrawing it and reading the product's own
classified refusal (the preflight row's `AccessDenied`, the Backup/Restore condition, the
enforcer's `state=Kept code=…`), nineteen units proved NOT required by a successful object
without them, the two `s3:ListBucket` prefix legs measured separately (both compound rows
were a leg too wide — `archiveWrite` lists only under `<prefix>/*`, the catalogSync reader
only under `logweir/*`), the table in `docs/kubernetes.md` §7a with every row citing its
harness row and the sentence that a broader grant is not required, `docs/install.md`
reconciled, and the reviewer's independent re-run of two roles matching exactly; D2 §15 U7
measured and holding. Named residue, none of it this task's acceptance: the retention
enforcement Job is handed the `archiveRead` grant as its evidence credential
(RET-EVIDENCE-GRANT-IS-ARCHIVEREAD, defect table — the one place the product still violates
"keep source archive access separate from evidence storage access"; the fix is in flight
on `claude/fix-retention-grant` and the U6 retention row re-runs at the batch refresh);
`destination.evidenceWritable` answered with the archive credential and a
`DestinationAccess` check that never probes writes (PREFLIGHT-EVIDENCEWRITABLE-WRONG-PRINCIPAL,
DESTINATIONACCESS-IGNORES-WRITEPROBE, defect table); D2 §3.11/§14.3/§15 wording reconciled
by the amendment recorded at this integration. Migration: none — additive kinds; inline
archive configuration converts through `:from-legacy` without changing in-flight plans.

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

**Completion record — Done (2026-09-23), PLAT-08.2.** Source on main as `claude/plat08-2` (integrated in `ac00819`; Tier-B pass with API mutants) plus the draft-submit fix from `claude/plat19-2` (DRAFT-PREFLIGHT-NEVER-READY). Proven live on lab-refresh-9 (lab at main `306cebf`: controller `sha256:f71fdcb4…`, runner `sha256:db8d8ade…`; report `claude/lab-refresh-9.result.md`; artifacts `claude/artifacts/lab-refresh-9/` and `claude/artifacts/{d2,d3}-live/lr9*`): `scripts/plat08-2-ui-e2e.mjs` 14 rows + 8 controls PASS, 0 blocked (`ui/plat08-2/lw-lr9p082-20260923t162743z/live.json`): HTTPS with path-style, explicitly configured local HTTP, destination edit during a draft (T3, and T3-submit as a real click — the page re-reads readiness, refuses naming `BackupDestination/dest-b`, sends nothing), custom endpoint, archive/evidence separation, a recovery point restoring its saved settings, and addressing never downgrading transport; two-destination operation. Required object permissions: the measured per-role table in `docs/kubernetes.md` (U6 re-measured on this build with the enforcer's `s3:GetObject`). Migration: inline→destination conversion states whether the archive moves and requires an explicit tick.

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

**Completion record — Done (2026-09-21), PLAT-09.2.** Source landed on main as D1 W5
(`7072b9d`, dynamic selection per run through the check runner: the frozen selection,
`status.selection {mode, coverage, visibility, limitedTopicCount, resolvedTopicCount}`,
exclusions and internal-topic rules, `SelectionEmpty`, `incompleteDiscovery: Refuse |
BackUpVisibleTopics`), the D1-DISCOVERY-IMAGE fix (`86a18b1`, the discovery Job carries
the configured runner image), and the console half (D1 W7, coverage rendered from
`status.selection` alone, mutant-pinned). Live proof on docker-desktop against the lab at
`af64073` by `scripts/live/d1` (`fe5e342`..`cb17eac`: L-09-3a, L-09-3b and L-09-5 added, the
ACL fixture `scripts/live/d1/fixtures/acl_kafka.py` — a KRaft broker with
`StandardAuthorizer`, SCRAM credentials set at storage-format time and a principal denied
`Describe` on one topic — and three harness defects fixed), all seven rows in one fenced
namespace, one build: **Topic creation/deletion between runs** — L-09-2 (`t3` created after
run 1 froze enters run 2's frozen list `["t1","t2","t3"]` while run 1's plan ConfigMap keeps
its bytes and `resourceVersion` and its receipt names two topics) and L-09-3a (after
`rc-gone` is deleted the next dynamic run freezes `["rc-keep"]`); **excluded/internal
topic** — L-09-1 (`__consumer_offsets` internal, `pfx-a`/`skip-me` excluded by rule, five
counting clauses); **empty resolution** — L-09-4 (`Failed=True/SelectionEmpty`,
`TopicsResolved=False`, no runner Job, no retry); **ACL limitation** — L-09-5 (`Refuse` →
`Failed/DiscoveryIncomplete` with no runner Job; `BackUpVisibleTopics` → `Succeeded`,
`coverage: VisibleUserTopicsOnly`, the undescribable topic absent from the frozen list, the
receipt's topics and its records; `visibility: unknown` is the contract here — D1 §7.5,
`check_contract.rs:2905` — because Kafka omits an undescribable topic from a listing rather
than erroring on it, confirmed by the review; the reviewer's independent re-run passed with
the same `resultSha256`); **discovery/execution race** — L-09-3a (a topic deleted between
freeze and execution: the frozen plan does not move by one byte across the held window, the
run fails with the runner naming the deleted topic — attributed from failure-marked runner
log lines only, since the runner echoes its input — or, on the other branch D1 §7.5 admits,
succeeds with zero records for it) and L-09-3b (the source changes between discovery and
freeze: the run is refused naming both resolution digests, no runner Job); **immutable
snapshot** — L-09-2's digest and L-09-3a's held window; **preserve named allowlists** —
L-09-6 (no discovery Job, `SelectedTopics/NamedTopics`, frozen `["t1"]`). Four negative
controls each fail on their own sentence (NEG-09-3a/3b/5 and NEG-1), and NEG-09-5 is the
sharpest evidence for the second acceptance clause: with `Describe` granted on every topic
the run still records `VisibleUserTopicsOnly` — `AllUserTopicsAttested` is reachable only
through an administrator attestation in the installation policy, never from a listing that
happened to succeed. Evidence `claude/artifacts/d1-live/20260921t1301z/` and (fix round)
`20260921t1738z/`; report `claude/plat09-2-rows.result.md`; review
`claude/plat09-2-rows.review.md` ACCEPT (one medium — the L-09-3a `Failed` arm accepted
any operational exit, a false-pass path — and one low fixed in `cb17eac`; the lean loop's single
pass). Gates: `just lint` rc 0, `scripts/live/d1` pytest 37, `test_rows` 75, gate_lint 13,
doc_lint 12. Not exercised live, by name: the administrator attestation path
(`AllUserTopicsAttested`) and the console label "Visible user topics only" on a real
limited run (the label is mutant-pinned in D1 W7's console half and the journey of
`incomplete discovery` ran in its review). Migration: none — additive rows; the fixture is
test-only.

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

**Completion record — Done (2026-09-23), PLAT-10.1 and PLAT-10.2.** Source on main as `claude/plat10` (`ddc5de7..27eb7c9`, the partial record below states what it does, two review rounds, re-check ACCEPT). The restore half that was BLOCKED became reachable when EVIDENCE-FETCH-JOB-UNBUILT landed (`4cd3e55..4f2c93e`) and was proved on lab-refresh-8's lab (main `f49849d`, controller `sha256:01338ec0…`, runner `sha256:ad705ba2…`). Evidence: `claude/artifacts/lab-refresh-8/plat10/lw-lr8p10-20260923t001510z/live.json` — `scripts/plat10-ui-e2e.mjs` at `79def87`/`b8933e3` (branch `claude/lab-refresh-8`), host `logweir-api` (sha256 `5b02367d…`) in localAdmin mode + Playwright, the lab controller the only reconciler, `statusWrites: 0`, 20 journeys and 11 negative controls PASS, 0 BLOCKED. PLAT-10.1: selected-topic and all-user-topic creation, edit (a later run freezes the new revision, the earlier keeps its own), invalid cron refused in the API's own words with the draft kept, a real terminal `notReady` Preflight rendered as not ready, keyboard-only completion (no pointer event), first-run redirect. PLAT-10.2: empty history, running/failed/verified runs from the lab controller, pause/resume with history still offered, a deleted schedule's history kept without schedule controls, each real point's own Restore from the controller's window, navigation to an older point binding the wizard to it, unavailable (`Missing`) beside healthy, catalog incomplete labelled as such; the pre-10.2 deep link `#/schedules?ns=` still reaches the list. Done evidence: create → backup → schedule detail → restore — the approved Restore Succeeded and the restored records equal the source's (comparator control refuses a shift of one record). Two rows of that run FAILED and belong to other tasks: the verified-approval page (CONSOLE-APPROVAL-VERIFIED-SUBJECT-UNMAPPED, PLAT-12.2, fixed on `claude/plat19-2`) and `status.completion` (RESTORE-COMPLETION-UNWRITTEN, PLAT-14.1). Residue: SCHEDULE-FIRES-SLOT-BEFORE-CREATION makes the empty-history journey time-of-day sensitive until decided; the PLAT-10 review's LOW (focus does not return to the control after the detail re-renders) belongs to PLAT-18.2. Migration: the list route and its deep links are unchanged; `POST .../schedules` accepts the whole policy additively and replays pre-upgrade bodies byte-identically.

**Partial record — In progress (2026-09-22), PLAT-10.1 and PLAT-10.2: source landed, restore half blocked.**
Source landed on main as `claude/plat10` (29 commits ending `27eb7c9`): `POST .../schedules`
takes the whole policy (one validator shared with `PUT`, one `schedule_invalid` code, zone parsed
in the same pass, saved destination by reference so the server alone builds the URL, explicit or
all-user-topic selection), additive and byte-compatible for old request bodies (an old-shape
create replays under its old `Idempotency-Key`, pinned by `schedule_create`); the catalog points
view gains an optional `incomplete: true`; the console gets a guided create/edit form (advanced
options collapsed, readiness from a real `Preflight` marked stale when its inputs change, the
API's own refusal words, draft kept) and a schedule detail page (`#/schedules?ns=…&name=…`; the
list route is unchanged) with source, resolved destination, policy revision, latest restorable
point and age, next run, active work, failures, history grouped by schedule UID (archived
same-name schedules never merged) and Back up now / Pause-Resume / Edit / per-row Restore that
re-render the detail in place. A run the catalog does not list reads `catalog incomplete`,
`catalog view expired`, `outside the catalog view` or — only for a complete current view — `not
in the catalog`; `Missing`/`Unreadable` points are distinct from healthy ones and never offered.
Reviews: two independent rounds (`claude/plat10.review.md`, REJECT ×2 on 5 then 2 HIGHs and 3
MEDIUMs, all closed; re-check ACCEPT at `27eb7c9`, one LOW open: focus does not return to the
control after the detail re-renders); 12 + 1 API/console mutants killed
(`claude/plat10-mutants.log`). Gates at `27eb7c9`: node 480/480, `cargo test --locked -p
logweir-api` 385/0, ui/chart/doc lint, `just lint`, links, chart/schema checks; full
`scripts/ci-check.sh` on main `27eb7c9` (`claude/main-27eb7c9-ci-check.log`).
Live (docker-desktop, host `logweir-api` in localAdmin + Playwright, shared lab controller at
`1a9aca6` as the only reconciler, zero harness status writes):
`claude/artifacts/plat10-ui/lw-p10-20260922t182514z/live.json`, 17 journeys / 7 controls PASS.
PLAT-10.1: selected-topic and all-user-topic creation, edit (g1→g2, a real run after the edit
freezes g2, the earlier run keeps g1), invalid cron (the 422's own sentence on screen, draft
kept), readiness failure (a real terminal `notReady` Preflight), keyboard-only completion (0
pointer events, 2px focus ring asserted) and first-run redirect — all PASS. PLAT-10.2: empty
history, running, failed and verified runs, unavailable-beside-healthy (`Missing` after deleting
only that point's manifest), catalog incomplete, paused and archived — PASS.
**BLOCKED:** "each recovery-point row has its own Restore", "navigation to an older backup" and
the done-evidence journey create → backup → detail → restore: no destination-backed run on this
build gets `windowCovered`, because the D2 §3.9 evidence-fetch Job was never built
(EVIDENCE-FETCH-JOB-UNBUILT, in progress on `claude/evidence-fetch`), so the console correctly
offers no Restore. Next: lab-refresh-8 with the evidence-fetch controller, then re-run
`scripts/plat10-ui-e2e.mjs` with an `ArchiveReadGrant` destination and an Approval minted with
the rotated lab approver key → Done records. Class sweep owed: `scripts/plat11-2-ui-e2e.mjs` and
`scripts/d2w13-ui-e2e.mjs` reload after actions and could hide an in-place re-render defect.

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

**Completion record — Done (2026-09-22), PLAT-11.2.** Source landed on main as
`claude/plat11-2` (`..d5090ed`; review `claude/plat11-2.review.md` ACCEPT-WITH-FIXES with two
HIGH — a scratch-mode prefix that desynchronised the preview from the run, and an untested
declaration path with no submitted wizard restore — then `claude/plat11-2.review-2.md`
ACCEPT-WITH-FIXES on the evidence, all applied; seventeen API and console mutants killed).
The wizard's audit found three of the eight tests already covered (timestamp boundary;
collision and invalid prefix server-side), two displayed but unenforced (target change,
stale preflight) and three missing (subset, duplicate mapping, failed retry), and none
needing the controller, CRD or runner, because the subset lives in the plan bytes. Landed:
subset selection from the selected point's FROZEN topic list; the saved target; the exact
new-name mapping per topic shown before submit, with one prefix state driving the preview,
the declaration and the request (the runner's per-mode rule in `effectivePrefix`; the API
rail refuses `scratch` with `unsupported_for_mode` because the route never parses the plan
the run reads), so the request equals the preview byte for byte — pinned by a node row over
the delivery seam, an API row, and the live wizard submit whose created object is read
back with `kubectl` and compared to the preview (`spec.planBytes` byte-identical, the
on-screen hash equal); duplicate mapping refused on the keystroke and by the API with a
422 naming both rows; invalid prefix refused by name; the recovery limits — target
replication/partition counts from the manifest, the sampled verification scope (never
"exhaustive"), the consumer-cutover limitation, "resume is not implemented" — each from a
contract constant pinned by a fixture; collision refused at the gate (`MappedTopicExists`
from the readiness route against a topic pre-created on the lab's target broker); a stale
or invalidated preflight refuses the submit, and a target swap marks the verdict stale and
refuses until the preflight is re-run (D2 §6.3/§6.6); a failed Restore retries to a fresh
target as a NEW Restore with a new prefix, a new plan hash and new names, the old approval
reference never sent (the controller's `PlanHashMismatch` is the backstop) and the old
Restore untouched — PLAT-12.2's retry identity; the point's window shown and a selection
outside it refused by name. Every listed test PASSES live on docker-desktop in localAdmin
console mode against the lab (`scripts/plat11-2-ui-e2e.mjs`: fifteen journeys, ten
negative controls each requiring its refusal, one honest NOT REACHED; the reviewers
re-drove the collision, duplicate, retry and wizard-submit steps in their own namespaces):
**subset restore** — an OLDER point, one of two topics unticked, the plan bytes carrying
exactly the other; **duplicate mapping**; **collision**; **invalid prefix**; **target
change**; **stale preflight**; **failed retry** — a real `Failed/ApprovalSubjectMismatch`
retried to a fresh prefix and two new minted names; **timestamp boundary**. Acceptance:
the submitted topics and mapping equal the preview, and no existing target topic is
overwritten by the ordinary path — proved by the byte comparison and the collision refusal.
Migration: approved byte identity is preserved after review (the reviewed-plan hash guard);
a material edit produces a new plan hash and needs a new approval where the namespace is
governed; resume is identified as unimplemented on screen. Residue, none of it this task's
acceptance: the console's own step-5 readiness check on this build ends
`ArchiveUrlUnreadable` because the Backup projection publishes no `destinationRef`
(BACKUP-PROJECTION-NO-DESTINATION, defect table — the collision refusal was proved through
the readiness route directly); a verified scorecard for a wizard-created Restore needs an
approver key the lab does not hold (the Restore ends `Pending/ApprovalNotVerified`, no
Job); the wizard's own collision journey is NOT REACHED for the same projection reason.

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

**Completion record — Done (2026-09-23), PLAT-12.1.** Source on main (the guided submit, PLAT-11.2/13.2 flows, and `claude/plat19-2`'s policy routing, integrated in `ac00819`). Proven live on lab-refresh-9 (lab at main `306cebf`: controller `sha256:f71fdcb4…`, runner `sha256:db8d8ade…`; report `claude/lab-refresh-9.result.md`; artifacts `claude/artifacts/lab-refresh-9/` and `claude/artifacts/{d2,d3}-live/lr9*`): `scripts/plat12-13-ui-e2e.mjs` 20/20 (success redirect, double click, lost response, browser refresh, API rejection) and the composed duplicate-submit and selected-point journeys (`e2e/journeys`, 12 PASS / 0 FAIL); approval-required submission routed by the frozen policy — Ordinary to execution and admitted by the controller, Governed to Awaiting approval, an unbound namespace to legacy.

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

**Live validation (2026-09-23, lab-refresh-8, lab at main `f49849d`; `claude/lab-refresh-8.result.md` §4–§5) — stays In progress.** Proven by the console journey `scripts/plat12-13-ui-e2e.mjs` 20/20: standalone approvals page, edited subject and route mismatch refused, pasted private key refused, an unsigned approval refused by weirkeeper, a forged subject binding refused by the controller. The owed verified-approval route FAILED on CONSOLE-APPROVAL-VERIFIED-SUBJECT-UNMAPPED (fixed on `claude/plat19-2`, merging in `claude/integration-2`); expiry on the page and retry identity were not re-run. Next: re-run the PLAT-10 journey's approvals row after integration.

**Live validation (2026-09-23, lab-refresh-9 (lab at main `306cebf`: controller `sha256:f71fdcb4…`, runner `sha256:db8d8ade…`; report `claude/lab-refresh-9.result.md`; artifacts `claude/artifacts/lab-refresh-9/` and `claude/artifacts/{d2,d3}-live/lr9*`)) — stays In progress.** The verified-approval route now PASSES (CONSOLE-APPROVAL-VERIFIED-SUBJECT-UNMAPPED closed live); standalone page, edited/forged subject, route mismatch, pasted key and retry identity proven. Remaining: the approval page's expiry rendering (row being written on `claude/harness-rows-12`).

**Completion record — Done (2026-09-23), PLAT-12.2.** Source on main (subject binding, fresh-target retry `claude/plat11-2`, the verified-approval mapping fix in `ac00819`). Proven live: lab-refresh-9 — standalone approvals route, edited/forged subject and route mismatch refused, pasted private key refused, unsigned approval refused by the controller, the controller-verified Approval rendered for exactly its Restore, retry identity to a fresh target; and harness-rows-12 on the lab at main `306cebf` (`claude/harness-rows-12`, merged `8569682`; report `claude/harness-rows-12.result.md`; artifacts `claude/artifacts/harness-rows-12/`), each row with a negative control that requires its outcome: an Approval past its key's expiry renders expired and never verified (`scripts/plat12-13-ui-e2e.mjs` 21/21; control: the page read verified before the key expired). Residue (LOW): an expired authorization *document* renders "refused" rather than "expired" (`ui/pages/approvals.js:422`); never green.

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

**Live validation (2026-09-23, lab-refresh-8, lab at main `f49849d`; `claude/lab-refresh-8.result.md` §4–§5) — stays In progress.** Proven: stream disconnect (`scripts/d3-ui-e2e.mjs` journey 17 ceiling, 17/17), refresh (journey 2), verification downgrade (`trust-revocation-flips-a-terminal-badge`, Valid → Untrusted on a terminal Backup), completed-Job cleanup for the evidence-fetch Job (`EVF-1.ttl`), and the Restore `Admitted` carry to terminal. Missing: committed rows for mount failure, unschedulable pod and engine crash; `Restore.status.completion` is never written (RESTORE-COMPLETION-UNWRITTEN).

**Live validation (2026-09-23, lab-refresh-9 (lab at main `306cebf`: controller `sha256:f71fdcb4…`, runner `sha256:db8d8ade…`; report `claude/lab-refresh-9.result.md`; artifacts `claude/artifacts/lab-refresh-9/` and `claude/artifacts/{d2,d3}-live/lr9*`)) — stays In progress.** Proven: mount failure ×2 (`VolumeMountFailed`), unschedulable pod (`PodUnschedulable`), engine crash, verification downgrade, stream disconnect, refresh; `status.completion` only beside a Valid verdict. Remaining: an observed TTL collection of a finished Job on this build (`claude/harness-rows-12`).

**Completion record — Done (2026-09-23), PLAT-14.1.** Source on main (D3 W11/W12 operation states; WARNING-DIAGNOSTICS-NOEXITCODE `b57753b`; RESTORE-COMPLETION-UNWRITTEN `fa3384e`). Proven live: lab-refresh-9 — mount failure ×2 (`VolumeMountFailed`), unschedulable pod (`PodUnschedulable`), engine crash, verification downgrade, stream disconnect and refresh (d3 console journeys 17 and 2), `status.completion` only beside a Valid verdict; and harness-rows-12 on the lab at main `306cebf` (`claude/harness-rows-12`, merged `8569682`; report `claude/harness-rows-12.result.md`; artifacts `claude/artifacts/harness-rows-12/`), each row with a negative control that requires its outcome: completed-Job cleanup — an evidence-fetch Job with a 600 s TTL collected ~3 s after it was due, before any cleanup ran (control: a 7-day-TTL Job survived until cleanup).

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

**Live validation (2026-09-23, lab-refresh-8, lab at main `f49849d`; `claude/lab-refresh-8.result.md` §4–§5) — stays In progress.** Proven, 12/12 rows: stale point, repeated-failure dedup, recovery notification exactly once, unavailable archive, sampled-vs-complete labelling, and refused-to-sound resolving exactly once (its window race fixed). Missing: a row for notification transport failure.

**Completion record — Done (2026-09-23), PLAT-14.2.** Source on main (D3 W6 and fixes). Proven live on lab-refresh-9 (lab at main `306cebf`: controller `sha256:f71fdcb4…`, runner `sha256:db8d8ade…`; report `claude/lab-refresh-9.result.md`; artifacts `claude/artifacts/lab-refresh-9/` and `claude/artifacts/{d2,d3}-live/lr9*`): stale point, repeated-failure dedup, recovery notification exactly once, unavailable archive, sampled-vs-complete labelling (protection-cases + protection-verdicts, 12 rows, `d3-live/lr9pp20260923t1828z/`), and notification transport failure (`notify-transport-failure-is-recorded-and-rewrites-nothing`, run alone: 3 attempts at the 60 s/300 s backoff, `DeliveryFailed`, no Backup rewritten).

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

**Live validation (2026-09-23, lab-refresh-8, lab at main `f49849d`; `claude/lab-refresh-8.result.md` §4–§5) — stays In progress.** L6 step 1 PASS (a governed standing Approval verifies: REHEARSAL-STANDING-APPROVAL-NEVER-VERIFIES closed live) and step 10 PASS (a retired key authorises nothing); step 2 FAILED on REHEARSAL-PLAN-AUTH-PLAINTEXT, so steps 3–9 and 7b (overlapping drill, cleanup failure, evidence retention, skipped slot) were not reached; the missing-point row was NOT-REACHED behind CATALOG-TRUST-ROSTER-ONLY. Both fixes are on `claude/rehearsal-catalog-trust` (review pending). Missing rows: unavailable target, failed verification.

**Completion record — Done (2026-09-24), PLAT-14.3.**
- **Source on main:**
  - `claude/rehearsal-fix`, merged `707343b`: REHEARSAL-PASS-RECORDED-AS-FAILED, FAILED-DRILL-EVIDENCE-UNPUBLISHED and CONSOLE-COMPLETION-ON-FAILED-RESTORE; Tier-A review REJECT (M1), then a fix round.
  - `claude/reserve-commit`, merged `86a554e`: REHEARSAL-FIRE-PASS-STATUS-LOST, with the class sweep (preflight `CheckPlanConflict`, backup `JobNameConflict`); Tier-A review ACCEPT-with-LOWs, L1 fixed, L3 mutants killed.
  - Earlier: the PLAT-14.3b standing authorization, and the REHEARSAL-PLAN-AUTH-PLAINTEXT and REHEARSAL-SKIP-DEFERS-SLOT fixes.
- **Proven live on docker-desktop:**
  - lab-refresh-11 at main `86a554e`: report `claude/lab-refresh-11.result.md` §4–§6; artifacts `claude/artifacts/d3-live/lr11*`, `claude/artifacts/lab-refresh-11/`.
  - lab-refresh-10 at `b426096`: report `claude/lab-refresh-10.result.md`.

  Every tracker test:
  - **missing recovery point:** `rehearsal-refused/revoked-point-is-never-selected` (`NoQualifyingPoint`), on lab-refresh-9's `306cebf`. `select_point` and `candidates` are unchanged since (`git diff 306cebf 86a554e -- crates/weirkeeper/src/rehearsal.rs` is empty).
  - **unavailable target:** `rehearsal-unavailable-target-skips-and-consumes-the-slot`, on `86a554e`.
  - **approval policy:** L6 step 1 (standing Approval Verified), step 10 (the retired-key arm reaches no Job), and `Authorized=True`.
  - **overlapping drill:** L6 steps 7 and 7b (`ConcurrencyBlocked`; the skipped slot is consumed, never fired late), plus `ConcurrencyBlocked` while a verdict is owed.
  - **failed verification:** `rehearsal-failed-verification-is-a-failed-rehearsal` (exit 2, `fail-integrity`, three signed evidence keys, verification `Valid`, `lastFailed`, never `lastSucceeded`, the console shows it failed).
  - **cleanup failure:** L6 step 8 (the leftover guard skips `LeftoverTopics`).
  - **successful evidence retention:** L6 steps 5, 6 and 9. Step 5 passed on four runs across the two refreshes: `lastSucceeded {restoreRef, at, evidence = the scorecard key, rtoSeconds}` written after the green verdict, with `RehearsalHealthy=True`. Step 6: unrelated topics survive, and the rehearsal's topics are gone after teardown. Step 9: the signed objects are still fetchable after the Job's TTL.
- **Bounds proven live:**
  - a rehearsal Restore deleted while its verdict is owed is recorded `RestoreDeleted` (×2, and the L1 variant);
  - a verdict still owed after the one-hour wait is recorded `EvidenceVerdictNotReached` (3,624 s after the run finished), and a verdict that arrives later is not promoted.
- **Unit and kube-mock only:** the five-minute unrecorded-verdict arm, and the in-pass Backup freeze→POST-409 interleave.
- **Follow-ups, none a tracker test:** REHEARSAL-LASTSUCCEEDED-POINTID-UNWRITTEN, REHEARSAL-RECOVERY-LOG-NOISE, RESTORE-STALE-CACHE-409-WARN.
- **Gates:** `scripts/ci-check.sh` rc 0 at `86a554e` (`claude/main-reservecommit-ci-check.log`), and CI run 35957294926.

**Live validation (2026-09-24, lab-refresh-10 (lab at main `b426096`: report `claude/lab-refresh-10.result.md`; artifacts `claude/artifacts/d3-live/lr10*` and `claude/artifacts/lab-refresh-10/`)) — stays In progress.**
- **All seven tracker tests are now proven live.** Six are proven on `b426096`: unavailable target, approval policy, overlapping drill (steps 7 and 7b), failed verification (exit 2, `fail-integrity`, signed evidence `Valid`, `lastFailed`), cleanup failure, and successful evidence retention (steps 6 and 9). The seventh, missing recovery point, was proven on lab-refresh-9's `306cebf`, in code this refresh doesn't change.
- **L6 step 5 passed twice:** `lastSucceeded` is written after the green verdict, with `RehearsalHealthy=True`. REHEARSAL-PASS-RECORDED-AS-FAILED and FAILED-DRILL-EVIDENCE-UNPUBLISHED are CLOSED-LIVE.
- **Blocked on REHEARSAL-FIRE-PASS-STATUS-LOST (row e):** `activeRestoreRef` is never written live, so a rehearsal deleted while its verdict is owed goes unrecorded, and `Authorized` stays `NoResult`. The fix is on `claude/reserve-commit`.
- **Not run:** `EvidenceVerdictNotReached`, which needs a 35-minute wait. It runs at lab-refresh-11.

**Live validation (2026-09-23, lab-refresh-9 (lab at main `306cebf`: controller `sha256:f71fdcb4…`, runner `sha256:db8d8ade…`; report `claude/lab-refresh-9.result.md`; artifacts `claude/artifacts/lab-refresh-9/` and `claude/artifacts/{d2,d3}-live/lr9*`)) — stays In progress.** Proven: missing recovery point, unavailable target, approval policy (steps 1, 10), overlapping drill (7 + 7b: `ConcurrencyBlocked`, slot consumed), cleanup failure (step 8), evidence retention (6, 9). Blocked on two new defects: REHEARSAL-PASS-RECORDED-AS-FAILED (step 5) and FAILED-DRILL-EVIDENCE-UNPUBLISHED (failed verification) — both on `claude/rehearsal-fix`.

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

**Live validation (2026-09-23, lab-refresh-8, lab at main `f49849d`; `claude/lab-refresh-8.result.md` §4–§5) — all six listed tests PROVEN on this build; held from Done.** Missing/corrupt manifest, duplicate identity, large catalog, partial access, stale index, schema-version compatibility and reconstruction after CR loss with a complete `receiptKey` (CATALOG-RECEIPTKEY-REDACTED closed live), 13/13 rows. Held because CATALOG-TRUST-ROSTER-ONLY sits in this task's verification material: the view ignores `TrustPolicy`, so a key revoked only in a policy would still read Verified. Done once that fix (on `claude/rehearsal-catalog-trust`) is proved live.

**Completion record — Done (2026-09-23), PLAT-15.1.** Source on main (D3 W3/W8/W11, CATALOG-RECEIPTKEY-REDACTED, CATALOG-TRUST-ROSTER-ONLY `2cb04c7`, TRUST-VALID-BASIS-CLASS `7b0277b`). Proven live on lab-refresh-9 (lab at main `306cebf`: controller `sha256:f71fdcb4…`, runner `sha256:db8d8ade…`; report `claude/lab-refresh-9.result.md`; artifacts `claude/artifacts/lab-refresh-9/` and `claude/artifacts/{d2,d3}-live/lr9*`): the 13 catalog rows — missing/corrupt manifest, duplicate identity, large catalog, partial access, stale index, schema-version compatibility and reconstruction after CR loss with a complete `receiptKey` — and the trust joins: `backupVerdict` Invalid/Untrusted, the view judged by the namespace's TrustPolicy, `TrustAvailable` naming the policy (`d3-live/lr9rr20260923t1800z/`).

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

**Live validation (2026-09-23, lab-refresh-9 (lab at main `306cebf`: controller `sha256:f71fdcb4…`, runner `sha256:db8d8ade…`; report `claude/lab-refresh-9.result.md`; artifacts `claude/artifacts/lab-refresh-9/` and `claude/artifacts/{d2,d3}-live/lr9*`)) — stays In progress.** Proven: CR loss (0 CRs, 100 records restored), untrusted signer refused by the Preflight (`CatalogPointSignerUntrusted`) and the runner (exit 3 `PointUntrusted`, no topic), unsigned receipt, forged binding, repeated import, storage denial, source offline, P10 by a real API server. Remaining on the catalog path: a Retired-key point, and an incomplete point as its own row (`claude/harness-rows-12`).

**Completion record — Done (2026-09-23), PLAT-15.2.** Source on main as `claude/plat15-2` (integrated `ac00819`; two review rounds incl. the runner's receipt-signature check and the Preflight trust re-check). Proven live: lab-refresh-9 — CR loss (0 CRs, 100 records restored), source offline, untrusted signer refused by the Preflight (`CatalogPointSignerUntrusted`) and by the runner (exit 3 `PointUntrusted`, no topic), unsigned receipt, forged binding (`PointBindingMismatch`), storage denial, repeated import, P10 by a real API server; and harness-rows-12 on the lab at main `306cebf` (`claude/harness-rows-12`, merged `8569682`; report `claude/harness-rows-12.result.md`; artifacts `claude/artifacts/harness-rows-12/`), each row with a negative control that requires its outcome: old signer — a catalog point signed by a Retired key is listed historical and its Restore succeeded with exactly the source's 100 records; incomplete point — missing receipt signature or receipt is not selectable and is refused `CatalogPointNotSelectable` (control: selectable again once the files return). Migration: release notes item 5.

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

**Live validation (2026-09-23, lab-refresh-8, lab at main `f49849d`; `claude/lab-refresh-8.result.md` §4–§5) — stays In progress.** Proven: dry preview, denied deletion, active restore (×3), legal hold, protected points survive, last usable point, partial failure, policy change, wrong-prefix rejection, attributable record and digest, unattributable refused, bounded retry and its resume on a spec change, scheduled backups continue, the unreadable-point skip, the enforcer in the image, and U6's `evidenceWrite` projection (9/9). Two listed tests are not proven: the provider object-lock half of "legal hold/lock" (`retention-legal-lock`: `object_store` 0.14 has no WORM readback, so the product records `ProviderEnforcedUnverified`; a lock-enabled MinIO bucket row proving the provider refusal is recorded is owed) and "shared segment" through the controller (`retention-shared-segment`: catalog view entries carry no segment keys, so the guarantee honestly reads `NotEnforced`; either the view carries segment keys or the archive format's segment sharing is shown impossible, with evidence).

**Completion record — Done (2026-09-23), PLAT-16.2.** Supported mode: the isolated Logweir retention worker (`logweir-retention`), preview by default, Enforce behind an approved plan digest. Source on main incl. `claude/ctl-batch-2` (`b57753b`: SHARED-SET-RETENTION, OBJECT-LOCK-DELETE-MARKER). Proven live on lab-refresh-9 (lab at main `306cebf`: controller `sha256:f71fdcb4…`, runner `sha256:db8d8ade…`; report `claude/lab-refresh-9.result.md`; artifacts `claude/artifacts/lab-refresh-9/` and `claude/artifacts/{d2,d3}-live/lr9*`): dry preview, denied deletion, active restore (×3), legal hold, last usable point, partial failure, policy change, wrong-prefix rejection, attributable record and digest, bounded retry and resume, scheduled backups continue; the lock half — on a versioned or Object Lock bucket Enforce deletes nothing, names every point `VersionedBucket`, degrades with the remedy and writes 0 delete markers, incl. the plain-then-versioned arm (`lock/03-run.json`); shared segment through the controller — two receipts over one set both kept `SharedSegment`, no plan line (`d3-live/lr9pp20260923t1828z/shared-set/evaluation.json`); the enforcer's minimal grant measured (U6 9/9, `VersionProbeRefused` without `s3:GetObject`). Accepted residual RET-VERSIONED-SUSPENDED-REWRITE is documented. Migration: release notes items 1–3 (`docs/release-notes.md`).

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

**Completion record — Done (2026-09-21), PLAT-17.1.** The bounded product endpoints landed
as D0 stages 1 and 3 (`4b571d1`..`de0207c`), the D1/D2/D3 route families (`72751ab`,
D2 W12, `0202648`..`0ca3386`), SSE (`0202648`, PLAT-14.1's stream), transient-check
cancellation (D2 W9/W12), `POST …/backups` (D1 W6), and every listed test and both
acceptance clauses were proven live through the API on lab-refresh-3 (record above). The
packaging stage — D0 stage 7 — landed as `bdcc721`..`4ab14e1` (`claude/plat17-1-stage7`):
`Dockerfile.console` builds `logweir-console` (`logweir-api` plus the twenty-two `ui/`
files at `/ui`, non-root `65532`, no `CMD`, the six-check `scripts/check-image-api.sh`
asserting the binary names itself and serves exactly the source bytes); the chart's
`api.console.*` block (D0's `console.*` nested under the chart's existing `api.*`
principal from D3 W13 — the reconciliation is stated in `charts/logweir/README.md` and in
D0's 2026-09-21 amendment) renders the Deployment (probes in shared mode, resources,
`securityContext`, the `<release>-api` ServiceAccount with its bound token, an immutable
content-addressed config ConfigMap that carries no credential — key, client secret and
TLS are Secret NAMES the chart never generates), a conditional PDB, the Ingress (TLS Secret
required), the NetworkPolicy (ingress from the configured controller pods on the console
port only; egress to DNS, the Kubernetes API and the OIDC CIDRs), and D3 W13's
per-namespace RoleBindings unchanged. Enabling the console forces the mode: `enabled`
without `mode` is refused at render time by name; **shared** is the only mode that renders
a Service or Ingress and is refused without HTTPS `publicBaseUrl`, TLS, or with an ingress
host that differs from `publicBaseUrl`'s authority; the **in-cluster administrator mode**
(`localAdmin`) binds loopback, renders nothing another pod can dial, is reached with
`kubectl port-forward deploy/<release>-api`, and its authorization surface is the
port-forward permission itself — stated in the README, `docs/install.md` §5e and
`docs/api.md`, with the residual-O1 disclosure D0:204 requires (the console's session and
cursor keys sit inside `weirkeeper`'s Job-create authority until D0 stage 5). Live on
docker-desktop under the lock (`claude/artifacts/plat17-1-stage7/20260921t172502z/`, image
`sha256:65e57809…`, revision-labelled, applied from the chart's own render into an
isolated namespace with explicit `mode: localAdmin`): the 85-row `kubectl auth can-i`
matrix for the ServiceAccount — reads of the console kinds in bound namespaces allowed,
an unbound namespace, Job and Secret creation and Secret reads all refused; a 20-probe
smoke through port-forward; 22/22 served bytes equal to the source; the ClusterIP that
was rendered that day unreachable from inside the cluster (HTTP 000 — the observation
that removed the Service from administrator mode). Gates: chart_lint 40, manifest_lint,
workflow_lint, doc_lint, `just chart-check`, `just crds-check`, render-install, `just
lint`, links; six planted chart/lint mutants killed (no securityContext, a credential in
the ConfigMap, shared mode without TLS, an ingress host mismatch, `enabled` without a
mode, a Service in administrator mode). Review `claude/plat17-1-stage7.review.md`
ACCEPT-WITH-FIXES (F1 the O1 disclosure, F2 the host check, F3 a stale kubeconfig
sentence, F4 the default mode — decided as above and amended into D0, F5 wording), all
fixed in `fced88a`..`4ab14e1`; the lean loop's single pass. Residue, none of it this
task's acceptance: shared mode has never run live in-cluster (no OIDC provider, TLS
ingress, session or browser — D0 stage 8, PLAT-17.2); NetworkPolicy deny behaviour needs an
enforcing CNI (D0's own wording); the console keys inside the controller's Job-create
authority until D0 stage 5 (PLAT-17.2 — shared mode is not declared secure); D0's `policy
bindings` and `capability flags` values have no config field (PLAT-19.2); CSP and the
security headers were not re-probed on the deployed image (one `curl -I` in stage 8's
harness); the live image is `linux/arm64` (this host cannot cross-build; CI's native
matrix publishes amd64 — the next batch lab refresh runs `scripts/check-image-api.sh` per
architecture); the shared lab release runs no console pod, and a second full-chart
release is impossible on one cluster while the chart's ClusterRoles carry fixed names —
**decided:** the roadmap accepts one full release per cluster (release-scoping those names
would break existing installs) and components are proved from the chart's render as here;
Helm's own install/upgrade lifecycle for the component and `bash scripts/ci-check.sh` were
not run by the worker (the merge gates run the workspace); the `fsGroup`/mode check the
console's key mount needed is owed as a class sweep over the other Secret volumes
(defect table, SECRET-VOLUME-MODE-SWEEP). Migration: additive — `api.console.enabled`
defaults to false; `ui.enabled` and `Dockerfile.ui` are byte-unchanged; rollback is
disabling the value.

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

**Completion record — Done (2026-09-24), PLAT-17.2.**
- **Where:** the PoC install on docker-desktop (v1.34.1), round `claude/poc-install` (merged `7beb7c8`; report `claude/poc-install.result.md` §7; artifacts `claude/artifacts/poc-install/final/p172/`, `final/governed/`, `final/g6-restart.log`, `final/checks.log`).
- **What was installed:**
  - a real TLS ingress: Traefik 41.6.0 with a cert-manager v1.21.2 local CA;
  - a real OIDC provider: Dex 0.24.1, static users bound by subject;
  - the scoped chart install from the **published** chart `0.1.0-sha-86a554e6…`, with images `weirkeeper@sha256:7dc60dd7…`, `logweir-console@sha256:5c45eedb…` (2 replicas) and runner `logweir@sha256:0aba7749…`.
- **Harness:** `scripts/live/poc/p172_ingress.py` — `phase1`, `save`, then `expired` more than 930 s later.
- **Every tracker test passed on the real entry point:**
  - phase1 **58/58**: TLS, the redirect and the security headers; sign-in ×4 with a `__Host-`/`Secure`/`HttpOnly`/`Path=/` cookie and no `Domain`; the role matrix, with object counts before and after every denied mutation.
  - The operator is refused approval submission (403); the trust read is administrator-only.
  - Unauthorized namespaces answer a byte-identical 404.
  - Forged `X-Remote-*`, `X-Forwarded-User` and `X-Auth-Request-*` headers are ignored, and `Impersonate-User` is refused.
  - CSRF ×5, plus a non-JSON content type; no CORS headers; a garbled cookie is refused.
  - Unauthenticated API ×4 and SSE are refused; SSE across namespaces is 404.
  - The legacy proxy paths are 404, and no `logweir-ui` is deployed.
  - Audit attribution is recorded on all four kinds, with 0 secrets in 1.6 MB of console logs.
  - Expired session **3/3** (refused at 1,205 s; the control accepted at 859 s). No-role user **5/5**.
  - Negative control: the same refused mutation is admitted WITH the token and the Origin.
  - G6: the trusted set follows a Traefik rollout, and another namespace's pod gets 421.
  - Governed separation of duties: requester and approver recorded as different principals.
- **Known limit, accepted as before:** docker-desktop does not enforce NetworkPolicy. The policies are applied, and the probe records the non-enforcement.
- **Not PLAT-17.2 acceptance, tracked separately:** console defects P1–P8 and P9 (rows POC-*).

**Live validation (2026-09-23, lab-refresh-9 (lab at main `306cebf`: controller `sha256:f71fdcb4…`, runner `sha256:db8d8ade…`; report `claude/lab-refresh-9.result.md`; artifacts `claude/artifacts/lab-refresh-9/` and `claude/artifacts/{d2,d3}-live/lr9*`)) — stays In progress.** Proven on `306cebf`: role matrix, forged headers, unauthorized namespace, CSRF, unauthenticated API and stream, the trusted-entry 421, audit attribution, denied mutation (32/32), `auth can-i` 77/77, the scoped controller with 0 refused calls. Remaining: a real ingress with TLS and a scoped chart install (both proven by the PoC install on docker-desktop: Traefik + cert-manager + Dex, `claude/chart-poc`), and an expired-session row (`claude/harness-rows-12`). NetworkPolicy deny cannot be measured on Docker Desktop (no enforcing CNI); the policies are rendered and render-tested.

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

**Design standard (owner decision, 2026-09-22): adopt the VMware Clarity design
language.** The console is an enterprise operations product, so its visual and
interaction system follows [Clarity](https://github.com/vmware-clarity):
Clarity's design tokens (color, typography, spacing, elevation, light/dark
themes), component anatomy and states (datagrid with filter/sort/pagination,
forms and validation messages, alerts and banners, wizard, modals, tabs, stack
views, badges/labels, signposts) and its accessibility patterns (focus order and
visible focus, ARIA roles, live-region announcements, contrast). It is adopted as
a **design language implemented in the existing static console**, not as a
runtime dependency: `vmware-clarity/core` (the framework-agnostic web
components, MIT) was archived upstream in February 2026 and receives no fixes,
and `vmware-clarity/ng-clarity` is Angular-only, which would be a framework
rewrite (see the rule above and `product-expansion.md`'s deferral of a frontend
framework migration). Tokens are defined once in `ui/style.css` (CSS custom
properties named after Clarity's token set) and every page consumes them; no page
hard-codes a color, size or spacing. **Additional acceptance:** a Clarity
conformance checklist per page (token use, component anatomy, states, keyboard
and screen-reader behaviour) recorded with screenshots in light and dark themes
and at small-screen widths; the offline gate (`scripts/check-ui-offline.sh`), the
UI image contents and every deep link unchanged; any Clarity asset copied into
the tree (for example token values or icons) carries its MIT notice in
`THIRD_PARTY_NOTICES.md`. **Additional tests:** a lint that fails on a literal
color/spacing value outside the token file, and the existing node suites plus a
Playwright visual/a11y pass over every primary route.

**Completion record — Done (2026-09-23), PLAT-18.2.** Source on main as `claude/plat18-2` (`9d638cc`) with the Clarity design language (tokens from `vmware-clarity/core` v6.17.0, MIT), follow-ups `8485665`, `691f016`. Proven live on lab-refresh-9 (lab at main `306cebf`: controller `sha256:f71fdcb4…`, runner `sha256:db8d8ade…`; report `claude/lab-refresh-9.result.md`; artifacts `claude/artifacts/lab-refresh-9/` and `claude/artifacts/{d2,d3}-live/lr9*`) (host API from main): 7 journeys PASS (`ui/plat18-2/lw-p182-20260923t163251z/branch/live.json`) — keyboard-only configure and restore, axe-core WCAG 2.1 AA over 72 route visits in light/dark/phone widths with 0 findings, loading/empty/error states 12/12, large inventory/history (history 1,000 rows first paint 118→60 ms; wizard 2,000 topics 140→8 ms; no framework or virtualization needed); every live UI harness passes on the new markup. Deep links unchanged.

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

**Completion record — Done (2026-09-21), PLAT-19.1.** Source landed on main as D3 W1
(`64fcd38`..`5fc1a72`: the trust lifecycle core, the `TrustPolicy` controller, `trust
export|migrate-roster`, the synthesised `legacy-roster-v1`), D3 W10 (`b3aed6f`..`3eb7897`:
verification and approval admission through the namespace's resolved trust, `Untrusted`
with `signedAt` and `trust.{basis,keyState,policy}`, the re-trust pass on a policy event),
the trust self-heal for pre-`signedAt` objects (`d8d3479`, TRUST-UPGRADE-SIGNEDAT), the exact
expiry timer (`532740d`/`efa16b2`, TRUST-EXPIRY-LAG), D3 W11's keys route and D3 W12's keys
view (`..de18b45`, `ui/pages/keys.js`). Every listed test is proven live on docker-desktop
from `e2e/k8s/d3` under the tracker's own names (harness-rows-4..7; lab builds `d387f87`
and `af64073`; report `claude/harness-rows-7.result.md` §2, review ACCEPT): **overlap** —
`trust-overlap`, the same terminal Backup `Valid` under the synthesised roster and under the
explicit policy that overlaps it, same key, basis `Current`; **retirement** —
`trust-rotation-old-evidence-still-verified`, `Valid` with basis `Historical`, keyState
`Retired`; **revocation** — `trust-revocation-flips-a-terminal-badge`, a `KeyCompromise`
revocation flips a terminal Backup to `Untrusted` on the next policy event with `phase`,
`exitCode` and `Complete` untouched; **unauthorized update** —
`trust-unauthorized-update-is-refused-by-rbac`, a non-admin subject's edit refused by RBAC
(not only CEL under cluster-admin); **old archive** —
`trust-old-archive-survives-its-signer-retiring` plus its restore half (`68eff22`: a Restore
from the archive signed by the since-retired key proceeds on the `Historical` basis while a
new Backup signed by that key is `Untrusted`); **multiple namespaces** —
`trust-two-namespaces-resolve-their-own-policies`, two explicit policies, one key trusted in
one namespace and `Invalid` where unlisted; **upgrade from the default roster** —
`trust-upgrade-from-default-roster`, objects judged under `legacy-roster-v1` still `Valid`
under the explicit policy that replaces it, and the five pre-`signedAt` lab objects healed
in place (lab-refresh-4); **unknown/stale expiry** — the console half: the keys view renders
`unknown` for an absent or stale evaluation with four freshness reasons (`NoStatus`,
`GenerationBehind`, `Stale`, `NoServerClock`), the server clock as the only clock, and four
planted mutants that render `valid` all die (`ui/tests/d3.spec.js`; KEYSVIEW-ABSENT-VALID
closed; review `claude/d3w12-finish.review.md`), while the API half is pinned by
`trust-two-namespaces…` (`Invalid` where the key is not listed) and
`trust-old-archive…` (`Untrusted` after retirement) and by the expiry timer's live row (the
refusal landed 4 ms after `notAfter`, lab-refresh-5). The first acceptance clause —
rotation permits new evidence and continued policy-correct verification of old evidence
without a blanket trust gap — is the overlap, retirement and old-archive rows read
together; the second is the keys view's rendering proof plus the evaluated half seen live
on a real `TrustPolicy` with a retired key in the D3 console journey
(`claude/artifacts/d3w12-live/`). Stated honestly: the `unknown` state itself was not
observed live — the CRD requires `notBefore`/`notAfter` on every key and the controller
evaluates a policy before a first read can land, so the window is shorter than a read; the
rendering is proved by fixture and mutant, which is what the acceptance sentence names. A
viewer's or operator's refusal on the keys page cannot be produced in localAdmin mode and
belongs to PLAT-17.2's shared-mode journey. Migration: an installation without a
`TrustPolicy` verifies exactly as before through the synthesised legacy roster; `trust
migrate-roster` and D3 §7 document rotation, retirement and revocation.

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

**Live validation (2026-09-23, lab-refresh-9 (lab at main `306cebf`: controller `sha256:f71fdcb4…`, runner `sha256:db8d8ade…`; report `claude/lab-refresh-9.result.md`; artifacts `claude/artifacts/lab-refresh-9/` and `claude/artifacts/{d2,d3}-live/lr9*`)) — stays In progress.** Proven: Ordinary v2 verified and admitted with the frozen policy and console key; Governed needs both signatures; self-approval 403; unbound namespace `legacy-governed-v1`; localAdmin never offers Ordinary. Remaining live rows: policy edit between verification and admission, document expiry, direct-CR writes of each failure, console-key retirement (`claude/harness-rows-12`).

**Completion record — Done (2026-09-23), PLAT-19.2.** Source on main as `claude/plat19-2` (integrated `ac00819`; two review rounds; D0 amended for the `{name, digest}` policy reference). Proven live: lab-refresh-9 — Ordinary v2 verified and admitted with the frozen policy and console key, Governed needs both signatures, self-approval 403, an unbound namespace keeps `legacy-governed-v1`, localAdmin never offers Ordinary; and harness-rows-12 on the lab at main `306cebf` (`claude/harness-rows-12`, merged `8569682`; report `claude/harness-rows-12.result.md`; artifacts `claude/artifacts/harness-rows-12/`), each row with a negative control that requires its outcome through a locked, byte-restored controller policy swap (`scripts/live/approval_policy_swap.py`): a policy edited after verification ends the held Restore `ApprovalPolicyMismatch` naming both digests; a document expiring while held ends `AuthorizationExpired`; eleven failure modes written directly with kubectl are each refused by name with no Job and no ConfigMap; a console key retired after verification is refused `KeyRetired` with no Job and no bundle (the bundle-time key check is unit-proven; the Approval controller refuses first). Migration: release notes item 7.

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

**Live validation (2026-09-23, lab-refresh-9 (lab at main `306cebf`: controller `sha256:f71fdcb4…`, runner `sha256:db8d8ade…`; report `claude/lab-refresh-9.result.md`; artifacts `claude/artifacts/lab-refresh-9/` and `claude/artifacts/{d2,d3}-live/lr9*`)) — stays In progress.** The full journey set 12 PASS / 0 FAIL, credential sweep clean, `--open lab-refresh-8` PASS. Remaining: `two-approvals` — its native row was never implemented (`claude/harness-rows-12`).

**Completion record — Done (2026-09-23), PLAT-20.1.** Source on main (`e2e/journeys`, `claude/plat20-1` integrated `ac00819`; `governed.py` from `claude/harness-rows-12`). Proven live: lab-refresh-9 — the full journey set 12 PASS / 0 FAIL with the credential sweep clean and its self-test killing, `--open lab-refresh-8` PASS; and harness-rows-12 on the lab at main `306cebf` (`claude/harness-rows-12`, merged `8569682`; report `claude/harness-rows-12.result.md`; artifacts `claude/artifacts/harness-rows-12/`), each row with a negative control that requires its outcome: `run.py run --open PLAT-19.2 --journeys two-approvals` PASS through the locked policy swap (control: FAILs with the swap disabled). The set covers SCRAM rotation, a new topic in a dynamic policy, overlap, two approvals, source offline and CR loss, stale namespace request, duplicate submit and old-point selection, each verifying archive data/evidence and durable resources. No new CI gate.

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

**Live validation (2026-09-24, the PoC install round, published chart and images at `86a554e`; report `claude/poc-install.result.md` §8) — stays In progress.**
- **Proven:**
  - (1) a clean install from an empty cluster;
  - (2) upgrades R1 (`v0.1.5`) and R2 (`sha-f49849d`), each with its rollback. Identities, schedules and archive readability were retained, and 17/17 receipts were verified independently;
  - (3) the recovery rehearsal through the console (backup Valid, restore Succeeded with completion, scorecards VALID), a catalog-verified point-bound restore, and a Governed restore approved by a second person;
  - (4) large-catalog measurements at 258 points (`docs/stability.md`);
  - (5) release-notes items 12–13 added.
- **Owed before Done:** re-prove on the next publication — console P2/P3/P4/P5/P8, P7 with D11, P9 and P10 — through the documented upgrade path. That publication is blocked on MINIO-IMAGES-WITHDRAWN.
- **Not reached:** 1,000 points (host emulation plus P10).

**Live validation (2026-09-24, the PoC upgrade round; PoC upgrade round `claude/poc-upgrade-1`, helm rev 5 to the published `sha-02dc44b6`; report `claude/poc-upgrade-1.result.md`) — stays In progress.**
- **Upgrade in place**, run exactly as README "Upgrade to a newer publication" says: the signing key, both schedules and every receipt were retained.
- **P1–P10 CLOSED-LIVE.** 51 rows PASS, 1 FAIL, 7 NOT RUN.
- README §10 was followed verbatim in the console, and journey J1–J7 PASS.
- **Owed before Done:**
  - P12, so that pre-upgrade legacy points verify and restore;
  - P11;
  - one more upgrade to the publication carrying `claude/console-ux-1` (the MCP round-1 fixes) and `claude/trust-revocation-durable`, with MCP round 2.
- **Residue:** 258 catalog points measured, not 1,000.

**Live validation (2026-09-25, PoC upgrade round 2 `claude/poc-upgrade-2`, helm rev 7 to the published `sha-b748fd5f`; report `claude/poc-upgrade-2.result.md`) — stays In progress.**
- **Upgrade in place:** identity unchanged; 314/314 receipts verified independently; 111 rows PASS.
- **Closed live:** P11, P12, trust durability, and the console fixes (functional half).
- **Owed before Done:**
  - P13/P14, found on the README §10 re-run;
  - MCP round 2.

**Live validation (2026-09-25, PoC upgrade round 3 `claude/poc-upgrade-3`, helm rev 7 → 8 → 9 to the published `sha-a54fb82385dc6740ecd0cee291ffb6d7de294e72`; report `claude/poc-upgrade-3.result.md`) — stays In progress.**
- **Publication:** chart digest `sha256:5becb4b9…`, CI run 36129705142, every job success.
- **Pre-upgrade KeyCompromise check:** empty.
- **Upgrade in place,** by README *Upgrade to a newer publication* verbatim:
  - the identity key, its private digest, the Secret uids and `logweir-signing-trust` were unchanged;
  - a 5-minute schedule fired 13/13 slots across both controller swaps;
  - **314 → 316 → 328 receipts, all VALID independently.**
- **Rows:** 97 PASS, 1 FAIL (P15, a new row), 8 NOT RUN.
- **Closed live:** P13/P14 (CLOSED-LIVE). README §10 was run in the console as a new user and is clean end to end; one stale sentence (D14) is fixed. J1–J7: 14/14.
- **MCP round 3:** verified the round-2 fixes (CONSOLE-MCP-ROUND2).
- **Owed before Done:** `docs/release-notes.md` still says the PoC reached "1,000+ points in one archive"; 258 were measured. `docs/release-handoff.md` still describes `306cebf`. The done-evidence clause needs both to be true. This landed as `claude/release-docs-final`, merged as `567a7b3e`; the record below follows.

**Completion record — Done (2026-09-25), PLAT-20.2.**

**Recorded by** the orchestrator, from `claude/poc-upgrade-3.result.md` §7 and the PoC rounds before it.

**Workers:**
- poc-install, poc-upgrade-1, poc-upgrade-2, poc-upgrade-3;
- poc-fixes-1…4;
- release-docs-final;
- the plat20-2 offline half;
- the orchestrator's MCP console rounds 1–3.

**Environment:**
- docker-desktop Kubernetes v1.34.1: an arm64 host, with the amd64 runner under emulation.
- Everything was installed with Helm from the **published** OCI chart `oci://registry-1.docker.io/vladyslavhaina/logweir-chart` and the Docker Hub images. There was no local build and no `kubectl patch`.
- Profile `deploy/poc/`: Traefik 41.6.0, cert-manager v1.21.2, Dex 0.24.1, and the console in shared mode behind Traefik with TLS and Dex SSO.

**Acceptance: "A new user follows one supported setup/recovery guide".**
- `deploy/poc/README.md` §10 was followed verbatim, in the console, as a new user, in every round.
- The final run at `a54fb823` is clean end to end:
  - connections made in the console, then Test connection and the destination's Test access;
  - a schedule created from the form, with readiness;
  - *Run first backup now*: `Valid`, with the badge;
  - *Restore this point* → readiness → Ordinary confirmation → `rst-mlxxlsbd…` `Succeeded`/`Valid` 150/150;
  - the topics on the target, and the scorecards independently `VALID`.
- One stale sentence (D14) was fixed on that round.
- J1–J7: 14/14 in each round.

**Acceptance: "upgrading retains identities, schedules and archive readability".**
- **Clean install** from an empty cluster at chart `0.1.0-sha-86a554e6…` (`sha256:90b4d41b…`, CI 35957294926).
- **Rehearsal R1** from `v0.1.5` (6 → 14 CRDs; the hand-provisioned key was adopted):
  - upgrade → `helm rollback logweir 1` → re-upgrade, with the identity unchanged throughout;
  - the schedule fired under each controller;
  - 17/17 receipts VALID;
  - a `v0.1.5` point restored 150/150.
- **Rehearsal R2** from `sha-f49849d…`:
  - release-note item 6 was refused at render;
  - the identity was adopted (`source=existing`);
  - rolled back.
- **Three in-place upgrades of the running install**, each by README *Upgrade to a newer publication* verbatim. Every upgrade kept the identity key id and its private digest:

  | From → to | helm revs | Chart digest | CI run | Result |
  |---|---|---|---|---|
  | `86a554e6` → `02dc44b6` | 3 → 5 | `sha256:7f448172…` | 36071480985 | 261 receipts VALID; P1–P10 closed live |
  | `02dc44b6` → `b748fd5f` | 5 → 7 | `sha256:3bba9455…` | 36100597420 | 314 receipts VALID; the three stuck pre-upgrade legacy points verified 3 s after the controller restart (P12); P11 and trust durability closed live |
  | `b748fd5f` → `a54fb823` | 7 → 9 | `sha256:5becb4b9…` | 36129705142 | a 5-minute schedule fired 13/13 slots across both controller swaps; **314 → 316 → 328 receipts VALID**; the pre-upgrade KeyCompromise check was empty |

- A legacy-point restore (`Valid` 150/150), a catalog-verified disaster-path restore, and a Governed restore approved by a second person were each proven live.

**Tests: "large-catalog measurements"** (`docs/stability.md`), at 258 real points:
- catalog Full sync 58.5 s;
- topic discovery 27.0 s fresh and 0.1 s reused;
- `GET /backups` of 258 rows 3.0 s;
- catalog points 6.6 s;
- operation status p50 71 ms;
- console render 0.7–2.1 s.

The offline rows bound 1,000 and 5,000 rows. The P10 bounds were measured live:
- 20 simultaneous manual runs → at most 4 runner pods;
- 3 restores at once → 2 run and 1 queues;
- a per-person `429`.

**Documents: supported paths, verification scope, retention authority, migration and rollback.**
- `docs/release-notes.md`: twenty numbered operator-facing changes, with the required actions in upgrade order and the *Pre-upgrade check*. That check ran empty before upgrade rounds 2 and 3. The notes also carry verification scope, retention authority, *Migration and rollback* and limitations.
- `docs/release-handoff.md` at `4e58d330`: the 41 tasks, the four publications' chart, image digests and CI runs, the tested environments, results, limitations and rollback. This meets the done-evidence clause.
- `doc_lint` pins the twenty items. Its negative controls are the sixteen-item notes and each deleted item.
- The console terminology was reworked by three human-like MCP passes, the last one verifying the round-2 fixes live.

**"Main publication stays the tested existing flow without redundant mandatory workflows":** the existing `ci` workflow (changes, check, e2e, publish amd64+arm64, promote) published every build used here. No workflow was added.

**Gates:**
- `deploy/poc/validate.sh` rc 0;
- `check-links.sh` rc 0;
- `check-unverified-labels.sh` rc 0;
- `pytest e2e/journeys scripts/live/poc`: 124 passed;
- `doc_lint`, `label_gate`, `withdrawn_claim` and `gate_lint` pass;
- `just lint` rc 0 (UI 747/747);
- the full `scripts/ci-check.sh` rc 0 on main `4e58d330`.

**Rollback (handoff):** `helm rollback logweir <previous revision>` restores the previous chart and its images together, after the checklist in release-notes *Migration and rollback*. The CRDs stay.
- From rev 9, `helm rollback logweir 7` returns to `b748fd5f`. The CRD change is description-only and the API change is additive.
- `[UNVERIFIED — rollback of an in-place upgrade was not run on the long-lived install; R1 and R2 rolled back live.]`

**Residue** (open, each with its own row or mark):
1. The large catalog was measured live at 258 points, not 1,000. It is marked `[UNVERIFIED]` in `stability.md` and the release notes; the offline rows carry 1,000/5,000.
2. POC-P15: the console's follow budgets are shorter than a check's 120 s. A fix is on `claude/poc-fixes-5`.
3. CONSOLE-MCP-ROUND3: open low rows.
4. REPLACE-MINIO: the demo, e2e and PoC MinIO is a rebuilt mirror of the withdrawn images.
5. Not run live:
   - R2's pre-upgrade states for items 1–5;
   - G1's negatives;
   - a `v0.1.5`-runner point through the legacy path on an upgraded install;
   - trust L9;
   - P12-L5/L6/L7/M1;
   - P14-D1 (not stageable inside the 900 s session);
   - D6/D7/D10 (need a rollback or an uninstall).

**Release notes owed (collected 2026-09-23 for this task to publish).** Every merged change whose behaviour an operator must know about:
1. **Retention — required action:** grant the retention delete credential `s3:GetObject` on `<bucket>/<prefix>/*` before upgrading. Without it the enforcer deletes nothing (`VersionProbeRefused`); a policy degraded for that reason re-probes 24 h after its last run, or resumes at once on a spec edit.
2. **Retention:** `Enforce` on a versioned or Object Lock bucket now deletes nothing and degrades with `VersionedBucket`. `Deleted` records written by earlier builds on such buckets are false: the data remains as noncurrent versions. Use an unversioned bucket or `mode: ExternalLifecycle`, and set such policies to `mode: Report` before a rollback. Do not change a bucket's versioning while a retention run is in flight, and do not enforce on a bucket whose versioning was ever enabled and later suspended (RET-VERSIONED-SUSPENDED-REWRITE).
3. **Retention:** re-run receipts over one backup set are protected (`SharedSegment`) until every receipt naming the set is due, and the set is then removed by one plan line (`co_point_ids`). Plan digests approved over such plans must be re-approved; plans without a shared set are byte-identical. An older worker refuses such a plan and deletes nothing.
4. **Operation states:** runs that end without an exit code may now read `Failed/VolumeMountFailed`, `Failed/PodUnschedulable` or `Failed/RunnerImageUnavailable` instead of `NoExitCode`. Review alert rules that match `NoExitCode`.
5. **Disaster restore (PLAT-15.2):** upgrade the controller and runner images together. A standalone `logweir restore run` of a point-bound plan needs `--evidence-keys`. A point-bound Restore whose bundle was created before the upgrade ends `ApprovalBundleConflict` and must be recreated.
6. **Shared console (PLAT-17.2):** existing shared-mode values stop rendering until `controller.watchNamespaces` is set and the console key namespace is listed. `trustedProxyCidrs` wider than /16 (IPv4) or /48 (IPv6) is refused when `requireTrustedProxy` is on. See `docs/install.md` §5e.
7. **Approval policy (PLAT-19.2):** Ordinary confirmation requires `allowOrdinaryConfirmation` plus an explicit namespace binding and is refused in `localAdmin`; Governed requires a ticket. A Restore submitted during a policy rollout may need resubmitting. See the D0 amendment on the `{name, digest}` policy reference.
8. **Restore completion:** `status.completion` appears only once the scorecard's verification is Valid; `recordsRestored` is the sampled-window count.
9. **Schedules:** a slot due before the schedule's `metadata.creationTimestamp` never fires (D1 §4.7 row 5a).
10. **API trust state:** `trust.state` for `Valid` + `RecordedBeforeRevocation` (or any non-pass basis) is now `untrusted`, where it read `verified`.

---

Documentation is licensed [CC-BY-4.0](../LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
