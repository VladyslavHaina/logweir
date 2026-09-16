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
| PLAT-04.1 | In progress (defect found) | closure-0413, w0-reservation, plat06-live | Source identity, focused tests (schedule_controller 44/44, crd_shape 24/24, retention 48/48) and published controller digest `sha256:bdaaf374…` verified at `4956785`; overlap, two-replica, restart, stale-finalizer and Allow cases have live evidence. Two gaps block Done: the deleted-Job case has controller-double evidence only (plat06's live run adds it), and the slot reservation is unauthorized by the shipped role (below), so the accepted live evidence does not describe a shipped install. |
| PLAT-04.1 defect (P0) | In progress (fix committed, live proof pending) | w0-reservation → plat06-live | `backup_schedule.rs:1359` reserves a Forbid slot with `replace_status`, which RBAC authorizes as `update` on `backupschedules/status`; the shipped role grants only `patch` (`config/rbac/role.yaml:118`, `charts/logweir/templates/clusterrole.yaml:29`). Confirmed on docker-desktop: the lab ServiceAccount has `update` no, `patch` yes, so with the default `Forbid` policy no scheduled Backup is created on a shipped install. PLAT-04.1's live run used custom namespace Roles and never exercised this. Fix: resourceVersion-conditional merge PATCH, an audit of every other call against the shipped role, a reverse "every call has a grant" lint with mutant evidence, and live proof under shipped RBAC. |
| PLAT-06.1 | In progress | plat06-live | Source, controller-double tests (weirkeeper 321/321, thirteen killed mutants) and docs are complete on `claude/plat06` (report `claude/plat06.result.md` §1-4, §6); the live docker-desktop run (manual, hostile annotation, scheduled under the shipped role, restart, deleted Job, snapshot equality, duplicate name) and its report sections are in progress. |
| PLAT-07.1 | In progress | plat07-finish | Versioned connection contract, one shared resolver for probe/backup/restore Jobs, TLS private CA, rotation, redaction, write-only credential builder; live SCRAM rotation and TLS cases. |
| PLAT-13.2 | Done | ui-correct, ui-correct-review, ui-correct-fix | Completion record under PLAT-13.2. Integrated into main as `2a34abd..8020876`. |
| PLAT-12.1 (immediate slice), PLAT-12.2 (subject slice) | In progress (slices landed) | ui-correct | The guided submit, idempotent durable Restore and subject binding landed with PLAT-13.2 (records under each task); remaining: PLAT-11.2/13.2-backed selection flow and PLAT-19.2 policy routing for 12.1, retry identity for 12.2. |
| PLAT-17.1 (stage 1) | In progress | plat17-api-finish | New `crates/logweir-api`: bounded `/api/v1` routes over current resources, local-admin mode only, idempotency, problem responses, cursors, static assets; mock-API tests and live smoke. OIDC/roles (PLAT-17.2), console image/chart and UI migration follow. |
| PLAT-04.2, 05.x, 06.2, 09.2 | Contract decided | [D1](decisions/D1-backup-scheduling.md) | Cadence/time zone, editable policy with per-run snapshots, retained history, dynamic selection, manual runs; nine worker tasks. d1w1-cadence-finish in progress (pure cadence engine, `chrono-tz` decision). |
| PLAT-03.x, 08.x, 09.1 | Contract decided | [D2](decisions/D2-destinations-discovery-readiness.md) | `BackupDestination`, `TopicDiscovery`, `Preflight`, one shared check runner; sixteen worker tasks. |
| PLAT-14.x, 15.x, 16.x, 19.1 | Contract decided | [D3](decisions/D3-status-catalog-retention-trust.md) | Operation states, protection freshness, rehearsals, durable catalog, retention enforcement boundary, trust lifecycle; fifteen worker tasks. |

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

D2 and D3 add eight kinds (`BackupDestination`, `TopicDiscovery`, `Preflight`,
`TrustPolicy`, `ProtectionPolicy`, `RehearsalSchedule`, `RecoveryCatalog`,
`RetentionPolicy`) under new ADR 0008 amendments, each justified by an
authorization or lifetime boundary rather than by convenience. A kind ships
only together with its controller, RBAC and documentation; an unserved schema
is not a delivery.

### Defects found by the 2026-09-15 contract spikes

Each was confirmed against current source (and, where marked, against the live
cluster). They are recorded here so no finding depends on a worker report
surviving. None is fixed yet except where a worker is named.

| Id | Defect | Owner |
| --- | --- | --- |
| P0-RESERVE | `backup_schedule.rs:1359` reserves a slot with `replace_status` (PUT, verb `update`) while the shipped role grants only `patch` on `backupschedules/status`; default `Forbid` schedules therefore never create a Backup on a shipped install. Confirmed live (`auth can-i`: update no, patch yes). | w0-reservation (dispatched) |
| SEC-ENVHTTP | The controller forwards its own `AWS_ENDPOINT_URL`, `AWS_REGION`, `AWS_ALLOW_HTTP` and `AWS_VIRTUAL_HOSTED_STYLE_REQUEST` into every runner Job (`controllers/backup.rs:192`, `restore.rs:1297`); the engine's `from_env()` then honours them, so a forwarded `AWS_ALLOW_HTTP=true` enables plaintext transport even when the approved plan says `allow_http: false`. A global setting overrides approved execution inputs. | PLAT-08.1 destination resolver (D2 W6b) |
| SEC-PODLOG | Pod lookup for exit codes and evidence keys matches on labels alone and takes the first result (`controllers/backup.rs:1683`, `restore.rs:2519`, `kafka_cluster.rs:832`); a tenant able to create a pod with `batch.kubernetes.io/job-name=<job>` can have its log read as the run's outcome. The pod's controller owner UID is never checked. | queued after plat06/plat07 merge |
| UI-HTTPDOWNGRADE | The restore wizard sets `allowHttp` from the path-style checkbox (`ui/pages/restore-wizard.js:1142`) and applies one endpoint/region/addressing to both the source archive and evidence store (`:1135`). | PLAT-08.2 UI slice, after ui-correct |
| UI-FAKEPREFLIGHT | Wizard step 5 "Target-topic preflight" shows only the target cluster's cached `status.reachable` (`ui/pages/restore-wizard.js:424`), and restore admission gates on the same cached value (`controllers/restore.rs:638`). | PLAT-03.2 |
| RET-WRONGBUCKET | Retention lists manifests through the controller's single global store while rendering commands for the schedule's own URL (`backup_schedule.rs:1417`), so a schedule on another bucket is reported against the wrong catalog. | PLAT-16.1 |
| ENGINE-PATHSTYLE | The pinned engine ignores `path_style` and forces path-style addressing whenever an endpoint is set (`kafka-backup-core storage/s3.rs:66`), so virtual-hosted addressing with a custom endpoint cannot be honoured and must be refused rather than advertised. | PLAT-08.1 (documented refusal) |

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

## PLAT-07 — Reuse saved cluster connections everywhere

**Priority P1 · Proposed.** SCRAM reference reuse is already fixed; extend the
same model to registration, discovery, preflight and restoration without
reintroducing separate credentials.

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

## PLAT-18 — Strengthen UI structure without a speculative rewrite

**Priority P2 · Proposed.** Scope: contracts, state, reusable interactions and
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
