# Release handoff

What has shipped on `main`, where each piece was tested and with what result,
what is still open, and how to roll back — the handoff PLAT-20.2 asks for, so a
person or an agent picking this release up does not have to reconstruct it from
the platform tracker. It records the tracker's Done records and ledger as of
**`main` `306cebf` (2026-09-23)**; the tracker
([platform-improvements.md](to-do/platform-improvements.md)) stays the authority
for each task's full record, and this file is updated per release rather than
claiming the roadmap complete.

**State at this revision.** The last version tag is `v0.1.5` (`9cc78a3`). The
offline half of PLAT-20.2 — the supported guide, the release notes, the
terminology audit and the measured limits — landed with this file. The live
half (a clean install and an upgrade from the last published image on
docker-desktop) waits for the shared cluster, which **lab-refresh-9** is using
to rebuild the lab from `306cebf` and to run every row owed since
lab-refresh-8; its report is the orchestration store's
`claude/lab-refresh-9.result.md`. [UNVERIFIED — the live rows below marked "owed at lab-refresh-9" have no result yet.]

## Tested environments

| Environment | What it is | What it can and cannot show |
|---|---|---|
| GitHub Actions `ci.yml` | `check` (fmt, clippy, workspace tests, UI behaviour, Python verifier, dependency, license, schema, CRD, chart and doc gates) and `e2e` (Compose Kafka + MinIO + the pinned engine) | every merged commit; no Kubernetes |
| GitHub Actions `images.yml` | builds, checks and publishes runner (amd64), controller (amd64 + arm64), console and UI images after CI on `main` | publication of the exact bytes; not a deployment |
| docker-desktop Kubernetes (v1.34.1), the shared lab | `weirkeeper` and runner images built from a named `main` commit (lab-refresh-2 … -8), a SCRAM Kafka pair, MinIO, the chart's roles and policy; per-task harnesses under `scripts/live/`, `e2e/k8s/d2`, `e2e/k8s/d3` and `scripts/*-ui-e2e.mjs` (Playwright against a host `logweir-api`) | live controller, runner, console and approval behaviour; **not** NetworkPolicy enforcement (Docker Desktop does not enforce it), AWS S3, MSK, EKS IAM or a real OIDC provider |
| Offline measurement | the product API over its in-process fake, the pure discovery steps, the console's render functions ([stability.md](stability.md), *Measured scale limits*) | cost at the product's own bounds; no network or real API server |

Image identities of the last lab: **lab-refresh-8** ran controller
`sha256:01338ec0…` and runner `sha256:ad705ba2…`, both built from `main`
`f49849d` (local builds, `imagePullPolicy: Never`). **lab-refresh-9** builds
controller `sha256:f71fdcb4f8743cebb2cdc54153e99e9f0f6b992060e52186a4d4ac7413b0a0cc`
(arm64) and runner
`sha256:db8d8adec8a4d0d789b33d07cd02c30035c4a2d7f493d075a02a0ed9877edd46`
(amd64), both from `306cebf`. A lab image is an author-only build and says
nothing about the published registry digests, which the release's candidate
record in [release-notes.md](release-notes.md) carries.

## Shipped: Done tasks

Each row is the tracker's completion record in brief: the commits that carry
it on `main`, the environment and build its acceptance ran on, and the result.

| Task | Done | Commits on `main` | Tested on | Result |
|---|---|---|---|---|
| PLAT-01.1, 01.2 — per-restore immutable execution bundle, runner revalidation | 2026-09-16 | source `4956785`; CI 35019727967 published controller `sha256:bdaaf374…`, runner `sha256:2d20f4bd…` | docker-desktop, images from `4956785` and from `92e0209` | 31/31 required cases |
| PLAT-02.1, 02.2 — managed installation identity; signing validated before data work | 2026-09-16 | `4b7a1f6` (bootstrap digest pin), `fbd124e` (harnesses) | docker-desktop, full chart | 18/18 chart cases (install, upgrade, rollback, reinstall, restore-first recovery, external adoption) |
| PLAT-03.1 — operation readiness with actionable failures | 2026-09-18 | D2 W4/W9/W12/W13, `83b8927`, `902f4a1`, `5bf6d25…33ff2eb` | D2 W14 at `e7d0e79`; lab-refresh-3 at `c6422a7` | every listed test live, incl. a redaction sweep with zero hits |
| PLAT-03.2 — restore preflight and invalidation | 2026-09-18 (live to 09-19) | D2 W4/W9, D3 W5, `e7f8c65`, `83b8927`, `52c1f00…85adc2e` | D2 W14; lab-refresh-3 … -6 | every listed test live |
| PLAT-04.1 — concurrency from actual run state | 2026-09-16 | `4956785`, `bdd26dc`, `10f6c28` | docker-desktop under the shipped role | overlap, replicas, restart and deleted-Job cases |
| PLAT-04.2 — cadence, missed slots, retries | 2026-09-18 | D1 W1/W2/W3/W6, W7 `a2ce381…700916e` | D1 W8, images from `e7d0e79` | live incl. the console journey |
| PLAT-05.1 — editable future policy, immutable run snapshots | 2026-09-18 | D1 W2/W6/W7, `6663ccb…d8d3479`, `0e68dfb…9fef8f6` | D1 W8; lab-refresh-4/5 | live incl. a real rollback |
| PLAT-05.2 — schedule deletion keeps history | 2026-09-18 | `58d68f6` … `f35c7cf` | D1 W8 at `e7d0e79` | all three cascade modes keep every run |
| PLAT-06.1 — runner inputs from the Backup contract | 2026-09-16 | `8e362f9..10f6c28` | docker-desktop | typed inputs, frozen plan, no annotation executed |
| PLAT-06.2 — Back up now, first-run execution | 2026-09-21 | `72751ab`, `a2ce381..700916e`, `..cc05b34` | docker-desktop, lab `af64073` | UI and CLI paths, idempotent intent |
| PLAT-07.1 — saved-connection contract v1 | 2026-09-16 | `6c534b2..199020a`, `50e641f` | docker-desktop | SCRAM, private-CA TLS, rotation, redaction |
| PLAT-07.2 — saved-cluster selection and health freshness | 2026-09-18 | `8bbe4d1..b65f23f`, `5bf6d25…33ff2eb`, `e20298f…b650885` | lab-refresh-4 | live incl. the dialling *Test connection* |
| PLAT-08.1 — destination settings and access per role | 2026-09-21 | `c13b0cc..56bd074`, `27fb924..0b25e95`, `e98eb8a`, D2 W11/W13 | lab-refresh-3/4, `plat08-u6` | live incl. the measured per-role permission table |
| PLAT-09.1 — bounded, honest topic inventory | 2026-09-18 | D2 W4/W8/W12/W13, `1da8faf` | D2 W14 at `e7d0e79` | 5,003-topic inventory, empty cluster, ACL-limited principal |
| PLAT-09.2 — explicit and dynamic selection per run | 2026-09-21 | `7072b9d`, `86a18b1`, `fe5e342..cb17eac` | lab at `af64073` | all seven L-09 rows on one build |
| PLAT-10.1, 10.2 — schedule creation, detail and history | 2026-09-23 | `8c092b2..27eb7c9` (+ `4cd3e55..4f2c93e`) | lab-refresh-8, `main` `f49849d` | 20 journeys and 11 negative controls PASS; create → backup → detail → restore |
| PLAT-11.1 — wizard bound to a selected point | 2026-09-16 | `5a6b1a8`, `a447791`, `3fd6985`, `02426c8` | docker-desktop | live journeys |
| PLAT-11.2 — topic subset, mapping and limits | 2026-09-22 | `claude/plat11-2` (`..d5090ed`) | docker-desktop | all eight tests through the console |
| PLAT-13.1 — navigation and namespace request lifetimes | 2026-09-15 | `a2bf522` | docker-desktop at `4956785` | 7/7 lifecycle, 61/61 UI behaviour |
| PLAT-13.2 — drafts and one mutation state | 2026-09-16 | `2a34abd..8020876` | docker-desktop | live browser harness |
| PLAT-16.1 — recommendations separated from enforcement | 2026-09-18 | D3 W9, `f5a6876` | D3 W14 at `e7d0e79`; lab-refresh-3/4/5 | live |
| PLAT-17.1 — bounded product endpoints, console packaging | 2026-09-21 | `4b571d1..de0207c`, `72751ab`, `0202648..0ca3386`, `bdcc721..4ab14e1` | lab-refresh-3; stage 7 live can-i matrix | live |
| PLAT-18.1 — typed clients, explicit workflow state | 2026-09-16 | `43576bf..48d5ec0` | node suites, both modes | contract-against-schema tests |
| PLAT-19.1 — trust lifecycle | 2026-09-21 | `64fcd38..5fc1a72`, `b3aed6f..3eb7897`, `d8d3479`, `532740d`/`efa16b2` | lab builds `d387f87`, `af64073` (harness-rows-4 … 7) | overlap, retirement, revocation, the keys view's `unknown` |

## On `main`, not yet Done

| Task | State | What its Done record waits for |
|---|---|---|
| PLAT-08.2, 15.2, 17.2, 19.2 (+ 12.1 policy routing), 20.1 | source on `main` since `ac00819` (integration-2); each passed its own review and worker journey | a lab build that carries them: owed at lab-refresh-9 |
| PLAT-18.2 | the Clarity console, on `main` since `9d638cc` | its Playwright pass on a lab build: owed at lab-refresh-9 |
| PLAT-12.1, 12.2 | slices landed | 12.2's verified-approval live route; 12.1's policy routing (PLAT-19.2) |
| PLAT-14.1, 14.2, 14.3, 15.1, 16.2 | D3 W0–W13 landed | the operation-state, retention-count and status-sweep rows at lab-refresh-9, and each task's named defect |
| PLAT-20.2 | offline half landed with this file | the clean-install and upgrade rows below |

The defects closed on `main` since lab-refresh-8 and whose live rows are owed at
lab-refresh-9 are the ten operator-facing changes in
[release-notes.md](release-notes.md) (OBJECT-LOCK-DELETE-MARKER,
SHARED-SET-RETENTION, WARNING-DIAGNOSTICS-NOEXITCODE,
RESTORE-COMPLETION-UNWRITTEN, SCHEDULE-FIRES-SLOT-BEFORE-CREATION,
TRUST-STATE-RBR-VERIFIED and the rest).

## What the live half of PLAT-20.2 must still show

On docker-desktop, in namespaces the run owns, after lab-refresh-9 releases the
cluster:

1. **A clean install** with the PoC profile, [deploy/poc/](../deploy/poc/README.md)
   (Helm and the published chart and images only — the candidate is the first
   `main` publication carrying the PoC chart fixes, `deploy/poc/versions.env`'s
   `LOGWEIR_COMMIT`; `sha-306cebf…` predates them and cannot run the profile), following
   [quickstart.md](quickstart.md)'s supported path from an empty namespace set: the Helm install with the managed identity,
   trust, a console, a saved connection and destination, a schedule, the first
   backup to a green badge, a restore of a chosen point through an approval, the
   independent verifier over its scorecard, and a disaster restore from a
   connected archive on a namespace with no `Backup` objects.
2. **Two upgrade rehearsals to the candidate** (`deploy/poc/versions.env`'s
   `LOGWEIR_TAG`; [deploy/poc/](../deploy/poc/README.md), *Upgrade rehearsals*,
   has each starting values file and command sequence), each following
   [release-notes.md](release-notes.md)'s order and each keeping:
   - the installation identity — the same `key-id` and private-key digest in
     `logweir-signing-key` / `logweir-signing-trust`;
   - every schedule — the same UID, spec and `metadata.generation`, its history
     retained, its next slot fired under the new controller;
   - archive readability — every pre-upgrade receipt and scorecard still
     verifies (the controller's badge and `docs/verify_scorecard.py`), and a
     pre-upgrade point still restores.

   | Rehearsal | Starting images (all published on Docker Hub, checked 2026-09-23) | Starting chart | What it crosses |
   |---|---|---|---|
   | **R1 — the last version tag** | `weirkeeper:v0.1.5` (`sha256:e933e7cc…`), `logweir:v0.1.5` (`sha256:f2a28c93…`), `logweir-ui:v0.1.5` (`sha256:51ead7bf…`); there is **no** `logweir-console:v0.1.5` — the console did not exist | `charts/logweir` at `v0.1.5` (`9cc78a3`): 6 CRDs, no managed identity (the signing Secret is provisioned by hand, as that tag's `docs/install.md` says), no console | 6 → 14 CRDs; the chart's identity bootstrap **adopting** the hand-provisioned signing Secret; `TrustRoster/default` becoming `legacy-roster-v1`; the console arriving; the execution contract v1 → v2 and frozen-input grammar v1 → v2 on runs in flight. This is the upgrade an adopter on the last release makes |
   | **R2 — the last build before the integration of PLAT-15.2/17.2/19.2** | `sha-f49849db035d01ff968df7472914f57fc6c2e988` for all four images (`weirkeeper` `sha256:42a4afaa…`, `logweir` `sha256:443d514e…`, `logweir-console` `sha256:e7b60be7…`) — the lab-refresh-8 build, before `ac00819` | `charts/logweir` at `f49849d`: 14 CRDs (6 of them change), a console without `controller.watchNamespaces` or `approvalPolicy.*` | every one of the ten release-note items (table below), with a chart diff of 465 template lines |

   `sha-7b0277b…` (the previous `main` publication) is **not** an upgrade proof:
   it differs from the candidate by retention and diagnostics commits only — no
   chart or CRD change — and crosses items 1–4 alone. It may be run as a smoke
   step, never as the evidence that closes this acceptance. Neither baseline can
   install with the candidate's PoC values file (`deploy/poc/`): R1 installs
   with `v0.1.5`'s own chart and procedure, R2 with `f49849d`'s chart and a
   shared-console values file written for that chart, and the upgrade step then
   applies the candidate's values.

   **Which release-note items each rehearsal exercises, and what state it must
   set up first:**

   | Item | R1 (`v0.1.5`) | R2 (`sha-f49849d`) — the pre-upgrade state to create |
   |---|---|---|
   | 1. retention delete grant `s3:GetObject` | no (no `RetentionPolicy` kind) | an `Enforce` policy whose delete credential lacks `s3:GetObject`: after the upgrade it keeps every point `VersionProbeRefused` |
   | 2. versioned buckets refused | no | an `Enforce` policy on a versioned MinIO bucket: after the upgrade `VersionedBucket`, nothing deleted |
   | 3. shared backup sets | no | a set with two receipts (a re-created runner Job) under `keepLast: 1`: the approved digest changes and must be re-approved |
   | 4. runs without an exit code named | no | a run whose projected ConfigMap never mounts, ending after the upgrade: `VolumeMountFailed`, not `NoExitCode` |
   | 5. controller and runner together; `ApprovalBundleConflict` | no (no point-bound restore) | a point-bound `Restore` whose bundle the old controller created and that has no Job at the upgrade |
   | 6. shared-console values migration | no (no console) | a `shared` console installed without `controller.watchNamespaces`: the candidate's `helm upgrade` refuses to render until migrated |
   | 7. approval-policy rollout | no | bind a namespace after the upgrade; a Restore submitted mid-rollout may need resubmitting |
   | 8. completion only from a valid scorecard | the first post-upgrade restore | the first post-upgrade restore |
   | 9. no slot before creation | a schedule's next slot under the new controller | the same |
   | 10. `trust.state` for `RecordedBeforeRevocation` | no (no API) | a Backup verified under a key revoked for compromise: the API reads `untrusted` |

3. **A rollback** from the candidate to each starting point with the release
   notes' rollback list, the same three properties kept.

[UNVERIFIED — clean install, upgrade and rollback on docker-desktop are owed by PLAT-20.2's live round.]

## Limitations carried by this release

The release notes' *Limitations and open items* and [stability.md](stability.md)'s
*Known limitations* are the list; the ones an operator meets first are: no AWS S3,
MSK, EKS IAM or real OIDC provider has been run; Docker Desktop cannot show a
NetworkPolicy deny; a `Restore` is not held by a retention lease; there is no
in-place runner signing-key cutover; and the product API's OpenAPI document is
still `1.0.0-alpha.1`.

## Rollback

[release-notes.md](release-notes.md), *Migration and rollback*, is the ordered
list: retention policies to `Report` and their leases clear; `SourceConnection`
preflights deleted; standing rehearsal `Approval`s deleted when rolling back
past Amendment G; destination-backed and `v2`-frozen backups finished; approval
policies unbound; controller and runner rolled back together; CRDs left in
place. The installation identity is restored first if it was lost
([install.md](install.md), *Back up and recover the installation identity*).
Archives, evidence and catalog records are untouched in either direction.

---

Documentation is licensed [CC-BY-4.0](LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
