# Release handoff

What has shipped on `main`, where each piece was tested and with what result,
what is still open, and how to roll back. PLAT-20.2's done evidence asks for
exactly this — "shipped task IDs, commit/image identities, tested
environments, results, limitations and rollback instructions" — so that a
person or an agent picking this release up does not have to rebuild it from the
platform tracker. This file is current at **`main` `815249cb` (2026-09-25)**.
The tracker ([platform-improvements.md](to-do/platform-improvements.md)) stays
the authority for each task's full completion record, and this file is updated
per release rather than claiming the roadmap complete.

**State at this revision.**
- The last version tag is `v0.1.5` (`9cc78a3`). **No tag is cut at
  `815249cb`.** Every row of the *Candidate record* in
  [release-notes.md](release-notes.md) therefore stays `—`: a tag needs its own
  CI run, release run and image digests recorded there, and a previous run
  does not validate new bytes.
- All 41 tasks of the platform tracker have shipped. Forty are Done with a
  completion record. PLAT-20.2 is Done on the orchestrator's record
  (2026-09-25), written from the last PoC round's final draft
  (`claude/poc-upgrade-3.result.md` §7 in the orchestration store).
- The PoC install is left running on docker-desktop at the published
  `sha-815249cb…` (CI run 36152835598, chart digest `sha256:820622f2…`): Helm
  release `logweir`, revision 11, in shared mode behind Traefik and Dex
  ([deploy/poc/](../deploy/poc/README.md)). Its fourth in-place upgrade, from
  `a54fb823`, is `claude/poc-upgrade-4.result.md` in the orchestration store.
- [product-expansion.md](to-do/product-expansion.md) is not started, by the
  user's decision (2026-09-23): this release carries platform improvements only.

## Shipped: the 41 tracker tasks

Each row is the tracker's completion record in brief: the commits that carry it
on `main`, the environment and build its acceptance ran on, and the result.
"lab-refresh-N" is the shared docker-desktop lab rebuilt from a named `main`
commit (see *Tested environments*).

| Task | State | Commits on `main` | Tested on | Result |
|---|---|---|---|---|
| PLAT-01.1, 01.2 — per-restore immutable execution bundle, runner revalidation | Done 2026-09-16 | source `4956785`; CI 35019727967 published controller `sha256:bdaaf374…`, runner `sha256:2d20f4bd…` | docker-desktop, images from `4956785` and from `92e0209` | 31/31 required cases |
| PLAT-02.1, 02.2 — managed installation identity; signing validated before data work | Done 2026-09-16 | `4b7a1f6` (bootstrap digest pin), `fbd124e` (harnesses) | docker-desktop, full chart | 18/18 chart cases (install, upgrade, rollback, reinstall, restore-first recovery, external adoption) |
| PLAT-03.1 — operation readiness with actionable failures | Done 2026-09-18 | D2 W4/W9/W12/W13, `83b8927`, `902f4a1`, `5bf6d25…33ff2eb` | D2 W14 at `e7d0e79`; lab-refresh-3 at `c6422a7` | every listed test live, incl. a redaction sweep with zero hits |
| PLAT-03.2 — restore preflight and invalidation | Done 2026-09-18 (live to 09-19) | D2 W4/W9, D3 W5, `e7f8c65`, `83b8927`, `52c1f00…85adc2e` | D2 W14; lab-refresh-3 … -6 | every listed test live |
| PLAT-04.1 — concurrency from actual run state | Done 2026-09-16 | `4956785`, `bdd26dc`, `10f6c28` | docker-desktop under the shipped role | overlap, replicas, restart and deleted-Job cases |
| PLAT-04.2 — cadence, missed slots, retries | Done 2026-09-18 | D1 W1/W2/W3/W6, W7 `a2ce381…700916e` | D1 W8, images from `e7d0e79` | live incl. the console journey |
| PLAT-05.1 — editable future policy, immutable run snapshots | Done 2026-09-18 | D1 W2/W6/W7, `6663ccb…d8d3479`, `0e68dfb…9fef8f6` | D1 W8; lab-refresh-4/5 | live incl. a real rollback |
| PLAT-05.2 — schedule deletion keeps history | Done 2026-09-18 | `58d68f6` … `f35c7cf` | D1 W8 at `e7d0e79` | all three cascade modes keep every run |
| PLAT-06.1 — runner inputs from the Backup contract | Done 2026-09-16 | `8e362f9..10f6c28` | docker-desktop | typed inputs, frozen plan, no annotation executed |
| PLAT-06.2 — Back up now, first-run execution | Done 2026-09-21 | `72751ab`, `a2ce381..700916e`, `..cc05b34` | docker-desktop, lab `af64073` | UI and CLI paths, idempotent intent |
| PLAT-07.1 — saved-connection contract v1 | Done 2026-09-16 | `6c534b2..199020a`, `50e641f` | docker-desktop | SCRAM, private-CA TLS, rotation, redaction |
| PLAT-07.2 — saved-cluster selection and health freshness | Done 2026-09-18 | `8bbe4d1..b65f23f`, `5bf6d25…33ff2eb`, `e20298f…b650885` | lab-refresh-4 | live incl. the dialling *Test connection* |
| PLAT-08.1 — destination settings and access per role | Done 2026-09-21 | `c13b0cc..56bd074`, `27fb924..0b25e95`, `e98eb8a`, D2 W11/W13 | lab-refresh-3/4, `plat08-u6` | live incl. the measured per-role permission table |
| PLAT-08.2 — destination defaults and storage choices | Done 2026-09-23 | `claude/plat08-2` in `ac00819`, plus the draft-submit fix from `claude/plat19-2` | lab-refresh-9 at `306cebf` | 14 rows + 8 controls PASS, 0 blocked; the per-role table re-measured with the enforcer's `s3:GetObject` |
| PLAT-09.1 — bounded, honest topic inventory | Done 2026-09-18 | D2 W4/W8/W12/W13, `1da8faf` | D2 W14 at `e7d0e79` | 5,003-topic inventory, empty cluster, ACL-limited principal |
| PLAT-09.2 — explicit and dynamic selection per run | Done 2026-09-21 | `7072b9d`, `86a18b1`, `fe5e342..cb17eac` | lab at `af64073` | all seven L-09 rows on one build |
| PLAT-10.1, 10.2 — schedule creation, detail and history | Done 2026-09-23 | `8c092b2..27eb7c9` (+ `4cd3e55..4f2c93e`) | lab-refresh-8, `main` `f49849d` | 20 journeys and 11 negative controls PASS; create → backup → detail → restore |
| PLAT-11.1 — wizard bound to a selected point | Done 2026-09-16 | `5a6b1a8`, `a447791`, `3fd6985`, `02426c8` | docker-desktop | live journeys |
| PLAT-11.2 — topic subset, mapping and limits | Done 2026-09-22 | `claude/plat11-2` (`..d5090ed`) | docker-desktop | all eight tests through the console |
| PLAT-12.1 — one submission, visible durable progress | Done 2026-09-23 | the guided submit and `claude/plat19-2`'s policy routing, in `ac00819` | lab-refresh-9 at `306cebf` | `plat12-13-ui-e2e.mjs` 20/20; journeys 12 PASS / 0 FAIL; Ordinary, Governed and unbound routing |
| PLAT-12.2 — approval subject handling and explicit retry | Done 2026-09-23 | subject binding, `claude/plat11-2`, the verified-approval mapping fix in `ac00819` | lab-refresh-9; harness-rows-12 (`8569682`) at `306cebf` | `plat12-13-ui-e2e.mjs` 21/21; residue (LOW): an expired authorization document renders "refused", never green |
| PLAT-13.1 — navigation and namespace request lifetimes | Done 2026-09-15 | `a2bf522` | docker-desktop at `4956785` | 7/7 lifecycle, 61/61 UI behaviour |
| PLAT-13.2 — drafts and one mutation state | Done 2026-09-16 | `2a34abd..8020876` | docker-desktop | live browser harness |
| PLAT-14.1 — user-facing operation states | Done 2026-09-23 | D3 W11/W12, `b57753b`, `fa3384e` | lab-refresh-9; harness-rows-12 at `306cebf` | mount failure ×2, unschedulable pod, engine crash, verification downgrade, completion only beside `Valid`, completed-Job cleanup |
| PLAT-14.2 — protection freshness and notifications | Done 2026-09-23 | D3 W6 and fixes | lab-refresh-9 at `306cebf` | 12 protection rows; notification transport failure (3 attempts, `DeliveryFailed`, no Backup rewritten) |
| PLAT-14.3 — basic recovery rehearsals | Done 2026-09-24 | `707343b` (`claude/rehearsal-fix`), `86a554e` (`claude/reserve-commit`), the 14.3b standing authorization | lab-refresh-10 at `b426096`; lab-refresh-11 at `86a554e`; CI 35957294926 | all seven tracker tests live |
| PLAT-15.1 — durable recovery-point metadata | Done 2026-09-23 | D3 W3/W8/W11, `2cb04c7`, `7b0277b` | lab-refresh-9 at `306cebf` | the 13 catalog rows and the trust joins |
| PLAT-15.2 — Connect existing archive, disaster restore | Done 2026-09-23 | `claude/plat15-2` in `ac00819` | lab-refresh-9; harness-rows-12 at `306cebf` | CR loss (0 CRs, 100 records restored); untrusted signer refused by the Preflight and the runner; a Retired-key point restored |
| PLAT-16.1 — recommendations separated from enforcement | Done 2026-09-18 | D3 W9, `f5a6876` | D3 W14 at `e7d0e79`; lab-refresh-3/4/5 | live |
| PLAT-16.2 — the supported enforcement boundary | Done 2026-09-23 | incl. `b57753b` (`claude/ctl-batch-2`) | lab-refresh-9 at `306cebf` | preview, denial, legal hold, bounded retry; versioned bucket deletes nothing; shared set kept; the enforcer's grant 9/9 |
| PLAT-17.1 — bounded product endpoints, console packaging | Done 2026-09-21 | `4b571d1..de0207c`, `72751ab`, `0202648..0ca3386`, `bdcc721..4ab14e1` | lab-refresh-3; stage 7 live can-i matrix | live |
| PLAT-17.2 — user identity, roles and audit attribution | Done 2026-09-24 | `claude/plat17-2` in `ac00819`; the ingress harness in `7beb7c8` | the PoC at the published `86a554e6` (Traefik + cert-manager + Dex) | 58/58 on the real entry point; expired session 3/3; no-role user 5/5 |
| PLAT-18.1 — typed clients, explicit workflow state | Done 2026-09-16 | `43576bf..48d5ec0` | node suites, both modes | contract-against-schema tests |
| PLAT-18.2 — dense workflows accessible and scalable | Done 2026-09-23 | `9d638cc` (Clarity), `8485665`, `691f016` | lab-refresh-9 (host API from `main`) | 7 journeys PASS; axe-core WCAG 2.1 AA over 72 route visits, 0 findings |
| PLAT-19.1 — trust lifecycle | Done 2026-09-21 | `64fcd38..5fc1a72`, `b3aed6f..3eb7897`, `d8d3479`, `532740d`/`efa16b2` | lab builds `d387f87`, `af64073` (harness-rows-4 … 7) | overlap, retirement, revocation, the keys view's `unknown` |
| PLAT-19.2 — Ordinary confirmation and Governed approval | Done 2026-09-23 | `claude/plat19-2` in `ac00819` | lab-refresh-9; harness-rows-12 at `306cebf` | Ordinary and Governed; eleven kubectl-written failure modes each refused by name |
| PLAT-20.1 — focused cross-layer journey set | Done 2026-09-23 | `e2e/journeys`, `claude/plat20-1` in `ac00819` | lab-refresh-9; harness-rows-12 at `306cebf` | 12 PASS / 0 FAIL; no new CI gate |
| PLAT-20.2 — supported behaviour, upgrade proof, limits | Done on the orchestrator's record (2026-09-25) | offline half `74876826` (`claude/plat20-2`); PoC rounds `7beb7c8`, `446fbaf`, `e9bb3894`, `4e58d330` | the PoC profile on docker-desktop, at the four publications below | see *What the live half of PLAT-20.2 showed* |

## Tested environments

| Environment | What it is | What it can and cannot show |
|---|---|---|
| GitHub Actions `ci.yml` | `check` (fmt, clippy, workspace tests, UI behaviour, Python verifier, dependency, license, schema, CRD, chart and doc gates), `e2e` (Compose Kafka + the MinIO mirror + the pinned engine), and `publish` (the reusable `images.yml`: builds amd64 and arm64, promotes, then publishes the chart) | every merged commit, and the exact published bytes; no Kubernetes |
| docker-desktop Kubernetes v1.34.1, the shared lab | `weirkeeper` and runner images built from a named `main` commit (lab-refresh-2 … -11), a SCRAM Kafka pair, MinIO, the chart's roles and policy; per-task harnesses under `scripts/live/`, `e2e/k8s/d2`, `e2e/k8s/d3` and `scripts/*-ui-e2e.mjs` (Playwright against a host `logweir-api`). Deleted on 2026-09-24 with the user's approval | live controller, runner, console and approval behaviour; **not** NetworkPolicy enforcement (Docker Desktop does not enforce it), AWS S3, MSK, EKS IAM or a real OIDC provider |
| docker-desktop Kubernetes v1.34.1, the PoC profile ([deploy/poc/](../deploy/poc/README.md)) | an arm64 host (Apple silicon), where the `linux/amd64` runner runs under emulation; Traefik chart 41.6.0 (v3.7.13), cert-manager v1.21.2 with a local CA, Dex chart 0.24.1 (v2.44.0) with static users; the console in shared mode, two replicas; the demo Kafka pair and the MinIO mirror; installed and upgraded with Helm from the **published** chart and Docker Hub images only, no local build and no `kubectl patch` | a clean install, upgrades and rollbacks as an adopter makes them, a real TLS ingress and OIDC sign-in, the console as a person uses it; **not** NetworkPolicy enforcement, a corporate IdP bound by group, AWS S3, MSK, EKS, or scale beyond what emulated runner pods allow |
| Offline measurement | the product API over its in-process fake, the pure discovery steps, the console's render functions ([stability.md](stability.md), *Measured scale limits*) | cost at the product's own bounds; no network or real API server |

The lab rounds, from which most Done records come, ran these author-only
builds (`imagePullPolicy: Never`); they say nothing about registry digests:

| Lab build | `main` commit | Controller | Runner |
|---|---|---|---|
| lab-refresh-8 | `f49849d` | `sha256:01338ec0…` | `sha256:ad705ba2…` |
| lab-refresh-9 (and harness-rows-12) | `306cebf` | `sha256:f71fdcb4…` (arm64) | `sha256:db8d8ade…` (amd64) |
| lab-refresh-10 | `b426096` | `sha256:0720cd92…` | `sha256:3e5ddaea…` |
| lab-refresh-11 | `86a554e6` | `sha256:70b06b12…` (arm64) | `sha256:4bb02b04…` (amd64) |

## Commit and image identities: the five publications the PoC ran

Each is one `main` commit published by `ci.yml` as four `sha-<commit>` images
and the chart `oci://registry-1.docker.io/vladyslavhaina/logweir-chart`
`--version 0.1.0-sha-<commit>`, whose image defaults are those four tags. Every
one was pulled anonymously, and every Logweir image carried
`org.opencontainers.image.revision=<commit>`. The image digests are the ones the
PoC pulled by tag and ran (the pods' `imageID`); the runner is `linux/amd64`
only.

| # | `main` commit | CI run | Chart digest | `weirkeeper` | `logweir-console` | `logweir` (runner) | Helm revisions on the PoC | Round |
|---|---|---|---|---|---|---|---|---|
| 1 | `86a554e6` (2026-09-23) | 35957294926 | `sha256:90b4d41b…` | `sha256:7dc60dd7…` | `sha256:5c45eedb…` | `sha256:0aba7749…` | 1–3 (clean install; 2 the Governed demo; 3 back to Ordinary) | `claude/poc-install`, 2026-09-24 |
| 2 | `02dc44b6` (2026-09-24) | 36071480985 | `sha256:7f448172…` | `sha256:c0d5c826…` | `sha256:90894590…` | `sha256:135c9b4f…` | 4 (images, binding off), 5 (binding) | `claude/poc-upgrade-1`, 2026-09-25 |
| 3 | `b748fd5f` (2026-09-24) | 36100597420 | `sha256:3bba9455…` | `sha256:f6f10d96…` | `sha256:4911f3a5…` | `sha256:29c78ea0…` | 6, 7 | `claude/poc-upgrade-2`, 2026-09-25 |
| 4 | `a54fb823` (2026-09-25) | 36129705142 | `sha256:5becb4b9…` | `sha256:61a9116c…` | `sha256:83445db0…` | `sha256:714deedb…` | 8, 9 | `claude/poc-upgrade-3`, 2026-09-25 |
| 5 | `815249cb` (2026-09-25) | 36152835598 | `sha256:820622f2…` | `sha256:70f1f4dc…` | `sha256:de26b7d5…` | `sha256:1b7e2280…` | 10 (images, binding off), 11 (deployed) | `claude/poc-upgrade-4`, 2026-09-25 |

The same, in full, under `docker.io/vladyslavhaina/`:

```text
86a554e6a1a216e5ce84abc805a2274c2893cc21
  logweir-chart     sha256:90b4d41bf5228deb768b32e279523e5a4cdb7da76eef9a9695392848fc9912dd
  weirkeeper        sha256:7dc60dd732114ec1ef384ead0998c4716fd305d0b440a8c28fa914d6f33ea3c9
  logweir-console   sha256:5c45eedbaa604197d62e039aee5513191e0777a6143907c22a7f8b05f461f214
  logweir           sha256:0aba7749428f042f5ddee20c4128f70308b389425fe5df37ee45e0f7ee968422
02dc44b6cf5492df74de1e5860753ff96fbd3c4b
  logweir-chart     sha256:7f44817224b61f13dc47b959d5277c78158011c3a80267ac824822e7206b7e4f
  weirkeeper        sha256:c0d5c826c0d41161737c91b75bc2d1c7983a1c0c83f58340e4c173227a042378
  logweir-console   sha256:908945906e6507f599dbf6dd84112d1c78a43601333cb404466b3be45a1e8cb0
  logweir           sha256:135c9b4f6503c9cd1a0e11127debbf7f5ab8d18162a78061cb1b16131e50ce39
b748fd5f65e8bd7b4ea9755afbb7a983ccc7b3bf
  logweir-chart     sha256:3bba94558093981809184f104186778f5c235f4fa5eb4cb999b651a901918e64
  weirkeeper        sha256:f6f10d9632259c4f34854093657eac4a12386671f68cb16d8f1efa26ea2bf97e
  logweir-console   sha256:4911f3a52524df11873175f879bcfff867cb9855f15afa5078551ba168d2f7e7
  logweir           sha256:29c78ea0133105ee44c3af6eb787a7f40480cad29dff453124e97e61e14faa70
a54fb82385dc6740ecd0cee291ffb6d7de294e72
  logweir-chart     sha256:5becb4b997b55c14d8ffd0b75aa07fde37fa3306c67a3723d84cfa0eecc683bd
  weirkeeper        sha256:61a9116c3027e3434b2edc492c2f441494516ec0562be3e85b091113104bd820
  logweir-console   sha256:83445db0b0137b7c2b640c132d2e0641ec23dca41a6d0f9b05bac80c9f7e749c
  logweir           sha256:714deedbe8f875a47929ca98ca552caf1bd765948caf484cd5a17d2e297cb8ed
815249cb366df13587acd11a3678246ca3aedd27
  logweir-chart     sha256:820622f2504a4ebd19acd482c186d1df0fae7bf95bfc99ac4cd351630f178f6f
  weirkeeper        sha256:70f1f4dc793cb6067f697269d8e51d6edd6e0561f14c66cebdab92ae147e93b3
  logweir-console   sha256:de26b7d5e4862f557ccd341040ef83fcc977abcc0d569180422abfbe26e029b8
  logweir           sha256:1b7e228030e046977d93129b5f2d5624a378939ba71c5120b3d1e0e6cc15a99c
  logweir-ui        sha256:f3d2c088e57065cdea382bb99eaaf4a64538273583c6d3a637d02e82f39d3529 (pulled; not deployed in shared mode)
```

The demo MinIO ran `minio-mirror@sha256:b4c3dc9f…` from the second publication
on; the first ran the withdrawn upstream image from the node's cache.

**The two rehearsal starting points** (published on Docker Hub; checked
2026-09-23):

| Rehearsal | Starting images | Starting chart |
|---|---|---|
| **R1 — the last version tag** | `weirkeeper:v0.1.5` (`sha256:e933e7cc…`), `logweir:v0.1.5` (`sha256:f2a28c93…`), `logweir-ui:v0.1.5` (`sha256:51ead7bf…`); there is **no** `logweir-console:v0.1.5` — the console did not exist | `charts/logweir` at `v0.1.5` (`9cc78a3`): 6 CRDs, no managed identity (the signing Secret provisioned by hand), no console |
| **R2 — the last build before `ac00819`** (the integration of PLAT-15.2/17.2/19.2) | `sha-f49849db035d01ff968df7472914f57fc6c2e988` for all four images (`weirkeeper` `sha256:42a4afaa…`, `logweir` `sha256:443d514e…`, `logweir-console` `sha256:e7b60be7…`) — the lab-refresh-8 build | `charts/logweir` at `f49849d`: 14 CRDs (6 of them change), a console without `controller.watchNamespaces` or `approvalPolicy.*` |

## What the live half of PLAT-20.2 showed

Everything below ran on docker-desktop with the PoC profile, from the published
chart and images, in namespaces the round owned. Receipts and scorecards were
checked independently with `docs/verify_scorecard.py` at every step. Each
round's report and artifacts are in the orchestration store
(`claude/<round>.result.md`, `claude/artifacts/<round>/`).

**1. The clean install and the two rehearsals** (`claude/poc-install`,
2026-09-24, at `86a554e6`; 38 of 42 brief items PASS, 2 FAIL, 2 NOT RUN):
- **Clean install** from an empty cluster by README steps 1–8, after one profile
  fix (Dex needs a writable `/tmp`). The console answered through Traefik with
  TLS from the local CA, HSTS and the security headers; the four users signed in
  through Dex, each with exactly its role, and a fifth user added later with
  none.
- **R1, from `v0.1.5`:** 6 → 14 CRDs; the hand-provisioned signing key
  (`28cf5606…`) adopted by the chart's identity bootstrap; upgrade, `helm
  rollback logweir 1`, and re-upgrade, with the key id and private-key digest
  unchanged in both namespaces throughout; the schedule fired every 4 minutes
  under each controller; 17 of 17 receipts passed the independent verifier; a
  `v0.1.5` point restored with completion 150/150 (with the evidence bucket
  typed by hand, before the item-14 fixes).
- **R2, from `sha-f49849d…`:** release-note item 6's refusal at render (rc 1,
  history unchanged); the managed identity (`5e35555b…`) kept
  (`source=existing`); items 7, 8 and 10 shown, and item 9's old behaviour on
  the baseline; rolled back to `f49849d` with 8 of 8 receipts passing.
- **PLAT-17.2 on the real entry point:** 58/58, the expired session 3/3, the
  no-role user 5/5.
- **Recovery:** README §10 and the J1–J7 console journey (except J3's *Test
  access*, which never showed its result: P8); a catalog-verified point
  restored through the wizard (`Valid`, 150/150); a Governed restore approved by
  a second person.
- **Measured at 258 real points** ([stability.md](stability.md#measured-scale-limits-plat-202)):
  catalog `Full` sync 58.5 s; topic discovery 27.0 s fresh, 0.1 s reused;
  `GET …/backups` of 258 rows 3.0 s; catalog points 6.6 s; one operation's
  status p50 71 ms (54 ms eight at a time); console render 0.7–2.1 s.
- The round found ten product defects (P1–P10) and eleven profile and doc
  defects (D1–D11). All were fixed; the first upgrade closed P1–P10, D1–D5 and
  D11 live, and D6–D10 stand on this round's own observation.

**2. Four in-place upgrades of the running install**, each by
[deploy/poc/](../deploy/poc/README.md) *Upgrade to a newer publication*, verbatim:
the new chart's CRDs applied `--server-side --force-conflicts` and Established
with an empty `kubectl diff`; then `helm upgrade` with the approval-policy
binding off; then `helm upgrade` with it. Nothing was in flight when the images
moved, and the identity hooks logged `source=existing` each time.

| Upgrade | Helm | What it kept | What it proved | Rows |
|---|---|---|---|---|
| `86a554e6` → `02dc44b6` (`claude/poc-upgrade-1`) | 3 → 5 | identity `d1d189d6…` and its private-key digest in both namespaces; a 5-minute schedule fired every slot across the upgrade; receipts 259 → 261, all `VALID`; pre-upgrade points restored with completion, 6 of 6 scorecards passing | P1–P10 closed live, among them: 20 manual runs from two people held to at most 4 runner pods, the rest `Queued`, through a controller restart; 3 restores at once ran 2 and queued 1; the per-person `429` at the 21st backup and the 11th restore (two console replicas) | 51 PASS, 1 FAIL, 7 NOT RUN |
| `02dc44b6` → `b748fd5f` (`claude/poc-upgrade-2`) | 5 → 7 | the pre-upgrade `KeyCompromise` check printed nothing; identity unchanged; receipts 294 → 296 → 314, all `VALID` | the three legacy points the first upgrade left `NotAttempted` turned `Valid` 3 s after the new controller started, and one restored 150/150 (P12); duplicate catalogs refused, takeover in 28 s (P11); a compromise recorded on one policy reached another in ≤ 2 s and held the recorder's deletion (minted key only); the console fixes from MCP round 1; 8 of 8 restore scorecards passing | 111 PASS, 2 FAIL, 7 NOT RUN |
| `b748fd5f` → `a54fb823` (`claude/poc-upgrade-3`) | 7 → 9 | the pre-upgrade check printed nothing; 7 CRD generations moved (descriptions only), UIDs unchanged; identity, private digest, Secret UIDs and `logweir-signing-trust` unchanged; a 5-minute schedule fired 13 of 13 slots across both controller swaps; **receipts 314 → 316 → 328, all `VALID` (328/328)** | P13 and P14 closed live; README §10 clean end to end as a new user, in the console; J1–J7 14/14; 2 of 2 restore scorecards passing | 97 PASS, 1 FAIL, 8 NOT RUN |
| `a54fb823` → `815249cb` (`claude/poc-upgrade-4`) | 9 → 11 | the pre-upgrade check printed nothing; no CRD generation moved (the two commits change no CRD); identity, private digest, Secret UIDs and `logweir-signing-trust` unchanged; a 5-minute schedule fired 15 of 15 slots across both controller swaps; **receipts 329 → 332 → 344, all `VALID` (344/344)** | P15 closed live on every follower: Test connection, the readiness panel, the schedule form, restore step 5 and Test access each read a 100–170 s check past its old budget to its verdict without a reload, with the read cadence in the product API's own request log, and Discover topics, which had no follower, settles on the page; MCP round 3's R3-1 (at the click and at the verdict), R2-10, R3-2 and R3-3 live; README §10 clean end to end as a new user; J1–J7 14/14; 2 of 2 restore scorecards passing | 100 PASS, 2 FAIL, 2 NOT RUN |

The FAILs: the first upgrade's was its `v0.1.5`-era row — no
`v0.1.5`-written point existed on the install, and the pre-upgrade
inline-archive points stayed `NotAttempted` (P12, fixed in `b748fd5f`); the
second's were two new console defects, P13 and P14, fixed in `a54fb823`; the
third's is P15, fixed in `815249cb`; the fourth's are one new console defect,
P16 (below), which R3-1's stricter row found.

**3. The console as a person uses it** (the orchestrator, through the Playwright
MCP, clicks only; 2026-09-24/25):
- round 1 at `86a554e6`: 34 findings, 5 of them high (no Sign in when signed
  out, raw problem JSON, no signed-in identity or Sign out, a clipped primary
  action, a 22,686 px wizard);
- round 2 at `b748fd5f`: round 1's findings fixed; the wizard usable 1.3 s
  after the click (it was 12.4 s); 16 new findings, 6 of them medium;
- round 3 at `a54fb823`: the 6 medium and 6 of the low round-2 findings fixed,
  one mitigated, two not re-checked; one low round-2 finding and 5 new low or
  low–medium ones open, none of which blocks a restore or misstates a verdict.

**4. What each round exercised of the release notes' items:**

| Item | R1 (`v0.1.5`) | R2 (`sha-f49849d`) | The in-place upgrades |
|---|---|---|---|
| 1–3 retention | no (no `RetentionPolicy` kind) | not set up | not set up |
| 4 runs without an exit code | no | not set up | — |
| 5 point-bound restore across the upgrade | no | not set up | — |
| 6 shared-console values | no (no console) | refused at render, then migrated | — |
| 7 approval policy | an Ordinary restore after the binding step | shown | Ordinary restores in every round |
| 8 completion from a valid scorecard | shown | shown | every restore |
| 9 no slot before creation | the old behaviour shown on `v0.1.5` | the old behaviour shown on `f49849d` | shown at `86a554e6` |
| 10 `trust.state` | no (no API) | shown | — |
| 11 one engine run per execution | the first run under the new controller wrote its claim | — | — |
| 12, 13 controller with runner; readiness principals | crossed | crossed | the runner moved with the controller in one `helm upgrade`; no readiness check after the first upgrade met `CheckContractMismatch` |
| 14 a point with no saved destination | — | — | the first upgrade (a point written after it), the second (points written before) |
| 15 manual-run pool | — | — | the first upgrade |
| 16 compromise guard | — | — | the second upgrade (minted key only) |
| 17 an admitted Restore's `Approval` (P9) | — | — | the first upgrade |
| 18, 19 (P11, P12) | — | — | the second upgrade |
| 20 (P14) | — | — | the third upgrade |

## Limitations carried by this release

1. **The large catalog was measured at 258 real points, not the 1,000 PLAT-20.2
   names.** The host could not run more pods of the emulated `amd64` runner,
   and before item 15 nothing bounded manual runs. The install now holds 344
   backups. [stability.md](stability.md#measured-scale-limits-plat-202) has the
   live timings and the offline rows at 1,000 and 5,000 rows.
   [UNVERIFIED — a 1,000-point archive was not reached live; 258 real points were measured on docker-desktop.]
2. **The residue of the last round's record** (`claude/poc-upgrade-3.result.md`
   §7):
   - R2's pre-upgrade states for release-note items 1–5 were not set up
     (retention `Enforce` without `s3:GetObject`, a versioned bucket, a shared
     set, a run whose ConfigMap never mounts, a point-bound Restore with no Job);
     those items stand on the lab rows their records name.
   - G1's three negatives (a CA-issued certificate at Dex's address, a renamed
     bundle key, an empty bundle) were not run live; the chart's tests carry them.
   - No disaster restore ran in a namespace with no `Backup` objects.
   - No point written by the `v0.1.5` runner went through the fixed legacy path
     (items 14 and 19) on an upgraded install; the points proven there were
     written by `sha-86a554e6`.
   - Not run live: trust L9 (evidence re-derived in another watched namespace);
     P12-L5/L6/L7 and M1; P14-D1 (*Discover topics* after its inventory went
     stale, which the 15-minute session makes unreachable); R2-14's Governed
     and mixed-role rows; H5 (the lab is gone).
   - D6, D7 and D10 (what a rollback to `v0.1.5` and an uninstall delete) can be
     shown again only by a rollback or an uninstall. No rollback of an in-place
     upgrade was run on the long-lived install; R1 and R2 rolled back live.
   [UNVERIFIED — R2's pre-upgrade states for release-note items 1–5 were not set up on the PoC.]
3. **P15 is fixed in `815249cb` and proven live** (`claude/poc-upgrade-4`):
   every console follower reads its check until the longest time a check may
   take, and says so, with *Run the check again*, if that passes. On the PoC,
   checks of 100–170 s — a store endpoint nothing answers, and checks queued
   behind the namespace's four check slots — were read to their verdicts
   without a reload on the five readiness followers, and *Discover topics*,
   which had no follower, now settles on the page. **P16, open (console, low):** at
   restore step 5 the first repaint of a running check scrolls the page to its
   top, so at 390 px the focused status line is off screen until the verdict
   lands (the verdict itself is brought into view). Scroll back to the status,
   or wait for the verdict.
4. **The MinIO mirror.** MinIO withdrew its public images; the chart's demo
   MinIO, the e2e stack and the PoC run the same releases rebuilt from the
   archived source (`vladyslavhaina/minio-mirror` and `mc-mirror`, AGPL-3.0;
   MINIO-IMAGES-WITHDRAWN, merged `e1700b7`). Replacing MinIO with a maintained,
   permissively licensed S3 server (REPLACE-MINIO) is open and not started.
5. **`product-expansion.md` is not started**, by the user's decision.
6. **Standing limits:** no AWS S3, MSK, EKS IAM or corporate identity provider
   has been run; Docker Desktop cannot show a NetworkPolicy deny; a `Restore`
   is not held by a retention lease; there is no in-place runner signing-key
   cutover; the product API's OpenAPI document is still `1.0.0-alpha.1`. The
   per-person run limit counts per console process, so two replicas double it
   (RATE-LIMIT-PER-CONSOLE-PROCESS), and an execution whose first run was made
   by a runner older than item 11 can still be overwritten if its Job is
   re-created after the upgrade (RECEIPT-DUP-UPGRADE-WINDOW). The release
   notes' *Limitations and open items* and [stability.md](stability.md)'s
   *Known limitations* are the full list.

## Rollback

`helm rollback logweir <revision>` restores the previous chart **and** its
images together, because a published chart names its own commit's four images.
Run the checklist in [release-notes.md](release-notes.md), *Migration and
rollback*, first: retention policies to `Report` and their leases clear;
`SourceConnection` preflights deleted; standing rehearsal `Approval`s deleted
when rolling back past Amendment G; destination-backed and `v2`-frozen backups
finished; point-bound restores given a Job or recreated after; approval
policies unbound; controller and runner rolled back together, the runner not
after the controller; objects the chart adopted re-created when going back to
`v0.1.5`; every compromised key out of the roster. **The CRDs stay**: Helm never
removes `crds/`, and every CRD change in this release is additive. Back up the
installation identity before either direction
([install.md](install.md), *Back up and recover the installation identity*).
Archives, evidence and catalog records are untouched either way.

**The concrete case on the PoC: revision 11 back to revision 9.** Revision 11
is `815249cb` with the approval-policy binding; revision 9 is `a54fb823` with the
same binding.

```bash
. deploy/poc/versions.env; CTX=docker-desktop
helm rollback logweir 9 --kube-context "$CTX" -n "$LOGWEIR_NAMESPACE" --wait
```

Helm records it as revision 12. The CRDs stay: `a54fb823` → `815249cb` changed
no CRD. Going back to revision 9 removes no release-note item; it brings back
P15 (a check slower than the page's follow is left "not finished") and the
round-3 console findings, because the console is the only image whose
behaviour those two commits changed. Going further back crosses items: to
revision 7 (`b748fd5f`) item 20, whose CRD change was descriptions only
(seven generations moved); to revision 5 (`02dc44b6`) also items 16, 18 and
19; to revision 3 (`86a554e6`) also 14, 15 and 17 — and each item's
**Rollback:** paragraph in the release notes says what returns.
[UNVERIFIED — this rollback was not run on the PoC; the rollbacks run live were R1's and R2's.]

---

Documentation is licensed [CC-BY-4.0](LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
