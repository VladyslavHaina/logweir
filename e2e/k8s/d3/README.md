# D3 live acceptance on docker-desktop

`d3_live.py` is the harness for decision D3 §15's catalog, retention and trust
scenarios — the live half of PLAT-15.1, PLAT-16.1 and PLAT-16.2, with evidence
toward PLAT-14.2 and PLAT-19.1. It is a *live* harness: it creates real
`Backup`s against a real Kafka, writes real objects into a real MinIO, and
reads the verdicts the controller wrote. A fixture-only run of it proves
nothing and it has no fixture mode.

## What it needs

- A docker-desktop cluster running the chart's controller. Every command it
  issues carries `--context docker-desktop`; it names no other context.
- The shared `logweir-scram-local` lab: it copies that namespace's
  `source-scram`, `logweir-s3`, `logweir-signing-key` and `minio-root` Secrets
  — and, for `rehearsal`, `target-scram`, the scratch broker's own SCRAM
  credential — into its own namespace and dials that namespace's Kafka and
  MinIO by service DNS. **Outside its own namespace it writes exactly two
  things**: the cluster-scoped `TrustPolicy` the trust phases and `rehearsal`
  need (deleted only after an owner-label check), and, in `rehearsal`, topics
  on the shared `kafka-target` broker — a witness named for this run and names
  under this run's own rendered prefixes — which the phase deletes again after
  an ownership check (see below). It never changes the shared release.
- `openssl` on the host, for the phases that mint a keypair (`old-archive`,
  `multiple-namespaces`, `rehearsal`).
- For `old-archive`: the lab roster's approver PRIVATE key, `approver.pem`,
  read from `$LOGWEIR_SCRAM_OUT`, else `$HOME/.logweir-lab/scram-e2e` (the lab's
  durable home since 2026-09-22), else the `/tmp/logweir-scram-e2e` symlink to
  it. It must be the private half of `TrustRoster/default.spec.approverKeys[0]`
  — after a lab rebuild that is the NEW approver key, and a private half left
  over from the previous lab is refused by the cluster.
- For `rehearsal`: a `logweir` binary with `drill approve --standing` —
  `LOGWEIR_BIN`, else the more recently built of `target/release/logweir` and
  `target/debug/logweir`. The phase asks the binary for `--standing` first and
  refuses, by name, one that lacks it.
- `minio/mc:latest` and `busybox:latest` present on the node
  (`imagePullPolicy: Never`).
- **An image carrying `logweir-retention`**, for the five enforcement phases
  only. The product runner image now carries it, so the default is the
  `LOGWEIR_RUNNER_IMAGE` the shared controller itself names — see
  §"The enforcer's image". `--retention-image` overrides it to measure a build
  the lab is not running; a phase records **NOT-RUN** only when neither can be
  resolved.
- A python3 of version 3.12 or newer with no third-party packages (the shared
  venv `/tmp/logweir-roadmap-run/venv/bin/python3` on the lab host; the macOS
  `/usr/bin/python3` is 3.9 and fails at the first PEP 701 f-string).

## Running it

```bash
export LOGWEIR_D3_STAMP=$(date -u +%Y%m%dt%H%Mz)
export LOGWEIR_D3_OUT=/tmp/d3-live/$LOGWEIR_D3_STAMP
RET=logweir:scram-local            # optional; the default is the controller's
                                   # own LOGWEIR_RUNNER_IMAGE — see below

python3 e2e/k8s/d3/d3_live.py setup            # namespace, destinations, buckets
python3 e2e/k8s/d3/d3_live.py catalog          # PLAT-15.1: reconstruction after CR loss
python3 e2e/k8s/d3/d3_live.py catalog-cases    # duplicate identity, Missing, stale index, format
python3 e2e/k8s/d3/d3_live.py catalog-scale    # PLAT-15.1: 104 REAL points past `viewLimit`
python3 e2e/k8s/d3/d3_live.py catalog-access   # PLAT-15.1: partial access, corrupt, future major
python3 e2e/k8s/d3/d3_live.py retention        # PLAT-16.1: two destinations, honest reports
python3 e2e/k8s/d3/d3_live.py legal-hold       # PLAT-16.2: spec.holds[]
python3 e2e/k8s/d3/d3_live.py packaging        # proves the shipped image DOES carry the enforcer
python3 e2e/k8s/d3/d3_live.py preview                 --retention-image $RET
python3 e2e/k8s/d3/d3_live.py no-evidence-credential  --retention-image $RET
python3 e2e/k8s/d3/d3_live.py enforce                 --retention-image $RET
python3 e2e/k8s/d3/d3_live.py wrong-prefix            --retention-image $RET
python3 e2e/k8s/d3/d3_live.py denied-deletion         --retention-image $RET
python3 e2e/k8s/d3/d3_live.py trust            # CLUSTER LOCK (TrustPolicy is cluster-scoped)
python3 e2e/k8s/d3/d3_live.py signed-at-probe  # CLUSTER LOCK
python3 e2e/k8s/d3/d3_live.py notify           # PLAT-14.2: staleness, dedup, transport
python3 e2e/k8s/d3/d3_live.py protection-cases # PLAT-14.2: recovery, unavailable archive, scope
python3 e2e/k8s/d3/d3_live.py protection-verdicts  # PLAT-14.2: PointFactsUnread, a refused
                                               #   signature, D3 §3.3's resolve column
python3 e2e/k8s/d3/d3_live.py rehearsal       # D3 §15 L6 / PLAT-14.3: a rehearsal
                                               #   end to end. CLUSTER LOCK
python3 e2e/k8s/d3/d3_live.py control          # the negative control — it MUST fail
python3 e2e/k8s/d3/d3_live.py report
python3 e2e/k8s/d3/d3_live.py cleanup          # CLUSTER LOCK (deletes the TrustPolicy)
```

`catalog-scale` and `catalog-access` each create their OWN bucket and destination (`-c`, `-d`)
rather than reusing dest-b: `retention` asserts dest-b holds exactly its six points, and a
corrupt manifest planted there would fail a phase that never asked for one. `catalog-scale`
runs 104 real Backups, which is the only honest way past `viewLimit`'s CRD floor of 100 —
nothing here edits that floor. `protection-cases` needs `catalog` (for the view it refreshes)
and `notify` (for the namespace's first alert), and both are declared in
`PHASE_PRECONDITIONS`.

`notify` and `protection-cases` each create the point their policy measures, as a manual run
OF a `BackupSchedule` (`spec.scheduleRef.{name,uid}` — D3 §3.2: "the `spec.scheduleRef.uid`
field is the authority and the label is the index"). Without it `identity::is_run_of_schedule`
excludes a manual `Backup` from a policy naming `scheduleRefs`, the policy has an EMPTY
candidate set, and `Unprotected` is an answer about nothing — which `status.health` cannot
distinguish from the real verdict, so all three rows carry `selector_matched_a_run`. They name
DIFFERENT schedules (`keeps-running`, `recovery-runs`) so neither selects the other's point.
`notify` also ages its point past the objective, because `catalog` deletes every dest-a CR to
prove reconstruction and the `Stale` arm had nothing to be stale about; that makes the phase
take about six minutes.

`rehearsal` is D3 §15's **L6** and PLAT-14.3's live half: ten steps as ten rows, every
clause named after `claude/plat14-3b.review.md` §4's own words. It needs `catalog` (it
restores a point out of dest-a and writes the rehearsal's evidence back into it) and
`trust` — an ORDERING, because `trust`'s last row leaves the lab signing key **Revoked**
on this namespace's `TrustPolicy` and a rehearsal that inherited that would fail at the
point's evidence verdict for a reason belonging to the previous phase; `rehearsal_trust`
deletes and recreates the object (the CRD's `spec.keys` is append-only with no
Revoked->Active transition), which is only correct after that phase has run. It mints the
standing authorization with the SHIPPED signer, `logweir drill approve --standing`
(`docs/kubernetes.md` §7g) — never in python — so a `logweir` binary that has that flag
is required (see "What it needs"). It signs with an approver keypair it MINTS per run
rather than the lab roster's, so the phase does not depend on the lab's approver private
key at all (which macOS deleted once from `/tmp`, measured 2026-09-22). Both minted
private halves — the Active one and the Retired one step 10 needs — are 0600 inside a
0700 directory, are minted inside the phase's `try`, never enter an object or an
artifact, and are deleted in its `finally` even when the broker sweep before them raises
(`rehearsal-minted-private-keys-never-outlive-the-row`).

**On the shared scratch broker it touches only what it made.** The unrelated topic D3
§15 L6 names is `rehearsal-not-ours-<ownertag>-<stamp>` — the scenario's name as a
prefix, this run's identity as the rest — because one fixed name on a shared broker is
nobody's: two runs each deleted the other's witness. It is planted only if absent (a
pre-existing one stops the phase, and is neither adopted nor deleted), and deleted only
if this run planted it. Every topic under a rendered prefix `rehearsal-<uid[..8]>-` of a
`RehearsalSchedule` this run created is deleted at the end — after the schedule is
suspended and quiet, and after the live object's owner label and uid are checked
against the ones its create returned. `rehearsal-shared-broker-left-as-found` records
that sweep and fails if anything under those prefixes, or the witness, is left.

**The four arms run one after another, never together.** All four name the same
`rehearsal-target`, and the controller checks `TargetBusy` (another schedule's rehearsal
still running against the target) BEFORE the authorization, so an arm left firing
turns the next arm's row into a verdict about the harness. Each arm is suspended as
soon as its row is recorded, and the next starts only when the previous one has no
rehearsal running (`quiesce_arm`; a rehearsal held at admission with no Job is deleted,
because it would hold the target busy for ever). `l6-rehearsal` is suspended only after
step 5's read, because a suspended schedule records no `lastSucceeded`.

**Steps 2-9 are a chain, and a step whose input never existed is recorded `NOT-REACHED`.**
That is a third verdict on purpose: on a controller image that predates PLAT-14.3b the
standing-authorized `Restore` holds terminally at `ApprovalNotReceived` — the documented
fail-closed rollback for an older controller — so step 2 FAILS, which is that defect
reproduced live, and steps 3-9 have no Job, no scorecard and no mapped topic to read.
Recording those as passes would credit the product for assertions that never ran and as
failures would blame it for a chain the first link broke. Step 2 requires ADMISSION — a
runner Job, or phase `Running`/`Succeeded` — not merely a reason other than
`ApprovalNotReceived`: a standing `Restore` refused `StandingAuthorizationRefused` or
`PlanHashMismatch` fails it.

The verdicts this phase can record, and what each means:

| verdict | meaning |
|---|---|
| `PASS` | every clause held |
| `FAIL` | a clause did not hold — a product fact, or a precondition the row names |
| `NOT-REACHED` | the row's input never existed (the chain broke earlier), or — step 7 only — the first rehearsal finished before the controller was obliged to evaluate the next slot against it (the next `* * * * *` boundary + `REQUEUE_SECONDS` (30) + 15 s). Never a pass. |
| `HARNESS-FAULT` | the harness's own ordering decided the row: a `TargetBusy` from another arm of this run, or (steps 5/6) a later ten-minute slot that fired before the arm was suspended. Names the busy `Restore`. Never a pass, and never blamed on the product. |
| `INCONCLUSIVE` | step 10 only: the refusal held, but the mechanism was not shown — the refused `Approval` did not say `KeyRetired`, or the passing arm's `Approval` was not `Verified=True` in the same run, so a build on which nothing verifies would look the same |

Step 7 FAILS on a second `Restore` listed while the first was still running, and on a
first rehearsal still running after that obligation with no skip recorded. Step 10, the
negative control, is independent of the chain and runs either way; it REQUIRES the
refusal it records, because zero Jobs is also what a build that never reconciles this
kind at all leaves behind — and it counts Jobs and bundle `ConfigMap`s by the child name
prefix `logweir-rehearsal-l6-refused-`, not only through `Restore`s, so a schedule-side
refusal does not make "zero" true by construction. Its `RehearsalHealthy` clause accepts
`False` or `Unknown/NoResult` — a spec correction of review §4 step 10's "`False`",
because a schedule refused before any rehearsal finished has no result to be false about
(`rehearsal_schedule.rs::REASON_NO_RESULT`).

The offline tests: `python3 -m pytest e2e/k8s/d3 e2e/k8s/d2`. `test_rows.py` drives every
row's predicate over planted-wrong fixtures; `test_rehearsal_sim.py` runs `rehearsal()`
itself, unmodified, against a fake cluster and controller on a fake clock, with mutants
for each defect the loops exist to catch. It is a model of the controller, not live
evidence.

`protection-verdicts` needs only `setup`: it creates its own three policies, its own two
catalogs, its own legacy destination and every Backup it measures, because its rows assert
exact alert ledgers and an incident another phase opened on the same policy would make
every count in them somebody else's. Three of its rows are written against the CORRECT —
fixed — behaviour of `PROTECTION-SECRETKEYS-UNPROTECTED`, so on a controller image that
predates the fix they FAIL, and that failure is the defect's live reproduction; each row's
detail says which of the two a reader is looking at, and `verdicts/controller.json` records
the image's `org.opencontainers.image.revision`. It writes no cluster-scoped object: the
refused signature is produced by SIGNING with a key the installation has never heard of
(this namespace's `logweir-signing-key`, restored afterwards) rather than by revoking a
known one, which would need a `TrustPolicy` and the cluster lock.

Who this run is, and where: `LOGWEIR_D3_OWNER` (default `d3w14`) is the
`logweir.dev/test-owner` label `cleanup` checks, the prefix of the buckets and of the shared
MinIO's minted user and policy — the shared MinIO has no namespaces, so two workers running
this harness at once must not share it. `LOGWEIR_D3_NS` names the namespace (default
`<owner>-<stamp>`).

`--retention-image <ref>` may also be given as `LOGWEIR_D3_RETENTION_IMAGE`.
Each phase appends to `$LOGWEIR_D3_OUT/state.json`, so they run one after
another in one namespace and `report` renders the whole table. The order above
matters in three places: `catalog` before `catalog-cases`, `retention` before
the enforcement phases (they plan against its points), and
`no-evidence-credential` before `enforce` (it asserts that nothing was deleted,
which is only meaningful while the plan's objects are still there). Run
`control` before `cleanup` so the negative control gets its live read.

## The cluster lock

`trust`, `signed-at-probe`, `rehearsal` and `cleanup` touch one cluster-scoped
object (the `TrustPolicy`, which binds only this run's namespace). Take the
orchestration's lock around exactly those phases and release it immediately
afterwards. Every
other phase is namespaced and needs no lock, and **no phase changes the shared
release** — not its image, not its env, not its CRDs. `packaging` reads the
controller's `LOGWEIR_RUNNER_IMAGE`; it never writes it.

## The enforcer's image

`crates/weirkeeper/src/controllers/retention_policy.rs` renders every
enforcement Job's command as `logweir-retention` and takes that Job's image from
the controller's `LOGWEIR_RUNNER_IMAGE`.

**RET-NOIMAGE was that no image this repository built contained that binary** —
the product `Dockerfile` ran `cargo build … -p logweir` and copied
`/usr/local/bin/logweir` alone, so `mode: Enforce` produced a Job that died at
the kubelet with exit 127, `executable file not found`, which the `packaging`
phase proved live.

**That defect is closed.** `Dockerfile` builds `logweir-retention` beside
`logweir`, and `scripts/check-image.sh` check 7 refuses a runner image that does
not carry it. `packaging` now proves the inverse from the same probe: the
container starts and exits `3`, `logweir_retention::EXIT_REFUSED` — the
enforcer's own refusal when it is given no plan, which only a binary that exists
and resolves by bare name through `$PATH` can produce. The row is named
`retention-enforcer-ships-in-the-runner-image`; it was
`retention-enforcer-ships-in-no-image` and asserted `not found` here. So the
enforcement phases default to the controller's own `LOGWEIR_RUNNER_IMAGE` and
need no hand-built image.

`Dockerfile.retention` beside this file stays as the *test* recipe for an
enforcer image the lab is not running — a build under review, or one from
another commit — reached with `--retention-image`. It starts from the product
`Dockerfile`'s own `builder` stage rather than copying it, so there is no second
home for the cross-compile setup to drift in, and it changes neither the product
`Dockerfile` nor the chart. Nothing in `justfile` or CI references it:

```bash
docker build --platform linux/amd64 --target builder \
  -f Dockerfile -t logweir-builder:d3w14 .

docker build --platform linux/amd64 --load \
  -f e2e/k8s/d3/Dockerfile.retention \
  --build-arg BUILDER_IMAGE=logweir-builder:d3w14 \
  --build-arg RUNNER_IMAGE=logweir:scram-local \
  --label org.opencontainers.image.revision="$(git rev-parse HEAD)" \
  -t logweir:scram-local-d3w14 .
```

The resulting image is the shipped runner plus one binary, so the enforcement
rows are about the enforcer and not about a runtime that differs from the
product's. `--label …revision` is not decoration: a locally built image has no
other way to say which commit it came from, and `WORKER-RULES.md` requires it.

**The enforcement scenarios are operator-run by design**, not a swap of the
shared controller's image. `docs/kubernetes.md` §7f says the real per-candidate
object count comes from an operator running `logweir-retention run … --dry-run`;
this harness runs that, and the enforced pass beside it, as Jobs in its own
namespace against the plan it wrote. Those five phases record NOT-RUN only when
no image can be resolved at all — neither an override nor the controller's own
`LOGWEIR_RUNNER_IMAGE` — which is the honest verdict, and not a skip.

## Safety rules it enforces in code, not in prose

- Every object it creates carries `logweir.dev/test-owner=<owner>`; `cleanup`
  re-reads the namespace's label AND compares the UID it recorded at `setup`
  before deleting anything, and refuses a bucket whose name is not this run's.
  Every phase that deletes a cluster-scoped `TrustPolicy` checks that label
  first and refuses another run's policy of the same name
  (`delete_owned_trust_policy`).
- Retention enforcement only ever runs against the buckets this run created.
  The fixture's `kafka-backups` is written (the legacy-archive Backups the
  Backup-level evidence verdict needs live there) and never deleted from.
- The one credential the harness mints — the read-only MinIO user the
  denied-deletion case needs — is generated per run, registered in `MINTED`, and
  scrubbed out of every recorded argv, every captured stream and every artifact.
  `redact` runs over both before either is written.
- `report` re-reads every artifact and fails the run on anything
  credential-shaped **or on any minted value**, and `sweep_selftest` plants both
  shapes first and fails the run if the sweep misses either — so the clean sweep
  means something.
- `control` asserts something false about a **live** read of the catalog's
  pages, through the same digest-checked path the catalog rows use, and fails
  the run if it passes.
