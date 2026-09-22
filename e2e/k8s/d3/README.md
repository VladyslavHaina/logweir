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
  into its own namespace and dials that namespace's Kafka and MinIO by service
  DNS. **It changes nothing outside its own namespace** except the one
  cluster-scoped `TrustPolicy` the trust phases need.
- `minio/mc:latest` and `busybox:latest` present on the node
  (`imagePullPolicy: Never`).
- **An image carrying `logweir-retention`**, for the five enforcement phases
  only. The product runner image now carries it, so the default is the
  `LOGWEIR_RUNNER_IMAGE` the shared controller itself names — see
  §"The enforcer's image". `--retention-image` overrides it to measure a build
  the lab is not running; a phase records **NOT-RUN** only when neither can be
  resolved.
- A python3 with no third-party packages.

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

`trust`, `signed-at-probe` and `cleanup` touch one cluster-scoped object (the
`TrustPolicy`, which binds only this run's namespace). Take the orchestration's
lock around exactly those phases and release it immediately afterwards. Every
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

- Every object it creates carries `logweir.dev/test-owner=d3w14`; `cleanup`
  re-reads the namespace's label AND compares the UID it recorded at `setup`
  before deleting anything, and refuses a bucket whose name is not this run's.
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
