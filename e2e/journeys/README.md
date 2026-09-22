# PLAT-20.1 cross-layer regression journeys

One command runs the tracker's named journey set against the docker-desktop lab
and writes one machine-readable verdict:

```bash
export LOGWEIR_PYTHON=/tmp/logweir-roadmap-run/venv/bin/python3   # any python >= 3.12, stdlib only
export NODE_PATH="$(npm root -g)"                                  # resolves playwright
cargo build --locked -p logweir -p logweir-api                     # target/debug/{logweir,logweir-api}

$LOGWEIR_PYTHON e2e/journeys/run.py plan        # the journeys, their rows and the phases; touches nothing
$LOGWEIR_PYTHON e2e/journeys/run.py run         # every un-gated journey, live (about 90 minutes)
$LOGWEIR_PYTHON e2e/journeys/run.py run --journeys overlap,cr-loss
$LOGWEIR_PYTHON e2e/journeys/run.py run --open lab-refresh-8     # once the refresh has landed
$LOGWEIR_PYTHON e2e/journeys/run.py summarise --out <run dir>    # re-judge a finished run offline
```

The run writes to `/tmp/logweir-roadmap-run/claude/artifacts/plat20-1/<stamp>/`
(`--out` overrides it). It exits 0 only when `summary.json` says `"ok": true`.

The offline tests are `$LOGWEIR_PYTHON -m pytest e2e/journeys -q`. They are not a CI job:
PLAT-20.1 adds no mandatory gate, and the simplified pipeline stays as it is.

## Design

**It composes the existing harnesses instead of copying them.** The existing live
suites already prove most of the tracker's cases against real archives and real
controller verdicts:
- `scripts/test-plat06-live.py` and `scripts/test-plat07-live.py`;
- `scripts/live/d1/`, `e2e/k8s/d2/` and `e2e/k8s/d3/`;
- the console scripts under `scripts/*-ui-e2e.mjs`.

A second copy of those checks would drift from the first. So the runner:
- invokes each harness as a subprocess, one phase per process;
- points it at its own namespace, owner label, archive prefix and output directory;
- reads its result document back through a small adapter (`suites.py`).

Journeys that no harness proves are implemented once. `native.py` holds the
controller/API journeys and `console.mjs` holds the browser ones.

The runner is Python with no third-party packages because the harnesses it drives
are. It needs no import from them, and it asks nothing of them but the command line
and result documents they already have. No existing harness was edited.

**The verdict arithmetic is strict by construction** (`core.py`, tested with
planted-failure twins in `test_core.py`):

| verdict | meaning |
|---|---|
| `PASS` | every row the journey names was read back from its harness's own result document as an exact pass |
| `FAIL` | a row failed, was missing, was `NOT-RUN`, `NOT-REACHED`, `BLOCKED`, `PARTIAL` or anything else, or its harness did not exit cleanly |
| `SKIPPED` | the journey declares `requires: lab-refresh-8` or `requires: PLAT-19.2` and that gate was not opened. The reason is printed. Never a pass. |

A harness's exit code counts too. Anything other than 0 fails every journey reading
that suite, except where `suites.py` states why (`why_rcs`, pinned by
`test_catalogue.py`):
- plat10 exits 3 exactly when it records a `blocked` row, and such a row is never a pass.
- plat12-13 and plat11-2 exit 1 when one of their journeys throws. Each records a
  journey only after all its checks hold, and stops at the first throw, so earlier rows
  are fully asserted.
  - This is accepted only when their result document records their namespace cleanup.
  - The failure is kept as a suite note in `summary.json`.

`summary.ok` needs four things:
- no FAIL;
- at least one PASS (a run of only SKIPPED is not ok);
- a clean credential sweep;
- a sweep self-test that killed its planted probe.

Before trusting itself, every run also feeds `summarise` a planted catalogue:
- a failing row;
- a missing row;
- a crashed harness;
- a closed gate.

It refuses to continue unless each comes out exactly as it must (`core.summary_selftest`).

**A journey is about data, not text.** Each row is classified by what its ASSERTION
line reads: `archive-data`, `evidence`, `durable-resource` or `rendered-text`.
`catalogue_violations` refuses:
- a journey made only of text;
- a journey that reads no durable resource back;
- a backup/restore journey (`data=True`) that reads neither archive data nor evidence.

`test_catalogue.py` re-reads every cited file. It fails when a cite points past the
end of the file, or when a composed row's name is no longer in the harness that
should record it.

**Failures leave redacted diagnostics.** When a suite has a non-passing row, the
runner snapshots its namespaces before the suite's own cleanup deletes them:
- every Logweir kind, Jobs, pods, ConfigMaps and events, never a Secret;
- the shared controller's log.

Each FAILED journey gets `diagnostics/<id>/diagnostics.json`, containing:
- the failing rows;
- the owning harness's own words for them;
- the tail of every phase log;
- pointers to the snapshots.

Everything written passes `core.redact`.

**The credential sweep can itself fail.** `core.sweep` reads every file under the
run directory, screenshots and Playwright traces included, and searches for two
things.
- **Patterns:** a PEM private-key header, `password`/`aws_secret_access_key`
  key-values, a positional `mc admin user add` secret, and a dumped Secret's `data`.
- **Exact values:**
  - every value of the shared lab's Secrets (`source-scram`, `target-scram`,
    `logweir-s3`, `logweir-signing-key`, `minio-root`), raw and base64;
  - the approver and signing private keys;
  - every secret in the run's private tree: plat07's `passwords.json`, d2's
    `credentials.json`, and every private key. State files are never needles: their
    strings are namespace names, and taking them made a live trial report 249 false
    hits.

These are loaded into memory only and never written. `core.sweep_selftest` plants a
minted exact value, a positional secret and a PEM header, and the run fails unless
the sweep reports all three. This is d3's `sweep_selftest`, extended to exact lab
values. The d1 and d2 sweeps have no self-test, and this one covers their output too.

## Safety

- Every kubectl call names `--context docker-desktop`.
- Every namespace the runner creates is `lw-plat20-*`, carries
  `logweir.dev/test-owner=plat20-1`, and is deleted only after that label and the
  UID recorded at creation are read back.
- The composed harnesses keep their own owner labels, in namespaces named with this
  run's stamp, and run their own `cleanup` in a `finally`.
- Nothing is written to the shared `logweir-scram-local` release. Three things that
  would write to it are never invoked:
  - plat06 `case-d`, which deletes the shared controller pod;
  - plat07's `lab-baseline`/`lab-swap`/`lab-restore`, which repoint the shared
    controller;
  - `scripts/test-k8s-scram.py`, which is the lab's installer.

  `test_catalogue.py::test_no_suite_runs_a_phase_that_touches_the_shared_release`
  pins this.
- The shared brokers carry exactly two topics this run creates, both named with its
  stamp: `plat20-<stamp>-orders` on `kafka-source` and its restored copy on
  `kafka-target`. Both are deleted at the end.
- The shared MinIO is written under `kafka-backups/lw-plat20-<stamp>/` only (native,
  console and plat06), plus d3's and plat10's own buckets, which they remove. The
  prefix and the evidence objects the native runs' statuses name are removed at the
  end.
- No phase changes cluster-scoped state, so the run needs no cluster lock. The one
  cluster-scoped delete a composed phase can make is d3 `cleanup` deleting its
  TrustPolicy, which it only does for a TrustPolicy its trust phases created, and
  none of those phases run here.

## The journeys

| id | tracker test | rows (suite: row) | verifies |
|---|---|---|---|
| `registration-and-discovery` | journey: registration and discovery | console: connection reached / destination Valid / discovery Succeeded naming the run's topic; d1 `L-09-1`; d2 `S1` | durable-resource, archive-data, evidence |
| `manual-backup` | journey: manual backup | plat06 `case-a`, plat07 `case-a`, d2 `S1` | evidence, durable-resource, archive-data |
| `scheduled-backup` | journey: scheduled backup | plat06 `case-c`; plat10 guided schedule creation and "verified runs" | evidence, archive-data, durable-resource |
| `scram-rotation` | SCRAM rotation | plat07 `case-e`, `case-f` | evidence, durable-resource |
| `new-topic-dynamic-policy` | new topic in dynamic policy | d1 `L-09-1`, `L-09-2` | durable-resource, evidence |
| `overlap` | overlap | plat06 `case-e` | durable-resource, evidence |
| `two-approvals` | two approvals | **requires: PLAT-19.2**, so it is skipped | — |
| `source-offline` | source offline | native: backup from an offline source; d2 `S11` | durable-resource, archive-data |
| `cr-loss` | CR loss | d3 `catalog-records-written`, `catalog-reconstruction-after-cr-loss`; plat06 `case-e` | archive-data, evidence, durable-resource |
| `stale-namespace-request` | stale namespace request | console: slow A never renders over B / left form writes nothing / submit after switch lands in B only | durable-resource |
| `duplicate-submit` | duplicate submit | plat12-13 double click / lost response / restore resubmission; plat06 `case-g`; native API replay | durable-resource, evidence |
| `old-point-selection` | old-point selection | plat12-13 older point stays selected; plat11-2 Restore is the preview byte for byte; native: the Restore names the older point, and exactly its records are restored | archive-data, durable-resource |
| `selected-point-restore-and-progress` | journey: selected-point restore and durable progress | native: exact records restored; scorecard verified by `logweir drill verify` and `docs/verify_scorecard.py`; progress sampled from the CR and projected by the API | archive-data, evidence, durable-resource |
| `scheduled-backup-restore` | journey: scheduled backup → restore | **requires: lab-refresh-8**: plat10's three rows blocked on EVIDENCE-FETCH-JOB-UNBUILT | — |

`$LOGWEIR_PYTHON e2e/journeys/run.py plan` prints every row with the file:line of its
assertion.

## What the gates wait for

- **`requires: lab-refresh-8`** needs a lab controller that carries the evidence-fetch
  Job (`claude/evidence-fetch`). Without it, a destination-backed run never gets
  `windowCovered`, and the console offers no Restore for a scheduled point.
  - Open the gate with `run --open lab-refresh-8` after the refresh.
  - The journey then requires these three `scripts/plat10-ui-e2e.mjs` rows to be
    recorded, not `blocked`, at :1278, :1357 and :1492.
  - plat10's row at :1509 ("Approval, admission and restored records") still records
    `blocked` after the refresh, because the harness mints no Approval. The native
    older-point journey here is the one that drives a restore to completion.
- **`requires: PLAT-19.2`** needs a governed approval policy. No build has one yet.
  - The approvals API says "Approval submission is PLAT-19.2 and has no route".
  - Opening this gate today FAILS the journey by name: its row is missing, because no
    harness records it.
