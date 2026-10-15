# FX-53 — CI/CD rebuilt: test each input once, build each thing once

- Row: FX-53 (chore, Tier B), [product-expansion tracker](../product-expansion.md). Owner decision: OD-22 (2026-10-15).
- Date: 2026-10-15, against main `0c9e792d`.
- **Evidence:** three read-only analyses, each citing run, job and step ids:
  - `cicd-timing`: where the minutes go, over 7 days and 352 ci runs;
  - `cicd-structure`: every gate, its inputs, the crate graph and the hidden file edges;
  - `cicd-techniques`: the techniques, judged against these workflows.

  The numbers below come from them.

## 0. Summary

1. **The cost.** CI uses about 17,300 to 19,000 job-minutes a week.
   - A main push takes **47.6 min** at the median (p90 52.5); a PR 27.9; a release tag 28 to 32.
   - Every non-docs change pays the full pipeline: `check` (28 min), five e2e shards (12 to 29 min each), the engine built five times, and six image compiles.
   - e2e is 68 % of the compute and `check` 18 %.
2. **Most of it repeats work whose inputs did not change:**
   - **Identical trees tested twice.** Main re-tests the tree its PR just passed (30 of 47 merges, about 3,200 job-min a week). `release.yml` re-runs all of `ci.yml` on a tree that already passed (14 of 15 releases; the v0.4.0 tree was tested 4 times).
   - **Builds repeated.** The engine is built 5× a run (about 1,800 min/week), the CLI 6×, the console 3–4×. Each image compiles its own Rust after `COPY . .`, so no layer survives to the next commit.
   - **Changes that touch no tested input run everything:**
     - `weirkeeper`, `logweir-api`, retention and reaper changes run all five e2e shards, though no e2e binary links them;
     - chart, `deploy/poc` and console changes run every shard and rebuild every image;
     - a docs-only push still runs the whole `check`.
   - **The cache overflows.** It holds 10.6 GB against a 10 GB limit. 8.2 GB of that is BuildKit builder layers, and 13 of the 15 large ones were never read again. That evicts everything but the two hot Rust caches.
3. **The design: content-addressed gate results.**
   - Every gate (a CI job) declares its inputs: files, crate closures, toolchain, its own definition.
   - It hashes them and skips itself only when a **marker** proves that the same gate passed on the same inputs in this repository.
   - Otherwise it runs and leaves a marker.

   This one rule does three jobs at once:
   - **per-component selection:** a docs change re-runs only the gates that read docs;
   - **no double testing:** main and a release reuse a green PR or publication run;
   - **soundness on failure:** a gate that failed left no marker, so it runs again.

   Repeated builds become one build, shared by artifact or by a cache keyed on the build's inputs.
4. **Nothing is dropped.**
   - Every gate in today's inventory (structure report §1, C1–C30, E1–E5, I1–I11, R1–R6, K1, M1–M5) runs whenever its inputs change. A parity table proves it.
   - An unclassified path runs everything. A change to CI itself runs everything.
   - A nightly full run (markers ignored) catches time-dependent and external drift: advisories, `npm audit`, mutable base images.
   - A weekly traced run proves the declared inputs complete: it fails when a test opens a repository file its gate does not declare.
5. **Gaps today, closed by this redesign:**
   - `docs/verify_scorecard.py` is code the e2e harness runs, but it is classified as docs and skips e2e.
   - `docs/to-do/**` is never linted, though `doc_lint` walks it.
   - `ci.yml:182-185` names a withdrawn test, so the step runs 0 tests and passes.
   - 20 `--features e2e` crate rows never run.
6. **Projected wall-clock** (minutes, warm caches; the reasoning is in the techniques report §11):

| Change | PR now | PR after | main now | main after |
|---|---|---|---|---|
| docs only | ~27 | ~6 | 26.7 | ~1–3 (markers) |
| console only | 28.9 | ~8 | ~57 | ~6 |
| chart or `deploy/poc` only | ~28 | ~6 | ~57 | ~5 |
| one leaf crate (retention) | ~28 | ~8 | ~57 | ~9 |
| a core crate (`logweir-core`) | ~28 | ~13 | 48–57 | ~14 |
| everything (Cargo.lock, toolchain, CI) | ~29 | ~15 | ~57 | ~20 |
| release tag (docs-only diff) | — | — | 32.3 | ~10 |

## 1. Principles

1. **A gate runs when any input it reads changed, or when no marker proves its inputs passed.** Skipping is a proof, never a guess.
2. **Fail safe.** An unclassified path, a missing map entry, an API error on the marker lookup, or a change to the gate machinery itself means: run.
3. **Same repository only.** A marker counts only when its run's `head_repository` is `VladyslavHaina/logweir`, its workflow is `ci.yml`, and its gate definition hash matches. A fork's `pull_request` run executes fork-controlled code, so it never proves anything.
4. **Publication is unchanged in meaning.** `sha-<commit>` images and the chart are published only after every gate for that tree passed: in this run, or proven by markers for the same inputs. A release still promotes one publication unchanged.
5. **Free runners only.** The repository is public. No paid service is used. Larger runners are a marked option, not a dependency.
6. **Measured.** Each stage lands as its own PR. Its effect is read from the runs API before the next stage starts.

## 2. Mechanism

### 2.1 The gate map (`ci/gates.toml`)
One checked-in file names every gate (job), with:
- its **inputs**:
  - path globs;
  - crate names, whose closure is expanded from `cargo metadata --locked` (the crate, its workspace dependencies' directories, `Cargo.lock`, the root `Cargo.toml`, `rust-toolchain.toml`);
  - **hidden readers**: the files its tests read at run time (structure report §2.3), such as `docs/**`, `console/fixtures/**`, `charts/**`, other crates' sources, `e2e/fixtures/**` and `examples/**`;
- its **definition**: the job's text in the workflow, plus the scripts and `justfile` recipes it calls;
- **`always = true`** for gates whose result depends on time or the outside world: `cargo deny` advisories, `npm audit`, the license allowlist. They are cheap and never skipped.

`scripts/ci-gate.sh hash <gate>` prints the gate's input hash: sha256 over the sorted `git ls-files` blob ids of its inputs, the definition, and the runner label. It is deterministic and needs no build.

### 2.2 Markers
- **Recording.** A gate that passes uploads an artifact named `gate-<gate>-<hash>`: a few bytes, with 30-day retention.
- **Lookup.** Before working, a gate asks `GET /repos/{repo}/actions/artifacts?name=gate-<gate>-<hash>`. It accepts a non-expired artifact whose `workflow_run.head_repository_id` equals the repository's id, from a run of `ci.yml`.
  - **Hit:** the job prints the proving run's URL and succeeds without working.
  - **Miss or error:** it works.
- **Where it lives.** One composite action, `.github/actions/gate`, does both. Every gate job wraps its work in it.

### 2.3 Builds shared within a run, and across runs by input key
- **The engine.**
  - One `engine` job per run. It restores `.engine/kafka-backup` from `actions/cache`, keyed by `engine-source.sh digest` + toolchain + runner OS/arch, or builds it.
  - It uploads the engine as a run artifact for the shards. The image's `engine-logweir` stage reads the same key.
- **The CLI and the e2e test binaries.**
  - One `build-e2e` job: `cargo build -p logweir` and `cargo nextest archive -p e2e --features e2e`.
  - The shards download the archive and need no toolchain, rust-cache or apt.
- **Images.**
  - The Dockerfiles copy only each image's inputs before compiling: `Cargo.toml`, `Cargo.lock`, `rust-toolchain.toml`, `crates/`, `xtask/`, `e2e/Cargo.toml`, `e2e/src`. So a builder layer's key is its Rust inputs, and a docs or chart change hits the cache.
  - The cache is exported per image and architecture into one overwritten scope: `type=gha,scope=<image>-<arch>,mode=max` only where the layer is reused; otherwise `mode=min`.
  - `promote` uses `driver: docker` and never pulls `moby/buildkit`.
- **Rust caches.**
  - Only `main` saves (`save-if`), with one `shared-key` per job kind. The shards stop racing for one key.
  - PR caches stop filling the 10 GB limit.

### 2.4 Parallel jobs
- **`check` becomes parallel jobs**, each a gate:
  - **lint:** fmt, clippy, the shell and Python lints;
  - **unit:** the workspace tests, run with nextest by crate group, each group keyed by its closure plus its hidden readers. The groups:
    - core libraries: core, evidence, verify, store, kafka, ffi, engine-oso;
    - `logweir`;
    - `weirkeeper`;
    - `logweir-api`;
    - retention with reaper;
    - xtask;
  - **generated:** schema, CRD and chart drift;
  - **supply-chain:** both `cargo deny` runs, the notices diff, the engine oracle;
  - **python:** the Python tests and `verify-py`;
  - **docs:** links, `doc_lint`, the unverified-label scan.
- **e2e:** about 8 shards, balanced by measured CI time (longest first), with `stack_params` as the partition guard.
  - Each shard is one gate, keyed by its binaries' closure, the harness, compose, fixtures, the engine and `docs/verify_scorecard.py`.
  - The 20 `--features e2e` crate rows run in a shard. Any that cannot run in CI is named with its reason.
- **`ci-ok`** aggregates every gate (`re-actors/alls-green`). `publish` needs `ci-ok`.
- **Flags:** `mold` and `CARGO_PROFILE_DEV_DEBUG=line-tables-only` for test builds; `CARGO_INCREMENTAL=0`.

### 2.5 Release
- `release.yml`'s `tests` still calls `ci.yml`. Markers make it re-run only the gates whose inputs differ from the publication's: on a docs-only diff, the docs readers.
- The CLI builds, the release drill, `assemble`, `publish-images` and `github-release` run as today.
- `docs/gates.md` and `docs/tag1-checklist.md` say: the tag's `tests` job is `ci.yml`, whose gates are proven in that run or by markers for the same inputs.

### 2.6 Backstops
- **`ci-nightly.yml`** (schedule, main): `ci.yml` with `force: true`, which ignores markers and runs every gate. A red night is an issue the orchestrator acts on before the next merge.
- **`ci-trace.yml`** (weekly): runs each unit and e2e gate under a file-open tracer (`strace -f -e trace=openat,open`). It fails when a gate opened a tracked repository file outside its declared inputs. This keeps `ci/gates.toml` complete as tests change.
- **`workflow_lint`** gains rows:
  - every job in `ci.yml` is a gate in `ci/gates.toml`, and every gate is a job;
  - every gate job uses `.github/actions/gate`;
  - `publish` needs `ci-ok`.

## 3. Stages (each a PR, measured before the next)

| Stage | What | Owner | Measured by |
|---|---|---|---|
| **1. Cheap and sound** | Fix the vacuous named rows (`scripts/run-named-tests.sh`). Close gaps 1 and 2. Conditional free-disk. rust-cache `save-if: main` and shared keys. The `engine` job, cached by input key. Narrowed Dockerfile `COPY` and the cache-export policy. `promote` on `driver: docker` | worker `cicd-w1` | shard setup minutes; `promote`; cache usage under 10 GB; engine cache hits |
| **2. Parallel** | `check` split into gates; nextest; `build-e2e` and about 8 shards; `ci-ok`; mold and debuginfo flags; the 20 crate rows | `cicd-w1` | main and PR wall-clock against 47.6 and 27.9; each job's critical-path share |
| **3. Markers** (built beside stages 1–2) | `ci/gates.toml`; `scripts/ci-gate.sh`, with unit rows for the hash and the fail-safe cases; `.github/actions/gate`; `ci-nightly.yml`, `ci-trace.yml`; the `workflow_lint` rows | worker `cicd-w3` | the classifier's rows; one traced run green |
| **4. Wired** | Every gate job of stage 2 inside `.github/actions/gate`. The release docs | `cicd-w1` with `cicd-w3`'s parts | a docs-only PR, a console-only PR, a chart-only PR, a leaf-crate PR, a core-crate PR, the PR→main pair, and the `v0.4.1` dry run and tag, against the table in §0 |
| **5. Later** (only if the numbers ask) | Consolidated test binaries; a light contract-test crate; cargo-chef or a shared builder for the three images; a CLI build cache | — | — |

**Rejected, with reasons** (techniques report §10):
- **sccache:** rust-cache already restores the dependencies, it cannot cache the 294 test binaries (bin crates link), and it adds many small entries to the 10 GB cache. Markers skip the compile outright.
- **cargo-hakari:** it conflicts with the dependency-boundary gates.
- **`dorny/paths-filter`:** it would fork the classifier from `release.sh`.
- **A Docker Hub registry cache:** it needs credentials before the checks.
- **Re-tagging old image digests for unchanged images:** it breaks the revision label and the provenance. Cache hits make the rebuild cheap instead.
- **A merge queue:** there is one maintainer.
- **cancel-in-progress on main:** publication order.

## 4. Invariants each PR must show

1. **Gate parity.** A table: every gate id of today's inventory, then where it runs now, with its inputs. Nothing missing.
2. **The fail-safe rows pass:** an unknown path, a CI change, a lookup error, a fork marker and a stale definition all run the gate.
3. **The contract tests are kept or consciously changed**, each with a reason (structure report §5): `workflow_lint`, `engine_build`, `stack_params`, `docs/gates.md`, `docs/tag1-checklist.md`.
4. **One measured run per change type the stage affects**, with its run id and minutes.

## 5. Not changed

- What ships: the images, the chart, the CLI archives and the signing.
- What a release promotes.
- The weekly `engine-matrix.yml` and `compat-weekly.yml`, except that they share the engine cache key.
- The `docs/to-do/**` exemption from **starting** a run: the tracker still starts nothing. It is linted by the nightly and by every run whose docs gate runs anyway.

Documentation is licensed [CC-BY-4.0](../../LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
