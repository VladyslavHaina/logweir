# CI, publishing and release checks

The normal development path has two checks: repository quality and a real
backup/restore integration suite. Image publishing follows only after both
pass. Release packaging adds checks for the artifacts users download.

## Pull requests and main

[ci.yml](../.github/workflows/ci.yml) runs on pull requests, pushes to `main`,
and manual dispatch. Other workflows can call the same checks.

A change that touches only `docs/to-do/` (the roadmap trackers and decision
records) starts no run: no build, test or image reads those files. The doc
footer lint, the link check and the label and cosign scans do walk them, so a
tracker-only commit is linted by the next run that starts; FX-53's docs gate
and nightly full run close that gap. A change that touches only other files
under `docs/` still runs `check`, because its contract tests read
`docs/api.md`, `docs/kubernetes.md`, `docs/install.md` and the doc footer and
link lints, but it skips `e2e` and therefore `publish`: no binary embeds a
document and no image copies one out of its build stage. The verifier
`docs/verify_scorecard.py` and its suite `docs/test_verify_scorecard.py` are
the exception: they are code (every e2e harness user runs the verifier, and a
release ships it), so a change to either runs everything.
[scripts/ci-changes.sh](../scripts/ci-changes.sh) decides this from the diff
and runs everything when it cannot tell (a new branch, a tag, a manual or
release run). Root Markdown files such as `THIRD_PARTY_NOTICES.md` are not
under `docs/` and always run everything, since images copy them.

| Job | Purpose |
|---|---|
| `changes` | Classifies the diff as docs-only or not; `engine`, `e2e` (and so `publish`) run only when something outside `docs/`, or the verifier, changed. It also says whether the rebuilt console's inputs changed (`console/`, `console/fixtures/console/` or the product API's OpenAPI document) |
| `console` | Only when the console's inputs changed: `just console`, the rebuilt console's gates (lockfile install, API types, TypeScript, ESLint, Vitest, the build and its shape, `npm audit` and the licence allowlist) |
| `check` (twelve gates in eight jobs) | Runs one part of [scripts/ci-check.sh](../scripts/ci-check.sh) per gate, at once; the gates that take a few minutes share a job, each as its own step (`lint` with `supply-chain`, `python` with `docs`, and the `retention`, `xtask` and `e2e` unit groups), so a run stays within GitHub Free's 20 concurrent jobs. The gates: `lint` (formatting, Clippy, the shell and Python lints, dependency boundaries, the cosign-pin scan), seven `unit-<group>` gates (the workspace tests by crate group with cargo-nextest, then the doctests: `core`, `logweir`, `weirkeeper`, `api`, `retention` with the reaper, `xtask`, and the e2e package's rows that need no stack), `generated` (schema, CRD and chart drift), `supply-chain` (the notices diff, licences and advisories over both graphs, the engine patch oracle), `python` (the script rows and the independent Python verifier), `docs` (links, the unverified-label scan, `doc_lint`) |
| `engine` | Builds Logweir's engine once per run with `scripts/engine-source.sh build`, or restores the binary that command built from the same inputs (a cache keyed by `engine-source.sh digest`, the recipe, the toolchain, the build packages and the runner image; saved by `main` only); checks that it prints the build env's `ENGINE_VERSION` and hands it to the shards as an artifact |
| `build-e2e` | Builds the debug `logweir` the suite drives and archives the e2e test binaries with cargo-nextest, once per run |
| `e2e` (eight shards) | Runs the feature-gated integration suite against Docker Compose Kafka and MinIO, one test at a time from `build-e2e`'s archive, balanced by measured time; with the `engine` job's build over OSO's extracted binary (OSO's pinned image seeds the archive; each shard checks the engine's `--version` again) and both evidence readers. One shard then runs the `--features e2e` rows under `crates/`. Retains Compose logs and tears down the stack |
| `ci-ok` | Every gate's verdict in one job: green only when each passed, or was skipped because `changes` said its inputs did not change |
| `publish` | On a successful push to `main` only, calls [images.yml](../.github/workflows/images.yml) to publish the tested image set; it needs `ci-ok` and an `e2e` that ran |

The Compose suite excludes the test that requires a local `docker-desktop`
Kubernetes listener. That exclusion does not establish Kubernetes deployment
coverage; use the Helm or local Kubernetes exercises below for that.

Run the repository checks locally with:

```bash
just gate
```

This invokes the same script as the CI `check` gates, all of its parts in one
run, with the workspace suite as one libtest run (no cargo-nextest needed);
`scripts/ci-check.sh <part>` runs one gate's part the way CI does (the parts
need cargo-nextest). It requires Rust, just, Node 20+, Helm 4+, kubectl,
cargo-deny, and Python with cryptography and pytest.
It needs Docker to extract and verify the pinned engine, but no running broker or Kubernetes cluster. It also runs `cargo deny` over the engine's own lockfile, prepared with the patch folder (`third_party/kafka-backup-deny.toml`), and `scripts/check-cosign-verify.py`. Dependency downloads
and advisory updates may use the network.

For a shorter iteration use `just lint` or the relevant test target. The gate
runs the workspace suite once in debug mode, plus the Python and integration
checks that prove different behavior. It does not repeat the entire suite in
release mode or enforce a workstation timing threshold. `just time-unit-suite`
and `just deps-count` remain optional diagnostics.

`just test-shifted-clock` is a third: it runs the workspace suite with every
test binary's wall clock a year and a day ahead (`+<days>d` sets another
offset). A test that compares the wall clock with a fixed instant then fails
before that instant passes, instead of on the day it does. It builds a small
clock shim with `cc`, on macOS and on Linux (glibc; it needs `gcc` and the libc
headers). No CI job runs it. The Rust toolchain keeps the real clock, because
cargo judges its shared cache in `~/.cargo` by it: the shim stands down in
cargo and rustc, and the run refuses unless a cargo started under the shim
sees the real clock. It also refuses, rather than running at the real clock
or hanging, when no shim is available, the shift does not reach a test binary,
a timed wait does not return under the shift, or `--target` is passed.

## Image publication

The shared [image workflow](../.github/workflows/images.yml) builds all four
images — runner, controller, console and UI — on native amd64 and arm64
runners. The runner's engine is Logweir's build of the vendored source
(PROD-00.2), so it exists for both. The image checks exercise the binaries,
the engine's declared identity (`scripts/check-image.sh` check 8), required
attribution and UI asset bytes before publication. After promotion the `sign`
job signs every published index keylessly, attests the runner's SBOM and
records SLSA provenance, then verifies them with the pins `docs/install.md`
documents (`scripts/check-cosign-verify.py` holds every verification command
to them).

[scripts/ci-images.sh](../scripts/ci-images.sh) pushes candidate images, checks
the registry digests, and promotes the complete successful set. Main builds
receive a `sha-<commit>` tag and update `main` and `latest`. Promotion is
serialized and protects the moving tags from older main runs.

Use the commit tag or digest to identify what was built. Moving tags are
convenient installation defaults, not immutable release identifiers. A green
unit test is not publication evidence: check the image job's outcome and
registry digests for the same commit.

## Versioned releases

[release.yml](../.github/workflows/release.yml) publishes on a pushed
`v<semver>` tag (`v0.2.0`, `v0.2.0-rc.1`) and on nothing else. Dispatching the
same workflow by hand is a **dry run**: every job up to `assemble` runs, and it
writes workflow artifacts only — no registry tag, chart, git tag, GitHub
Release or draft. The two publishing jobs carry an `if:` that admits a tag push
and nothing else, and `crates/logweir/tests/workflow_lint.rs` holds every job
that holds a credential or `contents: write` to that gate.

| Job | What it does | Writes |
|---|---|---|
| `validate` | the tag (a dry run's `rehearsal_tag` input), its version and pre-release flag, and whether this run may publish: a tag push that did not delete the tag, whose tag origin still points at the run's commit | — |
| `tests` | `ci.yml`: the same checks and Compose suite as `main` | — |
| `plan` | `dist plan` must announce exactly the three CLI archives | artifact `dist-manifest` |
| `build` (three) | [scripts/release-build.sh](../scripts/release-build.sh) on one native runner per target — Linux inside `rust:1.89-bookworm`, the runner image's own builder base, macOS on the runner; checks the archive's contents and notices, that no engine is inside, that the binary starts and performs `drill countersign`, and measures what it needs at run time | artifact `binary-<target>` |
| `drill` | [release-drill.yml](../.github/workflows/release-drill.yml): a backup, an approved drill and both verifiers, driven by the packaged Linux x86-64 binary against Compose Kafka and MinIO | artifact `release-drill-evidence` |
| `images` | finds the `sha-<commit>` publication the release ships, anonymously: the tagged commit's own, or its newest ancestor that differs from it only under `docs/`; checks each image's platforms and revision label | artifact `release-images` |
| `assemble` | checks every asset again, packages the chart once with its four images pinned by digest, and writes `release.json`, `SHA256SUMS` and the release notes | artifact `release-assets` |
| `publish-images` | tag push only, after `tests`: the version tag on the publication's own digests (an existing tag is never moved), then the chart package pushed as those bytes (a published version is never replaced); "absent" is only the registry's own "not found", and a read that cannot tell refuses before any write | Docker Hub |
| `github-release` | tag push only: the GitHub Release from the verified assets, downloaded back and verified | the GitHub Release |

A version's images are therefore never rebuilt: the version tag and the
`sha-<commit>` tag name the same digests, and `main` and `latest` do not move.
A tag on a commit `main` CI has not published fails at `images`. The
procedure, the asset list and how each asset is verified are in
[the release checklist](tag1-checklist.md); it is not a frozen claim that
historical runs passed.

## Deeper exercises

| Exercise | When to use it |
|---|---|
| `just e2e-up`, `just e2e`, `just e2e-down` | Local integration tests against Kafka, MinIO and the pinned engine |
| `just mvp-demo` | Backup, approved restore into new topics, and verification through the CLI; requires a clean Compose source |
| `just smoke`, `just smoke-weirkeeper`, `just smoke-console` | Check locally built runner, controller and console images |
| [helm-demo.yml](../.github/workflows/helm-demo.yml) | Manual Kubernetes/Helm deployment exercise on an isolated CI kind cluster, including scheduling, restore, verification and UI access |
| [engine-matrix.yml](../.github/workflows/engine-matrix.yml) | Weekly or manual compatibility checks across engine versions, and the record-semantics and topic-identity suites on each supported Apache Kafka line with the shipped engine |
| [compat-weekly.yml](../.github/workflows/compat-weekly.yml) | Weekly or manual: the compatibility contract's rows that need a compose profile CI's e2e job does not start (Confluent Platform, Redpanda, SeaweedFS, RustFS, versitygw, a directory archive, Karapace), and archives written by the engines published versions pinned, drilled with the shipped engine |
| [test-k8s-scram.py](../scripts/test-k8s-scram.py) | Authenticated backup/restore regression exercise restricted to `docker-desktop`; it also installs the shared lab, whose seed topics never expire (`retention.ms=-1`, checked offline by `python3 scripts/test_k8s_scram_seed.py`) |

The standalone no-OSO workflow and duplicate kind demo workflow have been
retired. Dependency isolation remains in the shared checks; Kubernetes coverage
is available through the manual Helm exercise and local tests.

Workflow configuration describes intended checks, not execution evidence.
Inspect [GitHub Actions](https://github.com/VladyslavHaina/logweir/actions)
for the exact commit being shipped; new workflow changes must complete a run
before being described as validated.

---

Documentation is licensed [CC-BY-4.0](LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
