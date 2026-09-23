# CI, publishing and release checks

The normal development path has two checks: repository quality and a real
backup/restore integration suite. Image publishing follows only after both
pass. Release packaging adds checks for the artifacts users download.

## Pull requests and main

[ci.yml](../.github/workflows/ci.yml) runs on pull requests, pushes to `main`,
and manual dispatch. Other workflows can call the same checks.

A change that touches only `docs/to-do/` (the roadmap trackers and decision
records) starts no run: no build, test or image reads those files. A change
that touches only other files under `docs/` still runs `check`, because its
contract tests read `docs/api.md`, `docs/kubernetes.md`, `docs/install.md` and
the doc footer and link lints, but it skips `e2e` and therefore `publish`: no
binary embeds a document and no image copies one out of its build stage.
[scripts/ci-changes.sh](../scripts/ci-changes.sh) decides this from the diff
and runs everything when it cannot tell (a new branch, a tag, a manual or
release run). Root Markdown files such as `THIRD_PARTY_NOTICES.md` are not
under `docs/` and always run everything, since images copy them.

| Job | Purpose |
|---|---|
| `changes` | Classifies the diff as docs-only or not; `e2e` (and so `publish`) runs only when something outside `docs/` changed |
| `check` | Runs [scripts/ci-check.sh](../scripts/ci-check.sh): formatting, Clippy, workspace tests, UI behavior, independent Python verification, dependency boundaries, licenses/advisories, generated schemas/CRDs/chart/install drift, documentation links and notices |
| `e2e` | Runs the feature-gated integration suite against Docker Compose Kafka and MinIO, with the pinned engine and both evidence readers; retains Compose logs and tears down the stack |
| `publish` | On a successful push to `main` only, calls [images.yml](../.github/workflows/images.yml) to publish the tested image set |

The Compose suite excludes the test that requires a local `docker-desktop`
Kubernetes listener. That exclusion does not establish Kubernetes deployment
coverage; use the Helm or local Kubernetes exercises below for that.

Run the repository checks locally with:

```bash
just gate
```

This invokes the same script as the CI `check` job. It requires Rust, just,
Node 20+, Helm 4+, kubectl, cargo-deny, and Python with cryptography and pytest.
It needs Docker to extract and verify the pinned engine, but no running broker or Kubernetes cluster. Dependency downloads
and advisory updates may use the network.

For a shorter iteration use `just lint` or the relevant test target. The gate
runs the workspace suite once in debug mode, plus the Python and integration
checks that prove different behavior. It does not repeat the entire suite in
release mode or enforce a workstation timing threshold. `just time-unit-suite`
and `just deps-count` remain optional diagnostics.

## Image publication

The shared [image workflow](../.github/workflows/images.yml) builds the
controller and UI on native amd64 and arm64 runners. The bundled Kafka engine
supports amd64, so the runner image is amd64 only. The image checks exercise
the binaries, required attribution and UI asset bytes before publication.

[scripts/ci-images.sh](../scripts/ci-images.sh) pushes candidate images, checks
the registry digests, and promotes the complete successful set. Main builds
receive a `sha-<commit>` tag and update `main` and `latest`. Promotion is
serialized and protects the moving tags from older main runs.

Use the commit tag or digest to identify what was built. Moving tags are
convenient installation defaults, not immutable release identifiers. A green
unit test is not publication evidence: check the image job's outcome and
registry digests for the same commit.

## Versioned releases

[release.yml](../.github/workflows/release.yml) runs from `v*` tags and refuses
a manual branch release. It reuses CI, packages native CLI archives, and calls
[release-drill.yml](../.github/workflows/release-drill.yml) to exercise the
packaged Linux binary against a real archive. The release installs the
independent verifier alongside the other downloadable assets.

Versioned images use the shared image workflow, publishing the version tag
without moving `main` or `latest`. Release publication waits for the required
checks. [The release checklist](tag1-checklist.md) describes what to verify
for each candidate; it is not a frozen claim that historical runs passed.

## Deeper exercises

| Exercise | When to use it |
|---|---|
| `just e2e-up`, `just e2e`, `just e2e-down` | Local integration tests against Kafka, MinIO and the pinned engine |
| `just mvp-demo` | Backup, approved restore into new topics, and verification through the CLI; requires a clean Compose source |
| `just smoke`, `just smoke-weirkeeper`, `just smoke-ui` | Check locally built runner, controller and UI images |
| [helm-demo.yml](../.github/workflows/helm-demo.yml) | Manual Kubernetes/Helm deployment exercise on an isolated CI kind cluster, including scheduling, restore, verification and UI access |
| [engine-matrix.yml](../.github/workflows/engine-matrix.yml) | Weekly or manual compatibility checks across engine versions |
| `just laptop-demo`, `just k8s-demo` | Local Kubernetes exercises; select the intended context explicitly |
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
