# Logweir

Logweir backs up Apache Kafka topics, restores a sampled point-in-time window
into new topics, reconciles records against the archive, and signs the result.
The CLI runs independently; the `weirkeeper` Kubernetes controller schedules
jobs and verifies their evidence, and a static UI operates the Kubernetes API.
The UI can run locally or through the Helm chart’s `logweir-ui` image.

Code is [Apache-2.0](LICENSE); documentation is
[CC-BY-4.0](docs/LICENSE-docs). See [NOTICE](NOTICE) and
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md) for dependency attribution.
Contributions require DCO sign-off (`git commit -s`), with no CLA.

## Quickstart

Run the local backup-and-restore demo with Docker Compose:

```bash
just e2e-up
just mvp-demo
just e2e-down
```

This produces records, takes a backup, verifies its signed receipt with two
independent readers, approves a restore into new topics, and verifies the
resulting scorecard. It requires Docker, Rust, `just`, OpenSSL, `awk`, and
Python with `cryptography`. Output goes to the ignored `.demo/mvp/` directory.
The generated demo keys prove integrity but have no independent provenance.

[Quickstart](docs/quickstart.md) starts with the one supported setup-and-recovery
path on Kubernetes, then covers the scratch-cluster drill demo and running
against your own archive.
[Verification](docs/verify-a-scorecard.md) explains what the signed result proves.

## Install

Kubernetes 1.29 or newer is required. The supported path is the published Helm
chart with its managed installation identity: [quickstart](docs/quickstart.md),
*The supported path*, then [the installation guide](docs/install.md) and
[the chart reference](charts/logweir/README.md). The low-level manifests and
locally built images remain in the installation guide.

**What is published.** A push to `main` that changes something outside
`docs/` and passes CI publishes the controller (`weirkeeper`), runner
(`logweir`), console (`logweir-console`) and UI (`logweir-ui`) images under
`sha-<commit>` (and moves `main` and `latest`), and the chart
`oci://registry-1.docker.io/vladyslavhaina/logweir-chart --version
0.1.0-sha-<commit>`, whose image defaults are those four tags; a push that
changes only `docs/` publishes nothing
([gates.md](docs/gates.md#pull-requests-and-main)). A version tag
(`v<X.Y.Z>`, or a pre-release such as `v0.2.0-rc.1`) runs
[the release pipeline](docs/gates.md#versioned-releases), which does not
rebuild the images: it gives the tagged commit's `sha-<commit>` images the
version tag (the same digests), publishes the chart `--version <X.Y.Z>` with
all four images pinned by digest, and creates a GitHub Release carrying the CLI
archives, the independent verifier (`verify_scorecard.py`), `LICENSE`, `NOTICE`,
`THIRD_PARTY_NOTICES.md`, the engine's licence, the chart package,
`release.json` and `SHA256SUMS`. No tag has done so yet: the runs for
`v0.1.1`–`v0.1.5` failed before publishing a release, and the pipeline was
repaired after them. The image tags those runs pushed before failing —
`logweir:v0.1.1`–`v0.1.5`, `weirkeeper:v0.1.2`–`v0.1.5` and
`logweir-ui:v0.1.3`–`v0.1.5` on Docker Hub — are leftovers of failed runs, not
releases: no chart, GitHub Release or release drill goes with them, so do not
install or pin them. [UNVERIFIED — no version tag has run the repaired pipeline yet; the first is the owner-approved release candidate.]

```bash
helm upgrade --install logweir oci://registry-1.docker.io/vladyslavhaina/logweir-chart \
  --version <X.Y.Z> -n logweir-system --create-namespace --wait --timeout 10m
```

From a checkout the chart's four image defaults are `:latest`; install a
published chart, or pin the images as the installation guide describes.

**The CLI** comes from a release's archives — check the download against the
release's `SHA256SUMS` — or from the checkout:

```bash
cargo install --path crates/logweir --locked
```

| Archive | Runs on |
|---|---|
| `logweir-x86_64-unknown-linux-gnu.tar.xz` | Linux x86-64 with Debian 12's glibc or newer, `libssl.so.3`, `libcrypto.so.3`, `libsasl2.so.2` and `libz.so.1` (Debian and Ubuntu: `libssl3`, `libsasl2-2`, `zlib1g`) |
| `logweir-aarch64-unknown-linux-gnu.tar.xz` | the same, on Linux arm64 |
| `logweir-aarch64-apple-darwin.tar.xz` | macOS 11 or newer on Apple silicon, with Homebrew's `openssl@3` |

The Linux archives are built in the runner image's own builder base, so they
need what the runner image installs; a distribution whose SASL library has
another soname (`libsasl2.so.3` on RHEL and Fedora) builds from the checkout.
Each release's notes give the requirement measured on its own binaries. An
approver countersigns a Governed restore with `logweir drill countersign` from
the archive for their own machine; their key never leaves it.

The standalone binary does not bundle the engine. Configure Logweir's build
of the `kafka-backup` engine through `PATH` or `LOGWEIR_ENGINE_BIN`, together
with `LOGWEIR_ENGINE_VERSION` and `LOGWEIR_ENGINE_DIGEST`, as described in the
[quickstart](docs/quickstart.md). The runner image bundles both binaries, for
`linux/amd64` and `linux/arm64`; build instructions are in
[the installation guide](docs/install.md).

## How it works

1. Admission guards check the requested archive, target and approved plan.
2. Logweir runs the digest-pinned `kafka-backup` engine as a subprocess.
3. A drill restores into a segregated scratch cluster; a restore writes into
   topics that did not exist. Restore-in-place into a live topic is unsupported.
4. Logweir reconciles the sampled records, measures RTO and archive-relative
   RPO, and signs a JSON scorecard. A backup produces its own signed receipt.
5. An auditor verifies the original bytes and signature using the Rust CLI or
   the independent [Python verifier](docs/verify_scorecard.py).

`logweir drill show` is a summary; `--format json` prints the signed document.
A successful signature does not establish who controlled the signing key or
prove the unsampled portion of the archive. Read the
[verification guide](docs/verify-a-scorecard.md) before relying on a result.

Logweir does not link upstream's core or consume OSO operator CRDs. It builds
the MIT-licensed engine from the vendored source with its own patch folder and
redistributes it, with the source and attribution in
[third_party](third_party/README.md). The engine the images ship is Logweir's
build of `kafka-backup` 0.23.3 (`0.23.3+logweir.2`) and the full-drill engine
floor is 0.21.0;
[the support matrix](docs/support-matrix.md) distinguishes exercised versions
from unsupported or untested ones. The engine's own `validation run` is not
invoked by the current adapter, so `engine_subreport` remains null.

See [architecture and decisions](docs/architecture.md) for the crate map,
trust boundaries and retained ADR rationale, and
[stability](docs/stability.md) for compatibility, exit codes and limitations.

## Logging and metrics

`logweir drill run` writes JSON lines to stdout and human-readable failures to
stderr. The default `info` output carries the run ID, with dependencies scoped
to `warn`. A nonblank `RUST_LOG` overrides that filter and may include dependency
messages without a run ID. Read the command's exit code directly before
processing its output.

Metrics are written as a Prometheus textfile using `--metrics-file`; there is
no metrics HTTP endpoint. See [metrics](docs/metrics.md) and
[Kubernetes operations](docs/kubernetes.md) for collection and Job exit handling.

## Threat model: what this does **not** protect against

Four residuals, accepted and stated here rather than left to be discovered.
They are not undiscovered bugs, and a report that one of them is true is not a
vulnerability report.

- **`weirkeeper` is a signing oracle wherever it holds Job CRUD over the
  namespace that holds the signing key.** The controller has no `get` on
  Secrets anywhere and never reads `logweir-signing-key` — but it creates Jobs,
  and a Job it creates can mount that Secret and sign whatever it likes with no
  Logweir crate involved. **Job CRUD over the signing-key namespace is
  equivalent to holding the key.** The hardened layout is to put the Secret in
  a namespace where `weirkeeper` has no Job CRUD, which removes the oracle;
  [docs/install.md](docs/install.md) gives that layout.
- **A cluster-admin defeats every control described here.** They can mount the
  signing Secret and sign, edit the `TrustRoster`, or delete an admission
  policy. Nothing here constrains that subject and nothing claims to.
- **RBAC bounds the viewer, not the page.** The UI is static files served by
  `kubectl proxy` under the viewer's own kubeconfig, so the shipped
  `logweir-viewer` / `logweir-operator` / `logweir-approver` ClusterRoles bind
  the **user** and bind nothing at all about the page. Running the UI from a
  cluster-admin kubeconfig gives the shipped bundle cluster-admin. The optional
  Helm UI proxy instead uses its ServiceAccount; its RBAC applies to everyone
  who can reach that proxy.
- **`self_attested: false` means only "two different keys".** One person
  holding both keypairs satisfies it. It is not evidence of an independent
  auditor, and a scorecard carrying it must not be read as one.

[SECURITY.md](SECURITY.md) carries the same four beside what **is** in scope.

**[Release notes](docs/release-notes.md) list the `ui/` bundle by digest.** The page runs with the
viewer's authority, so what is in the bundle matters: there is no telemetry in
it, nothing in it is fetched from anywhere else, and its contents are listed by
digest in the release notes so that the bytes a browser executed can be
compared against the bytes that were released.

## Documentation

This table is the documentation entrypoint. Each guide owns its subject;
package and fixture READMEs stay beside the files they describe.

| Task | Guide |
|---|---|
| Set up, back up, restore and recover — the one supported path | [Quickstart, *The supported path*](docs/quickstart.md) |
| Install with manifests or local images | [Installation](docs/install.md) |
| Install or configure Helm | [Chart guide](charts/logweir/README.md) |
| Run the demos, a drill, or the standalone CLI | [Quickstart, *Demos and the standalone CLI*](docs/quickstart.md) |
| Operate controllers, jobs, approvals and retention | [Kubernetes](docs/kubernetes.md) |
| Use or develop the static UI | [UI guide](ui/README.md) |
| Use the bounded product API (`logweir-api`) | [Product API](docs/api.md) |
| Verify signed evidence | [Auditor guide](docs/verify-a-scorecard.md) |
| See what the evidence shows for backup and restore-testing controls | [Control-evidence mapping](docs/control-evidence.md) |
| Generate, pin and rotate keys | [Signing keys](docs/keys.md) |
| Collect metrics | [Metrics](docs/metrics.md) |
| Interpret document fields | [Scorecard](docs/formats/drill-scorecard.md), [backup receipt](docs/formats/backup-receipt.md), [drill spec](docs/formats/drill-spec.md), [protection event](docs/formats/protection-event.md) |
| Understand architecture and decisions | [Architecture](docs/architecture.md) |
| Check compatibility and limitations | [Stability](docs/stability.md), [engine support](docs/support-matrix.md) |
| Build, test and contribute | [Contributing](CONTRIBUTING.md), [gate reference](docs/gates.md) |
| Read what changed and what an upgrade requires | [Release notes](docs/release-notes.md) |
| Assess release readiness | [Release checklist](docs/tag1-checklist.md), [release handoff](docs/release-handoff.md) |
| Report security issues | [Security policy](SECURITY.md) |
| Find project governance and attribution | [Maintainers](MAINTAINERS.md), [trademarks](TRADEMARKS.md), [third-party notices](THIRD_PARTY_NOTICES.md) |

---

Documentation is licensed [CC-BY-4.0](docs/LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
