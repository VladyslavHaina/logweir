# Logweir

**Logweir backs up Apache Kafka topics, restores a sampled point-in-time window
into a new topic, reconciles it per record, and signs the result — so that
"our backups work" is a document an auditor can verify rather than a claim.**

Not a policy document. Not a dry run. A restore that happened, timed, checked
byte-for-byte against the archive, and signed so an auditor can verify it
without trusting the machine that produced it.

**Licence: [Apache-2.0](LICENSE)**, with what the project owes its dependencies
in [NOTICE](NOTICE) and [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).
Documentation is CC-BY-4.0 ([docs/LICENSE-docs](docs/LICENSE-docs)).

**Contributions: DCO sign-off (`git commit -s`) on every commit; no
copyright-assignment CLA is required or accepted.** See
[CONTRIBUTING.md](CONTRIBUTING.md).

**Minimum Kubernetes: 1.29.** (CEL validation rules are GA there and the six
CRDs use them.) The CLI needs no cluster at all.

## Install

```bash
kubectl --context docker-desktop apply --server-side -f logweir.yaml
```

...but read **[docs/install.md](docs/install.md)** first, because that one line
is only half of it: there are **two supported paths** — the published image
digests, and an **author-only** local build — and until the release workflow
has run on a pushed tag the digest rows read **`blocked: images not published`**
and the first path is documented rather than exercised. (No tag has been pushed:
`release.yml` has never run. What **has** run is `ci.yml`, `no-oso.yml`,
`kind-demo.yml` and `helm-demo.yml`, green on 2026-09-12.) `docs/install.md` also carries
the five Secrets you must create **before** the first custom resource, the two
keypairs, the cluster-scoped `TrustRoster`, the per-namespace runner
ServiceAccount, and the uninstall with what it leaves behind.

`docs/install.md` is the **single** install document. Nothing else in this tree
carries install steps.

**Or with Helm** — the same control plane as one chart, with an optional
backend, two demo Kafka clusters and the UI behind three flags
([charts/logweir/README.md](charts/logweir/README.md); `docs/install.md` path (c)):

```bash
helm install logweir charts/logweir -n logweir-system --create-namespace \
  -f charts/logweir/examples/demo.values.yaml --wait --timeout 10m
cargo build -p logweir && bash scripts/helm-demo.sh   # the walk, end to end, then a clean teardown
```

## What it looks like

This is `logweir drill show` over the scorecard `scripts/demo.sh` produced on a
laptop — real output, pasted unedited:

```
logweir drill scorecard  (01M1RZW1F5KQE6HANC41M91CS9 v1.0.0)
------------------------------------------------------------------------
  outcome                     pass
  engine                      oso-cli 0.21.0 sha256:8ff5be71f92a118cde64c082a86d188a4187d8f8f64311458081b8727e99c317
  levers                      header_preflight=honoured  dry_run_check_segments=unknown-not-observable
  target                      5L6g3nShT-eMCtK--X86sw  mode=scratch  marker=logweir.scratch  2 mapping entry/ies
  approval                    demo@example.com (DEMO-1)

  rto requested→verified      7s
  rto approval→verified       7s
  rto restore only            0s
* rto excluding preflight     7s   <- compared against objectives.rto_seconds
  rpo                         12s   archive coverage gap at the requested point (NOT source-relative loss)

  integrity                   byte-fingerprint/pass  150/150 matched, 0 mismatch(es)
  target diff                 0 collision(s), 2 would-create (full)
  topic parity                intended []  unexpected []
  evidence                    immutable=false  create_only_enforced=false

  no capture gap or retention-pruned range overlaps the sampled window

  objectives (from the approved plan)
    rto_seconds                  900   compared against the starred row above
    rpo_seconds                  300
    pass_rate                      1   measured 1
    met                          yes

  qualifiers the fourteen rows above do not carry
    integrity.partial_reason  —
    engine_subreport          null — no engine sub-report was retained; this is NOT "the engine reported nothing wrong"
    redactions                —

  This table is a SUMMARY of a signed document, not the document. `--format json`
  prints the signed bytes; docs/verify-a-scorecard.md lists what the summary omits.
```

The table is a summary. The signed document is the JSON, and
[docs/verify-a-scorecard.md](docs/verify-a-scorecard.md) is written for the
person who has to decide how much weight to give it.

## Quickstart

One command, on a laptop, with no cloud resources — a source topic to a
verified point-in-time restore into a **new** topic and a signed receipt:

```bash
just e2e-up      # Kafka (KRaft) + MinIO, in docker compose
just mvp-demo
just e2e-down
```

`just mvp-demo` drives the product end to end and prints every exit code it
reads:

1. **Preflight.** Refuses unless the stack is up and healthy and `orders` and
   `payments` hold zero records — a second backup into a colliding `backup_id`
   does not accumulate, and a partial archive that a restore reads from
   happily is worse than a refusal.
2. **Produces** 1000 records into each topic and reads the end offsets back
   off the broker.
3. **`logweir backup run`** — the product taking the backup, behind the phase
   −1 admission guard, writing a DSSE-signed **backup receipt**.
4. **Verifies that receipt twice**: `logweir drill verify`, and
   `docs/verify_scorecard.py`, which shares no code with Logweir.
5. **`logweir drill approve`** over the exact plan bytes.
6. **`logweir restore run`** with `target.mode: newTopic` and a
   `restore.point_in_time` — the records land in topics that did not exist,
   on the same cluster, and nothing is torn down.
7. **Shows and verifies the scorecard**, twice again, and prints the evidence
   object keys an auditor would fetch.
8. **Prints one summary line**: the new topics, their record count read off
   the broker, the measured RTO and RPO, and the two evidence keys.

It needs `docker`, `cargo`, `openssl`, `awk` and `python3` with the
`cryptography` package (`pip install cryptography`; or point `LOGWEIR_PYTHON`
at an interpreter that has it). All of them are checked at second zero, before
anything is started. Everything it writes goes to `.demo/mvp/`, which is
gitignored, and it sweeps its own archive out of the shared bucket at both
ends.

**The keys it mints prove integrity, not provenance.** They are generated on
your machine, seconds before they sign, and nothing attests them — the demo
says so out loud rather than letting a green "verified" imply more than it
means. [docs/keys.md](docs/keys.md) is the rotation story.

There is also `./scripts/demo.sh`, the v0.1 **drill** demo: it seeds an archive
with the pinned engine and restores it into a segregated scratch cluster
behind a marker topic. It proves the drill path; `just mvp-demo` proves the
backup-and-recover path. Both leave your working tree clean.

See [docs/quickstart.md](docs/quickstart.md) for the same two paths in full,
and for running a drill against a cluster you already have.

### The whole thing, on Kubernetes, in one command

`just mvp-demo` is the CLI end to end. **`just laptop-demo` is the product end
to end** — the walk this project's definition of done is written as: apply one
file to docker-desktop Kubernetes and get CRDs, RBAC and the controller; serve
the UI as static files; create a `BackupSchedule`; watch a `Backup` produce a
signed receipt; create a `Restore`, approve it out of band, and read a signed
scorecard with measured RTO and RPO.

```bash
just e2e-up                                          # the stack is a precondition
LOGWEIR_DEMO_NONINTERACTIVE=1 just laptop-demo; echo "rc=$?"
```

Twelve numbered steps, each exit code printed on its own line, and a teardown
in a `trap` that deletes the install, both namespaces, the archive prefix, the
two author-only image tags, the proxy, the keypairs it minted, and the compose
stack. The transcript of the run that proved it is
[e2e/k8s/laptop-demo.md](e2e/k8s/laptop-demo.md).

**You never type a private key into the browser, and that is the point.** The
UI is a Kubernetes API client with no privilege of its own; the approval is
minted on your own machine with `logweir drill approve` and only the two public
documents it writes are pasted into the page. The demo's step 10 is install
gate **X-UIWRITE** and it has two halves: a scripted `create` returning `201`,
and the same create performed **by hand from the wizard's final step** — after
which `kubectl get restore <name> -o jsonpath='{.metadata.managedFields}'`
carries `"manager": "logweir-ui"`, which is how you know the write came from
the page and not from a command line.

Steps 1 and 3 are **author-only** and say so: they tag the local images with
the shipped repository names (the kubelet keys on the whole reference) and
point the controller at this laptop's MinIO. Neither relaxes the rule that
"published" means a pull from a registry the author does not control.

## Logging

`logweir drill run` writes structured JSON to **stdout**, one object per line;
stderr stays the human channel and carries the failure message on its own. The
default level is **`info` even when `RUST_LOG` is unset**, so a drill you did
not configure still emits a correlatable log: **every line Logweir emits at the
default level carries the run id** — on the event as `fields.run_id`, or on the
entered span as `span.run_id` — and every terminal path emits `drill finished`
with the exit code and what it means, which is the one place that meaning
survives into a log aggregator (on Kubernetes the code itself is buried; see
[docs/kubernetes.md](docs/kubernetes.md) §1).

That scope is enforced, not just worded. The default directive puts Logweir at
`info` and pins the dependencies that emit `tracing` — `h2`, `hyper_util`,
`object_store` and the `quinn` crates — to `warn`, because they log from
worker threads that never entered the run's span and their lines therefore
could not carry the id. A test re-derives that list from `Cargo.lock`, so a
future dependency bump cannot quietly widen the stream.

`RUST_LOG` still wins whenever it is set to anything non-blank
(`RUST_LOG=warn` keeps the error line and its run id and drops the rest;
`RUST_LOG=debug,ureq=warn` works as usual) — and an explicit `RUST_LOG` replaces
the scoping above, so a value like `debug` will show dependency lines with no
run id on them. A blank value is treated as unset rather than as "log nothing".
The run id is the same id in the signed scorecard and, as a leading comment, in
the `--metrics-file` textfile:

```bash
# Capture, then read: a pipe would replace `drill run`'s exit code with jq's,
# and the exit code is the contract (0/1/2/3/4 — docs/stability.md).
logweir drill run … > drill.log
echo $?
jq -r 'select(.level=="ERROR") | .fields.run_id' drill.log
```

## What Logweir is **not**

| Non-goal | Why, and what does it instead |
|---|---|
| **It writes no Kafka protocol code.** | The archive is produced and read by the pinned `kafka-backup` engine, which Logweir drives as a subprocess. Logweir's own client work goes through `rdkafka`. |
| **It does not write to the source cluster, on any path.** | Not a topic, not an offset commit, not a config. A drill restores into a *scratch* cluster, proved segregated by a marker topic before anything runs; a restore writes only into topics that did not exist. |
| **It does not consume OSO CRDs.** | No `KafkaRestore`, no `KafkaBackup`, no operator objects read or written. Logweir shells out to the engine binary and nothing else. |
| **It deletes nothing but the scratch topics it created.** | Retention **reports** and prints the commands; no Logweir component holds any object-store delete capability. Restore-in-place into a live topic is a **never**, not a later. |
| **It ships no HTTP surface and no UI image.** | The UI is a directory of static files served by `kubectl proxy --www=`; there is no server-side UI component, no sidecar, no `/metrics`, no `/healthz`. Metrics are a Prometheus textfile at `--metrics-file`. |
| **It reads no cluster metadata.** | No ACLs, no client quotas, no broker configs. Metadata snapshot and diff are a later tag. |

## Relationship to `osodevops/kafka-backup`

Logweir **drives** upstream's engine. It does not fork it, link it, or modify it.

- **MIT, redistributed.** Upstream is MIT-licensed; the licence and the source
  tarball for the pinned version are vendored in
  [third_party/](third_party/README.md) and the licence ships inside the
  container image.
- **Shelled out to, by digest.** `third_party/kafka-backup-binary.digest` pins
  an immutable image digest, never a tag. The allowlist `scripts/check-no-oso.sh`
  enforces is exactly four subcommands — `backup`, `restore`,
  `validate-restore`, `validation run` — and that is a *ceiling*, not a
  description: **v0.1 actually invokes two.** `OsoCliEngine` does not override
  `DataEngine::validation_run`, so the engine's own validation run is never
  executed and
  `engine_subreport` is `null` in every scorecard v0.1 produces. See
  [ADR 0002](docs/adr/0002-shell-out.md) and
  [stability.md](docs/stability.md).
- **Never linked.** No crate in this workspace depends on `kafka-backup-core`,
  under any feature or target; `scripts/check-no-oso.sh` proves it on every
  build with `cargo tree`, `cargo metadata --all-features` and a narrow linkage
  grep. See [ADR 0001](docs/adr/0001-no-core-link.md) and
  [ADR 0002](docs/adr/0002-shell-out.md).
- **Naming.** Logweir publishes nothing under the `osodevops/` namespace, the
  `kafkabackup.com` or `oso.sh` domains, or those API groups. Naming upstream's
  published image in order to *pull* it is a different thing from publishing
  under it, and is what the digest pin and the MIT redistribution above
  require.

Which engine versions are supported, and which are not:
[docs/stability.md](docs/stability.md) and
[docs/support-matrix.md](docs/support-matrix.md).

## Stability, in one paragraph each

- **Engine floor.** `kafka-backup` **0.16.0** is the floor for the
  unknown-config-key warning mechanism Logweir parses off the engine's streams;
  **0.21.0** is the floor for the full drill as shipped, and is the digest
  pinned here. `strimzi-backup-operator`'s hard-coded default `v0.19.1` is
  *below* that floor and is reported `unsupported (lever-absent)` — an operator
  default that has not caught up, never a fault Logweir raises.
- **Format policy.** `format_version` on the scorecard and the put-receipt is
  semver. A **minor** adds optional fields only; a reader ignores unknown
  fields within a major and **refuses** a higher major rather than guessing.
  v0.1.0 is the point at which this starts being a promise rather than a
  draft — see [docs/stability.md](docs/stability.md), "The v0.1.0 tag is the
  compatibility boundary".
- **Explicitly not a contract.** The Rust crates in this workspace are **not** a
  stable API before 1.0. Only the signed document formats and the `logweir`
  CLI's flags and exit codes are covered.

Read [docs/stability.md](docs/stability.md) before depending on any of it. It
also lists what v0.1 does **not** do — including `--from-cluster`, compacted
targets, `sample.anchor: tail|random`, and the fact that `engine_subreport` is
always `null`.

## Running the CLI on its own

**Logweir is not on crates.io at v0.1.0.** `cargo install logweir` does not work
and this README will not pretend otherwise: the workspace's internal
dependencies are declared by path with no version, so `cargo publish --dry-run`
refuses with *"all dependencies must have a version specified when
publishing"*. Publishing the five crates in dependency order is release work
that has not been done. Until it is:

```bash
# From a clone:
cargo install --path crates/logweir --locked
```

**The standalone binary carries no engine.** A `cargo install --path` or a release
tarball gives you `logweir` alone; it needs `kafka-backup` of the pinned digest
on `$PATH` (or at `$LOGWEIR_ENGINE_BIN`), plus `$LOGWEIR_ENGINE_VERSION` and
`$LOGWEIR_ENGINE_DIGEST` set to that image's version and digest. Logweir
refuses to sign a scorecard that names no engine, so an unset digest is an
error at run time, not a silently unattributed document.

The container image carries both:

```bash
docker build --platform linux/amd64 -t logweir:v0.1.0 .
```

**`--platform linux/amd64` is required, not optional.** Upstream publishes the
engine image for linux/amd64 only, so on an arm64 host a plain `docker build`
fails at the engine stage with `no match for platform in manifest: not found`.
The whole image is built for one platform deliberately: pinning only the engine
stage would put an amd64 binary inside an arm64 runtime, which fails at drill
time instead of at build time. **The Rust compile is not emulated**: the
builder stage runs on the build machine's own architecture and cross-compiles
to `x86_64-unknown-linux-gnu`, so on arm64 only the runtime stage's `apt-get`
and `COPY`s go through QEMU. `scripts/check-image.sh` asserts the shipped
binary's ELF `e_machine` rather than trusting that.

It is built `FROM debian:bookworm-slim` with `ca-certificates` and `libssl3` —
never musl or distroless, because the extracted engine is dynamically linked
against **glibc >= 2.36** and makes TLS connections.

Running it under Kubernetes has a small number of facts that will otherwise
cost you an afternoon — in particular, **the exit code that says "a drill ran
and did not pass" is nearly invisible to a Kubernetes operator** unless the Job
is shaped correctly. They are all in
[docs/kubernetes.md](docs/kubernetes.md); the install itself is
[docs/install.md](docs/install.md).

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
  cluster-admin kubeconfig gives the shipped bundle cluster-admin.
- **`self_attested: false` means only "two different keys".** One person
  holding both keypairs satisfies it. It is not evidence of an independent
  auditor, and a scorecard carrying it must not be read as one.

[SECURITY.md](SECURITY.md) carries the same four beside what **is** in scope.

**Release notes list the `ui/` bundle by digest.** The page runs with the
viewer's authority, so what is in the bundle matters: there is no telemetry in
it, nothing in it is fetched from anywhere else, and its contents are listed by
digest in the release notes so that the bytes a browser executed can be
compared against the bytes that were released.

## Documentation

| Document | For |
|---|---|
| [docs/install.md](docs/install.md) | **Installing Logweir.** The two paths, the five Secrets, the uninstall. |
| [docs/quickstart.md](docs/quickstart.md) | Running a drill against a cluster you already have. |
| [docs/verify-a-scorecard.md](docs/verify-a-scorecard.md) | The auditor who received a scorecard and has to decide what it proves. |
| [docs/formats/drill-scorecard.md](docs/formats/drill-scorecard.md) | Every field, its type, and its formula. |
| [docs/stability.md](docs/stability.md) | Version floors, format policy, and every known limitation of v0.1. |
| [docs/support-matrix.md](docs/support-matrix.md) | Which engine versions are tested green. |
| [docs/kubernetes.md](docs/kubernetes.md) | Scheduling drills on Kubernetes, and the exit-code trap. |
| [docs/adr/](docs/adr/0001-no-core-link.md) | Why the architecture is the way it is. |
| [SECURITY.md](SECURITY.md) | Reporting a vulnerability, and what is in scope. |
| [CONTRIBUTING.md](CONTRIBUTING.md) | DCO sign-off, no CLA, inbound = outbound Apache-2.0. |
| [MAINTAINERS.md](MAINTAINERS.md) | Who reviews, and what a format change needs. |
| [TRADEMARKS.md](TRADEMARKS.md) | LOGWEIR is a working name; what clearing it would take. |
| [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md) | Every package in the resolved graph, its SPDX expression and its copyright line. |

---

Licensed under [Apache-2.0](LICENSE); see [NOTICE](NOTICE). Contributions
require a DCO sign-off (`git commit -s`) and no CLA.

Documentation is licensed [CC-BY-4.0](docs/LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
