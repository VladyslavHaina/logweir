# The gates

**One command runs everything this repository can check on a laptop:**

```bash
just e2e-down          # the timing gate refuses while 9092 or 9000 answers
just gate; echo "rc=$?"
```

Every check here must be reachable by that command or it is not enforced.
**"Wired into `ci.yml`" is not a gate in this repository.** No workflow here has
ever executed — there is no git remote — so `.github/workflows/` is
documentation; `docs/tag1-checklist.md` clauses 2 and 4 record that as
`blocked: no remote`, and the state is stated again at the bottom of this file.

## What `just gate` costs, and what makes the number what it is

**Measured on a quiet 10-core Apple-silicon host, `CARGO_BUILD_JOBS=4`, compose
stack down (9092 and 9000 both refuse), debug target directory already warm and
the release profile cold** — i.e. the first `just gate` after an ordinary
`cargo test` inner loop. Load averages 3.8 at the start and 3.3 at the end; a
Docker Desktop VM was running throughout, with no compose stack in it.

| | |
|---|---|
| total | **420.1 s (7 min 0 s)** |
| of which the release compile | **315.7 s** — line 5, 75% of the run |
| the same gate re-run immediately, everything cached | **118.9 s** and **121.0 s** on two consecutive `just gate` runs, both rc 0 |
| workspace compilations | **two** — one debug, one release |
| lines | 25 |
| standing threshold | **840 s** — 2× the recorded total |

**The recipe contains exactly two workspace compilations, and the ordering is
what makes that true.** `./scripts/time-unit-suite.sh` sits immediately after
`cargo test --workspace` and before the `--release` line, because its
`SUITE_CMD` is that same `cargo test --workspace` in the same profile and the
same target directory: at that position it is a second **run** of artefacts the
previous line already built, not a third build. `cargo clippy --workspace
--all-targets` shares the debug target set. Twenty of the remaining lines
compile nothing at all. A fully cold run — no target directory at all, measured by the review on 2026-09-11 — took 698 s, 142 s under the 840 s threshold; a cold run that crosses the threshold is a report, not a `cargo clean`.

**Move the timing line above `cargo test --workspace` and nothing goes red.**
`just gate` still exits 0; what changes is that the timing line silently absorbs
a whole debug compile and the two-compilation claim above becomes false. **The
per-line figures in the table below are what catch that** — there is no guard,
and this row says so rather than implying one exists.
`crates/logweir/tests/gate_lint.rs::gate_lint_docs_record_the_measured_seconds`
keeps the figures present; it cannot keep them honest.

**Over 2× the recorded total, stop and report.** `cargo clean` is **not** the
remedy: STANDING RULE 6's triggers are 50,000 files in `target/debug/deps`
(`./scripts/check-deps-count.sh` is the detector, and it is line 17 of the gate)
or a single workspace *run* over five minutes. A slow `just gate` is neither.

**Nothing in the gate reaches the network.** `check-links.sh` never fetches
http/https; `cargo deny --offline` and `cargo metadata --offline` resolve
nothing; `check-one-signer.sh` and `check-no-oso.sh` read the committed
`Cargo.lock`; `check-ui-behaviour.sh` runs `node --test` over checked-in files;
`time-unit-suite.sh` exports a pinned `RUSTUP_TOOLCHAIN` precisely so rustup has
nothing to sync.

## The gate, line by line

Each row is one line of the `gate` recipe, in the order it runs.

| # | command | what it proves | what it does **not** prove | measured s |
|---|---|---|---|---|
| 1 | `cargo fmt --all -- --check` | the tree is formatted; a reviewer never reads a whitespace diff | nothing about behaviour | 0.5 |
| 2 | `cargo clippy --workspace --all-targets -- -D warnings` | no clippy lint fires anywhere, tests and examples included; **this is the first debug compilation** | that the code is correct — `cargo test` was green here while clippy was not | 1.0 |
| 3 | `cargo test --workspace` | the whole default suite passes in debug | anything needing a broker, a bucket, a cluster or a daemon: those are `#[cfg(feature = "e2e")]` or `#[ignore]`d | 22.6 |
| 4 | `./scripts/time-unit-suite.sh` | the whole suite is inside 120 s and **no single test is over 15 s** — the bound that catches a re-added 20 s dial timeout; it **re-runs** line 3's artefacts and compiles nothing | that no test dials: a bound is not an audit. The grep half is `crates/logweir/tests/no_network_in_unit_tests.rs` | 59.4 |
| 5 | `cargo test --workspace --release` | the suite passes with optimisations on — overflow checks off, different inlining; **this is the second and last compilation** | nothing the debug run did not, except where a release-only difference bites | 315.7 |
| 6 | `cargo deny --offline check licenses` | every package in the resolved graph offers a licence `deny.toml` admits (`--offline` is a **global** flag and goes before `check`) | that the copyright notices travel with the binary — that is line 24's obligation, and it is a different half of Global Constraint 15 | 0.4 |
| 7 | `./scripts/check-no-oso.sh` | `kafka-backup-core` is in no dependency graph, no manifest and no `use`; and every engine invocation names only the four GC3 subcommands | that the engine binary is absent — it is deliberately present under `.engine/`, pinned by digest | 0.6 |
| 8 | `./scripts/check-pure-core.sh` | the pure layer builds `--no-default-features` and carries no `aws-*`, `rusoto`, `kube`, `k8s-openapi` or `kafka-backup` dependency | that `weirkeeper` or `logweir-store` are pure — they are outside the layer by ADR | 0.1 |
| 9 | `./scripts/check-one-signer.sh` | a **link-time** property: the workspace crates reaching `logweir-evidence` are exactly `{logweir, e2e}`; and check 2's primitive set is **derived from the graph** minus `scripts/logweir-evidence-primitives.classify`, fail-closed | **that anything is unable to sign.** It is a link-time property and nothing more: the signing key is a Kubernetes Secret, and `create pods` in its namespace is equivalent to holding it, so the *capability* is unbounded wherever Job CRUD over that namespace is held (Global Constraint 27) | 0.4 |
| 10 | `./scripts/check-withdrawn-claim.sh` | the withdrawn stronger claim about signing is on no shipped surface, in no paraphrase | anything about the code — it is a corpus grep over prose | 0.7 |
| 11 | `./scripts/check-no-archive-write.sh` | no component outside the runner holds an archive-write capability; the controller's evidence credential is read-only | that nothing *can* delete — it answers "CAN it?", from the manifests and the source | 0.4 |
| 12 | `./scripts/check-ui-offline.sh` | the shipped UI loads no remote asset: no CDN, no font host, no analytics | that the page is correct — see line 13 | 1.9 |
| 13 | `./scripts/check-ui-behaviour.sh` | the UI's `node --test` suite passes over checked-in files (interface **I24**) | anything against a live cluster: no `kubectl proxy` is started here | 0.3 |
| 14 | `./scripts/check-unverified-labels.sh` | every mark in the shipped docs is labelled in place with what would verify it (spec §16 clause 9) | that the labelled claims are true — it checks that unverified ones say so | 0.3 |
| 15 | `./scripts/check-verifier-parity.sh` | the Rust reader and `docs/verify_scorecard.py` reach the same verdict over the signed corpus, scorecards **and** backup receipts; it **builds** `target/release/logweir` when neither profile has one | that either reader is right — it proves they agree, including on refusals | 1.0 |
| 16 | `./scripts/check-invariant-corpus.sh` | the auditor's reader alone walks `e2e/fixtures/invariants/index.json` and every arm's arithmetic closes (interface **I31**) | that the Rust reader agrees — that is line 15 and `two_reader_parity.rs` | 3.2 |
| 17 | `./scripts/check-deps-count.sh` | `target/debug/deps` is under the 50,000-file ceiling, and **prints the count on every run** so the trend is readable long before it fails | that the build is fast — it is the detector for the one cause that made a 13 s suite take twenty minutes | 0.1 |
| 18 | `./scripts/check-dod.sh` | the ASF attribution footer, the GC14 namespace rule and link integrity across the shipped docs; it prints what it could **not** check as UNVERIFIED | anything needing GitHub or a network — it is a laptop-only check by design | 0.9 |
| 19 | `./scripts/check-links.sh docs/ README.md … ui/` | every **relative** Markdown link in the named paths points at a file that exists, and it says how many files and links it checked | **it never fetches http/https** — a repository-integrity check, not a network check. And it walks `-name '*.md'` only: within `ui/` that is `ui/README.md` and `ui/tests/fixtures/README.md`, and **nothing else** — the UI's offline integrity is line 12's and its behaviour is line 13's | 0.1 |
| 20 | `./scripts/render-install.sh --check` | `logweir.yaml` is what `config/` renders to, byte for byte (interface **I26**) | that it applies — X-APPLY is a cluster gate and is in the table below | 0.2 |
| 21 | `just schema-check` | regenerating **both** schemas leaves `schemas/` byte-identical (Global Constraint 12's drift arm) | that the schemas describe anything an adopter wants — only that they still describe their types | 1.1 |
| 22 | `just crds-check` | regenerating the six CRDs leaves `config/crd` byte-identical: a CRD change is a **format** change and must arrive as a diff | that the CRDs install — that is X-APPLY, in the table below | 0.9 |
| 23 | `just receipt-schema-check` | the same recipe as line 21 under the name the plan's gate list uses for the backup-receipt arm. `just schema` regenerates **both** documents, so one drift check covers both; naming it twice runs the check twice rather than claiming a check the tree does not have | anything line 21 did not | 0.7 |
| 24 | `bash scripts/gen-third-party-notices.sh > target/tpn.check && diff -u THIRD_PARTY_NOTICES.md target/tpn.check` | the checked-in third-party inventory is exactly this generator's output over the resolved graph (interface **I30**) — the attribution half of Global Constraint 15 that line 6 does not cover | that the notices are legally sufficient; it proves they are complete and current | 0.3 |
| 25 | `just verify-py` | the auditor's Python verifier passes its own pytest suite, under the repository's interpreter-resolution order | that the Rust reader agrees — that is line 15 | 6.5 |

## The stack/cluster table

**`just gate` deliberately runs none of these seven.** Each needs the compose
stack, a Kubernetes cluster or a Docker build — and `./scripts/time-unit-suite.sh`
**refuses to run, exit 1, while 9092 or 9000 answers**, so a gate that started
the stack would fail itself. They are run explicitly, one at a time, by whoever
owns the shared resource at that moment (STANDING RULE 3).

`crates/logweir/tests/gate_lint.rs::gate_lint_every_check_is_in_the_gate`
asserts that every `scripts/check-*.sh` in the tree is invoked by `just gate`
**or** by a recipe named in this table, that the two sets are **disjoint**, and
that together they are **exhaustive**. Thirteen scripts are in the gate; the two
below are here; fifteen exist.

| recipe | what it proves | what it needs | `check-*.sh` it invokes |
|---|---|---|---|
| `just e2e` | the whole product against a real broker, a real MinIO and a real archive: backup, restore, verify, signed evidence, both readers | the compose stack **up** (`just e2e-up`), and `.engine/kafka-backup` extracted. On CI (`ci.yml`'s `e2e` job) one test is skipped by name, `a_pod_really_reaches_the_k8s_listener`: it needs the laptop's `docker-desktop` cluster, and its property is proven there by `kind-demo.yml`'s in-cluster probe | none directly — the suite is Rust, under `--features e2e` |
| `just smoke` | the runner image runs: the engine and `logweir` both answer `--version` inside it, a drill approval works, and LICENSE/NOTICE/THIRD_PARTY_NOTICES are present | a Docker daemon, and `just image` (which it depends on) | `scripts/check-image.sh` |
| `just smoke-weirkeeper` | the controller image runs: `weirkeeper --version` answers inside it, the org-root fingerprint is baked in, the licence files are present and no engine licence directory is | a Docker daemon, and `just image-weirkeeper` (which it depends on) | `scripts/check-image-weirkeeper.sh` |
| `just mvp-demo` | the **product's own CLI path** end to end on the local stack: produce → `backup run` → receipt verified by both readers → `drill approve` → `restore run` in `newTopic` mode at a point in time → scorecard verified by both readers | the compose stack up; it refuses a stack whose source topics already hold records | none directly |
| `just k8s-demo` | Phase B's exit criterion: the controller reconciling real objects on docker-desktop Kubernetes, with the runner as a Job on a digest-pinned image | docker-desktop Kubernetes, both local images, and the compose stack; its step 2 runs `just lint` **with the stack down**, before taking it | none directly |
| `just laptop-demo` | Phase C's exit criterion and install gate **X-UIWRITE**: a stranger's twelve steps from `kubectl apply` to a signed scorecard, with step 10(b) performed from the UI wizard so `managedFields` names `logweir-ui` | docker-desktop Kubernetes, both local images, the compose stack, a browser | none directly |
| `just pitr` | **G-PITR alone**: the inclusive point-in-time boundary across three partitions, against the real stack, for the price of one restore | the compose stack up (`just e2e-up` alone is enough) | none directly |

## Which workflows have executed

**None — no remote exists; see `docs/tag1-checklist.md` clauses 2 and 4.**

Both of those rows read `blocked: no remote`. `ci.yml` mirrors the gate set as
documentation and `crates/logweir/tests/gate_lint.rs::gate_lint_ci_mirrors_the_gate`
keeps the mirror complete and keeps that sentence agreeing with the checklist —
but a workflow that has never run has enforced nothing, and this file does not
describe one as a gate.

`kind-demo.yml`'s **mechanism** was proved once, locally, by hand, on
author-only images and under an explicit authorisation. That is not a CI run, it
does not close clause 2, and the row that would close it names what does: the
URL of one green `kind-demo` run on a cluster that run created.

---

Documentation is licensed [CC-BY-4.0](LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
