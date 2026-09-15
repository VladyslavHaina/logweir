# Contributing

Thanks for your interest in contributing to Logweir.

## Developer Certificate of Origin — required on every commit

Every commit must carry a `Signed-off-by:` trailer under the Developer
Certificate of Origin 1.1. Sign off with:

```bash
git commit -s
```

which appends

```
Signed-off-by: Your Name <you@example.com>
```

using your `user.name` and `user.email`. Use your real name and a real address:
the DCO is a certification, and an anonymous one certifies nothing. A commit
without the trailer is not merged; `git commit --amend -s` fixes the last one
and `git rebase --signoff <base>` fixes a branch.

### The certificate, in full

```
Developer Certificate of Origin
Version 1.1

Copyright (C) 2004, 2006 The Linux Foundation and its contributors.

Everyone is permitted to copy and distribute verbatim copies of this
license document, but changing it is not allowed.


Developer's Certificate of Origin 1.1

By making a contribution to this project, I certify that:

(a) The contribution was created in whole or in part by me and I
    have the right to submit it under the open source license
    indicated in the file; or

(b) The contribution is based upon previous work that, to the best
    of my knowledge, is covered under an appropriate open source
    license and I have the right under that license to submit that
    work with modifications, whether created in whole or in part
    by me, under the same open source license (unless I am
    permitted to submit under a different license), as indicated
    in the file; or

(c) The contribution was provided directly to me by some other
    person who certified (a), (b) or (c) and I have not modified
    it.

(d) I understand and agree that this project and the contribution
    are public and that a record of the contribution (including all
    personal information I submit with it, including my sign-off) is
    maintained indefinitely and may be redistributed consistent with
    this project or the open source license(s) involved.
```

## No CLA

**No copyright-assignment CLA is required or accepted.** You keep your
copyright; the DCO sign-off above is the only thing this project asks of a
contributor, and it is a certification rather than a transfer.

## Licence

Logweir is licensed under **Apache-2.0**. By contributing, you agree that your
contributions are licensed under the same terms: **inbound = outbound**,
Apache-2.0. The full text is in [LICENSE](LICENSE) and what the project owes
its dependencies is in [NOTICE](NOTICE) and
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).

**Documentation is CC-BY-4.0**, not Apache-2.0 — the text is in
[docs/LICENSE-docs](docs/LICENSE-docs). A change under `docs/` is contributed
under that licence, with the same DCO sign-off.

## Development setup and checks

Use the pinned Rust toolchain in `rust-toolchain.toml`. The workspace also
needs a C/C++ build toolchain and CMake for librdkafka. Install `just`, Node.js
20 or newer, Helm 4 or newer, and Python with `cryptography` and `pytest` for
the corresponding gates. Docker is needed to extract the engine and run demos.
See [architecture](docs/architecture.md) for the source map.

```bash
just engine                  # extract the digest-pinned engine on a fresh clone
cargo test --workspace --locked
```

Before pushing, run the full local gate with the Compose stack down:

```bash
just e2e-down                 # removes the demo stack and its volumes
just gate
```

[The gate reference](docs/gates.md) lists every check, prerequisites, recorded
timings and workflow evidence. The gate compiles the workspace in debug and
release profiles; individual test commands are useful during development.

Eight recipes run separately because they need Docker, Kafka or Kubernetes:
`e2e`, `smoke`, `smoke-weirkeeper`, `mvp-demo`, `k8s-demo`, `laptop-demo`,
`pitr`, and `helm-demo`. Run those relevant to your change. Release readiness
is tracked in [the release checklist](docs/tag1-checklist.md).

Changes to `charts/logweir/`, `config/crd/` or `ui/` need `just chart-check`.
The chart's CRDs and shipped UI assets are intentionally copied into the chart
so it can be distributed independently. Keep those copies byte-identical to
the canonical files and commit regenerated `charts/logweir/rendered/` output.
Do not delete them as duplicate files. Likewise, the generated schemas,
`logweir.yaml`, signed test fixtures and dependency notices are checked inputs
to distribution or regression tests.

## Before you open a pull request

- `just gate` exits 0. (`cargo test --workspace` is one of its lines, and
  `just lint` overlaps it almost entirely — fourteen of its fifteen lines are
  gate lines, and the fifteenth's property is held by a test the gate runs — so
  both remain useful for a fast inner loop.)
- A new dependency is a decision, not a detail: the workspace graph is closed
  and `THIRD_PARTY_NOTICES.md` is generated from it
  (`bash scripts/gen-third-party-notices.sh --write`), so adding a crate means
  regenerating that file in the same commit.
- A guard without a mutant is not a guard. A test that cannot fail is worse
  than no test, because the ledger records it as passing.
- **Every exit code is read directly, never through a pipe.** `cmd | grep`
  reports grep's status. `crates/logweir/tests/gate_lint.rs` lints the justfile
  and every file under `scripts/` for that.

---

Documentation is licensed [CC-BY-4.0](docs/LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
