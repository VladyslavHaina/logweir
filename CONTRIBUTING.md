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

That is a deliberate choice with a consequence, and the consequence is the
point. Relicensing a project requires the agreement of every copyright holder
unless a CLA has assigned those rights to one party. Without a CLA, once a
handful of outside contributors have landed code, **no single party — the
project's own author included — can quietly relicense the whole.** Terraform
and Redis were both relicensed by their owners; what preserved openness in each
case was the fork right under the old licence, not the licence family. Refusing
a CLA is how this project makes that reversal expensive for itself in advance.

## Licence

Logweir is licensed under **Apache-2.0**. By contributing, you agree that your
contributions are licensed under the same terms: **inbound = outbound**,
Apache-2.0. The full text is in [LICENSE](LICENSE) and what the project owes
its dependencies is in [NOTICE](NOTICE) and
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).

**Documentation is CC-BY-4.0**, not Apache-2.0 — the text is in
[docs/LICENSE-docs](docs/LICENSE-docs). A change under `docs/` is contributed
under that licence, with the same DCO sign-off.

## Before you push: run `just gate`

```bash
just e2e-down              # the timing gate refuses while 9092 or 9000 answers
just gate; echo "rc=$?"
```

**`just gate` is the one command that runs every check this repository has**, in
one order, on a laptop. It takes about **seven minutes** on a quiet
10-core machine from a warm debug target directory — of which most is the
release compile — and it contains exactly **two workspace compilations**, one
debug and one release. `docs/gates.md` carries the per-line seconds, what each
gate proves, and what each gate does **not** prove; read it before adding a
check, and add the row in the same commit as the check.

**Please do not treat `.github/workflows/` as the gate.** Three workflows have
now executed — `ci.yml` (run 34700987730), `no-oso.yml` (run 34700987811) and
`kind-demo.yml` (run 34700987743), all green on commit `a113dd2`, 2026-09-12 —
and `ci.yml` still mirrors the gate set as documentation rather than replacing
it: `just gate` is the enforcement point, it is what a laptop can run before a
push, and `release.yml` has never run at all (no tag has been pushed, so
`docs/tag1-checklist.md` clause 4 reads `blocked: no tag pushed` and the digest
rows read `blocked: images not published`). If a check is not in `just gate` or in
`docs/gates.md`'s stack/cluster table, nothing runs it, and
`crates/logweir/tests/gate_lint.rs` fails when a new `scripts/check-*.sh` is in
neither.

Seven recipes are deliberately **outside** `just gate` — `e2e`, `smoke`,
`smoke-weirkeeper`, `mvp-demo`, `k8s-demo`, `laptop-demo`, `pitr` — because each
needs the compose stack, a Kubernetes cluster or a Docker build. They are in
`docs/gates.md`'s table with what each proves. Run the one your change touches.

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
