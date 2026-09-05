# ADR 0001: Logweir never links `kafka-backup-core`

## Status

Accepted. Enforced mechanically — see *How this is proved* below.

## Context

Logweir drives an existing backup engine, `osodevops/kafka-backup`, whose Rust
workspace publishes a library crate, `kafka-backup-core`. Depending on that
crate directly would be the obvious way to reuse its manifest types, its
segment reader and its evidence envelope.

Three facts make it the wrong choice:

1. **Licence asymmetry.** Upstream is MIT with no CLA and no DCO; Logweir is
   Apache-2.0 with a `NOTICE`. Linking an MIT library into an Apache-2.0
   binary is permitted, but it makes upstream's relicensing risk Logweir's
   problem: an inbound contribution to upstream whose provenance nobody
   recorded becomes code inside Logweir's artifact.
2. **Version coupling.** `kafka-backup-core` is not a published, semver-stable
   API — it is the internal library of a binary. A link would make every
   upstream refactor a Logweir build break, and would make Logweir's supported
   engine range exactly one version wide.
3. **The claim Logweir makes.** A drill scorecard says "this archive restored,
   and here is what the restore actually produced". If Logweir decoded the
   archive with upstream's own code, a bug shared by both would be invisible:
   the reader and the writer would agree because they are the same program.
   Independent decoding is what makes a fingerprint mismatch mean something.

## Decision

**No crate in this workspace depends on `kafka-backup-core`, under any feature
or target.** Where Logweir needs an upstream data shape it **vendors the struct
definition** into `crates/logweir-engine-oso/src/vendored/`, with a
`[VERIFIED …]` citation naming the upstream file and line it was read from, and
`cargo xtask sync-upstream --tag <tag>` re-compares the vendored copies against
a read-only checkout of that tag.

Two consequences follow that are worth stating because they look like
inconsistencies and are not:

- **Pulling upstream's published container image is permitted** and is required
  by the digest-pinning and MIT-redistribution constraints. Global Constraint
  14 governs what Logweir *publishes* under, not what it *pulls* from
  (adjudicated as global ruling GR6).
- **One future crate is exempt.** The separate-repository
  `logweir-sasl-msk-iam` crate (SP4) is the single crate that will link
  `kafka-backup-core`, pinned, with its own compatibility matrix. It is not a
  dependency of anything Logweir ships, which is what keeps this ADR literally
  true of the released artifact.

## How this is proved

Not by a comment, and not only by a grep — a text match on a type name says
nothing about linkage. `scripts/check-no-oso.sh` runs on every build (`just
lint`, and as a required check in `.github/workflows/ci.yml`) and asserts three
separate things:

1. `cargo tree --workspace --invert kafka-backup-core` finds no consumer — and
   the script distinguishes "the package is absent from the graph" from "cargo
   errored", so a broken workspace cannot read as a pass.
2. `kafka-backup-core` appears nowhere in `cargo metadata --all-features`, so a
   dependency hidden behind an off-by-default feature is caught too.
3. No source file under `crates/` carries `use kafka_backup_core` or
   `extern crate kafka_backup_core`, excluding the vendored directory that
   legitimately *names* upstream types without linking them.

## Consequences

- Every upstream struct Logweir reads must be vendored and re-verified on an
  engine bump. `cargo xtask sync-upstream` is the mechanism; a drift is a
  failing CI job, not a surprise at drill time.
- Logweir can support a **range** of engine versions rather than one, which is
  what `docs/support-matrix.md` publishes.
- Logweir decodes `.kbak` segments itself. That is more code, and it is the
  code that makes an integrity claim independent.

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
