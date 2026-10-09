# third_party/kafka-backup-patches/

Logweir builds the `kafka-backup` engine from the vendored OSO source
(`third_party/kafka-backup-v<release>.tar.gz`), and this folder is the ordered
set of changes Logweir carries on top of that source. OD-3 (decided
2026-10-07, `docs/to-do/product-expansion.md`) is the decision; ADR 0002's
amendment in `docs/architecture.md` records it.

Without patches, Logweir's build is the OSO release's own source, built by
Logweir; it still carries Logweir's version identity (below), so nobody
mistakes it for OSO's binary.

| Patch | What it fixes | Oracle |
| --- | --- | --- |
| `0001-lockfile-rustls-h2-spin.patch` | The engine's Cargo.lock only: rustls 0.23.43 → 0.23.45 (RUSTSEC-2026-0285) with rustls-webpki 0.103.13 → 0.103.15, h2 0.4.15 → 0.4.19 (RUSTSEC-2026-0258), and the yanked spin 0.9.8 → 0.9.9. The first run of the engine's `cargo deny` gate (PROD-00.2) found all three in the shipped graph. Logweir's own graph already carries rustls 0.23.45, rustls-webpki 0.103.15 and h2 0.4.19 (it has spin 0.10.1, not 0.9). | `scripts/ci-check.sh`'s engine `cargo deny` (fails on the unpatched lock), and PROD-00.2's parity suites |
| `0002-manifest-replication-factor.patch` | `merge_manifests` (`crates/kafka-backup-core/src/backup/engine.rs`) carries `source_replication_factor` from the current session into a topic the stored manifest already holds, as it already did `original_partition_count`. Without it the manifest records the factor for the FIRST topic a backup saves only: the configuration capture puts every topic into the manifest before the first save, so each later topic is already stored without a factor and the merge kept that. Found by PROD-05.1, fixed by FX-21 (Logweir's phase 7 had read the missing factor as matching). An upstream bug-class fix. | The patch's own engine unit tests (`test_merge_manifests_updates_replication_factor`, `…_preserves_replication_factor_when_none`, `test_manifest_persistence_keeps_every_topics_replication_factor`; the first and third fail on the unpatched source), and FX-21's e2e row `e2e/tests/replication_factor_parity.rs` (a three-topic backup on `cluster3` records every topic's factor) |

## The policy: patch first

1. **A fix lands here as soon as its oracle passes.** The carrying window is
   zero: a fix is not held back for an upstream release.
2. **One line of reason, in the patch itself.** The first line of every patch
   file is `Reason: <one line>`: what the patch fixes and which oracle shows
   it (for example the PROD-00.3 row and its acceptance id).
3. **Upstream is optional, and comes after shipping.** A patch may carry an
   `Upstream: <https URL>` line, the second line, naming the upstream pull
   request or issue. Upstream PRs are for bug-class fixes only (C1, C4, C5, C6,
   C12, C14 to `kafka-backup`; C13 to `kafka-protocol-rs`), never the
   filter-rule or SASL-plugin patches.
4. **A patch is dropped when an OSO release contains it.** Moving the pin
   (PROD-00.3f's refresh procedure) re-applies every patch to the new source;
   a fix the new release already contains no longer applies cleanly, the
   build refuses, and the patch is deleted in the same change that moves the
   pin.

## The format

- File names are `NNNN-<slug>.patch`: four digits, then lowercase letters,
  digits and hyphens. The digits order the set, they are unique, and the
  build applies the patches in that order. Nothing else lives here except
  this README.
- Line 1 is `Reason: <one line>`. Line 2 is either blank, or
  `Upstream: https://…` followed by a blank line 3. Then comes a unified diff
  against the extracted source root (`a/Cargo.lock`, `a/crates/…`), as
  `git diff` writes it.
- The build applies each patch with `git apply` (exact context, no fuzz), so
  a patch that does not apply cleanly fails the build instead of landing
  somewhere else.

`scripts/engine-source.sh check` enforces the format, and
`crates/logweir/tests/engine_build.rs` enforces it again in the default test
set, each with negative controls.

## The version identity, and what changes with the folder

`third_party/kafka-backup-build.env` names Logweir's build:

- `ENGINE_VERSION=<release>+logweir.<n>`: what `kafka-backup --version`
  prints for Logweir's build (`kafka-backup 0.23.3+logweir.2`). `<n>` counts
  Logweir's builds of one OSO release, from 1.
- `ENGINE_DIGEST=sha256:<hex>`: the build-input digest, over the tarball's
  sha256, every patch's name and sha256 in order, and `ENGINE_VERSION`.
  `scripts/engine-source.sh digest` prints it.

**Any change to a patch or to the tarball is a new build:** bump `<n>`,
re-record `ENGINE_DIGEST` (`scripts/engine-source.sh digest`), and APPEND the
pair to `third_party/kafka-backup-builds.txt`, the ledger of every build, and
to `SHIPPED_BUILDS` in `crates/logweir/tests/engine_pin.rs`. That is enforced,
not only asked:

- the build and the test set refuse a digest that does not describe the
  inputs;
- `scripts/engine-source.sh` refuses a ledger whose last line is not the build
  env's pair, a version or a digest recorded twice, and an `<n>` that does not
  rise within a release;
- `engine_pin.rs` refuses a ledger that does not begin with every shipped
  build, unchanged, so a recorded line rewritten in place to describe new
  inputs under an old version fails too.

So two different engines never share one version. (An edit to this README
changes no digest and needs no bump.)

**The stamp is the CLI's `--version` only.** Inside the engine,
`env!("CARGO_PKG_VERSION")` still reads the OSO release (`0.23.3`): for
example the `tool_version` of the engine's own `validation run` evidence, which
Logweir never runs, and the crate versions an SBOM of the image lists for
`kafka-backup-cli` and `kafka-backup-core`. Logweir's identity is the CLI's
`--version`, `/etc/logweir/engine-identity` and the build env, never those.
`logweir doctor`, the controller's Job environment, the runner image and every
signed scorecard and receipt name this identity, so they move together
(`crates/logweir/tests/engine_pin.rs`).

Upstream's MIT licence for the source is `third_party/LICENSE-MIT`. Logweir's
patches are Apache-2.0, as the rest of this repository.
