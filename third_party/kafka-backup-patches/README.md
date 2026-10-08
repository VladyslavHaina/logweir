# third_party/kafka-backup-patches/

Logweir builds the `kafka-backup` engine from the vendored OSO source
(`third_party/kafka-backup-v<release>.tar.gz`), and this folder is the ordered
set of changes Logweir carries on top of that source. OD-3 (decided
2026-10-07, `docs/to-do/product-expansion.md`) is the decision; ADR 0002's
amendment in `docs/architecture.md` records it.

The folder is **empty of patches** until the first fix lands: Logweir's build
is then the OSO release's own source, built by Logweir, and it carries
Logweir's version identity (below) so nobody mistakes it for OSO's binary.

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
- Line 1 is `Reason: <one line>`; line 2 is either `Upstream: https://…` or
  blank; then a blank line, then a unified diff against the extracted source
  root (`a/crates/…`, `b/crates/…`), as `git diff` writes it.
- The build applies each patch with `git apply` (exact context, no fuzz), so
  a patch that does not apply cleanly fails the build instead of landing
  somewhere else.

`scripts/engine-source.sh check` enforces the format, and
`crates/logweir/tests/engine_build.rs` enforces it again in the default test
set, each with negative controls.

## The version identity, and what changes with the folder

`third_party/kafka-backup-build.env` names Logweir's build:

- `ENGINE_VERSION=<release>+logweir.<n>`: what `kafka-backup --version`
  prints for Logweir's build (`kafka-backup 0.23.3+logweir.1`). `<n>` counts
  Logweir's builds of one OSO release, from 1.
- `ENGINE_DIGEST=sha256:<hex>`: the build-input digest, over the tarball's
  sha256, every patch's name and sha256 in order, and `ENGINE_VERSION`.
  `scripts/engine-source.sh digest` prints it.

**Any change to this folder bumps `<n>` and re-records `ENGINE_DIGEST`.** The
build refuses a digest that does not describe the inputs, and so does the
test set; the bump keeps two different engines from sharing one version.
`logweir doctor`, the controller's Job environment, the runner image and every
signed scorecard and receipt name this identity, so they move together
(`crates/logweir/tests/engine_pin.rs`).

Upstream's MIT licence for the source is `third_party/LICENSE-MIT`. Logweir's
patches are Apache-2.0, as the rest of this repository.
