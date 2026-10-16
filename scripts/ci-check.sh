#!/usr/bin/env bash
# Shared local/CI quality check. Requires Rust, just, Node >= 20, Helm >= 4,
# kubectl, cargo-deny, Docker, and Python with cryptography and pytest.
# No running broker or Kubernetes cluster is needed.
#
#   scripts/ci-check.sh                    the whole gate, in one run (`just gate`)
#   scripts/ci-check.sh <part> [<group>]   one part, as one CI gate runs it
#   scripts/ci-check.sh unit-<group>       the same as `unit <group>`: a part's
#                                          name is its gate's in ci/gates.toml
#
# THE PARTS (FX-53 §2.4). CI runs each in a job of its own, at once:
#   lint            the CLI build, `just lint` without its two whole-tree
#                   scans (fmt, clippy, the shell and Python lints), the
#                   engine-string scan of the debug CLI, the pure-library
#                   dependency checks
#   unit <group>    one crate group's tests with cargo-nextest, then its
#                   doctests (nextest runs none); the groups are below
#   generated       schema, CRD and chart drift
#   supply-chain    the notices diff, `cargo deny` over both graphs, the engine
#                   patch oracle
#   python          the Python rows (the gate machinery's included) and
#                   `just verify-py`
#   docs            links, the whole-tree text scans that read the docs too
#                   (unverified labels, unsafe scope, cosign pins), `doc_lint`
# With no part the script runs every command of every part, in one process and
# in the order it always has, with the workspace suite as ONE libtest run
# (`cargo test --workspace`), so a laptop needs no nextest. The unit groups and
# `docs` are CI's split of that one suite: their union is the workspace, which
# crates/logweir/tests/workflow_lint.rs holds every member to.
set -euo pipefail
cd "$(dirname "$0")/.."

# Function bodies are not indented: several contract tests read the lines of
# this file (`just lint`, `just schema-check`, `just crds-check`, the `for
# crate in` loop) as they stand at the left margin.

prepare() {
cargo fetch --locked
bash scripts/extract-engine.sh
}

# Build the current CLI once for verifier parity and UI plan/hash behavior.
# Explicitly select debug so these checks never depend on a stale optimised binary.
cli() {
cargo build --locked -p logweir
export LOGWEIR_BIN="$PWD/${CARGO_TARGET_DIR:-target}/debug/logweir"
if [[ "${CARGO_TARGET_DIR:-target}" = /* ]]; then
  export LOGWEIR_BIN="$CARGO_TARGET_DIR/debug/logweir"
fi
}

# `just lint` (the justfile; label_gate.rs and cli_verify.rs hold its lines) is
# the local lint. CI runs it as two gates (FX-53): the two scans of the whole
# tree's text, which read every document, run in `docs`; the rest, which reads
# no document but the verifier's code, runs here, so a docs-only change hits
# `lint`. workflow_lint holds every line of the recipe to exactly one of
# lint_code() and docs().
lint_code() {
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets -- -D warnings
./scripts/check-no-oso.sh
./scripts/check-pure-core.sh
./scripts/check-verifier-parity.sh
./scripts/check-invariant-corpus.sh
python3 scripts/test_k8s_scram_seed.py
python3 scripts/test_plat06_case_e_rows.py
python3 scripts/test_bind_credential_rows.py
python3 scripts/test_no_duplicate_definitions.py
./scripts/check-one-signer.sh
./scripts/check-no-archive-write.sh
}

lint() {
cli
lint_code
lint_tail
}

lint_tail() {
# The release gate's engine check, on every run, so a new uncited engine string
# fails here rather than first at a tag.
bash scripts/check-no-engine-in-binary.sh "$LOGWEIR_BIN"

# Pure libraries must remain usable without cloud, Kubernetes or engine clients.
cargo check --locked -p logweir-core -p logweir-evidence -p logweir-kafka -p logweir-verify --no-default-features
for crate in logweir-core logweir-evidence logweir-kafka logweir-verify; do
  dependencies="$(cargo tree --locked -p "$crate" --no-default-features --prefix none --edges normal)"
  if grep -Ei '^(aws-|rusoto|kube|k8s-openapi|kafka-backup)' <<< "$dependencies"; then
    echo "ci-check: $crate has a forbidden cloud, Kubernetes or engine dependency" >&2
    exit 1
  fi
done
}

# PROD-00.2 security review: every cosign / gh attestation verification in the
# repository pins the exact signer identity, the calling repository, ref and
# trigger, and the issuer (images.yml is a reusable workflow). It reads every
# .md, .sh, .yml, .py and workflow in the tree.
cosign_pins() {
python3 scripts/check-cosign-verify.py
}

# THE UNIT GROUPS (FX-53 §2.4): the workspace's members, each in exactly one.
unit_packages() {
case "$1" in
  core) echo "logweir-core logweir-evidence logweir-verify logweir-store logweir-kafka logweir-rdkafka-ffi logweir-engine-oso" ;;
  logweir) echo "logweir" ;;
  weirkeeper) echo "weirkeeper" ;;
  api) echo "logweir-api" ;;
  retention) echo "logweir-retention logweir-reaper" ;;
  xtask) echo "xtask" ;;
  e2e) echo "e2e" ;;
  *)
    echo "ci-check: no unit group \`$1\`; the groups are core logweir weirkeeper api retention xtask e2e" >&2
    return 2
    ;;
esac
}

# One group's tests: every test target, examples and bins included, through
# cargo-nextest (one process per test, binaries in parallel; the `ci` profile
# in .config/nextest.toml), then the doctests, which nextest does not run.
# `doc_lint` is the `docs` part's, so the logweir group leaves it out.
unit() {
local group="${1:-}" packages args=() filter=() p
packages="$(unit_packages "$group")"
for p in $packages; do args+=(-p "$p"); done
if [ "$group" = logweir ]; then filter=(-E 'not binary_id(logweir::doc_lint)'); fi
# weirkeeper's runner rows drive `<root>/target/debug/logweir` (the argv the
# controller emits must be one the CLI accepts); the whole-workspace run built
# it before its tests, and so does this group.
if [ "$group" = weirkeeper ]; then cli; fi
cargo nextest run --locked --profile ci "${args[@]}" ${filter[@]+"${filter[@]}"}
# xtask is a bin crate only: it has no library, so no doctests.
if [ "$group" != xtask ]; then cargo test --locked --doc "${args[@]}"; fi
}

python_rows() {
cli
python3 scripts/test-ci-images.py
python3 scripts/test-check-cosign-verify.py
python3 scripts/test-release.py
python3 scripts/test-recovery-kit.py
# FX-53: the gate machinery's own rows (scripts/ci-gate.sh: the hash, the
# lookup's refusals, the fail-safe cases) and the traced run's parser's
# (scripts/ci-trace.py, on recorded strace output).
python3 scripts/test-ci-gate.py
python3 scripts/test-ci-trace.py
just verify-py
}

generated() {
just schema-check
just crds-check
just chart-check
}

docs() {
just links
./scripts/check-unverified-labels.sh
./scripts/check-unsafe-scope.sh
cosign_pins
cargo nextest run --locked --profile ci -p logweir --test doc_lint
}

supply_chain() {
mkdir -p target
bash scripts/gen-third-party-notices.sh > target/tpn.check
diff -u THIRD_PARTY_NOTICES.md target/tpn.check
cargo deny check licenses advisories sources bans

# PROD-00.2 (OD-3): the ENGINE's graph too. Logweir builds kafka-backup from
# the vendored source, so its lockfile — after the patch folder, exactly as
# the image build prepares it — is Logweir's to check, under its own policy
# (third_party/kafka-backup-deny.toml), sources included: crates.io only. A
# finding is fixed by a patch in third_party/kafka-backup-patches/, not by an
# ignore.
engine_src="target/engine-deny-src"
rm -rf "$engine_src"
bash scripts/engine-source.sh prepare "$engine_src" >/dev/null
cp third_party/kafka-backup-deny.toml "$engine_src/deny.toml"
cargo deny --locked --manifest-path "$engine_src/Cargo.toml" check licenses advisories sources bans

# FX-21 (review L2): an engine patch's own oracle runs here, over the same
# prepared tree, so a later patch or pin move that regresses it fails before an
# image is built (the e2e rows that show them live need a slot and are ignored
# in CI). Patch 0002's: the manifest merge, and the backup loop's save order,
# keep every topic's replication factor. Patch 0003's (PROD-00.3g): the restore
# checkpoint's hash leaves out the run's paths and map order, and a due
# checkpoint is saved. It builds in the debug profile of target/engine-build,
# beside `scripts/engine-source.sh build`'s optimised one (this script never
# builds an optimised profile).
engine_tests="target/engine-patch-oracle.log"
CARGO_TARGET_DIR="$PWD/target/engine-build" cargo test --locked \
  --manifest-path "$engine_src/Cargo.toml" -p kafka-backup-core --lib \
  -- merge_manifests manifest_persistence restore_config_hash save_checkpoint_if_due \
  > "$engine_tests" 2>&1 \
  || { cat "$engine_tests" >&2; echo "ci-check: an engine patch's oracle failed" >&2; exit 1; }
for oracle in backup::engine::tests::test_merge_manifests_updates_replication_factor \
              backup::engine::tests::test_merge_manifests_preserves_replication_factor_when_none \
              backup::engine::tests::test_manifest_persistence_keeps_every_topics_replication_factor \
              restore::engine::tests::restore_config_hash_ignores_the_checkpoint_and_report_paths \
              restore::engine::tests::restore_config_hash_does_not_depend_on_map_iteration_order \
              restore::helpers::tests::save_checkpoint_if_due_honours_the_interval; do
  grep -q "^test $oracle \.\.\. ok$" "$engine_tests" \
    || { cat "$engine_tests" >&2; echo "ci-check: the engine patch oracle $oracle did not run and pass" >&2; exit 1; }
done
}

usage() {
echo "usage: scripts/ci-check.sh [lint | unit <group> | unit-<group> | generated | supply-chain | python | docs]" >&2
exit 2
}

part="${1:-}"
# `unit-core` is `unit core`: the gate's name.
if [[ "$part" == unit-* && "$#" -eq 1 ]]; then
  set -- unit "${part#unit-}"
  part=unit
fi
case "$part" in
  "")
    prepare
    cli
    just lint
    lint_tail
    cosign_pins
    cargo test --locked --workspace
    python_rows
    generated
    just links
    supply_chain
    ;;
  unit)
    [ "$#" -eq 2 ] || usage
    unit_packages "$2" > /dev/null
    prepare
    unit "$2"
    ;;
  lint | generated | supply-chain | python | docs)
    [ "$#" -eq 1 ] || usage
    prepare
    case "$part" in
      lint) lint ;;
      generated) generated ;;
      supply-chain) supply_chain ;;
      python) python_rows ;;
      docs) docs ;;
    esac
    ;;
  *) usage ;;
esac
