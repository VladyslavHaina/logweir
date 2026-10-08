#!/usr/bin/env bash
# Shared local/CI quality check. Requires Rust, just, Node >= 20, Helm >= 4,
# kubectl, cargo-deny, Docker, and Python with cryptography and pytest.
# No running broker or Kubernetes cluster is needed.
set -euo pipefail
cd "$(dirname "$0")/.."

cargo fetch --locked
bash scripts/extract-engine.sh

# Build the current CLI once for verifier parity and UI plan/hash behavior.
# Explicitly select debug so these checks never depend on a stale release binary.
cargo build --locked -p logweir
export LOGWEIR_BIN="$PWD/${CARGO_TARGET_DIR:-target}/debug/logweir"
if [[ "${CARGO_TARGET_DIR:-target}" = /* ]]; then
  export LOGWEIR_BIN="$CARGO_TARGET_DIR/debug/logweir"
fi
just lint
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

cargo test --locked --workspace
python3 scripts/test-ci-images.py
# PROD-00.2 security review: every cosign / gh attestation verification in the
# repository pins the exact signer identity, the calling repository, ref and
# trigger, and the issuer (images.yml is a reusable workflow).
python3 scripts/check-cosign-verify.py
python3 scripts/test-check-cosign-verify.py
python3 scripts/test-release.py
just verify-py

just schema-check
just crds-check
just chart-check
bash scripts/render-install.sh --check
just links

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
# image is built (the e2e row that shows it live needs `cluster3` and is
# ignored in CI). Patch 0002's: the manifest merge, and the backup loop's save
# order, keep every topic's replication factor. The release profile shares
# target/engine-build's dependencies with `scripts/engine-source.sh build`.
engine_tests="target/engine-patch-oracle.log"
CARGO_TARGET_DIR="$PWD/target/engine-build" cargo test --locked --release \
  --manifest-path "$engine_src/Cargo.toml" -p kafka-backup-core --lib \
  -- merge_manifests manifest_persistence > "$engine_tests" 2>&1 \
  || { cat "$engine_tests" >&2; echo "ci-check: an engine patch's oracle failed" >&2; exit 1; }
for oracle in test_merge_manifests_updates_replication_factor \
              test_merge_manifests_preserves_replication_factor_when_none \
              test_manifest_persistence_keeps_every_topics_replication_factor; do
  grep -q "^test backup::engine::tests::$oracle \.\.\. ok$" "$engine_tests" \
    || { cat "$engine_tests" >&2; echo "ci-check: patch 0002's oracle $oracle did not run and pass" >&2; exit 1; }
done
