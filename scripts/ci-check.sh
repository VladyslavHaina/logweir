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
just verify-py

just schema-check
just crds-check
just chart-check
bash scripts/render-install.sh --check
just links

mkdir -p target
bash scripts/gen-third-party-notices.sh > target/tpn.check
diff -u THIRD_PARTY_NOTICES.md target/tpn.check
cargo deny check licenses advisories
