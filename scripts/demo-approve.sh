#!/usr/bin/env bash
# The APPROVER's half of the two-key flow. In a real deployment this runs on the
# approver's own machine, with the approver's own key, and only approval.json
# and approval.sig cross the boundary.
#
# Requires: bash, jq, cargo, and .demo/approver.pem (minted by scripts/demo.sh).
#
# Usage: demo-approve.sh [spec.yaml]   (default: .demo/drill.yaml)
#
# The spec path is an ARGUMENT because the approval binds to the sha256 of the
# exact bytes `logweir drill run --spec` will read. `scripts/demo.sh` rebinds
# `sample.window_*` into `.demo/drill.yaml`, so hashing `examples/drill.yaml`
# here would approve a document the drill never runs — which phase 1 refuses
# with exit 3, correctly, and which would look like a tooling bug.
set -euo pipefail
cd "$(dirname "$0")/.."

SPEC="${1:-.demo/drill.yaml}"
[ -f "$SPEC" ] || { echo "demo-approve: no such spec: $SPEC" >&2; exit 1; }
PLAN_HASH="sha256:$(shasum -a 256 "$SPEC" | cut -d' ' -f1)"
jq -n --arg h "$PLAN_HASH" \
      --arg a "demo@example.com" \
      --arg t "DEMO-1" \
      '{approver:$a, ticket:$t, plan_hash:$h, approved_at:(now|todate)}' \
  > .demo/approval.json

# The sidecar is DSSE: the signature covers PAE(payloadType, payload), never the
# bare bytes, so `logweir` and the Python verifier agree on what was signed.
cargo run --release -p logweir-evidence --example sign_approval \
  --  .demo/approver.pem .demo/approval.json .demo/approval.sig

echo "approved $SPEC plan_hash=$PLAN_HASH"
