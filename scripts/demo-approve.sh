#!/usr/bin/env bash
# The APPROVER's half of the two-key flow. In a real deployment this runs on the
# approver's own machine, with the approver's own key, and only approval.json
# and approval.sig cross the boundary.
#
# Requires: bash, a `logweir` binary (built here if $LOGWEIR_BIN is unset), and
# .demo/approver.pem (minted by scripts/demo.sh). No `jq` and no `shasum`: the
# hashing and the signing are `logweir drill approve`'s job, which is the only
# way an operator with nothing but the release artifact can do this at all.
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

# `logweir drill approve` — the SHIPPED command, not a cargo example. It hashes
# the spec, builds the approval document and writes the DSSE sidecar beside it
# at `.sig`, which is the only path `drill run` looks for. The sidecar is DSSE:
# the signature covers PAE(payloadType, payload), never the bare bytes, so
# `logweir` and the Python verifier agree on what was signed.
LOGWEIR_BIN="${LOGWEIR_BIN:-}"
if [ -z "$LOGWEIR_BIN" ]; then
  cargo build --release -p logweir
  LOGWEIR_BIN="target/release/logweir"
fi

"$LOGWEIR_BIN" drill approve \
  --spec "$SPEC" \
  --key .demo/approver.pem \
  --approver demo@example.com \
  --ticket DEMO-1 \
  --out .demo/approval.json
