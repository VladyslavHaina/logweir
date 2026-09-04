#!/usr/bin/env bash
# Deterministic failure-mode stub, sibling to fake-engine-clean.sh (same
# reasoning: no env-var branching, since tests/engine.rs drives this through
# OsoCliEngine -> subprocess::run_engine, which has no per-call env hook).
#
# validate-restore: stdout has no JSON object at all (a malformed run) —
# OsoCliEngine::preflight must report this as "printed no JSON object", not
# attempt to parse it.
# restore: fails with a distinctive non-zero exit code and a stderr message,
# so OsoCliEngine::restore's exit-code-to-Err mapping is provable.
set -uo pipefail
cmd="${1:-}"
case "$cmd" in
validate-restore)
  echo "ERROR kafka_backup: failed to open backup manifest: not found"
  # deliberately no '{' anywhere on stdout
  exit 1
  ;;
restore)
  echo "ERROR kafka_backup: restore failed: target broker unreachable" >&2
  exit 3
  ;;
*)
  echo "fake-engine-fail: unknown subcommand '$cmd'" >&2
  exit 64
  ;;
esac
