#!/usr/bin/env bash
# Deterministic stub proving `OsoCliEngine::assert_no_dropped_logweir_key`:
# otherwise a valid, clean dry run (so nothing else about the report would
# fail preflight()), except it warns about `restore.dry_run_check_segments` —
# a key `render_restore::render` ALWAYS renders (`dry_run_check_segments:
# true` is unconditional, never optional) — on stdout, exactly where the real
# engine puts it. An engine tagged below the declared floor that silently
# ignores this key must abort the run rather than report a false "clean"
# preflight.
set -uo pipefail
cmd="${1:-}"
case "$cmd" in
validate-restore)
  echo "WARN kafka_backup: Ignoring unknown config key \`restore.dry_run_check_segments\` — check for typos"
  cat <<'JSON'
{
  "backup_id": "backup-2026-08-30T02:00:00Z",
  "valid": true,
  "errors": [],
  "warnings": [],
  "segments_to_process": 2,
  "records_to_restore": 150,
  "bytes_to_restore": 15360,
  "time_range": [1619000000000, 1619000010000],
  "topics_to_restore": [
    {"source_topic": "orders", "target_topic": "drill-20260903-orders", "partitions": []}
  ],
  "consumer_offset_actions": []
}
JSON
  exit 0
  ;;
*)
  echo "fake-engine-dropped-key: unknown subcommand '$cmd'" >&2
  exit 64
  ;;
esac
