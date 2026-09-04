#!/usr/bin/env bash
# Deterministic (no env-var branching, unlike fake-engine.sh) stub used by
# crates/logweir-engine-oso/tests/engine.rs, which drives OsoCliEngine through
# `subprocess::run_engine` — a fixed API with no way to pass extra environment
# variables per call, so an env-var-driven fixture would need this test
# process's OWN environment mutated, racing every other test in the same file.
# A dedicated, hardcoded-behaviour script sidesteps that entirely.
#
# validate-restore: a clean, valid dry run with header_preflight fully
# honoured (scan_performed: true, mode: "full") and no unknown-key warnings.
# restore: succeeds, exit 0.
set -uo pipefail
cmd="${1:-}"
case "$cmd" in
validate-restore)
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
  "consumer_offset_actions": [],
  "header_preflight": {
    "backup_id": "backup-2026-08-30T02:00:00Z",
    "generated_at": "2026-08-30T02:05:00Z",
    "mode": "full",
    "offset_recovery_requested": true,
    "scan_performed": true,
    "partitions": [
      {
        "topic": "orders",
        "partition": 0,
        "state": "full",
        "segments_selected": 2,
        "segments_scanned": 2,
        "segments_missing": 0,
        "segments_corrupt": 0,
        "segments_unreadable": 0,
        "segments_legacy_format": 0,
        "manifest_record_count": 150,
        "records_scanned": 150,
        "records_with_offset_header": 150,
        "records_with_timestamp_header": 150,
        "records_with_source_cluster_header": 150,
        "records_with_required_headers": 150,
        "problems": []
      }
    ],
    "consumer_group_snapshot": null,
    "records_scanned_total": 150,
    "passed": true,
    "errors": [],
    "warnings": []
  }
}
JSON
  exit 0
  ;;
restore)
  echo "INFO kafka_backup: Starting restore from backup: backup-2026-08-30T02:00:00Z"
  echo "INFO kafka_backup: restore complete"
  exit 0
  ;;
*)
  echo "fake-engine-clean: unknown subcommand '$cmd'" >&2
  exit 64
  ;;
esac
