#!/usr/bin/env bash
# FX-23. A deterministic stub that writes the engine's OFFSET-MAPPING REPORT the
# way the pinned engine does after a restore that returned `Ok`
# (`kafka-backup-core/src/restore/engine.rs:426-437` and `:1390-1395` in
# 0.23.3: `serde_json::to_string_pretty(&OffsetMapping)`), to the path the
# rendered `restore.offset_report` names, with ONE entry, for target
# partition `drill-20260903-orders/0` -- the shape an engine leaves when it
# finished that topic. Used by `crates/logweir-engine-oso/tests/engine.rs`,
# which reads the report back through `OsoCliEngine::restore`.
#
# validate-restore: the same clean report as fake-engine-clean.sh.
# restore: writes the report, exits 0. Exits 42 when the rendered document
# names no offset_report, so a renderer that stopped rendering it fails loudly.
set -uo pipefail
cmd="${1:-}"
config_path=""
prev=""
for a in "$@"; do
  if [[ "$prev" == "--config" ]]; then
    config_path="$a"
  fi
  prev="$a"
done
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
  report=$(sed -n 's/^  offset_report: //p' "$config_path" | head -1)
  report=${report#\"}
  report=${report%\"}
  report=${report#\'}
  report=${report%\'}
  if [[ -z "$report" ]]; then
    echo "fake-engine-offset-report: the rendered document names no offset_report" >&2
    exit 42
  fi
  cat >"$report" <<'JSON'
{
  "entries": {
    "drill-20260903-orders/0": {
      "topic": "drill-20260903-orders",
      "partition": 0,
      "source_first_offset": 0,
      "source_last_offset": 149,
      "target_first_offset": null,
      "target_last_offset": null,
      "first_timestamp": 1787961600000,
      "last_timestamp": 1788055200000
    }
  },
  "detailed_mappings": {},
  "consumer_groups": {},
  "source_cluster_id": null,
  "target_cluster_id": null,
  "created_at": 1788055300000
}
JSON
  echo "INFO kafka_backup: restore complete"
  exit 0
  ;;
*)
  echo "fake-engine-offset-report: unknown subcommand '$cmd'" >&2
  exit 64
  ;;
esac
