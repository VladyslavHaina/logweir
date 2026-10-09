#!/usr/bin/env bash
# PROD-11.1. A deterministic stub for a restore made of several ENGINE RUNS
# (one per distinct partition subset). Every invocation appends one line to
# `engine-runs.log` beside the rendered document:
#   <subcommand> <config file name> <source_partitions value or "-"> <target topics>
# so a test can read back which documents were validated and restored, in
# what order. `restore` then writes the pinned engine's offset-report shape
# to the rendered `offset_report` path, one entry per target topic and
# partition (the run's `source_partitions`, or partition 0 without one).
# A document naming a target topic that contains `fail-me` exits 1 on
# `restore`, so a test can fail one run of several.
# Used by `crates/logweir-engine-oso/tests/engine_runs.rs`.
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
dir=$(dirname "$config_path")
name=$(basename "$config_path")
parts=$(sed -n 's/^  source_partitions: \[\(.*\)\]$/\1/p' "$config_path" | head -1)
targets=$(awk '/^  topic_mapping:$/{m=1;next} m&&/^    /{sub(/^    [^:]*: */,""); gsub(/"/,""); print; next} m{exit}' "$config_path" | tr '\n' ' ')
echo "$cmd $name ${parts:--} $targets" >>"$dir/engine-runs.log"
case "$cmd" in
validate-restore)
  cat <<'JSON'
{
  "backup_id": "b",
  "valid": true,
  "errors": [],
  "warnings": [],
  "segments_to_process": 1,
  "records_to_restore": 10,
  "bytes_to_restore": 100,
  "time_range": [1, 2],
  "topics_to_restore": [],
  "consumer_offset_actions": [],
  "header_preflight": {
    "backup_id": "b",
    "generated_at": "2026-08-30T02:05:00Z",
    "mode": "full",
    "offset_recovery_requested": true,
    "scan_performed": true,
    "partitions": [],
    "consumer_group_snapshot": null,
    "records_scanned_total": 0,
    "passed": true,
    "errors": [],
    "warnings": []
  }
}
JSON
  exit 0
  ;;
restore)
  case "$targets" in
  *fail-me*)
    echo "fake-engine-runs: failing $name on purpose" >&2
    exit 1
    ;;
  esac
  report=$(sed -n 's/^  offset_report: //p' "$config_path" | head -1)
  report=${report#\"}
  report=${report%\"}
  {
    echo '{"entries": {'
    first=1
    for t in $targets; do
      for p in $(echo "${parts:-0}" | tr ',' ' '); do
        if [[ $first -eq 0 ]]; then echo ','; fi
        first=0
        echo "\"$t/$p\": {\"topic\": \"$t\", \"partition\": $p, \"source_first_offset\": 0, \"source_last_offset\": 1, \"target_first_offset\": null, \"target_last_offset\": null, \"first_timestamp\": 1, \"last_timestamp\": 2}"
      done
    done
    echo '}, "detailed_mappings": {}, "consumer_groups": {}, "source_cluster_id": null, "target_cluster_id": null, "created_at": 3}'
  } >"$report"
  exit 0
  ;;
*)
  echo "fake-engine-runs: unknown subcommand '$cmd'" >&2
  exit 64
  ;;
esac
