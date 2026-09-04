#!/usr/bin/env bash
# Deterministic stub that proves the argv wiring end to end: not merely that
# `--config <path>` appears on the command line, but that <path> resolves to
# the ACTUAL restore.yaml OsoCliEngine just rendered and wrote to its workdir
# — by checking for the marker line render_restore::render always writes
# first (`mode: restore`). Exits 0 with a recognisable marker on success (also
# emitting a minimal clean DryRunReport for validate-restore, since
# OsoCliEngine::preflight parses stdout as JSON regardless of which stub wrote
# it); exits 42 if the config file is missing or unrecognisable, so a broken
# argv (wrong flag, wrong value, config never written) fails loudly rather
# than silently passing.
set -uo pipefail
cmd="${1:-}"
shift || true

config_path=""
prev=""
for a in "$@"; do
  if [[ "$prev" == "--config" ]]; then
    config_path="$a"
  fi
  prev="$a"
done

if [[ -z "$config_path" || ! -f "$config_path" ]]; then
  echo "fake-engine-argv-check: --config missing or unreadable: '$config_path'" >&2
  exit 42
fi
if ! grep -q '^mode: restore$' "$config_path"; then
  echo "fake-engine-argv-check: '$config_path' has no 'mode: restore' marker" >&2
  exit 42
fi

case "$cmd" in
validate-restore)
  cat <<'JSON'
{
  "backup_id": "backup-2026-08-30T02:00:00Z",
  "valid": true,
  "errors": [],
  "warnings": [],
  "segments_to_process": 0,
  "records_to_restore": 0,
  "bytes_to_restore": 0,
  "time_range": null,
  "topics_to_restore": [],
  "consumer_offset_actions": []
}
JSON
  exit 0
  ;;
restore)
  echo "INFO kafka_backup: argv-check passed, config file resolved correctly"
  exit 0
  ;;
*)
  echo "fake-engine-argv-check: unknown subcommand '$cmd'" >&2
  exit 64
  ;;
esac
