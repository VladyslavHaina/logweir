#!/usr/bin/env bash
# Deterministic stub that proves the argv wiring end to end: not merely that
# `--config <path>` appears on the command line, but that <path> resolves to
# the ACTUAL restore.yaml / backup.yaml OsoCliEngine just rendered and wrote to
# its workdir — by checking for the marker line the renderers always write
# first (`mode: restore` or `mode: backup`; both `render_restore::render` and
# `render_backup::render` emit theirs on line 2, after the "Rendered by
# logweir" comment). Exits 0 with a recognisable marker on success (also
# emitting a minimal clean DryRunReport for validate-restore, since
# OsoCliEngine::preflight parses stdout as JSON regardless of which stub wrote
# it); exits 42 if the config file is missing or unrecognisable, so a broken
# argv (wrong flag, wrong value, config never written) fails loudly rather
# than silently passing.
set -uo pipefail

# Task 4: the argv RECORDER. Every element, one per line, in order, appended to
# the file named by LOGWEIR_ARGV_LOG when that variable is set — captured
# BEFORE the `shift` below, so the subcommand is in the record too. This is how
# `e2e/tests/backup_argv.rs` reads the argv the engine was actually spawned
# with: asserting on a string the test itself built would prove nothing about
# the array `run_engine` passed to `Command::args`. Unset variable => no file
# and no behaviour change, so every existing use of this stub is unaffected.
if [[ -n "${LOGWEIR_ARGV_LOG:-}" ]]; then
  for a in "$@"; do
    printf '%s\n' "$a" >>"$LOGWEIR_ARGV_LOG"
  done
fi

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
if ! grep -qE '^mode: (restore|backup)$' "$config_path"; then
  echo "fake-engine-argv-check: '$config_path' has no 'mode: restore' or 'mode: backup' marker" >&2
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
backup)
  # No archive is written: this stub proves the ARGV, and the archive the
  # phase reads back afterwards is the one `scripts/e2e-seed.sh` already put in
  # MinIO. A stub that fabricated a manifest would be testing itself.
  echo "INFO kafka_backup: argv-check passed for backup, config file resolved correctly"
  exit 0
  ;;
*)
  echo "fake-engine-argv-check: unknown subcommand '$cmd'" >&2
  exit 64
  ;;
esac
