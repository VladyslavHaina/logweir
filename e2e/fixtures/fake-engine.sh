#!/usr/bin/env bash
# Stand-in for the pinned upstream `kafka-backup` binary. Task 13 extracts the
# real one from the digest-pinned image; Docker's daemon proxy is broken on
# this machine, so the real binary does not exist here and this script is what
# crates/logweir-engine-oso's tests run against instead (see task-12-report.md
# "what remains unproven without the real engine binary").
#
# Reproduces the two argv shapes Task 12 builds:
#   fake-engine.sh validate-restore --config <path> --format json
#   fake-engine.sh restore --config <path>
# and is driven by a handful of FAKE_ENGINE_* environment variables so one
# script can stand in for every shape task-12-brief.md documents (a clean
# report, an invalid report, a malformed one, a warning on stdout vs stderr, a
# chosen exit code) instead of one fixture file per shape.
#
# With NO environment variables set, `validate-restore` reproduces EXACTLY the
# fixture task-12-brief.md Step 1 specifies: a warning on STDERR for
# `restore.header_preflight`, an invalid DryRunReport on stdout, exit 1. That
# default is deliberate — `tests/subprocess.rs`'s brief-literal tests invoke
# this script with `--config /dev/null` and expect exactly that output, with
# none of these variables set.
set -uo pipefail

cmd="${1:-}"
shift || true

case "$cmd" in
validate-restore | restore) ;;
*)
  echo "fake-engine: subcommand '$cmd' is outside {restore, validate-restore} — this stub only implements those two" >&2
  exit 64
  ;;
esac

# Pull the value that follows --config out of the remaining args (no reliance
# on argv order beyond "the value right after the flag").
config_path=""
prev=""
for a in "$@"; do
  if [[ "$prev" == "--config" ]]; then
    config_path="$a"
  fi
  prev="$a"
done

# Opt-in only (FAKE_ENGINE_CHECK_CONFIG=1): proves the argv wiring end to end
# for callers that go through OsoCliEngine — not just that `--config` appears,
# but that its VALUE resolves to the restore.yaml OsoCliEngine just rendered
# and wrote, by checking for the marker line render_restore::render always
# writes. Off by default because tests/subprocess.rs's brief-literal tests
# call this script directly with `--config /dev/null`, which carries no such
# marker.
if [[ "${FAKE_ENGINE_CHECK_CONFIG:-0}" == "1" ]]; then
  if [[ -z "$config_path" ]]; then
    echo "fake-engine: no --config value found in argv: $*" >&2
    exit 65
  fi
  if [[ ! -f "$config_path" ]]; then
    echo "fake-engine: --config path '$config_path' does not exist" >&2
    exit 66
  fi
  if ! grep -q '^mode: restore$' "$config_path"; then
    echo "fake-engine: --config path '$config_path' does not look like a rendered restore.yaml (no 'mode: restore' line)" >&2
    exit 67
  fi
fi

# Unset means "use the default below"; explicitly set to the empty string
# means "emit no warning at all" (${VAR-default} only substitutes when VAR is
# UNSET, not when it is set-but-empty, unlike ${VAR:-default}).
warn_key="${FAKE_ENGINE_WARN_KEY-restore.header_preflight}"
if [[ -n "$warn_key" ]]; then
  line="WARN kafka_backup: Ignoring unknown config key \`${warn_key}\` — check for typos"
  if [[ "${FAKE_ENGINE_WARN_STREAM:-stderr}" == "stdout" ]]; then
    echo "$line"
  else
    echo "$line" >&2
  fi
fi

if [[ "$cmd" == "restore" ]]; then
  echo "INFO kafka_backup: Starting restore from backup: fake-backup"
  exit "${FAKE_ENGINE_EXIT:-0}"
fi

# cmd == validate-restore
if [[ "${FAKE_ENGINE_MALFORMED:-0}" == "1" ]]; then
  echo "not a json object, no opening brace anywhere on this stream"
  exit "${FAKE_ENGINE_EXIT:-1}"
fi

if [[ "${FAKE_ENGINE_VALID:-0}" == "1" ]]; then
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
    "partitions": [],
    "consumer_group_snapshot": null,
    "records_scanned_total": 150,
    "passed": true,
    "errors": [],
    "warnings": []
  }
}
JSON
  exit "${FAKE_ENGINE_EXIT:-0}"
fi

# Default (no FAKE_ENGINE_* set): task-12-brief.md Step 1's exact fixture body.
cat <<'JSON'
{
  "backup_id": "backup-2026-08-30T02:00:00Z",
  "valid": false,
  "errors": ["segment drills/.../000000000100.kbak is missing from storage"],
  "warnings": [],
  "segments_to_process": 4,
  "records_to_restore": 41822,
  "bytes_to_restore": 1048576,
  "time_range": [1756425600000, 1756519200000],
  "topics_to_restore": [
    {"source_topic": "orders", "target_topic": "drill-20260903-orders", "partitions": []}
  ],
  "consumer_offset_actions": []
}
JSON
exit "${FAKE_ENGINE_EXIT:-1}"
