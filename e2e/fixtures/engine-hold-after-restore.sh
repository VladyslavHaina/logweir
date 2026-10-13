#!/usr/bin/env bash
# PROD-04.2 C11, a NEGATIVE CONTROL that cannot race the runner: the real
# engine, and once its `restore` has returned -- every restored record
# written, phase 7 not started, because the runner waits for this process --
# a HOLD. It creates `$LOGWEIR_E2E_HOLD_DIR/restored` and waits for
# `$LOGWEIR_E2E_HOLD_DIR/planted` before exiting with the engine's own status.
# The test plants one record in the restored topic in between, so phase 7's
# complete verification always finds a record the archive does not explain
# (`integrity.verification.complete.replay.unexpected` > 0), however fast the
# runner is. Every other subcommand, and a failed restore, passes through.
#
# LOGWEIR_E2E_REAL_ENGINE names the real engine (the harness's `engine_bin()`,
# native or the docker shim). Used by `e2e/tests/switchover.rs::
# only_a_passing_complete_restore_maps_positions`.
set -euo pipefail
real="${LOGWEIR_E2E_REAL_ENGINE:?LOGWEIR_E2E_REAL_ENGINE names the real engine}"
hold="${LOGWEIR_E2E_HOLD_DIR:?LOGWEIR_E2E_HOLD_DIR names the hold directory}"
set +e
"$real" "$@"
rc=$?
set -e
if [[ "${1:-}" == "restore" && "$rc" -eq 0 ]]; then
  : > "$hold/restored"
  for _ in $(seq 1 1200); do
    if [[ -e "$hold/planted" ]]; then
      exit "$rc"
    fi
    sleep 0.1
  done
  echo "engine-hold-after-restore: nothing was planted within 120 s" >&2
  exit 1
fi
exit "$rc"
