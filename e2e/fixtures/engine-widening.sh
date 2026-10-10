#!/usr/bin/env bash
# PROD-11.1b, a NEGATIVE CONTROL: a faulty engine that ignores the partition
# filter. It deletes the `source_partitions` line from the document Logweir
# rendered (after Logweir has checked and hashed it) and then runs the real
# engine with the same arguments, so every partition of every topic in the run
# is restored, the unselected ones included. A Logweir that judged a
# partition-subset restore by the engine's exit status would sign it `pass`;
# phase 7 must fail it on both lanes.
#
# LOGWEIR_E2E_REAL_ENGINE names the real engine (the harness's `engine_bin()`,
# native or the docker shim). Used by `e2e/tests/replay_selection.rs::
# a_record_in_an_unselected_partition_fails_the_restore`.
set -euo pipefail
real="${LOGWEIR_E2E_REAL_ENGINE:?LOGWEIR_E2E_REAL_ENGINE names the real engine}"
prev=""
for a in "$@"; do
  if [[ "$prev" == "--config" && -f "$a" ]]; then
    # BSD and GNU sed both take `-i.bak`.
    sed -i.bak '/^  source_partitions:/d' "$a"
    rm -f "$a.bak"
  fi
  prev="$a"
done
exec "$real" "$@"
