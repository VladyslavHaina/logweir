#!/usr/bin/env bash
# Classify one engine-matrix row from what its steps actually did.
#
# `.github/workflows/engine-matrix.yml`'s "Record this row" step calls this with
# the step outcomes in the environment and writes the row from its two output
# lines:
#
#     outcome=<one of the outcomes docs/support-matrix.md defines>
#     reason=<one line of evidence, no `|`>
#
# Inputs (environment; a step that did not run reads as empty or `skipped`):
#   FLOOR           full | below
#   DIGEST          the resolved image digest, empty when the tag did not resolve
#   UP              outcome of `just e2e-up`
#   KAFKA_DECLARED  the broker version the row declares
#   BROKER_VERSION  the "Kafka version" the running broker logged (read back)
#   SEED BUILD FULL REDUCED CONTROL   step outcomes
#   REDUCED_LOG CONTROL_LOG           transcripts of the reduced row and control
#   RETENTION_DELETIONS RETENTION_TOPICS
#                   what the broker's time-retention check deleted during the
#                   run (scripts/engine-matrix-broker.sh --retention). A failed
#                   suite's reason names it; it never changes an outcome
#                   (run 36542777892, the engine route decision record 5.6).
#
# Every outcome is derived from what ran; nothing is assumed from the floor.
# The previous inline classifier recorded `unsupported(lever-absent)` for a
# below-floor row whatever its seed, reduced row and control did, and
# `fail(lever-not-honoured)` for a full row whose control had merely been
# skipped (a build failure): review M1 of PROD-00.1. A below-floor row is
# `unsupported(lever-absent)` only when Logweir was SEEN refusing the engine
# in both drills (the refusal text crates/logweir-engine-oso/src/engine.rs
# prints); a below-floor engine that a drill accepted is
# `fail(floor-not-enforced)`.
# crates/logweir/tests/engine_matrix.rs drives this table-first.
set -u

REFUSAL="below the declared floor"

emit() {
  printf 'outcome=%s\nreason=%s\n' "$1" "$2"
  exit 0
}

refused_in() {  # the log file holds Logweir's floor refusal
  [ -n "${1:-}" ] && [ -f "$1" ] && grep -qF "$REFUSAL" "$1"
}

FLOOR=${FLOOR:-}
DIGEST=${DIGEST:-}
UP=${UP:-}
KAFKA_DECLARED=${KAFKA_DECLARED:-}
BROKER_VERSION=${BROKER_VERSION:-}
SEED=${SEED:-}
BUILD=${BUILD:-}
FULL=${FULL:-}
REDUCED=${REDUCED:-}
CONTROL=${CONTROL:-}
RETENTION_DELETIONS=${RETENTION_DELETIONS:-}

# --- setup: what the row ran on --------------------------------------------
[ -n "$DIGEST" ] ||
  emit "fail(setup)" "the tag did not resolve to a digest whose revision is the tag's commit"
[ "$UP" = "success" ] ||
  emit "fail(setup)" "the compose stack did not come up (just e2e-up: ${UP:-not run})"
[ -n "$BROKER_VERSION" ] ||
  emit "fail(setup)" "the running broker's Kafka version could not be read back"
[ "$BROKER_VERSION" = "$KAFKA_DECLARED" ] ||
  emit "fail(setup)" "the stack ran Kafka ${BROKER_VERSION}, but the row declares ${KAFKA_DECLARED}"
[ "$SEED" = "success" ] ||
  emit "fail(seed)" "the seed did not produce a verifiable archive with this engine (seed step: ${SEED:-not run})"
[ "$BUILD" = "success" ] ||
  emit "fail(build)" "cargo build of logweir did not succeed (build step: ${BUILD:-not run})"

case "$FLOOR" in
  below)
    if [ "$REDUCED" = "success" ] || [ "$CONTROL" = "success" ]; then
      emit "fail(floor-not-enforced)" "a drill accepted an engine below the 0.21.0 full-drill floor (reduced row ${REDUCED:-not run}, control ${CONTROL:-not run})"
    fi
    if [ "$REDUCED" != "failure" ] || [ "$CONTROL" != "failure" ]; then
      emit "fail(setup)" "the reduced row or the control did not run (reduced row ${REDUCED:-not run}, control ${CONTROL:-not run})"
    fi
    if ! refused_in "${REDUCED_LOG:-}" || ! refused_in "${CONTROL_LOG:-}"; then
      emit "fail(floor-not-enforced)" "the reduced row and the control failed, but not with Logweir's floor refusal ('${REFUSAL}')"
    fi
    emit "unsupported(lever-absent)" "below the 0.21.0 full-drill floor: Logweir refused the engine in the reduced row and the control ('${REFUSAL}')"
    ;;
  full)
    [ "$CONTROL" != "failure" ] ||
      emit "fail(lever-not-honoured)" "the deleted-segment positive control did not block the restore at preflight"
    [ "$CONTROL" = "success" ] ||
      emit "fail(setup)" "the deleted-segment positive control did not run (control: ${CONTROL:-not run})"
    case "$FULL" in
      success) emit "pass" "full e2e suite and the deleted-segment positive control passed" ;;
      failure)
        case "$RETENTION_DELETIONS" in
          ''|0|*[!0-9]*) ;;
          *) emit "fail(e2e suite)" "the full e2e suite failed; during the run the broker's time-retention check deleted ${RETENTION_DELETIONS} segment(s) of ${RETENTION_TOPICS:-unnamed topics} (a fixture stamped older than the retention loses its records so; engine route decision record 5.6); see the job log" ;;
        esac
        emit "fail(e2e suite)" "the full e2e suite failed; see the job log" ;;
      *) emit "fail(setup)" "the full e2e suite did not run (full: ${FULL:-not run})" ;;
    esac
    ;;
  *)
    emit "fail(setup)" "unknown floor '${FLOOR}'"
    ;;
esac
