#!/usr/bin/env bash
# The artifact-directory ceiling (Task 5b, addendum A-census).
#
# WHY THIS EXISTS. Five tasks of mutation rounds built into the shared
# `target/`, and nothing ever cleaned it. `target/debug/deps` reached
# **873,349 files / 43.5 GiB**, and at that size cargo's own fingerprint scan
# of the directory cost ~30 s PER TEST BINARY: `cargo test --workspace` took
# twenty minutes sitting at 0% CPU, and was twice mistaken for a hang. The
# pure-core test binary that cargo took 30 s over runs its 51 tests in 17 ms
# when executed directly. The cost was never in the tests.
#
# It is a DETECTION, not the prevention. The prevention is the throwaway
# target dir every mutation round must use:
#
#     CARGO_TARGET_DIR=target/mutants cargo test ...   # then rm -rf target/mutants
#
# which `just mutant` wraps. This gate catches the round that forgot.
#
# THE CEILING is 50,000 files, measured rather than guessed (addendum A-census):
#
#   | state                                   | deps files | target size |
#   |-----------------------------------------|------------|-------------|
#   | fresh: clean + build + test --no-run    |      5,608 |     2.3 GiB |
#   | after one task (mutants, release, e2e)  |     10,026 |     4.4 GiB |
#   | five tasks, never cleaned (the defect)  |    873,349 |    43.5 GiB |
#
# 50,000 is five times the one-task figure and one-seventeenth of the
# pathological one. It is deliberately loose: the job is to catch the
# runaway regime early, not to police normal growth.
#
# THE COUNT IS PRINTED ON EVERY RUN, pass or fail, so the trend is visible
# long before the ceiling is reached.
#
# Every exit code is read on its own line, never through a pipe: `cmd | wc`
# reports wc's status, and a gate whose failure is invisible is worse than no
# gate.
set -euo pipefail

cd "$(dirname "$0")/.."

DEPS="target/debug/deps"
CEILING="${LOGWEIR_DEPS_FILE_CEILING:-50000}"

case "$CEILING" in
    '' | *[!0-9]*)
        echo "check-deps-count: \$LOGWEIR_DEPS_FILE_CEILING must be a whole number, got '$CEILING'" >&2
        exit 1
        ;;
esac

if [ ! -d "$DEPS" ]; then
    # Nothing built yet is not a failure: `just lint` runs `cargo clippy`
    # first, which populates it, but a bare `./scripts/check-deps-count.sh`
    # on a freshly cloned tree must not fail for having nothing to count.
    echo "check-deps-count: $DEPS does not exist yet (nothing built) — 0 files, ceiling $CEILING"
    exit 0
fi

# `ls -f` does not stat or sort, which matters at the scale this gate exists
# to catch — `find` on an 873k-entry directory took minutes. The two extra
# lines are `.` and `..`, which `ls -f` lists and which are not artifacts.
set +e
raw="$(ls -f "$DEPS" | wc -l)"
count_rc=$?
set -e
if [ "$count_rc" -ne 0 ]; then
    echo "check-deps-count: could not count $DEPS (exit $count_rc)" >&2
    exit 1
fi
count=$((raw - 2))
[ "$count" -ge 0 ] || count=0

if [ "$count" -gt "$CEILING" ]; then
    echo "check-deps-count: $DEPS holds $count files, over the $CEILING ceiling." >&2
    echo "" >&2
    echo "This is the twenty-minute-test-suite defect, caught early. At this scale" >&2
    echo "cargo spends its time fingerprinting the artifact directory, ~30 s per test" >&2
    echo "binary, at 0% CPU — the suite looks hung and is merely scanning." >&2
    echo "" >&2
    echo "Fix it:      cargo clean" >&2
    echo "Prevent it:  run mutation rounds under a throwaway target dir —" >&2
    echo "             just mutant '<cargo args>'   (CARGO_TARGET_DIR=target/mutants)" >&2
    echo "             and rm -rf target/mutants when the round is done." >&2
    exit 1
fi

echo "check-deps-count: $DEPS holds $count files (ceiling $CEILING)"
