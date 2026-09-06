#!/usr/bin/env bash
# The unit-suite timing harness (Task 5b, brief §5 Step 1a / §7 check 1).
#
# TWO BOUNDS, both enforced here:
#
#   * the WHOLE default suite, `cargo test --workspace`, under
#     $LOGWEIR_UNIT_SUITE_BUDGET_SECS (default 120); and
#   * NO SINGLE TEST over $LOGWEIR_UNIT_TEST_BUDGET_SECS (default 5).
#
# The second bound is what actually catches a dialer. A test that waits out
# `rdkafka_reader.rs`'s 20 s `const T`, or object_store's unconfigured retry
# budget, blows it on its own even while the total stays green.
#
# HOW THE PER-TEST BOUND IS PROVED WITHOUT TIMING 500 TESTS. Each test binary
# is run once with `--test-threads=1`. A binary whose SERIAL wall clock is
# under the per-test budget cannot contain a test over it — that is arithmetic,
# not a sample. Only a binary that exceeds the budget is drilled into, test by
# test with `--exact`, and only then can a single test be named as the
# offender. So the common case costs one extra serial pass and the expensive
# case is the one that has something to report.
#
# IT REFUSES TO RUN AGAINST A LIVE STACK (mutant M10). A timing number taken
# with a broker on 9092 or MinIO on 9000 answering is worse than no number: it
# measures the machine that is not the one this bound is about. `cargo test`
# with the stack DOWN is the run every agent does dozens of times a day, and
# it is the run that was twice mistaken for a hang.
#
# Every exit code is read into a variable on its own line, never through a
# pipe.
set -uo pipefail

cd "$(dirname "$0")/.."

SUITE_BUDGET="${LOGWEIR_UNIT_SUITE_BUDGET_SECS:-120}"
TEST_BUDGET="${LOGWEIR_UNIT_TEST_BUDGET_SECS:-5}"

# THE COMMAND UNDER TEST, as one string, so it can be printed and checked
# (mutant M9: a harness that quietly times `cargo test -p logweir-core`
# instead of the workspace reports a number about a suite nobody runs).
SUITE_CMD="cargo test --workspace"
case "$SUITE_CMD" in
    *--workspace*) : ;;
    *)
        echo "time-unit-suite: the command being timed does not contain --workspace: '$SUITE_CMD'" >&2
        echo "time-unit-suite: this harness exists to bound the WHOLE default suite; a subset number is not that bound." >&2
        exit 1
        ;;
esac

# ---------------------------------------------------------------- clock
# `date +%s` is whole seconds everywhere; python3 gives sub-second resolution
# where it exists (two other gates in scripts/ already need a python3, so this
# is the common case) and is NOT required.
if command -v python3 >/dev/null 2>&1; then
    now() { python3 -c 'import time; print("%.3f" % time.time())'; }
    elapsed() { python3 -c "print('%.2f' % ($2 - $1))" ; }
    over() { python3 -c "import sys; sys.exit(0 if ($1 > $2) else 1)"; }
else
    now() { date +%s; }
    elapsed() { echo $(( $2 - $1 )); }
    over() { [ "$(printf '%.0f' "$1")" -gt "$2" ]; }
fi

# ------------------------------------------------- the stack-down precondition
port_answers() {
    nc -z -w 2 127.0.0.1 "$1" </dev/null >/dev/null 2>&1
    return $?
}

port_answers 9092
kafka_rc=$?
port_answers 9000
minio_rc=$?

if [ "$kafka_rc" -eq 0 ] || [ "$minio_rc" -eq 0 ]; then
    echo "time-unit-suite: REFUSING to time the suite against a live stack." >&2
    [ "$kafka_rc" -eq 0 ] && echo "  127.0.0.1:9092 answers (a broker is up)" >&2
    [ "$minio_rc" -eq 0 ] && echo "  127.0.0.1:9000 answers (MinIO is up)" >&2
    echo "" >&2
    echo "This bound is about the run with NO stack — the one that took ~20 minutes and" >&2
    echo "was twice mistaken for a hang. Run 'just e2e-down' first. (\`just e2e\` is the" >&2
    echo "check that wants the stack UP, and it is a different check.)" >&2
    exit 1
fi
echo "time-unit-suite: the stack is down (9092 and 9000 both refuse)"

# ------------------------------------------------------------------- warm build
# So the timed number is the SUITE, not the compiler.
echo "time-unit-suite: building (cargo build --workspace --tests) so the timing excludes compilation"
cargo build --workspace --tests
build_rc=$?
if [ "$build_rc" -ne 0 ]; then
    echo "time-unit-suite: the build failed (exit $build_rc); there is nothing to time" >&2
    exit "$build_rc"
fi

# ------------------------------------------------------------------ total
echo "time-unit-suite: timing \`$SUITE_CMD\`"
t0="$(now)"
$SUITE_CMD
suite_rc=$?
t1="$(now)"
total="$(elapsed "$t0" "$t1")"

echo ""
echo "time-unit-suite: command      $SUITE_CMD"
echo "time-unit-suite: exit         $suite_rc"
echo "time-unit-suite: elapsed_secs $total"
echo "time-unit-suite: budget_secs  $SUITE_BUDGET"

# ------------------------------------------------- per-binary attribution
# The controller's Step-1 amendment: attribute before you fix. Which binary
# carries the time is the whole diagnosis — in the 20-minute regime EVERY
# binary cost ~30 s regardless of content, which is what identified cargo's
# fingerprint scan rather than any test as the cause.
bins="$(mktemp)"
arts="$(mktemp)"
trap 'rm -f "$bins" "$arts" "$bins.tests"' EXIT
cargo test --workspace --no-run --message-format=json > "$arts" 2>/dev/null
# One TAB-separated `<test binary>\t<package dir>` line per test artifact.
# python3 when it is there (two other gates in scripts/ already need one and
# it can read cargo's JSON properly); a sed over the same lines otherwise,
# which is a shade looser — it can pick up a `[[bin]]` artifact whose target
# also reports `"test":true` — and that only costs an extra harmless row.
if command -v python3 >/dev/null 2>&1; then
    python3 - "$arts" > "$bins" <<'PYEOF'
import json, os, sys
seen = set()
for line in open(sys.argv[1]):
    try:
        m = json.loads(line)
    except ValueError:
        continue
    if m.get("profile", {}).get("test") and m.get("executable"):
        row = (m["executable"], os.path.dirname(m["manifest_path"]))
        if row not in seen:
            seen.add(row)
            print("%s\t%s" % row)
PYEOF
else
    sed -n 's|.*"manifest_path":"\(.*\)/Cargo.toml".*"test":true[^}]*}.*"executable":"\([^"]*\)".*|\2\t\1|p' \
        "$arts" | sort -u > "$bins"
fi
nbins="$(wc -l < "$bins" | tr -d ' ')"

echo ""
echo "time-unit-suite: per test binary, serial (--test-threads=1), $nbins binaries"
slowest_test=""
slowest_test_secs=0
over_budget=0
while IFS="	" read -r exe dir; do
    [ -n "$exe" ] || continue
    [ -x "$exe" ] || continue
    # THE WORKING DIRECTORY IS LOAD-BEARING. `cargo test` runs each test
    # binary with its PACKAGE directory as cwd, and this repo's tests are
    # full of `../../examples/drill.yaml`-shaped relative paths. Run them
    # from anywhere else and half of them fail in milliseconds on a missing
    # file — which looks like a very fast suite and measures nothing.
    b0="$(now)"
    ( cd "$dir" && "$exe" --test-threads=1 >/dev/null 2>&1 )
    brc=$?
    b1="$(now)"
    bsecs="$(elapsed "$b0" "$b1")"
    printf '  %8ss  rc=%s  %s\n' "$bsecs" "$brc" "$(basename "$exe")"
    if [ "$brc" -ne 0 ]; then
        echo "    ^ exited $brc run on its own — the timing below it is not trustworthy" >&2
    fi

    # A binary under the per-test budget cannot hold a test over it.
    if over "$bsecs" "$TEST_BUDGET"; then
        echo "    ^ over the ${TEST_BUDGET}s per-test budget as a whole — drilling in, test by test:"
        ( cd "$dir" && "$exe" --list --format=terse 2>/dev/null ) \
            | sed -n 's/: test$//p' > "$bins.tests"
        while IFS= read -r tn; do
            [ -n "$tn" ] || continue
            s0="$(now)"
            ( cd "$dir" && "$exe" --exact "$tn" --test-threads=1 >/dev/null 2>&1 )
            s1="$(now)"
            tsecs="$(elapsed "$s0" "$s1")"
            if over "$tsecs" "$TEST_BUDGET"; then
                echo "      FAIL ${tsecs}s  $tn" >&2
                over_budget=$((over_budget + 1))
                slowest_test="$tn"
                slowest_test_secs="$tsecs"
            elif over "$tsecs" 1; then
                echo "      ${tsecs}s  $tn"
            fi
        done < "$bins.tests"
        rm -f "$bins.tests"
    fi
done < "$bins"

# ------------------------------------------------------------------ verdict
rc=0
if [ "$suite_rc" -ne 0 ]; then
    echo "time-unit-suite: FAIL — \`$SUITE_CMD\` exited $suite_rc" >&2
    rc=1
fi
if over "$total" "$SUITE_BUDGET"; then
    echo "time-unit-suite: FAIL — the suite took ${total}s, over the ${SUITE_BUDGET}s budget." >&2
    echo "  If the per-binary table above shows EVERY binary costing about the same" >&2
    echo "  regardless of content, the cost is cargo's fingerprint scan of" >&2
    echo "  target/debug/deps, not the tests: run ./scripts/check-deps-count.sh." >&2
    rc=1
fi
if [ "$over_budget" -ne 0 ]; then
    echo "time-unit-suite: FAIL — $over_budget test(s) over the ${TEST_BUDGET}s per-test budget." >&2
    echo "  slowest named: $slowest_test (${slowest_test_secs}s)" >&2
    echo "  A single unit test that takes seconds is almost always dialling: the 20 s" >&2
    echo "  const T in crates/logweir-kafka/src/rdkafka_reader.rs, or object_store's" >&2
    echo "  unconfigured retry budget. Point it at a double — do NOT shorten T (O18)." >&2
    rc=1
fi

if [ "$rc" -eq 0 ]; then
    echo "time-unit-suite: PASS — ${total}s total (budget ${SUITE_BUDGET}s), no test over ${TEST_BUDGET}s"
fi
exit "$rc"
