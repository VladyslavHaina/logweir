#!/usr/bin/env bash
# The unit-suite timing harness (Task 5b, brief §5 Step 1a / §7 check 1).
#
# TWO BOUNDS, both enforced here:
#
#   * the WHOLE default suite, `cargo test --workspace`, under
#     $LOGWEIR_UNIT_SUITE_BUDGET_SECS (default 120); and
#   * NO SINGLE TEST over $LOGWEIR_UNIT_TEST_BUDGET_SECS (default 15). The budget exists to catch a 20 s dial timeout hiding in the unit suite; the two-reader parity and shape walkers legitimately take 5–12 s over 15+ signed documents (measured at base, no load), so 5 was red on its own.
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

# ----------------------------------------------------------------- budgets
# THE ALIAS (Task 32, stage-2 carried item (d)). `LOGWEIR_TIME_BUDGET_SECS` sets
# BOTH budgets at once, because the caller who wants to widen them under load
# wants to widen both and should not have to learn two names to do it. The two
# EXPLICIT variables win when they are also set, so a caller can say "everything
# at 600, except the per-test bound at 15" in one environment.
#
# THE RESOLVED VALUES ARE PRINTED, WITH WHERE EACH CAME FROM. A harness that
# reports a verdict against a budget it does not name is unreadable the one time
# it matters — the run where somebody set the alias in a shell two levels up.
TIME_BUDGET_ALIAS="${LOGWEIR_TIME_BUDGET_SECS:-}"
if [ -n "${LOGWEIR_UNIT_SUITE_BUDGET_SECS:-}" ]; then
    SUITE_BUDGET="$LOGWEIR_UNIT_SUITE_BUDGET_SECS"
    SUITE_BUDGET_SOURCE="LOGWEIR_UNIT_SUITE_BUDGET_SECS"
elif [ -n "$TIME_BUDGET_ALIAS" ]; then
    SUITE_BUDGET="$TIME_BUDGET_ALIAS"
    SUITE_BUDGET_SOURCE="LOGWEIR_TIME_BUDGET_SECS (alias)"
else
    SUITE_BUDGET="120"
    SUITE_BUDGET_SOURCE="default"
fi
if [ -n "${LOGWEIR_UNIT_TEST_BUDGET_SECS:-}" ]; then
    TEST_BUDGET="$LOGWEIR_UNIT_TEST_BUDGET_SECS"
    TEST_BUDGET_SOURCE="LOGWEIR_UNIT_TEST_BUDGET_SECS"
elif [ -n "$TIME_BUDGET_ALIAS" ]; then
    TEST_BUDGET="$TIME_BUDGET_ALIAS"
    TEST_BUDGET_SOURCE="LOGWEIR_TIME_BUDGET_SECS (alias)"
else
    TEST_BUDGET="15"
    TEST_BUDGET_SOURCE="default"
fi
echo "time-unit-suite: suite budget    ${SUITE_BUDGET}s (from $SUITE_BUDGET_SOURCE)"
echo "time-unit-suite: per-test budget ${TEST_BUDGET}s (from $TEST_BUDGET_SOURCE)"

# THE LOAD AVERAGE, PRINTED ON EVERY RED (Task 32, stage-2 carried item (d)).
# This plan runs up to three agents concurrently at CARGO_BUILD_JOBS=4, and the
# unit suite has been measured at ~20-35 s idle and ~105-129 s under two
# concurrent agent builds — twice over the 120 s budget, and green on the same
# tree when run alone. Without this line a load-induced red is indistinguishable
# from a regression, and the reader of the transcript has no way to tell which
# one they are looking at an hour later.
load_average() {
    if command -v uptime >/dev/null 2>&1; then
        echo "time-unit-suite: load average $(uptime | sed 's/.*[Ll]oad average[s]*: *//')" >&2
    else
        echo "time-unit-suite: load average unavailable (no uptime on PATH)" >&2
    fi
}

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

# ------------------------------------------------------- the toolchain pin
# FIX ROUND 2 (review N1). This harness runs every test binary STANDALONE for
# its attribution pass, and `crates/logweir-core/tests/fixture_regen.rs` — the
# only test in the workspace that spawns a nested cargo — then builds into
# `target/fixture-regen/`.
#
# Standalone, that child does NOT inherit the `RUSTUP_TOOLCHAIN` that `cargo
# test` propagates. `env!("CARGO")` bakes in the real
# `~/.rustup/toolchains/1.89.0-.../bin/cargo`, which is not a rustup proxy, so
# the `rustc` it finds on PATH is rustup's shim with no toolchain selected —
# and that resolved rustup's DEFAULT channel (`stable`, 1.97.1 here) rather
# than this workspace's pin. MEASURED: `target/fixture-regen/` came out built
# by rustc 1.97.1, after which every `cargo test --workspace` failed with
# `E0514: found crate ... compiled by an incompatible version of rustc`, whose
# only printed remedy is `cargo clean` — the one command this task exists to
# make unnecessary. Worse, resolving `stable` made rustup SYNC THE CHANNEL FROM
# THE NETWORK, from inside a lint gate (GC17).
#
# Exporting the pin fixes both at once: the child resolves the pinned rustc,
# and rustup has nothing to fetch. VERIFIED: with the pin exported the
# standalone run passes, writes rustc 1.89.0 artifacts, emits zero
# "syncing channel updates" lines, and leaves `cargo test --workspace` green.
if [ -f rust-toolchain.toml ]; then
    PINNED_TOOLCHAIN="$(sed -n 's/^[[:space:]]*channel[[:space:]]*=[[:space:]]*"\([^"]*\)".*/\1/p' rust-toolchain.toml | head -1)"
    PIN_SOURCE="rust-toolchain.toml"
else
    # No pin file: fall back to the workspace's declared MSRV, and SAY so —
    # `rust-version` is a floor, not a pin, so it is a weaker guarantee.
    #
    # IT RESOLVES A FULL THREE-COMPONENT VERSION, AND NEVER EXPORTS THE FLOOR
    # ITSELF (Task 32, stage-2 carried item (c)). `Cargo.toml`'s `rust-version`
    # is `"1.89"` — TWO components — and this branch used to export that string
    # verbatim as `RUSTUP_TOOLCHAIN`. Handing rustup a channel it may not have
    # installed makes rustup SYNC IT FROM THE NETWORK, which is the exact
    # failure the paragraph above documents for `stable`, arriving from inside a
    # lint gate (STANDING RULE 7, Global Constraint 17) — and arriving on the
    # one code path that runs when the pin file is missing, i.e. when nobody is
    # watching.
    #
    # So: take the floor, list the toolchains rustup ALREADY HAS, select the
    # highest installed `<floor>.<patch>`, and export that. If none is
    # installed, EXIT 1 naming the floor. A lint gate does not install a
    # toolchain; refusing and saying which one is missing is the whole remedy.
    MSRV_FLOOR="$(sed -n 's/^[[:space:]]*rust-version[[:space:]]*=[[:space:]]*"\([^"]*\)".*/\1/p' Cargo.toml | head -1)"
    PINNED_TOOLCHAIN=""
    if [ -n "$MSRV_FLOOR" ] && command -v rustup >/dev/null 2>&1; then
        # The floor is a literal, not a pattern: `.` must not match any byte.
        MSRV_RE="$(printf '%s' "$MSRV_FLOOR" | sed 's/\./\\./g')"
        # `rustup toolchain list` prints `1.89.0-aarch64-apple-darwin (default)`;
        # the host triple and the ` (default)` / ` (override)` suffix are dropped
        # and what is left must be three components or it is not selected.
        PINNED_TOOLCHAIN="$(rustup toolchain list 2>/dev/null \
            | awk '{print $1}' \
            | grep -E "^${MSRV_RE}\.[0-9]+(-.*)?$" \
            | sed -E "s/^(${MSRV_RE}\.[0-9]+).*/\1/" \
            | sort -u -t. -k3,3n \
            | tail -1)"
    fi
    if [ -z "$PINNED_TOOLCHAIN" ]; then
        echo "time-unit-suite: no rust-toolchain.toml, and no installed toolchain matches the" >&2
        echo "  workspace MSRV floor ${MSRV_FLOOR:-<unreadable from Cargo.toml>} (looked for ${MSRV_FLOOR:-?}.<patch> in \`rustup toolchain list\`)." >&2
        echo "time-unit-suite: REFUSING to run. The floor is TWO components and exporting it as" >&2
        echo "  RUSTUP_TOOLCHAIN makes rustup sync that channel FROM THE NETWORK, from inside a" >&2
        echo "  lint gate. A lint gate does not install a toolchain: run" >&2
        echo "  \`rustup toolchain install ${MSRV_FLOOR:-1.89}\` yourself, or restore rust-toolchain.toml." >&2
        load_average
        exit 1
    fi
    PIN_SOURCE="Cargo.toml rust-version $MSRV_FLOOR, resolved to the highest installed $MSRV_FLOOR.x (no rust-toolchain.toml — an MSRV floor, not a pin)"
fi
if [ -z "$PINNED_TOOLCHAIN" ]; then
    echo "time-unit-suite: cannot determine the pinned toolchain from rust-toolchain.toml or Cargo.toml." >&2
    echo "time-unit-suite: REFUSING to run. Standalone test binaries here spawn a nested cargo, and" >&2
    echo "  without a pin it resolves rustup's DEFAULT toolchain — which poisons target/fixture-regen" >&2
    echo "  with artifacts from the wrong rustc and makes rustup fetch a channel from the network." >&2
    exit 1
fi
export RUSTUP_TOOLCHAIN="$PINNED_TOOLCHAIN"
echo "time-unit-suite: toolchain pinned to $RUSTUP_TOOLCHAIN (from $PIN_SOURCE)"

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
    load_average
    exit "$build_rc"
fi

# ------------------------------------------------------------------ total
echo "time-unit-suite: timing \`$SUITE_CMD\`"
# Captured to a file rather than piped: the exit code must be read on its own
# line, and the verdict below needs to quote the suite's FIRST real failure
# instead of guessing at a cause (review, prerequisite finding).
suite_log="$(mktemp)"
# THE TRAP IS INSTALLED HERE, NOT IN THE SUCCESS BRANCH (Task 32, stage-2
# carried item (b)). The only `rm` for this file used to live inside the
# attribution pass's trap, which sits inside `if [ "$suite_rc" -eq 0 ]` — so
# EVERY RED RUN LEAKED A TEMP FILE, on the runs an agent repeats most. The trap
# below covers every path including the red one; the trap in the attribution
# pass EXTENDS this list rather than replacing it.
trap 'rm -f "$suite_log"' EXIT
t0="$(now)"
$SUITE_CMD > "$suite_log" 2>&1
suite_rc=$?
t1="$(now)"
total="$(elapsed "$t0" "$t1")"
cat "$suite_log"

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
#
# ONLY WHEN THE SUITE PASSED (fix round 2: review N1 and the prerequisite
# finding). Timing a suite that is already failing measures the failure, not
# the suite: with `.engine/kafka-backup` missing, this pass reported
# `emit_fixture_reproduces_the_committed_scorecard_bytes (5.04s)` under the
# heading "almost always dialling" — a test that neither dials nor is slow,
# and was merely rebuilding a dependency tree. A gate that names the wrong
# cause costs more than one that names none. When the suite fails the verdict
# quotes the suite's own first panic and stops; nothing is left for a
# stopwatch to add, and fixture_regen is never run standalone on that path.
#
# The block below is deliberately NOT indented under its `if`: it contains a
# quoted heredoc, and an indented terminator does not terminate one.
slowest_test=""
slowest_test_secs=0
over_budget=0
if [ "$suite_rc" -eq 0 ]; then
bins="$(mktemp)"
arts="$(mktemp)"
trap 'rm -f "$bins" "$arts" "$bins.tests" "$suite_log"' EXIT
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
        # Do NOT drill in. A failing binary's per-test timings are the cost of
        # whatever is failing, and reporting one of them as "over the per-test
        # budget" blames an innocent test for a prerequisite problem.
        echo "    ^ exited $brc on its own — timing skipped; that is a failure to fix, not a slow test" >&2
        continue
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
else
echo ""
echo "time-unit-suite: the suite FAILED, so the per-binary attribution pass is SKIPPED."
echo "time-unit-suite: fix the failure first — a stopwatch adds nothing to a red suite."
fi

# ------------------------------------------------------------------ verdict
rc=0
if [ "$suite_rc" -ne 0 ]; then
    echo "time-unit-suite: FAIL — \`$SUITE_CMD\` exited $suite_rc" >&2
    # SAY WHAT ACTUALLY FAILED. The root message is usually excellent and
    # usually ~600 lines above the verdict an operator reads, so it is quoted
    # here. The prerequisite failures this most often surfaces say exactly what
    # to run — `run \`just engine\` first: .../.engine/kafka-backup missing`
    # is the common one — and repeating them costs two lines.
    # A REFERENCE, NOT A SECOND COPY (Task 32, stage-2 carried item (b)). The
    # suite's whole output was already streamed once, by the `cat "$suite_log"`
    # above; re-printing slices of it here made the same bytes appear twice in
    # one transcript, which is how a reader ends up debugging the echo. The
    # verdict now says WHERE to look in the output it already produced.
    panic_at="$(grep -m1 -n 'panicked at' "$suite_log")"
    if [ -n "$panic_at" ]; then
        panic_line="${panic_at%%:*}"
        panic_site="${panic_at#*panicked at }"
        echo "  first panic at suite-log line $panic_line: $panic_site" >&2
        echo "  (that line, and the rest, are in the suite output streamed above)" >&2
    else
        echo "  no 'panicked at' line found — the suite failed to build or was killed." >&2
        echo "  Its last lines are at the end of the suite output streamed above." >&2
    fi
    rc=1
fi
if over "$total" "$SUITE_BUDGET"; then
    echo "time-unit-suite: FAIL — the suite took ${total}s, over the ${SUITE_BUDGET}s budget." >&2
    echo "  If the per-binary table above shows EVERY binary costing about the same" >&2
    echo "  regardless of content, the cost is cargo's fingerprint scan of" >&2
    echo "  target/debug/deps, not the tests: run ./scripts/check-deps-count.sh." >&2
    rc=1
fi
# Only reachable when the suite PASSED — the attribution pass does not run
# otherwise — so every test counted here ran green and its time is its own.
# That is what makes the advice below safe to give.
if [ "$over_budget" -ne 0 ]; then
    echo "time-unit-suite: FAIL — $over_budget test(s) over the ${TEST_BUDGET}s per-test budget." >&2
    echo "  slowest named: $slowest_test (${slowest_test_secs}s)" >&2
    echo "  This test PASSES and is simply slow, which for a unit test here almost" >&2
    echo "  always means dialling: the 20 s const T in" >&2
    echo "  crates/logweir-kafka/src/rdkafka_reader.rs, or object_store's unconfigured" >&2
    echo "  retry budget. Point it at a double — do NOT shorten T (O18)." >&2
    rc=1
fi

if [ "$rc" -eq 0 ]; then
    echo "time-unit-suite: PASS — ${total}s total (budget ${SUITE_BUDGET}s), no test over ${TEST_BUDGET}s"
else
    # EVERY RED CARRIES THE LOAD AVERAGE. One call, at the one place every
    # timing red passes through, so no future failure arm can forget it.
    load_average
fi
exit "$rc"
