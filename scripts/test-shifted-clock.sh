#!/usr/bin/env bash
# The time-bomb guard (FX-9): run the workspace's unit suites with every TEST
# BINARY's wall clock shifted forward, so a test whose verdict depends on the
# wall clock against a FIXED instant fails today, not on the day that instant
# passes.
#
# Usage: scripts/test-shifted-clock.sh [+<days>d] [extra `cargo test` args]
#   The offset defaults to +366d (a year and a day). `just test-shifted-clock`
#   runs it.
#
# WHY THIS EXISTS. `crates/logweir/tests/standing_approve.rs` minted its
# fixture at a fixed instant (2026-09-01, thirty days) while the mint read the
# wall clock, so three rows went red on 2026-10-01 and blocked every
# integration on main. Nothing that runs at the real clock can see that class
# coming: the suite is green until the day it is not. A year-ahead clock sees a
# year ahead.
#
# HOW. cargo's per-target `runner` hook wraps each test binary in
# `target/shifted-clock/runner.sh`, which preloads a clock shim and execs the
# binary. cargo, rustc and build scripts keep the real clock, so fingerprints
# and the build are untouched, and children a test spawns (the `logweir` binary
# under test, a Python verifier) inherit the shift. Monotonic clocks stay real,
# so a timeout still means what it says.
#
# THE SHIM, PER PLATFORM.
#   * macOS: a twenty-line interposer built here with `cc`. It moves
#     `clock_gettime(CLOCK_REALTIME)` (what `SystemTime::now()`, and so every
#     `chrono::Utc::now()`, reads), `gettimeofday` and `time` forward, and
#     moves every ABSOLUTE `pthread_cond_timedwait` deadline back by the same
#     amount. That last part is why it is not libfaketime: libfaketime on macOS
#     shifts the clocks but not the deadlines, so a timed wait whose deadline is
#     computed from the shifted clock waits for the offset itself. FX-9
#     measured it (2026-10-05, libfaketime 0.9.13): Rust's
#     `Condvar::wait_timeout` and librdkafka's `cnd_timedwait_ms` both hang,
#     and the suite stalled in the first test that dials a broker.
#   * Linux: libfaketime through `LD_PRELOAD`, which does map
#     `pthread_cond_timedwait` there. NOT RUN by FX-9: no Linux host was used.
#
# IT REFUSES RATHER THAN SKIPS (exit 2). A "shifted-clock" run that quietly ran
# at the real clock, or that hangs, is the failure this guard exists to
# prevent. So before the suite, a small Rust probe is built with `rustc` and
# run through the SAME wrapper. It must see at least the offset less one day,
# and a 200 ms `Condvar::wait_timeout` inside it must return within ten
# seconds.
#
# LIMITS. Doctests run without the runner (cargo applies it to test binaries
# only). A child spawned through a SIP-protected macOS binary (`/bin/sh`,
# `/usr/bin/env`, `/usr/bin/perl`, and so `/tmp/lwtimeout`) or with a cleared
# environment loses the preload and runs at the real clock. Feature-gated
# suites (`--features e2e`) are not in the default run; pass the flags after
# the offset to include them.
#
# Every exit code is read into a variable on its own line, never through a
# pipe.
set -uo pipefail

cd "$(dirname "$0")/.." || exit 2

offset="+366d"
if [ $# -gt 0 ]; then
  offset="$1"
  shift
fi
case "$offset" in
  +[0-9]*d) days="${offset#+}"; days="${days%d}" ;;
  *)
    echo "usage: scripts/test-shifted-clock.sh [+<days>d] [cargo test args]; got offset '$offset'" >&2
    exit 2
    ;;
esac
case "$days" in
  *[!0-9]*|'')
    echo "the offset must be +<days>d with a whole number of days; got '$offset'" >&2
    exit 2
    ;;
esac
if [ "$days" -lt 1 ]; then
  echo "an offset of zero days shifts nothing; use plain \`cargo test\`" >&2
  exit 2
fi
seconds=$(( days * 86400 ))

dir=target/shifted-clock
mkdir -p "$dir"
runner="$PWD/$dir/runner.sh"

# ------------------------------------------------------------- the shim
case "$(uname -s)" in
  Darwin)
    cat > "$dir/shiftclock.c" <<'EOF'
/* FX-9's macOS clock shim. See scripts/test-shifted-clock.sh. */
#include <pthread.h>
#include <stdlib.h>
#include <sys/time.h>
#include <time.h>

/* dyld's interposing section, as <mach-o/dyld-interposing.h> spells it. */
#define DYLD_INTERPOSE(_replacement, _replacee)                                 \
    __attribute__((used)) static struct {                                       \
        const void *replacement;                                                \
        const void *replacee;                                                   \
    } _interpose_##_replacee __attribute__((section("__DATA,__interpose"))) = { \
        (const void *)(unsigned long)&_replacement,                             \
        (const void *)(unsigned long)&_replacee};

static long long shift_seconds(void) {
    static long long cached = -1;
    if (cached < 0) {
        const char *v = getenv("SHIFTED_CLOCK_SECONDS");
        long long s = v ? atoll(v) : 0;
        cached = s > 0 ? s : 0;
    }
    return cached;
}
static int shifted_clock_gettime(clockid_t clock, struct timespec *tp) {
    int rc = clock_gettime(clock, tp);
    if (rc == 0 && tp && clock == CLOCK_REALTIME) tp->tv_sec += shift_seconds();
    return rc;
}
static int shifted_gettimeofday(struct timeval *tv, void *tz) {
    int rc = gettimeofday(tv, tz);
    if (rc == 0 && tv) tv->tv_sec += shift_seconds();
    return rc;
}
static time_t shifted_time(time_t *out) {
    time_t t = time(NULL);
    if (t != (time_t)-1) t += (time_t)shift_seconds();
    if (out) *out = t;
    return t;
}
/* A deadline computed from the shifted clock, handed back to the kernel's. */
static int shifted_cond_timedwait(pthread_cond_t *c, pthread_mutex_t *m,
                                  const struct timespec *abstime) {
    if (!abstime) return pthread_cond_timedwait(c, m, abstime);
    struct timespec real = *abstime;
    real.tv_sec -= shift_seconds();
    return pthread_cond_timedwait(c, m, &real);
}
DYLD_INTERPOSE(shifted_clock_gettime, clock_gettime)
DYLD_INTERPOSE(shifted_gettimeofday, gettimeofday)
DYLD_INTERPOSE(shifted_time, time)
DYLD_INTERPOSE(shifted_cond_timedwait, pthread_cond_timedwait)
EOF
    cc -dynamiclib -O2 -o "$dir/libshiftclock.dylib" "$dir/shiftclock.c"
    built=$?
    if [ "$built" -ne 0 ]; then
      echo "REFUSED: the macOS clock shim did not build (cc exit $built); install the Xcode command line tools" >&2
      exit 2
    fi
    shim="$PWD/$dir/libshiftclock.dylib"
    # Exported INSIDE the wrapper, after /bin/sh has started: macOS strips
    # DYLD_* from a protected binary's own environment, but not from what that
    # shell then hands the test binary it execs.
    cat > "$runner" <<EOF
#!/bin/sh
export DYLD_INSERT_LIBRARIES='$shim'
export SHIFTED_CLOCK_SECONDS='$seconds'
exec "\$@"
EOF
    ;;
  Linux)
    shim=""
    for candidate in ${LIBFAKETIME:-} /usr/lib/x86_64-linux-gnu/faketime/libfaketime.so.1 \
      /usr/lib/aarch64-linux-gnu/faketime/libfaketime.so.1 /usr/lib/faketime/libfaketime.so.1 \
      /usr/local/lib/faketime/libfaketime.so.1; do
      if [ -f "$candidate" ]; then
        shim="$candidate"
        break
      fi
    done
    if [ -z "$shim" ]; then
      echo "REFUSED: libfaketime is not installed (apt-get install faketime), or set LIBFAKETIME to its path." >&2
      exit 2
    fi
    cat > "$runner" <<EOF
#!/bin/sh
export LD_PRELOAD='$shim'
export FAKETIME='+${days}d'
export FAKETIME_DONT_FAKE_MONOTONIC=1
exec "\$@"
EOF
    ;;
  *)
    echo "REFUSED: no clock shim is known for $(uname -s)" >&2
    exit 2
    ;;
esac
chmod +x "$runner"

# ------------------------------------------------------------- the probe
cat > "$dir/probe.rs" <<'EOF'
fn main() {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("a clock after 1970");
    println!("{}", now.as_secs());
    // A timed wait whose deadline the platform derives from the realtime
    // clock: it must return in about 200 ms, not after the offset.
    let lock = std::sync::Mutex::new(());
    let guard = lock.lock().expect("an unpoisoned lock");
    let _ = std::sync::Condvar::new()
        .wait_timeout(guard, std::time::Duration::from_millis(200))
        .expect("a timed wait");
    println!("waited");
}
EOF
rustc --edition 2021 -o "$dir/probe" "$dir/probe.rs"
built=$?
if [ "$built" -ne 0 ]; then
  echo "REFUSED: the clock probe did not build (rustc exit $built)" >&2
  exit 2
fi
"$dir/probe" > "$dir/probe.real" 2>&1
real="$(head -1 "$dir/probe.real")"
case "$real" in
  ''|*[!0-9]*)
    echo "REFUSED: the clock probe printed no instant at the real clock: $(cat "$dir/probe.real")" >&2
    exit 2
    ;;
esac
"$runner" "$dir/probe" > "$dir/probe.out" 2>&1 &
probe_pid=$!
for _ in $(seq 1 100); do
  kill -0 "$probe_pid" 2>/dev/null || break
  sleep 0.1
done
if kill -0 "$probe_pid" 2>/dev/null; then
  kill "$probe_pid" 2>/dev/null
  echo "REFUSED: a 200 ms timed wait did not return within ten seconds under the shift, so the suite would hang." >&2
  exit 2
fi
wait "$probe_pid"
probed=$?
shifted="$(head -1 "$dir/probe.out")"
if [ "$probed" -ne 0 ] || ! grep -qx waited "$dir/probe.out"; then
  echo "REFUSED: the clock probe failed under the shim (exit $probed): $(cat "$dir/probe.out")" >&2
  exit 2
fi
floor=$(( real + (days - 1) * 86400 ))
case "$shifted" in
  ''|*[!0-9]*) shifted=0 ;;
esac
if [ "$shifted" -lt "$floor" ]; then
  echo "REFUSED: the shift did not reach a Rust binary (real $real, through the wrapper '$shifted', wanted at least $floor)." >&2
  echo "The suite was not run: at the real clock it would prove nothing." >&2
  exit 2
fi
echo "shifted-clock: test binaries see $(( (shifted - real) / 86400 )) day(s) ahead, and timed waits return (shim: $shim)"

# ------------------------------------------------------------- the suite
host="$(rustc -vV | sed -n 's/^host: //p')"
runner_var="CARGO_TARGET_$(printf '%s' "$host" | tr 'a-z-' 'A-Z_')_RUNNER"
export "$runner_var=$runner"
cargo test --locked --workspace --no-fail-fast "$@"
status=$?
if [ "$status" -ne 0 ]; then
  echo "shifted-clock: the suite FAILED $days day(s) ahead (exit $status). A row that passes at the real clock and fails here compares the wall clock with a fixed instant: hand the code under test a fixed \`now\` (Global Constraint 1)." >&2
fi
exit "$status"
