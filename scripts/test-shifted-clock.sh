#!/usr/bin/env bash
# The time-bomb guard (FX-9): run the workspace's unit suites with every TEST
# BINARY's wall clock shifted forward, so a test whose verdict depends on the
# wall clock against a FIXED instant fails today, not on the day that instant
# passes.
#
# Usage: scripts/test-shifted-clock.sh [--self-check] [+<days>d] [extra `cargo test` args]
#   The offset defaults to +366d (a year and a day). `just test-shifted-clock`
#   runs it. `--self-check` runs every check below, then stops before the
#   suite.
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
# binary. What a test spawns inherits the shift: the `logweir` binary under
# test, a Python verifier, and a `cargo run` target (cargo hands that to the
# same runner). Monotonic clocks stay real, so a timeout still means what it
# says.
#
# THE TOOLCHAIN KEEPS THE REAL CLOCK. Tests spawn cargo (`cargo metadata`,
# `cargo run --example`), and cargo keeps shared state keyed by the clock: its
# global cache tracker (`$CARGO_HOME/.global-cache`) stamps each crate's last
# use, and its automatic GC deletes what looks long unused. A cargo that saw
# the shift once ran that GC as if it were 2100 on the shared `~/.cargo` (FX-9
# review, MEDIUM-1). Three fences stop that:
#   1. The shim stands down in cargo, rustc, rustdoc, rustup, rustfmt,
#      clippy-driver and every `cargo-*` and `rust-*` program. On macOS it also
#      drops itself from their environment, so nothing they start (rustc, a
#      build script, the linker) loads it, and it logs each stand-down to
#      `target/shifted-clock/stood-down.log`. On Linux, libfaketime's
#      FAKETIME_SKIP_CMDS names the same programs plus `build-script-build`,
#      which keeps their clock real but leaves libfaketime loaded in them
#      (see "Linux" below); anything else they start keeps the preload.
#   2. The runner sets CARGO_CACHE_AUTO_CLEAN_FREQUENCY=never, so no cargo
#      below a test binary runs the GC at all.
#   3. The self-check below refuses unless a cargo started under the shim
#      keeps the real clock.
#
# THE SHIM, PER PLATFORM.
#   * macOS: a small interposer built here with `cc`. It moves
#     `clock_gettime(CLOCK_REALTIME)` (what `SystemTime::now()`, and so every
#     `chrono::Utc::now()`, reads), `gettimeofday` and `time` forward, and
#     moves every ABSOLUTE `pthread_cond_timedwait` deadline back by the same
#     amount. That last part is why it is not libfaketime: libfaketime on macOS
#     shifts the clocks but not the deadlines, so a timed wait whose deadline is
#     computed from the shifted clock waits for the offset itself. FX-9
#     measured it (2026-10-05, libfaketime 0.9.13): Rust's
#     `Condvar::wait_timeout` and librdkafka's `cnd_timedwait_ms` both hang,
#     and the suite stalled in the first test that dials a broker.
#   * Linux: libfaketime through `LD_PRELOAD`. In a Debian bookworm container
#     (libfaketime 0.9.10, 2026-10-05) it moved `pthread_cond_timedwait`
#     deadlines back but not C11 `cnd_timedwait` ones (librdkafka's wait on
#     Linux), and `rustc` deadlocked with it preloaded even when
#     FAKETIME_SKIP_CMDS named rustc. So there the self-check refuses, in
#     about twelve seconds, and the suite has never run on Linux: that needs a
#     Linux port of the macOS shim, which does not exist yet.
#
# IT REFUSES RATHER THAN SKIPS (exit 2). A "shifted-clock" run that quietly
# runs at the real clock, hangs, or shifts cargo is the failure this guard
# exists to prevent. So, before the suite, three checks run through the SAME
# wrapper as the test binaries:
#   * a Rust probe must see at least the offset less one day, and its 200 ms
#     `Condvar::wait_timeout` must return within ten seconds (that wait has a
#     wall-clock deadline only on macOS with the pinned Rust 1.89; Rust 1.97's
#     macOS Condvar and Rust's Linux one do not hang under a bad shim);
#   * a C probe must see the shift on `time`, `gettimeofday` and
#     `clock_gettime`, and its 200 ms timed waits must return within ten
#     seconds: `pthread_cond_timedwait` with deadlines from `clock_gettime`
#     and from `gettimeofday` (librdkafka's macOS path) and, where <threads.h>
#     exists, C11 `cnd_timedwait` with a `timespec_get` deadline (librdkafka's
#     Linux path). This is the hang check that holds on every platform and
#     toolchain;
#   * cargo builds a throwaway crate in a throwaway CARGO_HOME and target
#     directory. Its `--timings` report is stamped with cargo's own clock and
#     the crate's build script prints its clock: both must be real.
# It also refuses `--target` and CARGO_BUILD_TARGET: the shim and the probes
# are built for the host and the runner is set for the host only, so a test
# binary built for another target would run at the real clock and pass. A run
# in which no test binary went through the runner at all (a `build.target` in
# a cargo config, or only doctests, or `--no-run`) fails after the suite. (A
# `--config` runner cannot replace this one: cargo prefers the environment
# variable the script exports, measured with cargo 1.89.) Where `timeout` or
# `gtimeout` exists, the suite runs under a two-hour deadline; stock macOS has
# neither, and there the probes are the hang guard.
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

refuse() {
  echo "REFUSED: $*" >&2
  exit 2
}

# Runs "$@" for at most $1 seconds, with its output in the file $2. Returns
# the command's exit code, or 124 once it (and its children) had to be killed.
bounded() {
  local secs="$1" out="$2" pid
  shift 2
  "$@" > "$out" 2>&1 &
  pid=$!
  for _ in $(seq 1 $(( secs * 10 ))); do
    kill -0 "$pid" 2>/dev/null || break
    sleep 0.1
  done
  if kill -0 "$pid" 2>/dev/null; then
    pkill -P "$pid" 2>/dev/null
    kill "$pid" 2>/dev/null
    wait "$pid" 2>/dev/null
    return 124
  fi
  wait "$pid"
}

self_check_only=0
if [ "${1:-}" = "--self-check" ]; then
  self_check_only=1
  shift
fi
offset="+366d"
if [ $# -gt 0 ]; then
  offset="$1"
  shift
fi
case "$offset" in
  +[0-9]*d) days="${offset#+}"; days="${days%d}" ;;
  *)
    echo "usage: scripts/test-shifted-clock.sh [--self-check] [+<days>d] [cargo test args]; got offset '$offset'" >&2
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

# The runner is set for the host target only (see the header).
host_only="the clock shim and the probes are built for the host, and the runner is set for the host target only, so a test binary built for another target would run at the real clock and pass"
for arg in "$@"; do
  case "$arg" in
    --) break ;;
    --target|--target=*) refuse "$arg: $host_only. Run it without --target." ;;
  esac
done
if [ -n "${CARGO_BUILD_TARGET:-}" ]; then
  refuse "CARGO_BUILD_TARGET=$CARGO_BUILD_TARGET: $host_only. Unset it."
fi

dir=target/shifted-clock
mkdir -p "$dir"
runner="$PWD/$dir/runner.sh"
ran="$PWD/$dir/ran.log"
stood="$PWD/$dir/stood-down.log"
rm -f "$ran" "$stood"

# ------------------------------------------------------------- the shim
case "$(uname -s)" in
  Darwin)
    cat > "$dir/shiftclock.c" <<'EOF'
/* FX-9's macOS clock shim. See scripts/test-shifted-clock.sh. */
#include <crt_externs.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

/* dyld's interposing section, as <mach-o/dyld-interposing.h> spells it. */
#define DYLD_INTERPOSE(_replacement, _replacee)                                 \
    __attribute__((used)) static struct {                                       \
        const void *replacement;                                                \
        const void *replacee;                                                   \
    } _interpose_##_replacee __attribute__((section("__DATA,__interpose"))) = { \
        (const void *)(unsigned long)&_replacement,                             \
        (const void *)(unsigned long)&_replacee};

static long long shift; /* seconds, set once before main */

/* The Rust toolchain keeps the real clock: cargo judges its shared cache
   against it (the script's header, "THE TOOLCHAIN KEEPS THE REAL CLOCK"). */
static int toolchain(const char *name) {
    static const char *const names[] = {"cargo",   "rustc",         "rustdoc", "rustup",
                                        "rustfmt", "clippy-driver", 0};
    for (int i = 0; names[i]; i++)
        if (strcmp(name, names[i]) == 0) return 1;
    return strncmp(name, "cargo-", 6) == 0 || strncmp(name, "rust-", 5) == 0;
}

/* One line per stand-down, the command line, appended to SHIFTED_CLOCK_LOG. */
static void log_stand_down(const char *name) {
    const char *path = getenv("SHIFTED_CLOCK_LOG");
    if (!path || !*path) return;
    char line[512];
    size_t room = sizeof line - 1, n = 0; /* a byte is kept for the newline */
    int argc = *_NSGetArgc();
    char **argv = *_NSGetArgv();
    for (int i = 0; i < argc && n + 1 < room; i++) {
        int w = snprintf(line + n, room - n, i ? " %s" : "%s", i ? argv[i] : name);
        if (w < 0) break;
        n += (size_t)w < room - n ? (size_t)w : room - n - 1;
    }
    line[n++] = '\n';
    int fd = open(path, O_WRONLY | O_APPEND | O_CREAT | O_CLOEXEC, 0644);
    if (fd >= 0) {
        (void)write(fd, line, n);
        close(fd);
    }
}

__attribute__((constructor)) static void read_shift(void) {
    const char *v = getenv("SHIFTED_CLOCK_SECONDS");
    long long s = v ? atoll(v) : 0;
    const char *name = getprogname();
    if (s > 0 && name && toolchain(name)) {
        /* Stand down here, and for everything this process starts. */
        unsetenv("DYLD_INSERT_LIBRARIES");
        unsetenv("SHIFTED_CLOCK_SECONDS");
        log_stand_down(name);
        s = 0;
    }
    shift = s > 0 ? s : 0;
}

static int shifted_clock_gettime(clockid_t clock, struct timespec *tp) {
    int rc = clock_gettime(clock, tp);
    if (rc == 0 && tp && clock == CLOCK_REALTIME) tp->tv_sec += shift;
    return rc;
}
static int shifted_gettimeofday(struct timeval *tv, void *tz) {
    int rc = gettimeofday(tv, tz);
    if (rc == 0 && tv) tv->tv_sec += shift;
    return rc;
}
static time_t shifted_time(time_t *out) {
    time_t t = time(NULL);
    if (t != (time_t)-1) t += (time_t)shift;
    if (out) *out = t;
    return t;
}
/* A deadline computed from the shifted clock, handed back to the kernel's. */
static int shifted_cond_timedwait(pthread_cond_t *c, pthread_mutex_t *m,
                                  const struct timespec *abstime) {
    if (!abstime) return pthread_cond_timedwait(c, m, abstime);
    struct timespec real = *abstime;
    real.tv_sec -= shift;
    return pthread_cond_timedwait(c, m, &real);
}
DYLD_INTERPOSE(shifted_clock_gettime, clock_gettime)
DYLD_INTERPOSE(shifted_gettimeofday, gettimeofday)
DYLD_INTERPOSE(shifted_time, time)
DYLD_INTERPOSE(shifted_cond_timedwait, pthread_cond_timedwait)
EOF
    bounded 300 "$dir/build.out" cc -dynamiclib -O2 -o "$dir/libshiftclock.dylib" "$dir/shiftclock.c"
    built=$?
    if [ "$built" -ne 0 ]; then
      refuse "the macOS clock shim did not build (cc exit $built); install the Xcode command line tools. $(cat "$dir/build.out")"
    fi
    shim="$PWD/$dir/libshiftclock.dylib"
    # Exported INSIDE the wrapper, after /bin/sh has started: macOS strips
    # DYLD_* from a protected binary's own environment, but not from what that
    # shell then hands the test binary it execs.
    cat > "$runner" <<EOF
#!/bin/sh
printf '%s\n' "\$1" >> '$ran'
export DYLD_INSERT_LIBRARIES='$shim'
export SHIFTED_CLOCK_SECONDS='$seconds'
export SHIFTED_CLOCK_LOG='$stood'
export CARGO_CACHE_AUTO_CLEAN_FREQUENCY=never
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
      refuse "libfaketime is not installed (apt-get install faketime), or set LIBFAKETIME to its path."
    fi
    cat > "$runner" <<EOF
#!/bin/sh
printf '%s\n' "\$1" >> '$ran'
export LD_PRELOAD='$shim'
export FAKETIME='+${days}d'
export FAKETIME_DONT_FAKE_MONOTONIC=1
export FAKETIME_SKIP_CMDS='cargo,rustc,rustdoc,rustup,rustfmt,cargo-fmt,cargo-clippy,clippy-driver,build-script-build'
export CARGO_CACHE_AUTO_CLEAN_FREQUENCY=never
exec "\$@"
EOF
    ;;
  *)
    refuse "no clock shim is known for $(uname -s)"
    ;;
esac
chmod +x "$runner"

# ------------------------------------------------------------- the probes
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
cat > "$dir/waitprobe.c" <<'EOF'
/* FX-9's timed-wait probe. See scripts/test-shifted-clock.sh. It prints the
   lowest of the wall clocks C code reads, then waits 200 ms the ways
   librdkafka does, each with an ABSOLUTE deadline taken from a wall clock: a
   shim that shifts the clock but not the deadline makes a wait last the whole
   offset. */
#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <sys/time.h>
#include <time.h>
#if defined(__has_include)
#if __has_include(<threads.h>)
#include <threads.h>
#define C11_THREADS 1
#endif
#endif

static void plus_200ms(struct timespec *ts) {
    ts->tv_nsec += 200 * 1000000L;
    ts->tv_sec += ts->tv_nsec / 1000000000L;
    ts->tv_nsec %= 1000000000L;
}

int main(void) {
    struct timespec ts;
    struct timeval tv;
    long long low = (long long)time(NULL);
    gettimeofday(&tv, NULL);
    if ((long long)tv.tv_sec < low) low = (long long)tv.tv_sec;
    clock_gettime(CLOCK_REALTIME, &ts);
    if ((long long)ts.tv_sec < low) low = (long long)ts.tv_sec;
    printf("%lld\n", low);
    fflush(stdout);

    pthread_mutex_t m = PTHREAD_MUTEX_INITIALIZER;
    pthread_cond_t c = PTHREAD_COND_INITIALIZER;
    int rc;
    pthread_mutex_lock(&m);
    clock_gettime(CLOCK_REALTIME, &ts);
    plus_200ms(&ts);
    do rc = pthread_cond_timedwait(&c, &m, &ts); while (rc == 0);
    if (rc != ETIMEDOUT) return 1;
    gettimeofday(&tv, NULL); /* librdkafka's deadline on macOS */
    ts.tv_sec = tv.tv_sec;
    ts.tv_nsec = (long)tv.tv_usec * 1000L;
    plus_200ms(&ts);
    do rc = pthread_cond_timedwait(&c, &m, &ts); while (rc == 0);
    if (rc != ETIMEDOUT) return 1;
    pthread_mutex_unlock(&m);
#ifdef C11_THREADS
    /* librdkafka's wait on Linux, where it uses C11 threads */
    mtx_t cm;
    cnd_t cc;
    if (mtx_init(&cm, mtx_plain) != thrd_success || cnd_init(&cc) != thrd_success) return 1;
    mtx_lock(&cm);
    timespec_get(&ts, TIME_UTC);
    plus_200ms(&ts);
    do rc = cnd_timedwait(&cc, &cm, &ts); while (rc == thrd_success);
    if (rc != thrd_timedout) return 1;
    mtx_unlock(&cm);
#endif
    puts("waited");
    return 0;
}
EOF
bounded 300 "$dir/build.out" rustc --edition 2021 -o "$dir/probe" "$dir/probe.rs"
built=$?
if [ "$built" -ne 0 ]; then
  refuse "the clock probe did not build (rustc exit $built). $(cat "$dir/build.out")"
fi
bounded 300 "$dir/build.out" cc -O2 -pthread -o "$dir/waitprobe" "$dir/waitprobe.c"
built=$?
if [ "$built" -ne 0 ]; then
  refuse "the timed-wait probe did not build (cc exit $built). $(cat "$dir/build.out")"
fi

bounded 10 "$dir/probe.real" "$dir/probe"
probed=$?
real="$(head -n 1 "$dir/probe.real")"
case "$real" in
  ''|*[!0-9]*) refuse "the clock probe printed no instant at the real clock (exit $probed): $(cat "$dir/probe.real")" ;;
esac
floor=$(( real + (days - 1) * 86400 ))

# Runs the probe $1 through the runner. Refuses unless it returns within ten
# seconds, prints "waited", and its first line is an instant at or past $floor.
probe_shifted() {
  local probe="$1" name rc first
  name="$(basename "$probe")"
  bounded 10 "$probe.out" "$runner" "$probe"
  rc=$?
  if [ "$rc" -eq 124 ]; then
    refuse "$name: a 200 ms timed wait did not return within ten seconds under the shift, so the suite would hang."
  fi
  if [ "$rc" -ne 0 ] || ! grep -qx waited "$probe.out"; then
    refuse "$name failed under the shim (exit $rc): $(cat "$probe.out")"
  fi
  first="$(head -n 1 "$probe.out")"
  case "$first" in
    ''|*[!0-9]*) first=0 ;;
  esac
  if [ "$first" -lt "$floor" ]; then
    refuse "the shift did not reach $name (real $real, through the wrapper '$first', wanted at least $floor). The suite was not run: at the real clock it would prove nothing."
  fi
  shifted="$first"
}
probe_shifted "$dir/probe"
probe_shifted "$dir/waitprobe"

# ------------------------------------------- the toolchain keeps the real clock
# A throwaway crate, its own workspace, built by a cargo started through the
# runner in a throwaway CARGO_HOME and target directory: if the shim did reach
# that cargo, what it touched is under target/shifted-clock.
sc="$PWD/$dir/selfcheck"
rm -rf "$sc"
mkdir -p "$sc/src"
cat > "$sc/Cargo.toml" <<'EOF'
[package]
name = "shifted-clock-selfcheck"
version = "0.0.0"
edition = "2021"
publish = false

[workspace]
EOF
cat > "$sc/build.rs" <<'EOF'
fn main() {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("a clock after 1970")
        .as_secs();
    println!("cargo:warning=build-script-clock={now}");
}
EOF
: > "$sc/src/lib.rs"
# The cargo that tests reach through `env!("CARGO")`, not the rustup proxy.
cargo_bin="$(rustup which cargo 2>/dev/null)" || cargo_bin="$(command -v cargo)"
before="$(date -u +%Y%m%dT%H%M)"
before_s="$(date +%s)"
bounded 300 "$sc/build.out" env CARGO_HOME="$sc/home" CARGO_TARGET_DIR="$sc/target" \
  "$runner" "$cargo_bin" build --offline --timings --manifest-path "$sc/Cargo.toml"
built=$?
after="$(date -u +%Y%m%dT%H%M)"
after_s="$(date +%s)"
if [ "$built" -ne 0 ]; then
  refuse "cargo, started under the shim, did not build a throwaway crate (exit $built): $(cat "$sc/build.out")"
fi
stamp=""
for report in "$sc"/target/cargo-timings/cargo-timing-[0-9]*.html; do
  if [ -f "$report" ]; then
    stamp="${report##*/cargo-timing-}"
    stamp="${stamp:0:13}"
  fi
done
case "$stamp" in
  [0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]T[0-9][0-9][0-9][0-9]) ;;
  *) refuse "cargo wrote no dated --timings report, so its clock cannot be read: $(cat "$sc/build.out")" ;;
esac
if [[ "$stamp" < "$before" || "$stamp" > "$after" ]]; then
  refuse "cargo, started under the shim, saw the shifted clock: its --timings report is stamped $stamp UTC while the real clock read $before to $after, so its cache GC would judge the shared CARGO_HOME at that date. The suite was not run."
fi
script_clock="$(sed -n 's/.*build-script-clock=\([0-9][0-9]*\).*/\1/p' "$sc/build.out" | head -n 1)"
case "$script_clock" in
  ''|*[!0-9]*) refuse "the throwaway crate's build script printed no clock: $(cat "$sc/build.out")" ;;
esac
if [ "$script_clock" -lt $(( before_s - 60 )) ] || [ "$script_clock" -gt $(( after_s + 60 )) ]; then
  refuse "a build script under that cargo saw the shifted clock ($script_clock while the real clock read $before_s to $after_s). The suite was not run."
fi
echo "shifted-clock: test binaries see $(( (shifted - real) / 86400 )) day(s) ahead and their timed waits return; cargo and its build scripts keep the real clock (shim: $shim)"
if [ "$self_check_only" -eq 1 ]; then
  exit 0
fi

# ------------------------------------------------------------- the suite
# The checks went through the runner too: count only the suite.
rm -f "$ran" "$stood"
host="$(rustc -vV | sed -n 's/^host: //p')"
runner_var="CARGO_TARGET_$(printf '%s' "$host" | tr 'a-z-' 'A-Z_')_RUNNER"
export "$runner_var=$runner"
deadline=""
for candidate in timeout gtimeout; do
  if command -v "$candidate" >/dev/null 2>&1; then
    deadline="$candidate"
    break
  fi
done
if [ -n "$deadline" ]; then
  "$deadline" -k 60 7200 cargo test --locked --workspace --no-fail-fast "$@"
else
  cargo test --locked --workspace --no-fail-fast "$@"
fi
status=$?
if [ -n "$deadline" ] && { [ "$status" -eq 124 ] || [ "$status" -eq 137 ]; }; then
  echo "shifted-clock: the suite did not finish within two hours ($deadline exit $status). A timed wait whose deadline the shim does not move back lasts the whole offset." >&2
elif [ "$status" -ne 0 ]; then
  echo "shifted-clock: the suite FAILED $days day(s) ahead (exit $status). A row that passes at the real clock and fails here compares the wall clock with a fixed instant: hand the code under test a fixed \`now\` (Global Constraint 1)." >&2
fi
if [ ! -s "$ran" ]; then
  echo "shifted-clock: no test binary went through the runner, so nothing ran at the shifted clock (a \`build.target\` in a cargo config? only doctests? \`--no-run\`?). The run proves nothing." >&2
  if [ "$status" -eq 0 ]; then
    status=1
  fi
fi
if [ -s "$stood" ]; then
  echo "shifted-clock: the shim stood down in $(grep -c '' "$stood") toolchain process(es) the tests started; see $dir/stood-down.log"
fi
exit "$status"
