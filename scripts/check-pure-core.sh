#!/usr/bin/env bash
# Global Constraint 1, MACHINE-CHECKED. `crates/logweir-core/src/lib.rs` opens
# with "No I/O, no clock, no network", and three separate comments in
# `crates/logweir` assert that the clock is read THERE and never in the pure
# layer. Until this script existed, nothing checked that dimension: the
# pure-layer CI job greps `cargo tree` for cloud/k8s/engine crates only, and
# `logweir_core::ids::new_run_id` called `ulid::Ulid::new()` — a
# `SystemTime::now()` plus an OS-entropy read — through every review of the
# build.
#
# Two independent checks, because either alone proves the wrong thing: a source
# grep says nothing about what a dependency does on the crate's behalf, and a
# dependency list says nothing about a direct `std` call.
set -euo pipefail
cd "$(dirname "$0")/.."

fail=0
CRATES="logweir-core"

echo "== no clock, entropy, filesystem, process or network API in the pure layer =="
# Whole-word alternatives, POSIX classes only (BSD grep on macOS silently
# matches nothing for the GNU `\s`/`\b` extensions and would report a clean
# run over a genuine violation). `Ulid::new` is named explicitly because it is
# the exact call that got past every previous review: it reads BOTH the clock
# and the entropy pool, and neither `SystemTime` nor `rand` appears at the call
# site.
pattern='SystemTime|Instant::now|Utc::now|Local::now|Ulid::new|rand::|thread_rng|getrandom|std::fs|std::net|std::process|std::env|File::open|TcpStream'
for c in $CRATES; do
  # `#[cfg(test)]` code is checked too, deliberately: a test that reads the
  # clock in the pure crate is how the next `Ulid::new()` gets reintroduced.
  if hits=$(grep -rnE "$pattern" "crates/$c/src" --include='*.rs' \
              | grep -v '^[^:]*:[0-9]*:[[:space:]]*//' \
              | grep -v '^[^:]*:[0-9]*:[[:space:]]*///'); then
    echo "FAIL: $c reaches an impure API:"
    printf '%s\n' "$hits"
    fail=1
  else
    echo "ok: $c/src names no clock, entropy, fs, process or network API"
  fi
done

echo "== the pure layer's dependency tree carries no clock or entropy crate =="
for c in $CRATES; do
  # `ulid` itself is permitted and is the point: `logweir-core` takes it with
  # `default-features = false`, which drops its `std` feature and with it the
  # `rand` dependency, so the crate links the ENCODER and not the generator.
  # `chrono` is likewise permitted — the pure layer TYPES timestamps that the
  # impure layer takes and passes in. What must never appear is a source of
  # entropy, which no caller can pass in and which could therefore only be
  # read here.
  deps=$(cargo tree -p "$c" --no-default-features --prefix none --edges normal 2>/dev/null \
         | grep -Ei '^(rand|rand_core|getrandom|fastrand) ' || true)
  if [ -n "$deps" ]; then
    echo "FAIL: $c pulls an entropy source:"; printf '%s\n' "$deps"; fail=1
  else
    echo "ok: $c pulls no entropy source"
  fi
done

exit "$fail"
