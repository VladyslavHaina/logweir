#!/usr/bin/env bash
# Run EXACTLY the named tests, and FAIL when a name matches nothing.
#
# `cargo test <filter>` exits 0 when the filter matches zero tests. Any check
# built on a bare filter therefore reports success having run NOTHING — the
# same "reported ok having examined nothing" shape this repository has had to
# remove from `check-links.sh` and `guard::scan_forbidden_keys`.
#
# It reached the product's own support matrix:
# `.github/workflows/engine-matrix.yml`'s positive control filtered on
# `dry_run_check_segments`, which is a struct field and a rendered-YAML key and
# has never been the name of a test. `cargo test --workspace --features e2e --
# --list` matched 0. `steps.control.outcome` was therefore ALWAYS `success`,
# and `fail(lever-not-honoured)` — the one outcome of the five that detects
# upstream accepting a lever and not acting on it — was unreachable by
# construction, while the weekly sweep published a green verdict about a check
# that never ran.
#
# So: the existence of every requested test is established BEFORE anything is
# run, against `--list`, and a name that matches nothing is exit 1 with the
# name printed. `--exact` then runs those tests and nothing else, so a name
# that is a prefix of a different test cannot silently stand in for it.
#
# Usage:
#   scripts/run-named-tests.sh <test-name> [<test-name>...]
#
# Cargo arguments come from $CARGO_TEST_ARGS (default: the e2e sweep's), and
# libtest arguments from $LIBTEST_ARGS (default: --test-threads=1, which the
# e2e suite requires — it shares one compose stack).
set -euo pipefail
cd "$(dirname "$0")/.."

if [ "$#" -lt 1 ]; then
  echo "usage: $0 <test-name> [<test-name>...]" >&2
  exit 2
fi

# Deliberately unquoted expansion below: these are argument LISTS.
# shellcheck disable=SC2206
CARGO_ARGS=(${CARGO_TEST_ARGS:---workspace --features e2e})
# shellcheck disable=SC2206
LIBTEST=(${LIBTEST_ARGS:---test-threads=1})

echo "== resolving $# test name(s) against the compiled test binaries =="
# `--list` prints one `<name>: test` line per test, across every test binary in
# the selection. Compilation failures surface here, before any assertion about
# what does or does not exist.
listing=$(cargo test "${CARGO_ARGS[@]}" -- --list)

missing=0
for name in "$@"; do
  if printf '%s\n' "$listing" | grep -qxF "$name: test"; then
    echo "ok: $name"
  else
    echo "FAIL: no test named \`$name\` exists in this selection." >&2
    echo "      A filter that matches nothing makes \`cargo test\` exit 0, so a" >&2
    echo "      check built on it reports success having run nothing. Fix the" >&2
    echo "      name or delete the check — do not leave it green and vacuous." >&2
    missing=1
  fi
done

if [ "$missing" -ne 0 ]; then
  echo "-- $(printf '%s\n' "$listing" | grep -c ': test$') test(s) were available; none of the missing names is among them --" >&2
  exit 1
fi

echo "== running $# named test(s) =="
cargo test "${CARGO_ARGS[@]}" -- "${LIBTEST[@]}" --exact "$@"
