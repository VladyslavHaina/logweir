#!/usr/bin/env bash
# Two-reader parity over the checked-in signed corpus (Task 3, T0-1).
#
# `logweir drill verify` and `docs/verify_scorecard.py` are documented as
# reaching the same verdict — `docs/verify-a-scorecard.md` says a disagreement
# "is a bug in the format". This script checks that claim mechanically on every
# `just lint`, over the three checked-in scorecard documents, including the one
# whose signature is genuine and whose approval claim is false.
#
# THE TWO READERS DO NOT SHARE AN EXIT-CODE SPACE and never have. `drill
# verify` follows Global Constraint 11 (0/1/2/3/4); the script's own contract
# is 0 VALID / 1 INVALID / 2 could-not-run. So parity is asserted as a VERDICT
# mapping plus byte-identical refusal text, never as numeric equality.
#
# Every exit code is captured into a variable on its own line. Never through a
# pipe: `cmd | head` reports head's status, and a verifier whose failure is
# invisible is worse than no verifier.
set -euo pipefail

cd "$(dirname "$0")/.."
ROOT="$PWD"
FIX="$ROOT/e2e/fixtures/signed"
VERIFIER="$ROOT/docs/verify_scorecard.py"

# Interpreter resolution. This repository already has two names for "the
# python3 that can run the auditor's verifier" — $LOGWEIR_PYTHON (README,
# scripts/demo.sh) and $LOGWEIR_E2E_PYTHON (e2e/tests/harness/mod.rs) — plus
# the repo-local .e2e/venv the e2e harness falls back to. Honour all three, in
# that order, so nobody has to discover a THIRD name for the same thing.
if [ -n "${LOGWEIR_PYTHON:-}" ]; then
    PY="$LOGWEIR_PYTHON"
elif [ -n "${LOGWEIR_E2E_PYTHON:-}" ]; then
    PY="$LOGWEIR_E2E_PYTHON"
elif [ -x "$ROOT/.e2e/venv/bin/python3" ]; then
    PY="$ROOT/.e2e/venv/bin/python3"
else
    PY="python3"
fi

fail() {
    echo "check-verifier-parity: $*" >&2
    exit 1
}

# NOT skipped when `cryptography` is missing. A skipped check is precisely the
# "documented guarantee the code does not deliver" this work exists to
# eliminate: the two-reader claim would go unchecked and nothing would say so.
set +e
"$PY" -c 'import cryptography' >/dev/null 2>&1
probe_rc=$?
set -e
if [ "$probe_rc" -ne 0 ]; then
    fail "python3 with the 'cryptography' package is required (pip install cryptography); the two-reader parity claim is not checkable without it. tried $PY — set \$LOGWEIR_PYTHON (or \$LOGWEIR_E2E_PYTHON, or create .e2e/venv) to point at an interpreter that has it"
fi

BIN="$ROOT/target/debug/logweir"
if [ ! -x "$BIN" ]; then
    BIN="$ROOT/target/release/logweir"
fi
if [ ! -x "$BIN" ]; then
    fail "no built logweir binary at target/debug/logweir or target/release/logweir; run 'cargo build -p logweir' first — the parity claim needs BOTH readers"
fi

# document<TAB>expected rust code<TAB>expected python code
# The mapping is explicit so a reviewer can read the contract off this file.
CASES="scorecard 0 0
scorecard-self-attested 0 0
scorecard-self-attested-bogus 4 1"

approval_line() {
    # The `APPROVAL CLAIM NOT VERIFIED: ...` line, with the Python arm's
    # `INVALID: ` prefix stripped, or empty when the reader did not refuse on
    # the approval claim.
    grep -h 'APPROVAL CLAIM NOT VERIFIED:' "$1" 2>/dev/null | sed 's/^INVALID: //' | head -1 || true
}

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

while IFS=' ' read -r name want_rust want_py; do
    [ -n "$name" ] || continue
    doc="$FIX/$name.json"
    sig="$FIX/$name.sig"
    pub="$FIX/public.pem"
    [ -f "$doc" ] || fail "$name.json is missing; the corpus this check walks is incomplete"
    [ -f "$sig" ] || fail "$name.sig is missing; the corpus this check walks is incomplete"

    set +e
    "$BIN" drill verify --scorecard "$doc" --signature "$sig" --public-key "$pub" \
        >"$tmp/rust.out" 2>"$tmp/rust.err"
    rust_rc=$?
    set -e

    set +e
    "$PY" "$VERIFIER" "$doc" "$sig" "$pub" >"$tmp/py.out" 2>"$tmp/py.err"
    py_rc=$?
    set -e

    if [ "$rust_rc" -ne "$want_rust" ]; then
        cat "$tmp/rust.err" >&2
        fail "$name: drill verify exited $rust_rc, expected $want_rust"
    fi
    if [ "$py_rc" -ne "$want_py" ]; then
        cat "$tmp/py.err" >&2
        fail "$name: verify_scorecard.py exited $py_rc, expected $want_py"
    fi

    cat "$tmp/rust.out" "$tmp/rust.err" >"$tmp/rust.all"
    cat "$tmp/py.out" "$tmp/py.err" >"$tmp/py.all"
    rust_msg="$(approval_line "$tmp/rust.all")"
    py_msg="$(approval_line "$tmp/py.all")"
    if [ -n "$rust_msg" ] || [ -n "$py_msg" ]; then
        if [ "$rust_msg" != "$py_msg" ]; then
            fail "$name: the approval refusal differs between the two readers.
  rust:   $rust_msg
  python: $py_msg"
        fi
    fi
    echo "check-verifier-parity: $name  rust=$rust_rc python=$py_rc  ok"
# A here-string, NOT `echo ... | while`: a pipeline runs the loop body in a
# subshell, where `fail`'s `exit 1` aborts only that subshell. This keeps the
# loop in the script's own shell so a mismatch really does fail `just lint`.
done <<< "$CASES"

echo "check-verifier-parity: both readers agree on all three documents"
