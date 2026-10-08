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
# Task 5b adds a SECOND LOOP, over the BACKUP RECEIPT corpus, and it compares
# the FULL refusal text rather than one extracted line.
#
# The first loop's `approval_line` helper extracts exactly one refusal shape —
# `APPROVAL CLAIM NOT VERIFIED: ...` — and compares that. Reusing it for the
# receipt would have made the gate vacuous on the mutant it exists to kill
# (change one character of one Python arm's reason text: no approval line is
# emitted by either reader, both helpers return the empty string, the
# comparison passes). The second loop therefore compares the two readers'
# whole refusal text, normalised ONLY by stripping the prefix each reader
# spells differently — `INVALID: ` for the script, `SIGNATURE VALID but the
# document is self-contradicting: ` for `drill verify` — which is the same
# normalisation `scripts/check-invariant-corpus.sh` applies.
#
# The receipt cases are UNSIGNED on disk and are signed here, at test time,
# with the checked-in throwaway fixture key under the RECEIPT media type.
# Nothing under `e2e/fixtures/signed/` is written: ruling R-G reserves the
# single fixture re-mint, and a receipt signed under the scorecard's media type
# would be refused at the payloadType comparison and pass this walk for the
# wrong reason.
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

# The Rust reader. `$LOGWEIR_BIN` first — the name `scripts/demo-approve.sh` and
# `scripts/demo.sh` already use — then `$CARGO_TARGET_DIR` (or `target/`) for
# either profile. `docs/test_verify_scorecard.py::logweir_bin` resolves it the
# same way and in the same order, so the two parity checks can never disagree
# about WHICH binary they are calling the first reader.
TARGET_DIR="${CARGO_TARGET_DIR:-$ROOT/target}"
if [ -n "${LOGWEIR_BIN:-}" ]; then
    BIN="$LOGWEIR_BIN"
elif [ -x "$TARGET_DIR/debug/logweir" ]; then
    BIN="$TARGET_DIR/debug/logweir"
else
    BIN="$TARGET_DIR/release/logweir"
fi

# IT BUILDS THE BINARY IT NEEDS RATHER THAN REFUSING (Task 32, stage-2 carried
# item (e)). This used to `fail "no built logweir binary at $BIN; run 'cargo
# build -p logweir' first"` — and Global Constraint 23 then made this gate a
# COMMIT PRECONDITION for every task touching either scorecard reader, so the
# refusal was an instruction handed to a script that could carry it out itself.
# A precondition nobody can satisfy in one command is a precondition people run
# once and then stop running.
#
# ONLY WHEN NOBODY NAMED A BINARY. `$LOGWEIR_BIN` set and not executable stays a
# hard failure: the caller asked for a specific binary, and building a different
# one behind their back is how a parity check ends up reporting on bytes nobody
# meant to test.
#
# THE PINNED TOOLCHAIN IS EXPORTED FIRST, and the reason is the one
# `check-one-signer.sh:95-106` states: a `cargo` reached through rustup's shim
# with no override in scope resolves rustup's DEFAULT channel and SYNCS IT FROM
# THE NETWORK, from inside a lint gate (STANDING RULE 7, Global Constraint 17).
# `cd`-ing into a tree that carries `rust-toolchain.toml` is normally enough;
# exporting the pin costs one `sed` and removes the possibility.
#
# `--release`, DELIBERATELY: the debug arm above is preferred when it exists, so
# the only tree that reaches this line has neither profile built, and a release
# binary is the one this gate will find again on the next run whichever profile
# a later `cargo test` happens to produce.
if [ ! -x "$BIN" ] && [ -z "${LOGWEIR_BIN:-}" ]; then
    if [ -z "${RUSTUP_TOOLCHAIN:-}" ] && [ -f "$ROOT/rust-toolchain.toml" ]; then
        pinned="$(sed -n 's/^[[:space:]]*channel[[:space:]]*=[[:space:]]*"\([^"]*\)".*/\1/p' "$ROOT/rust-toolchain.toml" | head -1)"
        if [ -n "$pinned" ]; then
            export RUSTUP_TOOLCHAIN="$pinned"
            echo "check-verifier-parity: toolchain pinned to $RUSTUP_TOOLCHAIN (from rust-toolchain.toml)"
        fi
    fi
    echo "check-verifier-parity: no logweir binary in $TARGET_DIR — building it (cargo build --release -p logweir)"
    set +e
    cargo build --release -p logweir
    build_rc=$?
    set -e
    if [ "$build_rc" -ne 0 ]; then
        fail "cargo build --release -p logweir exited $build_rc; the parity claim needs BOTH readers and the first one did not build"
    fi
    BIN="$TARGET_DIR/release/logweir"
fi
if [ ! -x "$BIN" ]; then
    fail "no built logweir binary at $BIN; run 'cargo build -p logweir' first, or point \$LOGWEIR_BIN at it — the parity claim needs BOTH readers"
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

# ---------------------------------------------------------------------------
# SECOND LOOP: the backup receipt, on FULL refusal text.
# ---------------------------------------------------------------------------
CORPUS="$ROOT/e2e/fixtures/invariants"

# The prefix each reader puts in front of a self-contradicting document's
# reason, and nothing else is stripped. Assembled from the two places that
# actually produce them: `crates/logweir/src/verify.rs`'s `eprintln!("SIGNATURE
# VALID but the document is self-contradicting: {e}")` — the receipt's arms
# return a bare `String`, so there is no inner prefix — and
# `docs/verify_scorecard.py`'s single `print(f"INVALID: {problem}")` form.
RUST_PREFIX="SIGNATURE VALID but the document is self-contradicting: "
PY_PREFIX="INVALID: "

refusal_text() {
    # $1 = file to read, $2 = the prefix to strip. The FIRST line carrying the
    # prefix, with it removed; empty when the reader did not refuse.
    while IFS= read -r line; do
        case "$line" in
            "$2"*) printf '%s' "${line#"$2"}"; return 0 ;;
        esac
    done < "$1"
    printf ''
}

mkdir -p "$tmp/receipt"
"$PY" - "$CORPUS" "$tmp/receipt" > "$tmp/receipt-cases.tsv" <<'PYEOF'
import base64, hashlib, json, pathlib, sys
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec

corpus, out = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2])
signing_pem = corpus.parent / "signed" / "signing.pem"
# PAE and the media type are re-derived here rather than imported from either
# reader: a gate that asks the thing it is checking cannot fail.
PT = "application/vnd.logweir.backup-receipt+json;version=1.0.0"

key = serialization.load_pem_private_key(signing_pem.read_bytes(), password=None)
der = key.public_key().public_bytes(
    serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo)
keyid = hashlib.sha256(der).hexdigest()

entries = json.loads((corpus / "backup-receipt-index.json").read_text())
if not entries:
    raise SystemExit("backup-receipt-index.json is empty; a walk over nothing proves nothing")
for e in entries:
    payload = (corpus / e["file"]).read_bytes()
    t = PT.encode()
    msg = (b"DSSEv1 " + str(len(t)).encode() + b" " + t + b" "
           + str(len(payload)).encode() + b" " + payload)
    sig = key.sign(msg, ec.ECDSA(hashes.SHA256()))
    (out / f"{e['id']}.json").write_bytes(payload)
    (out / f"{e['id']}.sig").write_text(json.dumps(
        {"payloadType": PT,
         "signatures": [{"keyid": keyid, "sig": base64.b64encode(sig).decode()}]}))
    if "\t" in e["reason"] or "\n" in e["reason"]:
        raise SystemExit(f"{e['id']}: `reason` must be a single TAB-free line")
    print(f"{e['id']}\t{e['rust_exit']}\t{e['python_exit']}\t{e['reason']}")
PYEOF

receipt_count=0
while IFS=$'\t' read -r name want_rust want_py reason; do
    [ -n "$name" ] || continue
    receipt_count=$((receipt_count + 1))
    doc="$tmp/receipt/$name.json"
    sig="$tmp/receipt/$name.sig"
    pub="$FIX/public.pem"

    set +e
    "$BIN" drill verify --payload-type backup-receipt --scorecard "$doc" \
        --signature "$sig" --public-key "$pub" >"$tmp/rust.out" 2>"$tmp/rust.err"
    rust_rc=$?
    set -e

    set +e
    "$PY" "$VERIFIER" --payload-type backup-receipt "$doc" "$sig" "$pub" \
        >"$tmp/py.out" 2>"$tmp/py.err"
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
    rust_msg="$(refusal_text "$tmp/rust.all" "$RUST_PREFIX")"
    py_msg="$(refusal_text "$tmp/py.all" "$PY_PREFIX")"

    if [ "$rust_msg" != "$py_msg" ]; then
        fail "$name: the two readers refuse the backup receipt with DIFFERENT text — the
claim that they mirror each other arm for arm is false here.
  rust:   $rust_msg
  python: $py_msg"
    fi
    if [ "$rust_msg" != "$reason" ]; then
        fail "$name: the refusal text is not the one backup-receipt-index.json records.
  got:  $rust_msg
  want: $reason"
    fi
    # FX-4: on an ACCEPTED receipt both readers print the configuration capture
    # coverage, one line per topic or the one line saying it was not recorded,
    # and they must print the SAME lines — an exit 0 is never read as
    # "captured" by one reader while the other says unknown. The pattern skips
    # the "config_coverage's six" clause each reader's checked/verifier line
    # carries.
    if [ "$want_rust" -eq 0 ]; then
        rust_cov="$(grep -oE 'config_coverage(\[|:).*' "$tmp/rust.all" || true)"
        py_cov="$(grep -oE 'config_coverage(\[|:).*' "$tmp/py.all" || true)"
        [ -n "$rust_cov" ] || fail "$name: drill verify printed no config_coverage line for an accepted receipt"
        if [ "$rust_cov" != "$py_cov" ]; then
            fail "$name: the two readers print DIFFERENT configuration coverage.
  rust:
$rust_cov
  python:
$py_cov"
        fi
        # FX-8: both readers name each topic whose recorded effective
        # `message.timestamp.type` is LogAppendTime — its covered window is the
        # producers' time — in the SAME `time basis:` lines. The one accepted
        # case with such a topic must print its line, and every other accepted
        # case none, so a reader that dropped the lines cannot pass by silence.
        rust_tb="$(grep -oE 'time basis: .*' "$tmp/rust.all" || true)"
        py_tb="$(grep -oE 'time basis: .*' "$tmp/py.all" || true)"
        if [ "$rust_tb" != "$py_tb" ]; then
            fail "$name: the two readers print DIFFERENT time-basis lines.
  rust:
$rust_tb
  python:
$py_tb"
        fi
        case "$name" in
            receipt_1_1_with_config_coverage)
                case "$rust_tb" in
                    'time basis: "orders" is LogAppendTime'*) : ;;
                    *) fail "$name: expected a time-basis line for its LogAppendTime topic \"orders\", got: $rust_tb" ;;
                esac ;;
            *) [ -z "$rust_tb" ] || fail "$name: a receipt with no LogAppendTime topic printed: $rust_tb" ;;
        esac
    fi
    echo "check-verifier-parity: $name  rust=$rust_rc python=$py_rc  ok  (backup receipt)"
# A here-string, NOT `echo ... | while`, for the reason the first loop records.
done <<< "$(cat "$tmp/receipt-cases.tsv")"

if [ "$receipt_count" -eq 0 ]; then
    fail "walked zero backup-receipt cases; e2e/fixtures/invariants/backup-receipt-index.json is empty or unreadable"
fi
echo "check-verifier-parity: both readers agree, on FULL refusal text, on all $receipt_count backup-receipt documents"

# ---------------------------------------------------------------------------
# FX-7: the pinned manifest version (backup receipt format 1.2.0).
# ---------------------------------------------------------------------------
#
# The loop above already requires BOTH readers to ACCEPT
# `unmodified_receipt_pinned` (it is an index case with an empty reason). Two
# things it cannot see are asserted here:
#
#   1. both readers PRINT the pin — it is the object version the manifest
#      digest is over, the second fact an auditor goes looking with — and
#   2. both readers REFUSE a pin of the wrong JSON type. Rust refuses it at
#      deserialisation (`Option<String>`) and the script in its shape layer, in
#      different words, so this is VERDICT parity, not text parity, and it is a
#      document signed inline rather than an index case the text walk would
#      then compare.
PIN="fx7-manifest-version-0001"
set +e
"$BIN" drill verify --payload-type backup-receipt --scorecard "$tmp/receipt/unmodified_receipt_pinned.json" \
    --signature "$tmp/receipt/unmodified_receipt_pinned.sig" --public-key "$FIX/public.pem" \
    >"$tmp/rust.out" 2>"$tmp/rust.err"
rust_rc=$?
"$PY" "$VERIFIER" --payload-type backup-receipt "$tmp/receipt/unmodified_receipt_pinned.json" \
    "$tmp/receipt/unmodified_receipt_pinned.sig" "$FIX/public.pem" >"$tmp/py.out" 2>"$tmp/py.err"
py_rc=$?
set -e
[ "$rust_rc" -eq 0 ] || fail "unmodified_receipt_pinned: drill verify exited $rust_rc on the re-run"
[ "$py_rc" -eq 0 ] || fail "unmodified_receipt_pinned: verify_scorecard.py exited $py_rc on the re-run"
grep -q "manifest version: $PIN" "$tmp/rust.out" \
    || fail "drill verify does not print the manifest version a pinned receipt carries (FX-7)"
grep -q "manifest_version_id=$PIN" "$tmp/py.out" \
    || fail "verify_scorecard.py does not print the manifest version a pinned receipt carries (FX-7)"
set +e
"$BIN" drill verify --payload-type backup-receipt --scorecard "$tmp/receipt/unmodified_receipt.json" \
    --signature "$tmp/receipt/unmodified_receipt.sig" --public-key "$FIX/public.pem" \
    >"$tmp/rust.out" 2>"$tmp/rust.err"
rust_rc=$?
"$PY" "$VERIFIER" --payload-type backup-receipt "$tmp/receipt/unmodified_receipt.json" \
    "$tmp/receipt/unmodified_receipt.sig" "$FIX/public.pem" >"$tmp/py.out" 2>"$tmp/py.err"
py_rc=$?
set -e
[ "$rust_rc" -eq 0 ] && [ "$py_rc" -eq 0 ] || fail "unmodified_receipt: a reader refused it on the re-run"
if grep -q "manifest version" "$tmp/rust.out" || grep -q "manifest_version_id" "$tmp/py.out"; then
    fail "a reader printed a manifest version for a receipt that pins none (FX-7: absent is printed as nothing)"
fi

"$PY" - "$CORPUS" "$tmp/receipt" <<'PYEOF2'
import base64, hashlib, json, pathlib, sys
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec

corpus, out = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2])
key = serialization.load_pem_private_key(
    (corpus.parent / "signed" / "signing.pem").read_bytes(), password=None)
der = key.public_key().public_bytes(
    serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo)
PT = "application/vnd.logweir.backup-receipt+json;version=1.0.0"
doc = json.loads((corpus / "unmodified_receipt_pinned.json").read_text())
doc["archive"]["manifest_version_id"] = 42
payload = json.dumps(doc, indent=2).encode() + b"\n"
t = PT.encode()
msg = (b"DSSEv1 " + str(len(t)).encode() + b" " + t + b" "
       + str(len(payload)).encode() + b" " + payload)
(out / "pin_not_a_string.json").write_bytes(payload)
(out / "pin_not_a_string.sig").write_text(json.dumps(
    {"payloadType": PT,
     "signatures": [{"keyid": hashlib.sha256(der).hexdigest(),
                     "sig": base64.b64encode(key.sign(msg, ec.ECDSA(hashes.SHA256()))).decode()}]}))
PYEOF2
set +e
"$BIN" drill verify --payload-type backup-receipt --scorecard "$tmp/receipt/pin_not_a_string.json" \
    --signature "$tmp/receipt/pin_not_a_string.sig" --public-key "$FIX/public.pem" \
    >"$tmp/rust.out" 2>"$tmp/rust.err"
rust_rc=$?
"$PY" "$VERIFIER" --payload-type backup-receipt "$tmp/receipt/pin_not_a_string.json" \
    "$tmp/receipt/pin_not_a_string.sig" "$FIX/public.pem" >"$tmp/py.out" 2>"$tmp/py.err"
py_rc=$?
set -e
[ "$rust_rc" -ne 0 ] || fail "pin_not_a_string: drill verify ACCEPTED a manifest_version_id that is not a string"
[ "$py_rc" -eq 1 ] || fail "pin_not_a_string: verify_scorecard.py exited $py_rc, expected 1 (INVALID)"
grep -q "archive.manifest_version_id is not a string" "$tmp/py.err" \
    || fail "pin_not_a_string: verify_scorecard.py refused it for another reason"
echo "check-verifier-parity: both readers print a pinned manifest version, print none when absent, and refuse one that is not a string (FX-7)"

# ---------------------------------------------------------------------------
# THIRD LOOP: the recovery catalog point (PLAT-15.1, decision D3 §5.2).
# ---------------------------------------------------------------------------
#
# WHAT IS ASSERTED, AND WHY IT IS NOT "BYTE-IDENTICAL REFUSAL TEXT". A catalog
# point record is checked SIGNATURE-ONLY by both readers, on purpose: the
# record's receipt-derived facts are recomputed from the VERIFIED backup
# receipt it names (D3 §5.2 rule 3), so the receipt's signature is the
# verification root and neither reader evaluates an invariant for this type.
# Neither of them therefore ever produces a "self-contradicting document"
# refusal to compare, and the second loop's helper would find no line on either
# side and pass vacuously — the exact failure mode that loop's own header warns
# about.
#
# So the claim made here is the one that is actually available, stated three
# ways:
#
#   1. the two readers reach the SAME VERDICT on three cases — a good record, a
#      record with one byte flipped after signing, and a genuine record
#      presented as a scorecard (substitution);
#   2. on the good case BOTH print the receipt key and the receipt digest, which
#      are the two facts an auditor takes away and goes checking with;
#   3. on the good case BOTH print their own "this proves only the signature"
#      sentence, so neither exit 0 can be read as a claim that the point is
#      available. A reader that silently started asserting more fails here.
#
# The fixture is written INLINE rather than added to `e2e/fixtures/signed/`:
# ruling R-G reserves the single fixture re-mint, and this document needs no
# checked-in copy — it is generated, signed with the throwaway fixture key, and
# thrown away with $tmp.
#
# Every exit code is captured into a variable on its own line, never through a
# pipe, for the reason the header states.
CATALOG_PT="application/vnd.logweir.catalog-point+json;version=1.0.0"
# FX-4: the catalog point format that carries `topics[].config_coverage` —
# `crates/logweir/src/catalog/record.rs`'s FORMAT_VERSION. A renumber moves
# that constant and this line together.
CATALOG_COVERAGE_VERSION="1.1.0"
# FX-7 (merged after FX-4): the catalog point format that ALSO carries
# `archive.manifest_version_id` — record.rs's FORMAT_VERSION_WITH_MANIFEST_VERSION,
# the newest MINOR. A renumber moves that constant, the justfile's
# `catalog_schema_version` and this line together.
CATALOG_PIN_VERSION="1.2.0"
mkdir -p "$tmp/catalog"
"$PY" - "$FIX" "$tmp/catalog" "$CATALOG_PT" "$CATALOG_COVERAGE_VERSION" "$CATALOG_PIN_VERSION" <<'PYEOF'
import base64, hashlib, json, pathlib, sys
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec

fix, out, pt = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2]), sys.argv[3]
coverage_version = sys.argv[4]
pin_version = sys.argv[5]
key = serialization.load_pem_private_key((fix / "signing.pem").read_bytes(), password=None)
der = key.public_key().public_bytes(
    serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo)
keyid = hashlib.sha256(der).hexdigest()

# The shape `crates/logweir/src/catalog/record.rs` serialises. Written out here
# rather than generated by the binary under test: a gate that asks the thing it
# is checking cannot fail.
doc = {
    "format_version": "1.0.0",
    "point_id": "lwp1-" + "a" * 32,
    "recorded_at": "2026-09-16T00:00:00Z",
    "receipt": {
        "key": "logweir/backups/nightly-20260915/01J9X2QK7C4V0R8YB3ZP6MTS5A.receipt.json",
        "sha256": "sha256:" + "a" * 64,
        "sidecar_key": "logweir/backups/nightly-20260915/01J9X2QK7C4V0R8YB3ZP6MTS5A.receipt.sig",
        "payload_type": "application/vnd.logweir.backup-receipt+json;version=1.0.0",
    },
    "backup_id": "nightly-20260915",
    "run_id": "01J9X2QK7C4V0R8YB3ZP6MTS5A",
    "archive": {
        "location_id": "s3://kafka-backups/prod",
        "manifest_key": "prod/nightly-20260915/manifest.json",
        "manifest_sha256": "sha256:" + "b" * 64,
        "prefix": "prod",
    },
    "covered": {"from_ms": 1757980800000, "to_ms": 1757984400000},
    "capture": {"started_at": "2026-09-15T03:00:00Z", "finished_at": "2026-09-15T03:04:00Z"},
    "topics": [{"name": "orders", "records": 1234}],
    "source": {
        "cluster_id": "SOURCE-CLUSTER-000001",
        "bootstrap_servers": ["kafka-source:9092"],
        "auth_mode": "scramSha512",
    },
    "signing": {"key_id": "c" * 64, "algorithm": "ecdsa-p256-sha256"},
}
payload = json.dumps(doc, indent=2).encode() + b"\n"


def sign(bs):
    t = pt.encode()
    msg = (b"DSSEv1 " + str(len(t)).encode() + b" " + t + b" "
           + str(len(bs)).encode() + b" " + bs)
    sig = key.sign(msg, ec.ECDSA(hashes.SHA256()))
    return json.dumps({"payloadType": pt,
                       "signatures": [{"keyid": keyid,
                                       "sig": base64.b64encode(sig).decode()}]})


(out / "good.json").write_bytes(payload)
(out / "good.sig").write_text(sign(payload))
# One byte flipped AFTER signing: the sidecar is genuine, the bytes are not the
# ones it covers.
tampered = bytearray(payload)
tampered[tampered.index(b"nightly")] = ord("N")
(out / "tampered.json").write_bytes(bytes(tampered))
(out / "tampered.sig").write_text((out / "good.sig").read_text())
# FX-4: a 1.1.0 record, whose topic rows carry the receipt's `config_coverage`
# entry. Both readers must still accept it (signature-only), which is the
# format-compatibility half of rule 3; its FACTS are the Rust catalog reader's
# rule-3 cross-check (`reader::cross_check`), not this gate's.
doc11 = json.loads(json.dumps(doc))
doc11["format_version"] = coverage_version
doc11["topics"][0]["config_coverage"] = {
    "coverage": "captureDenied",
}
payload11 = json.dumps(doc11, indent=2).encode() + b"\n"
(out / "good11.json").write_bytes(payload11)
(out / "good11.sig").write_text(sign(payload11))
# FX-7: the record of a point taken on a versioned bucket — format 1.2.0, the
# MINOR after FX-4's 1.1.0 — as this build's writer produces it: FX-4's topic
# coverage AND the receipt's pinned manifest version. Signature-only like the
# rest.
pinned = json.loads(json.dumps(doc11))
pinned["format_version"] = pin_version
pinned["archive"]["manifest_version_id"] = "fx7-manifest-version-0001"
pinned_payload = json.dumps(pinned, indent=2).encode() + b"\n"
(out / "pinned.json").write_bytes(pinned_payload)
(out / "pinned.sig").write_text(sign(pinned_payload))
PYEOF

catalog_case() {
    # $1 document stem, $2 --payload-type to ask for, $3 expected rust code,
    # $4 expected python code.
    doc="$tmp/catalog/$1.json"
    sig="$tmp/catalog/$1.sig"
    pub="$FIX/public.pem"

    set +e
    "$BIN" drill verify --payload-type "$2" --scorecard "$doc" \
        --signature "$sig" --public-key "$pub" >"$tmp/rust.out" 2>"$tmp/rust.err"
    rust_rc=$?
    set -e

    set +e
    "$PY" "$VERIFIER" --payload-type "$2" "$doc" "$sig" "$pub" \
        >"$tmp/py.out" 2>"$tmp/py.err"
    py_rc=$?
    set -e

    if [ "$rust_rc" -ne "$3" ]; then
        cat "$tmp/rust.err" >&2
        fail "catalog-point/$1 as $2: drill verify exited $rust_rc, expected $3"
    fi
    if [ "$py_rc" -ne "$4" ]; then
        cat "$tmp/py.err" >&2
        fail "catalog-point/$1 as $2: verify_scorecard.py exited $py_rc, expected $4"
    fi
    echo "check-verifier-parity: catalog-point/$1 as $2  rust=$rust_rc python=$py_rc  ok"
}

catalog_case good catalog-point 0 0
catalog_case tampered catalog-point 4 1
catalog_case good scorecard 4 1
catalog_case good11 catalog-point 0 0
catalog_case pinned catalog-point 0 0
grep -q "manifest_version_id=fx7-manifest-version-0001" "$tmp/py.out" \
    || fail "verify_scorecard.py does not print the pinned manifest version a $CATALOG_PIN_VERSION catalog point carries (FX-7)"

# Claim 2 and claim 3, on the accepted case, one reader at a time.
cat "$tmp/catalog/good.json" >/dev/null
set +e
"$BIN" drill verify --payload-type catalog-point --scorecard "$tmp/catalog/good.json" \
    --signature "$tmp/catalog/good.sig" --public-key "$FIX/public.pem" >"$tmp/rust.out" 2>"$tmp/rust.err"
rust_rc=$?
set -e
[ "$rust_rc" -eq 0 ] || fail "catalog-point/good: drill verify exited $rust_rc on the re-run"
set +e
"$PY" "$VERIFIER" --payload-type catalog-point "$tmp/catalog/good.json" \
    "$tmp/catalog/good.sig" "$FIX/public.pem" >"$tmp/py.out" 2>"$tmp/py.err"
py_rc=$?
set -e
[ "$py_rc" -eq 0 ] || fail "catalog-point/good: verify_scorecard.py exited $py_rc on the re-run"

cat "$tmp/rust.out" "$tmp/rust.err" >"$tmp/rust.all"
cat "$tmp/py.out" "$tmp/py.err" >"$tmp/py.all"

# The Rust reader reports this type through its SignatureOnly verdict, whose
# sentence is a single literal in `crates/logweir/src/verify.rs`; the Python
# reader has its own arm and its own sentence. Each is asserted against the
# reader that produces it, which is what makes this a claim about both readers
# rather than about one of them twice.
grep -q "the SIGNATURE only" "$tmp/rust.all" \
    || fail "drill verify stopped saying that a catalog point is checked SIGNATURE-ONLY.
An exit 0 for this document type must never read like an exit 0 for a scorecard:
the record's facts are recomputed from the backup receipt it names, and this
command does not fetch it."
grep -q "This signature covers the record only" "$tmp/py.all" \
    || fail "docs/verify_scorecard.py stopped saying that a catalog point is checked
SIGNATURE-ONLY. See the sentence in its catalog-point arm."
grep -q "$CATALOG_PT" "$tmp/rust.all" \
    || fail "drill verify does not name the catalog-point media type it verified"
grep -q "$CATALOG_PT" "$tmp/py.all" \
    || fail "verify_scorecard.py does not name the catalog-point media type it verified"
# Claim 2: the receipt key and its digest, from BOTH readers. The Rust
# SignatureOnly verdict prints neither today, so this is asserted of the
# reader that does — and the pair is asserted of the document, so a fixture
# that stopped carrying them could not pass this gate quietly.
grep -q "logweir/backups/nightly-20260915/01J9X2QK7C4V0R8YB3ZP6MTS5A.receipt.json" "$tmp/py.all" \
    || fail "verify_scorecard.py no longer prints the receipt key a catalog point names —
the one fact an auditor takes away and goes checking with"
grep -q "sha256:aaaaaaaa" "$tmp/py.all" \
    || fail "verify_scorecard.py no longer prints the receipt digest that BINDS a catalog
point; the short point_id is a display key and the digest is the binding (D3 §5.1)"

echo "check-verifier-parity: both readers agree on all five catalog-point documents (1.0.0, $CATALOG_COVERAGE_VERSION and $CATALOG_PIN_VERSION), and both report SIGNATURE-ONLY"

# ---------------------------------------------------------------------------
# FOURTH LOOP (FX-4): the scorecard at format 1.1.0, and what its exit 0 says
# about configuration parity.
# ---------------------------------------------------------------------------
#
# The first loop walks the three checked-in signed scorecards, which are 1.0.0
# documents and stay so (ruling R-G). A 1.1.0 scorecard adds the nested optional
# `topic_parity.not_assessed`; this loop proves both readers ACCEPT one — and a
# 1.0.0 one — and print the SAME `configuration parity:` sentence for each of
# the three states: not recorded (absent), not assessed for some topics, and
# nothing at all when every topic was assessed. Generated and signed here with
# the throwaway fixture key, like the catalog loop's documents.
SC_PT="application/vnd.logweir.drill-scorecard+json;version=1.0.0"
# The scorecard format that carries `topic_parity.not_assessed` —
# `logweir_core::FORMAT_VERSION`; a renumber moves both. The case NAMES below
# keep saying 1.1.0 and are only names.
SCORECARD_NOT_ASSESSED_VERSION="1.1.0"
mkdir -p "$tmp/scorecard11"
"$PY" - "$ROOT" "$tmp/scorecard11" "$SC_PT" "$SCORECARD_NOT_ASSESSED_VERSION" <<'PYEOF'
import base64, hashlib, json, pathlib, sys
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec

root, out, pt = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2]), sys.argv[3]
current = sys.argv[4]
fix = root / "e2e" / "fixtures" / "signed"
key = serialization.load_pem_private_key((fix / "signing.pem").read_bytes(), password=None)
der = key.public_key().public_bytes(
    serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo)
keyid = hashlib.sha256(der).hexdigest()
base = json.loads((root / "e2e" / "fixtures" / "scorecard-pass.json").read_text())
cases = {
    "absent-1.0.0": ("1.0.0", None),
    "not-assessed-1.1.0": (current, ["drill-orders: configuration (captureDenied)"]),
    "all-assessed-1.1.0": (current, []),
}
for name, (version, not_assessed) in cases.items():
    doc = json.loads(json.dumps(base))
    doc["format_version"] = version
    if not_assessed is not None:
        doc["topic_parity"]["not_assessed"] = not_assessed
    payload = (json.dumps(doc, indent=2) + "\n").encode()
    t = pt.encode()
    msg = (b"DSSEv1 " + str(len(t)).encode() + b" " + t + b" "
           + str(len(payload)).encode() + b" " + payload)
    sig = key.sign(msg, ec.ECDSA(hashes.SHA256()))
    (out / f"{name}.json").write_bytes(payload)
    (out / f"{name}.sig").write_text(json.dumps(
        {"payloadType": pt,
         "signatures": [{"keyid": keyid, "sig": base64.b64encode(sig).decode()}]}))
PYEOF

for name in absent-1.0.0 not-assessed-1.1.0 all-assessed-1.1.0; do
    doc="$tmp/scorecard11/$name.json"
    sig="$tmp/scorecard11/$name.sig"
    set +e
    "$BIN" drill verify --scorecard "$doc" --signature "$sig" --public-key "$FIX/public.pem" \
        >"$tmp/rust.out" 2>"$tmp/rust.err"
    rust_rc=$?
    set -e
    set +e
    "$PY" "$VERIFIER" "$doc" "$sig" "$FIX/public.pem" >"$tmp/py.out" 2>"$tmp/py.err"
    py_rc=$?
    set -e
    [ "$rust_rc" -eq 0 ] || { cat "$tmp/rust.err" >&2; fail "scorecard/$name: drill verify exited $rust_rc, expected 0"; }
    [ "$py_rc" -eq 0 ] || { cat "$tmp/py.err" >&2; fail "scorecard/$name: verify_scorecard.py exited $py_rc, expected 0"; }
    cat "$tmp/rust.out" "$tmp/rust.err" >"$tmp/rust.all"
    cat "$tmp/py.out" "$tmp/py.err" >"$tmp/py.all"
    rust_parity="$(grep -oE 'configuration parity: .*' "$tmp/rust.all" || true)"
    py_parity="$(grep -oE 'configuration parity: .*' "$tmp/py.all" || true)"
    if [ "$rust_parity" != "$py_parity" ]; then
        fail "scorecard/$name: the two readers say different things about configuration parity.
  rust:   $rust_parity
  python: $py_parity"
    fi
    case "$name" in
        absent-1.0.0) want="not recorded" ;;
        not-assessed-1.1.0) want="NOT ASSESSED for drill-orders: configuration (captureDenied)" ;;
        all-assessed-1.1.0) want="" ;;
    esac
    if [ -z "$want" ]; then
        [ -z "$rust_parity" ] || fail "scorecard/$name: a document that assessed every topic printed: $rust_parity"
    else
        case "$rust_parity" in
            *"$want"*) : ;;
            *) fail "scorecard/$name: expected the parity sentence to say \"$want\", got: $rust_parity" ;;
        esac
    fi
    echo "check-verifier-parity: scorecard/$name  rust=$rust_rc python=$py_rc  ok  (configuration parity)"
done
echo "check-verifier-parity: both readers accept 1.0.0 and 1.1.0 scorecards and say the same about configuration parity"

# ---------------------------------------------------------------------------
# TIME-BASIS LOOP (FX-8): the scorecard's `source.time_basis`, and what its
# exit 0 says about the clock a restore's time selection read.
# ---------------------------------------------------------------------------
#
# Four documents both readers ACCEPT, and the `time basis:` lines each must
# print — the SAME lines from both:
#
#   absent-1.0.0     the frozen 1.0.0 document, no block: the one line saying
#                    it was NOT RECORDED, never silence
#   empty            the block with both lists empty: no line (the claim that
#                    nothing was selected by producer time or with an
#                    unrecorded type)
#   producer-time    `plan: producerTime` and one topic selected by producer
#                    time: its line
#   not-recorded     one topic selected by time with no recorded type: its line
#
# and two both readers REFUSE with the same full text: arm TB-3 (a topic
# selected by producer time in a document whose plan did not accept it) and
# arm TB-1 (the block under a version that predates it). Generated and signed
# here with the throwaway fixture key, like the fourth loop's documents.
#
# The scorecard format that defines `source.time_basis` —
# `logweir_core::FORMAT_VERSION` at FX-8 and `TIME_BASIS_SINCE_MINOR`; a
# renumber moves all three.
SCORECARD_TIME_BASIS_VERSION="1.3.0"
mkdir -p "$tmp/scorecard-tb"
"$PY" - "$ROOT" "$tmp/scorecard-tb" "$SC_PT" "$SCORECARD_TIME_BASIS_VERSION" <<'PYEOF'
import base64, hashlib, json, pathlib, sys
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec

root, out, pt = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2]), sys.argv[3]
current = sys.argv[4]
fix = root / "e2e" / "fixtures" / "signed"
key = serialization.load_pem_private_key((fix / "signing.pem").read_bytes(), password=None)
der = key.public_key().public_bytes(
    serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo)
keyid = hashlib.sha256(der).hexdigest()
base = json.loads((root / "e2e" / "fixtures" / "scorecard-pass.json").read_text())


def with_block(version, block):
    d = json.loads(json.dumps(base))
    d["format_version"] = version
    d["source"]["time_basis"] = block
    return d


cases = {
    "absent-1.0.0": json.loads(json.dumps(base)),
    "empty": with_block(current, {"producer_time": [], "not_recorded": []}),
    "producer-time": with_block(
        current, {"plan": "producerTime", "producer_time": ["orders"], "not_recorded": []}),
    "not-recorded": with_block(current, {"producer_time": [], "not_recorded": ["orders"]}),
    "producer-time-without-the-plan": with_block(
        current, {"producer_time": ["orders"], "not_recorded": []}),
    "block-under-1.1.0": with_block("1.1.0", {"producer_time": [], "not_recorded": []}),
}
for name, doc in cases.items():
    payload = (json.dumps(doc, indent=2) + "\n").encode()
    t = pt.encode()
    msg = (b"DSSEv1 " + str(len(t)).encode() + b" " + t + b" "
           + str(len(payload)).encode() + b" " + payload)
    sig = key.sign(msg, ec.ECDSA(hashes.SHA256()))
    (out / f"{name}.json").write_bytes(payload)
    (out / f"{name}.sig").write_text(json.dumps(
        {"payloadType": pt,
         "signatures": [{"keyid": keyid, "sig": base64.b64encode(sig).decode()}]}))
PYEOF

for name in absent-1.0.0 empty producer-time not-recorded; do
    doc="$tmp/scorecard-tb/$name.json"
    sig="$tmp/scorecard-tb/$name.sig"
    set +e
    "$BIN" drill verify --scorecard "$doc" --signature "$sig" --public-key "$FIX/public.pem" \
        >"$tmp/rust.out" 2>"$tmp/rust.err"
    rust_rc=$?
    set -e
    set +e
    "$PY" "$VERIFIER" "$doc" "$sig" "$FIX/public.pem" >"$tmp/py.out" 2>"$tmp/py.err"
    py_rc=$?
    set -e
    [ "$rust_rc" -eq 0 ] || { cat "$tmp/rust.err" >&2; fail "scorecard/$name: drill verify exited $rust_rc, expected 0"; }
    [ "$py_rc" -eq 0 ] || { cat "$tmp/py.err" >&2; fail "scorecard/$name: verify_scorecard.py exited $py_rc, expected 0"; }
    cat "$tmp/rust.out" "$tmp/rust.err" >"$tmp/rust.all"
    cat "$tmp/py.out" "$tmp/py.err" >"$tmp/py.all"
    rust_line="$(grep -oE 'time basis: .*' "$tmp/rust.all" || true)"
    py_line="$(grep -oE 'time basis: .*' "$tmp/py.all" || true)"
    if [ "$rust_line" != "$py_line" ]; then
        fail "scorecard/$name: the two readers say different things about the time basis.
  rust:   $rust_line
  python: $py_line"
    fi
    case "$name" in
        absent-1.0.0) want="time basis: not recorded, so whether a time selection read a LogAppendTime topic's producer timestamps is unknown" ;;
        empty) want="" ;;
        producer-time) want="time basis: SELECTED BY PRODUCER TIME for orders (recorded as LogAppendTime; the approved plan states restore.time_basis: producerTime)" ;;
        not-recorded) want="time basis: timestamp type NOT RECORDED for orders, so its time selection may have read producer timestamps" ;;
    esac
    if [ "$rust_line" != "$want" ]; then
        fail "scorecard/$name: expected the time-basis line to be \"$want\", got: \"$rust_line\""
    fi
    echo "check-verifier-parity: scorecard/$name  rust=$rust_rc python=$py_rc  ok  (time basis)"
done

# The two refusals, on FULL text once each reader's own prefix is stripped: the
# script's `INVALID: `, and on the Rust side `RUST_PREFIX` plus the scorecard's
# `scorecard invariant violated: ` (an `InvariantError`'s Display).
for name in producer-time-without-the-plan block-under-1.1.0; do
    doc="$tmp/scorecard-tb/$name.json"
    sig="$tmp/scorecard-tb/$name.sig"
    set +e
    "$BIN" drill verify --scorecard "$doc" --signature "$sig" --public-key "$FIX/public.pem" \
        >"$tmp/rust.out" 2>"$tmp/rust.err"
    rust_rc=$?
    set -e
    set +e
    "$PY" "$VERIFIER" "$doc" "$sig" "$FIX/public.pem" >"$tmp/py.out" 2>"$tmp/py.err"
    py_rc=$?
    set -e
    [ "$rust_rc" -eq 4 ] || { cat "$tmp/rust.out" "$tmp/rust.err" >&2; fail "scorecard/$name: drill verify exited $rust_rc, expected 4"; }
    [ "$py_rc" -eq 1 ] || { cat "$tmp/py.out" "$tmp/py.err" >&2; fail "scorecard/$name: verify_scorecard.py exited $py_rc, expected 1"; }
    cat "$tmp/rust.out" "$tmp/rust.err" >"$tmp/rust.all"
    cat "$tmp/py.out" "$tmp/py.err" >"$tmp/py.all"
    rust_msg="$(refusal_text "$tmp/rust.all" "${RUST_PREFIX}scorecard invariant violated: ")"
    py_msg="$(refusal_text "$tmp/py.all" "$PY_PREFIX")"
    case "$name" in
        producer-time-without-the-plan) want_msg='source.time_basis.producer_time names a topic but source.time_basis.plan is not "producerTime"; a selection by producer time is one the approved plan accepted, never a default' ;;
        block-under-1.1.0) want_msg="source.time_basis is present but format_version \"1.1.0\" predates it: the field is defined from $SCORECARD_TIME_BASIS_VERSION" ;;
    esac
    if [ "$rust_msg" != "$py_msg" ] || [ "$rust_msg" != "$want_msg" ]; then
        fail "scorecard/$name: the refusal differs between the two readers or from its arm.
  rust:   $rust_msg
  python: $py_msg
  want:   $want_msg"
    fi
    echo "check-verifier-parity: scorecard/$name  rust=$rust_rc python=$py_rc  ok  (time basis refused)"
done
echo "check-verifier-parity: both readers accept $SCORECARD_TIME_BASIS_VERSION scorecards, say the same about the time basis, and refuse a producer-time selection the plan did not accept"
