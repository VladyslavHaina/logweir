#!/usr/bin/env bash
# The auditor-side half of the two-reader parity gate (Task 4, T0-6 / T0-3).
#
# `crates/logweir/tests/two_reader_parity.rs` walks
# `e2e/fixtures/invariants/index.json` with BOTH readers and needs cargo. This
# script walks the same index with the SECOND reader alone, so the corpus is
# still checked on a machine that has an auditor's python3 and no Rust
# toolchain — which is exactly the machine `docs/verify-a-scorecard.md` is
# written for. Task 5 inherits it unchanged: it adds cases to `index.json`, not
# to this file.
#
# The corpus cases are UNSIGNED on disk (see e2e/fixtures/invariants/README.md).
# `verify_scorecard.py` checks the signature before it evaluates any invariant,
# so each case is signed here, at test time, into a temp dir with the checked-in
# throwaway fixture key. Nothing under e2e/fixtures/signed/ is written: ruling
# R-G reserves the single fixture re-mint to Task 2.
#
# Every exit code is captured into a variable on its own line. NEVER through a
# pipe: `cmd | grep` reports grep's status, and a verifier whose failure is
# invisible is worse than no verifier.
set -euo pipefail

cd "$(dirname "$0")/.."
ROOT="$PWD"
CORPUS="$ROOT/e2e/fixtures/invariants"
VERIFIER="$ROOT/docs/verify_scorecard.py"

# Interpreter resolution: $LOGWEIR_PYTHON, then $LOGWEIR_E2E_PYTHON, then
# .e2e/venv/bin/python3, then python3 — the same names in the same order as
# scripts/check-verifier-parity.sh, e2e/tests/harness/mod.rs::python and
# crates/logweir/tests/two_reader_parity.rs::python. All four agree, so no two
# gates can check the two-reader claim against different second readers.
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
    echo "check-invariant-corpus: $*" >&2
    exit 1
}

# NOT skipped when `cryptography` is missing, for the same reason
# check-verifier-parity.sh does not skip: an unchecked parity claim with
# nothing saying it went unchecked is the defect this work exists to remove.
set +e
"$PY" -c 'import cryptography' >/dev/null 2>&1
probe_rc=$?
set -e
if [ "$probe_rc" -ne 0 ]; then
    fail "python3 with the 'cryptography' package is required (pip install cryptography); the invariant corpus cannot be signed without it. tried $PY — set \$LOGWEIR_PYTHON (or \$LOGWEIR_E2E_PYTHON, or create .e2e/venv) to point at an interpreter that has it"
fi

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

# Sign every case into $tmp and emit one TAB-separated line per case:
#   id <TAB> python_exit <TAB> reason
# PAE is re-derived from the spec here rather than imported from the verifier,
# so a bug in the verifier's own `pae()` cannot make this check agree with
# itself.
"$PY" - "$CORPUS" "$tmp" > "$tmp/cases.tsv" <<'PYEOF'
import base64, hashlib, json, pathlib, sys
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec

corpus, out = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2])
signing_pem = corpus.parent / "signed" / "signing.pem"
PT = "application/vnd.logweir.drill-scorecard+json;version=1.0.0"

key = serialization.load_pem_private_key(signing_pem.read_bytes(), password=None)
der = key.public_key().public_bytes(
    serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo)
keyid = hashlib.sha256(der).hexdigest()

entries = json.loads((corpus / "index.json").read_text())
if not entries:
    raise SystemExit("index.json is empty; a walk over nothing proves nothing")
# Every case is signed into ONE shared temp dir keyed by `id`, so a duplicate
# id would silently overwrite an earlier case's files and then compare the
# WRONG document against that earlier case's expected reason — a green walk
# over a case that was never run. Task 5 adds cases, so this fails loudly
# rather than staying latent.
ids = [e["id"] for e in entries]
dupes = sorted({i for i in ids if ids.count(i) > 1})
if dupes:
    raise SystemExit(f"index.json has duplicate id(s): {dupes}; every id must be unique")
for e in entries:
    # The signed payload is the file's bytes EXACTLY as written. Nothing is
    # re-serialised, or the reader would verify different bytes.
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
    print(f"{e['id']}\t{e['python_exit']}\t{e['reason']}")
PYEOF

count=0
while IFS=$'\t' read -r id want_py reason; do
    [ -n "$id" ] || continue
    count=$((count + 1))

    set +e
    "$PY" "$VERIFIER" "$tmp/$id.json" "$tmp/$id.sig" "$ROOT/e2e/fixtures/signed/public.pem" \
        >"$tmp/out" 2>"$tmp/err"
    py_rc=$?
    set -e

    if [ "$py_rc" -ne "$want_py" ]; then
        cat "$tmp/err" >&2
        fail "$id: verify_scorecard.py exited $py_rc, index.json expects $want_py"
    fi

    # The refusal text, with this reader's own `INVALID: ` prefix stripped.
    got=""
    while IFS= read -r line; do
        case "$line" in
            "INVALID: "*) got="${line#INVALID: }"; break ;;
        esac
    done < "$tmp/err"

    if [ "$got" != "$reason" ]; then
        fail "$id: the refusal text is not the one index.json records.
  got:  $got
  want: $reason"
    fi
    echo "check-invariant-corpus: $id  python=$py_rc  ok"
# A here-string, NOT `... | while`: a pipeline runs the loop body in a
# subshell, where `fail`'s `exit 1` aborts only that subshell and `just lint`
# goes green over a real mismatch.
done <<< "$(cat "$tmp/cases.tsv")"

if [ "$count" -eq 0 ]; then
    fail "walked zero cases; e2e/fixtures/invariants/index.json is empty or unreadable"
fi
echo "check-invariant-corpus: the auditor's verifier agrees with index.json on all $count cases"
