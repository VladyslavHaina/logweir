import json, pathlib, subprocess, sys, tempfile

ROOT = pathlib.Path(__file__).resolve().parents[1]
FIX = ROOT / "e2e" / "fixtures" / "signed"
VERIFIER = ROOT / "docs" / "verify_scorecard.py"


def run(sc, sig, pub):
    return subprocess.run(
        [sys.executable, str(VERIFIER), str(sc), str(sig), str(pub)],
        capture_output=True, text=True,
    )


def test_valid_scorecard_verifies():
    r = run(FIX / "scorecard.json", FIX / "scorecard.sig", FIX / "public.pem")
    assert r.returncode == 0, r.stderr
    assert "VALID" in r.stdout


def test_one_flipped_byte_fails():
    with tempfile.TemporaryDirectory() as d:
        bad = pathlib.Path(d) / "bad.json"
        raw = bytearray((FIX / "scorecard.json").read_bytes())
        raw[raw.index(b"1")] = ord("2")
        bad.write_bytes(bytes(raw))
        r = run(bad, FIX / "scorecard.sig", FIX / "public.pem")
        assert r.returncode == 1
        assert "INVALID" in r.stdout + r.stderr


def test_self_attested_is_reported():
    r = run(FIX / "scorecard-self-attested.json",
            FIX / "scorecard-self-attested.sig", FIX / "public.pem")
    assert r.returncode == 0
    assert "SELF-ATTESTED" in r.stdout


def test_missing_scorecard_file_fails_cleanly():
    r = run(FIX / "does-not-exist.json", FIX / "scorecard.sig", FIX / "public.pem")
    assert r.returncode == 1
    assert "Traceback" not in r.stderr
    assert "INVALID" in r.stdout + r.stderr


def test_missing_public_key_file_fails_cleanly():
    r = run(FIX / "scorecard.json", FIX / "scorecard.sig", FIX / "no-such-key.pem")
    assert r.returncode == 1
    assert "Traceback" not in r.stderr
    assert "INVALID" in r.stdout + r.stderr


def test_malformed_pem_fails_cleanly():
    with tempfile.TemporaryDirectory() as d:
        bad_pem = pathlib.Path(d) / "garbage.pem"
        bad_pem.write_bytes(b"this is not a PEM file at all\n")
        r = run(FIX / "scorecard.json", FIX / "scorecard.sig", bad_pem)
        assert r.returncode == 1
        assert "Traceback" not in r.stderr
        assert "INVALID" in r.stdout + r.stderr


def test_unsupported_key_type_fails_cleanly():
    # An RSA key is not one of the two key types this format defines
    # (ECDSA P-256, Ed25519). It must be rejected as unsupported, not
    # crash trying to run EC verification against it.
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.hazmat.primitives.serialization import (
        Encoding, PublicFormat,
    )

    with tempfile.TemporaryDirectory() as d:
        rsa_pem = pathlib.Path(d) / "rsa.pem"
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        rsa_pem.write_bytes(
            key.public_key().public_bytes(Encoding.PEM, PublicFormat.SubjectPublicKeyInfo)
        )
        r = run(FIX / "scorecard.json", FIX / "scorecard.sig", rsa_pem)
        assert r.returncode == 1
        assert "Traceback" not in r.stderr
        assert "unsupported public key type" in r.stdout + r.stderr


# ---------------------------------------------------------------- --payload-type
# Task 22, carried obligation 3. A drill publishes three signed documents, and
# before this flag existed the shipped verifier could check exactly one of
# them: it pinned `payloadType` to the scorecard's, so an auditor handed a
# `<run_id>.receipt.json` had no tool to verify it with and had to hand-roll
# PAE — which is how a signature that verifies nothing gets built.

def run_typed(kind, doc, sig, pub):
    return subprocess.run(
        [sys.executable, str(VERIFIER), "--payload-type", kind,
         str(doc), str(sig), str(pub)],
        capture_output=True, text=True,
    )


RECEIPT_TYPE = "application/vnd.logweir.drill-put-receipt+json;version=1.0.0"
TEARDOWN_TYPE = "application/vnd.logweir.drill-teardown+json;version=1.0.0"
SCORECARD_TYPE = "application/vnd.logweir.drill-scorecard+json;version=1.0.0"


def _sign(payload_type: str, payload: bytes) -> dict:
    """A DSSE sidecar over PAE(payload_type, payload), using the checked-in
    THROWAWAY fixture key. Written here in Python on purpose: it re-derives
    PAE from the spec rather than importing the verifier's own `pae()`, so a
    bug in that function cannot make these tests agree with themselves."""
    import base64 as _b64
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec

    key = serialization.load_pem_private_key(
        (FIX / "signing.pem").read_bytes(), password=None
    )
    t = payload_type.encode()
    message = b"DSSEv1 " + str(len(t)).encode() + b" " + t + b" " \
        + str(len(payload)).encode() + b" " + payload
    sig = key.sign(message, ec.ECDSA(hashes.SHA256()))
    return {"payloadType": payload_type,
            "signatures": [{"keyid": "fixture", "sig": _b64.b64encode(sig).decode()}]}


def _write_signed(d, name, payload_type, doc):
    payload = json.dumps(doc, indent=2).encode() + b"\n"
    p = pathlib.Path(d) / f"{name}.json"
    s = pathlib.Path(d) / f"{name}.sig"
    p.write_bytes(payload)
    s.write_text(json.dumps(_sign(payload_type, payload)))
    return p, s


RECEIPT = {
    "run_id": "01J9X2QK7C4V0R8YB3ZP6MTS5A",
    "scorecard_sha256": "sha256:" + "9" * 64,
    "scorecard_key": "logweir/drills/01J9X2QK7C4V0R8YB3ZP6MTS5A.json",
    "create_only_enforced": True,
    "version_id": None,
    "immutable": False,
    "retain_until": None,
    "observed_at": "2026-09-03T09:09:04Z",
}


def test_a_put_receipt_verifies_under_its_own_payload_type():
    with tempfile.TemporaryDirectory() as d:
        p, s = _write_signed(d, "receipt", RECEIPT_TYPE, RECEIPT)
        r = run_typed("receipt", p, s, FIX / "public.pem")
        assert r.returncode == 0, r.stderr
        assert "VALID" in r.stdout
        # The binding to the scorecard is the receipt's whole purpose, so the
        # tool must print it rather than leaving the reader to open the file.
        assert RECEIPT["scorecard_sha256"] in r.stdout
        assert RECEIPT["scorecard_key"] in r.stdout


def test_a_receipt_never_verifies_as_a_scorecard():
    # The default path must keep REFUSING a receipt: that refusal is the
    # property the payload type exists to give, and adding the flag must not
    # have weakened it.
    with tempfile.TemporaryDirectory() as d:
        p, s = _write_signed(d, "receipt", RECEIPT_TYPE, RECEIPT)
        r = run(p, s, FIX / "public.pem")
        assert r.returncode == 1
        assert "unexpected payloadType" in r.stdout + r.stderr


def test_a_scorecard_never_verifies_as_a_receipt():
    r = run_typed("receipt", FIX / "scorecard.json",
                  FIX / "scorecard.sig", FIX / "public.pem")
    assert r.returncode == 1
    assert "unexpected payloadType" in r.stdout + r.stderr


def test_a_flipped_byte_in_a_receipt_fails_under_the_right_type():
    # Selecting the right payload type must not become a way to pass: the
    # signature still has to cover these exact bytes.
    with tempfile.TemporaryDirectory() as d:
        p, s = _write_signed(d, "receipt", RECEIPT_TYPE, RECEIPT)
        raw = bytearray(p.read_bytes())
        raw[raw.index(b"9")] = ord("8")
        p.write_bytes(bytes(raw))
        r = run_typed("receipt", p, s, FIX / "public.pem")
        assert r.returncode == 1
        assert "does not verify" in r.stdout + r.stderr


def test_a_teardown_attestation_verifies_without_scorecard_specific_checks():
    # A teardown attestation has no `integrity` block. Verifying one must not
    # raise a traceback reaching for one — the script's contract is that every
    # failure is a one-line INVALID.
    doc = {"run_id": "01J9X2QK7C4V0R8YB3ZP6MTS5A", "topics_deleted": ["drill-orders"]}
    with tempfile.TemporaryDirectory() as d:
        p, s = _write_signed(d, "teardown", TEARDOWN_TYPE, doc)
        r = run_typed("teardown", p, s, FIX / "public.pem")
        assert r.returncode == 0, r.stderr
        assert "VALID" in r.stdout
        assert "Traceback" not in r.stderr


def test_an_unknown_payload_type_is_a_usage_error_not_a_bad_artifact():
    r = run_typed("sideways", FIX / "scorecard.json",
                  FIX / "scorecard.sig", FIX / "public.pem")
    assert r.returncode == 1
    assert "unknown --payload-type" in r.stdout + r.stderr
    assert "Traceback" not in r.stderr


def test_the_full_media_type_may_be_passed_instead_of_the_short_name():
    r = run_typed(SCORECARD_TYPE, FIX / "scorecard.json",
                  FIX / "scorecard.sig", FIX / "public.pem")
    assert r.returncode == 0, r.stderr
    assert "VALID" in r.stdout


def test_the_three_payload_types_match_the_rust_constants():
    # The verifier is only independent if it agrees with the signer on the
    # exact media types. A drift here means one of them signs or checks a
    # string the other never uses, and every fixture would still pass.
    rust = (ROOT / "crates" / "logweir-evidence" / "src" / "lib.rs").read_text()
    for t in (SCORECARD_TYPE, RECEIPT_TYPE, TEARDOWN_TYPE):
        assert t in rust, f"{t} is not declared in logweir-evidence/src/lib.rs"
    py = VERIFIER.read_text()
    for t in (SCORECARD_TYPE, RECEIPT_TYPE, TEARDOWN_TYPE):
        assert t in py, f"{t} is not declared in verify_scorecard.py"
