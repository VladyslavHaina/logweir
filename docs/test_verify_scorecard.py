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
