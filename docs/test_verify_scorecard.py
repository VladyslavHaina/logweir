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
