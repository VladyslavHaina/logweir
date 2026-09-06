import json, os, pathlib, subprocess, sys, tempfile

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
    # The REAL keyid — lowercase hex sha256 of the public key's SPKI DER.
    # `"fixture"` used to stand here, which worked only because the verifier
    # ignored `keyid` entirely. `logweir drill verify` selects the signature to
    # check BY keyid and refuses a sidecar that carries none for the key it was
    # given, so a placeholder made these fixtures documents the two verifiers
    # disagreed about — the exact defect this file's later tests pin.
    import hashlib as _h
    der = key.public_key().public_bytes(
        serialization.Encoding.DER,
        serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    return {"payloadType": payload_type,
            "signatures": [{"keyid": _h.sha256(der).hexdigest(),
                            "sig": _b64.b64encode(sig).decode()}]}


def _write_signed(d, name, payload_type, doc, ensure_ascii=True):
    # `ensure_ascii=False` matters: Python escapes non-ASCII to \uXXXX by
    # default, so a document with an accented topic name would land on disk as
    # pure ASCII and could not distinguish a byte-length PAE from a code-point
    # one. Rust's serde_json — what Logweir actually signs with — writes UTF-8
    # raw, so `False` is the shape a real scorecard has.
    payload = json.dumps(doc, indent=2, ensure_ascii=ensure_ascii).encode() + b"\n"
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


# ------------------------------------------------------------------ PAE lengths
# Task 22 fix round 1, FIX 4. `verify_scorecard.py`'s module docstring names this
# hazard in as many words — "a naive `len(payload_type)` in Python counts *code
# points*, which is only byte-correct for ASCII ... every fixture in this
# repository still verifies (they are pure ASCII), which is exactly the trap" —
# and then nothing checked it. A code-point implementation survived all fifteen
# tests above. These two close it: the first at the function, the second end to
# end through the shipped CLI.

def _verifier_module():
    """Import docs/verify_scorecard.py as a module, so `pae()` can be called
    directly. Every other test here spawns it as a subprocess, which is the
    right shape for behaviour but cannot reach a pure function."""
    import importlib.util
    spec = importlib.util.spec_from_file_location("vs_under_test", str(VERIFIER))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_pae_length_prefixes_are_byte_counts_not_code_points():
    v = _verifier_module()
    # 'ü' is 2 bytes in UTF-8, '→' is 3, '𝄞' is 4. A code-point count and a
    # byte count therefore disagree by a known amount in BOTH fields.
    ptype = "application/vnd.logweir.tëst→𝄞+json;version=1.0.0"
    payload = "hëllo→𝄞".encode("utf-8")

    tb = ptype.encode("utf-8")
    expected = (b"DSSEv1 " + str(len(tb)).encode() + b" " + tb + b" "
                + str(len(payload)).encode() + b" " + payload)
    assert v.pae(ptype, payload) == expected

    # The test must not be able to pass vacuously: assert the naive
    # code-point computation is genuinely DIFFERENT here, so this fixture
    # actually distinguishes the two implementations.
    naive = (b"DSSEv1 " + str(len(ptype)).encode() + b" " + tb + b" "
             + str(len(payload.decode())).encode() + b" " + payload)
    assert naive != expected, "this fixture cannot tell the two apart"

    # And the prefixes are the byte counts, spelled out.
    assert len(tb) > len(ptype), "the payload type must be multi-byte here"
    assert v.pae(ptype, payload).startswith(b"DSSEv1 " + str(len(tb)).encode() + b" ")


def test_a_scorecard_whose_content_is_multibyte_utf8_still_verifies():
    # The end-to-end half: a real signed document whose PAYLOAD is multi-byte,
    # verified through the CLI. A code-point length on the payload signs one
    # prefix and verifies against another, so this returns 1 instead of 0.
    doc = json.loads((ROOT / "e2e" / "fixtures" / "scorecard-pass.json").read_bytes())
    doc["triggered_by"] = "sürety-drill → Zürich · 𝄞 · 東京"
    with tempfile.TemporaryDirectory() as d:
        p, s = _write_signed(d, "utf8-scorecard", SCORECARD_TYPE, doc, ensure_ascii=False)
        raw = p.read_bytes()
        assert len(raw) > len(raw.decode()), "the payload must be multi-byte here"
        r = run(p, s, FIX / "public.pem")
        assert r.returncode == 0, r.stderr
        assert "VALID" in r.stdout


# ----------------------------------------------- a missing dependency, handled
# Task 22 fix round 1, FIX 5. `cryptography` is the script's one third-party
# import and the first thing a fresh machine hits. It used to raise, printing a
# traceback and exiting 1 — the code this script's own contract defines as "the
# signature did not verify". An auditor's automation could not tell "your
# scorecard is forged" from "I could not start".

def test_a_missing_cryptography_package_says_what_to_install_and_exits_2():
    import os
    with tempfile.TemporaryDirectory() as d:
        # Shadow the real package with one that refuses to import, FIRST on
        # sys.path. This reproduces a machine that has not run `pip install`
        # without uninstalling anything.
        shadow = pathlib.Path(d) / "cryptography"
        shadow.mkdir()
        (shadow / "__init__.py").write_text(
            'raise ImportError("simulated: cryptography is not installed")\n'
        )
        env = dict(os.environ, PYTHONPATH=d)
        r = subprocess.run(
            [sys.executable, str(VERIFIER),
             str(FIX / "scorecard.json"), str(FIX / "scorecard.sig"), str(FIX / "public.pem")],
            capture_output=True, text=True, env=env,
        )
        out = r.stdout + r.stderr
        assert "Traceback" not in out, f"a raw traceback reached the user:\n{out}"
        assert "pip install cryptography" in out, out
        # The load-bearing assertion: NOT 1. Exit 1 is a verdict on the
        # document, and no document was examined here.
        assert r.returncode == 2, f"expected exit 2, got {r.returncode}:\n{out}"
        assert "NOTHING WAS VERIFIED" in out
        assert "INVALID" not in out, "a setup failure must not be reported as an invalid document"


# ------------------------------------------------- the two verifiers must AGREE
# `docs/verify-a-scorecard.md` says both routes "reach the same verdict" and
# that a disagreement "is a bug in the format". For one release that was false:
# this script implemented ONE of the ~12 rules `Scorecard::validate_invariants`
# applies and parsed no other field, so each document below printed a clean
# `VALID` here and was refused by `logweir drill verify`. The three cases are
# the ones the final whole-branch review found by execution.

SCORECARD_PASS = ROOT / "e2e" / "fixtures" / "scorecard-pass.json"


def _signed_scorecard(d, **overrides):
    """A correctly-signed scorecard built from the checked-in format example,
    with `overrides` applied by dotted path. The SIGNATURE IS GENUINE — every
    case below is a document whose bytes really were signed by the fixture key,
    so a refusal can only come from the invariant check and never from the
    cryptography."""
    doc = json.loads(SCORECARD_PASS.read_bytes())
    for path, value in overrides.items():
        parts = path.split(".")
        node = doc
        for p in parts[:-1]:
            node = node[p]
        node[parts[-1]] = value
    return _write_signed(d, "case", SCORECARD_TYPE, doc)


def test_the_unmodified_format_example_is_valid_under_the_full_invariant_set():
    # The control. Without it, a `check_invariants` that refused everything
    # would make every test below pass for the wrong reason.
    with tempfile.TemporaryDirectory() as d:
        sc, sig = _signed_scorecard(d)
        r = run(sc, sig, FIX / "public.pem")
        assert r.returncode == 0, r.stderr
        assert "VALID" in r.stdout


def test_a_higher_major_format_version_is_refused():
    # Global Constraint 12. This script did not read `format_version` at all.
    #
    # The document ALSO violates the T0-2 evidence arm. That arm is scoped to
    # major 1, so on a 2.0.0 document it must not fire: dropping its
    # `if doc_major == 1:` guard makes this test report the evidence message
    # instead of the major-version one (brief §7 M9).
    with tempfile.TemporaryDirectory() as d:
        sc, sig = _signed_scorecard(
            d,
            format_version="2.0.0",
            **{"evidence.create_only_enforced": True},
        )
        r = run(sc, sig, FIX / "public.pem")
        assert r.returncode == 1, r.stdout
        assert "major version newer" in r.stderr
        assert "post-put fields are zeroed" not in r.stderr, (
            "the evidence arm is scoped to major 1 and must not fire on a 2.0.0 document"
        )


def test_a_higher_minor_format_version_is_still_accepted():
    # The other half of GC12: readers ignore unknown fields and accept a higher
    # MINOR. A refusal on any version difference would be wrong too.
    with tempfile.TemporaryDirectory() as d:
        sc, sig = _signed_scorecard(d, format_version="1.9.9")
        r = run(sc, sig, FIX / "public.pem")
        assert r.returncode == 0, r.stderr


# ------------------------------------- the evidence block is zeroed before signing
# T0-2. `verify_scorecard.py` printed "the four post-put fields are zeroed
# before signing" on EVERY successful verification while nothing checked it,
# over two committed fixtures that falsified it. One test per field, each
# asserting the message BYTE-IDENTICALLY with the Rust arm's.

def test_non_zeroed_evidence_refused():
    with tempfile.TemporaryDirectory() as d:
        sc, sig = _signed_scorecard(d, **{"evidence.create_only_enforced": True})
        r = run(sc, sig, FIX / "public.pem")
        assert r.returncode == 1, r.stdout
        assert ("evidence.create_only_enforced is true but the four post-put fields "
                "are zeroed before signing") in r.stderr


def test_a_set_evidence_version_id_is_refused():
    with tempfile.TemporaryDirectory() as d:
        sc, sig = _signed_scorecard(d, **{"evidence.version_id": "3HL4kqtJlcpXroDTDmJ"})
        r = run(sc, sig, FIX / "public.pem")
        assert r.returncode == 1, r.stdout
        assert ("evidence.version_id is set but the four post-put fields "
                "are zeroed before signing") in r.stderr


def test_a_set_evidence_retain_until_is_refused():
    with tempfile.TemporaryDirectory() as d:
        sc, sig = _signed_scorecard(d, **{"evidence.retain_until": "2027-01-01T00:00:00Z"})
        r = run(sc, sig, FIX / "public.pem")
        assert r.returncode == 1, r.stdout
        assert ("evidence.retain_until is set but the four post-put fields "
                "are zeroed before signing") in r.stderr


def test_a_true_evidence_immutable_is_refused():
    with tempfile.TemporaryDirectory() as d:
        sc, sig = _signed_scorecard(d, **{"evidence.immutable": True})
        r = run(sc, sig, FIX / "public.pem")
        assert r.returncode == 1, r.stdout
        assert ("evidence.immutable is true but the four post-put fields "
                "are zeroed before signing") in r.stderr


def test_a_lower_major_with_a_non_zeroed_evidence_block_is_accepted():
    """The evidence arm's `doc_major == 1` guard, where it is actually
    observable.

    On a HIGHER major the guard is unobservable — the major-version refusal
    above returns first, so removing the guard changes nothing (the brief names
    `test_a_higher_major_format_version_is_refused` as this mutant's killer; it
    is not, for the same reason M8 is an equivalent mutant on the Rust side).
    On a LOWER major both readers must ACCEPT a non-zeroed block, because a
    different major may define the block differently. `logweir-core`'s
    `the_evidence_arm_is_scoped_to_major_1_and_does_not_touch_major_0` is this
    test's other half; the two together are what keep the readers in step.
    """
    with tempfile.TemporaryDirectory() as d:
        sc, sig = _signed_scorecard(
            d,
            format_version="0.9.9",
            **{
                "evidence.version_id": "v-from-another-major",
                "evidence.immutable": True,
                "evidence.create_only_enforced": True,
            },
        )
        r = run(sc, sig, FIX / "public.pem")
        assert r.returncode == 0, (
            "the evidence arm is scoped to major 1 and must not fire on a 0.x "
            f"document; stderr was: {r.stderr}"
        )


def test_the_evidence_arm_matches_the_rust_arm_word_for_word_and_in_order():
    """TWO-VERIFIER PARITY, proved rather than asserted in prose.

    `docs/verify-a-scorecard.md` says the two routes "reach the same verdict"
    and that a disagreement "is a bug in the format" — and stage 1 found them
    disagreeing. This test reads the four message literals out of
    `Scorecard::validate_invariants` itself (the CODE half of scorecard.rs, not
    its test module) and drives this script's `check_invariants` with one
    violating document per field, asserting the SAME verdict, the SAME message
    and the SAME field order from both readers.
    """
    import re

    rust = (ROOT / "crates" / "logweir-core" / "src" / "scorecard.rs").read_text()
    code_half = rust.split("#[cfg(test)]")[0]
    rust_messages = re.findall(r'"(evidence\.[^"]*zeroed before signing)"', code_half)
    assert rust_messages == [
        "evidence.version_id is set but the four post-put fields are zeroed before signing",
        "evidence.retain_until is set but the four post-put fields are zeroed before signing",
        "evidence.immutable is true but the four post-put fields are zeroed before signing",
        "evidence.create_only_enforced is true but the four post-put fields are zeroed before signing",
    ], rust_messages

    check = _verifier_module().check_invariants
    base = json.loads(SCORECARD_PASS.read_bytes())
    assert check(base) == "", "the control document must hold under both readers"

    for field, value, expected in (
        ("version_id", "3HL4kqtJlcpXroDTDmJ", rust_messages[0]),
        ("retain_until", "2027-01-01T00:00:00Z", rust_messages[1]),
        ("immutable", True, rust_messages[2]),
        ("create_only_enforced", True, rust_messages[3]),
    ):
        doc = json.loads(SCORECARD_PASS.read_bytes())
        doc["evidence"][field] = value
        assert check(doc) == expected, field

    # Two fields at once: the shared field ORDER is what makes the two readers
    # produce the same message rather than merely both refusing.
    doc = json.loads(SCORECARD_PASS.read_bytes())
    doc["evidence"]["immutable"] = True
    doc["evidence"]["create_only_enforced"] = True
    assert check(doc) == rust_messages[2]


def test_the_committed_signed_fixtures_carry_a_zeroed_evidence_block():
    # The fixtures this repository ships as its worked example used to violate
    # the sentence the success path prints. Both readers refuse such a document
    # now, so a regression here is a re-mint that went wrong.
    for name in ("scorecard.json", "scorecard-self-attested.json"):
        doc = json.loads((FIX / name).read_bytes())
        assert doc["evidence"] == {
            "version_id": None,
            "retain_until": None,
            "immutable": False,
            "create_only_enforced": False,
        }, name
        assert _verifier_module().check_invariants(doc) == "", name


def test_the_script_prints_its_version():
    # The success path states the evidence guarantee as a fact. An auditor
    # reading that line needs to know whether the run that printed it also
    # ENFORCED it — that is what the version line answers.
    r = run(FIX / "scorecard.json", FIX / "scorecard.sig", FIX / "public.pem")
    assert r.returncode == 0, r.stderr
    assert f"verify_scorecard.py {_verifier_module().SCRIPT_VERSION}" in r.stdout


def test_the_version_line_names_the_current_invariant_set():
    # The literals below are deliberately HARD-CODED. Asserting
    # f"...{SCRIPT_VERSION}" — as `test_the_script_prints_its_version` above
    # does, on purpose, for a different reason — is constant-relative: it stays
    # green while the printed claim about which checks ran goes stale. One
    # small test holding the literal is what makes the constant load-bearing.
    # See Task 4 addendum A1.
    with tempfile.TemporaryDirectory() as d:
        sc, sig = _signed_scorecard(d)
        r = run(sc, sig, FIX / "public.pem")
        assert r.returncode == 0, r.stderr
        assert "verify_scorecard.py 1.3.0" in r.stdout, r.stdout
        assert "redactions" in r.stdout, r.stdout
        assert "trimmed-empty partial_reason" in r.stdout, r.stdout


def test_the_script_version_is_not_the_format_version():
    # GC12: SCRIPT_VERSION tracks the invariant SET, FORMAT_VERSION tracks the
    # format. Collapsing the two would make a reader-only tightening look like
    # a format bump.
    mod = _verifier_module()
    assert mod.SCRIPT_VERSION != mod.FORMAT_VERSION
    assert mod.FORMAT_VERSION == "1.0.0"


def test_a_negative_rpo_is_refused():
    # `-90` is the exact value docs/formats/drill-scorecard.md says a reader
    # "would most likely read as no data loss" — printed under a VALID banner
    # by the tool the auditor is told to trust more.
    with tempfile.TemporaryDirectory() as d:
        sc, sig = _signed_scorecard(d, **{"measured.rpo_seconds": -90})
        r = run(sc, sig, FIX / "public.pem")
        assert r.returncode == 1, r.stdout
        assert "negative" in r.stderr


def test_more_matching_records_than_sampled_is_refused():
    with tempfile.TemporaryDirectory() as d:
        sc, sig = _signed_scorecard(
            d,
            **{
                "integrity.records_sampled": 10,
                "integrity.records_sampled_matching": 9999,
            },
        )
        r = run(sc, sig, FIX / "public.pem")
        assert r.returncode == 1, r.stdout
        assert "exceeds records_sampled" in r.stderr


def test_a_matrix_fail_without_a_reason_is_refused():
    with tempfile.TemporaryDirectory() as d:
        sc, sig = _signed_scorecard(d, **{"engine.matrix_verdict": "fail"})
        r = run(sc, sig, FIX / "public.pem")
        assert r.returncode == 1, r.stdout
        assert "matrix_verdict_reason" in r.stderr


def test_a_pass_rate_measured_without_byte_fingerprint_is_refused():
    with tempfile.TemporaryDirectory() as d:
        sc, sig = _signed_scorecard(
            d,
            **{
                "integrity.level": "consume-only",
                "integrity.pass_rate_measured": 1.0,
                "objectives.met": None,
            },
        )
        r = run(sc, sig, FIX / "public.pem")
        assert r.returncode == 1, r.stdout
        assert "not byte-fingerprint" in r.stderr


def test_a_last_phase_completed_outside_the_domain_is_refused():
    # Global Constraint 18: ELEVEN phase slots, -1 through 9.
    for bad in (-2, 10):
        with tempfile.TemporaryDirectory() as d:
            sc, sig = _signed_scorecard(d, last_phase_completed=bad)
            r = run(sc, sig, FIX / "public.pem")
            assert r.returncode == 1, f"{bad}: {r.stdout}"
            assert "-1..=9" in r.stderr


def test_a_blank_partial_reason_is_refused():
    # T0-6. Python already refused `""` by truthiness while Rust's `.is_none()`
    # ACCEPTED AND SIGNED it — the live divergence in the file whose own
    # docstring says the two readers mirror each other arm for arm. `"   "` is
    # the other direction: a non-empty whitespace string is TRUTHY in Python,
    # so both readers accepted it until Task 4 put `.strip()` / `.trim()` on
    # both sides. This test pins Python's behaviour so a future "cleanup" to
    # `is None` cannot silently re-open the divergence.
    for blank in ("", "   ", "\t\n"):
        with tempfile.TemporaryDirectory() as d:
            sc, sig = _signed_scorecard(
                d, **{"integrity.result": "partial", "integrity.partial_reason": blank})
            r = run(sc, sig, FIX / "public.pem")
            assert r.returncode == 1, f"{blank!r}: {r.stdout}"
            assert "partial_reason is null" in r.stderr, f"{blank!r}: {r.stderr}"


def test_a_partial_result_that_names_its_reason_is_accepted():
    # The control for the test above. The arm narrows the accepted set; it does
    # not close it. Without this, an arm refusing every `partial` document
    # would pass for the wrong reason.
    with tempfile.TemporaryDirectory() as d:
        sc, sig = _signed_scorecard(
            d, **{"integrity.result": "partial",
                  "integrity.partial_reason": "only 2 of 3 partitions reached a conclusion"})
        r = run(sc, sig, FIX / "public.pem")
        assert r.returncode == 0, r.stderr


def test_non_empty_redactions_is_refused():
    # T0-3. `docs/formats/drill-scorecard.md` states "Always `[]` in v0.1" as a
    # property of the format; nothing enforced it and nothing displayed it, so
    # a document announcing that the field an auditor reads first had been
    # removed still printed VALID from both readers.
    with tempfile.TemporaryDirectory() as d:
        sc, sig = _signed_scorecard(d, redactions=[
            {"path": "/measured/rpo_seconds", "reason": "customer policy", "present": False}])
        r = run(sc, sig, FIX / "public.pem")
        assert r.returncode == 1, r.stdout
        assert "no way to produce one" in r.stderr, r.stderr
        # The message names the DOCUMENT's own format_version, and is
        # byte-identical to the Rust arm's — see
        # crates/logweir/tests/two_reader_parity.rs, which compares the two.
        assert (
            "INVALID: redactions is non-empty but format_version 1.0.0 has no way to "
            "produce one; --redact is a v0.1.1 feature" in r.stderr
        ), r.stderr


def test_the_redactions_arm_fires_after_the_partial_reason_arm():
    # Ordering is part of the parity contract: a document violating BOTH must
    # report the `partial_reason` message from both readers. Reordering the
    # Python arms leaves both exit codes correct and breaks only the message,
    # which is exactly what this asserts.
    with tempfile.TemporaryDirectory() as d:
        sc, sig = _signed_scorecard(
            d,
            redactions=[{"path": "/measured/rpo_seconds", "reason": "customer policy",
                         "present": False}],
            **{"integrity.result": "partial", "integrity.partial_reason": ""})
        r = run(sc, sig, FIX / "public.pem")
        assert r.returncode == 1, r.stdout
        assert "partial_reason is null" in r.stderr, r.stderr
        assert "redactions" not in r.stderr, r.stderr


def test_the_captured_by_logweir_biconditional_is_enforced_in_both_directions():
    with tempfile.TemporaryDirectory() as d:
        # true, but the source-relative RPO is still null with a reason.
        sc, sig = _signed_scorecard(d, **{"source.captured_by_logweir": True})
        r = run(sc, sig, FIX / "public.pem")
        assert r.returncode == 1, r.stdout
        assert "captured_by_logweir is true" in r.stderr
    with tempfile.TemporaryDirectory() as d:
        # false, but a source-relative RPO is present anyway.
        sc, sig = _signed_scorecard(
            d, **{"measured.rpo_source_relative_seconds": 5}
        )
        r = run(sc, sig, FIX / "public.pem")
        assert r.returncode == 1, r.stdout
        assert "never contacted" in r.stderr


def test_met_true_is_refused_when_the_pass_rate_was_not_measurable():
    with tempfile.TemporaryDirectory() as d:
        sc, sig = _signed_scorecard(
            d,
            **{
                "integrity.level": "consume-only",
                "integrity.pass_rate_measured": None,
                "objectives.met": True,
            },
        )
        r = run(sc, sig, FIX / "public.pem")
        assert r.returncode == 1, r.stdout
        assert "met must be null" in r.stderr


def test_the_format_version_matches_the_rust_constant():
    # The refusal above is only meaningful if this script and the signer agree
    # on what "this major" is.
    rust = (ROOT / "crates" / "logweir-core" / "src" / "lib.rs").read_text()
    assert 'pub const FORMAT_VERSION: &str = "1.0.0"' in rust
    assert _verifier_module().FORMAT_VERSION == "1.0.0"


# ------------------------------------------ no raw tracebacks, ever (the header
# promises "none of them should ever surface as a raw traceback")

def test_a_correctly_signed_non_scorecard_is_reported_not_traced_back():
    # A genuinely-signed document under the SCORECARD payload type that has no
    # `integrity` block. The old code did `doc["integrity"]` and raised
    # `KeyError: 'integrity'` — a raw traceback on a correctly-signed payload.
    with tempfile.TemporaryDirectory() as d:
        sc, sig = _write_signed(d, "notascorecard", SCORECARD_TYPE, {"hello": "world"})
        r = run(sc, sig, FIX / "public.pem")
        assert r.returncode == 1
        assert "Traceback" not in r.stderr, r.stderr
        assert "INVALID" in r.stderr


def test_a_correctly_signed_non_json_payload_is_reported_not_traced_back():
    # `json.loads(payload)` raised `json.JSONDecodeError` after the signature
    # had already verified.
    with tempfile.TemporaryDirectory() as d:
        payload = b"this is signed, and it is not JSON\n"
        p = pathlib.Path(d) / "blob.json"
        s = pathlib.Path(d) / "blob.sig"
        p.write_bytes(payload)
        s.write_text(json.dumps(_sign(SCORECARD_TYPE, payload)))
        r = run(p, s, FIX / "public.pem")
        assert r.returncode == 1
        assert "Traceback" not in r.stderr, r.stderr
        assert "not valid JSON" in r.stderr


def test_a_correctly_signed_non_receipt_is_reported_not_traced_back():
    with tempfile.TemporaryDirectory() as d:
        doc, sig = _write_signed(d, "notareceipt", RECEIPT_TYPE, {"hello": "world"})
        r = run_typed("receipt", doc, sig, FIX / "public.pem")
        assert r.returncode == 1
        assert "Traceback" not in r.stderr, r.stderr
        assert "INVALID" in r.stderr


# --------------------------------------------------------- multi-signature DSSE

def test_a_multi_signature_sidecar_verifies_on_any_matching_signature():
    # DSSE envelopes may carry one signature per signing key. This script
    # hard-indexed `signatures[0]`, so a sidecar whose FIRST entry belongs to a
    # key the auditor was not given reported INVALID while `logweir drill
    # verify` accepted it — a disagreement, in the one place this script exists
    # not to have one.
    import base64 as _b64
    with tempfile.TemporaryDirectory() as d:
        doc = json.loads(SCORECARD_PASS.read_bytes())
        payload = json.dumps(doc, indent=2).encode() + b"\n"
        p = pathlib.Path(d) / "multi.json"
        p.write_bytes(payload)
        sidecar = _sign(SCORECARD_TYPE, payload)
        genuine = sidecar["signatures"][0]
        # A syntactically valid signature by some OTHER key, placed FIRST. Its
        # keyid names a different key, so a verifier that selects by keyid
        # skips it and a verifier that hard-indexes [0] fails on it.
        other = {"keyid": "0" * 64,
                 "sig": _b64.b64encode(b"\x30\x44" + b"\x00" * 68).decode()}
        sidecar["signatures"] = [other, genuine]
        s = pathlib.Path(d) / "multi.sig"
        s.write_text(json.dumps(sidecar))
        r = run(p, s, FIX / "public.pem")
        assert r.returncode == 0, r.stderr + r.stdout


def test_a_sidecar_whose_signatures_are_all_wrong_still_fails():
    # The counterpart: "try every signature" must not become "accept anything".
    import base64 as _b64
    with tempfile.TemporaryDirectory() as d:
        doc = json.loads(SCORECARD_PASS.read_bytes())
        payload = json.dumps(doc, indent=2).encode() + b"\n"
        p = pathlib.Path(d) / "bogus.json"
        p.write_bytes(payload)
        s = pathlib.Path(d) / "bogus.sig"
        # Both entries claim THIS key, so keyid selection cannot excuse the
        # failure: the signature maths is what must reject them.
        real_keyid = _sign(SCORECARD_TYPE, payload)["signatures"][0]["keyid"]
        s.write_text(json.dumps({
            "payloadType": SCORECARD_TYPE,
            "signatures": [
                {"keyid": real_keyid,
                 "sig": _b64.b64encode(b"\x30\x44" + b"\x00" * 68).decode()},
                {"keyid": real_keyid,
                 "sig": _b64.b64encode(b"\x30\x44" + b"\x11" * 68).decode()},
            ],
        }))
        r = run(p, s, FIX / "public.pem")
        assert r.returncode == 1
        assert "does not verify" in r.stderr


def test_a_sidecar_naming_only_another_key_is_refused_by_name():
    # `logweir drill verify` says "no signature by key <id> in the sidecar" and
    # exits 4. This script used to ignore `keyid` altogether and verify the
    # first entry's bytes regardless — so a sidecar naming a different key
    # produced two different verdicts.
    import base64 as _b64
    with tempfile.TemporaryDirectory() as d:
        doc = json.loads(SCORECARD_PASS.read_bytes())
        payload = json.dumps(doc, indent=2).encode() + b"\n"
        p = pathlib.Path(d) / "wrongkey.json"
        p.write_bytes(payload)
        sidecar = _sign(SCORECARD_TYPE, payload)
        sidecar["signatures"][0]["keyid"] = "1" * 64
        s = pathlib.Path(d) / "wrongkey.sig"
        s.write_text(json.dumps(sidecar))
        r = run(p, s, FIX / "public.pem")
        assert r.returncode == 1
        assert "no signature by key" in r.stderr


# --------------------------------------------------------------------- T0-1
# `approval.self_attested` is DERIVED by both readers — `approval.key_id`
# compared against the key id of the signature that verified — never echoed
# out of the document being checked. A document whose claim disagrees with the
# derivation is refused by both.
#
# The two readers do NOT share an exit-code space and never have: this script's
# contract is 0 VALID / 1 INVALID / 2 could-not-run, while `drill verify`
# returns 4 for anything in the signing-or-lock class. Parity is asserted as a
# VERDICT-CLASS mapping plus byte-identical message text, never as numeric
# equality — `test_one_flipped_byte_fails` above already asserts 1 for the case
# `crates/logweir/tests/cli_verify.rs` asserts 4 for.

def _target_dir():
    """Cargo's output directory — `$CARGO_TARGET_DIR` when set, else `target/`."""
    override = os.environ.get("CARGO_TARGET_DIR")
    return pathlib.Path(override) if override else ROOT / "target"


def logweir_bin():
    """The Rust reader, resolved the same way `scripts/check-verifier-parity.sh`
    resolves it: `$LOGWEIR_BIN` first (the name `scripts/demo-approve.sh` and
    `scripts/demo.sh` already use), then the build output for either profile.

    Hardcoding `target/debug/logweir` made a release-only tree fail an assertion
    that has nothing to do with release builds. The env var comes first so a
    caller with the binary somewhere else — a CI job, a packaged build — can say
    so instead of being told to rebuild.

    Returns the path whether or not it exists; `require_logweir_bin` is what
    turns "absent" into a loud failure.
    """
    override = os.environ.get("LOGWEIR_BIN")
    if override:
        return pathlib.Path(override)
    target = _target_dir()
    for profile in ("debug", "release"):
        candidate = target / profile / "logweir"
        if candidate.exists():
            return candidate
    return target / "debug" / "logweir"


def require_logweir_bin():
    """The Rust reader, or a loud failure. NEVER a skip.

    A parity test that skips when one of the two readers is missing asserts
    nothing while reporting green, which is the precise defect class T0-1
    exists to eliminate: a documented guarantee nothing enforces. CI is
    expected to build the binary (`.github/workflows/ci.yml`, the
    `python-verifier` job), so an absent binary is a broken job, not a
    tolerable condition.
    """
    b = logweir_bin()
    if not b.exists():
        raise AssertionError(
            f"{b} is not built. The two-reader parity claim is the point of this "
            "test and cannot be checked without both readers: build with "
            "`cargo build -p logweir`, or point $LOGWEIR_BIN at the binary. "
            "This is NOT skipped."
        )
    return b

# document -> (rust exit code, python exit code)
PARITY_MAPPING = {
    "scorecard.json": (0, 0),
    "scorecard-self-attested.json": (0, 0),
    "scorecard-self-attested-bogus.json": (4, 1),
}

APPROVAL_REFUSAL = "APPROVAL CLAIM NOT VERIFIED:"


def _rust_verify(sc, sig, pub):
    """`drill verify` on the compiled binary. The exit code is read from
    `returncode` directly — never parsed out of a pipeline."""
    return subprocess.run(
        [str(require_logweir_bin()), "drill", "verify",
         "--scorecard", str(sc), "--signature", str(sig), "--public-key", str(pub)],
        capture_output=True, text=True,
    )


def _refusal_line(text):
    for line in text.splitlines():
        if APPROVAL_REFUSAL in line:
            # The Python arm prefixes its one-line refusals with "INVALID: ";
            # everything after that prefix must be byte-identical to Rust's.
            return line.split("INVALID: ", 1)[-1]
    return None


def test_bogus_self_attested_claim_refused():
    bogus = FIX / "scorecard-self-attested-bogus.json"
    r = run(bogus, FIX / "scorecard-self-attested-bogus.sig", FIX / "public.pem")
    both = r.stdout + r.stderr
    assert "Traceback" not in both, both
    assert "signature does not verify" not in both, (
        "the signature over this fixture is genuine; the refusal must come from the "
        "derivation, not from the cryptography: " + both
    )
    assert r.returncode == 1, both
    assert (
        "APPROVAL CLAIM NOT VERIFIED: the document claims self_attested=true but the "
        "approval key id "
    ) in both, both


def test_self_attested_parity():
    require_logweir_bin()
    for name, (want_rust, want_py) in PARITY_MAPPING.items():
        sc = FIX / name
        sig = FIX / (name[: -len(".json")] + ".sig")
        pub = FIX / "public.pem"
        rr = _rust_verify(sc, sig, pub)
        pr = run(sc, sig, pub)
        assert rr.returncode == want_rust, f"{name}: rust {rr.returncode}\n{rr.stderr}"
        assert pr.returncode == want_py, f"{name}: python {pr.returncode}\n{pr.stderr}"
        rust_line = _refusal_line(rr.stdout + rr.stderr)
        py_line = _refusal_line(pr.stdout + pr.stderr)
        assert (rust_line is None) == (py_line is None), (
            f"{name}: one reader refused the approval claim and the other did not.\n"
            f"rust: {rust_line!r}\npython: {py_line!r}"
        )
        if rust_line is not None:
            assert rust_line == py_line, (
                f"{name}: the refusal line must be byte-identical across the two "
                f"readers.\nrust:   {rust_line!r}\npython: {py_line!r}"
            )
