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
# The BACKUP receipt (Task 5's document, Task 5b's second reader) — not
# RECEIPT_TYPE above, which is the drill's post-put storage readback of a
# SCORECARD. Two documents, two media types.
BACKUP_RECEIPT_TYPE = "application/vnd.logweir.backup-receipt+json;version=1.0.0"


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


def test_the_four_payload_types_match_the_rust_constants():
    # The verifier is only independent if it agrees with the signer on the
    # exact media types. A drift here means one of them signs or checks a
    # string the other never uses, and every fixture would still pass.
    #
    # THE PATH FOLLOWED THE CONSTANTS. Task 14 moved the three constants into
    # `crates/logweir-verify/src/lib.rs`; `logweir-evidence` re-exports them
    # with `pub use logweir_verify::*;`, and a re-export contains none of the
    # three media-type literals — so reading the old file would assert a
    # property of a `pub use` line. Read the file that DECLARES them.
    rust = (ROOT / "crates" / "logweir-verify" / "src" / "lib.rs").read_text()
    for t in (SCORECARD_TYPE, BACKUP_RECEIPT_TYPE, RECEIPT_TYPE, TEARDOWN_TYPE):
        assert t in rust, f"{t} is not declared in logweir-verify/src/lib.rs"
    py = VERIFIER.read_text()
    for t in (SCORECARD_TYPE, BACKUP_RECEIPT_TYPE, RECEIPT_TYPE, TEARDOWN_TYPE):
        assert t in py, f"{t} is not declared in verify_scorecard.py"
    # …and the map really has FOUR entries, so a fifth type added to the Rust
    # crate and forgotten here is not silently covered by the loop above.
    assert len(_verifier_module().PAYLOAD_TYPES) == 4


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


# ------------------------------------- the offset report's key and digest travel together
# Task 9b. Two NESTED OPTIONAL fields on `evidence` — the only kind Global
# Constraint 12 as amended permits — carrying the object key and sha256 of the
# ENGINE's offset-mapping report, which the runner uploads beside the scorecard
# because the engine writes it to a pod-local path and the pod is deleted. The
# arm is symmetric and the messages are byte-identical with the Rust arm's.

def test_an_offset_report_key_without_its_sha256_is_refused():
    with tempfile.TemporaryDirectory() as d:
        sc, sig = _signed_scorecard(
            d,
            **{"evidence.offset_report_key": "logweir/drills/RUN.offsets.json"},
        )
        r = run(sc, sig, FIX / "public.pem")
        assert r.returncode == 1, r.stdout
        assert ("evidence.offset_report_key and evidence.offset_report_sha256 are present "
                "or absent together; a key with no digest names bytes nothing binds, and a "
                "digest with no key binds bytes nobody can fetch") in r.stderr


def test_an_offset_report_sha256_without_its_key_is_refused():
    # The other direction, same arm, same message: a digest with no key binds
    # bytes nobody can fetch.
    with tempfile.TemporaryDirectory() as d:
        sc, sig = _signed_scorecard(
            d,
            **{"evidence.offset_report_sha256": "sha256:" + "0" * 64},
        )
        r = run(sc, sig, FIX / "public.pem")
        assert r.returncode == 1, r.stdout
        assert "are present or absent together" in r.stderr


def test_a_blank_offset_report_key_counts_as_absent():
    # Ruling R-A: `.strip()` here, `trim().is_empty()` in Rust. A `""` key
    # beside a `""` digest is two absent fields, not two present ones — and a
    # `""` key beside a REAL digest is still the arm's refusal.
    with tempfile.TemporaryDirectory() as d:
        sc, sig = _signed_scorecard(
            d,
            **{"evidence.offset_report_key": "   ", "evidence.offset_report_sha256": ""},
        )
        r = run(sc, sig, FIX / "public.pem")
        assert r.returncode == 0, r.stderr
    with tempfile.TemporaryDirectory() as d:
        sc, sig = _signed_scorecard(
            d,
            **{
                "evidence.offset_report_key": "  ",
                "evidence.offset_report_sha256": "sha256:" + "0" * 64,
            },
        )
        r = run(sc, sig, FIX / "public.pem")
        assert r.returncode == 1, r.stdout
        assert "are present or absent together" in r.stderr

# --- target.mode / target.marker_topic (1.12.0, review F1) -----------------

MARKER_ARM = (
    "target.marker_topic is absent but target.mode is scratch; the marker topic is the "
    "segregation proof phase 0 verified, and a scratch document that omits it claims a "
    "check nothing recorded"
)


def _signed_scorecard_without_a_marker_topic(d, mode=None):
    """The format example with `target.marker_topic` REMOVED — key and all,
    which is how `drill::target_info` writes a `newTopic` document — and
    `target.mode` set when one is given. `_signed_scorecard` can override a
    value but cannot delete a key, and the absence is the whole point here."""
    doc = json.loads(SCORECARD_PASS.read_bytes())
    del doc["target"]["marker_topic"]
    if mode is not None:
        doc["target"]["mode"] = mode
    return _write_signed(d, "case", SCORECARD_TYPE, doc)


def test_a_scratch_document_with_no_marker_topic_is_refused():
    # The arm in the direction that matters: `target.marker_topic`'s own
    # documented meaning is the phase-0 segregation proof — the cluster is in
    # `allowedClusterIds` AND the topic exists — so a scratch document that
    # omits it claims a check nothing recorded. An ABSENT `mode` is `scratch`,
    # which is what every document written before the field existed carries.
    with tempfile.TemporaryDirectory() as d:
        sc, sig = _signed_scorecard_without_a_marker_topic(d)
        r = run(sc, sig, FIX / "public.pem")
        assert r.returncode == 1, r.stdout
        assert MARKER_ARM in r.stderr, r.stderr
    # An EXPLICIT `scratch` is the same document by another spelling.
    with tempfile.TemporaryDirectory() as d:
        sc, sig = _signed_scorecard_without_a_marker_topic(d, mode="scratch")
        r = run(sc, sig, FIX / "public.pem")
        assert r.returncode == 1, r.stdout
        assert MARKER_ARM in r.stderr, r.stderr


def test_a_new_topic_document_with_no_marker_topic_is_accepted():
    # The mode branch's own document, and the exact shape this tree now
    # writes: `newTopic` skips phase 0's marker and allowlist checks, so the
    # field is absent rather than echoed from the spec.
    with tempfile.TemporaryDirectory() as d:
        sc, sig = _signed_scorecard_without_a_marker_topic(d, mode="newTopic")
        r = run(sc, sig, FIX / "public.pem")
        assert r.returncode == 0, r.stderr
        assert "VALID" in r.stdout
    # And a `newTopic` document that DOES name one is accepted too: what this
    # tree writes is narrower than what its readers accept.
    with tempfile.TemporaryDirectory() as d:
        sc, sig = _signed_scorecard(d, **{"target.mode": "newTopic"})
        r = run(sc, sig, FIX / "public.pem")
        assert r.returncode == 0, r.stderr


def test_a_blank_marker_topic_counts_as_absent():
    # Ruling R-A: `.strip()` here, `trim().is_empty()` in Rust. Without it the
    # two readers split on `""` — the class T0-6 actually found.
    with tempfile.TemporaryDirectory() as d:
        sc, sig = _signed_scorecard(d, **{"target.marker_topic": "   "})
        r = run(sc, sig, FIX / "public.pem")
        assert r.returncode == 1, r.stdout
        assert MARKER_ARM in r.stderr, r.stderr
    with tempfile.TemporaryDirectory() as d:
        sc, sig = _signed_scorecard(
            d, **{"target.marker_topic": "", "target.mode": "newTopic"}
        )
        r = run(sc, sig, FIX / "public.pem")
        assert r.returncode == 0, r.stderr



def test_both_offset_report_fields_present_verify_and_are_printed():
    key = "logweir/drills/01J9X2QK7C4V0R8YB3ZP6MTS5A.offsets.json"
    digest = "sha256:" + "a" * 64
    with tempfile.TemporaryDirectory() as d:
        sc, sig = _signed_scorecard(
            d,
            **{"evidence.offset_report_key": key, "evidence.offset_report_sha256": digest},
        )
        r = run(sc, sig, FIX / "public.pem")
        assert r.returncode == 0, r.stderr
        # The presence-tolerant READ: printed only when the document carries
        # one, with the digest beside the key.
        assert key in r.stdout, r.stdout
        assert digest in r.stdout, r.stdout
        assert "applied to nothing" in r.stdout, r.stdout


def test_an_absent_offset_report_prints_no_offsets_line():
    # The control for the row above: an older document, which has neither
    # field, prints exactly what it always did.
    with tempfile.TemporaryDirectory() as d:
        sc, sig = _signed_scorecard(d)
        r = run(sc, sig, FIX / "public.pem")
        assert r.returncode == 0, r.stderr
        assert "offsets:" not in r.stdout, r.stdout


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
        assert "verify_scorecard.py 1.12.0" in r.stdout, r.stdout
        assert "redactions" in r.stdout, r.stdout
        assert "trimmed-empty partial_reason" in r.stdout, r.stdout
        assert "outcome-entailment" in r.stdout, r.stdout
        assert "all eleven required blocks in serde order" in r.stdout, r.stdout
        assert (
            "the six required non-block fields present and of the type their Rust "
            "type implies"
        ) in r.stdout, r.stdout
        assert "u64 domain with null refused where Rust has no Option" in r.stdout, r.stdout
        # 1.9.0's addition: Global Constraint 12's price for `target.auth`.
        # 1.10.0 extends the same clause: the mode's VALUE SET is closed, which
        # is the third `target.auth` arm.
        assert (
            "target.auth's mode present, not blank, and one of the two values the "
            "format defines when the block is"
        ) in r.stdout, r.stdout
        # 1.11.0's addition: GC12's price for `evidence.offset_report_key` and
        # `evidence.offset_report_sha256`, which travel together or not at all.
        assert (
            "evidence.offset_report_key and its sha256 present or absent together"
        ) in r.stdout, r.stdout
        # 1.12.0's addition (review F1): `target.marker_topic` is the scratch
        # segregation proof, so it is optional and `target.mode` is what says
        # which run it was.
        assert (
            "target.marker_topic present unless target.mode is newTopic"
        ) in r.stdout, r.stdout


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
    # `engine.matrix_verdict` is moved to `pass-degraded` — the value
    # `matrix_verdict_for` actually returns for a pass at a reduced integrity
    # level — because T0-4's matrix arm sits EARLIER in both readers and would
    # otherwise fire first and steal the failure. Same reason `objectives.met`
    # is nulled: isolate the arm this case is about.
    with tempfile.TemporaryDirectory() as d:
        sc, sig = _signed_scorecard(
            d,
            **{
                "integrity.level": "consume-only",
                "integrity.pass_rate_measured": 1.0,
                "objectives.met": None,
                "engine.matrix_verdict": "pass-degraded",
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
    #
    # T0-4: the other three overrides are what a `partial` integrity result
    # ACTUALLY travels with in a document Logweir can produce.
    # `phase8_score::decide` maps a non-`pass` integrity result to
    # `fail-integrity`, and `matrix_verdict_for` then returns `fail` with that
    # sentence as its reason. Before the outcome arms existed this case left
    # `outcome: "pass"` standing beside `integrity.result: "partial"` — a
    # document no writer emits — and both readers accepted it.
    with tempfile.TemporaryDirectory() as d:
        sc, sig = _signed_scorecard(
            d, **{"outcome": "fail-integrity",
                  "integrity.result": "partial",
                  "integrity.partial_reason": "only 2 of 3 partitions reached a conclusion",
                  "engine.matrix_verdict": "fail",
                  "engine.matrix_verdict_reason":
                      "the drill ran and did not pass: outcome fail-integrity"})
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


# ------------------------------------- `outcome` is entailed, not merely stated
# T0-4. `self.outcome` appeared in no arm of either reader, so a document
# saying `pass` beside its own contradicting evidence was accepted, signed and
# verified at exit 0. Six arms, mirrored ARM FOR ARM, IN ORDER with
# `Scorecard::validate_invariants`; each case below asserts the message
# BYTE-IDENTICALLY with the Rust arm's, because
# `crates/logweir/tests/two_reader_parity.rs` compares the two readers' refusal
# TEXT and not merely that both refused.


def test_a_pass_beside_a_non_pass_integrity_result_is_refused():
    with tempfile.TemporaryDirectory() as d:
        sc, sig = _signed_scorecard(
            d,
            **{
                "integrity.result": "partial",
                "integrity.partial_reason": "orders/7 never reconciled",
            },
        )
        r = run(sc, sig, FIX / "public.pem")
        assert r.returncode == 1, r.stdout
        assert "outcome is 'pass' but integrity.result is not 'pass'" in r.stderr


def test_a_pass_with_a_partial_reason_is_refused():
    with tempfile.TemporaryDirectory() as d:
        sc, sig = _signed_scorecard(
            d, **{"integrity.partial_reason": "orders/7 never reconciled"}
        )
        r = run(sc, sig, FIX / "public.pem")
        assert r.returncode == 1, r.stdout
        assert "outcome is 'pass' but integrity.partial_reason is present" in r.stderr


def test_a_pass_with_an_unmet_objective_is_refused():
    with tempfile.TemporaryDirectory() as d:
        sc, sig = _signed_scorecard(d, **{"objectives.met": False})
        r = run(sc, sig, FIX / "public.pem")
        assert r.returncode == 1, r.stdout
        assert "outcome is 'pass' but objectives.met is false" in r.stderr


def test_a_pass_with_an_incomplete_sample_is_refused():
    with tempfile.TemporaryDirectory() as d:
        sc, sig = _signed_scorecard(
            d,
            **{
                "integrity.records_sampled": 100,
                "integrity.records_sampled_matching": 50,
                "sample.records_expected": 100,
            },
        )
        r = run(sc, sig, FIX / "public.pem")
        assert r.returncode == 1, r.stdout
        assert "outcome is 'pass' but only 50 of 100 sampled records matched" in r.stderr


def test_sampling_more_records_than_were_expected_is_refused():
    with tempfile.TemporaryDirectory() as d:
        sc, sig = _signed_scorecard(
            d,
            **{
                "integrity.records_sampled": 100,
                "integrity.records_sampled_matching": 100,
                "sample.records_expected": 75,
            },
        )
        r = run(sc, sig, FIX / "public.pem")
        assert r.returncode == 1, r.stdout
        assert "records_sampled (100) exceeds sample.records_expected (75)" in r.stderr


def test_a_matrix_pass_below_byte_fingerprint_is_refused():
    # `pass_rate_measured` and `objectives.met` are nulled because the base
    # document carries `pass_rate_measured: 1.0` and `met: true`, and the
    # pre-existing `pass_rate_measured => byte-fingerprint` and
    # `met must be null when pass_rate is not measurable` arms would otherwise
    # fire first and steal the failure.
    with tempfile.TemporaryDirectory() as d:
        sc, sig = _signed_scorecard(
            d,
            **{
                "outcome": "fail-integrity",
                "integrity.result": "fail",
                "integrity.level": "consume-only",
                "integrity.pass_rate_measured": None,
                "objectives.met": None,
            },
        )
        r = run(sc, sig, FIX / "public.pem")
        assert r.returncode == 1, r.stdout
        assert (
            "engine.matrix_verdict is 'pass' but the drill did not pass at "
            "byte-fingerprint level"
        ) in r.stderr


# --- Task 5c: the `sample` reader asymmetry, closed -------------------------
#
# Every block below is a NON-optional field of `logweir_core::scorecard::
# Scorecard`, so the Rust reader refuses a document missing one at
# DESERIALISATION and `drill verify` exits 1. This script has no such layer.
# `evidence` has been in the block-presence loop since Task 2; `sample` was
# deliberately left out, and the measured cost of that omission was a document
# `drill verify` exited 1 on and this script printed VALID for — the exact
# two-reader disagreement `verify_scorecard.py`'s own module comment calls
# impossible. `crates/logweir/tests/two_reader_parity.rs::
# two_reader_parity_on_documents_refused_before_the_invariants` is the other
# half of these tests: it runs BOTH readers over the same two documents.


def _signed_scorecard_without(d, block):
    """The format example with one whole top-level block removed, signed."""
    doc = json.loads(SCORECARD_PASS.read_bytes())
    del doc[block]
    return _write_signed(d, "case", SCORECARD_TYPE, doc)


def test_a_document_with_no_sample_block_is_refused():
    with tempfile.TemporaryDirectory() as d:
        sc, sig = _signed_scorecard_without(d, "sample")
        r = run(sc, sig, FIX / "public.pem")
        assert r.returncode == 1, r.stdout
        assert (
            "the document has no sample block; it is not a drill scorecard"
        ) in r.stderr


def test_a_document_with_no_evidence_block_is_still_refused():
    # The twin, and the reason the loop exists at all. Pinned here so a
    # "cleanup" that trims the loop cannot quietly re-open the `sample` gap on
    # the block that was already covered.
    with tempfile.TemporaryDirectory() as d:
        sc, sig = _signed_scorecard_without(d, "evidence")
        r = run(sc, sig, FIX / "public.pem")
        assert r.returncode == 1, r.stdout
        assert (
            "the document has no evidence block; it is not a drill scorecard"
        ) in r.stderr


def test_a_records_expected_that_is_not_an_integer_is_refused():
    # `sample.records_expected` is a `u64` in Rust, so a string, a null, a
    # float or a bool is a deserialisation refusal there. Here it used to be a
    # SILENT SKIP: the coverage arm guarded with `isinstance(expected, int)`
    # and simply did not run, so `"records_expected": "75"` printed VALID.
    #
    # `True` is in the list because `isinstance(True, int)` is True in Python
    # and in no other reader.
    for bad in ("75", None, 75.0, True):
        with tempfile.TemporaryDirectory() as d:
            sc, sig = _signed_scorecard(d, **{"sample.records_expected": bad})
            r = run(sc, sig, FIX / "public.pem")
            assert r.returncode == 1, f"{bad!r}: {r.stdout}"
            assert "sample.records_expected is not an integer" in r.stderr, bad


def test_an_absent_records_expected_is_refused():
    with tempfile.TemporaryDirectory() as d:
        doc = json.loads(SCORECARD_PASS.read_bytes())
        del doc["sample"]["records_expected"]
        sc, sig = _write_signed(d, "case", SCORECARD_TYPE, doc)
        r = run(sc, sig, FIX / "public.pem")
        assert r.returncode == 1, r.stdout
        assert "sample.records_expected is not an integer" in r.stderr


def test_the_coverage_arm_still_fires_now_that_it_reads_expected_unguarded():
    # The control for the change above: dropping the `isinstance(expected, int)`
    # guard from the coverage arm must not have dropped the arm. A well-formed
    # document whose `records_sampled` exceeds its `records_expected` is still
    # refused with the same words.
    with tempfile.TemporaryDirectory() as d:
        sc, sig = _signed_scorecard(
            d,
            **{
                "integrity.records_sampled": 100,
                "integrity.records_sampled_matching": 100,
            },
        )
        r = run(sc, sig, FIX / "public.pem")
        assert r.returncode == 1, r.stdout
        assert (
            "records_sampled (100) exceeds sample.records_expected (75)"
        ) in r.stderr


# --------------------------------------------------------------------------
# Task 5d — the shape layer, closed. Three things, all measured in Task 5c's
# review and none of them theoretical:
#
#   1. The block-presence loop named seven of the ELEVEN required blocks. With
#      any of the other four deleted from a document, `drill verify` exited 1
#      and this script printed VALID — the same disagreement `sample` was in
#      before 1.5.0, four more times.
#   2. `isinstance(v, int)` mirrors serde's TYPE, not `u64`'s DOMAIN. Python's
#      `int` is unbounded, so `records_expected: 2**64` printed VALID here and
#      exited 1 from `drill verify`.
#   3. The loop's ORDER decided which block a multi-missing document was named
#      for, and it was not serde's order, so the two readers named different
#      blocks on the same bytes.
#
# `crates/logweir/tests/two_reader_parity.rs::
# every_required_block_has_a_shape_corpus_case` is the other half of these
# tests: it reads the Rust struct, this script's `REQUIRED_BLOCKS` and
# `shape-index.json` and refuses to let the three drift apart.


def _required_blocks():
    return list(_verifier_module().REQUIRED_BLOCKS)


def test_every_required_block_of_the_rust_struct_is_checked():
    # The list itself, pinned against the Rust struct's own field order. The
    # order is not cosmetic: serde reports the FIRST missing field in
    # declaration order, so any other order makes the two readers name
    # different blocks on a document missing several.
    assert _required_blocks() == [
        "engine",
        "source",
        "target",
        "approval",
        "measured",
        "objectives",
        "sample",
        "target_diff",
        "integrity",
        "topic_parity",
        "evidence",
    ]


def test_a_document_missing_any_required_block_is_refused():
    # One document per block, each `unmodified_example.json` with that one
    # block removed and nothing else touched.
    for block in _required_blocks():
        with tempfile.TemporaryDirectory() as d:
            sc, sig = _signed_scorecard_without(d, block)
            r = run(sc, sig, FIX / "public.pem")
            assert r.returncode == 1, f"{block}: {r.stdout}"
            assert (
                f"the document has no {block} block; it is not a drill scorecard"
            ) in r.stderr, block


def test_two_missing_blocks_are_named_in_serde_struct_order():
    # `measured` + `integrity` is the pair Task 5c's review measured diverging:
    # `drill verify` named `measured` (struct field 13) and this script named
    # `integrity` (17), on the same bytes. `measured` precedes `integrity` in
    # the struct, so `measured` is the answer both readers must give.
    with tempfile.TemporaryDirectory() as d:
        doc = json.loads(SCORECARD_PASS.read_bytes())
        del doc["measured"]
        del doc["integrity"]
        sc, sig = _write_signed(d, "case", SCORECARD_TYPE, doc)
        r = run(sc, sig, FIX / "public.pem")
        assert r.returncode == 1, r.stdout
        assert (
            "the document has no measured block; it is not a drill scorecard"
        ) in r.stderr, r.stderr


def test_a_records_expected_above_the_u64_ceiling_is_refused():
    # `u64::MAX + 1`. `drill verify` exits 1 — `invalid type: floating point
    # 1.8446744073709552e+19, expected u64` — and this script printed VALID for
    # it under every version up to 1.5.0, because Python's `int` is unbounded
    # and `isinstance(v, int)` says nothing about the domain.
    with tempfile.TemporaryDirectory() as d:
        sc, sig = _signed_scorecard(d, **{"sample.records_expected": 2**64})
        r = run(sc, sig, FIX / "public.pem")
        assert r.returncode == 1, r.stdout
        assert (
            "sample.records_expected is outside the u64 domain (0 <= v < 2**64): "
            "18446744073709551616"
        ) in r.stderr, r.stderr


def test_u64_max_itself_is_still_accepted():
    # The boundary control. A domain check written `<= 2**64` or `< 2**64 - 1`
    # would pass the test above and fail here.
    with tempfile.TemporaryDirectory() as d:
        sc, sig = _signed_scorecard(d, **{"sample.records_expected": 2**64 - 1})
        r = run(sc, sig, FIX / "public.pem")
        assert r.returncode == 0, r.stderr
        assert "VALID" in r.stdout


def test_a_negative_records_expected_is_refused_as_shape_not_as_an_invariant():
    # `-1` was refused by both readers before this change and STILL diverged:
    # Rust refused at DESERIALISATION (`invalid value: integer -1, expected
    # u64`) while this script reached an INVARIANT and reported
    # `records_sampled (75) exceeds sample.records_expected (-1)` — the same
    # verdict for a different reason. The domain check moves it to the shape
    # layer, where Rust already had it.
    with tempfile.TemporaryDirectory() as d:
        sc, sig = _signed_scorecard(d, **{"sample.records_expected": -1})
        r = run(sc, sig, FIX / "public.pem")
        assert r.returncode == 1, r.stdout
        assert (
            "sample.records_expected is outside the u64 domain (0 <= v < 2**64): -1"
        ) in r.stderr, r.stderr
        assert "exceeds sample.records_expected" not in r.stderr, r.stderr


def test_every_u64_field_is_domain_checked_not_just_records_expected():
    # A domain check on one field of a type is a reminder, not a rule. All
    # eleven `u64` fields of the document are walked; `phases[].duration_ms` is
    # included because it is a `u64` too, even though `phases` is a list rather
    # than a block.
    cases = {
        "measured.rto_seconds": "measured.rto_seconds",
        "measured.rto_requested_to_verified_seconds": "measured.rto_requested_to_verified_seconds",
        "measured.rto_restore_only_seconds": "measured.rto_restore_only_seconds",
        "measured.rto_excluding_preflight_seconds": "measured.rto_excluding_preflight_seconds",
        "objectives.rto_seconds": "objectives.rto_seconds",
        "sample.records_expected": "sample.records_expected",
        "sample.records_restored": "sample.records_restored",
        "integrity.records_sampled": "integrity.records_sampled",
        "integrity.records_sampled_matching": "integrity.records_sampled_matching",
        "integrity.mismatches": "integrity.mismatches",
    }
    for path, name in cases.items():
        with tempfile.TemporaryDirectory() as d:
            sc, sig = _signed_scorecard(d, **{path: 2**64})
            r = run(sc, sig, FIX / "public.pem")
            assert r.returncode == 1, f"{path}: {r.stdout}"
            assert f"{name} is outside the u64 domain" in r.stderr, path

    with tempfile.TemporaryDirectory() as d:
        doc = json.loads(SCORECARD_PASS.read_bytes())
        assert doc["phases"], "the format example has phase records"
        doc["phases"][0]["duration_ms"] = 2**64
        sc, sig = _write_signed(d, "case", SCORECARD_TYPE, doc)
        r = run(sc, sig, FIX / "public.pem")
        assert r.returncode == 1, r.stdout
        assert "phases[0].duration_ms is outside the u64 domain" in r.stderr, r.stderr


def test_the_u64_field_list_matches_the_rust_struct():
    # The list is derived from `crates/logweir-core/src/scorecard.rs` and has to
    # stay derived: a `u64` field added there with no entry here is a field with
    # no bound at all on the Python side. Counted from the Rust source, so this
    # fails when the struct grows one.
    #
    # THIS TEST IS NOT THE ANCHOR and must not be mistaken for one. It lives in
    # the same file a coordinated deletion touches, which is Task 5d's review
    # finding F1 exactly: deleting one `U64_FIELDS` entry, its case in the test
    # above and this function in one edit left pytest, the parity walker and
    # `scripts/check-invariant-corpus.sh` all green. The anchor is
    # `crates/logweir/tests/two_reader_parity.rs::
    # every_u64_field_has_the_same_domain_check_in_both_readers` plus the
    # arithmetic block in that shell gate — both outside `docs/`. This stays as
    # the fast local check.
    import re

    src = (ROOT / "crates" / "logweir-core" / "src" / "scorecard.rs").read_text()
    rust_u64 = re.findall(r"^    pub ([a-z0-9_]+): (Option<)?u64>?,$", src, re.M)
    rust = {(name, bool(opt)) for name, opt in rust_u64}
    listed = {(path.split(".")[-1], optional)
              for path, optional in _verifier_module().U64_FIELDS}
    assert rust == listed, (sorted(rust), sorted(listed))
    # The optionality is half the claim: five `Option<u64>` accept null and the
    # six plain `u64` do not. Counted over the TUPLE, never over `listed` — the
    # set above collapses `rto_seconds`, which `Measured` and `Objectives` both
    # declare, and a count taken from it reads 4 where the answer is 5. That
    # collapse is also why the dotted name, and not the bare key, is what the
    # anchor in `crates/logweir/tests/two_reader_parity.rs` compares.
    entries = list(_verifier_module().U64_FIELDS)
    assert sum(1 for _, optional in entries if optional) == 5, entries
    assert sum(1 for _, optional in entries if not optional) == 6, entries


def test_null_is_refused_on_every_non_option_u64_field():
    # Task 5d's review, finding F4. `if value is None: continue` skipped the
    # domain check for all eleven `u64` fields, and only five of them are
    # `Option<u64>` in Rust. Measured at `7e85937`: on
    # `sample.records_restored: null`, `integrity.records_sampled: null`,
    # `integrity.records_sampled_matching: null`, `integrity.mismatches: null`
    # and `phases[0].duration_ms: null`, `drill verify` exited 1 with `invalid
    # type: null, expected u64` and this script printed VALID and exited 0.
    # (`sample.records_expected` was the sixth, already covered by the older
    # `is not an integer` check that runs before the loop.)
    for path in (
        "sample.records_expected",
        "sample.records_restored",
        "integrity.records_sampled",
        "integrity.records_sampled_matching",
        "integrity.mismatches",
    ):
        with tempfile.TemporaryDirectory() as d:
            sc, sig = _signed_scorecard(d, **{path: None})
            r = run(sc, sig, FIX / "public.pem")
            assert r.returncode == 1, f"{path}: {r.stdout}"
            assert f"{path} is not an integer" in r.stderr, path

    with tempfile.TemporaryDirectory() as d:
        doc = json.loads(SCORECARD_PASS.read_bytes())
        assert doc["phases"], "the format example has phase records"
        doc["phases"][0]["duration_ms"] = None
        sc, sig = _write_signed(d, "case", SCORECARD_TYPE, doc)
        r = run(sc, sig, FIX / "public.pem")
        assert r.returncode == 1, r.stdout
        assert "phases[0].duration_ms is not an integer" in r.stderr, r.stderr


def test_an_absent_non_option_u64_field_is_refused_too():
    # An absent key reaches the loop as `None` exactly as an explicit null does,
    # and Rust refuses it too — `missing field ...` rather than `invalid type`.
    # Same claim, so the same refusal.
    for block, key in (("sample", "records_restored"), ("integrity", "mismatches")):
        with tempfile.TemporaryDirectory() as d:
            doc = json.loads(SCORECARD_PASS.read_bytes())
            del doc[block][key]
            sc, sig = _write_signed(d, "case", SCORECARD_TYPE, doc)
            r = run(sc, sig, FIX / "public.pem")
            assert r.returncode == 1, f"{block}.{key}: {r.stdout}"
            assert f"{block}.{key} is not an integer" in r.stderr, f"{block}.{key}"


def test_the_option_u64_fields_still_accept_null():
    # THE CONTROL for the change above, and the reason the flag exists at all.
    # These five are `Option<u64>` in Rust: `serde_json` accepts a null and so
    # must this script, or the fix would have traded one two-reader divergence
    # for five others in the opposite direction. Each is set to null on its own,
    # over a document that is otherwise the unmodified format example.
    for path in (
        "measured.rto_seconds",
        "measured.rto_requested_to_verified_seconds",
        "measured.rto_restore_only_seconds",
        "measured.rto_excluding_preflight_seconds",
        "objectives.rto_seconds",
    ):
        with tempfile.TemporaryDirectory() as d:
            sc, sig = _signed_scorecard(d, **{path: None})
            r = run(sc, sig, FIX / "public.pem")
            assert r.returncode == 0, f"{path}: {r.stderr}"
            assert "VALID" in r.stdout, path


# --------------------------------------------------------------------------
# Task 5e — the required NON-BLOCK fields (Task 5d's review, finding F3).
#
# `Scorecard` has six required fields whose type is not a block, so the
# block-presence loop structurally could not hold them. Measured at `7e85937`:
# with `run_id`, `requested_at` or `phases` absent, `drill verify` exited 1 with
# `missing field ...` and this script printed VALID and exited 0; with `outcome`
# absent it refused, but on an INVARIANT about `engine.matrix_verdict`.
#
# `crates/logweir/tests/two_reader_parity.rs::
# every_required_non_block_field_has_a_shape_corpus_case` is the anchor: it
# reads the Rust struct, this script's `REQUIRED_FIELDS` and `shape-index.json`
# and refuses to let the three drift apart.


def _required_fields():
    return list(_verifier_module().REQUIRED_FIELDS)


def test_every_required_non_block_field_of_the_rust_struct_is_checked():
    # The list itself, pinned against the struct's own field order — and, since
    # 1.8.0, against the JSON type each field's Rust type implies. The second
    # element arrived with Task 5f; the anchor for both halves is
    # `crates/logweir/tests/two_reader_parity.rs`, which derives them from
    # `crates/logweir-core/src/scorecard.rs`.
    assert _required_fields() == [
        ("format_version", "string"),        # String
        ("run_id", "string"),                # String
        ("outcome", "string"),               # Outcome, a kebab-case unit enum
        ("last_phase_completed", "integer"), # i8
        ("requested_at", "string"),          # DateTime<Utc>, an RFC 3339 string
        ("phases", "array"),                 # Vec<PhaseRecord>
    ]


def test_a_document_missing_any_required_non_block_field_is_refused():
    # One document per field, each the format example with that one key removed
    # and nothing else touched. `format_version` is the one exception and it is
    # deliberate: the Global Constraint 12 rule runs before this loop and
    # refuses an absent one as `not a parseable semver`, so the loop's own
    # message is unreachable there — argued at `REQUIRED_FIELDS`.
    for name, _type in _required_fields():
        with tempfile.TemporaryDirectory() as d:
            doc = json.loads(SCORECARD_PASS.read_bytes())
            del doc[name]
            sc, sig = _write_signed(d, "case", SCORECARD_TYPE, doc)
            r = run(sc, sig, FIX / "public.pem")
            assert r.returncode == 1, f"{name}: {r.stdout}"
            want = (
                "format_version None is not a parseable semver"
                if name == "format_version"
                else f"the document has no {name} field; it is not a drill scorecard"
            )
            assert want in r.stderr, f"{name}: {r.stderr}"


def test_the_field_loop_runs_after_the_block_loop():
    # NOT cosmetic. serde names the FIRST missing field in declaration order
    # over blocks and non-blocks alike, and `phases` sits between `approval` and
    # `measured`. On a document missing `phases` AND `engine`, `drill verify`
    # names `engine`; running the field loop first would make this script name
    # `phases` and turn a pair the two readers agree on into one they do not.
    with tempfile.TemporaryDirectory() as d:
        doc = json.loads(SCORECARD_PASS.read_bytes())
        del doc["phases"]
        del doc["engine"]
        sc, sig = _write_signed(d, "case", SCORECARD_TYPE, doc)
        r = run(sc, sig, FIX / "public.pem")
        assert r.returncode == 1, r.stdout
        assert (
            "the document has no engine block; it is not a drill scorecard"
        ) in r.stderr, r.stderr


# --------------------------------------------------------------------------
# Task 5f — the wrong-type residual `1.7.0` recorded rather than closed (Task
# 5e's review), and the two derivations its findings F1 and F2 asked for.
#
# Measured at `b99239a` over the release binary and documents derived from
# `e2e/fixtures/invariants/unmodified_example.json`: `run_id: 42` was `drill
# verify` exit 1 (`invalid type: integer 42, expected a string`) against `VALID`
# from `verify_scorecard.py` 1.7.0; `phases: "x"` and `requested_at: 5` the same;
# `outcome: 7` refused here, but on an INVARIANT about `engine.matrix_verdict`.
#
# The anchors are outside this file, as always:
# `crates/logweir/tests/two_reader_parity.rs::every_required_non_block_field_has_
# a_type_shape_corpus_case` and `::every_non_option_u64_field_has_a_null_shape_
# corpus_case`, plus the matching arithmetic in
# `scripts/check-invariant-corpus.sh`.


def test_a_required_non_block_field_of_the_wrong_type_is_refused():
    # One document per field, each the format example with that ONE key retyped
    # to something its Rust type refuses. `format_version` is again the
    # exception, and for the same reason its absent case is: the Global
    # Constraint 12 rule runs first and reports the semver, not the type.
    wrong = {
        "string": 42,
        "integer": "7",
        "array": "x",
        "boolean": "yes",
    }
    words = {"string": "a string", "integer": "an integer", "array": "an array",
             "boolean": "a boolean"}
    for name, want in _required_fields():
        with tempfile.TemporaryDirectory() as d:
            sc, sig = _signed_scorecard(d, **{name: wrong[want]})
            r = run(sc, sig, FIX / "public.pem")
            assert r.returncode == 1, f"{name}: {r.stdout}"
            expected = (
                "format_version 42 is not a parseable semver"
                if name == "format_version"
                else f"{name} is not {words[want]}"
            )
            assert expected in r.stderr, f"{name}: {r.stderr}"


def test_a_bool_is_not_an_integer_for_last_phase_completed():
    # `isinstance(True, int)` is True in Python, so the `int` arm excludes
    # `bool` explicitly — the same trap the u64 loop guards against, on the one
    # required non-block field whose JSON type is a number.
    with tempfile.TemporaryDirectory() as d:
        sc, sig = _signed_scorecard(d, **{"last_phase_completed": True})
        r = run(sc, sig, FIX / "public.pem")
        assert r.returncode == 1, r.stdout
        assert "last_phase_completed is not an integer" in r.stderr, r.stderr


def test_the_wrong_type_check_runs_in_declaration_order_with_the_presence_check():
    # Presence and type are checked per field in ONE pass, not in two sweeps.
    # serde aborts at the first fault it meets while visiting, so on a document
    # whose first fault is a wrong type and whose second is an absent key, Rust
    # names the type. Two sweeps would name the absent key here.
    with tempfile.TemporaryDirectory() as d:
        doc = json.loads(SCORECARD_PASS.read_bytes())
        doc["format_version"] = "1.0.0"
        doc["run_id"] = 42
        del doc["phases"]
        sc, sig = _write_signed(d, "case", SCORECARD_TYPE, doc)
        r = run(sc, sig, FIX / "public.pem")
        assert r.returncode == 1, r.stdout
        assert "run_id is not a string" in r.stderr, r.stderr


def test_the_u64_block_map_is_the_derived_required_block_set():
    # Task 5e's review, finding F2. `_u64_fields`' owner map was a four-entry
    # literal — `measured`, `objectives`, `sample`, `integrity` — inside a task
    # whose whole thesis is that hand-written lists drift. It is now
    # `{name: doc[name] for name in REQUIRED_BLOCKS}`, and this is the test that
    # the map really is that derived set: every owner `U64_FIELDS` names is a
    # required block (or `phases[]`, which is a `Vec` and takes its own branch).
    import inspect

    mod = _verifier_module()
    # (a) the map is BUILT from `REQUIRED_BLOCKS`, not written out. Asserted on
    #     the source, because a literal that happens to list today's four blocks
    #     is behaviourally identical until the day a `u64` lands in a fifth —
    #     which is the day it raises `KeyError` instead.
    src = inspect.getsource(mod._u64_fields)
    assert "blocks = {name: doc[name] for name in REQUIRED_BLOCKS}" in src, src
    assert "blocks[block]" not in src, src
    # (b) and every owner the u64 list names really is one of those blocks
    #     (`phases[]` is a `Vec` and takes its own branch).
    owners = {path.split(".", 1)[0] for path, _ in mod.U64_FIELDS}
    owners.discard("phases[]")
    assert owners <= set(mod.REQUIRED_BLOCKS), (
        sorted(owners), list(mod.REQUIRED_BLOCKS))
    doc = json.loads(SCORECARD_PASS.read_bytes())
    got = {name for name, _v, _o in mod._u64_fields(doc)}
    assert "sample.records_expected" in got and "integrity.mismatches" in got, got
    assert "phases[0].duration_ms" in got, got


def test_a_u64_owned_by_a_block_outside_the_old_literal_does_not_raise():
    # The regression F2 named: Task 5e's derivation walks ALL of `Scorecard`'s
    # struct-typed fields, so a `u64` added to (say) `TargetInfo` is REQUIRED by
    # the walker and the shell gate to appear in `U64_FIELDS` as
    # `target.<name>` — and `blocks["target"]` raised `KeyError` on every
    # document, out of the one function whose docstring promises that no field
    # access ever surfaces as a traceback. Simulated here by adding exactly that
    # entry to a freshly loaded module.
    mod = _verifier_module()
    mod.U64_FIELDS = tuple(mod.U64_FIELDS) + (("target.topic_mapping_entries", False),)
    doc = json.loads(SCORECARD_PASS.read_bytes())
    got = dict((name, value) for name, value, _o in mod._u64_fields(doc))
    assert got["target.topic_mapping_entries"] == doc["target"]["topic_mapping_entries"]
    assert mod.check_invariants(doc) == "", "the added entry must not change the verdict"


# --------------------------------------------------- Task 5b: the backup receipt
# The second reader's half of the backup receipt. Every test below drives the
# SHIPPED SCRIPT as a subprocess over a document it signs with the checked-in
# throwaway fixture key, because the auditor's copy is what the two-reader claim
# is about; the pure-function tests reach `check_backup_receipt_invariants`
# directly, which is what makes an arm's exact message assertable.

BACKUP_RECEIPT = json.loads(
    (FIX / "backup-receipt.json").read_bytes()
) if (FIX / "backup-receipt.json").exists() else None


def _receipt(**overrides):
    """The checked-in signed receipt as a dict, with dotted overrides applied.

    Derived from the FIXTURE rather than hand-written, so a document this
    script accepts here is a document the Rust minter really produced and
    `BackupReceipt::validate_invariants` really accepted.
    """
    doc = json.loads(json.dumps(BACKUP_RECEIPT))
    for path, value in overrides.items():
        parts = path.split(".")
        node = doc
        for part in parts[:-1]:
            node = node[part]
        if value is _DELETE:
            node.pop(parts[-1], None)
        else:
            node[parts[-1]] = value
    return doc


_DELETE = object()


def test_the_signed_backup_receipt_fixture_verifies_under_its_own_type():
    r = run_typed("backup-receipt", FIX / "backup-receipt.json",
                  FIX / "backup-receipt.sig", FIX / "public.pem")
    assert r.returncode == 0, r.stderr
    assert "VALID" in r.stdout
    # The verdict must say WHICH invariant set ran, or an exit 0 that checked
    # only the signature is indistinguishable from this one.
    assert "backup-receipt invariant set" in r.stdout, r.stdout
    # The window's end is EXCLUSIVE (I22) and the printer says so, because that
    # is the one thing a reader can get wrong by a whole record.
    assert "the end is EXCLUSIVE" in r.stdout, r.stdout
    assert "Traceback" not in r.stderr


def test_a_backup_receipt_never_verifies_as_a_scorecard():
    # The default path must keep refusing it: that refusal is the property the
    # payload type exists to give.
    r = run(FIX / "backup-receipt.json", FIX / "backup-receipt.sig", FIX / "public.pem")
    assert r.returncode == 1
    assert "unexpected payloadType" in r.stdout + r.stderr


def test_a_scorecard_never_verifies_as_a_backup_receipt():
    r = run_typed("backup-receipt", FIX / "scorecard.json",
                  FIX / "scorecard.sig", FIX / "public.pem")
    assert r.returncode == 1
    assert "unexpected payloadType" in r.stdout + r.stderr


def test_a_flipped_byte_in_a_backup_receipt_fails_under_the_right_type():
    # Selecting the right payload type must not become a way to pass.
    with tempfile.TemporaryDirectory() as d:
        bad = pathlib.Path(d) / "bad.json"
        raw = bytearray((FIX / "backup-receipt.json").read_bytes())
        raw[raw.index(b"1")] = ord("2")
        bad.write_bytes(bytes(raw))
        r = run_typed("backup-receipt", bad, FIX / "backup-receipt.sig", FIX / "public.pem")
        assert r.returncode == 1
        assert "does not verify" in r.stdout + r.stderr


def test_each_backup_receipt_arm_refuses_with_its_exact_message():
    # THE REFUSAL TEXT IS THE INTERFACE. Every string here is asserted in FULL
    # against `crates/logweir-core/tests/backup_receipt.rs`'s own assertions —
    # a `contains` would let the two readers say different things and still go
    # green, which is the exact defect the parity gate exists because of.
    check = _verifier_module().check_backup_receipt_invariants
    cases = [
        (
            _receipt(format_version="2.0.0"),
            'format_version "2.0.0" is not a 1.x version this reader understands',
        ),
        (
            # Arm 1 is STRICTER than the scorecard's `_major`: the whole string
            # must be three integers.
            _receipt(format_version="1.0"),
            'format_version "1.0" is not a 1.x version this reader understands',
        ),
        (
            _receipt(**{"archive.manifest_key": "   "}),
            "exit_code 0 and manifest_key absent disagree: a receipt names a manifest "
            "if and only if the backup exited 0",
        ),
        (
            _receipt(exit_code=1),
            'exit_code 1 and manifest_key "logweir/backups/logweir-backup-01J8Z9QK7V/'
            'manifest.json" disagree: a receipt names a manifest if and only if the '
            "backup exited 0",
        ),
        (
            _receipt(records={"orders": 1}),
            'records covers {"orders"} but the named topic set is {"orders", "payments"}',
        ),
        (
            _receipt(records={"orders": 1, "payments": 2, "invoices": 3}),
            'records covers {"invoices", "orders", "payments"} but the named topic set '
            'is {"orders", "payments"}',
        ),
        (
            _receipt(**{"covered.from_ms": 2, "covered.to_ms": 1}),
            "covered.from_ms 2 is not before covered.to_ms 1: the covered window's end "
            "is EXCLUSIVE, so an empty range covers no record",
        ),
        (
            # F3: the end is EXCLUSIVE, so an empty range is refused. Task 5
            # accepted this document.
            _receipt(**{"covered.from_ms": 7, "covered.to_ms": 7}),
            "covered.from_ms 7 is not before covered.to_ms 7: the covered window's end "
            "is EXCLUSIVE, so an empty range covers no record",
        ),
    ]
    for doc, want in cases:
        assert check(doc) == want, (check(doc), want)
    # …and the arms are not simply always refusing.
    assert check(_receipt()) == ""
    # A one-millisecond window is a real window: `[t, t+1)` is what a
    # single-record backup produces.
    assert check(_receipt(**{"covered.from_ms": 7, "covered.to_ms": 8})) == ""


def test_a_backup_receipt_missing_a_block_is_one_line_not_a_traceback():
    # The Rust reader gets this layer from `serde_json` and reports "the
    # payload is not a backup receipt: missing field `covered`" WITHOUT
    # reaching an invariant. This script has no such layer, so the shape is
    # asserted before the arms — and the contract is that no field access ever
    # surfaces as a traceback.
    mod = _verifier_module()
    for path, name in [("covered", "covered"), ("archive", "archive"),
                       ("records", "records"), ("source", "source")]:
        doc = _receipt(**{path: _DELETE})
        assert mod._receipt_shape(doc) == (
            f"the document has no {name} block; it is not a backup receipt")
    assert mod._receipt_shape(_receipt(run_id=_DELETE)) == (
        "the document has no run_id field; it is not a backup receipt")
    assert mod._receipt_shape(_receipt(exit_code="0")) == "exit_code is not an integer"
    # End to end, through the shipped script: one INVALID line, no traceback.
    with tempfile.TemporaryDirectory() as d:
        doc = _receipt(covered=_DELETE)
        pth, sig = _write_signed(d, "receipt", BACKUP_RECEIPT_TYPE, doc)
        r = run_typed("backup-receipt", pth, sig, FIX / "public.pem")
        assert r.returncode == 1, r.stdout
        assert "Traceback" not in r.stderr, r.stderr
        assert "no covered block" in r.stderr, r.stderr


def test_script_version_was_bumped_with_the_payload_type_map():
    # Task 5b's acceptance. `SCRIPT_VERSION` tells an auditor WHICH checks ran,
    # and the fourth payload type came with a whole invariant set — so the two
    # must have moved together. A version left at 1.8.0 beside a four-entry map
    # is a document claiming it was checked by a reader that did not know the
    # type it was handed.
    #
    # 1.10.0 (fix round 1) closed the auth mode's value set in BOTH documents:
    # one more arm in `check_invariants` and one more in
    # `check_backup_receipt_invariants`, which is an invariant-set change and
    # therefore a minor bump, with the payload-type map unchanged at four.
    #
    # 1.11.0 (Task 9b) added one more arm — the `evidence.offset_report_*` pair
    # — with the payload-type map still at four. `task-9b-brief.md` says
    # "1.9.0 -> 1.10.0"; 1.10.0 was already taken by 5b's fix round, so the
    # brief's number is the error and this is the bump.
    #
    # 1.12.0 (Task 9b fix round 1, review F1) added one more arm again —
    # `target.marker_topic` present unless `target.mode` is `newTopic` — plus
    # the new nested optional `target.mode` it reads. Map still four.
    mod = _verifier_module()
    assert len(mod.PAYLOAD_TYPES) == 4, sorted(mod.PAYLOAD_TYPES)
    assert mod.SCRIPT_VERSION == "1.12.0", mod.SCRIPT_VERSION
    assert "backup-receipt" in mod.PAYLOAD_TYPES
    assert mod.PAYLOAD_TYPES["backup-receipt"] == BACKUP_RECEIPT_TYPE


def test_the_payload_type_resolver_accepts_every_short_name_and_media_type():
    mod = _verifier_module()
    for short, media in mod.PAYLOAD_TYPES.items():
        assert mod.resolve_payload_type(short) == media
        # Clause 2: a full media type passes straight through.
        assert mod.resolve_payload_type(media) == media


def test_the_target_auth_arms_refuse_with_their_exact_messages():
    # Global Constraint 12's price for `target.auth` (Task 5b declares the
    # shape; Task 6 fills it). ABSENT IS LEGAL — every scorecard this tree has
    # written has no block — and BLANK IS NOT PLAINTEXT (ruling R-A).
    mod = _verifier_module()
    base = json.loads(SCORECARD_PASS.read_bytes())
    assert mod.check_invariants(base) == "", "no auth block at all must stay legal"

    def with_auth(auth):
        doc = json.loads(json.dumps(base))
        doc["target"]["auth"] = auth
        return doc

    assert mod.check_invariants(with_auth({"mode": "plaintext"})) == ""
    assert mod.check_invariants(
        with_auth({"mode": "scramSha512", "username": "logweir"})) == ""
    assert mod.check_invariants(with_auth(None)) == "", "null is absent"
    assert mod.check_invariants(with_auth({"mode": "   ", "username": "logweir"})) == (
        "target.auth names a username with no auth mode; a username without its "
        "mechanism is not a record of how the client authenticated"
    )
    assert mod.check_invariants(with_auth({"mode": ""})) == (
        "target.auth.mode is blank; an absent auth block is how a scorecard says plaintext"
    )

    # THE VALUE SET IS CLOSED (SCRIPT_VERSION 1.10.0, controller ruling). The
    # first value is the spelling this product's own receipt writer used until
    # the same round; the second is the one Task 5b's review signed and got
    # 0/0 from BOTH readers, which is the defect this arm removes. The trailing
    # case pins that the comparison is on the EXACT value, like the Rust arm's.
    closed = (
        "target.auth.mode is not one of the two values this format defines; it "
        'is "plaintext" or "scramSha512" and nothing else'
    )
    for mode in ["scram-sha-512", "totally-made-up", "SCRAMSHA512", " plaintext "]:
        assert mod.check_invariants(with_auth({"mode": mode})) == closed, mode
    # …and a username beside a good mode is still fine, so the arm cannot pass
    # by refusing every block that has one.
    assert mod.check_invariants(
        with_auth({"mode": "scramSha512", "username": "logweir"})) == ""


def test_the_receipts_auth_mode_value_set_is_closed_at_this_reader():
    # Arm 5, mirrored (Task 5b fix round 1). `Backup.status.auth.mode` is
    # copied FROM this field by the operator and its CRD description promises
    # these two values, so a third spelling reaching an auditor as an attested
    # claim is the failure this arm exists to prevent. The messages are
    # byte-identical to `BackupReceipt::validate_invariants`' arm 5, which
    # `scripts/check-invariant-corpus.sh` re-derives from both sources.
    mod = _verifier_module()
    base = json.loads((FIX / "backup-receipt.json").read_bytes())
    assert base["source"]["auth"]["mode"] == "scramSha512", (
        "the checked-in receipt fixture must carry the product's ONE spelling"
    )
    assert mod.check_backup_receipt_invariants(base) == ""

    def with_mode(mode):
        doc = json.loads(json.dumps(base))
        doc["source"]["auth"]["mode"] = mode
        return doc

    assert mod.check_backup_receipt_invariants(with_mode("plaintext")) == ""
    for mode in ["scram-sha-512", "totally-made-up", "SCRAMSHA512", " plaintext "]:
        assert mod.check_backup_receipt_invariants(with_mode(mode)) == (
            f"source.auth.mode {mod._rust_debug_str(mode)} is not one of the two values "
            'this format defines: "plaintext" or "scramSha512"'
        ), mode

    # A receipt with no `source.auth` block at all is refused in the SHAPE
    # layer, which is where Rust refuses it (serde, before any arm runs) —
    # never as an invariant, or the two readers would refuse one document in
    # two different layers with two different sentences.
    doc = json.loads(json.dumps(base))
    del doc["source"]["auth"]
    assert mod._receipt_shape(doc) == (
        "the document has no source.auth block; it is not a backup receipt"
    )
    doc = json.loads(json.dumps(base))
    doc["source"]["auth"]["mode"] = 512
    assert mod._receipt_shape(doc) == "source.auth.mode is not a string"
