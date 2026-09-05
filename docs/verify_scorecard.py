#!/usr/bin/env python3
"""Verify a Logweir drill scorecard against its DSSE sidecar signature.

This script is the auditor's independent check: it does not use, import, or
trust anything from Logweir's own Rust codebase. It re-implements the DSSE
v1 envelope from the specification, so if this script and `logweir drill
verify` disagree, the signed-scorecard format is broken, not merely this
script.

Requires only the `cryptography` package:

  pip install cryptography
  python3 verify_scorecard.py scorecard.json scorecard.sig public.pem

A Logweir drill publishes THREE signed documents, each under its own DSSE
payload type. `--payload-type` selects which one is being checked; the default
is the scorecard, so the three-positional-argument form above is unchanged.

  scorecard  application/vnd.logweir.drill-scorecard+json;version=1.0.0   (default)
  receipt    application/vnd.logweir.drill-put-receipt+json;version=1.0.0
  teardown   application/vnd.logweir.drill-teardown+json;version=1.0.0

  python3 verify_scorecard.py --payload-type receipt \
      <run_id>.receipt.json <run_id>.receipt.sig public.pem

The full media type may be given instead of the short name. Passing the wrong
one is a REFUSAL, not a warning: a sidecar for one kind of document must never
be accepted as the signature over another, which is the substitution the
payload type exists to prevent.

Exit 0 = the signature verifies over the bytes of scorecard.json exactly as
          stored, and the document does not contradict itself.
Exit 1 = it does not — or any of the three inputs cannot be read, parsed,
          or understood (a mistyped path, a truncated sidecar, a malformed
          PEM, or a public key of a type this script does not support).
          Every such case prints a one-line `INVALID: ...` reason to
          stderr; none of them should ever surface as a raw traceback.

DSSE v1 Pre-Authentication Encoding (PAE), from the DSSE specification
(https://github.com/secure-systems-lab/dsse/blob/master/protocol.md):

    PAE(type, body) = "DSSEv1" SP LEN(type) SP type SP LEN(body) SP body

SP is a single 0x20 byte. LEN is the ASCII-decimal count of BYTES, not
characters — for an ASCII payload type the two coincide, but a
naive `len(payload_type)` in Python counts *code points*, which is only
byte-correct for ASCII. Get this wrong and every fixture in this repository
still verifies (they are pure ASCII), which is exactly the trap: a
non-ASCII payload type would then sign one length prefix and verify against
another, and the bug would ship. Signing the codec, not just the plaintext.

Sidecar shape (the checked-in `.sig` files):

    {
      "payloadType": "application/vnd.logweir.drill-scorecard+json;version=1.0.0",
      "signatures": [ { "keyid": "<lowercase-hex-sha256-of-SPKI-DER>", "sig": "<base64>" } ]
    }

The one asymmetry worth stating twice: `sig` is base64 of **DER** for an
ECDSA P-256 key, but base64 of the **raw 64-byte** R||S value for Ed25519.
There is no ASN.1 for Ed25519 here — the format simply differs by key type.

IMPORTANT — this script does not solve key distribution. It only checks
that the signature over `scorecard.json` verifies under whatever
`public.pem` you hand it. If `public.pem` arrived from the same place as
the other two files, a successful VALID proves only that the three files
are mutually consistent, not that they came from the publisher you think
they did. See docs/verify-a-scorecard.md, "Where the public key comes
from", before trusting a VALID result.
"""
import base64
import binascii
import json
import sys

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec, ed25519

PAYLOAD_TYPE = "application/vnd.logweir.drill-scorecard+json;version=1.0.0"

# The three payload types Logweir signs. Keep byte-for-byte in step with
# `crates/logweir-evidence/src/lib.rs`'s PAYLOAD_TYPE_SCORECARD,
# PAYLOAD_TYPE_PUT_RECEIPT and PAYLOAD_TYPE_TEARDOWN; `docs/
# test_verify_scorecard.py::test_the_three_payload_types_match_the_rust_constants`
# fails if they ever drift.
PAYLOAD_TYPES = {
    "scorecard": PAYLOAD_TYPE,
    "receipt": "application/vnd.logweir.drill-put-receipt+json;version=1.0.0",
    "teardown": "application/vnd.logweir.drill-teardown+json;version=1.0.0",
}


def resolve_payload_type(name: str) -> str:
    """Short name -> media type, or a full media type passed straight through.

    An unknown value is an ERROR rather than a silent passthrough of anything
    that happens to contain a slash: a typo'd media type would otherwise turn
    into "unexpected payloadType" and read like a bad artifact rather than a
    bad command line.
    """
    if name in PAYLOAD_TYPES:
        return PAYLOAD_TYPES[name]
    if name in PAYLOAD_TYPES.values():
        return name
    raise ValueError(
        f"unknown --payload-type {name!r}; use one of "
        + ", ".join(sorted(PAYLOAD_TYPES)) + " or a full media type"
    )


def pae(payload_type: str, payload: bytes) -> bytes:
    """DSSE v1 Pre-Authentication Encoding of (payload_type, payload).

    LEN is a BYTE count: `payload_type.encode()` first, then `len()` on the
    resulting bytes. `len(payload_type)` alone would count characters and
    silently diverge from the signer on the first non-ASCII payload type.
    """
    type_bytes = payload_type.encode("utf-8")
    return b"".join([
        b"DSSEv1 ",
        str(len(type_bytes)).encode(), b" ", type_bytes, b" ",
        str(len(payload)).encode(), b" ", payload,
    ])


def verify_signature(public_key, message: bytes, signature: bytes) -> bool:
    """True iff `signature` is a valid signature over `message` by `public_key`.

    Callers must have already confirmed `public_key` is EC or Ed25519 —
    this function's `else` branch assumes EC. ECDSA P-256 signatures are
    DER-encoded; Ed25519 signatures are the raw 64-byte value.
    `cryptography`'s `verify()` raises InvalidSignature both for a genuine
    mismatch and for a signature blob that fails to decode (bad DER, wrong
    length) — both are "this does not check out", which is exactly the one
    bit this function reports.
    """
    try:
        if isinstance(public_key, ed25519.Ed25519PublicKey):
            public_key.verify(signature, message)
        else:
            public_key.verify(signature, message, ec.ECDSA(hashes.SHA256()))
        return True
    except InvalidSignature:
        return False


def main(
    scorecard_path: str,
    sig_path: str,
    pubkey_path: str,
    payload_type_wanted: str = PAYLOAD_TYPE,
) -> int:
    # The payload is the bytes as stored on disk, byte for byte, including
    # any trailing newline. Re-serialising the parsed JSON before verifying
    # would check a signature over a document nobody actually signed or
    # published — exactly the substitution this format is built to catch.
    try:
        payload = open(scorecard_path, "rb").read()
    except OSError as e:
        print(f"INVALID: cannot read scorecard {scorecard_path!r}: {e}", file=sys.stderr)
        return 1

    try:
        sidecar = json.load(open(sig_path))
    except OSError as e:
        print(f"INVALID: cannot read signature {sig_path!r}: {e}", file=sys.stderr)
        return 1
    except json.JSONDecodeError as e:
        print(f"INVALID: {sig_path!r} is not valid JSON: {e}", file=sys.stderr)
        return 1

    payload_type = sidecar.get("payloadType")
    if payload_type != payload_type_wanted:
        print(
            f"INVALID: unexpected payloadType {payload_type!r} "
            f"(expected {payload_type_wanted!r}; pass --payload-type to check "
            "a receipt or a teardown attestation)",
            file=sys.stderr,
        )
        return 1

    signatures = sidecar.get("signatures") or []
    if not signatures:
        print("INVALID: sidecar has no signatures", file=sys.stderr)
        return 1
    sig_b64 = signatures[0].get("sig", "")
    try:
        signature = base64.b64decode(sig_b64, validate=True)
    except (binascii.Error, ValueError) as e:
        print(f"INVALID: sig is not valid base64: {e}", file=sys.stderr)
        return 1

    try:
        pubkey_bytes = open(pubkey_path, "rb").read()
    except OSError as e:
        print(f"INVALID: cannot read public key {pubkey_path!r}: {e}", file=sys.stderr)
        return 1
    try:
        public_key = serialization.load_pem_public_key(pubkey_bytes)
    except ValueError as e:
        print(f"INVALID: {pubkey_path!r} is not a valid PEM public key: {e}", file=sys.stderr)
        return 1

    # Only ECDSA P-256 and Ed25519 are defined by this format (see the
    # module docstring's asymmetry note). Anything else — RSA, a different
    # EC curve, and so on — has no defined `sig` encoding here, so it is
    # reported as unsupported rather than fed into the EC verify path
    # below, which would raise a raw TypeError on a non-EC key.
    if not isinstance(public_key, (ec.EllipticCurvePublicKey, ed25519.Ed25519PublicKey)):
        print(
            f"INVALID: unsupported public key type {type(public_key).__name__}; "
            "only ECDSA P-256 and Ed25519 are supported",
            file=sys.stderr,
        )
        return 1

    message = pae(payload_type, payload)
    if not verify_signature(public_key, message, signature):
        print("INVALID: signature does not verify over these bytes", file=sys.stderr)
        return 1

    # The signature checks out; now ask whether the document is internally
    # consistent. A signature only proves who wrote the bytes, not that the
    # bytes make sense.
    #
    # These checks and this summary are SCORECARD-SPECIFIC. They are guarded on
    # the payload type rather than attempted over every document: a receipt has
    # no `integrity` block, and reaching for one would raise a KeyError — i.e.
    # the traceback this script's contract promises never to emit.
    doc = json.loads(payload)
    if payload_type_wanted == PAYLOAD_TYPES["scorecard"]:
        integrity = doc["integrity"]
        if integrity["result"] == "partial" and not integrity.get("partial_reason"):
            print("INVALID: integrity.result is 'partial' with no partial_reason", file=sys.stderr)
            return 1

        measured = doc["measured"]
        print(f"VALID  run_id={doc['run_id']}  outcome={doc['outcome']}")
        print(f"       rto_seconds={measured['rto_seconds']}  rpo_seconds={measured['rpo_seconds']}")
        print(f"       integrity={integrity['level']}/{integrity['result']}")
        if doc["approval"].get("self_attested"):
            print("       approval: SELF-ATTESTED — the approval key equals the signing key")
        # The four `evidence` fields are zeroed BEFORE signing, because they
        # describe an upload that has not happened yet. Say so, so nobody reads
        # the zeroes as a finding about their bucket.
        print(
            "       evidence: the four post-put fields are zeroed before signing; "
            "the storage facts live in the receipt"
        )
        return 0

    if payload_type_wanted == PAYLOAD_TYPES["receipt"]:
        # The receipt's whole job is to bind to ONE scorecard. Print the
        # binding, and say plainly what a `false` does and does not mean.
        print(f"VALID  receipt for {doc['scorecard_key']}")
        print(f"       binds to scorecard {doc['scorecard_sha256']}")
        print(
            f"       create_only_enforced={doc['create_only_enforced']}  "
            f"immutable={doc['immutable']}  version_id={doc.get('version_id')}"
        )
        print(f"       observed_at={doc['observed_at']}")
        if not doc["create_only_enforced"]:
            print(
                "       NOTE: create_only_enforced=false means the backend answered "
                "'not supported' and Logweir took a HEAD-then-PUT fallback. It is NOT "
                "a finding that the object was overwritten."
            )
        print(
            "       This signature covers the receipt only. Verify the scorecard "
            "separately, then check sha256(scorecard.json) equals the digest above."
        )
        return 0

    # teardown, and any future type: the signature is what was asked for, and
    # this script does not invent semantics for a document it does not model.
    print(f"VALID  payloadType={payload_type_wanted}")
    print(f"       {len(payload)} bytes verified; no document-specific checks apply")
    return 0


if __name__ == "__main__":
    # argparse is deliberately NOT used: this script's only dependency is
    # `cryptography`, and its argument surface is three positionals plus one
    # optional flag. Hand-parsing keeps the whole thing readable by an auditor
    # who is checking that it does not phone home or trust anything it was not
    # given.
    argv = sys.argv[1:]
    wanted = PAYLOAD_TYPE
    rest = []
    i = 0
    while i < len(argv):
        a = argv[i]
        if a == "--payload-type":
            if i + 1 >= len(argv):
                print("INVALID: --payload-type needs a value", file=sys.stderr)
                sys.exit(1)
            try:
                wanted = resolve_payload_type(argv[i + 1])
            except ValueError as exc:
                print(f"INVALID: {exc}", file=sys.stderr)
                sys.exit(1)
            i += 2
            continue
        if a.startswith("--payload-type="):
            try:
                wanted = resolve_payload_type(a.split("=", 1)[1])
            except ValueError as exc:
                print(f"INVALID: {exc}", file=sys.stderr)
                sys.exit(1)
            i += 1
            continue
        rest.append(a)
        i += 1
    if len(rest) != 3:
        print(
            f"usage: {sys.argv[0]} [--payload-type scorecard|receipt|teardown] "
            "<document.json> <document.sig> <public.pem>",
            file=sys.stderr,
        )
        sys.exit(1)
    sys.exit(main(rest[0], rest[1], rest[2], wanted))
