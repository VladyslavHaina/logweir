#!/usr/bin/env python3
"""Verify a Logweir drill scorecard against its DSSE sidecar signature.

This script is the auditor's independent check: it does not use, import, or
trust anything from Logweir's own Rust codebase. It re-implements the DSSE
v1 envelope from the specification, so if this script and `logweir drill
verify` disagree, the signed-scorecard format is broken, not merely this
script.

THAT CLAIM IS LOAD-BEARING AND IT IS WHY `check_invariants` BELOW IS AS LONG
AS IT IS. This script used to implement exactly ONE of the ~12 checks
`Scorecard::validate_invariants` performs, and parsed no other field — so a
document with `format_version: "2.0.0"`, or `measured.rpo_seconds: -90`, or
`records_sampled_matching: 9999` over `records_sampled: 10`, printed a clean
`VALID` here and was refused by `logweir drill verify`. The `-90` case is the
exact value `docs/formats/drill-scorecard.md` says a reader "would most likely
read as no data loss", printed under a VALID banner by the tool the auditor is
told to trust MORE. Every arm below mirrors one arm of the Rust validator, in
the same order and with the same wording, so a disagreement is a bug in the
format rather than an artefact of one implementation being shorter.

ONE CHECK DELIBERATELY SITS OUTSIDE `check_invariants`: the approval-claim
derivation (T0-1). `approval.self_attested` is a claim the document makes about
its own provenance, and deciding whether it is TRUE needs the verifying key —
which `Scorecard::validate_invariants` does not have and never will. So the
Rust puts that comparison in `crates/logweir/src/verify.rs`, after the
invariant set and before the summary, and this script puts it in exactly the
same place in `main`. It is still arm-for-arm with the Rust; it is just a
different Rust function. Both readers refuse a document whose claim disagrees
with the derivation, with the same message text.

The DSSE core — `pae`, `verify_signature` and the signature check in `main` —
is unchanged and is still about twenty lines. `check_invariants` is separate,
runs only AFTER the signature has verified, and answers a different question:
a signature proves who wrote the bytes, never that the bytes make sense.

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
Exit 2 = THIS SCRIPT COULD NOT RUN — its one dependency is missing. It is
          deliberately NOT 1: exit 1 is a verdict on the document, and
          "the verifier would not start" must never be mistaken for
          "the signature did not check out". Nothing was verified.

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
import hashlib
import json
import sys

# `cryptography` is this script's ONE third-party dependency, and it is not in
# the standard library, so a fresh machine hits this line first. An uncaught
# ImportError prints a traceback whose last line names a module the reader has
# to go and look up; worse, the interpreter exits 1, which this script's own
# contract defines as "the signature did not verify". Neither is acceptable in
# the tool an auditor reaches for, so say what to install and exit 2.
try:
    from cryptography.exceptions import InvalidSignature
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec, ed25519
except ImportError as _exc:  # pragma: no cover - exercised via a subprocess test
    print(
        f"CANNOT RUN: this script needs the `cryptography` package ({_exc}).\n"
        "\n"
        "    pip install cryptography\n"
        "\n"
        "or, without touching your system Python:\n"
        "\n"
        "    python3 -m venv venv && venv/bin/pip install cryptography\n"
        "    venv/bin/python3 verify_scorecard.py <document.json> <document.sig> <public.pem>\n"
        "\n"
        "NOTHING WAS VERIFIED. This is exit 2, not exit 1: it is not a verdict\n"
        "on the document.",
        file=sys.stderr,
    )
    raise SystemExit(2)

PAYLOAD_TYPE = "application/vnd.logweir.drill-scorecard+json;version=1.0.0"

# Global Constraint 12, and the single value this reader's major-version
# refusal is measured against. Keep in step with `logweir_core::FORMAT_VERSION`
# (crates/logweir-core/src/lib.rs); `docs/test_verify_scorecard.py::
# test_the_format_version_matches_the_rust_constant` fails if they drift.
FORMAT_VERSION = "1.0.0"

# This SCRIPT's own version — NOT the format version (GC12: FORMAT_VERSION stays
# "1.0.0"). Bumped whenever this script's VERDICT RULE changes: when there is a
# document it would decide differently from the previous version. That is
# strictly wider than "the invariant set changed", and it has to be — the
# `verifier:` line below is the only thing an auditor can read to learn which
# checks produced the verdict in front of them, and a verifier whose answer
# moves while its version string stays put is one nobody can reason about.
#
#   1.1.0  adds the evidence-zeroing arm (T0-2). An invariant arm.
#   1.2.0  DERIVES `approval.self_attested` from the key that verified the
#          signature instead of echoing the document's own claim, and refuses a
#          document whose claim disagrees (T0-1). NOT an invariant arm — it
#          needs the verifying key, which `check_invariants` has not got — but
#          it changes the verdict on real documents, which is what the bump
#          tracks.
#   1.3.0  the `partial_reason` arm now refuses BLANK as well as null
#          (`""` and `"   "`, T0-6 / ruling R-A — `""` was the live
#          Rust/Python disagreement and `"   "` was accepted by both), and
#          the `redactions` arm is new (T0-3). Two invariant arms.
#   1.4.0  `outcome` is READ FOR THE FIRST TIME (T0-4). Six arms make the
#          headline field an entailment of the rest of the document rather
#          than a free-standing label: `pass` implies a `pass` integrity
#          result, no non-blank `partial_reason`, `objectives.met` not
#          `false`, and every sampled record matching; `records_sampled`
#          never exceeds `sample.records_expected`; and a `pass` matrix
#          verdict implies the drill passed at byte-fingerprint level.
#   1.5.0  SHAPE, not an invariant arm — and a bump for exactly the reason the
#          paragraph above gives, because there are documents this script now
#          decides differently. `sample` joins the required-block list and
#          `sample.records_expected` must be an integer. Both are properties
#          the Rust reader gets from its own types and refuses at
#          deserialisation; here they were a silent skip, so a scorecard with
#          no `sample` block at all — or with `"records_expected": "75"` —
#          printed VALID from this script while `logweir drill verify` exited
#          1 on the same bytes. Measured, at `0640240`, not argued. That is
#          the two-reader disagreement this file's own module comment says is
#          impossible, so it is closed rather than recorded (Task 5c).
#
# ANY task that adds or removes an arm in `check_invariants` bumps this minor
# and updates the parenthetical in the success line below, IN THE SAME COMMIT.
# A version whose whole purpose is to tell an auditor which checks ran is worth
# nothing if it can go stale silently.
#
# The general rule for when to bump, and where an auditor reads it, is not
# written down anywhere yet; Task 23 owns writing it. Until it is, err towards
# bumping: an unnecessary bump costs an auditor one question, a missing one
# costs them a wrong answer.
SCRIPT_VERSION = "1.5.0"

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


def key_id(public_key) -> str:
    """The sidecar `keyid`: lowercase hex sha256 of the key's SPKI DER.

    This is how a sidecar says WHICH key signed, and it is how Logweir's own
    verifier selects the signature to check. Computing it here rather than
    ignoring `keyid` altogether is the difference between "some signature in
    this sidecar verifies under your key" and "the signature that CLAIMS to be
    by your key verifies under it" — and, more to the point, it is what makes
    this script and `logweir drill verify` reach the same verdict on a sidecar
    whose keyid names a different key.
    """
    der = public_key.public_bytes(
        serialization.Encoding.DER,
        serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    return hashlib.sha256(der).hexdigest()


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


def _major(version: str):
    """Leading integer of a dotted version string, or None if there is none.

    Mirrors `logweir_core::scorecard::major_version` exactly: split on ".",
    take the first field, parse it as an integer. Anything else is "not a
    parseable semver", which is a refusal rather than an assumption.
    """
    head = version.split(".")[0] if isinstance(version, str) else ""
    try:
        return int(head)
    except ValueError:
        return None


def _finite(x) -> bool:
    """True iff `x` is a finite number. JSON has no NaN/Infinity literal, but
    `json.loads` accepts the non-standard `NaN`/`Infinity` tokens by default,
    so a document carrying one reaches here as a float."""
    return isinstance(x, (int, float)) and not isinstance(x, bool) and x == x and abs(x) != float("inf")


def check_invariants(doc) -> str:
    """The scorecard's self-consistency rules, or "" when the document holds.

    ARM FOR ARM, IN ORDER, WITH `Scorecard::validate_invariants`
    (crates/logweir-core/src/scorecard.rs). The two implementations are
    documented as reaching the same verdict — `docs/verify-a-scorecard.md`
    says a disagreement "is a bug in the format" — and for one release they
    did not: this function checked one rule and the Rust checked twelve.

    A signature proves who wrote the bytes; these rules ask whether the bytes
    make sense. Both are required for `VALID`.

    Returns a one-line reason on failure so the caller can print it in the
    script's single `INVALID: ...` form. Every field access goes through
    `.get`, so a document that is missing a block is reported as malformed
    rather than raising the `KeyError` this script's contract promises never
    to surface.
    """
    if not isinstance(doc, dict):
        return "the payload is not a JSON object"

    # Global Constraint 12, FIRST: a reader must refuse a `format_version`
    # whose major is newer than the one it understands, before any other rule
    # is evaluated against fields that future major may have redefined. This
    # is the rule the README states for every reader and that this script did
    # not implement at all — it never read `format_version`.
    version = doc.get("format_version")
    doc_major = _major(version)
    if doc_major is None:
        return f"format_version {version!r} is not a parseable semver"
    known_major = _major(FORMAT_VERSION)
    if doc_major > known_major:
        return (
            f"format_version {version} has a major version newer than this reader "
            f"understands (this script knows {FORMAT_VERSION})"
        )

    # THE BLOCK-PRESENCE CHECKS ARE NOT INVARIANT ARMS. They are this script's
    # stand-in for what the Rust reader gets from its own type: every one of
    # these blocks is a NON-optional field of `logweir_core::scorecard::
    # Scorecard`, so `serde_json` refuses a document missing one at
    # DESERIALISATION — `drill verify` exits 1 with "signature verified but the
    # payload is not a scorecard: missing field `sample`" and never reaches
    # `validate_invariants` at all. Python has no such layer, so the shape has
    # to be asserted here, FIRST, before any rule that would otherwise read a
    # block that is not there.
    #
    # Task 5c: `sample` joins the list. Until now it was deliberately absent —
    # Task 5's §10 and addendum A1 forbade adding it, on the ground that a loop
    # entry here would be "a Python arm with no Rust counterpart". Measured at
    # `0640240`, the cost of that omission was a REAL DISAGREEMENT and not a
    # cosmetic one: on a scorecard with the whole `sample` block deleted,
    # `drill verify` exited 1 and this script printed `VALID` and exited 0. The
    # ground was also mistaken about the category — no entry in this loop is an
    # arm of the arm-for-arm mirror below, and `evidence` had been sitting here
    # since Task 2 for exactly the same reason `sample` does now. The whole
    # loop is documented as shape, and `sample` is in it.
    #
    # `sample` is placed next to `objectives` because that is where it sits in
    # the struct's own field order (measured, objectives, sample) and in the
    # document's key order. The rest of the loop's order is Task 2's and is
    # left alone: it decides only which block a document missing SEVERAL of
    # them is named for, and no corpus case can pin that — neither reader
    # reaches an invariant on such a document.
    integrity = doc.get("integrity")
    objectives = doc.get("objectives")
    sample = doc.get("sample")
    measured = doc.get("measured")
    source = doc.get("source")
    engine = doc.get("engine")
    evidence = doc.get("evidence")
    for name, block in (
        ("integrity", integrity),
        ("objectives", objectives),
        ("sample", sample),
        ("measured", measured),
        ("source", source),
        ("engine", engine),
        ("evidence", evidence),
    ):
        if not isinstance(block, dict):
            return f"the document has no {name} block; it is not a drill scorecard"

    # Also shape, and also the Rust reader's type doing the work over there:
    # `sample.records_expected` is a `u64`, so `null`, a string or an absent
    # key is a deserialisation refusal in Rust. Here it was a SILENT SKIP — the
    # coverage arm below guarded with `isinstance(expected, int)` and simply
    # did not run, so `"records_expected": "75"` printed VALID from this script
    # and exited 1 from `drill verify`. Same wording convention as the
    # `last_phase_completed is not an integer` check further down, which is the
    # same situation on an `i32` field.
    #
    # `bool` is excluded explicitly: `isinstance(True, int)` is True in Python,
    # and `"records_expected": true` is not an integer to any other reader.
    expected = sample.get("records_expected")
    if not isinstance(expected, int) or isinstance(expected, bool):
        return "sample.records_expected is not an integer"

    # T0-2: the four post-put fields are zeroed BEFORE signing, because they
    # describe an upload that has not happened yet. Mirrors the arm that sits
    # immediately after `refuse_unreadable_major` in
    # `Scorecard::validate_invariants` — same position, same field order
    # (version_id, retain_until, immutable, create_only_enforced), same words —
    # so a document violating two fields is refused with the SAME message by
    # both readers. A retroactive tightening of the 1.0.0 reader, not a format
    # change: no byte of the format changes, the accepted set narrows, and the
    # writer's zeroing is unconditional, so no document Logweir has ever written
    # is refused. Scoped to major 1 so a future major may redefine the block.
    if doc_major == 1:
        if evidence.get("version_id") is not None:
            return "evidence.version_id is set but the four post-put fields are zeroed before signing"
        if evidence.get("retain_until") is not None:
            return "evidence.retain_until is set but the four post-put fields are zeroed before signing"
        if evidence.get("immutable"):
            return "evidence.immutable is true but the four post-put fields are zeroed before signing"
        if evidence.get("create_only_enforced"):
            return "evidence.create_only_enforced is true but the four post-put fields are zeroed before signing"

    # T0-6 / ruling R-A: `.strip()` on both sides. Python truthiness already
    # refused `""` while the Rust arm's `.is_none()` accepted AND SIGNED it —
    # a live disagreement in the file whose docstring above says a
    # disagreement is impossible. Whitespace-only went the other way: `"   "`
    # is truthy here, so both readers accepted it. The Rust arm is now
    # `partial_reason.as_deref().unwrap_or("").trim().is_empty()` — the same
    # predicate, the same message, the same position.
    if integrity.get("result") == "partial" and not str(
        integrity.get("partial_reason") or ""
    ).strip():
        return "integrity.result is 'partial' but partial_reason is null"

    # The format's only two float fields.
    for name, value in (
        ("objectives.pass_rate", objectives.get("pass_rate")),
        ("integrity.pass_rate_measured", integrity.get("pass_rate_measured")),
    ):
        if value is not None and not _finite(value):
            return f"{name} is not finite (NaN or +/-Inf)"

    # Global Constraint 18(a): captured_by_logweir is a BICONDITIONAL.
    last_phase = doc.get("last_phase_completed")
    if not isinstance(last_phase, int) or isinstance(last_phase, bool):
        return "last_phase_completed is not an integer"
    rel = measured.get("rpo_source_relative_seconds")
    reason = measured.get("rpo_source_relative_unmeasured_reason")
    if source.get("captured_by_logweir"):
        if last_phase < -1:
            return "source.captured_by_logweir is true but last_phase_completed is below -1"
        if rel is None:
            return "source.captured_by_logweir is true but rpo_source_relative_seconds is null"
        if reason is not None:
            return "source.captured_by_logweir is true but an unmeasured reason is present"
    else:
        if rel is not None:
            return "rpo_source_relative_seconds is set but the source was never contacted"
        if reason is None:
            return (
                "source.captured_by_logweir is false but "
                "rpo_source_relative_unmeasured_reason is null"
            )

    if (
        integrity.get("level") != "byte-fingerprint"
        and objectives.get("pass_rate") is not None
        and objectives.get("met") is True
    ):
        return "objectives.met must be null when pass_rate is not measurable"

    # Every seconds-valued gap in the document is non-negative. `-90` here is
    # the exact value the format doc says a reader "would most likely read as
    # no data loss"; this script printed it under a VALID banner.
    for name, value in (
        ("measured.rpo_seconds", measured.get("rpo_seconds")),
        ("measured.rpo_source_relative_seconds", rel),
        ("objectives.rpo_seconds", objectives.get("rpo_seconds")),
    ):
        if value is not None and value < 0:
            return (
                f"{name} is negative ({value}); a recovery-point gap of less than zero "
                "is not a smaller gap, it is a meaningless one"
            )

    sampled = integrity.get("records_sampled")
    matching = integrity.get("records_sampled_matching")
    if isinstance(sampled, int) and isinstance(matching, int) and matching > sampled:
        return "records_sampled_matching exceeds records_sampled"

    # T0-4: `outcome` is not a label, it is a claim entailed by the rest of the
    # document. Until this block existed, `outcome` was read by NEITHER reader,
    # so a document saying `pass` beside its own contradicting evidence printed
    # VALID here and exited 0 from `logweir drill verify`. ARM FOR ARM, IN
    # ORDER with the block that sits immediately after the
    # `records_sampled_matching` arm in `Scorecard::validate_invariants`
    # (crates/logweir-core/src/scorecard.rs) — same position, same order, same
    # words, so a document violating two of them gets the SAME message from
    # both readers.
    #
    # `sample` and `sample.records_expected` are shape-checked at the top of
    # this function (Task 5c), so the coverage arm below can read `expected`
    # straight out rather than guarding with `isinstance` and skipping in
    # silence, which is what let a mistyped `records_expected` print VALID.
    outcome = doc.get("outcome")

    if outcome == "pass" and integrity.get("result") != "pass":
        return "outcome is 'pass' but integrity.result is not 'pass'"

    # The converse of the `partial => partial_reason` arm above, on the same
    # trimmed-empty predicate (ruling R-A), so `""` and `"   "` count as absent
    # in both readers.
    if outcome == "pass" and str(integrity.get("partial_reason") or "").strip():
        return "outcome is 'pass' but integrity.partial_reason is present"

    # `met: null` is legitimate on a pass; only an explicit `false` contradicts
    # the outcome.
    if outcome == "pass" and objectives.get("met") is False:
        return "outcome is 'pass' but objectives.met is false"

    if (
        outcome == "pass"
        and isinstance(sampled, int)
        and isinstance(matching, int)
        and matching != sampled
    ):
        return f"outcome is 'pass' but only {matching} of {sampled} sampled records matched"

    if isinstance(sampled, int) and sampled > expected:
        return f"records_sampled ({sampled}) exceeds sample.records_expected ({expected})"

    # Ruling R-F: `logweir::drill::phase8_score::matrix_verdict_for` returns
    # `pass` on exactly one path — a `pass` outcome at byte-fingerprint level,
    # reachable only when the phase-5 readback was itself `pass`. A pass at a
    # reduced integrity level is `pass-degraded`.
    if engine.get("matrix_verdict") == "pass" and not (
        outcome == "pass" and integrity.get("level") == "byte-fingerprint"
    ):
        return (
            "engine.matrix_verdict is 'pass' but the drill did not pass at "
            "byte-fingerprint level"
        )

    if engine.get("matrix_verdict") == "fail" and engine.get("matrix_verdict_reason") is None:
        return "engine.matrix_verdict is 'fail' but matrix_verdict_reason is null"

    if (
        integrity.get("level") != "byte-fingerprint"
        and integrity.get("pass_rate_measured") is not None
    ):
        return "integrity.pass_rate_measured is set but the level is not byte-fingerprint"

    # Global Constraint 18: ELEVEN phase slots, -1 through 9.
    if not (-1 <= last_phase <= 9):
        return "last_phase_completed outside -1..=9"

    # T0-3, mirrored: see the `redactions` arm at the end of
    # `Scorecard::validate_invariants` (crates/logweir-core/src/scorecard.rs)
    # for the full argument. `docs/formats/drill-scorecard.md` states "Always
    # `[]` in v0.1" as a property of the format and nothing enforced it, so a
    # document announcing that a field the auditor reads first had been removed
    # still printed VALID from both readers.
    #
    # LAST, exactly as in the Rust reader, and mutant-tested for it: a document
    # violating this and an earlier arm must report the earlier arm's message
    # from BOTH readers.
    if doc.get("redactions"):
        return (
            f"redactions is non-empty but format_version {version} has no way to "
            "produce one; --redact is a v0.1.1 feature"
        )

    return ""


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

    # EVERY signature whose `keyid` names THIS key is tried — not
    # `signatures[0]`, which this script used to hard-index. A DSSE envelope
    # may carry one signature per signing key, so the entry for the key the
    # auditor was handed need not be first; reporting INVALID for such a
    # sidecar (which `logweir drill verify` accepts) is the disagreement this
    # script exists not to have. Selecting BY keyid, rather than trying them
    # all blindly, is the other half of that agreement: Logweir's own verifier
    # refuses a sidecar that carries no signature claiming to be by your key,
    # and says so in those words.
    want = key_id(public_key)
    mine = []
    for entry in signatures:
        if not isinstance(entry, dict):
            print("INVALID: sidecar signatures entry is not an object", file=sys.stderr)
            return 1
        if entry.get("keyid") != want:
            continue
        try:
            mine.append(base64.b64decode(entry.get("sig", ""), validate=True))
        except (binascii.Error, ValueError) as e:
            print(f"INVALID: sig is not valid base64: {e}", file=sys.stderr)
            return 1
    if not mine:
        print(
            f"INVALID: no signature by key {want} in the sidecar",
            file=sys.stderr,
        )
        return 1

    message = pae(payload_type, payload)
    if not any(verify_signature(public_key, message, s) for s in mine):
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
    # The bytes verified; they may still not be JSON at all. An unguarded
    # `json.loads` raised `json.JSONDecodeError` here — a raw traceback on a
    # correctly-signed payload, which this script's own contract (see the
    # module docstring's exit-1 paragraph) promises never to emit.
    try:
        doc = json.loads(payload)
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        print(
            f"INVALID: the signature verified but the payload is not valid JSON: {e}",
            file=sys.stderr,
        )
        return 1

    if payload_type_wanted == PAYLOAD_TYPES["scorecard"]:
        problem = check_invariants(doc)
        if problem:
            print(f"INVALID: {problem}", file=sys.stderr)
            return 1

        # T0-1. `approval.self_attested` is a CLAIM the document makes about
        # itself, and this script used to print it back verbatim under a VALID
        # banner — so a document could assert or deny its own provenance and
        # both verifiers would repeat the assertion. The finding is DERIVED
        # instead: `approval.key_id` against the key id of the signature that
        # actually verified.
        #
        # `want` is that key id. `verify_detached` in
        # `crates/logweir-evidence/src/verify.rs` returns the matched sidecar
        # entry's `keyid`, and it only ever considers entries whose `keyid`
        # equals the presented key's `key_id()` — so the two quantities are
        # equal by construction and this mirrors the Rust exactly. Both are the
        # lowercase-hex sha256 of the key's SPKI DER (`key_id` above).
        #
        # This arm is NOT part of `check_invariants` and must not be moved into
        # it: it needs the verifying key, and `check_invariants` mirrors
        # `Scorecard::validate_invariants`, a layer that has no key. It sits
        # here, after the invariant set and before the summary, exactly where
        # `crates/logweir/src/verify.rs` puts its own — so a self-contradicting
        # document still gets the more fundamental finding first.
        approval = doc.get("approval")
        if not isinstance(approval, dict) or not isinstance(approval.get("key_id"), str) \
                or not isinstance(approval.get("self_attested"), bool):
            # Logweir's Rust reader cannot even parse such a document into a
            # `Scorecard`; it reports "the payload is not a scorecard". Same
            # verdict here, rather than a KeyError or a silently skipped check.
            print(
                "INVALID: the signature verified but the document has no usable approval "
                "block (approval.key_id and approval.self_attested are required)",
                file=sys.stderr,
            )
            return 1
        derived_self_attested = approval["key_id"] == want
        if derived_self_attested != approval["self_attested"]:
            # Byte-identical to the Rust message after the `INVALID: ` prefix —
            # `docs/test_verify_scorecard.py::test_self_attested_parity` and
            # `scripts/check-verifier-parity.sh` compare the two lines directly.
            if approval["self_attested"]:
                detail = (
                    f"the document claims self_attested=true but the approval key id "
                    f"{approval['key_id']} does not match the verifying key id {want}"
                )
            else:
                detail = (
                    f"the document claims self_attested=false but the approval key id "
                    f"{approval['key_id']} matches the verifying key id {want}"
                )
            print(f"INVALID: APPROVAL CLAIM NOT VERIFIED: {detail}", file=sys.stderr)
            return 1

        # Every field below is reachable: `check_invariants` returned "", which
        # required each of these blocks to be present and well-formed.
        integrity = doc["integrity"]
        measured = doc["measured"]
        print(f"VALID  run_id={doc.get('run_id')}  outcome={doc.get('outcome')}")
        print(f"       rto_seconds={measured.get('rto_seconds')}  rpo_seconds={measured.get('rpo_seconds')}")
        print(f"       integrity={integrity.get('level')}/{integrity.get('result')}")
        if derived_self_attested:
            # Printed because it was DERIVED — never because the document said
            # so. The wording is unchanged; what changed is what stands behind
            # it.
            print("       approval: SELF-ATTESTED — the approval key equals the signing key")
        # The four `evidence` fields are zeroed BEFORE signing, because they
        # describe an upload that has not happened yet. Say so, so nobody reads
        # the zeroes as a finding about their bucket.
        print(
            "       evidence: the four post-put fields are zeroed before signing; "
            "the storage facts live in the receipt"
        )
        # Which checks actually produced this verdict. The sentence above is a
        # GUARANTEE, and until SCRIPT_VERSION 1.1.0 nothing enforced it — an
        # auditor reading an older run's output cannot tell the two apart
        # without this. 1.2.0 adds the derivation clause for the same reason:
        # the `approval:` line above is now a DERIVED finding, and a reader who
        # cannot tell a derived line from an echoed one is back where T0-1
        # started. 1.3.0 names the two arms it added, so the parenthetical
        # enumerates the whole invariant set rather than one member of it.
        # 1.4.0 adds outcome-entailment: six arms that make `outcome` — the
        # field an auditor reads first — a claim the rest of the document has
        # to support, where before it was read by neither reader (T0-4).
        # 1.5.0 adds required-block shape: `sample` is now required and
        # `sample.records_expected` must be an integer, which is what the Rust
        # reader has always got from its own types. Named here because an
        # auditor holding an older run's VALID cannot otherwise tell whether
        # the document they were handed even had a `sample` block.
        #
        # `docs/test_verify_scorecard.py::
        # test_the_version_line_names_the_current_invariant_set` asserts the
        # literals in this line — NOT an f-string over SCRIPT_VERSION, which
        # would stay green while the claim went stale (Task 4 addendum A1).
        print(
            f"       verifier: verify_scorecard.py {SCRIPT_VERSION} "
            "(invariant set: evidence-zeroing, trimmed-empty partial_reason, redactions, "
            "outcome-entailment, required-block shape incl. sample; "
            "approval.self_attested derived, not echoed)"
        )
        return 0

    if payload_type_wanted == PAYLOAD_TYPES["receipt"]:
        # The receipt's whole job is to bind to ONE scorecard. Print the
        # binding, and say plainly what a `false` does and does not mean.
        if not isinstance(doc, dict) or "scorecard_sha256" not in doc:
            print(
                "INVALID: the signature verified under the receipt payload type but the "
                "document is not a put receipt (no scorecard_sha256)",
                file=sys.stderr,
            )
            return 1
        print(f"VALID  receipt for {doc.get('scorecard_key')}")
        print(f"       binds to scorecard {doc.get('scorecard_sha256')}")
        print(
            f"       create_only_enforced={doc.get('create_only_enforced')}  "
            f"immutable={doc.get('immutable')}  version_id={doc.get('version_id')}"
        )
        print(f"       observed_at={doc.get('observed_at')}")
        if not doc.get("create_only_enforced"):
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
