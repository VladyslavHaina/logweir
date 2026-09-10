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

A Logweir drill publishes THREE signed documents and a Logweir BACKUP publishes
a fourth, each under its own DSSE payload type. `--payload-type` selects which
one is being checked; the default is the scorecard, so the
three-positional-argument form above is unchanged.

  scorecard       application/vnd.logweir.drill-scorecard+json;version=1.0.0   (default)
  backup-receipt  application/vnd.logweir.backup-receipt+json;version=1.0.0
  receipt         application/vnd.logweir.drill-put-receipt+json;version=1.0.0
  teardown        application/vnd.logweir.drill-teardown+json;version=1.0.0

  python3 verify_scorecard.py --payload-type receipt \
      <run_id>.receipt.json <run_id>.receipt.sig public.pem

  python3 verify_scorecard.py --payload-type backup-receipt \
      <run_id>.receipt.json <run_id>.receipt.sig public.pem

`backup-receipt` is the signed record of one `logweir backup run` — a DIFFERENT
document from `receipt`, which is the drill's post-put storage readback of a
scorecard. Two of the four are checked ARM FOR ARM against their Rust reader
(`scorecard` and `backup-receipt`); for the other two this script verifies the
signature and says so rather than inventing semantics for a document it does
not model.

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
# `re` is stdlib, like every other import here: this script's ONLY third-party
# dependency is `cryptography` (see the module docstring), and Task 5b's
# `_receipt_parse_semver` needs a pattern rather than Python's `int()`, whose
# accepted set is wider than Rust's `str::parse::<u64>` in four different ways.
import re
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
#   1.6.0  SHAPE again, and again a bump for documents this script now decides
#          differently (Task 5d, from Task 5c's review). Three changes, no arm
#          weakened and no check removed. (a) THE u64 DOMAIN: every field the
#          Rust reader types as `u64` must satisfy `0 <= v < 2**64`, where
#          `isinstance(v, int)` mirrored serde's TYPE and left Python's
#          unbounded `int` unbounded — `sample.records_expected:
#          18446744073709551616` printed VALID here and exited 1 from `drill
#          verify`, and `-1` was refused by both but on an INVARIANT here and at
#          DESERIALISATION there. (b) `REQUIRED_BLOCKS` IS NOW THE WHOLE STRUCT:
#          `target`, `approval`, `target_diff` and `topic_parity` join the
#          block-presence loop, closing three more live instances of the same
#          disagreement `sample` was in 1.5.0 — `target`, `target_diff` or
#          `topic_parity` deleted: `drill verify` exited 1 and this script
#          printed VALID. `approval` absent was already refused by both, here by
#          `main`'s approval derivation; the loop now names it too. (c) THE ORDER
#          IS PINNED to serde's declaration order, so a document missing several
#          blocks is named for the SAME block by both readers; `measured` +
#          `integrity` missing together used to be named `measured` by Rust and
#          `integrity` here.
#   1.7.0  SHAPE a third time (Task 5e, from Task 5d's review findings F1, F3
#          and F4), and a bump for the same reason as 1.5.0 and 1.6.0: there are
#          documents this script now decides differently. No arm weakened, no
#          check removed. (a) THE REQUIRED NON-BLOCK FIELDS. `Scorecard` has six
#          required fields whose type is not one of the blocks —
#          `format_version`, `run_id`, `outcome`, `last_phase_completed`,
#          `requested_at` and `phases` — and the block-presence loop covered
#          none of them, because a block is a JSON object and these are not.
#          With `run_id`, `requested_at` or `phases` absent, `drill verify`
#          exited 1 (`missing field ...`) and this script printed `VALID`; with
#          `outcome` absent it refused, but on an INVARIANT about
#          `engine.matrix_verdict`, which tells an auditor something that is not
#          what is wrong with the document. `REQUIRED_FIELDS` is the whole list,
#          in the struct's declaration order. (b) `null` IS NO LONGER SKIPPED ON
#          A NON-`Option` u64. `if value is None: continue` treated all eleven
#          `u64` fields as nullable; only five are `Option<u64>` in Rust. On
#          `sample.records_restored: null`, `integrity.mismatches: null` and
#          `phases[].duration_ms: null`, `drill verify` exited 1 (`invalid type:
#          null, expected u64`) and this script printed `VALID`. Every
#          `U64_FIELDS` entry now carries the Rust optionality and the five
#          `Option<u64>` fields still accept null. (c) THE u64 LIST IS IN THE
#          STRUCT'S DECLARATION ORDER and has closed arithmetic OUTSIDE this
#          file, which 1.6.0's did not — argued at `U64_FIELDS`.
#   1.8.0  SHAPE a fourth time (Task 5f, from Task 5e's review findings F1, F2
#          and F3 and the wrong-type residual 1.7.0 recorded rather than
#          closed), and a bump for the same reason as 1.5.0, 1.6.0 and 1.7.0:
#          there are documents this script now decides differently. No arm
#          weakened, no check removed. (a) THE REQUIRED NON-BLOCK FIELDS ARE
#          TYPE-CHECKED, not merely counted. 1.7.0 asserted presence and said so
#          in as many words; the cost was measured by that review at `78bf570`
#          and re-measured at `b99239a`, on documents derived from
#          `e2e/fixtures/invariants/unmodified_example.json`:
#          `run_id: 42`, `phases: "x"` and `requested_at: 5` were each `drill
#          verify` exit 1 (`invalid type: integer 42, expected a string`, and so
#          on) against `VALID` here, and `outcome: 7` refused here on an
#          INVARIANT about `engine.matrix_verdict` rather than on the type.
#          `REQUIRED_FIELDS` now carries the JSON type each field's Rust type
#          implies, and the same arithmetic that keeps the NAMES derived from
#          the struct keeps the TYPES derived too. (b) THE u64 BLOCK MAP IS
#          DERIVED, not hand-written (`_u64_fields`): the four-entry literal
#          could `KeyError` on the first document the moment a `u64` was added
#          to a struct outside it, in a function whose stated contract is that
#          no field access surfaces as a traceback. (c) THE NULL CASES ARE
#          DERIVED FROM THE NON-`Option` u64 SET. Nothing changes in this file
#          for (c) — the guard line is 1.7.0's — but the shape corpus now owes
#          one `null:` case per non-`Option` `u64` field, so reverting the guard
#          and deleting the cases in one edit fails at assertion rather than
#          silently.
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
# 1.9.0 (Task 5b) adds THREE things in one commit, which is why one bump
# covers them: (a) this reader learns the BACKUP RECEIPT — a fourth payload
# type and a mirror of `logweir_core::backup_receipt::BackupReceipt`'s four
# invariant arms, so `logweir backup run`'s signed document is checkable by an
# auditor with this script and no Rust toolchain; (b) the scorecard gains two
# `target.auth` arms, Global Constraint 12's price for one nested optional
# field (`target.auth` is declared by Task 5b and FILLED by Task 6); (c) the
# payload-type map gains its fourth entry, which
# `test_script_version_was_bumped_with_the_payload_type_map` ties to this
# constant so the two can never move apart.
# 1.10.0 (Task 5b fix round 1) CLOSES THE AUTH MODE'S VALUE SET in both
# documents, which is one arm in each: `target.auth.mode` and
# `source.auth.mode` are `plaintext` or `scramSha512` and nothing else. Until
# this version neither field's VALUE was read by either reader — a document
# carrying `{"mode": "totally-made-up"}` verified 0/0, proved by execution in
# Task 5b's review — while `Backup.status.auth.mode`'s CRD description already
# promised the two values and Task 17 copies the receipt's field into it. The
# minor moves because the invariant SET grew, not because either format did:
# `FORMAT_VERSION` stays `1.0.0` for both documents (only a `description`
# changed in the receipt's schema), which is exactly the distinction
# `test_the_script_version_is_not_the_format_version` exists to keep.
SCRIPT_VERSION = "1.10.0"

# The FOUR payload types Logweir signs. Keep byte-for-byte in step with
# `crates/logweir-verify/src/lib.rs`'s PAYLOAD_TYPE_SCORECARD,
# PAYLOAD_TYPE_BACKUP_RECEIPT, PAYLOAD_TYPE_PUT_RECEIPT and
# PAYLOAD_TYPE_TEARDOWN; `docs/test_verify_scorecard.py::
# test_the_four_payload_types_match_the_rust_constants` fails if they ever
# drift.
#
# THE PATH FOLLOWED THE CONSTANTS. Task 14 moved them into the verify-only
# crate `logweir-verify`, which is the crate the control plane links;
# `logweir-evidence` re-exports them with `pub use logweir_verify::*;` and a
# re-export contains none of the four literals, so a test reading the old file
# would assert a property of a `pub use` line.
#
# `backup-receipt` is NOT `receipt`. `receipt` is the drill's post-put storage
# readback of a SCORECARD; `backup-receipt` is the signed record of one
# `logweir backup run`. Two documents, two media types, and the short names
# are the ones `logweir drill verify --payload-type` takes.
PAYLOAD_TYPES = {
    "scorecard": PAYLOAD_TYPE,
    "backup-receipt": "application/vnd.logweir.backup-receipt+json;version=1.0.0",
    "receipt": "application/vnd.logweir.drill-put-receipt+json;version=1.0.0",
    "teardown": "application/vnd.logweir.drill-teardown+json;version=1.0.0",
}


def resolve_payload_type(name: str) -> str:
    """Short name -> media type, or a full media type passed straight through.

    An unknown value is an ERROR rather than a silent passthrough of anything
    that happens to contain a slash: a typo'd media type would otherwise turn
    into "unexpected payloadType" and read like a bad artifact rather than a
    bad command line.

    THIS CONTRACT IS SHARED WITH `crates/logweir/src/verify.rs::
    resolve_payload_type`, byte for byte, including the `{name!r}` quoting and
    the "or a full media type" clause — `crates/logweir/tests/
    two_reader_parity_receipt.rs::the_two_payload_type_resolvers_agree` drives
    both readers over the same values and compares the messages. The Rust half
    reproduces CPython's `repr` rule for a `str` rather than using Rust's own
    `{:?}` (which quotes with `"`), because this script is the one an auditor
    is told to read and its message is the one the interface register quotes.
    """
    if name in PAYLOAD_TYPES:
        return PAYLOAD_TYPES[name]
    if name in PAYLOAD_TYPES.values():
        return name
    raise ValueError(
        f"unknown --payload-type {name!r}; use one of "
        + ", ".join(sorted(PAYLOAD_TYPES)) + " or a full media type"
    )


def _rust_debug_str(value) -> str:
    r"""A string as Rust's `{:?}` renders it: double quotes, `\` and `"` escaped.

    THE MIRRORED ARMS' REFUSAL TEXT IS THE INTERFACE, and Python's own `!r`
    renders `'x'` where Rust renders `"x"`. Two arms that differ by one
    character are two arms that disagree, and `scripts/check-verifier-parity.sh`
    compares the FULL refusal text since Task 5b, so this is not cosmetic.
    Rust's convention wins inside the invariant messages because
    `crates/logweir-core/tests/backup_receipt.rs` asserts them in full and
    those assertions are what a reviewer applies a mutant against; Python's
    convention wins in `resolve_payload_type`'s usage error, which is a
    command-line message and not a document's refusal. Each is stated where it
    is used, and both are pinned by tests.

    Rust escapes only `\` and the quote for a printable ASCII string, which is
    every value these arms interpolate (a `format_version` or a manifest key).
    """
    return '"' + str(value).replace("\\", "\\\\").replace('"', '\\"') + '"'


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


# EVERY REQUIRED BLOCK OF `logweir_core::scorecard::Scorecard`, IN SERDE'S
# STRUCT DECLARATION ORDER. Both halves of that sentence are load-bearing and
# both are machine-checked by `crates/logweir/tests/two_reader_parity.rs::
# every_required_block_has_a_shape_corpus_case`, which reads THIS tuple and the
# struct and fails if they drift.
#
# WHICH BLOCKS. A "block" is a field of `Scorecard` whose type is one of the
# structs declared beside it in `crates/logweir-core/src/scorecard.rs` — so
# `format_version` (a String), `last_phase_completed` (an i8), `phases` (a Vec)
# and the three `Option` fields are not blocks, and the eleven below are. None
# carries `#[serde(default)]`, so `serde_json` refuses a document missing any
# one of them at DESERIALISATION and `drill verify` never reaches
# `validate_invariants`.
#
# Task 5c put `sample` in this list and closed a measured disagreement by doing
# it. Task 5d completed the list: `target`, `approval`, `target_diff` and
# `topic_parity` were still absent, and their absence was the SAME live
# two-reader disagreement, measured at `6619090` on documents derived from
# `e2e/fixtures/invariants/unmodified_example.json` — with any one of the four
# deleted, `drill verify` exited 1 (`missing field ...`) and this script printed
# `VALID` and exited 0. Nothing here reads those four blocks, which is exactly
# why they were missed: an arm-for-arm mirror only ever grows the blocks its
# arms happen to need. The list is now taken from the struct instead, and the
# walker named above is what keeps it there — which is also what makes deleting
# an entry from this tuple, together with its corpus case and its pytest, fail
# at assertion time instead of silently. (Task 5c's review, finding F2: the
# shape corpus had no closed arithmetic and five of seven checks were protected
# by nothing at all.)
#
# WHY THE ORDER. serde reports the FIRST missing field in declaration order, so
# a document missing several blocks is named for its first one. Task 5c judged
# that unpinnable — "no corpus case can pin that" — and its review measured the
# opposite: `measured` + `integrity` missing together was named `measured` by
# `drill verify` and `integrity` by this script, and such a document is exactly
# what `shape-index.json` exists to record, since neither reader reaches an
# invariant on it. In this order the two readers name the same block on every
# multi-missing document; `no_measured_and_no_integrity_blocks` is the case that
# proves it.
REQUIRED_BLOCKS = (
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
)

# EVERY REQUIRED FIELD OF `logweir_core::scorecard::Scorecard` THAT IS NOT A
# BLOCK, IN SERDE'S STRUCT DECLARATION ORDER. Machine-checked against the struct
# by `crates/logweir/tests/two_reader_parity.rs::
# every_required_non_block_field_has_a_shape_corpus_case` and by
# `scripts/check-invariant-corpus.sh`, both of which read the struct rather than
# this tuple — the same fixed point, and for the same reason, as the block list
# above.
#
# WHICH FIELDS. Every field of `Scorecard` carrying no `#[serde(default)]` whose
# type is NOT one of the structs declared beside it: a `String`, an enum from
# another module, an `i8`, a `DateTime<Utc>` and a `Vec<PhaseRecord>`. None
# deserialises from a missing key, so `serde_json` refuses a document missing any
# one of them at DESERIALISATION exactly as it does for a block, and `drill
# verify` exits 1 with `missing field ...` without reaching
# `validate_invariants`.
#
# Task 5d's review, finding F3, measured what their absence cost, on documents
# derived from `e2e/fixtures/invariants/unmodified_example.json` and signed:
# with `run_id`, `requested_at` or `phases` deleted, `drill verify` exited 1 and
# this script printed `VALID` and exited 0 — the same live two-reader
# disagreement the block loop closed, three more times. `phases` is the one that
# also defeated 1.6.0's order claim: it sits BETWEEN `approval` and `measured` in
# the struct, so serde reaches it before `measured` and this script did not reach
# it at all.
#
# PRESENCE **AND TYPE** SINCE 1.8.0, and the second element is the type (Task
# 5f, from Task 5e's review). 1.7.0 asserted presence only and said so plainly:
# "these six carry five different Rust types and share no JSON shape, so the
# honest common claim is that the key is there". That was true of a list of bare
# names and false of the format — a Rust type implies a JSON type, and the
# implication is a rule rather than a guess:
#
#     String           -> "string"   `run_id`, `format_version`
#     DateTime<Utc>    -> "string"   `requested_at`, RFC 3339 on the wire
#     Outcome          -> "string"   a unit enum with `#[serde(rename_all = ...)]`
#     i8               -> "integer"  `last_phase_completed`
#     Vec<PhaseRecord> -> "array"    `phases`
#
# The names are JSON Schema's own ("string", "integer", "array", "boolean"), not
# Python's. "list" in particular is NOT used: `scripts/check-no-oso.sh` greps
# every .rs file under `crates/` for the quoted token "list", which is a
# kafka-backup subcommand under Global Constraint 3, and the mapping lives in one
# of those files.
#
# The mapping lives in `crates/logweir/tests/two_reader_parity.rs::
# json_type_of` and in `scripts/check-invariant-corpus.sh` — both OUTSIDE
# `docs/`, both keyed on the struct's own type text, so a new required non-block
# field of an unmapped Rust type fails loudly instead of arriving here with a
# guessed type or no type at all.
#
# What the presence-only version cost — measured by Task 5e's review at
# `78bf570` and re-measured here at `b99239a`, on documents derived from
# `e2e/fixtures/invariants/unmodified_example.json` and signed, over the release
# binary: `run_id: 42` was `drill verify`
# exit 1 (`invalid type: integer 42, expected a string`) against `VALID` here;
# `phases: "x"` was exit 1 (`invalid type: string "x", expected a sequence`)
# against `VALID`; `requested_at: 5` was exit 1 against `VALID`; and `outcome: 7`
# refused here on an INVARIANT about `engine.matrix_verdict`, which is the same
# verdict for a reason that is not what is wrong with the document.
#
# `bool` is excluded from `"integer"` explicitly, exactly as the u64 loop does it:
# `isinstance(True, int)` is True in Python and `"last_phase_completed": true`
# is not an integer to any other reader.
#
# `format_version` is in the list and BOTH its checks here are UNREACHABLE: the
# Global Constraint 12 rule at the top of `check_invariants` runs first and
# refuses an absent one as `format_version None is not a parseable semver`, and a
# non-string one as `format_version 1 is not a parseable semver`. Both stay,
# because the list is DERIVED from the struct and an exception is a hole in the
# derivation; the corpus cases `no_format_version_field` and
# `format_version_not_a_string` record the refusals that actually happen.
#
# WHERE THE LOOP RUNS: AFTER the block loop, never before. serde names the FIRST
# missing field in declaration order over blocks and non-blocks alike, and
# `phases` sits between `approval` and `measured`; running this loop first would
# make a document missing `phases` AND `engine` named `phases` here and `engine`
# there — turning a pair the two readers agree on today into one they do not.
# Running it after preserves every answer this script already gives on a
# multi-missing document and adds the six single-field ones.
#
# PRESENCE AND TYPE ARE CHECKED PER FIELD, IN ONE PASS, not in two passes over
# the whole list. serde aborts at the FIRST fault it meets while visiting the
# document, so on a document whose first fault is a wrong type and whose second
# is an absent key — `format_version: 1` with `run_id` deleted — Rust names the
# type. A presence-only sweep followed by a type sweep would name `run_id` here.
# One pass in declaration order is the arrangement that agrees.
REQUIRED_FIELDS = (
    ("format_version", "string"),
    ("run_id", "string"),
    ("outcome", "string"),
    ("last_phase_completed", "integer"),
    ("requested_at", "string"),
    ("phases", "array"),
)

# The JSON type names `REQUIRED_FIELDS` uses, and the words the refusal uses for
# each. The names are JSON Schema's; the words are this file's own convention,
# already set by `last_phase_completed is not an integer` and
# `sample.records_expected is not an integer` — which is why the refusal reads
# "is not an integer" rather than naming a Python type.
_JSON_TYPES = {"string": str, "integer": int, "array": list, "boolean": bool}
_JSON_TYPE_WORDS = {
    "string": "a string",
    "integer": "an integer",
    "array": "an array",
    "boolean": "a boolean",
}

# EVERY FIELD `logweir_core::scorecard::Scorecard` AND ITS BLOCKS TYPE AS `u64`,
# as (dotted name, the field is `Option<u64>` in Rust) pairs, IN SERDE'S
# DECLARATION ORDER — the list `check_invariants`'s domain check walks.
#
# It is a list and not a single field on purpose. Task 5c's review found the
# domain gap on `sample.records_expected`; fixing that one field alone would
# leave ten others whose Python side accepts `2**64` and whose Rust side refuses
# it, which is the same defect with a different name. Grep the Rust for `u64` in
# `crates/logweir-core/src/scorecard.rs` and this list is what comes back, minus
# `major_version`'s return type, which is not a document field.
#
# CLOSED ARITHMETIC, AND IT LIVES OUTSIDE `docs/` (Task 5e, from Task 5d's
# review finding F1). 1.6.0 derived this list from the struct in ONE place — a
# `def test_*` in `docs/test_verify_scorecard.py` — which is the same file an
# attacker deletes from. Measured at `e807376`: deleting `integrity.mismatches`
# from this tuple TOGETHER WITH its pytest case and
# `test_the_u64_field_list_matches_the_rust_struct` left pytest at 0, the parity
# walker at 0 and `scripts/check-invariant-corpus.sh` at 0 — and
# `integrity.mismatches: 2**64` was `drill verify` exit 1 against `VALID` here
# again. `crates/logweir/tests/two_reader_parity.rs::
# every_u64_field_has_the_same_domain_check_in_both_readers` and the arithmetic
# block in `scripts/check-invariant-corpus.sh` now re-derive this whole list —
# names, order AND optionality — from `crates/logweir-core/src/scorecard.rs`,
# which that deletion does not touch. Both print the two lists on a mismatch.
#
# THE SECOND ELEMENT IS THE RUST OPTIONALITY, and it decides what `null` means.
# `Option<u64>` accepts null; a plain `u64` does not, and `serde_json` says
# `invalid type: null, expected u64`. 1.6.0 skipped `None` for all eleven, so
# `sample.records_restored: null` printed `VALID` here and exited 1 there (Task
# 5d's review, finding F4). Five of the eleven are `Option<u64>` and are marked
# `True`; the six plain `u64` fields are marked `False` and refuse null.
#
# `phases[]` is the one entry whose block is not a block: `phases` is a
# `Vec<PhaseRecord>`, so the entry expands to one triple per record with the
# record's index substituted into the name. It is FIRST because `phases` is
# declared before `measured` in `Scorecard` and serde reports fields in
# declaration order.
U64_FIELDS = (
    ("phases[].duration_ms", False),
    ("measured.rto_seconds", True),
    ("measured.rto_requested_to_verified_seconds", True),
    ("measured.rto_restore_only_seconds", True),
    ("measured.rto_excluding_preflight_seconds", True),
    ("objectives.rto_seconds", True),
    ("sample.records_expected", False),
    ("sample.records_restored", False),
    ("integrity.records_sampled", False),
    ("integrity.records_sampled_matching", False),
    ("integrity.mismatches", False),
)


def _u64_fields(doc):
    """(dotted name, value, the field is `Option<u64>` in Rust) for every `u64`
    field of the document.

    `phases` is a `Vec<PhaseRecord>` rather than a block, so it is NOT in the
    block-presence loop and cannot be assumed well-formed here: a non-list, or
    an element that is not a dict, is skipped rather than raising. Rust refuses
    such a document at deserialisation; this function's contract is that no
    field access ever surfaces as a traceback. (`phases` being ABSENT is a
    different claim and is refused by the `REQUIRED_FIELDS` loop.)

    THE OWNER MAP IS DERIVED FROM `REQUIRED_BLOCKS`, not written out (Task 5f,
    from Task 5e's review finding F2). It used to be a four-entry literal —
    `measured`, `objectives`, `sample`, `integrity` — passed in as four
    positional arguments, which was true of the `U64_FIELDS` of the day and is
    not a property of the format. Task 5e made the walker and the shell gate
    DERIVE `U64_FIELDS` from `Scorecard`, so the moment a `u64` is added to, say,
    `TargetInfo`, both gates REQUIRE an entry named `target.<field>` — and
    `blocks["target"]` would then raise `KeyError` on every document, in the one
    function whose docstring promises that no field access ever surfaces as a
    traceback. Reading `REQUIRED_BLOCKS` instead makes the map exactly as wide as
    the block list the same file already keeps derived. Safe without a guard:
    the caller runs the block-presence loop first, so every name here is already
    proved to be a dict.
    """
    blocks = {name: doc[name] for name in REQUIRED_BLOCKS}
    phases = doc.get("phases")
    for path, optional in U64_FIELDS:
        block, key = path.split(".", 1)
        if block == "phases[]":
            if isinstance(phases, list):
                for n, phase in enumerate(phases):
                    if isinstance(phase, dict):
                        yield f"phases[{n}].{key}", phase.get(key), optional
            continue
        # `.get` with the document itself as the fallback, never `[...]`. Two
        # owners are possible and neither may raise: a REQUIRED block, which the
        # map above carries and the block loop has already proved is a dict; and
        # an OPTIONAL struct field such as `engine_subreport`, which
        # `rust_u64_fields` also expands and which a document may legitimately
        # omit or null. The first is read from the map, the second from the
        # document, and anything that is not a dict is skipped rather than
        # traced back — the same contract `phases` is held to above.
        owner = blocks.get(block, doc.get(block))
        if not isinstance(owner, dict):
            continue
        yield path, owner.get(key), optional


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
    # `REQUIRED_BLOCKS` is the whole list and its order is load-bearing; both
    # are argued at the constant's own definition above.
    for name in REQUIRED_BLOCKS:
        if not isinstance(doc.get(name), dict):
            return f"the document has no {name} block; it is not a drill scorecard"

    # THE REQUIRED NON-BLOCK FIELDS — the same layer, the same claim, for the six
    # required fields of `Scorecard` whose type is not a block and which the loop
    # above therefore structurally cannot hold (Task 5e, from Task 5d's review
    # finding F3: with `run_id`, `requested_at` or `phases` absent, `drill
    # verify` exited 1 and this script printed `VALID`).
    #
    # AFTER the block loop and never before it — the reason is argued in full at
    # `REQUIRED_FIELDS`.
    #
    # PRESENCE AND TYPE since 1.8.0 (Task 5f, from Task 5e's review). The type
    # each field must carry is the second element of its `REQUIRED_FIELDS` entry
    # and is derived from the field's Rust type by the two gates outside `docs/`;
    # measured at `78bf570`, `run_id: 42`, `phases: "x"` and `requested_at: 5`
    # were `drill verify` exit 1 against `VALID` here. `bool` is excluded from
    # `"integer"` for the same reason the u64 loop excludes it.
    for name, want in REQUIRED_FIELDS:
        if name not in doc:
            return f"the document has no {name} field; it is not a drill scorecard"
        value = doc[name]
        if not isinstance(value, _JSON_TYPES[want]) or (
            want == "integer" and isinstance(value, bool)
        ):
            return f"{name} is not {_JSON_TYPE_WORDS[want]}"

    # Bound only after the loop above has proved each one is a dict, so the
    # arms below can read through them without a second guard.
    engine = doc["engine"]
    source = doc["source"]
    target = doc["target"]
    measured = doc["measured"]
    objectives = doc["objectives"]
    sample = doc["sample"]
    integrity = doc["integrity"]
    evidence = doc["evidence"]

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

    # THE u64 DOMAIN, not merely the JSON type (Task 5d, from Task 5c's review
    # finding F1). `isinstance(v, int)` mirrors serde's TYPE and not `u64`'s
    # DOMAIN, and the gap was a live two-reader disagreement of exactly the
    # class the check above exists to close: on `sample.records_expected:
    # 18446744073709551616` (`u64::MAX + 1`), `drill verify` exited 1 —
    # `invalid type: floating point 1.8446744073709552e+19, expected u64` —
    # while this script printed `VALID` and exited 0. `-1` diverged the other
    # way: Rust refused at DESERIALISATION (`invalid value: integer -1,
    # expected u64`) and this script reached an INVARIANT and reported
    # `records_sampled (75) exceeds sample.records_expected (-1)` — the same
    # verdict by a route that says something else entirely. Both are measured
    # at `6619090`, not argued.
    #
    # Python's `int` is unbounded, so every field the Rust reader types as
    # `u64` needs the bound stated here or it has no bound at all. ALL ELEVEN
    # are listed — not just the one the review found — because a domain check
    # on one field of a type is a reminder, not a rule. The list is the `u64`
    # fields of `logweir_core::scorecard::Scorecard` and its blocks:
    # `sample.records_expected`, `sample.records_restored`,
    # `integrity.records_sampled`, `integrity.records_sampled_matching`,
    # `integrity.mismatches`, the four `measured.rto_*_seconds`,
    # `objectives.rto_seconds` and `phases[].duration_ms`. The `rpo` fields are
    # `i64` and are NOT here; their own arm below refuses a negative gap.
    #
    # The refusal wording is this repository's own, so `shape-index.json`
    # records it WHOLE, and Rust's half — serde's text, which cannot be matched
    # byte-for-byte from Python — is recorded there as a prefix, exactly as the
    # missing-`sample` case does it.
    #
    # `null` IS SKIPPED ONLY WHERE RUST HAS AN `Option<u64>` (Task 5e, from Task
    # 5d's review finding F4). 1.6.0 skipped it for all eleven; six of them are
    # plain `u64`, where `serde_json` says `invalid type: null, expected u64`. On
    # `sample.records_restored: null`, `integrity.mismatches: null` and
    # `phases[0].duration_ms: null`, `drill verify` exited 1 and this script
    # printed `VALID` — measured at `e807376`, not argued. An ABSENT key arrives
    # here as `None` as well and is refused by the same line, which is the same
    # claim by a different route: Rust says `missing field ...`, and either way
    # the document does not carry the integer it promises.
    for name, value, optional in _u64_fields(doc):
        if optional and value is None:
            continue
        if not isinstance(value, int) or isinstance(value, bool):
            return f"{name} is not an integer"
        if not 0 <= value < 2**64:
            return f"{name} is outside the u64 domain (0 <= v < 2**64): {value}"

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

    # `target.auth` (Task 5b), mirrored ARM FOR ARM and IN THIS POSITION from
    # `Scorecard::validate_invariants` — the two arms sit between the
    # `last_phase_completed` arm and the `redactions` arm in both readers, so a
    # document violating two of them gets the same message from each.
    #
    # ABSENT IS LEGAL and means plaintext: every scorecard this tree has ever
    # written has no `auth` block, and `crates/logweir/tests/
    # two_reader_parity.rs`'s twenty-one existing cases are what keep both
    # readers tolerant of that.
    #
    # BLANK IS NOT PLAINTEXT (ruling R-A — `.strip()` here, `trim().is_empty()`
    # in Rust). A blank mode read as "plaintext" would record a SCRAM run as an
    # unauthenticated one by omission, and a `username` beside a blank mode is a
    # principal recorded without the mechanism it authenticated with.
    #
    # A NON-DICT `auth` is not this layer's business: `TargetInfo.auth` is
    # `Option<AuthSummary>`, so Rust refuses `"auth": "plaintext"` at
    # DESERIALISATION with its own serde message — the class `shape-index.json`
    # records, where the two readers' texts are recorded separately because
    # neither reaches an invariant. `"auth": null` is absent in both.
    target_auth = target.get("auth")
    if isinstance(target_auth, dict):
        mode_blank = not str(target_auth.get("mode") or "").strip()
        username_named = bool(str(target_auth.get("username") or "").strip())
        if mode_blank and username_named:
            return (
                "target.auth names a username with no auth mode; a username without its "
                "mechanism is not a record of how the client authenticated"
            )
        if mode_blank:
            return (
                "target.auth.mode is blank; an absent auth block is how a scorecard says "
                "plaintext"
            )
        # THE VALUE SET IS CLOSED (SCRIPT_VERSION 1.10.0): exactly
        # `AuthSpec`'s two serde tags, which are the `KafkaCluster` CRD's
        # `auth.mode` enum and the only two strings `AuthSpec::mode_str()` can
        # return. The EXACT value, not a stripped one — the blank arm above
        # has already refused a whitespace-only mode, and the receipt's arm 5
        # does not strip either, so one spelling means one comparison.
        if str(target_auth.get("mode")) not in ("plaintext", "scramSha512"):
            return (
                "target.auth.mode is not one of the two values this format defines; it "
                "is \"plaintext\" or \"scramSha512\" and nothing else"
            )

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


# EVERY REQUIRED BLOCK AND FIELD of `logweir_core::backup_receipt::
# BackupReceipt`, in the struct's own declaration order.
#
# The receipt's counterpart of `REQUIRED_BLOCKS` / `REQUIRED_FIELDS`, and it
# exists for the same reason: over there `serde_json` refuses a document
# missing any of them at DESERIALISATION, and `logweir drill verify` reports
# "signature verified but the payload is not a backup receipt: missing field
# `covered`" without ever reaching an invariant. Python has no such layer, so
# the shape is asserted here — FIRST, before any arm that would otherwise read
# a block that is not there and raise the traceback this script's contract
# promises never to emit.
#
# THESE ARE NOT INVARIANT ARMS, and they are deliberately in their own
# function: `check_backup_receipt_invariants` below is the arm-for-arm mirror
# of `BackupReceipt::validate_invariants`, and `scripts/
# check-invariant-corpus.sh` derives the arm list from that function's body and
# from the Rust one. A shape message living in the same function would be
# counted as a fifth arm the Rust reader does not have.
RECEIPT_BLOCKS = ("source", "engine", "archive", "records", "covered")
RECEIPT_FIELDS = (
    ("format_version", "string"),
    ("run_id", "string"),
    ("backup_id", "string"),
    ("requested_at", "string"),
    ("started_at", "string"),
    ("finished_at", "string"),
    ("exit_code", "integer"),
    ("triggered_by", "string"),
)


def _receipt_shape(doc) -> str:
    """"" when `doc` has the shape a `BackupReceipt` deserialises from."""
    if not isinstance(doc, dict):
        return "the payload is not a JSON object"
    for name in RECEIPT_BLOCKS:
        if not isinstance(doc.get(name), dict):
            return f"the document has no {name} block; it is not a backup receipt"
    for name, want in RECEIPT_FIELDS:
        if name not in doc:
            return f"the document has no {name} field; it is not a backup receipt"
        value = doc[name]
        if not isinstance(value, _JSON_TYPES[want]) or (
            want == "integer" and isinstance(value, bool)
        ):
            return f"{name} is not {_JSON_TYPE_WORDS[want]}"
    for name in ("cluster_id", "topics"):
        if name not in doc["source"]:
            return f"the document has no source.{name} field; it is not a backup receipt"
    if not isinstance(doc["source"].get("topics"), list):
        return "source.topics is not an array"
    # `ReceiptSource.auth` is a required `ReceiptAuth` and `mode` a required
    # `String`, so Rust refuses both of these at DESERIALISATION, before any
    # arm runs. They belong in the SHAPE layer here for the same reason every
    # other check in this function does: arm 5 below reads
    # `source.auth.mode`, and a reader that reached an invariant on a document
    # the other reader never parsed would refuse the same bytes in a different
    # layer with a different sentence.
    if not isinstance(doc["source"].get("auth"), dict):
        return "the document has no source.auth block; it is not a backup receipt"
    if not isinstance(doc["source"]["auth"].get("mode"), str):
        return "source.auth.mode is not a string"
    for name in ("from_ms", "to_ms"):
        value = doc["covered"].get(name)
        if not isinstance(value, int) or isinstance(value, bool):
            return f"covered.{name} is not an integer"
    if "manifest_key" not in doc["archive"]:
        return "the document has no archive.manifest_key field; it is not a backup receipt"
    return ""


def _receipt_parse_semver(v):
    """`(major, minor, patch)` or None — the twin of `backup_receipt.rs`'s
    `parse_semver`, which is STRICTER than the scorecard's `_major`.

    Exactly three dot-separated non-negative integers: `"1"`, `"1.0"`,
    `"1.0.0.0"` and `"1.0.0-rc1"` are all refused, because invariant 1 claims
    the WHOLE string parses. Rust's `str::parse::<u64>` accepts an optional
    leading `+` and nothing else — no whitespace, no underscore, no unicode
    digit, and nothing above 2**64-1 — so the pattern and the bound below are
    that function's accepted set and not Python's `int()`, which is wider on
    every one of those points.
    """
    parts = str(v).split(".") if isinstance(v, str) else []
    if len(parts) != 3:
        return None
    out = []
    for part in parts:
        if not re.fullmatch(r"\+?[0-9]+", part):
            return None
        n = int(part)
        if n >= 2 ** 64:
            return None
        out.append(n)
    return tuple(out)


def _render_topic_set(names) -> str:
    """`{"a", "b"}` — sorted, de-duplicated, Rust-debug-quoted.

    The twin of `backup_receipt.rs`'s `render_set`, whose input is a
    `BTreeSet<&str>`: sorted and unique by the type, quoted by `{:?}`. Arm 3's
    message interpolates two of these, so the ordering and the quoting are
    part of the refusal text the two readers must agree on byte for byte.
    """
    return "{" + ", ".join(_rust_debug_str(n) for n in sorted(set(names))) + "}"


def check_backup_receipt_invariants(doc) -> str:
    """The backup receipt's self-consistency rules, or "" when it holds.

    ARM FOR ARM, IN ORDER, WITH `BackupReceipt::validate_invariants`
    (crates/logweir-core/src/backup_receipt.rs), and with BYTE-IDENTICAL
    refusal text: `crates/logweir-core/tests/backup_receipt.rs` asserts each
    Rust message in full, `crates/logweir/tests/two_reader_parity_receipt.rs`
    walks `e2e/fixtures/invariants/backup-receipt-index.json` with both readers
    and compares their refusals to each other and to the recorded text, and
    `scripts/check-invariant-corpus.sh` derives the arm list from BOTH bodies
    and fails if they are not the same five arms in the same order.

    Called only after `_receipt_shape` returns "", so every field read here is
    present and of the right JSON type.

    1. `format_version` parses as semver and its major is 1.
    2. `exit_code == 0` **iff** `archive.manifest_key` is non-blank.
    3. `records` covers exactly `source.topics`.
    4. `covered.from_ms < covered.to_ms` — the end is EXCLUSIVE.
    5. `source.auth.mode` is `plaintext` or `scramSha512` and nothing else.
    """
    # ARM 1. GC12 for this document: a reader refuses a major it has never
    # seen rather than guessing at a shape. FIRST, so a document from a future
    # major is refused before any other arm is evaluated against fields that
    # build may have redefined.
    version = doc.get("format_version")
    parsed = _receipt_parse_semver(version)
    if parsed is None or parsed[0] != 1:
        return (
            f"format_version {_rust_debug_str(version)} is not a 1.x version this reader "
            "understands"
        )

    # ARM 2. A biconditional, both directions, one message. TRIMMED-EMPTY
    # COUNTS AS ABSENT (ruling R-A): naming no manifest and naming a manifest
    # made of spaces are the same claim.
    manifest_key = doc["archive"].get("manifest_key")
    named = bool(str(manifest_key or "").strip())
    exit_code = doc["exit_code"]
    if (exit_code == 0) != named:
        rendered = _rust_debug_str(manifest_key) if named else "absent"
        return (
            f"exit_code {exit_code} and manifest_key {rendered} disagree: a receipt names a "
            "manifest if and only if the backup exited 0"
        )

    # ARM 3. The counted set and the named set are the same set. Both sides are
    # rendered SORTED and DEDUPLICATED so the message is deterministic — over
    # there `records` is a BTreeMap and the comparison is between two
    # BTreeSets.
    counted = list(doc["records"].keys())
    named_topics = [str(x) for x in doc["source"]["topics"]]
    if sorted(set(counted)) != sorted(set(named_topics)):
        return (
            f"records covers {_render_topic_set(counted)} but the named topic set is "
            f"{_render_topic_set(named_topics)}"
        )

    # ARM 4. THE END IS EXCLUSIVE (Task 5b, from Task 5's review finding F3):
    # the window is half-open, `config/crd/backups.yaml` documents
    # `status.windowCovered.toMs` the same way (I22), and `from_ms == to_ms` is
    # an EMPTY range rather than an instantaneous one. A single-record backup
    # is still a window: the writer converts the manifest's inclusive newest
    # timestamp to an exclusive bound, producing `[t, t+1)`.
    from_ms = doc["covered"]["from_ms"]
    to_ms = doc["covered"]["to_ms"]
    if from_ms >= to_ms:
        return (
            f"covered.from_ms {from_ms} is not before covered.to_ms {to_ms}: the covered "
            "window's end is EXCLUSIVE, so an empty range covers no record"
        )

    # ARM 5 (Task 5b fix round 1). THE AUTH MODE'S VALUE SET IS CLOSED: two
    # values, which are `AuthSpec`'s serde tags, the `KafkaCluster` CRD's
    # `auth.mode` enum byte for byte, and the only strings
    # `AuthSpec::mode_str()` returns. Until this arm the field was an
    # unconstrained string neither reader looked at, so a receipt naming
    # `"totally-made-up"` verified 0/0 — while Task 17 copies this exact field
    # into `Backup.status.auth.mode`, whose CRD description promises these two.
    #
    # The value IS interpolated, unlike the scorecard's `target.auth` arms:
    # every arm of this document already echoes an adopter-supplied string,
    # and `_rust_debug_str` is what makes `{:?}`'s rendering reproducible here.
    mode = doc["source"]["auth"]["mode"]
    if mode not in ("plaintext", "scramSha512"):
        return (
            f"source.auth.mode {_rust_debug_str(mode)} is not one of the two values this "
            "format defines: \"plaintext\" or \"scramSha512\""
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
            f"(expected {payload_type_wanted!r}; pass --payload-type "
            + "|".join(sorted(PAYLOAD_TYPES))
            + " to check one of the other documents)",
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
            "outcome-entailment, all eleven required blocks in serde order, "
            "the six required non-block fields present and of the type their Rust type "
            "implies, u64 domain with null refused where Rust has no Option, "
            "target.auth's mode present, not blank, and one of the two values the "
            "format defines when the block is; "
            "approval.self_attested derived, not echoed)"
        )
        return 0

    if payload_type_wanted == PAYLOAD_TYPES["backup-receipt"]:
        # THE BACKUP RECEIPT'S OWN INVARIANTS (Task 5b). This is the second
        # document this script EVALUATES rather than merely authenticates, and
        # the dispatch is on the RESOLVED media type — never on what the bytes
        # happen to parse as — so the scorecard's arms and the receipt's arms
        # can never run over the other's document.
        #
        # Shape first, then the arms: `logweir drill verify` gets the shape
        # layer from `serde_json` and reports a missing block WITHOUT reaching
        # an invariant, so a reader that went straight to the arms here would
        # raise a KeyError where the Rust reader prints one line.
        problem = _receipt_shape(doc) or check_backup_receipt_invariants(doc)
        if problem:
            print(f"INVALID: {problem}", file=sys.stderr)
            return 1
        archive = doc["archive"]
        covered = doc["covered"]
        records = doc["records"]
        print(f"VALID  backup receipt for {doc.get('backup_id')}")
        print(f"       run_id={doc.get('run_id')}  exit_code={doc.get('exit_code')}")
        print(
            f"       source cluster {doc['source'].get('cluster_id')} "
            f"auth={doc['source'].get('auth', {}).get('mode')} "
            f"topics={len(doc['source'].get('topics', []))}"
        )
        # The manifest and its digest are what an auditor goes and looks with.
        # An empty key is LEGAL and means the backup did not exit 0 (arm 2), so
        # it is spelled out rather than printed blank.
        if str(archive.get("manifest_key") or "").strip():
            print(f"       manifest {archive.get('manifest_key')}")
            print(f"       manifest_sha256={archive.get('manifest_sha256')}")
        else:
            print("       manifest: none — this receipt is for a backup that did not exit 0")
        print(
            f"       records={sum(v for v in records.values() if isinstance(v, int))} "
            f"across {len(records)} topic(s)"
        )
        # HALF-OPEN, and said so: `to_ms` is EXCLUSIVE (I22), which is the one
        # thing about these two integers a reader can get wrong by a whole
        # record.
        print(
            f"       covered [{covered.get('from_ms')}, {covered.get('to_ms')}) "
            "epoch ms — the end is EXCLUSIVE"
        )
        print(
            "       This signature covers the receipt only. It says what THIS run "
            "captured; it is not a claim about any other backup of the same topics."
        )
        print(
            f"       verifier: verify_scorecard.py {SCRIPT_VERSION} "
            "(backup-receipt invariant set: format_version's major, the "
            "exit_code/manifest_key biconditional with trimmed-empty counted as absent, "
            "records covering exactly the named topic set, a covered window whose "
            "EXCLUSIVE end is after its start, and source.auth.mode inside the closed "
            "two-value set)"
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
            f"usage: {sys.argv[0]} "
            "[--payload-type " + "|".join(sorted(PAYLOAD_TYPES)) + "] "
            "<document.json> <document.sig> <public.pem>",
            file=sys.stderr,
        )
        sys.exit(1)
    sys.exit(main(rest[0], rest[1], rest[2], wanted))
