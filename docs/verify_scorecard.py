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
# `calendar` is stdlib: PROD-04.1's arm 31 compares two RFC 3339 instants.
import calendar
import hashlib
import json
# `re` is stdlib, like every other import here: this script's ONLY third-party
# dependency is `cryptography` (see the module docstring), and Task 5b's
# `_receipt_parse_semver` needs a pattern rather than Python's `int()`, whose
# accepted set is wider than Rust's `str::parse::<u64>` in four different ways.
import re
import sys
# `unicodedata` is stdlib too: arm 18's "no control character" is Rust's
# `char::is_control`, which is exactly the Unicode general category `Cc`.
import unicodedata

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
#
# `1.1.0` since FX-4 (`topic_parity.not_assessed` and `target_diff.not_assessed`,
# the first scorecard fields added after the v0.1 tags — a MINOR bump,
# docs/stability.md). Only the MAJOR
# is ever compared, so a 1.0.0 document still verifies and a reader built
# before the bump still reads a 1.1.0 one.
#
# `1.2.0` since FX-3 (`topic_parity.not_reconstructed`: the source settings a
# `newTopic` restore did not reconstruct, which phase 7 had labelled
# `intentionally_deviated` with a scratch-only rationale in every mode).
#
# `1.3.0` since FX-8 (`source.time_basis`: which source topics a restore
# selected by producer time, and which it selected by time with no recorded
# timestamp type). 1.2.0 is FX-3's (`topic_parity.not_reconstructed`).
#
# `1.4.0` since PROD-08.1 (`integrity.verification`: whether the verdict
# covered a sample or every selected record, the structured capture gaps and
# pruned ranges, and a complete verification's archive integrity and replay
# comparison).
FORMAT_VERSION = "1.4.0"

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
# 1.11.0 (Task 9b) adds ONE arm: `evidence.offset_report_key` and
# `evidence.offset_report_sha256` are present or absent TOGETHER. They are two
# new NESTED OPTIONAL fields on the scorecard's evidence block — Global
# Constraint 12 as amended permits that kind and no other, and the document
# stays at its frozen 21 top-level properties and 17 required ones. They record
# the object key and digest of the ENGINE's offset-mapping report, which the
# runner uploads beside the scorecard because the engine writes it to a
# pod-local path [U:crates/kafka-backup-core/src/config.rs:857-858] and the pod
# is then deleted. Tag 1 RENDERS that report and applies nothing (Global
# Constraints 20 and 35): no consumer-group offset is committed anywhere.
#
# NOTE FOR ANY LATER TASK QUOTING A BRIEF: task-9b-brief.md says "1.9.0 ->
# 1.10.0". 1.10.0 was already taken, by Task 5b's fix round, which closed the
# auth mode's value set. The brief's number is the error, not this constant.
# 1.12.0 (Task 9b fix round 1, review F1) adds ONE arm and one new nested
# optional field. `target.mode` is the scorecard's first record of WHICH of the
# two target modes a run was in — `scratch` (absent on the wire, the default
# and everything v0.1 wrote) or `newTopic`. `target.marker_topic` becomes
# OPTIONAL, because its documented meaning is the scratch segregation proof —
# `cluster_id in allowedClusterIds` AND the topic exists, both verified at
# phase 0 — and `newTopic` mode skips both checks; a `newTopic` run that wrote
# the spec's value there was reporting a verification that never ran, measured
# on a real cluster with the marker topic DELETED and an EMPTY allowlist. The
# arm is the converse and it is the one that matters to an auditor: a document
# in `scratch` mode with NO marker topic claims the proof while omitting the
# thing the proof was about. Both are NESTED fields on `target` — Global
# Constraint 12 as amended permits that kind and no other — and the document
# stays at its frozen 21 top-level properties and 17 required ones.
#
# 1.13.0 (Task 10) closes Task 9b's re-review NIT-1 and pays Global Constraint
# 12's price for the restored-count bound.
#
# NIT-1, measured rather than reasoned: a scorecard carrying
# `target.mode: "bogus"` — or `"mode": null` — made `drill verify` exit 1 at
# DESERIALISATION (`signature verified but the payload is not a scorecard:
# unknown variant `bogus`, expected `scratch` or `newTopic`` and `signature
# verified but the payload is not a scorecard: expected value`) while THIS
# reader printed VALID and exited 0. That is a one-sided disagreement, which is
# the one thing the parity claim in this file's module comment forbids
# outright; `target.mode`'s value set is now closed here too, and both
# documents are corpus cases in `shape-index.json`.
#
# Global Constraint 12's price, the OTHER half: `phase7_verify`'s new
# restored-count bound writes its failure into `integrity.partial_reason`, a
# field that already exists and whose only invariant is that a `partial` result
# must not leave it blank. What was NOT pinned is the shape the bound actually
# writes — `integrity.result: fail` with a reason PRESENT, and the same
# document with it ABSENT — so `index.json` gains both as ACCEPT cases. No new
# field, no new arm, and no change to the frozen top-level shape.
#
# 1.15.0 (FX-4) reads the BACKUP RECEIPT's format 1.1.0 block
# `config_coverage` — per named topic, whether the archive's record of the
# topic configuration was captured (`captured`, `notCaptured` with a reason,
# `captureDenied`), and the EFFECTIVE `message.timestamp.type` with its source.
# Six arms, 6-11, mirrored byte for byte from `BackupReceipt::
# validate_invariants`: the block only under a minor of at least 1, covering
# exactly `source.topics`, closed sets for the coverage, the reason (present
# exactly when `notCaptured`) and the timestamp value and source, and no
# timestamp type recorded by a read that did not succeed. They read the new
# block and nothing else, so every 1.0.0 receipt is decided exactly as before;
# an ABSENT block is UNKNOWN coverage, never `captured`. The scorecard's
# FORMAT_VERSION moves to 1.1.0 for `topic_parity.not_assessed`, which gets no
# arm: it is printed, so an exit 0 is never read as configuration parity the
# document does not claim. `target_diff.not_assessed` (phase 3's collisions
# whose configuration was not assessed) is shape-checked like it and not
# printed: the collisions themselves are not printed either.
#
# 1.16.0 (FX-7, which merged after FX-4) reads the backup receipt's
# `archive.manifest_version_id` — the object version a receipt taken on a
# versioned bucket pins (receipt format 1.2.0) — refuses it at the SHAPE layer
# when it is present and not a string, and prints it, and prints a catalog point
# record's copy of it (catalog point format 1.2.0). No invariant arm and no
# payload type is added; a 1.15.0 reader reads a 1.2.0 receipt as the 1.1.0
# document under it and prints no version line.
#
# 1.17.0 (FX-3, which merged after FX-7) knows scorecard format 1.2.0 and its
# `topic_parity.not_reconstructed`. Five arms, NR-1 to NR-5, mirrored byte for
# byte and in position from `Scorecard::validate_invariants`: the block only
# under a version of at least 1.2.0, every entry also in
# `unexpected_divergence` (the fail-safe twin an older reader sees), none
# in `intentionally_deviated`, and, in a newTopic document, nothing intended
# and every unexpected deviation on a setting the restore decides named in the
# block (NR-4, NR-5: FX-3 review F1). They fire only on a document carrying the
# block, so every document without it is decided exactly as before. The shape
# layer now also refuses `topic_parity.intentionally_deviated` and
# `unexpected_divergence` that are not arrays of strings -- the Rust reader
# refuses them at deserialisation and this script printed VALID (measured at
# main b8b9263f) -- and the new field the same way. The `reconstruction:` line
# names what a newTopic restore did not reconstruct, and says that a newTopic
# document before 1.2.0 labelled those settings intended. A renumber moves this
# line, the two literal pins in docs/test_verify_scorecard.py and the guide's
# table.
#
# 1.18.0 (FX-8) knows scorecard format 1.3.0 and its `source.time_basis`: the
# approved plan's `restore.time_basis`, the source topics a restore selected by
# producer time (recorded `LogAppendTime`, accepted by the plan) and the ones it
# selected by time with no recorded timestamp type. Four arms, TB-1 to TB-4,
# mirrored byte for byte and in position from `Scorecard::validate_invariants`:
# the block only under a version of at least 1.3.0, `plan` only
# `producerTime`, a producer-time topic only under it, and no topic in both
# lists. They fire only on a document carrying the block, so every document
# without it is decided exactly as before. The shape layer refuses a block that
# is not an object of an optional string `plan` and two arrays of strings. The
# `time basis:` lines say which topics were selected by producer time or with
# an unrecorded type, and that a document before 1.3.0 does not say; a backup
# receipt's `time basis:` lines name its LogAppendTime topics, whose covered
# window is their producers' time. 1.16.0 is FX-7's and 1.17.0 FX-3's. A
# renumber moves this line, the literal pins in docs/test_verify_scorecard.py
# and the guide's table together.
#
# 1.19.0 (PROD-08.1) knows scorecard format 1.4.0 and its
# `integrity.verification`: `coverage` (sampled or complete), what the
# restored records were compared with, whether header order was verified,
# application validation, the verified partitions' capture gaps and pruned
# ranges, and a complete verification's archive integrity, replay comparison
# and per-partition counts. Seven arms, IV-1 to IV-7, mirrored byte for byte
# and in position from `Scorecard::validate_invariants`: the block only under a
# version of at least 1.4.0, a coverage of `sampled` or `complete`, header order
# `verified` only for complete coverage, the complete block exactly with
# complete coverage, an incomplete reason exactly when not covered, a pass only
# over a covered, clean complete block (clean in total and in every partition,
# over at least one partition), and totals that are the partitions' sums. They
# fire only on a document carrying the block, so every document without it is
# decided exactly as before. The shape layer refuses a block whose fields are not
# of the types the writer gives them. The `integrity coverage:` lines say what
# the verdict covered, and that a document before 1.4.0 covered a sample. Every
# blank test (ruling R-A) now strips `RUST_WHITESPACE`, Rust's own `trim` set,
# where it stripped Python's wider one (U+001C..U+001F too). A renumber moves
# this line, the literal pins in docs/test_verify_scorecard.py and the guide's
# table together.
#
# 1.20.0 (PROD-05.1) knows receipt format 1.3.0 and its `topic_configuration`
# and `owner_detection` (arms 12 to 20), and prints the configuration model,
# one line per topic, in the Rust reader's words.
#
# 1.21.0 (PROD-01.3) knows the three auth modes PROD-01.3 adds --
# `scramSha256`, `plain` (SASL/PLAIN, over TLS only) and `mtls` (a TLS client
# certificate) -- as VERSIONED values of the two existing auth-mode fields:
# the backup receipt's `source.auth.mode` from receipt format 1.4.0, and the
# scorecard's `target.auth.mode` from scorecard format 1.5.0. Each field's one
# arm becomes three statements, mirrored byte for byte and in position: the
# closed two below the new version (unchanged), a new value under a version
# that predates it, and the closed five from the new version. Every document
# that predates PROD-01.3 is decided exactly as before; an older script refuses
# a document naming a new mode, which is the safer verdict (OD-7, third case).
#
# 1.22.0 (FX-23) knows scorecard format 1.6.0 and its optional
# `sample.unsampled_topics`: the restored topics with records in the window
# that `sample.max_partitions` left without a sampled partition. Three arms,
# US-1 to US-3, mirrored byte for byte and in position from
# `Scorecard::validate_invariants`: the field only under a version of at least
# 1.6.0, a non-empty, sorted list of non-blank names with no repeat, and never
# beside a complete verification. They fire only on a document carrying the
# field, so every document without it is decided exactly as before (OD-7 (a)).
# The shape layer refuses a field that is not an array of strings, and the
# `sample coverage:` line names the unsampled topics in the Rust reader's
# words. A second `sample coverage:` line, for a sampled `pass` (FX-23 review
# M2), says what that pass proves at the document's version: from 1.6.0 the
# per-partition bound, every-topic sampling and the engine-report check;
# before 1.6.0 only the canary and one count bound over every topic together,
# because such a document is the same bytes whichever build signed it.
#
# 1.23.0 (PROD-11.1) knows scorecard format 1.7.0 and its optional
# `source.selection`: a narrowed restore's stated inclusive window start and
# its end (a START only: partition subsets are refused by the runner until the
# owner decides OD-9). Three arms, SEL-1 to SEL-3, mirrored byte for byte and
# in position from `Scorecard::validate_invariants`: the block only under a
# version of at least 1.7.0, a start before the end, and a complete block over
# the selection's window. They fire only on a document carrying the block, so
# every document without it is decided exactly as before (OD-7 (a)). The shape
# layer refuses a block that is not the writer's shape; a `replay selection:`
# coverage line names the window, and the sampled-pass line of a narrowed
# document is qualified by its window, in the Rust reader's words.
#
# 1.24.0 (PROD-03.0) knows backup receipt format 1.5.0 and its optional
# `schema_dependency`: per named topic, whether the archived keys or values
# carry Confluent wire-format framing (magic byte 0 and a schema id), with the
# ids seen, judged from archived bytes and never from a registry. Eight arms,
# 22 to 29, mirrored byte for byte and in position from
# `BackupReceipt::validate_invariants`: the block only from 1.5.0, its topic
# set, the closed verdict/reason/basis sets, both sides exactly when judged,
# the judged count against `records`, the schema ids, the one-in-ten
# threshold, and the verdict from its sides. They fire only on a document
# carrying the block, so every earlier receipt is decided exactly as before
# (OD-7 (a)). The shape layer refuses a block that is not the writer's shape,
# and `schema_dependency` lines say, per topic, what the Rust reader says.
# 1.25.0 (PROD-01.4a) knows receipt format 1.6.0 and its `generations`: per
# named topic, the topic ID (KIP-516) Logweir's own DescribeTopics read
# returned before the engine and after it, or `null` with the reason. Five arms,
# 36 to 40, mirrored byte for byte and in position from
# `BackupReceipt::validate_invariants`: the block only from 1.6.0, covering
# exactly the named topic set, every recorded ID in Kafka's text (22 URL-safe
# base64 characters over 16 bytes, never Kafka's reserved zero or (0, 1) ID), a reason exactly for
# a null ID from the closed set, and a source exactly for a recorded one. They
# fire only on a document carrying the block (OD-7 (a)). The shape layer
# refuses a block whose fields are not strings or null, and the `generations`
# lines say, per topic, whether the capture saw one generation, saw the topic
# recreated while it ran, or could not establish it by ID.
# 1.26.0 (PROD-04.1) knows receipt format 1.7.0 and its `consumer_positions`:
# the consumer position evidence of the groups a backup selected, as a
# summary — per group its outcome, type, states, members, active flag and
# position counts, the capture window, and the positions document beside the
# receipt by key, SHA-256 and length — whose size depends on the selection,
# never on partitions. Six arms, 30 to 35, mirrored byte for byte and in
# position from `BackupReceipt::validate_invariants`: the block only under a
# version of at least 1.7.0; a forward capture window, a closed listing and at
# least one group; this run's document by a well-formed digest; each group's
# outcome and reason from the closed sets; what a captured (never `Dead` with
# no member), a GroupTypeNotCaptured and any other group records; and an
# `active` the two states derive. With `--consumer-positions <file>` it also
# checks that document against the receipt — fourteen arms, CP-1 to CP-14,
# mirrored from `BackupReceipt::validate_consumer_positions_document`: bound
# by digest and length, the receipt's backup and run, the named topics with
# their partitions in order and well-formed marks, a derived changed flag,
# exactly the captured groups, no position on a changed topic, no capture
# over an unread topic, every partition accounted for so absence is never
# offset 0, each position's status, value, reason and derived coverage, and
# the receipt's counts — and prints each position.
#
# 1.27.0 (PROD-11.1b, the owner's decision OD-9 (a)) reads scorecard format
# 2.0.0, the format's first MAJOR, written ONLY for a restore that states a
# partition subset: 1.7.0's fields with the subset meaning (its
# `complete.partitions[]` and the sampled lane's fields name the SELECTED
# partitions). Major 2 is read for that shape alone -- a 2.x document without
# `source.selection.partitions` is refused before any arm (PS-1) -- and every
# arm of major 1 holds for it. PS-2 holds a format-1 block to a start and its
# end (so no subset rides in a document an older reader accepts); PS-3 to
# PS-5 judge the subset list, the engine runs and the complete block. The
# shape layer reads the 2.0.0 block (an optional start, the subsets, the
# runs); the `replay selection:` line names the subset and the sampled-pass
# line of a subset document is qualified by it, in the Rust reader's words.
# Every reader before 1.27.0 refuses a 2.0.0 document as an unsupported major.
#
# 1.28.0 (PROD-15.1) knows scorecard format 1.8.0 and its optional
# `target.original_name`: a restore under the source's ORIGINAL topic names,
# into absent topics (OD-2). Fourteen arms, ON-1 to ON-14, mirrored byte for
# byte and in position from `Scorecard::validate_invariants`: the block only
# under a 1.x version of at least 1.8.0, only in a newTopic document with the
# empty prefix, the approval subject `originalName`, the approval mode and the
# cluster condition from their closed sets, `targetIsNotSource` only beside a
# known source cluster id that is not the target's, somewhere looked for an
# owner, each owner from a place looked in, an owned name only on the owner
# path, a one-person confirmation only with the names typed, the resources file
# named by digest, a COMPLETE verification (never a sampled one, and never a
# pass that records none), and never beside a partition subset (ON-14, judged
# first: such a restore restores whole topics, so the block is format 1's and
# no 2.x document carries it).
# They fire only on a document carrying the block, so every document without
# it is decided exactly as before (OD-7 (a)). The shape layer refuses a block
# that is not the writer's shape; two `original name:` lines say what admitted
# the restore, in the Rust reader's words.
#
# 1.29.0 (PROD-16.2) knows scorecard format 1.9.0, and 2.1.0 of format 2, and
# their optional `approval.console`: a restore a SECOND PERSON APPROVED IN THE
# CONSOLE (no personal key: the console attests who asked and who approved,
# and signs both). The block says who approved and how -- the mode, the
# requester, the approver, when the request was made, when it was approved,
# when it would have expired, and the console key that signed both. Eight
# arms, CA-1 to CA-8, mirrored byte for byte and in position (after the ON
# arms, before `redactions`) from `Scorecard::validate_invariants`: the block
# only from 1.9.0 of format 1 or 2.1.0 of format 2; its mode `consoleApproval`;
# two people -- each an issuer and a subject of visible ASCII, neither the
# in-cluster administrator nor a Kubernetes system identity, of one issuer,
# with different subjects (compared without case); the approval inside the
# request's own window; `approval.approver` the approver's `<issuer>#<subject>`;
# `approval.key_id` the console's key, which is EXPECTED in this mode and is
# said so in the printed line; `approval.approved_at` the instant the second
# person approved, never the request's; and `target.original_name.approval_mode`
# `consoleApproval` exactly when the block is present. That closed set gains
# the member `consoleApproval` FROM 1.9.0: ON-5 is split by version, as
# PROD-01.3 split `target.auth.mode`'s arm -- under 1.8.0 the new member is
# refused as a value that version does not define, every 1.8.0 document is
# judged exactly as before, and from 1.9.0 the set is four. A 1.28.0 reader
# refuses a 1.9.0 document naming the new member by its own closed-set
# sentence, which is the safer verdict (OD-7's third case), and accepts every
# other 1.9.0 or 2.1.0 document, ignoring the block: it then reads the
# approver's principal beside the console's key, the shape of a one-person
# confirmation, never of a personal-key approval. CA-1 to CA-7 fire only
# on a document carrying the block, and CA-8 only on one carrying
# `target.original_name`, so every other document is decided exactly as before
# (OD-7 (a)). No arm's message carries a principal, an instant or a key id.
# The shape layer refuses a block that is not the writer's shape; one
# `console approval:` line says who approved and how, in the Rust reader's
# words.
SCRIPT_VERSION = "1.29.0"

# PROD-11.1b: the format of a partition-subset restore and its major --
# `FORMAT_VERSION_WITH_PARTITION_SUBSETS` and `PARTITION_SUBSETS_MAJOR` in
# `crates/logweir-core/src/scorecard.rs`, which they must equal
# (`docs/test_verify_scorecard.py::test_the_partition_subset_major_is_the_rust_readers`).
# The newest major this script reads, and only for that shape (arm PS-1).
SCORECARD_PARTITION_SUBSETS_VERSION = "2.0.0"
SCORECARD_PARTITION_SUBSETS_MAJOR = 2

# The first minor of SCORECARD format 1 whose `target.auth.mode` may be
# `scramSha256`, `plain` or `mtls` (PROD-01.3) -- `AUTH_MODES_SINCE_MINOR` in
# `crates/logweir-core/src/scorecard.rs`, which it must equal
# (`docs/test_verify_scorecard.py::test_the_auth_modes_minors_are_the_rust_readers`).
SCORECARD_AUTH_MODES_SINCE_MINOR = 5

# The first minor of the BACKUP RECEIPT's format 1 whose `source.auth.mode` may
# be `scramSha256`, `plain` or `mtls` (PROD-01.3) -- `AUTH_MODES_SINCE_MINOR` in
# `crates/logweir-core/src/backup_receipt.rs`, which it must equal.
RECEIPT_AUTH_MODES_SINCE_MINOR = 4

# The two auth modes every format defines, and the three PROD-01.3 adds --
# `ORIGINAL_AUTH_MODES` / `PROD_01_3_AUTH_MODES` in
# `crates/logweir-core/src/connection.rs`, which they must equal.
ORIGINAL_AUTH_MODES = ("plaintext", "scramSha512")
PROD_01_3_AUTH_MODES = ("scramSha256", "plain", "mtls")

# The first minor of SCORECARD format 1 that defines `source.selection` (arm
# SEL-1, PROD-11.1) -- `SELECTION_SINCE_MINOR` in
# `crates/logweir-core/src/scorecard.rs`, which it must equal
# (`docs/test_verify_scorecard.py::test_the_selection_minor_is_the_rust_readers`).
SCORECARD_SELECTION_SINCE_MINOR = 7

# The first minor of SCORECARD format 1 that defines `target.original_name`
# (arm ON-1, PROD-15.1) -- `ORIGINAL_NAME_SINCE_MINOR` in
# `crates/logweir-core/src/scorecard.rs`, which it must equal
# (`docs/test_verify_scorecard.py::test_the_original_name_minor_is_the_rust_readers`).
SCORECARD_ORIGINAL_NAME_SINCE_MINOR = 8

# `target.original_name`'s closed sets (PROD-15.1) -- `ORIGINAL_NAME_APPROVAL_MODES`
# in `scorecard.rs`, `CLUSTER_CONDITIONS` and `OWNER_DETECTION_PLACES` in
# `crates/logweir-core/src/original_name.rs`, and `OWNER_KINDS` in
# `topic_configuration.rs`, which they must equal.
ORIGINAL_NAME_APPROVAL_MODES = ("v1Approval", "governed", "ordinary", "consoleApproval")
# The same set under 1.8.0, before PROD-16.2 added its fourth member --
# `ORIGINAL_NAME_APPROVAL_MODES_AT_1_8_0` in `scorecard.rs`. ON-5 is split by
# version: a 1.8.0 document is judged against these three, exactly as before.
ORIGINAL_NAME_APPROVAL_MODES_AT_1_8_0 = ("v1Approval", "governed", "ordinary")
ORIGINAL_NAME_CLUSTER_CONDITIONS = ("targetIsNotSource", "autoCreateDisabled")
ORIGINAL_NAME_OWNER_DETECTION_PLACES = ("plan", "kafkaTopicResources", "pointReceipt")
ORIGINAL_NAME_OWNER_KINDS = ("strimzi", "external")

# PROD-16.2: `approval.console`, a restore a second person approved in the
# console. The first minor of format 1, and of format 2, that defines the
# block (arm CA-1) -- `CONSOLE_APPROVAL_SINCE_MINOR` and
# `CONSOLE_APPROVAL_SINCE_MINOR_OF_MAJOR_2` in
# `crates/logweir-core/src/scorecard.rs`, which they must equal
# (`docs/test_verify_scorecard.py::test_the_console_approval_minors_are_the_rust_readers`).
SCORECARD_CONSOLE_APPROVAL_SINCE_MINOR = 9
SCORECARD_CONSOLE_APPROVAL_SINCE_MINOR_OF_MAJOR_2 = 1

# The block's one mode, and the new member of `ORIGINAL_NAME_APPROVAL_MODES`
# above -- `APPROVAL_MODE_CONSOLE` in `scorecard.rs`. THE VOCABULARY IS HELD TO
# THE RUST READER'S BY A ROW, member for member and in order:
# `crates/logweir/tests/two_reader_parity.rs::
# the_approval_mode_vocabulary_is_the_same_set_in_both_readers` reads the two
# assignments out of this file's text.
APPROVAL_MODE_CONSOLE = "consoleApproval"

# The identity rule of a console approval (arm CA-3) -- `LOCAL_ADMIN_ISSUER`,
# `KUBERNETES_SYSTEM_SUBJECT_PREFIX`, `MAX_COMPARABLE_ISSUER_LEN` and
# `MAX_COMPARABLE_SUBJECT_LEN` in `crates/logweir-core/src/approval_policy.rs`,
# which they must equal
# (`docs/test_verify_scorecard.py::test_the_console_identity_rule_is_the_rust_readers`).
CONSOLE_LOCAL_ADMIN_ISSUER = "urn:logweir:local-admin"
CONSOLE_SYSTEM_SUBJECT_PREFIX = "system:"
CONSOLE_MAX_COMPARABLE_LEN = 255

# The first minor of SCORECARD format 1 that defines `sample.unsampled_topics`
# (arm US-1, FX-23) -- `UNSAMPLED_TOPICS_SINCE_MINOR` in
# `crates/logweir-core/src/scorecard.rs`, which it must equal
# (`docs/test_verify_scorecard.py::test_the_unsampled_topics_minor_is_the_rust_readers`).
SCORECARD_UNSAMPLED_TOPICS_SINCE_MINOR = 6

# The first minor of SCORECARD format 1 that defines `integrity.verification`
# (arm IV-1) -- `VERIFICATION_SINCE_MINOR` in
# `crates/logweir-core/src/scorecard.rs`, which it must equal
# (`docs/test_verify_scorecard.py::test_the_verification_minor_is_the_rust_readers`).
# A renumber moves both, and FORMAT_VERSION.
SCORECARD_VERIFICATION_SINCE_MINOR = 4

# The eight counts of a `ReplayComparison` (PROD-08.1), in its declaration
# order; arm IV-7 sums each over `complete.partitions`.
REPLAY_FIELDS = (
    "expected",
    "restored",
    "matching",
    "missing",
    "unexpected",
    "duplicates",
    "out_of_order",
    "mismatched",
)


def _replay_exact(replay) -> bool:
    """`ReplayComparison::is_exact`: no fault of any kind, every expected
    record restored once and matching, nothing else restored. Each conjunct is
    held to the Rust reader by its own corpus case (review M-1)."""
    return (
        replay["missing"] == 0
        and replay["unexpected"] == 0
        and replay["duplicates"] == 0
        and replay["out_of_order"] == 0
        and replay["mismatched"] == 0
        and replay["matching"] == replay["expected"]
        and replay["restored"] == replay["expected"]
    )


def _sat_add(a: int, b: int) -> int:
    """`u64::saturating_add`: the Rust reader's sums stop at `u64::MAX`, so
    IV-7 compares the same numbers in both readers."""
    return min(a + b, 2**64 - 1)

# The first minor of SCORECARD format 1 that defines `source.time_basis` (arm
# TB-1) -- `TIME_BASIS_SINCE_MINOR` in `crates/logweir-core/src/scorecard.rs`,
# which it must equal (`docs/test_verify_scorecard.py::
# test_the_time_basis_minor_is_the_rust_readers`). A renumber moves both, and
# FORMAT_VERSION.
SCORECARD_TIME_BASIS_SINCE_MINOR = 3

# The first minor of SCORECARD format 1 that defines
# `topic_parity.not_reconstructed` (arm NR-1) -- `NOT_RECONSTRUCTED_SINCE_MINOR`
# in `crates/logweir-core/src/scorecard.rs`, which it must equal
# (`docs/test_verify_scorecard.py::test_the_not_reconstructed_minor_is_the_rust_readers`).
# A renumber moves both, and FORMAT_VERSION.
TOPIC_PARITY_NOT_RECONSTRUCTED_SINCE_MINOR = 2

# The four settings a restore's own topic creation decides instead of copying
# from the source -- `RESTORE_DECIDED_SETTINGS` in
# `crates/logweir-core/src/scorecard.rs`, which it must equal
# (`docs/test_verify_scorecard.py::test_the_restore_decided_settings_are_the_rust_readers`).
# Arm NR-5 reads it.
RESTORE_DECIDED_SETTINGS = (
    "cleanup.policy",
    "partition_count",
    "replication_factor",
    "retention.ms",
)


def _parity_key(entry: str) -> str:
    """The `<key>` of a `topic_parity` entry `"<target topic>: <key>"`.

    Mirrors `logweir_core::scorecard::parity_key`: the text after the LAST
    `": "`, or the whole entry when it has none. Read by arm NR-5 only.
    """
    return entry.rsplit(": ", 1)[-1]

# The first minor of the BACKUP RECEIPT's format 1 that defines
# `config_coverage` (arm 6) — `CONFIG_COVERAGE_SINCE_MINOR` in
# `crates/logweir-core/src/backup_receipt.rs`, which it must equal
# (`docs/test_verify_scorecard.py::test_the_config_coverage_minor_is_the_rust_readers`).
# A renumber (for instance to 1.2.0) changes both, and SCRIPT_VERSION.
RECEIPT_CONFIG_COVERAGE_SINCE_MINOR = 1

# PROD-05.1: the first minor of the BACKUP RECEIPT's format 1 that defines
# `topic_configuration` (arm 12) — `TOPIC_CONFIGURATION_SINCE_MINOR` in
# `crates/logweir-core/src/backup_receipt.rs`, which it must equal
# (`docs/test_verify_scorecard.py::test_the_topic_configuration_minor_is_the_rust_readers`).
RECEIPT_TOPIC_CONFIGURATION_SINCE_MINOR = 3

# PROD-04.1: the first minor of the BACKUP RECEIPT's format 1 that defines
# `consumer_positions` (arm 30) — `CONSUMER_POSITIONS_SINCE_MINOR` in
# `crates/logweir-core/src/backup_receipt.rs`, which it must equal
# (`docs/test_verify_scorecard.py::test_the_consumer_positions_minor_is_the_rust_readers`).
RECEIPT_CONSUMER_POSITIONS_SINCE_MINOR = 7

# PROD-04.1: the closed vocabularies of `consumer_positions`, in the order
# `crates/logweir-core/src/consumer_positions.rs` declares them, which they must
# equal (`docs/test_verify_scorecard.py::test_the_consumer_positions_vocabulary_is_the_rust_readers`).
CP_EXCLUDED_REASONS = ("GroupTypeNotCaptured", "GroupNotFound")
CP_FAILED_REASONS = (
    "NotVisibleToPrincipal",
    "NotVisibleOrUnreachable",
    "NotAuthorized",
    "PositionsUnstable",
    "ListingInconsistent",
    "AbsenceUnproven",
    "TypeUnproven",
    "Unreachable",
    "DescribeFailed",
    "PositionsFailed",
    "GenerationChangedDuringCapture",
    "CaptureUnavailable",
    "GroupVanishedDuringCapture",
    "PartitionsNotRead",
)
CP_CAPTURED_TYPES = ("classic", "consumer")
CP_GROUP_STATES = (
    "PreparingRebalance",
    "CompletingRebalance",
    "Stable",
    "Dead",
    "Empty",
    "stateUnknownToClient",
)
CP_INACTIVE_STATES = ("Empty", "Dead")
CP_POSITION_STATUSES = ("captured", "excluded", "failed", "notObserved")
CP_POSITION_FAILED_REASONS = (
    "TopicNotAuthorized",
    "Unstable",
    "NotAPosition",
    "PartitionFailed",
    "MarksNotRead",
)
CP_NOT_OBSERVED_REASONS = ("PartitionAddedDuringCapture", "TopicNotObserved")
CP_COVERAGE_RELATIONS = (
    "beforeLogStart",
    "noArchivedData",
    "beforeArchive",
    "withinArchive",
    "atArchiveEnd",
    "beyondArchive",
)
CP_RELATED = ("withinArchive", "atArchiveEnd")
CP_LISTING_VALUES = ("complete", "notComplete")


def _cp_relation(position, facts):
    """`logweir_core::consumer_positions::relation`, word for word: the
    verdict a committed position's partition facts derive."""
    log_start = facts.get("log_start")
    high = facts.get("high_watermark")
    if log_start is None or high is None:
        return "MarksNotRead"
    if position > high:
        return "PositionBeyondEnd"
    if position < log_start:
        return "beforeLogStart"
    first = facts.get("archived_first")
    last = facts.get("archived_last")
    if first is None or last is None:
        return "noArchivedData"
    if position < first:
        return "beforeArchive"
    if position <= last:
        return "withinArchive"
    # `i64::saturating_add(1)`.
    if position == min(last + 1, 2**63 - 1):
        return "atArchiveEnd"
    return "beyondArchive"


def _cp_changed(partitions):
    """`logweir_core::consumer_positions::changed_during_capture`."""
    for p in partitions:
        for before, after in (
            ("log_start", "log_start_after"),
            ("high_watermark", "high_watermark_after"),
        ):
            b, a = p.get(before), p.get(after)
            if b is not None and a is not None and a < b:
                return True
    return False


def _cp_active(state, listed):
    """`logweir_core::consumer_positions::active`."""
    return state not in CP_INACTIVE_STATES or listed not in CP_INACTIVE_STATES


def _cp_vanished(state, members) -> bool:
    """`logweir_core::consumer_positions::vanished`: `Dead` with no member."""
    return state == "Dead" and members == 0


def _cp_document_key(backup_id, run_id) -> str:
    """`logweir_core::consumer_positions::document_key`."""
    return f"logweir/backups/{backup_id}/{run_id}.consumer-positions.json"


CP_COUNT_NAMES = ("related", "not_related", "never_committed", "beyond_end", "failed",
                  "not_observed")


def _cp_counts_of(group):
    """`PositionCounts::of` over one captured group's document entry."""
    c = dict.fromkeys(CP_COUNT_NAMES, 0)
    c["never_committed"] = group["no_committed_position"]
    for p in group["positions"]:
        status = p["status"]
        if status == "captured":
            slot = "related" if p.get("coverage") in CP_RELATED else "not_related"
        elif status == "excluded":
            slot = "beyond_end"
        elif status == "failed":
            slot = "failed"
        else:
            slot = "not_observed"
        c[slot] = min(c[slot] + 1, 2**32 - 1)
    return c


def _cp_counts_total(c) -> int:
    """`PositionCounts::total`."""
    return sum(c[n] for n in CP_COUNT_NAMES)


def _cp_counts_render(c) -> str:
    """`PositionCounts::render`, word for word."""
    return (
        f"{c['related']} related, {c['not_related']} not related, {c['never_committed']} "
        f"never committed, {c['beyond_end']} beyond the end, {c['failed']} failed, "
        f"{c['not_observed']} not observed"
    )


_RFC3339 = re.compile(
    r"([0-9]{4})-([0-9]{2})-([0-9]{2})[Tt]([0-9]{2}):([0-9]{2}):([0-9]{2})(\.[0-9]+)?"
    r"([Zz]|[+-][0-9]{2}:[0-9]{2})"
)


def _rfc3339_ns(value):
    """Nanoseconds since the epoch of an RFC 3339 instant, or None — what
    chrono's `DateTime<Utc>` deserialises, compared as the Rust reader does
    (a later instant is greater, whatever its offset)."""
    if not isinstance(value, str):
        return None
    m = _RFC3339.fullmatch(value)
    if not m:
        return None
    year, month, day, hour, minute, second = (int(g) for g in m.groups()[:6])
    if not (1 <= month <= 12 and 1 <= day <= 31 and hour <= 23 and minute <= 59
            and second <= 60):
        return None
    frac = m.group(7) or "."
    nanos = int((frac[1:] + "000000000")[:9])
    tz = m.group(8)
    offset = 0
    if tz not in ("Z", "z"):
        offset = (1 if tz[0] == "+" else -1) * (int(tz[1:3]) * 3600 + int(tz[4:6]) * 60)
    seconds = calendar.timegm((year, month, day, hour, minute, second, 0, 0, 0)) - offset
    return seconds * 10**9 + nanos


def _rust_bool(b) -> str:
    """A `bool` as Rust's `{}` renders it."""
    return "true" if b else "false"


def _cp_pair(a, b) -> bool:
    """Arm CP-6's pair rule: recorded whole, never negative, never inverted."""
    if a is None and b is None:
        return True
    if a is not None and b is not None:
        return 0 <= a <= b
    return False


def _cp_shown(value) -> str:
    """An `Option<String>` as the Rust arms render it: `{:?}` or `absent`."""
    return "absent" if value is None else _rust_debug_str(value)


def _cp_place(topic, partition) -> str:
    """A position's place, `"topic":partition`, as arm CP-11 and the lines render it."""
    return f"{_rust_debug_str(topic)}:{partition}"


# PROD-03.0: the first minor of the BACKUP RECEIPT's format 1 that defines
# `schema_dependency` (arm 22) — `SCHEMA_DEPENDENCY_SINCE_MINOR` in
# `crates/logweir-core/src/backup_receipt.rs`, which it must equal
# (`docs/test_verify_scorecard.py::test_the_schema_dependency_constants_are_the_rust_readers`).
RECEIPT_SCHEMA_DEPENDENCY_SINCE_MINOR = 5

# PROD-03.0: the detection contract's constants, which arms 24, 27 and 28 read
# — `logweir_core::schema_dependency`'s `VERDICTS`, `NOT_ASSESSED_REASONS`,
# `BASES`, `MAX_SCHEMA_ID`, `SCHEMA_IDS_LISTED` and
# `DEPENDENT_SHARE_DENOMINATOR`, which they must equal.
SCHEMA_DEPENDENCY_VERDICTS = ("schemaDependent", "notDetected", "notAssessed")
SCHEMA_DEPENDENCY_REASONS = (
    "noRecords",
    "segmentUnreadable",
    "segmentTooLargeForDetection",
    "detectionTimeBudgetExceeded",
)
SCHEMA_DEPENDENCY_BASES = ("sampled", "complete")
SCHEMA_ID_MAX = 0x00FFFFFF
SCHEMA_IDS_LISTED = 16
DEPENDENT_SHARE_DENOMINATOR = 10


def _dependent_by_share(framed: int, unframed: int) -> bool:
    """`logweir_core::schema_dependency::dependent_by_share`: at least one
    framed record, and at least one in ten of the side's non-null records
    framed. Python's integers are exact, as the Rust side's `u128` is."""
    return framed >= 1 and framed * DEPENDENT_SHARE_DENOMINATOR >= framed + unframed


def _shown_or_absent(value) -> str:
    """An optional string as arms 24 and 26 render it: `absent`, or Rust's
    `{:?}` of it."""
    return "absent" if value is None else _rust_debug_str(value)


def _side_records_shown(side) -> str:
    """An optional side as arm 25 renders it: `absent`, or its judged count."""
    return "absent" if side is None else f"{_judged_records(side)} records"


def _u32_list_debug(ids) -> str:
    """A `Vec<u32>` as Rust's `{:?}` renders it: `[3, 4]`, `[]`."""
    return "[" + ", ".join(str(i) for i in ids) + "]"


def _judged_records(side) -> int:
    """`logweir_core::schema_dependency::judged_records`: framed, unframed and
    nulls together, exact."""
    return side["framed"] + side["unframed"] + side["nulls"]
# PROD-01.4a: the first minor of the BACKUP RECEIPT's format 1 that defines
# `generations` (arm 36) — `GENERATIONS_SINCE_MINOR` in
# `crates/logweir-core/src/backup_receipt.rs`, which it must equal
# (`docs/test_verify_scorecard.py::test_the_generations_minor_is_the_rust_readers`).
RECEIPT_GENERATIONS_SINCE_MINOR = 6

# PROD-01.4a: why a topic ID is null (arm 39) and where a recorded one came
# from (arm 40) — `TOPIC_ID_REASONS` and `TOPIC_ID_SOURCES` in
# `crates/logweir-core/src/topic_identity.rs`, in their order.
RECEIPT_TOPIC_ID_REASONS = (
    "noTopicId",
    "notAuthorized",
    "topicNotFound",
    "readFailed",
    "notRead",
    "reservedTopicId",
)
RECEIPT_TOPIC_ID_SOURCES = ("describeTopics", "engineManifest")

_TOPIC_ID_ALPHABET = frozenset(
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
)


def _is_canonical_topic_id(text) -> bool:
    """Kafka's text form of a real topic ID — the twin of
    `logweir_core::topic_identity::is_canonical`: 22 URL-safe base64
    characters, no padding, over 16 bytes that re-encode to the SAME text (no
    stray trailing bits), and never one of Kafka's reserved IDs: the all-zero ID
    ("no ID") or (0, 1), `AAAAAAAAAAAAAAAAAAAAAQ` (`ONE_UUID`,
    `METADATA_TOPIC_ID`), which `org.apache.kafka.common.Uuid` never gives a
    topic (PROD-01.4a review M1)."""
    if not isinstance(text, str) or len(text) != 22 or not set(text) <= _TOPIC_ID_ALPHABET:
        return False
    try:
        raw = base64.urlsafe_b64decode(text + "==")
    except (ValueError, TypeError):
        return False
    if len(raw) != 16 or raw in (bytes(16), bytes(15) + b"\x01"):
        return False
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii") == text

# PROD-05.1: `ConfigEntry::portability`'s closed set (arm 16), in
# `logweir_core::topic_configuration::PORTABILITY_CLASSES`'s order, which the
# `topic_configuration` lines also count in.
PORTABILITY_CLASSES = (
    "portable",
    "inherited",
    "removedInKafka4",
    "clusterBound",
    "requiresTieredStorage",
    "providerOnly",
    "secret",
)

# PROD-05.1: `BackupReceipt::owner_detection`'s closed set (arm 20), in
# `logweir_core::topic_configuration::OWNER_DETECTION_SOURCES`'s order — WHERE
# the run looked for declarative owners — and the source each owner basis
# needs (arm 21, `detection_for_basis`).
RECEIPT_OWNER_DETECTION_SOURCES = ("declared", "kafkaTopicResources")
RECEIPT_OWNER_DETECTION_FOR_BASIS = {
    "declared": "declared",
    "kafkaTopicResource": "kafkaTopicResources",
}

# `TopicConfigCoverage`/`ConfigEntry` source's closed set (arms 11 and 16).
RECEIPT_CONFIG_SOURCES = (
    "dynamicTopicConfig",
    "dynamicBrokerConfig",
    "dynamicDefaultBrokerConfig",
    "staticBrokerConfig",
    "defaultConfig",
    "unknown",
)

# The FIVE payload types Logweir signs. Keep byte-for-byte in step with
# `crates/logweir-verify/src/lib.rs`'s PAYLOAD_TYPE_SCORECARD,
# PAYLOAD_TYPE_BACKUP_RECEIPT, PAYLOAD_TYPE_PUT_RECEIPT, PAYLOAD_TYPE_TEARDOWN
# and PAYLOAD_TYPE_CATALOG_POINT; `docs/test_verify_scorecard.py::
# test_the_five_payload_types_match_the_rust_constants` fails if they ever
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
#
# `catalog-point` (PLAT-15.1, decision D3 §5.2) is the recovery catalog's point
# record. It is checked SIGNATURE-ONLY by both readers, deliberately: the
# record's receipt-derived facts are recomputed from the VERIFIED backup
# receipt it names (D3 §5.2 rule 3), so the receipt's signature — not this one
# — is the verification root, and an exit 0 here means "these bytes were signed
# by this key" and nothing about whether the point is still available. The
# script's fall-through arm at the bottom of `main` is what produces that
# verdict, and it says so in as many words.
PAYLOAD_TYPES = {
    "scorecard": PAYLOAD_TYPE,
    "backup-receipt": "application/vnd.logweir.backup-receipt+json;version=1.0.0",
    "catalog-point": "application/vnd.logweir.catalog-point+json;version=1.0.0",
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
    take the first field, parse it as Rust's `str::parse::<u64>` does. Anything
    else is "not a parseable semver", which is a refusal rather than an
    assumption.

    Rust's parse accepts an optional leading `+` and ASCII digits below 2**64,
    and nothing else. Until SCRIPT_VERSION 1.17.0 this was `int(head)`, which
    is wider on three points -- whitespace, `_` and unicode digits -- and the
    gap was a live two-reader split: `" 1.0.0"`, `"0_1.0.0"` and `"١.0.0"` were
    `drill verify` exit 4 ("not a parseable semver") and VALID here (measured at
    main b8b9263f, FX-3's class sweep: `_minor` below needs the same parse).
    """
    head = version.split(".")[0] if isinstance(version, str) else ""
    if not re.fullmatch(r"\+?[0-9]+", head):
        return None
    n = int(head)
    return n if n < 2 ** 64 else None


def _defines_format_1_minor(version, since_minor) -> bool:
    """Whether a document of `version` defines a field format 1 added at
    `since_minor` -- the twin of `logweir_core::scorecard::
    defines_format_1_minor` (PROD-11.1b): a 1.x document from that minor on,
    and every document of major 2, which is 1.7.0's fields plus the subset
    meaning. False for a version that does not parse or names another
    major."""
    major = _major(version)
    if major == 1:
        minor = _minor(version)
        return minor is not None and minor >= since_minor
    return major == SCORECARD_PARTITION_SUBSETS_MAJOR


def _defines_original_name(version) -> bool:
    """Whether a document of `version` defines `target.original_name` -- the
    twin of `logweir_core::scorecard::defines_original_name` (arm ON-1): a 1.x
    document from 1.8.0 on, and NO document of another major. Unlike
    `_defines_format_1_minor`, major 2 does not define it: a 2.x document is a
    partition-subset restore's, and a restore under the original topic names
    restores whole topics (arm ON-14)."""
    minor = _minor(version)
    return (
        _major(version) == 1
        and minor is not None
        and minor >= SCORECARD_ORIGINAL_NAME_SINCE_MINOR
    )


def _defines_console_approval(version) -> bool:
    """Whether a document of `version` defines `approval.console` -- the twin
    of `logweir_core::scorecard::defines_console_approval` (arm CA-1): a 1.x
    document from 1.9.0 on, or a 2.x document from 2.1.0 on. A partition-subset
    restore may be approved in the console like any other, so the block is
    defined in both lines."""
    minor = _minor(version)
    if minor is None:
        return False
    major = _major(version)
    if major == 1:
        return minor >= SCORECARD_CONSOLE_APPROVAL_SINCE_MINOR
    if major == SCORECARD_PARTITION_SUBSETS_MAJOR:
        return minor >= SCORECARD_CONSOLE_APPROVAL_SINCE_MINOR_OF_MAJOR_2
    return False


def _days_from_civil(year: int, month: int, day: int) -> int:
    """Days since 1970-01-01 of a proleptic Gregorian date (any year)."""
    year -= month <= 2
    era = year // 400
    yoe = year - era * 400
    doy = (153 * (month + (-3 if month > 2 else 9)) + 2) // 5 + day - 1
    doe = yoe * 365 + yoe // 4 - yoe // 100 + doy
    return era * 146097 + doe - 719468


def _civil_from_days(days: int):
    """The inverse of `_days_from_civil`: (year, month, day)."""
    days += 719468
    era = days // 146097
    doe = days - era * 146097
    yoe = (doe - doe // 1460 + doe // 36524 - doe // 146096) // 365
    year = yoe + era * 400
    doy = doe - (365 * yoe + yoe // 4 - yoe // 100)
    mp = (5 * doy + 2) // 153
    day = doy - (153 * mp + 2) // 5 + 1
    month = mp + (3 if mp < 10 else -9)
    return year + (month <= 2), month, day


def _instant(value):
    """An RFC 3339 instant as `(seconds, fraction)`, or None -- what chrono's
    `DateTime<Utc>` holds, so two of them compare as the Rust reader compares
    them (arms CA-4 and CA-7): `seconds` since the epoch in UTC, whatever
    offset the text carries, and `fraction` in nanoseconds. A leap second
    (`:60`) is second 59 with a fraction of a whole second or more, exactly as
    chrono keeps it, so it sorts after `:59.999` and before the next minute.
    A date that does not exist (the 30th of February) is None: serde refuses it
    over there."""
    if not isinstance(value, str):
        return None
    m = _RFC3339.fullmatch(value)
    if not m:
        return None
    year, month, day, hour, minute, second = (int(g) for g in m.groups()[:6])
    if not (1 <= month <= 12 and hour <= 23 and minute <= 59 and second <= 60):
        return None
    leap_year = year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)
    days_in_month = (31, 29 if leap_year else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31)
    if not 1 <= day <= days_in_month[month - 1]:
        return None
    frac = m.group(7) or "."
    nanos = int((frac[1:] + "000000000")[:9])
    tz = m.group(8)
    offset = 0
    if tz not in ("Z", "z"):
        offset = (1 if tz[0] == "+" else -1) * (int(tz[1:3]) * 3600 + int(tz[4:6]) * 60)
    seconds = (
        _days_from_civil(year, month, day) * 86400
        + hour * 3600 + minute * 60 + min(second, 59) - offset
    )
    return seconds, nanos + (10**9 if second == 60 else 0)


def _instant_shown(instant) -> str:
    """An instant as the Rust reader prints it:
    `to_rfc3339_opts(SecondsFormat::AutoSi, true)` -- UTC with `Z`, and the
    fraction to the precision it carries (none, 3, 6 or 9 digits)."""
    seconds, fraction = instant
    leap = fraction >= 10**9
    if leap:
        fraction -= 10**9
    days, rest = divmod(seconds, 86400)
    year, month, day = _civil_from_days(days)
    hour, rest = divmod(rest, 3600)
    minute, second = divmod(rest, 60)
    if fraction == 0:
        shown = ""
    elif fraction % 10**6 == 0:
        shown = f".{fraction // 10**6:03d}"
    elif fraction % 10**3 == 0:
        shown = f".{fraction // 10**3:06d}"
    else:
        shown = f".{fraction:09d}"
    return (
        f"{year:04d}-{month:02d}-{day:02d}T{hour:02d}:{minute:02d}:"
        f"{second + (1 if leap else 0):02d}{shown}Z"
    )


def _visible_ascii(text) -> bool:
    """Non-empty, and every character visible ASCII (0x21 to 0x7E): no space,
    no control character, nothing outside ASCII -- the twin of
    `approval_policy::is_visible_ascii`."""
    return bool(text) and all(0x21 <= ord(c) <= 0x7E for c in text)


def _ascii_lower(text: str) -> str:
    """Rust's `to_ascii_lowercase`: A to Z only. Python's `str.lower` also
    folds letters outside ASCII, which the form check has already refused; this
    folds what the Rust reader folds and nothing else."""
    return "".join(chr(ord(c) + 32) if "A" <= c <= "Z" else c for c in text)


def _fold_issuer(issuer: str) -> str:
    """`approval_policy::fold_issuer`: trailing `/` removed, ASCII lower case."""
    return _ascii_lower(issuer.rstrip("/"))


def _principal_fault(party, issuer, subject):
    """`approval_policy::principal_fault`'s fixed clause for one principal, or
    None: a comparable form, then not the local administrator, then not a
    Kubernetes system identity."""
    if (
        not _visible_ascii(issuer)
        or not _visible_ascii(subject)
        or "#" in issuer
        or len(issuer) > CONSOLE_MAX_COMPARABLE_LEN
        or len(subject) > CONSOLE_MAX_COMPARABLE_LEN
    ):
        return (
            f"the {party} is not in a form that can be compared: a console approval compares "
            "an issuer and a subject made of visible ASCII characters only (no space, no "
            "control character, nothing outside ASCII; the issuer without `#`; each at most "
            "255 characters, as OpenID Connect bounds `sub`), and an identity it cannot "
            "compare cannot be shown to be a second person"
        )
    if _fold_issuer(issuer) == CONSOLE_LOCAL_ADMIN_ISSUER:
        return (
            f"the {party} is the in-cluster administrator console's one identity, which "
            "cannot be one of two people; a console approval needs two people the same "
            "identity provider vouches for"
        )
    if _ascii_lower(subject).startswith(CONSOLE_SYSTEM_SUBJECT_PREFIX):
        return (
            f"the {party} is a Kubernetes system identity (a service account, a node or a "
            "system user), not a person; a console approval is between two people"
        )
    return None


def _console_separation_words(requester, approver):
    """Why the two principals of `approval.console` are not two people, in
    arm CA-3's fixed words, or None when they are -- the twin of
    `approval_policy::separation_fault` and `scorecard::
    console_separation_words`: each a principal a console approval can compare
    and a person (the requester first), then one issuer, then two subjects.
    It compares the issuer and the subject and nothing else. No part of what it
    returns is the document's."""
    fault = _principal_fault("requester", requester["issuer"], requester["subject"])
    if fault is None:
        fault = _principal_fault("approver", approver["issuer"], approver["subject"])
    if fault is not None:
        return fault
    if _fold_issuer(requester["issuer"]) != _fold_issuer(approver["issuer"]):
        return (
            "the approver and the requester come from two issuers; whether a subject of one "
            "is a subject of the other cannot be known, so a principal of another issuer is "
            "never a second person"
        )
    if _ascii_lower(requester["subject"]) == _ascii_lower(approver["subject"]):
        return (
            "the approver is the requester (the issuer and the subject are compared without "
            "case); a two-person approval needs a second person, and no role changes that"
        )
    return None


def _narrows_partitions(block) -> bool:
    """`SelectionLabel::narrows_partitions`: the block names a partition
    subset (a non-empty `partitions` list). The shape layer has proved the
    block's shape before any caller reads it."""
    return isinstance(block, dict) and bool(block.get("partitions"))


def _minor(version: str):
    """Second integer of a dotted version string, or None if there is none.

    Mirrors `logweir_core::scorecard::minor_version`: split on ".", take the
    second field, parse it as Rust's `str::parse::<u64>` does -- an optional
    leading `+` and ASCII digits, below 2**64, and nothing else. Python's
    `int()` is wider (whitespace, `_`, unicode digits), and a wider parse here
    would let `"1. 2.0"` define a field the Rust reader says it predates
    (`_receipt_parse_semver` makes the same choice). Read by arms NR-1 (FX-3)
    and TB-1 (FX-8).
    """
    parts = version.split(".") if isinstance(version, str) else []
    if len(parts) < 2 or not re.fullmatch(r"\+?[0-9]+", parts[1]):
        return None
    n = int(parts[1])
    return n if n < 2 ** 64 else None


# RULING R-A'S "BLANK", EXACTLY AS THE RUST READER SPELLS IT (PROD-08.1 review
# L-1). Every blank test there is `str::trim().is_empty()`, and `trim` strips
# `char::is_whitespace` — the Unicode White_Space property, these 25 code
# points. Python's argument-less `str.strip()` also strips U+001C..U+001F (the
# file, group, record and unit separators, which `str.isspace` counts), so on a
# reason, key or mode made of those the two readers disagreed in both
# directions: `incomplete_reason: "\x1f"` beside `covered: false` was VALID from
# `drill verify` and refused here, and beside `covered: true` the reverse. The
# writer never produces either; the script mirrors the reference reader, so
# every blank test here strips this set and nothing else.
RUST_WHITESPACE = (
    "\t\n\x0b\x0c\r \x85\xa0\u1680"
    "\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200a"
    "\u2028\u2029\u202f\u205f\u3000"
)


def _time_basis_shape_ok(block) -> bool:
    """`source.time_basis` has the shape `TimeBasisLabel` deserialises: an
    object whose `plan` is absent, null or a string and whose `producer_time`
    and `not_recorded` are both PRESENT arrays of strings. Unknown keys are
    ignored, as serde ignores them."""
    if not isinstance(block, dict):
        return False
    plan = block.get("plan")
    if plan is not None and not isinstance(plan, str):
        return False
    for name in ("producer_time", "not_recorded"):
        listed = block.get(name)
        if not isinstance(listed, list) or not all(isinstance(t, str) for t in listed):
            return False
    return True


def _selection_shape_ok(block) -> bool:
    """`source.selection` has the shape `SelectionLabel` deserialises
    (PROD-11.1, format 1.7.0; PROD-11.1b, format 2.0.0): an object whose
    `window_end_ms` is an i64, whose `window_start_ms` is absent, null or an
    i64, whose `partitions` is absent, null or an array of `{topic: string,
    partitions: [i32]}`, and whose `engine_runs` is absent, null or a u32.
    Unknown keys are ignored, as serde ignores them. Which of the optional
    fields a document of each major must carry is an arm's (PS-1, PS-2)."""
    if not isinstance(block, dict) or not _int_in(block.get("window_end_ms"), 64):
        return False
    start = block.get("window_start_ms")
    if start is not None and not _int_in(start, 64):
        return False
    subsets = block.get("partitions")
    if subsets is not None:
        if not isinstance(subsets, list):
            return False
        for entry in subsets:
            if not isinstance(entry, dict) or not isinstance(entry.get("topic"), str):
                return False
            listed = entry.get("partitions")
            if not isinstance(listed, list) or not all(_int_in(p, 32) for p in listed):
                return False
    runs = block.get("engine_runs")
    if runs is not None and not (
        isinstance(runs, int) and not isinstance(runs, bool) and 0 <= runs < 2 ** 32
    ):
        return False
    return True


def _original_name_shape_ok(block) -> bool:
    """`target.original_name` has the shape `OriginalNameInfo` deserialises
    (PROD-15.1, format 1.8.0): strings `approval_subject`, `approval_mode` and
    `cluster_condition`; `source_cluster_id` absent, null or a string;
    `owner_detection` an array of strings; `owners` an array of objects of four
    strings `topic`, `kind`, `reference`, `found_in`; a bool `owner_path`; and
    `confirmation` and `kafka_topic_resources_sha256` absent, null or strings
    (OD-10 and review L2). Unknown keys are ignored, as serde ignores them."""
    if not isinstance(block, dict):
        return False
    for name in ("approval_subject", "approval_mode", "cluster_condition"):
        if not isinstance(block.get(name), str):
            return False
    source = block.get("source_cluster_id")
    if source is not None and not isinstance(source, str):
        return False
    if not _strings(block.get("owner_detection")):
        return False
    owners = block.get("owners")
    if not isinstance(owners, list):
        return False
    for owner in owners:
        if not isinstance(owner, dict) or not all(
            isinstance(owner.get(k), str) for k in ("topic", "kind", "reference", "found_in")
        ):
            return False
    for name in ("confirmation", "kafka_topic_resources_sha256"):
        value = block.get(name)
        if value is not None and not isinstance(value, str):
            return False
    return isinstance(block.get("owner_path"), bool)


def _console_approval_shape_ok(block) -> bool:
    """`approval.console` has the shape `ConsoleApprovalInfo` deserialises
    (PROD-16.2, format 1.9.0 and 2.1.0): strings `mode` and
    `confirmation_key_id`; `requester` and `approver` each an object of two
    strings, `issuer` and `subject`; and three RFC 3339 instants,
    `requested_at`, `approved_at` and `request_expires_at`. Unknown keys are
    ignored, as serde ignores them."""
    if not isinstance(block, dict):
        return False
    for name in ("mode", "confirmation_key_id"):
        if not isinstance(block.get(name), str):
            return False
    for name in ("requester", "approver"):
        principal = block.get(name)
        if not isinstance(principal, dict) or not all(
            isinstance(principal.get(k), str) for k in ("issuer", "subject")
        ):
            return False
    return all(
        _instant(block.get(name)) is not None
        for name in ("requested_at", "approved_at", "request_expires_at")
    )


def _is_sha256_prefixed(value) -> bool:
    """`sha256:` and 64 lowercase hex characters -- the twin of
    `logweir_core::check_contract::is_sha256_prefixed`."""
    if not isinstance(value, str) or not value.startswith("sha256:"):
        return False
    digest = value[len("sha256:"):]
    return len(digest) == 64 and all(c in "0123456789abcdef" for c in digest)


def _int_in(x, bits) -> bool:
    """`x` is a JSON integer in the signed `bits`-bit range (Rust's `i32` or
    `i64`), never a bool."""
    return isinstance(x, int) and not isinstance(x, bool) and -(2 ** (bits - 1)) <= x < 2 ** (bits - 1)


def _strings(x) -> bool:
    return isinstance(x, list) and all(isinstance(t, str) for t in x)


def _verification_shape_ok(block) -> bool:
    """`integrity.verification` has the shape `Verification` deserialises
    (PROD-08.1, format 1.4.0): four strings; `gaps` and `pruned`, arrays of
    `{topic: string, partition: i32, from_offset: i64, to_offset: i64}`; and
    `complete` absent, null, or an object with a bool `covered`, an optional
    string `incomplete_reason`, a `window` of an optional `start_ms` and an
    `end_ms` (i64), and `archive`, `replay` and `partitions` objects and
    arrays in the writer's shape.

    The `u64` counts are NOT checked here: `U64_FIELDS` names every one, and
    the domain loop refuses a null, absent, non-integer or out-of-range count
    in the words it uses for every other u64 of the document. Only their
    CONTAINERS are asserted, so that loop can read through them. Unknown keys
    are ignored, as serde ignores them."""
    if not isinstance(block, dict):
        return False
    for name in ("coverage", "comparison_basis", "header_order", "application"):
        if not isinstance(block.get(name), str):
            return False
    for name in ("gaps", "pruned"):
        ranges = block.get(name)
        if not isinstance(ranges, list):
            return False
        for r in ranges:
            if not (
                isinstance(r, dict)
                and isinstance(r.get("topic"), str)
                and _int_in(r.get("partition"), 32)
                and _int_in(r.get("from_offset"), 64)
                and _int_in(r.get("to_offset"), 64)
            ):
                return False
    complete = block.get("complete")
    if complete is None:
        return True
    if not isinstance(complete, dict) or not isinstance(complete.get("covered"), bool):
        return False
    reason = complete.get("incomplete_reason")
    if reason is not None and not isinstance(reason, str):
        return False
    window = complete.get("window")
    if not isinstance(window, dict) or not _int_in(window.get("end_ms"), 64):
        return False
    if window.get("start_ms") is not None and not _int_in(window.get("start_ms"), 64):
        return False
    archive = complete.get("archive")
    if not (
        isinstance(archive, dict)
        and _strings(archive.get("segments_failed"))
        and _strings(archive.get("segments_unverified"))
    ):
        return False
    if not isinstance(complete.get("replay"), dict):
        return False
    partitions = complete.get("partitions")
    if not isinstance(partitions, list):
        return False
    for p in partitions:
        if not (
            isinstance(p, dict)
            and isinstance(p.get("topic"), str)
            and _int_in(p.get("partition"), 32)
            and isinstance(p.get("target_topic"), str)
            and isinstance(p.get("compared"), bool)
            and isinstance(p.get("replay"), dict)
            and _strings(p.get("findings"))
        ):
            return False
    return True


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
    ("integrity.verification.complete.max_records", True),
    ("integrity.verification.complete.archive.segments", False),
    ("integrity.verification.complete.archive.segments_verified", False),
    ("integrity.verification.complete.archive.records_decoded", False),
    ("integrity.verification.complete.archive.offset_holes", False),
    ("integrity.verification.complete.replay.expected", False),
    ("integrity.verification.complete.replay.restored", False),
    ("integrity.verification.complete.replay.matching", False),
    ("integrity.verification.complete.replay.missing", False),
    ("integrity.verification.complete.replay.unexpected", False),
    ("integrity.verification.complete.replay.duplicates", False),
    ("integrity.verification.complete.replay.out_of_order", False),
    ("integrity.verification.complete.replay.mismatched", False),
    ("integrity.verification.complete.partitions[].segments", False),
    ("integrity.verification.complete.partitions[].segments_verified", False),
    ("integrity.verification.complete.partitions[].records_decoded", False),
    ("integrity.verification.complete.partitions[].offset_holes", False),
    ("integrity.verification.complete.partitions[].replay.expected", False),
    ("integrity.verification.complete.partitions[].replay.restored", False),
    ("integrity.verification.complete.partitions[].replay.matching", False),
    ("integrity.verification.complete.partitions[].replay.missing", False),
    ("integrity.verification.complete.partitions[].replay.unexpected", False),
    ("integrity.verification.complete.partitions[].replay.duplicates", False),
    ("integrity.verification.complete.partitions[].replay.out_of_order", False),
    ("integrity.verification.complete.partitions[].replay.mismatched", False),
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
        # PROD-08.1 (1.19.0): a u64 NESTED below its block, at any depth —
        # `integrity.verification.complete.archive.segments`, and through a
        # list, `integrity.verification.complete.partitions[].replay.expected`.
        # Each step is an OPTIONAL struct, a REQUIRED one inside an optional
        # one (whose presence the block's own shape check has already
        # asserted), or a `Vec` of structs; anything that is not the expected
        # container is skipped, never traced back.
        for name, value in _nested(owner, key.split("."), f"{block}."):
            yield name, value, optional


def _nested(owner, steps, shown):
    """(dotted name, value) for `steps` below `owner`, expanding a `name[]`
    step over every element of the list it names (the element's index
    replaces `[]` in the name, as `phases[0].duration_ms` does)."""
    head, rest = steps[0], steps[1:]
    if not rest:
        yield f"{shown}{head}", owner.get(head)
        return
    if head.endswith("[]"):
        items = owner.get(head[:-2])
        if isinstance(items, list):
            for n, item in enumerate(items):
                if isinstance(item, dict):
                    yield from _nested(item, rest, f"{shown}{head[:-2]}[{n}].")
        return
    child = owner.get(head)
    if isinstance(child, dict):
        yield from _nested(child, rest, f"{shown}{head}.")


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
    # PROD-11.1b: this script reads major 2, the partition-subset format, too
    # (and only for that shape: arm PS-1, after the shape layer below).
    if doc_major > SCORECARD_PARTITION_SUBSETS_MAJOR:
        return (
            f"format_version {version} has a major version newer than this reader "
            f"understands (this script knows {SCORECARD_PARTITION_SUBSETS_VERSION})"
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
    approval = doc["approval"]
    measured = doc["measured"]
    objectives = doc["objectives"]
    sample = doc["sample"]
    integrity = doc["integrity"]
    evidence = doc["evidence"]

    # Also shape (FX-8, scorecard 1.3.0): `source.time_basis` is an
    # `Option<TimeBasisLabel>` over there, so `null` is ABSENT and anything
    # that is not an object of an optional string `plan` and two REQUIRED
    # arrays of strings is refused at DESERIALISATION. First of the nested
    # shape checks, because serde meets `source` before `sample` and
    # `target_diff`. Every bad shape is a case in `shape-index.json`.
    time_basis = source.get("time_basis")
    if time_basis is not None and not _time_basis_shape_ok(time_basis):
        return (
            "source.time_basis is not an object of an optional string plan and two arrays "
            "of strings, producer_time and not_recorded"
        )

    # Also shape (PROD-11.1, scorecard 1.7.0): `source.selection` is an
    # `Option<SelectionLabel>` over there, so `null` is ABSENT and anything
    # that is not the writer's shape is refused at DESERIALISATION. Arms SEL-1
    # to SEL-7 below compare its fields, so the shape is asserted first. The
    # bad shapes are cases in `shape-index.json`.
    selection = source.get("selection")
    if selection is not None and not _selection_shape_ok(selection):
        return (
            "source.selection is not an object of the shape the writer gives it: a window end "
            "and an optional window start, both integers, an optional array of topic partition "
            "lists and an optional engine-run count"
        )

    # Also shape (PROD-15.1, scorecard 1.8.0): `target.original_name` is an
    # `Option<OriginalNameInfo>` over there, so `null` is ABSENT and anything
    # that is not the writer's shape is refused at DESERIALISATION. Arms ON-1
    # to ON-14 below compare its fields, so the shape is asserted first. The
    # bad shapes are cases in `shape-index.json`.
    original_name = target.get("original_name")
    if original_name is not None and not _original_name_shape_ok(original_name):
        return (
            "target.original_name is not an object of the shape the writer gives it: three "
            "strings, an optional source cluster id, the places looked in, the owners found and "
            "a bool owner_path"
        )

    # Also shape (PROD-16.2, scorecard 1.9.0 and 2.1.0): `approval.console` is
    # an `Option<ConsoleApprovalInfo>` over there, so `null` is ABSENT and
    # anything that is not the writer's shape is refused at DESERIALISATION.
    # Arms CA-1 to CA-8 below compare its fields, and CA-5 to CA-7 compare the
    # three fields of `approval` beside it, which the Rust reader's type has
    # always required (a string, a string and an instant); so both shapes are
    # asserted first, and only on a document carrying the block -- every
    # other document is decided exactly as before. After `target`'s, because
    # serde meets `approval` after `target`. The bad shapes are cases in
    # `shape-index.json`.
    console = approval.get("console")
    if console is not None:
        if not _console_approval_shape_ok(console):
            return (
                "approval.console is not an object of the shape the writer gives it: a string "
                "mode, a requester and an approver of a string issuer and a string subject "
                "each, three instants and a string confirmation key id"
            )
        if (
            not isinstance(approval.get("approver"), str)
            or not isinstance(approval.get("key_id"), str)
            or _instant(approval.get("approved_at")) is None
        ):
            return (
                "approval.console is present but approval.approver, approval.key_id and "
                "approval.approved_at are not two strings and an instant"
            )

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

    # Also shape (FX-23, scorecard 1.6.0): `sample.unsampled_topics` is an
    # `Option<Vec<String>>` over there, so `null` is ABSENT and anything that
    # is not an array of strings is refused at DESERIALISATION. Arms US-1 to
    # US-3 below compare its items, and `<` over mixed types raises here, so
    # the shape is asserted first. The bad shape is a case in
    # `shape-index.json`.
    unsampled = sample.get("unsampled_topics")
    if unsampled is not None and (
        not isinstance(unsampled, list) or not all(isinstance(t, str) for t in unsampled)
    ):
        return "sample.unsampled_topics is not an array of strings"

    # Also shape (FX-4 fix round, scorecard 1.1.0): `target_diff.not_assessed`
    # is the same `Option<Vec<String>>` as `topic_parity.not_assessed` below —
    # phase 3's collisions whose configuration difference was not assessed,
    # which FX-4 records in this new field instead of changing the existing
    # collision strings. Checked first because serde meets `target_diff`
    # first. Both bad shapes are cases in `shape-index.json`.
    target_not_assessed = doc["target_diff"].get("not_assessed")
    if target_not_assessed is not None and (
        not isinstance(target_not_assessed, list)
        or not all(isinstance(t, str) for t in target_not_assessed)
    ):
        return "target_diff.not_assessed is not an array of strings"

    # Also shape (FX-4, scorecard 1.1.0): `topic_parity.not_assessed` is an
    # `Option<Vec<String>>` over there, so `null` is ABSENT and anything that
    # is not an array of strings is refused at DESERIALISATION. Without this a
    # document carrying `"not_assessed": "x"` or `[1]` printed VALID here while
    # `drill verify` exited 1 — measured on this branch before this check
    # existed, not argued. Both are cases in `shape-index.json`.
    # Also shape (FX-3): `topic_parity.intentionally_deviated` and
    # `unexpected_divergence` are REQUIRED `Vec<String>`s over there, so a
    # missing one, `"x"` or `[1]` is refused at DESERIALISATION, and until
    # 1.17.0 this script printed VALID for all four (measured at main b8b9263f).
    # Arms NR-2 to NR-5 below read both lists, and `in` over a string would be
    # a SUBSTRING test here, so the shape is asserted first. In the struct's
    # declaration order, before `not_assessed`.
    for name in ("intentionally_deviated", "unexpected_divergence"):
        listed = doc["topic_parity"].get(name)
        if not isinstance(listed, list) or not all(isinstance(t, str) for t in listed):
            return f"topic_parity.{name} is not an array of strings"

    not_assessed = doc["topic_parity"].get("not_assessed")
    if not_assessed is not None and (
        not isinstance(not_assessed, list)
        or not all(isinstance(t, str) for t in not_assessed)
    ):
        return "topic_parity.not_assessed is not an array of strings"

    # Also shape (FX-3, scorecard 1.2.0): `topic_parity.not_reconstructed` is an
    # `Option<Vec<String>>` over there, like `not_assessed` above. `null` is
    # ABSENT. Both bad shapes are cases in `shape-index.json`.
    not_reconstructed = doc["topic_parity"].get("not_reconstructed")
    if not_reconstructed is not None and (
        not isinstance(not_reconstructed, list)
        or not all(isinstance(t, str) for t in not_reconstructed)
    ):
        return "topic_parity.not_reconstructed is not an array of strings"

    # Also shape (PROD-08.1, scorecard 1.4.0): `integrity.verification` is an
    # `Option<Verification>` over there, so `null` is ABSENT and a block whose
    # containers, strings, bools or signed integers are not the writer's is
    # refused at DESERIALISATION. Before the u64 loop below, which reads the
    # block's counts through those containers. Every bad shape is a case in
    # `shape-index.json`.
    verification = integrity.get("verification")
    if verification is not None and not _verification_shape_ok(verification):
        return (
            "integrity.verification is not an object of the shape the writer gives it: "
            "four strings, two arrays of offset ranges and an optional complete block"
        )

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
    # is refused. Scoped to the majors this script reads (1, and 2 since
    # PROD-11.1b, which is 1.7.0's fields), so a future major may redefine the
    # block.
    #
    # PS-1 first (PROD-11.1b), mirrored from `Scorecard::refuse_unreadable_major`,
    # which the Rust reader runs before every arm: major 2 is the format of a
    # partition-subset restore and nothing else. HERE, after the shape layer,
    # because the Rust reader reaches it only after `serde_json` has read the
    # document; the same words.
    if doc_major == SCORECARD_PARTITION_SUBSETS_MAJOR and not _narrows_partitions(selection):
        return (
            f"format_version {version} is the format of a partition-subset restore, and this "
            "document carries no source.selection.partitions; a reader reads major "
            f"{SCORECARD_PARTITION_SUBSETS_MAJOR} only for that shape"
        )
    if doc_major in (1, SCORECARD_PARTITION_SUBSETS_MAJOR):
        if evidence.get("version_id") is not None:
            return "evidence.version_id is set but the four post-put fields are zeroed before signing"
        if evidence.get("retain_until") is not None:
            return "evidence.retain_until is set but the four post-put fields are zeroed before signing"
        if evidence.get("immutable"):
            return "evidence.immutable is true but the four post-put fields are zeroed before signing"
        if evidence.get("create_only_enforced"):
            return "evidence.create_only_enforced is true but the four post-put fields are zeroed before signing"
        # `evidence.offset_report_*` (Task 9b — Global Constraint 12's price
        # for two nested optional fields), mirrored ARM FOR ARM and IN THIS
        # POSITION from `Scorecard::validate_invariants`, inside the same
        # `major == 1` scope, with the same words.
        #
        # ABSENT IS LEGAL and means this run recorded no offset-mapping report.
        # BOTH DIRECTIONS in ONE arm, because the incoherence is symmetric.
        #
        # BLANK COUNTS AS ABSENT (ruling R-A): `.strip()` here,
        # `trim().is_empty()` there. Without it the two readers would disagree
        # on `""` — Rust's `is_some()` is true for `Some("")` while Python's
        # truthiness is false — which is the class of split the parity gate
        # exists to catch, and the one T0-6 actually found.
        offset_key_named = bool(str(evidence.get("offset_report_key") or "").strip(RUST_WHITESPACE))
        offset_digest_named = bool(str(evidence.get("offset_report_sha256") or "").strip(RUST_WHITESPACE))
        if offset_key_named != offset_digest_named:
            return (
                "evidence.offset_report_key and evidence.offset_report_sha256 are present or "
                "absent together; a key with no digest names bytes nothing binds, and a digest "
                "with no key binds bytes nobody can fetch"
            )
        # `target.marker_topic` / `target.mode` (SCRIPT_VERSION 1.12.0, review
        # F1), mirrored ARM FOR ARM and IN THIS POSITION from
        # `Scorecard::validate_invariants` — last inside the same `major == 1`
        # scope, with the same words.
        #
        # ONE DIRECTION: an absent marker requires `newTopic`. A `newTopic`
        # document that DOES name a marker topic is accepted by both readers,
        # because what this tree writes is narrower than what its readers
        # accept and a reader must not refuse a document a future writer could
        # legitimately produce.
        #
        # BLANK COUNTS AS ABSENT (ruling R-A): `.strip()` here,
        # `trim().is_empty()` there.
        #
        # THE VALUE SET IS CLOSED (SCRIPT_VERSION 1.13.0, Task 9b re-review
        # NIT-1), and this arm comes FIRST because it decides what the arm
        # below is allowed to read as "scratch".
        #
        # ABSENT IS SCRATCH: `#[serde(default)]` plus `skip_serializing_if =
        # "TargetMode::is_scratch"` there, `"mode" not in target` here, and
        # every scorecard this tree has ever written omits the key. PRESENT is
        # a closed set of exactly `TargetMode`'s two serde tags.
        #
        # Until 1.13.0 this file did not check the value at all, and the
        # comment that stood here cited `shape-index.json` as the precedent for
        # leaving it unchecked. THAT WAS A MISREADING OF THE PRECEDENT, and it
        # is worth stating plainly because it is the failure mode this whole
        # directory exists to catch. Every one of `shape-index.json`'s entries
        # records a document BOTH readers REFUSE — `rust_exit` and
        # `python_exit` are both refusals in all of them — with each reader's
        # own text recorded separately because Rust's half is `serde_json`'s
        # message and this repository's half is its own wording. What that
        # index licenses is two DIFFERENT SENTENCES for the same verdict. It
        # has never licensed one reader printing VALID over a document the
        # other refuses, and `two_reader_parity_on_documents_refused_before_
        # the_invariants` fails the moment an entry tries.
        #
        # The two texts are therefore not byte-identical, deliberately: Rust's
        # are `unknown variant `bogus`, expected `scratch` or `newTopic`` and
        # `expected value` — the second of which carries no information a
        # second reader could honestly restate, and both of which are
        # `serde_json`'s vocabulary rather than this format's. Reproducing them
        # here would bind this file's output to a dependency's internal wording
        # and would say "I could not parse this" about a document this reader
        # parsed perfectly well. The VERDICT is what parity is about, and the
        # verdict now agrees.
        if "mode" in target and target["mode"] not in ("scratch", "newTopic"):
            return (
                "target.mode is not one of the two values this format defines; it "
                "is \"scratch\" or \"newTopic\" and nothing else"
            )

        # ABSENT-OR-`"scratch"` IS SCRATCH, which is exactly Rust's
        # `#[serde(default)]` plus `TargetMode::is_scratch` for every document
        # the Rust reader can read. By the time this runs, a PRESENT `mode` is
        # one of the two spellings and `mode is None` means the key was absent.
        marker_named = bool(str(target.get("marker_topic") or "").strip(RUST_WHITESPACE))
        mode = target.get("mode")
        if not marker_named and (mode is None or mode == "scratch"):
            return (
                "target.marker_topic is absent but target.mode is scratch; the marker topic "
                "is the segregation proof phase 0 verified, and a scratch document that omits "
                "it claims a check nothing recorded"
            )

    # T0-6 / ruling R-A: `.strip()` on both sides. Python truthiness already
    # refused `""` while the Rust arm's `.is_none()` accepted AND SIGNED it —
    # a live disagreement in the file whose docstring above says a
    # disagreement is impossible. Whitespace-only went the other way: `"   "`
    # is truthy here, so both readers accepted it. The Rust arm is now
    # `partial_reason.as_deref().unwrap_or("").trim().is_empty()` — the same
    # predicate, the same message, the same position.
    if integrity.get("result") == "partial" and not str(
        integrity.get("partial_reason") or ""
    ).strip(RUST_WHITESPACE):
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
    if outcome == "pass" and str(integrity.get("partial_reason") or "").strip(RUST_WHITESPACE):
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
        mode_blank = not str(target_auth.get("mode") or "").strip(RUST_WHITESPACE)
        username_named = bool(str(target_auth.get("username") or "").strip(RUST_WHITESPACE))
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
        #
        # PROD-01.3 (scorecard format 1.5.0) splits the arm by version, in the
        # Rust reader's order: a PROD-01.3 mode under a version before 1.5.0
        # (the message names the version, never the mode), the closed five from
        # 1.5.0, and the unchanged closed two below it.
        target_mode = str(target_auth.get("mode"))
        version = doc.get("format_version")
        five_defined = _defines_format_1_minor(version, SCORECARD_AUTH_MODES_SINCE_MINOR)
        if target_mode in PROD_01_3_AUTH_MODES:
            if not five_defined:
                return (
                    "target.auth.mode is a value defined from "
                    f"1.{SCORECARD_AUTH_MODES_SINCE_MINOR}.0 and format_version "
                    f"{_rust_debug_str(version)} predates it"
                )
        elif target_mode not in ORIGINAL_AUTH_MODES:
            if five_defined:
                return (
                    "target.auth.mode is not one of the five values this format defines; it "
                    "is \"plaintext\", \"scramSha512\", \"scramSha256\", \"plain\" or "
                    "\"mtls\" and nothing else"
                )
            return (
                "target.auth.mode is not one of the two values this format defines; it "
                "is \"plaintext\" or \"scramSha512\" and nothing else"
            )

    # `topic_parity.not_reconstructed` (format 1.2.0, FX-3): arms NR-1 to
    # NR-5, mirrored ARM FOR ARM, IN THIS POSITION (after `target.auth`,
    # before `redactions`) and with the same words from
    # `Scorecard::validate_invariants`. They fire ONLY on a document carrying
    # the block, so every 1.0.0 and 1.1.0 document -- and a 1.2.0 one whose
    # phase 7 never ran -- is decided exactly as before. Not interpolated except
    # NR-1's version: an entry names a topic. The shape layer above has proved
    # both lists and the block are arrays of strings.
    if not_reconstructed is not None:
        # NR-1. A version before 1.2.0 cannot carry the 1.2.0 field.
        if not _defines_format_1_minor(version, TOPIC_PARITY_NOT_RECONSTRUCTED_SINCE_MINOR):
            return (
                f"topic_parity.not_reconstructed is present but format_version "
                f"{_rust_debug_str(version)} predates it: the field is defined from "
                f"1.{TOPIC_PARITY_NOT_RECONSTRUCTED_SINCE_MINOR}.0"
            )
        parity_block = doc["topic_parity"]
        # NR-2. THE FAIL-SAFE TWIN an older reader sees.
        if any(e not in parity_block["unexpected_divergence"] for e in not_reconstructed):
            return (
                "topic_parity.not_reconstructed names a deviation that unexpected_divergence "
                "does not; a source setting the restore did not reconstruct is also an "
                "unexpected divergence, so a reader that predates not_reconstructed never "
                "reads it as parity"
            )
        # NR-3. Never an intended deviation.
        if any(e in parity_block["intentionally_deviated"] for e in not_reconstructed):
            return (
                "topic_parity.not_reconstructed names a deviation that intentionally_deviated "
                "also names; a source setting the restore did not reconstruct is never an "
                "intended deviation"
            )
        # NR-4 and NR-5 (FX-3 review F1): in a `newTopic` document the block
        # is the claim, and the two existing lists must agree with it, or a
        # writer that lost the mode signs `not_reconstructed: []` beside the
        # scratch labels. Only on a document carrying the block whose
        # `target.mode` is `newTopic`; each can only refuse.
        if doc["target"].get("mode") == "newTopic":
            # NR-4. A `newTopic` restore labels nothing intended.
            if parity_block["intentionally_deviated"]:
                return (
                    "topic_parity.intentionally_deviated is not empty in a newTopic document "
                    "that carries not_reconstructed; a newTopic restore's deviations are "
                    "source settings it did not reconstruct, never intended ones"
                )
            # NR-5. The converse of NR-2 over the settings the restore decides.
            if any(
                _parity_key(e) in RESTORE_DECIDED_SETTINGS and e not in not_reconstructed
                for e in parity_block["unexpected_divergence"]
            ):
                return (
                    "topic_parity.unexpected_divergence names a setting the restore decides "
                    "(cleanup.policy, retention.ms, partition_count or replication_factor) "
                    "that not_reconstructed does not, in a newTopic document; such a "
                    "deviation is a source setting the restore did not reconstruct"
                )

    # `source.time_basis` (format 1.3.0, FX-8): arms TB-1 to TB-4, mirrored ARM
    # FOR ARM, IN THIS POSITION (after `target.auth`, before `redactions`) and
    # with the same words from `Scorecard::validate_invariants`. They fire ONLY
    # on a document carrying the block, so every document before 1.3.0 is
    # decided exactly as before. Not interpolated except TB-1's version: the
    # lists name topics. The shape layer above has proved the block's shape.
    if time_basis is not None:
        # TB-1. A version before 1.3.0 cannot carry the 1.3.0 field.
        if not _defines_format_1_minor(version, SCORECARD_TIME_BASIS_SINCE_MINOR):
            return (
                f"source.time_basis is present but format_version "
                f"{_rust_debug_str(version)} predates it: the field is defined from "
                f"1.{SCORECARD_TIME_BASIS_SINCE_MINOR}.0"
            )
        # TB-2. One value.
        if time_basis.get("plan") is not None and time_basis["plan"] != "producerTime":
            return (
                "source.time_basis.plan is not \"producerTime\", the one value "
                "restore.time_basis has"
            )
        # TB-3. A selection by producer time is one the approved plan accepted.
        if time_basis["producer_time"] and time_basis.get("plan") != "producerTime":
            return (
                "source.time_basis.producer_time names a topic but source.time_basis.plan is "
                "not \"producerTime\"; a selection by producer time is one the approved plan "
                "accepted, never a default"
            )
        # TB-4. Recorded as LogAppendTime, or not recorded: never both.
        if any(t in time_basis["not_recorded"] for t in time_basis["producer_time"]):
            return (
                "source.time_basis names a topic in both producer_time and not_recorded; a "
                "topic's timestamp type was either recorded as LogAppendTime or not recorded"
            )

    # `integrity.verification` (format 1.4.0, PROD-08.1): arms IV-1 to IV-7,
    # mirrored ARM FOR ARM, IN THIS POSITION (after `source.time_basis`, before
    # `redactions`) and with the same words from
    # `Scorecard::validate_invariants`. They fire ONLY on a document carrying
    # the block, so every document before 1.4.0 is decided exactly as before.
    # Not interpolated except IV-1's version: the block names topics and
    # segment keys. The shape layer above has proved the block's shape and the
    # u64 loop every count's domain.
    if verification is not None:
        # IV-1. A version before 1.4.0 cannot carry the 1.4.0 field.
        if not _defines_format_1_minor(version, SCORECARD_VERIFICATION_SINCE_MINOR):
            return (
                f"integrity.verification is present but format_version "
                f"{_rust_debug_str(version)} predates it: the field is defined from "
                f"1.{SCORECARD_VERIFICATION_SINCE_MINOR}.0"
            )
        # IV-2. A coverage this reader does not know is refused.
        if verification["coverage"] not in ("sampled", "complete"):
            return "integrity.verification.coverage is neither \"sampled\" nor \"complete\""
        # IV-3. Only a complete verification compares headers in order.
        complete_cov = verification["coverage"] == "complete"
        order = verification["header_order"]
        if order not in ("verified", "notVerified") or (order == "verified" and not complete_cov):
            return (
                "integrity.verification.header_order is not \"verified\" or \"notVerified\", "
                "or claims \"verified\" for a coverage that is not complete; a sampled "
                "verification compares a fingerprint that sorts headers"
            )
        # IV-4. The complete block exactly with complete coverage.
        c = verification.get("complete")
        if complete_cov != (c is not None):
            return (
                "integrity.verification.complete is present exactly when "
                "integrity.verification.coverage is \"complete\""
            )
        if c is not None:
            # IV-5. A reason exactly when not covered; blank is no reason.
            reason_given = bool(str(c.get("incomplete_reason") or "").strip(RUST_WHITESPACE))
            if c["covered"] == reason_given:
                return (
                    "integrity.verification.complete.incomplete_reason is required exactly "
                    "when complete.covered is false"
                )
            replay = c["replay"]
            archive = c["archive"]
            # IV-6. A pass over a complete block is a covered, clean one.
            if integrity.get("result") == "pass":
                clean = (
                    c["covered"]
                    and bool(c["partitions"])
                    and not archive["segments_failed"]
                    and not archive["segments_unverified"]
                    and archive["segments_verified"] == archive["segments"]
                    and _replay_exact(replay)
                    and all(p["compared"] and _replay_exact(p["replay"]) for p in c["partitions"])
                )
                if not clean:
                    return (
                        "integrity.result is pass but integrity.verification.complete is not "
                        "covered, lists no partition, names a failed or unverified segment, or "
                        "records a missing, unexpected, duplicate, out-of-order or mismatched "
                        "record, in total or in a partition"
                    )
            # IV-7. The totals are the partitions' sums (saturating, as the
            # Rust reader adds), and every segment is accounted for.
            sums = {k: 0 for k in REPLAY_FIELDS}
            segs = verified_segs = decoded = holes = 0
            for p in c["partitions"]:
                for k in REPLAY_FIELDS:
                    sums[k] = _sat_add(sums[k], p["replay"][k])
                segs = _sat_add(segs, p["segments"])
                verified_segs = _sat_add(verified_segs, p["segments_verified"])
                decoded = _sat_add(decoded, p["records_decoded"])
                holes = _sat_add(holes, p["offset_holes"])
            accounted = _sat_add(
                _sat_add(archive["segments_verified"], len(archive["segments_failed"])),
                len(archive["segments_unverified"]),
            )
            if (
                any(sums[k] != replay[k] for k in REPLAY_FIELDS)
                or segs != archive["segments"]
                or verified_segs != archive["segments_verified"]
                or decoded != archive["records_decoded"]
                or holes != archive["offset_holes"]
                or accounted != archive["segments"]
            ):
                return (
                    "integrity.verification.complete's totals are not the sums of its "
                    "partitions, or its segments are not each verified, failed or unverified"
                )

    # `sample.unsampled_topics` (format 1.6.0, FX-23): arms US-1 to US-3,
    # mirrored ARM FOR ARM, IN THIS POSITION (after `integrity.verification`,
    # before `redactions`) and with the same words from
    # `Scorecard::validate_invariants`. They fire ONLY on a document carrying
    # the field, so every document before 1.6.0 is decided exactly as before.
    # Not interpolated except US-1's version: the list names topics. The shape
    # layer above has proved it is an array of strings. Python compares `str`
    # by code point and Rust `String` by UTF-8 byte, which order alike.
    if unsampled is not None:
        # US-1. A version before 1.6.0 cannot carry the 1.6.0 field.
        if not _defines_format_1_minor(version, SCORECARD_UNSAMPLED_TOPICS_SINCE_MINOR):
            return (
                f"sample.unsampled_topics is present but format_version "
                f"{_rust_debug_str(version)} predates it: the field is defined from "
                f"1.{SCORECARD_UNSAMPLED_TOPICS_SINCE_MINOR}.0"
            )
        # US-2. Each topic once, in order, and absent rather than empty.
        if (
            not unsampled
            or any(not t.strip(RUST_WHITESPACE) for t in unsampled)
            or any(a >= b for a, b in zip(unsampled, unsampled[1:]))
        ):
            return (
                "sample.unsampled_topics is empty, names a blank topic, or is not sorted and "
                "free of repeats; it names each topic max_partitions left unsampled once, in "
                "order, and is absent when there is none"
            )
        # US-3. A complete verification leaves no topic unsampled.
        if verification is not None and verification.get("coverage") == "complete":
            return (
                "sample.unsampled_topics is present but integrity.verification.coverage is "
                "\"complete\"; a complete verification compares every restored partition and "
                "leaves no topic unsampled"
            )

    # `source.selection` (format 1.7.0, PROD-11.1): arms SEL-1 to SEL-3, and
    # (format 2.0.0, PROD-11.1b, OD-9 (a)) PS-2 to PS-5, mirrored ARM FOR ARM,
    # IN THIS POSITION (after `sample.unsampled_topics`, before `redactions`)
    # and with the same words from `Scorecard::validate_invariants`. They fire
    # ONLY on a document carrying the block, so every document without it is
    # decided exactly as before. Not interpolated except SEL-1's version. The
    # shape layer above has proved the block's types.
    if selection is not None:
        # SEL-1. A version before 1.7.0 cannot carry the 1.7.0 block.
        if not _defines_format_1_minor(version, SCORECARD_SELECTION_SINCE_MINOR):
            return (
                f"source.selection is present but format_version "
                f"{_rust_debug_str(version)} predates it: the block is defined from "
                f"1.{SCORECARD_SELECTION_SINCE_MINOR}.0"
            )
        start = selection.get("window_start_ms")
        end = selection["window_end_ms"]
        subsets = selection.get("partitions")
        runs = selection.get("engine_runs")
        # PS-2. A format-1 block is a window start and its end, nothing else.
        if doc_major == 1 and (start is None or subsets is not None or runs is not None):
            return (
                "source.selection under major 1 is a stated window start and its end, and "
                "nothing else: a block without window_start_ms, or with partitions or "
                "engine_runs, is a partition-subset selection, which is format 2.0.0"
            )
        # SEL-2. A stated start is before the end.
        if start is not None and start >= end:
            return (
                "source.selection.window_start_ms is not before window_end_ms; a selection's "
                "window holds at least one instant after its start"
            )
        # SEL-3. The complete block's window is the selection's (no start for
        # a 2.0.0 block from the archive's floor, as the complete block's).
        complete = verification.get("complete") if verification is not None else None
        if complete is not None:
            window = complete["window"]
            if window.get("start_ms") != start or window["end_ms"] != end:
                return (
                    "integrity.verification.complete.window is not source.selection's window; "
                    "the expected output is selected by the plan's own start and end"
                )
        if subsets is not None:
            # PS-3. One spelling per selection.
            well_formed = all(
                a["topic"] < b["topic"] for a, b in zip(subsets, subsets[1:])
            ) and all(
                e["topic"].strip(RUST_WHITESPACE)
                and e["partitions"]
                and all(p >= 0 for p in e["partitions"])
                and all(a < b for a, b in zip(e["partitions"], e["partitions"][1:]))
                for e in subsets
            )
            if not well_formed:
                return (
                    "source.selection.partitions does not name each topic once, in order, with "
                    "a non-empty, sorted list of distinct partitions that are not negative"
                )
            # PS-4. One run per distinct subset, and at most one more.
            need = len({tuple(e["partitions"]) for e in subsets})
            if runs is None or runs not in (need, need + 1):
                return (
                    "source.selection.engine_runs is not one run per distinct partition subset, "
                    "or one more for the topics without one"
                )
            # PS-5. Nothing is expected from a partition the plan did not
            # select.
            if complete is not None:
                chosen = {e["topic"]: e["partitions"] for e in subsets}
                for entry in complete["partitions"]:
                    listed = chosen.get(entry["topic"])
                    if entry["replay"]["expected"] > 0 and (
                        listed is not None and entry["partition"] not in listed
                    ):
                        return (
                            "integrity.verification.complete.partitions expects records from a "
                            "partition source.selection does not select"
                        )

    # `target.original_name` (format 1.8.0, PROD-15.1): arms ON-1 to ON-14,
    # mirrored ARM FOR ARM, IN THIS POSITION (after `source.selection`, before
    # `redactions`) and with the same words from `Scorecard::validate_invariants`.
    # They fire ONLY on a document carrying the block, so every document before
    # 1.8.0 is decided exactly as before. Not interpolated except ON-1's
    # version. The shape layer above has proved the block's types. ON-14 is
    # judged FIRST.
    if original_name is not None:
        # ON-14, first. An original-name restore restores WHOLE topics: the
        # block never sits beside a partition subset. Every 2.x document names
        # a subset (PS-1), so this is the arm a 2.x document carrying the block
        # meets; a subset under major 1 has already met PS-2.
        if selection is not None and selection.get("partitions") is not None:
            return (
                "target.original_name is present beside source.selection.partitions; a "
                "restore under the original topic names restores whole topics, never a "
                "partition subset"
            )
        # ON-1. The block is format 1's, from 1.8.0. MAJOR 1 ON PURPOSE (the
        # older optional blocks read `_defines_format_1_minor`): a 2.x document
        # is a partition-subset restore's and never carries the block (ON-14).
        if not _defines_original_name(version):
            return (
                f"target.original_name is present but format_version "
                f"{_rust_debug_str(version)} does not define it: the block is format 1's, "
                f"from 1.{SCORECARD_ORIGINAL_NAME_SINCE_MINOR}.0, and no other major carries it"
            )
        # ON-2. The identity ban stays in scratch mode (absent mode is scratch).
        if target.get("mode") in (None, "scratch"):
            return (
                "target.original_name is present but target.mode is scratch; a scratch drill "
                "never restores under the original topic names"
            )
        # ON-3. The original names ARE the identity mapping.
        if target.get("topic_mapping_prefix") != "":
            return (
                "target.original_name is present but target.topic_mapping_prefix is not "
                "empty; an original-name restore maps every topic onto its own name"
            )
        # ON-4.
        if original_name["approval_subject"] != "originalName":
            return (
                "target.original_name.approval_subject is not \"originalName\"; an "
                "original-name restore is authorised only by its own approval subject"
            )
        # ON-5. PROD-16.2 (format 1.9.0) SPLITS THIS ARM BY VERSION, as
        # PROD-01.3 split `target.auth.mode`'s, and leaves every document
        # below 1.9.0 judged exactly as before: `consoleApproval` is a value
        # of 1.9.0 and later, so under an older minor it is refused as a value
        # that version does not define (naming the version, never the
        # document's word), and from 1.9.0 the closed set is four.
        four_defined = _defines_console_approval(version)
        if original_name["approval_mode"] == APPROVAL_MODE_CONSOLE:
            if not four_defined:
                return (
                    "target.original_name.approval_mode is a value defined from "
                    f"1.{SCORECARD_CONSOLE_APPROVAL_SINCE_MINOR}.0 and format_version "
                    f"{_rust_debug_str(version)} predates it"
                )
        elif original_name["approval_mode"] not in ORIGINAL_NAME_APPROVAL_MODES_AT_1_8_0:
            if four_defined:
                return (
                    "target.original_name.approval_mode is not one of the four values this "
                    "format defines; it is \"v1Approval\", \"governed\", \"ordinary\" or "
                    "\"consoleApproval\" and nothing else"
                )
            return (
                "target.original_name.approval_mode is not one of \"v1Approval\", "
                "\"governed\", \"ordinary\""
            )
        # ON-6.
        if original_name["cluster_condition"] not in ORIGINAL_NAME_CLUSTER_CONDITIONS:
            return (
                "target.original_name.cluster_condition is not one of \"targetIsNotSource\", "
                "\"autoCreateDisabled\""
            )
        # ON-7. "Not the source" is a comparison of two known ids.
        named_source = str(original_name.get("source_cluster_id") or "")
        if original_name["cluster_condition"] == "targetIsNotSource" and (
            not named_source.strip(RUST_WHITESPACE)
            or named_source == target.get("cluster_id")
        ):
            return (
                "target.original_name.cluster_condition is targetIsNotSource but "
                "source_cluster_id is absent or equals target.cluster_id; the condition is a "
                "comparison of two known cluster ids"
            )
        # ON-8. Somewhere was looked, each place once, from the closed set.
        places = original_name["owner_detection"]
        if (
            not places
            or len(set(places)) != len(places)
            or any(p not in ORIGINAL_NAME_OWNER_DETECTION_PLACES for p in places)
        ):
            return (
                "target.original_name.owner_detection is empty, repeats a place, or names "
                "one outside \"plan\", \"kafkaTopicResources\", \"pointReceipt\"; an owner "
                "nobody looked for is never read as no owner"
            )
        # ON-9.
        owners = original_name["owners"]
        if any(
            o["found_in"] not in places
            or o["kind"] not in ORIGINAL_NAME_OWNER_KINDS
            or not o["topic"].strip(RUST_WHITESPACE)
            for o in owners
        ):
            return (
                "target.original_name.owners names a place owner_detection does not list, a "
                "kind outside \"strimzi\" and \"external\", or a blank topic"
            )
        # ON-10.
        if owners and not original_name["owner_path"]:
            return (
                "target.original_name.owners is not empty and owner_path is false; an owned "
                "name is restored only on the owner path"
            )
        # ON-11 (OD-10). A one-person confirmation is signed only with the
        # topic names typed, and nothing else claims a typed confirmation.
        confirmation = original_name.get("confirmation")
        typed = confirmation == "typedTopicNames"
        if (original_name["approval_mode"] == "ordinary") != typed or (
            confirmation is not None and not typed
        ):
            return (
                "target.original_name.confirmation is not \"typedTopicNames\" exactly when "
                "approval_mode is \"ordinary\"; a one-person confirmation of an original-name "
                "restore is signed only with every original topic name re-typed"
            )
        # ON-12. The resources file a runner looked in is named by digest,
        # exactly when it is a place that was looked in.
        digest = original_name.get("kafka_topic_resources_sha256")
        listed = "kafkaTopicResources" in original_name["owner_detection"]
        digest_ok = _is_sha256_prefixed(digest)
        if listed != digest_ok or (digest is not None and not digest_ok):
            return (
                "target.original_name.kafka_topic_resources_sha256 is not a sha256 digest "
                "exactly when owner_detection lists \"kafkaTopicResources\"; the KafkaTopic "
                "resources a runner looked in are named by their digest"
            )
        # ON-13. An original-name restore is verified COMPLETELY, never by
        # sample; a pass that records no verification is refused too (the
        # third case, decided to the safer side). IV-2 has already refused a
        # coverage that is neither value.
        on_coverage = verification["coverage"] if verification is not None else None
        on_passes = doc.get("outcome") == "pass" or integrity.get("result") == "pass"
        if (on_coverage is not None and on_coverage != "complete") or (
            on_coverage is None and on_passes
        ):
            return (
                "target.original_name is present but integrity.verification.coverage is not "
                "\"complete\", or a pass records no verification; a restore under the original "
                "topic names is verified completely, never by sample"
            )

    # `approval.console` (format 1.9.0, and 2.1.0; PROD-16.2): arms CA-1 to
    # CA-8, mirrored ARM FOR ARM, IN THIS POSITION (after the ON arms, before
    # `redactions`) and with the same words from
    # `Scorecard::validate_invariants`. CA-1 to CA-7 fire ONLY on a document
    # carrying the block, so every document without it is decided exactly as
    # before. CA-5, CA-6 and CA-7 judge existing fields against the block and
    # can only refuse. Not interpolated except CA-1's version: no message
    # carries a principal, an instant or a key id from the document. The shape
    # layer above has proved the block's types and the three fields beside it.
    if console is not None:
        # CA-1. The block is defined from 1.9.0 of format 1 and from 2.1.0 of
        # format 2.
        if not _defines_console_approval(version):
            return (
                f"approval.console is present but format_version "
                f"{_rust_debug_str(version)} does not define it: the block is defined from "
                f"1.{SCORECARD_CONSOLE_APPROVAL_SINCE_MINOR}.0 of format 1 and from "
                f"2.{SCORECARD_CONSOLE_APPROVAL_SINCE_MINOR_OF_MAJOR_2}.0 of format 2"
            )
        # CA-2. The block describes one mode.
        if console["mode"] != APPROVAL_MODE_CONSOLE:
            return "approval.console.mode is not \"consoleApproval\""
        # CA-3. Two people: each in a form that can be compared, neither the
        # local administrator nor a system identity, of one issuer, with
        # different subjects.
        not_two_people = _console_separation_words(console["requester"], console["approver"])
        if not_two_people is not None:
            return f"approval.console does not name two people: {not_two_people}"
        # CA-4. No fabricated time: the approval lies inside the request's own
        # window.
        console_approved_at = _instant(console["approved_at"])
        if console_approved_at < _instant(console["requested_at"]) or (
            console_approved_at >= _instant(console["request_expires_at"])
        ):
            return (
                "approval.console.approved_at is before requested_at or not before "
                "request_expires_at; an approval is given after the request was made and "
                "before it expires"
            )
        # CA-5. The approver the scorecard names IS the second person.
        console_approver = console["approver"]
        if approval["approver"] != f"{console_approver['issuer']}#{console_approver['subject']}":
            return (
                "approval.approver is not approval.console.approver as "
                "\"<issuer>#<subject>\"; under a console approval the approver a scorecard "
                "names is the second person the console attested"
            )
        # CA-6. The console's key signs the approval, so the approver's key IS
        # the console key in this mode -- and a distinct personal key is
        # another mode.
        if (
            not console["confirmation_key_id"].strip(RUST_WHITESPACE)
            or approval["key_id"] != console["confirmation_key_id"]
        ):
            return (
                "approval.key_id is not approval.console.confirmation_key_id; under a "
                "console approval the console's key signs the approval, so the approver's "
                "key is the console key (expected in this mode), and a distinct personal key "
                "is not a console approval"
            )
        # CA-7. The approval time is the instant the second person approved,
        # and no other.
        if _instant(approval["approved_at"]) != console_approved_at:
            return (
                "approval.approved_at is not approval.console.approved_at; under a console "
                "approval the approval time is the instant the second person approved, never "
                "the request's"
            )
    # CA-8. A restore under the original topic names that a second person
    # approved in the console says so in BOTH places, and no other
    # original-name restore says so in either.
    if original_name is not None and (
        (original_name["approval_mode"] == APPROVAL_MODE_CONSOLE) != (console is not None)
    ):
        return (
            "target.original_name.approval_mode is \"consoleApproval\" exactly when "
            "approval.console is present; a console approval names who approved, and a "
            "governed, ordinary or v1 approval carries no console approver"
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
    # FX-7 (receipt format 1.2.0): `archive.manifest_version_id` is OPTIONAL and,
    # when present, a string — `ReceiptArchive::manifest_version_id` is an
    # `Option<String>`, so Rust accepts it absent or null and refuses any other
    # JSON type at deserialisation. Refused here at the same layer, so a
    # document the Rust reader never parsed is never VALID here.
    version_id = doc["archive"].get("manifest_version_id")
    if version_id is not None and not isinstance(version_id, str):
        return "archive.manifest_version_id is not a string"
    # FX-4, format 1.1.0: `config_coverage` is `Option<BTreeMap<String,
    # TopicConfigCoverage>>` over there, so every one of these is refused at
    # DESERIALISATION by the Rust reader before arm 6 runs; they belong in the
    # shape layer here for the reason `source.auth` above does. `null` is
    # absent on both sides.
    coverage = doc.get("config_coverage")
    if coverage is not None:
        if not isinstance(coverage, dict):
            return "config_coverage is not an object"
        for topic in sorted(coverage):
            entry = coverage[topic]
            where = f"config_coverage[{_rust_debug_str(topic)}]"
            if not isinstance(entry, dict):
                return f"{where} is not an object"
            if not isinstance(entry.get("coverage"), str):
                return f"{where}.coverage is not a string"
            if entry.get("reason") is not None and not isinstance(entry["reason"], str):
                return f"{where}.reason is not a string"
            observed = entry.get("timestamp_type")
            if observed is not None:
                if not isinstance(observed, dict):
                    return f"{where}.timestamp_type is not an object"
                for name in ("value", "source"):
                    if not isinstance(observed.get(name), str):
                        return f"{where}.timestamp_type.{name} is not a string"
    # PROD-05.1, format 1.3.0: `topic_configuration` is `Option<BTreeMap<
    # String, TopicConfiguration>>`; the counts are `Option<u32>`, `entries`
    # an `Option<BTreeMap<String, ConfigEntry>>` whose `value` is an
    # `Option<String>`, and `owner` an `Option<TopicOwner>` of three strings.
    # Rust refuses every one of these at DESERIALISATION, before arm 12 runs,
    # so they belong in the shape layer here for the reason `source.auth`
    # does. `null` is absent on both sides.
    model = doc.get("topic_configuration")
    if model is not None:
        if not isinstance(model, dict):
            return "topic_configuration is not an object"
        for topic in sorted(model):
            entry = model[topic]
            where = f"topic_configuration[{_rust_debug_str(topic)}]"
            if not isinstance(entry, dict):
                return f"{where} is not an object"
            for name in ("partitions", "replication_factor"):
                count = entry.get(name)
                if count is not None and (
                    not isinstance(count, int)
                    or isinstance(count, bool)
                    or not 0 <= count < 2 ** 32
                ):
                    return f"{where}.{name} is not a u32"
            entries = entry.get("entries")
            if entries is not None:
                if not isinstance(entries, dict):
                    return f"{where}.entries is not an object"
                for key in sorted(entries):
                    config = entries[key]
                    at = f"{where}.entries[{_rust_debug_str(key)}]"
                    if not isinstance(config, dict):
                        return f"{at} is not an object"
                    if config.get("value") is not None and not isinstance(config["value"], str):
                        return f"{at}.value is not a string"
                    for name in ("source", "portability"):
                        if not isinstance(config.get(name), str):
                            return f"{at}.{name} is not a string"
            owner = entry.get("owner")
            if owner is not None:
                if not isinstance(owner, dict):
                    return f"{where}.owner is not an object"
                for name in ("kind", "basis", "reference"):
                    if not isinstance(owner.get(name), str):
                        return f"{where}.owner.{name} is not a string"
    # PROD-05.1: `owner_detection` is `Option<Vec<String>>`, refused at
    # deserialisation when it is anything else.
    detection = doc.get("owner_detection")
    if detection is not None and (
        not isinstance(detection, list) or not all(isinstance(d, str) for d in detection)
    ):
        return "owner_detection is not a list of strings"
    # PROD-03.0, format 1.5.0: `schema_dependency` is `Option<BTreeMap<String,
    # TopicSchemaDependency>>`: a required `verdict` string, optional `reason`
    # and `basis` strings, and optional `key`/`value` `SideFraming`s of a
    # bool, four `u64`s and a `Vec<u32>`. Rust refuses every one of these at
    # DESERIALISATION, before arm 22 runs, so they belong in the shape layer
    # here for the reason `source.auth` does. `null` is absent on both sides.
    dependency = doc.get("schema_dependency")
    if dependency is not None:
        if not isinstance(dependency, dict):
            return "schema_dependency is not an object"
        for topic in sorted(dependency):
            entry = dependency[topic]
            where = f"schema_dependency[{_rust_debug_str(topic)}]"
            if not isinstance(entry, dict):
                return f"{where} is not an object"
            if not isinstance(entry.get("verdict"), str):
                return f"{where}.verdict is not a string"
            for name in ("reason", "basis"):
                if entry.get(name) is not None and not isinstance(entry[name], str):
                    return f"{where}.{name} is not a string"
            for name in ("key", "value"):
                side = entry.get(name)
                if side is None:
                    continue
                if not isinstance(side, dict):
                    return f"{where}.{name} is not an object"
                if not isinstance(side.get("dependent"), bool):
                    return f"{where}.{name}.dependent is not a boolean"
                for count in ("framed", "unframed", "nulls", "schema_id_count"):
                    value = side.get(count)
                    if (
                        not isinstance(value, int)
                        or isinstance(value, bool)
                        or not 0 <= value < 2 ** 64
                    ):
                        return f"{where}.{name}.{count} is not a u64"
                ids = side.get("schema_ids")
                if not isinstance(ids, list) or not all(
                    isinstance(i, int) and not isinstance(i, bool) and 0 <= i < 2 ** 32
                    for i in ids
                ):
                    return f"{where}.{name}.schema_ids is not a list of u32"
    # PROD-01.4a, format 1.6.0: `generations` is `Option<BTreeMap<String,
    # TopicIdentity>>`, whose five fields are each an `Option<String>`, so Rust
    # refuses any other JSON type at DESERIALISATION, before arm 36 runs. `null`
    # is absent on both sides.
    generations = doc.get("generations")
    if generations is not None:
        if not isinstance(generations, dict):
            return "generations is not an object"
        for topic in sorted(generations):
            entry = generations[topic]
            where = f"generations[{_rust_debug_str(topic)}]"
            if not isinstance(entry, dict):
                return f"{where} is not an object"
            for name in (
                "topic_id",
                "topic_id_after",
                "topic_id_source",
                "topic_id_reason",
                "topic_id_after_reason",
            ):
                value = entry.get(name)
                if value is not None and not isinstance(value, str):
                    return f"{where}.{name} is not a string"
    # PROD-04.1, format 1.7.0: `consumer_positions` is
    # `Option<ConsumerPositions>` over there; every one of these is refused at
    # DESERIALISATION by the Rust reader before arm 30 runs (an instant is
    # RFC 3339, a count and `members` a `u32`, `document.bytes` a `u64`,
    # `active` a `bool`), so they belong in the shape layer here for the
    # reason `source.auth` does. `null` is absent on both sides.
    shape = _consumer_positions_shape(doc.get("consumer_positions"))
    if shape:
        return shape
    return ""


def _is_int_in(value, low, high) -> bool:
    """A JSON integer (never a bool) in `[low, high]`."""
    return isinstance(value, int) and not isinstance(value, bool) and low <= value <= high


def _consumer_positions_shape(cp) -> str:
    """"" when `cp` is absent or has the shape `ConsumerPositions` reads (the
    receipt's 1.7.0 block: the summary, never the positions)."""
    if cp is None:
        return ""
    if not isinstance(cp, dict):
        return "consumer_positions is not an object"
    for name in ("observed_from", "observed_to"):
        if _rfc3339_ns(cp.get(name)) is None:
            return f"consumer_positions.{name} is not an RFC 3339 instant"
    if not isinstance(cp.get("listing"), str):
        return "consumer_positions.listing is not a string"
    document = cp.get("document")
    if not isinstance(document, dict):
        return "consumer_positions.document is not an object"
    for name in ("key", "sha256"):
        if not isinstance(document.get(name), str):
            return f"consumer_positions.document.{name} is not a string"
    if not _is_int_in(document.get("bytes"), 0, 2**64 - 1):
        return "consumer_positions.document.bytes is not a u64"
    if not isinstance(cp.get("groups"), dict):
        return "consumer_positions.groups is not an object"
    u32 = (0, 2**32 - 1)
    for group_id in sorted(cp["groups"]):
        group = cp["groups"][group_id]
        where = f"consumer_positions.groups[{_rust_debug_str(group_id)}]"
        if not isinstance(group, dict):
            return f"{where} is not an object"
        if not isinstance(group.get("outcome"), str):
            return f"{where}.outcome is not a string"
        for name in ("reason", "group_type", "state", "listed_state"):
            value = group.get(name)
            if value is not None and not isinstance(value, str):
                return f"{where}.{name} is not a string"
        members = group.get("members")
        if members is not None and not _is_int_in(members, *u32):
            return f"{where}.members is not a u32"
        active = group.get("active")
        if active is not None and not isinstance(active, bool):
            return f"{where}.active is not a boolean"
        counts = group.get("counts")
        if counts is None:
            continue
        if not isinstance(counts, dict):
            return f"{where}.counts is not an object"
        for name in CP_COUNT_NAMES:
            if not _is_int_in(counts.get(name), *u32):
                return f"{where}.counts.{name} is not a u32"
    return ""


def _positions_document_shape(pd) -> str:
    """"" when `pd` has the shape `PositionsDocument` reads; otherwise why not,
    as the Rust reader refuses it at deserialisation before arm CP-1."""
    if not isinstance(pd, dict):
        return "the positions document is not an object"
    for name in ("format_version", "backup_id", "run_id"):
        if not isinstance(pd.get(name), str):
            return f"the positions document's {name} is not a string"
    for name in ("topics", "groups"):
        if not isinstance(pd.get(name), dict):
            return f"the positions document's {name} is not an object"
    i64 = (-(2**63), 2**63 - 1)
    u32 = (0, 2**32 - 1)
    for topic in sorted(pd["topics"]):
        entry = pd["topics"][topic]
        where = f"the positions document's topics[{_rust_debug_str(topic)}]"
        if not isinstance(entry, dict):
            return f"{where} is not an object"
        if not isinstance(entry.get("partitions"), list):
            return f"{where}.partitions is not an array"
        if not isinstance(entry.get("changed_during_capture"), bool):
            return f"{where}.changed_during_capture is not a boolean"
        for i, facts in enumerate(entry["partitions"]):
            at = f"{where}.partitions[{i}]"
            if not isinstance(facts, dict):
                return f"{at} is not an object"
            if not _is_int_in(facts.get("partition"), *u32):
                return f"{at}.partition is not a u32"
            if not isinstance(facts.get("observed"), bool):
                return f"{at}.observed is not a boolean"
            for name in (
                "log_start",
                "high_watermark",
                "log_start_after",
                "high_watermark_after",
                "archived_first",
                "archived_last",
            ):
                value = facts.get(name)
                if value is not None and not _is_int_in(value, *i64):
                    return f"{at}.{name} is not an i64"
    for group_id in sorted(pd["groups"]):
        group = pd["groups"][group_id]
        where = f"the positions document's groups[{_rust_debug_str(group_id)}]"
        if not isinstance(group, dict):
            return f"{where} is not an object"
        if not _is_int_in(group.get("no_committed_position"), *u32):
            return f"{where}.no_committed_position is not a u32"
        if not isinstance(group.get("positions"), list):
            return f"{where}.positions is not an array"
        for i, entry in enumerate(group["positions"]):
            at = f"{where}.positions[{i}]"
            if not isinstance(entry, dict):
                return f"{at} is not an object"
            for name in ("topic", "status"):
                if not isinstance(entry.get(name), str):
                    return f"{at}.{name} is not a string"
            if not _is_int_in(entry.get("partition"), *u32):
                return f"{at}.partition is not a u32"
            position = entry.get("position")
            if position is not None and not _is_int_in(position, *i64):
                return f"{at}.position is not an i64"
            for name in ("reason", "coverage"):
                value = entry.get(name)
                if value is not None and not isinstance(value, str):
                    return f"{at}.{name} is not a string"
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
    5. `source.auth.mode` is `plaintext` or `scramSha512`, and from format
       1.4.0 (PROD-01.3) also `scramSha256`, `plain` or `mtls`.

    Arms 6-21 read the 1.1.0 and 1.3.0 blocks (FX-4, PROD-05.1), arms 22-29
    the 1.5.0 `schema_dependency` (PROD-03.0), arms 30-35 the 1.7.0
    `consumer_positions` summary (PROD-04.1), and arms 36-40 the 1.6.0
    `generations` (PROD-01.4a): present only from 1.6.0,
    covering exactly the named topic set, every recorded topic ID canonical, a
    reason exactly when an ID is null, and a source exactly when one is
    recorded. Each block's arms run only when it is present.
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
    named = bool(str(manifest_key or "").strip(RUST_WHITESPACE))
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
    #
    # PROD-01.3 (receipt format 1.4.0) splits the arm into three statements,
    # mirrored in the Rust reader's order: 5b, a PROD-01.3 mode under a version
    # that predates it; 5c, the closed five from 1.4.0; 5a, the closed two
    # below it, unchanged.
    mode = doc["source"]["auth"]["mode"]
    five_defined = parsed[1] >= RECEIPT_AUTH_MODES_SINCE_MINOR
    if mode in PROD_01_3_AUTH_MODES:
        if not five_defined:
            return (
                f"source.auth.mode {_rust_debug_str(mode)} is defined from "
                f"1.{RECEIPT_AUTH_MODES_SINCE_MINOR}.0 and format_version "
                f"{_rust_debug_str(version)} predates it"
            )
    elif mode not in ORIGINAL_AUTH_MODES:
        if five_defined:
            return (
                f"source.auth.mode {_rust_debug_str(mode)} is not one of the five values this "
                "format defines: \"plaintext\", \"scramSha512\", \"scramSha256\", \"plain\" "
                "or \"mtls\""
            )
        return (
            f"source.auth.mode {_rust_debug_str(mode)} is not one of the two values this "
            "format defines: \"plaintext\" or \"scramSha512\""
        )

    # ARMS 6-11 (format 1.1.0, FX-4): the `config_coverage` block, and ONLY
    # when it is present, so every 1.0.0 receipt is decided exactly as before.
    # Topics in NAME order — the Rust block is a BTreeMap — and arms 8-11 per
    # topic, in order.
    coverage = doc.get("config_coverage")
    if coverage is not None:
        # ARM 6. A document declaring 1.0.x cannot carry a 1.1 field. Arm 1 has
        # established the version parses and its major is 1.
        if parsed[1] < RECEIPT_CONFIG_COVERAGE_SINCE_MINOR:
            return (
                f"config_coverage is present but format_version {_rust_debug_str(version)} "
                f"predates it: the field is defined from 1.{RECEIPT_CONFIG_COVERAGE_SINCE_MINOR}.0"
            )
        # ARM 7. The covered set is the named set — arm 3's twin.
        covered = list(coverage.keys())
        if sorted(set(covered)) != sorted(set(named_topics)):
            return (
                f"config_coverage covers {_render_topic_set(covered)} but the named topic set "
                f"is {_render_topic_set(named_topics)}"
            )
        for topic in sorted(coverage):
            entry = coverage[topic]
            value = entry["coverage"]
            reason = entry.get("reason")
            observed = entry.get("timestamp_type")
            # ARM 8. The coverage vocabulary is closed.
            if value not in ("captured", "notCaptured", "captureDenied"):
                return (
                    f"config_coverage[{_rust_debug_str(topic)}].coverage "
                    f"{_rust_debug_str(value)} is not one of the three values this format "
                    "defines: \"captured\", \"notCaptured\" or \"captureDenied\""
                )
            # ARM 9. A reason exactly when `notCaptured`, from a closed set.
            if reason is not None:
                fits = value == "notCaptured" and reason in ("describeFailed", "manifestDiffers")
            else:
                fits = value != "notCaptured"
            if not fits:
                rendered = _rust_debug_str(reason) if reason is not None else "absent"
                return (
                    f"config_coverage[{_rust_debug_str(topic)}].reason {rendered} does not fit "
                    f"coverage {_rust_debug_str(value)}: a reason is present exactly when coverage "
                    "is \"notCaptured\", and is \"describeFailed\" or \"manifestDiffers\""
                )
            # ARM 10. An observation only where the read succeeded.
            if (value == "captureDenied" or reason == "describeFailed") and observed is not None:
                return (
                    f"config_coverage[{_rust_debug_str(topic)}] records a timestamp_type, but a topic "
                    "whose configuration read was denied or failed cannot have observed one"
                )
            # ARM 11. The observed value and its source, from closed sets.
            if observed is not None and (
                observed["value"] not in ("CreateTime", "LogAppendTime")
                or observed["source"] not in (
                    "dynamicTopicConfig",
                    "dynamicBrokerConfig",
                    "dynamicDefaultBrokerConfig",
                    "staticBrokerConfig",
                    "defaultConfig",
                    "unknown",
                )
            ):
                return (
                    f"config_coverage[{_rust_debug_str(topic)}].timestamp_type "
                    f"{_rust_debug_str(observed['value'])} from "
                    f"{_rust_debug_str(observed['source'])} is not a value and source this format "
                    "defines: the value is \"CreateTime\" or \"LogAppendTime\", and the source is "
                    "\"dynamicTopicConfig\", \"dynamicBrokerConfig\", "
                    "\"dynamicDefaultBrokerConfig\", \"staticBrokerConfig\", \"defaultConfig\" "
                    "or \"unknown\""
                )

    # ARMS 12-19 (format 1.3.0, PROD-05.1): the `topic_configuration` block,
    # and ONLY when it is present, so every earlier receipt is decided exactly
    # as before. Topics in NAME order, and per topic arm 15, then arms 16 and
    # 17 per entry in key order, then 18 and 19.
    model = doc.get("topic_configuration")
    if model is not None:
        # ARM 12. A document declaring a minor before 3 cannot carry a 1.3 field.
        if parsed[1] < RECEIPT_TOPIC_CONFIGURATION_SINCE_MINOR:
            return (
                f"topic_configuration is present but format_version {_rust_debug_str(version)} "
                "predates it: the field is defined from "
                f"1.{RECEIPT_TOPIC_CONFIGURATION_SINCE_MINOR}.0"
            )
        # ARM 13. The entries are judged against the read that produced them.
        coverage = doc.get("config_coverage")
        if coverage is None:
            return (
                f"topic_configuration is present under format_version {_rust_debug_str(version)} "
                "but config_coverage is not: a topic's configuration entries cannot be judged "
                "without the read that produced them"
            )
        # ARM 14. The modelled set is the named set — arms 3 and 7's twin.
        modelled = list(model.keys())
        if sorted(set(modelled)) != sorted(set(named_topics)):
            return (
                f"topic_configuration covers {_render_topic_set(modelled)} but the named topic "
                f"set is {_render_topic_set(named_topics)}"
            )
        for topic in sorted(model):
            entry = model[topic]
            entries = entry.get("entries")
            # ARM 15. Entries exactly where the read succeeded.
            read = coverage.get(topic)
            succeeded = read is not None and (
                read["coverage"] == "captured"
                or (read["coverage"] == "notCaptured" and read.get("reason") == "manifestDiffers")
            )
            if (entries is not None) != succeeded:
                if read is None:
                    rendered = "absent"
                elif read.get("reason") is not None:
                    rendered = _rust_debug_str(f"{read['coverage']}/{read['reason']}")
                else:
                    rendered = _rust_debug_str(read["coverage"])
                state = "present" if entries is not None else "absent"
                return (
                    f"topic_configuration[{_rust_debug_str(topic)}].entries {state} does not fit "
                    f"its config_coverage {rendered}: entries are recorded exactly when the "
                    "configuration read succeeded (\"captured\", or \"notCaptured\" with reason "
                    "\"manifestDiffers\")"
                )
            for key in sorted(entries or {}):
                config = entries[key]
                source = config["source"]
                portability = config["portability"]
                value = config.get("value")
                # ARM 16. The source and the class, from closed sets.
                if source not in RECEIPT_CONFIG_SOURCES or portability not in PORTABILITY_CLASSES:
                    return (
                        f"topic_configuration[{_rust_debug_str(topic)}].entries"
                        f"[{_rust_debug_str(key)}] source {_rust_debug_str(source)} and "
                        f"portability {_rust_debug_str(portability)} are not a source and class "
                        "this format defines: the source is \"dynamicTopicConfig\", "
                        "\"dynamicBrokerConfig\", \"dynamicDefaultBrokerConfig\", "
                        "\"staticBrokerConfig\", \"defaultConfig\" or \"unknown\", and the class "
                        "is \"portable\", \"inherited\", \"removedInKafka4\", \"clusterBound\", "
                        "\"requiresTieredStorage\", \"providerOnly\" or \"secret\""
                    )
                # ARM 17. A secret carries no value and nothing else lacks one;
                # otherwise `inherited` is exactly a value the topic did not set.
                secret = portability == "secret"
                if secret or value is None:
                    fits = secret and value is None
                else:
                    fits = (portability == "inherited") == (source != "dynamicTopicConfig")
                if not fits:
                    said = "a value" if value is not None else "no value"
                    return (
                        f"topic_configuration[{_rust_debug_str(topic)}].entries"
                        f"[{_rust_debug_str(key)}] is {_rust_debug_str(portability)} from "
                        f"{_rust_debug_str(source)} with {said}: an entry is \"secret\" exactly "
                        "when it carries no value, and otherwise \"inherited\" exactly when its "
                        "source is not \"dynamicTopicConfig\""
                    )
            # ARM 18. The owner, from closed sets, and a usable reference.
            owner = entry.get("owner")
            if owner is not None:
                kind = owner["kind"]
                basis = owner["basis"]
                reference = owner["reference"]
                kind_ok = kind in ("strimzi", "external")
                basis_ok = basis == "declared" or (
                    basis == "kafkaTopicResource" and kind == "strimzi"
                )
                reference_ok = (
                    bool(reference.strip(RUST_WHITESPACE))
                    and len(reference) <= 256
                    and not any(unicodedata.category(c) == "Cc" for c in reference)
                )
                if not (kind_ok and basis_ok and reference_ok):
                    return (
                        f"topic_configuration[{_rust_debug_str(topic)}].owner "
                        f"{_rust_debug_str(kind)} by {_rust_debug_str(basis)} is not an owner "
                        "this format defines: the kind is \"strimzi\" or \"external\", the basis "
                        "is \"kafkaTopicResource\" (for \"strimzi\" only) or \"declared\", and the "
                        "reference is 1 to 256 characters with no control character"
                    )
            # ARM 19. A recorded count is a count.
            partitions = entry.get("partitions")
            factor = entry.get("replication_factor")
            if partitions == 0 or factor == 0:
                shown_p = "absent" if partitions is None else str(partitions)
                shown_f = "absent" if factor is None else str(factor)
                return (
                    f"topic_configuration[{_rust_debug_str(topic)}] records partitions {shown_p} "
                    f"and replication_factor {shown_f}: a recorded count is at least 1"
                )

    # ARM 20 (format 1.3.0, PROD-05.1). Where the run looked for owners: only
    # beside the model it qualifies, from the closed set, each source at most
    # once.
    detection = doc.get("owner_detection")
    if detection is not None:
        seen = set()
        fits = model is not None
        for d in detection:
            if not fits:
                break
            fits = d in RECEIPT_OWNER_DETECTION_SOURCES and d not in seen
            seen.add(d)
        if not fits:
            return (
                f"owner_detection {_rust_debug_str_list(detection)} is not a detection this "
                "format defines: it is present only beside topic_configuration, and lists "
                "\"declared\" and \"kafkaTopicResources\" each at most once"
            )
    # ARM 21. An owner is recorded only from a source the run looked in. An
    # absent detection is an empty one.
    if model is not None:
        looked = detection if detection is not None else []
        for topic in sorted(model):
            owner = model[topic].get("owner")
            if owner is None:
                continue
            source = RECEIPT_OWNER_DETECTION_FOR_BASIS.get(owner["basis"])
            if source is None or source not in looked:
                return (
                    f"topic_configuration[{_rust_debug_str(topic)}].owner by "
                    f"{_rust_debug_str(owner['basis'])} names no source owner_detection "
                    f"{_rust_debug_str_list(looked)} lists: a \"declared\" owner needs "
                    "\"declared\", a \"kafkaTopicResource\" owner \"kafkaTopicResources\""
                )

    # ARMS 22-29 (format 1.5.0, PROD-03.0): the `schema_dependency` block, and
    # ONLY when it is present, so every earlier receipt is decided exactly as
    # before. Topics in NAME order, and per topic arms 24, 25 and 26, then 27
    # and 28 for the key side and then the value side, then 29.
    dependency = doc.get("schema_dependency")
    if dependency is not None:
        # ARM 22. A document declaring a minor before 5 cannot carry a 1.5 field.
        if parsed[1] < RECEIPT_SCHEMA_DEPENDENCY_SINCE_MINOR:
            return (
                f"schema_dependency is present but format_version {_rust_debug_str(version)} "
                "predates it: the field is defined from "
                f"1.{RECEIPT_SCHEMA_DEPENDENCY_SINCE_MINOR}.0"
            )
        # ARM 23. The judged set is the named set — arms 3, 7 and 14's twin.
        judged = list(dependency.keys())
        if sorted(set(judged)) != sorted(set(named_topics)):
            return (
                f"schema_dependency covers {_render_topic_set(judged)} but the named topic "
                f"set is {_render_topic_set(named_topics)}"
            )
        for topic in sorted(dependency):
            entry = dependency[topic]
            verdict = entry["verdict"]
            reason = entry.get("reason")
            basis = entry.get("basis")
            key = entry.get("key")
            value = entry.get("value")

            # ARM 24. The verdict, and a reason or a basis as it requires,
            # from closed sets.
            assessed = verdict != "notAssessed"
            if assessed:
                fits = reason is None and basis in SCHEMA_DEPENDENCY_BASES
            else:
                fits = basis is None and reason in SCHEMA_DEPENDENCY_REASONS
            if verdict not in SCHEMA_DEPENDENCY_VERDICTS or not fits:
                return (
                    f"schema_dependency[{_rust_debug_str(topic)}] verdict "
                    f"{_rust_debug_str(verdict)} with reason {_shown_or_absent(reason)} and basis "
                    f"{_shown_or_absent(basis)} is not a verdict this format defines: the verdict is "
                    "\"schemaDependent\", \"notDetected\" or \"notAssessed\"; a "
                    "\"notAssessed\" topic has a reason, \"noRecords\", "
                    "\"segmentUnreadable\", \"segmentTooLargeForDetection\" or "
                    "\"detectionTimeBudgetExceeded\", and no basis, and any other topic has "
                    "a basis, \"sampled\" or \"complete\", and no reason"
                )

            # ARM 25. Both sides exactly when the topic was judged, over the
            # same records, at least one.
            if key is not None and value is not None:
                fits = (
                    assessed
                    and _judged_records(key) == _judged_records(value)
                    and _judged_records(key) >= 1
                )
            elif key is None and value is None:
                fits = not assessed
            else:
                fits = False
            if not fits:
                return (
                    f"schema_dependency[{_rust_debug_str(topic)}] verdict "
                    f"{_rust_debug_str(verdict)} records key {_side_records_shown(key)} and value "
                    f"{_side_records_shown(value)}: a judged topic records a key side and a value side "
                    "over the same records, at least one, and a \"notAssessed\" topic records "
                    "neither"
                )

            # ARM 26. What was judged, against what the receipt counts.
            counted = doc["records"].get(topic, 0)
            judged_n = 0 if key is None else _judged_records(key)
            if basis == "complete":
                fits = judged_n == counted
            elif basis is not None:
                fits = judged_n <= counted
            elif reason == "noRecords":
                fits = counted == 0
            else:
                fits = True
            if not fits:
                under = _shown_or_absent(basis if basis is not None else reason)
                return (
                    f"schema_dependency[{_rust_debug_str(topic)}] judges {judged_n} records "
                    f"under {under} and records counts {counted}: a \"complete\" basis judges "
                    "every record the receipt counts, a \"sampled\" one at most that many, and "
                    "\"noRecords\" is said only of a topic that counts none"
                )

            for name, side in (("key", key), ("value", value)):
                if side is None:
                    continue
                ids = side["schema_ids"]
                count = side["schema_id_count"]
                framed = side["framed"]
                # ARM 27. The ids: distinct, ascending, plausible, as many as the
                # count allows, and a count that fits the framing.
                fits = (
                    all(a < b for a, b in zip(ids, ids[1:]))
                    and all(1 <= i <= SCHEMA_ID_MAX for i in ids)
                    and len(ids) == min(count, SCHEMA_IDS_LISTED)
                    and (count >= 1) == (framed >= 1)
                    and count <= framed
                )
                if not fits:
                    return (
                        f"schema_dependency[{_rust_debug_str(topic)}].{name} lists schema_ids "
                        f"{_u32_list_debug(ids)} with schema_id_count {count} and "
                        f"framed {framed}: the ids are distinct, ascending and from 1 to "
                        "16777215, all of them when the count is 16 or fewer and 16 otherwise, "
                        "and the count is at least 1 exactly when a record is framed and never "
                        "above the framed count"
                    )
                # ARM 28. The threshold: `dependent` is what the counts say.
                if side["dependent"] != _dependent_by_share(framed, side["unframed"]):
                    said = "dependent" if side["dependent"] else "not dependent"
                    return (
                        f"schema_dependency[{_rust_debug_str(topic)}].{name} is {said} with "
                        f"framed {framed} and unframed {side['unframed']}: a side is dependent "
                        "exactly when at least one record and at least one in ten of its "
                        "non-null records are framed"
                    )

            # ARM 29. The verdict is what the sides say.
            if key is not None and value is not None:
                dependent = key["dependent"] or value["dependent"]
                if (verdict == "schemaDependent") != dependent:
                    return (
                        f"schema_dependency[{_rust_debug_str(topic)}] verdict "
                        f"{_rust_debug_str(verdict)} does not fit its sides: a judged topic is "
                        "\"schemaDependent\" exactly when its key side or its value side is "
                        "dependent"
                    )

    # ARMS 30-35 (format 1.7.0, PROD-04.1): the `consumer_positions` block, and
    # ONLY when it is present. Groups in id order. The positions themselves
    # are in the document the block binds, checked by
    # `check_consumer_positions_document` (arms CP-1 to CP-14).
    cp = doc.get("consumer_positions")
    if cp is not None:
        # ARM 30. A document declaring a minor before 7 cannot carry a 1.7 field.
        if parsed[1] < RECEIPT_CONSUMER_POSITIONS_SINCE_MINOR:
            return (
                f"consumer_positions is present but format_version {_rust_debug_str(version)} "
                "predates it: the field is defined from "
                f"1.{RECEIPT_CONSUMER_POSITIONS_SINCE_MINOR}.0"
            )
        # ARM 31. The capture ends at or after it starts, the listing word is
        # closed, and at least one group is recorded.
        window_fits = _rfc3339_ns(cp["observed_to"]) >= _rfc3339_ns(cp["observed_from"])
        if not window_fits or cp["listing"] not in CP_LISTING_VALUES or not cp["groups"]:
            return (
                f"consumer_positions records listing {_rust_debug_str(cp['listing'])}, "
                f"{len(cp['groups'])} group(s) and a capture that "
                f"{'ends at or after it starts' if window_fits else 'ends before it starts'}: "
                "the capture ends at or after it starts, the listing is \"complete\" or "
                "\"notComplete\", and at least one group is recorded"
            )
        # ARM 32. The positions document is the one beside this receipt, named
        # by a well-formed digest over at least one byte.
        document = cp["document"]
        key = _cp_document_key(doc["backup_id"], doc["run_id"])
        digest = document["sha256"]
        digest_fits = (
            digest.startswith("sha256:")
            and len(digest) == 71
            and all(ch in "0123456789abcdef" for ch in digest[7:])
        )
        if document["key"] != key or not digest_fits or document["bytes"] == 0:
            return (
                f"consumer_positions.document is {_rust_debug_str(document['key'])} with sha256 "
                f"{_rust_debug_str(digest)} over {document['bytes']} bytes: the positions "
                f"document is {_rust_debug_str(key)}, its digest \"sha256:\" and 64 lowercase "
                "hex digits, over at least one byte"
            )
        shown = _cp_shown
        for group_id in sorted(cp["groups"]):
            group = cp["groups"][group_id]
            gid = _rust_debug_str(group_id)
            outcome = group["outcome"]
            reason = group.get("reason")
            # ARM 33. The outcome, and a reason exactly when it is not captured.
            if outcome == "captured":
                reason_fits = reason is None
            elif outcome == "excluded":
                reason_fits = reason in CP_EXCLUDED_REASONS
            elif outcome == "failed":
                reason_fits = reason in CP_FAILED_REASONS
            else:
                reason_fits = False
            if not reason_fits:
                return (
                    f"consumer_positions.groups[{gid}] has outcome {_rust_debug_str(outcome)} "
                    f"and reason {shown(reason)}: the outcome is \"captured\", \"excluded\" "
                    "or \"failed\", a reason is present exactly when it is not \"captured\", "
                    "and it is one this format defines for that outcome"
                )
            # ARM 34. What a group records follows from its outcome; a captured
            # group's counts cover at least one partition, and a group
            # described `Dead` with no member is never captured.
            captured = outcome == "captured"
            other = reason == "GroupTypeNotCaptured"
            group_type = group.get("group_type")
            state = group.get("state")
            listed = group.get("listed_state")
            members = group.get("members")
            active = group.get("active")
            counts = group.get("counts")
            if captured:
                fields_fit = (
                    group_type in CP_CAPTURED_TYPES
                    and state in CP_GROUP_STATES
                    and listed in CP_GROUP_STATES
                    and members is not None
                    and active is not None
                    and counts is not None
                    and _cp_counts_total(counts) >= 1
                    and not (state is not None and members is not None
                             and _cp_vanished(state, members))
                )
            else:
                only_type = (
                    state is None
                    and listed is None
                    and members is None
                    and active is None
                    and counts is None
                )
                ty = group_type == "other" if other else group_type is None
                fields_fit = only_type and ty
            counts_shown = (
                "absent" if counts is None else f"over {_cp_counts_total(counts)} partition(s)"
            )
            if not fields_fit:
                return (
                    f"consumer_positions.groups[{gid}] is {_rust_debug_str(outcome)} with "
                    f"group_type {shown(group_type)}, state {shown(state)}, listed_state "
                    f"{shown(listed)}, members {'absent' if members is None else members}, "
                    f"active {'absent' if active is None else _rust_bool(active)} and counts "
                    f"{counts_shown}: "
                    "a captured group records a type of \"classic\" or \"consumer\", both "
                    "states from the closed set, its members, active and counts over at least "
                    "one partition, and is never \"Dead\" with no member; a "
                    "GroupTypeNotCaptured group records group_type \"other\" and nothing else; "
                    "any other group records none of them"
                )
            if state is not None and listed is not None and active is not None:
                # ARM 35. `active` is what the two states say.
                derived = _cp_active(state, listed)
                if active != derived:
                    return (
                        f"consumer_positions.groups[{gid}].active is {_rust_bool(active)} but "
                        f"its states {_rust_debug_str(state)} and {_rust_debug_str(listed)} say "
                        f"{_rust_bool(derived)}: a group is active unless both its states are "
                        "\"Empty\" or \"Dead\""
                    )
    # ARMS 36-40 (format 1.6.0, PROD-01.4a): the `generations` block, and ONLY
    # when it is present, so every earlier receipt is decided exactly as
    # before. Topics in NAME order; per topic, arms 38, 39 and 40, each over
    # `topic_id` then `topic_id_after`.
    generations = doc.get("generations")
    if generations is not None:
        # ARM 36. A document declaring a minor before 6 cannot carry a 1.6 field.
        if parsed[1] < RECEIPT_GENERATIONS_SINCE_MINOR:
            return (
                f"generations is present but format_version {_rust_debug_str(version)} "
                "predates it: the field is defined from "
                f"1.{RECEIPT_GENERATIONS_SINCE_MINOR}.0"
            )
        # ARM 37. The observed set is the named set — arms 3, 7 and 14's twin.
        observed = list(generations.keys())
        if sorted(set(observed)) != sorted(set(named_topics)):
            return (
                f"generations covers {_render_topic_set(observed)} but the named topic set "
                f"is {_render_topic_set(named_topics)}"
            )
        for topic in sorted(generations):
            entry = generations[topic]
            reads = (
                ("topic_id", entry.get("topic_id"), entry.get("topic_id_reason")),
                (
                    "topic_id_after",
                    entry.get("topic_id_after"),
                    entry.get("topic_id_after_reason"),
                ),
            )
            # ARM 38. A recorded ID is Kafka's text form of a real ID.
            for field, topic_id, _ in reads:
                if topic_id is not None and not _is_canonical_topic_id(topic_id):
                    return (
                        f"generations[{_rust_debug_str(topic)}].{field} "
                        f"{_rust_debug_str(topic_id)} is not a topic ID this format defines: "
                        "22 characters of URL-safe base64 without padding over the ID's 16 "
                        "bytes, and never one of Kafka's reserved IDs (AAAAAAAAAAAAAAAAAAAAAA, "
                        "AAAAAAAAAAAAAAAAAAAAAQ)"
                    )
            # ARM 39. A reason exactly when the ID is null, from the closed set.
            for field, topic_id, reason in reads:
                if topic_id is not None:
                    fits = reason is None
                else:
                    fits = reason is not None and reason in RECEIPT_TOPIC_ID_REASONS
                if not fits:
                    rendered = "absent" if reason is None else _rust_debug_str(reason)
                    state = "recorded" if topic_id is not None else "null"
                    return (
                        f"generations[{_rust_debug_str(topic)}].{field}_reason {rendered} "
                        f"does not fit a {state} {field}: a reason is present exactly when "
                        "the ID is null, and is \"noTopicId\", \"notAuthorized\", "
                        "\"topicNotFound\", \"readFailed\", \"notRead\" or \"reservedTopicId\""
                    )
            # ARM 40. A source exactly when an ID is recorded, from the closed set.
            recorded = entry.get("topic_id") is not None or entry.get("topic_id_after") is not None
            source = entry.get("topic_id_source")
            if source is not None:
                fits = recorded and source in RECEIPT_TOPIC_ID_SOURCES
            else:
                fits = not recorded
            if not fits:
                rendered = "absent" if source is None else _rust_debug_str(source)
                return (
                    f"generations[{_rust_debug_str(topic)}].topic_id_source {rendered} does "
                    "not fit its IDs: a source is present exactly when an ID is recorded, and "
                    "is \"describeTopics\" or \"engineManifest\""
                )

    return ""


def _catalog_point_problem(doc) -> str:
    """The one check this script makes of a catalog point record's content, or
    "" when it holds — the twin of `logweir_core::topic_identity::
    refuse_copied_topic_ids`, with the same text (PROD-01.4a review M1): every
    topic ID the record copies (`topics[].identity.topic_id`,
    `.topic_id_after`) is a real topic ID in Kafka's text, never one of
    Kafka's reserved IDs."""
    topics = doc.get("topics") if isinstance(doc, dict) else None
    if not isinstance(topics, list):
        return ""
    for topic in topics:
        identity = topic.get("identity") if isinstance(topic, dict) else None
        if identity is None:
            continue
        name = topic.get("name")
        rendered = _rust_debug_str(name) if isinstance(name, str) else "?"
        for field in ("topic_id", "topic_id_after"):
            value = identity.get(field) if isinstance(identity, dict) else None
            if value is None:
                continue
            if not isinstance(value, str):
                return f"topics[{rendered}].identity.{field} is not a string"
            if not _is_canonical_topic_id(value):
                return (
                    f"topics[{rendered}].identity.{field} {_rust_debug_str(value)} is not a "
                    "topic ID this format defines: 22 characters of URL-safe base64 without "
                    "padding over the ID's 16 bytes, and never one of Kafka's reserved IDs "
                    "(AAAAAAAAAAAAAAAAAAAAAA, AAAAAAAAAAAAAAAAAAAAAQ)"
                )
    return ""


def check_consumer_positions_document(doc, raw, pd) -> str:
    """The positions document `pd` (its exact bytes `raw`) checked against the
    receipt `doc` that binds it, or "" when it holds — arms CP-1 to CP-14.

    ARM FOR ARM, IN ORDER, WITH `BackupReceipt::
    validate_consumer_positions_document`
    (crates/logweir-core/src/backup_receipt.rs), and with BYTE-IDENTICAL
    refusal text. Called only after the receipt's own arms accepted it and
    `_positions_document_shape(pd)` returned "".
    """
    # ARM CP-1. Only a receipt that selected groups binds a document.
    cp = doc.get("consumer_positions")
    if cp is None:
        return (
            f"the receipt of run {_rust_debug_str(doc['run_id'])} records no consumer_positions "
            "block, so it binds no positions document: only a backup that selected consumer "
            "groups writes one"
        )
    # ARM CP-2. The exact bytes the receipt's signature covers.
    digest = "sha256:" + hashlib.sha256(raw).hexdigest()
    if digest != cp["document"]["sha256"] or len(raw) != cp["document"]["bytes"]:
        return (
            f"the positions document is {digest} over {len(raw)} bytes but the receipt binds "
            f"{cp['document']['sha256']} over {cp['document']['bytes']} bytes: it is not the "
            "document this receipt signed"
        )
    # ARM CP-3. Format 1, for this receipt's own backup and run.
    parsed = _receipt_parse_semver(pd["format_version"])
    if (
        parsed is None
        or parsed[0] != 1
        or pd["backup_id"] != doc["backup_id"]
        or pd["run_id"] != doc["run_id"]
    ):
        return (
            f"the positions document is format {_rust_debug_str(pd['format_version'])} for "
            f"backup {_rust_debug_str(pd['backup_id'])} run {_rust_debug_str(pd['run_id'])} but "
            f"the receipt is backup {_rust_debug_str(doc['backup_id'])} run "
            f"{_rust_debug_str(doc['run_id'])}: a format-1 positions document names its "
            "receipt's own backup and run"
        )
    # ARM CP-4. The observed topics are the named topics.
    named_topics = [str(x) for x in doc["source"]["topics"]]
    observed = list(pd["topics"].keys())
    if sorted(set(observed)) != sorted(set(named_topics)):
        return (
            f"the positions document's topics cover {_render_topic_set(observed)} but the "
            f"named topic set is {_render_topic_set(named_topics)}"
        )
    changed = []
    unread = []
    facts_at = {}
    for topic in sorted(pd["topics"]):
        entry = pd["topics"][topic]
        for i, facts in enumerate(entry["partitions"]):
            # ARM CP-5. Every partition once, from 0, in order.
            if facts["partition"] != i:
                return (
                    f"the positions document's topics[{_rust_debug_str(topic)}].partitions[{i}] "
                    f"is partition {facts['partition']}: each topic lists its partitions from 0, "
                    "one entry each, in order"
                )
            # ARM CP-6. Marks and ranges are well formed.
            fits = (
                _cp_pair(facts.get("log_start"), facts.get("high_watermark"))
                and _cp_pair(facts.get("log_start_after"), facts.get("high_watermark_after"))
                and _cp_pair(facts.get("archived_first"), facts.get("archived_last"))
                and (
                    facts["observed"]
                    or (facts.get("log_start") is None and facts.get("high_watermark") is None)
                )
            )
            if not fits:
                return (
                    f"the positions document's topics[{_rust_debug_str(topic)}].partitions[{i}] "
                    "records marks that are not well formed: a log start and its high watermark "
                    "are recorded together with 0 <= log start <= high watermark, the archived "
                    "range is recorded whole with 0 <= first <= last, and a partition the "
                    "capture did not observe has no group-capture marks"
                )
            facts_at[(topic, facts["partition"])] = facts
        # ARM CP-7. `changed_during_capture` is what the marks say.
        derived = _cp_changed(entry["partitions"])
        if entry["changed_during_capture"] != derived:
            return (
                f"the positions document's topics[{_rust_debug_str(topic)}].changed_during_capture "
                f"is {_rust_bool(entry['changed_during_capture'])} but its marks say "
                f"{_rust_bool(derived)}: a topic changed during the capture exactly when a mark "
                "read after the engine is below the one read at group capture"
            )
        if derived:
            changed.append(topic)
        if not entry["partitions"]:
            unread.append(topic)
    # ARM CP-8. Positions for exactly the captured groups.
    captured = [g for g in cp["groups"] if cp["groups"][g]["outcome"] == "captured"]
    positioned = list(pd["groups"].keys())
    if sorted(set(positioned)) != sorted(set(captured)):
        return (
            f"the positions document records positions for the groups "
            f"{_render_topic_set(positioned)} but the receipt's captured groups are "
            f"{_render_topic_set(captured)}: it records exactly the captured groups"
        )
    shown = _cp_shown
    for group_id in sorted(cp["groups"]):
        group = cp["groups"][group_id]
        gid = _rust_debug_str(group_id)
        outcome = group["outcome"]
        reason = group.get("reason")
        positions = pd["groups"].get(group_id)
        # ARM CP-9. No kept position on a topic that changed, and no group
        # failed GenerationChangedDuringCapture when none changed.
        on_changed = any(
            e.get("position") is not None and e["topic"] in changed
            for e in (positions["positions"] if positions is not None else [])
        )
        blamed = reason == "GenerationChangedDuringCapture"
        if on_changed or (blamed and not changed):
            return (
                f"consumer_positions.groups[{gid}] is {_rust_debug_str(outcome)} with reason "
                f"{shown(reason)} while the topics that changed during the capture are "
                f"{_render_topic_set(changed)}: a group holding a position on such a topic "
                "fails GenerationChangedDuringCapture, and no group fails so when none changed"
            )
        # ARM CP-10. A group is captured only over topics whose partitions
        # were read.
        not_read = reason == "PartitionsNotRead"
        if (outcome == "captured" and unread) or (not_read and not unread):
            return (
                f"consumer_positions.groups[{gid}] is {_rust_debug_str(outcome)} with reason "
                f"{shown(reason)} while the topics whose partitions were never read are "
                f"{_render_topic_set(unread)}: a group is captured only when every named "
                "topic's partitions were read, and fails PartitionsNotRead only when one was not"
            )
        if positions is None:
            continue
        # ARM CP-11. The entries name partitions of the named topics, in order,
        # once each, every unobserved one among them; every other partition is
        # counted as having no committed position.
        entries = positions["positions"]
        out_of_place = None
        for i, e in enumerate(entries):
            here = (e["topic"], e["partition"])
            if here not in facts_at or (
                i > 0 and (entries[i - 1]["topic"], entries[i - 1]["partition"]) >= here
            ):
                out_of_place = i
                break
        listed = {(e["topic"], e["partition"]) for e in entries}
        unobserved_missing = sum(
            1 for at, f in facts_at.items() if not f["observed"] and at not in listed
        )
        total = len(facts_at)
        if (
            out_of_place is not None
            or unobserved_missing > 0
            or len(entries) + positions["no_committed_position"] != total
        ):
            first = (
                "none"
                if out_of_place is None
                else f"{out_of_place}, "
                + _cp_place(entries[out_of_place]["topic"], entries[out_of_place]["partition"])
            )
            return (
                f"the positions document's groups[{gid}] lists {len(entries)} position(s) "
                f"(first out of place: {first}), leaves {unobserved_missing} unobserved "
                f"partition(s) out and counts {positions['no_committed_position']} without a "
                f"committed position over {total} partition(s): a captured group lists, topics "
                "in name order and partitions in order, each partition of a named topic at most "
                "once and every one the capture did not observe, and counts every other "
                "partition as without a committed position"
            )
        for i, entry in enumerate(entries):
            facts = facts_at[(entry["topic"], entry["partition"])]
            status = entry["status"]
            position = entry.get("position")
            why = entry.get("reason")
            # ARM CP-12. The status, the position and the reason fit.
            if status == "excluded":
                reason_ok = why == "PositionBeyondEnd"
            elif status == "failed":
                reason_ok = why in CP_POSITION_FAILED_REASONS
            elif status == "notObserved":
                reason_ok = why in CP_NOT_OBSERVED_REASONS
            else:
                reason_ok = why is None
            fits = (
                status in CP_POSITION_STATUSES
                and (position is not None) == (status in ("captured", "excluded"))
                and (position is None or position >= 0)
                and reason_ok
                and (status == "notObserved") == (not facts["observed"])
            )
            if not fits:
                return (
                    f"the positions document's groups[{gid}].positions[{i}] has status "
                    f"{_rust_debug_str(status)}, position "
                    f"{'absent' if position is None else position} and reason {shown(why)}: the "
                    "status is \"captured\", \"excluded\", \"failed\" or \"notObserved\", a "
                    "position is present exactly when it is \"captured\" or \"excluded\" and is "
                    "never negative, a reason exactly when it is not \"captured\" and from that "
                    "status's set, and \"notObserved\" is exactly a partition the capture did "
                    "not observe"
                )
            # ARM CP-13. A coverage word exactly on a captured position, and a
            # kept position's verdict follows from its partition's facts.
            coverage = entry.get("coverage")
            derived = None if position is None else _cp_relation(position, facts)
            if status == "excluded":
                recorded = "PositionBeyondEnd"
            elif status == "captured":
                recorded = coverage
            else:
                recorded = None
            coverage_fits = (coverage is not None) == (status == "captured")
            if not coverage_fits or recorded != derived:
                return (
                    f"the positions document's groups[{gid}].positions[{i}] is "
                    f"{_rust_debug_str(status)} with coverage {shown(coverage)} at position "
                    f"{'absent' if position is None else position}, but its partition's facts "
                    f"make it {'unjudged' if derived is None else derived}: a coverage word is "
                    "recorded exactly on a captured position, and a kept position's coverage, "
                    "or its PositionBeyondEnd, follows from the marks and the archived range"
                )
        # ARM CP-14. The receipt's counts are the document's.
        derived = _cp_counts_of(positions)
        recorded = group.get("counts")
        if recorded is None or any(recorded[n] != derived[n] for n in CP_COUNT_NAMES):
            return (
                f"consumer_positions.groups[{gid}].counts are "
                f"{'absent' if recorded is None else _cp_counts_render(recorded)} but its "
                f"positions count {_cp_counts_render(derived)}: the receipt counts what the "
                "positions document records"
            )

    return ""


def _rust_debug_str_list(items):
    """A `Vec<String>` as Rust's `{:?}` renders it: `["a", "b"]`, `[]`."""
    return "[" + ", ".join(_rust_debug_str(i) for i in items) + "]"


def _coverage_lines(block):
    """The receipt's `config_coverage`, one line per topic in NAME order, or the
    line that says it is absent — the twin of `crates/logweir/src/verify.rs::
    coverage_lines`, in the same words (FX-4)."""
    if block is None:
        return [
            "config_coverage: not recorded, so every topic's configuration capture is "
            "UNKNOWN, never captured"
        ]
    lines = []
    for topic in sorted(block):
        entry = block[topic]
        coverage = entry["coverage"]
        if entry.get("reason") is not None:
            coverage = f"{coverage} ({entry['reason']})"
        observed = entry.get("timestamp_type")
        if observed is not None:
            timestamp = f"message.timestamp.type {observed['value']} from {observed['source']}"
        else:
            timestamp = "message.timestamp.type not recorded"
        lines.append(f"config_coverage[{_rust_debug_str(topic)}]: {coverage}, {timestamp}")
    return lines


def _generation_lines(block):
    """The receipt's `generations`, one line per topic in NAME order, or the
    line that says it is absent — the twin of `crates/logweir/src/verify.rs::
    generation_lines`, in the same words (PROD-01.4a). An unknown ID is never
    read as "the same": it says why, and that the generation is not
    established by ID."""
    if block is None:
        return [
            "generations: not recorded, so no topic ID is known from this receipt and each "
            "topic's generation is UNKNOWN, never the same as another point's"
        ]

    def side(topic_id, reason):
        if topic_id is not None:
            return topic_id
        if reason is not None:
            return f"not recorded ({reason})"
        return "not recorded"

    lines = []
    for topic in sorted(block):
        entry = block[topic]
        before = entry.get("topic_id")
        after = entry.get("topic_id_after")
        if before is not None and after is not None and before == after:
            source = entry.get("topic_id_source")
            said = (
                f"topic ID {before} before and after the capture "
                f"({source if source is not None else 'no source'}), one generation"
            )
        elif before is not None and after is not None:
            said = (
                f"topic ID CHANGED during the capture ({before} before, {after} after): the "
                "topic was deleted and recreated while it ran, so this point mixes two "
                "generations"
            )
        else:
            said = (
                f"topic ID {side(before, entry.get('topic_id_reason'))} before the capture and "
                f"{side(after, entry.get('topic_id_after_reason'))} after it, so its "
                "generation is not established by ID and is UNKNOWN"
            )
        lines.append(f"generations[{_rust_debug_str(topic)}]: {said}")
    return lines


def _topic_configuration_lines(block, detection=None):
    """The receipt's `topic_configuration`, one line per topic in NAME order,
    or the line that says it is absent — the twin of `crates/logweir/src/
    verify.rs::topic_configuration_lines`, in the same words (PROD-05.1).
    Counts and classes, never a configuration value.

    `detection` is the receipt's `owner_detection` (absent reads as empty): a
    topic without an owner is applied through the admin API only where the
    run looked for one, and otherwise its owner was not checked."""
    looked = detection if detection is not None else []
    if block is None:
        return [
            "topic_configuration: not recorded, so no topic's partition count, replication "
            "factor or settings are known to a restore from this receipt"
        ]

    def count(n):
        return "not recorded" if n is None else str(n)

    lines = []
    for topic in sorted(block):
        model = block[topic]
        entries = model.get("entries")
        if entries is None:
            said = "entries not recorded"
        else:
            by_class = []
            for cls in PORTABILITY_CLASSES:
                n = sum(1 for e in entries.values() if e["portability"] == cls)
                if n > 0:
                    by_class.append(f"{cls} {n}")
            said = f"{len(entries)} entries"
            if by_class:
                said += f" ({', '.join(by_class)})"
        owner = model.get("owner")
        if owner is None and not looked:
            route = "owner not checked, so how it is applied is not known"
        elif owner is None:
            route = (
                f"no declarative owner found ({', '.join(looked)}), so applied through the "
                "admin API"
            )
        else:
            route = (
                f"owned by {owner['kind']} ({owner['basis']} "
                f"{_rust_debug_str(owner['reference'])}), so restored by desired-state export"
            )
        lines.append(
            f"topic_configuration[{_rust_debug_str(topic)}]: partitions "
            f"{count(model.get('partitions'))}, replication factor "
            f"{count(model.get('replication_factor'))}, {said}, {route}"
        )
    return lines


def _consumer_positions_lines(block, verified=None):
    """The receipt's `consumer_positions`: a header, the positions document and
    whether it was verified, one line per group in id order and — only for a
    VERIFIED document — one line per listed position of each captured group
    and one with how many partitions have no committed position. The twin of
    `crates/logweir/src/verify.rs::consumer_positions_lines`, in the same
    words (PROD-04.1). Nothing when the receipt carries no block: the backup
    selected no group."""
    if block is None:
        return []
    document = block["document"]
    lines = [
        f"consumer_positions: {len(block['groups'])} group(s), listing {block['listing']}",
        f"consumer_positions: positions document {document['key']} ({document['sha256']}, "
        f"{document['bytes']} bytes) "
        + (
            "verified against this receipt"
            if verified is not None
            else "not checked: pass --consumer-positions <file> to verify it and print each "
            "position"
        ),
    ]
    for group_id in sorted(block["groups"]):
        g = block["groups"][group_id]
        c = g.get("counts")
        if g["outcome"] == "captured" and c is not None:
            members = g.get("members") if g.get("members") is not None else 0
            active = "inactive" if g.get("active") is False else "active"
            lines.append(
                f"consumer_positions[{_rust_debug_str(group_id)}]: captured "
                f"{g.get('group_type') or ''}, state {g.get('state') or ''} (listed "
                f"{g.get('listed_state') or ''}), {members} member(s), {active}; positions: "
                f"{c['related']} related to archived data, {c['not_related']} not related, "
                f"{c['never_committed']} never committed, {c['beyond_end']} beyond the end, "
                f"{c['failed']} failed, {c['not_observed']} not observed"
            )
        else:
            other = ", group type other" if g.get("group_type") == "other" else ""
            lines.append(
                f"consumer_positions[{_rust_debug_str(group_id)}]: {g['outcome']} "
                f"({g.get('reason') or ''}){other}, no position recorded"
            )
    if verified is None:
        return lines
    for group_id in sorted(verified["groups"]):
        g = verified["groups"][group_id]
        for e in g["positions"]:
            at = (
                f"consumer_positions[{_rust_debug_str(group_id)}]"
                f"[{_cp_place(e['topic'], e['partition'])}]"
            )
            position = e.get("position")
            if e["status"] == "captured" and position is not None:
                lines.append(f"{at}: position {position}, {e.get('coverage') or ''}")
            elif position is not None:
                lines.append(f"{at}: {e['status']} ({e.get('reason') or ''}), position {position}")
            else:
                lines.append(f"{at}: {e['status']} ({e.get('reason') or ''}), no position")
        lines.append(
            f"consumer_positions[{_rust_debug_str(group_id)}][*]: "
            f"{g['no_committed_position']} other partition(s) with no committed position, "
            "never offset 0"
        )
    return lines


def _schema_dependency_lines(block):
    """The receipt's `schema_dependency`, one line per topic in NAME order, or
    the line that says it is absent — the twin of `crates/logweir/src/
    verify.rs::schema_dependency_lines`, in the same words (PROD-03.0). Absent
    is NOT ASSESSED, never "not schema-dependent"."""
    if block is None:
        return [
            "schema_dependency: not assessed, so whether any topic's records need a schema "
            "registry is not known from this receipt"
        ]

    def side_words(name, side):
        out = f"{name} framed {side['framed']} of {side['framed'] + side['unframed']} non-null"
        if side["dependent"]:
            out += ", dependent"
        ids = side["schema_ids"]
        if ids:
            out += ", schema ids " + ", ".join(str(i) for i in ids)
            more = max(0, side["schema_id_count"] - len(ids))
            if more > 0:
                out += f" and {more} more"
        return out

    lines = []
    for topic in sorted(block):
        entry = block[topic]
        verdict = entry["verdict"]
        if verdict == "schemaDependent":
            said = "schema-dependent, registry not captured"
        elif verdict == "notDetected":
            said = "no schema framing detected"
        else:
            reason = entry.get("reason")
            said = f"not assessed ({reason if reason is not None else 'no reason recorded'})"
        key = entry.get("key")
        value = entry.get("value")
        if key is not None and value is not None:
            basis = entry.get("basis")
            lines.append(
                f"schema_dependency[{_rust_debug_str(topic)}]: {said}; "
                f"{_judged_records(key)} records judged "
                f"({basis if basis is not None else 'no basis recorded'}); "
                f"{side_words('key', key)}; {side_words('value', value)}"
            )
        else:
            lines.append(f"schema_dependency[{_rust_debug_str(topic)}]: {said}")
    return lines


def _verification_lines(block):
    """`integrity.verification` as lines -- the twin of `crates/logweir/src/
    verify.rs::verification_lines` (PROD-08.1), in the same words from the
    same cases. ABSENT is NOT RECORDED (every scorecard before 1.4.0) and read
    as a sample, never as complete."""
    if block is None:
        return [
            "integrity coverage: not recorded, so this verdict covered a sample, never every "
            "record"
        ]
    lines = [
        f"integrity coverage: {block['coverage']} (compared with the "
        f"{block['comparison_basis']}; header order {block['header_order']}; application "
        f"validation {block['application']})"
    ]
    c = block.get("complete")
    if c is not None:
        r, a = c["replay"], c["archive"]
        lines.append(
            f"integrity coverage: "
            f"{'every selected record compared' if c['covered'] else 'INCOMPLETE'}: "
            f"{r['expected']} expected, {r['restored']} restored, {r['matching']} matching, "
            f"{r['missing']} missing, {r['unexpected']} unexpected, {r['duplicates']} "
            f"duplicates, {r['out_of_order']} out of order, {r['mismatched']} different; "
            f"{a['segments_verified']} of {a['segments']} segments verified, "
            f"{len(a['segments_failed'])} failed, {len(a['segments_unverified'])} unverified; "
            f"{a['offset_holes']} offset holes"
        )
        if c.get("incomplete_reason") is not None:
            lines.append(f"integrity coverage: incomplete because {c['incomplete_reason']}")
    if block["gaps"] or block["pruned"]:
        lines.append(
            f"integrity coverage: the verified partitions record {len(block['gaps'])} capture "
            f"gaps and {len(block['pruned'])} pruned ranges"
        )
    return lines


def _sampled_pass_lines(doc):
    """What a SAMPLED `pass` proves at this document's version -- the twin of
    `crates/logweir/src/verify.rs::sampled_pass_lines` (FX-23 review M2), in
    the same words. Only a build with FX-23's checks writes 1.6.0; a 1.4.0 or
    1.5.0 document is the same bytes whichever build signed it. Nothing for a
    non-pass or a complete verification."""
    block = doc["integrity"].get("verification")
    sampled = block is None or block.get("coverage") == "sampled"
    if doc.get("outcome") != "pass" or not sampled:
        return []
    # PROD-11.1 (review H1): over a narrowed restore the guarantee is
    # QUALIFIED by the selection, in the Rust reader's words
    # (`sampled_pass_lines_over`): a 2.0.0 partition subset (PROD-11.1b), or
    # a 1.7.0 window start.
    start_clause = (
        "no record before the start was expected, and a sampled check does not prove that "
        "none was restored"
    )
    window = doc["source"].get("selection")
    if _narrows_partitions(window):
        start = window.get("window_start_ms")
        frm = "the archive's floor" if start is None else f"epoch-ms {start}"
        line = (
            f"sample coverage: a sampled pass over a partition subset from {frm} to epoch-ms "
            f"{window['window_end_ms']}: every selected partition was held to its own count "
            "bound over that window, every other partition of a narrowed topic was held empty, "
            "max_partitions reached every topic before a second partition of any, and a "
            "readable engine report lacking a selected partition with records in that window "
            "was refused"
        )
        if start is not None:
            line += "; " + start_clause
        return [line]
    if window is not None:
        return [
            f"sample coverage: a sampled pass over a replay selection from epoch-ms "
            f"{window.get('window_start_ms') or 0} to epoch-ms {window['window_end_ms']}: every "
            "mapped partition was held to its own count bound over that window, max_partitions "
            "reached every topic before a second partition of any, and a readable engine "
            "report lacking a partition with records in that window was refused; "
            + start_clause
        ]
    version = doc.get("format_version")
    # A 2.x document is written only by a build with FX-23's checks
    # (`proves_fx23_sampled_checks`).
    if _defines_format_1_minor(version, SCORECARD_UNSAMPLED_TOPICS_SINCE_MINOR):
        return [
            "sample coverage: a sampled pass at format 1.6.0 or later: every mapped partition "
            "was held to its own count bound, max_partitions reached every topic before a "
            "second partition of any, and a readable engine report lacking a partition with "
            "records in the window was refused"
        ]
    return [
        f"sample coverage: a sampled pass at format {version}, before 1.6.0: it proves the "
        "canary and one count bound over every topic together, not a per-partition count "
        "bound, a sample of every topic or an engine-report check (a build from before FX-23 "
        "may have signed it)"
    ]


def _before_the_start(doc):
    """What a document from a stated start proves about the records BEFORE
    that start -- the twin of `BeforeTheStart::of` in `crates/logweir-core/
    src/scorecard.rs` (PROD-11.1 review N1), in the same words. Only a
    COMPLETE verification whose `integrity.result` is `pass` shows none was
    restored (a restored record below the start is `unexpected` there, and
    IV-6 holds a complete pass to none); a SAMPLED check cannot (its sample is
    drawn from the window, and a segment straddling the start counts all of
    its records into the bound); anything else says only that none was
    expected."""
    block = doc["integrity"].get("verification")
    coverage = None if block is None else block.get("coverage")
    if coverage == "complete" and doc["integrity"].get("result") == "pass":
        return "no record before the start was restored or expected"
    if coverage == "sampled":
        return (
            "no record before the start was expected; a sampled check does not prove that "
            "none was restored"
        )
    return "no record before the start was expected"


def _outside_the_subset(doc):
    """What a 2.0.0 document proves about the OTHER partitions of a narrowed
    topic -- the twin of `OutsideTheSubset::of` in `crates/logweir-core/src/
    scorecard.rs` (PROD-11.1b), in the same words. A verification whose
    `integrity.result` is `pass` shows none of them holds a restored record,
    on either lane (the sampled lane holds them empty; the complete lane
    counts a record there as unexpected, and IV-6 refuses the pass);
    anything else shows only that none was expected."""
    block = doc["integrity"].get("verification")
    if block is not None and doc["integrity"].get("result") == "pass":
        return "no record of another partition of these topics was restored or expected"
    return "no record of another partition of these topics was expected"


def _selection_lines(block, before, outside):
    """`source.selection` as lines -- the twin of `crates/logweir/src/
    verify.rs::selection_lines` (PROD-11.1): the writer's sentence
    (`SelectionLabel::sentence`), ending in what this document proves -- of
    the other partitions of a narrowed topic (`outside`, `_outside_the_subset`,
    a 2.0.0 block) and of the records before a stated start (`before`,
    `_before_the_start`, review N1). Absent prints nothing: the restore
    selected every record from the archive's floor."""
    if block is None:
        return []
    start = block.get("window_start_ms")
    end = block["window_end_ms"]
    if not _narrows_partitions(block):
        # Format 1.7.0, a start only: the 1.23.0 sentence, byte for byte.
        return [
            f"replay selection: every partition of every restored topic, from epoch-ms "
            f"{start or 0} (the plan's stated window start, inclusive) to epoch-ms {end} "
            f"(inclusive); {before}"
        ]
    named = "; ".join(
        f"{e['topic']} partitions [{', '.join(str(p) for p in e['partitions'])}]"
        for e in block["partitions"]
    )
    frm = (
        "from the archive's floor"
        if start is None
        else f"from epoch-ms {start} (the plan's stated window start, inclusive)"
    )
    line = (
        f"replay selection: ONLY {named} (every partition of any other restored topic), {frm} "
        f"to epoch-ms {end} (inclusive), in {block.get('engine_runs') or 0} engine run(s); "
        f"{outside}"
    )
    if start is not None:
        line += f"; {before}"
    return [line]


def _original_name_lines(block):
    """`target.original_name` as lines -- the twin of `crates/logweir/src/
    verify.rs::original_name_lines` (PROD-15.1): the writer's two sentences
    (`OriginalNameInfo::lines`). Absent prints nothing: the restore did not
    write under the original topic names."""
    if block is None:
        return []
    source = block.get("source_cluster_id")
    if block["cluster_condition"] == "targetIsNotSource":
        cluster = f"the target cluster is not the source cluster ({source or 'unknown'})"
    elif source is not None:
        cluster = (
            f"the target may be the source cluster ({source}) and every broker reported "
            "auto.create.topics.enable=false"
        )
    else:
        cluster = (
            "no source cluster id was known and every broker reported "
            "auto.create.topics.enable=false"
        )
    owners = block["owners"]
    found = (
        ", ".join(
            f"{o['topic']} ({o['kind']} {o['reference']}, from {o['found_in']})" for o in owners
        )
        if owners
        else "none found"
    )
    confirmed = (
        " (the requester re-typed every original topic name)"
        if block.get("confirmation") == "typedTopicNames"
        else ""
    )
    digest = block.get("kafka_topic_resources_sha256")
    return [
        "original name: restored under the source's own topic names, into topics this run "
        "created (a new generation of each name, not the original topic); approval subject "
        f"{block['approval_subject']}, approved by {block['approval_mode']}{confirmed}; {cluster}",
        f"original name: declarative owners looked for in {', '.join(block['owner_detection'])}: "
        f"{found}"
        + ("; the approved plan chose the owner path" if block["owner_path"] else "")
        + (f"; KafkaTopic resources {digest}" if digest is not None else ""),
    ]


def _console_approval_lines(block):
    """`approval.console` as a line -- the twin of `crates/logweir/src/
    verify.rs::console_approval_lines` (PROD-16.2): the writer's sentence
    (`ConsoleApprovalInfo::lines`), which says WHO approved and HOW: the mode,
    who asked and when, who approved and when, when the request would have
    expired, and that the console key signed both documents, which is expected
    in this mode and no other. Absent prints nothing: no second person
    approved this run in the console. Called only on a document whose
    invariants hold, so both principals are bounded, visible ASCII (arm CA-3).
    Instants are printed in UTC, to the precision the document carries."""
    if block is None:
        return []
    requester, approver = block["requester"], block["approver"]
    return [
        f"console approval: mode {block['mode']}; requested by "
        f"{requester['issuer']}#{requester['subject']} at "
        f"{_instant_shown(_instant(block['requested_at']))}; approved in the console by "
        f"{approver['issuer']}#{approver['subject']} at "
        f"{_instant_shown(_instant(block['approved_at']))} (the request expired at "
        f"{_instant_shown(_instant(block['request_expires_at']))}); the console key "
        f"{block['confirmation_key_id']} signed the request and the approval, which is expected "
        "in this mode: no personal key is involved"
    ]


def _unsampled_lines(topics):
    """`sample.unsampled_topics` as lines -- the twin of `crates/logweir/src/
    verify.rs::unsampled_lines` (FX-23), in the same words. Absent or empty
    prints nothing: absent names no unsampled topic."""
    if not topics:
        return []
    return [
        f"sample coverage: no partition of {len(topics)} topic(s) was sampled, because "
        "sample.max_partitions is below the number of topics with records in the window: "
        f"{', '.join(topics)}; their partitions were held to the count bound only, never "
        "reconciled record by record"
    ]


def _time_basis_lines(block):
    """`source.time_basis` as lines -- the twin of `crates/logweir/src/
    verify.rs::time_basis_lines` (FX-8), in the same words from the same cases.
    ABSENT is NOT RECORDED (every scorecard before 1.3.0), never "every
    selection used the topic's own clock"; an empty block prints nothing."""
    if block is None:
        return [
            "time basis: not recorded, so whether a time selection read a LogAppendTime "
            "topic's producer timestamps is unknown"
        ]
    lines = []
    if block["producer_time"]:
        lines.append(
            "time basis: SELECTED BY PRODUCER TIME for "
            + ", ".join(block["producer_time"])
            + " (recorded as LogAppendTime; the approved plan states restore.time_basis: "
            "producerTime)"
        )
    if block["not_recorded"]:
        lines.append(
            "time basis: timestamp type NOT RECORDED for "
            + ", ".join(block["not_recorded"])
            + ", so its time selection may have read producer timestamps"
        )
    return lines


def _receipt_time_basis_lines(block):
    """The backup receipt's LogAppendTime topics, one line each in NAME order --
    the twin of `crates/logweir/src/verify.rs::receipt_time_basis_lines` (FX-8).
    The receipt's format is unchanged: its covered window reads those topics'
    producer timestamps, and this says so."""
    lines = []
    for topic in sorted(block or {}):
        observed = block[topic].get("timestamp_type")
        if observed is not None and observed.get("value") == "LogAppendTime":
            lines.append(
                f"time basis: {_rust_debug_str(topic)} is LogAppendTime, so the archive holds "
                "its producers' timestamps and the covered window reads them; a restore that "
                "selects it by time is refused unless its plan states restore.time_basis: "
                "producerTime"
            )
    return lines


def _parity_line(not_assessed):
    """`topic_parity.not_assessed` as one sentence, or "" when every topic was
    assessed — the twin of `crates/logweir/src/verify.rs::parity_line` (FX-4).
    ABSENT is NOT RECORDED (every 1.0.0 scorecard, and a 1.1.0 one whose drill
    stopped before phase 7), never "every topic assessed". Every entry is named
    as written, FX-21's `replication_factor (notRecorded)` included."""
    if not_assessed is None:
        return (
            "configuration parity: not recorded, so an empty unexpected_divergence proves "
            "nothing"
        )
    if not not_assessed:
        return ""
    return "configuration parity: NOT ASSESSED for " + "; ".join(str(t) for t in not_assessed)


def _reconstruction_line(mode, not_reconstructed, intentionally_deviated):
    """The scorecard's reconstruction sentence, or "" -- the twin of
    `crates/logweir/src/verify.rs::reconstruction_line` (FX-3), in the same
    words from the same three cases. `mode` is `target.mode` as read: absent
    is scratch. A newTopic document WITHOUT `not_reconstructed` predates format
    1.2.0, and its writer labelled the settings it did not reconstruct
    `intentionally_deviated`; that is re-read here as the weaker claim, never
    a stronger one, and changes no verdict."""
    if not_reconstructed is not None:
        if not not_reconstructed:
            return ""
        return "reconstruction: source settings NOT RECONSTRUCTED for " + "; ".join(
            str(e) for e in not_reconstructed
        )
    if mode == "newTopic" and intentionally_deviated:
        return (
            "reconstruction: not recorded, so the settings this newTopic document labels "
            "intentionally_deviated were NOT reconstructed: "
            + "; ".join(str(e) for e in intentionally_deviated)
        )
    return ""


def main(
    scorecard_path: str,
    sig_path: str,
    pubkey_path: str,
    payload_type_wanted: str = PAYLOAD_TYPE,
    positions_path=None,
) -> int:
    # PROD-04.1: the positions document is checked against the backup receipt
    # that binds it, and against nothing else.
    if positions_path is not None and payload_type_wanted != PAYLOAD_TYPES["backup-receipt"]:
        print(
            "INVALID: --consumer-positions applies to --payload-type backup-receipt only: the "
            "positions document is checked against the backup receipt that binds it",
            file=sys.stderr,
        )
        return 1
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
        # PROD-16.2: who approved and HOW, when a second person approved in
        # the console -- the approval's key is then the console's, which this
        # line says is expected in this mode. The same line `logweir drill
        # verify` prints (`crates/logweir/src/verify.rs::console_approval_lines`);
        # nothing for a document without the block.
        for line in _console_approval_lines(doc["approval"].get("console")):
            print(f"       approval: {line}")
        # The four `evidence` fields are zeroed BEFORE signing, because they
        # describe an upload that has not happened yet. Say so, so nobody reads
        # the zeroes as a finding about their bucket.
        print(
            "       evidence: the four post-put fields are zeroed before signing; "
            "the storage facts live in the receipt"
        )
        # THE OFFSET REPORT, read PRESENCE-TOLERANTLY (Task 9b). Two nested
        # optional fields: absent means this run recorded no offset-mapping
        # report, which is the truthful answer for a run that signed a document
        # without having completed a restore. Printed only when present, so an
        # older run's output is byte-identical to what it was — and the DIGEST
        # is printed beside the key, because a key alone tells an auditor where
        # to look and not whether what they find is what was signed.
        offsets_key = str(doc["evidence"].get("offset_report_key") or "").strip(RUST_WHITESPACE)
        if offsets_key:
            print(f"       offsets:  {offsets_key}")
            print(
                f"                 {doc['evidence'].get('offset_report_sha256')} "
                "— the engine's offset MAPPING, uploaded as evidence and applied to nothing"
            )
        # FX-4: an exit 0 is not configuration parity the document does not
        # claim. The same sentence `logweir drill verify` prints
        # (`crates/logweir/src/verify.rs::parity_line`).
        parity = _parity_line(doc["topic_parity"].get("not_assessed"))
        if parity:
            print(f"       parity:   {parity}")
        # FX-3: nor is it a restore that reconstructed the source's settings.
        # The same sentence `logweir drill verify` prints
        # (`crates/logweir/src/verify.rs::reconstruction_line`).
        reconstruction = _reconstruction_line(
            doc["target"].get("mode"),
            doc["topic_parity"].get("not_reconstructed"),
            doc["topic_parity"]["intentionally_deviated"],
        )
        if reconstruction:
            print(f"       parity:   {reconstruction}")
        # FX-8: nor a selection by the source topics' own clocks it does not
        # claim. The same lines `logweir drill verify` prints
        # (`crates/logweir/src/verify.rs::time_basis_lines`).
        for line in _time_basis_lines(doc["source"].get("time_basis")):
            print(f"       time:     {line}")
        # PROD-08.1: nor a verdict over every record when it covered a sample.
        # The same lines `logweir drill verify` prints
        # (`crates/logweir/src/verify.rs::verification_lines`).
        for line in _verification_lines(doc["integrity"].get("verification")):
            print(f"       coverage: {line}")
        # FX-23: what a sampled pass proves at this document's version (review
        # M2), and the topics the cap left out. The same lines `logweir drill
        # verify` prints (`crates/logweir/src/verify.rs::sampled_pass_lines`,
        # `unsampled_lines`).
        for line in _sampled_pass_lines(doc) + _unsampled_lines(
            doc["sample"].get("unsampled_topics")
        ):
            print(f"       coverage: {line}")
        # PROD-11.1: the replay selection a narrowed restore restored, in the
        # words `logweir drill verify` prints (`selection_lines`).
        for line in _selection_lines(
            doc["source"].get("selection"), _before_the_start(doc), _outside_the_subset(doc)
        ):
            print(f"       coverage: {line}")
        # PROD-15.1: a restore under the original topic names, and what
        # admitted it, in the words `logweir drill verify` prints
        # (`original_name_lines`).
        for line in _original_name_lines(doc["target"].get("original_name")):
            print(f"       target:   {line}")
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
            "format defines when the block is, or from 1.5.0 one of the five; "
            "evidence.offset_report_key and its sha256 present or absent together; "
            "target.marker_topic present unless target.mode is newTopic; "
            "target.mode absent or one of the two values the format defines; "
            "topic_parity.not_reconstructed only from 1.2.0, each entry also an unexpected "
            "divergence and never an intended one, and in a newTopic document nothing intended "
            "and every decided setting's divergence named in it; "
            "source.time_basis only from 1.3.0, its plan only producerTime, producer time "
            "only under it, and no topic in both of its lists; "
            "integrity.verification only from 1.4.0, its coverage sampled or complete, "
            "header order verified only for complete coverage, its complete block exactly "
            "with complete coverage, an incomplete reason exactly when not covered, a pass "
            "only over a covered and clean complete block, and totals that are its "
            "partitions' sums; "
            "sample.unsampled_topics only from 1.6.0, never empty, sorted, each topic once and "
            "not blank, and never beside a complete verification; "
            "source.selection only from 1.7.0, its start before its end, and a complete block "
            "over its window; "
            "format 2.0.0 only with source.selection.partitions, a format-1 selection a start "
            "only, each subset list sorted and distinct, one engine run per distinct subset or "
            "one more, and a complete block that expects nothing from an unselected partition; "
            "target.original_name only from 1.8.0 of format 1 and never beside a partition "
            "subset, only in a newTopic document with the empty "
            "prefix, its subject originalName, its approval mode and cluster condition from "
            "their closed sets, targetIsNotSource only beside a known other source cluster id, "
            "somewhere looked for an owner, each owner from a place looked in, an owned name "
            "only on the owner path, a one-person confirmation only with the names typed, "
            "the KafkaTopic resources looked in named by digest, and a complete verification; "
            "approval.console only from 1.9.0 of format 1 or 2.1.0 of format 2, its mode "
            "consoleApproval, a requester and an approver who are two people of one issuer, the "
            "approval inside the request's window, approval.approver the approver's principal, "
            "approval.key_id the console's key, approval.approved_at the instant of the "
            "approval, and target.original_name.approval_mode consoleApproval exactly beside it; "
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
        # PROD-04.1: the positions document, only when given, against the
        # receipt just verified — its exact bytes, never a re-serialisation.
        verified_positions = None
        if positions_path is not None:
            try:
                raw = open(positions_path, "rb").read()
            except OSError as e:
                print(
                    f"INVALID: cannot read positions document {positions_path!r}: {e}",
                    file=sys.stderr,
                )
                return 1
            try:
                pd = json.loads(raw)
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                print(f"INVALID: the positions document is not valid JSON: {e}", file=sys.stderr)
                return 1
            problem = _positions_document_shape(pd) or check_consumer_positions_document(
                doc, raw, pd
            )
            if problem:
                print(f"INVALID: {problem}", file=sys.stderr)
                return 1
            verified_positions = pd
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
        if str(archive.get("manifest_key") or "").strip(RUST_WHITESPACE):
            print(f"       manifest {archive.get('manifest_key')}")
            print(f"       manifest_sha256={archive.get('manifest_sha256')}")
            # FX-7: on a versioned bucket, WHICH version of that key the digest
            # is over — read it back with `?versionId=`. Absent means no version
            # was pinned, and nothing is printed rather than a placeholder.
            if archive.get("manifest_version_id") is not None:
                print(
                    f"       manifest_version_id={archive.get('manifest_version_id')} "
                    "(the object version the manifest digest is over)"
                )
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
        # FX-4: the configuration capture coverage, one line per topic, in the
        # same words `logweir drill verify` prints (`crates/logweir/src/
        # verify.rs::coverage_lines`); `scripts/check-verifier-parity.sh`
        # compares every line starting `config_coverage` between the two.
        for line in _coverage_lines(doc.get("config_coverage")):
            print(f"       {line}")
        # FX-8: a LogAppendTime topic's covered window is its producers' time.
        for line in _receipt_time_basis_lines(doc.get("config_coverage")):
            print(f"       {line}")
        # PROD-05.1: the configuration model, one line per topic, in the same
        # words `logweir drill verify` prints (`verify.rs::
        # topic_configuration_lines`); the parity script compares them.
        for line in _topic_configuration_lines(
            doc.get("topic_configuration"), doc.get("owner_detection")
        ):
            print(f"       {line}")
        # PROD-03.0: the schema dependency, one line per topic, in the same
        # words `logweir drill verify` prints (`verify.rs::
        # schema_dependency_lines`); the parity script compares them.
        for line in _schema_dependency_lines(doc.get("schema_dependency")):
            print(f"       {line}")
        # PROD-01.4a: the topic ID before and after the capture, one line per
        # topic, in the same words `logweir drill verify` prints (`verify.rs::
        # generation_lines`); the parity script compares them.
        for line in _generation_lines(doc.get("generations")):
            print(f"       {line}")
        # PROD-04.1: the consumer position evidence, one line per group, in the
        # same words `logweir drill verify` prints (`verify.rs::
        # consumer_positions_lines`); the parity script compares them.
        for line in _consumer_positions_lines(
            doc.get("consumer_positions"), verified_positions
        ):
            print(f"       {line}")
        print(
            "       This signature covers the receipt only. It says what THIS run "
            "captured; it is not a claim about any other backup of the same topics."
        )
        print(
            f"       verifier: verify_scorecard.py {SCRIPT_VERSION} "
            "(backup-receipt invariant set: format_version's major, the "
            "exit_code/manifest_key biconditional with trimmed-empty counted as absent, "
            "records covering exactly the named topic set, a covered window whose "
            "EXCLUSIVE end is after its start, source.auth.mode inside the closed "
            "two-value set, config_coverage's six: present only from 1.1.0, covering "
            "exactly the named topic set, closed coverage and reason sets, no timestamp "
            "type from a read that did not succeed, and a closed timestamp value and source, "
            "and topic_configuration's eight: present only from 1.3.0 and beside "
            "config_coverage, covering exactly the named topic set, entries exactly where the "
            "read succeeded, closed source and class sets, secret and inherited where they "
            "fit, a closed owner with a usable reference, and counts of at least one, "
            "owner_detection's two: a closed set present only beside "
            "topic_configuration, and an owner only from a source it lists, "
            "schema_dependency's eight: present only from 1.5.0, covering exactly the "
            "named topic set, closed verdict, reason and basis sets, both sides exactly when "
            "judged, a judged count that fits records, distinct plausible schema ids within "
            "the cap, the one-in-ten threshold, and a verdict its sides give, "
            "consumer_positions' six: present only from 1.7.0, a forward capture window with "
            "a closed listing and at least one group, this run's positions document by a "
            "well-formed digest, closed outcomes and reasons, fields and counts that fit the "
            "outcome with no captured group Dead and memberless, and an active flag the "
            "states derive, "
            "and the topic IDs' five: generations present only from 1.6.0, covering exactly "
            "the named topic set, every recorded ID in Kafka's text and never the zero ID, a "
            "reason exactly for a null ID, and a source exactly for a recorded one)"
        )
        if verified_positions is not None:
            print(
                "       positions document: the fourteen CP arms held (bound to this receipt "
                "by digest and length, its backup and run, its topic set, partitions in "
                "order, well-formed marks, a derived changed flag, exactly the captured "
                "groups, no position on a changed topic, no capture over an unread topic, "
                "every partition accounted for so absence is never offset 0, status, value "
                "and reason, a derived coverage, and the receipt's counts)"
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

    if payload_type_wanted == PAYLOAD_TYPES["catalog-point"]:
        # The signature, and ONE check of the record's own content (PROD-01.4a
        # review M1): every topic ID it copies is a real topic ID in Kafka's
        # text. Nothing else, and the lines below say why rather than leaving
        # an exit 0 to be read as more than it is. A catalog point record is an
        # INDEX over evidence that already exists; every other fact in it that
        # matters is recomputed from the backup receipt it names, and this
        # script deliberately does not fetch that receipt — it was handed three
        # local files and it phones nothing.
        problem = _catalog_point_problem(doc)
        if problem:
            print(f"INVALID: {problem}", file=sys.stderr)
            return 1
        print(f"VALID  payloadType={payload_type_wanted}")
        print(f"       {len(payload)} bytes verified under the presented key")
        if isinstance(doc, dict):
            print(
                f"       point_id={doc.get('point_id')} backup_id={doc.get('backup_id')} "
                f"run_id={doc.get('run_id')}"
            )
            receipt = doc.get("receipt")
            if isinstance(receipt, dict):
                # The binding an auditor goes and checks with. The short
                # point_id is its display form; this digest is the thing.
                print(f"       receipt {receipt.get('key')}")
                print(f"       receipt_sha256={receipt.get('sha256')}")
            archive = doc.get("archive")
            if isinstance(archive, dict) and archive.get("manifest_version_id") is not None:
                # FX-7 (record format 1.2.0): a COPY of the receipt's pin, and
                # informational like every copied fact — the receipt is the
                # authority.
                print(
                    f"       manifest_version_id={archive.get('manifest_version_id')} "
                    "(copied from the receipt)"
                )
        print(
            "       This signature covers the record only. It is NOT a claim that the point "
            "is available, that its archive is readable, or that its copied facts are true: "
            "fetch the backup receipt named above, verify it with --payload-type "
            "backup-receipt, and compare. One check of this document type is evaluated by "
            "this build: every topic ID the record copies (topics[].identity) is a real topic "
            "ID in Kafka's text. No other is."
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
    positions = None
    rest = []
    i = 0
    while i < len(argv):
        a = argv[i]
        # PROD-04.1: the positions document a backup receipt binds.
        if a == "--consumer-positions":
            if i + 1 >= len(argv):
                print("INVALID: --consumer-positions needs a value", file=sys.stderr)
                sys.exit(1)
            positions = argv[i + 1]
            i += 2
            continue
        if a.startswith("--consumer-positions="):
            positions = a.split("=", 1)[1]
            i += 1
            continue
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
            "[--consumer-positions <positions.json>] "
            "<document.json> <document.sig> <public.pem>",
            file=sys.stderr,
        )
        sys.exit(1)
    sys.exit(main(rest[0], rest[1], rest[2], wanted, positions))
