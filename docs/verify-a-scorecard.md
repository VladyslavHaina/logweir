# Verifying a Logweir drill scorecard

This guide explains how to authenticate a scorecard, check its consistency,
and interpret its limits using either Logweir or an independent Python verifier.

## What the artifact is

A **drill scorecard** records one Kafka restore drill: its archive, measured
RTO/RPO, sampled-record fingerprints and approval. Restore drills compare the
restored data with **the archive**, without contacting the source cluster;
`measured.rpo_source_relative_unmeasured_reason` records that limitation. The
separate `logweir backup run` command does contact a source cluster.

The scorecard is a [DSSE statement](https://github.com/secure-systems-lab/dsse).
Its JSON is never modified to carry a signature: a sidecar signs the exact
payload bytes. The independent [Python verifier](verify_scorecard.py) implements
DSSE and the document checks separately from Rust. A disagreement between the
readers should be reported; neither reader's verdict alone proves the underlying
measurements are true.

## The three files you receive

| File | Purpose |
| --- | --- |
| `scorecard.json` | The original measured result, as JSON. |
| `scorecard.sig` | DSSE sidecar naming the key and signature over the exact JSON bytes. |
| `public.pem` | Publisher's SPKI public key, authenticated independently of the scorecard handoff. |

These are the three inputs to the signature check. Never substitute a retyped
or reformatted scorecard; see [payload handling](#the-payload-is-never-re-serialised).

A storage receipt (`<run_id>.receipt.json` / `.receipt.sig`) and teardown
attestation (`<run_id>.teardown.json` / `.teardown.sig`) may also accompany the
scorecard. They are separate signed documents, each with its own `payloadType`.
Neither is needed to verify the scorecard, and neither substitutes for it.

## Where the public key comes from

**Authenticate the key through a channel independent of the document.** Someone
who substitutes a scorecard and signature can include their own matching public
key. A `VALID` result for that bundle proves only internal consistency, not that
it came from the organization you intended to trust.

Obtain the key from the publisher's established TLS website, an in-person or
voice exchange, or your organization's previously established trusted-key
registry. A key in the same email, folder or archive is insufficient by itself.

Pin the publisher's key fingerprint on first use:

```bash
openssl pkey -pubin -in public.pem -outform DER | openssl dgst -sha256
```

The hex digest is SHA-256 of the SPKI DER encoding. It is also the value used by
`signatures[].keyid`. Inspect the sidecar's first declared key ID with:

```bash
python3 -c "import json,sys; print(json.load(open(sys.argv[1]))['signatures'][0]['keyid'])" scorecard.sig
```

Compare it with your pinned fingerprint. Resolve any mismatch with the publisher
out of band before using either verification route; a verifier cannot decide
whether an unexpected key represents an authorized rotation or substitution.
See [key generation and rotation](keys.md).

Retain the fingerprint alongside the publisher's identity and reuse your trusted
`public.pem` for later scorecards. Check any replacement key against that trust
record; receiving a fresh bundled key does not authenticate it.

## Two ways to verify

The readers are intended to agree on acceptance or refusal. Running both gives
an additional independent check. Exit codes and malformed-document diagnostics
can differ, as described below; a disagreement in acceptance is a defect to report.

### Route 1: `logweir drill verify` (if you have the Logweir binary)

```bash
logweir drill verify \
  --scorecard scorecard.json \
  --signature scorecard.sig \
  --public-key public.pem
```

| Exit | Meaning |
| --- | --- |
| `0` | Signature valid; the document passes the reader's checks. |
| `1` | Operational or parsing failure, including malformed scorecard shape. |
| `4` | Signature, lock-proof or invariant failure, including an unsupported newer format major or an inconsistent approval claim. |

A valid signature does not override a refusal to interpret an unsupported or
self-contradictory document.

### Route 2: `verify_scorecard.py` (no Rust required)

Use Python 3 and the [cryptography](https://cryptography.io/) package, which
performs the elliptic-curve and Ed25519 signature math:

```bash
pip install cryptography
python3 verify_scorecard.py scorecard.json scorecard.sig public.pem
```

The command assumes the downloaded script is in your current directory. From
this repository's root, use `python3 docs/verify_scorecard.py ...`. For an
isolated installation:

```bash
python3 -m venv venv
venv/bin/pip install cryptography
venv/bin/python3 verify_scorecard.py scorecard.json scorecard.sig public.pem
```

`--payload-type scorecard|backup-receipt|receipt|teardown` selects the signed
document type; the default is `scorecard`. `backup-receipt` records a
[backup run](formats/backup-receipt.md); `receipt` records a scorecard's storage
readback. See [receipt verification](#verifying-the-receipts-signature).

| Exit/output | Meaning |
| --- | --- |
| `0`, `VALID` | Signature and applicable document checks passed. |
| `1`, `INVALID` on stderr | Refusal, with a reason such as signature mismatch, wrong payload type, malformed base64, unreadable input or document inconsistency. |
| `2`, `CANNOT RUN:` and `NOTHING WAS VERIFIED` | The `cryptography` dependency is missing. Install it and rerun; this is an infrastructure failure, not a verdict on the document. |

### What "verify" actually checks

Both routes check the sidecar's expected `payloadType`, the signature over DSSE
PAE of the type and raw payload bytes, then the applicable document rules.
Scorecards use `application/vnd.logweir.drill-scorecard+json;version=1.0.0`.
A signature for another document type cannot serve as a scorecard signature.

The current scorecard checks include:

- A readable format major, checked before interpreting its invariants; the
  eleven required blocks and six required non-block fields; `u64` fields within
  `0 <= v < 2**64`, with null allowed only for optional `u64` fields.
- The four post-put `evidence` fields remain zeroed. Offset-report key and
  digest are present or absent together. An absent target mode means `scratch`;
  a present mode is `scratch` or `newTopic`, and scratch requires a marker topic.
- A partial integrity result has a nonblank `partial_reason`; the two float
  fields (`objectives.pass_rate`, `integrity.pass_rate_measured`) are finite.
- Source-capture status agrees in both directions with the phase and source-RPO
  value/reason. `last_phase_completed` stays within `-1..=9`.
- Recovery-point gaps (`measured.rpo_seconds`, source-relative RPO and the RPO
  objective) are nonnegative. Matching records do not exceed sampled records,
  and sampled records do not exceed `sample.records_expected`.
- `outcome: pass` agrees with integrity, the absence of a partial reason,
  objectives and matching counts. An unmeasurable pass-rate objective cannot be
  reported as met, and `pass_rate_measured` is null outside `byte-fingerprint`.
- Matrix `fail` carries a reason; matrix `pass` requires a passing drill at
  `byte-fingerprint` level.
- An auth block names a nonblank supported mode (`plaintext` or `scramSha512`);
  a username without a mode is refused. `redactions` is empty.
- The claimed `approval.self_attested` agrees with a derivation from the key
  that actually verified the signature; see [approval](#reading-approvalself_attested).

Backup receipts have their own shape and invariants. Receipt and teardown
verification do not apply scorecard-specific integrity rules.

`logweir drill show` renders a document without verifying its signature. It
refuses an unsupported newer major with exit `1`, but it does not perform the
full verification above.

### The payload is never re-serialised

The verifier reads the payload as bytes (`open(path, "rb").read()`). The
signature covers trailing newlines, key order, whitespace, number formatting
and Unicode escaping. Semantically equivalent JSON can have different bytes.

**Do not pretty-print, run `jq .` over, or re-save the file before verification.**
Verify the delivered bytes. Parsing and writing JSON back out would check a
different payload rather than the stored artifact.

### The DSSE PAE encoding, precisely

The signed message is defined by the
[DSSE v1 specification](https://github.com/secure-systems-lab/dsse/blob/master/protocol.md):

```
PAE(type, body) = "DSSEv1" SP LEN(type) SP type SP LEN(body) SP body
```

`SP` is byte `0x20`; `LEN` is the ASCII-decimal count of **bytes**, not
characters. Encode the type to UTF-8 before measuring it. Python's
`len(payload_type)` counts code points, while `len(payload_type.encode("utf-8"))`
counts bytes; these differ for non-ASCII text. Logweir's media types are ASCII,
but independent implementations must still implement the byte-count rule.

### One asymmetry in the signature encoding

`signatures[].sig` is base64 of:

- **ECDSA P-256:** a DER-encoded `ECDSA-Sig-Value`, the ASN.1 sequence of integers
  `r` and `s`.
- **Ed25519:** the raw 64-byte `R || S` value, without ASN.1 wrapping.

Select the interpretation from the public key's type; the sidecar's JSON shape
alone does not distinguish them. The Python verifier branches on the loaded key
before checking the signature.

## Re-deriving `source.manifest_sha256` independently

A source block identifies the archive manifest, for example:

```json
"source": {
  "backup_id": "backup-2026-08-30T02:00:00Z",
  "manifest_sha256": "sha256:05b3abf2579a5eb66403cd78be557fd860633a1fe2103c7642030defe32c657f",
  "manifest_version_id": null,
  "captured_by_logweir": false
}
```

The signature authenticates this claim; it does not independently establish
which bytes were in storage. To check the claim:

1. Obtain the backup engine's bucket and key prefix for `backup_id` from your
   operator or runbook. The manifest is conventionally `manifest.json` beside
   its segment files, but the exact location is deployment-specific.
2. Fetch the manifest. If `manifest_version_id` is non-null, request that
   specific object version so an overwrite cannot silently change the input.
3. Run `sha256sum manifest.json` (or `shasum -a 256 manifest.json` on macOS).
4. Prefix the resulting hex digest with `sha256:` and compare it with
   `source.manifest_sha256`.

A mismatch is a finding: the fetched object differs from the signed claim,
possibly because it is a different manifest or version. This check uses the
underlying storage independently of either scorecard verifier.

## The storage receipt: a second signed document

The four post-put fields in every emitted scorecard start as:

```json
"evidence": { "create_only_enforced": false, "immutable": false,
              "retain_until": null, "version_id": null }
```

They describe an upload that has not happened when the scorecard is signed.
Updating them afterward would alter the signed bytes. Read them as **no storage
proof obtained at signing time**, not as evidence that the object was mutable,
unprotected or uploaded without a conditional put. Optional offset-report
fields may also appear in this block.

Post-upload facts live in a separately signed storage receipt:

| File | Purpose |
| --- | --- |
| `<run_id>.receipt.json` | Readback after uploading the scorecard. |
| `<run_id>.receipt.sig` | DSSE sidecar with type `application/vnd.logweir.drill-put-receipt+json;version=1.0.0`. |

```json
{
  "run_id": "01J9X2QK7C4V0R8YB3ZP6MTS5A",
  "scorecard_sha256": "sha256:9f0e…",
  "scorecard_key": "logweir/drills/01J9X2QK7C4V0R8YB3ZP6MTS5A.json",
  "create_only_enforced": true,
  "version_id": "3HL4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY+MTRCxf3vjVBH40Nr8X8gdRQBpUMLUo",
  "immutable": false,
  "retain_until": null,
  "observed_at": "2026-09-03T09:09:04Z"
}
```

`create_only_enforced: true` means the store performed a conditional put.
`false` records an unsupported-operation response and HEAD-then-PUT fallback;
it does not establish that anything was overwritten. `immutable` and
`retain_until` become true/non-null only with provider readback. Current
backends supply no such readback, so false/null is absence of proof.

### How the receipt binds to the scorecard

`scorecard_sha256` hashes the scorecard's **exact signed bytes**. Compare it
with the document you verified:

```bash
sha256sum scorecard.json
python3 -c "import json,sys; print(json.load(open(sys.argv[1]))['scorecard_sha256'])" receipt.json
```

Use the actual receipt filename in place of `receipt.json`. The second value
must equal `sha256:` plus the first digest. A mismatch means the receipt does
not bind to these bytes. Do not reformat the scorecard before hashing.

The binding is to payload bytes, not to a particular signature: signing unchanged
scorecard bytes again leaves the digest unchanged. `scorecard_key` records the
actual uploaded object key, which you can fetch and hash independently.

### Verifying the receipt's signature

```bash
python3 docs/verify_scorecard.py --payload-type receipt \
    '<run_id>.receipt.json' '<run_id>.receipt.sig' public.pem
```

Replace `<run_id>` with the real identifier. Example output:

```
VALID  receipt for logweir/drills/01M1RJZNEM507A7XQ7WCGPK6SJ.json
       binds to scorecard sha256:9b9df728…
       create_only_enforced=True  immutable=False  version_id=None
       observed_at=2026-09-05T10:49:22.143115Z
       This signature covers the receipt only. Verify the scorecard separately,
       then check sha256(scorecard.json) equals the digest above.
```

The flag accepts `scorecard`, `backup-receipt`, `receipt`, `teardown`, or the
corresponding full supported media type. A wrong type is a refusal. Selecting
the right type still requires a valid signature over those exact bytes; it does
not run scorecard-specific integrity checks on a receipt. Use the shipped flag
instead of hand-written receipt-verification scripts from older documentation.

The [independent-key requirement](#where-the-public-key-comes-from) also applies
to the receipt. An absent receipt means no storage evidence was published, not
that the upload lacked create-only protection. Receipt publication happens after
the scorecard is stored; failure is logged without retracting the measurement.

## The `logweir drill show` table is a SUMMARY, not the document

`logweir drill show scorecard.json` renders fourteen fixed rows covering outcome,
engine identity, levers, target, approval, RTO/RPO, integrity, target changes,
topic parity and evidence. The footer adds qualifications:

| Detail | Footer display |
| --- | --- |
| RTO, RPO and pass-rate objectives | `objectives (from the approved plan)` |
| `objectives.met` | `yes`, `NO` or `unmeasurable`; the last means a requested pass rate could not be measured. |
| `integrity.partial_reason` | Verbatim reason. |
| `engine_subreport.caveat` | Verbatim caveat, or an explicit notice that the block is null. |

The footer states that the table is a summary; `--format json` prints the signed
bytes. Read the JSON for details the summary omits:

- `phases`, including refusal notes; `source.manifest_sha256` and
  `target.topic_mapping_sha256`; `approval.plan_hash` and `approval.key_id`.
- `sample.records_expected` versus `integrity.records_sampled`. A null measured
  pass rate is not zero: the footer displays `measured —`, while
  `integrity.partial_reason` explains why it is unmeasured.
- `engine.matrix_verdict` and its reason; `redactions`.
- `last_phase_completed`: a completed drill signs `7` because phase 8's record
  and phase 9 teardown follow the frozen payload. This does not imply teardown
  was skipped; teardown has its own attestation. See the
  [format reference](formats/drill-scorecard.md).

Attach the JSON and sidecar when copying a table into a change record. The table
itself cannot be verified.

## What the scorecard does **not** claim

A signature authenticates the publisher's bytes. Assess the scope and strength
of the signed claims separately.

### The sample window is not a claim about the whole archive

The `sample` block's window, topics, partitions, expected/restored counts and
`coverage_note` describe what was sampled. A passing byte-fingerprint result
establishes agreement within that sampled window; it does not establish that
unsampled archive records would also match. Read `coverage_note` before making
claims about representativeness.

### The `evidence` block is not a finding about your bucket

The four post-put fields are zeroed before signing. Storage facts belong in the
[separate receipt](#the-storage-receipt-a-second-signed-document); false values
in the scorecard do not establish that the bucket is mutable or unprotected.

### `engine_subreport` corroborates nothing about Logweir's integrity claim

The current engine wrapper inherits the refusing default for `validation_run`,
so emitted scorecards have `engine_subreport: null`. Phase 8's missing-report
message is in structured logs, not the already frozen scorecard. Populated
checked-in fixtures are hand-authored format examples, not current production
output; their bodies can be minimal placeholders.

If a populated report is encountered, base64-decode `engine_subreport.body_b64`
and check `body_sha256` against the decoded bytes. There is no literal `body`
field: the paths below refer to that decoded JSON. The pinned upstream source
records two reasons not to treat it as independent corroboration:

- `integrity.checksums_valid` is constructed as literal `true`, rather than
  computed (`kafka-backup-core/src/evidence/emit.rs:109` in the upstream source).
- `restore.start_time`, `restore.end_time` and `restore.duration_seconds` are
  constructed as `None` (`emit.rs:100–104`), so they cannot corroborate RTO.

These are findings about the inspected upstream version, not promises about
future versions. The report is retained for provenance and traceability; a
matching body digest binds the included bytes but does not independently prove
who produced them or validate Logweir's integrity measurement. Read its caveat
and assess Logweir's `integrity` block against the declared sample.

## Reading `approval.self_attested`

```json
"approval": {
  "approver": "sre-oncall@example.com",
  "ticket": "CHG-40881",
  "plan_hash": "sha256:a97cc3ee6dda1bff8c8a2a185e94c7f31e9f8a4f9ef6372a8efee9d784d5808d",
  "approved_at": "2026-09-02T17:40:00Z",
  "key_id": "aaaa...",
  "self_attested": false
}
```

Self-attestation means the approval key equals the scorecard-signing key. It
indicates no separation at the key level; it does not independently identify the
humans operating those keys. Logweir permits and labels such runs.

**Both verifiers derive this finding** by comparing `approval.key_id` with the
key ID that actually verified the signature. They refuse a contradictory
`self_attested` claim: Rust exits `4`, Python exits `1`. The diagnostic is one
of the following, with Python adding `INVALID: `:

```
APPROVAL CLAIM NOT VERIFIED: the document claims self_attested=true but the approval key id <a> does not match the verifying key id <b>
APPROVAL CLAIM NOT VERIFIED: the document claims self_attested=false but the approval key id <a> matches the verifying key id <b>
```

`drill show` has neither signature nor key and cannot derive the result. Its
self-attestation row says
`SELF-ATTESTED (claimed; run 'drill verify' to check it against the signing key)`.
Do not treat it as a verification.

The stricter reader rule did not change `format_version: 1.0.0`; it rejects
contradictory claims without adding or changing fields. Both verifiers surface a
derived true with:

```
approval: SELF-ATTESTED — the approval key equals the signing key
```

The Python scorecard report also prints this reminder on every successful check:

```
evidence: the four post-put fields are zeroed before signing; the storage facts live in the receipt
```

Treat derived self-attestation as a reason to seek additional corroboration,
such as the change ticket, another reviewer or an independent drill. It is a
weaker governance signal, not by itself a defect in the signed artifact.

### What the `verifier:` line means, and why its version moves

The Python report ends with `verifier: verify_scorecard.py 1.13.0` followed by
the checks it applied. This is the **verifier's version**, not the document's
`format_version` (`1.0.0`). It changes when the reader's accepted-document set
changes. The compatibility history is:

| Version | Changed checks |
| --- | --- |
| `1.1.0` | Rejects nonzero post-put evidence fields in `1.0.x` scorecards. |
| `1.2.0` | Derives self-attestation from the verified key and rejects contradictory claims. |
| `1.3.0` | Rejects blank/whitespace partial reasons and nonempty redactions. |
| `1.4.0` | Checks outcome against integrity, partial reason, objectives and matching counts; bounds sampled counts by expected counts; requires matrix pass to be a passing byte-fingerprint drill. |
| `1.5.0` | Requires `sample` and integer `sample.records_expected`, closing Python/Rust disagreement on absent or mistyped sample data. |
| `1.6.0` | Checks all eleven required blocks in Rust declaration order and bounds every `u64` field to `0 <= v < 2**64`; closes missing target, target-diff and topic-parity gaps. |
| `1.7.0` | Requires all six non-block fields and rejects null for nonoptional `u64` values; the five optional `u64` fields still permit null. |
| `1.8.0` | Type-checks the six non-block fields using their Rust-implied JSON types, closing cases such as `run_id: 42`, `phases: "x"` and `requested_at: 5`. |
| `1.9.0` | Adds backup-receipt verification and scorecard auth-field consistency checks. |
| `1.10.0` | Restricts scorecard/backup-receipt auth modes to `plaintext` or `scramSha512`. |
| `1.11.0` | Requires offset-report key and digest to be present or absent together. |
| `1.12.0` | Requires a marker topic unless target mode is `newTopic`. |
| `1.13.0` | Rejects present target modes other than `scratch` or `newTopic`, including null; retains acceptance of failed integrity results with or without a partial reason. |

A known diagnostic-order difference remains: Python checks blocks before plain
fields. If both `run_id` and `engine` are absent, it reports `engine`, while Rust
reports `run_id`. Both refuse; this is not an acceptance disagreement.

**Rerun the current verifier over retained documents and sidecars checked with
older versions.** Earlier `VALID` results may reflect weaker consistency or
shape checks. The original payload bytes and signature stay unchanged; the
reader's rules become stricter. Keep the verifier version with your audit record,
not just the verdict.

---

Documentation is licensed [CC-BY-4.0](LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
