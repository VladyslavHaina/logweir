# Verifying a Logweir drill scorecard

This guide is for an auditor who has been handed a Logweir drill scorecard
and needs to check it — without installing Rust, without trusting Logweir's
own binary, and without reading a line of this project's source code.

## What the artifact is

A **drill scorecard** is a JSON document that records the measured result
of one Kafka restore drill: which backup it restored, what RTO/RPO it
measured, whether the restored data matched **the archive** by byte
fingerprint, who approved the drill, and more. (The archive, not the source
cluster: v0.1 never contacts the source, and the scorecard says so itself in
`measured.rpo_source_relative_unmeasured_reason`. Every other surface —
`README.md`, the format reference — says "archive" too; this page said "source"
and was the one page written for the reader least able to check it.) It is published as a
[DSSE (Dead Simple Signing Envelope)](https://github.com/secure-systems-lab/dsse)
signed statement: the scorecard itself is never modified to carry a
signature — instead, a separate sidecar file holds the signature over the
*exact bytes* of the scorecard file.

The claim Logweir makes about this artifact is: **the scorecard is
verifiable evidence, not something you have to take Logweir's word for.**
This document, together with `docs/verify_scorecard.py`, is the proof —
an independent, twenty-line-core re-implementation of the DSSE check, built
from the public DSSE specification rather than from Logweir's Rust. If this
script and Logweir's own `logweir drill verify` ever disagree, that is a bug
in the format, not a bug in this script.

## The three files you receive

An auditor needs exactly three files to check one scorecard:

| File | What it is |
|---|---|
| `scorecard.json` | The scorecard itself: the measured result, as a JSON document. |
| `scorecard.sig` | The DSSE sidecar: a JSON file naming the signing key and holding the signature over `scorecard.json`'s exact bytes. |
| `public.pem` | The publisher's public key, PEM-encoded (SPKI), used to check the signature. This is *not* secret, but **it must not arrive by the same channel as the other two files** — see the next section before you run anything. |

Do not accept a fourth input **to the signature check**. In particular, never
let anyone hand you a "re-typed" or "reformatted" copy of `scorecard.json` —
see [The payload is never re-serialised](#the-payload-is-never-re-serialised)
below for why that would silently defeat the check.

Two further files may accompany a scorecard, and they are **separate signed
documents, not extra inputs to the check above**: `<run_id>.receipt.json` /
`.receipt.sig`, the storage receipt (see
[The storage receipt](#the-storage-receipt-a-second-signed-document)), and
`<run_id>.teardown.json` / `.teardown.sig`, the teardown attestation. Each is
verified on its own, against its own `payloadType`. Neither is required to
verify a scorecard, and neither can substitute for one.

## Where the public key comes from

Read this before you verify anything — it changes what you do first, not
just how you interpret the result.

**Never verify a scorecard against a `public.pem` that arrived in the same
handoff as `scorecard.json` and `scorecard.sig`.** If someone hands you a
forged scorecard and a forged signature, they can just as easily hand you
the public half of whatever key they forged it with, all three in one
bundle. This script will print a clean `VALID` for that bundle — correctly,
by its own narrow contract: the three files really are consistent with each
other. That is not the same claim as "this scorecard was published by the
organization you think published it," and a `VALID` result does not
distinguish the two unless you have separately pinned the key.

**The key must reach you through a channel independent of the document
itself** — read off the publisher's own website over TLS, read aloud or
handed over in person, retrieved from your organization's own trusted-key
registry established ahead of time — anything other than "it was in the
same email, folder, or tarball as the scorecard."

**Pin it once, the first time you receive a key from a given publisher,**
by recording its fingerprint:

```bash
openssl pkey -pubin -in public.pem -outform DER | openssl dgst -sha256
```

This prints something like `SHA2-256(stdin)= 917cf9a2...`. That hex digest
is the SHA-256 of the key's SPKI DER encoding — which is exactly the value
the sidecar's `signatures[].keyid` field carries, so you can cross-check
the sidecar's own claim about which key it's signed by, before running any
cryptographic verification at all:

```bash
python3 -c "import json,sys; print(json.load(open(sys.argv[1]))['signatures'][0]['keyid'])" scorecard.sig
```

If that value does not match the fingerprint you pinned for this publisher,
stop. Do not proceed to Route 1 or Route 2 below — a mismatch means either
an untold-you key rotation or a forgery attempt, and either way it is
something to resolve with the publisher out of band, not something either
verifier can adjudicate for you.

For how a publisher generates the key behind that fingerprint, what the
fingerprint is a digest of, and what a legitimate rotation looks like from
the publisher's side, see [`keys.md`](keys.md).

Record the pinned fingerprint somewhere durable (next to the publisher's
name, alongside your own organization's other trusted keys) and reuse
*that* retained copy of `public.pem` for every later scorecard from this
publisher. A fresh `public.pem` that arrives bundled with a later scorecard
is worth nothing on its own, however convenient it is to use — check its
fingerprint against your pinned one first.

**To state the failure mode plainly: verifying `scorecard.json` against a
`public.pem` delivered in the same bundle proves only that the bundle is
internally consistent. It does not prove authenticity. Authenticity comes
from the key having reached you a different way.**

## Two ways to verify

Both routes check the same three files and reach the same verdict. Use
whichever is convenient; running both and comparing the verdict is even
better, since agreement between two independent implementations is stronger
evidence than either alone.

That equality is a property somebody has to maintain, and for one release it
did not hold: `verify_scorecard.py` implemented ONE of the ~12 self-consistency
rules `logweir drill verify` applies, so a document with
`format_version: "2.0.0"`, or `measured.rpo_seconds: -90`, or
`records_sampled_matching: 9999` over `records_sampled: 10`, printed `VALID`
here and was refused there. The Python verifier's `check_invariants` now
mirrors the Rust validator arm for arm, in the same order and with the same
wording. If you find a document the two disagree about, that is a bug in the
format — report it.

### Route 1: `logweir drill verify` (if you have the Logweir binary)

```bash
logweir drill verify \
  --scorecard scorecard.json \
  --signature scorecard.sig \
  --public-key public.pem
```

Exit code `0` means the signature is valid and the document does not
contradict itself. A non-zero exit code means it does not: `1` for an
operational problem such as a file that will not parse, and `4` for a
signature failure, a lock-proof failure, **or a document this reader cannot
honestly interpret** — a `format_version` whose major is newer than this build
understands, or any other self-contradiction `validate_invariants` catches.
The last case is a valid signature over a document the reader must refuse
anyway (Global Constraint 12), which is why it is not exit 0: a signature
proves who wrote the bytes, never that this reader may act on them.

### Route 2: `verify_scorecard.py` (no Rust required)

This is the route that matters for this document: it needs only Python 3
and one widely available third-party package, [`cryptography`](https://cryptography.io/),
which implements the actual elliptic-curve and Ed25519 signature math.
There is no pure-standard-library way to do that math in Python, so this
one dependency is unavoidable if the check is to be a real cryptographic
verification rather than a string comparison.

```bash
pip install cryptography
python3 verify_scorecard.py scorecard.json scorecard.sig public.pem
```

A drill publishes three signed documents, each under its own payload type.
`--payload-type scorecard|receipt|teardown` selects which one is being checked;
the default is the scorecard, so the three-argument form above is unchanged.
See [Verifying the receipt's signature](#verifying-the-receipts-signature).

(If you would rather not install into your system Python, create a
virtual environment first: `python3 -m venv venv && venv/bin/pip install
cryptography && venv/bin/python3 verify_scorecard.py ...`. Either way, this
is the only package the script imports beyond the Python standard library —
read `docs/verify_scorecard.py` yourself to confirm that.)

Exit code `0` and a line starting `VALID` means the signature checks out.
Exit code `1` and a line starting `INVALID` (printed to stderr) means it
does not, along with the specific reason (signature mismatch, unexpected
`payloadType`, malformed base64, or a self-contradicting document).

Exit code **`2` means the script could not run at all** — its one dependency,
`cryptography`, is not installed. It prints `CANNOT RUN:` and the `pip install`
line, and says `NOTHING WAS VERIFIED`. This is deliberately **not** exit 1: exit
1 is a verdict on your document, and "the verifier would not start" must never
be mistaken for "the signature did not check out". If you are branching on this
in automation, treat 2 as an infrastructure failure and re-run, never as a
finding.

### What "verify" actually checks

Both routes check three things, in order:

1. **The sidecar's declared `payloadType`** matches the one Logweir scorecards
   use (`application/vnd.logweir.drill-scorecard+json;version=1.0.0`). A
   sidecar for some other kind of document, even if genuinely signed, must
   not be accepted as a scorecard signature.
2. **The signature verifies** over the DSSE v1 Pre-Authentication Encoding
   (PAE) of `(payloadType, payload)`, where `payload` is the raw bytes of
   `scorecard.json` as they were read from disk — see below.
3. **The document does not contradict itself.** A signature only proves who
   wrote the bytes; it says nothing about whether the bytes make sense, so this
   check is separate from the cryptography — and **both routes apply the same
   set**, which is what makes the "same verdict" claim above true. The set is:

   - `format_version`'s major is not newer than this reader understands
     (Global Constraint 12), checked **first**, before any rule that depends on
     what a field means;
   - `integrity.result` is never `"partial"` without a `partial_reason`;
   - the two float fields are finite;
   - `source.captured_by_logweir` agrees, in **both** directions, with
     `last_phase_completed` and the two `rpo_source_relative_*` fields;
   - `objectives.met` is not `true` when the pass rate was not measurable;
   - no seconds-valued gap — `measured.rpo_seconds`,
     `measured.rpo_source_relative_seconds`, `objectives.rpo_seconds` — is
     negative;
   - `records_sampled_matching` does not exceed `records_sampled`;
   - `engine.matrix_verdict: "fail"` carries a `matrix_verdict_reason`;
   - `integrity.pass_rate_measured` is null unless the level is
     `byte-fingerprint`;
   - `last_phase_completed` is within `-1..=9`.

   `logweir drill show` is the third reader Logweir ships. It renders rather
   than verifies, so it applies only the first rule — it **refuses** a document
   whose major version it does not understand, and exits 1 rather than printing
   a table under field meanings a future major may have redefined.

### The payload is never re-serialised

`verify_scorecard.py` reads `scorecard.json` with `open(path, "rb").read()`
and signs/verifies exactly those bytes — including the file's trailing
newline, its exact key order, and its exact whitespace. It never parses the
JSON and writes it back out before verifying.

This matters because re-serialising would silently defeat the entire point
of the format. Two JSON documents can be semantically identical yet differ
byte-for-byte (key order, spacing, number formatting, Unicode escaping), and
a signature is a bitwise-exact check. If a verifier reformatted the
scorecard before checking the signature, it would in effect be checking a
signature against a document nobody actually published — exactly the kind
of substitution DSSE is designed to catch, defeated by the verifier itself.
So: **never "pretty-print", `jq .`, or re-save `scorecard.json` before
handing it to either verifier.** Verify the file exactly as it was
delivered to you.

### The DSSE PAE encoding, precisely

Both `logweir drill verify` and `verify_scorecard.py` sign and verify the
same bytes, defined by the [DSSE v1 specification](https://github.com/secure-systems-lab/dsse/blob/master/protocol.md):

```
PAE(type, body) = "DSSEv1" SP LEN(type) SP type SP LEN(body) SP body
```

`SP` is a single `0x20` byte. `LEN` is the ASCII-decimal count of **bytes**,
not characters. This distinction is invisible on every fixture in this
repository, because `payloadType` here is pure ASCII, where byte count and
character count are the same number — but it is not invisible in general.
In Python, `len(payload_type)` counts Unicode code points; `len(payload_type
.encode())` counts bytes. A payload type containing so much as one non-ASCII
character would make the two diverge, and a verifier using the wrong one
would silently check a different message than the one that was signed. This
is exactly the kind of bug that passes every test you have and fails in
production the first time it matters — so `verify_scorecard.py`'s `pae()`
function encodes to bytes *before* taking `len()`, and that is worth
checking for yourself by reading the function; it is four lines.

### One asymmetry in the signature encoding

The sidecar's `signatures[].sig` field is base64-encoded, but *what* it is
base64 of depends on the key type:

- For an **ECDSA P-256** key, `sig` is base64 of a **DER-encoded**
  `ECDSA-Sig-Value` (the ASN.1 SEQUENCE of two INTEGERs, r and s).
- For an **Ed25519** key, `sig` is base64 of the **raw 64-byte** `R || S`
  value. There is no ASN.1 encoding involved at all for Ed25519.

Nothing in the sidecar's JSON shape tells you which encoding to expect —
you have to know it from the public key's type, which is exactly what
`verify_scorecard.py` does (it branches on whether the loaded key is an
`Ed25519PublicKey` before choosing how to interpret `sig`). If you ever
write your own third implementation, get this asymmetry wrong and it will
work for one key type and silently produce "invalid signature" for the
other — never a crash, just a rejection that looks like tampering.

## Re-deriving `source.manifest_sha256` independently

The scorecard's `source` block names the backup it restored from:

```json
"source": {
  "backup_id": "backup-2026-08-30T02:00:00Z",
  "manifest_sha256": "sha256:05b3abf2579a5eb66403cd78be557fd860633a1fe2103c7642030defe32c657f",
  "manifest_version_id": null,
  "captured_by_logweir": false
}
```

`manifest_sha256` is a claim about a specific object in the backup engine's
own object-store bucket — the `manifest.json` that engine wrote when it
took the backup identified by `backup_id`. A signature on the scorecard
proves Logweir's publisher signed this claim; it does **not** prove the
claim is true. To check it independently:

1. Ask your operator (or consult your organization's backup runbook) for
   the object-store bucket and key prefix the backup engine writes to for
   this `backup_id`. Backup manifests are conventionally stored as a
   `manifest.json` object under a prefix tied to the backup, alongside the
   segment files it references — the exact bucket and prefix are a
   deployment detail, not something this document can hardcode.
2. Fetch that object. If `manifest_version_id` is non-null, fetch that
   *specific version* of the object (most S3-compatible stores, including
   MinIO, support fetching by version ID) — otherwise you may be hashing a
   manifest that has since been overwritten.
3. Hash it:
   ```bash
   sha256sum manifest.json
   ```
4. Compare the resulting hex digest, prefixed with `sha256:`, against
   `source.manifest_sha256` in the scorecard. A mismatch means either the
   scorecard is describing a different manifest than the one currently in
   the bucket, or the manifest has changed since the drill ran — either
   way, it is a finding, not something to wave through.

This step is entirely independent of the DSSE signature check above: it
does not use `verify_scorecard.py`, `logweir`, or any cryptography beyond
`sha256sum`. It is the auditor re-deriving a fact from the underlying
storage, not re-checking Logweir's own signature.

## The storage receipt: a second signed document

**Read this before quoting a scorecard's `evidence` block to anyone.**

The scorecard's `evidence` block —

```json
"evidence": { "create_only_enforced": false, "immutable": false,
              "retain_until": null, "version_id": null }
```

— is **always exactly that**, in every scorecard Logweir emits, on every
storage backend. It is not a finding. The reason is structural rather than a
limitation: all four fields describe the *upload of this scorecard*, an event
that has not happened when the scorecard is signed, and cannot happen before
it, because a signature covers bytes and the bytes have to exist first. The
document is never re-serialised afterwards — that would invalidate the
signature. So rather than sign four values nothing had established, Logweir
zeroes them.

Read those four fields as **"no proof was obtainable at signing time"**, never
as "the object is mutable", "the put was not conditional", or "no retention
applies". The block deliberately under-claims, and that is what guarantees a
valid Logweir signature can never cover an unsubstantiated WORM or
create-only assertion.

The real post-upload readback is published in a **second signed document**,
written after the put and stored beside the scorecard in the evidence bucket:

| File | What it is |
|---|---|
| `<run_id>.receipt.json` | The storage receipt: what the object store actually answered *after* the scorecard was uploaded. |
| `<run_id>.receipt.sig` | Its own DSSE sidecar, under `payloadType` `application/vnd.logweir.drill-put-receipt+json;version=1.0.0`. |

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

`create_only_enforced: true` here means the store performed a genuine
conditional put — the object could not have silently replaced a previous
drill's evidence. `false` means the backend answered "not supported" and
Logweir took a HEAD-then-PUT fallback, which is recorded honestly and is
**not** the same statement as "the object was overwritten". `immutable` and
`retain_until` are `true`/non-null only after a provider readback actually
answered; on every backend Logweir can build today that readback returns
nothing, so expect `false`/`null` and do not read it as a finding.

### How the receipt binds to the scorecard

`scorecard_sha256` is the SHA-256 of the **exact signed bytes** of
`scorecard.json` — not the run id, not a re-serialisation. That is what makes
the pairing checkable rather than asserted: any other scorecard, including the
same run signed a second time, produces a different digest, so a receipt
cannot be moved onto a document it does not describe.

Check the binding with no tooling at all:

```bash
sha256sum scorecard.json
python3 -c "import json,sys; print(json.load(open(sys.argv[1]))['scorecard_sha256'])" receipt.json
```

The second value is the first, prefixed with `sha256:`. If they differ, the
receipt describes a different document — treat that as a finding, not as a
formatting quirk. Do not `jq .` or re-save `scorecard.json` first, for the
same reason the signature check forbids it.

`scorecard_key` is the object key the scorecard was actually put at, carried
out of the upload rather than reconstructed, so you can fetch that exact
object and hash it yourself.

### Verifying the receipt's signature

The shipped verifier does it, with `--payload-type`:

```bash
python3 docs/verify_scorecard.py --payload-type receipt \
    <run_id>.receipt.json <run_id>.receipt.sig public.pem
```

```
VALID  receipt for logweir/drills/01M1RJZNEM507A7XQ7WCGPK6SJ.json
       binds to scorecard sha256:9b9df728…
       create_only_enforced=True  immutable=False  version_id=None
       observed_at=2026-09-05T10:49:22.143115Z
       This signature covers the receipt only. Verify the scorecard separately,
       then check sha256(scorecard.json) equals the digest above.
```

The flag takes `scorecard` (the default), `receipt`, `teardown`, or a full media
type. **Naming the wrong one is a refusal, not a warning** — the default path
still refuses a receipt, exactly as it did before the flag existed, because a
sidecar for one kind of document must never be accepted as the signature over
another. And selecting the right type is not a way to pass: the signature still
has to cover those exact bytes.

The scorecard-specific consistency checks (the `integrity.result: partial`
rule, the run summary) apply **only** on the scorecard path. A receipt has no
`integrity` block, and the script does not reach for one.

Until v0.1.0 this flag did not exist and an auditor had to hand-roll PAE in a
throwaway script to check a receipt at all — which is how a signature that
verifies nothing gets built. If you have such a script from an earlier draft of
this document, delete it and use the flag.

Everything the [Where the public key comes from](#where-the-public-key-comes-from)
section says applies unchanged: the receipt is signed by the same publisher
key, so a receipt verified against a `public.pem` from the same bundle proves
internal consistency and nothing about authenticity.

**A receipt that is absent means no storage evidence was published for that
run** — it does not mean the upload was not create-only. The receipt is
written after the scorecard is already signed and stored, and a failure to
write it is logged and deliberately does not retract a measurement.

## The `logweir drill show` table is a SUMMARY, not the document

`logweir drill show scorecard.json` renders a fixed-width table. It is the
image in the README and it is what most people will actually look at, so it is
worth being precise about what it is.

**The fourteen rows are frozen by the specification.** Their layout does not
change between versions, which is what makes them safe to paste into a ticket.
They carry: outcome; engine id/version/digest; the two levers; target cluster,
marker topic and mapping count; approval; the four RTO figures with the compared
one starred; RPO; integrity level/result and the sampled counts; the target
diff; topic parity; and the `evidence` block.

**Those fourteen rows omit three things that most qualify the result**, and a
reader who saw only them came away more confident than the signed document
supports. That was a real defect and it is fixed by a **footer** printed below
the table — the rows themselves are untouched:

| Omitted from the fourteen rows | Where it is now |
|---|---|
| `objectives.rto_seconds`, `rpo_seconds`, `pass_rate` — the starred row *cites* the RTO objective and never displayed it | footer, `objectives (from the approved plan)` |
| `objectives.met` — whether the drill met what it was asked to meet | footer, rendered as a **tri-state**: `yes` / `NO` / `unmeasurable`. `unmeasurable` is not met; it means a `pass_rate` objective was requested and could not be measured |
| `integrity.partial_reason` — why a `partial` result was partial | footer, verbatim |
| `engine_subreport.caveat` — which states the sub-report "corroborates nothing Logweir claims" | footer, verbatim; and when the block is `null`, the footer says so in words rather than leaving a blank |

The footer closes with the sentence that matters most:

> This table is a SUMMARY of a signed document, not the document. `--format
> json` prints the signed bytes; docs/verify-a-scorecard.md lists what the
> summary omits.

**What the table still does not show, by design.** The signed JSON is the
authority, and these live only there:

- The whole `phases` array, including the `notes` a `preflight-failed` run uses
  to say *why* it was refused.
- `source.manifest_sha256` and `target.topic_mapping_sha256` — the two digests
  that make the archive and the mapping checkable rather than described.
- `approval.plan_hash` and `approval.key_id`.
- `sample.records_expected` versus `integrity.records_sampled` — the canary size
  against what was actually reconciled.
- `integrity.pass_rate_measured`'s null-ness, which is a different statement
  from a measured 0. (It IS shown, as `measured —`, on the footer's `pass_rate`
  line; what the table cannot convey is *which* of the three null cases applies.
  `integrity.partial_reason`, printed verbatim below it, names it.)
- `engine.matrix_verdict` and `engine.matrix_verdict_reason` — the
  support-matrix row this run established. The table does not render them at
  all, so the JSON is the only place to read them. A `fail` here always carries
  its reason.
- `last_phase_completed`. A completed drill signs `7`, not `9` — phase 8's own
  record and phase 9's teardown are both written after the bytes are frozen.
  `7` is **not** "teardown was skipped"; teardown is attested in its own signed
  document. See [the format reference](formats/drill-scorecard.md).
- `redactions[]`.

If you are deciding what a scorecard proves, read the JSON. If you are pasting
evidence into a change record, paste the table **and** attach the JSON and its
`.sig` — a table cannot be verified.

## What the scorecard does **not** claim

A signature guarantees the *bytes* are what the publisher wrote and
attested to. It says nothing about the *scope* or *reliability* of what is
inside. Two things in particular are easy to over-read from this document,
and neither is a claim the scorecard is making:

### The sample window is not a claim about the whole archive

The `sample` block (`window_start`, `window_end`, `topics`, `partitions`,
`records_expected`, `records_restored`, `coverage_note`) describes the
records the drill actually verified byte-for-byte, not the entire backup
archive. A `pass` result with `integrity.level: "byte-fingerprint"` and
`integrity.result: "pass"` means every record *in the sampled window*
matched; it is not a statement that every record ever written to the
archive, outside that window, would also match. Read `sample.coverage_note`
for whatever the drill itself says about how representative the window is
— but do not extend a byte-fingerprint match on one window into a claim
about records the drill never touched.

### The `evidence` block is not a finding about your bucket

`evidence.create_only_enforced: false` and `evidence.immutable: false` are
what *every* Logweir scorecard says, because the upload had not happened when
the document was signed. They are not evidence that the object was
overwritable or unprotected. The storage facts live in the separately signed
receipt — see [The storage receipt](#the-storage-receipt-a-second-signed-document).

### `engine_subreport` corroborates nothing about Logweir's integrity claim

**In v0.1 this block is `null` in every scorecard the tool produces.** Logweir
never invokes the engine's `validation run`, so nothing writes a report for it
to retain: `OsoCliEngine` inherits `DataEngine::validation_run`'s default, which
refuses rather than fabricating one, and phase 8 leaves the field null. (Phase 8
also logs `"no engine validation report under the per-run prefix"`, on the
structured log and **not** in the scorecard — the signed document's `phases`
array ends before phase 8's own record, because phase 8 signs a frozen copy.
Do not go looking for that sentence in the JSON.) A drill that reaches phase 8
at all therefore publishes
`"engine_subreport": null`, and that is the correct reading of the field today:
no engine sub-report was retained. The rest of this section describes what the
block WOULD mean once the engine's own validation run is invoked, and it is here
now so that nobody who meets a populated one later mistakes it for corroboration.
(The checked-in fixtures under `e2e/fixtures/signed/` and
`e2e/fixtures/scorecard-pass.json` do carry a populated block; they are
hand-authored examples of the FORMAT, predate this limitation being established,
and are not what the shipping code emits.)

The `engine_subreport` block embeds the upstream backup engine's own
evidence report, retained verbatim (see `engine_subreport.caveat` in the
scorecard itself, which states this in the document). It is tempting to
read a `pass`-looking upstream sub-report as independent corroboration of
Logweir's own `integrity` block. It is not, for two concrete, verified
reasons. (The checked-in test fixtures under `e2e/fixtures/signed/` embed a
minimal placeholder body — decode `scorecard.json`'s
`engine_subreport.body_b64` yourself and you will find a two-field stub,
not a full report — so the paths below describe the real upstream engine's
report schema, the one a genuine production scorecard embeds, not
necessarily the bytes in these particular fixtures.)

- The literal JSON path **`engine_subreport.body.integrity.checksums_valid`**
  — where `body` denotes the JSON object you get by base64-decoding
  `engine_subreport.body_b64` (there is no field literally named `body` in
  the sidecar; decode `body_b64` first, then walk `.integrity
  .checksums_valid` in the result) — is the hardcoded literal `true` in
  every report the upstream engine ever emits. It is not computed from
  anything
  [VERIFIED `U/kafka-backup/crates/kafka-backup-core/src/evidence/emit.rs:109`
  — `checksums_valid: true,` is a literal in the struct construction].
  A field that is always `true` by construction cannot corroborate
  anything; it is not evidence, it is a constant.
- The same decoded report's restore timing fields — reachable the same
  way at `engine_subreport.body.restore.start_time`,
  `engine_subreport.body.restore.end_time`, and
  `engine_subreport.body.restore.duration_seconds` — are always `None`
  [VERIFIED `U/kafka-backup/crates/kafka-backup-core/src/evidence/emit.rs:100-104`
  — `RestoreInfo { target_bootstrap_servers, start_time: None, end_time:
  None, duration_seconds: None }`], so the sub-report cannot corroborate
  Logweir's own measured RTO figures either — there is nothing there to
  compare against.

In short: `engine_subreport` is included for provenance and traceability
(you can verify `body_sha256` against `body_b64` yourself, and confirm the
bytes really are what the upstream engine wrote), but you should never read
it as a second, independent check on Logweir's `integrity` or `measured`
blocks. Logweir's own signed claim stands or falls on its own `integrity`
block, checked against the sample described in `sample`, and nothing else
in this document backs it up.

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

`approval.self_attested` is `true` exactly when the key that approved the
drill plan is the same key that signed the resulting scorecard — that is,
the same party planned the drill, ran it, and vouches for its own result,
with no separation between approver and publisher. Logweir never refuses to
sign such a scorecard; it labels it instead (spec's own design choice: a
self-attested run is not a forgery, but it is a materially weaker
governance signal than one where a different approving party's key is on
record).

**The field in the document is a CLAIM. Both verifiers DERIVE the finding
instead.** They compare `approval.key_id` against the key id of the signature
that actually verified, which is the same comparison the writer makes when it
fills the field in. Neither reader reports the document's own `self_attested`
value, ever: for one release both did, so a document could assert or deny its
own provenance — the single most damaging property in the artifact — and both
verifiers would repeat the assertion under a `VALID` banner.

A document whose claim disagrees with the derivation is **refused**, not
annotated. `logweir drill verify` exits **4** — the same class as a bad
signature, because it is a provenance claim the signature cannot support —
and `docs/verify_scorecard.py` exits **1**, its own "INVALID" code. Both print
the same line, byte for byte (the script prefixes its copy with `INVALID: `):

```
APPROVAL CLAIM NOT VERIFIED: the document claims self_attested=true but the approval key id <a> does not match the verifying key id <b>
APPROVAL CLAIM NOT VERIFIED: the document claims self_attested=false but the approval key id <a> matches the verifying key id <b>
```

Both directions are refused. A document that *under*-reports its lack of
separation of duties is as false as one that over-claims, and a message that
said "does not match" in the second case would be stating a falsehood in the
one line whose whole purpose is to be trustworthy.

`logweir drill show` is the exception, and it says so on its own face: that
command renders a table from the scorecard alone and is handed no signature
and no key, so it cannot derive anything. Its approval row reads
`SELF-ATTESTED (claimed; run 'drill verify' to check it against the signing
key)`. Do not read `drill show` as a check.

This narrows what a `1.0.0` reader accepts without changing the format: no
field is added, removed or retyped, `format_version` stays `1.0.0`, and **no
document Logweir has ever written is refused**, because the writer has always
derived the field correctly.

Both verifiers surface a derived `true` rather than hiding it:
`verify_scorecard.py` prints, verbatim (this is the actual output — compare
your terminal against these exact characters, not a paraphrase of them):

```
VALID  run_id=01J9X2QK7C4V0R8YB3ZP6MTS5A  outcome=pass
       rto_seconds=512  rpo_seconds=0
       integrity=byte-fingerprint/pass
       approval: SELF-ATTESTED — the approval key equals the signing key
       evidence: the four post-put fields are zeroed before signing; the storage facts live in the receipt
       verifier: verify_scorecard.py 1.3.0 (invariant set: evidence-zeroing, trimmed-empty partial_reason, redactions; approval.self_attested derived, not echoed)
```

(The `evidence:` line is printed on **every** scorecard, self-attested or not.
It is there so nobody reads the zeroed `evidence` block as a finding about
their bucket — see [The `evidence` block is not a finding about your
bucket](#the-evidence-block-is-not-a-finding-about-your-bucket).)

### What the `verifier:` line means, and why its version moves

The last line names the script's own version — **not** the scorecard's
`format_version`, which is `1.0.0` and stays there. `verify_scorecard.py`'s
version tracks its **verdict rule**: it moves whenever there is a document this
script would now decide differently from the previous version. Every version so
far is such a move:

| Version | What it decides differently |
|---|---|
| `1.1.0` | Refuses a `1.0.x` scorecard whose four post-put `evidence` fields are not zeroed. |
| `1.2.0` | **Derives** `approval.self_attested` from the key that verified the signature and refuses a document whose claim disagrees, where `1.1.0` printed the document's own claim and returned `VALID`. |
| `1.3.0` | Refuses a `partial` integrity result whose `partial_reason` is **blank** (`""` or whitespace) and not merely null, and refuses any document with a non-empty `redactions`. Both were `VALID` under `1.2.0`. |

The parenthetical on the `verifier:` line enumerates the current invariant set,
so the line an auditor reads names the checks that actually produced the verdict
in front of them rather than one member of the set.

**What that means for you as an auditor.** A scorecard you verified with an
earlier version was checked by a weaker rule. If you retained the document and
its sidecar — and you should have; a signature is over a fixed byte string and
stays checkable forever — **re-run the current script over your retained
documents.** Nothing about the artifact changed and no signature is affected;
what changed is what this reader is willing to call `VALID`. A document that
passed under an earlier version and is refused under `1.3.0` was always making a
claim its signature could not support. The script was not catching it.

Read the version line, not just the verdict. The verdict alone cannot tell you
which rule produced it.

`logweir drill verify` prints the equivalent line as `approval:
SELF-ATTESTED — the approval key equals the signing key` among its own
report fields. Either way, the line to look for ends in `SELF-ATTESTED —
the approval key equals the signing key`.

If you are an auditor deciding how much weight to give a passing scorecard,
treat a **derived** `self_attested: true` as a reason to seek additional corroboration —
an out-of-band record of the change ticket, a second reviewer, or a
separate independent drill — rather than as a defect in the artifact
itself.


---

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
