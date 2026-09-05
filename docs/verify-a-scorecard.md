# Verifying a Logweir drill scorecard

This guide is for an auditor who has been handed a Logweir drill scorecard
and needs to check it — without installing Rust, without trusting Logweir's
own binary, and without reading a line of this project's source code.

## What the artifact is

A **drill scorecard** is a JSON document that records the measured result
of one Kafka restore drill: which backup it restored, what RTO/RPO it
measured, whether the restored data matched the source by byte fingerprint,
who approved the drill, and more. It is published as a
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

### Route 1: `logweir drill verify` (if you have the Logweir binary)

```bash
logweir drill verify \
  --scorecard scorecard.json \
  --signature scorecard.sig \
  --public-key public.pem
```

Exit code `0` means the signature is valid and the document does not
contradict itself. A non-zero exit code (`1` for an operational problem
such as a file that will not parse, `4` for a genuine signature or
lock-proof failure) means it does not.

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

(If you would rather not install into your system Python, create a
virtual environment first: `python3 -m venv venv && venv/bin/pip install
cryptography && venv/bin/python3 verify_scorecard.py ...`. Either way, this
is the only package the script imports beyond the Python standard library —
read `docs/verify_scorecard.py` yourself to confirm that.)

Exit code `0` and a line starting `VALID` means the signature checks out.
Exit code `1` and a line starting `INVALID` (printed to stderr) means it
does not, along with the specific reason (signature mismatch, unexpected
`payloadType`, malformed base64, or a self-contradicting document).

### What "verify" actually checks

Both routes check three things, in order:

1. **The sidecar's declared `payloadType`** matches the one Logweir scorecards
   use (`application/vnd.logweir.drill-scorecard+json;version=1.0.0`). A
   sidecar for some other kind of document, even if genuinely signed, must
   not be accepted as a scorecard signature.
2. **The signature verifies** over the DSSE v1 Pre-Authentication Encoding
   (PAE) of `(payloadType, payload)`, where `payload` is the raw bytes of
   `scorecard.json` as they were read from disk — see below.
3. **The document does not contradict itself**: specifically, that
   `integrity.result` is never `"partial"` without a `partial_reason`
   explaining why. A signature only proves who wrote the bytes; it says
   nothing about whether the bytes make sense, so this check is separate
   from the cryptography.

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

`verify_scorecard.py` will **refuse** the receipt, correctly: it pins
`payloadType` to the scorecard's, and a sidecar for a different kind of
document must never be accepted as a scorecard signature. Refusal here is the
script working, not failing.

To check the receipt, reuse the same script's two cryptographic helpers —
`pae()` and `verify_signature()`, the parts that are not scorecard-specific —
against the receipt's own payload type:

```bash
python3 - receipt.json receipt.sig public.pem <<'EOF'
import base64, importlib.util, json, sys
from cryptography.hazmat.primitives import serialization

RECEIPT_TYPE = "application/vnd.logweir.drill-put-receipt+json;version=1.0.0"

spec = importlib.util.spec_from_file_location("v", "verify_scorecard.py")
v = importlib.util.module_from_spec(spec)
spec.loader.exec_module(v)

payload = open(sys.argv[1], "rb").read()          # exact bytes, never re-serialised
sidecar = json.load(open(sys.argv[2]))
key = serialization.load_pem_public_key(open(sys.argv[3], "rb").read())

if sidecar.get("payloadType") != RECEIPT_TYPE:
    sys.exit(f"INVALID: unexpected payloadType {sidecar.get('payloadType')!r}")
sig = base64.b64decode(sidecar["signatures"][0]["sig"], validate=True)
if not v.verify_signature(key, v.pae(RECEIPT_TYPE, payload), sig):
    sys.exit("INVALID: receipt signature does not verify over these bytes")

r = json.loads(payload)
print(f"VALID  receipt for {r['scorecard_key']}")
print(f"       binds to {r['scorecard_sha256']}")
print(f"       create_only_enforced={r['create_only_enforced']}  immutable={r['immutable']}")
EOF
```

Everything the [Where the public key comes from](#where-the-public-key-comes-from)
section says applies unchanged: the receipt is signed by the same publisher
key, so a receipt verified against a `public.pem` from the same bundle proves
internal consistency and nothing about authenticity.

**A receipt that is absent means no storage evidence was published for that
run** — it does not mean the upload was not create-only. The receipt is
written after the scorecard is already signed and stored, and a failure to
write it is logged and deliberately does not retract a measurement.

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

Both verifiers surface this rather than hiding it: `verify_scorecard.py`
prints, verbatim (this is the actual output — compare your terminal
against these exact characters, not a paraphrase of them):

```
VALID  run_id=01J9X2QK7C4V0R8YB3ZP6MTS5A  outcome=pass
       rto_seconds=512  rpo_seconds=0
       integrity=byte-fingerprint/pass
       approval: SELF-ATTESTED — the approval key equals the signing key
```

`logweir drill verify` prints the equivalent line as `approval:
SELF-ATTESTED — the approval key equals the signing key` among its own
report fields. Either way, the line to look for ends in `SELF-ATTESTED —
the approval key equals the signing key`.

If you are an auditor deciding how much weight to give a passing scorecard,
treat `self_attested: true` as a reason to seek additional corroboration —
an out-of-band record of the change ticket, a second reviewer, or a
separate independent drill — rather than as a defect in the artifact
itself.
