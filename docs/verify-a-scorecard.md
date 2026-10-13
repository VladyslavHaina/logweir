# Verifying a Logweir drill scorecard

How to authenticate a scorecard, check it, and read its limits, with Logweir or
with the independent Python verifier. What the evidence supports against the
backup and restore-testing clauses of DORA, ISO/IEC 27001, SOC 2, NIS2 and
HIPAA is the [control-evidence mapping](control-evidence.md).

## What the artifact is

A **drill scorecard** records one Kafka restore: its archive, measured RTO/RPO,
sampled-record fingerprints and approval. A restore compares the restored data
with **the archive**, never contacting the source cluster
(`measured.rpo_source_relative_unmeasured_reason` says so;
[what a pass does not rule out](#a-pass-compares-the-restored-topic-with-the-archive-not-with-the-source)).
`logweir backup run` does contact a source.

The scorecard is a [DSSE statement](https://github.com/secure-systems-lab/dsse):
a sidecar signs the exact payload bytes, and the JSON is never modified to carry
a signature. The [Python verifier](verify_scorecard.py) implements DSSE and the
document checks separately from Rust. Report any disagreement between the two;
neither verdict alone proves the measurements are true.

## The three files you receive

| File | Purpose |
| --- | --- |
| `scorecard.json` | The measured result, as JSON. |
| `scorecard.sig` | DSSE sidecar naming the key and the signature over the exact JSON bytes. |
| `public.pem` | The publisher's SPKI public key, authenticated independently of the handoff. |

Never substitute a retyped or reformatted scorecard
([payload handling](#the-payload-is-never-re-serialised)). A storage receipt
(`<run_id>.receipt.json`/`.sig`) and a teardown attestation
(`<run_id>.teardown.json`/`.sig`) may come with it: separate signed documents,
each with its own `payloadType`, neither needed for nor a substitute for the
scorecard.

## Where the public key comes from

**Authenticate the key through a channel independent of the document.** Whoever
substitutes a scorecard and signature can include a matching key, and `VALID`
for that bundle proves only internal consistency. Get the key from the
publisher's established TLS website, an in-person or voice exchange, or your
organization's trusted-key registry; a key in the same email, folder or archive
is not enough. Pin its fingerprint on first use:

```bash
openssl pkey -pubin -in public.pem -outform DER | openssl dgst -sha256
python3 -c "import json,sys; print(json.load(open(sys.argv[1]))['signatures'][0]['keyid'])" scorecard.sig
```

The first is the SHA-256 of the SPKI DER, the value `signatures[].keyid`
carries; the second prints the sidecar's declared key id. Resolve any mismatch
with the publisher out of band before verifying: no verifier can tell an
authorized rotation from a substitution ([key generation and rotation](keys.md)).
Keep the fingerprint with the publisher's identity, and check every replacement
key against that record.

## Two ways to verify

The readers are meant to agree on acceptance or refusal; a disagreement in
acceptance is a defect to report. Exit codes and malformed-document diagnostics
differ, as below.

### Route 1: `logweir drill verify` (if you have the Logweir binary)

```bash
logweir drill verify --scorecard scorecard.json --signature scorecard.sig --public-key public.pem
```

| Exit | Meaning |
| --- | --- |
| `0` | Signature valid; the document passes the reader's checks. |
| `1` | Operational or parsing failure, including a malformed scorecard shape. |
| `4` | Signature, lock-proof or invariant failure, including an unsupported newer format major or an inconsistent approval claim. |

A valid signature never overrides a refusal to interpret an unsupported or
self-contradictory document.

### Route 2: `verify_scorecard.py` (no Rust required)

Python 3 and the [cryptography](https://cryptography.io/) package (the
elliptic-curve and Ed25519 math):

```bash
python3 -m venv venv && venv/bin/pip install cryptography
venv/bin/python3 verify_scorecard.py scorecard.json scorecard.sig public.pem
```

From this repository's root it is `python3 docs/verify_scorecard.py …`.
`--payload-type scorecard|backup-receipt|receipt|teardown|catalog-point`
selects the document type (default `scorecard`): `backup-receipt` is a
[backup run](formats/backup-receipt.md), `receipt` a scorecard's storage
readback, `catalog-point` a [catalog point](formats/catalog-point.md), checked
for its signature only ([receipts](#verifying-the-receipts-signature)).

| Exit/output | Meaning |
| --- | --- |
| `0`, `VALID` | Signature and applicable document checks passed. |
| `1`, `INVALID` on stderr | Refusal, with a reason: signature mismatch, wrong payload type, malformed base64, unreadable input or an inconsistent document. |
| `2`, `CANNOT RUN:` and `NOTHING WAS VERIFIED` | `cryptography` is missing: install it and rerun. Not a verdict on the document. |

### What "verify" actually checks

Both routes check the sidecar's expected `payloadType`, the signature over the
DSSE PAE of the type and the raw payload bytes, then the document's rules. A
scorecard's type is `application/vnd.logweir.drill-scorecard+json;version=1.0.0`;
a signature for another type is never a scorecard signature. The media type's
`version=1.0.0` names the envelope; the document's own version is
`format_version` inside it.

The scorecard checks:

- A readable format major, checked first: major 1, and major 2 (2.0.0, a
  partition-subset restore, PROD-11.1b) only for a document carrying
  `source.selection.partitions` (arm PS-1). The eleven required blocks and six
  required non-block fields; `u64` fields in `0 <= v < 2**64`, null only for the
  optional ones.
- The four post-put `evidence` fields are zeroed; offset-report key and digest
  are present or absent together. An absent target mode means `scratch`; a
  present one is `scratch` or `newTopic`, and scratch requires a marker topic.
- A partial integrity result has a nonblank `partial_reason`; the two floats
  (`objectives.pass_rate`, `integrity.pass_rate_measured`) are finite.
- Source-capture status agrees both ways with the phase and the source-RPO
  value or reason; `last_phase_completed` is within `-1..=9`.
- RPO gaps are nonnegative; matching records never exceed sampled ones, and
  sampled records never exceed `sample.records_expected`.
- `outcome: pass` agrees with integrity, no partial reason, objectives and
  matching counts; an unmeasurable pass-rate objective is never met, and
  `pass_rate_measured` is null outside `byte-fingerprint`. Matrix `fail` has a
  reason; matrix `pass` needs a passing `byte-fingerprint` drill.
- An auth block names a supported mode (`plaintext` or `scramSha512`, and from
  format 1.5.0 also `scramSha256`, `plain` or `mtls`); a username without a mode
  is refused. `redactions` is empty.
- Each optional block appears only under a format that defines it, with its own
  arms in [the format](formats/drill-scorecard.md):
  `topic_parity.not_reconstructed` (1.2.0; each entry also in
  `unexpected_divergence`, never in `intentionally_deviated`, and in a `newTopic`
  document every divergence on a setting the restore decides is listed);
  `source.time_basis` (1.3.0); `integrity.verification` (1.4.0: a `pass` only
  over a covered, clean complete block, totals that are its partitions' sums);
  `sample.unsampled_topics` (1.6.0: never empty, sorted, never beside a
  complete verification); `source.selection` (1.7.0, a start and its end; 2.0.0,
  a partition subset, arms PS-2 to PS-5); `target.original_name` (1.8.0, never
  in scratch, never beside a subset, only with a complete verification and, for
  a one-person confirmation, every name re-typed); `approval.console` (1.9.0 and
  2.1.0, two people of one issuer).
- The claimed `approval.self_attested` agrees with a derivation from the key
  that verified the signature ([approval](#reading-approvalself_attested)).

Beside the verdict, both readers print what the document says about its own
scope. None of these lines is a refusal or changes the exit code:

```
configuration parity: NOT ASSESSED for drill-orders: configuration (captureDenied)
configuration parity: not recorded, so an empty unexpected_divergence proves nothing
reconstruction: source settings NOT RECONSTRUCTED for restore-20260907T140500Z-orders: cleanup.policy; restore-20260907T140500Z-orders: replication_factor
time basis: SELECTED BY PRODUCER TIME for orders (recorded as LogAppendTime; the approved plan states restore.time_basis: producerTime)
time basis: not recorded, so whether a time selection read a LogAppendTime topic's producer timestamps is unknown
integrity coverage: complete (compared with the archive; header order verified; application validation notAttempted)
integrity coverage: not recorded, so this verdict covered a sample, never every record
sample coverage: no partition of 1 topic(s) was sampled, because sample.max_partitions is below the number of topics with records in the window: payments; their partitions were held to the count bound only, never reconciled record by record
original name: restored under the source's own topic names, into topics this run created (a new generation of each name, not the original topic); approval subject originalName, approved by v1Approval; the target cluster is not the source cluster (5L6g3nShT-eMCtK--X86sw)
```

- `configuration parity:` names topics a 1.1.0 document did not assess (since
  FX-21 also a replication factor or partition count the source's record lacks,
  `replication_factor (notRecorded)`); its second form is every 1.0.0 document
  and a 1.1.0 one whose drill stopped before phase 7. Nothing is printed when
  every topic was assessed.
- `reconstruction:` names the source settings a `newTopic` restore did not
  reconstruct; a `newTopic` document signed before 1.2.0 prints a `not recorded`
  form naming what its writer labelled `intentionally_deviated`. Nothing for a
  scratch drill.
- `time basis:` names the topics selected by producer time, the topics selected
  by time with no recorded type, or, for every document without the block, that
  this is unknown.
- `integrity coverage:` gives a complete verification's counts (or
  `INCOMPLETE`, then `incomplete because …`), says a sampled one did not verify
  header order, and says a document before 1.4.0 covered a sample.
- `sample coverage:` names the topics `sample.max_partitions` left unsampled
  (1.6.0 or later; before 1.6.0 the absence says nothing).
- `original name:` and its owner line appear only for a 1.8.0 restore under the
  source's names; the restored topic is a new generation of the name.

A backup receipt has its own forty-five arms since format 1.8.0
([the list](formats/backup-receipt.md#the-forty-five-arms-and-the-positions-documents-fourteen)),
and for an accepted receipt both readers print, per topic, its configuration
capture (`config_coverage["orders"]: captured, message.timestamp.type
LogAppendTime from dynamicDefaultBrokerConfig`, or `config_coverage: not
recorded, …` for a receipt before 1.1.0) and, for a `LogAppendTime` topic, that
its covered window is producer time. Receipt and teardown verification apply no
scorecard rules. `logweir drill show` verifies no signature; it refuses an
unsupported newer major, and a 2.x document stating no subset (PS-1), with exit
`1`.

### The payload is never re-serialised

The verifier reads the payload as bytes (`open(path, "rb").read()`). The
signature covers trailing newlines, key order, whitespace, number formatting and
Unicode escaping, so equivalent JSON can be different bytes. **Never
pretty-print, run `jq .` over, or re-save the file before verifying it.**

### The DSSE PAE encoding, precisely

From the [DSSE v1 specification](https://github.com/secure-systems-lab/dsse/blob/master/protocol.md):

```
PAE(type, body) = "DSSEv1" SP LEN(type) SP type SP LEN(body) SP body
```

`SP` is byte `0x20`; `LEN` is the ASCII-decimal count of **bytes**, not
characters: `len(payload_type.encode("utf-8"))`, not `len(payload_type)`.
Logweir's media types are ASCII, but an independent implementation must still
count bytes.

### One asymmetry in the signature encoding

`signatures[].sig` is base64 of a DER `ECDSA-Sig-Value` (the integers `r` and
`s`) for **ECDSA P-256**, and of the raw 64-byte `R || S` for **Ed25519**. The
public key's type decides which; the sidecar's shape does not.

## Re-deriving `source.manifest_sha256` independently

```json
"source": {
  "backup_id": "backup-2026-08-30T02:00:00Z",
  "manifest_sha256": "sha256:05b3abf2579a5eb66403cd78be557fd860633a1fe2103c7642030defe32c657f",
  "manifest_version_id": null,
  "captured_by_logweir": false
}
```

The signature authenticates the claim, not what storage held. To check it,
fetch the manifest of `backup_id` (conventionally `manifest.json` beside its
segments; ask the operator for the bucket and prefix), the exact version when
`manifest_version_id` is non-null, run `sha256sum manifest.json` (or `shasum -a
256`), and compare `sha256:<hex>` with `source.manifest_sha256`. A mismatch is a
finding: a different manifest or version.

## The storage receipt: a second signed document

Every scorecard is signed with its four post-put fields zeroed:

```json
"evidence": { "create_only_enforced": false, "immutable": false,
              "retain_until": null, "version_id": null }
```

They describe an upload that has not happened yet, so read them as **no storage
proof at signing time**, never as a mutable or unprotected object. The facts
after the upload are in a separately signed receipt, `<run_id>.receipt.json`,
with a `.receipt.sig` of type
`application/vnd.logweir.drill-put-receipt+json;version=1.0.0`:

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

`create_only_enforced: true` means the store performed a conditional put;
`false` records an unsupported-operation answer and a HEAD-then-PUT fallback,
not an overwrite. `immutable` and `retain_until` need a provider readback no
backend supplies yet, so false/null is absence of proof.

### How the receipt binds to the scorecard

`scorecard_sha256` hashes the scorecard's **exact signed bytes**, so it must
equal `sha256:` plus `sha256sum scorecard.json`; a mismatch means the receipt
does not bind to these bytes. Signing the same bytes again leaves it unchanged,
and `scorecard_key` is the uploaded object you can fetch and hash yourself.

### Verifying the receipt's signature

```bash
python3 docs/verify_scorecard.py --payload-type receipt \
    '<run_id>.receipt.json' '<run_id>.receipt.sig' public.pem
```

```
VALID  receipt for logweir/drills/01M1RJZNEM507A7XQ7WCGPK6SJ.json
       binds to scorecard sha256:9b9df728…
       create_only_enforced=True  immutable=False  version_id=None
       observed_at=2026-09-05T10:49:22.143115Z
       This signature covers the receipt only. Verify the scorecard separately,
       then check sha256(scorecard.json) equals the digest above.
```

A wrong type is a refusal, and the right type still needs a valid signature
over those bytes. The [independent key](#where-the-public-key-comes-from)
applies here too. An absent receipt means no storage evidence was published
(it is written after the scorecard, and a failure is logged without retracting
the measurement), not that the upload lacked create-only protection.

## The `logweir drill show` table is a SUMMARY, not the document

`logweir drill show scorecard.json` renders fourteen fixed rows (outcome,
engine, levers, target, approval, RTO/RPO, integrity, target changes, topic
parity, evidence). The footer adds the objectives and `objectives.met` (`yes`,
`NO` or `unmeasurable`), `integrity.partial_reason`, `engine_subreport.caveat`
(or that the block is null), and `sample.unsampled_topics` when named.
`--format json` prints the signed bytes. Read the JSON for `phases`, the
manifest and mapping digests, `approval.plan_hash` and `key_id`,
`sample.records_expected` against `integrity.records_sampled` (a null measured
pass rate shows `measured —` and is not zero), `engine.matrix_verdict`,
`redactions`, and `last_phase_completed`, which a completed drill signs as `7`
because phases 8 and 9 follow the frozen payload (teardown has its own
attestation; [format](formats/drill-scorecard.md)). Attach the JSON and sidecar
to a change record: the table cannot be verified.

## What the scorecard does **not** claim

A signature authenticates the bytes; judge the claims' scope separately. The
[control-evidence mapping](control-evidence.md#what-the-evidence-does-not-show)
states these limits against each clause.

### A pass compares the restored topic with the archive, not with the source

A loss that happened when the archive was written is in both, so the drill
cannot see it. With engine 0.21.0, and unchanged on 0.23.3 (PROD-01.1 record
§11):

- **Transactions.** The backup reads uncommitted data, so aborted and open
  transactions' records and their markers are restored as ordinary records,
  visible even under `read_committed`; the drill passes. Since PROD-01.1a an
  archive holding markers is refused unless the plan states
  `restore.allow_transaction_markers: true`, and `sample.coverage_note` says so
  ([details](stability.md#an-archive-holding-transaction-markers-is-refused-unless-the-plan-allows-it));
  a transaction open at backup time leaves no marker.
- **`LogAppendTime` topics.** The archive keeps each producer's timestamp and
  the restore writes it as `CreateTime`, so restored timestamps differ and the
  drill passes. Selecting by time is refused or labelled
  ([below](#a-point-in-time-restore-of-a-logappendtime-topic-is-refused-or-labelled)).
- **Repeated header keys.** Only one copy is archived and restored, at the first
  copy's position with the last copy's value; undetected. A record that already
  carried `x-original-offset` keeps only the backup's, and a drill sampling it
  fails.
- **Out-of-order timestamps.** The engine selects segments by their first and
  last timestamps. Since PROD-01.1b the runner reads each record's own
  timestamp first: the window starts at the oldest record, and a window whose
  record the engine would skip is refused before anything is written
  ([the limitation](stability.md#out-of-order-timestamps-inside-a-segment)). A scorecard signed by
  an earlier runner may be a point-in-time pass that omitted such a record.

Order within a partition, keys, values, null versus empty, header values,
tombstones and a compacted source's records at backup time are preserved. A
broker outage or lost acknowledgement during a restore can leave duplicates and
a partial target; every measured case exited `1` and signed nothing
([stability.md](stability.md#a-broker-outage-during-a-restore-can-leave-a-partial-target-with-duplicates);
measurements in
[`to-do/decisions/PROD-01.1-record-semantics.md`](to-do/decisions/PROD-01.1-record-semantics.md)).

### A point-in-time restore of a `LogAppendTime` topic is refused, or labelled

A scorecard before format 1.3.0 says nothing about the clock (PROD-01.1 measured
one that restored, at a point in 2001, records appended in 2026, signed `pass`).
Since FX-8 a selection by time over a topic recorded as `LogAppendTime` is
refused (exit 3, `PointInTimeByProducerTime`, no scorecard) unless the plan
states `restore.time_basis: producerTime`, and the scorecard lists the topic in
`source.time_basis.producer_time` (selected by the PRODUCERS' clocks), or in
`not_recorded` when no type was recorded (a plan bound to no receipt, or one
before 1.1.0)
([the format](formats/drill-scorecard.md#sourcetime_basis-format-130)).

### The sample window is not a claim about the whole archive

The `sample` block's window, topics, partitions, counts and `coverage_note`
describe what was sampled; a byte-fingerprint pass is agreement inside it. Since
format 1.4.0, `integrity.verification.coverage: sampled` is that sample
(`header_order: notVerified`: the fingerprint sorts headers), and `complete` is
every archived segment and every restored record of every restored partition,
each mapped back by `x-original-offset`, with expected, restored, matching,
missing, unexpected, duplicate, out-of-order and different counts per partition
and in total. Read `complete.covered` first: `false` means a bound stopped it or
a partition could not be compared (`incomplete_reason`), and is never a `pass`.
A document without the block covered a sample. Complete coverage still compares
with the archive and validates no application
([the contract](to-do/decisions/PROD-08.1-integrity-contract.md)).

### What a sampled `pass` guarantees, and what it does not

A sampled verification reconciles a canary, the first `records_per_partition`
records of each SELECTED partition, and holds the restored COUNTS to the
manifest. **Only format 1.6.0 or later carries the guarantees below**: only a
build with FX-23's checks writes it, for every sampled scorecard. Both readers
say which in a line printed for every sampled `pass`:

```
sample coverage: a sampled pass at format 1.6.0 or later: every mapped partition was held to its own count bound, max_partitions reached every topic before a second partition of any, and a readable engine report lacking a partition with records in the window was refused
sample coverage: a sampled pass at format 1.4.0, before 1.6.0: it proves the canary and one count bound over every topic together, not a per-partition count bound, a sample of every topic or an engine-report check (a build from before FX-23 may have signed it)
```

At 1.6.0 or later a sampled `pass` also means:

- **Every mapped partition is inside its own count bound**, from its own
  segments: wholly inside the window counts in full, and a segment the window
  cuts counts as at least ONE record when its first or last timestamp is inside
  it, at most its whole count. A partition proved to hold records in the window
  is never empty in a `pass` (`orders/0: drill-orders/0 holds no record but the
  manifest proves at least 1 in the window [...]`).
- **The engine's offset report names every such partition.** An engine stopped
  by SIGTERM finishes its topic, exits 0 and skips the rest, so its report lacks
  them (`the engine's offset report has no entry for N mapped partition(s)
  ...`). With no readable report this check is skipped and logged.
- **`sample.max_partitions` reaches every topic before a second partition of
  any**; topics it could not reach are named in `sample.unsampled_topics`,
  counted but never reconciled.

It still does not establish that every record of a partition is the archive's,
that header order was kept, or more than one record of a straddling segment:
ask for `sample.coverage: complete`. Since PROD-01.1b the bounds use each
segment's oldest and newest record; an earlier runner's assumed its timestamps
do not run backwards
([the limitation](stability.md#out-of-order-timestamps-inside-a-segment)).
A sampled 1.4.0 or 1.5.0 document, from before FX-23, was judged by one count
bound over all topics, and its `max_partitions` kept the first partitions in
manifest order: an engine stopped early could leave later topics empty under a
`pass` (measured: a SIGTERM mid-restore signed `pass` with the second topic
empty; it is now `fail-integrity`). Read its `sample.partitions` against the
partitions the archive holds in the window.

### The `evidence` block is not a finding about your bucket

The post-put fields are zeroed before signing; storage facts are in the
[separate receipt](#the-storage-receipt-a-second-signed-document).

### An empty `unexpected_divergence` is not configuration parity

`topic_parity` compares each restored topic's configuration with the
manifest's record of the source's, which is empty whenever the backup could not
read it. So an empty list is parity only for a topic `topic_parity.not_assessed`
does NOT name, in a 1.1.0 document that has the field
([the reasons](formats/drill-scorecard.md#topic_parity-and-what-its-silence-means)).
Since 1.1.0 each such topic also has an `unexpected_divergence` entry
`"<target topic>: configuration not assessed (<why>)"` (FX-21's twin:
`replication_factor not assessed (notRecorded)` or `partition_count`), so an
older reader sees a divergence, never a clean list; a key never contains a
space.

**A replication factor is parity only where the source's is recorded**, from
the manifest or else the bound receipt's `topic_configuration`. Before FX-21 the
target's factor was compared with itself where the manifest lacked the source's
(engine 0.23.3 omits it for every topic after the first). A runner-signed
document whose `engine.version` is `0.23.3+logweir.2` or later was written with
FX-21; for any other, read `topics[].source_replication_factor` in the archive's
manifest: without it there is no replication-factor finding.

**"Assessed" covers explicit topic-level OVERRIDES** on the engine's 24-key
allowlist. A value inherited from a broker default is never compared: a
`LogAppendTime` broker default restored as `CreateTime` shows no divergence
(measured by FX-4). The receipt's `config_coverage` records the effective value;
comparing other effective values is PROD-05.1's.

### A `newTopic` restore does not reconstruct the source's topic settings

A restore creates its topics at the plan's replication factor, with
`retention.ms=-1` and the target broker's `cleanup.policy`. In a scratch drill
these are intended (`intentionally_deviated`); in a `newTopic` restore they are
NOT reconstructed, listed in 1.2.0's `topic_parity.not_reconstructed` and in
`unexpected_divergence`. **A `newTopic` scorecard before 1.2.0 labels them
`intentionally_deviated`; that label is the defect FX-3 fixed, not intent**
([stability.md](stability.md#a-newtopic-restore-does-not-reconstruct-the-sources-topic-settings)).

### `engine_subreport` corroborates nothing about Logweir's integrity claim

Logweir never runs the engine's `validation run`, so scorecards carry
`engine_subreport: null`, and phase 8's missing-report message is in the logs.
Populated fixtures are hand-written format examples. For a populated report,
base64-decode `body_b64` and check `body_sha256`. The pinned upstream source
constructs `integrity.checksums_valid` as literal `true`
(`kafka-backup-core/src/evidence/emit.rs:109`) and the restore times as `None`
(`emit.rs:100–104`), so it corroborates neither integrity nor RTO. A matching
digest binds the bytes, not who produced them.

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

Self-attested means the approval key is the signing key: no separation at the
key level, which says nothing about the humans behind them. Logweir permits and
labels such runs. **Both verifiers derive it** from `approval.key_id` and the key
that verified the signature, and refuse a contradictory claim (Rust exit `4`,
Python exit `1`, with `INVALID: ` before):

```
APPROVAL CLAIM NOT VERIFIED: the document claims self_attested=true but the approval key id <a> does not match the verifying key id <b>
APPROVAL CLAIM NOT VERIFIED: the document claims self_attested=false but the approval key id <a> matches the verifying key id <b>
approval: SELF-ATTESTED — the approval key equals the signing key
evidence: the four post-put fields are zeroed before signing; the storage facts live in the receipt
```

The third is a derived true; the fourth ends every successful Python check.
`drill show` has no key, so it prints `SELF-ATTESTED (claimed; run 'drill
verify' to check it against the signing key)`, which is not a verification. The
rule added no field to `format_version: 1.0.0`. Treat self-attestation as a
reason to seek corroboration (the ticket, another reviewer, an independent
drill), not as a defect in the artifact.

### What the `verifier:` line means, and why its version moves

The Python report ends with `verifier: verify_scorecard.py 1.30.0` and the
checks applied. That is the **verifier's** version, not the document's
`format_version` (scorecard `1.0.0`; `1.1.0` FX-4; `1.2.0` FX-3; `1.3.0` FX-8;
`1.4.0` PROD-08.1; `1.5.0` PROD-01.3's auth modes; `1.6.0` every sampled drill
since FX-23; `1.7.0` a window start, PROD-11.1; `1.8.0` original names,
PROD-15.1; `1.9.0`/`2.1.0` a console approval, PROD-16.2; `2.0.0` a partition
subset, PROD-11.1b). It moves when the accepted-document set changes:

| Version | Changed checks |
| --- | --- |
| `1.1.0` | Rejects nonzero post-put evidence fields in `1.0.x` scorecards. |
| `1.2.0` | Derives self-attestation from the verified key; rejects contradictory claims. |
| `1.3.0` | Rejects blank partial reasons and nonempty redactions. |
| `1.4.0` | Outcome against integrity, partial reason, objectives and matching counts; sampled counts bounded by expected; matrix pass needs a passing byte-fingerprint drill. |
| `1.5.0` | Requires `sample` and integer `sample.records_expected`. |
| `1.6.0` | The eleven required blocks in Rust declaration order; every `u64` in `0 <= v < 2**64`. |
| `1.7.0` | The six non-block fields required; null refused for non-optional `u64` (the five optional ones allow it). |
| `1.8.0` | Type-checks the six non-block fields (`run_id: 42`, `phases: "x"`, `requested_at: 5`). |
| `1.9.0` | Backup-receipt verification; scorecard auth-field consistency. |
| `1.10.0` | Auth modes `plaintext` or `scramSha512` (versioned in `1.21.0`). |
| `1.11.0` | Offset-report key and digest present or absent together. |
| `1.12.0` | A marker topic unless target mode is `newTopic`. |
| `1.13.0` | Refuses target modes other than `scratch`/`newTopic`, null included. |
| `1.14.0` | `--payload-type catalog-point`, signature only. |
| `1.15.0` | Scorecard and receipt `1.1.0`: receipt `config_coverage` arms 6–11 and lines; `not_assessed` arrays; the configuration-parity line. |
| `1.16.0` | Receipt and catalog-point `1.2.0` (FX-7): `archive.manifest_version_id` a string; prints the pinned version. |
| `1.17.0` | Scorecard `1.2.0` (FX-3): NR-1 to NR-5, the reconstruction line; refuses non-string parity lists and a major Rust cannot parse (`" 1.0.0"`). |
| `1.18.0` | Scorecard `1.3.0` (FX-8): TB-1 to TB-4, the `time basis:` lines (and a receipt's per `LogAppendTime` topic). |
| `1.19.0` | Scorecard `1.4.0` (PROD-08.1): IV-1 to IV-7 and 25 nested counts (24 refuse null), the `integrity coverage:` lines; blank means exactly what Rust's `trim` strips. |
| `1.20.0` | Receipt and catalog-point `1.3.0` (PROD-05.1): `topic_configuration` arms 12–19, `owner_detection` 20–21, and their lines (never a configuration value). |
| `1.21.0` | Scorecard `1.5.0`, receipt `1.4.0` (PROD-01.3): `scramSha256`, `plain`, `mtls` only from those versions. |
| `1.22.0` | Scorecard `1.6.0` (FX-23): US-1 to US-3, both `sample coverage:` lines. |
| `1.23.0` | Scorecard `1.7.0` (PROD-11.1): SEL-1 to SEL-3, the `replay selection:` line, and the window-qualified `sample coverage:` line. |
| `1.24.0` | Receipt `1.5.0` (PROD-03.0): `schema_dependency` arms 22–29 and lines. |
| `1.25.0` | Receipt and catalog-point `1.6.0` (PROD-01.4a): `generations` arms 36–40 and lines; a catalog point's topic IDs must be real. |
| `1.26.0` | Receipt and catalog-point `1.7.0` (PROD-04.1): `consumer_positions` arms 30–35; with `--consumer-positions <file>`, the positions document's CP-1 to CP-14, and one line per position. |
| `1.27.0` | Scorecard `2.0.0` (PROD-11.1b, OD-9 (a)): major 2 only with `source.selection.partitions` (PS-1), PS-2 to PS-5; a newer major is refused naming `2.0.0`. |
| `1.28.0` | Scorecard `1.8.0` (PROD-15.1): ON-1 to ON-14, the two `original name:` lines. |
| `1.29.0` | Scorecard `1.9.0` and `2.1.0` (PROD-16.2): CA-1 to CA-8, ON-5 by version, the `console approval:` line. |
| `1.30.0` | Receipt `1.8.0` (PROD-02.1): `source_watermarks` arms 41–45; an empty capture only beside the block. |

Every document without a version's new field is decided as before it. One
diagnostic-order difference remains: with both `run_id` and `engine` absent,
Python names `engine` and Rust `run_id`; both refuse.

**What an older reader does with a newer document.** The major is unchanged
except where said, so it accepts and prints less:

| Reader older than (and a `logweir` before) | Given | It |
| --- | --- | --- |
| `1.15.0` (FX-4) | a 1.1.0 receipt or scorecard | accepts it (majors only) and prints neither coverage line; a not-assessed topic still shows as its `unexpected_divergence` entry |
| `1.16.0` (FX-7) | a 1.2.0 receipt or catalog point | ignores `archive.manifest_version_id` ([versions](formats/backup-receipt.md#versions)) |
| `1.17.0` (FX-3) | a 1.2.0 scorecard | checks no NR arm; an unreconstructed setting still shows under `unexpected`, never as intended |
| `1.18.0` (FX-8) | a 1.3.0 scorecard | ignores `source.time_basis` |
| `1.19.0` (PROD-08.1) | a 1.4.0 scorecard | ignores `integrity.verification`: a complete verification reads as a sample of every expected record |
| `1.20.0` (PROD-05.1) | a 1.3.0 receipt or catalog point | ignores `topic_configuration` and `owner_detection` |
| `1.21.0` (PROD-01.3) | a 1.5.0 scorecard or 1.4.0 receipt naming a new auth mode | **refuses** it, the safer verdict (OD-7, third case); documents naming `plaintext` or `scramSha512` keep their earlier version |
| `1.22.0` (FX-23) | a 1.6.0 scorecard | ignores `sample.unsampled_topics` |
| `1.23.0` (PROD-11.1) | a 1.7.0 scorecard | ignores `source.selection`; what it prints is true, but it names no start (`sample.coverage_note` does, [§5.3, §5.4](to-do/decisions/PROD-11.1-replay-selection.md)) |
| `1.25.0` (PROD-01.4a) | a 1.6.0 receipt or catalog point | ignores `generations`: it cannot say whether a topic was recreated |
| `1.26.0` (PROD-04.1) | a 1.7.0 receipt | ignores `consumer_positions` (use `--consumer-positions` with 1.26.0 or later) |
| `1.27.0` (PROD-11.1b) | a 2.0.0 or 2.1.0 document | **refuses** it by its major (`format_version 2.0.0 has a major version newer than this reader understands`; measured by `e2e/tests/replay_selection.rs::older_readers_refuse_a_2_0_0_document`) |
| `1.28.0` (PROD-15.1) | a 1.8.0 scorecard | prints `VALID` and the true coverage lines, but not that the restore wrote under the original names (measured on ten live scorecards) |
| `1.29.0` (PROD-16.2) | a 1.9.0 scorecard | accepts it without a `console approval:` line (an older `drill verify` prints the approver with no key id); 1.28.0 refuses such an original-name scorecard |

On a 2.0.0 document, `verify_scorecard.py` `1.16.0` to `1.24.0` exit 1 naming
the format they know, a `logweir` before PROD-11.1 exits 4 with the same
sentence, and one between PROD-11.1 and PROD-11.1b exits 4, or 1 for a subset
from the archive's floor (`missing field `window_start_ms``); none prints
`VALID`, and the media type stays `version=1.0.0`, so the refusal is never a
payload-type mismatch. `1.22.0` accepts a 1.7.0 receipt whose captured group
lists no position (measured).

**Rerun the current verifier over retained documents checked with older
versions.** An earlier `VALID` may reflect weaker checks; the bytes and
signatures are unchanged, only the reader's rules are stricter. Keep the
verifier version with your audit record, not just the verdict.

---

Documentation is licensed [CC-BY-4.0](LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
