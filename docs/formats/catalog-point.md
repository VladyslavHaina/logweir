# The recovery catalog point record, field by field

`application/vnd.logweir.catalog-point+json;version=1.0.0`

The machine-readable schema is
[`schemas/logweir-catalog-point-1.6.0.json`](../../schemas/logweir-catalog-point-1.6.0.json),
regenerated from the Rust type by `just schema` and `diff -u`'d against the
checked-in file by `just schema-check`, so this document and the schema cannot
drift apart silently. A MINOR bump is a new schema file beside the old one: the
[`1.5.0` schema](../../schemas/logweir-catalog-point-1.5.0.json) (PROD-03.0,
`topics[].schema_dependency`), which describes the records written before
PROD-01.4a, the
[`1.4.0` schema](../../schemas/logweir-catalog-point-1.4.0.json) (PROD-01.3's
auth modes) and the [`1.3.0` schema](../../schemas/logweir-catalog-point-1.3.0.json)
(PROD-05.1, `topics[].configuration`), which describe the records written
before PROD-03.0, the
[`1.2.0` schema](../../schemas/logweir-catalog-point-1.2.0.json) (FX-7,
`archive.manifest_version_id`) and the
[`1.1.0` schema](../../schemas/logweir-catalog-point-1.1.0.json) (FX-4,
`topics[].config_coverage`), which describe the records written before
PROD-05.1, and the
[`1.0.0` schema](../../schemas/logweir-catalog-point-1.0.0.json), which
describes every record written before format 1.1.0, are frozen beside it and
never regenerated.

If you are verifying a document rather than producing one, read
[../verify-a-scorecard.md](../verify-a-scorecard.md) for the mechanics of a
detached DSSE sidecar; everything it says about verifying-as-read applies here
unchanged. **Read [what a signature on this document proves](#what-a-signature-on-this-document-proves)
before you act on one.**

## What this is

A recovery point is a thing an operator needs long after the `Backup` object
that produced it is gone: after a namespace is deleted, after a cluster is
rebuilt, on a fresh installation that never saw the custom resource. Before
this format the only durable record of one was the signed backup receipt
([backup-receipt.md](backup-receipt.md)), keyed by `backup_id`, which carries
no ordering and no index.

The catalog is durable, append-only, create-only metadata in the archive's own
evidence root:

```
logweir/catalog/v1/points/<pointId>/record.json   # the signed point record — THIS document
logweir/catalog/v1/points/<pointId>/record.sig    # its detached DSSE sidecar
logweir/catalog/v1/log/<yyyy>/<mm>/<dd>/<recoveryPointAtMs:013>-<pointId>.json
```

The third key is a tiny, **unsigned** index entry. It exists so that listing a
page of recovery points costs one bounded listing rather than one `get` per
point, and every field in it is a copy of a field in the signed record beside
it. It is a pointer, not evidence: a reader that needs to TRUST a value fetches
`record_key` and verifies it.

Who writes these objects:

* `logweir backup run`, as a fifth create-only put immediately after the
  receipt and its sidecar. A failed catalog write is a **warning**: it does not
  change the exit code, the receipt, or the two evidence keys the run prints.
  By then the archive exists and its evidence is signed and uploaded, so a
  bucket that refused a metadata put must not turn a good backup into a failure
  an operator has to investigate.
* `logweir catalog sync`, which backfills exactly that case — and every archive
  written before this format existed.

## Point identity

```
pointId = "lwp1-" + lowercase_hex(sha256(receipt bytes))[0..32]
```

over the **exact stored bytes** of the signed backup receipt. Four consequences,
all of them wanted:

1. **It is content-derived**, so the same archive copied to a second bucket is
   one point in two places rather than two points. Two records for one id that
   differ only in `archive.location_id` describe one point; two that differ in a
   receipt-derived fact are a `Conflict` (below). A pinned manifest version
   (FX-7) does not change that: a version id belongs to the bucket that issued
   it, so a copy — which carries the pin and not the version — is checked by its
   manifest digest, and says that the pin could not be checked there.
2. **Anyone holding the receipt can compute it**, including a fresh
   installation that never saw the `Backup` object.
3. **It cannot be forged into another point's identity** without breaking the
   receipt's signature.
4. **Two receipts under one `backup_id` are two points.** Tracker defect
   `RECEIPT-DUP`: a Backup Job re-created from its frozen inputs wrote a second
   run-id receipt under the same execution id while overwriting the manifest at
   the same key. Run identity is idempotent; signed evidence is not. An identity
   taken from the `backup_id` or from the manifest digest would collapse those
   two runs and silently drop the older receipt's window. The overwrite itself
   is now prevented at the source — a run must win a create-only
   [execution claim](backup-receipt.md#the-execution-claim-one-engine-run-per-backup_id)
   before its engine starts, and (FX-7) must find the set's directory empty,
   so a new execution signs one receipt and never writes into a set an older
   build wrote — but sets written by older builds can still hold two receipts,
   and this rule is what keeps both of them visible.

`backup_id` remains the **archive set** identifier, and `pointId` is the
**recovery point**. The 128-bit id is a display and lookup key; the full
`receipt.sha256` travels beside it and is the binding.

## The document

```json
{
  "format_version": "1.0.0",
  "point_id": "lwp1-…",
  "recorded_at": "2026-09-16T00:00:00Z",
  "receipt": {
    "key": "logweir/backups/nightly-20260915/01J….receipt.json",
    "sha256": "sha256:…",
    "sidecar_key": "logweir/backups/nightly-20260915/01J….receipt.sig",
    "payload_type": "application/vnd.logweir.backup-receipt+json;version=1.0.0"
  },
  "backup_id": "nightly-20260915",
  "run_id": "01J9X2QK7C4V0R8YB3ZP6MTS5A",
  "archive": {
    "location_id": "s3://kafka-backups/prod",
    "manifest_key": "prod/nightly-20260915/manifest.json",
    "manifest_sha256": "sha256:…",
    "prefix": "prod"
  },
  "covered": { "from_ms": 1757980800000, "to_ms": 1757984400000 },
  "capture": {
    "started_at": "2026-09-15T03:00:00Z",
    "finished_at": "2026-09-15T03:04:00Z"
  },
  "topics": [{ "name": "orders", "records": 1234 }],
  "source": {
    "cluster_id": "SOURCE-CLUSTER-000001",
    "bootstrap_servers": ["kafka-source:9092"],
    "auth_mode": "scramSha512"
  },
  "signing": { "key_id": "<sha256 of the DER SPKI>", "algorithm": "ecdsa-p256-sha256" },
  "installation": { "key_id": "<sha256 of the DER SPKI>" }
}
```

### Identity and provenance

| Field | Type | Meaning |
|---|---|---|
| `format_version` | string | Semver of THIS format, independent of the receipt's and the scorecard's. Major `1`; this build writes `1.6.0` for a record whose receipt carries `generations` (PROD-01.4a: every receipt it signs), else `1.5.0` for a record whose receipt carries `schema_dependency` (PROD-03.0), else `1.4.0` for a record whose `source.auth_mode` is `scramSha256`, `plain` or `mtls` (PROD-01.3), else `1.3.0` for a record whose receipt carries `topic_configuration`, else `1.2.0` for a record that carries `archive.manifest_version_id`, else `1.1.0`. |
| `point_id` | string | `lwp1-` + 32 lowercase hex. See [Point identity](#point-identity). |
| `recorded_at` | RFC 3339 | When the RECORD was written. **Not** a fact about the backup. |
| `receipt.key` / `.sidecar_key` | string | Where the signed backup receipt and its sidecar are, in this archive's evidence root. |
| `receipt.sha256` | `sha256:<hex>` | Over the receipt bytes exactly as stored. **The binding.** |
| `receipt.payload_type` | string | The receipt's media type, so a reader knows which verifier to run without guessing from the bytes. |
| `backup_id` | string | The archive SET. Two runs appending to one set share it. |
| `run_id` | string | The run that produced the receipt. |
| `signing.key_id` / `.algorithm` | string | The key that signed THIS record. `key_id` is the SHA-256 of the DER SPKI — the same number `openssl` prints ([keys.md](../keys.md)) — and `algorithm` is a **closed set of two**, `ecdsa-p256-sha256` or `ed25519`, spelled exactly as the installation's public identity ConfigMap spells it. The schema publishes both values as an `enum`; `p256` is not one of them. |
| `installation.key_id` | string, **optional** | The key under which the writer VERIFIED the receipt — i.e. the installation that produced the backup, which is a different question from who wrote this record. On a backfill by another installation the two differ. |

### The archive

| Field | Type | Meaning |
|---|---|---|
| `archive.location_id` | string | `s3://<bucket>/<prefix>`, `gs://…`, `az://<account>/<container>/…` or `file://<path>` — **bucket and prefix only**. Never an endpoint, never a region, never a credential. It is deliberately NOT part of the identity, which is what makes one archive in two buckets one point in two places. |
| `archive.manifest_key` | string | Receipt-derived. |
| `archive.manifest_sha256` | `sha256:<hex>` | Receipt-derived. |
| `archive.manifest_version_id` | string, **optional** (format `1.2.0`) | Receipt-derived: the receipt's pinned manifest version ([backup-receipt.md](backup-receipt.md#the-pinned-manifest-version-versioned-buckets)), present exactly when the receipt carries one — a point taken on a versioned bucket. Absent means unknown. A reader takes the pin from the verified RECEIPT, never from this copy; a record whose copy differs from the receipt's, or that carries one the receipt does not, is a mismatch (`Conflict`), while a record without one (an older writer) is not. The pin is checked only in a bucket that holds the pinned version: there, a pinned version that is no longer current is `Conflict`; elsewhere (a copy, an unversioned bucket, a version that was expired or deleted) the digest decides — which an identical manifest over rewritten segments passes — and the entry's remedy says the pin could not be checked ([backup-receipt.md](backup-receipt.md#the-pinned-manifest-version-versioned-buckets)). |
| `archive.prefix` | string | The archive's own key prefix, as the receipt records it. |

### What was captured

| Field | Type | Meaning |
|---|---|---|
| `covered.from_ms` | int | INCLUSIVE start, epoch milliseconds. |
| `covered.to_ms` | int | **EXCLUSIVE** end, epoch milliseconds — the receipt's own half-open convention, copied and never reinterpreted. A restore's inclusive point in time is therefore `to_ms - 1`. |
| `capture.started_at` | RFC 3339 | The engine subprocess's start. **This is the recovery point** — see below. |
| `capture.finished_at` | RFC 3339 | The engine subprocess's end. |
| `topics[].name` | string | One entry per topic the receipt names. |
| `topics[].records` | int | Records this run captured for the topic. |
| `topics[].config_coverage` | object, **optional** (1.1.0) | The backup receipt's [`config_coverage`](backup-receipt.md#config_coverage--topic-configuration-capture-coverage-format-110) entry for the topic, COPIED: `coverage` (`captured`, `notCaptured`, `captureDenied`), `reason` for `notCaptured`, and the effective `timestamp_type` with its `source`. Receipt-derived: a record whose copy its receipt does not back is a `RecordMismatch` (rule 3). ABSENT means UNKNOWN — every 1.0.0 record, and every record derived from a receipt that predates 1.1.0 — and is never read as `captured`. |
| `topics[].partitions` | int, **optional** | The source's partition count, receipt-derived from the receipt's [`topic_configuration`](backup-receipt.md#topic_configuration--the-topic-configuration-model-format-130) (format 1.3.0): the count the archive manifest records and a restore creates the topic with. ABSENT — every record before 1.3.0, and a 1.3.0 one whose manifest recorded none — is unknown. See [absent means unknown](#absent-means-unknown-never-zero). |
| `topics[].configuration` | object, **optional** (1.3.0) | The backup receipt's [`topic_configuration`](backup-receipt.md#topic_configuration--the-topic-configuration-model-format-130) entry for the topic, COPIED: `partitions`, `replication_factor`, the recorded `entries` with their `source` and `portability`, and the declarative `owner`. Receipt-derived: a record whose copy its receipt does not back is a `RecordMismatch` (rule 3). ABSENT means NOT RECORDED — every record before 1.3.0 — and is never read as "no configuration". |
| `topics[].schema_dependency` | object, **optional** (1.5.0) | The backup receipt's [`schema_dependency`](backup-receipt.md#schema_dependency--does-a-restore-need-a-schema-registry-format-150) entry for the topic, COPIED: the `verdict` (`schemaDependent`, `notDetected`, `notAssessed`), its `basis` or `reason`, and the `key` and `value` sides with their framed counts and schema ids. A `schemaDependent` topic reads "schema-dependent, registry not captured": its archived records name schema ids a registry issued, and Logweir captures none. Receipt-derived: a record whose copy its receipt does not back is a `RecordMismatch` (rule 3). ABSENT means NOT ASSESSED — every record before 1.5.0 — and is never read as "not schema-dependent". |
| `topics[].identity` | object, **optional** (1.6.0) | The backup receipt's [`generations`](backup-receipt.md#generations--the-topics-id-before-and-after-the-engine-format-160) entry for the topic, COPIED: `topic_id` (before the engine) and `topic_id_after`, each Kafka's text or `null` with its reason, and `topic_id_source`. It is what tells a topic deleted and recreated under the same name — a NEW generation, whose offsets mean other records — from the same topic. Receipt-derived: a record whose copy its receipt does not back is a `RecordMismatch` (rule 3). ABSENT means UNKNOWN — every record before 1.5.0 — and is never read as "the same generation". |
| `owner_detection` | string[], **optional** (1.3.0) | The backup receipt's [`owner_detection`](backup-receipt.md#topic_configuration--the-topic-configuration-model-format-130), COPIED: where the run looked for declarative owners (`declared`, `kafkaTopicResources`). EMPTY means it looked nowhere, so a topic without an `owner` has its owner NOT CHECKED — never "applied through the admin API". Receipt-derived (rule 3). ABSENT means NOT RECORDED — every record before 1.3.0. |
| `source.cluster_id` | string | Read from the broker at admission and carried by the receipt — never from a spec. |
| `source.bootstrap_servers` | string[] | Addressing. |
| `source.auth_mode` | string | The receipt's `source.auth.mode`, copied: `plaintext` or `scramSha512`, and from 1.4.0 also `scramSha256`, `plain` or `mtls` — the receipt's versioned closed set. |

**Nothing in this document may hold a credential**, and there is deliberately no
`username` even though the receipt has one: a catalog is the surface an operator
lists in bulk, and a SASL principal is not a fact a recovery point needs. The
1.3.0 `topics[].configuration` copy holds no secret either: an entry the broker
flags sensitive is recorded by key with no value (class `secret`), and an
owner's `reference` names where desired state lives, never a credential.

### Provenance (optional, and mostly absent on this build)

`execution` describes the Kubernetes execution that produced the backup —
`kind`, `namespace`, `name`, `uid`, `execution_id`, `inputs_sha256`,
`schedule{name,uid,slot}`, `triggered_by`. Every field is optional.

`inputs_sha256` and `execution_id` belong to the frozen `execution-inputs.json`
grammar. This record **cites** them and never redefines them: it does not
describe what goes into that digest and does not recompute it.

What this build fills, and what it does not:

* **`triggered_by` IS filled**, from the receipt's own field, whenever the
  receipt carries a non-empty one. An empty `triggered_by` is not copied: `""`
  means the operator said nothing, and writing it would turn an absence into a
  value.
* **Everything else is absent.** Not because the controller does not know it —
  it records `Backup.status.execution` and freezes `execution-inputs.json` —
  but because the Backup **Job's argv carries the runner no execution
  identity**: it passes `--backup-id-override <execution_id>` and no namespace,
  name, UID or `inputsSha256`. A backfill by `logweir catalog sync` knows even
  less: a receipt and a bucket and no Kubernetes object at all.
* **`execution_id` stays absent even on a controller-driven run**, although
  `backup_id` happens to equal it there. The runner cannot tell an execution id
  from a schedule slot — `--backup-id-override` carries both — and a field that
  is right on one path and a fabrication on the other is worse than an absent
  one.
* A block that would establish nothing is written as **absent**, never as an
  object of nulls: absent is the one spelling of unknown.

**An absent block, or an absent field inside one, means UNKNOWN** — never
"no execution".

### The recovery point is the CAPTURE START

Freshness is measured from `capture.started_at`, not from `covered.to_ms`.
`covered.to_ms` is the newest *record* instant, so an idle topic would look
stale forever; `capture.finished_at` would under-report the gap for a
long-running capture. The day shard and the millisecond in the log key are both
taken from `capture.started_at`, so "how old is my newest recoverable backup"
and "which shard is it in" are one number.

## Reading rules a consumer must honour

1. **`format_version`'s major must be `1`.** A higher major makes that ENTRY
   unsupported; it never aborts a walk. A catalog written by a newer Logweir
   still lists, with the entries this build cannot read marked as such. The
   schema pins the major with a pattern (`^1\.[0-9]+\.[0-9]+$`) as well, so a
   schema-only validator refuses a `9.9.9` document too.
2. **Unknown fields are ignored inside major 1.** A minor bump adds optional
   fields and a 1.0.0 reader must still read a 1.1.0 to 1.6.0 record.
3. **Absent optional fields mean UNKNOWN — never zero.** See below.
4. **Everything except the receipt-derived facts is informational.** The
   receipt's signature is the verification root. `backup_id`, `run_id`,
   `covered`, `capture` and `archive.manifest_key`/`manifest_sha256` are
   recomputed from the verified receipt, and a record whose copies disagree is
   reported as a mismatch (availability `Conflict`) rather than believed.
   `topics[].config_coverage` (1.1.0) is receipt-derived too, one way: a record
   may carry LESS than its receipt (an older writer copies nothing) but never a
   coverage the receipt does not carry — `captured` beside a `captureDenied`
   receipt, or any coverage beside a 1.0.0 receipt, is a `RecordMismatch`. Two
   records of one point conflict on it only where both carry an entry.
   `topics[].configuration` and `topics[].partitions` (1.3.0) follow the same
   one-way rule: a model or a count the receipt does not back — an override
   added, an owner dropped, a class changed — is a `RecordMismatch`, and so is
   an `owner_detection` the receipt does not carry (a record claiming the run
   looked for owners it never looked for). Two records conflict on either only
   where both carry it. `topics[].identity` (1.6.0) follows the same rule: a
   topic ID the receipt does not back — one swapped, a recreation during the
   capture hidden, IDs beside a receipt that records none — is a
   `RecordMismatch`, and two records conflict on it only where both carry it.
5. **Nothing under `logweir/` is ever rewritten.** Every put is create-only. A
   correction is a new record under a new point id; a removal is a tombstone.
   An existing object at a record key is "already there", which is a success,
   not an overwrite.

### Absent means unknown, never zero

This is the rule that is easiest to get wrong and most expensive to get wrong.

* absent `topics[].partitions` → the partition count is **unknown**. A `0` would
  read as "this topic has no partitions" and would let a size filter accept a
  point it knows nothing about.
* absent `execution` → provenance **unknown** (an imported archive from another
  installation).
* absent `installation` → the writing installation is **unknown**.
* absent `topics[].configuration` → the topic's configuration model is **not
  recorded**; a restore that needs it knows none and says so.
* absent `topics[].schema_dependency` → whether that topic's records need a
  schema registry is **not assessed**, never "not schema-dependent"; the
  catalog's view, the API and the console say "not assessed".
* absent `topics[].config_coverage` → whether that topic's configuration was
  captured is **unknown**, and never `captured`. A restore does not read this
  copy at all: its configuration parity takes coverage from the bound point's
  VERIFIED receipt, and says `not assessed` when that receipt has none.

Absent fields are absent from the bytes, not `null`, so a reader in any language
sees nothing rather than a value.

## What a signature on this document proves

**It proves the bytes were signed by the holder of a key. It is not a claim
that the point is available, that its archive is readable, or that its copied
facts are true.**

Availability and verification are separate axes and both are separate from this
signature:

* *availability* — can the receipt, sidecar and manifest still be fetched, and
  does the manifest's digest still equal the receipt's — and, for a point that
  pins a manifest version, is that version still the current one? Only a fetch
  answers that, and this document is not one.
* *verification* — does the backup receipt's DSSE signature verify under a key
  you trust for evidence signing? That is the receipt's signature, not this one.

Both readers say so in as many words. Besides the signature they make ONE
check of the record's own content (PROD-01.4a, review M1): every topic ID it
copies (`topics[].identity.topic_id`, `.topic_id_after`) must be a real topic
ID in Kafka's text — never one of Kafka's reserved IDs
(`AAAAAAAAAAAAAAAAAAAAAA`, `AAAAAAAAAAAAAAAAAAAAAQ`), never another alphabet.
A record that copies one is refused by both (`drill verify` exit 4, the script
exit 1) with the same words, as the receipt it claims to copy would be by its
arm 38. Nothing else of this type is checked:

```
logweir drill verify --payload-type catalog-point \
  --scorecard record.json --signature record.sig --public-key public.pem

python3 docs/verify_scorecard.py --payload-type catalog-point \
  record.json record.sig public.pem
```

`scripts/check-verifier-parity.sh` walks the catalog-point documents — a good
one of each minor, one with a byte flipped after signing, a genuine one
presented as a scorecard, and a 1.6.0 one copying Kafka's reserved topic ID —
and fails if the two readers disagree about the verdict or the refusal text,
or if either stops printing the sentence that says what it checked.

The verification an auditor actually wants is two steps:

1. verify this record, to learn which receipt it points at;
2. fetch that receipt and verify it with `--payload-type backup-receipt`, then
   check `sha256(receipt bytes)` equals `receipt.sha256` here.

**A public key found beside an archive is a claim and is never trusted merely by
proximity** ([keys.md](../keys.md)). `logweir catalog sync` requires at least one
explicit `--public-key` and writes no record for a receipt that verifies under
none of them.

## Compatibility

* **Versioned twice**: in the key path (`catalog/v1/`) and in each record's
  `format_version`. A future `v2` writes under `logweir/catalog/v2/` and
  dual-reads during a documented window.
* **Refusal is per entry, not per catalog.** A `v1` reader meeting a major-2
  record marks that entry and keeps going.
* **A minor bump adds optional fields only.** Unknown fields inside major 1 are
  ignored, so a 1.0.0 reader reads a 1.1.0 record. **1.1.0 (FX-4)** is the first:
  `topics[].config_coverage`. The day-sharded index entry did not change and
  stays `format_version: 1.0.0`. An older `logweir` writing records from a
  1.1.0 receipt writes them without the field (coverage unknown), which
  `cross_check` accepts; a 1.0.0 record read by this build has no coverage and
  is never upgraded to `captured`. **1.2.0 (FX-7)** is the second:
  `archive.manifest_version_id`, written only for a point whose receipt pins
  its manifest's version; every other record this build writes stays 1.1.0. A
  record without it (an older writer, or an unversioned bucket) is unknown, and
  `cross_check` accepts it; one whose copy differs from the receipt's is a
  mismatch. **1.3.0 (PROD-05.1)** is the third: `topics[].configuration`, and
  the existing `topics[].partitions` filled, from a receipt that carries
  `topic_configuration` — every receipt a build from PROD-05.1 signs, so every
  record such a build writes is 1.3.0 or later, pinned or not (1.5.0 from
  PROD-03.0 and 1.6.0 from PROD-01.4a, below). A record backfilled from an older receipt
  keeps the format it would have had. The catalog's view lists a point's topics
  with their recorded layout from these fields (`PointView.topics[]` in the
  product API) for an `Available` point only.
  **1.4.0 (PROD-01.3)** is the fourth: no new field, three new values of
  `source.auth_mode` (`scramSha256`, `plain`, `mtls`), written only for a
  point whose receipt names one (and so is itself 1.4.0); every other record
  stays as above. No verifier evaluates a catalog record's invariants, so an
  older reader still reads it; the receipt it names is what an older verifier
  refuses.
  **1.5.0 (PROD-03.0)** is the fifth: `topics[].schema_dependency`, copied from
  a receipt that carries the block — every receipt PROD-03.0's builds sign, so
  every record they write is 1.5.0. A record backfilled from an older receipt keeps the
  format it would have had, and its topics' schema dependency is not assessed.
  The catalog's view lists each topic's verdict, the dependent sides and their
  schema ids (`PointView.topics[].schemaDependency` in the product API) for an
  `Available` point only.
  **1.6.0 (PROD-01.4a)** is the sixth: `topics[].identity`, the receipt's topic
  IDs before and after the engine, from a receipt that carries `generations` —
  every receipt this build signs, so every record it writes is 1.6.0. A record
  backfilled from an older receipt keeps the format it would have had, and its
  topics' generations read unknown. The view and the product API do not show
  the IDs yet (PROD-02.1's lineage is their first consumer).
* **A major bump** is for a change a `1.x` reader could misread — a field whose
  meaning changed, or a required field removed. It writes under a new key path.
* **Absent optional fields are unknown**, in every version.
* **Nothing is rewritten or deleted by catalog code**, in any version.
* **Upgrade**: an installation that has never run this build has no
  `logweir/catalog/` prefix at all. The first `logweir backup run` after the
  upgrade starts writing records; `logweir catalog sync` backfills the rest.
  Nothing existing changes — the receipt format, its keys and the two stdout
  keys `backup run` prints are untouched.
* **Rollback**: an older `logweir` ignores `logweir/catalog/` entirely and keeps
  writing receipts as before. The records already written stay valid and stay
  verifiable; they simply stop being added to.

## The operator commands

```
logweir catalog sync --url s3://<bucket> \
  --signing-key signing.pem --public-key evidence-public.pem \
  [--since <object key>] [--max <n>] \
  [--region <r>] [--endpoint <url>] [--path-style] [--allow-http]

logweir catalog list --url s3://<bucket> \
  [--since <object key>] [--max <n>] [--days <n>] …
```

`--url` names the archive's bucket; the evidence root `logweir/` is imposed and
is not read off the URL, so a deeper key prefix is refused rather than quietly
used. A URL carrying userinfo (`s3://key:secret@bucket`) is refused without
echoing it — a credential on an argv is visible in every process listing on the
host. `--allow-http` is explicit and is never derived from the endpoint's
scheme or from any environment value.

`sync` walks the receipt prefix in bounded, resumable pages and prints a
bounded summary — one `catalog-point=<id> state=<state> receipt=<key>` line per
receipt examined, then `catalog-scanned=`, `catalog-written=`,
`catalog-already-present=`, `catalog-conflict=`,
`catalog-unsupported-format=`, `catalog-unverified-signer=`,
`catalog-unreadable=`, and `catalog-next=<cursor>` when there is more to walk.
Re-running it is idempotent: identity is content-derived, so a second run over
the same archive produces the same ids and reports them as already present.

`list` reads only, and walks **day shards backwards from today**, stopping the
moment `--max` rows are held — which is what the day shard is for. `--days`
(default 400, decision D3 §5.3's own histogram bound) is how far back it will
look. Every run prints `catalog-listed=`, then
`catalog-unsupported-format=`, `catalog-unreadable=` and
`catalog-inconsistent=` — the entries it **skipped**, because "refusal is per
entry, not per catalog" is worth nothing if a short page cannot be told from a
complete one — then `catalog-searched-days=` and
`catalog-oldest-day-searched=`, so an empty page says which window it is empty
for rather than implying the catalog is empty. `catalog-truncated=true` appears
when `--max` filled with days still unlooked-at.

There is deliberately **no resume cursor for older points**: a windowed query
over history is an advertised-absent capability (D3 §5.3), and a
`catalog-next=` that nothing consumes would be the fake stub that section
forbids. Raise `--max`, or narrow with `--since`.

An index entry that this build cannot read — a higher major, a truncated
object, or one whose `record_key` is not the key its own `point_id` implies —
is **skipped and counted**, never fatal to the listing. That last check matters
because the log prefix is create-only but not append-restricted: anyone who can
write a *new* key there could otherwise publish a row attributing an arbitrary
record to a chosen identity. Its rows come from the unsigned index, and `list`
says so on every run.

Exit codes are the existing contract with no new variant: `0` completed, `1`
operational, `4` signing failed. Neither command produces `2` or `3` — they run
no plan and sign no verdict.

**`4` means what the contract says it means**: signing or lock-proof failed
*and nothing was uploaded*. It is produced only when the failure happened
before any put was attempted — a document that contradicts itself, or a signing
failure. A store that refuses or cannot complete a put is **`1`**, with a
message that says so in as many words: the record was signed before any put was
attempted, part of the point may already be stored, nothing under `logweir/` is
ever rewritten, and `--since` resumes the walk. Reporting a denied `PUT` as `4`
would send an operator to rotate signing material over a bucket policy, and
would assert "nothing was uploaded" in exactly the case where something was.

## Credentials

`logweir catalog sync` and `logweir catalog list` are **operator** commands:
they run with the operator's own object-store credentials from the operator's
own environment, create no Kubernetes object and no Job, and write nothing
outside `logweir/catalog/v1/`. A controller-driven catalog sync is a different
thing with a similar name — it runs as a Job with a destination-backed,
read-only credential and carries its complete, explicit `AWS_*` set, because a
Job must never inherit addressing or transport settings from a controller's
environment.

---

Documentation is licensed [CC-BY-4.0](../LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
