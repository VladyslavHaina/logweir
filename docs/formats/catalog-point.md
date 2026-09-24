# The recovery catalog point record, field by field

`application/vnd.logweir.catalog-point+json;version=1.0.0`

The machine-readable schema is
[`schemas/logweir-catalog-point-1.0.0.json`](../../schemas/logweir-catalog-point-1.0.0.json),
regenerated from the Rust type by `just schema` and `diff -u`'d against the
checked-in file by `just schema-check`, so this document and the schema cannot
drift apart silently.

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
   receipt-derived fact are a `Conflict` (below).
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
   before its engine starts, so a new execution signs one receipt — but sets
   written by older builds can still hold two receipts, and this rule is what
   keeps both of them visible.

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
| `format_version` | string | Semver of THIS format, independent of the receipt's and the scorecard's. Major `1`. |
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
| `topics[].partitions` | int, **optional** | ABSENT on every record this build writes: a backup receipt records no partition count at all. See [absent means unknown](#absent-means-unknown-never-zero). |
| `source.cluster_id` | string | Read from the broker at admission and carried by the receipt — never from a spec. |
| `source.bootstrap_servers` | string[] | Addressing. |
| `source.auth_mode` | string | `plaintext` or `scramSha512` — the receipt's closed two-value set. |

**Nothing in this document may hold a credential**, and there is deliberately no
`username` even though the receipt has one: a catalog is the surface an operator
lists in bulk, and a SASL principal is not a fact a recovery point needs.

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
   fields and a 1.0.0 reader must still read a 1.1.0 record.
3. **Absent optional fields mean UNKNOWN — never zero.** See below.
4. **Everything except the receipt-derived facts is informational.** The
   receipt's signature is the verification root. `backup_id`, `run_id`,
   `covered`, `capture` and `archive.manifest_key`/`manifest_sha256` are
   recomputed from the verified receipt, and a record whose copies disagree is
   reported as a mismatch (availability `Conflict`) rather than believed.
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

Absent fields are absent from the bytes, not `null`, so a reader in any language
sees nothing rather than a value.

## What a signature on this document proves

**It proves the bytes were signed by the holder of a key. It is not a claim
that the point is available, that its archive is readable, or that its copied
facts are true.**

Availability and verification are separate axes and both are separate from this
signature:

* *availability* — can the receipt, sidecar and manifest still be fetched, and
  does the manifest's digest still equal the receipt's? Only a fetch answers
  that, and this document is not one.
* *verification* — does the backup receipt's DSSE signature verify under a key
  you trust for evidence signing? That is the receipt's signature, not this one.

Both readers say so in as many words and both report SIGNATURE-ONLY for this
type:

```
logweir drill verify --payload-type catalog-point \
  --scorecard record.json --signature record.sig --public-key public.pem

python3 docs/verify_scorecard.py --payload-type catalog-point \
  record.json record.sig public.pem
```

`scripts/check-verifier-parity.sh` walks three catalog-point documents — a good
one, one with a byte flipped after signing, and a genuine one presented as a
scorecard — and fails if the two readers disagree about the verdict or if
either stops printing its signature-only sentence.

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
  ignored, so a 1.0.0 reader reads a 1.1.0 record.
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
