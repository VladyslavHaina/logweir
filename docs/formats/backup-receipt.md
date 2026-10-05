# The backup receipt format, field by field

`application/vnd.logweir.backup-receipt+json;version=1.0.0`

The machine-readable schema is
[`schemas/logweir-backup-receipt-1.2.0.json`](../../schemas/logweir-backup-receipt-1.2.0.json)
and CI regenerates it from the Rust type and `diff -u`s it against the checked-in
file on every build, so this document and the schema cannot drift apart
silently. A MINOR bump is a new schema file beside the old one: the
[`1.1.0` schema](../../schemas/logweir-backup-receipt-1.1.0.json), which
describes every receipt written without the manifest-version pin (FX-4's
format), and the [`1.0.0` schema](../../schemas/logweir-backup-receipt-1.0.0.json),
which describes every receipt written before format 1.1.0, are FROZEN beside it
and never regenerated. The payload type keeps `version=1.0.0`: it names the
major-1 envelope, and a new value would make every existing reader refuse every
new receipt at the payload-type comparison. A signed worked example is
[`e2e/fixtures/signed/backup-receipt.json`](../../e2e/fixtures/signed/backup-receipt.json)
with its detached sidecar
[`backup-receipt.sig`](../../e2e/fixtures/signed/backup-receipt.sig) — read
[`e2e/fixtures/signed/README.md`](../../e2e/fixtures/signed/README.md) first,
which says what the throwaway key in that directory is and is not for.

If you are verifying a document rather than producing one, read
[../verify-a-scorecard.md](../verify-a-scorecard.md) for the mechanics of a
detached DSSE sidecar; everything it says about verifying-as-read applies here
unchanged, and this document is a field reference.

## Why this is its own document and not eight new scorecard fields

The drill scorecard is **frozen** at `format_version 1.0.0` with 21 top-level
properties and 17 required ones. A backup happens on a different cluster, at a
different time, under a different command, and the facts it establishes — the
source cluster id read from the broker, the rendered auth mode, the named topic
set, the pinned engine's digest, the manifest key and its sha256, the per-topic
record counts, the covered window — are top-level facts about that operation.
Bolting them onto the scorecard would have cost eight new top-level properties
in the one document that exists to stay still, and would have left every
scorecard ever written claiming, by the shape of its own schema, to say
something about a backup it never observed.

So the receipt has its own media type, its own schema, its own
`format_version` — **independent of the scorecard's**: `1.1.0` since FX-4, or
`1.2.0` for a receipt that pins its manifest's version
([below](#the-pinned-manifest-version-versioned-buckets)) — and its own arms:
five in 1.0.0, and six more that read only 1.1.0's
[`config_coverage`](#config_coverage--topic-configuration-capture-coverage-format-110).
The pin adds no arm.

## Reading rules a consumer must honour

1. **`format_version` is semver, and its major is `1`.** Ignore unknown fields
   when the major matches what you support. **Refuse** a document whose major is
   higher rather than guessing at a shape you have never seen. The schema pins
   the major with a pattern (`^1\.[0-9]+\.[0-9]+$`) as well, so a schema-only
   validator refuses a `9.9.9` document too.
2. **Verify against the bytes as stored.** The signature covers the exact bytes,
   including the trailing newline. Never `jq .` a receipt, re-save it and then
   verify.
3. **`exit_code` is the ENGINE's exit status**, not `logweir`'s. The `logweir`
   process maps its own outcome through the exit-code contract in
   [../../README.md](../../README.md); this field is what the pinned engine
   subprocess returned.
4. **`covered` is in epoch milliseconds**, not RFC 3339. See
   [the covered window](#the-covered-window-and-why-it-is-not-rfc-3339) below.
5. **An ABSENT `config_coverage` is UNKNOWN coverage, never `captured`.** Every
   receipt written before format 1.1.0 lacks it; so may a 1.1.0 receipt. Read
   it as "nobody recorded whether this topic's configuration was captured",
   and never as "it was".

---

## Identity and provenance

| Field | Type | Meaning |
|---|---|---|
| `format_version` | string | Semver of **this** format. `1.1.0` since FX-4 (`1.0.0` before it), or `1.2.0` for a receipt that pins [`archive.manifest_version_id`](#the-pinned-manifest-version-versioned-buckets). Independent of the scorecard's. |
| `run_id` | string | ULID of the run that produced this receipt. Also the object key stem in the evidence bucket. |
| `backup_id` | string | The engine's identifier for the archive this run wrote — the **execution** id under Kubernetes. **Not** `run_id`: `archive.manifest_key` is keyed on this, and a set written by an older build can carry receipts from two runs. Since RECEIPT-DUP was fixed, at most one run per `backup_id` reaches the engine (see [the execution claim](#the-execution-claim-one-engine-run-per-backup_id)), so a new execution signs exactly one receipt. |
| `requested_at` | RFC 3339 | When the run was requested. |
| `started_at` | RFC 3339 | When the engine subprocess started. Logweir-measured. |
| `finished_at` | RFC 3339 | When the engine subprocess finished. Logweir-measured. |
| `exit_code` | integer | The engine's exit status. `0` **if and only if** `archive.manifest_key` names a manifest — invariant 2. |
| `triggered_by` | string | Free text from `--triggered-by`. Deliberately **not** a metric label: unbounded cardinality. |

`started_at`, `finished_at` and `exit_code` are Logweir-measured and never
engine-reported: the engine's `backup` subcommand has no `--format` and writes
no report file, so its start, finish and exit code are ours to time.

## `source` — the cluster the data came from

| Field | Type | Meaning |
|---|---|---|
| `source.cluster_id` | string | **Read from the broker**, never from the spec. The fourth rail of the backup guard records this value and re-asserts it is not the restore target; this is where the recorded value is attested. |
| `source.bootstrap_servers` | array of string | The bootstrap list the source client was given. |
| `source.auth.mode` | string | **A closed set of two: `plaintext` or `scramSha512`.** These are `AuthSpec`'s serde tag values, the `KafkaCluster` CRD's `auth.mode` enum byte for byte, and the only two strings `AuthSpec::mode_str()` returns — so the spec an adopter writes, the CRD they apply and this signed document all spell the mechanism the same way. **Any other value is refused by both readers** (arm 5). |
| `source.auth.username` | string \| null | The SASL username, when there is one. `null` under `plaintext` — which is not the same as an empty username. |
| `source.topics` | array of string | The named topic allowlist. A **named set with no glob metacharacter**, so this is the exact set of topics and not a pattern a reader would have to re-expand against a cluster it cannot see. |

**`auth` never carries a password, and has no field that could hold one.** The
secret reaches the engine through its own `${VAR}` environment expansion and is
never interpolated by Logweir — which is exactly what stops it being
interpolated into a document Logweir then signs and publishes.

## `engine` — what took the backup

| Field | Type | Meaning |
|---|---|---|
| `engine.id` | string | `oso-cli`. |
| `engine.version` | string | e.g. `v0.21.0`, at or above the supported floor. |
| `engine.digest` | string | `sha256:…`, from `third_party/kafka-backup-binary.digest`. |

`digest` is why this block is worth signing. Pinning is by digest and never by
tag; a receipt that named only a version would be satisfied by any binary
claiming that version.

## `archive` — what was written, and where

| Field | Type | Meaning |
|---|---|---|
| `archive.manifest_key` | string | The manifest's object key. Empty **if and only if** the backup did not exit 0 — invariant 2. |
| `archive.manifest_sha256` | string | `sha256:<hex>` over the manifest bytes **this run read back** — not over bytes Logweir remembers writing. |
| `archive.manifest_version_id` | string, **optional** (format `1.2.0`) | The object store's version id for those exact bytes — present only on a bucket with versioning enabled, where the read-back was answered with one. **Absent** means no version was pinned: an unversioned bucket, S3's `null` version, or a receipt from before the field. See [the pinned manifest version](#the-pinned-manifest-version-versioned-buckets). |
| `archive.prefix` | string | The object-store prefix everything this run wrote lives under. Logweir writes only under its own `logweir/` prefix. |

## `records` — per-topic counts

An object whose keys are topic names and whose values are unsigned integers.
It has **exactly one entry per `source.topics` entry and no others** — invariant
3. Serialised from a sorted map, so two runs over the same topic set produce
byte-identical bytes here.

## The covered window, and why it is not RFC 3339

| Field | Type | Meaning |
|---|---|---|
| `covered.from_ms` | integer (int64) | **Inclusive** start of the covered range, **epoch milliseconds**. |
| `covered.to_ms` | integer (int64) | **EXCLUSIVE** end of the covered range, **epoch milliseconds**. Strictly `> from_ms` — invariant 4. |

The window is **half-open**: `[from_ms, to_ms)`. That is the convention
`config/crd/backups.yaml` already documents for
`Backup.status.windowCovered.toMs`, which Task 17 fills by copying these two
integers, so the two documents describe one range under one rule. Task 5 shipped
invariant 4 as `<=` and a test asserting that `from_ms == to_ms` was a legal
"instantaneous window"; that was the disagreement, and the receipt is the side
that moved (Task 5's review, F3).

A backup whose records all share one millisecond is still a window, and still
legal: `crates/logweir/src/backup/phase_run.rs` derives `to_ms` from the newest
segment's **inclusive** `end_timestamp` and adds one millisecond, so such a run
publishes `[t, t+1)` — a window containing exactly those records — rather than
the empty `[t, t]` invariant 4 now refuses. That conversion happens once, where
the window is measured, and nowhere else.

This is the shape the operator's `Backup.status.windowCovered{fromMs,toMs}`
mirrors, as two `int64`s. A Kubernetes status subresource has no date-time type
to mirror a string into, so two representations of one window — a string here
and an integer there — would need a conversion nobody owns, and the first
disagreement between them would be invisible because both would still be
well-formed. The receipt speaks the operator's units and the operator copies the
numbers.

## `config_coverage` — topic-configuration capture coverage (format 1.1.0)

**Why it exists (FX-4).** The engine captures each topic's explicit
configuration overrides into the manifest's `configurations`, but it does so
NON-FATALLY — `logweir backup run` renders no `require_topic_configs`, so the
engine's default `false` applies (`config.rs:524-533`, `:639-640` in the pinned
source) and a failed capture is one warning — and ALL-OR-NOTHING: its
DescribeConfigs fails the whole call on the first per-resource error
(`kafka/admin.rs:476-487`), so ONE topic the principal may not DescribeConfigs
empties every topic's record. The manifest spells "captured, no overrides" and
"not captured" identically, as `configurations: {}`, and a restore's
configuration parity used to compare against that empty record and report no
divergence.

An object keyed by topic name, **one entry per `source.topics` entry and no
others** (arm 7):

```json
"config_coverage": {
  "orders":   { "coverage": "captured",
                "timestamp_type": { "value": "LogAppendTime", "source": "dynamicDefaultBrokerConfig" } },
  "payments": { "coverage": "captureDenied" },
  "ledger":   { "coverage": "notCaptured", "reason": "manifestDiffers",
                "timestamp_type": { "value": "CreateTime", "source": "defaultConfig" } }
}
```

| Field | Type | Meaning |
|---|---|---|
| `coverage` | string | `captured`, `notCaptured` or `captureDenied` — a closed set (arm 8). |
| `reason` | string, **present exactly when `coverage` is `notCaptured`** | `describeFailed` or `manifestDiffers` (arm 9). |
| `timestamp_type.value` | string, optional | The topic's EFFECTIVE `message.timestamp.type`: `CreateTime` or `LogAppendTime` (arm 11). |
| `timestamp_type.source` | string | Where that value came from — Kafka's `ConfigSource`, camel-cased: `dynamicTopicConfig` (a TOPIC OVERRIDE), `dynamicBrokerConfig`, `dynamicDefaultBrokerConfig`, `staticBrokerConfig`, `defaultConfig` (the broker's), or `unknown` (arm 11). |

**Where the answer comes from.** Not from the engine, whose capture outcome is a
log line naming at most one failing resource and whose record keeps explicit
overrides only. `logweir backup run` reads every named topic's configuration
ITSELF, in one DescribeConfigs request, through the same principal the engine
uses, **immediately before the engine starts**; after the engine it compares
what the engine WOULD have kept — its own filter, vendored in
`crates/logweir-engine-oso/src/vendored/topic_config.rs` (topic-override source,
not read-only, not sensitive, on its 24-key allowlist) — with what the manifest
DOES hold:

| `coverage` | `reason` | what it means |
|---|---|---|
| `captured` | — | The read succeeded and the manifest's `configurations` for the topic equal the overrides the engine captures from what that read saw. The archive's record is complete, so a configuration parity check may compare against it. It is a claim about the RECORD, not about the engine's call: a topic with no such overrides reads `captured` even in a run whose engine capture failed as a whole, because its empty record is accurate. |
| `captureDenied` | — | The broker's authorizer refused the read. The engine runs as the same principal, so an empty record says nothing. |
| `notCaptured` | `describeFailed` | The read failed for any other reason: no broker answered, the topic is unknown, or the reader cannot answer. |
| `notCaptured` | `manifestDiffers` | The read succeeded but the manifest does not record the same overrides: the engine's own capture failed (one denied topic empties them all), or the configuration changed between the two reads. |

**How a refusal is recognised, and its limit.** rust-rdkafka 0.36.2 never reads
librdkafka's per-resource DescribeConfigs error (`src/admin.rs:1121-1159`;
PROD-04.0 T13): a refused topic comes back as a SUCCESS with ZERO entries.
Kafka never answers a successful describe that way — an authorised, existing
topic gets every `LogConfig` entry, and an empty list comes only beside a
per-resource error (`ConfigHelper.scala:54-79`, `:88-98`, `:144-158` at 4.3.1).
So an empty answer is a failed read, and its cause is read from the same
principal's metadata for the topic: visible, or `TOPIC_AUTHORIZATION_FAILED` →
`captureDenied`; `UNKNOWN_TOPIC_OR_PARTITION` → `describeFailed`. For a visible
topic this is an inference — the only per-resource errors Kafka returns for an
existing, validly named topic are the authorizer's and an internal broker
error — and it becomes an observation when the per-resource code is readable
(owner choice AP-OC1). Either way it is never `captured`.

**The timestamp type** is Logweir's own observation and is recorded wherever
its read succeeded, including `manifestDiffers`; it is ABSENT — "not recorded",
never assumed `CreateTime` — where the read was denied or failed (arm 10) or
the broker reported a value outside the two. It is how a restore can tell a
`LogAppendTime` source whose type is a BROKER DEFAULT (FX-8): the manifest
carries topic overrides only.

**What `captured` does not claim.** That every setting of the topic was
archived: overrides outside the engine's allowlist (`local.retention.ms`, a
provider-specific key) are never part of the claim. Which settings are portable
and how they are applied is PROD-05.1 and 05.2.

---

## The eleven arms

`logweir_core::backup_receipt::BackupReceipt::validate_invariants` implements
these, and `docs/verify_scorecard.py::check_backup_receipt_invariants` mirrors
them ARM FOR ARM, IN ORDER. The messages below are the **exact** refusal text of
BOTH readers — compared byte-for-byte by
`crates/logweir-core/tests/backup_receipt.rs` (`backup_receipt_refuses_each_self_contradiction_arm_with_its_exact_message`
over arms 1–4, `arm_5_refuses_an_auth_mode_outside_the_closed_two` over arm 5,
one `arm_N_…` test per arm 6–11, and
`validate_invariants_has_exactly_eleven_return_err_statements` over the total),
by `crates/logweir/tests/two_reader_parity_receipt.rs::two_reader_parity_over_the_backup_receipt_corpus`
over the eighteen documents in `e2e/fixtures/invariants/backup-receipt-index.json`,
and by `scripts/check-verifier-parity.sh`'s second loop — and they are not to be
reworded. `scripts/check-invariant-corpus.sh` additionally derives the arm list
from both readers' source and refuses to balance if they are not the same eleven
arms in the same order.

Arms 6–11 read `config_coverage` and NOTHING ELSE, and run only when it is
present — so every receipt without it, which is every receipt written before
1.1.0, is accepted or refused exactly as before. Within the block, topics are
visited in name order and arms 8–11 run per topic, in order; the 1.0.0 arms
always run first.

1. **`format_version` parses as semver and its major is `1`.** Checked first, so
   a document from a future major is refused before any other arm is evaluated
   against fields that build may have redefined. "Parses as semver" is strict:
   exactly three dot-separated non-negative integers, so `1`, `1.0`, `1.0.0.0`
   and `1.0.0-rc1` are all refused.

   > `format_version "2.0.0" is not a 1.x version this reader understands`

2. **`exit_code == 0` if and only if `archive.manifest_key` is non-empty.** A
   receipt for a failed backup names no manifest, and a receipt naming a
   manifest did not fail. A whitespace-only `manifest_key` counts as **absent**,
   not as a manifest made of spaces.

   > `exit_code 1 and manifest_key "logweir/…/manifest.json" disagree: a receipt names a manifest if and only if the backup exited 0`

   > `exit_code 0 and manifest_key absent disagree: a receipt names a manifest if and only if the backup exited 0`

3. **`records` covers exactly `source.topics`.** A receipt that counts a topic
   the run was never asked to back up, or omits one it was, is describing some
   other run — and either way the per-topic figures cannot be read against the
   topic list beside them. Both sides are rendered sorted, so the message is
   deterministic.

   > `records covers {"orders"} but the named topic set is {"orders", "payments"}`

4. **`covered.from_ms < covered.to_ms`, strictly.** The end is EXCLUSIVE, so a
   window that ends before it begins is meaningless and one that ends where it
   begins is empty — and an archive that captured a record cannot cover an
   empty range.

   > `covered.from_ms 2 is not before covered.to_ms 1: the covered window's end is EXCLUSIVE, so an empty range covers no record`

5. **`source.auth.mode` is `plaintext` or `scramSha512`, and nothing else.** The
   only arm that is not a claim the document makes against itself: the receipt
   does not contradict itself, it names a mechanism this format has no spelling
   for. It is last for that reason — a document that contradicts itself should
   be told so first. The two values are `AuthSpec`'s serde tags, so `logweir`
   itself has exactly one writer of this field and no way to reach a third
   value; the arm is here for the documents this tree did not write, and
   because `Backup.status.auth.mode` — which the operator copies FROM this
   field — promises the same two in its own CRD description.

   > `source.auth.mode "scram-sha-512" is not one of the two values this format defines: "plaintext" or "scramSha512"`

6. **`config_coverage` is present only under a minor of at least 1.** A document
   that declares 1.0.x cannot carry a 1.1 field.

   > `config_coverage is present but format_version "1.0.0" predates it: the field is defined from 1.1.0`

7. **`config_coverage` covers exactly `source.topics`** — arm 3's twin.

   > `config_coverage covers {"orders"} but the named topic set is {"orders", "payments"}`

8. **Every `coverage` is `captured`, `notCaptured` or `captureDenied`.**

   > `config_coverage["orders"].coverage "unknown" is not one of the three values this format defines: "captured", "notCaptured" or "captureDenied"`

9. **`reason` is present exactly when `coverage` is `notCaptured`, and is
   `describeFailed` or `manifestDiffers`.** An absent reason is spelled `absent`.

   > `config_coverage["orders"].reason absent does not fit coverage "notCaptured": a reason is present exactly when coverage is "notCaptured", and is "describeFailed" or "manifestDiffers"`

10. **A `timestamp_type` exists only where the read succeeded.** A
    `captureDenied` topic, or a `notCaptured` one whose reason is
    `describeFailed`, cannot have observed one.

    > `config_coverage["payments"] records a timestamp_type, but a topic whose configuration read was denied or failed cannot have observed one`

11. **A `timestamp_type`'s value and source are from closed sets.**

    > `config_coverage["orders"].timestamp_type "LogAppendTime" from "DYNAMIC_DEFAULT_BROKER_CONFIG" is not a value and source this format defines: the value is "CreateTime" or "LogAppendTime", and the source is "dynamicTopicConfig", "dynamicBrokerConfig", "dynamicDefaultBrokerConfig", "staticBrokerConfig", "defaultConfig" or "unknown"`

A block that serde itself cannot read — a `timestamp_type` without its `source`,
a `coverage` that is not a string — is refused before any arm by both readers
(`drill verify` exits 1 with serde's message; `verify_scorecard.py` exits 1 with
its own shape message).

---

## Verifying a receipt

Two readers, one contract. Both check the detached DSSE sidecar against the
bytes as stored, and both take the document type by name:

```
logweir drill verify --payload-type backup-receipt \
  --scorecard e2e/fixtures/signed/backup-receipt.json \
  --signature e2e/fixtures/signed/backup-receipt.sig \
  --public-key e2e/fixtures/signed/public.pem
```

```
python3 docs/verify_scorecard.py --payload-type backup-receipt \
  e2e/fixtures/signed/backup-receipt.json \
  e2e/fixtures/signed/backup-receipt.sig \
  e2e/fixtures/signed/public.pem
```

`--payload-type` defaults to `scorecard` in both readers, so every invocation
that predates the receipt is unchanged. The accepted names are `scorecard`,
`backup-receipt`, `receipt` (the post-put storage readback of a scorecard) and
`teardown`; anything else is an **error** rather than a passthrough, because a
typo'd media type would otherwise surface as "unexpected payloadType" and read
like a bad artifact instead of a bad command line.

The `--scorecard` flag keeps its name even when it names a receipt. Renaming it
would break every existing invocation, every document and
`scripts/check-verifier-parity.sh` in exchange for a better word.

> **What each reader checks today, stated plainly rather than implied.** Both
> readers run **all eleven arms above** over a `--payload-type
> backup-receipt` document; arms 1–5 arrived together in Task 5b and arms 6–11
> together in FX-4, so the two readers never disagreed in between.
> `logweir drill verify` prints `checked:   the signature AND all eleven
> backup-receipt invariants …`; `docs/verify_scorecard.py` prints `verifier:
> verify_scorecard.py 1.16.0 (backup-receipt invariant set: …)`. Both also print
> the configuration capture coverage in the same words, one
> `config_coverage["<topic>"]: <coverage>[ (<reason>)], message.timestamp.type
> <value> from <source>` line per topic — or `config_coverage: not recorded, so
> every topic's configuration capture is UNKNOWN, never captured`, for every
> receipt without the block — and `scripts/check-verifier-parity.sh` compares those lines
> between the two readers on every accepted receipt. Both print a pinned
> `archive.manifest_version_id` when the receipt carries one (`manifest version:`
> and `manifest_version_id=`), and both refuse one that is not a string — Rust
> at deserialisation, the script in its shape layer (FX-7; verdict parity in
> `scripts/check-verifier-parity.sh`). Both compare the sidecar's `payloadType`
> **in full**, so a genuinely-signed scorecard presented as a receipt is refused
> as a substitution rather than accepted — `drill verify` exits 4 and says
> `PAYLOAD TYPE MISMATCH`, which is deliberately not `SIGNATURE INVALID`: the
> signature may be perfectly valid over some other document.
>
> An exit 0 from either reader therefore means "these bytes are signed by this
> key under this media type **and** the document does not contradict itself".
> The weaker sentence — `checked:   the SIGNATURE only …` — is still printed for
> `--payload-type receipt` and `--payload-type teardown`, whose invariant
> readers are not in tag 1, and
> `crates/logweir/tests/cli_verify.rs::the_signature_only_verdict_is_still_reachable`
> keeps it honest.
>
> **Where else the arms are enforced — all eleven since 1.1.0.** At the two places a receipt is
> WRITTEN: `crates/logweir-evidence/examples/mint_backup_receipt_fixture.rs`
> validates before it signs, and `logweir backup run` validates the exact
> document it is about to sign before it signs or uploads anything
> (`crates/logweir/src/backup/phase_run.rs::persist_receipt`, step 1) — a
> violating receipt is exit **4** with nothing in the bucket, asserted by
> `crates/logweir/tests/backup_run.rs::a_receipt_that_cannot_be_signed_is_exit_4_and_puts_nothing`.
> Before Task 5b this paragraph named `logweir backup run` while that command
> wrote no receipt at all; it is now true by execution.

## Where `logweir backup run` puts it

Two objects, both create-only, both under Global Constraint 6's `logweir/`
root:

```
logweir/backups/<backup_id>/<run_id>.receipt.json
logweir/backups/<backup_id>/<run_id>.receipt.sig
```

The prefix is an **assertion**, not a convention: `Store::put_create_only`
refuses any key outside `logweir/`, so a build that tried to write the receipt
elsewhere aborts rather than writing it. The evidence handle is derived from the
archive's own object-store location with the prefix replaced by `logweir/` —
`Backup.spec` carries one URL, the archive root, and `Store::from_url` refuses
any evidence prefix that is not exactly `logweir/`.

The **final two stdout lines** of a successful `logweir backup run` are, in this
order and with nothing after them:

```
receipt-key=logweir/backups/<backup_id>/<run_id>.receipt.json
sidecar-key=logweir/backups/<backup_id>/<run_id>.receipt.sig
```

That is a machine contract (interface **I7**): the Kubernetes pod log API has no
stream selector, so a controller reading a Job's output cannot separate stdout
from stderr and reads the last lines instead.

### The execution claim: one engine run per `backup_id`

A third object sits beside the receipts, and it is the only one there whose key
does not carry a run id:

```
logweir/backups/<backup_id>/execution.claim.json
```

**Why it exists (tracker defect RECEIPT-DUP).** The engine writes
`<prefix>/<backup_id>/manifest.json` with its own, unconditional store client. A
second engine run under the same `backup_id` — a Kubernetes Backup Job lost and
re-created from its frozen inputs, or a second `logweir backup run` with the
same spec — replaced the manifest the first run's receipt attests. When the
topic had advanced in between, the first receipt's `archive.manifest_sha256` no
longer matched the manifest in the bucket, and every verifier that reads the
archive back (a point-bound drill, the `catalogSync` deep check, an auditor with
`sha256sum`) reported the first receipt as describing an archive that is no
longer there. Logweir cannot make the engine's write conditional, so it makes
sure the second engine run never starts.

**It is worse than a changed digest (FX-7, measured on engine 0.21.0).** The
engine keys each segment by its start offset —
`<backup_id>/topics/<topic>/partition=<n>/segment-<start offset>.bin…` — so a
second run over the same set REWRITES the first run's segment objects in place,
and its get-merge-put keeps the first run's manifest entry for every key it
already had ("existing wins"). When the new records fall inside an existing
segment, the manifest bytes come out IDENTICAL while the segment under them now
holds different records: measured on compose slot 3 (MinIO unversioned and
SeaweedFS versioned), 100 records backed up, 50 produced, a second run over the
set — the first receipt's manifest digest still matched, and the segment's
recorded `sha256` no longer matched the object (150 records under an entry for
100). No manifest check can see that afterwards; only a run that never starts
keeps the first point true.

**What the runner does.** After every local and read-only check and
immediately before the engine starts, `logweir backup run` puts the claim with a
conditional create (`If-None-Match: *`) and then puts it a second time: the
second put must be refused as `AlreadyExists`. Only then does the engine start.

| What the store answers | Exit | The message names | What happened |
|---|---|---|---|
| first create succeeds, second is refused as already existing | — | — | the run holds the claim; the engine starts |
| the first create is refused because the claim **already exists** | **1** | `ExecutionAlreadyClaimed` | an earlier run of this `backup_id` reached the engine. **No engine run, no receipt.** Retry under a **new** `backup_id`: a new `Backup`, or — exit 1 being retryable — a schedule's next attempt `-r<k>` **when the schedule has `spec.retry`**; without it the slot is `RunFailed` |
| the first create is refused for any other reason (a missing `s3:PutObject` on `logweir/*`, a transport error) | **4** | `ExecutionClaimUnproven` | lock-proof failed, nothing uploaded — the engine never started |
| the backend reports conditional put unsupported (the store falls back to HEAD-then-PUT) | **4** | `ExecutionClaimUnproven` | a HEAD-then-PUT is not exclusive, so the claim is no lock |
| the **second** create succeeds | **4** | `ExecutionClaimUnproven` | the store accepts `If-None-Match: *` and overwrites anyway; a claim on it is no lock |
| the claim is won, but the archive already holds `<prefix>/<backup_id>/manifest.json` or a segment under `<prefix>/<backup_id>/topics/` (FX-7) | **1** | `ExecutionAlreadyClaimed` | an earlier run of this `backup_id` — by a build **without** the claim — wrote this set (or wrote a segment of it and died, or is still running). **No engine run, no receipt.** The same state and remedy as a claim that exists: a new `backup_id` |
| the claim is won, but a read of the archive to prove the set is new failed TRANSIENTLY — a transport error, a timeout, or a 5xx/429 the client had already retried for three minutes (FX-7 fix round) | **1** | — (`operational`) | nothing is proven about the set, so the engine never started. Retryable: a schedule with `spec.retry` starts a NEW execution `-r<k>`, which is a different set; a manual retry needs a new `backup_id` too, because this run's claim is taken |
| the claim is won, but that read failed for any other reason — a 401/403, a wrong bucket, region or CA, or an error this build cannot classify | **4** | `ExecutionClaimUnproven` | nothing is proven about the set and no retry changes it, so the engine never started; grant `s3:ListBucket` and `s3:GetObject` on the archive prefix (the read-back needs both too), then run again under a new `backup_id` |

Both refusals end with a final stdout line `failure-reason=ExecutionAlreadyClaimed` (exit 1) or
`failure-reason=ExecutionClaimUnproven` (exit 4), the exit-1/4 twin of exit 3's `refusal-reason=`.
A Kubernetes controller lifts it into `Backup.status.exitReason` and the terminal condition's
message, and only beside the exit code it belongs to.

**Transient failures are safe, and say so imperfectly.** The object-store client retries a 5xx:
if the server committed the claim before answering 5xx, the retried create is refused and the
run's own claim is reported `ExecutionAlreadyClaimed` (exit 1). A transport failure on either
create is exit 4. In both cases no engine ran and nothing was signed; an operator who can read the
evidence root can tell the first case apart by comparing the claim's `run_id` with the run's own.
`claimed_at` is the instant the run was requested, not the instant of the put.

A claim also stays behind for every execution that reached it and then failed — a few hundred
bytes each, on the same never-deleted lifecycle as receipts under `logweir/`.

The claim is **unsigned and never read by the runner**: it is a lock, not
evidence, and its existence is learned from the conditional put's own answer.
So it needs no permission the runner did not already hold — `s3:PutObject` on
`logweir/*` (`evidenceWrite`) — and adds no `s3:GetObject` or `s3:ListBucket`
under `logweir/`. Its body names the run that holds it, for an operator who can
read the evidence root:

```json
{"backup_id":"<backup_id>","claimed_at":"<RFC 3339>","format_version":"1.0.0","run_id":"<run_id>"}
```

It is never deleted by Logweir: the retention worker refuses every key under
`logweir/`, and a deleted claim would let a later run of the same execution
overwrite an attested manifest again.

**The set must be new, too (FX-7).** A set whose first run was made by a
build without the claim carries none, so the claim alone let a later run of that
`backup_id` start — at upgrade (a Backup Job lost while the controller was
upgraded, re-created with the new runner image) and for a standalone
`backup run` re-using a `backup_id` an older build wrote to
(RECEIPT-DUP-UPGRADE-WINDOW). So after winning the claim, as the last refusal
before the engine (only the topic-configuration read above, which writes
nothing and is never fatal, follows it), the runner reads the set through its read-only archive
handle — a one-key LIST of `<prefix>/<backup_id>/topics/` and a GET of
`<prefix>/<backup_id>/manifest.json` — and refuses when either exists: a
finished set, or the segments of a run that died or is still running. Anything
else under the directory is not the engine's output in the configuration
Logweir renders (`offsets.db` and `consumer-groups-snapshot.json` are written
only by continuous backups and an enabled snapshot, which `render_backup` never
turns on), so an upstream archive's snapshot planted beside a new set does not
refuse it. Both reads are under the prefix the run's read-back already reads, so
no permission is added. Between two runs of this build the claim still answers
first, with its own message.

**What it still does not cover.** A runner that ignores the claim and the set
check — a build from before them, after a ROLLBACK — can still run the engine
over a set this build wrote. On a versioned bucket that is DETECTED **for the
points this build signed**: their receipts pin the manifest version, and a
pinned version the bucket still holds that is no longer the current one is
refused by a point-bound restore and reported `Conflict` by the catalog
([below](#the-pinned-manifest-version-versioned-buckets)). **The rewriting
run's own point is not flagged**: the older runner signs a receipt that pins
nothing over the same, identical manifest, so that second point of the set stays
`Available` and selectable while the segments under it no longer match the
entries its manifest lists (measured, FX-7: a SeaweedFS versioned bucket). On an
unversioned bucket nothing is pinned, and an identical manifest over rewritten
segments is visible only to a check of the segment digests the manifest
records — no check this build runs reports it. **So before rolling the runner
back to a build without the execution claim, let in-flight Backups finish**
([release notes](../release-notes.md), "Before a rollback"). An older runner that is STILL RUNNING when its Job is
re-created, and has written nothing yet, is not seen by either check; let such
a Job finish before upgrading. Sets written before RECEIPT-DUP may carry two
receipts, and the catalog keeps both as two points (see
[`catalog-point.md`](catalog-point.md)).

### The pinned manifest version (versioned buckets)

**FX-7, receipt format `1.2.0`** (the MINOR after FX-4's `1.1.0`). On a bucket with versioning enabled the store
answers every read with the object's version id. `logweir backup run` keeps the
one its read-back of the manifest was answered with — the version of exactly the
bytes `archive.manifest_sha256` is over, i.e. the LAST manifest the engine wrote
(it re-puts the manifest several times in one run: five versions per run were
measured on SeaweedFS) — and signs it as `archive.manifest_version_id`, at
`format_version` `1.2.0`, beside the `config_coverage` block every receipt
carries. The catalog point record copies it, at its own `1.2.0`
([catalog-point.md](catalog-point.md)).

| The store answered the read-back with | The receipt |
|---|---|
| a version id | `format_version: 1.2.0`, `archive.manifest_version_id: <id>` |
| no version id (MinIO and SeaweedFS unversioned buckets; any filesystem store) | `format_version: 1.1.0`, no `manifest_version_id` key — byte-for-byte the document FX-4's build writes |
| S3's literal `null` (versioning never enabled, or suspended) | as above: a `null` version is replaced in place by the next write, so it pins nothing |

Measured on SeaweedFS 4.48 (versioned, Object Lock) and on MinIO and SeaweedFS
unversioned buckets; AWS S3
[UNVERIFIED — needs a real AWS S3 bucket and a credential source].

**What a reader does with it.** The engine restores from the key's CURRENT
version and knows no other, so a pin is compared with the current version
first. **But a version id belongs to one object in ONE bucket**, and the
catalog makes an archive copied to a second bucket one point in two places
([catalog-point.md](catalog-point.md)): a copy made by anything but
version-preserving replication — `aws s3 sync`, `mc mirror`, rclone, a
migration to another store, any unversioned destination — carries the pin and
not the pinned version. So when the current version is not the pin, both
readers read the pinned version BY ID (one more read, made only then) and let
its answer decide (FX-7 fix round; one rule for both, `catalog::pin`):

| The read of the pinned version | Point-bound restore | `catalogSync` deep check |
|---|---|---|
| **the bucket holds it** and it is not current: the set was written again in this bucket after the point was signed | exit 3 `PointBindingMismatch`, saying whether the attested manifest is still retained at that version | `Conflict`, not selectable; the remedy says the set was written again in this bucket |
| **the bucket does not hold it**: `404 NoSuchVersion`; `400 InvalidArgument` for an id the store could never have issued (MinIO answers that for any id that is not a UUID, measured); or a store that does not read by version at all — a copy, an unversioned bucket, a version a lifecycle rule expired | the manifest digest decides, as for a point without a pin; the run goes on and the runner logs `PointPinUnchecked`, "the pin could not be checked in this bucket" | the digest decides; the entry's `remedy` carries the same note after the state's own remedy |
| **any other failure** — a 403 (the principal lacks `s3:GetObjectVersion`), an outage | exit 1: could not tell, nothing restored | `Unreadable`: could not tell |

The deep check takes the pin from the verified RECEIPT, never from the record
(an older writer's record may lack it), and reserves the extra read in its
per-point object budget. A pinned point whose version IS the current one is
exactly as before, and costs no extra read. The read by id needs
`s3:GetObjectVersion` on the archive prefix, beside the `s3:GetObject` the
manifest read already needs.

**The cost of reading a copy as a copy.** "Not this bucket's history" and
"this bucket's history, expired" are one answer to a reader. When a lifecycle
rule has expired the noncurrent versions of a manifest that was written again
in its ORIGINAL bucket, the pinned version is gone, and that point degrades to
the unversioned case: the digest alone, which an identical manifest over
rewritten segments passes — with the note, never a refusal. A copy cannot see
either whether the original was written again before it was copied. Keep
noncurrent manifest versions at least as long as the points that pin them.

The digest alone cannot give that answer: an identical manifest over rewritten
segments hashes the same. **An auditor** reads the attested bytes by version and
hashes them:

```
aws s3api get-object --bucket <bucket> --key <archive.manifest_key> \
  --version-id <archive.manifest_version_id> manifest.json
sha256sum manifest.json    # equals archive.manifest_sha256
```

**Limits.** Only the MANIFEST is pinned: segments are not, so a rewrite is
detected, not undone, and a restore of a superseded point is refused rather than
attempted — the attested version is still in the bucket's history (the refusal
says so) for a recovery by hand. A pin is checked only in a bucket that holds
the pinned version. An unversioned bucket gets no pin. Absent never means
"version zero", and a pin is never inferred for a receipt that does not carry
one: `logweir catalog sync` copies the receipt's pin or writes none.

`--receipt-out <path>` additionally writes the same bytes to `<path>` and the
DSSE sidecar to `<path>` with the extension replaced by `.sig` — the pairing
`drill run --out` already uses. `--out` is the same flag by another name;
naming two DIFFERENT paths is refused before anything runs, because this command
writes exactly one document.

## Regenerating the fixture

```
just fixtures-sign
```

`mint_backup_receipt_fixture` **reads** the pinned throwaway key at
`e2e/fixtures/signed/signing.pem` and never mints one — a fresh key would orphan
the fingerprint [`../verify-a-scorecard.md`](../verify-a-scorecard.md) teaches
auditors to pin. It writes the document and the signature over exactly those
bytes itself, in one process, after validating the document against every
arm (the fixture is a 1.0.0 document, so arms 6–11 have nothing to read), so there is no window in which the tracked document and the tracked
signature over it disagree.

## Regenerating the schema

```
just schema
```

Regenerates the checked-in schemas from their Rust types. The CI drift arm
fails on any difference, so `just schema` is the only sanctioned way to change
the current receipt schema, `schemas/logweir-backup-receipt-1.2.0.json`. The
1.0.0 and FX-4's 1.1.0 files beside it are frozen and are not regenerated;
`crates/logweir-core/tests/schema_drift.rs::
the_frozen_1_0_0_receipt_schema_is_still_the_1_0_0_schema` and
`::the_frozen_1_1_0_receipt_schema_is_still_fx4s` keep them what they were.

## Upgrade, rollback and old receipts (format 1.1.0)

- **Every receipt this build signs carries `config_coverage`**, at 1.1.0 — or
  at 1.2.0 when it also pins its manifest's version (FX-7,
  [below](#upgrade-rollback-and-old-receipts-format-120)). The payload type is
  unchanged, so every reader that verifies a receipt today still verifies a new
  one.
- **Readers built before FX-4 accept 1.1.0 receipts**: they compare majors only
  and ignore the unknown field. Measured for FX-4 with both readers at
  `ac76cd0d` (`git show ac76cd0d:docs/verify_scorecard.py`, script 1.14.0, and
  a `logweir` built there): each exits 0 on the three accepted 1.1.0 corpus
  receipts in `e2e/fixtures/invariants/`, signed with the fixture key. They do
  not enforce arms 6–11 — they accept
  `config_coverage_value_outside_the_three.json` too — and they print no
  coverage, so an auditor who needs the coverage verifies with script 1.15.0 or
  a `logweir` built from FX-4 on.
- **Old receipts are never reinterpreted.** A 1.0.0 receipt verifies exactly as
  before under both readers, and every consumer — the catalog, a restore's
  configuration parity, FX-8's timestamp rule — reads its coverage as UNKNOWN.
  The signed fixture `e2e/fixtures/signed/backup-receipt.json` stays 1.0.0 and
  is that case.
- **Rollback.** An older `logweir backup run` writes 1.0.0 receipts again: the
  points it produces read coverage `unknown`, so a restore of them by a runner
  from FX-4 on reports configuration parity `not assessed` (a restore by an
  older runner reports parity as it always did). Receipts already written at
  1.1.0 stay valid and verifiable.
- **Permissions.** `captured` needs the backup principal to hold
  `DescribeConfigs` on every backed-up topic (beside `Read` and `Describe`).
  Without it on one topic the backup still runs: that topic reads
  `captureDenied`, and every other topic in the run that HAS overrides reads
  `notCaptured` (`manifestDiffers`), because the engine's capture is
  all-or-nothing and its record for them is empty. Measured on the compose
  stack in `e2e/tests/config_coverage.rs`.

## Upgrade, rollback and old receipts (format 1.2.0)

- **A pinned receipt is 1.2.0; every other receipt is FX-4's 1.1.0.** The pin
  is the only difference: `schemas/logweir-backup-receipt-1.2.0.json` is the
  frozen 1.1.0 schema plus the optional `archive.manifest_version_id`, with no
  other property, type or required field moved. The payload type keeps
  `version=1.0.0`, and no arm reads the pin.
- **Readers built before FX-7 accept 1.2.0 receipts and ignore the pin** —
  FX-4's (script 1.15.0, a `logweir` built after FX-4 and before FX-7) and the ones before
  them: they compare majors only, arm 6 reads the 1.2 minor as "at least 1",
  and none of the receipt's types refuses an unknown field. They print no
  manifest version, so an auditor who needs the pin verifies with script 1.16.0
  or a `logweir` built from FX-7 on.
- **Rollback.** A build from before FX-7 writes unpinned receipts again (1.1.0
  from FX-4's build, 1.0.0 before it), and its readers neither print nor check
  a pin. The 1.2.0 receipts already written stay valid and verifiable under
  every major-1 reader. Before rolling the runner back past the execution
  claim, read [what it still does not cover](#the-execution-claim-one-engine-run-per-backup_id).

---

Documentation is licensed [CC-BY-4.0](../LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
