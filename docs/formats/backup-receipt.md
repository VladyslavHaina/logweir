# The backup receipt format, field by field

`application/vnd.logweir.backup-receipt+json;version=1.0.0`

The machine-readable schema is
[`schemas/logweir-backup-receipt-1.5.0.json`](../../schemas/logweir-backup-receipt-1.5.0.json)
(PROD-04.1's `consumer_positions`; PROD-01.3's
[`1.4.0`](../../schemas/logweir-backup-receipt-1.4.0.json) and PROD-05.1's
[`1.3.0`](../../schemas/logweir-backup-receipt-1.3.0.json) files are frozen
beside it) and CI regenerates it from the Rust type and `diff -u`s it against the checked-in
file on every build, so this document and the schema cannot drift apart
silently. A MINOR bump is a new schema file beside the old one: the
[`1.2.0` schema](../../schemas/logweir-backup-receipt-1.2.0.json), which
describes every pinned receipt written before PROD-05.1 (FX-7's format), the
[`1.1.0` schema](../../schemas/logweir-backup-receipt-1.1.0.json), which
describes every unpinned receipt written before PROD-05.1 (FX-4's format), and
the [`1.0.0` schema](../../schemas/logweir-backup-receipt-1.0.0.json),
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
| `source.auth.mode` | string | **A closed set, versioned (PROD-01.3): `plaintext` or `scramSha512` in every format, and from format `1.4.0` also `scramSha256`, `plain` (SASL/PLAIN, only ever over TLS) or `mtls` (a TLS client certificate).** These are `AuthSpec`'s serde tag values, the `KafkaCluster` CRD's `auth.mode` enum byte for byte, and the only strings `AuthSpec::mode_str()` returns — so the spec an adopter writes, the CRD they apply and this signed document all spell the mechanism the same way. A receipt naming one of the three new values is written as `1.4.0`; **any other value, or a new value under a version that predates it, is refused by both readers** (arm 5). |
| `source.auth.username` | string \| null | The SASL username, when there is one. `null` under `plaintext` — which is not the same as an empty username — and under `mtls`, whose identity is the client certificate (no Logweir process reads it). |
| `source.topics` | array of string | The named topic allowlist. A **named set with no glob metacharacter**, so this is the exact set of topics and not a pattern a reader would have to re-expand against a cluster it cannot see. |

**`auth` never carries a password, and has no field that could hold one.** The
secret reaches the engine through its own `${VAR}` environment expansion and is
never interpolated by Logweir — which is exactly what stops it being
interpolated into a document Logweir then signs and publishes.

## `engine` — what took the backup

| Field | Type | Meaning |
|---|---|---|
| `engine.id` | string | `oso-cli`. |
| `engine.version` | string | The engine's `--version` token, at or above the supported floor: `0.23.3+logweir.2` for Logweir's build (PROD-00.2; `+logweir.1` before FX-21's patch 0002), `0.23.3` for OSO's release. |
| `engine.digest` | string | `sha256:…`: Logweir's build-input digest (`third_party/kafka-backup-build.env`) for Logweir's build, the image digest from `third_party/kafka-backup-binary.digest` for OSO's release. Inside the runner image both fields are what the image declares in `/etc/logweir/engine-identity`. |

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
carries topic overrides only. A restore bound to this receipt reads it: a
point-in-time selection over a topic recorded here as `LogAppendTime` is
refused, `PointInTimeByProducerTime`, unless the plan states
`restore.time_basis: producerTime`
([the plan field](drill-spec.md#restoretime_basis-fx-8)). The receipt's format
does not change for it; both readers print one `time basis:` line per such
topic, because the covered window above is that topic's PRODUCER time.

**What `captured` does not claim.** That every setting of the topic was
archived: overrides outside the engine's allowlist (`local.retention.ms`, a
provider-specific key) are never part of the claim. Which settings are portable
is [`topic_configuration`](#topic_configuration--the-topic-configuration-model-format-130)
below; how they are applied is PROD-05.2.

---

## `topic_configuration` — the topic configuration model (format 1.3.0)

**Why it exists (PROD-05.1).** A restore used to rebuild a topic from the
manifest's partition count and nothing else: the source's replication factor,
its settings and whether something outside Kafka manages it were not part of
any signed document. This block records them per named topic, so a recovery
point says what the topic WAS and what of it can be carried to a target.

An object keyed by topic name, **one entry per `source.topics` entry and no
others** (arm 14), always beside `config_coverage` (arm 13):

```json
"topic_configuration": {
  "orders": {
    "partitions": 6,
    "replication_factor": 3,
    "entries": {
      "cleanup.policy":      { "value": "compact",   "source": "dynamicTopicConfig", "portability": "portable" },
      "min.insync.replicas": { "value": "2",         "source": "dynamicTopicConfig", "portability": "portable" },
      "retention.ms":        { "value": "604800000", "source": "defaultConfig",      "portability": "inherited" }
    },
    "owner": { "kind": "strimzi", "basis": "kafkaTopicResource", "reference": "kafka/orders" }
  },
  "payments": { "partitions": 1, "replication_factor": 1 }
},
"owner_detection": ["kafkaTopicResources"]
```

| Field | Type | Meaning |
|---|---|---|
| `partitions` | integer ≥ 1, optional | The source's partition count as the archive manifest records it — the count a restore creates the topic with. Absent: not recorded. |
| `replication_factor` | integer ≥ 1, optional | The source's replication factor: `logweir backup run`'s own metadata read before the engine (the smallest replica count of the topic's partitions), and the manifest's only where that read named none — the pinned engine keeps it in the manifest for the first topic it saves only ([why](../to-do/decisions/PROD-05.1-configuration-model.md#5-the-replication-factor-an-engine-defect-measured)). Absent: not recorded. |
| `entries` | object, optional | The configuration entries Logweir's own read returned (FX-4's read, before the engine): every explicit override, and the effective value of each semantic key. **Present exactly when that read succeeded** (arm 15): absent for `captureDenied` and `describeFailed`, which is NOT RECORDED — never "no configuration". |
| `entries.<key>.value` | string, optional | The value in force. **Absent exactly when `portability` is `secret`** (arm 17): a sensitive entry is recorded by key, never by value. |
| `entries.<key>.source` | string | `config_coverage`'s six sources (arm 16). |
| `entries.<key>.portability` | string | `portable`, `inherited`, `removedInKafka4`, `clusterBound`, `requiresTieredStorage`, `providerOnly` or `secret` (arm 16); `inherited` exactly when the source is not `dynamicTopicConfig` (arm 17). |
| `owner.kind` | string | `strimzi` or `external`. |
| `owner.basis` | string | `kafkaTopicResource` (a Strimzi `KafkaTopic` named the topic; `strimzi` only) or `declared` (the plan's `source.topic_owners`). |
| `owner.reference` | string, 1–256 characters | Where the desired state lives: the `KafkaTopic`'s `namespace/name`, or the plan's words. Never a credential. |

`owner_detection` sits beside the block, at the top level of the receipt:

| Field | Type | Meaning |
|---|---|---|
| `owner_detection` | array of strings, optional | WHERE the run looked for declarative owners: `declared` (the plan carried `source.topic_owners`, an empty list included) and `kafkaTopicResources` (it was given `--kafka-topic-resources`), each at most once, only beside `topic_configuration` (arm 20). Every recorded `owner` names a source listed here (arm 21). **Empty means the run looked nowhere**: a topic without an `owner` then has its owner NOT CHECKED, and both readers print `owner not checked, so how it is applied is not known` for it — never the admin-API route. Absent reads as empty. |

How a topic is applied follows from the two together: an owned topic is
restored by exporting desired state for its owner; a topic without an owner is
applied through the admin API only where `owner_detection` is not empty (both
readers print `no declarative owner found (<sources>), so applied through the
admin API`); otherwise how it is applied is not known. A `Backup` or
`BackupSchedule` run by the controller passes neither a declaration nor
resources yet (child row PROD-05.1a), so its receipts record `owner_detection:
[]`.

**The classes, the measured table and what a restore does with each** are in
the [decision record](../to-do/decisions/PROD-05.1-configuration-model.md): the
36 topic keys Apache Kafka 3.9 defines and the 33 of 4.x, each with its
`CreateTopics validate_only` verdict on both lines. A class is the writer's at
write time; the arms judge it against its source and value, never its key.

**Owners.** `logweir backup run --kafka-topic-resources <file>
[--strimzi-cluster <name>]` reads Strimzi `KafkaTopic` resources (`kubectl get
kafkatopics -A -o yaml`): a resource labelled `strimzi.io/cluster` (that
cluster, when named), not annotated `strimzi.io/managed: "false"`, owns the
topic its `spec.topicName` (else its name) names. The plan's
`source.topic_owners` declares owners itself; phase −1 refuses, exit 3, a
declaration naming an unplanned topic, another kind or an unusable reference,
or a topic declared twice, and a declaration wins over a resource for the same
topic.

## `consumer_positions` — consumer position evidence (format 1.5.0)

**Why it exists (PROD-04.1).** A restore that brings a topic back still leaves
its applications to guess where to resume. This block records, at backup time,
the committed positions of the consumer groups the backup was asked about —
natively, through Logweir's own client, never through the engine — so a later
cutover (PROD-04.2) and an auditor read them from signed evidence, with the
source gone if need be.

**Selected, never discovered.** The block is present exactly when the backup
selected consumer groups: the plan's `source.consumer_groups`, `logweir backup
run --consumer-group <id>` (repeatable), or a `Backup`/`BackupSchedule`
`spec.consumerGroups`. At most 100 exact ids; phase −1 refuses, exit 3, a blank,
repeated or control-character id. A backup that selects none writes no block,
and its receipt is the 1.3.0 (or 1.4.0) document it was, byte for byte.

```json
"consumer_positions": {
  "observed_from": "2026-10-09T03:00:01.120Z",
  "observed_to": "2026-10-09T03:00:01.480Z",
  "listing": "complete",
  "topics": {
    "orders": {
      "partitions": [
        { "partition": 0, "observed": true, "log_start": 0, "high_watermark": 40,
          "log_start_after": 0, "high_watermark_after": 41, "archived_first": 0, "archived_last": 40 },
        { "partition": 1, "observed": false, "log_start_after": 0, "high_watermark_after": 2,
          "archived_first": 0, "archived_last": 1 }
      ],
      "changed_during_capture": false
    }
  },
  "groups": {
    "billing":   { "outcome": "captured", "group_type": "classic", "state": "Stable",
                   "listed_state": "Stable", "members": 2, "active": true,
                   "positions": [
                     { "topic": "orders", "partition": 0, "status": "captured", "position": 17, "coverage": "withinArchive" },
                     { "topic": "orders", "partition": 1, "status": "notObserved", "reason": "PartitionAddedDuringCapture" } ] },
    "word-count": { "outcome": "excluded", "reason": "GroupTypeNotCaptured", "group_type": "other" },
    "audit":     { "outcome": "failed", "reason": "NotVisibleToPrincipal" }
  }
}
```

**When it is read, and what that means.** Just before the engine starts, the
runner classifies every selected id (PROD-04.0's listings, §5), describes the
capturable ones, reads each one's committed positions on every partition of
every named topic (one RequireStable fetch per group, so a pending
transactional offset commit never yields the pre-transaction position), and
then reads each partition's log start and high watermark (`READ_UNCOMMITTED`).
After the engine it reads the marks again, and takes the archived range of each
partition from the manifest it digested. **Positions observed while
applications run are not atomic with the records the engine reads**: an
`active` group may commit again a moment later, and nothing makes the groups
and the records one consistent cut. `observed_from`/`observed_to` say when the
positions were read; the recovery point (`started_at`) follows them.

| Field | Type | Meaning |
|---|---|---|
| `observed_from`, `observed_to` | RFC 3339 | When the group capture started and ended — before the engine. |
| `listing` | `complete` or `notComplete` | Whether the group listings were complete (Describe on the cluster, no lost broker). `notComplete`: an unlisted id was classified by a targeted describe (PROD-04.0 §5, T14). |
| `topics.<t>.partitions[]` | array | One entry per partition, `partition` equal to its index (arm 24): the partitions the capture read, then any the read after the engine or the archive shows beyond them. |
| `…observed` | boolean | Whether the capture read this partition. `false`: added after the capture read the topic (or that read failed), so no position was asked for it. |
| `…log_start`, `…high_watermark` | integer ≥ 0, optional | The group-capture marks. Absent: not read (never 0). |
| `…log_start_after`, `…high_watermark_after` | integer ≥ 0, optional | The marks read again after the engine. |
| `…archived_first`, `…archived_last` | integer ≥ 0, optional | The lowest and highest offsets the manifest records for the partition (inclusive). Absent: nothing archived. |
| `topics.<t>.changed_during_capture` | boolean | A mark read after the engine is below the one read at group capture (a recreation or a truncation, PROD-01.4 §4.4). Re-derived by arm 26. |
| `groups.<id>.outcome` | `captured`, `excluded`, `failed` | Exactly one per selected id. |
| `groups.<id>.reason` | string | Present exactly when not `captured`. `excluded`: `GroupTypeNotCaptured` (a share or streams group, another protocol, or a type the client cannot name — **every group on a broker below ListGroups v5, Kafka 3.7.x**), `GroupNotFound`. `failed`: `NotVisibleToPrincipal`, `NotVisibleOrUnreachable`, `NotAuthorized`, `PositionsUnstable`, `ListingInconsistent`, `AbsenceUnproven`, `TypeUnproven`, `Unreachable`, `DescribeFailed`, `PositionsFailed`, `GenerationChangedDuringCapture`, `CaptureUnavailable`. |
| `groups.<id>.group_type` | string, optional | `classic` or `consumer` for a captured group, `other` for `GroupTypeNotCaptured`, absent otherwise. |
| `groups.<id>.state`, `listed_state` | string | Captured only: the description's state and the listing's, from `PreparingRebalance`, `CompletingRebalance`, `Stable`, `Dead`, `Empty`, `stateUnknownToClient`. They differ when the group changed between the two reads. |
| `groups.<id>.members` | integer | Captured only: the members the description listed. |
| `groups.<id>.active` | boolean | Captured only: either state is not `Empty`/`Dead`, so its positions may have moved after they were read (arm 30). |
| `groups.<id>.positions[]` | array | Captured only: **one per partition of every named topic**, topics in name order, partitions in order (arm 32). |
| `…status` | string | `captured` (a committed position the facts can judge), `noCommittedPosition` (the broker holds none — **never offset 0**), `excluded` (`PositionBeyondEnd`), `failed` (`TopicNotAuthorized`, `Unstable`, `NotAPosition`, `PartitionFailed`, `MarksNotRead`), `notObserved` (`PartitionAddedDuringCapture`, `TopicNotObserved`). |
| `…position` | integer ≥ 0 | The next offset the group would consume. Present exactly for `captured` and `excluded`. |
| `…coverage` | string | Captured only — whether the position relates to archived data (below). |

**Which positions relate to archived data.** One rule, re-derived by both
readers from the recorded facts (arm 34), first that holds:

| Condition | Status / coverage | Relates? |
|---|---|---|
| the marks were not read | `failed: MarksNotRead` | — |
| position > high watermark | `excluded: PositionBeyondEnd` (a commit made through an older generation's offsets, PROD-01.4 TI-04.1-2) | — |
| position < log start | `beforeLogStart`: the records it would read next had expired from the source | no |
| nothing archived for the partition | `noArchivedData` | no |
| position < first archived offset | `beforeArchive` | no |
| position ≤ last archived offset | `withinArchive` | **yes** |
| position = last archived offset + 1 | `atArchiveEnd`: the group had read everything archived | **yes** |
| otherwise | `beyondArchive` | no |

**A topic that changed during the capture** fails every group holding a kept
position on it, `GenerationChangedDuringCapture` (arm 31): those offsets may name
records of another generation. **Generation:** the receipt records no topic
generation token and no topic id yet (PROD-02.1, PROD-01.4a), so a snapshot
relates to this point's data only, through its marks, and to "generation
unknown" for every other point (PROD-01.4 TI-04.1-4).

**Engine snapshots.** Logweir's own backups never ask the engine for its
`consumer-groups-snapshot.json`. One found beside a foreign archive is read only
as an import source (FX-1's reader, `ConsumerGroupSnapshotRead::imported`),
every group typed `unknown`, never shown as a consumer group's and never
applied: the engine records no type and keeps a streams group's positions
untyped (PROD-04.0 §3.7).

Both readers print one `consumer_positions` line per group — its outcome, and
for a captured group its type, both states, members, whether it was active and
how many positions relate to archived data — and the catalog point carries the
same summary, bound by the block's digest
([catalog-point.md](catalog-point.md)).

### Recovering positions with the source offline

The positions are in the signed receipt in the evidence store, so they survive
the source cluster. With the source gone:

1. Find the point: `logweir catalog list` over the destination, or the
   `logweir/backups/<backup_id>/` prefix in the evidence bucket.
2. Fetch the receipt and its sidecar
   (`logweir/backups/<backup_id>/<run_id>.receipt.{json,sig}`), and verify them
   with no network: `logweir drill verify --payload-type backup-receipt
   --scorecard receipt.json --signature receipt.sig --public-key <key>` (or
   `docs/verify_scorecard.py --payload-type backup-receipt`). Both print each
   group's outcome.
3. Read `consumer_positions.groups.<id>.positions` from the verified document.
   Use only `captured` positions whose `coverage` relates (`withinArchive`,
   `atArchiveEnd`); every other status is a reason NOT to set a position, and a
   `noCommittedPosition` is never 0.
4. A position is a SOURCE offset. Restored records carry their source offset in
   the `x-original-offset` header: resume a group on the target at the first
   restored record whose `x-original-offset` is at or above the position (and
   at the partition's end for `atArchiveEnd`), with the group stopped
   (`kafka-consumer-groups.sh --reset-offsets --to-offset … --execute`).
   PROD-04.2 makes this a reviewed, audited step; until it lands it is an
   operator's.

---

## The thirty-four arms

`logweir_core::backup_receipt::BackupReceipt::validate_invariants` implements
these, and `docs/verify_scorecard.py::check_backup_receipt_invariants` mirrors
them ARM FOR ARM, IN ORDER. The messages below are the **exact** refusal text of
BOTH readers — compared byte-for-byte by
`crates/logweir-core/tests/backup_receipt.rs` (`backup_receipt_refuses_each_self_contradiction_arm_with_its_exact_message`
over arms 1–4, `arm_5_refuses_an_auth_mode_outside_the_closed_two` and
`arm_5_is_versioned_by_the_prod_01_3_modes` over arm 5, one `arm_N_…` test per
arm 6–34, and
`validate_invariants_has_exactly_thirty_six_return_err_statements` over the
total — thirty-four arms, thirty-six statements, because arm 5 is three since
1.4.0), by `crates/logweir/tests/two_reader_parity_receipt.rs::two_reader_parity_over_the_backup_receipt_corpus`
over the documents in `e2e/fixtures/invariants/backup-receipt-index.json`
(a refusing case for each half of arms 15–19, not only for each arm),
and by `scripts/check-verifier-parity.sh`'s second loop — and they are not to be
reworded. `scripts/check-invariant-corpus.sh` additionally derives the arm list
from both readers' source and refuses to balance if they are not the same
thirty-four arms in the same order.

Arms 6–11 read `config_coverage` and NOTHING ELSE, and run only when it is
present — so every receipt without it, which is every receipt written before
1.1.0, is accepted or refused exactly as before. Within the block, topics are
visited in name order and arms 8–11 run per topic, in order; the 1.0.0 arms
always run first.

Arms 12–19 run only when `topic_configuration` is present, after arms 1–11,
and judge it against `config_coverage` and `source.topics` — so every receipt
without it, which is every receipt written before 1.3.0, is decided exactly as
before. Topics in name order; per topic arm 15, then arms 16 and 17 per entry in
key order, then 18 and 19. Arms 20 and 21 run last, over `owner_detection`.

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

5. **`source.auth.mode` is `plaintext` or `scramSha512` (and from 1.4.0 also
   `scramSha256`, `plain` or `mtls`), and nothing else.** The
   only arm that is not a claim the document makes against itself: the receipt
   does not contradict itself, it names a mechanism this format has no spelling
   for. It is last for that reason — a document that contradicts itself should
   be told so first. The two values are `AuthSpec`'s serde tags, so `logweir`
   itself has exactly one writer of this field and no way to reach a third
   value; the arm is here for the documents this tree did not write, and
   because `Backup.status.auth.mode` — which the operator copies FROM this
   field — promises the same two in its own CRD description.

   > `source.auth.mode "scram-sha-512" is not one of the two values this format defines: "plaintext" or "scramSha512"`

   **Versioned since 1.4.0 (PROD-01.3), as three statements.** Below 1.4.0 the
   closed set is the two above, judged exactly as before (5a). `scramSha256`,
   `plain` or `mtls` under a version before 1.4.0 is a value no writer of that
   version produced (5b); from 1.4.0 the closed set is five (5c). An older
   reader refuses a 1.4.0 receipt that names a new mode through 5a — the safer
   verdict, which is what makes the change MINOR (OD-7, third case).

   > `source.auth.mode "plain" is defined from 1.4.0 and format_version "1.0.0" predates it`

   > `source.auth.mode "oauthbearer" is not one of the five values this format defines: "plaintext", "scramSha512", "scramSha256", "plain" or "mtls"`

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

12. **`topic_configuration` is present only under a minor of at least 3.**

    > `topic_configuration is present but format_version "1.2.0" predates it: the field is defined from 1.3.0`

13. **`topic_configuration` is present only beside `config_coverage`**, the read
    its entries are judged against.

    > `topic_configuration is present under format_version "1.3.0" but config_coverage is not: a topic's configuration entries cannot be judged without the read that produced them`

14. **`topic_configuration` covers exactly `source.topics`** — arms 3 and 7's twin.

    > `topic_configuration covers {"orders"} but the named topic set is {"orders", "payments"}`

15. **`entries` are present exactly when the topic's configuration read
    succeeded** (`captured`, or `notCaptured` with `manifestDiffers`). The
    coverage is rendered with its reason, `"notCaptured/describeFailed"`.

    > `topic_configuration["payments"].entries present does not fit its config_coverage "captureDenied": entries are recorded exactly when the configuration read succeeded ("captured", or "notCaptured" with reason "manifestDiffers")`

16. **Every entry's source and class are from the closed sets.**

    > `topic_configuration["orders"].entries["segment.ms"] source "dynamicTopicConfig" and portability "portableish" are not a source and class this format defines: the source is "dynamicTopicConfig", "dynamicBrokerConfig", "dynamicDefaultBrokerConfig", "staticBrokerConfig", "defaultConfig" or "unknown", and the class is "portable", "inherited", "removedInKafka4", "clusterBound", "requiresTieredStorage", "providerOnly" or "secret"`

17. **A `secret` carries no value and nothing else lacks one; otherwise
    `inherited` is exactly a value the topic did not set itself.** A broker
    default passed off as the topic's portable override is refused here.

    > `topic_configuration["orders"].entries["retention.ms"] is "portable" from "defaultConfig" with a value: an entry is "secret" exactly when it carries no value, and otherwise "inherited" exactly when its source is not "dynamicTopicConfig"`

18. **An `owner` is from the closed sets, with a usable reference.**

    > `topic_configuration["payments"].owner "external" by "kafkaTopicResource" is not an owner this format defines: the kind is "strimzi" or "external", the basis is "kafkaTopicResource" (for "strimzi" only) or "declared", and the reference is 1 to 256 characters with no control character`

19. **A recorded partition count or replication factor is at least 1.**

    > `topic_configuration["orders"] records partitions 6 and replication_factor 0: a recorded count is at least 1`

20. **`owner_detection` is from the closed set, each source at most once, and
    only beside `topic_configuration`.**

    > `owner_detection ["declared", "labels"] is not a detection this format defines: it is present only beside topic_configuration, and lists "declared" and "kafkaTopicResources" each at most once`

21. **Every recorded owner names a source the run looked in.** A `declared`
    owner needs `declared`, a `kafkaTopicResource` owner needs
    `kafkaTopicResources`; an absent `owner_detection` is an empty one.

    > `topic_configuration["orders"].owner by "kafkaTopicResource" names no source owner_detection ["declared"] lists: a "declared" owner needs "declared", a "kafkaTopicResource" owner "kafkaTopicResources"`

Arms 22 to 34 (format 1.5.0, PROD-04.1) read `consumer_positions` and run only
when it is present; every document without it is decided exactly as before.
Each has a corpus case in `e2e/fixtures/invariants/backup-receipt-index.json`
(`consumer_positions_*`).

22. **`consumer_positions` is present only under a minor of at least 5.**

    > `consumer_positions is present but format_version "1.4.0" predates it: the field is defined from 1.5.0`

23. **Its topics are exactly `source.topics`.**

    > `consumer_positions.topics covers {"orders"} but the named topic set is {"orders", "payments"}`

24. **Each topic lists its partitions from 0, once each, in order.**

    > `consumer_positions.topics["orders"].partitions[1] is partition 2: each topic lists its partitions from 0, one entry each, in order`

25. **Marks and ranges are whole, non-negative and ordered**, and an
    unobserved partition has no group-capture marks.

    > `consumer_positions.topics["orders"].partitions[0] records marks that are not well formed: a log start and its high watermark are recorded together with 0 <= log start <= high watermark, the archived range is recorded whole with 0 <= first <= last, and a partition the capture did not observe has no group-capture marks`

26. **`changed_during_capture` is what the marks say.**

    > `consumer_positions.topics["orders"].changed_during_capture is false but its marks say true: a topic changed during the capture exactly when a mark read after the engine is below the one read at group capture`

27. **The listing word is closed, and at least one group is recorded.**

    > `consumer_positions records listing "partial" and 5 group(s): the listing is "complete" or "notComplete", and at least one group is recorded`

28. **Each group's outcome is closed, with a reason exactly when it is not
    `captured`, from that outcome's set.**

    > `consumer_positions.groups["gone"] has outcome "excluded" and reason "NotVisibleToPrincipal": the outcome is "captured", "excluded" or "failed", a reason is present exactly when it is not "captured", and it is one this format defines for that outcome`

29. **What a group records follows from its outcome** — so a group that was not
    captured can never carry positions.

    > `consumer_positions.groups["hidden"] is "failed" with group_type absent, state absent, listed_state absent, members absent, active absent and positions present: a captured group records a type of "classic" or "consumer", both states from the closed set, its members, active and its positions; a GroupTypeNotCaptured group records group_type "other" and nothing else; any other group records none of them`

30. **`active` is what the two states say.**

    > `consumer_positions.groups["audit"].active is false but its states "Empty" and "PreparingRebalance" say true: a group is active unless both its states are "Empty" or "Dead"`

31. **No kept position on a topic that changed during the capture**, and no
    group fails `GenerationChangedDuringCapture` when none changed.

    > `consumer_positions.groups["audit"] is "captured" with reason absent while the topics that changed during the capture are {"orders"}: a group holding a position on such a topic fails GenerationChangedDuringCapture, and no group fails so when none changed`

32. **One position per partition of every named topic, in order** — a
    partition is never silently missing, so absence cannot pass for offset 0.

    > `consumer_positions.groups["billing"].positions[1] is "orders":2 where "orders":1 is expected: a captured group records one position per partition of every named topic, topics in name order, partitions in order`

33. **Each position's status, value and reason fit one another**, and
    `notObserved` is exactly an unobserved partition. `noCommittedPosition` with
    a position — absence read as 0 — is refused here.

    > `consumer_positions.groups["billing"].positions[1] has status "noCommittedPosition", position 0 and reason absent: the status is one this format defines, a position is present exactly when it is "captured" or "excluded", a reason exactly when it is "excluded", "failed" or "notObserved" and from that status's set, and "notObserved" is exactly a partition the capture did not observe`

34. **A coverage word exactly on a captured position, and every kept
    position's verdict is what its partition's facts derive.**

    > `consumer_positions.groups["audit"].positions[0] is "captured" with coverage "beyondArchive" at position 21, but its partition's facts make it PositionBeyondEnd: a coverage word is recorded exactly on a captured position, and a kept position's coverage, or its PositionBeyondEnd, follows from the marks and the archived range`

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
> readers run **all thirty-four arms above** over a `--payload-type
> backup-receipt` document; arms 1–5 arrived together in Task 5b, arms 6–11
> together in FX-4, arms 12–21 together in PROD-05.1 and arms 22–34 together
> in PROD-04.1, so the two readers never disagreed in between.
> `logweir drill verify` prints `checked:   the signature AND all thirty-four
> backup-receipt invariants …`; `docs/verify_scorecard.py` prints `verifier:
> verify_scorecard.py <SCRIPT_VERSION> (backup-receipt invariant set: …)`. Both also print
> the configuration capture coverage in the same words, one
> `config_coverage["<topic>"]: <coverage>[ (<reason>)], message.timestamp.type
> <value> from <source>` line per topic — or `config_coverage: not recorded, so
> every topic's configuration capture is UNKNOWN, never captured`, for every
> receipt without the block — and `scripts/check-verifier-parity.sh` compares those lines
> between the two readers on every accepted receipt. Both print the topic
> configuration model in the same words, one `topic_configuration["<topic>"]:
> partitions <n>, replication factor <n>, <n> entries (<class> <count>, …),
> <route>` line per topic — counts and classes, never a value; the route is
> `owned by …, so restored by desired-state export`, `no declarative owner
> found (<sources>), so applied through the admin API` or `owner not checked,
> so how it is applied is not known` — or
> `topic_configuration: not recorded, …` for a receipt without the block, and
> the parity script compares those lines too. Both print a pinned
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
> **Where else the arms are enforced — all of them, at each version.** At the two places a receipt is
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
| **the bucket does not hold it**: `404 NoSuchVersion`; `400 InvalidArgument` for an id the store could never have issued (MinIO answers that for any id that is not a UUID, measured); or a store that does not read by version at all — a copy, an unversioned bucket, a version that was expired or deleted | the manifest digest decides, as for a point without a pin; the run goes on and the runner logs `PointPinUnchecked`, "the pin could not be checked in this bucket" | the digest decides; the entry's `remedy` carries the same note after the state's own remedy |
| **any other failure** — a 403 (the principal lacks `s3:GetObjectVersion`), an outage | exit 1: could not tell, nothing restored | `Unreadable`: could not tell; the remedy names `s3:GetObjectVersion` |

The deep check takes the pin from the verified RECEIPT, never from the record
(an older writer's record may lack it), and reserves the extra read in its
per-point object budget. A pinned point whose version IS the current one is
exactly as before, and costs no extra read. The read by id needs
`s3:GetObjectVersion` on the archive prefix, beside the `s3:GetObject` the
manifest read already needs.

**The cost of reading a copy as a copy.** The pin is checked only where the
bucket being read still HOLDS the pinned version and serves it by id: "not this
bucket's history" and "this bucket's history, gone" are one answer to a reader.
So a set that really WAS written again reads like a copy — the digest alone,
which an identical manifest over rewritten segments passes, with the note and
never a refusal — by three routes:

- the pinned version is gone from the bucket that signed the point: a
  lifecycle rule expired it, or a principal holding `s3:DeleteObjectVersion`
  deleted it (measured, FX-7 re-check: deleting the pinned version turned that
  bucket's `Conflict` into `Available` with the note);
- the copy never held it: a copy synced AFTER the set was written again, or a
  copy rewritten after it was made (measured: both copies read `Available` with
  the note while the signing bucket read `Conflict`);
- the store cannot serve a version it holds (a misbehaving S3-compatible store,
  or a proxy that drops `?versionId=`; seen on neither MinIO nor SeaweedFS).

Keep noncurrent manifest versions at least as long as the points that pin them;
Object Lock retention covering a point's lifetime keeps its pinned version.
Measured on SeaweedFS 4.48 (FX-7 renumber, compose slot 2): a `GOVERNANCE`
retention on a rewritten point's pinned version refused that version's delete
(`AccessDenied`, the version count unchanged) and the point stayed `Conflict`.
Governance mode yields to a principal holding `s3:BypassGovernanceRetention`,
compliance mode to nobody; that a lifecycle rule cannot expire a retained
version, and AWS S3 itself, are
[UNVERIFIED — needs a real AWS S3 bucket and a credential source]. When the
signing bucket's catalog says `Conflict` and a copy's says `Available`, believe
the `Conflict`: it is evidence about the set, not about the place. Only a check
of the segment digests the manifest records closes all three routes, and this
build runs none.

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
the current receipt schema, `schemas/logweir-backup-receipt-1.3.0.json`. The
1.0.0, FX-4's 1.1.0 and FX-7's 1.2.0 files beside it are frozen and are not
regenerated; `crates/logweir-core/tests/schema_drift.rs::
the_frozen_1_0_0_receipt_schema_is_still_the_1_0_0_schema`,
`::the_frozen_1_1_0_receipt_schema_is_still_fx4s` and
`::the_frozen_1_2_0_receipt_schema_is_still_fx7s` keep them what they were.

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

## Upgrade, rollback and old receipts (format 1.3.0)

- **Every receipt this build signs carries `topic_configuration`, at 1.3.0**,
  pinned or not; an unpinned one still has no `manifest_version_id` key. The
  payload type keeps `version=1.0.0`.
- **Readers built before PROD-05.1 accept 1.3.0 receipts** and ignore the
  block: they compare majors only, arms 6 and 10 read the 1.3 minor as at least
  1, and no receipt type refuses an unknown field (measured: script 1.19.0 over
  1.3.0 receipts in `crates/logweir/tests/receipt_dup.rs` before the Python arms
  existed, and the parity gate's older-reader rows). They print no model, so an
  auditor who needs it verifies with script 1.20.0 or a `logweir` built from
  PROD-05.1 on.
- **Old receipts are never reinterpreted.** A receipt before 1.3.0 records no
  configuration model; a reader then knows none, and a restore says so.
- **Rollback.** An older `logweir backup run` writes 1.1.0 or 1.2.0 receipts
  again; the 1.3.0 receipts already written stay valid under every major-1
  reader.
- **Permissions.** Nothing new: the replication factor comes from the same
  metadata the run already reads (`Describe`), and the entries from FX-4's
  `DescribeConfigs` read.

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
  or a `logweir` built from FX-7 on. Measured (FX-7 renumber, 2026-10-05): a
  `logweir` built at main `b8b9263f` with script 1.15.0, and the released
  `v0.1.5` runner's `logweir` with script 1.14.0, each exit 0 on
  `unmodified_receipt_pinned.json` signed with the fixture key and print for it
  exactly what they print for the same document without the pin at 1.1.0
  (FX-4's two read and print its coverage too); all four exit 0 on a real 1.2.0
  receipt a run signed on a versioned SeaweedFS bucket. A 1.2.0 catalog point is
  accepted by the three that know the type; `v0.1.5` has no `catalog-point`
  payload type at all.
- **Rollback.** A build from before FX-7 writes unpinned receipts again (1.1.0
  from FX-4's build, 1.0.0 before it), and its readers neither print nor check
  a pin. The 1.2.0 receipts already written stay valid and verifiable under
  every major-1 reader. Before rolling the runner back past the execution
  claim, read [what it still does not cover](#the-execution-claim-one-engine-run-per-backup_id).

## Upgrade, rollback and old receipts (format 1.4.0)

- **A receipt is 1.4.0 exactly when its `source.auth.mode` is `scramSha256`,
  `plain` or `mtls`** (PROD-01.3), whatever else it carries — 1.4.0 includes
  PROD-05.1's 1.3.0 `topic_configuration` and FX-7's optional
  `archive.manifest_version_id`. Every receipt of a `plaintext` or
  `scramSha512` backup is the 1.3.0 document this build writes for it (or the
  1.1.0/1.2.0 document an earlier build wrote), byte for byte.
  `schemas/logweir-backup-receipt-1.4.0.json` differs from the frozen 1.3.0
  file in the `source.auth.mode` and `username` descriptions only: no
  property, type or required field moved. The payload type keeps
  `version=1.0.0`.
- **Readers built before PROD-01.3 refuse a 1.4.0 receipt naming a new mode**
  (arm 5a: a value outside the two they know) and accept every other receipt.
  Refusal is the safe direction — a reader that cannot say what a mode means
  does not vouch for the document — so the bump is MINOR (OD-7, third case),
  and an auditor verifying such a receipt uses script 1.21.0 or a `logweir`
  built from PROD-01.3 on.
- **Rollback.** A build from before PROD-01.3 cannot take a backup over
  `scramSha256`, `plain` or `mtls` at all (its `AuthSpec` has no such arm, so
  the spec does not parse), and writes no 1.4.0 receipt. The 1.4.0 receipts
  already written stay valid for every reader from PROD-01.3 on.

## Upgrade, rollback and old receipts (format 1.5.0)

- **A receipt is 1.5.0 exactly when its backup selected consumer groups**
  (PROD-04.1), whatever else it carries — 1.5.0 includes every earlier minor's
  fields. A backup that selects none writes the 1.3.0 or 1.4.0 document it
  wrote before, byte for byte, and a plan without `source.consumer_groups` is
  the plan it was. `schemas/logweir-backup-receipt-1.5.0.json` adds the one
  optional property; the payload type keeps `version=1.0.0`.
- **MINOR under OD-7 (a).** Arms 22 to 34 read only the new block. Readers
  built before PROD-04.1 (`verify_scorecard.py` 1.22.0 and earlier, an older
  `logweir`) accept a 1.5.0 receipt, ignore the block and print no
  `consumer_positions` line — they say nothing about the positions, and decide
  everything else as before.
- **Rollback.** An older runner ignores `source.consumer_groups` and
  `--consumer-group` is an unknown flag to it, so it records no positions; the
  1.5.0 receipts already written stay valid for every major-1 reader. An older
  controller refuses a frozen plan whose execution inputs carry
  `consumerGroups` (`PlanConfigMapConflict`): let such runs finish, or remove
  `spec.consumerGroups` from the schedule, before rolling it back.

---

Documentation is licensed [CC-BY-4.0](../LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
