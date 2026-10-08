# PROD-05.1 — Capture topic configuration with coverage and portability: the configuration model

Decision record for **PROD-05.1** ("Capture topic configuration with coverage and portability") in
the [product-expansion tracker](../product-expansion.md#prod-051--capture-topic-configuration-with-coverage-and-portability).
PROD-05.2 ("Apply a reviewed target topic configuration") consumes it.

- Date: 2026-10-08. Base: main `223fd4af`. Branch `claude/prod-05-1`.
- Kind: implementation row (Tier A). It ships the versioned model, the portability table, the
  projection into the receipt, the catalog point, the catalog's view and the product API, the
  detection of declarative owners, and FX-5's hand-off in the console.
- Engine: `kafka-backup` v0.23.3 (the pin since PROD-00.3f).
- Brokers: `apache/kafka` 3.9.2 and 4.3.1, pinned by digest (`e2e/compose/stack-env.sh --lines`).
- Code: `crates/logweir-core/src/topic_configuration.rs` (the table, the classification, the
  capture rule, the owners), `crates/logweir-core/src/backup_receipt.rs` (`TopicConfiguration`,
  arms 12–19), `crates/logweir/src/backup/config_coverage.rs` (`model`),
  `crates/logweir-kafka/src/reader.rs` (`ClusterReader::replication_factors`),
  `crates/logweir/src/catalog/` (record 1.3.0, the rule-3 cross-check),
  `crates/logweir/src/check/kinds/catalog_sync.rs` and `crates/weirkeeper/src/catalog_view.rs`
  (the view's `topics`), `crates/logweir-api/src/routes/catalogs.rs` (`PointView.topics`),
  `ui/pages/restore-wizard.js` (`sourceReplicationFactorsOf`).
- Oracles: `e2e/tests/topic_configuration.rs` (three live rows, run on both lines, §7),
  `crates/logweir-core/tests/backup_receipt.rs` (one row per arm), the corpus and the parity
  script (§6).

## 0. Decision summary

1. **Every receipt this build signs records each named topic's configuration model**, in the new
   optional block `topic_configuration` (receipt format 1.3.0): the source's partition count and
   replication factor, the configuration entries Logweir's own read returned, each with its source
   and a portability class, and the topic's declarative owner. The catalog point record (format
   1.3.0) copies it per topic.
2. **What is recorded** is every explicit topic override (any key, a provider's included) and the
   effective value of each SEMANTIC key (§3), whatever its source. Nothing else: a non-semantic
   inherited value is the target broker's business.
3. **Coverage is FX-4's.** The entries come from the same pre-engine DescribeConfigs read as
   `config_coverage`, and exist exactly where that read succeeded (`captured`, or `notCaptured`
   with `manifestDiffers`). A denied or failed read records NO entries, never an empty set, so the
   model can never say "no overrides" for a topic whose configuration was not read.
4. **The portability table is measured** on both broker lines (§2): 36 topic keys on 3.9, 33 on
   4.x, and the `CreateTopics` `validate_only` verdict of every key. Seven classes (§3). Keys removed
   in Kafka 4.0 are `removedInKafka4`: recorded, applied to a 3.9 target only, never read as a
   default. A secret is recorded by key, never by value.
5. **Declarative owners** come from the plan (`source.topic_owners`) or from Strimzi `KafkaTopic`
   resources (`--kafka-topic-resources`). An owned topic is restored by exporting desired state for
   its owner, never by the admin API around it (§4).
6. **The replication factor is Logweir's own metadata read**, not the engine manifest's, because
   the pinned engine keeps it for the first topic it saves only (§5, measured).
7. **The console's replication-factor default now starts from the source's factor**, read from a
   recovery catalog's view of the point and capped by the target's broker count, with where it came
   from said on both steps (§8). FX-5's refusal above the broker count stands.
8. **Versioning:** receipt and catalog point 1.3.0, `verify_scorecard.py` 1.20.0, arms 12–19 MINOR
   under OD-7 (a) (§6).

## 1. The model (receipt 1.3.0, `topic_configuration`)

One entry per `source.topics` entry and no others (arm 14), beside `config_coverage` (arm 13):

```json
"topic_configuration": {
  "orders": {
    "partitions": 6,
    "replication_factor": 3,
    "entries": {
      "cleanup.policy":         {"value": "compact",   "source": "dynamicTopicConfig", "portability": "portable"},
      "message.format.version": {"value": "3.0-IV1",   "source": "dynamicTopicConfig", "portability": "removedInKafka4"},
      "min.insync.replicas":    {"value": "2",         "source": "dynamicTopicConfig", "portability": "portable"},
      "retention.ms":           {"value": "604800000", "source": "defaultConfig",      "portability": "inherited"},
      "vendor.token":           {                      "source": "dynamicTopicConfig", "portability": "secret"}
    },
    "owner": {"kind": "strimzi", "basis": "kafkaTopicResource", "reference": "kafka/orders"}
  }
}
```

| field | from | absent means |
|---|---|---|
| `partitions` | the archive manifest's `original_partition_count` — the count a restore creates the topic with (`drill::phase3_diff::restore_partition_count`) | not recorded, never `0` |
| `replication_factor` | Logweir's metadata read before the engine: the SMALLEST replica count of the topic's partitions (a partition mid-reassignment also lists its adding replicas); the manifest's `source_replication_factor` only where that read named none (§5) | not recorded |
| `entries` | Logweir's DescribeConfigs read before the engine (FX-4's), filtered by the capture rule | the read was denied or failed: NOT RECORDED |
| `entries.<key>.value` | the broker's answer | the entry is `secret` (arm 17) |
| `entries.<key>.source` | the broker's `ConfigSource`, camel-cased (FX-4's six) | — |
| `entries.<key>.portability` | §3 | — |
| `owner` | §4 | no declarative owner is known: the topic is applied through the admin API |

A block that is ABSENT (every receipt before 1.3.0) means NOT RECORDED for every topic. The catalog
point record carries the same entry as `topics[].configuration` and fills its existing
`topics[].partitions` from it; both are receipt-derived (D3 §5.2 rule 3), and `reader::cross_check`
refuses a record whose copy the verified receipt does not back.

## 2. The portability table, measured

`logweir_core::topic_configuration::TABLE`. Measured by
`e2e/tests/topic_configuration.rs::the_portability_table_is_the_brokers` on the compose stack, slot
3, `--kafka 3.9` (3.9.2) and `--kafka 4.3` (4.3.1): DescribeConfigs of a fresh topic returns exactly
the keys the table defines for the line, and `CreateTopics` with `validate_only` and each key's
sample is accepted or refused exactly as recorded, on a broker without remote log storage.
Artifacts: `claude/artifacts/prod-05-1/live/{3.9,4.x}/the_portability_table_is_the_brokers.json`.

| key | class | semantic | sample | 3.9 | 4.x |
|---|---|---|---|---|---|
| `cleanup.policy` | portable | yes | `compact` | accepted | accepted |
| `compression.gzip.level` | portable | | `5` | accepted | accepted |
| `compression.lz4.level` | portable | | `9` | accepted | accepted |
| `compression.type` | portable | | `zstd` | accepted | accepted |
| `compression.zstd.level` | portable | | `3` | accepted | accepted |
| `delete.retention.ms` | portable | yes | `86400000` | accepted | accepted |
| `file.delete.delay.ms` | portable | | `60000` | accepted | accepted |
| `flush.messages` | portable | | `10000` | accepted | accepted |
| `flush.ms` | portable | | `1000` | accepted | accepted |
| `follower.replication.throttled.replicas` | clusterBound | | `*` | accepted | accepted |
| `index.interval.bytes` | portable | | `4096` | accepted | accepted |
| `leader.replication.throttled.replicas` | clusterBound | | `*` | accepted | accepted |
| `local.retention.bytes` | requiresTieredStorage | | `1000000` | accepted | accepted |
| `local.retention.ms` | requiresTieredStorage | | `60000` | accepted | accepted |
| `max.compaction.lag.ms` | portable | yes | `86400000` | accepted | accepted |
| `max.message.bytes` | portable | yes | `2097152` | accepted | accepted |
| `message.downconversion.enable` | removedInKafka4 | | `false` | accepted | refused (unknown) |
| `message.format.version` | removedInKafka4 | | `3.0-IV1` | accepted | refused (unknown) |
| `message.timestamp.after.max.ms` | portable | yes | `3600000` | accepted | accepted |
| `message.timestamp.before.max.ms` | portable | yes | `86400000` | accepted | accepted |
| `message.timestamp.difference.max.ms` | removedInKafka4 | yes | `86400000` | accepted | refused (unknown) |
| `message.timestamp.type` | portable | yes | `LogAppendTime` | accepted | accepted |
| `min.cleanable.dirty.ratio` | portable | | `0.3` | accepted | accepted |
| `min.compaction.lag.ms` | portable | yes | `1000` | accepted | accepted |
| `min.insync.replicas` | portable | yes | `2` | accepted | accepted |
| `preallocate` | portable | | `true` | accepted | accepted |
| `remote.log.copy.disable` | requiresTieredStorage | | `true` | accepted | accepted |
| `remote.log.delete.on.disable` | requiresTieredStorage | | `true` | accepted | accepted |
| `remote.storage.enable` | requiresTieredStorage | yes | `true` | refused (tiered storage disabled) | refused (tiered storage disabled) |
| `retention.bytes` | portable | yes | `1073741824` | accepted | accepted |
| `retention.ms` | portable | yes | `86400000` | accepted | accepted |
| `segment.bytes` | portable | | `104857600` | accepted | accepted |
| `segment.index.bytes` | portable | | `10485760` | accepted | accepted |
| `segment.jitter.ms` | portable | | `1000` | accepted | accepted |
| `segment.ms` | portable | | `3600000` | accepted | accepted |
| `unclean.leader.election.enable` | portable | yes | `true` | accepted | accepted |

Measured facts the table does not carry, and PROD-05.2 must:

- **The control**: `confluent.placement.constraints` (a provider's key) is refused on both lines
  ("Unknown topic config name"), and no `validate_only` probe left a topic behind.
- **No Apache Kafka topic key is sensitive** on either line (every DescribeConfigs entry reported
  `sensitive=false`). The `secret` class is defined from the broker's flag and proven on a scripted
  reader (`config_coverage::tests::the_model_records_overrides_semantic_defaults_and_secrets_by_key_only`).
- **An inherited default changed in 4.0**: `message.timestamp.after.max.ms` is
  `9223372036854775807` on 3.9 and `3600000` on 4.x. A 3.9 topic that inherits it, restored to a
  4.x target that inherits too, refuses records stamped more than an hour ahead of the broker's
  clock. Recorded as `inherited` with its value, so PROD-05.2 can name the difference.
- **4.x sets a cluster-wide dynamic default for `min.insync.replicas`** (source
  `dynamicDefaultBrokerConfig` on a fresh 4.3.1 topic, `defaultConfig` on 3.9.2).
- **`CreateTopics` accepts `min.insync.replicas` above the replication factor** on both lines (the
  topic then refuses `acks=all` writes). A valid create is not a writable topic.

## 3. The classes

`ConfigEntry::portability`'s closed set (arm 16), decided by `classify`, highest precedence first:

| class | when | applied by a restore (PROD-05.2) |
|---|---|---|
| `secret` | the broker flagged the entry sensitive | never; the value is not captured, only the key |
| `inherited` | any source but the topic's own override (`dynamicTopicConfig`) | never: the target's own broker decides; recorded so a difference can be explained (`retention.ms` inherited at 7 days on the source and 1 day on the target is a data-loss difference) |
| `portable` | an override of a key both lines define | yes, as an override |
| `removedInKafka4` | an override of a key Kafka 4.0 removed | to a 3.9 target only; never to 4.x, and its absence there is a named difference, never "the default" |
| `clusterBound` | an override naming the source cluster's replicas (the reassignment throttles) | never |
| `requiresTieredStorage` | an override that means something only where the target enables remote log storage | only after the target is checked for it (`remote.storage.enable=true` is refused without it) |
| `providerOnly` | an override of a key neither line defines | never |

**The capture rule** (`records`): every explicit override, and the effective value of each SEMANTIC
key whatever its source. A semantic key decides which records a restored topic keeps or accepts,
or how it stamps and replicates them: `cleanup.policy`, `delete.retention.ms`,
`max.compaction.lag.ms`, `max.message.bytes`, `message.timestamp.after.max.ms`,
`message.timestamp.before.max.ms`, `message.timestamp.difference.max.ms`,
`message.timestamp.type`, `min.compaction.lag.ms`, `min.insync.replicas`, `remote.storage.enable`,
`retention.bytes`, `retention.ms`, `unclean.leader.election.enable`. A topic with no override
records these fourteen (where its line defines them) and nothing `portable` — the live control of
§7, row 2.

**The class is the writer's, at write time.** The arms judge it against its source and value
(arm 17), never against the key: a later table refinement (a key a newer Kafka adds) must not make
an older receipt refuse itself. PROD-05.2 re-derives the class from its own table and the target's
line, and fails closed where the two disagree.

## 4. Declarative owners

| `owner.kind` | `owner.basis` | from |
|---|---|---|
| `strimzi` | `kafkaTopicResource` | a Strimzi `KafkaTopic` given to `logweir backup run --kafka-topic-resources <file>` (YAML as `kubectl get kafkatopics -A -o yaml` writes it) that is `kafka.strimzi.io/*`, carries a non-empty `strimzi.io/cluster` label (equal to `--strimzi-cluster` when given), is not annotated `strimzi.io/managed: "false"`, and names the topic (`spec.topicName`, else `metadata.name`). `reference` is `<namespace>/<name>`; two resources naming one topic (Strimzi's own conflict) give the reference that sorts first. |
| `strimzi` or `external` | `declared` | the plan's `source.topic_owners: [{topic, kind, reference}]`. Phase −1 refuses (exit 3) a declaration naming an unplanned topic, another kind, or a reference that is blank, longer than 256 characters or carries a control character. A declaration wins over a detected resource for the same topic: it is what the approved plan says. |

`reference` is copied into a signed document: never a credential. The product API publishes the
kind and an `applyRoute` of `desiredStateExport` for an owned topic, `adminApi` otherwise.

**Not detected, and why.** The controller does not list `KafkaTopic` resources for a `Backup` Job:
that needs read access to the third-party `kafka.strimzi.io` API group in the controller's
ClusterRole and a Strimzi install to prove it live (the PoC has none). Child row PROD-05.1a (§9).

## 5. The replication factor: an engine defect, measured

The first live run on 3.9 recorded `replication_factor` for ONE of five topics. The manifest the
engine wrote (`claude/artifacts/prod-05-1/live/3.9-run1/engine-manifest.json`) holds
`source_replication_factor: 1` for the first topic it saved and `null` for the other four. Engine
0.23.3 `backup/engine.rs:447-451` sets the field per topic before each `save_manifest`, and
`merge_manifests` (`:1683-1705`) carries `original_partition_count` from each later save into the
stored manifest but drops `source_replication_factor`. So the receipt reads the factor itself, from
the same reader and principal, before the engine (`ClusterReader::replication_factors`), and falls
back to the manifest's only where that read named none. The partition count stays the manifest's:
the merge keeps it, and it is the count the restore creates the topic with.

The defect also reaches phase 7, and there it reads as PARITY: `classify_parity` takes
`src_rf = t.source_replication_factor.unwrap_or(tgt_rf)`
(`crates/logweir/src/drill/phase7_verify.rs:1384`), so for every topic but the first of a
multi-topic set the source's factor is "the target's own" and no replication-factor divergence is
ever recorded (class sweep owed, §9). The upstream report is the owner's (the OD list keeps upstream
bug reports out of a worker's hands).

## 6. Versioning (OD-7) and compatibility

| document | new field | version |
|---|---|---|
| backup receipt | `topic_configuration` | 1.3.0 (`schemas/logweir-backup-receipt-1.3.0.json`; FX-7's 1.2.0 frozen) |
| catalog point record | `topics[].configuration`, and the existing `topics[].partitions` filled | 1.3.0 (`schemas/logweir-catalog-point-1.3.0.json`; 1.2.0 frozen) |
| catalog view entry, `PointView` | `topics[]`, `topicsOmitted` | the view grammar and the pre-release API (no format bump) |

Every receipt this build signs carries the block, so every one is 1.3.0, pinned or not; a 1.3.0
receipt without a pin has no `manifest_version_id` key. FX-7's "an unversioned receipt is FX-4's
1.1.0 byte for byte" holds for a build before this one.

**The arms** (both readers, byte-identical text, one corpus case each, the parity script):

| arm | refuses |
|---|---|
| 12 | `topic_configuration` under a minor before 3 |
| 13 | `topic_configuration` without `config_coverage` |
| 14 | a modelled set that is not `source.topics` |
| 15 | entries present where the read failed or was denied, or absent where it succeeded |
| 16 | a source or class outside the closed sets |
| 17 | a `secret` with a value, a non-secret without one, or `inherited` where the source is the topic's override (and the converse) |
| 18 | an owner outside the closed sets, a `kafkaTopicResource` owner that is not `strimzi`, or an unusable reference |
| 19 | a recorded partition count or replication factor of 0 |

All eight run only on a document carrying the new block, so no earlier receipt changes verdict.
They read `config_coverage` and `source.topics` as the context the block is judged in — FX-4's arm 7
reads `source.topics` the same way under the same ruling. That is OD-7 (a): **MINOR**. No existing
field's content or meaning changes; the catalog record's `topics[].partitions` is filled with the
value its 1.0.0 description already named (the partition count), and the only reader of it,
rehearsal selection's `maxPartitions` filter, can only refuse more points with it.

**Old readers** (measured): `verify_scorecard.py` 1.19.0 and a `logweir` built before this row
accept a 1.3.0 receipt and ignore the block (`crates/logweir/tests/receipt_dup.rs`'s Python row ran
over 1.3.0 receipts before the Python arms existed; the Rust reader ignores unknown fields inside
major 1). They print no model lines, so their exit 0 says nothing about configuration — as before.
**Rollback** writes 1.1.0/1.2.0 receipts again; 1.3.0 documents already written stay valid to both.

## 7. Live evidence

Compose slot 3, `--profiles acl`, once with `--kafka 3.9` and once with `--kafka 4.3`, `just
e2e-up`, `cargo test -p e2e --features e2e --test topic_configuration -- --ignored`. Artifacts:
`claude/artifacts/prod-05-1/live/{3.9,4.x}/`.

| row | 3.9.2 | 4.3.1 |
|---|---|---|
| `the_portability_table_is_the_brokers` | PASS: 36 keys, both directions; every verdict as recorded; the provider key refused | PASS: 33 keys; the three removed keys refused; the provider key refused |
| `a_backup_records_each_topics_model_and_its_owner` | PASS: compacted (`cleanup.policy=compact`, `min.compaction.lag.ms`, `min.insync.replicas=1` portable, `retention.ms` inherited, 3 partitions), delete-policy (`retention.ms`, `min.insync.replicas=2` portable), no-override control (only inherited), `message.format.version` and `message.timestamp.difference.max.ms` `removedInKafka4`, a Strimzi-labelled topic owned `kafka/strimzi-kt` while an unmanaged, an unlabelled and another cluster's `KafkaTopic` own nothing, a declared external owner; both readers exit 0 with the same five model lines; the catalog point copies the model | PASS: the broker refuses the removed keys at create (recorded) and the topic carries `message.timestamp.after.max.ms` instead; the rest as on 3.9 |
| `a_denied_describe_configs_records_no_entries_and_keeps_the_layout` | PASS: `captureDenied`, no entries, partitions 2 and factor 1 kept; the neighbour `notCaptured`/`manifestDiffers` with its entries; the super user's control records the denied topic's `cleanup.policy`; the SCRAM password in no output, receipt or record | PASS |

Live mutants on 4.3 (`claude/artifacts/prod-05-1/mutants-live/`): a removed key classed `portable`
fails row 1 (the key set), `remote.storage.enable` recorded as accepted fails row 1 (the verdict),
and an inherited value classed by its key makes the WRITER refuse to sign (exit 4, arm 17) in row 2.

## 8. The product API and the console (FX-5's hand-off)

- **The catalog's view** lists, for an `Available` point whose 1.3.0 record agreed with its verified
  receipt, every topic's recorded `partitions`, `replicationFactor`, `configCoverage` and owner kind
  (at most 64; more lists none and says how many in `topicsOmitted`). An entry keeps its list only
  while every later entry still fits slim in the sync body's 5 MB budget, so a topic list never
  costs a point its place in the view. The controller passes it through and the product API
  publishes it as `PointView.topics[]` with `applyRoute`.
- **The console** reads it: a catalog point carries its row; a `Backup`'s point is named by its
  receipt digest (`lwp1-` and the first 32 hex digits, D3 §5.1) and looked up in the namespace's
  recovery catalogs (at most four), before the first paint. The default is the largest selected
  topic's factor, capped by a fresh target broker count; the basis says
  ``capped at the target's 2 brokers; the source's is 3, as recovery catalog `primary` records point
  `lwp1-…`, the largest of the selected topics'``. A point no catalog lists, or lists without a
  layout, falls back to FX-5's broker-count default and says why. The review step shows the source
  partition counts the restore creates each topic with when the row records them.
- **The `Backup` status is not a second copy of the receipt** (`records_from_receipt`'s rule), so
  the factor is not copied into `Backup.status`; a namespace without a recovery catalog keeps
  FX-5's default and the note says so.

## 9. Limits and child rows

- **PROD-05.1a — the controller lists Strimzi `KafkaTopic` resources** (and accepts declared owners
  on `Backup`/`BackupSchedule`) and passes them into the runner plan. Needs a ClusterRole grant on
  `kafka.strimzi.io` (an owner call) and a Strimzi lab. Until then, owners reach a receipt through
  the CLI and the plan grammar only.
- **Class sweep owed — phase 7's replication-factor comparison**
  (`crates/logweir/src/drill/phase7_verify.rs:1384`, `src_rf = t.source_replication_factor
  .unwrap_or(tgt_rf)`) reads an absent manifest factor as the TARGET's, which is absent for every
  topic but the first of a multi-topic set (§5): the RF divergence, and FX-3's not-reconstructed RF
  in `newTopic` mode, are then silently "equal". Fix: take the bound receipt's
  `topic_configuration[t].replication_factor`, and record "not assessed" — never parity — when
  neither carries one. Outside this row's ownership (the restore's phase 7).
- **Rehearsal sizing does not read `topics[].partitions` yet**: `weirkeeper::rehearsal`'s
  candidates are built with `partitions: None` (`controllers/rehearsal_schedule.rs`). The catalog's
  view now carries the counts; wiring them is PROD-10's.
- **A provider cluster** (MSK, Confluent Cloud) is not measured; a provider's keys are
  `providerOnly` by construction and its sensitive entries `secret` by the broker's flag (OD-4).
- **The upstream engine defect** (§5) is the owner's to report.

## 10. Acceptance rows for PROD-05.2

Each with a pass predicate, a negative control and a fixture.

1. **Create from the model.** A `newTopic` restore bound to a 1.3.0 point creates each target topic
   with the model's `partitions`, a replication factor `min(model, target brokers)` with the change
   explained, and every `portable` override applied. *Control:* a 1.2.0 point (no model) creates
   exactly as today and says the model was not recorded. *Fixture:* row 2's compacted and
   delete-policy topics, 3.9 → 4.3.
2. **Never a default.** `inherited`, `clusterBound`, `providerOnly` and `secret` entries are never
   applied; each is listed in the target diff with its source value. *Control:* a topic with no
   override applies nothing and lists only inherited differences. *Fixture:* row 2's no-override
   topic.
3. **The line.** A `removedInKafka4` override is applied to a 3.9 target and refused, by name, for a
   4.x one — never silently dropped. *Control:* the same point to 3.9 applies it.
4. **Tiered storage.** A `requiresTieredStorage` override fails closed on a target whose
   `CreateTopics validate_only` refuses it (`remote.storage.enable=true` on the compose brokers).
5. **Owners.** An owned topic is not created through the admin API; reviewed `KafkaTopic` YAML (for
   `strimzi`) or a desired-state document (for `external`) is emitted instead. *Control:* the
   unowned neighbour is created.
6. **Coverage.** A topic whose model records no entries (`captureDenied`, `describeFailed`) is
   created with safe recovery settings and its configuration reported NOT RECORDED, never applied
   from the manifest's empty record. *Fixture:* row 3.
7. **The 4.0 default change.** A restore of an `inherited` `message.timestamp.after.max.ms` from a
   3.9 source to a 4.x target names the difference before Create.
8. **Re-derived classes.** PROD-05.2's own table decides the class against the target line; a
   recorded class it disagrees with fails closed, naming both.

---

Documentation is licensed [CC-BY-4.0](../../LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
