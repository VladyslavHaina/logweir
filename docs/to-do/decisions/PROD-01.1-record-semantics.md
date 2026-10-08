# PROD-01.1 — Record and transaction semantics: the capability contract

Decision record for **PROD-01.1** ("Prove record and transaction behaviour") in the
[product-expansion tracker](../product-expansion.md#prod-011--prove-record-and-transaction-behavior).

- Date: 2026-09-28. Base: main `adee0a16`. Branch `claude/prod-01-1`.
- Kind: research row (source first, then measured on the compose stack). It ships no product
  feature; the rails it decides are proposed ledger rows (§6, §9).
- **Addendum, 2026-10-07 (PROD-00.3f):** the pin moved to `kafka-backup` **0.23.3** and every live
  row here was re-run on it with the contract asserted (`CONTRACT_ENGINE`). **No contract change:**
  §11 records the runs. The rest of this record is as measured on 0.21.0.
- Engine: `kafka-backup` v0.21.0, the pinned source `third_party/kafka-backup-v0.21.0.tar.gz`
  (sha256 `0252a83735148331c16d7c4e737a41f099c0f52eda5d7a66db75b8848ddc405b`) and the pinned image
  `osodevops/kafka-backup@sha256:8ff5be71f92a118cde64c082a86d188a4187d8f8f64311458081b8727e99c317`.
  Engine paths below are inside that tarball, under `crates/kafka-backup-core/src/`.
- Protocol codec: the engine's `kafka-protocol` 0.18.0 (crates.io, sha256
  `099d5c2f1b40cd830cbf18ca4d2a0805f2875b811ca18f0372b2433e68fe2dda`, equal to the engine's
  `Cargo.lock:1880`), fetched for reading only. Its paths are cited as `kafka-protocol …`.
- Broker: Apache Kafka 3.7.1 (KRaft, one combined node), the `e2e/compose` stack.
- Oracle: `e2e/tests/record_semantics.rs` (live rows), its support module
  `e2e/tests/record_semantics_support/`, and the comparator's negative controls
  `e2e/tests/record_semantics_oracle.rs` (default test set, no broker).
- Parallel rows: PROD-00.1 owns the engine ROUTE of every capability named here (its capability
  table: control records and READ_COMMITTED, idempotent produce, min/max segment timestamps);
  PROD-01.4 owns topic identity and generations (§2.6 records only what recreation does to
  records); PROD-01.5 owns the compose profiles; FX-6 owns the user-document disclosure (§8 gives
  its evidence and wording).

## 0. Decision summary

1. **Capture keeps the log as a `READ_UNCOMMITTED` fetch returns it.** Aborted records, records of
   transactions open at capture, and every commit and abort marker are archived as ordinary
   records; a `LogAppendTime` source is archived with the producers' CreateTime; a repeated header
   key keeps one copy. Keys, values, null versus empty, tombstones, header order and a compacted
   log are kept exactly. Every source prediction a row exercised was confirmed (S1–S11, S13 and
   S12's capture-side half); S12's header-order and duplicate-collapse halves and S14 rest on
   source reading only. (Contract §3.1.)
2. **Replay reproduces the archive record for record, in order, as committed `CreateTime` data** —
   except where selection by segment first/last timestamps drops or skips a record (§2.2), where a
   record that already carried `x-original-offset` loses it (§2.4), where a lost produce
   acknowledgement resends a batch (§5.1: 3,000 duplicates after three resent requests, 2,000 after
   two, the engine exiting 0 over the second), and where a broker outage ends the restore early with
   a partial output left behind (§5.1). (Contract §3.2.)
3. **Verification compares the target with the archive, never with the source.** Transactions and
   `LogAppendTime` pass (19/19 and 9/9 matching); a skipped in-window record passes (16/16); a
   correct point-in-time restore fails (11/11 matching, count bound missed); a record below the
   floor fails a full restore and passes a point-in-time one (10/10 matching). (Contract §3.3.)
4. **Product claims.** Nothing may claim transactional, exactly-once, `read_committed` or
   source-faithful recovery. Each limit in §3 is a sentence FX-6 publishes (§8) and an acceptance
   row a dependent task must meet (§7).
5. **Rail for transactional topics: refuse by default, detect at capture, label an approved
   override (PROD-01.1a); committed-only capture through PROD-00.3a.** (§6.1.)
6. **Detection without DescribeProducers:** control-shaped records in the archive, confirmed by the
   offset gap; the last-stable-offset gap at the pre-backup probe; and a committed-versus-archive
   reconciliation of the tail after the engine, which alone sees a transaction opened after the
   probe (§6.2, measured).
7. **Fault injection around acknowledgements was run** (§5.1: duplicates and a partial output, never
   a signed pass); termination-based injection stays blocked on subprocess cancellation, because a
   killed `logweir` leaves its engine running to completion (§5.2).
8. **Rows:** PROD-01.1a, PROD-01.1b, the fix-now row FX-8 (point-in-time over `LogAppendTime`
   sources) and PROD-00.3a–e (§9); acceptance rows 02-1…08-7 (§7), with the PROD-07 rows naming the
   deterministic fixtures they need.

## 1. Source findings, read before any run

Each finding predicted an outcome before any run; the last column names where it was measured.

| # | Finding | Evidence | Predicted outcome | Confirmed by |
|---|---|---|---|---|
| S1 | Capture fetches `READ_UNCOMMITTED` and keeps every decoded record, control records included. | `kafka/fetch.rs:53` (`with_isolation_level(0)`), `:180-184` (every record at or above the fetch offset is kept); `kafka-protocol … records.rs:621-660` decodes a control batch like any other. | Aborted records, open-transaction records and commit/abort markers are archived. | §2.1 (capture: 7 markers; 5 aborted or open records archived) |
| S2 | An archived record has no transaction or producer fields and no timestamp type. | `manifest.rs:405-426` (`BackupRecord`: key, value, headers, timestamp, offset); `kafka/fetch.rs:200-219` drops `transactional`, `control`, `producer_id`, `producer_epoch`, `sequence`, `timestamp_type` that `kafka-protocol … records.rs:559-576` decoded; segment record layout `segment/format.rs:27-42`. | Nothing in the archive can tell a marker or an aborted record from data. | §2.1, §2.3 |
| S3 | A record's timestamp is the batch's first timestamp plus its delta; the batch's max timestamp is read and discarded. | `kafka-protocol … records.rs:582, 585, 862`. Apache Kafka's broker assigns `LogAppendTime` by rewriting only the batch's max timestamp and attribute bit. | A `LogAppendTime` source is archived with the producer's CreateTime. | §2.3 |
| S4 | Headers are held in an `IndexMap` at decode and at encode. | `kafka-protocol … records.rs:896-919` (`IndexMap::insert` per header); `kafka/produce.rs:84-90` (`collect` into `IndexMap<StrBytes, …>`). | A repeated header key collapses to one entry: first position, last value — at capture and again at replay. | §2.4 (p0@4 at capture, p0@6 at replay) |
| S5 | The backup appends `x-original-offset` and `x-original-timestamp` after the record's own headers. | `backup/engine.rs:1836-1858`; Logweir renders `include_offset_headers: true` (`crates/logweir-engine-oso/src/render_backup.rs:260`) and `strip_offset_headers: false` (`render_restore.rs:180`). | Every restored record carries two extra headers; a record that already carried `x-original-offset` is archived with two, collapsed to the engine's at replay. | §2.4 (p0@6); every row (two appended headers) |
| S6 | A segment's time bounds are its first and last records' timestamps. | `segment/writer.rs:236-242`; `manifest.rs:359-363`. | Non-monotonic timestamps inside a segment misstate its time range. | §2.2 |
| S7 | Replay selects segments by those bounds, then filters each record by its own timestamp, inclusive at both ends. | `restore/engine.rs:1684, 1957-1964` (`overlaps_time_window`, `manifest.rs:391-401`); `restore/helpers.rs:67-85`. | A segment whose first record is after the point is skipped whole. | §2.2 (ts-pit) |
| S8 | Logweir's window floor is the minimum of the segments' FIRST record timestamps. | `crates/logweir-core/src/engine.rs:130-141`, bound by `crates/logweir/src/drill/mod.rs:3229-3280` (guard G-WIN). | A record older than every segment's first record is dropped from every restore. | §2.2 (ts-floor) |
| S9 | Logweir's count bound treats first/last as if they were min/max. | `crates/logweir-core/src/engine.rs:193-214`; `crates/logweir/src/drill/phase7_verify.rs:1402-1416`. | A correct point-in-time restore can fail; a skipped segment is excluded from both sides and passes. | §2.2 (ts-bound false failure; ts-pit pass) |
| S10 | Replay produces non-transactionally and non-idempotently, `CreateTime`, sequentially per partition. | `kafka/produce.rs:92-104`; `restore/engine.rs:1814-1882`. | Order is kept per partition; everything restored is visible to every consumer. | every row (order); §2.1 (visibility) |
| S11 | A produce is resent on a connection error, and a client-side timeout is one. | `kafka/partition_router.rs:500-551` (5 connection and 20 leader retries); `kafka/connection_error.rs:88-100` ("timed out"); `kafka/client.rs:22-28` (10 s write, 60 s response). | A lost acknowledgement can duplicate a batch. | §5.1 samples 3 and 5 |
| S12 | Phase 7 compares sampled archive fingerprints with the target, keyed by the target's first `x-original-offset`. | `crates/logweir/src/drill/phase7_verify.rs:276-313`; `crates/logweir-kafka/src/fingerprint.rs:9-39` hashes key, value, the SORTED header list and the timestamp. | Capture-side losses are invisible; header order is never verified; target duplicates collapse in the map. | capture-side half: §2.1, §2.3, §2.4; header order and duplicate collapse: source only |
| S13 | Logweir creates the target topic itself, with `message.timestamp.type=CreateTime` and `retention.ms=-1` only. | `crates/logweir-kafka/src/reader.rs:353-356`; `render_restore.rs` renders `create_topics: false`. | A compacted source is restored into a delete-policy topic. | §2.5 |
| S14 | The segment format stores a header count and a header-key length as `u16`, cast with `as`. | `segment/format.rs:204-206`. | Not run: a record with more than 65,535 headers, or a header key longer than 65,535 bytes, is written with a truncated length. | not run |

The upstream v0.21.0 → v0.22.0 diff (PROD-00.1's artifact `upstream-v0.21.0..v0.22.0.diff`) keeps
`kafka-protocol` 0.18.0 and does not touch `kafka/fetch.rs`, `kafka/produce.rs`,
`kafka/partition_router.rs`, `segment/` or `restore/`; its one manifest change adds
`missing_topics`. So S1–S14 hold at v0.22.0 too, by source reading (not run).

## 2. Observed outcomes

Every row: a deterministic fixture on three partitions, `logweir backup run` with the pinned engine
(through `e2e/fixtures/engine-docker.sh` on this arm64 host), `logweir restore run`
(`target.mode: newTopic`), and three comparisons through the oracle — CAPTURE (raw source log
against the archive, decoded by Logweir's own `kbak` decoder), REPLAY (the archive records the
requested window selects, against the output), and END TO END (the committed input the window
selects, read `read_committed`, against the output). "Exact" means the oracle reported nothing:
same records, same order, same key, value, timestamp and headers, with only the two appended lineage
headers allowed. Logweir's own signed verdict for the same restore is the last column; it is
recorded, never used as the outcome.

Two full passes — the first at `7137e3ea` (2026-09-29, 04:00–04:09Z) and the last at `d5d3076b`
(04:24–04:34Z) — gave identical results in every row, Logweir's verdicts included. Each row writes
`.e2e/record-semantics/<row>.json` with every divergence listed; the orchestrator's copies are under
`/tmp/logweir-roadmap-run/claude/artifacts/prod-01-1/run1-7137e3ea/` and `…/final-d5d3076b/`.

| Row (function) | Fixture | Capture | Replay | End to end | Logweir verdict |
|---|---|---|---|---|---|
| TXN `transactional_topic_committed_input_versus_restored_output` | 4 transactions on 3 partitions: A, C committed; B aborted; D open during the backup, aborted after it | 7 `extra:control-marker` | exact | 5 `extra:uncommitted` + 7 `extra:control-marker`; the 7 committed records exact | exit 0 **`pass`** (19/19) |
| ts-floor `non_monotonic_create_time_below_the_window_floor` | 12 records; p0 `[+2000, +1000, +3000, +4000]`; full restore | exact | 1 `missing` (p0@1) | 1 `missing` (p0@1) | exit 2 `fail-integrity` (count 11 outside [12, 12]; 1 sample mismatch) |
| ts-pit `non_monotonic_create_time_skipped_at_the_point_in_time` | 20 records; p0 `[… \| +9000, +2500, +9100, +9200]`; point `+5000` | exact | 1 `missing` (p0@5) | 1 `missing` (p0@5) | exit 0 **`pass`** (16/16) |
| ts-bound `non_monotonic_create_time_inside_a_wholly_inside_segment` | 16 records; p0 `[+2000, +6000, +2400, +2600]`; point `+5000` | exact | exact | exact | exit 2 `fail-integrity` (count 11 outside [12, 12]; p0 short sample) — **false** |
| LAT full `log_append_time_source_versus_restored_output` | 9 records, `LogAppendTime`, CreateTime 2001 | 9 `timestamp-changed` | exact | 9 `timestamp-changed` + 9 `timestamp-type-changed` | exit 0 **`pass`** (9/9) |
| LAT point in time (same row) | point `C0+1500` (2001) | — | exact | 6 `extra:outside-model` | exit 0 **`pass`** (6/6) |
| shapes `keys_nulls_tombstones_and_duplicate_headers` | 15 records: nulls, empties, tombstone, repeated and null header values, own `x-original-offset`, binary, order, equal timestamps | 1 `headers-collapsed` (p0@4) | 1 `headers-collapsed` (p0@6) | 2 `headers-collapsed` | exit 2 `fail-integrity` (p0@6 only; 14/15) |
| compaction `compacted_topic_committed_input_versus_restored_output` | 12 records after the cleaner removed 9 superseded ones; 3 tombstones | exact | exact | exact | exit 0 `pass` (12/12) |
| recreation `recreated_topic_between_two_backups` | 4 records per partition, topic deleted and recreated, 2 per partition; one archive per generation | exact, both | exact, both | exact, both; offsets 0–1 reused for different records; a third run under generation 1's `backup_id` refused, exit 1 | exit 0 `pass`, both (12/12, 6/6) |

### 2.1 Transactions

`transactional_topic_committed_input_versus_restored_output`: one transactional producer
(`transactional.id` per run, librdkafka, send-time CreateTime) wrote four transactions over three
partitions: A committed (p0 ×2, p1, p2 ×2), B aborted (p0, p1 ×2), C committed (p1, p2), and D
(p0, p2) flushed and left OPEN while `logweir backup run` captured the topic, then aborted after
it.

| View | p0 | p1 | p2 | Records |
|---|---|---|---|---|
| Committed input (`read_committed`) | a0, a1 | a2, c0 | a3, a4, c1 | 7 |
| Raw log (`read_uncommitted`) | a0, a1, b0, d0 | a2, b1, b2, c0 | a3, a4, c1, d1 | 12 |
| Archive and output (source offsets) | a0 0, a1 1, **A 2**, b0 3, **B 4**, d0 5 | a2 0, **A 1**, b1 2, b2 3, **B 4**, c0 5, **C 6** | a3 0, a4 1, **A 2**, c1 3, **C 4**, d1 5 | 19 |

Bold entries are transaction markers (A, C commit; B abort) archived and restored as records with
key `00 00 00 01` or `00 00 00 00` and a 6-byte value, and the broker's timestamp.

- Capture: seven `extra:control-marker` (every marker below the captured high watermark, commit and
  abort types as expected); the aborted (b0, b1, b2) and open (d0, d1) records were archived like
  committed ones. D's abort marker, written after the backup at or above the captured high watermark
  (6 on p0 and p2), is not in the archive, so nothing in it says D never committed.
- Replay: exact — the output is the archive, record for record, in source-offset order.
- End to end: five `extra:uncommitted` (b0, d0, b1, b2, d1) and seven `extra:control-marker`; all
  seven committed records present, unchanged and in order. Every one of the nineteen output records
  is visible to a `read_committed` consumer, because the restore produced them non-transactionally.
- Logweir: exit 0, `pass`, 19 of 19 sampled records matching. FX-6's first hazard, measured.
- **Open-transaction probe** (taken while D was open, before the backup): a `read_committed`
  watermark query answered 5, 7, 5 and a `read_uncommitted` one 6, 7, 6 — the last stable offset
  is below the high watermark exactly on p0 and p2, where D was open. A `read_committed` consumer
  stopped at the same offsets (its position at end-of-partition: 5, 7, 5). Both are safe
  rust-rdkafka 0.36 calls; §6.2 uses them as the detection route for open transactions.
- **A transaction opened after the probe** (review M3; fix-round pass at `44838f31`): the same
  backup also captured a second topic, `txn-late`, holding one committed record per partition and a
  transaction E (two records on p1) that opened only after every probe and aborted after the backup.
  Its probe showed no gap (`read_committed` 1, 1, 1, `read_uncommitted` 1, 1, 1), its archive holds
  no marker, and its restore carried E's records as data: end to end 2 `extra:uncommitted`, and
  Logweir exit 0, `pass`, 5 of 5 sampled records matching. Only §6.2's third signal sees it: p1@1,
  p1@2, E's records; on the main topic the same signal returned p0@5, p2@5, D's.

### 2.2 Non-monotonic and equal CreateTime within one segment

Three rows, each with `segment_max_records: 4` so the segment layout is the fixture's (`T` is
`2025-10-09T08:53:20Z`; `+n` is milliseconds after it; `|` separates segments).

**Below the window floor** — `non_monotonic_create_time_below_the_window_floor`.
p0 `[+2000, +1000, +3000, +4000]`, p1 `[+2000, +2000, +2000, +2500]`, p2 `[+2100 … +2400]`; a
full restore (window end `+10000`), and the same archive at point `+3500` with the sample window
starting at the floor (review M1).

- The manifest records p0's segment as `+2000 … +4000` (first and last); its minimum is `+1000`.
  The rendered window floor is `+2000`, the minimum FIRST-record timestamp (S8).
- Capture exact. Replay and end to end: `missing` p0@1 — the `+1000` record is archived and never
  restored, by this or any restore of the archive, because every window starts at the floor.
- Equal timestamps on p1 were restored exactly.
- Logweir, full restore: exit 2, `fail-integrity` — "restored 11 records but the manifest bounds
  the window [1760000002000, 1760000010000] at [12, 12]", and 1 of p0's 4 sampled records did not
  reconcile. Detected, but the archive cannot be restored whole through Logweir.
- Point in time `+3500`: replay and end to end `missing` p0@1 again (the record is at or before the
  point and below the floor). p0's segment straddles the point, so the count bound counts it only
  as an upper bound (`crates/logweir-core/src/engine.rs:205-210`), and the record lies below the
  sample window, which filters archive fingerprints per record
  (`crates/logweir-engine-oso/src/engine.rs:511-513`). Logweir: **`pass`** (exit 0; 10 records
  restored, 10 of 10 sampled records matching, no count-bound finding). **The loss is signed as a
  pass** at a point in time, as the source predicted (fix-round pass at `44838f31`).

**A segment skipped at the point in time** — `non_monotonic_create_time_skipped_at_the_point_in_time`.
p0 `[+2000 … +2300 | +9000, +2500, +9100, +9200]`, p1 `[+2000, +5000, +5000, +5000]`,
p2 `[+2000 … +2300 | +2400, +4000, +4500, +5000]`; recovery point `+5000`.

- The manifest records p0's second segment as `+9000 … +9200`; it holds `+2500`.
- Capture exact. Replay and end to end: `missing` p0@5 — the `+2500` record is at or before the
  point and was not restored, because its segment's FIRST record is after the point (S7).
- The three records AT the point on p1 (equal timestamps, `+5000`) were restored: the inclusive end
  holds for equal timestamps.
- Logweir: exit 0, `pass`, 16 of 16 sampled records matching, the count inside its bound — the
  skipped segment is outside the window on both sides of every check. **The omission is signed as a
  pass**, which is FX-6's second hazard, measured.

**A wholly-inside segment that crosses the point** — `non_monotonic_create_time_inside_a_wholly_inside_segment`.
p0 `[+2000, +6000, +2400, +2600]`, p1 `[+2000 … +2300 | +9000 … +9300]`, p2 `[+2000 … +2300]`;
recovery point `+5000`.

- The manifest records p0's segment as `+2000 … +2600`; its maximum is `+6000`.
- Capture, replay and end to end all exact: the eleven records at or before the point were
  restored and `+6000` was not. The restore is right.
- Logweir: exit 2, `fail-integrity`, with 11 of 11 sampled records matching and no mismatch. Two
  checks fail on the segment's first/last bounds: the count bound ("restored 11 records but the
  manifest bounds the window [1760000002000, 1760000005000] at [12, 12]"), and p0's selection
  ("the archive returned 3 fingerprints where the manifest supports at least 4"). A correct
  point-in-time restore is signed as failed.

### 2.3 A `LogAppendTime` source

`log_append_time_source_versus_restored_output`: a topic with `message.timestamp.type=LogAppendTime`,
three records per partition produced with explicit CreateTime `C0`, `C0+1000`, `C0+2000`
(`C0` = `2001-09-09T01:46:40Z`).

- The source reported broker append times (`LogAppendTime`, epoch-ms 1790654620497–1790654620505,
  the run's own instant) for all nine records.
- Capture: `timestamp-changed` on all nine — the archive holds `C0`, `C0+1000`, `C0+2000`, the
  producers' CreateTime (S3). No record says it was `LogAppendTime`; the manifest's topic entry
  does, as `configurations: {"message.timestamp.type": "LogAppendTime"}`, because the topic set it as
  an override (a broker-wide default would not appear there: `manifest.rs:143-146` keeps explicit
  overrides only).
- Full restore: replay exact; end to end `timestamp-changed` ×9 and `timestamp-type-changed` ×9 —
  the output reports `CreateTime` in 2001 for records the source reported as appended in 2026.
  Logweir: exit 0, `pass`, 9 of 9 matching.
- Point-in-time restore at `C0+1500`: six records restored (the `C0` and `C0+1000` record of each
  partition), selected by the producers' clock. By the source's own clock none of them existed at
  that point, so end to end reports six `extra:outside-model`. Logweir: exit 0, `pass`, 6 of 6.

### 2.4 Keys, nulls, tombstones and headers

`keys_nulls_tombstones_and_duplicate_headers`: 15 records on three partitions — a plain record,
a null key, a null value (a tombstone on a delete-policy topic), an empty key and empty value,
repeated header keys, null and empty header values, a record that already carries its own
`x-original-offset` (as a previously restored topic's records do), binary key and value bytes,
four values of one key in order, and three records with the same timestamp.

| Record | Source headers | Archive headers | Output headers |
|---|---|---|---|
| p0@4 | `h=a, x=1, h=b, h=c` | `h=c, x=1`, then the two lineage headers | same as the archive |
| p0@6 | `x-original-offset=777` | `x-original-offset=777, x-original-offset=6, x-original-timestamp` | `x-original-offset=6, x-original-timestamp` |

- Capture: `headers-collapsed` at p0@4 only — the repeated key kept its FIRST position and its LAST
  value, exactly `indexmap_collapse` (S4). Replay: `headers-collapsed` at p0@6 only — the archive
  held both `x-original-offset` headers, and the produce side collapsed them to the engine's,
  losing the source's value. End to end: those two records.
- Everything else was exact: null versus empty key, value and header value, the tombstone, binary
  bytes, one key's four values in order, equal timestamps, and every partition's order.
- Logweir: exit 2, `fail-integrity`, 14 of 15 sampled records matching — the mismatch is p0@6,
  whose archive fingerprint carries two `x-original-offset` headers and whose output carries one.
  p0@4 matched: its loss happened at capture, so archive and output agree. A topic whose records
  already carry `x-original-offset` therefore fails a restore drill whenever such a record is sampled.

### 2.5 Compaction

`compacted_topic_committed_input_versus_restored_output`. Each partition was written
`k1=v1, k2=v1, k1=v2, k3=v1, k2=null, k1=v3` (offsets 0–5, CreateTime `T+0…T+50`) and a
roller one hour later rolled the active segment; the row waited until the log cleaner had
removed offsets 0–2 on every partition, then took the backup and read the source again to prove it
had not changed during the run.

- Source (`read_committed`) = raw = archive = output: 12 records, source offsets 3, 4, 5, 6 on
  each partition. Capture, replay and end to end: no divergence.
- The tombstone (`k2`, null value) was restored on every partition as a null value.
- The output topic is `cleanup.policy=delete`, `retention.ms=-1`, `message.timestamp.type=CreateTime`
  (read back with DescribeConfigs): compaction is not reconstructed, and with infinite retention the
  tombstones never expire.
- Logweir: exit 0, `pass`, 12 of 12 sampled records matching.

### 2.6 Topic recreation between runs

`recreated_topic_between_two_backups` (identity and generation decisions are PROD-01.4's; this
records only what happened to records). Generation 1: four records per partition; backup B1. The
topic was deleted, recreated under the same name and partition count, and given two records per
partition (generation 2); backup B2.

- Each archive restored its own generation exactly (capture, replay and end to end empty; Logweir
  `pass` both times, 12 and 6 records).
- The two outputs carry `x-original-offset` 0 and 1 on every partition for DIFFERENT records
  (`g1-…` and `g2-…` payloads): six `(partition, source offset)` pairs name two records each.
- Neither manifest has a topic-identity field (the row scans both; review L7b): their keys are `backup_id`, `created_at`,
  `source_brokers`, `source_cluster_id`, `topics` → `name`, `original_partition_count`,
  `source_replication_factor`, `configurations`, `partitions` → `partition_id`, `segments` →
  offsets, first/last timestamps, counts, sizes, `sha256`, `uploaded_at`.
- A third `logweir backup run` under B1's `backup_id` over the recreated topic was refused — exit 1
  naming `ExecutionAlreadyClaimed` (RECEIPT-DUP, `docs/stability.md`) — and B1's manifest bytes were
  unchanged, so the engine's in-place manifest rewrite (FX-7) cannot mix the generations through
  Logweir.

## 3. The capability contract

Three tables, one per stage. **Guarantee** is what holds for engine 0.21.0 as Logweir drives it;
**counterexample** is the measured case where the stronger property people assume does not hold;
**isolation** is the isolation level or delivery setting in force at that stage; **archive
prerequisite** is what an archive must carry for the guarantee to be checkable. A row whose
counterexample is non-empty is a limit that product surfaces must not contradict.

### 3.1 Capture (source log → archive)

| # | Guarantee | Known counterexample | Isolation | Archive prerequisite |
|---|---|---|---|---|
| C1 | Every record a `READ_UNCOMMITTED` fetch returns between the log start and the high watermark read at capture is archived once, in offset order, with its source offset. | Records deleted by retention before the fetch are a recorded `gaps` range, not data. Records arriving during the run may be archived past the captured high watermark (the fetch loop ignores its end bound, `backup/engine.rs:1520`). | `READ_UNCOMMITTED`, fixed by the engine (`kafka/fetch.rs:53`) | segment `start_offset`/`end_offset`/`record_count` |
| C2 | — (no committed-only capture) | Records of aborted transactions and of transactions open at capture are archived (TXN). | same | — |
| C3 | — | Commit and abort markers are archived as ordinary records, one per transaction per partition, with the broker's timestamp (TXN). | same | — |
| C4 | Key and value bytes, null versus empty keys and values, tombstones. | none observed | same | — |
| C5 | Header order, and null versus empty header values. | A repeated header key collapses to one entry (first position, last value) before the archive is written (SHAPES p0@4). | same | — |
| C6 | `CreateTime` timestamps. | A `LogAppendTime` source is archived with the producer's CreateTime; no record carries a timestamp type. Only a topic-level `message.timestamp.type` override survives, in the manifest's `configurations` (LAT; a broker-wide default does not, and a denied DescribeConfigs looks like no override — FX-4). | same | manifest `configurations` for the type, when present |
| C7 | A compacted source is archived as its log stood at the fetch: sparse source offsets, tombstones it still held. | none observed | same | — |
| C8 | — | Nothing identifies the topic generation: after a delete and recreate, offsets 0 and 1 name different records in two archives of the same topic name (RECREATE; PROD-01.4). | — | — |

### 3.2 Replay (archive → restored topic)

| # | Guarantee | Known counterexample | Isolation / delivery | Archive prerequisite |
|---|---|---|---|---|
| R1 | Records of a partition are produced in archive order, one batch at a time; the restored partition holds them in source-offset order (every row). | A lost produce acknowledgement resends the batch: 3,000 duplicates after three resent 1,000-record requests in §5.1 sample 3, and 2,000 after two in sample 5, where the engine still exited 0. | non-transactional, non-idempotent, `acks=all` (engine default), 30 s broker timeout, 60 s client response timeout | — |
| R2 | A full restore returns every archived record whose timestamp is at or above the archive floor. | The floor is the minimum FIRST-record timestamp, so a record older than every segment's first record is dropped from every restore, full and point-in-time (ts-floor, both measured). | — | segment `start_timestamp` (first record) |
| R3 | A point-in-time restore returns archived records whose own timestamp is at or before the point, inclusive, from the segments whose first/last range overlaps the window. | A segment whose first record is after the point is skipped although it holds an in-window record (ts-pit). | — | segment first/last timestamps |
| R4 | Key and value bytes, null versus empty, tombstones; the archived timestamp, produced as `CreateTime`; the archived headers. | The archived headers are collapsed again: a record archived with two `x-original-offset` headers keeps only the engine's (SHAPES p0@6). | — | — |
| R5 | Every restored record is visible to every consumer, `read_committed` included. | This is why C2 and C3 reach applications: aborted records and markers are committed data in the target (TXN). | non-transactional produce | — |
| R6 | The target is created by Logweir with `CreateTime`, `retention.ms=-1`, the source's partition count and the plan's replication factor. | Compaction is not reconstructed: a compacted source restores into a delete-policy topic (COMPACT; FX-3, PROD-05). | — | — |
| R7 | — | A restore cannot be stopped: after `logweir` was killed the engine kept running and completed the restore, unrecorded (§5.2; Later #13). | — | — |
| R8 | A restore that fails exits 1 and Logweir writes no scorecard. | A broker outage past its session timeout made the engine abort the topic ("Unknown broker ID: -1") and left a partial output with duplicates behind, undeleted (§5.1 sample 3; PROD-07.2); and Logweir's own 20 s client timeout failed a restore whose every record had landed (§5.1 sample 4; Later #14), as did a leaderless moment after a thaw (sample 5). | — | — |

### 3.3 Verification (Logweir's drill and restore verdict)

| # | Guarantee | Known counterexample | Isolation | Archive prerequisite |
|---|---|---|---|---|
| V1 | Sampled archive records and the restored records they map to (by `x-original-offset`) have equal fingerprints: key, value, the header multiset, timestamp. | Header ORDER is not compared (the fingerprint sorts headers). | target read `read_committed` (librdkafka default) | `x-original-offset` on every archived record (`include_offset_headers: true`) |
| V2 | The restored count lies inside the manifest's bound for the window. | The bound treats first/last as min/max: a correct point-in-time restore can fail (ts-bound); a skipped segment is outside both sides and passes (ts-pit); a below-floor loss inside a segment that straddles the point passes too (ts-floor at `+3500`). | same | segment first/last timestamps and `record_count` |
| V3 | — | A capture-side divergence is in the archive AND the target, so it is invisible: transactions, `LogAppendTime`, collapsed headers pass (TXN, LAT, SHAPES p0@4). | — | — |
| V4 | — | Duplicate target records collapse in the offset-keyed map (`phase7_verify.rs:285-296`); only the count bound can see them. No sample in §5.1 reached verification: each ended in exit 1 without a scorecard. | — | — |

## 4. What the archive cannot represent

A `.kbak` segment stores, per record, a timestamp, the source offset, a key, a value and an
ordered header list (`segment/format.rs:27-42`); the manifest stores, per segment, offsets, the
first and last record timestamps, a count, sizes and a digest (`manifest.rs:349-387`). Everything
else is gone once the archive is written, and no later stage can restore it:

| Not representable | Consequence |
|---|---|
| Transaction state: producer id, epoch and sequence; the batch's transactional and control flags; aborted-transaction ranges | A control record is data with a recognisable shape; an aborted record is indistinguishable from a committed one. Committed-only replay needs a different CAPTURE (PROD-00.3a), not a smarter restore. |
| Each record's timestamp type, and the broker's `LogAppendTime` | The archive's timestamp is the producer's; a `LogAppendTime` source's own timeline cannot be rebuilt from it. A topic-level `message.timestamp.type` override is kept in the manifest's `configurations` (§2.3), so a reader can at least tell which archives came from such a topic. |
| A header key that occurs more than once | Collapsed before the segment is written (the segment format itself could hold duplicates; the decoder in front of it cannot). |
| Per-segment minimum and maximum record timestamps | Only first and last are kept, so time selection over an out-of-order segment is a guess (PROD-00.3b). |
| The high watermark and last stable offset at capture | Not in the manifest; the open-transaction reading of §6.2 must be recorded by Logweir, beside the receipt. |
| Topic identity or generation | A recreated topic's records are indistinguishable from the earlier generation's at the same offsets (PROD-01.4). |
| Batch boundaries, compression, leader epochs | Not needed for replay; a restore re-batches. |
| More than 65,535 headers on one record, or a header key longer than 65,535 bytes | Written with a truncated `u16` length (`segment/format.rs:204-206`); source reading only, not run. |

## 5. Fault injection around acknowledgements

### 5.1 A lost produce acknowledgement — run

`a_lost_produce_acknowledgement_during_restore` (`#[ignore]`d: it stops the shared broker). A
60,000-record archive (three partitions) is restored; when the first restored records land, the
broker container is frozen (`docker compose pause`) for 75 s — longer than the engine's 60 s
response timeout — and thawed. Samples (outcomes under
`artifacts/prod-01-1/run1-7137e3ea/ack-fault-run*.json`, `final-*/ack-fault.json` and
`fix-44838f31/ack-fault.json`):

| Sample | Frozen at | Engine | Logweir | Output against the committed input |
|---|---|---|---|---|
| 1 | 55,000 / 60,000 | not kept | exit 1, no scorecard | 60,000 records, exact |
| 2 | 38,000 / 60,000 | not kept | not kept (the row's read met a leaderless partition and the row panicked before saving; fixed) | not read |
| 3 | 5,000 / 60,000 | three produce requests "timed out after 60s waiting for broker response" and were resent; after the thaw, NOT_LEADER on all three partitions, then "Unknown broker ID: -1" and "Restore completed with 1 error(s)", exit 1 | exit 1, "logweir could not do its job; NO scorecard was written" | 44,000 records: **3,000 `duplicate`** and 19,000 `missing` |
| 4 (final pass, at the tip) | 57,000 / 60,000 | logged no warning; it had returned before Logweir's next read | exit 1 at phase 6: Logweir's own post-restore `end_offsets` read timed out after 20 s during the freeze ("Meta data fetch error: OperationTimedOut"), no scorecard | 60,000 records, exact |
| 5 (fix-round pass, `44838f31`) | 9,000 / 60,000 | two produce requests failed with "Connection error during read response length" and were resent; then **exit 0** (phase 6 went on to its own read) | exit 1 at phase 6: Logweir's own post-restore `end_offsets` read met "NotLeaderForPartition" just after the thaw, no scorecard | 62,000 records: **2,000 `duplicate`** (p1@19000–19999, p2@17000–17999), nothing missing |
| 6 (engine **0.23.3**, PROD-00.3f, 2026-10-08) | 7,000 / 60,000 | one produce request "timed out after 60s waiting for broker response" and was resent ("Connection error on Produce request, reconnecting and retrying"); the restore completed | **exit 2, signed `fail-integrity`**: phase 7's count bound refused "restored 61000 records but the manifest bounds the window … at [60000, 60000]"; the 75 sampled records all matched | 61,000 records: **1,000 `duplicate`** (p0@19000–19999), nothing missing |

What this establishes, and what it does not:

- **A lost acknowledgement duplicates the in-flight batch.** In sample 3 the engine logged three
  produce requests timed out and resent, and 3,000 source offsets appear twice in the output —
  three batches' worth at `produce_batch_size` 1,000 (S11, measured). Which partitions held them
  and where the second copies landed were not recorded in that sample; the row now records the
  per-partition ranges. Sample 5 recorded them: two requests failed with a connection error and were
  resent, and exactly two 1,000-record ranges appear twice (p1@19000–19999, p2@17000–17999), nothing
  missing. **The engine exited 0 over that target**, so its exit code does not reveal a duplicate.
- **A broker outage longer than its session timeout can also abort the restore.** A single-node
  KRaft broker frozen past its session is fenced and re-registers when it thaws; the engine's
  metadata refresh met the moment a partition had no leader, treated leader `-1` as fatal and gave
  up on the topic. Logweir then exits 1 with no scorecard, and the partial output — duplicates
  included — stays behind as a new topic (Logweir deletes nothing it created in `newTopic` mode).
  On a multi-broker cluster the analogous event is a leader failover; not run.
- **Logweir's own client timeout fails a restore the engine completed.** In sample 4 every record
  landed exactly once, and phase 6's post-restore read (`crates/logweir/src/drill/phase6_restore.rs:113-117`,
  after `engine.restore()` returned) hit Logweir's fixed 20 s Kafka client timeout
  (`docs/stability.md` Later #14) during the freeze: exit 1, no scorecard, for a correct target. In
  sample 5 the same read failed on "NotLeaderForPartition" just after the thaw, while the
  re-registered broker's partitions were still without a leader.
- **Logweir never signed `pass` over a divergent output** in any sample: every fault ended in exit 1
  without a scorecard. The row asserts that invariant, that the fault was injected (both compose
  verbs exit 0), and nothing about the outcome, because the outcome depends on where the freeze
  lands (review L5c). A `Drop` guard thaws the broker on every exit path (L5b).
- **This row is not a deterministic fixture** (review M4). Duplicates appeared in two samples of
  five (3 and 5), the early engine exit in one (3), and a Logweir-side read failure after the engine
  finished in two (4 and 5). So no acceptance row rests on it: §7 names, for 07-1, 07-1b, 07-1c, 04-4 and 08-4, a fixture that produces its condition every
  time (a fault proxy that drops produce responses; a broker stop past the engine's retry budget; a
  pause at the engine's exit; a synthetic duplicated target), and PROD-07 builds them with
  PROD-01.5's profiles. The samples here are evidence that the conditions occur, not a rate.
- **Measured since (sample 6, engine 0.23.3): a duplicate that reaches a run Logweir completes
  fails a full restore's count bound.** The engine resent one timed-out batch, Logweir's phase 6
  read succeeded, and phase 7 signed `fail-integrity` (exit 2) because 61,000 restored records lie
  above the manifest's bound of 60,000. The fingerprint sample (75/75 matching) could not see the
  duplicate; the count bound did. Still not established: a point-in-time restore whose bound
  includes straddling segments may admit a duplicate (PROD-08.1's exact counts close this).

### 5.2 Termination before or after an acknowledgement — blocked on subprocess cancellation

`a_killed_restore_leaves_its_engine_writing` (`#[ignore]`d). The `logweir restore run` PID is
resolved while phases 0–5 run; `kill -KILL` is sent when the first restored record lands; then the
engine container and the target are read at once and every second for 20 s.

| Sample | Landed at the kill | Engine container right after the kill | Target afterwards | Logweir |
|---|---|---|---|---|
| 1 (earlier version of the row) | 26,000 / 60,000 | not read | 60,000 | killed, no scorecard |
| 2 | 8,000 / 60,000 | **running** (`d28844703f68`) | 60,000 within about a second; unchanged for 20 s; the container had exited | killed during phase 6, no scorecard |
| 3 (final pass, tip) | 3,000 / 60,000 | **running** (`88f03be3707f`) | 60,000 within about a second; unchanged for 20 s; the container had exited | killed, no scorecard |
| 4 (fix-round pass, `44838f31`) | 3,000 / 60,000 | **running** (`d6a6ba783675`) | 60,000 at the first read, 1.3 s after the kill; unchanged for 22 s; the container had exited | killed, no scorecard |
| 5 (engine **0.23.3**, PROD-00.3f) | 5,000 / 60,000 | **running** (`5a1086018dab`) | 60,000 at the first read, 1.3 s after the kill; unchanged for 22.5 s; the container had exited | killed, no scorecard |

The row asserts it (review L5d): an engine container of this worktree is running right after the
kill, and the target's last sample equals the whole archive. The engine outlives the process that
started it and completes the restore with nobody to verify,
sign or record it (measured on this host, where the engine runs through `engine-docker.sh`; a native
engine child would be left the same way, since nothing forwards the kill — not run). So "kill before the acknowledgement" and "kill after it" cannot be injected through Logweir:
the kill does not stop the writer. This is `docs/stability.md` Later #13 ("nothing in tag 1
propagates a cancel") made concrete, and it is why PROD-07.1's termination rows stay blocked until
PROD-07.2 propagates cancellation to the engine subprocess. Under Kubernetes the engine runs inside
the runner container, so deleting the pod ends it only when the kubelet stops that container
(Later #13: "a Job deleted mid-run leaves the child to the kubelet"); not measured here.

## 6. The product rail for transactional topics

### 6.1 Decision

**Refuse by default, detect at capture, label an explicit override, and support committed-only
recovery through a PROD-00.3 capability.** Of the three options the brief names:

| Option | Why not alone |
|---|---|
| Refuse | Kafka Streams exactly-once, Connect exactly-once sources and Flink write transactionally; a blanket refusal makes those topics unrecoverable until PROD-00.3 lands. Kept as the DEFAULT, not as the only path. |
| Label | A restore whose output holds aborted records and control records as data is wrong for every `read_committed` consumer (§2.1). A label that is only read after the fact does not stop a restore feeding a downstream application. Kept as the OVERRIDE's evidence. |
| Support (PROD-00.3) | The engine route: capture `READ_COMMITTED`, skip control batches, drop aborted transactions using the fetch response's aborted-transaction list, and end at the last stable offset. That is the only route to a restore whose output equals the committed input. It needs OD-3 and PROD-00.2 or an upstream release, so it cannot be the M2 answer. |

So: (a) every backup records whether its archive holds transaction control records and whether any
partition had an open transaction at capture; (b) a restore of such an archive is refused unless
its plan carries an explicit, approval-bound override, and the override's scorecard and receipt say
that uncommitted data and control records were replayed as data; (c) PROD-00.3a delivers
committed-only capture, after which (a) records `capture: readCommitted` and (b) no longer triggers
for archives written that way. No surface may say "transactional", "exactly-once" or
"read_committed" recovery before (c) is Done and its oracle row is green.

### 6.2 Detection route (rdkafka 0.36 has no DescribeProducers)

Three signals, all taken by Logweir around the engine run, none needing DescribeProducers,
ListTransactions, `unsafe` or an engine change. `e2e/tests/record_semantics.rs::detection_signals`
computes each one as a reference implementation and the TXN row asserts what each sees (§2.1):

1. **Control records in the archive** — every transaction that ENDED inside the captured range left
   its marker there (§2.1). Shape: key `00 00 00 0t` (`ControlRecordType` v0, t = 0 abort, 1
   commit), a 6-byte value starting `00 00` (`EndTransactionMarker` v0), and no header but the two
   lineage headers; the reference predicate is `oracle::is_control_shaped`. The shape is not proof
   on its own (review L1): a user record can have it (an integer key 0 or 1 with a 6-byte value
   starting `00 00`, such as a Confluent-framed 1-byte payload), and a marker of another encoding
   version does not. So every candidate is CONFIRMED by the offset gap: a `read_uncommitted` consumer sought
   to the candidate's offset skips a control record and returns a user record. A confirmed gap whose
   archived record does not match the shape (another marker version) is reported as
   `transactions.unrecognisedControlOffsets`, never ignored. The reference implementation confirms
   against the full `read_uncommitted` reading the row already has.
2. **An open transaction at the probe** — the last stable offset below the high watermark. A
   `read_committed` consumer's watermark query answers the last stable offset (librdkafka sends
   ListOffsets at the consumer's isolation level) and a `read_uncommitted` one the high watermark.
   Measured in the TXN row while transaction D was open: 5, 7, 5 against 6, 7, 6 — lower exactly on
   the two partitions D wrote to — and a `read_committed` consumer's end-of-partition position gave
   the same 5, 7, 5 (§2.1). Taken immediately before the engine starts, per captured partition.
3. **The uncommitted tail** (review M3) — a transaction that opens AFTER the probe and ends after
   the engine's last fetch leaves no marker in the archive and no gap in the probe. Its records are
   archived whether it opened before the engine planned its end or after (the engine keeps every
   record a fetch returns past its planned end: C1; `backup/engine.rs:1227, 1301-1308, 1520`). So after the engine exits, once the source's `read_committed` high mark has
   passed the archived range (bounded by the broker's `transaction.max.timeout.ms`, after which the
   broker aborts the transaction itself; a partition still short of it is flagged open), the
   archived records from the probe's last stable offset to the archive's end are compared with a
   `read_committed` reading of the same range; an archived data record that reading does not
   return is uncommitted data. This is the oracle's own committed-versus-archive method, restricted
   to the tail appended while the backup ran. Measured (§2.1): on a second topic whose only
   transaction, E, opened after every probe and aborted after the backup, signals 1 and 2 saw
   nothing (no marker; `read_committed` 1, 1, 1 against `read_uncommitted` 1, 1, 1) and signal 3
   returned exactly E's two records (p1@1, p1@2); on the main topic it returned D's two (p0@5,
   p2@5). Aborted records below the probe's mark (B's) are signal 1's: their abort marker is
   archived.

After PROD-00.3a the engine's own batch attributes (`transactional`, `control` —
`kafka-protocol … records.rs:559-560`, decoded and discarded today) recorded per segment replace
signals 1 and 3 for archives written that way.

DescribeProducers/ListTransactions (KIP-664) would name active producers, but are not in
rust-rdkafka 0.36's safe API; their route (raw protocol, FFI or upstream wrapper) is PROD-04.0's
administrative-path decision, not needed for this rail.

## 7. Acceptance rows for the dependent tasks

Each row: a pass predicate, a negative control that must make it fail, and the fixture. "Row X"
means the named function in `e2e/tests/record_semantics.rs`; "the oracle" means
`record_semantics_support::oracle::compare`, whose own controls are in
`e2e/tests/record_semantics_oracle.rs`. A dependent task extends those rows (or PROD-01.5's
profiles) rather than building a private fixture.

### PROD-02 — Continuous protection and recoverable history

| # | Pass predicate | Negative control | Fixture |
|---|---|---|---|
| 02-1 | A receipt states, per captured partition, the high watermark the engine read to, the last stable offset beside it, and `transactions.openAtCapture` (the one versioned field PROD-01.1a defines) exactly where they differ. | TXN row with transaction D open: p0 and p2 must carry the flag, p1 must not; a probe taken with the default `read_committed` watermark query on BOTH readings reports no difference and fails. | `transactional_topic_committed_input_versus_restored_output` |
| 02-2 | Coverage `from_ms`/`to_ms` and every recovery-point bound use the minimum and maximum RECORD timestamp per partition, never the first and last. | ts-floor fixture: a coverage floor of `T+2000` (first-record minimum) instead of `T+1000` fails. | `non_monotonic_create_time_below_the_window_floor` |
| 02-3 | A chained incremental capture seeded from the previous point's last archived offset restores, record for record under the oracle, exactly what one capture of the same range restores — including a transaction that is open at the first point and ends in the second. | Seed at last + 2: the oracle reports `missing` for the skipped offset and the row fails. | TXN row split into two points (extension) |
| 02-4 | An offset regression (the recreated topic's next capture starts below the previous point's end) forces a full copy under a new generation (PROD-01.4) instead of a chain. | Recreate row: seeding generation 2 from generation 1's end offset (4) reads nothing past gen 2's high watermark (2); the oracle reports every gen 2 record `missing`. | `recreated_topic_between_two_backups` |
| 02-5 | The continuous protocol's crash table (02.3) has a row for "capture ends while a transaction is open" and states how the later abort or commit marker reaches the archive. | A protocol that ends every run at the high watermark and never re-reads leaves the D records restored as data with no marker: TXN row's `extra:uncommitted` set is non-empty. | TXN row |

### PROD-04 — Consumer positions

| # | Pass predicate | Negative control | Fixture |
|---|---|---|---|
| 04-1 | Translating a committed source position `P` to "the first target record whose `x-original-offset` ≥ `P`" never lands on a restored control record or an aborted record: it is either the next committed record, or the translation is reported `unavailable`. | TXN row: a position equal to a marker's offset (§2.1 lists them) maps onto the marker's own target record with the naive rule; the row fails. | TXN row |
| 04-2 | A position is translated only within the archive of the SAME topic generation (PROD-01.4's identity). | Recreate row: `(partition, x-original-offset)` pairs 0 and 1 exist in both outputs with different payloads; a mapper that ignores the generation resolves a gen 2 position into gen 1 data. | `recreated_topic_between_two_backups` |
| 04-3 | The mapping report states the lineage depth it can prove: a record that already carried `x-original-offset` (a topic restored twice) keeps only the newest lineage. | Shapes row p0@6: the source's own `x-original-offset` (777) does not survive the restore; a report that claims the original lineage fails. | `keys_nulls_tombstones_and_duplicate_headers` |
| 04-4 | Duplicate copies of one source offset in a target (a retried batch) map a position to the FIRST copy. | A synthetic target carrying a repeated batch with its lineage headers (the shape the oracle's `a_retried_batch_is_reported_as_duplicates` builds): mapping to the second copy replays the batch twice. Deterministic; the ack-fault row reproduced duplicates only once in five samples (§5.1). | synthetic duplicated target |
| 04-5 | Time-based translation (offsets-for-times on the target) is refused for a source whose `message.timestamp.type` was `LogAppendTime`, because restored timestamps are the producers' CreateTime. | LAT row: a target lookup at a source append time returns end-of-partition, not the record. | `log_append_time_source_versus_restored_output` |

### PROD-07 — Interruption and resume

| # | Pass predicate | Negative control | Fixture |
|---|---|---|---|
| 07-1 | Resume and retry semantics bound duplicates by the in-flight produce requests of each partition (`produce_batch_size`, default 1000, per request) — the window §5.1 observed twice (sample 3: three requests resent, 3,000 duplicates; sample 5: two, 2,000). | Deterministic fixture PROD-07 must build: a fault proxy on its own broker listener (a PROD-01.5 profile) that forwards each produce request and drops its response once, so every in-flight request is appended and then resent. The row fails when the output holds more duplicate copies than dropped responses, or a duplicate of a request whose response was delivered. | fault-proxy profile (to build); the ack-fault row is observational evidence only |
| 07-1b | An engine run that ends early (exit 1) is reported as interrupted with the partial targets it wrote and their `x-original-offset` coverage, never as a generic failure; the partial target is not reused as if complete. | Deterministic fixture: stop the broker (`docker compose stop`, not pause) once the first records have landed, keep it down past the engine's connection-retry budget (5 retries with 0.5–2.5 s backoff, `kafka/partition_router.rs:500-551`), then start it; the engine exits 1 with a partial target every time. A report that names no partial target fails. §5.1 sample 3 showed the outcome once, by chance. | broker-stop fixture (to build) |
| 07-1c | A Logweir-side read failure after the engine exited 0 is reported as "restore finished, verification not completed", distinct from an engine failure, and can be re-verified without re-restoring. | Deterministic fixture: the fault proxy placed in front of Logweir's own client, or a broker pause started when the engine container exits and held past Logweir's 20 s client timeout. A state that does not say the engine finished fails. §5.1 samples 4 and 5 showed the outcome, by chance. | fault proxy / pause-at-engine-exit (to build) |
| 07-2 | Resume selects what remains by OFFSET (per partition, after the target tail's largest `x-original-offset`), never by re-applying the time window. | ts-pit row: a resume that re-selects segments by first/last timestamps skips the segment holding the in-window record (p0@5). | `non_monotonic_create_time_skipped_at_the_point_in_time` |
| 07-3 | The tail scan tolerates control records and aborted records in a `READ_UNCOMMITTED` archive: the tail's largest `x-original-offset` may be a marker's, and resuming after it loses nothing. | TXN row output truncated after a marker; a scan that only accepts data-shaped records resumes one record early and duplicates it. | TXN row |
| 07-4 | Termination-based fault injection (kill before and after an acknowledgement) is run only once a cancel reaches the engine subprocess (`docs/stability.md` Later #13, PROD-07.2): after the cancel, the engine is gone and the target stops growing. | §5.2: today the engine container is still running after `logweir` is killed and the target grows to completion; the kill row must then fail on a new "writer stopped" assertion. | `a_killed_restore_leaves_its_engine_writing` |

### PROD-08 — Verification that answers recovery questions

| # | Pass predicate | Negative control | Fixture |
|---|---|---|---|
| 08-1 | Complete mode computes the expected set per partition from each archived record's own timestamp and the requested window, and compares it with the output by `x-original-offset`. | ts-pit row: expected 17, output 16 (p0@5 missing) must FAIL; today it passes (§2.2). | `non_monotonic_create_time_skipped_at_the_point_in_time` |
| 08-2 | The same model does not fail a correct point-in-time restore. | ts-bound row: output exactly the 11 in-window records must PASS; today it fails on the count bound (§2.2). | `non_monotonic_create_time_inside_a_wholly_inside_segment` |
| 08-3 | A full restore's window has no lower bound below which archived records are dropped (or the floor is the minimum record timestamp), so a full restore returns every archived record. | ts-floor row: p0@1 must come back; today it is dropped on every restore (§2.2). | `non_monotonic_create_time_below_the_window_floor` |
| 08-4 | Duplicates and order are checked on `x-original-offset` over the whole output, not an offset-keyed map of the sample. | Deterministic: the synthetic duplicated target of 04-4 fed to phase 7 (a unit fixture over `phase7_verify::compare`), and the oracle's duplicate and out-of-order controls: a `BTreeMap` keyed by the lineage header (as `phase7_verify::compare` builds today) collapses the copies. | synthetic duplicated target |
| 08-5 | Evidence says what it compared against: `comparisonBasis: archive` today, and a transactional archive's scorecard carries the §6 label; no evidence field implies agreement with the SOURCE. | TXN, LAT and shapes rows: their capture-side divergences are invisible to an archive-to-target comparison, which reports `pass` (§2). | TXN, LAT, shapes rows |
| 08-6 | Header comparison is ordered and multiplicity-aware in complete mode, or the evidence states that header order is not verified. | The oracle's `any_other_header_difference_is_headers_changed` (reordered headers) against `logweir_kafka::fingerprint::record_fingerprint`, which sorts headers and so cannot see it. | oracle controls |
| 08-7 | Full streamed comparison (08.3) models the expected transformation exactly: the two appended lineage headers, and nothing else. | Shapes row: p0@4 and p0@6 must be reported (collapsed headers), not absorbed into a tolerance. | shapes row |

## 8. FX-6: the evidence and the wording

FX-6 edits the user documents; this record supplies what they may say. Each sentence below is
backed by a row of §2 and may be used verbatim.

### 8.1 `docs/verify-a-scorecard.md`, under "What the scorecard does **not** claim"

> ### A pass compares the restored topic with the archive, not with the source
>
> Restore drills fingerprint sampled records of the restored topic against the same records in
> the archive. A loss that happened when the archive was written is in both, so the comparison
> cannot see it. With engine 0.21.0:
>
> - **Transactions.** The backup reads uncommitted data. Records of aborted transactions, records
>   of transactions still open when the backup ran, and the transactions' commit and abort
>   markers are archived and restored as ordinary records. A consumer of the restored topic sees
>   all of them, even with `isolation.level=read_committed`, because the restore does not produce
>   transactionally. The drill passes.
> - **`LogAppendTime` topics.** The archive keeps the timestamp each producer set, not the time the
>   broker appended the record, and the restore writes it as `CreateTime`. Restored timestamps
>   therefore differ from what the source topic reported, and a point-in-time restore selects
>   records by the producers' clocks. The drill passes.
> - **Repeated header keys.** When a record carries the same header key more than once, only one
>   copy is archived and restored: at the first copy's position, with the last copy's value. The
>   drill does not detect it. A record that already carried `x-original-offset` (a topic that was
>   itself restored) keeps only the backup's own, and a drill that samples such a record fails.
> - **Out-of-order timestamps.** Recovery-point selection reads each archive segment's first and
>   last record timestamps. When timestamps are not increasing within a segment, a point-in-time
>   restore can omit a record at or before the requested point without the drill noticing; a
>   record older than every segment's first record is dropped from every restore, full or
>   point-in-time — a full restore's drill fails its count check, and a point-in-time drill can
>   pass; and a correct point-in-time restore can fail the count check.
>
> Record order within a partition, keys, values, null versus empty keys, values and header values,
> tombstones, and the records a compacted source held when it was backed up are preserved.
> `docs/to-do/decisions/PROD-01.1-record-semantics.md` records how each statement was measured.

### 8.2 `docs/stability.md`, "Known limitations of v0.1"

> ### Transactional topics are restored with aborted records and markers as data
>
> The pinned engine captures with `READ_UNCOMMITTED` and archives transaction control records as
> ordinary records; the restore replays every archived record non-transactionally. A restored
> transactional topic therefore holds aborted records, records of transactions open at backup time,
> and one record per commit or abort marker, all visible to every consumer. Logweir does not claim
> transactional or exactly-once recovery. Measured by
> `e2e/tests/record_semantics.rs::transactional_topic_committed_input_versus_restored_output`.
>
> ### Recovery-point selection uses segment first and last timestamps
>
> The archive describes each segment by its first and last record timestamps, not its minimum and
> maximum, and both the engine's segment selection and Logweir's window floor and count bound read
> those. When record timestamps are out of order within a segment: a record older than every
> segment's first record is dropped from every restore of that archive, full or point-in-time — a
> full restore's drill fails its count check, and a point-in-time drill can pass (measured: the same
> archive at a point the affected segment straddles was signed `pass`); a point-in-time restore
> omits a record at or before the point when its segment's first record is after the point, and the
> drill passes; and a correct point-in-time restore fails the count check when a segment whose first
> and last records are inside the window holds a later record. Measured by the three
> `non_monotonic_…` rows of `e2e/tests/record_semantics.rs`.
>
> ### `LogAppendTime` sources are restored with the producers' timestamps
>
> For a topic on `message.timestamp.type=LogAppendTime`, the archive holds the timestamp each
> producer set, not the broker's append time, and the restored topic reports it as `CreateTime`.
> Restored timestamps differ from what the source reported, and a point-in-time restore selects by
> the producers' clocks: in the measured case, a recovery point in 2001 restored records the broker
> appended in 2026. The drill passes in both cases. Measured by
> `e2e/tests/record_semantics.rs::log_append_time_source_versus_restored_output`.
>
> ### A broker outage during a restore can leave a partial target with duplicates
>
> The engine resends a produce whose acknowledgement it did not receive, and does not produce
> idempotently, so a lost acknowledgement duplicates that batch in the target. An outage long
> enough for the broker to re-register can also end the restore early: Logweir then exits 1 with no
> scorecard, and the partial target — duplicates included — remains. A stall longer than Logweir's
> own 20-second client timeout can also fail a restore whose target is complete. Measured by
> `e2e/tests/record_semantics.rs::a_lost_produce_acknowledgement_during_restore`.

### 8.3 The restore review screen

One sentence, shown when the plan names any topic (the console cannot yet tell which topics are
transactional; §6's detection makes it conditional later):

> Restores copy the archive as written: aborted transactions and transaction markers are restored
> as ordinary records, `LogAppendTime` timestamps come back as producer `CreateTime`, repeated
> header keys keep one copy, and when timestamps are out of order a full or point-in-time restore
> can miss records.

## 9. Proposed ledger rows

For the orchestrator to add to the Execution ledger: two Logweir-side rails (01.1a, 01.1b), one
fix-now row (FX-8, for the "Fix-now defects" table too) and five engine capabilities. Engine
capabilities are named by PROD-00.1's
capability-table entries; their ROUTE (upstream PR, fork patch, Logweir-native, unsupported) is
PROD-00.1's to propose and OD-3's to decide — this record supplies each one's oracle.

| Wave | Row | Title | P | M | Kind | Depends on | Gate | Lab | Tier |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | PROD-01.1a | Detect transactional archives; refuse by default, label an approved override | P1 | M2 | impl | 01.1, FX-6 | — | compose | A |
| 1 | PROD-01.1b | Make recovery-point selection safe for out-of-order timestamps | P1 | M2 | impl | 01.1 | — | compose | A |
| 0 | FX-8 | Refuse or label point-in-time selection over `LogAppendTime` sources | P1 | M1 | fix | 01.1; FX-4 for the broker-default arm | — | compose | A |
| 3 | PROD-00.3a | Committed-only capture (control records and READ_COMMITTED) | P2 | M3 | impl | 00.1; 00.2 for a patch route | OD-3 | compose | A |
| 3 | PROD-00.3b | Segment min/max record timestamps | P2 | M3 | impl | 00.1; 00.2 for a patch route | OD-3 | compose | A |
| 3 | PROD-00.3c | Keep `LogAppendTime` through capture | P2 | M3 | impl | 00.1; 00.2 for a patch route | OD-3 | compose | A |
| 3 | PROD-00.3d | Idempotent (or sequence-checked) restore produce | P2 | M3 | impl | 00.1; 00.2 for a patch route | OD-3 | compose | A |
| 3 | PROD-00.3e | Keep repeated header keys through capture and replay | P2 | M3 | impl | 00.1; 00.2 for a patch route | OD-3 | compose | A |

### PROD-01.1a — Detect transactional archives; refuse by default, label an approved override

- **Approach:** §6. Backup: take signal 2's two watermark readings per partition immediately before
  the engine starts; after it, run signal 1 (scan the new segments with `logweir_engine_oso::kbak`
  and confirm each candidate by the offset gap) and signal 3 (the tail reconciliation); record ONE
  versioned receipt and catalog field, `transactions { controlRecords, openAtCapture[],
  uncommittedTail, unrecognisedControlOffsets }`, in both verifiers and the parity script. Restore
  phase 0: refuse a set whose `transactions` block reports any signal, with the condition
  `TransactionalArchive` (exit 3, before any target is created), unless the approved plan carries
  `restore.transactionalData: replayAsData`; with it, the scorecard and the console review carry the
  label. A set whose receipt predates the field is scanned at phase 5 over the selected segments
  (signal 1 only: the source's state at capture is gone) and reads `transactions: partiallyAssessed`.
- **Acceptance:** the TXN row's archive is flagged by all three signals as §2.1 measured (7 control
  records; p0 and p2 open at the probe; D's two records in the tail), and its `txn-late` topic by
  signal 3 alone (E's two records); the default restore of either is refused before any target is
  created; the override restores it and the scorecard says `transactionalData: replayedAsData`; the
  shapes, compaction and timestamp rows are not flagged; old receipts verify unchanged in both
  verifiers.
- **Negative controls:** disabling the segment scan leaves the TXN restore running; disabling the
  tail reconciliation leaves the `txn-late` restore running (a transaction opened after the probe
  and aborted after the backup, review M3); an override outside the plan-hash-covered bytes is
  refused; a user record of marker shape at an offset a `read_uncommitted` consumer returns (an
  integer key 1 with a 6-byte value `00 00 …`) is NOT counted, and a mutant that skips the offset-gap
  confirmation counts it and fails.
- **Migration and rollback (rule 9):** drills and `RehearsalSchedule` runs over transactional topics
  start refusing at phase 0. The Restore's status carries `TransactionalArchive` naming the topics
  and the override; a scheduled run is recorded as refused with that reason; the console review
  shows both. Archives written before the field read as `partiallyAssessed`, never as clean. The
  field is additive: each verifier that predates it must be shown to accept a receipt carrying it or
  reject it clearly (rule 3). Rolling the runner back removes the refusal and leaves the field in
  newer receipts. `docs/stability.md` and `docs/kubernetes.md` record the change.

### PROD-01.1b — Make recovery-point selection safe for out-of-order timestamps

- **Approach:** (1) no restore window, full or point-in-time, has a lower bound that can drop an
  archived record: render the start as the minimum DECODED record timestamp over the named topics
  (or the minimum representable instant), and amend guard G-WIN's binding and its phase-5
  re-derivation to say so; (2) before a point-in-time restore, decode the segments whose first
  record is after the point and refuse with `PointInTimeSelectionIncomplete` when any of them holds
  a record at or before it, until PROD-00.3b selects by min/max; (3) the manifest count bound takes
  min/max from decoded records (or yields to PROD-08.1's exact counts).
- **Acceptance:** ts-floor restores all twelve records in full and all eleven at or before `+3500`
  at its point in time (p0@1 included), and both pass; ts-pit is refused before any write, naming
  the segment; ts-bound passes; G-PITR (`e2e/tests/pitr_boundary.rs`) stays green.
- **Negative controls:** the three `non_monotonic_…` rows as they stand today.

### FX-8 — Refuse or label point-in-time selection over `LogAppendTime` sources (fix-now)

- **Defect:** a point-in-time restore over a `LogAppendTime` source selects records by the
  producers' CreateTime, which is what the archive holds (S3), and Logweir signs it `pass`.
  Measured: a point in 2001 restored six records the broker appended in 2026 (§2.3,
  `final-…/lat.json` `restores[1]`). The receipt's `covered.from_ms/to_ms` and the console's offered
  range are producer time too. Same class as ts-pit, which 01.1b rails; this one needs no engine
  change to rail, and 00.3c stays the real fix (review M2).
- **Required fix (Logweir-side):** at restore planning, read each named topic's timestamp type: the
  manifest's `configurations["message.timestamp.type"]` when the source set it as a topic override
  (measured present, §2.3), or, for a broker-wide default the engine does not capture
  (`manifest.rs:143-146` keeps explicit overrides only), the effective value Logweir records at
  backup time through FX-4's per-topic configuration coverage. For a `LogAppendTime` topic, refuse
  a point-in-time restore with `PointInTimeByProducerTime` (exit 3, before any target is created)
  unless the approved plan states `restore.timeBasis: producerTime`; then the scorecard, the receipt
  and the console label the selection "by producer time". Full restores still run (FX-6 discloses
  their timestamps). A topic whose type was not recorded reads "timestamp type not recorded" in the
  plan, and the console warns.
- **Scope:** restore planning (phase 0), one optional plan field inside the plan hash, one versioned
  scorecard/receipt label in both verifiers and the parity script, console review text. Not the
  engine.
- **Dependencies:** PROD-01.1 (this record) for the topic-override arm; FX-4 for the
  broker-default arm.
  **Tier:** A (plan grammar, guards, evidence fields). **Lab:** compose.
- **Acceptance:** the LAT row's point-in-time restore is refused before any target is created,
  naming the topic; with `restore.timeBasis: producerTime` it runs and the scorecard carries the
  label; the LAT full restore still runs.
- **Negative controls:** a `CreateTime` topic's point-in-time restore (ts-pit, ts-bound) is not
  refused; a broker-default `LogAppendTime` source (the broker's `log.message.timestamp.type` set as
  a dynamic config, as `docs/stability.md`'s Task 8 measurement did, and no topic override) is
  refused once FX-4 lands and reads "timestamp type not recorded" before; a mutant that reads only
  the manifest override misses that case and fails.
- **Fix-now table row:** `| FX-8 | A point-in-time restore over a LogAppendTime source selects by
  producer time and is signed pass. | lat.json restores[1]: point 2001, six records appended in 2026,
  pass 6/6; S3; manifest configurations. | Refuse (or, by an approved plan field, label) point-in-time
  selection for LogAppendTime topics; override arm now, broker-default arm with FX-4. |`

### PROD-00.3a — Committed-only capture

- **Capability:** fetch at `isolation_level=1`, skip control batches, drop aborted transactions
  using the fetch response's `aborted_transactions`, and end each partition's range at its last
  stable offset; record `capture: readCommitted` in the manifest.
- **Oracle:** the TXN row. Pass: capture, replay and end-to-end divergence sets all empty, the
  archive holds no control-shaped record, and the D records are absent. Negative control: a build
  that fetches `READ_COMMITTED` but keeps control batches still reports seven
  `extra:control-marker` divergences.

### PROD-00.3b — Segment min/max record timestamps

- **Capability:** the manifest records each segment's minimum and maximum record timestamp and
  replay selects segments by them (first/last kept for old readers).
- **Oracle:** ts-pit's replay set becomes empty (p0@5 restored); ts-floor and ts-bound unchanged
  until PROD-01.1b's floor and bound change. Negative control: the pinned engine's archive.

### PROD-00.3c — Keep `LogAppendTime` through capture

- **Capability:** for a batch whose attribute says `LogAppendTime`, archive its max timestamp as
  each record's timestamp (the broker's own rule) and record the timestamp type per segment. The
  engine already parses the batch header by hand (`kafka/fetch.rs:156-191`); the max timestamp and
  the type bit are in the same header, so this needs no change to `kafka-protocol` (review L2).
- **Oracle:** the LAT row's capture set becomes empty and the restored timestamps equal the source's
  append times (end to end keeps only `timestamp-type-changed`: Logweir creates every target as
  `CreateTime` by design), and FX-8's refusal no longer triggers for archives written this way.
  Negative control: the pinned engine.

### PROD-00.3e — Keep repeated header keys

- **Capability:** hold headers as an ordered list at decode and at encode. `kafka-protocol`'s public
  `Record.headers` is an `IndexMap` (`records.rs:184`), so the route is an upstream change to
  tychedelia/kafka-protocol-rs or a fork, unlike 00.3c (review L2).
- **Oracle:** the shapes row's capture and replay sets become empty, AND Logweir's verdict on it
  passes. The second needs a Logweir-side change in the same acceptance: phase 7 keys the target by
  the FIRST `x-original-offset` (`crates/logweir/src/drill/phase7_verify.rs:288-293`, `.find`), so
  with both headers kept p0@6 would be keyed at 777 and reported absent; it must key by the LAST,
  as the oracle does. Negative control: the pinned engine.

### PROD-00.3d — Idempotent restore produce

- **Capability:** the engine produces with a producer id, epoch and sequence numbers
  (InitProducerId), so a resent batch is deduplicated by the broker.
- **Oracle:** the ack-fault row reports no `duplicate` divergence with the broker frozen past the
  engine's response timeout. Negative control: the pinned engine's result in §5.

## 10. Limits of this record

- **One broker line.** Every row ran on Apache Kafka 3.7.1 in the single-node compose stack.
  PROD-01.5's 3.9, 4.1 and 4.3 profiles should re-run `e2e/tests/record_semantics.rs`; nothing
  here depends on a broker version except S3's premise (the broker rewrites only the batch max
  timestamp for `LogAppendTime`), which is Kafka's documented batch format.
- **One producer library.** Fixtures were produced with librdkafka (rust-rdkafka 0.36.2). A Java
  producer batches and compresses differently; the engine's decode of a record does not depend on
  that, and the engine's own test over a Java-produced batch (`kafka/fetch.rs:618-663`) exercises the
  same decoder, but no Java fixture ran here.
- **Small fixtures.** Rows hold tens of records (the acknowledgement row 60,000). They establish
  presence or absence of each divergence, not rates; the transaction row does not measure how many
  aborted records a real Streams application leaves.
- **Single run per row.** Each outcome in §2 was produced by one run at the tip named in the
  report; the rows are deterministic by construction except the two fault rows, whose timing is
  described with their result.
- **Not run:** header-count and header-key-length overflow (S14); a Java-produced
  `LogAppendTime` batch; a transactional producer that uses `sendOffsetsToTransaction` (consumer
  positions are PROD-04's); cross-cluster restores.
- **Oracle attribution quirks** (review L8; detection holds, labels can mislead): a record produced
  to the wrong partition reads as a `duplicate` there plus a `missing`; one record moved early
  reads as one `out-of-order` per record it overtook; and the capture comparison sorts the archive
  by offset, so C1's "in offset order" rests on the replay comparison and on each segment's own
  offset range, not on the archive's physical order. A tampered later copy of a duplicate is now
  field-checked (`a_tampered_later_copy_is_reported_beside_the_duplicate`).
- **Contract gating** (review L4): the rows assert this record's contract only on the pinned
  engine (`CONTRACT_ENGINE`, 0.21.0 until PROD-00.3f, 0.23.3 since); `engine-matrix` runs of other
  releases record outcome files and assert nothing (on v0.19.2 the manifest carries no
  `configurations`, which arrived in 0.20.0). `e2e/tests/engine_pin.rs` fails when the constant
  and the pin disagree (PROD-00 decision record, A-3f-1).
- **Routes are not decided here.** Which of PROD-00.3a–d is an upstream PR, a fork patch or a
  Logweir-native path is PROD-00.1's proposal and OD-3's decision.

## 11. Re-measured on engine 0.23.3 (PROD-00.3f, 2026-10-08)

The pin moved from 0.21.0 to 0.23.3 ([PROD-00 decision record](PROD-00-engine-route.md) §12). From
source, nothing this record measures could change: `kafka/fetch.rs`, `kafka/produce.rs`,
`segment/`, `restore/filter.rs` and `kafka-protocol` 0.18.0 are byte-identical from 0.21.0 to
0.23.3, and the produce router still re-sends a timed-out batch with no producer id. 0.23.0's
restore changes batch the offset-mapping updates (the offset report's `first_timestamp` may now be
a segment's minimum where timestamps are not monotonic; Logweir hashes that report and parses
none of it) and scope the router's connection-pool eviction.

**Runs.** Compose slot 4, Apache Kafka 3.7.1 (read back from the running container), engine image
`sha256:cc7d5a8aefa422dadc602d6349624c4563b38478ee6893de5240b98f16a732db`, branch
`claude/prod-00-3f`; outcome files under `artifacts/prod-00-3f/runs/c1-full/record-semantics-v0.23.3/`
and `runs/c2-*/`.

| Row | On 0.23.3 |
|---|---|
| The eight live rows (TXN, which also writes txn-late; ts-pit; ts-floor; ts-bound; LAT; shapes; compaction; recreate) | **8 passed with the contract asserted** (no row printed "contract not asserted"); every outcome file names engine 0.23.3. The broker's time retention deleted no segment during the run (A-C20-2 holds) |
| Ack fault (§5.1) | sample 6: one resent batch, 1,000 duplicates, and for the first time a completed run, which Logweir signed `fail-integrity` on the count bound |
| Kill (§5.2) | sample 5: the engine outlived `logweir` and finished the restore, as on 0.21.0 |

**Contract change: none.** The capture, replay and verification statements of §3, the
counterexamples of §2 and the routes of §9 hold on 0.23.3 unchanged. The one new fact (sample 6)
is about Logweir's verdict on a duplicate it can count, not about the engine: C5 is as it was.

---

Documentation is licensed [CC-BY-4.0](../../LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
