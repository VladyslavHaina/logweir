# PROD-01.4 — Topic identity and generations

Date: 2026-09-28. Row: PROD-01.4 (research, Tier B). Base revision: main `adee0a16`.
Evidence: `e2e/tests/topic_identity.rs` (this row's oracle), run against Apache Kafka 3.7.1 and 4.3.1 (compose broker, one KRaft node) with the pinned engine v0.21.0 (`sha256:8ff5be71…c317`). The first round made three runs; the fix round (review of `b0f50d0d`) made one run per line at `d22dd170`.
Status: proposed for review. One owner-level choice (§6.4, TI-OC1) is presented with options and a recommendation, and is not taken here.
Addendum, 2026-10-10 (PROD-01.5c): the oracle was re-run on Kafka 3.9.2 and 4.1.2, and again on 3.7.1 and 4.3.1, with the engine the images now ship (`0.23.3+logweir.2`). It passed 53 of 53 on each line, and every live row equals this record's measurement (§5). No rule, verdict or limit changes except the broker lines of §10.

Engine paths below are inside the pinned tarball `third_party/kafka-backup-v0.21.0.tar.gz` (sha256 `0252a837…405b`), under `kafka-backup-0.21.0/crates/kafka-backup-core/src/`. librdkafka paths are inside the locked `rdkafka-sys 4.10.0+2.12.1` crate, under `librdkafka/`.

## 0. Decisions in one page

1. **Identity of recoverable history is (source cluster ID, topic name, generation).** A generation is one continuous offset history. Consumers react to a *break*, which is any pair of observations that cannot belong to one continuous history. Every recreation is a break, and so is a same-ID truncation. The heuristic cannot see one kind of recreation, the byte-identical replay of §4.7.
2. **`topic_id` is nullable.** It is Kafka's text form of the topic UUID: 22 URL-safe base64 characters derived from the UUID's two 64-bit halves. Null means *unknown*, never *same*, and the all-zero UUID is never written.
   - A capture records it twice: `topic_id` before the engine and `topic_id_after` after it.
   - It lives in receipt 1.1.0, catalog point 1.1.0 and the API, and in the engine manifest only through the engine route (§3).
   - The generation token that groups points comes from the lineage, not from the ID (§2).
3. **The heuristic usable now** compares a capture with its predecessor. It returns one of five verdicts: `continuous`, `unverified`, `suspected`, `break`, `unknown` (§4). It checks:
   - partition counts, and log start and end offsets read READ_UNCOMMITTED;
   - the predecessor's archived tail records against the SAME offsets in the current capture's own archive. It never uses a live read, which differs from the archive on `LogAppendTime` topics and on repeated header keys (§4.1).
   With IDs, the same checks still run, so gaps and a same-ID truncation stay visible.
4. **Measured** (§5): 19 live rows, with identical outcomes on Kafka 3.7.1 and 4.3.1, and on 3.9.2 and 4.1.2 since PROD-01.5c.
   - **8 rows make a new topic:** 6 detected (5 `break`, 1 `suspected`) and 2 known misses. The ID path detects all 8.
   - **11 rows keep the topic:** no false `break`. There is one known false positive, a `suspected` after partitions were added and every old tail deleted (c19), and the ID path clears it.
   - **The two rejected variants:** a source read gives false breaks on the `LogAppendTime` and repeated-header rows (c17, c18). Offsets alone give 2 `break` and 2 `suspected` among the 8 new topics, and `suspected` on two same-topic rows (c05, c19).
5. **Real IDs** (§6): the owner decides ONE scoped-`unsafe` policy for librdkafka calls that rdkafka 0.36.2 does not wrap (**TI-OC1**).
   - It covers this row's DescribeTopics and PROD-04.0's group, offset and ACL calls.
   - Recommended: one small FFI crate. The measured DescribeTopics prototype is 123 lines, needs no new third-party crate, and returns the broker's IDs.
   - The upstream wrapper is stalled: no rust-rdkafka release through 0.39.0 has it, and PR #721 has been open and conflicting since 2024-09.
   - The engine route is PROD-00.1's row, and identity does not need it.
6. **Consumer reactions** are in §7, and numbered acceptance rows for PROD-02.1, 04.1, 04.2, 07.1, 11.1 and 15.1 are in §8. Proposed child rows are in §9.

## 1. What exists today

### 1.1 The engine

| # | Fact | Evidence |
| --- | --- | --- |
| E1 | The engine sends Metadata **v9**. Topic IDs are in Metadata responses from v10 (KIP-516), so none reach it. Its `TopicMetadata` holds name, internal flag and partitions only. No `topic_id` appears anywhere in its source. Upstream v0.22.0 (tag commit `cc10aa4a`) changes neither. | `kafka/client.rs:591`; `kafka/metadata.rs:21-25`; PROD-00.1's diff `artifacts/prod-00-1/upstream-v0.21.0..v0.22.0.diff` |
| E2 | The manifest records, per partition, segments with first and last offset and first and last *record* timestamp, plus `gaps` and `pruned`. It records no watermarks and no ID. | `manifest.rs:124-150` (`TopicBackup`), `:176-198` (`PartitionBackup`), `:349-387` (`SegmentMetadata`) |
| E3 | Logweir gives every run a fresh `backup_id`, so every run is a full capture that starts at the log start. The manifest lives at `{backup_id}/manifest.json`. | `backup/engine.rs:639`, `:1047-1063`; D3 amendment RECEIPT-DUP; the tracker's PROD-02.2 issue ("gives each slot a fresh `backup_id`") |
| E4 | Inside one run, OFFSET_OUT_OF_RANGE either records an `OffsetGap` (the log start moved past) or fails with "beyond the log end offset (log truncated or topic recreated?)". A recreated topic that is already longer than the engine's position is read on silently. | `backup/engine.rs:1233-1283`, `:1552-1570` |
| E5 | The engine appends `x-original-offset` and `x-original-timestamp` (little-endian `i64`) *after* each record's own headers. Logweir renders `include_offset_headers: true` for backups and `strip_offset_headers: false` for restores. | `backup/engine.rs:1836-1858`; `crates/logweir-engine-oso/src/render_backup.rs:260`; `render_restore.rs:180` |
| E6 | Transaction markers are archived as ordinary records: the fetch decoder keeps every record of every batch. | `kafka/fetch.rs:148-198` |
| E7 | The engine reads READ_UNCOMMITTED. | `kafka/fetch.rs:53`, `:263`, `:337` |
| E8 | The engine does not archive every record as the broker holds it, in two ways. **Timestamps:** its decoder gives each record the batch's first timestamp plus the record's delta, and discards the batch's max timestamp whatever the timestamp type, so a `LogAppendTime` batch is archived with the producers' CreateTime. **Headers:** its decoder holds headers in an `IndexMap`, so a repeated key keeps one entry, at its first position with its last value. PROD-01.1 measured both (its S3 and S4). The decoder is kafka-protocol 0.18.0 (crates.io sha256 `099d5c2f…2dda`, equal to the engine's `Cargo.lock:1877-1880`); upstream v0.22.0 keeps the same version. | `kafka-protocol-0.18.0/src/records.rs:572-585`, `:861-862`, `:896-919`; `kafka/fetch.rs:200-219`; `docs/to-do/decisions/PROD-01.1-record-semantics.md` §1 S3–S4, §2.3–2.4 (on `claude/prod-01-1`); its artifacts `outcomes/lat.json` and `outcomes/shapes.json` |

### 1.2 Logweir

| # | Fact | Evidence |
| --- | --- | --- |
| L1 | `logweir-kafka` forbids `unsafe` and is the only broker-dialling crate (ADR 0004). `end_offsets` and `consume_range` discard the log start offset. Its consumer sets no `isolation.level`, so librdkafka's default `read_committed` applies. `rd_kafka_query_watermark_offsets` sends ListOffsets at that level, so the "high" it returns is the last stable offset while a transaction is open. | `crates/logweir-kafka/src/lib.rs:3`; `rdkafka_reader.rs:283`, `:448`, `:127-153`; `librdkafka/src/rdkafka_conf.c:1478-1484`; `rdkafka_request.c:929` |
| L2 | Phase −1 reads only the cluster ID. The readback reduces the manifest to per-topic counts and one covered window, so no per-partition offset reaches the receipt. | `crates/logweir/src/backup/phase_minus1_admit.rs:157-203`; `phase_run.rs:192-330` |
| L3 | The backup receipt is `format_version` 1.0.0 with five arms. The Python reader mirrors it, and both readers ignore unknown fields. The payload type is `…backup-receipt+json;version=1.0.0`. | `crates/logweir-core/src/backup_receipt.rs:54-105`, `:264-384`; `docs/verify_scorecard.py:1235-1287`, `:1325`; `crates/logweir-verify/src/lib.rs:69-70` |
| L4 | The catalog point is 1.0.0. An absent optional field means *unknown* (D3 §5.2 rule 2), and unknown fields are ignored inside major 1. | `crates/logweir/src/catalog/record.rs:14`, `:219-231`; `catalog/reader.rs:40-60` |
| L5 | An optional field is a MINOR bump with a new schema file beside the old one. The product API is pre-release (`1.0.0-alpha.1`). | `docs/stability.md:17-19` and "The product API's OpenAPI document is pre-release" |

### 1.3 Clients

| # | Fact | Evidence |
| --- | --- | --- |
| C1 | rdkafka 0.36.2's safe `AdminClient` has no DescribeTopics. The crate re-exports the raw bindings and exposes the native handle. | `rdkafka-0.36.2/src/admin.rs:51-332`; `src/lib.rs:275`; `src/client.rs:272` |
| C2 | The locked librdkafka 2.12.1 binds `rd_kafka_DescribeTopics`, `rd_kafka_TopicDescription_topic_id` and the `rd_kafka_Uuid_*` accessors. | `rdkafka-sys-4.10.0+2.12.1/src/bindings.rs:2817`, `:2870`, `:674-692` |
| C3 | librdkafka added topic IDs through DescribeTopics in v2.3.0. Brokers below inter-broker protocol 2.8 return zero-valued IDs. v2.10.0 fixed metadata-cache corruption when a name's topic ID changes. | `librdkafka/CHANGELOG.md:784-787`, `:576-580`, `:314-315` |
| C4 | librdkafka's `rd_kafka_Uuid_base64str` uses the **standard** base64 alphabet (`+`, `/`). Kafka prints topic IDs in the **URL-safe** alphabet (`-`, `_`). Measured: the broker printed `Cf6zT_mcTNCoxuPmv1Ztxw` where librdkafka returned `Cf6zT/mcTNCoxuPmv1Ztxw` for the same ID. | `librdkafka/src/rdkafka.c:5461-5485` → `rdbase64.c:116` (not the URL-safe `:130`); `artifacts/prod-01-4/ffi-route-evidence.txt` |
| C5 | Upstream rust-rdkafka releases 0.37.0 (2024-11-25), 0.38.0 (2025-07-05) and 0.39.0 (2026-01-25, commit `598ac4ba`) add no DescribeTopics and no topic ID. All three are MIT; 0.39.0 depends on rdkafka-sys 4.10.0. Issue #614 (open since 2023-10-15) asks for it. PR #721 (open since 2024-09-13, head `8f907bde`, +603/−1, `mergeable_state: dirty`, last updated 2025-08-01) adds `TopicDescription.topic_id: uuid::Uuid` and the `uuid` crate, which is not in Logweir's lockfile. | crates.io sha256 `14b52c81…`, `5f1856d7…`, `d7956f9a…`; `artifacts/prod-01-4/upstream/` |
| C6 | librdkafka gives every record of a `LogAppendTime` batch the batch's MaxTimestamp, and returns every header. A live read therefore differs from the archive (E8) on both counts. | `librdkafka/src/rdkafka_msgset_reader.c:942-947` |

## 2. Definitions

- **Offset history.** For one (cluster, topic, partition), the map from offset to record.
  - Inside one continuous history, the record at an offset never changes.
  - A record can disappear from the front (retention, DeleteRecords: the log start advances).
  - It can disappear from inside (compaction), or be invisible to a reader (transaction markers).
- **Generation.** One topic incarnation, from CreateTopics to DeleteTopics. The broker assigns it a topic ID and starts its offsets at zero. CreatePartitions adds partitions to the same generation.
- **Break.** Two observations that cannot belong to one continuous history. A new generation is a break. So is a truncation below a previously observed end under the same ID (unclean leader election): offsets past the truncation point are reused by different records, and every offset-dependent consumer needs the same reaction either way.
- **Lineage key.** (source cluster ID read from the broker, topic name). A point from another cluster ID is never a predecessor.
- **Predecessor.** The newest committed point with the same lineage key.
- **Generation token.** A display and grouping key, always `lwg1:<anchor point id>`. The anchor is this point when its verdict is `break`, `suspected` or `unknown`; otherwise it is the predecessor's anchor. The topic ID is an attribute of each point, not the token, and that has two consequences:
  - a same-ID truncation still starts a new token;
  - an unchanged topic keeps its token when IDs first appear, because R1 needs two IDs, so that first comparison runs the heuristic.

## 3. The field: `topic_id`

### 3.1 Representation

- **Text form.** 22 characters: the URL-safe base64 alphabet, no padding, over the 16 big-endian bytes most-significant-half ‖ least-significant-half. This is exactly what `kafka-topics.sh --describe` prints, measured equal on all four IDs recorded (C4). Writers derive it from the two halves (`rd_kafka_Uuid_most_significant_bits` / `…least…`), **never** from `rd_kafka_Uuid_base64str` (C4).
- **Value set.** `null`, or a string that decodes to 16 bytes, re-encodes to itself, and is not the all-zero UUID `AAAAAAAAAAAAAAAAAAAAAA` (Kafka's "no ID", C3). A writer maps a zero ID to `null`.
- **Source.** `topic_id_source` is `describeTopics` (Logweir's client, §6.1) or `engineManifest` (§6.3). It is omitted when the ID is null.

### 3.2 Where it lives

| Surface | Field | Version | Written when | Absent or null means |
| --- | --- | --- | --- | --- |
| Engine manifest | `topics[].topic_id` | engine format, additive `#[serde(default)]` in `crates/logweir-engine-oso/src/vendored/manifest.rs:29` (the xtask drift gate covers that file) | only through the PROD-00.3 engine route (§6.3); Logweir never writes the engine's manifest | unknown |
| Backup receipt | `generations.<topic>.topic_id` (read at phase −1) and `.topic_id_after` (read after the engine), beside the observation block of §4.1 | `format_version` **1.1.0** | every run once PROD-02.1 lands; `null` until TI-OC1's route lands | unknown |
| Catalog point | `topics[].topic_id`, `topics[].generation` | **1.1.0** | projected from the receipt | unknown (D3 §5.2 rule 2) |
| Product API | a NEW `topics` array on `PointView`, which has none today (`crates/logweir-api/src/routes/catalogs.rs:390`), with items `{name, topicId (nullable), generation}`. `AvailablePointView.topics` stays the bounded list of names (`protection.rs:156`), and a parallel `topicGenerations` array carries the same items. | `1.0.0-alpha.N` bump (L5) | read from the catalog | `generation.verdict: "unknown"` |
| Restore evidence (15.1, 07.1) | the created target's identity: `topic_id` or creation marks | nested optional in the scorecard's `target` block (GC12 allows nested optional fields), named by those rows | at target creation | unknown |

The receipt's `generations` block is keyed by topic name. Each entry holds:

- `topic_id`, `topic_id_after`, `topic_id_source` and `partition_count`;
- per partition: `before` and `after` marks, and `archived` (first and last offset, first and last record timestamp, and up to three `tail` records with offset and fingerprint), or `null` when nothing was archived;
- `lineage`, which has these fields:
  - `previous_point_id` and `previous_receipt_sha256`;
  - `reason`, present exactly when no comparison was made: `noPredecessor` or `previousNotRecorded`;
  - `comparisons[]`, each with partition, offset, result and `read_from: archive` (§4.1);
  - `signals[]`, `verdict` and `basis`.

  `lineage` is `null` when the run was given no predecessor facts, as a manual `logweir backup run` is; the catalog sync then computes it.

Receipt fields and values are snake_case, and signal names are PascalCase as in the oracle.

### 3.3 Versioning in both verifiers and the parity script

- **Receipt format.**
  - `format_version` becomes `1.1.0`, with `schemas/logweir-backup-receipt-1.1.0.json` beside 1.0.0.
  - `generations` is optional, appended last in the Rust struct (declaration order is byte order), and `skip_serializing_if = "Option::is_none"`.
  - **The payload type stays `…backup-receipt+json;version=1.0.0`.** It names the major-1 envelope. Changing it would make every existing verifier refuse every new receipt at the payload-type comparison, which rules out rollback for an additive field. `crates/logweir-core/src/trust.rs:1010-1030` already matches the base type, so either choice leaves the trust decision intact.
- **New arms in both readers.** Arms 6–10 are appended after arm 5 and run only when `generations` is present. Messages are byte-identical in `BackupReceipt::validate_invariants` and `docs/verify_scorecard.py::check_backup_receipt_invariants`.
  - **6.** `generations` under a 1.0.x `format_version` is refused. Precedent: the scorecard's `redactions` arm, `docs/verify_scorecard.py:1208-1212`.
  - **7.** The keys of `generations` equal `source.topics`, as arm 3 does for `records`.
  - **8.** Every `topic_id` and `topic_id_after` is `null` or in canonical form (§3.1), never zero.
  - **9.** Marks and ranges are well formed:
    - `0 <= log_start <= high_watermark` before and after;
    - `first_offset <= last_offset`;
    - 1–3 tail entries, strictly descending, the first equal to `last_offset`, all at or above `first_offset`;
    - fingerprints are 64 lowercase hex.
  - **10.** When `lineage` is present, it agrees with itself and with the document's own observation:
    - the verdict is in the closed set;
    - a break-class signal is present exactly when the verdict is `break`;
    - every `ChangedDuringCapture` that the document's own marks, archived ranges and two IDs derive (§4.4) is listed;
    - the verdict is `unknown` exactly when `reason` is present and nothing broke during the capture;
    - `reason: noPredecessor` names no previous point, `reason: previousNotRecorded` names one, and a comparison (no `reason`) names the point it compared with.

    So the first receipt after the upgrade names its 1.0.0 predecessor, says `unknown` with reason `previousNotRecorded`, and verifies. The oracle's `arm10` and `lineage` are the executable definition; their pure tests include the first receipt after the upgrade.
- **Corpus and version records.**
  - `SCRIPT_VERSION` moves 1.14.0 → 1.15.0, with a row in `docs/verify-a-scorecard.md`'s version table.
  - `crates/logweir-core/tests/backup_receipt.rs:265`'s arm count moves from 5 to 10.
  - `e2e/fixtures/invariants/backup-receipt-index.json` gains at least one case per new arm. `scripts/check-invariant-corpus.sh` derives both arm lists and fails if an arm is unmatched.
- **Parity script.** `scripts/check-verifier-parity.sh`'s receipt loop gains:
  - a 1.1.0 receipt with `topic_id: null` and one with a real ID, VALID in both readers;
  - one case per new arm, INVALID in both with identical text;
  - the existing 1.0.0 receipt keeps its VALID case (old archives).
- **Why MINOR, and who confirms it.** `docs/stability.md:19-21` makes a change to an identity rule (`validate_invariants`) a MAJOR bump that needs two maintainer approvals. Arms 6–10 fire only on the new optional `generations` block. Every document without it, which is every receipt written so far, is accepted or refused exactly as before, and the existing corpus and parity cases re-prove that on every `just lint`. Arm 6 follows the reader's own precedent for a field an older format cannot carry (`docs/verify_scorecard.py:1208-1212`).
  - This record therefore argues MINOR (1.1.0). The maintainers confirm or overrule that reading under `stability.md:19-21` when PROD-02.1 lands.
  - If they read it as MAJOR, the block ships as 2.0.0, every existing reader refuses new receipts at arm 1, and the release notes must say so.
- **Catalog point.** It becomes 1.1.0 (`schemas/logweir-catalog-point-1.1.0.json`) with the payload type unchanged. `RecordTopic` gains `topic_id` and `generation` (token, verdict, basis, signals, `previous_point_id`), both informational under D3 rule 3. The catalog sync recomputes the verdict from the two receipts and marks disagreement `RecordMismatch`. Python's `--payload-type catalog-point` mode is signature-only, so it gains no arm; the record states so.
- **API.** `PointView` gains a `topics` array, new because it lists no topics today. `AvailablePointView` keeps `topics` as names and gains `topicGenerations`; changing that field from strings to objects would retype an existing field. `just schema` regenerates the document, and `crates/logweir-api/tests/contract.rs` checks the drift.

### 3.4 Absent-value behaviour for every existing archive

| Archive | Carries | Read as |
| --- | --- | --- |
| Receipt 1.0.0 (every receipt written so far) | no `generations` | Generation unknown. PROD-02.1 shows "coverage not recorded". It is never `continuous` and never the same generation as anything. Its successor names it in `previous_point_id` with verdict `unknown` and reason `previousNotRecorded`, verifies (arm 10), and anchors a new token. |
| Catalog point 1.0.0 | no `topic_id`, no `generation` | unknown (rule 2) |
| Receipt 1.1.0 with `topic_id: null` | observation only | heuristic only; never an ID match |
| Engine manifests (v0.19–v0.22, OSO operators' archives) | no ID | unknown |
| Foreign archives imported through PLAT-15.2 | no receipt | Unknown. Restores work as today, and offset-dependent consumers refuse to use offsets across points. |
| Restore evidence and checkpoints written before 07.x and 15.1 | no target identity | unknown; never resumable (§7) |

## 4. The heuristic usable now

### 4.1 Inputs, per (lineage key, partition), for run *k*

- `P_k`: the partition count from Metadata at phase −1.
- `before_k` and `after_k`: the log start offset and high watermark, read at phase −1 and right after the engine exits, with `isolation.level=read_uncommitted` (the engine's level, E7).
- `archived_k`: read from this run's manifest.
  - The first offset (the lowest segment start) and the last offset `L_k` (the highest segment end).
  - The first and last record timestamps.
  - `tail_k`: the up to three highest archived offsets, each with its **source-equivalent fingerprint**. That is `record_fingerprint` (`crates/logweir-kafka/src/fingerprint.rs:9-39`) over the record as Logweir's decoder reads it (`crates/logweir-engine-oso/src/kbak.rs:68`), minus the two *trailing* headers. They are removed only when they are `x-original-offset` equal to the record's own offset and `x-original-timestamp` equal to its own timestamp. A record that carried such headers at the source keeps its own inner pair.
- `topic_id_k` and `topic_id_after_k`, once TI-OC1 delivers them; `null` until then.
- **Comparison read: archive against archive.** At run *k*, offset *o* of the predecessor's tail is read from run *k*'s OWN archive. The result is `At(fingerprint)` when that archive holds a record at exactly *o*, and `Absent` otherwise. An offset below `LS_k` never reaches the read (R6).
  - **What capture preserves, and why both sides must come from it.** The engine archives key and value bytes, null versus empty, and every offset exactly, transaction markers included (E6). It does not keep two things (E8):
    - a `LogAppendTime` batch's append time: the record carries the producer's CreateTime;
    - a repeated header key: it keeps one entry, at the first position with the last value.

    Two captures of the same unchanged offset lose exactly the same things, so the two sides stay equal. A live read does not: librdkafka reports the batch's append time and every header (C6). On an unchanged topic of either kind, a live-read comparison reports `BoundaryRecordChanged` at every capture (measured: c17 and c18).
  - Both sides are archived bytes, so anyone holding both points can recompute the comparison.
  - A run that does not re-archive *o* (PROD-02.2's incremental runs) overlaps its predecessor by at least one record.
  - A source read is not a comparison this contract allows. It becomes one only once the engine keeps `LogAppendTime` and repeated keys, which are PROD-01.1's proposed PROD-00.3c and PROD-00.3e.
- Timestamps are recorded for display and for time-window guards. They are **not** a detection signal (§4.5, c12).

### 4.2 The rule (run *k* against predecessor *j*)

1. **R0 predecessor.** With no predecessor, or one without generation data, no comparison is made: the lineage gets a `reason` (§3.3 arm 10), and the verdict is `unknown` unless run *k* changed within itself (§4.4).
2. **R1 IDs.** When both IDs are non-null and differ ⇒ `TopicIdChanged` (break), and nothing else is compared. When they are equal, R2–R6 all still run:
   - R5 still reports gaps;
   - R6 still catches a same-ID truncation refilled past the old end.

   Equal IDs change only the verdict (item 8). One ID alone decides nothing.
3. **R2 partitions.** `P_k < P_j` ⇒ `PartitionCountDecreased` (break); Kafka never removes a partition from a live topic. `P_k > P_j` ⇒ `PartitionCountIncreased` (not a break by itself).
4. **R3 start.** For each partition in both: `LS_k.before < max(LS_j.before, LS_j.after)` ⇒ `LogStartRegressed` (break).
5. **R4 end.** `HW_k.before < max(HW_j.before, HW_j.after, L_j + 1)` ⇒ `EndRegressed` (break).
6. **R5 gap.** `LS_k.before > L_j + 1` ⇒ `CaptureGap [L_j+1, LS_k.before)`: records produced after *j* and deleted before *k*. This is not a break.
7. **R6 boundary.** Over `tail_j`, newest first, reading run *k*'s archive (§4.1):
   - an offset below `LS_k` ⇒ `BoundaryDeleted`, stop;
   - an offset at or above `HW_k` ⇒ skip (R4 fired);
   - `At` with an equal fingerprint ⇒ `BoundaryRecordVerified`, stop;
   - `At` with a different fingerprint ⇒ `BoundaryRecordChanged` (break), stop;
   - `Absent` ⇒ `BoundaryRecordAbsent`, then try the next older candidate;
   - `OutOfRange` ⇒ `BoundaryDeleted`, stop. Only a live read can return it: the log start moved between the marks and the read.
8. **Verdict.**
   - Any break signal, including run *k*'s own within-run signals (§4.4) ⇒ `break`.
   - Otherwise equal IDs ⇒ `continuous`, basis `topicId`.
   - Otherwise `PartitionCountIncreased` with nothing verified ⇒ `suspected`.
   - Otherwise any `BoundaryRecordVerified` ⇒ `continuous`, basis `content`.
   - Otherwise `unverified`, basis `watermarks`.

`classify` in `e2e/tests/topic_identity.rs` implements R0 (as a cluster mismatch), R1–R6 and the verdict. `lineage` and `arm10` implement the predecessor rule and arm 10. Product code must agree with them on every oracle row (TI-02.1-3).

### 4.3 Verdicts

| Verdict | Meaning | Who may rely on it |
| --- | --- | --- |
| `continuous` (basis `topicId` or `content`) | Same history, verified. Offsets of *j* and *k* denote the same records. | Everyone, including offset-dependent consumers across the two points. |
| `unverified` (basis `watermarks`) | No break signal, and nothing left to compare: every tail record is deleted or compacted. Normal when retention is shorter than the backup interval. | Display and coverage (the token continues). Offset-dependent consumers must not use offsets across this link. |
| `suspected` | Partitions increased, nothing verified, and no two IDs prove the topic. CreatePartitions and a recreation with more partitions look alike (c16 and c19). | Treated as `break` by every consumer except display. |
| `break` | Definite, within §4.6's false positives. | Every consumer: new token, no reuse of offsets across it. |
| `unknown` | No comparison was made: no predecessor, or one without generation data. | Treated as a new lineage; old points are never merged into it. |

### 4.4 The within-run check

Run *k* alone flags `ChangedDuringCapture` when any of these holds:

- for a partition: `LS.after < LS.before`, or `HW.after < HW.before`;
- for a partition: the first archived offset is below `LS.before`;
- for a partition: `L_k >= HW.after`;
- for the topic: `topic_id` (phase −1) and `topic_id_after` (after the engine) are both non-null and differ.

Such a topic's verdict in that point is `break`, and the point is not selectable for that topic (§7). The receipt records every input this check needs, both marks, the archived range and both IDs, so arm 10 re-derives it from the document alone and refuses a receipt that hides the flag.

### 4.5 Input requirements, each measured

| Requirement | What goes wrong without it | Row |
| --- | --- | --- |
| Marks read READ_UNCOMMITTED | With a transaction open, READ_COMMITTED marks stop at the last stable offset (5) while the engine archived to offset 7. The result is a false `break` and a false within-run flag. | c11 |
| Source-equivalent fingerprints | They keep a receipt's tail fingerprints a fact about the SOURCE record. So they stay stable if the engine's appended headers change, and a live read can be compared with them once PROD-00.3c and PROD-00.3e allow one. The first round measured the alternative against a live read: across 102 archived tail records on 3.7.1, **none** of the verbatim fingerprints equalled the source record's, which gave a false `break` on all four verified same-topic rows (c05, c06, c11, c12) and missed the original-name restore (c15). | all (first round) |
| Read at *exactly* the offset | After compaction, a read from offset 19 returns offset 20, a different record. "First record from *o*" reads compaction as a new topic. | c09 |
| Up to three tail candidates | Compaction made all three newest candidates absent. | c09 |
| Archive against archive (§4.1) | A live read reports a `LogAppendTime` batch's append time where the archive holds the producer's CreateTime, which gives a false `break` at every capture of an unchanged topic. | c17 |
| Archive against archive (§4.1) | A live read returns every header where the archive keeps one per key, which gives a false `break` whenever a tail record repeats a key. | c18 |
| Timestamps are not a signal | Records produced after the capture carry *older* timestamps in the same topic. | c12 |
| Canonical ID text from the two halves | librdkafka's helper returns another alphabet for the same ID. | FFI evidence (C4) |

### 4.6 Known false positives (a break or `suspected` reported, same generation)

- **FP1: same-ID truncation.** Unclean leader election, or a lagging replica elected leader, can regress the end or the log start. Consumers need the break anyway (§2). With IDs available it is labelled "history truncated", not "recreated". Not measured: the fixtures have one broker.
- **FP2–FP5: the naive variants of §4.5.** These are READ_COMMITTED marks (c11), a verbatim archive fingerprint compared with a live read (the first round), an inexact read (c09) and timestamp rules (c12). All four are measured, and the input requirements exclude them.
- **FP6 and FP7: a source read on a `LogAppendTime` topic, or on a tail record that repeats a header key.** Measured: every source-read comparison on c17 and c18 reported `break`. The rule reads the archive and is immune: both rows are `continuous`, and TI-02.1-8 pins it. This matches PROD-01.1's measured S3 and S4.
- **FP8 (known, by design): partitions added, then every old tail deleted before the next capture.** Measured on c19: `suspected`.
  - **Trigger:** CreatePartitions, then retention, DeleteRecords or compaction past every old tail before the next capture. That is routine on a topic whose retention is shorter than the backup interval.
  - **Impact:** `suspected` reacts like `break`. Coverage starts a new token and the console says "possible recreation".
  - **Mitigation:** IDs; c19's ID path is `continuous`.
- **FP9 (possible, not measured): a leader move right after retention.** A follower adopts the leader's log start only from fetch responses (Kafka 3.7.1 `core/src/main/scala/kafka/server/ReplicaFetcherThread.scala:127`, `:137`; fetched, sha256 `5ad1a3a7…`). A leader change inside one fetch round-trip after retention on the old leader can therefore report a lower EARLIEST, giving `LogStartRegressed`. The window is one fetch interval, and the fixtures have one broker.
- **Not measured, and no false positive expected from source:**
  - tiered storage: librdkafka queries EARLIEST, the global log start;
  - follower fetching: watermarks are queried from the leader.

### 4.7 Known false negatives (a new topic not detected)

- **FN1: recreated, refilled past the old end, and every compared offset deleted before the next capture** (measured, c13: `unverified`). The `CaptureGap` it reports is really a generation boundary. The same holds when the new generation compacts the compared offsets away.
- **FN2: byte-identical replay** (measured, c14: `continuous`). The same keys, values, headers and timestamps land at the same offsets, as with a restore that strips Logweir's offset headers, or a replay tool that keeps timestamps.
- **FN3: recreation during a capture, when the new generation already extends past the engine's position.** The engine reads on (E4), and the marks need not regress. From source, not measured. Only IDs read before and after the engine close it.
- **FN4: no usable predecessor** (the first run after the upgrade, or a 1.0.0 predecessor). The verdict is `unknown`, which no consumer treats as `continuous`.
- **FN5: several recreations between two captures.** They are one break; the heuristic cannot count generations.

The ID path closes FN1 and FN2 where the broker supplies IDs (measured: c13 and c14 are `break` with `TopicIdChanged`), and FN3 through the within-run ID check.

## 5. Evidence: measured outcomes

Run with `LOGWEIR_TOPIC_IDENTITY_EVIDENCE=<file> cargo test --locked -p e2e --features e2e --test topic_identity -- --include-ignored --test-threads=1` against `just e2e-up`'s broker. Each row reads the broker's topic ID before and after both captures, and asserts it before any verdict.

- **Fix round**, at `d22dd170`, on 2026-09-29 (UTC). The comparison is archive against archive, and every row records four modes.
  - 3.7.1 (image `sha256:ed74d7d1…9b68`): 53 of 53 tests in 518 s — 33 pure tests, 19 live rows and the L9 row.
  - 4.3.1 (image `sha256:77e3df90…2837`): 53 of 53 in 536 s. Every mode of every row, and every classification, is equal to 3.7.1 (`oracle-fix-summary.txt`).
- **PROD-01.5c**, with the oracle as at main `64b66a15` (branch commit `fa7a9b0b`), on 2026-10-10 (UTC): the same command on compose slot 2, one line at a time, with engine `0.23.3+logweir.2` run natively from the published linux/arm64 runner image (the fix round ran 0.21.0 under emulation).
  - 3.7.1, 3.9.2, 4.1.2 and 4.3.1 (the digest-pinned images of `stack-env.sh --kafka`): 53 of 53 on each line, in 820, 1,079, 1,153 and 1,041 s on a loaded host.
  - Every mode of every row, with its signals, and every classification is equal on the four lines and equal to the fix round's 3.7.1 run (`artifacts/prod-01-5c/topic-identity-summary-all-lines.txt`). The engine change moved no verdict.
  - c10's retention check deleted its segment after 102 to 227 s.
- **First round**, three runs of c01–c16 (3.7.1 twice, 4.3.1 once). They used the source-read comparison and gave the same rule verdicts; the source and archive reads agree on every row except c17 and c18, which that round did not have (§11).

The offsets-only column is `classify` without the boundary comparison. The ID path is `classify` given the broker's IDs, standing in for PROD-01.4a.

| Row | Situation | Topic ID after | Offsets only | **Rule** (archive) | Source read | ID path | Outcome of the rule |
| --- | --- | --- | --- | --- | --- | --- | --- |
| c01 | recreated, 3 → 3 partitions, fewer records | changed | break | **break**: EndRegressed ×3 | break | break | detected |
| c02 | recreated, 3 → 3, refilled past the old end | changed | unverified | **break**: BoundaryRecordChanged ×3 | break | break | detected (comparison only) |
| c03 | recreated, 3 → 1 partition | changed | break | **break**: PartitionCountDecreased, BoundaryRecordChanged | break | break | detected |
| c04 | recreated, 3 → 5 partitions, refilled | changed | suspected | **break**: PartitionCountIncreased, BoundaryRecordChanged ×3 | break | break | detected |
| c05 | CreatePartitions 3 → 5 (same topic) | same | suspected | continuous: PartitionCountIncreased, BoundaryRecordVerified ×3 | continuous | continuous | correct |
| c06 | DeleteRecords inside the archive (to 5, 9, 10) | same | unverified | continuous: BoundaryRecordVerified ×2, BoundaryDeleted | continuous | continuous | correct |
| c07 | produced 10 more, DeleteRecords to 15 | same | unverified | unverified: CaptureGap [10,15) ×3, BoundaryDeleted ×3 | unverified | continuous, the same gaps | correct, gap reported |
| c08 | DeleteRecords to the end (log start = end) | same | unverified | unverified: BoundaryDeleted ×3 | unverified | continuous | correct |
| c09 | compaction removed offsets 17–19 | same | unverified | unverified: BoundaryRecordAbsent ×3 | unverified | continuous | correct |
| c10 | retention expiry (retention check after 86–286 s over five runs) | same | unverified | unverified: CaptureGap [10,15), BoundaryDeleted | unverified | continuous, the same gap | correct, gap reported |
| c11 | open transaction during and after both captures | same | unverified | continuous: BoundaryRecordVerified | continuous (READ_COMMITTED marks: `break`) | continuous | correct |
| c12 | later records carry older timestamps | same | unverified | continuous: BoundaryRecordVerified | continuous | continuous | correct |
| c13 | recreated, refilled, DeleteRecords past the old tail | changed | unverified | unverified: CaptureGap [10,12), BoundaryDeleted | unverified | break | **known miss (FN1)**; the ID path detects it |
| c14 | recreated, byte-identical replay | changed | unverified | continuous: BoundaryRecordVerified | continuous | break | **known miss (FN2)**; the ID path detects it |
| c15 | original-name restore, emulated with headers kept | changed | unverified | **break**: BoundaryRecordChanged | break | break | detected (comparison only) |
| c16 | recreated, 3 → 5, nothing left to compare | changed | suspected | **suspected**: PartitionCountIncreased, CaptureGap ×3, BoundaryDeleted ×3 | suspected | break | detected as suspected |
| c17 | unchanged `LogAppendTime` topic | same | unverified | continuous: BoundaryRecordVerified | **break** | continuous | correct; the source read's false break (FP6) |
| c18 | unchanged topic whose tail repeats a header key | same | unverified | continuous: BoundaryRecordVerified | **break** | continuous | correct; the source read's false break (FP7) |
| c19 | CreatePartitions 3 → 5, then every old tail deleted | same | suspected | **suspected**: PartitionCountIncreased, CaptureGap ×3, BoundaryDeleted ×3 | suspected | continuous, the same gaps | **known false positive (FP8)**; the ID path clears it |

Totals, identical on both broker lines, and on all four in PROD-01.5c's runs:

- **New-topic rows (8):**
  - the rule detects 6, as 5 `break` and 1 `suspected`, and misses 2 (c13, c14);
  - offsets only gives 2 `break` and 2 `suspected`;
  - the ID path gives `break` on all 8.
- **Same-topic rows (11):**
  - the rule gives no false `break`, and one known false positive (c19, `suspected`);
  - a source read gives a false `break` on c17 and c18;
  - offsets only gives `suspected` on c05 and c19;
  - the ID path gives no break, and keeps every gap.
- **The L9 row:** the engine dials a fake broker that never answers. At its deadline, the client and the engine's container are killed, on both lines. With the container cleanup removed, the row fails: the container was still running 30 s after the deadline (`l9-negative-control.log`).
- **Guards on the oracle itself:**
  - ground truth is asserted before any verdict;
  - 33 pure tests run with no broker and no feature;
  - the fix round's mutant pass killed 25 of 25, including the review's R1–R5 (`rule-mutants-fix.log`).

## 6. Route to real topic IDs

### 6.1 An FFI exception for librdkafka's admin calls

- **Scope: one place for all `unsafe` librdkafka calls.** Two rows need calls that rdkafka 0.36.2 does not wrap: this row's DescribeTopics, and PROD-04.0's group, offset and ACL operations. The locked librdkafka binds all of them (`rdkafka-sys-4.10.0+2.12.1/src/bindings.rs`):
  - `rd_kafka_DescribeTopics` `:2817`;
  - `rd_kafka_ListConsumerGroups` `:2926`, `rd_kafka_DescribeConsumerGroups` `:2983`, `rd_kafka_ConsumerGroupDescription_type` `:3034`;
  - `rd_kafka_ListConsumerGroupOffsets` `:3141`, `rd_kafka_AlterConsumerGroupOffsets` `:3179`;
  - `rd_kafka_CreateAcls` `:3503`, `rd_kafka_DescribeAcls` `:3518`.

  Two shapes are possible (§6.4):
  - **(a1)** a private module inside `logweir-kafka`. `#![forbid(unsafe_code)]` cannot be relaxed inside the crate, so the root becomes `#![deny(unsafe_code)]`, with `#[allow(unsafe_code)]` on that one module.
  - **(a2)** a small FFI crate, the shape PROD-04.0's own text names, that alone allows `unsafe`. `logweir-kafka` depends on it and keeps `forbid`.

  Either way, the safe surface is a method on `RdKafkaReader`, plus a `ClusterReader` method whose default returns `Ok(None)` for test fakes. No gate greps for the attribute; the change is visible in review.
- **Cost, measured with the throwaway prototype** (`artifacts/prod-01-4/ffi-probe/src/main.rs`, 123 lines, DescribeTopics only):
  - two `unsafe` sites: a one-line string helper (`main.rs:25`) and one 56-line block (`:37-92`);
  - **no new third-party crate and no new C code**: `rdkafka::bindings` is `rdkafka_sys::bindings` (C1, MIT), and librdkafka 2.12.1 (BSD 2-clause, already attributed in `NOTICE`) is already linked;
  - a fresh build took 26.66 s with the pinned toolchain (`artifacts/prod-01-4/ffi-probe-build.log`);
  - it returned the broker's own IDs for two generations on 3.7.1 and on 4.3.1, and `null` plus "Unknown topic or partition" for an absent topic.
- **Safety obligations.**
  - Destroy each of the collection, options, queue and event exactly once.
  - Copy strings before the event is destroyed.
  - Topic names carry no NUL.
  - Keep the queue poll bounded.
  - Map zero to `null`.
  - Derive the text from the two halves (C4).
- **Tests.**
  - A soak test (thousands of calls with a bounded resident set).
  - FFI IDs equal to the CLI's on every oracle row and both broker lines.
  - An ID that contains `_` or `-` is recorded with those characters.
  - Tier A review, and an ADR 0004 amendment that states the exit condition (§6.2).
- **What it delivers.**
  - IDs at phase −1 and after the engine, which closes FN1–FN3 wherever the broker supplies them.
  - IDs in topic discovery (the check runner).
  - Restore-target identity for 07.x and 15.1.
- **Limits.**
  - DescribeTopics needs Describe on the topic, like Metadata.
  - Brokers below inter-broker protocol 2.8 return zero, which is read as `null`, so the heuristic stays as the fallback.

### 6.2 An upstream wrapper

- No rust-rdkafka release wraps DescribeTopics, through 0.39.0 (C5).
- PR #721 would add it with a `uuid::Uuid` field. It has conflicted with the base branch since 2024 and has no maintainer review. A third-party rebased fork exists only as a git branch. Every one of the 389 packages in Logweir's lockfile resolves from crates.io, and a git dependency would be the first.
- **Cost if it ships:**
  - an rdkafka bump from 0.36 (0.38.0 changed `OwnedDeliveryResult`, a breaking API change);
  - a new dependency on `uuid` (MIT OR Apache-2.0), which needs notices regeneration and `cargo deny`;
  - the same text-form rule (C4), whatever the wrapper renders.
- **Timeline:** outside Logweir's control. Logweir can rebase PR #721 and ask for review.
- This is the exit path for §6.1, not a route by itself.

### 6.3 The engine, through PROD-00.3

- The engine would need Metadata v10 or later. It does not negotiate API versions (fixed table, `kafka/client.rs:588-611`), so raising the version changes which brokers it can reach; that is PROD-00.1's ApiVersions row.
- It would also have to carry the ID into `topics[].topic_id` in the manifest. Logweir's vendored reader and the drift gate would follow (§3.2).
- It yields IDs only for what the engine captured. Phase −1, discovery and restore targets still need Logweir's client.
- The route itself (upstream PR or fork) is OD-3's decision, and PROD-00.1's capability table holds the row "topic IDs". This record only cross-references it: **identity does not need it** if §6.1 is taken.

### 6.4 Owner choice TI-OC1: one scoped-`unsafe` policy for librdkafka (presented, not taken)

PROD-04.0 (not yet dispatched) must choose a path for group, offset and ACL calls. One of its options is "an FFI crate with a scoped `unsafe` exception", together with an ADR 0004 amendment. This row needs the same kind of exception for DescribeTopics. So the owner decides ONE policy for `unsafe` librdkafka calls, and it binds both rows. PROD-04.0 then chooses per operation within it, and needs no second ruling on `unsafe`.

| Option | Delivers | Cost and risk |
| --- | --- | --- |
| **(a2) One FFI crate for every librdkafka call the safe API lacks (recommended)** | IDs wherever the broker has them (capture, discovery, restore targets), and PROD-04.0's group, offset and ACL calls through the same reviewed perimeter | `logweir-kafka` and every other product crate keep `forbid(unsafe_code)`. One more workspace package, which the Global Constraint 38 comments count (`crates/logweir-kafka/Cargo.toml:15`). One ADR 0004 amendment covers both rows. Tier A review, a soak test per call family, and an exit condition per call. |
| (a1) The same policy as a private module inside `logweir-kafka` | The same | No new package, but the crate drops to `deny` + one `allow` for as long as any wrapper lives there. Suits DescribeTopics alone; it scales worse to PROD-04.0's larger call set. |
| (b) Wait for upstream (contribute to rust-rdkafka PR #721, and to wrappers that do not exist yet for groups and ACLs) | The same, safely | Unknown date. An rdkafka bump plus the `uuid` crate. FN1–FN3 and FP8 stay open until then, and PROD-04.0 waits too. |
| (c) Engine route: PROD-00.3 for IDs, Amendment D for group offsets | Archive-side IDs only; group offsets through the engine's subcommand | An engine patch or fork, a protocol-version change, no target-side IDs. Amendment D currently denies the engine's `offset` subcommand. |
| (d) A raw-protocol client (PROD-04.0's option) | IDs from Metadata v10+, and any admin call | A new dependency, and SASL, SCRAM and TLS reimplemented beside librdkafka. ADR 0004 prefers existing clients. |
| (e) No `unsafe`: the heuristic only, and PROD-04.0 limited to the safe consumer API | Nothing new | FN1–FN3 and FP8 stay. PROD-15.1 must refuse header stripping forever. PROD-04.0 loses group types and offset alteration. |

**Recommendation:**
- Take (a2) as the one policy. Its first wrapper is DescribeTopics (PROD-01.4a).
- Plan (b) as the exit, call by call: a wrapper leaves the FFI crate once a released rdkafka offers it safely.
- Keep the heuristic as the fallback for `null` IDs and for truncation.
- (c) is not needed for identity.

## 7. Consumer reactions

Common to all six rows:

- **Generation token.** Points are grouped by it.
- **Offsets across two points.** A consumer that uses offsets from two different points requires either every catalog link between them to be `continuous`, or equal non-null topic IDs and no break signal.
- **Verdicts.** `suspected` reacts like `break`, and `unknown` like a new lineage.
- **Timestamps** never place a boundary.
- **Target identity (04.2, 07.1, 11.1, 15.1).** Both sides of a target comparison are live reads by the same reader, READ_UNCOMMITTED. One is taken at creation or checkpoint; the other at cutover, resume or teardown. Neither side is ever an archive fingerprint compared with a live read (§4.1).
  - Restored targets are `CreateTime` topics (Logweir creates them so, `crates/logweir-kafka/src/reader.rs:353-356`), and they carry the archive's already-collapsed headers, so live reads of them are stable.
  - The rule still holds for any other target.

### PROD-02.1 — Show honest coverage for scheduled backups

- **What each capture records.** The observation of §4.1 and the lineage of §4.2 in receipt 1.1.0 (`generations`), projected into catalog 1.1.0 and the API.
  - The runner receives the predecessor's facts as an additive block of `execution-inputs.json` (D-SEAMS S4): point ID, receipt sha256, marks, last offsets, tail fingerprints and topic IDs. This adds no runner read authority.
  - When that input is absent (a manual `logweir backup run`), the receipt says `lineage: null` and the catalog sync computes the lineage later.
  - Watermarks come from a new READ_UNCOMMITTED read of the log start and high watermark. The existing `end_offsets` callers are unchanged.
  - The comparison reads the capture's own archive (§4.1), so the runner decodes the tail segments it just wrote.
  - The first capture after the upgrade names its 1.0.0 predecessor in `previous_point_id`, with verdict `unknown` and reason `previousNotRecorded`. It verifies (arm 10) and anchors a new token.
- **On `break` or `suspected`:**
  - the point starts a new token;
  - coverage, gaps and freshness never span the boundary (`maxRecoveryPointAgeSeconds` is evaluated over the current token's points only);
  - earlier points stay listed as "earlier generation";
  - label: "topic recreated or truncated since <point>", or "possible recreation: partitions added and nothing left to compare";
  - evidence fields: `generations.<t>.lineage.{verdict,signals}` and `topics[].generation`.
- **On `unverified`:** the token continues; label "continuity not verifiable: no overlap with the previous point"; `CaptureGap` renders as a per-partition gap.
- **On `unknown`:** a new lineage. Old (1.0.0) points read "coverage not recorded, generation unknown", never "complete".
- **On a within-run `ChangedDuringCapture`:**
  - the point is `break` for that topic;
  - recovery-point selection does not offer it for that topic (refusal reason `GenerationChangedDuringCapture`);
  - label: "topic changed during capture".

### PROD-04.1 — Archive consumer position evidence

- **Binding.** Every captured position records the topic's generation token, `topic_id` (nullable) and the marks read at group-capture time, in the same run as the data.
- **`PositionBeyondEnd`.** A committed offset above that partition's end at capture is excluded with reason `PositionBeyondEnd`. This is the state a consumer leaves when it commits its old position after its topic is recreated underneath it. The broker accepts such a commit. Measured on 3.7.1: offset 10 on a partition ending at 4 was committed, and the broker's group view then showed lag −6 (`artifacts/prod-01-4/commit-above-end-evidence.txt`). From source, neither version's commit path compares the offset with the log end (`GroupMetadataManager.scala:453-475` at Kafka 3.7.1; `OffsetMetadataManager.java:617-660` at 4.3.1).
- **`GenerationChangedDuringCapture`.** When the topic shows `ChangedDuringCapture`, or its group-capture marks regress against the post-run marks, the affected groups are `failed` with that reason.
- **Old snapshots.** A snapshot taken from an old point relates to "generation unknown". Absence is still never offset zero.

### PROD-04.2 — Translate positions and perform reviewed cutover

- **Source side.** Mapping requires the group snapshot and the restored data to share a token, and every link between their points to be `continuous`. Otherwise every affected mapping is `unavailable`, with reason `GenerationMismatch` (different tokens) or `ContinuityUnverified` (an `unverified` link).
- **Target side.**
  - The restore records each target topic's identity at creation: its `topic_id` when available, else its creation marks and the fingerprint of the last restored record.
  - Cutover re-reads that identity and refuses with `TargetGenerationChanged` before applying any offset if it changed.
- **Original-name targets (PROD-15.1).** Positions always go through `x-original-offset`, never copied because the names match.

### PROD-07.1 — Resolve checkpoint and delivery semantics

- **Binding.** Every checkpoint binds the plan hash, the execution ID, the point ID and manifest sha256, and a per-target identity: `topic_id` when available, else the creation marks, the last acknowledged offset and the fingerprint of the record at that offset.
- **Resume.** Resume re-reads that identity. An ID change, an end below the acknowledged offset, a log start above the checkpointed first offset, or a different record at the acknowledged offset refuses with `TargetGenerationChanged`, and the retry uses a fresh target (PLAT-12.2). The offsets-only check alone does not suffice: on c02 (a topic recreated and refilled past the old end) it reports `no break`.
- **Old checkpoints.** Ephemeral checkpoints from before this contract carry no identity. They are `unknown` and never resumable.

### PROD-11.1 — Add replay selection and safe clones

- **One point.** A topic with `ChangedDuringCapture` in the selected point is refused in both preview and execution (`GenerationChangedDuringCapture`).
- **Several points** (after PROD-02.2 chains): a window never crosses a link that is not `continuous`. The preview shows the boundary and refuses or splits at it.
- **Timestamps** never place the boundary (c12).
- **Clones.** Each clone records its target's identity at creation. Teardown deletes a clone only while that identity is unchanged, and otherwise skips it with reason `CloneTargetReplaced`.

### PROD-15.1 — Restore under the original name into an absent topic

- **Evidence.**
  - The point's generation token and `topic_id` for the source topic are recorded as `source_generation`.
  - After the exclusive create, the new target's identity (`topic_id` when available, else creation marks) is recorded.
  - The evidence says "restored as a new generation of <name>" and never claims the original identity: Kafka assigns IDs, and none can be preserved.
- **Header stripping.** Original-name restores keep `strip_offset_headers: false`.
  - With headers kept, the next capture reports `break` (c15).
  - With them stripped it reports `continuous` (c14, a known miss), which would merge the restored topic into the pre-deletion history.
  - So `strip_offset_headers: true` is refused for an original-name mapping while the point's `topic_id` is null.
- **Positions.** Consumer positions follow PROD-04.2's mapping, never copied.

## 8. Acceptance rows

Every negative control below can fail. Fixture row numbers refer to `e2e/tests/topic_identity.rs`.

### PROD-02.1

| # | Pass predicate | Negative control | Fixture |
| --- | --- | --- | --- |
| TI-02.1-1 | A 1.1.0 receipt with `generations` verifies VALID in `logweir drill verify` and `docs/verify_scorecard.py` in three forms: with `topic_id: null`, with real IDs, and with `lineage: null`. The existing 1.0.0 receipt stays VALID. | One corpus case per refusal, each refused by both readers with identical text: `generations` under 1.0.0 (arm 6); a missing topic key (7); the zero UUID, or `Cf6zT/mcTNCoxuPmv1Ztxw` as `topic_id` or `topic_id_after` (8); a tail above `last_offset` (9); `continuous` beside `EndRegressed` (10). Deleting an arm from one reader fails `check-invariant-corpus.sh`. | `backup-receipt-index.json`; `check-verifier-parity.sh` |
| TI-02.1-2 | During an open transaction the product records READ_UNCOMMITTED marks, and the next point's verdict is not `break`. | A build that reads the marks at librdkafka's default isolation produces `break` on c11. The oracle measured exactly this, so the row must fail on that build. | c11 |
| TI-02.1-3 | For c01–c19 (c10 with `--ignored`), the product verdict and its break-class signals, computed from two real `logweir backup run` receipts, equal `classify`'s. | Two builds fail, both measured. One without the boundary comparison (offsets only) gives `unverified` on c02 and c15 and `suspected` on c05. One that compares against a source read gives `break` on c17 and c18. | oracle rows, re-pointed at the runner |
| TI-02.1-4 | After c02's recreation is detected, the API shows two tokens. The covered window and freshness of the current token start at the post-recreation point. | An implementation that merges windows by topic name shows one window starting at the pre-recreation point, and fails the window-start assertion. | c02 plus a `ProtectionPolicy` |
| TI-02.1-5 | Every 1.0.0 point reads `generation: unknown` and "coverage not recorded". The first 1.1.0 receipt after a 1.0.0 predecessor names it in `previous_point_id`, says `unknown` with reason `previousNotRecorded`, and verifies VALID in both readers. | Both readers refuse the same receipt with reason `noPredecessor`, and refuse it with `previousNotRecorded` but no `previous_point_id` (arm 10). Rendering a 1.0.0 point as `continuous` or "complete" fails. | corpus cases; `e2e/fixtures/signed/backup-receipt.json`; the oracle's `the_first_receipt_after_the_upgrade_names_its_predecessor_and_verifies` |
| TI-02.1-6 | One receipt per within-run condition, five in all: the log start regressed, the end regressed, an archived offset below the pre-run log start, an archived offset at or beyond the post-run end, and a changed topic ID. Each carries `ChangedDuringCapture` and `break`, and selection does not offer the point for that topic. | Each receipt with its flag suppressed is refused by both readers (arm 10). Deleting any one condition from one reader fails the corpus gate. | five corpus cases, built from the oracle's `intra_run_flags_*` pure tests; c11's READ_COMMITTED marks for the end condition |
| TI-02.1-7 | c07 and c10 record `CaptureGap [10,15)` per partition in the receipt, the catalog and the API. | c06 (log start exactly one past the tail) records none. | c06, c07, c10 |
| TI-02.1-8 | The product compares archive against archive: on c17 (`LogAppendTime`) and c18 (a repeated header key), the next point's verdict is `continuous`. | A build that compares against a source read reports `break` on both rows. Each row's source-read variant measures exactly that, so the control fails such a build. | c17, c18 |
| TI-02.1-9 | Once PROD-01.4a lands: with equal IDs, a point still reports `CaptureGap` per partition, and a same-ID truncation refilled past the old end is still `break`. Measured on the ID path: c07's `[10,15)` ×3 gaps and `continuous`. | An ID path that stops at R4 drops c07's gaps and misses the refill. The oracle's `equal_ids_still_report_a_capture_gap` and `equal_ids_still_catch_a_refilled_truncation` kill exactly that mutant (N2). | c07 (ID path); pure tests |

### PROD-04.1

| # | Pass predicate | Negative control | Fixture |
| --- | --- | --- | --- |
| TI-04.1-1 | Every captured position carries a token, `topic_id` (nullable) and the group-capture marks. | A snapshot entry without a token is refused by the snapshot validator. | compose: one group on a c06-style topic |
| TI-04.1-2 | A committed offset of 10 on a partition whose end is 4 (c01's recreation, then a commit through the old position) is `excluded: PositionBeyondEnd`. | A commit exactly at the end (4) is `captured`. | c01 steps plus a group |
| TI-04.1-3 | A run whose group-capture marks regress against its post-run marks fails the affected groups with `GenerationChangedDuringCapture`. | The same run with stable marks captures them. | unit test over recorded marks |
| TI-04.1-4 | A snapshot from a point without generation data relates to "generation unknown". | Presenting it as related to a 1.1.0 point's data fails. | 1.0.0 receipt fixture |

### PROD-04.2

| # | Pass predicate | Negative control | Fixture |
| --- | --- | --- | --- |
| TI-04.2-1 | A snapshot from the pre-recreation point and data from the post-recreation point give `unavailable: GenerationMismatch` for every affected partition. | An implementation that maps by topic name reports `exact` mappings and fails. | c02 plus a group |
| TI-04.2-2 | Recreating the restore target between restore and cutover refuses the cutover with `TargetGenerationChanged` before any offset is applied. | With the target unchanged, cutover proceeds. | compose restore; c01 steps on the target |
| TI-04.2-3 | An original-name target maps through `x-original-offset`. | A restore from a point whose log start is 5 makes target offsets differ from source offsets by 5. An implementation that copies offsets when names match lands 5 records off and fails. | c15 emulation with c06's log start |

### PROD-07.1

| # | Pass predicate | Negative control | Fixture |
| --- | --- | --- | --- |
| TI-07.1-1 | 07.1's decision binds every checkpoint to the target identity of §7, and its failure-state table has a row "target recreated between attempts": refuse, then a fresh target. | Within 07.1, the orchestrator applies c02's measured outcome to the decision's identity rule. An identity made only of offsets and marks reports `no break` on c02 (measured), so a decision without the exact-offset fingerprint or the ID fails this row. | c02's recorded evidence; for PROD-07.3's live row, c02's steps applied to a restore target |
| TI-07.1-2 | A checkpoint without an identity block is `unknown` and never resumed. | A pre-contract checkpoint accepted for resume fails. | a checkpoint fixture with the block removed |
| TI-07.1-3 | An end below the acknowledged offset under the same ID (truncation) refuses resume. | Equal marks with a matching fingerprint resume. | unit test over recorded identities (a single broker cannot truncate) |

### PROD-11.1

| # | Pass predicate | Negative control | Fixture |
| --- | --- | --- | --- |
| TI-11.1-1 | Selecting a topic from a point flagged `ChangedDuringCapture` is refused in preview and in execution. | The same selection on a clean point is allowed. | the TI-02.1-6 corpus point |
| TI-11.1-2 | A window across c02's two points (different tokens) is refused or split at the boundary. | A window inside one token is allowed. | c02 (after PROD-02.2 chains) |
| TI-11.1-3 | c12 (older timestamps after the capture) keeps one token, and its window is allowed. | A boundary placed by timestamps splits c12 and fails. | c12 |
| TI-11.1-4 | Teardown of a clone whose target was recreated by someone else skips it with `CloneTargetReplaced`. | An unchanged clone target is deleted. | compose clone; c01 steps on the clone |

### PROD-15.1

| # | Pass predicate | Negative control | Fixture |
| --- | --- | --- | --- |
| TI-15.1-1 | Evidence records `source_generation` (token and `topic_id`) and the created target's identity. | Evidence without either field fails its validator. | compose original-name restore |
| TI-15.1-2 | Back up T, delete T, restore T under its name with `strip_offset_headers: false`, back up again: the lineage is `break` with `BoundaryRecordChanged`. c15 measured this with an emulated restore; the live row uses the real one. | A 15.1 build that renders `strip_offset_headers: true` for an original-name restore gives `continuous` on the next capture (c14 measured) and fails this row's predicate. TI-15.1-3's guard is what keeps such a build from shipping. | c15, c14 |
| TI-15.1-3 | While the point's `topic_id` is null, a plan with an original-name mapping and `strip_offset_headers: true` is refused with a named reason. | Accepting that plan fails. | guard unit test |
| TI-15.1-4 | Positions for an original-name target go through PROD-04.2's mapping (TI-04.2-3). | As in TI-04.2-3. | c15 emulation |

## 9. Proposed child rows

- **PROD-01.4a — Topic IDs through DescribeTopics** (impl, Tier A, gated on TI-OC1). It builds the first wrapper under the one policy of §6.4, in the FFI crate (a2) or module (a1) the owner picks. The IDs are wired into phase −1 and post-run capture (`topic_id`, `topic_id_after`), the check runner's inventory and restore targets. Acceptance:
  - FFI IDs equal the broker's on every oracle row, on each supported broker line;
  - the canonical text is derived from the two halves (C4);
  - zero reads as `null`, and a per-topic error is reported;
  - the soak test passes;
  - ADR 0004 is amended once, for the policy, with the exit condition.

  PROD-04.0's group, offset and ACL wrappers then join the same perimeter under their own rows.
- **PROD-01.4b — Upstream DescribeTopics** (contribution). Rebase rust-rdkafka PR #721 or a successor, with the URL-safe text form. It is the exit for 01.4a.
- The receipt 1.1.0 block, its verifier arms, the READ_UNCOMMITTED watermark read and the archive-side comparison belong to **PROD-02.1**, not a new row.
- The engine-side ID stays PROD-00.1's capability row. Making a source read usable (keeping `LogAppendTime` and repeated keys) is PROD-01.1's proposed PROD-00.3c and PROD-00.3e.

## 10. Limits of this record

- **Fixtures.** One KRaft broker per line: no replication, leader movement, unclean election (FP1, FP9), tiered storage or follower fetching. Rule 6's PROD-01.5 profiles had not landed; the current compose stack was used, with only the broker service.
- **Broker lines.** Four since PROD-01.5c: 3.7.1, which is past end of life, 3.9.2, 4.1.2 and 4.3.1, each as one KRaft broker. The 4.0 and 4.2 lines are not run.
- **Engine.** Only its capture path was used. This record's runs used 0.21.0, linux/amd64 under emulation on an arm64 host; PROD-01.5c's used `0.23.3+logweir.2`, linux/arm64, natively.
- **Comparison read.** The rule reads the capture's own archive. A source read stays excluded, not fixed: it reports false breaks on `LogAppendTime` topics and on repeated header keys (c17, c18) until PROD-00.3c and PROD-00.3e. Incremental runs must therefore overlap their predecessor by a record.
- **Not measured:**
  - FN3 (recreation during a capture) is from source;
  - FP1 and FP9 need more than one broker.
- **By design.** FP8 (c19) stays until IDs exist.
- **Timing.** Detection happens at the next capture. Until then, nothing observes a recreation and the old points read as current. IDs in discovery (01.4a) shorten that interval only for callers that ask.
- **Transactions.** The engine archives markers as ordinary records, so the archive-side comparison compares them like any other record. A tail made only of absent records (compaction) yields `unverified`. PROD-01.1 owns transaction semantics.
- **Harness bounds.** Every subprocess the oracle starts has a deadline, and an engine timeout also kills the engine's container (the L9 row). A timed-out `docker compose exec` leaves its Kafka tool running inside the broker container until `just e2e-down`.

## 11. Reproduction and artifacts

```sh
# the rule alone, no broker, no feature:
cargo test --locked -p e2e --test topic_identity
# the live rows:
just e2e-up   # the broker is enough; set KAFKA_VERSION=4.3.1 for the second line
LOGWEIR_TOPIC_IDENTITY_EVIDENCE=/tmp/ti.jsonl AWS_EC2_METADATA_DISABLED=true \
  cargo test --locked -p e2e --features e2e --test topic_identity -- \
  --include-ignored --test-threads=1 --nocapture
just e2e-down
```

Every broker address the live rows dial comes from one function, `broker_address` (`e2e/tests/topic_identity.rs:1159-1165`). That is where PROD-01.5's parameterised stack plugs in. The rows use no S3 endpoint.

Artifacts are in the run directory, `artifacts/prod-01-4/`:

- the first round's three runs (source-read comparison):
  - `oracle-run-1.log`, `topic-identity-evidence.jsonl` and `oracle-run-1-summary.txt` (3.7.1);
  - `oracle-run-2-kafka-4.3.1.log`, `topic-identity-evidence-4.3.1.jsonl` and `oracle-run-2-kafka-4.3.1-summary.txt`;
  - `oracle-run-3-tip.log`, `topic-identity-evidence-tip.jsonl` and `oracle-run-3-tip-summary.txt`;
- the fix round's runs (archive-against-archive comparison, all four modes): `oracle-fix-<version>.log`, `topic-identity-evidence-fix-<version>.jsonl` and `oracle-fix-summary.txt`;
- `ffi-route-evidence.txt`, `ffi-probe/` (the prototype's source only) and `ffi-probe-build.log`;
- `commit-above-end-evidence.txt` and `kafka-src/`: the Kafka commit-path source at tags 3.7.1 and 4.3.1, and `ReplicaFetcherThread.scala` at 3.7.1;
- `rule-mutants.{py,log}` (first round, 10 of 10 killed) and `rule-mutants-fix.{py,log}` (fix round, 25 of 25 killed, including the review's R1–R5);
- `upstream/`: the rdkafka 0.37.0–0.39.0 crates, the PR #721 diff (sha256 `ba214e48…`) and kafka-protocol 0.18.0 (sha256 `099d5c2f…`).

## 12. Landed: PROD-01.4a (2026-10-09)

Branch `claude/prod-01-4a`. OD-6 (a2): DescribeTopics joins PROD-04.0b's perimeter, the first wrapper TI-OC1 asked for.

**The call.** `logweir-rdkafka-ffi::topics::describe_topics(client, names, timeout)`, on `raw::run` like the group and ACL calls (PROD-04.0 §14's seven steps):
- bounded: librdkafka's request timeout plus `POLL_MARGIN`, and at most 1000 names per call (one Metadata request each);
- inputs refused before anything is sent: no names, more than 1000, a blank, NUL-bearing, repeated or over-249-byte name;
- the topic collection in a guard destroyed exactly once (librdkafka copies the names, `rdkafka_admin.c:9245-9249`);
- values only: the ID as its two `i64` halves, never `rd_kafka_Uuid_base64str` (C4); the per-topic error with its integer code (T12); a name the broker skipped is absent from the answer, never guessed.

**The meaning** (pure, `--no-default-features`): `logweir-kafka::topic_ids` maps an answer to `Id` (Kafka's text from the halves, `logweir-core::topic_identity::topic_id_text`), `NoId` (the zero UUID), `NotFound` (3), `NotAuthorized` (29, by name), `Failed` (another code, or no entry for a requested name). A whole-call failure is a `KafkaError`, and a transport failure (no result in the bound, `_TIMED_OUT`, `_TRANSPORT`, `_ALL_BROKERS_DOWN`, `_TIMED_OUT_QUEUE`, `_RESOLVE`) is `Unreachable`, never `TopicNotFound`. `ClusterReader::topic_ids` defaults to `NotRead`, so a reader that does not implement it can only leave an ID unknown.

**The field, as landed — three deviations from §3.2, each deliberate:**
- **Versions.** The receipt is **1.5.0** and the catalog point **1.5.0**, not 1.1.0: FX-4, FX-7, PROD-05.1 and PROD-01.3 took 1.1.0–1.4.0 first. The arms are **22–26**, not 6–10, for the same reason.
- **Reasons.** `generations.<topic>` carries `topic_id`, `topic_id_after`, `topic_id_source` (§3.1) and, beside each null ID, `topic_id_reason` / `topic_id_after_reason` from a closed five (`noTopicId`, `notAuthorized`, `topicNotFound`, `readFailed`, `notRead`). §3.1 says null is unknown; the reason says WHY, so a refused read is never mistaken for a broker with no IDs.
- **The catalog copy** is `topics[].identity` (the whole entry: both IDs, the source and the reasons) instead of a flat `topics[].topic_id`, which could hold neither the after-read nor a reason. PROD-02.1's `topics[].generation` (token, verdict) sits beside it when it lands.

Arms 22–26 (both readers, byte for byte): the block only from 1.5.0; covering exactly `source.topics`; every recorded ID canonical (22 URL-safe characters over 16 bytes that re-encode to themselves, never zero); a reason exactly for a null ID; a source exactly for a recorded one. Two different recorded IDs are not refused. MINOR under OD-7 (a). `verify_scorecard.py` 1.24.0 (1.23.0 was PROD-11.1's).

**The ID path in the backup.** `logweir backup run` reads every named topic's ID as the last read before the engine (after the execution claim and FX-4's configuration read) and again the moment the engine exits, and writes both into every receipt it signs. Never fatal: each failure is a recorded reason; a change during the capture is logged as a warning.

**The rule** (`logweir-core::topic_identity`): R1 and §4.4's ID condition exactly as `classify` and `intra_run` implement them — two different pre-capture IDs are `New` (`TopicIdChanged`), never a continuation; a capture whose two reads differ is `ChangedDuringCapture`; equal IDs and no change are `Same`; anything else, including a previous point from another source cluster, is `NotEstablished` with its reason. That last verdict is today's fallback: unknown, never the same generation, until PROD-02.1's offset rule (R2–R6) runs for it. Equal IDs still need R2–R6 for offset-dependent consumers (FP1).

**Not landed here, and why.** No product surface compares two points yet: the API's `PointView`/`AvailablePointView` and the catalog view carry no IDs, and nothing computes a lineage. PROD-02.1 owns that consumer (the lineage, the token, the view and the console); its rows TI-02.1-1, -6 and -9 can now use real IDs. The check runner's inventory (§6.1 "IDs in topic discovery") and restore-target identity (PROD-07.x, 15.1) are their rows' consumers of `ClusterReader::topic_ids`.

**Measured** (compose slot 3, `--profiles acl,auth`; `e2e/tests/topic_ids.rs` and this record's oracle, whose ground-truth read now also requires the product's DescribeTopics to equal `kafka-topics.sh --describe`):

| Line | product ID = CLI's | recreation between two real backups | refusal by name (`acl`) | absent topic | oracle (`topic_identity.rs`) |
| --- | --- | --- | --- | --- | --- |
| 3.7.1 | 4 topics per run, 3 and 4 IDs with `-`/`_` in two runs | `New`; control `Same`; readers agree; catalog copies | `NotAuthorized`; super user reads the ID; restored after the ACL is removed | `NotFound` | 52 passed, c10 ignored |
| 3.9.2 | 4 topics, all 4 IDs with `-`/`_` | `New`; control `Same`; readers agree; catalog copies | `NotAuthorized`; super user reads the ID; restored after the ACL is removed | `NotFound` | 52 passed, c10 ignored |
| 4.3.1 | 6 topics, 2 IDs with `-`/`_` | `New`; control `Same`; readers agree; catalog copies | `NotAuthorized`; super user reads the ID; restored after the ACL is removed | `NotFound` | 52 passed, c10 ignored |

The oracle's 18 live rows read the broker's ID four times each on every line, and the product's read equalled the CLI's every time. On 3.7.1 the whole e2e suite, run as CI runs it, passed (210 passed, 37 ignored). A closed port is `Unreachable` after the 2 s bound ("Timed out waiting for controller" on 3.7.1, "Failed while waiting for controller: Local: Timed out" on 3.9.2 and 4.3.1). Memory: both 100,000-call soaks +0 KiB, with and without Guard Malloc; a planted double destroy of the collection crashes under Guard Malloc; macOS `leaks --atExit` over the four live rows on 4.3.1: `0 leaks for 0 total leaked bytes`. Mutants: 45 planted (the FFI request and read, the answer mapping, the text and the rule, both readers' arms and lines, the backup's two reads, the catalog copy and rules 3–4), all killed; the one first-pass survivor (`between` ignoring the source cluster) gained its row.

**Fix round (2026-10-09), from the review's three MEDIUMs and five LOWs.** No surface or schema-shape change; the ID text, the reasons and the rule are the same. What tightened:

- **Reserved IDs are never an identity (M1).** Kafka reserves `Uuid.RESERVED`: ZERO `(0,0)` and `ONE_UUID` / `METADATA_TOPIC_ID` `(0,1)` = `AAAAAAAAAAAAAAAAAAAAAQ`. `topic_id_text` already dropped zero; it now drops both (`is_reserved`), the FFI/kafka read maps `(0,1)` to a null ID with the new sixth reason `reservedTopicId`, and the rule reads only a canonical, non-reserved text as an ID (`real`). Both verifiers refuse a receipt (arm 24, extended) **or a catalog point** that copies either reserved ID, with identical text; the catalog refusal is a new seventh parity document (`identity15reserved`) and `verify`'s `CatalogPoint` verdict. So a recreated topic can never read as the same generation through the sentinel. The receipt schema also gained an explicit `pattern` on `topic_id`/`topic_id_after` (L5), closing the schema-only gap.
- **A failed after-read is never `Same` (M2).** Equal pre-capture IDs give `Same` only when this capture's own two reads were recorded and agree; an after-read that recorded no ID yields `NotEstablished` with the new `Unestablished::CurrentAfterUnread` reason ("whether the topic changed while it ran is unknown"), matching §4.4's docs. A recreation DURING the capture (FN3) is no longer hidden.
- **The after-read happens after the engine (M3).** Pinned by an order-observing seam row: a fake `DataEngine` that changes the topic ID, so an after-read taken before the engine would see the old ID. This kills the review's RV13 (the after-read hoisted above `phase_run::run`), which the prior seam row could not see.
- **The LOWs.** L1: literal-code unit rows for the transport set (`[-185, -195, -187, -166, -193]`) and for `topics_call_failure` (`NoResult`→`Unreachable`), killing RV1a and RV2 off the live stack. L2: a row for previous `{A→B}`, current `{B,B}` ⇒ `New`, killing RV3. L3: `describe_topics` now checks the timeout bound before `send` builds any librdkafka object, so the "nothing is created" claim is true. L4 is an upstream librdkafka defect, reported to the owner in the worker report, not filed here (reproduced: a broker Metadata answer naming an unrequested topic drives `rd_kafka_DescribeTopicsResponse_parse` to its `orig_pos == -1` branch at `rdkafka_admin.c:9201`, which destroys `topicdesc` then logs the freed `topicdesc->topic` — a use-after-free, plus a leak of `mdi` via `goto err_parse`; reachable only from a hostile or buggy broker, so the risk to Logweir is low).

The release note is **item 45** (FX-24b holds 44). Mutants grew with the fix round's new seams (the reserved check, the `classify` Reserved arm, the M2 `within` gate, both verifiers' reserved refusal) alongside the four revived review mutants; all killed.

**For PROD-01.4b.** Rebase rust-rdkafka PR #721 (or a successor) with a text form derived from the halves in Kafka's URL-safe alphabet; once a released rdkafka wraps DescribeTopics safely, `topics.rs` leaves the perimeter. The L4 use-after-free belongs in that upstream report beside T20.

---

Documentation is licensed [CC-BY-4.0](../../LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
