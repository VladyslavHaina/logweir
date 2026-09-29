# PROD-01.4 — Topic identity and generations

Date: 2026-09-28. Row: PROD-01.4 (research, Tier B). Base revision: main `adee0a16`.
Evidence: `e2e/tests/topic_identity.rs` (this row's oracle) run against Apache Kafka 3.7.1 and 4.3.1 (compose broker, one KRaft node), with the pinned engine v0.21.0 (`sha256:8ff5be71…c317`).
Status: proposed for review. One owner-level choice (§6.4, TI-OC1) is presented with options and a recommendation, and is not taken here.

Engine paths below are inside the pinned tarball `third_party/kafka-backup-v0.21.0.tar.gz` (sha256 `0252a837…405b`), under `kafka-backup-0.21.0/crates/kafka-backup-core/src/`. librdkafka paths are inside the locked `rdkafka-sys 4.10.0+2.12.1` crate, under `librdkafka/`.

## 0. Decisions in one page

1. **Identity of recoverable history is (source cluster ID, topic name, generation).** A generation is one continuous offset history. Consumers react to a *break*, which is any pair of observations that cannot belong to one continuous history. Every recreation is a break, and so is a same-ID truncation. The heuristic cannot see one kind of recreation, the byte-identical replay of §4.7.
2. **`topic_id` is nullable.** It is Kafka's text form of the topic UUID: 22 URL-safe base64 characters derived from the UUID's two 64-bit halves. Null means *unknown*, never *same*. The all-zero UUID is never written. It lives in receipt 1.1.0, catalog point 1.1.0 and the API, and in the engine manifest only through the engine route (§3).
3. **The heuristic usable now** compares a capture with its predecessor. It checks partition counts, log start and end offsets (read READ_UNCOMMITTED), and the archived tail records at *exactly* their offsets, with the engine's two appended headers stripped. It returns one of five verdicts: `continuous`, `unverified`, `suspected`, `break`, `unknown` (§4).
4. **Measured on two broker lines** (§5): 16 live rows, identical outcomes on 3.7.1 and 4.3.1.
   - 8 rows make a new topic: 6 detected (5 `break`, 1 `suspected`), 2 known misses.
   - 8 rows keep the topic: 0 false breaks.
   - The offsets-only rule detects 2 of the 8.
   - Four naive variants produce false breaks or misses and are excluded by the rule's input requirements.
5. **Real IDs** (§6): recommend a scoped `unsafe` exception in `logweir-kafka` for librdkafka's DescribeTopics. The measured prototype is 123 lines, needs no new crate, and returns the broker's IDs. This is owner choice **TI-OC1**.
   - The upstream wrapper is stalled: no rust-rdkafka release through 0.39.0 has it, and PR #721 has been open and conflicting since 2024-09.
   - The engine route is PROD-00.1's capability row, and identity does not need it.
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

## 2. Definitions

- **Offset history.** For one (cluster, topic, partition), the map from offset to record.
  - Inside one continuous history, the record at an offset never changes.
  - A record can disappear from the front (retention, DeleteRecords: the log start advances).
  - It can disappear from inside (compaction), or be invisible to a reader (transaction markers).
- **Generation.** One topic incarnation, from CreateTopics to DeleteTopics. The broker assigns it a topic ID and starts its offsets at zero. CreatePartitions adds partitions to the same generation.
- **Break.** Two observations that cannot belong to one continuous history. A new generation is a break. So is a truncation below a previously observed end under the same ID (unclean leader election): offsets past the truncation point are reused by different records, and every offset-dependent consumer needs the same reaction either way.
- **Lineage key.** (source cluster ID read from the broker, topic name). A point from another cluster ID is never a predecessor.
- **Predecessor.** The newest committed point with the same lineage key.
- **Generation token.** A display and grouping key: `tid:<topic_id>` when the point has a topic ID, else `lwg1:<anchor point id>`. The anchor is this point when its verdict is `break`, `suspected` or `unknown`; otherwise it is the predecessor's anchor.

## 3. The field: `topic_id`

### 3.1 Representation

- **Text form.** 22 characters: the URL-safe base64 alphabet, no padding, over the 16 big-endian bytes most-significant-half ‖ least-significant-half. This is exactly what `kafka-topics.sh --describe` prints, measured equal on all four IDs recorded (C4). Writers derive it from the two halves (`rd_kafka_Uuid_most_significant_bits` / `…least…`), **never** from `rd_kafka_Uuid_base64str` (C4).
- **Value set.** `null`, or a string that decodes to 16 bytes, re-encodes to itself, and is not the all-zero UUID `AAAAAAAAAAAAAAAAAAAAAA` (Kafka's "no ID", C3). A writer maps a zero ID to `null`.
- **Source.** `topic_id_source` is `describeTopics` (Logweir's client, §6.1) or `engineManifest` (§6.3). It is omitted when the ID is null.

### 3.2 Where it lives

| Surface | Field | Version | Written when | Absent or null means |
| --- | --- | --- | --- | --- |
| Engine manifest | `topics[].topic_id` | engine format, additive `#[serde(default)]` in `crates/logweir-engine-oso/src/vendored/manifest.rs:29` (the xtask drift gate covers that file) | only through the PROD-00.3 engine route (§6.3); Logweir never writes the engine's manifest | unknown |
| Backup receipt | `generations.<topic>.topic_id`, beside the observation block of §4.1 | `format_version` **1.1.0** | every run once PROD-02.1 lands; `null` until TI-OC1's route lands | unknown |
| Catalog point | `topics[].topic_id`, `topics[].generation` | **1.1.0** | projected from the receipt | unknown (D3 §5.2 rule 2) |
| Product API | `PointView.topics[].topicId` (nullable), `.generation` | `1.0.0-alpha.N` bump (L5) | read from the catalog | `generation.verdict: "unknown"` |
| Restore evidence (15.1, 07.1) | the created target's identity: `topic_id` or creation marks | nested optional in the scorecard's `target` block (GC12 allows nested optional fields), named by those rows | at target creation | unknown |

The receipt's `generations` block is keyed by topic name. Each entry holds:

- `topic_id`, `topic_id_source` and `partition_count`;
- per partition: `before` and `after` marks, and `archived` (first and last offset, first and last record timestamp, and up to three `tail` records with offset and fingerprint), or `null` when nothing was archived;
- `lineage`: `previous_point_id`, `previous_receipt_sha256`, `comparisons[]` (each with partition, offset, result and `read_from`), `signals[]`, `verdict` and `basis`.

Receipt fields and values are snake_case, and signal names are PascalCase as in the oracle.

### 3.3 Versioning in both verifiers and the parity script

- **Receipt format.**
  - `format_version` becomes `1.1.0`, with `schemas/logweir-backup-receipt-1.1.0.json` beside 1.0.0.
  - `generations` is optional, appended last in the Rust struct (declaration order is byte order), and `skip_serializing_if = "Option::is_none"`.
  - **The payload type stays `…backup-receipt+json;version=1.0.0`.** It names the major-1 envelope. Changing it would make every existing verifier refuse every new receipt at the payload-type comparison, which rules out rollback for an additive field. `crates/logweir-core/src/trust.rs:1010-1030` already matches the base type, so either choice leaves the trust decision intact.
- **New arms in both readers.** Arms 6–10 are appended after arm 5 and run only when `generations` is present. Messages are byte-identical in `BackupReceipt::validate_invariants` and `docs/verify_scorecard.py::check_backup_receipt_invariants`.
  - **6.** `generations` under a 1.0.x `format_version` is refused. Precedent: the scorecard's `redactions` arm, `docs/verify_scorecard.py:1208-1212`.
  - **7.** The keys of `generations` equal `source.topics`, as arm 3 does for `records`.
  - **8.** Every `topic_id` is `null` or in canonical form (§3.1), never zero.
  - **9.** Marks and ranges are well formed:
    - `0 <= log_start <= high_watermark` before and after;
    - `first_offset <= last_offset`;
    - 1–3 tail entries, strictly descending, the first equal to `last_offset`, all at or above `first_offset`;
    - fingerprints are 64 lowercase hex.
  - **10.** The lineage agrees with itself:
    - the verdict is in the closed set;
    - a break-class signal is present exactly when the verdict is `break`;
    - the within-run derivation of §4.4 over this document's own marks yields `ChangedDuringCapture` ⇒ the signal is listed and the verdict is `break`;
    - `previous_point_id: null` ⇔ verdict `unknown`.
- **Corpus and version records.**
  - `SCRIPT_VERSION` moves 1.14.0 → 1.15.0, with a row in `docs/verify-a-scorecard.md`'s version table.
  - `crates/logweir-core/tests/backup_receipt.rs:265`'s arm count moves from 5 to 10.
  - `e2e/fixtures/invariants/backup-receipt-index.json` gains at least one case per new arm. `scripts/check-invariant-corpus.sh` derives both arm lists and fails if an arm is unmatched.
- **Parity script.** `scripts/check-verifier-parity.sh`'s receipt loop gains:
  - a 1.1.0 receipt with `topic_id: null` and one with a real ID, VALID in both readers;
  - one case per new arm, INVALID in both with identical text;
  - the existing 1.0.0 receipt keeps its VALID case (old archives).
- **Catalog point.** It becomes 1.1.0 (`schemas/logweir-catalog-point-1.1.0.json`) with the payload type unchanged. `RecordTopic` gains `topic_id` and `generation` (token, verdict, basis, signals, `previous_point_id`), both informational under D3 rule 3. The catalog sync recomputes the verdict from the two receipts and marks disagreement `RecordMismatch`. Python's `--payload-type catalog-point` mode is signature-only, so it gains no arm; the record states so.
- **API.** `topics[]` items gain `topicId` and `generation`. `just schema` regenerates the document, and `crates/logweir-api/tests/contract.rs` checks the drift.

### 3.4 Absent-value behaviour for every existing archive

| Archive | Carries | Read as |
| --- | --- | --- |
| Receipt 1.0.0 (every receipt written so far) | no `generations` | Generation unknown. PROD-02.1 shows "coverage not recorded". It is never `continuous` and never the same generation as anything. A successor of such a point gets verdict `unknown`, reason `previousNotRecorded`. |
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
- `topic_id_k`, once TI-OC1 delivers it; `null` until then.
- **Comparison read** at run *k* of offset *o* of the predecessor's tail: `At(fingerprint)` only when the first record returned carries offset *o*; `Absent` otherwise; `OutOfRange` below the log start. For a full capture (every run today, E3), read *o* from run *k*'s own archive. Both sides are then archived bytes, recomputable by anyone holding both points. A run that does not re-archive *o* (PROD-02.2's incremental runs) either overlaps its predecessor by one record or reads the source, READ_UNCOMMITTED, before its engine starts. The oracle measures the source read. For full captures the two are the same bytes, because the engine archives what the source holds at capture time.
- Timestamps are recorded for display and for time-window guards. They are **not** a detection signal (§4.5, c12).

### 4.2 The rule (run *k* against predecessor *j*)

1. **R0 predecessor.** With no predecessor, or one without generation data, the verdict is `unknown`.
2. **R1 IDs.** When both IDs are non-null: if they differ ⇒ `TopicIdChanged` (break). If they are equal, the basis is `topicId` and only R3–R4 still apply; a regression under the same ID is a truncation break.
3. **R2 partitions.** `P_k < P_j` ⇒ `PartitionCountDecreased` (break); Kafka never removes a partition from a live topic. `P_k > P_j` ⇒ `PartitionCountIncreased` (not a break by itself).
4. **R3 start.** For each partition in both: `LS_k.before < max(LS_j.before, LS_j.after)` ⇒ `LogStartRegressed` (break).
5. **R4 end.** `HW_k.before < max(HW_j.before, HW_j.after, L_j + 1)` ⇒ `EndRegressed` (break).
6. **R5 gap.** `LS_k.before > L_j + 1` ⇒ `CaptureGap [L_j+1, LS_k.before)`: records produced after *j* and deleted before *k*. This is not a break.
7. **R6 boundary.** Over `tail_j`, newest first:
   - an offset below `LS_k` ⇒ `BoundaryDeleted`, stop;
   - an offset at or above `HW_k` ⇒ skip (R4 fired);
   - `At` with an equal fingerprint ⇒ `BoundaryRecordVerified`, stop;
   - `At` with a different fingerprint ⇒ `BoundaryRecordChanged` (break), stop;
   - `Absent` ⇒ `BoundaryRecordAbsent`, then try the next older candidate;
   - `OutOfRange` ⇒ `BoundaryDeleted`, stop.
8. **Verdict.**
   - Any break signal ⇒ `break`.
   - Otherwise `PartitionCountIncreased` with nothing verified ⇒ `suspected`.
   - Otherwise any `BoundaryRecordVerified`, or equal IDs ⇒ `continuous`.
   - Otherwise `unverified`.

`classify` in `e2e/tests/topic_identity.rs` implements R0 (as a cluster mismatch) and R2–R6. R1 waits for IDs. Product code must agree with it on every oracle row (TI-02.1-3).

### 4.3 Verdicts

| Verdict | Meaning | Who may rely on it |
| --- | --- | --- |
| `continuous` (basis `topicId` or `content`) | Same history, verified. Offsets of *j* and *k* denote the same records. | Everyone, including offset-dependent consumers across the two points. |
| `unverified` (basis `watermarks`) | No break signal, and nothing left to compare: every tail record is deleted or compacted. Normal when retention is shorter than the backup interval. | Display and coverage (the token continues). Offset-dependent consumers must not use offsets across this link. |
| `suspected` | Partitions increased and nothing verified. CreatePartitions and a recreation with more partitions look alike. | Treated as `break` by every consumer except display. |
| `break` | Definite, within §4.6's false positives. | Every consumer: new token, no reuse of offsets across it. |
| `unknown` | No predecessor with generation data. | Treated as a new lineage; old points are never merged into it. |

### 4.4 The within-run check

Run *k* alone flags `ChangedDuringCapture` for a partition when any of these holds:

- `LS.after < LS.before`, or `HW.after < HW.before`;
- the first archived offset is below `LS.before`;
- `L_k >= HW.after`;
- with IDs, the topic ID at phase −1 differs from the one read after the engine.

Such a topic's verdict in that point is `break`, and the point is not selectable for that topic (§7). Arm 10 makes a receipt that hides the flag unverifiable.

### 4.5 Input requirements, each measured

| Requirement | What goes wrong without it | Row |
| --- | --- | --- |
| Marks read READ_UNCOMMITTED | With a transaction open, READ_COMMITTED marks stop at the last stable offset (5) while the engine archived to offset 7. The result is a false `break` and a false within-run flag. | c11 |
| Source-equivalent fingerprints | Across 102 archived tail records on 3.7.1, **none** of the verbatim fingerprints equalled the source record's. A verbatim probe reports a false `break` on all four verified same-topic rows (c05, c06, c11, c12) and misses the original-name restore (c15). | all |
| Read at *exactly* the offset | After compaction, a read from offset 19 returns offset 20, a different record. "First record from *o*" reads compaction as a new topic. | c09 |
| Up to three tail candidates | Compaction made all three newest candidates absent. Transaction markers (E6) do the same to the newest one; that is from source, not measured. | c09 |
| Timestamps are not a signal | Records produced after the capture carry *older* timestamps in the same topic. | c12 |
| Canonical ID text from the two halves | librdkafka's helper returns another alphabet for the same ID. | FFI evidence (C4) |

### 4.6 Known false positives (a break reported, same generation)

- **FP1: same-ID truncation.** Unclean leader election, or a lagging replica elected leader, can regress the end or the log start. Consumers need the break anyway (§2). With IDs available it is labelled "history truncated", not "recreated". Not measured: the fixtures have one broker.
- **FP2–FP5: the naive variants of §4.5.** Measured, and excluded by the input requirements.
- **Not measured, and no false positive expected from source:**
  - tiered storage (librdkafka queries EARLIEST, the global log start);
  - leader moves with `acks=all`;
  - follower fetching (watermarks are queried from the leader).

### 4.7 Known false negatives (a new topic not detected)

- **FN1: recreated, refilled past the old end, and every compared offset deleted before the next capture** (measured, c13: `unverified`). The `CaptureGap` it reports is really a generation boundary. The same holds when the new generation compacts the compared offsets away.
- **FN2: byte-identical replay** (measured, c14: `continuous`). The same keys, values, headers and timestamps land at the same offsets, as with a restore that strips Logweir's offset headers, or a replay tool that keeps timestamps.
- **FN3: recreation during a capture, when the new generation already extends past the engine's position.** The engine reads on (E4), and the marks need not regress. From source, not measured. Only IDs read before and after the engine close it.
- **FN4: no usable predecessor** (the first run after the upgrade, or a 1.0.0 predecessor). The verdict is `unknown`, which no consumer treats as `continuous`.
- **FN5: several recreations between two captures.** They are one break; the heuristic cannot count generations.

## 5. Evidence: measured outcomes

Run with `LOGWEIR_TOPIC_IDENTITY_EVIDENCE=<file> cargo test --locked -p e2e --features e2e --test topic_identity -- --include-ignored --test-threads=1` against `just e2e-up`'s broker. The two runs were made on 2026-09-29 (UTC):

- 3.7.1: image `sha256:ed74d7d1…9b68`, 29 of 29 tests in 435 s;
- 4.3.1: image `sha256:77e3df90…2837`, 29 of 29 in 430 s;
- 3.7.1 again, at the committed oracle (`0b987fda`, three more pure tests): 32 of 32 in 730 s, every row equal to the first run.

Every column below is identical on both lines. Evidence files are listed in §11.

| Row | Situation | Topic ID after | Offsets only | Full rule | Signals (full rule) | Outcome |
| --- | --- | --- | --- | --- | --- | --- |
| c01 | recreated, 3 → 3 partitions, fewer records | changed | break | **break** | EndRegressed ×3 | detected |
| c02 | recreated, 3 → 3, refilled past the old end | changed | no break | **break** | BoundaryRecordChanged ×3 | detected (probe only) |
| c03 | recreated, 3 → 1 partition | changed | break | **break** | PartitionCountDecreased, BoundaryRecordChanged | detected |
| c04 | recreated, 3 → 5 partitions, refilled | changed | no break | **break** | PartitionCountIncreased, BoundaryRecordChanged ×3 | detected (probe only) |
| c05 | CreatePartitions 3 → 5 (same topic) | same | no break | continuous | PartitionCountIncreased, BoundaryRecordVerified ×3 | correct |
| c06 | DeleteRecords inside the archive (to 5, 9, 10) | same | no break | continuous | BoundaryRecordVerified ×2, BoundaryDeleted | correct |
| c07 | produced 10 more, DeleteRecords to 15 | same | no break | unverified | CaptureGap [10,15) ×3, BoundaryDeleted ×3 | correct, gap reported |
| c08 | DeleteRecords to the end (log start = end) | same | no break | unverified | BoundaryDeleted ×3 | correct |
| c09 | compaction removed offsets 17–19 | same | no break | unverified | BoundaryRecordAbsent ×3 | correct |
| c10 | retention expiry (the retention check came after 101–286 s across the three runs) | same | no break | unverified | CaptureGap [10,15), BoundaryDeleted | correct, gap reported |
| c11 | open transaction during and after the capture | same | no break | continuous | BoundaryRecordVerified | correct; READ_COMMITTED marks: false `break` |
| c12 | later records carry older timestamps | same | no break | continuous | BoundaryRecordVerified | correct |
| c13 | recreated, refilled, DeleteRecords past the old tail | changed | no break | unverified | CaptureGap [10,12), BoundaryDeleted | **known miss (FN1)** |
| c14 | recreated, byte-identical replay | changed | no break | continuous | BoundaryRecordVerified | **known miss (FN2)** |
| c15 | original-name restore, emulated with headers kept | changed | no break | **break** | BoundaryRecordChanged | detected (probe only) |
| c16 | recreated, 3 → 5, nothing left to compare | changed | no break | **suspected** | PartitionCountIncreased, CaptureGap ×3, BoundaryDeleted ×3 | detected as suspected |

Totals per broker line:

- **New-topic rows (8):**
  - the offsets-only rule detects 2 (c01, c03);
  - the full rule detects 6, as 5 `break` and 1 `suspected`;
  - the full rule misses 2 (c13, c14).
- **Same-topic rows (8):** 0 false breaks; 4 `continuous` and 4 `unverified`.
- **Guards on the oracle itself:**
  - each live row asserts the broker's topic ID before its verdict;
  - 16 pure tests pin the rule's branches;
  - a mutant pass killed all 10 rule mutants (`artifacts/prod-01-4/rule-mutants.log`).

## 6. Route to real topic IDs

### 6.1 An FFI exception in `logweir-kafka`

- **Scope.** One private module, for example `topic_ids.rs`, behind a safe method on `RdKafkaReader` and a `ClusterReader` method whose default returns `Ok(None)` for test fakes. `#![forbid(unsafe_code)]` cannot be relaxed inside the crate, so the root becomes `#![deny(unsafe_code)]`, with `#[allow(unsafe_code)]` on that one module only. No gate greps for the attribute; the change is visible in review.
- **Cost, measured with the throwaway prototype** (`artifacts/prod-01-4/ffi-probe/src/main.rs`, 123 lines):
  - one `unsafe` block (about 65 lines) plus a one-line string helper;
  - **no new crate and no new C code**: `rdkafka::bindings` is `rdkafka_sys::bindings` (C1, MIT), and librdkafka 2.12.1 (BSD 2-clause, already attributed in `NOTICE`) is already linked;
  - it built in 32 s with the pinned toolchain;
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

### 6.4 Owner choice TI-OC1 (presented, not taken)

| Option | Delivers | Cost and risk |
| --- | --- | --- |
| **(a) Scoped `unsafe` exception now (recommended)** | IDs wherever the broker has them: capture, discovery, restore targets | One reviewed module, no new crate. `forbid` → `deny` at the crate root until (b) ships. |
| (b) Wait for upstream (contribute to PR #721) | The same, safely | Unknown date. An rdkafka bump plus the `uuid` crate. FN1–FN3 stay open until then. |
| (c) Engine route only (PROD-00.3, OD-3) | Archive-side IDs only | Engine patch or fork, a protocol-version change, no target-side IDs |
| (d) Heuristic only, permanently | Nothing new | FN1–FN3 stay; PROD-15.1 must refuse header stripping forever |

**Recommendation:** take (a), and plan (b) as its exit: restore `forbid` once a released rdkafka wraps DescribeTopics. Keep the heuristic as the fallback for `null` IDs and for truncation. (c) is not needed for identity.

## 7. Consumer reactions

Common to all six rows:

- **Generation token.** Points are grouped by it.
- **Offsets across two points.** A consumer that uses offsets from two different points requires either every catalog link between them to be `continuous`, or equal non-null topic IDs and no break signal.
- **Verdicts.** `suspected` reacts like `break`, and `unknown` like a new lineage.
- **Timestamps** never place a boundary.

### PROD-02.1 — Show honest coverage for scheduled backups

- **What each capture records.** The observation of §4.1 and the lineage of §4.2 in receipt 1.1.0 (`generations`), projected into catalog 1.1.0 and the API.
  - The runner receives the predecessor's facts as an additive block of `execution-inputs.json` (D-SEAMS S4): point ID, receipt sha256, marks, last offsets, tail fingerprints and topic IDs. This adds no runner read authority.
  - When that input is absent (a manual `logweir backup run`), the receipt says `lineage: null` and the catalog sync computes the lineage later.
  - Watermarks come from a new READ_UNCOMMITTED read of the log start and high watermark. The existing `end_offsets` callers are unchanged.
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
| TI-02.1-1 | A 1.1.0 receipt with `generations` verifies VALID in `logweir drill verify` and `docs/verify_scorecard.py`, once with `topic_id: null` and once with a real ID. The existing 1.0.0 receipt stays VALID. | Five corpus cases, each refused by both readers with identical text: `generations` under 1.0.0 (arm 6), a missing topic key (7), the zero UUID or `Cf6zT/mcTNCoxuPmv1Ztxw` (8), a tail above `last_offset` (9), and `continuous` beside `EndRegressed` (10). Deleting an arm from one reader fails `check-invariant-corpus.sh`. | `backup-receipt-index.json`; `check-verifier-parity.sh` |
| TI-02.1-2 | During an open transaction the product records READ_UNCOMMITTED marks, and the next point's verdict is not `break`. | A build that reads the marks at librdkafka's default isolation produces `break` on c11. The oracle measured exactly this, so the row must fail on that build. | c11 |
| TI-02.1-3 | For c01–c16, the product verdict and its break-class signals, computed from two real `logweir backup run` receipts, equal `classify`'s. | A build that fingerprints archived records verbatim reports `break` on c05, c06, c11 and c12 (measured) and fails. | oracle rows, re-pointed at the runner |
| TI-02.1-4 | After c02's recreation is detected, the API shows two tokens. The covered window and freshness of the current token start at the post-recreation point. | An implementation that merges windows by topic name shows one window starting at the pre-recreation point, and fails the window-start assertion. | c02 plus a `ProtectionPolicy` |
| TI-02.1-5 | Every 1.0.0 point reads `generation: unknown` and "coverage not recorded". A 1.1.0 point after a 1.0.0 predecessor is `unknown` (`previousNotRecorded`). | Rendering a 1.0.0 point as `continuous` or "complete" fails. | `e2e/fixtures/signed/backup-receipt.json` plus a 1.1.0 fixture |
| TI-02.1-6 | A receipt whose own marks derive `ChangedDuringCapture` carries `break`, and selection does not offer the point for that topic. | The same receipt with the flag suppressed is refused by both readers (arm 10). | corpus case built from c11's READ_COMMITTED marks, which derive the flag |
| TI-02.1-7 | c07 and c10 record `CaptureGap [10,15)` per partition in the receipt, the catalog and the API. | c06 (log start exactly one past the tail) records none. | c06, c07, c10 |

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
| TI-07.1-1 | The decision's failure-state table has a row "target recreated between attempts": refuse, then a fresh target. Checkpoints bind the target identity of §7. | For PROD-07.3: recreating and refilling the target past the checkpoint must refuse resume. The offsets-only check accepts it (c02 measured `no break`), so the test needs the exact-offset fingerprint or the ID. | c02 steps applied to a restore target |
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
| TI-15.1-2 | Back up T, delete T, restore T under its name with `strip_offset_headers: false`, back up again: the lineage is `break` with `BoundaryRecordChanged`. c15 measured this with an emulated restore; the live row uses the real one. | The same with `strip_offset_headers: true` gives `continuous` (c14 measured), so the row asserts the guard in TI-15.1-3 instead. | c15, c14 |
| TI-15.1-3 | While the point's `topic_id` is null, a plan with an original-name mapping and `strip_offset_headers: true` is refused with a named reason. | Accepting that plan fails. | guard unit test |
| TI-15.1-4 | Positions for an original-name target go through PROD-04.2's mapping (TI-04.2-3). | As in TI-04.2-3. | c15 emulation |

## 9. Proposed child rows

- **PROD-01.4a — Topic IDs through DescribeTopics** (impl, Tier A, gated on TI-OC1(a)). The module of §6.1, and the IDs wired into phase −1 and post-run capture, the check runner's inventory and restore targets. Acceptance:
  - FFI IDs equal the broker's on every oracle row, on each supported broker line;
  - the canonical text is derived from the two halves (C4);
  - zero reads as `null`, and a per-topic error is reported;
  - the soak test passes;
  - ADR 0004 is amended with the exit condition.
- **PROD-01.4b — Upstream DescribeTopics** (contribution). Rebase rust-rdkafka PR #721 or a successor, with the URL-safe text form. It is the exit for 01.4a.
- The receipt 1.1.0 block, its verifier arms and the READ_UNCOMMITTED watermark read belong to **PROD-02.1**, not a new row. The engine-side ID stays PROD-00.1's capability row.

## 10. Limits of this record

- **Fixtures.** One KRaft broker per line: no replication, leader movement, unclean election (FP1), tiered storage or follower fetching. Rule 6's PROD-01.5 profiles had not landed; the current compose stack was used, with only the broker service.
- **Broker lines.** Two: 3.7.1, which is past end of life, and 4.3.1. The 3.9 and 4.1 lines are PROD-01.5's to run with this oracle.
- **Engine.** Linux/amd64, run under emulation on an arm64 host; only its capture path was used.
- **Comparison read.** The oracle measures the source read. The archive read of §4.1 is argued equal for full captures, not measured separately.
- **Not measured.** FN3 (recreation during a capture) is from source.
- **Timing.** Detection happens at the next capture. Until then, nothing observes a recreation and the old points read as current. IDs in discovery (01.4a) shorten that interval only for callers that ask.
- **Transactions.** Long runs of markers (empty transactions) can exhaust the three tail candidates and yield `unverified`. PROD-01.1 owns transaction semantics.

## 11. Reproduction and artifacts

```sh
just e2e-up   # the broker is enough; set KAFKA_VERSION=4.3.1 for the second line
LOGWEIR_TOPIC_IDENTITY_EVIDENCE=/tmp/ti.jsonl AWS_EC2_METADATA_DISABLED=true \
  cargo test --locked -p e2e --features e2e --test topic_identity -- \
  --include-ignored --test-threads=1 --nocapture
just e2e-down
```

Artifacts are in the run directory, `artifacts/prod-01-4/`:

- `oracle-run-1.log`, `topic-identity-evidence.jsonl` and `oracle-run-1-summary.txt` (3.7.1);
- `oracle-run-2-kafka-4.3.1.log`, `topic-identity-evidence-4.3.1.jsonl` and `oracle-run-2-kafka-4.3.1-summary.txt`;
- `oracle-run-3-tip.log`, `topic-identity-evidence-tip.jsonl` and `oracle-run-3-tip-summary.txt` (3.7.1, at the committed oracle);
- `ffi-route-evidence.txt` and `ffi-probe/` (the prototype's source only);
- `commit-above-end-evidence.txt` and `kafka-src/` (the two Kafka commit-path source files, fetched at tags 3.7.1 and 4.3.1);
- `rule-mutants.py` and `rule-mutants.log`;
- `upstream/` (the rdkafka 0.37.0–0.39.0 crates and the PR #721 diff, sha256 `ba214e48…`).

---

Documentation is licensed [CC-BY-4.0](../../LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
