# PROD-08.1 — Complete archive integrity and exact counts: the integrity contract

Decision record for **PROD-08.1** ("Complete archive integrity and exact counts") in the
[product-expansion tracker](../product-expansion.md#prod-081--complete-archive-integrity-and-exact-counts).

- Date: 2026-10-07. Base: main `2c277dc1`. Branch `claude/prod-08-1`.
- Kind: implementation row (Tier A). It ships the complete verification, the signed fields and
  this contract, which PROD-03.2, 04.2, 05.2, 08.2, 08.3, 11.1 and 12.1 consume.
- Engine: `kafka-backup` v0.21.0 (the pinned source and image of
  [PROD-01.1](PROD-01.1-record-semantics.md)).
- Code: `crates/logweir/src/drill/phase7_verify/complete.rs` (the complete lane),
  `crates/logweir/src/drill/phase7_verify.rs` (`run_with_coverage`, `lineage_faults`),
  `crates/logweir/src/drill/phase4_sample.rs` (the complete selection),
  `crates/logweir/src/drill/phase0_admit.rs` (the coverage refusals),
  `crates/logweir-core/src/scorecard.rs` (`Verification`, arms IV-1 to IV-7),
  `crates/logweir-core/src/spec.rs` (`sample.coverage`, `sample.complete_max_records`),
  `crates/logweir-kafka/src/fingerprint.rs` (`record_digest_ordered`).
- Oracles: `crates/logweir/tests/complete_verify.rs` (the fault matrix over real KBAK segments),
  `crates/logweir/tests/orchestrator.rs` (the two complete shapes), and the live rows in
  `e2e/tests/record_semantics.rs` (§6).

## 0. Decision summary

1. **Two coverages, chosen by the approved plan.** `sample.coverage: sampled` (the default, and
   every earlier plan) keeps today's check. `sample.coverage: complete` verifies every archived
   segment and every restored record of every restored partition. The signed scorecard says which
   in the new nested optional block `integrity.verification` (format 1.4.0); a document without it
   covered a sample and is never read as complete.
2. **The expected output is computed from each archived record's OWN timestamp** (§2), never from
   the segments' first and last timestamps the engine selects by, so the complete lane is
   independent of the engine's selection.
3. **Restored records are mapped by their last `x-original-offset`** and compared byte for byte —
   key, value, timestamp and the headers in order, every occurrence — with exact per-partition
   counts of missing, unexpected, duplicate, out-of-order and different records. The manifest's
   first/last count bound is not consulted by the complete lane.
4. **Complete coverage is never silently replaced by sampling.** A bound
   (`sample.complete_max_records`), an archive whose records carry no lineage header, a segment
   that cannot be verified or decoded: each leaves the partition NOT compared, `covered: false`
   with the reason, and a verdict that is never `pass` (arm IV-6).
5. **One chokepoint.** Each partition is one `SelectionVerdict`; `roll_up` decides the verdict for
   both lanes.
6. **Evidence is separated** into the authenticated report (the DSSE envelope), archive integrity
   (`complete.archive`), the replay comparison (`complete.replay`, `complete.partitions`) and
   application validation (`application: notAttempted`).
7. **Versioning:** scorecard 1.4.0, `verify_scorecard.py` 1.19.0, seven arms read only the new
   block or judge `integrity.result` against it (MINOR under OD-7 (a)); two new causes for
   existing values move verdicts only to the safer side (OD-7's third case) — §5.

## 1. What each coverage checks

| | `sampled` | `complete` |
|---|---|---|
| Partitions | those whose segments' first/last bounds overlap the sample window, the first `max_partitions` | every partition the manifest lists for a restored topic, and every partition of its target topic |
| Archive integrity | sha256 of the segments overlapping the sample window | sha256 of EVERY segment, its decoded record count and offset range against the manifest |
| Expected output | the engine's archive selection (`OsoCliEngine::fingerprints`), capped at `records_per_partition` | every decoded record the window selects by its own timestamp (§2) |
| Target read | the first `records_per_partition` records | every record, offset 0 to the high watermark, in chunks of 5,000 |
| Record comparison | fingerprint with sorted headers | ordered digest (`record_digest_ordered`) |
| Count | the manifest's bound over first/last timestamps | exact, per partition |
| Duplicates and order | in the head read (`lineage_faults`, since this row) | over the whole output |
| Signed | `verification.coverage: sampled`, `header_order: notVerified`, gaps, pruned | the same, `complete`, `verified`, and the `complete` block |

## 2. The expected-output model (the contract dependents consume)

For a restore plan `P` with topic mapping `M`, window `W = (start, end)` and floor source `F`, and
a backup set whose manifest lists segments `S(t, p)` for source topic `t` and partition `p`:

- **Selection.** `selected(r) := r.timestamp <= end ∧ (F = ArchiveManifest ∨ r.timestamp >= start)`.
  Under `ArchiveManifest` (guard G-WIN: the window starts at the archive) there is NO lower bound,
  so a record older than every segment's first record — which the engine's floor drops
  (PROD-01.1 S8) — is expected, and its absence is a fault (acceptance row 08-3). Under
  `InheritedFromSpec` (PROD-11.1's sub-window) the plan's own start applies. The end is
  inclusive, as the engine's filter is; equal timestamps at the end are selected. The signed block
  records it as `complete.window {start_ms?, end_ms}` (`complete::window_of`).
- **Expected set.** `E(t, p)` is every record `r` decoded from every segment in `S(t, p)` (sorted
  by start offset) with `selected(r)`, keyed by its source offset `r.offset`, each with the ordered
  digest of `(key, value, headers in order, timestamp)` exactly as archived — the archived lineage
  headers included.
- **E is known only when** every segment in `S(t, p)` carries a sha256 that matches its bytes, the
  store holds it, the decoder reads it, its decoded count and offsets match the manifest, no source
  offset is archived twice, and every selected record carries an `x-original-offset` whose last
  occurrence names its own offset. Otherwise the partition is NOT compared: `compared: false`,
  `covered: false`, and the reason in `findings` and `incomplete_reason`. A failed segment is also
  an archive-integrity failure (`segments_failed`, verdict `fail`); an unverifiable one is
  `segments_unverified` (verdict at best `partial`).
- **Restored set.** Every record of target partition `M(t), p` from offset 0 to the high
  watermark, in target-offset order. Each is mapped by its LAST `x-original-offset` header (8 bytes,
  little-endian) to a source offset `o`; a record with none is `unexpected`.
- **Comparison, in target order.** A repeated `o` is a `duplicate` (not compared again). An `o`
  below the largest seen so far is `out_of_order` (and still compared). An `o` not in `E` is
  `unexpected`. Otherwise the first copy is `matching` when its ordered digest equals `E[o]`, else
  `mismatched`. Every `E` key never seen is `missing`.
- **A partition passes** exactly when `missing = unexpected = duplicates = out_of_order =
  mismatched = 0`, `matching = expected = restored`, and every one of its segments verified
  (`ReplayComparison::is_exact`). A partition the manifest does not list but the target holds has
  `E = ∅`; anything restored there is unexpected.
- **The run passes** exactly when every partition passes and was compared, over at least one
  partition (arm IV-6, which holds the signed totals AND every partition to it), through the same
  `roll_up` as the sampled lane.
- **A short read is refused, never compared.** When the reader stops answering below a target
  partition's high watermark (librdkafka's end-of-partition can arrive early, e.g. over a tail of
  control records), the lane refuses with `Operational` (exit 1, nothing signed), naming the offset:
  comparing what was read would undercount `restored` and could pass a target whose unread tail is
  unexpected records (review M-2; `complete_verify.rs::a_short_target_read_is_refused_and_never_a_smaller_comparison`).
- **Offset holes** (`complete.archive.offset_holes`): source offsets inside the decoded span — from
  the first decoded offset to the last — that no archived record holds and no recorded gap or
  pruned range explains: a compacted source's holes. Disclosed, never a fault: the archive holds the
  log as it was fetched (PROD-01.1 C7), and the replay reproduces exactly that. **A leading
  compacted range is NOT counted** — the common case, where the cleaner removed the oldest values —
  because the manifest records no partition start offset to measure it from; only a hole between
  two archived records is (measured live, `complete_coverage_discloses_a_compaction_hole_inside_the_span`).

### 2.1 What the model means for each dependent transformation

| Kind | Rule | Who owes it |
|---|---|---|
| **Time filters** | A sub-window is `InheritedFromSpec`: `selected` takes the plan's start. Nothing else changes. A recorded G-WIN amendment must state the start in the plan bytes (inside `plan_hash`), and `window_of` already reads it. | PROD-11.1 |
| **Partition subsets** | `E` is computed only for the plan's selected partitions; every other partition of the restored topic must be EMPTY on the target (its records are `unexpected`). The block must name the selection: a proposed additive field `complete.partitions_selected` (per topic), under a MINOR bump, with an arm that a partition listed in `partitions` and not selected has `expected = 0`. | PROD-11.1 |
| **Record filters** (erasure, offset ranges, YAML filter rules) | `E = selected ∧ keep(r)`, where `keep` is Logweir's OWN evaluation of the plan's filter over each decoded record, never the engine's report of what it dropped. Excluded records are counted in a proposed additive `complete.replay.excluded` and named by rule identity in `complete.filter {id, digest}`. A filter Logweir cannot evaluate makes the partition not compared. | PROD-00.3i, PROD-11.1 |
| **Compaction** | `E` is the archived (already compacted) log; `offset_holes` discloses the holes. A target that compacts BEFORE verification loses records and fails (`missing`), which is right: PROD-05.2's post-verification transition to `cleanup.policy=compact` happens after the signed verdict, never before. | PROD-05.2 |
| **Transformations** (schema ID rewrite, masking) | The expected record is `T(r)` for the plan's transformation `T`, deterministic and versioned: the comparison is `digest(T(r)) = digest(restored)`. Unchanged fields must stay byte-identical, so `T` is applied by field, not by re-encoding the record. `T`'s identity goes into a proposed `complete.transformation {id, version, digest}`. A record `T` cannot be computed for (a registry lookup that failed, a malformed payload) makes its WHOLE PARTITION `compared: false`, with a finding naming the record's source offset — so the block reads `covered: false` and the verdict is never `pass` (IV-5, IV-6); no per-record "uncomputed" count is defined. The lineage headers are never transformed. | PROD-03.2, PROD-11.2 |
| **Transactions** | Control records and aborted records are archived records: they are in `E` and must be restored (the comparison basis is the archive, PROD-01.1 V3). A committed-only archive (PROD-00.3a) changes `E` by changing the archive, not the model. | PROD-01.1a, PROD-00.3a |
| **Consumer positions** | **Translation is allowed only over a complete verification that PASSED for every partition it names**: the restore's signed scorecard reads `integrity.result: pass`, `integrity.verification.coverage: complete` and `complete.covered: true`, and `complete.partitions[]` lists each named partition with `compared: true` and an exact replay (every count 0 but `expected = restored = matching`). Anything else is REFUSED, for every position of the restore, naming the field that blocked it: a sampled or unrecorded coverage; `covered: false`; any `missing` (a record at or above `P` the consumer never read would be silently skipped), `unexpected`, `duplicates`, `out_of_order` (the first record with lineage `>= P` would re-deliver every later record below `P`) or `mismatched` record in a named partition. This contract defines no labelled partial translation. Over a passing partition the rule is PROD-01.1's 04-1: `P` maps to the first target record whose lineage offset is `>= P`; 04-4's "first copy of a duplicate" is the mapping function's definition for a target that holds one, which a passing verification excludes, so it is reachable only in PROD-04.2's own unit rows, never in a `Switchover`. | PROD-04.2 |
| **Replicated targets** | A target written by a replicator carries no `x-original-offset`. Where the replicator PRESERVES offsets (Cluster Linking, MSK Replicator in its offset-preserving mode), the key is the target offset itself: a proposed additive `comparison_key: lineage | offset` beside `comparison_basis`; with `offset`, a target offset absent from `E` is unexpected and order is the target's own. MirrorMaker 2 does NOT preserve offsets, so `offset` would mis-key every record of an MM2 target and is refused for one; its key — MM2's offset-sync mapping, or a content-and-order key — is PROD-12.1's to define and to version, and this contract defines neither. | PROD-12.1 |
| **Streaming and bounded memory** | The same contract, computed in a bounded window rather than one digest per expected record. An interrupted run is `covered: false`. | PROD-08.3 |
| **Exercises** | A rehearsal that asks for complete coverage carries it in `spec.bounds` (a CRD field, inside `templateDigest`) and its result in the status; complete coverage's cost bounds how often. | PROD-08.2, child row PROD-08.1a |

## 3. The signed fields (scorecard format 1.4.0)

`integrity.verification`, nested and optional (Global Constraint 12 as amended permits nested
optional fields), described field by field in
[the scorecard format](../../formats/drill-scorecard.md#integrityverification-format-140):
`coverage`, `comparison_basis` (`archive`), `header_order` (`verified` | `notVerified`),
`application` (`notAttempted`), `gaps[]` and `pruned[]` (structured `{topic, partition,
from_offset, to_offset}`, signed — the issue's "gaps are free text"), and `complete` with
`covered`, `incomplete_reason`, `max_records`, `window`, `archive {segments, segments_verified,
segments_failed[], segments_unverified[], records_decoded, offset_holes}`, `replay {expected,
restored, matching, missing, unexpected, duplicates, out_of_order, mismatched}` and
`partitions[]`. Under complete coverage the legacy counters carry the complete comparison:
`integrity.records_sampled` = the expected records of the compared partitions, matching their
matching records, `sample.records_expected` = the expected output of the COMPARED partitions — the
whole expected output only when `covered: true` (an uncompared partition is never decoded, so it
contributes 0; review L-3).

**Absent means not recorded, read as sampled.** Every document before 1.4.0, and every reader
built before this row, reads a sample; nothing re-reads old evidence as complete (rule 3).

### 3.1 The arms and their classification (OD-7)

| Arm | Refuses | Reads | OD-7 |
|---|---|---|---|
| IV-1 | the block under a version before 1.4.0 | the block, `format_version` | (a) |
| IV-2 | `coverage` not `sampled` or `complete` | the block | (a) |
| IV-3 | `header_order` not `verified`/`notVerified`, or `verified` beside sampled | the block | (a) |
| IV-4 | `complete` without `coverage: complete`, or the reverse | the block | (a) |
| IV-5 | `covered: false` without a non-blank reason, or a reason beside `covered: true` | the block | (a) |
| IV-6 | `integrity.result: pass` beside a complete block that is not covered, lists no partition, names a failed or unverified segment, counts any fault in total or in any one partition, or lists an uncompared partition | the block and `integrity.result` | (a): fires only on a document carrying the block, judges an existing field against it and can only refuse — the reading FX-3's NR-2 to NR-5 were given |
| IV-7 | totals that are not the partitions' sums; a segment neither verified, failed nor unverified | the block | (a) |

Both readers state each arm in the same position (after `source.time_basis`, before
`redactions`) with byte-identical text; `scripts/check-verifier-parity.sh` runs the seven refusals
and four accepted documents through both, and `e2e/fixtures/invariants/` carries one case per arm,
24 null cases for the block's plain `u64` counts and 5 shape cases
(`scripts/check-invariant-corpus.sh`).

### 3.2 New causes for existing values (OD-7's third case)

- **The sampled lane's order check.** `lineage_faults` fails a selection whose restored head repeats
  or goes backwards in `x-original-offset` — a new cause for `fail-integrity`, only to the safer
  side, and only for a target the engine wrote wrongly.
- **Phase 0's coverage refusals.** `coverage: complete` with `max_partitions`;
  `complete_max_records` without complete coverage; a bound of 0 — new causes for exit 3, each a
  plan value no earlier plan carried.
- **`compare` keys the head by the LAST `x-original-offset`** (it read the first). The pinned
  engine's targets carry one (PROD-01.1 R4), so no verdict moves; it removes PROD-00.3e's
  stated Logweir-side prerequisite.

## 4. Compatibility, upgrade and rollback

- **Plan bytes.** `sample.coverage` and `sample.complete_max_records` are omitted from the
  serialised plan at their defaults, so every existing plan, and every rendered rehearsal plan, is
  byte-identical and its `plan_hash` unchanged.
- **Old runners** ignore both keys (the grammar ignores unknown keys), run a sampled check and sign
  no block, which both readers print as "coverage not recorded".
- **Old readers** accept 1.4.0 documents (the major is unchanged; the block is a nested optional
  field) and check none of IV-1 to IV-7.
- **Rollback** writes 1.3.0 documents again; 1.4.0 documents stay valid.
- **Surfaces not changed:** the `Restore` and `RehearsalSchedule` CRDs, the product API
  (`verificationScope` derives from `integrity.level` and says `sampled` for a complete run, an
  understatement) and the console cannot ask for or show complete coverage — child row PROD-08.1a
  (§8).

## 5. Tests

- `crates/logweir/tests/complete_verify.rs`, 19 rows over real KBAK segments (encoded by
  `fixtures::kbak_segment`, independently of the decoder) and an engine-shaped target: a correct
  restore; the sampled block; ts-pit (08-1), ts-bound (08-2) and ts-floor (08-3) shapes, each
  against the sampled lane as its negative control; a corrupt unsampled segment; an omitted
  segment in the store and in the target; a duplicate (with the offset-keyed map's collapse
  measured); a reorder; reordered headers (08-6); compaction holes net of a recorded gap; the
  bound; a stray target partition; no lineage headers; no sha256; a manifest count that
  disagrees; last-header lineage; chunked reads; the writer's block satisfying IV-1..IV-7.
- `crates/logweir/tests/orchestrator.rs`: a complete plan through every phase signs a covered,
  exact block over all 500 records and never asks the engine for fingerprints; a record changed
  past the 25-record canary is found and scored `fail-integrity`.
- `crates/logweir-core/src/scorecard.rs`: one test per arm and conjunct, the accepted shapes, the
  position before `redactions`, the version pin, serialisation absence.
- `crates/logweir/src/drill/phase0_admit.rs` (the three refusals and three coherent controls),
  `crates/logweir/tests/phases_2_4.rs` (the complete selection), `crates/logweir/tests/show.rs`,
  `crates/logweir/tests/cli_verify.rs`, `docs/test_verify_scorecard.py`.

## 6. Live evidence (compose slot 2, engine 0.21.0, Kafka 3.7.1)

Each PROD-01.1 row restores its archive a second time with `coverage: complete`:

| Row | Sampled verdict | Complete verdict | Complete block |
|---|---|---|---|
| ts-pit (08-1) | exit 0 `pass` | exit 2 `fail-integrity` | expected 17, missing 1: `p0: source offset 5 is missing from the target` |
| ts-bound (08-2) | exit 2 `fail-integrity` (the bound) | exit 0 `pass` | expected = restored = matching = 11 |
| ts-floor full (08-3) | exit 2 `fail-integrity` | exit 2 `fail-integrity` | missing 1: `p0: source offset 1` |
| ts-floor at `+3500` (08-3) | exit 0 `pass` | exit 2 `fail-integrity` | missing 1: `p0: source offset 1` |
| shapes (08-6) | exit 2 `fail-integrity` | exit 2 `fail-integrity` | mismatched 1 (p0@6), header order verified |
| compaction | exit 0 `pass` | exit 0 `pass` | covered, 12 of 12 |

And two rows of their own:

- `complete_coverage_hashes_every_segment_outside_the_window`: a restore at `+1350` reads only each
  partition's first segment. Corrupting p1's second segment (p0's bytes copied over it) and
  removing p2's second segment both pass the sampled drill and the engine (exit 0) and fail the
  complete one (exit 2), each naming its segment (`does not match the manifest's sha256`, `the
  store does not hold it`); the unfaulted archive passes both ways.
- `complete_coverage_over_faulted_targets_on_the_real_broker_and_archive`: phase 7's complete lane
  over the slot's broker and MinIO, on targets produced from the archived records: exact → `pass`
  (24/24); a duplicate → `fail`, duplicates 1; a reorder → out_of_order 1; an omitted segment →
  missing 4; reordered headers → mismatched 1; a stray lineage offset → unexpected 1.

Outcome files: `.e2e/logweir-e2e-s2/record-semantics/*.json` (copied to the orchestrator's
artifacts for `prod-08-1`).

## 7. Cost (for PROD-10.1)

`complete_coverage_cost_per_gigabyte_and_partition` (`#[ignore]`d), three partitions of one-KiB
records in 10,000-record segments, full restores, signed phase-7 durations from the shipped debug
binary and the complete lane timed in process under `--release`. Host: Apple-silicon laptop, 10
CPUs, Docker Desktop, other workers building concurrently.

| Records | Uncompressed archive | Segments | Sampled phase 7 | Complete phase 7 (debug binary) | Complete lane, release, in process |
|---|---|---|---|---|---|
| 150,000 (50,000/partition) | 169.8 MB | 18 | 5.2 s | 30.0 s | 16.9 s (113 µs/record) |
| 450,000 (150,000/partition) | 509.5 MB | 51 | 4.7 s | 100.9 s | 28.0 s (62 µs/record) |

So, with an optimised build on this host: about **59 s per GiB** of uncompressed archive at the
larger size (a fixed cost of roughly 11 s, then about 35 s per GiB at the margin), and about
**9.3 s per partition** of 150,000 one-KiB records; the debug binary is 3–4 times slower. The
sampled check stays near 5 s whatever the size. Limits of the measurement: the values compress
extremely well (170 MB in 1.9 MB of zstd), so object-store transfer is a small share here and a
real archive pays more for it; one broker on the same host; one run per size. The bound
`sample.complete_max_records` is the control PROD-10.1 can expose; a time bound does not exist yet.

## 8. Limits and child rows

- **PROD-08.1a (proposed, P2, k8s, Tier A):** `Restore` and `RehearsalSchedule` request complete
  coverage (`spec.bounds.coverage`, `spec.bounds.completeMaxRecords`, inside `templateDigest`);
  the product API's `verificationScope` and the console read `integrity.verification` (a complete
  run reads `complete`, an incomplete one says why); the console's restore wizard offers it with
  its cost.
- **PROD-01.1b** still owns the floor and the engine's selection: complete coverage DETECTS the
  dropped and skipped records; it does not restore them.
- **Memory is ESTIMATED, not measured** (review L-8): one `BTreeMap<i64, [u8; 32]>` entry per
  expected record and one `HashSet<i64>` entry per restored record of the partition being compared
  — 40 B and 8 B of payload, roughly 80–100 B per record with node and table overhead, so about
  15 MB for a 150,000-record partition. No peak RSS was recorded; PROD-10.1 and PROD-08.3 must
  measure it. Bounded memory is PROD-08.3's.
- **A time bound** (stop after N seconds, sign incomplete) is not implemented; the record bound is.
- **Archives without lineage headers** (written with `include_offset_headers: false`) are not
  compared under complete coverage; Logweir's own backups always carry them.
- **Capture-side losses** stay invisible to any archive comparison (PROD-01.1 V3; 08-5's
  `comparison_basis: archive` says so).
- **Application validation** is not attempted (`application: notAttempted`); PROD-06.2.

## 9. Acceptance rows for the dependents

| # | Row | Pass predicate | Negative control | Fixture |
|---|---|---|---|---|
| 08.1-A1 | PROD-11.1 | A sub-window restore's complete block carries `window.start_ms` = the plan's start, and records below it are neither expected nor restored. | With the start ignored (`start_ms` absent), the records below it are reported missing. | ts rows with a stated start |
| 08.1-A2 | PROD-11.1 | A partition-subset restore's complete block names the selection and passes with every unselected target partition empty. | A record produced into an unselected partition is `unexpected` and fails. | `complete_coverage_over_faulted_targets…`, `stray` |
| 08.1-A3 | PROD-03.2 | A schema-ID rewrite passes complete coverage with `T(r)` as the expected record and every other field byte-identical. | Comparing against the untransformed `r` reports every rewritten record `mismatched`. | registry profile |
| 08.1-A4 | PROD-04.2 | Position translation runs only beside a scorecard whose complete verification PASSED for every partition it names (`integrity.result: pass`, covered, each named partition compared and exact), and maps `P` to the first target record whose lineage offset is `>= P`. | Each refuses translation, naming the field: a covered block with one record missing at or above `P` (`fail-integrity`); an `out_of_order` partition; a `duplicates` partition; a `covered: false` block; a sampled scorecard. | `complete_coverage_over_faulted_targets…` (omit, reorder, dup) and the bound row |
| 08.1-A5 | PROD-05.2 | A compacted restore passes complete coverage BEFORE the compaction transition, with `offset_holes` disclosed. | Compaction started before verification reports `missing` and fails. | compaction row |
| 08.1-A6 | PROD-08.3 | The streamed comparison yields the same block as the in-memory lane on the 450,000-record fixture. | An interrupted stream signs `covered: false`. | cost row |
| 08.1-A7 | PROD-12.1 | An OFFSET-PRESERVING replicated target compared with `comparison_key: offset` detects a missing partition and an offset shift. | With `lineage`, a target without lineage headers is unexpected throughout; with `offset`, an MM2 target is refused as mis-keyed. | an offset-preserving copy between the two PROD-01.5 clusters; MM2 only for the refusal |
| 08.1-A8 | PROD-08.2 | An exercise's result records its coverage, and a complete exercise's failure names the partition. | A sampled exercise is never shown as complete. | RehearsalSchedule over the compose topic |

---

Documentation is licensed [CC-BY-4.0](../../LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
