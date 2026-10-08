# PROD-11.1 — Replay selection: the filter contract and guard G-WIN's amendment

- Row: PROD-11.1 (impl, Tier A), [product-expansion tracker](../product-expansion.md#prod-111--add-replay-selection-and-safe-clones). Ledger row: `| 2 | PROD-11.1 | Replay selection and safe clones | P1 | M2 | impl | 01.1, 08.1 | — | k8s | A | Proposed |`.
- Date: 2026-10-08. Branch `claude/prod-11-1`, from main `93fe3f4a`.
- Scope delivered: **M1, selection with truthful evidence.** M2 (safe clones, the console's advanced selection) is proposed as child row PROD-11.1a (§9).
- Inputs: [PROD-01.1](PROD-01.1-record-semantics.md) (non-monotonic CreateTime, compaction, the window floor, S6–S9), [PROD-08.1](PROD-08.1-integrity-contract.md) §2 and §2.1 (the expected-output model, "Time filters" and "Partition subsets"), [PROD-07.1](PROD-07.1-resume-semantics.md) §2 (the engine's restore options), FX-23 (the per-partition sampled checks). The engine source is the vendored `third_party/kafka-backup-v0.23.3.tar.gz` (sha256 in `third_party/kafka-backup-v0.23.3.tar.gz.sha256`); `C23/` below is its `crates/kafka-backup-core/src/`.

## 0. Decision summary

1. **A plan may narrow a restore two ways, both in its bytes (inside `plan_hash`):** an INCLUSIVE window start, `restore.window_start`, and per-topic partition subsets, `restore.partitions`. Absent both, a restore is exactly what it was: every partition of every selected topic from the archive's floor (§3).
2. **Guard G-WIN is amended, not removed (§2).** What it refuses is a start inherited SILENTLY. A start STATED in the approved plan is admitted as the plan's own (`WindowFloorSource::InheritedFromSpec`), never earlier than the archive's coverage — a start before it is refused, never moved to the floor — re-derived by phase 5 from the spec and the manifest, and signed.
3. **One selection function** (`logweir_core::replay_selection`) decides which records, partitions and segments a plan selects, for the restore preflight's preview and for execution alike; a test resolves one archive both ways and gets the same answer (§3.3).
4. **The engine's `source_partitions` filter is run-wide**, so a plan whose topics carry different subsets is one engine run per distinct subset (§4).
5. **Every verdict is judged over the selection only** (phases 4 and 7, both lanes), and **the signed evidence states it** in a new optional block, `source.selection` (scorecard 1.7.0, MINOR), while the existing fields already name what was restored, so an older reader never reads a narrowed restore as a full one (§5).
6. **Refused, exit 3, before anything runs:** a malformed selection, a start before coverage, a partition the archive does not list, and an empty selection (§3.2). An empty restore is never a pass.

## 1. What the engine does with a window and a partition filter (0.23.3, from source)

| Fact | Evidence | Consequence |
|---|---|---|
| E1. `restore.time_window_start`/`_end` are `Option<i64>` epoch milliseconds; `source_partitions` is `Option<Vec<i32>>`. | `C23/config.rs:851-861` | Logweir renders integers, and the partition list only when a run has one. |
| E2. Each record is kept when `timestamp >= start && timestamp <= end`: both ends inclusive. | `C23/restore/helpers.rs:67-85` | The start is inclusive, like the end. Equal timestamps at the start are restored. |
| E3. A segment is read when its first/last timestamps overlap the window: `segment_end >= s && segment_start <= e`. | `C23/manifest.rs:398-408`; used at `C23/restore/engine.rs:2002` | The engine selects SEGMENTS by first/last, then records by their own timestamp. A segment whose last record is before the start is skipped whole, even when it holds an in-window record (PROD-01.1 S7, at the start). |
| E4. `source_partitions` filters `topic_backup.partitions` for EVERY topic the run restores. | `C23/restore/engine.rs:1253-1264` (restore), `:535-541` (dry run), `C23/restore/preflight.rs:252-257` (header preflight) | One filter per run. Topics with different subsets need different runs. |
| E5. A run with no matching partition for a topic returns an empty topic report and continues. | `C23/restore/engine.rs:1266-1280` | A run never silently widens to other partitions. |

## 2. Guard G-WIN's amendment

This section is the recorded amendment the row requires. It amends guard G-WIN's code and its doc comments. It is **not** an ADR 0008 amendment and **not** a `docs/stability.md` Never entry, so it needs no owner decision (rule 8).

**What G-WIN was.** Plan construction bound `RestorePlan.time_window.0` to the archive set's floor — the minimum first-record timestamp of the named topics' segments (`BackupSetFacts::earliest_covered_timestamp_ms`) — and phase 5 rendered the document and refused a `time_window_start` that was not that floor. `crates/logweir-core/src/spec.rs` said there would never be a window start, and `WindowFloorSource::InheritedFromSpec` was reserved and constructed nowhere. The hazard it closes: a later start taken from somewhere nobody approved silently drops every record before it, while the sampled lane reconciles only its sample and signs `pass`.

**What the amendment admits.** Exactly one stated start, with five properties, each enforced in code:

| # | Property | Where | Proved by |
|---|---|---|---|
| A1 | It is in the plan bytes, so inside `plan_hash`: an approver saw it, and an approval over a plan without it does not authorise the plan with it. | `RestoreSpecBlock::window_start` (skipped when absent) | `replay_selection::tests::an_absent_selection_serialises_to_the_bytes_it_had` |
| A2 | The plan says so: `window_floor_source = InheritedFromSpec`, `time_window.0` = the stated instant. | `drill::build_plan` | `replay_selection.rs::a_stated_start_and_subsets_are_bound_into_the_plan` |
| A3 | It is never earlier than the archive's coverage (the same floor G-WIN binds). A start before it is REFUSED, exit 3, naming both instants — never moved to the floor, which would restore a window nobody approved. Refused at resolution, at plan construction (the enum check's new arm) and at phase 5. | `ReplaySelection::resolve`, `build_plan_with_floor`, `phase5_preflight::check_rendered_selection` | `a_start_before_coverage_is_refused_everywhere_and_never_moved`, `an_inherited_start_before_the_floor_is_refused_at_construction`, the e2e row `refusals_before_anything_runs` |
| A4 | Phase 5 re-derives the expected start from the SPEC and the manifest — never from the plan's claim — and refuses a rendered `time_window_start` that differs, in every engine run's document. A plan whose start was moved back to the floor (a silent widening) is refused. | `check_rendered_selection` | `phase5_refuses_a_rendered_start_that_is_not_the_approved_one` |
| A5 | It is signed: `source.selection.window_start_ms`, `complete.window.start_ms`, and `sample.window_start` never earlier than it. | phase 4, phase 7, `source_info` | §5 rows |

A plan that states no start is bound, rendered, checked and signed exactly as before: G-WIN's original rows (`window_binding.rs`, ten rows) pass unchanged.

## 3. The selection contract (`logweir_core::replay_selection`)

### 3.1 The predicates

- **Record:** `ts <= end` and, when the plan states a start, `ts >= start` (`ReplaySelection::window_selects`). Without a stated start there is no lower bound: a record older than every segment's first record is still expected and its absence is the engine's floor dropping it (PROD-08.1 §2).
- **Partition:** its topic is selected and the plan names no subset for it, or names one containing it.
- **Segment (the engine's, E3):** a selected partition's segment whose first/last overlap `[start-or-floor, end]`.
- **End:** `restore.point_in_time` when stated, else `sample.window_end` (unchanged).

### 3.2 Refusals (all exit 3, before any target topic is created)

| Refusal | Code in the preview | Where execution refuses |
|---|---|---|
| a subset for a topic `source.topics` does not select; an empty subset; a repeated or negative partition | `plan.parse` `SelectionInvalid` | phase 0 |
| `window_start >= end`; `window_start > sample.window_end` | `SelectionInvalid` | phase 0 |
| `window_start` before the archive's floor | `archive.coverage` `WindowStartBeforeCoverage` | after `describe`, before phase 2 |
| a subset partition the manifest does not list | `PartitionNotInBackupSet` | the same |
| no segment of a selected partition overlaps the window | `SelectionEmpty` | the same |

### 3.3 One function for preview and execution

The restore preflight (`logweir check`, kind `restorePreflight`) projects the manifest JSON (`check::archive::topic_facts`) and resolves the plan's selection with `ReplaySelection::resolve`; execution resolves the same plan over `OsoCliEngine::describe`'s facts with the same function (`drill::resolve_selection`). `check_cli.rs::the_preview_and_execution_resolve_the_same_selection` builds one filesystem archive and asserts the two answers — segment keys, partitions, record bounds, floor, start, its source and the engine runs — are equal for three selections. The preview's `archive.segments` row checks exactly the segments the selection reads (`the_preview_checks_only_the_segments_the_selection_reads`, with a full-plan control that reports a segment the selection does not need).

## 4. Execution

- **Engine runs.** `render_restore::runs` groups the mapped topics by subset (`ReplaySelection::engine_runs`): one unfiltered run for topics without a subset, then one per distinct subset in ascending order. Each run renders its own document (`restore.run-<n>.yaml`), checkpoint and offset report (`<stem>.run-<n>.<ext>`), so two runs never share an engine file. A plan with no subset is one run whose document is byte for byte the old one (the golden snapshots are unchanged). `render` refuses a multi-run plan (`RenderError::MultipleRuns`) rather than return one run's document as the plan's.
- **Phase 5** validates every run's document with the engine and checks each against the spec (A4), including its `source_partitions` and its topics, so a subset dropped, merged or swapped is refused (`phase5_refuses_a_rendered_partition_selection_that_is_not_the_approved_one`).
- **Phase 6** runs the engine once per run, in order, refusing a run whose document is not the one phase 5 validated; the RTO spans all runs; the offset report phase 8 uploads is the runs' reports in run order (one report for a one-run plan, as before).
- **Phase 4** samples only selected partitions, from the stated start (`phase4_sample::run_selected`).
- **Phase 7** judges only the selection: the count bound and FX-23's per-partition presence check run over the selected partitions and `[start, end]`; a record in a partition the plan did not select fails the run (both lanes); the complete lane computes the expected output only for selected partitions (§5).

## 5. The signed evidence

### 5.1 `source.selection` (scorecard 1.7.0)

Present exactly when the plan states a selection. Fields:

| Field | Type | Meaning |
|---|---|---|
| `window_start_ms` | integer, optional | the plan's stated inclusive start; absent = the archive's floor |
| `window_end_ms` | integer | the inclusive end |
| `partitions` | `[{topic, partitions: [int]}]` | the per-topic subsets, sorted by topic, each list ascending and unique; a restored topic not listed was restored on every partition |
| `engine_runs` | integer ≥ 1 | how many engine runs restored it |

Arms SEL-1 to SEL-7, identical text in both readers (§5.3). Each reads only the new block or judges an existing field against it and can only refuse: MINOR under OD-7 (a) and the IV-6 precedent.

### 5.2 What a narrowed scorecard's EXISTING fields say (the orchestrator's note, 2026-10-08)

An older reader ignores `source.selection`. It must never read a narrowed restore as a full one, so the existing fields name the selection themselves:

| Existing field | Full restore | Narrowed restore |
|---|---|---|
| `sample.window_start` | the spec's sample start | never earlier than `restore.window_start` (phase 4 clamps) |
| `sample.coverage_note` | phase 4's gap notes | opens with `replay selection: …`, naming every subset and the start |
| `sample.topics`, `sample.partitions` | sampled counts | sampled counts of SELECTED partitions only |
| `integrity.verification.complete.window.start_ms` (1.4.0) | absent (the floor) | the stated start |
| `integrity.verification.complete.partitions[]` (1.4.0) | every listed partition | the selected partitions, plus any unselected partition holding a record (which fails) |

So the facts a reader of the older format can see are the selection's; nothing in them describes the archive's full coverage. No existing field changes how an older reader judges the document, so OD-7 MAJOR does not apply.

### 5.3 Arms

Both readers, identical words, in this position (after `sample.unsampled_topics`, before `redactions`); each fires only on a document carrying the block.

| Arm | Refuses | Reads | OD-7 |
|---|---|---|---|
| SEL-1 | the block under a version before 1.7.0 | the block, `format_version` | (a) |
| SEL-2 | a block with neither a start nor a subset | the block | (a) |
| SEL-3 | `window_start_ms >= window_end_ms` | the block | (a) |
| SEL-4 | subsets that are not each topic once in order, each with a non-empty, ascending list of distinct, non-negative partitions | the block | (a) |
| SEL-5 | `engine_runs: 0` | the block | (a) |
| SEL-6 | a complete block whose `window` is not the selection's start and end | the block and `complete.window` (1.4.0) | (a): judges an existing field against the block and can only refuse, as IV-6 |
| SEL-7 | a complete block expecting records (`replay.expected > 0`) from a partition the selection does not select | the block and `complete.partitions[]` | (a), as SEL-6 |

`scripts/check-verifier-parity.sh` runs four accepted documents (whose `replay selection:` lines both readers print identically) and the seven refusals through both readers; `e2e/fixtures/invariants/` carries 16 cases (one or more per arm, four accepts) and one shape case; `every_invariant_arm_has_a_corpus_case` closes the arithmetic over the seven new `return Err` statements.

### 5.4 Each signed field a selection changes, and what an older verifier concludes

| Field | Without a selection | With one | An older reader (verify_scorecard.py 1.21.0/1.22.0, `logweir` before this build) |
|---|---|---|---|
| `format_version` | 1.6.0 sampled / 1.4.0–1.5.0 complete, as before | 1.7.0 | accepts (same major) |
| `source.selection` | absent | the block (§5.1) | ignores it |
| `sample.window_start` | the spec's sample start | never earlier than `restore.window_start` | reads the narrowed start (the field it always read as "the window drilled") |
| `sample.coverage_note` | phase 4's notes | `replay selection: …; ` + phase 4's notes | `drill show` prints it: the selection in words |
| `sample.topics`, `sample.partitions`, `records_expected` | sampled counts | sampled counts of selected partitions only | the selection's counts |
| `integrity.*` verdict | over every partition from the floor | over the selection only (§4) | the same verdict, now about the selection |
| `integrity.verification.complete.window.start_ms` | absent (the floor) | the stated start | the narrowed start (a 1.4.0 field defined for exactly this) |
| `integrity.verification.complete.partitions[]` | every listed partition | the selected partitions, plus an unselected one only when it holds a record (then failing) | the selection's partitions |
| `evidence.offset_report_*` | the engine's report | one run: the engine's report; several: a JSON array of the runs' reports | the digest it always checked |

**The row that proves it** (live, slot 3): the narrowed scorecard of `the_start_is_inclusive_…`'s `after` restore (start `S+1`, complete coverage, 1.7.0) was checked by three readers: `verify_scorecard.py` from main `93fe3f4a` (1.21.0), from `claude/fx-23` (1.22.0) and from this branch (1.23.0). All three: `VALID`, exit 0, `integrity coverage: every selected record compared: 6 expected, 6 restored, 6 matching …`; only 1.23.0 prints `replay selection: every partition of every restored topic, from epoch-ms 1760000010001 (the plan's restore.window_start, inclusive) …`. None prints a claim of a full restore; the document's `sample.window_start` is `2025-10-09T08:53:30.001Z` (= the start) and its `sample.coverage_note` opens with the selection (artifacts `claude/artifacts/prod-11-1/old-reader/`). So no older reader reads it as a full restore, and OD-7 MAJOR does not apply.

## 6. Compatibility, upgrade and rollback

- **Plan bytes.** Both fields are skipped when absent: every existing plan, rehearsal slot and `plan_hash` is unchanged.
- **Old runners** ignore both keys (the grammar ignores unknown keys) and restore the FULL selection: wider than the plan states, into new topics only, and their scorecard carries no `source.selection` block — a truthful description of the full restore they did. A plan with a selection must run on a runner of this release or later; the controller and runner ship together, so this arises only in a mixed rollback.
- **Old readers** accept 1.7.0 documents (same major) and check none of SEL-1 to SEL-7; §5.2 is what they see.
- **Rollback** writes 1.4.0–1.6.0 documents again and ignores the selection keys (above).
- **Surfaces not changed:** the `Restore` and `RehearsalSchedule` CRDs (a `Restore` carries the plan bytes opaquely, so a CLI-built or API-submitted plan can state a selection today), the product API, and the console wizard (PROD-11.1a).

## 7. Tests and evidence

Unit and seam rows (all passing at the tip):

| File | Rows |
|---|---|
| `crates/logweir-core/src/replay_selection.rs` | 8: both ends inclusive and an absent start bounds nothing; partition selection; a start before coverage refused, at the floor the plan's own; the engine's segment rule over selected partitions with the record bound; different subsets are different runs; an empty selection and an unlisted partition refused; the shape refusals (incl. a start after `sample.window_end`); an absent selection keeps its bytes |
| `crates/logweir/tests/replay_selection.rs` | 9: binding into the plan; no selection is the floor as before; a start before coverage refused at resolution, construction and phase 5; the enum check's new arm; phase 5 refuses a start moved to the floor and the reverse; phase 5 refuses a dropped, merged or swapped subset; unlisted partition and empty selection; a start at a segment's last record still selects it; phase 4 samples only selected partitions from the start |
| `crates/logweir-engine-oso/tests/render_selection.rs` | 4: one unfiltered run (the golden document); one shared subset; three runs with their own files and `render` refusing; an unmapped subset adds no run |
| `crates/logweir-engine-oso/tests/engine_runs.rs` | 6: three runs validated and restored in order with the composed report; one run as before; divergence refused; a failing run named and the rest not started; merged preflight reports only worse; engine reports merged only when all read |
| `crates/logweir/tests/complete_verify.rs` | 3: a subset passes complete with the block naming the selection (controls: a stray record fails; without the subset the empty partition is missing); a sub-window passes and signs its start (controls: start ignored → missing; a record below the start → unexpected); the sampled lane judges over the selection (controls: no subset → fails by name; a stray record → fails naming the selection) |
| `crates/logweir/tests/orchestrator.rs` | 1: a stated selection is restored and signed 1.7.0 with its block and coverage note; control without a selection keeps its version |
| `crates/logweir/tests/check_cli.rs` | 4: the preview refuses a start before coverage; names an unlisted partition, an empty selection and a malformed subset; checks only the selection's segments (control: full plan reports the missing segment); preview and execution resolve the same selection |
| `crates/logweir-core/src/scorecard.rs` | 6: version rule and accepted shapes; SEL-1; SEL-2 to SEL-5; SEL-6 and SEL-7; arm position; the coverage note |
| `docs/test_verify_scorecard.py` | 6 (+2 updated): the minor, accepts, SEL-1 to SEL-5, SEL-6/7, the shape, the line |

**Mutants (17, all killed by a failing assertion, none by a compile error;** `claude/artifacts/prod-11-1/mutants.json`): the record predicate's inclusive start; the partition predicate; the coverage refusal in `resolve`; the plan's InheritedFromSpec arm; phase 5's start comparison; phase 5's subset comparison; phase 4's partition filter; phase 4's start clamp; phase 7's bound over the whole archive; phase 7's unselected-partition finding; the complete lane's unselected expectation; the engine adapter comparing one digest; the renderer dropping `source_partitions`; SEL-7; the 1.7.0 version step; the block not signed; the preview's segments row reading the floor.

**Live rows (compose slot 3, Kafka 3.7.1, engine `0.23.3+logweir.1` native arm64, `e2e/tests/replay_selection.rs`, 6 passed in 349 s; outcomes in `claude/artifacts/prod-11-1/e2e-run1/`):**

| Row | Observed |
|---|---|
| inclusive / exclusive at the start, equal and non-monotonic timestamps | start `S`: 9 records restored, both records at `S` included, `S-1` and `S-50` not, oracle diff empty, `pass`, `window_start_ms = S`, `complete.window.start_ms = S`, `sample.window_start = S`; start `S+1`: 6 restored, the two at `S` excluded, `pass` |
| a segment whose last record is before the start | the engine skipped p0's first segment and lost `S+50` (oracle: exactly that record missing); complete coverage: `fail-integrity`, exit 2, `missing: 1` — never `pass` |
| subsets `A: [0, 2]`, `B: [1]`, topic C not selected | `engine_runs: 2`; every selected partition exact, every unselected one empty, C not restored; `pass` under complete and sampled coverage |
| refusals | start 1 ms before coverage, an empty selection, an existing target name: exit 3, no scorecard, no target created; control at the floor: `pass` |
| compaction hole in a sub-window | target = the compacted log's records from the start (offsets 2–5 on p0); `pass`; `offset_holes: 3` (one per partition) |
| a newer backup set after approval | B2 written under the same archive prefix; the run restored B1's selection exactly and signed `backup_id = B1` |

Before the selection was executable, the same refusal row ran live against the fail-closed build: the three refusals as above and the control refused `SelectionNotYetExecutable` (`claude/artifacts/prod-11-1/failclosed/`).

## 8. Limits

- **The engine's segment rule at the start** (E3): a segment whose last record is before the start is skipped whole, even when it holds an in-window record. Complete coverage detects it (never `pass`); the sampled lane may not. Fixing the selection is PROD-01.1b's.
- **Consumer-position mapping over a filtered restore waits for PROD-04.2**: its rule (PROD-08.1 §2.1, "Consumer positions") maps over a complete verification that passed for every partition it names; a partition the plan did not select has no position to map.
- **Offset ranges** wait for PROD-00.3 (the tracker's approach).
- **Old runners widen** (§6); a version handshake that refuses a selection on an older runner is not built.

## 9. Proposed child row

- **PROD-11.1a (P1, M2, impl, k8s, Tier A): safe clones and the console's advanced selection.** Clones (new-topic targets) get a declared TTL and cleanup policy (only targets the same execution created; never the only recovery path), an `AllowedClusters` check, explicit header handling; the console's basic restore flow stays short, advanced selection (`restore.window_start`, `restore.partitions`) appears only when chosen; reuse PLAT-11.2's preview. Depends on PROD-11.1.

## 10. Rows for the orchestrator's PoC upgrade

No CRD changes. The runner image changes (selection, engine runs, phase 7, the block); the controller changes only in that rehearsal slots render the two new fields absent (byte-identical plans). Rows, each with its predicate and control:

| # | Row | Predicate | Control |
|---|---|---|---|
| K1 | A `Restore` (`newTopic`) whose `planBytes` state `restore.window_start` and `restore.partitions` with two different subsets, over a PoC topic with three partitions | Job `Succeeded`; signed scorecard 1.7.0 with `source.selection` (`engine_runs: 2`), `integrity.result: pass`; each target topic's unselected partitions empty and selected ones holding exactly the archive's records from the start | the same plan without the selection: scorecard 1.6.0, no block, every partition restored |
| K2 | A `Preflight` (restore) over a plan whose `window_start` is 1 ms before the recovery point's earliest covered timestamp | `archive.coverage` `notReady`, `WindowStartBeforeCoverage` | start at the floor: `ready`, `PointInTimeCovered`, message names the selection and its runs |
| K3 | A `Restore` whose plan's `restore.partitions` names a partition the archive does not list | the Job exits 3 before any target topic exists; `status.exitReason` `GuardRefused`; no scorecard | — |
| K4 | An existing `RehearsalSchedule`'s next slot after the upgrade | its plan bytes and `templateDigest` are byte-identical to the pre-upgrade slot's (no `window_start`/`partitions` keys) | — |
| K5 | The K1 scorecard downloaded through the product API, checked by `logweir drill verify` and `verify_scorecard.py` 1.23.0, and by the 1.22.0 script | 1.23.0 readers: `VALID` and the `replay selection:` line; 1.22.0: `VALID`, no line, `every selected record compared` | the K1 control's scorecard: no selection line from any reader |

---

Documentation is licensed [CC-BY-4.0](../../LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
