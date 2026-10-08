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

<!-- filled from the implementation -->

## 6. Compatibility, upgrade and rollback

- **Plan bytes.** Both fields are skipped when absent: every existing plan, rehearsal slot and `plan_hash` is unchanged.
- **Old runners** ignore both keys (the grammar ignores unknown keys) and restore the FULL selection: wider than the plan states, into new topics only, and their scorecard carries no `source.selection` block — a truthful description of the full restore they did. A plan with a selection must run on a runner of this release or later; the controller and runner ship together, so this arises only in a mixed rollback.
- **Old readers** accept 1.7.0 documents (same major) and check none of SEL-1 to SEL-7; §5.2 is what they see.
- **Rollback** writes 1.4.0–1.6.0 documents again and ignores the selection keys (above).
- **Surfaces not changed:** the `Restore` and `RehearsalSchedule` CRDs (a `Restore` carries the plan bytes opaquely, so a CLI-built or API-submitted plan can state a selection today), the product API, and the console wizard (PROD-11.1a).

## 7. Tests and evidence

<!-- filled from the runs -->

## 8. Limits

- **The engine's segment rule at the start** (E3): a segment whose last record is before the start is skipped whole, even when it holds an in-window record. Complete coverage detects it (never `pass`); the sampled lane may not. Fixing the selection is PROD-01.1b's.
- **Consumer-position mapping over a filtered restore waits for PROD-04.2**: its rule (PROD-08.1 §2.1, "Consumer positions") maps over a complete verification that passed for every partition it names; a partition the plan did not select has no position to map.
- **Offset ranges** wait for PROD-00.3 (the tracker's approach).
- **Old runners widen** (§6); a version handshake that refuses a selection on an older runner is not built.

## 9. Proposed child row

- **PROD-11.1a (P1, M2, impl, k8s, Tier A): safe clones and the console's advanced selection.** Clones (new-topic targets) get a declared TTL and cleanup policy (only targets the same execution created; never the only recovery path), an `AllowedClusters` check, explicit header handling; the console's basic restore flow stays short, advanced selection (`restore.window_start`, `restore.partitions`) appears only when chosen; reuse PLAT-11.2's preview. Depends on PROD-11.1.

## 10. Rows for the orchestrator's PoC upgrade

<!-- filled -->

---

Documentation is licensed [CC-BY-4.0](../../LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
