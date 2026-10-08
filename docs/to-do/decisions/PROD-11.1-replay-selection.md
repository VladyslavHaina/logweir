# PROD-11.1 — Replay selection: the filter contract and guard G-WIN's amendment

- Row: PROD-11.1 (impl, Tier A), [product-expansion tracker](../product-expansion.md#prod-111--add-replay-selection-and-safe-clones). Ledger row: `| 2 | PROD-11.1 | Replay selection and safe clones | P1 | M2 | impl | 01.1, 08.1 | — | k8s | A | Proposed |`.
- Date: 2026-10-08. Branch `claude/prod-11-1`, from main `93fe3f4a`; fix round after the M1 review (§11), main merged at `ad5e2ddb`.
- Scope delivered: **M1, a window START with truthful evidence.** Partition subsets are REFUSED by name until the owner decides OD-9 (§5.5). M2 (safe clones, the console's selection) is proposed as child row PROD-11.1a (§9).
- Inputs: [PROD-01.1](PROD-01.1-record-semantics.md) (non-monotonic CreateTime, compaction, the window floor, S6–S9), [PROD-08.1](PROD-08.1-integrity-contract.md) §2 and §2.1 (the expected-output model, "Time filters" and "Partition subsets"), [PROD-07.1](PROD-07.1-resume-semantics.md) §2 (the engine's restore options), FX-23 (the per-partition sampled checks). The engine source is the vendored `third_party/kafka-backup-v0.23.3.tar.gz` (sha256 in `third_party/kafka-backup-v0.23.3.tar.gz.sha256`); `C23/` below is its `crates/kafka-backup-core/src/`.

## 0. Decision summary

1. **A plan may state an INCLUSIVE window start, in its bytes (inside `plan_hash`), as the interval form of its point in time: `restore.point_in_time: "<start>/<end>"`.** It is the only grammar for a start: a `restore.window_start` key is refused at parse. A single instant is exactly what it was: every partition of every selected topic from the archive's floor (§3).
2. **A partition subset (`restore.partitions`) is refused by name** (`PartitionSubsetsAwaitOwnerDecision`, exit 3 at phase 0, `SelectionInvalid` in the preview) until the owner decides OD-9: a subset-narrowed scorecard is read as a full restore by every verifier before it (the review's H1), so it is MAJOR under OD-7 (§5.5).
3. **Guard G-WIN is amended, not removed (§2).** What it refuses is a start inherited SILENTLY. A start STATED in the approved plan is admitted as the plan's own (`WindowFloorSource::InheritedFromSpec`), never earlier than the archive's coverage — a start before it is refused, never moved to the floor — re-derived by phase 5 from the spec and the manifest, signed, and refused under a standing rehearsal authorization (A6).
4. **One selection function** (`logweir_core::replay_selection`) decides which records and segments a plan selects, for the restore preflight's preview and for execution alike; a test resolves one archive both ways and gets the same answer (§3.3).
5. **Every verdict is judged over the window only** (phases 4 and 7, both lanes), and **the signed evidence states it** in a new optional block, `source.selection {window_start_ms, window_end_ms}` (scorecard 1.7.0, MINOR), while the existing fields already name the start, so an older reader never reads it as a restore from the floor (§5).
6. **Refused, exit 3, before anything runs:** any partition subset, a start at or after the end, a start before coverage, and an empty window (§3.2). An empty restore is never a pass.
7. **A runner that predates PROD-11.1 refuses a plan with a start**: it cannot parse the interval form (`drill spec does not parse`, exit 1), so it never restores from the floor what the plan said to restore from a start (§6, live row).

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

**What the amendment admits.** Exactly one stated start, with six properties, each enforced in code:

| # | Property | Where | Proved by |
|---|---|---|---|
| A1 | It is in the plan bytes, so inside `plan_hash`: an approver saw it, and an approval over a plan without it does not authorise the plan with it. A single-instant `point_in_time` serialises as before. | `RestoreSpecBlock` (the interval form of `point_in_time`) | `replay_selection::tests::an_absent_selection_serialises_to_the_bytes_it_had`, `a_start_is_written_only_as_the_interval_form_of_point_in_time` |
| A2 | The plan says so: `window_floor_source = InheritedFromSpec`, `time_window.0` = the stated instant. | `drill::build_plan` | `replay_selection.rs::a_stated_start_is_bound_into_the_plan` |
| A3 | It is never earlier than the archive's coverage (the same floor G-WIN binds). A start before it is REFUSED, exit 3, naming both instants — never moved to the floor, which would restore a window nobody approved. Refused at resolution, at plan construction (the enum check's new arm) and at phase 5. | `ReplaySelection::resolve`, `build_plan_with_floor`, `phase5_preflight::check_rendered_selection` | `a_start_before_coverage_is_refused_everywhere_and_never_moved`, `an_inherited_start_before_the_floor_is_refused_at_construction` (at `FLOOR_MS - 60 000` and `FLOOR_MS - 1`, admitted at `FLOOR_MS`: review L1), the e2e row `refusals_before_anything_runs` |
| A4 | Phase 5 re-derives the expected start from the SPEC and the manifest — never from the plan's claim — and refuses a rendered `time_window_start` that differs. A plan whose start was moved back to the floor (a silent widening) is refused. | `check_rendered_selection` | `phase5_refuses_a_rendered_start_that_is_not_the_approved_one` |
| A5 | It is signed: `source.selection.window_start_ms`, `complete.window.start_ms`, and `sample.window_start` never earlier than it. | phase 4, phase 7, `source_info` | §5 rows |
| A6 | A STANDING rehearsal authorization admits no start (review M1): its plan bytes are not hash-bound to an approval, so `plan_within_scope` refuses a plan stating a start or a subset — a rehearsal restores every partition from the floor. | `execution_contract::plan_scope_facts` (`replay_selection`) and `plan_within_scope` | `execution_contract::tests::a_plan_stating_a_replay_selection_is_outside_every_standing_scope` (control: the same plan without them is within scope) |

A plan that states no start is bound, rendered, checked and signed exactly as before: G-WIN's original rows (`window_binding.rs`, ten rows) pass unchanged.

## 3. The selection contract (`logweir_core::replay_selection`)

### 3.1 The predicates

- **Record:** `ts <= end` and, when the plan states a start, `ts >= start` (`ReplaySelection::window_selects`). Without a stated start there is no lower bound: a record older than every segment's first record is still expected and its absence is the engine's floor dropping it (PROD-08.1 §2).
- **Partition:** every partition of every selected topic. A topic subset is the topics `source.topics` names. A partition subset is refused (§0.2).
- **Segment (the engine's, E3):** a segment whose first/last overlap `[start-or-floor, end]`.
- **End:** the end of `restore.point_in_time` when stated, else `sample.window_end` (unchanged).

### 3.2 Refusals (all exit 3, before any target topic is created)

| Refusal | Code in the preview | Where execution refuses |
|---|---|---|
| any `restore.partitions` (`PartitionSubsetsAwaitOwnerDecision: restore.partitions names a partition subset of …`) | `plan.parse` `SelectionInvalid` | phase 0, first, before every other selection check |
| a start at or after the end; a start after `sample.window_end` | `SelectionInvalid` | phase 0 |
| a start before the archive's floor | `archive.coverage` `WindowStartBeforeCoverage` | after `describe`, before phase 2 |
| no segment overlaps the window | `SelectionEmpty` | the same |
| a `restore.window_start` key | `plan.parse` (the plan does not parse) | parsing the plan, naming the interval form |

### 3.3 One function for preview and execution

The restore preflight (`logweir check`, kind `restorePreflight`) projects the manifest JSON (`check::archive::topic_facts`) and resolves the plan's selection with `ReplaySelection::resolve`; execution resolves the same plan over `OsoCliEngine::describe`'s facts with the same function (`drill::resolve_selection`). `check_cli.rs::the_preview_and_execution_resolve_the_same_selection` builds one filesystem archive and asserts the two answers — segment keys, record bounds, floor, start and its source — are equal. The preview's `archive.segments` row checks exactly the segments the selection reads (`the_preview_checks_only_the_segments_the_selection_reads`, with a full-plan control that reports a segment the selection does not need).

## 4. Execution

- **One engine run.** A plan with a start renders one document with `time_window_start` = the start; a plan without one is byte for byte the old document (the golden snapshots are unchanged).
- **Phase 5** validates the document with the engine and checks it against the spec (A4).
- **Phase 4** samples from the stated start (`phase4_sample::run_selected`).
- **Phase 7** judges only the window: the count bound and FX-23's per-partition presence check run over `[start, end]`; the complete lane computes the expected output by each record's own timestamp in it (§5).
- **The multi-run engine code stays and is unreachable.** M1 built one engine run per distinct partition subset (the engine's `source_partitions` filter applies to every topic of a run, E4), with per-run documents, checkpoints and offset reports, phase 5 checks of each run and phase 7's unselected-partition findings. `ReplaySelection::from_spec` refuses any subset FIRST, before a selection exists, so every one of those paths sees a selection with no subset and renders one run. The code and its rows (`render_selection.rs`, `engine_runs.rs`, `complete_verify.rs`'s subset rows) are kept for OD-9 (a); they are the only consumers, and the refusal is the only path from a plan to them (rows: `a_partition_subset_is_refused_by_name_on_every_path` — resolution and plan construction both refuse, with and without a start — and the orchestrator row `a_partition_subset_is_refused_by_name_and_never_signed`, a whole run refused at exit 3 with nothing signed; mutant F1, §7). `compose_offset_reports` refuses a run path equal to its output (review L5).

## 5. The signed evidence

### 5.1 `source.selection` (scorecard 1.7.0)

Present exactly when the plan states a start. Fields:

| Field | Type | Meaning |
|---|---|---|
| `window_start_ms` | integer | the plan's stated inclusive start, never earlier than the archive's floor |
| `window_end_ms` | integer | the inclusive end |

Arms SEL-1 to SEL-3, identical text in both readers (§5.3). Each reads only the new block or judges an existing field against it and can only refuse: MINOR under OD-7 (a) and the IV-6 precedent. **The version only rises:** `scorecard::newer_format_version` is the one comparison every version step uses (`format_version_with_selection`, `format_version_with_sample`), so a step never lowers a version an earlier one chose; a sampled restore from a start is 1.7.0 (row `every_version_step_takes_the_newer_minor`; live, the topics row's sampled scorecard).

### 5.2 What a 1.7.0 scorecard's EXISTING fields say (the orchestrator's note, 2026-10-08)

An older reader ignores `source.selection`. It must never read a restore from a stated start as one from the floor, so the existing fields name the start themselves:

| Existing field | Restore from the floor | Restore from a stated start |
|---|---|---|
| `sample.window_start` | the spec's sample start | never earlier than the stated start (phase 4 clamps) |
| `sample.coverage_note` | phase 4's gap notes | opens with `replay selection: every partition of every restored topic, from epoch-ms S (the plan's stated window start, inclusive) to epoch-ms E (inclusive); no record before the start was restored or expected`; under the sampled lane it then names the one limit a start adds (review L6) |
| `integrity.verification.complete.window.start_ms` (1.4.0) | absent (the floor) | the stated start |
| `integrity.verification.complete.partitions[]` (1.4.0) | every partition of every restored topic | the same: every partition is restored, so the 1.4.0 definition ("one entry per partition of every restored topic") holds unchanged |

`complete.window.start_ms` was defined in 1.4.0 for exactly this, and `sample.window_start` is "the point-in-time window drilled". So nothing an older reader can see describes the archive's full coverage, and no existing field changes how it judges the document: OD-7 MAJOR does not apply to a start. **It did apply to partition subsets** (§5.5), which is why they are refused: the M1 version of this section claimed the same for subsets, and the review showed it false (`complete.partitions[]` omitted unselected partitions, read by an older reader as every partition, and the sampled-pass line claimed every mapped partition was held to its bound).

### 5.3 Arms

Both readers, identical words, in this position (after `sample.unsampled_topics`, before `redactions`); each fires only on a document carrying the block.

| Arm | Refuses | Reads | OD-7 |
|---|---|---|---|
| SEL-1 | the block under a version before 1.7.0 | the block, `format_version` | (a) |
| SEL-2 | `window_start_ms >= window_end_ms` | the block | (a) |
| SEL-3 | a complete block whose `window` is not the block's start and end | the block and `complete.window` (1.4.0) | (a): judges an existing field against the block and can only refuse, as IV-6 |

A block that is not an object with both fields as integers is refused when the document is read (shape case `selection_not_an_object`); unknown keys in it are ignored, as everywhere in the document. `scripts/check-verifier-parity.sh` runs three accepted documents (a sampled and a complete pass from a start, a sampled pass without the block) — both readers print the same `replay selection:` and `sample coverage:` lines, compared whole, the sampled one QUALIFIED by the window — and the three refusals through both readers; `e2e/fixtures/invariants/` carries five cases (two accepts, one per arm) and one shape case; `every_invariant_arm_has_a_corpus_case` closes the arithmetic over the three `return Err` statements.

**The sampled-pass line (review H1, "in either case").** For a sampled `pass` over a document carrying the block, both 1.23.0 readers print `sample coverage: a sampled pass over a replay selection from epoch-ms S to epoch-ms E: every mapped partition was held to its own count bound over that window, max_partitions reached every topic before a second partition of any, and a readable engine report lacking a partition with records in that window was refused; no record before the start was restored or expected` instead of the unqualified 1.6.0 line (`verify::sampled_pass_lines_over`, `_sampled_pass_lines`; row `cli_verify.rs::a_sampled_pass_over_a_selection_says_so`, the parity loop, the e2e topics row).

### 5.4 Each signed field a start changes, and what an older verifier concludes

| Field | Without a start | With one | An older reader (verify_scorecard.py 1.21.0/1.22.0, `logweir` before this build) |
|---|---|---|---|
| `format_version` | 1.6.0 sampled / 1.4.0–1.5.0 complete, as before | 1.7.0 | accepts (same major) |
| `source.selection` | absent | the block (§5.1) | ignores it |
| `sample.window_start` | the spec's sample start | never earlier than the start | reads the start (the field it always read as "the window drilled") |
| `sample.coverage_note` | phase 4's notes | `replay selection: …; ` + phase 4's notes | `drill show` prints it: the window in words |
| `sample.topics`, `sample.partitions`, `records_expected` | sampled counts | sampled counts over the window | the window's counts |
| `integrity.*` verdict | over every partition from the floor | over every partition from the start (§4) | the same verdict, about the window |
| `integrity.verification.complete.window.start_ms` | absent (the floor) | the stated start | the start (a 1.4.0 field defined for exactly this) |
| `integrity.verification.complete.partitions[]` | every partition | every partition | every partition — true |

**The rows that prove it** (live, slot 2 with `COMPOSE_PROFILES=auth`): the two signed 1.7.0 scorecards of `a_topic_subset_from_a_start_is_restored_and_signed_under_both_coverages` (topics A and B of three, from `S = 1760000000030` to `E = 1760000010000`; `sample.window_start` `2025-10-09T08:53:20.030Z` = the start, `sample.coverage_note` opening with the selection) were read by `verify_scorecard.py` 1.21.0 (main `93fe3f4a`), 1.22.0 (main `ad5e2ddb`), this branch's 1.23.0, and main's `logweir drill verify` (`ad5e2ddb`). All four: `VALID`, exit 0, on both.

- **Sampled, 1.22.0 and main's `drill verify`:** `sample coverage: a sampled pass at format 1.6.0 or later: every mapped partition was held to its own count bound, max_partitions reached every topic before a second partition of any, and a readable engine report lacking a partition with records in the window was refused`. TRUE of this restore: every partition of A and B is mapped and was held to its bound over the restore window, which is `[S, E]`. **1.21.0** prints only `integrity coverage: sampled (compared with the archive; …)`. No reader claims a restore from the floor.
- **Complete, 1.21.0, 1.22.0 and main's `drill verify`:** `integrity coverage: every selected record compared: 18 expected, 18 restored, 18 matching, 0 missing, 0 unexpected, 0 duplicates, 0 out of order, 0 different; 6 of 6 segments verified, 0 failed, 0 unverified; 0 offset holes`. TRUE: `complete.partitions[]` lists all six partitions of A and B, each held to the archive's records in `[S, E]`, and `complete.window.start_ms` = `S`.
- **1.23.0** adds `replay selection: every partition of every restored topic, from epoch-ms 1760000000030 (the plan's stated window start, inclusive) to epoch-ms 1760000010000 (inclusive); no record before the start was restored or expected` to both, and for the sampled one prints `sample coverage: a sampled pass over a replay selection from epoch-ms 1760000000030 to epoch-ms 1760000010000: …` instead of the unqualified line.

So no older reader reads a restore from a stated start as one from the floor, and OD-7 MAJOR does not apply to a start. Outputs: `claude/artifacts/prod-11-1/fix-round/old-reader/` (the run at `3e83c0b4`) and `fix-round/old-reader-tip/` (the scorecards of the run at `780e5b70`, the same lines; this branch's `drill verify` prints what 1.23.0 prints).

### 5.5 The owner question: partition subsets (OD-9, proposed 2026-10-08)

**Question.** How is a partition-subset restore's scorecard versioned, so that no verifier reads it as a full restore?

**Proposed: (a) a new major, 2.0.0, written ONLY when the plan states a subset.** Every reader before it refuses an unknown major, so none can misread one; every other document (no subset, or a start only) stays 1.x and every existing reader keeps reading it. The 2.0.0 block would carry the subsets and the engine runs (M1's shape), `complete.partitions[]` would be defined over the SELECTED partitions, and the sampled-pass line would say every SELECTED partition was held to its bound and every unselected partition of a restored topic was held empty. A MINOR cannot carry it: unknown keys in `source.selection` are ignored, so a 1.23.0 reader would read a subset block as a start-only one.

**Its cost.** Both verifiers learn major 2 for this one shape (a second accepted-major arm, its corpus and parity rows); an adopter whose verifier predates it must upgrade to read a subset restore (it is refused, never misread); the multi-run engine code kept in the tree (§4) is re-enabled behind the new version and re-proved live; the scorecard schema gains a second published file.

The other options, as the tracker states them: (b) accept that older readers misread subset documents, as documented; (c) keep refusing subsets. Until the owner decides, (c) is in force: `restore.partitions` is refused by name, so no subset document exists.

## 6. Compatibility, upgrade and rollback

- **Plan bytes.** A single-instant `point_in_time` serialises exactly as before: every existing plan, rehearsal slot and `plan_hash` is unchanged.
- **Old runners REFUSE a plan with a start (review M2).** The mechanism is one an older runner already has: `RestoreSpecBlock::point_in_time` was a single RFC 3339 `DateTime`, so the interval form `"<start>/<end>"` does not deserialise and the runner stops at `drill spec does not parse`, exit 1, in `context()` — before any client, broker, bucket or target topic. It is used ONLY for a plan that states a start; a plan without one keeps the old bytes. An older controller's standing-authorization path refuses the same bytes at its own plan parse (`spec.planBytes does not parse as a restore plan`, main's `weirkeeper/src/controllers/restore.rs:1228`); the per-run path renders the Job and the older runner refuses. Live row `an_older_runner_refuses_a_plan_stating_a_start` ran main's runner (`ad5e2ddb`) on the plan: refused, nothing created, nothing signed; the same binary restores the same plan without the start (the control). So this does not arise only in a mixed rollback, and no runner "reports PROD-11.1 support": the refusal is the older runner's own parse.
- **The residual.** An older runner IGNORES a `restore.partitions` key (main's `RestoreSpecBlock` has no `deny_unknown_fields`) and restores every partition — the same live row shows it, signed `pass` with no selection block. No Logweir writer emits that key, and this release refuses it by name; a hand-written plan stating one and run on an older runner widens. The interval mechanism cannot cover it: a subset is a separate key, not a value an older parser rejects. When OD-9 re-enables subsets, their plan form needs its own refusal on older runners (for example, carried inside the interval value, or behind an execution-contract version an older runner refuses).
- **Old readers** accept 1.7.0 documents (same major) and check none of SEL-1 to SEL-3; §5.2 and §5.4 are what they see.
- **Rollback** writes 1.4.0–1.6.0 documents again; a pending plan with a start is refused by the older runner (above).
- **Surfaces not changed:** the `Restore` and `RehearsalSchedule` CRDs (a `Restore` carries the plan bytes opaquely, so a CLI-built or API-submitted plan can state a start today), the product API, and the console wizard (PROD-11.1a).

## 7. Tests and evidence

Unit and seam rows (all passing at the tip):

| File | Rows |
|---|---|
| `crates/logweir-core/src/replay_selection.rs` | 9: both ends inclusive and an absent start bounds nothing; partition selection; a start before coverage refused, at the floor the plan's own; the engine's segment rule with the record bound; different subsets are different runs (the kept code); the shape refusals, a partition subset first; a start written only as the interval form (a `restore.window_start` key refused at parse, an interval whose end does not parse refused, the round trip, and an older runner's single-instant type refusing the interval); an absent selection keeps its bytes; an empty window refused |
| `crates/logweir-core/src/execution_contract.rs` | 1 (A6): a plan stating a start or a subset is outside every standing scope; control within |
| `crates/logweir-core/src/time_basis.rs` | 1 (L2): a start over a `LogAppendTime` topic is refused naming `restore.point_in_time S/E` |
| `crates/logweir-core/src/scorecard.rs` | SEL-1, SEL-2, SEL-3, arm position, the coverage note, `every_version_step_takes_the_newer_minor` |
| `crates/logweir/tests/replay_selection.rs` | 10: binding into the plan; no selection is the floor as before; a partition subset refused on every path; a start before coverage refused at resolution, construction (`FLOOR_MS - 60 000` and `FLOOR_MS - 1`, admitted at `FLOOR_MS`) and phase 5; phase 5 refuses a start moved to the floor and the reverse; an empty window; a start at a segment's last record still selects it; phase 4 samples from the start |
| `crates/logweir/tests/orchestrator.rs` | 2: a partition subset refused by name, exit 3, nothing signed; a stated start restored and signed 1.7.0 with its block and coverage note (control without one keeps its version) |
| `crates/logweir/tests/check_cli.rs` | 4: the preview refuses a start before coverage; refuses a partition subset by name and names an empty window; checks only the window's segments (control: the full plan reports the missing segment); preview and execution resolve the same selection |
| `crates/logweir/tests/cli_verify.rs` | 1: a sampled pass over a selection prints the qualified line; without the block the 1.6.0 line; complete or non-pass nothing |
| `crates/logweir-engine-oso/tests/engine_runs.rs` | + 1 (L5): composing a report into itself is refused |
| `docs/test_verify_scorecard.py` | the minor, accepts, SEL-1 to SEL-3, the shape, the selection line and the qualified sampled-pass line |

**Mutants of the fix round (11, each killed by a failing assertion, none by a compile error;** `claude/artifacts/prod-11-1/fix-round/mutants.json`): F1 the partition-subset refusal deleted (killed by `a_partition_subset_is_refused_by_name_on_every_path` and, separately, by the whole-run row `a_partition_subset_is_refused_by_name_and_never_signed`); F2 the standing-scope selection refusal deleted (A6); F3 the sampled version step not monotonic; F4 plan construction's floor check off by one (the review's surviving R5, now killed by the `FLOOR_MS - 1` case); F5 a `restore.window_start` key accepted; F6 the qualified sampled-pass line dropped; F7 SEL-3 deleted; F8 `compose_offset_reports`' self-copy guard deleted — the row as first written HUNG under it (the file reached 368 MB in 28 minutes before I stopped it), so the row now runs the call on a thread with a 30-second bound and the mutant FAILS it; F9 the interval's start ignored by the selection; F10 the sampled lane's start limit not named. M1's 17 mutants (`claude/artifacts/prod-11-1/mutants.json`) were not re-run; the ones over subset paths (M2, M6, M7, M10, M11, M13, M14) target code the refusal now makes unreachable from a plan, and M14's arm (SEL-7) is gone.

**Live rows (compose slot 2, `COMPOSE_PROFILES=auth`, Kafka 3.7.1, engine `0.23.3+logweir.1` native arm64, `e2e/tests/replay_selection.rs`):** 8 passed in 344 s at `780e5b70` (the older-runner row included, with `LOGWEIR_E2E_OLDER_RUNNER` naming main's `logweir` built at `ad5e2ddb`); outcomes in `claude/artifacts/prod-11-1/fix-round/e2e-tip/`. A first run at `3e83c0b4` passed 5 of 6 and the older-runner row, and failed the new-point row: its fixture had a partition whose records all preceded the start (§8; `fix-round/e2e-run1/`).

| Row | Observed |
|---|---|
| inclusive / exclusive at the start, equal and non-monotonic timestamps | start `S`: both records at `S` restored, `S-1` and `S-50` not, oracle diff empty, `pass`, `window_start_ms = S`; start `S+1`: the two at `S` excluded, `pass` |
| a segment whose last record is before the start | the engine skipped p0's first segment and lost `S+50` (oracle: exactly that record missing); complete coverage `fail-integrity`, exit 2 — never `pass` |
| a topic subset from a start, both coverages | topics A and B of three from `T+30`: every partition exact, C not restored; `pass`, format 1.7.0, `source.selection` = `{window_start_ms: 1760000000030, window_end_ms: 1760000010000}` and nothing else, under complete AND sampled (the version step is monotonic); both readers exit 0 with the same `replay selection:` line, the sampled one with the qualified `sample coverage:` line |
| refusals | a start 1 ms before coverage, an empty window, an existing target, a partition subset without and with a start (`guard: plan refused by the admission guard: PartitionSubsetsAwaitOwnerDecision: restore.partitions names a partition subset of …`): exit 3, nothing signed, no target created; control at the floor `pass` |
| compaction hole in a sub-window | exact, `pass`, `offset_holes: 3` |
| a newer backup set after approval | B1's window restored exactly on every partition, `backup_id = B1`, `pass` |
| a partition with nothing in the window | p2's records all before the start: exit 2, `preflight-failed`, phase 5 note `<topic>/2 empty: no records in the selected window for this partition`, no target topic; the block still signed |
| an older runner (main `ad5e2ddb`) | plan with a start: exit 1, `operational: drill spec does not parse: restore.point_in_time: trailing input at line 22 column 18`, no target topic, nothing signed; control (same plan, single instant): `pass`, 7 of 7 restored; residual (single instant + `restore.partitions: {topic: [0]}`): `pass`, 7 of 7 restored on every partition, no selection block |

## 8. Limits

- **The engine's segment rule at the start** (E3): a segment whose last record is before the start is skipped whole, even when it holds an in-window record. Complete coverage detects it (never `pass`); the sampled lane may not, and a sampled scorecard from a start says so in `sample.coverage_note` (review L6). Fixing the selection is PROD-01.1b's.
- **A partition with no record in the window is `preflight-failed`** (exit 2, signed, naming it, nothing created): phase 5's existing rule that the engine's `empty` header-preflight state is never a positive pass, which applied before PROD-11.1 to a partition with nothing before the window's end. A start makes it likelier — every partition whose records all precede the start. Found live in the fix round (the new-point row's first fixture had such a partition, `claude/artifacts/prod-11-1/fix-round/e2e-run1/newpoint.json`) and kept as the row `a_partition_with_nothing_in_the_window_is_preflight_failed_never_pass`. Fail-closed, never a false `pass`; restoring such a partition empty and passing would need phase 5 to tell "empty because of the stated start" from "empty because the archive lost it", which is not built. The restore preview does not yet flag such a partition (its `SelectionEmpty` is for a window no segment of ANY partition overlaps); a per-partition preview row is PROD-11.1a's.
- **Partition subsets** wait for OD-9 (§5.5).
- **Consumer-position mapping over a filtered restore waits for PROD-04.2.**
- **Offset ranges** wait for PROD-00.3 (the tracker's approach).
- **An older runner ignores a hand-written `restore.partitions`** (§6, the residual).

## 9. Proposed child row

- **PROD-11.1a (P1, M2, impl, k8s, Tier A): safe clones and the console's selection.** Clones (new-topic targets) get a declared TTL and cleanup policy (only targets the same execution created; never the only recovery path), an `AllowedClusters` check, explicit header handling; the console's basic restore flow stays short, a window start (the interval form) appears only when chosen, and partition subsets only after OD-9; reuse PLAT-11.2's preview, adding a row that names a partition with no record in the chosen window (§8). Depends on PROD-11.1.

## 10. Rows for the orchestrator's PoC upgrade

No CRD changes. The runner image changes (the window start, phase 7, the block, the subset refusal); the controller changes in that `plan_within_scope` refuses a selection under a standing authorization and rehearsal slots render no start (byte-identical plans). Rows, each with its predicate and control:

| # | Row | Predicate | Control |
|---|---|---|---|
| K1 | A `Restore` (`newTopic`) whose `planBytes` state `restore.point_in_time: "<start>/<end>"` over a PoC topic with three partitions | Job `Succeeded`; signed scorecard 1.7.0 with `source.selection {window_start_ms, window_end_ms}`, `integrity.result: pass`; every target partition holds exactly the archive's records from the start | the same plan with a single-instant point: scorecard 1.6.0, no block |
| K2 | A `Preflight` (restore) over a plan whose start is 1 ms before the recovery point's earliest covered timestamp | `archive.coverage` `notReady`, `WindowStartBeforeCoverage` | start at the floor: `ready` |
| K3 | A `Restore` whose plan states `restore.partitions` | the Job exits 3 before any target topic exists; `status.exitReason` `GuardRefused`; the message opens `PartitionSubsetsAwaitOwnerDecision`; no scorecard | — |
| K4 | An existing `RehearsalSchedule`'s next slot after the upgrade | its plan bytes and `templateDigest` are byte-identical to the pre-upgrade slot's | — |
| K5 | The K1 scorecard downloaded through the product API, checked by `logweir drill verify` and `verify_scorecard.py` 1.23.0, and by the 1.22.0 script | 1.23.0 readers: `VALID` and the `replay selection:` line; 1.22.0: `VALID`, no line | the K1 control's scorecard: no selection line from any reader |
| K6 | A `Restore` under a STANDING authorization whose plan states a start | refused before any Job: outside the signed scope, naming the window start | the same plan with a single-instant point: admitted |

## 11. Fix round (2026-10-08): the review's findings and the orchestrator's decisions

| Finding | Decision / fix | Proof |
|---|---|---|
| H1 (subset scorecards misread by older readers) | Partition subsets refused by name until OD-9 (§0.2, §5.5); start-only ships as MINOR; the 1.23.0 sampled-pass line qualified by the window in both readers; §5.2/§5.4, `stability.md`, `verify-a-scorecard.md`, `drill-scorecard.md` corrected; old-reader rows for a start-only document under both lanes (§5.4) | the subset rows (§4), mutant F1; `a_sampled_pass_over_a_selection_says_so`, the parity loop; the live topics row and the quoted old-reader outputs |
| M1 (standing authorization admits a selection) | `plan_within_scope` refuses a start or a subset (A6) | `a_plan_stating_a_replay_selection_is_outside_every_standing_scope`, mutant F2 |
| M2 (an older runner widens a narrowed plan) | The start is written only as the interval form of `point_in_time`, which an older runner cannot parse (§6) | live `an_older_runner_refuses_a_plan_stating_a_start` on main's runner, with its control; the residual recorded |
| L1 (mutant R5 survived) | `FLOOR_MS - 1` boundary case added | mutant F4 killed |
| L2 (FX-8 refusal text) | names `restore.point_in_time S/E, a window with a stated start` | `a_window_start_over_a_log_append_time_topic_is_refused_and_named` |
| L3 (broken history table) | the 1.23.0 row is inside the table | `verify-a-scorecard.md` |
| L4 (fail-closed claim overstated) | corrected in the report: `2b7e7d23` (before the orchestrator's note) could sign a narrowed restore without the block; it is not on main's first-parent line | the report's fix-round section |
| L5 (`compose_offset_reports` into itself) | refused | `composing_a_report_into_itself_is_refused`, mutant F8 |
| L6 (info: sampled lane at the start) | named in the sampled `coverage_note` | the orchestrator row's coverage note |
| Version step monotonic | `newer_format_version` in every step | `every_version_step_takes_the_newer_minor`, mutant F3 |

---

Documentation is licensed [CC-BY-4.0](../../LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
