# ADR 0005: the week-1 velocity calibration

## Status

Accepted, and **recorded as NOT PERFORMED as specified.** Read the whole ADR
before quoting it — the honest answer here is a negative one.

## Context

`docs/ROADMAP.md` names exactly four events that may move a date. The fourth is
the **week-1 velocity calibration** (spec §3, assumption 13):

> if actual > 1.5× estimate, every remaining estimate is multiplied by the
> measured ratio and the week numbers are re-derived *before* Phase 1 starts.

The calibration is defined against a specific unit the ROADMAP also fixes: a
person-week is 40 focused hours, and capacity is **9 focused hours per calendar
week**, so one person-week is 4.4 calendar weeks. The ratio it computes is
`actual focused hours / estimated focused hours` for week 1's scoped work.

## Decision

**The calibration as the ROADMAP defines it has not been performed, and this
ADR records that rather than substituting a number that would look like one.**

The reason is that the quantity it measures does not exist for this build. The
29 tasks of SP1 were executed by AI subagents over two calendar days
(2026-09-03 to 2026-09-05), not by a part-time builder over calendar weeks at 9
focused hours each. There is therefore no "actual focused hours" figure that is
comparable to the estimates, and a ratio computed from wall-clock agent time
would not predict anything about the human capacity every remaining ROADMAP
estimate is denominated in. Publishing such a ratio, and then re-deriving week
numbers from it, would be the exact failure the ROADMAP diagnoses in its own
predecessor: arithmetic that is checkable but describes the wrong thing.

## What IS measurable, and is recorded instead

These are facts about the build, not a substitute calibration. They are useful
to a future estimator; they are not the ratio, and must not be quoted as one.

- **29 tasks planned; the main line completed through Task 22.** One task's
  code — `--from-cluster` / phase −1 — is scheduled into a follow-up Task 24
  rather than the main line (see `docs/adr/0007-from-cluster-in-v0.1.md`), and
  the resulting gap is recorded in `docs/stability.md`.
- **Rework was a large fraction of the total.** Several tasks required explicit
  fix rounds after review (Tasks 12, 14, 19, 20 and 21 each carry a fix report
  in the SDD tracking directory), and one defect class — a documented guarantee
  the code did not deliver — recurred across at least five separate tasks and
  three fix rounds. An estimator using this build as a prior should assume
  review-and-fix is a first-class cost, not a rounding error.
- **The test suite grew to 392 tests without a live stack and 428 with one.**
  A count is not coverage; it is recorded because it is the number a later
  estimate will be tempted to compare against.

## Consequences

- **Date-moving event (4) has not fired.** No ROADMAP week number has been
  re-derived by this ADR. Phase 1's dates stand as `docs/ROADMAP.md` states
  them.
- **The calibration remains owed** the first time a human builder works a
  scoped week at the ROADMAP's stated capacity. When that happens, this ADR
  should be superseded by one that carries the actual ratio.
- **Do not read this ADR as "the schedule was validated."** It says the
  opposite: the mechanism the ROADMAP relies on to catch an optimistic schedule
  has not run, so the schedule is as unvalidated now as it was before the
  build.

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
