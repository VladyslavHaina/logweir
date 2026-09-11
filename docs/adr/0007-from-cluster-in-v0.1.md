# ADR 0007: `--from-cluster` is in v0.1's scope — the deferral was revoked

## Status

Accepted, as a **reversal**. An earlier decision deferred `--from-cluster` to
v0.1.1; that deferral rested on a waiver which was refuted 3-0 in adversarial
review on 2026-09-03 and revoked. The superseded text is kept at the bottom of
this document, under its own heading, so a reader cannot mistake it for the
decision.

## Decision

**`logweir drill run --from-cluster` — phase −1, and `backup.yaml` as a third
rendered document — is in v0.1's scope.** Spec §14 SP1c reads **eleven phase
slots, −1 through 9**; spec §15's cut 0e ("defer `--from-cluster`") had its
2026-09-03 waiver revoked and sits at the bottom of the fire order. Global
Constraint 18 records the scope decision and states that no task may re-open it
in either direction.

## Why the deferral could not stand

**Cost, quoted.** Without `--from-cluster`, an adopter must *already possess* an
OSO `kafka-backup` archive. Risk register entry R2 records zero named adopters
for OSO OSS today, so the addressable population for gate G2 becomes "already
running OSO kafka-backup **and** willing to provision a scratch cluster" — a set
with no known members. The spec itself twice calls that not a viable funnel for
the gate that decides whether the project lives. It was the single
highest-risk deferral in the plan.

**Mitigation that remains available.** Spec §14 mechanism (ii): issue the
design-partner change-control ask **five weeks before** the adopter run it
unblocks.

**Consequence in code, as reversed.** Global Constraint 18:
`source.captured_by_logweir` is `true` exactly when phase −1 ran, and
`Scorecard::validate_invariants()` enforces the pairing **in both directions** —
`true` requires `last_phase_completed >= -1` and a non-null
`measured.rpo_source_relative_seconds` with a null
`rpo_source_relative_unmeasured_reason`; `false` requires the reverse. The field
therefore cannot be forged into a signed document, and it is a measured fact
rather than a dead constant. `last_phase_completed` keeps the `-1..=9` domain.

**What lands with it.** The mandatory named-topic allowlist with no wildcard;
the read-only assertion (no `reset_consumer_offsets`, no `auto_consumer_groups`,
no topic creation, no offset commit on the source); `purge_topics` / `dry_run`
refused in the rendered `backup.yaml` as they already are in `restore.yaml`; the
source `cluster_id` read, recorded and re-asserted `!= target` in phase 0;
`render_backup.rs` with its own `insta` golden; and
`source.captured_by_logweir = true` driving a real
`measured.rpo_source_relative_seconds` with a null reason.

**Funding.** ~10-14 h, from spec §15 cut 0b plus feature cuts 4 and 5.

**When cut 0e may fire after all.** Only once ROADMAP gate G2 is already met —
which is the precondition the revoked waiver had set aside.

## Scheduling: the code lands in Task 24, not in the main task line

The decision above is not re-opened: `--from-cluster` and phase −1 are in v0.1's
scope and funded as Global Constraint 18 records. What is deferred is only *when
in the task order* the code lands. No task in this plan has a body for phase −1,
`render_backup.rs`, the source-side admission guard or the `--from-cluster` CLI
flag — authoring one mid-flight would make it the least reviewed code in the
build. It is Task 24, authored after the main line is green. Recorded as a known
v0.1 limitation in `docs/stability.md` and in `IMPLEMENTATION-LOG.md`.

## What that means for the v0.1.0 tag, stated plainly

The format is ready and the execution path is not, and those are different
things:

- **Bound now.** The scorecard schema carries `source.captured_by_logweir`,
  `measured.rpo_source_relative_seconds` and
  `rpo_source_relative_unmeasured_reason`, and `validate_invariants` enforces
  their pairing in both directions. `format_version` is `1.0.0` and does not
  need to move when the execution path lands.
- **Not shipped in v0.1.0.** There is no `--from-cluster` flag on the CLI. Every
  scorecard v0.1.0 emits carries `source.captured_by_logweir: false`, a null
  `measured.rpo_source_relative_seconds` and a non-null
  `rpo_source_relative_unmeasured_reason`. `last_phase_completed` never takes
  the value `-1`.
- **Checkable.** `logweir drill run --from-cluster ...` is a usage error and
  exits 1, naming the unknown flag — it does not silently ignore it. That is the
  reading a user gets, and it matches this ADR.

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.

---

## Superseded — the 2026-09-03 deferral, kept for the record

**These two bullets are NOT the decision.** They are the text of the deferral
that was revoked, kept so the reversal is auditable rather than invisible.

- **Decision (superseded).** v0.1 ships eleven phase slots (−1 through 9).
  *The superseded text read "ten phase slots, 0-9", and deferred
  `--from-cluster` / phase −1 / a third rendered `backup.yaml` to v0.1.1.*
- **Precondition knowingly violated (superseded).** Spec §15 cut 0e states it
  "may only be taken when ROADMAP gate G2 is already met". G2 had not been
  evaluated. §14 places `--from-cluster` inside SP1c as the funnel fix for that
  very gate. The cut was therefore taken **before** its own precondition,
  deliberately and on the record — and that is exactly what the three refuters
  objected to.

Earlier drafts of this plan described "ten phase slots (0-9)". Any such comment
is wrong and predates this reversal; the domain is `-1..=9`. As of the v0.1.0
tag none remain — `grep -rn 'ten phase slots\|phases 0-9\|0\.\.=9' crates/ docs/`
is empty, `logweir_core::scorecard` enforces `-1..=9`, and
`crates/logweir/src/drill/mod.rs` opens by naming the orchestrator
"eleven-phase (-1..=9)" while stating that the ten modules beside it are phases
0 through 9.

Documentation is licensed [CC-BY-4.0](../LICENSE-docs).
