# ADR 0008: the MVP tag-1 constraint amendments (spec §3.1 A–D)

## Status

Accepted, 2026-09-09, as **amendments**. This is the O2 ADR the MVP tag-1 plan
(`docs/mvp/2026-09-09-mvp-tag1-plan.md`) requires: the record of every place
where tag 1 CHANGES a decision an earlier document made, written down as a
change rather than smuggled in as a reading of it.

## Context

Four constraints of spec §3.1 are revised by tag 1. Three of them are recorded
here because the earlier text says the opposite of what tag 1 does, and one is
recorded here because a reader would otherwise assume it had been revised and
it has not. An amendment that is only implied is an amendment nobody can review;
each section below therefore carries **the replacement text itself**, not a
description of it.

## Amendment A — the kind list

`docs/ROADMAP.md:294`'s kind list is replaced. The kinds are `KafkaCluster`,
`BackupSchedule`, `Backup`, `Restore`, `Approval` and `TrustRoster`, plus
`Switchover` (tag 2) and, reserved, `MetadataSnapshot`. `RestoreDrill` is
**retired** in favour of `Restore`. No kind may name a cluster, a fleet, a topic,
a connector or a backup. A new kind requires an ADR.

This is Global Constraint 34. Task 1 lands the `docs/ROADMAP.md` marker; the
kinds are implemented in code by Task 15b.

## Amendment B — GC14 is NOT amended

`gc.txt:18` (GC14) needs **no** amendment, and this section exists so that the
absence is on the record rather than inferred. Its naming exclusions stand
verbatim — never `kafkabackup.com`, never `oso.sh`, never the API groups
`kafka.oso.sh` or `kafkabackup.com`, never the `osodevops/` Docker Hub namespace
for Logweir's own artefacts, never OSO's crates.io names — its API group
`logweir.dev/v1alpha1` stands verbatim, and its ASF footer sentence stands
verbatim. The **only** change is that GC14's "deferred to SP5" clause is
**lifted**: `logweir.dev/v1alpha1` is Logweir's now, not later.

GC14 contains no kind list. The kind list is Amendment A, above, and Global
Constraint 34.

## Amendment C — Phase 5's adopter gate is overridden

`docs/ROADMAP.md:292`'s gate on Phase 5 — "Only if adopters ask by name.
Nothing here is scheduled; each item is unlocked by a request" — is
**overridden by owner decision**. The operator, the CRDs and the static UI are
scheduled work in tag 1 and are not waiting on an adopter request.

This is Global Constraint 26. It is recorded here, in writing, rather than
implied by the existence of the tasks that build them: a gate that is silently
walked past is a gate that was never really there.

## Amendment D — four runtime engine subcommands, not three

Global Constraint 3 (`gc.txt:7`) and `docs/adr/0002-shell-out.md`'s Decision
(`:21-25`) both now read:

> the `logweir` binary invokes **exactly four** engine subcommands at runtime:
> `backup`, `restore`, `validate-restore`, `validation run`.

`list`, `restore-status`, `offset`, `evidence-verify` and
`validation evidence-verify` stay denied.

**Why this is a change and not a reading.** ADR 0002's original Decision fixed
the count at "exactly three subcommands reachable from shipped code" and
contains **no `--from-cluster` clause anywhere in its 79 lines**. The sentence
"`backup` belongs to `--from-cluster`, which is deferred" is GC3's own, and it
**defers** the path rather than pre-authorising it. Meanwhile GC18 funds the
`--from-cluster` execution path, which renders a `backup.yaml` and runs it — so
GC3 and GC18 contradicted each other until this amendment resolved them. Spec §5
("Logweir drives the engine's `backup` command itself") and spec §16 item 10 are
the authority, and ADR 0007 (`docs/adr/0007-from-cluster-in-v0.1.md`) had already
revoked the `--from-cluster` deferral; this amendment is the count-side
consequence of that revocation.

**How it is enforced.** `scripts/check-no-oso.sh` carries two allowlists that are
deliberately not the same list:

- `ENGINE_RUNTIME_ALLOWLIST` — the contract: the four subcommand tokens above.
  This is what a refusal message quotes.
- `ENGINE_ARGV_ALLOWLIST` — what an argv array may legally contain: the same
  four plus `run` (the second word of the two-word `validation run`) and the
  argv furniture `--config`, `--format`, `json`.

The gate also changed **shape**, not just membership. Its primary check now
matches an **invocation**: a double-quoted literal that is an argv token and
that sits inside the balanced expression of `run_engine(` or of a direct
`Command::new` spawn of the engine binary. The old whole-file token grep is kept
as a secondary, minus `backup`, and its escape is now
`// engine-token-ok: <reason>` with a reason of at least ten characters — a bare
marker is not an escape. Neither check subsumes the other: the secondary owns
the two-word forms and tokens built outside an invocation, and its escape does
**not** excuse the primary.

## Sections E onward

`docs/adr/0008` is extended by Task 13 (§E first half) and Task 14 (§E second
half) — chain Z; nothing beyond Amendment D is written yet.

## Consequences

- Every later task that needs to cite an amendment cites **this file**, by
  section letter, rather than restating the reasoning.
- `docs/adr/0002-shell-out.md` carries its own dated **Amended** paragraph
  pointing here; `.superpowers/sdd/2026-09-05-stage2-tier0-phase1/gc.txt` item 3
  and `docs/ROADMAP.md` lines 292 and 294 carry `[REVISED …]` / `[AMENDED …]`
  markers naming this file. A constraint revised in one place and not the others
  is the failure this ADR exists to prevent.
- The retired kind is named exactly once in this document, in Amendment A, and
  `crates/logweir/tests/engine_allowlist.rs` asserts that count — so a later
  edit that reintroduces it by name has to argue with a test.

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
