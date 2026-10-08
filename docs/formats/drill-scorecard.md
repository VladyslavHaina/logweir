# The drill scorecard format, field by field

`application/vnd.logweir.drill-scorecard+json;version=1.0.0`

The machine-readable schema is
[`schemas/logweir-drill-scorecard-1.4.0.json`](../../schemas/logweir-drill-scorecard-1.4.0.json)
and CI diffs it against the code on every build, so this document and the
schema cannot drift apart silently. [`schemas/logweir-drill-scorecard-1.3.0.json`](../../schemas/logweir-drill-scorecard-1.3.0.json),
[`schemas/logweir-drill-scorecard-1.2.0.json`](../../schemas/logweir-drill-scorecard-1.2.0.json),
[`schemas/logweir-drill-scorecard-1.1.0.json`](../../schemas/logweir-drill-scorecard-1.1.0.json)
and [`schemas/logweir-drill-scorecard-1.0.0.json`](../../schemas/logweir-drill-scorecard-1.0.0.json)
are frozen beside it. Format **1.4.0** (PROD-08.1) added the nested optional
[`integrity.verification`](#integrityverification-format-140): whether the
verdict covered a sample or every selected record, and what a complete
verification found. Format **1.3.0** (FX-8) added the nested optional
[`source.time_basis`](#sourcetime_basis-format-130); format **1.2.0** (FX-3) added
`topic_parity.not_reconstructed`. Format **1.1.0** (FX-4) added the nested optional
[`topic_parity.not_assessed` and `target_diff.not_assessed`](#topic_parity-and-what-its-silence-means),
the first fields added after the v0.1 tags and therefore a MINOR bump with a new
schema file
([`docs/stability.md`](../stability.md#the-v010-tag-is-the-compatibility-boundary)).
The fail-safe entries it also writes into the existing
`topic_parity.unexpected_divergence` widen what that array's entries can say;
the owner ruled them MINOR on 2026-10-05
([OD-7](../to-do/product-expansion.md#owner-decisions)'s follow-up ruling),
because they can only weaken an older reader's verdict. Each
`target_diff.collisions` string is written exactly as before.
Format **1.2.0** (FX-3) adds the nested optional
[`topic_parity.not_reconstructed`](#topic_parity-in-a-newtopic-restore-not-reconstructed-120):
the source settings a `newTopic` restore did not reconstruct, which every
earlier writer signed as `intentionally_deviated` in both modes. Those
deviations are also written into the existing `unexpected_divergence`, and
[`docs/stability.md`](../stability.md#format-120-fx-3-what-a-newtopic-restore-did-not-reconstruct)
says how that is classified.
The payload type keeps `version=1.0.0`, the major-1 envelope. A worked example is
[`e2e/fixtures/scorecard-pass.json`](../../e2e/fixtures/scorecard-pass.json) —
read [`e2e/fixtures/README.md`](../../e2e/fixtures/README.md) first, which lists
the one field in it that v0.1's code cannot emit and why it is still there.

If you are receiving a scorecard rather than producing one, read
[../verify-a-scorecard.md](../verify-a-scorecard.md) instead; it is written for
that reader and this one is a reference.

## Reading rules a consumer must honour

1. **`format_version` is semver.** Ignore unknown fields when the **major**
   matches what you support. **Refuse** a document whose major is higher than
   you support rather than guessing at a shape you have never seen.
2. **Verify against the bytes as stored.** Never `jq .` a scorecard, re-save it
   and then verify — the signature covers the exact bytes, including the
   trailing newline.
3. **A null is not a zero and not a false.** Every optional field below says
   what its null means. `null` almost always means "not measured", which is
   different from "measured as none".
4. **The `logweir drill show` table is a summary.** It renders fourteen frozen
   rows plus a qualifiers footer; the signed document carries more. Where they
   differ, the document is authoritative.

---

## Identity and provenance

| Field | Type | Meaning |
|---|---|---|
| `format_version` | string | Semver of this format. `1.0.0` in v0.1; `1.1.0` since FX-4; `1.2.0` since FX-3; `1.3.0` since FX-8; `1.4.0` since PROD-08.1. |
| `run_id` | string | ULID. Also the object key stem in the evidence bucket. |
| `outcome` | enum | Exactly four values: `pass`, `fail-objective`, `fail-integrity`, `preflight-failed`. There is **no `refused` and no `error` outcome** — a refused plan and an operational failure produce **no scorecard at all** (exit 3 and exit 1); an outcome value for them would imply a signed document that does not exist. `drift` is not a v0.1 value either: v0.1 collects no metadata, so nothing could produce it. |
| `last_phase_completed` | integer | Domain `-1..=9` (eleven phase slots). `-1` is the `--from-cluster` source-capture phase, which is in v0.1's scope but whose code lands in a follow-up — see [ADR 0007](../architecture.md#adr-0007-source-capture-scope) — so **v0.1.0 never emits `-1`**. **A v0.1.0 SIGNED document reads 5, 6 or 7 and never 8 or 9** — see the note below. |
| `requested_at` | RFC 3339 | When the run was requested. |
| `approval_validated_at` | RFC 3339 | When the approval's signature was checked. |
| `triggered_by` | string \| null | Free text from `--triggered-by`. Deliberately **not** a metric label: unbounded cardinality. |

### Why a completed drill reads `last_phase_completed: 7`

The scorecard is the **exact bytes phase 8 signed**, and phase 8 signs a frozen
copy. Its own phase record is pushed onto the in-memory document afterwards,
and phase 9 (teardown) runs after the upload. So the highest phase a SIGNED
scorecard can record is 7, and the emittable values in v0.1.0 are:

| Value | What happened |
|---|---|
| `5` | The preflight blocked the plan, or the restore ran and left every sampled partition empty (`record` does not advance the counter on a phase that errored). |
| `6` | The restore completed and verification did not. |
| `7` | Every phase through verification completed — **the normal successful drill**. |

**`7` does not mean teardown was skipped.** Teardown is attested in its own
separately signed document, `<run_id>.teardown.json`. `drill run`'s stdout line
quotes this same value, from the artifact rather than from the in-memory copy,
so the console and the document cannot disagree about it.

`8` and `9` are in the DOMAIN — `validate_invariants` accepts `-1..=9` — and no
v0.1.0 writer produces them.

### `outcome` is ENTAILED by the rest of the document, and that is ENFORCED

`outcome` is the field an auditor reads first, and until Task 5 it was read by
**neither** verifier. `self.outcome` appeared in no arm of
`Scorecard::validate_invariants` and `outcome` in no arm of
`docs/verify_scorecard.py::check_invariants`, so a document claiming
`outcome: "pass"` beside its own contradicting evidence was accepted, signed
and verified at exit **0** by both readers. Two such documents were falsifiable
on the shipped code: `pass` beside an `integrity.partial_reason` naming an
unreconciled topic, and `pass` beside `records_sampled_matching: 50` against
`records_sampled: 100`.

Six rules now make `outcome` a claim the rest of the document has to support.
They are checked **in this order** by both readers, and a document violating
any of them is refused by `logweir drill verify` at exit **4** and by
`docs/verify_scorecard.py` at exit **1**, with the **same message**:

| # | Rule | Message |
|---|---|---|
| 1 | `outcome: "pass"` implies `integrity.result: "pass"` | `outcome is 'pass' but integrity.result is not 'pass'` |
| 2 | `outcome: "pass"` implies no non-blank `integrity.partial_reason` | `outcome is 'pass' but integrity.partial_reason is present` |
| 3 | `outcome: "pass"` implies `objectives.met` is not `false` | `outcome is 'pass' but objectives.met is false` |
| 4 | `outcome: "pass"` implies `records_sampled_matching == records_sampled` | `outcome is 'pass' but only <m> of <n> sampled records matched` |
| 5 | `integrity.records_sampled <= sample.records_expected`, on **every** outcome | `records_sampled (<n>) exceeds sample.records_expected (<e>)` |
| 6 | `engine.matrix_verdict: "pass"` implies the drill passed at `byte-fingerprint` level | `engine.matrix_verdict is 'pass' but the drill did not pass at byte-fingerprint level` |

Two values stay legal and are easy to misread as violations:

- **`objectives.met: null` remains legal on a pass.** Rule 3 fires only on an
  explicit `false`. `null` means either no objective was requested or a
  requested `pass_rate` could not be measured, and neither contradicts a pass —
  see [`objectives`](#objectives).
- **`matrix_verdict: "pass-degraded"` is the correct value for a pass at a
  non-`byte-fingerprint` level.** Rule 6 does not say a degraded drill cannot
  pass; it says such a drill's matrix row is `pass-degraded`, which is what
  that value is for.

Rule 5 is the one rule that binds on **every** outcome, not only a pass:
`sample.records_expected` is the canary size the drill **set out** to
reconcile, so reconciling more records than were selected is not a stronger
result, it is an incoherent one.

Rule 6 is stated as a property of the DOCUMENT rather than of one call site.
`crates/logweir/src/drill/phase8_score.rs`'s `matrix_verdict_for` returns
`pass` on exactly one path — a `pass` outcome at `byte-fingerprint` level,
reachable only when the phase-5 lever readback was itself `pass` — so the rule
holds for every value that writer can emit, and now also for every document an
auditor can be handed.

Like the `partial_reason`, `evidence` and `redactions` arms, this is a
**retroactive tightening of the 1.0.0 reader, not a format change**: no field
is added, `format_version` stays `1.0.0`, and the accepted set only narrows.
`crates/logweir/tests/two_reader_parity.rs` runs both readers over
`e2e/fixtures/invariants/` — nine documents for these six rules — and compares
their refusal text, so the agreement is checked rather than asserted.

## `engine`

| Field | Type | Meaning |
|---|---|---|
| `engine.id` | string | `oso-cli` in v0.1. |
| `engine.version` | string | Read off the engine that actually ran. **Never empty** — a signed document that names no engine is refused before signing. |
| `engine.digest` | string | The `sha256:` image digest the binary was extracted from. Never a tag. **Never empty**, for the same reason. |
| `engine.execution` | string | `subprocess` in v0.1. A string, not a closed enum, because SP5's Kubernetes-Job execution would add a value and the format is frozen at 1.0.0. |
| `engine.levers.header_preflight` | enum | Whether the engine honoured the preflight lever. |
| `engine.levers.dry_run_check_segments` | enum | Whether the segment check was honoured. `unknown-not-observable` is a real and common value: the lever's effect is not observable from outside on every version. |
| `engine.levers.unknown_key_warnings` | string[] | Keys Logweir rendered that the engine reported ignoring. A **non-empty array is a degraded restore**, not a cosmetic warning: the engine ran with less configuration than the approved plan specified. |
| `engine.matrix_verdict` | enum | The support-matrix row **this run** established, decided at signing time from what the drill actually did — never a `pass` inside a document whose own `outcome` is not `pass`. `pass` = the full drill ran and passed at `byte-fingerprint` level; `pass-degraded` = it passed at a reduced integrity level; `fail` = it ran and did not pass, with the reason below; `fail-lever-not-honoured` = phase 5 observed the engine accept a lever and not act on it, and that finding is never overwritten by a drill-level verdict; `unsupported-lever-absent` = the engine predates a lever Logweir needs. See [support-matrix.md](../support-matrix.md). |
| `engine.matrix_verdict_reason` | string \| null | Why, when the verdict is `fail`. **Required** for `fail` and null otherwise — enforced by `validate_invariants` and by both verifiers. |

## `source` and `target`

| Field | Type | Meaning |
|---|---|---|
| `source.backup_id` | string | The backup set restored from. |
| `source.manifest_sha256` | string | `sha256:` of the archive manifest as stored. Re-derivable by hand — [verify-a-scorecard.md](../verify-a-scorecard.md) shows how. |
| `source.manifest_version_id` | string \| null | The store's version id for the manifest object, when the backend returned one. |
| `source.captured_by_logweir` | bool | `true` **exactly when** phase −1 ran. `validate_invariants` enforces the pairing in **both** directions with `last_phase_completed` and the two `rpo_source_relative_*` fields, so it cannot be forged into a signed document. **Always `false` in v0.1.0.** |
| `source.time_basis` | object, **optional** (1.3.0) | Which source topics the restore's time selection read by **producer time**, and which it selected by time with no recorded timestamp type. See [below](#sourcetime_basis-format-130). ABSENT means not recorded. |
| `target.cluster_id` | string | The target cluster's own id, read from it. |
| `target.mode` | string, optional | Which of the two target modes the run was in: `scratch` or `newTopic`. **Absent means `scratch`**, which is what every document written before this field existed carries, so the three checked-in signed fixtures keep their bytes. |
| `target.marker_topic` | string, optional | The **scratch** segregation proof: the cluster is in `allowedClusterIds` **and** this topic exists, both verified at phase 0, whose failure refuses the drill with exit 3 before anything runs. **Absent in `newTopic` mode**, because that mode skips both checks — a reader that saw the field there would be reading a verification that never ran. Both readers REFUSE a document that is `scratch` and omits it. |
| `target.topic_mapping_prefix` | string | Prefix applied to restored topic names. |
| `target.topic_mapping_sha256` | string | `sha256:` of the mapping, so the mapping is attested rather than described. |
| `target.topic_mapping_entries` | integer | How many mapping entries there were. |

### `source.time_basis` (format 1.3.0)

```json
"time_basis": {
  "plan": "producerTime",
  "producer_time": ["orders"],
  "not_recorded": []
}
```

The pinned engine archives each record's **producer** timestamp, so every
selection by time — a stated `restore.point_in_time`, or a `sample.window_end`
earlier than the newest timestamp the archive manifest records for a topic —
reads producer time. For a topic on `message.timestamp.type=LogAppendTime` that
is not the topic's own clock, and phase 7 cannot notice: it compares the
restored topic with the archive, and both carry producer time. Since FX-8 the
runner refuses such a selection (`PointInTimeByProducerTime`, exit 3, no
scorecard) unless the approved plan states `restore.time_basis: producerTime`
([the plan field](drill-spec.md#restoretime_basis-fx-8)), and this block is what
the signed document says about the selections it did make.

| Field | Type | Meaning |
|---|---|---|
| `plan` | string, optional | The approved plan's `restore.time_basis`, copied: `producerTime`, the one value. ABSENT when the plan stated none. |
| `producer_time` | string[] | Source topics whose recorded timestamp type is `LogAppendTime` and which this restore selected by time — by the producers' clocks, as the plan accepted. Sorted. |
| `not_recorded` | string[] | Source topics this restore selected by time while their timestamp type was NOT RECORDED: no `message.timestamp.type` override in the archive manifest and no effective value in a verified backup receipt (FX-4's `config_coverage`). The selection may have read producer time. Sorted. |

A topic recorded as `CreateTime`, and a topic the restore did not select by
time, is in neither list. **Absent means not recorded** — every document before
1.3.0 — and is never read as "every selection used the topics' own clocks".
Every 1.3.0 run that reaches the decision writes the block, so a 1.3.0 block
with both lists empty is the claim that no topic was selected by producer time
or with an unrecorded type **as far as the archive manifest's segment bounds
show**. A plan with no point in time whose `sample.window_end` is at or after
every segment's first and last timestamp is not counted as a selection; with
out-of-order timestamps inside a segment the restore can still leave out a
record later than both of its segment's ends, which is PROD-01.1b's
([the limitation](../stability.md#recovery-point-selection-uses-segment-first-and-last-timestamps)).

Four arms, enforced by both readers in the same position (after `target.auth`,
before `redactions`) and words, fire only on a document carrying the block:

| Arm | Refuses |
|---|---|
| TB-1 | the block under a `format_version` before 1.3.0 |
| TB-2 | `plan` other than `producerTime` |
| TB-3 | a topic in `producer_time` when `plan` is not `producerTime`: a selection by producer time is one the approved plan accepted, never a default |
| TB-4 | a topic in both lists: its type was either recorded as `LogAppendTime` or not recorded |

Both readers print one `time basis:` line per non-empty list, or the one line
saying it was not recorded for a document without the block; `logweir drill
show` renders the same in its qualifiers footer (`source.time_basis`).

**The number.** 1.3.0; 1.2.0 is FX-3's. A renumber moves
`logweir_core::FORMAT_VERSION` and `scorecard::TIME_BASIS_SINCE_MINOR`
together, the justfile's `scorecard_schema_version` and this schema file's
name, `docs/verify_scorecard.py`'s `FORMAT_VERSION` and
`SCORECARD_TIME_BASIS_SINCE_MINOR`, the parity script's
`SCORECARD_TIME_BASIS_VERSION`, and the literal pins in
`crates/logweir-core/src/lib.rs`, `docs/test_verify_scorecard.py` and the
corpus cases `time_basis_*.json` (their `format_version` and TB-1's reason).

## `approval`

| Field | Type | Meaning |
|---|---|---|
| `approval.approver` | string | Who approved. |
| `approval.ticket` | string | Their change ticket. |
| `approval.plan_hash` | string | `sha256:` of the **exact spec bytes** the drill ran. Approving one document and running another is refused at phase 1. |
| `approval.approved_at` | RFC 3339 | When. |
| `approval.key_id` | string | Lowercase hex sha256 of the approver key's SPKI DER. |
| `approval.self_attested` | bool | `true` when the approving key **equals** the signing key: the same party planned, ran and vouches for the result. **The WRITER never refuses to sign such a run; it labels it.** Treat `true` as a reason to seek corroboration. **The READERS treat this field as a CLAIM, not a finding**: both derive the answer from `approval.key_id` against the key that verified the signature, and refuse a document whose claim disagrees with that derivation (`drill verify` exit 4, `verify_scorecard.py` exit 1). The two are not in tension — a self-attested run is signed and labelled; a document that *lies about* being self-attested, in either direction, is refused. That narrows the accepted set without changing the format: no field is added, removed or retyped, `format_version` stays `1.0.0`, and no document Logweir has ever written is refused, because the writer has always derived the field correctly. |

## `phases`

An array of `{phase, name, at, duration_ms, outcome, notes[]}` — the first five
are required, `notes` is omitted when empty. The record-before-return rule means
a phase that failed still has its entry, with the reason in `notes`: phase 5
carries each preflight finding's tag and detail there, so a `preflight-failed`
scorecard states *why* it was refused and not merely that it was.

## `measured` — the four RTO definitions, verbatim

There are **four** RTO figures because there are four honest answers to "how
long did recovery take", and picking one silently would be picking the
flattering one. Each is `Option<u64>` seconds, clamped at 0, and `null` means
not measured.

- **`rto_seconds`** — `approval_validated_at → verified_at`. The drill from the
  moment the approval was checked to the moment the restored data was verified.
- **`rto_requested_to_verified_seconds`** — `requested_at → verified_at`. The
  whole wall clock, including the time spent validating the approval.
- **`rto_restore_only_seconds`** — `restore_started_at → restore_finished_at`.
  The engine's restore alone, with no Logweir overhead.
- **`rto_excluding_preflight_seconds`** — `rto_seconds` minus phase 5's
  duration.

**`rto_excluding_preflight_seconds` is the one compared against
`objectives.rto_seconds`, and this is why:** phase 5 sets
`header_preflight: full`, which makes `scan_required` unconditionally true and
opens and decodes **every** segment in the window to inspect per-record headers,
with one HEAD request per segment on top. **No incident responder performs that
sweep.** Scoring it against a recovery-time objective compares unlike things —
it would make Logweir's own verification thoroughness look like your recovery
being slow. The other three are published beside it so you can see exactly what
was excluded rather than take the exclusion on trust.

### `rpo_seconds`

> **`rpo_seconds` is archive coverage gap at the requested recovery point, not
> source-relative data loss.**

Formula: `(requested_point_in_time - newest_restored_record) / 1000`, **in that
order**, clamped at 0.

It answers "how far short of the point I asked to recover to does the newest
record the archive could give me fall". It does **not** answer "how many records
the live source held that the archive did not" — Logweir never contacts the
source in v0.1.

The clamp is load-bearing. A record at or beyond the requested point means no
gap there, which is `0`. A **negative** gap is not a smaller gap, it is a
meaningless one, and a reader meeting `rpo_seconds: -90` in a signed document
would most likely read it as "no data loss". `validate_invariants` refuses a
negative value, so the rule holds for every writer and not only for today's
single call site.

| Field | Type | Meaning |
|---|---|---|
| `rpo_source_relative_seconds` | integer \| null | Source-relative loss. **Null in v0.1.0**; becomes a number only under `--from-cluster`. Never negative. |
| `rpo_source_relative_unmeasured_reason` | string \| null | Why it is null. Exactly one of this and the field above is non-null — enforced. In v0.1.0 it reads `source cluster never contacted`. |

## `objectives`

Copied verbatim from your spec, plus the verdict.

| Field | Type | Meaning |
|---|---|---|
| `rto_seconds` | integer \| null | Requested maximum RTO. |
| `rpo_seconds` | integer \| null | Requested maximum coverage gap. **Never negative** — a negative allowed gap is unsatisfiable, not strict, since the measured value is non-negative. |
| `pass_rate` | float \| null | Requested minimum reconciliation rate. |
| `met` | bool \| **null** | **Tri-state.** `true` every requested objective was met; `false` at least one was missed; **`null`** either a `pass_rate` objective was requested and could not be measured (unmeasurable, which is not met) **or no objective was requested at all** — `objectives: {}` reads `null`, never a vacuous `true`. Do not collapse `null` into either. |

## `sample`

| Field | Type | Meaning |
|---|---|---|
| `window_start`, `window_end` | RFC 3339 | The point-in-time window drilled. |
| `topics`, `partitions` | integer | How many of each were selected. Under complete coverage (1.4.0), every topic and partition the complete block lists. |
| `records_expected` | integer | **The canary size**: how many records this drill set out to reconcile — `records_per_partition` summed over the partitions actually selected. It is **not** how many records the manifest says the window holds; those differ by orders of magnitude on a real archive. Under complete coverage (1.4.0) the canary is the whole expected output, so this is `integrity.verification.complete.replay.expected`. |
| `records_restored` | integer | How many were restored. |
| `anchor` | string | The vocabulary is `head`, `tail`, `random`; **v0.1 implements only `head`** and REFUSES the other two at phase 0 with exit 3 rather than silently substituting. The scorecard field is a plain string (the closed enum lives on the input spec, `logweir_core::spec::Anchor`, which is where a bad value has to be caught); a v0.1.0 scorecard therefore always reads `head`. See [stability.md](../stability.md). |
| `coverage_note` | string | What the drill itself says about how representative the window is. Read it. |

### The block is REQUIRED, and both readers now enforce that

`sample` is in the schema's top-level `required` list and none of its fields is
nullable — reading rule 3 above says every optional field states what its null
means, and none of these does. `records_expected` in particular is the canary
size the whole `integrity` result is measured against, so a document without it
cannot be checked at all.

For one release only one reader acted on that. `logweir drill verify` refuses a
missing `sample` block at parse time (the field is `SampleInfo`, not an
`Option`), exiting **1** with ``signature verified but the payload is not a
scorecard: missing field `sample` ``; `docs/verify_scorecard.py` skipped the
block silently and printed **`VALID`**. Since `verify_scorecard.py` `1.5.0` it refuses
the same document, exiting **1** with `INVALID: the document has no sample
block; it is not a drill scorecard`, and refuses a `records_expected` that is
not an integer with `INVALID: sample.records_expected is not an integer`. The
pair is walked by both readers in
`crates/logweir/tests/two_reader_parity.rs::two_reader_parity_on_documents_refused_before_the_invariants`.

### EVERY required block, and the `u64` domain (`1.6.0`)

`sample` was one of eleven, and until `verify_scorecard.py` `1.6.0` the script
checked seven of them. Measured at `6619090` on documents derived from
`e2e/fixtures/invariants/unmodified_example.json`, a scorecard with `target`,
`target_diff` or `topic_parity` deleted was refused by `logweir drill verify`
(exit **1**, ``missing field `target` ``) and printed **`VALID`** here — the
same divergence `sample` was in, three more times. `1.6.0` takes the block list
from the struct instead of growing it one arm at a time, and
`crates/logweir/tests/two_reader_parity.rs::every_required_block_has_a_shape_corpus_case`
refuses to let the two drift apart.

The list is also **in the struct's declaration order**, because that is the
order `serde` reports a missing field in. On a document missing several blocks
the two readers named different ones — `measured` from `drill verify` and
`integrity` from the script; they now both name `measured`.

`1.6.0` also bounds the **domain** of every integer field the Rust reader types
as `u64`: the four `measured.rto_*_seconds`, `objectives.rto_seconds`,
`sample.records_expected`, `sample.records_restored`,
`integrity.records_sampled`, `integrity.records_sampled_matching`,
`integrity.mismatches` and `phases[].duration_ms`. Python's `int` is unbounded,
so `isinstance(v, int)` mirrored serde's *type* and not its domain, and
`"records_expected": 18446744073709551616` — one past `u64::MAX` — printed
`VALID` here while `drill verify` exited **1**. Both readers now refuse it, and
both refuse a negative value; the Rust texts are serde's own and are recorded
per-reader in `e2e/fixtures/invariants/shape-index.json`.

### Every required field that is NOT a block, and `null` on a plain `u64` (`1.7.0`)

Blocks were half the shape. `Scorecard` has **six** required fields whose type is
not a block — `format_version`, `run_id`, `outcome`, `last_phase_completed`,
`requested_at` and `phases` — and none carries `#[serde(default)]`, so
`serde_json` refuses a document missing one at parse time exactly as it does for
a block. Measured at `7e85937` on documents derived from
`e2e/fixtures/invariants/unmodified_example.json`: with `run_id`, `requested_at`
or `phases` absent, `logweir drill verify` exited **1** (``missing field
`run_id` ``) and `docs/verify_scorecard.py` printed **`VALID`**; with `outcome`
absent the script refused, but on an *invariant* about `engine.matrix_verdict`
rather than on the missing field. Since `1.7.0` it refuses each with `INVALID:
the document has no <name> field; it is not a drill scorecard`, and
`crates/logweir/tests/two_reader_parity.rs::every_required_non_block_field_has_a_shape_corpus_case`
keeps that list derived from the struct.

`1.7.0` also stops treating every `u64` as nullable. Five of the eleven are
`Option<u64>` in Rust — the four `measured.rto_*_seconds` and
`objectives.rto_seconds` — and those accept `null` from both readers. The other
six are plain `u64`, where `serde_json` says `invalid type: null, expected u64`;
the script skipped them, so `sample.records_restored: null`,
`integrity.records_sampled: null`, `integrity.records_sampled_matching: null`,
`integrity.mismatches: null` and `phases[].duration_ms: null` each printed
`VALID` here against exit **1** there. The optionality is now part of the list
the walker re-derives from the struct, so it cannot drift.

### The TYPE of every required non-block field (`1.8.0`)

`1.7.0` checked those six fields for PRESENCE and said so plainly: "the six
fields carry five different Rust types and share no JSON shape, so a type check
here would be five guesses rather than one rule". They share no JSON shape, but
each Rust type implies exactly one — `String` and `DateTime<Utc>` are strings on
the wire, the `Outcome` enum is a kebab-case string, `i8` is a number, and
`Vec<PhaseRecord>` is an array — so the implication is a rule, read off the
struct, and `1.8.0` reads it.

Measured at `b99239a` over the release binary, on documents derived from
`e2e/fixtures/invariants/unmodified_example.json` and signed:

| document | `drill verify` | `verify_scorecard.py` `1.7.0` | `1.8.0` |
|---|---|---|---|
| `run_id: 42` | **1** — ``invalid type: integer `42`, expected a string`` | **0 `VALID`** | 1 — `run_id is not a string` |
| `phases: "x"` | **1** — `invalid type: string "x", expected a sequence` | **0 `VALID`** | 1 — `phases is not an array` |
| `requested_at: 5` | **1** — ``invalid type: integer `5`, expected an RFC 3339 formatted date and time string`` | **0 `VALID`** | 1 — `requested_at is not a string` |
| `outcome: 7` | **1** — `expected value` | 1, on the `engine.matrix_verdict` *invariant* | 1 — `outcome is not a string` |
| `last_phase_completed: "7"` | **1** — `invalid type: string "7", expected i8` | 1 — `is not an integer` | unchanged |
| `format_version: 1` | **1** — ``invalid type: integer `1`, expected a string`` | 1 — `not a parseable semver` | unchanged (GC12 runs first) |

The mapping from Rust type to JSON type lives in
`crates/logweir/tests/two_reader_parity.rs::json_type_of` and in
`scripts/check-invariant-corpus.sh`, both outside `docs/`, and a required
non-block field of an unmapped Rust type fails those gates loudly rather than
arriving at the script untyped.

**What neither reader claims.** The script's field loop still runs after its
block loop, so a document missing a plain field *and* a block is named for the
block here and for whichever comes first in the struct there. Recorded rather
than closed: full order parity needs one merged loop over all seventeen required
fields.

## `target_diff`, `integrity`, `topic_parity`

| Field | Type | Meaning |
|---|---|---|
| `target_diff.collisions` | array | Mapped target topics that ALREADY EXIST on the target. |
| `target_diff.absent` | array | Mapped target topics that do not exist — the normal case on a scratch cluster. Omitted from the JSON when empty. |
| `target_diff.would_create` | array of `[name, partitions]` | Topics the restore will create, at the partition count it will build. |
| `target_diff.level` | string | `full` in v0.1. Becomes `shallow` only if spec §15 cut 0d is ever taken. |
| `target_diff.not_assessed` | string[], **optional** (1.1.0) | The `collisions` whose CONFIGURATION difference was not assessed, as `"<target topic>: configuration (<why>)"`. See [below](#topic_parity-and-what-its-silence-means). ABSENT means not recorded. |
| `integrity.level` | enum | `byte-fingerprint` (full per-record check), `consume-only` (degraded), `not-attempted` (**no check ran** — an absence of evidence, never a pass). |
| `integrity.result` | enum | `pass`, `fail`, `partial`. |
| `integrity.partial_reason` | string \| null | **Required and non-BLANK when `result` is `partial`** — null, `""` and whitespace-only are all refused, by **both** readers, with the same message. See [`partial_reason` must SAY something](#partial_reason-must-say-something). The `drill show` footer renders it. |
| `integrity.records_sampled` | integer | Measured against `sample.records_expected`. |
| `integrity.records_sampled_matching` | integer | How many reconciled byte-for-byte. |
| `integrity.mismatches` | integer | How many did not. A **compacted** target topic is reported through this path as a mismatch, not as `partial` — see [stability.md](../stability.md). |
| `integrity.pass_rate_measured` | float \| null | `matching / sampled`. **Null in three cases**, and a `byte-fingerprint` document with a null rate is well-formed: the level is not `byte-fingerprint`; not every selection reached a conclusion; or `records_sampled` is 0 (a zero denominator is withheld, never published as NaN). |
| `integrity.restoredPrincipalCouldConsume` | bool \| null | **SP3.** Null, never `false`, until then. The wire name is camelCase deliberately and permanently: renaming it later would be a major bump. |
| `integrity.verification` | object, **optional** (1.4.0) | What the verdict COVERED: `sampled` or `complete` coverage, what it compared against, whether header order was verified, the verified partitions' capture gaps and pruned ranges, and a complete verification's archive integrity and replay comparison. See [below](#integrityverification-format-140). ABSENT means not recorded, read as sampled and never as complete. |
| `topic_parity.intentionally_deviated` | string[] | A SCRATCH drill's deviations on the four settings the restore's own topic creation decides (`cleanup.policy`, `retention.ms`, `partition_count`, `replication_factor`), as `"<target topic>: <key>"`. Since 1.2.0 always `[]` in a `newTopic` restore; in a `newTopic` document before 1.2.0 its entries were NOT reconstructed, whatever the label ([below](#topic_parity-in-a-newtopic-restore-not-reconstructed-120)). |
| `topic_parity.unexpected_divergence` | string[] | Config keys that differed and should not have, as `"<target topic>: <key>"`. Since 1.1.0 also one fail-safe entry `"<target topic>: configuration not assessed (<why>)"` per topic `not_assessed` names ([below](#topic_parity-and-what-its-silence-means)). Since 1.2.0, in a `newTopic` restore, also every entry of `not_reconstructed` ([below](#topic_parity-in-a-newtopic-restore-not-reconstructed-120)). |
| `topic_parity.not_assessed` | string[], **optional** (1.1.0) | The mapped target topics whose CONFIGURATION parity was not assessed, as `"<target topic>: configuration (<why>)"`. See [below](#topic_parity-and-what-its-silence-means). ABSENT means not recorded. |
| `topic_parity.not_reconstructed` | string[], **optional** (1.2.0) | The source settings a `newTopic` restore did NOT reconstruct, as `"<target topic>: <key>"`; each is also in `unexpected_divergence` and never in `intentionally_deviated`. `[]` in a scratch drill. In a `newTopic` document carrying it, `intentionally_deviated` is `[]` and every divergence on the four settings is listed here (arms NR-4, NR-5). See [below](#topic_parity-in-a-newtopic-restore-not-reconstructed-120). ABSENT means not recorded. |

### `topic_parity`, and what its silence means

Before format 1.1.0 an empty `unexpected_divergence` was the whole parity claim,
and it could be empty for the wrong reason: the source's configuration is compared
from the archive MANIFEST's record, which the engine leaves EMPTY when its
DescribeConfigs was denied — and "no overrides recorded" compared as "no
divergence". Since 1.1.0 (FX-4) phase 7 assesses a topic's configuration only
where the backup receipt the restore is BOUND to, verified before phase 0, says
the capture was `captured` ([`config_coverage`](backup-receipt.md#config_coverage--topic-configuration-capture-coverage-format-110)).
Every other mapped topic is named in `not_assessed` with `<why>`:

| `<why>` | when |
|---|---|
| `unknown` | the restore is bound to no recovery point (a v1-shaped plan), or to one whose receipt has no `config_coverage` (every receipt before format 1.1.0) |
| `notCaptured` | the receipt says the capture was not complete (`describeFailed` or `manifestDiffers`) |
| `captureDenied` | the receipt says the source broker refused the configuration read |
| `targetReadDenied` | the source was captured, but the restore identity may not DescribeConfigs the TARGET topic; that topic's keys are then not compared at all. Only a topic whose backup recorded no overrides gets here: for one that did, the pinned engine's restore describes the target itself and phase 6 exits 1 first, with no scorecard |

For a listed topic the two arrays above still name every difference the archive's
own record shows — those are facts — but their SILENCE proves nothing. Partition
count and replication factor come from metadata, not DescribeConfigs, and are
classified either way.

**The fail-safe entry.** For every topic it names in `not_assessed`, phase 7
also writes `"<target topic>: configuration not assessed (<why>)"` into
`unexpected_divergence`. `not_assessed` is new in 1.1.0, so a reader that
predates it — `verify_scorecard.py` before 1.15.0, a `logweir drill show` built
before FX-4, a person with a 1.0.0 guide — reads only the two arrays it always
had, and must not see a clean list for a topic nobody assessed. For
`targetReadDenied` the writer before FX-4 compared the source's record with
the refused read as an empty map. That listed the source's overrides, but with
the pinned engine a topic with overrides never reaches phase 7 under such an
identity (the table above), so for the topics that do, which recorded none,
its list was empty: the entry ends that silence. A configuration key never
contains a space, so a parser of `"<topic>: <key>"` entries tells this one
apart; `not_assessed` stays the authoritative list.
Measured on the live 1.1.0 scorecards of FX-4's fix round with readers built
before FX-4 (main `804c5b2f`): `logweir drill show` prints the entry in its
`topic parity` row, and `logweir drill verify` and `verify_scorecard.py` 1.14.0
accept each document; the 1.0.0 scorecard the pre-FX-4 writer signed for the
same `targetReadDenied` restore says nothing about the topic.

**What "assessed" covers.** An assessed topic's parity compares the configuration
OVERRIDES the engine captured (explicit topic-level settings on its 24-key
allowlist) with the restored topic's values. A value the source inherited from a
broker default is not in the archive's record and is never compared: a source
whose `message.timestamp.type` is `LogAppendTime` from the broker's default,
restored as `CreateTime`, shows no divergence under `not_assessed: []` (measured
live by FX-4). The receipt's `config_coverage` records that effective value and
its source for FX-8; the other effective values are PROD-05.1's. `not_assessed: []` is the claim that every mapped topic was
assessed, and only phase 7 writes it; ABSENT — every 1.0.0 document, and one
whose phase 7 never ran — means not recorded and is never read as that claim.
`logweir drill verify`, `docs/verify_scorecard.py` and `logweir drill show` all
say so in words (`configuration parity: NOT ASSESSED for …` / `not recorded`).
Phase 3 qualifies its collisions the same way, in its own optional
`target_diff.not_assessed`: one `"<target topic>: configuration (<why>)"` entry,
with `<why>` one of the first three reasons above, for every collision whose
SOURCE configuration was not captured. Such a collision's
`differing config: […]` names only what the archive's own record shows, and an
empty list there proves nothing. The collision strings themselves are byte for
byte what a writer before FX-4 produced. `[]` is the claim that every
collision's difference was assessed (vacuously, with no collision), and only
phase 3 writes it; ABSENT means not recorded. Phase 0 refuses a mapped target
topic that already exists, so a collision reaches a scorecard only when one
appears between phase 0 and phase 3.

Neither reader adds an invariant for either field — they are informational —
but both refuse a value serde cannot read as an array of strings
(`drill verify` exit 1, the script exit 1; `shape-index.json` records both
fields). FX-3's `not_reconstructed`, below, has five arms.

### `topic_parity` in a `newTopic` restore: not reconstructed (1.2.0)

Four of the differences phase 7 can find are made by the restore's own topic
creation, in both modes: `cleanup.policy` is left to the target broker's
default (normally `delete`), `retention.ms` is `-1` (so a restored record
older than the broker's retention is not deleted before it is verified), and
the partition count and replication factor are the manifest's and the plan's
`default_replication_factor`. A **scratch** drill restores into a throwaway
cluster that runs `cleanup.policy=delete`, infinite retention and one broker on
purpose, so there they are `intentionally_deviated`, exactly as before. A
**`newTopic`** restore is the recovery itself, and every writer before format
1.2.0 applied the same scratch rationale to it: a compacted source restored as
a delete-policy topic, or a replication-factor-3 source restored at 1, was
signed as "intended".

Since 1.2.0 a `newTopic` restore writes each of the four that differs, as
`"<target topic>: <key>"` (`<key>` is `cleanup.policy`, `retention.ms`,
`partition_count` or `replication_factor`):

- into `not_reconstructed`, the authoritative list: a source setting the
  restore did NOT reconstruct on the target. Applying it is the operator's
  until PROD-05 does it ([stability.md](../stability.md#a-newtopic-restore-does-not-reconstruct-the-sources-topic-settings));
- into `unexpected_divergence`, the list every reader has always shown, so a
  reader older than 1.2.0 sees a divergence: a weaker conclusion than the
  label it replaces, never a stronger one, and never the silence that reads
  as parity;

and never into `intentionally_deviated`, which a 1.2.0 `newTopic` document
leaves `[]`. Every other key that differs is `unexpected` in both modes, as
before, and a scratch drill writes `not_reconstructed: []`.

| Document | `intentionally_deviated` | `unexpected_divergence` | `not_reconstructed` |
|---|---|---|---|
| scratch drill, any version | the four that differ | every other differing key | `[]` from 1.2.0; absent before |
| `newTopic` restore, 1.2.0 | `[]` | every other differing key **and** the four that differ | the four that differ |
| `newTopic` restore, before 1.2.0 | the four that differ, **NOT reconstructed whatever the label** | every other differing key | absent |

**Absent means not recorded:** every document before 1.2.0, and one whose
phase 7 never ran. It is never read as "everything was reconstructed"; `[]` is
that claim, and only phase 7 writes it. A `newTopic` document without the
field predates the distinction, and both readers say what its `intended`
entries are (below).

**Five arms**, in both readers and in this order, fire only on a document
that carries the field, so every document without it is decided exactly as
before. NR-4 and NR-5 also fire only when `target.mode` is `newTopic`:

| Arm | Refuses |
|---|---|
| NR-1 | the field under a `format_version` before 1.2.0: `topic_parity.not_reconstructed is present but format_version "<v>" predates it: the field is defined from 1.2.0` |
| NR-2 | an entry missing from `unexpected_divergence` — the "dropped instead of moved" document an older reader would read as silence |
| NR-3 | an entry also in `intentionally_deviated` |
| NR-4 | a `newTopic` document whose `intentionally_deviated` is not empty: the scratch labels a writer would sign if the mode were lost on its way to phase 7 |
| NR-5 | a `newTopic` document whose `unexpected_divergence` names one of the four settings (`"<target topic>: <key>"`, `<key>` the text after the last `": "`) that `not_reconstructed` omits, for instance `not_reconstructed: []` beside a lost `cleanup.policy` |

Phase 8 runs them before it signs, so no writer of this build can sign a
document that breaks them, and in particular cannot sign `not_reconstructed:
[]`, "nothing left unreconstructed", beside a `newTopic` restore's lost
settings. Both readers also refuse a `not_reconstructed`, an
`intentionally_deviated` or an `unexpected_divergence` that is not an array of
strings (`shape-index.json`).

**What both readers print.** `logweir drill verify` and
`docs/verify_scorecard.py` print the same `reconstruction:` sentence, and
neither changes an exit code:

```
reconstruction: source settings NOT RECONSTRUCTED for restore-20260907T140500Z-orders: cleanup.policy; restore-20260907T140500Z-orders: replication_factor
reconstruction: not recorded, so the settings this newTopic document labels intentionally_deviated were NOT reconstructed: restore-20260907T140500Z-orders: cleanup.policy
```

The first is a 1.2.0 document with a non-empty `not_reconstructed`; the second
a `newTopic` document from before 1.2.0 whose `intentionally_deviated` names
anything. Nothing is printed for `[]`, for a scratch drill, or for a `newTopic`
document with nothing to say. `logweir drill show` adds
`not reconstructed [...]`, or `intended = NOT reconstructed (a newTopic document
before format 1.2.0)`, to its `topic parity` row.

### `integrity.verification` (format 1.4.0)

```json
"verification": {
  "coverage": "complete",
  "comparison_basis": "archive",
  "header_order": "verified",
  "application": "notAttempted",
  "gaps": [{"topic": "orders", "partition": 0, "from_offset": 10, "to_offset": 19}],
  "pruned": [],
  "complete": {
    "covered": true,
    "incomplete_reason": null,
    "max_records": null,
    "window": {"start_ms": null, "end_ms": 1788055200000},
    "archive": {"segments": 4, "segments_verified": 4, "segments_failed": [],
                "segments_unverified": [], "records_decoded": 77, "offset_holes": 0},
    "replay": {"expected": 75, "restored": 75, "matching": 75, "missing": 0,
               "unexpected": 0, "duplicates": 0, "out_of_order": 0, "mismatched": 0},
    "partitions": [
      {"topic": "orders", "partition": 0, "target_topic": "drill-orders", "compared": true,
       "segments": 2, "segments_verified": 2, "records_decoded": 31, "offset_holes": 0,
       "replay": {"expected": 30, "restored": 30, "matching": 30, "missing": 0,
                  "unexpected": 0, "duplicates": 0, "out_of_order": 0, "mismatched": 0},
       "findings": []}
    ]
  }
}
```

A plan chooses how much of the restore phase 7 verifies with
`sample.coverage` ([the plan field](drill-spec.md#samplecoverage-and-samplecomplete_max_records-prod-081)).
**`sampled`**, the default, is the check every earlier scorecard records: the
first records of each sampled partition reconciled by a fingerprint that sorts
headers, the sha256 of the segments those records came from, and the manifest's
count bound for the window, which reads segment first and last timestamps.
**`complete`** reads every archived segment of every partition of every
restored topic, checks its sha256 and decodes it, computes the expected output
from each archived record's OWN timestamp, reads every restored record back,
and compares the two by `x-original-offset`, headers in order. The contract —
the expected-output model and what filters, partition subsets, compaction and
transformations do to it — is
[`PROD-08.1-integrity-contract.md`](../to-do/decisions/PROD-08.1-integrity-contract.md).

| Field | Type | Meaning |
|---|---|---|
| `coverage` | string | `sampled` or `complete` (arm IV-2): the plan's `sample.coverage`, as run. |
| `comparison_basis` | string | What the restored records were compared with: `archive`. Never the source: a loss that happened when the archive was written is in the archive and the target alike ([the archive, not the source](../verify-a-scorecard.md#a-pass-compares-the-restored-topic-with-the-archive-not-with-the-source)). |
| `header_order` | string | `verified` (complete coverage compares each record's headers in order, every occurrence) or `notVerified` (sampled coverage's fingerprint sorts them). Arm IV-3: `verified` only with `complete`. |
| `application` | string | Application-level validation of the restored data: `notAttempted`. |
| `gaps` | object[] | The capture gaps the manifest records for the partitions this run verified, each `{topic, partition, from_offset, to_offset}` (source offsets, inclusive), sorted. Structured and signed; `sample.coverage_note` keeps its sentence. |
| `pruned` | object[] | The ranges retention deliberately removed, for the same partitions, the same shape. |
| `complete` | object, optional | Present exactly when `coverage` is `complete` (arm IV-4). |
| `complete.covered` | bool | `true` when every partition was compared. `false` when the bound stopped the verification or a partition's expected output could not be established; never a `pass` (arm IV-6). |
| `complete.incomplete_reason` | string \| null | Why `covered` is `false`; null when it is `true` (arm IV-5). |
| `complete.max_records` | integer \| null | The plan's `sample.complete_max_records`, the bound in force. |
| `complete.window` | object | The selection the expected output was computed with: a record is expected when its own timestamp is at or before `end_ms` (inclusive) and, when `start_ms` is present, at or after it. `start_ms` ABSENT means no lower bound: the plan's window starts at the archive, so every archived record at or before the end is expected, including one older than every segment's first record. |
| `complete.archive.segments` | integer | Segments the manifest lists for the restored partitions. |
| `complete.archive.segments_verified` | integer | Segments read back whose sha256 matched the manifest, which decoded, and whose decoded count and offsets agreed with the manifest. |
| `complete.archive.segments_failed` | string[] | Segment keys examined and found wrong: a sha256 mismatch, an object the store does not hold, or a decoded count or offset range the manifest contradicts. |
| `complete.archive.segments_unverified` | string[] | Segment keys that could not be examined: no sha256 (written before 0.21), a format the decoder does not read, or past the bound. |
| `complete.archive.records_decoded` | integer | Archived records decoded, inside the window or not. |
| `complete.archive.offset_holes` | integer | Source offsets inside the decoded span that no archived record holds and no recorded gap or pruned range explains — a compacted source's holes. Disclosed, never a fault. |
| `complete.replay.expected` | integer | Archived records the window selects: the expected output. |
| `complete.replay.restored` | integer | Records the target partitions hold. |
| `complete.replay.matching` | integer | Expected records whose first restored copy is byte-identical: key, value, timestamp, and the headers in order. |
| `complete.replay.missing` | integer | Expected records with no restored copy. |
| `complete.replay.unexpected` | integer | Restored records that are no expected record: an `x-original-offset` outside the expected output, or none at all. |
| `complete.replay.duplicates` | integer | Restored records that repeat an `x-original-offset` already read. |
| `complete.replay.out_of_order` | integer | Restored records whose `x-original-offset` is below one read before them. |
| `complete.replay.mismatched` | integer | Expected records whose first restored copy differs from the archive. |
| `complete.partitions[]` | object[] | One entry per partition of every restored topic, sorted: `topic` (archive side), `partition`, `target_topic`, `compared` (false when this partition was not compared), `segments`, `segments_verified`, `records_decoded`, `offset_holes`, `replay` (the eight counts above, for this partition), and `findings`: the first 20 findings in words (which offsets are missing, duplicated, out of order or different, and why a partition was not compared), and one more saying how many were left out. The counts are complete; the words illustrate. |

**Absent means not recorded.** Every document before 1.4.0 — and one whose
phase 7 never ran — is read as a SAMPLED verdict, never a complete one. Every
1.4.0 run that reaches phase 7 writes the block. The legacy counters carry the
complete comparison under complete coverage: `integrity.records_sampled` is the
expected records of the compared partitions, `records_sampled_matching` their
matching records, and `sample.records_expected` the whole expected output.

Seven arms, enforced by both readers in the same position (after
`source.time_basis`, before `redactions`) and words, fire only on a document
carrying the block:

| Arm | Refuses |
|---|---|
| IV-1 | the block under a `format_version` before 1.4.0 |
| IV-2 | a `coverage` other than `sampled` or `complete` |
| IV-3 | a `header_order` other than `verified` or `notVerified`, or `verified` beside sampled coverage |
| IV-4 | a `complete` block without `coverage: complete`, or `coverage: complete` without one |
| IV-5 | `covered: false` without a non-blank `incomplete_reason`, or a reason beside `covered: true` |
| IV-6 | `integrity.result: pass` beside a complete block that is not covered, lists no partition, names a failed or unverified segment, records a missing, unexpected, duplicate, out-of-order or different record — in total or in any one partition — or lists a partition it did not compare |
| IV-7 | totals that are not the sums of `partitions[]`, or segments not each verified, failed or unverified |

Both readers print `integrity coverage:` lines — the coverage, its basis and
header order; a complete block's counts, or `INCOMPLETE` and why; and how many
gaps and pruned ranges the verified partitions record — or the one line saying
the coverage was not recorded; `logweir drill show` renders the same in its
qualifiers footer (`integrity.verification`).

**The number.** 1.4.0; 1.3.0 is FX-8's. A renumber moves
`logweir_core::FORMAT_VERSION` and `scorecard::VERIFICATION_SINCE_MINOR`
together, the justfile's `scorecard_schema_version` and this schema file's
name, `docs/verify_scorecard.py`'s `FORMAT_VERSION` and
`SCORECARD_VERIFICATION_SINCE_MINOR`, the parity script's
`SCORECARD_VERIFICATION_VERSION`, and the literal pins in
`crates/logweir-core/src/lib.rs`, `docs/test_verify_scorecard.py` and the
corpus cases `verification_*.json` (their `format_version` and IV-1's reason).

### `partial_reason` must SAY something

A `partial` integrity result is a statement that the check did not finish, and
`partial_reason` is the only place the document says why. Until Task 4 the two
readers disagreed about what counted as saying why:

- **Rust** tested `partial_reason.is_none()`, so `""` — and `"   "` — was
  accepted **and signed**.
- **`docs/verify_scorecard.py`** tested Python truthiness, so `""` was refused
  and `"   "`, being a truthy string, was accepted.

So `partial_reason: ""` was a document Logweir would sign and the auditor's own
verifier would refuse, in the exact file whose docstring says that if the two
readers disagree "the signed-scorecard format is broken". Both now apply the
same predicate — Rust
`partial_reason.as_deref().unwrap_or("").trim().is_empty()`, Python
`not str(integrity.get("partial_reason") or "").strip()` — in the same position
with the byte-identical message
`integrity.result is 'partial' but partial_reason is null`. `drill verify`
exits **4** (GC11); the Python verifier exits **1**.

Like the `evidence` arm below, this is a **retroactive tightening of the 1.0.0
reader, not a format change**: no byte of the format changes, the accepted set
narrows, and no document Logweir has ever written is refused —
`crates/logweir/src/drill/phase7_verify.rs` builds the field as
`(!notes.is_empty()).then(|| notes.join("; "))`, so it is either absent or says
something. `crates/logweir/tests/two_reader_parity.rs` runs both readers over
`e2e/fixtures/invariants/` and compares their refusal text, so the agreement is
checked rather than asserted.

## `engine_subreport`

**`null` in every scorecard v0.1 produces.** `OsoCliEngine` does not override
`DataEngine::validation_run`, so the engine's own `validation run` is never
invoked and nothing is retained. Reading `null` means **"no engine sub-report
was retained"**, not "the engine reported nothing wrong".

When populated (a future version), it carries `retained_verbatim`,
`retrieved_from`, `caveat`, `body_b64` and `body_sha256`. `body_b64` is the
engine's report as the **exact stored bytes**, base64 — never a parsed and
re-serialised value, because upstream's envelope covers the exact bytes and any
re-serialisation would break the digest it signed. `body_sha256` is the sha256
of the **decoded** bytes, so the binding is checkable without decoding.

And `caveat` is not decoration. It states, in the document, that the engine's
own `integrity.checksums_valid` is a hardcoded constant `true` and its restore
timing fields are all null — so the sub-report **corroborates nothing Logweir
claims**. `logweir drill show` renders it under the table for exactly that
reason.

## `evidence` — makes NO claim about the upload

All four fields describe facts that exist only **after** the scorecard has been
uploaded. The scorecard is signed **before** that upload (a signature covers
bytes, and the bytes must exist first) and is never re-serialised afterwards.
So Logweir **zeroes all four before signing**:

```json
"evidence": { "create_only_enforced": false, "immutable": false,
              "retain_until": null, "version_id": null }
```

Read those as **"no proof was obtainable at signing time"**, never as "the
object is mutable" or "the put was not conditional". The document deliberately
under-claims — which is what guarantees a valid Logweir signature can never
cover an unsubstantiated WORM or create-only assertion.

### The zeroing is ENFORCED, and that is a reader tightening, not a format change

For one release the paragraph above was a sentence and nothing else: the writer
zeroed the four fields, `docs/verify_scorecard.py` printed the guarantee on
every successful verification, and no validator checked it — while the two
signed fixtures this repository ships as its worked example carried
`create_only_enforced: true` and verified `VALID`.

Both readers now refuse it. `Scorecard::validate_invariants`
(`crates/logweir-core/src/scorecard.rs`) and
`docs/verify_scorecard.py::check_invariants` each carry the same arm,
immediately after the Global-Constraint-12 major-version refusal and before
every other rule, testing `version_id`, `retain_until`, `immutable` and
`create_only_enforced` in that order with byte-identical messages. A `1.0.x`
scorecard with any of the four set is refused by both — `drill verify` exits
**4** (GC11), the Python verifier exits **1**.

**This is a deliberate RETROACTIVE TIGHTENING of the 1.0.0 reader, not a format
change** — `format_version` stays `1.0.0` and **Global Constraint 12 holds**:

- No byte of the format changes. No field is added, renamed or removed, and
  `schemas/logweir-drill-scorecard-1.0.0.json` is untouched.
- The accepted set **narrows**. Narrowing what a reader accepts is not a major
  bump under GC12, which reserves a major for a changed *identity rule*.
- The writer's zeroing at `crates/logweir/src/drill/phase8_score.rs` is
  **unconditional**, so **no document Logweir has ever written is refused**.
  The only documents this arm refused when it landed were the two committed
  fixtures, which were re-minted in the same change.
- A document written by a **non-Logweir path** that carries a post-put claim
  **is** now refused, and that is precisely the intent: the guarantee is that a
  valid signature over a 1.0.x scorecard cannot cover a storage claim the
  signer was not in a position to make.

The arm is scoped to major 1, so a future major remains free to redefine the
block. `docs/verify_scorecard.py` reports which invariant set ran on its
success path (`verifier: verify_scorecard.py <SCRIPT_VERSION>`); `SCRIPT_VERSION`
tracks the invariant set and is **not** the format version.

The real readback is published in a **second signed document**, the put receipt
(`<run_id>.receipt.json` + `.sig`, payload type
`application/vnd.logweir.drill-put-receipt+json;version=1.0.0`). Verify it with
the shipped tool:

```bash
python3 docs/verify_scorecard.py --payload-type receipt \
    <run_id>.receipt.json <run_id>.receipt.sig public.pem
```

## `redactions`

Always `[]` in v0.1. `--redact` is a v0.1.1 feature and the only redactable
paths will be `/target/cluster_id` and `/approval/approver`.

### That sentence is ENFORCED, and that is a reader tightening, not a format change

For one release the sentence above was prose and nothing else. The field
existed, no reader checked it and no surface displayed it, so a third party
could hand an auditor a scorecard carrying

```json
"redactions": [{"path": "/measured/rpo_seconds", "reason": "customer policy", "present": false}]
```

and **both** readers printed `VALID` while the document itself said a field the
auditor reads first had been removed.

Both readers now refuse a non-empty `redactions`.
`Scorecard::validate_invariants` (`crates/logweir-core/src/scorecard.rs`) and
`docs/verify_scorecard.py::check_invariants` each carry the same arm, **last**,
after every other rule, with the byte-identical message

```
redactions is non-empty but format_version <the document's own version> has no way to produce one; --redact is a v0.1.1 feature
```

`drill verify` exits **4** (GC11); the Python verifier exits **1**. The
position is part of the contract and is mutant-tested: a document that violates
this arm *and* an earlier one reports the **earlier** arm's message, from both
readers.

### And every surface that shows you a scorecard now says so

Refusing it in the two verifiers is half the fix. The field was also *displayed
by nothing*, on **three** surfaces, so a redacted document looked whole
everywhere a human or a dashboard actually reads one. All three now carry it:

| Surface | What it shows for a non-empty `redactions` |
|---|---|
| `drill show --format table` | A line in the **qualifiers** footer: the count and every removed path, plus a note that `drill verify` and `docs/verify_scorecard.py` both refuse the document. `show` does not verify and its exit code is unchanged — a redaction is a qualifier, not a "this reader cannot honestly render this" condition. |
| Prometheus textfile (`--metrics-textfile`) | `logweir_drill_redactions{cluster="…"}`, emitted **unconditionally** — `0` for a whole document — so `logweir_drill_redactions > 0` is a valid alert. A count, never a path label: paths are document-controlled and would be unbounded cardinality. |
| Notification body (webhook / Slack / PagerDuty `custom_details`) | A `redactions` array of the removed paths, always present and empty for a whole document. This is the only surface that reaches a human away from a terminal. |

`--format json` needed no change: it prints the signed bytes, so the array was
always visible there. The put receipt needed none either — it binds the
scorecard by digest and reports storage facts, and echoes no scorecard field.

**The `reason` strings are deliberately not displayed** by the footer or the
notification body. A non-empty `redactions` is by construction a document no
Logweir writer produced, so its `reason` is free text that arrived with the
document, and both of those surfaces are read by a human deciding how much to
trust what they are looking at. The **path** is the whole actionable signal.

**This is a deliberate RETROACTIVE TIGHTENING of the 1.0.0 reader, not a format
change** — `format_version` stays `1.0.0` and **Global Constraint 12 holds**,
by the identical argument recorded for the `evidence` arm above:

- No byte of the format changes. No field is added, renamed or removed, and
  `schemas/logweir-drill-scorecard-1.0.0.json` is untouched.
- The accepted set **narrows**. Narrowing what a reader accepts is not a major
  bump under GC12, which reserves a major for a changed *identity rule*.
- **No document Logweir has ever written is refused**, because no v0.1 code
  path constructs a `Redaction` — `redactions` is only ever `vec![]`. Every
  committed fixture carries `[]` and none of them was re-minted for this
  change.
- A document from a **non-Logweir path** that carries a redaction **is** now
  refused, and that is the intent: v0.1 has no writer that can produce one, so
  a non-empty array means the document was edited after signing-time
  construction or came from a reader-incompatible producer.

A `2.x` document never reaches this arm — `refuse_unreadable_major` refuses it
first. In **0.1.1**, `--redact` **replaces** this arm with a path whitelist
(`/target/cluster_id`, `/approval/approver`); it does not delete it.

---

Documentation is licensed [CC-BY-4.0](../LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
