# The drill scorecard format, field by field

`application/vnd.logweir.drill-scorecard+json;version=1.0.0`

The machine-readable schema is
[`schemas/logweir-drill-scorecard-1.0.0.json`](../../schemas/logweir-drill-scorecard-1.0.0.json)
and CI diffs it against the code on every build, so this document and the
schema cannot drift apart silently. A worked example is
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
| `format_version` | string | Semver of this format. `1.0.0` in v0.1. |
| `run_id` | string | ULID. Also the object key stem in the evidence bucket. |
| `outcome` | enum | Exactly four values: `pass`, `fail-objective`, `fail-integrity`, `preflight-failed`. There is **no `refused` and no `error` outcome** — a refused plan and an operational failure produce **no scorecard at all** (exit 3 and exit 1); an outcome value for them would imply a signed document that does not exist. `drift` is not a v0.1 value either: v0.1 collects no metadata, so nothing could produce it. |
| `last_phase_completed` | integer | Domain `-1..=9` (eleven phase slots). `-1` is the `--from-cluster` source-capture phase, which is in v0.1's scope but whose code lands in a follow-up — see [ADR 0007](../adr/0007-from-cluster-in-v0.1.md) — so **v0.1.0 never emits `-1`**. **A v0.1.0 SIGNED document reads 5, 6 or 7 and never 8 or 9** — see the note below. |
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
| `target.cluster_id` | string | The scratch cluster's own id, read from it. |
| `target.marker_topic` | string | The segregation proof. Its absence refuses the drill at phase 0 with exit 3, before anything runs. |
| `target.topic_mapping_prefix` | string | Prefix applied to restored topic names. |
| `target.topic_mapping_sha256` | string | `sha256:` of the mapping, so the mapping is attested rather than described. |
| `target.topic_mapping_entries` | integer | How many mapping entries there were. |

## `approval`

| Field | Type | Meaning |
|---|---|---|
| `approval.approver` | string | Who approved. |
| `approval.ticket` | string | Their change ticket. |
| `approval.plan_hash` | string | `sha256:` of the **exact spec bytes** the drill ran. Approving one document and running another is refused at phase 1. |
| `approval.approved_at` | RFC 3339 | When. |
| `approval.key_id` | string | Lowercase hex sha256 of the approver key's SPKI DER. |
| `approval.self_attested` | bool | `true` when the approving key **equals** the signing key: the same party planned, ran and vouches for the result. Never refused, always labelled. Treat `true` as a reason to seek corroboration. |

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
| `topics`, `partitions` | integer | How many of each were selected. |
| `records_expected` | integer | **The canary size**: how many records this drill set out to reconcile — `records_per_partition` summed over the partitions actually selected. It is **not** how many records the manifest says the window holds; those differ by orders of magnitude on a real archive. |
| `records_restored` | integer | How many were restored. |
| `anchor` | string | The vocabulary is `head`, `tail`, `random`; **v0.1 implements only `head`** and REFUSES the other two at phase 0 with exit 3 rather than silently substituting. The scorecard field is a plain string (the closed enum lives on the input spec, `logweir_core::spec::Anchor`, which is where a bad value has to be caught); a v0.1.0 scorecard therefore always reads `head`. See [stability.md](../stability.md). |
| `coverage_note` | string | What the drill itself says about how representative the window is. Read it. |

## `target_diff`, `integrity`, `topic_parity`

| Field | Type | Meaning |
|---|---|---|
| `target_diff.collisions` | array | Mapped target topics that ALREADY EXIST on the target. |
| `target_diff.absent` | array | Mapped target topics that do not exist — the normal case on a scratch cluster. Omitted from the JSON when empty. |
| `target_diff.would_create` | array of `[name, partitions]` | Topics the restore will create, at the partition count it will build. |
| `target_diff.level` | string | `full` in v0.1. Becomes `shallow` only if spec §15 cut 0d is ever taken. |
| `integrity.level` | enum | `byte-fingerprint` (full per-record check), `consume-only` (degraded), `not-attempted` (**no check ran** — an absence of evidence, never a pass). |
| `integrity.result` | enum | `pass`, `fail`, `partial`. |
| `integrity.partial_reason` | string \| null | **Required and non-empty when `result` is `partial`** — `verify_scorecard.py` refuses a `partial` without one. The `drill show` footer renders it. |
| `integrity.records_sampled` | integer | Measured against `sample.records_expected`. |
| `integrity.records_sampled_matching` | integer | How many reconciled byte-for-byte. |
| `integrity.mismatches` | integer | How many did not. A **compacted** target topic is reported through this path as a mismatch, not as `partial` — see [stability.md](../stability.md). |
| `integrity.pass_rate_measured` | float \| null | `matching / sampled`. **Null in three cases**, and a `byte-fingerprint` document with a null rate is well-formed: the level is not `byte-fingerprint`; not every selection reached a conclusion; or `records_sampled` is 0 (a zero denominator is withheld, never published as NaN). |
| `integrity.restoredPrincipalCouldConsume` | bool \| null | **SP3.** Null, never `false`, until then. The wire name is camelCase deliberately and permanently: renaming it later would be a major bump. |
| `topic_parity.intentionally_deviated` | string[] | Config keys the drill deliberately set differently on the target. |
| `topic_parity.unexpected_divergence` | string[] | Config keys that differed and should not have. |

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

---

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
