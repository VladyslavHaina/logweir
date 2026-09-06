# The invariant corpus

One document per case that `Scorecard::validate_invariants`
(`crates/logweir-core/src/scorecard.rs`) and
`docs/verify_scorecard.py::check_invariants` must decide **the same way**.

`crates/logweir/tests/two_reader_parity.rs` walks every entry in
[`index.json`](index.json) and runs **both** readers over it. Logweir's
evidentiary claim rests on the second reader being independent and equivalent —
`docs/verify_scorecard.py` says in its own module comment that if the two
disagree, "the signed-scorecard format is broken, not merely this script". For
one release that claim was false: `partial_reason: ""` was accepted and signed
by Rust and refused by the script. This corpus is what makes the claim
checkable rather than merely asserted.

## The cases are UNSIGNED, and signed at test time

Every `<id>.json` here is a plain scorecard with no sidecar. The walker writes
the file's bytes **unchanged** into a temp directory, signs exactly those bytes
with the checked-in throwaway key `e2e/fixtures/signed/signing.pem`, and hands
the pair to both readers.

Signing is not optional, and not a formality. `logweir drill verify` checks the
signature **before** it parses and before it calls `validate_invariants`, so an
unsigned case would exit `4` from the signature path and the walker would pass
for the wrong reason.

Signing here is **not a re-mint**. Nothing under `e2e/fixtures/signed/` is
written — ruling R-G reserves the single fixture re-mint to Task 2, and this
directory is deliberately outside it.

## `index.json`

A JSON array. Each element is:

| field | meaning |
|---|---|
| `id` | the case name, used in failure messages |
| `file` | the document, relative to this directory |
| `rust_exit` | the exact exit code `logweir drill verify` must return |
| `python_exit` | the exact exit code `docs/verify_scorecard.py` must return |
| `reason` | the **bare** invariant string — no `scorecard invariant violated:` prefix, no `INVALID:` prefix. `""` marks the accept-control. |
| `arm` | required on refusing entries: a single-line verbatim fragment of the message literal of the statement that actually fires. Absent on the accept-control. |

**The two readers do not share an exit-code space and are not meant to.**
`drill verify` follows Global Constraint 11 (`4` for a self-contradicting
document); `verify_scorecard.py`'s own contract is `0` VALID / `1` INVALID /
`2` could-not-run. Parity is therefore asserted as the same *verdict*, each
with its own recorded code, plus a byte-identical reason once each reader's
fixed prefix is stripped. Recording both codes per case keeps the assertion
exact rather than a "non-zero" weakening.

`unmodified_example.json` is a byte copy of `e2e/fixtures/scorecard-pass.json`
and is the accept-control: without it, a walker that only ever asserts refusals
would pass against a reader that refused everything.

## `uncovered-arms.json`

The other half of the accounting. `every_invariant_arm_has_a_corpus_case`
counts every `return Err(InvariantError` **statement** in
`validate_invariants`'s body and requires each one to be either named by a
refusing `index.json` entry's `arm` or listed here with a `why`. Never both,
never neither.

The unit is the statement, not the conceptual arm, because that is the unit a
text scan can measure — Task 2's evidence arm is one idea and four statements.
`occurrences` (default `1`) says how many statements share an identical message
literal.

`occurrences: 0` is the one entry kind that is **not** an arm. It records a
**reader asymmetry** — a document the two readers decide differently for a
reason that lives outside `validate_invariants`, so no `index.json` case can
pin it and no statement count should include it. The walker adds `0` to the
accounting and skips the body check, so such an entry cannot inflate
`uncovered_total`.

**No entry uses it today.** Two did — a missing `evidence` block and a missing
or mistyped `sample` block — and Task 5c retired both by closing the gap they
recorded rather than by deleting the record: `docs/verify_scorecard.py` now
refuses a missing `sample` block and a non-integer `sample.records_expected`,
so both readers refuse both documents, and the pair is pinned by
[`shape-index.json`](shape-index.json) (below) instead. The kind stays
documented because the next asymmetry anybody finds should be written down the
same way, in the interval between finding it and fixing it.

Read the guarantee narrowly. What `occurrences: 0` cannot do is close the
arithmetic by *inflating* the uncovered side. What nothing here can do is
survive the opposite move: deleting an arm from `validate_invariants` together
with its entry drops `n` and `uncovered_total` in step and re-balances in
silence — measured, in Task 5's re-review, against the walker, the corpus shell
gate and pytest, all of which stayed green. **An entry, at any `occurrences`, is
only as strong as the per-arm Rust unit test behind it**, which asserts the
exact refusal text and is the one thing that fails. Every arm named here is
also named by a `#[test]` in `crates/logweir-core/src/scorecard.rs`.

Every entry either names a successor task or states in its `why` why no
successor is owed — the two `not finite` entries are uncorpusable by
construction (`serde_json` refuses the bare `NaN` token at *parse* time, so
Rust never reaches `validate_invariants`, while `json.loads` accepts it), and
they say so. "Later" and "nobody" are not successors.

## Extending it

Cases are added by writing a document here and an entry in `index.json` —
**never** by editing the walker. When a new case first covers a statement,
delete that statement's entry from `uncovered-arms.json` in the same change;
the arithmetic in `every_invariant_arm_has_a_corpus_case` will not close
otherwise, and that failure is the point. Tasks 5 and 5c did exactly that: six
documents, six `index.json` entries, six deletions from `uncovered-arms.json`,
and no change to the walker's own case handling.

A document **neither reader reaches an invariant on** is not an `index.json`
case at all — it goes in [`shape-index.json`](shape-index.json), described at
the end of this file, because the claim it carries is a different one and is
asserted differently.

## The `outcome` cases (T0-4, Task 5)

Nine of them, for the six arms that make `outcome` an entailment of the rest of
the document rather than a free-standing label. Six are one-per-arm:
`outcome_pass_with_partial_integrity`, `outcome_pass_with_partial_reason`,
`outcome_pass_with_unmet_objective`, `outcome_pass_with_incomplete_sample`,
`sample_exceeding_expected` and `matrix_pass_without_byte_fingerprint`. The
first four are the entailments a `pass` carries; the fifth says a drill cannot
reconcile more records than it selected; the sixth says a `pass` matrix verdict
means the drill passed **at byte-fingerprint level**.

The other three exist because one case per arm is not enough for the matrix
arm and for order:

| case | what only it can catch |
|---|---|
| `matrix_pass_on_a_degraded_pass` | a real `pass` at `consume-only`, whose honest matrix value is `pass-degraded`. Kills a matrix arm with the `level == byte-fingerprint` conjunct dropped — which `matrix_pass_without_byte_fingerprint` does **not**, because that document's outcome is already not a `pass`. |
| `matrix_pass_on_a_non_pass_at_byte_fingerprint` | the mirror: byte-fingerprint level, but the drill did not pass. Kills a matrix arm with the `outcome == pass` conjunct dropped. |
| `pass_with_a_fail_result_and_a_matrix_pass` | ORDER. The document violates the first new arm and the last one at once, so reordering either reader changes the message and the walker's text comparison fails. |

## The six pre-existing arms (Task 5c)

Task 4 built this corpus and Task 5 filled it for the six arms Task 5 wrote.
Six arms **older** than either had no case at all, and three of those had no
Rust unit test either — one occurrence of the message in the whole crate, the
arm itself. Task 5c closed both halves. Each document differs from
`unmodified_example.json` by **exactly** the overrides that make its arm fire,
plus any override an EARLIER arm forces:

| case | overrides | forced by an earlier arm |
|---|---|---|
| `objectives_met_at_a_reduced_level` | `integrity.level` → `consume-only` | — |
| `negative_rpo_objective` | `objectives.rpo_seconds` → `-300` | — |
| `more_matching_than_sampled` | `integrity.records_sampled_matching` → `100` | — |
| `matrix_fail_without_a_reason` | `engine.matrix_verdict` → `fail` | — |
| `pass_rate_measured_without_byte_fingerprint` | `integrity.level` → `consume-only` | `objectives.met` → `null` (the `met must be null when pass_rate is not measurable` arm) and `engine.matrix_verdict` → `pass-degraded` (the `matrix_verdict 'pass'` arm) — both sit earlier and would otherwise steal the failure |
| `last_phase_completed_out_of_domain` | `last_phase_completed` → `42` | — |

The three that were Rust-bare — `matrix_fail_without_a_reason`,
`pass_rate_measured_without_byte_fingerprint` and
`last_phase_completed_out_of_domain` — each also gained an
`invariants_refuse_…` unit test in `crates/logweir-core/src/scorecard.rs`. The
two protections answer different mutants and neither replaces the other: the
corpus case turns a Rust-only deletion into a visible two-reader disagreement,
and the unit test is the only thing that survives an attacker who deletes the
arm *and* its `uncovered-arms.json` entry in the same edit.

## `shape-index.json` — parity BEFORE the invariants

Three documents that neither reader reaches an invariant on:
`no_sample_block.json` and `no_evidence_block.json`, each
`unmodified_example.json` with one whole top-level block removed and nothing
else touched, and `records_expected_not_an_integer.json`, which is the same
document with the single override `sample.records_expected: 75 → "75"`.

The third is not decoration. Under mutation, deleting the
`records_expected` type check from `verify_scorecard.py` and leaving the two
block cases as the only shape cases left the walker **green**: no corpus
document had a mistyped `records_expected`, so nothing ran the check. It is
here because the mutant survived without it.

`sample` and `evidence` are non-optional fields of
`logweir_core::scorecard::Scorecard`, so `serde_json` refuses a document
missing one at **deserialisation** — `drill verify` exits `1` with `signature
verified but the payload is not a scorecard: missing field ...` and never calls
`validate_invariants`. `docs/verify_scorecard.py` has no such layer and asserts
the same shape in its block-presence loop.

These cannot be `index.json` entries, and that is measured rather than assumed:
an entry for `no_sample_block.json` makes
`two_reader_parity_over_the_invariant_corpus` report *"index.json records a
refusal reason, but at least one reader produced no invariant refusal line —
rust: None"*, because the walker strips two **invariant** prefixes off Rust's
stderr and there is no invariant refusal there to find. So the pair lives in
its own index and its own walker,
`two_reader_parity_on_documents_refused_before_the_invariants`, which asserts a
different and equally exact claim: both readers refuse, at the exit codes
recorded here, each with its own recorded text, **and neither on an invariant**.
That last clause is what fails if one reader ever moves such a document into
invariant space alone.

Until Task 5c the `sample` half was a live disagreement, not a formality: on
`no_sample_block.json`, `drill verify` exited `1` and `verify_scorecard.py`
printed `VALID` and exited `0`.

---

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
