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
`occurrences` (default `1`) exists only for the case where two statements share
an identical message literal.

## Extending it

Cases are added by writing a document here and an entry in `index.json` —
**never** by editing the walker. When a new case first covers a statement,
delete that statement's entry from `uncovered-arms.json` in the same change;
the arithmetic in `every_invariant_arm_has_a_corpus_case` will not close
otherwise, and that failure is the point. Task 5 did exactly that and touched
no walker code.

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

---

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
