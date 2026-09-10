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

Thirty-three documents that neither reader reaches an invariant on. Eleven are
`unmodified_example.json` with one whole required block removed and nothing else
touched; one has two removed at once; six have one required NON-block field
removed and six carry one at the WRONG JSON TYPE; three override
`sample.records_expected` alone; six set a non-`Option` `u64` field to `null`.

Every required field of `logweir_core::scorecard::Scorecard` — block or not —
carries no `#[serde(default)]`, so `serde_json` refuses a document missing one at
**deserialisation**: `drill verify` exits `1` with `signature verified but the
payload is not a scorecard: missing field ...` and never calls
`validate_invariants`. It refuses a field of the wrong type there too (`invalid
type: integer \`42\`, expected a string`). `docs/verify_scorecard.py` has no such
layer and asserts the same shape in its `REQUIRED_BLOCKS` and `REQUIRED_FIELDS`
loops — the second of which carries, since Task 5f, the JSON type each field's
Rust type implies as well as its name.

### The `check` field, and the arithmetic it closes (Task 5d)

Task 5c's review measured what this index could not do. Deleting a shape check
from `verify_scorecard.py` **together with** its corpus case and its pytest left
every gate green — the walker at 0, this directory's shell gate at 0 (reporting
one fewer case, with no complaint) and `just lint` green — because nothing
pinned the case count and nothing outside the deleted files knew the check had
existed. Five of the seven block checks then had no corpus case at all, so
deleting any of those loop entries was silent on its own.

Every entry now carries a `check` saying what it protects, and
`crates/logweir/tests/two_reader_parity.rs::every_required_block_has_a_shape_corpus_case`
plus `scripts/check-invariant-corpus.sh` require the arithmetic to close against
**the Rust struct**, which a deletion in the Python and the corpus does not
touch:

| `check` | what it binds |
|---|---|
| `block:<name>` | exactly one case per required block of `Scorecard`, and exactly one block per case. `REQUIRED_BLOCKS` in `verify_scorecard.py` must name all eleven, **in the struct's declaration order**. |
| `field:<name>` | exactly one case per required NON-block field of `Scorecard`, ABSENT, and exactly one field per case (Task 5e). `REQUIRED_FIELDS` in `verify_scorecard.py` must name all six, **in the struct's declaration order**. A required field is one carrying no `#[serde(default)]`; a block is one whose type is a struct declared in the same file, so these six are the complement. |
| `type:<name>` | exactly one case per required NON-block field, PRESENT but of a JSON type its Rust type refuses (Task 5f). The second element of each `REQUIRED_FIELDS` entry is that JSON type, derived from the Rust type by `json_type_of` in the walker and in this directory's shell gate. |
| `null:<dotted name>` | exactly one case per **non-`Option`** `u64` field of the document, set to `null` (Task 5f). Both recorded exits must be refusals. This is the kind that gives the optionality flag's USE the arithmetic its LIST already had. |
| `message:<fragment>` | a literal that must appear in `check_invariants`'s code, comments stripped — the shape layer's version of `index.json`'s `arm`. Several cases may name one fragment. **The weakest kind**: it closes over the code and not over the corpus, so prefer a kind with its own arithmetic wherever the struct can supply one. |
| `order:<a>,<b>` | a document missing several blocks. Both recorded refusals must name the same block: the first of them in the struct's declaration order. |

Task 5e added a THIRD closed list beside those two, and it is not a `check`
kind because it is not about documents at all: `U64_FIELDS` in
`verify_scorecard.py` — every field the Rust reader types as `u64`, with its
dotted name, in the struct's declaration order, and a flag that is `True`
exactly for `Option<u64>`. It is re-derived from
`crates/logweir-core/src/scorecard.rs` by
`crates/logweir/tests/two_reader_parity.rs::every_u64_field_has_the_same_domain_check_in_both_readers`
and by `scripts/check-invariant-corpus.sh`. Task 5d's review, finding F1,
measured why that mattered: the list's only struct-derived check lived in
`docs/test_verify_scorecard.py`, so deleting one entry, its pytest case and that
test in one edit left pytest, the walker and this directory's shell gate all
green — and put `integrity.mismatches: 2**64` back to `drill verify` exit `1`
against `VALID` from the script.

That is the one thing the invariant corpus cannot do (see the `occurrences: 0`
paragraph above): there `n` is counted from the same function the arm is deleted
from, so a coordinated deletion re-balances. Here `n` is counted from
`crates/logweir-core/src/scorecard.rs`.

### Why each odd one is here

* `records_expected_not_an_integer.json` (`75 → "75"`) is not decoration. Under
  mutation, deleting the `records_expected` type check while the two block cases
  were the only shape cases left the walker **green**: no corpus document had a
  mistyped `records_expected`, so nothing ran the check. It is here because the
  mutant survived without it (Task 5c).
* `records_expected_out_of_u64_domain.json` (`75 → 18446744073709551616`) and
  `records_expected_negative.json` (`75 → -1`) are Task 5c's review finding F1,
  measured at `6619090`: on the first, `drill verify` exited `1` and
  `verify_scorecard.py` printed `VALID`; on the second both refused, but Rust at
  deserialisation and Python on an *invariant* (`records_sampled (75) exceeds
  sample.records_expected (-1)`) — the same verdict for a different reason.
* `no_format_version_field.json`, `no_run_id_field.json`,
  `no_outcome_field.json`, `no_last_phase_completed_field.json`,
  `no_requested_at_field.json` and `no_phases_field.json` are Task 5d's review
  finding F3, measured at `7e85937`: with `run_id`, `requested_at` or `phases`
  absent, `drill verify` exited `1` and `verify_scorecard.py` printed `VALID`
  and exited `0`; with `outcome` absent it refused on an *invariant* about
  `engine.matrix_verdict`, which is the same verdict for a reason that is not
  what is wrong with the document. `no_format_version_field.json` is the one
  whose recorded `python_reason` is not the field loop's message: the Global
  Constraint 12 rule runs first and says `format_version None is not a parseable
  semver`. It is in the corpus anyway, because the arithmetic is derived from
  the struct and an exception would be a hole in it.
* The six `null` cases — `records_expected_null.json`,
  `records_restored_null.json`, `records_sampled_null.json`,
  `records_sampled_matching_null.json`, `mismatches_null.json` and
  `duration_ms_null.json` — are finding F4, measured the same way: `null` on a
  field Rust types as a plain `u64` was `invalid type: null, expected u64` from
  `drill verify` and `VALID` here, because `if value is None: continue` skipped
  all eleven `u64` fields when only five are `Option<u64>`. Task 5e added three
  of them as `message:` cases sharing one fragment; **Task 5e's review, finding
  F1, measured that that was not enough** — reverting `if optional and value is
  None:` to `if value is None:` and deleting those three cases and both null
  pytests in ONE edit left the walker at 0, this gate at 0 (`all 42 cases`,
  three fewer and no complaint) and pytest at 0, with
  `sample.records_restored: null` back to `drill verify` exit `1` against
  `VALID`. Task 5f derives them instead: one `null:` case per **non-`Option`**
  `u64` field, counted against the struct, so the same edit now fails at
  assertion with the missing cases named. The control lives in `index.json` as
  `option_rto_seconds_null.json` (`measured.rto_seconds → null`), an ACCEPT case
  both readers exit `0` on: without it, refusing null everywhere would look like
  a fix.
* The six `type:` cases — `format_version_not_a_string.json` (`"1.0.0" → 1`),
  `run_id_not_a_string.json` (`→ 42`), `outcome_not_a_string.json` (`→ 7`),
  `last_phase_completed_not_an_integer.json` (`7 → "7"`),
  `requested_at_not_a_string.json` (`→ 5`) and `phases_not_a_list.json`
  (the whole array `→ "x"`) — are the wrong-type residual `1.7.0` recorded and
  Task 5e's review confirmed, measured at `b99239a`: `run_id: 42`,
  `phases: "x"` and `requested_at: 5` were each `drill verify` exit `1` against
  `VALID` here, and `outcome: 7` refused here on an *invariant* about
  `engine.matrix_verdict`. The last two rows are documents both readers already
  refused, and they are in the corpus anyway for the same reason
  `no_format_version_field.json` is: the arithmetic is derived from the struct
  and an exception would be a hole in it.
* `no_measured_and_no_integrity_blocks.json` is finding F3 of Task 5c's review,
  and it is the pair
  that actually diverged: `drill verify` named `measured` and
  `verify_scorecard.py` named `integrity`. (The review's own probe — `sample`
  and `evidence` — agrees under either order, because `sample` precedes
  `evidence` both ways.) Pinning the loop to the struct's declaration order is
  what makes both readers name `measured`.

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
printed `VALID` and exited `0`. Task 5d found three more of exactly that shape —
`target`, `target_diff` and `topic_parity`, each measured the same way at
`6619090` — which is what the closed arithmetic above is for: the block list is
now taken from the struct rather than grown one arm at a time. Task 5e found
three more outside the block list entirely (`run_id`, `requested_at`, `phases`)
and five more on `null`, and closed both by widening the same derivation. Task 5f
found three more again — the same three fields at the wrong TYPE — and closed
them by widening it once more, this time to carry the JSON type each field's Rust
type implies.

## `backup-receipt-index.json` — the BACKUP RECEIPT's corpus (Task 5b)

Eight documents, and a different document type: `logweir_core::backup_receipt::
BackupReceipt`, whose five arms are mirrored by
`docs/verify_scorecard.py::check_backup_receipt_invariants`. Same six fields as
`index.json` (interface **I31**), same accept-control discipline, and the same
"the cases are UNSIGNED and signed at test time" rule — with one difference that
matters: they are signed under the **receipt's own media type**
(`application/vnd.logweir.backup-receipt+json;version=1.0.0`). A receipt signed
under the scorecard's type is refused by both readers at the `payloadType`
comparison, and every case would then "agree" for the wrong reason.

`unmodified_receipt.json` is a byte copy of
`e2e/fixtures/signed/backup-receipt.json` and is the accept-control. The other
seven each differ from it by exactly the override that makes one arm fire:

| case | override | arm |
|---|---|---|
| `format_version_major_2` | `format_version` → `2.0.0` | 1 |
| `exit_code_zero_without_a_manifest` | `archive.manifest_key` → `""` | 2 |
| `manifest_without_exit_code_zero` | `exit_code` → `1` | 2, the other direction |
| `records_missing_a_named_topic` | `records` loses `payments` | 3 |
| `records_names_an_unlisted_topic` | `records` gains `invoices` | 3, the other direction |
| `covered_from_after_to` | `covered.from_ms` and `to_ms` swapped | 4 |
| `source_auth_mode_is_the_legacy_spelling` | `source.auth.mode` → `scram-sha-512` | 5 |

Arms 2 and 3 are BICONDITIONALS, which is why each has two cases: a single case
per arm passes against a reader that checks one direction only.

Arm 5 — the CLOSED VALUE SET on `source.auth.mode` — arrived in Task 5b's fix
round 1 with the controller's one-spelling ruling, and its case names the value
the product itself used to write (`scram-sha-512`). That is deliberate: the
cheapest way for this fix to regress is for the writer at
`crates/logweir/src/backup/phase_run.rs::receipt_auth` to drift back, and this
case is what refuses the document that drift would produce. Every OTHER receipt
case — the accept-control included — carries `scramSha512`, because a corpus
whose documents all violated the newest arm would refuse each case for the
wrong reason.

### Why `arm` is the whole reason string here

In `index.json`, `arm` is a verbatim fragment of the message literal in
`Scorecard::validate_invariants`, and
`every_invariant_arm_has_a_corpus_case` joins on it by substring. The receipt's
five messages **interpolate** — a `format_version`, an exit code, two rendered
topic sets, two timestamps — so no such fragment exists. `arm` is therefore
byte-equal to `reason`, and the join is on the message **SKELETON**: the format
string with every `{…}` placeholder normalised, re-derived from BOTH readers'
source text by `scripts/check-invariant-corpus.sh`.

That gate asserts three things, and the middle one is what the scorecard corpus
cannot do:

1. every case's reason matches exactly one arm skeleton, and every skeleton is
   matched by at least one case;
2. **the two readers implement the same five arms, in the same order, with the
   same message** — so deleting an arm from ONE reader together with its corpus
   case and its pytest does not balance, because the other reader still has
   five;
3. the accept-control exists, and case ids are unique across all three indexes.

Deleting an arm from **both** readers plus its case plus its pytest is the one
edit this arithmetic cannot catch — the same limit the `occurrences: 0`
paragraph above states for `index.json` — and the answer is the same: the
per-arm Rust unit tests in
`crates/logweir-core/tests/backup_receipt.rs`, which assert each of the five
messages in FULL and which such an edit does not touch — plus
`validate_invariants_has_exactly_five_return_err_statements` in the same file,
which reads the function's own source text, so an arm deleted from both readers
and from this corpus still fails a named test.

The two-reader walk over these eight documents is
`crates/logweir/tests/two_reader_parity_receipt.rs` — its **own test binary**,
because Global Constraint 22's 15 s bound is per `#[test]` and
`two_reader_parity.rs` already measures 5–12 s.

## The three `target.auth` cases in `index.json` (Task 5b, +1 in fix round 1)

`target_auth_mode_absent_is_plaintext`, `target_auth_username_without_mode` and
`target_auth_mode_is_the_legacy_spelling` are Global Constraint 12's price for
one nested optional field: `TargetInfo.auth`, declared by Task 5b and FILLED by
Task 6.

All three are `unmodified_example.json` with a `target.auth` block added, and all
three are refused by both readers with byte-identical text. **An absent block is
legal** — it means plaintext, and every scorecard this tree has ever written has
none, which is what the other twenty-one cases keep true. What is refused is a
block whose `mode` is BLANK (ruling R-A: `trim().is_empty()` in Rust,
`.strip()` in Python), because a SCRAM run recorded as plaintext by omission is
the one claim an auditor must never have to guess at — and a `username` beside a
blank mode is a principal recorded without the mechanism it authenticated with,
which is the same defect with more evidence that it was not an accident.

The THIRD case is fix round 1's, and it closes the hole Task 5b's own review
proved by execution: a scorecard carrying `{"mode": "totally-made-up"}`
verified 0/0 at BOTH readers, because the two arms above look only at whether
the mode is blank. The accepted set is now exactly `AuthSpec`'s two serde tags,
`plaintext` and `scramSha512`, and the case names `scram-sha-512` — the spelling
this product's own receipt writer used until the same round — so the corpus
refuses the document a regression would produce rather than an invented one.

Neither message interpolates, so both `arm` fields are verbatim fragments of
`Scorecard::validate_invariants` and `every_invariant_arm_has_a_corpus_case`
joins on them exactly as it does for every other case here.

---

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
