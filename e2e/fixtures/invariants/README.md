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

Forty-four documents that neither reader reaches an invariant on. Eleven are
`unmodified_example.json` with one whole required block removed and nothing else
touched; one has two removed at once; six have one required NON-block field
removed and six carry one at the WRONG JSON TYPE; three override
`sample.records_expected` alone; six set a non-`Option` `u64` field to `null`;
two give `target.mode` a value outside its set (Task 10); four give FX-4's two
`not_assessed` fields a type serde refuses; and five do the same to
`topic_parity`'s lists, or remove one (FX-3, below).

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

## The two `target.mode` cases in `shape-index.json` (Task 10)

`target_mode_unknown_value` (`target.mode` → `"bogus"`) and `target_mode_null`
(`target.mode` → `null`) close Task 9b's re-review finding NIT-1, measured:
`drill verify` exited `1` at DESERIALISATION on both — `TargetMode` is a real
Rust enum, so `unknown variant \`bogus\`, expected \`scratch\` or \`newTopic\`` and
`expected value` respectively — while `docs/verify_scorecard.py` printed `VALID`
and exited `0`. One reader accepting what the other refuses is the disagreement
this whole directory exists to make impossible; `SCRIPT_VERSION 1.13.0` closes
the value set on the Python side.

They are shape cases and not `index.json` cases because Rust reaches no
invariant on them, and their two recorded texts DIFFER for the reason every
other entry here has two: Rust's half is `serde_json`'s own vocabulary, and
`expected value` carries nothing a second reader could honestly restate.
Reproducing it would bind this repository's output to a dependency's internal
wording and would claim this reader could not parse a document it parsed
perfectly well. **What this index has never licensed is the shape NIT-1 found:**
all of its entries record BOTH readers refusing, and the comment in
`verify_scorecard.py` that once cited it as grounds for leaving `target.mode`
unchecked has been corrected in place.

The `check` kind is `message:` — the weakest kind, and the only applicable one:
`target.mode` is a NESTED optional field, so none of the `block:`/`field:`/
`type:`/`null:` arithmetics, all of which are derived from `Scorecard`'s
top-level required fields, can supply a count for it. The per-arm protections
are therefore `docs/test_verify_scorecard.py::
test_an_unknown_target_mode_is_refused_as_rust_refuses_it` (seven bad values)
and `::test_an_absent_target_mode_is_still_scratch_and_still_accepted` (the
accept control, in both spellings and absent).

## The two `fail`-with-a-reason cases in `index.json` (Task 10)

`fail_integrity_with_a_bound_partial_reason` and
`fail_integrity_without_a_partial_reason` are ACCEPT cases, and they are Global
Constraint 12's price for phase 7's restored-count bound (guard **G-WIN**,
second half). The bound writes its failure —
`restored <n> records but the manifest bounds the window [<floor_ms>, <pit_ms>]
at [<lower>, <upper>]` — into `integrity.partial_reason` beside
`integrity.result: fail`, which is a field that already existed and a shape no
case pinned: the only `partial_reason` arm is about a `partial` result, and
`outcome: pass`'s arm is about a `pass`. Both documents are
`matrix_pass_on_a_non_pass_at_byte_fingerprint.json` with a coherent
`matrix_verdict: fail` and its reason, differing from each other by the
`partial_reason` alone — PRESENT in one, `null` in the other — so a reader that
started refusing either direction is caught. Neither adds an arm, so
`every_invariant_arm_has_a_corpus_case`'s arithmetic is untouched.

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

The two-reader walk over these twenty-nine documents (FX-4 added nine `config_coverage` cases, FX-7 the pinned one, PROD-05.1 eleven `topic_configuration` cases: two accepts — the pinned one with an un-owned topic, so the parity gate compares the admin-API route line too — and one refusal per arm 12–19, two for arm 17) is
`crates/logweir/tests/two_reader_parity_receipt.rs` — its **own test binary**,
because Global Constraint 22's 15 s bound is per `#[test]` and
`two_reader_parity.rs` already measures 5–12 s.

**Two accept cases (FX-7).** `unmodified_receipt_pinned.json` is
`unmodified_receipt.json` at `format_version` `1.2.0` — the MINOR after FX-4's
`1.1.0` — with the optional `archive.manifest_version_id` a receipt taken on a
versioned bucket carries, and with FX-4's `config_coverage` block (the one
`receipt_1_1_with_config_coverage.json` carries), because this build writes that
block on every receipt, pinned or not. Both readers must accept it, and
`scripts/check-verifier-parity.sh` asserts that both PRINT the pin, and that
both print the same coverage lines: an absent pin keeps `unmodified_receipt.json`
the byte-identical `1.0.0` document it always was, and a present one is no new
arm.

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

## PROD-01.3: the versioned auth-mode cases (receipt 1.4.0, scorecard 1.5.0)

PROD-01.3 adds `scramSha256`, `plain` and `mtls` to both auth-mode fields, as
values of a NEW minor only: receipt format 1.4.0 (after PROD-05.1's 1.3.0) for `source.auth.mode`,
scorecard format 1.5.0 for `target.auth.mode`. Each field's one closed-set arm
becomes three statements in both readers, and each statement has a case. Every
case is its file's unmodified document with exactly the overrides named.

| case | index | override | pins |
|---|---|---|---|
| `source_auth_mode_mtls_under_1_4_0` | receipt | `format_version` → `1.4.0`, mode `mtls`, no username | ACCEPT: the document an mTLS backup writes |
| `source_auth_mode_mtls_under_1_3_0` | receipt | the same under `1.3.0` | the BOUNDARY: PROD-05.1's 1.3.0 predates the new values (5b) |
| `source_auth_mode_plain_under_1_0_0` | receipt | mode `plain` under `1.0.0` | 5b: a new mode under a version that predates it, named by version |
| `source_auth_mode_outside_the_five` | receipt | `1.4.0`, mode `oauthbearer` | 5c: the closed five from 1.4.0 (OAUTHBEARER is deferred, OD-3) |
| `target_auth_mode_plain_under_1_5_0` | scorecard | `1.5.0`, mode `plain` | ACCEPT: the document a restore into a PLAIN target writes |
| `target_auth_mode_mtls_under_1_4_0` | scorecard | `1.4.0`, mode `mtls` | the BOUNDARY: PROD-08.1's 1.4.0 predates the new values |
| `target_auth_mode_scram_sha_256_under_1_0_0` | scorecard | mode `scramSha256` under `1.0.0` | a new mode under an old version; the message names the version, never the mode |
| `target_auth_mode_outside_the_five` | scorecard | `1.5.0`, mode `oauthbearer` | the closed five from 1.5.0 |

The unchanged closed-two statement keeps its Task 5b cases
(`…_is_the_legacy_spelling`), which are 1.0.0 documents and so still answer it.
An older reader refuses every new-mode document through that statement, the
safer verdict (OD-7, third case).

## FX-3: `topic_parity.not_reconstructed` (scorecard format 1.2.0)

Ten `index.json` cases for the field and its five arms, NR-1 to NR-5, which
both readers state in the same position (after `target.auth`, before
`redactions`) and words. Each is `unmodified_example.json` with exactly the
overrides that make its case; a `newTopic` one also drops `marker_topic`, sets
`target.mode` and names its targets `restore-20260903T090000Z-orders`.

| case | what it pins |
|---|---|
| `new_topic_1_2_with_not_reconstructed` | ACCEPT: the 1.2.0 `newTopic` shape phase 7 writes, the three settings in `not_reconstructed` and in `unexpected_divergence`, nothing intended |
| `scratch_1_2_with_empty_not_reconstructed` | ACCEPT: a 1.2.0 drill, intended deviations beside `not_reconstructed: []` |
| `new_topic_1_1_with_intended_labels` | ACCEPT: a `newTopic` document from before FX-3, all three intended and no field, decided exactly as before |
| `not_reconstructed_under_format_1_1_0` | NR-1: the field under `1.1.0` |
| `not_reconstructed_without_its_unexpected_twin` | NR-2: two settings dropped from `unexpected_divergence` instead of moved, which a reader older than 1.2.0 would read as silence |
| `not_reconstructed_also_intended` | NR-3: one setting also intended |
| `not_reconstructed_copied_into_intended` | ORDER: copied into `intentionally_deviated` and missing its twins, so NR-2 and NR-3 both fire; both readers report NR-2 |
| `new_topic_1_2_with_scratch_labels_beside_empty_not_reconstructed` | NR-4: the three settings intended beside `not_reconstructed: []`, what a writer that lost the mode would sign (FX-3 review F1) |
| `new_topic_1_2_decided_divergence_missing_from_not_reconstructed` | NR-5: the three settings unexpected beside `not_reconstructed: []` |
| `new_topic_1_2_other_divergence_beside_empty_not_reconstructed` | ACCEPT: a key the restore does not decide (`min.insync.replicas`) beside `not_reconstructed: []`; NR-5 reads only the four settings |

NR-1's message interpolates the document's `format_version`, so its `arm` is the
literal text before the placeholder, as the redactions arm's is. None of NR-2 to
NR-5 interpolates an entry: an entry names a topic. NR-4 and NR-5 fire only on a
`newTopic` document; `scratch_1_2_with_empty_not_reconstructed` (intended
deviations beside `[]`) is NR-4's scratch control.

Five `shape-index.json` cases (`check: message:`), because `drill verify`
refuses them at deserialisation and script 1.15.0 printed `VALID` for all five
(the two existing lists' cases measured at `b8b9263f`; the new field's it did
not know):
`topic_parity_not_reconstructed_not_an_array`,
`topic_parity_not_reconstructed_item_not_a_string`,
`topic_parity_intentionally_deviated_not_an_array`,
`topic_parity_intentionally_deviated_absent` and
`topic_parity_unexpected_divergence_item_not_a_string`. The script's shape check
for the two existing lists is what makes NR-2 and NR-3 list membership there:
over a string, Python's `in` is a substring test.

## FX-8: `source.time_basis` (scorecard format 1.3.0)

Six `index.json` cases for the block and its four arms, TB-1 to TB-4, which
both readers state in the same position (after `target.auth`, before
`redactions`) and words. Each is `unmodified_example.json` with
`format_version` and `source.time_basis` set and nothing else touched.

| case | what it pins |
|---|---|
| `time_basis_1_3_producer_time` | ACCEPT: the plan's `producerTime` and one topic selected by producer time, the shape a restore of a `LogAppendTime` topic writes under the opt-in |
| `time_basis_1_3_not_recorded` | ACCEPT: one topic selected by time with no recorded timestamp type, and no plan value |
| `time_basis_under_format_1_1_0` | TB-1: the block under `1.1.0` |
| `time_basis_plan_outside_its_set` | TB-2: `plan: appendTime` |
| `time_basis_producer_time_without_the_plan` | TB-3: a topic selected by producer time in a document whose plan did not accept it |
| `time_basis_topic_in_both_lists` | TB-4: one topic in both lists |

TB-1's message interpolates the document's `format_version`, so its `arm` is the
literal text before the placeholder, as the redactions arm's is. TB-2 to TB-4 do
not interpolate (the lists name topics); TB-2's and TB-3's messages quote
`"producerTime"`, which the Rust source spells `\"`, so their `arm`s are the
quote-free clause at the end of each message.

Four `shape-index.json` cases (`check: message:`), because `drill verify`
refuses them at deserialisation (`TimeBasisLabel`'s two lists carry no serde
default, and `plan` is an `Option<String>`): `time_basis_not_an_object`,
`time_basis_producer_time_absent`,
`time_basis_not_recorded_item_not_a_string` and
`time_basis_plan_not_a_string`.

## PROD-08.1: `integrity.verification` (scorecard format 1.4.0)

Eleven `index.json` cases for the block and its seven arms, IV-1 to IV-7, which
both readers state in the same position (after `source.time_basis`, before
`redactions`) and words. Each is `unmodified_example.json` with
`format_version` and `integrity.verification` set — and, where the case is not
a pass, `outcome`, `integrity.result`, `integrity.partial_reason` and
`engine.matrix_verdict` moved off a pass — and nothing else touched. Generated
by the worker's script (recorded in its report); every literal is in the files.

| case | what it pins |
|---|---|
| `verification_1_4_sampled` | ACCEPT: a sampled verification, header order not verified, one capture gap |
| `verification_1_4_complete_pass` | ACCEPT: a covered, exact complete verification over two partitions, signed `pass` |
| `verification_1_4_complete_incomplete_partial` | ACCEPT: a complete verification its bound stopped, signed `partial` with the reason |
| `verification_1_4_complete_duplicate_fail` | ACCEPT: a complete verification that counted a duplicate, signed `fail` |
| `verification_under_format_1_3_0` | IV-1: the block under `1.3.0` |
| `verification_coverage_outside_its_set` | IV-2: `coverage: full` |
| `verification_sampled_claims_header_order` | IV-3: header order `verified` beside sampled coverage |
| `verification_complete_coverage_without_its_block` | IV-4: `coverage: complete` with no `complete` block |
| `verification_incomplete_without_a_reason` | IV-5: `covered: false` with no reason |
| `verification_pass_over_a_missing_record` | IV-6: a `pass` beside a complete block that counts a missing record |
| `verification_totals_not_the_partitions_sums` | IV-7: a total that is not its partitions' sum |

Since PROD-08.1's review (M-1), IV-6 and IV-7 also carry one case per
CONJUNCT, each violating exactly that conjunct, so a reader that drops one
answers with a later arm's words or `VALID` and the walker fails it:
`verification_iv6_*` (not covered, no partition, a failed segment, an
unverified segment, segments not all verified, each total fault —
`missing`, `unexpected`, `duplicates`, `out_of_order`, `mismatched`, matching
short, restored over — a partition not compared, and partitions inexact with
exact totals) and `verification_iv7_*` (each of the eight replay sums, the
segment, verified-segment, decoded-record and offset-hole sums, and the
accounted segments). Two more pin review L-1: a reason made of a unit
separator (U+001F) is not blank to either reader —
`verification_incomplete_reason_a_unit_separator` (ACCEPT) and
`verification_covered_with_a_unit_separator_reason` (IV-5).

IV-1's message interpolates the document's `format_version`, so its `arm` is the
literal text before the placeholder. IV-2, IV-3 and IV-4 quote values the Rust
source spells with `\"`, so their `arm`s are the quote-free text before the
first quote (IV-2, IV-4) or the clause after the last (IV-3).

Twenty-nine `shape-index.json` cases. Twenty-four are `null:` cases, one per
plain `u64` count of the block — `complete.archive.*`, `complete.replay.*`,
`complete.partitions[].*` and `complete.partitions[].replay.*` — which the
closed u64 arithmetic requires once both walkers go below a block's first level
(PROD-08.1 made them depth-first: `ReplayComparison` is reached at two paths).
Five are `message:` cases, because `drill verify` refuses them at
deserialisation: `verification_not_an_object`,
`verification_coverage_not_a_string`, `verification_gaps_absent`,
`verification_covered_not_a_bool` and `verification_partitions_not_an_array`.

## FX-23: `sample.unsampled_topics` (scorecard format 1.6.0)

Three arms, US-1 to US-3, which both readers state in the same position (after
`integrity.verification`, before `redactions`). Every case is
`verification_1_4_sampled.json` (or, for US-3,
`verification_1_4_complete_pass.json`) with `format_version` and
`sample.unsampled_topics` set and nothing else touched.

| case | what it pins |
|---|---|
| `unsampled_topics_1_6_sampled` | ACCEPT: two topics `max_partitions` left unsampled, sorted, under 1.6.0, beside a sampled verification |
| `unsampled_topics_1_6_none_named` | ACCEPT: a 1.6.0 sampled document with no field — what this build writes for every sampled drill whose sample reached every topic |
| `unsampled_topics_under_format_1_5_0` | US-1: the field under `1.5.0` |
| `unsampled_topics_empty` | US-2: `[]` (absent is the spelling of none) |
| `unsampled_topics_unordered` | US-2: two topics out of order |
| `unsampled_topics_repeated` | US-2: one topic twice |
| `unsampled_topics_blank` | US-2: a name made of U+2003, blank to both readers |
| `unsampled_topics_beside_complete` | US-3: the field beside a complete verification |

US-1's message interpolates the document's `format_version`, so its `arm` is the
literal text before the placeholder; US-3's quotes `"complete"`, so its `arm` is
the text before the quote. One `shape-index.json` case,
`unsampled_topics_not_an_array` (`"orders"`), is a `message:` case: `drill
verify` refuses it at deserialisation.

## PROD-11.1: `source.selection` (scorecard format 1.7.0)

Three arms, SEL-1 to SEL-3, which both readers state in the same position
(after `sample.unsampled_topics`, before `redactions`). A format-1 block is a
stated window START only (PS-2 below; partition subsets are format 2.0.0).
Every case is `verification_1_4_sampled.json` (or, for
SEL-3, `verification_1_4_complete_pass.json`) with `format_version` and
`source.selection` set (and, for the complete cases, the complete block's
`window`) and nothing else touched.

| case | what it pins |
|---|---|
| `selection_1_7_sampled` | ACCEPT: a start under 1.7.0, beside a sampled verification |
| `selection_1_7_complete_pass` | ACCEPT: a complete pass whose window is the selection's |
| `selection_under_format_1_6_0` | SEL-1: the block under `1.6.0` |
| `selection_start_at_its_end` | SEL-2: the start equal to the end |
| `selection_complete_window_is_not_its_own` | SEL-3: a complete block whose window has no start beside a selection that states one |

SEL-1's message interpolates the document's `format_version`, so its `arm` is
the literal text before the placeholder; every other `arm` is the text of the
arm's first source line. One `shape-index.json` case, `selection_not_an_object`
(`"orders"`), is a `message:` case: `drill verify` refuses it at
deserialisation. The other bad shapes (a missing or non-integer end, a
non-integer start, a value outside `i64`, a malformed subset list or engine-run
count) are pinned by
`docs/test_verify_scorecard.py::test_the_selection_shape_is_refused_before_its_arms`.

## PROD-11.1b: partition subsets (scorecard format 2.0.0, OD-9 (a))

A restore that states a partition subset signs format **2.0.0**, the format's
first MAJOR: 1.7.0's fields with `source.selection.partitions` and
`engine_runs` required, and `complete.partitions[]` and the sampled lane's
fields naming the SELECTED partitions. Every reader before 1.27.0 refuses such
a document as an unsupported major. Five arms: PS-1 (major 2 is read only for
a document carrying a subset) is `Scorecard::refuse_unreadable_major`'s, which
the arithmetic below does not count (it slices `validate_invariants` only), so
its two-reader row is `scripts/check-verifier-parity.sh`'s subset loop; PS-2
to PS-5 are `validate_invariants` statements, after SEL-1 to SEL-3 in this
order: SEL-1, PS-2, SEL-2, SEL-3, PS-3, PS-4, PS-5. The sampled cases start
from `verification_1_4_sampled.json`, the complete ones from
`verification_1_4_complete_pass.json` (its partitions `orders/0` and
`orders/1`, both expecting records), with `format_version` and
`source.selection` set and, for a start, the complete block's
`window.start_ms`.

| case | what it pins |
|---|---|
| `subsets_2_0_sampled` | ACCEPT: `orders` [0, 2] from the archive's floor, one run, a sampled verification |
| `subsets_2_0_complete_pass` | ACCEPT: `orders` [0, 1] from a start, a complete pass over its window |
| `subsets_2_0_complete_mixed_pass` | ACCEPT: `orders` [0] narrowed and `payments` restored WHOLE (no subset, every partition selected), `engine_runs` 2, a complete pass: PS-5 reads a topic without a subset as selected (review L2) |
| `subsets_under_format_1_7_0` | PS-2: a subset under 1.7.0, which an older reader would read as every partition |
| `selection_1_7_without_a_start` | PS-2: a 1.7.0 block with no start |
| `subsets_list_unsorted` | PS-3: `[2, 0]` |
| `subsets_engine_runs_short` | PS-4: two distinct subsets restored by one run |
| `subsets_complete_expects_an_unselected_partition` | PS-5: `orders` [0] beside a complete block expecting records from `orders/1` |

One `shape-index.json` case, `subsets_engine_runs_negative` (`engine_runs:
-1`), is a `message:` case: `drill verify` refuses it at deserialisation (a
`u32`).

---

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.

## PROD-04.1: `consumer_positions` (receipt 1.7.0) and `consumer-positions-index.json`

A 1.7.0 receipt carries a bounded SUMMARY of the selected consumer groups and
binds, by SHA-256 and length, a positions document beside it. So the corpus is
two-sided:

- **`backup-receipt-index.json`**, ids `receipt_1_7_with_consumer_positions`
  (the accept-control) and `consumer_positions_*`: one refusing case per
  receipt arm 30–35, and for arm 34 one per clause of its captured branch
  (`active`, `members`, `listed_state`, `counts` missing, and a group described
  `Dead` with no member recorded as captured).
- **`consumer-positions-index.json`**: a receipt (`receipt`) and the positions
  document (`document`) both readers are handed with `--consumer-positions`,
  one refusing case per document arm CP-1 to CP-14 and one accept-control. Its
  entries carry SEVEN fields — `id`, `receipt`, `document`, `rust_exit`,
  `python_exit`, `reason`, `arm` — and `scripts/check-invariant-corpus.sh`
  closes the same arithmetic over
  `BackupReceipt::validate_consumer_positions_document` and
  `check_consumer_positions_document` that it closes over the receipt's arms.
  The two-reader half is
  `crates/logweir/tests/two_reader_parity_receipt.rs::two_reader_parity_over_the_positions_document_corpus`
  and `scripts/check-verifier-parity.sh`'s positions loop.

**These cases are GENERATED** by `scripts/fixtures/consumer_positions_corpus.py`
(idempotent; rerun after changing a case or an arm's message). A document case
needs a receipt that binds THAT document's exact bytes — otherwise it tests
CP-2 instead of the arm it names — so each one is the accepted receipt and the
accepted document with one change, rebound. The recorded `reason` is the
Python reader's; the Rust reader is held to it by the two-reader walk, and
every Rust message by `crates/logweir-core/tests/backup_receipt.rs`.
