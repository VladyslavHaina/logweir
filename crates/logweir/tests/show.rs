//! `logweir drill show` — the fixed-width table is the README image (spec
//! §13), so it is golden-tested rather than asserted field by field. The
//! three tests below are exactly task-21b-brief.md Step 1, verbatim.
//!
//! The tests after that are this task's own self-review coverage: the brief
//! and its addendum freeze the golden and these three tests, but the task
//! also demands that a reader can never mistake a non-passing drill, or a
//! `Partial` integrity result, for a clean pass. Those properties hold in the
//! given renderer today (every enum is Debug-printed verbatim, never
//! hardcoded), but nothing pinned them down before this file. Each test names
//! the exact guarantee it protects and is written so that reverting the
//! guarantee (e.g. hardcoding "Pass") fails it at assertion time, not merely
//! at compile time — see task-21b-report.md's mutant table for the mutations
//! actually applied to confirm this.
mod fixtures;

use logweir_core::outcome::{IntegrityLevel, IntegrityResult, Outcome};

#[test]
fn the_table_matches_the_golden_that_the_readme_pastes() {
    insta::assert_snapshot!(
        "drill_show_table",
        logweir::show::render_table(&fixtures::scorecard_pass())
    );
}

#[test]
fn a_self_attested_scorecard_is_labelled_in_the_table() {
    let mut sc = fixtures::scorecard_pass();
    sc.approval.self_attested = true;
    assert!(logweir::show::render_table(&sc).contains("SELF-ATTESTED"));
}

#[test]
fn the_compared_rto_is_the_starred_one() {
    let t = logweir::show::render_table(&fixtures::scorecard_pass());
    let line = t
        .lines()
        .find(|l| l.contains("excluding preflight"))
        .unwrap();
    assert!(
        line.trim_start().starts_with('*'),
        "the value compared against the objective must be the starred row: {line}"
    );
}

// ---------------------------------------------------------------- self-review

/// Find the one line in the table whose label (after the leading `  ` or
/// `* `) is `label` — never a substring match, so this cannot accidentally
/// hit the wrong row (e.g. "rto ..." rows all contain "rto").
fn row_line<'a>(table: &'a str, label: &str) -> &'a str {
    table
        .lines()
        .find(|l| {
            l.trim_start()
                .trim_start_matches('*')
                .trim_start()
                .starts_with(label)
        })
        .unwrap_or_else(|| panic!("no row for {label:?} in:\n{table}"))
}

/// Guarantee: a drill that did not pass must read unmistakably as not
/// passing. `render_table` never branches on `outcome`, it prints the real
/// enum's WIRE spelling, so a scorecard whose top-level outcome is
/// `fail-integrity` renders exactly that on the `outcome` row and never
/// `pass`.
///
/// The wire spelling, not Rust `Debug`: this row used to read `FailIntegrity`
/// while the signed JSON said `fail-integrity` and `drill run`'s stdout line
/// said `fail-integrity` — three spellings of one value out of one binary,
/// with the table (the surface a human reads INSTEAD of the JSON) the odd one
/// out.
#[test]
fn a_non_pass_outcome_never_renders_as_pass() {
    let mut sc = fixtures::scorecard_pass();
    sc.outcome = Outcome::FailIntegrity;
    let table = logweir::show::render_table(&sc);
    let line = row_line(&table, "outcome");
    assert!(
        line.contains("fail-integrity"),
        "the outcome row must name the real outcome, in the spelling the signed \
         document uses: {line}"
    );
    assert!(
        !line.contains("pass"),
        "a failing outcome must never render as pass: {line}"
    );
}

/// Same guarantee for `preflight-failed`, the outcome a refused drill records.
#[test]
fn a_preflight_failed_outcome_never_renders_as_pass() {
    let mut sc = fixtures::scorecard_pass();
    sc.outcome = Outcome::PreflightFailed;
    let table = logweir::show::render_table(&sc);
    let line = row_line(&table, "outcome");
    assert!(line.contains("preflight-failed"), "{line}");
    assert!(!line.contains("pass"), "{line}");
}

/// Guarantee: a `Partial` integrity result must not render like a `Pass`.
/// The `integrity` row prints `result`'s wire spelling next to `level`'s, so
/// `partial` and `pass` produce visibly different rows.
#[test]
fn a_partial_integrity_result_does_not_render_as_pass() {
    let mut sc = fixtures::scorecard_pass();
    sc.integrity.result = IntegrityResult::Partial;
    sc.integrity.partial_reason = Some("compacted topic: fewer records on target".into());
    let table = logweir::show::render_table(&sc);
    let line = row_line(&table, "integrity");
    assert!(
        line.contains("partial"),
        "a partial integrity result must be visible: {line}"
    );
    assert!(
        !line.contains("/pass"),
        "a partial result must never render as the pass result: {line}"
    );
}

/// Guarantee: anything the scorecard marks as not-attempted must be visible,
/// not dropped for tidiness. `IntegrityLevel::NotAttempted` is printed in the
/// same `integrity` row as `byte-fingerprint`/`consume-only` — it is never
/// special-cased away.
#[test]
fn a_not_attempted_integrity_level_is_visible() {
    let mut sc = fixtures::scorecard_pass();
    sc.integrity.level = IntegrityLevel::NotAttempted;
    let table = logweir::show::render_table(&sc);
    let line = row_line(&table, "integrity");
    assert!(
        line.contains("not-attempted"),
        "a not-attempted integrity level must not be dropped for tidiness: {line}"
    );
}

/// Guarantee: the four RTO figures and the RPO figure each render under their
/// own row, at the value the scorecard actually carries, suffixed `s`
/// (seconds) — not scaled, not swapped with a sibling field, not silently
/// dropped. `crates/logweir/tests/fixtures/mod.rs`'s `scorecard_pass` is
/// parsed from `e2e/fixtures/scorecard-pass.json`, whose five figures
/// (542, 512, 214, 300, 0) are all distinct, so a mutant that swaps any two
/// of these rows' fields is caught by exactly one of the five assertions
/// below, not merely by drift in the golden.
#[test]
fn the_five_timing_figures_render_under_their_own_row_in_seconds() {
    let t = logweir::show::render_table(&fixtures::scorecard_pass());
    assert!(
        row_line(&t, "rto requested").contains("542s"),
        "rto_requested_to_verified_seconds (542) must appear under its own row: {}",
        row_line(&t, "rto requested")
    );
    assert!(
        row_line(&t, "rto approval").contains("512s"),
        "rto_seconds (512) must appear under its own row: {}",
        row_line(&t, "rto approval")
    );
    assert!(
        row_line(&t, "rto restore only").contains("214s"),
        "rto_restore_only_seconds (214) must appear under its own row: {}",
        row_line(&t, "rto restore only")
    );
    let starred = row_line(&t, "rto excluding preflight");
    assert!(
        starred.contains("300s"),
        "rto_excluding_preflight_seconds (300) must appear on the starred row: {starred}"
    );
    let rpo = row_line(&t, "rpo");
    assert!(
        rpo.contains("0s"),
        "rpo_seconds (0) must appear under its own row: {rpo}"
    );
}

/// Guarantee: `rpo_seconds` renders with its real sign — a non-zero value
/// must appear verbatim, not silently made absolute, not scaled (e.g.
/// mistaken for milliseconds). `Scorecard::validate_invariants` refuses a
/// negative `rpo_seconds` before a document is ever signed, so this checks
/// the reachable case: a positive gap prints as that exact number, not 0 and
/// not some other magnitude.
#[test]
fn rpo_seconds_renders_the_exact_measured_value() {
    let mut sc = fixtures::scorecard_pass();
    sc.measured.rpo_seconds = Some(37);
    let table = logweir::show::render_table(&sc);
    let rpo = row_line(&table, "rpo");
    assert!(
        rpo.contains("37s"),
        "rpo_seconds must render its exact value, not a scaled or rounded one: {rpo}"
    );
}

/// Guarantee: a null timing figure is rendered as an explicit placeholder
/// (`opt`'s "—"), never as an empty string or a bare zero that a reader could
/// mistake for a measured `0`.
#[test]
fn an_unmeasured_timing_figure_renders_as_a_placeholder_not_zero() {
    let mut sc = fixtures::scorecard_pass();
    sc.measured.rto_seconds = None;
    let table = logweir::show::render_table(&sc);
    let line = row_line(&table, "rto approval");
    assert!(
        !line.contains("0s"),
        "a null rto_seconds must not render as a measured 0: {line}"
    );
    assert!(
        line.contains('\u{2014}'),
        "a null rto_seconds must render as the explicit placeholder: {line}"
    );
}

// ------------------------------------ Task 22, carried obligation 3: the
// qualifiers footer. The fourteen frozen rows cite `objectives.rto_seconds`
// and never show it, and drop `integrity.partial_reason` and
// `engine_subreport.caveat` entirely. A footer below the table now carries
// all three. These tests pin each one; a mutant that deletes the footer, or
// that renders a tri-state `met` as a boolean, fails one of them at assertion
// time rather than only drifting the golden.

/// Find the one footer line whose label is `label`, searching only BELOW the
/// fourteen frozen rows so this can never accidentally read a table row.
fn footer_line<'a>(table: &'a str, label: &str) -> &'a str {
    table
        .lines()
        .skip_while(|l| !l.contains("objectives (from the approved plan)"))
        .find(|l| l.trim_start().starts_with(label))
        .unwrap_or_else(|| panic!("no footer line for {label:?} in:\n{table}"))
}

/// Guarantee: the value the starred row says it is compared against is
/// actually displayed. Before this footer existed, `objectives.rto_seconds`
/// appeared in the table only as the NAME of a field the reader could not see.
#[test]
fn the_objective_the_starred_row_is_compared_against_is_displayed() {
    let sc = fixtures::scorecard_pass();
    let want = sc
        .objectives
        .rto_seconds
        .expect("fixture sets an rto objective");
    let table = logweir::show::render_table(&sc);
    let line = footer_line(&table, "rto_seconds");
    assert!(
        line.contains(&want.to_string()),
        "objectives.rto_seconds ({want}) must be displayed, not merely cited: {line}"
    );
}

/// Guarantee: a MISSED set of objectives never renders like a met one.
#[test]
fn a_missed_objective_never_renders_as_met() {
    let mut sc = fixtures::scorecard_pass();
    sc.objectives.met = Some(false);
    let table = logweir::show::render_table(&sc);
    let line = footer_line(&table, "met");
    assert!(
        line.contains("NO"),
        "objectives.met == false must be visible as a miss: {line}"
    );
    assert!(
        !line.contains("yes"),
        "a missed objective must never render as met: {line}"
    );
}

/// Guarantee: `objectives.met` is a TRI-state. `null` means "a pass_rate was
/// asked for and could not be measured" — it is not a met objective and must
/// not be collapsed into one, nor into a miss.
#[test]
fn an_unmeasurable_objective_renders_as_neither_met_nor_missed() {
    let mut sc = fixtures::scorecard_pass();
    sc.objectives.met = None;
    let table = logweir::show::render_table(&sc);
    let line = footer_line(&table, "met");
    assert!(
        line.contains("unmeasurable"),
        "a null objectives.met must say so: {line}"
    );
    assert!(
        !line.contains("yes") && !line.contains("NO"),
        "an unmeasurable verdict must not be collapsed into met or missed: {line}"
    );
}

/// Guarantee: `integrity.result: partial` carries its reason into the rendered
/// output. `verify_scorecard.py` refuses a `partial` with no reason, so a
/// reader who sees `Partial` in the table can always see WHY here.
#[test]
fn a_partial_integrity_result_shows_its_reason_in_the_footer() {
    let mut sc = fixtures::scorecard_pass();
    sc.integrity.result = IntegrityResult::Partial;
    sc.integrity.partial_reason = Some("only 2 of 3 partitions reached a conclusion".into());
    let table = logweir::show::render_table(&sc);
    let line = footer_line(&table, "integrity.partial_reason");
    assert!(
        line.contains("only 2 of 3 partitions reached a conclusion"),
        "the partial reason must be rendered verbatim: {line}"
    );
}

/// Guarantee: the engine sub-report's own caveat — which in this repository's
/// fixture states the sub-report "corroborates nothing Logweir claims" — is
/// shown, verbatim, rather than left in the JSON for a reader who will not
/// open it.
#[test]
fn the_engine_subreport_caveat_is_rendered_verbatim() {
    let sc = fixtures::scorecard_pass();
    let caveat = sc
        .engine_subreport
        .as_ref()
        .expect("the fixture carries a populated sub-report")
        .caveat
        .clone();
    let table = logweir::show::render_table(&sc);
    let line = footer_line(&table, "engine_subreport.caveat");
    assert!(
        line.contains(&caveat),
        "the engine caveat must be rendered verbatim: {line}"
    );
}

/// Guarantee: `engine_subreport: null` — which is what EVERY scorecard v0.1
/// actually produces — is not rendered as silence. Silence reads as "nothing
/// to report"; the field means "no sub-report was retained".
#[test]
fn a_null_engine_subreport_is_named_rather_than_left_blank() {
    let mut sc = fixtures::scorecard_pass();
    sc.engine_subreport = None;
    let table = logweir::show::render_table(&sc);
    let line = footer_line(&table, "engine_subreport");
    assert!(
        line.contains("null") && line.contains("no engine sub-report was retained"),
        "a null sub-report must be named, not omitted: {line}"
    );
    assert!(
        line.contains("NOT"),
        "the reading a null invites — \"the engine reported nothing wrong\" — must be \
         refused in the output: {line}"
    );
}

/// Guarantee: the output says plainly that it is a summary and where the full
/// document is. This is the sentence the carried obligation asks for; a mutant
/// that drops it fails here.
#[test]
fn the_table_says_it_is_a_summary_of_a_signed_document() {
    let t = logweir::show::render_table(&fixtures::scorecard_pass());
    assert!(
        t.contains("SUMMARY of a signed document"),
        "the table must not present itself as the document:\n{t}"
    );
    assert!(
        t.contains("docs/verify-a-scorecard.md"),
        "the table must point at the document that lists what it omits:\n{t}"
    );
}
