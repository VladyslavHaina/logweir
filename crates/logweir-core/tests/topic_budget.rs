//! **FX-33 — the topic budget is held by rows that exist.**
//!
//! `logweir_core::topic_budget` states numbers; rows in the runner's and the
//! controller's crates measure them. This row is here, in the pure crate's
//! integration tests and not beside the constants, because it reads source
//! files and `logweir-core/src` names no filesystem API, a test module
//! included (`scripts/check-pure-core.sh`).

/// **The budget is held by rows that exist.** The numbers in
/// `logweir_core::topic_budget` are true only because rows in two other
/// crates measure them: one
/// topic's cost against its budget, the projection against the receipt's
/// schema, the two refusals, the controller's cap. A row deleted there
/// would leave every constant there unguarded and nothing else failing, so
/// each is named here and must be a `#[test]`.
///
/// KILLS: the per-topic budget test removed.
#[test]
fn the_budget_is_held_by_rows_that_exist() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("crates/logweir-core has a grandparent");
    for (file, rows) in [
        (
            "crates/logweir/tests/topic_budget.rs",
            &[
                "one_topic_costs_no_more_than_its_budget",
                "the_acceptance_sizes_fit_their_bounds_and_five_thousand_does_not",
                "no_real_receipt_is_larger_than_its_projection",
                "the_projection_carries_every_field_the_receipt_defines",
                "a_selection_over_the_maximum_is_refused_before_any_client_is_used",
                "a_receipt_that_would_be_over_the_bound_is_refused_before_the_engine",
                "the_controllers_reference_sidecars_are_what_the_signer_writes",
                "the_documentation_states_the_budget_in_the_constants_own_numbers",
            ][..],
        ),
        (
            "crates/weirkeeper/tests/topic_budget.rs",
            &[
                "the_controllers_document_cap_is_one_of_its_two_rows",
                "a_backup_of_many_topics_is_verified_by_the_controller",
                "the_largest_receipt_fits_the_relay",
                "the_controller_builds_no_tree_of_a_receipt_on_any_path",
                "a_receipt_read_and_a_receipt_relay_stay_bounded",
            ][..],
        ),
    ] {
        let text = std::fs::read_to_string(root.join(file))
            .unwrap_or_else(|e| panic!("{file} is checked in: {e}"));
        for row in rows {
            let as_a_test = format!("#[test]\nfn {row}()");
            assert!(
                text.contains(&as_a_test),
                "{file} no longer holds the row `{row}` as a test. The topic budget's \
                 constants are measurements that row makes; put it back, or change the \
                 budget and this list together"
            );
        }
    }
}
