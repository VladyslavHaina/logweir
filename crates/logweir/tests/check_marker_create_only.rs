//! RECEIPT-DUP review F3: the destination readiness write probe proves the
//! evidence store ENFORCES conditional create, before the first backup.
//!
//! The backup runner's execution claim is a lock only on a store that honours
//! `If-None-Match: *`; on any other store every backup exits 4
//! `ExecutionClaimUnproven` before its engine. `put_marker` therefore creates a
//! fresh marker twice and requires the second create to be refused, so such a
//! destination is `notReady / ConditionalCreateUnsupported` at readiness time.
//!
//! Driven against `Store`'s own `ObjectAccess` implementation and the three
//! in-memory doubles the runner's claim rows use, so the probe and the claim
//! are measured against the same store behaviours. No network.
use logweir::check::store::{marker_key, put_marker, MarkerOutcome};
use logweir_core::check_contract::CheckCode;
use logweir_engine_oso::storage::Store;

const UID: &str = "3f1c9d2e-8a7b-4c6d-9e0f-1a2b3c4d5e6f";

#[test]
fn an_enforcing_store_is_written_then_already_present() {
    let store = Store::in_memory("logweir/");
    assert_eq!(put_marker(&store, UID).unwrap(), MarkerOutcome::Written);
    assert!(store.get(&marker_key(UID)).is_ok());
    // The next probe: the precondition refused, so the grant AND the
    // enforcement are both proven.
    assert_eq!(
        put_marker(&store, UID).unwrap(),
        MarkerOutcome::AlreadyPresent
    );
}

#[test]
fn a_store_that_ignores_if_none_match_is_not_ready() {
    let store = Store::in_memory_ignoring_conditional_put("logweir/");
    let failure = put_marker(&store, UID).expect_err("a second create that succeeds is refused");
    assert_eq!(failure.code, CheckCode::ConditionalCreateUnsupported);
    assert!(
        failure
            .message
            .contains("a second create of the same key SUCCEEDED"),
        "{}",
        failure.message
    );
    // And on every later probe too: the marker is there, yet the store
    // accepts a create over it.
    let again = put_marker(&store, UID).expect_err("still refused");
    assert_eq!(again.code, CheckCode::ConditionalCreateUnsupported);
}

#[test]
fn a_store_without_conditional_put_is_not_ready() {
    let store = Store::in_memory_without_conditional_put("logweir/");
    let failure = put_marker(&store, UID).expect_err("HEAD-then-PUT is not a lock");
    assert_eq!(failure.code, CheckCode::ConditionalCreateUnsupported);
    assert!(
        failure.message.contains("HEAD-then-PUT"),
        "{}",
        failure.message
    );
}

#[test]
fn an_error_on_the_second_create_is_not_proof() {
    let store = Store::in_memory_erroring_on_existing_key("logweir/");
    let failure = put_marker(&store, UID).expect_err("an erroring probe proves nothing");
    assert_ne!(
        failure.code,
        CheckCode::MarkerWritten,
        "an error is never read as the refusal that proves enforcement"
    );
    assert!(
        failure.message.contains("second create"),
        "{}",
        failure.message
    );
}
