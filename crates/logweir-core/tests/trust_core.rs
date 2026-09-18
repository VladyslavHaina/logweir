//! Decision D3 §7.4's table, **every row**, plus the two questions it is the
//! answer to one half of.
//!
//! # Why this file is a table test and not thirty assertions
//!
//! §7.4 IS a table. A per-row test function can pass while the row above it
//! silently stopped being reachable — the property is the whole mapping, and a
//! mapping is only asserted by enumerating it. [`the_whole_table`] walks every
//! row with its own name; the tests around it assert the things a table cannot
//! say: the boundary instants, the fail-closed rules, and the four mutants
//! this module is the natural home of.
//!
//! # Nothing here reads a clock
//!
//! `NOW` is a constant. That is not a testing convenience — it is the property
//! `scripts/check-pure-core.sh` enforces on the module under test, and
//! [`the_verdict_is_a_function_of_its_arguments_and_nothing_else`] is the
//! mutant that would die if `decide` ever grew a `Utc::now()`.

use chrono::{DateTime, Duration, Utc};
use logweir_core::rehearsal_scope::{RehearsalScope, MODE_SCRATCH};
use logweir_core::trust::{
    claimed_signing_time, decide, effective_state, may_sign_new, read_claimed_signing_time,
    usable_for_new_signatures, usable_for_verification, ClaimAbsence, EffectiveState,
    EvidenceClaim, IndependentObservation, KeyState, KeyUsage, RevocationReason, SigningRefusal,
    TrustBasis, TrustKeyState, TrustResult, TrustedKey, UntrustReason, VerificationUse,
};
use serde_json::json;

/// The one instant every test in this file is written against.
fn now() -> DateTime<Utc> {
    at("2026-09-16T12:00:00Z")
}

/// An RFC 3339 instant.
fn at(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s)
        .expect("a fixture instant")
        .with_timezone(&Utc)
}

/// An `Active` `EvidenceSigning` key valid for all of 2026.
fn active() -> TrustedKey {
    TrustedKey {
        key_id: "a".repeat(64),
        principal_id: "install:fixture".to_string(),
        usages: vec![KeyUsage::EvidenceSigning],
        not_before: at("2026-01-01T00:00:00Z"),
        not_after: at("2027-01-01T00:00:00Z"),
        state: KeyState::Active,
        retired_at: None,
        revoked_at: None,
        revocation_reason: None,
        revocation_effective_from: None,
    }
}

/// The same key, `Active`, whose window closed in March.
fn expired() -> TrustedKey {
    TrustedKey {
        not_after: at("2026-03-01T00:00:00Z"),
        ..active()
    }
}

/// The same key, retired in March, window still open.
fn retired() -> TrustedKey {
    TrustedKey {
        state: KeyState::Retired,
        retired_at: Some(at("2026-03-01T00:00:00Z")),
        ..active()
    }
}

/// The same key with a window that has not opened yet — the successor staged
/// at §7.6 step 1, before the cutover.
fn not_yet_valid() -> TrustedKey {
    TrustedKey {
        not_before: at("2027-01-01T00:00:00Z"),
        not_after: at("2028-01-01T00:00:00Z"),
        ..active()
    }
}

/// The same key, revoked in March for `reason`.
fn revoked(reason: RevocationReason) -> TrustedKey {
    TrustedKey {
        state: KeyState::Revoked,
        retired_at: None,
        revoked_at: Some(at("2026-03-01T00:00:00Z")),
        revocation_reason: Some(reason),
        revocation_effective_from: Some(at("2026-03-01T00:00:00Z")),
        ..active()
    }
}

/// `decide` for one claim, with no independent observation.
fn verdict_at(key: &TrustedKey, signed: &str) -> logweir_core::trust::Verdict {
    decide(
        Some(key),
        KeyUsage::EvidenceSigning,
        &EvidenceClaim::at(at(signed)),
        &IndependentObservation::none(),
        now(),
    )
}

// ---------------------------------------------------------------------------
// D3 §7.4's table, every row
// ---------------------------------------------------------------------------

/// **THE TABLE.** Each row is `(name, verdict, expected result, expected basis,
/// expected reason)`, and the row's name is what an assertion failure prints —
/// so a broken row says WHICH row rather than "assertion failed at line 140".
#[test]
fn the_whole_table() {
    let compromised = revoked(RevocationReason::KeyCompromise);
    let observed = decide(
        Some(&compromised),
        KeyUsage::EvidenceSigning,
        // The document's own claim is deliberately LATE, to prove the verdict
        // did not come from it.
        &EvidenceClaim::at(at("2026-06-01T00:00:00Z")),
        &IndependentObservation::at(at("2026-02-01T00:00:00Z")),
        now(),
    );
    let unobserved = decide(
        Some(&compromised),
        KeyUsage::EvidenceSigning,
        &EvidenceClaim::at(at("2026-02-01T00:00:00Z")),
        &IndependentObservation::none(),
        now(),
    );
    let absent = decide(
        None,
        KeyUsage::EvidenceSigning,
        &EvidenceClaim::at(at("2026-02-01T00:00:00Z")),
        &IndependentObservation::none(),
        now(),
    );
    let wrong_usage = decide(
        Some(&active()),
        KeyUsage::GovernedApproval,
        &EvidenceClaim::at(at("2026-02-01T00:00:00Z")),
        &IndependentObservation::none(),
        now(),
    );

    let rows: Vec<(
        &str,
        logweir_core::trust::Verdict,
        TrustResult,
        TrustBasis,
        Option<UntrustReason>,
    )> = vec![
        (
            "Active, signed inside validity",
            verdict_at(&active(), "2026-06-01T00:00:00Z"),
            TrustResult::Valid,
            TrustBasis::Current,
            None,
        ),
        (
            "Expired, signed at or before notAfter",
            verdict_at(&expired(), "2026-02-01T00:00:00Z"),
            TrustResult::Valid,
            TrustBasis::Historical,
            None,
        ),
        (
            "Retired, signed at or before retiredAt",
            verdict_at(&retired(), "2026-02-01T00:00:00Z"),
            TrustResult::Valid,
            TrustBasis::Historical,
            None,
        ),
        (
            "Expired, claiming a later signing time",
            verdict_at(&expired(), "2026-06-01T00:00:00Z"),
            TrustResult::Untrusted,
            TrustBasis::None,
            Some(UntrustReason::SignedOutsideValidity),
        ),
        (
            "Retired, claiming a later signing time",
            verdict_at(&retired(), "2026-06-01T00:00:00Z"),
            TrustResult::Untrusted,
            TrustBasis::None,
            Some(UntrustReason::SignedOutsideValidity),
        ),
        (
            "Revoked/Superseded, signed before revocationEffectiveFrom",
            verdict_at(
                &revoked(RevocationReason::Superseded),
                "2026-02-01T00:00:00Z",
            ),
            TrustResult::Valid,
            TrustBasis::Historical,
            None,
        ),
        (
            "Revoked/Unspecified, signed before revocationEffectiveFrom",
            verdict_at(
                &revoked(RevocationReason::Unspecified),
                "2026-02-01T00:00:00Z",
            ),
            TrustResult::Valid,
            TrustBasis::Historical,
            None,
        ),
        (
            "Revoked/Superseded, signed after revocationEffectiveFrom",
            verdict_at(
                &revoked(RevocationReason::Superseded),
                "2026-06-01T00:00:00Z",
            ),
            TrustResult::Untrusted,
            TrustBasis::None,
            Some(UntrustReason::SignedOutsideValidity),
        ),
        (
            "Revoked/KeyCompromise, independent observation before revocation",
            observed,
            TrustResult::Untrusted,
            TrustBasis::RecordedBeforeRevocation,
            Some(UntrustReason::RecordedBeforeRevocation),
        ),
        (
            "Revoked/KeyCompromise, no independent observation",
            unobserved,
            TrustResult::Untrusted,
            TrustBasis::None,
            Some(UntrustReason::Revoked),
        ),
        (
            "key absent from the policy",
            absent,
            TrustResult::Untrusted,
            TrustBasis::None,
            Some(UntrustReason::UntrustedSigner),
        ),
        (
            "usage mismatch",
            wrong_usage,
            TrustResult::Untrusted,
            TrustBasis::None,
            Some(UntrustReason::KeyUsageMismatch),
        ),
        // The two rows the W1 review added. Neither is in §7.4's printed
        // table; both are the fail-closed reading of it, and both were
        // FAIL-OPEN before review findings F3 and F4.
        (
            "NotYetValid, claiming a time inside the unopened window",
            decide(
                Some(&not_yet_valid()),
                KeyUsage::EvidenceSigning,
                &EvidenceClaim::at(at("2027-06-01T00:00:00Z")),
                &IndependentObservation::none(),
                now(),
            ),
            TrustResult::Untrusted,
            TrustBasis::None,
            Some(UntrustReason::SignedOutsideValidity),
        ),
        (
            "Active, claiming a signing time in the future",
            verdict_at(&active(), "2026-12-31T00:00:00Z"),
            TrustResult::Untrusted,
            TrustBasis::None,
            Some(UntrustReason::SignedOutsideValidity),
        ),
    ];

    assert_eq!(
        rows.len(),
        14,
        "D3 §7.4's table has twelve distinguishable rows once the two \
         Revoked/KeyCompromise cases and the two Superseded directions are counted, plus the \
         two fail-closed rows the W1 review added (F3, F4); a row removed here is a row nobody \
         checks"
    );
    for (name, verdict, result, basis, reason) in rows {
        assert_eq!(verdict.result, result, "row `{name}`: result");
        assert_eq!(verdict.basis, basis, "row `{name}`: basis");
        assert_eq!(verdict.reason, reason, "row `{name}`: reason");
    }
}

/// `trust.keyState` is reported on every row, including the refusals.
#[test]
fn every_verdict_names_the_key_state_it_saw() {
    assert_eq!(
        verdict_at(&active(), "2026-06-01T00:00:00Z").key_state,
        TrustKeyState::Active
    );
    assert_eq!(
        verdict_at(&expired(), "2026-02-01T00:00:00Z").key_state,
        TrustKeyState::Expired
    );
    assert_eq!(
        verdict_at(&retired(), "2026-02-01T00:00:00Z").key_state,
        TrustKeyState::Retired
    );
    assert_eq!(
        verdict_at(
            &revoked(RevocationReason::KeyCompromise),
            "2026-02-01T00:00:00Z"
        )
        .key_state,
        TrustKeyState::Revoked
    );
    let absent = decide(
        None,
        KeyUsage::EvidenceSigning,
        &EvidenceClaim::unknown(),
        &IndependentObservation::none(),
        now(),
    );
    assert_eq!(
        absent.key_state,
        TrustKeyState::Unknown,
        "a key the policy does not carry has no state to report, and `Unknown` is the honest \
         word for it — not `Revoked`, which would claim somebody withdrew it"
    );
}

/// A green badge is `Valid` on a `Current` or `Historical` basis, and the
/// compromise row is excluded twice over.
#[test]
fn only_current_and_historical_may_render_green() {
    assert!(verdict_at(&active(), "2026-06-01T00:00:00Z").may_render_green());
    assert!(verdict_at(&retired(), "2026-02-01T00:00:00Z").may_render_green());
    let observed = decide(
        Some(&revoked(RevocationReason::KeyCompromise)),
        KeyUsage::EvidenceSigning,
        &EvidenceClaim::at(at("2026-02-01T00:00:00Z")),
        &IndependentObservation::at(at("2026-02-01T00:00:00Z")),
        now(),
    );
    assert!(
        !observed.may_render_green(),
        "D3 §7.4: a RecordedBeforeRevocation verdict is rendered with the recorded instant and \
         NEVER green"
    );
}

// ---------------------------------------------------------------------------
// The boundaries, which a table cannot state
// ---------------------------------------------------------------------------

/// "Signed **at or before** `notAfter`/`retiredAt`" — the boundary instant
/// itself is inside, and one second later is outside.
#[test]
fn the_historical_boundary_is_inclusive_and_one_second_later_is_not() {
    let key = retired();
    let boundary = key.retired_at.expect("a retired key has retiredAt");
    let inside = decide(
        Some(&key),
        KeyUsage::EvidenceSigning,
        &EvidenceClaim::at(boundary),
        &IndependentObservation::none(),
        now(),
    );
    assert_eq!(inside.basis, TrustBasis::Historical);
    let outside = decide(
        Some(&key),
        KeyUsage::EvidenceSigning,
        &EvidenceClaim::at(boundary + Duration::seconds(1)),
        &IndependentObservation::none(),
        now(),
    );
    assert_eq!(outside.reason, Some(UntrustReason::SignedOutsideValidity));
}

/// The EARLIEST bound wins. A key retired in March whose `notAfter` is in
/// December stopped signing in March.
#[test]
fn the_boundary_is_the_earliest_bound_not_the_latest() {
    let key = retired();
    assert_eq!(
        key.accepted_through(),
        Some(at("2026-03-01T00:00:00Z")),
        "retiredAt is March and notAfter is next January; taking the later of the two would \
         accept ten months of signatures the operator had already withdrawn permission for"
    );
    assert_eq!(
        verdict_at(&key, "2026-04-01T00:00:00Z").reason,
        Some(UntrustReason::SignedOutsideValidity)
    );
}

/// A claim BEFORE `notBefore` is outside the window in both directions.
#[test]
fn a_claim_before_not_before_is_outside_validity() {
    assert_eq!(
        verdict_at(&active(), "2025-06-01T00:00:00Z").reason,
        Some(UntrustReason::SignedOutsideValidity)
    );
}

/// **Fail closed.** A document claiming no signing time at all is refused, not
/// accepted.
#[test]
fn a_document_with_no_claimed_signing_time_is_refused() {
    for key in [active(), expired(), retired()] {
        let v = decide(
            Some(&key),
            KeyUsage::EvidenceSigning,
            &EvidenceClaim::unknown(),
            &IndependentObservation::none(),
            now(),
        );
        assert_eq!(
            v.result,
            TrustResult::Untrusted,
            "the claim is what the window is checked against, so its absence is `nothing to \
             check`, never `no constraint applies`"
        );
        assert_eq!(v.reason, Some(UntrustReason::SignedOutsideValidity));
    }
}

/// An observation AT `revocationEffectiveFrom` is not BEFORE it.
#[test]
fn the_compromise_observation_must_be_strictly_before_the_revocation() {
    let key = revoked(RevocationReason::KeyCompromise);
    let effective = key
        .revocation_effective_from
        .expect("a revoked key has revocationEffectiveFrom");
    let v = decide(
        Some(&key),
        KeyUsage::EvidenceSigning,
        &EvidenceClaim::at(at("2026-02-01T00:00:00Z")),
        &IndependentObservation::at(effective),
        now(),
    );
    assert_eq!(
        v.reason,
        Some(UntrustReason::Revoked),
        "an observation made AT the instant the compromise took effect proves nothing about \
         the moment before it"
    );
}

/// A compromise revocation with no `revocationEffectiveFrom` at all fails
/// closed, whatever the observation says.
#[test]
fn a_compromise_with_no_effective_instant_fails_closed() {
    let key = TrustedKey {
        revoked_at: None,
        revocation_effective_from: None,
        ..revoked(RevocationReason::KeyCompromise)
    };
    let v = decide(
        Some(&key),
        KeyUsage::EvidenceSigning,
        &EvidenceClaim::at(at("2026-01-02T00:00:00Z")),
        &IndependentObservation::at(at("2026-01-02T00:00:00Z")),
        now(),
    );
    assert_eq!(v.reason, Some(UntrustReason::Revoked));
    assert_eq!(key.accepted_through(), None);
}

// ---------------------------------------------------------------------------
// The OTHER question
// ---------------------------------------------------------------------------

/// **Overlap**: two `Active` `EvidenceSigning` keys both sign and both verify.
/// This is the rotation window PLAT-19.1 exists to make expressible.
#[test]
fn two_active_evidence_keys_overlap() {
    let old = TrustedKey {
        key_id: "1".repeat(64),
        ..active()
    };
    let new = TrustedKey {
        key_id: "2".repeat(64),
        not_before: at("2026-09-01T00:00:00Z"),
        ..active()
    };
    for key in [&old, &new] {
        assert!(may_sign_new(Some(key), KeyUsage::EvidenceSigning, now()).is_ok());
        assert_eq!(
            verdict_at(key, "2026-09-10T00:00:00Z").basis,
            TrustBasis::Current
        );
    }
}

/// **Retirement**: the old key's archives still verify and it may sign nothing
/// new. The two answers disagree, which is the whole point.
#[test]
fn a_retired_key_verifies_old_evidence_and_signs_nothing_new() {
    let key = retired();
    assert_eq!(
        verdict_at(&key, "2026-02-01T00:00:00Z").basis,
        TrustBasis::Historical,
        "the archive it signed in February is still valid"
    );
    assert_eq!(
        may_sign_new(Some(&key), KeyUsage::EvidenceSigning, now()),
        Err(SigningRefusal::KeyRetired),
        "and it authorises nothing new"
    );
}

/// Every refusal `may_sign_new` can produce, named.
#[test]
fn may_sign_new_refuses_for_each_reason_separately() {
    assert_eq!(
        may_sign_new(None, KeyUsage::EvidenceSigning, now()),
        Err(SigningRefusal::UntrustedSigner)
    );
    assert_eq!(
        may_sign_new(Some(&active()), KeyUsage::GovernedApproval, now()),
        Err(SigningRefusal::KeyUsageMismatch)
    );
    assert_eq!(
        may_sign_new(Some(&expired()), KeyUsage::EvidenceSigning, now()),
        Err(SigningRefusal::KeyIdExpired),
        "`KeyIdExpired` keeps the spelling today's roster path writes onto \
         status.conditions[type=Verified].reason and that docs/kubernetes.md §8 lists"
    );
    assert_eq!(
        may_sign_new(Some(&retired()), KeyUsage::EvidenceSigning, now()),
        Err(SigningRefusal::KeyRetired)
    );
    assert_eq!(
        may_sign_new(
            Some(&revoked(RevocationReason::Superseded)),
            KeyUsage::EvidenceSigning,
            now()
        ),
        Err(SigningRefusal::KeyRevoked),
        "a superseded key still verifies old evidence and still signs nothing new"
    );
    let future = TrustedKey {
        not_before: at("2027-01-01T00:00:00Z"),
        not_after: at("2028-01-01T00:00:00Z"),
        ..active()
    };
    assert_eq!(
        may_sign_new(Some(&future), KeyUsage::EvidenceSigning, now()),
        Err(SigningRefusal::KeyNotYetValid)
    );
    assert!(may_sign_new(Some(&active()), KeyUsage::EvidenceSigning, now()).is_ok());
}

/// `notAfter` is exclusive for signing: the instant it names is already past.
#[test]
fn the_signing_window_closes_at_not_after_not_after_it() {
    let key = active();
    assert_eq!(
        may_sign_new(Some(&key), KeyUsage::EvidenceSigning, key.not_after),
        Err(SigningRefusal::KeyIdExpired)
    );
    assert!(may_sign_new(
        Some(&key),
        KeyUsage::EvidenceSigning,
        key.not_after - Duration::seconds(1)
    )
    .is_ok());
    assert!(may_sign_new(Some(&key), KeyUsage::EvidenceSigning, key.not_before).is_ok());
    assert_eq!(
        may_sign_new(
            Some(&key),
            KeyUsage::EvidenceSigning,
            key.not_before - Duration::seconds(1)
        ),
        Err(SigningRefusal::KeyNotYetValid)
    );
}

// ---------------------------------------------------------------------------
// The status projections
// ---------------------------------------------------------------------------

/// `status.keys[].effectiveState`, every value the pure layer can produce.
#[test]
fn effective_state_resolves_the_declared_state_against_the_clock() {
    assert_eq!(effective_state(&active(), now()), EffectiveState::Active);
    assert_eq!(effective_state(&expired(), now()), EffectiveState::Expired);
    assert_eq!(effective_state(&retired(), now()), EffectiveState::Retired);
    assert_eq!(
        effective_state(&revoked(RevocationReason::Superseded), now()),
        EffectiveState::Revoked
    );
    let future = TrustedKey {
        not_before: at("2027-01-01T00:00:00Z"),
        not_after: at("2028-01-01T00:00:00Z"),
        ..active()
    };
    assert_eq!(effective_state(&future, now()), EffectiveState::NotYetValid);
    assert_ne!(
        effective_state(&active(), now()),
        EffectiveState::Unparseable,
        "the pure layer never sees key material, so it can never produce this value"
    );
}

/// `status.keys[].usableForVerification`, and `Historical` is not a failure.
#[test]
fn usable_for_verification_separates_full_historical_and_none() {
    assert_eq!(
        usable_for_verification(&active(), now()),
        VerificationUse::Full
    );
    assert_eq!(
        usable_for_verification(&retired(), now()),
        VerificationUse::Historical
    );
    assert_eq!(
        usable_for_verification(&expired(), now()),
        VerificationUse::Historical
    );
    assert_eq!(
        usable_for_verification(&revoked(RevocationReason::Superseded), now()),
        VerificationUse::Historical
    );
    assert_eq!(
        usable_for_verification(&revoked(RevocationReason::KeyCompromise), now()),
        VerificationUse::None,
        "a compromised key's own claim is worth nothing, so nothing it signed verifies on the \
         strength of it"
    );
    assert!(usable_for_new_signatures(&active(), now()));
    assert!(!usable_for_new_signatures(&retired(), now()));
    assert!(!usable_for_new_signatures(&expired(), now()));
}

// ---------------------------------------------------------------------------
// `claimed_signing_time`
// ---------------------------------------------------------------------------

/// The five document types and their fields (D3 §7.4).
#[test]
fn the_claimed_signing_time_is_read_per_document_type() {
    let cases: [(&str, serde_json::Value, &str); 5] = [
        (
            "application/vnd.logweir.backup-receipt+json;version=1.0.0",
            json!({"finished_at": "2026-05-01T01:00:00Z"}),
            "2026-05-01T01:00:00Z",
        ),
        (
            "application/vnd.logweir.drill-scorecard+json;version=1.0.0",
            json!({"phases": [{"at": "2026-05-01T00:00:00Z"}, {"at": "2026-05-01T02:00:00Z"}]}),
            "2026-05-01T02:00:00Z",
        ),
        (
            "application/vnd.logweir.drill-teardown+json;version=1.0.0",
            json!({"deleted_at": "2026-05-01T03:00:00Z"}),
            "2026-05-01T03:00:00Z",
        ),
        (
            "application/vnd.logweir.drill-approval+json;version=1.0.0",
            json!({"approved_at": "2026-05-01T04:00:00Z"}),
            "2026-05-01T04:00:00Z",
        ),
        (
            "application/vnd.logweir.catalog-point+json;version=1.0.0",
            json!({"recorded_at": "2026-05-01T05:00:00Z"}),
            "2026-05-01T05:00:00Z",
        ),
    ];
    for (payload_type, document, want) in cases {
        assert_eq!(
            claimed_signing_time(payload_type, &document),
            Some(at(want)),
            "{payload_type}"
        );
    }
}

/// The base media type decides, so a future `;version=` keeps working.
#[test]
fn a_later_version_of_the_same_media_type_still_resolves() {
    assert_eq!(
        claimed_signing_time(
            "application/vnd.logweir.backup-receipt+json;version=1.1.0",
            &json!({"finished_at": "2026-05-01T01:00:00Z"})
        ),
        Some(at("2026-05-01T01:00:00Z")),
        "a version bump that silently produced `None` would, under decide's fail-closed rule, \
         refuse every document of that version"
    );
}

/// The scorecard's LAST phase, not its first.
#[test]
fn the_scorecard_claim_is_the_last_phase_not_the_first() {
    let card = json!({"phases": [{"at": "2026-01-01T00:00:00Z"}, {"at": "2026-08-01T00:00:00Z"}]});
    assert_eq!(
        claimed_signing_time(
            "application/vnd.logweir.drill-scorecard+json;version=1.0.0",
            &card
        ),
        Some(at("2026-08-01T00:00:00Z"))
    );
}

/// Unknown type, missing field, empty phase list and a non-instant all produce
/// `None` — which refuses — and each one NAMES ITSELF (review finding F7).
#[test]
fn an_unreadable_claim_is_none_and_names_why() {
    const RECEIPT: &str = "application/vnd.logweir.backup-receipt+json;version=1.0.0";
    const CARD: &str = "application/vnd.logweir.drill-scorecard+json;version=1.0.0";
    let cases: [(&str, serde_json::Value, ClaimAbsence); 4] = [
        (
            "application/json",
            json!({"finished_at": "2026-05-01T01:00:00Z"}),
            ClaimAbsence::UnknownPayloadType,
        ),
        (RECEIPT, json!({}), ClaimAbsence::FieldAbsent),
        (CARD, json!({"phases": []}), ClaimAbsence::EmptyPhases),
        (
            RECEIPT,
            json!({"finished_at": "not an instant"}),
            ClaimAbsence::Unparseable,
        ),
    ];
    for (payload_type, document, absence) in cases {
        assert_eq!(claimed_signing_time(payload_type, &document), None);
        assert_eq!(
            read_claimed_signing_time(payload_type, &document),
            Err(absence),
            "{payload_type} {document}"
        );
        let claim = EvidenceClaim::from_document(payload_type, &document);
        assert_eq!(claim.signed_at, None);
        assert_eq!(claim.absence, Some(absence));
        // And it refuses, with a detail an operator can act on.
        let v = decide(
            Some(&active()),
            KeyUsage::EvidenceSigning,
            &claim,
            &IndependentObservation::none(),
            now(),
        );
        assert_eq!(v.reason, Some(UntrustReason::SignedOutsideValidity));
        assert!(!absence.detail().is_empty());
    }
    assert_eq!(
        ClaimAbsence::EmptyPhases.detail(),
        "this scorecard records no phase, so it carries no signing time and the key's validity \
         window could not be checked",
        "the named refusal exists so `SignedOutsideValidity` does not arrive as the answer to \
         `why is my scorecard untrusted` — a scorecard with no phase is schema-valid, because \
         the scorecard schema puts no minItems on `phases`"
    );
}

/// The production constructor names the reason; the test shorthand does not.
#[test]
fn from_document_always_names_an_absence() {
    let claim = EvidenceClaim::from_document(
        "application/vnd.logweir.drill-scorecard+json;version=1.0.0",
        &json!({"phases": [{"at": "2026-05-01T00:00:00Z"}]}),
    );
    assert_eq!(claim.signed_at, Some(at("2026-05-01T00:00:00Z")));
    assert_eq!(claim.absence, None);
    assert_eq!(EvidenceClaim::unknown().absence, None);
}

// ---------------------------------------------------------------------------
// The rehearsal scope types (D3 §4.3)
// ---------------------------------------------------------------------------

/// The scope round-trips through its wire shape, in camelCase.
#[test]
fn the_rehearsal_scope_serialises_in_camel_case() {
    let scope = RehearsalScope {
        template_digest: "sha256:abc".to_string(),
        target_cluster_id: "target-1".to_string(),
        topic_prefix: "rehearsal-3f2a91c7-".to_string(),
        topics: vec!["orders".to_string()],
        max_partitions: 12,
        records_per_partition: 100,
        deadline_seconds: 900,
        modes: vec![MODE_SCRATCH.to_string()],
    };
    let wire = serde_json::to_value(&scope).expect("a serialisable scope");
    for field in [
        "templateDigest",
        "targetClusterId",
        "topicPrefix",
        "topics",
        "maxPartitions",
        "recordsPerPartition",
        "deadlineSeconds",
        "modes",
    ] {
        assert!(
            wire.get(field).is_some(),
            "the signed authorization document names `{field}`; a scope that serialised it \
             differently would verify against a document nobody signed"
        );
    }
    let back: RehearsalScope = serde_json::from_value(wire).expect("a round trip");
    assert_eq!(back, scope);
    assert!(scope.is_scratch_only());
}

/// A scope naming any other mode is not scratch-only, and an empty mode list
/// is not either.
#[test]
fn a_scope_naming_another_mode_is_not_scratch_only() {
    let base = RehearsalScope {
        template_digest: "sha256:abc".to_string(),
        target_cluster_id: "target-1".to_string(),
        topic_prefix: "p-".to_string(),
        topics: vec![],
        max_partitions: 1,
        records_per_partition: 1,
        deadline_seconds: 1,
        modes: vec![MODE_SCRATCH.to_string(), "newTopic".to_string()],
    };
    assert!(!base.is_scratch_only());
    assert!(!RehearsalScope {
        modes: vec![],
        ..base
    }
    .is_scratch_only());
}

/// **MUTANT 7 — a `NotYetValid` key verifying green** (review finding F3).
///
/// The planted mutation is dropping `decide`'s `NotYetValid` arm. The key
/// below is the successor staged at §7.6 step 1: `notBefore` next January,
/// `notAfter` the January after. A document claiming a time inside that
/// unopened window then satisfies `notBefore <= signed_at < notAfter` and
/// renders `Valid`/`Current`/GREEN — while `status.keys[]` reports
/// `usableForVerification: None` for the SAME key at the SAME instant.
///
/// The oracle is `usable_for_verification`, deliberately: asserting against a
/// literal would let the two surfaces drift apart again, and it is the
/// disagreement between them that is the defect.
#[test]
fn mutant_a_key_whose_window_has_not_opened_verifies_nothing() {
    let key = not_yet_valid();
    assert_eq!(
        effective_state(&key, now()),
        EffectiveState::NotYetValid,
        "the fixture only tests what it claims if the window really is closed"
    );
    assert_eq!(
        usable_for_verification(&key, now()),
        VerificationUse::None,
        "the status surface says None ..."
    );
    let v = decide(
        Some(&key),
        KeyUsage::EvidenceSigning,
        &EvidenceClaim::at(at("2027-06-01T00:00:00Z")),
        &IndependentObservation::none(),
        now(),
    );
    assert_eq!(
        v.result,
        TrustResult::Untrusted,
        "... and the badge must agree with it"
    );
    assert_eq!(v.basis, TrustBasis::None);
    assert_eq!(v.reason, Some(UntrustReason::SignedOutsideValidity));
    assert!(!v.may_render_green());
    assert_eq!(
        may_sign_new(Some(&key), KeyUsage::EvidenceSigning, now()),
        Err(SigningRefusal::KeyNotYetValid),
        "and the third surface already agreed before the fix"
    );
}

/// **A claim in the future is not a claim** (review finding F4).
///
/// `signed_at` is a field the DOCUMENT controls. Without the `signed_at <= now`
/// bound a signer may name any instant up to `notAfter` and be trusted as
/// `Current` today — a fail-open on attacker-influenced input, and the thing
/// that made the `NotYetValid` hole above reachable.
#[test]
fn a_claim_after_now_is_outside_validity() {
    let key = active();
    assert!(
        now() < key.not_after,
        "the fixture needs a claim that is inside the window and still in the future"
    );
    let future = verdict_at(&key, "2026-12-31T00:00:00Z");
    assert_eq!(future.reason, Some(UntrustReason::SignedOutsideValidity));
    // The boundary: `now` itself is accepted.
    let boundary = decide(
        Some(&key),
        KeyUsage::EvidenceSigning,
        &EvidenceClaim::at(now()),
        &IndependentObservation::none(),
        now(),
    );
    assert_eq!(boundary.basis, TrustBasis::Current);
}

// ---------------------------------------------------------------------------
// MUTANTS
// ---------------------------------------------------------------------------

/// **MUTANT 1 — a revoked-compromised key verifying without an observation.**
///
/// The planted mutation is `decide`'s compromise arm falling through to the
/// window check instead of returning `Revoked`: the key's `notAfter` is next
/// January and its claim is February, so the window check would call it
/// `Valid`/`Historical`. This test is what kills it, and it asserts the
/// verdict AND the basis so a mutant that returned `Untrusted` with a
/// `Historical` basis — enough to make a badge green in a consumer that reads
/// the basis — dies too.
#[test]
fn mutant_a_compromised_key_never_verifies_without_an_observation() {
    let v = verdict_at(
        &revoked(RevocationReason::KeyCompromise),
        "2026-02-01T00:00:00Z",
    );
    assert_eq!(v.result, TrustResult::Untrusted);
    assert_eq!(v.basis, TrustBasis::None);
    assert_eq!(v.reason, Some(UntrustReason::Revoked));
    assert!(!v.may_render_green());
}

/// **MUTANT 2 — a retired key accepted for a new signature.**
///
/// The planted mutation is `may_sign_new` checking only the validity window,
/// which a retired key still satisfies (its `notAfter` is next January). The
/// assertion is the REFUSAL VARIANT and not merely `is_err`, so a mutant that
/// refused for the wrong reason — reporting `KeyIdExpired` on a key whose
/// window is open — dies as well.
#[test]
fn mutant_a_retired_key_may_not_sign_anything_new() {
    let key = retired();
    assert!(
        now() < key.not_after,
        "the fixture only tests what it claims if the retired key's window is still open"
    );
    assert_eq!(
        may_sign_new(Some(&key), KeyUsage::EvidenceSigning, now()),
        Err(SigningRefusal::KeyRetired)
    );
}

/// **MUTANT 5 — `now` read from the clock inside the pure layer.**
///
/// Three halves, because each alone can be satisfied by accident:
///
/// 1. `effective_state` over the SAME key at two instants on opposite sides of
///    its window must disagree. A function that ignored its `now` argument and
///    called `Utc::now()` returns the same answer for both, whatever the real
///    date is — so this arm needs no assumption about when the suite runs,
///    which an assertion against the fixture `now` alone would.
/// 2. `decide` reports the same document as `Current` at a `now` inside the
///    window and `Historical` at a `now` past it.
/// 3. `scripts/check-pure-core.sh` greps the crate for `Utc::now` and every
///    other clock API, and runs in `just lint`. That is the structural half;
///    the two above are the behavioural ones.
///
/// MEASURED: with arm 1 absent, a planted `let now = Utc::now();` inside
/// `effective_state` left this whole file GREEN and was caught only by the
/// script. Arm 1 is what makes the behavioural half of this mutant real.
#[test]
fn the_verdict_is_a_function_of_its_arguments_and_nothing_else() {
    // ---- 1. the state resolver reads its argument ------------------------
    let key = active();
    assert_eq!(
        effective_state(&key, at("2025-01-01T00:00:00Z")),
        EffectiveState::NotYetValid,
        "before the window opens"
    );
    assert_eq!(
        effective_state(&key, at("2030-01-01T00:00:00Z")),
        EffectiveState::Expired,
        "and long after it closed — two instants a clock read could not tell apart"
    );

    // ---- 2. the verdict reads its argument -------------------------------
    let first = verdict_at(&key, "2026-06-01T00:00:00Z");
    let second = verdict_at(&key, "2026-06-01T00:00:00Z");
    assert_eq!(first, second, "the same arguments, the same verdict");

    let later = decide(
        Some(&key),
        KeyUsage::EvidenceSigning,
        &EvidenceClaim::at(at("2026-06-01T00:00:00Z")),
        &IndependentObservation::none(),
        at("2028-01-01T00:00:00Z"),
    );
    assert_eq!(
        first.basis,
        TrustBasis::Current,
        "at the fixture `now` the key is inside its window"
    );
    assert_eq!(
        later.basis,
        TrustBasis::Historical,
        "and at a `now` past its notAfter the SAME document is Historical — which can only be \
         true if `now` is the argument and not a clock read"
    );
}

// ===========================================================================
// TRUST-UPGRADE-SIGNEDAT — a status that predates `signedAt` is not a document
// that claims none
// ===========================================================================

/// **The whole rule, in one assertion.** A claim whose absence is
/// [`ClaimAbsence::NotRecorded`] produces an UNDECIDED verdict: basis
/// `Unverified`, never green, and `awaits_signing_time()` true so the one
/// caller that can re-read the document knows to.
///
/// KILLS: "treat `NotRecorded` like `FieldAbsent`" — the basis below would be
/// `None` and `awaits_signing_time()` false, which is exactly the shipped
/// behaviour that re-derived five sound 2026-09-14 lab objects from `Valid` to
/// `Untrusted` without reading one byte of any archive.
///
/// ALSO KILLS: "make the undecided verdict `Valid` so nothing flips" — a caller
/// that reads `result` alone must still refuse, because an undecided verdict is
/// not a pass. Both halves are asserted.
#[test]
fn a_status_that_predates_signedat_is_undecided_and_never_green() {
    let key = active();
    let verdict = decide(
        Some(&key),
        KeyUsage::EvidenceSigning,
        &EvidenceClaim::absent(ClaimAbsence::NotRecorded),
        &IndependentObservation::none(),
        at("2026-09-18T13:14:39Z"),
    );
    assert!(
        verdict.awaits_signing_time(),
        "the caller has to be able to tell 'not yet asked' from 'asked and refused'"
    );
    assert_eq!(verdict.basis, TrustBasis::Unverified);
    assert_eq!(
        verdict.basis.as_str(),
        "Unverified",
        "the wire spelling a status renders"
    );
    assert!(
        !verdict.may_render_green(),
        "an undecided verdict is not a pass, and D3 §7.4 admits only Current and Historical"
    );
    assert_eq!(
        verdict.result,
        TrustResult::Untrusted,
        "FAIL CLOSED BY DEFAULT: a caller that never learns about `awaits_signing_time` refuses, \
         which is the direction every other row of the table fails in"
    );

    // …and the DOCUMENT's own absence is untouched: still a refusal, still on
    // no basis at all, and it does not await anything.
    let document = decide(
        Some(&key),
        KeyUsage::EvidenceSigning,
        &EvidenceClaim::absent(ClaimAbsence::FieldAbsent),
        &IndependentObservation::none(),
        at("2026-09-18T13:14:39Z"),
    );
    assert!(!document.awaits_signing_time());
    assert_eq!(document.basis, TrustBasis::None);
    assert_eq!(document.reason, Some(UntrustReason::SignedOutsideValidity));
}

/// **The ratchet.** Every row that does not read the claim still takes effect
/// immediately on a pre-`signedAt` object — a compromise revocation above all.
///
/// KILLS: "check `NotRecorded` first, before the key rows" — the most
/// dangerous edit available here. It would park a `KeyCompromise` revocation on
/// `Unverified` and keep the previous `Valid` result while a stolen key's
/// signatures stayed accepted until an archive happened to be readable. Each of
/// the four rows below is a separate assertion, and moving the arm above any of
/// them fails this test.
#[test]
fn a_compromise_revocation_flips_a_pre_signedat_object_with_no_read() {
    let now = at("2026-09-18T13:14:39Z");
    let effective = at("2026-09-10T00:00:00Z");
    let claim = EvidenceClaim::absent(ClaimAbsence::NotRecorded);

    // ---- no key on the policy at all -------------------------------------
    let stranger = decide(
        None,
        KeyUsage::EvidenceSigning,
        &claim,
        &IndependentObservation::none(),
        now,
    );
    assert!(!stranger.awaits_signing_time());
    assert_eq!(stranger.reason, Some(UntrustReason::UntrustedSigner));

    // ---- the key exists and carries the wrong usage -----------------------
    let mut approval_only = active();
    approval_only.usages = vec![KeyUsage::GovernedApproval];
    let mismatch = decide(
        Some(&approval_only),
        KeyUsage::EvidenceSigning,
        &claim,
        &IndependentObservation::none(),
        now,
    );
    assert!(!mismatch.awaits_signing_time());
    assert_eq!(mismatch.reason, Some(UntrustReason::KeyUsageMismatch));

    // ---- KeyCompromise, no local history: fails closed NOW ----------------
    let mut compromised = active();
    compromised.state = KeyState::Revoked;
    compromised.revoked_at = Some(effective);
    compromised.revocation_reason = Some(RevocationReason::KeyCompromise);
    compromised.revocation_effective_from = Some(effective);
    let revoked = decide(
        Some(&compromised),
        KeyUsage::EvidenceSigning,
        &claim,
        &IndependentObservation::none(),
        now,
    );
    assert!(
        !revoked.awaits_signing_time(),
        "a stolen private half is not a question about when the document was signed, and waiting \
         for an archive read before acting on it is the one delay this rule may not take"
    );
    assert_eq!(revoked.result, TrustResult::Untrusted);
    assert_eq!(revoked.reason, Some(UntrustReason::Revoked));

    // ---- KeyCompromise WITH a controller-written observation ---------------
    let recorded = decide(
        Some(&compromised),
        KeyUsage::EvidenceSigning,
        &claim,
        &IndependentObservation::at(at("2026-09-04T00:00:00Z")),
        now,
    );
    assert!(!recorded.awaits_signing_time());
    assert_eq!(recorded.basis, TrustBasis::RecordedBeforeRevocation);
    assert_eq!(
        recorded.reason,
        Some(UntrustReason::RecordedBeforeRevocation),
        "the compromise rows read the OBSERVATION and never the claim, so the claim's absence \
         cannot reach them at all"
    );
}

/// `NotRecorded` is a fact about a STATUS, so no DOCUMENT ever produces it.
///
/// KILLS: "return `NotRecorded` from `read_claimed_signing_time` for a missing
/// field" — the shortest wrong way to implement this rule. It would make every
/// genuinely timestamp-less document undecided forever, waiting for a re-read
/// that can never supply what the bytes do not contain.
#[test]
fn no_document_ever_claims_notrecorded() {
    let shapes = [
        ("application/vnd.logweir.backup-receipt+json", json!({})),
        (
            "application/vnd.logweir.backup-receipt+json",
            json!({"finished_at": 7}),
        ),
        (
            "application/vnd.logweir.backup-receipt+json",
            json!({"finished_at": "not an instant"}),
        ),
        (
            "application/vnd.logweir.drill-scorecard+json",
            json!({"phases": []}),
        ),
        ("application/vnd.logweir.drill-scorecard+json", json!({})),
        ("application/vnd.logweir.drill-teardown+json", json!({})),
        ("application/vnd.logweir.drill-approval+json", json!({})),
        ("application/vnd.logweir.catalog-point+json", json!({})),
        ("application/vnd.example.unknown+json", json!({})),
        ("", json!({})),
    ];
    for (payload_type, document) in shapes {
        let absence = logweir_core::trust::read_claimed_signing_time(payload_type, &document)
            .expect_err("none of these documents carries a readable signing time");
        assert_ne!(
            absence,
            ClaimAbsence::NotRecorded,
            "`NotRecorded` says 'this installation never recorded it', which no set of document \
             bytes can be evidence for: {payload_type} {document}"
        );
    }
}

/// The two absences do not say the same thing to an operator.
///
/// KILLS: "give `NotRecorded` `FieldAbsent`'s sentence" — which is how the
/// defect presented in the first place: five sound receipts reported as
/// documents that *"carry no signing-time field"*.
#[test]
fn notrecorded_says_it_is_about_the_status_and_not_the_document() {
    assert_eq!(ClaimAbsence::NotRecorded.as_str(), "NotRecorded");
    let detail = ClaimAbsence::NotRecorded.detail();
    assert_ne!(detail, ClaimAbsence::FieldAbsent.detail());
    assert!(
        detail.contains("this status was written before"),
        "it has to name the STATUS as the thing that is old: {detail}"
    );
    assert!(
        !detail.contains("the document carries no"),
        "and it must not report a fact about a document nobody read: {detail}"
    );
    for other in [
        ClaimAbsence::UnknownPayloadType,
        ClaimAbsence::FieldAbsent,
        ClaimAbsence::EmptyPhases,
        ClaimAbsence::Unparseable,
    ] {
        assert_ne!(other.as_str(), ClaimAbsence::NotRecorded.as_str());
    }
}
