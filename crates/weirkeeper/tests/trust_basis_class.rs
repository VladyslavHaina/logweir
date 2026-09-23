//! **TRUST-VALID-BASIS-CLASS.** Every controller reader of a stored
//! `verification.result: Valid` reads it WITH its `trust.basis`, through one
//! rule: `weirkeeper::verification::ValidBasis`.
//!
//! D3 §7.4: "the green rule becomes `Valid ∧ (basis Current|Historical) ∧ run
//! success`". D3 §12: "`trust` absent → … the badge uses the pre-existing
//! rule". So a `Valid` is a pass on `Current`, on `Historical`, and with no
//! trust block at all — and on nothing else.
//!
//! Four sites compared `result == "Valid"` alone (review of
//! `claude/api-trust-state`, item 4):
//!
//! | site | what a non-pass `Valid` used to do |
//! |---|---|
//! | `protection::Evidence::from_verification` | counted as protecting evidence |
//! | `catalog_view::is_reached_refusal` | was never a refusal |
//! | `rehearsal_schedule::candidate_from_backup` `selectable` | was selectable |
//! | `catalog_view`'s `decide` mapping | a wildcard read as `VerifiedHistorical` |
//!
//! None misfired on a verdict a current controller writes (`decide` pairs
//! `Valid` with `Current`/`Historical` only), but a lab object written between
//! `03c2a85` and `e247cf9` carries `Valid` + `Unverified`. Each table below
//! runs every shape against one site; each site's MUTANT (named on its test)
//! reverts it to the plain `Valid` check and fails the rows marked "not a
//! pass".

use chrono::{TimeZone, Utc};
use logweir_core::trust::{TrustBasis, TrustKeyState, TrustResult, UntrustReason, Verdict};
use serde_json::{json, Value};
use weirkeeper::catalog_view::{
    self as view, BackupVerdictFacts, ControllerRefusals, Verification,
};
use weirkeeper::controllers::rehearsal_schedule as rs;
use weirkeeper::crds::backup::Backup;
use weirkeeper::protection as p;
use weirkeeper::verification::{self as v, ValidBasis, VerificationVerdict};

const NS: &str = "logweir-trust-basis";
const RECEIPT_SHA: &str = "sha256:1111111111111111111111111111111111111111111111111111111111111111";
const POINT_ID: &str = "lwp1-11111111111111111111111111111111";
const BACKUP_ID: &str = "b-20260919";

/// What one shape of `verification.trust` must read as.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Expect {
    /// A pass (`Current`, or no block).
    Pass,
    /// A pass on a retired key.
    PassHistorical,
    /// Nothing compared yet: `NotAttempted`.
    Undecided,
    /// A verdict this installation does not accept, or a block it cannot
    /// read: `Untrusted`.
    Refused,
}

/// Every shape of the `trust` block beside a `Valid`, with its reading.
/// `None` is a `Valid` with NO `trust` key at all (D3 §12).
fn shapes() -> Vec<(&'static str, Option<Value>, Expect)> {
    vec![
        (
            "Current",
            Some(json!({"basis": "Current", "keyState": "Active"})),
            Expect::Pass,
        ),
        (
            "Historical",
            Some(json!({"basis": "Historical", "keyState": "Retired"})),
            Expect::PassHistorical,
        ),
        ("no trust block (D3 §12)", None, Expect::Pass),
        (
            "Unverified (the 03c2a85..e247cf9 lab shape)",
            Some(json!({"basis": "Unverified", "keyState": "Active"})),
            Expect::Undecided,
        ),
        (
            "RecordedBeforeRevocation",
            Some(json!({"basis": "RecordedBeforeRevocation", "keyState": "Revoked"})),
            Expect::Refused,
        ),
        (
            "None",
            Some(json!({"basis": "None", "keyState": "Active"})),
            Expect::Refused,
        ),
        ("an empty block", Some(json!({})), Expect::Refused),
        (
            "a null basis",
            Some(json!({"basis": null, "keyState": "Active"})),
            Expect::Refused,
        ),
        (
            "a word this build does not know",
            Some(json!({"basis": "SomeFutureBasis"})),
            Expect::Refused,
        ),
    ]
}

fn is_pass(expect: Expect) -> bool {
    matches!(expect, Expect::Pass | Expect::PassHistorical)
}

/// `status.evidence.verification` for a `Valid` with `trust`.
fn valid_block(trust: Option<&Value>) -> Value {
    let mut block = json!({
        "result": "Valid",
        "matchedKeyId": "key-1",
        "verifiedAt": "2026-09-19T02:10:00Z",
        "payloadType": "application/vnd.logweir.backup-receipt+json"
    });
    if let Some(t) = trust {
        block["trust"] = t.clone();
    }
    block
}

/// A succeeded `Backup` whose receipt was captured and verified as `Valid`
/// with `trust`.
fn backup_value(trust: Option<&Value>) -> Value {
    json!({
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "Backup",
        "metadata": {"name": "b-1", "namespace": NS, "uid": "uid-b-1", "resourceVersion": "9",
                     "creationTimestamp": "2026-09-19T02:00:00Z"},
        "spec": {
            "sourceRef": {"name": "prod-kafka"},
            "topics": ["orders"],
            "archive": {"url": "logweir-destination://primary"},
            "destinationRef": {"name": "primary"},
            "triggeredBy": "manual",
            "deadlineSeconds": 3600
        },
        "status": {
            "phase": "Succeeded",
            "exitCode": 0,
            "backupId": BACKUP_ID,
            "capture": {"startedAt": "2026-09-19T02:00:00Z"},
            "windowCovered": {"fromMs": 1_758_240_000_000_i64, "toMs": 1_758_326_400_000_i64},
            "evidence": {
                "receiptKey": "logweir/backups/b-20260919.json",
                "receiptSha256": RECEIPT_SHA,
                "verification": valid_block(trust)
            }
        }
    })
}

fn typed_backup(trust: Option<&Value>) -> Backup {
    serde_json::from_value(backup_value(trust)).expect("the Backup fixture parses")
}

/// The typed block, as a typed reader (`Backup.status`) sees it.
fn typed_block(trust: Option<&Value>) -> Option<weirkeeper::crds::TrustBasis> {
    trust.map(|t| serde_json::from_value(t.clone()).expect("the trust block parses"))
}

/// A `Verified`, selectable catalog row for the same receipt digest.
fn verified_row() -> view::ViewEntry {
    serde_json::from_value(json!({
        "pointId": POINT_ID,
        "backupId": BACKUP_ID,
        "runId": "run-b",
        "recoveryPointAtMs": 1_758_247_200_000_i64,
        "coveredFromMs": 1_758_240_000_000_i64,
        "coveredToMs": 1_758_326_400_000_i64,
        "locations": [{"locationId": "primary", "availability": "Available"}],
        "receiptKey": "logweir/backups/b-20260919.json",
        "receiptSha256": RECEIPT_SHA,
        "availability": "Available",
        "verification": "Verified",
        "selectable": true
    }))
    .expect("the catalog row parses")
}

// ===========================================================================
// The rule itself
// ===========================================================================

/// `ValidBasis` over every shape, from the raw JSON and from the typed block.
/// The two read paths agree on every shape a stored object can carry.
#[test]
fn the_one_rule_admits_current_historical_and_an_absent_block_only() {
    for (label, trust, expect) in shapes() {
        let from_json = ValidBasis::of_json(trust.as_ref());
        let from_block = ValidBasis::of_block(typed_block(trust.as_ref()).as_ref());
        let want = match (expect, trust.is_some()) {
            (Expect::Pass, false) => ValidBasis::Absent,
            (Expect::Pass, true) => ValidBasis::Current,
            (Expect::PassHistorical, _) => ValidBasis::Historical,
            (Expect::Undecided, _) => ValidBasis::Unverified,
            (Expect::Refused, _) => ValidBasis::Refused,
        };
        assert_eq!(from_json, want, "{label}: raw JSON");
        assert_eq!(from_block, want, "{label}: typed block");
        assert_eq!(from_json.is_pass(), is_pass(expect), "{label}");
        assert_eq!(
            v::stored_result_is_pass(Some("Valid"), from_json),
            is_pass(expect),
            "{label}"
        );
    }
    // Only `Valid` can be a pass, whatever the basis.
    for result in [
        None,
        Some("Invalid"),
        Some("Untrusted"),
        Some("NotAttempted"),
        Some("Pending"),
        Some("valid"),
    ] {
        assert!(
            !v::stored_result_is_pass(result, ValidBasis::Current),
            "{result:?} is not a pass on a Current basis"
        );
    }
    // `decide`'s own bases.
    assert_eq!(
        ValidBasis::of_core(TrustBasis::Current),
        ValidBasis::Current
    );
    assert_eq!(
        ValidBasis::of_core(TrustBasis::Historical),
        ValidBasis::Historical
    );
    assert_eq!(
        ValidBasis::of_core(TrustBasis::Unverified),
        ValidBasis::Unverified
    );
    for refused in [TrustBasis::RecordedBeforeRevocation, TrustBasis::None] {
        assert_eq!(ValidBasis::of_core(refused), ValidBasis::Refused);
    }
}

/// The controller's badge reads the same rule (`verification_is_valid` is the
/// verification half of `backup_badge`/`restore_badge`), so every site below
/// agrees with the surface an operator looks at.
#[test]
fn the_badge_and_the_rule_agree_on_every_shape() {
    for (label, trust, expect) in shapes() {
        let status = json!({"evidence": {"verification": valid_block(trust.as_ref())}});
        assert_eq!(
            v::verification_is_valid(&status),
            is_pass(expect),
            "{label}"
        );
    }
}

// ===========================================================================
// Site 1: protection — `Evidence::from_verification`
// ===========================================================================

/// MUTANT: revert `Evidence::from_verification`'s `Valid` arm to
/// `(Some("Valid"), Some("Historical")) => ValidHistorical, (Some("Valid"), _)
/// => Valid`. The `Unverified`, `RecordedBeforeRevocation`, `None`, empty,
/// null-basis and unknown-word rows read as protecting evidence and fail.
#[test]
fn protection_counts_a_valid_as_evidence_only_on_a_passing_basis() {
    for (label, trust, expect) in shapes() {
        let block = typed_block(trust.as_ref());
        let evidence = p::Evidence::from_verification(Some("Valid"), block.as_ref());
        let want = match expect {
            Expect::Pass => p::Evidence::Valid,
            Expect::PassHistorical => p::Evidence::ValidHistorical,
            Expect::Undecided => p::Evidence::NotAttempted,
            Expect::Refused => p::Evidence::Untrusted,
        };
        assert_eq!(evidence, want, "{label}");
        assert_eq!(
            evidence.is_verified(),
            is_pass(expect),
            "{label}: requireVerifiedEvidence"
        );
        // `Unverified` compared nothing: the catalog may answer for it, as for
        // `NotAttempted`. A refused basis is a reached refusal it may not.
        assert_eq!(
            evidence.was_reached(),
            expect != Expect::Undecided,
            "{label}: was_reached"
        );
    }
}

// ===========================================================================
// Site 2: catalog_view — the refusal check
// ===========================================================================

/// MUTANT: revert `is_reached_refusal`'s `Valid` arm to `Some("Valid") =>
/// false` (the old `!matches!(.., "Valid")`). The refused rows stop being
/// refusals — in the function, and in the join built from BOTH read paths
/// (`from_json`, which retention, preflight, rehearsal and the API use, and
/// `from_backup`) — and fail.
#[test]
fn a_valid_on_a_refused_basis_is_a_reached_refusal_and_is_named_untrusted() {
    for (label, trust, expect) in shapes() {
        let refused = expect == Expect::Refused;
        assert_eq!(
            view::is_reached_refusal(Some("Valid"), ValidBasis::of_json(trust.as_ref())),
            refused,
            "{label}"
        );

        let object = backup_value(trust.as_ref());
        let typed = typed_backup(trust.as_ref());
        for (path, facts) in [
            ("from_json", BackupVerdictFacts::from_json(&object)),
            ("from_backup", BackupVerdictFacts::from_backup(&typed)),
        ] {
            let refusals = ControllerRefusals::from_facts([facts]);
            assert_eq!(refusals.is_empty(), !refused, "{label}: {path}");
            assert_eq!(
                refusals.refusal_for(&verified_row()),
                refused.then_some(view::REFUSED_VALID_VERDICT),
                "{label}: {path} — a refused Valid is published as `Untrusted`, never as `Valid`"
            );
        }
    }
    assert_eq!(view::REFUSED_VALID_VERDICT, "Untrusted");
    // The basis is read for `Valid` only.
    for (result, refusal) in [
        (None, false),
        (Some("NotAttempted"), false),
        (Some("Pending"), false),
        (Some("Invalid"), true),
        (Some("Untrusted"), true),
        (Some("SomeFutureVerdict"), true),
    ] {
        for basis in [ValidBasis::Current, ValidBasis::Refused, ValidBasis::Absent] {
            assert_eq!(
                view::is_reached_refusal(result, basis),
                refusal,
                "{result:?} on {basis:?}"
            );
        }
    }
}

// ===========================================================================
// Site 3: rehearsal — a `Backup` candidate's `selectable`
// ===========================================================================

/// MUTANT: revert `candidate_from_backup`'s `selectable` to
/// `!needs_catalog_capture && verdict == Some("Valid")`. Every non-pass row
/// becomes selectable on its own and fails.
///
/// A refused basis is also a refusal no `Verified` catalog row may overrule;
/// `Unverified` defers to the catalog like `NotAttempted` (the badge's word).
#[test]
fn a_rehearsal_candidate_is_selectable_only_on_a_passing_basis() {
    for (label, trust, expect) in shapes() {
        let candidate = rs::candidate_from_backup(&typed_backup(trust.as_ref()))
            .expect("a succeeded Backup with a receipt digest is a candidate");
        assert_eq!(candidate.point_id, POINT_ID);
        assert_eq!(candidate.selectable, is_pass(expect), "{label}: selectable");
        assert_eq!(
            candidate.verdict_refused,
            expect == Expect::Refused,
            "{label}: verdict_refused"
        );

        let mut by_id = std::collections::BTreeMap::from([(candidate.point_id.clone(), candidate)]);
        rs::merge_catalog_entry(
            &mut by_id,
            verified_row(),
            Some("primary".to_string()),
            &ControllerRefusals::default(),
        );
        assert_eq!(
            by_id[POINT_ID].selectable,
            expect != Expect::Refused,
            "{label}: a Verified catalog row answers for an undecided Valid and never for a \
             refused one"
        );
    }
}

// ===========================================================================
// Site 4: catalog_view — `decide`'s verdict onto the view's vocabulary
// ===========================================================================

fn valid_verdict(basis: TrustBasis, key_state: TrustKeyState) -> Verdict {
    Verdict {
        result: TrustResult::Valid,
        basis,
        key_state,
        reason: None,
    }
}

/// MUTANT: revert `verification_of_verdict` to `(Valid, Current) => Verified,
/// (Valid, _) => VerifiedHistorical`. The `Unverified`,
/// `RecordedBeforeRevocation` and `None` rows become selectable and fail.
///
/// `decide`'s verdict always carries a basis, so D3 §12's "no block" row does
/// not exist here; `ValidBasis::of_core` never answers `Absent`.
#[test]
fn the_catalog_view_maps_a_valid_verdict_by_an_allow_list() {
    let rows = [
        (
            TrustBasis::Current,
            TrustKeyState::Active,
            Verification::Verified,
        ),
        (
            TrustBasis::Historical,
            TrustKeyState::Retired,
            Verification::VerifiedHistorical,
        ),
        (
            TrustBasis::Unverified,
            TrustKeyState::Active,
            Verification::NotAttempted,
        ),
        (
            TrustBasis::RecordedBeforeRevocation,
            TrustKeyState::Revoked,
            Verification::Revoked,
        ),
        (
            TrustBasis::None,
            TrustKeyState::Active,
            Verification::Invalid,
        ),
    ];
    for (basis, key_state, want) in rows {
        let got = view::verification_of_verdict(&valid_verdict(basis, key_state));
        assert_eq!(got, want, "Valid on {basis:?}");
        assert_eq!(
            got.selectable(),
            matches!(basis, TrustBasis::Current | TrustBasis::Historical),
            "Valid on {basis:?}: selectable"
        );
    }
    // The `Untrusted` rows are unchanged.
    let untrusted = |reason| Verdict {
        result: TrustResult::Untrusted,
        basis: TrustBasis::None,
        key_state: TrustKeyState::Active,
        reason: Some(reason),
    };
    assert_eq!(
        view::verification_of_verdict(&untrusted(UntrustReason::Revoked)),
        Verification::Revoked
    );
    assert_eq!(
        view::verification_of_verdict(&untrusted(UntrustReason::KeyUsageMismatch)),
        Verification::UntrustedSigner
    );
    assert_eq!(
        view::verification_of_verdict(&untrusted(UntrustReason::SignedOutsideValidity)),
        Verification::Invalid
    );
}

// ===========================================================================
// The sweep: the same rule on the catalog's §5.4 word and on a fresh verdict
// ===========================================================================

/// `verification::catalog_verification` read `Valid` as "Historical, else
/// Verified" — the same catch-all. MUTANT: restore it; the `Unverified`,
/// `RecordedBeforeRevocation` and `None` rows read `Verified` and fail.
#[test]
fn the_catalog_word_for_a_valid_verdict_is_an_allow_list() {
    assert_eq!(
        v::catalog_verification(VerificationVerdict::Valid, None).state,
        "Verified",
        "no verdict is D3 §12's absent block"
    );
    for (basis, key_state, want) in [
        (TrustBasis::Current, TrustKeyState::Active, "Verified"),
        (
            TrustBasis::Historical,
            TrustKeyState::Retired,
            "VerifiedHistorical",
        ),
        (
            TrustBasis::Unverified,
            TrustKeyState::Active,
            "NotAttempted",
        ),
        (
            TrustBasis::RecordedBeforeRevocation,
            TrustKeyState::Revoked,
            "Revoked",
        ),
        (TrustBasis::None, TrustKeyState::Active, "UntrustedSigner"),
    ] {
        let verdict = valid_verdict(basis, key_state);
        assert_eq!(
            v::catalog_verification(VerificationVerdict::Valid, Some(&verdict)).state,
            want,
            "Valid on {basis:?}"
        );
    }
}

/// `VerificationResult::is_pass` gates the receipt facts the `Backup`
/// reconciler copies onto a status. A result with no trust projection is D3
/// §12's absent block; only `Valid` passes.
#[test]
fn a_fresh_verification_passes_only_as_valid() {
    let at = Utc.with_ymd_and_hms(2026, 9, 19, 2, 10, 0).unwrap();
    let mut result = v::VerificationResult {
        result: VerificationVerdict::Valid,
        matched_key_id: Some("key-1".to_string()),
        payload_type: "application/vnd.logweir.backup-receipt+json".to_string(),
        verified_at: at,
        detail: None,
        trust: None,
    };
    assert!(result.is_pass());
    for other in [
        VerificationVerdict::Invalid,
        VerificationVerdict::Untrusted,
        VerificationVerdict::NotAttempted,
        VerificationVerdict::Pending,
    ] {
        result.result = other;
        assert!(!result.is_pass(), "{other:?}");
    }
}
