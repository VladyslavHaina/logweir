//! `TRUSTPOLICY-DELETE-DROPS-REVOCATION` — the reproduction, one row per
//! consumer of a trust decision.
//!
//! # The observation (PoC round, rehearsal R2)
//!
//! A `TrustPolicy` revoked the installation signer for `KeyCompromise`; every
//! backup it had signed turned `Untrusted` with basis
//! `RecordedBeforeRevocation`. The policy was then deleted, the namespace fell
//! back to `legacy-roster-v1`, synthesised from a `TrustRoster/default` that
//! still listed the key as an ordinary signing key — and all six backups
//! re-verified `Valid`. The revocation had lived only on the object that was
//! removed.
//!
//! # How each row is built, and why it compiles against the base commit
//!
//! Every row drives ONLY interfaces that existed before the fix (`resolve_in`,
//! `retrust`, the catalog view, the preflight signer row, the runner keyring,
//! the approval seam, the policy status, and `reconcile_policy` over a route
//! table). So this file was run unchanged at `02dc44b6` to record what each
//! consumer said there (the result file quotes the failures), and it is the
//! regression suite now. Nothing here assumes HOW the fix works: the
//! "deletion" row asks the reconciler what it did to the policy, and models
//! `kubectl delete` from that — an object the reconciler put a finalizer on
//! survives the delete with a `deletionTimestamp`, an object it did not is
//! gone.
//!
//! Two ways a namespace stops resolving through the recording policy, both
//! observed or one edit away:
//!
//! * **deleted** — `kubectl delete trustpolicy <name>` (R2's rollback step, and
//!   the delete-and-re-create remedy for a policy applied with a wrong
//!   immutable field);
//! * **re-bound** — the namespace removed from `spec.namespaces`, so it falls
//!   to the roster. No finalizer can see this one.
//!
//! and one way a key the cluster knows is compromised is listed as trusted
//! somewhere else: **another policy** — a successor applied from an export
//! taken before the revocation — listing it `Active`.

use chrono::{DateTime, Duration, Utc};
use kube::api::ObjectMeta;
use logweir_core::check_contract::{CheckCode, CheckState};
use logweir_core::trust::{EvidenceClaim, IndependentObservation, KeyUsage, TrustResult};
use serde_json::{json, Value};
use weirkeeper::catalog_view::{classify_verification, SignatureVerdict, TrustView, Verification};
use weirkeeper::controllers::preflight::{signer_rostered_row, RosterFacts};
use weirkeeper::crds::preflight::PreflightOperation;
use weirkeeper::crds::trust_policy::{
    KeyAlgorithm, KeyPrincipal, KeyState, KeyUsage as SpecUsage, RevocationReason, TrustPolicy,
    TrustPolicySpec, TrustedKey,
};
use weirkeeper::crds::trust_roster::{KeyEntry, TrustRosterSpec};
use weirkeeper::testing::Route;
use weirkeeper::trust::{resolve_in, Resolution};
use weirkeeper::verification::{backup_badge, restore_badge, retrust};

// ---------------------------------------------------------------------------
// Public material. The same two PUBLIC halves `trust_policy_controller.rs`
// uses; no private half is written down anywhere (`check-one-signer.sh`).
// ---------------------------------------------------------------------------

/// The installation signer — the key R2 revoked. Ed25519, SPKI PEM.
const SIGNER_PEM: &str =
    "-----BEGIN PUBLIC KEY-----\nMCowBQYDK2VwAyEApFpEU8uY5S8Lv43HL4DcXKKyM8WHurCPZIxvq8ZBfpY=\n-----END PUBLIC KEY-----\n";
/// `sha256(SIGNER_PEM's SPKI DER)`.
const SIGNER: &str = "f27c7f51aad0700db76887b306d413a039156b44ee147c1d82c5e4dc339558f6";

/// An approver key, revoked for compromise in the approval rows.
const APPROVER_PEM: &str =
    "-----BEGIN PUBLIC KEY-----\nMCowBQYDK2VwAyEAKTrTSpPTt1d9M1kMim3Imkt2s1OjRm48GfqVh+fjvyk=\n-----END PUBLIC KEY-----\n";
/// `sha256(APPROVER_PEM's SPKI DER)`.
const APPROVER: &str = "067bf4d360d3c0658620a75a225d4d3e2e038cdb12cdf38ef6611241b9d380d2";

/// The namespace the recording policy governed.
const NS: &str = "lw-team-a";
/// A namespace another policy governs.
const OTHER_NS: &str = "lw-team-b";
/// The recording policy.
const RECORDER: &str = "incident-2026-09";

fn at(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s)
        .expect("a fixture instant")
        .with_timezone(&Utc)
}

/// When every row is asked.
fn now() -> DateTime<Utc> {
    at("2026-09-12T08:00:00Z")
}

/// The compromise revocation's instants.
const REVOKED_AT: &str = "2026-09-10T12:00:00Z";

// ---------------------------------------------------------------------------
// Builders
// ---------------------------------------------------------------------------

fn active(key_id: &str, pem: &str, usage: SpecUsage) -> TrustedKey {
    TrustedKey {
        key_id: key_id.to_string(),
        spki_pem: pem.to_string(),
        algorithm: KeyAlgorithm::Ed25519,
        usages: vec![usage],
        principal: KeyPrincipal {
            id: format!("install:{key_id}"),
            display: None,
        },
        not_before: at("2026-01-01T00:00:00Z"),
        not_after: at("2027-06-01T00:00:00Z"),
        state: KeyState::Active,
        retired_at: None,
        revoked_at: None,
        revocation_reason: None,
        revocation_effective_from: None,
    }
}

fn compromised(key: TrustedKey) -> TrustedKey {
    TrustedKey {
        state: KeyState::Revoked,
        revoked_at: Some(at(REVOKED_AT)),
        revocation_reason: Some(RevocationReason::KeyCompromise),
        revocation_effective_from: Some(at(REVOKED_AT)),
        ..key
    }
}

fn policy(name: &str, namespaces: &[&str], keys: Vec<TrustedKey>) -> TrustPolicy {
    TrustPolicy {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            uid: Some(format!("uid-{name}")),
            generation: Some(2),
            resource_version: Some("17".to_string()),
            ..ObjectMeta::default()
        },
        spec: TrustPolicySpec {
            default: false,
            namespaces: (!namespaces.is_empty())
                .then(|| namespaces.iter().map(|n| (*n).to_string()).collect()),
            allowed_target_cluster_ids: Some(vec!["scratch-cluster-id".to_string()]),
            keys,
        },
        status: None,
    }
}

/// The recording policy: it governs [`NS`] and revokes both keys for
/// compromise.
fn recorder() -> TrustPolicy {
    policy(
        RECORDER,
        &[NS],
        vec![
            compromised(active(SIGNER, SIGNER_PEM, SpecUsage::EvidenceSigning)),
            compromised(active(APPROVER, APPROVER_PEM, SpecUsage::GovernedApproval)),
        ],
    )
}

/// `TrustRoster/default` as R2 had it: the signer and the approver listed as
/// ordinary keys, because the roster has no lifecycle to say otherwise.
fn roster() -> TrustRosterSpec {
    let entry = |key_id: &str, pem: &str| KeyEntry {
        key_id: key_id.to_string(),
        spki_pem: pem.to_string(),
        subject: None,
        not_after: None,
    };
    TrustRosterSpec {
        approver_keys: vec![entry(APPROVER, APPROVER_PEM)],
        signing_keys: vec![entry(SIGNER, SIGNER_PEM)],
        allowed_cluster_ids: vec!["scratch-cluster-id".to_string()],
    }
}

/// A Backup verified by an earlier reconcile at 10:05, BEFORE the
/// revocation took effect at 12:00 — the controller-written observation D3
/// §7.4's `RecordedBeforeRevocation` row reads.
fn verified_backup() -> Value {
    json!({
        "phase": "Succeeded",
        "exitCode": 0,
        "evidence": {"verification": {
            "result": "Valid",
            "matchedKeyId": SIGNER,
            "payloadType": logweir_verify::PAYLOAD_TYPE_SCORECARD,
            "signedAt": "2026-09-10T10:00:00Z",
            "verifiedAt": "2026-09-10T10:05:00Z",
            "trust": {"basis": "Current", "keyState": "Active",
                      "policy": {"name": RECORDER, "uid": format!("uid-{RECORDER}"), "generation": 1}},
        }},
    })
}

/// The same, as a Restore (`outcome: pass`).
fn verified_restore() -> Value {
    let mut status = verified_backup();
    status["outcome"] = json!("pass");
    status
}

/// Apply one re-trust pass to `status`, or keep it when the pass sends nothing.
fn after(
    status: &Value,
    resolution: &Resolution,
    badge: fn(&Value) -> weirkeeper::verification::Badge,
) -> Value {
    match retrust(status, resolution, badge, None, Some(2), now()) {
        Some(r) => {
            let mut next = status.clone();
            next["evidence"]["verification"] = r.verification;
            next
        }
        None => status.clone(),
    }
}

fn trust_of(resolution: &Resolution) -> &weirkeeper::trust::ResolvedTrust {
    match resolution {
        Resolution::Trust(t) => t,
        other => panic!("expected resolved trust; got {other:?}"),
    }
}

/// What `kubectl delete trustpolicy <RECORDER>` leaves behind, decided by
/// what the reconciler did to the object first.
///
/// The reconciler is run over a route table that answers every read and
/// write it could make. If it placed a finalizer on the policy (a `PATCH` of
/// the object itself, not `/status`, whose body names `finalizers`), the API
/// server keeps the object after the delete with a `deletionTimestamp`; if it
/// did not, the object is gone.
async fn list_after_kubectl_delete() -> Vec<TrustPolicy> {
    let p = recorder();
    let object = serde_json::to_string(&p).expect("serialises");
    let roster_object = json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "TrustRoster",
        "metadata": {"name": "default", "uid": "uid-roster", "generation": 1, "resourceVersion": "41"},
        "spec": serde_json::to_value(roster()).expect("roster"),
    })
    .to_string();
    let list = json!({"apiVersion": "logweir.dev/v1alpha1", "kind": "TrustPolicyList",
                      "metadata": {"resourceVersion": "1"}, "items": [p]})
    .to_string();
    let (client, _recorder, bodies) = weirkeeper::testing::mock_client_recording_bodies(vec![
        Route {
            method: "GET",
            path_suffix: "/trustpolicies",
            status: 200,
            body: list,
        },
        Route {
            method: "PATCH",
            path_suffix: "/trustpolicies/incident-2026-09/status",
            status: 200,
            body: object.clone(),
        },
        Route {
            method: "PATCH",
            path_suffix: "/trustpolicies/incident-2026-09",
            status: 200,
            body: object,
        },
        Route {
            method: "GET",
            path_suffix: "/trustrosters/default",
            status: 200,
            body: roster_object.clone(),
        },
        Route {
            method: "PATCH",
            path_suffix: "/trustrosters/default/status",
            status: 200,
            body: roster_object,
        },
    ]);
    weirkeeper::controllers::trust_policy::reconcile_policy(&p, &client)
        .await
        .expect("the reconcile completes");
    let finalizers: Vec<String> = bodies
        .lock()
        .expect("bodies")
        .iter()
        .filter(|b| {
            b.method == "PATCH"
                && b.uri
                    .split('?')
                    .next()
                    .is_some_and(|u| u.ends_with(&format!("/trustpolicies/{RECORDER}")))
        })
        .filter_map(|b| serde_json::from_str::<Value>(&b.body).ok())
        .filter_map(|v| v.pointer("/metadata/finalizers").cloned())
        .filter_map(|f| serde_json::from_value::<Vec<String>>(f).ok())
        .next_back()
        .unwrap_or_default();
    if finalizers.is_empty() {
        // Nothing holds it: the API server removes it at once.
        return Vec::new();
    }
    let mut held = p;
    held.metadata.finalizers = Some(finalizers);
    held.metadata.deletion_timestamp = Some(k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(
        now() - Duration::hours(1),
    ));
    vec![held]
}

/// The recording policy with [`NS`] removed from `spec.namespaces`.
fn rebound() -> Vec<TrustPolicy> {
    let mut p = recorder();
    p.spec.namespaces = None;
    vec![p]
}

/// Both at once: the namespace re-bound away first, then `kubectl delete` on
/// the recording policy. Whatever the delete leaves behind no longer governs
/// [`NS`], so the namespace resolves through the roster — and a record that
/// is only honoured while it GOVERNS would be lost here even with the object
/// held.
async fn rebound_then_deleted() -> Vec<TrustPolicy> {
    list_after_kubectl_delete()
        .await
        .into_iter()
        .map(|mut p| {
            p.spec.namespaces = None;
            p
        })
        .collect()
}

// ---------------------------------------------------------------------------
// The baseline: while the recording policy governs, every consumer refuses
// ---------------------------------------------------------------------------

/// The state R2 recorded before the deletion, reproduced: `Untrusted`,
/// `RecordedBeforeRevocation`, never green. Every other row starts here.
#[test]
fn baseline_while_the_recording_policy_governs_the_key_is_refused() {
    let res = resolve_in(NS, &[recorder()], Some(&roster()));
    let backup = after(&verified_backup(), &res, backup_badge);
    assert_eq!(backup["evidence"]["verification"]["result"], "Untrusted");
    assert_eq!(
        backup["evidence"]["verification"]["trust"]["basis"],
        "RecordedBeforeRevocation"
    );
    assert!(!backup_badge(&backup).green);
}

// ---------------------------------------------------------------------------
// The rows. Each asserts the FIXED behaviour: nothing the compromised key
// signed or authorises reads as trusted. At the base commit each failed.
// ---------------------------------------------------------------------------

/// **Evidence re-trust, Backup** — the row R2 observed: the six backups.
#[tokio::test]
async fn a_backup_verdict_stays_refused_after_the_recording_policy_is_deleted() {
    let before = resolve_in(NS, &[recorder()], Some(&roster()));
    let refused = after(&verified_backup(), &before, backup_badge);
    for (scenario, policies) in [
        ("deleted", list_after_kubectl_delete().await),
        ("re-bound", rebound()),
        ("re-bound, then deleted", rebound_then_deleted().await),
    ] {
        let res = resolve_in(NS, &policies, Some(&roster()));
        let now_status = after(&refused, &res, backup_badge);
        let v = &now_status["evidence"]["verification"];
        assert_eq!(
            v["result"], "Untrusted",
            "{scenario}: the compromised key's backups re-verified {v}"
        );
        assert_eq!(
            v["trust"]["basis"], "RecordedBeforeRevocation",
            "{scenario}: {v}"
        );
        assert!(!backup_badge(&now_status).green, "{scenario}: green again");
    }
}

/// **Evidence re-trust, Restore** — the same rule on the other badge.
#[tokio::test]
async fn a_restore_verdict_stays_refused_after_the_recording_policy_is_deleted() {
    let before = resolve_in(NS, &[recorder()], Some(&roster()));
    let refused = after(&verified_restore(), &before, restore_badge);
    for (scenario, policies) in [
        ("deleted", list_after_kubectl_delete().await),
        ("re-bound", rebound()),
        ("re-bound, then deleted", rebound_then_deleted().await),
    ] {
        let res = resolve_in(NS, &policies, Some(&roster()));
        let now_status = after(&refused, &res, restore_badge);
        assert!(
            !restore_badge(&now_status).green,
            "{scenario}: {}",
            now_status["evidence"]["verification"]
        );
        assert_eq!(
            now_status["evidence"]["verification"]["result"], "Untrusted",
            "{scenario}"
        );
    }
}

/// **Fresh evidence** — a document signed AFTER the deletion (a forged
/// receipt, or a run still using the compromised key) verified by a new
/// evidence fetch: the verdict `verify_fetched` reaches is this seam's.
#[tokio::test]
async fn new_evidence_signed_by_the_key_is_refused_after_deletion() {
    for (scenario, policies) in [
        ("deleted", list_after_kubectl_delete().await),
        ("re-bound", rebound()),
        ("re-bound, then deleted", rebound_then_deleted().await),
    ] {
        let res = resolve_in(NS, &policies, Some(&roster()));
        let verdict = trust_of(&res).decide_for(
            SIGNER,
            KeyUsage::EvidenceSigning,
            &EvidenceClaim::at(now() - Duration::hours(1)),
            &IndependentObservation::none(),
            now(),
        );
        assert_eq!(
            verdict.result,
            TrustResult::Untrusted,
            "{scenario}: a new document signed by the compromised key is {verdict:?}"
        );
    }
}

/// **Recovery catalog** — the points the view lists as restorable.
#[tokio::test]
async fn the_catalog_never_lists_the_keys_points_verified_after_deletion() {
    for (scenario, policies) in [
        ("deleted", list_after_kubectl_delete().await),
        ("re-bound", rebound()),
        ("re-bound, then deleted", rebound_then_deleted().await),
    ] {
        let view = TrustView::from_resolution(&resolve_in(NS, &policies, Some(&roster())));
        let verdict = classify_verification(
            SignatureVerdict::Verified,
            Some(SIGNER),
            Some(at("2026-09-10T10:00:00Z")),
            &view,
            now(),
        );
        assert_eq!(
            verdict,
            Verification::Revoked,
            "{scenario}: the catalog view classifies the compromised signer's point {verdict:?}"
        );
        assert!(
            !weirkeeper::catalog_view::accepts(&view, SIGNER),
            "{scenario}: status.signers[].trusted would read true"
        );
    }
}

/// **Rehearsal / restore runner** — the evidence keyring the runner verifies
/// a bound point's receipt against. A key RENDERED there must carry its
/// revocation, or the runner accepts a point the controller refuses.
#[tokio::test]
async fn the_runner_keyring_carries_the_revocation_after_deletion() {
    for (scenario, policies) in [
        ("deleted", list_after_kubectl_delete().await),
        ("re-bound", rebound()),
        ("re-bound, then deleted", rebound_then_deleted().await),
    ] {
        let res = resolve_in(NS, &policies, Some(&roster()));
        let bytes = weirkeeper::controllers::restore::evidence_keyring_bytes(trust_of(&res))
            .expect("the keyring renders");
        let keyring: Value = serde_json::from_str(&bytes).expect("JSON");
        let signer = keyring["keys"]
            .as_array()
            .expect("keys")
            .iter()
            .find(|k| k["trust"]["key_id"] == SIGNER)
            .unwrap_or_else(|| panic!("{scenario}: the signer is rendered: {keyring}"));
        assert_eq!(
            signer["trust"]["state"], "Revoked",
            "{scenario}: the runner is handed the compromised key as {signer}"
        );
        assert_eq!(
            signer["trust"]["revocation_reason"], "KeyCompromise",
            "{scenario}"
        );
    }
}

/// **New approvals** — an approval signed by the compromised APPROVER key
/// after the deletion. Admission is a new use (`may_sign_new`).
#[tokio::test]
async fn a_new_approval_by_the_compromised_approver_is_refused_after_deletion() {
    for (scenario, policies) in [
        ("deleted", list_after_kubectl_delete().await),
        ("re-bound", rebound()),
        ("re-bound, then deleted", rebound_then_deleted().await),
    ] {
        let res = resolve_in(NS, &policies, Some(&roster()));
        let answer = trust_of(&res).may_sign_new_for(APPROVER, KeyUsage::GovernedApproval, now());
        assert_eq!(
            answer,
            Err(logweir_core::trust::SigningRefusal::KeyRevoked),
            "{scenario}: a fresh approval under the compromised approver key is {answer:?}"
        );
    }
}

/// **Readiness** — `signer.rostered` for a Backup preflight whose runner
/// still holds the compromised key.
#[tokio::test]
async fn the_signer_readiness_row_refuses_the_key_after_deletion() {
    for (scenario, policies) in [
        ("deleted", list_after_kubectl_delete().await),
        ("re-bound", rebound()),
        ("re-bound, then deleted", rebound_then_deleted().await),
    ] {
        let resolution = resolve_in(NS, &policies, Some(&roster()));
        let facts = RosterFacts {
            found: true,
            uid: "uid-roster".to_string(),
            generation: 1,
            signing_keys: vec![(SIGNER.to_string(), None)],
            approver_keys: vec![(APPROVER.to_string(), None)],
            allowed_cluster_ids: vec!["scratch-cluster-id".to_string()],
            ..RosterFacts::default()
        }
        .with_resolution(Ok(&resolution));
        let row = signer_rostered_row(PreflightOperation::Backup, &facts, Some(SIGNER), now());
        assert_eq!(
            row.state,
            CheckState::NotReady,
            "{scenario}: signer.rostered reads {:?}/{:?}",
            row.state,
            row.code
        );
        assert_ne!(row.code, CheckCode::SignerRostered, "{scenario}");
    }
}

/// **Keys view / API `trust-policies`** — a SECOND policy (a successor applied
/// from an export taken before the revocation) lists the key `Active`. Its
/// own status is what the keys page's `EVALUATION` column and the API's
/// `effectiveState` render; `Active` there is a green badge over a key this
/// cluster knows is compromised.
#[test]
fn another_policy_listing_the_key_active_evaluates_it_revoked() {
    let successor = policy(
        "org-default",
        &[OTHER_NS],
        vec![active(SIGNER, SIGNER_PEM, SpecUsage::EvidenceSigning)],
    );
    let all = vec![recorder(), successor.clone()];
    let verdict = weirkeeper::controllers::trust_policy::evaluate(&successor, &all, now());
    let key = verdict
        .keys
        .iter()
        .find(|k| k.key_id == SIGNER)
        .expect("the key is reported");
    assert_eq!(
        key.effective_state, "Revoked",
        "the successor's status reports the compromised key as {key:?}"
    );
    assert_eq!(key.usable_for_new_signatures, Some(false));
    assert_eq!(key.usable_for_verification.as_deref(), Some("None"));

    // And the namespace it governs refuses the key's evidence.
    let res = resolve_in(OTHER_NS, &all, Some(&roster()));
    let backup = after(&verified_backup(), &res, backup_badge);
    assert_eq!(
        backup["evidence"]["verification"]["result"], "Untrusted",
        "{backup}"
    );
    assert!(!backup_badge(&backup).green);
}

/// **NEGATIVE CONTROL.** A key nobody revoked is untouched by any of this: the
/// same builders, the recording policy revoking a DIFFERENT key, and every
/// consumer still says what it said before. A fix that refused everything
/// would pass the rows above and fail here.
#[test]
fn a_key_nobody_revoked_still_verifies_everywhere() {
    let unrelated = policy(
        RECORDER,
        &[NS],
        vec![
            active(SIGNER, SIGNER_PEM, SpecUsage::EvidenceSigning),
            compromised(active(APPROVER, APPROVER_PEM, SpecUsage::GovernedApproval)),
        ],
    );
    for policies in [vec![unrelated.clone()], {
        let mut p = unrelated.clone();
        p.spec.namespaces = None;
        vec![p]
    }] {
        let res = resolve_in(NS, &policies, Some(&roster()));
        let backup = after(&verified_backup(), &res, backup_badge);
        assert_eq!(
            backup["evidence"]["verification"]["result"], "Valid",
            "{backup}"
        );
        assert!(backup_badge(&backup).green);
        let view = TrustView::from_resolution(&res);
        assert_eq!(
            classify_verification(
                SignatureVerdict::Verified,
                Some(SIGNER),
                Some(at("2026-09-10T10:00:00Z")),
                &view,
                now(),
            ),
            Verification::Verified
        );
    }
}
