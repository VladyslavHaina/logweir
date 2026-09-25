//! `TRUSTPOLICY-DELETE-DROPS-REVOCATION`, the API half: what `trust.state`
//! and `GET /api/v1/trust-policies` publish once a namespace no longer
//! resolves through the policy that recorded a `KeyCompromise` revocation.
//!
//! The API decides no trust of its own — it projects what the controller
//! wrote — so each row runs the CONTROLLER's pass (`weirkeeper::trust` and
//! `weirkeeper::verification`, the same functions the reconcilers call) and
//! then the API's projection over the object that pass leaves behind. A row
//! that projected a hand-written block would prove only the projection.

use chrono::{DateTime, Utc};
use kube::api::ObjectMeta;
use logweir_api::status::{backup_view, TrustState};
use serde_json::{json, Value};
use weirkeeper::crds::backup::Backup;
use weirkeeper::crds::trust_policy::{
    KeyAlgorithm, KeyPrincipal, KeyState, KeyUsage, RevocationReason, TrustPolicy, TrustPolicySpec,
    TrustedKey,
};
use weirkeeper::crds::trust_roster::{KeyEntry, TrustRosterSpec};
use weirkeeper::trust::resolve_in;
use weirkeeper::verification::{backup_badge, retrust};

const SIGNER_PEM: &str =
    "-----BEGIN PUBLIC KEY-----\nMCowBQYDK2VwAyEApFpEU8uY5S8Lv43HL4DcXKKyM8WHurCPZIxvq8ZBfpY=\n-----END PUBLIC KEY-----\n";
const SIGNER: &str = "f27c7f51aad0700db76887b306d413a039156b44ee147c1d82c5e4dc339558f6";

/// The namespace of the live fixture this file starts from.
const NS: &str = "team-a";

fn at(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s)
        .expect("an instant")
        .with_timezone(&Utc)
}

fn now() -> DateTime<Utc> {
    at("2026-09-21T08:00:00Z")
}

fn key(state: KeyState) -> TrustedKey {
    let revoked = state == KeyState::Revoked;
    TrustedKey {
        key_id: SIGNER.to_string(),
        spki_pem: SIGNER_PEM.to_string(),
        algorithm: KeyAlgorithm::Ed25519,
        usages: vec![KeyUsage::EvidenceSigning],
        principal: KeyPrincipal {
            id: format!("install:{SIGNER}"),
            display: None,
        },
        not_before: at("2026-01-01T00:00:00Z"),
        not_after: at("2027-06-01T00:00:00Z"),
        state,
        retired_at: None,
        revoked_at: revoked.then(|| at("2026-09-20T00:00:00Z")),
        revocation_reason: revoked.then_some(RevocationReason::KeyCompromise),
        revocation_effective_from: revoked.then(|| at("2026-09-20T00:00:00Z")),
    }
}

fn policy(name: &str, namespaces: Option<Vec<String>>, keys: Vec<TrustedKey>) -> TrustPolicy {
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
            namespaces,
            allowed_target_cluster_ids: None,
            keys,
        },
        status: None,
    }
}

fn roster() -> TrustRosterSpec {
    TrustRosterSpec {
        approver_keys: Vec::new(),
        signing_keys: vec![KeyEntry {
            key_id: SIGNER.to_string(),
            spki_pem: SIGNER_PEM.to_string(),
            subject: None,
            not_after: None,
        }],
        allowed_cluster_ids: Vec::new(),
    }
}

/// The live `backup-succeeded-verified.json` object, its signer swapped for
/// [`SIGNER`] — verified at 01:26:16 on 2026-09-19, before the revocation.
fn backup() -> Value {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/backup-succeeded-verified.json"
    );
    let mut object: Value =
        serde_json::from_str(&std::fs::read_to_string(path).expect("the fixture")).expect("JSON");
    object["status"]["evidence"]["verification"]["matchedKeyId"] = json!(SIGNER);
    object
}

/// One controller re-trust pass over `object` against `policies`.
fn pass(object: &Value, policies: &[TrustPolicy]) -> Value {
    let resolution = resolve_in(NS, policies, Some(&roster()));
    let mut next = object.clone();
    if let Some(r) = retrust(
        &object["status"],
        &resolution,
        backup_badge,
        None,
        Some(1),
        now(),
    ) {
        next["status"]["evidence"]["verification"] = r.verification;
    }
    next
}

fn state(object: &Value) -> TrustState {
    let backup: Backup = serde_json::from_value(object.clone()).expect("a Backup");
    backup_view(&backup, now()).trust.state
}

/// `trust.state` stays `untrusted` when the namespace is re-bound away from
/// the recording policy; and the control row shows what the API published at
/// the base commit once nothing recorded the compromise: `verified`.
#[test]
fn trust_state_stays_untrusted_after_the_namespace_leaves_the_recording_policy() {
    let recorder = policy(
        "incident",
        Some(vec![NS.to_string()]),
        vec![key(KeyState::Revoked)],
    );
    let refused = pass(&backup(), std::slice::from_ref(&recorder));
    assert_eq!(state(&refused), TrustState::Untrusted);

    let mut rebound = recorder;
    rebound.spec.namespaces = None;
    let after = pass(&refused, &[rebound]);
    assert_eq!(
        state(&after),
        TrustState::Untrusted,
        "{}",
        after["status"]["evidence"]["verification"]
    );

    // CONTROL: the record gone altogether — the state the base commit reached
    // after `kubectl delete`, which the finalizer now makes unreachable while
    // the roster lists the key.
    let gone = pass(&refused, &[]);
    assert_eq!(state(&gone), TrustState::Verified);
}

/// `GET /api/v1/trust-policies` over a policy that lists the key `Active`
/// while ANOTHER records its compromise: the declared `state` is the policy's
/// own, and `effectiveState` is what the controller decides — `Revoked`, with
/// no usability verdict that could read green.
#[test]
fn the_trust_policy_view_publishes_an_inherited_compromise_as_the_effective_state() {
    let recorder = policy("incident", None, vec![key(KeyState::Revoked)]);
    let lister = policy(
        "team-b",
        Some(vec!["team-b".to_string()]),
        vec![key(KeyState::Active)],
    );
    let all = vec![recorder, lister.clone()];
    let verdict = weirkeeper::controllers::trust_policy::evaluate_in(&lister, &all, None, now());
    let mut evaluated = lister.clone();
    evaluated.status = Some(weirkeeper::controllers::trust_policy::status_for(
        &lister,
        &verdict,
        now(),
    ));
    let view = logweir_api::routes::trust::view(&evaluated, now(), &["team-b".to_string()]);
    let row = view
        .keys
        .iter()
        .find(|k| k.key_id == SIGNER)
        .expect("the key");
    assert_eq!(
        row.state, "Active",
        "the declared state is the policy's own"
    );
    assert_eq!(row.effective_state, "Revoked");
    assert_eq!(row.usable_for_new_signatures, Some(false));
    assert_eq!(row.usable_for_verification.as_deref(), Some("None"));
    let guard = view
        .conditions
        .iter()
        .find(|c| c.type_ == "CompromiseGuard")
        .expect("the CompromiseGuard condition is projected");
    assert_eq!(guard.reason.as_deref(), Some("CompromiseInherited"));
}
