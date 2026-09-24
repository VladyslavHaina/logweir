//! `TRUSTPOLICY-DELETE-DROPS-REVOCATION` — the durability guard itself: the
//! cluster-wide compromise overlay, the deletion guard, the finalizer the
//! `TrustPolicy` reconciler holds, and the `CompromiseGuard` condition.
//!
//! `trust_revocation_consumers.rs` is the reproduction, one row per consumer;
//! this file pins the mechanism those rows depend on, so each of its mutants
//! has a named row that kills it (the result file lists them).

use chrono::{DateTime, Duration, Utc};
use kube::api::ObjectMeta;
use serde_json::{json, Value};
use weirkeeper::controllers::trust_policy::{
    self, finalizer_action, finalizer_patch, FinalizerAction, COMPROMISE_FINALIZER,
    CONDITION_COMPROMISE_GUARD, REASON_COMPROMISE_INHERITED, REASON_COMPROMISE_RECORDED,
    REASON_DELETION_BLOCKED, REASON_NO_COMPROMISE_RECORDED,
};
use weirkeeper::crds::trust_policy::{
    KeyAlgorithm, KeyPrincipal, KeyState, KeyUsage as SpecUsage, RevocationReason, TrustPolicy,
    TrustPolicySpec, TrustedKey,
};
use weirkeeper::crds::trust_roster::{KeyEntry, TrustRosterSpec};
use weirkeeper::testing::{mock_client_recording_bodies, Route, SeenBody};
use weirkeeper::trust::{
    compromise_records, deletion_guard, resolve_in, Resolution, ResolvedTrust, ROSTER_SOURCE,
};

const SIGNER_PEM: &str =
    "-----BEGIN PUBLIC KEY-----\nMCowBQYDK2VwAyEApFpEU8uY5S8Lv43HL4DcXKKyM8WHurCPZIxvq8ZBfpY=\n-----END PUBLIC KEY-----\n";
const SIGNER: &str = "f27c7f51aad0700db76887b306d413a039156b44ee147c1d82c5e4dc339558f6";
const OTHER_PEM: &str =
    "-----BEGIN PUBLIC KEY-----\nMCowBQYDK2VwAyEAKTrTSpPTt1d9M1kMim3Imkt2s1OjRm48GfqVh+fjvyk=\n-----END PUBLIC KEY-----\n";
const OTHER: &str = "067bf4d360d3c0658620a75a225d4d3e2e038cdb12cdf38ef6611241b9d380d2";

const NS: &str = "lw-team-a";

fn at(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s)
        .expect("a fixture instant")
        .with_timezone(&Utc)
}

fn now() -> DateTime<Utc> {
    at("2026-09-12T08:00:00Z")
}

fn active(key_id: &str, pem: &str) -> TrustedKey {
    TrustedKey {
        key_id: key_id.to_string(),
        spki_pem: pem.to_string(),
        algorithm: KeyAlgorithm::Ed25519,
        usages: vec![SpecUsage::EvidenceSigning],
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

fn revoked(key: TrustedKey, reason: RevocationReason, effective: &str) -> TrustedKey {
    TrustedKey {
        state: KeyState::Revoked,
        revoked_at: Some(at(effective)),
        revocation_reason: Some(reason),
        revocation_effective_from: Some(at(effective)),
        ..key
    }
}

fn compromised(effective: &str) -> TrustedKey {
    revoked(
        active(SIGNER, SIGNER_PEM),
        RevocationReason::KeyCompromise,
        effective,
    )
}

fn policy(name: &str, namespaces: &[&str], keys: Vec<TrustedKey>) -> TrustPolicy {
    TrustPolicy {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            uid: Some(format!("uid-{name}")),
            generation: Some(3),
            resource_version: Some("17".to_string()),
            ..ObjectMeta::default()
        },
        spec: TrustPolicySpec {
            default: false,
            namespaces: (!namespaces.is_empty())
                .then(|| namespaces.iter().map(|n| (*n).to_string()).collect()),
            allowed_target_cluster_ids: None,
            keys,
        },
        status: None,
    }
}

fn deleting(mut p: TrustPolicy, finalizers: &[&str]) -> TrustPolicy {
    p.metadata.deletion_timestamp = Some(k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(
        now() - Duration::minutes(5),
    ));
    p.metadata.finalizers = Some(finalizers.iter().map(|f| (*f).to_string()).collect());
    p
}

fn with_finalizers(mut p: TrustPolicy, finalizers: &[&str]) -> TrustPolicy {
    p.metadata.finalizers = Some(finalizers.iter().map(|f| (*f).to_string()).collect());
    p
}

fn roster_listing(ids: &[(&str, &str)]) -> TrustRosterSpec {
    TrustRosterSpec {
        approver_keys: Vec::new(),
        signing_keys: ids
            .iter()
            .map(|(id, pem)| KeyEntry {
                key_id: (*id).to_string(),
                spki_pem: (*pem).to_string(),
                subject: None,
                not_after: None,
            })
            .collect(),
        allowed_cluster_ids: Vec::new(),
    }
}

fn trust(resolution: Resolution) -> ResolvedTrust {
    match resolution {
        Resolution::Trust(t) => *t,
        other => panic!("expected trust; got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// The overlay
// ---------------------------------------------------------------------------

/// A compromise recorded ANYWHERE reaches the key whichever source answers:
/// the roster (the namespace was re-bound away), and another policy that
/// lists the key `Active`. The instant is the recorder's, and the key names
/// who recorded it.
///
/// KILLS M4 ("`resolve_in` does not apply the records"): both halves read
/// `Active`.
#[test]
fn a_compromise_recorded_anywhere_reaches_every_source() {
    let recorder = policy("incident", &[], vec![compromised("2026-09-10T12:00:00Z")]);
    let other = policy("team-b", &["lw-team-b"], vec![active(SIGNER, SIGNER_PEM)]);
    let all = vec![recorder, other];
    let roster = roster_listing(&[(SIGNER, SIGNER_PEM)]);
    for (ns, source) in [(NS, "legacy-roster-v1"), ("lw-team-b", "team-b")] {
        let t = trust(resolve_in(ns, &all, Some(&roster)));
        assert_eq!(t.source.name(), source);
        let key = t.key(SIGNER).expect("listed");
        assert_eq!(
            key.trust.state,
            logweir_core::trust::KeyState::Revoked,
            "{ns}"
        );
        assert_eq!(
            key.trust.revocation_reason,
            Some(logweir_core::trust::RevocationReason::KeyCompromise),
            "{ns}"
        );
        assert_eq!(
            key.trust.revocation_effective_from,
            Some(at("2026-09-10T12:00:00Z")),
            "{ns}"
        );
        assert_eq!(
            key.compromise_inherited_from,
            vec!["incident".to_string()],
            "{ns}"
        );
    }
    // A key the answering source does not list stays unlisted: the overlay
    // never ADDS trust material, it only revokes what is there.
    let t = trust(resolve_in(
        "lw-team-b",
        &[
            policy("incident", &[], vec![compromised("2026-09-10T12:00:00Z")]),
            policy("team-b", &["lw-team-b"], vec![active(OTHER, OTHER_PEM)]),
        ],
        None,
    ));
    assert!(t.key(SIGNER).is_none());
}

/// A cluster with ONE record reads byte-for-byte as it did before the overlay
/// existed — so no object's stored verdict moves on upgrade — and two records
/// of one compromise take the EARLIEST instant.
#[test]
fn one_record_is_left_alone_and_two_take_the_earliest_instant() {
    let own = policy("incident", &[NS], vec![compromised("2026-09-10T12:00:00Z")]);
    let before = weirkeeper::trust::from_policy(&own);
    let after = trust(resolve_in(NS, std::slice::from_ref(&own), None));
    assert_eq!(before.keys, after.keys, "a lone record is not rewritten");

    let earlier = policy(
        "second-look",
        &[],
        vec![compromised("2026-09-09T06:00:00Z")],
    );
    let t = trust(resolve_in(NS, &[own, earlier], None));
    let key = t.key(SIGNER).expect("listed");
    assert_eq!(
        key.trust.revocation_effective_from,
        Some(at("2026-09-09T06:00:00Z")),
        "two records of one compromise: the earlier instant separates fewer observations"
    );
    assert!(
        key.compromise_inherited_from.is_empty(),
        "the answering policy records it itself, so it inherits nothing"
    );
    let records = compromise_records(&[
        policy("a", &[], vec![compromised("2026-09-10T12:00:00Z")]),
        policy("b", &[], vec![compromised("2026-09-09T06:00:00Z")]),
    ]);
    assert_eq!(records[SIGNER].recorded_by, vec!["a", "b"]);
}

/// A supersession is a supersession: only `KeyCompromise` is carried across
/// sources, because only a compromise is a fact about the key material.
#[test]
fn a_superseded_revocation_is_not_carried_across_sources() {
    let superseded = policy(
        "rotation",
        &[],
        vec![revoked(
            active(SIGNER, SIGNER_PEM),
            RevocationReason::Superseded,
            "2026-09-10T12:00:00Z",
        )],
    );
    let t = trust(resolve_in(
        NS,
        &[superseded],
        Some(&roster_listing(&[(SIGNER, SIGNER_PEM)])),
    ));
    assert_eq!(
        t.key(SIGNER).expect("listed").trust.state,
        logweir_core::trust::KeyState::Active
    );
}

// ---------------------------------------------------------------------------
// The deletion guard
// ---------------------------------------------------------------------------

/// The table `deletion_guard` decides, row by row.
///
/// KILLS M1 ("a terminating carrier counts"): row (c) releases both.
/// KILLS M2 ("the roster is not consulted"): row (a) releases.
#[test]
fn the_deletion_guard_releases_only_what_the_cluster_would_still_know() {
    let record = compromised("2026-09-10T12:00:00Z");
    let dying = deleting(
        policy("incident", &[NS], vec![record.clone()]),
        &[COMPROMISE_FINALIZER],
    );
    let roster = roster_listing(&[(SIGNER, SIGNER_PEM)]);

    // (a) the only record, and the roster still lists the key: HELD.
    let g = deletion_guard(&dying, std::slice::from_ref(&dying), Some(&roster));
    assert!(g.guards_anything());
    assert!(!g.releasable(), "{g:?}");
    assert_eq!(g.held[0].still_listed_by, vec![ROSTER_SOURCE.to_string()]);

    // (b) a live successor records the same compromise: RELEASED.
    let successor = policy("incident-v2", &[NS], vec![record.clone()]);
    let g = deletion_guard(&dying, &[dying.clone(), successor.clone()], Some(&roster));
    assert!(g.releasable(), "{g:?}");
    assert_eq!(g.held[0].carried_by, vec!["incident-v2".to_string()]);

    // (c) the successor is being deleted too: neither carries the other.
    let dying_successor = deleting(successor.clone(), &[COMPROMISE_FINALIZER]);
    let both = vec![dying.clone(), dying_successor.clone()];
    assert!(!deletion_guard(&dying, &both, Some(&roster)).releasable());
    assert!(!deletion_guard(&dying_successor, &both, Some(&roster)).releasable());

    // (d) nothing lists the key any more: RELEASED, and an older controller
    // reading only the roster agrees.
    assert!(deletion_guard(&dying, std::slice::from_ref(&dying), None).releasable());
    assert!(deletion_guard(
        &dying,
        std::slice::from_ref(&dying),
        Some(&roster_listing(&[(OTHER, OTHER_PEM)]))
    )
    .releasable());

    // (e) another policy lists the key as trusted: HELD, and named.
    let lists_it = policy("team-b", &["lw-team-b"], vec![active(SIGNER, SIGNER_PEM)]);
    let g = deletion_guard(&dying, &[dying.clone(), lists_it], None);
    assert!(!g.releasable());
    assert_eq!(
        g.held[0].still_listed_by,
        vec!["TrustPolicy/team-b".to_string()]
    );

    // (f) a policy recording nothing guards nothing.
    let plain = deleting(
        policy("plain", &[NS], vec![active(SIGNER, SIGNER_PEM)]),
        &[COMPROMISE_FINALIZER],
    );
    let g = deletion_guard(&plain, std::slice::from_ref(&plain), Some(&roster));
    assert!(!g.guards_anything());
    assert!(g.releasable());
}

/// `finalizer_action`, every cell.
///
/// KILLS M3 ("never place the finalizer"): the first row.
#[test]
fn the_finalizer_is_placed_held_and_released_by_the_guard() {
    let roster = roster_listing(&[(SIGNER, SIGNER_PEM)]);
    let record = policy("incident", &[NS], vec![compromised("2026-09-10T12:00:00Z")]);
    let guard = |p: &TrustPolicy, all: &[TrustPolicy]| deletion_guard(p, all, Some(&roster));

    // Not deleting, recording a compromise, no finalizer yet → Add.
    assert_eq!(
        finalizer_action(&record, &guard(&record, std::slice::from_ref(&record))),
        FinalizerAction::Add
    );
    // Already held → nothing to do.
    let held = with_finalizers(record.clone(), &[COMPROMISE_FINALIZER]);
    assert_eq!(
        finalizer_action(&held, &guard(&held, std::slice::from_ref(&held))),
        FinalizerAction::None
    );
    // Deleting, the only record, roster lists it → Hold.
    let dying = deleting(record.clone(), &[COMPROMISE_FINALIZER]);
    assert_eq!(
        finalizer_action(&dying, &guard(&dying, std::slice::from_ref(&dying))),
        FinalizerAction::Hold
    );
    // Deleting, carried by a live successor → Release.
    let successor = policy(
        "incident-v2",
        &[],
        vec![compromised("2026-09-10T12:00:00Z")],
    );
    assert_eq!(
        finalizer_action(&dying, &guard(&dying, &[dying.clone(), successor])),
        FinalizerAction::Release
    );
    // Deleting WITHOUT the finalizer: the API server refuses a new one, so
    // nothing is attempted (the residual the docs name).
    let unguarded = deleting(record.clone(), &[]);
    assert_eq!(
        finalizer_action(
            &unguarded,
            &guard(&unguarded, std::slice::from_ref(&unguarded))
        ),
        FinalizerAction::None
    );
    // A policy recording nothing is never given one.
    let plain = policy("plain", &[NS], vec![active(SIGNER, SIGNER_PEM)]);
    assert_eq!(
        finalizer_action(&plain, &guard(&plain, std::slice::from_ref(&plain))),
        FinalizerAction::None
    );
}

/// **The one non-status body this controller sends to `trustpolicies`** is
/// metadata only: name, the resourceVersion precondition, and the WHOLE
/// finalizer list (another controller's finalizer carried forward). A `spec`
/// or `status` key here would be the controller editing trust.
#[test]
fn the_finalizer_patch_is_metadata_and_nothing_else() {
    let meta = ObjectMeta {
        name: Some("incident".to_string()),
        resource_version: Some("18".to_string()),
        ..ObjectMeta::default()
    };
    let body = finalizer_patch(
        &meta,
        "incident",
        &[
            "example.com/keep".to_string(),
            COMPROMISE_FINALIZER.to_string(),
        ],
    )
    .expect("an object with a resourceVersion");
    assert_eq!(
        body,
        json!({"metadata": {"name": "incident", "resourceVersion": "18",
                            "finalizers": ["example.com/keep", COMPROMISE_FINALIZER]}})
    );
    assert!(finalizer_patch(&ObjectMeta::default(), "incident", &[]).is_err());
}

// ---------------------------------------------------------------------------
// The reconciler over a route table
// ---------------------------------------------------------------------------

fn roster_object(spec: &TrustRosterSpec) -> String {
    json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "TrustRoster",
        "metadata": {"name": "default", "uid": "uid-roster", "generation": 1, "resourceVersion": "41"},
        "spec": serde_json::to_value(spec).expect("roster"),
    })
    .to_string()
}

/// Run one reconcile of `p` over a cluster holding `all` and `roster`, and
/// return every body the reconciler sent.
async fn reconcile(
    p: &TrustPolicy,
    all: &[TrustPolicy],
    roster: Option<&TrustRosterSpec>,
) -> (Result<(), String>, Vec<SeenBody>) {
    let object = serde_json::to_string(p).expect("serialises");
    let list = json!({"apiVersion": "logweir.dev/v1alpha1", "kind": "TrustPolicyList",
                      "metadata": {"resourceVersion": "1"}, "items": all})
    .to_string();
    let mut routes = vec![
        Route {
            method: "GET",
            path_suffix: "/trustpolicies",
            status: 200,
            body: list,
        },
        Route {
            method: "PATCH",
            path_suffix: "/trustpolicies/incident/status",
            status: 200,
            body: object.clone(),
        },
        Route {
            method: "PATCH",
            path_suffix: "/trustpolicies/incident",
            status: 200,
            body: object,
        },
    ];
    match roster {
        Some(spec) => {
            routes.push(Route {
                method: "GET",
                path_suffix: "/trustrosters/default",
                status: 200,
                body: roster_object(spec),
            });
            routes.push(Route {
                method: "PATCH",
                path_suffix: "/trustrosters/default/status",
                status: 200,
                body: roster_object(spec),
            });
        }
        None => routes.push(Route {
            method: "GET",
            path_suffix: "/trustrosters/default",
            status: 404,
            body: r#"{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"NotFound","code":404}"#.to_string(),
        }),
    }
    let (client, _recorder, bodies) = mock_client_recording_bodies(routes);
    let outcome = trust_policy::reconcile_policy(p, &client)
        .await
        .map(|_| ())
        .map_err(|e| e.to_string());
    let seen = bodies.lock().expect("bodies").clone();
    (outcome, seen)
}

/// The PATCHes sent to the policy OBJECT (not `/status`).
fn object_patches(seen: &[SeenBody]) -> Vec<Value> {
    seen.iter()
        .filter(|b| b.method == "PATCH")
        .filter(|b| {
            b.uri
                .split('?')
                .next()
                .is_some_and(|u| u.ends_with("/trustpolicies/incident"))
        })
        .map(|b| serde_json::from_str(&b.body).expect("a JSON body"))
        .collect()
}

/// The `CompromiseGuard` condition the status PATCH carried.
fn guard_condition(seen: &[SeenBody]) -> Value {
    let status: Value = seen
        .iter()
        .filter(|b| b.method == "PATCH")
        .find(|b| {
            b.uri
                .split('?')
                .next()
                .is_some_and(|u| u.ends_with("/trustpolicies/incident/status"))
        })
        .map(|b| serde_json::from_str(&b.body).expect("JSON"))
        .expect("a status patch was sent");
    status["status"]["conditions"]
        .as_array()
        .expect("conditions")
        .iter()
        .find(|c| c["type"] == CONDITION_COMPROMISE_GUARD)
        .cloned()
        .expect("the CompromiseGuard condition")
}

/// **Placed**: a policy recording a compromise gets the finalizer, in a body
/// preconditioned on the resourceVersion the status patch RETURNED (the mock
/// enforces seam S7 exactly as the API server does, so the handed copy's
/// `17` would be refused 409).
#[tokio::test]
async fn the_reconcile_places_the_finalizer_on_a_compromise_record() {
    let p = policy("incident", &[NS], vec![compromised("2026-09-10T12:00:00Z")]);
    let roster = roster_listing(&[(SIGNER, SIGNER_PEM)]);
    let (outcome, seen) = reconcile(&p, std::slice::from_ref(&p), Some(&roster)).await;
    outcome.expect("the reconcile completes");
    let patches = object_patches(&seen);
    assert_eq!(patches.len(), 1, "{seen:?}");
    assert_eq!(
        patches[0],
        json!({"metadata": {"name": "incident", "resourceVersion": "18",
                            "finalizers": [COMPROMISE_FINALIZER]}})
    );
    let c = guard_condition(&seen);
    assert_eq!(c["status"], "True");
    assert_eq!(c["reason"], REASON_COMPROMISE_RECORDED);
    let message = c["message"].as_str().unwrap_or_default();
    assert!(message.contains(SIGNER), "{message}");
    assert!(
        message.contains("TrustRoster/default still lists"),
        "the rollback hazard is named: {message}"
    );
}

/// **Held**: deleting the only record while the roster lists the key sends
/// NO finalizer patch, and the condition says what would release it.
#[tokio::test]
async fn a_deletion_that_would_lose_the_record_is_held() {
    let p = deleting(
        policy("incident", &[NS], vec![compromised("2026-09-10T12:00:00Z")]),
        &[COMPROMISE_FINALIZER],
    );
    let roster = roster_listing(&[(SIGNER, SIGNER_PEM)]);
    let (outcome, seen) = reconcile(&p, std::slice::from_ref(&p), Some(&roster)).await;
    outcome.expect("the reconcile completes");
    assert!(object_patches(&seen).is_empty(), "{seen:?}");
    let c = guard_condition(&seen);
    assert_eq!(c["reason"], REASON_DELETION_BLOCKED);
    let message = c["message"].as_str().unwrap_or_default();
    for fact in [
        SIGNER,
        ROSTER_SOURCE,
        COMPROMISE_FINALIZER,
        "Replacing a TrustPolicy",
    ] {
        assert!(message.contains(fact), "names {fact}: {message}");
    }
}

/// **Released**: a live successor recording the same compromise lets the
/// deletion through, and every OTHER finalizer on the object is kept.
#[tokio::test]
async fn a_deletion_whose_record_is_carried_is_released() {
    let p = deleting(
        policy("incident", &[NS], vec![compromised("2026-09-10T12:00:00Z")]),
        &["example.com/keep", COMPROMISE_FINALIZER],
    );
    let successor = policy(
        "incident-v2",
        &[NS],
        vec![compromised("2026-09-10T12:00:00Z")],
    );
    let roster = roster_listing(&[(SIGNER, SIGNER_PEM)]);
    let (outcome, seen) = reconcile(&p, &[p.clone(), successor], Some(&roster)).await;
    outcome.expect("the reconcile completes");
    let patches = object_patches(&seen);
    assert_eq!(patches.len(), 1, "{seen:?}");
    assert_eq!(
        patches[0]["metadata"]["finalizers"],
        json!(["example.com/keep"])
    );
}

/// A policy that records nothing is never written outside `/status`.
#[tokio::test]
async fn a_policy_recording_nothing_is_never_given_a_finalizer() {
    let p = policy("incident", &[NS], vec![active(SIGNER, SIGNER_PEM)]);
    let (outcome, seen) = reconcile(&p, std::slice::from_ref(&p), None).await;
    outcome.expect("the reconcile completes");
    assert!(object_patches(&seen).is_empty(), "{seen:?}");
    let c = guard_condition(&seen);
    assert_eq!(c["status"], "False");
    assert_eq!(c["reason"], REASON_NO_COMPROMISE_RECORDED);
}

/// **A roster that cannot be read releases nothing** — "could not read it"
/// must never be taken for "it lists nothing".
#[tokio::test]
async fn a_failed_roster_read_releases_nothing() {
    let p = deleting(
        policy("incident", &[NS], vec![compromised("2026-09-10T12:00:00Z")]),
        &[COMPROMISE_FINALIZER],
    );
    let object = serde_json::to_string(&p).expect("serialises");
    let list = json!({"apiVersion": "logweir.dev/v1alpha1", "kind": "TrustPolicyList",
                      "metadata": {"resourceVersion": "1"}, "items": [p.clone()]})
    .to_string();
    let (client, _r, bodies) = mock_client_recording_bodies(vec![
        Route {
            method: "GET",
            path_suffix: "/trustpolicies",
            status: 200,
            body: list,
        },
        Route {
            method: "GET",
            path_suffix: "/trustrosters/default",
            status: 500,
            body: r#"{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"InternalError","code":500}"#.to_string(),
        },
        Route {
            method: "PATCH",
            path_suffix: "/trustpolicies/incident",
            status: 200,
            body: object,
        },
    ]);
    assert!(trust_policy::reconcile_policy(&p, &client).await.is_err());
    assert!(
        bodies
            .lock()
            .expect("bodies")
            .iter()
            .all(|b| b.method != "PATCH"),
        "nothing is written on a pass that could not read the roster"
    );
}

// ---------------------------------------------------------------------------
// The condition on a policy that INHERITS a compromise
// ---------------------------------------------------------------------------

/// A policy listing a key another policy revoked for compromise says so, and
/// what to do: record it here too, so an older controller agrees.
#[test]
fn a_policy_listing_a_key_compromised_elsewhere_says_so() {
    let recorder = policy("incident", &[], vec![compromised("2026-09-10T12:00:00Z")]);
    let lister = policy("team-b", &["lw-team-b"], vec![active(SIGNER, SIGNER_PEM)]);
    let verdict = trust_policy::evaluate_in(&lister, &[recorder, lister.clone()], None, now());
    let c = verdict
        .conditions
        .iter()
        .find(|c| c.r#type == CONDITION_COMPROMISE_GUARD)
        .expect("the condition");
    assert_eq!(c.status, "True");
    assert_eq!(c.reason.as_deref(), Some(REASON_COMPROMISE_INHERITED));
    let message = c.message.clone().unwrap_or_default();
    assert!(
        message.contains("recorded by TrustPolicy/incident") && message.contains("KeyCompromise"),
        "{message}"
    );
    assert!(
        !verdict.guard.guards_anything(),
        "inheriting is not recording: no finalizer is owed on the lister"
    );
}

// ---------------------------------------------------------------------------
// What the refusal says
// ---------------------------------------------------------------------------

/// A verdict refused because of an INHERITED compromise names the object that
/// records it; one refused by its own policy's record reads exactly as before.
#[test]
fn the_refusal_names_the_policy_that_records_the_compromise() {
    let stored = json!({
        "phase": "Succeeded", "exitCode": 0,
        "evidence": {"verification": {
            "result": "Valid", "matchedKeyId": SIGNER,
            "payloadType": logweir_verify::PAYLOAD_TYPE_SCORECARD,
            "signedAt": "2026-09-10T10:00:00Z", "verifiedAt": "2026-09-10T10:05:00Z",
            "trust": {"basis": "Current", "keyState": "Active", "policy": {"name": "incident"}},
        }},
    });
    let roster = roster_listing(&[(SIGNER, SIGNER_PEM)]);
    let detail = |policies: &[TrustPolicy]| -> String {
        let r = weirkeeper::verification::retrust(
            &stored,
            &resolve_in(NS, policies, Some(&roster)),
            weirkeeper::verification::backup_badge,
            None,
            Some(3),
            now(),
        )
        .expect("the verdict changes");
        r.verification["detail"]
            .as_str()
            .unwrap_or_default()
            .to_string()
    };
    let own = detail(&[policy(
        "incident",
        &[NS],
        vec![compromised("2026-09-10T12:00:00Z")],
    )]);
    assert!(!own.contains("recorded by"), "{own}");
    let inherited = detail(&[policy(
        "incident",
        &[],
        vec![compromised("2026-09-10T12:00:00Z")],
    )]);
    assert!(
        inherited.contains("recorded by TrustPolicy/incident"),
        "{inherited}"
    );
    assert!(inherited.contains("legacy-roster-v1"), "{inherited}");
}

// ---------------------------------------------------------------------------
// The shared keys-page fixture: what this controller writes IS what the
// console renders (`ui/tests/d3.spec.js`)
// ---------------------------------------------------------------------------

/// `ui/tests/fixtures/d3/trustpolicy-compromise-inherited.json` is a policy
/// listing the installation signer `Active` while ANOTHER policy records its
/// `KeyCompromise` revocation. This row reads the fixture's own `spec`,
/// evaluates it beside the recording policy at the fixture's instant, and
/// asserts the controller's status IS the fixture's `status` — so the page
/// test renders exactly what this build writes.
#[test]
fn the_shared_keys_fixture_is_what_the_controller_writes() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../ui/tests/fixtures/d3/trustpolicy-compromise-inherited.json"
    );
    let fixture: Value =
        serde_json::from_str(&std::fs::read_to_string(path).expect("the shared fixture"))
            .expect("JSON");
    let mut lister: TrustPolicy = serde_json::from_value(fixture.clone()).expect("a TrustPolicy");
    lister.status = None;
    let recorder = policy(
        "incident-2026-09",
        &[],
        vec![compromised("2026-09-10T12:00:00Z")],
    );
    let evaluated_at = at("2026-09-12T08:00:00Z");
    let verdict =
        trust_policy::evaluate_in(&lister, &[recorder, lister.clone()], None, evaluated_at);
    let status = serde_json::to_value(trust_policy::status_for(&lister, &verdict, evaluated_at))
        .expect("status");
    assert_eq!(
        status,
        fixture["status"],
        "the fixture's status must be byte-for-byte what the controller writes; it wrote:\n{}",
        serde_json::to_string_pretty(&status).unwrap_or_default()
    );
}
