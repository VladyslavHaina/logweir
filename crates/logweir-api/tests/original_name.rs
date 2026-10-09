//! **PROD-15.1 at the product API**: a restore under the ORIGINAL topic names
//! is a distinct request (`target.topicNaming.originalName: true`, `newTopic`,
//! `prefix: ""`), stored as a distinct declaration, shown with a distinct
//! approval subject, and the console signs that subject — `originalName` —
//! into the authorization document only for such a Restore. Every other
//! request, object and document is what it was.

mod support;

use std::sync::Arc;

use logweir_api::approval::{ApprovalSettings, ConfirmationKey, InstallationIdentityRef};
use logweir_core::approval_policy::{ApprovalPolicySet, MarkerClaim, RestoreAuthorization};
use logweir_evidence::keys::SigningKey;
use serde_json::{json, Value};
use support::{FakeKube, Options, TestApp, NS_A, NS_B};

const RELEASE_NS: &str = "logweir-system";
const IDENTITY_CM: &str = "logweir-signing-trust";
const POLICY: &str = "logweir-installation";
const POLICY_UID: &str = "00000000-0000-4000-8000-0000000015a1";

fn signing_id() -> String {
    "a".repeat(64)
}

/// A fresh install (PROD-16.1): an unbound namespace is one-person
/// confirmation in the console, so a create signs a v2 document at once.
fn fresh_install() -> TestApp {
    let key = SigningKey::generate_ed25519();
    let console_id = key.key_id();
    let fake = FakeKube::new();
    let claim = MarkerClaim {
        policy_name: POLICY.into(),
        policy_uid: POLICY_UID.into(),
        signing_key_id: signing_id(),
        console_key_id: console_id.clone(),
    }
    .to_annotation();
    fake.seed(
        "configmaps",
        RELEASE_NS,
        json!({
            "metadata": {"name": IDENTITY_CM, "annotations": {
                "logweir.dev/identity-state": "established",
                "logweir.dev/approval-default": claim}},
            "data": {"key-id": signing_id(), "algorithm": "ecdsa-p256-sha256"}
        }),
    );
    fake.seed_cluster(
        "trustpolicies",
        json!({
            "metadata": {"name": POLICY, "uid": POLICY_UID, "annotations": {
                "logweir.dev/created-by": "identity-bootstrap",
                "logweir.dev/approval-default": "confirm"}},
            "spec": {"default": true, "keys": [
                {"keyId": signing_id(), "algorithm": "p256", "usages": ["EvidenceSigning"],
                 "state": "Active", "notBefore": "2026-10-07T09:55:00Z",
                 "notAfter": "9999-12-31T23:59:59Z",
                 "principal": {"id": format!("install:{RELEASE_NS}/logweir-signing-key")},
                 "spkiPem": "-----BEGIN PUBLIC KEY-----\nx\n-----END PUBLIC KEY-----\n"},
                {"keyId": console_id, "algorithm": "ed25519",
                 "usages": ["ConsoleConfirmation"], "state": "Active",
                 "notBefore": "2026-10-07T09:55:00Z", "notAfter": "9999-12-31T23:59:59Z",
                 "principal": {"id": format!("console:{RELEASE_NS}/logweir-console-confirmation")},
                 "spkiPem": "-----BEGIN PUBLIC KEY-----\ny\n-----END PUBLIC KEY-----\n"}]}
        }),
    );
    let settings = ApprovalSettings {
        policies: ApprovalPolicySet::parse("").expect("valid"),
        confirmation: Some(ConfirmationKey::from_key(key).expect("key")),
        installation: Some(InstallationIdentityRef {
            namespace: RELEASE_NS.into(),
            config_map: IDENTITY_CM.into(),
        }),
        ..ApprovalSettings::default()
    };
    TestApp::with(
        fake,
        Options {
            approval: Arc::new(settings),
            ..Options::default()
        },
    )
}

/// The request for a restore under the original topic names.
fn original_body() -> Value {
    let mut body = support::restore_body(&support::golden_plan());
    body["target"]["topicNaming"] = json!({"prefix": "", "originalName": true});
    body
}

async fn post(app: &TestApp, ns: &str, key: &str, body: &Value) -> support::TestResponse {
    app.post(
        &format!("/api/v1/namespaces/{ns}/restores"),
        Some(key),
        &body.to_string(),
    )
    .await
}

fn last_approval_posted(fake: &FakeKube) -> Value {
    fake.requests()
        .into_iter()
        .filter(|r| r.method == "POST" && r.path.ends_with("/approvals"))
        .map(|r| serde_json::from_str::<Value>(&r.body).expect("json"))
        .next_back()
        .expect("the console stored its confirmation")
}

/// **The original-name request is stored, shown and signed distinctly.**
/// KILLS: dropping the declaration from the stored object; signing an
/// ordinary document for it; showing it as an ordinary restore.
#[tokio::test]
async fn an_original_name_restore_is_stored_shown_and_signed_with_its_own_subject() {
    let app = fresh_install();
    let created = post(&app, NS_B, "original-name-0001", &original_body()).await;
    assert_eq!(
        created.status,
        201,
        "{}",
        String::from_utf8_lossy(&created.body)
    );
    let body = created.json();
    assert_eq!(body["item"]["approvalSubject"], "originalName");
    assert_eq!(body["item"]["target"]["originalName"], true);
    assert_eq!(body["item"]["target"]["topicPrefix"], "");
    let name = body["item"]["name"].as_str().unwrap().to_string();
    let stored = app.fake.object("restores", NS_B, &name).unwrap();
    assert_eq!(
        stored["spec"]["target"]["topicNaming"]["originalName"],
        true
    );
    assert_eq!(stored["spec"]["target"]["topicNaming"]["prefix"], "");

    let approval = last_approval_posted(&app.fake);
    let doc = RestoreAuthorization::from_bytes(
        approval["spec"]["approvalBytes"]
            .as_str()
            .unwrap()
            .as_bytes(),
    )
    .expect("a v2 document");
    assert_eq!(doc.approval_subject.as_deref(), Some("originalName"));
    app.fake.assert_strict();
}

/// The control: an ordinary request is stored, shown and signed exactly as
/// before — no declaration key, no subject key. KILLS: a subject written for
/// every restore.
#[tokio::test]
async fn an_ordinary_restore_carries_no_subject_anywhere() {
    let app = fresh_install();
    let created = post(
        &app,
        NS_B,
        "ordinary-name-0001",
        &support::restore_body(&support::golden_plan()),
    )
    .await;
    assert_eq!(
        created.status,
        201,
        "{}",
        String::from_utf8_lossy(&created.body)
    );
    let body = created.json();
    assert_eq!(body["item"]["approvalSubject"], "ordinary");
    assert_eq!(body["item"]["target"]["originalName"], false);
    let name = body["item"]["name"].as_str().unwrap().to_string();
    let stored = app.fake.object("restores", NS_B, &name).unwrap();
    assert!(stored["spec"]["target"]["topicNaming"]
        .get("originalName")
        .is_none());
    let approval = last_approval_posted(&app.fake);
    let bytes = approval["spec"]["approvalBytes"].as_str().unwrap();
    assert!(!bytes.contains("approvalSubject"), "{bytes}");
}

/// **The refusals**, each before anything is stored. KILLS: admitting the
/// declaration in scratch mode or beside a prefix; admitting an empty prefix
/// without the declaration.
#[tokio::test]
async fn an_original_name_request_is_refused_outside_its_one_shape() {
    let app = TestApp::new();
    let cases: [(&str, Value, &str, &str); 3] = [
        (
            "in scratch mode",
            {
                let mut b = original_body();
                b["target"]["mode"] = json!("scratch");
                b
            },
            "target.topicNaming.originalName",
            "requires_new_topic",
        ),
        (
            "beside a prefix",
            {
                let mut b = original_body();
                b["target"]["topicNaming"]["prefix"] = json!("restore-");
                b
            },
            "target.topicNaming.prefix",
            "prefix_with_original_name",
        ),
        (
            "an empty prefix nobody opted into",
            {
                let mut b = original_body();
                b["target"]["topicNaming"] = json!({"prefix": ""});
                b
            },
            "target.topicNaming.prefix",
            "invalid_prefix",
        ),
    ];
    for (i, (label, body, field, code)) in cases.into_iter().enumerate() {
        let refused = post(&app, NS_A, &format!("original-refuse-{i:04}"), &body).await;
        refused.assert_problem(422, "validation_failed");
        let errors = refused.json()["errors"].clone();
        assert!(
            errors
                .as_array()
                .unwrap()
                .iter()
                .any(|e| e["field"] == field && e["code"] == code),
            "{label}: {errors}"
        );
    }
    assert_eq!(app.fake.count("restores", NS_A), 0);
}

/// The declared mapping: the identity map is THE mapping of an original-name
/// request and is refused for every other one. KILLS: `mapping_identity`
/// raised for the original-name request; a rename admitted beside it.
#[tokio::test]
async fn the_identity_mapping_is_declared_only_by_an_original_name_request() {
    let app = TestApp::new();
    let mut ok = original_body();
    ok["topicMapping"] = json!([
        {"source": "orders", "target": "orders"},
        {"source": "payments", "target": "payments"},
    ]);
    let created = post(&app, NS_A, "original-map-00001", &ok).await;
    assert_eq!(
        created.status,
        201,
        "{}",
        String::from_utf8_lossy(&created.body)
    );

    let mut renamed = original_body();
    renamed["topicMapping"] = json!([{"source": "orders", "target": "orders-v2"}]);
    let refused = post(&app, NS_A, "original-map-00002", &renamed).await;
    refused.assert_problem(422, "validation_failed");
    assert_eq!(refused.json()["errors"][0]["code"], "mapping_mismatch");

    let mut ordinary = support::restore_body(&support::golden_plan());
    ordinary["target"]["topicNaming"]["prefix"] = json!("orders");
    ordinary["topicMapping"] = json!([{"source": "orders", "target": "orders"}]);
    let refused = post(&app, NS_A, "original-map-00003", &ordinary).await;
    refused.assert_problem(422, "validation_failed");
    assert_eq!(refused.json()["errors"][0]["code"], "mapping_identity");
}

/// **The Approval view shows the subject its SIGNED bytes carry.** KILLS:
/// reading it from anything but the document; showing an unreadable document
/// as ordinary.
#[tokio::test]
async fn the_approval_view_shows_the_signed_subject() {
    let app = TestApp::new();
    let sidecar = r#"{"payloadType":"application/vnd.logweir.drill-approval+json;version=1.0.0","signatures":[]}"#;
    for (name, bytes, want) in [
        (
            "a-original",
            json!({"plan_hash": "sha256:aa", "approval_subject": "originalName"}).to_string(),
            "originalName",
        ),
        (
            "a-ordinary",
            json!({"plan_hash": "sha256:aa"}).to_string(),
            "ordinary",
        ),
        ("a-unreadable", "approver: ops\n".to_string(), "unknown"),
    ] {
        app.fake.seed(
            "approvals",
            NS_A,
            json!({
                "metadata": {"name": name},
                "spec": {"subjectRef": {"kind": "Restore", "name": "rst-x"},
                         "planHash": "sha256:aa", "approvalBytes": bytes,
                         "sidecarBytes": sidecar}
            }),
        );
        let view = app
            .get(&format!("/api/v1/namespaces/{NS_A}/approvals/{name}"))
            .await
            .json();
        assert_eq!(view["item"]["approvalSubject"], want, "{name}");
    }
}

/// **An unbound namespace records a v1 approval only for its own subject.**
/// An ordinary approval file submitted for an original-name Restore is
/// refused before an Approval exists. KILLS: deleting the route's subject
/// check (the controller would refuse later, but the Approval would exist).
#[tokio::test]
async fn a_legacy_approval_for_the_wrong_subject_is_refused_before_it_is_stored() {
    let app = TestApp::new();
    let created = post(&app, NS_A, "original-legacy-001", &original_body()).await;
    assert_eq!(
        created.status,
        201,
        "{}",
        String::from_utf8_lossy(&created.body)
    );
    let item = created.json()["item"].clone();
    let name = item["name"].as_str().unwrap().to_string();
    let plan_hash = item["planHash"].as_str().unwrap().to_string();
    let sidecar = r#"{"payloadType":"application/vnd.logweir.drill-approval+json;version=1.0.0","signatures":[]}"#;
    let ordinary = json!({"plan_hash": plan_hash, "subject_kind": "Restore"}).to_string();
    let refused = app
        .post(
            &format!("/api/v1/namespaces/{NS_A}/restores/{name}/approval"),
            None,
            &json!({"approvalBytes": ordinary, "sidecarBytes": sidecar}).to_string(),
        )
        .await;
    refused.assert_problem(422, "validation_failed");
    assert_eq!(
        refused.json()["errors"][0]["code"],
        "approval_subject_mismatch",
        "{}",
        refused.json()
    );
    assert_eq!(app.fake.count("approvals", NS_A), 0);

    let matching = json!({"plan_hash": plan_hash, "subject_kind": "Restore",
                          "approval_subject": "originalName"})
    .to_string();
    let recorded = app
        .post(
            &format!("/api/v1/namespaces/{NS_A}/restores/{name}/approval"),
            None,
            &json!({"approvalBytes": matching, "sidecarBytes": sidecar}).to_string(),
        )
        .await;
    assert!(
        recorded.status == 200 || recorded.status == 201,
        "{}",
        String::from_utf8_lossy(&recorded.body)
    );
}
