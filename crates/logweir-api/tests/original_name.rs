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

/// The request for a restore under the original topic names: the
/// declaration, and the complete verification it requires.
fn original_body() -> Value {
    let mut body = support::restore_body(&support::golden_plan());
    body["target"]["topicNaming"] = json!({"prefix": "", "originalName": true});
    body["coverage"] = json!("complete");
    body
}

/// OD-10: the same request with the original topic names re-typed (the
/// golden plan restores `orders` and `payments`).
fn typed_original_body(names: &[&str]) -> Value {
    let mut body = original_body();
    body["originalNameConfirmation"] = json!({"typedTopics": names});
    body
}

/// `team-a` (`NS_A`) bound to `binding` under an explicit policy document, and
/// a console key.
fn bound_app(binding: &str) -> TestApp {
    let key = SigningKey::generate_ed25519();
    let policies = ApprovalPolicySet::parse(&format!(
        "allowOrdinaryConfirmation: true\npolicies:\n  - name: team-ordinary\n    mode: \
         Ordinary\n  - name: prod-governed\n    mode: Governed\nnamespaces:\n  {NS_A}: {binding}\n"
    ))
    .expect("valid");
    TestApp::with(
        FakeKube::new(),
        Options {
            approval: Arc::new(ApprovalSettings {
                policies,
                confirmation: Some(ConfirmationKey::from_key(key).expect("key")),
                ..ApprovalSettings::default()
            }),
            ..Options::default()
        },
    )
}

fn errors_of(response: &support::TestResponse) -> Vec<(String, String, String)> {
    response.json()["errors"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| {
            (
                e["field"].as_str().unwrap_or_default().to_string(),
                e["code"].as_str().unwrap_or_default().to_string(),
                e["message"].as_str().unwrap_or_default().to_string(),
            )
        })
        .collect()
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
    let created = post(
        &app,
        NS_B,
        "original-name-0001",
        &typed_original_body(&["payments", "orders"]),
    )
    .await;
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
    // A document that carries the subject is format 2.1.0, never 2.0.0: a
    // reader that predates the subject refuses it.
    assert_eq!(doc.format_version, "2.1.0");
    assert!(approval["spec"]["approvalBytes"]
        .as_str()
        .unwrap()
        .starts_with("{\"formatVersion\":\"2.1.0\","));
    // The declared coverage is stored beside the declaration.
    assert_eq!(stored["spec"]["coverage"], "complete");
    // OD-10: what the requester typed is signed beside the subject.
    assert_eq!(
        doc.original_name_confirmation
            .as_ref()
            .map(|c| c.typed_topics.clone()),
        Some(vec!["payments".to_string(), "orders".to_string()])
    );
    app.fake.assert_strict();
}

/// **OD-10 (review M1): a one-person confirmation needs every original topic
/// name re-typed, exactly**, refused by name before anything is created: no
/// typed names, a missing name, a name typed in another case. KILLS: the API
/// signing an original-name confirmation without the typed names, or with
/// names that are not the plan's.
#[tokio::test]
async fn a_one_person_confirmation_needs_the_original_names_typed_exactly() {
    let app = fresh_install();
    let refused = post(&app, NS_B, "original-typed-0001", &original_body()).await;
    refused.assert_problem(422, "validation_failed");
    assert!(
        errors_of(&refused).iter().any(|(field, code, _)| field
            == "originalNameConfirmation.typedTopics"
            && code == "typed_topics_required"),
        "{:?}",
        errors_of(&refused)
    );
    for (i, (names, words)) in [
        (vec!["orders"], "not typed: \"payments\""),
        (vec!["orders", "Payments"], "not typed: \"payments\""),
        (
            vec!["orders", "payments", "audit"],
            "typed but not restored by this plan: \"audit\"",
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let refused = post(
            &app,
            NS_B,
            &format!("original-typed-{:04}", i + 2),
            &typed_original_body(&names),
        )
        .await;
        refused.assert_problem(422, "validation_failed");
        let errors = errors_of(&refused);
        assert!(
            errors.iter().any(|(field, code, message)| field
                == "originalNameConfirmation.typedTopics"
                && code == "typed_topics_mismatch"
                && message.contains(words)),
            "{names:?}: {errors:?}"
        );
    }
    assert_eq!(app.fake.count("restores", NS_B), 0, "nothing was created");
    assert_eq!(app.fake.count("approvals", NS_B), 0);
}

/// Typed names never stand in for anything else: refused on an ordinary
/// restore, and in an unbound namespace (which signs nothing). KILLS: a typed
/// list accepted where it would be signed beside nothing it confirms.
#[tokio::test]
async fn typed_names_are_refused_where_they_would_replace_nothing() {
    let app = fresh_install();
    let mut ordinary = support::restore_body(&support::golden_plan());
    ordinary["originalNameConfirmation"] = json!({"typedTopics": ["orders", "payments"]});
    let refused = post(&app, NS_B, "typed-ordinary-0001", &ordinary).await;
    refused.assert_problem(422, "validation_failed");
    assert!(errors_of(&refused)
        .iter()
        .any(|(field, code, _)| field == "originalNameConfirmation" && code == "not_accepted"));

    let legacy = TestApp::new();
    let refused = post(
        &legacy,
        NS_A,
        "typed-legacy-0001",
        &typed_original_body(&["orders", "payments"]),
    )
    .await;
    refused.assert_problem(422, "validation_failed");
    assert!(errors_of(&refused)
        .iter()
        .any(|(field, code, _)| field == "originalNameConfirmation" && code == "not_accepted"));
    assert_eq!(legacy.fake.count("restores", NS_A), 0);
}

/// **OD-10: a strict (Governed) namespace still needs the second person.**
/// An original-name request is stored and confirmed by the console only as
/// `<approvalRef>-confirmation`, which authorises nothing; the Approval the
/// Restore references does not exist until an approver countersigns. Typed
/// names are refused there: they never replace the second person. KILLS:
/// typed names admitted as the approval in a strict namespace; the console
/// storing an authorising Approval for an original-name restore under strict.
#[tokio::test]
async fn a_strict_namespace_still_needs_the_second_person_for_an_original_name_restore() {
    let app = bound_app("prod-governed");
    let mut body = original_body();
    body["ticket"] = json!("CHG-4711");
    let created = post(&app, NS_A, "original-strict-0001", &body).await;
    assert_eq!(
        created.status,
        201,
        "{}",
        String::from_utf8_lossy(&created.body)
    );
    let answer = created.json();
    assert_eq!(
        answer["authorization"]["state"], "awaitingApproval",
        "{answer}"
    );
    let approval_ref = answer["item"]["approvalRef"]["name"]
        .as_str()
        .unwrap_or("approval-1234abcd")
        .to_string();
    let posted: Vec<String> = app
        .fake
        .requests()
        .into_iter()
        .filter(|r| r.method == "POST" && r.path.ends_with("/approvals"))
        .map(|r| {
            serde_json::from_str::<Value>(&r.body).expect("json")["metadata"]["name"]
                .as_str()
                .unwrap_or_default()
                .to_string()
        })
        .collect();
    assert_eq!(
        posted,
        vec![format!("{approval_ref}-confirmation")],
        "only the confirmation, which authorises nothing"
    );

    let mut typed = typed_original_body(&["orders", "payments"]);
    typed["ticket"] = json!("CHG-4711");
    let refused = post(&app, NS_A, "original-strict-0002", &typed).await;
    refused.assert_problem(422, "validation_failed");
    assert!(errors_of(&refused)
        .iter()
        .any(|(field, code, _)| field == "originalNameConfirmation" && code == "not_accepted"));
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
    // ... and stays format 2.0.0, byte for byte: the bytes are exactly a
    // document with neither field, re-serialised.
    assert!(
        bytes.starts_with("{\"formatVersion\":\"2.0.0\",\"kind\":\"RestoreAuthorization\","),
        "{bytes}"
    );
    assert!(!bytes.contains("originalNameConfirmation"), "{bytes}");
    let doc = RestoreAuthorization::from_bytes(bytes.as_bytes()).expect("a v2 document");
    assert_eq!(doc.format_version, "2.0.0");
    assert_eq!(doc.approval_subject, None);
    assert_eq!(doc.original_name_confirmation, None);
    assert_eq!(String::from_utf8(doc.to_bytes()).unwrap(), bytes);
}

/// **The refusals**, each before anything is stored. KILLS: admitting the
/// declaration in scratch mode, beside a prefix, or with SAMPLED coverage
/// (stated or left to its default: an original-name restore requires
/// complete verification); admitting an empty prefix without the
/// declaration.
#[tokio::test]
async fn an_original_name_request_is_refused_outside_its_one_shape() {
    let app = TestApp::new();
    let cases: [(&str, Value, &str, &str); 5] = [
        (
            "with no coverage stated (sampled by default)",
            {
                let mut b = original_body();
                b.as_object_mut().unwrap().remove("coverage");
                b
            },
            "coverage",
            "original_name_requires_complete",
        ),
        (
            "with sampled coverage",
            {
                let mut b = original_body();
                b["coverage"] = json!("sampled");
                b
            },
            "coverage",
            "original_name_requires_complete",
        ),
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

    // The sampled refusal says why, by name.
    let mut sampled = original_body();
    sampled.as_object_mut().unwrap().remove("coverage");
    let refused = post(&app, NS_A, "original-refuse-sampled", &sampled).await;
    let message = errors_of(&refused)
        .into_iter()
        .find(|(_, code, _)| code == "original_name_requires_complete")
        .map(|(_, _, message)| message)
        .expect("the named refusal");
    assert!(message.contains("verified completely"), "{message}");
    assert!(message.contains("never by sample"), "{message}");

    // CONTROLS: the complete request is created, and an ORDINARY sampled
    // request is what it always was.
    let created = post(&app, NS_A, "original-accept-0001", &original_body()).await;
    assert_eq!(
        created.status,
        201,
        "{}",
        String::from_utf8_lossy(&created.body)
    );
    let ordinary = support::restore_body(&support::golden_plan());
    assert!(ordinary.get("coverage").is_none());
    let created = post(&app, NS_B, "ordinary-sampled-0001", &ordinary).await;
    assert_eq!(
        created.status,
        201,
        "{}",
        String::from_utf8_lossy(&created.body)
    );
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

/// The custom resource of a Restore whose creation step lost a race: exit 1,
/// `exitReason: TargetTopicAppeared`, and the controller's
/// `status.targetTopicsAppeared` — one name someone else created, one topic
/// this run created and left.
fn creation_stopped_cr() -> Value {
    let mut cr = support::fixture("restore-valid-pass.json");
    cr["spec"]["target"]["mode"] = json!("newTopic");
    cr["spec"]["target"]["topicNaming"] = json!({"prefix": "", "originalName": true});
    cr["spec"]["coverage"] = json!("complete");
    cr["status"] = json!({
        "phase": "Failed",
        "exitCode": 1,
        "exitReason": "TargetTopicAppeared",
        "reason": "Operational",
        "lastPhaseCompleted": 5,
        "targetTopicsAppeared": {"appeared": ["payments"], "left": ["orders"]},
        "jobRef": {"name": "orders-drill-a-job"},
        "conditions": [{
            "type": "Failed", "status": "True", "reason": "Operational",
            "lastTransitionTime": "2026-09-11T12:42:20Z", "observedGeneration": 1
        }]
    });
    cr
}

/// **After a lost race the Restore view names every topic the run created
/// and left** (the orchestrator's ruling of 2026-10-09: nothing is deleted
/// under an original name, so the operator must be told what is there). The
/// view carries the controller's two lists verbatim and the one instruction;
/// the console fixture `console/restore-creation-stopped.json` IS this
/// projection, decoded and rendered by `ui/tests/original-name.spec.js`, so
/// the field names cannot drift. A Restore without the status block has no
/// key. KILLS: a projection that drops the block or a list; an instruction
/// that differs from the runner's sentence; a view that claims a deletion.
#[test]
fn the_restore_view_names_the_topics_a_stopped_creation_step_left() {
    use weirkeeper::crds::restore::Restore as RestoreCr;

    let stopped: RestoreCr =
        serde_json::from_value(creation_stopped_cr()).expect("the fixture deserialises");
    let projected = serde_json::to_value(logweir_api::projection::restore(&stopped, true))
        .expect("the projection serialises");
    assert_eq!(
        projected["targetTopicsAppeared"],
        json!({
            "appeared": ["payments"],
            "left": ["orders"],
            "leftInstruction": "created by this restore and left empty; remove it yourself \
                                once you have checked nothing writes to it"
        }),
        "{projected}"
    );
    assert_eq!(
        projected["targetTopicsAppeared"]["leftInstruction"],
        logweir_core::guard::LEFT_TOPIC_SENTENCE
    );
    // The run's operation names the closed state beside it.
    let operation =
        serde_json::to_value(logweir_api::status::restore_operation(&stopped)).expect("serialises");
    assert_eq!(operation["result"]["exitReason"], "TargetTopicAppeared");
    assert_eq!(operation["result"]["exitCode"], 1);
    let golden = support::fixture("console/restore-creation-stopped.json");
    assert_eq!(
        projected, golden["item"],
        "the console fixture is this crate's projection of the same status"
    );

    // No stopped creation step, no key.
    let passed: RestoreCr = serde_json::from_value(support::fixture("restore-valid-pass.json"))
        .expect("the fixture deserialises");
    let projected = serde_json::to_value(logweir_api::projection::restore(&passed, true))
        .expect("the projection serialises");
    assert!(
        projected.get("targetTopicsAppeared").is_none(),
        "{projected}"
    );
}
