//! PLAT-19.2 (and PLAT-12.1's policy routing) at the product API: a Restore
//! submission is routed by the namespace's frozen approval policy — to
//! execution under Ordinary, to Awaiting approval under Governed and under the
//! legacy synthesis — and a governed approval is submitted by a DIFFERENT
//! principal, never the requester.
//!
//! These rows sign for real: the console key is an in-memory Ed25519 key, and
//! every stored document is verified here with the same `verify_detached` the
//! controller and the runner use.

mod support;

use std::sync::Arc;

use logweir_api::approval::{ApprovalSettings, ConfirmationKey};
use logweir_api::authz::Role;
use logweir_core::approval_policy::{
    ApprovalPolicySet, RestoreAuthorization, PAYLOAD_TYPE_RESTORE_AUTHORIZATION,
};
use logweir_evidence::keys::SigningKey;
use logweir_evidence::sign::sign_detached;
use logweir_evidence::Sidecar;
use serde_json::{json, Value};
use support::{
    FakeKube, Options, SharedApp, SharedOptions, TestApp, ISSUER, LOCAL_ADMIN_ACTOR, NS_A, NS_B,
};

const POLICIES: &str = "allowOrdinaryConfirmation: true
policies:
  - name: team-ordinary
    mode: Ordinary
  - name: prod-governed
    mode: Governed
";

struct Console {
    settings: Arc<ApprovalSettings>,
    public: logweir_evidence::keys::VerifyingKey,
}

/// `team-a` bound to `binding`, `team-b` unbound, and a console key.
fn console(binding: &str) -> Console {
    let key = SigningKey::generate_ed25519();
    let public = key.verifying_key();
    let policies =
        ApprovalPolicySet::parse(&format!("{POLICIES}namespaces:\n  {NS_A}: {binding}\n"))
            .expect("valid");
    Console {
        settings: Arc::new(ApprovalSettings {
            policies,
            confirmation: Some(ConfirmationKey::from_key(key).expect("key")),
        }),
        public,
    }
}

fn app(console: &Console) -> TestApp {
    TestApp::with(
        FakeKube::new(),
        Options {
            approval: Arc::clone(&console.settings),
            ..Options::default()
        },
    )
}

fn approvals_posted(fake: &FakeKube) -> Vec<Value> {
    fake.requests()
        .into_iter()
        .filter(|r| r.method == "POST" && r.path.ends_with("/approvals"))
        .map(|r| serde_json::from_str(&r.body).expect("json"))
        .collect()
}

async fn create(app: &TestApp, ns: &str, key: &str) -> support::TestResponse {
    let body = support::restore_body(&support::golden_plan());
    app.post(
        &format!("/api/v1/namespaces/{ns}/restores"),
        Some(key),
        &body.to_string(),
    )
    .await
}

fn verify(document: &str, sidecar: &str, key: &logweir_evidence::keys::VerifyingKey) -> bool {
    let sidecar: Sidecar = serde_json::from_str(sidecar).expect("a sidecar");
    logweir_evidence::verify::verify_detached(
        key,
        PAYLOAD_TYPE_RESTORE_AUTHORIZATION,
        document.as_bytes(),
        &sidecar,
    )
    .is_ok()
}

// ---------------------------------------------------------------------------
// Routing a submission by the frozen policy (PLAT-12.1)
// ---------------------------------------------------------------------------

/// **Existing installations keep their approval requirement.** An unbound
/// namespace signs nothing and routes to Awaiting approval, exactly as before.
#[tokio::test]
async fn an_unbound_namespace_signs_nothing_and_awaits_a_governed_approval() {
    let console = console("team-ordinary");
    let app = app(&console);
    let created = create(&app, NS_B, "unbound-restore-01").await;
    assert_eq!(
        created.status,
        201,
        "{}",
        String::from_utf8_lossy(&created.body)
    );
    let authorization = &created.json()["authorization"];
    assert_eq!(authorization["state"], "awaitingApproval");
    assert_eq!(authorization["mode"], "governed");
    assert_eq!(authorization["legacy"], true);
    assert_eq!(authorization["policy"], "legacy-governed-v1");
    assert!(authorization.get("policyDigest").is_none());
    assert!(approvals_posted(&app.fake).is_empty(), "nothing is signed");
    app.fake.assert_strict();

    // And a console with NO policy configured at all behaves identically.
    let plain = TestApp::new();
    let created = create(&plain, NS_A, "unbound-restore-02").await;
    assert_eq!(created.json()["authorization"]["state"], "awaitingApproval");
    assert!(approvals_posted(&plain.fake).is_empty());
}

/// **The ordinary path is simple**: one submission, and the Approval the
/// Restore references is the console's signed confirmation, bound to this
/// Restore's UID, plan hash and the policy digest.
#[tokio::test]
async fn an_ordinary_namespace_confirms_and_routes_to_execution() {
    let console = console("team-ordinary");
    let app = app(&console);
    let created = create(&app, NS_A, "ordinary-restore-01").await;
    assert_eq!(
        created.status,
        201,
        "{}",
        String::from_utf8_lossy(&created.body)
    );
    let body = created.json();
    let authorization = &body["authorization"];
    assert_eq!(authorization["state"], "confirmed");
    assert_eq!(authorization["mode"], "ordinary");
    assert_eq!(authorization["legacy"], false);
    assert_eq!(authorization["requester"], LOCAL_ADMIN_ACTOR);
    assert_eq!(authorization["approvalName"], "approval-1234abcd");
    assert!(authorization.get("confirmationName").is_none());

    let stored = app
        .fake
        .object("approvals", NS_A, "approval-1234abcd")
        .expect("the referenced Approval exists");
    let document = stored["spec"]["approvalBytes"].as_str().expect("bytes");
    let sidecar = stored["spec"]["sidecarBytes"].as_str().expect("sidecar");
    assert!(
        verify(document, sidecar, &console.public),
        "the console signed the exact bytes"
    );
    let doc = RestoreAuthorization::from_bytes(document.as_bytes()).expect("a v2 document");
    let restore = &body["item"];
    assert_eq!(doc.subject.uid, restore["uid"].as_str().expect("uid"));
    assert_eq!(doc.subject.name, restore["name"].as_str().expect("name"));
    assert_eq!(doc.plan_hash, restore["planHash"].as_str().expect("hash"));
    let policy = console.settings.policies.resolve(NS_A);
    let policy = policy.bound().expect("bound");
    assert_eq!(doc.policy.digest, policy.digest());
    assert_eq!(doc.requester.principal_id(), LOCAL_ADMIN_ACTOR);
    assert_eq!(
        (doc.expires_at - doc.issued_at).num_seconds(),
        policy.max_age_seconds
    );
    assert_eq!(stored["spec"]["subjectRef"]["name"], restore["name"]);
    app.fake.assert_strict();
}

/// **Recovery after a lost response** (D0: "Replays complete an interrupted
/// create sequence by reading Restore/Approval; they do not create a second
/// subject"). The Approval create fails once; the replay completes it; a
/// second replay signs nothing new.
#[tokio::test]
async fn a_replay_completes_an_interrupted_confirmation_and_signs_once() {
    let console = console("team-ordinary");
    let app = app(&console);
    app.fake.inject(support::Fault {
        method: "POST",
        path_contains: "/approvals".into(),
        status: 500,
        reason: "InternalError",
        delay: None,
        remaining: 1,
    });
    let first = create(&app, NS_A, "ordinary-restore-02").await;
    assert_ne!(
        first.status, 201,
        "the confirmation failed, so the create did"
    );
    let replay = create(&app, NS_A, "ordinary-restore-02").await;
    assert_eq!(
        replay.status,
        200,
        "{}",
        String::from_utf8_lossy(&replay.body)
    );
    assert_eq!(replay.json()["replayed"], true);
    assert_eq!(replay.json()["authorization"]["state"], "confirmed");
    let again = create(&app, NS_A, "ordinary-restore-02").await;
    assert_eq!(again.status, 200);
    assert_eq!(
        approvals_posted(&app.fake).len(),
        2,
        "one failed attempt and one success; the second replay reads, it does not sign again"
    );
    let listed = app
        .get(&format!("/api/v1/namespaces/{NS_A}/restores"))
        .await
        .json();
    assert_eq!(
        listed["items"].as_array().map(Vec::len),
        Some(1),
        "one subject: every replay converged on the same Restore"
    );
}

/// An Approval this Restore did not produce is never adopted as its
/// confirmation.
#[tokio::test]
async fn a_foreign_approval_under_the_referenced_name_is_never_adopted() {
    let console = console("team-ordinary");
    let app = app(&console);
    app.fake.seed(
        "approvals",
        NS_A,
        json!({
            "apiVersion": "logweir.dev/v1alpha1", "kind": "Approval",
            "metadata": {"name": "approval-1234abcd", "namespace": NS_A},
            "spec": {"subjectRef": {"kind": "Restore", "name": "someone-else"},
                     "planHash": "sha256:x", "approvalBytes": "{}", "sidecarBytes": "{}"}
        }),
    );
    let created = create(&app, NS_A, "ordinary-restore-03").await;
    assert_eq!(
        created.status,
        409,
        "{}",
        String::from_utf8_lossy(&created.body)
    );
    assert_eq!(created.code(), "state_conflict");
    assert!(approvals_posted(&app.fake).is_empty());
}

/// **A confirmation for a PREVIOUS subject of the same name is not this
/// one's.** The console's own document, byte-for-byte what it signs for this
/// plan, policy and requester -- except that it names another UID (a Restore
/// of this name that was deleted and recreated). Adopting it would route the
/// new Restore to execution on an authorization that names a different
/// object; the controller would refuse it, but the console must not answer
/// `confirmed` for it either. The control is the same document with the
/// current UID, which a replay adopts.
#[tokio::test]
async fn a_confirmation_naming_another_uid_is_never_adopted() {
    let console = console("team-ordinary");
    let first = app(&console);
    let created = create(&first, NS_A, "ordinary-restore-uid").await;
    assert_eq!(
        created.status,
        201,
        "{}",
        String::from_utf8_lossy(&created.body)
    );
    let restore_uid = created.json()["item"]["uid"]
        .as_str()
        .expect("a uid")
        .to_string();
    let mut stored = approvals_posted(&first.fake)
        .pop()
        .expect("the console stored its confirmation");
    let bytes = stored["spec"]["approvalBytes"]
        .as_str()
        .expect("bytes")
        .to_string();
    assert!(
        bytes.contains(&restore_uid),
        "the document names the subject UID"
    );

    // THE ROW: the same bytes naming a previous object's UID. The Restore is
    // still created (the subject comes first), but its confirmation is not
    // adopted and nothing is signed over the foreign object.
    let seeded = |uid: &str| {
        let mut object = stored.clone();
        object["spec"]["approvalBytes"] = Value::String(bytes.replace(&restore_uid, uid));
        let target = app(&console);
        target.fake.seed("approvals", NS_A, object);
        target
    };
    let foreign = seeded("00000000-0000-4000-8000-0000000000aa");
    let refused = create(&foreign, NS_A, "ordinary-restore-uid").await;
    assert_eq!(
        refused.status,
        409,
        "{}",
        String::from_utf8_lossy(&refused.body)
    );
    assert_eq!(refused.code(), "state_conflict");
    assert!(
        approvals_posted(&foreign.fake).is_empty(),
        "nothing is signed over the foreign object"
    );
    let listed = foreign
        .get(&format!("/api/v1/namespaces/{NS_A}/restores"))
        .await
        .json();
    let subject_uid = listed["items"][0]["uid"]
        .as_str()
        .expect("the Restore was created")
        .to_string();

    // THE CONTROL: the identical sequence, with the document naming THIS
    // subject's UID, is adopted as this Restore's confirmation.
    let matching = seeded(&subject_uid);
    let adopted = create(&matching, NS_A, "ordinary-restore-uid").await;
    assert_eq!(
        adopted.status,
        201,
        "{}",
        String::from_utf8_lossy(&adopted.body)
    );
    assert_eq!(
        adopted.json()["item"]["uid"].as_str(),
        Some(subject_uid.as_str())
    );
    assert_eq!(adopted.json()["authorization"]["state"], "confirmed");
    assert!(
        approvals_posted(&matching.fake).is_empty(),
        "the matching confirmation is adopted, not signed again"
    );
}

/// **The governed path**: the console's confirmation is a separate object
/// that authorises nothing, and the submission routes to Awaiting approval.
#[tokio::test]
async fn a_governed_namespace_confirms_into_a_separate_object_and_awaits_approval() {
    let console = console("prod-governed");
    let app = app(&console);
    let created = create(&app, NS_A, "governed-restore-01").await;
    assert_eq!(
        created.status,
        201,
        "{}",
        String::from_utf8_lossy(&created.body)
    );
    let authorization = &created.json()["authorization"];
    assert_eq!(authorization["state"], "awaitingApproval");
    assert_eq!(authorization["mode"], "governed");
    assert_eq!(
        authorization["confirmationName"],
        "approval-1234abcd-confirmation"
    );
    assert!(
        app.fake
            .object("approvals", NS_A, "approval-1234abcd")
            .is_none(),
        "the referenced Approval does not exist until an approver submits"
    );
    let confirmation = app
        .fake
        .object("approvals", NS_A, "approval-1234abcd-confirmation")
        .expect("the confirmation exists");
    let doc = RestoreAuthorization::from_bytes(
        confirmation["spec"]["approvalBytes"]
            .as_str()
            .expect("bytes")
            .as_bytes(),
    )
    .expect("v2");
    assert_eq!(
        doc.authorization_mode,
        logweir_core::approval_policy::ApprovalMode::Governed
    );
}

#[tokio::test]
async fn a_governed_approval_name_must_leave_room_for_its_confirmation() {
    let console = console("prod-governed");
    let app = app(&console);
    let mut body = support::restore_body(&support::golden_plan());
    body["approvalRef"]["name"] = json!("a".repeat(250));
    let created = app
        .post(
            &format!("/api/v1/namespaces/{NS_A}/restores"),
            Some("governed-long-01"),
            &body.to_string(),
        )
        .await;
    assert_eq!(
        created.status,
        422,
        "{}",
        String::from_utf8_lossy(&created.body)
    );
}

#[tokio::test]
async fn the_effective_policy_is_readable_per_namespace() {
    let console = console("prod-governed");
    let app = app(&console);
    let bound = app
        .get(&format!("/api/v1/namespaces/{NS_A}/approval-policy"))
        .await;
    assert_eq!(
        bound.status,
        200,
        "{}",
        String::from_utf8_lossy(&bound.body)
    );
    let item = &bound.json()["item"];
    assert_eq!(item["name"], "prod-governed");
    assert_eq!(item["mode"], "governed");
    assert_eq!(item["legacy"], false);
    assert_eq!(item["requireDistinctPrincipal"], true);
    assert_eq!(
        item["installationDigest"],
        console.settings.policies.digest().as_str()
    );
    assert_eq!(
        item["confirmationKeyId"],
        console
            .settings
            .confirmation
            .as_ref()
            .map(|k| k.key_id().to_string())
            .expect("key")
    );
    let unbound = app
        .get(&format!("/api/v1/namespaces/{NS_B}/approval-policy"))
        .await
        .json();
    assert_eq!(unbound["item"]["name"], "legacy-governed-v1");
    assert_eq!(unbound["item"]["legacy"], true);
    assert!(unbound["item"].get("digest").is_none());
}

// ---------------------------------------------------------------------------
// Governed approval submission and separation of duties (D0)
// ---------------------------------------------------------------------------

/// The confirmation's exact bytes, countersigned by `approver`.
fn countersigned(app: &TestApp, ns: &str, approver: &SigningKey) -> String {
    let confirmation = app
        .fake
        .object("approvals", ns, "approval-1234abcd-confirmation")
        .expect("the confirmation");
    let document = confirmation["spec"]["approvalBytes"]
        .as_str()
        .expect("bytes");
    let mut sidecar: Sidecar = serde_json::from_str(
        confirmation["spec"]["sidecarBytes"]
            .as_str()
            .expect("sidecar"),
    )
    .expect("sidecar");
    let mine = sign_detached(
        approver,
        PAYLOAD_TYPE_RESTORE_AUTHORIZATION,
        document.as_bytes(),
    )
    .expect("sign");
    sidecar.signatures.extend(mine.signatures);
    serde_json::to_string(&sidecar).expect("json")
}

fn restore_name(created: &support::TestResponse) -> String {
    created.json()["item"]["name"]
        .as_str()
        .expect("name")
        .to_string()
}

/// **Self-approval is refused at the API**: the local administrator created
/// the request and holds every action, and still cannot approve it.
#[tokio::test]
async fn the_requester_cannot_approve_their_own_request() {
    let console = console("prod-governed");
    let app = app(&console);
    let created = create(&app, NS_A, "governed-restore-02").await;
    let name = restore_name(&created);
    let sidecar = countersigned(&app, NS_A, &SigningKey::generate_ed25519());
    let refused = app
        .post(
            &format!("/api/v1/namespaces/{NS_A}/restores/{name}/approval"),
            None,
            &json!({"sidecarBytes": sidecar}).to_string(),
        )
        .await;
    assert_eq!(
        refused.status,
        403,
        "{}",
        String::from_utf8_lossy(&refused.body)
    );
    assert_eq!(refused.code(), "forbidden");
    assert!(
        String::from_utf8_lossy(&refused.body).contains(LOCAL_ADMIN_ACTOR),
        "the refusal names the requester"
    );
    assert!(app
        .fake
        .object("approvals", NS_A, "approval-1234abcd")
        .is_none());
}

/// A submission outside an explicit Governed binding has nothing to approve.
#[tokio::test]
async fn a_submission_outside_a_governed_binding_is_policy_mismatch() {
    let console = console("team-ordinary");
    let app = app(&console);
    let created = create(&app, NS_A, "ordinary-restore-04").await;
    let name = restore_name(&created);
    let refused = app
        .post(
            &format!("/api/v1/namespaces/{NS_A}/restores/{name}/approval"),
            None,
            &json!({"sidecarBytes": "{\"payloadType\":\"x\",\"signatures\":[]}"}).to_string(),
        )
        .await;
    assert_eq!(
        refused.status,
        409,
        "{}",
        String::from_utf8_lossy(&refused.body)
    );
    assert_eq!(refused.code(), "policy_mismatch");
}

fn governed_shared_app() -> (SharedApp, Console) {
    let console = console("prod-governed");
    let app = SharedApp::new(
        FakeKube::new(),
        support::idp::MockIdp::new(ISSUER, &[]),
        SharedOptions {
            bindings: support::RoleBindings {
                revision: "p192-1".into(),
                bindings: vec![
                    support::binding(Role::Operator, NS_A, &["ops"]),
                    support::binding(Role::Approver, NS_A, &["approvers"]),
                    support::binding(Role::Administrator, NS_A, &["admins"]),
                    support::binding(Role::Approver, NS_A, &["admins"]),
                ],
            },
            approval: Arc::clone(&console.settings),
            ..SharedOptions::default()
        },
    );
    (app, console)
}

async fn shared_create(
    app: &SharedApp,
    subject: &str,
    group: &str,
    key: &str,
) -> support::TestResponse {
    let cookie = app.session_cookie(subject, &[group]);
    let csrf = app.csrf_for(subject);
    app.post(
        &format!("/api/v1/namespaces/{NS_A}/restores"),
        &cookie,
        Some(&csrf),
        Some(key),
        &support::restore_body(&support::golden_plan()).to_string(),
    )
    .await
}

async fn shared_submit(
    app: &SharedApp,
    subject: &str,
    group: &str,
    restore: &str,
    sidecar: &str,
) -> support::TestResponse {
    let cookie = app.session_cookie(subject, &[group]);
    let csrf = app.csrf_for(subject);
    app.post(
        &format!("/api/v1/namespaces/{NS_A}/restores/{restore}/approval"),
        &cookie,
        Some(&csrf),
        None,
        &json!({"sidecarBytes": sidecar}).to_string(),
    )
    .await
}

/// **A distinct approver submits, and the referenced Approval is created**
/// from the confirmation's exact bytes with both signatures; a replay is 200.
/// An operator — the role that requests — cannot submit at all.
#[tokio::test]
async fn a_distinct_approver_submits_and_an_operator_cannot() {
    let (app, console) = governed_shared_app();
    let created = shared_create(&app, "alice", "ops", "governed-shared-01").await;
    assert_eq!(
        created.status,
        201,
        "{}",
        String::from_utf8_lossy(&created.body)
    );
    let name = restore_name(&created);
    let bob = SigningKey::generate_ed25519();
    let sidecar = countersigned(&app.app, NS_A, &bob);

    let operator = shared_submit(&app, "alice", "ops", &name, &sidecar).await;
    assert_eq!(
        operator.status, 403,
        "the operator role never submits a governed approval"
    );

    let submitted = shared_submit(&app, "bob", "approvers", &name, &sidecar).await;
    assert_eq!(
        submitted.status,
        201,
        "{}",
        String::from_utf8_lossy(&submitted.body)
    );
    let stored = app
        .app
        .fake
        .object("approvals", NS_A, "approval-1234abcd")
        .expect("the referenced Approval now exists");
    let confirmation = app
        .app
        .fake
        .object("approvals", NS_A, "approval-1234abcd-confirmation")
        .expect("confirmation");
    assert_eq!(
        stored["spec"]["approvalBytes"], confirmation["spec"]["approvalBytes"],
        "the exact confirmed bytes, never re-serialised"
    );
    let merged: Sidecar =
        serde_json::from_str(stored["spec"]["sidecarBytes"].as_str().expect("sidecar"))
            .expect("sidecar");
    assert_eq!(merged.signatures.len(), 2);
    let document = stored["spec"]["approvalBytes"].as_str().expect("bytes");
    let sidecar_text = stored["spec"]["sidecarBytes"].as_str().expect("sidecar");
    assert!(verify(document, sidecar_text, &console.public));
    assert!(verify(document, sidecar_text, &bob.verifying_key()));

    let replay = shared_submit(&app, "bob", "approvers", &name, &sidecar).await;
    assert_eq!(
        replay.status,
        200,
        "{}",
        String::from_utf8_lossy(&replay.body)
    );
    assert_eq!(replay.json()["replayed"], true);
}

/// **An administrator is not a self-approval bypass**: bound as Operator AND
/// Approver (via `admins`), the requester is still refused.
#[tokio::test]
async fn an_administrator_who_requested_cannot_approve() {
    let (app, _console) = governed_shared_app();
    let created = shared_create(&app, "carol", "admins", "governed-shared-02").await;
    assert_eq!(
        created.status,
        201,
        "{}",
        String::from_utf8_lossy(&created.body)
    );
    let name = restore_name(&created);
    let sidecar = countersigned(&app.app, NS_A, &SigningKey::generate_ed25519());
    let refused = shared_submit(&app, "carol", "admins", &name, &sidecar).await;
    assert_eq!(
        refused.status,
        403,
        "{}",
        String::from_utf8_lossy(&refused.body)
    );
    assert!(String::from_utf8_lossy(&refused.body).contains("requested this restore"));
}

#[tokio::test]
async fn an_expired_or_uncountersigned_request_cannot_be_approved() {
    let (app, _console) = governed_shared_app();
    let created = shared_create(&app, "alice", "ops", "governed-shared-03").await;
    let name = restore_name(&created);
    // The console's signature alone adds nothing.
    let confirmation = app
        .app
        .fake
        .object("approvals", NS_A, "approval-1234abcd-confirmation")
        .expect("confirmation");
    let only_console = confirmation["spec"]["sidecarBytes"]
        .as_str()
        .expect("sidecar");
    let refused = shared_submit(&app, "bob", "approvers", &name, only_console).await;
    assert_eq!(
        refused.status,
        422,
        "{}",
        String::from_utf8_lossy(&refused.body)
    );

    // Past the policy's maxAgeSeconds.
    let sidecar = countersigned(&app.app, NS_A, &SigningKey::generate_ed25519());
    app.app.clock.advance(86_400 + 1);
    let late = shared_submit(&app, "bob", "approvers", &name, &sidecar).await;
    assert_eq!(late.status, 409, "{}", String::from_utf8_lossy(&late.body));
    assert_eq!(late.code(), "state_conflict");
}

/// Startup refuses a served namespace bound to a policy with no console key.
#[test]
fn a_bound_namespace_without_a_console_key_refuses_to_start() {
    let dir = std::env::temp_dir().join(format!("logweir-p192-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("dir");
    let path = dir.join("policy.yaml");
    std::fs::write(
        &path,
        format!("{POLICIES}namespaces:\n  {NS_A}: team-ordinary\n"),
    )
    .expect("write");
    let err = ApprovalSettings::load(Some(&path), None, &[NS_A.to_string()]).expect_err("refused");
    assert!(err.contains("confirmationKeyFile"), "{err}");
    assert!(ApprovalSettings::load(Some(&path), None, &[NS_B.to_string()]).is_ok());
    let _ = std::fs::remove_dir_all(&dir);
}

/// A confirmation issued under a policy the installation has since replaced is
/// not approvable: the approver is told to have the Restore submitted again
/// (D0: "binding/policy mismatch requires re-confirmation/re-approval").
#[tokio::test]
async fn a_confirmation_from_before_a_policy_change_cannot_be_approved() {
    let (app, _console) = governed_shared_app();
    let created = shared_create(&app, "alice", "ops", "governed-shared-04").await;
    assert_eq!(
        created.status,
        201,
        "{}",
        String::from_utf8_lossy(&created.body)
    );
    let name = restore_name(&created);
    let sidecar = countersigned(&app.app, NS_A, &SigningKey::generate_ed25519());

    // The same cluster, a console restarted under an EDITED Governed policy.
    let edited = ApprovalPolicySet::parse(&format!(
        "{}  - name: prod-governed\n    mode: Governed\n    maxAgeSeconds: 7200\nnamespaces:\n  {NS_A}: prod-governed\n",
        "allowOrdinaryConfirmation: true\npolicies:\n  - name: team-ordinary\n    mode: Ordinary\n"
    ))
    .expect("valid");
    let restarted = SharedApp::new(
        app.app.fake.clone(),
        support::idp::MockIdp::new(ISSUER, &[]),
        SharedOptions {
            bindings: support::RoleBindings {
                revision: "p192-2".into(),
                bindings: vec![support::binding(Role::Approver, NS_A, &["approvers"])],
            },
            approval: Arc::new(ApprovalSettings {
                policies: edited,
                confirmation: Some(
                    ConfirmationKey::from_key(SigningKey::generate_ed25519()).expect("key"),
                ),
            }),
            ..SharedOptions::default()
        },
    );
    let refused = shared_submit(&restarted, "bob", "approvers", &name, &sidecar).await;
    assert_eq!(
        refused.status,
        409,
        "{}",
        String::from_utf8_lossy(&refused.body)
    );
    assert_eq!(refused.code(), "policy_mismatch");
    assert!(restarted
        .app
        .fake
        .object("approvals", NS_A, "approval-1234abcd")
        .is_none());
}
