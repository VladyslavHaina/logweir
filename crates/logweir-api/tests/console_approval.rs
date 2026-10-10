//! PROD-16.2 at the product API — **two-person approval in the console**.
//!
//! A namespace bound to a policy whose `approverSignature` is `Console`
//! (`two-person`): an operator requests, a SECOND person signs in and clicks
//! Approve, and the console signs the approval. These rows are the console's
//! half of the threat table in the PROD-16.2 decision record: self-approval
//! in every spelling, a planted or replayed confirmation, replays and races,
//! role confusion, cross-site requests, and what the console will and will
//! not put its signature on.
//!
//! They sign for real. The console key is an in-memory Ed25519 key; every
//! stored document is verified here with the verifier the controller and the
//! runner use, and the happy path hands the stored Approval to the
//! controller's own verdict function.

mod support;

use std::io::Write;
use std::sync::{Arc, Mutex};

use axum::body::Body;
use chrono::{DateTime, Utc};
use http::Request;
use logweir_api::app::Clock as _;
use logweir_api::approval::{ApprovalSettings, ConfirmationKey};
use logweir_api::auth::oidc::Identity;
use logweir_api::auth::session::{self, SessionClaims};
use logweir_api::authz::Role;
use logweir_core::approval_policy::{
    ApprovalPolicy, ApprovalPolicySet, Approver, ApproverSignature, ExpectedSubject, Requester,
    RestoreAuthorization, PAYLOAD_TYPE_RESTORE_AUTHORIZATION,
};
use logweir_evidence::keys::SigningKey;
use logweir_evidence::Sidecar;
use serde_json::{json, Value};
use support::{
    FakeKube, Options, SharedApp, SharedOptions, TestApp, TestResponse, ISSUER, NS_A, NS_B,
    SHARED_HOST, SHARED_ORIGIN,
};

/// `team-a` is bound per row; the policies are one two-person, one strict
/// (the SAME shape with a personal key) and one confirm.
const POLICIES: &str = "allowOrdinaryConfirmation: true
policies:
  - name: prod-pair
    mode: two-person
    maxAgeSeconds: 3600
  - name: prod-strict
    mode: strict
    maxAgeSeconds: 3600
  - name: team-confirm
    mode: confirm
";

const TICKET: &str = "CHG-4711";

struct Console {
    settings: Arc<ApprovalSettings>,
    public: logweir_evidence::keys::VerifyingKey,
    /// The key again, as PKCS#8 text held in this process only: a policy
    /// ROLLOUT is the same console key under another policy document, and
    /// `ConfirmationKey` deliberately does not clone.
    pkcs8: String,
}

impl Console {
    fn key(&self) -> &ConfirmationKey {
        self.settings.confirmation.as_ref().expect("a console key")
    }

    fn policy(&self) -> ApprovalPolicy {
        self.settings
            .policies
            .resolve(NS_A)
            .bound()
            .cloned()
            .expect("team-a is bound")
    }
}

fn console_over(document: &str, binding: &str) -> Console {
    let pkcs8 = SigningKey::generate_ed25519()
        .to_pkcs8_pem()
        .expect("pkcs8")
        .to_string();
    console_with_key(&pkcs8, document, binding)
}

/// A console holding the key `pkcs8` spells, under `document` with `team-a`
/// bound to `binding`.
fn console_with_key(pkcs8: &str, document: &str, binding: &str) -> Console {
    let key = SigningKey::from_pkcs8_pem(pkcs8).expect("the key parses");
    let public = key.verifying_key();
    let policies =
        ApprovalPolicySet::parse(&format!("{document}namespaces:\n  {NS_A}: {binding}\n"))
            .expect("valid");
    Console {
        settings: Arc::new(ApprovalSettings {
            policies,
            confirmation: Some(ConfirmationKey::from_key(key).expect("key")),
            ..ApprovalSettings::default()
        }),
        public,
        pkcs8: pkcs8.to_string(),
    }
}

fn console(binding: &str) -> Console {
    console_over(POLICIES, binding)
}

fn bindings() -> support::RoleBindings {
    support::RoleBindings {
        revision: "p162-1".into(),
        bindings: vec![
            support::binding(Role::Viewer, NS_A, &["viewers"]),
            support::binding(Role::Operator, NS_A, &["ops"]),
            support::binding(Role::Approver, NS_A, &["approvers"]),
            // An administrator who is NOT an approver.
            support::binding(Role::Administrator, NS_A, &["admins"]),
        ],
    }
}

fn shared(console: &Console) -> SharedApp {
    shared_over(FakeKube::new(), console)
}

fn shared_over(fake: FakeKube, console: &Console) -> SharedApp {
    SharedApp::new(
        fake,
        support::idp::MockIdp::new(ISSUER, &[]),
        SharedOptions {
            bindings: bindings(),
            approval: Arc::clone(&console.settings),
            ..SharedOptions::default()
        },
    )
}

/// The two-person namespace's shared console.
fn pair_app() -> (SharedApp, Console) {
    let console = console("prod-pair");
    (shared(&console), console)
}

/// A session, and its synchronizer token, for any issuer and subject — the
/// rows about identity need issuers and subjects the helper in `support`
/// (which always signs in at `ISSUER`) cannot spell.
struct Session {
    cookie: String,
    csrf: String,
}

fn session_as(app: &SharedApp, sid: &str, issuer: &str, subject: &str, groups: &[&str]) -> Session {
    let now = app.app.clock.now();
    let identity = Identity {
        issuer: issuer.to_string(),
        subject: subject.to_string(),
        display_name: format!("{subject} display"),
        groups: groups.iter().map(|g| (*g).to_string()).collect(),
        auth_time: now,
    };
    let claims = SessionClaims::issue(&identity, sid.to_string(), app.keys.version(), now, 900);
    Session {
        cookie: session::set_cookie(&app.keys, &claims, now)
            .split(';')
            .next()
            .expect("a cookie pair")
            .to_string(),
        csrf: app.keys.csrf_token(sid),
    }
}

/// A session at the configured issuer.
fn signed_in(app: &SharedApp, subject: &str, groups: &[&str]) -> Session {
    session_as(app, &format!("sid-{subject}"), ISSUER, subject, groups)
}

fn restore_request(approval: &str) -> Value {
    let mut body = support::restore_body(&support::golden_plan());
    body["approvalRef"]["name"] = json!(approval);
    body["ticket"] = json!(TICKET);
    body
}

/// What a request left behind.
struct Requested {
    restore: String,
    approval: String,
    confirmation: String,
}

/// `subject` (an operator) requests a restore; `tag` keeps the idempotency
/// key and the approval name of several requests in one namespace apart.
async fn request_as(app: &SharedApp, who: &Session, tag: &str, body: Value) -> TestResponse {
    app.post(
        &format!("/api/v1/namespaces/{NS_A}/restores"),
        &who.cookie,
        Some(&who.csrf),
        Some(&format!("p162-request-{tag}")),
        &body.to_string(),
    )
    .await
}

async fn requested(app: &SharedApp, subject: &str, tag: &str) -> Requested {
    let approval = format!("approval-{tag}");
    let created = request_as(
        app,
        &signed_in(app, subject, &["ops"]),
        tag,
        restore_request(&approval),
    )
    .await;
    assert_eq!(created.status, 201, "{}", created.text());
    let json = created.json();
    assert_eq!(json["authorization"]["state"], "awaitingApproval");
    assert_eq!(json["authorization"]["operatorMode"], "two-person");
    Requested {
        restore: json["item"]["name"].as_str().expect("a name").to_string(),
        confirmation: format!("{approval}-confirmation"),
        approval,
    }
}

async fn view(app: &SharedApp, who: &Session, restore: &str) -> TestResponse {
    app.get(
        &format!("/api/v1/namespaces/{NS_A}/restores/{restore}/approval-request"),
        &who.cookie,
    )
    .await
}

/// The `confirmationSha256` the approver is shown for a pending request.
async fn shown(app: &SharedApp, who: &Session, restore: &str) -> String {
    let seen = view(app, who, restore).await;
    assert_eq!(seen.status, 200, "{}", seen.text());
    seen.json()["item"]["confirmationSha256"]
        .as_str()
        .unwrap_or_else(|| panic!("a pending request shows its digest: {}", seen.text()))
        .to_string()
}

async fn click_with(app: &SharedApp, who: &Session, restore: &str, body: &Value) -> TestResponse {
    app.post(
        &format!("/api/v1/namespaces/{NS_A}/restores/{restore}/console-approval"),
        &who.cookie,
        Some(&who.csrf),
        None,
        &body.to_string(),
    )
    .await
}

async fn click(app: &SharedApp, who: &Session, restore: &str, sha: &str) -> TestResponse {
    click_with(app, who, restore, &json!({"confirmationSha256": sha})).await
}

/// The digest of whatever request is stored now, read from the cluster — for
/// a row whose point is that the click is refused for another reason than a
/// stale digest.
fn stored_sha(app: &SharedApp, confirmation: &str) -> String {
    let stored = app
        .app
        .fake
        .object("approvals", NS_A, confirmation)
        .expect("the request is stored");
    logweir_core::ids::sha256_prefixed(
        stored["spec"]["approvalBytes"]
            .as_str()
            .expect("bytes")
            .as_bytes(),
    )
}

fn approvals_posted(fake: &FakeKube) -> usize {
    fake.requests()
        .into_iter()
        .filter(|r| r.method == "POST" && r.path.ends_with("/approvals"))
        .count()
}

fn verifies(document: &str, sidecar: &str, key: &logweir_evidence::keys::VerifyingKey) -> bool {
    let sidecar: Sidecar = serde_json::from_str(sidecar).expect("a sidecar");
    logweir_evidence::verify::verify_detached(
        key,
        PAYLOAD_TYPE_RESTORE_AUTHORIZATION,
        document.as_bytes(),
        &sidecar,
    )
    .is_ok()
}

fn stored_document(app: &SharedApp, name: &str) -> (RestoreAuthorization, String, String) {
    let stored = app
        .app
        .fake
        .object("approvals", NS_A, name)
        .unwrap_or_else(|| panic!("Approval {name} is stored"));
    let bytes = stored["spec"]["approvalBytes"].as_str().expect("bytes");
    let sidecar = stored["spec"]["sidecarBytes"].as_str().expect("sidecar");
    (
        RestoreAuthorization::from_bytes(bytes.as_bytes()).expect("a v2 document"),
        bytes.to_string(),
        sidecar.to_string(),
    )
}

/// Replace the stored request of `requested` with `document`, signed by
/// `key`, under whatever metadata `annotations` says.
fn plant(
    app: &SharedApp,
    requested: &Requested,
    document: &RestoreAuthorization,
    sidecar: &str,
    annotations: Value,
) {
    app.app.fake.seed(
        "approvals",
        NS_A,
        json!({
            "metadata": {"name": requested.confirmation, "annotations": annotations},
            "spec": {
                "subjectRef": {"kind": "Restore", "name": requested.restore},
                "planHash": document.plan_hash,
                "approvalBytes": String::from_utf8(document.to_bytes()).expect("utf-8"),
                "sidecarBytes": sidecar,
            }
        }),
    );
}

fn signed_by_console(console: &Console, document: &RestoreAuthorization) -> String {
    serde_json::to_string(&console.key().sign(&document.to_bytes()).expect("sig")).expect("json")
}

// ------------------------------------------------------------ log capture

#[derive(Clone, Default)]
struct Buffer(Arc<Mutex<Vec<u8>>>);

struct BufferWriter(Arc<Mutex<Vec<u8>>>);

impl Write for BufferWriter {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .expect("the buffer lock holds")
            .extend_from_slice(data);
        Ok(data.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Buffer {
    type Writer = BufferWriter;

    fn make_writer(&'a self) -> Self::Writer {
        BufferWriter(Arc::clone(&self.0))
    }
}

impl Buffer {
    fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().expect("the buffer lock holds")).into_owned()
    }

    /// The audit record and its notes for one request id.
    fn audit(&self, response: &TestResponse) -> (Value, Value) {
        let id = response.header("x-request-id").expect("a request id");
        let found: Vec<(Value, Value)> = self
            .text()
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .filter(|line| line["target"] == "logweir_api::audit")
            .filter_map(|line| {
                let record = line["fields"]["audit"]
                    .as_str()
                    .and_then(|audit| serde_json::from_str::<Value>(audit).ok())?;
                let notes = line["fields"]["notes"]
                    .as_str()
                    .and_then(|notes| serde_json::from_str::<Value>(notes).ok())
                    .unwrap_or(Value::Null);
                Some((record, notes))
            })
            .filter(|(record, _)| record["auditId"] == id.as_str())
            .collect();
        assert_eq!(
            found.len(),
            1,
            "one audit record for {id}:\n{}",
            self.text()
        );
        found[0].clone()
    }
}

fn capture() -> (Buffer, tracing::subscriber::DefaultGuard) {
    let buffer = Buffer::default();
    let subscriber = tracing_subscriber::fmt()
        .json()
        .with_writer(buffer.clone())
        .with_env_filter(logweir_api::audit::log_filter_from("debug"))
        .finish();
    let guard = tracing::subscriber::set_default(subscriber);
    (buffer, guard)
}

// ---------------------------------------------------------------------------
// The click
// ---------------------------------------------------------------------------

/// **Two people, one click, no key.** alice requests; bob signs in, is shown
/// the request from its verified bytes, and clicks Approve. The stored
/// Approval is the request's document with `approver`, `approvedAt` and
/// `formatVersion` 2.2.0 added and NOTHING else changed, signed by the
/// console key — and the controller's own verdict function accepts it and
/// names bob.
///
/// KILLS: an approval built from anything but the verified request; an
/// approval the readers refuse; an audit record that does not say who
/// approved whose request.
#[tokio::test]
async fn a_second_person_approves_with_one_click_and_the_console_signs_who_and_when() {
    let (log, _guard) = capture();
    let (app, console) = pair_app();
    let r = requested(&app, "alice", "happy").await;
    // The request: today's governed confirmation, byte for byte a 2.0.0
    // document, and nothing under the Restore's approvalRef yet.
    let (request, request_bytes, request_sidecar) = stored_document(&app, &r.confirmation);
    assert!(request_bytes.starts_with("{\"formatVersion\":\"2.0.0\","));
    assert_eq!(request.requester.principal_id(), format!("{ISSUER}#alice"));
    assert!(request.approver.is_none() && request.approved_at.is_none());
    assert!(verifies(&request_bytes, &request_sidecar, &console.public));
    assert!(app
        .app
        .fake
        .object("approvals", NS_A, &r.approval)
        .is_none());

    // What bob is shown, before the click.
    let bob = signed_in(&app, "bob", &["approvers"]);
    let seen = view(&app, &bob, &r.restore).await;
    assert_eq!(seen.status, 200, "{}", seen.text());
    let item = seen.json()["item"].clone();
    assert_eq!(item["state"], "pending");
    assert_eq!(item["requester"], format!("{ISSUER}#alice"));
    assert_eq!(item["planHash"], request.plan_hash);
    assert_eq!(item["approvalSubject"], "ordinary");
    assert_eq!(item["policy"], "prod-pair");
    assert_eq!(item["policyDigest"], console.policy().digest());
    assert_eq!(item["ticket"], TICKET);
    assert_eq!(
        item["expiresAt"],
        json!(request.expires_at),
        "the expiry is the signed one"
    );
    assert_eq!(
        item["confirmationSha256"],
        logweir_core::ids::sha256_prefixed(request_bytes.as_bytes())
    );
    assert_eq!(item["approve"]["offered"], true, "{item}");
    // THE SCOPE: every topic and the name it is restored under, the target
    // cluster, the source and the coverage, from the plan the request names.
    assert_eq!(item["scopeComplete"], true, "{item}");
    assert!(item.get("scopeIncomplete").is_none());
    let scope = &item["scope"];
    assert_eq!(scope["topicsCount"], 2);
    assert_eq!(
        scope["topics"],
        json!([
            {"source": "orders", "target": "restore-20260907T140500Z-orders", "originalName": false},
            {"source": "payments", "target": "restore-20260907T140500Z-payments", "originalName": false}
        ])
    );
    assert_eq!(
        scope["target"]["bootstrapServers"],
        json!(["localhost:9092"])
    );
    assert_eq!(scope["target"]["mode"], "newTopic");
    assert_eq!(
        scope["source"]["storage"]["location"],
        "s3://kafka-backups/drill-demo"
    );
    assert_eq!(scope["source"]["backup"], "01JB7Z0000000000000000000B");
    assert_eq!(scope["recovery"]["pointInTime"], "2026-09-07T14:05:00Z");
    assert_eq!(scope["verification"]["coverage"], "sampled");
    let before = approvals_posted(&app.app.fake);

    // The click, a minute later.
    app.app.clock.advance(60);
    let bob = signed_in(&app, "bob", &["approvers"]);
    let clicked = click(
        &app,
        &bob,
        &r.restore,
        item["confirmationSha256"].as_str().expect("sha"),
    )
    .await;
    assert_eq!(clicked.status, 201, "{}", clicked.text());
    assert_eq!(clicked.json()["replayed"], false);
    assert_eq!(approvals_posted(&app.app.fake), before + 1);

    let (approved, bytes, sidecar) = stored_document(&app, &r.approval);
    assert!(bytes.starts_with("{\"formatVersion\":\"2.2.0\","));
    assert!(verifies(&bytes, &sidecar, &console.public));
    assert_eq!(
        approved.approver,
        Some(Approver {
            issuer: ISSUER.into(),
            subject: "bob".into()
        })
    );
    assert_eq!(approved.approved_at, Some(app.app.clock.now()));
    // EVERYTHING ELSE IS THE VERIFIED REQUEST'S.
    assert_eq!(
        RestoreAuthorization {
            format_version: request.format_version.clone(),
            approver: None,
            approved_at: None,
            ..approved.clone()
        },
        request
    );
    // One signature, the console's: no second key anywhere.
    let parsed: Sidecar = serde_json::from_str(&sidecar).expect("a sidecar");
    assert_eq!(parsed.signatures.len(), 1);
    assert_eq!(parsed.signatures[0].keyid, console.key().key_id());

    // THE CONTROLLER'S OWN VERDICT over exactly what was stored.
    let restore = app
        .app
        .fake
        .object("restores", NS_A, &r.restore)
        .expect("the Restore");
    let expected = ExpectedSubject {
        namespace: NS_A.into(),
        name: r.restore.clone(),
        uid: restore["metadata"]["uid"].as_str().expect("uid").into(),
        plan_hash: logweir_core::ids::sha256_prefixed(
            restore["spec"]["planBytes"]
                .as_str()
                .expect("plan")
                .as_bytes(),
        ),
    };
    let verdict = weirkeeper::controllers::approval::evaluate_authorization_v2(
        bytes.as_bytes(),
        sidecar.as_bytes(),
        &trust_of(&console),
        app.app.clock.now(),
        &expected,
        &console.policy(),
    )
    .expect("the controller verifies what the console signed");
    assert_eq!(verdict.approver, format!("{ISSUER}#bob"));
    assert_eq!(verdict.matched_key_id, console.key().key_id());
    // ... and it refuses the REQUEST offered in its place.
    let pending = weirkeeper::controllers::approval::evaluate_authorization_v2(
        request_bytes.as_bytes(),
        request_sidecar.as_bytes(),
        &trust_of(&console),
        app.app.clock.now(),
        &expected,
        &console.policy(),
    )
    .expect_err("a request is not an approval");
    assert_eq!(pending.reason(), "GovernedApprovalRequired");

    // THE AUDIT RECORD: requester, approver, plan hash, policy, outcome.
    let (record, notes) = log.audit(&clicked);
    assert_eq!(record["action"], "approval.submit");
    assert_eq!(record["decision"], "allow");
    assert_eq!(record["actorId"], format!("{ISSUER}#bob"));
    assert_eq!(record["planHash"], request.plan_hash);
    assert_eq!(
        record["policyDigest"],
        format!("prod-pair@{}", console.policy().digest())
    );
    assert_eq!(record["httpStatus"], 201);
    assert_eq!(record["failureCode"], "");
    assert_eq!(notes["requester"], format!("{ISSUER}#alice"));
    assert_eq!(notes["approverPrincipal"], format!("{ISSUER}#bob"));
    assert_eq!(notes["separation"], "distinct");
    // The scope was shown complete: every topic of the plan, counted twice.
    assert_eq!(notes["scope"], "complete");
    assert_eq!(notes["scopeTopicsShown"], "2");
    assert_eq!(notes["scopeTopicsInPlan"], "2");
    assert_eq!(notes["approval"], format!("{NS_A}/{}", r.approval));
    assert!(notes["approvedAt"].is_string() && notes["expiresAt"].is_string());

    // The view now says who approved, from the bytes the console signed.
    let after = view(&app, &bob, &r.restore).await.json()["item"].clone();
    assert_eq!(after["state"], "approved");
    assert_eq!(after["approver"], format!("{ISSUER}#bob"));
    assert_eq!(after["approve"]["offered"], false);
}

/// The trust a namespace gives the console key: `ConsoleConfirmation` and
/// nothing else, as the identity hook writes it.
fn trust_of(console: &Console) -> weirkeeper::trust::ResolvedTrust {
    use weirkeeper::crds::trust_policy::{
        KeyAlgorithm, KeyPrincipal, KeyState, KeyUsage, TrustPolicy, TrustPolicySpec, TrustedKey,
    };
    let at = |s: &str| {
        DateTime::parse_from_rfc3339(s)
            .expect("an instant")
            .with_timezone(&Utc)
    };
    weirkeeper::trust::from_policy(&TrustPolicy {
        metadata: kube::api::ObjectMeta {
            name: Some("org-default".into()),
            uid: Some("uid-org-default".into()),
            generation: Some(1),
            resource_version: Some("1".into()),
            ..Default::default()
        },
        spec: TrustPolicySpec {
            default: false,
            namespaces: Some(vec![NS_A.to_string()]),
            allowed_target_cluster_ids: None,
            keys: vec![TrustedKey {
                key_id: console.key().key_id().to_string(),
                spki_pem: console.key().public_pem().to_string(),
                algorithm: KeyAlgorithm::Ed25519,
                usages: vec![KeyUsage::ConsoleConfirmation],
                principal: KeyPrincipal {
                    id: "console:logweir-system/logweir-console-confirmation".into(),
                    display: None,
                },
                not_before: at("2026-01-01T00:00:00Z"),
                not_after: at("2099-01-01T00:00:00Z"),
                state: KeyState::Active,
                retired_at: None,
                revoked_at: None,
                revocation_reason: None,
                revocation_effective_from: None,
            }],
        },
        status: None,
    })
}

// ---------------------------------------------------------------------------
// Self-approval
// ---------------------------------------------------------------------------

/// What a refused click leaves: the status and code asked for, nothing
/// stored under the Restore's approvalRef, and no POST to `approvals`.
fn assert_refused(
    app: &SharedApp,
    r: &Requested,
    response: &TestResponse,
    status: u16,
    code: &str,
) {
    response.assert_problem(status, code);
    assert!(
        app.app
            .fake
            .object("approvals", NS_A, &r.approval)
            .is_none(),
        "a refused click stores nothing"
    );
}

/// **The requester cannot approve their own request**, whatever roles they
/// hold, from any session, and under every spelling of themselves: another
/// case, an issuer written with a trailing slash. And a principal the
/// console cannot compare — trailing whitespace, another Unicode
/// normalisation, another issuer — is never a second person.
///
/// THE CONTROL is last: bob, a second subject of the same issuer, approves
/// the very request every row above was refused for.
#[tokio::test]
async fn the_requester_cannot_approve_their_own_request_under_any_spelling() {
    let (log, _guard) = capture();
    let (app, _console) = pair_app();
    let r = requested(&app, "alice", "self").await;
    let sha = stored_sha(&app, &r.confirmation);
    let both = &["ops", "approvers", "admins"];
    let cases: Vec<(&str, Session, &str)> = vec![
        (
            "the requester, holding every role",
            signed_in(&app, "alice", both),
            "is the requester",
        ),
        (
            "the requester, from a second session",
            session_as(&app, "sid-another-browser", ISSUER, "alice", both),
            "is the requester",
        ),
        (
            "the requester in another case",
            session_as(&app, "sid-case", ISSUER, "ALICE", both),
            "is the requester",
        ),
        (
            "the requester behind an issuer with a trailing slash",
            session_as(&app, "sid-slash", &format!("{ISSUER}/"), "alice", both),
            "is the requester",
        ),
        (
            "the same subject from another issuer",
            session_as(
                &app,
                "sid-other-idp",
                "https://other-idp.test",
                "alice",
                both,
            ),
            "two issuers",
        ),
        (
            "another subject from another issuer",
            session_as(
                &app,
                "sid-other-idp-bob",
                "https://other-idp.test",
                "bob",
                both,
            ),
            "two issuers",
        ),
        (
            "a subject with trailing whitespace",
            session_as(&app, "sid-space", ISSUER, "alice ", both),
            "not in a form that can be compared",
        ),
        (
            "a subject with a zero-width space",
            session_as(&app, "sid-zw", ISSUER, "ali\u{200b}ce", both),
            "not in a form that can be compared",
        ),
        (
            "a decomposed spelling",
            session_as(&app, "sid-nfd", ISSUER, "alice\u{301}", both),
            "not in a form that can be compared",
        ),
        (
            "a service account",
            session_as(
                &app,
                "sid-sa",
                ISSUER,
                "system:serviceaccount:team-a:approver",
                both,
            ),
            "Kubernetes system identity",
        ),
    ];
    for (label, who, needle) in cases {
        let seen = view(&app, &who, &r.restore).await.json()["item"].clone();
        assert_eq!(seen["approve"]["offered"], false, "{label}: {seen}");
        let clicked = click(&app, &who, &r.restore, &sha).await;
        assert_refused(&app, &r, &clicked, 403, "forbidden");
        assert!(
            clicked.text().contains(needle),
            "{label}: {}",
            clicked.text()
        );
        let (record, notes) = log.audit(&clicked);
        assert_eq!(record["decision"], "deny", "{label}");
        assert_eq!(record["failureCode"], "self_approval_forbidden", "{label}");
        assert_eq!(notes["separation"], "refused", "{label}");
        assert_eq!(notes["requester"], format!("{ISSUER}#alice"), "{label}");
    }
    // The view names the requester's own case by its own word.
    let own = view(&app, &signed_in(&app, "alice", both), &r.restore)
        .await
        .json()["item"]["approve"]["refusal"]
        .clone();
    assert_eq!(own, "requester");

    // THE CONTROL.
    let bob = signed_in(&app, "bob", &["approvers"]);
    let clicked = click(&app, &bob, &r.restore, &sha).await;
    assert_eq!(clicked.status, 201, "{}", clicked.text());
}

/// **A second subject of the same issuer is a second person even when the
/// session's issuer is spelled with a trailing slash** (the control for the
/// trailing-slash row above).
#[tokio::test]
async fn an_issuer_with_a_trailing_slash_is_the_same_issuer() {
    let (app, _console) = pair_app();
    let r = requested(&app, "alice", "slash").await;
    let sha = stored_sha(&app, &r.confirmation);
    let bob = session_as(
        &app,
        "sid-bob-slash",
        &format!("{ISSUER}/"),
        "bob",
        &["approvers"],
    );
    let clicked = click(&app, &bob, &r.restore, &sha).await;
    assert_eq!(clicked.status, 201, "{}", clicked.text());
    let (approved, _, _) = stored_document(&app, &r.approval);
    assert_eq!(
        approved.approver.expect("an approver").issuer,
        format!("{ISSUER}/"),
        "the approver is recorded as the session attested them"
    );
}

/// **A requester the console could never tell apart from an approver makes no
/// request at all**: a service account, and an identity that is not in a
/// comparable form. Nothing is created.
#[tokio::test]
async fn a_requester_who_cannot_be_compared_makes_no_two_person_request() {
    let (log, _guard) = capture();
    let (app, _console) = pair_app();
    for (label, sid, subject, needle) in [
        (
            "a service account",
            "sid-sa-req",
            "system:serviceaccount:team-a:deployer",
            "Kubernetes system identity",
        ),
        (
            "trailing whitespace",
            "sid-sp-req",
            "alice ",
            "not in a form that can be compared",
        ),
        (
            "a non-ASCII subject",
            "sid-uni-req",
            "jos\u{e9}",
            "not in a form that can be compared",
        ),
    ] {
        let who = session_as(&app, sid, ISSUER, subject, &["ops"]);
        let created =
            request_as(&app, &who, sid, restore_request(&format!("approval-{sid}"))).await;
        created.assert_problem(409, "policy_mismatch");
        assert!(
            created.text().contains(needle),
            "{label}: {}",
            created.text()
        );
        let (record, _) = log.audit(&created);
        assert_eq!(record["failureCode"], "requester_not_comparable", "{label}");
    }
    assert_eq!(app.app.fake.count("restores", NS_A), 0);
    assert_eq!(app.app.fake.count("approvals", NS_A), 0);
}

/// **A request the console signed for the local administrator or for a
/// service account is never approved** — a document an older or misconfigured
/// console could have signed with the same key. bob is a genuine second
/// person here; what refuses is the REQUESTER inside the verified bytes.
#[tokio::test]
async fn a_request_signed_for_the_local_admin_or_a_service_account_is_never_approved() {
    let (app, console) = pair_app();
    for (tag, issuer, subject, needle) in [
        (
            "la",
            "urn:logweir:local-admin",
            "admin",
            "administrator console",
        ),
        (
            "sa",
            ISSUER,
            "system:serviceaccount:team-a:deployer",
            "Kubernetes system identity",
        ),
    ] {
        let r = requested(&app, "alice", tag).await;
        let (mut doc, _, _) = stored_document(&app, &r.confirmation);
        doc.requester = Requester {
            issuer: issuer.into(),
            subject: subject.into(),
        };
        plant(
            &app,
            &r,
            &doc,
            &signed_by_console(&console, &doc),
            json!({}),
        );
        let bob = signed_in(&app, "bob", &["approvers"]);
        let seen = view(&app, &bob, &r.restore).await.json()["item"].clone();
        assert_eq!(seen["state"], "pending", "{seen}");
        assert_eq!(seen["approve"]["offered"], false, "{seen}");
        assert_eq!(seen["approve"]["refusal"], "notSecondPerson", "{seen}");
        let clicked = click(&app, &bob, &r.restore, &stored_sha(&app, &r.confirmation)).await;
        assert_refused(&app, &r, &clicked, 403, "forbidden");
        assert!(clicked.text().contains(needle), "{tag}: {}", clicked.text());
    }
}

// ---------------------------------------------------------------------------
// A planted confirmation, and a signature copied from another request
// ---------------------------------------------------------------------------

/// **A confirmation planted with `kubectl` in another user's name is never
/// approved, and none of it is shown.** alice wants to approve her own
/// restore, so she replaces its stored request with one naming carol as the
/// requester: unsigned, signed by her own key, or carrying garbage where the
/// sidecar goes. The console verifies its OWN signature first; none of these
/// carries it.
///
/// KILLS: an approve route that does not verify the stored request's
/// signature; one that reads the requester before it has.
#[tokio::test]
async fn a_confirmation_planted_in_another_users_name_is_refused_and_not_shown() {
    let (log, _guard) = capture();
    let (app, _console) = pair_app();
    let r = requested(&app, "alice", "plant").await;
    let (genuine, _, genuine_sidecar) = stored_document(&app, &r.confirmation);
    let mut planted = genuine.clone();
    planted.requester = Requester {
        issuer: ISSUER.into(),
        subject: "carol".into(),
    };
    let attacker = SigningKey::generate_ed25519();
    let attacker_sidecar = serde_json::to_string(
        &logweir_evidence::sign::sign_detached(
            &attacker,
            PAYLOAD_TYPE_RESTORE_AUTHORIZATION,
            &planted.to_bytes(),
        )
        .expect("sig"),
    )
    .expect("json");
    let unsigned =
        format!(r#"{{"payloadType":"{PAYLOAD_TYPE_RESTORE_AUTHORIZATION}","signatures":[]}}"#);
    // A genuine CONSOLE signature over the planted bytes — by another
    // installation's console key, which this console's trust is not.
    let other_console = signed_by_console(&console("prod-pair"), &planted);
    // THIS console's genuine signature, lifted from the request it did
    // confirm: valid, over other bytes.
    let lifted = genuine_sidecar.clone();
    let says_carol = json!({"logweir.dev/requester": format!("{ISSUER}#carol")});
    let alice = signed_in(&app, "alice", &["ops", "approvers"]);
    let before = approvals_posted(&app.app.fake);
    for (label, sidecar) in [
        ("no signature", unsigned.as_str()),
        (
            "a signature by the attacker's own key",
            attacker_sidecar.as_str(),
        ),
        (
            "a signature by another console's key",
            other_console.as_str(),
        ),
        ("this console's signature over other bytes", lifted.as_str()),
        ("not a sidecar", "carol approved this"),
    ] {
        plant(&app, &r, &planted, sidecar, says_carol.clone());
        let seen = view(&app, &alice, &r.restore).await;
        assert_eq!(seen.status, 200, "{label}");
        let item = seen.json()["item"].clone();
        assert_eq!(item["state"], "notConfirmed", "{label}: {item}");
        assert_eq!(item["approve"]["offered"], false, "{label}");
        for field in [
            "requester",
            "planHash",
            "ticket",
            "expiresAt",
            "confirmationSha256",
        ] {
            assert!(
                item.get(field).is_none(),
                "{label}: {field} is shown: {item}"
            );
        }
        assert!(!seen.text().contains("carol"), "{label}: {}", seen.text());
        let clicked = click(&app, &alice, &r.restore, &stored_sha(&app, &r.confirmation)).await;
        assert_refused(&app, &r, &clicked, 409, "policy_mismatch");
        assert!(!clicked.text().contains("carol"), "{label}");
        let (record, _) = log.audit(&clicked);
        assert_eq!(
            record["failureCode"], "confirmation_not_verified",
            "{label}"
        );
    }
    assert_eq!(
        approvals_posted(&app.app.fake),
        before,
        "nothing was signed"
    );

    // NEGATIVE CONTROL: the request the console DID confirm, back in place,
    // is shown — and alice, its requester, is still not its approver.
    plant(&app, &r, &genuine, &genuine_sidecar, json!({}));
    let item = view(&app, &alice, &r.restore).await.json()["item"].clone();
    assert_eq!(item["state"], "pending", "{item}");
    assert_eq!(item["requester"], format!("{ISSUER}#alice"));
    let bob = signed_in(&app, "bob", &["approvers"]);
    let clicked = click(&app, &bob, &r.restore, &stored_sha(&app, &r.confirmation)).await;
    assert_eq!(clicked.status, 201, "{}", clicked.text());
}

/// **A valid console signature copied from ANOTHER request binds that other
/// request.** Each document below carries THIS console's genuine signature —
/// the strongest thing an attacker with `create approvals` can copy — and
/// each names another namespace, Restore name, UID, plan, policy or mode
/// than the click is about. alice plants it over her own request so that the
/// requester reads `carol`.
///
/// KILLS: an approve route that verifies the signature and then trusts the
/// document for the Restore in the URL.
#[tokio::test]
async fn a_valid_signature_copied_from_another_request_binds_that_request() {
    let (log, _guard) = capture();
    let (app, console) = pair_app();
    let r = requested(&app, "alice", "replay").await;
    let (genuine, _, _) = stored_document(&app, &r.confirmation);
    let strict = console_over(POLICIES, "prod-strict").policy();
    let alice = signed_in(&app, "alice", &["ops", "approvers"]);
    let carol = || Requester {
        issuer: ISSUER.into(),
        subject: "carol".into(),
    };
    let mut cases: Vec<(&str, RestoreAuthorization)> = Vec::new();
    let mut d = genuine.clone();
    d.requester = carol();
    d.subject.namespace = NS_B.into();
    cases.push(("another namespace", d));
    let mut d = genuine.clone();
    d.requester = carol();
    d.subject.name = "rst-someone-elses".into();
    cases.push(("another Restore", d));
    let mut d = genuine.clone();
    d.requester = carol();
    d.subject.uid = "another-uid".into();
    cases.push(("another UID", d));
    let mut d = genuine.clone();
    d.requester = carol();
    d.plan_hash = logweir_core::ids::sha256_prefixed(b"another plan");
    cases.push(("another plan", d));
    let mut d = genuine.clone();
    d.requester = carol();
    d.policy.name = strict.name.clone();
    d.policy.digest = strict.digest();
    cases.push(("another policy", d));
    let mut d = genuine.clone();
    d.requester = carol();
    d.policy.digest = format!("sha256:{}", "c".repeat(64));
    cases.push(("another policy digest", d));
    let before = approvals_posted(&app.app.fake);
    for (label, document) in cases {
        plant(
            &app,
            &r,
            &document,
            &signed_by_console(&console, &document),
            json!({}),
        );
        let item = view(&app, &alice, &r.restore).await.json()["item"].clone();
        assert_eq!(item["state"], "notConfirmed", "{label}: {item}");
        assert!(item.get("requester").is_none(), "{label}");
        let clicked = click(&app, &alice, &r.restore, &stored_sha(&app, &r.confirmation)).await;
        assert_refused(&app, &r, &clicked, 409, "policy_mismatch");
        let (record, _) = log.audit(&clicked);
        assert_eq!(record["failureCode"], "confirmation_not_bound", "{label}");
    }
    assert_eq!(
        approvals_posted(&app.app.fake),
        before,
        "nothing was signed"
    );

    // NEGATIVE CONTROL: the genuine request back in place, and a second
    // person: approved.
    plant(
        &app,
        &r,
        &genuine,
        &signed_by_console(&console, &genuine),
        json!({}),
    );
    let bob = signed_in(&app, "bob", &["approvers"]);
    let clicked = click(&app, &bob, &r.restore, &stored_sha(&app, &r.confirmation)).await;
    assert_eq!(clicked.status, 201, "{}", clicked.text());
}

/// **Nothing is read from the stored object but its two signed strings.** The
/// genuine request, re-stored with metadata that lies: an annotation naming
/// carol as the requester, a `spec.planHash` that is not the plan's, a
/// `spec.subjectRef` naming another Restore, a label. alice is still refused
/// as the requester (the SIGNED requester is alice), and bob's approval
/// carries the signed plan hash and subject, not the object's.
///
/// KILLS: the requester taken from the annotation; the plan hash or the
/// subject taken from `spec`.
#[tokio::test]
async fn the_stored_objects_metadata_and_unsigned_fields_decide_nothing() {
    let (app, _console) = pair_app();
    let r = requested(&app, "alice", "meta").await;
    let stored = app
        .app
        .fake
        .object("approvals", NS_A, &r.confirmation)
        .expect("the request");
    app.app.fake.seed(
        "approvals",
        NS_A,
        json!({
            "metadata": {
                "name": r.confirmation,
                "annotations": {"logweir.dev/requester": format!("{ISSUER}#carol"),
                                "logweir.dev/approver": format!("{ISSUER}#alice")},
                "labels": {"logweir.dev/requester": "carol"}
            },
            "spec": {
                "subjectRef": {"kind": "Restore", "name": "rst-someone-elses"},
                "planHash": format!("sha256:{}", "f".repeat(64)),
                "approvalBytes": stored["spec"]["approvalBytes"],
                "sidecarBytes": stored["spec"]["sidecarBytes"],
            }
        }),
    );
    let sha = stored_sha(&app, &r.confirmation);
    let alice = signed_in(&app, "alice", &["ops", "approvers"]);
    let seen = view(&app, &alice, &r.restore).await.json()["item"].clone();
    assert_eq!(seen["requester"], format!("{ISSUER}#alice"), "{seen}");
    assert_eq!(seen["approve"]["refusal"], "requester", "{seen}");
    let clicked = click(&app, &alice, &r.restore, &sha).await;
    assert_refused(&app, &r, &clicked, 403, "forbidden");

    let bob = signed_in(&app, "bob", &["approvers"]);
    let clicked = click(&app, &bob, &r.restore, &sha).await;
    assert_eq!(clicked.status, 201, "{}", clicked.text());
    let approval = app
        .app
        .fake
        .object("approvals", NS_A, &r.approval)
        .expect("the approval");
    let (approved, _, _) = stored_document(&app, &r.approval);
    assert_eq!(approval["spec"]["subjectRef"]["name"], r.restore.as_str());
    assert_eq!(approval["spec"]["planHash"], approved.plan_hash.as_str());
    assert_ne!(
        approval["spec"]["planHash"],
        format!("sha256:{}", "f".repeat(64))
    );
    assert_eq!(approved.requester.subject, "alice");
    assert_eq!(
        approval["metadata"]["annotations"]["logweir.dev/requester"],
        format!("{ISSUER}#alice"),
        "the new object's annotation is written from the signed bytes"
    );
}

/// **The body names the request and supplies nothing.** A body that tries to
/// supply the requester, the plan hash, the policy or the expiry is refused
/// as carrying unknown fields; a body with no digest, or a malformed one, is
/// refused; and a digest that is not the stored request's is a conflict —
/// the request changed since it was shown.
#[tokio::test]
async fn the_request_body_names_the_request_and_supplies_no_field() {
    let (log, _guard) = capture();
    let (app, console) = pair_app();
    let r = requested(&app, "alice", "body").await;
    let sha = stored_sha(&app, &r.confirmation);
    let bob = signed_in(&app, "bob", &["approvers"]);
    for (label, body) in [
        (
            "a requester",
            json!({"confirmationSha256": sha, "requester": {"issuer": ISSUER, "subject": "carol"}}),
        ),
        (
            "a plan hash",
            json!({"confirmationSha256": sha, "planHash": "sha256:00"}),
        ),
        (
            "an expiry",
            json!({"confirmationSha256": sha, "expiresAt": "2099-01-01T00:00:00Z"}),
        ),
        (
            "a policy",
            json!({"confirmationSha256": sha, "policy": {"name": "team-confirm"}}),
        ),
        (
            "an approver",
            json!({"confirmationSha256": sha, "approver": {"issuer": ISSUER, "subject": "erin"}}),
        ),
        (
            "a subject",
            json!({"confirmationSha256": sha, "subject": {"uid": "x"}}),
        ),
    ] {
        let clicked = click_with(&app, &bob, &r.restore, &body).await;
        assert!(
            clicked.status == 400 || clicked.status == 422,
            "{label}: {} {}",
            clicked.status,
            clicked.text()
        );
        assert!(
            clicked.text().contains("unknown"),
            "{label}: {}",
            clicked.text()
        );
        assert!(
            app.app
                .fake
                .object("approvals", NS_A, &r.approval)
                .is_none(),
            "{label}"
        );
    }
    for (label, body) in [
        ("no digest", json!({})),
        (
            "a digest that is not one",
            json!({"confirmationSha256": "latest"}),
        ),
        (
            "an upper-case digest",
            json!({"confirmationSha256": sha.to_uppercase()}),
        ),
    ] {
        let clicked = click_with(&app, &bob, &r.restore, &body).await;
        assert!(
            clicked.status == 400 || clicked.status == 422,
            "{label}: {} {}",
            clicked.status,
            clicked.text()
        );
        assert!(
            app.app
                .fake
                .object("approvals", NS_A, &r.approval)
                .is_none(),
            "{label}"
        );
    }
    // THE REQUEST CHANGED BETWEEN THE VIEW AND THE CLICK: bob was shown one
    // request; the stored one is now another the console also signed (the
    // requester submitted again with another ticket).
    let shown_sha = shown(&app, &bob, &r.restore).await;
    let (mut replaced, _, _) = stored_document(&app, &r.confirmation);
    replaced.ticket = Some("CHG-9999".into());
    plant(
        &app,
        &r,
        &replaced,
        &signed_by_console(&console, &replaced),
        json!({}),
    );
    let clicked = click(&app, &bob, &r.restore, &shown_sha).await;
    assert_refused(&app, &r, &clicked, 409, "state_conflict");
    let (record, _) = log.audit(&clicked);
    assert_eq!(record["failureCode"], "request_changed");
    // NEGATIVE CONTROL: shown again, the changed request is approved.
    let fresh = shown(&app, &bob, &r.restore).await;
    assert_ne!(fresh, shown_sha);
    assert_eq!(click(&app, &bob, &r.restore, &fresh).await.status, 201);
    assert_eq!(
        stored_document(&app, &r.approval).0.ticket.as_deref(),
        Some("CHG-9999")
    );
}

/// **A stored approval is never a request.** An approval the console signed,
/// put where a request goes — verbatim over the Restore it approved, and
/// re-addressed to a second Restore with a fresh console signature (the most
/// an attacker holding a signing oracle could copy) — is refused: a request
/// names no approver, so an approval is never approved again.
#[tokio::test]
async fn a_stored_approval_is_never_approved_again() {
    let (log, _guard) = capture();
    let (app, console) = pair_app();
    let r = requested(&app, "alice", "again").await;
    let bob = signed_in(&app, "bob", &["approvers"]);
    let sha = stored_sha(&app, &r.confirmation);
    assert_eq!(click(&app, &bob, &r.restore, &sha).await.status, 201);
    let (approved, _, sidecar) = stored_document(&app, &r.approval);
    let carol = signed_in(&app, "carol", &["approvers"]);

    // The approval's own bytes and sidecar, verbatim, in the request slot of
    // the Restore it approved.
    plant(&app, &r, &approved, &sidecar, json!({}));
    let item = view(&app, &carol, &r.restore).await.json()["item"].clone();
    assert_eq!(item["state"], "notConfirmed", "{item}");
    let clicked = click(&app, &carol, &r.restore, &stored_sha(&app, &r.confirmation)).await;
    clicked.assert_problem(409, "policy_mismatch");
    let (record, _) = log.audit(&clicked);
    assert_eq!(record["failureCode"], "confirmation_not_bound");

    // The same approval, re-addressed to a second Restore and signed by the
    // console key again.
    let second = requested(&app, "alice", "again-2").await;
    let (target, _, _) = stored_document(&app, &second.confirmation);
    let mut lifted = approved.clone();
    lifted.subject = target.subject.clone();
    plant(
        &app,
        &second,
        &lifted,
        &signed_by_console(&console, &lifted),
        json!({}),
    );
    let item = view(&app, &carol, &second.restore).await.json()["item"].clone();
    assert_eq!(item["state"], "notConfirmed", "{item}");
    let sha = stored_sha(&app, &second.confirmation);
    let clicked = click(&app, &carol, &second.restore, &sha).await;
    assert_refused(&app, &second, &clicked, 409, "policy_mismatch");
}

// ---------------------------------------------------------------------------
// Replay and races
// ---------------------------------------------------------------------------

/// **The same approver's second click is a replay; a second approver is a
/// conflict.** Create-only: the stored approval is bob's, at the instant of
/// his first click, whatever comes after.
#[tokio::test]
async fn a_second_click_is_a_replay_and_a_second_approver_a_conflict() {
    let (log, _guard) = capture();
    let (app, _console) = pair_app();
    let r = requested(&app, "alice", "twice").await;
    let sha = stored_sha(&app, &r.confirmation);
    let bob = signed_in(&app, "bob", &["approvers"]);
    let first = click(&app, &bob, &r.restore, &sha).await;
    assert_eq!(first.status, 201, "{}", first.text());
    let (stored, bytes, _) = stored_document(&app, &r.approval);
    app.app.clock.advance(30);
    let bob = signed_in(&app, "bob", &["approvers"]);
    let again = click(&app, &bob, &r.restore, &sha).await;
    assert_eq!(again.status, 200, "{}", again.text());
    assert_eq!(again.json()["replayed"], true);
    assert_eq!(stored_document(&app, &r.approval).1, bytes, "not re-signed");
    assert_eq!(
        stored_document(&app, &r.approval).0.approved_at,
        stored.approved_at
    );
    // carol, a third person and a genuine approver, a moment later.
    let carol = signed_in(&app, "carol", &["approvers"]);
    let late = click(&app, &carol, &r.restore, &sha).await;
    late.assert_problem(409, "state_conflict");
    let (record, _) = log.audit(&late);
    assert_eq!(record["failureCode"], "already_approved");
    let (still, still_bytes, _) = stored_document(&app, &r.approval);
    assert_eq!(still_bytes, bytes, "nothing was replaced");
    assert_eq!(still.approver.expect("approver").subject, "bob");
}

/// **An expired request authorises nothing**, and the last second before the
/// expiry still does.
#[tokio::test]
async fn an_expired_request_is_not_approved() {
    let (log, _guard) = capture();
    let (app, _console) = pair_app();
    let r = requested(&app, "alice", "expiry").await;
    let late = requested(&app, "alice", "expiry-late").await;
    let sha = stored_sha(&app, &r.confirmation);
    let late_sha = stored_sha(&app, &late.confirmation);
    // maxAgeSeconds is 3600: one second before the expiry.
    app.app.clock.advance(3599);
    let bob = signed_in(&app, "bob", &["approvers"]);
    assert_eq!(click(&app, &bob, &r.restore, &sha).await.status, 201);
    app.app.clock.advance(1);
    let bob = signed_in(&app, "bob", &["approvers"]);
    let item = view(&app, &bob, &late.restore).await.json()["item"].clone();
    assert_eq!(item["state"], "expired", "{item}");
    assert_eq!(item["approve"]["offered"], false);
    let clicked = click(&app, &bob, &late.restore, &late_sha).await;
    assert_refused(&app, &late, &clicked, 409, "state_conflict");
    let (record, notes) = log.audit(&clicked);
    assert_eq!(record["failureCode"], "request_expired");
    assert_eq!(notes["requester"], format!("{ISSUER}#alice"));
}

/// **A Restore deleted and recreated under the same name is another
/// Restore**: the request names the first one's UID.
#[tokio::test]
async fn a_restore_recreated_under_the_same_name_is_not_approved() {
    let (app, _console) = pair_app();
    let r = requested(&app, "alice", "uid").await;
    let sha = stored_sha(&app, &r.confirmation);
    let mut recreated = app
        .app
        .fake
        .object("restores", NS_A, &r.restore)
        .expect("the Restore");
    recreated["metadata"]["uid"] = json!("a-recreated-restore-uid");
    app.app.fake.seed("restores", NS_A, recreated);
    let bob = signed_in(&app, "bob", &["approvers"]);
    let item = view(&app, &bob, &r.restore).await.json()["item"].clone();
    assert_eq!(item["state"], "notConfirmed", "{item}");
    let clicked = click(&app, &bob, &r.restore, &sha).await;
    assert_refused(&app, &r, &clicked, 409, "policy_mismatch");
}

/// **A policy change after the request needs a new request.** The namespace's
/// two-person policy is edited (another digest), rebound to the strict
/// policy, or rebound to confirm: the click is refused in each.
#[tokio::test]
async fn a_request_made_under_another_policy_is_not_approved() {
    let (app, console) = pair_app();
    let r = requested(&app, "alice", "policy").await;
    let sha = stored_sha(&app, &r.confirmation);
    let rollout = |document: &str, binding: &str| {
        // The same console key, the same cluster, another policy document:
        // an installation-admin rollout.
        shared_over(
            app.app.fake.clone(),
            &console_with_key(&console.pkcs8, document, binding),
        )
    };
    for (label, document, binding, code) in [
        (
            "the policy's lifetime edited",
            POLICIES.replace(
                "mode: two-person\n    maxAgeSeconds: 3600",
                "mode: two-person\n    maxAgeSeconds: 7200",
            ),
            "prod-pair",
            "policy_mismatch",
        ),
        (
            "rebound to strict",
            POLICIES.to_string(),
            "prod-strict",
            "policy_mismatch",
        ),
        (
            "rebound to confirm",
            POLICIES.to_string(),
            "team-confirm",
            "policy_mismatch",
        ),
    ] {
        let after = rollout(&document, binding);
        let bob = signed_in(&after, "bob", &["approvers"]);
        let clicked = click(&after, &bob, &r.restore, &sha).await;
        clicked.assert_problem(409, code);
        assert!(
            after
                .app
                .fake
                .object("approvals", NS_A, &r.approval)
                .is_none(),
            "{label}"
        );
    }
    // NEGATIVE CONTROL: the unchanged policy.
    let same = rollout(POLICIES, "prod-pair");
    let bob = signed_in(&same, "bob", &["approvers"]);
    assert_eq!(click(&same, &bob, &r.restore, &sha).await.status, 201);
}

// ---------------------------------------------------------------------------
// Roles
// ---------------------------------------------------------------------------

/// **An Administrator who is not an Approver cannot approve**, and is told
/// so; a viewer and an operator cannot either. Bound as an Approver as well,
/// the administrator can (and is still held to "not the requester").
#[tokio::test]
async fn an_administrator_is_not_an_approver_unless_bound_as_one() {
    let (log, _guard) = capture();
    let (app, _console) = pair_app();
    let r = requested(&app, "alice", "roles").await;
    let sha = stored_sha(&app, &r.confirmation);
    for (subject, groups) in [
        ("dave", &["admins"][..]),
        ("olga", &["ops"][..]),
        ("vera", &["viewers"][..]),
        ("dora", &["admins", "ops", "viewers"][..]),
    ] {
        let who = signed_in(&app, subject, groups);
        let item = view(&app, &who, &r.restore).await.json()["item"].clone();
        assert_eq!(item["approve"]["offered"], false, "{subject}: {item}");
        assert_eq!(
            item["approve"]["refusal"], "notApprover",
            "{subject}: {item}"
        );
        let clicked = click(&app, &who, &r.restore, &sha).await;
        assert_refused(&app, &r, &clicked, 403, "forbidden");
        let (record, _) = log.audit(&clicked);
        assert_eq!(record["action"], "approval.submit", "{subject}");
        assert_eq!(record["failureCode"], "forbidden", "{subject}");
    }
    // NEGATIVE CONTROL: the same administrator, also bound as an approver.
    let erin = signed_in(&app, "erin", &["admins", "approvers"]);
    assert_eq!(click(&app, &erin, &r.restore, &sha).await.status, 201);
}

/// **Which is authoritative, and for how long.** The role BINDINGS are read
/// on every request; the session's GROUPS are those of the sign-in and stand
/// until the session expires (at most 900 s).
///
/// * A binding removed after sign-in refuses the next click (revocation is
///   immediate).
/// * A SUBJECT binding added after sign-in counts on the next click.
/// * A GROUP binding added for a group the session does not carry counts only
///   after a new sign-in.
/// * A session past its expiry approves nothing, whatever it carried.
#[tokio::test]
async fn the_binding_table_is_read_per_click_and_the_sessions_groups_are_the_sign_ins() {
    let (app, _console) = pair_app();
    let r = requested(&app, "alice", "revoke").await;
    let sha = stored_sha(&app, &r.confirmation);
    let bob = signed_in(&app, "bob", &["approvers", "viewers"]);
    // His session approves-by-role right now: the view offers the button.
    let offered = view(&app, &bob, &r.restore).await.json()["item"]["approve"].clone();
    assert_eq!(offered["offered"], true, "{offered}");
    // REVOKED: the approver binding is removed while bob's session lives. He
    // is still a viewer here, so the answer is 403 and not the 404 an
    // ungranted namespace gets.
    let mut without = bindings();
    without.bindings.retain(|b| b.role != Role::Approver);
    app.authorizer.replace(without.clone());
    let clicked = click(&app, &bob, &r.restore, &sha).await;
    assert_refused(&app, &r, &clicked, 403, "forbidden");
    let offered = view(&app, &bob, &r.restore).await.json()["item"]["approve"].clone();
    assert_eq!(offered["refusal"], "notApprover", "{offered}");
    // ... and a session whose ONLY binding was the approver one now has no
    // grant in the namespace at all.
    let only = signed_in(&app, "ben", &["approvers"]);
    click(&app, &only, &r.restore, &sha)
        .await
        .assert_problem(404, "not_found");

    // GRANTED BY SUBJECT after sign-in: frank signed in with no approver
    // group, and is then bound by `<issuer>#<subject>`.
    let frank = signed_in(&app, "frank", &["viewers"]);
    let clicked = click(&app, &frank, &r.restore, &sha).await;
    assert_refused(&app, &r, &clicked, 403, "forbidden");
    let mut by_subject = without.clone();
    by_subject.bindings.push(support::subject_binding(
        Role::Approver,
        NS_A,
        &[&format!("{ISSUER}#frank")],
    ));
    app.authorizer.replace(by_subject);

    // GRANTED BY A GROUP THE SESSION DOES NOT CARRY: gina's session says
    // `viewers`; the directory has since added her to `late-approvers`, and
    // that group is now bound. Her session still says what the sign-in said.
    let gina = signed_in(&app, "gina", &["viewers"]);
    let mut by_group = without.clone();
    by_group
        .bindings
        .push(support::binding(Role::Approver, NS_A, &["late-approvers"]));
    by_group.bindings.push(support::subject_binding(
        Role::Approver,
        NS_A,
        &[&format!("{ISSUER}#frank")],
    ));
    app.authorizer.replace(by_group);
    let clicked = click(&app, &gina, &r.restore, &sha).await;
    assert_refused(&app, &r, &clicked, 403, "forbidden");

    // A session past its expiry: 901 s after sign-in.
    let stale = signed_in(&app, "frank", &["viewers"]);
    app.app.clock.advance(901);
    let clicked = click(&app, &stale, &r.restore, &sha).await;
    clicked.assert_problem(401, "session_expired");
    assert!(app
        .app
        .fake
        .object("approvals", NS_A, &r.approval)
        .is_none());

    // frank, signed in again, approves on the subject binding; and gina's NEW
    // sign-in carries the group.
    let frank = signed_in(&app, "frank", &["viewers"]);
    assert_eq!(click(&app, &frank, &r.restore, &sha).await.status, 201);
    let second = requested(&app, "alice", "revoke-2").await;
    let gina = signed_in(&app, "gina", &["viewers", "late-approvers"]);
    let clicked = click(
        &app,
        &gina,
        &second.restore,
        &stored_sha(&app, &second.confirmation),
    )
    .await;
    assert_eq!(clicked.status, 201, "{}", clicked.text());
}

// ---------------------------------------------------------------------------
// Cross-site
// ---------------------------------------------------------------------------

/// One request with exactly the headers a row names, so a row can leave out
/// or falsify any of the three things the transport checks.
#[allow(clippy::too_many_arguments)]
async fn raw(
    app: &SharedApp,
    path: &str,
    cookie: &str,
    origin: Option<&str>,
    csrf: Option<String>,
    content_type: &str,
    method: &str,
    body: &str,
) -> TestResponse {
    let mut builder = Request::builder()
        .method(method)
        .uri(path)
        .header("host", SHARED_HOST)
        .header("content-type", content_type)
        .header("cookie", cookie);
    if let Some(origin) = origin {
        builder = builder.header("origin", origin);
    }
    if let Some(csrf) = csrf {
        builder = builder.header("x-csrf-token", csrf);
    }
    app.app
        .send(
            builder
                .body(if method == "GET" {
                    Body::empty()
                } else {
                    Body::from(body.to_string())
                })
                .expect("a request"),
        )
        .await
}

/// **A cross-origin POST with the approver's cookie, a missing or wrong CSRF
/// token, and a GET all approve nothing.** bob's session is live and he is a
/// genuine second person throughout: only the transport refuses.
#[tokio::test]
async fn a_cross_site_request_or_a_get_approves_nothing() {
    let (app, _console) = pair_app();
    let r = requested(&app, "alice", "xsite").await;
    let sha = stored_sha(&app, &r.confirmation);
    let bob = signed_in(&app, "bob", &["approvers"]);
    let path = format!(
        "/api/v1/namespaces/{NS_A}/restores/{}/console-approval",
        r.restore
    );
    let body = json!({"confirmationSha256": sha}).to_string();
    let send = |origin: Option<&'static str>,
                csrf: Option<String>,
                content_type: &'static str,
                method: &'static str| {
        raw(
            &app,
            &path,
            &bob.cookie,
            origin,
            csrf,
            content_type,
            method,
            &body,
        )
    };
    let json = "application/json";
    for (label, origin, csrf, content_type, status, code) in [
        (
            "another origin",
            Some("https://evil.test"),
            Some(bob.csrf.clone()),
            json,
            403,
            "origin_mismatch",
        ),
        (
            "no Origin",
            None,
            Some(bob.csrf.clone()),
            json,
            403,
            "origin_mismatch",
        ),
        (
            "the origin with a path",
            Some("https://console.test/"),
            Some(bob.csrf.clone()),
            json,
            403,
            "origin_mismatch",
        ),
        (
            "no CSRF token",
            Some(SHARED_ORIGIN),
            None,
            json,
            403,
            "forbidden",
        ),
        (
            "a wrong CSRF token",
            Some(SHARED_ORIGIN),
            Some("not-the-token".to_string()),
            json,
            403,
            "forbidden",
        ),
        (
            "a form post",
            Some(SHARED_ORIGIN),
            Some(bob.csrf.clone()),
            "application/x-www-form-urlencoded",
            415,
            "unsupported_media_type",
        ),
    ] {
        let response = send(origin, csrf, content_type, "POST").await;
        response.assert_problem(status, code);
        assert!(
            app.app
                .fake
                .object("approvals", NS_A, &r.approval)
                .is_none(),
            "{label}"
        );
    }
    // Another session's token is not this session's.
    let other = signed_in(&app, "carol", &["approvers"]);
    let response = send(Some(SHARED_ORIGIN), Some(other.csrf.clone()), json, "POST").await;
    response.assert_problem(403, "forbidden");
    // A GET on the click's route.
    let got = send(Some(SHARED_ORIGIN), Some(bob.csrf.clone()), json, "GET").await;
    assert_eq!(got.status, 405, "{}", got.text());
    // The read route signs and stores nothing, however often it is asked.
    let before = app.app.fake.requests().len();
    for _ in 0..3 {
        assert_eq!(view(&app, &bob, &r.restore).await.status, 200);
    }
    assert!(app
        .app
        .fake
        .requests()
        .into_iter()
        .skip(before)
        .all(|recorded| recorded.method == "GET"));
    assert!(app
        .app
        .fake
        .object("approvals", NS_A, &r.approval)
        .is_none());
    // NEGATIVE CONTROL: the same session, the same body, its own headers.
    let ok = send(Some(SHARED_ORIGIN), Some(bob.csrf.clone()), json, "POST").await;
    assert_eq!(ok.status, 201, "{}", ok.text());
}

// ---------------------------------------------------------------------------
// Modes
// ---------------------------------------------------------------------------

/// **The in-cluster administrator console serves no part of a two-person
/// approval** (PROD-16.1 review L3, owed to this row): it makes no request
/// there, shows that nobody can approve through it, and its click is refused
/// — its one identity cannot be two people.
#[tokio::test]
async fn the_local_admin_console_neither_requests_nor_approves_a_two_person_restore() {
    let console = console("prod-pair");
    let app = TestApp::with(
        FakeKube::new(),
        Options {
            approval: Arc::clone(&console.settings),
            ..Options::default()
        },
    );
    let policy = app
        .get(&format!("/api/v1/namespaces/{NS_A}/approval-policy"))
        .await
        .json()["item"]
        .clone();
    assert_eq!(policy["operatorMode"], "two-person");
    assert_eq!(policy["consoleApprovalAvailable"], false, "{policy}");
    let created = app
        .post(
            &format!("/api/v1/namespaces/{NS_A}/restores"),
            Some("p162-local-admin-01"),
            &restore_request("approval-la").to_string(),
        )
        .await;
    created.assert_problem(409, "policy_mismatch");
    assert!(
        created.text().contains("cannot be two people"),
        "{}",
        created.text()
    );
    assert_eq!(app.fake.count("restores", NS_A), 0, "nothing was created");
    assert_eq!(app.fake.count("approvals", NS_A), 0);

    // A Restore and a genuine request left by the SHARED console (the same
    // key): the administrator console still approves nothing.
    let shared = shared_over(app.fake.clone(), &console);
    let r = requested(&shared, "alice", "la-click").await;
    let item = app
        .get(&format!(
            "/api/v1/namespaces/{NS_A}/restores/{}/approval-request",
            r.restore
        ))
        .await
        .json()["item"]
        .clone();
    assert_eq!(item["state"], "pending", "{item}");
    assert_eq!(item["approve"]["offered"], false);
    assert_eq!(item["approve"]["refusal"], "localAdmin");
    let clicked = app
        .post(
            &format!(
                "/api/v1/namespaces/{NS_A}/restores/{}/console-approval",
                r.restore
            ),
            None,
            &json!({"confirmationSha256": stored_sha(&shared, &r.confirmation)}).to_string(),
        )
        .await;
    clicked.assert_problem(409, "policy_mismatch");
    assert!(
        clicked.text().contains("cannot be two people"),
        "{}",
        clicked.text()
    );
    assert!(app.fake.object("approvals", NS_A, &r.approval).is_none());
    // NEGATIVE CONTROL: the shared console, a second person.
    let bob = signed_in(&shared, "bob", &["approvers"]);
    let sha = stored_sha(&shared, &r.confirmation);
    assert_eq!(click(&shared, &bob, &r.restore, &sha).await.status, 201);
}

/// **A strict and a confirm namespace are what they were.** Neither has a
/// request to approve in the console (409 on both routes, nothing stored);
/// the strict namespace's own flow — a request, a personal-key countersign
/// — is `approval_policy.rs`'s, unchanged. And a TWO-PERSON namespace takes
/// no personal-key countersignature.
#[tokio::test]
async fn console_approval_exists_only_under_a_two_person_policy_and_takes_no_countersignature() {
    for binding in ["prod-strict", "team-confirm"] {
        let console = console(binding);
        let app = shared(&console);
        // A Restore in that namespace, by its own flow.
        let alice = signed_in(&app, "alice", &["ops"]);
        let mut body = restore_request("approval-other");
        if binding == "team-confirm" {
            body.as_object_mut().expect("object").remove("ticket");
        }
        let created = request_as(&app, &alice, binding, body).await;
        assert_eq!(created.status, 201, "{binding}: {}", created.text());
        let restore = created.json()["item"]["name"]
            .as_str()
            .expect("name")
            .to_string();
        let bob = signed_in(&app, "bob", &["approvers"]);
        view(&app, &bob, &restore)
            .await
            .assert_problem(409, "policy_mismatch");
        let before = approvals_posted(&app.app.fake);
        let clicked = click(&app, &bob, &restore, &format!("sha256:{}", "0".repeat(64))).await;
        clicked.assert_problem(409, "policy_mismatch");
        assert_eq!(approvals_posted(&app.app.fake), before, "{binding}");
        let policy = app
            .get(
                &format!("/api/v1/namespaces/{NS_A}/approval-policy"),
                &bob.cookie,
            )
            .await
            .json()["item"]
            .clone();
        assert_eq!(policy["consoleApprovalAvailable"], false, "{binding}");
        assert_eq!(
            policy["operatorMode"],
            if binding == "prod-strict" {
                "strict"
            } else {
                "confirm"
            }
        );
    }

    // A two-person namespace, offered a personal-key countersignature of its
    // own request: refused, nothing stored.
    let (log, _guard) = capture();
    let (app, _console) = pair_app();
    let r = requested(&app, "alice", "countersign").await;
    let (_, bytes, sidecar) = stored_document(&app, &r.confirmation);
    let personal = SigningKey::generate_p256();
    let mut merged: Sidecar = serde_json::from_str(&sidecar).expect("sidecar");
    merged.signatures.extend(
        logweir_evidence::sign::sign_detached(
            &personal,
            PAYLOAD_TYPE_RESTORE_AUTHORIZATION,
            bytes.as_bytes(),
        )
        .expect("sig")
        .signatures,
    );
    let bob = signed_in(&app, "bob", &["approvers"]);
    let submitted = app
        .post(
            &format!("/api/v1/namespaces/{NS_A}/restores/{}/approval", r.restore),
            &bob.cookie,
            Some(&bob.csrf),
            None,
            &json!({"sidecarBytes": serde_json::to_string(&merged).expect("json")}).to_string(),
        )
        .await;
    assert_refused(&app, &r, &submitted, 409, "policy_mismatch");
    assert!(
        submitted
            .text()
            .contains("personal-key countersignature is not accepted"),
        "{}",
        submitted.text()
    );
    let (record, _) = log.audit(&submitted);
    assert_eq!(record["failureCode"], "countersignature_not_accepted");
    let policy = app
        .get(
            &format!("/api/v1/namespaces/{NS_A}/approval-policy"),
            &bob.cookie,
        )
        .await
        .json()["item"]
        .clone();
    assert_eq!(policy["operatorMode"], "two-person");
    assert_eq!(policy["consoleApprovalAvailable"], true);
    assert_eq!(policy["ticketRequired"], true);
}

// ---------------------------------------------------------------------------
// The combination with PROD-15.1
// ---------------------------------------------------------------------------

/// **An original-name restore approved by a second person.** The request
/// carries the `originalName` subject (2.1.0) and NO typed names — typed
/// names are a one-person confirmation's, and are refused here by name; the
/// approver is shown the subject and the topic names before the click; and
/// the approval carries the subject and the approver, at 2.2.0.
#[tokio::test]
async fn an_original_name_restore_is_shown_with_its_topics_and_approved_as_exactly_that_subject() {
    let (app, console) = pair_app();
    let alice = signed_in(&app, "alice", &["ops"]);
    let mut body = restore_request("approval-original");
    // THE PLAN OF AN ORIGINAL-NAME RESTORE: the scope a second person is
    // shown comes from these bytes, and an original-name request over a plan
    // that writes under a prefix is not one (its scope is incomplete).
    let plan = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../ui/tests/fixtures/plan-original-name.golden.yaml"),
    )
    .expect("the original-name plan golden");
    body["planBytes"] = json!(plan);
    body["planHash"] = json!(logweir_core::ids::sha256_prefixed(plan.as_bytes()));
    body["target"]["topicNaming"] = json!({"prefix": "", "originalName": true});
    body["coverage"] = json!("complete");
    // Typed names: not accepted where a second person approves.
    let mut typed = body.clone();
    typed["originalNameConfirmation"] = json!({"typedTopics": ["orders", "payments"]});
    let refused = request_as(&app, &alice, "original-typed", typed).await;
    refused.assert_problem(422, "validation_failed");
    assert!(
        refused.text().contains("not_accepted"),
        "{}",
        refused.text()
    );
    assert_eq!(app.app.fake.count("restores", NS_A), 0);

    let created = request_as(&app, &alice, "original", body).await;
    assert_eq!(created.status, 201, "{}", created.text());
    let restore = created.json()["item"]["name"]
        .as_str()
        .expect("name")
        .to_string();
    let r = Requested {
        restore,
        approval: "approval-original".into(),
        confirmation: "approval-original-confirmation".into(),
    };
    let (request, bytes, _) = stored_document(&app, &r.confirmation);
    assert!(bytes.starts_with("{\"formatVersion\":\"2.1.0\","));
    assert_eq!(request.approval_subject.as_deref(), Some("originalName"));
    assert!(request.original_name_confirmation.is_none());

    let bob = signed_in(&app, "bob", &["approvers"]);
    let item = view(&app, &bob, &r.restore).await.json()["item"].clone();
    assert_eq!(item["state"], "pending", "{item}");
    assert_eq!(item["approvalSubject"], "originalName");
    assert_eq!(item["scopeComplete"], true, "{item}");
    assert_eq!(
        item["scope"]["topics"],
        json!([
            {"source": "orders", "target": "orders", "originalName": true},
            {"source": "payments", "target": "payments", "originalName": true}
        ]),
        "{item}"
    );
    assert_eq!(item["scope"]["topicsCount"], 2);
    assert_eq!(item["scope"]["target"]["topicPrefix"], "");
    let clicked = click(
        &app,
        &bob,
        &r.restore,
        item["confirmationSha256"].as_str().expect("sha"),
    )
    .await;
    assert_eq!(clicked.status, 201, "{}", clicked.text());
    let (approved, bytes, sidecar) = stored_document(&app, &r.approval);
    assert!(bytes.starts_with("{\"formatVersion\":\"2.2.0\","));
    assert_eq!(approved.approval_subject.as_deref(), Some("originalName"));
    assert!(approved.original_name_confirmation.is_none());
    assert_eq!(approved.approver.expect("approver").subject, "bob");
    assert!(verifies(&bytes, &sidecar, &console.public));
    assert_eq!(clicked.json()["item"]["approvalSubject"], "originalName");
}

/// **The policy snapshot the console signs under is the two-person one**, so
/// a request can never be mistaken for a strict namespace's: the same policy
/// with a personal key has another digest.
#[tokio::test]
async fn the_request_names_the_two_person_policys_own_digest() {
    let (app, console) = pair_app();
    let r = requested(&app, "alice", "digest").await;
    let (request, _, _) = stored_document(&app, &r.confirmation);
    let pair = console.policy();
    assert_eq!(pair.approver_signature, ApproverSignature::Console);
    assert_eq!(request.policy.digest, pair.digest());
    let mut personal = pair.clone();
    personal.approver_signature = ApproverSignature::PersonalKey;
    assert_ne!(request.policy.digest, personal.digest());
}

// ---------------------------------------------------------------------------
// The coordinator's addition 5: text somebody else chose
// ---------------------------------------------------------------------------

/// **A principal reaches the audit record unambiguously, and a refusal
/// sentence only bounded and cleaned.** An issuer and a subject come from an
/// identity provider's token: to the console they are text somebody else
/// chose.
///
/// 1. An email-shaped subject (visible ASCII, so comparable; and the shape
///    the record's own redaction treats as URL userinfo) is recorded as its
///    issuer, its subject and the SHA-256 of the whole `<issuer>#<subject>`,
///    for the requester and for the approver.
/// 2. Two requesters whose 255-character subjects differ only in their last
///    character have the SAME bounded subject note and DIFFERENT digests: the
///    record tells them apart.
/// 3. A session whose subject carries a line break, a control character and
///    markup is refused (it cannot be compared), and the sentence the caller
///    gets and the record's notes carry it only escaped and bounded.
///
/// KILLS: a principal recorded only as one bounded note (rows 1 and 2); a
/// refusal that copies a principal raw (row 3).
#[tokio::test]
async fn a_principal_reaches_the_audit_record_unambiguously_and_a_refusal_only_cleaned() {
    let (log, _guard) = capture();
    let (app, _console) = pair_app();
    let digest =
        |subject: &str| logweir_core::ids::sha256_hex(format!("{ISSUER}#{subject}").as_bytes());

    // ---- 1. an email-shaped pair ------------------------------------------
    let alice = "alice@example.com";
    let bob = "bob@example.com";
    let created = request_as(
        &app,
        &session_as(&app, "sid-email-alice", ISSUER, alice, &["ops"]),
        "email",
        restore_request("approval-email"),
    )
    .await;
    assert_eq!(created.status, 201, "{}", created.text());
    let restore = created.json()["item"]["name"]
        .as_str()
        .expect("a name")
        .to_string();
    let approver = session_as(&app, "sid-email-bob", ISSUER, bob, &["approvers"]);
    let sha = shown(&app, &approver, &restore).await;
    let clicked = click(&app, &approver, &restore, &sha).await;
    assert_eq!(clicked.status, 201, "{}", clicked.text());
    let (record, notes) = log.audit(&clicked);
    assert_eq!(record["decision"], "allow");
    assert_eq!(notes["requesterIssuer"], ISSUER);
    assert_eq!(notes["requesterSubject"], alice);
    assert_eq!(notes["requesterSha256"], digest(alice));
    assert_eq!(notes["approverIssuer"], ISSUER);
    assert_eq!(notes["approverSubject"], bob);
    assert_eq!(notes["approverSha256"], digest(bob));
    assert_ne!(notes["requesterSha256"], notes["approverSha256"]);

    // ---- 2. two long subjects that share their first 254 characters -------
    let long = |last: char| format!("{}{last}", "u".repeat(254));
    let mut seen = Vec::new();
    for (tag, last) in [("long-a", 'a'), ("long-b", 'b')] {
        let subject = long(last);
        assert_eq!(subject.len(), 255, "the longest subject the rule compares");
        let created = request_as(
            &app,
            &session_as(&app, &format!("sid-{tag}"), ISSUER, &subject, &["ops"]),
            tag,
            restore_request(&format!("approval-{tag}")),
        )
        .await;
        assert_eq!(created.status, 201, "{}", created.text());
        let restore = created.json()["item"]["name"]
            .as_str()
            .expect("a name")
            .to_string();
        let sha = shown(&app, &approver, &restore).await;
        let clicked = click(&app, &approver, &restore, &sha).await;
        assert_eq!(clicked.status, 201, "{}", clicked.text());
        let (_, notes) = log.audit(&clicked);
        let note = notes["requesterSubject"]
            .as_str()
            .expect("a note")
            .to_string();
        assert!(note.len() < 255, "the note is bounded: {}", note.len());
        assert_eq!(notes["requesterSha256"], digest(&subject));
        seen.push((
            note,
            notes["requesterSha256"].as_str().expect("hex").to_string(),
        ));
    }
    assert_eq!(seen[0].0, seen[1].0, "the bounded notes are the same text");
    assert_ne!(seen[0].1, seen[1].1, "and the digests tell the two apart");

    // ---- 3. a subject nobody can compare, on the approver's side ----------
    let created = request_as(
        &app,
        &session_as(&app, "sid-hostile-requester", ISSUER, "carol", &["ops"]),
        "hostile",
        restore_request("approval-hostile"),
    )
    .await;
    assert_eq!(created.status, 201, "{}", created.text());
    let restore = created.json()["item"]["name"]
        .as_str()
        .expect("a name")
        .to_string();
    let hostile = "dave\nfailure-reason=Approved\u{1b}[2K\u{202e}<script>alert(1)</script>";
    let dave = session_as(&app, "sid-hostile", ISSUER, hostile, &["approvers"]);
    let sha = stored_sha(&app, "approval-hostile-confirmation");
    let refused = click(&app, &dave, &restore, &sha).await;
    refused.assert_problem(403, "forbidden");
    let text = refused.text();
    // The body is JSON, so a raw line break could not be in it anyway; what
    // matters is what the SENTENCE holds once decoded.
    let detail = refused.json()["detail"]
        .as_str()
        .expect("a sentence")
        .to_string();
    assert!(
        detail.chars().all(|c| matches!(c, ' '..='~')),
        "only printable ASCII reaches the sentence: {detail:?}"
    );
    assert!(
        detail.contains("\\u{a}") && detail.contains("\\u{1b}") && detail.contains("\\u{202e}"),
        "{detail}"
    );
    assert!(!detail.contains("<script>alert(1)</script>\n"), "{detail}");
    assert!(detail.len() < 1_500, "{}: {text}", detail.len());
    let (record, notes) = log.audit(&refused);
    assert_eq!(record["decision"], "deny");
    assert_eq!(record["failureCode"], "self_approval_forbidden");
    assert_eq!(notes["separation"], "refused");
    assert_eq!(notes["approverSha256"], digest(hostile));
    assert!(app
        .app
        .fake
        .object("approvals", NS_A, "approval-hostile")
        .is_none());
    // And the same subject can make no REQUEST under this policy at all.
    let asked = request_as(
        &app,
        &session_as(&app, "sid-hostile-ops", ISSUER, hostile, &["ops"]),
        "hostile-request",
        restore_request("approval-hostile-request"),
    )
    .await;
    asked.assert_problem(409, "policy_mismatch");
    let detail = asked.json()["detail"]
        .as_str()
        .expect("a sentence")
        .to_string();
    assert!(
        detail.chars().all(|c| matches!(c, ' '..='~')),
        "only printable ASCII reaches the sentence: {detail:?}"
    );
}

// ---------------------------------------------------------------------------
// The coordinator's addition 4: the console decides from the table
// ---------------------------------------------------------------------------

/// **The console decides from `ApprovalPolicy::route`, and refuses the pair
/// that is no row.** Every route of this service that acts on a policy asks
/// `logweir_api::approval::route_of`, which is the core's table and nothing
/// else about the policy:
///
/// | the bound policy | the row | this console (shared) may request | may approve |
/// |---|---|---|---|
/// | `Ordinary` | the requester confirms | yes | no |
/// | `Governed` + `Console` | a second person in the console | yes | yes |
/// | `Governed`, personal key | a personal key | yes | no |
/// | `Ordinary` + `Console` | NOT A ROW | refused, `policy_mismatch` | refused |
///
/// The fourth pair cannot be loaded: an installation document that says so
/// (written with `kubectl`, in the operator's words or the internal ones) is
/// refused when the console reads its policy file, naming the file, so the
/// console does not start on it. Built in memory, it is the console's own
/// `policy_mismatch` (409) all the same.
///
/// KILLS: a `route_of` that maps the fourth pair to a row; a console that
/// loads it.
#[test]
fn the_console_decides_from_the_table_and_refuses_a_pair_that_is_no_row() {
    use logweir_api::approval::route_of;
    use logweir_api::problem::ProblemCode;
    use logweir_core::approval_policy::{
        ApprovalMode, ApprovalRoute, ApproverSignature, ConsoleKind,
    };
    let policy = |mode, approver_signature| ApprovalPolicy {
        name: "p".into(),
        mode,
        max_age_seconds: 900,
        require_distinct_principal: mode == ApprovalMode::Governed,
        approver_signature,
    };
    let row = |mode, signature| route_of(&policy(mode, signature)).map_err(|e| (e.code, e.detail));
    assert_eq!(
        row(ApprovalMode::Ordinary, ApproverSignature::PersonalKey),
        Ok(ApprovalRoute::RequesterConfirms)
    );
    assert_eq!(
        row(ApprovalMode::Governed, ApproverSignature::Console),
        Ok(ApprovalRoute::SecondPersonInConsole)
    );
    assert_eq!(
        row(ApprovalMode::Governed, ApproverSignature::PersonalKey),
        Ok(ApprovalRoute::PersonalKey)
    );
    let (code, detail) =
        row(ApprovalMode::Ordinary, ApproverSignature::Console).expect_err("not a row");
    assert_eq!(code, ProblemCode::PolicyMismatch);
    assert_eq!(code.status().as_u16(), 409);
    assert!(
        detail.contains("not a policy anything is confirmed, approved or run under")
            && detail.ends_with("Nothing was created or approved."),
        "{detail}"
    );
    // What each row lets each console do: the click exists under one row and
    // in one console.
    for route in [
        ApprovalRoute::RequesterConfirms,
        ApprovalRoute::SecondPersonInConsole,
        ApprovalRoute::PersonalKey,
    ] {
        for console in [ConsoleKind::Shared, ConsoleKind::LocalAdmin] {
            assert_eq!(
                route.console_may_approve(console),
                route == ApprovalRoute::SecondPersonInConsole && console == ConsoleKind::Shared,
                "{route:?} in {console:?}"
            );
            assert_eq!(
                route.console_may_request(console),
                !(route == ApprovalRoute::SecondPersonInConsole
                    && console == ConsoleKind::LocalAdmin),
                "{route:?} in {console:?}"
            );
        }
    }

    // An installation document that carries the fourth pair is refused where
    // the console reads it.
    // The crate's own convention for a scratch directory (no temp-file
    // dependency here): unique per process, removed at the end.
    let dir = std::env::temp_dir().join(format!("logweir-prod162-table-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("a directory");
    for (name, document) in [
        (
            "internal-words.yaml",
            "allowOrdinaryConfirmation: true\npolicies:\n  - name: p\n    mode: Ordinary\n    approverSignature: Console\nnamespaces:\n  team-a: p\n",
        ),
        (
            "operator-words.yaml",
            "allowOrdinaryConfirmation: true\npolicies:\n  - name: p\n    mode: confirm\n    approverSignature: Console\nnamespaces:\n  team-a: p\n",
        ),
    ] {
        let file = dir.join(name);
        std::fs::write(&file, document).expect("written");
        let refused = ApprovalSettings::load(Some(&file), None, false, &[NS_A.to_string()])
            .err()
            .unwrap_or_else(|| panic!("{name} is loaded"));
        assert!(
            refused.contains(name) && refused.contains("approver"),
            "{refused}"
        );
    }
    // NEGATIVE CONTROL: the two-person document, in either spelling, loads.
    for document in [
        "policies:\n  - name: p\n    mode: two-person\nnamespaces:\n  team-a: p\n",
        "policies:\n  - name: p\n    mode: Governed\n    approverSignature: Console\nnamespaces:\n  team-a: p\n",
    ] {
        let file = dir.join("two-person.yaml");
        std::fs::write(&file, document).expect("written");
        let key = dir.join("console.pem");
        std::fs::write(
            &key,
            SigningKey::generate_ed25519()
                .to_pkcs8_pem()
                .expect("a throwaway key"),
        )
        .expect("written");
        let loaded = ApprovalSettings::load(Some(&file), Some(&key), false, &[NS_A.to_string()])
            .unwrap_or_else(|e| panic!("the two-person document loads: {e}"));
        let bound = loaded
            .policies
            .resolve(NS_A)
            .bound()
            .cloned()
            .expect("bound");
        assert_eq!(route_of(&bound).ok(), Some(ApprovalRoute::SecondPersonInConsole));
    }
    std::fs::remove_dir_all(&dir).expect("removed");
}

// ---------------------------------------------------------------------------
// The coordinator's addition 6: what is approved is what was shown
// ---------------------------------------------------------------------------

/// A plan of the golden's shape over `topics` (the wizard's own grammar),
/// as a request body for the two-person namespace.
fn plan_over(topics: &[String]) -> String {
    let golden = support::golden_plan();
    let start = golden.find("  topics:\n").expect("the golden lists topics");
    let end = golden.find("target:\n").expect("then the target");
    let list: String = topics.iter().map(|t| format!("    - \"{t}\"\n")).collect();
    format!("{}  topics:\n{list}{}", &golden[..start], &golden[end..])
}

fn body_over(plan: &str, approval: &str) -> Value {
    let mut body = restore_request(approval);
    body["planBytes"] = json!(plan);
    body["planHash"] = json!(logweir_core::ids::sha256_prefixed(plan.as_bytes()));
    body
}

fn topic_names(n: usize, width: usize) -> Vec<String> {
    (0..n)
        .map(|i| {
            let stem = format!("t{i:05}-");
            format!("{stem}{}", "x".repeat(width.saturating_sub(stem.len())))
        })
        .collect()
}

/// **At the bound, every topic is in the scope; one over, nothing is
/// requested at all.** A two-person request is made only for a plan a second
/// person can be shown in full: the largest restore this mode accepts is
/// `MAX_SCOPE_TOPICS` topics, and one more is refused at the create, by name,
/// with nothing created. At the bound the view carries every topic — counted,
/// and checked by first, middle and last — and the click's audit record says
/// it was shown complete.
///
/// KILLS: a view that slices the list; a create that lets a request nobody
/// can be shown be made.
#[tokio::test]
async fn a_request_at_the_bound_shows_every_topic_and_one_over_is_never_made() {
    use logweir_core::approval_scope::MAX_SCOPE_TOPICS;
    let (log, _guard) = capture();
    let (app, _console) = pair_app();
    let alice = signed_in(&app, "alice", &["ops"]);

    // One over: refused before anything exists.
    let over = plan_over(&topic_names(MAX_SCOPE_TOPICS + 1, 8));
    let refused = request_as(&app, &alice, "over", body_over(&over, "approval-over")).await;
    refused.assert_problem(422, "validation_failed");
    assert!(
        refused.text().contains("scope_incomplete")
            && refused.text().contains("1025 topics")
            && refused.text().contains("Split it"),
        "{}",
        refused.text()
    );
    assert_eq!(
        app.app.fake.count("restores", NS_A),
        0,
        "nothing was created"
    );
    assert_eq!(approvals_posted(&app.app.fake), 0);

    // At the bound: made, shown whole, approved.
    let names = topic_names(MAX_SCOPE_TOPICS, 8);
    let at = plan_over(&names);
    assert!(at.len() < 256 * 1024, "inside the plan's own byte bound");
    let created = request_as(&app, &alice, "at", body_over(&at, "approval-at")).await;
    assert_eq!(created.status, 201, "{}", created.text());
    let restore = created.json()["item"]["name"]
        .as_str()
        .expect("name")
        .to_string();
    let bob = signed_in(&app, "bob", &["approvers"]);
    let seen = view(&app, &bob, &restore).await;
    assert_eq!(seen.status, 200, "{}", seen.text());
    let item = seen.json()["item"].clone();
    assert_eq!(item["scopeComplete"], true);
    assert_eq!(item["approve"]["offered"], true);
    let topics = item["scope"]["topics"].as_array().expect("topics");
    assert_eq!(topics.len(), MAX_SCOPE_TOPICS);
    assert_eq!(item["scope"]["topicsCount"], MAX_SCOPE_TOPICS);
    for index in [0, MAX_SCOPE_TOPICS / 2, MAX_SCOPE_TOPICS - 1] {
        assert_eq!(topics[index]["source"], names[index]);
        assert_eq!(
            topics[index]["target"],
            format!("restore-20260907T140500Z-{}", names[index])
        );
    }
    let sha = item["confirmationSha256"]
        .as_str()
        .expect("sha")
        .to_string();
    let clicked = click(&app, &bob, &restore, &sha).await;
    assert_eq!(clicked.status, 201, "{}", clicked.text());
    let (_, notes) = log.audit(&clicked);
    assert_eq!(notes["scope"], "complete");
    assert_eq!(notes["scopeTopicsShown"], MAX_SCOPE_TOPICS.to_string());
    assert_eq!(notes["scopeTopicsInPlan"], MAX_SCOPE_TOPICS.to_string());
}

/// **A request the console cannot show in full is offered to nobody, and
/// the click refuses it by name** — whoever calls it, with the right hash.
///
/// Two ways a stored request can name what cannot be shown, each made the
/// way it could be in the field:
///
/// 1. **the Restore's plan is not the one the request names** (a direct edit
///    of the stored object; the CRD makes `spec` immutable, the fake does
///    not). The scope comes from the request's signed hash and nothing else,
///    so the request no longer binds: it is `notConfirmed`, NO scope and no
///    field is shown, nothing is offered, and a direct POST with the view's
///    earlier hash is refused (`confirmation_not_bound`);
/// 2. **a request over a plan too large to show**, signed by this console's
///    key (an older console without the create-time bound, made here by
///    signing it directly): it binds, and its scope is incomplete. The view
///    says `tooManyTopics` and offers nothing; a direct POST with the view's
///    own hash is refused `scope_incomplete`, by the click's own check.
///
/// KILLS: an approve route that skips the completeness check (case 2 would be
/// approved); a scope built from the Restore object rather than the signed
/// hash (case 1 would show the edited plan).
#[tokio::test]
async fn a_request_that_cannot_be_shown_in_full_is_offered_to_nobody_and_refused_on_the_click() {
    use logweir_core::approval_scope::MAX_SCOPE_TOPICS;
    let (log, _guard) = capture();
    let (app, console) = pair_app();
    let bob = signed_in(&app, "bob", &["approvers"]);

    // ---- 1. the plan is not the request's ---------------------------------
    let r = requested(&app, "alice", "swapped").await;
    let before = view(&app, &bob, &r.restore).await.json()["item"].clone();
    assert_eq!(before["scopeComplete"], true, "THE CONTROL: as requested");
    assert_eq!(before["approve"]["offered"], true);
    let sha = before["confirmationSha256"]
        .as_str()
        .expect("sha")
        .to_string();
    let mut restore = app
        .app
        .fake
        .object("restores", NS_A, &r.restore)
        .expect("the Restore");
    restore["spec"]["planBytes"] = json!(plan_over(&["orders".into(), "payroll".into()]));
    app.app.fake.seed("restores", NS_A, restore);
    let item = view(&app, &bob, &r.restore).await.json()["item"].clone();
    assert_eq!(item["state"], "notConfirmed", "{item}");
    assert_eq!(item["scopeComplete"], false);
    assert!(item.get("scope").is_none() && item.get("requester").is_none());
    assert_eq!(item["approve"]["offered"], false);
    let clicked = click(&app, &bob, &r.restore, &sha).await;
    clicked.assert_problem(409, "policy_mismatch");
    assert_eq!(
        log.audit(&clicked).0["failureCode"],
        "confirmation_not_bound"
    );
    assert!(app
        .app
        .fake
        .object("approvals", NS_A, &r.approval)
        .is_none());

    // ---- 2. a request over a plan too large to show -----------------------
    let r = requested(&app, "alice", "large").await;
    let large = plan_over(&topic_names(MAX_SCOPE_TOPICS + 1, 8));
    let mut restore = app
        .app
        .fake
        .object("restores", NS_A, &r.restore)
        .expect("the Restore");
    restore["spec"]["planBytes"] = json!(large);
    app.app.fake.seed("restores", NS_A, restore);
    let (mut request, _, _) = stored_document(&app, &r.confirmation);
    request.plan_hash = logweir_core::ids::sha256_prefixed(large.as_bytes());
    plant(
        &app,
        &r,
        &request,
        &signed_by_console(&console, &request),
        json!({}),
    );
    let item = view(&app, &bob, &r.restore).await.json()["item"].clone();
    assert_eq!(item["state"], "pending", "{item}");
    assert_eq!(item["scopeComplete"], false);
    assert_eq!(item["scopeIncomplete"], "tooManyTopics");
    assert!(item.get("scope").is_none(), "no partial scope, ever");
    assert!(
        item["scopeSentence"]
            .as_str()
            .is_some_and(|s| s.contains("1025 topics") && s.contains("Split it")),
        "{item}"
    );
    assert_eq!(item["approve"]["offered"], false);
    assert_eq!(item["approve"]["refusal"], "scopeIncomplete");
    let sha = item["confirmationSha256"]
        .as_str()
        .expect("sha")
        .to_string();
    let clicked = click(&app, &bob, &r.restore, &sha).await;
    clicked.assert_problem(409, "state_conflict");
    let (record, notes) = log.audit(&clicked);
    assert_eq!(record["failureCode"], "scope_incomplete");
    assert_eq!(notes["scope"], "incomplete:tooManyTopics");
    assert!(app
        .app
        .fake
        .object("approvals", NS_A, &r.approval)
        .is_none());
}

/// **The click re-derives the scope itself.** A request whose Restore still
/// binds it — the same plan bytes, the same hash — but whose plan the scope
/// cannot show (a value a page cannot render faithfully: a bucket name with a
/// tab) is refused on the click with `scope_incomplete`, the view's verdict
/// being only advisory. The plan is accepted by the create in a namespace
/// that is NOT two-person, and the namespace is then rebound — the case of a
/// policy change, or an older console that made the request.
///
/// KILLS: an approve route that skips the completeness check.
#[tokio::test]
async fn the_click_refuses_a_request_whose_scope_is_incomplete_whatever_the_view_said() {
    let (log, _guard) = capture();
    let (app, console) = pair_app();
    // A request the console signs for a plan whose bucket carries a tab: made
    // here by signing it directly with the console's key, as an older console
    // without the create-time check would have.
    let plan =
        support::golden_plan().replace("bucket: \"kafka-backups\"", "bucket: \"kafka\\tbackups\"");
    assert!(
        plan.contains("kafka\\tbackups"),
        "the plan carries the escape"
    );
    let created = request_as(
        &app,
        &signed_in(&app, "alice", &["ops"]),
        "tab",
        body_over(&support::golden_plan(), "approval-tab"),
    )
    .await;
    assert_eq!(created.status, 201, "{}", created.text());
    let restore = created.json()["item"]["name"]
        .as_str()
        .expect("name")
        .to_string();
    // Swap the plan AND re-sign the request over it with the console's key,
    // so the binding holds and only the scope can refuse.
    let mut object = app
        .app
        .fake
        .object("restores", NS_A, &restore)
        .expect("Restore");
    object["spec"]["planBytes"] = json!(plan);
    app.app.fake.seed("restores", NS_A, object);
    let (mut request, _, _) = stored_document(&app, "approval-tab-confirmation");
    request.plan_hash = logweir_core::ids::sha256_prefixed(plan.as_bytes());
    let r = Requested {
        restore: restore.clone(),
        approval: "approval-tab".into(),
        confirmation: "approval-tab-confirmation".into(),
    };
    plant(
        &app,
        &r,
        &request,
        &signed_by_console(&console, &request),
        json!({}),
    );
    let bob = signed_in(&app, "bob", &["approvers"]);
    let item = view(&app, &bob, &restore).await.json()["item"].clone();
    assert_eq!(item["state"], "pending", "{item}");
    assert_eq!(item["scopeComplete"], false, "{item}");
    assert_eq!(item["scopeIncomplete"], "valueNotShowable");
    assert_eq!(item["approve"]["offered"], false);
    let sha = item["confirmationSha256"]
        .as_str()
        .expect("sha")
        .to_string();
    let clicked = click(&app, &bob, &restore, &sha).await;
    clicked.assert_problem(409, "state_conflict");
    assert!(
        clicked
            .text()
            .contains("A second person approves only what they are shown in full"),
        "{}",
        clicked.text()
    );
    assert!(
        !clicked.text().contains("kafka"),
        "the sentence repeats no value of the plan"
    );
    let (record, notes) = log.audit(&clicked);
    assert_eq!(record["failureCode"], "scope_incomplete");
    assert_eq!(notes["scope"], "incomplete:valueNotShowable");
    assert!(app
        .app
        .fake
        .object("approvals", NS_A, "approval-tab")
        .is_none());
    // NEGATIVE CONTROL: the same request over the plan it was first made for
    // is complete and approved.
    let mut object = app
        .app
        .fake
        .object("restores", NS_A, &restore)
        .expect("Restore");
    object["spec"]["planBytes"] = json!(support::golden_plan());
    app.app.fake.seed("restores", NS_A, object);
    let (_, original_bytes, original_sidecar) = {
        let mut original = request.clone();
        original.plan_hash = logweir_core::ids::sha256_prefixed(support::golden_plan().as_bytes());
        plant(
            &app,
            &r,
            &original,
            &signed_by_console(&console, &original),
            json!({}),
        );
        stored_document(&app, "approval-tab-confirmation")
    };
    assert!(verifies(
        &original_bytes,
        &original_sidecar,
        &console.public
    ));
    let sha = stored_sha(&app, "approval-tab-confirmation");
    let clicked = click(&app, &bob, &restore, &sha).await;
    assert_eq!(clicked.status, 201, "{}", clicked.text());
}
