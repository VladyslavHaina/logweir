//! Audit attribution on the durable objects the API creates (PLAT-17.2).
//!
//! D0 §"Audit attribution": created CRs carry the audit ID, the request and
//! idempotency hashes and a non-secret actor reference in reserved
//! annotations. PLAT-17.2 adds what an investigator needs next to them — how
//! the actor authenticated, which product action and binding revision
//! authorized the write, which Kubernetes principal the write was made as
//! (the name Kubernetes audit records for the same call), and, for a restore,
//! the recovery point it selected. The adapter stamps every create from the
//! request's own audit record, so no route passes any of it and a route added
//! later cannot forget it; `tests/support`'s fake API server additionally
//! records any create without the five base annotations as UNEXPECTED, which
//! turns every `assert_strict()` in this crate into an attribution guard.

mod support;

use std::io::Write;
use std::sync::{Arc, Mutex};

use logweir_api::audit::{self, AuditContext};
use logweir_api::authz::Role;
use serde_json::{json, Value};
use support::{FakeKube, SharedApp, SharedOptions, TestApp, ISSUER, KUBERNETES_PRINCIPAL, NS_A};

// ------------------------------------------------------------ log capture

#[derive(Clone, Default)]
struct Buffer(Arc<Mutex<Vec<u8>>>);

struct BufferWriter(Arc<Mutex<Vec<u8>>>);

impl Write for BufferWriter {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(data);
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
        String::from_utf8_lossy(&self.0.lock().unwrap()).into_owned()
    }

    fn record(&self, audit_id: &str) -> Value {
        self.text()
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .filter(|line| line["target"] == "logweir_api::audit")
            .filter_map(|line| {
                line["fields"]["audit"]
                    .as_str()
                    .and_then(|a| serde_json::from_str::<Value>(a).ok())
            })
            .find(|r| r["auditId"] == audit_id)
            .unwrap_or_else(|| panic!("no audit record for {audit_id}:\n{}", self.text()))
    }
}

/// Install a capturing subscriber for this thread.
fn capture() -> (Buffer, tracing::subscriber::DefaultGuard) {
    let buffer = Buffer::default();
    let subscriber = tracing_subscriber::fmt()
        .json()
        .with_writer(buffer.clone())
        .with_env_filter(audit::log_filter_from("debug"))
        .finish();
    let guard = tracing::subscriber::set_default(subscriber);
    (buffer, guard)
}

fn shared() -> SharedApp {
    SharedApp::new(
        FakeKube::new(),
        support::idp::MockIdp::new(ISSUER, &[]),
        SharedOptions {
            bindings: support::RoleBindings {
                revision: "attribution-rev-3".into(),
                bindings: vec![support::binding(Role::Operator, NS_A, &["lw-a-operators"])],
            },
            ..SharedOptions::default()
        },
    )
}

fn annotations(object: &Value) -> serde_json::Map<String, Value> {
    object["metadata"]["annotations"]
        .as_object()
        .cloned()
        .unwrap_or_default()
}

// ------------------------------------------------------------------ rows

/// **A restore created in shared mode names who asked, how they signed in,
/// under which binding revision and action, as which Kubernetes principal, and
/// WHICH RECOVERY POINT they selected — on the object and in the audit line,
/// correlated by the request ID.**
#[tokio::test]
async fn a_shared_mode_restore_carries_actor_recovery_point_and_execution_identity() {
    let (log, _guard) = capture();
    let app = shared();
    let cookie = app.session_cookie("u-op", &["lw-a-operators"]);
    let plan = support::golden_plan();
    // The route itself refuses an archive URL with userinfo or a query, so
    // the recovery point's own redaction is held by the unit row below.
    let body = support::restore_body(&plan);
    let created = app
        .post(
            &format!("/api/v1/namespaces/{NS_A}/restores"),
            &cookie,
            Some(&app.csrf_for("u-op")),
            Some("attribution-restore-01"),
            &body.to_string(),
        )
        .await;
    assert_eq!(created.status, 201, "{}", created.text());
    let request_id = created.header("x-request-id").unwrap();
    let name = created.json()["item"]["name"].as_str().unwrap().to_string();

    let stored = app.app.fake.object("restores", NS_A, &name).unwrap();
    let a = annotations(&stored);
    assert_eq!(a["api.logweir.dev/actor"], format!("{ISSUER}#u-op"));
    assert_eq!(a["api.logweir.dev/request-id"], request_id.as_str());
    assert_eq!(a["api.logweir.dev/authentication-mode"], "oidc");
    assert_eq!(a["api.logweir.dev/action"], "restore.create");
    assert_eq!(a["api.logweir.dev/binding-revision"], "attribution-rev-3");
    assert_eq!(
        a["api.logweir.dev/kubernetes-principal"],
        KUBERNETES_PRINCIPAL
    );
    assert_eq!(
        a["api.logweir.dev/recovery-point"],
        "backupSet=01JB7Z0000000000000000000B pointInTime=2026-09-07T14:05:00Z \
         source=s3://kafka-backups/drill-demo"
    );
    // The D0 correlation annotations are still there beside them.
    assert!(a.contains_key("api.logweir.dev/request-sha256"));
    assert!(a.contains_key("api.logweir.dev/idempotency-scope-sha256"));

    let record = log.record(&request_id);
    assert_eq!(record["kubernetesPrincipal"], KUBERNETES_PRINCIPAL);
    assert_eq!(
        record["recoveryPoint"],
        a["api.logweir.dev/recovery-point"].as_str().unwrap()
    );
    assert_eq!(record["objectName"], name.as_str());

    // NO CREDENTIAL, ANYWHERE: the request named a credential Secret by
    // reference, and neither that reference's contents nor the plan bytes
    // reach the annotations.
    let stored_annotations = Value::Object(a).to_string();
    assert!(!stored_annotations.contains(plan.lines().next().unwrap()));
    app.app.fake.assert_strict();
}

/// **A write-only credential Secret is attributed like every other object, and
/// no annotation carries its value.**
#[tokio::test]
async fn a_credential_secret_is_attributed_and_its_annotations_hold_no_value() {
    // EVERY TEST IN THIS BINARY CAPTURES: `tracing`'s per-callsite interest
    // cache is shared across the test threads, and a thread with no
    // subscriber can leave another test's capture empty.
    let (_log, _guard) = capture();
    let app = shared();
    let cookie = app.session_cookie("u-op", &["lw-a-operators"]);
    let created = app
        .post(
            &format!("/api/v1/namespaces/{NS_A}/destinations"),
            &cookie,
            Some(&app.csrf_for("u-op")),
            Some("attribution-secret-01"),
            &support::destination_body_with_new_credential("primary").to_string(),
        )
        .await;
    assert_eq!(created.status, 201, "{}", created.text());
    let secret = app
        .app
        .fake
        .object("secrets", NS_A, "lwd-primary-archive-read")
        .unwrap();
    let a = annotations(&secret);
    assert_eq!(a["api.logweir.dev/actor"], format!("{ISSUER}#u-op"));
    assert_eq!(a["api.logweir.dev/action"], "destination.manage");
    assert_eq!(
        a["api.logweir.dev/kubernetes-principal"],
        KUBERNETES_PRINCIPAL
    );
    assert!(!a.contains_key("api.logweir.dev/recovery-point"));
    let text = Value::Object(a).to_string();
    for value in [support::SECRET_ACCESS_KEY, support::ACCESS_KEY_ID] {
        assert!(!text.contains(value), "a credential value in {text}");
    }
    // The destination itself is attributed too.
    let destination = app
        .app
        .fake
        .object("backupdestinations", NS_A, "primary")
        .unwrap();
    assert_eq!(
        annotations(&destination)["api.logweir.dev/action"],
        "destination.manage"
    );
    app.app.fake.assert_strict();
}

/// **localAdmin mode attributes the local administrator and says so.** No
/// binding revision (the mode has no bindings), and no recovery point on an
/// object that restores nothing.
#[tokio::test]
async fn a_local_admin_create_is_attributed_to_the_local_administrator() {
    // EVERY TEST IN THIS BINARY CAPTURES: `tracing`'s per-callsite interest
    // cache is shared across the test threads, and a thread with no
    // subscriber can leave another test's capture empty.
    let (_log, _guard) = capture();
    let app = TestApp::new();
    let created = app
        .post(
            &format!("/api/v1/namespaces/{NS_A}/schedules"),
            Some("attribution-sched-01"),
            &support::schedule_body().to_string(),
        )
        .await;
    assert_eq!(created.status, 201, "{}", created.text());
    let name = created.json()["item"]["name"].as_str().unwrap().to_string();
    let a = annotations(&app.fake.object("backupschedules", NS_A, &name).unwrap());
    assert_eq!(a["api.logweir.dev/actor"], "urn:logweir:local-admin#admin");
    assert_eq!(a["api.logweir.dev/authentication-mode"], "localAdmin");
    assert_eq!(a["api.logweir.dev/action"], "schedule.create");
    assert_eq!(
        a["api.logweir.dev/kubernetes-principal"],
        KUBERNETES_PRINCIPAL
    );
    assert!(!a.contains_key("api.logweir.dev/binding-revision"));
    assert!(!a.contains_key("api.logweir.dev/recovery-point"));
    app.fake.assert_strict();
}

/// **A create with no request around it is refused before anything is sent.**
/// NEGATIVE CONTROL for the adapter's stamp: without it this would reach the
/// fake and create an unattributed object.
#[tokio::test]
async fn a_create_outside_any_request_is_refused_and_never_sent() {
    // EVERY TEST IN THIS BINARY CAPTURES: `tracing`'s per-callsite interest
    // cache is shared across the test threads, and a thread with no
    // subscriber can leave another test's capture empty.
    let (_log, _guard) = capture();
    let fake = FakeKube::new();
    let adapter = logweir_api::kube::KubeAdapter::new(fake.client());
    let refused = adapter.create(NS_A, &cluster()).await;
    assert!(
        matches!(refused, Err(logweir_api::kube::KubeFailure::Unattributed)),
        "{refused:?}"
    );
    assert!(fake.requests().is_empty(), "{:?}", fake.requests());
}

/// **Inside a request, a create before authentication is refused too.** The
/// same adapter call, inside an audit scope whose record has no actor — and
/// then, as the POSITIVE CONTROL, with an actor and a decided action, when it
/// goes out carrying the stamp.
#[tokio::test]
async fn a_create_before_the_actor_is_known_is_refused_and_never_sent() {
    // EVERY TEST IN THIS BINARY CAPTURES: `tracing`'s per-callsite interest
    // cache is shared across the test threads, and a thread with no
    // subscriber can leave another test's capture empty.
    let (_log, _guard) = capture();
    let fake = FakeKube::new();
    let adapter = logweir_api::kube::KubeAdapter::new(fake.client());
    let context = Arc::new(AuditContext::new("audit-x", "POST", "/api/v1/x"));
    let refused = audit::scope(Arc::clone(&context), adapter.create(NS_A, &cluster())).await;
    assert!(matches!(
        refused,
        Err(logweir_api::kube::KubeFailure::Unattributed)
    ));
    // A decided action without an actor is refused — each half is required
    // on its own, not only together.
    let no_actor = Arc::new(AuditContext::new("audit-y", "POST", "/api/v1/x"));
    no_actor.set_decision(NS_A, "connection.create", &[], "r", audit::Decision::Allow);
    let refused = audit::scope(no_actor, adapter.create(NS_A, &cluster())).await;
    assert!(matches!(
        refused,
        Err(logweir_api::kube::KubeFailure::Unattributed)
    ));
    // An actor without a decided action is still refused.
    context.set_actor("oidc", "https://idp#u-1", "U", None);
    let refused = audit::scope(Arc::clone(&context), adapter.create(NS_A, &cluster())).await;
    assert!(matches!(
        refused,
        Err(logweir_api::kube::KubeFailure::Unattributed)
    ));
    assert!(fake.requests().is_empty());

    context.set_decision(NS_A, "connection.create", &[], "r", audit::Decision::Allow);
    context.set_kubernetes_principal("system:serviceaccount:x:y");
    let created = audit::scope(context, adapter.create(NS_A, &cluster()))
        .await
        .expect("an attributed create is sent");
    let a = created.metadata.annotations.unwrap();
    assert_eq!(a["api.logweir.dev/actor"], "https://idp#u-1");
    assert_eq!(a["api.logweir.dev/action"], "connection.create");
    assert_eq!(
        a["api.logweir.dev/kubernetes-principal"],
        "system:serviceaccount:x:y"
    );
    assert_eq!(fake.count("kafkaclusters", NS_A), 1);
}

fn cluster() -> weirkeeper::crds::kafka_cluster::KafkaCluster {
    let mut object = support::fixture("cluster-scram.json");
    object["metadata"] = json!({"name": "unattributed", "namespace": NS_A});
    object.as_object_mut().unwrap().remove("status");
    serde_json::from_value(object).expect("the fixture is a KafkaCluster")
}

// ------------------------------------------------------ the recovery point

#[test]
fn the_recovery_point_is_read_from_the_request_and_never_carries_a_credential() {
    // EVERY TEST IN THIS BINARY CAPTURES: `tracing`'s per-callsite interest
    // cache is shared across the test threads, and a thread with no
    // subscriber can leave another test's capture empty.
    let (_log, _guard) = capture();
    let with_destination = json!({
        "backupSetRef": "01JB",
        "pointInTime": "2026-09-07T14:05:00Z",
        "sourceDestinationRef": {"name": "primary"},
        "sourceArchive": {"url": "logweir-destination://primary"}
    });
    assert_eq!(
        audit::recovery_point_of(&with_destination).unwrap(),
        "backupSet=01JB pointInTime=2026-09-07T14:05:00Z source=destination/primary"
    );
    let legacy = json!({
        "backupSetRef": "01JB",
        "sourceArchive": {"url": "s3://u:p@bucket/x?X-Amz-Signature=deadbeef#f"}
    });
    let legacy = audit::recovery_point_of(&legacy).unwrap();
    assert_eq!(legacy, "backupSet=01JB source=s3://redacted@bucket/x");
    let catalog = json!({"recoveryPointRef": {"name": "rp-7"}, "pointInTime": "t"});
    assert_eq!(
        audit::recovery_point_of(&catalog).unwrap(),
        "recoveryPoint=rp-7 pointInTime=t"
    );
    // A request that selects no recovery point records none.
    assert!(audit::recovery_point_of(&support::schedule_body()).is_none());
    assert!(audit::recovery_point_of(&support::connection_body()).is_none());
}
