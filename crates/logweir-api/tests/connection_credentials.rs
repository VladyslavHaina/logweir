//! PROD-01.3 security follow-up: a connection's credential is ENTERED once and
//! becomes a Secret this service creates, bound to the connection; an existing
//! Secret is never named.
//!
//! THE THREAT these rows close. Before the follow-up, `auth.credentialRef`
//! named "an existing Secret", so a console operator could name another
//! team's credential Secret (one they cannot read), point the connection's
//! bootstrap at a host they control, and have Logweir's probe and runner
//! present that credential there. Each refusal below has its NEGATIVE CONTROL:
//! the same request shape that is accepted, so a refusal that refused
//! everything would fail the control.

mod support;

use std::sync::Arc;

use base64::Engine as _;
use logweir_api::auth::Actor;
use logweir_api::authz::{Action, Authorizer};
use serde_json::{json, Value};
use support::{Options, TestApp, NS_A};

/// A password value no fixture, golden or doc in the tree spells: every
/// response, stored object and request body but the Secret's own is scanned for
/// it (and for its base64).
const SEEDED_PASSWORD: &str = "seeded-Pw-9c1f7e2a-never-echoed";

fn connections() -> String {
    format!("/api/v1/namespaces/{NS_A}/connections")
}

fn posts(app: &TestApp, plural: &str) -> Vec<(String, Value)> {
    app.fake
        .requests()
        .into_iter()
        .filter(|r| r.method == "POST" && r.path.ends_with(plural))
        .map(|r| (r.query.clone(), serde_json::from_str(&r.body).unwrap()))
        .collect()
}

fn sasl_body(mode: &str, tls: bool) -> Value {
    json!({
        "role": "source",
        "bootstrapServers": ["kafka.example:9093"],
        "auth": {
            "mode": mode,
            "username": "logweir",
            "credential": {"password": SEEDED_PASSWORD},
            "tls": tls
        }
    })
}

/// Test PEM shapes, assembled at run time so no source line carries a
/// contiguous PEM block (WORKER-RULES, credential-shaped fixtures). Shape
/// checks only: nothing here is parsed as X.509.
fn pem(label: &str, body: &str) -> String {
    format!("-----BEGIN {label}-----\n{body}\n-----END {label}-----\n")
}

fn mtls_body(tls: bool) -> Value {
    json!({
        "role": "source",
        "bootstrapServers": ["kafka.example:9094"],
        "auth": {
            "mode": "mtls",
            "credential": {
                "certificatePem": pem("CERTIFICATE", "TUlJQ2Zha2VDZXJ0aWZpY2F0ZQ=="),
                "privateKeyPem": pem(concat!("PRIVATE", " KEY"), SEEDED_PASSWORD),
            },
            "tls": tls,
            "tlsCa": {"configMapKeyRef": {"name": "kafka-ca", "key": "ca.crt"}}
        }
    })
}

/// **Naming an existing Secret is refused**, before anything is written, with
/// the named code — and the control: the same connection with the password
/// ENTERED is created.
#[tokio::test]
async fn naming_an_existing_secret_is_refused_and_entering_the_value_is_accepted() {
    let app = TestApp::new();
    let mut foreign = sasl_body("scramSha512", true);
    foreign["auth"]
        .as_object_mut()
        .unwrap()
        .remove("credential");
    foreign["auth"]["credentialRef"] = json!({"name": "another-teams-sasl"});
    let refused = app
        .post(
            &connections(),
            Some("conn-foreign-01"),
            &foreign.to_string(),
        )
        .await;
    refused.assert_problem(422, "validation_failed");
    let text = String::from_utf8_lossy(&refused.body).to_string();
    assert!(text.contains("existing_credential_refused"), "{text}");
    assert!(
        app.fake.requests().iter().all(|r| r.method != "POST"),
        "nothing may be written for a refused request"
    );

    // Even beside an entered value: naming a Secret is never accepted.
    let mut both = sasl_body("scramSha512", true);
    both["auth"]["credentialRef"] = json!({"name": "another-teams-sasl"});
    app.post(&connections(), Some("conn-foreign-02"), &both.to_string())
        .await
        .assert_problem(422, "validation_failed");

    // CONTROL: the value entered, no name — accepted.
    let created = app
        .post(
            &connections(),
            Some("conn-entered-01"),
            &sasl_body("scramSha512", true).to_string(),
        )
        .await;
    assert_eq!(
        created.status,
        201,
        "{}",
        String::from_utf8_lossy(&created.body)
    );
    app.fake.assert_strict();
}

/// **The created Secret is the connection's own**: the deterministic name the
/// `KafkaCluster` names, the write-only contract's type and labels, an owner
/// reference to the connection's UID, and the connection's binding — the same
/// value the controller projects as expected
/// (`weirkeeper::connection::ResolvedConnection::credential_binding`). And the
/// password reaches NOTHING but that Secret's data: not the response, not the
/// stored `KafkaCluster`, not a list.
#[tokio::test]
async fn the_entered_password_becomes_a_bound_owned_secret_and_is_never_echoed() {
    let app = TestApp::new();
    let created = app
        .post(
            &connections(),
            Some("conn-bound-01"),
            &sasl_body("scramSha256", false).to_string(),
        )
        .await;
    assert_eq!(
        created.status,
        201,
        "{}",
        String::from_utf8_lossy(&created.body)
    );
    let item = created.json()["item"].clone();
    let name = item["name"].as_str().unwrap().to_string();
    let uid = item["uid"].as_str().unwrap().to_string();
    let secret_name = format!("{name}-credential");
    assert_eq!(item["auth"]["mode"], "scramSha256");
    assert_eq!(item["auth"]["credentialRef"]["name"], secret_name);

    let clusters = posts(&app, "/kafkaclusters");
    let (_, cluster) = clusters.last().unwrap();
    assert_eq!(cluster["spec"]["auth"]["secretRef"]["name"], secret_name);

    // The Secret: a dry-run probe first, then the real create.
    let secrets = posts(&app, "/secrets");
    assert_eq!(secrets.len(), 2, "{secrets:?}");
    assert!(secrets[0].0.contains("dryRun=All"), "{}", secrets[0].0);
    assert!(
        secrets[0].1["data"]
            .as_object()
            .is_none_or(|d| d.is_empty()),
        "the probe carries no value"
    );
    let (query, secret) = &secrets[1];
    assert!(!query.contains("dryRun"), "{query}");
    assert_eq!(secret["metadata"]["name"], secret_name);
    assert_eq!(secret["type"], "logweir.dev/kafka-sasl-password");
    assert_eq!(
        secret["metadata"]["labels"]["app.kubernetes.io/managed-by"],
        "logweir"
    );
    assert_eq!(secret["metadata"]["labels"]["logweir.dev/connection"], name);
    let owner = &secret["metadata"]["ownerReferences"][0];
    assert_eq!(owner["kind"], "KafkaCluster");
    assert_eq!(owner["uid"], uid);
    let data = secret["data"].as_object().unwrap();
    let decode = |k: &str| {
        String::from_utf8(
            base64::engine::general_purpose::STANDARD
                .decode(data[k].as_str().unwrap())
                .unwrap(),
        )
        .unwrap()
    };
    assert_eq!(decode("password"), SEEDED_PASSWORD);
    // The binding is the controller's expected value for THIS object.
    let stored: weirkeeper::crds::kafka_cluster::KafkaCluster = serde_json::from_value({
        let mut c = cluster.clone();
        c["metadata"]["uid"] = json!(uid);
        c
    })
    .unwrap();
    let expected =
        weirkeeper::connection::resolve(&stored, weirkeeper::connection::ConnectionUse::Probe)
            .unwrap()
            .credential_binding()
            .unwrap();
    assert_eq!(decode("logweir-binding"), expected);

    // SEEDED-SECRET SCAN: the value, and its base64, appear in that one Secret
    // body and NOWHERE else this service wrote or answered.
    let b64 = base64::engine::general_purpose::STANDARD.encode(SEEDED_PASSWORD);
    let list = app.get(&connections()).await;
    let one = app.get(&format!("{}/{name}", connections())).await;
    for (label, text) in [
        (
            "create response",
            String::from_utf8_lossy(&created.body).to_string(),
        ),
        (
            "list response",
            String::from_utf8_lossy(&list.body).to_string(),
        ),
        (
            "get response",
            String::from_utf8_lossy(&one.body).to_string(),
        ),
        ("KafkaCluster body", cluster.to_string()),
        ("dry-run probe", secrets[0].1.to_string()),
        ("Secret metadata", secret["metadata"].to_string()),
    ] {
        assert!(
            !text.contains(SEEDED_PASSWORD),
            "{label} carries the password: {text}"
        );
        assert!(
            !text.contains(&b64),
            "{label} carries the password's base64: {text}"
        );
    }
    app.fake.assert_strict();
}

/// **mTLS**: the certificate and key become a `kafka-client-certificate`
/// Secret at `tls.crt`/`tls.key`, bound and owned; the connection names it as
/// `clientCertificate`; the CA is a ConfigMap reference. The key is never
/// echoed.
#[tokio::test]
async fn an_mtls_certificate_and_key_become_a_bound_secret() {
    let app = TestApp::new();
    let created = app
        .post(
            &connections(),
            Some("conn-mtls-01"),
            &mtls_body(true).to_string(),
        )
        .await;
    assert_eq!(
        created.status,
        201,
        "{}",
        String::from_utf8_lossy(&created.body)
    );
    let item = created.json()["item"].clone();
    let name = item["name"].as_str().unwrap().to_string();
    assert_eq!(item["auth"]["mode"], "mtls");
    assert_eq!(
        item["auth"]["clientCertificateRef"]["name"],
        format!("{name}-credential")
    );
    assert_eq!(item["auth"]["tlsCa"]["kind"], "configMap");
    let (_, cluster) = posts(&app, "/kafkaclusters").pop().unwrap();
    assert_eq!(
        cluster["spec"]["auth"]["clientCertificate"]["name"],
        format!("{name}-credential")
    );
    assert!(cluster["spec"]["auth"]["secretRef"].is_null());
    let (_, secret) = posts(&app, "/secrets").pop().unwrap();
    assert_eq!(secret["type"], "logweir.dev/kafka-client-certificate");
    let keys: Vec<&str> = secret["data"]
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(keys, vec!["logweir-binding", "tls.crt", "tls.key"]);
    let text = String::from_utf8_lossy(&created.body).to_string();
    assert!(!text.contains(SEEDED_PASSWORD), "{text}");
    app.fake.assert_strict();
}

/// The transport rules, each with its accepted twin: PLAIN without TLS is
/// `PlainWithoutTls`; mTLS without TLS is refused; an encrypted key and a
/// non-PEM certificate are refused by SHAPE without echoing either; a CA from a
/// Secret is not in this request's grammar at all.
#[tokio::test]
async fn the_transport_and_shape_rules_refuse_with_named_codes() {
    let app = TestApp::new();
    let plain = app
        .post(
            &connections(),
            Some("conn-plain-01"),
            &sasl_body("plain", false).to_string(),
        )
        .await;
    plain.assert_problem(422, "validation_failed");
    let text = String::from_utf8_lossy(&plain.body).to_string();
    assert!(
        text.contains("plain_requires_tls") && text.contains("PlainWithoutTls"),
        "{text}"
    );
    assert!(!text.contains(SEEDED_PASSWORD), "{text}");
    // CONTROL: PLAIN over TLS.
    assert_eq!(
        app.post(
            &connections(),
            Some("conn-plain-02"),
            &sasl_body("plain", true).to_string()
        )
        .await
        .status,
        201
    );

    let mtls = app
        .post(
            &connections(),
            Some("conn-mtls-02"),
            &mtls_body(false).to_string(),
        )
        .await;
    mtls.assert_problem(422, "validation_failed");
    assert!(String::from_utf8_lossy(&mtls.body).contains("mtls_requires_tls"));

    let mut encrypted = mtls_body(true);
    encrypted["auth"]["credential"]["privateKeyPem"] =
        json!(pem(concat!("ENCRYPTED PRIVATE", " KEY"), SEEDED_PASSWORD));
    let refused = app
        .post(&connections(), Some("conn-mtls-03"), &encrypted.to_string())
        .await;
    refused.assert_problem(422, "validation_failed");
    let text = String::from_utf8_lossy(&refused.body).to_string();
    assert!(text.contains("encrypted"), "{text}");
    assert!(!text.contains(SEEDED_PASSWORD), "{text}");

    let mut not_pem = mtls_body(true);
    not_pem["auth"]["credential"]["certificatePem"] = json!("not a certificate");
    app.post(&connections(), Some("conn-mtls-04"), &not_pem.to_string())
        .await
        .assert_problem(422, "validation_failed");

    let mut secret_ca = mtls_body(true);
    secret_ca["auth"]["tlsCa"] = json!({"secretKeyRef": {"name": "x", "key": "ca.crt"}});
    let response = app
        .post(&connections(), Some("conn-mtls-05"), &secret_ca.to_string())
        .await;
    assert!(
        response.status.as_u16() >= 400,
        "a Secret-backed CA is not in this request's grammar: {}",
        response.status
    );
}

struct NoCredentialWrite;

impl Authorizer for NoCredentialWrite {
    fn namespaces(&self, _actor: &Actor) -> Vec<String> {
        vec![NS_A.to_string()]
    }

    fn allows(&self, _actor: &Actor, _namespace: &str, action: Action) -> bool {
        !matches!(action, Action::WriteCredential)
    }
}

/// Entering a credential needs `credential.write` as well as
/// `connection.create` — the same split the destinations route enforces. A
/// plaintext connection (no value) is the control.
#[tokio::test]
async fn entering_a_credential_needs_credential_write() {
    let app = TestApp::with(
        support::FakeKube::new(),
        Options {
            authorizer: Some(Arc::new(NoCredentialWrite)),
            ..Options::default()
        },
    );
    let refused = app
        .post(
            &connections(),
            Some("conn-authz-01"),
            &sasl_body("scramSha512", true).to_string(),
        )
        .await;
    assert_eq!(
        refused.status,
        403,
        "{}",
        String::from_utf8_lossy(&refused.body)
    );
    assert!(app.fake.requests().iter().all(|r| r.method != "POST"));
    let control = json!({
        "role": "source",
        "bootstrapServers": ["kafka.example:9092"],
        "auth": {"mode": "plaintext", "tls": false}
    });
    assert_eq!(
        app.post(&connections(), Some("conn-authz-02"), &control.to_string())
            .await
            .status,
        201
    );
}

/// A Secret already under the deterministic name is NEVER adopted on a fresh
/// request: `state_conflict`, nothing written — not even the connection.
#[tokio::test]
async fn a_taken_credential_name_writes_nothing() {
    // The name depends only on the actor, the namespace, the route and the
    // key, so one throwaway app learns it…
    let learn = TestApp::new();
    let created = learn
        .post(
            &connections(),
            Some("conn-taken-01"),
            &sasl_body("scramSha512", true).to_string(),
        )
        .await;
    assert_eq!(created.status, 201);
    let name = created.json()["item"]["name"].as_str().unwrap().to_string();

    // …and a fresh cluster holds a FOREIGN Secret under that credential name.
    let fake = support::FakeKube::new();
    fake.seed(
        "secrets",
        NS_A,
        json!({"metadata": {"name": format!("{name}-credential")}, "type": "Opaque"}),
    );
    let app = TestApp::with(fake.clone(), Options::default());
    let again = app
        .post(
            &connections(),
            Some("conn-taken-01"),
            &sasl_body("scramSha512", true).to_string(),
        )
        .await;
    again.assert_problem(409, "state_conflict");
    let writes: Vec<_> = fake
        .requests()
        .iter()
        .filter(|r| r.method == "POST" && !r.query.contains("dryRun"))
        .map(|r| r.path.clone())
        .collect();
    assert!(writes.is_empty(), "nothing may be written: {writes:?}");
    assert_eq!(fake.count("kafkaclusters", NS_A), 0);
}

/// The create-only Secret shape's `Debug` prints key NAMES, never the base64
/// data — a stray `{:?}` in a log line, a tracing field or a panic message
/// cannot carry a credential (the derived `Debug` it replaced did).
#[test]
fn a_write_only_credential_debug_names_keys_and_no_value() {
    let parts = weirkeeper::connection::credential::build_kafka_credential_secret(
        weirkeeper::connection::credential::NewKafkaCredential {
            namespace: NS_A,
            secret_name: "c-credential",
            connection_name: "c",
            password: weirkeeper::connection::credential::WriteOnlyPassword::new(
                SEEDED_PASSWORD.to_string(),
            ),
            request_id: None,
            binding: Some("v1:uid:sha256:00"),
            owner_uid: Some("uid"),
        },
    )
    .unwrap()
    .into_parts();
    let credential = logweir_api::kube::WriteOnlyCredential::from_parts(parts);
    let shown = format!("{credential:?}");
    let b64 = base64::engine::general_purpose::STANDARD.encode(SEEDED_PASSWORD);
    assert!(shown.contains("password"), "the key name is shown: {shown}");
    assert!(
        !shown.contains(SEEDED_PASSWORD) && !shown.contains(&b64),
        "{shown}"
    );
}

/// The request's credential block prints as `<redacted>` under `{:?}`, so a
/// stray debug line, a tracing field or a panic message on the request path
/// cannot carry the value. CONTROL: the username — not a secret — is shown.
#[test]
fn the_request_credential_debug_is_redacted() {
    let request: logweir_api::contract::ConnectionAuthRequest =
        serde_json::from_value(sasl_body("scramSha256", true)["auth"].clone()).unwrap();
    let shown = format!("{request:?}");
    assert!(shown.contains("logweir"), "the username is shown: {shown}");
    assert!(shown.contains("<redacted>"), "{shown}");
    assert!(!shown.contains(SEEDED_PASSWORD), "{shown}");
    let mtls: logweir_api::contract::ConnectionAuthRequest =
        serde_json::from_value(mtls_body(true)["auth"].clone()).unwrap();
    assert!(!format!("{mtls:?}").contains(SEEDED_PASSWORD));
}
