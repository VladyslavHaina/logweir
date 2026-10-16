//! **PROD-03.1: a connection's Schema Registry, entered once.** The URL and
//! username are on the `KafkaCluster`; the password is written into the
//! connection's own Secret under `registry-password`, bound to the connection
//! (its binding covers the registry) and labelled with the path's workspace,
//! and never echoed.

mod support;

use base64::Engine as _;
use serde_json::{json, Value};
use support::{connections, sasl_body, TestApp, NS_A, SEEDED_PASSWORD, WORKSPACE_LABEL};

fn posts(app: &TestApp, plural: &str) -> Vec<Value> {
    app.fake
        .requests()
        .into_iter()
        .filter(|r| r.method == "POST" && r.path.ends_with(plural) && !r.query.contains("dryRun"))
        .map(|r| serde_json::from_str(&r.body).unwrap())
        .collect()
}

fn registry_body(base: Value) -> Value {
    let mut body = base;
    body["schemaRegistry"] = json!({
        "url": "https://sr.example:8081",
        "username": "sr-user",
        "password": SEEDED_PASSWORD
    });
    body
}

fn decode(secret: &Value, key: &str) -> Option<String> {
    secret["data"][key].as_str().map(|v| {
        String::from_utf8(base64::engine::general_purpose::STANDARD.decode(v).unwrap()).unwrap()
    })
}

#[tokio::test]
async fn a_plaintext_connection_with_a_registry_password_gets_one_bound_secret() {
    let app = TestApp::new();
    let body = registry_body(json!({
        "role": "source",
        "bootstrapServers": ["kafka.example:9092"],
        "auth": {"mode": "plaintext", "tls": false}
    }));
    let created = app
        .post(&connections(), Some("conn-sr-01"), &body.to_string())
        .await;
    assert_eq!(
        created.status,
        201,
        "{}",
        String::from_utf8_lossy(&created.body)
    );
    let item = created.json()["item"].clone();
    let name = item["name"].as_str().unwrap().to_string();
    let secret_name = format!("{name}-credential");
    assert_eq!(item["schemaRegistry"]["url"], "https://sr.example:8081");
    assert_eq!(item["schemaRegistry"]["credentialRef"]["name"], secret_name);
    let cluster = posts(&app, "/kafkaclusters").pop().unwrap();
    assert_eq!(
        cluster["spec"]["schemaRegistry"],
        json!({"url": "https://sr.example:8081", "username": "sr-user",
               "secretRef": {"name": secret_name}})
    );
    let secret = posts(&app, "/secrets")
        .pop()
        .expect("the Secret is created");
    assert_eq!(secret["metadata"]["name"], secret_name);
    assert_eq!(secret["metadata"]["labels"][WORKSPACE_LABEL], NS_A);
    assert_eq!(
        decode(&secret, "registry-password").as_deref(),
        Some(SEEDED_PASSWORD)
    );
    assert_eq!(decode(&secret, "password"), None);
    // The binding is the controller's, and covers the registry.
    let mut stored = cluster.clone();
    stored["metadata"]["uid"] = item["uid"].clone();
    let stored: weirkeeper::crds::kafka_cluster::KafkaCluster =
        serde_json::from_value(stored).unwrap();
    let expected =
        weirkeeper::connection::resolve(&stored, weirkeeper::connection::ConnectionUse::Probe)
            .unwrap()
            .credential_binding()
            .unwrap();
    assert_eq!(
        decode(&secret, "logweir-binding").as_deref(),
        Some(expected.as_str())
    );
    // Never echoed.
    for text in [
        String::from_utf8_lossy(&created.body).to_string(),
        cluster.to_string(),
    ] {
        assert!(!text.contains(SEEDED_PASSWORD), "{text}");
    }
    app.fake.assert_strict();
}

#[tokio::test]
async fn a_sasl_connection_keeps_both_passwords_in_its_one_secret() {
    let app = TestApp::new();
    let body = registry_body(sasl_body("scramSha512", true));
    let created = app
        .post(&connections(), Some("conn-sr-02"), &body.to_string())
        .await;
    assert_eq!(
        created.status,
        201,
        "{}",
        String::from_utf8_lossy(&created.body)
    );
    let secret = posts(&app, "/secrets").pop().unwrap();
    assert_eq!(
        decode(&secret, "password").as_deref(),
        Some(SEEDED_PASSWORD)
    );
    assert_eq!(
        decode(&secret, "registry-password").as_deref(),
        Some(SEEDED_PASSWORD)
    );
}

#[tokio::test]
async fn a_bad_registry_block_is_refused_before_anything_is_written() {
    let app = TestApp::new();
    for registry in [
        json!({"url": "https://user:pw@sr:8081"}),
        json!({"url": "ftp://sr:8081"}),
        json!({"url": "https://sr:8081", "username": "u"}),
        json!({"url": "https://sr:8081", "password": SEEDED_PASSWORD}),
    ] {
        let mut body = sasl_body("scramSha512", true);
        body["schemaRegistry"] = registry.clone();
        app.post(&connections(), Some("conn-sr-bad"), &body.to_string())
            .await
            .assert_problem(422, "validation_failed");
    }
    assert!(app.fake.requests().iter().all(|r| r.method != "POST"));
    // CONTROL: a registry with no auth is accepted, and writes no extra key.
    let mut body = sasl_body("scramSha512", true);
    body["schemaRegistry"] = json!({"url": "http://sr:8081"});
    let created = app
        .post(&connections(), Some("conn-sr-ok"), &body.to_string())
        .await;
    assert_eq!(
        created.status,
        201,
        "{}",
        String::from_utf8_lossy(&created.body)
    );
    let secret = posts(&app, "/secrets").pop().unwrap();
    assert_eq!(decode(&secret, "registry-password"), None);
}
