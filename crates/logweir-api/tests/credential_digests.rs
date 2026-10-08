//! PROD-01.3 fix round, review F1 (HIGH): **no stored or logged digest moves
//! when only an entered credential moves.**
//!
//! THE DEFECT THIS PINS. A create's canonical request was hashed into
//! `api.logweir.dev/request-sha256` (on the object), the audit record's
//! `requestHash` and the "created" log line — and the request carried the
//! entered SASL password (or S3 secret key). The scope digest mixed into the
//! hash is itself published (`api.logweir.dev/idempotency-scope-sha256`), so
//! anyone who may `get` the object — the chart's `logweir-viewer` included —
//! could rebuild the request from the public spec with a guessed password,
//! hash and compare: an offline confirmation oracle. The fix is that the
//! write-only DTOs serialize a placeholder, never a value.
//!
//! Each row creates the SAME request twice, on two fresh fakes under the same
//! Idempotency-Key, differing ONLY in the credential value, and requires every
//! published or logged digest to be byte-identical. NEGATIVE CONTROL: the same
//! pair differing in a NON-secret field (the username, the bucket) must move
//! the request digest, so the equality above is not a digest that hashes
//! nothing. And the reviewer's oracle, attempted: the request rebuilt with the
//! REAL value does not reproduce the published hash, while the request rebuilt
//! with the placeholder does (the recompute harness is right).
//!
//! A TEST BINARY OF ITS OWN, for the reason `connection_credentials_log.rs`
//! states: it installs a thread-local `tracing` subscriber.

mod support;

use std::sync::Arc;

use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use support::{TestApp, NS_A};

const KEY: &str = "digest-probe-key-0001";
const PASSWORD_ONE: &str = concat!("Summer", "2026!");
const PASSWORD_TWO: &str = concat!("Winter", "2025?");

#[derive(Clone, Default)]
struct LogBuffer(Arc<std::sync::Mutex<Vec<u8>>>);

struct LogWriter(Arc<std::sync::Mutex<Vec<u8>>>);

impl std::io::Write for LogWriter {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .expect("the log buffer lock holds")
            .extend_from_slice(data);
        Ok(data.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogBuffer {
    type Writer = LogWriter;

    fn make_writer(&'a self) -> Self::Writer {
        LogWriter(Arc::clone(&self.0))
    }
}

/// Every digest one create published or logged: the object's two
/// annotations, the audit record's two hashes, and the "created" line's two.
#[derive(Debug, PartialEq, Eq)]
struct Digests {
    annotation_request: String,
    annotation_scope: String,
    audit_request: String,
    audit_scope: String,
    log_request: String,
    log_scope: String,
}

fn connection_body(username: &str, password: &str) -> Value {
    json!({
        "role": "source",
        "bootstrapServers": ["kafka.example:9093"],
        "auth": {
            "mode": "plain",
            "username": username,
            "credential": {"password": password},
            "tls": true
        }
    })
}

fn destination_body(bucket: &str, secret_access_key: &str) -> Value {
    let mut body = support::destination_body("primary");
    body["storage"]["bucket"] = json!(bucket);
    body["access"]["archiveRead"] = json!({
        "mode": "secretKeys",
        "secret": {"new": {
            "accessKeyId": concat!("AKIA", "DIGESTPROBE01"),
            "secretAccessKey": secret_access_key,
        }}
    });
    body
}

/// One create on a FRESH fake under [`KEY`], and every digest it produced.
async fn create(path: &str, plural: &str, body: &Value) -> (Digests, Value) {
    let buffer = LogBuffer::default();
    let subscriber = tracing_subscriber::fmt()
        .json()
        .with_writer(buffer.clone())
        .with_env_filter(logweir_api::audit::log_filter_from("debug"))
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);
    let app = TestApp::new();
    let created = app.post(path, Some(KEY), &body.to_string()).await;
    assert_eq!(
        created.status,
        201,
        "{}",
        String::from_utf8_lossy(&created.body)
    );
    let object: Value = app
        .fake
        .requests()
        .into_iter()
        .find(|r| r.method == "POST" && r.path.ends_with(plural) && !r.query.contains("dryRun"))
        .map(|r| serde_json::from_str(&r.body).unwrap())
        .expect("the object create");
    let annotations = object["metadata"]["annotations"].clone();
    let text = String::from_utf8_lossy(&buffer.0.lock().unwrap()).into_owned();
    let lines: Vec<Value> = text
        .lines()
        .filter_map(|l| serde_json::from_str(l).ok())
        .collect();
    let audit: Vec<Value> = lines
        .iter()
        .filter(|l| l["target"] == "logweir_api::audit")
        .filter_map(|l| {
            l["fields"]["audit"]
                .as_str()
                .and_then(|a| serde_json::from_str::<Value>(a).ok())
        })
        .filter(|r| r["method"] == "POST")
        .collect();
    assert_eq!(audit.len(), 1, "one audit record for the create:\n{text}");
    let created_line = lines
        .iter()
        .find(|l| l["fields"]["message"] == "created")
        .unwrap_or_else(|| panic!("no `created` line:\n{text}"));
    let field = |v: &Value| v.as_str().unwrap_or_default().to_string();
    (
        Digests {
            annotation_request: field(&annotations["api.logweir.dev/request-sha256"]),
            annotation_scope: field(&annotations["api.logweir.dev/idempotency-scope-sha256"]),
            audit_request: field(&audit[0]["requestHash"]),
            audit_scope: field(&audit[0]["idempotencyKeyHash"]),
            log_request: field(&created_line["fields"]["request_sha256"]),
            log_scope: field(&created_line["fields"]["idempotency_scope_sha256"]),
        },
        object,
    )
}

/// The published request hash, recomputed the way the reviewer's probe did
/// it: from the route, the PUBLISHED scope digest and canonical bytes.
fn recompute(route: &str, scope: &str, canonical: &[u8]) -> String {
    let mut buf = b"logweir-api/request/v2\n".to_vec();
    buf.extend_from_slice(&(route.len() as u64).to_be_bytes());
    buf.extend_from_slice(route.as_bytes());
    buf.extend_from_slice(&hex::decode(scope.strip_prefix("sha256:").unwrap()).unwrap());
    buf.extend_from_slice(canonical);
    format!("sha256:{}", hex::encode(Sha256::digest(&buf)))
}

fn assert_nothing_is_empty(d: &Digests) {
    for (name, value) in [
        ("annotation request", &d.annotation_request),
        ("annotation scope", &d.annotation_scope),
        ("audit request", &d.audit_request),
        ("audit scope", &d.audit_scope),
        ("log request", &d.log_request),
        ("log scope", &d.log_scope),
    ] {
        assert!(value.starts_with("sha256:"), "{name} is a digest: {d:?}");
    }
}

#[tokio::test]
async fn no_stored_or_logged_digest_moves_when_only_the_credential_moves() {
    // ---- connections: the SASL password ------------------------------------
    let path = format!("/api/v1/namespaces/{NS_A}/connections");
    let (one, kc) = create(
        &path,
        "/kafkaclusters",
        &connection_body("logweir", PASSWORD_ONE),
    )
    .await;
    let (two, _) = create(
        &path,
        "/kafkaclusters",
        &connection_body("logweir", PASSWORD_TWO),
    )
    .await;
    assert_nothing_is_empty(&one);
    assert_eq!(
        one, two,
        "a connection's stored or logged digests moved when ONLY the password moved: \
         they are an offline confirmation oracle for it"
    );
    // NEGATIVE CONTROL: a non-secret field moves the request digest.
    let (other, _) = create(
        &path,
        "/kafkaclusters",
        &connection_body("someone-else", PASSWORD_ONE),
    )
    .await;
    assert_ne!(one.annotation_request, other.annotation_request);
    assert_ne!(one.audit_request, other.audit_request);
    assert_eq!(
        one.annotation_scope, other.annotation_scope,
        "same key, same scope"
    );

    // THE ORACLE, ATTEMPTED, from the object alone: the request the reviewer's
    // probe rebuilt — the typed request with the real password, serialized —
    // as the bytes the pre-fix serialization produced.
    let route = logweir_api::routes::connections::ROUTE_CREATE;
    let typed: logweir_api::contract::CreateConnectionRequest =
        serde_json::from_value(connection_body("logweir", PASSWORD_ONE)).unwrap();
    let placeholder = String::from_utf8(serde_json::to_vec(&typed).unwrap()).unwrap();
    assert!(
        !placeholder.contains(PASSWORD_ONE),
        "the canonical request carries the password: {placeholder}"
    );
    // CONTROL: the harness reproduces the published hash from what WAS hashed.
    assert_eq!(
        recompute(route, &one.annotation_scope, placeholder.as_bytes()),
        one.annotation_request,
        "the recompute harness must reproduce the published digest, or the next \
         assertion proves nothing"
    );
    let marker = format!(
        "\"password\":\"{}\"",
        logweir_api::contract::WRITE_ONLY_PLACEHOLDER
    );
    assert!(placeholder.contains(&marker), "{placeholder}");
    for guess in ["password", "logweir", PASSWORD_TWO, PASSWORD_ONE] {
        let with_value = placeholder.replace(&marker, &format!("\"password\":\"{guess}\""));
        assert_ne!(
            recompute(route, &one.annotation_scope, with_value.as_bytes()),
            one.annotation_request,
            "the published digest confirms the guess {guess:?}"
        );
    }
    assert!(
        !kc.to_string().contains(PASSWORD_ONE),
        "the KafkaCluster carries the password"
    );

    // ---- destinations: the S3 secret access key ------------------------------
    let path = format!("/api/v1/namespaces/{NS_A}/destinations");
    let first = concat!("wJalrXUtnFEMI", "K7MDENGbPxRfiCYEXAMPLEKEYONE");
    let second = concat!("wJalrXUtnFEMI", "K7MDENGbPxRfiCYEXAMPLEKEYTWO");
    let (one, dest) = create(
        &path,
        "/backupdestinations",
        &destination_body("kafka-backups", first),
    )
    .await;
    let (two, _) = create(
        &path,
        "/backupdestinations",
        &destination_body("kafka-backups", second),
    )
    .await;
    assert_nothing_is_empty(&one);
    assert_eq!(
        one, two,
        "a destination's stored or logged digests moved when ONLY the secret key moved"
    );
    let (other, _) = create(
        &path,
        "/backupdestinations",
        &destination_body("other-bucket", first),
    )
    .await;
    assert_ne!(
        one.annotation_request, other.annotation_request,
        "negative control"
    );
    assert!(
        !dest.to_string().contains(first),
        "the destination carries the key"
    );
}

/// The write-only DTOs, serialized on their own: a placeholder for every
/// present field, `null` for an absent one, never a value.
#[test]
fn the_write_only_dtos_serialize_no_value() {
    let connection: logweir_api::contract::NewConnectionCredentialRequest =
        serde_json::from_value(json!({
            "password": PASSWORD_ONE,
            "privateKeyPem": PASSWORD_TWO,
        }))
        .unwrap();
    assert_eq!(
        connection.password.as_deref(),
        Some(PASSWORD_ONE),
        "deserialized as entered"
    );
    assert_eq!(
        serde_json::to_value(&connection).unwrap(),
        json!({
            "password": logweir_api::contract::WRITE_ONLY_PLACEHOLDER,
            "certificatePem": null,
            "privateKeyPem": logweir_api::contract::WRITE_ONLY_PLACEHOLDER,
        })
    );
    let s3: logweir_api::contract::NewCredentialRequest = serde_json::from_value(json!({
        "accessKeyId": "AKIA",
        "secretAccessKey": PASSWORD_ONE,
        "sessionToken": PASSWORD_TWO,
    }))
    .unwrap();
    let text = serde_json::to_string(&s3).unwrap();
    for value in ["AKIA", PASSWORD_ONE, PASSWORD_TWO] {
        assert!(!text.contains(value), "{text}");
    }
}
