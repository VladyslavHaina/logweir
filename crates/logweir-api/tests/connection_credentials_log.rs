//! PROD-01.3 security follow-up: the SEEDED-SECRET SCAN OF THE LOG for the
//! write-only connection credential.
//!
//! A TEST BINARY OF ITS OWN, deliberately. It installs a thread-local
//! `tracing` subscriber, and `tracing` caches each call site's interest
//! process-wide: in a binary whose other tests run the same handlers on other
//! threads with no subscriber, a call site can be cached as uninteresting
//! before this test's subscriber exists, and the capture comes back empty —
//! which the non-vacuity assertion below would report, but as a flake. Alone
//! in its binary, the capture is deterministic.

mod support;

use std::sync::Arc;

use base64::Engine as _;
use serde_json::{json, Value};
use support::{TestApp, NS_A};

/// The same seeded value `connection_credentials.rs` scans for.
const SEEDED_PASSWORD: &str = "seeded-Pw-9c1f7e2a-never-echoed";

fn connections() -> String {
    format!("/api/v1/namespaces/{NS_A}/connections")
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

// ------------------------------------------------------------ log capture

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

/// **SEEDED-SECRET SCAN OF THE LOG.** Every request below carries the seeded
/// value — a SASL password accepted, an mTLS private key accepted, and a
/// PLAIN-without-TLS password refused — under the production log filter at
/// its most verbose. The WHOLE captured log (request lines, audit records,
/// the handler's own line) carries neither the value nor its base64. The
/// capture is proven non-vacuous by the handler's "credential secret created"
/// line, which names the Secret and nothing else.
#[tokio::test]
async fn no_entered_credential_reaches_the_log() {
    let buffer = LogBuffer::default();
    let subscriber = tracing_subscriber::fmt()
        .json()
        .with_writer(buffer.clone())
        .with_env_filter(logweir_api::audit::log_filter_from("debug"))
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);

    let app = TestApp::new();
    let sasl = app
        .post(
            &connections(),
            Some("conn-log-sasl"),
            &sasl_body("plain", true).to_string(),
        )
        .await;
    assert_eq!(sasl.status, 201, "{}", String::from_utf8_lossy(&sasl.body));
    let mtls = app
        .post(
            &connections(),
            Some("conn-log-mtls"),
            &mtls_body(true).to_string(),
        )
        .await;
    assert_eq!(mtls.status, 201, "{}", String::from_utf8_lossy(&mtls.body));
    let refused = app
        .post(
            &connections(),
            Some("conn-log-clear"),
            &sasl_body("plain", false).to_string(),
        )
        .await;
    assert_eq!(
        refused.status,
        422,
        "{}",
        String::from_utf8_lossy(&refused.body)
    );

    let text = String::from_utf8_lossy(&buffer.0.lock().unwrap()).into_owned();
    assert!(
        text.matches("connection credential secret created").count() == 2,
        "the capture is not vacuous: both creates log the Secret's NAME:\n{text}"
    );
    let b64 = base64::engine::general_purpose::STANDARD.encode(SEEDED_PASSWORD);
    for (label, needle) in [("the value", SEEDED_PASSWORD), ("its base64", b64.as_str())] {
        assert!(!text.contains(needle), "{label} reached the log:\n{text}");
    }
    for response in [&sasl, &mtls, &refused] {
        assert!(!String::from_utf8_lossy(&response.body).contains(SEEDED_PASSWORD));
    }
}
