//! Every routed request passes through the access layer — checked at runtime,
//! per declared route, from the audit record the layer signs (review M1).
//!
//! WHY A RUNTIME CHECK AS WELL AS THE SOURCE POSITION. A route that runs
//! outside the layer still gets a 401 from its handler's own `Actor`
//! extractor, and a handler that authorizes on its own still refuses the
//! wrong role — so every HTTP-answer sweep passes for a route the layer never
//! saw (the reviewer's mutant R5 survived 65/65 that way). The layer records
//! the declaration it applied as the audit note `routeAccess`; this test sends
//! one request to every declared route through the real router and requires
//! that note, with the declared kind, on its record.

mod support;

use std::io::Write;
use std::sync::{Arc, Mutex};

use axum::body::Body;
use http::Request;
use logweir_api::access::{Access, ROUTES};
use serde_json::Value;
use support::{FakeKube, SharedApp, SharedOptions, NS_A, SHARED_HOST, SHARED_ORIGIN};

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

fn notes_of(buffer: &Buffer, audit_id: &str) -> Value {
    let text = String::from_utf8_lossy(&buffer.0.lock().unwrap()).into_owned();
    text.lines()
        .filter_map(|l| serde_json::from_str::<Value>(l).ok())
        .filter(|l| l["target"] == "logweir_api::audit")
        .find_map(|l| {
            let record: Value = serde_json::from_str(l["fields"]["audit"].as_str()?).ok()?;
            (record["auditId"] == audit_id).then(|| {
                serde_json::from_str(l["fields"]["notes"].as_str().unwrap_or("{}"))
                    .unwrap_or(Value::Null)
            })
        })
        .unwrap_or_else(|| panic!("no audit record for {audit_id}"))
}

fn concrete(path: &str) -> String {
    path.replace("{ns}", NS_A)
        .replace("{kind}", "backup")
        .replace("{*path}", "index.html")
        .replace("{name}", "x1")
        .replace("{id}", "x1")
}

/// **Every declared route's request carries the layer's signature, with the
/// declared kind.** NEGATIVE CONTROL: an unrouted path (the fallback) carries
/// none, so the note is the layer's and not something every record has.
#[tokio::test]
async fn every_declared_route_is_decided_by_the_access_layer() {
    let buffer = Buffer::default();
    let subscriber = tracing_subscriber::fmt()
        .json()
        .with_writer(buffer.clone())
        .with_env_filter(logweir_api::audit::log_filter_from("info"))
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);
    let app = SharedApp::new(
        FakeKube::new(),
        support::idp::MockIdp::new(support::ISSUER, &[]),
        SharedOptions {
            bindings: support::default_bindings(),
            ..SharedOptions::default()
        },
    );
    let mut checked = 0;
    for entry in ROUTES {
        let mut builder = Request::builder()
            .method(entry.method)
            .uri(concrete(entry.path))
            .header("host", SHARED_HOST);
        if entry.method != "GET" {
            builder = builder
                .header("origin", SHARED_ORIGIN)
                .header("content-type", "application/json");
        }
        let response = app.app.send(builder.body(Body::from("{}")).unwrap()).await;
        let id = response.header("x-request-id").unwrap();
        let notes = notes_of(&buffer, &id);
        let want = entry.access.kind();
        assert_eq!(
            notes["routeAccess"], want,
            "{} {} ran without the access layer's decision (notes {notes})",
            entry.method, entry.path
        );
        if entry.access != Access::Public {
            assert_eq!(response.status, 401, "{} {}", entry.method, entry.path);
        }
        checked += 1;
    }
    assert!(checked > 50, "{checked}");

    let unrouted = app
        .app
        .send(
            Request::builder()
                .uri("/api/v1/nowhere")
                .header("host", SHARED_HOST)
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    assert_eq!(unrouted.status, 404);
    let notes = notes_of(&buffer, &unrouted.header("x-request-id").unwrap());
    assert!(notes.get("routeAccess").is_none(), "{notes}");
}
