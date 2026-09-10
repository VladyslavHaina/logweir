//! A `kube::Client` over a recorded route table.
//!
//! WHAT THIS IS FOR. Every reconciler this crate will hold is a function of
//! what the API server answers. A test that cannot say "the API server answers
//! exactly these calls and nothing else" cannot distinguish a reconciler that
//! read the right objects from one that happened to reach the same conclusion
//! after reading the wrong ones. So the double does two things a stub usually
//! does not: it RECORDS every request in order, and it PANICS on a request it
//! was not given a route for.
//!
//! THE PANIC IS THE POINT, AND A 404 IS NOT AN ACCEPTABLE SUBSTITUTE. A 404
//! is a legitimate API-server answer: `Api::get` on an absent object returns
//! one, and reconcilers are written to handle it. A double that answers 404
//! for "no route recorded" is therefore indistinguishable, from inside the
//! reconciler, from "the object does not exist" — and every later test in
//! chain O silently loses its "called nothing it did not expect" property.
//! Refusing loudly, naming the method and the path, is what keeps that
//! property.
//!
//! WHERE THE PANIC IS OBSERVABLE, AND WHY THERE. `kube::Client::new` wraps the
//! service it is handed in `tower::buffer::Buffer`
//! (`kube-client-0.99.0/src/client/mod.rs:157`), which drives the inner
//! service on a `tokio::spawn`ed worker task. A panic on that task is caught
//! by tokio's task harness: the message reaches stderr through the panic hook
//! and the request fails, but the unwind cannot reach the test's own thread, so
//! `#[should_panic]` around a `kube::Api` call would never fire. The route
//! resolution is therefore a plain function, [`answer`], which the service
//! calls and which a test can call directly — one implementation, two entry
//! points, and the refusal is asserted where it can be seen. The end-to-end
//! path is asserted separately, over a MATCHED route, in `tests/linkage.rs`.
//!
//! NOTHING HERE DIALS ANYTHING. There is no socket, no `Config`, no
//! kubeconfig read and no `Client::try_default`; the transport is a closure.
//! That is what lets every reconciler test live in the default
//! `cargo test --workspace` suite under Global Constraint 22's 15 s bound
//! instead of behind an `e2e` feature gate.

use std::sync::{Arc, Mutex};

use http::{Request, Response};
use kube::client::Body;
use tower::util::service_fn;

/// One recorded answer.
///
/// `path_suffix` is matched with `str::ends_with` against the request path,
/// deliberately: a `kube::Api` builds long, versioned, namespaced paths
/// (`/apis/logweir.dev/v1alpha1/namespaces/logweir-t16/restores/r1`) whose
/// prefix is the client's business and not the test's. A suffix keeps the
/// route table readable while still failing on the wrong resource, the wrong
/// namespace or the wrong name.
#[derive(Debug, Clone)]
pub struct Route {
    /// The HTTP method, compared case-insensitively (`"GET"`, `"PATCH"`).
    pub method: &'static str,
    /// The tail of the request path this route answers.
    pub path_suffix: &'static str,
    /// The status to answer with.
    pub status: u16,
    /// The response body, verbatim.
    pub body: String,
}

/// One request the double was asked for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeenRequest {
    /// The method as the client sent it.
    pub method: String,
    /// The request target, query string included.
    pub uri: String,
}

/// The ordered log of everything the double was asked for.
///
/// Shared rather than returned by value because the client owns its service
/// once [`mock_client`] hands it over; the test keeps this end.
pub type Recorder = Arc<Mutex<Vec<SeenRequest>>>;

/// A fresh, empty [`Recorder`].
#[must_use]
pub fn recorder() -> Recorder {
    Arc::new(Mutex::new(Vec::new()))
}

/// Resolve one request against a route table, recording it first.
///
/// # Panics
///
/// When no route matches, naming the method and the path — see the module
/// documentation for why this is a panic and not a 404.
#[must_use]
pub fn answer(routes: &[Route], recorder: &Recorder, method: &str, uri: &str) -> Response<Body> {
    // The path without its query string: a route table is written against
    // paths, and `Api::list`-shaped calls carry field/label selectors that a
    // test should not have to spell to match a route.
    let path = uri.split('?').next().unwrap_or(uri);

    // Recorded BEFORE the match, so an unmatched request is in the log the
    // panic message is about. A recorder that only sees matched requests
    // cannot answer "what did the reconciler actually ask for?", which is the
    // first question anyone asks when this panic fires.
    recorder
        .lock()
        .expect("the recorder mutex is never held across a panic in this module")
        .push(SeenRequest {
            method: method.to_string(),
            uri: uri.to_string(),
        });

    let hit = routes
        .iter()
        .find(|r| r.method.eq_ignore_ascii_case(method) && path.ends_with(r.path_suffix));

    match hit {
        Some(r) => Response::builder()
            .status(r.status)
            .body(Body::from(r.body.clone().into_bytes()))
            .expect("a recorded status and body always build a response"),
        None => {
            let table = routes
                .iter()
                .map(|r| format!("{} …{}", r.method, r.path_suffix))
                .collect::<Vec<_>>()
                .join(", ");
            panic!(
                "weirkeeper mock_client: no route for {method} {path}\n  \
                 the route table holds: [{table}]\n  \
                 A reconciler asking for something its test did not record is either a bug in \
                 the reconciler or a gap in the table. It is never a 404: a 404 is a real \
                 API-server answer and would be indistinguishable from 'the object does not \
                 exist'."
            )
        }
    }
}

/// A [`kube::Client`] answering `routes`, plus the [`Recorder`] it writes to.
///
/// # Panics
///
/// `kube::Client::new` builds a `tower::buffer::Buffer`, which spawns; call
/// this from inside a tokio runtime (`#[tokio::test]`).
#[must_use]
pub fn mock_client_recording(routes: Vec<Route>) -> (kube::Client, Recorder) {
    let recorder = recorder();
    let table = Arc::new(routes);
    let svc = {
        let recorder = Arc::clone(&recorder);
        service_fn(move |req: Request<Body>| {
            let recorder = Arc::clone(&recorder);
            let table = Arc::clone(&table);
            async move {
                let method = req.method().to_string();
                let uri = req.uri().to_string();
                Ok::<_, std::convert::Infallible>(answer(&table, &recorder, &method, &uri))
            }
        })
    };
    // `"default"` is the client's DEFAULT namespace only — the namespace an
    // `Api::default_namespaced` would use when a test does not say. Every
    // route in this plan's tests names its namespace in the path.
    (kube::Client::new(svc, "default"), recorder)
}

/// A [`kube::Client`] answering `routes`.
///
/// The plain form, for a test that asserts over the reconciler's OUTPUT rather
/// than over the call sequence. Use [`mock_client_recording`] when the call
/// sequence itself is the property.
///
/// # Panics
///
/// As [`mock_client_recording`]: this builds a `tower::buffer::Buffer` and
/// must be called from inside a tokio runtime.
#[must_use]
pub fn mock_client(routes: Vec<Route>) -> kube::Client {
    mock_client_recording(routes).0
}
