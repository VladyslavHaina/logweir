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
use http_body_util::BodyExt as _;
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

/// One request the double was asked for, **with its body**.
///
/// WHY A SECOND TYPE AND NOT A FOURTH FIELD ON [`SeenRequest`] (Task 18). A
/// request body is only reachable asynchronously — `http_body::Body` is a
/// stream — so the plain [`answer`] route resolver, which is a synchronous
/// function two existing tests call directly, cannot produce one. Adding a
/// `body` field to `SeenRequest` would either force that function async or
/// leave the field empty in the one place a test can observe it; a separate
/// log, filled by the service closure that already owns the whole `Request`,
/// keeps both truthful.
///
/// WHY ANY TEST NEEDS IT. A `POST` to a collection carries the object's
/// `metadata.name` in its **body**, never in its path — `Api::create` targets
/// `…/namespaces/<ns>/backups` with no name in the URI at all. Guard
/// **G-SLOT**'s property is "both `POST`s carried the identical
/// `metadata.name`", so it is unassertable from method and URI alone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeenBody {
    /// The method as the client sent it.
    pub method: String,
    /// The request target, query string included.
    pub uri: String,
    /// The request body as UTF-8. Empty for a request with no body, and for a
    /// body that did not collect — neither of which any test in this tree
    /// asserts over.
    pub body: String,
}

/// The ordered log of every request body the double was asked for.
pub type BodyRecorder = Arc<Mutex<Vec<SeenBody>>>;

/// A fresh, empty [`BodyRecorder`].
#[must_use]
pub fn body_recorder() -> BodyRecorder {
    Arc::new(Mutex::new(Vec::new()))
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

/// A [`kube::Client`] answering `routes`, plus both recorders.
///
/// The form for a test whose property is what a request **contained** — see
/// [`SeenBody`] for why method and URI are not enough for a `POST` to a
/// collection. The [`Recorder`] end is returned unchanged, so a test can assert
/// the call sequence and the bodies off one client.
///
/// # Panics
///
/// As [`mock_client_recording`]: this builds a `tower::buffer::Buffer` and must
/// be called from inside a tokio runtime.
#[must_use]
pub fn mock_client_recording_bodies(routes: Vec<Route>) -> (kube::Client, Recorder, BodyRecorder) {
    let recorder = recorder();
    let bodies = body_recorder();
    let table = Arc::new(routes);
    let svc = {
        let recorder = Arc::clone(&recorder);
        let bodies = Arc::clone(&bodies);
        service_fn(move |req: Request<Body>| {
            let recorder = Arc::clone(&recorder);
            let bodies = Arc::clone(&bodies);
            let table = Arc::clone(&table);
            async move {
                let method = req.method().to_string();
                let uri = req.uri().to_string();
                // The body is COLLECTED BEFORE the route is resolved, so an
                // unmatched request's body is in the log the panic is about —
                // the same ordering `answer` uses for the request log, and for
                // the same reason.
                let body = req
                    .into_body()
                    .collect()
                    .await
                    .map(|c| String::from_utf8_lossy(&c.to_bytes()).into_owned())
                    .unwrap_or_default();
                bodies
                    .lock()
                    .expect("the body recorder mutex is never held across a panic")
                    .push(SeenBody {
                        method: method.clone(),
                        uri: uri.clone(),
                        body,
                    });
                Ok::<_, std::convert::Infallible>(answer(&table, &recorder, &method, &uri))
            }
        })
    };
    (kube::Client::new(svc, "default"), recorder, bodies)
}

/// [`mock_client_recording_bodies`], with ONE route that changes its answer
/// after it has been asked `after` times.
///
/// # Why the double needs this at all
///
/// A [`Route`] answers every matching request identically, which is right for
/// almost everything and wrong for exactly one shape of property: *the Nth
/// write of a pass is refused*. A reconciler that writes `/status` more than
/// once in a pass writes to ONE path, so a table cannot say "the first two
/// land and the third conflicts" — and that is the only sequence in which
/// defect RET-STARTRUN-PATCH-OUTCOME's failure occurs. Answering 409 to all of
/// them refuses the first write instead, which is a different branch and
/// proves a different thing.
///
/// `after` is a count of MATCHING requests already answered: `after: 2` leaves
/// the first two alone and gives the third and every later one `status` and
/// `body`. The switch is counted inside the service, so it holds across the
/// `tower::buffer::Buffer` worker the client spawns.
///
/// # Panics
///
/// As [`mock_client_recording`]: this builds a `tower::buffer::Buffer` and must
/// be called from inside a tokio runtime.
#[must_use]
pub fn mock_client_failing_after(
    routes: Vec<Route>,
    method: &'static str,
    path_suffix: &'static str,
    after: usize,
    status: u16,
    body: String,
) -> (kube::Client, Recorder, BodyRecorder) {
    let recorder = recorder();
    let bodies = body_recorder();
    let table = Arc::new(routes);
    let seen = Arc::new(Mutex::new(0usize));
    let svc = {
        let recorder = Arc::clone(&recorder);
        let bodies = Arc::clone(&bodies);
        service_fn(move |req: Request<Body>| {
            let recorder = Arc::clone(&recorder);
            let bodies = Arc::clone(&bodies);
            let table = Arc::clone(&table);
            let seen = Arc::clone(&seen);
            let body = body.clone();
            async move {
                let request_method = req.method().to_string();
                let uri = req.uri().to_string();
                let request_body = req
                    .into_body()
                    .collect()
                    .await
                    .map(|c| String::from_utf8_lossy(&c.to_bytes()).into_owned())
                    .unwrap_or_default();
                bodies
                    .lock()
                    .expect("the body recorder mutex is never held across a panic")
                    .push(SeenBody {
                        method: request_method.clone(),
                        uri: uri.clone(),
                        body: request_body,
                    });
                let path = uri.split('?').next().unwrap_or(&uri).to_string();
                let matches =
                    request_method.eq_ignore_ascii_case(method) && path.ends_with(path_suffix);
                let refuse = matches && {
                    let mut count = seen
                        .lock()
                        .expect("the counter mutex is never held across a panic");
                    *count += 1;
                    *count > after
                };
                if refuse {
                    // RECORDED LIKE ANY OTHER REQUEST, so the call sequence a
                    // test asserts over still contains the refused write.
                    recorder
                        .lock()
                        .expect("the recorder mutex is never held across a panic")
                        .push(SeenRequest {
                            method: request_method,
                            uri,
                        });
                    return Ok::<_, std::convert::Infallible>(
                        Response::builder()
                            .status(status)
                            .body(Body::from(body.into_bytes()))
                            .expect("a recorded status and body always build a response"),
                    );
                }
                Ok::<_, std::convert::Infallible>(answer(&table, &recorder, &request_method, &uri))
            }
        })
    };
    (kube::Client::new(svc, "default"), recorder, bodies)
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
