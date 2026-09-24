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
    let (status, body) = answer_parts(routes, recorder, method, uri);
    respond(status, body)
}

fn respond(status: u16, body: String) -> Response<Body> {
    Response::builder()
        .status(status)
        .body(Body::from(body.into_bytes()))
        .expect("a recorded status and body always build a response")
}

/// [`answer`], as the status and body the route recorded, so the
/// resourceVersion ledger can rewrite the body before it is sent.
fn answer_parts(routes: &[Route], recorder: &Recorder, method: &str, uri: &str) -> (u16, String) {
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
        Some(r) => (r.status, r.body.clone()),
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

/// Where every object this double has accepted a write for now stands.
///
/// # THE DOUBLE ENFORCES SEAM S7, BECAUSE THE API SERVER DOES
///
/// Defect REHEARSAL-FIRE-PASS-STATUS-LOST lived for a whole release behind a
/// double that answered every `PATCH` identically: a reconciler that reserved
/// a slot with one resourceVersion-preconditioned write and then committed the
/// pass with a SECOND write preconditioned on the SAME, now stale, version was
/// answered `200` twice here and `409` on the second write by every real API
/// server. So the double keeps the one piece of state that makes the
/// difference, per object path (`…/<kind>/<name>`, `/status` folded in, since
/// the subresource shares its parent's version):
///
/// * An accepted `PATCH`/`PUT` BUMPS the object's version, and the response
///   body is rewritten to carry the new one, exactly as the API server answers
///   with the object it stored.
/// * A write whose body carries `metadata.resourceVersion` different from the
///   version the ledger holds is answered `409 Conflict` — the API server's
///   own Status — and is recorded like any other request.
/// * A `GET` of an object the ledger holds is rewritten to carry its current
///   version, so a reconciler that re-reads after a conflict sees what a real
///   re-read would.
///
/// Until the first accepted write the ledger knows nothing about an object,
/// so the first write of a test is never refused on a version: the fixture's
/// own `resourceVersion` is the starting point. Every mock constructor in this
/// module applies it — a test cannot opt out of the API server's semantics.
pub type RvLedger = Arc<Mutex<std::collections::HashMap<String, String>>>;

/// The ledger key: the object path, with the `/status` subresource folded
/// into its parent.
fn object_key(path: &str) -> &str {
    path.strip_suffix("/status").unwrap_or(path)
}

fn precondition_of(body: &str) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(body).ok()?;
    value
        .get("metadata")?
        .get("resourceVersion")?
        .as_str()
        .filter(|v| !v.is_empty())
        .map(str::to_string)
}

fn version_of(body: &str) -> Option<String> {
    precondition_of(body)
}

fn bumped(version: &str) -> String {
    version
        .parse::<u64>()
        .map_or_else(|_| format!("{version}.1"), |n| (n + 1).to_string())
}

fn with_version(body: String, version: &str) -> String {
    match serde_json::from_str::<serde_json::Value>(&body) {
        Ok(serde_json::Value::Object(mut object))
            if object
                .get("metadata")
                .is_some_and(serde_json::Value::is_object) =>
        {
            object["metadata"]["resourceVersion"] = serde_json::Value::String(version.to_string());
            serde_json::to_string(&object).unwrap_or(body)
        }
        _ => body,
    }
}

/// The API server's `409 Conflict` for a stale precondition.
fn conflict_status(path: &str) -> String {
    serde_json::json!({
        "kind": "Status",
        "apiVersion": "v1",
        "metadata": {},
        "status": "Failure",
        "message": format!(
            "Operation cannot be fulfilled on {path}: the object has been modified; please \
             apply your changes to the latest version and try again"
        ),
        "reason": "Conflict",
        "details": {},
        "code": 409
    })
    .to_string()
}

/// Resolve one request through the route table AND the [`RvLedger`].
fn answer_enforcing(
    routes: &[Route],
    recorder: &Recorder,
    ledger: &RvLedger,
    method: &str,
    uri: &str,
    body: &str,
) -> Response<Body> {
    let path = uri.split('?').next().unwrap_or(uri);
    let key = object_key(path).to_string();
    let write = method.eq_ignore_ascii_case("PATCH") || method.eq_ignore_ascii_case("PUT");
    if write {
        let precondition = precondition_of(body);
        let current = ledger
            .lock()
            .expect("the ledger mutex is never held across a panic")
            .get(&key)
            .cloned();
        if let (Some(wanted), Some(stored)) = (&precondition, &current) {
            if wanted != stored {
                recorder
                    .lock()
                    .expect("the recorder mutex is never held across a panic")
                    .push(SeenRequest {
                        method: method.to_string(),
                        uri: uri.to_string(),
                    });
                return respond(409, conflict_status(path));
            }
        }
        let (status, response) = answer_parts(routes, recorder, method, uri);
        if (200..300).contains(&status) {
            let base = current.or(precondition);
            // A ROUTE THAT ANSWERS WITH ITS OWN, DIFFERENT VERSION IS BELIEVED:
            // it is the test author saying where the write left the object.
            // Only a static body that did not move (or carries none) is bumped.
            let answered = version_of(&response).filter(|v| Some(v) != base.as_ref());
            if let Some(next) = answered.or_else(|| base.as_deref().map(bumped)) {
                ledger
                    .lock()
                    .expect("the ledger mutex is never held across a panic")
                    .insert(key, next.clone());
                return respond(status, with_version(response, &next));
            }
        }
        return respond(status, response);
    }
    let (status, response) = answer_parts(routes, recorder, method, uri);
    if method.eq_ignore_ascii_case("GET") && (200..300).contains(&status) {
        let current = ledger
            .lock()
            .expect("the ledger mutex is never held across a panic")
            .get(&key)
            .cloned();
        if let Some(current) = current {
            return respond(status, with_version(response, &current));
        }
    }
    respond(status, response)
}

async fn collect_body(req: Request<Body>) -> String {
    req.into_body()
        .collect()
        .await
        .map(|c| String::from_utf8_lossy(&c.to_bytes()).into_owned())
        .unwrap_or_default()
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
    let ledger = RvLedger::default();
    let svc = {
        let recorder = Arc::clone(&recorder);
        service_fn(move |req: Request<Body>| {
            let recorder = Arc::clone(&recorder);
            let table = Arc::clone(&table);
            let ledger = Arc::clone(&ledger);
            async move {
                let method = req.method().to_string();
                let uri = req.uri().to_string();
                let body = collect_body(req).await;
                Ok::<_, std::convert::Infallible>(answer_enforcing(
                    &table, &recorder, &ledger, &method, &uri, &body,
                ))
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
    let ledger = RvLedger::default();
    let svc = {
        let recorder = Arc::clone(&recorder);
        let bodies = Arc::clone(&bodies);
        service_fn(move |req: Request<Body>| {
            let recorder = Arc::clone(&recorder);
            let bodies = Arc::clone(&bodies);
            let table = Arc::clone(&table);
            let ledger = Arc::clone(&ledger);
            async move {
                let method = req.method().to_string();
                let uri = req.uri().to_string();
                // The body is COLLECTED BEFORE the route is resolved, so an
                // unmatched request's body is in the log the panic is about —
                // the same ordering `answer` uses for the request log, and for
                // the same reason.
                let body = collect_body(req).await;
                bodies
                    .lock()
                    .expect("the body recorder mutex is never held across a panic")
                    .push(SeenBody {
                        method: method.clone(),
                        uri: uri.clone(),
                        body: body.clone(),
                    });
                Ok::<_, std::convert::Infallible>(answer_enforcing(
                    &table, &recorder, &ledger, &method, &uri, &body,
                ))
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
    let ledger = RvLedger::default();
    let svc = {
        let recorder = Arc::clone(&recorder);
        let bodies = Arc::clone(&bodies);
        service_fn(move |req: Request<Body>| {
            let recorder = Arc::clone(&recorder);
            let bodies = Arc::clone(&bodies);
            let table = Arc::clone(&table);
            let seen = Arc::clone(&seen);
            let ledger = Arc::clone(&ledger);
            let body = body.clone();
            async move {
                let request_method = req.method().to_string();
                let uri = req.uri().to_string();
                let request_body = collect_body(req).await;
                bodies
                    .lock()
                    .expect("the body recorder mutex is never held across a panic")
                    .push(SeenBody {
                        method: request_method.clone(),
                        uri: uri.clone(),
                        body: request_body.clone(),
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
                Ok::<_, std::convert::Infallible>(answer_enforcing(
                    &table,
                    &recorder,
                    &ledger,
                    &request_method,
                    &uri,
                    &request_body,
                ))
            }
        })
    };
    (kube::Client::new(svc, "default"), recorder, bodies)
}

/// One write the [`ObjectStore`] answered, in order.
#[derive(Debug, Clone, PartialEq)]
pub struct StoreWrite {
    /// The method (`PATCH`, `PUT`, `POST`, `DELETE`).
    pub method: String,
    /// The request path, query string dropped.
    pub path: String,
    /// What the store answered: `200`/`201`, `404` or `409`.
    pub status: u16,
    /// The request body, parsed (`Null` when it was not JSON).
    pub body: serde_json::Value,
}

/// A stateful stand-in for the objects a test needs to SURVIVE a pass.
///
/// # Why the route table is not enough for a reservation protocol
///
/// A [`Route`] answers every request identically, so a table cannot express
/// "the first write of the pass moved the object, and the second write — the
/// one carrying the pass's result — was answered 409 and nothing of it was
/// stored". [`RvLedger`] makes the double REFUSE the stale write; this makes
/// the refusal OBSERVABLE: the object as it stands after the pass is here, and
/// the next pass can be handed exactly that object, as a watch would hand it.
///
/// Objects are keyed by a path suffix (`/rehearsalschedules/weekly-orders`),
/// matched with `ends_with` like a [`Route`]. For a tracked object the store
/// answers `GET` (the object, or `404` once removed), merge `PATCH` of the
/// object and of its `/status` subresource (seam S7's precondition enforced,
/// `metadata.resourceVersion` bumped on every accepted write; a `/status`
/// patch changes only `status`, a main-resource patch never changes it), and
/// `DELETE`. A COLLECTION registered with [`ObjectStore::collection`] also
/// answers `POST`: the object is stored under `<collection>/<metadata.name>`
/// with a UID and a version, and a name already held is the API server's
/// `409 AlreadyExists`. Everything else falls through to the route table.
#[derive(Debug, Default)]
pub struct ObjectStore {
    objects: std::collections::HashMap<String, Option<serde_json::Value>>,
    collections: Vec<String>,
    writes: Vec<StoreWrite>,
    next_uid: u64,
    interleaved: Vec<Interleave>,
}

/// ANOTHER WRITER, landing between two requests of the pass under test.
#[derive(Debug, Clone)]
struct Interleave {
    trigger_method: String,
    trigger_suffix: String,
    target: String,
    patch: serde_json::Value,
}

/// The shared handle a test keeps while the client owns the other end.
pub type SharedStore = Arc<Mutex<ObjectStore>>;

impl ObjectStore {
    /// A fresh, empty, shareable store.
    #[must_use]
    pub fn shared() -> SharedStore {
        Arc::new(Mutex::new(Self::default()))
    }

    /// Track `object` under `suffix`, replacing whatever was there.
    pub fn put(&mut self, suffix: &str, object: serde_json::Value) {
        self.objects.insert(suffix.to_string(), Some(object));
    }

    /// The object under `suffix`, if it is tracked and not removed.
    #[must_use]
    pub fn get(&self, suffix: &str) -> Option<serde_json::Value> {
        self.objects.get(suffix).cloned().flatten()
    }

    /// Delete the object under `suffix`: later `GET`s answer `404`.
    pub fn remove(&mut self, suffix: &str) -> Option<serde_json::Value> {
        self.objects.insert(suffix.to_string(), None).flatten()
    }

    /// Answer `POST`s to the collection ending in `suffix`.
    pub fn collection(&mut self, suffix: &str) {
        self.collections.push(suffix.to_string());
    }

    /// Once, right after the store answers the first `method` request whose
    /// path ends with `trigger`, apply `patch` (a merge patch over the whole
    /// object, `status` included) to the object under `target` and bump its
    /// version — another writer landing between two requests of the pass
    /// under test, which is the only way a genuine conflict can be staged.
    /// When nothing is stored under `target`, the other writer CREATES it and
    /// `patch` is the whole object.
    pub fn interleave(
        &mut self,
        method: &str,
        trigger: &str,
        target: &str,
        patch: serde_json::Value,
    ) {
        self.interleaved.push(Interleave {
            trigger_method: method.to_string(),
            trigger_suffix: trigger.to_string(),
            target: target.to_string(),
            patch,
        });
    }

    fn run_interleaved(&mut self, method: &str, path: &str) {
        let Some(at) = self.interleaved.iter().position(|i| {
            i.trigger_method.eq_ignore_ascii_case(method) && path.ends_with(&i.trigger_suffix)
        }) else {
            return;
        };
        let other = self.interleaved.remove(at);
        if let Some(mut object) = self.get(&other.target) {
            let current = object
                .pointer("/metadata/resourceVersion")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("0")
                .to_string();
            crate::conditions::apply_merge_patch(&mut object, &other.patch);
            object["metadata"]["resourceVersion"] = serde_json::json!(bumped(&current));
            self.writes.push(StoreWrite {
                method: "PATCH".to_string(),
                path: format!("{} (another writer)", other.target),
                status: 200,
                body: other.patch,
            });
            self.objects.insert(other.target, Some(object));
        } else {
            // THE OTHER WRITER CREATES IT: `patch` is the whole object.
            self.writes.push(StoreWrite {
                method: "POST".to_string(),
                path: format!("{} (another writer)", other.target),
                status: 201,
                body: other.patch.clone(),
            });
            self.objects.insert(other.target, Some(other.patch));
        }
    }

    /// Every write answered so far, in order.
    #[must_use]
    pub fn writes(&self) -> Vec<StoreWrite> {
        self.writes.clone()
    }

    fn tracked(&self, path: &str) -> Option<(String, bool)> {
        self.objects.keys().find_map(|key| {
            if path.ends_with(key.as_str()) {
                Some((key.clone(), false))
            } else if path.ends_with(&format!("{key}/status")) {
                Some((key.clone(), true))
            } else {
                None
            }
        })
    }

    fn record(&mut self, method: &str, path: &str, status: u16, body: &str) {
        self.writes.push(StoreWrite {
            method: method.to_string(),
            path: path.to_string(),
            status,
            body: serde_json::from_str(body).unwrap_or(serde_json::Value::Null),
        });
    }

    /// [`Self::answer_one`], then any [`Self::interleave`] it triggers.
    fn answer(&mut self, method: &str, path: &str, body: &str) -> Option<(u16, String)> {
        let answered = self.answer_one(method, path, body);
        if answered.is_some() {
            self.run_interleaved(method, path);
        }
        answered
    }

    /// Answer a request for a tracked object or collection, or `None` to fall
    /// through to the route table.
    fn answer_one(&mut self, method: &str, path: &str, body: &str) -> Option<(u16, String)> {
        if method.eq_ignore_ascii_case("POST") {
            let collection = self
                .collections
                .iter()
                .find(|c| path.ends_with(c.as_str()))?
                .clone();
            let mut object: serde_json::Value = serde_json::from_str(body).ok()?;
            let name = object
                .pointer("/metadata/name")
                .and_then(serde_json::Value::as_str)?
                .to_string();
            let key = format!("{collection}/{name}");
            if self.get(&key).is_some() {
                self.record(method, path, 409, body);
                return Some((409, already_exists_status(&name)));
            }
            self.next_uid += 1;
            object["metadata"]["uid"] = serde_json::json!(format!("store-uid-{}", self.next_uid));
            object["metadata"]["resourceVersion"] = serde_json::json!("1");
            self.objects.insert(key, Some(object.clone()));
            self.record(method, path, 201, body);
            return Some((201, object.to_string()));
        }
        let (key, status_subresource) = self.tracked(path)?;
        if method.eq_ignore_ascii_case("GET") {
            return Some(match self.get(&key) {
                Some(object) => (200, object.to_string()),
                None => (404, not_found_status(path)),
            });
        }
        if method.eq_ignore_ascii_case("DELETE") {
            let status = if self.remove(&key).is_some() {
                200
            } else {
                404
            };
            self.record(method, path, status, body);
            return Some((
                status,
                serde_json::json!({"kind": "Status", "status": "Success"}).to_string(),
            ));
        }
        if !(method.eq_ignore_ascii_case("PATCH") || method.eq_ignore_ascii_case("PUT")) {
            return None;
        }
        let Some(mut object) = self.get(&key) else {
            self.record(method, path, 404, body);
            return Some((404, not_found_status(path)));
        };
        let current = object
            .pointer("/metadata/resourceVersion")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("0")
            .to_string();
        if let Some(wanted) = precondition_of(body) {
            if wanted != current {
                self.record(method, path, 409, body);
                return Some((409, conflict_status(path)));
            }
        }
        let patch: serde_json::Value = serde_json::from_str(body).unwrap_or_default();
        if status_subresource {
            if let Some(status) = patch.get("status") {
                crate::conditions::apply_merge_patch(
                    object
                        .as_object_mut()
                        .expect("a stored object is a JSON object")
                        .entry("status")
                        .or_insert(serde_json::Value::Null),
                    status,
                );
            }
        } else if let Some(fields) = patch.as_object() {
            for (field, value) in fields {
                if field == "status" {
                    continue;
                }
                let mut value = value.clone();
                if let Some(meta) = value.as_object_mut().filter(|_| field == "metadata") {
                    meta.remove("resourceVersion");
                }
                crate::conditions::apply_merge_patch(
                    object
                        .as_object_mut()
                        .expect("a stored object is a JSON object")
                        .entry(field.clone())
                        .or_insert(serde_json::Value::Null),
                    &value,
                );
            }
        }
        object["metadata"]["resourceVersion"] = serde_json::json!(bumped(&current));
        self.objects.insert(key, Some(object.clone()));
        self.record(method, path, 200, body);
        Some((200, object.to_string()))
    }
}

fn not_found_status(path: &str) -> String {
    serde_json::json!({
        "kind": "Status", "apiVersion": "v1", "metadata": {}, "status": "Failure",
        "message": format!("{path} not found"), "reason": "NotFound", "details": {}, "code": 404
    })
    .to_string()
}

fn already_exists_status(name: &str) -> String {
    serde_json::json!({
        "kind": "Status", "apiVersion": "v1", "metadata": {}, "status": "Failure",
        "message": format!("{name} already exists"), "reason": "AlreadyExists",
        "details": {"name": name}, "code": 409
    })
    .to_string()
}

/// A [`kube::Client`] over `store` first and `routes` second, plus both
/// recorders. See [`ObjectStore`].
///
/// # Panics
///
/// As [`mock_client_recording`]: this builds a `tower::buffer::Buffer` and must
/// be called from inside a tokio runtime.
#[must_use]
pub fn mock_client_with_store(
    routes: Vec<Route>,
    store: SharedStore,
) -> (kube::Client, Recorder, BodyRecorder) {
    let recorder = recorder();
    let bodies = body_recorder();
    let table = Arc::new(routes);
    let ledger = RvLedger::default();
    let svc = {
        let recorder = Arc::clone(&recorder);
        let bodies = Arc::clone(&bodies);
        service_fn(move |req: Request<Body>| {
            let recorder = Arc::clone(&recorder);
            let bodies = Arc::clone(&bodies);
            let table = Arc::clone(&table);
            let ledger = Arc::clone(&ledger);
            let store = Arc::clone(&store);
            async move {
                let method = req.method().to_string();
                let uri = req.uri().to_string();
                let body = collect_body(req).await;
                bodies
                    .lock()
                    .expect("the body recorder mutex is never held across a panic")
                    .push(SeenBody {
                        method: method.clone(),
                        uri: uri.clone(),
                        body: body.clone(),
                    });
                let path = uri.split('?').next().unwrap_or(&uri).to_string();
                let stored = store
                    .lock()
                    .expect("the store mutex is never held across a panic")
                    .answer(&method, &path, &body);
                if let Some((status, response)) = stored {
                    recorder
                        .lock()
                        .expect("the recorder mutex is never held across a panic")
                        .push(SeenRequest { method, uri });
                    return Ok::<_, std::convert::Infallible>(respond(status, response));
                }
                Ok::<_, std::convert::Infallible>(answer_enforcing(
                    &table, &recorder, &ledger, &method, &uri, &body,
                ))
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
