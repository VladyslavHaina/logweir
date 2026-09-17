//! Test support: a strict, stateful fake Kubernetes API and an in-process
//! harness around the real router.
//!
//! THE FAKE IS STRICT. Like `weirkeeper::testing::mock_client`, it speaks
//! HTTP to a real `kube::Client` through a tower service (no socket) and
//! RECORDS every request. Unlike that double it keeps state — objects,
//! resourceVersions, UIDs — so create/replay/restart sequences run against
//! one store. It answers exactly the calls the adapter is allowed to make
//! (`/version`, and list/get/create/merge-patch on the five product plurals
//! under `logweir.dev/v1alpha1`); ANY other request is recorded in
//! `unexpected()` and answered 500, and every test asserts that list is
//! empty. A 404 is never used for "not in my table": 404 is a real answer.
//!
//! THE STORE SURVIVES THE ROUTER. `FakeKube` is `Clone` over shared state,
//! so a second `TestApp` built on the same fake is a restarted API process
//! looking at the same cluster.

#![allow(dead_code)]

pub mod idp;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::Body;
use axum::Router;
use chrono::{DateTime, Utc};
use http::{Request, Response, StatusCode};
use http_body_util::BodyExt as _;
use logweir_api::app::{AppState, Clock, Settings, SharedMode};
use logweir_api::auth::{Authenticator, LocalAdminAuthenticator};
use logweir_api::authz::{Authorizer, LocalAdminAuthorizer};
use logweir_api::cursor::CursorKey;
use logweir_api::kube::KubeAdapter;
use serde_json::{json, Value};
use tower::ServiceExt as _;

pub const ORIGIN: &str = "http://127.0.0.1:8484";
pub const HOST: &str = "127.0.0.1:8484";
/// The shared-mode origin, host and issuer used by every identity test.
pub const SHARED_ORIGIN: &str = "https://console.test";
pub const SHARED_HOST: &str = "console.test";
pub const ISSUER: &str = "https://idp.test/realms/logweir";
pub const CLIENT_ID: &str = "logweir-console";
pub const REDIRECT_URI: &str = "https://console.test/auth/callback";
pub const NS_A: &str = "team-a";
pub const NS_B: &str = "team-b";
pub const PLURALS: [&str; 8] = [
    "kafkaclusters",
    "backupschedules",
    "backups",
    "restores",
    "approvals",
    "backupdestinations",
    "topicdiscoveries",
    "preflights",
];
const PREFIX: &str = "/apis/logweir.dev/v1alpha1/namespaces/";
/// The core group's namespaced path. TWO OBJECTS ONLY, each with ONE verb:
/// `configmaps` GET (a check's own stored result) and `secrets` POST (a
/// write-only credential). Anything else under it is `unexpected`.
const CORE_PREFIX: &str = "/api/v1/namespaces/";
pub const CORE_PLURALS: [&str; 2] = ["configmaps", "secrets"];

/// One request the fake received.
#[derive(Clone, Debug)]
pub struct Recorded {
    pub method: String,
    pub path: String,
    pub query: String,
    pub headers: Vec<(String, String)>,
    pub body: String,
}

/// A response to force for matching requests.
#[derive(Clone, Debug)]
pub struct Fault {
    pub method: &'static str,
    pub path_contains: String,
    pub status: u16,
    pub reason: &'static str,
    pub delay: Option<Duration>,
    pub remaining: usize,
}

#[derive(Default)]
struct State {
    objects: BTreeMap<(String, String, String), Value>,
    next_rv: u64,
    next_uid: u64,
    requests: Vec<Recorded>,
    unexpected: Vec<String>,
    faults: Vec<Fault>,
    slow: Vec<Slow>,
    expire_continue_tokens: bool,
}

/// A delay applied to the NORMAL answer, after it has taken effect.
///
/// `Fault` short-circuits: it returns its status instead of doing the work, so
/// a delayed `Fault` on a POST never creates anything. That makes the one case
/// D0 asks about untestable — the write Kubernetes ACCEPTED whose response the
/// client never read. This delays the real answer instead, so the object is
/// stored and the response is still in flight.
#[derive(Clone, Debug)]
pub struct Slow {
    pub method: &'static str,
    pub path_contains: String,
    pub delay: Duration,
    pub remaining: usize,
}

/// The fake API server.
#[derive(Clone, Default)]
pub struct FakeKube {
    state: Arc<Mutex<State>>,
}

fn status_body(code: u16, reason: &str, message: &str) -> String {
    json!({
        "kind": "Status",
        "apiVersion": "v1",
        "metadata": {},
        "status": "Failure",
        "message": message,
        "reason": reason,
        "code": code,
    })
    .to_string()
}

fn kind_of(plural: &str) -> &'static str {
    match plural {
        "kafkaclusters" => "KafkaCluster",
        "backupschedules" => "BackupSchedule",
        "backups" => "Backup",
        "restores" => "Restore",
        "approvals" => "Approval",
        "backupdestinations" => "BackupDestination",
        "topicdiscoveries" => "TopicDiscovery",
        "preflights" => "Preflight",
        "configmaps" => "ConfigMap",
        "secrets" => "Secret",
        _ => "Unknown",
    }
}

fn merge(target: &mut Value, patch: &Value) {
    match (target, patch) {
        (Value::Object(t), Value::Object(p)) => {
            for (k, v) in p {
                if v.is_null() {
                    t.remove(k);
                } else {
                    merge(t.entry(k.clone()).or_insert(Value::Null), v);
                }
            }
        }
        (t, p) => *t = p.clone(),
    }
}

fn labels_match(object: &Value, selector: &str) -> bool {
    selector.split(',').filter(|t| !t.is_empty()).all(|term| {
        let (k, v) = term.split_once('=').unwrap_or((term, ""));
        object
            .pointer("/metadata/labels")
            .and_then(|l| l.get(k))
            .and_then(Value::as_str)
            == Some(v)
    })
}

impl FakeKube {
    pub fn new() -> Self {
        let fake = Self::default();
        fake.state.lock().unwrap().next_rv = 1000;
        fake
    }

    /// A kube client over this fake.
    pub fn client(&self) -> kube::Client {
        let state = Arc::clone(&self.state);
        let svc = tower::service_fn(move |req: Request<kube::client::Body>| {
            let state = Arc::clone(&state);
            async move {
                let method = req.method().to_string();
                let path = req.uri().path().to_string();
                let query = req.uri().query().unwrap_or("").to_string();
                let headers = req
                    .headers()
                    .iter()
                    .map(|(k, v)| (k.as_str().to_string(), v.to_str().unwrap_or("").to_string()))
                    .collect::<Vec<_>>();
                let body = req
                    .into_body()
                    .collect()
                    .await
                    .map(|c| String::from_utf8_lossy(&c.to_bytes()).into_owned())
                    .unwrap_or_default();
                let recorded = Recorded {
                    method,
                    path,
                    query,
                    headers,
                    body,
                };
                let (delay, status, text) = answer(&state, recorded);
                if let Some(delay) = delay {
                    tokio::time::sleep(delay).await;
                }
                Ok::<_, std::convert::Infallible>(
                    Response::builder()
                        .status(status)
                        .header("content-type", "application/json")
                        .body(kube::client::Body::from(text.into_bytes()))
                        .unwrap(),
                )
            }
        });
        kube::Client::new(svc, "default")
    }

    /// Store an object as if it already existed in the cluster.
    pub fn seed(&self, plural: &str, namespace: &str, mut object: Value) -> Value {
        let mut s = self.state.lock().unwrap();
        s.next_rv += 1;
        s.next_uid += 1;
        let name = object["metadata"]["name"]
            .as_str()
            .expect("seeded objects have a name")
            .to_string();
        let meta = object["metadata"].as_object_mut().unwrap();
        meta.insert("namespace".into(), json!(namespace));
        meta.entry("uid")
            .or_insert(json!(format!("seed-uid-{}", s.next_uid)));
        meta.insert("resourceVersion".into(), json!(s.next_rv.to_string()));
        meta.entry("creationTimestamp")
            .or_insert(json!("2026-09-15T10:00:00Z"));
        object["apiVersion"] = json!(if CORE_PLURALS.contains(&plural) {
            "v1"
        } else {
            "logweir.dev/v1alpha1"
        });
        object["kind"] = json!(kind_of(plural));
        s.objects.insert(
            (plural.to_string(), namespace.to_string(), name),
            object.clone(),
        );
        object
    }

    pub fn object(&self, plural: &str, namespace: &str, name: &str) -> Option<Value> {
        self.state
            .lock()
            .unwrap()
            .objects
            .get(&(plural.into(), namespace.into(), name.into()))
            .cloned()
    }

    pub fn count(&self, plural: &str, namespace: &str) -> usize {
        self.state
            .lock()
            .unwrap()
            .objects
            .keys()
            .filter(|(p, n, _)| p == plural && n == namespace)
            .count()
    }

    pub fn requests(&self) -> Vec<Recorded> {
        self.state.lock().unwrap().requests.clone()
    }

    pub fn clear_requests(&self) {
        self.state.lock().unwrap().requests.clear();
    }

    pub fn unexpected(&self) -> Vec<String> {
        self.state.lock().unwrap().unexpected.clone()
    }

    pub fn inject(&self, fault: Fault) {
        self.state.lock().unwrap().faults.push(fault);
    }

    /// Delay the next matching request's real answer. See [`Slow`].
    pub fn slow_next(&self, method: &'static str, path_contains: &str, delay: Duration) {
        self.state.lock().unwrap().slow.push(Slow {
            method,
            path_contains: path_contains.to_string(),
            delay,
            remaining: 1,
        });
    }

    pub fn expire_continue_tokens(&self) {
        self.state.lock().unwrap().expire_continue_tokens = true;
    }

    /// Assert the adapter asked for nothing outside the allowed surface.
    pub fn assert_strict(&self) {
        let unexpected = self.unexpected();
        assert!(
            unexpected.is_empty(),
            "unexpected Kubernetes requests: {unexpected:#?}"
        );
        for r in self.requests() {
            for (name, _) in &r.headers {
                assert!(
                    !name.starts_with("impersonate-"),
                    "an outbound Kubernetes request carried {name}"
                );
            }
        }
    }
}

/// The kinds whose spec this fake will accept a merge patch for.
pub const PATCHABLE: [&str; 4] = [
    "backupschedules",
    "backupdestinations",
    "topicdiscoveries",
    "preflights",
];

/// The core group: `configmaps` GET and `secrets` POST, and nothing else.
///
/// THE SECRET ECHO IS REAL. A create answers with `data` echoed exactly as the
/// API server does, so the write-only guarantee is exercised rather than
/// assumed: if the adapter's type could deserialize `data`, the value would be
/// in the process and a projection could leak it.
fn core_answer(s: &mut State, recorded: &Recorded, rest: &str) -> (Option<Duration>, u16, String) {
    let parts: Vec<&str> = rest.split('/').collect();
    let (namespace, plural, name) = match parts.as_slice() {
        [ns, plural] => (ns.to_string(), plural.to_string(), None),
        [ns, plural, name] => (ns.to_string(), plural.to_string(), Some(name.to_string())),
        _ => {
            s.unexpected
                .push(format!("{} {}", recorded.method, recorded.path));
            return (
                None,
                500,
                status_body(500, "InternalError", "not allowed by the fake"),
            );
        }
    };
    if !CORE_PLURALS.contains(&plural.as_str()) {
        s.unexpected
            .push(format!("{} {}", recorded.method, recorded.path));
        return (
            None,
            500,
            status_body(500, "InternalError", "not allowed by the fake"),
        );
    }
    match (recorded.method.as_str(), plural.as_str(), name) {
        ("GET", "configmaps", Some(name)) => {
            match s
                .objects
                .get(&("configmaps".to_string(), namespace.clone(), name.clone()))
            {
                Some(v) => (None, 200, v.to_string()),
                None => (
                    None,
                    404,
                    status_body(404, "NotFound", &format!("configmaps \"{name}\" not found")),
                ),
            }
        }
        ("POST", "secrets", None) => {
            let Ok(mut object) = serde_json::from_str::<Value>(&recorded.body) else {
                return (
                    None,
                    400,
                    status_body(400, "BadRequest", "body is not JSON"),
                );
            };
            let Some(name) = object
                .pointer("/metadata/name")
                .and_then(Value::as_str)
                .map(str::to_string)
            else {
                return (
                    None,
                    422,
                    status_body(422, "Invalid", "metadata.name: Required value"),
                );
            };
            let key = ("secrets".to_string(), namespace.clone(), name.clone());
            if s.objects.contains_key(&key) {
                return (
                    None,
                    409,
                    status_body(
                        409,
                        "AlreadyExists",
                        &format!("secrets \"{name}\" already exists"),
                    ),
                );
            }
            // `dryRun=All` RUNS THE CONFLICT CHECK AND DISCARDS THE WRITE,
            // exactly as the API server does. The conflict arm above is
            // deliberately BEFORE this one, so a dry run against a taken name
            // still answers `AlreadyExists` — which is the whole point of the
            // probe.
            let dry_run = serde_urlencoded::from_str::<BTreeMap<String, String>>(&recorded.query)
                .unwrap_or_default()
                .get("dryRun")
                .is_some_and(|v| v == "All");
            if dry_run {
                let meta = object["metadata"].as_object_mut().unwrap();
                meta.insert("namespace".into(), json!(namespace));
                meta.insert("uid".into(), json!("00000000-0000-4000-9000-dryrun000000"));
                object["apiVersion"] = json!("v1");
                object["kind"] = json!("Secret");
                return (None, 201, object.to_string());
            }
            s.next_rv += 1;
            s.next_uid += 1;
            let uid = format!("00000000-0000-4000-9000-{:012}", s.next_uid);
            let rv = s.next_rv.to_string();
            let meta = object["metadata"].as_object_mut().unwrap();
            meta.insert("namespace".into(), json!(namespace));
            meta.insert("uid".into(), json!(uid));
            meta.insert("resourceVersion".into(), json!(rv));
            meta.insert("creationTimestamp".into(), json!("2026-09-15T12:00:00Z"));
            object["apiVersion"] = json!("v1");
            object["kind"] = json!("Secret");
            s.objects.insert(key, object.clone());
            (None, 201, object.to_string())
        }
        _ => {
            s.unexpected
                .push(format!("{} {}", recorded.method, recorded.path));
            (
                None,
                500,
                status_body(500, "InternalError", "not allowed by the fake"),
            )
        }
    }
}

fn answer(state: &Arc<Mutex<State>>, recorded: Recorded) -> (Option<Duration>, u16, String) {
    // Taken BEFORE the answer and applied AFTER it, so the work really happens
    // and only the response is late. The lock is released before `answer_inner`
    // takes it again.
    let slow = {
        let mut s = state.lock().unwrap();
        s.slow
            .iter()
            .position(|e| {
                e.remaining > 0
                    && e.method.eq_ignore_ascii_case(&recorded.method)
                    && recorded.path.contains(&e.path_contains)
            })
            .map(|i| {
                s.slow[i].remaining -= 1;
                s.slow[i].delay
            })
    };
    let (delay, status, body) = answer_inner(state, recorded);
    (slow.or(delay), status, body)
}

fn answer_inner(state: &Arc<Mutex<State>>, recorded: Recorded) -> (Option<Duration>, u16, String) {
    let mut s = state.lock().unwrap();
    s.requests.push(recorded.clone());

    if let Some(i) = s.faults.iter().position(|f| {
        f.remaining > 0
            && f.method.eq_ignore_ascii_case(&recorded.method)
            && recorded.path.contains(&f.path_contains)
    }) {
        s.faults[i].remaining -= 1;
        let f = s.faults[i].clone();
        return (
            f.delay,
            f.status,
            status_body(f.status, f.reason, "an injected failure carrying a secret-looking token eyJhbGciOiJIUzI1NiJ9abcdefghijklmnop"),
        );
    }

    if recorded.method == "GET" && recorded.path == "/version" {
        return (None, 200, json!({"major": "1", "minor": "29", "gitVersion": "v1.29.0", "platform": "linux/amd64"}).to_string());
    }

    if let Some(rest) = recorded.path.strip_prefix(CORE_PREFIX) {
        return core_answer(&mut s, &recorded, rest);
    }
    let Some(rest) = recorded.path.strip_prefix(PREFIX) else {
        s.unexpected
            .push(format!("{} {}", recorded.method, recorded.path));
        return (
            None,
            500,
            status_body(500, "InternalError", "not allowed by the fake"),
        );
    };
    let parts: Vec<&str> = rest.split('/').collect();
    let (namespace, plural, name) = match parts.as_slice() {
        [ns, plural] => (ns.to_string(), plural.to_string(), None),
        [ns, plural, name] => (ns.to_string(), plural.to_string(), Some(name.to_string())),
        _ => {
            s.unexpected
                .push(format!("{} {}", recorded.method, recorded.path));
            return (
                None,
                500,
                status_body(500, "InternalError", "not allowed by the fake"),
            );
        }
    };
    if !PLURALS.contains(&plural.as_str()) {
        s.unexpected
            .push(format!("{} {}", recorded.method, recorded.path));
        return (
            None,
            500,
            status_body(500, "InternalError", "not allowed by the fake"),
        );
    }
    let query: BTreeMap<String, String> =
        serde_urlencoded::from_str(&recorded.query).unwrap_or_default();

    match (recorded.method.as_str(), name) {
        ("GET", None) => {
            if query.contains_key("continue") && s.expire_continue_tokens {
                return (
                    None,
                    410,
                    status_body(410, "Expired", "the continue token is too old"),
                );
            }
            let limit: usize = query
                .get("limit")
                .and_then(|l| l.parse().ok())
                .unwrap_or(usize::MAX);
            let after = query
                .get("continue")
                .map(|c| c.strip_prefix("fake-continue:").unwrap_or("").to_string());
            let selector = query.get("labelSelector").cloned().unwrap_or_default();
            let mut items: Vec<Value> = s
                .objects
                .iter()
                .filter(|((p, n, _), _)| p == &plural && n == &namespace)
                .filter(|((_, _, name), _)| {
                    after.as_ref().is_none_or(|a| name.as_str() > a.as_str())
                })
                .map(|(_, v)| v.clone())
                .filter(|v| labels_match(v, &selector))
                .collect();
            let more = items.len() > limit;
            items.truncate(limit);
            let mut metadata = json!({"resourceVersion": s.next_rv.to_string()});
            if more {
                let last = items
                    .last()
                    .and_then(|v| v["metadata"]["name"].as_str())
                    .unwrap_or("");
                metadata["continue"] = json!(format!("fake-continue:{last}"));
            }
            (
                None,
                200,
                json!({
                    "apiVersion": "logweir.dev/v1alpha1",
                    "kind": format!("{}List", kind_of(&plural)),
                    "metadata": metadata,
                    "items": items,
                })
                .to_string(),
            )
        }
        ("GET", Some(name)) => {
            match s
                .objects
                .get(&(plural.clone(), namespace.clone(), name.clone()))
            {
                Some(v) => (None, 200, v.to_string()),
                None => (
                    None,
                    404,
                    status_body(404, "NotFound", &format!("{plural} \"{name}\" not found")),
                ),
            }
        }
        ("POST", None) => {
            let Ok(mut object) = serde_json::from_str::<Value>(&recorded.body) else {
                return (
                    None,
                    400,
                    status_body(400, "BadRequest", "body is not JSON"),
                );
            };
            let Some(name) = object
                .pointer("/metadata/name")
                .and_then(Value::as_str)
                .map(str::to_string)
            else {
                return (
                    None,
                    422,
                    status_body(422, "Invalid", "metadata.name: Required value"),
                );
            };
            let key = (plural.clone(), namespace.clone(), name.clone());
            if s.objects.contains_key(&key) {
                return (
                    None,
                    409,
                    status_body(
                        409,
                        "AlreadyExists",
                        &format!("{plural} \"{name}\" already exists"),
                    ),
                );
            }
            s.next_rv += 1;
            s.next_uid += 1;
            let uid = format!("00000000-0000-4000-8000-{:012}", s.next_uid);
            let rv = s.next_rv.to_string();
            let meta = object["metadata"].as_object_mut().unwrap();
            meta.insert("namespace".into(), json!(namespace));
            meta.insert("uid".into(), json!(uid));
            meta.insert("resourceVersion".into(), json!(rv));
            meta.insert("creationTimestamp".into(), json!("2026-09-15T12:00:00Z"));
            meta.insert("generation".into(), json!(1));
            s.objects.insert(key, object.clone());
            (None, 201, object.to_string())
        }
        ("PATCH", Some(name)) if PATCHABLE.contains(&plural.as_str()) => {
            let content_type = recorded
                .headers
                .iter()
                .find(|(k, _)| k == "content-type")
                .map(|(_, v)| v.as_str())
                .unwrap_or("");
            if content_type != "application/merge-patch+json" {
                s.unexpected
                    .push(format!("PATCH with content-type {content_type}"));
                return (
                    None,
                    415,
                    status_body(415, "UnsupportedMediaType", "merge patches only"),
                );
            }
            let Ok(patch) = serde_json::from_str::<Value>(&recorded.body) else {
                return (
                    None,
                    400,
                    status_body(400, "BadRequest", "body is not JSON"),
                );
            };
            // THE ALLOWED PATCH SURFACE, PER KIND. `metadata` may carry only
            // `resourceVersion`; `spec` may carry only the fields that kind's
            // CEL leaves mutable. A patch outside it is `unexpected`, so a
            // route that learned to write something else fails every test.
            let allowed_spec: &[&str] = match plural.as_str() {
                // D1 §5.1's mutability matrix. `sourceRef` is NOT here: no
                // route may build a patch that names it, and a route that
                // learned to would fail every test in this crate.
                "backupschedules" => &[
                    "suspend",
                    "schedule",
                    "timeZone",
                    "topics",
                    "allUserTopics",
                    "archive",
                    "destinationRef",
                    "concurrencyPolicy",
                    "startingDeadlineSeconds",
                    "catchUpPolicy",
                    "retry",
                    "activeDeadlineSeconds",
                    "retention",
                ],
                "backupdestinations" => &["access", "transport"],
                "topicdiscoveries" | "preflights" => &["cancelRequested"],
                _ => &[],
            };
            let allowed = patch.as_object().is_some_and(|o| {
                o.keys().all(|k| k == "metadata" || k == "spec")
                    && o.get("metadata").is_none_or(|m| {
                        m.as_object()
                            .is_some_and(|m| m.keys().all(|k| k == "resourceVersion"))
                    })
                    && o.get("spec").is_none_or(|sp| {
                        sp.as_object()
                            .is_some_and(|sp| sp.keys().all(|k| allowed_spec.contains(&k.as_str())))
                    })
            });
            // A `transport` patch may name `caBundle` and nothing else:
            // `security` is immutable, and a fake that accepted it would let a
            // transport downgrade pass every test in this crate.
            let transport_ok = patch.pointer("/spec/transport").is_none_or(|v| {
                v.as_object()
                    .is_some_and(|o| o.keys().all(|k| k == "caBundle"))
            });
            if !allowed || !transport_ok {
                s.unexpected.push(format!(
                    "PATCH outside the mutable surface: {}",
                    recorded.body
                ));
                return (
                    None,
                    422,
                    status_body(422, "Invalid", "only the declared fields are mutable"),
                );
            }
            let key = (plural.clone(), namespace.clone(), name.clone());
            let Some(current) = s.objects.get(&key).cloned() else {
                return (None, 404, status_body(404, "NotFound", "not found"));
            };
            if let Some(expected) = patch
                .pointer("/metadata/resourceVersion")
                .and_then(Value::as_str)
            {
                if current["metadata"]["resourceVersion"].as_str() != Some(expected) {
                    return (
                        None,
                        409,
                        status_body(
                            409,
                            "Conflict",
                            "Operation cannot be fulfilled: the object has been modified",
                        ),
                    );
                }
            }
            let mut updated = current;
            let mut body = patch.clone();
            body.as_object_mut().unwrap().remove("metadata");
            let spec_changed = body.get("spec").is_some();
            merge(&mut updated, &body);
            // THE CRD'S OWN CEL, ON THE MERGED RESULT, LIKE A REAL API SERVER.
            // D1 §5.2 says the API must NOT keep a copy of R1-R3 and pre-empt
            // them; the API server refuses and the API maps the refusal. A
            // fake that accepted these shapes would let that mapping go
            // untested and would let a route ship a second, drifting copy of
            // the rules.
            if plural == "backupschedules" {
                if let Some(message) = refused_by_schedule_cel(&updated) {
                    return (None, 422, status_body(422, "Invalid", &message));
                }
            }
            s.next_rv += 1;
            updated["metadata"]["resourceVersion"] = json!(s.next_rv.to_string());
            if spec_changed {
                let generation = updated["metadata"]["generation"].as_i64().unwrap_or(1);
                updated["metadata"]["generation"] = json!(generation + 1);
            }
            s.objects.insert(key, updated.clone());
            (None, 200, updated.to_string())
        }
        _ => {
            s.unexpected
                .push(format!("{} {}", recorded.method, recorded.path));
            (
                None,
                500,
                status_body(500, "InternalError", "not allowed by the fake"),
            )
        }
    }
}

/// The `BackupSchedule` CEL rule a merged object breaks, if any (D1 §5.2).
///
/// R1 is not here: no adapter method can build a `sourceRef` key, so the
/// transition it would refuse is unreachable rather than merely refused.
fn refused_by_schedule_cel(object: &Value) -> Option<String> {
    let spec = object.get("spec")?;
    let topics = spec
        .get("topics")
        .and_then(Value::as_array)
        .map_or(0, Vec::len);
    if spec.get("allUserTopics").is_some() && topics > 0 {
        return Some(format!(
            "BackupSchedule.logweir.dev \"x\" is invalid: spec: Invalid value: \"object\": {}",
            weirkeeper::crds::backup_schedule::SELECTION_SHAPE_MESSAGE
        ));
    }
    let name = object
        .pointer("/metadata/name")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let max_retries = spec
        .pointer("/retry/maxRetries")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    if max_retries > 0 && name.len() > 29 {
        return Some(format!(
            "BackupSchedule.logweir.dev \"{name}\" is invalid: <nil>: Invalid value: \"object\": {}",
            weirkeeper::crds::backup_schedule::RETRY_NAME_BUDGET_MESSAGE
        ));
    }
    let destination = spec.pointer("/destinationRef/name").and_then(Value::as_str);
    let url = spec
        .pointer("/archive/url")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let has_secret = spec.pointer("/archive/secretRef").is_some();
    let sentinel_ok = match destination {
        Some(name) => {
            url == format!(
                "{}{name}",
                weirkeeper::crds::backup_destination::DESTINATION_URL_SCHEME
            ) && !has_secret
        }
        None => !url.starts_with(weirkeeper::crds::backup_destination::DESTINATION_URL_SCHEME),
    };
    if !sentinel_ok {
        return Some(format!(
            "BackupSchedule.logweir.dev \"{name}\" is invalid: spec: Invalid value: \"object\": {}",
            weirkeeper::crds::backup_schedule::DESTINATION_SENTINEL_MESSAGE
        ));
    }
    None
}

/// A settable clock.
pub struct TestClock(Mutex<DateTime<Utc>>);

impl TestClock {
    pub fn new() -> Arc<Self> {
        Arc::new(Self(Mutex::new(
            DateTime::parse_from_rfc3339("2026-09-15T12:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
        )))
    }

    pub fn advance(&self, seconds: i64) {
        let mut t = self.0.lock().unwrap();
        *t += chrono::Duration::seconds(seconds);
    }
}

impl Clock for TestClock {
    fn now(&self) -> DateTime<Utc> {
        *self.0.lock().unwrap()
    }
}

/// Harness options.
pub struct Options {
    pub namespaces: Vec<String>,
    pub authenticator: Option<Arc<dyn Authenticator>>,
    pub authorizer: Option<Arc<dyn Authorizer>>,
    pub deadline: Duration,
    pub ui_dir: PathBuf,
    pub cursor_key: Vec<u8>,
    pub public_origin: String,
    pub allowed_hosts: Vec<String>,
    pub shared: Option<Arc<SharedMode>>,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            namespaces: vec![NS_A.to_string(), NS_B.to_string()],
            authenticator: None,
            authorizer: None,
            deadline: Duration::from_secs(10),
            ui_dir: repo_root().join("ui"),
            cursor_key: vec![0x5a; 32],
            public_origin: ORIGIN.to_string(),
            allowed_hosts: vec![
                "127.0.0.1:8484".into(),
                "localhost:8484".into(),
                "[::1]:8484".into(),
            ],
            shared: None,
        }
    }
}

pub fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crates/logweir-api is two levels under the workspace root")
        .to_path_buf()
}

/// The router plus its fake.
pub struct TestApp {
    pub router: Router,
    pub fake: FakeKube,
    pub clock: Arc<TestClock>,
}

/// A buffered response.
pub struct TestResponse {
    pub status: StatusCode,
    pub headers: http::HeaderMap,
    pub body: Vec<u8>,
}

impl TestResponse {
    pub fn json(&self) -> Value {
        serde_json::from_slice(&self.body).unwrap_or_else(|e| {
            panic!(
                "body is not JSON ({e}): {}",
                String::from_utf8_lossy(&self.body)
            )
        })
    }

    /// The whole body as text, for the leak assertions.
    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }

    pub fn code(&self) -> String {
        let v = self.json();
        v["code"].as_str().unwrap_or("").to_string()
    }

    pub fn header(&self, name: &str) -> Option<String> {
        self.headers
            .get(name)
            .map(|v| v.to_str().unwrap().to_string())
    }

    /// Assert a problem response with this code and status.
    pub fn assert_problem(&self, status: u16, code: &str) {
        assert_eq!(
            (self.status.as_u16(), self.code().as_str()),
            (status, code),
            "body: {}",
            String::from_utf8_lossy(&self.body)
        );
        assert_eq!(
            self.header("content-type").as_deref(),
            Some("application/problem+json")
        );
        let v = self.json();
        assert_eq!(v["status"], status);
        assert_eq!(
            v["requestId"].as_str(),
            self.header("x-request-id").as_deref()
        );
        assert!(v["type"]
            .as_str()
            .unwrap()
            .starts_with("https://logweir.dev/problems/"));
        assert!(v["title"].is_string() && v["detail"].is_string() && v["retryable"].is_boolean());
    }
}

impl TestApp {
    pub fn new() -> Self {
        Self::with(FakeKube::new(), Options::default())
    }

    pub fn with(fake: FakeKube, options: Options) -> Self {
        let clock = TestClock::new();
        Self::with_clock(fake, options, clock)
    }

    pub fn with_clock(fake: FakeKube, options: Options, clock: Arc<TestClock>) -> Self {
        let authenticator = options.authenticator.unwrap_or_else(|| {
            Arc::new(LocalAdminAuthenticator::new("admin", "Local administrator"))
        });
        let authorizer = options
            .authorizer
            .unwrap_or_else(|| Arc::new(LocalAdminAuthorizer::new(options.namespaces.clone())));
        let state = AppState::new(Settings {
            authenticator,
            authorizer,
            kube: KubeAdapter::with_deadline(fake.client(), options.deadline),
            cursor_key: CursorKey::new(options.cursor_key),
            clock: clock.clone() as Arc<dyn Clock>,
            public_origin: options.public_origin.clone(),
            allowed_hosts: options.allowed_hosts.clone(),
            assets: logweir_api::assets::StaticAssets::load(&options.ui_dir)
                .expect("the UI directory loads"),
            readiness_namespace: options.namespaces[0].clone(),
            shared: options.shared.clone(),
        });
        Self {
            router: logweir_api::app::router(state),
            fake,
            clock,
        }
    }

    /// A restarted process over the same cluster and clock.
    pub fn restart(&self) -> Self {
        Self::with_clock(self.fake.clone(), Options::default(), self.clock.clone())
    }

    pub async fn send(&self, request: Request<Body>) -> TestResponse {
        let response = self
            .router
            .clone()
            .oneshot(request)
            .await
            .expect("the router is infallible");
        let status = response.status();
        let headers = response.headers().clone();
        let body = response
            .into_body()
            .collect()
            .await
            .expect("the body collects")
            .to_bytes()
            .to_vec();
        TestResponse {
            status,
            headers,
            body,
        }
    }

    pub async fn get(&self, path: &str) -> TestResponse {
        self.send(
            Request::builder()
                .method("GET")
                .uri(path)
                .header("host", HOST)
                .body(Body::empty())
                .unwrap(),
        )
        .await
    }

    pub async fn put(&self, path: &str, body: &str) -> TestResponse {
        self.send(
            Request::builder()
                .method("PUT")
                .uri(path)
                .header("host", HOST)
                .header("origin", ORIGIN)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
    }

    pub async fn post(&self, path: &str, key: Option<&str>, body: &str) -> TestResponse {
        let mut builder = Request::builder()
            .method("POST")
            .uri(path)
            .header("host", HOST)
            .header("origin", ORIGIN)
            .header("content-type", "application/json");
        if let Some(key) = key {
            builder = builder.header("idempotency-key", key);
        }
        self.send(builder.body(Body::from(body.to_string())).unwrap())
            .await
    }
}

// ---------------------------------------------------------------- fixtures

pub fn connection_body() -> Value {
    json!({
        "role": "source",
        "bootstrapServers": ["kafka-source.kafka.svc.cluster.local:9096"],
        "auth": {
            "mode": "scramSha512",
            "username": "scram-user",
            "credentialRef": {"name": "source-scram"},
            "tls": false
        }
    })
}

pub fn schedule_body() -> Value {
    json!({
        "schedule": "0 3 * * *",
        "sourceRef": {"name": "source"},
        "topics": ["orders", "payments"],
        "archive": {"url": "s3://kafka-backups/orders", "credentialRef": {"name": "archive-credentials"}},
        "suspended": true
    })
}

pub fn golden_plan() -> String {
    std::fs::read_to_string(repo_root().join("ui/tests/fixtures/plan.golden.yaml"))
        .expect("the plan golden exists")
}

pub fn restore_body(plan: &str) -> Value {
    json!({
        "planBytes": plan,
        "planHash": logweir_core::ids::sha256_prefixed(plan.as_bytes()),
        "approvalRef": {"name": "approval-1234abcd"},
        "sourceArchive": {"url": "s3://kafka-backups/drill-demo", "credentialRef": {"name": "archive-credentials"}},
        "backupSetRef": "01JB7Z0000000000000000000B",
        "pointInTime": "2026-09-07T14:05:00Z",
        "target": {"clusterRef": {"name": "target"}, "mode": "newTopic", "topicNaming": {"prefix": "restore-20260907t140500z-"}},
        "deadlineSeconds": 3600
    })
}

pub fn fixture(name: &str) -> Value {
    let path = repo_root().join("ui/tests/fixtures").join(name);
    serde_json::from_str(
        &std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display())),
    )
    .expect("fixtures are JSON")
}

// ------------------------------------------------- D2 W12 fixtures and seeds

/// The local administrator's stable actor id, which is what the ownership
/// annotation on a check created through `TestApp` carries.
pub const LOCAL_ADMIN_ACTOR: &str = "urn:logweir:local-admin#admin";

/// The idempotency annotation naming the actor that created an object.
pub const ACTOR_ANNOTATION: &str = "api.logweir.dev/actor";

/// A credential value that must never appear in a response, a log or a stored
/// projection. Distinctive on purpose: every leak assertion greps for it.
pub const SECRET_ACCESS_KEY: &str = "sEcReT-aCcEsS-kEy-D2W12-NEVER-ECHOED";
/// The access key id entered beside it.
pub const ACCESS_KEY_ID: &str = "AKIAD2W12NEVERECHOED";

/// A `CreateDestinationRequest` naming an EXISTING Secret.
pub fn destination_body(name: &str) -> Value {
    json!({
        "name": name,
        "description": "Production archive (MinIO, private CA)",
        "storage": {
            "provider": "s3",
            "bucket": "kafka-backups",
            "prefix": "team-a/prod",
            "region": "us-east-1",
            "endpoint": "https://minio.storage.svc:9000",
            "addressing": "pathStyle"
        },
        "transport": {"security": "tls", "caBundle": {"configMapName": "minio-ca", "key": "ca.crt"}},
        "access": {
            "archiveWrite": {"mode": "secretKeys", "secret": {"existing": {"name": "logweir-s3"}}},
            "archiveRead": {"mode": "secretKeys", "secret": {"existing": {"name": "archive-reader"}}},
            "evidenceRead": {"mode": "archiveReadGrant"}
        },
        "readiness": {"writeProbe": "createOnlyMarker"}
    })
}

/// The same request, with the archive-read grant entered as a VALUE.
pub fn destination_body_with_new_credential(name: &str) -> Value {
    let mut body = destination_body(name);
    body["access"]["archiveRead"] = json!({
        "mode": "secretKeys",
        "secret": {"new": {"accessKeyId": ACCESS_KEY_ID, "secretAccessKey": SECRET_ACCESS_KEY}}
    });
    body
}

/// A stored `BackupDestination`, as the controller would leave it.
pub fn seed_destination(fake: &FakeKube, namespace: &str, name: &str) -> Value {
    fake.seed(
        "backupdestinations",
        namespace,
        json!({
            "metadata": {"name": name, "generation": 3},
            "spec": {
                "description": "seeded",
                "storage": {
                    "provider": "S3",
                    "bucket": "kafka-backups",
                    "prefix": "team-a/prod",
                    "region": "us-east-1",
                    "endpoint": "https://minio.storage.svc:9000",
                    "addressing": "PathStyle"
                },
                "transport": {"security": "TLS", "caBundle": {"configMapName": "minio-ca", "key": "ca.crt"}},
                "access": {
                    "archiveWrite": {"mode": "SecretKeys", "secret": {"name": "logweir-s3", "accessKeyIdKey": "access-key-id", "secretAccessKeyKey": "secret-access-key"}},
                    "archiveRead": {"mode": "SecretKeys", "secret": {"name": "archive-reader", "accessKeyIdKey": "access-key-id", "secretAccessKeyKey": "secret-access-key"}},
                    "evidenceRead": {"mode": "ArchiveReadGrant"}
                },
                "readiness": {"writeProbe": "CreateOnlyMarker"}
            },
            "status": {
                "observedGeneration": 3,
                "reason": "Valid",
                "canonicalUrl": "s3://kafka-backups/team-a/prod",
                "locationDigest": "sha256:0000000000000000000000000000000000000000000000000000000000000000",
                "caBundleSha256": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
                "conditions": [{"type": "Valid", "status": "True", "reason": "Valid"}]
            }
        }),
    )
}

/// One canonical TSV line.
pub fn topic_line(name: &str, partitions: u32, flags: &str) -> String {
    format!("{name}\t{partitions}\t{flags}\n")
}

/// A stored discovery with `chunks` chunks of stored TSV, and the immutable
/// owned `ConfigMap`s that hold them.
pub fn seed_discovery(
    fake: &FakeKube,
    namespace: &str,
    name: &str,
    connection: &str,
    actor: Option<&str>,
    chunks: &[Vec<String>],
) -> Value {
    let uid = format!("uid-{name}");
    let all: String = chunks.iter().flatten().cloned().collect();
    let mut index = Vec::new();
    for (i, lines) in chunks.iter().enumerate() {
        let data: String = lines.concat();
        let sha256 = logweir_core::ids::sha256_prefixed(data.as_bytes());
        let chunk_name = format!("lwc-{name}-r{i:03}");
        fake.seed(
            "configmaps",
            namespace,
            json!({
                "metadata": {
                    "name": chunk_name,
                    "annotations": {"logweir.dev/result-sha256": sha256, "logweir.dev/result-format": "logweir.dev/topic-inventory/v1"},
                    "ownerReferences": [{"apiVersion": "logweir.dev/v1alpha1", "kind": "TopicDiscovery", "name": name, "uid": uid, "controller": true, "blockOwnerDeletion": true}]
                },
                "immutable": true,
                "data": {"topics.tsv": data}
            }),
        );
        let first = lines
            .first()
            .and_then(|l| l.split('\t').next())
            .unwrap_or("")
            .to_string();
        let last = lines
            .last()
            .and_then(|l| l.split('\t').next())
            .unwrap_or("")
            .to_string();
        index.push(json!({
            "name": chunk_name,
            "sha256": sha256,
            "count": lines.len(),
            "firstName": first,
            "lastName": last
        }));
    }
    let mut metadata =
        json!({"name": name, "uid": uid, "labels": {"logweir.dev/connection": connection}});
    if let Some(actor) = actor {
        metadata["annotations"] = json!({ACTOR_ANNOTATION: actor});
    }
    fake.seed(
        "topicdiscoveries",
        namespace,
        json!({
            "metadata": metadata,
            "spec": {"request": {"connectionRef": {"name": connection}, "includeInternal": false, "maxTopics": 20000, "timeoutSeconds": 60}, "cancelRequested": false},
            "status": {
                "phase": "Succeeded",
                "reason": "Succeeded",
                "binding": {"connectionName": connection, "connectionUid": "seed-connection-uid", "connectionGeneration": 1, "principal": "User:scram-user", "authMode": "scramSha512"},
                "observedAt": "2026-09-15T11:55:00Z",
                "freshUntil": "2026-09-15T12:10:00Z",
                "result": {
                    "format": "logweir.dev/topic-inventory/v1",
                    "clusterId": "M29I2S7FQPyHBEX12Vx7XA",
                    "brokerCount": 1,
                    "counts": {"listed": all.lines().count() as i64, "returned": all.lines().count() as i64, "internalExcluded": 0, "errored": 0},
                    "truncated": false,
                    "visibility": {"state": "unknown", "basis": []},
                    "topicsSha256": logweir_core::ids::sha256_prefixed(all.as_bytes()),
                    "chunks": index
                },
                "conditions": [{"type": "Complete", "status": "True", "reason": "Succeeded"}]
            }
        }),
    )
}

/// A discovery that is still running, so a cancel has something to write.
pub fn seed_running_discovery(
    fake: &FakeKube,
    namespace: &str,
    name: &str,
    connection: &str,
    actor: &str,
) -> Value {
    fake.seed(
        "topicdiscoveries",
        namespace,
        json!({
            "metadata": {
                "name": name,
                "uid": format!("uid-{name}"),
                "labels": {"logweir.dev/connection": connection},
                "annotations": {ACTOR_ANNOTATION: actor}
            },
            "spec": {"request": {"connectionRef": {"name": connection}, "includeInternal": false, "maxTopics": 20000, "timeoutSeconds": 60}, "cancelRequested": false},
            "status": {"phase": "Running", "reason": "Running"}
        }),
    )
}

/// A completed preflight with one blocking check, one advisory warning, one
/// execution-only note and a details document.
pub fn seed_preflight(
    fake: &FakeKube,
    namespace: &str,
    name: &str,
    operation: &str,
    plan_hash: Option<&str>,
    actor: Option<&str>,
) -> Value {
    let uid = format!("uid-{name}");
    let details = "{\"check\":\"target.mappedTopics\",\"topic\":\"restore-orders\"}\n{\"check\":\"archive.segments\",\"key\":\"missing-0\"}\n";
    let sha256 = logweir_core::ids::sha256_prefixed(details.as_bytes());
    let details_name = format!("lwc-{name}-details");
    fake.seed(
        "configmaps",
        namespace,
        json!({
            "metadata": {
                "name": details_name,
                "annotations": {"logweir.dev/result-sha256": sha256, "logweir.dev/result-format": "logweir.dev/check-details/v1"},
                "ownerReferences": [{"apiVersion": "logweir.dev/v1alpha1", "kind": "Preflight", "name": name, "uid": uid, "controller": true, "blockOwnerDeletion": true}]
            },
            "immutable": true,
            "data": {"details.jsonl": details}
        }),
    );
    let request = match operation {
        "Restore" => json!({
            "operation": "Restore",
            "restore": {"planBytes": "plan", "planHash": plan_hash.unwrap_or("sha256:aaaa"), "targetRef": {"name": "target"}},
            "timeoutSeconds": 120
        }),
        "DestinationAccess" => json!({
            "operation": "DestinationAccess",
            "destinationAccess": {"destinationRef": {"name": "primary"}, "roles": ["ArchiveWrite"]},
            "timeoutSeconds": 120
        }),
        _ => json!({
            "operation": "Backup",
            "backup": {"sourceRef": {"name": "source"}, "destinationRef": {"name": "primary"}, "topics": ["orders"]},
            "timeoutSeconds": 120
        }),
    };
    let mut metadata =
        json!({"name": name, "uid": uid, "labels": {"logweir.dev/destination": "primary"}});
    if let Some(actor) = actor {
        metadata["annotations"] = json!({ACTOR_ANNOTATION: actor});
    }
    fake.seed(
        "preflights",
        namespace,
        json!({
            "metadata": metadata,
            "spec": {"request": request, "cancelRequested": false},
            "status": {
                "phase": "Completed",
                "reason": "NotReady",
                "binding": {
                    "operation": operation,
                    "planHash": plan_hash,
                    "inputsDigest": "sha256:2222222222222222222222222222222222222222222222222222222222222222",
                    "referents": [{"kind": "KafkaCluster", "name": "target", "uid": "uid-target", "generation": 1}]
                },
                "observedAt": "2026-09-15T11:59:00Z",
                "result": {
                    "state": "notReady",
                    "expiresAt": "2026-09-15T12:14:00Z",
                    "checks": [
                        {"id": "target.mappedTopics", "category": "target", "state": "notReady", "gating": "blocking", "authority": "checkJob", "code": "MappedTopicExists", "message": "2 mapped target topics already exist", "remedy": "Choose another prefix.", "observedAt": "2026-09-15T11:59:00Z", "expiresAt": "2026-09-15T12:14:00Z"},
                        {"id": "archive.retention", "category": "archive", "state": "notReady", "gating": "advisory", "authority": "controller", "code": "RetentionWindowShort"},
                        {"id": "target.logAppendTime", "category": "target", "state": "unknown", "gating": "executionOnly", "authority": "checkJob", "code": "ExecutionOnly", "message": "Verified only when the run executes."}
                    ],
                    "detailsRef": {"name": details_name, "sha256": sha256}
                },
                "conditions": [{"type": "Complete", "status": "True", "reason": "Completed"}]
            }
        }),
    )
}

/// A running preflight, so a cancel has something to write.
pub fn seed_running_preflight(fake: &FakeKube, namespace: &str, name: &str, actor: &str) -> Value {
    fake.seed(
        "preflights",
        namespace,
        json!({
            "metadata": {"name": name, "uid": format!("uid-{name}"), "annotations": {ACTOR_ANNOTATION: actor}},
            "spec": {
                "request": {"operation": "Backup", "backup": {"sourceRef": {"name": "source"}, "destinationRef": {"name": "primary"}, "topics": ["orders"]}, "timeoutSeconds": 120},
                "cancelRequested": false
            },
            "status": {"phase": "Running", "reason": "Running"}
        }),
    )
}

// ------------------------------------------------------- shared-mode harness

use logweir_api::auth::keys::{CookieKeys, VersionedKey};
use logweir_api::auth::oidc::{OidcSettings, Provider, Secret, TokenAuthMethod};
use logweir_api::auth::ratelimit::{RateLimiter, StreamSlots};
use logweir_api::auth::session::{self, SessionClaims};
use logweir_api::auth::shared::SessionAuthenticator;
pub use logweir_api::authz::{Role, RoleBinding, RoleBindings, SharedAuthorizer};

/// How a shared-mode test app is put together.
pub struct SharedOptions {
    /// The role-binding table.
    pub bindings: RoleBindings,
    /// The session key's version.
    pub key_version: u32,
    /// The session key bytes.
    pub key_bytes: Vec<u8>,
    /// The session lifetime.
    pub session_max_age_seconds: i64,
    /// The allowed JWS algorithms.
    pub allowed_algorithms: Vec<String>,
    /// Peer ranges whose forwarded headers may be recorded.
    pub trusted_proxy_cidrs: Vec<logweir_api::config::Cidr>,
    /// The login rate limiter.
    pub login_limiter: Option<RateLimiter>,
}

impl Default for SharedOptions {
    fn default() -> Self {
        Self {
            bindings: RoleBindings::default(),
            key_version: 1,
            key_bytes: vec![0x7e; 32],
            session_max_age_seconds: 900,
            allowed_algorithms: vec!["RS256".into(), "ES256".into()],
            trusted_proxy_cidrs: Vec::new(),
            login_limiter: None,
        }
    }
}

/// One binding, for the table a test declares.
pub fn binding(role: Role, namespace: &str, groups: &[&str]) -> RoleBinding {
    RoleBinding {
        role,
        namespace: namespace.to_string(),
        groups: groups.iter().map(|g| (*g).to_string()).collect(),
        subjects: Vec::new(),
    }
}

/// One binding by exact subject.
pub fn subject_binding(role: Role, namespace: &str, subjects: &[&str]) -> RoleBinding {
    RoleBinding {
        role,
        namespace: namespace.to_string(),
        groups: Vec::new(),
        subjects: subjects.iter().map(|s| (*s).to_string()).collect(),
    }
}

/// The default two-namespace table the matrix tests use.
pub fn default_bindings() -> RoleBindings {
    RoleBindings {
        revision: "rev-1".into(),
        bindings: vec![
            binding(Role::Viewer, NS_A, &["lw-a-viewers"]),
            binding(Role::Operator, NS_A, &["lw-a-operators"]),
            binding(Role::Approver, NS_A, &["lw-a-approvers"]),
            binding(Role::Administrator, NS_A, &["lw-a-admins"]),
            binding(Role::Viewer, NS_B, &["lw-b-viewers"]),
            binding(Role::Operator, NS_B, &["lw-b-operators"]),
            binding(Role::Approver, NS_B, &["lw-b-approvers"]),
            binding(Role::Administrator, NS_B, &["lw-b-admins"]),
        ],
    }
}

/// A shared-mode app, its provider double and its session keys.
pub struct SharedApp {
    pub app: TestApp,
    pub idp: idp::MockIdp,
    pub keys: Arc<CookieKeys>,
    pub authorizer: Arc<SharedAuthorizer>,
}

impl SharedApp {
    /// Build one over a fake cluster and a provider double.
    pub fn new(fake: FakeKube, idp: idp::MockIdp, options: SharedOptions) -> Self {
        let clock = TestClock::new();
        Self::with_clock(fake, idp, options, clock)
    }

    pub fn with_clock(
        fake: FakeKube,
        idp: idp::MockIdp,
        options: SharedOptions,
        clock: Arc<TestClock>,
    ) -> Self {
        let keys = Arc::new(CookieKeys::new(&VersionedKey::from_parts(
            options.key_version,
            options.key_bytes.clone(),
        )));
        let authorizer = Arc::new(SharedAuthorizer::new(options.bindings.clone()));
        let provider = Provider::new(
            OidcSettings {
                issuer: ISSUER.to_string(),
                client_id: CLIENT_ID.to_string(),
                client_secret: Secret::new("a-client-secret".into()),
                redirect_uri: REDIRECT_URI.to_string(),
                allowed_algorithms: options.allowed_algorithms.clone(),
                scopes: vec!["openid".into(), "profile".into(), "groups".into()],
                groups_claim: "groups".into(),
                display_name_claim: "name".into(),
                token_auth_method: TokenAuthMethod::ClientSecretBasic,
            },
            Box::new(idp.clone()),
        );
        let shared = Arc::new(SharedMode {
            provider,
            keys: Arc::clone(&keys),
            login_limiter: options.login_limiter.unwrap_or_else(RateLimiter::for_login),
            streams: StreamSlots::new(),
            session_max_age_seconds: options.session_max_age_seconds,
            trusted_proxy_cidrs: options.trusted_proxy_cidrs.clone(),
        });
        let app = TestApp::with_clock(
            fake,
            Options {
                authenticator: Some(Arc::new(SessionAuthenticator::new(
                    Arc::clone(&keys),
                    clock.clone() as Arc<dyn Clock>,
                ))),
                authorizer: Some(Arc::clone(&authorizer) as Arc<dyn Authorizer>),
                public_origin: SHARED_ORIGIN.to_string(),
                allowed_hosts: vec![SHARED_HOST.to_string()],
                shared: Some(shared),
                ..Options::default()
            },
            clock,
        );
        Self {
            app,
            idp,
            keys,
            authorizer,
        }
    }

    /// A sealed session cookie for an identity, without going through login.
    ///
    /// The login flow is exercised end to end in its own tests; the matrix and
    /// CSRF tests want a session without repeating it thirty times.
    pub fn session_cookie(&self, subject: &str, groups: &[&str]) -> String {
        self.session_cookie_with("sid-".to_string() + subject, subject, groups, 900)
    }

    pub fn session_cookie_with(
        &self,
        session_id: String,
        subject: &str,
        groups: &[&str],
        lifetime: i64,
    ) -> String {
        let identity = logweir_api::auth::oidc::Identity {
            issuer: ISSUER.to_string(),
            subject: subject.to_string(),
            display_name: format!("{subject} display"),
            groups: groups.iter().map(|g| (*g).to_string()).collect(),
            auth_time: self.app.clock.now(),
        };
        let claims = SessionClaims::issue(
            &identity,
            session_id,
            self.keys.version(),
            self.app.clock.now(),
            lifetime,
        );
        session::set_cookie(&self.keys, &claims, self.app.clock.now())
            .split(';')
            .next()
            .unwrap()
            .to_string()
    }

    /// The synchronizer token for a session cookie made by
    /// [`Self::session_cookie`].
    pub fn csrf_for(&self, subject: &str) -> String {
        self.keys.csrf_token(&format!("sid-{subject}"))
    }

    /// A GET carrying a session cookie.
    pub async fn get(&self, path: &str, cookie: &str) -> TestResponse {
        self.app
            .send(
                Request::builder()
                    .method("GET")
                    .uri(path)
                    .header("host", SHARED_HOST)
                    .header("cookie", cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
    }

    /// A POST carrying a session cookie, an idempotency key and a CSRF token.
    pub async fn post(
        &self,
        path: &str,
        cookie: &str,
        csrf: Option<&str>,
        key: Option<&str>,
        body: &str,
    ) -> TestResponse {
        let mut builder = Request::builder()
            .method("POST")
            .uri(path)
            .header("host", SHARED_HOST)
            .header("origin", SHARED_ORIGIN)
            .header("content-type", "application/json")
            .header("cookie", cookie);
        if let Some(csrf) = csrf {
            builder = builder.header("x-csrf-token", csrf);
        }
        if let Some(key) = key {
            builder = builder.header("idempotency-key", key);
        }
        self.app
            .send(builder.body(Body::from(body.to_string())).unwrap())
            .await
    }
}
