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
pub const PLURALS: [&str; 5] = [
    "kafkaclusters",
    "backupschedules",
    "backups",
    "restores",
    "approvals",
];
const PREFIX: &str = "/apis/logweir.dev/v1alpha1/namespaces/";

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
        object["apiVersion"] = json!("logweir.dev/v1alpha1");
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
        ("PATCH", Some(name)) if plural == "backupschedules" => {
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
            // The allowed patch surface: metadata.resourceVersion and
            // spec.suspend, nothing else.
            let allowed = patch.as_object().is_some_and(|o| {
                o.keys().all(|k| k == "metadata" || k == "spec")
                    && o.get("metadata").is_none_or(|m| {
                        m.as_object()
                            .is_some_and(|m| m.keys().all(|k| k == "resourceVersion"))
                    })
                    && o.get("spec").is_none_or(|sp| {
                        sp.as_object()
                            .is_some_and(|sp| sp.keys().all(|k| k == "suspend"))
                    })
            });
            if !allowed {
                s.unexpected
                    .push(format!("PATCH outside spec.suspend: {}", recorded.body));
                return (
                    None,
                    422,
                    status_body(422, "Invalid", "only spec.suspend is mutable"),
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
            merge(&mut updated, &body);
            s.next_rv += 1;
            updated["metadata"]["resourceVersion"] = json!(s.next_rv.to_string());
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

// ------------------------------------------------------- shared-mode harness

use logweir_api::auth::keys::{CookieKeys, VersionedKey};
use logweir_api::auth::oidc::{OidcSettings, Provider, Secret, TokenAuthMethod};
use logweir_api::auth::ratelimit::{RateLimiter, StreamSlots};
use logweir_api::auth::session::{self, SessionClaims};
use logweir_api::auth::shared::SessionAuthenticator;
use logweir_api::authz::{Role, RoleBinding, RoleBindings, SharedAuthorizer};

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
