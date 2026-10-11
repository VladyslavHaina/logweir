//! Helpers shared by `crates/weirkeeper/tests/*.rs`. Declare with `mod support;`
//! and import the members a file uses. Unused items in a given binary are
//! expected, hence the allow.
#![allow(dead_code)]

use std::path::{Path, PathBuf};

use chrono::{DateTime, TimeZone, Utc};
use k8s_openapi::api::core::v1::Pod;
use logweir_core::check_contract::{
    Authority, CheckCode, CheckId, CheckOutcome, CheckState, Gating,
};
use logweir_store::Store;
use serde_json::{json, Value};
use weirkeeper::testing::{mock_client_recording_bodies, Recorder, Route, SeenBody, SeenRequest};

/// One status's conditions as `(type, status, reason)`, in array order.
pub fn conditions_of(status: &Value) -> Vec<(String, String, String)> {
    status["conditions"]
        .as_array()
        .map(|cs| {
            cs.iter()
                .map(|c| {
                    (
                        c["type"].as_str().unwrap_or_default().to_string(),
                        c["status"].as_str().unwrap_or_default().to_string(),
                        c["reason"].as_str().unwrap_or_default().to_string(),
                    )
                })
                .collect()
        })
        .unwrap_or_default()
}

/// A recorded URI's PATH, without its query string.
///
/// `kube` appends a `?` to every request target it builds, empty query
/// included (`…/backups/<n>/status?`), so an `ends_with("/status")` over the
/// raw URI is false for every request the client actually makes. Stripping the
/// query once, here, is what keeps every assertion below about the route
/// rather than about the client's URL-building.
pub fn path(uri: &str) -> &str {
    uri.split('?').next().unwrap_or(uri)
}

/// Every `PATCH …/status` body the double saw, as JSON.
pub fn patched_statuses(bodies: &[SeenBody]) -> Vec<Value> {
    bodies
        .iter()
        .filter(|b| b.method == "PATCH" && path(&b.uri).ends_with("/status"))
        .map(|b| serde_json::from_str::<Value>(&b.body).expect("a status patch is JSON"))
        .map(|v| v["status"].clone())
        .collect()
}

/// The workspace root: this crate's manifest directory is
/// `crates/weirkeeper`.
pub fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crates/weirkeeper sits two levels under the workspace root")
        .to_path_buf()
}

/// This process's own peak resident set, in bytes (`ru_maxrss` is kilobytes on
/// Linux and bytes on macOS). Read by the CHILD at its end, so each child
/// reports only its own peak and no ordering between children matters.
pub fn self_peak_rss() -> u64 {
    // Linux keeps `ru_maxrss` across `execve`, so a child reports its parent's
    // peak when that was higher, and a difference between two children is
    // noise. The child's own address space has its own high-water mark,
    // `VmHWM`, reset at exec.
    if cfg!(target_os = "linux") {
        if let Some(kib) = std::fs::read_to_string("/proc/self/status")
            .ok()
            .and_then(|status| {
                status.lines().find_map(|l| {
                    l.strip_prefix("VmHWM:")
                        .and_then(|v| v.trim().trim_end_matches("kB").trim().parse::<u64>().ok())
                })
            })
        {
            return kib * 1024;
        }
    }
    let usage = nix::sys::resource::getrusage(nix::sys::resource::UsageWho::RUSAGE_SELF)
        .expect("getrusage(RUSAGE_SELF)");
    let max = u64::try_from(usage.max_rss()).unwrap_or(0);
    if cfg!(target_os = "macos") {
        max
    } else {
        max * 1024
    }
}

pub fn put(s: &Store, key: &str, bytes: &[u8]) {
    s.put_create_only(key, bytes)
        .expect("the fixture object is written");
}

/// A UTC instant, spelled as five integers so a test reads like a calendar.
pub fn utc(y: i32, m: u32, d: u32, h: u32, min: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(y, m, d, h, min, 0)
        .single()
        .expect("the fixture instant exists")
}

/// A 404 `Status`, the shape `Api::get_opt` reads as "absent".
pub fn not_found_body(kind: &str, name: &str) -> String {
    format!(
        r#"{{"kind":"Status","apiVersion":"v1","status":"Failure",
  "message":"{kind} \"{name}\" not found","reason":"NotFound","code":404}}"#
    )
}

pub fn at(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s)
        .expect("a fixture instant")
        .with_timezone(&Utc)
}

/// The workspace root: two levels above this crate's manifest directory.
pub fn workspace_root() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("the workspace root is two levels above this crate")
        .to_path_buf()
}

/// Every request the double saw, method and path, query string dropped.
pub fn seen(recorder: &Recorder) -> Vec<(String, String)> {
    recorder
        .lock()
        .expect("the recorder mutex")
        .iter()
        .map(|SeenRequest { method, uri }| {
            (
                method.clone(),
                uri.split('?').next().unwrap_or(uri).to_string(),
            )
        })
        .collect()
}

pub fn spki(marker: u8) -> String {
    format!(
        "-----BEGIN PUBLIC KEY-----\nMFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAE{marker:02x}\n\
         -----END PUBLIC KEY-----\n"
    )
}

/// An empty `TrustPolicy` list: no policy governs the namespace, so the
/// catalog resolves the synthesised `legacy-roster-v1` from the roster route.
pub fn no_trust_policies() -> String {
    json!({"apiVersion": "logweir.dev/v1alpha1", "kind": "TrustPolicyList", "metadata": {"resourceVersion": "1"}, "items": []}).to_string()
}

/// A minimal but WELL-FORMED answer: `kube` deserialises every response into
/// the typed object, so a `{}` body fails as a transport error and would hide
/// whatever the reconciler actually did.
pub fn empty_config_map() -> String {
    json!({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {}}).to_string()
}

pub struct Fixture {
    pub client: kube::Client,
    pub recorder: Recorder,
    pub bodies: std::sync::Arc<std::sync::Mutex<Vec<SeenBody>>>,
}

pub fn fixture(routes: Vec<Route>) -> Fixture {
    let (client, recorder, bodies) = mock_client_recording_bodies(routes);
    Fixture {
        client,
        recorder,
        bodies,
    }
}

pub fn route(method: &'static str, path_suffix: &'static str, body: String) -> Route {
    Route {
        method,
        path_suffix,
        status: 200,
        body,
    }
}

/// The `connection.authenticated` row
/// `logweir::check::kinds::from_broker_failure` builds for a classified broker
/// failure.
pub fn broker_failure(
    code: CheckCode,
    state: CheckState,
    message: &str,
    remedy: &str,
) -> CheckOutcome {
    CheckOutcome::new(
        CheckId::ConnectionAuthenticated,
        state,
        Gating::Blocking,
        Authority::CheckJob,
        code,
    )
    .with_message(message)
    .with_remedy(remedy)
}

pub fn backup_request() -> Value {
    json!({
        "operation": "Backup",
        "backup": {
            "sourceRef": {"name": "source"},
            "destinationRef": {"name": "primary"},
            "topics": ["orders"]
        },
        "timeoutSeconds": 120
    })
}

/// A sidecar document, as the UTF-8 text `spec.sidecarBytes` carries.
pub fn sidecar(payload_type: &str, signatures: &[(&str, &str)]) -> String {
    let sigs = signatures
        .iter()
        .map(|(keyid, sig)| format!(r#"{{"keyid":"{keyid}","sig":"{sig}"}}"#))
        .collect::<Vec<_>>()
        .join(",");
    format!(r#"{{"payloadType":"{payload_type}","signatures":[{sigs}]}}"#)
}

pub fn posted_job(bodies: &[SeenBody]) -> Value {
    let body = bodies
        .iter()
        .find(|b| b.method == "POST" && path(&b.uri).ends_with("/jobs"))
        .expect("the reconciler POSTed a Job");
    serde_json::from_str(&body.body).expect("a Job POST body is JSON")
}

/// How many `/status` `PATCH`es the double was asked for.
pub fn status_patch_count(bodies: &[SeenBody]) -> usize {
    bodies
        .iter()
        .filter(|b| b.method == "PATCH" && path(&b.uri).ends_with("/status"))
        .count()
}

pub fn repo() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("the repository root is above crates/weirkeeper")
}

pub fn pod_list(pods: Vec<Pod>) -> String {
    serde_json::to_string(&json!({
        "apiVersion": "v1", "kind": "PodList", "metadata": {}, "items": pods
    }))
    .expect("a serialisable list")
}

pub fn leak(s: String) -> &'static str {
    Box::leak(s.into_boxed_str())
}

pub fn not_found(kind: &str, name: &str) -> String {
    format!(
        r#"{{"kind":"Status","apiVersion":"v1","status":"Failure","message":"{kind} \"{name}\" not found","reason":"NotFound","code":404}}"#
    )
}
