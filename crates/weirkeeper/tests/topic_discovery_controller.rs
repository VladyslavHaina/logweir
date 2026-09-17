//! The `TopicDiscovery` reconciler — D2 §5, PLAT-09.1.
//!
//! EVERY TEST HERE IS A PURE-FUNCTION TEST OR A `mock_client` TEST. Nothing
//! dials a broker, nothing waits on a Job, and the double PANICS on a request
//! it was not given a route for — which is what turns "the reconciler did not
//! read that pod's log", "it created nothing while queued" and "it never
//! touched the previous discovery" into assertions rather than absences of
//! evidence.
//!
//! READ THESE THREE FIRST, in this order:
//!
//! 1. [`a_successful_listing_alone_is_unknown_and_never_attested_complete`] —
//!    D-SEAMS **S3** and the whole honesty claim of PLAT-09.1. Kafka silently
//!    omits topics a principal cannot describe, so a clean listing proves
//!    nothing about completeness. The mutant is the one-line change that would
//!    make a green run claim `attestedComplete`.
//! 2. [`a_label_wearing_pod_that_the_job_does_not_own_is_never_read`] —
//!    D-SEAMS **S6**, defect `SEC-PODLOG`. A check's stdout becomes a status
//!    and then an API response, and `batch.kubernetes.io/job-name` is writable
//!    by anything that can create a pod.
//! 3. [`the_chunks_are_written_before_the_status_and_the_ttl_after_it`] — the
//!    commit point. A restart anywhere in that sequence has to leave either
//!    nothing or a complete, readable result.

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{TimeZone, Utc};
use k8s_openapi::api::core::v1::Pod;
use logweir_core::check_contract::{
    frames, topic_tsv_sha256, CheckCode, CheckPlan, CheckPlanKind, CheckRequest, CheckResult,
    ExpectedSummary, InventoryCounts, InventoryResult, Stream, TopicEntry, TruncationReason,
    TOPIC_INVENTORY_FORMAT,
};
use serde_json::{json, Value};
use weirkeeper::check::{chunks, job as cjob, plan};
use weirkeeper::controllers::topic_discovery::{
    self as td, DiscoveryContext, TerminalDiscovery, PHASE_CANCELLED, PHASE_FAILED, PHASE_QUEUED,
    PHASE_RUNNING, PHASE_SUCCEEDED,
};
use weirkeeper::crds::topic_discovery::TopicDiscovery;
use weirkeeper::job::RunnerImage;
use weirkeeper::testing::{mock_client_recording_bodies, BodyRecorder, Recorder, Route};

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

const NS: &str = "logweir-d2-w8";
const NAME: &str = "td-orders";
/// The subject UID. The Job name below is `sha256(this)`'s first 20 hex
/// characters, and `the_job_name_is_a_pure_function_of_this_objects_uid` holds
/// the two together so a change to either has to be argued for twice.
const UID: &str = "9f2b1c44-0000-4000-8000-0000000000a1";
const JOB: &str = "lwc-td-288fecc03251c1c31851";
const JOB_UID: &str = "1a2b3c4d-0000-4000-8000-0000000000b2";
const OTHER_JOB_UID: &str = "deadbeef-0000-4000-8000-0000000000c3";
const CLUSTER_UID: &str = "c0ffee00-0000-4000-8000-0000000000d4";
const CLUSTER_ID: &str = "M29I2S7FQPyHBEX12Vx7XA";
const POD: &str = "lwc-td-288fecc03251c1c31851-abcde";
const IMPOSTOR_POD: &str = "not-mine-zzzzz";
const PRINCIPAL: &str = "User:backup";

const JOB_PATH: &str = "/jobs/lwc-td-288fecc03251c1c31851";
const PLAN_PATH: &str = "/configmaps/lwc-td-288fecc03251c1c31851-plan";
const POD_LOG_PATH: &str = "/pods/lwc-td-288fecc03251c1c31851-abcde/log";
const IMPOSTOR_LOG_PATH: &str = "/pods/not-mine-zzzzz/log";
const STATUS_PATH: &str = "/topicdiscoveries/td-orders/status";
const CLUSTER_PATH: &str = "/kafkaclusters/source";

/// The digest the plan `ConfigMap` annotation carries and the end frame
/// declares. Any value works; what matters is that ONE value is in both
/// places, because that is the comparison the relay verification is.
const PLAN_SHA: &str = "sha256:1111111111111111111111111111111111111111111111111111111111111111";

fn now() -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 9, 16, 12, 0, 0)
        .single()
        .expect("the fixture instant exists")
}

fn context(client: kube::Client, policy_ref: Option<(String, String)>) -> DiscoveryContext {
    DiscoveryContext {
        client,
        runner_image: RunnerImage::default(),
        policy_ref,
        policy_cache: Arc::new(weirkeeper::check::policy::PolicyCache::new()),
    }
}

/// One `TopicDiscovery`, with whatever status the test needs.
fn discovery(status: Value, extra_request: Value) -> TopicDiscovery {
    let mut request = json!({
        "connectionRef": {"name": "source"},
        "maxTopics": 20000,
        "timeoutSeconds": 60
    });
    if let (Some(base), Some(extra)) = (request.as_object_mut(), extra_request.as_object()) {
        for (k, v) in extra {
            base.insert(k.clone(), v.clone());
        }
    }
    serde_json::from_value(json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "TopicDiscovery",
        "metadata": {
            "name": NAME, "namespace": NS, "uid": UID,
            "generation": 1, "resourceVersion": "4242",
            "creationTimestamp": "2026-09-16T11:58:00Z"
        },
        "spec": {"request": request},
        "status": status
    }))
    .expect("the fixture is a TopicDiscovery")
}

/// A fresh request with no status at all.
fn fresh() -> TopicDiscovery {
    discovery(json!({}), json!({}))
}

/// A request whose Job exists and whose status says so.
fn running() -> TopicDiscovery {
    discovery(
        json!({
            "phase": PHASE_RUNNING,
            "reason": "PodNotStarted",
            "jobRef": {"name": JOB},
            "queuedAt": "2026-09-16T11:58:30Z",
            "binding": {
                "connectionName": "source", "connectionUid": CLUSTER_UID,
                "connectionGeneration": 1, "principal": PRINCIPAL,
                "authMode": "scramSha512",
                "bootstrapSha256": "sha256:0000000000000000000000000000000000000000000000000000000000000000",
                "policyDigest": "sha256:0000000000000000000000000000000000000000000000000000000000000000"
            }
        }),
        json!({}),
    )
}

fn cluster_body() -> String {
    serde_json::to_string(&json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "KafkaCluster",
        "metadata": {
            "name": "source", "namespace": NS, "uid": CLUSTER_UID, "generation": 1,
            "resourceVersion": "7", "creationTimestamp": "2026-09-16T10:00:00Z"
        },
        "spec": {
            "bootstrapServers": ["kafka-source:9092"],
            "role": "source",
            "auth": {"mode": "scramSha512", "username": "backup", "tls": false,
                     "secretRef": {"name": "kafka-source-scram", "passwordKey": "password"}}
        }
    }))
    .expect("a serialisable KafkaCluster")
}

fn discovery_body(d: &TopicDiscovery) -> String {
    serde_json::to_string(d).expect("a serialisable TopicDiscovery")
}

fn not_found(message: &str) -> String {
    format!(
        r#"{{"kind":"Status","apiVersion":"v1","status":"Failure","message":"{message}","reason":"NotFound","code":404}}"#
    )
}

fn conflict(message: &str) -> String {
    format!(
        r#"{{"kind":"Status","apiVersion":"v1","status":"Failure","message":"{message}","reason":"AlreadyExists","code":409}}"#
    )
}

/// A check Job owned by the discovery, finished or not.
fn job_object(finished: Option<&str>, owner_uid: &str) -> Value {
    let status = match finished {
        Some(kind) => json!({"conditions":[{
            "type": kind, "status": "True", "reason": "x",
            "lastProbeTime": "2026-09-16T11:59:00Z",
            "lastTransitionTime": "2026-09-16T11:59:00Z"
        }]}),
        None => json!({}),
    };
    json!({
        "apiVersion": "batch/v1", "kind": "Job",
        "metadata": {
            "name": JOB, "namespace": NS, "uid": JOB_UID,
            "creationTimestamp": "2026-09-16T11:58:30Z",
            "labels": {
                "app.kubernetes.io/component": "check",
                "logweir.dev/check-kind": "topicInventory",
                "logweir.dev/check-connection-uid": CLUSTER_UID
            },
            "ownerReferences": [{
                "apiVersion": "logweir.dev/v1alpha1", "kind": "TopicDiscovery",
                "name": NAME, "uid": owner_uid, "controller": true,
                "blockOwnerDeletion": true
            }]
        },
        "spec": {"template": {"spec": {"containers": [], "restartPolicy": "Never"}}},
        "status": status
    })
}

fn pod_object(name: &str, owner_uid: Option<&str>, container: Value) -> Pod {
    let owners = match owner_uid {
        Some(uid) => json!([{
            "apiVersion": "batch/v1", "kind": "Job", "name": JOB,
            "uid": uid, "controller": true, "blockOwnerDeletion": true
        }]),
        None => json!([]),
    };
    serde_json::from_value(json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {
            "name": name, "namespace": NS,
            "creationTimestamp": "2026-09-16T11:58:40Z",
            "ownerReferences": owners,
            "labels": {"batch.kubernetes.io/job-name": JOB}
        },
        "spec": {"containers": []},
        "status": container
    }))
    .expect("the fixture is a Pod")
}

fn terminated(exit_code: i32) -> Value {
    json!({"phase":"Succeeded","containerStatuses":[{
        "name":"runner","image":"x","imageID":"x","ready":false,"restartCount":0,
        "state":{"terminated":{"exitCode":exit_code,
                 "startedAt":"2026-09-16T11:58:45Z","finishedAt":"2026-09-16T11:59:00Z"}}
    }]})
}

fn pod_list(pods: Vec<Pod>) -> String {
    serde_json::to_string(&json!({
        "apiVersion": "v1", "kind": "PodList", "metadata": {}, "items": pods
    }))
    .expect("a serialisable list")
}

fn job_list(items: Vec<Value>) -> String {
    serde_json::to_string(&json!({
        "apiVersion": "batch/v1", "kind": "JobList", "metadata": {}, "items": items
    }))
    .expect("a serialisable list")
}

fn plan_config_map(owner_uid: &str, immutable: bool, digest: &str) -> String {
    serde_json::to_string(&json!({
        "apiVersion": "v1", "kind": "ConfigMap",
        "metadata": {
            "name": format!("{JOB}-plan"), "namespace": NS,
            "annotations": {plan::DIGEST_ANNOTATION: digest},
            "ownerReferences": [{
                "apiVersion": "logweir.dev/v1alpha1", "kind": "TopicDiscovery",
                "name": NAME, "uid": owner_uid, "controller": true,
                "blockOwnerDeletion": true
            }]
        },
        "immutable": immutable,
        "data": {cjob::CHECK_PLAN_KEY: "{}"}
    }))
    .expect("a serialisable ConfigMap")
}

/// The inventory a runner would print for `entries`.
fn inventory_of(entries: &[TopicEntry], counts: InventoryCounts) -> InventoryResult {
    InventoryResult {
        format: TOPIC_INVENTORY_FORMAT.to_string(),
        cluster_id: Some(CLUSTER_ID.to_string()),
        broker_count: Some(1),
        counts,
        truncated: false,
        truncation_reason: None,
        topic_authorization_error_in_listing: false,
        expected: ExpectedSummary::default(),
        expected_results: Vec::new(),
        topics_sha256: topic_tsv_sha256(entries),
    }
}

fn counts_for(entries: &[TopicEntry], internal_excluded: u32) -> InventoryCounts {
    let returned = u32::try_from(entries.len()).expect("a small fixture");
    InventoryCounts {
        listed: returned + internal_excluded,
        returned,
        internal_excluded,
        errored: 0,
    }
}

/// A complete, verifiable relay for `entries` plus `inventory`.
fn relay_log(entries: &[TopicEntry], inventory: &InventoryResult) -> String {
    let mut result = CheckResult::new(CheckPlanKind::TopicInventory);
    result.inventory = Some(inventory.clone());
    let bytes = result.to_canonical_json().expect("a serialisable result");
    let parts = frames::write_parts(Stream::Result, &bytes).expect("small enough to frame");
    let mut streams = BTreeMap::new();
    streams.insert(Stream::Result, (bytes.clone(), parts.len()));
    let end = frames::end_frame(PLAN_SHA, UID, &streams, Some(entries));

    let mut out = String::new();
    for e in entries {
        out.push_str(&frames::write_topic_line(e).expect("a framable entry"));
        out.push('\n');
    }
    for p in parts {
        out.push_str(&p);
        out.push('\n');
    }
    out.push_str(&frames::write_end(&end).expect("a framable end"));
    out.push('\n');
    out
}

/// The routes every "the Job has finished and its pod is readable" test needs.
fn finished_routes(log: String, extra: Vec<Route>) -> Vec<Route> {
    let mut routes = vec![
        Route {
            method: "GET",
            path_suffix: JOB_PATH,
            status: 200,
            body: job_object(Some("Complete"), UID).to_string(),
        },
        Route {
            method: "GET",
            path_suffix: "/pods",
            status: 200,
            body: pod_list(vec![pod_object(POD, Some(JOB_UID), terminated(0))]),
        },
        Route {
            method: "GET",
            path_suffix: POD_LOG_PATH,
            status: 200,
            body: log,
        },
        Route {
            method: "GET",
            path_suffix: PLAN_PATH,
            status: 200,
            body: plan_config_map(UID, true, PLAN_SHA),
        },
        Route {
            method: "PATCH",
            path_suffix: STATUS_PATH,
            status: 200,
            body: discovery_body(&running()),
        },
        Route {
            method: "PATCH",
            path_suffix: JOB_PATH,
            status: 200,
            body: job_object(Some("Complete"), UID).to_string(),
        },
    ];
    routes.extend(extra);
    routes
}

/// The body of the first request matching `method` and a path containing
/// `needle`, as JSON.
fn body_of(bodies: &BodyRecorder, method: &str, needle: &str) -> Value {
    let seen = bodies.lock().expect("the body recorder");
    let hit = seen
        .iter()
        .find(|b| b.method == method && b.uri.contains(needle))
        .unwrap_or_else(|| {
            panic!(
                "no {method} request whose path contains `{needle}`; the double saw {:?}",
                seen.iter()
                    .map(|b| format!("{} {}", b.method, b.uri))
                    .collect::<Vec<_>>()
            )
        });
    serde_json::from_str(&hit.body).unwrap_or_else(|e| {
        panic!("the recorded body is not JSON ({e}): {}", hit.body);
    })
}

fn bodies_of(bodies: &BodyRecorder, method: &str, needle: &str) -> Vec<Value> {
    bodies
        .lock()
        .expect("the body recorder")
        .iter()
        .filter(|b| b.method == method && b.uri.contains(needle))
        .map(|b| serde_json::from_str(&b.body).expect("a JSON body"))
        .collect()
}

fn calls(recorder: &Recorder) -> Vec<String> {
    recorder
        .lock()
        .expect("the recorder")
        .iter()
        .map(|r| format!("{} {}", r.method, r.uri.split('?').next().unwrap_or(&r.uri)))
        .collect()
}

fn index_of(calls: &[String], method: &str, needle: &str) -> usize {
    calls
        .iter()
        .position(|c| c.starts_with(method) && c.contains(needle))
        .unwrap_or_else(|| panic!("no {method} …{needle} in {calls:?}"))
}

fn status_of(bodies: &BodyRecorder) -> Value {
    body_of(bodies, "PATCH", "/topicdiscoveries/")["status"].clone()
}

// ---------------------------------------------------------------------------
// The start path — D2 §5.2 steps 2–4
// ---------------------------------------------------------------------------

/// The Job's name is `lwc-td-<first 20 hex of sha256(uid)>`, independent of the
/// object's name — so there is no `NameTooLong` path for this kind and a
/// duplicate reconcile gets a 409 rather than a second check Job.
#[test]
fn the_job_name_is_a_pure_function_of_this_objects_uid() {
    assert_eq!(
        cjob::check_job_name(CheckPlanKind::TopicInventory, UID),
        JOB
    );
    assert_eq!(JOB.len(), 27, "a check Job name is always 27 characters");
}

fn start_routes(active: Vec<Value>) -> Vec<Route> {
    vec![
        Route {
            method: "GET",
            path_suffix: JOB_PATH,
            status: 404,
            body: not_found("jobs \\\"lwc-td-288fecc03251c1c31851\\\" not found"),
        },
        Route {
            method: "GET",
            path_suffix: CLUSTER_PATH,
            status: 200,
            body: cluster_body(),
        },
        Route {
            method: "GET",
            path_suffix: "/jobs",
            status: 200,
            body: job_list(active),
        },
        Route {
            method: "POST",
            path_suffix: "/configmaps",
            status: 201,
            body: "{}".to_string(),
        },
        Route {
            method: "POST",
            path_suffix: "/jobs",
            status: 201,
            body: job_object(None, UID).to_string(),
        },
        Route {
            method: "PATCH",
            path_suffix: STATUS_PATH,
            status: 200,
            body: discovery_body(&running()),
        },
    ]
}

/// A new request resolves its connection, renders ONE immutable owned plan
/// `ConfigMap`, creates ONE check Job, and publishes the binding it was
/// resolved against.
#[tokio::test]
async fn a_new_request_renders_one_plan_and_creates_one_owned_check_job() {
    let (client, recorder, bodies) = mock_client_recording_bodies(start_routes(vec![]));
    let outcome = td::reconcile_discovery(&fresh(), &context(client, None))
        .await
        .expect("the reconcile answers");

    assert_eq!(outcome.phase, PHASE_RUNNING);
    assert!(outcome.created_job);
    assert_eq!(outcome.chunks_written, 0);
    assert!(!outcome.ttl_patched, "an unfinished Job gets no TTL");

    // THE PLAN BEFORE THE JOB. A Job whose plan `ConfigMap` does not exist sits
    // in `ContainerCreating` until its deadline and reports nothing useful.
    let seen = calls(&recorder);
    assert!(
        index_of(&seen, "POST", "/configmaps") < index_of(&seen, "POST", "/jobs"),
        "the plan is created before the Job: {seen:?}"
    );

    let cm = body_of(&bodies, "POST", "/configmaps");
    assert_eq!(cm["immutable"], json!(true), "a mounted plan cannot change");
    assert_eq!(cm["metadata"]["name"], json!(format!("{JOB}-plan")));
    let owner = &cm["metadata"]["ownerReferences"][0];
    assert_eq!(owner["uid"], json!(UID));
    assert_eq!(owner["controller"], json!(true));
    assert_eq!(owner["kind"], json!("TopicDiscovery"));
    assert!(
        cm["metadata"]["annotations"][plan::DIGEST_ANNOTATION]
            .as_str()
            .is_some_and(|d| d.starts_with("sha256:")),
        "the plan digest is pinned on an annotation: {cm}"
    );

    let job = body_of(&bodies, "POST", "/jobs");
    assert_eq!(job["metadata"]["name"], json!(JOB));
    assert_eq!(
        job["metadata"]["labels"][cjob::LABEL_CHECK_OWNER_UID],
        json!(UID)
    );
    assert_eq!(
        job["metadata"]["labels"][cjob::LABEL_CHECK_CONNECTION_UID],
        json!(CLUSTER_UID),
        "the connection UID label is what the per-connection ceiling counts"
    );
    assert_eq!(job["spec"]["activeDeadlineSeconds"], json!(60 + 90));
    assert_eq!(job["spec"]["backoffLimit"], json!(0));
    assert_eq!(
        job["spec"]["template"]["spec"]["automountServiceAccountToken"],
        json!(false),
        "a check Job carries no Kubernetes token"
    );

    let status = status_of(&bodies);
    assert_eq!(status["phase"], json!(PHASE_RUNNING));
    assert_eq!(status["jobRef"]["name"], json!(JOB));
    assert_eq!(status["binding"]["connectionUid"], json!(CLUSTER_UID));
    assert_eq!(status["binding"]["principal"], json!(PRINCIPAL));
    assert_eq!(status["binding"]["authMode"], json!("scramSha512"));
    assert!(
        status["binding"]["bootstrapSha256"]
            .as_str()
            .is_some_and(|d| d.starts_with("sha256:")),
        "the bootstrap digest is what D2 §5.7 compares for staleness: {status}"
    );
}

/// **D-SEAMS S2, and the credential rule.** The plan is an input to a CHECK
/// Job: its kind is `topicInventory`, it names the environment variable the
/// kubelet projects the SASL password into, and it carries no password, no
/// bucket and nothing a `Backup` would execute.
#[tokio::test]
async fn the_plan_is_a_check_input_that_names_a_variable_and_never_a_credential() {
    let (client, _recorder, bodies) = mock_client_recording_bodies(start_routes(vec![]));
    td::reconcile_discovery(&fresh(), &context(client, None))
        .await
        .expect("the reconcile answers");

    let cm = body_of(&bodies, "POST", "/configmaps");
    let raw = cm["data"][cjob::CHECK_PLAN_KEY]
        .as_str()
        .expect("the plan document is a string in `data`")
        .to_string();
    let document: CheckPlan = serde_json::from_str(&raw).expect("the plan parses strictly");

    assert_eq!(document.kind(), CheckPlanKind::TopicInventory);
    assert_eq!(document.subject_uid, UID);
    let CheckRequest::TopicInventory(request) = &document.request else {
        panic!("a discovery renders a topicInventory request, got {document:?}");
    };
    assert_eq!(
        request.connection.password_env.as_deref(),
        Some("LOGWEIR_SOURCE_PASSWORD"),
        "the plan names the VARIABLE the kubelet projects the password into"
    );
    assert_eq!(request.connection.principal, PRINCIPAL);
    assert_eq!(request.max_topics, 20_000);
    assert!(
        !request.include_internal,
        "internal topics are out by default"
    );

    // The whole document, as bytes, carries no credential-shaped anything. The
    // plan `ConfigMap` is readable by anything that can read `ConfigMap`s in
    // this namespace.
    for forbidden in ["password\":\"", "secretRef", "AWS_SECRET", "hunter2"] {
        assert!(
            !raw.contains(forbidden),
            "the check plan carries `{forbidden}`: {raw}"
        );
    }
}

/// The plan digest is a pure function of the document, so a second pass renders
/// the same bytes and the 409 rule is a comparison rather than a coin toss.
#[tokio::test]
async fn two_passes_render_a_byte_identical_plan() {
    let mut rendered = Vec::new();
    for _ in 0..2 {
        let (client, _r, bodies) = mock_client_recording_bodies(start_routes(vec![]));
        td::reconcile_discovery(&fresh(), &context(client, None))
            .await
            .expect("the reconcile answers");
        rendered.push(body_of(&bodies, "POST", "/configmaps"));
    }
    assert_eq!(
        rendered[0], rendered[1],
        "a re-render must be byte-identical or the plan 409 rule refuses its own object"
    );
}

/// A connection that does not exist is TERMINAL, because `spec.request` is
/// immutable: this object can never name a different one.
#[tokio::test]
async fn a_missing_connection_is_terminal_and_says_the_request_cannot_be_repointed() {
    let routes = vec![
        Route {
            method: "GET",
            path_suffix: JOB_PATH,
            status: 404,
            body: not_found("jobs not found"),
        },
        Route {
            method: "GET",
            path_suffix: CLUSTER_PATH,
            status: 404,
            body: not_found("kafkaclusters.logweir.dev \\\"source\\\" not found"),
        },
        Route {
            method: "PATCH",
            path_suffix: STATUS_PATH,
            status: 200,
            body: discovery_body(&fresh()),
        },
    ];
    // NO POST ROUTE AT ALL. A request whose connection is missing must create
    // nothing — the double panics if it tries.
    let (client, _r, bodies) = mock_client_recording_bodies(routes);
    let outcome = td::reconcile_discovery(&fresh(), &context(client, None))
        .await
        .expect("the reconcile answers");

    assert_eq!(outcome.phase, PHASE_FAILED);
    assert_eq!(outcome.reason, CheckCode::ConnectionNotFound.as_str());
    let status = status_of(&bodies);
    assert_eq!(status["conditions"][0]["type"], json!("Complete"));
    assert_eq!(status["conditions"][0]["status"], json!("False"));
    assert!(
        status["message"]
            .as_str()
            .is_some_and(|m| m.contains("immutable")),
        "the message says why this is terminal: {status}"
    );
}

/// A connection the resolver refuses is `ConnectionInvalid` — and the refusals
/// are the ones a BACKUP gets, because a discovery that accepted a connection a
/// run would refuse would be a green light for work that cannot happen.
#[tokio::test]
async fn a_connection_the_resolver_refuses_is_terminal_with_connection_invalid() {
    let broken = serde_json::to_string(&json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "KafkaCluster",
        "metadata": {"name": "source", "namespace": NS, "uid": CLUSTER_UID,
                     "generation": 1, "resourceVersion": "7"},
        // SCRAM with no secretRef: the credential cannot be rendered.
        "spec": {"bootstrapServers": ["kafka-source:9092"], "role": "source",
                 "auth": {"mode": "scramSha512", "username": "backup"}}
    }))
    .expect("a serialisable KafkaCluster");
    let routes = vec![
        Route {
            method: "GET",
            path_suffix: JOB_PATH,
            status: 404,
            body: not_found("jobs not found"),
        },
        Route {
            method: "GET",
            path_suffix: CLUSTER_PATH,
            status: 200,
            body: broken,
        },
        Route {
            method: "PATCH",
            path_suffix: STATUS_PATH,
            status: 200,
            body: discovery_body(&fresh()),
        },
    ];
    let (client, _r, bodies) = mock_client_recording_bodies(routes);
    let outcome = td::reconcile_discovery(&fresh(), &context(client, None))
        .await
        .expect("the reconcile answers");

    assert_eq!(outcome.phase, PHASE_FAILED);
    assert_eq!(outcome.reason, CheckCode::ConnectionInvalid.as_str());
    assert!(
        status_of(&bodies)["message"]
            .as_str()
            .is_some_and(|m| !m.is_empty()),
        "the refusal names the field"
    );
}

/// Over the per-connection ceiling the request is QUEUED and creates nothing —
/// not a Job and not a plan `ConfigMap`. The route table holds no `POST`, so
/// the double panics if either were attempted.
#[tokio::test]
async fn over_the_per_connection_ceiling_the_request_is_queued_and_creates_nothing() {
    let routes = vec![
        Route {
            method: "GET",
            path_suffix: JOB_PATH,
            status: 404,
            body: not_found("jobs not found"),
        },
        Route {
            method: "GET",
            path_suffix: CLUSTER_PATH,
            status: 200,
            body: cluster_body(),
        },
        Route {
            method: "GET",
            path_suffix: "/jobs",
            status: 200,
            // One ACTIVE topicInventory Job against this very connection.
            body: job_list(vec![job_object(None, "someone-else")]),
        },
        Route {
            method: "PATCH",
            path_suffix: STATUS_PATH,
            status: 200,
            body: discovery_body(&fresh()),
        },
    ];
    let (client, _r, bodies) = mock_client_recording_bodies(routes);
    let outcome = td::reconcile_discovery(&fresh(), &context(client, None))
        .await
        .expect("the reconcile answers");

    assert_eq!(outcome.phase, PHASE_QUEUED);
    assert_eq!(outcome.reason, CheckCode::ConcurrencyLimited.as_str());
    assert!(!outcome.created_job);
    assert_eq!(outcome.requeue_seconds, 10);
    let status = status_of(&bodies);
    // THE BINDING TRAVELS WITH A QUEUED STATUS, so an operator can see which
    // connection is waiting.
    assert_eq!(status["binding"]["connectionUid"], json!(CLUSTER_UID));
    assert_eq!(status["conditions"][0]["status"], json!("Unknown"));
}

// ---------------------------------------------------------------------------
// The tracker's required cases
// ---------------------------------------------------------------------------

/// **Large catalog.** 5,003 entries become exactly three chunks, each within
/// BOTH of D2 §5.5's bounds, and the status carries the index and not the
/// names.
///
/// MUTANT: raise `chunks::MAX_CHUNK_LINES`, or write one `ConfigMap` for the
/// whole inventory. The chunk count, the per-chunk line count and the per-chunk
/// byte count each fail.
#[tokio::test]
async fn a_large_catalog_spans_three_chunks_inside_both_size_bounds() {
    let entries: Vec<TopicEntry> = (0..5_003)
        .map(|i| TopicEntry::new(&format!("bulk-{i:05}"), 3))
        .collect();
    let inventory = inventory_of(&entries, counts_for(&entries, 0));
    let routes = finished_routes(
        relay_log(&entries, &inventory),
        vec![Route {
            method: "POST",
            path_suffix: "/configmaps",
            status: 201,
            body: "{}".to_string(),
        }],
    );
    let (client, recorder, bodies) = mock_client_recording_bodies(routes);
    let outcome = td::reconcile_discovery(&running(), &context(client, None))
        .await
        .expect("the reconcile answers");

    assert_eq!(outcome.phase, PHASE_SUCCEEDED);
    assert_eq!(outcome.chunks_written, 3, "2500 + 2500 + 3");

    let written = bodies_of(&bodies, "POST", "/configmaps");
    assert_eq!(written.len(), 3);
    for (i, cm) in written.iter().enumerate() {
        let data = cm["data"][chunks::CHUNK_KEY]
            .as_str()
            .expect("a chunk carries its TSV in `data`");
        let lines = data.lines().count();
        assert!(
            lines <= chunks::MAX_CHUNK_LINES,
            "chunk {i} holds {lines} lines, over D2 §5.5's {}",
            chunks::MAX_CHUNK_LINES
        );
        assert!(
            data.len() <= chunks::MAX_CHUNK_BYTES,
            "chunk {i} is {} bytes, over D2 §5.5's {}",
            data.len(),
            chunks::MAX_CHUNK_BYTES
        );
        assert_eq!(cm["immutable"], json!(true));
        assert_eq!(cm["metadata"]["ownerReferences"][0]["uid"], json!(UID));
        assert_eq!(
            cm["metadata"]["annotations"][chunks::CHUNK_ANNOTATION],
            json!(format!("{}/3", i + 1))
        );
    }
    assert_eq!(
        written[0]["data"][chunks::CHUNK_KEY]
            .as_str()
            .expect("data")
            .lines()
            .count(),
        2_500
    );
    assert_eq!(
        written[2]["data"][chunks::CHUNK_KEY]
            .as_str()
            .expect("data")
            .lines()
            .count(),
        3
    );

    let status = status_of(&bodies);
    let index = status["result"]["chunks"]
        .as_array()
        .expect("the status carries a chunk index");
    assert_eq!(index.len(), 3);
    assert_eq!(index[0]["name"], json!(format!("{JOB}-r000")));
    assert_eq!(index[0]["count"], json!(2500));
    assert_eq!(index[0]["firstName"], json!("bulk-00000"));
    assert_eq!(index[0]["lastName"], json!("bulk-02499"));
    assert_eq!(index[2]["firstName"], json!("bulk-05000"));
    assert_eq!(index[2]["lastName"], json!("bulk-05002"));
    assert_eq!(status["result"]["counts"]["returned"], json!(5003));
    assert_eq!(
        status["result"]["topicsSha256"],
        json!(topic_tsv_sha256(&entries)),
        "the status publishes a digest it computed over the frames it verified"
    );
    // THE NAMES DO NOT LIVE IN THE STATUS.
    let text = serde_json::to_string(&status).expect("a serialisable status");
    assert!(
        !text.contains("bulk-00001"),
        "a topic inventory is unbounded and a status is not a store"
    );

    // THE COMMIT POINT: every chunk exists before the status patch.
    let seen = calls(&recorder);
    let last_chunk = seen
        .iter()
        .rposition(|c| c.starts_with("POST") && c.contains("/configmaps"))
        .expect("three chunk POSTs");
    assert!(
        last_chunk < index_of(&seen, "PATCH", "/topicdiscoveries/"),
        "the status patch that indexes the chunks is the commit: {seen:?}"
    );
}

/// **Empty cluster.** `counts.returned: 0` with an empty chunk index — a FACT,
/// and never "unknown". PLAT-09.1's acceptance is that a user can tell empty
/// from failed from stale from permission-limited, and this is the first of the
/// four.
#[tokio::test]
async fn an_empty_cluster_is_a_count_of_zero_and_not_a_failure() {
    let entries: Vec<TopicEntry> = Vec::new();
    let inventory = inventory_of(&entries, counts_for(&entries, 0));
    // NO `POST /configmaps` ROUTE: an empty inventory writes no chunk at all,
    // and the double panics if one were attempted.
    let (client, _r, bodies) =
        mock_client_recording_bodies(finished_routes(relay_log(&entries, &inventory), vec![]));
    let outcome = td::reconcile_discovery(&running(), &context(client, None))
        .await
        .expect("the reconcile answers");

    assert_eq!(outcome.phase, PHASE_SUCCEEDED);
    assert_eq!(outcome.chunks_written, 0);
    let status = status_of(&bodies);
    assert_eq!(status["result"]["counts"]["returned"], json!(0));
    assert_eq!(
        status["result"]["chunks"],
        json!([]),
        "an empty list and not an absent one: `stored nothing` differs from `no result yet`"
    );
    assert_eq!(status["result"]["visibility"]["state"], json!("unknown"));
    assert_eq!(status["conditions"][0]["status"], json!("True"));
}

/// **ACL-limited principal.** One expected topic the broker refused to describe
/// makes the whole observation `limited`, and the basis says which of D2 §5.4's
/// two detections fired.
#[tokio::test]
async fn an_expected_topic_the_broker_refuses_makes_the_observation_limited() {
    let entries = vec![TopicEntry::new("orders", 6)];
    let mut inventory = inventory_of(&entries, counts_for(&entries, 0));
    inventory.expected = ExpectedSummary {
        requested: 2,
        visible: 1,
        not_authorized: 1,
        not_found: 0,
        unknown: 0,
    };
    let (client, _r, bodies) = mock_client_recording_bodies(finished_routes(
        relay_log(&entries, &inventory),
        vec![Route {
            method: "POST",
            path_suffix: "/configmaps",
            status: 201,
            body: "{}".to_string(),
        }],
    ));
    td::reconcile_discovery(&running(), &context(client, None))
        .await
        .expect("the reconcile answers");

    let status = status_of(&bodies);
    assert_eq!(status["result"]["visibility"]["state"], json!("limited"));
    assert_eq!(
        status["result"]["visibility"]["basis"],
        json!(["expectedTopicNotAuthorized"])
    );
    assert_eq!(status["result"]["expected"]["notAuthorized"], json!(1));
    assert_eq!(status["result"]["expected"]["requested"], json!(2));
}

/// A listing entry that carried `TopicAuthorizationFailed` is D2 §5.4's other
/// detection, and it is `limited` on its own.
#[tokio::test]
async fn a_listing_authorization_error_is_limited_on_its_own() {
    let entries = vec![TopicEntry::new("orders", 6)];
    let mut inventory = inventory_of(&entries, counts_for(&entries, 0));
    inventory.topic_authorization_error_in_listing = true;
    let (client, _r, bodies) = mock_client_recording_bodies(finished_routes(
        relay_log(&entries, &inventory),
        vec![Route {
            method: "POST",
            path_suffix: "/configmaps",
            status: 201,
            body: "{}".to_string(),
        }],
    ));
    td::reconcile_discovery(&running(), &context(client, None))
        .await
        .expect("the reconcile answers");
    let status = status_of(&bodies);
    assert_eq!(status["result"]["visibility"]["state"], json!("limited"));
    assert_eq!(
        status["result"]["visibility"]["basis"],
        json!(["topicAuthorizationErrorInListing"])
    );
}

/// **THE HONESTY CLAIM.** A completely successful listing, with every expected
/// topic visible and no error anywhere, is `unknown` — because Kafka silently
/// omits topics a principal may not describe, so a clean list proves nothing.
///
/// MUTANT: make a `Succeeded` phase write `attestedComplete`, or make the
/// absence of an authorization error mean completeness. Either fails here, and
/// `an_administrator_attestation_is_the_only_route_to_attested_complete` is the
/// other half — the ONE thing that does upgrade it.
#[tokio::test]
async fn a_successful_listing_alone_is_unknown_and_never_attested_complete() {
    let entries = vec![TopicEntry::new("orders", 6), TopicEntry::new("payments", 3)];
    let mut inventory = inventory_of(&entries, counts_for(&entries, 0));
    inventory.expected = ExpectedSummary {
        requested: 2,
        visible: 2,
        not_authorized: 0,
        not_found: 0,
        unknown: 0,
    };
    let (client, _r, bodies) = mock_client_recording_bodies(finished_routes(
        relay_log(&entries, &inventory),
        vec![Route {
            method: "POST",
            path_suffix: "/configmaps",
            status: 201,
            body: "{}".to_string(),
        }],
    ));
    td::reconcile_discovery(&running(), &context(client, None))
        .await
        .expect("the reconcile answers");

    let status = status_of(&bodies);
    assert_eq!(
        status["result"]["visibility"]["state"],
        json!("unknown"),
        "a successful listing alone is `unknown`: {status}"
    );
    assert_eq!(
        status["result"]["visibility"]["attestation"],
        Value::Null,
        "nothing attested this"
    );
    assert!(
        status["message"]
            .as_str()
            .is_some_and(|m| m.contains("cannot describe")),
        "the message says what `unknown` means: {status}"
    );
}

fn policy_body(attestations: Value) -> String {
    let policy = json!({
        "version": 1,
        "discovery": {
            "freshSeconds": 900, "retentionSeconds": 86400, "keepPerConnection": 5,
            "defaultMaxTopics": 20000, "hardMaxTopics": 50000,
            "visibilityAttestations": attestations
        }
    });
    serde_json::to_string(&json!({
        "apiVersion": "v1", "kind": "ConfigMap",
        "metadata": {"name": "weirkeeper-policy", "namespace": "logweir-system"},
        "data": {"policy.json": serde_json::to_string(&policy).expect("policy")}
    }))
    .expect("a serialisable ConfigMap")
}

fn attestation(principal: &str, cluster_id: &str, expires: &str) -> Value {
    json!({
        "id": "att-orders-prod", "namespace": NS, "kafkaCluster": "source",
        "clusterId": cluster_id, "principal": principal,
        "attestedBy": "platform-admin@example.invalid",
        "attestedAt": "2026-09-15T00:00:00Z", "expiresAt": expires,
        "statement": "User:backup has DESCRIBE on literal Topic:* with no DENY"
    })
}

async fn attested_status(attestations: Value) -> Value {
    let entries = vec![TopicEntry::new("orders", 6)];
    let inventory = inventory_of(&entries, counts_for(&entries, 0));
    let (client, _r, bodies) = mock_client_recording_bodies(finished_routes(
        relay_log(&entries, &inventory),
        vec![
            Route {
                method: "POST",
                path_suffix: "/configmaps",
                status: 201,
                body: "{}".to_string(),
            },
            Route {
                method: "GET",
                path_suffix: "/configmaps/weirkeeper-policy",
                status: 200,
                body: policy_body(attestations),
            },
        ],
    ));
    td::reconcile_discovery(
        &running(),
        &context(
            client,
            Some((
                "logweir-system".to_string(),
                "weirkeeper-policy".to_string(),
            )),
        ),
    )
    .await
    .expect("the reconcile answers");
    status_of(&bodies)
}

/// An administrator attestation out of the installation policy `ConfigMap` —
/// which only a release-namespace administrator can write — is the ONE route to
/// `attestedComplete`, and it is recorded as an attribution and never as a
/// verification.
#[tokio::test]
async fn an_administrator_attestation_is_the_only_route_to_attested_complete() {
    let status = attested_status(json!([attestation(
        PRINCIPAL,
        CLUSTER_ID,
        "2026-12-15T00:00:00Z"
    )]))
    .await;
    assert_eq!(
        status["result"]["visibility"]["state"],
        json!("attestedComplete")
    );
    assert_eq!(
        status["result"]["visibility"]["attestation"],
        json!("att-orders-prod"),
        "the status records WHICH attestation applied; who signed it lives in the policy"
    );
    assert_eq!(
        status["result"]["visibility"]["basis"],
        json!(["administratorAttestation"])
    );
}

/// An attestation for a DIFFERENT principal does not apply, and the mismatch is
/// recorded rather than silently dropped — "attested" drifting onto the wrong
/// credential is the failure this basis entry exists to make visible.
#[tokio::test]
async fn an_attestation_for_another_principal_or_cluster_does_not_apply() {
    let wrong_principal = attested_status(json!([attestation(
        "User:someone-else",
        CLUSTER_ID,
        "2026-12-15T00:00:00Z"
    )]))
    .await;
    assert_eq!(
        wrong_principal["result"]["visibility"]["state"],
        json!("unknown")
    );
    assert!(
        wrong_principal["result"]["visibility"]["basis"]
            .as_array()
            .expect("a basis")
            .contains(&json!("attestationPrincipalMismatch")),
        "{wrong_principal}"
    );

    let wrong_cluster = attested_status(json!([attestation(
        PRINCIPAL,
        "some-other-cluster",
        "2026-12-15T00:00:00Z"
    )]))
    .await;
    assert_eq!(
        wrong_cluster["result"]["visibility"]["state"],
        json!("unknown")
    );
    assert!(
        wrong_cluster["result"]["visibility"]["basis"]
            .as_array()
            .expect("a basis")
            .contains(&json!("attestationClusterIdMismatch")),
        "{wrong_cluster}"
    );

    let expired = attested_status(json!([attestation(
        PRINCIPAL,
        CLUSTER_ID,
        "2026-01-01T00:00:00Z"
    )]))
    .await;
    assert_eq!(expired["result"]["visibility"]["state"], json!("unknown"));
    assert!(
        expired["result"]["visibility"]["basis"]
            .as_array()
            .expect("a basis")
            .contains(&json!("attestationExpired")),
        "{expired}"
    );
}

/// A policy document that does not parse FAILS CLOSED: the fail-closed policy
/// carries no attestations, so nothing observed under it can be
/// `attestedComplete`.
#[tokio::test]
async fn an_unreadable_policy_fails_closed_and_cannot_attest() {
    let entries = vec![TopicEntry::new("orders", 6)];
    let inventory = inventory_of(&entries, counts_for(&entries, 0));
    let broken = serde_json::to_string(&json!({
        "apiVersion": "v1", "kind": "ConfigMap",
        "metadata": {"name": "weirkeeper-policy", "namespace": "logweir-system"},
        "data": {"policy.json": "{not json"}
    }))
    .expect("a serialisable ConfigMap");
    let (client, _r, bodies) = mock_client_recording_bodies(finished_routes(
        relay_log(&entries, &inventory),
        vec![
            Route {
                method: "POST",
                path_suffix: "/configmaps",
                status: 201,
                body: "{}".to_string(),
            },
            Route {
                method: "GET",
                path_suffix: "/configmaps/weirkeeper-policy",
                status: 200,
                body: broken,
            },
        ],
    ));
    td::reconcile_discovery(
        &running(),
        &context(
            client,
            Some((
                "logweir-system".to_string(),
                "weirkeeper-policy".to_string(),
            )),
        ),
    )
    .await
    .expect("the reconcile answers");

    let status = status_of(&bodies);
    assert_eq!(status["result"]["visibility"]["state"], json!("unknown"));
    assert_eq!(status["result"]["visibility"]["attestation"], Value::Null);
}

/// **Timeout.** A Job the API server failed with `DeadlineExceeded` is a FAILED
/// discovery naming the deadline, distinguishable from an empty one.
#[tokio::test]
async fn a_job_that_hit_its_deadline_is_a_failed_discovery() {
    let job = json!({
        "apiVersion": "batch/v1", "kind": "Job",
        "metadata": {"name": JOB, "namespace": NS, "uid": JOB_UID,
                     "creationTimestamp": "2026-09-16T11:58:30Z",
                     "ownerReferences": [{"apiVersion": "logweir.dev/v1alpha1",
                         "kind": "TopicDiscovery", "name": NAME, "uid": UID,
                         "controller": true, "blockOwnerDeletion": true}]},
        "spec": {"template": {"spec": {"containers": [], "restartPolicy": "Never"}}},
        "status": {"conditions": [{"type": "Failed", "status": "True",
                    "reason": "DeadlineExceeded",
                    "lastProbeTime": "2026-09-16T11:59:30Z",
                    "lastTransitionTime": "2026-09-16T11:59:30Z"}]}
    });
    let routes = vec![
        Route {
            method: "GET",
            path_suffix: JOB_PATH,
            status: 200,
            body: job.to_string(),
        },
        Route {
            method: "GET",
            path_suffix: "/pods",
            status: 200,
            body: pod_list(vec![]),
        },
        Route {
            method: "GET",
            path_suffix: PLAN_PATH,
            status: 200,
            body: plan_config_map(UID, true, PLAN_SHA),
        },
        Route {
            method: "PATCH",
            path_suffix: STATUS_PATH,
            status: 200,
            body: discovery_body(&running()),
        },
        Route {
            method: "PATCH",
            path_suffix: JOB_PATH,
            status: 200,
            body: job.to_string(),
        },
    ];
    let (client, _r, bodies) = mock_client_recording_bodies(routes);
    let outcome = td::reconcile_discovery(&running(), &context(client, None))
        .await
        .expect("the reconcile answers");

    assert_eq!(outcome.phase, PHASE_FAILED);
    assert_eq!(outcome.reason, CheckCode::DeadlineExceeded.as_str());
    assert_eq!(outcome.chunks_written, 0, "a timeout stores no inventory");
    let status = status_of(&bodies);
    assert_eq!(
        status["result"],
        Value::Null,
        "there is no result to publish"
    );
    assert_eq!(status["conditions"][0]["reason"], json!("DeadlineExceeded"));
}

/// **Internal topics.** Excluded by default, counted separately, and NOT in the
/// stored chunks — the count is what tells a user they exist.
#[tokio::test]
async fn internal_topics_are_excluded_by_default_and_counted() {
    let entries = vec![TopicEntry::new("orders", 6)];
    let inventory = inventory_of(&entries, counts_for(&entries, 2));
    let (client, _r, bodies) = mock_client_recording_bodies(finished_routes(
        relay_log(&entries, &inventory),
        vec![Route {
            method: "POST",
            path_suffix: "/configmaps",
            status: 201,
            body: "{}".to_string(),
        }],
    ));
    td::reconcile_discovery(&running(), &context(client, None))
        .await
        .expect("the reconcile answers");

    let status = status_of(&bodies);
    assert_eq!(status["result"]["counts"]["internalExcluded"], json!(2));
    assert_eq!(status["result"]["counts"]["returned"], json!(1));
    assert_eq!(status["result"]["counts"]["listed"], json!(3));
    let stored = body_of(&bodies, "POST", "/configmaps")["data"][chunks::CHUNK_KEY]
        .as_str()
        .expect("a chunk")
        .to_string();
    assert!(
        !stored.contains("__"),
        "an excluded internal topic is not stored: {stored}"
    );
    // And the rule itself, which is a NAME rule and nothing cleverer.
    assert!(TopicEntry::name_is_internal("__consumer_offsets"));
    assert!(
        !TopicEntry::name_is_internal("_schemas"),
        "`_schemas` is configurable and is never guessed"
    );
}

/// **Refresh.** A refresh is a new object; the previous one is terminal and is
/// not touched. The route table holds NOTHING for it, so the double panics if
/// the reconciler read or wrote it.
#[tokio::test]
async fn a_terminal_discovery_is_never_reconciled_again() {
    let done = discovery(
        json!({
            "phase": PHASE_SUCCEEDED, "reason": "Succeeded",
            "observedAt": "2026-09-16T11:59:00Z",
            "result": {"format": TOPIC_INVENTORY_FORMAT,
                       "counts": {"listed": 2, "returned": 2, "internalExcluded": 0, "errored": 0},
                       "visibility": {"state": "unknown"}}
        }),
        json!({}),
    );
    // AN EMPTY ROUTE TABLE. Any call at all panics.
    let (client, recorder, _b) = mock_client_recording_bodies(vec![]);
    let outcome = td::reconcile_discovery(&done, &context(client, None))
        .await
        .expect("the reconcile answers");

    assert_eq!(outcome.phase, PHASE_SUCCEEDED);
    assert!(outcome.is_terminal());
    assert!(
        calls(&recorder).is_empty(),
        "a terminal discovery costs one guard and no API call: {:?}",
        calls(&recorder)
    );
}

/// **Refresh, the other half.** While a NEW observation is running, its own
/// status patch carries no `result` key at all — a merge patch with no `result`
/// leaves whatever is there alone, which is what "the old result stays readable
/// until the new one lands" means for a consumer paging an object.
///
/// MUTANT: write `result: null` (or an empty result) on a `Running` patch. The
/// old inventory would vanish the instant a refresh started.
#[tokio::test]
async fn a_running_pass_never_writes_a_result_and_never_writes_a_chunk() {
    let routes = vec![
        Route {
            method: "GET",
            path_suffix: JOB_PATH,
            status: 200,
            body: job_object(None, UID).to_string(),
        },
        Route {
            method: "GET",
            path_suffix: "/pods",
            status: 200,
            body: pod_list(vec![pod_object(
                POD,
                Some(JOB_UID),
                json!({"phase": "Running"}),
            )]),
        },
        Route {
            method: "GET",
            path_suffix: PLAN_PATH,
            status: 200,
            body: plan_config_map(UID, true, PLAN_SHA),
        },
        Route {
            method: "PATCH",
            path_suffix: STATUS_PATH,
            status: 200,
            body: discovery_body(&running()),
        },
    ];
    // NO POST ROUTE, NO pods/log ROUTE. A running check has no end frame yet,
    // so reading its stdout would always be `ResultUnreadable`; the framework
    // does not read it and the double proves the reconciler did not either.
    let (client, _r, bodies) = mock_client_recording_bodies(routes);
    let outcome = td::reconcile_discovery(&running(), &context(client, None))
        .await
        .expect("the reconcile answers");

    assert_eq!(outcome.phase, PHASE_RUNNING);
    assert!(!outcome.ttl_patched, "an unfinished Job keeps its pod");
    let status = status_of(&bodies);
    assert!(
        status.get("result").is_none(),
        "a Running patch carries no `result` key: {status}"
    );
    assert!(
        status.get("binding").is_none(),
        "the binding is written once, when the Job is created, and never refreshed: {status}"
    );
}

/// **Credential rotation.** The Job's environment names the Secret and key the
/// connection resolves to NOW, so a re-run picks up a rotated reference; the
/// controller reads no Secret value at any point.
#[tokio::test]
async fn a_re_run_projects_the_secret_the_connection_resolves_to_now() {
    let rotated = serde_json::to_string(&json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "KafkaCluster",
        "metadata": {"name": "source", "namespace": NS, "uid": CLUSTER_UID,
                     "generation": 4, "resourceVersion": "9"},
        "spec": {"bootstrapServers": ["kafka-source:9092"], "role": "source",
                 "auth": {"mode": "scramSha512", "username": "backup-v2", "tls": false,
                          "secretRef": {"name": "kafka-source-scram-v2",
                                        "passwordKey": "password-2026-09"}}}
    }))
    .expect("a serialisable KafkaCluster");
    let mut routes = start_routes(vec![]);
    routes[1].body = rotated;
    let (client, _r, bodies) = mock_client_recording_bodies(routes);
    td::reconcile_discovery(&fresh(), &context(client, None))
        .await
        .expect("the reconcile answers");

    let job = body_of(&bodies, "POST", "/jobs");
    let env = job["spec"]["template"]["spec"]["containers"][0]["env"]
        .as_array()
        .expect("the runner container carries env")
        .clone();
    let password = env
        .iter()
        .find(|e| e["name"] == json!("LOGWEIR_SOURCE_PASSWORD"))
        .expect("the SASL password is projected");
    assert_eq!(
        password["valueFrom"]["secretKeyRef"]["name"],
        json!("kafka-source-scram-v2")
    );
    assert_eq!(
        password["valueFrom"]["secretKeyRef"]["key"],
        json!("password-2026-09")
    );
    assert_eq!(
        password.get("value"),
        None,
        "the controller writes a REFERENCE; the kubelet projects the value"
    );

    // And the binding records the generation and principal a later reader
    // compares against to decide the result is stale (D2 §5.7).
    let status = status_of(&bodies);
    assert_eq!(status["binding"]["connectionGeneration"], json!(4));
    assert_eq!(status["binding"]["principal"], json!("User:backup-v2"));
}

// ---------------------------------------------------------------------------
// The seams
// ---------------------------------------------------------------------------

/// **D-SEAMS S6, defect `SEC-PODLOG`.** A pod wearing
/// `batch.kubernetes.io/job-name` whose controller owner is a DIFFERENT Job is
/// never read. The route table has no `pods/log` route for it, so the double
/// panics if it were.
///
/// MUTANT: match pods by label instead of by controller-owner UID. The impostor
/// would be read and this test panics inside the double.
#[tokio::test]
async fn a_label_wearing_pod_that_the_job_does_not_own_is_never_read() {
    let entries = vec![TopicEntry::new("orders", 6)];
    let inventory = inventory_of(&entries, counts_for(&entries, 0));
    let mut routes = finished_routes(
        relay_log(&entries, &inventory),
        vec![Route {
            method: "POST",
            path_suffix: "/configmaps",
            status: 201,
            body: "{}".to_string(),
        }],
    );
    // The impostor is FIRST in the list, so a label-only match would take it.
    routes[1].body = pod_list(vec![
        pod_object(IMPOSTOR_POD, Some(OTHER_JOB_UID), terminated(0)),
        pod_object(POD, Some(JOB_UID), terminated(0)),
    ]);
    let (client, recorder, _b) = mock_client_recording_bodies(routes);
    td::reconcile_discovery(&running(), &context(client, None))
        .await
        .expect("the reconcile answers");

    let seen = calls(&recorder);
    assert!(
        seen.iter().any(|c| c.contains(POD_LOG_PATH)),
        "the owned pod's log IS read: {seen:?}"
    );
    assert!(
        !seen.iter().any(|c| c.contains(IMPOSTOR_LOG_PATH)),
        "the impostor's log is never read: {seen:?}"
    );
}

/// An ownerless pod wearing the label is not adopted either, and with no owned
/// pod there is no relay: `ResultUnreadable`, and nothing is invented from the
/// exit code.
#[tokio::test]
async fn an_ownerless_label_wearing_pod_leaves_the_result_unreadable() {
    let mut routes = finished_routes(String::new(), vec![]);
    routes[1].body = pod_list(vec![pod_object(IMPOSTOR_POD, None, terminated(0))]);
    // The log route is REMOVED: nothing may be read at all.
    routes.remove(2);
    let (client, _r, bodies) = mock_client_recording_bodies(routes);
    let outcome = td::reconcile_discovery(&running(), &context(client, None))
        .await
        .expect("the reconcile answers");

    assert_eq!(outcome.phase, PHASE_FAILED);
    assert_eq!(outcome.reason, CheckCode::ResultUnreadable.as_str());
    assert_eq!(status_of(&bodies)["result"], Value::Null);
}

/// A result `ConfigMap` owned by something else is NEVER adopted: the digest a
/// status publishes must be over bytes this pass produced.
#[tokio::test]
async fn a_foreign_owner_result_config_map_is_never_adopted() {
    let entries = vec![TopicEntry::new("orders", 6)];
    let inventory = inventory_of(&entries, counts_for(&entries, 0));
    let foreign = serde_json::to_string(&json!({
        "apiVersion": "v1", "kind": "ConfigMap",
        "metadata": {
            "name": format!("{JOB}-r000"), "namespace": NS,
            "annotations": {chunks::SHA256_ANNOTATION: "sha256:dead"},
            "ownerReferences": [{"apiVersion": "logweir.dev/v1alpha1",
                "kind": "TopicDiscovery", "name": "someone-else",
                "uid": "11111111-2222-3333-4444-555555555555",
                "controller": true, "blockOwnerDeletion": true}]
        },
        "immutable": true,
        "data": {chunks::CHUNK_KEY: "not-yours\t1\t-\n"}
    }))
    .expect("a serialisable ConfigMap");
    let mut routes = finished_routes(
        relay_log(&entries, &inventory),
        vec![
            Route {
                method: "POST",
                path_suffix: "/configmaps",
                status: 409,
                body: conflict(
                    "configmaps \\\"lwc-td-288fecc03251c1c31851-r000\\\" already exists",
                ),
            },
            Route {
                method: "GET",
                path_suffix: "/configmaps/lwc-td-288fecc03251c1c31851-r000",
                status: 200,
                body: foreign,
            },
        ],
    );
    // The plan route must still win for `…-plan`; it is earlier in the table.
    routes.rotate_left(0);
    let (client, _r, bodies) = mock_client_recording_bodies(routes);
    let outcome = td::reconcile_discovery(&running(), &context(client, None))
        .await
        .expect("the reconcile answers");

    assert_eq!(outcome.phase, PHASE_FAILED);
    assert_eq!(outcome.reason, CheckCode::ResultStorageConflict.as_str());
    assert_eq!(
        status_of(&bodies)["result"],
        Value::Null,
        "nothing is published over somebody else's bytes"
    );
}

/// A plan `ConfigMap` this subject does not own cannot supply the digest a
/// relay is verified against — the verification would be against nothing.
#[tokio::test]
async fn a_plan_config_map_owned_by_another_subject_is_a_terminal_conflict() {
    let entries = vec![TopicEntry::new("orders", 6)];
    let inventory = inventory_of(&entries, counts_for(&entries, 0));
    let mut routes = finished_routes(relay_log(&entries, &inventory), vec![]);
    routes[3].body = plan_config_map("11111111-2222-3333-4444-555555555555", true, PLAN_SHA);
    let (client, _r, _b) = mock_client_recording_bodies(routes);
    let outcome = td::reconcile_discovery(&running(), &context(client, None))
        .await
        .expect("the reconcile answers");
    assert_eq!(outcome.phase, PHASE_FAILED);
    assert_eq!(outcome.reason, CheckCode::CheckPlanConflict.as_str());
}

/// **The commit ordering.** Chunks, then the status patch that indexes them,
/// then the Job's TTL. Nothing else is an acceptable order.
///
/// MUTANT: drop the TTL patch, or move it before the status write. The first
/// leaves finished check Jobs forever; the second lets garbage collection race
/// the log read a restart still has to make.
#[tokio::test]
async fn the_chunks_are_written_before_the_status_and_the_ttl_after_it() {
    let entries = vec![TopicEntry::new("orders", 6)];
    let inventory = inventory_of(&entries, counts_for(&entries, 0));
    let (client, recorder, bodies) = mock_client_recording_bodies(finished_routes(
        relay_log(&entries, &inventory),
        vec![Route {
            method: "POST",
            path_suffix: "/configmaps",
            status: 201,
            body: "{}".to_string(),
        }],
    ));
    let outcome = td::reconcile_discovery(&running(), &context(client, None))
        .await
        .expect("the reconcile answers");
    assert!(outcome.ttl_patched);

    let seen = calls(&recorder);
    let chunk = index_of(&seen, "POST", "/configmaps");
    let commit = index_of(&seen, "PATCH", "/topicdiscoveries/");
    let ttl = index_of(&seen, "PATCH", JOB_PATH);
    assert!(chunk < commit, "chunks before the commit: {seen:?}");
    assert!(commit < ttl, "the TTL after the commit: {seen:?}");
    assert_eq!(
        body_of(&bodies, "PATCH", JOB_PATH),
        json!({"spec": {"ttlSecondsAfterFinished": 600}}),
        "the TTL patch is the framework's, and touches nothing else"
    );
}

/// **D-SEAMS S7.** Every status write is a merge PATCH whose BODY carries
/// `metadata.resourceVersion` as the update precondition — never a `PUT`, which
/// the API server authorises as `update` and this role grants on nothing.
#[tokio::test]
async fn every_status_write_is_a_resource_version_preconditioned_merge_patch() {
    let entries = vec![TopicEntry::new("orders", 6)];
    let inventory = inventory_of(&entries, counts_for(&entries, 0));
    let (client, recorder, bodies) = mock_client_recording_bodies(finished_routes(
        relay_log(&entries, &inventory),
        vec![Route {
            method: "POST",
            path_suffix: "/configmaps",
            status: 201,
            body: "{}".to_string(),
        }],
    ));
    td::reconcile_discovery(&running(), &context(client, None))
        .await
        .expect("the reconcile answers");

    let body = body_of(&bodies, "PATCH", "/topicdiscoveries/");
    assert_eq!(body["metadata"]["resourceVersion"], json!("4242"));
    assert_eq!(body["metadata"]["name"], json!(NAME));
    assert!(
        calls(&recorder).iter().all(|c| !c.starts_with("PUT")),
        "no PUT anywhere: {:?}",
        calls(&recorder)
    );
}

/// A 409 on the status write is the precondition WORKING: nothing else is
/// written, and in particular the Job's TTL is not patched, because the relay
/// on the pod is what the next pass still has to read.
#[tokio::test]
async fn a_status_conflict_leaves_the_pod_alive_for_the_next_pass() {
    let entries = vec![TopicEntry::new("orders", 6)];
    let inventory = inventory_of(&entries, counts_for(&entries, 0));
    let mut routes = finished_routes(
        relay_log(&entries, &inventory),
        vec![Route {
            method: "POST",
            path_suffix: "/configmaps",
            status: 201,
            body: "{}".to_string(),
        }],
    );
    routes[4] = Route {
        method: "PATCH",
        path_suffix: STATUS_PATH,
        status: 409,
        body: r#"{"kind":"Status","apiVersion":"v1","status":"Failure","message":"conflict","reason":"Conflict","code":409}"#.to_string(),
    };
    let (client, recorder, _b) = mock_client_recording_bodies(routes);
    let outcome = td::reconcile_discovery(&running(), &context(client, None))
        .await
        .expect("a 409 is not an error");

    assert!(!outcome.ttl_patched);
    assert!(
        !calls(&recorder)
            .iter()
            .any(|c| c.starts_with("PATCH") && c.contains(JOB_PATH)),
        "no TTL before a commit: {:?}",
        calls(&recorder)
    );
}

// ---------------------------------------------------------------------------
// Cancellation, staleness and the result guards
// ---------------------------------------------------------------------------

/// **Cancel.** The owned Job's deadline is collapsed, the object becomes
/// `Cancelled`, and NO chunk is written — the route table has no `POST`.
#[tokio::test]
async fn a_cancel_collapses_the_owned_jobs_deadline_and_stores_nothing() {
    let cancelled = discovery(
        json!({"phase": PHASE_RUNNING, "reason": "PodNotStarted", "jobRef": {"name": JOB}}),
        json!({}),
    );
    let mut object: Value = serde_json::to_value(&cancelled).expect("serialisable");
    object["spec"]["cancelRequested"] = json!(true);
    let cancelled: TopicDiscovery = serde_json::from_value(object).expect("a TopicDiscovery");

    let routes = vec![
        Route {
            method: "GET",
            path_suffix: JOB_PATH,
            status: 200,
            body: job_object(None, UID).to_string(),
        },
        Route {
            method: "PATCH",
            path_suffix: JOB_PATH,
            status: 200,
            body: job_object(None, UID).to_string(),
        },
        Route {
            method: "PATCH",
            path_suffix: STATUS_PATH,
            status: 200,
            body: discovery_body(&running()),
        },
    ];
    let (client, _r, bodies) = mock_client_recording_bodies(routes);
    let outcome = td::reconcile_discovery(&cancelled, &context(client, None))
        .await
        .expect("the reconcile answers");

    assert_eq!(outcome.phase, PHASE_CANCELLED);
    assert_eq!(outcome.reason, CheckCode::CancelRequested.as_str());
    assert_eq!(outcome.chunks_written, 0);
    assert_eq!(
        body_of(&bodies, "PATCH", JOB_PATH),
        json!({"spec": {"activeDeadlineSeconds": 1}}),
        "cancel is a deadline collapse and never a delete: this role grants `delete` on nothing"
    );
}

/// A Job carrying the same name but a DIFFERENT controller owner is never
/// patched: a name is not an identity. The route table holds no `PATCH` for the
/// Job, so the double panics if it were attempted.
#[tokio::test]
async fn a_cancel_never_touches_a_job_this_object_does_not_own() {
    let cancelled = discovery(
        json!({"phase": PHASE_RUNNING, "reason": "PodNotStarted", "jobRef": {"name": JOB}}),
        json!({}),
    );
    let mut object: Value = serde_json::to_value(&cancelled).expect("serialisable");
    object["spec"]["cancelRequested"] = json!(true);
    let cancelled: TopicDiscovery = serde_json::from_value(object).expect("a TopicDiscovery");

    let routes = vec![
        Route {
            method: "GET",
            path_suffix: JOB_PATH,
            status: 200,
            body: job_object(None, "11111111-2222-3333-4444-555555555555").to_string(),
        },
        Route {
            method: "PATCH",
            path_suffix: STATUS_PATH,
            status: 200,
            body: discovery_body(&running()),
        },
    ];
    let (client, recorder, _b) = mock_client_recording_bodies(routes);
    let outcome = td::reconcile_discovery(&cancelled, &context(client, None))
        .await
        .expect("the reconcile answers");

    assert_eq!(outcome.phase, PHASE_CANCELLED);
    assert!(
        !calls(&recorder)
            .iter()
            .any(|c| c.starts_with("PATCH") && c.contains(JOB_PATH)),
        "a foreign Job is never touched: {:?}",
        calls(&recorder)
    );
}

/// A non-terminal object whose Job has vanished becomes `Failed/Stalled` — but
/// only after twice its own budget plus five minutes, because "the Job is not
/// there" is also what a stale watch cache looks like.
#[tokio::test]
async fn a_vanished_job_becomes_stalled_only_after_the_grace() {
    let long_ago = discovery(
        json!({"phase": PHASE_RUNNING, "reason": "PodNotStarted", "jobRef": {"name": JOB},
               "queuedAt": "2026-09-16T10:00:00Z"}),
        json!({}),
    );
    let routes = vec![
        Route {
            method: "GET",
            path_suffix: JOB_PATH,
            status: 404,
            body: not_found("jobs not found"),
        },
        Route {
            method: "PATCH",
            path_suffix: STATUS_PATH,
            status: 200,
            body: discovery_body(&running()),
        },
    ];
    let (client, _r, bodies) = mock_client_recording_bodies(routes);
    let outcome = td::reconcile_discovery(&long_ago, &context(client, None))
        .await
        .expect("the reconcile answers");
    assert_eq!(outcome.phase, PHASE_FAILED);
    assert_eq!(outcome.reason, CheckCode::Stalled.as_str());
    assert!(status_of(&bodies)["message"]
        .as_str()
        .is_some_and(|m| m.contains("no longer exists")));

    // And inside the grace it is NOT stalled, and creates nothing. The instant
    // is taken from THIS machine's clock, because the reconciler reads the real
    // one: a fixture timestamp would make "inside the grace" a claim about the
    // day the fixture was written.
    let recent = discovery(
        json!({"phase": PHASE_RUNNING, "reason": "PodNotStarted", "jobRef": {"name": JOB},
               "queuedAt": Utc::now().to_rfc3339()}),
        json!({}),
    );
    let (client, recorder, _b) = mock_client_recording_bodies(vec![Route {
        method: "GET",
        path_suffix: JOB_PATH,
        status: 404,
        body: not_found("jobs not found"),
    }]);
    let outcome = td::reconcile_discovery(&recent, &context(client, None))
        .await
        .expect("the reconcile answers");
    assert_eq!(outcome.phase, PHASE_RUNNING);
    assert_eq!(
        calls(&recorder).len(),
        1,
        "inside the grace nothing is written: {:?}",
        calls(&recorder)
    );
}

/// A result document whose counts disagree with the frames it arrived with is
/// `ResultUnreadable`. The frames are the MEASURED half — the decoder proved
/// their count and digest — so a document that contradicts them is not adopted.
///
/// MUTANT: copy `counts` and `topicsSha256` straight out of the runner's
/// document. Both assertions here go green for a runner that lies.
#[tokio::test]
async fn a_result_document_that_contradicts_its_frames_is_result_unreadable() {
    let entries = vec![TopicEntry::new("orders", 6)];
    let mut inventory = inventory_of(&entries, counts_for(&entries, 0));
    inventory.counts.returned = 5_003;
    let (client, _r, _b) =
        mock_client_recording_bodies(finished_routes(relay_log(&entries, &inventory), vec![]));
    let outcome = td::reconcile_discovery(&running(), &context(client, None))
        .await
        .expect("the reconcile answers");
    assert_eq!(outcome.phase, PHASE_FAILED);
    assert_eq!(outcome.reason, CheckCode::ResultUnreadable.as_str());
    assert_eq!(outcome.chunks_written, 0, "nothing is stored");
}

/// A `topicsSha256` the runner invented is refused for the same reason: the
/// status publishes only a digest the controller computed itself.
#[tokio::test]
async fn a_topics_digest_the_runner_invented_is_refused() {
    let entries = vec![TopicEntry::new("orders", 6)];
    let mut inventory = inventory_of(&entries, counts_for(&entries, 0));
    inventory.topics_sha256 =
        "sha256:0000000000000000000000000000000000000000000000000000000000000000".to_string();
    let (client, _r, _b) =
        mock_client_recording_bodies(finished_routes(relay_log(&entries, &inventory), vec![]));
    let outcome = td::reconcile_discovery(&running(), &context(client, None))
        .await
        .expect("the reconcile answers");
    assert_eq!(outcome.phase, PHASE_FAILED);
    assert_eq!(outcome.reason, CheckCode::ResultUnreadable.as_str());
}

/// A truncated inventory records the reason in D2 §5.1's CamelCase spelling and
/// can never be `attestedComplete`, however good the attestation is.
#[tokio::test]
async fn a_truncated_inventory_records_its_reason_and_cannot_be_attested() {
    let entries = vec![TopicEntry::new("orders", 6)];
    let mut inventory = inventory_of(&entries, counts_for(&entries, 0));
    inventory.truncated = true;
    inventory.truncation_reason = Some(TruncationReason::RelayLimit);
    let (client, _r, bodies) = mock_client_recording_bodies(finished_routes(
        relay_log(&entries, &inventory),
        vec![
            Route {
                method: "POST",
                path_suffix: "/configmaps",
                status: 201,
                body: "{}".to_string(),
            },
            Route {
                method: "GET",
                path_suffix: "/configmaps/weirkeeper-policy",
                status: 200,
                body: policy_body(json!([attestation(
                    PRINCIPAL,
                    CLUSTER_ID,
                    "2026-12-15T00:00:00Z"
                )])),
            },
        ],
    ));
    td::reconcile_discovery(
        &running(),
        &context(
            client,
            Some((
                "logweir-system".to_string(),
                "weirkeeper-policy".to_string(),
            )),
        ),
    )
    .await
    .expect("the reconcile answers");

    let status = status_of(&bodies);
    assert_eq!(status["result"]["truncated"], json!(true));
    assert_eq!(status["result"]["truncationReason"], json!("RelayLimit"));
    assert_ne!(
        status["result"]["visibility"]["state"],
        json!("attestedComplete"),
        "an incomplete listing is never a completeness claim: {status}"
    );
}

/// `observedAt` is the RUNNER CONTAINER's `finishedAt` and never `now`, so a
/// re-read of the same Job produces a byte-identical patch. That is what stopped
/// the `KafkaCluster` probe's measured 3,388-reconciles-in-ninety-seconds loop.
#[tokio::test]
async fn observed_at_is_the_runners_own_instant_and_not_a_clock_read() {
    let entries = vec![TopicEntry::new("orders", 6)];
    let inventory = inventory_of(&entries, counts_for(&entries, 0));
    let (client, _r, bodies) = mock_client_recording_bodies(finished_routes(
        relay_log(&entries, &inventory),
        vec![Route {
            method: "POST",
            path_suffix: "/configmaps",
            status: 201,
            body: "{}".to_string(),
        }],
    ));
    td::reconcile_discovery(&running(), &context(client, None))
        .await
        .expect("the reconcile answers");

    let status = status_of(&bodies);
    assert_eq!(status["observedAt"], json!("2026-09-16T11:59:00Z"));
    assert_eq!(status["startedAt"], json!("2026-09-16T11:58:45Z"));
    // `freshUntil` is `observedAt` plus the default 900 s window.
    assert_eq!(status["freshUntil"], json!("2026-09-16T12:14:00Z"));
}

// ---------------------------------------------------------------------------
// The schema bound on the chunk index (review M1)
// ---------------------------------------------------------------------------

/// **The reviewer's measured case.** A runner that honours its relay budget but
/// not its plan's `maxTopics` relays 165,000 frames — 5,115,000 bytes, inside
/// the plan's own 6 MiB budget, inside the decoder's budget and inside the 8 MiB
/// `pods/log` read. Split unbounded that is **66** chunks, three over the CRD's
/// `maxItems: 64`, so 66 `ConfigMap`s land in etcd and then the `/status` PATCH
/// is refused 422: the object never reaches a terminal phase, the Job's TTL is
/// never set, and `error_policy` requeues the same doomed pass every thirty
/// seconds forever.
///
/// The entries are cut to the plan's ceiling BEFORE anything is written, so the
/// chunks, the index and `topicsSha256` are one set and the API's §5.6
/// integrity triple still holds.
///
/// MUTANT: split `relay.topics` instead of the clamped slice. The chunk count,
/// the `truncated` flag and the digest each fail.
#[tokio::test]
async fn a_relay_that_ignores_its_plan_is_cut_to_what_the_status_can_index() {
    let entries: Vec<TopicEntry> = (0..165_000)
        .map(|i| TopicEntry::new(&format!("bulk-{i:06}"), 3))
        .collect();
    let inventory = inventory_of(&entries, counts_for(&entries, 0));
    let (client, _r, bodies) = mock_client_recording_bodies(finished_routes(
        relay_log(&entries, &inventory),
        vec![Route {
            method: "POST",
            path_suffix: "/configmaps",
            status: 201,
            body: "{}".to_string(),
        }],
    ));
    let outcome = td::reconcile_discovery(&running(), &context(client, None))
        .await
        .expect("the reconcile answers");

    // The request's `maxTopics` is 20,000, so 20,000 entries and eight chunks.
    assert_eq!(outcome.phase, PHASE_SUCCEEDED);
    assert_eq!(outcome.chunks_written, 8);
    assert!(
        outcome.chunks_written <= td::MAX_STATUS_CHUNKS,
        "a status can index at most {} chunks",
        td::MAX_STATUS_CHUNKS
    );

    let written = bodies_of(&bodies, "POST", "/configmaps");
    assert_eq!(written.len(), 8, "no ConfigMap is written past the bound");

    let status = status_of(&bodies);
    assert_eq!(status["result"]["counts"]["returned"], json!(20_000));
    assert_eq!(
        status["result"]["counts"]["listed"],
        json!(165_000),
        "what the broker listed is unchanged; only what is STORED was cut"
    );
    assert_eq!(status["result"]["truncated"], json!(true));
    assert_eq!(status["result"]["truncationReason"], json!("MaxTopics"));
    assert_eq!(
        status["result"]["chunks"]
            .as_array()
            .expect("an index")
            .len(),
        8
    );
    assert_eq!(
        status["result"]["topicsSha256"],
        json!(topic_tsv_sha256(&entries[..20_000])),
        "the digest is over the entries that were STORED, so a reader that fetches the chunks \
         reproduces it"
    );
    assert!(
        status["message"]
            .as_str()
            .is_some_and(|m| m.contains("165000") && m.contains("truncated")),
        "the message names what happened: {status}"
    );
}

/// The bound, at the boundary, without an API server.
#[test]
fn the_chunk_index_can_never_exceed_the_schemas_max_items() {
    // The plan ceiling binds at the contract's own maximum: 50,000 entries is
    // twenty chunks, nowhere near the schema bound.
    assert_eq!(td::storable_entry_ceiling(50_000), 50_000);
    // And the schema backstop binds only if a policy ever raised `hardMaxTopics`
    // past it.
    assert_eq!(
        td::storable_entry_ceiling(500_000),
        td::MAX_STATUS_CHUNKS * chunks::MAX_CHUNK_LINES
    );
    assert_eq!(td::storable_entry_ceiling(500_000), 160_000);

    for count in [0usize, 1, 2_500, 2_501, 160_000, 165_000] {
        let entries: Vec<TopicEntry> = (0..count)
            .map(|i| TopicEntry::new(&format!("t-{i:06}"), 1))
            .collect();
        let inventory = inventory_of(&entries, counts_for(&entries, 0));
        let ceiling = td::storable_entry_ceiling(50_000);
        let (clamped, stored) = td::clamp_to_storable(&inventory, &entries, ceiling);
        let split = chunks::split(stored);
        let index = td::chunk_index(stored, &split, "lwc-td-x");
        assert!(
            index.len() <= td::MAX_STATUS_CHUNKS,
            "{count} entries produced {} chunks",
            index.len()
        );
        assert_eq!(index.len(), split.len(), "the index describes the chunks");
        assert_eq!(
            clamped.counts.returned as usize,
            stored.len(),
            "the published count is what was stored"
        );
        assert_eq!(clamped.truncated, count > ceiling);
    }
}

/// Clamping cuts the ENTRIES, never the index alone: a truncated result's
/// digest is still reproducible from the chunks that exist.
#[test]
fn clamping_keeps_the_chunks_the_index_and_the_digest_one_set() {
    let entries: Vec<TopicEntry> = (0..10)
        .map(|i| TopicEntry::new(&format!("t-{i}"), 1))
        .collect();
    let inventory = inventory_of(&entries, counts_for(&entries, 0));
    let (clamped, stored) = td::clamp_to_storable(&inventory, &entries, 4);
    assert_eq!(stored.len(), 4);
    assert_eq!(clamped.counts.returned, 4);
    assert_eq!(clamped.counts.listed, 10, "the broker's own count is kept");
    assert!(clamped.truncated);
    assert_eq!(clamped.truncation_reason, Some(TruncationReason::MaxTopics));
    assert_eq!(clamped.topics_sha256, topic_tsv_sha256(stored));

    // A reason the RUNNER supplied is the more specific fact and is kept.
    let mut relayed = inventory.clone();
    relayed.truncated = true;
    relayed.truncation_reason = Some(TruncationReason::RelayLimit);
    let (kept, _) = td::clamp_to_storable(&relayed, &entries, 4);
    assert_eq!(kept.truncation_reason, Some(TruncationReason::RelayLimit));

    // Inside the ceiling nothing moves.
    let (untouched, all) = td::clamp_to_storable(&inventory, &entries, 10);
    assert_eq!(all.len(), 10);
    assert!(!untouched.truncated);
}

// ---------------------------------------------------------------------------
// The TTL on the Failed path (review M2)
// ---------------------------------------------------------------------------

/// **A terminal status that 409'd did not land**, so this pass's conclusion is
/// not on the server and the pod that still holds the relay must stay alive for
/// the pass that reads the newer object. The route table holds no `PATCH` for
/// the Job, so the double panics if the TTL were set.
///
/// The scenario is two replicas: A classifies `Failed/DeadlineExceeded`, its
/// commit 409s against B's stale `Running`, and if A sets the TTL the TTL
/// controller deletes the Job **and its pod together** 600 s later, taking the
/// relay — and the reason the operator has to act on — with them.
///
/// MUTANT: call `finish_job` unconditionally on the `Failed` arm, which is what
/// the code did before this fix.
#[tokio::test]
async fn a_failed_status_conflict_never_patches_the_ttl() {
    let job = json!({
        "apiVersion": "batch/v1", "kind": "Job",
        "metadata": {"name": JOB, "namespace": NS, "uid": JOB_UID,
                     "creationTimestamp": "2026-09-16T11:58:30Z",
                     "ownerReferences": [{"apiVersion": "logweir.dev/v1alpha1",
                         "kind": "TopicDiscovery", "name": NAME, "uid": UID,
                         "controller": true, "blockOwnerDeletion": true}]},
        "spec": {"template": {"spec": {"containers": [], "restartPolicy": "Never"}}},
        "status": {"conditions": [{"type": "Failed", "status": "True",
                    "reason": "DeadlineExceeded",
                    "lastProbeTime": "2026-09-16T11:59:30Z",
                    "lastTransitionTime": "2026-09-16T11:59:30Z"}]}
    });
    let routes = vec![
        Route {
            method: "GET",
            path_suffix: JOB_PATH,
            status: 200,
            body: job.to_string(),
        },
        Route {
            method: "GET",
            path_suffix: "/pods",
            status: 200,
            body: pod_list(vec![]),
        },
        Route {
            method: "GET",
            path_suffix: PLAN_PATH,
            status: 200,
            body: plan_config_map(UID, true, PLAN_SHA),
        },
        Route {
            method: "PATCH",
            path_suffix: STATUS_PATH,
            status: 409,
            body: r#"{"kind":"Status","apiVersion":"v1","status":"Failure","message":"conflict","reason":"Conflict","code":409}"#.to_string(),
        },
    ];
    let (client, recorder, _b) = mock_client_recording_bodies(routes);
    let outcome = td::reconcile_discovery(&running(), &context(client, None))
        .await
        .expect("a 409 is not an error");

    assert_eq!(outcome.phase, PHASE_FAILED);
    assert!(!outcome.committed, "the terminal status did not land");
    assert!(!outcome.ttl_patched);
    assert!(
        !calls(&recorder)
            .iter()
            .any(|c| c.starts_with("PATCH") && c.contains(JOB_PATH)),
        "no TTL before a commit, on the Failed path as on the Succeeded one: {:?}",
        calls(&recorder)
    );
}

/// The same path with a 200: the TTL IS patched, so the test above is about the
/// conflict and not about the branch never running.
#[tokio::test]
async fn a_failed_status_that_lands_does_patch_the_ttl() {
    let mut routes = finished_routes(String::new(), vec![]);
    routes[1].body = pod_list(vec![]);
    routes.remove(2);
    let (client, _r, _b) = mock_client_recording_bodies(routes);
    let outcome = td::reconcile_discovery(&running(), &context(client, None))
        .await
        .expect("the reconcile answers");
    assert_eq!(outcome.phase, PHASE_FAILED);
    assert!(outcome.committed);
    assert!(outcome.ttl_patched);
}

// ---------------------------------------------------------------------------
// The attestation fails closed on an absent value (review L1)
// ---------------------------------------------------------------------------

/// **An unknown principal is not a wildcard.** The binding's principal reaches
/// the policy matcher as `""` when no binding was ever recorded — reachable
/// when the start-path status patch 409'd — and `""` compares EQUAL to an
/// attestation whose own `principal` is empty. That is a false
/// `attestedComplete` produced by two values being unknown.
///
/// MUTANT: call `attestation_for` instead of `attestation_candidate`.
#[tokio::test]
async fn an_attestation_with_an_empty_principal_never_matches_an_unknown_one() {
    // A discovery whose start-path status patch never landed: no binding.
    let no_binding = discovery(
        json!({"phase": PHASE_RUNNING, "reason": "PodNotStarted", "jobRef": {"name": JOB}}),
        json!({}),
    );
    let entries = vec![TopicEntry::new("orders", 6)];
    let inventory = inventory_of(&entries, counts_for(&entries, 0));
    let blank = json!({
        "id": "att-blank", "namespace": NS, "kafkaCluster": "source",
        "clusterId": CLUSTER_ID, "principal": "",
        "attestedBy": "platform-admin@example.invalid",
        "attestedAt": "2026-09-15T00:00:00Z", "expiresAt": "2026-12-15T00:00:00Z",
        "statement": "an attestation an administrator left half-written"
    });
    let (client, _r, bodies) = mock_client_recording_bodies(finished_routes(
        relay_log(&entries, &inventory),
        vec![
            Route {
                method: "POST",
                path_suffix: "/configmaps",
                status: 201,
                body: "{}".to_string(),
            },
            Route {
                method: "GET",
                path_suffix: "/configmaps/weirkeeper-policy",
                status: 200,
                body: policy_body(json!([blank])),
            },
        ],
    ));
    td::reconcile_discovery(
        &no_binding,
        &context(
            client,
            Some((
                "logweir-system".to_string(),
                "weirkeeper-policy".to_string(),
            )),
        ),
    )
    .await
    .expect("the reconcile answers");

    let status = status_of(&bodies);
    assert_eq!(
        status["result"]["visibility"]["state"],
        json!("unknown"),
        "an unknown principal must never attest: {status}"
    );
    assert_eq!(status["result"]["visibility"]["attestation"], Value::Null);
}

/// The same rule, at the boundary, without an API server.
#[test]
fn an_attestation_candidate_needs_every_value_on_both_sides() {
    let good: Vec<logweir_core::check_contract::Attestation> =
        serde_json::from_value(json!([attestation(
            PRINCIPAL,
            CLUSTER_ID,
            "2026-12-15T00:00:00Z"
        )]))
        .expect("attestations parse");
    assert!(td::attestation_candidate(&good, NS, "source", PRINCIPAL, CLUSTER_ID).is_some());
    // An absent observation value on either side is not a match.
    assert!(td::attestation_candidate(&good, NS, "source", "", CLUSTER_ID).is_none());
    assert!(td::attestation_candidate(&good, NS, "source", PRINCIPAL, "").is_none());
    assert!(td::attestation_candidate(&good, NS, "source", "  ", CLUSTER_ID).is_none());
    assert!(td::attestation_candidate(&good, "", "source", PRINCIPAL, CLUSTER_ID).is_none());

    // And an attestation that names neither is not a candidate for anything.
    let blank: Vec<logweir_core::check_contract::Attestation> =
        serde_json::from_value(json!([attestation("", "", "2026-12-15T00:00:00Z")]))
            .expect("attestations parse");
    assert!(td::attestation_candidate(&blank, NS, "source", PRINCIPAL, CLUSTER_ID).is_none());
    assert!(td::attestation_candidate(&blank, NS, "source", "", "").is_none());
}

// ---------------------------------------------------------------------------
// Pure functions
// ---------------------------------------------------------------------------

/// The policy may LOWER a request's ceiling and never raise it.
#[test]
fn the_policy_ceiling_lowers_a_request_and_never_raises_it() {
    assert_eq!(td::effective_max_topics(20_000, 5_000), 5_000);
    assert_eq!(td::effective_max_topics(1_000, 50_000), 1_000);
    assert_eq!(td::effective_max_topics(50_000, 50_000), 50_000);
    // A nonsense request is clamped rather than trusted.
    assert_eq!(td::effective_max_topics(0, 50_000), 1);
    assert_eq!(td::effective_max_topics(-7, 50_000), 1);
}

/// `freshUntil` is `observedAt` plus the policy window, and nothing else — the
/// other two staleness rules are the API's, because both compare this object
/// against something that changes after it is written.
#[test]
fn fresh_until_is_observed_at_plus_the_policy_window() {
    assert_eq!(
        td::fresh_until(now(), 900),
        Utc.with_ymd_and_hms(2026, 9, 16, 12, 15, 0)
            .single()
            .unwrap()
    );
}

/// D2 §5.1's status spelling of a truncation reason is CamelCase, beside every
/// other reason in this repository; the wire contract's is camelCase. ONE seam
/// maps them, so a third spelling cannot appear.
#[test]
fn a_truncation_reason_has_exactly_one_status_spelling() {
    assert_eq!(
        td::truncation_reason_str(TruncationReason::MaxTopics),
        "MaxTopics"
    );
    assert_eq!(
        td::truncation_reason_str(TruncationReason::RelayLimit),
        "RelayLimit"
    );
    // And the wire form is the OTHER one, which is the whole reason the map
    // exists.
    assert_eq!(
        serde_json::to_value(TruncationReason::MaxTopics).expect("serialisable"),
        json!("maxTopics")
    );
}

/// Retention and the per-connection cohort, each on its own, over a total
/// order — D2 §4.3's `gc.rs` and §5.8.
///
/// NOTHING CALLS THIS YET: the `delete` verb it implies is granted nowhere and
/// `manifest_lint` asserts twice that it is. The rule is tested here so wiring
/// it is a grant plus a call site rather than a design.
#[test]
fn expired_terminal_keeps_the_newest_five_per_connection_and_honours_retention() {
    let at = |minutes: i64| now() - chrono::Duration::minutes(minutes);
    let mut all: Vec<TerminalDiscovery> = (0..7)
        .map(|i| TerminalDiscovery {
            name: format!("td-{i}"),
            uid: format!("uid-{i}"),
            connection_uid: Some("conn-a".to_string()),
            observed_at: at(i),
        })
        .collect();
    // A different connection: its own cohort of one, and kept.
    all.push(TerminalDiscovery {
        name: "td-other".to_string(),
        uid: "uid-other".to_string(),
        connection_uid: Some("conn-b".to_string()),
        observed_at: at(3),
    });
    // No connection at all: no cohort to be the sixth of.
    all.push(TerminalDiscovery {
        name: "td-none".to_string(),
        uid: "uid-none".to_string(),
        connection_uid: None,
        observed_at: at(4),
    });

    let collected = td::expired_terminal(&all, 86_400, 5, now());
    assert_eq!(
        collected,
        vec!["uid-5".to_string(), "uid-6".to_string()],
        "the two oldest of conn-a's seven, and nothing else"
    );

    // The age bound alone, with a cohort that never fills.
    let old = vec![TerminalDiscovery {
        name: "td-old".to_string(),
        uid: "uid-old".to_string(),
        connection_uid: Some("conn-a".to_string()),
        observed_at: now() - chrono::Duration::hours(30),
    }];
    assert_eq!(
        td::expired_terminal(&old, 86_400, 5, now()),
        vec!["uid-old"]
    );
    assert!(
        td::expired_terminal(&old, 86_400 * 3, 5, now()).is_empty(),
        "inside the retention window, and inside the cohort"
    );
}

/// The chunk index carries the first and last name of each chunk, which is what
/// lets the API skip whole chunks on a prefix search without fetching them.
#[test]
fn the_chunk_index_carries_each_chunks_first_and_last_name() {
    let entries: Vec<TopicEntry> = (0..3)
        .map(|i| TopicEntry::new(&format!("t-{i}"), 1))
        .collect();
    let split = chunks::split(&entries);
    let index = td::chunk_index(&entries, &split, JOB);
    assert_eq!(index.len(), 1);
    assert_eq!(index[0].name, format!("{JOB}-r000"));
    assert_eq!(index[0].count, 3);
    assert_eq!(index[0].first_name.as_deref(), Some("t-0"));
    assert_eq!(index[0].last_name.as_deref(), Some("t-2"));
    assert_eq!(index[0].sha256, split[0].sha256);
}

/// An attestation for a different namespace or a different `KafkaCluster` is
/// not a candidate at all — it is about another cluster.
#[test]
fn an_attestation_for_another_namespace_is_not_a_candidate() {
    let att: Vec<logweir_core::check_contract::Attestation> =
        serde_json::from_value(json!([attestation(
            PRINCIPAL,
            CLUSTER_ID,
            "2026-12-15T00:00:00Z"
        )]))
        .expect("attestations parse");
    assert!(td::attestation_for(&att, NS, "source").is_some());
    assert!(td::attestation_for(&att, "other-ns", "source").is_none());
    assert!(td::attestation_for(&att, NS, "other-cluster").is_none());
}

// ---------------------------------------------------------------------------
// Source-level guards
// ---------------------------------------------------------------------------

fn source() -> String {
    let path =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/controllers/topic_discovery.rs");
    std::fs::read_to_string(path).expect("the reconciler's own source")
}

/// Lines that are not comments, so a rule named in prose does not satisfy a
/// grep for the thing the prose says is absent.
fn code_only(src: &str) -> String {
    src.lines()
        .filter(|l| {
            let t = l.trim_start();
            !(t.starts_with("//") || t.starts_with("*") || t.starts_with("///"))
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// **The reconciler reads no Secret, deletes nothing, and builds no
/// `Api<Event>`.**
///
/// Each of the three is a grant this role does not hold: `secrets` at any verb,
/// `delete` on anything, and `events: list`. A call added before its grant 403s
/// in production while every route-table test above stays green — which is the
/// exact shape of the `replace_status` P0.
#[test]
fn the_reconciler_names_no_grant_this_role_does_not_hold() {
    let code = code_only(&source());
    for forbidden in [
        "Api<Secret>",
        "Api<Event>",
        ".delete(",
        ".delete_opt(",
        "DeleteParams",
        "replace_status",
        "Patch::Apply",
    ] {
        assert!(
            !code.contains(forbidden),
            "controllers/topic_discovery.rs names `{forbidden}`, which config/rbac/role.yaml \
             grants nothing for"
        );
    }
}

/// The reconciler takes its instant ONCE, from `Utc::now()` in
/// `reconcile_discovery` and nowhere else — every other `now` is an argument,
/// which is what makes a boundary like [`td::STALLED_GRACE_SECONDS`] assertable
/// rather than observable only by waiting.
#[test]
fn the_reconciler_reads_one_clock_in_one_place() {
    let code = code_only(&source());
    assert_eq!(
        code.matches("Utc::now()").count(),
        1,
        "two clock reads can put a verdict and the condition reporting it on either side of \
         the same instant"
    );
    for forbidden in ["SystemTime::now", "Instant::now"] {
        assert!(!code.contains(forbidden));
    }
}
