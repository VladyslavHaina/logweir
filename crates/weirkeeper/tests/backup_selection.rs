//! Dynamic topic selection per run — D1 §7.2, PLAT-09.2.
//!
//! EVERY TEST HERE IS A PURE-FUNCTION TEST OR A `mock_client` TEST. Nothing
//! dials a broker, nothing waits on a Job, nothing shells out, and the double
//! PANICS on a request it was not given a route for — which is what turns "it
//! created no runner Job", "it never read that impostor's log" and "it ran no
//! second discovery" into assertions rather than absences of evidence.
//!
//! READ THESE THREE FIRST, in this order:
//!
//! 1. [`refuse_policy_fails_discovery_incomplete_without_runner_job`] — the
//!    honesty claim of the whole mode. Kafka silently omits topics a principal
//!    cannot describe, so a clean listing is `unknown` and never proof; an
//!    operator who asked for `Refuse` gets a failed run rather than a backup
//!    labelled "all user topics" that quietly missed some.
//! 2. [`an_impostor_pod_is_never_read`] — D-SEAMS **S6**, defect `SEC-PODLOG`.
//!    A discovery pod's stdout becomes this run's ALLOWLIST, and
//!    `batch.kubernetes.io/job-name` is writable by anything that can create a
//!    pod.
//! 3. [`the_discovery_job_ttl_is_patched_only_after_the_status_landed`] — the
//!    commit point. The relay lives on the pod and the TTL controller deletes a
//!    Job and its pods together, so a TTL set before the freeze is recorded
//!    lets garbage collection race a read a later pass may still have to make.
//!
//! **A FIXTURE `Backup` CARRIES `metadata.resourceVersion`.** Every status
//! write on this path is a `resourceVersion`-preconditioned merge PATCH
//! (D-SEAMS **S7**), so an object without one is refused by name rather than
//! written without its precondition.

use std::collections::BTreeMap;

use chrono::{DateTime, TimeZone, Utc};
use logweir_core::check_contract::{
    frames, topic_tsv_sha256, CheckCode, CheckPlan, CheckRequest, CheckResult, ExpectedSummary,
    InventoryCounts, InventoryResult, Stream, TopicEntry, TruncationReason, VisibilityState,
    TOPIC_INVENTORY_FORMAT,
};
use serde_json::{json, Value};
use weirkeeper::backup_execution::{SelectionInputs, INPUTS_KEY};
use weirkeeper::check::{job as cjob, plan};
use weirkeeper::conditions::{
    CONDITION_TOPICS_RESOLVED, PHASE_RESOLVING, REASON_DISCOVERY_RUNNING, REASON_RESOLVED,
    TERMINAL_STATE_DISCOVERY_FAILED, TERMINAL_STATE_DISCOVERY_INCOMPLETE,
    TERMINAL_STATE_DISCOVERY_RESULT_UNREADABLE, TERMINAL_STATE_SELECTION_EMPTY,
    TERMINAL_STATE_SELECTION_TOO_LARGE, TERMINAL_STATE_SOURCE_CHANGED_DURING_RESOLUTION,
};
use weirkeeper::controllers::backup::{
    reconcile_backup, reconcile_backup_with_runner_image, unobserved_archive,
};
use weirkeeper::controllers::backup_selection as sel;
use weirkeeper::crds::backup::Backup;
use weirkeeper::crds::selection::{Coverage, IncompleteDiscovery, SelectionMode};
use weirkeeper::testing::{mock_client_recording_bodies, Route, SeenBody};
use weirkeeper::verification::unverified_evidence;

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

const NS: &str = "logweir-d1-w5";
const NAME: &str = "logweir-backup-nightly-20260916-020000";
const UID: &str = "3f1c8a5e-0000-4000-8000-0000000000a1";
/// A SECOND `Backup`, for `two_backups_freeze_two_discovery_results`.
const UID_B: &str = "3f1c8a5e-0000-4000-8000-0000000000b2";
const NAME_B: &str = "logweir-backup-nightly-20260916-030000";
const CLUSTER_UID: &str = "7a2b9c1d-0000-4000-8000-0000000000c1";
const CLUSTER_ID: &str = "kJ8tqf2wSMqK3yA6vQv2dA";
const DISCOVERY_JOB_UID: &str = "bbbbbbbb-0000-4000-8000-0000000000d1";
const OTHER_JOB_UID: &str = "deadbeef-0000-4000-8000-0000000000e1";
const POD: &str = "lwd-3f1c8a5e-0000-4000-8000-0000000000a1-abcde";
const IMPOSTOR_POD: &str = "not-mine-zzzzz";
const PRINCIPAL: &str = "User:logweir";

/// The digest the discovery plan `ConfigMap` annotation carries and the end
/// frame declares. Any value works; what matters is that ONE value is in both
/// places, because that comparison IS the relay verification.
const PLAN_SHA: &str = "sha256:2222222222222222222222222222222222222222222222222222222222222222";

fn discovery_job(uid: &str) -> String {
    format!("lwd-{uid}")
}

fn now() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 9, 16, 2, 0, 0)
        .single()
        .expect("the fixture instant exists")
}

/// A dynamic `Backup`: `topics: []` beside `spec.allUserTopics` — D1 §7.1's
/// second shape.
fn dynamic_backup(name: &str, uid: &str, exclude: Value, incomplete: &str) -> Backup {
    let mut all = json!({ "incompleteDiscovery": incomplete });
    if !exclude.is_null() {
        all["exclude"] = exclude;
    }
    serde_json::from_value(json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "Backup",
        "metadata": {
            "name": name, "namespace": NS, "uid": uid,
            "generation": 1, "resourceVersion": "4242"
        },
        "spec": {
            "sourceRef": { "name": "prod" },
            "topics": [],
            "allUserTopics": all,
            "archive": { "url": "s3://kafka-backups/logweir",
                         "secretRef": { "name": "logweir-s3" } },
            "triggeredBy": "manual",
            "deadlineSeconds": 3600
        }
    }))
    .expect("the fixture is a Backup")
}

/// The ordinary dynamic run: no exclusions, visible-only policy.
fn visible_only() -> Backup {
    dynamic_backup(NAME, UID, Value::Null, "BackUpVisibleTopics")
}

/// The strict dynamic run.
fn refusing() -> Backup {
    dynamic_backup(NAME, UID, Value::Null, "Refuse")
}

/// A NAMED run, for the coverage row that proves existing allowlists are
/// untouched.
fn named_backup() -> Backup {
    serde_json::from_value(json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "Backup",
        "metadata": {
            "name": NAME, "namespace": NS, "uid": UID,
            "generation": 1, "resourceVersion": "4242"
        },
        "spec": {
            "sourceRef": { "name": "prod" },
            "topics": ["orders", "payments"],
            "archive": { "url": "s3://kafka-backups/logweir",
                         "secretRef": { "name": "logweir-s3" } },
            "triggeredBy": "manual",
            "deadlineSeconds": 3600
        }
    }))
    .expect("the fixture is a Backup")
}

fn kafka_cluster_json(cluster_id: &str) -> String {
    json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "KafkaCluster",
        "metadata": { "name": "prod", "namespace": NS, "uid": CLUSTER_UID, "generation": 1 },
        "spec": {
            "bootstrapServers": ["broker-0.prod:9093", "broker-1.prod:9093"],
            "auth": { "mode": "scramSha512", "username": "logweir",
                      "secretRef": { "name": "prod-sasl" }, "tls": true },
            "role": "source"
        },
        "status": { "reachable": true, "clusterId": cluster_id }
    })
    .to_string()
}

/// The same `KafkaCluster`, on a pass where the probe has cleared
/// `status.clusterId` — which it does on any unreachable or unreadable probe.
fn kafka_cluster_without_observed_id() -> String {
    let mut v: Value = serde_json::from_str(&kafka_cluster_json(CLUSTER_ID)).expect("JSON");
    v["status"] = json!({ "reachable": false });
    v.to_string()
}

fn not_found(kind: &str, name: &str) -> String {
    json!({"kind":"Status","apiVersion":"v1","status":"Failure",
           "message": format!("{kind} \"{name}\" not found"),
           "reason":"NotFound","code":404})
    .to_string()
}

/// The source resolution digest a correctly dispatched discovery Job carries.
fn source_sha(cluster_id: &str) -> String {
    let cluster = serde_json::from_str(&kafka_cluster_json(cluster_id)).expect("a KafkaCluster");
    let resolved =
        weirkeeper::connection::resolve(&cluster, weirkeeper::connection::ConnectionUse::Discovery)
            .expect("the fixture connection resolves");
    sel::source_digest(&resolved, &cluster).expect("it digests")
}

/// The single owner reference a `Backup`-owned object carries.
fn backup_owner(name: &str, uid: &str) -> Value {
    json!([{
        "apiVersion": "logweir.dev/v1alpha1", "kind": "Backup",
        "name": name, "uid": uid, "controller": true, "blockOwnerDeletion": true
    }])
}

/// The runner Job the API server hands back from the create — the shape
/// `compatible_backup_job` demands, so a fixture never fails for the wrong
/// reason.
fn runner_job_body(name: &str, uid: &str) -> String {
    json!({
        "apiVersion": "batch/v1", "kind": "Job",
        "metadata": {
            "name": name, "namespace": NS, "uid": DISCOVERY_JOB_UID,
            "ownerReferences": backup_owner(name, uid)
        },
        "spec": {"template": {"spec": {"containers": [], "restartPolicy": "Never"}}},
        "status": {}
    })
    .to_string()
}

/// The run's own plan `ConfigMap`, as the create response.
fn run_plan_body(name: &str, uid: &str) -> String {
    json!({
        "apiVersion": "v1", "kind": "ConfigMap",
        "metadata": {
            "name": format!("{name}-plan"), "namespace": NS,
            "ownerReferences": backup_owner(name, uid)
        },
        "immutable": true,
        "data": {}
    })
    .to_string()
}

/// The discovery Job, as the API server would hand it back.
fn discovery_job_body(uid: &str, owner_uid: &str, finished: Option<&str>, source: &str) -> String {
    let status = match finished {
        Some(kind) => json!({"conditions":[{
            "type": kind, "status": "True", "reason": "x",
            "lastProbeTime": "2026-09-16T01:59:00Z",
            "lastTransitionTime": "2026-09-16T01:59:00Z"
        }]}),
        None => json!({}),
    };
    json!({
        "apiVersion": "batch/v1", "kind": "Job",
        "metadata": {
            "name": discovery_job(uid), "namespace": NS, "uid": DISCOVERY_JOB_UID,
            "creationTimestamp": "2026-09-16T01:58:30Z",
            "annotations": { sel::SOURCE_SHA256_ANNOTATION: source },
            "labels": {
                sel::LABEL_PURPOSE: sel::PURPOSE_TOPIC_DISCOVERY,
                "logweir.dev/check-kind": "topicInventory"
            },
            "ownerReferences": backup_owner(NAME, owner_uid)
        },
        "spec": {"template": {"spec": {"containers": [], "restartPolicy": "Never"}}},
        "status": status
    })
    .to_string()
}

/// The discovery plan `ConfigMap`, owned and immutable, carrying [`PLAN_SHA`].
fn discovery_plan_config_map(uid: &str, owner_uid: &str) -> String {
    json!({
        "apiVersion": "v1", "kind": "ConfigMap",
        "metadata": {
            "name": format!("{}-plan", discovery_job(uid)), "namespace": NS,
            "annotations": { plan::DIGEST_ANNOTATION: PLAN_SHA },
            "ownerReferences": backup_owner(NAME, owner_uid)
        },
        "immutable": true,
        "data": { cjob::CHECK_PLAN_KEY: "{}" }
    })
    .to_string()
}

fn pod_object(name: &str, owner_uid: Option<&str>, job: &str) -> Value {
    let owners = match owner_uid {
        Some(uid) => json!([{
            "apiVersion": "batch/v1", "kind": "Job", "name": job,
            "uid": uid, "controller": true, "blockOwnerDeletion": true
        }]),
        None => json!([]),
    };
    json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {
            "name": name, "namespace": NS,
            "creationTimestamp": "2026-09-16T01:58:40Z",
            "ownerReferences": owners,
            "labels": { "batch.kubernetes.io/job-name": job }
        },
        "spec": { "containers": [] },
        "status": {"phase":"Succeeded","containerStatuses":[{
            "name":"runner","image":"x","imageID":"x","ready":false,"restartCount":0,
            "state":{"terminated":{"exitCode":0,
                     "startedAt":"2026-09-16T01:58:45Z","finishedAt":"2026-09-16T01:59:00Z"}}
        }]}
    })
}

/// A finished Job the API server dated nowhere: no `completionTime`, and a
/// terminal condition with no `lastTransitionTime`.
fn undated_finished_job() -> k8s_openapi::api::batch::v1::Job {
    let mut v: Value = serde_json::from_str(&discovery_job_body(
        UID,
        UID,
        Some("Complete"),
        &source_sha(CLUSTER_ID),
    ))
    .expect("JSON");
    v["status"] = json!({"conditions": [{"type": "Complete", "status": "True", "reason": "x"}]});
    serde_json::from_value(v).expect("a Job")
}

/// Its pod: the `runner` container terminated, and no `finishedAt`.
fn undated_pod() -> k8s_openapi::api::core::v1::Pod {
    let mut v = pod_object(POD, Some(DISCOVERY_JOB_UID), &discovery_job(UID));
    v["status"]["containerStatuses"][0]["state"]["terminated"] =
        json!({"exitCode": 0, "startedAt": "2026-09-16T01:58:45Z"});
    serde_json::from_value(v).expect("a Pod")
}

fn pod_list(pods: Vec<Value>) -> String {
    json!({"apiVersion":"v1","kind":"PodList","metadata":{},"items":pods}).to_string()
}

/// One listing entry.
fn entry(name: &str, partitions: u32) -> TopicEntry {
    TopicEntry::new(name, partitions)
}

fn internal_entry(name: &str) -> TopicEntry {
    let mut e = TopicEntry::new(name, 50);
    e.internal = true;
    e
}

fn denied_entry(name: &str) -> TopicEntry {
    let mut e = TopicEntry::new(name, 0);
    e.error = Some(CheckCode::TopicAuthorizationFailed);
    e
}

fn inventory_of(entries: &[TopicEntry], cluster_id: &str, limited: bool) -> InventoryResult {
    let returned = u32::try_from(entries.len()).expect("a small fixture");
    InventoryResult {
        format: TOPIC_INVENTORY_FORMAT.to_string(),
        cluster_id: Some(cluster_id.to_string()),
        broker_count: Some(2),
        counts: InventoryCounts {
            listed: returned,
            returned,
            internal_excluded: 0,
            errored: u32::from(limited),
        },
        truncated: false,
        truncation_reason: None,
        topic_authorization_error_in_listing: limited,
        expected: ExpectedSummary::default(),
        expected_results: Vec::new(),
        topics_sha256: topic_tsv_sha256(entries),
    }
}

/// A complete, verifiable relay for `entries` plus `inventory`.
fn relay_log(entries: &[TopicEntry], inventory: &InventoryResult, subject_uid: &str) -> String {
    let mut result = CheckResult::new(logweir_core::check_contract::CheckPlanKind::TopicInventory);
    result.inventory = Some(inventory.clone());
    let bytes = result.to_canonical_json().expect("a serialisable result");
    let parts = frames::write_parts(Stream::Result, &bytes).expect("small enough to frame");
    let mut streams = BTreeMap::new();
    streams.insert(Stream::Result, (bytes.clone(), parts.len()));
    let end = frames::end_frame(PLAN_SHA, subject_uid, &streams, Some(entries));

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

/// The routes a pass needs when the discovery Job does NOT exist yet.
fn start_routes() -> Vec<Route> {
    vec![
        Route {
            method: "GET",
            path_suffix: "/jobs/logweir-backup-nightly-20260916-020000",
            status: 404,
            body: not_found("jobs.batch", NAME),
        },
        Route {
            method: "GET",
            path_suffix: "/kafkaclusters/prod",
            status: 200,
            body: kafka_cluster_json(CLUSTER_ID),
        },
        Route {
            method: "GET",
            path_suffix: "/jobs/lwd-3f1c8a5e-0000-4000-8000-0000000000a1",
            status: 404,
            body: not_found("jobs.batch", &discovery_job(UID)),
        },
        Route {
            method: "POST",
            path_suffix: "/configmaps",
            status: 201,
            body: run_plan_body(NAME, UID),
        },
        Route {
            method: "POST",
            path_suffix: "/jobs",
            status: 201,
            body: runner_job_body(NAME, UID),
        },
        Route {
            method: "PATCH",
            path_suffix: "/backups/logweir-backup-nightly-20260916-020000/status",
            status: 200,
            body: serde_json::to_string(&visible_only()).expect("serialisable"),
        },
    ]
}

/// The routes a pass needs when the discovery Job has FINISHED and its pod is
/// readable: `log` is what the pod prints, `pods` is what the listing answers.
fn finished_routes(
    name: &str,
    uid: &str,
    log: String,
    pods: String,
    cluster_id: &str,
) -> Vec<Route> {
    let source = source_sha(CLUSTER_ID);
    let runner_job: &'static str = Box::leak(format!("/jobs/{name}").into_boxed_str());
    let disc_job: &'static str =
        Box::leak(format!("/jobs/{}", discovery_job(uid)).into_boxed_str());
    let disc_plan: &'static str =
        Box::leak(format!("/configmaps/{}-plan", discovery_job(uid)).into_boxed_str());
    let status_path: &'static str = Box::leak(format!("/backups/{name}/status").into_boxed_str());
    let pod_log: &'static str = Box::leak(format!("/pods/{POD}/log").into_boxed_str());
    let impostor_log: &'static str =
        Box::leak(format!("/pods/{IMPOSTOR_POD}/log").into_boxed_str());
    vec![
        Route {
            method: "GET",
            path_suffix: runner_job,
            status: 404,
            body: not_found("jobs.batch", name),
        },
        Route {
            method: "GET",
            path_suffix: "/kafkaclusters/prod",
            status: 200,
            body: kafka_cluster_json(cluster_id),
        },
        Route {
            method: "GET",
            path_suffix: disc_job,
            status: 200,
            body: discovery_job_body(uid, uid, Some("Complete"), &source),
        },
        Route {
            method: "GET",
            path_suffix: "/pods",
            status: 200,
            body: pods,
        },
        Route {
            method: "GET",
            path_suffix: pod_log,
            status: 200,
            body: log,
        },
        // RECORDED AND NEVER EXPECTED. A route the reconciler must not use is
        // worth more than a missing one: without it an impostor read would
        // panic as "no route", which is indistinguishable from a typo in the
        // table. With it, reading the impostor SUCCEEDS and the assertion that
        // it was never asked for is the property.
        Route {
            method: "GET",
            path_suffix: impostor_log,
            status: 200,
            body: "logweir-check-topic=nonsense\n".to_string(),
        },
        Route {
            method: "GET",
            path_suffix: disc_plan,
            status: 200,
            body: discovery_plan_config_map(uid, uid),
        },
        Route {
            method: "PATCH",
            path_suffix: disc_job,
            status: 200,
            body: discovery_job_body(uid, uid, Some("Complete"), &source),
        },
        Route {
            method: "POST",
            path_suffix: "/configmaps",
            status: 201,
            body: run_plan_body(name, uid),
        },
        Route {
            method: "POST",
            path_suffix: "/jobs",
            status: 201,
            body: runner_job_body(name, uid),
        },
        Route {
            method: "PATCH",
            path_suffix: status_path,
            status: 200,
            body: serde_json::to_string(&visible_only()).expect("serialisable"),
        },
    ]
}

/// The happy-path routes for [`visible_only`] over `entries`.
fn resolved_routes(entries: &[TopicEntry]) -> Vec<Route> {
    let inventory = inventory_of(
        entries,
        CLUSTER_ID,
        entries.iter().any(|e| e.error.is_some()),
    );
    finished_routes(
        NAME,
        UID,
        relay_log(entries, &inventory, UID),
        pod_list(vec![pod_object(
            POD,
            Some(DISCOVERY_JOB_UID),
            &discovery_job(UID),
        )]),
        CLUSTER_ID,
    )
}

async fn reconcile_with(b: &Backup, routes: Vec<Route>) -> (Option<String>, Vec<SeenBody>) {
    let (client, _seen, bodies) = mock_client_recording_bodies(routes);
    let outcome = reconcile_backup(b, &client, &unobserved_archive, &unverified_evidence, now())
        .await
        .expect("a refusal is an outcome, not an error");
    let bodies = bodies.lock().expect("readable").clone();
    (outcome.terminal_state, bodies)
}

/// [`reconcile_with`] for a controller process that was handed an image and a
/// pull policy — `main`'s `Context::runner_image`, which is what every
/// installation that sets `LOGWEIR_RUNNER_IMAGE` has.
async fn reconcile_with_runner_image(
    b: &Backup,
    routes: Vec<Route>,
    runner: &weirkeeper::job::RunnerImage,
) -> Vec<SeenBody> {
    let (client, _seen, bodies) = mock_client_recording_bodies(routes);
    reconcile_backup_with_runner_image(
        b,
        &client,
        &unobserved_archive,
        &unverified_evidence,
        now(),
        runner,
    )
    .await
    .expect("a refusal is an outcome, not an error");
    let bodies = bodies.lock().expect("readable").clone();
    bodies
}

fn path(uri: &str) -> &str {
    uri.split('?').next().unwrap_or(uri)
}

fn calls(bodies: &[SeenBody]) -> Vec<String> {
    bodies
        .iter()
        .map(|b| format!("{} {}", b.method, path(&b.uri)))
        .collect()
}

fn patched_statuses(bodies: &[SeenBody]) -> Vec<Value> {
    bodies
        .iter()
        .filter(|b| b.method == "PATCH" && path(&b.uri).ends_with("/status"))
        .map(|b| serde_json::from_str::<Value>(&b.body).expect("a status patch is JSON"))
        .collect()
}

/// Every object POSTed to `…/jobs`, as JSON.
fn posted_jobs(bodies: &[SeenBody]) -> Vec<Value> {
    bodies
        .iter()
        .filter(|b| b.method == "POST" && path(&b.uri).ends_with("/jobs"))
        .map(|b| serde_json::from_str::<Value>(&b.body).expect("a Job is JSON"))
        .collect()
}

/// Every object POSTed to `…/configmaps`, as JSON.
fn posted_config_maps(bodies: &[SeenBody]) -> Vec<Value> {
    bodies
        .iter()
        .filter(|b| b.method == "POST" && path(&b.uri).ends_with("/configmaps"))
        .map(|b| serde_json::from_str::<Value>(&b.body).expect("a ConfigMap is JSON"))
        .collect()
}

/// The frozen execution inputs the run's plan `ConfigMap` carries.
fn frozen_inputs(bodies: &[SeenBody]) -> Value {
    let cm = posted_config_maps(bodies)
        .into_iter()
        .find(|cm| {
            // The RUN's plan, not the discovery's: the two go to the same
            // collection and are told apart by the `lwd-` prefix.
            cm["metadata"]["name"]
                .as_str()
                .is_some_and(|n| !n.starts_with(sel::DISCOVERY_JOB_PREFIX))
        })
        .expect("the run's plan ConfigMap was POSTed");
    serde_json::from_str(cm["data"][INPUTS_KEY].as_str().expect("the snapshot key"))
        .expect("the snapshot is JSON")
}

/// One condition out of a status patch.
fn condition_of(status: &Value, r#type: &str) -> Option<Value> {
    status["status"]["conditions"]
        .as_array()?
        .iter()
        .find(|c| c["type"] == json!(r#type))
        .cloned()
}

/// The `Observed` a pure resolution test starts from.
fn observed(
    entries: &[TopicEntry],
    exclusions: &sel::Exclusions,
    state: VisibilityState,
) -> sel::Observed {
    let inventory = inventory_of(
        entries,
        CLUSTER_ID,
        entries.iter().any(|e| e.error.is_some()),
    );
    sel::Observed {
        classification: sel::classify(entries, exclusions),
        inventory,
        visibility: state,
        observed_at: now(),
        job_name: discovery_job(UID),
    }
}

// ===========================================================================
// Unit — the pure half of D1 §7.2
// ===========================================================================

/// **THE RESOLUTION IS A PURE FUNCTION OF WHAT DISCOVERY SAW AND WHAT THE
/// POLICY SAYS** — D1 §12's first PLAT-09.2 unit row.
///
/// Two clusters that showed the same entries under the same policy resolve to
/// the same names, the same counts and the same coverage, whatever ran in
/// between; and a cluster that gained a topic since resolves to a list that
/// contains it. That second half is the tracker's "a newly created user topic
/// enters the next dynamic backup", stated where it can be asserted without a
/// broker.
///
/// KILLS: caching a resolution across observations; taking the coverage from
/// anywhere but the visibility verdict and the policy; letting the observation
/// instant or the Job name leak into the resolved names.
#[test]
fn resolution_is_a_pure_function_of_discovery_and_policy() {
    let exclusions = sel::Exclusions::default();
    let before = [entry("orders", 6), entry("payments", 3)];
    let a = sel::resolved_selection(
        &observed(&before, &exclusions, VisibilityState::Unknown),
        &exclusions,
        IncompleteDiscovery::BackUpVisibleTopics,
        NAME,
    )
    .expect("it resolves");
    let b = sel::resolved_selection(
        &observed(&before, &exclusions, VisibilityState::Unknown),
        &exclusions,
        IncompleteDiscovery::BackUpVisibleTopics,
        NAME,
    )
    .expect("it resolves");
    assert_eq!(a.topics, b.topics, "the same observation resolves the same");
    assert_eq!(a.selection, b.selection);
    assert_eq!(a.topics, vec!["orders".to_string(), "payments".to_string()]);

    // A topic created between two runs is in the SECOND run's list, and in
    // nothing else: the first run's frozen names are untouched by it.
    let after = [entry("audit", 1), entry("orders", 6), entry("payments", 3)];
    let next = sel::resolved_selection(
        &observed(&after, &exclusions, VisibilityState::Unknown),
        &exclusions,
        IncompleteDiscovery::BackUpVisibleTopics,
        NAME,
    )
    .expect("it resolves");
    assert_eq!(
        next.topics,
        vec![
            "audit".to_string(),
            "orders".to_string(),
            "payments".to_string()
        ]
    );
    assert_eq!(a.topics.len(), 2, "the earlier resolution did not move");
    assert_eq!(next.selection.resolved_topic_count, 3);

    // The coverage is derived from the verdict, not computed a second way.
    for (state, policy, expected) in [
        (
            VisibilityState::Unknown,
            IncompleteDiscovery::BackUpVisibleTopics,
            Some(Coverage::VisibleUserTopicsOnly),
        ),
        (
            VisibilityState::Limited,
            IncompleteDiscovery::BackUpVisibleTopics,
            Some(Coverage::VisibleUserTopicsOnly),
        ),
        (
            VisibilityState::AttestedComplete,
            IncompleteDiscovery::Refuse,
            Some(Coverage::AllUserTopicsAttested),
        ),
        (VisibilityState::Unknown, IncompleteDiscovery::Refuse, None),
        (VisibilityState::Limited, IncompleteDiscovery::Refuse, None),
    ] {
        assert_eq!(
            sel::coverage_for(state, policy),
            expected,
            "{state:?} under {policy:?}"
        );
    }
}

/// **AN INTERNAL TOPIC IS EXCLUDED BY THE FLAG *AND* BY THE `__` RULE** —
/// D1 §7.2 R5, §7.5.
///
/// Both, not either. The flag is the runner's reading of the same convention,
/// and re-applying the rule in the controller is what makes an older or a lying
/// runner unable to slip `__consumer_offsets` into somebody's backup — a topic
/// whose contents are the cluster's own bookkeeping and whose restore would be
/// actively harmful.
///
/// KILLS: trusting `entry.internal` alone; trusting the `__` prefix alone;
/// counting an internal topic as `excludedByRule` (which would make
/// `internalExcludedCount` read zero on a cluster full of them).
#[test]
fn internal_flag_and_double_underscore_are_excluded() {
    let entries = [
        entry("orders", 6),
        internal_entry("__consumer_offsets"),
        // The flag says nothing; the NAME does.
        entry("__transaction_state", 50),
        // The flag says internal and the name does not. A runner that knows
        // something this controller does not is still obeyed.
        {
            let mut e = TopicEntry::new("_schemas", 1);
            e.internal = true;
            e
        },
    ];
    let c = sel::classify(&entries, &sel::Exclusions::default());
    assert_eq!(c.resolved, vec!["orders".to_string()]);
    assert_eq!(
        c.internal,
        vec![
            "__consumer_offsets".to_string(),
            "__transaction_state".to_string(),
            "_schemas".to_string()
        ],
        "byte-sorted, and every one of the three rules fired"
    );
    assert!(c.excluded.is_empty(), "an internal topic is not a rule hit");
    assert_eq!(c.visible, 4);
}

/// **AN EXCLUSION IS A LITERAL NAME OR A LITERAL PREFIX, AND NEVER A PATTERN**
/// — D1 §7.1, guard **G-GLOB**.
///
/// `orders-` excludes `orders-eu` because it is a prefix; `orders` does not
/// exclude `orders-eu`, because an exact name is an exact name. A `*` is not
/// writable in either field (the CRD's character set has no glob characters),
/// and one that somehow arrived excludes NOTHING rather than everything — the
/// safe direction, since the other one silently drops a user's data.
///
/// KILLS: treating `exclude.topics` as prefixes; treating `exclude.prefixes` as
/// suffixes or substrings; interpreting `*` or `?`; an empty prefix excluding
/// the whole cluster.
#[test]
fn exact_and_prefix_exclusions_are_literal() {
    let exclusions = sel::Exclusions {
        topics: vec!["orders".to_string()],
        prefixes: vec!["tmp-".to_string()],
    };
    assert!(exclusions.excludes("orders"));
    assert!(!exclusions.excludes("orders-eu"), "an exact name is exact");
    assert!(!exclusions.excludes("ordersX"));
    assert!(exclusions.excludes("tmp-1"));
    assert!(exclusions.excludes("tmp-"));
    assert!(
        !exclusions.excludes("a-tmp-1"),
        "a prefix is not a substring"
    );

    let starred = sel::Exclusions {
        topics: vec!["orders*".to_string()],
        prefixes: vec!["*".to_string(), String::new()],
    };
    assert!(
        !starred.excludes("orders-eu") && !starred.excludes("payments"),
        "a metacharacter excludes nothing; an empty prefix excludes nothing"
    );

    let entries = [
        entry("orders", 6),
        entry("orders-eu", 2),
        entry("tmp-import", 1),
        entry("payments", 3),
    ];
    let c = sel::classify(&entries, &exclusions);
    assert_eq!(
        c.resolved,
        vec!["orders-eu".to_string(), "payments".to_string()]
    );
    assert_eq!(
        c.excluded,
        vec!["orders".to_string(), "tmp-import".to_string()]
    );
}

/// **AN EMPTY RESOLUTION IS `SelectionEmpty`, AND NOT AN EMPTY ALLOWLIST** —
/// D1 §7.2 R7, guard **G-GLOB**.
///
/// The one outcome the mandatory allowlist exists to make impossible is an
/// empty `source.topics` in `backup.yaml`, which the engine would read as "no
/// allowlist, so everything". Every way of reaching an empty resolution — an
/// empty cluster, a cluster of nothing but internal topics, exclusions that
/// took the lot — ends in the same named terminal state.
///
/// KILLS: returning `Ok` with an empty list; refusing with a different state;
/// distinguishing "empty cluster" from "excluded everything" in a way that lets
/// one of them through.
#[test]
fn empty_resolution_is_selection_empty() {
    let all_out = sel::Exclusions {
        topics: vec!["orders".to_string()],
        prefixes: Vec::new(),
    };
    for (entries, exclusions) in [
        (vec![], sel::Exclusions::default()),
        (
            vec![internal_entry("__consumer_offsets")],
            sel::Exclusions::default(),
        ),
        (vec![entry("orders", 6)], all_out.clone()),
        (vec![denied_entry("orders")], sel::Exclusions::default()),
    ] {
        let o = observed(&entries, &exclusions, VisibilityState::Unknown);
        let (state, message) = sel::resolved_selection(
            &o,
            &exclusions,
            IncompleteDiscovery::BackUpVisibleTopics,
            NAME,
        )
        .expect_err("an empty resolution never freezes");
        assert_eq!(state, TERMINAL_STATE_SELECTION_EMPTY, "{message}");
        assert!(
            message.contains("G-GLOB"),
            "the message says why it is a refusal and not an empty run: {message}"
        );
    }
}

/// **A TOPIC THE BROKER REFUSED TO DESCRIBE IS `limited`, NEVER `resolved`** —
/// D1 §7.2 R5, §7.5, PLAT-09.2's "ACL limitation".
///
/// Kafka answers `TOPIC_AUTHORIZATION_FAILED` whether or not the topic exists,
/// so the entry proves only that this principal cannot read it. Freezing the
/// name would put a topic in `backup.yaml` that the run then fails on; dropping
/// it silently would lose the one signal that says the coverage is incomplete.
/// It goes in `limitedTopicCount`, which is what turns
/// `VisibleUserTopicsOnly` from a label into a number.
///
/// KILLS: resolving an errored entry; counting it as internal or as a rule
/// exclusion; losing the count that makes the incompleteness visible.
#[test]
fn authorization_failed_entries_are_limited_not_resolved() {
    let entries = [
        entry("orders", 6),
        denied_entry("secrets"),
        denied_entry("hr-payroll"),
    ];
    let c = sel::classify(&entries, &sel::Exclusions::default());
    assert_eq!(c.resolved, vec!["orders".to_string()]);
    assert_eq!(
        c.limited,
        vec!["hr-payroll".to_string(), "secrets".to_string()]
    );

    let o = observed(
        &entries,
        &sel::Exclusions::default(),
        VisibilityState::Limited,
    );
    let resolved = sel::resolved_selection(
        &o,
        &sel::Exclusions::default(),
        IncompleteDiscovery::BackUpVisibleTopics,
        NAME,
    )
    .expect("visible-only accepts it");
    let discovery = resolved
        .selection
        .discovery
        .as_ref()
        .expect("a dynamic freeze records its discovery");
    assert_eq!(discovery.limited_topic_count, 2);
    assert_eq!(discovery.visibility, "limited");
    assert_eq!(resolved.selection.coverage, Coverage::VisibleUserTopicsOnly);
    assert!(
        !resolved.selection.coverage.claims_whole_cluster(),
        "an ACL-limited run never claims the cluster"
    );
}

/// **A RESULT DOCUMENT THAT DISAGREES WITH ITS OWN FRAMES IS NOT ADOPTED** —
/// D1 §7.2 R3, and the discovery half of PLAT-09.2's "discovery/execution
/// race".
///
/// The frames are the measured half: the decoder proved their count and their
/// digest against what the plan pinned. A document claiming a different
/// `counts.returned`, or a `topicsSha256` that is not the digest of the lines
/// that arrived, is a document that describes a listing this run did not
/// receive — a truncated log, a rotated one, or a runner that lied. Either way
/// the answer is `DiscoveryResultUnreadable`, which is NOT retryable.
///
/// KILLS: taking the counts from the document; copying `topicsSha256` instead
/// of computing it; treating a malformed result as a retryable failure.
#[test]
fn summary_count_and_digest_mismatch_is_unreadable() {
    let entries = [entry("orders", 6), entry("payments", 3)];
    let good = inventory_of(&entries, CLUSTER_ID, false);
    assert!(
        weirkeeper::controllers::topic_discovery::check_result_against_frames(&good, &entries)
            .is_ok()
    );

    let mut miscounted = good.clone();
    miscounted.counts.returned = 5_003;
    let refusal = weirkeeper::controllers::topic_discovery::check_result_against_frames(
        &miscounted,
        &entries,
    )
    .expect_err("a count the frames do not support");
    assert_eq!(refusal.code(), CheckCode::ResultUnreadable);

    let mut misdigested = good;
    misdigested.topics_sha256 =
        "sha256:0000000000000000000000000000000000000000000000000000000000000000".to_string();
    let refusal = weirkeeper::controllers::topic_discovery::check_result_against_frames(
        &misdigested,
        &entries,
    )
    .expect_err("a digest over bytes nobody relayed");
    assert_eq!(refusal.code(), CheckCode::ResultUnreadable);

    // And the state that reaches the object is the NOT-RETRYABLE one; an
    // operational failure is the retryable one.
    assert_eq!(
        sel::discovery_failure_state(CheckCode::ResultUnreadable),
        TERMINAL_STATE_DISCOVERY_RESULT_UNREADABLE
    );
    assert_eq!(
        sel::discovery_failure_state(CheckCode::CheckContractMismatch),
        TERMINAL_STATE_DISCOVERY_RESULT_UNREADABLE
    );
    for retryable in [
        CheckCode::DeadlineExceeded,
        CheckCode::PodUnschedulable,
        CheckCode::CredentialSecretNotFound,
        CheckCode::BrokerUnreachable,
    ] {
        assert_eq!(
            sel::discovery_failure_state(retryable),
            TERMINAL_STATE_DISCOVERY_FAILED,
            "{retryable} is operational, so a NEW Backup can succeed"
        );
    }
}

/// **THE `v2` `selection` BLOCK IS CANONICAL, SORTED AND BOUNDED** — D1 §3.3,
/// §7.2 R8, R9.
///
/// The names are byte-sorted and deduplicated, the block records counts and
/// provenance rather than a second copy of the list, and the two name lists it
/// does carry are bounded (50 internal, 200 rule-excluded) with `truncated`
/// telling a reader that they were cut. A plan `ConfigMap` is one MiB, and a
/// cluster can hold thousands of topics.
///
/// KILLS: emitting the names in listing order; duplicating the topic list
/// inside `selection`; unbounded `internalExcluded.names` /
/// `excludedByRule.names`; dropping `truncated` when they are cut.
#[test]
fn inputs_v2_selection_block_is_canonical_and_sorted() {
    let exclusions = sel::Exclusions {
        topics: Vec::new(),
        prefixes: vec!["tmp-".to_string()],
    };
    let mut entries = vec![entry("orders", 6), entry("audit", 1), entry("orders", 6)];
    for i in 0..60 {
        entries.push(internal_entry(&format!("__internal-{i:03}")));
    }
    for i in 0..250 {
        entries.push(entry(&format!("tmp-{i:03}"), 1));
    }
    let o = observed(&entries, &exclusions, VisibilityState::Unknown);
    let resolved = sel::resolved_selection(
        &o,
        &exclusions,
        IncompleteDiscovery::BackUpVisibleTopics,
        NAME,
    )
    .expect("it resolves");

    assert_eq!(
        resolved.topics,
        vec!["audit".to_string(), "orders".to_string()],
        "byte-sorted and deduplicated"
    );
    assert_eq!(resolved.selection.mode, SelectionMode::AllUserTopics);
    assert_eq!(resolved.selection.resolved_topic_count, 2);
    assert_eq!(
        resolved.selection.resolved_topic_bytes,
        ("audit".len() + "orders".len()) as i64
    );
    let discovery = resolved.selection.discovery.as_ref().expect("provenance");
    assert_eq!(discovery.internal_excluded.count, 60);
    assert_eq!(
        discovery.internal_excluded.names.len(),
        sel::MAX_INTERNAL_NAMES
    );
    assert!(discovery.internal_excluded.truncated);
    assert_eq!(discovery.excluded_by_rule.count, 250);
    assert_eq!(
        discovery.excluded_by_rule.names.len(),
        sel::MAX_EXCLUDED_NAMES
    );
    assert!(discovery.excluded_by_rule.truncated);
    assert_eq!(discovery.basis, "metadata-list");
    assert_eq!(discovery.discovery_job, discovery_job(UID));
    assert_eq!(discovery.cluster_id, CLUSTER_ID);
    assert_eq!(
        resolved
            .selection
            .exclude
            .as_ref()
            .map(|e| e.prefixes.clone()),
        Some(vec!["tmp-".to_string()])
    );

    // The block serialises with the names NOT repeated: `topics` is the one
    // copy and `selection` is its provenance.
    let json = serde_json::to_value(&resolved.selection).expect("serialisable");
    assert!(
        json.get("topics").is_none(),
        "the selection block never carries a second copy of the list: {json}"
    );

    // And the status projection is one rendering of the same values.
    let status = resolved.selection.status();
    assert_eq!(status.resolved_topic_count, 2);
    assert_eq!(status.internal_excluded_count, Some(60));
    assert_eq!(status.excluded_by_rule_count, Some(250));
    assert_eq!(status.visibility.as_deref(), Some("unknown"));
}

/// **THE TWO SIZE BOUNDS ARE ENFORCED, AND A TRUNCATED LISTING IS REFUSED** —
/// D1 §7.2 R8.
///
/// A run may freeze at most 5,000 names and at most 256 KiB of them, because
/// the plan `ConfigMap` a run mounts is bounded at one MiB. And a listing the
/// runner had to CUT is refused outright: its names are a prefix of what the
/// principal can see, and freezing a prefix while labelling the run "all user
/// topics" is the silent claim this whole mode exists to avoid.
///
/// KILLS: freezing a truncated listing; dropping either bound; reporting a
/// too-large selection as `SelectionEmpty` or as a discovery failure.
#[test]
fn an_oversized_or_truncated_listing_is_selection_too_large() {
    let exclusions = sel::Exclusions::default();
    let many: Vec<TopicEntry> = (0..=sel::MAX_RESOLVED_TOPICS)
        .map(|i| entry(&format!("topic-{i:06}"), 1))
        .collect();
    let (state, message) = sel::resolved_selection(
        &observed(&many, &exclusions, VisibilityState::Unknown),
        &exclusions,
        IncompleteDiscovery::BackUpVisibleTopics,
        NAME,
    )
    .expect_err("over the count bound");
    assert_eq!(state, TERMINAL_STATE_SELECTION_TOO_LARGE, "{message}");

    let entries = [entry("orders", 6)];
    let mut cut = observed(&entries, &exclusions, VisibilityState::Unknown);
    cut.inventory.truncated = true;
    cut.inventory.truncation_reason = Some(TruncationReason::MaxTopics);
    let (state, message) = sel::resolved_selection(
        &cut,
        &exclusions,
        IncompleteDiscovery::BackUpVisibleTopics,
        NAME,
    )
    .expect_err("a prefix of the cluster is not a selection");
    assert_eq!(state, TERMINAL_STATE_SELECTION_TOO_LARGE, "{message}");
    assert!(message.contains("PREFIX"), "{message}");
}

/// **THE DISCOVERY JOB'S NAME, DEADLINE AND SHAPE ARE D1 §7.2 R2's** — and the
/// pod it runs in is the isolated check pod, not an execution pod.
///
/// KILLS: naming the Job anything but `lwd-<backup uid>`; mounting a
/// ServiceAccount token; mounting the signing key or an archive credential;
/// letting the deadline exceed `min(300, spec.deadlineSeconds)`; giving the
/// runner a `/plan` mount it would read a `backup.yaml` out of.
#[test]
fn the_discovery_job_is_the_check_job_shape_named_after_the_backup() {
    assert_eq!(sel::discovery_job_name(UID), format!("lwd-{UID}"));
    assert_eq!(sel::discovery_job_name(UID).len(), 40);

    for (deadline, expect_job, expect_plan) in [
        (3600_i64, 300_i64, 210_u32),
        (300, 300, 210),
        (120, 120, 30),
        (
            sel::MIN_DYNAMIC_DEADLINE_SECONDS,
            sel::MIN_DYNAMIC_DEADLINE_SECONDS,
            30,
        ),
    ] {
        assert_eq!(
            sel::discovery_budget(deadline),
            Some((expect_job, expect_plan)),
            "deadlineSeconds {deadline}"
        );
    }
    // A DEADLINE THAT CANNOT FUND A DISCOVERY HAS NO BUDGET, IT DOES NOT GET A
    // ONE-SECOND ONE. A clamp here is how a run comes to die `DiscoveryFailed`
    // with nothing naming the deadline as the cause.
    for deadline in [sel::MIN_DYNAMIC_DEADLINE_SECONDS - 1, 60, 30, 1] {
        assert_eq!(
            sel::discovery_budget(deadline),
            None,
            "deadlineSeconds {deadline} cannot fund a discovery"
        );
    }

    let cluster = serde_json::from_str(&kafka_cluster_json(CLUSTER_ID)).expect("a KafkaCluster");
    let resolved =
        weirkeeper::connection::resolve(&cluster, weirkeeper::connection::ConnectionUse::Discovery)
            .expect("it resolves");
    let projection = resolved.project();
    let spec = cjob::CheckJobSpec {
        kind: logweir_core::check_contract::CheckPlanKind::TopicInventory,
        namespace: NS.to_string(),
        owner: sel::owner_of(&visible_only(), UID),
        connection_uid: resolved.uid.clone(),
        plan_config_map: plan::plan_config_map_name(&discovery_job(UID)),
        plan_sha256: PLAN_SHA.to_string(),
        subject_uid: UID.to_string(),
        timeout_seconds: 210,
        service_account_name: weirkeeper::connection::ConnectionUse::Discovery
            .service_account_name()
            .to_string(),
        secret_mounts: projection.secret_mounts,
        config_map_mounts: projection.config_map_mounts,
        env_from_secret: projection.env_from_secret,
        env_literal: projection.env_literal,
        image: None,
        image_pull_policy: None,
    };
    let job = sel::build_discovery_job(&spec, &discovery_job(UID), 300, "sha256:abc");
    let value = serde_json::to_value(&job).expect("serialisable");

    assert_eq!(value["metadata"]["name"], json!(discovery_job(UID)));
    assert_eq!(
        value["metadata"]["labels"][sel::LABEL_PURPOSE],
        json!(sel::PURPOSE_TOPIC_DISCOVERY)
    );
    assert_eq!(
        value["spec"]["template"]["metadata"]["labels"][sel::LABEL_PURPOSE],
        json!(sel::PURPOSE_TOPIC_DISCOVERY)
    );
    assert_eq!(
        value["metadata"]["labels"]["app.kubernetes.io/component"],
        json!(sel::COMPONENT_RUN_DISCOVERY),
        "the component key is shared with interactive checks so ONE listing \
         finds both; the VALUE is what keeps the two ceilings apart"
    );
    assert_ne!(
        value["metadata"]["labels"]["app.kubernetes.io/component"],
        json!("check"),
        "a run's own discovery does not spend the interactive check pool"
    );
    assert_eq!(
        value["spec"]["template"]["metadata"]["labels"]["app.kubernetes.io/component"],
        json!(sel::COMPONENT_RUN_DISCOVERY),
        "and the pod wears the same map, so one `kubectl get pods -l` finds it"
    );
    assert_eq!(
        value["metadata"]["annotations"][sel::SOURCE_SHA256_ANNOTATION],
        json!("sha256:abc")
    );
    assert_eq!(value["spec"]["activeDeadlineSeconds"], json!(300));

    // AND THE CEILING BINDS WHERE IT ACTUALLY DIFFERS FROM THE FRAMEWORK'S OWN
    // BUDGET-PLUS-MARGIN. At the smallest deadline a dynamic run may carry, D1
    // §7.2 R2 says 120 and `cjob::runner_job_spec` would say 30 + 90 — the same
    // number by construction, so the case that separates them is the one just
    // above the floor, where the ceiling clamps at 300 and the framework would
    // not.
    let (long_job, long_plan) = sel::discovery_budget(3600).expect("an hour funds a discovery");
    let mut long = spec.clone();
    long.timeout_seconds = i64::from(long_plan) + 45;
    let long = sel::build_discovery_job(&long, &discovery_job(UID), long_job, "sha256:abc");
    assert_eq!(
        serde_json::to_value(&long).expect("serialisable")["spec"]["activeDeadlineSeconds"],
        json!(300),
        "the run's ceiling binds, not the framework's plan-plus-margin"
    );
    assert!(
        value["spec"]["ttlSecondsAfterFinished"].is_null(),
        "no TTL at creation: the relay lives on the pod"
    );
    assert_eq!(
        value["spec"]["template"]["spec"]["automountServiceAccountToken"],
        json!(false),
        "a check pod holds no Kubernetes token"
    );
    let text = value.to_string();
    assert!(
        !text.contains("logweir-signing-key") && !text.contains("logweir-s3"),
        "no signing key and no archive credential reaches a discovery pod: {text}"
    );
    let args: Vec<String> =
        serde_json::from_value(value["spec"]["template"]["spec"]["containers"][0]["args"].clone())
            .expect("argv");
    assert_eq!(args, cjob::runner_argv(), "D2 §4.2's argv, verbatim");
}

/// **THE DISCOVERY PLAN CARRIES NO CREDENTIAL, ASKS FOR THE INTERNAL TOPICS AND
/// NAMES NO EXPECTED TOPIC** — D1 §7.2 R2/R5 and D-SEAMS **S1**.
///
/// `includeInternal: true` is the opposite of an interactive discovery's
/// default and is deliberate: D1 §3.3 requires the frozen block to record the
/// internal topics BY NAME, and a runner that dropped them would leave the
/// controller with a count and nothing to show. The exclusion itself is the
/// controller's, twice over.
///
/// KILLS: a password, a Secret name or a CA body in a world-readable plan;
/// asking the runner to exclude the internal topics (which would empty
/// `internalExcluded.names`); a `maxTopics` at or below what a run may freeze,
/// which would turn "too many topics" into a silently truncated listing.
#[test]
fn the_discovery_plan_is_a_topic_inventory_with_no_credential() {
    let cluster = serde_json::from_str(&kafka_cluster_json(CLUSTER_ID)).expect("a KafkaCluster");
    let resolved =
        weirkeeper::connection::resolve(&cluster, weirkeeper::connection::ConnectionUse::Discovery)
            .expect("it resolves");
    let document = sel::plan_document(&resolved, UID, 210);
    document.validate().expect("the plan is contract-legal");
    assert_eq!(document.subject_uid, UID);

    let CheckRequest::TopicInventory(request) = &document.request else {
        panic!("a per-run discovery is a topicInventory check");
    };
    assert!(
        request.include_internal,
        "the names are the controller's to record"
    );
    assert!(request.expected_topics.is_empty());
    assert_eq!(request.max_topics, sel::DISCOVERY_MAX_TOPICS);
    assert!(
        request.max_topics as usize > sel::MAX_RESOLVED_TOPICS,
        "asking for more than a run may freeze is what makes the refusal precise"
    );
    assert_eq!(request.connection.principal, PRINCIPAL);
    assert_eq!(
        request.connection.password_env.as_deref(),
        Some("LOGWEIR_SOURCE_PASSWORD"),
        "the NAME of the variable, never a value"
    );

    let bytes = logweir_core::det_json::to_deterministic_json(&document).expect("serialisable");
    let text = String::from_utf8(bytes).expect("UTF-8");
    assert!(
        !text.contains("prod-sasl"),
        "no Secret name in a world-readable ConfigMap: {text}"
    );
    assert!(
        !text.to_lowercase().contains("password\":\""),
        "and no credential VALUE either — `ConnectionPlan` has no password field \
         at all, and this is the assertion that would notice if one appeared: {text}"
    );
    // A second render of the same inputs is byte-identical, which is what makes
    // the plan's 409 rule a comparison rather than a coin toss.
    let again =
        logweir_core::det_json::to_deterministic_json(&sel::plan_document(&resolved, UID, 210))
            .expect("serialisable");
    assert_eq!(again, text.as_bytes());
    // And it round-trips through the runner's own parser.
    let parsed: CheckPlan = serde_json::from_str(&text).expect("the runner parses it");
    assert_eq!(parsed.subject_uid, UID);
}

/// **A NAME NO KAFKA BROKER WOULD ACCEPT IS NOT ADOPTED** — D1 §3.3's
/// "Kafka-legal names `^[a-zA-Z0-9._-]{1,249}$` are re-validated before
/// freeze", D1 §7.2 R3.
///
/// This list comes off a runner's stdout, not out of a CRD field the API
/// server pattern-checked, and the glob rail covers six characters — not a
/// space, a slash, a control character or a 250-character name. A name the
/// cluster could not hold would otherwise freeze into an immutable plan and
/// fail opaquely inside the engine.
///
/// It is `DiscoveryResultUnreadable`, the NOT-retryable class: nothing is
/// wrong with the selection the operator wrote; the runner's output is what
/// did not verify.
///
/// KILLS: relying on the glob rail alone; accepting an over-length name;
/// reporting it as `InvalidTopicSelection` (which would blame the spec) or as
/// the retryable `DiscoveryFailed`; putting the raw entry into a status message
/// unbounded.
#[test]
fn a_relayed_name_that_is_not_kafka_legal_is_unreadable() {
    let exclusions = sel::Exclusions::default();
    for bad in [
        "orders eu",
        "orders/eu",
        "orders:9092",
        &"x".repeat(250),
        "ordërs",
    ] {
        let entries = [entry("orders", 6), entry(bad, 1)];
        let (state, message) = sel::resolved_selection(
            &observed(&entries, &exclusions, VisibilityState::Unknown),
            &exclusions,
            IncompleteDiscovery::BackUpVisibleTopics,
            NAME,
        )
        .expect_err("a name the broker could not hold never freezes");
        assert_eq!(
            state,
            TERMINAL_STATE_DISCOVERY_RESULT_UNREADABLE,
            "`{}`: {message}",
            bad.escape_debug()
        );
        assert!(
            message.contains("^[a-zA-Z0-9._-]{1,249}$"),
            "the message names the grammar: {message}"
        );
        // BOUNDED IN THE MESSAGE. A 250-character name is spec-derived text,
        // not a credential, but a condition is not a place for an unbounded
        // string.
        assert!(
            message.len() < 512,
            "the message is bounded: {}",
            message.len()
        );
    }

    // The glob rail does NOT cover this, which is why the second rail exists.
    assert!(logweir_core::guard::reject_glob_metacharacters(&["orders eu".to_string()]).is_ok());

    // AND THE FREEZE BOUNDARY REFUSES IT FOR EVERY PRODUCER, not only for the
    // discovery path.
    let cluster: weirkeeper::crds::kafka_cluster::KafkaCluster =
        serde_json::from_str(&kafka_cluster_json(CLUSTER_ID)).expect("a KafkaCluster");
    let mut illegal = weirkeeper::backup_execution::ResolvedSelection::named(&named_backup().spec);
    illegal.topics = vec!["orders eu".to_string()];
    let refusal = weirkeeper::controllers::backup::desired_execution_inputs_for(
        &named_backup(),
        &cluster,
        &illegal,
    )
    .expect_err("an illegal name never freezes");
    assert!(
        matches!(
            refusal,
            weirkeeper::controllers::backup::BackupError::Refused(
                weirkeeper::conditions::TERMINAL_STATE_INVALID_TOPIC_SELECTION,
                _
            )
        ),
        "{refusal}"
    );
}

/// **A `deadlineSeconds` THAT CANNOT FUND A DISCOVERY IS REFUSED UP FRONT** —
/// D1 §7.2 R2, and the operability half of it.
///
/// `spec.deadlineSeconds` has no `minimum` on the CRD, and the discovery Job's
/// deadline is `min(300, spec.deadlineSeconds)` with ninety seconds of that
/// spent on image pull, scheduling and container start. So `deadlineSeconds:
/// 60` — entirely legal, and reasonable for a small cluster — leaves the runner
/// under a second. Dispatching that Job produces `DiscoveryFailed` with nothing
/// naming the deadline; raising the Job's own deadline would break R2's
/// ceiling. The honest answer is to refuse, naming the field and the floor.
///
/// KILLS: clamping the runner's budget to one second and dispatching anyway;
/// refusing without naming `spec.deadlineSeconds`; creating the plan ConfigMap
/// or the Job before the check.
#[tokio::test]
async fn a_deadline_too_small_to_fund_a_discovery_is_refused_before_any_job() {
    let mut value = serde_json::to_value(visible_only()).expect("serialisable");
    value["spec"]["deadlineSeconds"] = json!(60);
    let b: Backup = serde_json::from_value(value).expect("a Backup");

    let (terminal, bodies) = reconcile_with(&b, start_routes()).await;
    assert_eq!(
        terminal.as_deref(),
        Some(weirkeeper::conditions::TERMINAL_STATE_EXECUTION_SPEC_INVALID),
        "{:?}",
        calls(&bodies)
    );
    assert!(
        posted_jobs(&bodies).is_empty() && posted_config_maps(&bodies).is_empty(),
        "nothing is created for a run that cannot discover: {:?}",
        calls(&bodies)
    );
    let status = patched_statuses(&bodies).pop().expect("a terminal status");
    let text = status.to_string();
    assert!(
        text.contains("spec.deadlineSeconds")
            && text.contains(&sel::MIN_DYNAMIC_DEADLINE_SECONDS.to_string()),
        "the refusal names the field and the floor: {status}"
    );

    // AND THE FLOOR ITSELF IS FUNDABLE: one second more than the refusal
    // boundary dispatches a Job.
    let mut value = serde_json::to_value(visible_only()).expect("serialisable");
    value["spec"]["deadlineSeconds"] = json!(sel::MIN_DYNAMIC_DEADLINE_SECONDS);
    let b: Backup = serde_json::from_value(value).expect("a Backup");
    let (terminal, bodies) = reconcile_with(&b, start_routes()).await;
    assert_eq!(terminal, None, "{:?}", calls(&bodies));
    assert_eq!(posted_jobs(&bodies).len(), 1);
}

/// **THE PROVENANCE NAMES ARE BOUNDED IN BYTES AS WELL AS IN COUNT** — D1 §3.3,
/// and the plan `ConfigMap`'s one-MiB ceiling.
///
/// `internalExcluded.names` and `excludedByRule.names` are a SAMPLE an operator
/// reads to answer "which ones?", not a store. The count bounds alone (50 and
/// 200) admit 250 names of 249 bytes — about 61 KiB of provenance beside a
/// topic list already bounded at 256 KiB, inside a `ConfigMap` bounded at one
/// MiB. The two lists therefore share one 16 KiB budget, spent in a fixed
/// order, and `truncated` says when the sample was cut.
///
/// **The counts stay exact whatever the sample does.** That is the property a
/// reader depends on, and the one a byte cap must not damage.
///
/// KILLS: dropping the byte bound; dropping the count bound (a byte bound alone
/// admits a hundred thousand one-character names); cutting the COUNT instead of
/// the sample; forgetting `truncated`; spending the budget in an order two runs
/// could disagree about.
#[test]
fn the_provenance_name_lists_are_bounded_in_bytes_and_in_count() {
    let long = "x".repeat(200);
    let exclusions = sel::Exclusions {
        topics: Vec::new(),
        prefixes: vec![long.clone()],
    };
    let mut entries = vec![entry("orders", 6)];
    // 40 internal names of 200 bytes = 8,000 bytes, inside both bounds.
    for i in 0..40 {
        entries.push(internal_entry(&format!("__{}{i:03}", "y".repeat(195))));
    }
    // 200 rule-excluded names of 203 bytes each = 40,600 bytes, far over the
    // shared budget's remainder.
    for i in 0..200 {
        entries.push(entry(&format!("{long}{i:03}"), 1));
    }
    let o = observed(&entries, &exclusions, VisibilityState::Unknown);
    let resolved = sel::resolved_selection(
        &o,
        &exclusions,
        IncompleteDiscovery::BackUpVisibleTopics,
        NAME,
    )
    .expect("it resolves");
    let d = resolved.selection.discovery.as_ref().expect("provenance");

    // THE COUNTS ARE EXACT.
    assert_eq!(d.internal_excluded.count, 40);
    assert_eq!(d.excluded_by_rule.count, 200);

    // THE SAMPLES ARE NOT, AND SAY SO.
    let internal_bytes: usize = d.internal_excluded.names.iter().map(String::len).sum();
    let excluded_bytes: usize = d.excluded_by_rule.names.iter().map(String::len).sum();
    assert!(
        internal_bytes + excluded_bytes <= sel::MAX_PROVENANCE_NAME_BYTES,
        "the two lists share one budget: {internal_bytes} + {excluded_bytes}"
    );
    assert!(
        d.excluded_by_rule.truncated,
        "the sample that ran out of budget says so: {} of 200 names",
        d.excluded_by_rule.names.len()
    );
    assert!(
        !d.internal_excluded.names.is_empty(),
        "and the first list still gets its share"
    );

    // DETERMINISTIC: the same observation freezes the same bytes, because the
    // budget is spent in a fixed order.
    let again = sel::resolved_selection(
        &observed(&entries, &exclusions, VisibilityState::Unknown),
        &exclusions,
        IncompleteDiscovery::BackUpVisibleTopics,
        NAME,
    )
    .expect("it resolves");
    assert_eq!(resolved.selection, again.selection);

    // AND THE COUNT BOUND IS STILL THERE: many short names are cut at 50/200,
    // not at the byte budget.
    let mut short = vec![entry("orders", 6)];
    for i in 0..300 {
        short.push(internal_entry(&format!("__i{i:03}")));
    }
    for i in 0..900 {
        short.push(entry(&format!("tmp-{i:03}"), 1));
    }
    let short_exclusions = sel::Exclusions {
        topics: Vec::new(),
        prefixes: vec!["tmp-".to_string()],
    };
    let d = sel::resolved_selection(
        &observed(&short, &short_exclusions, VisibilityState::Unknown),
        &short_exclusions,
        IncompleteDiscovery::BackUpVisibleTopics,
        NAME,
    )
    .expect("it resolves")
    .selection
    .discovery
    .expect("provenance");
    assert_eq!(d.internal_excluded.names.len(), sel::MAX_INTERNAL_NAMES);
    assert_eq!(d.excluded_by_rule.names.len(), sel::MAX_EXCLUDED_NAMES);
    assert_eq!(d.internal_excluded.count, 300);
    assert_eq!(d.excluded_by_rule.count, 900);
    assert!(d.internal_excluded.truncated && d.excluded_by_rule.truncated);
}

// ===========================================================================
// Double — the reconciler, over a route table that panics on a stray request
// ===========================================================================

/// **A DYNAMIC RUN STARTS ONE DISCOVERY JOB, CREATES NO RUNNER JOB, AND SAYS SO
/// ON THE OBJECT** — D1 §7.2 R2.
///
/// KILLS: creating the runner Job before the names exist; creating the Job
/// before its plan `ConfigMap` (a pod that stalls in `ContainerCreating` until
/// its deadline); a terminal phase while discovery is merely running; a status
/// write with no `resourceVersion` precondition.
#[tokio::test]
async fn a_dynamic_run_starts_one_discovery_job_and_no_runner_job() {
    let (terminal, bodies) = reconcile_with(&visible_only(), start_routes()).await;
    assert_eq!(terminal, None, "{:?}", calls(&bodies));

    let jobs = posted_jobs(&bodies);
    assert_eq!(jobs.len(), 1, "one Job: {:?}", calls(&bodies));
    assert_eq!(jobs[0]["metadata"]["name"], json!(discovery_job(UID)));
    assert_eq!(
        jobs[0]["metadata"]["ownerReferences"][0]["uid"],
        json!(UID),
        "owned by THIS Backup, so its cascade collects the Job"
    );
    assert_eq!(
        jobs[0]["metadata"]["ownerReferences"][0]["controller"],
        json!(true)
    );

    let maps = posted_config_maps(&bodies);
    assert_eq!(
        maps.len(),
        1,
        "only the discovery plan: {:?}",
        calls(&bodies)
    );
    assert_eq!(
        maps[0]["metadata"]["name"],
        json!(format!("{}-plan", discovery_job(UID)))
    );
    assert_eq!(maps[0]["immutable"], json!(true));
    assert_eq!(maps[0]["metadata"]["ownerReferences"][0]["uid"], json!(UID));

    // THE PLAN BEFORE THE JOB.
    let order: Vec<String> = calls(&bodies);
    let cm = order.iter().position(|c| c == "POST /apis/../configmaps");
    let _ = cm; // the paths are versioned; compare by suffix instead:
    let plan_at = bodies
        .iter()
        .position(|b| b.method == "POST" && path(&b.uri).ends_with("/configmaps"))
        .expect("the plan was POSTed");
    let job_at = bodies
        .iter()
        .position(|b| b.method == "POST" && path(&b.uri).ends_with("/jobs"))
        .expect("the Job was POSTed");
    assert!(plan_at < job_at, "the plan ConfigMap first: {order:?}");

    let status = patched_statuses(&bodies)
        .pop()
        .expect("a status is written");
    assert_eq!(status["status"]["phase"], json!(PHASE_RESOLVING));
    assert_eq!(
        status["metadata"]["resourceVersion"],
        json!("4242"),
        "D-SEAMS S7: every status write is a compare-and-set"
    );
    let condition = condition_of(&status, CONDITION_TOPICS_RESOLVED).expect("TopicsResolved");
    assert_eq!(condition["status"], json!("False"));
    assert_eq!(condition["reason"], json!(REASON_DISCOVERY_RUNNING));
}

/// **TWO DYNAMIC `Backup`s FREEZE TWO INDEPENDENT DISCOVERY RESULTS** —
/// D1 §12's "topic creation/deletion between runs".
///
/// Each run owns its own Job, its own plan and its own frozen list, named after
/// its own UID. A topic that appeared between them is in the second run's list
/// and in nothing the first run froze — which is the tracker's acceptance
/// criterion, and the reason a dynamic run discovers afresh rather than reading
/// a `TopicDiscovery` (D-SEAMS **S2**).
///
/// KILLS: sharing a discovery Job or a plan between runs; caching a resolution;
/// naming either object after anything but the run's own UID.
#[tokio::test]
async fn two_backups_freeze_two_discovery_results() {
    let first = [entry("orders", 6)];
    let (terminal, bodies) = reconcile_with(&visible_only(), resolved_routes(&first)).await;
    assert_eq!(terminal, None, "{:?}", calls(&bodies));
    let a = frozen_inputs(&bodies);
    assert_eq!(a["topics"], json!(["orders"]));

    // A SECOND run, a second UID, a topic created in between.
    let b2 = dynamic_backup(NAME_B, UID_B, Value::Null, "BackUpVisibleTopics");
    let second = [entry("audit", 1), entry("orders", 6)];
    let inventory = inventory_of(&second, CLUSTER_ID, false);
    let routes = finished_routes(
        NAME_B,
        UID_B,
        relay_log(&second, &inventory, UID_B),
        pod_list(vec![pod_object(
            POD,
            Some(DISCOVERY_JOB_UID),
            &discovery_job(UID_B),
        )]),
        CLUSTER_ID,
    );
    let (terminal, bodies_b) = reconcile_with(&b2, routes).await;
    assert_eq!(terminal, None, "{:?}", calls(&bodies_b));
    let b = frozen_inputs(&bodies_b);
    assert_eq!(
        b["topics"],
        json!(["audit", "orders"]),
        "a topic created between the runs enters the NEXT dynamic backup"
    );
    assert_ne!(a["topics"], b["topics"]);
    assert_eq!(
        a["selection"]["discovery"]["discoveryJob"],
        json!(discovery_job(UID))
    );
    assert_eq!(
        b["selection"]["discovery"]["discoveryJob"],
        json!(discovery_job(UID_B))
    );
    assert_ne!(
        a["selection"]["discovery"]["resultSha256"], b["selection"]["discovery"]["resultSha256"],
        "two observations, two result digests"
    );
}

/// **THE EXCLUDED AND INTERNAL COUNTS REACH BOTH THE FROZEN INPUTS AND THE
/// STATUS** — D1 §3.3, §7.6, and D1 §12's "excluded/internal topic" row.
///
/// The names live in the immutable inputs (bounded); the counts live in
/// `status.selection`, which is what an operator and the console read. They are
/// ONE projection of one block, so the two cannot disagree.
///
/// KILLS: writing the status counts from a second computation; leaving the
/// internal or excluded names out of the frozen document; putting the (
/// unbounded) names on the status.
#[tokio::test]
async fn excluded_and_internal_counts_reach_inputs_and_status() {
    let b = dynamic_backup(
        NAME,
        UID,
        json!({"topics": ["payments"], "prefixes": ["tmp-"]}),
        "BackUpVisibleTopics",
    );
    let entries = [
        entry("orders", 6),
        entry("payments", 3),
        entry("tmp-import", 1),
        internal_entry("__consumer_offsets"),
        denied_entry("hr-payroll"),
    ];
    let inventory = inventory_of(&entries, CLUSTER_ID, true);
    let routes = finished_routes(
        NAME,
        UID,
        relay_log(&entries, &inventory, UID),
        pod_list(vec![pod_object(
            POD,
            Some(DISCOVERY_JOB_UID),
            &discovery_job(UID),
        )]),
        CLUSTER_ID,
    );
    let (terminal, bodies) = reconcile_with(&b, routes).await;
    assert_eq!(terminal, None, "{:?}", calls(&bodies));

    let inputs = frozen_inputs(&bodies);
    assert_eq!(inputs["topics"], json!(["orders"]));
    let discovery = &inputs["selection"]["discovery"];
    assert_eq!(discovery["internalExcluded"]["count"], json!(1));
    assert_eq!(
        discovery["internalExcluded"]["names"],
        json!(["__consumer_offsets"])
    );
    assert_eq!(discovery["excludedByRule"]["count"], json!(2));
    assert_eq!(
        discovery["excludedByRule"]["names"],
        json!(["payments", "tmp-import"])
    );
    assert_eq!(discovery["limitedTopicCount"], json!(1));
    assert_eq!(discovery["visibleTopicCount"], json!(5));
    assert_eq!(
        inputs["selection"]["exclude"],
        json!({"topics": ["payments"], "prefixes": ["tmp-"]})
    );

    let selection_status = patched_statuses(&bodies)
        .into_iter()
        .find_map(|s| {
            let v = s["status"]["selection"].clone();
            (!v.is_null()).then_some(v)
        })
        .expect("status.selection is written at the freeze");
    assert_eq!(selection_status["mode"], json!("AllUserTopics"));
    assert_eq!(selection_status["coverage"], json!("VisibleUserTopicsOnly"));
    assert_eq!(selection_status["internalExcludedCount"], json!(1));
    assert_eq!(selection_status["excludedByRuleCount"], json!(2));
    assert_eq!(selection_status["limitedTopicCount"], json!(1));
    assert_eq!(selection_status["resolvedTopicCount"], json!(1));
    assert!(
        selection_status.get("names").is_none()
            && !selection_status.to_string().contains("__consumer_offsets"),
        "an unbounded name list never reaches a status: {selection_status}"
    );
}

/// **AN EMPTY RESOLUTION CREATES NO RUNNER JOB AND IS NOT RETRIED** —
/// D1 §7.2 R7, §7.5.
///
/// KILLS: creating a runner Job with an empty allowlist; requeueing a refusal
/// over a CEL-immutable spec; writing the refusal without saying why.
#[tokio::test]
async fn selection_empty_creates_no_runner_job_and_is_not_retried() {
    let entries = [internal_entry("__consumer_offsets")];
    let (terminal, bodies) = reconcile_with(&visible_only(), resolved_routes(&entries)).await;
    assert_eq!(
        terminal.as_deref(),
        Some(TERMINAL_STATE_SELECTION_EMPTY),
        "{:?}",
        calls(&bodies)
    );
    assert!(
        posted_jobs(&bodies).is_empty(),
        "no runner Job at all: {:?}",
        calls(&bodies)
    );
    assert!(
        posted_config_maps(&bodies).is_empty(),
        "and no run plan was frozen: {:?}",
        calls(&bodies)
    );
    let status = patched_statuses(&bodies).pop().expect("a terminal status");
    assert_eq!(status["status"]["phase"], json!("Failed"));
    assert_eq!(status["status"]["exitReason"], json!("operational"));
    assert!(
        status["status"]["exitCode"].is_null(),
        "nothing ran, so no exit code is invented: {status}"
    );
    let failed = condition_of(&status, "Failed").expect("Failed");
    assert_eq!(failed["reason"], json!(TERMINAL_STATE_SELECTION_EMPTY));
    let resolved = condition_of(&status, CONDITION_TOPICS_RESOLVED).expect("TopicsResolved");
    assert_eq!(
        resolved["reason"],
        json!(TERMINAL_STATE_SELECTION_EMPTY),
        "one patch says both true things (D1 §3.4): {status}"
    );
}

/// **`Refuse` FAILS THE RUN RATHER THAN BACKING UP PART OF A CLUSTER** —
/// D1 §7.2 R6, §7.4, PLAT-09.2's "ACL limitation".
///
/// Kafka omits topics a principal cannot describe, so a successful listing
/// alone is `unknown` and never proof. An operator who chose `Refuse` asked for
/// a failed run rather than a backup labelled "all user topics" that quietly
/// missed some; there is somebody who will restore from it.
///
/// KILLS: treating a clean listing as complete; running anyway and labelling it
/// later; reaching `AllUserTopicsAttested` without an attestation.
#[tokio::test]
async fn refuse_policy_fails_discovery_incomplete_without_runner_job() {
    let entries = [entry("orders", 6), denied_entry("secrets")];
    let (terminal, bodies) = reconcile_with(&refusing(), resolved_routes(&entries)).await;
    assert_eq!(
        terminal.as_deref(),
        Some(TERMINAL_STATE_DISCOVERY_INCOMPLETE),
        "{:?}",
        calls(&bodies)
    );
    assert!(
        posted_jobs(&bodies).is_empty(),
        "no runner Job: {:?}",
        calls(&bodies)
    );
    assert!(posted_config_maps(&bodies).is_empty(), "nothing was frozen");

    // And a CLEAN listing is `unknown`, which `Refuse` also refuses: a listing
    // that succeeded is not a listing that saw everything.
    let clean = [entry("orders", 6)];
    let (terminal, bodies) = reconcile_with(&refusing(), resolved_routes(&clean)).await;
    assert_eq!(
        terminal.as_deref(),
        Some(TERMINAL_STATE_DISCOVERY_INCOMPLETE),
        "a successful listing ALONE is `unknown`: {:?}",
        calls(&bodies)
    );
    let status = patched_statuses(&bodies).pop().expect("a terminal status");
    assert!(
        status.to_string().contains("Kafka omits topics"),
        "the message explains why a clean listing is not proof: {status}"
    );
}

/// **A VISIBLE-ONLY RUN IS LABELLED VISIBLE-ONLY, EVERYWHERE** — D1 §7.4,
/// D-SEAMS **S3**.
///
/// The label is derived from the visibility verdict and nothing else, and
/// `AllUserTopicsAttested` is the only value any surface may render as "all
/// topics".
///
/// KILLS: labelling a visible-only run `AllUserTopicsAttested`; computing the
/// coverage separately in the status and in the frozen document.
#[tokio::test]
async fn visible_only_policy_records_the_coverage_label() {
    let entries = [entry("orders", 6), entry("payments", 3)];
    let (terminal, bodies) = reconcile_with(&visible_only(), resolved_routes(&entries)).await;
    assert_eq!(terminal, None, "{:?}", calls(&bodies));

    let inputs = frozen_inputs(&bodies);
    assert_eq!(
        inputs["selection"]["coverage"],
        json!("VisibleUserTopicsOnly")
    );
    assert_eq!(inputs["selection"]["mode"], json!("AllUserTopics"));
    assert_eq!(
        inputs["selection"]["incompleteDiscovery"],
        json!("BackUpVisibleTopics")
    );
    assert_eq!(
        inputs["selection"]["discovery"]["visibility"],
        json!("unknown")
    );

    let selection_status = patched_statuses(&bodies)
        .into_iter()
        .find_map(|s| {
            let v = s["status"]["selection"].clone();
            (!v.is_null()).then_some(v)
        })
        .expect("status.selection");
    assert_eq!(selection_status["coverage"], json!("VisibleUserTopicsOnly"));
    assert!(
        !Coverage::VisibleUserTopicsOnly.claims_whole_cluster(),
        "and only one coverage value may ever be read as `all topics`"
    );
    assert!(Coverage::VisibleUserTopicsOnly
        .label()
        .contains("completeness not established"));
}

/// **A SOURCE THAT MOVED UNDER THE RESOLUTION IS REFUSED** — D1 §7.2 R4, and
/// PLAT-09.2's "discovery/execution race".
///
/// Two ways for it to move, and both are checked: the broker answered with a
/// different `clusterId` than the `KafkaCluster` has observed, or the saved
/// connection itself changed between the dispatch and the read. Either way the
/// names belong to a cluster this run did not resolve, and adopting them would
/// back up somebody else's topics under this run's receipt.
///
/// KILLS: comparing only the cluster id; comparing only the resolution; taking
/// the runner's `clusterId` as authoritative; adopting a result whose Job was
/// dispatched against a different connection.
#[tokio::test]
async fn source_change_between_discovery_and_freeze_is_refused() {
    // (a) the broker's cluster id is not the one the KafkaCluster observed.
    let entries = [entry("orders", 6)];
    let inventory = inventory_of(&entries, "a-completely-different-cluster", false);
    let routes = finished_routes(
        NAME,
        UID,
        relay_log(&entries, &inventory, UID),
        pod_list(vec![pod_object(
            POD,
            Some(DISCOVERY_JOB_UID),
            &discovery_job(UID),
        )]),
        CLUSTER_ID,
    );
    let (terminal, bodies) = reconcile_with(&visible_only(), routes).await;
    assert_eq!(
        terminal.as_deref(),
        Some(TERMINAL_STATE_SOURCE_CHANGED_DURING_RESOLUTION),
        "{:?}",
        calls(&bodies)
    );
    assert!(posted_jobs(&bodies).is_empty(), "nothing runs");

    // (b) the saved connection changed: the Job was dispatched against one
    // resolution and the freeze resolves another.
    let inventory = inventory_of(&entries, CLUSTER_ID, false);
    let mut routes = finished_routes(
        NAME,
        UID,
        relay_log(&entries, &inventory, UID),
        pod_list(vec![pod_object(
            POD,
            Some(DISCOVERY_JOB_UID),
            &discovery_job(UID),
        )]),
        CLUSTER_ID,
    );
    for route in &mut routes {
        if route.method == "GET" && route.path_suffix.ends_with(&discovery_job(UID)) {
            route.body = discovery_job_body(UID, UID, Some("Complete"), "sha256:stale");
        }
    }
    let (terminal, bodies) = reconcile_with(&visible_only(), routes).await;
    assert_eq!(
        terminal.as_deref(),
        Some(TERMINAL_STATE_SOURCE_CHANGED_DURING_RESOLUTION),
        "{:?}",
        calls(&bodies)
    );
    assert!(posted_jobs(&bodies).is_empty());
}

/// **A DISCOVERY FAILURE IS RETRYABLE, AND THE RETRY REDISCOVERS** —
/// D1 §7.2 R3, §7.7.
///
/// "Retryable" for a `Backup` means a NEW `Backup`: `spec` is CEL-immutable and
/// a terminal run is never restarted, so a retry is a fresh object — with a
/// fresh UID, and therefore a fresh discovery Job and a fresh listing. The
/// second half of this row is what makes that concrete: the retry asks the
/// cluster again rather than reusing anything.
///
/// KILLS: reporting an operational failure as `DiscoveryResultUnreadable`;
/// re-reading the failed run's result; reusing the first run's discovery Job
/// name for the retry.
#[tokio::test]
async fn discovery_failure_is_retryable_and_the_retry_rediscovers() {
    // A finished Job whose pod printed nothing a decoder can verify.
    let routes = finished_routes(
        NAME,
        UID,
        "runner: could not reach any broker\n".to_string(),
        pod_list(vec![pod_object(
            POD,
            Some(DISCOVERY_JOB_UID),
            &discovery_job(UID),
        )]),
        CLUSTER_ID,
    );
    let (terminal, bodies) = reconcile_with(&visible_only(), routes).await;
    assert_eq!(
        terminal.as_deref(),
        Some(TERMINAL_STATE_DISCOVERY_RESULT_UNREADABLE),
        "an unverifiable relay is the not-retryable class: {:?}",
        calls(&bodies)
    );
    assert!(posted_jobs(&bodies).is_empty());

    // THE RETRY IS A NEW `Backup`, and it discovers from scratch: a different
    // UID means a different discovery Job, and the pass that finds none starts
    // one.
    let retry = dynamic_backup(NAME_B, UID_B, Value::Null, "BackUpVisibleTopics");
    let routes = vec![
        Route {
            method: "GET",
            path_suffix: "/jobs/logweir-backup-nightly-20260916-030000",
            status: 404,
            body: not_found("jobs.batch", NAME_B),
        },
        Route {
            method: "GET",
            path_suffix: "/kafkaclusters/prod",
            status: 200,
            body: kafka_cluster_json(CLUSTER_ID),
        },
        Route {
            method: "GET",
            path_suffix: "/jobs/lwd-3f1c8a5e-0000-4000-8000-0000000000b2",
            status: 404,
            body: not_found("jobs.batch", &discovery_job(UID_B)),
        },
        Route {
            method: "POST",
            path_suffix: "/configmaps",
            status: 201,
            body: run_plan_body(NAME_B, UID_B),
        },
        Route {
            method: "POST",
            path_suffix: "/jobs",
            status: 201,
            body: runner_job_body(NAME_B, UID_B),
        },
        Route {
            method: "PATCH",
            path_suffix: "/backups/logweir-backup-nightly-20260916-030000/status",
            status: 200,
            body: serde_json::to_string(&retry).expect("serialisable"),
        },
    ];
    let (terminal, bodies) = reconcile_with(&retry, routes).await;
    assert_eq!(terminal, None, "{:?}", calls(&bodies));
    let jobs = posted_jobs(&bodies);
    assert_eq!(jobs.len(), 1);
    assert_eq!(
        jobs[0]["metadata"]["name"],
        json!(discovery_job(UID_B)),
        "the retry discovers in its OWN Job"
    );
}

/// **A FROZEN DYNAMIC `Backup` NEVER RUNS A SECOND DISCOVERY** — D1 §12,
/// §7.2's call-site gate.
///
/// A pass that re-enters the create branch because the Job was collected reads
/// the selection back out of the plan it was admitted with. Resolving again
/// would ask the cluster a question whose answer has moved on, and
/// `verify_frozen_config_map` would then refuse the run as a
/// `PlanConfigMapConflict` **on an archive that may be half written**.
///
/// KILLS: removing the `status.execution` gate; resolving before the gate;
/// building `desired` from a fresh discovery when a plan exists.
#[tokio::test]
async fn a_frozen_dynamic_backup_never_reruns_discovery() {
    // First, freeze one for real.
    let entries = [entry("orders", 6), entry("payments", 3)];
    let (_, bodies) = reconcile_with(&visible_only(), resolved_routes(&entries)).await;
    let frozen_cm = posted_config_maps(&bodies)
        .into_iter()
        .find(|cm| cm["metadata"]["name"] == json!(format!("{NAME}-plan")))
        .expect("the run's plan ConfigMap");
    let recorded = patched_statuses(&bodies)
        .into_iter()
        .find(|s| !s["status"]["execution"].is_null())
        .expect("status.execution");

    // Now the object as that freeze left it, with its runner Job collected.
    let mut b = serde_json::to_value(visible_only()).expect("serialisable");
    b["status"] = recorded["status"].clone();
    let b: Backup = serde_json::from_value(b).expect("a Backup");

    let routes = vec![
        Route {
            method: "GET",
            path_suffix: "/jobs/logweir-backup-nightly-20260916-020000",
            status: 404,
            body: not_found("jobs.batch", NAME),
        },
        Route {
            method: "GET",
            path_suffix: "/kafkaclusters/prod",
            status: 200,
            body: kafka_cluster_json(CLUSTER_ID),
        },
        Route {
            method: "GET",
            path_suffix: "/configmaps/logweir-backup-nightly-20260916-020000-plan",
            status: 200,
            body: frozen_cm.to_string(),
        },
        Route {
            method: "POST",
            path_suffix: "/configmaps",
            status: 409,
            body: json!({"kind":"Status","apiVersion":"v1","status":"Failure",
                         "message":"already exists","reason":"AlreadyExists","code":409})
            .to_string(),
        },
        Route {
            method: "POST",
            path_suffix: "/jobs",
            status: 201,
            body: runner_job_body(NAME, UID),
        },
        Route {
            method: "PATCH",
            path_suffix: "/backups/logweir-backup-nightly-20260916-020000/status",
            status: 200,
            body: serde_json::to_string(&visible_only()).expect("serialisable"),
        },
    ];
    let (terminal, bodies) = reconcile_with(&b, routes).await;
    assert_eq!(
        terminal,
        None,
        "a frozen run continues from the plan it was admitted with: {:?}",
        calls(&bodies)
    );
    // THE ROUTE TABLE IS THE PROOF: there is no route for the discovery Job, no
    // route for a pod list and no route for a pod log, and the double PANICS on
    // a request it was not given a route for.
    assert!(
        !calls(&bodies)
            .iter()
            .any(|c| c.contains("lwd-") || c.contains("/pods")),
        "no discovery was re-run: {:?}",
        calls(&bodies)
    );
    let jobs = posted_jobs(&bodies);
    assert_eq!(jobs.len(), 1, "exactly one Job, the runner's");
    assert_eq!(jobs[0]["metadata"]["name"], json!(NAME));
}

/// **NO GLOB AND NO EMPTY LIST EVER REACHES `backup.yaml`** — guard **G-GLOB**,
/// D1 §3.3's freeze-boundary rails.
///
/// The rails are at the freeze boundary because W5's list comes from a runner's
/// stdout: whatever produced a `ResolvedSelection`, the last place to refuse an
/// empty or patterned allowlist is the one every producer goes through. And the
/// rendered `backup.yaml` of a real dynamic run carries the exact frozen names
/// and nothing that could be read as a pattern.
///
/// KILLS: removing either rail; rendering `spec.topics` (empty, in this mode)
/// into the spec document; expanding a pattern rather than refusing it.
#[tokio::test]
async fn no_glob_or_empty_list_reaches_backup_yaml() {
    let entries = [entry("orders", 6), entry("payments", 3)];
    let (_, bodies) = reconcile_with(&visible_only(), resolved_routes(&entries)).await;
    let cm = posted_config_maps(&bodies)
        .into_iter()
        .find(|cm| cm["metadata"]["name"] == json!(format!("{NAME}-plan")))
        .expect("the run's plan ConfigMap");
    let spec = cm["data"]["backup.yaml"]
        .as_str()
        .expect("the rendered spec");
    assert!(
        spec.contains("orders") && spec.contains("payments"),
        "the frozen names reach the engine: {spec}"
    );
    for metacharacter in ['*', '?', '['] {
        assert!(
            !spec.contains(metacharacter),
            "no pattern reaches backup.yaml: {spec}"
        );
    }
    let inputs = frozen_inputs(&bodies);
    assert_eq!(inputs["topics"], json!(["orders", "payments"]));
    assert!(
        !inputs["topics"].as_array().expect("a list").is_empty(),
        "and it is never empty"
    );

    // THE RAIL ITSELF, for any producer: an empty or patterned resolved list is
    // refused at the freeze boundary and never rendered.
    let cluster: weirkeeper::crds::kafka_cluster::KafkaCluster =
        serde_json::from_str(&kafka_cluster_json(CLUSTER_ID)).expect("a KafkaCluster");
    let mut empty = weirkeeper::backup_execution::ResolvedSelection::named(&named_backup().spec);
    empty.topics.clear();
    let refusal = weirkeeper::controllers::backup::desired_execution_inputs_for(
        &named_backup(),
        &cluster,
        &empty,
    )
    .expect_err("an empty resolved list never freezes");
    assert!(
        matches!(
            refusal,
            weirkeeper::controllers::backup::BackupError::Refused(
                TERMINAL_STATE_SELECTION_EMPTY,
                _
            )
        ),
        "{refusal}"
    );
    let mut globbed = weirkeeper::backup_execution::ResolvedSelection::named(&named_backup().spec);
    globbed.topics = vec!["orders*".to_string()];
    let refusal = weirkeeper::controllers::backup::desired_execution_inputs_for(
        &named_backup(),
        &cluster,
        &globbed,
    )
    .expect_err("a pattern never freezes");
    assert!(
        matches!(
            refusal,
            weirkeeper::controllers::backup::BackupError::Refused(
                weirkeeper::conditions::TERMINAL_STATE_INVALID_TOPIC_SELECTION,
                _
            )
        ),
        "{refusal}"
    );
}

/// **A NAMED ALLOWLIST IS UNTOUCHED, AND RECORDS `NamedTopics`** — D1 §7.1,
/// §12's "preserve named allowlists" row.
///
/// The one thing this whole task must not break. A named run reads no cluster
/// for its selection, creates no discovery Job and claims nothing about the
/// cluster — and the route table is the proof, because a discovery GET would
/// panic as an unrecorded request.
///
/// KILLS: running discovery for a named run; changing the coverage a named run
/// records; sorting or rewriting `spec.topics`.
#[tokio::test]
async fn named_mode_records_named_topics_coverage() {
    let routes = vec![
        Route {
            method: "GET",
            path_suffix: "/jobs/logweir-backup-nightly-20260916-020000",
            status: 404,
            body: not_found("jobs.batch", NAME),
        },
        Route {
            method: "GET",
            path_suffix: "/kafkaclusters/prod",
            status: 200,
            body: kafka_cluster_json(CLUSTER_ID),
        },
        Route {
            method: "POST",
            path_suffix: "/configmaps",
            status: 201,
            body: run_plan_body(NAME, UID),
        },
        Route {
            method: "GET",
            path_suffix: "/configmaps/logweir-backup-nightly-20260916-020000-plan",
            status: 404,
            body: not_found("configmaps", &format!("{NAME}-plan")),
        },
        Route {
            method: "POST",
            path_suffix: "/jobs",
            status: 201,
            body: runner_job_body(NAME, UID),
        },
        Route {
            method: "PATCH",
            path_suffix: "/backups/logweir-backup-nightly-20260916-020000/status",
            status: 200,
            body: serde_json::to_string(&named_backup()).expect("serialisable"),
        },
    ];
    let (terminal, bodies) = reconcile_with(&named_backup(), routes).await;
    assert_eq!(terminal, None, "{:?}", calls(&bodies));
    assert!(
        !calls(&bodies).iter().any(|c| c.contains("lwd-")),
        "a named run never discovers: {:?}",
        calls(&bodies)
    );
    let inputs = frozen_inputs(&bodies);
    assert_eq!(
        inputs["topics"],
        json!(["orders", "payments"]),
        "spec order, verbatim"
    );
    assert_eq!(inputs["selection"]["mode"], json!("SelectedTopics"));
    assert_eq!(inputs["selection"]["coverage"], json!("NamedTopics"));
    assert!(
        inputs["selection"]["discovery"].is_null(),
        "a named run has no discovery to record"
    );
    assert!(
        !patched_statuses(&bodies)
            .iter()
            .any(|s| condition_of(s, CONDITION_TOPICS_RESOLVED).is_some()),
        "and no TopicsResolved condition: the condition is dynamic-mode only"
    );
}

/// **AN IMPOSTOR POD IS NEVER READ** — D-SEAMS **S6**, defect `SEC-PODLOG`.
///
/// `batch.kubernetes.io/job-name` is writable by anything that can create a
/// pod, and a discovery pod's stdout becomes this run's ALLOWLIST and then a
/// signed receipt's topic list. The controller `ownerReference` UID decides
/// which pod may be read; the label only narrows the listing.
///
/// The impostor's log HAS a route, and it answers 200. That is the point: the
/// property is that the reconciler never asked for it, not that asking would
/// have failed.
///
/// KILLS: matching a pod by label; falling back to the label when no owned pod
/// is found; reading the newest pod that wears the label.
#[tokio::test]
async fn an_impostor_pod_is_never_read() {
    let entries = [entry("orders", 6)];
    let inventory = inventory_of(&entries, CLUSTER_ID, false);
    let routes = finished_routes(
        NAME,
        UID,
        relay_log(&entries, &inventory, UID),
        // The label is right; the owner is a different Job.
        pod_list(vec![pod_object(
            IMPOSTOR_POD,
            Some(OTHER_JOB_UID),
            &discovery_job(UID),
        )]),
        CLUSTER_ID,
    );
    let (terminal, bodies) = reconcile_with(&visible_only(), routes).await;
    assert!(
        !calls(&bodies)
            .iter()
            .any(|c| c.contains(&format!("/pods/{IMPOSTOR_POD}/log"))),
        "the impostor's log was never read: {:?}",
        calls(&bodies)
    );
    assert_eq!(
        terminal.as_deref(),
        Some(TERMINAL_STATE_DISCOVERY_RESULT_UNREADABLE),
        "with no provable pod there is no result, and nothing is inferred: {:?}",
        calls(&bodies)
    );
    assert!(posted_jobs(&bodies).is_empty(), "and nothing ran");
}

/// **A FOREIGN JOB AT THE DISCOVERY NAME IS NEVER OBSERVED** — the Job half of
/// the same rule.
///
/// KILLS: adopting a Job by name; reading the log of a pod owned by a Job this
/// `Backup` does not control.
#[tokio::test]
async fn a_discovery_job_this_backup_does_not_control_is_refused() {
    let entries = [entry("orders", 6)];
    let inventory = inventory_of(&entries, CLUSTER_ID, false);
    let mut routes = finished_routes(
        NAME,
        UID,
        relay_log(&entries, &inventory, UID),
        pod_list(vec![pod_object(
            POD,
            Some(DISCOVERY_JOB_UID),
            &discovery_job(UID),
        )]),
        CLUSTER_ID,
    );
    for route in &mut routes {
        if route.method == "GET" && route.path_suffix.ends_with(&discovery_job(UID)) {
            route.body = discovery_job_body(
                UID,
                OTHER_JOB_UID,
                Some("Complete"),
                &source_sha(CLUSTER_ID),
            );
        }
    }
    let (terminal, bodies) = reconcile_with(&visible_only(), routes).await;
    assert_eq!(
        terminal.as_deref(),
        Some(weirkeeper::conditions::TERMINAL_STATE_JOB_NAME_CONFLICT),
        "{:?}",
        calls(&bodies)
    );
    assert!(
        !calls(&bodies).iter().any(|c| c.contains("/pods")),
        "its pod was not even listed: {:?}",
        calls(&bodies)
    );
}

/// **THE DISCOVERY JOB'S TTL IS PATCHED ONLY AFTER THE FREEZE IS ON THE
/// OBJECT** — D1 §7.2 R9, D-SEAMS **S7**.
///
/// The relay lives on the pod and the TTL controller deletes a Job and its pods
/// together. A TTL set before `status.execution`/`status.selection` are
/// recorded lets garbage collection race a read a later pass may still have to
/// make; a run that then could not re-read its own discovery would be a run
/// whose archive is half written and whose plan cannot be verified.
///
/// KILLS: patching the TTL at Job creation; patching it before the status;
/// patching it on a pass that only read the plan back.
#[tokio::test]
async fn the_discovery_job_ttl_is_patched_only_after_the_status_landed() {
    let entries = [entry("orders", 6)];
    let (terminal, bodies) = reconcile_with(&visible_only(), resolved_routes(&entries)).await;
    assert_eq!(terminal, None, "{:?}", calls(&bodies));

    let ttl_at = bodies
        .iter()
        .position(|b| {
            b.method == "PATCH"
                && path(&b.uri).ends_with(&discovery_job(UID))
                && b.body.contains("ttlSecondsAfterFinished")
        })
        .expect("the discovery Job's TTL is patched");
    let selection_at = bodies
        .iter()
        .position(|b| {
            b.method == "PATCH" && path(&b.uri).ends_with("/status") && b.body.contains("selection")
        })
        .expect("the freeze is recorded");
    let resolved_at = bodies
        .iter()
        .position(|b| {
            b.method == "PATCH"
                && path(&b.uri).ends_with("/status")
                && b.body.contains(REASON_RESOLVED)
        })
        .expect("TopicsResolved=True is recorded");
    assert!(
        selection_at < resolved_at && resolved_at < ttl_at,
        "status.selection, then TopicsResolved, then the TTL: {:?}",
        calls(&bodies)
    );

    // The Job POST itself carried no TTL.
    let posted = posted_jobs(&bodies);
    assert!(
        posted
            .iter()
            .all(|j| j["spec"]["ttlSecondsAfterFinished"].is_null()),
        "no TTL at creation time"
    );

    // And the condition it wrote says the run resolved.
    let resolved = patched_statuses(&bodies)
        .iter()
        .find_map(|s| condition_of(s, CONDITION_TOPICS_RESOLVED))
        .expect("TopicsResolved");
    let _ = resolved;
    let last = patched_statuses(&bodies);
    let condition = last
        .iter()
        .rev()
        .find_map(|s| condition_of(s, CONDITION_TOPICS_RESOLVED))
        .expect("TopicsResolved survives the later patches");
    assert_eq!(condition["status"], json!("True"));
    assert_eq!(condition["reason"], json!(REASON_RESOLVED));
}

/// **PROBE CHURN ON THE `KafkaCluster` IS NOT A SOURCE CHANGE** — D1 §7.2 R1
/// and R4, and the one field the R4 digest must not pin.
///
/// `KafkaCluster.status.clusterId` is written asynchronously by ANOTHER
/// controller, and `controllers::kafka_cluster` clears it to `None` on every
/// probe pass that reports the cluster unreachable or whose output it could not
/// read. So it flips `Some → None → Some` inside the ≤ 300 s a discovery Job
/// runs. Hashing it into the resolution digest would refuse a perfectly good
/// run as `SourceChangedDuringResolution` — with a message saying the saved
/// connection changed when nothing about it had, no runner Job, and a new
/// `Backup` needed.
///
/// The repointed-endpoint case is still caught: `source_unchanged` compares the
/// broker-reported id against the observed one whenever BOTH are present, which
/// is the non-volatile form of the same rule.
///
/// KILLS: putting `status.clusterId` back into `SourceFacts`; making the
/// cluster-id comparison fire when either side is absent.
#[tokio::test]
async fn probe_churn_on_the_observed_cluster_id_is_not_a_source_change() {
    // The digest is a pure function of the connection and the cluster UID, and
    // of nothing the probe writes.
    let with_id: weirkeeper::crds::kafka_cluster::KafkaCluster =
        serde_json::from_str(&kafka_cluster_json(CLUSTER_ID)).expect("a KafkaCluster");
    let without_id: weirkeeper::crds::kafka_cluster::KafkaCluster =
        serde_json::from_str(&kafka_cluster_without_observed_id()).expect("a KafkaCluster");
    let resolved =
        weirkeeper::connection::resolve(&with_id, weirkeeper::connection::ConnectionUse::Discovery)
            .expect("it resolves");
    assert_eq!(
        sel::source_digest(&resolved, &with_id).expect("digests"),
        sel::source_digest(&resolved, &without_id).expect("digests"),
        "gaining or losing status.clusterId is not a change to the SOURCE"
    );

    // And a whole pass agrees: the Job was dispatched while the id was there,
    // the probe has since cleared it, and the run freezes.
    let entries = [entry("orders", 6)];
    let inventory = inventory_of(&entries, CLUSTER_ID, false);
    let mut routes = finished_routes(
        NAME,
        UID,
        relay_log(&entries, &inventory, UID),
        pod_list(vec![pod_object(
            POD,
            Some(DISCOVERY_JOB_UID),
            &discovery_job(UID),
        )]),
        CLUSTER_ID,
    );
    for route in &mut routes {
        if route.method == "GET" && route.path_suffix.ends_with("/kafkaclusters/prod") {
            route.body = kafka_cluster_without_observed_id();
        }
    }
    let (terminal, bodies) = reconcile_with(&visible_only(), routes).await;
    assert_eq!(
        terminal,
        None,
        "a cleared status.clusterId does not kill a good run: {:?}",
        calls(&bodies)
    );
    assert_eq!(frozen_inputs(&bodies)["topics"], json!(["orders"]));
}

/// **EVERY REFUSAL ARMS THE DISCOVERY JOB'S TTL, AND ONLY AFTER THE TERMINAL
/// STATUS LANDED** — D1 §7.2 R9's ordering, applied to the paths that do not
/// freeze.
///
/// A refused run has reached its conclusion, so the relay on the discovery pod
/// is no longer needed and the Job may be collected. Without this, a namespace
/// with a dynamic schedule against an under-permissioned principal accumulates
/// one finished Job, one pod and one plan `ConfigMap` per attempt.
///
/// KILLS: arming the TTL only on the resolved path; arming it before the
/// terminal status write; arming it on a refusal raised before the Job exists
/// (a 404 the route table would panic on).
#[tokio::test]
async fn every_refusal_leaves_the_discovery_job_collectable() {
    let ttl_path = format!("/jobs/{}", discovery_job(UID));
    // (backup, entries, cluster id the broker reports, expected state)
    let cases: Vec<(Backup, Vec<TopicEntry>, &str, &str)> = vec![
        (
            visible_only(),
            vec![internal_entry("__consumer_offsets")],
            CLUSTER_ID,
            TERMINAL_STATE_SELECTION_EMPTY,
        ),
        (
            refusing(),
            vec![entry("orders", 6)],
            CLUSTER_ID,
            TERMINAL_STATE_DISCOVERY_INCOMPLETE,
        ),
        (
            visible_only(),
            vec![entry("orders eu", 6)],
            CLUSTER_ID,
            TERMINAL_STATE_DISCOVERY_RESULT_UNREADABLE,
        ),
        (
            visible_only(),
            vec![entry("orders", 6)],
            "a-different-cluster",
            TERMINAL_STATE_SOURCE_CHANGED_DURING_RESOLUTION,
        ),
    ];
    for (b, entries, reported, expected) in cases {
        let inventory = inventory_of(&entries, reported, false);
        let routes = finished_routes(
            NAME,
            UID,
            relay_log(&entries, &inventory, UID),
            pod_list(vec![pod_object(
                POD,
                Some(DISCOVERY_JOB_UID),
                &discovery_job(UID),
            )]),
            CLUSTER_ID,
        );
        let (terminal, bodies) = reconcile_with(&b, routes).await;
        assert_eq!(terminal.as_deref(), Some(expected), "{:?}", calls(&bodies));
        let status_at = bodies
            .iter()
            .position(|r| r.method == "PATCH" && path(&r.uri).ends_with("/status"))
            .expect("a terminal status is written");
        let ttl_at = bodies
            .iter()
            .position(|r| {
                r.method == "PATCH"
                    && path(&r.uri).ends_with(&ttl_path)
                    && r.body.contains("ttlSecondsAfterFinished")
            })
            .unwrap_or_else(|| {
                panic!(
                    "{expected} leaves the Job collectable: {:?}",
                    calls(&bodies)
                )
            });
        assert!(
            status_at < ttl_at,
            "{expected}: the terminal status lands before the TTL: {:?}",
            calls(&bodies)
        );
    }

    // AND A REFUSAL RAISED BEFORE ANY JOB EXISTS PATCHES NOTHING — `start_routes`
    // has no PATCH route for the discovery Job, so the double would panic.
    let mut value = serde_json::to_value(visible_only()).expect("serialisable");
    value["spec"]["deadlineSeconds"] = json!(60);
    let early: Backup = serde_json::from_value(value).expect("a Backup");
    let (terminal, bodies) = reconcile_with(&early, start_routes()).await;
    assert_eq!(
        terminal.as_deref(),
        Some(weirkeeper::conditions::TERMINAL_STATE_EXECUTION_SPEC_INVALID)
    );
    assert!(
        !calls(&bodies)
            .iter()
            .any(|c| c.starts_with("PATCH") && c.contains("lwd-")),
        "no TTL for a Job that was never created: {:?}",
        calls(&bodies)
    );
}

/// **A FINISHED DISCOVERY WITH NO RECORDED FINISH INSTANT IS REFUSED, NOT
/// DATED FROM THE CLOCK** — the freeze's byte-stability, D1 §3.3.
///
/// `selection.discovery.observedAt` goes into an IMMUTABLE plan, and the plan's
/// digest is what `verify_frozen_config_map` compares on every later pass. A
/// value taken from `now` would differ between the pass that POSTed the plan
/// and any pass that re-renders it — turning a run that was fine into a
/// terminal `PlanConfigMapConflict` for no reason but the passage of time.
///
/// KILLS: falling back to `now` in `recorded_finish`; taking the instant from
/// anywhere the API server did not record it.
#[tokio::test]
async fn a_discovery_with_no_recorded_finish_instant_is_refused() {
    assert!(
        sel::recorded_finish(&undated_finished_job(), Some(&undated_pod())).is_none(),
        "no terminated finishedAt, no completionTime and no condition timestamp is no instant"
    );
    // The ordinary fixture DOES carry one, so the refusal is not vacuous.
    let dated: k8s_openapi::api::batch::v1::Job = serde_json::from_str(&discovery_job_body(
        UID,
        UID,
        Some("Complete"),
        &source_sha(CLUSTER_ID),
    ))
    .expect("a Job");
    let dated_pod: k8s_openapi::api::core::v1::Pod = serde_json::from_value(pod_object(
        POD,
        Some(DISCOVERY_JOB_UID),
        &discovery_job(UID),
    ))
    .expect("a Pod");
    assert!(sel::recorded_finish(&dated, Some(&dated_pod)).is_some());

    let entries = [entry("orders", 6)];
    let inventory = inventory_of(&entries, CLUSTER_ID, false);
    let mut routes = finished_routes(
        NAME,
        UID,
        relay_log(&entries, &inventory, UID),
        pod_list(vec![serde_json::to_value(undated_pod()).expect("a Pod")]),
        CLUSTER_ID,
    );
    for route in &mut routes {
        if route.method == "GET" && route.path_suffix.ends_with(&discovery_job(UID)) {
            route.body = serde_json::to_string(&undated_finished_job()).expect("serialisable");
        }
    }
    let (terminal, bodies) = reconcile_with(&visible_only(), routes).await;
    assert_eq!(
        terminal.as_deref(),
        Some(TERMINAL_STATE_DISCOVERY_RESULT_UNREADABLE),
        "{:?}",
        calls(&bodies)
    );
    assert!(posted_config_maps(&bodies).is_empty(), "nothing was frozen");
}

/// **A `v1`-FROZEN `Backup` IS UNTOUCHED BY ANY OF THIS** — D1 §3.3, §7.7.
///
/// A run frozen by a PLAT-06.1 controller carries a `v1` plan with no
/// `selection` block at all. It keeps executing: `stored_selection` answers
/// `None`, the shape check reads the named allowlist its `spec` still carries,
/// and no discovery happens. The alternative — refusing it — would turn every
/// in-flight `Backup` in the cluster terminal at the moment the new controller
/// starts.
///
/// KILLS: requiring a `selection` block to execute a run; running discovery for
/// a `v1` object; writing a coverage label onto a run that was frozen without
/// one.
#[test]
fn a_v1_frozen_backup_is_untouched() {
    let cluster: weirkeeper::crds::kafka_cluster::KafkaCluster =
        serde_json::from_str(&kafka_cluster_json(CLUSTER_ID)).expect("a KafkaCluster");
    let frozen =
        weirkeeper::controllers::backup::desired_execution_inputs(&named_backup(), &cluster)
            .expect("a named run freezes");
    let v1 = frozen.inputs.as_version_v1();
    assert!(
        v1.selection.is_none(),
        "a v1 document has no selection block"
    );

    let mut cm = json!({
        "apiVersion": "v1", "kind": "ConfigMap",
        "metadata": { "name": format!("{NAME}-plan"), "namespace": NS },
        "immutable": true,
        "data": {}
    });
    cm["data"][INPUTS_KEY] = json!(logweir_core::det_json::to_deterministic_json(&v1)
        .map(|b| String::from_utf8(b).expect("UTF-8"))
        .expect("serialisable"));
    let cm: k8s_openapi::api::core::v1::ConfigMap =
        serde_json::from_value(cm).expect("a ConfigMap");
    assert!(
        weirkeeper::backup_execution::stored_selection(&cm).is_none(),
        "a v1 plan yields no stored selection, so the fresh named resolution is used"
    );
    let fresh: SelectionInputs =
        weirkeeper::backup_execution::ResolvedSelection::named(&named_backup().spec).selection;
    assert_eq!(fresh.coverage, Coverage::NamedTopics);
    assert!(fresh.discovery.is_none());
}

/// **THE DISCOVERY JOB RUNS THE IMAGE THE PROCESS WAS CONFIGURED WITH** —
/// defect `D1-DISCOVERY-IMAGE`, measured live on 2026-09-18.
///
/// `LOGWEIR_RUNNER_IMAGE` and `LOGWEIR_RUNNER_PULL_POLICY` reach the
/// controller once, as `Context::runner_image`, and every Job this controller
/// creates takes them. The discovery Job did not: `resolve` never received a
/// `RunnerImage`, so `CheckJobSpec::{image, image_pull_policy}` were a
/// hard-coded `None` pair and the Job named `job::RUNNER_IMAGE` — a
/// compile-time digest — under `imagePullPolicy: Never`. On the lab that was
/// `ErrImageNeverPull`, then `DeadlineExceeded`, then
/// `TopicsResolved=False/DiscoveryFailed`, while the RUNNER Job of the same
/// namespace, the same controller and the same minute ran `logweir:scram-local`
/// and completed in four seconds. On a default chart install it is worse: the
/// runner Job gets a pullable tag and the discovery Job an unpublished digest
/// under `Never`, so PLAT-09.2's dynamic half is unusable everywhere.
///
/// **THE PROPERTY IS AN EQUALITY BETWEEN THE TWO JOBS, NOT A LITERAL.** Both
/// Jobs are POSTed by the same reconciler for the same run; the defect was
/// that they disagreed. So this reconciles twice with ONE `RunnerImage` — the
/// pass that starts the discovery, and the pass that freezes and starts the
/// runner — and holds the two containers to the same image and the same pull
/// policy.
///
/// KILLS: `image: None` / `image_pull_policy: None` at the discovery
/// `CheckJobSpec`; dropping either of the two threaded parameters; passing
/// `RunnerImage::default()` at the `controllers::backup` call site.
#[tokio::test]
async fn the_discovery_job_runs_the_configured_runner_image() {
    let configured = weirkeeper::job::RunnerImage {
        image: Some("logweir:scram-local".to_string()),
        image_pull_policy: Some("IfNotPresent".to_string()),
    };

    // PASS 1 — no discovery Job exists, so this pass POSTs it.
    let started = reconcile_with_runner_image(&visible_only(), start_routes(), &configured).await;
    let discovery = posted_jobs(&started);
    assert_eq!(discovery.len(), 1, "one Job: {:?}", calls(&started));
    assert_eq!(
        discovery[0]["metadata"]["name"],
        json!(discovery_job(UID)),
        "the Job this pass POSTed is the DISCOVERY Job"
    );
    let discovery_container = &discovery[0]["spec"]["template"]["spec"]["containers"][0];
    assert_eq!(
        discovery_container["name"],
        json!(weirkeeper::job::CONTAINER_NAME)
    );
    assert_eq!(
        discovery_container["image"],
        json!("logweir:scram-local"),
        "the discovery Job names the image the OPERATOR configured, never the compile-time pin: \
         a node that does not hold {} answers ErrImageNeverPull and the run dies DiscoveryFailed",
        weirkeeper::job::RUNNER_IMAGE
    );
    assert_ne!(
        discovery_container["image"],
        json!(weirkeeper::job::RUNNER_IMAGE),
        "D1-DISCOVERY-IMAGE: the pin is what this Job used to carry"
    );
    assert_eq!(
        discovery_container["imagePullPolicy"],
        json!("IfNotPresent"),
        "the pull policy override reaches this Job too; it used to stay at the compiled-in {}",
        weirkeeper::job::IMAGE_PULL_POLICY
    );

    // PASS 2 — the discovery finished, so this pass freezes it and POSTs the
    // RUNNER Job, from the same process and the same configuration.
    let resolved = reconcile_with_runner_image(
        &visible_only(),
        resolved_routes(&[entry("orders", 6)]),
        &configured,
    )
    .await;
    let runner = posted_jobs(&resolved);
    assert_eq!(runner.len(), 1, "one Job: {:?}", calls(&resolved));
    assert_eq!(
        runner[0]["metadata"]["name"],
        json!(NAME),
        "the Job this pass POSTed is the RUNNER Job"
    );
    let runner_container = &runner[0]["spec"]["template"]["spec"]["containers"][0];
    assert_eq!(
        discovery_container["image"], runner_container["image"],
        "ONE controller, ONE configured image: the discovery Job and the run's own Job cannot \
         name different images, which is exactly what the live run measured"
    );
    assert_eq!(
        discovery_container["imagePullPolicy"], runner_container["imagePullPolicy"],
        "and one configured pull policy"
    );
}

/// The other half of [`the_discovery_job_runs_the_configured_runner_image`]:
/// **an installation that configures NOTHING creates exactly the Job it
/// created before the override existed.**
///
/// `RunnerImage::default()` is `None`/`None`, and `None` means the compiled-in
/// constant — so this is the backward-compatibility arm, and it is why every
/// other row in this file (which reconciles through `reconcile_backup`) still
/// describes the shipped behaviour.
///
/// KILLS: defaulting the threaded image to something other than the pin;
/// making the override mandatory.
#[tokio::test]
async fn an_unconfigured_controller_leaves_the_discovery_job_on_the_pin() {
    let bodies = reconcile_with_runner_image(
        &visible_only(),
        start_routes(),
        &weirkeeper::job::RunnerImage::default(),
    )
    .await;
    let discovery = posted_jobs(&bodies);
    assert_eq!(discovery.len(), 1, "one Job: {:?}", calls(&bodies));
    let container = &discovery[0]["spec"]["template"]["spec"]["containers"][0];
    assert_eq!(
        container["image"],
        json!(weirkeeper::job::RUNNER_IMAGE),
        "no override means the shipped pin, unchanged"
    );
    assert_eq!(
        container["imagePullPolicy"],
        json!(weirkeeper::job::IMAGE_PULL_POLICY),
        "and the compiled-in pull policy, unchanged"
    );
}
