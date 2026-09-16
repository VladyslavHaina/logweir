//! The `KafkaCluster` probe reconciler: one Job, two stdout lines read by key
//! name, and a `status.reachable` that is never a guess.
//!
//! EVERY TEST HERE IS A PURE-FUNCTION TEST, A `mock_client` TEST OR A
//! SOURCE-READING TEST. Nothing dials a socket, nothing waits on a Job, nothing
//! runs `kubectl`. The double PANICS on a request it was not given a route for,
//! which is what makes a ZERO COUNT — no `pods/exec`, no `pods/attach`, no
//! `POST` at all on the refused path — mean "the reconciler did not ask" rather
//! than "the table forgot a route".
//!
//! READ `a_probe_log_with_no_contract_lines_is_not_a_guess` FIRST. It is the
//! property the whole module exists for: the probe's two lines are the contract,
//! and a log that carries neither leaves `reachable` UNSET. Global Constraint
//! 11's exit 1 covers an unreachable broker, a wrong image, a bad flag and an
//! unprojected credential alike, so a controller that inferred `false` from the
//! code would publish a control-plane mistake as a fact about somebody's
//! cluster.
//!
//! **Interface I28 is a declared late binding.** Reading the two lines needs
//! `get` on the `pods/log` subresource, and the ClusterRole that grants it is
//! Task 21's. Every test here runs against `testing::mock_client` and needs no
//! RBAC at all.

use chrono::{DateTime, TimeZone, Utc};
use serde_json::Value;
use weirkeeper::conditions::apply_merge_patch;
use weirkeeper::conditions::{
    TERMINAL_STATES, TERMINAL_STATE_CONNECTION_CONFIG_INVALID,
    TERMINAL_STATE_CREDENTIAL_NOT_RENDERABLE, TERMINAL_STATE_NAME_TOO_LONG,
};
use weirkeeper::connection::{resolve, ConnectionUse};
use weirkeeper::controllers::backup::{JOB_NAME_LABEL, JOB_NAME_LABEL_LEGACY, KEY_SCAN_TAIL_LINES};
use weirkeeper::controllers::kafka_cluster::{
    action_for, auth_mode_flag, crashed_status_patch, name_limit_for_cluster, observed_at,
    observed_status_patch, probe_job_name, probe_report, probe_started_patch, reconcile_cluster,
    refused_status_patch, runner_argv, runner_job_spec, verdict, ProbeReport, Requeue,
    CLUSTER_ID_PREFIX, CONDITION_REACHABLE, PROBE_CONDITION_REASONS, PROBE_DEADLINE_SECONDS,
    PROBE_JOB_PREFIX, PROBE_TTL_SECONDS, REACHABLE_PREFIX, REASON_PROBE_OUTPUT_UNREADABLE,
    REASON_PROBE_REPORTED_UNREACHABLE, REASON_PROBE_RUNNING, REASON_REACHABLE, REQUEUE_SECS,
    RE_PROBE_SECS, SOURCE_PASSWORD_ENV, SOURCE_PASSWORD_SECRET_KEY,
};
use weirkeeper::crds::kafka_cluster::{AuthMode, KafkaCluster, KafkaClusterStatus};
use weirkeeper::job;
use weirkeeper::testing::{mock_client_recording_bodies, Route, SeenBody};

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

/// The task's namespace (STANDING RULE 13).
const NS: &str = "logweir-t15c";

/// The `KafkaCluster`'s name. Deliberately well inside
/// `name_limit_for_cluster()` — see
/// `a_kafka_cluster_whose_name_is_too_long_is_refused_before_any_post` for the
/// other side.
const NAME: &str = "orders-prod";
/// The probe Job's name, spelled out rather than composed, so a change to
/// `probe_job_name` has to be argued for here as well as asserted there.
const JOB: &str = "logweir-probe-orders-prod";
/// The pod the job controller made.
const POD: &str = "logweir-probe-orders-prod-abcde";

const UID: &str = "7d1f4a52-0000-4000-8000-0000000000f1";

/// The cluster id interface **I14**'s first line carries on the reachable arm.
const CLUSTER_ID: &str = "ALLOWED0000000000000000";

const PLAINTEXT_AUTH: &str = r#"{ "mode": "plaintext", "tls": false }"#;
const SCRAM_AUTH: &str = r#"{ "mode": "scramSha512", "username": "logweir",
  "secretRef": { "name": "orders-sasl" }, "tls": true }"#;

fn cluster_json(name: &str, auth: &str, status: &str) -> String {
    format!(
        r#"{{
  "apiVersion": "logweir.dev/v1alpha1",
  "kind": "KafkaCluster",
  "metadata": {{ "name": "{name}", "namespace": "{NS}", "uid": "{UID}", "generation": 1 }},
  "spec": {{
    "bootstrapServers": ["b0.orders:9092", "b1.orders:9092"],
    "auth": {auth},
    "role": "source",
    "markerTopic": "logweir.scratch"
  }},
  "status": {status}
}}"#
    )
}

fn cluster() -> KafkaCluster {
    serde_json::from_str(&cluster_json(NAME, PLAINTEXT_AUTH, "{}"))
        .expect("the fixture is a KafkaCluster")
}

fn scram_cluster() -> KafkaCluster {
    serde_json::from_str(&cluster_json(NAME, SCRAM_AUTH, "{}"))
        .expect("the fixture is a KafkaCluster")
}

fn utc(y: i32, m: u32, d: u32, h: u32, min: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(y, m, d, h, min, 0)
        .single()
        .expect("the fixture instant exists")
}

fn now() -> DateTime<Utc> {
    utc(2026, 9, 10, 12, 0)
}

fn not_found_body(kind: &str, name: &str) -> String {
    format!(
        r#"{{"kind":"Status","apiVersion":"v1","status":"Failure",
  "message":"{kind} \"{name}\" not found","reason":"NotFound","code":404}}"#
    )
}

/// A finished Job, `Complete` or `Failed`.
fn job_body(condition: &str) -> String {
    format!(
        r#"{{"apiVersion":"batch/v1","kind":"Job",
  "metadata":{{"name":"{JOB}","namespace":"{NS}","uid":"bbbbbbbb-0000-4000-8000-0000000000b3"}},
  "spec":{{"template":{{"spec":{{"containers":[],"restartPolicy":"Never"}}}}}},
  "status":{{"conditions":[{{"type":"{condition}","status":"True",
     "lastProbeTime":"2026-09-10T11:59:00Z","lastTransitionTime":"2026-09-10T11:59:00Z"}}]}}}}"#
    )
}

/// A Job that exists and has not finished.
fn running_job_body() -> String {
    format!(
        r#"{{"apiVersion":"batch/v1","kind":"Job",
  "metadata":{{"name":"{JOB}","namespace":"{NS}","uid":"bbbbbbbb-0000-4000-8000-0000000000b3"}},
  "spec":{{"template":{{"spec":{{"containers":[],"restartPolicy":"Never"}}}}}},
  "status":{{"active":1}}}}"#
    )
}

/// A pod list holding one pod whose `runner` container terminated with
/// `exit_code`, preceded by an unrelated sidecar at index 0.
///
/// THE SIDECAR IS AT INDEX 0 ON PURPOSE. A suite whose happy path has `runner`
/// at index 0 cannot tell a by-name reader from a by-index one.
fn pod_list_terminated(exit_code: i32) -> String {
    format!(
        r#"{{"apiVersion":"v1","kind":"PodList","metadata":{{}},"items":[
  {{"apiVersion":"v1","kind":"Pod",
    "metadata":{{"name":"{POD}","namespace":"{NS}",
      "labels":{{"{JOB_NAME_LABEL}":"{JOB}","{JOB_NAME_LABEL_LEGACY}":"{JOB}"}}}},
    "spec":{{"containers":[]}},
    "status":{{"phase":"Succeeded","containerStatuses":[
      {{"name":"log-shipper","ready":false,"restartCount":0,"image":"x","imageID":"x",
        "state":{{"terminated":{{"exitCode":0,"finishedAt":"2026-09-10T11:59:00Z"}}}}}},
      {{"name":"runner","ready":false,"restartCount":0,"image":"x","imageID":"x",
        "state":{{"terminated":{{"exitCode":{exit_code},"finishedAt":"2026-09-10T11:59:00Z"}}}}}}
    ]}}}}]}}"#
    )
}

/// A pod list holding one pod whose `runner` never terminated.
fn pod_list_no_exit_code() -> String {
    format!(
        r#"{{"apiVersion":"v1","kind":"PodList","metadata":{{}},"items":[
  {{"apiVersion":"v1","kind":"Pod",
    "metadata":{{"name":"{POD}","namespace":"{NS}",
      "labels":{{"{JOB_NAME_LABEL}":"{JOB}"}}}},
    "spec":{{"containers":[]}},
    "status":{{"phase":"Failed","containerStatuses":[
      {{"name":"runner","ready":false,"restartCount":0,"image":"x","imageID":"x",
        "state":{{"running":{{"startedAt":"2026-09-10T11:59:00Z"}}}}}}
    ]}}}}]}}"#
    )
}

/// Interface **I14**'s two lines, in the contract's order.
fn i14_tail() -> String {
    format!("{CLUSTER_ID_PREFIX}{CLUSTER_ID}\n{REACHABLE_PREFIX}true\n")
}

/// A pod log: a couple of ordinary lines, then whatever `tail` says.
fn log_body(tail: &str) -> String {
    format!(
        "  WARN librdkafka: connecting to b0.orders:9092\n\
         probing b0.orders:9092,b1.orders:9092\n{tail}"
    )
}

/// The route table for a first pass: no Job yet, the `POST` and the status
/// `PATCH` available.
fn creating_routes() -> Vec<Route> {
    vec![
        Route {
            method: "GET",
            path_suffix: "/jobs/logweir-probe-orders-prod",
            status: 404,
            body: not_found_body("jobs.batch", JOB),
        },
        Route {
            method: "POST",
            path_suffix: "/jobs",
            status: 201,
            body: job_body("Complete"),
        },
        Route {
            method: "PATCH",
            path_suffix: "/kafkaclusters/orders-prod/status",
            status: 200,
            body: cluster_json(NAME, PLAINTEXT_AUTH, "{}"),
        },
    ]
}

/// The route table for a second pass: the Job has finished, the pod is
/// listable, its log is readable, the status is patchable and the Job takes its
/// TTL.
fn finished_routes(exit_code: i32, log: String) -> Vec<Route> {
    vec![
        Route {
            method: "GET",
            path_suffix: "/jobs/logweir-probe-orders-prod",
            status: 200,
            body: job_body("Complete"),
        },
        Route {
            method: "GET",
            path_suffix: "/pods",
            status: 200,
            body: pod_list_terminated(exit_code),
        },
        Route {
            method: "GET",
            path_suffix: "/pods/logweir-probe-orders-prod-abcde/log",
            status: 200,
            body: log,
        },
        Route {
            method: "PATCH",
            path_suffix: "/kafkaclusters/orders-prod/status",
            status: 200,
            body: cluster_json(NAME, PLAINTEXT_AUTH, "{}"),
        },
        Route {
            method: "PATCH",
            path_suffix: "/jobs/logweir-probe-orders-prod",
            status: 200,
            body: job_body("Complete"),
        },
    ]
}

fn path(uri: &str) -> &str {
    uri.split('?').next().unwrap_or(uri)
}

fn count(bodies: &[SeenBody], method: &str, suffix: &str) -> usize {
    bodies
        .iter()
        .filter(|b| b.method == method && path(&b.uri).ends_with(suffix))
        .count()
}

/// How many requests, of any method, named `needle` anywhere in their path.
fn mentioning(bodies: &[SeenBody], needle: &str) -> usize {
    bodies
        .iter()
        .filter(|b| path(&b.uri).contains(needle))
        .count()
}

fn patched_statuses(bodies: &[SeenBody]) -> Vec<Value> {
    bodies
        .iter()
        .filter(|b| b.method == "PATCH" && path(&b.uri).ends_with("/status"))
        .map(|b| serde_json::from_str::<Value>(&b.body).expect("a status patch is JSON"))
        .map(|v| v["status"].clone())
        .collect()
}

fn conditions_of(status: &Value) -> Vec<(String, String, String)> {
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

fn posted_job(bodies: &[SeenBody]) -> Value {
    let body = bodies
        .iter()
        .find(|b| b.method == "POST" && path(&b.uri).ends_with("/jobs"))
        .expect("the reconciler POSTed a Job");
    serde_json::from_str(&body.body).expect("a Job POST body is JSON")
}

// ---------------------------------------------------------------------------
// The reconcile, over the double
// ---------------------------------------------------------------------------

/// **The acceptance row.** One `POST …/jobs` whose body carries
/// `restartPolicy: "Never"`, `backoffLimit: 0`,
/// `automountServiceAccountToken: false` and an argv whose first element is
/// `cluster-probe`; then one `GET …/pods/<p>/log`; then one
/// `PATCH …/kafkaclusters/<name>/status` carrying `reachable: true` and
/// `clusterId`. Zero requests name `pods/exec` or `pods/attach`.
///
/// TWO PASSES, AND THAT IS THE SHAPE OF THE PROPERTY RATHER THAN A CONCESSION.
/// A reconciler cannot create a Job and read its finished pod's log in the same
/// pass — the pod does not exist yet — so the sequence the acceptance names is
/// one pass that creates and one pass that reads, each with its OWN recorder and
/// each with **exactly one** status `PATCH`. Asserting it over a single merged
/// recorder would be asserting a sequence no reconciler can produce.
#[tokio::test]
async fn kafka_cluster_reconcile_creates_a_probe_job_and_writes_status() {
    // ---- PASS 1: no Job -> POST, and a status that says a probe is running.
    let (client, _rec, bodies) = mock_client_recording_bodies(creating_routes());
    let outcome = reconcile_cluster(&cluster(), &client, now())
        .await
        .expect("the reconcile completes");
    let seen = bodies.lock().expect("the recorder is readable").clone();

    assert!(outcome.created, "pass 1 creates the probe Job");
    assert_eq!(
        count(&seen, "POST", "/jobs"),
        1,
        "exactly one POST to /jobs"
    );
    let job = posted_job(&seen);
    let pod_spec = &job["spec"]["template"]["spec"];
    assert_eq!(
        pod_spec["restartPolicy"].as_str(),
        Some("Never"),
        "restartPolicy Never: `OnFailure` restarts in place and then DELETES the pod, and the \
         exit code lives only on the pod"
    );
    assert_eq!(
        job["spec"]["backoffLimit"].as_i64(),
        Some(0),
        "backoffLimit 0: one Job yields exactly one pod, so one probe yields one answer"
    );
    assert_eq!(
        pod_spec["automountServiceAccountToken"].as_bool(),
        Some(false),
        "a probe pod makes ZERO Kubernetes API calls; a ServiceAccount is named, the TOKEN is \
         refused"
    );
    let argv: Vec<String> = pod_spec["containers"][0]["args"]
        .as_array()
        .expect("the container carries an argv")
        .iter()
        .map(|a| a.as_str().unwrap_or_default().to_string())
        .collect();
    assert_eq!(
        argv.first().map(String::as_str),
        Some("cluster-probe"),
        "the subcommand is `cluster-probe` — a purpose-built probe, not `doctor` with fewer \
         flags. Got {argv:?}"
    );
    assert!(
        !argv.iter().any(|a| a == "doctor"),
        "and it is not `doctor` under any flag list: {argv:?}"
    );
    let statuses = patched_statuses(&seen);
    assert_eq!(statuses.len(), 1, "exactly one status patch on pass 1");
    assert_eq!(
        conditions_of(&statuses[0]),
        vec![(
            CONDITION_REACHABLE.to_string(),
            "Unknown".to_string(),
            REASON_PROBE_RUNNING.to_string()
        )],
        "one condition, `Reachable=Unknown/ProbeRunning`: nothing is known yet, and an object \
         with NO status is indistinguishable from one this controller never saw"
    );
    assert!(
        statuses[0].get("reachable").is_none() && statuses[0].get("clusterId").is_none(),
        "a probe in flight neither invents an answer nor erases the last one: {}",
        statuses[0]
    );
    assert_eq!(
        action_for(&outcome),
        action_for(&outcome),
        "the action mapping is a function of the outcome"
    );
    assert_eq!(outcome.requeue, Requeue::After(REQUEUE_SECS));

    // ---- PASS 2: the Job has finished -> the log, the verdict, the TTL.
    let (client, _rec, bodies) =
        mock_client_recording_bodies(finished_routes(0, log_body(&i14_tail())));
    let outcome = reconcile_cluster(&cluster(), &client, now())
        .await
        .expect("the reconcile completes");
    let seen = bodies.lock().expect("the recorder is readable").clone();

    assert_eq!(
        count(&seen, "POST", "/jobs"),
        0,
        "pass 2 creates nothing: the Job it would create already exists"
    );
    assert_eq!(
        count(&seen, "GET", &format!("/pods/{POD}/log")),
        1,
        "exactly one GET on the `pods/log` subresource — the only route to a runner's stdout \
         (interface I28, granted by Task 21)"
    );
    let statuses = patched_statuses(&seen);
    assert_eq!(statuses.len(), 1, "exactly one status patch on pass 2");
    assert_eq!(
        statuses[0]["reachable"].as_bool(),
        Some(true),
        "`reachable: true`, because the pod printed `{REACHABLE_PREFIX}true`: {}",
        statuses[0]
    );
    assert_eq!(
        statuses[0]["clusterId"].as_str(),
        Some(CLUSTER_ID),
        "and the cluster id READ FROM THE BROKER, never from a spec: {}",
        statuses[0]
    );
    assert_eq!(
        conditions_of(&statuses[0]),
        vec![(
            CONDITION_REACHABLE.to_string(),
            "True".to_string(),
            REASON_REACHABLE.to_string()
        )],
        "exactly ONE condition (errata E5c: a condition array is a map keyed by `type`)"
    );
    assert_eq!(
        statuses[0]["reason"].as_str(),
        Some(REASON_REACHABLE),
        "the scalar `status.reason` is verbatim the condition's own (review finding M2)"
    );
    assert!(
        statuses[0].get("observedAt").is_some(),
        "and the observation carries its time: {}",
        statuses[0]
    );
    assert_eq!(outcome.reachable, Some(true));
    assert_eq!(outcome.cluster_id.as_deref(), Some(CLUSTER_ID));
    assert!(outcome.ttl_patched, "the Job took its TTL");
    assert_eq!(
        outcome.requeue,
        Requeue::After(RE_PROBE_SECS),
        "and the next look is a RE-PROBE, not a re-read"
    );

    // The zero counts, over BOTH passes' route tables. The double panics on an
    // unrouted request, so these are "the reconciler did not ask".
    for forbidden in ["pods/exec", "pods/attach"] {
        assert_eq!(
            mentioning(&seen, forbidden),
            0,
            "a probe never reaches into a pod: zero requests may name `{forbidden}`"
        );
    }
    assert_eq!(
        mentioning(&seen, "/secrets"),
        0,
        "and the controller reads NO Secret (spec §9) — which is the whole reason a probe is a \
         Job"
    );
}

/// **Nothing is guessed.** With a log body of `connection refused` the patched
/// status carries no `clusterId`, `reachable` is ABSENT, and the condition
/// reason is `ProbeOutputUnreadable`.
///
/// THE EXIT CODE IS 1 IN THIS ARM, DELIBERATELY. Exit 1 is the code a genuinely
/// unreachable broker produces — so a reconciler that derived `reachable: false`
/// from the code would pass every other row in this file and fail only here.
/// Global Constraint 11 gives 1 to a wrong image, a bad flag and an unprojected
/// credential as well, and none of those is a fact about somebody's cluster.
#[tokio::test]
async fn a_probe_log_with_no_contract_lines_is_not_a_guess() {
    let (client, _rec, bodies) =
        mock_client_recording_bodies(finished_routes(1, "connection refused".to_string()));
    let outcome = reconcile_cluster(&cluster(), &client, now())
        .await
        .expect("the reconcile completes");
    let seen = bodies.lock().expect("the recorder is readable").clone();
    let statuses = patched_statuses(&seen);
    assert_eq!(statuses.len(), 1, "exactly one status patch");
    assert!(
        statuses[0].get("clusterId").is_none(),
        "no cluster id was observed, so none is written — a derived one would point \
         `status.clusterId` at an id nothing returned, and Global Constraint 18's fourth rail \
         later compares that value against a target's: {}",
        statuses[0]
    );
    assert!(
        statuses[0].get("reachable").is_none(),
        "`reachable` is ABSENT, not `false`: {}",
        statuses[0]
    );
    assert_eq!(
        conditions_of(&statuses[0]),
        vec![(
            CONDITION_REACHABLE.to_string(),
            "Unknown".to_string(),
            REASON_PROBE_OUTPUT_UNREADABLE.to_string()
        )],
        "`Reachable=Unknown`, reason `ProbeOutputUnreadable` — a named observation, not a shrug"
    );
    assert_eq!(
        statuses[0]["reason"].as_str(),
        Some(REASON_PROBE_OUTPUT_UNREADABLE),
        "and the scalar says the same thing"
    );
    assert!(
        statuses[0]["conditions"][0]["message"]
            .as_str()
            .unwrap_or_default()
            .contains(&KEY_SCAN_TAIL_LINES.to_string()),
        "the message says how much of the log was looked at: {}",
        statuses[0]
    );
    assert_eq!(outcome.reachable, None);
    assert_eq!(outcome.cluster_id, None);
    assert!(
        outcome.ttl_patched,
        "the Job still takes its TTL — an unreadable probe has been read, and the next pass must \
         re-probe rather than re-read the same log"
    );
}

/// A probe that RAN and said `reachable=false` writes `false` — the arm the row
/// above must not be confused with.
#[tokio::test]
async fn a_probe_that_reported_unreachable_writes_false_and_no_cluster_id() {
    let tail = format!("{CLUSTER_ID_PREFIX}\n{REACHABLE_PREFIX}false\n");
    let (client, _rec, bodies) = mock_client_recording_bodies(finished_routes(1, log_body(&tail)));
    let outcome = reconcile_cluster(&cluster(), &client, now())
        .await
        .expect("the reconcile completes");
    let seen = bodies.lock().expect("the recorder is readable").clone();
    let statuses = patched_statuses(&seen);
    assert_eq!(
        statuses[0]["reachable"].as_bool(),
        Some(false),
        "the LINE said false, so the status says false: {}",
        statuses[0]
    );
    assert!(
        statuses[0].get("clusterId").is_none(),
        "interface I14 prints an EMPTY `{CLUSTER_ID_PREFIX}` on this arm, which is an absence \
         and is recorded as one — `Some(\"\")` in `status.clusterId` would make Global \
         Constraint 18's `!= target` rail compare two empty strings: {}",
        statuses[0]
    );
    assert_eq!(
        conditions_of(&statuses[0]),
        vec![(
            CONDITION_REACHABLE.to_string(),
            "False".to_string(),
            REASON_PROBE_REPORTED_UNREACHABLE.to_string()
        )]
    );
    assert_eq!(
        outcome.requeue,
        Requeue::After(RE_PROBE_SECS),
        "A FALSE PROBE IS A STATUS, NOT AN ERROR: it is written, the Job takes its TTL, and the \
         cluster is probed again on that cadence. Requeueing it as a failure would spin."
    );
    assert_eq!(outcome.reachable, Some(false));
}

/// **The Job is named from the CR**, and the owner reference is a controlling,
/// blocking one.
#[tokio::test]
async fn the_probe_job_is_named_from_the_cr() {
    let (client, _rec, bodies) = mock_client_recording_bodies(creating_routes());
    reconcile_cluster(&cluster(), &client, now())
        .await
        .expect("the reconcile completes");
    let seen = bodies.lock().expect("the recorder is readable").clone();
    let job = posted_job(&seen);
    assert_eq!(
        job["metadata"]["name"].as_str(),
        Some(JOB),
        "`{PROBE_JOB_PREFIX}<cr name>`"
    );
    assert_eq!(probe_job_name(NAME), JOB, "and the function agrees");
    let owner = &job["metadata"]["ownerReferences"][0];
    assert_eq!(owner["kind"].as_str(), Some("KafkaCluster"));
    assert_eq!(owner["name"].as_str(), Some(NAME));
    assert_eq!(owner["uid"].as_str(), Some(UID));
    assert_eq!(
        owner["controller"].as_bool(),
        Some(true),
        "controller: true — a Job with `false` can be adopted by something else"
    );
    assert_eq!(
        owner["blockOwnerDeletion"].as_bool(),
        Some(true),
        "blockOwnerDeletion: true — deleting the cluster object takes its probe Job with it"
    );
    assert_eq!(
        job["apiVersion"].as_str(),
        Some("batch/v1"),
        "and the owner's apiVersion comes from the derive, not a literal"
    );
    assert_eq!(owner["apiVersion"].as_str(), Some("logweir.dev/v1alpha1"));
}

/// A `KafkaCluster` whose name is too long for its probe Job's pod label is
/// refused TERMINALLY, **before any `POST`** — errata **E5d**, review finding
/// MEDIUM-1.
///
/// THE ROUTE TABLE HAS THE `POST` PRESENT. The double panics on an unrouted
/// request, so "zero POSTs" is only assertable against a table that could have
/// answered one.
#[tokio::test]
async fn a_kafka_cluster_whose_name_is_too_long_is_refused_before_any_post() {
    let limit = name_limit_for_cluster();
    assert_eq!(
        limit,
        63 - PROBE_JOB_PREFIX.len(),
        "the budget is the label's 63 characters minus the prefix this task adds"
    );
    let long = "c".repeat(limit + 1);
    let long_cluster: KafkaCluster =
        serde_json::from_str(&cluster_json(&long, PLAINTEXT_AUTH, "{}"))
            .expect("the fixture is a KafkaCluster");
    let routes = vec![
        Route {
            method: "POST",
            path_suffix: "/jobs",
            status: 201,
            body: job_body("Complete"),
        },
        Route {
            method: "PATCH",
            path_suffix: "/status",
            status: 200,
            body: cluster_json(&long, PLAINTEXT_AUTH, "{}"),
        },
    ];
    let (client, _rec, bodies) = mock_client_recording_bodies(routes);
    let outcome = reconcile_cluster(&long_cluster, &client, now())
        .await
        .expect("a refusal is an outcome, not an error");
    let seen = bodies.lock().expect("the recorder is readable").clone();

    assert_eq!(
        count(&seen, "POST", "/jobs"),
        0,
        "nothing is created: the API server would refuse the Job (`spec.template.labels … must \
         be no more than 63 characters`), and a requeue on that turns into `status: null` \
         FOREVER"
    );
    assert_eq!(
        count(&seen, "GET", "/jobs"),
        0,
        "and the name is checked BEFORE the Job is even looked for"
    );
    let statuses = patched_statuses(&seen);
    assert_eq!(statuses.len(), 1, "exactly one status patch");
    assert_eq!(
        conditions_of(&statuses[0]),
        vec![(
            CONDITION_REACHABLE.to_string(),
            "Unknown".to_string(),
            TERMINAL_STATE_NAME_TOO_LONG.to_string()
        )],
        "`Unknown` and not `False`: a name that is too long says nothing about whether the \
         broker is up"
    );
    assert_eq!(
        statuses[0]["reason"].as_str(),
        Some(TERMINAL_STATE_NAME_TOO_LONG)
    );
    assert!(
        statuses[0].get("reachable").is_none(),
        "and `reachable` is left alone: {}",
        statuses[0]
    );
    let message = statuses[0]["conditions"][0]["message"]
        .as_str()
        .unwrap_or_default();
    assert!(
        message.contains(&(limit + 1).to_string()) && message.contains("63"),
        "the message names the length it got and the limit: {message}"
    );
    assert_eq!(
        outcome.requeue,
        Requeue::AwaitChange,
        "TERMINAL: `metadata.name` cannot change, so a requeue would never succeed"
    );
    assert!(
        TERMINAL_STATES.contains(&TERMINAL_STATE_NAME_TOO_LONG),
        "and the state is the crate's own spelling, shared with the Backup path"
    );
}

/// A `KafkaCluster` whose saved connection does not resolve is refused BEFORE
/// any Job is looked for or created, and the refusal CLEARS `reachable` —
/// PLAT-07.1.
///
/// `reachable` is cleared because a `Restore` admits a target on
/// `status.reachable == true`: a stale `true`, written by an earlier controller
/// that dialled the same object with different settings, would otherwise let a
/// restore reach Job construction on an observation this controller does not
/// stand behind.
///
/// KILLS: resolve the connection after the Job `GET`, or leave `reachable`
/// standing on a refusal.
#[tokio::test]
async fn a_connection_that_does_not_resolve_is_refused_before_any_job() {
    // `plaintext` with `tls: true` — TLS without SASL, which earlier releases
    // dialled in the clear.
    let cluster: KafkaCluster = serde_json::from_str(&cluster_json(
        NAME,
        r#"{ "mode": "plaintext", "tls": true }"#,
        r#"{ "reachable": true, "clusterId": "OLDOBSERVATION000000001" }"#,
    ))
    .expect("the fixture is a KafkaCluster");
    let routes = vec![
        Route {
            method: "GET",
            path_suffix: "/jobs/logweir-probe-orders-prod",
            status: 404,
            body: not_found_body("jobs.batch", JOB),
        },
        Route {
            method: "POST",
            path_suffix: "/jobs",
            status: 201,
            body: job_body("Complete"),
        },
        Route {
            method: "PATCH",
            path_suffix: "/status",
            status: 200,
            body: cluster_json(NAME, PLAINTEXT_AUTH, "{}"),
        },
    ];
    let (client, _rec, bodies) = mock_client_recording_bodies(routes);
    let outcome = reconcile_cluster(&cluster, &client, now())
        .await
        .expect("a refusal is an outcome, not an error");
    let seen = bodies.lock().expect("the recorder is readable").clone();
    assert_eq!(
        count(&seen, "POST", "/jobs"),
        0,
        "no probe Job exists for a connection this controller will not dial"
    );
    assert_eq!(
        count(&seen, "GET", "/jobs/logweir-probe-orders-prod"),
        1,
        "the connection is resolved BEFORE the Job is looked for, and the Job is then looked \
         for exactly once — not to decide anything about this cluster, but because a refusal \
         does not make an earlier build's probe Job disappear (review finding L2). With none \
         there, nothing else happens."
    );
    assert_eq!(count(&seen, "PATCH", "/jobs/logweir-probe-orders-prod"), 0);
    assert!(!outcome.ttl_patched, "there was no Job to give a TTL to");
    let statuses = patched_statuses(&seen);
    assert_eq!(statuses.len(), 1, "exactly one status patch");
    assert_eq!(
        conditions_of(&statuses[0]),
        vec![(
            CONDITION_REACHABLE.to_string(),
            "Unknown".to_string(),
            TERMINAL_STATE_CONNECTION_CONFIG_INVALID.to_string()
        )],
    );
    assert!(
        statuses[0]["reachable"].is_null()
            && statuses[0]
                .as_object()
                .expect("an object")
                .contains_key("reachable"),
        "`reachable: null` REMOVES the stale observation under a merge patch: {}",
        statuses[0]
    );
    let message = statuses[0]["conditions"][0]["message"]
        .as_str()
        .unwrap_or_default();
    assert!(
        message.contains("auth.tls") && message.contains("refused rather than dialled"),
        "the message names the field and says what the earlier behaviour was: {message}"
    );
    assert_eq!(
        outcome.requeue,
        Requeue::AwaitChange,
        "`spec` is immutable, so nothing about this object will change by itself — but the          refusal is re-evaluated on every reconcile, so a controller that understands the          object clears it with no edit"
    );
}

/// **A REFUSED CONNECTION STILL ADOPTS THE PROBE JOB AN EARLIER BUILD LEFT
/// BEHIND** — PLAT-07.1 review finding L2.
///
/// Rolling a controller forward while a probe is in flight against an object
/// the new build refuses is not hypothetical: PLAT-07.1's own live swap moved
/// the lab's `missing-reference` from `ProbeReportedUnreachable` to
/// `CredentialNotRenderable` with its Job already created. Before this fix the
/// refusal `return`ed before the Job was even looked for, so a FINISHED one
/// never got the `ttlSecondsAfterFinished` that collects it and, with
/// `Requeue::AwaitChange` over a CEL-immutable `spec`, nothing ever looked
/// again.
///
/// TWO SHAPES, TWO ANSWERS, AND A THIRD THING THAT MUST NOT HAPPEN:
/// * finished → the same TTL STEP 3 writes, and `AwaitChange`;
/// * in flight → left alone, but the reconciler comes back on the requeue
///   clock so a later pass can collect it once it finishes;
/// * neither → its LOG IS NEVER READ and `reachable` is never written from
///   it. The Job dialled settings this build refuses to dial, so a verdict
///   from it would be an observation the controller does not stand behind —
///   which is the same reason the refusal clears `reachable`.
#[tokio::test]
async fn a_refused_connection_adopts_an_existing_probe_job_without_reading_it() {
    // `scramSha512` with no `secretRef` — the lab's `missing-reference` shape.
    let cluster: KafkaCluster = serde_json::from_str(&cluster_json(
        NAME,
        r#"{ "mode": "scramSha512", "username": "logweir", "tls": true }"#,
        r#"{ "reachable": true, "clusterId": "OLDOBSERVATION000000001" }"#,
    ))
    .expect("the fixture is a KafkaCluster");

    for (what, body, expect_ttl, expect_requeue) in [
        (
            "a finished Job",
            job_body("Complete"),
            true,
            Requeue::AwaitChange,
        ),
        (
            "an in-flight Job",
            running_job_body(),
            false,
            Requeue::After(REQUEUE_SECS),
        ),
    ] {
        let routes = vec![
            Route {
                method: "GET",
                path_suffix: "/jobs/logweir-probe-orders-prod",
                status: 200,
                body,
            },
            Route {
                method: "PATCH",
                path_suffix: "/jobs/logweir-probe-orders-prod",
                status: 200,
                body: job_body("Complete"),
            },
            Route {
                method: "PATCH",
                path_suffix: "/status",
                status: 200,
                body: cluster_json(NAME, PLAINTEXT_AUTH, "{}"),
            },
        ];
        let (client, _rec, bodies) = mock_client_recording_bodies(routes);
        let outcome = reconcile_cluster(&cluster, &client, now())
            .await
            .expect("a refusal is an outcome, not an error");
        let seen = bodies.lock().expect("the recorder is readable").clone();

        // The refusal itself is unchanged: no new Job, `reachable` cleared.
        assert_eq!(
            count(&seen, "POST", "/jobs"),
            0,
            "{what}: no probe is created"
        );
        let statuses = patched_statuses(&seen);
        assert_eq!(statuses.len(), 1, "{what}: exactly one status patch");
        assert_eq!(
            conditions_of(&statuses[0]),
            vec![(
                CONDITION_REACHABLE.to_string(),
                "Unknown".to_string(),
                TERMINAL_STATE_CREDENTIAL_NOT_RENDERABLE.to_string()
            )],
            "{what}"
        );
        assert!(statuses[0]["reachable"].is_null(), "{what}");

        // NOTHING WAS READ FROM THE JOB. The double panics on an unrouted
        // request, so these zeros are "the reconciler did not ask".
        assert_eq!(mentioning(&seen, "/pods"), 0, "{what}: no pod list, no log");
        assert_eq!(outcome.reachable, None, "{what}");
        assert_eq!(outcome.cluster_id, None, "{what}");

        // …and the Job itself is handled by its state.
        assert_eq!(
            count(&seen, "PATCH", "/jobs/logweir-probe-orders-prod"),
            usize::from(expect_ttl),
            "{what}: the TTL patch"
        );
        assert_eq!(outcome.ttl_patched, expect_ttl, "{what}");
        assert_eq!(
            outcome.requeue, expect_requeue,
            "{what}: an in-flight Job holds the reconciler on the clock so a later pass can \
             collect it; a finished one is already collected and `spec` cannot change"
        );
        if expect_ttl {
            let patch: Value = serde_json::from_str(
                &seen
                    .iter()
                    .find(|b| b.method == "PATCH" && path(&b.uri).ends_with(JOB))
                    .expect("the TTL patch")
                    .body,
            )
            .expect("the patch is JSON");
            assert_eq!(
                patch["spec"]["ttlSecondsAfterFinished"], PROBE_TTL_SECONDS,
                "the same TTL the observed path writes, so a refused probe is collected on the \
                 same schedule as a read one"
            );
        }
    }
}

/// A probe Job that finished with no terminated `runner` container leaves
/// `reachable` alone and names the sub-case.
#[tokio::test]
async fn a_crashed_probe_job_leaves_reachable_alone() {
    let routes = vec![
        Route {
            method: "GET",
            path_suffix: "/jobs/logweir-probe-orders-prod",
            status: 200,
            body: job_body("Failed"),
        },
        Route {
            method: "GET",
            path_suffix: "/pods",
            status: 200,
            body: pod_list_no_exit_code(),
        },
        Route {
            method: "PATCH",
            path_suffix: "/kafkaclusters/orders-prod/status",
            status: 200,
            body: cluster_json(NAME, PLAINTEXT_AUTH, "{}"),
        },
        // Present and unused: the log must NOT be read when there is no code.
        Route {
            method: "GET",
            path_suffix: "/pods/logweir-probe-orders-prod-abcde/log",
            status: 200,
            body: log_body(&i14_tail()),
        },
    ];
    let (client, _rec, bodies) = mock_client_recording_bodies(routes);
    let outcome = reconcile_cluster(&cluster(), &client, now())
        .await
        .expect("the reconcile completes");
    let seen = bodies.lock().expect("the recorder is readable").clone();
    assert_eq!(
        count(&seen, "GET", "/log"),
        0,
        "no exit code means the run's outcome is unrecoverable; the crashed-Job branch is taken \
         BEFORE the log read, so a log that happens to hold contract lines cannot be credited to \
         a pod that never terminated"
    );
    let statuses = patched_statuses(&seen);
    let conditions = conditions_of(&statuses[0]);
    assert_eq!(conditions.len(), 1, "exactly one condition");
    assert_eq!(conditions[0].1, "Unknown");
    assert!(
        TERMINAL_STATES.contains(&conditions[0].2.as_str()),
        "the reason is one of the crate's own crash states: {:?}",
        conditions[0]
    );
    assert!(
        statuses[0].get("reachable").is_none() && statuses[0].get("clusterId").is_none(),
        "and nothing about the cluster is written: {}",
        statuses[0]
    );
    assert!(
        !outcome.ttl_patched,
        "no TTL is patched on a path that wrote no verdict off a log"
    );
}

/// A probe Job that exists and has not finished is a running status and nothing
/// else.
#[tokio::test]
async fn a_running_probe_job_writes_only_that_a_probe_is_running() {
    let routes = vec![
        Route {
            method: "GET",
            path_suffix: "/jobs/logweir-probe-orders-prod",
            status: 200,
            body: running_job_body(),
        },
        Route {
            method: "PATCH",
            path_suffix: "/kafkaclusters/orders-prod/status",
            status: 200,
            body: cluster_json(NAME, PLAINTEXT_AUTH, "{}"),
        },
        Route {
            method: "POST",
            path_suffix: "/jobs",
            status: 201,
            body: job_body("Complete"),
        },
    ];
    let (client, _rec, bodies) = mock_client_recording_bodies(routes);
    let outcome = reconcile_cluster(&cluster(), &client, now())
        .await
        .expect("the reconcile completes");
    let seen = bodies.lock().expect("the recorder is readable").clone();
    assert_eq!(
        count(&seen, "POST", "/jobs"),
        0,
        "a probe already in flight is not probed again — the route was available"
    );
    assert_eq!(
        conditions_of(&patched_statuses(&seen)[0])[0].2,
        REASON_PROBE_RUNNING
    );
    assert_eq!(outcome.requeue, Requeue::After(REQUEUE_SECS));
    assert!(!outcome.created && !outcome.ttl_patched);
}

// ---------------------------------------------------------------------------
// The key scan — by name, from a bounded tail (erratum E4)
// ---------------------------------------------------------------------------

/// The two lines are read **by key name**, wherever they sit in the tail.
///
/// FOUR SHAPES, AND THE FOURTH IS THE ONE THE SUITE USED TO MISS (Task 17
/// review, Probe F): a diagnostic sentence arriving AFTER the contract lines,
/// which is what a merged pod log does in nondeterministic order (erratum E4).
/// "The last line of the pod log" is not a reader any controller here may be.
#[test]
fn the_two_lines_are_read_by_name_from_a_bounded_tail() {
    let expected = ProbeReport {
        cluster_id: Some(CLUSTER_ID.to_string()),
        reachable: Some(true),
    };

    // In the contract's order.
    assert_eq!(probe_report(&log_body(&i14_tail())), expected);

    // REVERSED. The scan matches on the key name, so the order of the two lines
    // in the LOG cannot change which field gets which value.
    let reversed = format!("{REACHABLE_PREFIX}true\n{CLUSTER_ID_PREFIX}{CLUSTER_ID}\n");
    assert_eq!(probe_report(&log_body(&reversed)), expected);

    // A DIAGNOSTIC AFTER THEM — the merged-stream shape.
    let then_stderr = format!("{}WARN librdkafka: shutting down\n", i14_tail());
    assert_eq!(probe_report(&log_body(&then_stderr)), expected);

    // BLANK LINES AND `\r\n` between them.
    let noisy = format!("{CLUSTER_ID_PREFIX}{CLUSTER_ID}\r\n\n\n{REACHABLE_PREFIX}true\r\n\n");
    assert_eq!(probe_report(&log_body(&noisy)), expected);

    // ABSENT: neither line, and nothing invented.
    assert_eq!(probe_report("connection refused"), ProbeReport::default());

    // BOUNDED: lines pushed past the tail are not found, which is the cost of a
    // bounded scan and the reason the subcommand prints its two lines LAST.
    let buried = format!(
        "{}{}",
        i14_tail(),
        (0..KEY_SCAN_TAIL_LINES)
            .map(|i| format!("noise {i}\n"))
            .collect::<String>()
    );
    assert_eq!(
        probe_report(&buried),
        ProbeReport::default(),
        "past the {KEY_SCAN_TAIL_LINES}-line tail the lines are gone; the bound is what stops a \
         200 MB log becoming the expensive part of a reconcile"
    );

    // The LAST occurrence of each key wins, as on the Backup path.
    let redrafted = format!(
        "{CLUSTER_ID_PREFIX}DRAFT\n{REACHABLE_PREFIX}false\n{}",
        i14_tail()
    );
    assert_eq!(probe_report(&redrafted), expected);
}

/// An EMPTY `cluster-id=` is an absence, and an unparseable `reachable=` is
/// `None` — never `false`.
#[test]
fn an_empty_or_unparseable_value_is_an_absence_and_never_a_false() {
    let r = probe_report(&format!("{CLUSTER_ID_PREFIX}\n{REACHABLE_PREFIX}false\n"));
    assert_eq!(r.cluster_id, None, "an empty id is not an observation");
    assert_eq!(r.reachable, Some(false));

    for bad in ["TRUE", "yes", "1", "", "tru"] {
        let r = probe_report(&format!("{REACHABLE_PREFIX}{bad}\n"));
        assert_eq!(
            r.reachable, None,
            "`{REACHABLE_PREFIX}{bad}` is not one of the contract's two values, and a reader \
             that took it for `false` would report a truncated log as an outage"
        );
    }
    assert_eq!(
        probe_report(&format!("{REACHABLE_PREFIX}true\n")).reachable,
        Some(true)
    );
    assert_eq!(
        probe_report(&format!("{REACHABLE_PREFIX}false\n")).reachable,
        Some(false)
    );
}

/// **The verdict cannot see the exit code.** Asserted structurally: `verdict`
/// takes one argument, so there is no code for it to derive `reachable` from.
#[test]
fn the_verdict_is_a_function_of_the_log_alone() {
    let unreadable = verdict(&ProbeReport::default());
    assert_eq!(unreadable.reachable, None);
    assert_eq!(unreadable.reason, REASON_PROBE_OUTPUT_UNREADABLE);
    assert_eq!(unreadable.status, "Unknown");

    let reachable = verdict(&ProbeReport {
        cluster_id: Some(CLUSTER_ID.to_string()),
        reachable: Some(true),
    });
    assert_eq!(reachable.reachable, Some(true));
    assert_eq!(reachable.cluster_id.as_deref(), Some(CLUSTER_ID));

    let unreachable = verdict(&ProbeReport {
        cluster_id: Some("STALE".to_string()),
        reachable: Some(false),
    });
    assert_eq!(
        unreachable.cluster_id, None,
        "an unreachable probe observed no id, so a `cluster-id=` line beside a \
         `{REACHABLE_PREFIX}false` is not carried onto the status"
    );

    // The source-level half: one parameter, so the code is unreachable from
    // here. A mutant that wanted to derive `false` from exit 1 would have to
    // change this signature first.
    let src = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/controllers/kafka_cluster.rs"),
    )
    .expect("the reconciler source is readable");
    assert!(
        src.contains("pub fn verdict(report: &ProbeReport) -> Verdict"),
        "`verdict` must take the report and nothing else"
    );
}

// ---------------------------------------------------------------------------
// The Job spec and the argv
// ---------------------------------------------------------------------------

/// Interface **I14**'s argv, built from the RESOLVED connection (PLAT-07.1) —
/// and `--marker-topic` passed through UNCONDITIONALLY.
#[test]
fn the_argv_is_interface_i14s_flag_list() {
    let plaintext =
        resolve(&cluster(), ConnectionUse::Probe).expect("the plaintext connection resolves");
    let scram =
        resolve(&scram_cluster(), ConnectionUse::Probe).expect("the SCRAM connection resolves");
    assert_eq!(
        runner_argv(&cluster(), &plaintext),
        vec![
            "cluster-probe",
            "--bootstrap",
            "b0.orders:9092,b1.orders:9092",
            "--auth-mode",
            "plaintext",
            "--marker-topic",
            "logweir.scratch",
        ]
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>(),
        "`--bootstrap` is ONE comma-joined value, and `--marker-topic` is passed through even \
         though the probe never asserts it: this controller does not branch on a field whose \
         only consumer is a drill-time guard"
    );

    assert_eq!(
        runner_argv(&scram_cluster(), &scram),
        vec![
            "cluster-probe",
            "--bootstrap",
            "b0.orders:9092,b1.orders:9092",
            "--auth-mode",
            "scramSha512",
            "--username",
            "logweir",
            "--tls",
            "--marker-topic",
            "logweir.scratch",
        ]
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>(),
        "`--tls` is emitted only when the transport really is TLS — a boolean flag has no false \
         form — and NO PASSWORD APPEARS ANYWHERE ON THE ARGV"
    );

    for a in runner_argv(&scram_cluster(), &scram) {
        assert!(
            !a.contains("password") && !a.contains("secret"),
            "the argv names no credential: {a}"
        );
    }
}

/// `--auth-mode`'s two values are the CRD enum's own spellings.
#[test]
fn the_auth_mode_flag_is_the_crd_enum_spelling() {
    assert_eq!(auth_mode_flag(AuthMode::Plaintext), "plaintext");
    assert_eq!(auth_mode_flag(AuthMode::ScramSha512), "scramSha512");
    // Byte-identical to what the CRD accepts, which is what lets the value be
    // copied rather than translated (interface I33).
    let v = serde_json::to_value(AuthMode::ScramSha512).expect("the enum serialises");
    assert_eq!(v.as_str(), Some(auth_mode_flag(AuthMode::ScramSha512)));
    let v = serde_json::to_value(AuthMode::Plaintext).expect("the enum serialises");
    assert_eq!(v.as_str(), Some(auth_mode_flag(AuthMode::Plaintext)));
}

/// The password is PROJECTED as an env reference, never mounted and never read.
#[test]
fn the_password_is_projected_as_a_secret_key_ref_and_never_read() {
    let spec = runner_job_spec(&scram_cluster()).expect("the Job spec builds");
    assert!(
        spec.secret_mounts.is_empty(),
        "a probe signs nothing and reads no approval, so it mounts no Secret at all unless the \
         connection names a private CA (PLAT-07.1)"
    );
    assert_eq!(spec.env_from_secret.len(), 1);
    let e = &spec.env_from_secret[0];
    assert_eq!(e.name, SOURCE_PASSWORD_ENV);
    assert_eq!(e.secret_name, "orders-sasl");
    assert_eq!(e.key, SOURCE_PASSWORD_SECRET_KEY);
    assert_eq!(spec.deadline_seconds, PROBE_DEADLINE_SECONDS);
    assert!(
        spec.plan_config_map.is_none(),
        "`cluster-probe` reads no `--spec`, and a Job with a `/plan` mount whose ConfigMap does \
         not exist stalls in ContainerCreating until its deadline fires"
    );

    // A plaintext cluster gets no variable...
    let spec = runner_job_spec(&cluster()).expect("the Job spec builds");
    assert!(spec.env_from_secret.is_empty());

    // ...and a `scramSha512` cluster with NO `secretRef` is REFUSED before a
    // Job exists (PLAT-07.1). It used to get a probe with no variable, which
    // then printed `reachable=false` about a configuration mistake — a fact
    // about this control plane reported as a fact about somebody's cluster.
    let no_secret: KafkaCluster = serde_json::from_str(&cluster_json(
        NAME,
        r#"{ "mode": "scramSha512", "username": "logweir", "tls": true }"#,
        "{}",
    ))
    .expect("the fixture is a KafkaCluster");
    let refusal = runner_job_spec(&no_secret).expect_err("no reference, no Job");
    assert!(
        refusal
            .to_string()
            .contains(TERMINAL_STATE_CREDENTIAL_NOT_RENDERABLE),
        "the refusal keeps the state a backup reported for the same object: {refusal}"
    );
}

/// The Job shape comes from `job::build` and is not rebuilt here.
#[test]
fn the_job_is_built_through_job_build() {
    let spec = runner_job_spec(&cluster()).expect("the Job spec builds");
    let built = job::build(&spec);
    let pod = built
        .spec
        .as_ref()
        .and_then(|s| s.template.spec.as_ref())
        .expect("the template has a pod spec");
    assert_eq!(pod.containers.len(), 1);
    assert_eq!(pod.containers[0].name, job::CONTAINER_NAME);
    assert_eq!(
        pod.containers[0].image.as_deref(),
        Some(job::RUNNER_IMAGE),
        "interface I15: the runner image is named in ONE place"
    );
    assert_eq!(
        built
            .spec
            .as_ref()
            .and_then(|s| s.ttl_seconds_after_finished),
        None,
        "NO TTL AT CREATION TIME: pod garbage collection must never race the log read"
    );
}

/// The env variable the controller projects is the one the subcommand reads.
///
/// ASSERTED ACROSS THE CRATE BOUNDARY BY SOURCE SCAN, NOT BY LINKAGE. Global
/// Constraint 27 keeps `weirkeeper` linking `logweir-verify` and `logweir-core`
/// only — it does not link `logweir`, so the constant cannot be imported — and
/// two spellings of one variable name is a probe that dials unauthenticated
/// while the Job spec looks correct.
#[test]
fn the_probe_password_variable_matches_the_subcommand() {
    let probe_rs = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../logweir/src/probe.rs");
    let src = std::fs::read_to_string(&probe_rs).expect("crates/logweir/src/probe.rs is readable");
    assert!(
        src.contains(&format!("\"{SOURCE_PASSWORD_ENV}\"")),
        "`logweir cluster-probe` must read the variable this controller projects \
         ({SOURCE_PASSWORD_ENV})"
    );
    // And the subcommand name on the argv is the one the CLI declares.
    let cli_rs = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../logweir/src/cli.rs"),
    )
    .expect("crates/logweir/src/cli.rs is readable");
    assert!(
        cli_rs.contains("ClusterProbe {"),
        "the argv's `cluster-probe` is clap's kebab-case rendering of `Command::ClusterProbe`, \
         which must exist"
    );
    for flag in ["bootstrap", "auth_mode", "username", "tls", "marker_topic"] {
        assert!(
            cli_rs.contains(&format!("{flag}:")),
            "the subcommand must declare `--{}`, which this controller's argv writes",
            flag.replace('_', "-")
        );
    }
}

// ---------------------------------------------------------------------------
// Status-shape invariants
// ---------------------------------------------------------------------------

/// Every patch this module builds writes **exactly one** condition and a scalar
/// `reason` equal to it.
///
/// ERRATA **E5c**: a condition array is a map keyed by `type`, so two
/// `Reachable` entries would be malformed however their statuses read — and the
/// day any task gives the array `x-kubernetes-list-type: map` the API server
/// would reject the patch. Review finding **M2**: the scalar exists so the state
/// is readable without parsing the condition array.
#[test]
fn every_status_write_is_one_condition_and_a_matching_scalar_reason() {
    let c = cluster();
    let patches = vec![
        probe_started_patch(&c, JOB, now()),
        observed_status_patch(
            &c,
            &verdict(&ProbeReport {
                cluster_id: Some(CLUSTER_ID.to_string()),
                reachable: Some(true),
            }),
            0,
            now(),
        ),
        observed_status_patch(&c, &verdict(&ProbeReport::default()), 1, now()),
        crashed_status_patch(&c, "NoExitCode", JOB, now()),
        refused_status_patch(&c, TERMINAL_STATE_NAME_TOO_LONG, "too long", now()),
    ];
    for p in &patches {
        let status = &p["status"];
        let conditions = conditions_of(status);
        assert_eq!(conditions.len(), 1, "exactly one condition in {p}");
        assert_eq!(
            conditions[0].0, CONDITION_REACHABLE,
            "and its type is `Reachable` in {p}"
        );
        assert_eq!(
            status["reason"].as_str(),
            Some(conditions[0].2.as_str()),
            "the scalar `reason` is verbatim the condition's own in {p}"
        );
        assert!(
            ["True", "False", "Unknown"].contains(&conditions[0].1.as_str()),
            "a metav1.Condition status is one of three values in {p}"
        );
        assert!(
            status["conditions"][0]["message"]
                .as_str()
                .is_some_and(|m| !m.is_empty()),
            "and the condition carries a message in {p}"
        );
    }

    // Source-level: a patch builder that wrote `conditions` and no `reason`
    // cannot be added without this failing.
    let src = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/controllers/kafka_cluster.rs"),
    )
    .expect("the reconciler source is readable");
    let conditions_keys = src.matches("\"conditions\"").count();
    let reason_keys = src.matches("\"reason\"").count();
    assert_eq!(
        reason_keys, conditions_keys,
        "every place this file writes a `conditions` key also writes a scalar `reason` key. A \
         patch builder added with `conditions` and no `reason` lands here as {conditions_keys} \
         conditions against {reason_keys} reasons."
    );
    // THE COUNT USED TO BE `conditions_keys + 1`, and the `+ 1` was the
    // `"reason"` STRING KEY inside this file's own `condition` builder. Task
    // 16b replaced that hand-built `json!({…})` with the serialised
    // `crds::Condition` the shared `conditions::merge_condition` returns — one
    // transition-time comparison for all six reconcilers, plan erratum
    // E11(d) — so the builder no longer spells the key and the counts are now
    // equal. Asserted rather than left implicit: if the builder ever stops
    // going through the shared merge, the equality above would silently start
    // meaning something else.
    assert!(
        src.contains("json!(merge_condition("),
        "the condition builder goes through `conditions::merge_condition`, which is why the \
         count above is an equality and not `+ 1`"
    );
}

/// Every condition `reason` this module writes is a valid `metav1.Condition`
/// reason — **errata E5b**: the upstream pattern forbids `-`.
#[test]
fn every_probe_condition_reason_is_a_valid_metav1_reason() {
    // `^[A-Za-z]([A-Za-z0-9_,:]*[A-Za-z0-9_])?$`, hand-checked rather than
    // regexed: Global Constraint 38 closes the workspace graph and this crate
    // takes no regex dependency.
    fn valid(r: &str) -> bool {
        let b = r.as_bytes();
        if b.is_empty() || !b[0].is_ascii_alphabetic() {
            return false;
        }
        if b.len() == 1 {
            return true;
        }
        let last = b[b.len() - 1];
        if !(last.is_ascii_alphanumeric() || last == b'_') {
            return false;
        }
        b[1..b.len() - 1]
            .iter()
            .all(|c| c.is_ascii_alphanumeric() || *c == b'_' || *c == b',' || *c == b':')
    }
    for r in PROBE_CONDITION_REASONS {
        assert!(
            valid(r),
            "`{r}` is not a valid metav1.Condition reason; `-` in particular is forbidden by the \
             upstream pattern (errata E5b)"
        );
        assert!(
            r.chars().next().is_some_and(char::is_uppercase),
            "`{r}` must be CamelCase"
        );
    }
    assert!(
        valid(TERMINAL_STATE_NAME_TOO_LONG),
        "and so is the shared terminal state this module reaches"
    );
    assert_eq!(
        PROBE_CONDITION_REASONS.len(),
        8,
        "the four probe verdicts — Reachable, ProbeReportedUnreachable, ProbeOutputUnreadable, \
         ProbeRunning — plus the four PLAT-07.1 saved-connection refusals this loop writes \
         before any Job exists, which are the shared terminal states and not a second vocabulary"
    );
    for r in PROBE_CONDITION_REASONS.iter().skip(4) {
        assert!(
            TERMINAL_STATES.contains(r),
            "`{r}` is written as a condition reason here, so it must be one of the shared \
             terminal states rather than a spelling only this module knows"
        );
    }
}

/// The re-probe cadence is the Job's TTL, and the requeue clears it.
#[test]
fn the_re_probe_cadence_is_the_jobs_ttl() {
    assert_eq!(
        PROBE_TTL_SECONDS, 300,
        "five minutes — the brief names no cadence, so this is the recorded choice"
    );
    assert_eq!(
        RE_PROBE_SECS,
        PROBE_TTL_SECONDS as u64 + 15,
        "the requeue lands AFTER the TTL has collected the Job — at exactly the TTL, half the \
         passes find it still present, write the same verdict again and wait another full \
         interval, so a five-minute cadence silently becomes a ten-minute one"
    );
    assert_eq!(
        REQUEUE_SECS, 15,
        "a probe in flight is looked at every 15 s, far more often than a finished one is \
         re-run — a run in flight is the state an operator is actually watching"
    );
}

/// `action_for` is the ONE place a `Requeue` becomes an `Action`.
#[test]
fn the_requeue_maps_onto_the_action_the_runtime_gets() {
    let src = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/controllers/kafka_cluster.rs"),
    )
    .expect("the reconciler source is readable");
    let code: String = src
        .lines()
        .filter(|l| {
            let t = l.trim_start();
            !(t.starts_with("//") || t.starts_with("///") || t.starts_with("//!"))
        })
        .collect::<Vec<_>>()
        .join("\n");
    assert_eq!(
        code.matches("Action::requeue(").count(),
        2,
        "twice: once inside `action_for` and once in `error_policy`, which is the transport-error \
         path and takes no `Requeue`"
    );
    assert_eq!(
        code.matches("Action::await_change()").count(),
        1,
        "and `await_change` exists exactly once, inside `action_for`"
    );
    // And the reconciler DELETES nothing.
    for token in [".delete(", "delete_opt(", "delete_collection("] {
        assert!(
            !code.contains(token),
            "a reconciler in this directory deletes nothing — the re-probe cadence is the Job's \
             own TTL, applied by the API server. Found `{token}`."
        );
    }
}

/// **The hot loop, and its regression test.** Two reconciles over the SAME
/// finished probe Job write BYTE-IDENTICAL status patches.
///
/// # What went wrong live, and what it cost
///
/// With `observedAt` taken from the controller's clock, the shipped reconciler
/// did **3,388 reconciles in ninety seconds** on docker-desktop against three
/// `KafkaCluster` objects. `controller().owns(jobs)` is half the mechanism and a
/// status field that changes on every read is the other: a patch carrying a
/// fresh `now` bumps `resourceVersion`, the object's own watch fires, the
/// reconcile re-reads the same finished Job, writes another fresh `now`, and the
/// loop feeds itself at ~2,000 passes a minute. (The TTL patch is innocent: a
/// merge patch whose content is unchanged bumps nothing.)
///
/// The fix is `observed_at`, which takes the instant from the PROBE — the
/// runner container's `finishedAt`, the Job's `completionTime`, or its terminal
/// condition — so the whole patch is a function of the Job and a re-read is a
/// no-op at the API server. This row is what stops the clock creeping back in:
/// it drives the real reconcile twice over one route table and compares the two
/// patches as bytes, with two DIFFERENT `now` values to make the point that the
/// controller's clock must not appear in the result at all.
#[tokio::test]
async fn the_observed_status_patch_is_stable_across_passes() {
    // A finished Job whose pod's `runner` reports a fixed `finishedAt`, which is
    // what the fixtures above already carry.
    let mut patches = Vec::new();
    for clock in [now(), utc(2026, 9, 10, 18, 30)] {
        let (client, _rec, bodies) =
            mock_client_recording_bodies(finished_routes(0, log_body(&i14_tail())));
        reconcile_cluster(&cluster(), &client, clock)
            .await
            .expect("the reconcile completes");
        let seen = bodies.lock().expect("the recorder is readable").clone();
        patches.push(patched_statuses(&seen)[0].clone());
    }
    assert_eq!(
        patches[0], patches[1],
        "two passes over the same finished Job must write the SAME bytes, whatever the \
         controller's clock says — otherwise every pass bumps `resourceVersion`, the object's \
         own watch fires, and the reconciler spins (measured live: 3,388 passes in 90 s)"
    );
    let observed = patches[0]["observedAt"]
        .as_str()
        .expect("the patch carries observedAt");
    assert_eq!(
        observed, "2026-09-10T11:59:00Z",
        "and the instant is the RUNNER CONTAINER's own `finishedAt`, not either clock: {observed}"
    );

    // The helper's four sources, in order, over the shipped fixtures.
    let job: k8s_openapi::api::batch::v1::Job =
        serde_json::from_str(&job_body("Complete")).expect("the fixture is a Job");
    let pods: kube::core::ObjectList<k8s_openapi::api::core::v1::Pod> =
        serde_json::from_str(&pod_list_terminated(0)).expect("the fixture is a PodList");
    assert_eq!(
        observed_at(&job, pods.items.first(), now()).to_rfc3339(),
        "2026-09-10T11:59:00+00:00",
        "source 1: the runner container's `finishedAt`"
    );
    let no_pod = observed_at(&job, None, now());
    assert_eq!(
        no_pod.to_rfc3339(),
        "2026-09-10T11:59:00+00:00",
        "source 3: the Job's terminal condition's `lastTransitionTime` when no pod is left \
         (this fixture carries no `completionTime`)"
    );
    // And `now` only when the API server offered nothing at all.
    let bare: k8s_openapi::api::batch::v1::Job = serde_json::from_str(
        r#"{"apiVersion":"batch/v1","kind":"Job","metadata":{"name":"x"},"status":{}}"#,
    )
    .expect("the fixture is a Job");
    assert_eq!(
        observed_at(&bare, None, now()),
        now(),
        "source 4, the fallback: nothing on the Job and no pod, so the controller's clock is all \
         there is"
    );
}

// ===========================================================================
// TASK 16b — THE STEADY-OBJECT ROW, plan erratum E11(d)
// ===========================================================================

/// A steady `KafkaCluster` is patched ONCE and then never again.
///
/// # Why this row exists on a reconciler that was already measured QUIET
///
/// Task 15c fixed this kind's instance of the family by taking the instant
/// from the PROBE rather than the clock — see
/// [`the_observed_status_patch_is_stable_across_passes`], which asserts the two
/// patches are the same BYTES. Quiet at the API server is not the same claim as
/// *silent*: identical bytes still travel as a `PATCH` on every pass, and the
/// API server's "this changed nothing" is what made it invisible. Task 16b's
/// third rule is that a reconcile whose computed status equals the stored one
/// sends nothing at all, and THIS is the row that can see the difference — a
/// route-table count, not a byte comparison.
///
/// The second object is not hand-written: it is the first pass's own patch,
/// applied to the first object exactly as the API server would apply it
/// (`conditions::apply_merge_patch`, RFC 7386), so the test cannot pass by
/// asserting over a status the reconciler would never have produced.
#[tokio::test]
async fn a_steady_kafka_cluster_issues_no_second_status_patch() {
    let (client, _rec, bodies) =
        mock_client_recording_bodies(finished_routes(0, log_body(&i14_tail())));
    reconcile_cluster(&cluster(), &client, now())
        .await
        .expect("the first reconcile completes");
    let first = bodies.lock().expect("the recorder is readable").clone();
    assert_eq!(
        count(&first, "PATCH", "/status"),
        1,
        "the first pass writes the verdict: {first:?}"
    );

    let mut stored = Value::Null;
    apply_merge_patch(&mut stored, &patched_statuses(&first)[0]);
    let mut steady = cluster();
    steady.status = Some(
        serde_json::from_value::<KafkaClusterStatus>(stored)
            .expect("the patched status is a KafkaClusterStatus — the API server stores it"),
    );

    // A DIFFERENT CLOCK, six and a half hours later, over the same finished
    // Job and the same log.
    let (client, _rec, bodies) =
        mock_client_recording_bodies(finished_routes(0, log_body(&i14_tail())));
    reconcile_cluster(&steady, &client, utc(2026, 9, 10, 18, 30))
        .await
        .expect("the second reconcile completes");
    let second = bodies.lock().expect("the recorder is readable").clone();
    assert_eq!(
        count(&second, "PATCH", "/status"),
        0,
        "the second pass over an unchanged object writes NOTHING. A status patch is what wakes \
         this reconciler, so a patch per pass is what a hot loop is made of — measured live at \
         376a09e on the two reconcilers that had no comparison at all: 4,654 and 2,557 \
         reconciles, and exactly as many resourceVersion bumps, in 90 s. Calls: {second:?}"
    );
}
