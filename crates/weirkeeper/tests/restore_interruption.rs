//! **PROD-07.2: the controller's part in an interrupted restore** — the
//! cancel, `Interrupted` from a run's exit or a lost pod, the reconcile Job
//! and how its end is read, the cleanup Job, the held point and the pool —
//! each through `reconcile_restore` over the recording mock API, asserting
//! the requests and their bodies (OD-23-core §3, rows IR-7 to IR-9 on the
//! controller's side).

use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use weirkeeper::controllers::restore::restore_interruption::{
    self as ri, CANCEL_ANNOTATION, CLEANUP_ANNOTATION,
};
use weirkeeper::controllers::restore::{reconcile_restore, unobserved_scorecard, Requeue};
use weirkeeper::crds::restore::Restore;
use weirkeeper::testing::{mock_client_recording_bodies, Route, SeenBody};
use weirkeeper::verification::unverified_evidence;

const NS: &str = "logweir-t20";
const NAME: &str = "rst-orders";
const UID: &str = "5c2e7b91-0000-4000-8000-0000000000a2";
const JOB_UID: &str = "bbbbbbbb-0000-4000-8000-0000000000b2";
const RUN_ID: &str = "01JRUN0000000000000000000A";
const RECONCILE_RUN: &str = "01JREC0000000000000000000B";
const RECONCILE_JOB: &str = "logweir-reconcile-5c2e7b91-0000-4000-8000-0000000000a2-1";
const CLEANUP_JOB: &str = "logweir-cleanup-5c2e7b91-0000-4000-8000-0000000000a2-1";

fn leak(s: String) -> &'static str {
    s.leak()
}

fn now() -> DateTime<Utc> {
    chrono::DateTime::parse_from_rfc3339("2026-10-16T12:00:00Z")
        .unwrap()
        .with_timezone(&Utc)
}

fn plan() -> String {
    "source:\n  storage: {backend: s3, bucket: b, prefix: p, region: us-east-1}\n  \
     backup: latestCompleted\n  topics: [orders]\n\
     target:\n  bootstrap_servers: [\"k:9092\"]\n  mode: newTopic\n  \
     topic_naming: {prefix: \"r-\"}\n\
     sample:\n  window_start: \"2026-10-13T00:00:00Z\"\n  \
     window_end: \"2026-10-13T01:00:00Z\"\n\
     objectives: {}\n\
     evidence: {backend: s3, bucket: e, prefix: logweir/, region: us-east-1}\n"
        .to_string()
}

fn restore_value(annotations: Value, status: Value) -> Value {
    json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "Restore",
        "metadata": {"name": NAME, "namespace": NS, "uid": UID, "generation": 1,
                     "resourceVersion": "100", "annotations": annotations,
                     "labels": {"logweir.dev/workspace": "dev"}},
        "spec": {
            "planBytes": plan(), "approvalRef": {"name": "a1"},
            "sourceArchive": {"url": "s3://b/p"}, "backupSetRef": "s",
            "pointInTime": "2026-10-13T01:00:00Z",
            "target": {"clusterRef": {"name": "target"}, "mode": "newTopic",
                       "topicNaming": {"prefix": "r-"}},
            "deadlineSeconds": 600
        },
        "status": status
    })
}

fn restore(value: &Value) -> Restore {
    serde_json::from_value(value.clone()).expect("a Restore")
}

fn owner() -> Value {
    json!([{"apiVersion": "logweir.dev/v1alpha1", "kind": "Restore", "name": NAME,
            "uid": UID, "controller": true, "blockOwnerDeletion": true}])
}

fn job(name: &str, uid: &str, args: Value, suspend: bool, status: Value) -> String {
    json!({
        "apiVersion": "batch/v1", "kind": "Job",
        "metadata": {"name": name, "namespace": NS, "uid": uid, "ownerReferences": owner()},
        "spec": {"backoffLimit": 0, "suspend": suspend, "activeDeadlineSeconds": 1800,
                 "template": {"spec": {"restartPolicy": "Never",
                     "containers": [{"name": "runner", "image": "runner:x", "args": args}]}}},
        "status": status
    })
    .to_string()
}

fn run_args() -> Value {
    json!([
        "restore",
        "run",
        "--spec",
        "/plan/restore.yaml",
        "--signing-key",
        "/signing/key.pem",
        "--run-id",
        RUN_ID
    ])
}

fn finished(condition: &str) -> Value {
    json!({"conditions": [{"type": condition, "status": "True",
        "lastTransitionTime": "2026-10-16T11:00:00Z"}]})
}

fn pods(job_name: &str, job_uid: &str, exit_code: Option<i32>) -> String {
    let items = match exit_code {
        None => json!([]),
        Some(code) => json!([{
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": format!("{job_name}-abcde"), "namespace": NS,
                "ownerReferences": [{"apiVersion": "batch/v1", "kind": "Job", "name": job_name,
                                     "uid": job_uid, "controller": true}],
                "labels": {"batch.kubernetes.io/job-name": job_name, "job-name": job_name}},
            "spec": {"containers": []},
            "status": {"phase": "Failed", "containerStatuses": [{
                "name": "runner", "ready": false, "restartCount": 0, "image": "x", "imageID": "x",
                "state": {"terminated": {"exitCode": code, "finishedAt": "2026-10-16T11:00:00Z"}}}]}
        }]),
    };
    json!({"apiVersion": "v1", "kind": "PodList", "metadata": {}, "items": items}).to_string()
}

fn running_pod(job_name: &str) -> String {
    json!({"apiVersion": "v1", "kind": "PodList", "metadata": {}, "items": [{
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": format!("{job_name}-abcde"), "namespace": NS,
            "ownerReferences": [{"apiVersion": "batch/v1", "kind": "Job", "name": job_name,
                                 "uid": JOB_UID, "controller": true}],
            "labels": {"batch.kubernetes.io/job-name": job_name, "job-name": job_name}},
        "spec": {"containers": []},
        "status": {"phase": "Running"}}]})
    .to_string()
}

fn route(method: &'static str, suffix: String, status: u16, body: String) -> Route {
    Route {
        method,
        path_suffix: leak(suffix),
        status,
        body,
    }
}

fn not_found() -> String {
    json!({"kind": "Status", "apiVersion": "v1", "status": "Failure", "reason": "NotFound",
           "code": 404, "message": "not found"})
    .to_string()
}

fn path(uri: &str) -> &str {
    uri.split('?').next().unwrap()
}

fn bodies_of(bodies: &[SeenBody], method: &str, suffix: &str) -> Vec<Value> {
    bodies
        .iter()
        .filter(|b| b.method.eq_ignore_ascii_case(method) && path(&b.uri).ends_with(suffix))
        .map(|b| serde_json::from_str(&b.body).unwrap_or(Value::Null))
        .collect()
}

fn status_patches(bodies: &[SeenBody]) -> Vec<Value> {
    bodies_of(bodies, "PATCH", &format!("/restores/{NAME}/status"))
        .into_iter()
        .map(|b| b["status"].clone())
        .collect()
}

async fn pass(value: &Value, routes: Vec<Route>) -> (Requeue, Vec<SeenBody>) {
    let (client, _rec, bodies) = mock_client_recording_bodies(routes);
    let outcome = reconcile_restore(
        &restore(value),
        &client,
        &unobserved_scorecard,
        &unverified_evidence,
        now(),
    )
    .await
    .expect("a pass");
    let seen = bodies.lock().unwrap().clone();
    (outcome.requeue, seen)
}

fn status_route(value: &Value) -> Route {
    route(
        "PATCH",
        format!("/restores/{NAME}/status"),
        200,
        value.to_string(),
    )
}

/// **A cancel before the Job exists**: `Failed`, `Cancelled`, and no Job is
/// ever created. KILLS: a cancel that creates the run anyway.
#[tokio::test]
async fn a_cancel_before_the_job_fails_cancelled() {
    let value = restore_value(
        json!({CANCEL_ANNOTATION: "alice"}),
        json!({"phase": "Pending"}),
    );
    let (_, seen) = pass(
        &value,
        vec![
            route("GET", format!("/jobs/{NAME}"), 404, not_found()),
            status_route(&value),
        ],
    )
    .await;
    assert!(bodies_of(&seen, "POST", "/jobs").is_empty());
    let status = status_patches(&seen).remove(0);
    assert_eq!(status["phase"], "Failed", "{status}");
    assert_eq!(status["reason"], "Cancelled", "{status}");
}

/// **A cancel while it runs suspends the run Job** and reports `Stopping`
/// while its pod is there: the writer is not gone yet, so nothing is
/// `Interrupted`. KILLS: a cancel that does not stop the Job; one that
/// reports the run stopped before its pod is gone.
#[tokio::test]
async fn a_cancel_while_running_suspends_the_job_and_reports_stopping() {
    let value = restore_value(
        json!({CANCEL_ANNOTATION: "alice"}),
        json!({"phase": "Running"}),
    );
    let (requeue, seen) = pass(
        &value,
        vec![
            route(
                "GET",
                format!("/jobs/{NAME}"),
                200,
                job(NAME, JOB_UID, run_args(), false, json!({"active": 1})),
            ),
            route(
                "PATCH",
                format!("/jobs/{NAME}"),
                200,
                job(NAME, JOB_UID, run_args(), true, json!({"active": 1})),
            ),
            route("GET", "/pods".into(), 200, running_pod(NAME)),
            status_route(&value),
        ],
    )
    .await;
    let suspended = bodies_of(&seen, "PATCH", &format!("/jobs/{NAME}"));
    assert_eq!(suspended, vec![json!({"spec": {"suspend": true}})]);
    let status = status_patches(&seen).remove(0);
    assert_eq!(status["phase"], "Running", "{status}");
    assert_eq!(status["reason"], "Stopping", "{status}");
    assert_eq!(requeue, Requeue::After(ri::POLL_SECONDS));
}

/// **Once no pod of the suspended run is left, it is `Interrupted`**, cause
/// `Cancelled`, naming the run's id for the reconcile.
#[tokio::test]
async fn a_cancelled_run_with_no_pod_left_is_interrupted() {
    let value = restore_value(
        json!({CANCEL_ANNOTATION: "alice"}),
        json!({"phase": "Running"}),
    );
    let (_, seen) = pass(
        &value,
        vec![
            route(
                "GET",
                format!("/jobs/{NAME}"),
                200,
                job(NAME, JOB_UID, run_args(), true, json!({})),
            ),
            route("GET", "/pods".into(), 200, pods(NAME, JOB_UID, None)),
            status_route(&value),
        ],
    )
    .await;
    assert!(bodies_of(&seen, "PATCH", &format!("/jobs/{NAME}")).is_empty());
    let status = status_patches(&seen).remove(0);
    assert_eq!(status["phase"], "Interrupted", "{status}");
    assert_eq!(status["interruption"]["cause"], "Cancelled");
    assert_eq!(status["interruption"]["requestedBy"], "alice");
    assert_eq!(status["interruption"]["runId"], RUN_ID);
    assert_eq!(status["interruption"]["verification"], "Running");
}

fn log(tail: &str) -> String {
    format!("{{\"level\":\"INFO\"}}\n{tail}")
}

fn exit_routes(value: &Value, log_tail: &str, exit_code: i32) -> Vec<Route> {
    vec![
        route(
            "GET",
            format!("/jobs/{NAME}"),
            200,
            job(NAME, JOB_UID, run_args(), false, finished("Failed")),
        ),
        route("GET", "/pods".into(), 200, pods(NAME, JOB_UID, Some(exit_code))),
        route("GET", "/log".into(), 200, log(log_tail)),
        status_route(value),
        route(
            "PATCH",
            format!("/jobs/{NAME}"),
            200,
            job(NAME, JOB_UID, run_args(), false, finished("Failed")),
        ),
    ]
}

/// **IR-1/IR-2's controller half: an exit that signed nothing after the run
/// created its targets is `Interrupted`, never a generic `Failed`**, and the
/// run Job keeps no TTL (the reconcile copies it). The runner's own
/// `failure-reason=Cancelled` makes the cause `RunnerStopped`; without it,
/// `RunFailed`. Control: the same exit without the targets line is `Failed`
/// as before. KILLS: reading every exit 1 as Interrupted, or none.
#[tokio::test]
async fn an_exit_without_a_scorecard_after_creating_targets_is_interrupted() {
    let value = restore_value(
        json!({}),
        json!({"phase": "Running", "jobRef": {"name": NAME}}),
    );
    for (tail, cause) in [
        (
            "targets-record-key=logweir/drills/R.targets.json\nfailure-reason=Cancelled\n",
            "RunnerStopped",
        ),
        (
            "targets-record-key=logweir/drills/R.targets.json\n",
            "RunFailed",
        ),
    ] {
        let (requeue, seen) = pass(&value, exit_routes(&value, tail, 1)).await;
        let status = status_patches(&seen).remove(0);
        assert_eq!(status["phase"], "Interrupted", "{status}");
        assert_eq!(status["interruption"]["cause"], cause, "{status}");
        assert_eq!(status["exitCode"], 1);
        assert!(
            bodies_of(&seen, "PATCH", &format!("/jobs/{NAME}")).is_empty(),
            "no TTL on the run Job"
        );
        assert_eq!(requeue, Requeue::After(1));
    }
    let (_, seen) = pass(&value, exit_routes(&value, "", 1)).await;
    let status = status_patches(&seen).remove(0);
    assert_eq!(status["phase"], "Failed", "the control: {status}");
}

/// **A runner killed by the kernel (OOMKilled, 137) or by a panic (101)
/// prints no targets line, and is still `Interrupted`**, cause
/// `RunnerStopped` (od23 review M-1): the runner never chooses those codes,
/// so it may have created topics, and the reconcile decides. The control is
/// the orderly exit 1 with no line above, which stays `Failed`.
#[tokio::test]
async fn a_runner_killed_without_a_line_is_interrupted() {
    let value = restore_value(
        json!({}),
        json!({"phase": "Running", "jobRef": {"name": NAME}}),
    );
    for code in [137, 101] {
        let (_, seen) = pass(&value, exit_routes(&value, "", code)).await;
        let status = status_patches(&seen).remove(0);
        assert_eq!(status["phase"], "Interrupted", "{code}: {status}");
        assert_eq!(status["interruption"]["cause"], "RunnerStopped");
        assert_eq!(status["interruption"]["runId"], RUN_ID);
    }
}

/// **A lost pod (no exit code) of a named run is `Interrupted`**, cause
/// `RunnerStopped`: the reconcile decides whether it created anything.
/// Control: a Job that names no run id stays `Failed`, `NoExitCode`.
#[tokio::test]
async fn a_lost_pod_of_a_named_run_is_interrupted() {
    let value = restore_value(
        json!({}),
        json!({"phase": "Running", "jobRef": {"name": NAME}}),
    );
    for (args, phase) in [
        (run_args(), "Interrupted"),
        (json!(["restore", "run"]), "Failed"),
    ] {
        let (_, seen) = pass(
            &value,
            vec![
                route(
                    "GET",
                    format!("/jobs/{NAME}"),
                    200,
                    job(NAME, JOB_UID, args.clone(), false, finished("Failed")),
                ),
                route("GET", "/pods".into(), 200, pods(NAME, JOB_UID, None)),
                status_route(&value),
            ],
        )
        .await;
        let status = status_patches(&seen).remove(0);
        assert_eq!(status["phase"], phase, "{status}");
        if phase == "Interrupted" {
            assert_eq!(status["interruption"]["cause"], "RunnerStopped");
        }
    }
}

fn interrupted(extra: Value) -> Value {
    let mut i = json!({"cause": "Cancelled", "runId": RUN_ID, "verification": "Running",
                       "attempts": 0});
    if let (Some(i), Some(e)) = (i.as_object_mut(), extra.as_object()) {
        for (k, v) in e {
            i.insert(k.clone(), v.clone());
        }
    }
    json!({"phase": "Interrupted", "reason": "Cancelled", "jobRef": {"name": NAME},
           "interruption": i})
}

/// **The reconcile Job**: a copy of the run Job running `restore reconcile`
/// with the run's flags and its run id, a fresh line token, owned by the
/// `Restore`, under a deterministic name; recorded as attempt 1. KILLS: a
/// reconcile that restores (`run`), loses the run id, or reuses the run's
/// token.
#[tokio::test]
async fn an_interrupted_restore_starts_its_reconcile_job() {
    let value = restore_value(json!({}), interrupted(json!({})));
    let mut args = run_args();
    args.as_array_mut()
        .unwrap()
        .extend([json!("--runner-line-token"), json!("aa".repeat(20))]);
    let (requeue, seen) = pass(
        &value,
        vec![
            route(
                "GET",
                format!("/jobs/{NAME}"),
                200,
                job(NAME, JOB_UID, args, true, json!({})),
            ),
            route("GET", format!("/jobs/{RECONCILE_JOB}"), 404, not_found()),
            route(
                "POST",
                "/jobs".into(),
                201,
                job(RECONCILE_JOB, "r", json!([]), false, json!({})),
            ),
            status_route(&value),
        ],
    )
    .await;
    let posted = bodies_of(&seen, "POST", "/jobs").remove(0);
    assert_eq!(posted["metadata"]["name"], RECONCILE_JOB);
    assert_eq!(posted["metadata"]["ownerReferences"][0]["uid"], UID);
    assert_eq!(posted["spec"]["suspend"], false);
    let args: Vec<String> =
        serde_json::from_value(posted["spec"]["template"]["spec"]["containers"][0]["args"].clone())
            .unwrap();
    assert_eq!(args[..2], ["restore".to_string(), "reconcile".to_string()]);
    assert!(
        args.windows(2).any(|w| w == ["--run-id", RUN_ID]),
        "{args:?}"
    );
    let token = args
        .iter()
        .position(|a| a == "--runner-line-token")
        .expect("a token");
    assert_ne!(args[token + 1], "aa".repeat(20), "its own token");
    let status = status_patches(&seen).remove(0);
    assert_eq!(status["interruption"]["attempts"], 1);
    assert_eq!(status["interruption"]["jobRef"]["name"], RECONCILE_JOB);
    assert_eq!(requeue, Requeue::After(ri::POLL_SECONDS));
}

/// **IR-8: a controller restart during the reconcile starts no second
/// reconcile Job**: a running reconcile is only looked at.
#[tokio::test]
async fn a_running_reconcile_is_only_watched() {
    let value = restore_value(
        json!({}),
        interrupted(json!({"attempts": 1, "jobRef": {"name": RECONCILE_JOB}})),
    );
    let (requeue, seen) = pass(
        &value,
        vec![route(
            "GET",
            format!("/jobs/{RECONCILE_JOB}"),
            200,
            job(RECONCILE_JOB, "r", json!([]), false, json!({"active": 1})),
        )],
    )
    .await;
    assert!(bodies_of(&seen, "POST", "/jobs").is_empty());
    assert!(status_patches(&seen).is_empty());
    assert_eq!(requeue, Requeue::After(ri::POLL_SECONDS));
}

fn ended_reconcile_routes(value: &Value, exit_code: i32, tail: &str) -> Vec<Route> {
    vec![
        route(
            "GET",
            format!("/jobs/{RECONCILE_JOB}"),
            200,
            job(
                RECONCILE_JOB,
                "r-uid",
                json!([]),
                false,
                finished("Complete"),
            ),
        ),
        route(
            "GET",
            "/pods".into(),
            200,
            pods(RECONCILE_JOB, "r-uid", Some(exit_code)),
        ),
        route("GET", "/log".into(), 200, log(tail)),
        status_route(value),
        route(
            "PATCH",
            format!("/jobs/{RECONCILE_JOB}"),
            200,
            job(
                RECONCILE_JOB,
                "r-uid",
                json!([]),
                false,
                finished("Complete"),
            ),
        ),
    ]
}

/// **How a reconcile's end is written.** A signed `fail-integrity` (exit 2)
/// stays `Interrupted`, its verification `Done` and its run id kept for the
/// cleanup; a signed `pass` (exit 0) is `Succeeded`, `ReconciledComplete`
/// (07.1-I6, IR-6); `reconcile-none=` is `Failed` with the reason. The
/// scorecard's keys are the restore's evidence. KILLS: a reconcile read as an
/// ordinary run (`Failed` at exit 2), or a pass not reconciled complete.
#[tokio::test]
async fn a_reconcile_end_is_written_as_the_reconciles() {
    let value = restore_value(
        json!({}),
        interrupted(json!({"attempts": 1, "jobRef": {"name": RECONCILE_JOB}})),
    );
    let keys = format!(
        "scorecard-key=logweir/drills/{RECONCILE_RUN}.json\n\
         sidecar-key=logweir/drills/{RECONCILE_RUN}.sig\n"
    );
    let (_, seen) = pass(&value, ended_reconcile_routes(&value, 2, &keys)).await;
    let status = status_patches(&seen).remove(0);
    assert_eq!(status["phase"], "Interrupted", "{status}");
    assert_eq!(status["interruption"]["verification"], "Done");
    assert_eq!(status["interruption"]["reconcileRunId"], RECONCILE_RUN);
    assert_eq!(
        status["evidence"]["scorecardKey"],
        format!("logweir/drills/{RECONCILE_RUN}.json")
    );

    let (_, seen) = pass(&value, ended_reconcile_routes(&value, 0, &keys)).await;
    let status = status_patches(&seen).remove(0);
    assert_eq!(status["phase"], "Succeeded", "{status}");
    assert_eq!(status["reason"], "ReconciledComplete", "{status}");

    let (_, seen) = pass(
        &value,
        ended_reconcile_routes(&value, 0, "reconcile-none=run R wrote no targets record\n"),
    )
    .await;
    let status = status_patches(&seen).remove(0);
    assert_eq!(status["phase"], "Failed", "{status}");
    assert_eq!(status["reason"], "Cancelled", "{status}");
    assert!(status["interruption"]["verificationReason"]
        .as_str()
        .is_some_and(|r| r.contains("no targets record")));
}

/// **IR-9: a reconcile that keeps failing** is tried at most three times;
/// the third failure is `verification: Failed`, the restore stays
/// `Interrupted`, so its point stays held and cleanup is not offered. KILLS:
/// retrying for ever, or releasing the point.
#[tokio::test]
async fn a_reconcile_failing_three_times_holds_the_point() {
    let value = restore_value(
        json!({}),
        interrupted(json!({"attempts": 3, "jobRef": {"name": RECONCILE_JOB}})),
    );
    let (requeue, seen) = pass(
        &value,
        ended_reconcile_routes(&value, 1, "failure-detail=kafka: authorization failed\n"),
    )
    .await;
    assert!(bodies_of(&seen, "POST", "/jobs").is_empty());
    let status = status_patches(&seen).remove(0);
    assert_eq!(status["interruption"]["verification"], "Failed", "{status}");
    assert!(status.get("phase").is_none(), "the phase stays Interrupted");
    assert_eq!(requeue, Requeue::AwaitChange);
    let mut stored = value.clone();
    weirkeeper::conditions::apply_merge_patch(&mut stored["status"], &status);
    let stored = restore(&stored);
    assert!(ri::holds_point(&stored));
    assert!(!weirkeeper::controllers::retention_policy::is_terminal_restore(&stored));
}

/// **Clean up**: after a signed reconcile and the annotation, a cleanup Job
/// copied from the run Job runs `restore cleanup --run-id <the reconcile's
/// run>`, wired with the read grant; its `Removed` copies what it deleted to
/// `status.teardown` and releases the point (IR-7). KILLS: a cleanup naming
/// the interrupted run (it has no signed record), one started before the
/// annotation, or a point held after the cleanup.
#[tokio::test]
async fn cleanup_runs_over_the_reconciles_record_and_releases_the_point() {
    let done = interrupted(json!({"verification": "Done", "reconcileRunId": RECONCILE_RUN}));
    // No annotation, nothing.
    let quiet = restore_value(json!({}), done.clone());
    let (_, seen) = pass(&quiet, vec![]).await;
    assert!(seen.is_empty(), "{seen:?}");

    let value = restore_value(json!({CLEANUP_ANNOTATION: "bob"}), done.clone());
    let (_, seen) = pass(
        &value,
        vec![
            route(
                "GET",
                format!("/jobs/{NAME}"),
                200,
                job(NAME, JOB_UID, run_args(), true, json!({})),
            ),
            route("GET", format!("/jobs/{CLEANUP_JOB}"), 404, not_found()),
            route(
                "POST",
                "/jobs".into(),
                201,
                job(CLEANUP_JOB, "c", json!([]), false, json!({})),
            ),
            status_route(&value),
        ],
    )
    .await;
    let posted = bodies_of(&seen, "POST", "/jobs").remove(0);
    let args: Vec<String> =
        serde_json::from_value(posted["spec"]["template"]["spec"]["containers"][0]["args"].clone())
            .unwrap();
    assert_eq!(args[..2], ["restore".to_string(), "cleanup".to_string()]);
    assert!(
        args.windows(2).any(|w| w == ["--run-id", RECONCILE_RUN]),
        "{args:?}"
    );
    assert!(
        args.iter().any(|a| a == "--evidence-read-grant"),
        "{args:?}"
    );
    let status = status_patches(&seen).remove(0);
    assert_eq!(status["interruption"]["cleanup"]["phase"], "Removing");
    assert_eq!(status["interruption"]["cleanup"]["requestedBy"], "bob");

    // The cleanup ended: Removed, the lists on status.teardown.
    let mut removing = done;
    removing["interruption"]["cleanup"] =
        json!({"phase": "Removing", "attempts": 1, "jobRef": {"name": CLEANUP_JOB}});
    let value = restore_value(json!({CLEANUP_ANNOTATION: "bob"}), removing);
    let line = format!(
        "clone-cleanup={}",
        json!({"run_id": RECONCILE_RUN, "deleted": ["r-orders"], "already_absent": [],
               "left": [], "attestation_key": format!("logweir/drills/{RECONCILE_RUN}.cleanup.json")})
    );
    let (_, seen) = pass(
        &value,
        vec![
            route(
                "GET",
                format!("/jobs/{CLEANUP_JOB}"),
                200,
                job(CLEANUP_JOB, "c-uid", json!([]), false, finished("Complete")),
            ),
            route(
                "GET",
                "/pods".into(),
                200,
                pods(CLEANUP_JOB, "c-uid", Some(0)),
            ),
            route("GET", "/log".into(), 200, log(&line)),
            status_route(&value),
        ],
    )
    .await;
    let status = status_patches(&seen).remove(0);
    assert_eq!(
        status["interruption"]["cleanup"]["phase"], "Removed",
        "{status}"
    );
    assert_eq!(status["teardown"]["deleted"], json!(["r-orders"]));
    let mut stored = value.clone();
    weirkeeper::conditions::apply_merge_patch(&mut stored["status"], &status);
    let stored = restore(&stored);
    assert!(!ri::holds_point(&stored));
    assert!(weirkeeper::controllers::retention_policy::is_terminal_restore(&stored));
}

/// An `Interrupted` restore frees its manual-restore slot: its writer is
/// gone.
#[test]
fn an_interrupted_restore_frees_its_pool_slot() {
    let value = restore_value(json!({}), interrupted(json!({})));
    assert_eq!(
        weirkeeper::run_pool::restore_standing(&restore(&value)),
        weirkeeper::run_pool::Standing::Finished
    );
}

/// A new run Job names its run and gives its pod the stop's grace.
#[test]
fn a_run_job_names_its_run() {
    let mut j: k8s_openapi::api::batch::v1::Job = serde_json::from_str(&job(
        NAME,
        JOB_UID,
        json!(["restore", "run"]),
        false,
        json!({}),
    ))
    .unwrap();
    ri::prepare_run_job(&mut j, Some(RUN_ID));
    assert_eq!(ri::run_id_of(&j).as_deref(), Some(RUN_ID));
    assert_eq!(
        j.spec
            .unwrap()
            .template
            .spec
            .unwrap()
            .termination_grace_period_seconds,
        Some(ri::RUN_GRACE_SECONDS)
    );
}
