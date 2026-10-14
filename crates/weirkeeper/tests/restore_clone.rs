//! **PROD-11.1a: the controller's part in a TTL clone** — the finalizer at
//! admission, a deletion before the run, the suspended cleanup Job and
//! `Active` after a passing run, and `Removed` from the cleanup's line — each
//! through `restore_clone::around` over the recording mock API, asserting
//! the requests and their bodies.

use serde_json::{json, Value};
use weirkeeper::controllers::restore::restore_clone::{around, FINALIZER};
use weirkeeper::controllers::restore::{
    Requeue, RestoreError, RestoreEvidenceKeys, RestoreOutcome,
};
use weirkeeper::crds::restore::Restore;
use weirkeeper::testing::{mock_client_recording_bodies, BodyRecorder, Route};

const NS: &str = "logweir-t20";
const NAME: &str = "clone-orders";
const UID: &str = "5c2e7b91-0000-4000-8000-0000000000a2";
const RUN_JOB_UID: &str = "bbbbbbbb-0000-4000-8000-0000000000b2";
const CLEANUP_JOB: &str = "logweir-clone-cleanup-5c2e7b91-0000-4000-8000-0000000000a2-1";
const CLEANUP_JOB_UID: &str = "cccccccc-0000-4000-8000-0000000000c3";

fn plan(teardown: &str, mode: &str) -> String {
    format!(
        "source:\n  storage: {{backend: s3, bucket: b, prefix: p, region: us-east-1}}\n  \
         backup: latestCompleted\n  topics: [orders]\n\
         target:\n  bootstrap_servers: [\"k:9092\"]\n  mode: {mode}\n  \
         topic_mapping_prefix: \"clone-\"\n  teardown: {teardown}\n\
         sample:\n  window_start: \"2026-10-13T00:00:00Z\"\n  \
         window_end: \"2026-10-13T01:00:00Z\"\n\
         objectives: {{}}\n\
         evidence: {{backend: s3, bucket: e, prefix: logweir/, region: us-east-1}}\n"
    )
}

fn restore_value(plan: &str, finalizers: &[&str], status: Value) -> Value {
    json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "Restore",
        "metadata": {"name": NAME, "namespace": NS, "uid": UID, "generation": 1,
                     "resourceVersion": "100", "finalizers": finalizers},
        "spec": {
            "planBytes": plan, "approvalRef": {"name": "a1"},
            "sourceArchive": {"url": "s3://b/p"}, "backupSetRef": "s",
            "pointInTime": "2026-10-13T01:00:00Z",
            "target": {"clusterRef": {"name": "scratch"}, "mode": "scratch",
                       "topicNaming": {"prefix": "clone-"}},
            "deadlineSeconds": 600
        },
        "status": status
    })
}

fn restore(value: &Value) -> Restore {
    serde_json::from_value(value.clone()).expect("a Restore")
}

fn leak(s: String) -> &'static str {
    s.leak()
}

fn now() -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::parse_from_rfc3339("2026-10-13T12:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc)
}

fn outcome() -> RestoreOutcome {
    RestoreOutcome {
        job_name: NAME.into(),
        created: false,
        admission: None,
        exit_code: None,
        terminal_state: None,
        keys: RestoreEvidenceKeys::default(),
        ttl_patched: false,
        requeue: Requeue::AwaitChange,
    }
}

fn bodies_of(bodies: &BodyRecorder, method: &str, suffix: &str) -> Vec<Value> {
    bodies
        .lock()
        .unwrap()
        .iter()
        .filter(|b| {
            b.method.eq_ignore_ascii_case(method)
                && b.uri.split('?').next().unwrap().ends_with(suffix)
        })
        .map(|b| serde_json::from_str(&b.body).unwrap_or(Value::Null))
        .collect()
}

fn run_job() -> String {
    json!({
        "apiVersion": "batch/v1", "kind": "Job",
        "metadata": {"name": NAME, "namespace": NS, "uid": RUN_JOB_UID,
            "ownerReferences": [{"apiVersion": "logweir.dev/v1alpha1", "kind": "Restore",
                "name": NAME, "uid": UID, "controller": true, "blockOwnerDeletion": true}]},
        "spec": {"backoffLimit": 0, "template": {"spec": {"restartPolicy": "Never",
            "containers": [{"name": "runner", "image": "runner:old",
                "args": ["restore", "run", "--spec", "/plan/restore.yaml",
                         "--signing-key", "/signing/key.pem"]}]}}},
        "status": {"conditions": [{"type": "Complete", "status": "True"}]}
    })
    .to_string()
}

/// **At admission a clone gets the finalizer, before its run Job exists**,
/// and the pass ends there (the patch moved the version the reconcile's
/// writes are preconditioned on). KILLS: a clone admitted without the
/// finalizer (deleting it during the run would orphan its topics); the
/// reconcile running on a version it did not read.
#[tokio::test]
async fn a_clone_gets_its_finalizer_before_its_run_starts() {
    let value = restore_value(&plan("{ttl_seconds: 300}", "scratch"), &[], Value::Null);
    let (client, _rec, bodies) = mock_client_recording_bodies(vec![Route {
        method: "PATCH",
        path_suffix: leak(format!("/restores/{NAME}")),
        status: 200,
        body: value.to_string(),
    }]);
    let mut ran = false;
    let out = around(&restore(&value), &client, now(), None, async {
        ran = true;
        Ok(outcome())
    })
    .await
    .unwrap();
    assert!(!ran, "the reconcile waits for the next pass");
    assert_eq!(out.requeue, Requeue::After(1));
    let patches = bodies_of(&bodies, "PATCH", &format!("/restores/{NAME}"));
    assert_eq!(patches.len(), 1);
    assert_eq!(patches[0]["metadata"]["finalizers"], json!([FINALIZER]));
    assert_eq!(patches[0]["metadata"]["resourceVersion"], "100");
}

/// A plan that is not a clone is the reconcile, untouched: no request.
#[tokio::test]
async fn a_restore_that_is_not_a_clone_is_the_reconcile_unchanged() {
    let value = restore_value(&plan("delete", "scratch"), &[], Value::Null);
    let (client, rec, _bodies) = mock_client_recording_bodies(vec![]);
    let mut ran = false;
    around(&restore(&value), &client, now(), None, async {
        ran = true;
        Ok(outcome())
    })
    .await
    .unwrap();
    assert!(ran);
    assert!(rec.lock().unwrap().is_empty());
}

/// The clone shape the runner refuses is refused before any Job:
/// `CloneTtlNeedsScratch` in `newTopic` mode.
#[tokio::test]
async fn a_clone_outside_scratch_mode_is_refused_before_any_job() {
    let value = restore_value(&plan("{ttl_seconds: 300}", "newTopic"), &[], Value::Null);
    let (client, rec, _bodies) = mock_client_recording_bodies(vec![]);
    let err = around(&restore(&value), &client, now(), None, async {
        Ok(outcome())
    })
    .await
    .unwrap_err();
    match err {
        RestoreError::Refused(_, message) => {
            assert!(message.starts_with("CloneTtlNeedsScratch: "), "{message}")
        }
        other => panic!("{other}"),
    }
    assert!(rec.lock().unwrap().is_empty());
}

/// **K8's unit half: deleting a clone whose run never started** releases the
/// finalizer at once and never hands the deleting object to the reconcile,
/// which would create its run Job.
#[tokio::test]
async fn deleting_a_clone_before_its_run_releases_it_without_starting_the_run() {
    let mut value = restore_value(
        &plan("{ttl_seconds: 300}", "scratch"),
        &[FINALIZER],
        Value::Null,
    );
    value["metadata"]["deletionTimestamp"] = json!("2026-10-13T11:59:00Z");
    let (client, _rec, bodies) = mock_client_recording_bodies(vec![
        Route {
            method: "GET",
            path_suffix: leak(format!("/jobs/{NAME}")),
            status: 404,
            body: json!({"kind": "Status", "apiVersion": "v1", "status": "Failure",
                         "reason": "NotFound", "code": 404, "message": "not found"})
            .to_string(),
        },
        Route {
            method: "PATCH",
            path_suffix: leak(format!("/restores/{NAME}")),
            status: 200,
            body: value.to_string(),
        },
    ]);
    let mut ran = false;
    around(&restore(&value), &client, now(), None, async {
        ran = true;
        Ok(outcome())
    })
    .await
    .unwrap();
    assert!(!ran);
    let patches = bodies_of(&bodies, "PATCH", &format!("/restores/{NAME}"));
    assert_eq!(patches[0]["metadata"]["finalizers"], json!([]));
}

/// **After a passing run the clone is kept**: its cleanup Job is created
/// SUSPENDED as a copy of the run Job with `restore cleanup`'s argv, and the
/// status records `Active` with `expiresAt` = now + 300 s. KILLS: a cleanup
/// started at once; a Job built from anything but the run's own pod.
#[tokio::test]
async fn a_passing_clone_is_kept_with_its_cleanup_job_suspended() {
    let status = json!({"phase": "Succeeded", "exitCode": 0,
        "evidence": {"scorecardKey": "logweir/drills/01J9X2QK7C4V0R8YB3ZP6MTS5A.json"}});
    let value = restore_value(&plan("{ttl_seconds: 300}", "scratch"), &[FINALIZER], status);
    let (client, _rec, bodies) = mock_client_recording_bodies(vec![
        Route {
            method: "GET",
            path_suffix: leak(format!("/restores/{NAME}")),
            status: 200,
            body: value.to_string(),
        },
        Route {
            method: "GET",
            path_suffix: leak(format!("/jobs/{CLEANUP_JOB}")),
            status: 404,
            body: json!({"kind": "Status", "apiVersion": "v1", "status": "Failure",
                         "reason": "NotFound", "code": 404, "message": "not found"})
            .to_string(),
        },
        Route {
            method: "GET",
            path_suffix: leak(format!("/jobs/{NAME}")),
            status: 200,
            body: run_job(),
        },
        Route {
            method: "POST",
            path_suffix: "/jobs",
            status: 201,
            body: run_job(),
        },
        Route {
            method: "PATCH",
            path_suffix: leak(format!("/restores/{NAME}/status")),
            status: 200,
            body: value.to_string(),
        },
    ]);
    let out = around(
        &restore(&value),
        &client,
        now(),
        Some("runner:new"),
        async { Ok(outcome()) },
    )
    .await
    .unwrap();
    assert_eq!(out.requeue, Requeue::After(300));
    let posted = bodies_of(&bodies, "POST", "/jobs");
    assert_eq!(posted.len(), 1);
    let job = &posted[0];
    assert_eq!(job["metadata"]["name"], CLEANUP_JOB);
    assert_eq!(job["spec"]["suspend"], true);
    let c = &job["spec"]["template"]["spec"]["containers"][0];
    assert_eq!(c["image"], "runner:new");
    // FX-43: the last two arguments are this Job's own line token, read back
    // by `runner_log::read_own_terminal`; the rest is the cleanup's argv.
    let mut args = c["args"].as_array().expect("an argv").clone();
    let token = args.pop().expect("the line token");
    assert_eq!(
        args.pop().expect("its flag"),
        logweir_core::runner_line::RUNNER_LINE_TOKEN_ARG
    );
    assert!(
        logweir_core::refusal_detail::LineToken::parse(token.as_str().unwrap()).is_some(),
        "a fresh line token"
    );
    assert_eq!(
        Value::Array(args),
        json!([
            "restore",
            "cleanup",
            "--spec",
            "/plan/restore.yaml",
            "--signing-key",
            "/signing/key.pem",
            "--run-id",
            "01J9X2QK7C4V0R8YB3ZP6MTS5A",
            "--attempt",
            "1",
            // D1: the read grant is named; an inline restore's is the plan's
            // own archive credential, which the copied pod already holds.
            "--evidence-read-grant",
            weirkeeper::controllers::switchover::INLINE_READ_GRANT
        ])
    );
    let status = bodies_of(&bodies, "PATCH", &format!("/restores/{NAME}/status"));
    let clone = &status[0]["status"]["clone"];
    assert_eq!(clone["phase"], "Active");
    assert_eq!(clone["expiresAt"], "2026-10-13T12:05:00Z");
    assert_eq!(clone["jobRef"]["name"], CLEANUP_JOB);
}

/// **K7's unit half: a cleanup that exited 0 is `Removed`**: its
/// `clone-cleanup=` line reaches `status.teardown` (deleted, left, the
/// attestation key) and the finalizer is released.
#[tokio::test]
async fn a_cleanup_that_succeeded_is_removed_and_releases_the_finalizer() {
    let status = json!({"phase": "Succeeded", "exitCode": 0,
        "evidence": {"scorecardKey": "logweir/drills/01J9X2QK7C4V0R8YB3ZP6MTS5A.json"},
        "clone": {"ttlSeconds": 300, "expiresAt": "2026-10-13T11:00:00Z", "phase": "Removing",
                  "attempts": 1, "jobRef": {"name": CLEANUP_JOB}}});
    let value = restore_value(&plan("{ttl_seconds: 300}", "scratch"), &[FINALIZER], status);
    let line =
        "clone-cleanup={\"runId\":\"01J9X2QK7C4V0R8YB3ZP6MTS5A\",\"deleted\":[\"clone-orders\"],\
                \"alreadyAbsent\":[],\"left\":[],\"attestationKey\":\
                \"logweir/drills/01J9X2QK7C4V0R8YB3ZP6MTS5A.cleanup.json\"}";
    let cleanup_job = json!({
        "apiVersion": "batch/v1", "kind": "Job",
        "metadata": {"name": CLEANUP_JOB, "namespace": NS, "uid": CLEANUP_JOB_UID},
        "status": {"conditions": [{"type": "Complete", "status": "True",
                                   "lastTransitionTime": "2026-10-13T11:59:00Z"}]}
    });
    let pods = json!({"apiVersion": "v1", "kind": "PodList", "metadata": {}, "items": [{
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": "cleanup-pod", "namespace": NS,
            "labels": {"batch.kubernetes.io/job-name": CLEANUP_JOB, "job-name": CLEANUP_JOB},
            "ownerReferences": [{"apiVersion": "batch/v1", "kind": "Job", "name": CLEANUP_JOB,
                "uid": CLEANUP_JOB_UID, "controller": true}]},
        "spec": {"containers": []},
        "status": {"phase": "Succeeded", "containerStatuses": [{"name": "runner", "ready": false,
            "restartCount": 0, "image": "x", "imageID": "x",
            "state": {"terminated": {"exitCode": 0}}}]}}]});
    let (client, _rec, bodies) = mock_client_recording_bodies(vec![
        Route {
            method: "GET",
            path_suffix: leak(format!("/restores/{NAME}")),
            status: 200,
            body: value.to_string(),
        },
        Route {
            method: "GET",
            path_suffix: leak(format!("/jobs/{CLEANUP_JOB}")),
            status: 200,
            body: cleanup_job.to_string(),
        },
        Route {
            method: "GET",
            path_suffix: "/pods",
            status: 200,
            body: pods.to_string(),
        },
        Route {
            method: "GET",
            path_suffix: "/log",
            status: 200,
            body: format!("starting\n{line}\n"),
        },
        Route {
            method: "PATCH",
            path_suffix: leak(format!("/restores/{NAME}/status")),
            status: 200,
            body: value.to_string(),
        },
        Route {
            method: "PATCH",
            path_suffix: leak(format!("/restores/{NAME}")),
            status: 200,
            body: value.to_string(),
        },
    ]);
    around(&restore(&value), &client, now(), None, async {
        Ok(outcome())
    })
    .await
    .unwrap();
    let status = bodies_of(&bodies, "PATCH", &format!("/restores/{NAME}/status"));
    assert_eq!(status[0]["status"]["clone"]["phase"], "Removed");
    assert_eq!(
        status[0]["status"]["teardown"],
        json!({"attestationKey": "logweir/drills/01J9X2QK7C4V0R8YB3ZP6MTS5A.cleanup.json",
               "deleted": ["clone-orders"], "deletedCount": 1, "failed": [], "failedCount": 0})
    );
    let finalizer = bodies_of(&bodies, "PATCH", &format!("/restores/{NAME}"));
    assert_eq!(finalizer[0]["metadata"]["finalizers"], json!([]));
}

// ---- The commit review's fail-open findings: the finalizer is released only
// on a settled clone phase or recorded proof that no run Job was created. ----

fn not_found() -> String {
    json!({"kind": "Status", "apiVersion": "v1", "status": "Failure",
           "reason": "NotFound", "code": 404, "message": "not found"})
    .to_string()
}

/// A cleanup Job owned by the Restore, for `attempt`: suspended, with the
/// cleanup's argv, naming the read grant when this build built it
/// (`with_grant`), on the image an older controller's would run.
fn suspended_cleanup_job(name: &str, attempt: u32, with_grant: bool) -> String {
    let mut args = vec![
        "restore",
        "cleanup",
        "--spec",
        "/plan/restore.yaml",
        "--signing-key",
        "/signing/key.pem",
        "--run-id",
        "01J9X2QK7C4V0R8YB3ZP6MTS5A",
        "--attempt",
    ]
    .into_iter()
    .map(str::to_string)
    .collect::<Vec<_>>();
    args.push(attempt.to_string());
    if with_grant {
        args.extend([
            "--evidence-read-grant".to_string(),
            "evidenceRead of BackupDestination logweir-t20/d".to_string(),
        ]);
    }
    json!({
        "apiVersion": "batch/v1", "kind": "Job",
        "metadata": {"name": name, "namespace": NS, "uid": CLEANUP_JOB_UID,
            "ownerReferences": [{"apiVersion": "logweir.dev/v1alpha1", "kind": "Restore",
                "name": NAME, "uid": UID, "controller": true, "blockOwnerDeletion": true}]},
        "spec": {"backoffLimit": 0, "suspend": true, "template": {"spec": {
            "restartPolicy": "Never",
            "containers": [{"name": "runner", "image": "runner:old", "args": args}]}}},
        "status": {}
    })
    .to_string()
}

fn deleting(mut value: Value) -> Value {
    value["metadata"]["deletionTimestamp"] = json!("2026-10-13T11:59:00Z");
    value
}

/// **Finding 1: a plan this controller cannot parse never releases the
/// finalizer.** The status says `RemovalFailed` with why and the manual step;
/// nothing is deleted and the deleting object is not reconciled. KILLS:
/// `parse_plan(..).ok()?` read as "not a clone". CONTROL: a plan that parses
/// and is not a clone releases a finalizer left on it.
#[tokio::test]
async fn an_unreadable_plan_keeps_the_finalizer_and_says_why() {
    let value = deleting(restore_value("not: [a, plan", &[FINALIZER], Value::Null));
    let (client, _rec, bodies) = mock_client_recording_bodies(vec![Route {
        method: "PATCH",
        path_suffix: leak(format!("/restores/{NAME}/status")),
        status: 200,
        body: value.to_string(),
    }]);
    let mut ran = false;
    around(&restore(&value), &client, now(), None, async {
        ran = true;
        Ok(outcome())
    })
    .await
    .unwrap();
    assert!(!ran);
    let status = bodies_of(&bodies, "PATCH", &format!("/restores/{NAME}/status"));
    assert_eq!(status[0]["status"]["clone"]["phase"], "RemovalFailed");
    let reason = status[0]["status"]["clone"]["reason"].as_str().unwrap();
    assert!(
        reason.contains("does not parse") && reason.contains("remove the finalizer"),
        "{reason}"
    );
    assert!(bodies_of(&bodies, "PATCH", &format!("/restores/{NAME}")).is_empty());

    // CONTROL.
    let value = deleting(restore_value(
        &plan("delete", "scratch"),
        &[FINALIZER],
        Value::Null,
    ));
    let (client, _rec, bodies) = mock_client_recording_bodies(vec![Route {
        method: "PATCH",
        path_suffix: leak(format!("/restores/{NAME}")),
        status: 200,
        body: value.to_string(),
    }]);
    around(&restore(&value), &client, now(), None, async {
        Ok(outcome())
    })
    .await
    .unwrap();
    let released = bodies_of(&bodies, "PATCH", &format!("/restores/{NAME}"));
    assert_eq!(released[0]["metadata"]["finalizers"], json!([]));
}

/// **Finding 2: a clone shape refused while deleting never releases the
/// finalizer**, even with a run Job in place (an upgrade that changed the
/// shape rules after the run started). KILLS: the release on a shape
/// refusal.
#[tokio::test]
async fn a_refused_shape_while_deleting_keeps_the_finalizer() {
    let value = deleting(restore_value(
        &plan("{ttl_seconds: 10}", "scratch"),
        &[FINALIZER],
        json!({"phase": "Running", "jobRef": {"name": NAME}}),
    ));
    let (client, _rec, bodies) = mock_client_recording_bodies(vec![Route {
        method: "PATCH",
        path_suffix: leak(format!("/restores/{NAME}/status")),
        status: 200,
        body: value.to_string(),
    }]);
    around(&restore(&value), &client, now(), None, async {
        Ok(outcome())
    })
    .await
    .unwrap();
    let status = bodies_of(&bodies, "PATCH", &format!("/restores/{NAME}/status"));
    assert_eq!(status[0]["status"]["clone"]["phase"], "RemovalFailed");
    assert!(status[0]["status"]["clone"]["reason"]
        .as_str()
        .unwrap()
        .starts_with("CloneTtlOutOfRange: "));
    assert!(bodies_of(&bodies, "PATCH", &format!("/restores/{NAME}")).is_empty());
}

/// **Finding 3: a missing run Job is not proof that nothing was created.**
/// Deleted before its outcome, with a run Job recorded (`jobRef`) and gone:
/// `RemovalFailed`, the finalizer kept, nothing reconciled. KILLS: the release
/// on `get_opt(..).is_none()`. CONTROL:
/// `deleting_a_clone_before_its_run_releases_it_without_starting_the_run`,
/// where no run Job was ever recorded.
#[tokio::test]
async fn a_recorded_run_job_that_is_gone_keeps_the_finalizer() {
    let value = deleting(restore_value(
        &plan("{ttl_seconds: 300}", "scratch"),
        &[FINALIZER],
        json!({"phase": "Running", "jobRef": {"name": NAME}}),
    ));
    let (client, _rec, bodies) = mock_client_recording_bodies(vec![
        Route {
            method: "GET",
            path_suffix: leak(format!("/jobs/{NAME}")),
            status: 404,
            body: not_found(),
        },
        Route {
            method: "PATCH",
            path_suffix: leak(format!("/restores/{NAME}/status")),
            status: 200,
            body: value.to_string(),
        },
    ]);
    let mut ran = false;
    around(&restore(&value), &client, now(), None, async {
        ran = true;
        Ok(outcome())
    })
    .await
    .unwrap();
    assert!(!ran);
    let status = bodies_of(&bodies, "PATCH", &format!("/restores/{NAME}/status"));
    assert_eq!(status[0]["status"]["clone"]["phase"], "RemovalFailed");
    assert!(status[0]["status"]["clone"]["reason"]
        .as_str()
        .unwrap()
        .contains("whether the run created topics is unknown"));
    assert!(bodies_of(&bodies, "PATCH", &format!("/restores/{NAME}")).is_empty());
}

/// **A run that ended with no exit code is not settled**: its runner's
/// teardown never ran, so the clone is `RemovalFailed` and the finalizer
/// kept. CONTROL: a run that exited (2) is `NotKept` and released.
#[tokio::test]
async fn a_run_without_an_exit_code_is_not_settled() {
    for (status, settled) in [
        (json!({"phase": "Failed", "jobRef": {"name": NAME}}), false),
        // Review MEDIUM-1: an OOM-killed runner (137) did not exit.
        (
            json!({"phase": "Failed", "exitCode": 137, "jobRef": {"name": NAME}}),
            false,
        ),
        (
            json!({"phase": "Failed", "exitCode": 2, "jobRef": {"name": NAME}}),
            true,
        ),
    ] {
        let value = restore_value(&plan("{ttl_seconds: 300}", "scratch"), &[FINALIZER], status);
        let (client, _rec, bodies) = mock_client_recording_bodies(vec![
            Route {
                method: "GET",
                path_suffix: leak(format!("/restores/{NAME}")),
                status: 200,
                body: value.to_string(),
            },
            Route {
                method: "PATCH",
                path_suffix: leak(format!("/restores/{NAME}/status")),
                status: 200,
                body: value.to_string(),
            },
            Route {
                method: "PATCH",
                path_suffix: leak(format!("/restores/{NAME}")),
                status: 200,
                body: value.to_string(),
            },
        ]);
        around(&restore(&value), &client, now(), None, async {
            Ok(outcome())
        })
        .await
        .unwrap();
        let status = bodies_of(&bodies, "PATCH", &format!("/restores/{NAME}/status"));
        let released = bodies_of(&bodies, "PATCH", &format!("/restores/{NAME}"));
        if settled {
            assert_eq!(status[0]["status"]["clone"]["phase"], "NotKept");
            assert_eq!(released[0]["metadata"]["finalizers"], json!([]));
        } else {
            assert_eq!(status[0]["status"]["clone"]["phase"], "RemovalFailed");
            assert!(released.is_empty(), "the finalizer was released");
        }
    }
}

/// **A `Removing` clone that records no attempt is not retried on a guess**
/// (the class the review named "+1 more": `watch`'s default attempt): it is
/// `RemovalFailed`, with no Job created and the finalizer kept.
#[tokio::test]
async fn a_removing_clone_without_its_attempt_is_not_retried() {
    let status = json!({"phase": "Succeeded", "exitCode": 0,
        "evidence": {"scorecardKey": "logweir/drills/01J9X2QK7C4V0R8YB3ZP6MTS5A.json"},
        "clone": {"ttlSeconds": 300, "expiresAt": "2026-10-13T11:00:00Z", "phase": "Removing",
                  "jobRef": {"name": CLEANUP_JOB}}});
    let value = restore_value(&plan("{ttl_seconds: 300}", "scratch"), &[FINALIZER], status);
    let (client, rec, bodies) = mock_client_recording_bodies(vec![
        Route {
            method: "GET",
            path_suffix: leak(format!("/restores/{NAME}")),
            status: 200,
            body: value.to_string(),
        },
        Route {
            method: "PATCH",
            path_suffix: leak(format!("/restores/{NAME}/status")),
            status: 200,
            body: value.to_string(),
        },
    ]);
    around(&restore(&value), &client, now(), None, async {
        Ok(outcome())
    })
    .await
    .unwrap();
    let status = bodies_of(&bodies, "PATCH", &format!("/restores/{NAME}/status"));
    assert_eq!(status[0]["status"]["clone"]["phase"], "RemovalFailed");
    assert!(rec.lock().unwrap().iter().all(|r| !r.uri.contains("/jobs")));
}

/// **Review LOW-2: a transient API error at expiry is retried, not
/// `RemovalFailed`.** Resuming the suspended cleanup Job answers 503: the pass
/// returns the API error (the next pass tries again) and records nothing.
/// CONTROL: a 404 (the suspended Job is gone) is `RemovalFailed`, finalizer
/// kept.
#[tokio::test]
async fn a_transient_error_at_expiry_is_retried_and_a_gone_job_is_recorded() {
    let status = json!({"phase": "Succeeded", "exitCode": 0,
        "evidence": {"scorecardKey": "logweir/drills/01J9X2QK7C4V0R8YB3ZP6MTS5A.json"},
        "clone": {"ttlSeconds": 300, "expiresAt": "2026-10-13T11:00:00Z", "phase": "Active",
                  "attempts": 0, "jobRef": {"name": CLEANUP_JOB}}});
    let value = restore_value(&plan("{ttl_seconds: 300}", "scratch"), &[FINALIZER], status);
    for (code, recorded) in [(503u16, false), (404u16, true)] {
        let (client, _rec, bodies) = mock_client_recording_bodies(vec![
            Route {
                method: "GET",
                path_suffix: leak(format!("/restores/{NAME}")),
                status: 200,
                body: value.to_string(),
            },
            // The suspended Job this build built (it names the read grant),
            // or none.
            Route {
                method: "GET",
                path_suffix: leak(format!("/jobs/{CLEANUP_JOB}")),
                status: if code == 404 { 404 } else { 200 },
                body: if code == 404 {
                    not_found()
                } else {
                    suspended_cleanup_job(CLEANUP_JOB, 1, true)
                },
            },
            Route {
                method: "PATCH",
                path_suffix: leak(format!("/jobs/{CLEANUP_JOB}")),
                status: code,
                body: json!({"kind": "Status", "apiVersion": "v1", "status": "Failure",
                             "reason": if code == 404 { "NotFound" } else { "ServiceUnavailable" },
                             "code": code, "message": "x"})
                .to_string(),
            },
            Route {
                method: "PATCH",
                path_suffix: leak(format!("/restores/{NAME}/status")),
                status: 200,
                body: value.to_string(),
            },
        ]);
        let result = around(&restore(&value), &client, now(), None, async {
            Ok(outcome())
        })
        .await;
        let status = bodies_of(&bodies, "PATCH", &format!("/restores/{NAME}/status"));
        if recorded {
            result.unwrap();
            assert_eq!(status[0]["status"]["clone"]["phase"], "RemovalFailed");
        } else {
            assert!(matches!(result, Err(RestoreError::Api(_))), "{code}");
            assert!(status.is_empty(), "a transient error recorded {status:?}");
        }
        assert!(bodies_of(&bodies, "PATCH", &format!("/restores/{NAME}")).is_empty());
    }
}

// ---- Theme A's D1 and D4: the upgrade path out of an older build's grants ----

fn cleanup_name(attempt: u32) -> String {
    format!("logweir-clone-cleanup-{UID}-{attempt}")
}

fn job_routes_for(value: &Value, jobs: Vec<(String, u16, String)>) -> Vec<Route> {
    let mut routes = vec![
        Route {
            method: "GET",
            path_suffix: leak(format!("/restores/{NAME}")),
            status: 200,
            body: value.to_string(),
        },
        Route {
            method: "POST",
            path_suffix: "/jobs",
            status: 201,
            body: run_job(),
        },
        Route {
            method: "PATCH",
            path_suffix: leak(format!("/jobs/{}", cleanup_name(1))),
            status: 200,
            body: run_job(),
        },
        Route {
            method: "PATCH",
            path_suffix: leak(format!("/restores/{NAME}/status")),
            status: 200,
            body: value.to_string(),
        },
    ];
    for (name, status, body) in jobs {
        routes.push(Route {
            method: "GET",
            path_suffix: leak(format!("/jobs/{name}")),
            status,
            body,
        });
    }
    routes
}

/// **D1's upgrade path: a suspended cleanup Job an older controller built is
/// never resumed.** Its pod would read the scorecard with the run's write
/// grant (the PoC's 403). At expiry the next attempt is built from it with
/// this build's grants and image, unsuspended; the clone is `Removing` on
/// attempt 2. CONTROL: a suspended Job this build built (it names the read
/// grant) is resumed, and nothing is created.
///
/// KILLS: `Start(1)` resuming whatever Job-1 is (the older grant runs);
/// `build_attempt` copying an older template without `rewire`.
#[tokio::test]
async fn an_older_suspended_cleanup_job_is_replaced_not_resumed() {
    let status = json!({"phase": "Succeeded", "exitCode": 0,
        "evidence": {"scorecardKey": "logweir/drills/01J9X2QK7C4V0R8YB3ZP6MTS5A.json"},
        "clone": {"ttlSeconds": 300, "expiresAt": "2026-10-13T11:00:00Z", "phase": "Active",
                  "attempts": 0, "jobRef": {"name": CLEANUP_JOB}}});
    let value = restore_value(&plan("{ttl_seconds: 300}", "scratch"), &[FINALIZER], status);
    for older in [true, false] {
        let (client, _rec, bodies) = mock_client_recording_bodies(job_routes_for(
            &value,
            vec![
                (
                    cleanup_name(1),
                    200,
                    suspended_cleanup_job(&cleanup_name(1), 1, !older),
                ),
                (cleanup_name(2), 404, not_found()),
            ],
        ));
        around(
            &restore(&value),
            &client,
            now(),
            Some("runner:new"),
            async { Ok(outcome()) },
        )
        .await
        .unwrap();
        let resumed = bodies_of(&bodies, "PATCH", &format!("/jobs/{}", cleanup_name(1)));
        let posted = bodies_of(&bodies, "POST", "/jobs");
        let clone = bodies_of(&bodies, "PATCH", &format!("/restores/{NAME}/status"))[0]["status"]
            ["clone"]
            .clone();
        if older {
            assert!(resumed.is_empty(), "the older Job is never resumed");
            assert_eq!(posted.len(), 1);
            let job = &posted[0];
            assert_eq!(job["metadata"]["name"], cleanup_name(2).as_str());
            assert_eq!(job["spec"]["suspend"], false);
            let c = &job["spec"]["template"]["spec"]["containers"][0];
            assert_eq!(c["image"], "runner:new", "this build's runner");
            let args: Vec<&str> = c["args"]
                .as_array()
                .unwrap()
                .iter()
                .map(|a| a.as_str().unwrap())
                .collect();
            let at = args
                .iter()
                .position(|a| *a == "--evidence-read-grant")
                .expect("the read grant is named");
            assert_eq!(
                args[at + 1],
                weirkeeper::controllers::switchover::INLINE_READ_GRANT
            );
            assert_eq!(
                args[args.iter().position(|a| *a == "--attempt").unwrap() + 1],
                "2"
            );
            assert_eq!(clone["phase"], "Removing");
            assert_eq!(clone["attempts"], 2);
            assert_eq!(clone["jobRef"]["name"], cleanup_name(2).as_str());
        } else {
            assert_eq!(resumed.len(), 1, "this build's Job is resumed");
            assert_eq!(resumed[0]["spec"]["suspend"], false);
            assert!(posted.is_empty());
            assert_eq!(clone["attempts"], 1);
        }
    }
}

/// **D4's way out: a `RemovalFailed` clone whose last cleanup Job an older
/// controller built is re-armed with this build's grants**, built
/// from attempt 1's Job; the clone is `Removing` on attempt 4 and the old
/// reason is cleared. CONTROL: when the last Job is this build's own (it
/// names the read grant), the clone holds: nothing is created and nothing
/// recorded, the finalizer stays for the operator.
///
/// KILLS: `Step::Hold` never re-arming (the PoC's clones stay held after the
/// upgrade); re-arming whatever failed (a loop over this build's own
/// failures).
#[tokio::test]
async fn a_removal_failed_clone_an_older_job_left_gets_one_more_attempt() {
    let status = json!({"phase": "Succeeded", "exitCode": 0,
        "evidence": {"scorecardKey": "logweir/drills/01J9X2QK7C4V0R8YB3ZP6MTS5A.json"},
        "clone": {"ttlSeconds": 300, "expiresAt": "2026-10-13T11:00:00Z",
                  "phase": "RemovalFailed", "attempts": 3,
                  "jobRef": {"name": cleanup_name(3)},
                  "reason": "the cleanup Job exited 1; 3 attempts failed"}});
    let value = restore_value(&plan("{ttl_seconds: 300}", "scratch"), &[FINALIZER], status);
    for older in [true, false] {
        let (client, rec, bodies) = mock_client_recording_bodies(job_routes_for(
            &value,
            vec![
                (
                    cleanup_name(3),
                    200,
                    suspended_cleanup_job(&cleanup_name(3), 3, !older),
                ),
                (
                    cleanup_name(1),
                    200,
                    suspended_cleanup_job(&cleanup_name(1), 1, !older),
                ),
                (cleanup_name(4), 404, not_found()),
            ],
        ));
        around(
            &restore(&value),
            &client,
            now(),
            Some("runner:new"),
            async { Ok(outcome()) },
        )
        .await
        .unwrap();
        let posted = bodies_of(&bodies, "POST", "/jobs");
        let status = bodies_of(&bodies, "PATCH", &format!("/restores/{NAME}/status"));
        if older {
            assert_eq!(posted.len(), 1);
            assert_eq!(posted[0]["metadata"]["name"], cleanup_name(4).as_str());
            let c = &posted[0]["spec"]["template"]["spec"]["containers"][0];
            assert_eq!(c["image"], "runner:new");
            assert!(c["args"]
                .as_array()
                .unwrap()
                .iter()
                .any(|a| a == "--evidence-read-grant"));
            let clone = &status[0]["status"]["clone"];
            assert_eq!(clone["phase"], "Removing");
            assert_eq!(clone["attempts"], 4);
            assert_eq!(clone["jobRef"]["name"], cleanup_name(4).as_str());
            assert!(clone["reason"].is_null(), "the old reason is cleared");
        } else {
            assert!(posted.is_empty(), "this build's own failure holds");
            assert!(status.is_empty());
            assert!(
                !rec.lock()
                    .unwrap()
                    .iter()
                    .any(|r| r.uri.contains(&cleanup_name(4))),
                "no next attempt is even looked for"
            );
        }
        assert!(bodies_of(&bodies, "PATCH", &format!("/restores/{NAME}")).is_empty());
    }
}

/// **HIGH-1 of the fix round's review: a clone `Removing` when the upgrade
/// lands.** Its attempt-1 Job, built by an older controller (no
/// `--evidence-read-grant`, image `runner:old`), failed exit 1. The new
/// controller, configured with `runner:new`, builds attempt 2 from it: the
/// template is rewired, so attempt 2 runs THIS controller's image, never the
/// copied one (an older runner refuses the flag the rewire adds, and the clone
/// would hold for good). CONTROL: a failed attempt whose template carries the
/// flag is copied with its own image.
///
/// KILLS: the rewire path taking the failed Job's image (the review's probe).
#[tokio::test]
async fn a_rewired_retry_runs_this_controllers_image_and_a_copy_keeps_its_own() {
    let status = json!({"phase": "Succeeded", "exitCode": 0,
        "evidence": {"scorecardKey": "logweir/drills/01J9X2QK7C4V0R8YB3ZP6MTS5A.json"},
        "clone": {"ttlSeconds": 300, "expiresAt": "2026-10-13T11:00:00Z", "phase": "Removing",
                  "attempts": 1, "jobRef": {"name": CLEANUP_JOB}}});
    let value = restore_value(&plan("{ttl_seconds: 300}", "scratch"), &[FINALIZER], status);
    let line = "clone-cleanup={\"runId\":\"01J9X2QK7C4V0R8YB3ZP6MTS5A\",\"deleted\":[],\
                \"alreadyAbsent\":[],\"left\":[]}";
    let pods = json!({"apiVersion": "v1", "kind": "PodList", "metadata": {}, "items": [{
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": "cleanup-pod", "namespace": NS,
            "labels": {"batch.kubernetes.io/job-name": CLEANUP_JOB, "job-name": CLEANUP_JOB},
            "ownerReferences": [{"apiVersion": "batch/v1", "kind": "Job", "name": CLEANUP_JOB,
                "uid": CLEANUP_JOB_UID, "controller": true}]},
        "spec": {"containers": []},
        "status": {"phase": "Failed", "containerStatuses": [{"name": "runner", "ready": false,
            "restartCount": 0, "image": "x", "imageID": "x",
            "state": {"terminated": {"exitCode": 1}}}]}}]});
    for older in [true, false] {
        let mut failed: Value =
            serde_json::from_str(&suspended_cleanup_job(CLEANUP_JOB, 1, !older)).unwrap();
        failed["spec"]["suspend"] = json!(false);
        failed["status"] = json!({"conditions": [{"type": "Failed", "status": "True",
                                     "lastTransitionTime": "2026-10-13T11:00:00Z"}]});
        let (client, _rec, bodies) = mock_client_recording_bodies(vec![
            Route {
                method: "GET",
                path_suffix: leak(format!("/restores/{NAME}")),
                status: 200,
                body: value.to_string(),
            },
            Route {
                method: "GET",
                path_suffix: leak(format!("/jobs/{}", cleanup_name(2))),
                status: 404,
                body: not_found(),
            },
            Route {
                method: "GET",
                path_suffix: leak(format!("/jobs/{CLEANUP_JOB}")),
                status: 200,
                body: failed.to_string(),
            },
            Route {
                method: "GET",
                path_suffix: "/pods",
                status: 200,
                body: pods.to_string(),
            },
            Route {
                method: "GET",
                path_suffix: "/log",
                status: 200,
                body: format!("starting\n{line}\n"),
            },
            Route {
                method: "POST",
                path_suffix: "/jobs",
                status: 201,
                body: run_job(),
            },
            Route {
                method: "PATCH",
                path_suffix: leak(format!("/restores/{NAME}/status")),
                status: 200,
                body: value.to_string(),
            },
        ]);
        around(
            &restore(&value),
            &client,
            now(),
            Some("runner:new"),
            async { Ok(outcome()) },
        )
        .await
        .unwrap();
        let posted = bodies_of(&bodies, "POST", "/jobs");
        assert_eq!(posted.len(), 1, "attempt 2 is built");
        assert_eq!(posted[0]["metadata"]["name"], cleanup_name(2).as_str());
        let c = &posted[0]["spec"]["template"]["spec"]["containers"][0];
        assert!(
            c["args"]
                .as_array()
                .unwrap()
                .iter()
                .any(|a| a == "--evidence-read-grant"),
            "attempt 2 names the read grant"
        );
        if older {
            assert_eq!(
                c["image"], "runner:new",
                "a rewired template runs this build"
            );
        } else {
            assert_eq!(
                c["image"], "runner:old",
                "a copy keeps the failed attempt's image"
            );
        }
    }
}
