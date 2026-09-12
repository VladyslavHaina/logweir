//! The `Backup` reconciler, the Job shape that keeps an exit code readable,
//! and the crashed-Job case.
//!
//! EVERY TEST HERE IS A PURE-FUNCTION TEST, A `mock_client` TEST OR A
//! FIXTURE-PARSING TEST. Nothing dials a socket, nothing waits on a Job,
//! nothing builds an image, nothing shells out, and nothing approaches Global
//! Constraint 22's 15 s per-test bound — the transport is a `tower` closure
//! and the clock is an argument.
//!
//! READ `ttl_is_patched_only_after_status` FIRST. It is the one property that
//! is an ORDERING rather than a value: pod garbage collection must never race
//! the exit-code read, because the code lives on the pod and the TTL
//! controller deletes the Job together with its pods. Everything else in this
//! file makes that ordering meaningful (there IS an exit code to lose; it is
//! read from the right container; it reaches the right field).

use chrono::{DateTime, TimeZone, Utc};
use futures::future::BoxFuture;
use logweir_store::Store;
use serde_json::Value;
use weirkeeper::conditions::apply_merge_patch;
use weirkeeper::conditions::{
    reason_for_exit, wire_reason_for_exit, CONDITION_REASONS, CONDITION_TYPES,
    REASON_DRILL_NOT_PASS, REASON_GUARD_REFUSED, REASON_OK, REASON_OPERATIONAL,
    REASON_SIGNING_OR_LOCK, TERMINAL_STATES, TERMINAL_STATE_ARCHIVE_URL_UNREADABLE,
};
use weirkeeper::controllers::backup::{
    covered_from_receipt, crash_terminal_state, crashed_status_patch, evidence_keys,
    observe_archive, orphan_state, plan_backup_id, plan_config_map_name, pod_selectors,
    reconcile_backup, refusal_state, runner_argv, runner_job_spec, terminated_exit_code,
    unobserved_archive, ArchiveObservation, EvidenceKeys, EvidencePresence, JOB_NAME_LABEL,
    JOB_NAME_LABEL_LEGACY, RUNNER_SERVICE_ACCOUNT, SIGNING_KEY_SECRET, TTL_SECONDS_AFTER_FINISHED,
};
use weirkeeper::controllers::backup_schedule::RUNNER_ARGV_ANNOTATION;
use weirkeeper::crds::backup::{Backup, BackupStatus};
use weirkeeper::job::{self, ENGINE_DIGEST, ENGINE_VERSION, RUNNER_IMAGE};
use weirkeeper::testing::{mock_client_recording, mock_client_recording_bodies, Route, SeenBody};
use weirkeeper::verification::unverified_evidence;

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

/// The task's namespace (STANDING RULE 13).
const NS: &str = "logweir-t17";

/// The `Backup`'s name. Deliberately the shape Task 18's
/// `scheduled_backup_name` produces — `logweir-backup-<schedule>-<slot>` —
/// because the Job is named after this object VERBATIM and the 63-character
/// `batch.kubernetes.io/job-name` cap is the whole reason that ruling exists.
const NAME: &str = "logweir-backup-nightly-20261109-031700";

/// The pod the job controller made.
const POD: &str = "logweir-backup-nightly-20261109-031700-abcde";

const UID: &str = "3f1c8a5e-0000-4000-8000-0000000000a1";

/// The source `KafkaCluster`'s object UID, and the cluster id its controller
/// read FROM THE BROKER. Two different things, deliberately: the allowlist
/// carries the broker-observed id and never the Kubernetes object's.
const CLUSTER_UID: &str = "7a2b9c1d-0000-4000-8000-0000000000c1";
const CLUSTER_ID: &str = "MkU3OEVBNTcwNTJENDM2Qk";

/// The `BackupSchedule`'s UID, for [`scheduled_backup`].
const SCHEDULE_UID: &str = "9c4d2e6f-0000-4000-8000-0000000000d1";

/// The two keys interface **I7** prints.
const RECEIPT_KEY: &str = "logweir/backups/b1/r1.receipt.json";
const SIDECAR_KEY: &str = "logweir/backups/b1/r1.receipt.sig";

/// A `Backup` as the API server would hand it over, with Task 18's runner
/// argv on its annotation.
fn backup_json() -> String {
    let argv = serde_json::to_string(&[
        "backup",
        "run",
        "--spec",
        "/plan/backup.yaml",
        "--allowed-clusters",
        "/plan/allowed-clusters.json",
        "--signing-key",
        "/signing/key.pem",
        "--out",
        "/work/backup.json",
        "--receipt-out",
        "/work/receipt.json",
        "--triggered-by",
        "schedule",
        "--backup-id-override",
        "b1",
    ])
    .expect("the argv serialises");
    // DOUBLE-ENCODED ON PURPOSE: an annotation VALUE is a string, and Task 18
    // writes `serde_json::to_string(&argv)` into it. A fixture carrying a raw
    // JSON array would be a shape the API server cannot store and would make
    // every assertion below true about something the cluster never holds.
    let argv = serde_json::to_string(&argv).expect("the annotation value is a JSON string");
    format!(
        r#"{{
  "apiVersion": "logweir.dev/v1alpha1",
  "kind": "Backup",
  "metadata": {{
    "name": "{NAME}",
    "namespace": "{NS}",
    "uid": "{UID}",
    "generation": 3,
    "annotations": {{ "{RUNNER_ARGV_ANNOTATION}": {argv} }}
  }},
  "spec": {{
    "sourceRef": {{ "name": "prod" }},
    "topics": ["orders", "payments"],
    "archive": {{ "url": "s3://kafka-backups/logweir", "secretRef": {{ "name": "logweir-s3" }} }},
    "slot": "20261109-031700",
    "triggeredBy": "schedule",
    "deadlineSeconds": 3600
  }}
}}"#
    )
}

fn backup() -> Backup {
    serde_json::from_str(&backup_json()).expect("the fixture is a Backup")
}

/// A `Backup` as **Task 18's reconciler** creates it: owned by its
/// `BackupSchedule` with `controller: true`, and carrying the argv override
/// Task 18 computes from that owner's UID and the slot.
///
/// A SECOND FIXTURE RATHER THAN AN EDIT TO THE FIRST. [`backup_json`] has no
/// owner reference, which is the shape of a `Backup` created by hand or by
/// Task 26's page, and both arms of `plan_backup_id` are worth asserting.
/// Built by MUTATING the parsed fixture rather than by re-templating its text,
/// so the two cannot drift.
fn scheduled_backup() -> Backup {
    let backup_id = weirkeeper::slot::backup_id_for(SCHEDULE_UID, "20261109-031700");
    let mut argv = runner_argv(&backup()).expect("the fixture carries a runner argv");
    let at = argv
        .iter()
        .position(|a| a == "--backup-id-override")
        .expect("the argv carries the override flag");
    argv[at + 1] = backup_id;

    let mut value: Value = serde_json::from_str(&backup_json()).expect("the fixture is JSON");
    value["metadata"]["ownerReferences"] = serde_json::json!([{
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "BackupSchedule",
        "name": "nightly",
        "uid": SCHEDULE_UID,
        "controller": true,
        "blockOwnerDeletion": true,
    }]);
    value["metadata"]["annotations"][RUNNER_ARGV_ANNOTATION] =
        serde_json::json!(serde_json::to_string(&argv).expect("the argv serialises"));
    serde_json::from_value(value).expect("the mutated fixture is a Backup")
}

/// A UTC instant, spelled as five integers so a test reads like a calendar.
fn utc(y: i32, m: u32, d: u32, h: u32, min: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(y, m, d, h, min, 0)
        .single()
        .expect("the fixture instant exists")
}

/// The `KafkaCluster` `spec.sourceRef` names, as the API server would hand it
/// over — `status.clusterId` INCLUDED, because it is what the rendered
/// allowlist carries.
fn kafka_cluster_json() -> String {
    format!(
        r#"{{
  "apiVersion": "logweir.dev/v1alpha1",
  "kind": "KafkaCluster",
  "metadata": {{ "name": "prod", "namespace": "{NS}", "uid": "{CLUSTER_UID}" }},
  "spec": {{
    "bootstrapServers": ["broker-0.prod:9093", "broker-1.prod:9093"],
    "auth": {{ "mode": "scramSha512", "username": "logweir", "secretRef": {{ "name": "prod-sasl" }}, "tls": true }},
    "role": "source"
  }},
  "status": {{ "reachable": true, "clusterId": "{CLUSTER_ID}" }}
}}"#
    )
}

/// The routes a CREATE pass needs: the absent Job, the source `KafkaCluster`,
/// the plan ConfigMap `POST`, the Job `POST`, and the status patch.
///
/// `configmap_status` is the ConfigMap `POST`'s answer, so one helper serves
/// the 201 case and the two 409 cases.
fn create_routes(configmap_status: u16, existing_configmap: String) -> Vec<Route> {
    vec![
        Route {
            method: "GET",
            path_suffix: "/jobs/logweir-backup-nightly-20261109-031700",
            status: 404,
            body: not_found_body("jobs.batch", NAME),
        },
        Route {
            method: "GET",
            path_suffix: "/kafkaclusters/prod",
            status: 200,
            body: kafka_cluster_json(),
        },
        Route {
            method: "POST",
            path_suffix: "/configmaps",
            status: configmap_status,
            body: if configmap_status == 409 {
                r#"{"kind":"Status","apiVersion":"v1","status":"Failure",
                    "message":"configmaps "x" already exists","reason":"AlreadyExists",
                    "code":409}"#
                    .to_string()
            } else {
                existing_configmap.clone()
            },
        },
        Route {
            method: "GET",
            path_suffix: "/configmaps/logweir-backup-nightly-20261109-031700-plan",
            status: if existing_configmap.contains("\"kind\":\"Status\"") {
                404
            } else {
                200
            },
            body: existing_configmap,
        },
        Route {
            method: "POST",
            path_suffix: "/jobs",
            status: 201,
            body: running_job_body(),
        },
        Route {
            method: "PATCH",
            path_suffix: "/backups/logweir-backup-nightly-20261109-031700/status",
            status: 200,
            body: backup_json(),
        },
    ]
}

/// A plan ConfigMap as the API server would hand it back, owned by `owner_uid`
/// with `controller: true`.
fn existing_plan_config_map(owner_uid: &str) -> String {
    format!(
        r#"{{
  "apiVersion": "v1", "kind": "ConfigMap",
  "metadata": {{
    "name": "{NAME}-plan", "namespace": "{NS}",
    "ownerReferences": [{{
      "apiVersion": "logweir.dev/v1alpha1", "kind": "Backup", "name": "{NAME}",
      "uid": "{owner_uid}", "controller": true, "blockOwnerDeletion": true
    }}]
  }},
  "data": {{ "backup.yaml": "already here", "allowed-clusters.json": "{{}}" }}
}}"#
    )
}

/// A 404 `Status`, the shape `Api::get_opt` reads as "absent".
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
  "metadata":{{"name":"{NAME}","namespace":"{NS}","uid":"bbbbbbbb-0000-4000-8000-0000000000b1"}},
  "spec":{{"template":{{"spec":{{"containers":[],"restartPolicy":"Never"}}}}}},
  "status":{{"conditions":[{{"type":"{condition}","status":"True",
     "lastProbeTime":"2026-11-09T03:20:00Z","lastTransitionTime":"2026-11-09T03:20:00Z"}}]}}}}"#
    )
}

/// A Job that exists and has not finished.
fn running_job_body() -> String {
    format!(
        r#"{{"apiVersion":"batch/v1","kind":"Job",
  "metadata":{{"name":"{NAME}","namespace":"{NS}","uid":"bbbbbbbb-0000-4000-8000-0000000000b1"}},
  "spec":{{"template":{{"spec":{{"containers":[],"restartPolicy":"Never"}}}}}},
  "status":{{"active":1}}}}"#
    )
}

/// A pod list holding one pod whose `runner` container terminated with
/// `exit_code`, preceded by an unrelated sidecar at index 0.
///
/// THE SIDECAR IS AT INDEX 0 ON PURPOSE, IN EVERY FIXTURE. A test suite whose
/// happy path has `runner` at index 0 cannot tell a by-name reader from a
/// by-index one, so the by-index mutant would survive every test but the one
/// written for it.
fn pod_list_terminated(exit_code: i32) -> String {
    format!(
        r#"{{"apiVersion":"v1","kind":"PodList","metadata":{{}},"items":[
  {{"apiVersion":"v1","kind":"Pod",
    "metadata":{{"name":"{POD}","namespace":"{NS}",
      "labels":{{"{JOB_NAME_LABEL}":"{NAME}","{JOB_NAME_LABEL_LEGACY}":"{NAME}"}}}},
    "spec":{{"containers":[]}},
    "status":{{"phase":"Failed","containerStatuses":[
      {{"name":"log-shipper","ready":false,"restartCount":0,"image":"x","imageID":"x",
        "state":{{"terminated":{{"exitCode":0,"finishedAt":"2026-11-09T03:19:00Z"}}}}}},
      {{"name":"runner","ready":false,"restartCount":0,"image":"x","imageID":"x",
        "state":{{"terminated":{{"exitCode":{exit_code},"finishedAt":"2026-11-09T03:19:00Z"}}}}}}
    ]}}}}]}}"#
    )
}

/// An empty pod list — the "the selector matched nothing" answer.
const EMPTY_POD_LIST: &str = r#"{"apiVersion":"v1","kind":"PodList","metadata":{},"items":[]}"#;

/// A pod list holding one pod with NO terminated state, plus whatever
/// `status_extra` says about why.
fn pod_list_untermined(status_extra: &str) -> String {
    format!(
        r#"{{"apiVersion":"v1","kind":"PodList","metadata":{{}},"items":[
  {{"apiVersion":"v1","kind":"Pod",
    "metadata":{{"name":"{POD}","namespace":"{NS}",
      "labels":{{"{JOB_NAME_LABEL}":"{NAME}"}}}},
    "spec":{{"containers":[]}},
    "status":{{{status_extra}}}}}]}}"#
    )
}

/// The pod log body: `n` ordinary lines, then whatever `tail` says.
fn log_body(tail: &str) -> String {
    format!(
        "{{\"level\":\"INFO\",\"fields\":{{\"run_id\":\"r1\"}}}}\n\
         {{\"level\":\"INFO\",\"message\":\"backup finished\"}}\n{tail}"
    )
}

/// The two interface-I7 lines, in the contract's order.
fn i7_tail() -> String {
    format!("receipt-key={RECEIPT_KEY}\nsidecar-key={SIDECAR_KEY}\n")
}

/// The route table for a reconcile that finds a finished Job.
fn finished_routes(pods: &str, log: String, status_code: u16, job_condition: &str) -> Vec<Route> {
    vec![
        Route {
            method: "GET",
            path_suffix: "/jobs/logweir-backup-nightly-20261109-031700",
            status: 200,
            body: job_body(job_condition),
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
            body: log,
        },
        Route {
            method: "PATCH",
            path_suffix: "/backups/logweir-backup-nightly-20261109-031700/status",
            status: status_code,
            body: if status_code == 200 {
                backup_json()
            } else {
                r#"{"kind":"Status","apiVersion":"v1","status":"Failure",
                    "message":"the status subresource is unavailable","code":500}"#
                    .to_string()
            },
        },
        Route {
            method: "PATCH",
            path_suffix: "/jobs/logweir-backup-nightly-20261109-031700",
            status: 200,
            body: job_body(job_condition),
        },
        // DELETE IS ROUTED SO THAT "IT DID NOT DELETE" IS AN ASSERTION AND NOT
        // AN INABILITY. The double panics on an unrouted request, so with no
        // DELETE route a reconciler that deleted something would fail inside
        // `testing.rs` naming a missing route — a real failure, but not the
        // one the zero-DELETE assertions are about, and not one that says
        // "Global Constraint 6 gives no component a delete capability".
        Route {
            method: "DELETE",
            path_suffix: "/jobs/logweir-backup-nightly-20261109-031700",
            status: 200,
            body: job_body(job_condition),
        },
        Route {
            method: "DELETE",
            path_suffix: "/backups/logweir-backup-nightly-20261109-031700",
            status: 200,
            body: backup_json(),
        },
    ]
}

/// A recorded URI's PATH, without its query string.
///
/// `kube` appends a `?` to every request target it builds, empty query
/// included (`…/backups/<n>/status?`), so an `ends_with("/status")` over the
/// raw URI is false for every request the client actually makes. Stripping the
/// query once, here, is what keeps every assertion below about the route
/// rather than about the client's URL-building.
fn path(uri: &str) -> &str {
    uri.split('?').next().unwrap_or(uri)
}

/// Every `PATCH …/status` body the double saw, as JSON.
fn patched_statuses(bodies: &[SeenBody]) -> Vec<Value> {
    bodies
        .iter()
        .filter(|b| b.method == "PATCH" && path(&b.uri).ends_with("/status"))
        .map(|b| serde_json::from_str::<Value>(&b.body).expect("a status patch is JSON"))
        .map(|v| v["status"].clone())
        .collect()
}

/// One status's conditions as `(type, status, reason)`, in array order.
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

/// A condition array is a MAP KEYED BY `type`, so two entries sharing one
/// `type` is a malformed status whatever their statuses say.
///
/// Asserted as a HELPER because it is a property of every status this
/// controller writes, not of one arm — review finding HIGH-2.
fn assert_no_duplicate_condition_types(status: &Value, what: &str) {
    let conditions = conditions_of(status);
    let mut types: Vec<&str> = conditions.iter().map(|(t, _, _)| t.as_str()).collect();
    types.sort_unstable();
    let unique = {
        let mut t = types.clone();
        t.dedup();
        t
    };
    assert_eq!(
        types, unique,
        "{what}: two conditions share a `type`. A condition array is a map keyed by `type` (the \
         CRD's item description says `a metav1.Condition`), so a standard FindStatusCondition \
         reader sees whichever comes first, `kubectl wait --for=condition=Failed` matches only by \
         array order, and the day the array is given `x-kubernetes-list-type: map` the API \
         server REJECTS the patch. Got {conditions:?}"
    );
}

/// A file's text, read relative to the WORKSPACE ROOT.
///
/// IT PANICS RATHER THAN SKIPPING: a fixture-parsing assertion that cannot
/// find its fixture has not passed, it has not run.
fn workspace_file(relative: &str) -> String {
    let path = workspace_root().join(relative);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

/// The workspace root: two levels above this crate's manifest directory.
fn workspace_root() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("the workspace root is two levels above this crate")
        .to_path_buf()
}

/// A YAML document from the workspace, parsed.
fn yaml(relative: &str) -> serde_yaml::Value {
    serde_yaml::from_str(&workspace_file(relative))
        .unwrap_or_else(|e| panic!("{relative} is not YAML: {e}"))
}

/// The `Backup` CRD as the EMITTER renders it, in process.
///
/// MUTATION ROUND FINDING, AND THE REASON BOTH CRD ASSERTIONS BELOW READ TWO
/// SOURCES. A test that reads only `config/crd/backups.yaml` cannot see a
/// change to `crds/backup.rs` until somebody runs `just crds` — so the brief's
/// "add a `status.outcome`" and "rename a printer column" mutants both PASSED
/// the tests they are filed against, and died only in Task 15b's drift gate.
/// The drift gate catching them is correct and is not a substitute: it says
/// "the checked-in file no longer matches the emitter", which is a different
/// finding from "`Backup` has grown a second source of truth for the green
/// badge". Reading both the emitter and the file makes each assertion true of
/// the SOURCE and of the artefact the API server consumes, and keeps the drift
/// gate's own job (they disagree) distinct.
fn emitted_backup_crd() -> serde_yaml::Value {
    let rendered = weirkeeper::crds::render_all();
    let backup = rendered
        .iter()
        .find(|r| r.kind == "Backup")
        .expect("the emitter renders a Backup CRD");
    serde_yaml::from_str(&backup.yaml).expect("the emitted CRD is YAML")
}

// ---------------------------------------------------------------------------
// The Job shape
// ---------------------------------------------------------------------------

/// One `POST …/jobs`, with the pinned failure policy IN ORDER.
///
/// KILLS: `restartPolicy: OnFailure`; a reordering of the two
/// `podFailurePolicy` rules; a third rule.
#[tokio::test]
async fn backup_reconcile_creates_exactly_one_job_with_the_pinned_failure_policy() {
    let routes = create_routes(201, existing_plan_config_map(UID));
    let (client, seen, bodies) = mock_client_recording_bodies(routes);
    let outcome = reconcile_backup(
        &backup(),
        &client,
        &unobserved_archive,
        &unverified_evidence,
        utc(2026, 11, 9, 3, 17),
    )
    .await
    .expect("the reconcile succeeds");
    assert!(outcome.created, "this reconcile created the Job");

    let seen = seen.lock().expect("the recorder is readable");
    let job_posts: Vec<_> = seen
        .iter()
        .filter(|r| r.method == "POST" && path(&r.uri).ends_with("/jobs"))
        .collect();
    assert_eq!(
        job_posts.len(),
        1,
        "EXACTLY ONE Job is created per reconcile; got {job_posts:?}"
    );
    // AND EXACTLY ONE PLAN CONFIGMAP, in the same pass. Filtered by path
    // rather than counting every `POST`, because the create pass now writes
    // two objects and a bare `POST` count would say nothing about which
    // (errata E5a).
    let cm_posts: Vec<_> = seen
        .iter()
        .filter(|r| r.method == "POST" && path(&r.uri).ends_with("/configmaps"))
        .collect();
    assert_eq!(
        cm_posts.len(),
        1,
        "and exactly one plan ConfigMap; got {cm_posts:?}"
    );

    let bodies = bodies.lock().expect("the body recorder is readable");
    let post = bodies
        .iter()
        .find(|b| b.method == "POST" && path(&b.uri).ends_with("/jobs"))
        .expect("the Job POST body was recorded");
    let job: Value = serde_json::from_str(&post.body).expect("the POSTed Job is JSON");

    // AN AFFORDANCE, NOT AN ASSERTION, and it is here rather than in a second
    // test because the body this test already holds IS the Job the reconciler
    // creates. `LOGWEIR_DUMP_RUNNER_JOB=<path> cargo test -p weirkeeper --test
    // backup_controller backup_reconcile_creates` writes it out as YAML, which
    // is how the live cluster acceptance for this task applied the REAL
    // generated Job instead of a hand-transcribed lookalike — and how Task 21
    // and Task 24 can see the shape they are granting RBAC for and demoing.
    // Unset in every ordinary run, so nothing is printed and nothing is
    // written.
    if let Ok(path) = std::env::var("LOGWEIR_DUMP_RUNNER_JOB") {
        let yaml = serde_yaml::to_string(&job).expect("the Job serialises as YAML");
        std::fs::write(&path, yaml).unwrap_or_else(|e| panic!("{path}: {e}"));
    }

    assert_eq!(
        job["metadata"]["name"].as_str(),
        Some(NAME),
        "the Job is named after the `Backup` VERBATIM — never `logweir-backup-<cr name>`, which \
         would pass the 63-character `batch.kubernetes.io/job-name` cap for any schedule name of \
         18 characters or more (Task 18's review ruling)"
    );
    assert_eq!(
        job["spec"]["backoffLimit"].as_i64(),
        Some(0),
        "`backoffLimit: 0` is half of what yields exactly ONE pod"
    );
    assert_eq!(
        job["spec"]["template"]["spec"]["restartPolicy"].as_str(),
        Some("Never"),
        "`restartPolicy: Never` is the other half. `OnFailure` restarts the container in place \
         and then DELETES the pod, and the exit code lives ONLY on the pod object — so the code \
         is not buried in `lastState`, it is GONE (measured live, docs/kubernetes.md §1)"
    );
    assert_eq!(
        job["spec"]["template"]["spec"]["automountServiceAccountToken"].as_bool(),
        Some(false),
        "a runner pod makes ZERO Kubernetes API calls, and the key-holder is deliberately the \
         component with no cluster credential"
    );
    assert_eq!(
        job["spec"]["template"]["spec"]["serviceAccountName"].as_str(),
        Some(RUNNER_SERVICE_ACCOUNT),
        "the ServiceAccount is NAMED even though its token is not mounted: with none named a pod \
         silently gets `default`, which is the account most likely to have been granted something"
    );
    assert!(
        job["spec"]["ttlSecondsAfterFinished"].is_null(),
        "NO `ttlSecondsAfterFinished` AT CREATION TIME. The TTL controller deletes the Job AND \
         its pods, and the exit code lives on the pod — see `ttl_is_patched_only_after_status`. \
         Got: {:?}",
        job["spec"]["ttlSecondsAfterFinished"]
    );
    assert_eq!(
        job["spec"]["activeDeadlineSeconds"].as_i64(),
        Some(3600),
        "`activeDeadlineSeconds` comes from `Backup.spec.deadlineSeconds`"
    );

    let containers = job["spec"]["template"]["spec"]["containers"]
        .as_array()
        .expect("the pod template has containers");
    assert_eq!(
        containers.len(),
        1,
        "EXACTLY ONE container. A second one would shift `containerStatuses` indices and is \
         precisely what makes by-index reads wrong; got {containers:?}"
    );
    assert_eq!(
        containers[0]["name"].as_str(),
        Some("runner"),
        "the container is always named `runner`, and the exit-code read finds it by that name"
    );
    assert_eq!(
        containers[0]["image"].as_str(),
        Some(RUNNER_IMAGE),
        "the image is `job::RUNNER_IMAGE` (interface I15) and this task writes no image literal"
    );
    assert_eq!(
        containers[0]["imagePullPolicy"].as_str(),
        Some("Never"),
        "Global Constraint 17 (zero cloud spend) and GC37 (`blocked: images not published`): the image is \
         local, and `Never` makes a missing one fail as ErrImageNeverPull rather than as an \
         opaque pull error"
    );

    let rules = job["spec"]["podFailurePolicy"]["rules"]
        .as_array()
        .expect("podFailurePolicy has rules");
    assert_eq!(
        rules.len(),
        2,
        "TWO RULES AND NO THIRD. `podFailurePolicy` is INERT while `backoffLimit` is 0 — both \
         rules are FailJob and one pod failure already fails the Job — so this pins a \
         DECLARATION, not a behaviour: exit 1 is never retried and a disruption is a failure, so \
         a future `backoffLimit > 0` cannot quietly change either. Got {rules:?}"
    );
    assert_eq!(
        rules[0]["action"].as_str(),
        Some("FailJob"),
        "rule 0 is FailJob"
    );
    assert_eq!(
        rules[0]["onPodConditions"][0]["type"].as_str(),
        Some("DisruptionTarget"),
        "RULE 0, IN THIS POSITION, is the disruption pattern. Rules are evaluated in order and \
         the first match wins, so a pod that was disrupted AND reported a code must classify as \
         disrupted"
    );
    assert_eq!(
        rules[0]["onPodConditions"][0]["status"].as_str(),
        Some("True"),
        "the disruption pattern matches `status: True`"
    );
    assert!(
        rules[0]["onExitCodes"].is_null(),
        "one of onExitCodes and onPodConditions per rule, never both"
    );
    assert_eq!(
        rules[1]["action"].as_str(),
        Some("FailJob"),
        "rule 1 is FailJob"
    );
    assert_eq!(
        rules[1]["onExitCodes"]["operator"].as_str(),
        Some("In"),
        "RULE 1, IN THIS POSITION, is the exit-code set"
    );
    assert_eq!(
        rules[1]["onExitCodes"]["containerName"].as_str(),
        Some("runner"),
        "`containerName: runner` — the rule is about the runner's code and not a sidecar's"
    );
    assert_eq!(
        rules[1]["onExitCodes"]["values"]
            .as_array()
            .map(|v| v.iter().filter_map(Value::as_i64).collect::<Vec<_>>()),
        Some(vec![2, 3, 4]),
        "`In [2, 3, 4]`: a result that is not a pass, a guard refusal and a signing failure are \
         all real answers and none of them is retried"
    );
    assert!(
        rules.get(2).is_none(),
        "THERE IS NO RULE 2. An `Ignore` on `[1]` would need `backoffLimit > 0`, which yields \
         SEVERAL pods while `status.exitCode` is a single value (spec §3.2 M15)"
    );
}

/// No rule anywhere carries `action: "Ignore"`.
///
/// KILLS: an `Ignore` rule on `[1]`.
#[test]
fn the_failure_policy_has_no_ignore_on_one() {
    let policy = job::failure_policy();
    for (i, rule) in policy.rules.iter().enumerate() {
        assert_ne!(
            rule.action, "Ignore",
            "rule {i} carries `action: Ignore`. A retry needs `backoffLimit > 0`, which yields \
             several pods while `Backup.status.exitCode` is ONE value — two codes and one field \
             is a status that is either wrong or arbitrary. Spec §3.2 M15 drops the rule."
        );
    }
    assert!(
        policy.rules.iter().all(|r| r.action == "FailJob"),
        "every rule this task ships is FailJob; got {:?}",
        policy.rules.iter().map(|r| &r.action).collect::<Vec<_>>()
    );
}

// ---------------------------------------------------------------------------
// Task 33 — the runner image the operator was handed
// ---------------------------------------------------------------------------

/// `job::configured_runner_image`: the four answers, one of which is a defect.
///
/// **`Ok("")` IS UNSET** — plan erratum E19(e), the same ruling
/// `retention::configured_archive_url` carries. A Kubernetes `env:` entry with
/// an empty `value:` makes `env::var` return `Ok("")`, not `Err(NotPresent)`,
/// and a controller that read that as a configured value would create every
/// Job with `image: ""`. Whitespace trims to the same answer.
///
/// KILLS: a predicate that tests `is_ok()` instead of the trimmed value.
#[test]
fn an_empty_runner_image_override_is_unset() {
    assert_eq!(
        job::configured_runner_image(Ok(String::new())),
        None,
        "an empty `value:` on the Deployment's env entry is the variable being UNSET (E19(e)); \
         read as configured it puts `image: \"\"` in every Job this controller creates"
    );
    assert_eq!(
        job::configured_runner_image(Ok("   ".to_string())),
        None,
        "whitespace is the same fact as empty"
    );
    assert_eq!(
        job::configured_runner_image(Err(std::env::VarError::NotPresent)),
        None,
        "an absent variable is unset"
    );
    assert_eq!(
        job::configured_runner_image(Ok("logweir:check".to_string())),
        Some("logweir:check".to_string()),
        "a reference is a reference; nothing about it is validated here, because the kubelet is \
         the only thing that can say whether a reference resolves on a node"
    );
    assert_eq!(
        job::configured_runner_image(Ok("  logweir:check\n".to_string())),
        Some("logweir:check".to_string()),
        "trimmed, so a YAML block scalar's trailing newline is not part of the reference"
    );
}

/// With no override the Job carries the shipped pin — the DEFAULT is unchanged.
///
/// This is the same assertion
/// `backup_reconcile_creates_exactly_one_job_with_the_pinned_failure_policy`
/// makes over the wire, restated over `job::build` so the default and the
/// override are one pair of rows.
///
/// KILLS: a `build` that reaches for the override when there is none.
#[test]
fn the_runner_image_defaults_to_the_shipped_pin() {
    let spec = runner_job_spec(&backup()).expect("the fixture yields a spec");
    assert_eq!(
        spec.image, None,
        "`runner_job_spec` is a pure function of the custom resource and builds NO image: the \
         override is a property of the process, read once in `main`"
    );
    let built = job::build(&spec);
    let container = built
        .spec
        .as_ref()
        .and_then(|s| s.template.spec.as_ref())
        .and_then(|p| p.containers.first())
        .expect("the pod template has the one runner container");
    assert_eq!(
        container.image.as_deref(),
        Some(RUNNER_IMAGE),
        "interface I15: with no override the image is `job::RUNNER_IMAGE`, exactly as before \
         Task 33"
    );
}

/// With an override the Job carries it — and the pull policy does NOT move.
///
/// KILLS (M1): a `build` that ignores `RunnerJobSpec::image`.
/// KILLS (M5): a pull policy that follows the image, or that becomes
/// overridable — the override exists for images LOADED onto the node, which is
/// the one case `Never` is exactly right for, and `Never` is what makes a
/// wrong reference fail as `ErrImageNeverPull` naming the whole reference
/// rather than as a pull against a namespace that resolves to nothing.
#[test]
fn the_runner_image_override_does_not_touch_the_pull_policy() {
    let mut spec = runner_job_spec(&backup()).expect("the fixture yields a spec");
    spec.image = Some("logweir:check".to_string());
    let built = job::build(&spec);
    let container = built
        .spec
        .as_ref()
        .and_then(|s| s.template.spec.as_ref())
        .and_then(|p| p.containers.first())
        .expect("the pod template has the one runner container");
    assert_eq!(
        container.image.as_deref(),
        Some("logweir:check"),
        "the image the operator handed this controller is the image the Job names — the whole \
         point of `job::RUNNER_IMAGE_ENV`, and what `kubectl get job -o yaml` shows"
    );
    assert_ne!(
        container.image.as_deref(),
        Some(RUNNER_IMAGE),
        "and it is NOT the compile-time pin, which is the reference a cluster that did not build \
         it cannot start"
    );
    assert_eq!(
        container.image_pull_policy.as_deref(),
        Some(job::IMAGE_PULL_POLICY),
        "the POLICY is not overridable and stays `Never`"
    );
    assert_eq!(
        job::IMAGE_PULL_POLICY,
        "Never",
        "and `Never` is what it is: the override is for an image LOADED onto the node, so a \
         wrong reference must fail as ErrImageNeverPull naming the whole reference"
    );
    assert_eq!(
        container.name,
        job::CONTAINER_NAME,
        "nothing else about the container moved"
    );
    assert_eq!(
        container.args.as_ref(),
        Some(&spec.args),
        "and the argv is still passed through unchanged"
    );
}

/// The Job's name is the CR's name, with no prefix added.
#[test]
fn the_job_name_is_the_cr_name() {
    let spec = runner_job_spec(&backup()).expect("the fixture yields a spec");
    assert_eq!(
        spec.name, NAME,
        "`metadata.name = backup.name_any()`, VERBATIM. A scheduled `Backup` is already \
         `logweir-backup-<schedule>-<slot>`, so `logweir-backup-<cr name>` would be \
         `logweir-backup-logweir-backup-…` and would pass 63 characters — the cap on the \
         `batch.kubernetes.io/job-name` label, which is how the pod carrying the exit code is \
         found"
    );
    assert!(
        spec.name.len() <= 63,
        "the fixture's own name has to fit the label cap for this test to mean anything; got {}",
        spec.name.len()
    );
    assert_eq!(
        job::build(&spec).metadata.name.as_deref(),
        Some(NAME),
        "`job::build` carries the name through unchanged"
    );
}

/// The argv is the annotation's, verbatim — `--backup-id-override` included.
#[test]
fn the_argv_is_the_annotation_verbatim() {
    let b = backup();
    let from_annotation = runner_argv(&b).expect("the fixture carries a parseable argv");
    let spec = runner_job_spec(&b).expect("the fixture yields a spec");
    assert_eq!(
        spec.args, from_annotation,
        "the argv is READ from `logweir.dev/runner-argv` and PASSED THROUGH. Rebuilding one here \
         silently drops `--backup-id-override` (interface I10), which is the flag that makes a \
         re-run reuse its slot's backup id rather than mint a second one — so a dropped flag is \
         a second, PARTIAL archive rather than a visible error (Task 18's review ruling)"
    );
    assert!(
        spec.args.iter().any(|a| a == "--backup-id-override"),
        "the fixture's argv carries the flag whose loss this test is about; got {:?}",
        spec.args
    );
    assert_eq!(
        spec.args.first().map(String::as_str),
        Some("backup"),
        "the leading token names the `logweir` subcommand, not the engine's; Global Constraint 3 \
         as revised by Task 1 admits it either way"
    );

    let job = job::build(&spec);
    let args = job.spec.as_ref().and_then(|s| {
        s.template
            .spec
            .as_ref()
            .and_then(|p| p.containers.first())
            .and_then(|c| c.args.clone())
    });
    assert_eq!(
        args.as_ref(),
        Some(&from_annotation),
        "`job::build` puts the argv on the container unchanged"
    );

    let mut without = backup();
    without.metadata.annotations = None;
    assert!(
        runner_job_spec(&without).is_err(),
        "a `Backup` with no runner argv is an error to REPORT, never an argv to invent"
    );
}

/// The two mandatory engine variables are on the container, and they mirror
/// what the `Dockerfile` pins.
#[test]
fn the_engine_env_mirrors_the_dockerfile() {
    let dockerfile = workspace_file("Dockerfile");
    assert!(
        dockerfile.contains(ENGINE_DIGEST),
        "`job::ENGINE_DIGEST` must be the digest the `Dockerfile`'s engine stage pins. Without \
         these two variables `logweir backup run` exits 1 BEFORE the engine spawns — no archive \
         and no receipt — because a signed receipt must name the engine build that produced it. \
         The Dockerfile sets `LOGWEIR_ENGINE_BIN` and neither of these, so the Job template is \
         where they have to be stated and this assertion is what keeps the statement true."
    );
    let pinned = workspace_file("third_party/kafka-backup-binary.digest");
    assert_eq!(
        pinned.trim(),
        ENGINE_DIGEST,
        "`third_party/kafka-backup-binary.digest` and the Dockerfile's `FROM …@sha256:` are \
         updated together, never separately; this constant is the third place that value has to \
         agree with"
    );
    assert!(
        workspace_root()
            .join("third_party")
            .join(format!("kafka-backup-v{ENGINE_VERSION}.tar.gz"))
            .exists(),
        "`job::ENGINE_VERSION` must be the vendored engine's own version — the tarball \
         `third_party/kafka-backup-v{ENGINE_VERSION}.tar.gz` is what makes it checkable, and \
         Global Constraint 8 fixes the floor at that value"
    );

    let spec = runner_job_spec(&backup()).expect("the fixture yields a spec");
    let job = job::build(&spec);
    let env = job
        .spec
        .as_ref()
        .and_then(|s| s.template.spec.as_ref())
        .and_then(|p| p.containers.first())
        .and_then(|c| c.env.clone())
        .expect("the container carries env");
    let literal = |name: &str| {
        env.iter()
            .find(|e| e.name == name)
            .and_then(|e| e.value.clone())
    };
    assert_eq!(
        literal("LOGWEIR_ENGINE_VERSION").as_deref(),
        Some(ENGINE_VERSION),
        "the Job sets LOGWEIR_ENGINE_VERSION"
    );
    assert_eq!(
        literal("LOGWEIR_ENGINE_DIGEST").as_deref(),
        Some(ENGINE_DIGEST),
        "the Job sets LOGWEIR_ENGINE_DIGEST"
    );
    assert_eq!(
        literal("TMPDIR").as_deref(),
        Some("/work"),
        "TMPDIR points at the writable emptyDir: the container's root filesystem is read-only, \
         so the engine's temporary files have nowhere else to go"
    );

    // The credential arrives BY REFERENCE, never as a literal.
    let s3 = env
        .iter()
        .find(|e| e.name == "AWS_SECRET_ACCESS_KEY")
        .expect("the archive credential is on the container");
    assert!(
        s3.value.is_none(),
        "an object-store credential is `valueFrom.secretKeyRef` and NEVER a literal: a literal \
         would appear in `kubectl get job -o yaml` for anyone with job read"
    );
    assert_eq!(
        s3.value_from
            .as_ref()
            .and_then(|v| v.secret_key_ref.as_ref())
            .map(|r| r.name.clone()),
        Some("logweir-s3".to_string()),
        "the Secret is the one `spec.archive.secretRef` names"
    );

    // The signing key is a FILE, at 0440, with fsGroup — the four values that
    // were verified live, all four combinations (docs/kubernetes.md §3).
    let volumes = job
        .spec
        .as_ref()
        .and_then(|s| s.template.spec.as_ref())
        .and_then(|p| p.volumes.clone())
        .expect("the pod has volumes");
    let signing = volumes
        .iter()
        .find(|v| v.name == "signing")
        .and_then(|v| v.secret.clone())
        .expect("the signing key is a Secret volume");
    assert_eq!(
        signing.secret_name.as_deref(),
        Some(SIGNING_KEY_SECRET),
        "the signing key comes from the Secret the plan names, and NO private key material is \
         anywhere in this tree"
    );
    assert_eq!(
        signing.default_mode,
        Some(0o440),
        "`0440`, not `0400`: with `fsGroup` set, kubelet writes the file `root:65532` and ORs \
         group-read in anyway, so `0400` lands on disk as `0440` regardless"
    );
    assert_eq!(
        job.spec
            .as_ref()
            .and_then(|s| s.template.spec.as_ref())
            .and_then(|p| p.security_context.as_ref())
            .and_then(|c| c.fs_group),
        Some(65532),
        "WITHOUT `fsGroup` EVERY RUN DIES OPENING ITS OWN SIGNING KEY: kubelet writes Secret \
         files `root:root`, so a container running as 65532 reads nothing through the owner bits"
    );
    assert!(
        volumes.iter().any(|v| v.name == "plan"),
        "the runner's argv points at /plan/backup.yaml, so the plan mount has to exist; \
         `plan_config_map_name` fixes the name — {:?}",
        plan_config_map_name(NAME)
    );
}

// ---------------------------------------------------------------------------
// The exit code, and where it comes from
// ---------------------------------------------------------------------------

/// The code and the keys come from `pods/log`, and the pod is found by the
/// job-name label — the prefixed one first, the legacy one as a fallback.
///
/// KILLS: reading the log through `pods/exec`.
#[tokio::test]
async fn the_exit_code_and_the_keys_come_from_the_logs_subresource() {
    // THE TWO FORBIDDEN SUBRESOURCES ARE ROUTED, DELIBERATELY, AND THAT MAKES
    // THIS TEST STRONGER RATHER THAN LOOSER. The double panics on a request it
    // has no route for, so with `exec` and `attach` unrouted a reconciler that
    // read the log through either would fail inside `testing.rs` — a real
    // failure, but not the one this test is about, and not one that says
    // "zero requests named pods/exec". Routing them means the reconciler CAN
    // reach them and the assertion below is what refuses: the property is
    // "it did not ask", asserted over the recorder, and not "it could not".
    let mut routes = finished_routes(
        &pod_list_terminated(0),
        log_body(&i7_tail()),
        200,
        "Complete",
    );
    for forbidden in ["/exec", "/attach"] {
        routes.push(Route {
            method: "GET",
            path_suffix: forbidden,
            status: 200,
            body: log_body(&i7_tail()),
        });
    }
    let (client, seen) = mock_client_recording(routes);
    reconcile_backup(
        &backup(),
        &client,
        &unobserved_archive,
        &unverified_evidence,
        utc(2026, 11, 9, 3, 20),
    )
    .await
    .expect("the reconcile succeeds");

    let table: Vec<(String, String)> = seen
        .lock()
        .expect("the recorder is readable")
        .iter()
        .map(|r| (r.method.clone(), r.uri.clone()))
        .collect();

    // MATCHED AS A PATH SEGMENT, NOT AS THE RBAC RESOURCE STRING. The RBAC
    // resource is `pods/exec`, but the REQUEST PATH is
    // `/api/v1/namespaces/<ns>/pods/<name>/exec` — the pod's NAME sits between
    // the two words, so `contains("pods/exec")` matches no real request and
    // the assertion was true of everything. Found by applying this test's own
    // `pods/exec` mutant: the substring form passed it and the log count
    // caught the mutant instead, which is an assertion silently carrying the
    // wrong property.
    for forbidden in ["/exec", "/attach"] {
        let hits: Vec<_> = table
            .iter()
            .filter(|(_, u)| path(u).ends_with(forbidden))
            .collect();
        assert!(
            hits.is_empty(),
            "ZERO requests may end in `{forbidden}` — the `pods{forbidden}` subresource. Either \
             would let the controller run a process inside the pod that holds the SIGNING KEY, \
             which is strictly more than reading its stdout — and the RBAC request under \
             config/rbac/ asks for neither. Got {hits:?}"
        );
    }

    let logs: Vec<_> = table
        .iter()
        .filter(|(m, u)| m == "GET" && path(u).contains("/pods/") && path(u).ends_with("/log"))
        .collect();
    assert_eq!(
        logs.len(),
        1,
        "EXACTLY ONE `GET …/pods/<p>/log`. It is the only route to a runner's stdout, and \
         interface I7's contract is about stdout because the pod log API has NO STREAM SELECTOR \
         (Global Constraint 11) — nothing on stderr is distinguishable by a controller. Got \
         {table:?}"
    );
    assert!(
        path(&logs[0].1).contains(&format!("/pods/{POD}/log")),
        "the log is read from the pod the selector found, by name; got {:?}",
        logs[0]
    );

    let selector = format!(
        "labelSelector={}",
        urlencode(&format!("{JOB_NAME_LABEL}={NAME}"))
    );
    assert!(
        table
            .iter()
            .any(|(m, u)| m == "GET" && u.contains("/pods?") && u.contains(&selector)),
        "the pod is found by `labelSelector=batch.kubernetes.io%2Fjob-name%3D<job>` — the \
         reconciler holds a Job, not a Pod, and critique B M10 asked for a stated rule rather \
         than an improvisation. Expected {selector} in {table:?}"
    );

    // SECOND ARM: the prefixed selector answers empty, and the legacy one is
    // tried exactly once.
    let (client, seen) = mock_client_recording(finished_routes(
        EMPTY_POD_LIST,
        log_body(&i7_tail()),
        200,
        "Failed",
    ));
    reconcile_backup(
        &backup(),
        &client,
        &unobserved_archive,
        &unverified_evidence,
        utc(2026, 11, 9, 3, 20),
    )
    .await
    .expect("a Job with no pod is a terminal status, not an error");

    let table: Vec<String> = seen
        .lock()
        .expect("the recorder is readable")
        .iter()
        .filter(|r| r.uri.contains("/pods?"))
        .map(|r| r.uri.clone())
        .collect();
    let legacy = format!(
        "labelSelector={}",
        urlencode(&format!("{JOB_NAME_LABEL_LEGACY}={NAME}"))
    );
    assert_eq!(
        table.iter().filter(|u| u.contains(&legacy)).count(),
        1,
        "the legacy `job-name=<job>` selector is tried EXACTLY ONCE when the prefixed one \
         returns nothing. Both labels are set on 1.29 and only the prefixed one is current, so a \
         controller using only one of the two breaks on a different cluster in each case. Got \
         {table:?}"
    );
    assert_eq!(
        table.len(),
        2,
        "two list calls and no more — the fallback is a fallback, not a retry loop; got {table:?}"
    );
}

/// Percent-encode the way `kube`'s query builder does, for the selector
/// assertions above. Only the two characters a label selector needs.
fn urlencode(s: &str) -> String {
    s.replace('/', "%2F").replace('=', "%3D")
}

/// The container is selected by NAME, over a pod whose index 0 exited 0.
///
/// KILLS: `containerStatuses[0]` instead of the container named `runner` —
/// the patched code would be 0, not 2.
#[tokio::test]
async fn the_container_is_selected_by_name() {
    let (client, _seen, bodies) = {
        let routes = finished_routes(&pod_list_terminated(2), log_body(&i7_tail()), 200, "Failed");
        mock_client_recording_bodies(routes)
    };
    let outcome = reconcile_backup(
        &backup(),
        &client,
        &unobserved_archive,
        &unverified_evidence,
        utc(2026, 11, 9, 3, 20),
    )
    .await
    .expect("the reconcile succeeds");

    assert_eq!(
        outcome.exit_code,
        Some(2),
        "index 0 is an unrelated sidecar that exited 0; the `runner` container exited 2"
    );
    let statuses = patched_statuses(&bodies.lock().expect("the body recorder is readable"));
    assert_eq!(statuses.len(), 1, "one status patch; got {statuses:?}");
    assert_eq!(
        statuses[0]["exitCode"].as_i64(),
        Some(2),
        "`status.exitCode` IS 2. `containerStatuses` is not ordered by anything a controller may \
         rely on, and an init container or a logging sidecar puts an unrelated `exitCode: 0` at \
         index 0 — which turns a failed run into a green badge, because the badge rule reads \
         `exitCode == 0`"
    );
    assert_eq!(
        statuses[0]["phase"].as_str(),
        Some("Failed"),
        "a non-zero code is `phase: Failed`"
    );
    assert_eq!(
        statuses[0]["exitReason"].as_str(),
        Some(REASON_DRILL_NOT_PASS),
        "exit 2's wire reason is `drill-not-pass` — a result that is not a pass, with a document \
         written AND signed, which is the most valuable result the tool produces"
    );

    // And the pure function, directly, so the property has a test that does
    // not go through a client at all.
    let list: Value =
        serde_json::from_str(&pod_list_terminated(2)).expect("the fixture is a PodList");
    let pod: k8s_openapi::api::core::v1::Pod =
        serde_json::from_value(list["items"][0].clone()).expect("the item is a Pod");
    assert_eq!(
        terminated_exit_code(&pod),
        Some(2),
        "`terminated_exit_code` finds `runner` by name"
    );
}

/// The TTL patch comes after the status patch, and never without it.
///
/// KILLS: patching `ttlSecondsAfterFinished` at Job creation.
#[tokio::test]
async fn ttl_is_patched_only_after_status() {
    let (client, seen, bodies) = mock_client_recording_bodies(finished_routes(
        &pod_list_terminated(0),
        log_body(&i7_tail()),
        200,
        "Complete",
    ));
    let outcome = reconcile_backup(
        &backup(),
        &client,
        &unobserved_archive,
        &unverified_evidence,
        utc(2026, 11, 9, 3, 20),
    )
    .await
    .expect("the reconcile succeeds");
    assert!(outcome.ttl_patched, "the Job was patched with a TTL");

    // THE GUARDS ARE SCOPED, so nothing is held across the second arm's
    // `await`: a `std::sync::MutexGuard` alive across an await point is a
    // deadlock waiting for a multi-threaded runtime, and clippy is right to
    // refuse it even in a test.
    {
        let seen = seen.lock().expect("the recorder is readable");
        let status_index = seen
            .iter()
            .position(|r| r.method == "PATCH" && path(&r.uri).ends_with("/status"))
            .expect("the status was patched");
        let job_index = seen
            .iter()
            .position(|r| {
                r.method == "PATCH"
                    && path(&r.uri).contains("/jobs/")
                    && !path(&r.uri).ends_with("/status")
            })
            .expect("the Job was patched");
        assert!(
            status_index < job_index,
            "THE ORDER IS LOAD-BEARING. The TTL controller deletes the Job AND ITS PODS, and the \
             exit code lives only on the pod — so a TTL that exists before the status write is a \
             race pod GC can win (design-operator.md:264-266). status index {status_index}, job \
             index {job_index}: {seen:?}"
        );

        let bodies = bodies.lock().expect("the body recorder is readable");
        let ttl_patch: Value = bodies
            .iter()
            .find(|b| {
                b.method == "PATCH"
                    && path(&b.uri).contains("/jobs/")
                    && !path(&b.uri).ends_with("/status")
            })
            .map(|b| serde_json::from_str(&b.body).expect("the Job patch is JSON"))
            .expect("the Job patch body was recorded");
        assert_eq!(
            ttl_patch["spec"]["ttlSecondsAfterFinished"].as_i64(),
            Some(i64::from(TTL_SECONDS_AFTER_FINISHED)),
            "the TTL is the one constant, patched onto the Job's spec"
        );
    }

    // SECOND ARM: THE JOB CARRIES NO TTL AT CREATION TIME.
    //
    // MUTATION ROUND FINDING. Without this arm the brief's "patch
    // `ttlSecondsAfterFinished` at Job creation" mutant SURVIVED this test:
    // the ordering assertion above is about two PATCHes and says nothing about
    // the POST, so a Job created with a TTL already on it still produced a
    // status patch before a Job patch and passed. The absence assertion lived
    // only in `backup_reconcile_creates_exactly_one_job_with_the_pinned_failure_policy`,
    // which is a different test than the one the mutant is filed against — and
    // "some other test catches it" is exactly how a named pair rots. The whole
    // property is one property and now lives in one test.
    let create_routes = create_routes(201, existing_plan_config_map(UID));
    let (client, _seen, bodies) = mock_client_recording_bodies(create_routes);
    reconcile_backup(
        &backup(),
        &client,
        &unobserved_archive,
        &unverified_evidence,
        utc(2026, 11, 9, 3, 17),
    )
    .await
    .expect("the create pass succeeds");
    {
        let bodies = bodies.lock().expect("the body recorder is readable");
        let posted: Value = bodies
            .iter()
            .find(|b| b.method == "POST")
            .map(|b| serde_json::from_str(&b.body).expect("the POSTed Job is JSON"))
            .expect("the Job was created");
        assert!(
            posted["spec"]["ttlSecondsAfterFinished"].is_null(),
            "A JOB CREATED WITH A TTL IS A RACE THE CONTROLLER CANNOT WIN. The TTL controller \
             deletes the Job AND its pods, the exit code lives only on the pod, and the pod can \
             terminate before this controller's next reconcile — so the TTL must not exist until \
             the code is on the status. Got {:?}",
            posted["spec"]["ttlSecondsAfterFinished"]
        );
    }

    // THIRD ARM: a status patch answered 500 produces ZERO Job patches.
    let (client, seen) = mock_client_recording(finished_routes(
        &pod_list_terminated(2),
        log_body(&i7_tail()),
        500,
        "Failed",
    ));
    let err = reconcile_backup(
        &backup(),
        &client,
        &unobserved_archive,
        &unverified_evidence,
        utc(2026, 11, 9, 3, 20),
    )
    .await
    .expect_err("a 500 on the status patch is an error, and the reconcile requeues");
    assert!(
        format!("{err}").contains("kubernetes API error"),
        "the error names the API failure; got {err}"
    );
    let job_patches: Vec<_> = seen
        .lock()
        .expect("the recorder is readable")
        .iter()
        .filter(|r| r.method == "PATCH" && r.uri.contains("/jobs/"))
        .cloned()
        .collect();
    assert!(
        job_patches.is_empty(),
        "ZERO Job patches when the status write did not succeed. The `?` on the status patch is \
         what makes the ordering a guarantee rather than a comment: pod GC cannot start on a run \
         whose code was never recorded. Got {job_patches:?}"
    );
}

/// The five codes and their five wire reasons — a table test.
#[test]
fn every_exit_code_maps_to_its_wire_reason() {
    for (code, expected) in [
        (0, REASON_OK),
        (1, REASON_OPERATIONAL),
        (2, REASON_DRILL_NOT_PASS),
        (3, REASON_GUARD_REFUSED),
        (4, REASON_SIGNING_OR_LOCK),
    ] {
        assert_eq!(
            wire_reason_for_exit(code),
            expected,
            "Global Constraint 11: exit {code} is `{expected}` on `status.exitReason`, in ONE \
             spelling everywhere — the same spelling `logweir drill`'s own outcome strings use"
        );
    }
    // The catch-all is reached ONLY for codes the contract does not define.
    for code in [-1, 5, 6, 42, 128, 137, 139, 255] {
        assert_eq!(
            wire_reason_for_exit(code),
            REASON_OPERATIONAL,
            "exit {code} is outside GC11's contract — a SIGKILL, a segfault, a shell that could \
             not find the binary — and every one of those is the runner failing in a way the \
             contract does not describe, which is `operational` and never a guess at one of the \
             other four"
        );
    }
    assert_eq!(
        [
            REASON_OK,
            REASON_OPERATIONAL,
            REASON_DRILL_NOT_PASS,
            REASON_GUARD_REFUSED,
            REASON_SIGNING_OR_LOCK,
        ]
        .iter()
        .filter(|r| r.chars().any(char::is_uppercase))
        .count(),
        0,
        "the five are WIRE STRINGS — lowercase-hyphenated — and condition reasons are a \
         DIFFERENT, CamelCase vocabulary. A consumer that had to accept both `drill-not-pass` and \
         `DrillNotPass` is a consumer with a bug waiting"
    );
    assert_eq!(
        TERMINAL_STATES.len(),
        16,
        "the sixteen terminal states that are NOT an exit code — the original ten, plus \
         `NameTooLong` (errata E5d) and `ReferentNotFound` / `PlanConfigMapConflict` / \
         `ArchiveUrlUnreadable` (errata E5a), plus `PlanHashMismatch` / `ClusterNotReachable` \
         (Task 20's `Restore` admission, and note that its third admission reason \
         `ApprovalNotVerified` is deliberately NOT here — it is a thirty-second HOLD under \
         interface I19, so it lives in `CONDITION_REASONS`); got {TERMINAL_STATES:?}"
    );
    for state in TERMINAL_STATES {
        assert!(
            state.chars().next().is_some_and(|c| c.is_ascii_uppercase()),
            "`{state}` is a terminal state and terminal states are CamelCase"
        );
    }
}

/// The CONDITION reason for each code — **CamelCase**, and a different
/// function from the wire reason.
///
/// ERRATA **E5b**, review finding LOW-2. The brief mandated
/// `reason_for_exit(code)` as the condition `reason` and its values were
/// lowercase-hyphenated, so every failed `Backup` carried a `reason` the real
/// `metav1.Condition` validation pattern forbids.
///
/// KILLS: `reason_for_exit` returning the wire string again.
#[test]
fn every_exit_code_maps_to_a_camel_case_condition_reason() {
    for (code, expected) in [
        (0, "Ok"),
        (1, "Operational"),
        (2, "DrillNotPass"),
        (3, "GuardRefused"),
        (4, "SigningOrLock"),
    ] {
        assert_eq!(
            reason_for_exit(code),
            expected,
            "exit {code}'s CONDITION reason is `{expected}`"
        );
    }
    for code in [-1, 5, 42, 137, 139, 255] {
        assert_eq!(
            reason_for_exit(code),
            "Operational",
            "the two functions partition the codes the SAME way; only the spelling differs"
        );
    }
}

/// **Every** condition reason this crate can write matches the real
/// `metav1.Condition` `reason` pattern.
///
/// `^[A-Za-z]([A-Za-z0-9_,:]*[A-Za-z0-9_])?$` — upstream's own validation
/// (`staging/src/k8s.io/apimachinery/pkg/apis/meta/v1/types.go`), which
/// **forbids `-`**. Today's CRD hand-rolls the condition schema with no
/// pattern, so a hyphenated reason is accepted by the API server and is still
/// wrong: the day any task gives that schema the real pattern, or any consumer
/// validates against it, every failed `Backup` becomes unwritable.
///
/// MATCHED BY HAND AND NOT BY `regex`. Global Constraint 38 closes the
/// workspace graph and `regex` is not in `Cargo.lock`, so a dependency for one
/// character-class walk would be a stop-and-report. The matcher below is the
/// pattern read literally, and `a_hyphenated_reason_is_rejected_by_the_matcher`
/// below it is what stops the matcher from being a function that returns true.
///
/// KILLS: a hyphenated reason constant anywhere in `conditions.rs`.
#[test]
fn the_condition_reasons_are_valid_metav1_reasons() {
    // The pattern, literally: one leading letter, then any of
    // `[A-Za-z0-9_,:]`, and the LAST character may not be `,` or `:`.
    fn is_valid_reason(s: &str) -> bool {
        let mut chars = s.chars();
        let Some(first) = chars.next() else {
            return false;
        };
        if !first.is_ascii_alphabetic() {
            return false;
        }
        if !s
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == ',' || c == ':')
        {
            return false;
        }
        s.chars()
            .next_back()
            .is_some_and(|c| c.is_ascii_alphanumeric() || c == '_')
    }

    // THE MATCHER IS TESTED FIRST. A validator that cannot reject has not
    // validated anything, and the value it has to reject is exactly the one
    // this errata is about.
    assert!(!is_valid_reason("guard-refused"), "a hyphen is forbidden");
    assert!(!is_valid_reason("drill-not-pass"), "a hyphen is forbidden");
    assert!(!is_valid_reason("signing-or-lock"), "a hyphen is forbidden");
    assert!(!is_valid_reason(""), "an empty reason is not a reason");
    assert!(
        !is_valid_reason("9Lives"),
        "the first character is a letter"
    );
    assert!(!is_valid_reason("Trailing:"), "`:` may not be last");
    assert!(is_valid_reason("GuardRefused"));
    assert!(is_valid_reason("NoExitCode"));

    let mut checked = 0;
    for reason in CONDITION_REASONS.iter().chain(TERMINAL_STATES.iter()) {
        checked += 1;
        assert!(
            is_valid_reason(reason),
            "`{reason}` is written into a `metav1.Condition`'s `reason` and does not match \
             ^[A-Za-z]([A-Za-z0-9_,:]*[A-Za-z0-9_])?$"
        );
    }
    for r#type in CONDITION_TYPES {
        checked += 1;
        assert!(
            is_valid_reason(r#type),
            "`{type}` is a condition TYPE and the same convention applies",
            type = r#type
        );
    }
    assert!(
        checked >= 20,
        "the iteration covered only {checked} strings — a loop over an empty list asserts \
         nothing"
    );
    // And every code's condition reason is IN the list the loop walked, so a
    // sixth code with a reason nobody added to `CONDITION_REASONS` cannot slip
    // past the regex by not being enumerated.
    for code in [0, 1, 2, 3, 4, 137] {
        assert!(
            CONDITION_REASONS.contains(&reason_for_exit(code)),
            "`reason_for_exit({code})` = `{}` is not in CONDITION_REASONS, so the regex test \
             never saw it",
            reason_for_exit(code)
        );
    }
}

/// The two vocabularies never overlap, and each stays in its own field.
///
/// This is the property errata **E5b** actually asserts: `exitReason` keeps
/// GC11's wire strings and the condition `reason` is CamelCase. If a future
/// edit re-spelled the wire strings CamelCase "for consistency", `exitReason`
/// would disagree with the shipped CRD's own field description and with
/// `logweir drill`'s outcome strings — so the disjointness is the guard.
#[test]
fn the_two_reason_vocabularies_never_overlap() {
    let wire: Vec<&str> = (0..=4).map(wire_reason_for_exit).collect();
    for reason in CONDITION_REASONS.iter().chain(TERMINAL_STATES.iter()) {
        assert!(
            !wire.contains(reason),
            "`{reason}` is a condition reason AND a `status.exitReason` wire string; the two \
             vocabularies must never overlap"
        );
    }
    for w in &wire {
        assert!(
            !w.chars().any(char::is_uppercase),
            "`{w}` lands on `status.exitReason` and the wire vocabulary is \
             lowercase-hyphenated"
        );
    }
    // The CRD's own field description is the third surface, and it still names
    // the wire strings — which is what "exitReason keeps its vocabulary" means
    // in practice.
    let described = emitted_backup_crd()["spec"]["versions"][0]["schema"]["openAPIV3Schema"]
        ["properties"]["status"]["properties"]["exitReason"]["description"]
        .as_str()
        .expect("the exitReason field carries a description")
        .to_string();
    for w in &wire {
        assert!(
            described.contains(*w),
            "the shipped CRD's `status.exitReason` description names `{w}`; got: {described}"
        );
    }
}

/// **Interface I13, enforced over THIS module's source text.** Every `Store`
/// call in `controllers/backup.rs` is inside a `spawn_blocking(…)` closure.
///
/// # Why this exists beside Task 19's `no_store_call_is_made_outside_spawn_blocking`
///
/// Task 19's test names this file in its `I13_FILES` and is green — and it is
/// **blind to most of this file's oracle wiring**, which I found by planting a
/// mutant and watching it survive. Its `sanitize` treats a `'` as the start of
/// a character literal and blanks everything up to the next `'`; Rust
/// LIFETIMES are spelled with the same character, and `ArchiveOracle`'s own
/// signature is `-> BoxFuture<'static, …>`, so the span from that tick to the
/// next one in the file — which includes the whole `reconcile` closure that
/// clones the handle — is blanked before its token scan runs. Measured: the
/// planted `let store = store.clone();` inside `reconcile`'s async block was
/// blanked to spaces and the guard passed, 1 passed / 0 failed.
///
/// So this is the same property, checked with a sanitizer that knows a
/// lifetime from a character literal, over the one file this task owns. Task
/// 19's guard keeps its wider file list; a fix to its sanitizer is that task's
/// and is reported rather than made here.
///
/// KILLS: any `Store` touch in an async region of this module outside
/// `spawn_blocking` — the shape that COMPILES CLEANLY and panics with *Cannot
/// start a runtime from within a runtime* at the first reconcile.
#[test]
fn no_store_call_in_this_module_is_made_outside_spawn_blocking() {
    /// Comments, strings and raw strings blanked; LIFETIMES LEFT ALONE.
    ///
    /// At a `'`: an escaped next byte or a closing `'` two bytes on is a
    /// character literal; anything else is a lifetime, and only the tick is
    /// consumed. `'a'` and `'\n'` are literals; `'static`, `'a`, `'_` are not.
    fn sanitize(src: &str) -> String {
        let b = src.as_bytes();
        let mut out: Vec<u8> = b
            .iter()
            .map(|c| if *c == b'\n' { b'\n' } else { b' ' })
            .collect();
        let mut i = 0;
        while i < b.len() {
            if b[i] == b'/' && b.get(i + 1) == Some(&b'/') {
                while i < b.len() && b[i] != b'\n' {
                    i += 1;
                }
                continue;
            }
            if b[i] == b'r' && matches!(b.get(i + 1), Some(&b'"') | Some(&b'#')) {
                let mut k = i + 1;
                let start = k;
                while b.get(k) == Some(&b'#') {
                    k += 1;
                }
                let hashes = k - start;
                if b.get(k) == Some(&b'"') {
                    k += 1;
                    let close = format!("\"{}", "#".repeat(hashes));
                    while k < b.len() {
                        if b[k] == b'"' && src[k..].starts_with(&close) {
                            k += close.len();
                            break;
                        }
                        k += 1;
                    }
                    i = k;
                    continue;
                }
            }
            if b[i] == b'"' {
                i += 1;
                while i < b.len() {
                    if b[i] == b'\\' {
                        i += 2;
                        continue;
                    }
                    if b[i] == b'"' {
                        i += 1;
                        break;
                    }
                    i += 1;
                }
                continue;
            }
            if b[i] == b'\'' {
                let literal = b.get(i + 1) == Some(&b'\\') || b.get(i + 2) == Some(&b'\'');
                if literal {
                    i += 1;
                    while i < b.len() {
                        if b[i] == b'\\' {
                            i += 2;
                            continue;
                        }
                        if b[i] == b'\'' {
                            i += 1;
                            break;
                        }
                        i += 1;
                    }
                    continue;
                }
                // A LIFETIME. Consume only the tick and keep going, so the
                // code after it is still scanned.
                i += 1;
                continue;
            }
            out[i] = b[i];
            i += 1;
        }
        String::from_utf8_lossy(&out).into_owned()
    }

    /// The balanced `{…}` or `(…)` span beginning at or after `from`.
    fn span_after(text: &str, from: usize, open_ch: u8, close_ch: u8) -> Option<(usize, usize)> {
        let b = text.as_bytes();
        let open = (from..b.len()).find(|&i| b[i] == open_ch)?;
        let mut depth = 0usize;
        for (i, ch) in b.iter().enumerate().skip(open) {
            if *ch == open_ch {
                depth += 1;
            } else if *ch == close_ch {
                depth -= 1;
                if depth == 0 {
                    return Some((open, i));
                }
            }
        }
        None
    }

    fn occurrences(text: &str, needle: &str) -> Vec<usize> {
        let mut out = Vec::new();
        let mut from = 0;
        while let Some(at) = text[from..].find(needle) {
            out.push(from + at);
            from += at + 1;
        }
        out
    }

    // THE SANITIZER IS TESTED BEFORE IT IS TRUSTED, and the case it is here
    // for is the first one.
    let probe = "fn f() -> BoxFuture<'static, u8> { let store = store.clone(); }";
    assert!(
        sanitize(probe).contains("store.clone()"),
        "a lifetime tick must NOT blank the code after it — this is the exact blindness that let \
         the planted mutant survive Task 19's guard. Got: {}",
        sanitize(probe)
    );
    assert!(
        !sanitize("let s = \"store.get(\";").contains("store.get("),
        "a string literal IS blanked"
    );
    assert!(
        !sanitize("// store.get(\n").contains("store.get("),
        "a comment IS blanked"
    );
    assert!(
        sanitize("if c == '\\'' { store.get(k) }").contains("store.get(k)"),
        "an escaped-quote character literal must not swallow the rest of the line"
    );

    let raw = workspace_file("crates/weirkeeper/src/controllers/backup.rs");
    let text = sanitize(&raw);
    assert!(
        text.contains("spawn_blocking("),
        "the scan found no `spawn_blocking(` at all — a scan of a blanked file asserts nothing"
    );

    // Every async REGION: `async fn` bodies and `async move`/`async` blocks.
    // Both, because the oracle's future is an `async move` block nested in an
    // `async fn`, and a future built outside one is just as fatal.
    let mut regions: Vec<(usize, usize)> = Vec::new();
    for marker in ["async fn ", "async move {", "async {"] {
        for at in occurrences(&text, marker) {
            if let Some(span) = span_after(&text, at, b'{', b'}') {
                regions.push(span);
            }
        }
    }
    assert!(
        regions.len() >= 5,
        "the scan found {} async regions in a file with several async functions — a broken walk",
        regions.len()
    );
    let blocking: Vec<(usize, usize)> = occurrences(&text, "spawn_blocking(")
        .into_iter()
        .filter_map(|at| span_after(&text, at, b'(', b')'))
        .collect();
    assert!(
        !blocking.is_empty(),
        "and at least one `spawn_blocking(…)` group"
    );

    // `observe_archive(` IS IN THE TOKEN LIST, and Task 19's does not have it.
    // It is the function that holds the two `Store` reads, so calling it
    // outside `spawn_blocking` is the failure this property is about even
    // though the call site names no `Store` at all.
    let mut offences = Vec::new();
    let mut checked = 0;
    for token in ["store.", "Store::", "observe_archive("] {
        for at in occurrences(&text, token) {
            if !regions.iter().any(|(s, e)| at > *s && at < *e) {
                continue;
            }
            checked += 1;
            if blocking.iter().any(|(s, e)| at > *s && at < *e) {
                continue;
            }
            let line = text[..at].lines().count();
            offences.push(format!("line {line}: `{token}`"));
        }
    }
    assert!(
        checked >= 1,
        "no token was found inside any async region — after the oracle wiring landed there is at \
         least one (`observe_archive(` inside `reconcile`'s `spawn_blocking`), so a count of \
         zero means the walk or the sanitizer is broken and this test asserts nothing"
    );
    assert!(
        offences.is_empty(),
        "interface I13: every `Store` call from a reconciler goes through \
         `tokio::task::spawn_blocking(move || …).await`. `Store` drives its own current-thread \
         runtime and `kube` drives reconcilers ON one, so a direct call COMPILES CLEANLY and \
         panics with *Cannot start a runtime from within a runtime* at the first reconcile.\n  {}",
        offences.join("\n  ")
    );
}

// ---------------------------------------------------------------------------
// The plan ConfigMap — errata E5a, review finding HIGH-1
// ---------------------------------------------------------------------------

/// The ConfigMap body the reconciler `POST`s, as JSON.
fn posted_config_map(bodies: &[SeenBody]) -> Value {
    let body = bodies
        .iter()
        .find(|b| b.method == "POST" && path(&b.uri).ends_with("/configmaps"))
        .expect("the plan ConfigMap was POSTed");
    serde_json::from_str(&body.body).expect("the POSTed ConfigMap is JSON")
}

/// One create pass, returning `(recorder, body recorder)`.
async fn create_pass(routes: Vec<Route>) -> (Vec<weirkeeper::testing::SeenRequest>, Vec<SeenBody>) {
    let (client, seen, bodies) = mock_client_recording_bodies(routes);
    reconcile_backup(
        &backup(),
        &client,
        &unobserved_archive,
        &unverified_evidence,
        utc(2026, 11, 9, 3, 17),
    )
    .await
    .expect("the create pass succeeds");
    let seen = seen.lock().expect("the recorder is readable").clone();
    let bodies = bodies
        .lock()
        .expect("the body recorder is readable")
        .clone();
    (seen, bodies)
}

/// `backup.yaml` **parses back as the type the CLI parses**, field by field.
///
/// ERRATA **E5a**, review finding HIGH-1: nothing in the plan rendered this
/// ConfigMap, and the Job mounts it. Measured live before the fix, the pod
/// stalled in `ContainerCreating` on `MountVolume.SetUp failed for volume
/// "plan": configmap "<name>-plan" not found` and every scheduled backup
/// failed as an unexplained `NoExitCode`.
///
/// PARSED BACK RATHER THAN STRING-MATCHED. A golden string would pass for a
/// document `logweir backup run` cannot load; `serde_yaml::from_str::<BackupSpec>`
/// is the same call the CLI makes (`crates/logweir/src/backup/mod.rs:261`), so
/// this asserts loadability and not resemblance.
///
/// KILLS: rendering the bootstrap servers from `Backup.spec` (it has none);
/// dropping the `storage` block's `backend` tag; putting the cluster id in
/// `allowed_cluster_ids`.
#[tokio::test]
async fn the_rendered_plan_parses_back_as_a_backup_spec() {
    let (_seen, bodies) = create_pass(create_routes(201, existing_plan_config_map(UID))).await;
    let cm = posted_config_map(&bodies);

    assert_eq!(
        cm["metadata"]["name"].as_str(),
        Some(plan_config_map_name(NAME).as_str()),
        "the ConfigMap is named `<backup name>-plan`, which is what `job::build` mounts"
    );
    assert_eq!(
        cm["metadata"]["namespace"].as_str(),
        Some(NS),
        "in the `Backup`'s own namespace"
    );

    // EXACTLY TWO KEYS. A third would be a file the runner does not read and a
    // surface an auditor has to account for.
    let data = cm["data"].as_object().expect("the ConfigMap carries data");
    let mut keys: Vec<&str> = data.keys().map(String::as_str).collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        vec!["allowed-clusters.json", "backup.yaml"],
        "EXACTLY the two keys Task 18's argv points `--spec` and `--allowed-clusters` at"
    );

    // ---- backup.yaml, parsed with the CLI's own call --------------------
    let yaml_text = data["backup.yaml"]
        .as_str()
        .expect("backup.yaml is a string");
    let spec: logweir_core::spec::BackupSpec =
        serde_yaml::from_str(yaml_text).unwrap_or_else(|e| {
            panic!("the rendered backup.yaml does not parse as BackupSpec: {e}\n{yaml_text}")
        });

    assert_eq!(
        spec.source.bootstrap_servers,
        vec![
            "broker-0.prod:9093".to_string(),
            "broker-1.prod:9093".to_string()
        ],
        "the addresses come from the `KafkaCluster` `sourceRef` names, NEVER from `Backup.spec`, \
         which carries none"
    );
    assert_eq!(
        spec.source.topics,
        vec!["orders".to_string(), "payments".to_string()],
        "`topics` is `Backup.spec.topics` VERBATIM — no expansion, no reordering, no addition"
    );
    assert_eq!(
        spec.source.auth,
        logweir_core::spec::AuthSpec::ScramSha512 {
            username: "logweir".to_string(),
            tls: true
        },
        "the auth block is the referent's `mode`, `username` and `tls` — and NO PASSWORD at any \
         variant: the credential reaches the runner through the engine's own `${{VAR}}` expansion"
    );
    // ONE PARSER, AND THAT IS THE ASSERTION. `storage` is `spec.archive.url`
    // through `retention::storage_url_for` — the SAME function the
    // controller's own read-only archive handle is built with — so the runner
    // and the controller cannot disagree about where the archive is. A
    // stringly-typed renderer would emit `backend: filesystem` beside a
    // `bucket:` key and the engine's config load would fail with a hard
    // missing-field error the unknown-key readback cannot catch.
    assert_eq!(
        spec.storage,
        weirkeeper::retention::storage_url_for("s3://kafka-backups/logweir")
            .expect("the fixture archive URL parses"),
        "the rendered `storage` block IS `storage_url_for`'s output, not a second parse of the \
         same string"
    );
    match &spec.storage {
        logweir_core::engine::StorageUrl::S3 { bucket, prefix, .. } => {
            assert_eq!(bucket, "kafka-backups", "the bucket comes off the URL");
            assert_eq!(prefix, "logweir", "and so does the prefix");
        }
        other => panic!("`s3://…` renders the S3 variant, tagged `backend: s3`; got {other:?}"),
    }
    assert!(
        yaml_text.contains("backend: s3"),
        "the enum is INTERNALLY TAGGED and the tag is in the document — that is what makes the \
         variants' incompatible required fields decidable by the engine's own loader; got:\n\
         {yaml_text}"
    );
    assert_eq!(
        spec.backup.compression, "zstd",
        "the `backup:` block is `logweir-core`'s own defaults; `Backup.spec` has no tunables and \
         inventing CRD fields for them is Task 15b's decision"
    );
    assert_eq!(spec.backup.segment_max_records, 1000);
    assert_eq!(spec.backup.max_concurrent_partitions, 3);

    // `backup_id`, BOTH ARMS. The main fixture carries no owner reference —
    // the shape of a `Backup` created by hand or by Task 26's page — so it
    // takes the object's own UID, which is unique per object per cluster and
    // is the argument `slot::backup_id_for` itself makes for using a UID.
    assert_eq!(
        spec.backup_id, UID,
        "a `Backup` with no controller owner takes its own UID as the archive's backup id"
    );

    // AND FOR A SCHEDULED `Backup` IT IS BYTE-IDENTICAL TO THE ARGV'S
    // OVERRIDE, which is the coupling that matters: the override WINS at run
    // time (`crates/logweir/src/backup/mod.rs:333-337`, interface I10), so a
    // disagreement would be invisible until a `Backup` with no override wrote
    // under a different archive prefix than the one an operator was told
    // about. The flag is read out of the annotation HERE and not in `src/`:
    // this crate's own guard
    // (`tests/schedule_controller.rs::the_backup_id_override_is_passed_not_defined`)
    // permits the token on code lines in `backup_schedule.rs` alone.
    let scheduled = scheduled_backup();
    let argv = runner_argv(&scheduled).expect("the fixture carries a runner argv");
    let at = argv
        .iter()
        .position(|a| a == "--backup-id-override")
        .expect("the argv carries the override flag (interface I10)");
    assert_eq!(
        plan_backup_id(&scheduled),
        argv[at + 1],
        "the rendered `backup_id` and the argv's override are ONE decision"
    );
    assert_eq!(
        plan_backup_id(&scheduled),
        weirkeeper::slot::backup_id_for(SCHEDULE_UID, "20261109-031700"),
        "and the decision is `backup_id_for(<controller owner uid>, <slot>)` — the owner of a \
         scheduled `Backup` IS its schedule, so this is Task 18's own value without this file \
         knowing that schedules exist"
    );

    // NO CREDENTIAL ANYWHERE IN EITHER KEY. A ConfigMap has no encryption at
    // rest and a much wider read surface than a Secret.
    let rendered = format!(
        "{}{}",
        data["backup.yaml"].as_str().unwrap_or_default(),
        data["allowed-clusters.json"].as_str().unwrap_or_default()
    );
    for forbidden in ["password", "sasl_password", "secret-access-key", "BEGIN "] {
        assert!(
            !rendered.contains(forbidden),
            "the plan ConfigMap must not name `{forbidden}`: the SASL password reaches the engine \
             through its own environment expansion and the object-store credential reaches the \
             runner as a `secretKeyRef` env var, so neither passes through a rendered document"
        );
    }

    // ---- allowed-clusters.json, in the format the reader parses ---------
    let allowed_text = data["allowed-clusters.json"]
        .as_str()
        .expect("allowed-clusters.json is a string");
    let allowed: logweir_core::spec::AllowedClusters = serde_json::from_str(allowed_text)
        .unwrap_or_else(|e| panic!("the rendered allowlist does not parse: {e}\n{allowed_text}"));
    assert_eq!(
        allowed.source_cluster_id.as_deref(),
        Some(CLUSTER_ID),
        "the cluster id goes in `source_cluster_id` — the id `KafkaCluster.status.clusterId` \
         carries, READ FROM THE BROKER and never from a spec"
    );
    assert!(
        allowed.allowed_cluster_ids.is_empty(),
        "AND `allowed_cluster_ids` IS EMPTY. On the BACKUP path that list is the restore-TARGET \
         allowlist and Global Constraint 18(c) rail 4 REFUSES a run whose observed source cluster \
         id appears in it (`crates/logweir/src/backup/phase_minus1_admit.rs:183-190`), because a \
         cluster cannot be both the source of an archive and one whose topics a drill deletes. \
         Putting the source id there would make every backup exit 3. Got {:?}",
        allowed.allowed_cluster_ids
    );
}

// ---------------------------------------------------------------------------
// `status.backupId` — Task 28a, defect 1
// ---------------------------------------------------------------------------

/// **THE TERMINAL STATUS CARRIES THE BACKUP ID, AND NO OTHER PATCH DOES.**
///
/// `BackupStatus.backup_id` was declared on the CRD from the first draft and
/// NOTHING WROTE IT. The one producer, `plan_backup_id`, put the id into the
/// runner's plan ConfigMap alone, so the archive prefix a run wrote under was
/// readable from the runner's INPUT and from nowhere on the object it belongs
/// to. Task 28 found it by walking the UI against a live cluster:
/// `ui/pages/restore-wizard.js::initialState` sets `fields.backupSetRef =
/// status.backupId`, `renderPlanBytes` refuses a document without one, and the
/// restore wizard therefore threw before its first step rendered — the page
/// was an error box on every real cluster, and the walkthrough had to
/// `kubectl patch --subresource=status` the field in by hand to get past it.
///
/// KILLS: dropping the `backupId` line from `finished_status_patch` (arm 1);
/// computing it from `metadata.name` instead of from `plan_backup_id` (arm 1's
/// second assertion and arm 3, and `the_status_backup_id_is_the_plan_document_id`
/// beside it); putting it on the running patch, where no archive exists yet
/// (arm 2).
#[tokio::test]
async fn the_finished_status_patch_carries_the_backup_id() {
    // ARM 1: the finished pass, through the reconciler, so this is the body
    // the API server is actually sent and not a builder call.
    let (client, _seen, bodies) = mock_client_recording_bodies(finished_routes(
        &pod_list_terminated(0),
        log_body(&i7_tail()),
        200,
        "Complete",
    ));
    reconcile_backup(
        &backup(),
        &client,
        &unobserved_archive,
        &unverified_evidence,
        utc(2026, 11, 9, 3, 20),
    )
    .await
    .expect("the reconcile succeeds");
    let statuses = patched_statuses(&bodies.lock().expect("the body recorder is readable"));
    assert_eq!(
        statuses.len(),
        1,
        "ONE status write per pass (Task 16b); got {statuses:?}"
    );
    assert_eq!(
        statuses[0]["backupId"].as_str(),
        Some(plan_backup_id(&backup()).as_str()),
        "the terminal patch carries `backupId`, and it is `plan_backup_id`'s value — the SAME \
         pure function the runner's plan document is rendered from. Without this the field the \
         CRD declares is never written by anything and the restore wizard cannot name a backup \
         set. Got: {}",
        statuses[0]
    );
    assert_eq!(
        statuses[0]["backupId"].as_str(),
        Some(UID),
        "and for this fixture — a `Backup` with no controller owner — that value is the object's \
         own UID, which is what `plan_backup_id`'s second arm returns"
    );
    assert_ne!(
        statuses[0]["backupId"].as_str(),
        Some(NAME),
        "AND IT IS NOT `metadata.name`. The name is `logweir-backup-<schedule>-<slot>`; the \
         archive prefix is a UID-derived id. A status that named the object instead of the \
         archive would send every restore looking for a set that is not there"
    );

    // ARM 2: THE RUNNING PATCH DOES NOT CARRY IT. An id that named a set
    // before the run had written one would be a promise, not a record.
    let (client, _seen, bodies) = mock_client_recording_bodies(running_routes());
    reconcile_backup(
        &backup(),
        &client,
        &unobserved_archive,
        &unverified_evidence,
        utc(2026, 11, 9, 3, 20),
    )
    .await
    .expect("the running reconcile succeeds");
    let running = patched_statuses(&bodies.lock().expect("the body recorder is readable"));
    assert_eq!(running.len(), 1, "the running pass patches once");
    assert!(
        running[0].get("backupId").is_none(),
        "`running_status_patch` carries no `backupId`: the run has not reached the archive yet. \
         Got {}",
        running[0]
    );

    // ARM 3: THE CRASHED AND REFUSED PATCHES DO NOT CARRY IT EITHER — a run
    // that produced no archive names no set. Reached through the builders,
    // which is where the absence is decided.
    let crashed = crashed_status_patch(&backup(), "NoExitCode", NAME, utc(2026, 11, 9, 3, 20));
    assert!(
        crashed["status"].get("backupId").is_none(),
        "`crashed_status_patch` carries no `backupId`; got {crashed}"
    );
    let refused = weirkeeper::controllers::backup::refused_status_patch(
        &backup(),
        TERMINAL_STATE_ARCHIVE_URL_UNREADABLE,
        "the archive URL is not an object-store location",
        utc(2026, 11, 9, 3, 20),
    );
    assert!(
        refused["status"].get("backupId").is_none(),
        "`refused_status_patch` carries no `backupId` either — a run the guard refused never \
         reached the archive. Got {refused}"
    );

    // AND THE SCHEDULED SHAPE, where the id is neither the name nor the
    // object's own UID: `backup_id_for(<controller owner uid>, <slot>)`. This
    // is the arm a `metadata.name` implementation cannot satisfy by accident.
    let scheduled = scheduled_backup();
    let patch = weirkeeper::controllers::backup::finished_status_patch(
        &scheduled,
        0,
        &evidence_keys(&log_body(&i7_tail())),
        None,
        None,
        None,
        None,
        utc(2026, 11, 9, 3, 20),
    );
    assert_eq!(
        patch["status"]["backupId"].as_str(),
        Some(weirkeeper::slot::backup_id_for(SCHEDULE_UID, "20261109-031700").as_str()),
        "a SCHEDULED `Backup`'s status id is its schedule's UID and its slot — the same value \
         Task 18 puts on the runner's `--backup-id-override`, and nothing like either \
         `metadata.name` or the object's own UID. Got {patch}"
    );
}

/// **ONE VALUE, TWO CONSUMERS.** The id on the status is byte-identical to the
/// `backup_id` in the plan document the runner is handed.
///
/// This is the coupling the field exists for. The runner writes its archive
/// under the plan's `backup_id`; the restore wizard resolves a backup set by
/// the status's `backupId`; if those two strings could differ, the page would
/// point an approved restore at a prefix nothing was written to — and the
/// disagreement would only surface at phase 0 of the drill, after an approver
/// had signed.
///
/// Both values are taken from the bytes the reconciler actually SENT: the
/// `POST`ed ConfigMap of a create pass and the `PATCH`ed status of a finished
/// pass, over the same fixture object.
///
/// KILLS: computing the status id from `metadata.name` (or from anything other
/// than `plan_backup_id`) — the two sides part and this fails naming both.
#[tokio::test]
async fn the_status_backup_id_is_the_plan_document_id() {
    // The runner's side: the plan ConfigMap, parsed with the CLI's own call.
    let (_seen, bodies) = create_pass(create_routes(201, existing_plan_config_map(UID))).await;
    let cm = posted_config_map(&bodies);
    let yaml_text = cm["data"]["backup.yaml"]
        .as_str()
        .expect("backup.yaml is a string");
    let spec: logweir_core::spec::BackupSpec = serde_yaml::from_str(yaml_text)
        .unwrap_or_else(|e| panic!("the rendered backup.yaml does not parse: {e}\n{yaml_text}"));

    // The object's side: the terminal status patch.
    let (client, _seen, bodies) = mock_client_recording_bodies(finished_routes(
        &pod_list_terminated(0),
        log_body(&i7_tail()),
        200,
        "Complete",
    ));
    reconcile_backup(
        &backup(),
        &client,
        &unobserved_archive,
        &unverified_evidence,
        utc(2026, 11, 9, 3, 20),
    )
    .await
    .expect("the reconcile succeeds");
    let statuses = patched_statuses(&bodies.lock().expect("the body recorder is readable"));

    assert_eq!(
        statuses[0]["backupId"].as_str(),
        Some(spec.backup_id.as_str()),
        "`status.backupId` and the plan document's `backup_id` are ONE value from ONE function. \
         status: {:?}; plan: {:?}",
        statuses[0]["backupId"],
        spec.backup_id
    );
    assert!(
        !spec.backup_id.is_empty(),
        "…and it is not the empty string on both sides, which would satisfy the equality above \
         while naming nothing"
    );
}

/// The plan ConfigMap `POST` **precedes** the Job `POST`, in the recorded
/// route table.
///
/// THE ORDER IS THE WHOLE FIX. A Job created first mounts a ConfigMap that
/// does not exist yet: the pod stalls in `ContainerCreating`, the
/// `activeDeadlineSeconds` fires, the job controller DELETES the pod, and the
/// reconciler writes a terminal `NoExitCode` with no exit code to read —
/// measured live.
///
/// KILLS: swapping the two writes.
#[tokio::test]
async fn the_plan_config_map_is_posted_before_the_job() {
    let (seen, _bodies) = create_pass(create_routes(201, existing_plan_config_map(UID))).await;
    let posts: Vec<&str> = seen
        .iter()
        .filter(|r| r.method == "POST")
        .map(|r| path(&r.uri))
        .collect();
    let cm_at = posts
        .iter()
        .position(|p| p.ends_with("/configmaps"))
        .expect("the plan ConfigMap was POSTed");
    let job_at = posts
        .iter()
        .position(|p| p.ends_with("/jobs"))
        .expect("the Job was POSTed");
    assert!(
        cm_at < job_at,
        "the ConfigMap `POST` is at index {cm_at} and the Job `POST` at {job_at}; the plan must \
         exist before the pod that mounts it. Got {posts:?}"
    );

    // AND THE SOURCE CLUSTER IS READ BEFORE EITHER, because the document is
    // rendered from it.
    let all: Vec<(&str, &str)> = seen
        .iter()
        .map(|r| (r.method.as_str(), path(&r.uri)))
        .collect();
    let cluster_at = all
        .iter()
        .position(|(m, p)| *m == "GET" && p.ends_with("/kafkaclusters/prod"))
        .expect("the source KafkaCluster was read");
    let first_post = all
        .iter()
        .position(|(m, _)| *m == "POST")
        .expect("something was POSTed");
    assert!(
        cluster_at < first_post,
        "the referent is read before anything is created; got {all:?}"
    );
}

/// The plan ConfigMap is owner-referenced to the `Backup`, with **both**
/// flags.
///
/// `controller: true` so garbage collection treats the `Backup` as the owner
/// and so the 409 case is decidable at all; `blockOwnerDeletion: true` so a
/// half-deleted `Backup` cannot orphan a ConfigMap naming its source cluster.
///
/// KILLS: dropping either flag; owning it from the `BackupSchedule` instead.
#[tokio::test]
async fn the_plan_config_map_is_owned_by_the_backup_with_both_flags() {
    let (_seen, bodies) = create_pass(create_routes(201, existing_plan_config_map(UID))).await;
    let cm = posted_config_map(&bodies);
    let owners = cm["metadata"]["ownerReferences"]
        .as_array()
        .expect("the ConfigMap carries owner references");
    assert_eq!(owners.len(), 1, "exactly one owner; got {owners:?}");
    let owner = &owners[0];
    assert_eq!(
        owner["kind"].as_str(),
        Some("Backup"),
        "the OWNER IS THE BACKUP, not the schedule: the plan is rendered from THIS object's spec \
         and dies with it"
    );
    assert_eq!(owner["apiVersion"].as_str(), Some("logweir.dev/v1alpha1"));
    assert_eq!(owner["name"].as_str(), Some(NAME));
    assert_eq!(owner["uid"].as_str(), Some(UID));
    assert_eq!(
        owner["controller"].as_bool(),
        Some(true),
        "`controller: true` — and it is what makes the 409 case decidable"
    );
    assert_eq!(
        owner["blockOwnerDeletion"].as_bool(),
        Some(true),
        "`blockOwnerDeletion: true` — a half-deleted `Backup` must not orphan its plan"
    );
    // The api_version and kind come from the derive's own `Resource` impl, so
    // they cannot drift from the CRD.
    let crd = emitted_backup_crd();
    assert_eq!(
        format!(
            "{}/{}",
            crd["spec"]["group"].as_str().unwrap_or_default(),
            crd["spec"]["versions"][0]["name"]
                .as_str()
                .unwrap_or_default()
        ),
        owner["apiVersion"].as_str().unwrap_or_default(),
        "the owner reference's apiVersion is the CRD's own group/version"
    );
}

/// A 409 on the ConfigMap `POST` is success **only** when the existing object
/// is ours.
///
/// The ordinary 409 is this same reconcile's previous pass, and the object is
/// byte-identical because a rendered plan is a pure function of an immutable
/// spec. A 409 on a FOREIGN object is a plan document a stranger wrote, at the
/// mount path of the pod that holds this `Backup`'s signing key.
///
/// KILLS: treating every 409 as success; keying the check on the name instead
/// of the UID.
#[tokio::test]
async fn a_conflicting_plan_config_map_is_terminal_only_when_it_is_not_ours() {
    // ARM 1 — 409, and the existing object IS ours. The Job is still created.
    let (client, seen, _bodies) =
        mock_client_recording_bodies(create_routes(409, existing_plan_config_map(UID)));
    let outcome = reconcile_backup(
        &backup(),
        &client,
        &unobserved_archive,
        &unverified_evidence,
        utc(2026, 11, 9, 3, 17),
    )
    .await
    .expect("an owned 409 is success");
    assert!(
        outcome.created,
        "a 409 from our own previous pass does not stop the Job from being created"
    );
    assert_eq!(outcome.terminal_state, None);
    {
        let seen = seen.lock().expect("the recorder is readable");
        assert!(
            seen.iter()
                .any(|r| r.method == "GET" && path(&r.uri).ends_with(&plan_config_map_name(NAME))),
            "the 409 is DECIDED by reading the existing object, not assumed; got {:?}",
            *seen
        );
    }

    // ARM 2 — 409, and the existing object belongs to a DIFFERENT `Backup`.
    let (client, seen, bodies) = mock_client_recording_bodies(create_routes(
        409,
        existing_plan_config_map("00000000-dead-4000-8000-00000000beef"),
    ));
    let outcome = reconcile_backup(
        &backup(),
        &client,
        &unobserved_archive,
        &unverified_evidence,
        utc(2026, 11, 9, 3, 17),
    )
    .await
    .expect("a refusal is an OUTCOME, never an error");
    assert_eq!(
        outcome.terminal_state.as_deref(),
        Some("PlanConfigMapConflict"),
        "a foreign plan ConfigMap is terminal"
    );
    assert!(!outcome.created, "and the Job is NOT created");
    {
        let seen = seen.lock().expect("the recorder is readable");
        assert!(
            !seen
                .iter()
                .any(|r| r.method == "POST" && path(&r.uri).ends_with("/jobs")),
            "ZERO Job `POST`s, with the route present so the assertion is what refuses; got {:?}",
            *seen
        );
    }
    let statuses = patched_statuses(&bodies.lock().expect("the body recorder is readable"));
    assert_eq!(statuses[0]["phase"].as_str(), Some("Failed"));
    assert_eq!(statuses[0]["exitReason"].as_str(), Some("operational"));
    assert_eq!(
        conditions_of(&statuses[0]),
        vec![(
            "Failed".to_string(),
            "True".to_string(),
            "PlanConfigMapConflict".to_string()
        )]
    );
}

/// `spec.sourceRef` naming a `KafkaCluster` that does not exist is
/// **terminal**, not a requeue.
///
/// `Backup.spec` is CEL-immutable, so a referent that resolves to nothing
/// resolves to nothing on every later pass; a requeue leaves the CR with an
/// empty status and no explanation, which is review finding MEDIUM-1's shape.
///
/// KILLS: requeueing on an absent referent; creating the Job anyway.
#[tokio::test]
async fn a_missing_source_cluster_is_terminal_and_not_a_requeue() {
    let mut routes = create_routes(201, existing_plan_config_map(UID));
    for r in &mut routes {
        if r.path_suffix == "/kafkaclusters/prod" {
            r.status = 404;
            r.body = not_found_body("kafkaclusters.logweir.dev", "prod");
        }
    }
    let (client, seen, bodies) = mock_client_recording_bodies(routes);
    let outcome = reconcile_backup(
        &backup(),
        &client,
        &unobserved_archive,
        &unverified_evidence,
        utc(2026, 11, 9, 3, 17),
    )
    .await
    .expect("a refusal is an OUTCOME: an error would requeue and write nothing");
    assert_eq!(
        outcome.terminal_state.as_deref(),
        Some("ReferentNotFound"),
        "the referent is missing and the answer is on the object"
    );
    assert!(!outcome.created);
    {
        let seen = seen.lock().expect("the recorder is readable");
        assert!(
            !seen.iter().any(|r| r.method == "POST"),
            "NOTHING is created — not the ConfigMap and not the Job, both routed; got {:?}",
            *seen
        );
    }
    let statuses = patched_statuses(&bodies.lock().expect("the body recorder is readable"));
    assert_eq!(statuses[0]["phase"].as_str(), Some("Failed"));
    assert_eq!(statuses[0]["exitReason"].as_str(), Some("operational"));
    assert!(statuses[0]["exitCode"].is_null(), "nothing ran");
    assert_eq!(
        conditions_of(&statuses[0]),
        vec![(
            "Failed".to_string(),
            "True".to_string(),
            "ReferentNotFound".to_string()
        )]
    );
    let message = statuses[0]["conditions"][0]["message"]
        .as_str()
        .expect("the condition carries a message");
    assert!(
        message.contains("prod") && message.contains("KafkaCluster"),
        "the message names the referent it could not find; got: {message}"
    );
}

/// A glob metacharacter in `spec.topics` is a **terminal refusal before any
/// `POST`**.
///
/// Global Constraint 18(c) rail 1 and guard **G-GLOB**: `topics` is a
/// mandatory NAMED allowlist, and `orders*` handed to the engine's own
/// selector means "every topic starting with orders" — the one shape a
/// mandatory allowlist exists to make impossible. The rail is
/// `logweir_core::guard`'s, shared and not reimplemented, so the controller and
/// the runner refuse the same six characters.
///
/// KILSS nothing subtle and everything blunt: rendering a pattern into
/// `backup.yaml` and letting the runner refuse it later would mean a Job, a
/// pod, a broker connection and an exit 3 for a fact knowable from the spec.
#[tokio::test]
async fn a_wildcard_topic_is_refused_before_any_post() {
    for bad in ["orders*", "orders?", "events[1]", "a]b", "x{1}", "y}z"] {
        let json = backup_json().replace(r#""orders", "payments""#, &format!(r#""{bad}""#));
        let b: Backup = serde_json::from_str(&json).expect("the fixture is a Backup");
        assert_eq!(
            b.spec.topics,
            vec![bad.to_string()],
            "the fixture really carries the pattern"
        );
        let (client, seen, bodies) =
            mock_client_recording_bodies(create_routes(201, existing_plan_config_map(UID)));
        let outcome = reconcile_backup(
            &b,
            &client,
            &unobserved_archive,
            &unverified_evidence,
            utc(2026, 11, 9, 3, 17),
        )
        .await
        .expect("a refusal is an OUTCOME, never an error");
        assert_eq!(
            outcome.terminal_state.as_deref(),
            Some("GuardRefused"),
            "`{bad}` is refused by the shared rail"
        );
        {
            let seen = seen.lock().expect("the recorder is readable");
            assert!(
                !seen.iter().any(|r| r.method == "POST"),
                "ZERO `POST`s for `{bad}`, with both routes present; got {:?}",
                *seen
            );
            assert!(
                !seen
                    .iter()
                    .any(|r| path(&r.uri).ends_with("/kafkaclusters/prod")),
                "and the referent is not even read: the pattern is knowable from the spec alone"
            );
        }
        let statuses = patched_statuses(&bodies.lock().expect("the body recorder is readable"));
        assert_eq!(
            conditions_of(&statuses[0]),
            vec![(
                "Failed".to_string(),
                "True".to_string(),
                "GuardRefused".to_string()
            )],
            "`{bad}`'s condition"
        );
        let message = statuses[0]["conditions"][0]["message"]
            .as_str()
            .expect("the condition carries a message");
        assert!(
            message.contains(bad),
            "the message NAMES the offending entry, so an operator does not have to diff the \
             list; got: {message}"
        );
    }

    // And a plain named list is not refused.
    let (_seen, bodies) = create_pass(create_routes(201, existing_plan_config_map(UID))).await;
    assert!(
        posted_config_map(&bodies)["data"]["backup.yaml"].is_string(),
        "`orders` and `payments` carry no metacharacter and are rendered"
    );
}

/// A `scramSha512` cluster with no `auth.username` cannot be rendered, and
/// that is terminal too.
///
/// Spec §4 — "the approval binds the identity, not just the address" — is
/// unsatisfiable with no identity to bind, and rendering `sasl_username: ""`
/// would produce a run that authenticates as nobody and a SIGNED receipt
/// saying so.
#[tokio::test]
async fn a_scram_cluster_with_no_username_is_refused() {
    let mut routes = create_routes(201, existing_plan_config_map(UID));
    for r in &mut routes {
        if r.path_suffix == "/kafkaclusters/prod" {
            r.body = kafka_cluster_json().replace(r#""username": "logweir", "#, "");
        }
    }
    let (client, seen, bodies) = mock_client_recording_bodies(routes);
    let outcome = reconcile_backup(
        &backup(),
        &client,
        &unobserved_archive,
        &unverified_evidence,
        utc(2026, 11, 9, 3, 17),
    )
    .await
    .expect("a refusal is an OUTCOME");
    assert_eq!(
        outcome.terminal_state.as_deref(),
        Some("CredentialNotRenderable"),
        "the same terminal state the RUNNER prints for the same fact, reached one step earlier"
    );
    assert!(
        !seen
            .lock()
            .expect("the recorder is readable")
            .iter()
            .any(|r| r.method == "POST"),
        "nothing is created"
    );
    let statuses = patched_statuses(&bodies.lock().expect("the body recorder is readable"));
    assert_eq!(statuses[0]["phase"].as_str(), Some("Failed"));
}

/// An `archive.url` with no object-store scheme is terminal, not a requeue.
#[tokio::test]
async fn an_unreadable_archive_url_is_refused() {
    let json = backup_json().replace("s3://kafka-backups/logweir", "kafka-backups/logweir");
    let b: Backup = serde_json::from_str(&json).expect("the fixture is a Backup");
    let (client, _seen, bodies) =
        mock_client_recording_bodies(create_routes(201, existing_plan_config_map(UID)));
    let outcome = reconcile_backup(
        &b,
        &client,
        &unobserved_archive,
        &unverified_evidence,
        utc(2026, 11, 9, 3, 17),
    )
    .await
    .expect("a refusal is an OUTCOME");
    assert_eq!(
        outcome.terminal_state.as_deref(),
        Some("ArchiveUrlUnreadable"),
        "the rendered `storage` block is TYPED, so a URL with no scheme cannot be rendered at all"
    );
    let statuses = patched_statuses(&bodies.lock().expect("the body recorder is readable"));
    assert_eq!(statuses[0]["phase"].as_str(), Some("Failed"));
    assert_eq!(statuses[0]["exitReason"].as_str(), Some("operational"));
}

// ---------------------------------------------------------------------------
// The two evidence keys
// ---------------------------------------------------------------------------

/// The keys come off the final two stdout lines — read BY KEY NAME.
///
/// KILLS: taking the keys by line position (arm 2); guessing a key from the
/// backup id when the lines are absent (arm 3).
#[tokio::test]
async fn the_runner_keys_are_read_from_the_final_two_stdout_lines() {
    // ARM 1: the contract's order.
    let (client, _seen, bodies) = mock_client_recording_bodies(finished_routes(
        &pod_list_terminated(0),
        log_body(&i7_tail()),
        200,
        "Complete",
    ));
    reconcile_backup(
        &backup(),
        &client,
        &unobserved_archive,
        &unverified_evidence,
        utc(2026, 11, 9, 3, 20),
    )
    .await
    .expect("the reconcile succeeds");
    let statuses = patched_statuses(&bodies.lock().expect("the body recorder is readable"));
    assert_eq!(
        statuses[0]["evidence"]["receiptKey"].as_str(),
        Some(RECEIPT_KEY),
        "`receipt-key=<k>` lands on `status.evidence.receiptKey`"
    );
    assert_eq!(
        statuses[0]["evidence"]["sidecarKey"].as_str(),
        Some(SIDECAR_KEY),
        "`sidecar-key=<k>` lands on `status.evidence.sidecarKey`"
    );
    assert!(
        statuses[0]["evidence"]["verification"].is_null(),
        "THIS TASK PERFORMS NO VERIFICATION — Task 24 does. The keys are recorded and \
         `evidence.verification` is left alone; got {:?}",
        statuses[0]["evidence"]
    );

    // ARM 2: the two lines REVERSED. Each key still lands in its own field.
    let reversed = format!("sidecar-key={SIDECAR_KEY}\nreceipt-key={RECEIPT_KEY}\n");
    let (client, _seen, bodies) = mock_client_recording_bodies(finished_routes(
        &pod_list_terminated(0),
        log_body(&reversed),
        200,
        "Complete",
    ));
    reconcile_backup(
        &backup(),
        &client,
        &unobserved_archive,
        &unverified_evidence,
        utc(2026, 11, 9, 3, 20),
    )
    .await
    .expect("the reconcile succeeds");
    let statuses = patched_statuses(&bodies.lock().expect("the body recorder is readable"));
    assert_eq!(
        statuses[0]["evidence"]["receiptKey"].as_str(),
        Some(RECEIPT_KEY),
        "THE KEYS ARE READ BY NAME AND NOT BY POSITION. A position reader would write the `.sig` \
         key into `receiptKey` and the `.json` key into `sidecarKey` — both of which LOOK like \
         object keys, and neither of which a verifier could then fetch"
    );
    assert_eq!(
        statuses[0]["evidence"]["sidecarKey"].as_str(),
        Some(SIDECAR_KEY),
        "and the sidecar key likewise"
    );

    // ARM 3: neither line. Both keys ABSENT, and the reason names the cause.
    let (client, _seen, bodies) = mock_client_recording_bodies(finished_routes(
        &pod_list_terminated(0),
        log_body("{\"level\":\"INFO\",\"message\":\"done\"}\n"),
        200,
        "Complete",
    ));
    reconcile_backup(
        &backup(),
        &client,
        &unobserved_archive,
        &unverified_evidence,
        utc(2026, 11, 9, 3, 20),
    )
    .await
    .expect("the reconcile succeeds");
    let statuses = patched_statuses(&bodies.lock().expect("the body recorder is readable"));
    assert!(
        statuses[0]["evidence"].is_null()
            || (statuses[0]["evidence"]["receiptKey"].is_null()
                && statuses[0]["evidence"]["sidecarKey"].is_null()),
        "NO KEY IS EVER GUESSED. A receipt key is DERIVABLE — \
         `logweir/backups/<backup_id>/<run_id>.receipt.json` — and a controller that derived one \
         would point the status at an object that may not exist, which Task 24's verifier would \
         then report as `Invalid` for a run whose evidence was merely UNREAD. Got {:?}",
        statuses[0]["evidence"]
    );
    let reasons: Vec<&str> = statuses[0]["conditions"]
        .as_array()
        .expect("conditions is an array")
        .iter()
        .filter_map(|c| c["reason"].as_str())
        .collect();
    assert!(
        reasons.contains(&"EvidenceKeysUnreadable"),
        "the condition reason `EvidenceKeysUnreadable` is how the absence is EXPLAINED rather \
         than merely present; got {reasons:?}"
    );

    // And the pure function, over the three shapes plus a trailing blank line
    // and a `\r\n`.
    let keys = evidence_keys(&format!(
        "noise\nreceipt-key={RECEIPT_KEY}\r\nsidecar-key={SIDECAR_KEY}\r\n\n"
    ));
    assert_eq!(keys.receipt.as_deref(), Some(RECEIPT_KEY));
    assert_eq!(keys.sidecar.as_deref(), Some(SIDECAR_KEY));
    assert!(keys.complete());
    let none = evidence_keys("nothing here\nor here\n");
    assert!(
        none.receipt.is_none() && none.sidecar.is_none() && !none.complete(),
        "absent is absent; got {none:?}"
    );
    let half = evidence_keys(&format!("receipt-key={RECEIPT_KEY}\n"));
    assert!(
        half.receipt.is_some() && half.sidecar.is_none() && !half.complete(),
        "one line yields one key and one absence, and neither is derived from the other; got \
         {half:?}"
    );
}

/// A `Backup` whose own name is too long is refused **before any `POST`**,
/// with a status.
///
/// REVIEW FINDING **MEDIUM-1**, plan errata **E5d**. Measured on the live
/// cluster before this fix: the API server refuses the Job
/// (`spec.template.labels: Invalid value: … must be no more than 63
/// characters`), the reconciler turned that into `BackupError::Api` ->
/// `error_policy` -> a 15-second requeue, and the `Backup` sat with
/// `status: null` — no phase, no condition, an empty `PHASE` column — forever.
///
/// KILLS: doing the length check after the `POST`; returning a `BackupError`
/// instead of writing a status; using a spelling other than Task 18's.
#[tokio::test]
async fn a_backup_whose_name_is_too_long_is_refused_before_any_post() {
    // Sixty-four characters: one past the label limit, which is the only
    // interesting length.
    let long = "b".repeat(64);
    assert_eq!(long.len(), 64);
    let json = backup_json().replace(NAME, &long);
    let b: Backup = serde_json::from_str(&json).expect("the long-named fixture is a Backup");

    // THE ROUTE TABLE CARRIES THE JOB ROUTES ANYWAY. The double panics on an
    // unrouted request, so with no `POST /jobs` route a reconciler that
    // created the Job would fail inside `testing.rs` naming a missing route —
    // a real failure, but not the one "zero POSTs" is about. Routed, the
    // zero-POST assertion is what refuses. The `GET` route's suffix is a run
    // of the fixture name's own character because `Route::path_suffix` is
    // `&'static str` and the name is built at run time.
    let routes = vec![
        Route {
            method: "GET",
            path_suffix: "bbbbbbbb",
            status: 404,
            body: not_found_body("jobs.batch", &long),
        },
        Route {
            method: "POST",
            path_suffix: "/jobs",
            status: 201,
            body: running_job_body(),
        },
        Route {
            method: "PATCH",
            path_suffix: "/status",
            status: 200,
            body: json.clone(),
        },
    ];
    let (client, seen, bodies) = mock_client_recording_bodies(routes);
    let outcome = reconcile_backup(
        &b,
        &client,
        &unobserved_archive,
        &unverified_evidence,
        utc(2026, 11, 9, 3, 17),
    )
    .await
    .expect("a refusal is an OUTCOME, never an error: an error requeues and writes nothing");

    assert_eq!(
        outcome.terminal_state.as_deref(),
        Some("NameTooLong"),
        "Task 18's spelling, because two halves of one refusal reported under two names is how \
         an operator comes to think they are two problems"
    );
    assert!(!outcome.created, "nothing was created");
    assert_eq!(outcome.exit_code, None, "nothing ran, so there is no code");

    {
        // SCOPED so the guard is not held across the second reconcile's
        // `.await` — `clippy::await_holding_lock` under `-D warnings`.
        let seen = seen.lock().expect("the recorder is readable");
        let posts: Vec<_> = seen.iter().filter(|r| r.method == "POST").collect();
        assert!(
            posts.is_empty(),
            "ZERO `POST`s: the check is BEFORE the Job is created, not after the API server \
             refuses it. Got {posts:?}"
        );
        assert!(
            !seen
                .iter()
                .any(|r| r.method == "GET" && path(&r.uri).contains("/jobs/")),
            "and before the Job is even looked up — there is nothing to look up. Got {:?}",
            *seen
        );
    }

    let statuses = patched_statuses(&bodies.lock().expect("the body recorder is readable"));
    assert_eq!(
        statuses.len(),
        1,
        "exactly one status write, and it is the terminal one"
    );
    assert_eq!(
        statuses[0]["phase"].as_str(),
        Some("Failed"),
        "TERMINAL. `status: null` forever was the defect"
    );
    assert_eq!(
        statuses[0]["exitReason"].as_str(),
        Some("operational"),
        "the run could not be attempted and no artifact was written, which is what GC11's code \
         1 means — in `exitReason`'s own vocabulary"
    );
    assert!(
        statuses[0]["exitCode"].is_null(),
        "AND NO CODE IS INVENTED: nothing ran. Got {:?}",
        statuses[0]["exitCode"]
    );
    assert_eq!(
        conditions_of(&statuses[0]),
        vec![(
            "Failed".to_string(),
            "True".to_string(),
            "NameTooLong".to_string()
        )],
        "one condition, naming the sub-case"
    );
    assert!(
        TERMINAL_STATES.contains(&"NameTooLong"),
        "`conditions::TERMINAL_STATES` gains it, so the regex test and every consumer that \
         enumerates the states sees it"
    );
    let message = statuses[0]["conditions"][0]["message"]
        .as_str()
        .expect("the condition carries a message");
    assert!(
        message.contains("64") && message.contains("63"),
        "the message names the length AND the limit, so an operator does not have to look \
         either up; got: {message}"
    );

    // A 63-character name is FINE — the boundary is `> 63`, not `>= 63`.
    let ok_name = "c".repeat(63);
    let json = backup_json().replace(NAME, &ok_name);
    let b: Backup = serde_json::from_str(&json).expect("the 63-char fixture is a Backup");
    let routes = vec![
        Route {
            method: "GET",
            path_suffix: "cccccccc",
            status: 404,
            body: not_found_body("jobs.batch", &ok_name),
        },
        Route {
            method: "GET",
            path_suffix: "/kafkaclusters/prod",
            status: 200,
            body: kafka_cluster_json(),
        },
        Route {
            method: "POST",
            path_suffix: "/configmaps",
            status: 201,
            body: existing_plan_config_map(UID),
        },
        Route {
            method: "POST",
            path_suffix: "/jobs",
            status: 201,
            body: running_job_body(),
        },
        Route {
            method: "PATCH",
            path_suffix: "/status",
            status: 200,
            body: json,
        },
    ];
    let (client, _seen, _bodies) = mock_client_recording_bodies(routes);
    let outcome = reconcile_backup(
        &b,
        &client,
        &unobserved_archive,
        &unverified_evidence,
        utc(2026, 11, 9, 3, 17),
    )
    .await
    .expect("the reconcile succeeds");
    assert!(
        outcome.created,
        "63 characters fits the label limit exactly and the Job is created"
    );
    assert_eq!(outcome.terminal_state, None);
}

/// A failed run carries **exactly ONE `Failed` condition**, and the evidence
/// fact is its own type raised only at exit 0.
///
/// REVIEW FINDING **HIGH-2**, plan errata **E5c**. Measured live before this
/// fix, every refused `Backup` came back carrying
/// `Failed/True/guard-refused` **and** `Failed/False/EvidenceKeysUnreadable`,
/// because the evidence condition was appended whenever the two key lines were
/// absent — which is ALWAYS for exits 1, 3 and 4, since GC11 says those runs
/// write no artifact. A condition array is a map keyed by `type`.
///
/// KILLS: restoring the old append (arm 1 sees two `Failed` conditions);
/// raising the evidence condition at a non-zero exit (arms 1 and 4); dropping
/// the positive arm (arm 3).
#[tokio::test]
async fn a_failed_run_carries_exactly_one_failed_condition() {
    // ARM 1 — EXIT 3, THE CASE THE DEFECT LANDED ON. A refusal, whose log
    // carries a `refusal-reason=` line and NO key lines.
    let (client, _seen, bodies) = mock_client_recording_bodies(finished_routes(
        &pod_list_terminated(3),
        log_body("guard: refused\nrefusal-reason=TargetTopicConfigRefused\n"),
        200,
        "Failed",
    ));
    reconcile_backup(
        &backup(),
        &client,
        &unobserved_archive,
        &unverified_evidence,
        utc(2026, 11, 9, 3, 20),
    )
    .await
    .expect("the reconcile succeeds");
    let statuses = patched_statuses(&bodies.lock().expect("the body recorder is readable"));
    let conditions = conditions_of(&statuses[0]);
    assert_eq!(
        conditions.len(),
        1,
        "exit 3 carries EXACTLY ONE condition; got {conditions:?}"
    );
    assert_eq!(
        conditions[0],
        (
            "Failed".to_string(),
            "True".to_string(),
            "GuardRefused".to_string()
        ),
        "and it is `Failed=True` with the code's CamelCase reason"
    );
    assert!(
        !conditions.iter().any(|(t, _, _)| t == "EvidenceRecorded"),
        "NO EVIDENCE CONDITION AT A NON-ZERO EXIT. A guard-refused run produced no evidence BY \
         CONTRACT (GC11), so `EvidenceKeysUnreadable` would be true about nothing. Got \
         {conditions:?}"
    );
    assert_eq!(
        conditions
            .iter()
            .filter(|(t, _, _)| t == "Failed")
            .map(|(_, s, _)| s.as_str())
            .collect::<Vec<_>>(),
        vec!["True"],
        "ONE `Failed`, and its status is `True` — never a second `Failed/False` alongside it"
    );
    assert_no_duplicate_condition_types(&statuses[0], "exit 3");

    // ARM 2 — EXIT 0 WITHOUT THE KEY LINES. `Complete=True` AND
    // `EvidenceRecorded=False`: two conditions, two DIFFERENT types.
    let (client, _seen, bodies) = mock_client_recording_bodies(finished_routes(
        &pod_list_terminated(0),
        log_body("{\"level\":\"INFO\",\"message\":\"done\"}\n"),
        200,
        "Complete",
    ));
    reconcile_backup(
        &backup(),
        &client,
        &unobserved_archive,
        &unverified_evidence,
        utc(2026, 11, 9, 3, 20),
    )
    .await
    .expect("the reconcile succeeds");
    let statuses = patched_statuses(&bodies.lock().expect("the body recorder is readable"));
    let conditions = conditions_of(&statuses[0]);
    assert_eq!(
        conditions,
        vec![
            ("Complete".to_string(), "True".to_string(), "Ok".to_string()),
            (
                "EvidenceRecorded".to_string(),
                "False".to_string(),
                "EvidenceKeysUnreadable".to_string()
            ),
        ],
        "exit 0 with no key lines is `Complete=True` plus `EvidenceRecorded=False`, and the \
         second condition's TYPE is what stops it from contradicting the first"
    );
    assert_no_duplicate_condition_types(&statuses[0], "exit 0, no keys");

    // ARM 3 — EXIT 0 WITH BOTH KEY LINES. `EvidenceRecorded=True` is PINNED
    // as present rather than absent: "the keys are recorded" and "nobody has
    // looked" are different answers.
    let (client, _seen, bodies) = mock_client_recording_bodies(finished_routes(
        &pod_list_terminated(0),
        log_body(&i7_tail()),
        200,
        "Complete",
    ));
    reconcile_backup(
        &backup(),
        &client,
        &unobserved_archive,
        &unverified_evidence,
        utc(2026, 11, 9, 3, 20),
    )
    .await
    .expect("the reconcile succeeds");
    let statuses = patched_statuses(&bodies.lock().expect("the body recorder is readable"));
    let conditions = conditions_of(&statuses[0]);
    assert_eq!(
        conditions,
        vec![
            ("Complete".to_string(), "True".to_string(), "Ok".to_string()),
            (
                "EvidenceRecorded".to_string(),
                "True".to_string(),
                "EvidenceKeysRecorded".to_string()
            ),
        ],
        "exit 0 with both key lines carries `EvidenceRecorded=True`"
    );
    assert_no_duplicate_condition_types(&statuses[0], "exit 0, both keys");

    // ARM 4 — EXITS 1 AND 4 CARRY ONE CONDITION EACH AND NO EVIDENCE
    // CONDITION, for the same reason as exit 3.
    for code in [1, 4] {
        let (client, _seen, bodies) = mock_client_recording_bodies(finished_routes(
            &pod_list_terminated(code),
            log_body("{\"level\":\"ERROR\",\"message\":\"boom\"}\n"),
            200,
            "Failed",
        ));
        reconcile_backup(
            &backup(),
            &client,
            &unobserved_archive,
            &unverified_evidence,
            utc(2026, 11, 9, 3, 20),
        )
        .await
        .expect("the reconcile succeeds");
        let statuses = patched_statuses(&bodies.lock().expect("the body recorder is readable"));
        let conditions = conditions_of(&statuses[0]);
        assert_eq!(
            conditions.len(),
            1,
            "exit {code} carries exactly one condition; got {conditions:?}"
        );
        assert_eq!(conditions[0].0, "Failed");
        assert_eq!(conditions[0].1, "True");
        assert_eq!(conditions[0].2, reason_for_exit(code));
        assert_no_duplicate_condition_types(&statuses[0], &format!("exit {code}"));
    }

    // AND THE CRASHED-JOB CASE, which writes its own patch: one `Failed`,
    // reason the sub-case, and no evidence condition either.
    let crashed = crashed_status_patch(&backup(), "NoExitCode", NAME, utc(2026, 11, 9, 3, 20));
    let conditions = conditions_of(&crashed["status"]);
    assert_eq!(
        conditions,
        vec![(
            "Failed".to_string(),
            "True".to_string(),
            "NoExitCode".to_string()
        )],
        "the crashed-Job case is one `Failed` condition naming the sub-case"
    );
    assert_no_duplicate_condition_types(&crashed["status"], "the crashed-Job case");
}

/// `windowCovered` is two epoch-millisecond integers **on the patched
/// status**.
///
/// KILLS: writing `windowCovered` as two RFC 3339 strings.
#[tokio::test]
async fn window_covered_is_epoch_milliseconds_on_the_status() {
    // The fixture receipt's `covered` block, in the shape `BackupReceipt`
    // writes it (Task 5): integers, with `to_ms` EXCLUSIVE.
    let receipt: Value = serde_json::from_str(
        r#"{"format_version":"1.0.0","backup_id":"b1",
            "covered":{"from_ms":1700000000000,"to_ms":1700000600000}}"#,
    )
    .expect("the fixture receipt is JSON");
    let covered = covered_from_receipt(&receipt).expect("the receipt names its window");
    // The oracle is ASYNC (interface I13: the real one's two `Store` reads
    // happen inside one `spawn_blocking`, and a synchronous `Fn` cannot
    // `.await`). `ArchiveObservation` is `Copy`, so the closure hands the same
    // observation to every call without borrowing anything.
    let oracle = move |_keys: EvidenceKeys| -> BoxFuture<'static, Option<ArchiveObservation>> {
        Box::pin(async move {
            Some(ArchiveObservation {
                receipt_sha256: None,
                presence: EvidencePresence {
                    payload: true,
                    sidecar: true,
                },
                covered: Some(covered),
            })
        })
    };

    let (client, _seen, bodies) = mock_client_recording_bodies(finished_routes(
        &pod_list_terminated(0),
        log_body(&i7_tail()),
        200,
        "Complete",
    ));
    reconcile_backup(
        &backup(),
        &client,
        &oracle,
        &unverified_evidence,
        utc(2026, 11, 9, 3, 20),
    )
    .await
    .expect("the reconcile succeeds");
    let statuses = patched_statuses(&bodies.lock().expect("the body recorder is readable"));
    let window = &statuses[0]["windowCovered"];

    assert_eq!(
        window["fromMs"].as_i64(),
        Some(1_700_000_000_000),
        "`fromMs` is the receipt's `covered.from_ms`, verbatim"
    );
    assert_eq!(
        window["toMs"].as_i64(),
        Some(1_700_000_600_000),
        "`toMs` is the receipt's `covered.to_ms`, verbatim and EXCLUSIVE"
    );
    assert!(
        window["fromMs"].is_i64() && window["toMs"].is_i64(),
        "BOTH INTEGERS, NEITHER AN RFC 3339 STRING (interface I22, critique C M4). The receipt \
         these two fields mirror carries integers, and a controller that converted between the \
         two representations is a controller that can round a window boundary. Got {window:?}"
    );
    assert!(
        !window.to_string().contains('T') && !window.to_string().contains('Z'),
        "no date-time punctuation anywhere in the rendered window; got {window}"
    );

    // AND, WITH THE RECEIPT UNREAD, THE KEY IS ABSENT RATHER THAN ZERO. An
    // absent key in a merge patch means "leave it alone", which is what stops
    // a pass whose archive handle was unavailable from overwriting a window a
    // previous pass recorded.
    let (client, _seen, bodies) = mock_client_recording_bodies(finished_routes(
        &pod_list_terminated(0),
        log_body(&i7_tail()),
        200,
        "Complete",
    ));
    reconcile_backup(
        &backup(),
        &client,
        &unobserved_archive,
        &unverified_evidence,
        utc(2026, 11, 9, 3, 20),
    )
    .await
    .expect("the reconcile succeeds");
    let statuses = patched_statuses(&bodies.lock().expect("the body recorder is readable"));
    assert!(
        statuses[0]["windowCovered"].is_null(),
        "an unobserved archive writes no window at all, and certainly not a zero one; got {:?}",
        statuses[0]["windowCovered"]
    );

    // A half-read window is no window at all.
    assert!(
        covered_from_receipt(&serde_json::json!({"covered": {"from_ms": 1}})).is_none(),
        "both keys or nothing: a consumer cannot tell `from here to unknown` from `nothing`"
    );
    assert!(
        covered_from_receipt(&serde_json::json!({})).is_none(),
        "a receipt with no `covered` block yields no window"
    );

    // And the CRD says integer, in the checked-in file the API server reads.
    let doc = yaml("config/crd/backups.yaml");
    let window_schema = &doc["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]
        ["status"]["properties"]["windowCovered"]["properties"];
    for field in ["fromMs", "toMs"] {
        assert_eq!(
            window_schema[field]["type"].as_str(),
            Some("integer"),
            "`status.windowCovered.{field}` is declared `integer` in the shipped CRD"
        );
        assert_ne!(
            window_schema[field]["format"].as_str(),
            Some("date-time"),
            "and never `format: date-time`"
        );
    }
}

/// The REAL oracle, over a real `Store`: two `get`s, one observation.
///
/// A PLAIN `#[test]`, DELIBERATELY. `Store::get` drives the store's own
/// current-thread runtime, so calling it from inside a `#[tokio::test]` panics
/// with *Cannot start a runtime from within a runtime* — which is the whole
/// reason the production call is wrapped in `tokio::task::spawn_blocking`
/// (interface **I13**), and why this test's shape mirrors the inside of that
/// closure rather than the reconcile around it.
///
/// KILLS: an oracle that derives the sidecar's presence from the receipt's, an
/// oracle that reports `payload: true` for a key it never looked up, and one
/// that treats "no keys at all" as "both absent".
#[test]
fn the_real_oracle_reads_two_objects_and_the_window_off_one_get() {
    let receipt = br#"{"format_version":"1.0.0","backup_id":"b1",
        "covered":{"from_ms":1700000000000,"to_ms":1700000600000}}"#;

    // 1. BOTH OBJECTS PRESENT. `covered` comes off the receipt's own bytes,
    //    which is the second fact the one `get` already paid for.
    let store = Store::in_memory("logweir/");
    store
        .put_create_only(RECEIPT_KEY, receipt)
        .expect("the fixture receipt is written");
    store
        .put_create_only(SIDECAR_KEY, b"dsse")
        .expect("the fixture sidecar is written");
    let keys = EvidenceKeys {
        receipt: Some(RECEIPT_KEY.to_string()),
        sidecar: Some(SIDECAR_KEY.to_string()),
    };
    let observed = observe_archive(&store, &keys).expect("both keys were looked up");
    assert_eq!(
        observed.presence,
        EvidencePresence {
            payload: true,
            sidecar: true
        }
    );
    assert_eq!(
        observed.covered,
        Some((1_700_000_000_000, 1_700_000_600_000)),
        "the window is read off the receipt this same call fetched, not off a second round trip"
    );
    assert_eq!(
        orphan_state(4, Some(observed.presence)),
        None,
        "a payload WITH its sidecar is not an orphan"
    );

    // 2. THE ASYMMETRIC CASE, which is the only reason presence is two
    //    booleans: the payload exists and the sidecar does not.
    let store = Store::in_memory("logweir/");
    store
        .put_create_only(RECEIPT_KEY, receipt)
        .expect("the fixture receipt is written");
    let observed = observe_archive(&store, &keys).expect("both keys were looked up");
    assert_eq!(
        observed.presence,
        EvidencePresence {
            payload: true,
            sidecar: false
        },
        "the two booleans are INDEPENDENT: neither is derived from the other"
    );
    assert_eq!(
        orphan_state(4, Some(observed.presence)),
        Some("OrphanedScorecard"),
        "and that is what an exit-4 orphan is"
    );

    // 3. A RECEIPT THAT IS NOT JSON is present without a window. `payload`
    //    answers "does the object exist"; `covered` answers "did it name its
    //    window", and a controller that collapsed them would report an
    //    unparseable receipt as an absent one.
    let store = Store::in_memory("logweir/");
    store
        .put_create_only(RECEIPT_KEY, b"not json at all")
        .expect("the fixture receipt is written");
    let observed = observe_archive(&store, &keys).expect("both keys were looked up");
    assert!(observed.presence.payload);
    assert_eq!(observed.covered, None);

    // 4. NO KEYS AT ALL IS `None` — NOT OBSERVED, and never "both absent".
    //    The distinction is load-bearing: `orphan_state` turns
    //    `Some(payload: true, sidecar: false)` into `OrphanedScorecard`, so a
    //    run whose log carried no key lines must not be observable as one.
    assert!(
        observe_archive(&store, &EvidenceKeys::default()).is_none(),
        "an observation with nothing to look up is NOT OBSERVED"
    );
    assert_eq!(
        orphan_state(4, None),
        None,
        "and an unobserved archive records no orphan"
    );
}

// ---------------------------------------------------------------------------
// The crashed-Job case
// ---------------------------------------------------------------------------

/// A Job that finished with no terminated state gets a TERMINAL status —
/// four sub-cases, each with `exitCode` ABSENT.
///
/// KILLS: fabricating `exitCode: 1` when no terminated state exists.
#[tokio::test]
async fn a_job_that_finished_without_a_terminated_state_gets_a_terminal_status() {
    let arms: [(&str, String, &str); 4] = [
        (
            "a disrupted node",
            pod_list_untermined(
                r#""phase":"Failed","conditions":[
                   {"type":"DisruptionTarget","status":"True","reason":"DeletionByTaintManager"}]"#,
            ),
            "DisruptedMidDrill",
        ),
        (
            "a pod that never scheduled",
            pod_list_untermined(
                r#""phase":"Pending","conditions":[
                   {"type":"PodScheduled","status":"False","reason":"Unschedulable"}]"#,
            ),
            "PodUnschedulable",
        ),
        (
            "the label selector returned zero pods",
            EMPTY_POD_LIST.to_string(),
            "NoExitCode",
        ),
        (
            "anything else",
            pod_list_untermined(r#""phase":"Failed","conditions":[]"#),
            "NoExitCode",
        ),
    ];

    for (label, pods, expected) in arms {
        // THE LOG AND THE DELETE ARE BOTH ROUTED, AND THE REASON IS THE SAME
        // REASON THE FORBIDDEN SUBRESOURCES ARE (see
        // `the_exit_code_and_the_keys_come_from_the_logs_subresource`). The
        // double panics on an unrouted request, so with no `/log` route a
        // reconciler that FABRICATED an exit code and then went to read the
        // log would fail inside `testing.rs` naming a missing route — and the
        // assertion this test exists for, "exitCode is absent", would never
        // run. Routing them means the reconciler CAN do the wrong thing and
        // the patched status is what refuses.
        let routes = vec![
            Route {
                method: "GET",
                path_suffix: "/jobs/logweir-backup-nightly-20261109-031700",
                status: 200,
                body: job_body("Failed"),
            },
            Route {
                method: "GET",
                path_suffix: "/pods",
                status: 200,
                body: pods,
            },
            Route {
                method: "GET",
                path_suffix: "/log",
                status: 200,
                body: log_body(&i7_tail()),
            },
            Route {
                method: "PATCH",
                path_suffix: "/backups/logweir-backup-nightly-20261109-031700/status",
                status: 200,
                body: backup_json(),
            },
            Route {
                method: "PATCH",
                path_suffix: "/jobs/logweir-backup-nightly-20261109-031700",
                status: 200,
                body: job_body("Failed"),
            },
            Route {
                method: "DELETE",
                path_suffix: "/jobs/logweir-backup-nightly-20261109-031700",
                status: 200,
                body: job_body("Failed"),
            },
        ];
        let (client, seen, bodies) = mock_client_recording_bodies(routes);
        let outcome = reconcile_backup(
            &backup(),
            &client,
            &unobserved_archive,
            &unverified_evidence,
            utc(2026, 11, 9, 3, 20),
        )
        .await
        .unwrap_or_else(|e| {
            panic!("{label}: the reconcile writes a status rather than erroring: {e}")
        });

        // THE ABSENCE FIRST, because it is the property this test is named
        // for and the one the "fabricate exitCode 1" mutant is filed against:
        // an earlier assertion firing first would hide it behind a message
        // about a terminal state.
        assert_eq!(
            outcome.exit_code, None,
            "{label}: there is no exit code and none is invented"
        );
        assert_eq!(
            outcome.terminal_state.as_deref(),
            Some(expected),
            "{label}: the terminal state is `{expected}`"
        );

        let statuses = patched_statuses(&bodies.lock().expect("the body recorder is readable"));
        assert_eq!(statuses.len(), 1, "{label}: one status patch");
        assert!(
            statuses[0]["exitCode"].is_null(),
            "{label}: `exitCode` IS ABSENT. A fabricated `1` is indistinguishable from a real \
             operational failure and a fabricated `0` turns a lost run into a GREEN BADGE — the \
             badge rule reads `exitCode == 0`. Got {:?}",
            statuses[0]["exitCode"]
        );
        assert_eq!(
            statuses[0]["phase"].as_str(),
            Some("Failed"),
            "{label}: NEVER LEAVE THE CR IN `Running` FOREVER. A Job that finished has finished, \
             whatever its pod did or did not report"
        );
        assert_eq!(
            statuses[0]["exitReason"].as_str(),
            Some(REASON_OPERATIONAL),
            "{label}: the GC11 classification of a run with no recoverable code is `operational` \
             — no artifact was written either — and the SUB-CASE is the condition's reason"
        );
        let reasons: Vec<&str> = statuses[0]["conditions"]
            .as_array()
            .expect("conditions is an array")
            .iter()
            .filter_map(|c| c["reason"].as_str())
            .collect();
        assert_eq!(
            reasons,
            vec![expected],
            "{label}: the condition reason is the terminal state"
        );
        assert!(
            TERMINAL_STATES.contains(&expected),
            "{label}: `{expected}` is one of the declared terminal states"
        );

        let seen = seen.lock().expect("the recorder is readable");
        assert!(
            seen.iter().all(|r| r.method != "DELETE"),
            "{label}: nothing is deleted — a failed pod is the only place its exit code exists. \
             Got {seen:?}"
        );
        assert!(
            seen.iter().all(|r| !path(&r.uri).ends_with("/log")),
            "{label}: a Job with no exit code has no evidence keys to read, so the log is not \
             fetched at all — there is nothing in it this status could use, and a fetch would \
             suggest otherwise to anyone reading the audit trail. Got {seen:?}"
        );
        assert!(
            seen.iter().all(|r| !(r.method == "PATCH"
                && path(&r.uri).contains("/jobs/")
                && !path(&r.uri).ends_with("/status"))),
            "{label}: and no TTL is patched onto a Job whose pod never reported: the one thing a \
             TTL would delete is the pod an operator now has to go and look at. Got {seen:?}"
        );
    }

    // The classifier, directly, including the zero-pods case.
    assert_eq!(
        crash_terminal_state(None),
        "NoExitCode",
        "a finished Job with ZERO pods is `NoExitCode`, a sub-case of this step and not a crash: \
         the pod was garbage-collected or never created, and the code went with it"
    );

    // And the patch shape, once, without a client.
    let patch = crashed_status_patch(
        &backup(),
        "DisruptedMidDrill",
        NAME,
        utc(2026, 11, 9, 3, 20),
    );
    assert_eq!(
        patch["status"]["conditions"][0]["message"].as_str(),
        Some(
            "the Job finished but no container named runner reported a terminated state; the \
             exit code is unrecoverable"
        ),
        "the message says what was observed, in the words the brief fixes"
    );
    assert_eq!(
        patch["status"]["jobRef"]["name"].as_str(),
        Some(NAME),
        "`jobRef` still points at the Job, so an operator can go and look"
    );
}

/// Exit 4 with a payload and no sidecar records `OrphanedScorecard`, and
/// deletes nothing.
///
/// KILLS: deleting the orphaned scorecard on exit 4.
#[tokio::test]
async fn exit_four_with_a_payload_and_no_sidecar_is_orphaned_scorecard() {
    let payload_without_sidecar =
        |_keys: EvidenceKeys| -> BoxFuture<'static, Option<ArchiveObservation>> {
            Box::pin(async {
                Some(ArchiveObservation {
                    receipt_sha256: None,
                    presence: EvidencePresence {
                        payload: true,
                        sidecar: false,
                    },
                    covered: None,
                })
            })
        };
    let (client, seen, bodies) = mock_client_recording_bodies(finished_routes(
        &pod_list_terminated(4),
        log_body(&i7_tail()),
        200,
        "Failed",
    ));
    let outcome = reconcile_backup(
        &backup(),
        &client,
        &payload_without_sidecar,
        &unverified_evidence,
        utc(2026, 11, 9, 3, 20),
    )
    .await
    .expect("the reconcile succeeds");

    assert_eq!(
        outcome.exit_code,
        Some(4),
        "exit 4 is signing or lock-proof failure, with nothing uploaded"
    );
    assert_eq!(
        outcome.terminal_state.as_deref(),
        Some("OrphanedScorecard"),
        "the terminal state is set"
    );
    let statuses = patched_statuses(&bodies.lock().expect("the body recorder is readable"));
    assert_eq!(
        statuses[0]["exitReason"].as_str(),
        Some("OrphanedScorecard"),
        "the terminal state is the most specific thing known about the run, so it is what \
         `exitReason` carries"
    );
    assert_eq!(
        statuses[0]["conditions"][0]["reason"].as_str(),
        Some(reason_for_exit(4)),
        "the condition's reason stays the CODE's reason — `SigningOrLock`, the CamelCase half of \
         the wire string `signing-or-lock` (errata E5b). The condition says what the code was; \
         `exitReason` says the most specific thing known about the run."
    );
    assert_eq!(
        reason_for_exit(4),
        "SigningOrLock",
        "and it is CamelCase, because a `metav1.Condition`'s `reason` pattern forbids `-`"
    );
    assert_ne!(
        statuses[0]["conditions"][0]["reason"].as_str(),
        Some(REASON_SIGNING_OR_LOCK),
        "the WIRE string never reaches a condition reason"
    );

    let seen = seen.lock().expect("the recorder is readable");
    let deletes: Vec<_> = seen.iter().filter(|r| r.method == "DELETE").collect();
    assert!(
        deletes.is_empty(),
        "ZERO DELETE REQUESTS. It does not delete, does not repair and does not render the orphan \
         as a result (design-operator.md:527-535): an orphaned payload is a fact about the archive \
         an operator has to decide about, and Global Constraint 6 gives no Logweir component a \
         delete capability in tag 1. Got {deletes:?}"
    );

    // The decision, directly, over the four combinations plus "not observed".
    assert_eq!(
        orphan_state(
            4,
            Some(EvidencePresence {
                payload: true,
                sidecar: false
            })
        ),
        Some("OrphanedScorecard")
    );
    for (payload, sidecar) in [(true, true), (false, true), (false, false)] {
        assert_eq!(
            orphan_state(4, Some(EvidencePresence { payload, sidecar })),
            None,
            "payload={payload} sidecar={sidecar} is not an orphan"
        );
    }
    assert_eq!(
        orphan_state(4, None),
        None,
        "NOT OBSERVED IS NEVER `ABSENT`. A controller that read an unavailable archive handle as \
         `the sidecar is missing` would put `OrphanedScorecard` on every exit-4 run in a cluster \
         with no archive credential"
    );
    for code in [0, 1, 2, 3] {
        assert_eq!(
            orphan_state(
                code,
                Some(EvidencePresence {
                    payload: true,
                    sidecar: false
                })
            ),
            None,
            "the orphan check is exit 4's alone; code {code} does not reach it"
        );
    }
}

/// Exit 3's terminal state comes off the `refusal-reason=` line, and an
/// absent line is a NAMED observation.
#[tokio::test]
async fn exit_three_takes_its_terminal_state_from_the_refusal_reason_line() {
    let (client, _seen, bodies) = mock_client_recording_bodies(finished_routes(
        &pod_list_terminated(3),
        log_body("refusal-reason=CredentialNotRenderable\n"),
        200,
        "Failed",
    ));
    reconcile_backup(
        &backup(),
        &client,
        &unobserved_archive,
        &unverified_evidence,
        utc(2026, 11, 9, 3, 20),
    )
    .await
    .expect("the reconcile succeeds");
    let statuses = patched_statuses(&bodies.lock().expect("the body recorder is readable"));
    assert_eq!(
        statuses[0]["exitReason"].as_str(),
        Some("CredentialNotRenderable"),
        "Global Constraint 11: every guard refusal prints `refusal-reason=<TerminalState>` as its \
         final stdout line, because the pod log API has NO stream selector and nothing on stderr \
         is distinguishable by a controller"
    );

    let (client, _seen, bodies) = mock_client_recording_bodies(finished_routes(
        &pod_list_terminated(3),
        log_body("{\"level\":\"ERROR\",\"message\":\"refused\"}\n"),
        200,
        "Failed",
    ));
    reconcile_backup(
        &backup(),
        &client,
        &unobserved_archive,
        &unverified_evidence,
        utc(2026, 11, 9, 3, 20),
    )
    .await
    .expect("the reconcile succeeds");
    let statuses = patched_statuses(&bodies.lock().expect("the body recorder is readable"));
    assert_eq!(
        statuses[0]["exitReason"].as_str(),
        Some("GuardRefusedUnknownReason"),
        "an exit 3 whose log carries no such line is a NAMED observation, not a shrug and not a \
         guess at which guard fired"
    );

    assert_eq!(
        refusal_state("a\nrefusal-reason=Expired\n"),
        Some("Expired".to_string())
    );
    assert_eq!(refusal_state("nothing\n"), None);
}

/// The `Running` pass patches the status and does nothing else.
#[tokio::test]
async fn a_running_job_patches_only_the_phase_and_the_job_ref() {
    let routes = vec![
        Route {
            method: "GET",
            path_suffix: "/jobs/logweir-backup-nightly-20261109-031700",
            status: 200,
            body: running_job_body(),
        },
        Route {
            method: "PATCH",
            path_suffix: "/backups/logweir-backup-nightly-20261109-031700/status",
            status: 200,
            body: backup_json(),
        },
    ];
    let (client, seen, bodies) = mock_client_recording_bodies(routes);
    let outcome = reconcile_backup(
        &backup(),
        &client,
        &unobserved_archive,
        &unverified_evidence,
        utc(2026, 11, 9, 3, 18),
    )
    .await
    .expect("the reconcile succeeds");
    assert!(!outcome.created, "the Job already existed");
    assert!(!outcome.ttl_patched, "an unfinished Job gets no TTL");

    let statuses = patched_statuses(&bodies.lock().expect("the body recorder is readable"));
    assert_eq!(
        statuses[0]["phase"].as_str(),
        Some("Running"),
        "step 2 sets `phase: Running` and `jobRef`. NOTHING ELSE — every other field is absent \
         from the merge patch, which means `leave it alone`, which is what stops a running pass \
         clobbering the fields a finished one will write"
    );
    assert_eq!(
        statuses[0]["jobRef"]["name"].as_str(),
        Some(NAME),
        "`jobRef` names the Job"
    );
    assert!(
        statuses[0]["exitCode"].is_null() && statuses[0]["evidence"].is_null(),
        "no exit code and no evidence while the run is in flight; got {:?}",
        statuses[0]
    );
    assert_eq!(
        statuses[0]["conditions"][0]["reason"].as_str(),
        Some("JobCreated"),
        "the condition is `JobCreated`"
    );

    // NO `/pods` OR `/log` ROUTE IN THE TABLE ABOVE. The double panics on an
    // unrecorded request, so a reconciler that read a log for a Job that has
    // not finished would fail here.
    let seen = seen.lock().expect("the recorder is readable");
    assert!(
        seen.iter().all(|r| !r.uri.contains("/pods")),
        "an unfinished Job's pod is not read at all; got {seen:?}"
    );
}

/// A terminal status plus a garbage-collected Job re-creates nothing.
#[tokio::test]
async fn a_finished_backup_whose_job_is_gone_is_not_re_run() {
    let mut b = backup();
    b.status = Some(serde_json::from_str(r#"{"phase":"Succeeded","exitCode":0}"#).expect("status"));
    let routes = vec![Route {
        method: "GET",
        path_suffix: "/jobs/logweir-backup-nightly-20261109-031700",
        status: 404,
        body: not_found_body("jobs.batch", NAME),
    }];
    let (client, seen) = mock_client_recording(routes);
    let outcome = reconcile_backup(
        &b,
        &client,
        &unobserved_archive,
        &unverified_evidence,
        utc(2026, 11, 10, 0, 0),
    )
    .await
    .expect("the reconcile succeeds");
    assert!(!outcome.created, "nothing is created");
    let seen = seen.lock().expect("the recorder is readable");
    assert!(
        seen.iter().all(|r| r.method == "GET"),
        "RE-CREATING THE JOB WOULD RE-RUN AN ARCHIVE CAPTURE WHOSE RECEIPT IS ALREADY SIGNED AND \
         ALREADY IN THE BUCKET. The check reads `phase` and not `exitCode`, because the \
         crashed-Job case writes a terminal phase with NO code and a check keyed on the code \
         would re-run exactly that run. Got {seen:?}"
    );
}

// ---------------------------------------------------------------------------
// The contract in the shipped files
// ---------------------------------------------------------------------------

/// The printer columns of `Backup` are the contract, in order.
///
/// KILLS: renaming a printer column.
#[test]
fn the_printer_columns_are_the_contract() {
    // BOTH SOURCES: the emitter (so a `crds/backup.rs` edit is caught here and
    // not only by the drift gate) and the checked-in file (so what the API
    // server consumes is what was asserted). See `emitted_backup_crd`.
    for (source, doc) in [
        ("the emitter", emitted_backup_crd()),
        ("config/crd/backups.yaml", yaml("config/crd/backups.yaml")),
    ] {
        let columns = doc["spec"]["versions"][0]["additionalPrinterColumns"]
            .as_sequence()
            .unwrap_or_else(|| panic!("{source} declares additionalPrinterColumns"))
            .clone();
        let names: Vec<&str> = columns
            .iter()
            .map(|c| c["name"].as_str().expect("a column has a name"))
            .collect();
        assert_eq!(
            names,
            vec!["PHASE", "EXIT", "RECORDS", "SIGNED", "AGE"],
            "{source}: `kubectl get backups` renders these five, in this order. EXIT is the \
             column Global Constraint 11 exists for: without it every non-zero exit is a generic \
             `Error` to an operator glancing at the namespace, and `your backup failed its drill` \
             looks exactly like `the drill could not run`"
        );
    }

    let doc = yaml("config/crd/backups.yaml");
    let columns = doc["spec"]["versions"][0]["additionalPrinterColumns"]
        .as_sequence()
        .expect("backups.yaml declares additionalPrinterColumns");
    let exit = columns
        .iter()
        .find(|c| c["name"].as_str() == Some("EXIT"))
        .expect("EXIT is one of them");
    assert_eq!(
        exit["jsonPath"].as_str(),
        Some(".status.exitCode"),
        "EXIT reads `.status.exitCode` — the field the green rule reads"
    );
    assert_eq!(
        exit["type"].as_str(),
        Some("integer"),
        "an exit code is an integer"
    );
}

/// The green rule reads `exitCode`, and `Backup` has no `outcome`.
///
/// KILLS: adding a `status.outcome` to `Backup` and making the badge read it.
#[test]
fn the_backup_green_rule_reads_exit_code_and_not_an_outcome() {
    // BOTH SOURCES — see `emitted_backup_crd` for why the checked-in file
    // alone is not enough.
    for (source, doc) in [
        ("the emitter", emitted_backup_crd()),
        ("config/crd/backups.yaml", yaml("config/crd/backups.yaml")),
    ] {
        let status = doc["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]
            ["status"]["properties"]
            .clone();
        let props: Vec<String> = status
            .as_mapping()
            .unwrap_or_else(|| panic!("{source}: status has properties"))
            .keys()
            .map(|k| k.as_str().expect("a property name").to_string())
            .collect();
        assert!(
            !props.iter().any(|p| p == "outcome"),
            "{source}: `Backup.status` DECLARES NO `outcome` (spec §3.2, C95). `Restore` has \
             one; adding one here would give spec §8's badge two sources of truth that a partial \
             status could disagree about — and the field it actually reads is `exitCode`. Got \
             {props:?}"
        );
        assert!(
            props.iter().any(|p| p == "exitCode"),
            "{source}: and `exitCode` is there; got {props:?}"
        );
    }

    let doc = yaml("config/crd/backups.yaml");
    let status = &doc["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["status"]
        ["properties"];
    let description = status["exitCode"]["description"]
        .as_str()
        .expect("exitCode carries a description");
    assert!(
        description.contains("green requires verification Valid and exitCode 0"),
        "`kubectl explain backup.status.exitCode` has to STATE the rule, because the rule lives \
         in two places at once (the badge and this field) and a reader of one must find the \
         other. Got: {description}"
    );
    assert!(
        description.contains("containerStatuses"),
        "and it has to name the one path in Kubernetes that carries the code, so an operator \
         debugging by hand looks in the right place. Got: {description}"
    );
}

/// The RBAC fragments request `pods/log` and nothing more.
///
/// THE REQUESTING HALF OF INTERFACE **I28**. Task 21's
/// `weirkeeper_can_read_pod_logs_and_nothing_more` asserts what the INSTALLED
/// ClusterRole grants; this asserts what the request asks for, so that task's
/// implementer has a checkable pointer rather than a sentence.
///
/// RENAMED IN THIS TASK'S FIX ROUND (errata **E5a**). It was
/// `the_rbac_fragments_request_pod_logs_and_nothing_more`; the reconciler now
/// also CREATES the plan ConfigMap the runner Job mounts and READS the
/// `KafkaCluster` it renders that plan from, so "pod logs and nothing more"
/// had stopped being what the request says. The name states the whole request
/// rather than half of it.
#[test]
fn the_rbac_fragments_request_pod_logs_the_plan_config_map_and_nothing_more() {
    let role = yaml("config/rbac/backup-reconciler-rbac-request.yaml");
    assert_eq!(
        role["kind"].as_str(),
        Some("ClusterRole"),
        "the request is written in the shape the grant will take"
    );
    let rules = role["rules"]
        .as_sequence()
        .expect("the request carries rules");
    let rule_for = |resource: &str| -> Option<Vec<String>> {
        rules
            .iter()
            .find(|r| {
                r["resources"]
                    .as_sequence()
                    .is_some_and(|rs| rs.iter().any(|x| x.as_str() == Some(resource)))
            })
            .map(|r| {
                r["verbs"]
                    .as_sequence()
                    .expect("a rule has verbs")
                    .iter()
                    .map(|v| v.as_str().expect("a verb").to_string())
                    .collect()
            })
    };

    assert_eq!(
        rule_for("pods/log"),
        Some(vec!["get".to_string()]),
        "`pods/log` IS A SUBRESOURCE AND `pods` DOES NOT COVER IT (critique B H8). A ClusterRole \
         listing only `pods` produces a controller that reads every pod's spec, cannot read one \
         line of any pod's stdout, and fails at exactly the step interface I7 exists for — with a \
         403 that looks like a transient API error. `get` only: the final two stdout lines are \
         READ, nothing is written and no process is started."
    );
    // THE ESCAPE BELOW IS NOT A WEAKENING OF `check-no-oso.sh`. Its check B
    // greps `crates/**/*.rs` for a denied ENGINE subcommand token, and one of
    // those tokens is spelled the same as the Kubernetes RBAC verb this
    // assertion is about. The gate cannot tell a `kafka-backup list` from a
    // ClusterRole verb in a string literal — nothing in a grep can — so the
    // sanctioned same-line escape with a reason is exactly the mechanism for
    // this case, and Task 18's `runner_argv` note records the mirror-image
    // situation for the permitted `backup` token.
    let list_verb = "list".to_string(); // engine-token-ok: a Kubernetes RBAC verb on pods, never the engine's denied `list` subcommand
    assert!(
        rule_for("pods").is_some_and(|v| v.contains(&list_verb)),
        "`list` on `pods` is what finds the pod carrying the exit code, by the job-name label"
    );

    // THE PLAN CONFIGMAP — `create` AND `get`, AND NOTHING ELSE. `get` is how
    // the 409 is decided (whose ConfigMap is it?); `patch`/`update` would be
    // the ability to REWRITE a plan document a running pod already mounted,
    // and `delete` is refused everywhere by Global Constraint 6.
    assert_eq!(
        rule_for("configmaps"),
        Some(vec!["create".to_string(), "get".to_string()]),
        "the runner Job mounts `<backup name>-plan` at `/plan` and Task 17 renders it in the \
         same reconcile pass, ConfigMap `POST` before Job `POST` (errata E5a). `create` writes \
         it; `get` decides the 409. NOT `patch`, because the plan is a pure function of a \
         CEL-immutable spec and a pass that could rewrite it is a pass that could change the \
         document a running pod already mounted."
    );
    for forbidden_verb in ["patch", "update", "delete", "deletecollection"] {
        assert!(
            !rule_for("configmaps").is_some_and(|v| v.contains(&forbidden_verb.to_string())),
            "the ConfigMap rule must not ask for `{forbidden_verb}`"
        );
    }

    // THE REFERENT — `get` ONLY, and no cache. The bootstrap addresses and the
    // auth block in the rendered `backup.yaml` come from here and never from
    // `Backup.spec`, which carries neither.
    assert_eq!(
        rule_for("kafkaclusters"),
        Some(vec!["get".to_string()]),
        "`spec.sourceRef` is read as ONE named object. `list`/`watch` would be a cache this \
         reconciler does not keep, and a wider read than the render needs."
    );

    let text = workspace_file("config/rbac/backup-reconciler-rbac-request.yaml");
    let requested: String = text
        .lines()
        .filter(|l| !l.trim_start().starts_with('#'))
        .collect::<Vec<_>>()
        .join("\n");
    for forbidden in ["secrets", "pods/exec", "pods/attach", "delete"] {
        assert!(
            !requested.contains(forbidden),
            "the request must not name `{forbidden}`. `secrets`: spec §9 — `weirkeeper` holds NO \
             `get` on Secrets anywhere, and the runner's key reaches its pod because KUBELET \
             projects it, a read the controller never performs. `pods/exec`/`pods/attach`: either \
             would let the controller run a process inside the pod that HOLDS THE SIGNING KEY. \
             `delete`: Global Constraint 6 gives no Logweir component a delete capability in tag \
             1, and a failed pod is the only place its exit code exists."
        );
    }

    let sa = yaml("config/rbac/backup-runner-serviceaccount.yaml");
    assert_eq!(
        sa["metadata"]["name"].as_str(),
        Some(RUNNER_SERVICE_ACCOUNT),
        "the ServiceAccount fragment names the account the Job template names"
    );
    assert_eq!(
        sa["automountServiceAccountToken"].as_bool(),
        Some(false),
        "TWO STATEMENTS OF ONE PROPERTY, and both are needed: the pod spec is what kubelet reads \
         and this line is what an operator reads. A projected token would be a credential in the \
         one pod that holds the signing key — the component design-operator.md:355-359 keeps away \
         from the cluster API."
    );
    assert!(
        !workspace_file("config/rbac/backup-runner-serviceaccount.yaml")
            .lines()
            .any(|l| !l.trim_start().starts_with('#') && l.contains("RoleBinding")),
        "there is nothing to bind: the runner account is granted no verb on any resource, and a \
         binding to an empty role is a line an operator has to audit for no benefit"
    );
}

/// The pod selectors, in order, with the exact spellings.
#[test]
fn the_pod_selectors_are_the_prefixed_label_then_the_legacy_one() {
    assert_eq!(
        pod_selectors("j1"),
        [
            "batch.kubernetes.io/job-name=j1".to_string(),
            "job-name=j1".to_string()
        ],
        "the prefixed label is the 1.27+ spelling and the current one; the legacy label is still \
         set on 1.29 and is the fallback. Order matters: a controller preferring the legacy label \
         breaks on the release that drops it."
    );
}

// ===========================================================================
// TASK 16b — THE STEADY-OBJECT ROW, plan erratum E11(d)
// ===========================================================================

/// The routes a RUNNING pass needs: the Job exists and has not finished, and
/// the status is patchable.
fn running_routes() -> Vec<Route> {
    vec![
        Route {
            method: "GET",
            path_suffix: "/jobs/logweir-backup-nightly-20261109-031700",
            status: 200,
            body: running_job_body(),
        },
        Route {
            method: "PATCH",
            path_suffix: "/backups/logweir-backup-nightly-20261109-031700/status",
            status: 200,
            body: backup_json(),
        },
    ]
}

/// How many `/status` `PATCH`es the double was asked for.
fn status_patch_count(bodies: &[SeenBody]) -> usize {
    bodies
        .iter()
        .filter(|b| b.method == "PATCH" && path(&b.uri).ends_with("/status"))
        .count()
}

/// **Task 16b.** A steady `Backup` — one whose Job is still running — is
/// patched once and then never again.
///
/// # Why the RUNNING state and not the terminal one
///
/// The terminal state was never at risk: `reconcile_backup`'s `status_is_terminal`
/// guard returns before any patch, so a finished `Backup` has always been
/// silent. The running state is the one this reconciler spends its time in,
/// and on a 15 s requeue it used to send `running_status_patch` on every
/// single pass — an identical body, so the API server bumped nothing and Task
/// 15c measured this reconciler QUIET, but a request all the same. Quiet is
/// not silent; plan erratum E11(d)'s third rule is that a reconcile which
/// computes the status the object already carries sends NOTHING, and a
/// route-table count is what can see the difference.
///
/// The second object is the first pass's own patch, applied as the API server
/// would apply it (`conditions::apply_merge_patch`, RFC 7386).
#[tokio::test]
async fn a_steady_backup_issues_no_second_status_patch() {
    let (client, _seen, bodies) = mock_client_recording_bodies(running_routes());
    reconcile_backup(
        &backup(),
        &client,
        &unobserved_archive,
        &unverified_evidence,
        utc(2026, 11, 9, 3, 20),
    )
    .await
    .expect("the first reconcile succeeds");
    let first = bodies
        .lock()
        .expect("the body recorder is readable")
        .clone();
    assert_eq!(
        status_patch_count(&first),
        1,
        "the first pass records the running Job: {first:?}"
    );

    let mut stored = Value::Null;
    apply_merge_patch(&mut stored, &patched_statuses(&first)[0]);
    let mut steady = backup();
    steady.status = Some(
        serde_json::from_value::<BackupStatus>(stored)
            .expect("the patched status is a BackupStatus — the API server stores it"),
    );

    // FOUR REQUEUES LATER, a different clock, the same still-running Job.
    let (client, _seen, bodies) = mock_client_recording_bodies(running_routes());
    reconcile_backup(
        &steady,
        &client,
        &unobserved_archive,
        &unverified_evidence,
        utc(2026, 11, 9, 3, 21),
    )
    .await
    .expect("the second reconcile succeeds");
    let second = bodies
        .lock()
        .expect("the body recorder is readable")
        .clone();
    assert_eq!(
        status_patch_count(&second),
        0,
        "the second pass over an unchanged object writes NOTHING. At REQUEUE_SECS = 15 that is \
         5,760 API writes a day per running Backup this reconciler no longer makes: {second:?}"
    );
}
