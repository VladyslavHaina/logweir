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
use weirkeeper::backup_execution::{runner_argv, ExecutionTrigger, INPUTS_SHA256_ANNOTATION};
use weirkeeper::check::pod as cpod;
use weirkeeper::conditions::apply_merge_patch;
use weirkeeper::conditions::{
    reason_for_exit, wire_reason_for_exit, CONDITION_REASONS, CONDITION_TYPES,
    REASON_DRILL_NOT_PASS, REASON_GUARD_REFUSED, REASON_OK, REASON_OPERATIONAL,
    REASON_SIGNING_OR_LOCK, TERMINAL_STATES, TERMINAL_STATE_ARCHIVE_URL_UNREADABLE,
    TERMINAL_STATE_NO_EXIT_CODE, TERMINAL_STATE_POD_OWNERSHIP_CONTESTED,
};
use weirkeeper::controllers::backup::{
    covered_from_receipt, crash_terminal_state, crashed_status_patch, evidence_keys,
    observe_archive, orphan_state, plan_backup_id, plan_config_map_name, pod_selectors,
    reconcile_backup, refusal_state, runner_job_spec, terminated_exit_code, unobserved_archive,
    ArchiveObservation, EvidenceKeys, EvidencePresence, JOB_NAME_LABEL, JOB_NAME_LABEL_LEGACY,
    RUNNER_SERVICE_ACCOUNT, SIGNING_KEY_SECRET, TTL_SECONDS_AFTER_FINISHED,
};
use weirkeeper::crds::backup::{Backup, BackupExecution, BackupStatus};
use weirkeeper::crds::LocalRef;
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

/// The runner Job's own `metadata.uid`, and the ONLY thing that makes [`POD`]
/// this run's pod — D-SEAMS **S6**, defect `SEC-PODLOG`. Named rather than
/// spelled inline because it now has to appear in two places that must agree:
/// the Job fixture the reconciler reads and the `ownerReferences` of the pod
/// it is allowed to read back.
const JOB_UID: &str = "bbbbbbbb-0000-4000-8000-0000000000b1";

/// A pod's `ownerReferences` naming `job_uid` as its **controller**.
///
/// `controller` and `kind` are parameters because the negative cases are
/// exactly "one of these three is wrong": another Job's UID, a
/// non-controller reference, or a `Job`-shaped UID on a `ReplicaSet`.
fn pod_owner_json(kind: &str, job_uid: &str, controller: bool) -> String {
    pod_owner_json_in("batch/v1", kind, job_uid, controller)
}

/// [`pod_owner_json`] with the owner's `apiVersion` as a parameter.
///
/// THE GROUP IS PART OF THE CHECK (review finding R3). `Job` is not a
/// `batch/v1`-exclusive kind — `volcano.sh/v1alpha1` ships one — so a
/// reference naming another group's `Job` must be refused like any other
/// stranger.
fn pod_owner_json_in(api_version: &str, kind: &str, job_uid: &str, controller: bool) -> String {
    format!(
        r#"[{{"apiVersion":"{api_version}","kind":"{kind}","name":"{NAME}","uid":"{job_uid}",
      "controller":{controller},"blockOwnerDeletion":true}}]"#
    )
}

/// A bare `Pod` with a name, an optional single owner reference
/// `(apiVersion, kind, uid, controller)`, and an optional `creationTimestamp`.
///
/// For the pure `check::pod` tests, where the pod needs no status at all.
fn owner_pod(
    name: &str,
    owner: Option<(&str, &str, &str, bool)>,
    created: Option<&str>,
) -> k8s_openapi::api::core::v1::Pod {
    let mut v = serde_json::json!({"metadata": {"name": name}});
    if let Some((api_version, kind, uid, controller)) = owner {
        v["metadata"]["ownerReferences"] = serde_json::json!([{
            "apiVersion": api_version, "kind": kind, "name": NAME, "uid": uid,
            "controller": controller
        }]);
    }
    if let Some(created) = created {
        v["metadata"]["creationTimestamp"] = serde_json::json!(created);
    }
    serde_json::from_value(v).expect("the fixture is a Pod")
}

/// The ordinary case: owned, by the controller reference, by [`JOB_UID`].
fn owned_by_job() -> String {
    pod_owner_json("Job", JOB_UID, true)
}

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

/// A MANUAL `Backup` as the API server would hand it over: typed spec, the
/// server-generated UID, and NO annotation. PLAT-06.1: this is the whole
/// contract a person or a client has to meet — the run identity is the UID and
/// the argv is derived.
///
/// The name keeps the shape Task 18's `scheduled_backup_name` produces —
/// `logweir-backup-<schedule>-<slot>` — because the Job is named after this
/// object VERBATIM and the 63-character `batch.kubernetes.io/job-name` cap is
/// the whole reason that ruling exists; a manual Backup may carry any name.
fn backup_json() -> String {
    format!(
        r#"{{
  "apiVersion": "logweir.dev/v1alpha1",
  "kind": "Backup",
  "metadata": {{
    "name": "{NAME}",
    "namespace": "{NS}",
    "uid": "{UID}",
    "generation": 3
  }},
  "spec": {{
    "sourceRef": {{ "name": "prod" }},
    "topics": ["orders", "payments"],
    "archive": {{ "url": "s3://kafka-backups/logweir", "secretRef": {{ "name": "logweir-s3" }} }},
    "triggeredBy": "manual",
    "deadlineSeconds": 3600
  }}
}}"#
    )
}

fn backup() -> Backup {
    serde_json::from_str(&backup_json()).expect("the fixture is a Backup")
}

/// A `Backup` as **Task 18's reconciler** creates it: owned by its
/// `BackupSchedule` with `controller: true`, naming that schedule and the slot,
/// and `triggeredBy: schedule` — the complete scheduled identity, and still no
/// annotation.
///
/// A SECOND FIXTURE RATHER THAN AN EDIT TO THE FIRST. [`backup_json`] has no
/// owner reference, which is the shape of a `Backup` created by hand or by
/// Task 26's page, and both identities are worth asserting. Built by MUTATING
/// the parsed fixture rather than by re-templating its text, so the two cannot
/// drift.
fn scheduled_backup() -> Backup {
    let mut value: Value = serde_json::from_str(&backup_json()).expect("the fixture is JSON");
    value["metadata"]["ownerReferences"] = serde_json::json!([{
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "BackupSchedule",
        "name": "nightly",
        "uid": SCHEDULE_UID,
        "controller": true,
        "blockOwnerDeletion": true,
    }]);
    value["spec"]["scheduleRef"] = serde_json::json!({ "name": "nightly" });
    value["spec"]["slot"] = serde_json::json!("20261109-031700");
    value["spec"]["triggeredBy"] = serde_json::json!("schedule");
    serde_json::from_value(value).expect("the mutated fixture is a Backup")
}

/// The inputs digest [`frozen_backup`]'s `status.execution` records and the
/// fixture Jobs carry. A label for "the same frozen inputs", not a real digest:
/// the Job-observing paths compare the two strings and never recompute them.
const FIXTURE_INPUTS_SHA256: &str =
    "sha256:f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1";

/// [`backup`] after this controller froze its inputs and created its Job:
/// `status.execution` recorded, matching the digest the fixture Jobs carry.
/// The shape every Job-observing row starts from.
fn frozen_backup() -> Backup {
    let mut b = backup();
    b.status = Some(BackupStatus {
        execution: Some(BackupExecution {
            id: UID.to_string(),
            inputs_ref: LocalRef {
                name: plan_config_map_name(NAME),
            },
            inputs_sha256: FIXTURE_INPUTS_SHA256.to_string(),
        }),
        ..BackupStatus::default()
    });
    b
}

/// The exact single owner reference a Job this controller created carries.
fn job_owner_json() -> String {
    format!(
        r#"[{{"apiVersion":"logweir.dev/v1alpha1","kind":"Backup","name":"{NAME}","uid":"{UID}","controller":true,"blockOwnerDeletion":true}}]"#
    )
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

/// The `BackupSchedule` [`scheduled_backup`] belongs to, as the API server
/// would hand it over — D1 §3.1 rule 2's referent.
fn backup_schedule_json(uid: &str) -> String {
    format!(
        r#"{{
  "apiVersion": "logweir.dev/v1alpha1",
  "kind": "BackupSchedule",
  "metadata": {{ "name": "nightly", "namespace": "{NS}", "uid": "{uid}", "generation": 7 }},
  "spec": {{
    "schedule": "0 3 * * *",
    "sourceRef": {{ "name": "prod" }},
    "topics": ["orders", "payments"],
    "archive": {{ "url": "s3://kafka-backups/logweir", "secretRef": {{ "name": "logweir-s3" }} }}
  }}
}}"#
    )
}

/// The routes a CREATE pass needs: the absent Job, the source `KafkaCluster`,
/// the `BackupSchedule` a scheduled run's identity names, the plan ConfigMap
/// `POST`, the Job `POST`, and the status patch.
///
/// `configmap_status` is the ConfigMap `POST`'s answer, so one helper serves
/// the 201 case and the two 409 cases.
///
/// THE SCHEDULE ROUTE IS UNUSED BY EVERY MANUAL ROW, and that is the point: a
/// manual `Backup` must not read a `BackupSchedule` at all (D1 §8.3), and
/// `manual_runs_never_read_a_backup_schedule` asserts the absence against this
/// very table — an answer that exists and is not asked for.
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
            path_suffix: "/backupschedules/nightly",
            status: 200,
            body: backup_schedule_json(SCHEDULE_UID),
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
    let cluster = serde_json::from_str(&kafka_cluster_json()).unwrap();
    let cm = weirkeeper::controllers::backup::plan_config_map(&backup(), &cluster).unwrap();
    let mut cm = serde_json::to_value(cm).unwrap();
    cm["metadata"]["ownerReferences"][0]["uid"] = serde_json::json!(owner_uid);
    cm.to_string()
}

/// A 404 `Status`, the shape `Api::get_opt` reads as "absent".
fn not_found_body(kind: &str, name: &str) -> String {
    format!(
        r#"{{"kind":"Status","apiVersion":"v1","status":"Failure",
  "message":"{kind} \"{name}\" not found","reason":"NotFound","code":404}}"#
    )
}

/// A finished Job, `Complete` or `Failed`, controlled by exactly this `Backup`
/// and carrying the frozen inputs digest [`frozen_backup`] records.
fn job_body(condition: &str) -> String {
    let owners = job_owner_json();
    format!(
        r#"{{"apiVersion":"batch/v1","kind":"Job",
  "metadata":{{"name":"{NAME}","namespace":"{NS}","uid":"{JOB_UID}",
    "ownerReferences":{owners},
    "annotations":{{"{INPUTS_SHA256_ANNOTATION}":"{FIXTURE_INPUTS_SHA256}"}}}},
  "spec":{{"template":{{"spec":{{"containers":[],"restartPolicy":"Never"}}}}}},
  "status":{{"conditions":[{{"type":"{condition}","status":"True",
     "lastProbeTime":"2026-11-09T03:20:00Z","lastTransitionTime":"2026-11-09T03:20:00Z"}}]}}}}"#
    )
}

/// A Job that exists and has not finished, controlled by exactly this
/// `Backup` and carrying the frozen inputs digest.
fn running_job_body() -> String {
    let owners = job_owner_json();
    format!(
        r#"{{"apiVersion":"batch/v1","kind":"Job",
  "metadata":{{"name":"{NAME}","namespace":"{NS}","uid":"{JOB_UID}",
    "ownerReferences":{owners},
    "annotations":{{"{INPUTS_SHA256_ANNOTATION}":"{FIXTURE_INPUTS_SHA256}"}}}},
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
    pod_list_terminated_owned_by(exit_code, &owned_by_job())
}

/// [`pod_list_terminated`] with the pod's `ownerReferences` as a parameter.
///
/// THE OWNER IS A PARAMETER BECAUSE IT IS THE SECURITY BOUNDARY. `"null"`
/// gives an ownerless pod, `pod_owner_json(…)` gives a wrong one, and both
/// must end at the same place as an empty list: no log read, no exit code.
fn pod_list_terminated_owned_by(exit_code: i32, owners: &str) -> String {
    format!(
        r#"{{"apiVersion":"v1","kind":"PodList","metadata":{{}},"items":[
  {{"apiVersion":"v1","kind":"Pod",
    "metadata":{{"name":"{POD}","namespace":"{NS}","ownerReferences":{owners},
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

/// A pod list holding one owned pod whose `runner` container is RUNNING —
/// what D3 §2.3's derivation sees on the ordinary in-flight pass.
///
/// The sidecar stays at index 0 for [`pod_list_terminated`]'s reason: a
/// fixture whose `runner` is first cannot tell a by-name reader from a
/// by-index one.
fn pod_list_running() -> String {
    let owners = owned_by_job();
    format!(
        r#"{{"apiVersion":"v1","kind":"PodList","metadata":{{}},"items":[
  {{"apiVersion":"v1","kind":"Pod",
    "metadata":{{"name":"{POD}","namespace":"{NS}","ownerReferences":{owners},
      "creationTimestamp":"2026-11-09T03:17:00Z",
      "labels":{{"{JOB_NAME_LABEL}":"{NAME}","{JOB_NAME_LABEL_LEGACY}":"{NAME}"}}}},
    "spec":{{"containers":[]}},
    "status":{{"phase":"Running",
      "conditions":[{{"type":"PodScheduled","status":"True",
        "lastTransitionTime":"2026-11-09T03:17:05Z"}}],
      "containerStatuses":[
      {{"name":"log-shipper","ready":true,"restartCount":0,"image":"x","imageID":"x",
        "state":{{"running":{{"startedAt":"2026-11-09T03:17:10Z"}}}}}},
      {{"name":"runner","ready":true,"restartCount":0,"image":"x","imageID":"x",
        "state":{{"running":{{"startedAt":"2026-11-09T03:17:12Z"}}}}}}
    ]}}}}]}}"#
    )
}

/// A pod list holding one owned pod whose `runner` container is WAITING with
/// `reason`/`message` — the kubelet's own words, which D2's classification
/// table turns into a code.
fn pod_list_waiting(reason: &str, message: &str) -> String {
    let owners = owned_by_job();
    format!(
        r#"{{"apiVersion":"v1","kind":"PodList","metadata":{{}},"items":[
  {{"apiVersion":"v1","kind":"Pod",
    "metadata":{{"name":"{POD}","namespace":"{NS}","ownerReferences":{owners},
      "creationTimestamp":"2026-11-09T03:17:00Z",
      "labels":{{"{JOB_NAME_LABEL}":"{NAME}","{JOB_NAME_LABEL_LEGACY}":"{NAME}"}}}},
    "spec":{{"containers":[]}},
    "status":{{"phase":"Pending",
      "conditions":[{{"type":"PodScheduled","status":"True",
        "lastTransitionTime":"2026-11-09T03:17:05Z"}}],
      "containerStatuses":[
      {{"name":"log-shipper","ready":false,"restartCount":0,"image":"x","imageID":"x",
        "state":{{"running":{{"startedAt":"2026-11-09T03:17:10Z"}}}}}},
      {{"name":"runner","ready":false,"restartCount":0,"image":"x","imageID":"x",
        "state":{{"waiting":{{"reason":"{reason}","message":"{message}"}}}}}}
    ]}}}}]}}"#
    )
}

/// The progress channel a v2 runner prints at the top of its log — D3 §2.4.
fn progress_log(phase: &str) -> String {
    format!("progress-contract=2\nprogress-phase=-1:admit\nprogress-phase={phase}\n")
}

/// An empty pod list — the "the selector matched nothing" answer.
const EMPTY_POD_LIST: &str = r#"{"apiVersion":"v1","kind":"PodList","metadata":{},"items":[]}"#;

/// A pod list holding one pod with NO terminated state, plus whatever
/// `status_extra` says about why.
fn pod_list_untermined(status_extra: &str) -> String {
    let owners = owned_by_job();
    format!(
        r#"{{"apiVersion":"v1","kind":"PodList","metadata":{{}},"items":[
  {{"apiVersion":"v1","kind":"Pod",
    "metadata":{{"name":"{POD}","namespace":"{NS}","ownerReferences":{owners},
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
    let spec = runner_job_spec(
        &backup(),
        &serde_json::from_str(&kafka_cluster_json()).unwrap(),
    )
    .expect("the fixture yields a spec");
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
    let mut spec = runner_job_spec(
        &backup(),
        &serde_json::from_str(&kafka_cluster_json()).unwrap(),
    )
    .expect("the fixture yields a spec");
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

/// `job::configured_runner_pull_policy`: the eight answers, two of which are
/// refusals — Task 37.
///
/// THE EMPTY STRING IS UNSET, for plan erratum **E19(e)**'s reason: a
/// Kubernetes `env:` entry with an empty `value:` makes `env::var` return
/// `Ok("")`, and a controller that read that as a configured value would put
/// `imagePullPolicy: ""` in every Job it created — which the API server
/// rejects at CREATE.
///
/// AND A VALUE THAT IS NOT A POLICY IS AN `Err`, WHICH IS THE ONE PLACE THIS
/// PREDICATE DIFFERS FROM `configured_runner_image`. An image reference cannot
/// be validated here (only a kubelet can say whether one resolves on a node);
/// a pull policy's legal set is closed, is known here, and is validated by the
/// API server at every Job CREATE. Accepting `always` would mean an install
/// that silently created rejected Jobs forever.
///
/// KILLS: a predicate that lower-cases, title-cases or otherwise "helps" the
/// value through; a predicate that falls back to the constant instead of
/// refusing; an empty value read as configured.
#[test]
fn an_empty_runner_pull_policy_override_is_unset() {
    assert_eq!(
        job::configured_runner_pull_policy(Ok(String::new())),
        Ok(None),
        "an empty `value:` on the Deployment's env entry is the variable being UNSET (E19(e)); \
         read as configured it puts `imagePullPolicy: \"\"` in every Job this controller creates"
    );
    assert_eq!(
        job::configured_runner_pull_policy(Ok("  ".to_string())),
        Ok(None),
        "whitespace is the same fact as empty"
    );
    assert_eq!(
        job::configured_runner_pull_policy(Err(std::env::VarError::NotPresent)),
        Ok(None),
        "an absent variable is unset — and unset is the compiled-in `job::IMAGE_PULL_POLICY`, \
         which is what every path that LOADS an image onto the node still wants"
    );
    for policy in job::PULL_POLICIES {
        assert_eq!(
            job::configured_runner_pull_policy(Ok(policy.to_string())),
            Ok(Some(policy.to_string())),
            "`{policy}` is one of the three Kubernetes accepts"
        );
    }
    assert_eq!(
        job::configured_runner_pull_policy(Ok("  Always\n".to_string())),
        Ok(Some("Always".to_string())),
        "trimmed, so a YAML block scalar's trailing newline is not part of the policy"
    );
    for wrong in ["always", "Sometimes", "ALWAYS", "IfNotpresent", "never"] {
        let refused = job::configured_runner_pull_policy(Ok(wrong.to_string()));
        let message = refused.expect_err(&format!(
            "`{wrong}` is not an imagePullPolicy and must be refused, not coerced"
        ));
        assert!(
            message.contains(job::RUNNER_PULL_POLICY_ENV),
            "the refusal must name the environment variable — an operator reading a crashed \
             controller's last line has nothing else to go on: {message}"
        );
        assert!(
            message.contains(wrong),
            "and the value it refused: {message}"
        );
    }
    assert_eq!(
        job::PULL_POLICIES,
        ["Never", "IfNotPresent", "Always"],
        "the closed set is Kubernetes' own three, spelt Kubernetes' way"
    );
    assert_eq!(
        job::RUNNER_PULL_POLICY_ENV,
        "LOGWEIR_RUNNER_PULL_POLICY",
        "the variable the chart renders beside the image one"
    );
}

/// With a pull-policy override the Job carries it — and **nothing else in the
/// Job moves**, the image least of all.
///
/// THE ASSERTION IS A WHOLE-OBJECT DIFF, not a field read. The two Jobs are
/// serialised and compared after the one field this override is allowed to
/// change is set to the same value on both; anything else `build` did
/// differently shows up as a JSON inequality naming itself. That is the
/// property the ruling asks for — "otherwise byte-identical to the default
/// Job" — rather than a spot check that a future edit could walk around.
///
/// KILLS: a `build` that ignores `RunnerJobSpec::image_pull_policy`; a `build`
/// that lets the policy override move the image (or the argv, the volumes, the
/// failure policy, the security context, the deadline …).
#[test]
fn a_runner_pull_policy_override_moves_only_the_policy() {
    let default_spec = runner_job_spec(
        &backup(),
        &serde_json::from_str(&kafka_cluster_json()).unwrap(),
    )
    .expect("the fixture yields a spec");
    assert_eq!(
        default_spec.image_pull_policy, None,
        "`runner_job_spec` is a pure function of the custom resource and builds NO pull policy: \
         the override is a property of the process, read once in `main`"
    );
    let mut overridden = default_spec.clone();
    overridden.image_pull_policy = Some("Always".to_string());

    let default_job = job::build(&default_spec);
    let overridden_job = job::build(&overridden);

    let container_of = |j: &k8s_openapi::api::batch::v1::Job| {
        j.spec
            .as_ref()
            .and_then(|s| s.template.spec.as_ref())
            .and_then(|p| p.containers.first())
            .expect("the pod template has the one runner container")
            .clone()
    };
    assert_eq!(
        container_of(&default_job).image_pull_policy.as_deref(),
        Some(job::IMAGE_PULL_POLICY),
        "with no override the policy is the compiled-in constant, exactly as before Task 37"
    );
    assert_eq!(
        container_of(&overridden_job).image_pull_policy.as_deref(),
        Some("Always"),
        "with an override it is the operator's value — the whole point of \
         `job::RUNNER_PULL_POLICY_ENV`, and what `kubectl get job -o yaml` shows"
    );
    assert_eq!(
        container_of(&overridden_job).image.as_deref(),
        Some(RUNNER_IMAGE),
        "and the POLICY override does not touch the IMAGE: the two are separate decisions, and \
         this is the mirror of `the_runner_image_override_does_not_touch_the_pull_policy`"
    );

    // The whole-object diff: normalise the one permitted difference away and
    // require byte equality of everything else.
    let mut normalised: Value =
        serde_json::to_value(&overridden_job).expect("the Job serialises to JSON");
    normalised["spec"]["template"]["spec"]["containers"][0]["imagePullPolicy"] =
        Value::String(job::IMAGE_PULL_POLICY.to_string());
    assert_eq!(
        serde_json::to_value(&default_job).expect("the Job serialises to JSON"),
        normalised,
        "the pull-policy override changes `imagePullPolicy` and NOTHING else in the Job"
    );
}

/// The Job's name is the CR's name, with no prefix added.
#[test]
fn the_job_name_is_the_cr_name() {
    let spec = runner_job_spec(
        &backup(),
        &serde_json::from_str(&kafka_cluster_json()).unwrap(),
    )
    .expect("the fixture yields a spec");
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

/// The argv is DERIVED — PLAT-06.1 — from `spec.triggeredBy` and the
/// server-generated run identity, and no annotation contributes to it: not a
/// missing one, not a malformed one, and not a hostile one naming another
/// subcommand, spec path, signing key or backup id.
///
/// KILLS: reading `logweir.dev/runner-argv` again (any arm); deriving the
/// manual identity from anything but the object's UID; deriving the scheduled
/// identity from anything but the schedule UID and the slot; dropping the
/// backup id override flag, which is what makes a re-created Job reuse its
/// run's backup id.
#[test]
fn the_argv_is_derived_and_no_annotation_contributes_to_it() {
    let cluster = serde_json::from_str(&kafka_cluster_json()).unwrap();

    // MANUAL: the object's own UID, triggered by `manual`.
    let manual = runner_job_spec(&backup(), &cluster).expect("a manual Backup yields a spec");
    assert_eq!(
        manual.args,
        runner_argv(ExecutionTrigger::Manual, UID),
        "a manual Backup with NO annotation runs: its argv is derived, not read"
    );
    let at = manual
        .args
        .iter()
        .position(|a| a == "--backup-id-override")
        .expect("the derived argv states the executed identity");
    assert_eq!(manual.args[at + 1], UID, "a manual run IS its UID");
    let at = manual
        .args
        .iter()
        .position(|a| a == "--triggered-by")
        .expect("the derived argv names the trigger");
    assert_eq!(manual.args[at + 1], "manual");
    assert_eq!(
        manual.args.first().map(String::as_str),
        Some("backup"),
        "the leading token names the `logweir` subcommand, not the engine's"
    );

    // SCHEDULED: the schedule UID and the slot, triggered by `schedule`.
    let scheduled =
        runner_job_spec(&scheduled_backup(), &cluster).expect("a scheduled Backup yields a spec");
    assert_eq!(
        scheduled.args,
        runner_argv(
            ExecutionTrigger::Schedule,
            &weirkeeper::slot::backup_id_for(SCHEDULE_UID, "20261109-031700")
        )
    );

    // `job::build` puts the derived argv on the container unchanged.
    let job = job::build(&manual);
    let args = job.spec.as_ref().and_then(|s| {
        s.template
            .spec
            .as_ref()
            .and_then(|p| p.containers.first())
            .and_then(|c| c.args.clone())
    });
    assert_eq!(args.as_ref(), Some(&manual.args));

    // AND NO ANNOTATION CHANGES ANY OF IT.
    for hostile in [
        "not json at all",
        r#"{"argv":"backup"}"#,
        r#"["restore","run","--spec","/plan/restore.yaml"]"#,
        r#"["backup","run","--spec","/tmp/attacker.yaml","--allowed-clusters","/plan/allowed-clusters.json","--signing-key","/signing/key.pem","--receipt-out","/work/receipt.json","--triggered-by","manual","--backup-id-override","attacker"]"#,
        r#"["backup","run","--spec","/plan/backup.yaml","--allowed-clusters","/plan/allowed-clusters.json","--signing-key","/tmp/other-key.pem","--receipt-out","/work/receipt.json","--triggered-by","schedule","--backup-id-override","9c4d2e6f-0000-4000-8000-0000000000d1-20261109-031700"]"#,
    ] {
        for (label, base, expected) in [
            ("manual", backup(), &manual.args),
            ("scheduled", scheduled_backup(), &scheduled.args),
        ] {
            let mut annotated = base;
            annotated
                .metadata
                .annotations
                .get_or_insert_with(Default::default)
                .insert(
                    weirkeeper::backup_execution::RUNNER_ARGV_ANNOTATION.to_string(),
                    hostile.to_string(),
                );
            let spec = runner_job_spec(&annotated, &cluster)
                .expect("an annotation never makes a runnable Backup unrunnable");
            assert_eq!(
                &spec.args, expected,
                "{label}: the annotation {hostile:?} changed the executed argv"
            );
            assert!(
                !spec.args.iter().any(|a| hostile.contains(a.as_str())
                    && ["/tmp/attacker.yaml", "/tmp/other-key.pem", "attacker"]
                        .contains(&a.as_str())),
                "{label}: a token only the annotation names reached the argv"
            );
        }
    }
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

    let spec = runner_job_spec(
        &backup(),
        &serde_json::from_str(&kafka_cluster_json()).unwrap(),
    )
    .expect("the fixture yields a spec");
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
        &frozen_backup(),
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
        &frozen_backup(),
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

/// A Job with NO `metadata.uid` adopts nothing — and does not even list.
///
/// A Job the API server returned always carries one; `metadata.uid` is
/// `Option` in the type, not in reality. The point is what the absent case
/// falls back to: nothing can be proved to belong to an object with no
/// identity, so the answer is "no pod", not "the label will do".
///
/// THE ROUTE TABLE IS EMPTY, WHICH IS THE ASSERTION. The double refuses every
/// request it has no route for, so an `Ok(None)` here is only reachable by a
/// code path that made no request at all.
///
/// KILLS: falling back to the label when the UID is absent; listing first and
/// deciding afterwards.
#[tokio::test]
async fn a_job_with_no_uid_adopts_nothing_and_lists_nothing() {
    let (client, seen) = mock_client_recording(vec![]);
    let found = cpod::find_owned_pod_by_selectors(&client, NS, NAME, None, &pod_selectors(NAME))
        .await
        .expect("a Job with no UID is an answer, not an error");
    assert!(found.pod.is_none(), "nothing is adopted");
    assert!(
        found.contested.is_empty(),
        "and there is no contest to report either — nothing was listed"
    );
    assert!(
        seen.lock().expect("the recorder is readable").is_empty(),
        "and NO request was made — the pod list is not read at all, because no listing could \
         answer the question"
    );
}

/// A pod wearing this Job's name label but not owned by it is NEVER read —
/// D-SEAMS **S6**, defect `SEC-PODLOG`.
///
/// `batch.kubernetes.io/job-name` is a plain label. Anything that can create a
/// pod in the namespace can set it, and the reconciler then lifts that pod's
/// `state.terminated.exitCode` and the last two lines of its stdout onto the
/// `Backup` — `status.exitCode`, the `Complete` condition, and interface
/// **I7**'s two evidence keys, which is what the UI renders and what the
/// retention and verification paths address the archive by. So the property is
/// an ABSENCE: no `GET …/pods/<p>/log` at all, and a terminal `NoExitCode`
/// rather than a borrowed success.
///
/// Every impostor here carries `exitCode: 0` on a `runner` container, so a
/// reconciler that read it would report `phase: Succeeded` with a GREEN badge —
/// the loudest possible difference from the expected `Failed`/`NoExitCode`.
///
/// KILLS: the label-only `list.items.into_iter().next()`; the ownerless-pod
/// fallback (`plat06` review L2); matching an owner reference without
/// `controller: true`; matching an owner reference of any kind.
#[tokio::test]
async fn a_labelled_pod_this_job_does_not_own_is_never_read() {
    for (label, owners) in [
        (
            "an ownerless pod — the shape a `kubectl run` with the label produces, and the one \
             the previous code ADOPTED",
            "null".to_string(),
        ),
        (
            "a pod owned by a DIFFERENT Job's UID — a re-created Job's predecessor, or another \
             tenant's run",
            pod_owner_json("Job", "cccccccc-0000-4000-8000-0000000000c9", true),
        ),
        (
            "a NON-controller owner reference carrying the right UID — an association anybody \
             with pod-create can add to their own pod",
            pod_owner_json("Job", JOB_UID, false),
        ),
        (
            "a ReplicaSet owner reference carrying the Job's UID string",
            pod_owner_json("ReplicaSet", JOB_UID, true),
        ),
        (
            "a `Job` in ANOTHER API GROUP carrying the Job's UID string — `Job` is not a \
             batch/v1-exclusive kind (review finding R3)",
            pod_owner_json_in("volcano.sh/v1alpha1", "Job", JOB_UID, true),
        ),
    ] {
        let (client, seen, bodies) = mock_client_recording_bodies(finished_routes(
            &pod_list_terminated_owned_by(0, &owners),
            log_body(&i7_tail()),
            200,
            "Complete",
        ));
        let outcome = reconcile_backup(
            &frozen_backup(),
            &client,
            &unobserved_archive,
            &unverified_evidence,
            utc(2026, 11, 9, 3, 20),
        )
        .await
        .unwrap_or_else(|e| panic!("{label}: an unowned pod is a status, not an error: {e}"));

        let seen = seen.lock().expect("the recorder is readable");
        assert!(
            seen.iter().all(|r| !path(&r.uri).ends_with("/log")),
            "{label}: the log of a pod this Job does not own is NOT FETCHED. A `pods/log` read \
             is the whole attack: its last two lines become this run's evidence keys. Got {seen:?}"
        );
        assert_eq!(
            outcome.exit_code, None,
            "{label}: and no exit code is taken from it"
        );
        assert_eq!(
            outcome.terminal_state.as_deref(),
            Some(TERMINAL_STATE_NO_EXIT_CODE),
            "{label}: zero OWNED pods is the same state as zero pods — `NoExitCode`, the branch \
             that already exists for a garbage-collected pod"
        );
        assert_eq!(
            outcome.keys,
            EvidenceKeys::default(),
            "{label}: no evidence key is recorded from a stranger's stdout"
        );
        let statuses = patched_statuses(&bodies.lock().expect("the body recorder is readable"));
        assert_eq!(statuses.len(), 1, "{label}: one status patch");
        assert!(
            statuses[0]["exitCode"].is_null(),
            "{label}: `exitCode` is ABSENT on the object, not `0`. Got {:?}",
            statuses[0]["exitCode"]
        );
        assert_eq!(
            statuses[0]["phase"].as_str(),
            Some("Failed"),
            "{label}: and the phase is not `Succeeded`"
        );
    }
}

/// The positive control, and the contest: an OWNED pod IS read, and two pods
/// claiming one Job are BOTH refused under their own terminal state — review
/// finding **R1**.
///
/// Without the first arm the guard is satisfiable by a reconciler that never
/// reads any pod at all. Without the second, the residual the owner check
/// leaves open — an `ownerReference` is author-written, so a tenant who can
/// read the Job UID can mint a second claimant, and it is by construction the
/// NEWER one — is decided in the planter's favour.
///
/// KILLS: refusing every pod; newest-wins restored over a contested Job;
/// oldest-wins; reporting a contest as an ordinary `NoExitCode`.
#[tokio::test]
async fn an_owned_pod_is_read_and_two_claimants_are_refused() {
    // ARM 1 — the ordinary case, end to end.
    let (client, seen) = mock_client_recording(finished_routes(
        &pod_list_terminated(0),
        log_body(&i7_tail()),
        200,
        "Complete",
    ));
    let outcome = reconcile_backup(
        &frozen_backup(),
        &client,
        &unobserved_archive,
        &unverified_evidence,
        utc(2026, 11, 9, 3, 20),
    )
    .await
    .expect("the reconcile succeeds");
    assert_eq!(
        outcome.exit_code,
        Some(0),
        "the pod this Job's UID owns IS read — the guard refuses impostors, not everything"
    );
    assert_eq!(
        outcome.keys.receipt.as_deref(),
        Some(RECEIPT_KEY),
        "and its stdout is where the evidence keys come from"
    );
    let logs: Vec<String> = seen
        .lock()
        .expect("the recorder is readable")
        .iter()
        .filter(|r| path(&r.uri).ends_with("/log"))
        .map(|r| r.uri.clone())
        .collect();
    assert_eq!(logs.len(), 1, "exactly one log read; got {logs:?}");
    assert!(
        path(&logs[0]).contains(&format!("/pods/{POD}/log")),
        "from the owned pod, by name; got {logs:?}"
    );

    // ARM 2 — a forged second claimant. It carries the Job's real UID, a real
    // controller reference and the real `batch/v1` group, because that is all
    // an `ownerReferences` entry is: metadata its author writes, which the API
    // server does not validate. It reports SUCCESS and it is the NEWER pod, so
    // a newest-wins rule would put its exit code and its evidence keys on this
    // `Backup`.
    let owners = owned_by_job();
    let claimant = |suffix: &str, created: &str, exit: i32| {
        format!(
            r#"{{"apiVersion":"v1","kind":"Pod",
    "metadata":{{"name":"{POD}-{suffix}","namespace":"{NS}","ownerReferences":{owners},
      "creationTimestamp":"{created}",
      "labels":{{"{JOB_NAME_LABEL}":"{NAME}"}}}},
    "spec":{{"containers":[]}},
    "status":{{"phase":"Failed","containerStatuses":[
      {{"name":"runner","ready":false,"restartCount":0,"image":"x","imageID":"x",
        "state":{{"terminated":{{"exitCode":{exit},"finishedAt":"2026-11-09T03:19:00Z"}}}}}}]}}}}"#
        )
    };
    let genuine = claimant("genuine", "2026-11-09T03:17:00Z", 1);
    let forged = claimant("forged", "2026-11-09T03:19:00Z", 0);
    for (label, items) in [
        ("forged last", format!("[{genuine},{forged}]")),
        ("forged first", format!("[{forged},{genuine}]")),
    ] {
        let list =
            format!(r#"{{"apiVersion":"v1","kind":"PodList","metadata":{{}},"items":{items}}}"#);
        let (client, seen, bodies) = mock_client_recording_bodies(finished_routes(
            &list,
            log_body(&i7_tail()),
            200,
            "Complete",
        ));
        let outcome = reconcile_backup(
            &frozen_backup(),
            &client,
            &unobserved_archive,
            &unverified_evidence,
            utc(2026, 11, 9, 3, 20),
        )
        .await
        .unwrap_or_else(|e| panic!("{label}: a contested Job is a status, not an error: {e}"));

        let seen = seen.lock().expect("the recorder is readable");
        assert!(
            seen.iter().all(|r| !path(&r.uri).ends_with("/log")),
            "{label}: NEITHER claimant's log is read. The forged pod is the newer one by \
             construction, so ranking them hands the read to whoever planted it. Got {seen:?}"
        );
        assert_eq!(
            outcome.exit_code, None,
            "{label}: and neither claimant's exit code is taken — not the forged `0`, and not \
             the genuine `1` either, because which is which is what this controller cannot tell"
        );
        assert_eq!(
            outcome.terminal_state.as_deref(),
            Some(TERMINAL_STATE_POD_OWNERSHIP_CONTESTED),
            "{label}: under its OWN name. `NoExitCode` would send an operator looking for a \
             deleted pod, and there are two"
        );
        assert_eq!(
            outcome.keys,
            EvidenceKeys::default(),
            "{label}: no evidence key from either"
        );
        let statuses = patched_statuses(&bodies.lock().expect("the body recorder is readable"));
        assert_eq!(statuses.len(), 1, "{label}: one status patch");
        assert!(
            statuses[0]["exitCode"].is_null(),
            "{label}: `exitCode` absent, not `0`. Got {:?}",
            statuses[0]["exitCode"]
        );
        assert_eq!(
            statuses[0]["phase"].as_str(),
            Some("Failed"),
            "{label}: and the phase is not `Succeeded`"
        );
        let reasons: Vec<&str> = statuses[0]["conditions"]
            .as_array()
            .expect("conditions is an array")
            .iter()
            .filter_map(|c| c["reason"].as_str())
            .collect();
        assert_eq!(
            reasons,
            vec![TERMINAL_STATE_POD_OWNERSHIP_CONTESTED],
            "{label}: the condition names the contest"
        );
        assert!(
            TERMINAL_STATES.contains(&TERMINAL_STATE_POD_OWNERSHIP_CONTESTED),
            "{label}: and it is in the one closed list the metav1-reason test walks"
        );
    }
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
        &frozen_backup(),
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
        &frozen_backup(),
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
        &frozen_backup(),
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
        40,
        "the forty terminal states that are NOT an exit code — the original ten, plus \
         `NameTooLong` (errata E5d) and `ReferentNotFound` / `PlanConfigMapConflict` / \
         `ApprovalBundleConflict` / `ApprovalSubjectMismatch` / `JobNameConflict` / \
         `ArchiveUrlUnreadable` (errata E5a), plus `PlanHashMismatch` / `ClusterNotReachable` \
         (Task 20's `Restore` admission, and note that its third admission reason \
         `ApprovalNotVerified` is deliberately NOT here — it is a thirty-second HOLD under \
         interface I19, so it lives in `CONDITION_REASONS`), plus `ExecutionSpecInvalid` \
         (PLAT-06.1: a typed spec that states no runnable identity), plus PLAT-07.1's four \
         saved-connection refusals `ConnectionConfigInvalid` / `ConnectionReferenceInvalid` / \
         `ConnectionFieldUnsupported` / `ConnectionPlanMismatch`, plus D1 §3.4's ten — the \
         three identity refusals `ScheduledIdentityMismatch` / `ScheduleNotFound` / \
         `RunPolicyDigestMismatch` and the seven selection and discovery refusals \
         `InvalidTopicSelection` / `DiscoveryFailed` / `DiscoveryIncomplete` / \
         `DiscoveryResultUnreadable` / `SelectionEmpty` / `SelectionTooLarge` / \
         `SourceChangedDuringResolution`, of which only `DiscoveryFailed` is retryable, plus \
         `PodOwnershipContested` (D-SEAMS S6 / `SEC-PODLOG` review finding R1: more than one pod \
         claimed this run's Job, so none of them was read — NOT a sub-case of `NoExitCode`, \
         because a contested Job is a namespace to look at and a garbage-collected pod is not, \
         and deliberately not retryable because a retry invites the same second claimant), \
         plus D3 §2.2's four — `VolumeMountFailed` / `CredentialReferenceMissing` / \
         `RunnerImageUnavailable` / `PodCreationForbidden`, the `RunnerReady=False` reasons that \
         cannot get better on their own. Each REPLACES `NoExitCode` only when the matching \
         diagnostic was recorded before the Job ended, so `crash_terminal_state`'s existing \
         table is unchanged and `exitCode` stays absent in all four, plus D3 §5.5 step 6's \
         `PointBindingMismatch` — W5's hand-off, DECLARED here and not yet produced by any \
         runner (see the constant's note for the two `logweir-core`/`logweir` edits that make \
         it reachable); \
         got {TERMINAL_STATES:?}"
    );
    // D3 §2.2's four are the `RunnerReady` PROJECTION of a diagnosis and not
    // its raw code, which is why `SigningKeyMissing` — a real
    // `diagnostics::Code` — is deliberately absent from this list.
    for state in [
        "VolumeMountFailed",
        "CredentialReferenceMissing",
        "RunnerImageUnavailable",
        "PodCreationForbidden",
    ] {
        assert!(
            TERMINAL_STATES.contains(&state),
            "`{state}` is what a fail-fast cancellation's crashed-Job pass writes; without it \
             the run is `NoExitCode` and the operator is told nothing"
        );
    }
    assert!(
        !TERMINAL_STATES.contains(&"WaitingForPod"),
        "`WaitingForPod` is a `RunnerReady` reason and NEVER a verdict: \"nothing has happened \
         yet\" cannot be what a run finished as"
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

/// A successful probe and a backup of the same cluster must project the same
/// credential. Inspect the actual POST, not just the pure Job builder.
#[tokio::test]
async fn scram_backups_reuse_the_probed_cluster_connection_configuration() {
    for tls in [false, true] {
        for secret_name in ["prod-sasl", "rotated-source-sasl"] {
            let mut cluster: Value = serde_json::from_str(&kafka_cluster_json()).unwrap();
            cluster["spec"]["auth"]["tls"] = serde_json::json!(tls);
            cluster["spec"]["auth"]["secretRef"]["name"] = serde_json::json!(secret_name);
            let mut routes = create_routes(201, existing_plan_config_map(UID));
            routes
                .iter_mut()
                .find(|r| r.path_suffix == "/kafkaclusters/prod")
                .unwrap()
                .body = cluster.to_string();
            let (seen, bodies) = create_pass(routes).await;
            let posted = bodies
                .iter()
                .find(|b| b.method == "POST" && path(&b.uri).ends_with("/jobs"))
                .unwrap();
            let job: Value = serde_json::from_str(&posted.body).unwrap();
            let env = job["spec"]["template"]["spec"]["containers"][0]["env"]
                .as_array()
                .unwrap();
            let passwords: Vec<_> = env
                .iter()
                .filter(|e| e["name"] == "LOGWEIR_SOURCE_PASSWORD")
                .collect();
            assert_eq!(
                passwords.len(),
                1,
                "backup must project the source password exactly once"
            );
            assert_eq!(
                passwords[0]["valueFrom"]["secretKeyRef"]["name"],
                secret_name
            );
            assert_eq!(passwords[0]["valueFrom"]["secretKeyRef"]["key"], "password");
            assert_ne!(passwords[0]["valueFrom"]["secretKeyRef"]["optional"], true);
            assert!(
                passwords[0].get("value").is_none(),
                "never copy a password into a Job literal"
            );

            let cluster = serde_json::from_value(cluster).unwrap();
            let probe = job::build(
                &weirkeeper::controllers::kafka_cluster::runner_job_spec(&cluster).unwrap(),
            );
            let probe = serde_json::to_value(probe).unwrap();
            let probe_env = probe["spec"]["template"]["spec"]["containers"][0]["env"]
                .as_array()
                .unwrap();
            assert_eq!(
                passwords[0],
                probe_env
                    .iter()
                    .find(|e| e["name"] == "LOGWEIR_SOURCE_PASSWORD")
                    .unwrap()
            );
            assert_eq!(job["metadata"]["namespace"], probe["metadata"]["namespace"]);
            assert!(!env.iter().any(|e| e["name"] == "LOGWEIR_TARGET_PASSWORD"));
            for name in ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"] {
                let archive = env.iter().find(|e| e["name"] == name).unwrap();
                assert_eq!(archive["valueFrom"]["secretKeyRef"]["name"], "logweir-s3");
            }

            let cm = posted_config_map(&bodies);
            let plan: logweir_core::spec::BackupSpec =
                serde_yaml::from_str(cm["data"]["backup.yaml"].as_str().unwrap()).unwrap();
            assert_eq!(
                plan.source.bootstrap_servers,
                cluster.spec.bootstrap_servers
            );
            assert_eq!(
                plan.source.auth,
                logweir_core::spec::AuthSpec::ScramSha512 {
                    username: "logweir".to_string(),
                    tls
                }
            );
            assert!(
                !cm.to_string().contains(secret_name),
                "the plan carries identity, not credentials"
            );
            assert!(
                !seen.iter().any(|r| path(&r.uri).contains("/secrets")),
                "the controller must not read Secret values"
            );
            assert_eq!(
                seen.iter()
                    .filter(|r| r.method == "GET" && path(&r.uri).ends_with("/kafkaclusters/prod"))
                    .count(),
                1,
                "plan and credential must use one cluster snapshot"
            );
        }
    }
}

#[tokio::test]
async fn plaintext_backups_do_not_project_an_incidental_source_secret() {
    let mut routes = create_routes(201, existing_plan_config_map(UID));
    let route = routes
        .iter_mut()
        .find(|r| r.path_suffix == "/kafkaclusters/prod")
        .unwrap();
    let mut cluster: Value = serde_json::from_str(&route.body).unwrap();
    cluster["spec"]["auth"]["mode"] = serde_json::json!("plaintext");
    // …and `tls: false` with it: PLAT-07.1 refuses `plaintext` with `tls: true`
    // (TLS without SASL) rather than dialling it in the clear, so the leftover
    // credential this row is about has to sit on a connection that resolves.
    cluster["spec"]["auth"]["tls"] = serde_json::json!(false);
    route.body = cluster.to_string();
    let (_, bodies) = create_pass(routes).await;
    let posted = bodies
        .iter()
        .find(|b| b.method == "POST" && path(&b.uri).ends_with("/jobs"))
        .unwrap();
    let job: Value = serde_json::from_str(&posted.body).unwrap();
    let env = job["spec"]["template"]["spec"]["containers"][0]["env"]
        .as_array()
        .unwrap();
    assert!(!env
        .iter()
        .any(|e| e["name"] == "LOGWEIR_SOURCE_PASSWORD" || e["name"] == "LOGWEIR_TARGET_PASSWORD"));
}

#[tokio::test]
async fn scram_backup_without_a_source_secret_is_refused_before_creating_resources() {
    for secret_ref in [
        Value::Null,
        serde_json::json!({"name": ""}),
        serde_json::json!({"name": " "}),
    ] {
        let mut routes = create_routes(201, existing_plan_config_map(UID));
        let route = routes
            .iter_mut()
            .find(|r| r.path_suffix == "/kafkaclusters/prod")
            .unwrap();
        let mut cluster: Value = serde_json::from_str(&route.body).unwrap();
        cluster["spec"]["auth"]["secretRef"] = secret_ref;
        route.body = cluster.to_string();
        let (client, seen, bodies) = mock_client_recording_bodies(routes);
        let outcome = reconcile_backup(
            &backup(),
            &client,
            &unobserved_archive,
            &unverified_evidence,
            utc(2026, 11, 9, 3, 17),
        )
        .await
        .unwrap();
        assert_eq!(
            outcome.terminal_state.as_deref(),
            Some("CredentialNotRenderable")
        );
        assert!(!seen.lock().unwrap().iter().any(|r| r.method == "POST"));
        let statuses = patched_statuses(&bodies.lock().unwrap());
        assert_eq!(statuses[0]["phase"], "Failed");
        assert!(statuses[0].to_string().contains("auth.secretRef"));
    }
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

    // EXACTLY THREE KEYS: the two documents the argv points `--spec` and
    // `--allowed-clusters` at, and the canonical snapshot both are rendered
    // from (PLAT-06.1). A fourth would be a file nothing produces from the
    // snapshot and a surface an auditor has to account for.
    let data = cm["data"].as_object().expect("the ConfigMap carries data");
    let mut keys: Vec<&str> = data.keys().map(String::as_str).collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        vec![
            "allowed-clusters.json",
            "backup.yaml",
            "execution-inputs.json"
        ],
        "EXACTLY the two runner documents and the snapshot they are rendered from"
    );
    assert_eq!(
        cm["immutable"].as_bool(),
        Some(true),
        "the plan is frozen: `immutable: true`, create-only, never patched"
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
    let argv = runner_job_spec(
        &scheduled,
        &serde_json::from_str(&kafka_cluster_json()).unwrap(),
    )
    .expect("the scheduled fixture yields a spec")
    .args;
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
        &frozen_backup(),
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
        &frozen_backup(),
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
        &frozen_backup(),
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

/// An unchanged plan can be reused only when the existing object is ours.
/// Foreign ownership must fail even when all plan data matches.
///
/// KILLS: treating every 409 as success; keying the check on the name instead
/// of the UID.
#[tokio::test]
async fn a_conflicting_plan_config_map_checks_ownership_even_when_data_matches() {
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

#[tokio::test]
async fn a_probe_observation_does_not_invalidate_an_owned_backup_plan_retry() {
    let mut cluster: weirkeeper::crds::kafka_cluster::KafkaCluster =
        serde_json::from_str(&kafka_cluster_json()).unwrap();
    cluster.status = None;
    let cm = weirkeeper::controllers::backup::plan_config_map(&backup(), &cluster).unwrap();
    let (client, seen, _) =
        mock_client_recording_bodies(create_routes(409, serde_json::to_string(&cm).unwrap()));
    let outcome = reconcile_backup(
        &backup(),
        &client,
        &unobserved_archive,
        &unverified_evidence,
        utc(2026, 11, 9, 3, 17),
    )
    .await
    .unwrap();
    assert!(
        outcome.created,
        "a probe's new clusterId is not a connection change"
    );
    assert!(seen
        .lock()
        .unwrap()
        .iter()
        .any(|r| r.method == "POST" && path(&r.uri).ends_with("/jobs")));
}

#[tokio::test]
async fn an_owned_stale_backup_plan_is_refused_before_creating_a_job() {
    for changed_key in ["backup.yaml", "allowed-clusters.json", "allowed-target-ids"] {
        let cluster = serde_json::from_str(&kafka_cluster_json()).unwrap();
        let cm = weirkeeper::controllers::backup::plan_config_map(&backup(), &cluster).unwrap();
        let mut existing = serde_json::to_value(cm).unwrap();
        if changed_key == "allowed-target-ids" {
            existing["data"]["allowed-clusters.json"] =
                serde_json::json!(r#"{"allowed_cluster_ids":["unexpected-target"]}"#);
        } else {
            existing["data"][changed_key] = serde_json::json!("a previous cluster configuration");
        }
        let (client, seen, bodies) =
            mock_client_recording_bodies(create_routes(409, existing.to_string()));
        let outcome = reconcile_backup(
            &backup(),
            &client,
            &unobserved_archive,
            &unverified_evidence,
            utc(2026, 11, 9, 3, 17),
        )
        .await
        .unwrap();
        assert_eq!(
            outcome.terminal_state.as_deref(),
            Some("PlanConfigMapConflict")
        );
        assert!(!outcome.created);
        let seen = seen.lock().unwrap();
        assert!(!seen
            .iter()
            .any(|r| r.method == "POST" && path(&r.uri).ends_with("/jobs")));
        assert!(
            !seen
                .iter()
                .any(|r| ["PATCH", "PUT", "DELETE"].contains(&r.method.as_str())
                    && path(&r.uri).contains("/configmaps")),
            "never rewrite a plan another reconcile may already have mounted"
        );
        let statuses = patched_statuses(&bodies.lock().unwrap());
        assert_eq!(statuses[0]["phase"], "Failed");
    }
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

/// The third selection shape — a named allowlist **and** a dynamic block — is a
/// **terminal refusal before any `POST`**, and so is an empty allowlist with no
/// block.
///
/// # Why the controller refuses this and not only the API server
///
/// `crds::backup::SELECTION_SHAPE_RULE` refuses it at admission, which handles
/// every object created against this CRD revision. It does NOT handle an object
/// admitted by an OLDER CRD and reconciled by this controller, which is the
/// ordinary state of affairs during an upgrade — CRDs are applied before the
/// controller, so the reverse (a newer controller reading objects admitted by
/// an older schema) is the supported direction and the one that reaches here.
///
/// AND THE FAILURE MODE IS THE BAD KIND. Nothing in this build resolves
/// `allUserTopics`, so without this rail an operator who asked for
/// whole-cluster coverage would get a two-topic run, a signed receipt
/// attesting two topics, and no signal anywhere. `topics: []` alone fails SAFE
/// — the runner's empty-list rail exits 3 before contacting the engine — and is
/// refused here too, earlier and by name.
///
/// TERMINAL, because `spec` is CEL-immutable: nothing about waiting changes a
/// shape that cannot be edited.
#[tokio::test]
async fn a_selection_that_is_neither_shape_is_refused_before_any_post() {
    let both = {
        let mut v: Value = serde_json::from_str(&backup_json()).expect("the fixture is JSON");
        v["spec"]["allUserTopics"] =
            serde_json::json!({ "incompleteDiscovery": "BackUpVisibleTopics" });
        v
    };
    let neither = {
        let mut v: Value = serde_json::from_str(&backup_json()).expect("the fixture is JSON");
        v["spec"]["topics"] = serde_json::json!([]);
        v
    };

    for (case, value, field) in [
        (
            "a named allowlist beside a dynamic block",
            both,
            "spec.allUserTopics",
        ),
        (
            "an empty allowlist with no dynamic block",
            neither,
            "spec.topics",
        ),
    ] {
        let b: Backup = serde_json::from_value(value).expect("the mutated fixture is a Backup");
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
            Some("InvalidTopicSelection"),
            "case `{case}`: D1 §3.4's terminal state, not a generic one"
        );
        {
            let seen = seen.lock().expect("the recorder is readable");
            assert!(
                !seen.iter().any(|r| r.method == "POST"),
                "case `{case}`: ZERO `POST`s — no ConfigMap, no Job; got {:?}",
                *seen
            );
        }
        let statuses = patched_statuses(&bodies.lock().expect("the body recorder is readable"));
        assert_eq!(
            conditions_of(&statuses[0]),
            vec![(
                "Failed".to_string(),
                "True".to_string(),
                "InvalidTopicSelection".to_string()
            )],
            "case `{case}`'s condition"
        );
        let message = statuses[0]["conditions"][0]["message"]
            .as_str()
            .expect("the condition carries a message");
        assert!(
            message.contains(field),
            "case `{case}`: the message NAMES the field that is wrong, so an operator does not \
             have to guess which half to change; got: {message}"
        );
    }

    // THE CONTROL. The unmodified fixture is a named allowlist and is rendered,
    // so the two refusals above failed on the SHAPE and on nothing else.
    let (_seen, bodies) = create_pass(create_routes(201, existing_plan_config_map(UID))).await;
    assert!(
        posted_config_map(&bodies)["data"]["backup.yaml"].is_string(),
        "a non-empty `topics` with no `allUserTopics` is one of the two legal shapes"
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
        &frozen_backup(),
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
        &frozen_backup(),
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
        &frozen_backup(),
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
            // The API server answers a create with the object it stored: this
            // Backup's own plan, owned by the 63-character name.
            body: serde_json::to_string(
                &weirkeeper::controllers::backup::plan_config_map(
                    &b,
                    &serde_json::from_str(&kafka_cluster_json()).unwrap(),
                )
                .expect("the 63-character Backup renders its plan"),
            )
            .unwrap(),
        },
        Route {
            method: "POST",
            path_suffix: "/jobs",
            status: 201,
            body: running_job_body().replace(NAME, &ok_name),
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
        &frozen_backup(),
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
        &frozen_backup(),
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
        &frozen_backup(),
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
            &frozen_backup(),
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
                // D3 W2: two additive observation fields. This row is about
                // `windowCovered`'s units, so both are `None` — and that is a
                // negative control for STATUS-RECORDS: an unobserved count
                // writes no `status.records`.
                records: None,
                capture: None,
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
        &frozen_backup(),
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
        &frozen_backup(),
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
            &frozen_backup(),
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
                    records: None,
                    capture: None,
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
        &frozen_backup(),
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
        &frozen_backup(),
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
        &frozen_backup(),
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

/// The `Running` pass patches the status, observes the pod, and writes no
/// outcome.
///
/// # This row CHANGED at D3 W2 (PLAT-14.1), and the change is the point
///
/// It used to assert that an unfinished Job's pod is **not read at all**. D3
/// §2.3 makes that exactly backwards: the pod is where every answer to "why
/// has nothing happened for four minutes" lives, and a `Backup` whose runner
/// sits in `ImagePullBackOff` was `phase: Running` by every field that existed
/// before. What has NOT changed, and is asserted below, is everything the old
/// row was really protecting: no exit code, no evidence, no outcome and no
/// Job write while the run is in flight — and the pod is still reached only
/// through the owner-UID-verified selectors (seam S6).
#[tokio::test]
async fn a_running_job_patches_only_the_phase_and_the_job_ref() {
    let (client, seen, bodies) = mock_client_recording_bodies(running_routes());
    let outcome = reconcile_backup(
        &frozen_backup(),
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
    // D3 §2.2 — the progress block, and `RunnerReady` beside `JobCreated` in
    // the SAME array, because a merge patch replaces arrays.
    assert_eq!(
        statuses[0]["progress"]["stage"].as_str(),
        Some("Running"),
        "the runner container is running, so the stage is `Running`; got {}",
        statuses[0]["progress"]
    );
    assert_eq!(
        statuses[0]["progress"]["runner"]["podName"].as_str(),
        Some(POD),
        "the runner block names the one owned pod"
    );
    assert_eq!(
        statuses[0]["progress"]["runnerPhase"]["name"].as_str(),
        Some("engine"),
        "and the runner's own phase, out of the ratified progress channel"
    );
    assert!(
        statuses[0]["progress"]["diagnostics"].is_null(),
        "nothing is wrong, so there is no diagnostic — an empty array would render as a \
         warning badge; got {}",
        statuses[0]["progress"]
    );
    let (status, reason, _) = condition_named(&statuses[0], "RunnerReady").expect("RunnerReady");
    assert_eq!(
        (status.as_str(), reason.as_str()),
        ("True", "RunnerStarted"),
        "the container has been seen running"
    );

    // THE POD IS LISTED THROUGH THE SELECTORS AND NOTHING IS WRITTEN TO IT.
    let seen = seen.lock().expect("the recorder is readable");
    assert!(
        seen.iter()
            .any(|r| r.method == "GET" && r.uri.contains("/pods?")),
        "the owned pod is found by the two selectors, never by name; got {seen:?}"
    );
    assert!(
        seen.iter()
            .all(|r| r.method == "GET" || (r.method == "PATCH" && r.uri.contains("/backups/"))),
        "an unfinished run writes only its own status — no Job patch, no TTL, no delete; got \
         {seen:?}"
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
    running_routes_with(pod_list_running(), progress_log("-1:engine"))
}

/// [`running_routes`] with the pod list and the pod log as parameters.
///
/// **THE POD LIST AND THE LOG ROUTE ARE NEW IN D3 W2 (PLAT-14.1).** Before
/// §2.3 the running pass read the Job and nothing else; it now derives the one
/// diagnosis for every Job-backed run, which means finding the owned pod
/// (owner-UID verified, seam S6) and — while the runner is running — reading
/// the bounded progress tail. Both are parameters so a row can put a
/// `CreateContainerConfigError` or an empty list in front of the reconciler.
fn running_routes_with(pods: String, log: String) -> Vec<Route> {
    vec![
        Route {
            method: "GET",
            path_suffix: "/jobs/logweir-backup-nightly-20261109-031700",
            status: 200,
            body: running_job_body(),
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
            body: log,
        },
        Route {
            method: "GET",
            path_suffix: "/events",
            status: 200,
            body: r#"{"apiVersion":"v1","kind":"EventList","metadata":{},"items":[]}"#.to_string(),
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
            body: running_job_body(),
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
        &frozen_backup(),
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

    // Onto the status the object ALREADY had — `status.execution` included —
    // exactly as the API server merges it.
    let mut stored = serde_json::to_value(frozen_backup().status).expect("the status serialises");
    apply_merge_patch(&mut stored, &patched_statuses(&first)[0]);
    let mut steady = frozen_backup();
    steady.status = Some(
        serde_json::from_value::<BackupStatus>(stored)
            .expect("the patched status is a BackupStatus — the API server stores it"),
    );

    // ONE REQUEUE LATER (`REQUEUE_SECS` = 15), the same still-running Job.
    let (client, _seen, bodies) = mock_client_recording_bodies(running_routes());
    reconcile_backup(
        &steady,
        &client,
        &unobserved_archive,
        &unverified_evidence,
        utc(2026, 11, 9, 3, 20) + chrono::Duration::seconds(15),
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
    // A RESTART WITH THE JOB PRESENT RE-READS NO INPUTS (PLAT-06.1): the
    // frozen inputs are not re-read, re-rendered or re-created while a Job
    // this Backup controls is running. The reads that DO happen are D3 §2.3's
    // observation — the Job, its owned pod, and nothing else: the runner is
    // running so no events are listed, and `lastObservedTime` is 15 s old so
    // the progress read is throttled out.
    assert_eq!(
        calls(&second),
        vec![
            (
                "GET".to_string(),
                format!("/apis/batch/v1/namespaces/{NS}/jobs/{NAME}")
            ),
            ("GET".to_string(), format!("/api/v1/namespaces/{NS}/pods")),
        ],
        "the running pass is one Job read and one owned-pod list: {:?}",
        calls(&second)
    );

    // ---- D3 §13's regression row, second half: the heartbeat --------------
    //
    // `lastObservedTime` is rewritten AT MOST ONCE PER 60 s (E11(d)), which
    // means it IS rewritten after 60 s — the field exists so a console can
    // tell "still running" from "the controller stopped looking", and one that
    // never moved could not. At +61 s exactly one patch is sent, and the only
    // thing in it that moved is that field.
    let (client, _seen, bodies) = mock_client_recording_bodies(running_routes());
    reconcile_backup(
        &steady,
        &client,
        &unobserved_archive,
        &unverified_evidence,
        utc(2026, 11, 9, 3, 20) + chrono::Duration::seconds(61),
    )
    .await
    .expect("the heartbeat reconcile succeeds");
    let third = bodies
        .lock()
        .expect("the body recorder is readable")
        .clone();
    assert_eq!(
        status_patch_count(&third),
        1,
        "one heartbeat per minute per running run, and not one per requeue: {third:?}"
    );
    let heartbeat = &patched_statuses(&third)[0];
    let before = &patched_statuses(&first)[0];
    assert_ne!(
        heartbeat["progress"]["lastObservedTime"], before["progress"]["lastObservedTime"],
        "the heartbeat is what moved"
    );
    assert_eq!(
        heartbeat["progress"]["lastTransitionTime"], before["progress"]["lastTransitionTime"],
        "and `lastTransitionTime` did NOT: neither the stage nor the reason changed, which is \
         the `metav1.Condition` rule applied to the progress block"
    );
    assert_eq!(
        heartbeat["progress"]["stage"], before["progress"]["stage"],
        "nor the stage"
    );
}

// ===========================================================================
// PLAT-06.1 — the execution contract: derived identity, frozen inputs, no
// annotation ever executed
// ===========================================================================

use weirkeeper::backup_execution::{
    canonical_inputs, execution_identity, BackupExecutionInputs, FrozenInputs,
    EXECUTION_ID_ANNOTATION, INPUTS_KEY, INPUTS_VERSION, INPUTS_VERSIONS_READ, INPUTS_VERSION_V1,
    INPUTS_VERSION_V2, RUNNER_ARGV_ANNOTATION,
};
use weirkeeper::conditions::{
    CONDITION_EXECUTION_INPUTS_UNVERIFIED, CONDITION_RUNNER_ARGV_ANNOTATION_IGNORED,
    REASON_JOB_INPUTS_MISMATCH, REASON_LEGACY_EXECUTION, REASON_RUNNER_ARGV_ANNOTATION_IGNORED,
    REASON_RUNNER_ARGV_ANNOTATION_MALFORMED, TERMINAL_STATE_EXECUTION_SPEC_INVALID,
    TERMINAL_STATE_INVALID_TOPIC_SELECTION, TERMINAL_STATE_JOB_NAME_CONFLICT,
    TERMINAL_STATE_PLAN_CONFIG_MAP_CONFLICT, TERMINAL_STATE_RUN_POLICY_DIGEST_MISMATCH,
    TERMINAL_STATE_SCHEDULED_IDENTITY_MISMATCH, TERMINAL_STATE_SCHEDULE_NOT_FOUND,
};
use weirkeeper::controllers::backup::{
    desired_execution_inputs, execution_status_patch, plan_config_map, runner_job,
    runner_job_spec_from_inputs, running_status_patch, with_status_patch,
};
use weirkeeper::crds::kafka_cluster::KafkaCluster;

/// The source `KafkaCluster` fixture, typed.
fn prod_cluster() -> KafkaCluster {
    serde_json::from_str(&kafka_cluster_json()).expect("the fixture is a KafkaCluster")
}

/// What this controller would freeze for `b` against [`prod_cluster`].
fn desired_for(b: &Backup) -> FrozenInputs {
    desired_execution_inputs(b, &prod_cluster()).expect("the fixture resolves")
}

/// `b` as a previous pass left it: inputs frozen and recorded, its Job created
/// and reported running. The state a controller restart, or a deleted Job,
/// starts from.
fn running_after_freeze(b: &Backup) -> Backup {
    let frozen = desired_for(b);
    let mut recorded = with_status_patch(b, &execution_status_patch(&frozen));
    let running = running_status_patch(&recorded, NAME, utc(2026, 11, 9, 3, 17));
    recorded = with_status_patch(&recorded, &running);
    recorded
}

/// The plan ConfigMap this controller freezes for `b`, as JSON.
fn frozen_config_map(b: &Backup) -> Value {
    serde_json::to_value(plan_config_map(b, &prod_cluster()).expect("the plan renders"))
        .expect("a ConfigMap serialises")
}

/// Every recorded request as `(METHOD, path)`.
fn calls(seen: &[SeenBody]) -> Vec<(String, String)> {
    seen.iter()
        .map(|r| (r.method.clone(), path(&r.uri).to_string()))
        .collect()
}

/// Whether any request wrote to a ConfigMap other than by `POST` to the
/// collection — a patch, a replace or a delete of an existing plan.
fn rewrote_a_config_map(seen: &[SeenBody]) -> bool {
    seen.iter().any(|r| {
        ["PATCH", "PUT", "DELETE"].contains(&r.method.as_str())
            && path(&r.uri).contains("/configmaps")
    })
}

/// The first `POST …/jobs` body, as JSON.
fn posted_job(seen: &[SeenBody]) -> Option<Value> {
    seen.iter()
        .find(|b| b.method == "POST" && path(&b.uri).ends_with("/jobs"))
        .map(|b| serde_json::from_str(&b.body).expect("the POSTed Job is JSON"))
}

/// A condition of `type` on a status value, as `(status, reason, message)`.
fn condition_named(status: &Value, r#type: &str) -> Option<(String, String, String)> {
    status["conditions"].as_array()?.iter().find_map(|c| {
        (c["type"] == r#type).then(|| {
            (
                c["status"].as_str().unwrap_or_default().to_string(),
                c["reason"].as_str().unwrap_or_default().to_string(),
                c["message"].as_str().unwrap_or_default().to_string(),
            )
        })
    })
}

/// One sequenced route: `(method, path suffix, the answers in order)`.
type SequencedRoute = (&'static str, &'static str, Vec<(u16, String)>);

/// A double whose answer to each `(method, path suffix)` is the next entry of
/// that route's list, the last one repeating. For the rows whose property is
/// an ORDER of answers on one path — a Job that is absent, then present.
fn sequenced_client(
    routes: Vec<SequencedRoute>,
) -> (
    kube::Client,
    std::sync::Arc<std::sync::Mutex<Vec<SeenBody>>>,
) {
    use http_body_util::BodyExt as _;
    use std::sync::{Arc, Mutex};
    let seen: Arc<Mutex<Vec<SeenBody>>> = Arc::new(Mutex::new(Vec::new()));
    let counters: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(vec![0; routes.len()]));
    let routes = Arc::new(routes);
    let svc = {
        let seen = Arc::clone(&seen);
        tower::util::service_fn(move |req: http::Request<kube::client::Body>| {
            let seen = Arc::clone(&seen);
            let counters = Arc::clone(&counters);
            let routes = Arc::clone(&routes);
            async move {
                let method = req.method().to_string();
                let uri = req.uri().to_string();
                let body = req
                    .into_body()
                    .collect()
                    .await
                    .map(|c| String::from_utf8_lossy(&c.to_bytes()).into_owned())
                    .unwrap_or_default();
                seen.lock().expect("readable").push(SeenBody {
                    method: method.clone(),
                    uri: uri.clone(),
                    body,
                });
                let p = uri.split('?').next().unwrap_or(&uri).to_string();
                let at = routes
                    .iter()
                    .position(|(m, suffix, _)| {
                        m.eq_ignore_ascii_case(&method) && p.ends_with(suffix)
                    })
                    .unwrap_or_else(|| panic!("sequenced double: no route for {method} {p}"));
                let answers = &routes[at].2;
                let n = {
                    let mut counters = counters.lock().expect("readable");
                    let n = counters[at];
                    counters[at] += 1;
                    n
                };
                let (status, payload) = answers[n.min(answers.len() - 1)].clone();
                Ok::<_, std::convert::Infallible>(
                    http::Response::builder()
                        .status(status)
                        .body(kube::client::Body::from(payload.into_bytes()))
                        .expect("a response builds"),
                )
            }
        })
    };
    (kube::Client::new(svc, "default"), seen)
}

/// **A MANUAL BACKUP WITH NO ANNOTATION RUNS**, and the order is the contract:
/// the inputs are frozen in the immutable plan ConfigMap, `status.execution`
/// records them, and only then does a Job exist — built from those inputs and
/// stamped with their digest.
///
/// KILLS: requiring the runner-argv annotation again; creating the Job before
/// `status.execution` is recorded; a mutable plan; a Job whose digest is not the
/// recorded one; a snapshot that names a Secret.
#[tokio::test]
async fn a_manual_backup_without_an_annotation_freezes_its_inputs_then_runs_them() {
    let (client, _seen, bodies) =
        mock_client_recording_bodies(create_routes(201, existing_plan_config_map(UID)));
    let outcome = reconcile_backup(
        &backup(),
        &client,
        &unobserved_archive,
        &unverified_evidence,
        utc(2026, 11, 9, 3, 17),
    )
    .await
    .expect("a manual Backup with no annotation reconciles");
    assert!(outcome.created, "the Job is created: {outcome:?}");
    assert_eq!(outcome.terminal_state, None);
    let bodies = bodies.lock().expect("readable").clone();
    let order = calls(&bodies);

    // THE ORDER: ConfigMap POST < status.execution PATCH < Job POST < running PATCH.
    let at = |method: &str, suffix: &str, nth: usize| {
        order
            .iter()
            .enumerate()
            .filter(|(_, (m, p))| m == method && p.ends_with(suffix))
            .nth(nth)
            .map(|(i, _)| i)
            .unwrap_or_else(|| panic!("no {method} …{suffix} #{nth} in {order:?}"))
    };
    let cm_post = at("POST", "/configmaps", 0);
    let recorded = at("PATCH", "/status", 0);
    let job_post = at("POST", "/jobs", 0);
    let running = at("PATCH", "/status", 1);
    assert!(
        cm_post < recorded && recorded < job_post && job_post < running,
        "freeze, record, THEN create: {order:?}"
    );
    assert!(!rewrote_a_config_map(&bodies));

    // THE CONFIGMAP: immutable, three keys, digest and identity annotated.
    let cm = posted_config_map(&bodies);
    assert_eq!(cm["immutable"], true, "the plan is frozen: {cm}");
    let snapshot_text = cm["data"][INPUTS_KEY].as_str().expect("the snapshot key");
    let digest = logweir_core::ids::sha256_prefixed(snapshot_text.as_bytes());
    assert_eq!(
        cm["metadata"]["annotations"][INPUTS_SHA256_ANNOTATION],
        digest
    );
    assert_eq!(cm["metadata"]["annotations"][EXECUTION_ID_ANNOTATION], UID);
    let snapshot: Value = serde_json::from_str(snapshot_text).expect("the snapshot is JSON");
    assert_eq!(snapshot["version"], INPUTS_VERSION);
    assert_eq!(snapshot["execution"]["id"], UID, "a manual run IS its UID");
    assert_eq!(snapshot["execution"]["trigger"], "manual");
    assert_eq!(
        snapshot["execution"]["backup"],
        serde_json::json!({"namespace": NS, "name": NAME, "uid": UID})
    );
    assert!(snapshot["execution"].get("schedule").is_none());
    assert_eq!(snapshot["source"]["cluster"]["uid"], CLUSTER_UID);
    assert_eq!(
        snapshot["runner"]["args"],
        serde_json::json!(runner_argv(ExecutionTrigger::Manual, UID))
    );
    for forbidden in [
        "prod-sasl",
        "logweir-s3",
        "password",
        "secret-access-key",
        "access-key-id",
    ] {
        assert!(
            !cm.to_string().contains(forbidden),
            "the frozen plan names no Secret and no credential key: `{forbidden}` in {cm}"
        );
    }

    // STATUS.EXECUTION AND STATUS.SELECTION, exactly, and in a patch of its own
    // that replaces no array. D1 §7.6: the coverage label is written AT THE
    // FREEZE, so it is on the object for the whole time a run is watched, not
    // only once it finishes.
    let statuses = patched_statuses(&bodies);
    assert_eq!(
        statuses[0],
        serde_json::json!({
            "execution": {
                "id": UID,
                "inputsRef": { "name": plan_config_map_name(NAME) },
                "inputsSha256": digest,
            },
            "selection": {
                "mode": "SelectedTopics",
                "coverage": "NamedTopics",
                "resolvedTopicCount": 2,
                "resolvedTopicBytes": 14,
            }
        }),
        "the first status write records the frozen inputs and the frozen selection, and nothing \
         else"
    );

    // THE JOB: the frozen argv, stamped with the recorded digest and identity.
    let job = posted_job(&bodies).expect("the Job was POSTed");
    assert_eq!(
        job["spec"]["template"]["spec"]["containers"][0]["args"],
        snapshot["runner"]["args"]
    );
    for annotations in [
        &job["metadata"]["annotations"],
        &job["spec"]["template"]["metadata"]["annotations"],
    ] {
        assert_eq!(annotations[INPUTS_SHA256_ANNOTATION], digest, "{job}");
        assert_eq!(annotations[EXECUTION_ID_ANNOTATION], UID, "{job}");
    }
    // …and the POSTed Job is exactly the pure builder's Job for those inputs.
    let frozen = desired_for(&backup());
    assert_eq!(frozen.sha256, digest);
    assert_eq!(
        job,
        serde_json::to_value(
            runner_job(
                &backup(),
                &prod_cluster(),
                &frozen,
                &job::RunnerImage::default()
            )
            .unwrap()
        )
        .unwrap()
    );

    // NO ANNOTATION, NO ANNOTATION CONDITION.
    assert_eq!(
        condition_named(&statuses[1], CONDITION_RUNNER_ARGV_ANNOTATION_IGNORED),
        None,
        "a Backup with no annotation is surfaced as nothing: {}",
        statuses[1]
    );
}

/// **A SCHEDULED BACKUP KEEPS ITS DETERMINISTIC IDENTITY** — the schedule UID
/// and the slot — in the snapshot, the plan document and the argv, with no
/// annotation.
///
/// KILLS: taking a scheduled run's identity from its own UID; losing the slot
/// from the snapshot; a plan document whose `backup_id` is not the argv's.
#[tokio::test]
async fn a_scheduled_backup_freezes_its_slot_identity() {
    let scheduled = scheduled_backup();
    let expected_id = weirkeeper::slot::backup_id_for(SCHEDULE_UID, "20261109-031700");
    let (client, _seen, bodies) =
        mock_client_recording_bodies(create_routes(201, existing_plan_config_map(UID)));
    let outcome = reconcile_backup(
        &scheduled,
        &client,
        &unobserved_archive,
        &unverified_evidence,
        utc(2026, 11, 9, 3, 17),
    )
    .await
    .expect("the scheduled Backup reconciles");
    assert!(outcome.created);
    let bodies = bodies.lock().expect("readable").clone();
    let cm = posted_config_map(&bodies);
    let snapshot: Value =
        serde_json::from_str(cm["data"][INPUTS_KEY].as_str().unwrap()).expect("JSON");
    assert_eq!(snapshot["execution"]["id"], expected_id);
    assert_eq!(snapshot["execution"]["trigger"], "schedule");
    assert_eq!(
        snapshot["execution"]["schedule"],
        serde_json::json!({"name": "nightly", "uid": SCHEDULE_UID, "slot": "20261109-031700"})
    );
    let plan: logweir_core::spec::BackupSpec =
        serde_yaml::from_str(cm["data"]["backup.yaml"].as_str().unwrap()).unwrap();
    assert_eq!(plan.backup_id, expected_id);
    let job = posted_job(&bodies).expect("the Job was POSTed");
    assert_eq!(
        job["spec"]["template"]["spec"]["containers"][0]["args"],
        serde_json::json!(runner_argv(ExecutionTrigger::Schedule, &expected_id))
    );
    assert_eq!(
        patched_statuses(&bodies)[0]["execution"]["id"],
        expected_id,
        "status.execution.id is the scheduled identity"
    );
}

/// **SNAPSHOT EQUALITY.** The same resolved inputs are the same canonical bytes
/// and the same digest; a manual and a scheduled run of the same spec differ
/// ONLY in the identity and the argv that carries it; an informational probe
/// observation changes the bytes but not the executable inputs; a real input
/// change changes both.
///
/// KILLS: a non-deterministic encoding (a map in hash order, a clock read);
/// an identity leaking into another field; comparing restarts by digest alone,
/// which would refuse every retry after a probe.
#[test]
fn identical_resolved_inputs_are_identical_bytes_and_only_identity_separates_triggers() {
    let one = desired_for(&backup());
    let two = desired_for(&backup());
    assert_eq!(one.canonical, two.canonical);
    assert_eq!(one.sha256, two.sha256);
    assert_eq!(
        one.sha256,
        logweir_core::ids::sha256_prefixed(one.canonical.as_bytes())
    );
    let reparsed: BackupExecutionInputs = serde_json::from_str(&one.canonical).unwrap();
    assert_eq!(
        canonical_inputs(&reparsed).unwrap(),
        one.canonical,
        "the encoding round-trips"
    );

    // Manual vs scheduled: mask the identity, the trigger, the schedule
    // reference and the argv; nothing else differs.
    let scheduled = desired_for(&scheduled_backup());
    let masked = |f: &FrozenInputs| {
        let mut v: Value = serde_json::from_str(&f.canonical).unwrap();
        v["execution"] = Value::Null;
        v["trigger"] = Value::Null;
        v["scheduleRef"] = Value::Null;
        v["runner"]["args"] = Value::Null;
        v
    };
    assert_ne!(one.sha256, scheduled.sha256);
    assert_eq!(
        masked(&one),
        masked(&scheduled),
        "a manual and a scheduled run of one spec differ only in identity and trigger"
    );
    assert_ne!(one.inputs.execution, scheduled.inputs.execution);

    // An informational observation: different bytes, same executable inputs.
    let mut probed = prod_cluster();
    probed.status.as_mut().unwrap().cluster_id = Some("ANOTHER-OBSERVATION".to_string());
    let observed = desired_execution_inputs(&backup(), &probed).unwrap();
    assert_ne!(observed.sha256, one.sha256);
    assert_eq!(observed.inputs.executable(), one.inputs.executable());

    // A real change: the bootstrap addresses.
    let mut moved = prod_cluster();
    moved.spec.bootstrap_servers = vec!["elsewhere:9093".to_string()];
    let changed = desired_execution_inputs(&backup(), &moved).unwrap();
    assert_ne!(changed.inputs.executable(), one.inputs.executable());
}

/// **HOSTILE AND MALFORMED ANNOTATIONS ARE IGNORED AND SURFACED.** Whatever the
/// annotation says — another subcommand, another spec path, another signing
/// key, another backup id, or nothing parseable — the frozen inputs and the Job
/// are byte-identical to an un-annotated Backup's, and a
/// `RunnerArgvAnnotationIgnored` condition names the annotation by size and
/// digest and never by content.
///
/// KILLS: executing any part of the annotation; echoing its content into the
/// status; failing to surface it; surfacing a matching legacy argv as a
/// difference.
#[tokio::test]
async fn hostile_and_malformed_runner_argv_annotations_are_ignored_and_surfaced() {
    let clean = desired_for(&backup());
    let matching = serde_json::to_string(&runner_argv(ExecutionTrigger::Manual, UID)).unwrap();
    for (value, reason, comparison) in [
        (
            r#"["restore","run","--spec","/tmp/attacker.yaml","--signing-key","/tmp/attacker-key.pem","--backup-id-override","attacker-id"]"#.to_string(),
            REASON_RUNNER_ARGV_ANNOTATION_IGNORED,
            "DIFFERS",
        ),
        ("{not an argv".to_string(), REASON_RUNNER_ARGV_ANNOTATION_MALFORMED, "not a JSON array"),
        (matching, REASON_RUNNER_ARGV_ANNOTATION_IGNORED, "equals"),
    ] {
        let mut annotated = backup();
        annotated
            .metadata
            .annotations
            .get_or_insert_with(Default::default)
            .insert(RUNNER_ARGV_ANNOTATION.to_string(), value.clone());
        let (client, _seen, bodies) =
            mock_client_recording_bodies(create_routes(201, existing_plan_config_map(UID)));
        let outcome = reconcile_backup(
            &annotated,
            &client,
            &unobserved_archive,
            &unverified_evidence,
            utc(2026, 11, 9, 3, 17),
        )
        .await
        .expect("an annotated Backup reconciles");
        assert!(outcome.created, "{value}: the run proceeds from the typed spec");
        let bodies = bodies.lock().expect("readable").clone();

        let cm = posted_config_map(&bodies);
        assert_eq!(
            cm["data"],
            serde_json::to_value(clean.documents().unwrap()).unwrap(),
            "{value}: the frozen inputs are the un-annotated Backup's, byte for byte"
        );
        let job = posted_job(&bodies).expect("the Job was POSTed");
        assert_eq!(
            job["spec"]["template"]["spec"]["containers"][0]["args"],
            serde_json::json!(clean.inputs.runner.args),
            "{value}: the executed argv is the derived one"
        );
        for token in ["attacker", "/tmp/", "restore"] {
            assert!(
                !job.to_string().contains(token) && !cm.to_string().contains(token),
                "{value}: `{token}` reached the Job or the plan"
            );
        }

        let statuses = patched_statuses(&bodies);
        let running = statuses.last().expect("a running status was written");
        let (status, got_reason, message) =
            condition_named(running, CONDITION_RUNNER_ARGV_ANNOTATION_IGNORED)
                .unwrap_or_else(|| panic!("{value}: the annotation is surfaced: {running}"));
        assert_eq!(status, "True");
        assert_eq!(got_reason, reason, "{value}");
        assert!(
            message.contains(&logweir_core::ids::sha256_prefixed(value.as_bytes()))
                && message.contains(&format!("{} bytes", value.len()))
                && message.contains(comparison),
            "{value}: the message names size, digest and the comparison: {message}"
        );
        assert!(
            !message.contains("attacker") && !message.contains("/tmp/"),
            "{value}: the annotation's content is never echoed: {message}"
        );
        assert_no_duplicate_condition_types(running, "the annotated running status");
    }
}

/// **A RESTART, OR A DELETED JOB, RE-CREATES THE JOB FROM THE SAME FROZEN
/// INPUTS** and writes nothing else: the recorded ConfigMap is read first and
/// verified, no ConfigMap is created or rewritten, the Job is exactly the Job
/// those inputs build, and a status that already says all of it is not
/// patched.
///
/// KILLS: re-rendering the plan on a retry; creating a second ConfigMap;
/// building the recreated Job from anything but the frozen inputs; a status
/// write per retry.
#[tokio::test]
async fn a_nonterminal_backup_whose_job_is_gone_recreates_it_from_the_frozen_inputs() {
    for base in [backup(), scheduled_backup()] {
        let stored = running_after_freeze(&base);
        let frozen = desired_for(&base);
        let routes = vec![
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
                method: "GET",
                path_suffix: "/configmaps/logweir-backup-nightly-20261109-031700-plan",
                status: 200,
                body: frozen_config_map(&base).to_string(),
            },
            // ROUTED SO THAT "NO SECOND CONFIGMAP" IS AN ASSERTION.
            Route {
                method: "POST",
                path_suffix: "/configmaps",
                status: 201,
                body: frozen_config_map(&base).to_string(),
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
                body: backup_json(),
            },
        ];
        let (client, _seen, bodies) = mock_client_recording_bodies(routes);
        let outcome = reconcile_backup(
            &stored,
            &client,
            &unobserved_archive,
            &unverified_evidence,
            utc(2026, 11, 9, 3, 30),
        )
        .await
        .expect("the retry reconciles");
        let bodies = bodies.lock().expect("readable").clone();
        assert!(
            outcome.created,
            "the missing Job is re-created: {:?}",
            calls(&bodies)
        );
        assert!(
            !bodies
                .iter()
                .any(|b| b.method == "POST" && path(&b.uri).ends_with("/configmaps")),
            "no second plan is created: {:?}",
            calls(&bodies)
        );
        assert!(!rewrote_a_config_map(&bodies));
        assert_eq!(
            posted_job(&bodies).expect("the Job was POSTed"),
            serde_json::to_value(
                runner_job(
                    &base,
                    &prod_cluster(),
                    &frozen,
                    &job::RunnerImage::default()
                )
                .unwrap()
            )
            .unwrap(),
            "the re-created Job is exactly the Job the frozen inputs build"
        );
        assert_eq!(
            status_patch_count(&bodies),
            0,
            "status.execution, the phase, the jobRef and JobCreated already say all of it: {:?}",
            calls(&bodies)
        );
    }
}

/// One mutation of a frozen plan: `(what it is, how to make it, whether the
/// recorded digest is the thing that differs, the cause the refusal must name)`.
type PlanCase = (&'static str, Box<dyn Fn(&mut Value)>, bool, &'static str);

/// One mutation of a `Backup` spec: `(what it is, how to make it)`.
type SpecCase = (&'static str, &'static str, Box<dyn Fn(&mut Value)>);

/// **AN EXISTING PLAN THAT IS NOT EXACTLY THIS BACKUP'S FROZEN INPUTS IS REFUSED
/// AND NEVER REWRITTEN** — on a `409` retry and on a restart that recorded
/// `status.execution` alike, before any Job exists.
///
/// KILLS: accepting ownership without content; accepting content without
/// exact ownership; adopting a legacy mutable plan; trusting a digest
/// annotation without recomputing it; trusting documents without re-rendering
/// them; combining a frozen plan with a changed connection; ignoring the
/// recorded digest.
#[tokio::test]
async fn an_existing_plan_that_is_not_this_backups_frozen_inputs_is_refused() {
    let good = frozen_config_map(&backup());
    let frozen = desired_for(&backup());
    let reencode = |cm: &mut Value, inputs: &BackupExecutionInputs| {
        let refrozen = FrozenInputs::freeze(inputs.clone()).unwrap();
        cm["data"] = serde_json::to_value(refrozen.documents().unwrap()).unwrap();
        cm["metadata"]["annotations"][INPUTS_SHA256_ANNOTATION] = refrozen.sha256.clone().into();
    };
    let cases: Vec<PlanCase> = vec![
        (
            "foreign owner UID",
            Box::new(|cm| {
                cm["metadata"]["ownerReferences"][0]["uid"] =
                    "00000000-dead-4000-8000-00000000beef".into();
            }),
            false,
            "a foreign or ownerless object",
        ),
        (
            "an extra owner",
            Box::new(|cm| {
                let extra = cm["metadata"]["ownerReferences"][0].clone();
                let mut extra = extra;
                extra["uid"] = "11111111-0000-4000-8000-000000000011".into();
                extra["controller"] = false.into();
                cm["metadata"]["ownerReferences"]
                    .as_array_mut()
                    .unwrap()
                    .push(extra);
            }),
            false,
            "exactly one, this Backup, is required",
        ),
        (
            "an owner without blockOwnerDeletion",
            Box::new(|cm| {
                cm["metadata"]["ownerReferences"][0]["blockOwnerDeletion"] = Value::Null;
            }),
            false,
            "complete controller reference",
        ),
        (
            "a mutable object",
            Box::new(|cm| {
                cm["immutable"] = false.into();
            }),
            false,
            "it is not immutable",
        ),
        (
            "a legacy mutable plan",
            Box::new(|cm| {
                cm["data"].as_object_mut().unwrap().remove(INPUTS_KEY);
                cm.as_object_mut().unwrap().remove("immutable");
                cm["metadata"]
                    .as_object_mut()
                    .unwrap()
                    .remove("annotations");
            }),
            false,
            "written by a controller that predates frozen execution inputs",
        ),
        (
            "a tampered snapshot under the old digest",
            Box::new(|cm| {
                let text = cm["data"][INPUTS_KEY]
                    .as_str()
                    .unwrap()
                    .replace("broker-0.prod", "evil-0.prod");
                cm["data"][INPUTS_KEY] = text.into();
            }),
            false,
            "annotation is not the digest",
        ),
        (
            "a tampered runner document",
            Box::new(|cm| {
                let text = cm["data"]["backup.yaml"]
                    .as_str()
                    .unwrap()
                    .replace("broker-0.prod", "evil-0.prod");
                cm["data"]["backup.yaml"] = text.into();
            }),
            false,
            "runner documents are not the documents rendered",
        ),
        (
            "binaryData",
            Box::new(|cm| {
                cm["binaryData"] = serde_json::json!({"extra": "AAAA"});
            }),
            false,
            "binaryData",
        ),
        (
            "a fourth key",
            Box::new(|cm| {
                cm["data"]["extra.json"] = "{}".into();
            }),
            false,
            "its keys are",
        ),
        (
            "a non-canonical snapshot with its own digest",
            Box::new(|cm| {
                let compact: Value =
                    serde_json::from_str(cm["data"][INPUTS_KEY].as_str().unwrap()).unwrap();
                let compact = compact.to_string();
                cm["metadata"]["annotations"][INPUTS_SHA256_ANNOTATION] =
                    logweir_core::ids::sha256_prefixed(compact.as_bytes()).into();
                cm["data"][INPUTS_KEY] = compact.into();
            }),
            false,
            "canonical encoding",
        ),
        (
            "another grammar version",
            Box::new(|cm| {
                let text = cm["data"][INPUTS_KEY]
                    .as_str()
                    .unwrap()
                    .replace(INPUTS_VERSION, "logweir.dev/backup-execution-inputs/v9");
                cm["data"][INPUTS_KEY] = text.into();
            }),
            false,
            "names grammar",
        ),
        (
            "the wrong execution id annotation",
            Box::new(|cm| {
                cm["metadata"]["annotations"][EXECUTION_ID_ANNOTATION] = "someone-else".into();
            }),
            false,
            "annotation is not the snapshot's execution id",
        ),
        (
            "the recorded digest differs",
            Box::new(|_| {}),
            true,
            "status.execution records",
        ),
    ];
    for (label, mutate, record_other_digest, cause) in cases {
        for recorded in [false, true] {
            if record_other_digest && !recorded {
                continue;
            }
            let mut existing = good.clone();
            mutate(&mut existing);
            let mut b = backup();
            if recorded {
                let mut execution = frozen.status();
                if record_other_digest {
                    execution.inputs_sha256 = "sha256:0000".to_string();
                }
                b = with_status_patch(&b, &serde_json::json!({"status": {"execution": execution}}));
            }
            let (client, _seen, bodies) =
                mock_client_recording_bodies(create_routes(409, existing.to_string()));
            let outcome = reconcile_backup(
                &b,
                &client,
                &unobserved_archive,
                &unverified_evidence,
                utc(2026, 11, 9, 3, 17),
            )
            .await
            .expect("a refusal is an outcome");
            let bodies = bodies.lock().expect("readable").clone();
            assert_eq!(
                outcome.terminal_state.as_deref(),
                Some(TERMINAL_STATE_PLAN_CONFIG_MAP_CONFLICT),
                "{label} (recorded={recorded}) is refused: {:?}",
                calls(&bodies)
            );
            assert!(posted_job(&bodies).is_none(), "{label}: ZERO Job POSTs");
            assert!(!rewrote_a_config_map(&bodies), "{label}: never rewritten");
            let statuses = patched_statuses(&bodies);
            let last = statuses.last().expect("the refusal is written");
            assert_eq!(last["phase"], "Failed", "{label}");
            let (_, reason, message) = condition_named(last, "Failed").expect("a Failed condition");
            assert_eq!(reason, TERMINAL_STATE_PLAN_CONFIG_MAP_CONFLICT, "{label}");
            assert!(
                message.contains(cause),
                "{label} (recorded={recorded}): the refusal names its cause `{cause}`: {message}"
            );
        }
    }

    // AND A PLAN THAT IS CONSISTENT WITH ITSELF BUT FROZE A DIFFERENT
    // CONNECTION: snapshot, documents and digest all re-rendered for another
    // bootstrap address. Owned, immutable, canonical — and still refused.
    let mut stale = good.clone();
    let mut moved = frozen.inputs.clone();
    moved.source.bootstrap_servers = vec!["previous-cluster:9093".to_string()];
    reencode(&mut stale, &moved);
    let (client, _seen, bodies) =
        mock_client_recording_bodies(create_routes(409, stale.to_string()));
    let outcome = reconcile_backup(
        &backup(),
        &client,
        &unobserved_archive,
        &unverified_evidence,
        utc(2026, 11, 9, 3, 17),
    )
    .await
    .unwrap();
    assert_eq!(
        outcome.terminal_state.as_deref(),
        Some(TERMINAL_STATE_PLAN_CONFIG_MAP_CONFLICT)
    );
    assert!(posted_job(&bodies.lock().unwrap()).is_none());
}

/// A plan that disappeared before its Job existed is re-created only when the
/// fresh resolution digests to exactly the recorded value.
///
/// KILLS: re-freezing different inputs under a recorded execution.
#[tokio::test]
async fn a_missing_frozen_plan_is_recreated_only_from_identical_inputs() {
    let frozen = desired_for(&backup());
    for (recorded_digest, expect_created) in [
        (frozen.sha256.clone(), true),
        ("sha256:0000".to_string(), false),
    ] {
        let mut execution = frozen.status();
        execution.inputs_sha256 = recorded_digest.clone();
        let b = with_status_patch(
            &backup(),
            &serde_json::json!({"status": {"execution": execution}}),
        );
        let mut routes = create_routes(201, existing_plan_config_map(UID));
        for r in &mut routes {
            if r.method == "GET" && r.path_suffix.ends_with("-plan") {
                r.status = 404;
                r.body = not_found_body("configmaps", &plan_config_map_name(NAME));
            }
        }
        let (client, _seen, bodies) = mock_client_recording_bodies(routes);
        let outcome = reconcile_backup(
            &b,
            &client,
            &unobserved_archive,
            &unverified_evidence,
            utc(2026, 11, 9, 3, 17),
        )
        .await
        .unwrap();
        let bodies = bodies.lock().unwrap().clone();
        let cm_posts = bodies
            .iter()
            .filter(|r| r.method == "POST" && path(&r.uri).ends_with("/configmaps"))
            .count();
        if expect_created {
            assert!(outcome.created, "{:?}", calls(&bodies));
            assert_eq!(cm_posts, 1, "the identical plan is re-created once");
        } else {
            assert_eq!(
                outcome.terminal_state.as_deref(),
                Some(TERMINAL_STATE_PLAN_CONFIG_MAP_CONFLICT)
            );
            assert_eq!(
                cm_posts, 0,
                "different inputs are never frozen under this record"
            );
            assert!(posted_job(&bodies).is_none());
        }
    }
}

/// **DUPLICATE CREATION.** A `409` on the Job create is admitted only when the
/// winner is controlled by exactly this `Backup`, and then nothing is created
/// twice; a foreign winner is refused, not adopted.
///
/// KILLS: treating every `409` as success; treating an owned winner as a
/// failure.
#[tokio::test]
async fn a_concurrent_job_create_is_admitted_only_for_this_backups_job() {
    let foreign = running_job_body().replace(UID, "00000000-dead-4000-8000-00000000beef");
    for (winner, created, refusal) in [
        (running_job_body(), false, None),
        (foreign, false, Some(TERMINAL_STATE_JOB_NAME_CONFLICT)),
    ] {
        let conflict = r#"{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"AlreadyExists","code":409}"#.to_string();
        let (client, seen) = sequenced_client(vec![
            (
                "GET",
                "/jobs/logweir-backup-nightly-20261109-031700",
                vec![
                    (404, not_found_body("jobs.batch", NAME)),
                    (200, winner.clone()),
                ],
            ),
            (
                "GET",
                "/kafkaclusters/prod",
                vec![(200, kafka_cluster_json())],
            ),
            (
                "POST",
                "/configmaps",
                vec![(201, existing_plan_config_map(UID))],
            ),
            ("POST", "/jobs", vec![(409, conflict)]),
            ("PATCH", "/status", vec![(200, backup_json())]),
        ]);
        let outcome = reconcile_backup(
            &backup(),
            &client,
            &unobserved_archive,
            &unverified_evidence,
            utc(2026, 11, 9, 3, 17),
        )
        .await
        .expect("an outcome");
        let seen = seen.lock().unwrap().clone();
        assert_eq!(outcome.created, created, "{:?}", calls(&seen));
        assert_eq!(
            outcome.terminal_state.as_deref(),
            refusal,
            "{:?}",
            calls(&seen)
        );
        assert_eq!(
            seen.iter()
                .filter(|r| r.method == "POST" && path(&r.uri).ends_with("/jobs"))
                .count(),
            1,
            "one create attempt, never a second"
        );
    }
}

/// **A JOB THIS BACKUP DOES NOT CONTROL IS NEVER OBSERVED OR ADOPTED** — no
/// owner, another `Backup` UID, a second owner, or `controller: false`. Its pod
/// is not listed and its log is not read, so a stranger's exit code and
/// evidence keys cannot reach this object.
///
/// KILLS: selecting the Job by name alone; lifting a foreign pod's exit code.
#[tokio::test]
async fn an_existing_job_this_backup_does_not_control_is_refused_not_observed() {
    let owned = job_body("Complete");
    let variants = [
        ("no owner", {
            let mut v: Value = serde_json::from_str(&owned).unwrap();
            v["metadata"]
                .as_object_mut()
                .unwrap()
                .remove("ownerReferences");
            v.to_string()
        }),
        (
            "another Backup UID",
            owned.replace(UID, "00000000-dead-4000-8000-00000000beef"),
        ),
        ("a second owner", {
            let mut v: Value = serde_json::from_str(&owned).unwrap();
            let mut extra = v["metadata"]["ownerReferences"][0].clone();
            extra["uid"] = "22222222-0000-4000-8000-000000000022".into();
            extra["controller"] = false.into();
            v["metadata"]["ownerReferences"]
                .as_array_mut()
                .unwrap()
                .push(extra);
            v.to_string()
        }),
        ("controller: false", {
            let mut v: Value = serde_json::from_str(&owned).unwrap();
            v["metadata"]["ownerReferences"][0]["controller"] = false.into();
            v.to_string()
        }),
    ];
    for (label, body) in variants {
        let mut routes = finished_routes(
            &pod_list_terminated(0),
            log_body(&i7_tail()),
            200,
            "Complete",
        );
        routes[0].body = body;
        let (client, _seen, bodies) = mock_client_recording_bodies(routes);
        let outcome = reconcile_backup(
            &frozen_backup(),
            &client,
            &unobserved_archive,
            &unverified_evidence,
            utc(2026, 11, 9, 3, 20),
        )
        .await
        .expect("a refusal is an outcome");
        let bodies = bodies.lock().unwrap().clone();
        assert_eq!(
            outcome.terminal_state.as_deref(),
            Some(TERMINAL_STATE_JOB_NAME_CONFLICT),
            "{label}"
        );
        assert_eq!(outcome.exit_code, None, "{label}: no exit code is lifted");
        assert!(
            !bodies
                .iter()
                .any(|r| path(&r.uri).ends_with("/pods") || path(&r.uri).ends_with("/log")),
            "{label}: the foreign Job's pod is never read: {:?}",
            calls(&bodies)
        );
        assert!(
            !bodies
                .iter()
                .any(|r| path(&r.uri).contains("/jobs") && r.method != "GET"),
            "{label}: the foreign Job is never created, patched or deleted"
        );
        let statuses = patched_statuses(&bodies);
        assert_eq!(statuses.len(), 1, "{label}");
        assert_eq!(
            condition_named(&statuses[0], "Failed").map(|c| c.1),
            Some(TERMINAL_STATE_JOB_NAME_CONFLICT.to_string())
        );
    }
}

/// **AN IN-FLIGHT LEGACY BACKUP IS OBSERVED UNCHANGED.** A Job an older
/// controller created from the runner-argv annotation — no inputs digest, no
/// `status.execution` — keeps running and finishing exactly as before: no plan
/// is read, created or rewritten, the Job is not re-created or changed (only
/// the TTL after the terminal status, as always), the status reports
/// `ExecutionInputsUnverified=True LegacyExecution`, and the terminal backup id
/// is the one the older controller would have reported.
///
/// KILLS: re-deriving or re-freezing a legacy run in flight; re-executing its
/// annotation; hiding that its inputs were never frozen; changing its reported
/// backup id.
#[tokio::test]
async fn an_in_flight_legacy_job_is_observed_unchanged_and_reported() {
    let legacy_argv = r#"["backup","run","--spec","/plan/backup.yaml","--allowed-clusters","/plan/allowed-clusters.json","--signing-key","/signing/key.pem","--receipt-out","/work/receipt.json","--triggered-by","manual"]"#;
    let mut legacy = backup();
    legacy
        .metadata
        .annotations
        .get_or_insert_with(Default::default)
        .insert(RUNNER_ARGV_ANNOTATION.to_string(), legacy_argv.to_string());
    let legacy_job = |condition: Option<&str>| {
        let mut v: Value = serde_json::from_str(&match condition {
            Some(c) => job_body(c),
            None => running_job_body(),
        })
        .unwrap();
        v["metadata"].as_object_mut().unwrap().remove("annotations");
        v.to_string()
    };

    // RUNNING.
    let mut routes = running_routes();
    routes[0].body = legacy_job(None);
    let (client, _seen, bodies) = mock_client_recording_bodies(routes);
    reconcile_backup(
        &legacy,
        &client,
        &unobserved_archive,
        &unverified_evidence,
        utc(2026, 11, 9, 3, 20),
    )
    .await
    .expect("a legacy running Job reconciles");
    let bodies = bodies.lock().unwrap().clone();
    assert_eq!(
        calls(&bodies)
            .iter()
            .map(|(m, _)| m.as_str())
            .collect::<Vec<_>>(),
        vec!["GET", "GET", "GET", "PATCH"],
        "the Job is read, its owned pod is listed, its progress tail is read and the status is \
         patched — and NOTHING else. No plan read, no plan write, no Job write. The three GETs \
         are D3 §2.3's observation (added at D3 W2); the ONE write is still this object's own \
         status: {:?}",
        calls(&bodies)
    );
    assert!(
        calls(&bodies)
            .iter()
            .all(|(m, p)| m != "PATCH" || p.contains("/backups/")),
        "and in particular the legacy Job's pod template is never rewritten: {:?}",
        calls(&bodies)
    );
    let running = &patched_statuses(&bodies)[0];
    let (status, reason, message) =
        condition_named(running, CONDITION_EXECUTION_INPUTS_UNVERIFIED).expect("reported");
    assert_eq!(
        (status.as_str(), reason.as_str()),
        ("True", REASON_LEGACY_EXECUTION)
    );
    assert!(message.contains("never changed"), "{message}");
    assert_eq!(
        condition_named(running, CONDITION_RUNNER_ARGV_ANNOTATION_IGNORED),
        None,
        "this controller did not ignore or execute anything for a Job it did not create"
    );
    assert!(
        running.get("execution").is_none(),
        "nothing is re-frozen for a Job in flight"
    );

    // FINISHED.
    let mut routes = finished_routes(
        &pod_list_terminated(0),
        log_body(&i7_tail()),
        200,
        "Complete",
    );
    routes[0].body = legacy_job(Some("Complete"));
    let (client, seen) = mock_client_recording(routes);
    let outcome = reconcile_backup(
        &legacy,
        &client,
        &unobserved_archive,
        &unverified_evidence,
        utc(2026, 11, 9, 3, 25),
    )
    .await
    .expect("a legacy finished Job reconciles");
    assert_eq!(outcome.exit_code, Some(0));
    let seen = seen.lock().unwrap().clone();
    assert!(
        !seen.iter().any(|r| path(&r.uri).contains("/configmaps")),
        "no plan is touched: {seen:?}"
    );
    assert_eq!(
        seen.iter()
            .filter(|r| path(&r.uri).contains("/jobs/") && r.method == "PATCH")
            .count(),
        1,
        "the legacy Job gets exactly the TTL patch it always got"
    );
    assert!(!seen
        .iter()
        .any(|r| r.method == "POST" || r.method == "DELETE"));
    let terminal = weirkeeper::controllers::backup::finished_status_patch(
        &legacy,
        0,
        &evidence_keys(&log_body(&i7_tail())),
        None,
        None,
        None,
        None,
        utc(2026, 11, 9, 3, 25),
    );
    assert_eq!(
        terminal["status"]["backupId"],
        weirkeeper::backup_execution::legacy_backup_id(&legacy),
        "the legacy run reports the id the older controller would have"
    );
}

/// A Job whose digest does not match `status.execution` — for example one an
/// older controller created after a rollback — is reported and left alone.
///
/// KILLS: accepting any annotated Job as frozen; accepting a missing digest.
#[tokio::test]
async fn a_job_that_does_not_carry_the_recorded_inputs_is_reported() {
    for job in [
        {
            let mut v: Value = serde_json::from_str(&running_job_body()).unwrap();
            v["metadata"].as_object_mut().unwrap().remove("annotations");
            v.to_string()
        },
        running_job_body().replace(FIXTURE_INPUTS_SHA256, "sha256:another"),
    ] {
        let mut routes = running_routes();
        routes[0].body = job;
        let (client, _seen, bodies) = mock_client_recording_bodies(routes);
        reconcile_backup(
            &frozen_backup(),
            &client,
            &unobserved_archive,
            &unverified_evidence,
            utc(2026, 11, 9, 3, 20),
        )
        .await
        .unwrap();
        let bodies = bodies.lock().unwrap().clone();
        let running = &patched_statuses(&bodies)[0];
        assert_eq!(
            condition_named(running, CONDITION_EXECUTION_INPUTS_UNVERIFIED).map(|c| c.1),
            Some(REASON_JOB_INPUTS_MISMATCH.to_string()),
            "{running}"
        );
        assert!(!bodies
            .iter()
            .any(|r| r.method == "POST" || r.method == "DELETE"));
    }
}

/// **THE TYPED SPEC STATES THE IDENTITY, OR NOTHING RUNS.** A hand-written
/// Backup that claims a schedule it is not owned by, a manual Backup naming a
/// slot, an unknown trigger, a scheduled object under the wrong name, and a
/// non-positive deadline are refused before anything is created — and a
/// hostile annotation on such a Backup is still surfaced.
///
/// **THE TERMINAL STATE IS PER ROW, AND THE VOCABULARY MOVED (D1 §3.4).**
/// PLAT-06.1 answered every one of these `ExecutionSpecInvalid`; D1 §3.1 gives
/// a run's IDENTITY its own refusal, `ScheduledIdentityMismatch`, and keeps
/// `ExecutionSpecInvalid` for what is wrong with the spec as an EXECUTION
/// request — an unknown `triggeredBy`, a deadline a Job cannot carry. An
/// operator reading `ScheduledIdentityMismatch` is told the object's own fields
/// do not compose the run it claims to be; one reading `ExecutionSpecInvalid`
/// is told a value is out of range. Every row is still terminal and still
/// refused before any `POST`, which is the property that matters.
///
/// KILLS: silently running a false `schedule` claim as manual (its receipt
/// would say `schedule`); a scheduled identity from a non-schedule owner;
/// accepting a slot that is fifteen digits and not a date; letting an unknown
/// `triggeredBy` through as manual now that the trigger kind is a field.
#[tokio::test]
async fn a_spec_that_states_no_runnable_identity_is_refused_before_any_post() {
    let mutations: Vec<SpecCase> = vec![
        (
            "schedule trigger without an owner",
            TERMINAL_STATE_SCHEDULED_IDENTITY_MISMATCH,
            Box::new(|v| {
                v["spec"]["triggeredBy"] = "schedule".into();
                v["spec"]["scheduleRef"] = serde_json::json!({"name": "nightly"});
                v["spec"]["slot"] = "20261109-031700".into();
            }),
        ),
        (
            "schedule owner of another kind",
            TERMINAL_STATE_SCHEDULED_IDENTITY_MISMATCH,
            Box::new(|v| {
                v["spec"]["triggeredBy"] = "schedule".into();
                v["spec"]["scheduleRef"] = serde_json::json!({"name": "nightly"});
                v["spec"]["slot"] = "20261109-031700".into();
                v["metadata"]["ownerReferences"] = serde_json::json!([{
                    "apiVersion": "logweir.dev/v1alpha1", "kind": "Restore", "name": "nightly",
                    "uid": SCHEDULE_UID, "controller": true
                }]);
            }),
        ),
        (
            "schedule under another name",
            TERMINAL_STATE_SCHEDULED_IDENTITY_MISMATCH,
            Box::new(|v| {
                v["spec"]["triggeredBy"] = "schedule".into();
                v["spec"]["scheduleRef"] = serde_json::json!({"name": "hourly"});
                v["spec"]["slot"] = "20261109-031700".into();
                v["metadata"]["ownerReferences"] = serde_json::json!([{
                    "apiVersion": "logweir.dev/v1alpha1", "kind": "BackupSchedule", "name": "hourly",
                    "uid": SCHEDULE_UID, "controller": true
                }]);
            }),
        ),
        (
            "schedule with a malformed slot",
            TERMINAL_STATE_SCHEDULED_IDENTITY_MISMATCH,
            Box::new(|v| {
                v["spec"]["triggeredBy"] = "schedule".into();
                v["spec"]["scheduleRef"] = serde_json::json!({"name": "nightly"});
                v["spec"]["slot"] = "20261309-031700".into();
                v["metadata"]["ownerReferences"] = serde_json::json!([{
                    "apiVersion": "logweir.dev/v1alpha1", "kind": "BackupSchedule", "name": "nightly",
                    "uid": SCHEDULE_UID, "controller": true
                }]);
            }),
        ),
        (
            "manual with a slot",
            TERMINAL_STATE_SCHEDULED_IDENTITY_MISMATCH,
            Box::new(|v| {
                v["spec"]["slot"] = "20261109-031700".into();
            }),
        ),
        (
            "an unknown trigger",
            TERMINAL_STATE_EXECUTION_SPEC_INVALID,
            Box::new(|v| {
                v["spec"]["triggeredBy"] = "--spec".into();
            }),
        ),
        (
            "a zero deadline",
            TERMINAL_STATE_EXECUTION_SPEC_INVALID,
            Box::new(|v| {
                v["spec"]["deadlineSeconds"] = 0.into();
            }),
        ),
    ];
    for (label, expected, mutate) in mutations {
        let mut v: Value = serde_json::from_str(&backup_json()).unwrap();
        mutate(&mut v);
        v["metadata"]["annotations"] =
            serde_json::json!({ RUNNER_ARGV_ANNOTATION: "[\"backup\"]" });
        let b: Backup = serde_json::from_value(v).unwrap();
        assert!(
            label == "a zero deadline" || execution_identity(&b).is_err(),
            "{label}: no identity is derived"
        );
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
        .expect("a refusal is an outcome");
        let bodies = bodies.lock().unwrap().clone();
        assert_eq!(
            outcome.terminal_state.as_deref(),
            Some(expected),
            "{label}: {:?}",
            calls(&bodies)
        );
        assert!(
            !bodies.iter().any(|r| r.method == "POST"),
            "{label}: nothing is created"
        );
        let refused = patched_statuses(&bodies)
            .pop()
            .expect("the refusal is written");
        assert_eq!(refused["phase"], "Failed");
        assert_eq!(
            condition_named(&refused, CONDITION_RUNNER_ARGV_ANNOTATION_IGNORED).map(|c| c.0),
            Some("True".to_string()),
            "{label}: the annotation on a refused Backup is surfaced too: {refused}"
        );
        assert_no_duplicate_condition_types(&refused, label);
    }
}

/// **THE MANUAL CONTRACT PLAT-06.2 BUILDS ON.** A manual run IS its object's
/// UID: the same object always derives the same identity (so re-creating a
/// name that exists is the API server's `AlreadyExists` and no second run),
/// a deliberately new name — or a deleted-and-recreated name — is a new UID and
/// therefore a new run.
#[test]
fn a_manual_identity_is_the_object_uid_and_nothing_else() {
    let first = execution_identity(&backup()).unwrap();
    assert_eq!(first, execution_identity(&backup()).unwrap());
    assert_eq!(first.id, UID);

    let renamed: Backup = serde_json::from_str(
        &backup_json()
            .replace(NAME, "backup-now-2")
            .replace(UID, "5b0c0d0e-0000-4000-8000-0000000000e2"),
    )
    .unwrap();
    let recreated: Backup =
        serde_json::from_str(&backup_json().replace(UID, "5b0c0d0e-0000-4000-8000-0000000000e3"))
            .unwrap();
    for other in [renamed, recreated] {
        let identity = execution_identity(&other).unwrap();
        assert_ne!(identity.id, first.id, "a new object is a new run");
        assert_eq!(identity.id, other.metadata.uid.clone().unwrap());
    }

    // Annotations, labels and generation are not identity.
    let mut decorated = backup();
    decorated.metadata.labels = Some(
        [("team".to_string(), "x".to_string())]
            .into_iter()
            .collect(),
    );
    decorated.metadata.generation = Some(99);
    decorated
        .metadata
        .annotations
        .get_or_insert_with(Default::default)
        .insert(
            RUNNER_ARGV_ANNOTATION.to_string(),
            "[\"--backup-id-override\",\"x\"]".to_string(),
        );
    assert_eq!(execution_identity(&decorated).unwrap(), first);
}

/// A re-created Job must not read its predecessor's pod, and NOTHING the label
/// alone selects is read — D-SEAMS **S6**, defect `SEC-PODLOG`.
///
/// This is the pure half of the rule the three execution controllers now share
/// with the check framework ([`weirkeeper::check::pod`]). The controller half,
/// over the double and asserting the ABSENCE of a `pods/log` request, is
/// [`a_labelled_pod_this_job_does_not_own_is_never_read`].
///
/// KILLS: taking the first pod the selector returns; matching an owner
/// reference of any kind; matching a non-controller owner reference; matching
/// a `Job` in another API group; falling back to an ownerless pod when nothing
/// is owned (the shape this file's previous version of this test ASSERTED,
/// and which the `plat06` review recorded as L2).
#[test]
fn a_recreated_job_reads_only_its_own_pod() {
    let stale = owner_pod("old", Some(("batch/v1", "Job", "old-job-uid", true)), None);
    let mine = owner_pod("new", Some(("batch/v1", "Job", "new-job-uid", true)), None);

    let listed = [stale.clone(), mine.clone()];
    let seen = cpod::claimants(&listed, "new-job-uid");
    assert_eq!(
        seen.owned.and_then(|p| p.metadata.name.clone()),
        Some("new".to_string()),
        "the pod this Job owns, and not the first one the selector returned"
    );
    assert!(
        seen.contested.is_empty(),
        "one claimant is not a contest — the predecessor's pod claims the OLD Job"
    );
    assert_eq!(
        seen.foreign
            .iter()
            .filter_map(|p| p.metadata.name.clone())
            .collect::<Vec<_>>(),
        vec!["old".to_string()],
        "and the predecessor's pod is REPORTED as ignored, not silently dropped"
    );

    // Each way of not being this Job's pod, one at a time, alone in the list.
    for (label, impostor) in [
        (
            "the deleted Job's pod is nobody's evidence for the new Job",
            owner_pod("old", Some(("batch/v1", "Job", "old-job-uid", true)), None),
        ),
        (
            "an ownerless pod wearing the label is a pod somebody created by hand",
            owner_pod("ownerless", None, None),
        ),
        (
            "a NON-controller owner reference is an association somebody else made",
            owner_pod(
                "associated",
                Some(("batch/v1", "Job", "new-job-uid", false)),
                None,
            ),
        ),
        (
            "a ReplicaSet that happens to carry the UID string is not this Job",
            owner_pod(
                "replicaset-owned",
                Some(("batch/v1", "ReplicaSet", "new-job-uid", true)),
                None,
            ),
        ),
        (
            "a `Job` IN ANOTHER API GROUP is not a batch/v1 Job (review finding R3)",
            owner_pod(
                "volcano-owned",
                Some(("volcano.sh/v1alpha1", "Job", "new-job-uid", true)),
                None,
            ),
        ),
    ] {
        let listed = [impostor];
        let seen = cpod::claimants(&listed, "new-job-uid");
        assert!(seen.owned.is_none(), "{label}");
        assert!(
            seen.contested.is_empty(),
            "{label}: one pod is not a contest"
        );
        assert_eq!(seen.foreign.len(), 1, "{label}: and it is reported");
    }
}

/// **Two pods claiming one Job is a REFUSAL, not a ranking** — review finding
/// **R1**.
///
/// An `ownerReference` is ordinary metadata written by whoever creates the
/// pod: the API server does not check that the owner exists, that the UID is
/// right, or that the creator may claim it. So a tenant who can read the Job's
/// `metadata.uid` can mint a pod that passes all four conditions — and, being
/// created after the genuine runner pod, it is BY CONSTRUCTION the newer one.
/// An earlier version of this module handed that case to `newest`, which
/// handed the read to the planter every single time, silently, with no
/// `ForeignPodIgnored` line because the pod passed.
///
/// `backoffLimit: 0` plus `restartPolicy: Never` means the job controller
/// cannot produce a second pod for one Job, so a second claimant is
/// illegitimate by construction and the only safe answer is to read neither.
///
/// KILLS: newest-wins restored; oldest-wins (the mirror bug — a planter who
/// creates their pod before the Job wins that one); returning one claimant and
/// reporting the other as foreign; dropping the claimants from the report;
/// letting a pod that FAILED the owner check contest the Job.
#[test]
fn two_pods_claiming_one_job_are_both_refused() {
    let owner = Some(("batch/v1", "Job", JOB_UID, true));
    let genuine = owner_pod("run-genuine", owner, Some("2026-11-09T03:17:00Z"));
    let forged = owner_pod("run-forged", owner, Some("2026-11-09T03:19:00Z"));

    for (label, listed) in [
        ("forged last", vec![genuine.clone(), forged.clone()]),
        ("forged first", vec![forged.clone(), genuine.clone()]),
    ] {
        let seen = cpod::claimants(&listed, JOB_UID);
        assert!(
            seen.owned.is_none(),
            "{label}: NOTHING is read when the Job is contested — the forged pod is the newer \
             one by construction, so ranking hands the read to whoever planted it"
        );
        let mut named: Vec<String> = seen
            .contested
            .iter()
            .filter_map(|p| p.metadata.name.clone())
            .collect();
        named.sort();
        assert_eq!(
            named,
            vec!["run-forged".to_string(), "run-genuine".to_string()],
            "{label}: BOTH claimants are reported. Which of them is the real runner pod is \
             exactly what this controller cannot tell, so naming one would suggest otherwise"
        );
        assert!(
            seen.foreign.is_empty(),
            "{label}: a rival claimant is not a `foreign` pod — it passed the owner check, and \
             that is the point"
        );
    }

    // ONE claimant plus any number of strangers is still the ordinary case.
    let with_strangers = [
        genuine.clone(),
        owner_pod("ownerless", None, Some("2026-11-09T03:20:00Z")),
        owner_pod(
            "other-job",
            Some(("batch/v1", "Job", "another-uid", true)),
            Some("2026-11-09T03:21:00Z"),
        ),
    ];
    let seen = cpod::claimants(&with_strangers, JOB_UID);
    assert_eq!(
        seen.owned.and_then(|p| p.metadata.name.clone()),
        Some("run-genuine".to_string()),
        "a pod that FAILS the owner check is not a claimant, so it cannot contest the Job — \
         otherwise anyone able to set the LABEL could shut every run in the namespace down"
    );
    assert!(seen.contested.is_empty());
    assert_eq!(seen.foreign.len(), 2);
}

/// `newest` is still a total order, and still exercised — it is no longer what
/// resolves a contested Job (see [`two_pods_claiming_one_job_are_both_refused`])
/// but it is the documented answer for a caller whose Job legitimately owns
/// several pods.
///
/// KILLS: picking the oldest; a same-second tie resolved the other way; a
/// timestamp-less pod winning a tie-break.
#[test]
fn newest_is_a_total_order_over_creation_time_then_name() {
    let owner = Some(("batch/v1", "Job", JOB_UID, true));
    let first = owner_pod("run-aaaaa", owner, Some("2026-11-09T03:17:00Z"));
    let replacement = owner_pod("run-zzzzz", owner, Some("2026-11-09T03:18:00Z"));

    for listed in [vec![&first, &replacement], vec![&replacement, &first]] {
        assert_eq!(
            cpod::newest(&listed).and_then(|p| p.metadata.name.clone()),
            Some("run-zzzzz".to_string()),
            "the NEWEST by creationTimestamp, whichever order the listing arrived in"
        );
    }

    // A `Time` is second-granular, so two pods created in the same second is
    // not exotic; the name breaks the tie and the answer is still one value.
    let a = owner_pod("run-aaaaa", owner, Some("2026-11-09T03:17:00Z"));
    let b = owner_pod("run-bbbbb", owner, Some("2026-11-09T03:17:00Z"));
    assert_eq!(
        cpod::newest(&[&a, &b]).and_then(|p| p.metadata.name.clone()),
        Some("run-bbbbb".to_string()),
        "the lexically greatest name among pods of the same second — any total order would do, \
         but it has to BE one"
    );

    // And a pod with no creationTimestamp never wins against one that has it.
    let undated = owner_pod("run-zzzzzz-undated", owner, None);
    assert_eq!(
        cpod::newest(&[&undated, &first]).and_then(|p| p.metadata.name.clone()),
        Some("run-aaaaa".to_string()),
        "the API server always sets creationTimestamp, so its absence means a fabricated object"
    );
}

/// A hostile annotation on a RUNNING typed Backup is surfaced once, and a steady
/// pass over that object writes nothing.
///
/// KILLS: a `lastTransitionTime` that moves on every pass; dropping the
/// condition from the running patch.
#[tokio::test]
async fn a_surfaced_annotation_on_a_running_backup_is_steady() {
    let mut annotated = frozen_backup();
    annotated
        .metadata
        .annotations
        .get_or_insert_with(Default::default)
        .insert(
            RUNNER_ARGV_ANNOTATION.to_string(),
            "[\"restore\"]".to_string(),
        );
    let (client, _seen, bodies) = mock_client_recording_bodies(running_routes());
    reconcile_backup(
        &annotated,
        &client,
        &unobserved_archive,
        &unverified_evidence,
        utc(2026, 11, 9, 3, 20),
    )
    .await
    .unwrap();
    let first = bodies.lock().unwrap().clone();
    let patch = serde_json::from_str::<Value>(
        &first
            .iter()
            .find(|b| b.method == "PATCH")
            .expect("the first pass surfaces the annotation")
            .body,
    )
    .unwrap();
    assert!(condition_named(&patch["status"], CONDITION_RUNNER_ARGV_ANNOTATION_IGNORED).is_some());
    let steady = with_status_patch(&annotated, &patch);

    // ONE REQUEUE LATER, and INSIDE the 60 s heartbeat window (D3 §2.2): a
    // second pass at exactly +60 s would legitimately rewrite
    // `progress.lastObservedTime`, which is a different rule and has its own
    // row in `a_steady_backup_issues_no_second_status_patch`.
    let (client, _seen, bodies) = mock_client_recording_bodies(running_routes());
    reconcile_backup(
        &steady,
        &client,
        &unobserved_archive,
        &unverified_evidence,
        utc(2026, 11, 9, 3, 20) + chrono::Duration::seconds(15),
    )
    .await
    .unwrap();
    assert_eq!(status_patch_count(&bodies.lock().unwrap()), 0);
}

// ---------------------------------------------------------------------------
// PLAT-06.1 × PLAT-07.1: the frozen inputs and the ONE connection resolver
// ---------------------------------------------------------------------------

/// The `execution-inputs.json` bytes the controller froze for [`backup`] and
/// [`prod_cluster`] **before the saved-connection contract was merged**,
/// captured from `main 10f6c28` (PLAT-06.1, pre-rebase) and stored as bytes.
///
/// A LITERAL, NOT A RE-DERIVATION. A fixture regenerated from the build it is
/// supposed to constrain proves nothing about compatibility; these bytes are
/// what the older controller actually wrote.
const PRE_CONTRACT_INPUTS: &str =
    include_str!("fixtures/backup_execution/pre-connection-contract-inputs.json");

/// A `KafkaCluster` that is [`kafka_cluster_json`] plus an `auth.tlsCa`.
fn cluster_with_ca(ca: Value) -> String {
    let mut value: Value =
        serde_json::from_str(&kafka_cluster_json()).expect("the fixture is JSON");
    value["spec"]["auth"]["tlsCa"] = ca;
    value.to_string()
}

/// [`create_routes`] answering the `KafkaCluster` GET with `cluster_json`.
fn create_routes_for(
    configmap_status: u16,
    existing_configmap: String,
    cluster_json: String,
) -> Vec<Route> {
    let mut routes = create_routes(configmap_status, existing_configmap);
    for route in &mut routes {
        if route.path_suffix == "/kafkaclusters/prod" {
            route.body = cluster_json.clone();
        }
    }
    routes
}

/// A plan ConfigMap carrying exactly `snapshot` as its `execution-inputs.json`,
/// with the two runner documents rendered from it and both annotations
/// recomputed — the object an older controller would have left behind.
fn config_map_around(snapshot: &str) -> Value {
    let parsed: BackupExecutionInputs =
        serde_json::from_str(snapshot).expect("the snapshot parses under this grammar");
    let frozen = FrozenInputs::freeze(parsed).expect("it freezes");
    let mut cm = frozen_config_map(&backup());
    cm["data"] = serde_json::to_value(frozen.documents().expect("documents render")).unwrap();
    cm["metadata"]["annotations"][INPUTS_SHA256_ANNOTATION] =
        serde_json::json!(frozen.sha256.clone());
    cm["metadata"]["annotations"][EXECUTION_ID_ANNOTATION] =
        serde_json::json!(frozen.inputs.execution.id.clone());
    cm
}

/// Reconcile [`backup`] with a `409` that hands back `existing`, against
/// `cluster_json`.
async fn reconcile_against(
    existing: &Value,
    cluster_json: String,
) -> (Option<String>, Vec<SeenBody>) {
    let (client, _seen, bodies) =
        mock_client_recording_bodies(create_routes_for(409, existing.to_string(), cluster_json));
    let outcome = reconcile_backup(
        &backup(),
        &client,
        &unobserved_archive,
        &unverified_evidence,
        utc(2026, 11, 9, 3, 17),
    )
    .await
    .expect("a refusal is an outcome");
    let bodies = bodies.lock().expect("readable").clone();
    (outcome.terminal_state, bodies)
}

/// **A PLAN FROZEN BEFORE THE SAVED-CONNECTION CONTRACT IS STILL ADMITTED,
/// UNCHANGED** — grammar `…/backup-execution-inputs/v1` did not fork when
/// PLAT-07.1 added the CA reference to the source block.
///
/// The CA reference is `skip_serializing_if = "Option::is_none"`, so a
/// connection that names no CA freezes the same bytes it always did: the older
/// controller's snapshot parses, re-encodes to ITSELF, renders the same two
/// runner documents, and the run continues into its Job.
///
/// **AND IT IS THE `v1` GRAMMAR, WHICH `v2` DID NOT REPLACE** — D1 §3.3,
/// D-SEAMS **S4**. The same fixture is the `v1` compatibility row: a document
/// written under `logweir.dev/backup-execution-inputs/v1` loads, re-encodes to
/// its own bytes, verifies against a fresh `v2` resolution through
/// [`BackupExecutionInputs::as_version_v1`], and its run continues into its
/// Job. THE MUTANT THIS KILLS IS THE CHEAP ONE: dropping `v1` from the
/// accepted grammars, or comparing a stored `v1` document against an
/// undowngraded `v2` resolution, turns every `Backup` in flight at the upgrade
/// into a terminal `PlanConfigMapConflict` at its next pass — silently, and
/// for the whole cluster at once.
///
/// KILLS: bumping the grammar version for an additive optional field;
/// serialising `tlsCa: null` into a no-CA snapshot; reading only the grammar
/// this controller writes; comparing a `v1` plan against `v2` fields it could
/// not have carried.
#[tokio::test]
async fn a_plan_frozen_before_the_connection_contract_is_still_admitted() {
    // The fixture really is the older grammar: no CA key at all.
    assert!(
        !PRE_CONTRACT_INPUTS.contains("tlsCa"),
        "the pre-contract fixture names no CA; it was captured before the field existed"
    );
    let parsed: BackupExecutionInputs = serde_json::from_str(PRE_CONTRACT_INPUTS)
        .expect("the older snapshot parses under the merged grammar");
    assert_eq!(
        parsed.version, INPUTS_VERSION_V1,
        "the fixture is the v1 grammar, and v1 is still one of the two this controller READS"
    );
    assert!(
        INPUTS_VERSIONS_READ.contains(&INPUTS_VERSION_V1),
        "v1 is accepted: {INPUTS_VERSIONS_READ:?}"
    );
    assert_eq!(
        INPUTS_VERSION, INPUTS_VERSION_V2,
        "and v2 is the one it WRITES"
    );
    assert_eq!(
        FrozenInputs::freeze(parsed).expect("it freezes").canonical,
        PRE_CONTRACT_INPUTS,
        "the merged controller re-encodes the older snapshot to the SAME BYTES; anything else \
         is a digest that no longer matches its annotation and a run that cannot continue"
    );

    // And the v1 VIEW of what this controller freezes now is byte-identical to
    // it: every v2 block is additive, so the fields a v1 plan actually froze
    // are exactly the fields this resolution still produces.
    let now = desired_for(&backup());
    assert_eq!(
        canonical_inputs(&now.inputs.as_version_v1()).expect("the v1 view encodes"),
        PRE_CONTRACT_INPUTS,
        "a connection that names no CA freezes exactly what it froze before PLAT-07.1, and the \
         v2 blocks are additions and not rewrites"
    );
    assert_ne!(
        now.inputs.version, INPUTS_VERSION_V1,
        "the resolution itself is v2"
    );

    // The whole pass: the older object is admitted and the Job is created.
    let (terminal, bodies) = reconcile_against(
        &config_map_around(PRE_CONTRACT_INPUTS),
        kafka_cluster_json(),
    )
    .await;
    assert_eq!(terminal, None, "no refusal: {:?}", calls(&bodies));
    let job = posted_job(&bodies).expect("the Job is created from the older frozen inputs");
    assert_eq!(
        job["spec"]["template"]["spec"]["containers"][0]["args"],
        serde_json::json!(runner_argv(ExecutionTrigger::Manual, UID)),
        "the Job runs the argv the older snapshot froze"
    );
    assert!(
        !rewrote_a_config_map(&bodies),
        "the older plan is never rewritten"
    );
}

/// **A SOURCE `KafkaCluster` WITH `auth.tlsCa` FREEZES THE CA REFERENCE THE
/// RESOLVER DECIDED, AND A CHANGED REFERENCE IS A CONFLICT** — PLAT-07.1's
/// projection is inside PLAT-06.1's comparison, not beside it.
///
/// The Job's CA mount is not an extra the Job builder adds after the fact: the
/// object, the key and the kind are in the immutable snapshot, so a `tlsCa`
/// edited after the freeze cannot be combined with approved plan bytes. Without
/// this the controller would mount a CA nobody froze — the run would verify the
/// broker against a root the plan was never approved for, silently.
///
/// KILLS: leaving `tls_ca` out of `SourceInputs`; clearing it in
/// `BackupExecutionInputs::executable()`; comparing only the CA's name.
#[tokio::test]
async fn a_source_cluster_with_a_private_ca_freezes_its_reference_and_a_change_is_a_conflict() {
    let ca = serde_json::json!({ "configMapKeyRef": { "name": "kafka-ca", "key": "ca.crt" } });
    let cluster_json = cluster_with_ca(ca);
    let cluster: KafkaCluster = serde_json::from_str(&cluster_json).expect("still a KafkaCluster");
    let frozen = desired_execution_inputs(&backup(), &cluster).expect("it resolves");

    // THE SNAPSHOT NAMES THE REFERENCE, and still names no credential.
    let snapshot: Value = serde_json::from_str(&frozen.canonical).expect("the snapshot is JSON");
    assert_eq!(
        snapshot["source"]["tlsCa"],
        serde_json::json!({ "kind": "configMap", "name": "kafka-ca", "key": "ca.crt" }),
        "the frozen inputs carry the CA reference the ONE resolver decided"
    );
    assert_eq!(
        frozen.inputs.executable().source.tls_ca,
        frozen.inputs.source.tls_ca,
        "and it is EXECUTABLE — unlike `observedClusterId`, a CA reference changes what the run \
         trusts, so `verify_frozen_config_map`'s comparison must see it"
    );
    for forbidden in ["prod-sasl", "logweir-s3", "password"] {
        assert!(
            !frozen.canonical.contains(forbidden),
            "a CA reference is public certificate material; a CREDENTIAL reference still reaches \
             no ConfigMap: `{forbidden}` in {}",
            frozen.canonical
        );
    }

    // THE JOB MOUNTS IT, from that same resolution.
    let (terminal, bodies) =
        reconcile_against(&config_map_around(&frozen.canonical), cluster_json.clone()).await;
    assert_eq!(
        terminal,
        None,
        "the CA connection runs: {:?}",
        calls(&bodies)
    );
    let job = posted_job(&bodies).expect("the Job is created");
    let pod = &job["spec"]["template"]["spec"];
    let env = pod["containers"][0]["env"]
        .as_array()
        .expect("the container has env");
    let ca_file = env
        .iter()
        .find(|v| v["name"] == "LOGWEIR_SOURCE_TLS_CA_FILE")
        .expect("the CA path is handed to the runner");
    assert_eq!(ca_file["value"], "/connection/source-ca/ca.crt");
    assert!(
        ca_file.get("valueFrom").is_none(),
        "a PATH, never certificate text and never a readback"
    );
    let volume = pod["volumes"]
        .as_array()
        .expect("volumes")
        .iter()
        .find(|v| v["name"] == "source-ca")
        .expect("the CA volume");
    assert_eq!(volume["configMap"]["name"], "kafka-ca");
    assert_eq!(
        volume["configMap"]["items"],
        serde_json::json!([{ "key": "ca.crt", "path": "ca.crt" }])
    );
    let mount = pod["containers"][0]["volumeMounts"]
        .as_array()
        .expect("mounts")
        .iter()
        .find(|m| m["name"] == "source-ca")
        .expect("the CA mount");
    assert_eq!(mount["mountPath"], "/connection/source-ca");
    assert_eq!(mount["readOnly"], true);

    // AND A CHANGED REFERENCE IS A CONFLICT, in every part of it.
    let plan = config_map_around(&frozen.canonical);
    for (what, now) in [
        (
            "another object",
            serde_json::json!({ "configMapKeyRef": { "name": "kafka-ca-2", "key": "ca.crt" } }),
        ),
        (
            "another key",
            serde_json::json!({ "configMapKeyRef": { "name": "kafka-ca", "key": "other.crt" } }),
        ),
        (
            "another kind",
            serde_json::json!({ "secretKeyRef": { "name": "kafka-ca", "key": "ca.crt" } }),
        ),
    ] {
        let (terminal, bodies) = reconcile_against(&plan, cluster_with_ca(now)).await;
        assert_eq!(
            terminal.as_deref(),
            Some(TERMINAL_STATE_PLAN_CONFIG_MAP_CONFLICT),
            "a CA reference that now names {what} is a conflict: {:?}",
            calls(&bodies)
        );
        assert!(posted_job(&bodies).is_none(), "{what}: ZERO Job POSTs");
        assert!(!rewrote_a_config_map(&bodies), "{what}: never rewritten");
    }

    // …and so is a CA that was frozen and has since been REMOVED.
    let (terminal, bodies) = reconcile_against(&plan, kafka_cluster_json()).await;
    assert_eq!(
        terminal.as_deref(),
        Some(TERMINAL_STATE_PLAN_CONFIG_MAP_CONFLICT),
        "a removed CA is a conflict, not a quiet fall back to the image's public roots: {:?}",
        calls(&bodies)
    );
    assert!(posted_job(&bodies).is_none());

    // AND THE PURE BUILDER REFUSES IT TOO, without the reconciler's verify pass:
    // `runner_job_spec_from_inputs` is public, and a consumer holding frozen
    // inputs may reach it directly. It must not combine them with a connection
    // they did not come from.
    let other_ca: KafkaCluster = serde_json::from_str(&cluster_with_ca(
        serde_json::json!({ "configMapKeyRef": { "name": "kafka-ca-2", "key": "ca.crt" } }),
    ))
    .expect("still a KafkaCluster");
    let refusal = runner_job_spec_from_inputs(&backup(), &other_ca, &frozen)
        .expect_err("a changed CA reference cannot be combined with frozen inputs");
    assert!(
        matches!(
            refusal,
            weirkeeper::controllers::backup::BackupError::Refused(
                TERMINAL_STATE_PLAN_CONFIG_MAP_CONFLICT,
                _
            )
        ),
        "a terminal conflict, not a requeue: {refusal}"
    );
    assert!(
        runner_job_spec_from_inputs(&backup(), &cluster, &frozen).is_ok(),
        "and the connection it DID come from still builds"
    );

    // The mirror of the row above: a plan frozen with NO CA is refused against a
    // cluster that has since grown one, so the two directions are both closed.
    let (terminal, _bodies) = reconcile_against(
        &config_map_around(PRE_CONTRACT_INPUTS),
        cluster_with_ca(
            serde_json::json!({ "configMapKeyRef": { "name": "kafka-ca", "key": "ca.crt" } }),
        ),
    )
    .await;
    assert_eq!(
        terminal.as_deref(),
        Some(TERMINAL_STATE_PLAN_CONFIG_MAP_CONFLICT)
    );
}

// ===========================================================================
// D1 W3b — the run contract: identity for four trigger kinds, grammar v2, the
// frozen revision, `status.selection`, and manual runs
// ===========================================================================

use weirkeeper::backup_execution::{ResolvedSelection, ScheduleRefInputs};
use weirkeeper::crds::backup::{ScheduleRef, Trigger, TriggerKind};
use weirkeeper::crds::selection::{Coverage, SelectionMode};
use weirkeeper::identity::{declared_trigger, is_run_of_schedule, run_identity};

/// The run policy digest [`backup`]'s own fields produce — the value a
/// `scheduleRef.runPolicySha256` that was honestly copied must equal.
fn policy_digest_of(b: &Backup) -> String {
    weirkeeper::policy::run_policy_sha256(&b.spec)
}

/// [`scheduled_backup`] as **D1** creates one: no owner reference (PLAT-05.2
/// keeps the history when the schedule is deleted), the full `scheduleRef`
/// with the schedule's UID, generation and copied policy digest, and the
/// explicit `spec.trigger`.
fn d1_scheduled_backup() -> Backup {
    let mut value: Value = serde_json::from_str(&backup_json()).expect("the fixture is JSON");
    value["spec"]["slot"] = serde_json::json!("20261109-031700");
    value["spec"]["triggeredBy"] = serde_json::json!("schedule");
    value["spec"]["trigger"] = serde_json::json!({ "kind": "Scheduled", "attempt": 0 });
    let mut b: Backup = serde_json::from_value(value).expect("the mutated fixture is a Backup");
    let digest = policy_digest_of(&b);
    b.spec.schedule_ref = Some(ScheduleRef {
        name: "nightly".to_string(),
        uid: Some(SCHEDULE_UID.to_string()),
        generation: Some(7),
        run_policy_sha256: Some(digest),
    });
    b
}

/// The routes a create pass needs, with the `BackupSchedule` GET answering
/// `(status, body)` instead of the live schedule.
fn routes_with_schedule(status: u16, body: String) -> Vec<Route> {
    let mut routes = create_routes(201, existing_plan_config_map(UID));
    for route in &mut routes {
        if route.path_suffix == "/backupschedules/nightly" {
            route.status = status;
            route.body = body.clone();
        }
    }
    routes
}

/// Reconcile `b` against `routes` and return `(terminal state, bodies)`.
async fn reconcile_with(b: &Backup, routes: Vec<Route>) -> (Option<String>, Vec<SeenBody>) {
    let (client, _seen, bodies) = mock_client_recording_bodies(routes);
    let outcome = reconcile_backup(
        b,
        &client,
        &unobserved_archive,
        &unverified_evidence,
        utc(2026, 11, 9, 3, 17),
    )
    .await
    .expect("a refusal is an outcome, not an error");
    let bodies = bodies.lock().expect("readable").clone();
    (outcome.terminal_state, bodies)
}

/// The `execution-inputs.json` a create pass POSTed, as JSON.
fn posted_snapshot(bodies: &[SeenBody]) -> Value {
    let cm = posted_config_map(bodies);
    serde_json::from_str(cm["data"][INPUTS_KEY].as_str().expect("the snapshot key"))
        .expect("the snapshot is JSON")
}

/// **GRAMMAR `v2` IS GRAMMAR `v1` PLUS FIVE OPTIONAL BLOCKS, AND THE `v1`
/// FIELDS ARE UNTOUCHED** — D1 §3.3, D-SEAMS **S4**.
///
/// The key set is asserted whole, in order, because `logweir_core::det_json`
/// emits struct fields in DECLARATION order and this document's bytes are
/// re-encoded and compared on every later pass: reordering a field is not a
/// refactor, it is a cluster-wide `PlanConfigMapConflict`.
///
/// KILLS: renaming or reordering a `v1` key; making a `v2` block required (a
/// `v1` document would stop parsing); serialising a `v2` block as `null`
/// rather than omitting it (a `v1` document would stop re-encoding to itself).
#[test]
fn the_v2_grammar_is_v1_plus_five_optional_blocks() {
    let frozen = desired_for(&d1_scheduled_backup());
    let snapshot: Value = serde_json::from_str(&frozen.canonical).expect("JSON");
    let keys: Vec<&str> = snapshot
        .as_object()
        .expect("an object")
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(
        keys,
        vec![
            // v1, in the order PLAT-06.1 wrote them
            "version",
            "execution",
            // v2
            "trigger",
            "scheduleRef",
            "runPolicySha256",
            // v1
            "source",
            "topics",
            // v2
            "selection",
            // v1
            "archive",
            "runner",
        ],
        "the v2 document is the v1 document with blocks inserted, in declaration order: {}",
        frozen.canonical
    );
    assert_eq!(snapshot["version"], INPUTS_VERSION_V2);
    assert!(
        snapshot.get("destination").is_none(),
        "the reserved destination block is OMITTED, never written as null: {}",
        frozen.canonical
    );

    // The v1 view drops exactly the v2 blocks and nothing else.
    let v1 = frozen.inputs.as_version_v1();
    let v1_value: Value =
        serde_json::from_str(&canonical_inputs(&v1).expect("it encodes")).expect("JSON");
    let v1_keys: Vec<&str> = v1_value
        .as_object()
        .expect("an object")
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(
        v1_keys,
        vec![
            "version",
            "execution",
            "source",
            "topics",
            "archive",
            "runner"
        ],
        "the v1 view is exactly PLAT-06.1's document"
    );
    for (key, value) in v1_value.as_object().expect("an object") {
        if key == "version" {
            continue;
        }
        assert_eq!(
            value, &snapshot[key],
            "the v1 fields are IDENTICAL in both views; `{key}` differs"
        );
    }

    // And the document round-trips through the typed grammar.
    let reparsed: BackupExecutionInputs =
        serde_json::from_str(&frozen.canonical).expect("v2 parses");
    assert_eq!(
        canonical_inputs(&reparsed).expect("it re-encodes"),
        frozen.canonical
    );
}

/// **THE FROZEN DOCUMENT RECORDS WHICH SCHEDULE REVISION THE RUN COPIED** —
/// D1 §5.3. `scheduleRef {name, uid, generation, runPolicySha256}` and the
/// run's own `runPolicySha256`, in the immutable plan, so the revision
/// survives the schedule's deletion.
///
/// KILLS: dropping `generation` or the copied digest from the freeze (the
/// answer to "which policy did this archive run under?" would die with the
/// schedule); recomputing the copied digest from the live schedule instead of
/// copying what the object states (an edit between admission and freeze would
/// rewrite history).
#[tokio::test]
async fn the_frozen_inputs_record_the_schedule_revision_the_run_copied() {
    let b = d1_scheduled_backup();
    let digest = policy_digest_of(&b);
    let (terminal, bodies) =
        reconcile_with(&b, create_routes(201, existing_plan_config_map(UID))).await;
    assert_eq!(terminal, None, "it runs: {:?}", calls(&bodies));
    let snapshot = posted_snapshot(&bodies);
    assert_eq!(
        snapshot["scheduleRef"],
        serde_json::json!({
            "name": "nightly",
            "uid": SCHEDULE_UID,
            "generation": 7,
            "runPolicySha256": digest,
        }),
        "the revision is frozen whole: {snapshot}"
    );
    assert_eq!(
        snapshot["runPolicySha256"], digest,
        "and the run records the digest of its OWN fields beside the copied one"
    );
    assert_eq!(
        snapshot["trigger"],
        serde_json::json!({ "kind": "Scheduled", "attempt": 0 })
    );
    assert_eq!(
        snapshot["execution"]["id"],
        weirkeeper::slot::backup_id_for(SCHEDULE_UID, "20261109-031700"),
        "a D1 scheduled run has no owner reference and still derives the slot identity, from \
         spec.scheduleRef.uid"
    );

    // A DIFFERENT REVISION IS DIFFERENT BYTES. Same policy, later generation.
    let mut later = b.clone();
    later.spec.schedule_ref.as_mut().expect("a ref").generation = Some(9);
    assert_ne!(
        desired_for(&later).sha256,
        desired_for(&b).sha256,
        "the generation is inside the digested document, so `which revision ran` is part of what \
         the plan's digest names"
    );
}

/// **A COPIED POLICY DIGEST THAT IS NOT THIS OBJECT'S OWN IS TERMINAL** — D1
/// §3.1 rule 5, before any read and before any `POST`.
///
/// An integrity check against control-plane bugs, not a security boundary (D1
/// §8.7): the digest is recomputed from the same CEL-immutable spec, so a
/// mismatch means the copy and the fields disagree.
///
/// KILLS: trusting `scheduleRef.runPolicySha256` as the run's policy digest
/// (a client-supplied field would then decide what the run claims to be); and
/// taking the digest from the schedule at freeze time rather than from the
/// object.
#[tokio::test]
async fn a_copied_policy_digest_that_is_not_this_objects_own_is_terminal() {
    let mut b = d1_scheduled_backup();
    b.spec
        .schedule_ref
        .as_mut()
        .expect("a ref")
        .run_policy_sha256 =
        Some("sha256:0000000000000000000000000000000000000000000000000000000000000000".to_string());
    let (terminal, bodies) =
        reconcile_with(&b, create_routes(201, existing_plan_config_map(UID))).await;
    assert_eq!(
        terminal.as_deref(),
        Some(TERMINAL_STATE_RUN_POLICY_DIGEST_MISMATCH),
        "{:?}",
        calls(&bodies)
    );
    assert!(
        !bodies.iter().any(|r| r.method == "POST"),
        "nothing is created: {:?}",
        calls(&bodies)
    );
    assert_eq!(
        calls(&bodies).iter().filter(|(m, _)| m == "GET").count(),
        0,
        "and nothing is READ either — the check is on the object's own fields: {:?}",
        calls(&bodies)
    );
}

/// **A SCHEDULED RUN WHOSE SCHEDULE IS GONE IS `ScheduleNotFound`, BEFORE THE
/// FREEZE** — D1 §3.1 rule 2, §12's PLAT-05.2 row.
///
/// PLAT-05.2 removes the controller ownerReference so that deleting a schedule
/// KEEPS its history instead of collecting it. The cost is that an unfrozen run
/// of a deleted schedule would otherwise start a Job under a policy nobody can
/// look up; "deleting a schedule stops future work" is said here instead.
///
/// KILLS: skipping the referent check once the ownerReference is gone;
/// adopting a same-named schedule with a new UID (two schedules' runs would
/// share an archive prefix); requeueing instead of refusing.
#[tokio::test]
async fn an_unfrozen_scheduled_run_whose_schedule_is_gone_is_schedule_not_found() {
    for (label, status, body) in [
        (
            "deleted",
            404,
            not_found_body("backupschedules.logweir.dev", "nightly"),
        ),
        (
            "recreated under a new UID",
            200,
            backup_schedule_json("11111111-0000-4000-8000-000000000999"),
        ),
    ] {
        let (terminal, bodies) =
            reconcile_with(&d1_scheduled_backup(), routes_with_schedule(status, body)).await;
        assert_eq!(
            terminal.as_deref(),
            Some(TERMINAL_STATE_SCHEDULE_NOT_FOUND),
            "{label}: {:?}",
            calls(&bodies)
        );
        assert!(
            !bodies.iter().any(|r| r.method == "POST"),
            "{label}: no plan and no Job: {:?}",
            calls(&bodies)
        );
        let refused = patched_statuses(&bodies)
            .pop()
            .expect("the refusal is written");
        assert_eq!(refused["phase"], "Failed", "{label}: {refused}");
    }
}

/// **A FROZEN RUN IS NEVER RE-CHECKED AGAINST ITS SCHEDULE** — the other half
/// of D1 §3.1 rule 2, and §12's `a_frozen_run_is_not_rechecked_after_schedule_deletion`.
///
/// A run whose inputs are frozen executes the policy it copied. Deleting the
/// schedule while its Job runs must not turn a running archive into a terminal
/// failure, and a Job that has to be re-created from those same frozen inputs
/// must not start asking about an object the run no longer depends on.
///
/// KILLS: moving the referent check outside the `status.execution.is_none()`
/// gate — every re-create pass after a schedule deletion would then refuse a
/// run whose archive is half written.
#[tokio::test]
async fn a_frozen_run_is_not_rechecked_after_schedule_deletion() {
    let b = d1_scheduled_backup();
    let frozen = running_after_freeze(&b);
    let mut routes = routes_with_schedule(
        404,
        not_found_body("backupschedules.logweir.dev", "nightly"),
    );
    // The plan is already there and is read back, not re-POSTed.
    for route in &mut routes {
        if route.method == "POST" && route.path_suffix == "/configmaps" {
            route.status = 409;
            route.body = r#"{"kind":"Status","apiVersion":"v1","status":"Failure",
                "message":"already exists","reason":"AlreadyExists","code":409}"#
                .to_string();
        }
        if route.method == "GET" && route.path_suffix.ends_with("-plan") {
            route.status = 200;
            route.body = frozen_config_map(&b).to_string();
        }
    }
    let (terminal, bodies) = reconcile_with(&frozen, routes).await;
    assert_eq!(
        terminal,
        None,
        "the frozen run continues although its schedule is gone: {:?}",
        calls(&bodies)
    );
    assert!(
        !calls(&bodies)
            .iter()
            .any(|(_, p)| p.contains("/backupschedules/")),
        "and it does not ask about the schedule at all: {:?}",
        calls(&bodies)
    );
    assert!(
        posted_job(&bodies).is_some(),
        "the Job is re-created from the frozen inputs: {:?}",
        calls(&bodies)
    );
}

/// The canonical manual `Backup` of D1 §8.1, named by the API from the
/// idempotency scope.
const MANUAL_NAME: &str = "logweir-manual-2v4qk7bhq8nwz3xr9fcm5td6ea";

/// [`backup`] as the PLAT-06.2 API route creates it: the deterministic name,
/// `trigger.kind: Manual`, and the schedule revision it copied.
fn canonical_manual_backup() -> Backup {
    let mut value: Value = serde_json::from_str(&backup_json()).expect("the fixture is JSON");
    value["metadata"]["name"] = serde_json::json!(MANUAL_NAME);
    value["metadata"]["labels"] = serde_json::json!({
        "logweir.dev/schedule": "nightly",
        "logweir.dev/schedule-uid": SCHEDULE_UID,
        "logweir.dev/trigger": "manual",
        "logweir.dev/attempt": "0",
    });
    value["spec"]["trigger"] = serde_json::json!({ "kind": "Manual", "attempt": 0 });
    let mut b: Backup = serde_json::from_value(value).expect("the mutated fixture is a Backup");
    let digest = policy_digest_of(&b);
    b.spec.schedule_ref = Some(ScheduleRef {
        name: "nightly".to_string(),
        uid: Some(SCHEDULE_UID.to_string()),
        generation: Some(7),
        run_policy_sha256: Some(digest),
    });
    b
}

/// The route table for [`canonical_manual_backup`]. **It carries an answer for
/// the `BackupSchedule` GET that must never be asked for.**
fn manual_routes() -> Vec<Route> {
    vec![
        Route {
            method: "GET",
            path_suffix: "/jobs/logweir-manual-2v4qk7bhq8nwz3xr9fcm5td6ea",
            status: 404,
            body: not_found_body("jobs.batch", MANUAL_NAME),
        },
        Route {
            method: "GET",
            path_suffix: "/backupschedules/nightly",
            status: 404,
            body: not_found_body("backupschedules.logweir.dev", "nightly"),
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
            body: serde_json::to_string(
                &plan_config_map(&canonical_manual_backup(), &prod_cluster())
                    .expect("the manual plan renders"),
            )
            .expect("a ConfigMap serialises"),
        },
        Route {
            method: "POST",
            path_suffix: "/jobs",
            status: 201,
            body: manual_running_job_body(),
        },
        Route {
            method: "PATCH",
            path_suffix: "/status",
            status: 200,
            body: backup_json(),
        },
    ]
}

/// [`running_job_body`] for [`MANUAL_NAME`]: a Job that exists, has not
/// finished, and is controlled by exactly that `Backup`.
fn manual_running_job_body() -> String {
    running_job_body().replace(NAME, MANUAL_NAME)
}

/// **THE CANONICAL MANUAL BACKUP RUNS, RECORDS THE REVISION IT COPIED, AND
/// NEVER READS A `BackupSchedule`** — D1 §8.1 and §8.3.
///
/// A manual run is "run this policy now". It is allowed while the schedule is
/// suspended, it is allowed while a scheduled run is active, and it survives
/// the schedule being deleted between the request and the freeze — so the
/// controller must not make the schedule a precondition of it. The route table
/// answers `404` for the schedule and the pass must never ask.
///
/// KILLS: applying D1 §3.1 rule 2 to a manual run (a "Back up now" pressed a
/// second after a schedule delete would fail); dropping the copied revision
/// from a manual run's frozen inputs (the console could no longer say which
/// policy the button ran); taking a manual run's identity from its
/// `scheduleRef` rather than its own UID.
#[tokio::test]
async fn the_canonical_manual_backup_runs_and_records_its_schedules_revision() {
    let b = canonical_manual_backup();
    let digest = policy_digest_of(&b);
    let (terminal, bodies) = reconcile_with(&b, manual_routes()).await;
    assert_eq!(terminal, None, "it runs: {:?}", calls(&bodies));
    assert!(
        !calls(&bodies)
            .iter()
            .any(|(_, p)| p.contains("/backupschedules/")),
        "a manual run never reads a BackupSchedule, so a suspended, blocked or DELETED schedule \
         cannot stop it: {:?}",
        calls(&bodies)
    );
    let snapshot = posted_snapshot(&bodies);
    assert_eq!(
        snapshot["execution"]["id"], UID,
        "a manual run IS its own UID, whatever schedule it copied: {snapshot}"
    );
    assert_eq!(snapshot["execution"]["trigger"], "manual");
    assert!(
        snapshot["execution"].get("schedule").is_none(),
        "and it holds no scheduled identity"
    );
    assert_eq!(
        snapshot["trigger"],
        serde_json::json!({ "kind": "Manual", "attempt": 0 })
    );
    assert_eq!(
        snapshot["scheduleRef"],
        serde_json::json!({
            "name": "nightly",
            "uid": SCHEDULE_UID,
            "generation": 7,
            "runPolicySha256": digest,
        }),
        "the revision the button copied is recorded, and it is a RECORD and not an identity"
    );
    assert_eq!(
        snapshot["runner"]["args"],
        serde_json::json!(runner_argv(ExecutionTrigger::Manual, UID)),
        "the receipt this run signs says `manual`"
    );
}

/// **A MANUAL RUN IS A MEMBER OF ITS SCHEDULE AND IS NOT ONE OF ITS RUNS** —
/// D1 §2 and §8.3, stated here because the trap is one line away in the two
/// files that will consume it.
///
/// PLAT-05.2 moves membership from the controller ownerReference to
/// `identity::is_run_of_schedule`, which matches on `spec.scheduleRef.uid` —
/// and a manual `Backup` created from a schedule carries exactly that. So the
/// moment the schedule controller's `Forbid` accounting switches to the new
/// membership test, **a manual run starts blocking scheduled slots** unless the
/// filter also reads `spec.trigger.kind`. D1 §2 is explicit: only
/// `Scheduled`, `CatchUp` and `Retry` participate in `concurrencyPolicy`.
///
/// KILLS: a `concurrencyPolicy` filter written as membership alone (D1 W2/W4).
#[test]
fn a_manual_run_is_a_member_of_its_schedule_but_not_a_scheduled_run() {
    let manual = canonical_manual_backup();
    assert!(
        is_run_of_schedule(&manual, "nightly", SCHEDULE_UID),
        "membership is `spec.scheduleRef`, and a manual run from a schedule HAS one"
    );
    let (kind, attempt, _) = declared_trigger(&manual);
    assert_eq!(kind, TriggerKind::Manual);
    assert_eq!(attempt, 0);
    assert_eq!(
        run_identity(&manual).expect("it has an identity").kind,
        TriggerKind::Manual,
        "and the derivation agrees: this is not one of the schedule's runs, so it is neither \
         counted by concurrencyPolicy nor blocked by it"
    );

    let scheduled = d1_scheduled_backup();
    assert!(is_run_of_schedule(&scheduled, "nightly", SCHEDULE_UID));
    assert_eq!(declared_trigger(&scheduled).0, TriggerKind::Scheduled);
}

/// **A RETRY IS A NEW EXECUTION ID, AND A CATCH-UP IS THE SAME SLOT** — D1
/// §3.1's identity table, through the freeze.
///
/// KILLS: giving a catch-up an identity of its own (a restart would write a
/// second archive of one window); reusing attempt 0's `backup_id` for a retry
/// (attempt `k+1` would append into attempt `k`'s partial prefix).
#[test]
fn a_retry_freezes_a_new_execution_id_and_a_catch_up_reuses_the_slots() {
    let base = d1_scheduled_backup();
    let slot = "20261109-031700";

    let mut catch_up = base.clone();
    catch_up.spec.trigger = Some(Trigger {
        kind: TriggerKind::CatchUp,
        attempt: 0,
        retry_of: None,
        time_zone: Some("Europe/Berlin".to_string()),
    });
    let frozen = desired_for(&catch_up);
    assert_eq!(
        frozen.inputs.execution.id,
        weirkeeper::slot::backup_id_for(SCHEDULE_UID, slot),
        "a catch-up IS slot S, started late"
    );
    assert_eq!(
        frozen
            .inputs
            .trigger
            .as_ref()
            .expect("a trigger block")
            .kind,
        TriggerKind::CatchUp
    );
    assert_eq!(
        frozen
            .inputs
            .trigger
            .as_ref()
            .expect("a trigger block")
            .time_zone
            .as_deref(),
        Some("Europe/Berlin"),
        "the zone the slot was computed in is frozen with the run, so a later timeZone edit does \
         not relabel this row's local time"
    );

    let mut retry: Value = serde_json::to_value(&base).expect("a Backup serialises");
    retry["metadata"]["name"] = serde_json::json!(format!("logweir-backup-nightly-{slot}-r1"));
    retry["spec"]["trigger"] = serde_json::json!({
        "kind": "Retry", "attempt": 1,
        "retryOf": { "name": format!("logweir-backup-nightly-{slot}") }
    });
    let retry: Backup = serde_json::from_value(retry).expect("a Backup");
    let frozen = desired_for(&retry);
    assert_eq!(
        frozen.inputs.execution.id,
        weirkeeper::slot::backup_id_for_attempt(SCHEDULE_UID, slot, 1),
        "a retry writes under its OWN prefix; attempt 0's archive may be half written"
    );
    assert_ne!(
        frozen.inputs.execution.id,
        weirkeeper::slot::backup_id_for(SCHEDULE_UID, slot)
    );
    assert_eq!(
        frozen.inputs.trigger,
        Some(weirkeeper::backup_execution::RunInputs {
            kind: TriggerKind::Retry,
            attempt: 1,
            retry_of: Some(format!("logweir-backup-nightly-{slot}")),
            time_zone: None,
        })
    );
}

/// **A DYNAMIC SELECTION STARTS A DISCOVERY, AND NO EMPTY ALLOWLIST EVER
/// REACHES THE ENGINE** — D1 §7.1, §7.2 R2, guard **G-GLOB**.
///
/// `spec.allUserTopics` with `topics: []` is D1 §7.1's second shape. The only
/// two things this controller may do with it are resolve it or refuse it; what
/// it must never do is render `spec.topics` — empty, in this mode — into
/// `backup.yaml`, which is precisely the "no allowlist means everything" shape
/// the mandatory allowlist exists to make impossible.
///
/// Since PLAT-09.2 it RESOLVES it: one discovery Job owned by this `Backup`,
/// its plan `ConfigMap` first, phase `Resolving`, and no runner Job until the
/// exact names are frozen. The whole state machine lives in
/// `tests/backup_selection.rs`; this row is the `backup_controller` half —
/// that the shape reaches the resolver at all, and that the freeze boundary
/// still refuses an empty or patterned list from ANY producer.
///
/// KILLS: falling through to the freeze with an empty list; creating the runner
/// Job before the names exist; refusing the dynamic shape as the third
/// (invalid) shape; creating the discovery Job before its plan.
#[tokio::test]
async fn a_dynamic_selection_starts_a_discovery_and_no_empty_allowlist_reaches_the_engine() {
    let mut value: Value = serde_json::from_str(&backup_json()).expect("JSON");
    value["spec"]["topics"] = serde_json::json!([]);
    value["spec"]["allUserTopics"] =
        serde_json::json!({ "incompleteDiscovery": "BackUpVisibleTopics" });
    // EVERY STATUS WRITE ON THE DYNAMIC PATH IS A `resourceVersion`
    // COMPARE-AND-SET (D-SEAMS S7), so the fixture carries one.
    value["metadata"]["resourceVersion"] = serde_json::json!("4242");
    let b: Backup = serde_json::from_value(value).expect("a Backup");

    let mut routes = create_routes(201, existing_plan_config_map(UID));
    routes.push(Route {
        method: "GET",
        path_suffix: "/jobs/lwd-3f1c8a5e-0000-4000-8000-0000000000a1",
        status: 404,
        body: not_found_body("jobs.batch", "lwd-3f1c8a5e-0000-4000-8000-0000000000a1"),
    });
    let (terminal, bodies) = reconcile_with(&b, routes).await;
    assert_eq!(terminal, None, "{:?}", calls(&bodies));

    let job = posted_job(&bodies).expect("a Job is created");
    assert_eq!(
        job["metadata"]["name"],
        serde_json::json!("lwd-3f1c8a5e-0000-4000-8000-0000000000a1"),
        "the ONE Job this pass creates is the discovery Job: {:?}",
        calls(&bodies)
    );
    assert_eq!(
        bodies
            .iter()
            .filter(|r| r.method == "POST" && path(&r.uri).ends_with("/jobs"))
            .count(),
        1,
        "and no runner Job: {:?}",
        calls(&bodies)
    );
    let plan = posted_config_map(&bodies);
    assert_eq!(
        plan["metadata"]["name"],
        serde_json::json!("lwd-3f1c8a5e-0000-4000-8000-0000000000a1-plan"),
        "the discovery plan, POSTed before the Job"
    );
    let status = patched_statuses(&bodies)
        .pop()
        .expect("the Resolving status is written");
    assert_eq!(status["phase"], serde_json::json!("Resolving"));
    assert_eq!(
        condition_named(&status, "TopicsResolved").map(|(s, r, _)| (s, r)),
        Some(("False".to_string(), "DiscoveryRunning".to_string())),
        "{status}"
    );

    // AND THE FREEZE BOUNDARY ITSELF REFUSES AN EMPTY OR PATTERNED LIST,
    // whatever produced it. The discovery resolver is the second producer of a
    // `ResolvedSelection`, and this is the rail it is behind.
    let cluster = prod_cluster();
    let mut empty = ResolvedSelection::named(&backup().spec);
    empty.topics.clear();
    let refusal =
        weirkeeper::controllers::backup::desired_execution_inputs_for(&backup(), &cluster, &empty)
            .expect_err("an empty resolved list never freezes");
    assert!(
        matches!(
            refusal,
            weirkeeper::controllers::backup::BackupError::Refused(
                weirkeeper::conditions::TERMINAL_STATE_SELECTION_EMPTY,
                _
            )
        ),
        "an empty resolved selection is SelectionEmpty: {refusal}"
    );

    let mut globbed = ResolvedSelection::named(&backup().spec);
    globbed.topics = vec!["orders*".to_string()];
    let refusal = weirkeeper::controllers::backup::desired_execution_inputs_for(
        &backup(),
        &cluster,
        &globbed,
    )
    .expect_err("a pattern never freezes");
    assert!(
        matches!(
            refusal,
            weirkeeper::controllers::backup::BackupError::Refused(
                TERMINAL_STATE_INVALID_TOPIC_SELECTION,
                _
            )
        ),
        "a glob in a RESOLVED list is refused at the freeze, not expanded: {refusal}"
    );
}

/// **`status.selection` IS WRITTEN AT THE FREEZE, AND IT IS THE FROZEN
/// DOCUMENT'S OWN PROJECTION** — D1 §7.6, §7.4.
///
/// KILLS: writing the coverage label only when a run finishes; computing the
/// status counts separately from the frozen ones; rendering a named allowlist
/// as anything but `NamedTopics` (only `AllUserTopicsAttested` may ever be read
/// as "all topics").
#[tokio::test]
async fn the_frozen_selection_reaches_the_status_at_the_freeze() {
    let (terminal, bodies) = reconcile_with(
        &d1_scheduled_backup(),
        create_routes(201, existing_plan_config_map(UID)),
    )
    .await;
    assert_eq!(terminal, None, "{:?}", calls(&bodies));
    let snapshot = posted_snapshot(&bodies);
    assert_eq!(
        snapshot["selection"],
        serde_json::json!({
            "mode": "SelectedTopics",
            "coverage": "NamedTopics",
            "resolvedTopicCount": 2,
            "resolvedTopicBytes": 14,
        }),
        "the provenance of `topics` is frozen beside it: {snapshot}"
    );
    assert_eq!(
        snapshot["topics"],
        serde_json::json!(["orders", "payments"]),
        "and the NAMES live once, in the v1 field the engine document is rendered from"
    );

    let statuses = patched_statuses(&bodies);
    assert_eq!(
        statuses[0]["selection"],
        serde_json::to_value(
            weirkeeper::backup_execution::SelectionInputs {
                mode: SelectionMode::SelectedTopics,
                coverage: Coverage::NamedTopics,
                resolved_topic_count: 2,
                resolved_topic_bytes: 14,
                exclude: None,
                incomplete_discovery: None,
                discovery: None,
            }
            .status()
        )
        .expect("it serialises"),
        "the status is the frozen block's own projection, written in the SAME patch as \
         status.execution: {}",
        statuses[0]
    );
    assert!(
        !Coverage::NamedTopics.claims_whole_cluster(),
        "a named allowlist never claims the cluster"
    );
}

/// One row of [`every_v2_block_is_compared_when_a_stored_plan_is_admitted`]:
/// the block's name, the edit that moves it in the STORED snapshot, and a
/// fragment the refusal message must name.
type BlockCase = (
    &'static str,
    Box<dyn Fn(&mut BackupExecutionInputs)>,
    &'static str,
);

/// **A STORED `v2` PLAN IS COMPARED WHOLE: EVERY BLOCK D1 ADDED IS A
/// CONFLICT WHEN IT DIFFERS** — and each refusal NAMES the block.
///
/// The generic "the inputs differ" message was enough while the document held
/// a connection and a topic list. It is not enough now: an operator reading a
/// terminal `PlanConfigMapConflict` needs to know whether the trigger, the
/// revision, the policy, the selection or the destination moved.
///
/// KILLS: leaving a `v2` block out of the comparison (a plan frozen for one
/// revision could be executed for another); comparing a `v2` plan through the
/// `v1` downgrade (the same hole, reached the other way).
#[tokio::test]
async fn every_v2_block_is_compared_when_a_stored_plan_is_admitted() {
    let b = d1_scheduled_backup();
    let frozen = desired_for(&b);

    let mutate = |f: &dyn Fn(&mut BackupExecutionInputs)| -> Value {
        let mut inputs = frozen.inputs.clone();
        f(&mut inputs);
        let refrozen = FrozenInputs::freeze(inputs).expect("it freezes");
        let mut cm = frozen_config_map(&b);
        cm["metadata"]["ownerReferences"][0]["name"] = serde_json::json!(NAME);
        cm["data"] = serde_json::to_value(refrozen.documents().expect("documents render")).unwrap();
        cm["metadata"]["annotations"][INPUTS_SHA256_ANNOTATION] =
            serde_json::json!(refrozen.sha256);
        cm["metadata"]["annotations"][EXECUTION_ID_ANNOTATION] =
            serde_json::json!(refrozen.inputs.execution.id);
        cm
    };

    let cases: Vec<BlockCase> = vec![
        (
            "trigger",
            Box::new(|i: &mut BackupExecutionInputs| {
                i.trigger.as_mut().expect("a trigger").kind = TriggerKind::CatchUp;
            }),
            "froze trigger",
        ),
        (
            "scheduleRef",
            Box::new(|i: &mut BackupExecutionInputs| {
                i.schedule_ref = Some(ScheduleRefInputs {
                    name: "nightly".to_string(),
                    uid: Some(SCHEDULE_UID.to_string()),
                    generation: Some(9),
                    run_policy_sha256: None,
                });
            }),
            "BackupSchedule revision",
        ),
        (
            "runPolicySha256",
            Box::new(|i: &mut BackupExecutionInputs| {
                i.run_policy_sha256 = Some("sha256:beef".to_string());
            }),
            "froze run policy",
        ),
        (
            "selection",
            Box::new(|i: &mut BackupExecutionInputs| {
                i.selection.as_mut().expect("a selection").coverage =
                    Coverage::VisibleUserTopicsOnly;
            }),
            "different topic selection",
        ),
    ];

    for (label, mutation, needle) in cases {
        let existing = mutate(mutation.as_ref());
        let mut routes = create_routes(409, existing.to_string());
        for route in &mut routes {
            if route.method == "GET" && route.path_suffix.ends_with("-plan") {
                route.status = 200;
                route.body = existing.to_string();
            }
        }
        let (terminal, bodies) = reconcile_with(&b, routes).await;
        assert_eq!(
            terminal.as_deref(),
            Some(TERMINAL_STATE_PLAN_CONFIG_MAP_CONFLICT),
            "{label}: a changed {label} is a conflict: {:?}",
            calls(&bodies)
        );
        assert!(posted_job(&bodies).is_none(), "{label}: no Job");
        assert!(!rewrote_a_config_map(&bodies), "{label}: nothing rewritten");
        let refused = patched_statuses(&bodies)
            .pop()
            .expect("the refusal is written");
        let message = refused["conditions"][0]["message"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        assert!(
            message.contains(needle),
            "{label}: the refusal names the block that moved, not just `the inputs differ`: \
             {message}"
        );
    }
}

/// **AN IDENTITY IS NEVER TAKEN FROM A CLIENT-SUPPLIED FIELD** — D1 §3.1
/// rules 1, 2 and 5 together, from the reconciler's side.
///
/// The three fields a client controls that LOOK like identity are
/// `spec.scheduleRef.uid`, `spec.scheduleRef.runPolicySha256` and
/// `metadata.name`. Each is CHECKED against something the client does not
/// control: the schedule object, the object's own policy fields, and the name
/// the trigger composes.
///
/// KILLS: trusting `scheduleRef.uid` as the archive prefix without reading the
/// schedule; re-labelling a failed scheduled claim as a manual run (its receipt
/// would say `schedule` about a run no schedule created).
#[tokio::test]
async fn an_identity_is_never_taken_from_a_client_supplied_field() {
    // (1) A UID no BackupSchedule has.
    let mut invented = d1_scheduled_backup();
    invented.spec.schedule_ref.as_mut().expect("a ref").uid =
        Some("deadbeef-0000-4000-8000-00000000dead".to_string());
    let (terminal, bodies) =
        reconcile_with(&invented, create_routes(201, existing_plan_config_map(UID))).await;
    assert_eq!(
        terminal.as_deref(),
        Some(TERMINAL_STATE_SCHEDULE_NOT_FOUND),
        "an invented schedule UID does not become an archive prefix: {:?}",
        calls(&bodies)
    );
    assert!(!bodies.iter().any(|r| r.method == "POST"));

    // (2) A name that is not the one the trigger composes.
    let mut renamed: Value = serde_json::to_value(d1_scheduled_backup()).expect("JSON");
    renamed["metadata"]["name"] = serde_json::json!("logweir-backup-nightly-20261109-031701");
    let renamed: Backup = serde_json::from_value(renamed).expect("a Backup");
    let refusal = run_identity(&renamed).expect_err("the name does not compose");
    assert_eq!(
        refusal.terminal_state(),
        TERMINAL_STATE_SCHEDULED_IDENTITY_MISMATCH,
        "and it is never re-read as a manual run: {refusal}"
    );

    // (3) The scheduled claim survives neither as manual nor as scheduled.
    let mut half: Value = serde_json::to_value(d1_scheduled_backup()).expect("JSON");
    half["spec"]["triggeredBy"] = serde_json::json!("manual");
    let half: Backup = serde_json::from_value(half).expect("a Backup");
    let refusal = execution_identity(&half).expect_err("the two fields contradict each other");
    assert_eq!(
        refusal.state, TERMINAL_STATE_SCHEDULED_IDENTITY_MISMATCH,
        "spec.triggeredBy is what the SIGNED RECEIPT carries, so it may not contradict \
         spec.trigger.kind: {refusal:?}"
    );
}

/// **THE TWO RULES THAT USED TO BE COMPENSATIONS, THROUGH THE RECONCILER** —
/// a slot that is not a date, and a legacy ownerReference for another schedule.
///
/// Both now live in `identity.rs` (D1 §3.1 rules 1 and 3) and are guarded there
/// by `tests/run_identity.rs`. These two rows exist because the rows that LOOK
/// like they cover them do not: `a_spec_that_states_no_runnable_identity_…`
/// leaves `metadata.name` at `logweir-backup-nightly-20261109-031700` while
/// changing `scheduleRef.name` or `spec.slot`, so rule 1 refuses on the
/// composed name before either rule is reached. Here the name **composes**, so
/// the only thing that can refuse is the rule under test — and the refusal is
/// asserted where an operator meets it, on the object, with nothing created.
///
/// KILLS: disabling the calendar round-trip in `identity::valid_slot`;
/// dropping the name equality from `identity::legacy_owner_uid`.
#[tokio::test]
async fn a_month_thirteen_slot_and_a_foreign_legacy_owner_are_refused_before_any_post() {
    // (1) A slot that is fifteen digits and not a date. The name composes from
    // it, so rule 1 is satisfied and only the calendar check can refuse.
    const BAD_SLOT: &str = "20261309-031700";
    const BAD_NAME: &str = "logweir-backup-nightly-20261309-031700";
    assert_eq!(
        weirkeeper::slot::scheduled_backup_name("nightly", BAD_SLOT).expect("it composes"),
        BAD_NAME,
        "the premise: this object's name IS the name its fields compose"
    );
    let mut v: Value = serde_json::to_value(d1_scheduled_backup()).expect("JSON");
    v["metadata"]["name"] = serde_json::json!(BAD_NAME);
    v["spec"]["slot"] = serde_json::json!(BAD_SLOT);
    let month_thirteen: Backup = serde_json::from_value(v).expect("a Backup");

    let mut routes = create_routes(201, existing_plan_config_map(UID));
    routes.push(Route {
        method: "GET",
        path_suffix: "/jobs/logweir-backup-nightly-20261309-031700",
        status: 404,
        body: not_found_body("jobs.batch", BAD_NAME),
    });
    routes.push(Route {
        method: "PATCH",
        path_suffix: "/backups/logweir-backup-nightly-20261309-031700/status",
        status: 200,
        body: backup_json(),
    });
    let (terminal, bodies) = reconcile_with(&month_thirteen, routes).await;
    assert_eq!(
        terminal.as_deref(),
        Some(TERMINAL_STATE_SCHEDULED_IDENTITY_MISMATCH),
        "a slot naming a thirteenth month is not an instant, so there is no archive prefix to \
         run under: {:?}",
        calls(&bodies)
    );
    assert!(
        !bodies.iter().any(|r| r.method == "POST"),
        "nothing is created: {:?}",
        calls(&bodies)
    );

    // (2) A legacy object — `scheduleRef {name}` with no uid — whose controller
    // ownerReference names `hourly` while the run claims `nightly`. The name
    // composes from `nightly`, so again only the rule under test can refuse.
    let mut v: Value = serde_json::from_str(&backup_json()).expect("JSON");
    v["spec"]["slot"] = serde_json::json!("20261109-031700");
    v["spec"]["triggeredBy"] = serde_json::json!("schedule");
    v["spec"]["scheduleRef"] = serde_json::json!({ "name": "nightly" });
    v["metadata"]["ownerReferences"] = serde_json::json!([{
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "BackupSchedule",
        "name": "hourly",
        "uid": "9a8b7c6d-0000-4000-8000-0000000000ff",
        "controller": true,
        "blockOwnerDeletion": true,
    }]);
    let foreign_owner: Backup = serde_json::from_value(v).expect("a Backup");
    assert_eq!(
        foreign_owner.metadata.name.as_deref(),
        Some(NAME),
        "the premise: the object is named for `nightly`, the schedule it claims"
    );

    let (terminal, bodies) = reconcile_with(
        &foreign_owner,
        create_routes(201, existing_plan_config_map(UID)),
    )
    .await;
    assert_eq!(
        terminal.as_deref(),
        Some(TERMINAL_STATE_SCHEDULED_IDENTITY_MISMATCH),
        "an ownerReference to `hourly` must not become `nightly`'s archive prefix: {:?}",
        calls(&bodies)
    );
    assert!(
        !bodies.iter().any(|r| r.method == "POST"),
        "nothing is created: {:?}",
        calls(&bodies)
    );

    // The same object with the owner renamed runs, and under THAT owner's UID —
    // which is the whole reason the name has to match.
    let mut v: Value = serde_json::to_value(&foreign_owner).expect("JSON");
    v["metadata"]["ownerReferences"][0]["name"] = serde_json::json!("nightly");
    let matching: Backup = serde_json::from_value(v).expect("a Backup");
    let (terminal, bodies) = reconcile_with(
        &matching,
        routes_with_schedule(
            200,
            backup_schedule_json("9a8b7c6d-0000-4000-8000-0000000000ff"),
        ),
    )
    .await;
    assert_eq!(terminal, None, "it runs: {:?}", calls(&bodies));
    assert_eq!(
        posted_snapshot(&bodies)["execution"]["id"],
        weirkeeper::slot::backup_id_for("9a8b7c6d-0000-4000-8000-0000000000ff", "20261109-031700"),
        "the legacy identity IS the owner's UID"
    );
}

/// **A FROZEN RUN EXECUTES THE SELECTION IT WAS ADMITTED WITH, AND NEVER
/// RESOLVES A SECOND ONE** — the NAMED-shape half of the post-freeze gate.
///
/// # Which half this is, and where the other one lives
///
/// The fixture is [`backup`] — `spec.topics: ["orders","payments"]` and **no**
/// `spec.allUserTopics` — so its declared shape is `SelectedTopics` and the
/// dynamic arm of the gate is never entered here. What it holds is the half
/// that is true for every run: a `v2` plan whose frozen `topics` and
/// `selection` are **not** what a fresh resolution would produce — here
/// `AllUserTopics` coverage over a list the spec does not name — is still what
/// a Job-recreate pass executes, because the gate reads the plan back rather
/// than re-resolving from `spec`.
///
/// **The dynamic half — D1 §12's row of this name — is
/// `tests/backup_selection.rs`'s `a_frozen_dynamic_backup_never_reruns_discovery`**,
/// which freezes a real dynamic run and re-passes it over a route table with no
/// discovery-Job route and no pod route at all, so a second discovery is a
/// panic rather than an assertion. A gate bypass scoped to
/// `SelectionShape::AllUserTopics` alone leaves THIS row passing and kills that
/// one.
///
/// Without the gate, `desired` is built from `spec` (or, in dynamic mode, from
/// a fresh discovery), the stored `topics`/`selection` no longer match it, and
/// `verify_frozen_config_map` refuses the run terminally — on an archive that
/// may be half written, for no reason but the passage of time.
///
/// KILLS: removing the `status.execution` gate; building `desired` from `spec`
/// when a plan exists; re-rendering the plan instead of verifying it.
#[tokio::test]
async fn a_frozen_dynamic_backup_never_reruns_discovery() {
    // A plan frozen for a selection the spec does not state: the resolved list
    // is `audit`, not the spec's `orders`/`payments`, and the coverage is the
    // dynamic one. Only a pass that READS this plan can agree with it.
    let b = backup();
    let mut inputs = desired_for(&b).inputs;
    inputs.topics = vec!["audit".to_string()];
    inputs.selection = Some(weirkeeper::backup_execution::SelectionInputs {
        mode: SelectionMode::AllUserTopics,
        coverage: Coverage::VisibleUserTopicsOnly,
        resolved_topic_count: 1,
        resolved_topic_bytes: 5,
        exclude: None,
        incomplete_discovery: Some(
            weirkeeper::crds::selection::IncompleteDiscovery::BackUpVisibleTopics,
        ),
        discovery: None,
    });
    let frozen = FrozenInputs::freeze(inputs).expect("it freezes");
    assert_ne!(
        frozen.sha256,
        desired_for(&b).sha256,
        "the premise: the stored plan is NOT what a fresh resolution produces"
    );

    let existing = config_map_around(&frozen.canonical);
    // …and the Backup as the freeze left it: status.execution recording THAT
    // plan's digest, phase Running, its Job since collected.
    let recorded = with_status_patch(&b, &execution_status_patch(&frozen));
    let recorded = with_status_patch(
        &recorded,
        &running_status_patch(&recorded, NAME, utc(2026, 11, 9, 3, 17)),
    );

    let mut routes = create_routes(409, existing.to_string());
    for route in &mut routes {
        if route.method == "GET" && route.path_suffix.ends_with("-plan") {
            route.status = 200;
            route.body = existing.to_string();
        }
    }
    let (terminal, bodies) = reconcile_with(&recorded, routes).await;

    assert_eq!(
        terminal,
        None,
        "a frozen run continues from the plan it was admitted with: {:?}",
        calls(&bodies)
    );
    let job = posted_job(&bodies).expect("the Job is re-created");
    assert_eq!(
        job["metadata"]["annotations"][INPUTS_SHA256_ANNOTATION], frozen.sha256,
        "and it is built from the STORED inputs, not from a fresh resolution"
    );
    assert!(
        !rewrote_a_config_map(&bodies),
        "the plan is read and verified, never re-rendered: {:?}",
        calls(&bodies)
    );

    // Nothing resolved a selection a second time: no discovery Job, and the
    // only Job POSTed is the runner's.
    assert!(
        !bodies.iter().any(|r| {
            r.method == "POST" && path(&r.uri).ends_with("/jobs") && r.body.contains("lwd-")
        }),
        "no discovery Job is created for a run whose selection is already frozen: {:?}",
        calls(&bodies)
    );
    assert_eq!(
        bodies
            .iter()
            .filter(|r| r.method == "POST" && path(&r.uri).ends_with("/jobs"))
            .count(),
        1,
        "exactly one Job, the runner's: {:?}",
        calls(&bodies)
    );
}

// ===========================================================================
// D3 §13 — PLAT-14.1's controller rows
// ===========================================================================

use serde_json::json;
use weirkeeper::controllers::backup::{
    capture_from_receipt, records_from_receipt, tail_lines, BUDGETED_TRAILING_LINES,
    KEY_SCAN_TAIL_LINES,
};
use weirkeeper::diagnostics::{
    self, BACKUP_STEPS, PROGRESS_LIMIT_BYTES, PROGRESS_TAIL_LINES, RESTORE_PHASES,
};
use weirkeeper::verification::{EvidenceRef, VerificationResult, VerificationVerdict};

/// A verification oracle that answers `Valid` — the only verdict that lets
/// `status.records` and `status.capture` be written (defect STATUS-RECORDS).
fn valid_evidence(r: EvidenceRef) -> BoxFuture<'static, VerificationResult> {
    Box::pin(async move {
        VerificationResult {
            result: VerificationVerdict::Valid,
            matched_key_id: Some("test-key".to_string()),
            payload_type: r.payload_type.to_string(),
            verified_at: utc(2026, 11, 9, 3, 20),
            detail: None,
            trust: None,
        }
    })
}

/// **D3 §13's "Mount failure" controller row, first half.** The reconcile
/// WRITES the diagnostic, and it does it off the pod and the events and
/// nothing else.
#[tokio::test]
async fn a_credential_reference_that_is_not_there_reaches_the_status_within_one_requeue() {
    let (client, _seen, bodies) = mock_client_recording_bodies(running_routes_with(
        pod_list_waiting(
            "CreateContainerConfigError",
            "secret \\\"logweir-archive\\\" not found",
        ),
        String::new(),
    ));
    reconcile_backup(
        &frozen_backup(),
        &client,
        &unobserved_archive,
        &unverified_evidence,
        // 3 minutes in: past every grace, inside the 300 s fail-fast window.
        utc(2026, 11, 9, 3, 20),
    )
    .await
    .expect("the reconcile succeeds");
    let status = &patched_statuses(&bodies.lock().expect("the body recorder is readable"))[0];
    let d = &status["progress"]["diagnostics"][0];
    assert_eq!(
        d["code"].as_str(),
        Some("CredentialSecretNotFound"),
        "the SPECIFIC code, which is what tells an operator to create a Secret rather than add \
         a key to one: {status}"
    );
    assert_eq!(d["severity"].as_str(), Some("Error"));
    assert_eq!(d["object"]["kind"].as_str(), Some("Pod"));
    assert_eq!(d["object"]["name"].as_str(), Some(POD));
    assert_eq!(d["count"].as_i64(), Some(1));
    assert!(
        d["message"]
            .as_str()
            .is_some_and(|m| m.contains("logweir-archive")),
        "the message NAMES the Secret — a diagnostic nobody can act on is no diagnostic. It is \
         already a reference in the spec every viewer of this namespace can read: {d}"
    );
    let (state, reason, _) =
        condition_named(status, "RunnerReady").expect("the RunnerReady condition");
    assert_eq!(
        (state.as_str(), reason.as_str()),
        ("False", "CredentialReferenceMissing"),
        "D3 §15's L1 asserts exactly this pair live"
    );
    assert_eq!(status["progress"]["stage"].as_str(), Some("Preparing"));
    assert!(
        status["exitCode"].is_null() && status["phase"].as_str() == Some("Running"),
        "nothing is terminal yet: the Job has not failed, and a controller that decided the \
         outcome from a waiting pod would be inventing one: {status}"
    );
}

/// **D3 §13's "Mount failure" controller row, second half.** After
/// `failFastSeconds` of the same non-transient diagnostic, the Job's deadline
/// is collapsed — through **D2's `cancel.rs`**, and only after the status
/// recording it has landed.
#[tokio::test]
async fn a_run_that_cannot_start_has_its_job_deadline_collapsed_after_the_fail_fast_window() {
    // The object already carries the diagnostic, first seen five minutes ago.
    let mut stuck = frozen_backup();
    let seen_at = utc(2026, 11, 9, 3, 15);
    let stored = json!({
        "stage": "Preparing",
        "reason": "CredentialReferenceMissing",
        "lastTransitionTime": seen_at,
        "lastObservedTime": seen_at,
        "diagnostics": [{
            "code": "CredentialSecretNotFound",
            "severity": "Error",
            "message": "secret \"logweir-archive\" not found",
            "object": {"kind": "Pod", "name": POD},
            "firstSeen": seen_at,
            "lastSeen": seen_at,
            "count": 5,
        }],
    });
    stuck.status.as_mut().expect("a status").progress =
        Some(serde_json::from_value(stored).expect("the stored block is a RunProgress"));

    let (client, seen, bodies) = mock_client_recording_bodies(running_routes_with(
        pod_list_waiting(
            "CreateContainerConfigError",
            "secret \\\"logweir-archive\\\" not found",
        ),
        String::new(),
    ));
    reconcile_backup(
        &stuck,
        &client,
        &unobserved_archive,
        &unverified_evidence,
        // Exactly 300 s after `firstSeen` — the default `failFastSeconds`.
        utc(2026, 11, 9, 3, 20),
    )
    .await
    .expect("the reconcile succeeds");

    let bodies = bodies
        .lock()
        .expect("the body recorder is readable")
        .clone();
    let job_patch = bodies
        .iter()
        .find(|b| b.method == "PATCH" && b.uri.contains("/jobs/"))
        .expect("the Job's deadline is patched");
    let patch: Value = serde_json::from_str(&job_patch.body).expect("the patch is JSON");
    assert_eq!(
        patch["spec"]["activeDeadlineSeconds"].as_i64(),
        Some(1),
        "D2's `cancel.rs` and not a second cancellation path: the weirkeeper ClusterRole grants \
         `delete` on nothing, and collapsing the deadline makes the Job FAIL with \
         `DeadlineExceeded` so the existing crashed-Job path runs and the object stays around \
         to say what happened. Got {patch}"
    );
    assert_eq!(
        patch.as_object().map(serde_json::Map::len),
        Some(1),
        "and it patches the deadline and NOTHING else: {patch}"
    );
    // ORDER: the status that RECORDS the diagnostic lands BEFORE the
    // cancellation, because the crashed pass reads that record to name the
    // terminal state.
    let calls = calls(&bodies);
    let status_at = calls
        .iter()
        .position(|(m, p)| m == "PATCH" && p.contains("/backups/"));
    let cancel_at = calls
        .iter()
        .position(|(m, p)| m == "PATCH" && p.contains("/jobs/"));
    assert!(
        matches!((status_at, cancel_at), (Some(s), Some(c)) if s < c),
        "the deadline patch is sent AFTER the status write it is justified by: {calls:?}"
    );
    assert!(
        seen.lock()
            .expect("the recorder is readable")
            .iter()
            .all(|r| r.method != "DELETE"),
        "nothing is deleted, ever"
    );
}

/// **D3 §13's "Unschedulable pod" controller row.** The diagnostic is written
/// and NO fail-fast patch is issued — D3 §15's L2.
///
/// MUTANT: moving `PodUnschedulable` out of the transient class makes this row
/// fail on the Job patch.
#[tokio::test]
async fn an_unschedulable_run_is_reported_and_left_to_its_own_deadline() {
    let unschedulable = {
        let owners = owned_by_job();
        format!(
            r#"{{"apiVersion":"v1","kind":"PodList","metadata":{{}},"items":[
  {{"apiVersion":"v1","kind":"Pod",
    "metadata":{{"name":"{POD}","namespace":"{NS}","ownerReferences":{owners},
      "creationTimestamp":"2026-11-09T03:17:00Z",
      "labels":{{"{JOB_NAME_LABEL}":"{NAME}"}}}},
    "spec":{{"containers":[]}},
    "status":{{"phase":"Pending",
      "conditions":[{{"type":"PodScheduled","status":"False","reason":"Unschedulable",
        "message":"0/1 nodes are available: 1 Insufficient memory.",
        "lastTransitionTime":"2026-11-09T03:17:05Z"}}],
      "containerStatuses":[
      {{"name":"runner","ready":false,"restartCount":0,"image":"x","imageID":"x",
        "state":{{"waiting":{{"reason":"ContainerCreating"}}}}}}
    ]}}}}]}}"#
        )
    };
    // The object has carried the finding for an hour.
    let mut stuck = frozen_backup();
    let seen_at = utc(2026, 11, 9, 2, 20);
    stuck.status.as_mut().expect("a status").progress = Some(
        serde_json::from_value(json!({
            "stage": "Preparing",
            "reason": "PodUnschedulable",
            "lastTransitionTime": seen_at,
            "lastObservedTime": seen_at,
            "diagnostics": [{
                "code": "PodUnschedulable", "severity": "Warning",
                "object": {"kind": "Pod", "name": POD},
                "firstSeen": seen_at, "lastSeen": seen_at, "count": 60,
            }],
        }))
        .expect("a RunProgress"),
    );
    let (client, _seen, bodies) =
        mock_client_recording_bodies(running_routes_with(unschedulable, String::new()));
    reconcile_backup(
        &stuck,
        &client,
        &unobserved_archive,
        &unverified_evidence,
        utc(2026, 11, 9, 3, 20),
    )
    .await
    .expect("the reconcile succeeds");
    let bodies = bodies
        .lock()
        .expect("the body recorder is readable")
        .clone();
    assert!(
        !bodies
            .iter()
            .any(|b| b.method == "PATCH" && b.uri.contains("/jobs/")),
        "AN HOUR of being unschedulable issues no fail-fast patch. A node can join a cluster, a \
         pod can be preempted and a cluster autoscaler exists — so this is reported and left to \
         the Job's own `activeDeadlineSeconds`, which is D3 §15's L2 verbatim: {:?}",
        calls(&bodies)
    );
    let status = &patched_statuses(&bodies)[0];
    assert_eq!(
        status["progress"]["diagnostics"][0]["code"].as_str(),
        Some("PodUnschedulable")
    );
    assert_eq!(
        status["progress"]["diagnostics"][0]["severity"].as_str(),
        Some("Warning"),
        "it may still resolve, so it is a warning and not an error"
    );
    assert!(
        status["progress"]["diagnostics"][0]["message"]
            .as_str()
            .is_some_and(|m| m.contains("unschedulable")),
        "the message says what is happening; got {}",
        status["progress"]["diagnostics"][0]
    );
    assert_eq!(
        status["progress"]["diagnostics"][0]["count"].as_i64(),
        Some(61),
        "the count moves with the heartbeat and not with the requeue: an hour of 15-second \
         reconciles is 60 minutes, not 240 passes"
    );
}

/// The terminal state a fail-fast cancellation produces on the NEXT pass —
/// D3 §2.2's "replaces `NoExitCode` only when the matching diagnostic was
/// recorded before the Job ended".
#[tokio::test]
async fn the_recorded_diagnostic_names_the_terminal_state_of_a_cancelled_run() {
    let mut stuck = frozen_backup();
    let seen_at = utc(2026, 11, 9, 3, 15);
    stuck.status.as_mut().expect("a status").progress = Some(
        serde_json::from_value(json!({
            "stage": "Preparing",
            "reason": "CredentialReferenceMissing",
            "diagnostics": [{
                "code": "CredentialSecretNotFound", "severity": "Error",
                "message": "secret \"logweir-archive\" not found",
                "object": {"kind": "Pod", "name": POD},
                "firstSeen": seen_at, "lastSeen": seen_at, "count": 5,
            }],
        }))
        .expect("a RunProgress"),
    );
    // The Job failed on the deadline the fail-fast collapsed; the pod never
    // ran, so there is no terminated state and no exit code.
    let (client, _seen, bodies) = mock_client_recording_bodies(finished_routes(
        &pod_list_untermined(
            r#""phase":"Pending","containerStatuses":[
               {"name":"runner","ready":false,"restartCount":0,"image":"x","imageID":"x",
                "state":{"waiting":{"reason":"CreateContainerConfigError"}}}]"#,
        ),
        log_body(""),
        200,
        "Failed",
    ));
    reconcile_backup(
        &stuck,
        &client,
        &unobserved_archive,
        &unverified_evidence,
        utc(2026, 11, 9, 3, 25),
    )
    .await
    .expect("the reconcile succeeds");
    let status = &patched_statuses(&bodies.lock().expect("the body recorder is readable"))[0];
    let (_, reason, _) = condition_named(status, "Failed").expect("a Failed condition");
    assert_eq!(
        reason, "CredentialReferenceMissing",
        "and NOT `NoExitCode`. The pod never started, so there is no code to lift — but the \
         controller wrote down WHY three passes ago, and `NoExitCode` would throw that away and \
         tell the operator nothing: {status}"
    );
    assert!(
        status["exitCode"].is_null(),
        "`exitCode` stays ABSENT in all four of D3 §2.2's new states — a fabricated 1 is \
         indistinguishable from a real operational failure: {status}"
    );
    assert_eq!(status["phase"].as_str(), Some("Failed"));
    assert_eq!(
        status["progress"]["stage"].as_str(),
        Some("Finished"),
        "and the progress block says the run is over, or D3 §2.5's staleness row would call a \
         finished run `unknown` five minutes later"
    );
    assert!(
        status["progress"]["lastObservedTime"].is_null(),
        "with the heartbeat cleared by an explicit null"
    );
}

/// The diagnostic does NOT override a terminal state the pod itself answers.
///
/// MUTANT: applying the recorded state unconditionally loses `DisruptedMidDrill`
/// and `PodUnschedulable`, both of which are stronger observations.
#[tokio::test]
async fn a_pod_that_answers_for_itself_is_not_overridden_by_a_recorded_diagnostic() {
    let mut stuck = frozen_backup();
    stuck.status.as_mut().expect("a status").progress = Some(
        serde_json::from_value(json!({
            "stage": "Preparing",
            "diagnostics": [{
                "code": "CredentialSecretNotFound", "severity": "Error",
                "object": {"kind": "Pod", "name": POD},
                "firstSeen": utc(2026, 11, 9, 3, 15),
                "lastSeen": utc(2026, 11, 9, 3, 15), "count": 1,
            }],
        }))
        .expect("a RunProgress"),
    );
    let (client, _seen, bodies) = mock_client_recording_bodies(finished_routes(
        &pod_list_untermined(
            r#""phase":"Pending","conditions":[
               {"type":"DisruptionTarget","status":"True",
                "lastTransitionTime":"2026-11-09T03:19:00Z"}]"#,
        ),
        log_body(""),
        200,
        "Failed",
    ));
    reconcile_backup(
        &stuck,
        &client,
        &unobserved_archive,
        &unverified_evidence,
        utc(2026, 11, 9, 3, 25),
    )
    .await
    .expect("the reconcile succeeds");
    let status = &patched_statuses(&bodies.lock().expect("the body recorder is readable"))[0];
    let (_, reason, _) = condition_named(status, "Failed").expect("a Failed condition");
    assert_eq!(
        reason, "DisruptedMidDrill",
        "the node going away is what happened to THIS run, now; the recorded diagnostic is what \
         was happening before. `crash_terminal_state`'s existing table is unchanged"
    );
}

/// **D3 §13's "Completed Job cleanup" row, the repair half.** A terminal
/// object whose finished Job has no TTL gets one on the next reconcile.
#[tokio::test]
async fn a_terminal_object_whose_job_lost_its_ttl_has_it_repaired() {
    let mut terminal = frozen_backup();
    terminal.status.as_mut().expect("a status").phase = Some("Succeeded".to_string());
    terminal.status.as_mut().expect("a status").exit_code = Some(0);

    let (client, _seen, bodies) = mock_client_recording_bodies(finished_routes(
        &pod_list_terminated(0),
        log_body(&i7_tail()),
        200,
        "Complete",
    ));
    let outcome = reconcile_backup(
        &terminal,
        &client,
        &unobserved_archive,
        &unverified_evidence,
        utc(2026, 11, 9, 3, 25),
    )
    .await
    .expect("the reconcile succeeds");
    assert!(
        outcome.ttl_patched,
        "the TTL is repaired: the step-2b guard gives up RETRYING the pass that made the object \
         terminal, and the TTL is the half of that pass whose loss is silent — a finished Job \
         with no TTL is never collected and sits in somebody's quota for ever"
    );
    let bodies = bodies
        .lock()
        .expect("the body recorder is readable")
        .clone();
    assert_eq!(
        status_patch_count(&bodies),
        0,
        "and NOTHING is written to the status: the pod is not read, the exit code is not \
         re-derived, and step 2b's whole point is preserved: {:?}",
        calls(&bodies)
    );
    let patch: Value = serde_json::from_str(
        &bodies
            .iter()
            .find(|b| b.method == "PATCH" && b.uri.contains("/jobs/"))
            .expect("the Job is patched")
            .body,
    )
    .expect("the patch is JSON");
    assert_eq!(
        patch["spec"]["ttlSecondsAfterFinished"].as_i64(),
        Some(i64::from(TTL_SECONDS_AFTER_FINISHED)),
        "with the configured value, whose default is the compiled-in seven days"
    );
    assert!(
        !bodies.iter().any(|b| b.uri.contains("/pods")),
        "no pod list and no log read: {:?}",
        calls(&bodies)
    );
}

/// …and a Job that already has a TTL, or that this object does not own, is not
/// touched.
///
/// MUTANT: dropping the owner check makes the second arm patch a stranger's
/// Job — which is deleting somebody else's work on a timer.
#[tokio::test]
async fn the_ttl_repair_never_patches_a_job_that_has_one_or_that_is_not_ours() {
    let mut terminal = frozen_backup();
    terminal.status.as_mut().expect("a status").phase = Some("Succeeded".to_string());

    // ARM 1: the Job already has a TTL.
    let with_ttl = {
        let mut v: Value = serde_json::from_str(&job_body("Complete")).expect("JSON");
        v["spec"]["ttlSecondsAfterFinished"] = json!(604_800);
        v.to_string()
    };
    let mut routes = finished_routes(
        &pod_list_terminated(0),
        log_body(&i7_tail()),
        200,
        "Complete",
    );
    routes[0].body = with_ttl;
    let (client, _seen, bodies) = mock_client_recording_bodies(routes);
    let outcome = reconcile_backup(
        &terminal,
        &client,
        &unobserved_archive,
        &unverified_evidence,
        utc(2026, 11, 9, 3, 25),
    )
    .await
    .expect("the reconcile succeeds");
    assert!(!outcome.ttl_patched, "there is nothing to repair");
    assert!(
        !bodies
            .lock()
            .expect("readable")
            .iter()
            .any(|b| b.method == "PATCH"),
        "and a repair that re-sent the value every reconcile would be the write loop E11(d) is \
         about"
    );

    // ARM 2: a Job of the same name that this Backup does not control. The
    // compatibility guard refuses it before the repair is even reached, which
    // is the belt; `repair_ttl`'s own owner check is the braces.
    let foreign = {
        let mut v: Value = serde_json::from_str(&job_body("Complete")).expect("JSON");
        v["metadata"]["ownerReferences"][0]["uid"] = json!("someone-elses-uid");
        v.to_string()
    };
    let mut routes = finished_routes(
        &pod_list_terminated(0),
        log_body(&i7_tail()),
        200,
        "Complete",
    );
    routes[0].body = foreign;
    let (client, _seen, bodies) = mock_client_recording_bodies(routes);
    reconcile_backup(
        &terminal,
        &client,
        &unobserved_archive,
        &unverified_evidence,
        utc(2026, 11, 9, 3, 25),
    )
    .await
    .expect("the reconcile succeeds");
    assert!(
        !bodies
            .lock()
            .expect("readable")
            .iter()
            .any(|b| b.method == "PATCH"),
        "a Job's NAME proves nothing. Patching a stranger's Job with a TTL is deleting somebody \
         else's work on a timer"
    );
}

// ---------------------------------------------------------------------------
// Defect STATUS-RECORDS — `Backup.status.records`
// ---------------------------------------------------------------------------

/// The signed receipt this suite's archive oracle serves.
fn receipt_document() -> Value {
    json!({
        "format_version": "1.0.0",
        "run_id": "r1",
        "backup_id": UID,
        "requested_at": "2026-11-09T03:17:00Z",
        "started_at": "2026-11-09T03:17:30Z",
        "finished_at": "2026-11-09T03:19:00Z",
        "exit_code": 0,
        "triggered_by": "manual",
        "records": {"orders": 120_000_u64, "payments": 4_096_u64},
        "covered": {"from_ms": 1_760_000_000_000_i64, "to_ms": 1_760_000_060_000_i64},
    })
}

/// **Defect STATUS-RECORDS.** `status.records` is written, and it is the sum
/// of the receipt's per-topic counts.
#[test]
fn the_record_count_is_the_sum_of_the_receipts_own_per_topic_counts() {
    assert_eq!(
        records_from_receipt(&receipt_document()),
        Some(124_096),
        "`Backup.status.records` is ONE integer and the receipt counts per topic, so the answer \
         is their sum — which is what the CRD's field description and the RECORDS printer \
         column have always said. The per-topic breakdown stays where it is ATTESTED"
    );
    assert_eq!(
        records_from_receipt(&json!({"records": {}})),
        Some(0),
        "a run over an empty topic set counted zero records, which is a fact"
    );
    assert_eq!(
        records_from_receipt(&json!({})),
        None,
        "MUTANT: a receipt with no `records` block is UNREAD, not zero. A blank column is \
         honest and a zero is a claim"
    );
    assert_eq!(
        records_from_receipt(&json!({"records": {"orders": -1}})),
        None,
        "and a value that is not a count is not counted"
    );
    assert_eq!(
        capture_from_receipt(&receipt_document()).map(|(s, f)| (s.to_rfc3339(), f.to_rfc3339())),
        Some((
            "2026-11-09T03:17:30+00:00".to_string(),
            "2026-11-09T03:19:00+00:00".to_string()
        )),
        "and `status.capture` is copied verbatim from the same document"
    );
    assert_eq!(
        capture_from_receipt(&json!({"started_at": "2026-11-09T03:17:30Z"})),
        None,
        "both instants or neither — a half-read window cannot be told apart from one that is \
         genuinely open-ended"
    );
}

/// **Defect STATUS-RECORDS, through the reconciler.** The count reaches the
/// status only on a VERIFIED receipt.
#[tokio::test]
async fn the_record_count_is_written_from_a_verified_receipt_and_from_nothing_else() {
    let bytes = serde_json::to_vec(&receipt_document()).expect("the receipt serialises");
    let observation = move |_keys: EvidenceKeys| -> BoxFuture<'static, Option<ArchiveObservation>> {
        let doc: Value = serde_json::from_slice(&bytes).expect("JSON");
        Box::pin(async move {
            Some(ArchiveObservation {
                presence: EvidencePresence {
                    payload: true,
                    sidecar: true,
                },
                covered: covered_from_receipt(&doc),
                receipt_sha256: Some("sha256:deadbeef".to_string()),
                records: records_from_receipt(&doc),
                capture: capture_from_receipt(&doc),
            })
        })
    };

    // ARM 1: `Valid`.
    let (client, _seen, bodies) = mock_client_recording_bodies(finished_routes(
        &pod_list_terminated(0),
        log_body(&i7_tail()),
        200,
        "Complete",
    ));
    reconcile_backup(
        &frozen_backup(),
        &client,
        &observation,
        &valid_evidence,
        utc(2026, 11, 9, 3, 20),
    )
    .await
    .expect("the reconcile succeeds");
    let statuses = patched_statuses(&bodies.lock().expect("the body recorder is readable"));
    let second = statuses.last().expect("the verification patch");
    assert_eq!(
        second["records"].as_i64(),
        Some(124_096),
        "defect STATUS-RECORDS: the field the CRD has declared since the kind existed, with a \
         RECORDS printer column, was blank on every Backup the PLAT-06.1 and PLAT-07.1 live \
         runs produced while the counts sat in the signed receipt. Got: {second}"
    );
    assert_eq!(
        second["capture"]["startedAt"].as_str(),
        Some("2026-11-09T03:17:30Z"),
        "and D3 §2.2's capture window beside it — `ProtectionPolicy` measures freshness from \
         `startedAt`, because a four-hour backup that STARTED at 02:00 protects you to 02:00"
    );
    assert!(
        statuses[0]["records"].is_null(),
        "ON THE VERIFICATION PATCH AND NO OTHER. The terminal patch speaks before any signature \
         has been checked: {}",
        statuses[0]
    );

    // ARM 2: the same bytes, no verification. NOTHING is written.
    let (client, _seen, bodies) = mock_client_recording_bodies(finished_routes(
        &pod_list_terminated(0),
        log_body(&i7_tail()),
        200,
        "Complete",
    ));
    reconcile_backup(
        &frozen_backup(),
        &client,
        &observation,
        &unverified_evidence,
        utc(2026, 11, 9, 3, 20),
    )
    .await
    .expect("the reconcile succeeds");
    for status in patched_statuses(&bodies.lock().expect("the body recorder is readable")) {
        assert!(
            status["records"].is_null() && status["capture"].is_null(),
            "MUTANT: writing the count off the OBSERVATION rather than the VERIFIED receipt. A \
             count on a Backup has to be a count some key this installation accepts attested \
             to, or the RECORDS column is a number anybody who can write to the bucket chose. \
             Got: {status}"
        );
    }
}

// ---------------------------------------------------------------------------
// D3 W5 review finding F5 — the key-scan window's budget
// ---------------------------------------------------------------------------

/// The controller's key-scan window has room for everything a passing restore
/// prints, and the budget is stated rather than assumed.
///
/// **MIRRORED** from `crates/logweir/tests/progress_channel.rs`'s
/// `the_trailing_lines_a_passing_restore_prints_fit_the_controllers_scan_window`,
/// which measures the count by running the real runner. A `weirkeeper`
/// dependency in that crate would invert the layering, so the number lives on
/// both sides and each side names the other. This is the side that answers
/// "does the window still fit"; that side answers "did the runner grow".
#[test]
fn the_key_scan_window_has_room_for_the_runners_trailing_block() {
    assert_eq!(
        BUDGETED_TRAILING_LINES, 7,
        "measured on a passing restore at execution contract v2: the summary, \
         `topic-preflight=`, `teardown-key=`, `scorecard-key=`, `sidecar-key=`, \
         `offset-report-key=`, and the `drill finished` line production's `tracing` subscriber \
         emits. If the runner grows a line, raise this — and then this row says whether the \
         window still fits"
    );
    // `black_box` because both sides are compile-time constants and clippy
    // folds a const comparison into `assert!(true)` and refuses it. The
    // comparison is the whole point of the row, so it is kept opaque rather
    // than dropped.
    let budget = std::hint::black_box(BUDGETED_TRAILING_LINES);
    let window = std::hint::black_box(KEY_SCAN_TAIL_LINES);
    assert!(
        budget < window,
        "the scan matches by key NAME, so a key pushed out of the window is a SILENTLY ABSENT \
         status field and not an error. That is why this is a budget and not a hope"
    );
    assert!(
        window - budget >= 8,
        "…with room for a whole second trailing block. At eight the margin was ONE line (D3 W5 \
         review finding F5), which is why the constant was raised rather than the runner trimmed"
    );

    // AND THE SCAN FINDS EVERY KEY AT THE MEASURED SHAPE. A budget nobody
    // exercises is arithmetic.
    let tail = format!(
        "run r1 outcome=pass\n\
         topic-preflight={{\"ok\":true}}\n\
         teardown-key=logweir/drills/r1.teardown.json\n\
         scorecard-key=logweir/drills/r1.json\n\
         sidecar-key={SIDECAR_KEY}\n\
         offset-report-key=logweir/drills/r1.offsets.json\n\
         {{\"level\":\"INFO\",\"message\":\"drill finished\"}}\n"
    );
    let long_log = format!("{}{tail}", "noise\n".repeat(200));
    let scanned = tail_lines(&long_log);
    for prefix in [
        "topic-preflight=",
        "teardown-key=",
        "scorecard-key=",
        "sidecar-key=",
        "offset-report-key=",
    ] {
        assert!(
            scanned.iter().any(|l| l.starts_with(prefix)),
            "`{prefix}` is inside the {KEY_SCAN_TAIL_LINES}-line window: {scanned:?}"
        );
    }
    // The BACKUP tail too — I7's two keys plus D3 W3's conditional one.
    let backup_tail = format!(
        "run r1 outcome=ok\n\
         catalog-key=logweir/catalog/v1/p.json\n\
         receipt-key={RECEIPT_KEY}\n\
         sidecar-key={SIDECAR_KEY}\n\
         {{\"level\":\"INFO\",\"message\":\"backup finished\"}}\n"
    );
    let keys = evidence_keys(&format!("{}{backup_tail}", "noise\n".repeat(200)));
    assert_eq!(keys.receipt.as_deref(), Some(RECEIPT_KEY));
    assert_eq!(keys.sidecar.as_deref(), Some(SIDECAR_KEY));
    // A WIDER window is safe by construction, and this says why: the scan
    // takes the LAST occurrence of each prefix.
    let redrafted = format!("receipt-key=logweir/draft.json\n{backup_tail}");
    assert_eq!(
        evidence_keys(&redrafted).receipt.as_deref(),
        Some(RECEIPT_KEY),
        "a runner that logged an earlier draft of the key has the FINAL one be the one that was \
         written — which is why widening the window can only find a key it would otherwise have \
         missed, never a different one"
    );
}

/// The progress read's own bounds are D3 §2.4's, and the two vocabularies are
/// the runner's.
#[test]
fn the_progress_read_is_bounded_and_its_vocabularies_are_the_runners() {
    assert_eq!(PROGRESS_TAIL_LINES, 50, "D3 §2.4's `tail_lines`");
    assert_eq!(PROGRESS_LIMIT_BYTES, 65_536, "D3 §2.4's `limit_bytes`");
    assert_eq!(
        RESTORE_PHASES.len(),
        10,
        "phases 0..9, the drill's own, in its own order"
    );
    assert_eq!(BACKUP_STEPS.len(), 5, "and the backup's five named steps");
    assert_eq!(
        diagnostics::EVENT_LIMIT,
        20,
        "D3 §2.3's `limit=20` per events list — a namespace with ten thousand events must not \
         make a diagnostic the expensive part of a reconcile"
    );
    for name in RESTORE_PHASES.iter().chain(BACKUP_STEPS.iter()) {
        assert!(
            !name.contains(':') && name.len() <= 32,
            "`{name}` has to survive `<n>:<name>` and the CRD's 32-byte bound"
        );
    }
}

// ===========================================================================
// Review round 1 — F1, F4 and Q1
// ===========================================================================

/// **F1: every terminal builder carries `RunnerReady` forward.**
///
/// D3 §2.2: "Every terminal builder carries `RunnerReady` and `Verified`
/// forward". It did not. A JSON merge patch REPLACES arrays, so the terminal
/// patch that RECORDS a failure was deleting the one condition that says what
/// the failure was — and a terminal object is never reconciled again, so no
/// later pass rewrote it. D3 §15's L1 asserts that condition live, **after**
/// the fail-fast terminal patch.
///
/// MUTANT: dropping `CONDITION_RUNNER_READY` from `backup::carry_conditions`.
#[tokio::test]
async fn no_terminal_patch_deletes_the_condition_that_says_why_the_run_failed() {
    let stored_runner_ready = json!([{
        "type": "RunnerReady",
        "status": "False",
        "reason": "CredentialReferenceMissing",
        "message": "secret \"logweir-archive\" not found",
        "lastTransitionTime": "2026-11-09T03:15:00Z",
    }]);

    // Every terminal path, through the reconciler, with the condition already
    // on the object exactly as the progress path left it.
    let cases: Vec<(&str, String, &str)> = vec![
        (
            "the crashed-Job path (what a fail-fast cancellation produces)",
            pod_list_untermined(
                r#""phase":"Pending","containerStatuses":[
                   {"name":"runner","ready":false,"restartCount":0,"image":"x","imageID":"x",
                    "state":{"waiting":{"reason":"CreateContainerConfigError"}}}]"#,
            ),
            "Failed",
        ),
        (
            "the ordinary finished path, exit 0",
            pod_list_terminated(0),
            "Complete",
        ),
        (
            "the ordinary finished path, exit 2",
            pod_list_terminated(2),
            "Failed",
        ),
    ];

    for (label, pods, condition) in cases {
        let mut object = frozen_backup();
        let status = object.status.as_mut().expect("a status");
        status.conditions =
            Some(serde_json::from_value(stored_runner_ready.clone()).expect("conditions"));
        let (client, _seen, bodies) = mock_client_recording_bodies(finished_routes(
            &pods,
            log_body(&i7_tail()),
            200,
            condition,
        ));
        reconcile_backup(
            &object,
            &client,
            &unobserved_archive,
            &unverified_evidence,
            utc(2026, 11, 9, 3, 25),
        )
        .await
        .unwrap_or_else(|e| panic!("[{label}] the reconcile succeeds: {e}"));
        let statuses = patched_statuses(&bodies.lock().expect("the body recorder is readable"));
        for (n, status) in statuses.iter().enumerate() {
            let (state, reason, _) = condition_named(status, "RunnerReady").unwrap_or_else(|| {
                panic!(
                    "[{label}] patch {n} dropped `RunnerReady`. A merge patch replaces arrays, \
                     so the terminal patch that records the failure would delete the only \
                     condition that says what the failure WAS — and a terminal object is never \
                     reconciled again, so nothing rewrites it. Got: {status}"
                )
            });
            assert_eq!(
                (state.as_str(), reason.as_str()),
                ("False", "CredentialReferenceMissing"),
                "[{label}] patch {n} carries it VERBATIM — the terminal builder does not own \
                 this condition and must not re-derive it from a pod that may already be \
                 collected"
            );
        }
    }
}

/// …and the refusal path, which never reaches a Job at all.
#[tokio::test]
async fn a_controller_refusal_carries_the_runner_ready_condition_too() {
    let mut object = backup();
    object.metadata.name = Some("x".repeat(crate_name_limit() + 1));
    object.status = Some(BackupStatus {
        conditions: Some(
            serde_json::from_value(json!([{
                "type": "RunnerReady", "status": "True", "reason": "RunnerStarted",
                "message": "the `runner` container has been seen running or terminated",
                "lastTransitionTime": "2026-11-09T03:15:00Z",
            }]))
            .expect("conditions"),
        ),
        ..BackupStatus::default()
    });
    let (client, _seen, bodies) = mock_client_recording_bodies(vec![Route {
        method: "PATCH",
        path_suffix: "/status",
        status: 200,
        body: backup_json(),
    }]);
    reconcile_backup(
        &object,
        &client,
        &unobserved_archive,
        &unverified_evidence,
        utc(2026, 11, 9, 3, 25),
    )
    .await
    .expect("a self-decided refusal is an outcome, not an error");
    let status = &patched_statuses(&bodies.lock().expect("the body recorder is readable"))[0];
    assert!(
        condition_named(status, "RunnerReady").is_some(),
        "`refused_status_patch` owns `Failed` and nothing else; every other condition on the \
         object is a debt it owes. Got: {status}"
    );
}

/// `slot::NAME_LIMIT`, without importing the module into this file's namespace.
fn crate_name_limit() -> usize {
    weirkeeper::slot::NAME_LIMIT
}

/// **F4: the STATUS-RECORDS guard refuses every verdict that is not `Valid`.**
///
/// The shipped code was already correct; the GUARD was not. The reviewer's
/// mutant widened `== Valid` to `!= NotAttempted` and survived, because the
/// only negative arm used `unverified_evidence` — which answers `NotAttempted`
/// — so `Invalid` and `Untrusted`, the two verdicts that mean *a receipt was
/// presented and this installation refused it*, were never exercised.
///
/// MUTANT: any widening of the condition beyond `Valid`.
#[tokio::test]
async fn a_receipt_this_installation_refuses_never_populates_the_records_column() {
    let bytes = serde_json::to_vec(&receipt_document()).expect("the receipt serialises");
    let observation = move |_keys: EvidenceKeys| -> BoxFuture<'static, Option<ArchiveObservation>> {
        let doc: Value = serde_json::from_slice(&bytes).expect("JSON");
        Box::pin(async move {
            Some(ArchiveObservation {
                presence: EvidencePresence {
                    payload: true,
                    sidecar: true,
                },
                covered: covered_from_receipt(&doc),
                receipt_sha256: Some("sha256:deadbeef".to_string()),
                records: records_from_receipt(&doc),
                capture: capture_from_receipt(&doc),
            })
        })
    };

    // EVERY verdict that is not `Valid`, each with what it means.
    let refused: Vec<(&str, VerificationVerdict, &str)> = vec![
        (
            "Invalid",
            VerificationVerdict::Invalid,
            "the bytes do not match the signature — the receipt is not the document that was \
             signed",
        ),
        (
            "Untrusted",
            VerificationVerdict::Untrusted,
            "the signature verifies and the key is one this installation will NOT accept — \
             revoked, unknown, or holding the wrong usage",
        ),
        (
            "NotAttempted",
            VerificationVerdict::NotAttempted,
            "no credential, or the document could not be fetched at all",
        ),
    ];

    for (label, verdict, means) in refused {
        let oracle = move |r: EvidenceRef| -> BoxFuture<'static, VerificationResult> {
            Box::pin(async move {
                VerificationResult {
                    result: verdict,
                    matched_key_id: None,
                    payload_type: r.payload_type.to_string(),
                    verified_at: utc(2026, 11, 9, 3, 20),
                    detail: Some(means.to_string()),
                    trust: None,
                }
            })
        };
        let (client, _seen, bodies) = mock_client_recording_bodies(finished_routes(
            &pod_list_terminated(0),
            log_body(&i7_tail()),
            200,
            "Complete",
        ));
        reconcile_backup(
            &frozen_backup(),
            &client,
            &observation,
            &oracle,
            utc(2026, 11, 9, 3, 20),
        )
        .await
        .unwrap_or_else(|e| panic!("[{label}] the reconcile succeeds: {e}"));
        for (n, status) in patched_statuses(&bodies.lock().expect("the body recorder is readable"))
            .iter()
            .enumerate()
        {
            assert!(
                status["records"].is_null() && status["capture"].is_null(),
                "[{label}] patch {n}: {means}. A count in the RECORDS column has to be one some \
                 key this installation ACCEPTS attested to — that is the entire justification \
                 for the field, and `{label}` is precisely the case where no such key exists. \
                 Got: {status}"
            );
        }
    }
}

/// **Q1: a DIAGNOSING steady object is as quiet as a healthy one.**
///
/// `merge_diagnostics` rewrites `message` and `severity` on every pass, outside
/// the heartbeat. If a D2 `waiting` message ever embedded a moving token — a
/// kubelet backoff duration, an event instant — every reconcile of a
/// diagnosing object would become a status write and E11(d)'s "zero patches
/// between heartbeats" would be lost for exactly the objects an operator is
/// watching. No current message moves; this row is what notices if one starts.
#[tokio::test]
async fn a_steady_backup_that_is_diagnosing_also_issues_no_second_status_patch() {
    let pods = pod_list_waiting(
        "ImagePullBackOff",
        "Back-off pulling image \\\"logweir:x\\\"",
    );
    let (client, _seen, bodies) =
        mock_client_recording_bodies(running_routes_with(pods.clone(), String::new()));
    reconcile_backup(
        &frozen_backup(),
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
    assert_eq!(status_patch_count(&first), 1, "the first pass records it");
    assert_eq!(
        patched_statuses(&first)[0]["progress"]["diagnostics"][0]["code"].as_str(),
        Some("RunnerImagePullFailed"),
        "…and it IS diagnosing, or this row proves nothing"
    );

    let mut stored = serde_json::to_value(frozen_backup().status).expect("the status serialises");
    apply_merge_patch(&mut stored, &patched_statuses(&first)[0]);
    let mut steady = frozen_backup();
    steady.status = Some(serde_json::from_value::<BackupStatus>(stored).expect("a BackupStatus"));

    let (client, _seen, bodies) =
        mock_client_recording_bodies(running_routes_with(pods, String::new()));
    reconcile_backup(
        &steady,
        &client,
        &unobserved_archive,
        &unverified_evidence,
        utc(2026, 11, 9, 3, 20) + chrono::Duration::seconds(15),
    )
    .await
    .expect("the second reconcile succeeds");
    assert_eq!(
        status_patch_count(&bodies.lock().expect("the body recorder is readable")),
        0,
        "a diagnosing object is as quiet as a healthy one between heartbeats. `count` and \
         `lastSeen` move together and only on the heartbeat; `message` and `severity` are \
         recomputed every pass, so this is the row that fails if a classification message ever \
         starts carrying a clock"
    );
}

/// **F2, the half a pure row cannot reach: a window with no phase line of its
/// own does not retract the phase the object already carries.**
///
/// A run prints one `progress-phase=` line per phase. A phase that lasts two
/// minutes therefore has eight reconciles whose fifty-line window holds only
/// ordinary output — and because `apply` writes the WHOLE `progress` object,
/// anything the read does not return is erased rather than left alone. So
/// "no new phase line" has to mean "the phase has not changed", never "there
/// is no phase".
///
/// MUTANT: dropping the carry in `read_progress`.
#[tokio::test]
async fn a_log_window_carrying_no_phase_line_does_not_blank_the_stored_phase() {
    // The object is mid-run and already knows its phase.
    let mut running = frozen_backup();
    running.status.as_mut().expect("a status").progress = Some(
        serde_json::from_value(json!({
            "stage": "Running",
            "reason": "RunnerStarted",
            "lastTransitionTime": utc(2026, 11, 9, 3, 18),
            "lastObservedTime": utc(2026, 11, 9, 3, 18),
            "runnerPhase": {"number": -1, "name": "readback"},
        }))
        .expect("a RunProgress"),
    );

    // Its log window is ordinary output: the announcement and every phase line
    // scrolled away long ago.
    let (client, _seen, bodies) = mock_client_recording_bodies(running_routes_with(
        pod_list_running(),
        "consuming partition 3\nconsuming partition 4\nflushed 10000 records\n".to_string(),
    ));
    reconcile_backup(
        &running,
        &client,
        &unobserved_archive,
        &unverified_evidence,
        // Past the heartbeat, so the log IS read on this pass.
        utc(2026, 11, 9, 3, 20),
    )
    .await
    .expect("the reconcile succeeds");
    let status = &patched_statuses(&bodies.lock().expect("the body recorder is readable"))[0];
    assert_eq!(
        status["progress"]["runnerPhase"]["name"].as_str(),
        Some("readback"),
        "MUTANT: the phase must SURVIVE a window that says nothing about it. Without the carry \
         `runnerPhase` blinks out on every pass of a long phase — and because D3 §2.5's \
         `Verifying` stage is a pure function of it, `readback` blanking is the API losing \
         `verifying` for the rest of the run. Got: {}",
        status["progress"]
    );
    assert_eq!(
        status["progress"]["stage"].as_str(),
        Some("Verifying"),
        "…which is the consequence: `readback` is one of D3 §2.5's three backup verifying steps"
    );
}
// ===========================================================================
// D2 W10 — destination-backed execution (PLAT-08.1)
//
// The rows here are about ONE question: what does a run that names a saved
// `BackupDestination` address, and what does the controller's own process
// contribute to it. `tests/plan_addressing.rs` holds the two rows that need a
// mutated PROCESS ENVIRONMENT (a separate binary, for its own reason); these
// are the rows that do not.
// ===========================================================================

use weirkeeper::check::policy::{IdentityLocation, Policy};
use weirkeeper::controllers::backup::{
    admit_destination, desired_execution_inputs_for_destination, destination_hold_budget,
    destination_hold_expired, engine_custom_ca_allowed, engine_custom_ca_refusal,
    BackupDestinations, DestinationAdmission, DESTINATION_HOLD_MAX_SECONDS,
};
use weirkeeper::crds::backup_destination::BackupDestination;
use weirkeeper::destination::{
    resolve, DestinationRole, ResolvedDestination, ResolvedGrant, ARCHIVE_CA_PLAN_KEY,
    ARCHIVE_CREDENTIALS_ENV, AWS_ALLOW_HTTP_ENV, AWS_ENDPOINT_URL_ENV, AWS_METADATA_ENDPOINT_ENV,
    AWS_REGION_ENV, AWS_VIRTUAL_HOSTED_ENV, STORE_CONTRACT_VERSION, STORE_CONTRACT_VERSION_ARG,
    STORE_CONTRACT_VERSION_ENV,
};

/// A minimal, PARSEABLE PEM: `30 06 aa 01 02 03 04 05 06`, a DER SEQUENCE of
/// length six. `check_ca_bundle` refuses anything that is not a real PEM
/// bundle, and a fixture that only LOOKS like one would have this row passing
/// for the wrong reason.
const CA_PEM: &str = "-----BEGIN CERTIFICATE-----\nMAaqAQIDBAUG\n-----END CERTIFICATE-----\n";
/// A DIFFERENT bundle, for the digest bind.
const OTHER_CA_PEM: &str = "-----BEGIN CERTIFICATE-----\nMAa7AQIDBAUG\n-----END CERTIFICATE-----\n";

const DEST_A_UID: &str = "d0000000-0000-4000-8000-00000000000a";
const DEST_B_UID: &str = "d0000000-0000-4000-8000-00000000000b";

/// `dest-a`: MinIO over TLS, path-style, a region, a separate read grant, and
/// NO CA bundle — the U1 gate (`ENGINE_CUSTOM_CA_VERIFIED`) refuses a bundle
/// for engine-driven runs by default, and these rows are about addressing
/// rather than about that gate. `a_ca_bundle_is_refused_for_an_engine_driven_run`
/// is the row that carries one.
fn dest_a_value() -> Value {
    serde_json::json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "BackupDestination",
        "metadata": {
            "name": "dest-a", "namespace": NS, "uid": DEST_A_UID,
            "generation": 3, "resourceVersion": "1001"
        },
        "spec": {
            "storage": {
                "provider": "S3", "bucket": "lw-a", "prefix": "team-a/prod",
                "region": "us-east-1", "endpoint": "https://minio-a.storage.svc:9000",
                "addressing": "PathStyle"
            },
            "transport": {"security": "TLS"},
            "access": {
                "archiveWrite": {"mode": "SecretKeys", "secret": {
                    "name": "lw-a-writer",
                    "accessKeyIdKey": "access-key-id",
                    "secretAccessKeyKey": "secret-access-key"
                }},
                "archiveRead": {"mode": "SecretKeys", "secret": {
                    "name": "lw-a-reader",
                    "accessKeyIdKey": "access-key-id",
                    "secretAccessKeyKey": "secret-access-key"
                }},
                "evidenceRead": {"mode": "ArchiveReadGrant"}
            }
        },
        "status": {
            "observedGeneration": 3,
            "conditions": [{
                "type": "Valid", "status": "True", "reason": "Valid",
                "observedGeneration": 3
            }]
        }
    })
}

/// `dest-b`: a DIFFERENT bucket on a DIFFERENT endpoint over EXPLICIT plaintext
/// HTTP, with a different credential and a session token. The second half of
/// the two-destination contract (D2 §3.13).
fn dest_b_value() -> Value {
    serde_json::json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "BackupDestination",
        "metadata": {
            "name": "dest-b", "namespace": NS, "uid": DEST_B_UID,
            "generation": 1, "resourceVersion": "2002"
        },
        "spec": {
            "storage": {
                "provider": "S3", "bucket": "lw-b", "prefix": "",
                "endpoint": "http://minio-b.storage.svc:9000", "addressing": "PathStyle"
            },
            "transport": {"security": "InsecureHTTP"},
            "access": {
                "archiveWrite": {"mode": "SecretKeys", "secret": {
                    "name": "lw-b-writer",
                    "accessKeyIdKey": "id",
                    "secretAccessKeyKey": "key",
                    "sessionTokenKey": "token"
                }}
            }
        },
        "status": {
            "observedGeneration": 1,
            "conditions": [{
                "type": "Valid", "status": "True", "reason": "Valid",
                "observedGeneration": 1
            }]
        }
    })
}

fn destination(value: Value) -> BackupDestination {
    serde_json::from_value(value).expect("the fixture is a BackupDestination")
}

fn resolved(value: Value, role: DestinationRole) -> ResolvedDestination {
    resolve(&destination(value), role, &Policy::defaults()).expect("the fixture resolves")
}

/// The ONE object, resolved for BOTH roles a backup run needs — what
/// `admit_destination` produces. `evidenceWrite` falls back to `archiveWrite`
/// on both fixtures, so the two grants are equal and the Job is told
/// `LOGWEIR_EVIDENCE_CREDENTIALS=archive`.
fn both_roles(value: Value) -> BackupDestinations {
    BackupDestinations {
        archive: resolved(value.clone(), DestinationRole::ArchiveWrite),
        evidence: resolved(value, DestinationRole::EvidenceWrite),
    }
}

/// [`backup`], re-pointed at a saved destination: the sentinel `archive.url`
/// the CEL rule requires, no `secretRef`, and the ref itself.
fn destination_backed_backup(name: &str) -> Backup {
    let mut value: Value = serde_json::from_str(&backup_json()).expect("the fixture is JSON");
    // THE HOLD IS MEASURED FROM `creationTimestamp`, the one timestamp a
    // `Backup` carries that no controller wrote. A fixture without one is not
    // a fixture of anything an API server ever serves.
    value["metadata"]["creationTimestamp"] = serde_json::json!("2026-11-09T03:00:00Z");
    value["spec"]["archive"] = serde_json::json!({
        "url": format!("logweir-destination://{name}")
    });
    value["spec"]["destinationRef"] = serde_json::json!({ "name": name });
    serde_json::from_value(value).expect("the mutated fixture is a Backup")
}

/// **THE FROZEN SNAPSHOT IS WHAT THE JOB IS RENDERED FROM, AND A LATER EDIT TO
/// THE DESTINATION CANNOT REACH A RUN THAT ALREADY EXISTS.**
///
/// # The defect this closes
///
/// D2 §3.7. A runner Job is created once and may be RE-created: garbage
/// collected, a node lost, a controller restarted mid-run. If the second Job
/// were rendered from the destination as it reads NOW, an access-key rotation
/// or a re-pointed endpoint between the freeze and the re-creation would change
/// what an approved, half-written run addresses — silently, with the plan
/// document still saying the old thing.
///
/// KILLS: rendering `runner_job_spec_from_inputs`' destination environment from
/// a freshly resolved `ResolvedDestination` instead of from
/// `inputs.destination`. The mutant passes every row that freezes and renders
/// in one pass; this one freezes against `dest-a` and renders against a
/// snapshot while `dest-b` is what the cluster now holds.
#[test]
fn the_job_is_rendered_from_the_frozen_destination_and_not_from_the_object() {
    let backed = destination_backed_backup("dest-a");
    let pair = both_roles(dest_a_value());
    let a = &pair.archive;
    let frozen = desired_execution_inputs_for_destination(
        &backed,
        &prod_cluster(),
        &weirkeeper::backup_execution::ResolvedSelection::named(&backed.spec),
        Some(&pair),
    )
    .expect("the destination-backed inputs resolve");

    let snapshot = frozen
        .inputs
        .destination
        .as_ref()
        .expect("a destination-backed run freezes its destination");
    assert_eq!(snapshot.uid, DEST_A_UID, "the UID is frozen, not the name");
    assert_eq!(snapshot.generation, 3);
    assert_eq!(
        snapshot.location_digest, a.location_digest,
        "the digest a recovery point is indexed by comes from the resolution"
    );

    // ONE RENDERER, TWO CALLERS. The snapshot's environment is the live
    // resolution's environment, variable for variable — that equality is what
    // makes "rendered from the snapshot" safe rather than a second answer.
    assert_eq!(
        snapshot.job_env().literals,
        a.job_env().literals,
        "the frozen block renders the same environment the live object does"
    );
    assert_eq!(snapshot.job_env().from_secret, a.job_env().from_secret);

    // AND IT IS `dest-a`'s ENVIRONMENT, not `dest-b`'s, even though the two
    // differ on every setting that matters.
    let b = resolved(dest_b_value(), DestinationRole::ArchiveWrite);
    let env = snapshot.job_env();
    assert_eq!(
        env.literal(AWS_ALLOW_HTTP_ENV),
        Some("false"),
        "dest-a declares TLS; dest-b declares InsecureHTTP, and a run frozen against the first \
         must never take the second's transport"
    );
    assert_eq!(b.job_env().literal(AWS_ALLOW_HTTP_ENV), Some("true"));
    assert_eq!(env.literal(AWS_REGION_ENV), Some("us-east-1"));
    assert_eq!(
        env.literal(AWS_VIRTUAL_HOSTED_ENV),
        Some("false"),
        "PathStyle, from the addressing field alone"
    );
    assert_eq!(
        env.literal(AWS_METADATA_ENDPOINT_ENV),
        Some(logweir_store::DEAD_METADATA_ENDPOINT),
        "G16: a missing workload identity is a refusal and never the node's instance role"
    );
    assert_eq!(
        env.literal(STORE_CONTRACT_VERSION_ENV),
        Some(STORE_CONTRACT_VERSION)
    );
    assert_eq!(env.literal(ARCHIVE_CREDENTIALS_ENV), Some("static"));
    assert_eq!(
        env.literal(AWS_ENDPOINT_URL_ENV),
        None,
        "ABSENT BY CONSTRUCTION: the endpoint travels inside the plan's own storage block, where \
         the runner reads it explicitly. A variable `AmazonS3Builder::from_env()` would sweep up \
         is a second, silent answer to `where is the bucket`"
    );

    // THE PLAN'S STORAGE BLOCK IS THE DESTINATION'S LOCATION, and the frozen
    // addressing environment is EMPTY — the controller's process contributes
    // nothing at all to a destination-backed run.
    assert_eq!(frozen.inputs.archive.storage, a.plan_storage());
    assert!(
        frozen.inputs.archive.addressing_env.is_empty(),
        "SEC-ENVHTTP: a destination-backed run freezes no forwarded addressing. Got {:?}",
        frozen.inputs.archive.addressing_env
    );

    // THE HANDSHAKE IS ON THE ARGV.
    let argv = frozen.inputs.runner.args.clone();
    let at = argv
        .iter()
        .position(|a| a == STORE_CONTRACT_VERSION_ARG)
        .unwrap_or_else(|| panic!("the argv carries the store-contract flag: {argv:?}"));
    assert_eq!(
        argv.get(at + 1).map(String::as_str),
        Some(STORE_CONTRACT_VERSION)
    );

    // AND A LEGACY RUN CARRIES NEITHER.
    let legacy = desired_for(&backup());
    assert!(legacy.inputs.destination.is_none());
    assert!(
        !legacy
            .inputs
            .runner
            .args
            .iter()
            .any(|a| a == STORE_CONTRACT_VERSION_ARG),
        "the flag is the promise that every store setting arrives explicitly; on the legacy path \
         it does not, so the flag is absent and an older runner still runs the argv"
    );
}

/// **TWO DESTINATIONS IN ONE NAMESPACE SHARE NO BUCKET, NO CREDENTIAL AND NO
/// EVIDENCE LOCATION** — D2 §3.13's acceptance contract, at the unit layer.
///
/// W14 proves it live; this row proves the CONTROLLER never mixes them, which
/// is the half a live test cannot isolate from a MinIO policy being wrong.
///
/// KILLS: any `job_env` that reads a setting from a source other than the
/// resolution it was called on — the mutant "take `allow_http` from the
/// addressing style" (defect UI-HTTPDOWNGRADE's server-side twin) makes
/// `dest-a` and `dest-b` agree, and this row separates them.
#[test]
fn two_destinations_share_no_bucket_credential_or_evidence_location() {
    let a = resolved(dest_a_value(), DestinationRole::ArchiveWrite);
    let b = resolved(dest_b_value(), DestinationRole::ArchiveWrite);

    assert_ne!(a.plan_storage(), b.plan_storage());
    assert_ne!(a.evidence_storage(), b.evidence_storage());
    assert_ne!(a.location_digest, b.location_digest);

    let (ea, eb) = (a.job_env(), b.job_env());
    let secrets = |env: &weirkeeper::destination::DestinationEnv| {
        env.from_secret
            .iter()
            .map(|e| e.secret_name.clone())
            .collect::<std::collections::BTreeSet<_>>()
    };
    assert!(
        secrets(&ea).is_disjoint(&secrets(&eb)),
        "neither destination's Job may reference the other's Secret: {:?} vs {:?}",
        secrets(&ea),
        secrets(&eb)
    );
    assert_eq!(ea.literal(AWS_ALLOW_HTTP_ENV), Some("false"));
    assert_eq!(
        eb.literal(AWS_ALLOW_HTTP_ENV),
        Some("true"),
        "dest-b declares InsecureHTTP EXPLICITLY, which is the only thing that may produce this"
    );

    // AND NO CREDENTIAL VALUE IS ANYWHERE IN EITHER RESOLUTION. A resolution is
    // serialised into a frozen plan `ConfigMap`, which has no encryption at
    // rest and a much wider read surface than a Secret.
    //
    // **THE ASSERTION IS STRUCTURAL, NOT A GREP.** The first version of this
    // block searched the encoded snapshot for `AKIA` and `password` — strings
    // no fixture in this file contains in any field, so it could not fail for
    // the reason it named (the independent review's **L3**). What actually
    // holds is that `ResolvedDestinationSnapshot` has NO VALUE-BEARING FIELD:
    // a grant is a Secret NAME plus DATA KEY names, and the kubelet resolves
    // the value in the pod. So the check is on the key set — every key the
    // encoding emits is one of a closed list — which fails the moment somebody
    // adds a field that could carry a value.
    for resolved in [&a, &b] {
        let encoded: Value = serde_json::from_slice(
            &resolved
                .snapshot()
                .canonical_bytes()
                .expect("the snapshot encodes"),
        )
        .expect("the snapshot is JSON");
        let grant = encoded["grant"].as_object().expect("a grant object");
        for key in grant.keys() {
            assert!(
                [
                    "mode",
                    "secret",
                    "accessKeyIdKey",
                    "secretAccessKeyKey",
                    "sessionTokenKey",
                    "serviceAccountName",
                ]
                .contains(&key.as_str()),
                "`{key}` is not one of the reference-only keys a frozen grant may carry; a \
                 snapshot field that could hold a credential VALUE would reach a ConfigMap with \
                 no encryption at rest. Got {grant:?}"
            );
            assert!(
                !key.to_ascii_lowercase().contains("value"),
                "and no key is a value: {key}"
            );
        }
        // The one value-shaped thing a grant DOES carry is the name of a data
        // key, which is public by construction — it is in the Secret's own
        // `data` map, readable by anybody who can read the Secret at all.
        assert_eq!(
            grant.get("mode").and_then(Value::as_str),
            Some("SecretKeys"),
            "both fixtures use the mode whose encoding carries the most keys, so this row \
             exercises the widest shape: {grant:?}"
        );
    }
}

/// **AN ABSENT DESTINATION HOLDS, AND THE HOLD ENDS.**
///
/// D2 §3.6 step 1. The ref and the object are two `kubectl apply`s in one
/// directory and the order is the server's, so a first-pass refusal would fail
/// a correct manifest set on a race the operator cannot influence — and
/// `Backup.spec` is CEL-immutable, so that refusal could never be repaired in
/// place. It ends because a hold that never ends is an object at `Pending` for
/// the life of the cluster with nothing watching it.
///
/// KILLS: `is_hold()` widened to every refusal (a `DestinationRoleNotConfigured`
/// would then requeue forever), and the expiry dropped (the hold would never
/// become terminal).
#[tokio::test]
async fn an_absent_destination_holds_within_its_budget_and_then_fails() {
    let backup = destination_backed_backup("dest-a");
    let created = backup
        .metadata
        .creation_timestamp
        .clone()
        .map(|t| t.0)
        .unwrap_or_else(|| utc(2026, 11, 9, 3, 0));

    assert_eq!(
        destination_hold_budget(&backup),
        DESTINATION_HOLD_MAX_SECONDS,
        "`deadlineSeconds: 3600` is capped at the ten-minute ceiling"
    );
    let mut short = backup.clone();
    short.spec.deadline_seconds = 60;
    assert_eq!(
        destination_hold_budget(&short),
        60,
        "a run allowed sixty seconds does not spend ten minutes waiting to start one"
    );

    let routes = vec![Route {
        method: "GET",
        path_suffix: "/backupdestinations/dest-a",
        status: 404,
        body: serde_json::json!({
            "kind": "Status", "apiVersion": "v1", "status": "Failure",
            "reason": "NotFound", "code": 404,
            "message": "backupdestinations.logweir.dev \"dest-a\" not found"
        })
        .to_string(),
    }];
    let (client, _r, _b) = mock_client_recording_bodies(routes.clone());
    match admit_destination(&backup, &client, NS, created + chrono::Duration::seconds(5))
        .await
        .expect("the read succeeds")
    {
        DestinationAdmission::Holding { reason, message } => {
            assert_eq!(reason, "DestinationNotFound");
            assert!(
                message.contains("dest-a") && message.contains("namespace-local"),
                "the hold names the object and why the ref is not resolved elsewhere: {message}"
            );
        }
        other => panic!("an absent destination holds within its budget; got {other:?}"),
    }

    let (client, _r, _b) = mock_client_recording_bodies(routes);
    let expired = created + chrono::Duration::seconds(DESTINATION_HOLD_MAX_SECONDS + 1);
    assert!(destination_hold_expired(&backup, expired));
    match admit_destination(&backup, &client, NS, expired).await {
        Err(e) => assert!(
            e.to_string().contains("DestinationNotFound"),
            "the terminal refusal carries the SAME reason the hold carried, so an operator reads \
             one fault and not two: {e}"
        ),
        Ok(other) => panic!("the hold ends; got {other:?}"),
    }
}

/// **A DESTINATION THAT DECLARES A CA BUNDLE IS REFUSED FOR AN ENGINE-DRIVEN
/// RUN UNTIL SOMEBODY MEASURES THE ENGINE.**
///
/// D2 §3.5's `[UNVERIFIED — U1: whether the engine honours a custom CA file]`.
/// The claim is that the engine honours `SSL_CERT_FILE`; it is plausible and undemonstrated, and if it is wrong the
/// observable failure is a TLS handshake inside the engine child reported as
/// an opaque operational error with no mention of certificates.
///
/// KILLS: `ENGINE_CUSTOM_CA_VERIFIED = true` flipped without the measurement,
/// and the gate dropped from `admit_destination`.
#[tokio::test]
async fn a_ca_bundle_is_refused_for_an_engine_driven_run() {
    let mut value = dest_a_value();
    value["spec"]["transport"] = serde_json::json!({
        "security": "TLS",
        "caBundle": {"configMapName": "minio-a-ca", "key": "ca.crt"}
    });
    let backup = destination_backed_backup("dest-a");
    let (client, _r, _b) = mock_client_recording_bodies(vec![
        Route {
            method: "GET",
            path_suffix: "/backupdestinations/dest-a",
            status: 200,
            body: value.to_string(),
        },
        Route {
            method: "GET",
            path_suffix: "/configmaps/minio-a-ca",
            status: 200,
            body: serde_json::json!({
                "apiVersion": "v1", "kind": "ConfigMap",
                "metadata": {"name": "minio-a-ca", "namespace": NS},
                "data": {"ca.crt": CA_PEM}
            })
            .to_string(),
        },
    ]);
    let err = admit_destination(&backup, &client, NS, utc(2026, 11, 9, 3, 17))
        .await
        .expect_err("an unmeasured engine CA is refused");
    assert!(
        err.to_string().contains("CaBundleUnsupportedByEngine"),
        "the refusal names the check code so the Backup and the destination agree: {err}"
    );

    // THE POLICY KEY IS THE ADMINISTRATOR'S ESCAPE HATCH, and it is the only
    // one: a namespace operator cannot set it.
    assert!(!engine_custom_ca_allowed(&Policy::defaults()));
    let mut opted_in = Policy::defaults();
    opted_in.engine.allow_unverified_custom_ca = true;
    assert!(engine_custom_ca_allowed(&opted_in));
    assert!(
        engine_custom_ca_refusal(NS, "dest-a").contains("engine.allowUnverifiedCustomCa"),
        "the message says which key an administrator sets"
    );
}

/// **`evidenceRead` ABSENT IS A DEFINED ANSWER AND NOT A SILENCE.**
///
/// D2 §3.9 step 2. A destination with no reader gets `NotAttempted` naming the
/// field to add and the command that verifies the receipt without a
/// controller-held credential — never a green badge, and never `Invalid`,
/// which would be a claim about a document nobody fetched.
///
/// KILLS: falling back to the controller's global handle when the grant is
/// `NotConfigured` — the mutant reads a DIFFERENT bucket with a DIFFERENT
/// principal and reports whatever it finds there.
#[test]
fn an_unconfigured_evidence_read_is_not_configured_and_not_a_wider_grant() {
    // `dest-b` declares `archiveWrite` and nothing else.
    let b = resolved(dest_b_value(), DestinationRole::EvidenceRead);
    assert_eq!(
        b.grant,
        ResolvedGrant::NotConfigured,
        "an absent evidenceRead is a defined answer and NEVER a fall-back to the write grant"
    );

    // `dest-a` asks for the explicit read grant, which is rule R9: a write
    // grant is never reused to read evidence back.
    let a = resolved(dest_a_value(), DestinationRole::EvidenceRead);
    match a.grant {
        ResolvedGrant::SecretKeys { ref secret, .. } => assert_eq!(secret, "lw-a-reader"),
        ref other => panic!("ArchiveReadGrant resolves to the READ secret; got {other:?}"),
    }

    // AND `ControllerIdentity` IS ALLOWLISTED BY THE INSTALLATION AND NOT BY
    // THE NAMESPACE. A destination that asks for it at a location the chart
    // did not list is refused, so an operator cannot point the controller's
    // own principal anywhere they can name.
    let mut value = dest_b_value();
    value["spec"]["access"]["evidenceRead"] = serde_json::json!({"mode": "ControllerIdentity"});
    let refused = resolve(
        &destination(value.clone()),
        DestinationRole::EvidenceRead,
        &Policy::defaults(),
    )
    .expect_err("an unlisted location is refused");
    assert_eq!(refused.code.as_str(), "ControllerIdentityNotAllowlisted");

    let mut policy = Policy::defaults();
    policy.evidence.controller_identity_locations = vec![IdentityLocation {
        bucket: "lw-b".to_string(),
        endpoint: "http://minio-b.storage.svc:9000".to_string(),
        region: String::new(),
    }];
    let allowed = resolve(&destination(value), DestinationRole::EvidenceRead, &policy)
        .expect("the listed location resolves");
    assert_eq!(allowed.grant, ResolvedGrant::ControllerIdentity);
}

/// **THE CA BYTES AND THE DIGEST THAT NAMES THEM TRAVEL TOGETHER, OR NOTHING
/// IS FROZEN.**
///
/// D2 §3.7: the bundle is copied into the same immutable plan `ConfigMap` and
/// its digest is part of the canonical snapshot. A plan whose bytes are not the
/// bytes its own snapshot commits to would let a rotated root be swapped into a
/// run that already exists.
///
/// KILLS: `freeze_with_ca` accepting any bytes, and the fourth `ConfigMap` key
/// being written without the digest bind.
#[test]
fn a_frozen_ca_bundle_is_bound_to_the_digest_in_its_own_snapshot() {
    use weirkeeper::backup_execution::FrozenInputs;
    let backed = destination_backed_backup("dest-a");
    let mut value = dest_a_value();
    value["spec"]["transport"] = serde_json::json!({
        "security": "TLS",
        "caBundle": {"configMapName": "minio-a-ca", "key": "ca.crt"}
    });
    let pem = CA_PEM;
    let with_ca = resolve(
        &destination(value),
        DestinationRole::ArchiveWrite,
        &Policy::defaults(),
    )
    .expect("the fixture resolves")
    .with_ca(&weirkeeper::destination::CaObservation::Present(
        pem.as_bytes().to_vec(),
    ))
    .expect("the bundle is usable");
    let with_ca = BackupDestinations {
        evidence: with_ca.clone(),
        archive: with_ca,
    };

    let frozen = desired_execution_inputs_for_destination(
        &backed,
        &prod_cluster(),
        &weirkeeper::backup_execution::ResolvedSelection::named(&backed.spec),
        Some(&with_ca),
    )
    .expect("the inputs resolve");
    let documents = frozen.documents().expect("the documents render");
    assert_eq!(
        documents.get(ARCHIVE_CA_PLAN_KEY).map(String::as_str),
        Some(pem),
        "the bundle is a FOURTH key beside the plan, in the run's own immutable object"
    );

    // THE BIND, IN BOTH DIRECTIONS.
    let swapped =
        FrozenInputs::freeze_with_ca(frozen.inputs.clone(), Some(OTHER_CA_PEM.to_string()))
            .expect_err("bytes the snapshot does not name are refused");
    assert!(swapped.to_string().contains("digest"), "got {swapped}");
    let missing = FrozenInputs::freeze_with_ca(frozen.inputs.clone(), None)
        .expect_err("a snapshot naming a CA with no bundle beside it is refused");
    assert!(
        missing.to_string().contains(ARCHIVE_CA_PLAN_KEY),
        "got {missing}"
    );
    let unexpected =
        FrozenInputs::freeze_with_ca(desired_for(&backup()).inputs, Some(pem.to_string()))
            .expect_err("a bundle nothing in the document commits to is refused");
    assert!(
        unexpected.to_string().contains("commit"),
        "got {unexpected}"
    );
}

// ===========================================================================
// D2 W10 fix round 1 — the rows the independent review found missing
// ===========================================================================

use weirkeeper::backup_execution::stored_destination;
use weirkeeper::controllers::backup::{
    desired_execution_inputs_frozen, evidence_source_for, EvidenceSource,
};
use weirkeeper::destination::{
    ARCHIVE_CREDENTIALS_ENV as ARCHIVE_CREDS, EVIDENCE_ACCESS_KEY_ID_ENV, EVIDENCE_CREDENTIALS_ENV,
    EVIDENCE_SECRET_ACCESS_KEY_ENV,
};

/// A route table answering `GET …/backupdestinations/dest-x` with `value`.
fn destination_route(name: &'static str, value: Value) -> Vec<Route> {
    vec![Route {
        method: "GET",
        path_suffix: name,
        status: 200,
        body: value.to_string(),
    }]
}

/// `dest-b` with an `evidenceRead` of the given mode.
fn with_evidence_read(mode: Value) -> Value {
    let mut value = dest_b_value();
    value["spec"]["access"]["evidenceRead"] = mode;
    value
}

/// **A DESTINATION-BACKED RUN'S EVIDENCE IS NEVER READ THROUGH THE
/// CONTROLLER'S GLOBAL HANDLE.**
///
/// # The sentence this row exists to make true
///
/// D2 §3.10: "`Context::archive` is used only for objects without destination
/// refs." The controller's one global store takes its bucket, region, endpoint
/// AND credential from the controller's own process (grounding **G2**), so
/// reading a second destination's receipt through it means the wrong bucket or
/// the wrong principal — and the `NotAttempted` that results reads as "no
/// evidence" rather than "wrong bucket". Worse, a document that IS found there
/// is reported as this run's verification.
///
/// # Every arm, because the routing is the whole property
///
/// The independent review measured that making the `SecretKeys` /
/// `WorkloadIdentity` arm return `GlobalHandle` left all 91 rows of this file
/// green. Asserting on the VARIANT and not on the detail text is deliberate:
/// the detail is prose and will be edited, the variant is the routing.
#[tokio::test]
async fn evidence_is_routed_by_grant_and_never_falls_back_to_the_global_handle() {
    let now = utc(2026, 11, 9, 3, 17);
    let global = |source: &EvidenceSource| matches!(source, EvidenceSource::GlobalHandle);

    // ---- no ref: the legacy object, and ONLY the legacy object ----------
    let (client, _r, _b) = mock_client_recording_bodies(Vec::new());
    assert!(
        global(
            &evidence_source_for(None, &client, NS, now)
                .await
                .expect("no ref performs no read")
        ),
        "an object with no destinationRef keeps the handle it has always used"
    );

    // ---- evidenceRead ABSENT: a defined answer -------------------------
    let reference = LocalRef {
        name: "dest-b".to_string(),
    };
    let (client, _r, _b) = mock_client_recording_bodies(destination_route(
        "/backupdestinations/dest-b",
        dest_b_value(),
    ));
    match evidence_source_for(Some(&reference), &client, NS, now)
        .await
        .expect("the read succeeds")
    {
        EvidenceSource::NotAttempted { detail } => assert!(
            detail.contains("evidenceRead"),
            "the detail names the field an operator adds: {detail}"
        ),
        other => panic!("an absent evidenceRead is NotAttempted; got {other:?}"),
    }

    // ---- SecretKeys and WorkloadIdentity: NotAttempted, NEVER the handle
    //
    // THE ROW THE REVIEWER'S SURVIVING MUTANT NEEDED. Both of these grants are
    // read by an evidence-fetch Job in the object's own namespace (D2 §3.9),
    // because the controller holds no verb on `secrets` and must not. Until
    // that Job exists the honest answer is `NotAttempted` naming the missing
    // capability — and the one answer that must never be given is the global
    // handle's, which is a different principal over a different bucket.
    for mode in [
        serde_json::json!({"mode": "SecretKeys", "secret": {
            "name": "lw-b-evidence-reader",
            "accessKeyIdKey": "id", "secretAccessKeyKey": "key"
        }}),
        serde_json::json!({"mode": "WorkloadIdentity",
                           "workloadIdentity": {"serviceAccountName": "lw-b-reader"}}),
    ] {
        let (client, _r, _b) = mock_client_recording_bodies(destination_route(
            "/backupdestinations/dest-b",
            with_evidence_read(mode.clone()),
        ));
        let source = evidence_source_for(Some(&reference), &client, NS, now)
            .await
            .expect("the read succeeds");
        assert!(
            !global(&source),
            "grounding G2: a grant only a pod may hold must NEVER route to the controller's own \
             principal over the controller's own bucket. Mode {mode:?} gave {source:?}"
        );
        match source {
            EvidenceSource::NotAttempted { detail } => assert!(
                detail.contains("evidence-fetch Job"),
                "the detail names the capability that is missing: {detail}"
            ),
            other => panic!("got {other:?}"),
        }
    }

    // ---- ControllerIdentity, UNLISTED: refused, and it says why ---------
    //
    // AND IT SAYS WHICH CLOSED DOOR IT IS. With no policy `ConfigMap`
    // configured on this installation, every key reads its closed default and
    // NOBODY can list a location — a message blaming the administrator's
    // allowlist would send an operator to edit an object that does not exist.
    let (client, _r, _b) = mock_client_recording_bodies(destination_route(
        "/backupdestinations/dest-b",
        with_evidence_read(serde_json::json!({"mode": "ControllerIdentity"})),
    ));
    match evidence_source_for(Some(&reference), &client, NS, now)
        .await
        .expect("the read succeeds")
    {
        EvidenceSource::NotAttempted { detail } => {
            assert!(
                detail.contains("ControllerIdentityNotAllowlisted"),
                "the resolver's own code: {detail}"
            );
            assert!(
                detail.contains("policy ConfigMap"),
                "…and, because this test process names no policy document, WHICH closed door \
                 it is — an operator told only `not allowlisted` goes looking for an object \
                 that does not exist (D2 W11 renders it from the chart's engine.* and \
                 evidence.* values): {detail}"
            );
        }
        other => panic!("an unlisted location is NotAttempted; got {other:?}"),
    }

    // ---- a destination that no longer resolves --------------------------
    //
    // NOT a refusal: this runs AFTER a Job has finished, and a destination
    // deleted or edited in the meantime must not turn an observed run into one.
    let (client, _r, _b) = mock_client_recording_bodies(vec![Route {
        method: "GET",
        path_suffix: "/backupdestinations/dest-b",
        status: 404,
        body: serde_json::json!({
            "kind": "Status", "apiVersion": "v1", "status": "Failure",
            "reason": "NotFound", "code": 404, "message": "gone"
        })
        .to_string(),
    }]);
    let source = evidence_source_for(Some(&reference), &client, NS, now)
        .await
        .expect("a 404 is not an error here");
    assert!(
        matches!(source, EvidenceSource::NotAttempted { .. }),
        "a destination deleted after the run finished is NotAttempted, never a refusal and \
         never the global handle: {source:?}"
    );
}

/// **A DESTINATION EDITED AFTER THE FREEZE CHANGES NOTHING FOR THE RUNNING
/// RUN** — D2 §3.7, and the review's M1/M2.
///
/// # The defect, exactly
///
/// Every edit to a `BackupDestination` bumps `metadata.generation`, the frozen
/// block records the generation it resolved, and `verify_frozen_config_map`
/// compares that block WHOLE. So a pass that re-creates a garbage-collected Job
/// by re-resolving the LIVE object terminates the run with a
/// `PlanConfigMapConflict` — on an archive that may be half written — because
/// somebody rotated a Secret name or added an `evidenceRead` grant while it
/// ran. Fail-closed, and still exactly the thing D2 §3.7 says cannot happen.
///
/// `stored_destination` is the readback that fixes it, mirroring
/// `stored_selection`, which has had one since D1 W5 for the same reason.
///
/// KILLS: dropping the readback (the re-created plan then differs from the
/// stored one); `if false && frozen.inputs.destination != expected.destination`
/// together with `view.destination = None` in `executable()` — the review's
/// surviving mutant R3 — because this row asserts the plan the re-create
/// renders is BYTE-IDENTICAL to the stored one, which no comparison-free
/// controller can produce from an edited object.
#[test]
fn a_destination_edited_after_the_freeze_does_not_change_a_created_run() {
    let backed = destination_backed_backup("dest-a");
    let selection = weirkeeper::backup_execution::ResolvedSelection::named(&backed.spec);

    // The plan this run was admitted with, and the ConfigMap that holds it.
    let frozen = desired_execution_inputs_for_destination(
        &backed,
        &prod_cluster(),
        &selection,
        Some(&both_roles(dest_a_value())),
    )
    .expect("the freeze resolves");
    let plan = weirkeeper::backup_execution::inputs_config_map(&backed, &frozen)
        .expect("the plan renders");

    // THE EDIT. A rotated writer Secret and a new read grant: two things an
    // operator does while a backup runs, and both bump the generation.
    let mut edited = dest_a_value();
    edited["metadata"]["generation"] = serde_json::json!(4);
    edited["status"]["observedGeneration"] = serde_json::json!(4);
    edited["status"]["conditions"][0]["observedGeneration"] = serde_json::json!(4);
    edited["spec"]["access"]["archiveWrite"]["secret"]["name"] =
        serde_json::json!("lw-a-writer-v2");
    let live = both_roles(edited);

    // A re-resolution against the edited object is a DIFFERENT plan — which is
    // what would terminate the run, and is why the readback has to exist.
    let re_resolved =
        desired_execution_inputs_for_destination(&backed, &prod_cluster(), &selection, Some(&live))
            .expect("it resolves");
    assert_ne!(
        re_resolved.sha256, frozen.sha256,
        "the premise: the edited destination resolves to different inputs, so a re-create that \
         re-resolves would refuse the run"
    );

    // THE READBACK PATH. The plan the Job-gone pass renders is byte-identical
    // to the one the run was admitted with.
    let (snapshot, ca_pem) = stored_destination(&plan).expect("the plan carries its destination");
    assert_eq!(
        snapshot.generation, 3,
        "the frozen generation, not the live one"
    );
    let recreated = desired_execution_inputs_frozen(
        &backed,
        &prod_cluster(),
        &selection,
        Some(&snapshot),
        ca_pem,
    )
    .expect("the frozen inputs re-render");
    assert_eq!(
        recreated.canonical, frozen.canonical,
        "a Job re-created after the destination was edited renders from the FROZEN block, so its \
         plan is the plan the run was approved with, byte for byte"
    );
    assert_eq!(recreated.sha256, frozen.sha256);

    // …AND `verify_frozen_config_map` ADMITS IT. This is the comparison the
    // review's mutant R3 disabled with no row noticing.
    weirkeeper::backup_execution::verify_frozen_config_map(&plan, &backed, &recreated, None)
        .expect("the stored plan is admitted against the frozen re-render");
    let refused =
        weirkeeper::backup_execution::verify_frozen_config_map(&plan, &backed, &re_resolved, None)
            .expect_err("and a plan resolved from the EDITED destination is refused");
    assert!(
        refused
            .to_string()
            .contains("froze a different resolved BackupDestination"),
        "the refusal names the block: {refused}"
    );
}

/// **A DESTINATION-BACKED BACKUP JOB CARRIES THE EVIDENCE CREDENTIAL ITS
/// RUNNER REFUSES TO RUN WITHOUT** — the review's C1.
///
/// # What was broken, and how it hid
///
/// A backup writes TWO things: the archive through `archiveWrite`, and its
/// signed receipt through `evidenceWrite` over Global Constraint 6's `logweir/`
/// root. The runner builds two stores and refuses a run whose
/// `LOGWEIR_EVIDENCE_CREDENTIALS` it does not recognise — including the empty
/// string an unset variable produces. `render_job_env` never emitted it, so
/// **every destination-backed run exited 3 at the store builder**, after the
/// archive handles were opened and before a byte was archived. The erratum
/// **E20** failure class, one layer further in.
///
/// It hid because the end-to-end row asserted the run stops at
/// `$LOGWEIR_SOURCE_PASSWORD is unset`, which happens BEFORE any store is
/// built. `schedule_controller::the_destination_backed_job_env_drives_the_real_runner_past_its_store_builders`
/// is the row that closes that hole; this one is its unit half.
///
/// KILLS: dropping `evidence_job_env` from the rendered Job.
#[test]
fn a_destination_backed_backup_job_names_its_evidence_credential() {
    let backed = destination_backed_backup("dest-a");
    let selection = weirkeeper::backup_execution::ResolvedSelection::named(&backed.spec);

    // ---- the common destination: one grant, one credential --------------
    let frozen = desired_execution_inputs_for_destination(
        &backed,
        &prod_cluster(),
        &selection,
        Some(&both_roles(dest_a_value())),
    )
    .expect("the freeze resolves");
    let spec =
        runner_job_spec_from_inputs(&backed, &prod_cluster(), &frozen).expect("the Job renders");
    let env: std::collections::BTreeMap<&str, &str> = spec
        .env_literal
        .iter()
        .map(|(n, v)| (n.as_str(), v.as_str()))
        .collect();
    assert_eq!(
        env.get(EVIDENCE_CREDENTIALS_ENV).copied(),
        Some("archive"),
        "`evidenceWrite` falls back to `archiveWrite` (D2 §3.4), so the receipt store reuses the \
         archive grant and the pod carries ONE credential. Got {env:?}"
    );
    assert_eq!(env.get(ARCHIVE_CREDS).copied(), Some("static"));
    assert!(
        !spec
            .env_from_secret
            .iter()
            .any(|e| e.name.starts_with("LOGWEIR_EVIDENCE_AWS_")),
        "and nothing second is projected: {:?}",
        spec.env_from_secret
    );

    // ---- a SEPARATE evidenceWrite principal: two, unshadowed ------------
    let mut separated = dest_a_value();
    separated["spec"]["access"]["evidenceWrite"] = serde_json::json!({
        "mode": "SecretKeys",
        "secret": {"name": "lw-a-evidence-writer",
                   "accessKeyIdKey": "access-key-id",
                   "secretAccessKeyKey": "secret-access-key"}
    });
    let frozen = desired_execution_inputs_for_destination(
        &backed,
        &prod_cluster(),
        &selection,
        Some(&both_roles(separated)),
    )
    .expect("the freeze resolves");
    let snapshot = frozen
        .inputs
        .destination
        .as_ref()
        .expect("a destination-backed run freezes its destination");
    assert!(
        snapshot.evidence_grant.is_some(),
        "an evidenceWrite that DIFFERS from archiveWrite is frozen, so a Job re-created after a \
         restart still projects it"
    );
    let spec =
        runner_job_spec_from_inputs(&backed, &prod_cluster(), &frozen).expect("the Job renders");
    let env: std::collections::BTreeMap<&str, &str> = spec
        .env_literal
        .iter()
        .map(|(n, v)| (n.as_str(), v.as_str()))
        .collect();
    assert_eq!(
        env.get(EVIDENCE_CREDENTIALS_ENV).copied(),
        Some("static"),
        "two principals, so the receipt store gets its own: {env:?}"
    );
    let projected: std::collections::BTreeMap<&str, &str> = spec
        .env_from_secret
        .iter()
        .map(|e| (e.name.as_str(), e.secret_name.as_str()))
        .collect();
    assert_eq!(
        projected.get("AWS_ACCESS_KEY_ID").copied(),
        Some("lw-a-writer"),
        "the engine writes the archive with `archiveWrite`: {projected:?}"
    );
    assert_eq!(
        projected.get(EVIDENCE_ACCESS_KEY_ID_ENV).copied(),
        Some("lw-a-evidence-writer"),
        "and the receipt with `evidenceWrite`, under a name that cannot shadow it: {projected:?}"
    );
    assert!(projected.contains_key(EVIDENCE_SECRET_ACCESS_KEY_ENV));

    // ---- AND A LEGACY RUN CARRIES NEITHER -------------------------------
    let legacy = runner_job_spec_from_inputs(&backup(), &prod_cluster(), &desired_for(&backup()))
        .expect("the legacy Job renders");
    assert!(
        !legacy
            .env_literal
            .iter()
            .any(|(n, _)| n.starts_with("LOGWEIR_EVIDENCE_")),
        "the legacy runner builds its evidence store the way it always has: {:?}",
        legacy.env_literal
    );
}

/// **A JOB RE-CREATED AFTER THE DESTINATION WAS EDITED READS THE PLAN AND NOT
/// THE OBJECT** — the reconcile half of the review's M1/M2.
///
/// # Why the pure row above is not enough
///
/// `a_destination_edited_after_the_freeze_does_not_change_a_created_run` proves
/// `desired_execution_inputs_frozen` renders the stored block faithfully. It
/// says nothing about whether the RECONCILER calls it — and a mutant replacing
/// `stored_destination` with `None` survived that row while re-resolving the
/// live object on every Job-gone pass, which is the defect itself.
///
/// # The route table is the assertion
///
/// **There is no `backupdestinations` route.** The double panics on a request
/// it was not given a route for, so a pass that reads the destination at all
/// fails here by construction — which is exactly the property D2 §3.7 states:
/// a run that is already frozen does not consult the object again. The edited
/// object is never even served, so the row cannot be satisfied by a
/// coincidentally-equal resolution.
///
/// KILLS: `stored_destination` replaced by `None` in `reconcile_backup_inner`
/// (the reconcile then GETs the destination and the double panics); and
/// `desired_execution_inputs_frozen` swapped back for the resolving arm.
#[tokio::test]
async fn a_frozen_destination_backed_backup_recreates_its_job_without_reading_the_destination() {
    let backed = destination_backed_backup("dest-a");
    let selection = weirkeeper::backup_execution::ResolvedSelection::named(&backed.spec);
    let frozen = desired_execution_inputs_for_destination(
        &backed,
        &prod_cluster(),
        &selection,
        Some(&both_roles(dest_a_value())),
    )
    .expect("the freeze resolves");
    let plan = serde_json::to_value(
        weirkeeper::backup_execution::inputs_config_map(&backed, &frozen)
            .expect("the plan renders"),
    )
    .expect("a ConfigMap serialises");

    // The object as a previous pass left it: inputs frozen and recorded, the
    // Job created and reported running — and then the Job is gone.
    let mut stored = with_status_patch(
        &backed,
        &weirkeeper::controllers::backup::execution_status_patch(&frozen),
    );
    let running = running_status_patch(&stored, NAME, utc(2026, 11, 9, 3, 17));
    stored = with_status_patch(&stored, &running);

    let routes = vec![
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
            method: "GET",
            path_suffix: "/configmaps/logweir-backup-nightly-20261109-031700-plan",
            status: 200,
            body: plan.to_string(),
        },
        // ROUTED SO THAT "NO SECOND PLAN" IS AN ASSERTION AND NOT AN ABSENCE.
        Route {
            method: "POST",
            path_suffix: "/configmaps",
            status: 201,
            body: plan.to_string(),
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
            body: backup_json(),
        },
        // …AND DELIBERATELY NO `/backupdestinations/dest-a`. See the doc
        // comment: reading it at all is the defect, and the double panics.
    ];
    let (client, _seen, bodies) = mock_client_recording_bodies(routes);
    let outcome = reconcile_backup(
        &stored,
        &client,
        &unobserved_archive,
        &unverified_evidence,
        utc(2026, 11, 9, 3, 30),
    )
    .await
    .expect("a frozen destination-backed run re-creates its Job from the plan it holds");
    let bodies = bodies.lock().expect("readable").clone();

    assert!(
        outcome.created,
        "the missing Job is re-created rather than the run being refused: {:?}",
        calls(&bodies)
    );
    assert_eq!(
        outcome.terminal_state, None,
        "and it is NOT a PlanConfigMapConflict, which is what re-resolving an edited \
         destination would produce on an archive that may be half written"
    );
    assert!(
        !bodies
            .iter()
            .any(|b| path(&b.uri).contains("/backupdestinations")),
        "D2 §3.7: a frozen run does not consult its BackupDestination again. Got {:?}",
        calls(&bodies)
    );
    assert!(
        !bodies
            .iter()
            .any(|b| b.method == "POST" && path(&b.uri).ends_with("/configmaps")),
        "no second plan is created: {:?}",
        calls(&bodies)
    );
    assert!(!rewrote_a_config_map(&bodies));
    assert_eq!(
        posted_job(&bodies).expect("the Job was POSTed"),
        serde_json::to_value(
            runner_job(
                &backed,
                &prod_cluster(),
                &frozen,
                &job::RunnerImage::default()
            )
            .expect("the Job renders")
        )
        .expect("a Job serialises"),
        "and the Job is the one the FROZEN inputs describe, evidence credential included"
    );
}

// ===========================================================================
// Defect D2-EVIDENCE-NOTATTEMPTED-UNWRITTEN — the honest verdict is published
// ===========================================================================

/// [`frozen_backup`], re-pointed at a saved destination: the run is already
/// frozen (so the §3.6 admission is behind it) and its Job has finished, which
/// is the only state in which evidence is read at all.
fn frozen_destination_backed_backup(name: &str) -> Backup {
    let mut b = destination_backed_backup(name);
    b.status = Some(BackupStatus {
        execution: Some(BackupExecution {
            id: UID.to_string(),
            inputs_ref: LocalRef {
                name: plan_config_map_name(NAME),
            },
            inputs_sha256: FIXTURE_INPUTS_SHA256.to_string(),
        }),
        ..BackupStatus::default()
    });
    b
}

/// [`finished_routes`] plus the `GET …/backupdestinations/<name>` the evidence
/// routing makes.
fn finished_routes_for_destination(
    pods: &str,
    log: String,
    destination_name: &'static str,
    destination_value: Value,
) -> Vec<Route> {
    let mut routes = finished_routes(pods, log, 200, "Complete");
    routes.push(Route {
        method: "GET",
        path_suffix: destination_name,
        status: 200,
        body: destination_value.to_string(),
    });
    routes
}

/// **A DESTINATION-BACKED RUN PUBLISHES ITS `NotAttempted` VERDICT** — defect
/// `D2-EVIDENCE-NOTATTEMPTED-UNWRITTEN`, measured live on 2026-09-18.
///
/// # The defect
///
/// `evidence_source` answers `NotAttempted` with a sentence naming exactly why
/// there is no reader — for `SecretKeys` and `WorkloadIdentity`, that the grant
/// is one only a pod may hold (D2 §3.9's evidence-fetch Job, which this build
/// does not create) and that the operator can run `logweir drill verify` or
/// move the destination to an allowlisted `ControllerIdentity`. Because
/// nothing was fetched, `receipt_sha256` is `None` BY CONSTRUCTION — and the
/// second patch used to be fenced on that digest, so the whole block was
/// skipped and the operator saw **no** `status.evidence.verification` field at
/// all. The sentence existed and reached nobody; `S1.statusVerification` read
/// `null` on both live `Backup`s.
///
/// # What is asserted, and why it is not the digest
///
/// D2 §3.9 step 2: the mode is chosen once BOTH KEYS are present, and the
/// `NotConfigured` arm writes its verdict with no fetch at all. So the guard is
/// "was a receipt written" — which the runner's two key lines say — and never
/// "was a receipt read", which is the question the verdict answers.
///
/// KILLS: re-fencing the block on `receipt_sha256`; answering `Valid`,
/// `Invalid` or `Untrusted` without bytes; dropping the detail sentence;
/// writing `records`/`capture` off an unverified run.
#[tokio::test]
async fn a_destination_backed_run_with_a_pod_only_grant_publishes_not_attempted() {
    let evidence_read = serde_json::json!({"mode": "SecretKeys", "secret": {
        "name": "lw-b-evidence-reader",
        "accessKeyIdKey": "id", "secretAccessKeyKey": "key"
    }});
    let mut value = dest_b_value();
    value["spec"]["access"]["evidenceRead"] = evidence_read;

    let (client, _seen, bodies) = mock_client_recording_bodies(finished_routes_for_destination(
        &pod_list_terminated(0),
        log_body(&i7_tail()),
        "/backupdestinations/dest-b",
        value,
    ));
    reconcile_backup(
        &frozen_destination_backed_backup("dest-b"),
        &client,
        // THE GLOBAL HANDLE IS NEVER CONSULTED for a destination-backed run
        // (D2 §3.10), and this oracle would answer `None` anyway: the point is
        // that a `None` observation no longer silences the verdict.
        &unobserved_archive,
        &unverified_evidence,
        utc(2026, 11, 9, 3, 20),
    )
    .await
    .expect("the reconcile succeeds");

    let statuses = patched_statuses(&bodies.lock().expect("the body recorder is readable"));
    let second = statuses.last().expect("the verification patch was written");
    let verification = &second["evidence"]["verification"];
    assert!(
        !verification.is_null(),
        "D2-EVIDENCE-NOTATTEMPTED-UNWRITTEN: an ABSENT field is what an operator used to get, \
         and an absent field is indistinguishable from a controller that never looked. Statuses: \
         {statuses:?}"
    );
    assert_eq!(
        verification["result"],
        json!("NotAttempted"),
        "the honest verdict, and the ONLY one writable with no bytes: {verification}"
    );
    let detail = verification["detail"]
        .as_str()
        .expect("the verdict carries its detail");
    assert!(
        detail.contains("evidence-fetch Job"),
        "the sentence names the capability that is missing — that is the whole reason to \
         publish the verdict: {detail}"
    );
    assert!(
        detail.contains("logweir drill verify"),
        "and the command an operator can run instead: {detail}"
    );
    assert!(
        verification["matchedKeyId"].is_null(),
        "nothing was verified, so no key matched: {verification}"
    );
    assert!(
        second["records"].is_null() && second["capture"].is_null(),
        "defect STATUS-RECORDS is NOT widened: those two are copied out of a VERIFIED receipt \
         and this run has none: {second}"
    );
    let (state, reason, _) = condition_named(second, "Verified").expect("the Verified condition");
    assert_eq!(
        (state.as_str(), reason.as_str()),
        ("False", "VerificationNotAttempted"),
        "and the badge that follows from it — `Verified=False` with the reason NAMING the \
         verdict, not `Valid` and not silence: {second}"
    );
    assert_eq!(
        statuses[0]["exitCode"].as_i64(),
        Some(0),
        "the TERMINAL patch still speaks first and still carries the code; the verdict is a \
         second patch and cannot delay it: {}",
        statuses[0]
    );
}

/// The GC11 half of [`a_destination_backed_run_with_a_pod_only_grant_publishes_not_attempted`],
/// and the decision it records: **"no artifact was written" still writes
/// nothing.**
///
/// D2 §3.9 step 2 gates the whole evidence flow on both keys being present,
/// and Global Constraint 11 says exits 1, 3 and 4 wrote no artifact — so the
/// runner prints no `receipt-key=`/`sidecar-key=` lines and there is no
/// document to have an opinion about. A `NotAttempted` block here would be a
/// verdict about a receipt that does not exist, which reads as "we could not
/// check your backup" when the truth is "this run produced nothing to check".
/// The absence is the distinction, and it is kept.
///
/// KILLS: hoisting the `NotAttempted` verdict out of the key guard as well as
/// out of the digest guard.
#[tokio::test]
async fn a_run_that_wrote_no_artifact_still_writes_no_verification_block() {
    let evidence_read = serde_json::json!({"mode": "SecretKeys", "secret": {
        "name": "lw-b-evidence-reader",
        "accessKeyIdKey": "id", "secretAccessKeyKey": "key"
    }});
    let mut value = dest_b_value();
    value["spec"]["access"]["evidenceRead"] = evidence_read;

    let (client, _seen, bodies) = mock_client_recording_bodies(finished_routes_for_destination(
        &pod_list_terminated(1),
        // GC11: an operational failure writes no archive and no receipt, so
        // the log carries neither key line.
        log_body("logweir: the broker refused the connection\n"),
        "/backupdestinations/dest-b",
        value,
    ));
    reconcile_backup(
        &frozen_destination_backed_backup("dest-b"),
        &client,
        &unobserved_archive,
        &unverified_evidence,
        utc(2026, 11, 9, 3, 20),
    )
    .await
    .expect("the reconcile succeeds");

    let statuses = patched_statuses(&bodies.lock().expect("the body recorder is readable"));
    for status in &statuses {
        assert!(
            status["evidence"]["verification"].is_null(),
            "GC11: exit 1 wrote no artifact, so there is no document to have an opinion about \
             and no verdict is published: {status}"
        );
    }
    assert_eq!(
        statuses[0]["exitCode"].as_i64(),
        Some(1),
        "the exit code is still recorded: {}",
        statuses[0]
    );
}
