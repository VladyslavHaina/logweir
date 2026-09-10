//! The `Restore` reconciler: the admission that runs before any Job exists,
//! the plan hash recomputed from the spec bytes, and `mode: scratch` as a
//! field.
//!
//! EVERY TEST HERE IS A PURE-FUNCTION TEST, A `mock_client` TEST OR A
//! FIXTURE-PARSING TEST. Nothing dials a socket, nothing waits on a Job,
//! nothing runs `kubectl`, and nothing approaches Global Constraint 22's 15 s
//! per-test bound — the transport is a `tower` closure and the clock is an
//! argument.
//!
//! READ `no_job_exists_until_the_approval_is_verified` FIRST. It is the
//! property the whole module exists for, and it is a ZERO COUNT over a route
//! table that HAS the `POST` routes present: the double panics on a request it
//! was not given a route for, so a table with the `POST`s missing would make
//! "no Job was created" indistinguishable from "the test forgot a route".
//! Global Constraint 6's operator half — an unapproved plan creates NOTHING —
//! is only assertable that way round.

use chrono::{DateTime, TimeZone, Utc};
use futures::future::BoxFuture;
use logweir_core::ids::sha256_prefixed;
use logweir_store::Store;
use serde_json::Value;
use weirkeeper::conditions::{
    CONDITION_REASONS, CONDITION_REASON_GUARD_REFUSED, CONDITION_REASON_OK, REASON_ADMITTED,
    REASON_APPROVAL_NOT_VERIFIED, REASON_DRILL_NOT_PASS, REASON_GUARD_REFUSED, REASON_OK,
    REASON_OPERATIONAL, REASON_SIGNING_OR_LOCK, TERMINAL_STATES,
    TERMINAL_STATE_APPROVAL_NOT_RECEIVED, TERMINAL_STATE_CLUSTER_NOT_REACHABLE,
    TERMINAL_STATE_CREDENTIAL_NOT_RENDERABLE, TERMINAL_STATE_GUARD_REFUSED_UNKNOWN_REASON,
    TERMINAL_STATE_NAME_TOO_LONG, TERMINAL_STATE_PLAN_CONFIG_MAP_CONFLICT,
    TERMINAL_STATE_PLAN_HASH_MISMATCH, TERMINAL_STATE_TARGET_TOPIC_CONFIG_REFUSED,
    TERMINAL_STATE_WINDOW_NOT_COVERED,
};
use weirkeeper::controllers::backup::SIGNING_VOLUME;
use weirkeeper::controllers::backup::{JOB_NAME_LABEL, JOB_NAME_LABEL_LEGACY};
use weirkeeper::controllers::restore::{
    action_for, admit, approval_plan_hash, approver_key_ids, observe_scorecard,
    plan_config_map_name, recomputed_plan_hash, reconcile_restore, restore_evidence_keys,
    runner_argv, runner_job_spec, scorecard_observation, topic_mapping, triggered_by,
    unobserved_scorecard, window_not_covered, Requeue, RestoreAdmission, RestoreEvidenceKeys,
    ScorecardObservation, ADMISSION_REQUEUE_SECS, ALLOWED_CLUSTERS_FILE, APPROVAL_BUNDLE_SECRET,
    APPROVAL_DOC_FILE, APPROVAL_SIG_FILE, APPROVER_KEY_FILE, OFFSET_REPORT_KEY_PREFIX,
    OFFSET_REPORT_OUT_PATH, OUTCOME_FAIL_COVERAGE, PLAN_SPEC_KEY, REFERENT_NOT_FOUND_REASON,
    SCORECARD_KEY_PREFIX, SCORECARD_OUT_PATH, SIDECAR_KEY_PREFIX, TARGET_PASSWORD_ENV,
    TARGET_PASSWORD_SECRET_KEY,
};
use weirkeeper::crds::restore::Restore;
use weirkeeper::job::{self, APPROVAL_MOUNT_PATH, APPROVAL_VOLUME};
use weirkeeper::testing::{mock_client_recording_bodies, Route, SeenBody};

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

/// The task's namespace (STANDING RULE 13).
const NS: &str = "logweir-t20";

/// The `Restore`'s name. The Job is named after this object VERBATIM, so it is
/// deliberately inside the 63-character `batch.kubernetes.io/job-name` cap —
/// see `a_restore_whose_name_is_too_long_is_refused_before_any_post` for the
/// other side.
const NAME: &str = "logweir-restore-incident-4471";
/// The pod the job controller made.
const POD: &str = "logweir-restore-incident-4471-abcde";

const UID: &str = "5c2e7b91-0000-4000-8000-0000000000a2";
const CLUSTER_UID: &str = "8b3c1d2e-0000-4000-8000-0000000000c2";
const APPROVAL: &str = "a1";

/// The three keys interface **I8** prints, in the contract's order.
const SCORECARD_KEY: &str = "logweir/drills/01JB7Z0000000000000000000A.json";
const SIDECAR_KEY: &str = "logweir/drills/01JB7Z0000000000000000000A.json.sig";
const OFFSET_REPORT_KEY: &str = "logweir/drills/01JB7Z0000000000000000000A.offsets.json";

/// The two roster approver key ids, one of them expired.
const KEY_ID_LIVE: &str = "sha256:1111111111111111111111111111111111111111111111111111111111111111";
const KEY_ID_EXPIRED: &str =
    "sha256:2222222222222222222222222222222222222222222222222222222222222222";

/// `spec.planBytes` — **the runner's own `restore.yaml` grammar** (interface
/// **I20**), and the fixture every ConfigMap assertion is made over.
///
/// # IT ENDS WITH TWO SPACES AND A NEWLINE, DELIBERATELY
///
/// `the_plan_configmap_carries_the_spec_bytes_verbatim` is about trailing
/// whitespace and nothing else: `plan_hash` binds these exact bytes, so a
/// renderer that round-tripped them through `serde_yaml::from_str` +
/// `to_string` — which is the mutant — produces a document whose hash no
/// approval matches, while every gate reports green. Trailing whitespace is
/// the cheapest thing a round trip destroys and the hardest to notice by eye,
/// which is exactly why it is the fixture.
///
/// # IT IS A DOCUMENT THE RUNNER PARSES
///
/// `the_plan_bytes_fixture_is_a_document_the_runner_parses` feeds it to
/// `serde_yaml::from_str::<logweir_core::spec::RestoreSpec>` — the same type
/// `logweir restore run --spec` deserialises with. An invented shape would
/// pass the 201, pass `drill approve` (which hashes whatever bytes it reads),
/// pass all of `admit` because the hash matches, and fail only inside the
/// runner pod after every gate had reported green.
const PLAN_BYTES: &str = "\
source:
  storage:
    backend: s3
    bucket: kafka-backups
    prefix: drill-demo
    region: us-east-1
    endpoint: http://minio.logweir-t20:9000
    path_style: true
    allow_http: true
  backup: latestCompleted
  topics: [orders, payments]
target:
  bootstrap_servers: [scratch-0.logweir-t20:9092]
  mode: scratch
  topic_mapping_prefix: \"drill-\"
  marker_topic: logweir.scratch
  default_replication_factor: 1
  teardown: delete
restore:
  point_in_time: \"2026-09-07T14:05:00Z\"
sample:
  window_start: \"2026-09-07T12:00:00Z\"
  window_end: \"2026-09-07T15:00:00Z\"
  records_per_partition: 25
  anchor: head
objectives:
  rto_seconds: 900
  rpo_seconds: 300
  pass_rate: 1.0
evidence:
  backend: s3
  bucket: logweir-evidence
  prefix: logweir/
  region: us-east-1
  endpoint: http://minio.logweir-t20:9000
  path_style: true
  allow_http: true  
";

/// The same plan with `mode: newTopic` and a `topic_naming` block — the second
/// half of `scratch_mode_and_new_topic_mode_produce_the_same_job_shape`.
fn new_topic_plan_bytes() -> String {
    PLAN_BYTES.replace(
        "  mode: scratch\n",
        "  mode: newTopic\n  topic_naming:\n    prefix: \"incident-4471-\"\n",
    )
}

/// `sha256_prefixed(PLAN_BYTES)` — computed, never written out as a literal.
///
/// A HARD-CODED HASH WOULD MAKE THE MUTANT SURVIVE. If the expected value were
/// a literal and the reconciler's recomputation were replaced by a read of
/// `Approval.status.planHash`, a fixture whose status happened to carry the
/// same literal would still pass. Deriving it here from the same bytes the
/// ConfigMap carries is what makes the comparison about the FUNCTION.
fn plan_hash() -> String {
    sha256_prefixed(PLAN_BYTES.as_bytes())
}

/// The approval DOCUMENT — the UTF-8 text `spec.approvalBytes` carries,
/// verbatim, **never base64** (interface **I18**).
fn approval_doc(plan_hash: &str) -> String {
    format!(
        r#"{{"approver":"sre-oncall@example.com","ticket":"CHG-40881",
  "plan_hash":"{plan_hash}","subject_kind":"Restore"}}"#
    )
}

/// A `Restore` as the API server would hand it over.
///
/// `plan_bytes` and `mode` are parameters so one template serves both modes
/// and the mismatch arms; every other field is fixed.
fn restore_json(plan_bytes: &str, approval_ref: &str, name: &str) -> String {
    let plan = serde_json::to_string(plan_bytes).expect("planBytes is a JSON string");
    format!(
        r#"{{
  "apiVersion": "logweir.dev/v1alpha1",
  "kind": "Restore",
  "metadata": {{ "name": "{name}", "namespace": "{NS}", "uid": "{UID}", "generation": 2 }},
  "spec": {{
    "planBytes": {plan},
    "approvalRef": {{ "name": "{approval_ref}" }},
    "sourceArchive": {{ "url": "s3://kafka-backups/logweir", "secretRef": {{ "name": "logweir-s3" }} }},
    "backupSetRef": "drill-demo",
    "pointInTime": "2026-09-07T14:05:00Z",
    "target": {{
      "clusterRef": {{ "name": "scratch" }},
      "mode": "scratch",
      "topicNaming": {{ "prefix": "drill-" }}
    }},
    "deadlineSeconds": 1800
  }}
}}"#
    )
}

fn restore() -> Restore {
    serde_json::from_str(&restore_json(PLAN_BYTES, APPROVAL, NAME))
        .expect("the fixture is a Restore")
}

/// The same object with `spec.approvalRef.name` empty — the ref that names
/// nothing.
fn restore_with_no_approval_ref() -> Restore {
    serde_json::from_str(&restore_json(PLAN_BYTES, "", NAME)).expect("the fixture is a Restore")
}

/// The `newTopic` twin.
fn restore_new_topic() -> Restore {
    let plan = new_topic_plan_bytes();
    let mut value: Value =
        serde_json::from_str(&restore_json(&plan, APPROVAL, NAME)).expect("the fixture is JSON");
    value["spec"]["target"]["mode"] = serde_json::json!("newTopic");
    value["spec"]["target"]["topicNaming"]["prefix"] = serde_json::json!("incident-4471-");
    serde_json::from_value(value).expect("the mutated fixture is a Restore")
}

/// A UTC instant, spelled as five integers so a test reads like a calendar.
fn utc(y: i32, m: u32, d: u32, h: u32, min: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(y, m, d, h, min, 0)
        .single()
        .expect("the fixture instant exists")
}

fn now() -> DateTime<Utc> {
    utc(2026, 9, 10, 12, 0)
}

/// An `Approval` as the API server would hand it over.
///
/// `doc_hash` is the hash INSIDE the signed bytes; `status_hash` is what the
/// STATUS claims. They are separate parameters because the whole property of
/// `the_plan_hash_is_recomputed_from_the_spec_bytes_at_job_creation`'s second
/// arm is that only the first one is read.
fn approval_json(verified: bool, doc_hash: &str, status_hash: &str) -> String {
    let doc = serde_json::to_string(&approval_doc(doc_hash)).expect("the doc is a JSON string");
    format!(
        r#"{{
  "apiVersion": "logweir.dev/v1alpha1",
  "kind": "Approval",
  "metadata": {{ "name": "{APPROVAL}", "namespace": "{NS}", "uid": "aaaaaaaa-0000-4000-8000-00000000000a" }},
  "spec": {{
    "subjectRef": {{ "kind": "Restore", "name": "{NAME}" }},
    "planHash": "{status_hash}",
    "approvalBytes": {doc},
    "sidecarBytes": "{{}}"
  }},
  "status": {{
    "verified": {verified},
    "matchedKeyId": "{KEY_ID_LIVE}",
    "planHash": "{status_hash}",
    "conditions": [{{ "type": "Verified", "status": "{}", "reason": "{}" }}]
  }}
}}"#,
        if verified { "True" } else { "False" },
        if verified {
            "Verified"
        } else {
            "SignatureInvalid"
        }
    )
}

fn approval(verified: bool) -> weirkeeper::crds::approval::Approval {
    serde_json::from_str(&approval_json(verified, &plan_hash(), &plan_hash()))
        .expect("the fixture is an Approval")
}

/// The target `KafkaCluster`, as the API server would hand it over.
fn cluster_json(reachable: bool, auth: &str) -> String {
    format!(
        r#"{{
  "apiVersion": "logweir.dev/v1alpha1",
  "kind": "KafkaCluster",
  "metadata": {{ "name": "scratch", "namespace": "{NS}", "uid": "{CLUSTER_UID}" }},
  "spec": {{
    "bootstrapServers": ["scratch-0.logweir-t20:9092"],
    "auth": {auth},
    "role": "scratch",
    "markerTopic": "logweir.scratch"
  }},
  "status": {{ "reachable": {reachable}, "clusterId": "MkU3OEVBNTcwNTJENDM2Qk" }}
}}"#
    )
}

const PLAINTEXT_AUTH: &str = r#"{ "mode": "plaintext", "tls": false }"#;
const SCRAM_AUTH: &str = r#"{ "mode": "scramSha512", "username": "logweir",
  "secretRef": { "name": "scratch-sasl" }, "tls": true }"#;

fn cluster(reachable: bool) -> weirkeeper::crds::kafka_cluster::KafkaCluster {
    serde_json::from_str(&cluster_json(reachable, PLAINTEXT_AUTH))
        .expect("the fixture is a KafkaCluster")
}

/// A `TrustRoster` whose `approverKeys` hold two ids, one of them reported
/// expired by its own status.
fn roster_json() -> String {
    format!(
        r#"{{
  "apiVersion": "logweir.dev/v1alpha1",
  "kind": "TrustRoster",
  "metadata": {{ "name": "default", "uid": "dddddddd-0000-4000-8000-00000000000d" }},
  "spec": {{
    "approverKeys": [
      {{ "keyId": "{KEY_ID_LIVE}", "spkiPem": "-----BEGIN PUBLIC KEY-----\nA\n-----END PUBLIC KEY-----\n" }},
      {{ "keyId": "{KEY_ID_EXPIRED}", "spkiPem": "-----BEGIN PUBLIC KEY-----\nB\n-----END PUBLIC KEY-----\n" }}
    ],
    "signingKeys": [],
    "allowedClusterIds": ["MkU3OEVBNTcwNTJENDM2Qk"]
  }},
  "status": {{ "loaded": true, "expiredKeyIds": ["{KEY_ID_EXPIRED}"] }}
}}"#
    )
}

fn roster() -> weirkeeper::crds::trust_roster::TrustRoster {
    serde_json::from_str(&roster_json()).expect("the fixture is a TrustRoster")
}

/// A 404 `Status`, the shape `Api::get_opt` reads as "absent".
fn not_found_body(kind: &str, name: &str) -> String {
    format!(
        r#"{{"kind":"Status","apiVersion":"v1","status":"Failure",
  "message":"{kind} \"{name}\" not found","reason":"NotFound","code":404}}"#
    )
}

/// A Job that exists and has not finished.
fn running_job_body() -> String {
    format!(
        r#"{{"apiVersion":"batch/v1","kind":"Job",
  "metadata":{{"name":"{NAME}","namespace":"{NS}","uid":"bbbbbbbb-0000-4000-8000-0000000000b2"}},
  "spec":{{"template":{{"spec":{{"containers":[],"restartPolicy":"Never"}}}}}},
  "status":{{"active":1}}}}"#
    )
}

/// A finished Job, `Complete` or `Failed`.
fn job_body(condition: &str) -> String {
    format!(
        r#"{{"apiVersion":"batch/v1","kind":"Job",
  "metadata":{{"name":"{NAME}","namespace":"{NS}","uid":"bbbbbbbb-0000-4000-8000-0000000000b2"}},
  "spec":{{"template":{{"spec":{{"containers":[],"restartPolicy":"Never"}}}}}},
  "status":{{"conditions":[{{"type":"{condition}","status":"True",
     "lastProbeTime":"2026-09-10T11:59:00Z","lastTransitionTime":"2026-09-10T11:59:00Z"}}]}}}}"#
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
      "labels":{{"{JOB_NAME_LABEL}":"{NAME}","{JOB_NAME_LABEL_LEGACY}":"{NAME}"}}}},
    "spec":{{"containers":[]}},
    "status":{{"phase":"Failed","containerStatuses":[
      {{"name":"log-shipper","ready":false,"restartCount":0,"image":"x","imageID":"x",
        "state":{{"terminated":{{"exitCode":0,"finishedAt":"2026-09-10T11:59:00Z"}}}}}},
      {{"name":"runner","ready":false,"restartCount":0,"image":"x","imageID":"x",
        "state":{{"terminated":{{"exitCode":{exit_code},"finishedAt":"2026-09-10T11:59:00Z"}}}}}}
    ]}}}}]}}"#
    )
}

/// The pod log body: two ordinary lines, then whatever `tail` says.
fn log_body(tail: &str) -> String {
    format!(
        "{{\"level\":\"INFO\",\"fields\":{{\"run_id\":\"r1\"}}}}\n\
         {{\"level\":\"INFO\",\"message\":\"restore finished\"}}\n{tail}"
    )
}

/// Interface **I8**'s three lines, in the contract's order.
fn i8_tail() -> String {
    format!(
        "{SCORECARD_KEY_PREFIX}{SCORECARD_KEY}\n\
         {SIDECAR_KEY_PREFIX}{SIDECAR_KEY}\n\
         {OFFSET_REPORT_KEY_PREFIX}{OFFSET_REPORT_KEY}\n"
    )
}

/// A scorecard document, as the runner signed it.
fn scorecard_json(outcome: &str, integrity_result: &str, partial_reason: &str) -> String {
    let reason = if partial_reason.is_empty() {
        "null".to_string()
    } else {
        format!("\"{partial_reason}\"")
    };
    format!(
        r#"{{
  "format_version": "1.0.0",
  "run_id": "01JB7Z0000000000000000000A",
  "outcome": "{outcome}",
  "last_phase_completed": 7,
  "objectives": {{ "rto_seconds": 900, "rpo_seconds": 300, "pass_rate": 1.0, "met": true }},
  "integrity": {{ "level": "byte-fingerprint", "result": "{integrity_result}",
                  "partial_reason": {reason}, "records_sampled": 75,
                  "records_sampled_matching": 75, "mismatches": 0 }},
  "measured": {{ "rto_seconds": 512, "rpo_seconds": 0 }},
  "evidence": {{ "immutable": false, "create_only_enforced": false,
                 "offset_report_key": "{OFFSET_REPORT_KEY}",
                 "offset_report_sha256": "sha256:abc" }}
}}"#
    )
}

// ---------------------------------------------------------------------------
// Route tables
// ---------------------------------------------------------------------------

/// The routes an ADMISSION pass needs, with **every write route present** so
/// that a zero `POST` count is an assertion about the reconciler and not about
/// the table.
///
/// `approval_status` of 404 is the dangling-reference case;
/// `configmap_status` and `job_status` are 201 for the happy path.
fn admission_routes(
    approval_status: u16,
    approval_body: String,
    cluster_status: u16,
    cluster_body: String,
) -> Vec<Route> {
    vec![
        Route {
            method: "GET",
            path_suffix: "/jobs/logweir-restore-incident-4471",
            status: 404,
            body: not_found_body("jobs.batch", NAME),
        },
        Route {
            method: "GET",
            path_suffix: "/approvals/a1",
            status: approval_status,
            body: approval_body,
        },
        Route {
            method: "GET",
            path_suffix: "/kafkaclusters/scratch",
            status: cluster_status,
            body: cluster_body,
        },
        Route {
            method: "GET",
            path_suffix: "/trustrosters/default",
            status: 200,
            body: roster_json(),
        },
        // PRESENT ON PURPOSE — see this function's own note and the module
        // header. A table without these three cannot tell "created nothing"
        // from "the test forgot a route", because the double panics on an
        // unrouted request.
        Route {
            method: "POST",
            path_suffix: "/configmaps",
            status: 201,
            body: existing_plan_config_map(UID),
        },
        Route {
            method: "GET",
            path_suffix: "/configmaps/logweir-restore-incident-4471-plan",
            status: 200,
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
            path_suffix: "/restores/logweir-restore-incident-4471/status",
            status: 200,
            body: restore_json(PLAN_BYTES, APPROVAL, NAME),
        },
    ]
}

/// A plan ConfigMap as the API server would hand it back, owned by `owner_uid`
/// with `controller: true`.
fn existing_plan_config_map(owner_uid: &str) -> String {
    let plan = serde_json::to_string(PLAN_BYTES).expect("planBytes is a JSON string");
    format!(
        r#"{{
  "apiVersion": "v1", "kind": "ConfigMap",
  "metadata": {{
    "name": "{NAME}-plan", "namespace": "{NS}",
    "ownerReferences": [{{
      "apiVersion": "logweir.dev/v1alpha1", "kind": "Restore", "name": "{NAME}",
      "uid": "{owner_uid}", "controller": true, "blockOwnerDeletion": true
    }}]
  }},
  "data": {{ "{PLAN_SPEC_KEY}": {plan} }}
}}"#
    )
}

/// The route table for a reconcile that finds a FINISHED Job.
fn finished_routes(pods: String, log: String, job_condition: &str) -> Vec<Route> {
    vec![
        Route {
            method: "GET",
            path_suffix: "/jobs/logweir-restore-incident-4471",
            status: 200,
            body: job_body(job_condition),
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
            method: "PATCH",
            path_suffix: "/restores/logweir-restore-incident-4471/status",
            status: 200,
            body: restore_json(PLAN_BYTES, APPROVAL, NAME),
        },
        Route {
            method: "PATCH",
            path_suffix: "/jobs/logweir-restore-incident-4471",
            status: 200,
            body: job_body(job_condition),
        },
        // DELETE IS ROUTED SO THAT "IT DID NOT DELETE" IS AN ASSERTION AND NOT
        // AN INABILITY. The double panics on an unrouted request, so with no
        // DELETE route a reconciler that deleted something would fail inside
        // `testing.rs` naming a missing route — a real failure, but not the
        // one Global Constraint 6 is about.
        Route {
            method: "DELETE",
            path_suffix: "/jobs/logweir-restore-incident-4471",
            status: 200,
            body: job_body(job_condition),
        },
        Route {
            method: "DELETE",
            path_suffix: "/restores/logweir-restore-incident-4471",
            status: 200,
            body: restore_json(PLAN_BYTES, APPROVAL, NAME),
        },
    ]
}

// ---------------------------------------------------------------------------
// Recording helpers
// ---------------------------------------------------------------------------

/// A recorded URI's PATH, without its query string.
///
/// `kube` appends a `?` to every request target it builds, empty query
/// included, so an `ends_with("/status")` over the raw URI is false for every
/// request the client actually makes.
fn path(uri: &str) -> &str {
    uri.split('?').next().unwrap_or(uri)
}

/// How many `POST`s reached `suffix`.
fn post_count(bodies: &[SeenBody], suffix: &str) -> usize {
    bodies
        .iter()
        .filter(|b| b.method == "POST" && path(&b.uri).ends_with(suffix))
        .count()
}

/// Every `PATCH …/status` body the double saw, as its `status` object.
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

/// The body of the one `POST …/configmaps`, as JSON.
fn posted_config_map(bodies: &[SeenBody]) -> Value {
    let body = bodies
        .iter()
        .find(|b| b.method == "POST" && path(&b.uri).ends_with("/configmaps"))
        .expect("the reconciler POSTed a ConfigMap");
    serde_json::from_str(&body.body).expect("a ConfigMap POST body is JSON")
}

/// The body of the one `POST …/jobs`, as JSON.
fn posted_job(bodies: &[SeenBody]) -> Value {
    let body = bodies
        .iter()
        .find(|b| b.method == "POST" && path(&b.uri).ends_with("/jobs"))
        .expect("the reconciler POSTed a Job");
    serde_json::from_str(&body.body).expect("a Job POST body is JSON")
}

/// This file's own source text, for the two source-reading assertions.
fn this_module_source() -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("controllers")
        .join("restore.rs");
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
fn workspace_yaml(relative: &str) -> serde_yaml::Value {
    let path = workspace_root().join(relative);
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    serde_yaml::from_str(&text).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

// ===========================================================================
// ADMISSION — no Job exists until the approval is verified
// ===========================================================================

/// **The property this module exists for.** With
/// `Approval.status.verified = Some(false)`: **zero** `POST …/jobs`, the
/// patched `status.phase` is `Pending`, and the condition reason is
/// `ApprovalNotVerified`.
///
/// KILLS: create the Job when `status.verified` is `Some(false)`.
///
/// THE `POST` ROUTES ARE PRESENT. See [`admission_routes`]: the double panics
/// on an unrouted request, so a table without them would make the zero count
/// unfalsifiable.
#[tokio::test]
async fn no_job_exists_until_the_approval_is_verified() {
    let (client, _rec, bodies) = mock_client_recording_bodies(admission_routes(
        200,
        approval_json(false, &plan_hash(), &plan_hash()),
        200,
        cluster_json(true, PLAINTEXT_AUTH),
    ));
    let outcome = reconcile_restore(&restore(), &client, &unobserved_scorecard, now())
        .await
        .expect("the reconcile completes");

    let seen = bodies
        .lock()
        .expect("the body recorder is readable")
        .clone();
    assert_eq!(
        post_count(&seen, "/jobs"),
        0,
        "an unapproved plan creates NOTHING (Global Constraint 6): zero POSTs to /jobs. Saw: {:?}",
        seen.iter()
            .filter(|b| b.method == "POST")
            .map(|b| path(&b.uri).to_string())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        post_count(&seen, "/configmaps"),
        0,
        "and zero POSTs to /configmaps — the plan bytes do not reach the cluster either"
    );
    assert!(!outcome.created, "and the outcome says so");

    let statuses = patched_statuses(&seen);
    assert_eq!(statuses.len(), 1, "exactly one status patch");
    assert_eq!(
        statuses[0]["phase"].as_str(),
        Some("Pending"),
        "phase is Pending — a hold, not a failure: {}",
        statuses[0]
    );
    assert_eq!(
        conditions_of(&statuses[0]),
        vec![(
            "Admitted".to_string(),
            "False".to_string(),
            REASON_APPROVAL_NOT_VERIFIED.to_string()
        )],
        "exactly one condition, its own type, reason ApprovalNotVerified"
    );
    assert!(
        statuses[0].get("exitCode").is_none() && statuses[0].get("exitReason").is_none(),
        "no run was attempted, so no exit code and no exit reason are invented: {}",
        statuses[0]
    );
}

/// **Interface I19.** With `spec.approvalRef.name` naming an `Approval` the
/// mock answers **404**, the returned `Action` is a requeue of 30 s and the
/// condition reason is `ApprovalNotVerified`; a second reconcile with the
/// `Approval` present and `verified: true` posts the Job.
///
/// THIS IS WHAT MAKES THE WIZARD'S MINTED-BOTH-NAMES-FIRST ORDERING WORKABLE.
///
/// KILLS: treat a 404 on the `Approval` as terminal.
#[tokio::test]
async fn an_approval_that_does_not_exist_yet_requeues_at_thirty_seconds() {
    // ---- pass 1: the Approval is not there yet --------------------------
    let (client, _rec, bodies) = mock_client_recording_bodies(admission_routes(
        404,
        not_found_body("approvals.logweir.dev", APPROVAL),
        200,
        cluster_json(true, PLAINTEXT_AUTH),
    ));
    let outcome = reconcile_restore(&restore(), &client, &unobserved_scorecard, now())
        .await
        .expect("a dangling approvalRef is not an error");

    assert_eq!(
        outcome.requeue,
        Requeue::After(30),
        "interface I19: a dangling approvalRef requeues at THIRTY seconds, so the object is \
         released the moment the Approval is verified"
    );
    assert_eq!(
        ADMISSION_REQUEUE_SECS, 30,
        "and the constant is the interval the interface fixes"
    );
    assert_ne!(
        outcome.requeue,
        Requeue::AwaitChange,
        "a terminal `await_change` here would strand every Restore created before its Approval"
    );
    let seen = bodies
        .lock()
        .expect("the body recorder is readable")
        .clone();
    assert_eq!(post_count(&seen, "/jobs"), 0, "and no Job exists yet");
    let statuses = patched_statuses(&seen);
    assert_eq!(
        conditions_of(&statuses[0])[0].2,
        REASON_APPROVAL_NOT_VERIFIED,
        "a 404 on the Approval is ApprovalNotVerified — the approval has not arrived — and NOT \
         ApprovalNotReceived, which means the Restore asked for none"
    );
    assert_eq!(
        outcome.admission,
        Some(RestoreAdmission::ApprovalNotVerified {
            approval: APPROVAL.to_string()
        }),
        "and the verdict names the Approval it waited for"
    );

    // ---- pass 2: the Approval arrives, verified -------------------------
    let (client2, _rec2, bodies2) = mock_client_recording_bodies(admission_routes(
        200,
        approval_json(true, &plan_hash(), &plan_hash()),
        200,
        cluster_json(true, PLAINTEXT_AUTH),
    ));
    let outcome2 = reconcile_restore(&restore(), &client2, &unobserved_scorecard, now())
        .await
        .expect("the reconcile completes");
    let seen2 = bodies2
        .lock()
        .expect("the body recorder is readable")
        .clone();
    assert!(outcome2.created, "the second pass creates the Job");
    assert_eq!(
        post_count(&seen2, "/jobs"),
        1,
        "exactly one POST …/jobs once the approval is verified"
    );
    assert_eq!(
        outcome2.admission,
        Some(RestoreAdmission::Ok),
        "and the admission passed"
    );
}

/// `spec.approvalRef` names nothing: `ApprovalNotReceived`, and the CR is
/// **not** requeued — asserted on the returned `Action`.
///
/// KILLS: requeue an `ApprovalNotReceived`.
#[tokio::test]
async fn a_missing_approval_ref_is_terminal() {
    // The Approval route is deliberately ABSENT from this table: a reconciler
    // that asked the API server about an empty name would panic inside
    // `testing.rs`, which is a stronger assertion than a zero count.
    let routes = vec![
        Route {
            method: "GET",
            path_suffix: "/jobs/logweir-restore-incident-4471",
            status: 404,
            body: not_found_body("jobs.batch", NAME),
        },
        Route {
            method: "GET",
            path_suffix: "/kafkaclusters/scratch",
            status: 200,
            body: cluster_json(true, PLAINTEXT_AUTH),
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
            path_suffix: "/restores/logweir-restore-incident-4471/status",
            status: 200,
            body: restore_json(PLAN_BYTES, APPROVAL, NAME),
        },
    ];
    let (client, _rec, bodies) = mock_client_recording_bodies(routes);
    let outcome = reconcile_restore(
        &restore_with_no_approval_ref(),
        &client,
        &unobserved_scorecard,
        now(),
    )
    .await
    .expect("a terminal refusal is an outcome, never an error");

    assert_eq!(
        outcome.requeue,
        Requeue::AwaitChange,
        "TERMINAL: a Restore that names no approval names none forever — spec is sealed by a CEL \
         rule — so a requeue would spin over an immutable spec"
    );
    assert_eq!(
        format!("{:?}", action_for(&outcome)),
        format!("{:?}", kube::runtime::controller::Action::await_change()),
        "and the Action the runtime gets is `await_change`, not a requeue"
    );
    assert_eq!(
        outcome.terminal_state.as_deref(),
        Some(TERMINAL_STATE_APPROVAL_NOT_RECEIVED)
    );

    let seen = bodies
        .lock()
        .expect("the body recorder is readable")
        .clone();
    assert_eq!(post_count(&seen, "/jobs"), 0);
    assert_eq!(post_count(&seen, "/configmaps"), 0);
    let statuses = patched_statuses(&seen);
    assert_eq!(
        statuses[0]["phase"].as_str(),
        Some("Failed"),
        "a terminal refusal is Failed, not Pending: {}",
        statuses[0]
    );
    assert_eq!(
        conditions_of(&statuses[0]),
        vec![(
            "Failed".to_string(),
            "True".to_string(),
            TERMINAL_STATE_APPROVAL_NOT_RECEIVED.to_string()
        )],
        "exactly ONE Failed condition (errata E5c)"
    );
    assert!(
        statuses[0].get("exitCode").is_none(),
        "nothing ran, so no exit code is invented: {}",
        statuses[0]
    );
}

/// An `Approval` whose `status` is `verified: true` and whose **bytes** carry a
/// `plan_hash` that does not match `spec.planBytes`: zero `POST …/jobs`,
/// terminal `PlanHashMismatch` naming both hashes. A second arm sets the
/// *status* to carry the correct hash and asserts it changes nothing.
///
/// KILLS: read `plan_hash` from `Approval.status` instead of from its bytes;
/// base64-decode `approvalBytes` before parsing.
#[tokio::test]
async fn the_plan_hash_is_recomputed_from_the_spec_bytes_at_job_creation() {
    let wrong = "sha256:0000000000000000000000000000000000000000000000000000000000000000";

    // ---- arm 1: the DOCUMENT's hash is wrong ---------------------------
    let (client, _rec, bodies) = mock_client_recording_bodies(admission_routes(
        200,
        approval_json(true, wrong, wrong),
        200,
        cluster_json(true, PLAINTEXT_AUTH),
    ));
    let outcome = reconcile_restore(&restore(), &client, &unobserved_scorecard, now())
        .await
        .expect("a terminal refusal is an outcome");
    let seen = bodies
        .lock()
        .expect("the body recorder is readable")
        .clone();
    assert_eq!(
        post_count(&seen, "/jobs"),
        0,
        "no Job for a plan nobody approved"
    );
    assert_eq!(post_count(&seen, "/configmaps"), 0);
    assert_eq!(
        outcome.terminal_state.as_deref(),
        Some(TERMINAL_STATE_PLAN_HASH_MISMATCH)
    );
    assert_eq!(
        outcome.requeue,
        Requeue::AwaitChange,
        "terminal, not a requeue"
    );
    let statuses = patched_statuses(&seen);
    let message = statuses[0]["conditions"][0]["message"]
        .as_str()
        .expect("the condition carries a message")
        .to_string();
    assert!(
        message.contains(&plan_hash()) && message.contains(wrong),
        "the refusal names BOTH hashes so an operator can see which bytes were approved. Got: \
         {message}"
    );

    // ---- arm 2: the STATUS carries the CORRECT hash. It changes nothing.
    let (client2, _rec2, bodies2) = mock_client_recording_bodies(admission_routes(
        200,
        // doc says the wrong thing; status says the right thing
        approval_json(true, wrong, &plan_hash()),
        200,
        cluster_json(true, PLAINTEXT_AUTH),
    ));
    let outcome2 = reconcile_restore(&restore(), &client2, &unobserved_scorecard, now())
        .await
        .expect("a terminal refusal is an outcome");
    let seen2 = bodies2
        .lock()
        .expect("the body recorder is readable")
        .clone();
    assert_eq!(
        post_count(&seen2, "/jobs"),
        0,
        "a status field is written by a controller and is not part of anything anyone signed, so \
         it cannot rescue an approval that binds different bytes. A reconciler that read \
         Approval.status.planHash would have created a Job here"
    );
    assert_eq!(
        outcome2.terminal_state.as_deref(),
        Some(TERMINAL_STATE_PLAN_HASH_MISMATCH)
    );

    // ---- and the pure function agrees, both ways -------------------------
    let restore = restore();
    assert_eq!(
        recomputed_plan_hash(&restore),
        plan_hash(),
        "the hash is sha256_prefixed of spec.planBytes' own bytes"
    );
    let good: weirkeeper::crds::approval::Approval =
        serde_json::from_str(&approval_json(true, &plan_hash(), wrong))
            .expect("the fixture is an Approval");
    assert_eq!(
        approval_plan_hash(&good).as_deref(),
        Some(plan_hash().as_str()),
        "and it is read from INSIDE spec.approvalBytes"
    );
    assert_eq!(
        admit(&restore, Some(&good), Some(&cluster(true))),
        RestoreAdmission::Ok
    );
}

/// `approvalBytes` is document text, never base64 — interface **I18**, from
/// the other side.
///
/// KILLS: base64-decode `approvalBytes` before parsing. A decoder finds no
/// JSON in the raw text, so `approval_plan_hash` returns `None`, the recomputed
/// hash matches nothing, and every approval in the cluster becomes a
/// `PlanHashMismatch`. This test pins the DISTINGUISHABILITY: the raw-text
/// fixture yields the hash and a base64 of the same document does not.
#[test]
fn approval_bytes_are_document_text_and_not_base64() {
    let raw: weirkeeper::crds::approval::Approval =
        serde_json::from_str(&approval_json(true, &plan_hash(), &plan_hash()))
            .expect("the fixture is an Approval");
    assert_eq!(
        approval_plan_hash(&raw).as_deref(),
        Some(plan_hash().as_str())
    );

    // The same document, base64'd by hand (no dependency; this is the shape a
    // decode step would expect to find).
    let b64 = base64_encode(approval_doc(&plan_hash()).as_bytes());
    let mut value: Value = serde_json::from_str(&approval_json(true, &plan_hash(), &plan_hash()))
        .expect("the fixture is JSON");
    value["spec"]["approvalBytes"] = serde_json::json!(b64);
    let encoded: weirkeeper::crds::approval::Approval =
        serde_json::from_value(value).expect("the mutated fixture is an Approval");
    assert_eq!(
        approval_plan_hash(&encoded),
        None,
        "a base64 layer between the approver's file and the verified bytes is the class of \
         transformation planBytes exists to forbid, and the two paths must be DISTINGUISHABLE so \
         a decode step cannot be added silently"
    );
}

/// Base64, standard alphabet with padding. Hand-written: Global Constraint 38
/// closes the workspace graph, and one test's negative fixture is not worth a
/// dependency edge.
fn base64_encode(bytes: &[u8]) -> String {
    const A: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        out.push(A[((n >> 18) & 63) as usize] as char);
        out.push(A[((n >> 12) & 63) as usize] as char);
        out.push(if chunk.len() > 1 {
            A[((n >> 6) & 63) as usize] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            A[(n & 63) as usize] as char
        } else {
            '='
        });
    }
    out
}

/// A target the control plane cannot see is terminal, and it is checked LAST.
///
/// Two arms: the referent is absent, and the referent exists with
/// `status.reachable: false`. Both are `ClusterNotReachable`, both create
/// nothing.
#[tokio::test]
async fn an_unreachable_target_cluster_is_terminal_and_creates_nothing() {
    for (label, status, body) in [
        (
            "absent",
            404,
            not_found_body("kafkaclusters.logweir.dev", "scratch"),
        ),
        ("not reachable", 200, cluster_json(false, PLAINTEXT_AUTH)),
    ] {
        let (client, _rec, bodies) = mock_client_recording_bodies(admission_routes(
            200,
            approval_json(true, &plan_hash(), &plan_hash()),
            status,
            body,
        ));
        let outcome = reconcile_restore(&restore(), &client, &unobserved_scorecard, now())
            .await
            .unwrap_or_else(|e| panic!("[{label}] a terminal refusal is an outcome: {e}"));
        let seen = bodies
            .lock()
            .expect("the body recorder is readable")
            .clone();
        assert_eq!(post_count(&seen, "/jobs"), 0, "[{label}] no Job");
        assert_eq!(
            post_count(&seen, "/configmaps"),
            0,
            "[{label}] no plan either"
        );
        assert_eq!(
            outcome.terminal_state.as_deref(),
            Some(TERMINAL_STATE_CLUSTER_NOT_REACHABLE),
            "[{label}]"
        );
        assert_eq!(outcome.requeue, Requeue::AwaitChange, "[{label}] terminal");
    }

    // AND THE ORDER: the cluster is checked LAST, so an unverified approval
    // over an unreachable cluster reports the APPROVAL. An admission that
    // reported the cluster first would send an operator to look at a broker
    // when the answer is that nobody approved the run.
    assert_eq!(
        admit(&restore(), None, None),
        RestoreAdmission::ApprovalNotVerified {
            approval: APPROVAL.to_string()
        },
        "the approval is checked before the cluster"
    );
    assert_eq!(
        admit(&restore(), Some(&approval(false)), None),
        RestoreAdmission::ApprovalNotVerified {
            approval: APPROVAL.to_string()
        },
        "…and an UNVERIFIED approval over an absent cluster still reports the approval"
    );
    assert_eq!(
        admit(&restore(), Some(&approval(true)), None),
        RestoreAdmission::ClusterNotReachable {
            cluster: "scratch".to_string()
        },
        "only once the approval is verified does the cluster become the answer"
    );
}

/// A `Restore` whose name exceeds 63 characters is refused BEFORE any `POST` —
/// errata **E5d**.
///
/// The API server refuses the Job (`spec.template.labels … must be no more
/// than 63 characters`), which used to become a 15-second requeue with
/// `status: null` FOREVER. Measured on the `Backup` path; the same refusal for
/// the same reason here, and a 63-character name still creates the Job.
#[tokio::test]
async fn a_restore_whose_name_is_too_long_is_refused_before_any_post() {
    let long = "r".repeat(64);
    let routes = vec![
        // BOTH WRITE ROUTES PRESENT, so the zero count is the assertion.
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
            body: restore_json(PLAN_BYTES, APPROVAL, &long),
        },
    ];
    let (client, _rec, bodies) = mock_client_recording_bodies(routes);
    let object: Restore = serde_json::from_str(&restore_json(PLAN_BYTES, APPROVAL, &long))
        .expect("the fixture is a Restore");
    let outcome = reconcile_restore(&object, &client, &unobserved_scorecard, now())
        .await
        .expect("a terminal refusal is an outcome");

    let seen = bodies
        .lock()
        .expect("the body recorder is readable")
        .clone();
    assert_eq!(
        seen.iter().filter(|b| b.method == "POST").count(),
        0,
        "zero POSTs of any kind — and note the Job GET is not made either, because the check is \
         step 0"
    );
    assert_eq!(
        outcome.terminal_state.as_deref(),
        Some(TERMINAL_STATE_NAME_TOO_LONG)
    );
    let statuses = patched_statuses(&seen);
    assert_eq!(statuses[0]["phase"].as_str(), Some("Failed"));
    assert_eq!(
        statuses[0]["exitReason"].as_str(),
        Some(REASON_OPERATIONAL),
        "the run could not be attempted and no artifact was written, which is GC11's code 1"
    );
    assert!(
        statuses[0].get("exitCode").is_none(),
        "and no code is invented"
    );
    let message = statuses[0]["conditions"][0]["message"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    assert!(
        message.contains("64") && message.contains("63"),
        "the refusal names both numbers: {message}"
    );
    assert!(
        TERMINAL_STATES.contains(&TERMINAL_STATE_NAME_TOO_LONG),
        "and the state is on the shared list"
    );
}

/// The `Approval`'s own `ReferentNotFound` is routed explicitly, and is still a
/// HOLD — Task 16's MEDIUM-2, routed here.
///
/// An `Approval` reconciled before this `Restore` existed reports that it
/// cannot find its own subject. That is a RACE, not a signature problem, so
/// the verdict is unchanged — a thirty-second hold — while the reconciler
/// routes on the reason so the log line says which of the two it is.
#[tokio::test]
async fn an_approval_that_cannot_find_its_subject_is_still_a_hold() {
    let mut value: Value = serde_json::from_str(&approval_json(false, &plan_hash(), &plan_hash()))
        .expect("the fixture is JSON");
    value["status"]["conditions"][0]["reason"] = serde_json::json!(REFERENT_NOT_FOUND_REASON);
    let body = serde_json::to_string(&value).expect("the mutated fixture serialises");

    let (client, _rec, bodies) = mock_client_recording_bodies(admission_routes(
        200,
        body,
        200,
        cluster_json(true, PLAINTEXT_AUTH),
    ));
    let outcome = reconcile_restore(&restore(), &client, &unobserved_scorecard, now())
        .await
        .expect("the reconcile completes");
    assert_eq!(
        outcome.requeue,
        Requeue::After(ADMISSION_REQUEUE_SECS),
        "a race is a hold: the Approval controller looks again on its own interval"
    );
    let seen = bodies
        .lock()
        .expect("the body recorder is readable")
        .clone();
    assert_eq!(post_count(&seen, "/jobs"), 0);

    // AND THE SPELLING IS THE APPROVAL MODULE'S OWN. `ReferentProblem::reason`
    // is a method on a value and this reconciler holds only the STRING the
    // condition carries, so the two are pinned against each other here.
    assert_eq!(
        REFERENT_NOT_FOUND_REASON,
        weirkeeper::controllers::approval::ReferentProblem::ReferentNotFound {
            kind: "Restore".to_string(),
            name: NAME.to_string(),
        }
        .reason(),
        "the constant restates ReferentProblem's own reason; a drift here is a route that stops \
         matching silently"
    );
}

// ===========================================================================
// The plan ConfigMap
// ===========================================================================

/// The `POST …/configmaps` body's `data["restore.yaml"]` is **byte-equal** to
/// `spec.planBytes`, trailing whitespace included.
///
/// KILLS: round-trip `planBytes` through `serde_yaml::from_str` + `to_string`
/// before writing the ConfigMap. The fixture's `planBytes` ends with two
/// spaces and a newline, which any parse-and-re-serialise destroys.
#[tokio::test]
async fn the_plan_configmap_carries_the_spec_bytes_verbatim() {
    let (client, _rec, bodies) = mock_client_recording_bodies(admission_routes(
        200,
        approval_json(true, &plan_hash(), &plan_hash()),
        200,
        cluster_json(true, PLAINTEXT_AUTH),
    ));
    reconcile_restore(&restore(), &client, &unobserved_scorecard, now())
        .await
        .expect("the reconcile completes");

    let seen = bodies
        .lock()
        .expect("the body recorder is readable")
        .clone();
    let cm = posted_config_map(&seen);
    let written = cm["data"][PLAN_SPEC_KEY]
        .as_str()
        .expect("the ConfigMap carries the plan key");
    assert_eq!(
        written, PLAN_BYTES,
        "BYTE-EQUAL. planHash binds these exact bytes, so any transformation between the \
         approver's file and the mounted file produces a run nobody authorised"
    );
    assert!(
        PLAN_BYTES.ends_with("  \n"),
        "the fixture must END WITH TWO SPACES AND A NEWLINE or this test asserts nothing about a \
         round trip"
    );
    assert!(
        written.ends_with("  \n"),
        "…and the written value must keep them"
    );
    assert_eq!(
        cm["data"].as_object().map(serde_json::Map::len),
        Some(1),
        "exactly one key: the Restore path COPIES bytes, so there is nothing else to render — \
         the cluster allowlist lives in the approval-bundle Secret (see APPROVAL_BUNDLE_SECRET)"
    );
    assert_eq!(
        cm["metadata"]["name"].as_str(),
        Some(plan_config_map_name(NAME).as_str())
    );

    // The owner reference, both flags.
    let owner = &cm["metadata"]["ownerReferences"][0];
    assert_eq!(owner["kind"].as_str(), Some("Restore"));
    assert_eq!(owner["uid"].as_str(), Some(UID));
    assert_eq!(owner["controller"].as_bool(), Some(true));
    assert_eq!(owner["blockOwnerDeletion"].as_bool(), Some(true));

    // …and it is POSTed BEFORE the Job, because the Job mounts it.
    let posts: Vec<&str> = seen
        .iter()
        .filter(|b| b.method == "POST")
        .map(|b| path(&b.uri))
        .collect();
    let cm_at = posts
        .iter()
        .position(|p| p.ends_with("/configmaps"))
        .expect("the ConfigMap was POSTed");
    let job_at = posts
        .iter()
        .position(|p| p.ends_with("/jobs"))
        .expect("the Job was POSTed");
    assert!(
        cm_at < job_at,
        "the ConfigMap POST precedes the Job POST: a Job created first is a pod that stalls in \
         ContainerCreating on `configmap not found` until its deadline fires. Order: {posts:?}"
    );
}

/// **Interface I20.** The fixture's `planBytes` is accepted by
/// `serde_yaml::from_str::<logweir_core::spec::RestoreSpec>` — so the ConfigMap
/// assertion above is made over the grammar spec §6.1 defines and not over an
/// invented string.
#[test]
fn the_plan_bytes_fixture_is_a_document_the_runner_parses() {
    let parsed: logweir_core::spec::RestoreSpec = serde_yaml::from_str(PLAN_BYTES)
        .expect("planBytes IS the runner's restore.yaml — one grammar (interface I20)");
    assert_eq!(parsed.source.topics, vec!["orders", "payments"]);
    assert_eq!(
        parsed.target.mode,
        logweir_core::spec::TargetMode::Scratch,
        "the fixture is a drill: a Restore with target.mode scratch"
    );
    assert_eq!(parsed.target.topic_mapping_prefix, "drill-");

    // The `newTopic` twin parses too, and its mode really is the other one.
    let other: logweir_core::spec::RestoreSpec = serde_yaml::from_str(&new_topic_plan_bytes())
        .expect("the newTopic fixture is the same grammar");
    assert_eq!(other.target.mode, logweir_core::spec::TargetMode::NewTopic);

    // AND THE SHIPPED EXAMPLE IS THE SAME GRAMMAR — the one definition this
    // task cites rather than inventing a second.
    let example = std::fs::read_to_string(workspace_root().join("examples/restore.yaml"))
        .expect("examples/restore.yaml ships");
    let shipped: logweir_core::spec::RestoreSpec = serde_yaml::from_str(&example)
        .expect("examples/restore.yaml IS the plan document (interface I20)");
    assert_eq!(
        shipped.target.mode,
        logweir_core::spec::TargetMode::NewTopic
    );
}

/// A 409 on the plan ConfigMap is success only when the existing object is
/// OURS.
#[tokio::test]
async fn a_conflicting_plan_config_map_is_terminal_only_when_it_is_not_ours() {
    for (label, owner_uid, expect_job) in [
        ("ours", UID, true),
        (
            "a stranger's",
            "ffffffff-0000-4000-8000-0000000000ff",
            false,
        ),
    ] {
        let mut routes = admission_routes(
            200,
            approval_json(true, &plan_hash(), &plan_hash()),
            200,
            cluster_json(true, PLAINTEXT_AUTH),
        );
        for route in &mut routes {
            if route.method == "POST" && route.path_suffix.ends_with("/configmaps") {
                route.status = 409;
                route.body = r#"{"kind":"Status","apiVersion":"v1","status":"Failure",
                    "message":"configmaps already exists","reason":"AlreadyExists","code":409}"#
                    .to_string();
            }
            if route.method == "GET" && route.path_suffix.contains("/configmaps/") {
                route.body = existing_plan_config_map(owner_uid);
            }
        }
        let (client, _rec, bodies) = mock_client_recording_bodies(routes);
        let outcome = reconcile_restore(&restore(), &client, &unobserved_scorecard, now())
            .await
            .unwrap_or_else(|e| panic!("[{label}] an outcome, never an error: {e}"));
        let seen = bodies
            .lock()
            .expect("the body recorder is readable")
            .clone();
        if expect_job {
            assert_eq!(
                post_count(&seen, "/jobs"),
                1,
                "[{label}] a 409 on an object we own is this same reconcile's previous pass; its \
                 one key is spec.planBytes and spec is immutable, so it is the same bytes"
            );
            assert!(outcome.terminal_state.is_none(), "[{label}]");
        } else {
            assert_eq!(
                post_count(&seen, "/jobs"),
                0,
                "[{label}] the runner Job would mount a plan document this object did not write, \
                 in the pod that holds the signing key"
            );
            assert_eq!(
                outcome.terminal_state.as_deref(),
                Some(TERMINAL_STATE_PLAN_CONFIG_MAP_CONFLICT),
                "[{label}]"
            );
        }
    }
}

// ===========================================================================
// The Job shape and the argv
// ===========================================================================

/// The two Jobs differ **only** in the plan ConfigMap's contents; the argv,
/// the mounts and the failure policy are identical, asserted field by field.
///
/// KILLS: give `scratch` mode a different Job shape (a second container).
#[tokio::test]
async fn scratch_mode_and_new_topic_mode_produce_the_same_job_shape() {
    let mut jobs = Vec::new();
    let mut plans = Vec::new();
    for object in [restore(), restore_new_topic()] {
        let (client, _rec, bodies) = mock_client_recording_bodies(admission_routes(
            200,
            approval_json(
                true,
                &sha256_prefixed(object.spec.plan_bytes.as_bytes()),
                &plan_hash(),
            ),
            200,
            cluster_json(true, PLAINTEXT_AUTH),
        ));
        reconcile_restore(&object, &client, &unobserved_scorecard, now())
            .await
            .expect("both modes are admitted");
        let seen = bodies
            .lock()
            .expect("the body recorder is readable")
            .clone();
        jobs.push(posted_job(&seen));
        plans.push(
            posted_config_map(&seen)["data"][PLAN_SPEC_KEY]
                .as_str()
                .expect("the plan key")
                .to_string(),
        );
    }

    assert_ne!(
        plans[0], plans[1],
        "the two plans differ — that is the whole of the mode difference"
    );
    assert_eq!(
        jobs[0], jobs[1],
        "…and the Jobs are IDENTICAL. `spec.target.mode` adds a marker topic, an allowlisted \
         cluster, a source-equals-target check and phase-9 teardown, and every one of those is a \
         decision the RUNNER makes from the plan document it parses. A controller that gave \
         `scratch` a different Job would put half the mode's meaning in a place the approval does \
         not cover, because planBytes is what the approver signed and the Job template is not"
    );

    // …and the fields that matter, named individually, so a failure says which.
    let spec = &jobs[0]["spec"];
    assert_eq!(spec["backoffLimit"].as_i64(), Some(0));
    assert_eq!(spec["activeDeadlineSeconds"].as_i64(), Some(1800));
    assert!(
        spec.get("ttlSecondsAfterFinished").is_none(),
        "no TTL at creation time: pod GC must never race the exit-code read"
    );
    let pod = &spec["template"]["spec"];
    assert_eq!(pod["restartPolicy"].as_str(), Some("Never"));
    assert_eq!(pod["automountServiceAccountToken"].as_bool(), Some(false));
    let containers = pod["containers"].as_array().expect("containers");
    assert_eq!(containers.len(), 1, "exactly one container");
    assert_eq!(containers[0]["name"].as_str(), Some("runner"));
    assert_eq!(containers[0]["imagePullPolicy"].as_str(), Some("Never"));
    let rules = spec["podFailurePolicy"]["rules"]
        .as_array()
        .expect("the failure policy ships");
    assert_eq!(rules.len(), 2, "two rules, in order");
    assert_eq!(
        rules[0]["onPodConditions"][0]["type"].as_str(),
        Some("DisruptionTarget")
    );
    assert_eq!(
        rules[1]["onExitCodes"]["values"]
            .as_array()
            .map(|v| v.iter().filter_map(Value::as_i64).collect::<Vec<_>>()),
        Some(vec![2, 3, 4])
    );
}

/// The argv, verbatim, with every path a mount path.
#[test]
fn the_runner_argv_is_the_contract() {
    let argv = runner_argv(&restore(), &[KEY_ID_LIVE.to_string()]);
    assert_eq!(
        argv,
        vec![
            "restore".to_string(),
            "run".to_string(),
            "--spec".to_string(),
            "/plan/restore.yaml".to_string(),
            "--approval".to_string(),
            "/approval/approval.json".to_string(),
            "--approver-key".to_string(),
            "/approval/approver.pub.pem".to_string(),
            "--approver-key-ids".to_string(),
            KEY_ID_LIVE.to_string(),
            "--allowed-clusters".to_string(),
            "/approval/allowed-clusters.json".to_string(),
            "--signing-key".to_string(),
            "/signing/key.pem".to_string(),
            "--out".to_string(),
            "/work/scorecard.json".to_string(),
            "--offset-report-out".to_string(),
            "/work/offsets.json".to_string(),
            "--triggered-by".to_string(),
            format!("approval/{APPROVAL}"),
        ],
        "the argv this task fixes, in this order"
    );
    // The paths are the mount paths, from the constants and not from literals.
    assert_eq!(SCORECARD_OUT_PATH, "/work/scorecard.json");
    assert_eq!(OFFSET_REPORT_OUT_PATH, "/work/offsets.json");
    assert_eq!(APPROVAL_MOUNT_PATH, "/approval");

    // `--triggered-by` names the AUTHORISATION, because Restore.spec has no
    // triggeredBy field at all — see `triggered_by`'s own doc comment.
    assert_eq!(triggered_by(&restore()), format!("approval/{APPROVAL}"));
    let emitted = weirkeeper::crds::render_all();
    let restore_crd = emitted
        .iter()
        .find(|r| r.kind == "Restore")
        .expect("the emitter renders a Restore CRD");
    let doc: serde_yaml::Value =
        serde_yaml::from_str(&restore_crd.yaml).expect("the emitted CRD is YAML");
    let props = doc["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]
        ["properties"]
        .as_mapping()
        .expect("spec has properties");
    assert!(
        !props.contains_key(serde_yaml::Value::String("triggeredBy".to_string())),
        "Restore.spec carries no `triggeredBy`, which is WHY the value is derived. If a later \
         task adds the field, this assertion is the reminder to read it instead"
    );
}

/// One flag per UNEXPIRED roster key id — interface **I16**.
///
/// Task 22 owns `the_restore_job_projects_every_unexpired_roster_key_id`,
/// because it owns both sides of the flag. What is asserted here is the
/// FILTER: the expired id the roster's own status names is not passed.
#[test]
fn only_unexpired_roster_key_ids_reach_the_argv() {
    let ids = approver_key_ids(Some(&roster()));
    assert_eq!(
        ids,
        vec![KEY_ID_LIVE.to_string()],
        "the expired id is filtered out, and expiry is read from the STATUS the TrustRoster \
         reconciler wrote — one clock, not two"
    );
    assert!(
        approver_key_ids(None).is_empty(),
        "no roster contributes no flags: an empty --approver-key-ids VALUE would be a key id \
         nothing matches, which turns a missing roster into a signature refusal"
    );

    let argv = runner_argv(&restore(), &ids);
    let flags = argv.iter().filter(|a| *a == "--approver-key-ids").count();
    assert_eq!(flags, 1, "one flag per remaining entry");
    assert!(
        !argv.contains(&KEY_ID_EXPIRED.to_string()),
        "and the expired id is nowhere in the argv"
    );

    // Two live ids produce two flags, each with its own value.
    let two = runner_argv(&restore(), &["a".to_string(), "b".to_string()]);
    assert_eq!(
        two.iter().filter(|a| *a == "--approver-key-ids").count(),
        2,
        "repeated flags, never one comma-joined value"
    );
}

/// The Job's mounts: the plan ConfigMap, the approval bundle Secret and the
/// signing key, each at its own path.
#[test]
fn the_restore_job_mounts_the_plan_the_approval_bundle_and_the_signing_key() {
    let spec = runner_job_spec(&restore(), &cluster(true), &[KEY_ID_LIVE.to_string()])
        .expect("the fixture builds a job spec");
    let built = job::build(&spec);
    let pod = built
        .spec
        .as_ref()
        .and_then(|s| s.template.spec.as_ref())
        .expect("the Job has a pod spec");

    let volumes: Vec<&str> = pod
        .volumes
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(|v| v.name.as_str())
        .collect();
    assert_eq!(
        volumes,
        vec![APPROVAL_VOLUME, SIGNING_VOLUME, "plan", "work"],
        "the two Secret volumes are sorted by name, then the plan ConfigMap, then the writable \
         scratch volume — a deterministic order so the rendered spec is a function of the inputs"
    );

    let approval_volume = pod
        .volumes
        .as_deref()
        .unwrap_or_default()
        .iter()
        .find(|v| v.name == APPROVAL_VOLUME)
        .expect("the approval bundle is projected");
    let secret = approval_volume
        .secret
        .as_ref()
        .expect("as a SECRET and not a ConfigMap");
    assert_eq!(secret.secret_name.as_deref(), Some(APPROVAL_BUNDLE_SECRET));
    assert_eq!(
        secret.default_mode,
        Some(0o440),
        "0440 with fsGroup set is the permission that actually exists on disk"
    );
    let items: Vec<&str> = secret
        .items
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(|i| i.key.as_str())
        .collect();
    assert_eq!(
        items,
        vec![
            APPROVAL_DOC_FILE,
            APPROVAL_SIG_FILE,
            APPROVER_KEY_FILE,
            ALLOWED_CLUSTERS_FILE
        ],
        "all four bundle files, each at its own name"
    );

    let mounts: Vec<(&str, &str)> = pod.containers[0]
        .volume_mounts
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(|m| (m.name.as_str(), m.mount_path.as_str()))
        .collect();
    assert!(mounts.contains(&(APPROVAL_VOLUME, APPROVAL_MOUNT_PATH)));
    assert!(mounts.contains(&(SIGNING_VOLUME, "/signing")));
    assert!(mounts.contains(&("plan", "/plan")));
    assert!(mounts.contains(&("work", "/work")));
    assert_eq!(
        pod.security_context.as_ref().and_then(|c| c.fs_group),
        Some(65532),
        "without fsGroup every run dies opening its own signing key"
    );
}

/// A `scramSha512` target's password reaches the runner as `secretKeyRef`
/// only — interface **I11**, and the controller reads nothing.
#[test]
fn a_scram_target_password_is_projected_by_reference_and_never_read() {
    let scram: weirkeeper::crds::kafka_cluster::KafkaCluster =
        serde_json::from_str(&cluster_json(true, SCRAM_AUTH)).expect("the fixture is a cluster");
    let spec = runner_job_spec(&restore(), &scram, &[]).expect("the job spec builds");
    let built = job::build(&spec);
    let env = built
        .spec
        .as_ref()
        .and_then(|s| s.template.spec.as_ref())
        .map(|p| p.containers[0].env.clone().unwrap_or_default())
        .unwrap_or_default();
    let password = env
        .iter()
        .find(|e| e.name == TARGET_PASSWORD_ENV)
        .expect("a scramSha512 target projects its password variable");
    assert!(
        password.value.is_none(),
        "NEVER A LITERAL: a value here would appear in `kubectl get job -o yaml` for anyone with \
         Job read"
    );
    let key_ref = password
        .value_from
        .as_ref()
        .and_then(|f| f.secret_key_ref.as_ref())
        .expect("valueFrom.secretKeyRef");
    assert_eq!(key_ref.name, "scratch-sasl");
    assert_eq!(key_ref.key, TARGET_PASSWORD_SECRET_KEY);

    // A plaintext target projects NO password variable at all.
    let plain = runner_job_spec(&restore(), &cluster(true), &[]).expect("the job spec builds");
    assert!(
        !job::build(&plain)
            .spec
            .as_ref()
            .and_then(|s| s.template.spec.as_ref())
            .map(|p| p.containers[0].env.clone().unwrap_or_default())
            .unwrap_or_default()
            .iter()
            .any(|e| e.name == TARGET_PASSWORD_ENV),
        "a plaintext target has no password to project"
    );

    // A scramSha512 target with NO secretRef projects nothing either — and the
    // runner then refuses at exit 3 with CredentialNotRenderable, which is
    // interface I11's division of labour and not a gap on this side.
    let mut value: Value =
        serde_json::from_str(&cluster_json(true, SCRAM_AUTH)).expect("the fixture is JSON");
    value["spec"]["auth"]
        .as_object_mut()
        .expect("auth is an object")
        .remove("secretRef");
    let no_secret: weirkeeper::crds::kafka_cluster::KafkaCluster =
        serde_json::from_value(value).expect("the mutated fixture is a cluster");
    let spec = runner_job_spec(&restore(), &no_secret, &[]).expect("the job spec still builds");
    assert!(
        !job::build(&spec)
            .spec
            .as_ref()
            .and_then(|s| s.template.spec.as_ref())
            .map(|p| p.containers[0].env.clone().unwrap_or_default())
            .unwrap_or_default()
            .iter()
            .any(|e| e.name == TARGET_PASSWORD_ENV),
        "no secretRef, no variable — and no controller-side refusal either"
    );
    assert!(
        TERMINAL_STATES.contains(&TERMINAL_STATE_CREDENTIAL_NOT_RENDERABLE),
        "the state the RUNNER refuses with is on the shared list, ready to be mapped"
    );
}

// ===========================================================================
// Interface I8 — the three evidence keys
// ===========================================================================

/// The mock's log body ends with the three lines; the patched
/// `status.evidence.{scorecardKey,sidecarKey,offsetReportKey}` are those three
/// values. A second arm reorders them and asserts the reconciler reads them by
/// KEY NAME, not by position.
///
/// KILLS: take the three evidence keys by position.
#[tokio::test]
async fn the_three_evidence_keys_are_read_from_the_final_three_stdout_lines() {
    // ---- arm 1: in the contract's order --------------------------------
    let (client, _rec, bodies) = mock_client_recording_bodies(finished_routes(
        pod_list_terminated(0),
        log_body(&i8_tail()),
        "Complete",
    ));
    reconcile_restore(&restore(), &client, &unobserved_scorecard, now())
        .await
        .expect("the reconcile completes");
    let seen = bodies
        .lock()
        .expect("the body recorder is readable")
        .clone();
    let status = patched_statuses(&seen).remove(0);
    assert_eq!(
        status["evidence"]["scorecardKey"].as_str(),
        Some(SCORECARD_KEY)
    );
    assert_eq!(status["evidence"]["sidecarKey"].as_str(), Some(SIDECAR_KEY));
    assert_eq!(
        status["evidence"]["offsetReportKey"].as_str(),
        Some(OFFSET_REPORT_KEY)
    );

    // ---- arm 2: REVERSED. Each key still lands in its own field ---------
    let reversed = format!(
        "{OFFSET_REPORT_KEY_PREFIX}{OFFSET_REPORT_KEY}\n\
         {SIDECAR_KEY_PREFIX}{SIDECAR_KEY}\n\
         {SCORECARD_KEY_PREFIX}{SCORECARD_KEY}\n"
    );
    let (client2, _rec2, bodies2) = mock_client_recording_bodies(finished_routes(
        pod_list_terminated(0),
        log_body(&reversed),
        "Complete",
    ));
    reconcile_restore(&restore(), &client2, &unobserved_scorecard, now())
        .await
        .expect("the reconcile completes");
    let seen2 = bodies2
        .lock()
        .expect("the body recorder is readable")
        .clone();
    let status2 = patched_statuses(&seen2).remove(0);
    assert_eq!(
        status2["evidence"]["scorecardKey"].as_str(),
        Some(SCORECARD_KEY),
        "READ BY NAME. A positional reader would have written the `.sig` key into scorecardKey \
         and the `.offsets.json` key into sidecarKey — both `look like` object keys, and neither \
         is one a verifier could fetch"
    );
    assert_eq!(
        status2["evidence"]["sidecarKey"].as_str(),
        Some(SIDECAR_KEY)
    );
    assert_eq!(
        status2["evidence"]["offsetReportKey"].as_str(),
        Some(OFFSET_REPORT_KEY)
    );

    // ---- arm 3: the pure scanner, over the shapes a pod log really has --
    // E4: a pod log is stdout and stderr MERGED IN NONDETERMINISTIC ORDER, so
    // the discriminator lines are not last in either direction.
    let interleaved = format!(
        "{SCORECARD_KEY_PREFIX}{SCORECARD_KEY}\n\
         a human sentence the runner wrote to stderr\n\
         {SIDECAR_KEY_PREFIX}{SIDECAR_KEY}\n\
         another one\n"
    );
    let keys = restore_evidence_keys(&log_body(&interleaved));
    assert_eq!(keys.scorecard.as_deref(), Some(SCORECARD_KEY));
    assert_eq!(keys.sidecar.as_deref(), Some(SIDECAR_KEY));
    assert_eq!(keys.offset_report, None);
    assert!(
        keys.mandatory_complete(),
        "TWO OF THREE IS COMPLETE. Interface I8's third line is printed exactly when the engine \
         wrote an offset report, so a two-line tail at exit 0 is a truthful answer and not an \
         unreadable log"
    );

    // A log with no key lines at all yields three absences and no guess.
    let none = restore_evidence_keys(&log_body("the run was refused\n"));
    assert_eq!(none, RestoreEvidenceKeys::default());
    assert!(!none.mandatory_complete());
}

/// The two MANDATORY keys missing at exit 0 is `EvidenceRecorded=False` /
/// `EvidenceKeysUnreadable`; the third key's absence is not.
///
/// Errata **E5c**: the condition exists only at exit 0, under its own type,
/// because Global Constraint 11 says exits 1, 3 and 4 write no artifact.
#[tokio::test]
async fn the_two_mandatory_keys_missing_at_exit_zero_is_its_own_condition() {
    // ---- arm 1: exit 0, no key lines at all ----------------------------
    let (client, _rec, bodies) = mock_client_recording_bodies(finished_routes(
        pod_list_terminated(0),
        log_body("the runner said nothing machine-readable\n"),
        "Complete",
    ));
    reconcile_restore(&restore(), &client, &unobserved_scorecard, now())
        .await
        .expect("the reconcile completes");
    let seen = bodies
        .lock()
        .expect("the body recorder is readable")
        .clone();
    let status = patched_statuses(&seen).remove(0);
    let conditions = conditions_of(&status);
    assert_eq!(
        conditions,
        vec![
            (
                "Complete".to_string(),
                "True".to_string(),
                // CamelCase, per errata E5b: `Ok`, not the wire string `ok`.
                CONDITION_REASON_OK.to_string()
            ),
            (
                "EvidenceRecorded".to_string(),
                "False".to_string(),
                "EvidenceKeysUnreadable".to_string()
            ),
        ],
        "its OWN type, so it can never collide with Complete or Failed (errata E5c)"
    );
    assert!(
        status.get("evidence").is_none(),
        "and no key is guessed from the run id: {status}"
    );

    // ---- arm 2: exit 0 with only the TWO mandatory lines ----------------
    let two = format!("{SCORECARD_KEY_PREFIX}{SCORECARD_KEY}\n{SIDECAR_KEY_PREFIX}{SIDECAR_KEY}\n");
    let (client2, _rec2, bodies2) = mock_client_recording_bodies(finished_routes(
        pod_list_terminated(0),
        log_body(&two),
        "Complete",
    ));
    reconcile_restore(&restore(), &client2, &unobserved_scorecard, now())
        .await
        .expect("the reconcile completes");
    let seen2 = bodies2
        .lock()
        .expect("the body recorder is readable")
        .clone();
    let status2 = patched_statuses(&seen2).remove(0);
    assert_eq!(
        conditions_of(&status2)[1],
        (
            "EvidenceRecorded".to_string(),
            "True".to_string(),
            "EvidenceKeysRecorded".to_string()
        ),
        "the third line is CONDITIONAL, so its absence must NOT raise EvidenceKeysUnreadable on \
         a run that simply had no offset report"
    );
    assert!(
        status2["evidence"].get("offsetReportKey").is_none(),
        "and the absent key is absent rather than empty: {status2}"
    );

    // ---- arm 3: a refusal raises NO evidence condition at all -----------
    let (client3, _rec3, bodies3) = mock_client_recording_bodies(finished_routes(
        pod_list_terminated(3),
        log_body("refusal-reason=TargetTopicConfigRefused\n"),
        "Failed",
    ));
    reconcile_restore(&restore(), &client3, &unobserved_scorecard, now())
        .await
        .expect("the reconcile completes");
    let seen3 = bodies3
        .lock()
        .expect("the body recorder is readable")
        .clone();
    let status3 = patched_statuses(&seen3).remove(0);
    assert_eq!(
        conditions_of(&status3).len(),
        1,
        "EXACTLY ONE condition for a failed run (errata E5c). A guard-refused run produced no \
         evidence BY CONTRACT, so nothing was `unreadable`: {status3}"
    );
    assert_no_duplicate_condition_types(&status3);
}

/// No two conditions in one status share a `type`.
///
/// A condition array is a MAP KEYED BY `type`, so two entries sharing one is a
/// malformed status whatever their statuses say — and the day any task gives
/// the array the standard `x-kubernetes-list-type: map` the API server rejects
/// the patch (review finding HIGH-2, errata **E5c**).
fn assert_no_duplicate_condition_types(status: &Value) {
    let mut types: Vec<String> = conditions_of(status).into_iter().map(|c| c.0).collect();
    let before = types.len();
    types.sort();
    types.dedup();
    assert_eq!(
        types.len(),
        before,
        "two conditions share a type in {status}; a condition array is a map keyed by `type`"
    );
}

// ===========================================================================
// The exit-code contract
// ===========================================================================

/// A table over `TargetTopicConfigRefused` and `CredentialNotRenderable`: the
/// mock's log body ends in `refusal-reason=<X>` and the patched condition
/// reason is `<X>`. A third row whose log body has no such line yields
/// `GuardRefusedUnknownReason`, never a guess.
///
/// KILLS: pick the first terminal state on a tie instead of reading the
/// refusal line; guess a terminal state when the line is absent.
#[tokio::test]
async fn an_exit_three_maps_to_the_terminal_state_its_refusal_line_names() {
    for (label, tail, expect) in [
        (
            "target topic",
            format!("refusal-reason={TERMINAL_STATE_TARGET_TOPIC_CONFIG_REFUSED}\n"),
            TERMINAL_STATE_TARGET_TOPIC_CONFIG_REFUSED,
        ),
        (
            "credential",
            // E4: the discriminator is NOT last here. A reader that took the
            // last line would report `GuardRefusedUnknownReason` for a
            // perfectly explicit refusal.
            format!(
                "refusal-reason={TERMINAL_STATE_CREDENTIAL_NOT_RENDERABLE}\n\
                 guard: $LOGWEIR_TARGET_PASSWORD is unset\n"
            ),
            TERMINAL_STATE_CREDENTIAL_NOT_RENDERABLE,
        ),
        (
            "no refusal line at all",
            "the pod said nothing about why\n".to_string(),
            TERMINAL_STATE_GUARD_REFUSED_UNKNOWN_REASON,
        ),
    ] {
        let (client, _rec, bodies) = mock_client_recording_bodies(finished_routes(
            pod_list_terminated(3),
            log_body(&tail),
            "Failed",
        ));
        let outcome = reconcile_restore(&restore(), &client, &unobserved_scorecard, now())
            .await
            .unwrap_or_else(|e| panic!("[{label}] {e}"));
        assert_eq!(outcome.exit_code, Some(3), "[{label}]");
        assert_eq!(outcome.terminal_state.as_deref(), Some(expect), "[{label}]");
        let seen = bodies
            .lock()
            .expect("the body recorder is readable")
            .clone();
        let status = patched_statuses(&seen).remove(0);
        assert_eq!(status["exitCode"].as_i64(), Some(3), "[{label}]");
        assert_eq!(
            status["exitReason"].as_str(),
            Some(expect),
            "[{label}] the terminal state is the most specific thing known about the run"
        );
        assert_eq!(
            conditions_of(&status),
            vec![(
                "Failed".to_string(),
                "True".to_string(),
                // CamelCase, per errata E5b — `exitReason` keeps the wire
                // string and the CONDITION does not.
                CONDITION_REASON_GUARD_REFUSED.to_string()
            )],
            "[{label}] the CONDITION says only `a guard refused`; WHICH guard is exitReason"
        );
        assert!(
            TERMINAL_STATES.contains(&expect),
            "[{label}] {expect} is on the shared list"
        );
    }
}

/// Exit 2 whose scorecard `outcome` names an archive-coverage failure is
/// `WindowNotCovered`.
#[test]
fn window_not_covered_is_exit_two_with_a_coverage_outcome() {
    assert_eq!(
        window_not_covered(2, Some(OUTCOME_FAIL_COVERAGE)),
        Some(TERMINAL_STATE_WINDOW_NOT_COVERED)
    );
    assert_eq!(window_not_covered(2, Some("fail-objective")), None);
    assert_eq!(window_not_covered(2, None), None);
    assert_eq!(
        window_not_covered(0, Some(OUTCOME_FAIL_COVERAGE)),
        None,
        "exit 0 is a pass whatever a document says; the code and the outcome must AGREE for this \
         mapping to fire"
    );
    assert_eq!(window_not_covered(3, Some(OUTCOME_FAIL_COVERAGE)), None);

    // AND THE VALUE IS NOT IN THE FROZEN 1.0.0 ENUM — recorded, not papered
    // over. The controller reads `outcome` as a string and copies it verbatim,
    // so a document carrying this value is one it will classify; no runner
    // this build ships can produce one.
    let schema: Value = serde_json::from_str(
        &std::fs::read_to_string(
            workspace_root().join("schemas/logweir-drill-scorecard-1.0.0.json"),
        )
        .expect("the frozen schema ships"),
    )
    .expect("the schema is JSON");
    let allowed: Vec<String> = schema["definitions"]["Outcome"]["enum"]
        .as_array()
        .expect("Outcome is an enum")
        .iter()
        .filter_map(|v| v.as_str().map(str::to_string))
        .collect();
    assert!(
        !allowed.contains(&OUTCOME_FAIL_COVERAGE.to_string()),
        "if a later minor adds `fail-coverage` to the frozen enum, this assertion is the \
         reminder that the mapping above becomes live. Got: {allowed:?}"
    );
}

/// The crashed-Job case: a Job that finished with no terminated state for
/// `runner` gets a TERMINAL status with `exitCode` ABSENT.
#[tokio::test]
async fn a_job_that_finished_without_a_terminated_state_gets_a_terminal_status() {
    let empty = r#"{"apiVersion":"v1","kind":"PodList","metadata":{},"items":[]}"#.to_string();
    let (client, _rec, bodies) =
        mock_client_recording_bodies(finished_routes(empty, log_body(""), "Failed"));
    let outcome = reconcile_restore(&restore(), &client, &unobserved_scorecard, now())
        .await
        .expect("the reconcile completes");
    assert_eq!(outcome.exit_code, None, "no code is invented");
    assert_eq!(outcome.terminal_state.as_deref(), Some("NoExitCode"));
    assert!(
        !outcome.ttl_patched,
        "and no TTL is patched on a run with no code"
    );
    let seen = bodies
        .lock()
        .expect("the body recorder is readable")
        .clone();
    let status = patched_statuses(&seen).remove(0);
    assert!(
        status.get("exitCode").is_none(),
        "a fabricated 1 is indistinguishable from a real operational failure and a fabricated 0 \
         is a green badge for a run that never reported: {status}"
    );
    assert_eq!(status["exitReason"].as_str(), Some(REASON_OPERATIONAL));
    assert_eq!(status["phase"].as_str(), Some("Failed"));
    assert_eq!(
        outcome.requeue,
        Requeue::AwaitChange,
        "and it never watches forever"
    );
}

/// The TTL is patched only AFTER the status write.
#[tokio::test]
async fn ttl_is_patched_only_after_status() {
    let (client, rec, _bodies) = mock_client_recording_bodies(finished_routes(
        pod_list_terminated(0),
        log_body(&i8_tail()),
        "Complete",
    ));
    let outcome = reconcile_restore(&restore(), &client, &unobserved_scorecard, now())
        .await
        .expect("the reconcile completes");
    assert!(outcome.ttl_patched);
    let seen = rec.lock().expect("the recorder is readable").clone();
    let patches: Vec<&str> = seen
        .iter()
        .filter(|r| r.method == "PATCH")
        .map(|r| path(&r.uri))
        .collect();
    let status_at = patches
        .iter()
        .position(|p| p.ends_with("/status"))
        .expect("the status was patched");
    let job_at = patches
        .iter()
        .position(|p| !p.ends_with("/status"))
        .expect("the Job was patched");
    assert!(
        status_at < job_at,
        "pod garbage collection must never race the exit-code read: the TTL controller deletes \
         the Job AND its pods, and the code lives on the pod. Order: {patches:?}"
    );

    // …and a status patch that answered 500 produces ZERO Job patches.
    let mut routes = finished_routes(pod_list_terminated(0), log_body(&i8_tail()), "Complete");
    for route in &mut routes {
        if route.method == "PATCH" && route.path_suffix.ends_with("/status") {
            route.status = 500;
            route.body = r#"{"kind":"Status","apiVersion":"v1","status":"Failure",
                "message":"the status subresource is unavailable","code":500}"#
                .to_string();
        }
    }
    let (client2, rec2, _b2) = mock_client_recording_bodies(routes);
    let err = reconcile_restore(&restore(), &client2, &unobserved_scorecard, now())
        .await
        .expect_err("a 500 on the status patch is an error, not an outcome");
    assert!(format!("{err}").contains("kubernetes API error"));
    let seen2 = rec2.lock().expect("the recorder is readable").clone();
    assert_eq!(
        seen2
            .iter()
            .filter(|r| r.method == "PATCH" && !path(&r.uri).ends_with("/status"))
            .count(),
        0,
        "the `?` on the status patch is what makes the ordering a guarantee rather than a comment"
    );
}

// ===========================================================================
// The scorecard's own values — I21, I34
// ===========================================================================

/// For the fixture scorecard whose `integrity.result` is `partial`,
/// `status.integrity.partialReason` is the scorecard's string and
/// `status.objectives` has the scorecard's four values.
///
/// KILLS: drop `objectives` from the status write.
#[tokio::test]
async fn the_restore_status_carries_objectives_and_partial_reason() {
    const REASON: &str = "the archive returned a zero-length fingerprint for 2 of 75 records";
    let doc = scorecard_json("fail-integrity", "partial", REASON);
    let observation = scorecard_observation(doc.as_bytes()).expect("the fixture is a scorecard");
    let oracle = move |key: String| -> BoxFuture<'static, Option<ScorecardObservation>> {
        assert_eq!(
            key, SCORECARD_KEY,
            "the oracle is asked for the key the log named"
        );
        let o = observation.clone();
        Box::pin(async move { Some(o) })
    };

    let (client, _rec, bodies) = mock_client_recording_bodies(finished_routes(
        pod_list_terminated(2),
        log_body(&i8_tail()),
        "Failed",
    ));
    reconcile_restore(&restore(), &client, &oracle, now())
        .await
        .expect("the reconcile completes");
    let seen = bodies
        .lock()
        .expect("the body recorder is readable")
        .clone();
    let status = patched_statuses(&seen).remove(0);

    assert_eq!(
        status["integrity"]["partialReason"].as_str(),
        Some(REASON),
        "interface I34: a `partial` with no reason is a badge an auditor cannot act on"
    );
    assert_eq!(status["integrity"]["result"].as_str(), Some("partial"));
    assert_eq!(
        status["integrity"]["level"].as_str(),
        Some("byte-fingerprint"),
        "the scorecard's own kebab-case spelling, carried through unchanged"
    );
    assert_eq!(status["objectives"]["rtoSeconds"].as_i64(), Some(900));
    assert_eq!(status["objectives"]["rpoSeconds"].as_i64(), Some(300));
    assert_eq!(status["objectives"]["passRate"].as_f64(), Some(1.0));
    assert_eq!(status["objectives"]["met"].as_bool(), Some(true));
    assert_eq!(status["measured"]["rtoSeconds"].as_i64(), Some(512));
    assert_eq!(status["measured"]["rpoSeconds"].as_i64(), Some(0));
    assert_eq!(status["outcome"].as_str(), Some("fail-integrity"));
    assert_eq!(status["lastPhaseCompleted"].as_i64(), Some(7));
    assert_eq!(
        status["evidence"]["offsetReportSha256"].as_str(),
        Some("sha256:abc"),
        "COPIED from the signed document, not recomputed"
    );
    assert_eq!(
        status["evidence"]["scorecardSha256"].as_str(),
        Some(sha256_prefixed(doc.as_bytes()).as_str()),
        "…and the scorecard's own digest is COMPUTED, because a document cannot carry its own"
    );

    // `passRate` and `met` are ABSENT where the scorecard's are null.
    let null_doc = scorecard_json("pass", "pass", "")
        .replace("\"pass_rate\": 1.0", "\"pass_rate\": null")
        .replace("\"met\": true", "\"met\": null");
    let null_observed =
        scorecard_observation(null_doc.as_bytes()).expect("the fixture is a scorecard");
    assert_eq!(null_observed.objective_pass_rate, None);
    assert_eq!(null_observed.objective_met, None);
    assert_eq!(
        null_observed.integrity_partial_reason, None,
        "and a null partial_reason is an absence, never an empty string"
    );

    // A `None` observation OMITS every scorecard-derived key.
    let (client2, _rec2, bodies2) = mock_client_recording_bodies(finished_routes(
        pod_list_terminated(2),
        log_body(&i8_tail()),
        "Failed",
    ));
    reconcile_restore(&restore(), &client2, &unobserved_scorecard, now())
        .await
        .expect("the reconcile completes");
    let seen2 = bodies2
        .lock()
        .expect("the body recorder is readable")
        .clone();
    let status2 = patched_statuses(&seen2).remove(0);
    for key in [
        "outcome",
        "objectives",
        "integrity",
        "measured",
        "lastPhaseCompleted",
    ] {
        assert!(
            status2.get(key).is_none(),
            "`None` means NOT OBSERVED, so `{key}` is omitted — a merge patch with no key means \
             `leave it alone`, which is the only honest thing to write about a document that \
             could not be fetched: {status2}"
        );
    }
    assert_eq!(
        status2["exitCode"].as_i64(),
        Some(2),
        "…while the exit code, which is a fact about the POD, is still recorded"
    );
    assert!(
        status2["evidence"].get("scorecardSha256").is_none(),
        "and no digest is invented for bytes nobody read"
    );
}

/// `status.topicPreflight` is NOT written, and the CRD says why.
///
/// Guard **G-TS**'s observation is returned by phase 0 in
/// `logweir::drill::RestoreOutcome::topic_preflight` and, by Global Constraint
/// 12 as amended, is deliberately not a scorecard field. **Nothing carries it
/// out of the pod**: interface I8 fixes three stdout key lines and none of
/// them is a preflight. So the field stays absent rather than fabricated, and
/// the gap is recorded in the field's own description.
#[tokio::test]
async fn the_topic_preflight_has_no_producer_and_is_left_absent() {
    let doc = scorecard_json("pass", "pass", "");
    let observation = scorecard_observation(doc.as_bytes()).expect("the fixture is a scorecard");
    let oracle = move |_key: String| -> BoxFuture<'static, Option<ScorecardObservation>> {
        let o = observation.clone();
        Box::pin(async move { Some(o) })
    };
    let (client, _rec, bodies) = mock_client_recording_bodies(finished_routes(
        pod_list_terminated(0),
        log_body(&i8_tail()),
        "Complete",
    ));
    reconcile_restore(&restore(), &client, &oracle, now())
        .await
        .expect("the reconcile completes");
    let seen = bodies
        .lock()
        .expect("the body recorder is readable")
        .clone();
    let status = patched_statuses(&seen).remove(0);
    assert!(
        status.get("topicPreflight").is_none(),
        "ABSENT, never fabricated — the same rule the evidence keys follow: {status}"
    );

    let doc = restore_crd_field_description(&["status", "topicPreflight"]);
    assert!(
        doc.contains("no producer") || doc.contains("not written"),
        "and the CRD field's own description records the gap in place, so a reader of the shipped \
         schema is not left wondering why it is always empty. Got: {doc}"
    );
}

/// **Interface I21.** `crds/restore.rs`'s `outcome` field description states
/// that green requires verification `Valid` and outcome `pass`, and
/// `config/crd/restores.yaml` declares `status.outcome`.
#[test]
fn the_restore_green_rule_reads_the_outcome() {
    let doc = restore_crd_field_description(&["status", "outcome"]);
    for needle in ["green", "Valid", "pass"] {
        assert!(
            doc.contains(needle),
            "the outcome field's description must state the green rule and name `{needle}`. Got: \
             {doc}"
        );
    }

    // And the SHIPPED file declares it, not just the emitter.
    let shipped = workspace_yaml("config/crd/restores.yaml");
    let props = &shipped["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]
        ["status"]["properties"];
    assert!(
        props.get("outcome").is_some(),
        "config/crd/restores.yaml declares status.outcome — the field the badge reads"
    );
    assert!(
        props.get("objectives").is_some() && props.get("integrity").is_some(),
        "…and interface I34's two blocks"
    );
}

/// The printer columns are the contract.
#[test]
fn the_restore_printer_columns_are_the_contract() {
    let shipped = workspace_yaml("config/crd/restores.yaml");
    let columns: Vec<String> = shipped["spec"]["versions"][0]["additionalPrinterColumns"]
        .as_sequence()
        .expect("the Restore CRD declares printer columns")
        .iter()
        .map(|c| {
            c["name"]
                .as_str()
                .expect("every column has a name")
                .to_string()
        })
        .collect();
    assert_eq!(
        columns,
        vec![
            "MODE",
            "PHASE",
            "EXIT",
            "REASON",
            "OUTCOME",
            "INTEGRITY",
            "RTO",
            "SIGNED",
            "AGE"
        ],
        "`kubectl get restores` is an interface: a renamed or reordered column breaks a runbook \
         nobody will think to update"
    );
}

/// The emitted `Restore` CRD's description for a `["status", "<field>"]` path.
fn restore_crd_field_description(path: &[&str]) -> String {
    let emitted = weirkeeper::crds::render_all();
    let restore_crd = emitted
        .iter()
        .find(|r| r.kind == "Restore")
        .expect("the emitter renders a Restore CRD");
    let doc: serde_yaml::Value =
        serde_yaml::from_str(&restore_crd.yaml).expect("the emitted CRD is YAML");
    let mut node = &doc["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"];
    for segment in path {
        node = &node[*segment];
        node = &node["properties"];
    }
    // The last hop over-stepped into `properties`; step back to the node and
    // read its description.
    let mut node = &doc["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"];
    for (i, segment) in path.iter().enumerate() {
        node = &node[*segment];
        if i + 1 < path.len() {
            node = &node["properties"];
        }
    }
    node["description"]
        .as_str()
        .unwrap_or_else(|| panic!("{path:?} carries no description"))
        .to_string()
}

// ===========================================================================
// The topics, derived from the approved bytes
// ===========================================================================

/// `oldTopics` are the source names and `newTopics` are the mapped ones,
/// derived from `spec.planBytes` through the ONE prefix rule.
#[tokio::test]
async fn the_status_carries_the_old_and_new_topic_names() {
    // scratch: the prefix is `target.topic_mapping_prefix`.
    let (old, new) = topic_mapping(&restore()).expect("the fixture's planBytes parse");
    assert_eq!(old, vec!["orders", "payments"]);
    assert_eq!(new, vec!["drill-orders", "drill-payments"]);

    // newTopic: the prefix is `target.topic_naming.prefix`.
    let (old2, new2) = topic_mapping(&restore_new_topic()).expect("the newTopic planBytes parse");
    assert_eq!(old2, vec!["orders", "payments"]);
    assert_eq!(
        new2,
        vec!["incident-4471-orders", "incident-4471-payments"],
        "`logweir_core::spec::target_topic_prefix` is the ONE place the rule lives, so the \
         renderer, the runner's refusal messages and this list cannot derive it differently"
    );

    // planBytes that are not a RestoreSpec yield NOTHING rather than a
    // fabricated list.
    let mut value: Value =
        serde_json::from_str(&restore_json(PLAN_BYTES, APPROVAL, NAME)).expect("JSON");
    value["spec"]["planBytes"] = serde_json::json!("not: a: restore: spec:\n");
    let broken: Restore = serde_json::from_value(value).expect("the mutated fixture is a Restore");
    assert_eq!(
        topic_mapping(&broken),
        None,
        "the runner will refuse the same bytes; a fabricated topic list would be a status field \
         naming topics no run touched"
    );

    // …and both lists reach the status.
    let (client, _rec, bodies) = mock_client_recording_bodies(finished_routes(
        pod_list_terminated(0),
        log_body(&i8_tail()),
        "Complete",
    ));
    reconcile_restore(&restore(), &client, &unobserved_scorecard, now())
        .await
        .expect("the reconcile completes");
    let seen = bodies
        .lock()
        .expect("the body recorder is readable")
        .clone();
    let status = patched_statuses(&seen).remove(0);
    assert_eq!(
        status["oldTopics"]
            .as_array()
            .map(|a| a.iter().filter_map(Value::as_str).collect::<Vec<_>>()),
        Some(vec!["orders", "payments"])
    );
    assert_eq!(
        status["newTopics"]
            .as_array()
            .map(|a| a.iter().filter_map(Value::as_str).collect::<Vec<_>>()),
        Some(vec!["drill-orders", "drill-payments"])
    );

    // The CRD field description says WHY these two exist in tag 1.
    let doc = restore_crd_field_description(&["status", "oldTopics"]);
    assert!(
        doc.contains("Switchover"),
        "the two lists exist in tag 1 solely so tag 2's Switchover retirement has a list to \
         validate against, and the field description says so. Got: {doc}"
    );
}

// ===========================================================================
// Source-reading properties
// ===========================================================================

/// **Interface I11.** `restore.rs` names neither the runner's credential
/// predicate nor a namespaced Secret API, and its doc comment states the
/// runner enforces it.
///
/// KILLS: re-use `credential_is_renderable` in the controller.
#[test]
fn the_controller_performs_no_credential_check() {
    let src = this_module_source();
    for forbidden in ["credential_is_renderable", "Api::<Secret>", "\"secrets\""] {
        assert!(
            !src.contains(forbidden),
            "`{forbidden}` must not appear in controllers/restore.rs. `weirkeeper` holds NO `get` \
             on Secrets anywhere (spec §9), so it never sees the projected value and has nothing \
             to validate; the check runs in the RUNNER, at the moment it reads the password \
             variable, and exits 3 with CredentialNotRenderable (interface I11)"
        );
    }
    // …and the division of labour is WRITTEN DOWN, so a reader does not go
    // looking for a controller-side check that "no get on Secrets" forbids.
    assert!(
        src.contains("VALIDATED BY THE RUNNER"),
        "the module header must state that the runner enforces the credential check"
    );
    assert!(
        src.contains("interface **I11**") || src.contains("Interface **I11**"),
        "…and cite the interface it is"
    );
}

/// The controller never parses a scorecard into a typed struct and re-emits
/// it.
///
/// `Scorecard` has no `deny_unknown_fields` and every field is
/// `#[serde(default)]`, so a field the reader's struct does not declare is
/// silently DROPPED on parse — and a controller that re-emitted the result
/// would publish a status block that quietly disagrees with the signed
/// document.
///
/// KILLS: parse the scorecard into `logweir_core::scorecard::Scorecard`.
#[test]
fn the_controller_never_types_a_scorecard() {
    let src = this_module_source();
    // THE TOKENS ARE ASSEMBLED, NOT SPELLED. This test reads
    // `controllers/restore.rs`, and that file's own module header explains at
    // length why it never parses a scorecard into a typed struct — so a
    // literal here would be matched by the prose that argues for it, exactly
    // as `tests/linkage.rs::the_controller_never_reads_a_secret` is defeated
    // by a comment that names its token.
    let typed = format!("scorecard::{}", "Scorecard");
    let from_slice = format!("from_slice::<{}>", "Scorecard");
    let from_str = format!("from_str::<{}>", "Scorecard");
    for forbidden in [typed.as_str(), from_slice.as_str(), from_str.as_str()] {
        assert!(
            !src.contains(forbidden),
            "`{forbidden}` must not appear in controllers/restore.rs: the values it needs are \
             lifted out of a serde_json::Value by pointer and copied verbatim"
        );
    }
    assert!(
        src.contains("let doc: Value = serde_json::from_slice(bytes)"),
        "…and the reader really is a `Value` reader, so a field this module does not declare is \
         carried by the document rather than dropped on parse"
    );
    assert!(
        src.contains("doc.pointer("),
        "…which lifts the values it needs out by POINTER and copies them"
    );
    // The observation carries only what it copies, and it round-trips a
    // document with fields the observation does not declare.
    let with_extra = scorecard_json("pass", "pass", "").replace(
        "\"format_version\": \"1.0.0\",",
        "\"format_version\": \"1.0.0\", \"a_field_no_reader_declares\": {\"x\": 1},",
    );
    let observed = scorecard_observation(with_extra.as_bytes())
        .expect("a document with unknown fields is still read");
    assert_eq!(observed.outcome.as_deref(), Some("pass"));
    assert_eq!(
        observed.scorecard_sha256.as_deref(),
        Some(sha256_prefixed(with_extra.as_bytes()).as_str()),
        "the digest is over the bytes AS FETCHED, so it binds the document including whatever \
         this reader does not understand"
    );
    // Not-JSON is NOT OBSERVED, never an empty observation.
    assert_eq!(scorecard_observation(b"not json"), None);
    assert_eq!(
        scorecard_observation(b"[1,2,3]"),
        None,
        "and neither is a JSON array"
    );
}

/// Every reason string this reconciler can write is a member of
/// `conditions::TERMINAL_STATES` or of `conditions::CONDITION_REASONS`.
///
/// A table test over the reconciler's own enum, so a sixth `RestoreAdmission`
/// variant cannot reach the cluster as a `reason` nobody put on a list.
#[test]
fn each_terminal_state_is_in_the_shared_list() {
    let admissions = [
        RestoreAdmission::Ok,
        RestoreAdmission::ApprovalNotVerified {
            approval: APPROVAL.to_string(),
        },
        RestoreAdmission::ApprovalNotReceived {
            approval: String::new(),
        },
        RestoreAdmission::PlanHashMismatch {
            recomputed: "sha256:a".to_string(),
            approval_says: "sha256:b".to_string(),
        },
        RestoreAdmission::ClusterNotReachable {
            cluster: "scratch".to_string(),
        },
    ];
    for admission in &admissions {
        let reason = admission.reason();
        assert!(
            TERMINAL_STATES.contains(&reason) || CONDITION_REASONS.contains(&reason),
            "`{reason}` is written into a condition and is on neither shared list"
        );
        // CamelCase, per errata E5b: `metav1.Condition`'s own pattern is
        // `^[A-Za-z]([A-Za-z0-9_,:]*[A-Za-z0-9_])?$`, which forbids `-`.
        assert!(
            !reason.contains('-') && reason.starts_with(|c: char| c.is_ascii_uppercase()),
            "`{reason}` is not a valid metav1.Condition reason"
        );
        // Every message names something an operator can act on.
        assert!(
            admission.to_string().len() > 40,
            "`{reason}`'s message is too short to explain itself: {admission}"
        );
    }

    // The TERMINAL split, asserted on the values and not on a comment.
    assert!(!admissions[0].is_terminal(), "Ok is not terminal");
    assert!(
        !admissions[1].is_terminal(),
        "ApprovalNotVerified is a HOLD — interface I19 — and requeues at 30 s"
    );
    for a in &admissions[2..] {
        assert!(
            a.is_terminal(),
            "{} is over a CEL-immutable spec and can never succeed on a later pass",
            a.reason()
        );
    }

    // The five wire reasons stay their own vocabulary and never appear as a
    // condition reason here.
    for wire in [
        REASON_OK,
        REASON_OPERATIONAL,
        REASON_DRILL_NOT_PASS,
        REASON_GUARD_REFUSED,
        REASON_SIGNING_OR_LOCK,
    ] {
        assert!(
            !admissions.iter().any(|a| a.reason() == wire),
            "`{wire}` is a wire string for status.exitReason, not a condition reason"
        );
    }
    assert!(
        !TERMINAL_STATES.contains(&REASON_APPROVAL_NOT_VERIFIED),
        "ApprovalNotVerified is deliberately NOT terminal: it is the one admission outcome that \
         can change without anybody touching the object (interface I19)"
    );
    assert!(CONDITION_REASONS.contains(&REASON_APPROVAL_NOT_VERIFIED));
    assert!(CONDITION_REASONS.contains(&REASON_ADMITTED));
}

/// [`Requeue`] maps onto the `Action` the runtime actually gets.
///
/// The interval a test asserts and the interval `kube` receives must not be
/// two different numbers, and `Action` implements no `PartialEq` — so the
/// mapping is pinned through `Debug`, which is the only observable it has.
#[test]
fn the_requeue_maps_onto_the_action_the_runtime_gets() {
    let outcome = |requeue| weirkeeper::controllers::restore::RestoreOutcome {
        job_name: NAME.to_string(),
        created: false,
        admission: None,
        exit_code: None,
        terminal_state: None,
        keys: RestoreEvidenceKeys::default(),
        ttl_patched: false,
        requeue,
    };
    assert_eq!(
        format!("{:?}", action_for(&outcome(Requeue::AwaitChange))),
        format!("{:?}", kube::runtime::controller::Action::await_change())
    );
    assert_eq!(
        format!("{:?}", action_for(&outcome(Requeue::After(30)))),
        format!(
            "{:?}",
            kube::runtime::controller::Action::requeue(std::time::Duration::from_secs(30))
        )
    );
    assert_ne!(
        format!("{:?}", action_for(&outcome(Requeue::After(30)))),
        format!(
            "{:?}",
            kube::runtime::controller::Action::requeue(std::time::Duration::from_secs(15))
        ),
        "…and 30 is distinguishable from 15, or the interface-I19 assertion means nothing"
    );
}

/// The archive read is ONE `get`, and an unreadable object is NOT OBSERVED.
///
/// The real oracle's only `Store` touch, exercised over `Store::in_memory` —
/// the double this tree already has — so interface **I13**'s `spawn_blocking`
/// shape has something to be about while STANDING RULE 18's dial-token audit
/// stays green with no `ALLOWED` entry owed. Whether the handle can WRITE is
/// guard **G-RET** and is tested in Task 19's own file; what is tested here is
/// the read.
///
/// A PLAIN `#[test]`, DELIBERATELY. `Store::get` drives the store's own
/// current-thread runtime, so this must not run inside one — which is exactly
/// the property interface I13's `spawn_blocking` exists for on the reconcile
/// path.
#[test]
fn the_real_oracle_reads_one_object_and_an_unreadable_one_is_not_observed() {
    let doc = scorecard_json("pass", "pass", "");
    let store = Store::in_memory("logweir/");
    store
        .put_create_only(SCORECARD_KEY, doc.as_bytes())
        .expect("the fixture scorecard is written");

    let observed = observe_scorecard(&store, SCORECARD_KEY).expect("the object is readable");
    assert_eq!(observed.outcome.as_deref(), Some("pass"));
    assert_eq!(
        observed.scorecard_sha256.as_deref(),
        Some(sha256_prefixed(doc.as_bytes()).as_str())
    );
    assert_eq!(
        observe_scorecard(&store, "logweir/drills/absent.json"),
        None,
        "an unreadable object is NOT OBSERVED and never an error the reconcile returns: a run \
         whose scorecard cannot be fetched still has an exit code, and that code is the fact the \
         status exists to record"
    );
    assert_eq!(
        observe_scorecard(&store, "   "),
        None,
        "and an empty key is not looked up at all"
    );
}
