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
use http::{Request, Response};
use http_body_util::BodyExt as _;
use kube::client::Body;
use logweir_core::ids::sha256_prefixed;
use logweir_store::Store;
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tower::service_fn;
use weirkeeper::conditions::apply_merge_patch;
use weirkeeper::conditions::{
    CONDITION_REASONS, CONDITION_REASON_GUARD_REFUSED, CONDITION_REASON_OK, REASON_ADMITTED,
    REASON_APPROVAL_NOT_VERIFIED, REASON_DRILL_NOT_PASS, REASON_GUARD_REFUSED, REASON_OK,
    REASON_OPERATIONAL, REASON_SIGNING_OR_LOCK, TERMINAL_STATES,
    TERMINAL_STATE_APPROVAL_BUNDLE_CONFLICT, TERMINAL_STATE_APPROVAL_NOT_RECEIVED,
    TERMINAL_STATE_APPROVAL_SUBJECT_MISMATCH, TERMINAL_STATE_CLUSTER_NOT_REACHABLE,
    TERMINAL_STATE_CREDENTIAL_NOT_RENDERABLE, TERMINAL_STATE_GUARD_REFUSED_UNKNOWN_REASON,
    TERMINAL_STATE_JOB_NAME_CONFLICT, TERMINAL_STATE_NAME_TOO_LONG,
    TERMINAL_STATE_PLAN_CONFIG_MAP_CONFLICT, TERMINAL_STATE_PLAN_HASH_MISMATCH,
    TERMINAL_STATE_POD_OWNERSHIP_CONTESTED, TERMINAL_STATE_TARGET_TOPIC_CONFIG_REFUSED,
    TERMINAL_STATE_WINDOW_NOT_COVERED,
};
use weirkeeper::controllers::backup::SIGNING_VOLUME;
use weirkeeper::controllers::backup::{JOB_NAME_LABEL, JOB_NAME_LABEL_LEGACY};
use weirkeeper::controllers::restore::TOPIC_PREFLIGHT_KEY_PREFIX;
use weirkeeper::controllers::restore::{
    action_for, admission_hold_patch, admit, approval_bundle_config_map,
    approval_bundle_config_map_name, approval_plan_hash, approver_key_ids,
    compatible_approval_bundle, compatible_legacy_plan_config_map, compatible_restore_job,
    crashed_status_patch, finished_status_patch, observe_scorecard, plan_config_map,
    plan_config_map_name, recomputed_plan_hash, reconcile_restore, refused_status_patch,
    restore_evidence_keys, runner_argv, runner_job_spec, running_status_patch,
    scorecard_observation, topic_mapping, triggered_by, unobserved_scorecard, window_not_covered,
    Requeue, RestoreAdmission, RestoreEvidenceKeys, ScorecardObservation, ADMISSION_REQUEUE_SECS,
    ALLOWED_CLUSTERS_FILE, APPROVAL_DOC_FILE, APPROVAL_SIG_FILE, APPROVER_KEY_FILE,
    BUNDLE_APPROVAL_NAME_ANNOTATION, BUNDLE_APPROVAL_UID_ANNOTATION, BUNDLE_PLAN_HASH_ANNOTATION,
    BUNDLE_RESTORE_UID_ANNOTATION, OFFSET_REPORT_KEY_PREFIX, OFFSET_REPORT_OUT_PATH,
    OUTCOME_FAIL_COVERAGE, PLAN_SPEC_KEY, REFERENT_NOT_FOUND_REASON, SCORECARD_KEY_PREFIX,
    SCORECARD_OUT_PATH, SIDECAR_KEY_PREFIX, TARGET_PASSWORD_ENV, TARGET_PASSWORD_SECRET_KEY,
};
use weirkeeper::crds::restore::{Restore, RestoreStatus};
use weirkeeper::job::{self, APPROVAL_MOUNT_PATH, APPROVAL_VOLUME};
use weirkeeper::testing::{mock_client_recording_bodies, Route, SeenBody};
use weirkeeper::verification::{unverified_evidence, VerificationResult, VerificationVerdict};

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

/// The runner Job's own `metadata.uid`, and the ONLY thing that makes [`POD`]
/// this run's pod — D-SEAMS **S6**, defect `SEC-PODLOG`.
const JOB_UID: &str = "bbbbbbbb-0000-4000-8000-0000000000b2";

/// A pod's `ownerReferences` naming `job_uid` as its **controller**.
///
/// `kind` and `controller` are parameters because the negative cases are
/// exactly "one of these three is wrong".
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

/// The ordinary case: owned, by the controller reference, by [`JOB_UID`].
fn owned_by_job() -> String {
    pod_owner_json("Job", JOB_UID, true)
}

const UID: &str = "5c2e7b91-0000-4000-8000-0000000000a2";
const CLUSTER_UID: &str = "8b3c1d2e-0000-4000-8000-0000000000c2";
const APPROVAL: &str = "a1";
type JsonMutation = (&'static str, fn(&mut Value));

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

/// The same plan with the target's `auth` block — the shape the UI emits for a
/// `scramSha512` connection, and the one PLAT-07.1 requires to MATCH the
/// `KafkaCluster` `spec.target.clusterRef` names: the runner dials the plan's
/// address with that connection's credential, so the two must describe one
/// connection.
fn scram_plan_bytes() -> String {
    PLAN_BYTES.replace(
        "  mode: scratch\n",
        "  auth:\n    mode: scramSha512\n    username: logweir\n    tls: true\n  mode: scratch\n",
    )
}

/// The `Restore` carrying [`scram_plan_bytes`], with an `Approval` whose signed
/// bytes name that plan's hash.
fn scram_restore() -> (Restore, weirkeeper::crds::approval::Approval) {
    let plan = scram_plan_bytes();
    let hash = sha256_prefixed(plan.as_bytes());
    let restore: Restore = serde_json::from_str(&restore_json(&plan, APPROVAL, NAME))
        .expect("the fixture is a Restore");
    let approval: weirkeeper::crds::approval::Approval =
        serde_json::from_str(&approval_json(true, &hash, &hash))
            .expect("the fixture is an Approval");
    (restore, approval)
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
/// `doc_hash` is the hash INSIDE the signed bytes; `cached_hash` is what
/// `spec.planHash` claims. They are separate parameters because the whole
/// property of
/// `the_plan_hash_is_recomputed_from_the_spec_bytes_at_job_creation`'s second
/// arm is that only the first one is read.
///
/// **`spec.planHash` IS THE ONE AN AUTHOR TYPES, AND IT IS NOT SIGNED.** It is
/// a plain CRD field beside `approvalBytes`, so it can say anything at all
/// while the DOCUMENT says something else — which is exactly what the mutant
/// exploits. `Approval.status` carries no plan hash of any kind (measured
/// against the shipped schema in the second arm below), so the nearest
/// reachable form of "read it from a status" is "read it from this unsigned
/// spec field".
fn approval_json(verified: bool, doc_hash: &str, cached_hash: &str) -> String {
    let doc = serde_json::to_string(&approval_doc(doc_hash)).expect("the doc is a JSON string");
    let subject_binding = if verified {
        format!(
            r#", "verifiedSubjectRef": {{ "apiVersion": "logweir.dev/v1alpha1", "kind": "Restore", "name": "{NAME}", "namespace": "{NS}", "uid": "{UID}" }}"#
        )
    } else {
        String::new()
    };
    format!(
        r#"{{
  "apiVersion": "logweir.dev/v1alpha1",
  "kind": "Approval",
  "metadata": {{ "name": "{APPROVAL}", "namespace": "{NS}", "uid": "aaaaaaaa-0000-4000-8000-00000000000a" }},
  "spec": {{
    "subjectRef": {{ "kind": "Restore", "name": "{NAME}" }},
    "planHash": "{cached_hash}",
    "approvalBytes": {doc},
    "sidecarBytes": "{{}}"
  }},
  "status": {{
    "verified": {verified},
    "matchedKeyId": "{KEY_ID_LIVE}"{subject_binding},
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

/// The trust a ROSTER-ONLY cluster resolves to.
///
/// PLAT-19.1 moved the approval bundle from `TrustRoster/default` to the
/// namespace's resolved trust (D3 §7.1), and `synthesize_legacy` is exactly
/// what `reconcile_restore` reaches for a cluster with no `TrustPolicy`. Every
/// bundle assertion below predates that and describes such a cluster, so
/// routing them through the synthesis is what makes them evidence that the
/// legacy path still renders the same bytes.
fn legacy_trust() -> weirkeeper::trust::ResolvedTrust {
    weirkeeper::trust::synthesize_legacy(&roster().spec)
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
  "metadata":{{"name":"{NAME}","namespace":"{NS}","uid":"{JOB_UID}",
    "ownerReferences":[{{"apiVersion":"logweir.dev/v1alpha1","kind":"Restore","name":"{NAME}","uid":"{UID}","controller":true,"blockOwnerDeletion":true}}]}},
  "spec":{{"template":{{"spec":{{"containers":[],"restartPolicy":"Never"}}}}}},
  "status":{{"active":1}}}}"#
    )
}

/// A finished Job, `Complete` or `Failed`.
fn job_body(condition: &str) -> String {
    format!(
        r#"{{"apiVersion":"batch/v1","kind":"Job",
  "metadata":{{"name":"{NAME}","namespace":"{NS}","uid":"{JOB_UID}",
    "ownerReferences":[{{"apiVersion":"logweir.dev/v1alpha1","kind":"Restore","name":"{NAME}","uid":"{UID}","controller":true,"blockOwnerDeletion":true}}]}},
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
    pod_list_terminated_owned_by(exit_code, &owned_by_job())
}

/// [`pod_list_terminated`] with the pod's `ownerReferences` as a parameter.
///
/// THE OWNER IS A PARAMETER BECAUSE IT IS THE SECURITY BOUNDARY (D-SEAMS
/// **S6**): `"null"` gives an ownerless pod, `pod_owner_json(…)` gives a wrong
/// one, and both must end where an empty list ends — no log read, no exit code.
fn pod_list_terminated_owned_by(exit_code: i32, owners: &str) -> String {
    format!(
        r#"{{"apiVersion":"v1","kind":"PodList","metadata":{{}},"items":[
  {{"apiVersion":"v1","kind":"Pod",
    "metadata":{{"name":"{POD}","namespace":"{NS}","ownerReferences":{owners},
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
        // PLAT-19.1: materialization resolves the NAMESPACE's trust (D3 §7.1),
        // so the policy list is read before the roster it falls back to. An
        // empty list is a cluster that has not migrated, which is what every
        // fixture below describes.
        Route {
            method: "GET",
            path_suffix: "/trustpolicies",
            status: 200,
            body: r#"{"apiVersion":"logweir.dev/v1alpha1","kind":"TrustPolicyList",
                      "metadata":{"resourceVersion":"1"},"items":[]}"#
                .to_string(),
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
            method: "GET",
            path_suffix: "/configmaps/logweir-restore-incident-4471-approval-bundle",
            status: 200,
            body: existing_approval_bundle(UID),
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
    let mut plan = plan_config_map(&restore()).expect("the fixture materializes a plan");
    plan.metadata.owner_references.as_mut().unwrap()[0].uid = owner_uid.to_string();
    serde_json::to_string(&plan).expect("the plan ConfigMap serializes")
}

fn existing_approval_bundle(owner_uid: &str) -> String {
    let mut bundle =
        approval_bundle_config_map(&restore(), &approval(true), &legacy_trust(), now())
            .expect("the fixture materializes an approval bundle");
    bundle.metadata.owner_references.as_mut().unwrap()[0].uid = owner_uid.to_string();
    serde_json::to_string(&bundle).expect("the approval bundle serializes")
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
    let outcome = reconcile_restore(
        &restore(),
        &client,
        &unobserved_scorecard,
        &unverified_evidence,
        now(),
    )
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
    let outcome = reconcile_restore(
        &restore(),
        &client,
        &unobserved_scorecard,
        &unverified_evidence,
        now(),
    )
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
    let outcome2 = reconcile_restore(
        &restore(),
        &client2,
        &unobserved_scorecard,
        &unverified_evidence,
        now(),
    )
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
        &unverified_evidence,
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
    let outcome = reconcile_restore(
        &restore(),
        &client,
        &unobserved_scorecard,
        &unverified_evidence,
        now(),
    )
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

    // ---- arm 2: the UNSIGNED CACHED FIELD carries the CORRECT hash, and it
    //             changes nothing.
    //
    // `Approval.status` carries no plan hash at all — asserted below — so the
    // nearest reachable mutant is "read `spec.planHash`", the plain CRD field
    // an author types beside the signed bytes. It can say anything while the
    // document says something else, which is the whole reason check 7 on the
    // Approval side and `admit` on this side both RECOMPUTE.
    let shipped = workspace_yaml("config/crd/approvals.yaml");
    let status_props = &shipped["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]
        ["status"]["properties"];
    assert!(
        status_props.get("planHash").is_none(),
        "Approval.status declares NO plan hash, so there is nothing on a status to read; the \
         mutant this arm kills reads the unsigned spec.planHash instead"
    );
    let (client2, _rec2, bodies2) = mock_client_recording_bodies(admission_routes(
        200,
        // the document says the wrong thing; the unsigned cached field says
        // the right thing
        approval_json(true, wrong, &plan_hash()),
        200,
        cluster_json(true, PLAINTEXT_AUTH),
    ));
    let outcome2 = reconcile_restore(
        &restore(),
        &client2,
        &unobserved_scorecard,
        &unverified_evidence,
        now(),
    )
    .await
    .expect("a terminal refusal is an outcome");
    let seen2 = bodies2
        .lock()
        .expect("the body recorder is readable")
        .clone();
    assert_eq!(
        post_count(&seen2, "/jobs"),
        0,
        "spec.planHash is a plain field an author typed and is not part of anything anyone \
         signed, so it cannot rescue an approval that binds different bytes. A reconciler that \
         read it instead of parsing spec.approvalBytes would have created a Job here"
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

#[test]
fn admission_binds_the_complete_verified_subject_identity() {
    let restore = restore();
    let cluster = cluster(true);
    assert_eq!(
        admit(&restore, Some(&approval(true)), Some(&cluster)),
        RestoreAdmission::Ok
    );

    let cases: [JsonMutation; 5] = [
        ("another Restore with the same plan", |value: &mut Value| {
            value["spec"]["subjectRef"]["name"] = serde_json::json!("another-restore");
        }),
        ("a Backup/Drill kind substitution", |value: &mut Value| {
            value["spec"]["subjectRef"]["kind"] = serde_json::json!("Backup");
        }),
        ("another namespace", |value: &mut Value| {
            value["metadata"]["namespace"] = serde_json::json!("other-namespace");
        }),
        ("a recreated Restore UID", |value: &mut Value| {
            value["status"]["verifiedSubjectRef"]["uid"] = serde_json::json!("previous-uid");
        }),
        (
            "a recreated UID after verification was revoked",
            |value: &mut Value| {
                value["status"]["verified"] = serde_json::json!(false);
                value["status"]["verifiedSubjectRef"]["uid"] = serde_json::json!("previous-uid");
            },
        ),
    ];
    for (label, mutate) in cases {
        let mut value: Value =
            serde_json::from_str(&approval_json(true, &plan_hash(), &plan_hash())).unwrap();
        mutate(&mut value);
        let approval = serde_json::from_value(value).unwrap();
        let verdict = admit(&restore, Some(&approval), Some(&cluster));
        assert!(
            matches!(verdict, RestoreAdmission::ApprovalSubjectMismatch { .. }),
            "{label} must not replay an Approval: {verdict:?}"
        );
        assert_eq!(verdict.reason(), TERMINAL_STATE_APPROVAL_SUBJECT_MISMATCH);
    }
}

#[test]
fn a_legacy_verified_status_waits_for_subject_provenance_refresh() {
    let mut value: Value =
        serde_json::from_str(&approval_json(true, &plan_hash(), &plan_hash())).unwrap();
    value["status"]
        .as_object_mut()
        .unwrap()
        .remove("verifiedSubjectRef");
    let approval = serde_json::from_value(value).unwrap();
    assert_eq!(
        admit(&restore(), Some(&approval), Some(&cluster(true))),
        RestoreAdmission::ApprovalNotVerified {
            approval: APPROVAL.to_string()
        },
        "upgrade is fail-closed but retryable until the Approval controller records the UID"
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
        let outcome = reconcile_restore(
            &restore(),
            &client,
            &unobserved_scorecard,
            &unverified_evidence,
            now(),
        )
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
    let outcome = reconcile_restore(
        &object,
        &client,
        &unobserved_scorecard,
        &unverified_evidence,
        now(),
    )
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
    let outcome = reconcile_restore(
        &restore(),
        &client,
        &unobserved_scorecard,
        &unverified_evidence,
        now(),
    )
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
    reconcile_restore(
        &restore(),
        &client,
        &unobserved_scorecard,
        &unverified_evidence,
        now(),
    )
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
        "the plan remains a separate one-key ConfigMap; approval artifacts live in the immutable \
         per-Restore bundle"
    );
    assert_eq!(
        cm["metadata"]["name"].as_str(),
        Some(plan_config_map_name(NAME).as_str())
    );
    assert_eq!(cm["immutable"].as_bool(), Some(true));
    assert_eq!(
        cm["metadata"]["annotations"][BUNDLE_RESTORE_UID_ANNOTATION].as_str(),
        Some(UID)
    );
    assert_eq!(
        cm["metadata"]["annotations"][BUNDLE_PLAN_HASH_ANNOTATION].as_str(),
        Some(plan_hash().as_str())
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
    let created_maps: Vec<Value> = seen
        .iter()
        .filter(|body| body.method == "POST" && path(&body.uri).ends_with("/configmaps"))
        .map(|body| serde_json::from_str(&body.body).expect("a ConfigMap POST body is JSON"))
        .collect();
    assert_eq!(
        created_maps.len(),
        2,
        "plan plus per-Restore approval bundle"
    );
    assert_eq!(
        created_maps[1]["metadata"]["name"].as_str(),
        Some(approval_bundle_config_map_name(NAME).as_str())
    );
    assert_eq!(created_maps[1]["immutable"].as_bool(), Some(true));
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

#[test]
fn the_approval_bundle_is_immutable_exact_and_bound_to_the_restore() {
    let restore = restore();
    let approval = approval(true);
    let bundle = approval_bundle_config_map(&restore, &approval, &legacy_trust(), now())
        .expect("verified inputs materialize");

    assert_eq!(bundle.immutable, Some(true));
    assert_eq!(
        bundle.metadata.name.as_deref(),
        Some(approval_bundle_config_map_name(NAME).as_str())
    );
    let owner = &bundle.metadata.owner_references.as_ref().unwrap()[0];
    assert_eq!(owner.uid, UID);
    assert_eq!(owner.controller, Some(true));
    assert_eq!(owner.block_owner_deletion, Some(true));

    let annotations = bundle.metadata.annotations.as_ref().unwrap();
    assert_eq!(
        annotations
            .get(BUNDLE_RESTORE_UID_ANNOTATION)
            .map(String::as_str),
        Some(UID)
    );
    assert_eq!(
        annotations
            .get(BUNDLE_PLAN_HASH_ANNOTATION)
            .map(String::as_str),
        Some(plan_hash().as_str())
    );
    assert_eq!(
        annotations
            .get(BUNDLE_APPROVAL_NAME_ANNOTATION)
            .map(String::as_str),
        Some(APPROVAL)
    );
    assert_eq!(
        annotations
            .get(BUNDLE_APPROVAL_UID_ANNOTATION)
            .map(String::as_str),
        approval.metadata.uid.as_deref()
    );

    let data = bundle.data.as_ref().unwrap();
    assert_eq!(data.len(), 4);
    assert_eq!(
        data.get(APPROVAL_DOC_FILE).map(String::as_str),
        Some(approval.spec.approval_bytes.as_str()),
        "signed approval bytes are copied verbatim"
    );
    assert_eq!(
        data.get(APPROVAL_SIG_FILE).map(String::as_str),
        Some(approval.spec.sidecar_bytes.as_str()),
        "detached sidecar bytes are copied verbatim"
    );
    assert_eq!(
        data.get(APPROVER_KEY_FILE).map(String::as_str),
        Some("-----BEGIN PUBLIC KEY-----\nA\n-----END PUBLIC KEY-----\n")
    );
    let allowed: logweir_core::spec::AllowedClusters =
        serde_json::from_str(&data[ALLOWED_CLUSTERS_FILE]).expect("runner allowlist grammar");
    assert_eq!(allowed.allowed_cluster_ids, vec!["MkU3OEVBNTcwNTJENDM2Qk"]);
    assert!(
        data.values().all(|value| !value.contains("PRIVATE KEY")),
        "private signing material is never a public bundle member"
    );
}

#[test]
fn an_existing_bundle_must_match_owner_bindings_immutability_and_every_byte() {
    let desired =
        approval_bundle_config_map(&restore(), &approval(true), &legacy_trust(), now()).unwrap();
    assert!(compatible_approval_bundle(&desired, &desired, UID));

    let mut changed = desired.clone();
    changed.data.as_mut().unwrap().insert(
        ALLOWED_CLUSTERS_FILE.to_string(),
        r#"{"allowed_cluster_ids":["substitute"]}"#.to_string(),
    );
    assert!(!compatible_approval_bundle(&changed, &desired, UID));

    let mut changed = desired.clone();
    changed.immutable = Some(false);
    assert!(!compatible_approval_bundle(&changed, &desired, UID));

    let mut changed = desired.clone();
    changed.metadata.annotations.as_mut().unwrap().insert(
        BUNDLE_PLAN_HASH_ANNOTATION.to_string(),
        "sha256:other".to_string(),
    );
    assert!(!compatible_approval_bundle(&changed, &desired, UID));

    let mut changed = desired.clone();
    changed.metadata.owner_references.as_mut().unwrap()[0].uid = "other-uid".to_string();
    assert!(!compatible_approval_bundle(&changed, &desired, UID));

    let mut extra_owner = desired.clone();
    let mut secondary = extra_owner.metadata.owner_references.as_ref().unwrap()[0].clone();
    secondary.kind = "Backup".to_string();
    secondary.name = "secondary-owner".to_string();
    secondary.uid = "secondary-owner-uid".to_string();
    secondary.controller = Some(false);
    extra_owner
        .metadata
        .owner_references
        .as_mut()
        .unwrap()
        .push(secondary);
    assert!(
        !compatible_approval_bundle(&extra_owner, &desired, UID),
        "the expected owner plus a second owner is not the expected single-owner set"
    );

    for mutate in [
        |owner: &mut k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference| {
            owner.api_version = "v1".to_string();
        },
        |owner: &mut k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference| {
            owner.kind = "Backup".to_string();
        },
        |owner: &mut k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference| {
            owner.name = "other".to_string();
        },
        |owner: &mut k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference| {
            owner.controller = Some(false);
        },
        |owner: &mut k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference| {
            owner.block_owner_deletion = Some(false);
        },
    ] {
        let mut changed = desired.clone();
        mutate(&mut changed.metadata.owner_references.as_mut().unwrap()[0]);
        assert!(
            !compatible_approval_bundle(&changed, &desired, UID),
            "every ownerReference field is load-bearing"
        );
    }

    let mut admission_mutated = desired.clone();
    admission_mutated
        .metadata
        .annotations
        .as_mut()
        .unwrap()
        .insert("admission.example/injected".to_string(), "true".to_string());
    assert!(compatible_approval_bundle(
        &admission_mutated,
        &desired,
        UID
    ));
}

#[test]
fn a_pre_job_legacy_plan_is_accepted_only_with_exact_bytes_and_complete_owner() {
    let desired = plan_config_map(&restore()).unwrap();
    let mut legacy = desired.clone();
    legacy.immutable = None;
    legacy.metadata.annotations = None;
    assert!(compatible_legacy_plan_config_map(&legacy, &desired, UID));

    legacy
        .data
        .as_mut()
        .unwrap()
        .insert(PLAN_SPEC_KEY.to_string(), "substituted plan".to_string());
    assert!(!compatible_legacy_plan_config_map(&legacy, &desired, UID));

    let mut legacy = desired.clone();
    legacy.immutable = None;
    legacy.metadata.annotations = None;
    legacy.metadata.owner_references.as_mut().unwrap()[0].block_owner_deletion = Some(false);
    assert!(!compatible_legacy_plan_config_map(&legacy, &desired, UID));
}

#[test]
fn distinct_restores_get_distinct_bundle_names_and_bindings() {
    let first_restore = restore();
    let first = approval_bundle_config_map(&first_restore, &approval(true), &legacy_trust(), now())
        .unwrap();

    let mut second_restore = restore();
    second_restore.metadata.name = Some("logweir-restore-incident-4472".to_string());
    second_restore.metadata.uid = Some("bbbbbbbb-0000-4000-8000-000000000002".to_string());
    let mut second_approval = approval(true);
    second_approval.metadata.name = Some("a2".to_string());
    second_approval.metadata.uid = Some("aaaaaaaa-0000-4000-8000-00000000000b".to_string());
    let second =
        approval_bundle_config_map(&second_restore, &second_approval, &legacy_trust(), now())
            .unwrap();

    assert_ne!(first.metadata.name, second.metadata.name);
    assert_ne!(
        first.metadata.annotations.as_ref().unwrap()[BUNDLE_RESTORE_UID_ANNOTATION],
        second.metadata.annotations.as_ref().unwrap()[BUNDLE_RESTORE_UID_ANNOTATION]
    );
    assert_ne!(
        first.metadata.annotations.as_ref().unwrap()[BUNDLE_APPROVAL_UID_ANNOTATION],
        second.metadata.annotations.as_ref().unwrap()[BUNDLE_APPROVAL_UID_ANNOTATION]
    );
}

/// A restart or concurrent reconcile may observe both create-only ConfigMaps
/// already present. Exact owned bytes are idempotent; a foreign plan is not.
#[tokio::test]
async fn config_map_retries_are_idempotent_only_for_exact_owned_artifacts() {
    for (label, owner_uid, append_owner, expect_job) in [
        ("ours", UID, false, true),
        (
            "a stranger's",
            "ffffffff-0000-4000-8000-0000000000ff",
            false,
            false,
        ),
        ("ours plus a secondary owner", UID, true, false),
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
            if route.method == "GET" && route.path_suffix.ends_with("-plan") {
                let mut plan: Value =
                    serde_json::from_str(&existing_plan_config_map(owner_uid)).unwrap();
                if append_owner {
                    plan["metadata"]["ownerReferences"]
                        .as_array_mut()
                        .unwrap()
                        .push(serde_json::json!({
                            "apiVersion": "logweir.dev/v1alpha1",
                            "kind": "Backup",
                            "name": "secondary-owner",
                            "uid": "secondary-owner-uid",
                            "controller": false,
                            "blockOwnerDeletion": true
                        }));
                }
                route.body = serde_json::to_string(&plan).unwrap();
            }
            if route.method == "GET" && route.path_suffix.ends_with("-approval-bundle") {
                route.body = existing_approval_bundle(owner_uid);
            }
        }
        let (client, _rec, bodies) = mock_client_recording_bodies(routes);
        let outcome = reconcile_restore(
            &restore(),
            &client,
            &unobserved_scorecard,
            &unverified_evidence,
            now(),
        )
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

#[tokio::test]
async fn a_bundle_materialization_failure_is_visible_and_starts_no_job() {
    let mut routes = admission_routes(
        200,
        approval_json(true, &plan_hash(), &plan_hash()),
        200,
        cluster_json(true, PLAINTEXT_AUTH),
    );
    let mut missing_key: Value = serde_json::from_str(&roster_json()).unwrap();
    missing_key["spec"]["approverKeys"] = serde_json::json!([]);
    for route in &mut routes {
        if route.method == "GET" && route.path_suffix == "/trustrosters/default" {
            route.body = serde_json::to_string(&missing_key).unwrap();
        }
    }
    let (client, _rec, bodies) = mock_client_recording_bodies(routes);
    let outcome = reconcile_restore(
        &restore(),
        &client,
        &unobserved_scorecard,
        &unverified_evidence,
        now(),
    )
    .await
    .expect("materialization failure is an observable outcome, not a controller error");

    assert!(!outcome.created);
    assert_eq!(outcome.requeue, Requeue::After(ADMISSION_REQUEUE_SECS));
    let seen = bodies.lock().unwrap().clone();
    assert_eq!(post_count(&seen, "/jobs"), 0);
    let statuses = patched_statuses(&seen);
    assert_eq!(statuses.len(), 1);
    assert_eq!(
        statuses[0]["reason"].as_str(),
        Some("ApprovalBundleMaterializationFailed")
    );
    assert_eq!(statuses[0]["phase"].as_str(), Some("Pending"));
    // PLAT-19.1: the message names the object an operator would actually EDIT.
    // This fixture is a roster-only cluster, so that is still the roster — but
    // the sentence is now rendered from the RESOLVED source, which is the whole
    // point of review finding F1: a namespace governed by a `TrustPolicy` is
    // told to edit the policy, not an object D3 §7.2 says no longer decides
    // anything for it.
    let message = statuses[0]["conditions"][0]["message"].as_str().unwrap();
    assert!(
        message.contains("the TrustRoster 'default' does not carry"),
        "the refusal names the resolved trust source; got {message}"
    );
    assert!(
        message.contains(KEY_ID_LIVE),
        "…and the key it could not find: {message}"
    );
}

#[tokio::test]
async fn an_owned_name_collision_cannot_substitute_bundle_content() {
    let mut routes = admission_routes(
        200,
        approval_json(true, &plan_hash(), &plan_hash()),
        200,
        cluster_json(true, PLAINTEXT_AUTH),
    );
    let mut substitute: Value = serde_json::from_str(&existing_approval_bundle(UID)).unwrap();
    substitute["data"][ALLOWED_CLUSTERS_FILE] =
        serde_json::json!(r#"{"allowed_cluster_ids":["substitute"]}"#);
    for route in &mut routes {
        if route.method == "POST" && route.path_suffix == "/configmaps" {
            route.status = 409;
            route.body = r#"{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"AlreadyExists","code":409}"#.to_string();
        }
        if route.method == "GET" && route.path_suffix.ends_with("-approval-bundle") {
            route.body = serde_json::to_string(&substitute).unwrap();
        }
    }
    let (client, _rec, bodies) = mock_client_recording_bodies(routes);
    let outcome = reconcile_restore(
        &restore(),
        &client,
        &unobserved_scorecard,
        &unverified_evidence,
        now(),
    )
    .await
    .expect("a content collision is a terminal controller verdict");

    assert_eq!(
        outcome.terminal_state.as_deref(),
        Some(TERMINAL_STATE_APPROVAL_BUNDLE_CONFLICT)
    );
    let seen = bodies.lock().unwrap().clone();
    assert_eq!(post_count(&seen, "/jobs"), 0);
    let statuses = patched_statuses(&seen);
    assert_eq!(
        statuses[0]["reason"].as_str(),
        Some(TERMINAL_STATE_APPROVAL_BUNDLE_CONFLICT)
    );
}

#[tokio::test]
async fn a_plan_create_deletion_race_is_retryable_not_terminal() {
    let mut routes = admission_routes(
        200,
        approval_json(true, &plan_hash(), &plan_hash()),
        200,
        cluster_json(true, PLAINTEXT_AUTH),
    );
    for route in &mut routes {
        if route.method == "POST" && route.path_suffix == "/configmaps" {
            route.status = 409;
        }
        if route.method == "GET" && route.path_suffix.ends_with("-plan") {
            route.status = 404;
            route.body = not_found_body("configmaps", "plan");
        }
    }
    let (client, _recorder, bodies) = mock_client_recording_bodies(routes);
    let outcome = reconcile_restore(
        &restore(),
        &client,
        &unobserved_scorecard,
        &unverified_evidence,
        now(),
    )
    .await
    .expect("a deletion race is reported and retried");
    assert_eq!(outcome.requeue, Requeue::After(ADMISSION_REQUEUE_SECS));
    assert!(outcome.terminal_state.is_none());
    assert_eq!(post_count(&bodies.lock().unwrap(), "/jobs"), 0);
    let seen = bodies.lock().unwrap();
    let statuses = patched_statuses(&seen);
    let message = statuses[0]["conditions"][0]["message"]
        .as_str()
        .expect("the retry status carries an actionable message");
    assert!(
        message.contains("plan ConfigMap") && message.contains("deleted concurrently"),
        "the operator must see which input raced and why it is retryable: {message}"
    );
}

#[tokio::test]
async fn upgrade_after_legacy_plan_creation_safely_creates_a_pinned_job() {
    let mut routes = admission_routes(
        200,
        approval_json(true, &plan_hash(), &plan_hash()),
        200,
        cluster_json(true, PLAINTEXT_AUTH),
    );
    let desired = plan_config_map(&restore()).unwrap();
    let mut legacy = desired.clone();
    legacy.immutable = None;
    legacy.metadata.annotations = None;
    for route in &mut routes {
        if route.method == "POST" && route.path_suffix == "/configmaps" {
            route.status = 409;
        }
        if route.method == "GET" && route.path_suffix.ends_with("-plan") {
            route.body = serde_json::to_string(&legacy).unwrap();
        }
    }
    let (client, _recorder, bodies) = mock_client_recording_bodies(routes);
    let outcome = reconcile_restore(
        &restore(),
        &client,
        &unobserved_scorecard,
        &unverified_evidence,
        now(),
    )
    .await
    .expect("the exact owned legacy plan is a supported crash transition");
    assert!(outcome.created);
    let job = posted_job(&bodies.lock().unwrap());
    let env = job["spec"]["template"]["spec"]["containers"][0]["env"]
        .as_array()
        .unwrap();
    assert!(env.iter().any(|entry| {
        entry["name"] == logweir_core::execution_contract::PLAN_SHA256_ENV
            && entry["value"] == plan_hash()
    }));
}

#[tokio::test]
async fn a_concurrent_job_create_is_accepted_only_after_owner_uid_revalidation() {
    let job_gets = Arc::new(Mutex::new(0usize));
    let job_gets_for_service = Arc::clone(&job_gets);
    let service = service_fn(move |request: Request<Body>| {
        let job_gets = Arc::clone(&job_gets_for_service);
        async move {
            let method = request.method().as_str().to_string();
            let path = request.uri().path().to_string();
            let _ = request.into_body().collect().await;
            let (status, body) = if method == "GET" && path.ends_with(&format!("/jobs/{NAME}")) {
                let mut count = job_gets.lock().unwrap();
                *count += 1;
                if *count == 1 {
                    (404, not_found_body("jobs.batch", NAME))
                } else {
                    (200, running_job_body())
                }
            } else if method == "GET" && path.ends_with("/approvals/a1") {
                (200, approval_json(true, &plan_hash(), &plan_hash()))
            } else if method == "GET" && path.ends_with("/kafkaclusters/scratch") {
                (200, cluster_json(true, PLAINTEXT_AUTH))
            } else if method == "GET" && path.ends_with("/trustpolicies") {
                (
                    200,
                    r#"{"apiVersion":"logweir.dev/v1alpha1","kind":"TrustPolicyList",
                        "metadata":{"resourceVersion":"1"},"items":[]}"#
                        .to_string(),
                )
            } else if method == "GET" && path.ends_with("/trustrosters/default") {
                (200, roster_json())
            } else if method == "POST" && path.ends_with("/configmaps") {
                (201, existing_plan_config_map(UID))
            } else if method == "POST" && path.ends_with("/jobs") {
                (
                    409,
                    r#"{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"AlreadyExists","code":409}"#.to_string(),
                )
            } else if method == "PATCH" && path.ends_with(&format!("/restores/{NAME}/status")) {
                (200, restore_json(PLAN_BYTES, APPROVAL, NAME))
            } else {
                panic!("unexpected request in concurrent-create test: {method} {path}")
            };
            Ok::<_, std::convert::Infallible>(
                Response::builder()
                    .status(status)
                    .body(Body::from(body.into_bytes()))
                    .unwrap(),
            )
        }
    });
    let client = kube::Client::new(service, "default");
    let outcome = reconcile_restore(
        &restore(),
        &client,
        &unobserved_scorecard,
        &unverified_evidence,
        now(),
    )
    .await
    .expect("the losing reconcile revalidates the winning Job");
    assert!(!outcome.created);
    assert_eq!(outcome.requeue, Requeue::After(15));
    assert_eq!(*job_gets.lock().unwrap(), 2);
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
        reconcile_restore(
            &object,
            &client,
            &unobserved_scorecard,
            &unverified_evidence,
            now(),
        )
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
    let contract_digests: Vec<(String, String)> = jobs
        .iter()
        .map(|job| {
            let env = job["spec"]["template"]["spec"]["containers"][0]["env"]
                .as_array()
                .expect("runner env");
            let value = |name: &str| {
                env.iter()
                    .find(|entry| entry["name"] == name)
                    .and_then(|entry| entry["value"].as_str())
                    .expect("contract digest")
                    .to_string()
            };
            (
                value(logweir_core::execution_contract::PLAN_SHA256_ENV),
                value(logweir_core::execution_contract::APPROVAL_SHA256_ENV),
            )
        })
        .collect();
    assert_ne!(contract_digests[0], contract_digests[1]);
    for job in &mut jobs {
        job["spec"]["template"]["spec"]["containers"][0]["env"]
            .as_array_mut()
            .unwrap()
            .retain(|entry| {
                !matches!(
                    entry["name"].as_str(),
                    Some(
                        logweir_core::execution_contract::PLAN_SHA256_ENV
                            | logweir_core::execution_contract::APPROVAL_SHA256_ENV
                    )
                )
            });
    }
    assert_eq!(
        jobs[0], jobs[1],
        "…and the Jobs are identical except for the mandatory exact plan/approval digests. \
         `spec.target.mode` adds a marker topic, an allowlisted \
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
            logweir_core::execution_contract::VERSION_ARG.to_string(),
            logweir_core::execution_contract::VERSION.to_string(),
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

/// The Job's mounts: the plan ConfigMap, the immutable approval ConfigMap and
/// the private signing-key Secret, each at its own path.
#[test]
fn the_restore_job_mounts_the_plan_the_approval_bundle_and_the_signing_key() {
    let spec = runner_job_spec(
        &restore(),
        &cluster(true),
        &[KEY_ID_LIVE.to_string()],
        &approval(true),
        &legacy_trust(),
        now(),
    )
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
        vec![SIGNING_VOLUME, APPROVAL_VOLUME, "plan", "work"],
        "the private Secret, public approval ConfigMap, plan ConfigMap and writable scratch volume"
    );

    let approval_volume = pod
        .volumes
        .as_deref()
        .unwrap_or_default()
        .iter()
        .find(|v| v.name == APPROVAL_VOLUME)
        .expect("the approval bundle is projected");
    assert!(
        approval_volume.secret.is_none(),
        "the legacy namespace-wide Secret is not mounted by a new Job"
    );
    let config_map = approval_volume
        .config_map
        .as_ref()
        .expect("the approval bundle is a public ConfigMap");
    assert_eq!(config_map.name, approval_bundle_config_map_name(NAME));
    assert_eq!(config_map.optional, Some(false));
    let items: Vec<&str> = config_map
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

    let env: std::collections::BTreeMap<&str, &str> = pod.containers[0]
        .env
        .as_deref()
        .unwrap_or_default()
        .iter()
        .filter_map(|entry| Some((entry.name.as_str(), entry.value.as_deref()?)))
        .collect();
    assert_eq!(
        env[logweir_core::execution_contract::VERSION_ENV],
        logweir_core::execution_contract::VERSION
    );
    assert_eq!(env[logweir_core::execution_contract::SUBJECT_UID_ENV], UID);
    assert_eq!(
        env[logweir_core::execution_contract::APPROVAL_UID_ENV],
        "aaaaaaaa-0000-4000-8000-00000000000a"
    );
    assert_eq!(
        env[logweir_core::execution_contract::PLAN_SHA256_ENV],
        plan_hash()
    );
    let bundle =
        approval_bundle_config_map(&restore(), &approval(true), &legacy_trust(), now()).unwrap();
    let data = bundle.data.unwrap();
    assert_eq!(
        env[logweir_core::execution_contract::ALLOWED_CLUSTERS_SHA256_ENV],
        sha256_prefixed(data[ALLOWED_CLUSTERS_FILE].as_bytes())
    );
}

/// A `scramSha512` target's password reaches the runner as `secretKeyRef`
/// only — interface **I11**, and the controller reads nothing.
#[test]
fn a_scram_target_password_is_projected_by_reference_and_never_read() {
    let scram: weirkeeper::crds::kafka_cluster::KafkaCluster =
        serde_json::from_str(&cluster_json(true, SCRAM_AUTH)).expect("the fixture is a cluster");
    let (scram_restore, scram_approval) = scram_restore();
    let spec = runner_job_spec(
        &scram_restore,
        &scram,
        &[],
        &scram_approval,
        &legacy_trust(),
        now(),
    )
    .expect("the job spec builds");
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
    let plain = runner_job_spec(
        &restore(),
        &cluster(true),
        &[],
        &approval(true),
        &legacy_trust(),
        now(),
    )
    .expect("the job spec builds");
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
    let refusal = runner_job_spec(
        &scram_restore,
        &no_secret,
        &[],
        &scram_approval,
        &legacy_trust(),
        now(),
    )
    .expect_err("no reference, no Job (PLAT-07.1)");
    assert!(
        refusal
            .to_string()
            .contains(TERMINAL_STATE_CREDENTIAL_NOT_RENDERABLE),
        "the controller now refuses this before any ConfigMap or Job exists, keeping the state \
         the runner reports for the same object: {refusal}"
    );
    assert!(
        TERMINAL_STATES.contains(&TERMINAL_STATE_CREDENTIAL_NOT_RENDERABLE),
        "the state the RUNNER refuses with is on the shared list, ready to be mapped"
    );
}

/// An approved plan that names a different target than the saved connection is
/// refused before any ConfigMap or Job exists — PLAT-07.1.
///
/// The runner dials the PLAN's address and authenticates with the CONNECTION's
/// credential and CA, so a mismatch sends a saved credential somewhere the
/// saved connection does not name, or dials without the TLS it requires.
///
/// KILLS: drop `check_restore_plan` from `runner_job_spec`.
#[test]
fn a_plan_naming_another_target_than_the_saved_connection_is_refused() {
    let scram: weirkeeper::crds::kafka_cluster::KafkaCluster =
        serde_json::from_str(&cluster_json(true, SCRAM_AUTH)).expect("the fixture is a cluster");
    // The plan says plaintext; the connection says SCRAM over TLS.
    let refusal = runner_job_spec(
        &restore(),
        &scram,
        &[],
        &approval(true),
        &legacy_trust(),
        now(),
    )
    .expect_err("a plaintext plan may not spend a SCRAM connection's credential");
    assert!(
        refusal
            .to_string()
            .contains(weirkeeper::conditions::TERMINAL_STATE_CONNECTION_PLAN_MISMATCH),
        "got {refusal}"
    );

    // The plan names another address; everything else agrees.
    let (mut elsewhere, elsewhere_approval) = scram_restore();
    elsewhere.spec.plan_bytes =
        scram_plan_bytes().replace("scratch-0.logweir-t20:9092", "elsewhere.example:9092");
    let refusal = runner_job_spec(
        &elsewhere,
        &scram,
        &[],
        &elsewhere_approval,
        &legacy_trust(),
        now(),
    )
    .expect_err("a plan may not point the connection's credential elsewhere");
    let text = refusal.to_string();
    assert!(
        text.contains(weirkeeper::conditions::TERMINAL_STATE_CONNECTION_PLAN_MISMATCH)
            && text.contains("elsewhere.example:9092"),
        "the refusal names the address the plan asked for: {text}"
    );

    // And a plan built from the saved connection builds a Job.
    let (matching, matching_approval) = scram_restore();
    runner_job_spec(
        &matching,
        &scram,
        &[],
        &matching_approval,
        &legacy_trust(),
        now(),
    )
    .expect("a plan built from the saved connection builds a Job");
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
    reconcile_restore(
        &restore(),
        &client,
        &unobserved_scorecard,
        &unverified_evidence,
        now(),
    )
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
    reconcile_restore(
        &restore(),
        &client2,
        &unobserved_scorecard,
        &unverified_evidence,
        now(),
    )
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
    reconcile_restore(
        &restore(),
        &client,
        &unobserved_scorecard,
        &unverified_evidence,
        now(),
    )
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
    reconcile_restore(
        &restore(),
        &client2,
        &unobserved_scorecard,
        &unverified_evidence,
        now(),
    )
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
    reconcile_restore(
        &restore(),
        &client3,
        &unobserved_scorecard,
        &unverified_evidence,
        now(),
    )
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
///
/// # ROW 3 IS THE TIE, AND IT WAS ADDED BECAUSE THE MUTANT SURVIVED WITHOUT IT
///
/// Measured: a mutant that answered *"the first member of
/// `conditions::TERMINAL_STATES` the log body mentions anywhere"* instead of
/// reading the `refusal-reason=` line passed this table at 32 / 0. Every row
/// mentioned exactly ONE terminal state, so "the state on the refusal line"
/// and "the first state the body mentions" could not disagree — the table was
/// a table of one case written three ways. Row 3 makes them disagree: the
/// prose mentions `TargetTopicConfigRefused` and the refusal line names
/// `CredentialNotRenderable`, so a reader that scans for state NAMES rather
/// than for the KEY answers the wrong one. Row 4 does the same to a
/// first-line-wins reader with two refusal lines.
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
            // THE TIE. Two terminal states appear in the body; only one is on
            // the refusal line, and it is not the one a name-scan finds first.
            "two states named, one on the refusal line",
            format!(
                "guard: the target topic config was fine, so \
                 {TERMINAL_STATE_TARGET_TOPIC_CONFIG_REFUSED} was not the problem\n\
                 refusal-reason={TERMINAL_STATE_CREDENTIAL_NOT_RENDERABLE}\n"
            ),
            TERMINAL_STATE_CREDENTIAL_NOT_RENDERABLE,
        ),
        (
            // TWO refusal lines: the LAST one in the tail wins, because a
            // runner that logged an earlier draft would have the final one be
            // the one it refused on.
            "two refusal lines",
            format!(
                "refusal-reason={TERMINAL_STATE_TARGET_TOPIC_CONFIG_REFUSED}\n\
                 refusal-reason={TERMINAL_STATE_CREDENTIAL_NOT_RENDERABLE}\n"
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
        let outcome = reconcile_restore(
            &restore(),
            &client,
            &unobserved_scorecard,
            &unverified_evidence,
            now(),
        )
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

/// A pod wearing this Job's name label but not owned by it is NEVER read —
/// D-SEAMS **S6**, defect `SEC-PODLOG`.
///
/// This reconciler used to take `list.items.into_iter().next()`: the first pod
/// the label selector returned, with no owner check at all.
/// `batch.kubernetes.io/job-name` is a plain label that anything able to
/// create a pod in the namespace can set, so a planted pod's `exitCode` and
/// the three interface **I8** keys in its stdout became this `Restore`'s
/// recorded outcome — on the object an approver reads after data went back
/// into a cluster.
///
/// Every impostor carries `exitCode: 0`, so a reconciler that read one would
/// report `phase: Succeeded`. The expected answer is a terminal `NoExitCode`
/// with no `exitCode` on the object and NO `GET …/pods/<p>/log` at all.
///
/// KILLS: `list.items.into_iter().next()`; an ownerless-pod fallback; matching
/// an owner reference without `controller: true`; matching an owner reference
/// of any kind.
#[tokio::test]
async fn a_labelled_pod_this_job_does_not_own_is_never_read() {
    for (label, owners) in [
        (
            "an ownerless pod — the shape a hand-created pod with the label has",
            "null".to_string(),
        ),
        (
            "a pod owned by a DIFFERENT Job's UID",
            pod_owner_json("Job", "cccccccc-0000-4000-8000-0000000000c9", true),
        ),
        (
            "a NON-controller owner reference carrying the right UID",
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
        let (client, rec, bodies) = mock_client_recording_bodies(finished_routes(
            pod_list_terminated_owned_by(0, &owners),
            log_body(&i8_tail()),
            "Complete",
        ));
        let outcome = reconcile_restore(
            &restore(),
            &client,
            &unobserved_scorecard,
            &unverified_evidence,
            now(),
        )
        .await
        .unwrap_or_else(|e| panic!("{label}: an unowned pod is a status, not an error: {e}"));

        let seen = rec.lock().expect("the recorder is readable").clone();
        assert!(
            seen.iter().all(|r| !path(&r.uri).ends_with("/log")),
            "{label}: the log of a pod this Job does not own is NOT FETCHED. Got {seen:?}"
        );
        assert_eq!(
            outcome.exit_code, None,
            "{label}: and no exit code is taken from it"
        );
        assert_eq!(
            outcome.terminal_state.as_deref(),
            Some("NoExitCode"),
            "{label}: zero OWNED pods is the same state as zero pods"
        );
        assert_eq!(
            outcome.keys,
            RestoreEvidenceKeys::default(),
            "{label}: and no evidence key is recorded from a stranger's stdout"
        );
        let bodies = bodies
            .lock()
            .expect("the body recorder is readable")
            .clone();
        let status = patched_statuses(&bodies).remove(0);
        assert!(
            status.get("exitCode").is_none(),
            "{label}: `exitCode` is ABSENT on the object, not `0`: {status}"
        );
        assert_eq!(
            status["phase"].as_str(),
            Some("Failed"),
            "{label}: and the phase is not `Succeeded`"
        );
    }
}

/// The positive control, and the contest: an OWNED pod IS read, and two pods
/// claiming one Job are BOTH refused under their own terminal state — review
/// finding **R1**.
///
/// An `ownerReference` is ordinary metadata its author writes and the API
/// server does not validate, so a tenant who can read the Job's `metadata.uid`
/// can mint a second claimant — and it is newer than the genuine runner pod by
/// construction. Ranking would hand this `Restore`'s recorded outcome, the
/// object an approver reads after data went back into a cluster, to whoever
/// planted it. Nothing is read instead.
///
/// KILLS: refusing every pod; newest-wins over a contested Job; oldest-wins;
/// reporting a contest as an ordinary `NoExitCode`.
#[tokio::test]
async fn an_owned_pod_is_read_and_two_claimants_are_refused() {
    // ARM 1 — the ordinary case.
    let (client, rec, bodies) = mock_client_recording_bodies(finished_routes(
        pod_list_terminated(0),
        log_body(&i8_tail()),
        "Complete",
    ));
    let outcome = reconcile_restore(
        &restore(),
        &client,
        &unobserved_scorecard,
        &unverified_evidence,
        now(),
    )
    .await
    .expect("the reconcile completes");
    assert_eq!(
        outcome.exit_code,
        Some(0),
        "the pod this Job's UID owns IS read — the guard refuses impostors, not everything"
    );
    let bodies_seen = bodies
        .lock()
        .expect("the body recorder is readable")
        .clone();
    assert_eq!(
        patched_statuses(&bodies_seen).remove(0)["evidence"]["scorecardKey"].as_str(),
        Some(SCORECARD_KEY),
        "and its stdout is where the evidence keys come from"
    );
    let logs: Vec<String> = rec
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

    // ARM 2 — a forged second claimant, in both listing orders.
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
        "state":{{"terminated":{{"exitCode":{exit},"finishedAt":"2026-09-10T11:59:00Z"}}}}}}]}}}}"#
        )
    };
    let genuine = claimant("genuine", "2026-09-10T11:50:00Z", 3);
    let forged = claimant("forged", "2026-09-10T11:55:00Z", 0);
    for (label, items) in [
        ("forged last", format!("[{genuine},{forged}]")),
        ("forged first", format!("[{forged},{genuine}]")),
    ] {
        let list =
            format!(r#"{{"apiVersion":"v1","kind":"PodList","metadata":{{}},"items":{items}}}"#);
        let (client, rec, bodies) =
            mock_client_recording_bodies(finished_routes(list, log_body(&i8_tail()), "Complete"));
        let outcome = reconcile_restore(
            &restore(),
            &client,
            &unobserved_scorecard,
            &unverified_evidence,
            now(),
        )
        .await
        .unwrap_or_else(|e| panic!("{label}: a contested Job is a status, not an error: {e}"));
        let seen = rec.lock().expect("the recorder is readable").clone();
        assert!(
            seen.iter().all(|r| !path(&r.uri).ends_with("/log")),
            "{label}: NEITHER claimant's log is read; got {seen:?}"
        );
        assert_eq!(
            outcome.exit_code, None,
            "{label}: and neither claimant's exit code is taken — not the forged `0`, and not \
             the genuine `3` either"
        );
        assert_eq!(
            outcome.terminal_state.as_deref(),
            Some(TERMINAL_STATE_POD_OWNERSHIP_CONTESTED),
            "{label}: under its OWN name, not `NoExitCode`"
        );
        assert_eq!(
            outcome.keys,
            RestoreEvidenceKeys::default(),
            "{label}: and no evidence key from either"
        );
        let bodies = bodies
            .lock()
            .expect("the body recorder is readable")
            .clone();
        let status = patched_statuses(&bodies).remove(0);
        assert!(
            status.get("exitCode").is_none(),
            "{label}: `exitCode` absent, not `0`: {status}"
        );
        assert_eq!(status["phase"].as_str(), Some("Failed"), "{label}");
        assert!(
            TERMINAL_STATES.contains(&TERMINAL_STATE_POD_OWNERSHIP_CONTESTED),
            "{label}: and the state is in the one closed list"
        );
    }
}

/// The crashed-Job case: a Job that finished with no terminated state for
/// `runner` gets a TERMINAL status with `exitCode` ABSENT.
#[tokio::test]
async fn a_job_that_finished_without_a_terminated_state_gets_a_terminal_status() {
    let empty = r#"{"apiVersion":"v1","kind":"PodList","metadata":{},"items":[]}"#.to_string();
    let (client, _rec, bodies) =
        mock_client_recording_bodies(finished_routes(empty, log_body(""), "Failed"));
    let outcome = reconcile_restore(
        &restore(),
        &client,
        &unobserved_scorecard,
        &unverified_evidence,
        now(),
    )
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

// ---------------------------------------------------------------------------
// The steady object, and the terminal object whose pod is gone — Task 24
// fix round 1. The `Backup` twins live in `tests/verification.rs`.
// ---------------------------------------------------------------------------

/// A scorecard oracle that answers with a PASSING, digestible observation.
///
/// The digest is what makes the verification reachable at all: the second
/// patch is attempted only when both mandatory keys AND a digest the
/// controller computed over the bytes it fetched are present.
fn passing_scorecard(_key: String) -> BoxFuture<'static, Option<ScorecardObservation>> {
    Box::pin(async {
        Some(ScorecardObservation {
            outcome: Some("pass".to_string()),
            last_phase_completed: Some(9),
            scorecard_sha256: Some(
                "sha256:1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c"
                    .to_string(),
            ),
            ..ScorecardObservation::default()
        })
    })
}

/// A verify oracle answering `Valid`, reached at `at`.
///
/// THE INSTANT IS A PARAMETER FOR THE REASON THE `Backup` TWIN'S IS
/// (`tests/verification.rs::valid_oracle_at`): `verify_evidence` reads its own
/// clock, so a re-verification on a later pass really does reach a later
/// instant, and the rule that keeps a steady object quiet is that an UNCHANGED
/// verdict keeps the instant it was first reached at. An oracle returning a
/// constant cannot see that rule at all.
fn valid_evidence_at(
    at: DateTime<Utc>,
) -> impl Fn(weirkeeper::verification::EvidenceRef) -> BoxFuture<'static, VerificationResult> {
    move |r| {
        Box::pin(async move {
            VerificationResult {
                result: VerificationVerdict::Valid,
                matched_key_id: Some(
                    "917cf9a299872cbf8b2715999ce457464705bb8f48df0a07e9b1e19bb9f383fd".to_string(),
                ),
                payload_type: r.payload_type.to_string(),
                verified_at: at,
                detail: None,
                // PLAT-19.1: this oracle stands in for a signature that
                // verified under a key this fixture does not model, so there
                // is no trust projection to carry and the verdict is the
                // signature's alone.
                trust: None,
            }
        })
    }
}

/// One finished, verified pass over a fresh `Restore`, and the object the API
/// server would hold afterwards.
///
/// Built by RUNNING the pass and applying its patches the way the server does
/// (`conditions::apply_merge_patch`), never by writing a status literal: a
/// hand-written fixture pins what this file's author believes a verified
/// `Restore` looks like, and both rows below are about what the controller
/// does to the one it actually produced.
async fn settled_verified_restore() -> (Restore, Vec<Value>) {
    let (client, _rec, bodies) = mock_client_recording_bodies(finished_routes(
        pod_list_terminated(0),
        log_body(&i8_tail()),
        "Complete",
    ));
    reconcile_restore(
        &restore(),
        &client,
        &passing_scorecard,
        &valid_evidence_at(utc(2026, 9, 10, 12, 0)),
        now(),
    )
    .await
    .expect("the first reconcile succeeds");
    let seen = bodies.lock().expect("readable").clone();
    let patches = patched_statuses(&seen);
    let mut status = Value::Object(serde_json::Map::new());
    for p in &patches {
        apply_merge_patch(&mut status, p);
    }
    let mut settled: Value = serde_json::from_str(&restore_json(PLAN_BYTES, APPROVAL, NAME))
        .expect("the fixture parses");
    settled["status"] = status;
    (
        serde_json::from_value(settled).expect("the settled object is a Restore"),
        patches,
    )
}

/// **A VERIFIED `Restore` RECONCILES AGAIN AND SENDS NOTHING** — the twin of
/// `tests/verification.rs::a_verified_object_reconciles_without_a_patch`,
/// which had only the `Backup` half.
///
/// Plan erratum **E11(d)**, MEASURED on a live cluster during the Phase B run:
/// **20 `Restore` reconciles per second**, each a real write, because a JSON
/// merge patch REPLACES arrays and the terminal patch's `conditions` deleted
/// the `Verified` condition the second patch had just added.
///
/// KILLS: removing `verification::carry_verified` from `finished_status_patch`
/// — asserted twice over, because the two guards are independent. The
/// already-terminal return (step 2b) stops the SECOND PASS from reaching the
/// builder at all, so the last arm reaches the builder DIRECTLY: a patch built
/// for a verified object carries `Verified` whatever the caller does.
#[tokio::test]
async fn a_verified_restore_reconciles_without_a_patch() {
    let (settled, first) = settled_verified_restore().await;
    assert_eq!(first.len(), 2, "the first pass writes both patches");
    assert_eq!(
        first[1].pointer("/evidence/verification/result"),
        Some(&Value::String("Valid".to_string())),
        "the second patch is the verification; got {}",
        first[1]
    );
    let status = serde_json::to_value(settled.status.as_ref().expect("a settled status"))
        .expect("the status serialises");
    let types: Vec<String> = conditions_of(&status)
        .into_iter()
        .map(|(t, ..)| t)
        .collect();
    assert!(
        types.iter().any(|t| t == "Verified") && types.iter().any(|t| t == "Complete"),
        "after both patches the object carries BOTH the run's own conditions and the \
         controller's verdict about it; got {types:?}"
    );
    assert_eq!(status["outcome"], Value::String("pass".to_string()));

    // PASS TWO, over that object, with BOTH clocks moved — the reconcile's
    // (12:00 -> 12:30) and the verification's own (12:00 -> 18:45). NOTHING IS
    // SENT.
    let (client, _rec, bodies) = mock_client_recording_bodies(finished_routes(
        pod_list_terminated(0),
        log_body(&i8_tail()),
        "Complete",
    ));
    reconcile_restore(
        &settled,
        &client,
        &passing_scorecard,
        &valid_evidence_at(utc(2026, 9, 11, 18, 45)),
        utc(2026, 9, 10, 12, 30),
    )
    .await
    .expect("the second reconcile succeeds");
    let seen = bodies.lock().expect("readable").clone();
    let second = patched_statuses(&seen);
    assert_eq!(
        second.len(),
        0,
        "A STEADY OBJECT ISSUES ZERO `/status` PATCHES. A reconciler's own status write is what \
         wakes it, so one patch per pass is a LOOP and not an inefficiency. Sent: {second:?}"
    );

    // AND THE BUILDER ITSELF, REACHED DIRECTLY. `finished_status_patch` owns
    // the whole `conditions` array it writes, so it owes the parts of it that
    // are not its own: `Verified` is the controller's fact about the run,
    // computed after the terminal write, and a builder that dropped it would
    // start the loop again the moment any caller reached it.
    let rebuilt = finished_status_patch(
        &settled,
        0,
        &restore_evidence_keys(&log_body(&i8_tail())),
        None,
        None,
        None,
        None,
        utc(2026, 9, 10, 12, 30),
    );
    let rebuilt_types: Vec<String> = conditions_of(&rebuilt["status"])
        .into_iter()
        .map(|(t, ..)| t)
        .collect();
    assert!(
        rebuilt_types.iter().any(|t| t == "Verified"),
        "`finished_status_patch` carries the existing `Verified` condition forward \
         (`verification::carry_verified`); got {rebuilt_types:?} from {rebuilt}"
    );
}

/// **A TERMINAL, VERIFIED `Restore` WHOSE RUNNER POD IS GONE IS LEFT ALONE.**
///
/// Task 24 review, Milestone 5 — **proven on a live cluster**. A `Restore` at
/// `phase: Succeeded`, `exitCode: 0`, `outcome: pass` and
/// `verification.result: Valid` became `phase: Failed`, `exitReason:
/// operational`, its `[Complete, EvidenceRecorded, Verified]` conditions
/// replaced by one `Failed/NoExitCode` — after a plain `kubectl rollout
/// restart deploy/weirkeeper`. Not a crash: an upgrade, a node reboot, an
/// eviction or an OOM does the same thing, because the restarted controller
/// re-lists a finished Job whose pod has been garbage-collected and re-derives
/// an exit code from it. The Job outlives its pod by seven days.
///
/// KILLS: removing `reconcile_restore`'s already-terminal guard (step 2b,
/// before `find_pod`). Without it this pass finds no pod, takes the crashed
/// branch and PATCHES.
#[tokio::test]
async fn a_terminal_verified_restore_whose_pod_is_gone_is_not_re_patched() {
    let (settled, _) = settled_verified_restore().await;
    let before = serde_json::to_value(settled.status.as_ref().expect("a settled status"))
        .expect("the status serialises");
    assert_eq!(before["phase"], Value::String("Succeeded".to_string()));

    // THE POD IS GONE AND THE JOB IS NOT — and the `/pods` route is PRESENT,
    // so "it did not list the pods" is an assertion and not an inability.
    let empty = r#"{"apiVersion":"v1","kind":"PodList","metadata":{},"items":[]}"#.to_string();
    let (client, rec, bodies) =
        mock_client_recording_bodies(finished_routes(empty, log_body(""), "Complete"));
    let outcome = reconcile_restore(
        &settled,
        &client,
        &passing_scorecard,
        &valid_evidence_at(utc(2026, 9, 11, 18, 45)),
        utc(2026, 9, 10, 12, 30),
    )
    .await
    .expect("the reconcile completes");

    let seen = bodies.lock().expect("readable").clone();
    let patches = patched_statuses(&seen);
    assert_eq!(
        patches.len(),
        0,
        "A FINISHED RUN WHOSE POD HAS BEEN GARBAGE-COLLECTED IS NOT RE-JUDGED. A merge patch \
         REPLACES arrays, so every patch listed here deletes `Complete` and `EvidenceRecorded` \
         off an object that earned them and relabels a run that exited 0 with a signed, `Valid`, \
         PASSING scorecard as `Failed`/`operational`. Sent: {patches:?}"
    );
    assert_eq!(
        outcome.exit_code,
        Some(0),
        "the outcome reports the code already ON the object, not a code re-derived from a pod \
         that no longer exists"
    );
    assert_eq!(outcome.terminal_state, None);
    assert_eq!(outcome.requeue, Requeue::AwaitChange);
    let requests = rec.lock().expect("readable").clone();
    assert!(
        !requests.iter().any(|r| path(&r.uri).ends_with("/pods")),
        "…and the pod list is not even READ: the guard is before `find_pod`, so a terminal \
         object costs one `GET /jobs` and nothing else. Saw: {:?}",
        requests
            .iter()
            .map(|r| (&r.method, path(&r.uri)))
            .collect::<Vec<_>>()
    );

    // BELT AND BRACES, AND A SEPARATE CLAIM. Even reached directly — by a
    // future edit that moves the guard, or by a caller this file does not know
    // about — the crashed-path builder no longer owns the whole condition
    // array.
    let crashed = crashed_status_patch(&settled, "NoExitCode", NAME, utc(2026, 9, 10, 12, 30));
    let types: Vec<String> = conditions_of(&crashed["status"])
        .into_iter()
        .map(|(t, ..)| t)
        .collect();
    assert!(
        types.iter().any(|t| t == "Verified"),
        "`crashed_status_patch` goes through `verification::carry_verified`, so the controller's \
         own verdict about the run survives a patch that is not about it. Got {crashed}"
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
    let outcome = reconcile_restore(
        &restore(),
        &client,
        &unobserved_scorecard,
        &unverified_evidence,
        now(),
    )
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
    let err = reconcile_restore(
        &restore(),
        &client2,
        &unobserved_scorecard,
        &unverified_evidence,
        now(),
    )
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
    reconcile_restore(&restore(), &client, &oracle, &unverified_evidence, now())
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
    reconcile_restore(
        &restore(),
        &client2,
        &unobserved_scorecard,
        &unverified_evidence,
        now(),
    )
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

/// `status.topicPreflight` is scanned by key name, and ABSENT when the runner
/// printed no line.
///
/// # What this test asserted before Task 24, and why it changed
///
/// Guard **G-TS**'s observation is returned by phase 0 in
/// `logweir::drill::RestoreOutcome::topic_preflight` and, by Global Constraint
/// 12 as amended, is deliberately not a scorecard field — so for two slots
/// **nothing carried it out of the pod**, this test asserted the field was
/// never written, and the CRD's own description recorded the gap in place
/// (plan erratum **E10(c)**). It also said what closing the gap would take: a
/// fourth machine-read stdout line, which is the interface owner's change.
///
/// Task 24 made that change on both sides, so the property is now the pair:
/// **present when the line is, absent when it is not.** An assertion that the
/// field is never written would now be asserting the bug.
///
/// KILLS: deriving the block from anything but the line (arm 2 would fabricate
/// one), and reading the line by POSITION rather than by name (arm 1 puts it
/// before interface I8's three keys, which is where the runner prints it).
#[tokio::test]
async fn the_topic_preflight_is_scanned_by_key_name_and_absent_without_it() {
    let doc = scorecard_json("pass", "pass", "");
    let observation = scorecard_observation(doc.as_bytes()).expect("the fixture is a scorecard");
    let oracle = move |_key: String| -> BoxFuture<'static, Option<ScorecardObservation>> {
        let o = observation.clone();
        Box::pin(async move { Some(o) })
    };

    // ARM 1 — the runner printed it, before I8's three keys.
    let tail = format!(
        "{TOPIC_PREFLIGHT_KEY_PREFIX}{{\"timestampType\":\"CreateTime\",\"retentionMs\":604800000,\"timestampBound\":1760000000000}}\n{}",
        i8_tail()
    );
    let (client, _rec, bodies) = mock_client_recording_bodies(finished_routes(
        pod_list_terminated(0),
        log_body(&tail),
        "Complete",
    ));
    reconcile_restore(&restore(), &client, &oracle, &unverified_evidence, now())
        .await
        .expect("the reconcile completes");
    let seen = bodies
        .lock()
        .expect("the body recorder is readable")
        .clone();
    let status = patched_statuses(&seen).remove(0);
    let pre = status.get("topicPreflight").unwrap_or_else(|| {
        panic!("the preflight line was printed, so the field is written: {status}")
    });
    assert_eq!(pre["timestampType"], serde_json::json!("CreateTime"));
    assert_eq!(pre["retentionMs"], serde_json::json!(604_800_000i64));
    assert_eq!(
        pre["timestampBound"],
        serde_json::json!(1_760_000_000_000i64)
    );
    assert_eq!(
        pre.as_object().map(serde_json::Map::len),
        Some(3),
        "THE THREE FIELDS AND NO OTHERS: a fourth key on the line would be a property the \
         structural schema prunes, so it is dropped here rather than misfiled. Got {pre}"
    );

    // ARM 2 — no line, so no field. The same rule the evidence keys follow: a
    // run that did not complete phase 0 read nothing about the target's
    // config, and a fabricated block would be a claim about a check nobody
    // made.
    let (client, _rec, bodies) = mock_client_recording_bodies(finished_routes(
        pod_list_terminated(0),
        log_body(&i8_tail()),
        "Complete",
    ));
    reconcile_restore(&restore(), &client, &oracle, &unverified_evidence, now())
        .await
        .expect("the reconcile completes");
    let seen = bodies
        .lock()
        .expect("the body recorder is readable")
        .clone();
    let status = patched_statuses(&seen).remove(0);
    assert!(
        status.get("topicPreflight").is_none(),
        "ABSENT, never fabricated: {status}"
    );

    // …and the CRD's description says the producer exists and when the field
    // is still absent, so a reader of the shipped schema is not left wondering.
    let doc = restore_crd_field_description(&["status", "topicPreflight"]);
    assert!(
        doc.contains("topic-preflight=") && doc.to_lowercase().contains("absent"),
        "the field description names the stdout key its value comes from AND the case in which \
         it is still absent. Got: {doc}"
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

// ===========================================================================
// `status.reason` — the REASON printer column. FIX ROUND 1, review finding M2
// ===========================================================================

/// **The `REASON` column reads `.status.reason`, and every status this
/// reconciler writes sets it.**
///
/// THE DEFECT THIS PINS, MEASURED LIVE AT THE TASK 20 REVIEW. The column read
/// `.status.exitReason`, which [`refused_status_patch`] can only write as
/// `operational` — Global Constraint 11 has no code for "the controller
/// refused before anything ran", and no run means no code to lift. So
/// `kubectl get restore` printed the identical `operational` for
/// `ApprovalNotReceived`, `ApprovalNotVerified`, `PlanHashMismatch`,
/// `ClusterNotReachable` and `NameTooLong` — five different facts under one
/// word, with the specific state visible only to `kubectl describe`. For a
/// `Restore` those are exactly the states an operator scans a list for.
///
/// THREE ARMS, AND THE THIRD IS THE ONE THAT CANNOT ROT.
/// 1. The SHIPPED CRD's `REASON` column names `.status.reason`, and the status
///    schema declares the field. (`Backup` is asserted UNAFFECTED in the same
///    arm: it has no `REASON` column at all, so nothing on that kind read
///    `.status.exitReason` and nothing there was changed.)
/// 2. All five status-patch builders set `reason`, VERBATIM the reason of the
///    condition each writes about the run's terminal or current state — so
///    this is not a third vocabulary beside errata **E5b**'s two, and it is
///    never one of GC11's lowercase-hyphenated wire strings. One arm drives a
///    real `reconcile_restore` so the field is proven to reach the wire
///    through `patch_status` and not merely to exist in a builder's return.
/// 3. A SOURCE SCAN over `controllers/restore.rs`: every `"conditions"` key
///    written into a status object has a `"reason"` key in the same builder.
///    A sixth patch shape added later without one fails here, which is the
///    only way "every status write" stays true of a file that grows.
#[tokio::test]
async fn every_status_write_sets_the_scalar_reason() {
    // ---- ARM 1: the column, and the field it names ------------------------
    let shipped = workspace_yaml("config/crd/restores.yaml");
    let columns = shipped["spec"]["versions"][0]["additionalPrinterColumns"]
        .as_sequence()
        .expect("the Restore CRD declares printer columns");
    let reason_column = columns
        .iter()
        .find(|c| c["name"].as_str() == Some("REASON"))
        .expect("the Restore CRD has a REASON column");
    assert_eq!(
        reason_column["jsonPath"].as_str(),
        Some(".status.reason"),
        "the REASON column must read the scalar condition reason. `.status.exitReason` is \
         `operational` for every refusal this controller makes itself, which is what review \
         finding M2 measured live on two objects"
    );
    assert!(
        shipped["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["status"]
            ["properties"]
            .get("reason")
            .is_some(),
        "…and the shipped status schema declares the field the column reads, or the column \
         renders blank forever"
    );
    // `exitReason` IS NOT REMOVED. It is the honest home of GC11's wire string
    // and of the runner's own `refusal-reason=` terminal state.
    assert!(
        shipped["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["status"]
            ["properties"]
            .get("exitReason")
            .is_some(),
        "this fix ADDS a field; it does not take GC11's vocabulary away"
    );
    // `Backup` IS UNAFFECTED, and this says so mechanically.
    let backup_columns: Vec<String> = workspace_yaml("config/crd/backups.yaml")["spec"]["versions"]
        [0]["additionalPrinterColumns"]
        .as_sequence()
        .expect("the Backup CRD declares printer columns")
        .iter()
        .map(|c| c["name"].as_str().unwrap_or_default().to_string())
        .collect();
    assert!(
        !backup_columns.contains(&"REASON".to_string()),
        "the `Backup` CRD does NOT have this defect and was NOT changed: it has no REASON column \
         to repoint (PHASE/EXIT/RECORDS/SIGNED/AGE). If a later task adds one, it reads \
         `.status.reason` and adds the field the same way. Got: {backup_columns:?}"
    );

    // ---- ARM 2: every builder, and the condition it must agree with -------
    let r = restore();
    let hold = RestoreAdmission::ApprovalNotVerified {
        approval: "a1".to_string(),
    };
    let keys = RestoreEvidenceKeys::default();
    // (label, the status, the index of the condition `reason` must equal)
    let cases: Vec<(&str, Value, usize)> = vec![
        (
            "admission_hold_patch",
            admission_hold_patch(&r, &hold, now())["status"].clone(),
            0,
        ),
        (
            "refused_status_patch",
            refused_status_patch(&r, TERMINAL_STATE_NAME_TOO_LONG, "too long", now())["status"]
                .clone(),
            0,
        ),
        (
            "running_status_patch (the creating pass, two conditions)",
            // THE CURRENT CONDITION IS `JobCreated`, THE ARRAY'S SECOND
            // ELEMENT. `Admitted` is first and is about a check that already
            // finished, so a scalar taken from the array's head would print
            // `Admitted` once and `JobCreated` on every later pass over an
            // object whose state never changed.
            running_status_patch(&r, "r1", true, now())["status"].clone(),
            1,
        ),
        (
            "running_status_patch (a later pass, one condition)",
            running_status_patch(&r, "r1", false, now())["status"].clone(),
            0,
        ),
        (
            "finished_status_patch exit 0 (two conditions)",
            // `Complete` is first and `EvidenceRecorded` is appended after it;
            // the TERMINAL condition is the one the column must show.
            finished_status_patch(&r, 0, &keys, None, None, None, None, now())["status"].clone(),
            0,
        ),
        (
            "finished_status_patch exit 3",
            finished_status_patch(
                &r,
                3,
                &keys,
                Some(TERMINAL_STATE_GUARD_REFUSED_UNKNOWN_REASON),
                None,
                None,
                None,
                now(),
            )["status"]
                .clone(),
            0,
        ),
        (
            "crashed_status_patch",
            crashed_status_patch(&r, "NoExitCode", "r1", now())["status"].clone(),
            0,
        ),
    ];
    assert_eq!(
        cases.len(),
        7,
        "five builders over seven shapes; a builder added without a case here is caught by ARM 3"
    );
    for (label, status, primary) in &cases {
        let reason = status["reason"].as_str().unwrap_or_else(|| {
            panic!("[{label}] every status write sets a scalar `reason`. Got: {status}")
        });
        assert!(
            !reason.is_empty(),
            "[{label}] and it is never the empty string, which renders as a blank column"
        );
        let conditions = conditions_of(status);
        assert_eq!(
            reason, conditions[*primary].2,
            "[{label}] `status.reason` is VERBATIM the reason of the condition describing this \
             run's terminal or current state (index {primary} of {conditions:?}) — not a third \
             vocabulary beside errata E5b's two"
        );
        // NEVER GC11'S WIRE VOCABULARY. This is the assertion that fails if
        // anyone repoints this field at `exitReason` again: `operational`,
        // `ok`, `drill-not-pass`, `guard-refused` and `signing-or-lock` are
        // all excluded, the first by name and the rest by the shape.
        assert_ne!(
            reason, REASON_OPERATIONAL,
            "[{label}] `operational` in this column is the whole of review finding M2"
        );
        let first = reason.chars().next().expect("non-empty");
        assert!(
            first.is_ascii_uppercase() && !reason.contains('-'),
            "[{label}] a condition reason is CamelCase with no `-` (errata E5b, and \
             metav1.Condition's own pattern). Got: {reason}"
        );
    }

    // …AND IT REACHES THE WIRE. One real reconcile, the exit-3 path.
    let (client, _rec, bodies) = mock_client_recording_bodies(finished_routes(
        pod_list_terminated(3),
        log_body(&format!(
            "refusal-reason={TERMINAL_STATE_TARGET_TOPIC_CONFIG_REFUSED}\n"
        )),
        "Failed",
    ));
    reconcile_restore(
        &restore(),
        &client,
        &unobserved_scorecard,
        &unverified_evidence,
        now(),
    )
    .await
    .expect("the reconcile completes");
    let seen = bodies
        .lock()
        .expect("the body recorder is readable")
        .clone();
    let status = patched_statuses(&seen).remove(0);
    assert_eq!(
        status["reason"].as_str(),
        Some(CONDITION_REASON_GUARD_REFUSED),
        "the field reaches the API server through `patch_status`, not only a builder's return \
         value. Got: {status}"
    );
    assert_eq!(
        status["exitReason"].as_str(),
        Some(TERMINAL_STATE_TARGET_TOPIC_CONFIG_REFUSED),
        "and `exitReason` still carries the runner's own more specific terminal state — two \
         fields, two questions"
    );

    // ---- ARM 3: no sixth patch shape can omit it -------------------------
    let src = this_module_source();
    let mut offences: Vec<String> = Vec::new();
    for (i, line) in src.lines().enumerate() {
        if !line.contains("\"conditions\"") {
            continue;
        }
        // The builder this line belongs to: back up to the enclosing `fn`.
        let mut start: Option<(usize, String)> = None;
        for (j, candidate) in src.lines().take(i).enumerate() {
            if candidate.starts_with("pub fn ") || candidate.starts_with("fn ") {
                start = Some((j, candidate.to_string()));
            }
        }
        let Some((from, signature)) = start else {
            continue;
        };
        // The body runs to the next top-level `fn`, or to the end.
        let to = src
            .lines()
            .enumerate()
            .skip(i + 1)
            .find(|(_, l)| l.starts_with("pub fn ") || l.starts_with("fn "))
            .map_or(src.lines().count(), |(j, _)| j);
        let body: String = src
            .lines()
            .skip(from)
            .take(to - from)
            .collect::<Vec<_>>()
            .join("\n");
        if !body.contains("\"reason\"") {
            offences.push(format!(
                "line {} in `{}` writes `\"conditions\"` into a status with no `\"reason\"` \
                 beside it",
                i + 1,
                signature.trim()
            ));
        }
    }
    assert!(
        offences.is_empty(),
        "review finding M2: EVERY status this reconciler writes carries the scalar \
         `status.reason` the REASON printer column reads, because a status with conditions and \
         no scalar is a row that renders blank in `kubectl get restore` while `describe` knows \
         the answer.\n  {}",
        offences.join("\n  ")
    );
    assert!(
        src.matches("\"conditions\"").count() >= 5,
        "the scan found {} `\"conditions\"` writes; there are five patch builders, so a lower \
         count means this walk found nothing and asserts nothing",
        src.matches("\"conditions\"").count()
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
    reconcile_restore(
        &restore(),
        &client,
        &unobserved_scorecard,
        &unverified_evidence,
        now(),
    )
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

// ===========================================================================
// TASK 16b — THE STEADY-OBJECT ROW, plan erratum E11(d)
// ===========================================================================

/// The routes a RUNNING pass needs: the Job exists and has not finished, and
/// the status is patchable. No admission routes: the Job already exists, so
/// the reconcile never reaches the approval.
fn running_routes() -> Vec<Route> {
    vec![
        Route {
            method: "GET",
            path_suffix: "/jobs/logweir-restore-incident-4471",
            status: 200,
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

/// How many `/status` `PATCH`es the double was asked for.
fn status_patch_count(bodies: &[SeenBody]) -> usize {
    bodies
        .iter()
        .filter(|b| b.method == "PATCH" && path(&b.uri).ends_with("/status"))
        .count()
}

/// An already-created Job is the migration boundary. The controller observes
/// it with its original namespace-wide Secret transport and never rewrites or
/// backfills a new bundle into the in-flight pod template.
#[tokio::test]
async fn an_in_flight_legacy_restore_is_not_silently_migrated() {
    let (client, recorder, bodies) = mock_client_recording_bodies(running_routes());
    reconcile_restore(
        &restore(),
        &client,
        &unobserved_scorecard,
        &unverified_evidence,
        now(),
    )
    .await
    .expect("an in-flight legacy Job remains observable");

    let calls = recorder.lock().unwrap().clone();
    assert!(
        calls.iter().all(|call| {
            !path(&call.uri).contains("/approvals/")
                && !path(&call.uri).contains("/configmaps/")
                && !path(&call.uri).contains("/trustrosters/")
        }),
        "an existing Job is not rematerialized or rebound: {calls:?}"
    );
    assert_eq!(post_count(&bodies.lock().unwrap(), "/configmaps"), 0);
}

#[tokio::test]
async fn a_same_name_job_is_never_adopted_across_an_identity_boundary() {
    let mutations: [JsonMutation; 5] = [
        ("missing owner", |job: &mut Value| {
            job["metadata"]
                .as_object_mut()
                .unwrap()
                .remove("ownerReferences");
        }),
        ("previous Restore UID", |job: &mut Value| {
            job["metadata"]["ownerReferences"][0]["uid"] = serde_json::json!("previous-uid");
        }),
        ("wrong owner kind", |job: &mut Value| {
            job["metadata"]["ownerReferences"][0]["kind"] = serde_json::json!("Backup");
        }),
        ("non-blocking owner", |job: &mut Value| {
            job["metadata"]["ownerReferences"][0]["blockOwnerDeletion"] = serde_json::json!(false);
        }),
        ("appended secondary owner", |job: &mut Value| {
            let second = serde_json::json!({
                "apiVersion": "logweir.dev/v1alpha1",
                "kind": "Backup",
                "name": "secondary-owner",
                "uid": "secondary-owner-uid",
                "controller": false,
                "blockOwnerDeletion": true
            });
            job["metadata"]["ownerReferences"]
                .as_array_mut()
                .unwrap()
                .push(second);
        }),
    ];

    for (label, mutate) in mutations {
        let mut routes = running_routes();
        let mut job: Value = serde_json::from_str(&running_job_body()).unwrap();
        mutate(&mut job);
        routes[0].body = serde_json::to_string(&job).unwrap();
        let (client, _recorder, bodies) = mock_client_recording_bodies(routes);
        let outcome = reconcile_restore(
            &restore(),
            &client,
            &unobserved_scorecard,
            &unverified_evidence,
            now(),
        )
        .await
        .expect("a collision is a status verdict");
        assert_eq!(
            outcome.terminal_state.as_deref(),
            Some(TERMINAL_STATE_JOB_NAME_CONFLICT),
            "{label}"
        );
        assert_eq!(post_count(&bodies.lock().unwrap(), "/jobs"), 0);
    }
}

#[tokio::test]
async fn successful_create_responses_reject_an_appended_secondary_owner() {
    let secondary = serde_json::json!({
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "Backup",
        "name": "secondary-owner",
        "uid": "secondary-owner-uid",
        "controller": false,
        "blockOwnerDeletion": true
    });

    let mut config_map_routes = admission_routes(
        200,
        approval_json(true, &plan_hash(), &plan_hash()),
        200,
        cluster_json(true, PLAINTEXT_AUTH),
    );
    for route in &mut config_map_routes {
        if route.method == "POST" && route.path_suffix == "/configmaps" {
            let mut response: Value = serde_json::from_str(&route.body).unwrap();
            response["metadata"]["ownerReferences"]
                .as_array_mut()
                .unwrap()
                .push(secondary.clone());
            route.body = serde_json::to_string(&response).unwrap();
        }
    }
    let (client, _recorder, bodies) = mock_client_recording_bodies(config_map_routes);
    let outcome = reconcile_restore(
        &restore(),
        &client,
        &unobserved_scorecard,
        &unverified_evidence,
        now(),
    )
    .await
    .expect("a mutated ConfigMap creation response is a status verdict");
    assert_eq!(
        outcome.terminal_state.as_deref(),
        Some(TERMINAL_STATE_PLAN_CONFIG_MAP_CONFLICT)
    );
    assert_eq!(post_count(&bodies.lock().unwrap(), "/jobs"), 0);

    let mut job_routes = admission_routes(
        200,
        approval_json(true, &plan_hash(), &plan_hash()),
        200,
        cluster_json(true, PLAINTEXT_AUTH),
    );
    for route in &mut job_routes {
        if route.method == "POST" && route.path_suffix == "/jobs" {
            let mut response: Value = serde_json::from_str(&route.body).unwrap();
            response["metadata"]["ownerReferences"]
                .as_array_mut()
                .unwrap()
                .push(secondary.clone());
            route.body = serde_json::to_string(&response).unwrap();
        }
    }
    let (client, _recorder, bodies) = mock_client_recording_bodies(job_routes);
    let outcome = reconcile_restore(
        &restore(),
        &client,
        &unobserved_scorecard,
        &unverified_evidence,
        now(),
    )
    .await
    .expect("a mutated Job creation response is a status verdict");
    assert_eq!(
        outcome.terminal_state.as_deref(),
        Some(TERMINAL_STATE_JOB_NAME_CONFLICT)
    );
    assert_eq!(post_count(&bodies.lock().unwrap(), "/jobs"), 1);
}

#[test]
fn an_owned_legacy_job_remains_compatible_only_for_the_same_restore_uid() {
    let job: k8s_openapi::api::batch::v1::Job = serde_json::from_str(&running_job_body()).unwrap();
    assert!(compatible_restore_job(&job, &restore()));

    let mut recreated = restore();
    recreated.metadata.uid = Some("recreated-uid".to_string());
    assert!(!compatible_restore_job(&job, &recreated));
}

/// **Task 16b.** A steady `Restore` — one whose Job is still running — is
/// patched once and then never again.
///
/// The terminal state was never at risk (`status_is_terminal` returns before
/// any patch). The RUNNING state is where this reconciler spends its time, and
/// on a 15 s requeue it used to send an identical `running_status_patch` on
/// every pass: quiet at the API server, which is why Task 15c measured this
/// reconciler QUIET, but a request all the same. Erratum E11(d)'s third rule
/// is that a pass computing the status the object already carries sends
/// nothing, and a route-table count is what can see it.
#[tokio::test]
async fn a_steady_restore_issues_no_second_status_patch() {
    let (client, _rec, bodies) = mock_client_recording_bodies(running_routes());
    reconcile_restore(
        &restore(),
        &client,
        &unobserved_scorecard,
        &unverified_evidence,
        now(),
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
    let mut steady = restore();
    steady.status = Some(
        serde_json::from_value::<RestoreStatus>(stored)
            .expect("the patched status is a RestoreStatus — the API server stores it"),
    );

    let (client, _rec, bodies) = mock_client_recording_bodies(running_routes());
    reconcile_restore(
        &steady,
        &client,
        &unobserved_scorecard,
        &unverified_evidence,
        now() + chrono::Duration::minutes(1),
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
        "the second pass over an unchanged object writes NOTHING: {second:?}"
    );
}

// ---------------------------------------------------------------------------
// Task 22 — the roster reaches the runner, END TO END
// ---------------------------------------------------------------------------

/// The runner's DEBUG binary — the one the e2e harness runs too (erratum
/// **E9**), located from this crate's manifest directory.
///
/// A weirkeeper test cannot use `env!("CARGO_BIN_EXE_logweir")`: that variable
/// exists only for targets of the package that declares the binary, and
/// `weirkeeper` must NOT declare `logweir` as a dependency of any kind —
/// `scripts/check-one-signer.sh`'s check 1 counts normal, build **and** dev
/// edges, and the set of crates from which `logweir-evidence` is reachable is
/// pinned at exactly `{logweir, e2e}`. Taking even a dev-dependency here would
/// put `weirkeeper` in that set and turn the link-time single-signer gate red.
/// So the two sides meet through the filesystem and a process, which is also
/// what they do in a cluster.
fn runner_binary() -> PathBuf {
    let target = std::env::var_os("CARGO_TARGET_DIR").map_or_else(
        || {
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../target")
                .to_path_buf()
        },
        PathBuf::from,
    );
    let bin = target.join("debug").join("logweir");
    assert!(
        bin.exists(),
        "the runner binary is not at {}. This row is END-TO-END on purpose: it feeds the argv \\
         THIS crate emits to the parser that has to accept it. `cargo test --workspace` builds \\
         it; `cargo test -p weirkeeper` alone does not — run `cargo build -p logweir` first.",
        bin.display()
    );
    bin
}

/// A roster with THREE approver keys, one of them expired by its own status.
///
/// The first id is the REAL key id of the checked-in fixture public key,
/// computed here rather than written down, so the end-to-end half is pinning
/// the id the runner will actually derive from `--approver-key` and not a
/// string that merely looks like one. (The other two are the file's existing
/// synthetic ids; nothing verifies a signature on this path.)
fn roster_of_three(first_key_id: &str) -> weirkeeper::crds::trust_roster::TrustRoster {
    const KEY_ID_THIRD: &str =
        "sha256:3333333333333333333333333333333333333333333333333333333333333333";
    let json = format!(
        r#"{{
  "apiVersion": "logweir.dev/v1alpha1",
  "kind": "TrustRoster",
  "metadata": {{ "name": "default", "uid": "dddddddd-0000-4000-8000-00000000000d" }},
  "spec": {{
    "approverKeys": [
      {{ "keyId": "{first_key_id}", "spkiPem": "-----BEGIN PUBLIC KEY-----\nA\n-----END PUBLIC KEY-----\n" }},
      {{ "keyId": "{KEY_ID_EXPIRED}", "spkiPem": "-----BEGIN PUBLIC KEY-----\nB\n-----END PUBLIC KEY-----\n" }},
      {{ "keyId": "{KEY_ID_THIRD}", "spkiPem": "-----BEGIN PUBLIC KEY-----\nC\n-----END PUBLIC KEY-----\n" }}
    ],
    "signingKeys": [],
    "allowedClusterIds": ["MkU3OEVBNTcwNTJENDM2Qk"]
  }},
  "status": {{ "loaded": true, "expiredKeyIds": ["{KEY_ID_EXPIRED}"] }}
}}"#
    );
    serde_json::from_str(&json).expect("the fixture is a TrustRoster")
}

/// Every UNEXPIRED roster key id reaches the Job's argv, in roster order — and
/// the runner ACCEPTS every id this crate emits.
///
/// # This row is end-to-end, and erratum **E10** is why
///
/// Task 20 landed the argv half while the shipped `logweir restore run` still
/// refused the flag: measured in-pod, `error: unexpected argument
/// '--approver-key-ids' found`, exit 1, so a `Restore` with a non-empty
/// `TrustRoster` could not run at all. An argv-shape assertion cannot see that
/// — both sides looked correct in isolation. **The mechanism here is
/// process-level**: the `--approver-key-ids <id>` pairs are taken out of
/// `runner_argv`'s own output and handed to the runner's DEBUG binary, with
/// the fixture's public key as `--approver-key` and a plan phase 0 refuses on
/// a purely local check.
///
/// Three things then have to be true at once, and each has its own failure
/// mode: the flag parses (a usage error would be exit 1 with `unexpected
/// argument`), the id the reconciler emitted is INSIDE the pinned set (a miss
/// is exit 3 naming the pinned set), and the run reached phase 0 (whose local
/// refusal is the message asserted). The negative twin at the end removes the
/// matching id and requires the refusal, so a guard that admitted everything
/// would fail too.
///
/// No broker is dialled and none is needed: the pinned check is hoisted ahead
/// of phase 0, and phase 0's mapping check is local.
#[test]
fn the_restore_job_projects_every_unexpired_roster_key_id() {
    let fixture_pub =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../e2e/fixtures/signed/public.pem");
    let fixture_id = logweir_verify::VerifyingKey::from_pem_file(&fixture_pub)
        .expect("the checked-in fixture public key parses")
        .key_id();
    let roster = roster_of_three(&fixture_id);

    // --- the projection -----------------------------------------------------
    let ids = approver_key_ids(Some(&roster));
    assert_eq!(
        ids,
        vec![
            fixture_id.clone(),
            "sha256:3333333333333333333333333333333333333333333333333333333333333333".to_string()
        ],
        "three keys, one expired: exactly the two unexpired ids, in ROSTER order"
    );

    let argv = runner_argv(&restore(), &ids);
    let pairs: Vec<(String, String)> = argv
        .windows(2)
        .filter(|w| w[0] == "--approver-key-ids")
        .map(|w| (w[0].clone(), w[1].clone()))
        .collect();
    assert_eq!(
        pairs.len(),
        2,
        "exactly two values, one flag each: {argv:?}"
    );
    assert_eq!(
        pairs.iter().map(|(_, v)| v.clone()).collect::<Vec<_>>(),
        ids,
        "in roster order"
    );
    assert!(
        !argv.contains(&KEY_ID_EXPIRED.to_string()),
        "and the expired id is nowhere in the argv: {argv:?}"
    );

    // --- end to end: the runner accepts every id this crate emitted ---------
    let dir = scratch_dir("t22-roster-argv");
    let spec = dir.join("phase0-refuses.yaml");
    let plan = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/drill.yaml"),
    )
    .expect("the shipped example plan is readable")
    .replace(
        "topic_mapping_prefix: \"drill-\"",
        "topic_mapping_prefix: \"\"",
    );
    std::fs::write(&spec, plan).expect("the scratch plan is writable");

    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let approval = dir.join("approval.json");
    std::fs::copy(root.join("examples/approval.json"), &approval)
        .expect("copy the approval fixture");
    std::fs::copy(
        root.join("e2e/fixtures/signed/scorecard.sig"),
        approval.with_extension("sig"),
    )
    .expect("copy a parseable, wrong-purpose sidecar");
    let mut cmd = std::process::Command::new(runner_binary());
    cmd.args(["restore", "run", "--spec"])
        .arg(&spec)
        .arg("--approval")
        .arg(&approval)
        .arg("--approver-key")
        .arg(&fixture_pub)
        .arg("--allowed-clusters")
        .arg(root.join("examples/allowed-clusters.json"))
        .arg("--signing-key")
        .arg(root.join("e2e/fixtures/signed/signing.pem"));
    for (flag, value) in &pairs {
        cmd.arg(flag).arg(value);
    }
    let out = cmd.output().expect("the runner binary runs");
    let stderr = String::from_utf8_lossy(&out.stderr).to_string();

    assert!(
        !stderr.contains("unexpected argument"),
        "THE ERRATUM E10 FAILURE, and the reason this row is end-to-end: the runner refused a \\
         flag this crate emits. {stderr}"
    );
    assert_eq!(
        out.status.code(),
        Some(3),
        "the accepted pin reaches independent approval verification: {stderr}"
    );
    assert!(
        !stderr.contains("is not in the pinned set"),
        "every id the reconciler emitted must be ACCEPTED — the approver key's own id is the \\
         first entry of the roster: {stderr}"
    );
    assert!(
        stderr.contains("approval signature does not verify"),
        "the matching pin proceeds to the pre-phase-0 approval gate: {stderr}"
    );

    // --- the negative twin: drop the matching id and it refuses -------------
    let mut cmd = std::process::Command::new(runner_binary());
    cmd.args(["restore", "run", "--spec"])
        .arg(&spec)
        .arg("--approval")
        .arg(&approval)
        .arg("--approver-key")
        .arg(&fixture_pub)
        .arg("--allowed-clusters")
        .arg(root.join("examples/allowed-clusters.json"))
        .arg("--signing-key")
        .arg(root.join("e2e/fixtures/signed/signing.pem"))
        .args(["--approver-key-ids", KEY_ID_EXPIRED]);
    let out = cmd.output().expect("the runner binary runs");
    let stderr = String::from_utf8_lossy(&out.stderr).to_string();
    assert_eq!(out.status.code(), Some(3), "{stderr}");
    assert!(
        stderr.contains(&format!(
            "approver key id {fixture_id} is not in the pinned set"
        )),
        "pinning only the EXPIRED id refuses the run before phase 0 — so the flag is \\
         load-bearing and the positive half above is not vacuous: {stderr}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// A scratch directory under the system temp dir, made by hand.
///
/// `tempfile` is not a dependency of this crate and is not being added for one
/// test: `tests/linkage.rs` measures this crate's declared entries, and a new
/// one would have to justify itself there. The directory is created fresh and
/// removed at the end of the test that made it.
fn scratch_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("logweir-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("a scratch directory under the temp dir");
    dir
}

// ===========================================================================
// PLAT-19.1 fix round 1 — the approval bundle is resolved trust, not the roster
// (review finding F1)
// ===========================================================================

/// The public half the policy fixtures carry — the same opaque string the
/// roster fixture uses, so the ONLY difference between the two paths is where
/// the key was found.
const POLICY_KEY_PEM: &str = "-----BEGIN PUBLIC KEY-----\nA\n-----END PUBLIC KEY-----\n";

/// A `TrustPolicy` governing this namespace, carrying one `GovernedApproval`
/// key.
fn governing_policy(
    key: weirkeeper::crds::trust_policy::TrustedKey,
) -> weirkeeper::trust::ResolvedTrust {
    use weirkeeper::crds::trust_policy::{TrustPolicy, TrustPolicySpec};
    weirkeeper::trust::from_policy(&TrustPolicy {
        metadata: kube::api::ObjectMeta {
            name: Some("org-default".to_string()),
            uid: Some("uid-org-default".to_string()),
            generation: Some(3),
            ..kube::api::ObjectMeta::default()
        },
        spec: TrustPolicySpec {
            default: false,
            namespaces: Some(vec![NS.to_string()]),
            // DELIBERATELY DIFFERENT from the roster's, so the bundle's
            // allowlist proves WHICH source it came from.
            allowed_target_cluster_ids: Some(vec!["POLICY000000000000001".to_string()]),
            keys: vec![key],
        },
        status: None,
    })
}

/// One edit to a policy key, as a table row carries it.
type PolicyKeyMutation = Box<dyn Fn(&mut weirkeeper::crds::trust_policy::TrustedKey)>;

/// One policy key over [`POLICY_KEY_PEM`].
fn policy_approver_key() -> weirkeeper::crds::trust_policy::TrustedKey {
    use weirkeeper::crds::trust_policy::{KeyAlgorithm, KeyPrincipal, KeyState, KeyUsage};
    weirkeeper::crds::trust_policy::TrustedKey {
        key_id: KEY_ID_LIVE.to_string(),
        spki_pem: POLICY_KEY_PEM.to_string(),
        algorithm: KeyAlgorithm::Ed25519,
        usages: vec![KeyUsage::GovernedApproval],
        principal: KeyPrincipal {
            id: format!("install:{KEY_ID_LIVE}"),
            display: None,
        },
        not_before: utc(2026, 1, 1, 0, 0),
        not_after: utc(2099, 1, 1, 0, 0),
        state: KeyState::Active,
        retired_at: None,
        revoked_at: None,
        revocation_reason: None,
        revocation_effective_from: None,
    }
}

/// **F1.** A `GovernedApproval` key that exists only in a `TrustPolicy`
/// materialises the bundle.
///
/// # The split this closes
///
/// PLAT-19.1 wired ADMISSION to the resolved policy and left materialization
/// reading `TrustRoster/default`. D3 §7.6 step 1 stages a successor approver
/// key on the policy, and §15's L7 namespace B has no roster at all — so
/// `approval::decide` wrote `Verified=True` and the `Restore` then sat for ever
/// in a NON-TERMINAL `ApprovalBundleMaterializationFailed` hold, pointing the
/// operator at an object §7.2 says no longer decides anything for their
/// namespace. Fail-closed, and a permanent restore hold on the one rotation
/// procedure the decision documents.
///
/// KILLS: "read the key material from `TrustRoster.spec.approverKeys`" — the
/// roster this namespace resolves away from does not carry the key, so the
/// bundle refuses; and "render the allowlist from `TrustRoster.allowedClusterIds`"
/// — the second assertion names the policy's own list.
#[test]
fn a_policy_only_approver_key_materializes_the_bundle() {
    let trust = governing_policy(policy_approver_key());

    let bundle = approval_bundle_config_map(&restore(), &approval(true), &trust, now())
        .expect("a key the resolved policy carries materialises, and admission already said so");
    let data = bundle
        .data
        .expect("the bundle carries its four public members");
    assert_eq!(
        data.get(APPROVER_KEY_FILE).map(String::as_str),
        Some(POLICY_KEY_PEM),
        "the approver's public half comes from the policy entry the verdict named"
    );
    let allowed: serde_json::Value =
        serde_json::from_str(&data[ALLOWED_CLUSTERS_FILE]).expect("runner allowlist grammar");
    assert_eq!(
        allowed["allowed_cluster_ids"],
        serde_json::json!(["POLICY000000000000001"]),
        "`allowedTargetClusterIds` REPLACES the roster's `allowedClusterIds` (D3 §7.2), and the \
         two fixtures carry different values precisely so this cannot pass against the wrong one"
    );
}

/// **F1, the expiry half.** The bundle's re-check is `may_sign_new`, not the
/// roster's `status.expiredKeyIds`.
///
/// A bundle is the last thing written before a runner executes under that key,
/// so the question is D3 §7.4's first one — *may this key authorise something
/// new* — which is false for a `Retired` or `Revoked` key whose `notAfter` has
/// not arrived and which `expiredKeyIds` cannot express at all.
///
/// KILLS: "check `roster.status.expiredKeyIds`" — every key below is unexpired
/// by that measure, so all four would materialise.
#[test]
fn a_withdrawn_approver_key_writes_no_bundle() {
    use weirkeeper::crds::trust_policy::{KeyState, RevocationReason};
    let cases: Vec<(&str, PolicyKeyMutation)> = vec![
        (
            "retired",
            Box::new(|k: &mut weirkeeper::crds::trust_policy::TrustedKey| {
                k.state = KeyState::Retired;
                k.retired_at = Some(utc(2026, 9, 1, 0, 0));
            }),
        ),
        (
            "revoked for compromise",
            Box::new(|k: &mut weirkeeper::crds::trust_policy::TrustedKey| {
                k.state = KeyState::Revoked;
                k.revoked_at = Some(utc(2026, 9, 1, 0, 0));
                k.revocation_reason = Some(RevocationReason::KeyCompromise);
                k.revocation_effective_from = Some(utc(2026, 9, 1, 0, 0));
            }),
        ),
        (
            "past its notAfter",
            Box::new(|k: &mut weirkeeper::crds::trust_policy::TrustedKey| {
                k.not_after = utc(2026, 9, 1, 0, 0);
            }),
        ),
        (
            "staged for a rotation that has not started",
            Box::new(|k: &mut weirkeeper::crds::trust_policy::TrustedKey| {
                k.not_before = utc(2099, 1, 1, 0, 0);
            }),
        ),
    ];
    for (label, mutate) in cases {
        let mut key = policy_approver_key();
        mutate(&mut key);
        let error =
            approval_bundle_config_map(&restore(), &approval(true), &governing_policy(key), now())
                .expect_err(label);
        let message = error.to_string();
        assert!(
            message.contains("no longer accepts for a new authorisation"),
            "{label}: the refusal says the key may not authorise something NEW; got {message}"
        );
        assert!(
            message.contains("the TrustPolicy 'org-default'"),
            "{label}: …and names the object an operator would edit, which for a policy-governed \
             namespace is the policy; got {message}"
        );
        assert!(
            message.contains(KEY_ID_LIVE),
            "{label}: …and the key; got {message}"
        );
    }

    // THE ACTIVE CONTROL. The same fixture with an untouched key materialises,
    // so every row above is about the lifecycle and nothing else.
    assert!(approval_bundle_config_map(
        &restore(),
        &approval(true),
        &governing_policy(policy_approver_key()),
        now()
    )
    .is_ok());
}

/// **F1.** A key the resolved trust does not carry refuses, and the message
/// names the RESOLVED source rather than always the roster.
///
/// KILLS: the pre-fix message `"absent from the TrustRoster"` — for a
/// policy-governed namespace it sends the operator to the wrong object.
#[test]
fn a_bundle_refusal_names_the_object_an_operator_would_edit() {
    let mut stranger = policy_approver_key();
    stranger.key_id =
        "sha256:0000000000000000000000000000000000000000000000000000000000000000".to_string();
    let error = approval_bundle_config_map(
        &restore(),
        &approval(true),
        &governing_policy(stranger),
        now(),
    )
    .expect_err("the policy carries no key with the id the verdict named");
    let message = error.to_string();
    assert!(
        message.contains("the TrustPolicy 'org-default' does not carry"),
        "got {message}"
    );
    assert!(
        message.contains("no longer resolves to"),
        "the sentence says WHY the roster is the wrong place to add it; got {message}"
    );

    // AND THE LEGACY PATH STILL SAYS `TrustRoster`, because for a roster-only
    // cluster that IS the object to edit.
    let mut legacy_roster = roster();
    legacy_roster.spec.approver_keys.clear();
    let error = approval_bundle_config_map(
        &restore(),
        &approval(true),
        &weirkeeper::trust::synthesize_legacy(&legacy_roster.spec),
        now(),
    )
    .expect_err("an empty roster carries no approver key either");
    assert!(
        error
            .to_string()
            .contains("the TrustRoster 'default' does not carry"),
        "got {error}"
    );
}
