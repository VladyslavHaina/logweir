//! **The `Switchover` reconciler over a recording API server**: what one pass
//! creates and writes.
//!
//! * **The bundle (review M1).** The Job mounts `<sw>-switchover` by name, so a
//!   ConfigMap already there is used only when it is exactly this admission's:
//!   one controller owner (this Switchover UID), immutable, every member byte
//!   for byte. Anything else ends the run `ApprovalBundleConflict` and no Job
//!   is created.
//! * **One admission (review L6).** `Admitted=True` is written before the Job
//!   and is the only admission the approval controller reads. A Job named after
//!   the Switchover that no admission created is not adopted, and a recorded
//!   Job that is gone ends the run: it is never admitted again.

use std::sync::Arc;

use logweir_core::approval_policy::ApprovalPolicySet;
use logweir_core::switchover::{self as so, Expected, PlanGroup, PlanTopic, RestoreRef};
use serde_json::{json, Value};
use weirkeeper::approval_policy::PolicySource;
use weirkeeper::controllers::switchover::{self as sw, JobStep};
use weirkeeper::controllers::Context;
use weirkeeper::crds::switchover::Switchover;
use weirkeeper::job::RunnerImage;
use weirkeeper::testing::{mock_client_recording_bodies, Route, SeenBody};

const NS: &str = "logweir-t20";
const SW: &str = "sw1";
const SW_UID: &str = "5a5a5a5a-0000-4000-8000-00000000005a";
const RESTORE: &str = "r1";
const RESTORE_UID: &str = "6b6b6b6b-0000-4000-8000-00000000006b";
const APPROVAL: &str = "a1";
const KEY_ID: &str = "4c3f8e2a91b0d7c65e14f2a3b8d9c0e1f2a3b4c5d6e7f8091a2b3c4d5e6f7081";
const BUNDLE: &str = "sw1-switchover";

/// The restore plan (restore_controller's fixture, `newTopic`).
const RESTORE_PLAN: &str = "\
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
  mode: newTopic
  topic_naming:
    prefix: \"incident-4471-\"
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

fn sha(c: char) -> String {
    format!("sha256:{}", c.to_string().repeat(64))
}

fn plan_bytes() -> String {
    let plan = so::SwitchoverPlan {
        format_version: so::PLAN_FORMAT_VERSION.into(),
        restore: RestoreRef {
            name: RESTORE.into(),
            uid: RESTORE_UID.into(),
            plan_hash: sha('a'),
            scorecard_sha256: sha('b'),
            mapping_sha256: sha('c'),
        },
        rollback_of: None,
        groups: vec![PlanGroup {
            source: "app".into(),
            target: "app".into(),
            topics: [(
                "incident-4471-orders".to_string(),
                PlanTopic {
                    offsets: [(0, 40)].into(),
                    approximate: vec![],
                    expected: Expected::none(),
                },
            )]
            .into(),
        }],
    };
    serde_yaml::to_string(&plan).expect("the plan renders")
}

fn switchover(status: Value) -> Value {
    json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "Switchover",
        "metadata": {"name": SW, "namespace": NS, "uid": SW_UID, "generation": 1,
                     "resourceVersion": "100", "creationTimestamp": "2026-09-09T12:59:00Z"},
        "spec": {"planBytes": plan_bytes(), "approvalRef": {"name": APPROVAL},
                 "restoreRef": {"name": RESTORE, "uid": RESTORE_UID}, "deadlineSeconds": 600},
        "status": status
    })
}

fn restore() -> Value {
    json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "Restore",
        "metadata": {"name": RESTORE, "namespace": NS, "uid": RESTORE_UID},
        "spec": {"planBytes": RESTORE_PLAN, "approvalRef": {"name": "r1-approval"},
                 "sourceArchive": {"url": "s3://kafka-backups/drill-demo"}, "backupSetRef": "b1",
                 "pointInTime": "2026-09-07T14:05:00Z",
                 "target": {"clusterRef": {"name": "scratch"}, "mode": "newTopic",
                            "topicNaming": {"prefix": "incident-4471-"}},
                 "deadlineSeconds": 600},
        "status": {"phase": "Succeeded",
                   "evidence": {"scorecardKey": "logweir/drills/R.json",
                                "verification": {"result": "Valid"}}}
    })
}

fn approval() -> Value {
    json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "Approval",
        "metadata": {"name": APPROVAL, "namespace": NS, "uid": "approval-uid-1",
                     "creationTimestamp": "2026-09-09T12:59:30Z"},
        "spec": {"subjectRef": {"kind": "Switchover", "name": SW},
                 "planHash": logweir_core::ids::sha256_prefixed(plan_bytes().as_bytes()),
                 "approvalBytes": "{\"signed\":\"approval\"}", "sidecarBytes": "{\"sig\":1}"},
        "status": {"verified": true, "matchedKeyId": KEY_ID,
                   "verifiedSubjectRef": {"apiVersion": "logweir.dev/v1alpha1",
                                          "kind": "Switchover", "name": SW, "namespace": NS,
                                          "uid": SW_UID}}
    })
}

fn cluster() -> Value {
    json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "KafkaCluster",
        "metadata": {"name": "scratch", "namespace": NS, "uid": "cluster-uid"},
        "spec": {"bootstrapServers": ["scratch-0.logweir-t20:9092"],
                 "auth": {"mode": "plaintext", "tls": false},
                 "role": "scratch", "markerTopic": "logweir.scratch"},
        "status": {"reachable": true, "clusterId": "MkU3OEVBNTcwNTJENDM2Qk"}
    })
}

fn roster() -> Value {
    json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "TrustRoster",
        "metadata": {"name": "default", "uid": "roster-uid"},
        "spec": {"approverKeys": [{"keyId": KEY_ID,
                    "spkiPem": "-----BEGIN PUBLIC KEY-----\nA\n-----END PUBLIC KEY-----\n"}],
                 "signingKeys": [], "allowedClusterIds": ["MkU3OEVBNTcwNTJENDM2Qk"]},
        "status": {"loaded": true, "expiredKeyIds": []}
    })
}

fn job() -> Value {
    json!({
        "apiVersion": "batch/v1", "kind": "Job",
        "metadata": {"name": SW, "namespace": NS, "uid": "job-uid"},
        "spec": {"template": {"spec": {"containers": [], "restartPolicy": "Never"}}},
        "status": {"active": 1}
    })
}

fn route(method: &'static str, path_suffix: &'static str, status: u16, body: Value) -> Route {
    Route {
        method,
        path_suffix,
        status,
        body: body.to_string(),
    }
}

/// Every read and write an admitting pass may make. `job` answers the Job
/// read; `bundle` the bundle's create (`None`: created) and read.
fn routes(job_found: Option<Value>, existing_bundle: Option<Value>) -> Vec<Route> {
    let not_found = json!({"kind": "Status", "apiVersion": "v1", "status": "Failure",
                           "reason": "NotFound", "code": 404});
    let conflict = json!({"kind": "Status", "apiVersion": "v1", "status": "Failure",
                          "reason": "AlreadyExists", "code": 409});
    let mut table = vec![
        match job_found {
            Some(j) => route("GET", "/jobs/sw1", 200, j),
            None => route("GET", "/jobs/sw1", 404, not_found.clone()),
        },
        route("GET", "/restores/r1", 200, restore()),
        route(
            "GET",
            "/switchovers",
            200,
            json!({"apiVersion": "logweir.dev/v1alpha1", "kind": "SwitchoverList",
                   "metadata": {"resourceVersion": "1"}, "items": [switchover(json!({}))]}),
        ),
        route("GET", "/approvals/a1", 200, approval()),
        route("GET", "/kafkaclusters/scratch", 200, cluster()),
        route(
            "GET",
            "/trustpolicies",
            200,
            json!({"apiVersion": "logweir.dev/v1alpha1", "kind": "TrustPolicyList",
                   "metadata": {"resourceVersion": "1"}, "items": []}),
        ),
        route("GET", "/trustrosters/default", 200, roster()),
        route("POST", "/jobs", 201, job()),
        route(
            "PATCH",
            "/switchovers/sw1/status",
            200,
            switchover(json!({})),
        ),
    ];
    match existing_bundle {
        None => table.push(route(
            "POST",
            "/configmaps",
            201,
            json!({"apiVersion": "v1", "kind": "ConfigMap",
                   "metadata": {"name": BUNDLE, "namespace": NS}}),
        )),
        Some(existing) => {
            table.push(route("POST", "/configmaps", 409, conflict));
            table.push(route("GET", "/configmaps/sw1-switchover", 200, existing));
        }
    }
    table
}

async fn pass(status: Value, table: Vec<Route>) -> Vec<SeenBody> {
    let (client, _recorder, bodies) = mock_client_recording_bodies(table);
    let ctx = Arc::new(Context {
        client,
        archive: None,
        runner_image: RunnerImage::default(),
    });
    let object: Switchover = serde_json::from_value(switchover(status)).expect("a Switchover");
    sw::reconcile(
        Arc::new(object),
        ctx,
        PolicySource::fixed(Arc::new(ApprovalPolicySet::default())),
    )
    .await
    .expect("the pass completes");
    let seen = bodies.lock().expect("readable").clone();
    seen
}

fn path_of(b: &SeenBody) -> &str {
    b.uri.split('?').next().unwrap_or(&b.uri)
}

fn sent(bodies: &[SeenBody], method: &str, suffix: &str) -> Vec<Value> {
    bodies
        .iter()
        .filter(|b| b.method == method && path_of(b).ends_with(suffix))
        .map(|b| serde_json::from_str(&b.body).expect("a JSON body"))
        .collect()
}

fn statuses(bodies: &[SeenBody]) -> Vec<Value> {
    sent(bodies, "PATCH", "/switchovers/sw1/status")
        .into_iter()
        .map(|p| p["status"].clone())
        .collect()
}

fn admitted(status: &Value) -> bool {
    status["conditions"].as_array().is_some_and(|cs| {
        cs.iter()
            .any(|c| c["type"] == "Admitted" && c["status"] == "True")
    })
}

/// The bundle this admission writes, as the first pass sent it.
async fn own_bundle() -> Value {
    let bodies = pass(json!({}), routes(None, None)).await;
    let mut bundle = sent(&bodies, "POST", "/configmaps")
        .pop()
        .expect("the first pass creates the bundle");
    bundle["metadata"]["creationTimestamp"] = json!("2026-09-09T13:00:58Z");
    bundle
}

/// **The admitting pass** writes the bundle, then `Admitted=True`, then the
/// Job, then `jobRef`, in that order. The marker is the admission the
/// approval controller reads; it precedes the Job, so a Job never exists for
/// an admission that was not recorded.
#[tokio::test]
async fn an_admitted_switchover_marks_its_admission_before_its_job() {
    let bodies = pass(json!({}), routes(None, None)).await;
    let order: Vec<String> = bodies
        .iter()
        .filter(|b| b.method != "GET")
        .map(|b| {
            format!(
                "{} {}",
                b.method,
                path_of(b).rsplit('/').next().unwrap_or("")
            )
        })
        .collect();
    assert_eq!(
        order,
        [
            "POST configmaps",
            "PATCH status",
            "POST jobs",
            "PATCH status"
        ],
        "{order:?}"
    );
    let written = statuses(&bodies);
    assert!(admitted(&written[0]), "{:?}", written[0]);
    assert!(
        written[0]["jobRef"].is_null(),
        "no Job yet: {:?}",
        written[0]
    );
    assert_eq!(written[1]["jobRef"], json!(SW));
    assert!(
        written[1].get("conditions").is_none(),
        "the Job's patch leaves the marker as written: {:?}",
        written[1]
    );
}

/// **M1: a bundle already there is used only when it is this admission's.**
///
/// CONTROL: the byte-identical own bundle (the pass that stopped after
/// creating it) still proceeds to the Job.
#[tokio::test]
async fn a_bundle_that_is_not_this_admissions_is_refused_and_no_job_is_created() {
    let own = own_bundle().await;

    let mut differing = own.clone();
    differing["data"]["approval.json"] = json!("{\"signed\":\"someone else's plan\"}");
    let mut foreign_owner = own.clone();
    foreign_owner["metadata"]["ownerReferences"][0]["uid"] = json!("another-switchover");
    let mut two_owners = own.clone();
    let extra = own["metadata"]["ownerReferences"][0].clone();
    two_owners["metadata"]["ownerReferences"] = json!([extra.clone(), {
        "apiVersion": "v1", "kind": "ConfigMap", "name": "keeper", "uid": "keeper-uid"}]);
    let mut mutable = own.clone();
    mutable["immutable"] = json!(false);
    let mut extra_member = own.clone();
    extra_member["data"]["approver.pub.pem"] =
        json!("-----BEGIN PUBLIC KEY-----\nPLANTED\n-----END PUBLIC KEY-----\n");

    for (case, existing, member) in [
        ("one differing member", differing, Some("approval.json")),
        ("another owner", foreign_owner, None),
        ("a second owner", two_owners, None),
        ("not immutable", mutable, None),
        (
            "a planted approver key",
            extra_member,
            Some("approver.pub.pem"),
        ),
    ] {
        let bodies = pass(json!({}), routes(None, Some(existing))).await;
        assert!(
            sent(&bodies, "POST", "/jobs").is_empty(),
            "{case}: no Job is created over a bundle that is not this admission's"
        );
        let written = statuses(&bodies);
        let last = written.last().expect("the refusal");
        assert_eq!(last["phase"], json!("Refused"), "{case}: {last:?}");
        assert_eq!(last["reason"], json!("ApprovalBundleConflict"), "{case}");
        assert!(
            written.iter().all(|s| !admitted(s)),
            "{case}: no admission is recorded, so no Approval is consumed"
        );
        if let Some(member) = member {
            assert!(
                last["message"].as_str().is_some_and(|m| m.contains(member)),
                "{case}: the refusal names the member: {last:?}"
            );
        }
    }

    // CONTROL: the controller's own bundle, byte for byte.
    let bodies = pass(json!({}), routes(None, Some(own))).await;
    assert_eq!(
        sent(&bodies, "POST", "/jobs").len(),
        1,
        "its own bundle proceeds"
    );
    assert!(statuses(&bodies).iter().any(admitted));
}

/// **L6: a run whose Job is gone is never admitted again.** The admission
/// recorded the Job and the Job was deleted before it finished: the run ends
/// `Failed`, `SwitchoverJobLost`, with no read of the Approval and no Job.
/// An older build's run (a recorded Job, no marker) is ended the same way.
#[tokio::test]
async fn a_switchover_whose_job_is_deleted_ends_and_is_not_readmitted() {
    let marker = json!([{"type": "Admitted", "status": "True", "reason": "Admitted",
                          "lastTransitionTime": "2026-09-09T13:01:00Z"}]);
    for (case, status) in [
        (
            "admitted, Job recorded",
            json!({"phase": "Running", "jobRef": SW, "conditions": marker}),
        ),
        (
            "an older build's run",
            json!({"phase": "Running", "jobRef": SW}),
        ),
    ] {
        // Only the Job read and the status write: an admission would read the
        // Restore and the Approval, which this table does not serve.
        let not_found = json!({"kind": "Status", "apiVersion": "v1", "status": "Failure",
                               "reason": "NotFound", "code": 404});
        let table = vec![
            route("GET", "/jobs/sw1", 404, not_found),
            route(
                "PATCH",
                "/switchovers/sw1/status",
                200,
                switchover(json!({})),
            ),
        ];
        let bodies = pass(status, table).await;
        let written = statuses(&bodies);
        assert_eq!(written.len(), 1, "{case}");
        assert_eq!(written[0]["phase"], json!("Failed"), "{case}");
        assert_eq!(written[0]["reason"], json!(sw::JOB_LOST), "{case}");
        assert_eq!(written[0]["jobRef"], json!(SW), "{case}");
    }

    // CONTROL: admitted, the Job not yet created (the pass stopped between the
    // marker and the Job): that one admission completes its Job.
    let bodies = pass(
        json!({"phase": "Running", "reason": "Admitted", "conditions": marker}),
        routes(None, Some(own_bundle().await)),
    )
    .await;
    assert_eq!(sent(&bodies, "POST", "/jobs").len(), 1);
    let kept = statuses(&bodies)
        .into_iter()
        .find(admitted)
        .expect("the marker is written again");
    assert_eq!(
        kept["conditions"][0]["lastTransitionTime"],
        json!("2026-09-09T13:01:00Z"),
        "the admission instant does not move"
    );
}

/// **L6: a Job no admission created is not adopted.** A Job named after a
/// Switchover that was never admitted holds it `Pending`
/// (`SwitchoverJobNotAdmitted`): no `jobRef`, no marker, so no Approval is
/// consumed by it.
#[tokio::test]
async fn a_job_someone_else_created_is_not_adopted_and_admits_nothing() {
    let table = vec![
        route("GET", "/jobs/sw1", 200, job()),
        route(
            "PATCH",
            "/switchovers/sw1/status",
            200,
            switchover(json!({})),
        ),
    ];
    let bodies = pass(json!({"phase": "Pending"}), table).await;
    let written = statuses(&bodies);
    assert_eq!(written.len(), 1);
    assert_eq!(written[0]["phase"], json!("Pending"));
    assert_eq!(written[0]["reason"], json!(sw::JOB_NOT_ADMITTED));
    assert!(written[0]["jobRef"].is_null() && !admitted(&written[0]));

    // CONTROL: the run's own Job (the marker is there) is observed.
    let marker = json!([{"type": "Admitted", "status": "True", "reason": "Admitted",
                          "lastTransitionTime": "2026-09-09T13:01:00Z"}]);
    let table = vec![
        route("GET", "/jobs/sw1", 200, job()),
        route(
            "PATCH",
            "/switchovers/sw1/status",
            200,
            switchover(json!({})),
        ),
    ];
    let bodies = pass(
        json!({"phase": "Running", "reason": "Admitted", "conditions": marker}),
        table,
    )
    .await;
    let written = statuses(&bodies);
    assert_eq!(written[0]["jobRef"], json!(SW), "recorded once observed");
    assert!(admitted(&written[0]), "and the marker kept");
}

/// [`sw::job_step`], tabled.
#[test]
fn the_job_step_reads_the_marker_and_the_recorded_job() {
    let status = |v: Value| -> weirkeeper::crds::switchover::SwitchoverStatus {
        serde_json::from_value(v).expect("a status")
    };
    let marker = json!([{"type": "Admitted", "status": "True", "reason": "Admitted",
                          "lastTransitionTime": "2026-09-09T13:01:00Z"}]);
    let held = json!([{"type": "Admitted", "status": "False", "reason": "ApprovalNotVerified",
                        "lastTransitionTime": "2026-09-09T13:01:00Z"}]);
    for (case, s, job, want) in [
        ("nothing", json!({}), false, JobStep::Admit),
        ("a stranger's Job", json!({}), true, JobStep::NotAdmitted),
        (
            "a False marker is none",
            json!({"conditions": held}),
            true,
            JobStep::NotAdmitted,
        ),
        (
            "admitted, its Job",
            json!({"conditions": marker}),
            true,
            JobStep::Observe,
        ),
        (
            "admitted, Job not yet made",
            json!({"conditions": marker}),
            false,
            JobStep::Admit,
        ),
        (
            "admitted, Job gone",
            json!({"conditions": marker, "jobRef": SW}),
            false,
            JobStep::Lost,
        ),
        (
            "an older build's run",
            json!({"jobRef": SW}),
            true,
            JobStep::Observe,
        ),
        (
            "an older build's run, Job gone",
            json!({"jobRef": SW}),
            false,
            JobStep::Lost,
        ),
    ] {
        assert_eq!(sw::job_step(Some(&status(s)), job), want, "{case}");
    }
}
