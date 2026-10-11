//! **FX-34 (review finding MED-3): what the pass that CREATES a runner Job
//! logs, and that the Job's line token is in none of it.**
//!
//! From `job::add_line_token` on, the token is a plain `String` among the
//! Job's arguments. The static row in `tests/line_token.rs` counts every
//! mention of `LineToken::expose_token`, and cannot see code that logs the
//! JOB: a `?desired_job` on a debug line never names the accessor. That is
//! the likeliest way for the token to reach a log, so the rows here run a
//! real create pass of each kind with every event captured, at every level
//! and from every target, and hold the POSTed Job's token out of all of them.
//!
//! # Every row here installs a capturing subscriber
//!
//! `tracing` caches a callsite's interest the first time the callsite is hit.
//! A binary that mixes capturing rows with rows that install no subscriber can
//! cache "never" and drop the capturing rows' events (FX-37). So this file
//! holds ONLY rows that capture, as `tests/refusal_read.rs` does, and the rows
//! about what a create pass WRITES stay in `tests/restore_controller.rs` and
//! `tests/backup_controller.rs`
//! (`a_created_job_carries_a_fresh_line_token_and_nothing_else_does`).
//!
//! # The fixtures
//!
//! The smallest copies of those two files' create-pass fixtures: one object
//! of each kind, the objects its admission reads, and a route for every
//! request the pass makes. The recording double panics on an unrouted
//! request, so a fixture that drifted from the reconciler fails loudly here.

use std::io::Write;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, TimeZone, Utc};
use k8s_openapi::api::batch::v1::Job;
use logweir_core::ids::sha256_prefixed;
use serde_json::Value;
use weirkeeper::controllers::backup::{reconcile_backup, unobserved_archive};
use weirkeeper::controllers::restore::{
    approval_bundle_config_map, plan_config_map, reconcile_restore, unobserved_scorecard,
};
use weirkeeper::crds::backup::Backup;
use weirkeeper::crds::restore::Restore;
use weirkeeper::testing::{mock_client_recording_bodies, Route, SeenBody};
use weirkeeper::verification::unverified_evidence;

// ---------------------------------------------------------------------------
// The capture
// ---------------------------------------------------------------------------

#[derive(Clone, Default)]
struct Sink(Arc<Mutex<Vec<u8>>>);

impl Write for Sink {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Every event emitted on this thread while it lives: at TRACE and above,
/// from EVERY target (this crate's and the client library's), as the JSON
/// lines the controller binary's own subscriber writes.
struct Capture {
    sink: Sink,
    _guard: tracing::subscriber::DefaultGuard,
}

impl Capture {
    fn start() -> Self {
        let sink = Sink::default();
        let writer = sink.clone();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .with_max_level(tracing::Level::TRACE)
            .with_writer(move || writer.clone())
            .finish();
        Self {
            sink,
            _guard: tracing::subscriber::set_default(subscriber),
        }
    }

    /// The captured text, exactly as written: the search for the token runs
    /// over these bytes, so no parsing stands between a leak and the row.
    fn text(&self) -> String {
        String::from_utf8(self.sink.0.lock().unwrap().clone()).expect("the formatter writes UTF-8")
    }

    fn events(&self) -> Vec<Value> {
        self.text()
            .lines()
            .map(|l| serde_json::from_str(l).expect("the json formatter writes one object a line"))
            .collect()
    }
}

/// `(level, target, message)` of every event, with `token` blanked, so a
/// failing row can say which event it was without printing the value.
fn summary(events: &[Value], token: &str) -> Vec<String> {
    events
        .iter()
        .map(|e| {
            format!(
                "{} {} {}",
                e["level"].as_str().unwrap_or("?"),
                e["target"].as_str().unwrap_or("?"),
                e["fields"].to_string().replace(token, "<the token>")
            )
        })
        .collect()
}

/// The messages this crate logged at `level`.
fn ours_at(events: &[Value], level: &str) -> Vec<String> {
    events
        .iter()
        .filter(|e| e["level"] == level)
        .filter(|e| {
            e["target"]
                .as_str()
                .is_some_and(|t| t.starts_with("weirkeeper"))
        })
        .map(|e| {
            e["fields"]["message"]
                .as_str()
                .unwrap_or_default()
                .to_string()
        })
        .collect()
}

fn path(uri: &str) -> &str {
    uri.split('?').next().unwrap_or(uri)
}

/// The line token of the one Job the pass POSTed, read the way the controller
/// reads it back: off the Job (`weirkeeper::job::line_token`).
fn posted_jobs_token(seen: &[SeenBody]) -> String {
    let posts: Vec<&SeenBody> = seen
        .iter()
        .filter(|b| b.method == "POST" && path(&b.uri).ends_with("/jobs"))
        .collect();
    assert_eq!(posts.len(), 1, "the pass POSTs one Job");
    let job: Job = serde_json::from_str(&posts[0].body).expect("a Job");
    let token = weirkeeper::job::line_token(&job)
        .expect("the created Job carries a line token")
        .expose_token()
        .to_string();
    assert_eq!(token.len(), 40, "160 bits as hex");
    token
}

/// What both rows assert once a create pass has run under `capture`.
///
/// * THE CONTROL, three ways: the pass logged its creation line at INFO
///   (`created`); the capture holds a TRACE event (so "no event at TRACE"
///   is a statement about a capture that sees TRACE); and the same search,
///   over the same capture, FINDS the token once an event carrying it is
///   emitted on purpose (so the search can see what it is looking for).
/// * THE PROPERTY: before that seeded event, the token is in no byte of
///   anything any target logged, and the pass warned about nothing.
fn assert_the_pass_logged_and_never_the_token(capture: &Capture, token: &str, created: &str) {
    tracing::trace!(
        target: "weirkeeper::create_pass_log_row",
        "the capture sees TRACE"
    );
    let text = capture.text();
    let events = capture.events();
    let info = ours_at(&events, "INFO");
    assert!(
        info.iter().any(|m| m.starts_with(created)),
        "the control: the create pass logged `{created}`: {info:?}"
    );
    assert_eq!(
        ours_at(&events, "TRACE"),
        vec!["the capture sees TRACE".to_string()],
        "the control: the capture is at TRACE"
    );
    assert!(
        !text.contains(token),
        "the Job's line token is in what the create pass logged: {:#?}",
        summary(
            &events
                .iter()
                .filter(|e| e.to_string().contains(token))
                .cloned()
                .collect::<Vec<_>>(),
            token
        )
    );
    assert_eq!(
        ours_at(&events, "WARN"),
        Vec::<String>::new(),
        "and a create pass that worked warned about nothing"
    );

    // NEGATIVE CONTROL: the search sees the token when an event carries it,
    // in the shape the likeliest leak has (the Job's arguments, Debug-printed).
    let args = vec!["--line-token".to_string(), token.to_string()];
    tracing::debug!(
        target: "weirkeeper::create_pass_log_row",
        runner_args = ?args,
        "seeded on purpose"
    );
    assert!(
        capture.text().contains(token),
        "the search would have seen a logged token"
    );
}

fn utc(y: i32, m: u32, d: u32, h: u32, min: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(y, m, d, h, min, 0)
        .single()
        .expect("the fixture instant exists")
}

fn not_found_body(kind: &str, name: &str) -> String {
    format!(
        r#"{{"kind":"Status","apiVersion":"v1","status":"Failure",
  "message":"{kind} \"{name}\" not found","reason":"NotFound","code":404}}"#
    )
}

// ---------------------------------------------------------------------------
// Restore
// ---------------------------------------------------------------------------

mod restore_fixture {
    use super::*;

    pub const NS: &str = "logweir-t20";
    pub const NAME: &str = "logweir-restore-incident-4471";
    const UID: &str = "5c2e7b91-0000-4000-8000-0000000000a2";
    const JOB_UID: &str = "bbbbbbbb-0000-4000-8000-0000000000b2";
    const CLUSTER_UID: &str = "8b3c1d2e-0000-4000-8000-0000000000c2";
    const APPROVAL: &str = "a1";

    /// `spec.planBytes`: the runner's own `restore.yaml` grammar.
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

    pub fn now() -> DateTime<Utc> {
        utc(2026, 9, 10, 12, 0)
    }

    fn restore_json() -> String {
        let plan = serde_json::to_string(PLAN_BYTES).expect("planBytes is a JSON string");
        format!(
            r#"{{
  "apiVersion": "logweir.dev/v1alpha1",
  "kind": "Restore",
  "metadata": {{ "name": "{NAME}", "namespace": "{NS}", "uid": "{UID}", "generation": 2,
                  "resourceVersion": "4071" }},
  "spec": {{
    "planBytes": {plan},
    "approvalRef": {{ "name": "{APPROVAL}" }},
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

    pub fn restore() -> Restore {
        serde_json::from_str(&restore_json()).expect("the fixture is a Restore")
    }

    /// The roster's one approver key id. Assembled at run time: it is a
    /// public key's digest, and no source line needs to hold its shape.
    fn key_id() -> String {
        format!("sha256:{}", "1".repeat(64))
    }

    /// A verified `Approval` whose signed document names the plan's hash.
    fn approval_json() -> String {
        let plan_hash = sha256_prefixed(PLAN_BYTES.as_bytes());
        let key_id = key_id();
        let doc = serde_json::to_string(&format!(
            r#"{{"approver":"sre-oncall@example.com","ticket":"CHG-40881",
  "plan_hash":"{plan_hash}","subject_kind":"Restore"}}"#
        ))
        .expect("the doc is a JSON string");
        format!(
            r#"{{
  "apiVersion": "logweir.dev/v1alpha1",
  "kind": "Approval",
  "metadata": {{ "name": "{APPROVAL}", "namespace": "{NS}", "uid": "aaaaaaaa-0000-4000-8000-00000000000a" }},
  "spec": {{
    "subjectRef": {{ "kind": "Restore", "name": "{NAME}" }},
    "planHash": "{plan_hash}",
    "approvalBytes": {doc},
    "sidecarBytes": "{{}}"
  }},
  "status": {{
    "verified": true,
    "matchedKeyId": "{key_id}",
    "verifiedSubjectRef": {{ "apiVersion": "logweir.dev/v1alpha1", "kind": "Restore", "name": "{NAME}", "namespace": "{NS}", "uid": "{UID}" }},
    "conditions": [{{ "type": "Verified", "status": "True", "reason": "Verified" }}]
  }}
}}"#
        )
    }

    fn cluster_json() -> String {
        format!(
            r#"{{
  "apiVersion": "logweir.dev/v1alpha1",
  "kind": "KafkaCluster",
  "metadata": {{ "name": "scratch", "namespace": "{NS}", "uid": "{CLUSTER_UID}" }},
  "spec": {{
    "bootstrapServers": ["scratch-0.logweir-t20:9092"],
    "auth": {{ "mode": "plaintext", "tls": false }},
    "role": "scratch",
    "markerTopic": "logweir.scratch"
  }},
  "status": {{ "reachable": true, "clusterId": "MkU3OEVBNTcwNTJENDM2Qk" }}
}}"#
        )
    }

    fn roster_json() -> String {
        let key_id = key_id();
        format!(
            r#"{{
  "apiVersion": "logweir.dev/v1alpha1",
  "kind": "TrustRoster",
  "metadata": {{ "name": "default", "uid": "dddddddd-0000-4000-8000-00000000000d" }},
  "spec": {{
    "approverKeys": [
      {{ "keyId": "{key_id}", "spkiPem": "-----BEGIN PUBLIC KEY-----\nA\n-----END PUBLIC KEY-----\n" }}
    ],
    "signingKeys": [],
    "allowedClusterIds": ["MkU3OEVBNTcwNTJENDM2Qk"]
  }},
  "status": {{ "loaded": true, "expiredKeyIds": [] }}
}}"#
        )
    }

    fn running_job_body() -> String {
        format!(
            r#"{{"apiVersion":"batch/v1","kind":"Job",
  "metadata":{{"name":"{NAME}","namespace":"{NS}","uid":"{JOB_UID}",
    "ownerReferences":[{{"apiVersion":"logweir.dev/v1alpha1","kind":"Restore","name":"{NAME}","uid":"{UID}","controller":true,"blockOwnerDeletion":true}}]}},
  "spec":{{"template":{{"spec":{{"containers":[],"restartPolicy":"Never"}}}}}},
  "status":{{"active":1}}}}"#
        )
    }

    fn plan_config_map_body() -> String {
        serde_json::to_string(&plan_config_map(&restore()).expect("a plan ConfigMap"))
            .expect("it serializes")
    }

    fn approval_bundle_body() -> String {
        let approval = serde_json::from_str(&approval_json()).expect("an Approval");
        let roster: weirkeeper::crds::trust_roster::TrustRoster =
            serde_json::from_str(&roster_json()).expect("a TrustRoster");
        let trust = weirkeeper::trust::synthesize_legacy(&roster.spec);
        serde_json::to_string(
            &approval_bundle_config_map(&restore(), &approval, &trust, now())
                .expect("an approval bundle"),
        )
        .expect("it serializes")
    }

    /// Every request an admitted create pass makes.
    pub fn create_routes() -> Vec<Route> {
        let route =
            |method: &'static str, path_suffix: &'static str, status: u16, body: String| Route {
                method,
                path_suffix,
                status,
                body,
            };
        vec![
            route(
                "GET",
                "/jobs/logweir-restore-incident-4471",
                404,
                not_found_body("jobs.batch", NAME),
            ),
            route("GET", "/approvals/a1", 200, approval_json()),
            route("GET", "/kafkaclusters/scratch", 200, cluster_json()),
            route(
                "GET",
                "/trustpolicies",
                200,
                r#"{"apiVersion":"logweir.dev/v1alpha1","kind":"TrustPolicyList",
                    "metadata":{"resourceVersion":"1"},"items":[]}"#
                    .to_string(),
            ),
            route("GET", "/trustrosters/default", 200, roster_json()),
            route("POST", "/configmaps", 201, plan_config_map_body()),
            route(
                "GET",
                "/configmaps/logweir-restore-incident-4471-plan",
                200,
                plan_config_map_body(),
            ),
            route(
                "GET",
                "/configmaps/logweir-restore-incident-4471-approval-bundle",
                200,
                approval_bundle_body(),
            ),
            route("POST", "/jobs", 201, running_job_body()),
            route(
                "PATCH",
                "/restores/logweir-restore-incident-4471/status",
                200,
                restore_json(),
            ),
        ]
    }
}

/// **A `Restore`'s create pass logs its creation and never the Job's line
/// token**, at any level, from any target.
///
/// KILLS: the Job, its pod template or its runner arguments logged on the
/// create pass (the review's mutant R4b: the runner's arguments at `warn`
/// just before the `POST`).
#[tokio::test]
async fn a_restores_create_pass_logs_its_creation_and_never_the_jobs_line_token() {
    let capture = Capture::start();
    let (client, _requests, bodies) =
        mock_client_recording_bodies(restore_fixture::create_routes());
    let outcome = reconcile_restore(
        &restore_fixture::restore(),
        &client,
        &unobserved_scorecard,
        &unverified_evidence,
        restore_fixture::now(),
    )
    .await
    .expect("the Restore is admitted and its Job created");
    assert_eq!(outcome.job_name, restore_fixture::NAME);
    let seen = bodies.lock().expect("readable").clone();
    let token = posted_jobs_token(&seen);
    assert_the_pass_logged_and_never_the_token(&capture, &token, "created the runner Job");
}

// ---------------------------------------------------------------------------
// Backup
// ---------------------------------------------------------------------------

mod backup_fixture {
    use super::*;

    pub const NS: &str = "logweir-t17";
    pub const NAME: &str = "logweir-backup-nightly-20261109-031700";
    const UID: &str = "3f1c8a5e-0000-4000-8000-0000000000a1";
    const JOB_UID: &str = "bbbbbbbb-0000-4000-8000-0000000000b1";
    const CLUSTER_UID: &str = "7a2b9c1d-0000-4000-8000-0000000000c1";

    pub fn now() -> DateTime<Utc> {
        utc(2026, 11, 9, 3, 17)
    }

    /// A MANUAL `Backup`: typed spec, the server-generated UID, no owner.
    fn backup_json() -> String {
        format!(
            r#"{{
  "apiVersion": "logweir.dev/v1alpha1",
  "kind": "Backup",
  "metadata": {{
    "name": "{NAME}",
    "namespace": "{NS}",
    "uid": "{UID}",
    "generation": 3,
    "resourceVersion": "4071"
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

    pub fn backup() -> Backup {
        serde_json::from_str(&backup_json()).expect("the fixture is a Backup")
    }

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
  "status": {{ "reachable": true, "clusterId": "MkU3OEVBNTcwNTJENDM2Qk" }}
}}"#
        )
    }

    fn plan_config_map_body() -> String {
        let cluster = serde_json::from_str(&kafka_cluster_json()).expect("a KafkaCluster");
        serde_json::to_string(
            &weirkeeper::controllers::backup::plan_config_map(&backup(), &cluster)
                .expect("a plan ConfigMap"),
        )
        .expect("it serializes")
    }

    /// The Job the API answers the create with: controlled by this `Backup`,
    /// and carrying the inputs digest of the plan ConfigMap the pass froze.
    fn running_job_body() -> String {
        let plan: Value = serde_json::from_str(&plan_config_map_body()).expect("JSON");
        let annotation = weirkeeper::backup_execution::INPUTS_SHA256_ANNOTATION;
        let digest = plan["metadata"]["annotations"][annotation]
            .as_str()
            .expect("the plan ConfigMap carries its inputs digest")
            .to_string();
        format!(
            r#"{{"apiVersion":"batch/v1","kind":"Job",
  "metadata":{{"name":"{NAME}","namespace":"{NS}","uid":"{JOB_UID}",
    "ownerReferences":[{{"apiVersion":"logweir.dev/v1alpha1","kind":"Backup","name":"{NAME}","uid":"{UID}","controller":true,"blockOwnerDeletion":true}}],
    "annotations":{{"{annotation}":"{digest}"}}}},
  "spec":{{"template":{{"spec":{{"containers":[],"restartPolicy":"Never"}}}}}},
  "status":{{"active":1}}}}"#
        )
    }

    /// Every request a create pass makes.
    pub fn create_routes() -> Vec<Route> {
        let route =
            |method: &'static str, path_suffix: &'static str, status: u16, body: String| Route {
                method,
                path_suffix,
                status,
                body,
            };
        vec![
            route(
                "GET",
                "/jobs/logweir-backup-nightly-20261109-031700",
                404,
                not_found_body("jobs.batch", NAME),
            ),
            route("GET", "/kafkaclusters/prod", 200, kafka_cluster_json()),
            route("POST", "/configmaps", 201, plan_config_map_body()),
            route(
                "GET",
                "/configmaps/logweir-backup-nightly-20261109-031700-plan",
                200,
                plan_config_map_body(),
            ),
            route("POST", "/jobs", 201, running_job_body()),
            route(
                "PATCH",
                "/backups/logweir-backup-nightly-20261109-031700/status",
                200,
                backup_json(),
            ),
        ]
    }
}

/// **A `Backup`'s create pass logs its creation and never the Job's line
/// token**, at any level, from any target.
///
/// KILLS: the Job, its pod template or its runner arguments logged on the
/// create pass.
#[tokio::test]
async fn a_backups_create_pass_logs_its_creation_and_never_the_jobs_line_token() {
    let capture = Capture::start();
    let (client, _requests, bodies) = mock_client_recording_bodies(backup_fixture::create_routes());
    // Boxed: the reconciler's future is large, and a test thread's stack is
    // not the controller's.
    Box::pin(reconcile_backup(
        &backup_fixture::backup(),
        &client,
        &unobserved_archive,
        &unverified_evidence,
        backup_fixture::now(),
    ))
    .await
    .expect("the create pass succeeds");
    let seen = bodies.lock().expect("readable").clone();
    let token = posted_jobs_token(&seen);
    assert_the_pass_logged_and_never_the_token(
        &capture,
        &token,
        "the runner Job runs the frozen execution inputs",
    );
}
