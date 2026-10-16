//! PROD-19.1 §4.1 at the controller: the workspace gate every namespaced
//! reconcile passes first (`weirkeeper::workspace::gate`).
//!
//! * C3: a labelled `dev` Backup with `dev` references is admitted; unlabelled,
//!   or labelled with a workspace the install does not list, it is
//!   `WorkspaceUnknown`.
//! * C4: one row per spec reference field: the referent in the same workspace
//!   is admitted, in another `WorkspaceMismatch`, the message naming both.
//! * Secrets: a Secret an object names (directly, or through its connection or
//!   destination) must carry the referrer's workspace. The signing key and the
//!   console's keys carry none, so a destination naming `logweir-signing-key`
//!   is refused and nothing can mount it.
//!
//! A refusal writes `phase: Failed` and one `Failed=True` condition, and
//! nothing else: the requests are asserted exactly, so a gate that went on to
//! create a ConfigMap or a Job would be seen.

use serde_json::{json, Value};
use weirkeeper::crds::approval::Approval;
use weirkeeper::crds::backup::Backup;
use weirkeeper::crds::backup_destination::BackupDestination;
use weirkeeper::crds::backup_schedule::BackupSchedule;
use weirkeeper::crds::kafka_cluster::KafkaCluster;
use weirkeeper::crds::recovery_catalog::RecoveryCatalog;
use weirkeeper::crds::restore::Restore;
use weirkeeper::crds::switchover::Switchover;
use weirkeeper::crds::topic_discovery::TopicDiscovery;
use weirkeeper::testing::{mock_client_recording_bodies, Route};
use weirkeeper::workspace::{self, Referent, Referrer, REASON_MISMATCH, REASON_UNKNOWN};

const NS: &str = "logweir";

fn labels(ws: Option<&str>) -> Value {
    ws.map_or(json!({}), |w| json!({ "logweir.dev/workspace": w }))
}

fn meta(name: &str, ws: Option<&str>) -> Value {
    json!({"name": name, "namespace": NS, "uid": format!("uid-{name}"), "generation": 1,
           "resourceVersion": "7", "labels": labels(ws)})
}

fn cluster(name: &str, ws: Option<&str>, secret: Option<&str>) -> Value {
    let mut auth = json!({"mode": "plaintext", "tls": false});
    if let Some(s) = secret {
        auth = json!({"mode": "scramSha512", "username": "u", "secretRef": {"name": s},
                      "tls": true});
    }
    json!({"apiVersion": "logweir.dev/v1alpha1", "kind": "KafkaCluster",
           "metadata": meta(name, ws),
           "spec": {"bootstrapServers": ["k:9092"], "auth": auth, "role": "source"}})
}

fn destination(name: &str, ws: Option<&str>, secret: &str) -> Value {
    json!({"apiVersion": "logweir.dev/v1alpha1", "kind": "BackupDestination",
    "metadata": meta(name, ws),
    "spec": {
        "storage": {"provider": "S3", "bucket": "b", "prefix": "p",
                    "endpoint": "http://minio:9000", "addressing": "PathStyle"},
        "transport": {"security": "InsecureHTTP"},
        "access": {"archiveWrite": {"mode": "SecretKeys", "secret": {
            "name": secret, "accessKeyIdKey": "a", "secretAccessKeyKey": "s"}}}
    }})
}

fn secret(name: &str, ws: Option<&str>) -> Value {
    json!({"apiVersion": "v1", "kind": "Secret", "metadata": meta(name, ws)})
}

fn backup(ws: Option<&str>) -> Backup {
    serde_json::from_value(backup_json(ws)).expect("a Backup")
}

fn backup_json(ws: Option<&str>) -> Value {
    json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "Backup",
        "metadata": meta("b1", ws),
        "spec": {"sourceRef": {"name": "source"}, "destinationRef": {"name": "primary"},
                 "topics": ["orders"], "archive": {"url": "s3://b/p"},
                 "triggeredBy": "manual", "deadlineSeconds": 900}
    })
}

fn get(suffix: &'static str, body: &Value) -> Route {
    Route {
        method: "GET",
        path_suffix: suffix,
        status: 200,
        body: body.to_string(),
    }
}

/// The status write, answered with the object as the API server returns it.
fn patch(suffix: &'static str, object: &Value) -> Route {
    Route {
        method: "PATCH",
        path_suffix: suffix,
        status: 200,
        body: object.to_string(),
    }
}

/// The world a `dev` Backup sees, with each object's workspace as given.
fn routes(cluster_ws: Option<&str>, dest_ws: Option<&str>, secret_ws: Option<&str>) -> Vec<Route> {
    vec![
        get(
            "/kafkaclusters/source",
            &cluster("source", cluster_ws, Some("source-cred")),
        ),
        get(
            "/backupdestinations/primary",
            &destination("primary", dest_ws, "dest-cred"),
        ),
        get("/secrets/source-cred", &secret("source-cred", Some("dev"))),
        get("/secrets/dest-cred", &secret("dest-cred", secret_ws)),
        patch("/backups/b1/status", &backup_json(Some("dev"))),
    ]
}

/// The verdict, the status the gate wrote (if any) and every method it used.
async fn run(object: &Backup, table: Vec<Route>) -> (bool, Option<Value>, Vec<String>) {
    let (client, seen, bodies) = mock_client_recording_bodies(table);
    let refused = workspace::gate(&client, object)
        .await
        .expect("every read is answered")
        .is_some();
    let status = bodies
        .lock()
        .unwrap()
        .iter()
        .find(|b| b.method == "PATCH")
        .map(|b| serde_json::from_str::<Value>(&b.body).expect("JSON")["status"].clone());
    let methods = seen
        .lock()
        .unwrap()
        .iter()
        .map(|r| r.method.clone())
        .collect();
    (refused, status, methods)
}

fn reason(status: &Option<Value>) -> String {
    status.as_ref().expect("a status was written")["conditions"][0]["reason"]
        .as_str()
        .unwrap_or_default()
        .to_string()
}

fn message(status: &Option<Value>) -> String {
    status.as_ref().expect("a status was written")["conditions"][0]["message"]
        .as_str()
        .unwrap_or_default()
        .to_string()
}

/// **C3.** A `dev` Backup whose connection, destination and both credentials
/// are `dev` is admitted and nothing is written. Unlabelled, or labelled `qa`
/// (not listed), it is `WorkspaceUnknown`, `phase: Failed`, and nothing is
/// read or created for it.
#[tokio::test]
async fn an_object_of_a_listed_workspace_passes_and_any_other_is_unknown() {
    let (refused, status, methods) = run(
        &backup(Some("dev")),
        routes(Some("dev"), Some("dev"), Some("dev")),
    )
    .await;
    assert!(!refused, "{status:?}");
    assert!(status.is_none());
    assert!(methods.iter().all(|m| m == "GET"), "{methods:?}");

    for ws in [None, Some("qa")] {
        let (refused, status, methods) =
            run(&backup(ws), routes(Some("dev"), Some("dev"), Some("dev"))).await;
        assert!(refused, "{ws:?}");
        assert_eq!(reason(&status), REASON_UNKNOWN, "{ws:?}");
        assert_eq!(status.as_ref().unwrap()["phase"], "Failed");
        assert_eq!(
            methods,
            vec!["PATCH".to_string()],
            "{ws:?}: nothing read, nothing made"
        );
    }
}

/// **C4 and the Secret rule, through the Backup.** The connection in `prd`,
/// the destination in `prd`, and the destination's credential in `prd` or in
/// no workspace (the shape of `logweir-signing-key`): each is
/// `WorkspaceMismatch`, the message naming both workspaces, and the only write
/// is the status. CONTROL: the all-`dev` row above.
#[tokio::test]
async fn a_referent_or_a_secret_of_another_workspace_is_a_mismatch() {
    for (label, table, names) in [
        (
            "connection in prd",
            routes(Some("prd"), Some("dev"), Some("dev")),
            "KafkaCluster `source` is in workspace `prd`",
        ),
        (
            "destination in prd",
            routes(Some("dev"), Some("prd"), Some("dev")),
            "BackupDestination `primary` is in workspace `prd`",
        ),
        (
            "credential in prd",
            routes(Some("dev"), Some("dev"), Some("prd")),
            "Secret `dest-cred` is in workspace `prd`",
        ),
        (
            "credential in no workspace",
            routes(Some("dev"), Some("dev"), None),
            "Secret `dest-cred` is in no workspace",
        ),
    ] {
        let (refused, status, methods) = run(&backup(Some("dev")), table).await;
        assert!(refused, "{label}");
        assert_eq!(reason(&status), REASON_MISMATCH, "{label}");
        let text = message(&status);
        assert!(
            text.contains(names) && text.contains("this Backup is in workspace `dev`"),
            "{label}: {text}"
        );
        assert_eq!(
            methods.iter().filter(|m| *m != "GET").count(),
            1,
            "{label}: one status write and nothing else: {methods:?}"
        );
    }
}

/// **A destination naming `logweir-signing-key` is refused.** The key Secret
/// carries no workspace label, so a `BackupDestination` (or a connection, or
/// a retention credential) that names it never reaches a Job.
/// CONTROL: the same destination naming a `dev` credential is admitted.
#[tokio::test]
async fn a_destination_naming_the_signing_key_is_refused() {
    let dest: BackupDestination =
        serde_json::from_value(destination("primary", Some("dev"), "logweir-signing-key"))
            .expect("a BackupDestination");
    let key = || {
        get(
            "/secrets/logweir-signing-key",
            &secret("logweir-signing-key", None),
        )
    };
    let (client, _, bodies) = mock_client_recording_bodies(vec![
        key(),
        patch(
            "/backupdestinations/primary/status",
            &destination("primary", Some("dev"), "logweir-signing-key"),
        ),
    ]);
    assert!(workspace::gate(&client, &dest).await.unwrap().is_some());
    let patch = bodies
        .lock()
        .unwrap()
        .iter()
        .find(|b| b.method == "PATCH")
        .map(|b| b.body.clone())
        .expect("the status");
    let status: Value = serde_json::from_str(&patch).unwrap();
    assert_eq!(status["status"]["conditions"][0]["reason"], REASON_MISMATCH);

    let ok: BackupDestination =
        serde_json::from_value(destination("primary", Some("dev"), "dest-cred")).unwrap();
    let (client, _, _) = mock_client_recording_bodies(vec![get(
        "/secrets/dest-cred",
        &secret("dest-cred", Some("dev")),
    )]);
    assert!(workspace::gate(&client, &ok).await.unwrap().is_none());
}

fn object<K: serde::de::DeserializeOwned>(value: Value) -> K {
    serde_json::from_value(value).expect("the fixture deserialises")
}

/// **C4, the table.** Every spec reference field of PROD-19.1 §4.1 is a
/// referent of its kind, so the gate reads it and holds it to the workspace.
/// A field missing from a kind's list would be a reference nobody checks.
#[test]
fn every_spec_reference_field_is_a_referent() {
    let backup: Backup = object(json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "Backup", "metadata": meta("b", Some("dev")),
        "spec": {"sourceRef": {"name": "src"}, "destinationRef": {"name": "dst"},
                 "topics": ["t"], "archive": {"url": "s3://b/p", "secretRef": {"name": "arch"}},
                 "triggeredBy": "manual", "deadlineSeconds": 900,
                 "trigger": {"kind": "Retry", "attempt": 1, "retryOf": {"name": "b0"}}}
    }));
    let schedule: BackupSchedule = object(json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "BackupSchedule", "metadata": meta("s", Some("dev")),
        "spec": {"schedule": "0 * * * *", "sourceRef": {"name": "src"}, "destinationRef": {"name": "dst"},
                 "topics": ["t"], "archive": {"url": "s3://b/p"}, "concurrencyPolicy": "Forbid"}
    }));
    let restore: Restore = object(json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "Restore", "metadata": meta("r", Some("dev")),
        "spec": {"planBytes": "x", "approvalRef": {"name": "a"},
                 "sourceArchive": {"url": "s3://b/p"}, "sourceDestinationRef": {"name": "sd"},
                 "evidenceDestinationRef": {"name": "ed"}, "backupSetRef": "bs",
                 "pointInTime": "2026-01-01T00:00:00Z",
                 "target": {"clusterRef": {"name": "tgt"}, "mode": "scratch",
                            "topicNaming": {"prefix": "x-"}},
                 "deadlineSeconds": 900}
    }));
    let approval: Approval = object(json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "Approval", "metadata": meta("a", Some("dev")),
        "spec": {"subjectRef": {"kind": "Restore", "name": "r"}, "planHash": "sha256:x",
                 "approvalBytes": "{}", "sidecarBytes": "{}"}
    }));
    let switchover: Switchover = object(json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "Switchover", "metadata": meta("sw", Some("dev")),
        "spec": {"planBytes": "x", "approvalRef": {"name": "a"},
                 "restoreRef": {"name": "r", "uid": "u"}, "deadlineSeconds": 600}
    }));
    let catalog: RecoveryCatalog = object(json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "RecoveryCatalog", "metadata": meta("c", Some("dev")),
        "spec": {"destinationRef": {"name": "dst"}, "sync": {}}
    }));
    let discovery: TopicDiscovery = object(json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "TopicDiscovery", "metadata": meta("d", Some("dev")),
        "spec": {"request": {"connectionRef": {"name": "src"}}}
    }));
    let cluster: KafkaCluster = object(cluster("src", Some("dev"), Some("cred")));

    let names = |r: &[Referent]| -> Vec<String> { r.iter().map(|x| format!("{x:?}")).collect() };
    let has = |r: Vec<Referent>, want: &[&str]| {
        let got = names(&r);
        for w in want {
            assert!(got.iter().any(|g| g == w), "{w} missing from {got:?}");
        }
    };
    has(
        backup.referents(),
        &[
            r#"Cluster("src")"#,
            r#"Destination("dst")"#,
            r#"Backup("b0")"#,
            r#"Secret("arch")"#,
        ],
    );
    has(
        schedule.referents(),
        &[r#"Cluster("src")"#, r#"Destination("dst")"#],
    );
    has(
        restore.referents(),
        &[
            r#"Cluster("tgt")"#,
            r#"Approval("a")"#,
            r#"Destination("sd")"#,
            r#"Destination("ed")"#,
        ],
    );
    has(approval.referents(), &[r#"Restore("r")"#]);
    has(
        switchover.referents(),
        &[r#"Approval("a")"#, r#"Restore("r")"#],
    );
    has(catalog.referents(), &[r#"Destination("dst")"#]);
    has(discovery.referents(), &[r#"Cluster("src")"#]);
    has(cluster.referents(), &[r#"Secret("cred")"#]);
}

/// **What the controller creates carries its parent's workspace**: a
/// schedule's run, and every runner Job (through its owner), so lists, caps
/// and per-workspace views see them. CONTROL: an owner with no label makes a
/// Job with none (the gate refused such an owner before any Job).
#[test]
fn created_backups_and_jobs_carry_the_parents_workspace() {
    let schedule: BackupSchedule = object(json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "BackupSchedule",
        "metadata": meta("nightly", Some("prd")),
        "spec": {"schedule": "0 2 * * *", "sourceRef": {"name": "src"},
                 "topics": ["t"], "archive": {"url": "s3://b/p"}, "concurrencyPolicy": "Forbid"}
    }));
    let run = weirkeeper::controllers::backup_schedule::scheduled_run(
        &schedule,
        "uid-nightly",
        &weirkeeper::controllers::backup_schedule::RunPlan::scheduled(
            "20261016T020000",
            "logweir-backup-nightly-20261016t020000",
        ),
    );
    assert_eq!(workspace::label_of(&run), Some("prd"));

    let job_of = |b: &Backup| {
        let spec = weirkeeper::job::RunnerJobSpec {
            name: "b1".into(),
            namespace: NS.into(),
            owner: weirkeeper::job::RunnerOwner::of(b, "uid-b1"),
            args: Vec::new(),
            deadline_seconds: 900,
            service_account_name: "runner".into(),
            secret_mounts: Vec::new(),
            config_map_mounts: Vec::new(),
            env_from_secret: Vec::new(),
            env_literal: Vec::new(),
            plan_config_map: None,
            image: None,
            image_pull_policy: None,
            resources: None,
        };
        weirkeeper::job::build(&spec)
    };
    assert_eq!(
        workspace::label_of(&job_of(&backup(Some("dev")))),
        Some("dev")
    );
    assert_eq!(workspace::label_of(&job_of(&backup(None))), None);
}
