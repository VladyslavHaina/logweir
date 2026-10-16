//! Configured values reach the code that acts on them, at values their
//! defaults cannot imitate: a value rendered and parsed at its default is
//! exactly what a template or a controller that ignored the value produces.
//!
//! These rows set the PROCESS ENVIRONMENT. The controller reads the
//! installation policy's reference, its two diagnostic windows and its
//! object-store addressing from its own environment (the `Preflight`,
//! `Backup` and manual-run paths take no injectable reference), and
//! `std::env::set_var` is process-global, so they are a test binary of their
//! own. Every row that reads the environment calls [`tuned_environment`]
//! first, which sets the SAME values once: the literal `env` of the controller
//! Deployment the chart renders from `examples/tuned.values.yaml`
//! (`charts/logweir/rendered/tuned.yaml`), plus the release namespace the
//! downward API would project. A value is followed from the values file,
//! through the rendered environment, into the reader.
//!
//! Each behavioural row also runs a CONTROL: the same objects with the chart's
//! DEFAULT policy document (`rendered/default.yaml`) served instead, under a
//! different `now` wherever the reader goes through a process-wide policy
//! cache, so the cache cannot carry the tuned document across. A reader that
//! ignored the loaded document fails the tuned half; a row whose premise was
//! wrong fails the control.

mod support;
use support::{backup_request, leak, not_found, repo};

use std::collections::BTreeSet;
use std::sync::{Arc, Once};

use chrono::{DateTime, Duration, TimeZone, Utc};
use logweir_core::approval_policy::ApprovalPolicySet;
use logweir_core::check_contract::{
    topic_tsv_sha256, ExpectedSummary, InventoryCounts, InventoryResult, TopicEntry,
    VisibilityState, TOPIC_INVENTORY_FORMAT,
};
use serde::Deserialize as _;
use serde_json::{json, Value};
use weirkeeper::backup_execution::inputs_config_map;
use weirkeeper::check::policy::{self, Policy, PolicyCache};
use weirkeeper::controllers::backup::{
    admit_destination, desired_execution_inputs, evidence_source_for, reconcile_backup_pooled,
    runner_job, unobserved_archive, with_status_patch, DestinationAdmission, EvidenceSource,
};
use weirkeeper::controllers::preflight as pf;
use weirkeeper::controllers::restore::admit_restore_destinations;
use weirkeeper::controllers::Context;
use weirkeeper::crds::approval::Approval;
use weirkeeper::crds::backup::Backup;
use weirkeeper::crds::kafka_cluster::KafkaCluster;
use weirkeeper::crds::preflight::Preflight;
use weirkeeper::crds::restore::Restore;
use weirkeeper::crds::trust_roster::TrustRoster;
use weirkeeper::crds::LocalRef;
use weirkeeper::evidence_fetch::{self, Step};
use weirkeeper::job::{RunnerImage, RunnerOwner};
use weirkeeper::run_pool::{Pool, PoolKind, Reservations};
use weirkeeper::testing::{mock_client_recording_bodies, Route, SeenBody};
use weirkeeper::verification::unverified_evidence;

const NS: &str = "team-a";
/// The workspace every fixture below is in (PROD-19.1).
const WS: &str = "dev";
const RELEASE_NS: &str = "logweir-system";
const CLUSTER_UID: &str = "7a2b9c1d-0000-4000-8000-0000000000c1";
const PF_UID: &str = "aaaaaaaa-0000-4000-8000-00000000000a";
const DEST_A_UID: &str = "cafe0001-0000-4000-8000-0000000000a1";

// The chart's tuned render, and this process's environment

/// Every document of one committed render.
fn render(name: &str) -> Vec<serde_yaml::Value> {
    let path = repo().join(format!("charts/logweir/rendered/{name}.yaml"));
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    serde_yaml::Deserializer::from_str(&text)
        .map(|doc| serde_yaml::Value::deserialize(doc).expect("the render is YAML"))
        .filter(|v| !v.is_null())
        .collect()
}

/// The `weirkeeper-policy` document of one render.
fn policy_json(name: &str) -> String {
    render(name)
        .into_iter()
        .find(|d| {
            d["kind"].as_str() == Some("ConfigMap")
                && d["metadata"]["name"].as_str() == Some(policy::DEFAULT_POLICY_NAME)
        })
        .and_then(|d| d["data"][policy::POLICY_KEY].as_str().map(str::to_string))
        .unwrap_or_else(|| panic!("{name}.yaml renders no weirkeeper-policy"))
}

/// The LITERAL `env` of the controller container in one render — every
/// `value:`, none of the `valueFrom:`s (a Secret or the downward API).
fn controller_env(name: &str) -> Vec<(String, String)> {
    let deployment = render(name)
        .into_iter()
        .find(|d| {
            d["kind"].as_str() == Some("Deployment")
                && d["metadata"]["name"].as_str() == Some("weirkeeper")
        })
        .unwrap_or_else(|| panic!("{name}.yaml renders no weirkeeper Deployment"));
    let containers = deployment["spec"]["template"]["spec"]["containers"]
        .as_sequence()
        .expect("containers");
    let controller = containers
        .iter()
        .find(|c| c["name"].as_str() == Some("weirkeeper"))
        .expect("the weirkeeper container");
    controller["env"]
        .as_sequence()
        .expect("an env list")
        .iter()
        .filter_map(|e| {
            Some((
                e["name"].as_str()?.to_string(),
                e["value"].as_str()?.to_string(),
            ))
        })
        .collect()
}

static ENVIRONMENT: Once = Once::new();

/// This process's environment, set ONCE to the tuned controller Deployment's:
/// its literal `env`, plus `LOGWEIR_INSTALLATION_NAMESPACE`, which the chart
/// projects from `metadata.namespace` and which is the release namespace the
/// render was made for.
fn tuned_environment() {
    ENVIRONMENT.call_once(|| {
        for (name, value) in controller_env("tuned") {
            std::env::set_var(name, value);
        }
        std::env::set_var(policy::INSTALLATION_NAMESPACE_ENV, RELEASE_NS);
    });
}

/// The `GET` of the installation policy, answered with `document`.
fn policy_route(document: String) -> Route {
    let mut data = serde_json::Map::new();
    data.insert(policy::POLICY_KEY.to_string(), Value::String(document));
    Route {
        method: "GET",
        path_suffix: "/configmaps/weirkeeper-policy",
        status: 200,
        body: json!({
            "apiVersion": "v1", "kind": "ConfigMap",
            "metadata": {"name": policy::DEFAULT_POLICY_NAME, "namespace": RELEASE_NS},
            "data": data
        })
        .to_string(),
    }
}

fn utc(h: u32, m: u32, s: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 9, 24, h, m, s)
        .single()
        .expect("the instant exists")
}

// The environment: controller.*, archive.s3.* and the policy reference

/// **The controller's environment is the chart's, and every reader of it
/// reads the tuned value.**
///
/// The tuned values set `controller.failFastSeconds: 120`,
/// `controller.jobTtlSeconds: 7200` and the four `archive.s3.*` values; the
/// render turns them into `LOGWEIR_FAIL_FAST_SECONDS`, `LOGWEIR_JOB_TTL_SECONDS`
/// and `AWS_*`. Each reader is called here, after the environment is set, and
/// returns the tuned value where its default is different:
///
/// | reader | tuned | default |
/// |---|---|---|
/// | `diagnostics::fail_fast_seconds()` | 120 s | 300 s |
/// | `diagnostics::job_ttl_seconds()` | 7200 | 604 800 |
/// | `retention::storage_url_for` region, endpoint | `eu-west-1`, the tuned endpoint | none, none |
/// | its `path_style`, `allow_http` | `false`, `true` | `true`, `false` |
/// | `run_pool::policy_ref()` | `logweir-system/weirkeeper-policy` | none |
///
/// MUTANTS: read `FAIL_FAST_SECONDS_ENV` in `job_ttl_seconds`; misspell any
/// one variable on either side (the render's name or the constant); drop the
/// `!` from `path_style: !env_flag(…)`.
#[test]
fn the_controllers_environment_is_the_charts_and_its_readers_read_the_tuned_values() {
    let env: std::collections::BTreeMap<String, String> =
        controller_env("tuned").into_iter().collect();
    for (name, want) in [
        (weirkeeper::diagnostics::FAIL_FAST_SECONDS_ENV, "120"),
        (weirkeeper::diagnostics::JOB_TTL_SECONDS_ENV, "7200"),
        ("AWS_REGION", "eu-west-1"),
        ("AWS_ENDPOINT_URL", "http://minio.tuned.svc:9000"),
        ("AWS_ALLOW_HTTP", "true"),
        ("AWS_VIRTUAL_HOSTED_STYLE_REQUEST", "true"),
        (
            weirkeeper::retention::ARCHIVE_URL_ENV,
            "s3://lw-tuned-archive/logweir",
        ),
    ] {
        assert_eq!(
            env.get(name).map(String::as_str),
            Some(want),
            "the tuned render's controller env must carry {name}={want}: {env:?}"
        );
    }
    // The default render carries neither window at all: "" is the build's own.
    let defaults: BTreeSet<String> = controller_env("default")
        .into_iter()
        .map(|(k, _)| k)
        .collect();
    assert!(!defaults.contains(weirkeeper::diagnostics::FAIL_FAST_SECONDS_ENV));
    assert!(!defaults.contains(weirkeeper::diagnostics::JOB_TTL_SECONDS_ENV));

    tuned_environment();
    assert_eq!(
        weirkeeper::diagnostics::fail_fast_seconds(),
        Some(std::time::Duration::from_secs(120))
    );
    assert_eq!(weirkeeper::diagnostics::job_ttl_seconds(), 7200);
    match weirkeeper::retention::storage_url_for("s3://lw-tuned-archive/logweir")
        .expect("an s3:// URL")
    {
        logweir_core::engine::StorageUrl::S3 {
            bucket,
            prefix,
            region,
            endpoint,
            path_style,
            allow_http,
        } => {
            assert_eq!(
                (bucket.as_str(), prefix.as_str()),
                ("lw-tuned-archive", "logweir")
            );
            assert_eq!(region.as_deref(), Some("eu-west-1"));
            assert_eq!(endpoint.as_deref(), Some("http://minio.tuned.svc:9000"));
            assert!(!path_style, "virtualHostedStyle: true is NOT path-style");
            assert!(allow_http, "allowHttp: true");
        }
        other => panic!("an s3:// URL is S3, got {other:?}"),
    }
    assert_eq!(
        weirkeeper::retention::configured_archive_url(std::env::var(
            weirkeeper::retention::ARCHIVE_URL_ENV
        ))
        .as_deref(),
        Some("s3://lw-tuned-archive/logweir")
    );
    assert_eq!(
        weirkeeper::run_pool::policy_ref(),
        Some((
            RELEASE_NS.to_string(),
            policy::DEFAULT_POLICY_NAME.to_string()
        )),
        "the chart renders LOGWEIR_POLICY_CONFIGMAP empty and the namespace from the pod"
    );
}

// ---------------------------------------------------------------------------
// preflight.retentionSeconds
// ---------------------------------------------------------------------------

fn now_pf() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 9, 16, 12, 0, 0).unwrap()
}

/// A terminal `Preflight` that is only there to make the reconcile reach the
/// collector; its own verdict is `notReady`, which nothing revalidates.
fn terminal_preflight() -> Preflight {
    serde_json::from_value(json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "Preflight",
        "metadata": {"labels": {"logweir.dev/workspace": WS}, "name": "pf-1", "namespace": NS, "uid": PF_UID,
                     "generation": 1, "resourceVersion": "100"},
        "spec": {"request": backup_request()},
        "status": {
            "phase": "Completed", "reason": "NotReady",
            "binding": {"inputsDigest": "sha256:aa", "referents": []},
            "result": {"state": "notReady", "checks": []}
        }
    }))
    .expect("the Preflight fixture parses")
}

fn listed_preflight(name: &str, uid: &str, basis: DateTime<Utc>) -> Value {
    json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "Preflight",
        "metadata": {"labels": {"logweir.dev/workspace": WS}, "name": name, "namespace": NS, "uid": uid, "resourceVersion": "9"},
        "spec": {"request": backup_request()},
        "status": {
            "phase": "Completed", "reason": "Valid",
            "observedAt": basis.to_rfc3339(),
            "result": {"state": "ready", "expiresAt": basis.to_rfc3339(), "checks": []}
        }
    })
}

fn preflight_routes(document: String, delete: bool) -> Vec<Route> {
    let mut routes = vec![
        Route {
            method: "GET",
            path_suffix: "/preflights",
            status: 200,
            body: json!({
                "apiVersion": "logweir.dev/v1alpha1", "kind": "PreflightList",
                "metadata": {"resourceVersion": "9"},
                // Expired TWO MINUTES before `now`: past the tuned 90 s,
                // inside the default 3 600 s.
                "items": [listed_preflight("pf-old", "uid-old", now_pf() - Duration::minutes(2))]
            })
            .to_string(),
        },
        policy_route(document),
        Route {
            method: "PATCH",
            path_suffix: "/pf-1/status",
            status: 200,
            body: json!({
                "apiVersion": "logweir.dev/v1alpha1", "kind": "Preflight",
                "metadata": {"labels": {"logweir.dev/workspace": WS}, "name": "pf-1", "namespace": NS, "uid": PF_UID, "resourceVersion": "101"},
                "spec": {"request": backup_request()}
            })
            .to_string(),
        },
    ];
    if delete {
        routes.push(Route {
            method: "DELETE",
            path_suffix: "/preflights/pf-old",
            status: 200,
            body: json!({"kind": "Status", "status": "Success"}).to_string(),
        });
    }
    routes
}

async fn deletes_after_one_pass(document: String, delete: bool) -> Vec<String> {
    let (client, recorder, _b) = mock_client_recording_bodies(preflight_routes(document, delete));
    let ctx = Context {
        client,
        archive: None,
        runner_image: RunnerImage::default(),
    };
    pf::reconcile_preflight(&terminal_preflight(), &ctx, &PolicyCache::new(), now_pf())
        .await
        .expect("the reconcile completes");
    let deletes: Vec<String> = recorder
        .lock()
        .expect("recorder")
        .iter()
        .filter(|r| r.method == "DELETE")
        .map(|r| r.uri.clone())
        .collect();
    deletes
}

/// **`checks.preflight.retentionSeconds` reaches the `Preflight` collector.**
///
/// A terminal `Preflight` whose verdict expired two minutes ago is collected
/// under the chart's tuned 90 s window. CONTROL: served the DEFAULT render's
/// document (3 600 s), the same pass deletes nothing, and its route table
/// holds no `DELETE`, so a deletion would panic the double.
///
/// MUTANT: `collect_expired(&api, &namespace, 3600, now)` at the call site.
#[tokio::test]
async fn the_installation_policys_preflight_window_reaches_the_preflight_collector() {
    tuned_environment();
    let deletes = deletes_after_one_pass(policy_json("tuned"), true).await;
    assert_eq!(deletes.len(), 1, "{deletes:?}");
    assert!(deletes[0].contains("/preflights/pf-old"));
    let deletes = deletes_after_one_pass(policy_json("default"), false).await;
    assert!(
        deletes.is_empty(),
        "CONTROL: inside the default hour: {deletes:?}"
    );
}

// runs.*: the manual-run ceilings

fn cluster() -> KafkaCluster {
    serde_json::from_value(json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "KafkaCluster",
        "metadata": {"labels": {"logweir.dev/workspace": WS}, "name": "prod", "namespace": NS, "uid": CLUSTER_UID},
        "spec": {
            "bootstrapServers": ["broker-0.prod:9093"],
            "auth": {"mode": "scramSha512", "username": "logweir",
                     "secretRef": {"name": "prod-sasl"}, "tls": true},
            "role": "source"
        },
        "status": {"reachable": true, "clusterId": "MkU3OEVBNTcwNTJENDM2Qk"}
    }))
    .expect("the fixture is a KafkaCluster")
}

/// A MANUAL `Backup`, as the API creates one ("Back up now").
fn manual(name: &str, uid: &str, created: DateTime<Utc>) -> Backup {
    serde_json::from_value(json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "Backup",
        "metadata": {"labels": {"logweir.dev/workspace": WS},
            "name": name, "namespace": NS, "uid": uid,
            "generation": 1, "resourceVersion": "100",
            "creationTimestamp": created.to_rfc3339()
        },
        "spec": {
            "sourceRef": {"name": "prod"},
            "topics": ["orders", "payments"],
            "archive": {"url": "s3://kafka-backups/logweir", "secretRef": {"name": "logweir-s3"}},
            "triggeredBy": "manual",
            "trigger": {"kind": "Manual", "attempt": 0},
            "deadlineSeconds": 3600
        }
    }))
    .expect("the fixture is a Backup")
}

/// Everything a CREATE pass of `b` may ask for, the policy included.
fn create_routes(b: &Backup, document: String) -> Vec<Route> {
    let name = b.metadata.name.clone().expect("named");
    let frozen = desired_execution_inputs(b, &cluster()).expect("the fixture resolves");
    let plan = inputs_config_map(b, &frozen).expect("the plan renders");
    let job = runner_job(b, &cluster(), &frozen, &RunnerImage::default()).expect("the Job renders");
    vec![
        Route {
            method: "GET",
            path_suffix: leak(format!("/jobs/{name}")),
            status: 404,
            body: not_found("jobs.batch", &name),
        },
        Route {
            method: "GET",
            path_suffix: "/kafkaclusters/prod",
            status: 200,
            body: serde_json::to_string(&cluster()).expect("serialises"),
        },
        Route {
            method: "GET",
            path_suffix: leak(format!("/configmaps/{name}-plan")),
            status: 200,
            body: serde_json::to_string(&plan).expect("serialises"),
        },
        policy_route(document),
        Route {
            method: "POST",
            path_suffix: "/configmaps",
            status: 201,
            body: serde_json::to_string(&plan).expect("serialises"),
        },
        Route {
            method: "POST",
            path_suffix: "/jobs",
            status: 201,
            body: serde_json::to_string(&job).expect("serialises"),
        },
        Route {
            method: "PATCH",
            path_suffix: leak(format!("/backups/{name}/status")),
            status: 200,
            body: serde_json::to_string(b).expect("a Backup serialises"),
        },
    ]
}

fn posts(seen: &[SeenBody], suffix: &str) -> usize {
    seen.iter()
        .filter(|r| {
            r.method == "POST" && r.uri.split('?').next().unwrap_or(&r.uri).ends_with(suffix)
        })
        .count()
}

fn patched_statuses(seen: &[SeenBody]) -> Vec<Value> {
    seen.iter()
        .filter(|r| r.method == "PATCH")
        .map(|r| serde_json::from_str::<Value>(&r.body).expect("a patch is JSON")["status"].clone())
        .collect()
}

/// One pooled pass of `candidate` beside `peers`, the ceiling read from the
/// installation policy (`limit: None`, as production passes it).
async fn pooled_pass(
    candidate: &Backup,
    peers: Vec<Backup>,
    document: String,
    now: DateTime<Utc>,
) -> Vec<SeenBody> {
    let (client, _seen, bodies) = mock_client_recording_bodies(create_routes(candidate, document));
    let snapshot: Vec<Arc<Backup>> = peers.into_iter().map(Arc::new).collect();
    let source = move || Some(snapshot.clone());
    let reservations = Reservations::new();
    let pool = Pool {
        peers: &source,
        limit: None,
        reservations: &reservations,
    };
    reconcile_backup_pooled(
        candidate,
        &client,
        &unobserved_archive,
        &unverified_evidence,
        now,
        &RunnerImage::default(),
        None,
        &pool,
    )
    .await
    .expect("the pass reconciles");
    let seen = bodies.lock().expect("readable").clone();
    seen
}

/// **`runs.maxManualBackupsActivePerWorkspace` reaches the manual-run gate:
/// environment, `ConfigMap`, ceiling, queue.**
///
/// Five manual `Backup`s are running and a sixth arrives. The chart's tuned
/// document RAISES the ceiling to six, so the sixth is admitted and its Job is
/// created. CONTROL: served the default document (four), the same pass queues
/// it (`queue.limit: 4`, nothing created).
///
/// MUTANT: `crate::run_pool::ceiling(PoolKind::Backup, &Policy::defaults())`
/// at the gate, or `4` inside `ceiling`.
#[tokio::test]
async fn the_installation_policys_manual_backup_ceiling_admits_the_sixth_run() {
    tuned_environment();
    let running: Vec<Backup> = (0..5u32)
        .map(|i| {
            let name = format!("logweir-manual-r{i}");
            with_status_patch(
                &manual(
                    &name,
                    &format!("00000000-0000-4000-8000-0000000000a{i}"),
                    utc(16, 0, i),
                ),
                &json!({"status": {"phase": "Running", "jobRef": {"name": name}}}),
            )
        })
        .collect();
    let sixth = manual(
        "logweir-manual-sixth",
        "00000000-0000-4000-8000-0000000000b6",
        utc(16, 0, 9),
    );
    let mut peers = running;
    peers.push(sixth.clone());

    let seen = pooled_pass(&sixth, peers.clone(), policy_json("tuned"), utc(16, 30, 0)).await;
    assert_eq!(
        posts(&seen, "/jobs"),
        1,
        "the tuned ceiling is six, so the sixth starts beside five"
    );

    // CONTROL, an hour later so the process-wide cache holds nothing current.
    let seen = pooled_pass(&sixth, peers, policy_json("default"), utc(17, 30, 0)).await;
    assert_eq!(posts(&seen, "/jobs"), 0, "CONTROL: four may run, five do");
    assert_eq!(posts(&seen, "/configmaps"), 0, "and no plan");
    let statuses = patched_statuses(&seen);
    assert_eq!(statuses.len(), 1, "{statuses:?}");
    assert_eq!(statuses[0]["phase"], "Queued");
    assert_eq!(statuses[0]["queue"], json!({"limit": 4}));
}

/// **`runs.maxManualRestoresActivePerWorkspace` is the ceiling the restore
/// gate applies.**
///
/// The mapping both gates share (`run_pool::ceiling`): the tuned document's
/// restores (8) and backups (6) differ from each other and from their defaults
/// (2 and 4), so a crossed or constant mapping fails, and each gate is pinned
/// to call it over the document it loaded. The restore gate's end-to-end pass
/// is `the_installation_policys_manual_restore_ceiling_admits_the_third_restore`.
///
/// MUTANTS: swap the two arms of `ceiling`; pass `PoolKind::Backup` at the
/// restore gate; pass `&Policy::defaults()` there.
#[test]
fn the_installation_policys_manual_restore_ceiling_is_the_one_the_pool_applies() {
    let tuned = policy::parse(policy_json("tuned").as_bytes()).expect("the tuned document parses");
    assert_eq!(weirkeeper::run_pool::ceiling(PoolKind::Restore, &tuned), 8);
    assert_eq!(weirkeeper::run_pool::ceiling(PoolKind::Backup, &tuned), 6);
    assert_eq!(
        weirkeeper::run_pool::ceiling(PoolKind::Restore, &Policy::defaults()),
        2
    );
    assert_eq!(
        weirkeeper::run_pool::ceiling(PoolKind::Backup, &Policy::defaults()),
        4
    );

    let squeeze = |path: &str| -> String {
        std::fs::read_to_string(repo().join(path))
            .expect("a source file")
            .chars()
            .filter(|c| !c.is_whitespace())
            .collect()
    };
    for (path, kind) in [
        ("crates/weirkeeper/src/controllers/restore.rs", "Restore"),
        ("crates/weirkeeper/src/controllers/backup.rs", "Backup"),
    ] {
        let wiring = format!(
            "None=>crate::run_pool::ceiling(crate::run_pool::PoolKind::{kind},crate::check::policy::load(client,crate::run_pool::policy_ref().as_ref(),crate::run_pool::policy_cache(),now,)"
        );
        assert!(
            squeeze(path).contains(&wiring),
            "{path}: the gate must take its ceiling from `run_pool::ceiling(PoolKind::{kind}, …)` \
             over the policy it LOADED"
        );
    }
}

// runs.maxManualRestoresActivePerWorkspace, end to end

const RESTORE_PLAN: &str = "\
source:
  storage:
    backend: s3
    bucket: kafka-backups
    prefix: drill-demo
    region: us-east-1
  backup: latestCompleted
  topics: [orders]
target:
  bootstrap_servers: [scratch-0.team-a:9092]
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
";
const APPROVER_KEY_ID: &str =
    "sha256:1111111111111111111111111111111111111111111111111111111111111111";

/// A MANUAL inline-archive `Restore` in [`NS`], approved by `a1`.
fn manual_restore(name: &str, uid: &str, minute: u32, status: Value) -> Restore {
    let mut value = json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "Restore",
        "metadata": {"labels": {"logweir.dev/workspace": WS}, "name": name, "namespace": NS, "uid": uid, "generation": 1,
                     "resourceVersion": "7", "creationTimestamp": utc(11, minute, 0).to_rfc3339()},
        "spec": {
            "planBytes": RESTORE_PLAN,
            "approvalRef": {"name": "a1"},
            "sourceArchive": {"url": "s3://kafka-backups/logweir", "secretRef": {"name": "logweir-s3"}},
            "backupSetRef": "drill-demo",
            "pointInTime": "2026-09-07T14:05:00Z",
            "target": {"clusterRef": {"name": "scratch"}, "mode": "scratch",
                       "topicNaming": {"prefix": "drill-"}},
            "deadlineSeconds": 1800
        }
    });
    if !status.is_null() {
        value["status"] = status;
    }
    serde_json::from_value(value).expect("a Restore")
}

/// Today's (v1) approval of `restore`, verified, under no approval policy.
fn restore_approval(restore: &Restore) -> Approval {
    let name = restore.metadata.name.clone().expect("named");
    let uid = restore.metadata.uid.clone().expect("uid");
    let hash = logweir_core::ids::sha256_prefixed(RESTORE_PLAN.as_bytes());
    serde_json::from_value(json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "Approval",
        "metadata": {"labels": {"logweir.dev/workspace": WS}, "name": "a1", "namespace": NS, "uid": "aaaaaaaa-0000-4000-8000-0000000000f1"},
        "spec": {
            "subjectRef": {"kind": "Restore", "name": name},
            "planHash": hash,
            "approvalBytes": format!(
                r#"{{"approver":"sre-oncall@example.com","ticket":"CHG-1","plan_hash":"{hash}","subject_kind":"Restore"}}"#
            ),
            "sidecarBytes": "{}"
        },
        "status": {
            "verified": true,
            "matchedKeyId": APPROVER_KEY_ID,
            "verifiedSubjectRef": {"apiVersion": "logweir.dev/v1alpha1", "kind": "Restore",
                                   "name": name, "namespace": NS, "uid": uid},
            "conditions": [{"type": "Verified", "status": "True", "reason": "Verified"}]
        }
    }))
    .expect("an Approval")
}

fn roster() -> TrustRoster {
    serde_json::from_value(json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "TrustRoster",
        "metadata": {"name": "default", "uid": "dddddddd-0000-4000-8000-0000000000f1"},
        "spec": {
            "approverKeys": [{"keyId": APPROVER_KEY_ID,
                              "spkiPem": "-----BEGIN PUBLIC KEY-----\nA\n-----END PUBLIC KEY-----\n"}],
            "signingKeys": [],
            "allowedClusterIds": ["MkU3OEVBNTcwNTJENDM2Qk"]
        },
        "status": {"loaded": true}
    }))
    .expect("a TrustRoster")
}

/// Everything one pass of `restore` may ask for, the policy included: the
/// admission reads, the trust (a roster, no `TrustPolicy`), and the plan,
/// bundle and Job it creates.
fn restore_routes(restore: &Restore, document: String, now: DateTime<Utc>) -> Vec<Route> {
    let name = restore.metadata.name.clone().expect("named");
    let uid = restore.metadata.uid.clone().expect("uid");
    let approval = restore_approval(restore);
    let plan = weirkeeper::controllers::restore::plan_config_map(restore).expect("a plan");
    let bundle = weirkeeper::controllers::restore::approval_bundle_config_map(
        restore,
        &approval,
        &weirkeeper::trust::synthesize_legacy(&roster().spec),
        now,
    )
    .expect("a bundle");
    let job = json!({
        "apiVersion": "batch/v1", "kind": "Job",
        "metadata": {"name": name, "namespace": NS, "uid": "bbbbbbbb-0000-4000-8000-0000000000f1",
                     "ownerReferences": [{"apiVersion": "logweir.dev/v1alpha1", "kind": "Restore",
                                          "name": name, "uid": uid, "controller": true,
                                          "blockOwnerDeletion": true}]},
        "spec": {"template": {"spec": {"containers": [], "restartPolicy": "Never"}}},
        "status": {"active": 1}
    });
    vec![
        Route {
            method: "GET",
            path_suffix: leak(format!("/jobs/{name}")),
            status: 404,
            body: not_found("jobs.batch", &name),
        },
        Route {
            method: "GET",
            path_suffix: "/approvals/a1",
            status: 200,
            body: serde_json::to_string(&approval).expect("JSON"),
        },
        Route {
            method: "GET",
            path_suffix: "/kafkaclusters/scratch",
            status: 200,
            body: json!({
                "apiVersion": "logweir.dev/v1alpha1", "kind": "KafkaCluster",
                "metadata": {"labels": {"logweir.dev/workspace": WS}, "name": "scratch", "namespace": NS,
                             "uid": "cccccccc-0000-4000-8000-0000000000f1"},
                "spec": {"bootstrapServers": ["scratch-0.team-a:9092"],
                         "auth": {"mode": "plaintext", "tls": false},
                         "role": "scratch", "markerTopic": "logweir.scratch"},
                "status": {"reachable": true, "clusterId": "MkU3OEVBNTcwNTJENDM2Qk"}
            })
            .to_string(),
        },
        policy_route(document),
        Route {
            method: "GET",
            path_suffix: "/trustpolicies",
            status: 200,
            body: json!({"apiVersion": "logweir.dev/v1alpha1", "kind": "TrustPolicyList",
                         "metadata": {"resourceVersion": "1"}, "items": []})
            .to_string(),
        },
        Route {
            method: "GET",
            path_suffix: "/trustrosters/default",
            status: 200,
            body: serde_json::to_string(&roster()).expect("JSON"),
        },
        Route {
            method: "POST",
            path_suffix: "/configmaps",
            status: 201,
            body: serde_json::to_string(&plan).expect("JSON"),
        },
        Route {
            method: "GET",
            path_suffix: leak(format!("/configmaps/{name}-plan")),
            status: 200,
            body: serde_json::to_string(&plan).expect("JSON"),
        },
        Route {
            method: "GET",
            path_suffix: leak(format!("/configmaps/{name}-approval-bundle")),
            status: 200,
            body: serde_json::to_string(&bundle).expect("JSON"),
        },
        Route {
            method: "POST",
            path_suffix: "/jobs",
            status: 201,
            body: job.to_string(),
        },
        Route {
            method: "PATCH",
            path_suffix: leak(format!("/restores/{name}/status")),
            status: 200,
            body: serde_json::to_string(restore).expect("JSON"),
        },
    ]
}

/// One pooled pass of `candidate` beside `peers`, its ceiling read from the
/// installation policy (`limit: None`, as production passes it).
async fn pooled_restore_pass(
    candidate: &Restore,
    peers: Vec<Restore>,
    document: String,
    now: DateTime<Utc>,
) -> Vec<SeenBody> {
    let (client, _seen, bodies) =
        mock_client_recording_bodies(restore_routes(candidate, document, now));
    let snapshot: Vec<Arc<Restore>> = peers.into_iter().map(Arc::new).collect();
    let source = move || Some(snapshot.clone());
    let reservations = Reservations::new();
    let pool = Pool {
        peers: &source,
        limit: None,
        reservations: &reservations,
    };
    weirkeeper::controllers::restore::reconcile_restore_pooled(
        candidate,
        &client,
        &weirkeeper::controllers::restore::unobserved_scorecard,
        &unverified_evidence,
        now,
        &RunnerImage::default(),
        &ApprovalPolicySet::default(),
        None,
        &pool,
    )
    .await
    .expect("the pass reconciles");
    let seen = bodies.lock().expect("readable").clone();
    seen
}

/// **`runs.maxManualRestoresActivePerWorkspace` reaches the restore gate:
/// environment, `ConfigMap`, ceiling, admission.**
///
/// Two manual `Restore`s are running and an approved third arrives. The
/// chart's tuned document raises the ceiling to eight, so the third is
/// admitted (`Admitted=True` naming `= 8`) and its Job is created. CONTROL:
/// served the default document (two), the same pass queues it
/// (`queue.limit: 2`, nothing created). This drives the gate's USE of the
/// ceiling, not only its mapping.
///
/// MUTANTS: `&Policy::defaults()` for the loaded policy at the restore gate;
/// `limit` replaced by `2` in its `decide` call.
#[tokio::test]
async fn the_installation_policys_manual_restore_ceiling_admits_the_third_restore() {
    tuned_environment();
    let running = |name: &str, uid: &str, minute: u32| {
        manual_restore(
            name,
            uid,
            minute,
            json!({"phase": "Running", "jobRef": {"name": name}}),
        )
    };
    let third = manual_restore(
        "rst-third",
        "5c2e7b91-0000-4000-8000-0000000000f3",
        9,
        Value::Null,
    );
    let peers = vec![
        running("rst-a", "5c2e7b91-0000-4000-8000-0000000000f1", 1),
        running("rst-b", "5c2e7b91-0000-4000-8000-0000000000f2", 2),
        third.clone(),
    ];

    let seen =
        pooled_restore_pass(&third, peers.clone(), policy_json("tuned"), utc(12, 0, 0)).await;
    assert_eq!(
        posts(&seen, "/jobs"),
        1,
        "the tuned ceiling is eight: the third starts"
    );
    let first = &patched_statuses(&seen)[0];
    let admitted = first["conditions"]
        .as_array()
        .and_then(|cs| cs.iter().find(|c| c["type"] == "Admitted"))
        .expect("the admission record comes first");
    assert_eq!(admitted["status"], "True");
    assert!(
        admitted["message"]
            .as_str()
            .unwrap_or_default()
            .contains("maxManualRestoresActivePerWorkspace` = 8"),
        "{admitted}"
    );

    // CONTROL, an hour later so the process-wide cache holds nothing current.
    let seen = pooled_restore_pass(&third, peers, policy_json("default"), utc(13, 0, 0)).await;
    assert_eq!(posts(&seen, "/jobs"), 0, "CONTROL: two may run, two do");
    assert_eq!(posts(&seen, "/configmaps"), 0, "and no plan");
    let statuses = patched_statuses(&seen);
    assert_eq!(statuses.len(), 1, "{statuses:?}");
    assert_eq!(statuses[0]["phase"], "Queued");
    assert_eq!(statuses[0]["queue"]["limit"], 2);
}

// checks.maxEvidenceFetchActivePerWorkspace, from the caller

const BACKUP_UID: &str = "00000000-0000-4000-8000-0000000000e1";

/// A destination whose `evidenceRead` only a pod may hold: its evidence is
/// fetched by an evidence-fetch Job.
fn fetch_destination() -> Value {
    json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "BackupDestination",
        "metadata": {"labels": {"logweir.dev/workspace": WS}, "name": "dest-f", "namespace": NS,
                     "uid": "cafe0003-0000-4000-8000-0000000000f3",
                     "generation": 1, "resourceVersion": "11"},
        "spec": {
            "storage": {"provider": "S3", "bucket": "lw-f", "prefix": "team-a",
                        "region": "us-east-1", "endpoint": "https://minio-f.storage.svc:9000",
                        "addressing": "PathStyle"},
            "transport": {"security": "TLS"},
            "access": {
                "archiveWrite": {"mode": "SecretKeys", "secret": {"name": "lw-f-writer",
                    "accessKeyIdKey": "id", "secretAccessKeyKey": "key"}},
                "evidenceRead": {"mode": "SecretKeys", "secret": {"name": "lw-f-reader",
                    "accessKeyIdKey": "id", "secretAccessKeyKey": "key"}}
            }
        },
        "status": {"observedGeneration": 1,
                   "conditions": [{"type": "Valid", "status": "True", "reason": "Valid",
                                   "observedGeneration": 1}]}
    })
}

/// `n` evidence-fetch Jobs running in [`NS`] for other runs.
fn active_fetches(n: u32) -> String {
    let items: Vec<Value> = (0..n)
        .map(|i| {
            json!({
                "apiVersion": "batch/v1", "kind": "Job",
                "metadata": {
                    "name": format!("lwc-ev-other{i}"), "namespace": NS,
                    "uid": format!("eeeeeeee-0000-4000-8000-00000000000{i}"),
                    "labels": {"app.kubernetes.io/component": "check",
                               "logweir.dev/check-kind": "evidenceFetch",
                               "logweir.dev/workspace": WS}
                },
                "spec": {"template": {"spec": {"containers": [], "restartPolicy": "Never"}}},
                "status": {"active": 1}
            })
        })
        .collect();
    json!({"apiVersion": "batch/v1", "kind": "JobList", "metadata": {}, "items": items}).to_string()
}

/// One pass from the caller: [`evidence_source_for`] loads the policy and
/// resolves the destination, and its `checks` drive [`evidence_fetch::advance`]
/// beside `active` running fetches.
async fn fetch_step(document: String, now: DateTime<Utc>, active: u32) -> (Step, Vec<SeenBody>) {
    let job_name = weirkeeper::check::job::evidence_fetch_job_name(BACKUP_UID, 1);
    let (client, _seen, bodies) = mock_client_recording_bodies(vec![
        Route {
            method: "GET",
            path_suffix: "/backupdestinations/dest-f",
            status: 200,
            body: fetch_destination().to_string(),
        },
        policy_route(document),
        Route {
            method: "GET",
            path_suffix: leak(format!("/jobs/{job_name}")),
            status: 404,
            body: not_found("jobs.batch", &job_name),
        },
        Route {
            method: "GET",
            path_suffix: "/jobs",
            status: 200,
            body: active_fetches(active),
        },
        Route {
            method: "POST",
            path_suffix: "/configmaps",
            status: 201,
            body: json!({"apiVersion": "v1", "kind": "ConfigMap",
                         "metadata": {"name": format!("{job_name}-plan"), "namespace": NS}})
            .to_string(),
        },
        Route {
            method: "POST",
            path_suffix: "/jobs",
            status: 201,
            body: json!({"apiVersion": "batch/v1", "kind": "Job",
                         "metadata": {"name": job_name, "namespace": NS,
                                      "uid": "eeeeeeee-0000-4000-8000-0000000000ff"},
                         "spec": {"template": {"spec": {"containers": [],
                                                        "restartPolicy": "Never"}}}})
            .to_string(),
        },
    ]);
    let reference = LocalRef {
        name: "dest-f".to_string(),
    };
    let source = evidence_source_for(Some(&reference), &client, NS, now)
        .await
        .expect("the reads succeed");
    assert!(
        matches!(source, EvidenceSource::FetchJob { .. }),
        "a SecretKeys evidenceRead is fetched by a Job: {source:?}"
    );
    // As `controllers/backup.rs` hands them to `advance`.
    let (destination, checks, policy_digest, unresolved) = source.fetch_inputs();
    let owner = RunnerOwner {
        api_version: "logweir.dev/v1alpha1".to_string(),
        kind: "Backup".to_string(),
        name: "logweir-manual-ev".to_string(),
        uid: BACKUP_UID.to_string(),
        workspace: Some(WS.to_string()),
    };
    let request = evidence_fetch::Request {
        payload_key: "logweir/receipts/ev.json".to_string(),
        sidecar_key: "logweir/receipts/ev.json.sig".to_string(),
        payload_type: logweir_verify::PAYLOAD_TYPE_BACKUP_RECEIPT,
    };
    let step = evidence_fetch::advance(&evidence_fetch::Inputs {
        client: &client,
        namespace: NS,
        workspace: WS,
        owner: &owner,
        attempt: 1,
        request: &request,
        destination,
        unresolved: unresolved.as_deref(),
        checks: &checks,
        policy_digest,
        image: &RunnerImage::default(),
        now,
    })
    .await
    .expect("the pass completes");
    let seen = bodies.lock().expect("readable").clone();
    (step, seen)
}

/// **`checks.maxEvidenceFetchActivePerWorkspace` reaches the evidence-fetch
/// pool FROM ITS CALLER**: the policy `evidence_source_for` loads is the one
/// `advance` admits by.
///
/// Four evidence-fetch Jobs run in the workspace. The tuned document's pool is
/// five, so a fifth is created; beside five it waits, naming `= 5`. CONTROL:
/// served the default document (four), the pass beside four waits, naming
/// `= 4`, and creates nothing. `limits::admit` is driven at a non-default
/// value elsewhere; this drives the caller (`controllers/backup.rs`,
/// `checks: load.policy().checks`).
///
/// MUTANT: `checks: Policy::defaults().checks` in `evidence_source_for`.
#[tokio::test]
async fn the_installation_policys_evidence_fetch_pool_reaches_the_fetch_from_its_caller() {
    tuned_environment();
    let (step, seen) = fetch_step(policy_json("tuned"), utc(18, 0, 0), 4).await;
    assert!(matches!(step, Step::Running { .. }), "{step:?}");
    assert_eq!(
        posts(&seen, "/jobs"),
        1,
        "the tuned pool is five: the fifth starts"
    );
    let queued = |step: Step| match step {
        Step::Queued { detail } => detail,
        other => panic!("a full pool waits: {other:?}"),
    };
    let (step, seen) = fetch_step(policy_json("tuned"), utc(18, 30, 0), 5).await;
    let detail = queued(step);
    assert!(
        detail.contains("maxEvidenceFetchActivePerWorkspace = 5"),
        "{detail}"
    );
    assert_eq!(posts(&seen, "/jobs") + posts(&seen, "/configmaps"), 0);

    // CONTROL, an hour later so the process-wide cache holds nothing current.
    let (step, seen) = fetch_step(policy_json("default"), utc(19, 0, 0), 4).await;
    let detail = queued(step);
    assert!(
        detail.contains("maxEvidenceFetchActivePerWorkspace = 4"),
        "{detail}"
    );
    assert_eq!(posts(&seen, "/jobs") + posts(&seen, "/configmaps"), 0);
}

// checks.discovery.visibilityAttestations, at a Backup's run-time discovery

/// A clean listing of the tuned attestation's cluster: no authorization error,
/// not truncated.
fn clean_listing(cluster_id: &str) -> InventoryResult {
    InventoryResult {
        format: TOPIC_INVENTORY_FORMAT.to_string(),
        cluster_id: Some(cluster_id.to_string()),
        broker_count: Some(3),
        counts: InventoryCounts {
            listed: 2,
            returned: 2,
            internal_excluded: 0,
            errored: 0,
        },
        truncated: false,
        truncation_reason: None,
        topic_authorization_error_in_listing: false,
        expected: ExpectedSummary::default(),
        expected_results: Vec::new(),
        topics_sha256: topic_tsv_sha256(&[
            TopicEntry::new("orders", 3),
            TopicEntry::new("payments", 3),
        ]),
    }
}

/// **`checks.discovery.visibilityAttestations` reaches a `Backup`'s run-time
/// discovery** (`backup_selection::run_time_visibility`, the read `resolve`
/// makes), from the policy reference in this process's environment.
///
/// The tuned document attests `team-a`'s `source` cluster
/// (`M29I2S7FQPyHBEX12Vx7XA`) for `User:backup` until 2027, so a clean listing
/// of it is `attestedComplete`, the one state a `Backup` with
/// `incompleteDiscovery: Refuse` resolves under. CONTROL: served the default
/// document (no attestation), the same listing is `unknown`, and a listing of
/// another cluster id under the tuned document is `unknown` too.
///
/// MUTANT: `&Policy::defaults().discovery.visibility_attestations` at the read.
#[tokio::test]
async fn the_installation_policys_attestation_reaches_a_backups_run_time_discovery() {
    tuned_environment();
    let verdict = |document: String, cluster_id: &'static str| async move {
        let (client, _r, _b) = mock_client_recording_bodies(vec![policy_route(document)]);
        weirkeeper::controllers::backup_selection::run_time_visibility(
            &client,
            &clean_listing(cluster_id),
            NS,
            "source",
            "User:backup",
            utc(12, 0, 0),
        )
        .await
        .expect("the policy read succeeds")
        .state
    };
    let attested = "M29I2S7FQPyHBEX12Vx7XA";
    assert_eq!(
        verdict(policy_json("tuned"), attested).await,
        VisibilityState::AttestedComplete
    );
    assert_eq!(
        verdict(policy_json("default"), attested).await,
        VisibilityState::Unknown,
        "CONTROL: no attestation in the default document"
    );
    assert_eq!(
        verdict(policy_json("tuned"), "aaaaaaaaaaaaaaaaaaaaaa").await,
        VisibilityState::Unknown,
        "CONTROL: the attestation is for the cluster it names"
    );
}

// ---------------------------------------------------------------------------
// engine.allowUnverifiedCustomCa
// ---------------------------------------------------------------------------

const CA_PEM: &str = "-----BEGIN CERTIFICATE-----\nMAaqAQIDBAUG\n-----END CERTIFICATE-----\n";

fn destination_with_ca() -> Value {
    json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "BackupDestination",
        "metadata": {"labels": {"logweir.dev/workspace": WS}, "name": "dest-a", "namespace": NS, "uid": DEST_A_UID,
                     "generation": 3, "resourceVersion": "1001"},
        "spec": {
            "storage": {
                "provider": "S3", "bucket": "lw-a", "prefix": "team-a/prod",
                "region": "us-east-1", "endpoint": "https://minio-a.storage.svc:9000",
                "addressing": "PathStyle"
            },
            "transport": {"security": "TLS",
                          "caBundle": {"configMapName": "minio-a-ca", "key": "ca.crt"}},
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
                }}
            }
        },
        "status": {
            "observedGeneration": 3,
            "conditions": [{"type": "Valid", "status": "True", "reason": "Valid",
                            "observedGeneration": 3}]
        }
    })
}

fn destination_backed_backup() -> Backup {
    let mut value = serde_json::to_value(manual(
        "logweir-manual-dest",
        "00000000-0000-4000-8000-0000000000d3",
        utc(16, 0, 0),
    ))
    .expect("serialises");
    value["spec"]["archive"] = json!({"url": "logweir-destination://dest-a"});
    value["spec"]["destinationRef"] = json!({"name": "dest-a"});
    serde_json::from_value(value).expect("a Backup")
}

fn ca_routes(document: String) -> Vec<Route> {
    vec![
        Route {
            method: "GET",
            path_suffix: "/backupdestinations/dest-a",
            status: 200,
            body: destination_with_ca().to_string(),
        },
        Route {
            method: "GET",
            path_suffix: "/configmaps/minio-a-ca",
            status: 200,
            body: json!({
                "apiVersion": "v1", "kind": "ConfigMap",
                "metadata": {"name": "minio-a-ca", "namespace": NS},
                "data": {"ca.crt": CA_PEM}
            })
            .to_string(),
        },
        policy_route(document),
    ]
}

async fn admit_with(document: String, now: DateTime<Utc>) -> Result<DestinationAdmission, String> {
    let (client, _r, _b) = mock_client_recording_bodies(ca_routes(document));
    admit_destination(&destination_backed_backup(), &client, NS, now)
        .await
        .map_err(|e| e.to_string())
}

/// A `Restore` reading from, and writing its evidence to, the CA-bundled
/// destination. Its `planBytes` are not a plan: the CA gate runs BEFORE the
/// plan is read, so a pass that clears the gate is refused at the plan
/// instead, which is what tells the two halves apart.
fn destination_backed_restore() -> Restore {
    serde_json::from_value(json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "Restore",
        "metadata": {"labels": {"logweir.dev/workspace": WS}, "name": "restore-ca", "namespace": NS,
                     "uid": "cafe0002-0000-4000-8000-0000000000a2",
                     "generation": 1, "resourceVersion": "1"},
        "spec": {
            "planBytes": "not a restore plan",
            "approvalRef": {"name": "a1"},
            "sourceArchive": {"url": "logweir-destination://dest-a"},
            "sourceDestinationRef": {"name": "dest-a"},
            "evidenceDestinationRef": {"name": "dest-a"},
            "backupSetRef": "drill-demo",
            "pointInTime": "2026-09-07T14:05:00Z",
            "target": {"clusterRef": {"name": "scratch"}, "mode": "scratch",
                       "topicNaming": {"prefix": "drill-"}},
            "deadlineSeconds": 1800
        }
    }))
    .expect("the fixture is a Restore")
}

async fn admit_restore_with(document: String, now: DateTime<Utc>) -> Result<(), String> {
    let (client, _r, _b) = mock_client_recording_bodies(ca_routes(document));
    admit_restore_destinations(&destination_backed_restore(), None, &client, NS, now)
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// **`engine.allowUnverifiedCustomCa` reaches backup AND restore admission.**
///
/// A destination that declares a CA bundle is refused
/// (`CaBundleUnsupportedByEngine`) for an engine-driven run unless the
/// installation opts in. The chart's tuned document opts in:
///
/// * the BACKUP gate (`admit_destination`) resolves the destination;
/// * the RESTORE gate (`admit_restore_destinations`) lets it past the CA check
///   to the plan check, where this fixture's non-plan is refused
///   `PlanUnparseable`.
///
/// CONTROL: served the default document, both refuse at the gate with
/// `CaBundleUnsupportedByEngine`. `backup_controller.rs`'s
/// `a_ca_bundle_is_refused_for_an_engine_driven_run` drives the predicate on a
/// hand-built policy; this row drives the policy each gate LOADED.
///
/// MUTANTS: `engine_custom_ca_allowed(&Policy::defaults())` in
/// `admit_destination` or in `admit_restore_destinations`.
#[tokio::test]
async fn the_installation_policys_custom_ca_switch_reaches_backup_and_restore_admission() {
    tuned_environment();
    let at = |h: u32| Utc.with_ymd_and_hms(2026, 11, 9, h, 17, 0).unwrap();
    match admit_with(policy_json("tuned"), at(3)).await {
        Ok(DestinationAdmission::Resolved(resolved)) => {
            assert!(
                resolved.archive.ca_bundle.is_some(),
                "the bundle travels with it"
            );
        }
        other => panic!("the tuned installation opted in, so the destination resolves: {other:?}"),
    }
    match admit_with(policy_json("default"), at(4)).await {
        Err(e) => assert!(e.contains("CaBundleUnsupportedByEngine"), "CONTROL: {e}"),
        Ok(other) => panic!("CONTROL: the default installation refuses it, got {other:?}"),
    }
    // THE RESTORE GATE loads its own policy (a fresh cache each pass).
    match admit_restore_with(policy_json("tuned"), at(5)).await {
        Err(e) => assert!(
            e.contains("PlanUnparseable") && !e.contains("CaBundleUnsupportedByEngine"),
            "the tuned installation opted in, so the restore passes the CA gate and stops at \
             the plan: {e}"
        ),
        Ok(()) => panic!("this fixture's planBytes are not a plan; check 6 must refuse it"),
    }
    match admit_restore_with(policy_json("default"), at(6)).await {
        Err(e) => assert!(e.contains("CaBundleUnsupportedByEngine"), "CONTROL: {e}"),
        Ok(()) => panic!("CONTROL: the default installation refuses it at the gate"),
    }
}

// The retention Job's binding names: the controller's side

/// **The enforcement Job's binding variables are the ones the retention
/// worker reads.**
///
/// `weirkeeper` projects them from `controllers::retention_policy::env`; the
/// worker reads its own `logweir_retention::env`; the two crates share no
/// edge, on purpose (the worker is the deletion boundary), so a rename on
/// either side would be silent and the worker's ceilings would fall back to 50
/// and 20 000. This row reads the WORKER's source and compares; its twin,
/// `logweir-retention/tests/worker.rs`
/// `the_binding_names_are_the_ones_the_controller_projects`, reads this
/// crate's.
///
/// MUTANT: rename `MAX_OBJECTS` on either side.
#[test]
fn the_retention_jobs_binding_names_are_the_ones_the_worker_reads() {
    use weirkeeper::controllers::retention_policy::env;
    let projected: BTreeSet<String> = [
        env::PLAN_SHA256,
        env::POLICY_UID,
        env::POLICY_GENERATION,
        env::SCOPE_PREFIX,
        env::RUN_ID,
        env::APPROVER,
        env::MAX_DELETIONS,
        env::MAX_OBJECTS,
        env::LOCATION,
    ]
    .iter()
    .map(|s| (*s).to_string())
    .collect();
    let source = std::fs::read_to_string(repo().join("crates/logweir-retention/src/lib.rs"))
        .expect("the worker's source");
    let start = source
        .find("pub mod env {")
        .expect("the worker names its binding in `pub mod env`");
    let block = &source[start..];
    let block = &block[..block.find("\n}").expect("the module closes")];
    let read: BTreeSet<String> = block
        .lines()
        .filter_map(|l| {
            let rest = l.trim().strip_prefix("pub const ")?;
            Some(rest.split('"').nth(1)?.to_string())
        })
        .collect();
    assert_eq!(
        projected, read,
        "what weirkeeper projects onto an enforcement Job and what logweir-retention reads must \
         be one set of names"
    );
    assert_eq!(read.len(), 9);
}
