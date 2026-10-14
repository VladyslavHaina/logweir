//! **Theme A's PoC defect D1, the controller's half: every Job that reads a
//! restore's evidence reads it with the evidence destination's READ grant.**
//!
//! On the PoC the evidence writer may only `PutObject` under `logweir/`. The
//! `Switchover` Job and a TTL clone's cleanup Job read with it, were refused
//! 403, and the switchover called that `MappingDigestMismatch`. These rows
//! hold the Jobs to the grant table: the `evidenceRead` grant in the archive
//! slot (`AWS_*`, which the runner reads with), the `evidenceWrite` grant in
//! the evidence slot (`LOGWEIR_EVIDENCE_*`, which it writes with), and the
//! read grant named on the argv. The D4 rows hold what a failed clone says.

use k8s_openapi::api::batch::v1::Job;
use serde_json::{json, Value};
use weirkeeper::check::policy::Policy;
use weirkeeper::controllers::restore::restore_clone::{
    carries_read_grant, ended, removal_failed_reason, rewire, Ended, Wiring,
};
use weirkeeper::controllers::switchover::{job_spec, restore_access};
use weirkeeper::crds::backup_destination::BackupDestination;
use weirkeeper::destination::{
    self, evidence_access_env, DestinationRole, EvidenceAccessEnv, ResolvedDestination,
    STORE_ENV_NAMES,
};
use weirkeeper::job::EnvFromSecret;

const NS: &str = "logweir-poc";
const UID: &str = "7a1e0000-0000-4000-8000-0000000000d1";

/// The PoC's shape: the writer may write the archive and only PUT the
/// evidence (`archiveWrite`, and so `evidenceWrite`), the reader reads the
/// archive, and a third user only reads the evidence (`evidenceRead`).
fn poc_destination(evidence_read: Value) -> BackupDestination {
    serde_json::from_value(json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "BackupDestination",
        "metadata": {"name": "poc", "namespace": NS, "uid": UID, "generation": 1,
                     "resourceVersion": "1"},
        "spec": {
            "storage": {"provider": "S3", "bucket": "kafka-backups", "prefix": "poc",
                        "endpoint": "http://logweir-minio.logweir-system.svc:9000",
                        "addressing": "PathStyle"},
            "transport": {"security": "InsecureHTTP"},
            "access": {
                "archiveWrite": {"mode": "SecretKeys", "secret": {"name": "poc-writer"}},
                "archiveRead": {"mode": "SecretKeys", "secret": {"name": "poc-reader"}},
                "evidenceRead": evidence_read
            }
        },
        "status": {"observedGeneration": 1, "reason": "Valid", "conditions": [
            {"type": "Valid", "status": "True", "reason": "Valid", "observedGeneration": 1}]}
    }))
    .expect("a BackupDestination")
}

fn resolved(dest: &BackupDestination, role: DestinationRole) -> ResolvedDestination {
    destination::resolve(dest, role, &Policy::defaults()).expect("resolves")
}

fn poc_access() -> EvidenceAccessEnv {
    let dest = poc_destination(json!({"mode": "SecretKeys", "secret": {"name": "poc-evidence"}}));
    evidence_access_env(
        &resolved(&dest, DestinationRole::EvidenceWrite),
        Some(&resolved(&dest, DestinationRole::EvidenceRead)),
    )
    .expect("renders")
}

fn secret_of<'a>(vars: &'a [EnvFromSecret], name: &str) -> Option<&'a str> {
    vars.iter()
        .find(|v| v.name == name)
        .map(|v| v.secret_name.as_str())
}

/// **The grant table, on the PoC's shape**: the archive slot reads with the
/// `evidenceRead` Secret, the evidence slot writes with the `evidenceWrite`
/// one (the writer), and no variable comes from the archive READ grant, which
/// these Jobs do not use. CONTROL: an `evidenceRead` that IS the write grant
/// renders the evidence slot as `archive`, one credential.
///
/// KILLS: the read slot rendered from the write grant (the PoC's 403); the
/// write slot rendered from the read grant (the record could not be written).
#[test]
fn the_read_slot_is_the_evidence_read_grant_and_the_write_slot_the_writers() {
    let access = poc_access();
    assert_eq!(
        secret_of(&access.read.from_secret, "AWS_ACCESS_KEY_ID"),
        Some("poc-evidence")
    );
    assert_eq!(
        access.read.literal("LOGWEIR_ARCHIVE_CREDENTIALS"),
        Some("static")
    );
    assert_eq!(
        access.write.literal("LOGWEIR_EVIDENCE_CREDENTIALS"),
        Some("static")
    );
    assert_eq!(
        secret_of(
            &access.write.from_secret,
            "LOGWEIR_EVIDENCE_AWS_ACCESS_KEY_ID"
        ),
        Some("poc-writer")
    );
    assert!(
        access
            .read
            .from_secret
            .iter()
            .chain(&access.write.from_secret)
            .all(|v| v.secret_name != "poc-reader"),
        "the archive read grant is not projected"
    );
    assert_eq!(
        access.read_grant,
        format!("evidenceRead of BackupDestination {NS}/poc")
    );

    // CONTROL: one credential for both.
    let same = poc_destination(json!({"mode": "SecretKeys", "secret": {"name": "poc-writer"}}));
    let one = evidence_access_env(
        &resolved(&same, DestinationRole::EvidenceWrite),
        Some(&resolved(&same, DestinationRole::EvidenceRead)),
    )
    .expect("renders");
    assert_eq!(
        one.write.literal("LOGWEIR_EVIDENCE_CREDENTIALS"),
        Some("archive")
    );
    assert!(one.write.from_secret.is_empty());
}

/// **An `evidenceRead` no Job can hold falls back to the write grant, and
/// says so**: not configured, or not resolvable. The read slot is then the
/// writer's, the evidence slot `archive`, and the label names
/// `evidenceWrite` and why; a writer that cannot read then fails by that name
/// (the runner's `EvidenceUnreadable`), never as a verdict.
///
/// KILLS: the fallback naming `evidenceRead` (the operator would fix the wrong
/// grant).
#[test]
fn an_evidence_read_no_job_can_hold_falls_back_to_the_write_grant_by_name() {
    let dest = poc_destination(Value::Null);
    let mut value = serde_json::to_value(&dest).unwrap();
    value["spec"]["access"]
        .as_object_mut()
        .unwrap()
        .remove("evidenceRead");
    let dest: BackupDestination = serde_json::from_value(value).unwrap();
    let write = resolved(&dest, DestinationRole::EvidenceWrite);
    for read in [Some(resolved(&dest, DestinationRole::EvidenceRead)), None] {
        let access = evidence_access_env(&write, read.as_ref()).expect("renders");
        assert_eq!(
            secret_of(&access.read.from_secret, "AWS_ACCESS_KEY_ID"),
            Some("poc-writer")
        );
        assert_eq!(
            access.write.literal("LOGWEIR_EVIDENCE_CREDENTIALS"),
            Some("archive")
        );
        assert!(
            access.read_grant.starts_with(&format!(
                "evidenceWrite of BackupDestination {NS}/poc (its evidenceRead "
            )),
            "{}",
            access.read_grant
        );
    }
}

/// **Every variable the two renderers can write is in
/// [`STORE_ENV_NAMES`]**, the list a copied pod drops before it takes new
/// grants: over a static and a workload-identity grant, a session token and a
/// CA bundle. A renderer that grows a variable without the list fails here,
/// instead of leaving the old grant's value in a cleanup Job. The read slot's
/// CA is the evidence destination's key in the run's plan ConfigMap.
#[test]
fn every_rendered_store_variable_is_listed() {
    let mut value = serde_json::to_value(poc_destination(json!({
        "mode": "WorkloadIdentity", "workloadIdentity": {"serviceAccountName": "ev-reader"}
    })))
    .unwrap();
    value["spec"]["transport"] = json!({"security": "TLS",
        "caBundle": {"configMapName": "poc-ca", "key": "ca.crt"}});
    value["spec"]["storage"]["endpoint"] = json!("https://minio.example:9000");
    value["spec"]["storage"]["region"] = json!("us-east-1");
    value["spec"]["access"]["archiveWrite"]["secret"]["sessionTokenKey"] = json!("token");
    let wi: BackupDestination = serde_json::from_value(value).unwrap();
    let static_access = poc_access();
    let wi_access = evidence_access_env(
        &resolved(&wi, DestinationRole::EvidenceWrite),
        Some(&resolved(&wi, DestinationRole::EvidenceRead)),
    )
    .expect("renders");
    assert_eq!(wi_access.service_account_name(), Some("ev-reader"));
    assert_eq!(
        wi_access.read.literal("LOGWEIR_ARCHIVE_CA_FILE"),
        Some("/plan/evidence-ca.pem")
    );
    for access in [&static_access, &wi_access] {
        for name in access.read.names().into_iter().chain(access.write.names()) {
            assert!(STORE_ENV_NAMES.contains(&name), "{name} is not listed");
        }
    }
}

/// The run Job an older wiring copied: the archive READ grant in the archive
/// slot, the writer in the evidence slot, the target's password beside them.
fn old_run_job() -> Job {
    let secret = |name: &str, secret: &str, key: &str| json!({"name": name, "valueFrom": {"secretKeyRef": {"name": secret, "key": key}}});
    serde_json::from_value(json!({
        "apiVersion": "batch/v1", "kind": "Job",
        "metadata": {"name": "logweir-clone-cleanup-x-1", "namespace": NS},
        "spec": {"template": {"spec": {"restartPolicy": "Never",
            "serviceAccountName": "logweir-runner",
            "containers": [{"name": "runner", "image": "runner:old",
                "args": ["restore", "cleanup", "--spec", "/plan/restore.yaml",
                         "--run-id", "R", "--attempt", "1"],
                "env": [
                    {"name": "LOGWEIR_ARCHIVE_CREDENTIALS", "value": "static"},
                    secret("AWS_ACCESS_KEY_ID", "poc-reader", "access-key-id"),
                    secret("AWS_SECRET_ACCESS_KEY", "poc-reader", "secret-access-key"),
                    {"name": "LOGWEIR_EVIDENCE_CREDENTIALS", "value": "static"},
                    secret("LOGWEIR_EVIDENCE_AWS_ACCESS_KEY_ID", "poc-writer", "access-key-id"),
                    secret("LOGWEIR_TARGET_PASSWORD", "scratch-sasl", "password")
                ]}]}}}
    }))
    .expect("a Job")
}

fn env_secret(job: &Job, name: &str) -> Option<String> {
    job.spec.as_ref()?.template.spec.as_ref()?.containers[0]
        .env
        .as_ref()?
        .iter()
        .find(|v| v.name == name)
        .and_then(|v| v.value_from.as_ref())
        .and_then(|f| f.secret_key_ref.as_ref())
        .map(|r| r.name.clone())
}

/// **A copied cleanup Job takes this build's grants** (D1's upgrade path):
/// the old store variables are dropped and the read and write grants
/// rendered, the target's credential is kept, the read grant is named on the
/// argv (once), and the image is this build's. CONTROL: the Job before
/// [`rewire`] does not carry the read grant.
///
/// KILLS: `rewire` keeping the copied store variables (the old grant reads);
/// dropping a variable outside the store set (the target credential).
#[test]
fn a_copied_cleanup_job_takes_this_builds_grants() {
    let before = old_run_job();
    assert!(!carries_read_grant(&before), "CONTROL: an older Job");
    let access = poc_access();
    let wiring = Wiring {
        read_grant: access.read_grant.clone(),
        env: Some(access),
    };
    let after = rewire(rewire(before, &wiring, "runner:new"), &wiring, "runner:new");
    assert!(carries_read_grant(&after));
    assert_eq!(
        env_secret(&after, "AWS_ACCESS_KEY_ID").as_deref(),
        Some("poc-evidence")
    );
    assert_eq!(
        env_secret(&after, "LOGWEIR_EVIDENCE_AWS_ACCESS_KEY_ID").as_deref(),
        Some("poc-writer")
    );
    assert_eq!(
        env_secret(&after, "LOGWEIR_TARGET_PASSWORD").as_deref(),
        Some("scratch-sasl")
    );
    let c = &after
        .spec
        .as_ref()
        .unwrap()
        .template
        .spec
        .as_ref()
        .unwrap()
        .containers[0];
    assert_eq!(c.image.as_deref(), Some("runner:new"));
    let env = c.env.as_ref().unwrap();
    assert!(
        env.iter().all(|v| {
            v.value_from
                .as_ref()
                .and_then(|f| f.secret_key_ref.as_ref())
                .is_none_or(|r| r.name != "poc-reader")
        }),
        "nothing of the archive read grant is left"
    );
    assert_eq!(
        env.iter().filter(|v| v.name == "AWS_ACCESS_KEY_ID").count(),
        1,
        "one variable per name"
    );
    let args = c.args.as_ref().unwrap();
    assert_eq!(
        args.iter()
            .filter(|a| *a == "--evidence-read-grant")
            .count(),
        1,
        "named once, even rewired twice"
    );
    assert_eq!(
        args.last().map(String::as_str),
        Some(format!("evidenceRead of BackupDestination {NS}/poc").as_str())
    );
}

const PLAN: &str = "\
source:
  storage: {backend: s3, bucket: kafka-backups, prefix: poc, region: us-east-1}
  backup: latestCompleted
  topics: [orders]
target:
  bootstrap_servers: [kafka-target:9092]
  mode: newTopic
  topic_mapping_prefix: \"r-\"
  topic_naming: {prefix: \"r-\"}
sample:
  window_start: \"2026-10-13T00:00:00Z\"
  window_end: \"2026-10-13T01:00:00Z\"
objectives: {}
evidence: {backend: s3, bucket: kafka-backups, prefix: logweir/, region: us-east-1}
";

/// **The `Switchover` Job reads with the read grant and names it**: its pod
/// carries the `evidenceRead` Secret in the archive slot and the writer in
/// the evidence slot, nothing of the archive read grant, and the argv names
/// the read grant after the store-contract handshake. CONTROL: an inline
/// restore's Job names the plan's own credential.
///
/// KILLS: `restore_access` projecting the source destination's archive grant
/// (the read would use it); `runner_argv` without the flag.
#[test]
fn the_switchover_job_reads_with_the_read_grant_and_names_it() {
    let restore: weirkeeper::crds::restore::Restore = serde_json::from_value(json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "Restore",
        "metadata": {"name": "incident", "namespace": NS, "uid": "r-uid"},
        "spec": {"planBytes": PLAN, "approvalRef": {"name": "a"},
                 "sourceArchive": {"url": "s3://kafka-backups/poc",
                                   "secretRef": {"name": "inline-s3"}},
                 "backupSetRef": "b", "pointInTime": "2026-10-13T01:00:00Z",
                 "target": {"clusterRef": {"name": "target"}, "mode": "newTopic",
                            "topicNaming": {"prefix": "r-"}},
                 "deadlineSeconds": 600}
    }))
    .expect("a Restore");
    let plan: logweir_core::spec::RestoreSpec = serde_yaml::from_str(PLAN).expect("a plan");
    let cluster: weirkeeper::crds::kafka_cluster::KafkaCluster = serde_json::from_value(json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "KafkaCluster",
        "metadata": {"name": "target", "namespace": NS, "uid": "c-uid"},
        "spec": {"bootstrapServers": ["kafka-target:9092"],
                 "auth": {"mode": "plaintext", "tls": false}, "role": "target"},
        "status": {"reachable": true, "clusterId": "MkU3OEVBNTcwNTJENDM2Qk"}
    }))
    .expect("a KafkaCluster");
    let sw: weirkeeper::crds::switchover::Switchover = serde_json::from_value(json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "Switchover",
        "metadata": {"name": "sw", "namespace": NS, "uid": "s-uid"},
        "spec": {"planBytes": "p", "approvalRef": {"name": "a"},
                 "restoreRef": {"name": "incident", "uid": "r-uid"}, "deadlineSeconds": 600}
    }))
    .expect("a Switchover");
    let access = poc_access();
    let spec = job_spec(
        &sw,
        restore_access(&restore, &plan, &cluster, Some(&access)).expect("resolves"),
        "logweir/drills/R.json",
        false,
    );
    assert_eq!(
        secret_of(&spec.env_from_secret, "AWS_ACCESS_KEY_ID"),
        Some("poc-evidence")
    );
    assert_eq!(
        secret_of(&spec.env_from_secret, "LOGWEIR_EVIDENCE_AWS_ACCESS_KEY_ID"),
        Some("poc-writer")
    );
    assert!(spec
        .env_from_secret
        .iter()
        .all(|v| v.secret_name != "poc-reader" && v.secret_name != "inline-s3"));
    let at = spec
        .args
        .iter()
        .position(|a| a == "--evidence-read-grant")
        .expect("named");
    assert_eq!(
        spec.args[at + 1],
        format!("evidenceRead of BackupDestination {NS}/poc")
    );
    assert!(spec.args.iter().any(|a| a == "--store-contract-version"));

    // CONTROL: inline.
    let inline = job_spec(
        &sw,
        restore_access(&restore, &plan, &cluster, None).expect("resolves"),
        "logweir/drills/R.json",
        false,
    );
    let at = inline
        .args
        .iter()
        .position(|a| a == "--evidence-read-grant")
        .expect("named");
    assert_eq!(
        inline.args[at + 1],
        weirkeeper::controllers::switchover::INLINE_READ_GRANT
    );
    assert!(!inline.args.iter().any(|a| a == "--store-contract-version"));
}

/// **D4: a clone whose cleanup failed before deleting anything names the
/// cause and points at no list.** The last attempt's `clone-cleanup=` line
/// carried only its error (the PoC's 403): the reason names it, says nothing
/// was deleted, and `status.teardown` is not written. CONTROL: an attempt that
/// left topics points at `status.teardown.failed`, which it writes.
///
/// KILLS: the cause dropped from the reason; the old text that points at an
/// absent `status.teardown.failed`; a teardown written from a report that
/// names no topic.
#[test]
fn a_cleanup_that_deleted_nothing_names_its_cause_and_no_list() {
    let errored = logweir_core::target_creation::CloneCleanupReport {
        run_id: "R".into(),
        error: Some("EvidenceUnreadable: could not read logweir/drills/R.json".into()),
        ..Default::default()
    };
    let Ended::Failed(why, report) = ended(Some(1), Some(errored)) else {
        panic!("a failure");
    };
    assert_eq!(
        why,
        "the cleanup Job exited 1: EvidenceUnreadable: could not read logweir/drills/R.json"
    );
    assert!(
        report.is_none(),
        "no teardown from a report naming no topic"
    );
    let reason = removal_failed_reason(&why, 3, report.as_ref());
    assert!(reason.starts_with(&why), "{reason}");
    assert!(reason.contains("the last deleted nothing"), "{reason}");
    assert!(!reason.contains("status.teardown.failed"), "{reason}");
    // An older runner's attempt printed no line at all: still no list.
    let Ended::Failed(why, report) = ended(Some(1), None) else {
        panic!("a failure");
    };
    assert!(!removal_failed_reason(&why, 3, report.as_ref()).contains("status.teardown"));

    // CONTROL: topics left.
    let left = logweir_core::target_creation::CloneCleanupReport {
        run_id: "R".into(),
        left: vec![logweir_core::target_creation::CloneCleanupLeft {
            topic: "c-a".into(),
            reason: "Broker: Topic authorization failed".into(),
        }],
        attestation_key: Some("logweir/drills/R.cleanup.json".into()),
        ..Default::default()
    };
    let Ended::Failed(why, report) = ended(Some(1), Some(left)) else {
        panic!("a failure");
    };
    assert!(report.is_some());
    assert!(removal_failed_reason(&why, 3, report.as_ref()).contains("status.teardown.failed"));
}
