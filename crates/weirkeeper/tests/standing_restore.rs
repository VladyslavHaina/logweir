//! PROD-17.1 C1 (OD-12) — the `Restore` reconciler's STANDING arm is
//! withdrawn.
//!
//! A `Restore` carrying `spec.authorization` (a standing rehearsal
//! authorization) is refused BY NAME at admission, whatever `Approval` exists,
//! and an ordinary `Restore` keeps its per-run shape. The reconcile-level row
//! (zero `POST`s, no `Approval` read) is
//! `restore_controller.rs`'s `a_standing_restore_is_refused_by_name_and_creates_nothing`.

use chrono::{DateTime, TimeZone, Utc};
use weirkeeper::controllers::restore::{
    admit, parse_plan, runner_argv, triggered_by, RestoreAdmission, APPROVAL_DOC_FILE,
};
use weirkeeper::crds::restore::Restore;

const NS: &str = "logweir-plat14-3b";
const SCHEDULE: &str = "weekly-orders";
const SCHEDULE_UID: &str = "3f2a91c7-1111-4222-8333-444444444444";
const APPROVAL: &str = "weekly-orders-standing";
const APPROVAL_UID: &str = "beef-1111";
const TARGET: &str = "kafka-target";
const TARGET_CLUSTER_ID: &str = "TARGET00000000000000000";
const PREFIX: &str = "rehearsal-3f2a91c7-";

fn now() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 9, 20, 3, 30, 0)
        .single()
        .expect("the fixture instant exists")
}

/// The plan every row freezes: inside the scope below, and nothing else.
///
/// The exact `DrillSpec` grammar the runner parses — `plan_scope_facts` reads
/// `target.mode`, the topic prefix, `source.topics`, `sample.max_partitions`
/// and `sample.records_per_partition` out of it, so a fixture that drifted
/// from the real shape would make every scope row vacuous.
fn plan_bytes() -> String {
    format!(
        "source:\n  storage:\n    backend: s3\n    bucket: archives\n    prefix: logweir/\n\
         \n  backup: latestCompleted\n  topics: [orders]\ntarget:\n  bootstrap_servers: \
         [kafka-target:9092]\n  mode: scratch\n  topic_mapping_prefix: \"{PREFIX}\"\n  \
         marker_topic: logweir.scratch\n  default_replication_factor: 1\n  teardown: \
         delete\nsample:\n  window_start: \"2026-09-20T00:00:00Z\"\n  window_end: \
         \"2026-09-20T02:00:00Z\"\n  records_per_partition: 25\n  max_partitions: 200\n  \
         anchor: head\nobjectives: {{}}\nevidence:\n  backend: s3\n  bucket: archives\n  \
         prefix: logweir/\n"
    )
}

fn scope() -> serde_json::Value {
    serde_json::json!({
        "templateDigest": "sha256:aa",
        "targetClusterId": TARGET_CLUSTER_ID,
        "topicPrefix": PREFIX,
        "topics": ["orders"],
        "maxPartitions": 200,
        "recordsPerPartition": 25,
        "deadlineSeconds": 3600,
        "modes": ["scratch"],
    })
}

/// The SIGNED standing authorization, as the bytes `Approval.spec` carries.
fn envelope_with(scope: serde_json::Value, issued: DateTime<Utc>, days: i64) -> String {
    serde_json::to_string(&serde_json::json!({
        "formatVersion": "1.0.0",
        "kind": "StandingRehearsalAuthorization",
        "subjectRef": {
            "apiVersion": "logweir.dev/v1alpha1",
            "kind": "RehearsalSchedule",
            "namespace": NS,
            "name": SCHEDULE,
            "uid": SCHEDULE_UID,
        },
        "scope": scope,
        "issuedAt": issued.to_rfc3339(),
        "expiresAt": (issued + chrono::Duration::days(days)).to_rfc3339(),
    }))
    .expect("the fixture envelope serialises")
}

fn envelope() -> String {
    envelope_with(scope(), now() - chrono::Duration::days(2), 30)
}

/// A standing `Approval`: verified, bound to this schedule's UID, over
/// `envelope`.
fn approval_over(envelope: &str) -> weirkeeper::crds::approval::Approval {
    serde_json::from_value(serde_json::json!({
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "Approval",
        "metadata": {"name": APPROVAL, "namespace": NS, "uid": APPROVAL_UID},
        "spec": {
            "subjectRef": {"kind": "RehearsalSchedule", "name": SCHEDULE},
            "planHash": "sha256:aa",
            "approvalBytes": envelope,
            "sidecarBytes": "{\"signatures\":[]}",
        },
        "status": {
            "verified": true,
            "matchedKeyId": "f27c7f51aad0700db76887b306d413a039156b44ee147c1d82c5e4dc339558f6",
            "verifiedSubjectRef": {
                "apiVersion": "logweir.dev/v1alpha1",
                "kind": "RehearsalSchedule",
                "name": SCHEDULE,
                "namespace": NS,
                "uid": SCHEDULE_UID,
            },
        },
    }))
    .expect("the fixture is an Approval")
}

fn approval() -> weirkeeper::crds::approval::Approval {
    approval_over(&envelope())
}

/// The rehearsal `Restore` a firing slot creates — `spec.authorization`, no
/// `spec.approvalRef`, and the slot label `triggered_by` reads.
fn standing_restore() -> Restore {
    serde_json::from_value(serde_json::json!({
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "Restore",
        "metadata": {
            "name": "logweir-rehearsal-weekly-orders-20260920-030000",
            "namespace": NS,
            "uid": "restore-uid-1",
            "labels": {
                "logweir.dev/rehearsal-schedule": SCHEDULE,
                "logweir.dev/rehearsal-slot": "20260920T030000",
            },
            // The UID of the Approval this slot was authorised against — the
            // `RehearsalSchedule` reconciler writes it, and `admit` requires
            // the object it resolved BY NAME to carry it.
            "annotations": {"logweir.dev/approval-uid": APPROVAL_UID},
        },
        "spec": {
            "planBytes": plan_bytes(),
            "authorization": {
                "kind": "Standing",
                "approvalRef": {"name": APPROVAL},
                "rehearsalScheduleRef": {"name": SCHEDULE},
            },
            "sourceArchive": {"url": "logweir-destination://primary"},
            "sourceDestinationRef": {"name": "primary"},
            "evidenceDestinationRef": {"name": "primary"},
            "backupSetRef": "nightly-7",
            "pointInTime": "2026-09-20T02:00:00Z",
            "target": {"clusterRef": {"name": TARGET}, "mode": "scratch", "topicNaming": {"prefix": PREFIX}},
            "deadlineSeconds": 3600,
        },
    }))
    .expect("the fixture is a Restore")
}

/// An ORDINARY `Restore`: `spec.approvalRef`, no `spec.authorization`.
fn ordinary_restore() -> Restore {
    let mut value: serde_json::Value =
        serde_json::to_value(standing_restore()).expect("the fixture serialises");
    value["spec"]["authorization"] = serde_json::Value::Null;
    value["spec"]["approvalRef"] = serde_json::json!({"name": APPROVAL});
    value["metadata"]["name"] = serde_json::json!("ordinary-restore");
    let mut restore: Restore = serde_json::from_value(value).expect("the fixture is a Restore");
    restore.spec.authorization = None;
    restore
}

fn cluster(reachable: bool) -> weirkeeper::crds::kafka_cluster::KafkaCluster {
    serde_json::from_value(serde_json::json!({
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "KafkaCluster",
        "metadata": {"name": TARGET, "namespace": NS, "uid": "cluster-uid"},
        "spec": {"bootstrapServers": ["kafka-target:9092"], "auth": {"mode": "plaintext"}, "role": "scratch"},
        "status": {"reachable": reachable, "clusterId": TARGET_CLUSTER_ID},
    }))
    .expect("the fixture is a KafkaCluster")
}

// ---------------------------------------------------------------------------
// The named refusal
// ---------------------------------------------------------------------------

/// **A standing `Restore` is refused BY NAME, whatever its `Approval`.** The
/// fixture's standing `Approval` is `Verified=True` and bound to the schedule,
/// and the target is reachable — every input the withdrawn arm used to admit
/// on — and the verdict is still `StandingAuthorizationRefused`. With no
/// `Approval` and no cluster at all it is the same refusal: nothing else is
/// looked at.
///
/// NEGATIVE CONTROL: removing step 0 of `admit_with_policy` makes this row
/// fail — the standing `Restore` has no `spec.approvalRef`, so it then falls
/// to `ApprovalNotReceived` (terminal, but not by name).
#[test]
fn a_standing_restore_is_refused_by_name_whatever_its_approval() {
    for (approval, cluster) in [(Some(approval()), Some(cluster(true))), (None, None)] {
        let verdict = admit(&standing_restore(), approval.as_ref(), cluster.as_ref());
        assert_eq!(
            verdict,
            RestoreAdmission::StandingAuthorizationRefused {
                approval: APPROVAL.to_string()
            }
        );
        assert!(verdict.is_terminal(), "and it creates no Job");
        assert_eq!(verdict.reason(), "StandingAuthorizationRefused");
        let message = verdict.to_string();
        assert!(
            message.contains("spec.authorization") && message.contains("withdrawn in v1"),
            "{message}"
        );
    }
}

// ---------------------------------------------------------------------------
// The per-run path is unchanged
// ---------------------------------------------------------------------------

/// An ordinary `Restore`'s argv carries `--approval` and no standing material,
/// and its trigger names the approval.
#[test]
fn an_ordinary_restore_keeps_its_approval_argv_and_trigger() {
    let restore = ordinary_restore();
    let argv = runner_argv(
        &restore,
        &parse_plan(&restore).expect("the plan parses"),
        &[],
    );
    assert!(
        argv.windows(2)
            .any(|w| w[0] == "--approval" && w[1].ends_with(APPROVAL_DOC_FILE)),
        "{argv:?}"
    );
    for absent in ["--standing-authorization", "--authorization-keys"] {
        assert!(
            !argv.iter().any(|a| a == absent),
            "an ordinary Restore carries no standing material: {argv:?}"
        );
    }
    assert_eq!(triggered_by(&restore), format!("approval/{APPROVAL}"));
}

/// **A standing document never admits an ORDINARY `Restore`.** Its
/// `approvalRef` names the standing `Approval`, and the ordinary path refuses
/// a `RehearsalSchedule` approval as a subject mismatch.
#[test]
fn an_ordinary_restore_is_never_admitted_by_a_standing_document() {
    let verdict = admit(&ordinary_restore(), Some(&approval()), Some(&cluster(true)));
    assert!(
        matches!(verdict, RestoreAdmission::ApprovalSubjectMismatch { .. }),
        "an ordinary Restore is judged on the ordinary path, where a RehearsalSchedule approval \
         is a subject mismatch; got {verdict:?}"
    );
    assert!(verdict.is_terminal(), "and it creates no Job");
}
