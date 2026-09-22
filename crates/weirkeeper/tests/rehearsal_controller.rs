//! The `RehearsalSchedule` reconciler and its pure half — PLAT-14.3, D3 §4.
//!
//! EVERY TEST HERE IS A PURE-FUNCTION TEST OR A `mock_client` TEST. The double
//! PANICS on a request it was not given a route for, which is what turns "an
//! out-of-scope plan created no `Restore` and no `ConfigMap`" into an assertion
//! rather than an absence of evidence: the route tables below deliberately KEEP
//! the `POST /restores` and `POST /configmaps` routes present in the refusal
//! tests, so a controller that created either would be recorded, not rejected.
//!
//! READ THESE FIVE FIRST, in this order:
//!
//! 1. [`an_approval_for_another_schedules_uid_never_fires_a_slot`] — the whole
//!    point of D3 §4.3's "checked twice". The mutant is comparing the
//!    `Approval`'s subject NAME instead of its UID, which is exactly what a
//!    deleted-and-recreated schedule would slip through.
//! 2. [`a_plan_outside_the_signed_scope_reaches_no_restore_at_all`] — asserted
//!    over a route table that HAS the `POST` routes.
//! 3. [`the_rendered_bundle_is_what_the_runner_loads`] — the controller's half
//!    of the contract, driven through the runner's own
//!    `logweir_core::execution_contract` loaders and the same digest comparison
//!    `logweir::drill::validate_execution_contract` makes.
//! 4. [`a_revoked_approver_key_refuses_the_next_slot`] — a revocation between
//!    one slot and the next must stop the next slot, which `may_sign_new_for`
//!    answers and "notAfter is in the future" does not.
//! 5. [`leftover_topics_block_the_next_slot_and_the_controller_deletes_nothing`]
//!    — D3 §4.4, over a route table with no `DELETE` route anywhere.

use chrono::{DateTime, TimeZone, Utc};
use serde_json::{json, Value};
use weirkeeper::controllers::rehearsal_schedule as rs;
use weirkeeper::crds::rehearsal_schedule::RehearsalSchedule;
use weirkeeper::rehearsal;
use weirkeeper::testing::{mock_client_recording_bodies, BodyRecorder, Recorder, Route};

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

const NS: &str = "logweir-d3-w7";
const SCHEDULE: &str = "weekly-orders";
const SCHEDULE_UID: &str = "3f2a91c7-1111-4222-8333-444444444444";
const OTHER_UID: &str = "aaaaaaaa-1111-4222-8333-444444444444";
const APPROVAL: &str = "weekly-orders-standing";
const APPROVAL_UID: &str = "beef0000-1111-4222-8333-444444444444";
const TARGET: &str = "kafka-target";
const TARGET_CLUSTER_ID: &str = "scratch-cluster-id";
const SOURCE_CLUSTER_ID: &str = "prod-cluster-id";
const DESTINATION: &str = "primary";
const BACKUP_SCHEDULE: &str = "nightly";

/// An Ed25519 **public** key, SubjectPublicKeyInfo PEM. Public material, and
/// the same one `trust_policy_controller.rs` uses.
const KEY_PEM: &str =
    "-----BEGIN PUBLIC KEY-----\nMCowBQYDK2VwAyEApFpEU8uY5S8Lv43HL4DcXKKyM8WHurCPZIxvq8ZBfpY=\n-----END PUBLIC KEY-----\n";
/// `sha256(KEY_PEM's SPKI DER)`, lowercase hex.
const KEY_ID: &str = "f27c7f51aad0700db76887b306d413a039156b44ee147c1d82c5e4dc339558f6";

const RECEIPT_SHA: &str = "sha256:1111111111111111111111111111111111111111111111111111111111111111";
const MANIFEST_SHA: &str =
    "sha256:2222222222222222222222222222222222222222222222222222222222222222";
/// `lwp1-` + the first 32 hex characters of [`RECEIPT_SHA`].
const POINT_ID: &str = "lwp1-11111111111111111111111111111111";

const SCHEDULE_STATUS_PATH: &str = "/rehearsalschedules/weekly-orders/status";
const RESTORES_PATH: &str = "/restores";
const CONFIGMAPS_PATH: &str = "/configmaps";

fn now() -> DateTime<Utc> {
    // A Sunday at 03:30 UTC: the `0 3 * * 0` slot is due and 30 minutes old.
    Utc.with_ymd_and_hms(2026, 9, 20, 3, 30, 0)
        .single()
        .expect("the fixture instant exists")
}

fn at(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s)
        .expect("a fixture instant")
        .with_timezone(&Utc)
}

// ---------------------------------------------------------------------------
// Builders
// ---------------------------------------------------------------------------

fn spec_value() -> Value {
    json!({
        "schedule": "0 3 * * 0",
        "suspend": false,
        "point": {
            "scheduleRefs": [{"name": BACKUP_SCHEDULE}],
            "selection": "NewestAvailable",
            "minAgeSeconds": 0,
            "topics": ["orders"],
            "requireVerifiedEvidence": true
        },
        "target": {
            "clusterRef": {"name": TARGET},
            "topicPrefix": "rehearsal-",
            "markerTopic": "logweir.scratch",
            "replicationFactor": 1
        },
        "bounds": {
            "concurrencyPolicy": "Forbid",
            "deadlineSeconds": 3600,
            "startingDeadlineSeconds": 3600,
            "recordsPerPartition": 25,
            "maxPartitions": 200
        },
        "authorization": { "standingApprovalRef": {"name": APPROVAL} }
    })
}

fn schedule_value(status: Value) -> Value {
    json!({
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "RehearsalSchedule",
        "metadata": {
            "name": SCHEDULE,
            "namespace": NS,
            "uid": SCHEDULE_UID,
            "generation": 1,
            "resourceVersion": "101"
        },
        "spec": spec_value(),
        "status": status
    })
}

fn schedule_with(status: Value) -> RehearsalSchedule {
    serde_json::from_value(schedule_value(status)).expect("the fixture schedule parses")
}

fn schedule() -> RehearsalSchedule {
    schedule_with(json!({}))
}

/// The digest of this fixture's sealed spec — what the standing authorization
/// signs and what the controller recomputes every slot.
fn template_digest() -> String {
    rehearsal::template_digest(&schedule().spec).expect("the fixture spec canonicalises")
}

/// The rendered prefix this schedule maps through.
fn prefix() -> String {
    rehearsal::rendered_prefix("rehearsal-", SCHEDULE_UID)
}

/// The scope the schedule's sealed spec asks for.
fn scope_value() -> Value {
    json!({
        "templateDigest": template_digest(),
        "targetClusterId": TARGET_CLUSTER_ID,
        "topicPrefix": prefix(),
        "topics": ["orders"],
        "maxPartitions": 200,
        "recordsPerPartition": 25,
        "deadlineSeconds": 3600,
        "modes": ["scratch"]
    })
}

/// The signed standing envelope, as `Approval.spec.approvalBytes` carries it.
///
/// It also carries `plan_hash` and `subject_kind`, which are the fields the
/// `Approval` reconciler's checks 7 and 8 read — the SAME document, read by two
/// readers that name its fields differently. Unknown fields are ignored by
/// both, which is what makes one document enough (W5's report, §R1.3).
fn envelope_with(subject_uid: &str, scope: Value, expires: &str) -> String {
    serde_json::to_string_pretty(&json!({
        "formatVersion": "1.0.0",
        "kind": "StandingRehearsalAuthorization",
        "subjectRef": {
            "apiVersion": "logweir.dev/v1alpha1",
            "kind": "RehearsalSchedule",
            "namespace": NS,
            "name": SCHEDULE,
            "uid": subject_uid
        },
        "scope": scope,
        "issuedAt": "2026-09-01T00:00:00Z",
        "expiresAt": expires,
        "plan_hash": template_digest(),
        "subject_kind": "RehearsalSchedule"
    }))
    .expect("the fixture envelope serialises")
}

fn envelope() -> String {
    envelope_with(SCHEDULE_UID, scope_value(), "2026-11-01T00:00:00Z")
}

fn sidecar() -> String {
    json!({
        "payloadType": "application/vnd.logweir.standing-rehearsal-authorization+json;version=1.0.0",
        "signatures": [{"keyid": KEY_ID, "sig": "ZmFrZQ=="}]
    })
    .to_string()
}

fn approval_value(envelope: &str) -> Value {
    json!({
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "Approval",
        "metadata": {
            "name": APPROVAL,
            "namespace": NS,
            "uid": APPROVAL_UID,
            "resourceVersion": "7"
        },
        "spec": {
            "subjectRef": {"kind": "RehearsalSchedule", "name": SCHEDULE},
            "planHash": template_digest(),
            "approvalBytes": envelope,
            "sidecarBytes": sidecar()
        },
        "status": {
            "verified": true,
            "matchedKeyId": KEY_ID,
            "verifiedSubjectRef": {
                "apiVersion": "logweir.dev/v1alpha1",
                "kind": "RehearsalSchedule",
                "name": SCHEDULE,
                "namespace": NS,
                "uid": SCHEDULE_UID
            }
        }
    })
}

fn trust_policy_value(state: &str, revoked_at: Option<&str>) -> Value {
    let mut key = json!({
        "keyId": KEY_ID,
        "spkiPem": KEY_PEM,
        "algorithm": "ed25519",
        "usages": ["GovernedApproval"],
        "principal": {"id": "person:approver"},
        "notBefore": "2026-01-01T00:00:00Z",
        "notAfter": "2027-06-01T00:00:00Z",
        "state": state
    });
    if let Some(when) = revoked_at {
        key["revokedAt"] = json!(when);
        key["revocationReason"] = json!("KeyCompromise");
        key["revocationEffectiveFrom"] = json!(when);
    }
    json!({
        "apiVersion": "v1",
        "kind": "TrustPolicyList",
        "metadata": {"resourceVersion": "1"},
        "items": [{
            "apiVersion": "logweir.dev/v1alpha1",
            "kind": "TrustPolicy",
            "metadata": {"name": "org-default", "uid": "uid-org-default", "generation": 1, "resourceVersion": "3"},
            "spec": {
                "default": true,
                "allowedTargetClusterIds": [TARGET_CLUSTER_ID],
                "keys": [key]
            }
        }]
    })
}

fn cluster_value(reachable: bool, cluster_id: Option<&str>) -> Value {
    json!({
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "KafkaCluster",
        "metadata": {"name": TARGET, "namespace": NS, "uid": "cluster-uid", "resourceVersion": "5"},
        "spec": {"bootstrapServers": ["kafka-target:9092"], "auth": {"mode": "plaintext"}, "role": "scratch"},
        "status": {"reachable": reachable, "clusterId": cluster_id}
    })
}

fn backup_value(name: &str, capture: &str, topics: Value, verified: bool) -> Value {
    json!({
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "Backup",
        "metadata": {"name": name, "namespace": NS, "uid": format!("uid-{name}"), "resourceVersion": "9", "creationTimestamp": capture},
        "spec": {
            "sourceRef": {"name": "prod-kafka"},
            "topics": topics,
            "archive": {"url": format!("logweir-destination://{DESTINATION}")},
            "destinationRef": {"name": DESTINATION},
            "scheduleRef": {"name": BACKUP_SCHEDULE, "uid": "sched-uid"},
            "triggeredBy": "schedule/nightly",
            "deadlineSeconds": 3600
        },
        "status": {
            "phase": "Succeeded",
            "exitCode": 0,
            "backupId": "b-20260919",
            "manifestSha256": MANIFEST_SHA,
            "capture": {"startedAt": capture},
            "windowCovered": {"fromMs": 1_758_240_000_000_i64, "toMs": 1_758_326_400_000_i64},
            "evidence": {
                "receiptKey": "logweir/backups/b-20260919.json",
                "receiptSha256": RECEIPT_SHA,
                "verification": {"result": if verified {"Valid"} else {"Invalid"}}
            }
        }
    })
}

fn backup_list(items: Vec<Value>) -> String {
    json!({
        "apiVersion": "v1",
        "kind": "BackupList",
        "metadata": {"resourceVersion": "1"},
        "items": items
    })
    .to_string()
}

fn restore_list(items: Vec<Value>) -> String {
    json!({
        "apiVersion": "v1",
        "kind": "RestoreList",
        "metadata": {"resourceVersion": "1"},
        "items": items
    })
    .to_string()
}

/// A page that says there is MORE — `metadata.continue` non-empty.
fn restore_page(items: Vec<Value>) -> String {
    json!({
        "apiVersion": "v1",
        "kind": "RestoreList",
        "metadata": {"resourceVersion": "1", "continue": "eyJwYWdlIjoyfQ"},
        "items": items
    })
    .to_string()
}

/// One finished rehearsal belonging to ANOTHER schedule, against this target.
fn other_schedules_rehearsal(name: &str, phase: &str, outcome: Option<&str>) -> Value {
    json!({
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "Restore",
        "metadata": {
            "name": name,
            "namespace": NS,
            "uid": format!("uid-{name}"),
            "resourceVersion": "3",
            "labels": {
                "logweir.dev/rehearsal-schedule": "weekly-payments",
                "logweir.dev/rehearsal-target": TARGET
            }
        },
        "spec": {
            "planBytes": "{}",
            "sourceArchive": {"url": "s3://x"},
            "backupSetRef": "b",
            "pointInTime": "2026-09-13T02:00:00Z",
            "target": {"clusterRef": {"name": TARGET}, "mode": "scratch", "topicNaming": {"prefix": "rehearsal-"}},
            "deadlineSeconds": 3600
        },
        "status": {"phase": phase, "exitCode": 0, "outcome": outcome}
    })
}

/// Every recorded request, method and path, query string KEPT.
fn seen(recorder: &Recorder) -> Vec<(String, String)> {
    recorder
        .lock()
        .expect("the recorder is not poisoned")
        .iter()
        .map(|r| (r.method.clone(), r.uri.clone()))
        .collect()
}

fn destination_value() -> Value {
    json!({
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "BackupDestination",
        "metadata": {"name": DESTINATION, "namespace": NS, "uid": "dest-uid", "resourceVersion": "4"},
        "spec": {
            "storage": {
                "provider": "S3",
                "bucket": "kafka-backups",
                "prefix": "team-a",
                "region": "us-east-1",
                "endpoint": "https://minio.example:9000",
                "addressing": "PathStyle"
            },
            "transport": {"security": "TLS"},
            "access": {
                "archiveWrite": {"mode": "SecretKeys", "secret": {
                    "name": "lw-writer", "accessKeyIdKey": "id", "secretAccessKeyKey": "key"
                }},
                "archiveRead": {"mode": "SecretKeys", "secret": {
                    "name": "lw-reader", "accessKeyIdKey": "id", "secretAccessKeyKey": "key"
                }}
            }
        }
    })
}

/// The route table a happy slot needs. Every refusal test reuses it, so a
/// controller that created something in a case that must create nothing is
/// RECORDED by the double rather than rejected by it.
fn routes(
    approval: Value,
    policies: Value,
    cluster: Value,
    backups: String,
    busy: String,
) -> Vec<Route> {
    vec![
        Route {
            method: "GET",
            path_suffix: "/trustpolicies",
            status: 200,
            body: policies.to_string(),
        },
        Route {
            method: "GET",
            path_suffix: "/approvals/weekly-orders-standing",
            status: 200,
            body: approval.to_string(),
        },
        Route {
            method: "GET",
            path_suffix: "/kafkaclusters/kafka-target",
            status: 200,
            body: cluster.to_string(),
        },
        Route {
            method: "GET",
            path_suffix: RESTORES_PATH,
            status: 200,
            body: busy,
        },
        Route {
            method: "GET",
            path_suffix: "/backups",
            status: 200,
            body: backups,
        },
        Route {
            method: "GET",
            path_suffix: "/backupdestinations/primary",
            status: 200,
            body: destination_value().to_string(),
        },
        Route {
            method: "PATCH",
            path_suffix: SCHEDULE_STATUS_PATH,
            status: 200,
            body: schedule_value(json!({})).to_string(),
        },
        Route {
            method: "POST",
            path_suffix: RESTORES_PATH,
            status: 201,
            body: json!({
                "apiVersion": "logweir.dev/v1alpha1",
                "kind": "Restore",
                "metadata": {"name": "logweir-rehearsal-weekly-orders-20260920-030000", "namespace": NS, "uid": "child-uid", "resourceVersion": "1"},
                "spec": {
                    "planBytes": "{}",
                    "authorization": {
                        "kind": "Standing",
                        "approvalRef": {"name": APPROVAL},
                        "rehearsalScheduleRef": {"name": SCHEDULE}
                    },
                    "sourceArchive": {"url": "logweir-destination://primary"},
                    "backupSetRef": "b-20260919",
                    "pointInTime": "2026-09-20T02:00:00Z",
                    "target": {"clusterRef": {"name": TARGET}, "mode": "scratch", "topicNaming": {"prefix": "rehearsal-"}},
                    "deadlineSeconds": 3600
                }
            })
            .to_string(),
        },
        Route {
            method: "POST",
            path_suffix: CONFIGMAPS_PATH,
            status: 201,
            body: json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": "logweir-rehearsal-weekly-orders-20260920-030000-approval", "namespace": NS, "resourceVersion": "1"}
            })
            .to_string(),
        },
    ]
}

fn happy_routes() -> Vec<Route> {
    routes(
        approval_value(&envelope()),
        trust_policy_value("Active", None),
        cluster_value(true, Some(TARGET_CLUSTER_ID)),
        backup_list(vec![backup_value(
            "logweir-backup-nightly-20260919-020000",
            "2026-09-19T02:00:00Z",
            json!(["orders", "payments"]),
            true,
        )]),
        restore_list(vec![]),
    )
}

fn context(client: kube::Client) -> weirkeeper::controllers::Context {
    weirkeeper::controllers::Context {
        client,
        archive: None,
        runner_image: weirkeeper::job::RunnerImage::default(),
    }
}

/// Whether a recorded request is a `POST` to this path, query string dropped —
/// `Api::create` appends `?fieldManager=…`, which is the client's business.
fn is_post_to(method: &str, uri: &str, suffix: &str) -> bool {
    method == "POST" && uri.split('?').next().unwrap_or("").ends_with(suffix)
}

fn posted(recorder: &Recorder, suffix: &str) -> usize {
    recorder
        .lock()
        .expect("the recorder is not poisoned")
        .iter()
        .filter(|r| is_post_to(&r.method, &r.uri, suffix))
        .count()
}

fn patch_bodies(bodies: &BodyRecorder) -> Vec<Value> {
    bodies
        .lock()
        .expect("the body recorder is not poisoned")
        .iter()
        .filter(|seen| seen.method == "PATCH")
        .filter_map(|seen| serde_json::from_str::<Value>(&seen.body).ok())
        .collect()
}

fn last_skip(bodies: &BodyRecorder) -> Option<String> {
    patch_bodies(bodies)
        .into_iter()
        .filter_map(|b| {
            b.pointer("/status/lastSkipped/reason")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .next_back()
}

// ===========================================================================
// D3 §13 — PLAT-14.3's rows
// ===========================================================================

/// **Missing recovery point.** `select_point` empties, the slot is skipped and
/// recorded, and NO `Restore` is created — over a route table that has the
/// `POST /restores` route.
#[tokio::test]
async fn a_slot_with_no_qualifying_point_is_recorded_and_creates_nothing() {
    let (client, recorder, bodies) = mock_client_recording_bodies(routes(
        approval_value(&envelope()),
        trust_policy_value("Active", None),
        cluster_value(true, Some(TARGET_CLUSTER_ID)),
        backup_list(vec![]),
        restore_list(vec![]),
    ));
    let outcome = rs::reconcile_schedule(&schedule(), &context(client), now())
        .await
        .expect("the reconcile answers");
    assert!(
        matches!(&outcome.verdict, rs::Verdict::Skipped(s) if s.reason == rehearsal::SkipReason::NoQualifyingPoint),
        "{:?}",
        outcome.verdict
    );
    assert_eq!(posted(&recorder, RESTORES_PATH), 0, "no Restore is created");
    assert_eq!(
        posted(&recorder, CONFIGMAPS_PATH),
        0,
        "no bundle is written"
    );
    assert_eq!(last_skip(&bodies).as_deref(), Some("NoQualifyingPoint"));
}

/// **Unavailable target.** A cluster that does not report `reachable: true` is
/// a `TargetUnavailable` skip, not a run against a broker nobody probed.
#[tokio::test]
async fn an_unreachable_target_is_a_recorded_target_unavailable_skip() {
    let (client, recorder, bodies) = mock_client_recording_bodies(routes(
        approval_value(&envelope()),
        trust_policy_value("Active", None),
        cluster_value(false, Some(TARGET_CLUSTER_ID)),
        backup_list(vec![]),
        restore_list(vec![]),
    ));
    let outcome = rs::reconcile_schedule(&schedule(), &context(client), now())
        .await
        .expect("the reconcile answers");
    assert!(
        matches!(&outcome.verdict, rs::Verdict::Skipped(s) if s.reason == rehearsal::SkipReason::TargetUnavailable),
        "{:?}",
        outcome.verdict
    );
    assert_eq!(posted(&recorder, RESTORES_PATH), 0);
    assert_eq!(last_skip(&bodies).as_deref(), Some("TargetUnavailable"));
}

/// A cluster that IS reachable but reports no id is also `TargetUnavailable`:
/// the signed scope names a cluster ID, and `spec.role` is not authority.
#[tokio::test]
async fn a_target_with_no_reported_cluster_id_is_refused() {
    let (client, _, _) = mock_client_recording_bodies(routes(
        approval_value(&envelope()),
        trust_policy_value("Active", None),
        cluster_value(true, None),
        backup_list(vec![]),
        restore_list(vec![]),
    ));
    let outcome = rs::reconcile_schedule(&schedule(), &context(client), now())
        .await
        .expect("the reconcile answers");
    let rs::Verdict::Skipped(skip) = &outcome.verdict else {
        panic!("{:?}", outcome.verdict)
    };
    assert_eq!(skip.reason, rehearsal::SkipReason::TargetUnavailable);
    assert!(skip.detail.contains("status.clusterId"), "{skip}");
}

/// **Approval policy, first arm.** An absent standing `Approval` is a recorded
/// refusal with zero `ConfigMap` and zero `Restore` calls.
#[tokio::test]
async fn an_absent_standing_approval_creates_nothing() {
    let mut table = happy_routes();
    for route in &mut table {
        if route.path_suffix == "/approvals/weekly-orders-standing" {
            route.status = 404;
            route.body = r#"{"kind":"Status","apiVersion":"v1","status":"Failure","message":"approvals.logweir.dev \"weekly-orders-standing\" not found","reason":"NotFound","code":404}"#.to_string();
        }
    }
    let (client, recorder, bodies) = mock_client_recording_bodies(table);
    let outcome = rs::reconcile_schedule(&schedule(), &context(client), now())
        .await
        .expect("the reconcile answers");
    assert!(
        matches!(&outcome.verdict, rs::Verdict::Skipped(s) if s.reason == rehearsal::SkipReason::AuthorizationInvalid),
        "{:?}",
        outcome.verdict
    );
    assert_eq!(posted(&recorder, RESTORES_PATH), 0);
    assert_eq!(posted(&recorder, CONFIGMAPS_PATH), 0);
    assert_eq!(last_skip(&bodies).as_deref(), Some("AuthorizationInvalid"));
}

/// **Approval policy, second arm.** An EXPIRED standing authorization is its
/// own reason — `AuthorizationExpired`, the runner's own word for the same
/// fault — and creates nothing.
#[tokio::test]
async fn an_expired_standing_authorization_creates_nothing() {
    let expired = envelope_with(SCHEDULE_UID, scope_value(), "2026-09-10T00:00:00Z");
    let (client, recorder, bodies) = mock_client_recording_bodies(routes(
        approval_value(&expired),
        trust_policy_value("Active", None),
        cluster_value(true, Some(TARGET_CLUSTER_ID)),
        backup_list(vec![]),
        restore_list(vec![]),
    ));
    let outcome = rs::reconcile_schedule(&schedule(), &context(client), now())
        .await
        .expect("the reconcile answers");
    assert!(
        matches!(&outcome.verdict, rs::Verdict::Skipped(s) if s.reason == rehearsal::SkipReason::AuthorizationExpired),
        "{:?}",
        outcome.verdict
    );
    assert_eq!(posted(&recorder, RESTORES_PATH), 0);
    assert_eq!(last_skip(&bodies).as_deref(), Some("AuthorizationExpired"));
}

/// **Approval policy, third arm — the brief's own row.** An `Approval` verified
/// against ANOTHER schedule's UID fires nothing.
///
/// MUTANT: comparing `verifiedSubjectRef.name` instead of `.uid` makes this
/// pass, because a deleted-and-recreated schedule reuses the name.
#[tokio::test]
async fn an_approval_for_another_schedules_uid_never_fires_a_slot() {
    let mut approval = approval_value(&envelope_with(
        OTHER_UID,
        scope_value(),
        "2026-11-01T00:00:00Z",
    ));
    approval["status"]["verifiedSubjectRef"]["uid"] = json!(OTHER_UID);
    let (client, recorder, bodies) = mock_client_recording_bodies(routes(
        approval,
        trust_policy_value("Active", None),
        cluster_value(true, Some(TARGET_CLUSTER_ID)),
        backup_list(vec![]),
        restore_list(vec![]),
    ));
    let outcome = rs::reconcile_schedule(&schedule(), &context(client), now())
        .await
        .expect("the reconcile answers");
    let rs::Verdict::Skipped(skip) = &outcome.verdict else {
        panic!("{:?}", outcome.verdict)
    };
    assert_eq!(skip.reason, rehearsal::SkipReason::AuthorizationInvalid);
    assert!(skip.detail.contains("uid"), "{skip}");
    assert_eq!(posted(&recorder, RESTORES_PATH), 0);
    assert_eq!(last_skip(&bodies).as_deref(), Some("AuthorizationInvalid"));
}

/// **The brief's own row.** A signed scope the rendered plan exceeds refuses
/// BEFORE any `Restore` exists — asserted over a table that HAS both `POST`
/// routes.
///
/// The scope here permits five records per partition and the sealed spec asks
/// for twenty-five, so `plan_within_scope` refuses on a plan field.
#[tokio::test]
async fn a_plan_outside_the_signed_scope_reaches_no_restore_at_all() {
    let mut scope = scope_value();
    scope["recordsPerPartition"] = json!(5);
    let (client, recorder, bodies) = mock_client_recording_bodies(routes(
        approval_value(&envelope_with(SCHEDULE_UID, scope, "2026-11-01T00:00:00Z")),
        trust_policy_value("Active", None),
        cluster_value(true, Some(TARGET_CLUSTER_ID)),
        backup_list(vec![backup_value(
            "logweir-backup-nightly-20260919-020000",
            "2026-09-19T02:00:00Z",
            json!(["orders", "payments"]),
            true,
        )]),
        restore_list(vec![]),
    ));
    let outcome = rs::reconcile_schedule(&schedule(), &context(client), now())
        .await
        .expect("the reconcile answers");
    let rs::Verdict::Skipped(skip) = &outcome.verdict else {
        panic!("{:?}", outcome.verdict)
    };
    assert_eq!(skip.reason, rehearsal::SkipReason::AuthorizationInvalid);
    assert!(
        skip.detail.contains("records per partition"),
        "the refusal names the field: {skip}"
    );
    assert_eq!(
        posted(&recorder, RESTORES_PATH),
        0,
        "an out-of-scope plan creates no Restore"
    );
    assert_eq!(
        posted(&recorder, CONFIGMAPS_PATH),
        0,
        "and therefore no bundle"
    );
    assert_eq!(last_skip(&bodies).as_deref(), Some("AuthorizationInvalid"));
}

/// A scope bound to ANOTHER template digest is refused before the plan is even
/// rendered — D3 §4.3(a), the check the runner structurally cannot make.
#[test]
fn a_scope_bound_to_another_template_is_refused() {
    let expected = rehearsal::expected_scope(
        &schedule().spec,
        &template_digest(),
        TARGET_CLUSTER_ID,
        &prefix(),
    );
    let mut signed = expected.clone();
    signed.template_digest = "sha256:0000".to_string();
    let refusal =
        rehearsal::scope_agrees(&signed, &expected).expect_err("a different template is refused");
    assert!(refusal.contains("templateDigest"), "{refusal}");
    assert!(
        rehearsal::scope_agrees(&expected, &expected).is_ok(),
        "the agreeing case still passes"
    );
}

/// The controller's half of D3 §4.3(d) that the shared predicate does not make:
/// a deadline the signed scope does not cover.
#[test]
fn a_deadline_the_signed_scope_does_not_cover_is_refused() {
    let expected = rehearsal::expected_scope(
        &schedule().spec,
        &template_digest(),
        TARGET_CLUSTER_ID,
        &prefix(),
    );
    let mut signed = expected.clone();
    signed.deadline_seconds = 600;
    let refusal = rehearsal::scope_agrees(&signed, &expected).expect_err("refused");
    assert!(refusal.contains("deadlineSeconds"), "{refusal}");
}

/// **Overlapping drill, first arm.** `Forbid` under a second reconcile: this
/// schedule's own unfinished child blocks the slot.
#[tokio::test]
async fn this_schedules_unfinished_rehearsal_blocks_the_next_slot() {
    let running = json!({
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "Restore",
        "metadata": {"name": "logweir-rehearsal-weekly-orders-20260913-030000", "namespace": NS, "uid": "r1", "resourceVersion": "3"},
        "spec": {
            "planBytes": "{}",
            "sourceArchive": {"url": "s3://x"},
            "backupSetRef": "b",
            "pointInTime": "2026-09-13T02:00:00Z",
            "target": {"clusterRef": {"name": TARGET}, "mode": "scratch", "topicNaming": {"prefix": "rehearsal-"}},
            "deadlineSeconds": 3600
        },
        "status": {"phase": "Running"}
    });
    let mut table = happy_routes();
    table.push(Route {
        method: "GET",
        path_suffix: "/restores/logweir-rehearsal-weekly-orders-20260913-030000",
        status: 200,
        body: running.to_string(),
    });
    let (client, recorder, bodies) = mock_client_recording_bodies(table);
    let schedule = schedule_with(json!({
        "activeRestoreRef": {"name": "logweir-rehearsal-weekly-orders-20260913-030000"}
    }));
    let outcome = rs::reconcile_schedule(&schedule, &context(client), now())
        .await
        .expect("the reconcile answers");
    assert!(
        matches!(&outcome.verdict, rs::Verdict::Skipped(s) if s.reason == rehearsal::SkipReason::ConcurrencyBlocked),
        "{:?}",
        outcome.verdict
    );
    assert_eq!(posted(&recorder, RESTORES_PATH), 0);
    assert_eq!(last_skip(&bodies).as_deref(), Some("ConcurrencyBlocked"));
}

/// **Overlapping drill, second arm.** ANOTHER schedule rehearsing against the
/// same target cluster is `TargetBusy` — a different reason, because the remedy
/// is different.
#[tokio::test]
async fn another_schedules_rehearsal_on_the_same_target_is_target_busy() {
    let other = json!({
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "Restore",
        "metadata": {
            "name": "logweir-rehearsal-weekly-payments-20260920-030000",
            "namespace": NS,
            "uid": "r2",
            "resourceVersion": "3",
            "labels": {
                "logweir.dev/rehearsal-schedule": "weekly-payments",
                "logweir.dev/rehearsal-target": TARGET
            }
        },
        "spec": {
            "planBytes": "{}",
            "sourceArchive": {"url": "s3://x"},
            "backupSetRef": "b",
            "pointInTime": "2026-09-20T02:00:00Z",
            "target": {"clusterRef": {"name": TARGET}, "mode": "scratch", "topicNaming": {"prefix": "rehearsal-"}},
            "deadlineSeconds": 3600
        },
        "status": {"phase": "Running"}
    });
    let (client, recorder, bodies) = mock_client_recording_bodies(routes(
        approval_value(&envelope()),
        trust_policy_value("Active", None),
        cluster_value(true, Some(TARGET_CLUSTER_ID)),
        backup_list(vec![]),
        restore_list(vec![other]),
    ));
    let outcome = rs::reconcile_schedule(&schedule(), &context(client), now())
        .await
        .expect("the reconcile answers");
    assert!(
        matches!(&outcome.verdict, rs::Verdict::Skipped(s) if s.reason == rehearsal::SkipReason::TargetBusy),
        "{:?}",
        outcome.verdict
    );
    assert_eq!(posted(&recorder, RESTORES_PATH), 0);
    assert_eq!(last_skip(&bodies).as_deref(), Some("TargetBusy"));
}

/// A FINISHED rehearsal against the same target does NOT block: `TargetBusy` is
/// about a broker in use, not about a label that exists.
#[tokio::test]
async fn a_finished_rehearsal_on_the_same_target_does_not_block() {
    let finished = json!({
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "Restore",
        "metadata": {
            "name": "logweir-rehearsal-weekly-payments-20260913-030000",
            "namespace": NS,
            "uid": "r3",
            "resourceVersion": "3",
            "labels": {
                "logweir.dev/rehearsal-schedule": "weekly-payments",
                "logweir.dev/rehearsal-target": TARGET
            }
        },
        "spec": {
            "planBytes": "{}",
            "sourceArchive": {"url": "s3://x"},
            "backupSetRef": "b",
            "pointInTime": "2026-09-13T02:00:00Z",
            "target": {"clusterRef": {"name": TARGET}, "mode": "scratch", "topicNaming": {"prefix": "rehearsal-"}},
            "deadlineSeconds": 3600
        },
        "status": {"phase": "Succeeded", "exitCode": 0, "outcome": "pass"}
    });
    let (client, recorder, _) = mock_client_recording_bodies(routes(
        approval_value(&envelope()),
        trust_policy_value("Active", None),
        cluster_value(true, Some(TARGET_CLUSTER_ID)),
        backup_list(vec![backup_value(
            "logweir-backup-nightly-20260919-020000",
            "2026-09-19T02:00:00Z",
            json!(["orders", "payments"]),
            true,
        )]),
        restore_list(vec![finished]),
    ));
    let outcome = rs::reconcile_schedule(&schedule(), &context(client), now())
        .await
        .expect("the reconcile answers");
    assert!(
        matches!(outcome.verdict, rs::Verdict::Fire(_)),
        "{:?}",
        outcome.verdict
    );
    assert_eq!(posted(&recorder, RESTORES_PATH), 1);
}

/// **Failed verification.** A rehearsal `Restore` with `outcome: fail-integrity`
/// sets `RehearsalHealthy=False` — which is D3 §3's `RehearsalFailure` input.
#[tokio::test]
async fn a_failed_rehearsal_sets_rehearsal_healthy_false() {
    let failed = json!({
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "Restore",
        "metadata": {"name": "logweir-rehearsal-weekly-orders-20260913-030000", "namespace": NS, "uid": "r1", "resourceVersion": "3"},
        "spec": {
            "planBytes": "{}",
            "sourceArchive": {"url": "s3://x"},
            "backupSetRef": "b",
            "pointInTime": "2026-09-13T02:00:00Z",
            "target": {"clusterRef": {"name": TARGET}, "mode": "scratch", "topicNaming": {"prefix": "rehearsal-"}},
            "deadlineSeconds": 3600
        },
        "status": {"phase": "Failed", "exitCode": 2, "outcome": "fail-integrity"}
    });
    let mut table = happy_routes();
    table.push(Route {
        method: "GET",
        path_suffix: "/restores/logweir-rehearsal-weekly-orders-20260913-030000",
        status: 200,
        body: failed.to_string(),
    });
    let (client, _, bodies) = mock_client_recording_bodies(table);
    let schedule = schedule_with(json!({
        "activeRestoreRef": {"name": "logweir-rehearsal-weekly-orders-20260913-030000"}
    }));
    rs::reconcile_schedule(&schedule, &context(client), now())
        .await
        .expect("the reconcile answers");
    let patch = patch_bodies(&bodies)
        .into_iter()
        .find(|b| b.pointer("/status/conditions").is_some())
        .expect("a status patch carries the conditions");
    let conditions = patch["status"]["conditions"]
        .as_array()
        .expect("conditions is an array");
    let health = conditions
        .iter()
        .find(|c| c["type"] == "RehearsalHealthy")
        .expect("RehearsalHealthy is published");
    assert_eq!(health["status"], "False");
    assert_eq!(health["reason"], "Failed");
    assert_eq!(
        patch
            .pointer("/status/lastFailed/reason")
            .and_then(Value::as_str),
        Some("fail-integrity"),
        "and the reason is recorded verbatim"
    );
}

/// **Cleanup failure.** Leftover topics block the next slot, and there is no
/// `DELETE` route anywhere in this table — the controller deletes nothing.
#[tokio::test]
async fn leftover_topics_block_the_next_slot_and_the_controller_deletes_nothing() {
    let (client, recorder, bodies) = mock_client_recording_bodies(vec![Route {
        method: "PATCH",
        path_suffix: SCHEDULE_STATUS_PATH,
        status: 200,
        body: schedule_value(json!({})).to_string(),
    }]);
    let schedule = schedule_with(json!({
        "cleanup": {"pendingTopics": ["rehearsal-3f2a91c7-orders"], "since": "2026-09-13T04:00:00Z"}
    }));
    // The trust list and the approval GET are NOT in the table: reaching them
    // would panic the double, which is how "leftover topics are checked before
    // anything else is read" becomes an assertion.
    let outcome = rs::reconcile_schedule(&schedule, &context(client), now())
        .await
        .expect("the reconcile answers");
    assert!(
        matches!(&outcome.verdict, rs::Verdict::Skipped(s) if s.reason == rehearsal::SkipReason::LeftoverTopics),
        "{:?}",
        outcome.verdict
    );
    assert_eq!(posted(&recorder, RESTORES_PATH), 0);
    assert!(
        recorder
            .lock()
            .expect("the recorder is not poisoned")
            .iter()
            .all(|r| r.method != "DELETE"),
        "the controller deletes nothing, ever"
    );
    assert_eq!(last_skip(&bodies).as_deref(), Some("LeftoverTopics"));
}

// ===========================================================================
// The brief's own rows
// ===========================================================================

/// The happy slot: one deterministic `Restore`, one bundle, the reservation
/// first, and the standing authorization on the child's spec.
#[tokio::test]
async fn a_due_slot_reserves_then_creates_the_deterministic_restore_and_its_bundle() {
    let (client, recorder, bodies) = mock_client_recording_bodies(happy_routes());
    let outcome = rs::reconcile_schedule(&schedule(), &context(client), now())
        .await
        .expect("the reconcile answers");
    let rs::Verdict::Fire(order) = &outcome.verdict else {
        panic!("{:?}", outcome.verdict)
    };
    assert_eq!(order.slot, "20260920-030000");
    assert_eq!(order.selected.point.point_id, POINT_ID);
    assert_eq!(
        outcome.created.as_deref(),
        Some("logweir-rehearsal-weekly-orders-20260920-030000")
    );
    assert_eq!(posted(&recorder, RESTORES_PATH), 1, "exactly one Restore");
    assert_eq!(posted(&recorder, CONFIGMAPS_PATH), 1, "exactly one bundle");

    // THE RESERVATION CAME FIRST, and it carried the precondition.
    let seen = recorder
        .lock()
        .expect("the recorder is not poisoned")
        .clone();
    let first_patch = seen
        .iter()
        .position(|r| r.method == "PATCH")
        .expect("a status patch happened");
    let create = seen
        .iter()
        .position(|r| is_post_to(&r.method, &r.uri, RESTORES_PATH))
        .expect("the Restore was created");
    assert!(
        first_patch < create,
        "the reservation is written before the child exists"
    );
    let reservation = patch_bodies(&bodies)
        .into_iter()
        .find(|b| b.pointer("/status/pendingRestoreRef").is_some())
        .expect("the reservation patch");
    assert_eq!(reservation["metadata"]["resourceVersion"], "101");
    assert_eq!(
        reservation
            .pointer("/status/pendingRestoreRef/name")
            .and_then(Value::as_str),
        Some("logweir-rehearsal-weekly-orders-20260920-030000")
    );

    // The child: standing authorization, no approvalRef, scratch mode, the
    // rendered prefix, and the owner that does NOT block deletion.
    let child = bodies
        .lock()
        .expect("the body recorder is not poisoned")
        .iter()
        .find(|seen| is_post_to(&seen.method, &seen.uri, RESTORES_PATH))
        .map(|seen| serde_json::from_str::<Value>(&seen.body).expect("the child parses"))
        .expect("the child was posted");
    assert!(
        child["spec"]["approvalRef"].is_null(),
        "no per-run approval"
    );
    assert_eq!(
        child
            .pointer("/spec/authorization/kind")
            .and_then(Value::as_str),
        Some("Standing")
    );
    assert_eq!(
        child
            .pointer("/spec/authorization/approvalRef/name")
            .and_then(Value::as_str),
        Some(APPROVAL)
    );
    assert_eq!(
        child
            .pointer("/spec/authorization/rehearsalScheduleRef/name")
            .and_then(Value::as_str),
        Some(SCHEDULE)
    );
    assert_eq!(
        child.pointer("/spec/target/mode").and_then(Value::as_str),
        Some("scratch")
    );
    assert_eq!(
        child
            .pointer("/spec/target/topicNaming/prefix")
            .and_then(Value::as_str),
        Some(prefix().as_str())
    );
    assert_eq!(
        child
            .pointer("/metadata/ownerReferences/0/blockOwnerDeletion")
            .and_then(Value::as_bool),
        Some(false),
        "deleting the schedule must never be blocked by a rehearsal in flight"
    );
    assert_eq!(
        child
            .pointer("/metadata/labels/logweir.dev~1rehearsal-target")
            .and_then(Value::as_str),
        Some(TARGET)
    );
    assert_eq!(
        child
            .pointer("/spec/sourceArchive/url")
            .and_then(Value::as_str),
        Some("logweir-destination://primary"),
        "the destination sentinel, so the CEL rule admits it"
    );
    // **THE APPROVAL'S UID, PINNED ON THE CHILD.**
    // `spec.authorization.approvalRef` is a `LocalRef` and carries a name
    // only, so the `Restore` reconciler resolves the standing `Approval` by
    // name and would otherwise accept a DIFFERENT object that later took that
    // name. This annotation is the UID of the object THIS slot authorised
    // against, and `restore::admit` requires the resolved Approval to carry it
    // — a blank one is refused, so this asserts the VALUE and not the key.
    assert_eq!(
        child
            .pointer("/metadata/annotations/logweir.dev~1approval-uid")
            .and_then(Value::as_str),
        Some(APPROVAL_UID),
        "the child pins the Approval object the slot was authorised against"
    );
}

/// **The brief's own row.** The rendered bundle is what the runner loads: every
/// digest the contract pins equals `sha256_prefixed` of the mounted member (the
/// exact comparison `logweir::drill::validate_execution_contract` makes), the
/// keyring parses, the envelope parses and is admitted, and the plan falls
/// inside the scope the envelope carries.
#[tokio::test]
async fn the_rendered_bundle_is_what_the_runner_loads() {
    use logweir_core::execution_contract as wire;

    let (client, _, bodies) = mock_client_recording_bodies(happy_routes());
    rs::reconcile_schedule(&schedule(), &context(client), now())
        .await
        .expect("the reconcile answers");

    let recorded = bodies
        .lock()
        .expect("the body recorder is not poisoned")
        .clone();
    let bundle: Value = recorded
        .iter()
        .find(|seen| is_post_to(&seen.method, &seen.uri, CONFIGMAPS_PATH))
        .map(|seen| serde_json::from_str(&seen.body).expect("the bundle parses"))
        .expect("the bundle was posted");
    let child: Value = recorded
        .iter()
        .find(|seen| is_post_to(&seen.method, &seen.uri, RESTORES_PATH))
        .map(|seen| serde_json::from_str(&seen.body).expect("the child parses"))
        .expect("the child was posted");

    assert_eq!(bundle["immutable"], true, "the bundle is immutable");
    let data = bundle["data"].as_object().expect("the bundle carries data");
    for member in [
        // THE STANDING DOCUMENT HAS ITS OWN NAME. D3 W5's landed fixture mounts
        // it at `standing-authorization.json` with its sidecar DERIVED at
        // `.sig`; writing it into `approval.json` instead makes the runner
        // verify it under `PAYLOAD_TYPE_APPROVAL` and report a correctly signed
        // rehearsal as a SUBSTITUTED approval.
        "standing-authorization.json",
        "standing-authorization.sig",
        "authorization-keys.json",
        "allowed-clusters.json",
        "approver.pub.pem",
    ] {
        assert!(data.contains_key(member), "the bundle carries {member}");
    }
    // **FIVE MEMBERS, AND NO PER-RUN APPROVAL SLOT** — PLAT-14.3b. D3 W7 wrote
    // the standing envelope into `approval.json` as a placeholder because
    // `--approval` was mandatory; the runner then verified it under
    // `PAYLOAD_TYPE_APPROVAL` and reported a correctly signed rehearsal as a
    // SUBSTITUTED approval, which is why no rehearsal could execute. The
    // standing document now REPLACES the per-run approval.
    for absent in ["approval.json", "approval.sig"] {
        assert!(
            !data.contains_key(absent),
            "a rehearsal bundle carries no per-run approval slot, and {absent} is in it"
        );
    }
    assert_eq!(data.len(), 5, "exactly the five members of the file table");
    assert_eq!(
        data["standing-authorization.sig"]
            .as_str()
            .expect("a string"),
        sidecar(),
        "the sidecar is copied verbatim to the path the runner DERIVES"
    );

    // ---- the keyring: parses, carries the approver key, no private material
    let keyring: wire::AuthorizationKeyring =
        serde_json::from_str(data["authorization-keys.json"].as_str().expect("a string"))
            .expect("the keyring is the shape the runner parses");
    assert!(
        !keyring.keys.is_empty(),
        "an empty keyring is refused by the runner"
    );
    let key = keyring
        .keys
        .iter()
        .find(|k| k.key_id == KEY_ID)
        .expect("the approver key");
    assert!(key.may_authorize(), "and it is allowed to authorise");
    assert!(
        !keyring
            .keys
            .iter()
            .any(|k| k.public_key_pem.contains("PRIVATE")),
        "no private material ever reaches a bundle"
    );

    // ---- the envelope: parses, and the runner's own admission accepts it
    let doc: wire::StandingAuthorization = serde_json::from_str(
        data["standing-authorization.json"]
            .as_str()
            .expect("a string"),
    )
    .expect("the envelope is the shape the runner parses");
    wire::admit_standing_authorization(&doc, Some(SCHEDULE_UID), now())
        .expect("the runner admits this document");

    // ---- the allowlist is EXACTLY the signed target cluster id
    let allowed: logweir_core::spec::AllowedClusters =
        serde_json::from_str(data["allowed-clusters.json"].as_str().expect("a string"))
            .expect("the allowlist parses");
    assert_eq!(
        allowed.allowed_cluster_ids,
        vec![TARGET_CLUSTER_ID.to_string()]
    );

    // ---- plan ∈ scope, from the FROZEN plan bytes
    let plan: logweir_core::spec::DrillSpec =
        serde_yaml::from_str(child["spec"]["planBytes"].as_str().expect("a string"))
            .expect("the frozen plan is a document the runner parses");
    wire::plan_within_scope(&wire::plan_scope_facts(&plan, &allowed), &doc.scope)
        .expect("the frozen plan is inside the signed scope");
    assert_eq!(
        plan.sample.max_partitions,
        Some(200),
        "an absent partition bound is a refusal at the runner (W5 R1.3 obligation 2)"
    );
    assert_eq!(plan.target.mode.to_string(), "scratch");
    assert_eq!(plan.target.topic_mapping_prefix, prefix());
    let point = plan.source.point.as_ref().expect("the v2 point binding");
    assert_eq!(point.point_id, POINT_ID);
    assert_eq!(point.receipt_sha256, RECEIPT_SHA);
    assert_eq!(point.manifest_sha256, MANIFEST_SHA);

    // ---- the env contract: every digest equals the mounted bytes ----------
    let map: k8s_openapi::api::core::v1::ConfigMap =
        serde_json::from_value(bundle.clone()).expect("the bundle is a ConfigMap");
    let restore: weirkeeper::crds::restore::Restore =
        serde_json::from_value(child.clone()).expect("the child is a Restore");
    let mut restore = restore;
    restore.metadata.uid = Some("child-uid".to_string());
    let env: std::collections::BTreeMap<String, String> =
        weirkeeper::controllers::restore::standing_execution_contract_env(
            &restore,
            &map,
            SCHEDULE_UID,
        )
        .expect("the env renders")
        .into_iter()
        .collect();
    let digest_of = |member: &str| {
        logweir_core::ids::sha256_prefixed(data[member].as_str().expect("a string").as_bytes())
    };
    assert_eq!(env[wire::VERSION_ENV], "2");
    assert_eq!(env[wire::AUTHORIZATION_KIND_ENV], "standing");
    assert_eq!(env[wire::REHEARSAL_SCHEDULE_UID_ENV], SCHEDULE_UID);
    // The two per-run approval digests are NOT emitted: the runner refuses a
    // `standing` contract that pins them (PLAT-14.3b).
    for absent in [wire::APPROVAL_SHA256_ENV, wire::APPROVAL_SIDECAR_SHA256_ENV] {
        assert!(
            !env.contains_key(absent),
            "a standing contract pins no per-run approval digest, and {absent} is set"
        );
    }
    assert_eq!(
        env[wire::APPROVER_KEY_SHA256_ENV],
        digest_of("approver.pub.pem")
    );
    assert_eq!(
        env[wire::ALLOWED_CLUSTERS_SHA256_ENV],
        digest_of("allowed-clusters.json")
    );
    assert_eq!(
        env[wire::PLAN_SHA256_ENV],
        logweir_core::ids::sha256_prefixed(restore.spec.plan_bytes.as_bytes())
    );
    // THE THIRTEEN MANDATORY NAMES, PRESENT **AND NON-BLANK**.
    //
    // `contains_key` alone is TAUTOLOGICAL for this purpose and it let a real
    // defect through: the runner's own `required()` filters on
    // `!value.trim().is_empty()` before deciding a name is present, so a value
    // emitted present-and-blank is a value the runner reports as MISSING and
    // the whole contract is refused with `GuardRefusal: incomplete Restore
    // execution contract` before phase 0. `APPROVAL_UID` was exactly that: the
    // standing bundle wrote no `logweir.dev/approval-uid` annotation and the
    // environment read it with `.unwrap_or_default()`. This loop is the rule
    // the runner applies, and it is the row that kills that mutant.
    for name in wire::STANDING_MANDATORY_ENV {
        let value = env
            .get(name)
            .unwrap_or_else(|| panic!("{name} is missing from the contract"));
        assert!(
            !value.trim().is_empty(),
            "{name} is present-and-blank, which the runner's required() treats as missing"
        );
    }
    for name in [
        wire::AUTHORIZATION_KIND_ENV,
        wire::AUTHORIZATION_SHA256_ENV,
        wire::AUTHORIZATION_SIDECAR_SHA256_ENV,
        wire::AUTHORIZATION_KEYS_SHA256_ENV,
        wire::REHEARSAL_SCHEDULE_UID_ENV,
    ] {
        let value = env
            .get(name)
            .unwrap_or_else(|| panic!("{name} is missing from the standing contract"));
        assert!(!value.trim().is_empty(), "{name} is present-and-blank");
    }
    assert_eq!(
        env[wire::APPROVAL_UID_ENV],
        APPROVAL_UID,
        "the contract names the Approval object the bundle committed to"
    );
    // EVERY PINNED DIGEST HAS A MOUNTED MEMBER AND EVERY MOUNTED MEMBER A
    // PINNED DIGEST — the both-directions rule `validate_execution_contract`
    // applies over the v2 optional members, MIRRORED here because `weirkeeper`
    // cannot depend on the `logweir` crate — that split is what keeps the
    // signer out of the controller, and `scripts/check-one-signer.sh` enforces
    // it. The rule itself therefore has two statements of it; what stops them
    // drifting is `logweir-core`, which both sides read: the same
    // `execution_contract` constants and the same
    // `logweir_core::ids::sha256_prefixed`.
    //
    // The standing ENVELOPE is not mirrored — it is shared as committed bytes.
    // `crates/logweir-core/tests/fixtures/standing-authorization.json` is
    // minted by `logweir drill approve --standing`, re-minted and byte-compared
    // by `crates/logweir/tests/standing_approve.rs::
    // the_committed_fixture_is_what_this_command_mints_today`, and admitted by
    // `crates/weirkeeper/tests/standing_restore.rs::
    // the_minted_standing_authorization_is_admitted_by_the_controller`.
    // **THE MUTANT IS NOW DISCRIMINATING.** While the per-run slot held a
    // duplicate of the standing envelope, pinning one digest where the other
    // belonged was an EQUIVALENT mutant and this file said so. PLAT-14.3b
    // removed the duplicate, so `standing-authorization.json` is the only
    // member carrying those bytes and the three assertions below kill a
    // mutant that swaps any of them.
    for (pinned, member) in [
        (
            wire::AUTHORIZATION_SHA256_ENV,
            "standing-authorization.json",
        ),
        (
            wire::AUTHORIZATION_SIDECAR_SHA256_ENV,
            "standing-authorization.sig",
        ),
        (
            wire::AUTHORIZATION_KEYS_SHA256_ENV,
            "authorization-keys.json",
        ),
    ] {
        assert_eq!(
            env[pinned],
            digest_of(member),
            "{pinned} must pin {member}'s own bytes"
        );
    }
    // PLAT-19.2's two are NOT set: a pinned digest with nothing mounted is a
    // refusal at the runner.
    assert!(!env.contains_key(wire::POLICY_SNAPSHOT_SHA256_ENV));
    assert!(!env.contains_key(wire::CONFIRMATION_KEY_SHA256_ENV));
}

/// **The brief's own row.** A revoked approver key refuses on the NEXT slot.
///
/// MUTANT: asking `notAfter > now` instead of `may_sign_new_for` makes this
/// pass — the key's `notAfter` is 2027 and the revocation is today.
#[tokio::test]
async fn a_revoked_approver_key_refuses_the_next_slot() {
    let (client, recorder, bodies) = mock_client_recording_bodies(routes(
        approval_value(&envelope()),
        trust_policy_value("Revoked", Some("2026-09-19T00:00:00Z")),
        cluster_value(true, Some(TARGET_CLUSTER_ID)),
        backup_list(vec![backup_value(
            "logweir-backup-nightly-20260919-020000",
            "2026-09-19T02:00:00Z",
            json!(["orders", "payments"]),
            true,
        )]),
        restore_list(vec![]),
    ));
    let outcome = rs::reconcile_schedule(&schedule(), &context(client), now())
        .await
        .expect("the reconcile answers");
    let rs::Verdict::Skipped(skip) = &outcome.verdict else {
        panic!("{:?}", outcome.verdict)
    };
    assert_eq!(skip.reason, rehearsal::SkipReason::AuthorizationInvalid);
    assert!(
        skip.detail.contains("no longer authorise"),
        "the refusal names the withdrawal: {skip}"
    );
    assert_eq!(posted(&recorder, RESTORES_PATH), 0);
    assert_eq!(last_skip(&bodies).as_deref(), Some("AuthorizationInvalid"));
}

/// A key whose only usage is `EvidenceSigning` may not authorise a rehearsal,
/// even with a `Verified=True` approval — D3 §7.3's usage separation.
#[tokio::test]
async fn the_installations_own_signing_key_may_not_authorise_its_own_rehearsals() {
    let mut policies = trust_policy_value("Active", None);
    policies["items"][0]["spec"]["keys"][0]["usages"] = json!(["EvidenceSigning"]);
    let (client, recorder, _) = mock_client_recording_bodies(routes(
        approval_value(&envelope()),
        policies,
        cluster_value(true, Some(TARGET_CLUSTER_ID)),
        backup_list(vec![]),
        restore_list(vec![]),
    ));
    let outcome = rs::reconcile_schedule(&schedule(), &context(client), now())
        .await
        .expect("the reconcile answers");
    let rs::Verdict::Skipped(skip) = &outcome.verdict else {
        panic!("{:?}", outcome.verdict)
    };
    assert!(skip.detail.contains("EvidenceSigning"), "{skip}");
    assert_eq!(posted(&recorder, RESTORES_PATH), 0);
}

/// A console-confirmation key is not enough in the pre-PLAT-19.2 format: the
/// schedule carries no immutable ordinary/governed policy mode, so admission
/// fails closed before a child or bundle exists.
#[tokio::test]
async fn console_confirmation_alone_never_fires_a_current_format_schedule() {
    let mut policies = trust_policy_value("Active", None);
    policies["items"][0]["spec"]["keys"][0]["usages"] = json!(["ConsoleConfirmation"]);
    let (client, recorder, bodies) = mock_client_recording_bodies(routes(
        approval_value(&envelope()),
        policies,
        cluster_value(true, Some(TARGET_CLUSTER_ID)),
        backup_list(vec![]),
        restore_list(vec![]),
    ));
    let outcome = rs::reconcile_schedule(&schedule(), &context(client), now())
        .await
        .expect("the reconcile answers");
    let rs::Verdict::Skipped(skip) = &outcome.verdict else {
        panic!("{:?}", outcome.verdict)
    };
    assert_eq!(skip.reason, rehearsal::SkipReason::AuthorizationInvalid);
    assert!(skip.detail.contains("ConsoleConfirmation"), "{skip}");
    assert!(skip.detail.contains("PLAT-19.2"), "{skip}");
    assert_eq!(posted(&recorder, RESTORES_PATH), 0);
    assert_eq!(posted(&recorder, CONFIGMAPS_PATH), 0);
    assert_eq!(last_skip(&bodies).as_deref(), Some("AuthorizationInvalid"));
}

/// A suspended schedule fires nothing, skips nothing, and reads nothing: the
/// route table has only the status patch, so any other call panics the double.
#[tokio::test]
async fn a_suspended_schedule_does_nothing_at_all() {
    let (client, recorder, bodies) = mock_client_recording_bodies(vec![Route {
        method: "PATCH",
        path_suffix: SCHEDULE_STATUS_PATH,
        status: 200,
        body: schedule_value(json!({})).to_string(),
    }]);
    let mut value = schedule_value(json!({}));
    value["spec"]["suspend"] = json!(true);
    let schedule: RehearsalSchedule =
        serde_json::from_value(value).expect("the suspended fixture parses");
    let outcome = rs::reconcile_schedule(&schedule, &context(client), now())
        .await
        .expect("the reconcile answers");
    assert!(matches!(outcome.verdict, rs::Verdict::Idle));
    assert_eq!(posted(&recorder, RESTORES_PATH), 0);
    let patch = patch_bodies(&bodies).pop().expect("one status patch");
    assert!(
        patch["status"]["nextFireTime"].is_null(),
        "a suspended schedule advertises no next fire time"
    );
    assert!(
        patch["status"]["lastSkipped"].is_null(),
        "and suspension is not a skip — nothing was refused"
    );
}

/// A slot older than `startingDeadlineSeconds` is skipped and recorded, never
/// run late.
#[tokio::test]
async fn a_slot_past_its_starting_deadline_is_skipped_and_recorded() {
    let (client, recorder, bodies) = mock_client_recording_bodies(happy_routes());
    // Ten hours after the 03:00 slot, with a one-hour horizon.
    let late = at("2026-09-20T13:00:00Z");
    let outcome = rs::reconcile_schedule(&schedule(), &context(client), late)
        .await
        .expect("the reconcile answers");
    assert!(
        matches!(&outcome.verdict, rs::Verdict::Skipped(s) if s.reason == rehearsal::SkipReason::ConcurrencyBlocked),
        "{:?}",
        outcome.verdict
    );
    assert_eq!(posted(&recorder, RESTORES_PATH), 0);
    assert!(last_skip(&bodies).is_some(), "the miss is recorded");
}

/// The same slot is not fired twice: `lastScheduledSlot` is the record, and a
/// second reconcile over it is idle.
#[tokio::test]
async fn a_slot_already_recorded_does_not_fire_again() {
    let (client, recorder, _) = mock_client_recording_bodies(happy_routes());
    let schedule = schedule_with(json!({ "lastScheduledSlot": "20260920-030000" }));
    let outcome = rs::reconcile_schedule(&schedule, &context(client), now())
        .await
        .expect("the reconcile answers");
    assert!(matches!(outcome.verdict, rs::Verdict::Idle));
    assert_eq!(posted(&recorder, RESTORES_PATH), 0);
}

/// A point captured from the cluster the rehearsal targets is refused BEFORE a
/// Job could refuse it: the runner's phase 0 enforces `source != target` too,
/// and a refusal that costs no pod says WHY while a `GuardRefused` exit says
/// only that a guard fired.
#[test]
fn a_point_whose_source_is_the_target_is_never_selected() {
    let mut point = rehearsal::PointCandidate {
        point_id: POINT_ID.to_string(),
        backup_id: "b-20260919".to_string(),
        backup_name: None,
        recovery_point_at: at("2026-09-19T02:00:00Z"),
        covered: Some(rehearsal::Window {
            from_ms: 1_758_240_000_000,
            to_ms: 1_758_326_400_000,
        }),
        topics: Some(vec!["orders".to_string()]),
        partitions: Some(4),
        receipt_key: "logweir/backups/b.json".to_string(),
        receipt_sha256: RECEIPT_SHA.to_string(),
        manifest_sha256: Some(MANIFEST_SHA.to_string()),
        destination: Some(DESTINATION.to_string()),
        source_cluster_id: Some(SOURCE_CLUSTER_ID.to_string()),
        selectable: true,
        verdict_refused: false,
        retention_lease: false,
    };
    let topics = vec!["orders".to_string()];
    let rules = rehearsal::SelectionRules {
        topics: &topics,
        min_age_seconds: 0,
        max_partitions: 200,
        target_cluster_id: TARGET_CLUSTER_ID,
    };
    rehearsal::select_point(std::slice::from_ref(&point), &rules, now())
        .expect("a point from another cluster is selectable");

    point.source_cluster_id = Some(TARGET_CLUSTER_ID.to_string());
    let skip = rehearsal::select_point(&[point], &rules, now())
        .expect_err("a point from the target cluster is not");
    assert_eq!(skip.reason, rehearsal::SkipReason::TargetUnavailable);
}

/// **Review finding F4.** The per-target concurrency walk FOLLOWS the API
/// server's continue token.
///
/// A rehearsal child is named `logweir-rehearsal-<schedule>-<slot>` and a
/// label-selected list comes back in NAME order, so one capped page holds the
/// OLDEST rehearsals against a cluster. Nothing prunes them — they are the
/// audit trail — so after about two months of a daily schedule the page holds
/// only finished runs and the one still IN FLIGHT is exactly the object outside
/// it. A single-page walk would therefore stop raising `TargetBusy` precisely
/// when it starts mattering, and two rehearsals would run against one broker.
///
/// The double cannot vary a body per page, so this asserts the property that
/// distinguishes the two implementations: the reconcile issues MORE THAN ONE
/// list against `/restores`, and every request after the first carries a
/// `continue=` parameter. The mutant — dropping the loop — makes it exactly one.
#[tokio::test]
async fn the_per_target_walk_follows_the_continue_token() {
    let page = restore_page(vec![other_schedules_rehearsal(
        "logweir-rehearsal-weekly-payments-20260101-030000",
        "Succeeded",
        Some("pass"),
    )]);
    let (client, recorder, _) = mock_client_recording_bodies(routes(
        approval_value(&envelope()),
        trust_policy_value("Active", None),
        cluster_value(true, Some(TARGET_CLUSTER_ID)),
        backup_list(vec![]),
        page,
    ));
    rs::reconcile_schedule(&schedule(), &context(client), now())
        .await
        .expect("the reconcile answers");

    let lists: Vec<String> = seen(&recorder)
        .into_iter()
        .filter(|(method, uri)| {
            method == "GET" && uri.split('?').next().unwrap_or("").ends_with(RESTORES_PATH)
        })
        .map(|(_, uri)| uri)
        .collect();
    assert!(
        lists.len() > 1,
        "a page that says there is MORE must be followed; the walk stopped after {} request(s)",
        lists.len()
    );
    assert!(
        lists[1..].iter().all(|uri| uri.contains("continue=")),
        "every request after the first carries the server's continue token: {lists:?}"
    );
    assert!(
        lists.len() <= rs::MAX_TARGET_PAGES as usize,
        "and the walk stays bounded: {} requests",
        lists.len()
    );
}

/// The walk stops at the FIRST non-terminal hit — the question is "is anybody
/// else running", not "how many" — so the ordinary case costs one round trip
/// even when the server says there are more pages.
#[tokio::test]
async fn the_per_target_walk_stops_at_the_first_live_rehearsal() {
    let page = restore_page(vec![other_schedules_rehearsal(
        "logweir-rehearsal-weekly-payments-20260920-030000",
        "Running",
        None,
    )]);
    let (client, recorder, bodies) = mock_client_recording_bodies(routes(
        approval_value(&envelope()),
        trust_policy_value("Active", None),
        cluster_value(true, Some(TARGET_CLUSTER_ID)),
        backup_list(vec![]),
        page,
    ));
    let outcome = rs::reconcile_schedule(&schedule(), &context(client), now())
        .await
        .expect("the reconcile answers");
    assert!(
        matches!(&outcome.verdict, rs::Verdict::Skipped(s) if s.reason == rehearsal::SkipReason::TargetBusy),
        "{:?}",
        outcome.verdict
    );
    let lists = seen(&recorder)
        .into_iter()
        .filter(|(method, uri)| {
            method == "GET" && uri.split('?').next().unwrap_or("").ends_with(RESTORES_PATH)
        })
        .count();
    assert_eq!(lists, 1, "a live hit on page one ends the walk");
    assert_eq!(posted(&recorder, RESTORES_PATH), 0);
    assert_eq!(last_skip(&bodies).as_deref(), Some("TargetBusy"));
}

/// **PLAT-14.3b lifted the hold.** Until 14.3b the `Restore` reconciler
/// refused every standing `Restore` with `ApprovalNotReceived` before a Job
/// could exist, and this schedule reported that under its own reason
/// `StandingAuthorizationNotAdmitted`, naming the five unwired functions.
/// `restore.rs` now reads `spec.authorization`, so that reason is GONE: a
/// standing `Restore` that ends terminally ended for a reason about this
/// rehearsal, and it is recorded as the failure it is.
///
/// The case here is the one the old hold would have swallowed: an
/// authorization the `Restore` reconciler refused at admission
/// (`StandingAuthorizationRefused` — expired, a withdrawn key, a wrong usage,
/// a plan outside the signed scope). It is `RehearsalHealthy=False/Failed`
/// with the Restore's own terminal reason in the message, and D3 §4's
/// `rehearsalLast*` still records the attempt.
#[tokio::test]
async fn a_rehearsal_whose_authorization_was_refused_is_recorded_as_a_failure() {
    let refused = json!({
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "Restore",
        "metadata": {"name": "logweir-rehearsal-weekly-orders-20260913-030000", "namespace": NS, "uid": "r1", "resourceVersion": "3"},
        "spec": {
            "planBytes": "{}",
            "authorization": {
                "kind": "Standing",
                "approvalRef": {"name": APPROVAL},
                "rehearsalScheduleRef": {"name": SCHEDULE}
            },
            "sourceArchive": {"url": "logweir-destination://primary"},
            "backupSetRef": "b",
            "pointInTime": "2026-09-13T02:00:00Z",
            "target": {"clusterRef": {"name": TARGET}, "mode": "scratch", "topicNaming": {"prefix": "rehearsal-"}},
            "deadlineSeconds": 3600
        },
        "status": {"phase": "Failed", "exitReason": "StandingAuthorizationRefused"}
    });
    let mut table = happy_routes();
    table.push(Route {
        method: "GET",
        path_suffix: "/restores/logweir-rehearsal-weekly-orders-20260913-030000",
        status: 200,
        body: refused.to_string(),
    });
    let (client, _, bodies) = mock_client_recording_bodies(table);
    let schedule = schedule_with(json!({
        "activeRestoreRef": {"name": "logweir-rehearsal-weekly-orders-20260913-030000"}
    }));
    rs::reconcile_schedule(&schedule, &context(client), now())
        .await
        .expect("the reconcile answers");
    let patch = patch_bodies(&bodies)
        .into_iter()
        .find(|b| b.pointer("/status/conditions").is_some())
        .expect("a status patch carries the conditions");
    let health = patch["status"]["conditions"]
        .as_array()
        .expect("conditions is an array")
        .iter()
        .find(|c| c["type"] == "RehearsalHealthy")
        .expect("RehearsalHealthy is published")
        .clone();
    assert_eq!(health["status"], "False");
    assert_eq!(
        health["reason"], "Failed",
        "PLAT-14.3b landed, so a refused authorization is a rehearsal FAILURE and not a hold"
    );
    let message = health["message"].as_str().expect("a message");
    assert!(
        message.contains("StandingAuthorizationRefused"),
        "the message names the Restore's own terminal reason: {message}"
    );
    // THE HOLD IS GONE, AND NOTHING MAY REINTRODUCE IT UNDER ANOTHER NAME.
    let whole = patch.to_string();
    assert!(
        !whole.contains("StandingAuthorizationNotAdmitted") && !whole.contains("PLAT-14.3b"),
        "the PLAT-14.3b hold must not survive the task that closed it: {whole}"
    );
}

/// And an ORDINARY failure is still `Failed`: the hold reason must not swallow
/// a real one.
#[tokio::test]
async fn an_ordinary_rehearsal_failure_is_still_reported_as_failed() {
    let failed = json!({
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "Restore",
        "metadata": {"name": "logweir-rehearsal-weekly-orders-20260913-030000", "namespace": NS, "uid": "r1", "resourceVersion": "3"},
        "spec": {
            "planBytes": "{}",
            "authorization": {
                "kind": "Standing",
                "approvalRef": {"name": APPROVAL},
                "rehearsalScheduleRef": {"name": SCHEDULE}
            },
            "sourceArchive": {"url": "logweir-destination://primary"},
            "backupSetRef": "b",
            "pointInTime": "2026-09-13T02:00:00Z",
            "target": {"clusterRef": {"name": TARGET}, "mode": "scratch", "topicNaming": {"prefix": "rehearsal-"}},
            "deadlineSeconds": 3600
        },
        "status": {"phase": "Failed", "exitCode": 2, "outcome": "fail-integrity"}
    });
    let mut table = happy_routes();
    table.push(Route {
        method: "GET",
        path_suffix: "/restores/logweir-rehearsal-weekly-orders-20260913-030000",
        status: 200,
        body: failed.to_string(),
    });
    let (client, _, bodies) = mock_client_recording_bodies(table);
    let schedule = schedule_with(json!({
        "activeRestoreRef": {"name": "logweir-rehearsal-weekly-orders-20260913-030000"}
    }));
    rs::reconcile_schedule(&schedule, &context(client), now())
        .await
        .expect("the reconcile answers");
    let patch = patch_bodies(&bodies)
        .into_iter()
        .find(|b| b.pointer("/status/conditions").is_some())
        .expect("a status patch carries the conditions");
    let health = patch["status"]["conditions"]
        .as_array()
        .expect("conditions is an array")
        .iter()
        .find(|c| c["type"] == "RehearsalHealthy")
        .expect("RehearsalHealthy is published")
        .clone();
    assert_eq!(health["status"], "False");
    assert_eq!(health["reason"], "Failed");
}

/// An `Approval` with no `metadata.uid` is refused at authorization rather than
/// defaulted to `""`.
///
/// `LOGWEIR_EXECUTION_APPROVAL_UID` is one of the thirteen mandatory contract
/// values and the runner treats a present-and-blank value as MISSING, so a
/// default here renders a bundle every Job refuses — with a message about the
/// contract rather than about this `Approval`. Unreachable from a real API
/// server, which is exactly why it is a refusal and not a fallback.
#[tokio::test]
async fn an_approval_with_no_uid_is_refused_rather_than_defaulted() {
    let mut approval = approval_value(&envelope());
    approval["metadata"]
        .as_object_mut()
        .expect("an object")
        .remove("uid");
    let (client, recorder, bodies) = mock_client_recording_bodies(routes(
        approval,
        trust_policy_value("Active", None),
        cluster_value(true, Some(TARGET_CLUSTER_ID)),
        backup_list(vec![backup_value(
            "logweir-backup-nightly-20260919-020000",
            "2026-09-19T02:00:00Z",
            json!(["orders", "payments"]),
            true,
        )]),
        restore_list(vec![]),
    ));
    let outcome = rs::reconcile_schedule(&schedule(), &context(client), now())
        .await
        .expect("the reconcile answers");
    let rs::Verdict::Skipped(skip) = &outcome.verdict else {
        panic!("{:?}", outcome.verdict)
    };
    assert_eq!(skip.reason, rehearsal::SkipReason::AuthorizationInvalid);
    assert!(skip.detail.contains("metadata.uid"), "{skip}");
    assert_eq!(posted(&recorder, RESTORES_PATH), 0);
    assert_eq!(posted(&recorder, CONFIGMAPS_PATH), 0);
    assert_eq!(last_skip(&bodies).as_deref(), Some("AuthorizationInvalid"));
}

// ===========================================================================
// Pure halves
// ===========================================================================

/// The template digest ignores `suspend` and NOTHING else.
///
/// MUTANT: including `suspend` in the digest makes the first assertion fail —
/// and in production it would make pausing a rehearsal invalidate the document
/// authorising it.
#[test]
fn the_template_digest_ignores_suspend_and_nothing_else() {
    let base = schedule();
    let mut suspended = base.clone();
    suspended.spec.suspend = true;
    assert_eq!(
        rehearsal::template_digest(&base.spec).expect("a digest"),
        rehearsal::template_digest(&suspended.spec).expect("a digest"),
        "pausing a rehearsal must not invalidate its authorization"
    );

    let mut widened = base.clone();
    widened.spec.bounds.max_partitions = 2000;
    assert_ne!(
        rehearsal::template_digest(&base.spec).expect("a digest"),
        rehearsal::template_digest(&widened.spec).expect("a digest"),
        "every other field is inside the digest"
    );

    let mut retargeted = base;
    retargeted.spec.target.topic_prefix = "rehearsal-other-".to_string();
    assert_ne!(
        rehearsal::template_digest(&retargeted.spec).expect("a digest"),
        rehearsal::template_digest(&schedule().spec).expect("a digest"),
    );
}

/// The digest is `sha256_prefixed` over deterministic JSON, which is exactly
/// what the `Approval` reconciler's check 7 recomputes.
#[test]
fn the_digest_is_the_hash_the_approval_controller_recomputes() {
    let bytes = rehearsal::template_bytes(&schedule().spec).expect("the bytes");
    assert_eq!(
        rehearsal::template_digest(&schedule().spec).expect("a digest"),
        logweir_core::ids::sha256_prefixed(&bytes)
    );
    let value: Value = serde_json::from_slice(&bytes).expect("the bytes are JSON");
    assert!(
        value.get("suspend").is_none(),
        "the one mutable field is not in the signed bytes"
    );
    assert_eq!(value["target"]["topicPrefix"], "rehearsal-");
}

/// The keyring carries only keys that may authorise TODAY.
#[test]
fn a_retired_key_never_reaches_the_keyring() {
    let policies: kube::core::ObjectList<weirkeeper::crds::trust_policy::TrustPolicy> =
        serde_json::from_value(trust_policy_value("Retired", None)).expect("the list parses");
    let resolution = weirkeeper::trust::resolve_in(NS, &policies.items, None);
    let weirkeeper::trust::Resolution::Trust(trust) = resolution else {
        panic!("the default policy governs every namespace")
    };
    let ring = rs::keyring(&trust, now());
    assert!(
        ring.keys.is_empty(),
        "a retired key may not authorise anything new, so it is not written into a bundle"
    );

    let policies: kube::core::ObjectList<weirkeeper::crds::trust_policy::TrustPolicy> =
        serde_json::from_value(trust_policy_value("Active", None)).expect("the list parses");
    let weirkeeper::trust::Resolution::Trust(trust) =
        weirkeeper::trust::resolve_in(NS, &policies.items, None)
    else {
        panic!("the default policy governs every namespace")
    };
    let ring = rs::keyring(&trust, now());
    assert_eq!(ring.keys.len(), 1);
    assert_eq!(ring.keys[0].key_id, KEY_ID);

    let mut console_only = trust_policy_value("Active", None);
    console_only["items"][0]["spec"]["keys"][0]["usages"] = json!(["ConsoleConfirmation"]);
    let policies: kube::core::ObjectList<weirkeeper::crds::trust_policy::TrustPolicy> =
        serde_json::from_value(console_only).expect("the list parses");
    let weirkeeper::trust::Resolution::Trust(trust) =
        weirkeeper::trust::resolve_in(NS, &policies.items, None)
    else {
        panic!("the default policy governs every namespace")
    };
    assert!(
        rs::keyring(&trust, now()).keys.is_empty(),
        "the runner bundle pins no ConsoleConfirmation key until policy mode is bound"
    );
}

/// A `Backup` whose evidence did not verify is never selectable on its own —
/// `spec.point.requireVerifiedEvidence` is `true` in v1 and there is no way to
/// turn it off. A destination-backed run that captured the runner's receipt
/// digest remains joinable so the catalog can supply the facts it measured.
#[test]
fn an_unverified_backup_is_never_a_candidate() {
    let verified: weirkeeper::crds::backup::Backup = serde_json::from_value(backup_value(
        "b1",
        "2026-09-19T02:00:00Z",
        json!(["orders"]),
        true,
    ))
    .expect("the fixture parses");
    let candidate = rs::candidate_from_backup(&verified).expect("a candidate");
    assert!(candidate.selectable);
    assert_eq!(candidate.point_id, POINT_ID);
    assert_eq!(candidate.destination.as_deref(), Some(DESTINATION));

    let unverified: weirkeeper::crds::backup::Backup = serde_json::from_value(backup_value(
        "b2",
        "2026-09-19T02:00:00Z",
        json!(["orders"]),
        false,
    ))
    .expect("the fixture parses");
    assert!(
        !rs::candidate_from_backup(&unverified)
            .expect("still a candidate object")
            .selectable
    );

    let mut valid_without_capture = backup_value(
        "b-valid-no-capture",
        "2026-09-19T02:00:00Z",
        json!(["orders"]),
        true,
    );
    valid_without_capture["status"]["capture"] = Value::Null;
    valid_without_capture["status"]["windowCovered"] = Value::Null;
    let valid_without_capture: weirkeeper::crds::backup::Backup =
        serde_json::from_value(valid_without_capture).expect("the capture-less fixture parses");
    assert!(
        !rs::candidate_from_backup(&valid_without_capture)
            .expect("the digest still makes the point joinable")
            .selectable,
        "creationTimestamp is only a join placeholder: even Valid verification cannot make it a \
         receipt capture time"
    );

    let mut destination_backed = backup_value(
        "b3",
        "2026-09-19T02:00:00Z",
        json!(["orders", "payments"]),
        false,
    );
    destination_backed["status"]["capture"] = Value::Null;
    destination_backed["status"]["windowCovered"] = Value::Null;
    destination_backed["status"]["evidence"]["verification"]["result"] = json!("NotAttempted");
    let destination_backed: weirkeeper::crds::backup::Backup =
        serde_json::from_value(destination_backed).expect("the NotAttempted fixture parses");
    let candidate = rs::candidate_from_backup(&destination_backed)
        .expect("a succeeded run with a captured receipt digest remains a joinable candidate");
    assert_eq!(
        candidate.topics,
        Some(vec!["orders".to_string(), "payments".to_string()]),
        "the immutable named selection supplies D3 §4.2's topic subset fact"
    );
    assert!(
        !candidate.selectable,
        "NotAttempted is not silently upgraded; a matching verified catalog row decides \
         selectability when candidates merge"
    );

    let mut by_id = std::collections::BTreeMap::from([(candidate.point_id.clone(), candidate)]);
    let catalog_entry: weirkeeper::catalog_view::ViewEntry = serde_json::from_value(json!({
        "pointId": POINT_ID,
        "backupId": "set-b3",
        "runId": "run-b3",
        "recoveryPointAtMs": 1_758_240_000_000_i64,
        "coveredFromMs": 1_758_236_400_000_i64,
        "coveredToMs": 1_758_240_000_000_i64,
        "locations": [{"locationId": DESTINATION, "availability": "Available"}],
        "receiptKey": "receipts/set-b3.json",
        "receiptSha256": RECEIPT_SHA,
        "manifestSha256": MANIFEST_SHA,
        "availability": "Available",
        "verification": "Verified",
        "selectable": true
    }))
    .expect("the catalog entry parses");
    let mut colliding_entry = catalog_entry.clone();
    colliding_entry.receipt_sha256 =
        "sha256:1111111111111111111111111111111122222222222222222222222222222222".to_string();
    rs::merge_catalog_entry(
        &mut by_id,
        colliding_entry,
        Some(DESTINATION.to_string()),
        &Default::default(),
    );
    let unmerged = by_id.get(POINT_ID).expect("the Backup candidate remains");
    assert!(
        !unmerged.selectable && unmerged.covered.is_none(),
        "the truncated pointId is not equality: a different full receipt digest supplies no \
         capture, window, or selectability"
    );

    rs::merge_catalog_entry(
        &mut by_id,
        catalog_entry,
        Some(DESTINATION.to_string()),
        &Default::default(),
    );
    let merged = by_id.get(POINT_ID).expect("the matching point merged");
    assert!(
        merged.selectable,
        "the catalog supplies its selectability axis"
    );
    assert_eq!(
        merged.topics,
        Some(vec!["orders".to_string(), "payments".to_string()]),
        "the merge retains the immutable named topics from Backup.spec"
    );
    assert_eq!(
        merged.covered,
        Some(weirkeeper::rehearsal::Window {
            from_ms: 1_758_236_400_000,
            to_ms: 1_758_240_000_000,
        }),
        "the catalog supplies the receipt-derived window"
    );
}

/// A verified, selectable catalog row for exactly this receipt digest.
fn matching_catalog_row(recovery_point_at_ms: i64) -> weirkeeper::catalog_view::ViewEntry {
    serde_json::from_value(json!({
        "pointId": POINT_ID,
        "backupId": "b-20260919",
        "runId": "run-b",
        "recoveryPointAtMs": recovery_point_at_ms,
        "coveredFromMs": 1_758_236_400_000_i64,
        "coveredToMs": 1_758_240_000_000_i64,
        "locations": [{"locationId": DESTINATION, "availability": "Available"}],
        "receiptKey": "logweir/backups/b-20260919.json",
        "receiptSha256": RECEIPT_SHA,
        "manifestSha256": MANIFEST_SHA,
        "availability": "Available",
        "verification": "Verified",
        "selectable": true
    }))
    .expect("the catalog entry parses")
}

/// A capture-less Backup candidate whose own verification result is `result`.
fn capture_less_candidate(name: &str, result: &str) -> rehearsal::PointCandidate {
    let mut value = backup_value(name, "2026-09-19T02:00:00Z", json!(["orders"]), false);
    value["status"]["capture"] = Value::Null;
    value["status"]["windowCovered"] = Value::Null;
    value["status"]["evidence"]["verification"]["result"] = json!(result);
    let backup: weirkeeper::crds::backup::Backup =
        serde_json::from_value(value).expect("the fixture parses");
    rs::candidate_from_backup(&backup).expect("the digest keeps the Backup joinable")
}

/// Review MEDIUM-1 (final round): a verdict the controller REACHED decides, and
/// a catalog row — served until `viewExpiresAt`, so possibly harvested before
/// the receipt was replaced or its signer revoked — never overrules it. Only
/// `NotAttempted` defers, exactly as `protection::evidence_objective_met`.
#[test]
fn a_catalog_row_never_overrules_a_reached_backup_refusal() {
    for result in ["Invalid", "Untrusted", "SomeFutureVerdict"] {
        let candidate = capture_less_candidate("b-refused", result);
        assert!(candidate.verdict_refused, "{result} is a reached refusal");
        assert!(!candidate.selectable);
        let mut by_id = std::collections::BTreeMap::from([(candidate.point_id.clone(), candidate)]);
        rs::merge_catalog_entry(
            &mut by_id,
            matching_catalog_row(1_758_240_000_000),
            Some(DESTINATION.to_string()),
            &Default::default(),
        );
        let merged = by_id.get(POINT_ID).expect("the Backup candidate remains");
        assert!(
            !merged.selectable,
            "a stale Verified catalog row for the same digest must not make a {result} Backup \
             selectable"
        );
        assert!(
            merged.covered.is_none(),
            "a refused Backup takes no capture window from the catalog ({result})"
        );
        let topics = vec!["orders".to_string()];
        let rules = rehearsal::SelectionRules {
            topics: &topics,
            min_age_seconds: 0,
            max_partitions: 200,
            target_cluster_id: TARGET_CLUSTER_ID,
        };
        let candidates: Vec<_> = by_id.into_values().collect();
        assert!(
            rehearsal::select_point(&candidates, &rules, now()).is_err(),
            "no point is selected for a {result} Backup"
        );
    }

    // The captured Invalid fixture (capture present) is refused the same way.
    let invalid: weirkeeper::crds::backup::Backup = serde_json::from_value(backup_value(
        "b-invalid",
        "2026-09-19T02:00:00Z",
        json!(["orders"]),
        false,
    ))
    .expect("the fixture parses");
    let candidate = rs::candidate_from_backup(&invalid).expect("a candidate object");
    let mut by_id = std::collections::BTreeMap::from([(candidate.point_id.clone(), candidate)]);
    rs::merge_catalog_entry(
        &mut by_id,
        matching_catalog_row(1_758_240_000_000),
        Some(DESTINATION.to_string()),
        &Default::default(),
    );
    assert!(!by_id[POINT_ID].selectable);

    // Control: the honest "could not look" still defers to the same row.
    let not_attempted = capture_less_candidate("b-unread", "NotAttempted");
    assert!(!not_attempted.verdict_refused);
    let mut by_id =
        std::collections::BTreeMap::from([(not_attempted.point_id.clone(), not_attempted)]);
    rs::merge_catalog_entry(
        &mut by_id,
        matching_catalog_row(1_758_240_000_000),
        Some(DESTINATION.to_string()),
        &Default::default(),
    );
    assert!(
        by_id[POINT_ID].selectable,
        "NotAttempted defers to the catalog"
    );
}

/// Review LOW-2 (final round): the catalog decides selectability only together
/// with an in-range capture time. A row whose `recoveryPointAtMs` chrono cannot
/// represent, or that is `0`/negative, leaves the creationTimestamp placeholder
/// in place, and a placeholder is never selectable.
#[test]
fn a_catalog_row_without_an_in_range_capture_time_makes_nothing_selectable() {
    for ms in [i64::MAX, i64::MIN, 0, -1] {
        let candidate = capture_less_candidate("b-unread", "NotAttempted");
        let placeholder = candidate.recovery_point_at;
        let mut by_id = std::collections::BTreeMap::from([(candidate.point_id.clone(), candidate)]);
        rs::merge_catalog_entry(
            &mut by_id,
            matching_catalog_row(ms),
            Some(DESTINATION.to_string()),
            &Default::default(),
        );
        let merged = by_id.get(POINT_ID).expect("the Backup candidate remains");
        assert!(
            !merged.selectable,
            "recoveryPointAtMs {ms} is not a capture time and cannot make the point selectable"
        );
        assert_eq!(merged.recovery_point_at, placeholder);
        assert!(merged.covered.is_none());
    }
    // A catalog-only row with no in-range capture time is not a candidate.
    let mut by_id = std::collections::BTreeMap::new();
    rs::merge_catalog_entry(
        &mut by_id,
        matching_catalog_row(i64::MAX),
        Some(DESTINATION.to_string()),
        &Default::default(),
    );
    assert!(by_id.is_empty());
}

/// The point id a `Backup` yields is the one D3 §5.1 defines, so a
/// `Backup`-derived candidate and a catalog entry for the same point merge.
#[test]
fn the_point_id_derivation_is_the_catalogs_own() {
    assert_eq!(
        rs::point_id_from_receipt_digest(RECEIPT_SHA).as_deref(),
        Some(POINT_ID)
    );
    assert!(rs::point_id_from_receipt_digest("not-a-digest").is_none());
    assert!(rs::point_id_from_receipt_digest("sha256:short").is_none());
}

// ===========================================================================
// REHEARSAL-SKIP-DEFERS-SLOT — a skipped slot is skipped
// ===========================================================================

/// The `0 3 * * 0` slot `now()` is inside.
const DUE_SLOT: &str = "20260920-030000";
/// The previous week's rehearsal, the blocker in the rows below.
const PREVIOUS_CHILD: &str = "logweir-rehearsal-weekly-orders-20260913-030000";

fn previous_child_value(phase: &str) -> Value {
    json!({
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "Restore",
        "metadata": {"name": PREVIOUS_CHILD, "namespace": NS, "uid": "r1", "resourceVersion": "3"},
        "spec": {
            "planBytes": "{}",
            "sourceArchive": {"url": "s3://x"},
            "backupSetRef": "b",
            "pointInTime": "2026-09-13T02:00:00Z",
            "target": {"clusterRef": {"name": TARGET}, "mode": "scratch", "topicNaming": {"prefix": "rehearsal-"}},
            "deadlineSeconds": 3600
        },
        "status": if phase == "Running" {
            json!({"phase": "Running"})
        } else {
            json!({"phase": phase, "exitCode": 0, "outcome": "pass"})
        }
    })
}

fn routes_with_previous_child(phase: &str) -> Vec<Route> {
    let mut table = happy_routes();
    table.push(Route {
        method: "GET",
        path_suffix: "/restores/logweir-rehearsal-weekly-orders-20260913-030000",
        status: 200,
        body: previous_child_value(phase).to_string(),
    });
    table
}

/// The schedule as the API server would hold it after `patch` was applied.
fn after(schedule_status: Value, patch: &Value) -> RehearsalSchedule {
    let mut status = schedule_status;
    weirkeeper::conditions::apply_merge_patch(&mut status, &patch["status"]);
    let mut value = schedule_value(status);
    value["metadata"]["resourceVersion"] = json!("102");
    serde_json::from_value(value).expect("the patched schedule parses")
}

/// **The defect row.** Slot `20260920-030000` is due while last week's
/// rehearsal is still running: it is skipped `ConcurrencyBlocked`, and the
/// skip names THAT slot — not the 03:30 instant the controller looked — and
/// consumes it. Fifteen minutes later the blocker has finished, well inside
/// the one-hour `startingDeadlineSeconds`: the next pass records the finished
/// run and creates NO `Restore`, because the slot was skipped, not deferred.
///
/// MUTANT: drop the `lastScheduledSlot` insert from the skip block of
/// `rehearsal_schedule::status_patch`. Pass 2 fires the skipped slot late and
/// this row fails; `lastSkipped.slot = slot_name(now)` fails the first half.
#[tokio::test]
async fn a_blocker_that_finishes_inside_the_horizon_gets_no_late_restore() {
    let start = json!({"activeRestoreRef": {"name": PREVIOUS_CHILD}});

    // ---- pass 1: 03:30, the blocker is still running ----------------------
    let (client, recorder, bodies) =
        mock_client_recording_bodies(routes_with_previous_child("Running"));
    let outcome = rs::reconcile_schedule(&schedule_with(start.clone()), &context(client), now())
        .await
        .expect("the reconcile answers");
    assert!(
        matches!(&outcome.verdict, rs::Verdict::Skipped(s) if s.reason == rehearsal::SkipReason::ConcurrencyBlocked),
        "{:?}",
        outcome.verdict
    );
    assert_eq!(posted(&recorder, RESTORES_PATH), 0);
    let patch = patch_bodies(&bodies).pop().expect("the skip is written");
    assert_eq!(
        patch["status"]["lastSkipped"],
        json!({"slot": DUE_SLOT, "reason": "ConcurrencyBlocked"}),
        "the skip names the DUE slot it refused, not the instant it looked"
    );
    assert_eq!(
        patch["status"]["lastScheduledSlot"], DUE_SLOT,
        "and consumes it"
    );
    assert_eq!(
        patch["metadata"]["resourceVersion"], "101",
        "under the compare-and-set precondition (seam S7)"
    );

    // ---- pass 2: 03:45, the blocker finished inside the horizon -----------
    let skipped = after(start.clone(), &patch);
    let (client, recorder, bodies) =
        mock_client_recording_bodies(routes_with_previous_child("Succeeded"));
    let later = at("2026-09-20T03:45:00Z");
    let outcome = rs::reconcile_schedule(&skipped, &context(client), later)
        .await
        .expect("the reconcile answers");
    assert!(
        matches!(outcome.verdict, rs::Verdict::Idle),
        "a skipped slot is not fired late: {:?}",
        outcome.verdict
    );
    assert_eq!(posted(&recorder, RESTORES_PATH), 0, "no late Restore");
    assert_eq!(posted(&recorder, CONFIGMAPS_PATH), 0);
    let patch = patch_bodies(&bodies)
        .pop()
        .expect("the finished run is written");
    assert!(
        patch["status"]["lastSucceeded"].is_object(),
        "the finished blocker is still recorded"
    );
    assert!(
        patch["status"].get("lastSkipped").is_none(),
        "an idle pass does not rewrite the skip record"
    );

    // CONTROL: the same second pass over the PRE-FIX status (the slot not
    // consumed) does fire — so the table above could have produced a Restore,
    // and "no Restore" is a measurement.
    let (client, recorder, _) =
        mock_client_recording_bodies(routes_with_previous_child("Succeeded"));
    let outcome = rs::reconcile_schedule(&schedule_with(start), &context(client), later)
        .await
        .expect("the reconcile answers");
    assert!(
        matches!(outcome.verdict, rs::Verdict::Fire(_)),
        "{:?}",
        outcome.verdict
    );
    assert_eq!(posted(&recorder, RESTORES_PATH), 1);
}

/// Every skip reason consumes the slot it refused, and names it. D3 §4.1 lists
/// the eight; none of them means "try this slot again later".
#[test]
fn every_skip_reason_names_and_consumes_the_due_slot() {
    use rehearsal::SkipReason as R;
    let schedule = schedule();
    for reason in [
        R::NoQualifyingPoint,
        R::TargetUnavailable,
        R::AuthorizationInvalid,
        R::AuthorizationExpired,
        R::ConcurrencyBlocked,
        R::TargetBusy,
        R::LeftoverTopics,
        R::PointRetentionInProgress,
    ] {
        let update = rs::StatusUpdate {
            verdict: rs::Verdict::Skipped(rehearsal::Skip::new(reason, "why")),
            slot: rs::due_unconsumed_slot(&schedule, now()),
            next_fire: None,
            observation: rs::Observation::default(),
            template_digest: template_digest(),
            created: None,
        };
        let patch = rs::status_patch(&schedule, &update, now());
        assert_eq!(
            patch["status"]["lastSkipped"],
            json!({"slot": DUE_SLOT, "reason": reason.as_str()})
        );
        assert_eq!(
            patch["status"]["lastScheduledSlot"],
            DUE_SLOT,
            "{} consumes its slot",
            reason.as_str()
        );
    }

    // No due, undecided slot: neither record is (re)written, so `lastSkipped`
    // stays the last slot actually refused rather than moving every requeue.
    let decided = schedule_with(json!({
        "lastScheduledSlot": DUE_SLOT,
        "lastSkipped": {"slot": DUE_SLOT, "reason": "TargetBusy"}
    }));
    assert_eq!(rs::due_unconsumed_slot(&decided, now()), None);
    let update = rs::StatusUpdate {
        verdict: rs::Verdict::Skipped(rehearsal::Skip::new(R::LeftoverTopics, "why")),
        slot: rs::due_unconsumed_slot(&decided, now()),
        next_fire: None,
        observation: rs::Observation::default(),
        template_digest: template_digest(),
        created: None,
    };
    let patch = rs::status_patch(&decided, &update, now());
    assert!(patch["status"].get("lastSkipped").is_none());
    assert!(patch["status"].get("lastScheduledSlot").is_none());
}

/// The stale-slot skip names the slot that went stale (03:00), not the 13:00
/// pass that noticed, and the next pass inside the same slot is idle.
#[tokio::test]
async fn a_stale_slot_skip_names_the_stale_slot_and_is_written_once() {
    let (client, _, bodies) = mock_client_recording_bodies(happy_routes());
    let late = at("2026-09-20T13:00:00Z");
    rs::reconcile_schedule(&schedule(), &context(client), late)
        .await
        .expect("the reconcile answers");
    let patch = patch_bodies(&bodies).pop().expect("the miss is written");
    assert_eq!(patch["status"]["lastSkipped"]["slot"], DUE_SLOT);
    assert_eq!(patch["status"]["lastScheduledSlot"], DUE_SLOT);

    let (client, recorder, bodies) = mock_client_recording_bodies(happy_routes());
    let outcome = rs::reconcile_schedule(
        &after(json!({}), &patch),
        &context(client),
        at("2026-09-20T13:00:30Z"),
    )
    .await
    .expect("the reconcile answers");
    assert!(matches!(outcome.verdict, rs::Verdict::Idle));
    assert_eq!(posted(&recorder, RESTORES_PATH), 0);
    assert!(patch_bodies(&bodies)
        .iter()
        .all(|p| p["status"].get("lastSkipped").is_none()));
}

/// The pre-read `LeftoverTopics` path names the due slot too, and a pass
/// inside a slot already decided does not rewrite the record.
#[tokio::test]
async fn a_leftover_topics_skip_names_the_due_slot_once() {
    let patch_only = || {
        vec![Route {
            method: "PATCH",
            path_suffix: SCHEDULE_STATUS_PATH,
            status: 200,
            body: schedule_value(json!({})).to_string(),
        }]
    };
    let pending = json!({
        "cleanup": {"pendingTopics": ["rehearsal-3f2a91c7-orders"], "since": "2026-09-13T04:00:00Z"}
    });
    let (client, _, bodies) = mock_client_recording_bodies(patch_only());
    rs::reconcile_schedule(&schedule_with(pending.clone()), &context(client), now())
        .await
        .expect("the reconcile answers");
    let patch = patch_bodies(&bodies).pop().expect("the skip is written");
    assert_eq!(
        patch["status"]["lastSkipped"],
        json!({"slot": DUE_SLOT, "reason": "LeftoverTopics"})
    );
    assert_eq!(patch["status"]["lastScheduledSlot"], DUE_SLOT);

    let (client, _, bodies) = mock_client_recording_bodies(patch_only());
    let outcome = rs::reconcile_schedule(
        &after(pending, &patch),
        &context(client),
        at("2026-09-20T03:31:00Z"),
    )
    .await
    .expect("the reconcile answers");
    assert!(
        matches!(&outcome.verdict, rs::Verdict::Skipped(s) if s.reason == rehearsal::SkipReason::LeftoverTopics),
        "the refusal is still the verdict and still in Ready"
    );
    let patch = patch_bodies(&bodies).pop().expect("a status write");
    assert!(patch["status"].get("lastSkipped").is_none());
    assert!(patch["status"].get("lastScheduledSlot").is_none());
}

/// The two trust-resolution refusals are skips of the due slot as well — they
/// are decided before `decide` runs, so they name and consume it themselves.
#[tokio::test]
async fn an_unconfigured_trust_skip_names_and_consumes_the_due_slot() {
    let empty = json!({
        "apiVersion": "v1",
        "kind": "TrustPolicyList",
        "metadata": {"resourceVersion": "1"},
        "items": []
    });
    let mut table = happy_routes();
    table.retain(|r| r.path_suffix != "/trustpolicies");
    table.push(Route {
        method: "GET",
        path_suffix: "/trustpolicies",
        status: 200,
        body: empty.to_string(),
    });
    // No TrustPolicy, and no legacy roster either: nothing in this namespace
    // can verify an approver.
    table.push(Route {
        method: "GET",
        path_suffix: "/trustrosters/default",
        status: 404,
        body: json!({"kind": "Status", "apiVersion": "v1", "status": "Failure", "reason": "NotFound", "code": 404}).to_string(),
    });
    let (client, recorder, bodies) = mock_client_recording_bodies(table);
    let outcome = rs::reconcile_schedule(&schedule(), &context(client), now())
        .await
        .expect("the reconcile answers");
    assert!(
        matches!(&outcome.verdict, rs::Verdict::Skipped(s) if s.reason == rehearsal::SkipReason::AuthorizationInvalid),
        "{:?}",
        outcome.verdict
    );
    assert_eq!(posted(&recorder, RESTORES_PATH), 0);
    let patch = patch_bodies(&bodies).pop().expect("the skip is written");
    assert_eq!(
        patch["status"]["lastSkipped"],
        json!({"slot": DUE_SLOT, "reason": "AuthorizationInvalid"})
    );
    assert_eq!(patch["status"]["lastScheduledSlot"], DUE_SLOT);
}

// ===========================================================================
// Class sweep: the catalog-only arm of the rehearsal join
// ===========================================================================

/// A catalog-only candidate — no `Backup` of this schedule's `scheduleRefs`
/// names it — is still not the catalog's to decide when a `Backup` in the
/// namespace refused the same receipt. Before the sweep this arm set
/// `verdict_refused: false` and took `selectable` from the (possibly stale) row.
///
/// MUTANT: `selectable: entry.selectable` in `merge_catalog_entry`'s
/// catalog-only arm. The refused point is selected and this row fails.
#[test]
fn a_catalog_only_row_for_a_refused_backup_is_never_selectable() {
    let topics: Vec<String> = Vec::new();
    let rules = rehearsal::SelectionRules {
        topics: &topics,
        min_age_seconds: 0,
        max_partitions: 200,
        target_cluster_id: TARGET_CLUSTER_ID,
    };
    for (result, refused) in [
        ("Invalid", true),
        ("Untrusted", true),
        ("SomeFutureVerdict", true),
        ("NotAttempted", false),
        ("Pending", false),
        ("Valid", false),
    ] {
        // A Backup of ANOTHER schedule — not a candidate of this one.
        let mut other = backup_value("b-other", "2026-09-19T02:00:00Z", json!(["orders"]), false);
        other["spec"]["scheduleRef"] = json!({"name": "someone-else", "uid": "x"});
        other["status"]["evidence"]["verification"]["result"] = json!(result);
        let other: weirkeeper::crds::backup::Backup =
            serde_json::from_value(other).expect("the fixture parses");
        let refusals = weirkeeper::catalog_view::ControllerRefusals::from_backups([&other]);

        let mut by_id = std::collections::BTreeMap::new();
        rs::merge_catalog_entry(
            &mut by_id,
            matching_catalog_row(1_758_240_000_000),
            Some(DESTINATION.to_string()),
            &refusals,
        );
        let merged = by_id.get(POINT_ID).expect("the catalog-only candidate");
        assert_eq!(merged.verdict_refused, refused, "{result}");
        assert_eq!(merged.selectable, !refused, "{result}");
        let candidates: Vec<_> = by_id.into_values().collect();
        assert_eq!(
            rehearsal::select_point(&candidates, &rules, now()).is_ok(),
            !refused,
            "{result}: a refused receipt is never rehearsed from a catalog row"
        );
    }
}

/// A `catalogRef`-only schedule lists the namespace's `Backup`s too — without
/// that read the refusal set above would always be empty.
///
/// MUTANT: list Backups only when `scheduleRefs` is set. The recorder never
/// sees `GET …/backups` and this row fails.
#[tokio::test]
async fn a_catalog_only_schedule_reads_the_backup_verdicts() {
    let mut value = schedule_value(json!({}));
    value["spec"]["point"]
        .as_object_mut()
        .expect("point")
        .remove("scheduleRefs");
    value["spec"]["point"]["catalogRef"] = json!({"name": "primary"});
    let schedule: RehearsalSchedule =
        serde_json::from_value(value).expect("the catalog-only fixture parses");
    let mut table = happy_routes();
    // An expired view: the join stops after the Backup read, which is all this
    // row is about.
    table.push(Route {
        method: "GET",
        path_suffix: "/recoverycatalogs/primary",
        status: 200,
        body: json!({
            "apiVersion": "logweir.dev/v1alpha1",
            "kind": "RecoveryCatalog",
            "metadata": {"name": "primary", "namespace": NS, "uid": "cat-uid", "resourceVersion": "2"},
            "spec": {
                "destinationRef": {"name": DESTINATION},
                "sync": {"deepCheck": "ManifestDigest", "intervalSeconds": 3600,
                         "maxObjectsPerRun": 100000, "mode": "Index", "viewLimit": 4}
            },
            "status": {"viewExpiresAt": "2026-09-01T00:00:00Z"}
        })
        .to_string(),
    });
    let (client, recorder, _) = mock_client_recording_bodies(table);
    rs::reconcile_schedule(&schedule, &context(client), now())
        .await
        .expect("the reconcile answers");
    assert!(
        seen(&recorder)
            .iter()
            .any(|(m, uri)| m == "GET" && uri.split('?').next().unwrap_or("").ends_with("/backups")),
        "a catalogRef-only schedule must read the Backup verdicts: {:?}",
        seen(&recorder)
    );
    assert_eq!(posted(&recorder, RESTORES_PATH), 0);
}

/// A `catalogRef`-only, topic-agnostic schedule (PLAT-15.2's shape), with an
/// authorization minted for ITS sealed spec.
fn catalog_only_schedule() -> (RehearsalSchedule, Value) {
    let mut value = schedule_value(json!({}));
    let point = value["spec"]["point"].as_object_mut().expect("point");
    point.remove("scheduleRefs");
    point.insert("catalogRef".to_string(), json!({"name": "primary"}));
    point.insert("topics".to_string(), json!([]));
    let schedule: RehearsalSchedule =
        serde_json::from_value(value).expect("the catalog-only fixture parses");
    let digest = rehearsal::template_digest(&schedule.spec).expect("the spec canonicalises");
    let mut scope = scope_value();
    scope["templateDigest"] = json!(digest);
    scope["topics"] = json!([]);
    let mut envelope: Value =
        serde_json::from_str(&envelope_with(SCHEDULE_UID, scope, "2026-11-01T00:00:00Z"))
            .expect("the envelope is JSON");
    envelope["plan_hash"] = json!(digest);
    let mut approval = approval_value(&serde_json::to_string_pretty(&envelope).expect("JSON"));
    approval["spec"]["planHash"] = json!(digest);
    (schedule, approval)
}

fn catalog_only_routes(approval: Value, backups: Vec<Value>) -> Vec<Route> {
    let row = serde_json::to_string(&matching_catalog_row(1_758_240_000_000)).expect("JSON");
    let mut table = routes(
        approval,
        trust_policy_value("Active", None),
        cluster_value(true, Some(TARGET_CLUSTER_ID)),
        backup_list(backups),
        restore_list(vec![]),
    );
    table.push(Route {
        method: "GET",
        path_suffix: "/recoverycatalogs/primary",
        status: 200,
        body: json!({
            "apiVersion": "logweir.dev/v1alpha1",
            "kind": "RecoveryCatalog",
            "metadata": {"name": "primary", "namespace": NS, "uid": "cat-uid", "resourceVersion": "2"},
            "spec": {
                "destinationRef": {"name": DESTINATION},
                "sync": {"deepCheck": "ManifestDigest", "intervalSeconds": 3600,
                         "maxObjectsPerRun": 100000, "mode": "Index", "viewLimit": 4}
            },
            "status": {
                "viewExpiresAt": "2026-09-21T00:00:00Z",
                "pages": [{"configMapName": "primary-g1-p0", "index": 0, "count": 1}]
            }
        })
        .to_string(),
    });
    table.push(Route {
        method: "GET",
        path_suffix: "/configmaps/primary-g1-p0",
        status: 200,
        body: json!({
            "apiVersion": "v1", "kind": "ConfigMap",
            "metadata": {"name": "primary-g1-p0", "namespace": NS, "resourceVersion": "1"},
            "immutable": true,
            "data": {(weirkeeper::catalog_view::PAGE_DATA_KEY): format!("{row}\n")}
        })
        .to_string(),
    });
    table
}

/// End to end through the reconciler: the catalog row is fresh and
/// selectable, and the ONLY difference between the two passes is the verdict
/// on a `Backup` of another schedule for the same receipt.
///
/// MUTANT: leave the pass's refusal set empty (`candidates` never fills it).
/// The refused pass fires and this row fails.
#[tokio::test]
async fn the_reconciler_applies_backup_refusals_to_catalog_only_points() {
    let (schedule, approval) = catalog_only_schedule();
    let backup = |result: &str| {
        let mut v = backup_value("b-other", "2026-09-19T02:00:00Z", json!(["orders"]), false);
        v["spec"]["scheduleRef"] = json!({"name": "someone-else", "uid": "x"});
        v["status"]["evidence"]["verification"]["result"] = json!(result);
        v
    };

    // CONTROL: NotAttempted — the catalog decides, and the slot fires.
    let (client, recorder, _) = mock_client_recording_bodies(catalog_only_routes(
        approval.clone(),
        vec![backup("NotAttempted")],
    ));
    let outcome = rs::reconcile_schedule(&schedule, &context(client), now())
        .await
        .expect("the reconcile answers");
    assert!(
        matches!(outcome.verdict, rs::Verdict::Fire(_)),
        "{:?}",
        outcome.verdict
    );
    assert_eq!(posted(&recorder, RESTORES_PATH), 1);

    // Invalid — the same fresh row is refused, and the slot is skipped.
    let (client, recorder, bodies) =
        mock_client_recording_bodies(catalog_only_routes(approval, vec![backup("Invalid")]));
    let outcome = rs::reconcile_schedule(&schedule, &context(client), now())
        .await
        .expect("the reconcile answers");
    assert!(
        matches!(&outcome.verdict, rs::Verdict::Skipped(s) if s.reason == rehearsal::SkipReason::NoQualifyingPoint),
        "{:?}",
        outcome.verdict
    );
    assert_eq!(posted(&recorder, RESTORES_PATH), 0);
    assert_eq!(last_skip(&bodies).as_deref(), Some("NoQualifyingPoint"));
}

/// Two `Backup`s over one receipt — this schedule's reads `NotAttempted`,
/// another's was refused — is a refused receipt: the catalog row cannot make
/// this schedule's candidate selectable either.
///
/// MUTANT: consult only the candidate's own `verdict_refused` in the
/// Backup-candidate arm of `merge_catalog_entry`. The row makes it selectable
/// and this row fails.
#[test]
fn a_refusal_on_another_backup_of_the_same_receipt_is_honoured() {
    let mine = capture_less_candidate("b-unread", "NotAttempted");
    let mut other = backup_value("b-other", "2026-09-19T02:00:00Z", json!(["orders"]), false);
    other["status"]["evidence"]["verification"]["result"] = json!("Untrusted");
    let other: weirkeeper::crds::backup::Backup =
        serde_json::from_value(other).expect("the fixture parses");
    let refusals = weirkeeper::catalog_view::ControllerRefusals::from_backups([&other]);
    let mut by_id = std::collections::BTreeMap::from([(mine.point_id.clone(), mine)]);
    rs::merge_catalog_entry(
        &mut by_id,
        matching_catalog_row(1_758_240_000_000),
        Some(DESTINATION.to_string()),
        &refusals,
    );
    let merged = &by_id[POINT_ID];
    assert!(merged.verdict_refused);
    assert!(!merged.selectable);
}

// ---- review L1 / M1: the Backup walk is complete, newest-first and lenient -

/// A walk the bound cuts short admits no catalog-only point, and the skip says
/// why — the refusal on a Backup the walk did not reach cannot be ruled out.
///
/// MUTANT: drop `refusals.is_complete()` from the catalog-only arm of
/// `merge_catalog_entry`. The slot fires from an unproven row; this row fails.
#[tokio::test]
async fn a_truncated_backup_walk_admits_no_catalog_only_point() {
    let (schedule, approval) = catalog_only_schedule();
    let mut table = catalog_only_routes(approval, vec![]);
    table.retain(|r| r.path_suffix != "/backups");
    table.push(Route {
        method: "GET",
        path_suffix: "/backups",
        status: 200,
        body: json!({
            "apiVersion": "v1", "kind": "BackupList",
            "metadata": {"resourceVersion": "1", "continue": "eyJwYWdlIjoyfQ"},
            "items": []
        })
        .to_string(),
    });
    let (client, recorder, _) = mock_client_recording_bodies(table);
    let outcome = rs::reconcile_schedule(&schedule, &context(client), now())
        .await
        .expect("the reconcile answers");
    match &outcome.verdict {
        rs::Verdict::Skipped(s) => {
            assert_eq!(s.reason, rehearsal::SkipReason::NoQualifyingPoint);
            assert!(s.detail.contains("Backup objects"), "{}", s.detail);
        }
        other => panic!("expected a NoQualifyingPoint skip, got {other:?}"),
    }
    assert_eq!(posted(&recorder, RESTORES_PATH), 0);
    let pages = seen(&recorder)
        .iter()
        .filter(|(m, uri)| m == "GET" && uri.split('?').next().unwrap_or("").ends_with("/backups"))
        .count();
    assert_eq!(
        pages,
        rs::MAX_BACKUP_PAGES,
        "every page up to the bound is read"
    );
}

/// More wanted Backups than `MAX_BACKUPS_SCANNED`, in NAME order oldest first:
/// the newest is still a candidate and is the point rehearsed.
///
/// MUTANT: keep the first `MAX_BACKUPS_SCANNED` in listing order (drop the
/// newest-first sort). The newest is cut and an older point fires.
#[tokio::test]
async fn the_newest_backup_is_never_cut_by_the_candidate_bound() {
    let base = at("2026-09-01T00:00:00Z");
    let items: Vec<Value> = (0..=rs::MAX_BACKUPS_SCANNED)
        .map(|i| {
            let capture = (base + chrono::Duration::minutes(i as i64 * 60)).to_rfc3339();
            let mut v = backup_value(
                &format!("logweir-backup-nightly-{i:04}"),
                &capture,
                json!(["orders"]),
                true,
            );
            v["status"]["backupId"] = json!(format!("b-{i:04}"));
            v["status"]["evidence"]["receiptSha256"] = json!(format!("sha256:{i:032x}{i:032x}"));
            v
        })
        .collect();
    // Named by its FULL digest: `lwp1-` ids are the first 32 hex characters,
    // so the point ids below must differ in them.
    let n = rs::MAX_BACKUPS_SCANNED;
    let newest_digest = format!("sha256:{n:032x}{n:032x}");
    let table = routes(
        approval_value(&envelope()),
        trust_policy_value("Active", None),
        cluster_value(true, Some(TARGET_CLUSTER_ID)),
        backup_list(items),
        restore_list(vec![]),
    );
    let (client, _, _) = mock_client_recording_bodies(table);
    let outcome = rs::reconcile_schedule(&schedule(), &context(client), now())
        .await
        .expect("the reconcile answers");
    match &outcome.verdict {
        rs::Verdict::Fire(order) => {
            assert_eq!(order.selected.point.receipt_sha256, newest_digest)
        }
        other => panic!("expected the newest point to fire, got {other:?}"),
    }
}

/// A Backup this build cannot type does not fail the pass: it is simply no
/// candidate, and the rest of the walk still decides.
///
/// MUTANT: list typed `Backup`s again. The pass errors; this row fails.
#[tokio::test]
async fn a_malformed_backup_does_not_fail_the_rehearsal_pass() {
    // A trigger kind a newer build wrote (`spec.trigger.kind` is a CLOSED enum
    // here), and — a second object — no `spec` at all: neither decodes as
    // this build's typed `Backup`.
    let mut future = backup_value("b-future", "2026-09-19T01:00:00Z", json!(["orders"]), true);
    future["spec"]["trigger"] = json!({"kind": "SomeFutureTriggerKind", "attempt": 0});
    let mut bare = future.clone();
    bare["metadata"]["name"] = json!("b-bare");
    bare.as_object_mut().expect("object").remove("spec");
    future["status"]["evidence"]["receiptSha256"] = json!(format!("sha256:{}", "9".repeat(64)));
    let mut table = happy_routes();
    table.retain(|r| r.path_suffix != "/backups");
    table.push(Route {
        method: "GET",
        path_suffix: "/backups",
        status: 200,
        body: backup_list(vec![
            future,
            bare,
            backup_value(
                "logweir-backup-nightly-20260919-020000",
                "2026-09-19T02:00:00Z",
                json!(["orders", "payments"]),
                true,
            ),
        ]),
    });
    let (client, recorder, _) = mock_client_recording_bodies(table);
    let outcome = rs::reconcile_schedule(&schedule(), &context(client), now())
        .await
        .expect("a malformed Backup does not fail the pass");
    assert!(
        matches!(outcome.verdict, rs::Verdict::Fire(_)),
        "{:?}",
        outcome.verdict
    );
    assert_eq!(posted(&recorder, RESTORES_PATH), 1);
}
