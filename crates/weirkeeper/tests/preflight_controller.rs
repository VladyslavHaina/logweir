//! `Preflight`: the catalogue's controller half, the check Job's life cycle,
//! and the two properties the tracker's PLAT-03.1 / PLAT-03.2 acceptance
//! criteria are made of — every failed prerequisite is NAMED with a remedy, a
//! check time and a scope, and a green preview cannot survive an edit or
//! bypass a later collision.
//!
//! # What each layer is for
//!
//! * **Pure rows.** Every controller-authority row is a function of one reduced
//!   fact, so each of D2 §6.3's codes has a test that names the code rather
//!   than a route table.
//! * **Controller doubles.** `weirkeeper::testing`'s client panics on a request
//!   it was not given a route for, so "the reconciler called nothing its test
//!   did not record" is a property and not a hope.
//! * **Source scans.** D2 §6.8(a) is a claim about code that does NOT exist:
//!   no execution path may read a `Preflight`. The only way to assert that is
//!   to read the sources.
//!
//! # The planted mutants
//!
//! "A guard without a mutant is not a guard." Each was applied to the shipped
//! source, measured, and reverted; the rows that die are named beside it in the
//! worker report.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Duration, TimeZone, Utc};
use logweir_core::check_contract::{
    frames, Authority, CheckCode, CheckId, CheckOutcome, CheckPlanKind, CheckResult, CheckState,
    EndFrame, Gating, OverallState, Stream,
};
use logweir_core::check_contract::{Referent, RosterRef, StaleReason};
use serde_json::{json, Value};

use weirkeeper::check::{self, Projections, Waiting};
use weirkeeper::controllers::preflight::{
    self as pf, assemble, cluster_identity_row, connection_row, controller_rows, destination_row,
    entry_of, job_rows, plan_bindings_row, plan_names_row, plan_parse_row, pod_outcomes,
    recovery_point_row, result_for, signer_rostered_row, stale_against_status, ApprovalFacts,
    BindingFacts, Inputs, PlanFacts, RecoveryPointFacts, RosterFacts,
};
use weirkeeper::controllers::Context;
use weirkeeper::crds::preflight::{Preflight, PreflightOperation};
use weirkeeper::testing::{mock_client_recording_bodies, Route};

const NS: &str = "team-a";
const PF_UID: &str = "aaaaaaaa-0000-4000-8000-00000000000a";
const CLUSTER_UID: &str = "bbbbbbbb-0000-4000-8000-00000000000b";
const DEST_UID: &str = "cccccccc-0000-4000-8000-00000000000c";
const ROSTER_UID: &str = "dddddddd-0000-4000-8000-00000000000d";
const JOB_UID: &str = "eeeeeeee-0000-4000-8000-00000000000e";
const POD_UID: &str = "ffffffff-0000-4000-8000-00000000000f";
const BACKUP_UID: &str = "99999999-0000-4000-8000-000000000009";

/// The value that must never appear in a status, a request body or a rendered
/// document. It stands for the bytes behind every `secretRef` in this file.
const FIXTURE_SECRET_VALUE: &str = "wJalrXUtnFEMI-K7MDENG-bPxRfiCYEXAMPLEKEY";

fn now() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 9, 16, 12, 0, 0).unwrap()
}

fn job_name(kind: CheckPlanKind) -> String {
    check::job::check_job_name(kind, PF_UID)
}

// ===========================================================================
// Fixtures
// ===========================================================================

fn preflight(request: Value) -> Preflight {
    serde_json::from_value(json!({
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "Preflight",
        "metadata": {
            "name": "pf-1", "namespace": NS, "uid": PF_UID,
            "generation": 1, "resourceVersion": "100"
        },
        "spec": { "request": request }
    }))
    .expect("the Preflight fixture parses")
}

fn backup_request() -> Value {
    json!({
        "operation": "Backup",
        "backup": {
            "sourceRef": {"name": "source"},
            "destinationRef": {"name": "primary"},
            "topics": ["orders"]
        },
        "timeoutSeconds": 120
    })
}

fn restore_request(extra: Value) -> Value {
    let mut restore = json!({
        "planBytes": plan_yaml("restore-", "s3-bucket"),
        "planHash": logweir_core::ids::sha256_prefixed(
            plan_yaml("restore-", "s3-bucket").as_bytes()
        ),
        "targetRef": {"name": "target"},
        "sourceDestinationRef": {"name": "primary"},
        "evidenceDestinationRef": {"name": "evidence"}
    });
    if let (Some(a), Some(b)) = (restore.as_object_mut(), extra.as_object()) {
        for (k, v) in b {
            a.insert(k.clone(), v.clone());
        }
    }
    json!({"operation": "Restore", "restore": restore, "timeoutSeconds": 120})
}

/// A restore plan in the runner's OWN grammar — `logweir_core::spec::DrillSpec`
/// — because `planHash` binds these exact bytes and a plan the controller
/// re-serialised would be a different document.
fn plan_yaml(prefix: &str, bucket: &str) -> String {
    format!(
        "source:\n  storage:\n    backend: s3\n    bucket: {bucket}\n    prefix: ''\n    \
         path_style: true\n  backup: bk-1\n  topics:\n    - orders\ntarget:\n  \
         bootstrap_servers:\n    - target-kafka:9092\n  mode: scratch\n  marker_topic: \
         logweir-marker\n  topic_mapping_prefix: '{prefix}'\nsample:\n  window_start: \
         2026-09-01T00:00:00Z\n  window_end: 2026-09-15T00:00:00Z\nobjectives:\n  rto_seconds: \
         3600\nevidence:\n  backend: s3\n  bucket: {bucket}\n  prefix: logweir/\n  \
         path_style: true\n"
    )
}

fn kafka_cluster(name: &str, cluster_id: Option<&str>) -> Value {
    json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "KafkaCluster",
        "metadata": {"name": name, "namespace": NS, "uid": CLUSTER_UID, "generation": 2},
        "spec": {
            "bootstrapServers": [format!("{name}-kafka:9092")],
            "role": if name == "source" { "source" } else { "target" },
            "markerTopic": "logweir-marker",
            "auth": {
                "mode": "scramSha512", "username": "backup", "tls": true,
                "secretRef": {"name": "kafka-src", "passwordKey": "password"}
            }
        },
        "status": {"clusterId": cluster_id}
    })
}

fn backup_destination(name: &str) -> Value {
    json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "BackupDestination",
        "metadata": {"name": name, "namespace": NS, "uid": DEST_UID, "generation": 3},
        "spec": {
            "storage": {"provider": "S3", "bucket": "s3-bucket", "addressing": "PathStyle"},
            "transport": {"security": "TLS"},
            "access": {
                "archiveWrite": {
                    "mode": "SecretKeys",
                    "secret": {
                        "name": "logweir-s3",
                        "accessKeyIdKey": "AWS_ACCESS_KEY_ID",
                        "secretAccessKeyKey": "AWS_SECRET_ACCESS_KEY"
                    }
                }
            }
        },
        "status": {
            "observedGeneration": 3,
            "reason": "Valid",
            "conditions": [{
                "type": "Valid", "status": "True", "observedGeneration": 3,
                "reason": "Valid", "message": "ok"
            }]
        }
    })
}

fn roster(signing_key: &str, allowed: Vec<&str>) -> Value {
    json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "TrustRoster",
        "metadata": {"name": "default", "uid": ROSTER_UID, "generation": 4},
        "spec": {
            "approverKeys": [{"keyId": "approver-1", "spkiPem": "-----BEGIN PUBLIC KEY-----\nAA\n-----END PUBLIC KEY-----"}],
            "signingKeys": [{"keyId": signing_key, "spkiPem": "-----BEGIN PUBLIC KEY-----\nBB\n-----END PUBLIC KEY-----"}],
            "allowedClusterIds": allowed
        }
    })
}

fn finished_job(name: &str) -> Value {
    json!({
        "apiVersion": "batch/v1", "kind": "Job",
        "metadata": {
            "name": name, "namespace": NS, "uid": JOB_UID,
            "ownerReferences": [owner_reference()]
        },
        "status": {"conditions": [{"type": "Complete", "status": "True"}]}
    })
}

fn running_job(name: &str) -> Value {
    json!({
        "apiVersion": "batch/v1", "kind": "Job",
        "metadata": {
            "name": name, "namespace": NS, "uid": JOB_UID,
            "creationTimestamp": "2026-09-16T11:59:00Z",
            "ownerReferences": [owner_reference()]
        },
        "status": {"active": 1}
    })
}

/// The `ownerReference` `check::cancel` verifies before it patches anything: a
/// Job's NAME is a pure function of the subject's UID, and a name is not an
/// identity.
fn owner_reference() -> Value {
    json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "Preflight",
        "name": "pf-1", "uid": PF_UID, "controller": true
    })
}

fn owned_pod(job: &str, state: Value) -> Value {
    json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {
            "name": "check-pod", "namespace": NS, "uid": POD_UID,
            "labels": {"batch.kubernetes.io/job-name": job},
            "ownerReferences": [{
                "apiVersion": "batch/v1", "kind": "Job", "name": job,
                "uid": JOB_UID, "controller": true
            }]
        },
        "status": {"containerStatuses": [{
            "name": "runner", "image": "logweir:latest",
            "imageID": "sha256:abcdef", "ready": false,
            "restartCount": 0, "state": state
        }]}
    })
}

fn terminated(exit_code: i32) -> Value {
    json!({"terminated": {"exitCode": exit_code, "finishedAt": "2026-09-16T11:59:30Z"}})
}

fn list_of(items: Vec<Value>) -> String {
    json!({"apiVersion": "v1", "kind": "List", "items": items}).to_string()
}

/// A pod log carrying one verified relay of `checks`.
fn relay_log(plan_sha: &str, checks: Vec<CheckOutcome>, details: Option<&str>) -> String {
    let mut result = CheckResult::new(CheckPlanKind::OperationReadiness);
    result.checks = checks;
    let payload = result.to_canonical_json().expect("the result serialises");
    let mut lines: Vec<String> = Vec::new();
    let mut streams: BTreeMap<Stream, (Vec<u8>, usize)> = BTreeMap::new();
    let parts = frames::write_parts(Stream::Result, &payload).expect("parts");
    streams.insert(Stream::Result, (payload.clone(), parts.len()));
    lines.extend(parts);
    if let Some(details) = details {
        let bytes = details.as_bytes().to_vec();
        let parts = frames::write_parts(Stream::Details, &bytes).expect("parts");
        streams.insert(Stream::Details, (bytes, parts.len()));
        lines.extend(parts);
    }
    let end: EndFrame = frames::end_frame(plan_sha, PF_UID, &streams, None);
    lines.push(frames::write_end(&end).expect("end frame"));
    lines.join("\n")
}

/// A runner-owned outcome, with the runner's own gating and expiry.
fn runner_row(id: CheckId, state: CheckState, code: CheckCode, gating: Gating) -> CheckOutcome {
    CheckOutcome::new(id, state, gating, Authority::CheckJob, code)
        .with_message("from the check Job")
        .with_times(now(), now() + Duration::minutes(15))
}

fn plan_config_map(job: &str, digest: &str) -> Value {
    json!({
        "apiVersion": "v1", "kind": "ConfigMap",
        "metadata": {
            "name": format!("{job}-plan"), "namespace": NS,
            "annotations": {"logweir.dev/check-plan-sha256": digest},
            "ownerReferences": [{
                "apiVersion": "logweir.dev/v1alpha1", "kind": "Preflight",
                "name": "pf-1", "uid": PF_UID, "controller": true
            }]
        },
        "immutable": true,
        "data": {"check-plan.json": "{}"}
    })
}

fn context(client: kube::Client) -> Context {
    Context {
        client,
        archive: None,
        runner_image: weirkeeper::job::RunnerImage::default(),
    }
}

// ===========================================================================
// 1. The catalogue
// ===========================================================================

/// Every row the controller claims to own can be CONSTRUCTED, and nothing else
/// can.
///
/// `pf::outcome` panics for an id the operation's tables do not carry, which is
/// what makes gating and expiry properties of the row rather than of the call
/// site. Without this, a typo'd id would be a panic in production on the one
/// pass that hit it.
#[test]
fn every_controller_row_has_a_catalogue_entry() {
    for operation in [
        PreflightOperation::Backup,
        PreflightOperation::Restore,
        PreflightOperation::DestinationAccess,
    ] {
        let rows = controller_rows(operation);
        assert!(!rows.is_empty(), "{operation:?} owns no row at all");
        for row in &rows {
            let built = pf::outcome(
                operation,
                row.id,
                CheckState::Ready,
                CheckCode::Valid,
                now(),
            );
            assert_eq!(built.gating, row.gating, "{} gating", row.id);
            assert_eq!(built.authority, row.authority, "{} authority", row.id);
            assert_eq!(
                built.expires_at.is_some(),
                row.expiry.is_some(),
                "{} expiry presence",
                row.id
            );
        }
        // No id appears twice: two rows with one id is two answers to one
        // question, which is what the whole merge order exists to prevent.
        let ids: BTreeSet<CheckId> = rows.iter().map(|r| r.id).collect();
        assert_eq!(ids.len(), rows.len(), "{operation:?} lists an id twice");
        // And no controller row is also a Job row.
        for id in job_rows(operation) {
            assert!(
                !ids.contains(&id),
                "{id} is claimed by both the controller and the check Job for {operation:?}"
            );
        }
    }
}

/// The five-minute rows are five minutes, and the "until bytes change" rows
/// carry no expiry at all.
///
/// D2 §6.3 gives `target.mappedTopics` and `target.topicCreate` the shortest
/// expiry in the catalogue because a topic can appear on the target between a
/// preview and a run — which is exactly the race PLAT-03.2 names. Those two are
/// the RUNNER's rows; the controller's own "until bytes change" pair is the
/// other half of the same rule.
#[test]
fn the_expiry_table_is_the_decisions_table() {
    let parse = pf::outcome(
        PreflightOperation::Restore,
        CheckId::PlanParse,
        CheckState::Ready,
        CheckCode::PlanParsed,
        now(),
    );
    assert_eq!(
        parse.expires_at, None,
        "`plan.parse` is a function of bytes, not of time (D2 §6.3 \"until bytes change\"); an \
         expiry here would pull the aggregate's expiry forward for no reason"
    );
    let names = pf::outcome(
        PreflightOperation::Restore,
        CheckId::PlanNames,
        CheckState::Ready,
        CheckCode::MappedNamesLegal,
        now(),
    );
    assert_eq!(names.expires_at, None);
    let approval = pf::outcome(
        PreflightOperation::Restore,
        CheckId::ApprovalState,
        CheckState::Ready,
        CheckCode::ApprovalVerified,
        now(),
    );
    assert_eq!(
        approval.expires_at,
        Some(now() + Duration::minutes(10)),
        "D2 §6.3 gives approval.state min(10 m, key notAfter)"
    );
    let resolved = pf::outcome(
        PreflightOperation::Backup,
        CheckId::ConnectionResolved,
        CheckState::Ready,
        CheckCode::Resolved,
        now(),
    );
    assert_eq!(resolved.expires_at, Some(now() + Duration::minutes(15)));
}

/// An expiry is pulled forward by a key's own `notAfter`, never pushed back.
#[test]
fn a_verdict_about_a_key_never_outlives_the_key() {
    let soon = now() + Duration::minutes(2);
    let late = now() + Duration::hours(4);
    let base = pf::outcome(
        PreflightOperation::Backup,
        CheckId::SignerRostered,
        CheckState::Ready,
        CheckCode::SignerRostered,
        now(),
    );
    assert_eq!(
        pf::cap_expiry(base.clone(), Some(soon)).expires_at,
        Some(soon)
    );
    assert_eq!(
        pf::cap_expiry(base, Some(late)).expires_at,
        Some(now() + Duration::minutes(15)),
        "a key valid for four hours does not extend a fifteen-minute row"
    );
}

// ===========================================================================
// 2. The controller-authority rows, code by code
// ===========================================================================

#[test]
fn a_connection_that_does_not_exist_is_named_and_not_guessed_at() {
    let row = connection_row(PreflightOperation::Backup, None, "source", now());
    assert_eq!(row.id, CheckId::ConnectionResolved);
    assert_eq!(row.state, CheckState::NotReady);
    assert_eq!(row.code, CheckCode::ConnectionNotFound);
    assert!(row.message.contains("source"), "the name is in the message");
    assert!(
        !row.remedy.is_empty(),
        "every failed prerequisite has a remedy"
    );
    assert_eq!(
        row.scope.as_ref().map(|s| s.kind.as_str()),
        Some("KafkaCluster"),
        "the tracker's acceptance is `names each failed prerequisite and its remedy with check \
         time and scope`"
    );
    assert!(row.observed_at.is_some(), "and its check time");
}

#[test]
fn a_restore_reports_the_target_rows_and_not_the_connection_rows() {
    let row = connection_row(PreflightOperation::Restore, None, "target", now());
    assert_eq!(
        row.id,
        CheckId::TargetResolved,
        "D2 §6.3: operation Restore runs the TARGET equivalents"
    );
}

#[test]
fn an_unrenderable_credential_is_its_own_code() {
    let refusal = weirkeeper::connection::ConnectionRefusal {
        reason: weirkeeper::conditions::TERMINAL_STATE_CREDENTIAL_NOT_RENDERABLE,
        field: "spec.auth.secretRef".to_string(),
        message: "the connection names no credential Secret".to_string(),
    };
    let row = connection_row(
        PreflightOperation::Backup,
        Some(&Err(refusal)),
        "source",
        now(),
    );
    assert_eq!(row.code, CheckCode::CredentialReferenceMissing);
    let other = weirkeeper::connection::ConnectionRefusal {
        reason: weirkeeper::conditions::TERMINAL_STATE_CONNECTION_CONFIG_INVALID,
        field: "spec.auth.tls".to_string(),
        message: "plaintext with tls: true is refused, not downgraded".to_string(),
    };
    let row = connection_row(
        PreflightOperation::Backup,
        Some(&Err(other)),
        "source",
        now(),
    );
    assert_eq!(row.code, CheckCode::ConnectionInvalid);
}

#[test]
fn the_destination_row_carries_the_resolvers_own_code() {
    let refusal = weirkeeper::destination::DestinationRefusal {
        code: CheckCode::DestinationRoleNotConfigured,
        field: "spec.access.archiveRead".to_string(),
        message: "the archiveRead grant is not configured".to_string(),
    };
    let row = destination_row(PreflightOperation::Restore, &Err(refusal), "primary", now());
    assert_eq!(row.id, CheckId::DestinationResolved);
    assert_eq!(row.code, CheckCode::DestinationRoleNotConfigured);
    assert_eq!(row.gating, Gating::Blocking);
    assert!(row.remedy.contains("BackupDestination"));
}

#[test]
fn the_egress_row_is_unknown_forever_and_names_the_ports() {
    let row = pf::egress_row(
        PreflightOperation::Backup,
        &["9094".to_string()],
        &["9000".to_string()],
        now(),
    );
    assert_eq!(row.gating, Gating::ExecutionOnly);
    assert_eq!(
        row.state,
        CheckState::Unknown,
        "`CheckOutcome::new` forces an execution-only row to unknown whatever a caller passes"
    );
    assert_eq!(row.code, CheckCode::NetworkPolicyEnforcementNotObservable);
    assert!(row.remedy.contains("9094") && row.remedy.contains("9000"));
    assert_eq!(
        logweir_core::check_contract::aggregate(&[row]),
        OverallState::Unknown,
        "an execution-only row is excluded from aggregation, so a set of only E rows is unknown"
    );
}

#[test]
fn the_signer_row_walks_the_whole_roster_table() {
    let key = "runner-key-1";
    let empty = RosterFacts::default();
    assert_eq!(
        signer_rostered_row(PreflightOperation::Backup, &empty, Some(key), now()).code,
        CheckCode::TrustRosterNotFound
    );
    let no_keys = RosterFacts {
        found: true,
        ..RosterFacts::default()
    };
    assert_eq!(
        signer_rostered_row(PreflightOperation::Backup, &no_keys, Some(key), now()).code,
        CheckCode::TrustRosterNotLoaded
    );
    let loaded = RosterFacts {
        found: true,
        signing_keys: vec![(key.to_string(), None)],
        ..RosterFacts::default()
    };
    let unobserved = signer_rostered_row(PreflightOperation::Backup, &loaded, None, now());
    assert_eq!(
        (unobserved.state, unobserved.code),
        (CheckState::Unknown, CheckCode::SignerKeyIdNotObserved),
        "the PUBLIC key id is the check Job's to report; the controller may not read the Secret, \
         so `SignerNotRostered` before the pod ran would be a claim about a key nobody saw"
    );
    assert_eq!(
        signer_rostered_row(PreflightOperation::Backup, &loaded, Some("other"), now()).code,
        CheckCode::SignerNotRostered
    );
    let expired = RosterFacts {
        found: true,
        signing_keys: vec![(key.to_string(), Some(now() - Duration::hours(1)))],
        ..RosterFacts::default()
    };
    let row = signer_rostered_row(PreflightOperation::Backup, &expired, Some(key), now());
    assert_eq!(row.code, CheckCode::SignerKeyExpired);
    assert_eq!(row.expires_at, Some(now() - Duration::hours(1)));
    let ok = signer_rostered_row(PreflightOperation::Backup, &loaded, Some(key), now());
    assert_eq!(
        (ok.state, ok.code),
        (CheckState::Ready, CheckCode::SignerRostered)
    );
    assert_eq!(ok.facts.get("signerKeyId").map(String::as_str), Some(key));
}

#[test]
fn a_source_on_the_restore_allowlist_is_refused_before_the_run() {
    let row = cluster_identity_row(
        PreflightOperation::Backup,
        Some("scratch-id"),
        Some("scratch-id"),
        &["scratch-id".to_string()],
        None,
        false,
        now(),
    );
    assert_eq!(
        row.code,
        CheckCode::SourceIsAllowlistedTarget,
        "the runner's phase -1 rail refuses to back up a restore target; reporting it here is \
         how an operator learns before the run instead of from an exit code"
    );
}

#[test]
fn a_changed_cluster_identity_beats_every_later_question() {
    let row = cluster_identity_row(
        PreflightOperation::Restore,
        Some("observed"),
        Some("recorded"),
        &["observed".to_string()],
        None,
        true,
        now(),
    );
    assert_eq!(row.code, CheckCode::ClusterIdentityChanged);
    assert_eq!(
        row.facts.get("clusterId").map(String::as_str),
        Some("observed")
    );
}

#[test]
fn a_scratch_target_must_be_allowlisted_and_a_new_topic_target_need_not_be() {
    let scratch = cluster_identity_row(
        PreflightOperation::Restore,
        Some("t"),
        None,
        &[],
        None,
        true,
        now(),
    );
    assert_eq!(scratch.code, CheckCode::TargetNotAllowlisted);
    let new_topic = cluster_identity_row(
        PreflightOperation::Restore,
        Some("t"),
        None,
        &[],
        None,
        false,
        now(),
    );
    assert_eq!(
        new_topic.code,
        CheckCode::TargetAllowed,
        "D2 §6.3 spells TargetNotAllowlisted `(scratch)`: a newTopic restore writes into a named \
         cluster on purpose"
    );
}

#[test]
fn a_target_that_is_the_source_is_refused_whatever_the_allowlist_says() {
    let row = cluster_identity_row(
        PreflightOperation::Restore,
        Some("same"),
        None,
        &["same".to_string()],
        Some("same"),
        true,
        now(),
    );
    assert_eq!(row.code, CheckCode::TargetEqualsSource);
}

#[test]
fn an_unobserved_cluster_id_is_unknown_and_not_a_refusal() {
    let row = cluster_identity_row(
        PreflightOperation::Backup,
        None,
        Some("recorded"),
        &[],
        None,
        false,
        now(),
    );
    assert_eq!(
        (row.state, row.code),
        (CheckState::Unknown, CheckCode::ClusterIdentityNotObserved)
    );
}

// ===========================================================================
// 3. The restore-only rows
// ===========================================================================

fn plan_facts(prefix: &str, bucket: &str, claimed: Option<&str>) -> PlanFacts {
    let text = plan_yaml(prefix, bucket);
    PlanFacts::of(text.as_bytes(), claimed)
}

#[test]
fn the_plan_hash_is_recomputed_and_the_claim_is_never_trusted() {
    let facts = plan_facts(
        "restore-",
        "s3-bucket",
        Some("sha256:0000000000000000000000000000000000000000000000000000000000000000"),
    );
    let row = plan_parse_row(&facts, now());
    assert_eq!(row.code, CheckCode::PlanHashMismatch);
    assert!(
        row.message
            .contains(&weirkeeper::controllers::preflight::short_digest(
                &facts.recomputed_hash
            )),
        "the message quotes a SHORT digest: `redact`'s long-hex rule replaces a whole sha256, so \
         a message carrying one reaches a status as `[redacted]`. Got: {}",
        row.message
    );
}

#[test]
fn a_plan_that_does_not_parse_says_so_and_nothing_else_is_guessed() {
    let facts = PlanFacts::of(b"target: [this is not a plan", None);
    assert_eq!(
        plan_parse_row(&facts, now()).code,
        CheckCode::PlanUnparseable
    );
    assert_eq!(
        plan_names_row(&facts, now()).code,
        CheckCode::PlanUnparseable
    );
    assert_eq!(
        plan_bindings_row(&facts, &BindingFacts::default(), now()).code,
        CheckCode::PlanUnparseable
    );
}

#[test]
fn the_mapped_names_walk_phase_zeros_four_refusals() {
    // A legal plan first, so the rest are differences from something that works.
    let ok = plan_facts("restore-", "s3-bucket", None);
    assert_eq!(plan_names_row(&ok, now()).code, CheckCode::MappedNamesLegal);

    let glob = PlanFacts::of(
        plan_yaml("restore-", "s3-bucket")
            .replace("- orders", "- 'orders*'")
            .as_bytes(),
        None,
    );
    assert_eq!(plan_names_row(&glob, now()).code, CheckCode::GlobInTopic);

    let expansion = PlanFacts::of(plan_yaml("restore-${HOME}-", "s3-bucket").as_bytes(), None);
    assert_eq!(
        plan_names_row(&expansion, now()).code,
        CheckCode::ExpansionInTopic,
        "the engine's configuration loader expands `${{NAME}}` in pre-parse text, so a name that \
         can name an environment variable is a name that can read one"
    );

    let identity = PlanFacts::of(plan_yaml("", "s3-bucket").as_bytes(), None);
    assert_eq!(
        plan_names_row(&identity, now()).code,
        CheckCode::TopicMappingIdentity
    );

    let long = "x".repeat(250);
    let illegal = PlanFacts::of(plan_yaml(&long, "s3-bucket").as_bytes(), None);
    assert_eq!(
        plan_names_row(&illegal, now()).code,
        CheckCode::MappedTopicNameIllegal
    );
}

#[test]
fn the_bindings_row_names_which_reference_moved() {
    let facts = plan_facts("restore-", "s3-bucket", None);
    let storage = |bucket: &str| logweir_core::engine::StorageUrl::S3 {
        bucket: bucket.to_string(),
        prefix: String::new(),
        region: None,
        endpoint: None,
        path_style: true,
        allow_http: false,
    };
    let evidence = |bucket: &str| logweir_core::engine::StorageUrl::S3 {
        bucket: bucket.to_string(),
        prefix: "logweir/".to_string(),
        region: None,
        endpoint: None,
        path_style: true,
        allow_http: false,
    };
    let ok = BindingFacts {
        target_bootstrap: vec!["target-kafka:9092".to_string()],
        source_storage: Some(storage("s3-bucket")),
        evidence_storage: Some(evidence("s3-bucket")),
        recovery_point_topics: Some(vec!["orders".to_string()]),
    };
    assert_eq!(
        plan_bindings_row(&facts, &ok, now()).code,
        CheckCode::PlanMatchesReferences
    );

    let moved_target = BindingFacts {
        target_bootstrap: vec!["somewhere-else:9092".to_string()],
        ..ok.clone()
    };
    assert_eq!(
        plan_bindings_row(&facts, &moved_target, now()).code,
        CheckCode::PlanTargetMismatch
    );

    let moved_source = BindingFacts {
        source_storage: Some(storage("other-bucket")),
        ..ok.clone()
    };
    assert_eq!(
        plan_bindings_row(&facts, &moved_source, now()).code,
        CheckCode::PlanDestinationMismatch
    );

    let moved_evidence = BindingFacts {
        evidence_storage: Some(evidence("other-bucket")),
        ..ok.clone()
    };
    assert_eq!(
        plan_bindings_row(&facts, &moved_evidence, now()).code,
        CheckCode::PlanEvidenceDestinationMismatch
    );

    let short = BindingFacts {
        recovery_point_topics: Some(vec!["payments".to_string()]),
        ..ok
    };
    let row = plan_bindings_row(&facts, &short, now());
    assert_eq!(row.code, CheckCode::PlanTopicsNotInRecoveryPoint);
    assert!(
        row.detail.as_ref().and_then(|d| d.get("sample")).is_some(),
        "the bounded sample is what a UI shows; the full list belongs in the details ConfigMap"
    );
}

#[test]
fn a_recreated_recovery_point_is_not_the_recovery_point_that_was_pinned() {
    let facts = RecoveryPointFacts::Found {
        name: "nightly-1".to_string(),
        uid: "new-uid".to_string(),
        expected_uid: Some(BACKUP_UID.to_string()),
        phase: Some("Succeeded".to_string()),
        location_digest: None,
        expected_location_digest: None,
    };
    let row = recovery_point_row(&facts, now()).expect("a requested recovery point has a row");
    assert_eq!(
        row.code,
        CheckCode::RecoveryPointUidChanged,
        "PLAT-11.1 fixes a Backup UID as the recovery point's identity; a same-named replacement \
         is a different run over a different window"
    );
}

#[test]
fn the_recovery_point_row_walks_its_table() {
    assert!(
        recovery_point_row(&RecoveryPointFacts::NotRequested, now()).is_none(),
        "a request that pinned no recovery point gets no row, rather than a row about nothing"
    );
    assert_eq!(
        recovery_point_row(
            &RecoveryPointFacts::NotFound {
                name: "gone".to_string()
            },
            now()
        )
        .unwrap()
        .code,
        CheckCode::RecoveryPointNotFound
    );
    let running = RecoveryPointFacts::Found {
        name: "nightly-1".to_string(),
        uid: BACKUP_UID.to_string(),
        expected_uid: None,
        phase: Some("Running".to_string()),
        location_digest: None,
        expected_location_digest: None,
    };
    assert_eq!(
        recovery_point_row(&running, now()).unwrap().code,
        CheckCode::RecoveryPointNotSucceeded
    );
    let ok = RecoveryPointFacts::Found {
        name: "nightly-1".to_string(),
        uid: BACKUP_UID.to_string(),
        expected_uid: Some(BACKUP_UID.to_string()),
        phase: Some("Succeeded".to_string()),
        location_digest: None,
        expected_location_digest: None,
    };
    let row = recovery_point_row(&ok, now()).unwrap();
    assert_eq!(row.code, CheckCode::RecoveryPointSucceeded);
    assert_eq!(
        row.scope.as_ref().and_then(|s| s.uid.clone()),
        Some(BACKUP_UID.to_string()),
        "the scope carries the UID the verdict is about"
    );
}

fn approver_roster(not_after: Option<DateTime<Utc>>) -> RosterFacts {
    RosterFacts {
        found: true,
        uid: ROSTER_UID.to_string(),
        generation: 4,
        signing_keys: vec![("runner-key-1".to_string(), None)],
        approver_keys: vec![("approver-1".to_string(), not_after)],
        allowed_cluster_ids: Vec::new(),
    }
}

fn found_approval(verified: Option<bool>, approved_hash: Option<&str>) -> ApprovalFacts {
    ApprovalFacts::Found {
        name: "ap-1".to_string(),
        uid: "ap-uid".to_string(),
        resource_version: "7".to_string(),
        verified,
        reason: Some("the DSSE signature did not verify".to_string()),
        matched_key_id: Some("approver-1".to_string()),
        approved_plan_hash: approved_hash.map(str::to_string),
        subject_name: Some("r-1".to_string()),
    }
}

#[test]
fn a_draft_plan_is_skipped_and_a_skip_is_not_an_answer() {
    let rows = pf::approval_rows(
        &ApprovalFacts::Draft,
        &approver_roster(None),
        "sha256:aa",
        None,
        None,
        now(),
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(
        (rows[0].state, rows[0].code),
        (CheckState::Skipped, CheckCode::SubjectNotCreated)
    );
    assert_eq!(
        logweir_core::check_contract::aggregate(&rows),
        OverallState::Unknown,
        "D2 §6.2: a skipped BLOCKING check keeps the overall verdict unknown"
    );
}

#[test]
fn an_expired_approver_key_is_an_expired_approval() {
    let rows = pf::approval_rows(
        &found_approval(Some(true), Some("sha256:aa")),
        &approver_roster(Some(now() - Duration::minutes(1))),
        "sha256:aa",
        Some("r-1"),
        None,
        now(),
    );
    let state = rows
        .iter()
        .find(|r| r.id == CheckId::ApprovalState)
        .unwrap();
    assert_eq!(state.code, CheckCode::ApprovalExpired);
    assert_eq!(
        state.expires_at,
        Some(now() - Duration::minutes(1)),
        "a verdict about a key never outlives the key"
    );
}

#[test]
fn an_approval_for_another_plan_is_a_plan_mismatch_and_not_a_pending_approval() {
    let rows = pf::approval_rows(
        &found_approval(Some(false), Some("sha256:bb")),
        &approver_roster(None),
        "sha256:aa",
        Some("r-1"),
        None,
        now(),
    );
    let state = rows
        .iter()
        .find(|r| r.id == CheckId::ApprovalState)
        .unwrap();
    assert_eq!(
        state.code,
        CheckCode::ApprovalPlanMismatch,
        "reporting a plan mismatch as `not verified yet` sends an operator to wait for something \
         that has already happened"
    );
}

#[test]
fn an_approval_with_no_status_yet_is_pending_and_not_a_refusal() {
    let rows = pf::approval_rows(
        &found_approval(None, Some("sha256:aa")),
        &approver_roster(None),
        "sha256:aa",
        Some("r-1"),
        None,
        now(),
    );
    let state = rows
        .iter()
        .find(|r| r.id == CheckId::ApprovalState)
        .unwrap();
    assert_eq!(
        (state.state, state.code),
        (CheckState::Unknown, CheckCode::ApprovalPending)
    );
}

#[test]
fn an_approval_that_names_another_subject_is_a_subject_mismatch() {
    let rows = pf::approval_rows(
        &found_approval(Some(true), Some("sha256:aa")),
        &approver_roster(None),
        "sha256:aa",
        Some("r-2"),
        None,
        now(),
    );
    let state = rows
        .iter()
        .find(|r| r.id == CheckId::ApprovalState)
        .unwrap();
    assert_eq!(state.code, CheckCode::ApprovalSubjectMismatch);
}

#[test]
fn a_key_that_expires_mid_restore_is_a_warning_and_not_a_refusal() {
    let rows = pf::approval_rows(
        &found_approval(Some(true), Some("sha256:aa")),
        &approver_roster(Some(now() + Duration::minutes(5))),
        "sha256:aa",
        Some("r-1"),
        Some(now() + Duration::hours(1)),
        now(),
    );
    let validity = rows
        .iter()
        .find(|r| r.id == CheckId::ApprovalKeyValidity)
        .unwrap();
    assert_eq!(validity.code, CheckCode::ApproverKeyExpiresBeforeDeadline);
    assert_eq!(
        validity.gating,
        Gating::Advisory,
        "a key that expires mid-run does not invalidate an approval that verified"
    );
    let state = rows
        .iter()
        .find(|r| r.id == CheckId::ApprovalState)
        .unwrap();
    assert_eq!(state.code, CheckCode::ApprovalVerified);
}

// ===========================================================================
// 4. Pod status, attribution and assembly
// ===========================================================================

fn projections() -> Projections {
    Projections {
        connection_secret: Some("kafka-src".to_string()),
        destination_secret: Some("logweir-s3".to_string()),
        signer_secret: Some("logweir-signing-key".to_string()),
        trust_config_maps: vec!["private-ca".to_string()],
    }
}

fn waiting(code: CheckCode, secret: Option<&str>) -> Waiting {
    Waiting {
        code,
        secret: secret.map(str::to_string),
        key: None,
        config_map: None,
        volume: None,
        message: "the kubelet said so".to_string(),
    }
}

/// PLAT-03.1's "missing Secret/key": the SAME code lands on a different row
/// depending on WHICH Secret the kubelet named.
#[test]
fn a_missing_secret_is_attributed_by_name_and_never_by_position() {
    let (rows, blocked) = pod_outcomes(
        PreflightOperation::Backup,
        Some(&waiting(
            CheckCode::CredentialSecretNotFound,
            Some("kafka-src"),
        )),
        &projections(),
        false,
        None,
        now(),
    );
    assert!(
        blocked,
        "a pod that did not start blocks every Job-sourced row"
    );
    let named = rows
        .iter()
        .find(|r| r.code == CheckCode::CredentialSecretNotFound)
        .expect("the code is reported");
    assert_eq!(named.id, CheckId::ConnectionCredentialProjected);

    let (rows, _) = pod_outcomes(
        PreflightOperation::Backup,
        Some(&waiting(
            CheckCode::CredentialSecretNotFound,
            Some("logweir-s3"),
        )),
        &projections(),
        false,
        None,
        now(),
    );
    let named = rows
        .iter()
        .find(|r| r.code == CheckCode::CredentialSecretNotFound)
        .expect("the code is reported");
    assert_eq!(named.id, CheckId::DestinationCredentialProjected);
}

#[test]
fn a_restore_attributes_the_connection_secret_to_the_target_row() {
    let (rows, _) = pod_outcomes(
        PreflightOperation::Restore,
        Some(&waiting(
            CheckCode::CredentialSecretKeyMissing,
            Some("kafka-src"),
        )),
        &projections(),
        false,
        None,
        now(),
    );
    let named = rows
        .iter()
        .find(|r| r.code == CheckCode::CredentialSecretKeyMissing)
        .expect("the code is reported");
    assert_eq!(
        named.id,
        CheckId::TargetCredentialProjected,
        "a restore's catalogue has no `connection.credentialProjected` row to put it on"
    );
}

/// D2 §4.3: "An unmatched code blocks the whole pod … and the named cause is
/// reported."
#[test]
fn a_secret_this_plan_did_not_project_is_reported_and_attributed_to_nothing() {
    let (rows, blocked) = pod_outcomes(
        PreflightOperation::Backup,
        Some(&waiting(
            CheckCode::CredentialSecretNotFound,
            Some("somebody-elses-secret"),
        )),
        &projections(),
        false,
        None,
        now(),
    );
    assert!(blocked);
    let carrier = rows
        .iter()
        .find(|r| r.code == CheckCode::CredentialSecretNotFound)
        .expect("the cause is still named");
    assert_eq!(
        carrier.id,
        CheckId::RunnerPod,
        "attributing it to a credential row would send an operator to rotate the wrong \
         credential"
    );
    for row in rows.iter().filter(|r| r.id != CheckId::RunnerPod) {
        assert_eq!(row.code, CheckCode::PodNotStarted);
    }
}

#[test]
fn an_image_that_will_never_pull_lands_on_the_image_row() {
    let (rows, _) = pod_outcomes(
        PreflightOperation::Backup,
        Some(&waiting(CheckCode::RunnerImageNotPresent, None)),
        &projections(),
        false,
        None,
        now(),
    );
    let row = rows.iter().find(|r| r.id == CheckId::RunnerImage).unwrap();
    assert_eq!(
        (row.state, row.code),
        (CheckState::NotReady, CheckCode::RunnerImageNotPresent)
    );
    assert!(
        row.remedy.contains("architecture"),
        "the remedy names the architecture and the pull policy, per D2 §4.3"
    );
}

#[test]
fn a_verified_relay_is_the_proof_that_every_projection_worked() {
    let (rows, blocked) = pod_outcomes(
        PreflightOperation::Backup,
        None,
        &projections(),
        true,
        Some("sha256:abcdef"),
        now(),
    );
    assert!(!blocked);
    assert!(rows.iter().all(|r| r.state == CheckState::Ready));
    let image = rows.iter().find(|r| r.id == CheckId::RunnerImage).unwrap();
    assert_eq!(
        image.facts.get("imageID").map(String::as_str),
        Some("sha256:abcdef")
    );
}

#[test]
fn a_destination_access_check_reports_no_connection_credential_row() {
    let ids: BTreeSet<CheckId> = pf::pod_rows(PreflightOperation::DestinationAccess)
        .iter()
        .map(|r| r.id)
        .collect();
    assert!(!ids.contains(&CheckId::ConnectionCredentialProjected));
    assert!(!ids.contains(&CheckId::TargetCredentialProjected));
    assert!(ids.contains(&CheckId::DestinationCredentialProjected));
}

/// PLAT-03.1's "timeout": the deadline is the Job's, the dependents are
/// `BlockedByPrerequisite`, and NOTHING is reported as ready.
#[test]
fn a_blocked_pod_makes_every_job_sourced_row_unknown() {
    let (pod, blocked) = pod_outcomes(
        PreflightOperation::Backup,
        Some(&waiting(CheckCode::DeadlineExceeded, None)),
        &projections(),
        false,
        None,
        now(),
    );
    let checks = assemble(
        PreflightOperation::Backup,
        pod,
        vec![runner_row(
            CheckId::ConnectionAuthenticated,
            CheckState::Ready,
            CheckCode::Authenticated,
            Gating::Blocking,
        )],
        blocked,
        &BTreeSet::new(),
        now(),
    );
    for id in job_rows(PreflightOperation::Backup) {
        let row = checks
            .iter()
            .find(|c| c.id == id)
            .unwrap_or_else(|| panic!("{id} is never simply absent"));
        assert_eq!(
            (row.state, row.code),
            (CheckState::Unknown, CheckCode::BlockedByPrerequisite),
            "{id} must not carry a relayed answer for a pod that was blocked"
        );
    }
    let pod_row = checks.iter().find(|c| c.id == CheckId::RunnerPod).unwrap();
    assert_eq!(
        (pod_row.state, pod_row.code),
        (CheckState::NotReady, CheckCode::DeadlineExceeded),
        "the deadline belongs to no specific prerequisite, so it is reported on `runner.pod`"
    );
    assert_ne!(
        logweir_core::check_contract::aggregate(&checks),
        OverallState::Ready,
        "nothing may read as ready for a pod that ran out of time"
    );
}

#[test]
fn a_row_nobody_answered_is_blocking_and_never_absent() {
    let checks = assemble(
        PreflightOperation::Restore,
        Vec::new(),
        Vec::new(),
        true,
        &BTreeSet::new(),
        now(),
    );
    for id in job_rows(PreflightOperation::Restore) {
        let row = checks.iter().find(|c| c.id == id).unwrap();
        assert_eq!(row.gating, Gating::Blocking);
    }
    assert_eq!(
        logweir_core::check_contract::aggregate(&checks),
        OverallState::Unknown,
        "an advisory placeholder would let a verdict be `ready` for a check that never ran"
    );
}

#[test]
fn a_skipped_row_is_present_skipped_and_keeps_the_verdict_unknown() {
    let skip: BTreeSet<CheckId> = [CheckId::ArchiveSegments].into_iter().collect();
    let relayed: Vec<CheckOutcome> = job_rows(PreflightOperation::Restore)
        .into_iter()
        .filter(|id| !skip.contains(id))
        .map(|id| {
            runner_row(
                id,
                CheckState::Ready,
                CheckCode::Succeeded,
                Gating::Blocking,
            )
        })
        .collect();
    let checks = assemble(
        PreflightOperation::Restore,
        Vec::new(),
        relayed,
        false,
        &skip,
        now(),
    );
    let row = checks
        .iter()
        .find(|c| c.id == CheckId::ArchiveSegments)
        .expect("a skipped row is rendered by the controller that chose the skip");
    assert_eq!(row.state, CheckState::Skipped);
    assert_eq!(
        logweir_core::check_contract::aggregate(&checks),
        OverallState::Unknown,
        "skipping a question is not answering it (D2 §6.2)"
    );
}

#[test]
fn the_controller_answers_a_row_the_runner_also_reported_exactly_once() {
    let controller = vec![plan_parse_row(
        &plan_facts("restore-", "s3-bucket", None),
        now(),
    )];
    let relayed = vec![runner_row(
        CheckId::PlanParse,
        CheckState::NotReady,
        CheckCode::PlanUnparseable,
        Gating::Blocking,
    )];
    let checks = assemble(
        PreflightOperation::Restore,
        controller,
        relayed,
        false,
        &BTreeSet::new(),
        now(),
    );
    let rows: Vec<&CheckOutcome> = checks
        .iter()
        .filter(|c| c.id == CheckId::PlanParse)
        .collect();
    assert_eq!(
        rows.len(),
        1,
        "two rows with one id is two answers to one question"
    );
    assert_eq!(
        rows[0].authority,
        Authority::Controller,
        "D2 §6.3's legend makes `plan.parse` a C row; the controller holds the claimed hash"
    );
}

// ===========================================================================
// 5. The verdict, the entries and staleness
// ===========================================================================

#[test]
fn the_aggregate_expiry_is_the_soonest_row_and_bytes_rows_do_not_pull_it() {
    let mut soon = runner_row(
        CheckId::TargetMappedTopics,
        CheckState::Ready,
        CheckCode::MappedTopicsAbsent,
        Gating::Blocking,
    );
    soon.expires_at = Some(now() + Duration::minutes(5));
    let bytes = plan_parse_row(&plan_facts("restore-", "s3-bucket", None), now());
    assert_eq!(bytes.expires_at, None);
    let result = result_for(&[soon, bytes], None);
    assert_eq!(result.expires_at, Some(now() + Duration::minutes(5)));
}

#[test]
fn a_facts_bearing_row_keeps_its_facts_in_the_message() {
    let row = signer_rostered_row(
        PreflightOperation::Backup,
        &approver_roster(None),
        Some("runner-key-1"),
        now(),
    );
    let entry = entry_of(&row);
    assert!(
        entry
            .message
            .as_deref()
            .unwrap_or_default()
            .contains("signerKeyId=runner-key-1"),
        "the shipped CRD carries neither `facts` nor `detail`; dropping them silently would lose \
         the one non-secret fact an operator most often needs"
    );
    assert_eq!(entry.authority.as_deref(), Some("controller"));
    assert_eq!(entry.gating.as_deref(), Some("blocking"));
    assert_eq!(entry.state, "ready");
    assert!(entry.observed_at.is_some());
}

#[test]
fn an_empty_check_set_is_unknown_and_never_ready() {
    assert_eq!(result_for(&[], None).state, "unknown");
}

/// PLAT-03.2's "plan edits": editing the plan changes the hash, and the stored
/// verdict stops applying.
#[test]
fn a_changed_plan_hash_makes_a_recorded_verdict_stale() {
    let recorded = pf::binding_status(&binding_inputs("sha256:aa", CLUSTER_UID, 2));
    let current = binding_inputs("sha256:bb", CLUSTER_UID, 2);
    let stale = stale_against_status(
        &recorded,
        Some(now() + Duration::minutes(5)),
        &current,
        now(),
    );
    assert!(stale.contains(&StaleReason::PlanHashChanged), "{stale:?}");
}

/// A destination edited underneath a green preview changes its generation.
#[test]
fn a_moved_referent_is_named_in_the_stale_reasons() {
    let recorded = pf::binding_status(&binding_inputs("sha256:aa", CLUSTER_UID, 2));
    let current = binding_inputs("sha256:aa", CLUSTER_UID, 3);
    let stale = stale_against_status(
        &recorded,
        Some(now() + Duration::minutes(5)),
        &current,
        now(),
    );
    assert!(
        stale.contains(&StaleReason::ReferentChanged(
            "KafkaCluster/source".to_string()
        )),
        "{stale:?}"
    );
    // A recreated object — same name, new UID — is the other half of the rule.
    let recreated = binding_inputs("sha256:aa", "a-new-uid", 2);
    let stale = stale_against_status(
        &recorded,
        Some(now() + Duration::minutes(5)),
        &recreated,
        now(),
    );
    assert!(stale
        .iter()
        .any(|r| matches!(r, StaleReason::ReferentChanged(_))));
}

#[test]
fn an_expired_verdict_is_stale_with_no_api_call_at_all() {
    let recorded = pf::binding_status(&binding_inputs("sha256:aa", CLUSTER_UID, 2));
    let current = binding_inputs("sha256:aa", CLUSTER_UID, 2);
    assert_eq!(
        stale_against_status(
            &recorded,
            Some(now() - Duration::seconds(1)),
            &current,
            now()
        ),
        vec![StaleReason::Expired]
    );
    assert!(
        stale_against_status(&recorded, None, &current, now()).contains(&StaleReason::Expired),
        "a verdict with no expiry at all is not a verdict that never expires"
    );
    assert!(stale_against_status(
        &recorded,
        Some(now() + Duration::minutes(1)),
        &current,
        now()
    )
    .is_empty());
}

#[test]
fn a_stale_ready_verdict_is_downgraded_to_unknown_and_never_to_not_ready() {
    let ready = weirkeeper::crds::preflight::PreflightResult {
        state: "ready".to_string(),
        expires_at: Some(now()),
        checks: None,
        details_ref: None,
    };
    let downgraded = pf::downgrade(&ready, &[StaleReason::Expired]).expect("it is downgraded");
    assert_eq!(
        downgraded.state, "unknown",
        "nothing was found wrong; the answer simply stopped being about the current objects"
    );
    assert!(
        pf::downgrade(&ready, &[]).is_none(),
        "a verdict that still applies is not rewritten"
    );
    let not_ready = weirkeeper::crds::preflight::PreflightResult {
        state: "notReady".to_string(),
        ..ready
    };
    assert!(
        pf::downgrade(&not_ready, &[StaleReason::Expired]).is_none(),
        "notReady does not get better by going stale"
    );
}

fn binding_inputs(
    plan_hash: &str,
    cluster_uid: &str,
    generation: i64,
) -> logweir_core::check_contract::BindingInputs {
    logweir_core::check_contract::BindingInputs {
        operation: logweir_core::check_contract::CheckOperation::Restore,
        plan_hash: Some(plan_hash.to_string()),
        topics: None,
        referents: vec![Referent {
            kind: "KafkaCluster".to_string(),
            namespace: NS.to_string(),
            name: "source".to_string(),
            uid: cluster_uid.to_string(),
            generation: Some(generation),
        }],
        ca_bundles: Vec::new(),
        roster: RosterRef {
            uid: ROSTER_UID.to_string(),
            generation: 4,
        },
        approval: None,
        policy_digest: "sha256:policy".to_string(),
    }
}

// ===========================================================================
// 6. The reconciler, over a double that panics on an unrecorded route
// ===========================================================================

const PLAN_DIGEST: &str = "sha256:1111111111111111111111111111111111111111111111111111111111111111";

fn route(method: &'static str, path_suffix: &'static str, body: String) -> Route {
    Route {
        method,
        path_suffix,
        status: 200,
        body,
    }
}

/// The body a write must answer with.
///
/// `kube` DESERIALISES the response of a `patch`/`create` into the typed object,
/// so `{}` is a deserialisation error rather than a silent success — which is
/// itself a useful property of the double: a route that answers nonsense fails
/// loudly instead of being mistaken for a working write.
fn echo(kind: &str, name: &str) -> String {
    json!({
        "apiVersion": if kind == "Job" { "batch/v1" } else if kind == "ConfigMap" { "v1" } else { "logweir.dev/v1alpha1" },
        "kind": kind,
        "metadata": {"name": name, "namespace": NS, "uid": PF_UID, "resourceVersion": "101"},
        "spec": if kind == "Preflight" { backup_request_spec() } else { json!({}) }
    })
    .to_string()
}

fn backup_request_spec() -> Value {
    json!({"request": backup_request()})
}

fn not_found(method: &'static str, path_suffix: &'static str) -> Route {
    Route {
        method,
        path_suffix,
        status: 404,
        body: json!({
            "kind": "Status", "apiVersion": "v1", "status": "Failure",
            "reason": "NotFound", "code": 404
        })
        .to_string(),
    }
}

/// The routes every backup-readiness pass needs before it looks at a Job.
fn referent_routes(cluster_id: Option<&str>, allowed: Vec<&str>) -> Vec<Route> {
    vec![
        route(
            "GET",
            "/trustrosters/default",
            roster("runner-key-1", allowed).to_string(),
        ),
        route(
            "GET",
            "/kafkaclusters/source",
            kafka_cluster("source", cluster_id).to_string(),
        ),
        route(
            "GET",
            "/backupdestinations/primary",
            backup_destination("primary").to_string(),
        ),
    ]
}

/// The routes an OBSERVING pass needs: the owned pod, its events, the mounted
/// plan digest and the log.
fn observation_routes(job: &'static str, pod: Value, log: String) -> Vec<Route> {
    vec![
        route("GET", "/pods", list_of(vec![pod])),
        route("GET", "/events", list_of(vec![])),
        route(
            "GET",
            "-plan",
            plan_config_map(job, PLAN_DIGEST).to_string(),
        ),
        route("GET", "/log", log),
        route("PATCH", "/pf-1/status", echo("Preflight", "pf-1")),
        route("PATCH", job, echo("Job", job)),
    ]
}

/// The last `/status` PATCH body, parsed.
fn last_status(bodies: &weirkeeper::testing::BodyRecorder) -> Value {
    let guard = bodies.lock().expect("the body recorder");
    let body = guard
        .iter()
        .filter(|b| b.method == "PATCH" && b.uri.contains("/preflights/pf-1/status"))
        .next_back()
        .expect("the reconciler wrote a status")
        .body
        .clone();
    serde_json::from_str::<Value>(&body).expect("the status patch is JSON")["status"].clone()
}

fn check_entry<'a>(status: &'a Value, id: &str) -> &'a Value {
    status["result"]["checks"]
        .as_array()
        .unwrap_or_else(|| panic!("the verdict carries no checks: {status}"))
        .iter()
        .find(|c| c["id"] == id)
        .unwrap_or_else(|| panic!("no `{id}` row in {status}"))
}

async fn reconcile_with(
    pf_object: &Preflight,
    routes: Vec<Route>,
) -> (Value, weirkeeper::testing::Recorder) {
    let (client, recorder, bodies) = mock_client_recording_bodies(routes);
    let ctx = context(client);
    let cache = weirkeeper::check::policy::PolicyCache::new();
    pf::reconcile_preflight(pf_object, &ctx, &cache, now())
        .await
        .expect("the reconcile completes");
    (last_status(&bodies), recorder)
}

/// PLAT-03.1: the whole happy path, and the shape every other case is a
/// difference from.
#[tokio::test]
async fn a_relayed_backup_readiness_publishes_a_verdict_with_codes_and_scopes() {
    let job = job_name(CheckPlanKind::OperationReadiness);
    // The two FACT-BEARING rows are the ones the J+C verdicts need: the
    // controller cannot read the signing Secret or dial the broker, so
    // `signer.rostered` and `connection.clusterIdentity` are answered from what
    // the pod reported and from the objects only the controller can see.
    let relayed: Vec<CheckOutcome> = job_rows(PreflightOperation::Backup)
        .into_iter()
        .map(|id| {
            let row = runner_row(
                id,
                CheckState::Ready,
                CheckCode::Succeeded,
                Gating::Blocking,
            );
            match id {
                CheckId::SignerPrivateKeyUsable => row.with_fact("signerKeyId", "runner-key-1"),
                CheckId::ConnectionAuthenticated => row.with_fact("clusterId", "prod-id"),
                _ => row,
            }
        })
        .collect();
    let log = relay_log(PLAN_DIGEST, relayed, None);

    let mut routes = referent_routes(Some("prod-id"), vec![]);
    routes.push(route(
        "GET",
        leak(job.clone()),
        finished_job(&job).to_string(),
    ));
    routes.extend(observation_routes(
        leak(job.clone()),
        owned_pod(&job, terminated(0)),
        log,
    ));
    let (status, _) = reconcile_with(&preflight(backup_request()), routes).await;

    assert_eq!(status["phase"], "Completed");
    let not_ready: Vec<&Value> = status["result"]["checks"]
        .as_array()
        .expect("checks")
        .iter()
        .filter(|c| c["state"] != "ready")
        .collect();
    assert_eq!(
        status["result"]["state"], "ready",
        "rows that are not ready: {not_ready:#?}"
    );
    assert_eq!(
        check_entry(&status, "connection.resolved")["state"],
        "ready"
    );
    assert_eq!(
        check_entry(&status, "connection.clusterIdentity")["code"],
        "ClusterIdentityMatches"
    );
    assert_eq!(
        check_entry(&status, "signer.rostered")["code"],
        "SignerRostered"
    );
    assert_eq!(
        check_entry(&status, "runner.image")["code"],
        "ImageAvailable"
    );
    assert_eq!(
        check_entry(&status, "configuration.egress")["state"],
        "unknown",
        "an execution-only row is `unknown` forever and says so"
    );
    assert!(
        status["binding"]["inputsDigest"].as_str().is_some(),
        "a verdict that cannot say what it was about is worthless"
    );
    assert!(status["result"]["expiresAt"].as_str().is_some());
}

/// PLAT-03.1's "missing Secret/key", end to end: the kubelet's message, the
/// attributed row, the early cancel, and no relay read.
#[tokio::test]
async fn a_missing_credential_secret_cancels_the_job_and_names_the_row() {
    let job = job_name(CheckPlanKind::OperationReadiness);
    let pod = owned_pod(
        &job,
        json!({"waiting": {
            "reason": "CreateContainerConfigError",
            "message": "secret \"kafka-src\" not found"
        }}),
    );
    let mut routes = referent_routes(None, vec![]);
    routes.push(route(
        "GET",
        leak(job.clone()),
        running_job(&job).to_string(),
    ));
    routes.push(route("GET", "/pods", list_of(vec![pod])));
    routes.push(route("GET", "/events", list_of(vec![])));
    routes.push(route(
        "GET",
        "-plan",
        plan_config_map(&job, PLAN_DIGEST).to_string(),
    ));
    routes.push(route("PATCH", leak(job.clone()), echo("Job", &job)));
    routes.push(route("PATCH", "/pf-1/status", echo("Preflight", "pf-1")));
    let (client, recorder, bodies) = mock_client_recording_bodies(routes);
    let ctx = context(client);
    let cache = weirkeeper::check::policy::PolicyCache::new();
    pf::reconcile_preflight(&preflight(backup_request()), &ctx, &cache, now())
        .await
        .expect("the reconcile completes");

    let status = last_status(&bodies);
    assert_eq!(status["phase"], "Completed");
    assert_eq!(status["result"]["state"], "notReady");
    let row = check_entry(&status, "connection.credentialProjected");
    assert_eq!(row["code"], "CredentialSecretNotFound");
    assert!(
        row["remedy"]
            .as_str()
            .unwrap_or_default()
            .contains("Secret"),
        "the tracker's acceptance is that the UI names each failed prerequisite AND ITS REMEDY"
    );
    assert_eq!(
        check_entry(&status, "connection.authenticated")["code"],
        "BlockedByPrerequisite",
        "a pod that never started answered nothing, and nothing may read as ready"
    );

    let seen = recorder.lock().expect("recorder");
    assert!(
        seen.iter()
            .any(|r| r.method == "PATCH" && r.uri.contains(&job)),
        "a terminal waiting state is cancelled NOW rather than at activeDeadlineSeconds; the \
         user is otherwise watching a spinner for a Secret that does not exist"
    );
    assert!(
        !seen.iter().any(is_pod_log),
        "no relay is read from a pod that never started its runner container"
    );
}

/// PLAT-03.1's "wrong credentials" and "storage denial": the runner's own codes
/// reach the verdict verbatim.
#[tokio::test]
async fn relayed_authentication_and_storage_refusals_are_blocking_not_ready() {
    let job = job_name(CheckPlanKind::OperationReadiness);
    let relayed = vec![
        runner_row(
            CheckId::ConnectionAuthenticated,
            CheckState::NotReady,
            CheckCode::AuthenticationFailed,
            Gating::Blocking,
        ),
        runner_row(
            CheckId::DestinationArchiveListable,
            CheckState::NotReady,
            CheckCode::AccessDenied,
            Gating::Blocking,
        ),
    ];
    let log = relay_log(PLAN_DIGEST, relayed, None);
    let mut routes = referent_routes(None, vec![]);
    routes.push(route(
        "GET",
        leak(job.clone()),
        finished_job(&job).to_string(),
    ));
    routes.extend(observation_routes(
        leak(job.clone()),
        owned_pod(&job, terminated(0)),
        log,
    ));
    let (status, _) = reconcile_with(&preflight(backup_request()), routes).await;

    assert_eq!(status["phase"], "Completed");
    assert_eq!(status["result"]["state"], "notReady");
    assert_eq!(
        check_entry(&status, "connection.authenticated")["code"],
        "AuthenticationFailed"
    );
    assert_eq!(
        check_entry(&status, "destination.archiveListable")["code"],
        "AccessDenied"
    );
    assert_eq!(
        check_entry(&status, "destination.credentialProjected")["code"],
        "Projected",
        "the credential PROJECTED; it is the credential that did not WORK, and the two are \
         different findings with different remedies"
    );
}

/// PLAT-03.1's "timeout".
#[tokio::test]
async fn a_job_that_reached_its_deadline_reports_the_deadline_and_blocks_the_rest() {
    let job = job_name(CheckPlanKind::OperationReadiness);
    let deadline_job = json!({
        "apiVersion": "batch/v1", "kind": "Job",
        "metadata": {
            "name": job, "namespace": NS, "uid": JOB_UID,
            "ownerReferences": [owner_reference()]
        },
        "status": {"conditions": [{
            "type": "Failed", "status": "True", "reason": "DeadlineExceeded"
        }]}
    });
    let mut routes = referent_routes(None, vec![]);
    routes.push(route("GET", leak(job.clone()), deadline_job.to_string()));
    routes.push(route(
        "GET",
        "/pods",
        list_of(vec![owned_pod(&job, terminated(137))]),
    ));
    routes.push(route("GET", "/events", list_of(vec![])));
    routes.push(route(
        "GET",
        "-plan",
        plan_config_map(&job, PLAN_DIGEST).to_string(),
    ));
    routes.push(route("GET", "/log", String::new()));
    routes.push(route("PATCH", "/pf-1/status", echo("Preflight", "pf-1")));
    routes.push(route("PATCH", leak(job.clone()), echo("Job", &job)));
    let (status, _) = reconcile_with(&preflight(backup_request()), routes).await;

    assert_eq!(status["reason"], "DeadlineExceeded");
    assert_eq!(
        status["phase"], "Failed",
        "no result could be produced (D2 §6.2)"
    );
    for id in ["connection.authenticated", "destination.archiveListable"] {
        assert_eq!(check_entry(&status, id)["code"], "BlockedByPrerequisite");
    }
}

/// D-SEAMS **S6** / defect SEC-PODLOG: a pod that carries the Job's name label
/// but is not owned by it is never read.
#[tokio::test]
async fn an_impostor_pod_is_ignored_and_its_log_is_never_read() {
    let job = job_name(CheckPlanKind::OperationReadiness);
    let impostor = json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {
            "name": "impostor", "namespace": NS, "uid": "impostor-uid",
            "labels": {"batch.kubernetes.io/job-name": job},
            "ownerReferences": [{
                "apiVersion": "batch/v1", "kind": "Job", "name": job,
                "uid": "a-different-job-uid", "controller": true
            }]
        },
        "status": {"containerStatuses": [{
            "name": "runner", "image": "x", "imageID": "sha256:evil",
            "ready": false, "restartCount": 0, "state": terminated(0)
        }]}
    });
    let mut routes = referent_routes(None, vec![]);
    routes.push(route(
        "GET",
        leak(job.clone()),
        finished_job(&job).to_string(),
    ));
    routes.push(route("GET", "/pods", list_of(vec![impostor])));
    routes.push(route("GET", "/events", list_of(vec![])));
    routes.push(route(
        "GET",
        "-plan",
        plan_config_map(&job, PLAN_DIGEST).to_string(),
    ));
    routes.push(route("PATCH", "/pf-1/status", echo("Preflight", "pf-1")));
    routes.push(route("PATCH", leak(job.clone()), echo("Job", &job)));
    let (client, recorder, bodies) = mock_client_recording_bodies(routes);
    let ctx = context(client);
    let cache = weirkeeper::check::policy::PolicyCache::new();
    pf::reconcile_preflight(&preflight(backup_request()), &ctx, &cache, now())
        .await
        .expect("the reconcile completes");

    let seen = recorder.lock().expect("recorder");
    assert!(
        !seen.iter().any(is_pod_log),
        "a label is writable by anything that can create a pod; ownership is the \
         `ownerReferences` UID and nothing else (D-SEAMS S6)"
    );
    let status = last_status(&bodies);
    assert_eq!(status["reason"], "ResultUnreadable");
    let serialised = status.to_string();
    assert!(
        !serialised.contains("sha256:evil"),
        "nothing from the impostor reaches the status"
    );
}

/// D2 §4.3's plan `ConfigMap` 409 rule: a same-named object owned by somebody
/// else is a TERMINAL refusal and is never adopted.
#[tokio::test]
async fn a_foreign_owner_plan_config_map_is_never_adopted() {
    let job = job_name(CheckPlanKind::OperationReadiness);
    let foreign = json!({
        "apiVersion": "v1", "kind": "ConfigMap",
        "metadata": {
            "name": format!("{job}-plan"), "namespace": NS,
            "annotations": {"logweir.dev/check-plan-sha256": PLAN_DIGEST},
            "ownerReferences": [{
                "apiVersion": "logweir.dev/v1alpha1", "kind": "Preflight",
                "name": "somebody-else", "uid": "another-uid", "controller": true
            }]
        },
        "immutable": true, "data": {"check-plan.json": "{}"}
    });
    let mut routes = referent_routes(None, vec![]);
    routes.push(not_found("GET", leak(job.clone())));
    routes.push(route("GET", "/apis/batch/v1/jobs", list_of(vec![])));
    routes.push(route("PATCH", "/pf-1/status", echo("Preflight", "pf-1")));
    routes.push(Route {
        method: "POST",
        path_suffix: "/configmaps",
        status: 409,
        body: json!({
            "kind": "Status", "apiVersion": "v1", "status": "Failure",
            "reason": "AlreadyExists", "code": 409
        })
        .to_string(),
    });
    routes.push(route("GET", "-plan", foreign.to_string()));
    let (client, recorder, bodies) = mock_client_recording_bodies(routes);
    let ctx = context(client);
    let cache = weirkeeper::check::policy::PolicyCache::new();
    pf::reconcile_preflight(&preflight(backup_request()), &ctx, &cache, now())
        .await
        .expect("the reconcile completes");

    let status = last_status(&bodies);
    assert_eq!(status["phase"], "Failed");
    assert_eq!(status["reason"], "CheckPlanConflict");
    let seen = recorder.lock().expect("recorder");
    assert!(
        !seen
            .iter()
            .any(|r| r.method == "POST" && r.uri.contains("/jobs")),
        "running the OTHER plan would execute a check against inputs this pass never rendered"
    );
}

/// D2 §6.6's consequence, from the controller's own side: a verdict whose
/// expiry has passed stops being `ready`, and the downgrade costs no API call.
#[tokio::test]
async fn an_expired_ready_verdict_is_downgraded_without_re_resolving_anything() {
    let mut object = preflight(backup_request());
    object.status = Some(
        serde_json::from_value(json!({
            "phase": "Completed",
            "reason": "Valid",
            "binding": {"inputsDigest": "sha256:aa", "referents": []},
            "result": {
                "state": "ready",
                "expiresAt": (now() - Duration::minutes(1)).to_rfc3339(),
                "checks": []
            }
        }))
        .expect("the status fixture parses"),
    );
    // THE ONLY ROUTE. A revalidation that re-read the cluster would make an
    // expired verdict cost a full resolution pass; the clock is enough.
    let routes = vec![route("PATCH", "/pf-1/status", echo("Preflight", "pf-1"))];
    let (status, recorder) = reconcile_with(&object, routes).await;
    assert_eq!(status["result"]["state"], "unknown");
    assert!(
        status["message"]
            .as_str()
            .unwrap_or_default()
            .contains("expired"),
        "a stale flag with no reason is the defect PLAT-03.2 is about"
    );
    assert_eq!(recorder.lock().expect("recorder").len(), 1);
}

/// PLAT-03.2's "new target conflict after a green preview", from the binding's
/// side: the destination is edited, its generation moves, and the stored
/// verdict stops being `ready`.
#[tokio::test]
async fn a_ready_verdict_is_downgraded_when_a_referent_moves_under_it() {
    let recorded = pf::binding_status(&logweir_core::check_contract::BindingInputs {
        operation: logweir_core::check_contract::CheckOperation::Backup,
        plan_hash: None,
        topics: Some(vec!["orders".to_string()]),
        referents: vec![Referent {
            kind: "BackupDestination".to_string(),
            namespace: NS.to_string(),
            name: "primary".to_string(),
            uid: DEST_UID.to_string(),
            generation: Some(2),
        }],
        ca_bundles: Vec::new(),
        roster: RosterRef {
            uid: ROSTER_UID.to_string(),
            generation: 4,
        },
        approval: None,
        policy_digest: String::new(),
    });
    let mut object = preflight(backup_request());
    object.status = Some(
        serde_json::from_value(json!({
            "phase": "Completed",
            "reason": "Valid",
            "binding": serde_json::to_value(&recorded).unwrap(),
            "result": {
                "state": "ready",
                "expiresAt": (now() + Duration::minutes(10)).to_rfc3339(),
                "checks": []
            }
        }))
        .expect("the status fixture parses"),
    );
    let mut routes = referent_routes(None, vec![]);
    routes.push(route("PATCH", "/pf-1/status", echo("Preflight", "pf-1")));
    let (status, _) = reconcile_with(&object, routes).await;
    assert_eq!(status["result"]["state"], "unknown");
    let message = status["message"].as_str().unwrap_or_default();
    assert!(
        message.contains("referentChanged:BackupDestination/primary"),
        "the reason names the object that moved; got {message}"
    );
}

#[tokio::test]
async fn a_cancel_request_collapses_the_job_deadline_and_nothing_else() {
    let job = job_name(CheckPlanKind::OperationReadiness);
    let mut object = preflight(backup_request());
    object.spec.cancel_requested = true;
    let routes = vec![
        route("GET", leak(job.clone()), running_job(&job).to_string()),
        route("PATCH", leak(job.clone()), echo("Job", &job)),
        route("PATCH", "/pf-1/status", echo("Preflight", "pf-1")),
    ];
    let (client, recorder, bodies) = mock_client_recording_bodies(routes);
    let ctx = context(client);
    let cache = weirkeeper::check::policy::PolicyCache::new();
    pf::reconcile_preflight(&object, &ctx, &cache, now())
        .await
        .expect("the reconcile completes");
    let status = last_status(&bodies);
    assert_eq!(status["phase"], "Cancelled");
    assert_eq!(status["reason"], "CancelRequested");
    let seen = recorder.lock().expect("recorder");
    assert!(
        !seen.iter().any(|r| r.method == "DELETE"),
        "the weirkeeper ClusterRole grants `delete` on nothing; a cancel is a collapsed deadline"
    );
    let bodies = bodies.lock().expect("bodies");
    let patch = bodies
        .iter()
        .find(|b| b.method == "PATCH" && b.uri.contains(&job))
        .expect("the Job was patched");
    assert!(patch.body.contains("\"activeDeadlineSeconds\":1"));
}

/// Whether a recorded request is a `pods/log` read.
///
/// `uri.contains("/log")` is NOT this predicate: every logweir.dev path carries
/// `/logweir.dev/`, so the naive form was true of the status patch and the
/// assertions it guarded could never fail.
fn is_pod_log(request: &weirkeeper::testing::SeenRequest) -> bool {
    request.uri.contains("/pods/") && request.uri.contains("/log")
}

/// Leak a `String` so a [`Route`]'s `&'static str` suffix can be computed.
fn leak(s: String) -> &'static str {
    Box::leak(s.into_boxed_str())
}

// ===========================================================================
// 7. The restore preflight (PLAT-03.2)
// ===========================================================================

fn restore_referent_routes() -> Vec<Route> {
    vec![
        route(
            "GET",
            "/trustrosters/default",
            roster("runner-key-1", vec!["target-id"]).to_string(),
        ),
        route(
            "GET",
            "/kafkaclusters/target",
            kafka_cluster("target", Some("target-id")).to_string(),
        ),
        route(
            "GET",
            "/backupdestinations/primary",
            backup_destination("primary").to_string(),
        ),
        route(
            "GET",
            "/backupdestinations/evidence",
            backup_destination("evidence").to_string(),
        ),
    ]
}

/// PLAT-03.2's "missing segment": the bounded sample reaches the verdict and
/// the FULL list reaches one immutable, owned `ConfigMap`.
#[tokio::test]
async fn a_missing_segment_publishes_a_sample_and_writes_the_details_config_map() {
    let job = job_name(CheckPlanKind::RestorePreflight);
    let details = "{\"check\":\"archive.segments\",\"missingSegment\":\"bk-1/orders/0/000.log\"}";
    let relayed = vec![runner_row(
        CheckId::ArchiveSegments,
        CheckState::NotReady,
        CheckCode::SegmentMissing,
        Gating::Blocking,
    )
    .with_detail(json!({"count": 1, "sample": ["bk-1/orders/0/000.log"]}))];
    let log = relay_log(PLAN_DIGEST, relayed, Some(details));

    let mut routes = restore_referent_routes();
    routes.push(route(
        "GET",
        leak(job.clone()),
        finished_job(&job).to_string(),
    ));
    routes.push(route(
        "GET",
        "/pods",
        list_of(vec![owned_pod(&job, terminated(0))]),
    ));
    routes.push(route("GET", "/events", list_of(vec![])));
    routes.push(route(
        "GET",
        "-plan",
        plan_config_map(&job, PLAN_DIGEST).to_string(),
    ));
    routes.push(route("GET", "/log", log));
    routes.push(route("POST", "/configmaps", echo("ConfigMap", "details")));
    routes.push(route("PATCH", "/pf-1/status", echo("Preflight", "pf-1")));
    routes.push(route("PATCH", leak(job.clone()), echo("Job", &job)));

    let (client, _recorder, bodies) = mock_client_recording_bodies(routes);
    let ctx = context(client);
    let cache = weirkeeper::check::policy::PolicyCache::new();
    pf::reconcile_preflight(&preflight(restore_request(json!({}))), &ctx, &cache, now())
        .await
        .expect("the reconcile completes");

    let status = last_status(&bodies);
    assert_eq!(status["result"]["state"], "notReady");
    let row = check_entry(&status, "archive.segments");
    assert_eq!(row["code"], "SegmentMissing");
    assert!(
        row["message"]
            .as_str()
            .unwrap_or_default()
            .contains("bk-1/orders/0/000.log"),
        "the bounded sample is what an operator acts on"
    );
    assert_eq!(
        status["result"]["detailsRef"]["name"],
        check::chunks::details_name(&job),
        "the full list goes to one immutable owned ConfigMap, never to the status"
    );

    let recorded = bodies.lock().expect("bodies");
    let posted = recorded
        .iter()
        .find(|b| b.method == "POST" && b.uri.contains("/configmaps"))
        .expect("the details ConfigMap was written");
    assert!(posted.body.contains("\"immutable\":true"));
    assert!(
        posted.body.contains(PF_UID),
        "it is owned by the Preflight, so the owner cascade is what deletes it"
    );
}

/// PLAT-03.2's "expired approval": the controller reads `Approval.status` and
/// the `TrustRoster`, and nothing else can answer it.
#[tokio::test]
async fn an_expired_approver_key_is_reported_before_the_restore_is_submitted() {
    let job = job_name(CheckPlanKind::RestorePreflight);
    let plan = plan_yaml("restore-", "s3-bucket");
    let plan_hash = logweir_core::ids::sha256_prefixed(plan.as_bytes());
    let restore_object = json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "Restore",
        "metadata": {"name": "r-1", "namespace": NS, "uid": "restore-uid", "generation": 1},
        "spec": {
            "planBytes": plan,
            "approvalRef": {"name": "ap-1"},
            "sourceArchive": {"url": "logweir-destination://primary"},
            "sourceDestinationRef": {"name": "primary"},
            "evidenceDestinationRef": {"name": "evidence"},
            "backupSetRef": "bk-1",
            "pointInTime": "2026-09-15T00:00:00Z",
            "target": {
                "clusterRef": {"name": "target"}, "mode": "scratch",
                "topicNaming": {"prefix": "restore-"}
            },
            "deadlineSeconds": 3600
        }
    });
    let approval = json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "Approval",
        "metadata": {"name": "ap-1", "namespace": NS, "uid": "ap-uid", "resourceVersion": "9"},
        "spec": {
            "subjectRef": {"kind": "Restore", "name": "r-1"},
            "planHash": plan_hash,
            "approvalBytes": json!({"plan_hash": plan_hash}).to_string(),
            "sidecarBytes": "{}"
        },
        "status": {"verified": true, "matchedKeyId": "approver-1"}
    });
    let expired_roster = json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "TrustRoster",
        "metadata": {"name": "default", "uid": ROSTER_UID, "generation": 4},
        "spec": {
            "approverKeys": [{
                "keyId": "approver-1",
                "spkiPem": "-----BEGIN PUBLIC KEY-----\nAA\n-----END PUBLIC KEY-----",
                "notAfter": "2026-09-16T11:00:00Z"
            }],
            "signingKeys": [{"keyId": "runner-key-1", "spkiPem": "-----BEGIN PUBLIC KEY-----\nBB\n-----END PUBLIC KEY-----"}],
            "allowedClusterIds": ["target-id"]
        }
    });

    let mut routes = vec![
        route("GET", "/trustrosters/default", expired_roster.to_string()),
        route("GET", "/restores/r-1", restore_object.to_string()),
        route("GET", "/approvals/ap-1", approval.to_string()),
        route(
            "GET",
            "/kafkaclusters/target",
            kafka_cluster("target", Some("target-id")).to_string(),
        ),
        route(
            "GET",
            "/backupdestinations/primary",
            backup_destination("primary").to_string(),
        ),
        route(
            "GET",
            "/backupdestinations/evidence",
            backup_destination("evidence").to_string(),
        ),
    ];
    routes.push(route(
        "GET",
        leak(job.clone()),
        finished_job(&job).to_string(),
    ));
    routes.push(route(
        "GET",
        "/pods",
        list_of(vec![owned_pod(&job, terminated(0))]),
    ));
    routes.push(route("GET", "/events", list_of(vec![])));
    routes.push(route(
        "GET",
        "-plan",
        plan_config_map(&job, PLAN_DIGEST).to_string(),
    ));
    routes.push(route("GET", "/log", relay_log(PLAN_DIGEST, vec![], None)));
    routes.push(route("PATCH", "/pf-1/status", echo("Preflight", "pf-1")));
    routes.push(route("PATCH", leak(job.clone()), echo("Job", &job)));

    let request = json!({"operation": "Restore", "restore": {
        "restoreRef": {"name": "r-1"},
        "sourceDestinationRef": {"name": "primary"},
        "evidenceDestinationRef": {"name": "evidence"}
    }, "timeoutSeconds": 120});
    let (status, _) = reconcile_with(&preflight(request), routes).await;

    assert_eq!(status["result"]["state"], "notReady");
    assert_eq!(
        check_entry(&status, "approval.state")["code"],
        "ApprovalExpired"
    );
    assert_eq!(
        check_entry(&status, "plan.parse")["code"],
        "PlanParsed",
        "the plan bytes come from the Restore when the request names one"
    );
    assert!(
        status["binding"]["planHash"].as_str().is_some(),
        "the verdict is bound to the exact plan hash (D2 §6.6)"
    );
}

/// A draft is checked in full, and the one row it cannot answer says so.
#[tokio::test]
async fn a_draft_restore_is_checked_and_its_approval_row_is_skipped() {
    let job = job_name(CheckPlanKind::RestorePreflight);
    let mut routes = restore_referent_routes();
    routes.push(route(
        "GET",
        leak(job.clone()),
        finished_job(&job).to_string(),
    ));
    routes.push(route(
        "GET",
        "/pods",
        list_of(vec![owned_pod(&job, terminated(0))]),
    ));
    routes.push(route("GET", "/events", list_of(vec![])));
    routes.push(route(
        "GET",
        "-plan",
        plan_config_map(&job, PLAN_DIGEST).to_string(),
    ));
    routes.push(route("GET", "/log", relay_log(PLAN_DIGEST, vec![], None)));
    routes.push(route("PATCH", "/pf-1/status", echo("Preflight", "pf-1")));
    routes.push(route("PATCH", leak(job.clone()), echo("Job", &job)));
    let (status, _) = reconcile_with(&preflight(restore_request(json!({}))), routes).await;

    assert_eq!(check_entry(&status, "approval.state")["state"], "skipped");
    assert_eq!(
        check_entry(&status, "approval.state")["code"],
        "SubjectNotCreated"
    );
    assert_eq!(
        check_entry(&status, "plan.names")["code"],
        "MappedNamesLegal"
    );
    assert_eq!(
        check_entry(&status, "plan.bindings")["code"],
        "PlanMatchesReferences"
    );
    assert_eq!(
        status["result"]["state"], "unknown",
        "a skipped blocking check keeps the overall verdict unknown, whatever else passed"
    );
}

/// PLAT-03.2's "denied access", on the restore side: the destination the plan
/// names did not resolve, so NO Job is created at all.
#[tokio::test]
async fn a_destination_that_does_not_resolve_creates_no_job() {
    let denied = json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "BackupDestination",
        "metadata": {"name": "primary", "namespace": NS, "uid": DEST_UID, "generation": 3},
        "spec": {
            "storage": {"provider": "S3", "bucket": "s3-bucket", "addressing": "PathStyle"},
            "transport": {"security": "TLS"},
            "access": {"archiveWrite": {
                "mode": "SecretKeys",
                "secret": {
                    "name": "logweir-s3",
                    "accessKeyIdKey": "AWS_ACCESS_KEY_ID",
                    "secretAccessKeyKey": "AWS_SECRET_ACCESS_KEY"
                }
            }}
        },
        "status": {
            "observedGeneration": 3, "reason": "CaBundleNotFound",
            "conditions": [{
                "type": "Valid", "status": "False", "observedGeneration": 3,
                "reason": "CaBundleNotFound", "message": "the CA ConfigMap does not exist"
            }]
        }
    });
    let mut routes = restore_referent_routes();
    routes.retain(|r| r.path_suffix != "/backupdestinations/primary");
    routes.push(route(
        "GET",
        "/backupdestinations/primary",
        denied.to_string(),
    ));
    routes.push(route("PATCH", "/pf-1/status", echo("Preflight", "pf-1")));

    let (client, recorder, bodies) = mock_client_recording_bodies(routes);
    let ctx = context(client);
    let cache = weirkeeper::check::policy::PolicyCache::new();
    pf::reconcile_preflight(&preflight(restore_request(json!({}))), &ctx, &cache, now())
        .await
        .expect("the reconcile completes");

    let status = last_status(&bodies);
    assert_eq!(status["phase"], "Completed");
    assert_eq!(status["result"]["state"], "notReady");
    assert_eq!(
        check_entry(&status, "destination.resolved")["state"],
        "notReady"
    );
    let seen = recorder.lock().expect("recorder");
    assert!(
        !seen.iter().any(|r| r.method == "POST"),
        "a check plan that cannot be rendered creates NOTHING — no ConfigMap and no Job"
    );
    for id in ["archive.backupSet", "archive.coverage", "archive.segments"] {
        assert_eq!(check_entry(&status, id)["code"], "BlockedByPrerequisite");
    }
}

/// D2 §6.7: **no writes.** The reconciler patches its own `/status` and its own
/// Job's TTL, and touches no target.
#[tokio::test]
async fn a_restore_preflight_writes_to_no_target() {
    let job = job_name(CheckPlanKind::RestorePreflight);
    let mut routes = restore_referent_routes();
    routes.push(route(
        "GET",
        leak(job.clone()),
        finished_job(&job).to_string(),
    ));
    routes.push(route(
        "GET",
        "/pods",
        list_of(vec![owned_pod(&job, terminated(0))]),
    ));
    routes.push(route("GET", "/events", list_of(vec![])));
    routes.push(route(
        "GET",
        "-plan",
        plan_config_map(&job, PLAN_DIGEST).to_string(),
    ));
    routes.push(route("GET", "/log", relay_log(PLAN_DIGEST, vec![], None)));
    routes.push(route("PATCH", "/pf-1/status", echo("Preflight", "pf-1")));
    routes.push(route("PATCH", leak(job.clone()), echo("Job", &job)));

    let (client, recorder, _bodies) = mock_client_recording_bodies(routes);
    let ctx = context(client);
    let cache = weirkeeper::check::policy::PolicyCache::new();
    pf::reconcile_preflight(&preflight(restore_request(json!({}))), &ctx, &cache, now())
        .await
        .expect("the reconcile completes");

    let seen = recorder.lock().expect("recorder");
    for request in seen.iter() {
        let writes = matches!(request.method.as_str(), "POST" | "PUT" | "PATCH" | "DELETE");
        if !writes {
            continue;
        }
        let allowed = request.uri.contains("/preflights/pf-1/status")
            || request.uri.contains(&job)
            || request.uri.ends_with("/configmaps");
        assert!(
            allowed,
            "a restore preflight wrote to {} {}: the only writes it may make are its own \
             status, its own Job's TTL and its own owned ConfigMaps. The validate-only \
             CreateTopics of D2 §6.7 happens INSIDE the check pod, never here.",
            request.method, request.uri
        );
        assert!(
            !request.uri.contains("/restores/") && !request.uri.contains("/kafkaclusters/"),
            "no target or subject is ever patched"
        );
    }
}

// ===========================================================================
// 8. Redaction, and the two source scans D2 §6.8(a) asks for
// ===========================================================================

/// PLAT-03.1's "redaction", as a body scan over EVERY request the reconciler
/// made.
///
/// The runner is the adversary here: the relay carries an access key id, a
/// secret access key, a URL with userinfo and an S3 error body, exactly as a
/// careless message would. Nothing of it may reach a status.
#[tokio::test]
async fn no_status_or_config_map_body_carries_a_credential() {
    let job = job_name(CheckPlanKind::OperationReadiness);
    let leaky = format!(
        "AKIAIOSFODNN7EXAMPLE could not sign: {FIXTURE_SECRET_VALUE} at \
         https://root:{FIXTURE_SECRET_VALUE}@minio.invalid:9000 \
         <Error><Code>SignatureDoesNotMatch</Code></Error>"
    );
    let relayed = vec![
        // `CheckOutcome::with_message` redacts on the WRITE side; the decoder
        // does not use it, which is why `CheckRelay::result` sanitises. Both
        // halves are exercised: this row is built through the constructor and
        // then travels through the frames.
        runner_row(
            CheckId::DestinationArchiveListable,
            CheckState::NotReady,
            CheckCode::InvalidCredentials,
            Gating::Blocking,
        )
        .with_message(&leaky)
        .with_remedy(&leaky)
        .with_fact("clusterId", &leaky),
    ];
    let log = relay_log(PLAN_DIGEST, relayed, Some(&leaky));
    let mut routes = referent_routes(None, vec![]);
    routes.push(route(
        "GET",
        leak(job.clone()),
        finished_job(&job).to_string(),
    ));
    routes.push(route(
        "GET",
        "/pods",
        list_of(vec![owned_pod(&job, terminated(0))]),
    ));
    routes.push(route("GET", "/events", list_of(vec![])));
    routes.push(route(
        "GET",
        "-plan",
        plan_config_map(&job, PLAN_DIGEST).to_string(),
    ));
    routes.push(route("GET", "/log", log));
    routes.push(route("POST", "/configmaps", echo("ConfigMap", "details")));
    routes.push(route("PATCH", "/pf-1/status", echo("Preflight", "pf-1")));
    routes.push(route("PATCH", leak(job.clone()), echo("Job", &job)));

    let (client, _recorder, bodies) = mock_client_recording_bodies(routes);
    let ctx = context(client);
    let cache = weirkeeper::check::policy::PolicyCache::new();
    pf::reconcile_preflight(&preflight(backup_request()), &ctx, &cache, now())
        .await
        .expect("the reconcile completes");

    let recorded = bodies.lock().expect("bodies");
    assert!(!recorded.is_empty());
    for body in recorded.iter() {
        assert!(
            !body.body.contains(FIXTURE_SECRET_VALUE),
            "{} {} carried the fixture secret",
            body.method,
            body.uri
        );
        assert!(
            !body.body.contains("AKIAIOSFODNN7EXAMPLE"),
            "{} {} carried an access key id",
            body.method,
            body.uri
        );
    }
    // The DETAILS ConfigMap is written from the same bytes and gets the same
    // treatment: it is a stream, not a message, so the check is that the
    // secret is gone rather than that the line is.
    assert!(recorded
        .iter()
        .any(|b| b.method == "POST" && b.uri.contains("/configmaps")));
}

/// D2 §6.8(a): **no execution path reads a `Preflight` or a `TopicDiscovery`.**
///
/// A green preview cannot bypass a later collision, and this is the only way to
/// say so about code that does not exist. The planted mutant is an `import` of
/// `Preflight` into `controllers/restore.rs`.
#[test]
fn no_execution_path_reads_preflight_or_discovery() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crates/")
        .parent()
        .expect("the repository root");
    let mut files: Vec<std::path::PathBuf> = [
        "crates/weirkeeper/src/controllers/backup.rs",
        "crates/weirkeeper/src/controllers/backup_schedule.rs",
        "crates/weirkeeper/src/controllers/restore.rs",
        "crates/weirkeeper/src/controllers/approval.rs",
    ]
    .iter()
    .map(|p| root.join(p))
    .collect();
    files.extend(rust_files_under(&root.join("crates/logweir/src/backup")));
    files.extend(rust_files_under(&root.join("crates/logweir/src/drill")));
    assert!(
        files.len() >= 8,
        "the scan found only {} files; it has gone quiet and would pass vacuously",
        files.len()
    );

    const FORBIDDEN: [&str; 4] = [
        "Preflight",
        "TopicDiscovery",
        "preflights",
        "topicdiscoveries",
    ];
    for file in &files {
        let text = std::fs::read_to_string(file)
            .unwrap_or_else(|e| panic!("could not read {}: {e}", file.display()));
        for (n, line) in text.lines().enumerate() {
            // The word may appear in PROSE — an execution path is allowed to
            // explain why it does not consult one — so only code lines count.
            let code = line.split("//").next().unwrap_or("");
            for needle in FORBIDDEN {
                assert!(
                    !names_word(code, needle),
                    "{}:{} names `{needle}` in code: {line}\n\
                     D2 §6.8: the preflight replaces no execution-time guard, and an execution \
                     path that could read one is an execution path a green preview could bypass.",
                    file.display(),
                    n + 1
                );
            }
        }
    }
}

/// D-SEAMS **S2** / PLAT-03.2's "stale inventory": a preflight discovers
/// afresh inside its own check Job and never reads a `TopicDiscovery` result.
#[test]
fn preflight_controller_never_reads_topicdiscovery() {
    let source = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/controllers/preflight.rs"),
    )
    .expect("the controller source is readable");
    for (n, line) in source.lines().enumerate() {
        let code = line.split("//").next().unwrap_or("");
        for needle in ["TopicDiscovery", "topicdiscoveries"] {
            assert!(
                !names_word(code, needle),
                "preflight.rs:{} names `{needle}` in code. D-SEAMS S2: a discovery result is \
                 never an input to anything that runs.",
                n + 1
            );
        }
    }
}

/// The reconciler holds no `Api<Secret>`, and the whole crate's invariant is
/// restated here because this is the first controller that projects THREE
/// different Secrets into a pod it creates.
#[test]
fn the_preflight_controller_reads_no_secret() {
    let source = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/controllers/preflight.rs"),
    )
    .expect("the controller source is readable");
    assert!(
        !source.contains("Api<Secret>") && !source.contains("Api::<Secret>"),
        "writing a reference into a PodSpec is not reading a value; the kubelet projects it and \
         the controller holds no verb on `secrets` (G8)"
    );
}

/// Whether `code` names `needle` as a WHOLE identifier.
///
/// `contains` is not enough and the difference is not pedantry:
/// `controllers/restore.rs` carries a `topicPreflight` status field — phase 0's
/// own broker-config observation, which has nothing to do with this kind — and
/// a substring scan would report it forever and be switched off. A word-boundary
/// scan still catches `use crate::crds::preflight::Preflight`,
/// `Api<Preflight>` and the plural in a string literal, which is every shape
/// the forbidden read can take.
fn names_word(code: &str, needle: &str) -> bool {
    let bytes = code.as_bytes();
    let mut from = 0usize;
    while let Some(at) = code[from..].find(needle) {
        let start = from + at;
        let end = start + needle.len();
        let before_ok = start == 0 || !is_ident(bytes[start - 1] as char);
        let after_ok = end == bytes.len() || !is_ident(bytes[end] as char);
        if before_ok && after_ok {
            return true;
        }
        from = start + 1;
    }
    false
}

fn is_ident(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

fn rust_files_under(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return out;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            out.extend(rust_files_under(&path));
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
    out
}

// ===========================================================================
// 9. The seam W10 wires, and the plan the pod mounts
// ===========================================================================

/// The check pod is the EXECUTION pod's shape: same signing mount, same
/// credential projections, no Kubernetes token, and the plan at `/check`.
#[test]
fn the_check_pod_mirrors_the_execution_pod() {
    let cluster: weirkeeper::crds::kafka_cluster::KafkaCluster =
        serde_json::from_value(kafka_cluster("source", Some("prod-id"))).expect("fixture");
    let destination: weirkeeper::crds::backup_destination::BackupDestination =
        serde_json::from_value(backup_destination("primary")).expect("fixture");
    let policy = weirkeeper::check::policy::Policy::defaults();
    let resolved_connection = weirkeeper::connection::resolve(
        &cluster,
        weirkeeper::connection::ConnectionUse::PreflightSource,
    )
    .expect("the fixture connection resolves");
    let resolved_destination = weirkeeper::destination::resolve(
        &destination,
        weirkeeper::destination::DestinationRole::ArchiveWrite,
        &policy,
    )
    .expect("the fixture destination resolves");

    let inputs = Inputs {
        operation: PreflightOperation::Backup,
        namespace: NS.to_string(),
        timeout_seconds: 120,
        policy_digest: policy.digest(),
        cluster_uid: Some(CLUSTER_UID.to_string()),
        connection: Some(Ok(resolved_connection)),
        archive_name: Some("primary".to_string()),
        archive: Some(Ok(resolved_destination)),
        roles: vec![
            weirkeeper::destination::DestinationRole::ArchiveWrite,
            weirkeeper::destination::DestinationRole::EvidenceWrite,
        ],
        topics: vec!["orders".to_string()],
        ..Inputs::default()
    };
    let owner = weirkeeper::job::RunnerOwner {
        api_version: "logweir.dev/v1alpha1".to_string(),
        kind: "Preflight".to_string(),
        name: "pf-1".to_string(),
        uid: PF_UID.to_string(),
    };
    let shape = weirkeeper::controllers::preflight::build_job_shape(
        &inputs,
        &owner,
        &weirkeeper::job::RunnerImage::default(),
    )
    .expect("the shape renders");
    let job = weirkeeper::check::job::build(&shape.spec);
    let spec = job.spec.expect("the Job has a spec");
    let pod = spec.template.spec.expect("the pod template has a spec");
    assert_eq!(pod.automount_service_account_token, Some(false));
    assert_eq!(spec.backoff_limit, Some(0));
    assert_eq!(
        spec.ttl_seconds_after_finished, None,
        "no TTL at creation: the relay lives on the pod"
    );
    let container = &pod.containers[0];
    assert_eq!(container.name, "runner");
    assert_eq!(
        container.args.as_ref().expect("argv"),
        &weirkeeper::check::job::runner_argv()
    );
    let volumes: BTreeSet<String> = pod
        .volumes
        .unwrap_or_default()
        .iter()
        .map(|v| v.name.clone())
        .collect();
    assert!(
        volumes.contains("signing"),
        "the signer is a FILE the runner opens"
    );
    assert!(volumes.contains("check-plan"));

    // The plan itself: no credential value anywhere in it.
    let text = String::from_utf8(shape.documents.check_plan.clone()).expect("the plan is UTF-8");
    assert!(
        text.contains("\"passwordEnv\""),
        "the plan names the variable"
    );
    assert!(
        !text.contains(FIXTURE_SECRET_VALUE),
        "the plan carries NO credential value; the kubelet projects it"
    );
    assert!(
        text.contains("logweir.dev/check-plan/v1"),
        "the contract the runner refuses anything else for"
    );
    assert_eq!(
        shape.spec.plan_sha256,
        logweir_core::ids::sha256_prefixed(&shape.documents.check_plan),
        "the digest the Job pins is the digest of the document the ConfigMap carries"
    );
}

/// The Job's name is a pure function of the subject's UID, so a duplicate
/// reconcile gets a 409 rather than a second check pod.
#[test]
fn the_job_name_is_independent_of_the_subjects_name() {
    let a = check::job::check_job_name(CheckPlanKind::OperationReadiness, PF_UID);
    let b = check::job::check_job_name(CheckPlanKind::RestorePreflight, PF_UID);
    assert_ne!(a, b, "two kinds for one subject are two Jobs");
    assert!(a.len() <= 63 && a.starts_with("lwc-rd-"));
    assert!(b.starts_with("lwc-rp-"));
}

/// The plan kind follows the operation, and nothing else does.
#[test]
fn each_operation_runs_its_own_plan_kind() {
    assert_eq!(
        pf::plan_kind(PreflightOperation::Backup),
        CheckPlanKind::OperationReadiness
    );
    assert_eq!(
        pf::plan_kind(PreflightOperation::Restore),
        CheckPlanKind::RestorePreflight
    );
    assert_eq!(
        pf::plan_kind(PreflightOperation::DestinationAccess),
        CheckPlanKind::DestinationAccess
    );
}
