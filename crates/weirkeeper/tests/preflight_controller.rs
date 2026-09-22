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
use logweir_core::check_contract::{CheckRequest, Referent, RosterRef, StaleReason};
use logweir_core::destination::DestinationRole;
use serde_json::{json, Value};

use weirkeeper::check::{self, Projections, Waiting};
use weirkeeper::controllers::preflight::{
    self as pf, assemble, cluster_identity_row, connection_row, controller_rows, destination_row,
    entry_of, fill_scopes, job_rows, plan_bindings_row, plan_names_row, plan_parse_row,
    pod_outcomes, recovery_point_row, result_for, signer_rostered_row, stale_against_status,
    unrendered_job_rows, ApprovalFacts, BindingFacts, Inputs, PlanFacts, RecoveryPointFacts,
    RosterFacts, ScopeReferents,
};
use weirkeeper::controllers::Context;
use weirkeeper::crds::approval::ApproverKeyWindow;
use weirkeeper::crds::preflight::{Preflight, PreflightOperation};
use weirkeeper::testing::{mock_client_recording_bodies, Route};

const NS: &str = "team-a";
const PF_UID: &str = "aaaaaaaa-0000-4000-8000-00000000000a";
const CLUSTER_UID: &str = "bbbbbbbb-0000-4000-8000-00000000000b";
const DEST_UID: &str = "cccccccc-0000-4000-8000-00000000000c";
const ROSTER_UID: &str = "dddddddd-0000-4000-8000-00000000000d";
const JOB_UID: &str = "eeeeeeee-0000-4000-8000-00000000000e";
const POD_UID: &str = "ffffffff-0000-4000-8000-00000000000f";
/// The runner's PUBLIC signing key id, in the form the roster is actually
/// written in: the sha256 of a SubjectPublicKeyInfo DER, 64 lowercase hex
/// characters (`logweir_core::trust::TrustedKey::key_id`). These fixtures used
/// to say `runner-key-1`, which is not one, and D2-SIGNERID-REDACTED turned on
/// exactly that difference: the value the controller compared was `[redacted]`.
const RUNNER_KEY_ID: &str = "11d4c0d6a5fd4c5c9b2d7e6f8a90b1c2d3e4f5061728394a5b6c7d8e9f0a1b2c";
/// A key id the roster does NOT list — a key id all the same.
const OTHER_KEY_ID: &str = "22e5d1e7b60e5d6dac3e8f709ab1c2d3e4f5061728394a5b6c7d8e9f0a1b2c3d";
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
        PreflightOperation::SourceConnection,
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
        for id in unrendered_job_rows(operation) {
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
    let key = RUNNER_KEY_ID;
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
        signer_rostered_row(
            PreflightOperation::Backup,
            &loaded,
            Some(OTHER_KEY_ID),
            now()
        )
        .code,
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

    // EVERY ANSWER NAMES THE OBJECT ITS REMEDY IS ABOUT. Four of the five ask
    // the operator to edit the `TrustRoster`, and the live run published all of
    // them with `scope: null`.
    for row in [
        signer_rostered_row(PreflightOperation::Backup, &empty, Some(key), now()),
        signer_rostered_row(PreflightOperation::Backup, &no_keys, Some(key), now()),
        signer_rostered_row(PreflightOperation::Backup, &loaded, None, now()),
        signer_rostered_row(
            PreflightOperation::Backup,
            &loaded,
            Some(OTHER_KEY_ID),
            now(),
        ),
        signer_rostered_row(PreflightOperation::Backup, &expired, Some(key), now()),
        ok,
        signer_rostered_row(PreflightOperation::Restore, &loaded, Some(key), now()),
    ] {
        let scope = row
            .scope
            .as_ref()
            .unwrap_or_else(|| panic!("`{}` carries no scope", row.code));
        assert_eq!(scope.kind, "TrustRoster");
        assert_eq!(scope.name, "default");
    }
    // The UID is the roster that was actually READ, so a verdict taken against
    // a roster that has since been replaced is visibly about another object.
    let identified = RosterFacts {
        found: true,
        uid: ROSTER_UID.to_string(),
        signing_keys: vec![(key.to_string(), None)],
        ..RosterFacts::default()
    };
    assert_eq!(
        signer_rostered_row(PreflightOperation::Backup, &identified, Some(key), now())
            .scope
            .and_then(|s| s.uid)
            .as_deref(),
        Some(ROSTER_UID)
    );
    assert_eq!(
        signer_rostered_row(PreflightOperation::Backup, &empty, Some(key), now())
            .scope
            .and_then(|s| s.uid),
        None,
        "a roster that does not exist has no UID to name"
    );
}

/// **D2-SIGNERID-REDACTED, the controller half.** A relayed `signerKeyId` that
/// is not a key id is not compared against the roster at all.
///
/// The live run (`results.json#E5`) recorded `signer.rostered
/// notReady/SignerNotRostered` for a key whose SPKI sha256 IS the roster's,
/// because the fact reached this comparison as the literal `[redacted]` and the
/// roster does not list a key by that name. The redactor no longer blanks a key
/// id; this is the belt, and it matters because the two failures are opposite
/// in kind: `SignerNotRostered` is a BLOCKING refusal about a key, and
/// `SignerKeyIdNotObserved` is `unknown` about nothing.
#[test]
fn a_relayed_signer_key_id_that_is_not_a_key_id_is_not_observed() {
    let loaded = RosterFacts {
        found: true,
        uid: ROSTER_UID.to_string(),
        signing_keys: vec![(RUNNER_KEY_ID.to_string(), None)],
        ..RosterFacts::default()
    };
    for bad in [
        "[redacted]",
        "",
        "runner-key-1",
        // Right alphabet, wrong width.
        &RUNNER_KEY_ID[..63],
        &format!("{RUNNER_KEY_ID}0"),
        // Right width, wrong alphabet: upper case is not the roster's form.
        &RUNNER_KEY_ID.to_uppercase(),
    ] {
        let row = signer_rostered_row(PreflightOperation::Backup, &loaded, Some(bad), now());
        assert_eq!(
            (row.state, row.code),
            (CheckState::Unknown, CheckCode::SignerKeyIdNotObserved),
            "`{bad}` was compared against the roster and refused as a key"
        );
        assert!(
            !row.message.contains(bad) || bad.is_empty(),
            "a value that is not a key id is not quoted back as one: {}",
            row.message
        );
    }
    // And the real thing still answers.
    let row = signer_rostered_row(
        PreflightOperation::Backup,
        &loaded,
        Some(RUNNER_KEY_ID),
        now(),
    );
    assert_eq!(
        (row.state, row.code),
        (CheckState::Ready, CheckCode::SignerRostered)
    );
}

/// **D2 §6.3's "with check time AND scope", for the rows that had none.**
///
/// The live run published six `notReady` rows with `scope: null`
/// (`objects/s14/notready-rows.json`): `connection.credentialProjected`
/// (`podStatus`), `connection.authenticated` (`checkJob`) and `signer.rostered`
/// (`controller`). Each authority is a statement about WHO observed the thing,
/// so each has a referent the controller knows, and a row that named its own
/// object keeps it.
#[test]
fn every_row_without_a_scope_gets_the_referent_its_authority_implies() {
    let referents = ScopeReferents {
        subject: Some(logweir_core::check_contract::CheckScope {
            kind: "Preflight".to_string(),
            name: "pf-1".to_string(),
            uid: Some(PF_UID.to_string()),
        }),
        job: Some(logweir_core::check_contract::CheckScope {
            kind: "Job".to_string(),
            name: "lw-check-pf-1".to_string(),
            uid: Some(JOB_UID.to_string()),
        }),
        pod: Some(logweir_core::check_contract::CheckScope {
            kind: "Pod".to_string(),
            name: "lw-check-pf-1-abcde".to_string(),
            uid: Some(POD_UID.to_string()),
        }),
    };
    let row = |id: CheckId, authority: Authority| {
        CheckOutcome::new(
            id,
            CheckState::NotReady,
            Gating::Blocking,
            authority,
            CheckCode::BlockedByPrerequisite,
        )
    };
    let mut checks = vec![
        row(CheckId::ConnectionCredentialProjected, Authority::PodStatus),
        row(CheckId::ConnectionAuthenticated, Authority::CheckJob),
        row(CheckId::SignerRostered, Authority::Controller),
        // A row that DID name its own object keeps it: the referent is the
        // better answer where a row has one.
        row(CheckId::DestinationResolved, Authority::Controller).with_scope(
            logweir_core::check_contract::CheckScope {
                kind: "BackupDestination".to_string(),
                name: "prod-archive".to_string(),
                uid: Some(DEST_UID.to_string()),
            },
        ),
    ];
    fill_scopes(&mut checks, &referents);
    let kinds: Vec<(&str, &str)> = checks
        .iter()
        .map(|c| {
            let s = c
                .scope
                .as_ref()
                .unwrap_or_else(|| panic!("`{}` carries no scope", c.id));
            assert!(s.uid.is_some(), "`{}`'s scope names no UID", c.id);
            (c.id.as_str(), s.kind.as_str())
        })
        .collect();
    assert_eq!(
        kinds,
        vec![
            ("connection.credentialProjected", "Pod"),
            ("connection.authenticated", "Job"),
            ("signer.rostered", "Preflight"),
            ("destination.resolved", "BackupDestination"),
        ]
    );

    // BEFORE THE JOB HAS A POD, a pod-authority row is still about the Job that
    // would have made one; before either exists, everything is about the
    // `Preflight`. A row with nothing to name would be the bug again.
    let mut checks = vec![
        row(CheckId::ConnectionCredentialProjected, Authority::PodStatus),
        row(CheckId::ConnectionAuthenticated, Authority::CheckJob),
    ];
    fill_scopes(
        &mut checks,
        &ScopeReferents {
            pod: None,
            ..referents.clone()
        },
    );
    assert!(checks
        .iter()
        .all(|c| c.scope.as_ref().unwrap().kind == "Job"));

    let mut checks = vec![
        row(CheckId::ConnectionCredentialProjected, Authority::PodStatus),
        row(CheckId::ConnectionAuthenticated, Authority::CheckJob),
        row(CheckId::SignerRostered, Authority::Controller),
    ];
    fill_scopes(
        &mut checks,
        &ScopeReferents {
            subject: referents.subject.clone(),
            job: None,
            pod: None,
        },
    );
    assert!(checks
        .iter()
        .all(|c| c.scope.as_ref().unwrap().kind == "Preflight"));
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

    // REVIEW F4. THAT IS A `Backup` VERDICT, AND ONLY A `Backup` VERDICT. A
    // connectivity check asks whether the connection answers; it did. Reported
    // for `SourceConnection` it made a successful dial to a legitimate restore
    // target read `not ready` under a remedy advising a backup nobody asked
    // for — the panel's whole job is to say what the dial found.
    let connectivity = cluster_identity_row(
        PreflightOperation::SourceConnection,
        Some("scratch-id"),
        Some("scratch-id"),
        &["scratch-id".to_string()],
        None,
        false,
        now(),
    );
    assert_eq!(connectivity.code, CheckCode::ClusterIdentityMatches);
    assert_eq!(connectivity.state, CheckState::Ready);
    assert_eq!(
        connectivity.facts.get("clusterId").map(String::as_str),
        Some("scratch-id")
    );

    // AND THE ROW STILL BLOCKS ON THE FACT THAT IS ABOUT THE CONNECTION: a
    // broker naming a different cluster than the object records is a
    // connectivity finding whatever the operation.
    let moved = cluster_identity_row(
        PreflightOperation::SourceConnection,
        Some("observed-id"),
        Some("recorded-id"),
        &[],
        None,
        false,
        now(),
    );
    assert_eq!(moved.code, CheckCode::ClusterIdentityChanged);
    assert_eq!(moved.state, CheckState::NotReady);
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

/// A `Succeeded` recovery point that froze `frozen`, checked against a source
/// destination that resolved `expected`.
fn recovery_point_at(frozen: Option<&str>, expected: Option<&str>) -> RecoveryPointFacts {
    RecoveryPointFacts::Found {
        name: "nightly-1".to_string(),
        uid: BACKUP_UID.to_string(),
        expected_uid: Some(BACKUP_UID.to_string()),
        phase: Some("Succeeded".to_string()),
        location_digest: frozen.map(str::to_string),
        expected_location_digest: expected.map(str::to_string),
    }
}

/// **A RECOVERY POINT IS ONLY RESTORABLE FROM THE DESTINATION IT WAS WRITTEN
/// TO** — D2 §3.12, and the row that finally says so.
///
/// # The defect this closes
///
/// Until `Backup.status.destination` existed there was no digest on the object
/// to compare, so `recoveryPoint.state` answered `RecoveryPointSucceeded` for a
/// point sitting in the OTHER destination's bucket. A restore started from that
/// green row reaches `archive.backupSet` and fails there, against a bucket the
/// operator never chose, with a store error instead of a sentence naming the
/// two locations.
///
/// # Why `unknown` and not `ready` for a legacy point
///
/// A point archived before saved destinations existed publishes no location at
/// all. `ready` would be this blocking row reporting a comparison nobody made;
/// `notReady` would refuse a restore that is very probably fine. `unknown` is
/// the third answer the aggregate already understands, and it holds the verdict
/// until a human confirms the location.
///
/// KILLS, one mutant each:
/// * the mismatch arm answering `Ready`/`RecoveryPointSucceeded`;
/// * the mismatch message dropping either digest;
/// * the `(None, Some(_))` arm falling through to `Ready`;
/// * the `(None, Some(_))` arm widened to `(_, _)`, which would make the
///   mirror case — a frozen point under a check that resolved no destination —
///   `unknown` too and pin every legacy restore preflight at `unknown`.
#[test]
fn the_recovery_point_location_comparison_has_three_answers() {
    let here = "sha256:1111111111111111111111111111111111111111111111111111111111111111";
    let there = "sha256:2222222222222222222222222222222222222222222222222222222222222222";

    let same = recovery_point_row(&recovery_point_at(Some(here), Some(here)), now()).unwrap();
    assert_eq!(same.code, CheckCode::RecoveryPointSucceeded);
    assert_eq!(same.state, CheckState::Ready);

    let moved = recovery_point_row(&recovery_point_at(Some(here), Some(there)), now()).unwrap();
    assert_eq!(moved.code, CheckCode::RecoveryPointLocationMismatch);
    assert_eq!(moved.state, CheckState::NotReady);
    let message = moved.message.as_str();
    assert!(
        message.contains(here) && message.contains(there),
        "BOTH digests belong in the sentence: an operator holding two \
         destinations has to be told which one this point is in. Got: {message}"
    );
    assert!(
        !moved.remedy.is_empty(),
        "every notReady row names a way forward"
    );

    let legacy = recovery_point_row(&recovery_point_at(None, Some(here)), now()).unwrap();
    assert_eq!(legacy.code, CheckCode::RecoveryPointLocationUnknown);
    assert_eq!(
        legacy.state,
        CheckState::Unknown,
        "a point with no frozen destination is not a point proved to be in this one"
    );
    assert!(
        legacy.message.contains(here),
        "the row names the destination it could not compare against"
    );

    // THE MIRROR CASE, AND IT IS NOT THE SAME CASE. A check that resolved no
    // source destination makes no location claim, so there is nothing for this
    // row to compare and `plan.bindings` is what holds the legacy plan's
    // location to account. Answering `unknown` here would pin every legacy
    // restore preflight at `unknown` for a question nobody asked.
    let no_claim = recovery_point_row(&recovery_point_at(Some(here), None), now()).unwrap();
    assert_eq!(no_claim.code, CheckCode::RecoveryPointSucceeded);
    let neither = recovery_point_row(&recovery_point_at(None, None), now()).unwrap();
    assert_eq!(neither.code, CheckCode::RecoveryPointSucceeded);
}

/// A roster, for the rows that legitimately read one.
///
/// SINCE PREFLIGHT-APPROVAL-ROSTER THAT IS `signer.rostered` AND NOTHING ELSE
/// in this file: the signing key's roster membership IS a roster fact, and the
/// approver key's lifecycle is not — it belongs to the trust policy the
/// Approval controller resolves through. The `approver_keys` entry stays on
/// the fixture because `RosterFacts` still carries it for the binding digest.
fn approver_roster(not_after: Option<DateTime<Utc>>) -> RosterFacts {
    RosterFacts {
        found: true,
        uid: ROSTER_UID.to_string(),
        generation: 4,
        signing_keys: vec![(RUNNER_KEY_ID.to_string(), None)],
        approver_keys: vec![("approver-1".to_string(), not_after)],
        allowed_cluster_ids: Vec::new(),
    }
}

/// One `Approval` as the preflight reduces it.
///
/// `reason` is the `Verified` condition's REASON token — what the row routes
/// on — and `message` is its prose, which is what an operator reads. They used
/// to be one field holding whichever existed, and that conflation is half of
/// PREFLIGHT-APPROVAL-ROSTER: nothing could branch on a sentence, so the row
/// branched on the roster instead.
fn found_approval_with(
    verified: Option<bool>,
    reason: Option<&str>,
    message: Option<&str>,
    approved_hash: Option<&str>,
) -> ApprovalFacts {
    ApprovalFacts::Found {
        name: "ap-1".to_string(),
        uid: "ap-uid".to_string(),
        resource_version: "7".to_string(),
        verified,
        reason: reason.map(str::to_string),
        message: message.map(str::to_string),
        matched_key_id: Some("approver-1".to_string()),
        // NO WINDOW BY DEFAULT — which is what a controller image that
        // predates `status.approverKeyWindow` publishes, and what an
        // `Approval` that matched no key publishes. Every row that wants one
        // says so through [`windowed`].
        approver_key_window: None,
        approved_plan_hash: approved_hash.map(str::to_string),
        subject_name: Some("r-1".to_string()),
    }
}

/// The same facts, carrying the window `controllers::approval` publishes for
/// `key_id` — `notBefore` an hour back, `notAfter` at `not_after`.
fn windowed(facts: ApprovalFacts, key_id: &str, not_after: DateTime<Utc>) -> ApprovalFacts {
    let ApprovalFacts::Found {
        name,
        uid,
        resource_version,
        verified,
        reason,
        message,
        matched_key_id,
        approved_plan_hash,
        subject_name,
        ..
    } = facts
    else {
        panic!("only a Found approval carries a window");
    };
    ApprovalFacts::Found {
        name,
        uid,
        resource_version,
        verified,
        reason,
        message,
        matched_key_id,
        approver_key_window: Some(Box::new(ApproverKeyWindow {
            key_id: key_id.to_string(),
            not_before: now() - Duration::hours(1),
            not_after,
        })),
        approved_plan_hash,
        subject_name,
    }
}

/// The `approval.keyValidity` row out of one `approval_rows` call.
fn validity_row(rows: &[CheckOutcome]) -> CheckOutcome {
    rows.iter()
        .find(|r| r.id == CheckId::ApprovalKeyValidity)
        .expect("every Found approval reports approval.keyValidity")
        .clone()
}

fn found_approval(verified: Option<bool>, approved_hash: Option<&str>) -> ApprovalFacts {
    found_approval_with(
        verified,
        verified.and_then(|v| (!v).then_some("SignatureInvalid")),
        verified.and_then(|v| (!v).then_some("the DSSE signature did not verify")),
        approved_hash,
    )
}

fn state_row(rows: &[CheckOutcome]) -> CheckOutcome {
    rows.iter()
        .find(|r| r.id == CheckId::ApprovalState)
        .expect("every Found approval reports approval.state")
        .clone()
}

#[test]
fn a_draft_plan_is_skipped_and_a_skip_is_not_an_answer() {
    let rows = pf::approval_rows(&ApprovalFacts::Draft, "sha256:aa", None, None, now());
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

/// **PREFLIGHT-APPROVAL-ROSTER.** The expiry verdict is the Approval
/// controller's, read off its `Verified` condition's reason.
///
/// The defect: this row resolved the approver key's `notAfter` from
/// `TrustRoster/default` and decided expiry itself, while
/// `controllers::approval` resolves the same key through the **trust policy**
/// that governs the namespace (D3 W10). On `lab-refresh-4` an isolated
/// `TrustPolicy` with an expiring approver key moved the Approval to
/// `Verified=False, KeyIdExpired` while this row — reading the roster, which
/// is immutable and still showed the key open — said `ready`. A preflight that
/// authorises what the controller has already refused is the one direction
/// this kind may never fail in.
#[test]
fn an_approval_expired_under_a_trust_policy_is_an_expired_approval() {
    // Exactly what the Approval controller publishes in that case: the key
    // matched, the signature was fine, and the RESOLVED POLICY's entry for it
    // had passed its notAfter. The roster is not consulted and cannot be.
    let rows = pf::approval_rows(
        &found_approval_with(
            Some(false),
            Some("KeyIdExpired"),
            Some(
                "the approval verified under key id approver-1, whose notAfter \
                 2026-09-18T00:00:00Z has passed; a key past notAfter does not authorise anything",
            ),
            Some("sha256:aa"),
        ),
        "sha256:aa",
        Some("r-1"),
        None,
        now(),
    );
    let state = state_row(&rows);
    assert_eq!(state.code, CheckCode::ApprovalExpired);
    assert_eq!(state.state, CheckState::NotReady);
    // THE RESTORE IS REFUSED: `approval.state` is blocking, so the aggregate
    // is notReady however green everything else is.
    assert_eq!(state.gating, Gating::Blocking);
    assert_eq!(
        logweir_core::check_contract::aggregate(&rows),
        OverallState::NotReady
    );
    // THE CONTROLLER'S OWN SENTENCE, which names the key and the instant this
    // row no longer resolves and must not invent.
    assert!(
        state.message.contains("notAfter") && state.message.contains("approver-1"),
        "the row relays the controller's message: {}",
        state.message
    );
    assert!(state.remedy.contains("re-approved"));
}

/// The reason token this row routes on is the one the Approval controller
/// writes, read from its own table.
#[test]
fn the_expiry_reason_is_the_one_the_approval_controller_writes() {
    let refusal = weirkeeper::controllers::approval::ApprovalRefusal::KeyIdExpired {
        key_id: "approver-1".to_string(),
        not_after: "2026-09-18T00:00:00Z".to_string(),
    };
    assert_eq!(
        pf::APPROVAL_REASON_KEY_ID_EXPIRED,
        refusal.reason(),
        "a rename in `controllers::approval` must be red here, not a preflight that quietly \
         stops recognising an expiry"
    );
}

/// A retired or revoked key is NOT an expired key, and this row does not
/// pretend otherwise.
///
/// `controllers::approval` is explicit: reporting a revocation as an expiry
/// sends an operator to extend a window when the remedy is an investigation,
/// and a retirement has no `notAfter` to extend at all. The closed check
/// vocabulary has one code for "did not verify"; the controller's reason and
/// message carry which.
#[test]
fn a_retired_or_revoked_key_is_not_reported_as_an_expiry() {
    for (reason, message) in [
        (
            "KeyRetired",
            "which the resolved trust policy records as Retired at 2026-09-01",
        ),
        ("KeyRevoked", "records as Revoked (KeyCompromise)"),
        (
            "KeyNotYetValid",
            "whose notBefore 2027-01-01T00:00:00Z has not arrived",
        ),
        (
            "KeyIdNotInRoster",
            "no key on the TrustRoster 'default' approverKeys authorises this",
        ),
        ("TrustPolicyConflict", "two policies claim this namespace"),
    ] {
        let rows = pf::approval_rows(
            &found_approval_with(Some(false), Some(reason), Some(message), Some("sha256:aa")),
            "sha256:aa",
            Some("r-1"),
            None,
            now(),
        );
        let state = state_row(&rows);
        assert_eq!(
            state.code,
            CheckCode::ApprovalNotVerified,
            "`{reason}` is a refusal and not an expiry"
        );
        assert_eq!(state.state, CheckState::NotReady);
        assert_eq!(
            state.message, message,
            "the controller's own words reach the row verbatim"
        );
    }
}

#[test]
fn a_verified_approval_is_ready_and_says_what_it_read() {
    let rows = pf::approval_rows(
        &found_approval(Some(true), Some("sha256:aa")),
        "sha256:aa",
        Some("r-1"),
        None,
        now(),
    );
    let state = state_row(&rows);
    assert_eq!(
        (state.state, state.code),
        (CheckState::Ready, CheckCode::ApprovalVerified)
    );
    assert!(
        state.message.contains("Verified condition"),
        "it names the fact it read, not a roster it no longer looks at: {}",
        state.message
    );
}

#[test]
fn an_approval_for_another_plan_is_a_plan_mismatch_and_not_a_pending_approval() {
    let rows = pf::approval_rows(
        &found_approval(Some(false), Some("sha256:bb")),
        "sha256:aa",
        Some("r-1"),
        None,
        now(),
    );
    assert_eq!(
        state_row(&rows).code,
        CheckCode::ApprovalPlanMismatch,
        "reporting a plan mismatch as `not verified yet` sends an operator to wait for something \
         that has already happened"
    );
    // THE ORDER IS UNCHANGED: plan hash, then expiry, then verified. An
    // approval that expired AND names another plan is a plan mismatch, because
    // the mismatch is the stronger and more actionable finding.
    let both = pf::approval_rows(
        &found_approval_with(
            Some(false),
            Some("KeyIdExpired"),
            Some("expired"),
            Some("sha256:bb"),
        ),
        "sha256:aa",
        Some("r-1"),
        None,
        now(),
    );
    assert_eq!(state_row(&both).code, CheckCode::ApprovalPlanMismatch);
}

#[test]
fn an_approval_with_no_status_yet_is_pending_and_not_a_refusal() {
    let rows = pf::approval_rows(
        &found_approval(None, Some("sha256:aa")),
        "sha256:aa",
        Some("r-1"),
        None,
        now(),
    );
    let state = state_row(&rows);
    assert_eq!(
        (state.state, state.code),
        (CheckState::Unknown, CheckCode::ApprovalPending)
    );
}

#[test]
fn an_approval_that_is_missing_or_unnamed_is_named_as_such() {
    let missing = pf::approval_rows(
        &ApprovalFacts::NotFound {
            name: "ap-gone".to_string(),
        },
        "sha256:aa",
        Some("r-1"),
        None,
        now(),
    );
    assert_eq!(
        missing.len(),
        1,
        "a missing Approval has no key to report on"
    );
    assert_eq!(missing[0].code, CheckCode::ApprovalNotVerified);
    assert_eq!(missing[0].state, CheckState::NotReady);
    assert!(missing[0].message.contains("ap-gone"));

    let unnamed = pf::approval_rows(
        &ApprovalFacts::NotNamed,
        "sha256:aa",
        Some("r-1"),
        None,
        now(),
    );
    assert_eq!(unnamed[0].code, CheckCode::ApprovalNotVerified);
    assert!(unnamed[0].message.contains("names no Approval"));
}

#[test]
fn an_approval_that_names_another_subject_is_a_subject_mismatch() {
    let rows = pf::approval_rows(
        &found_approval(Some(true), Some("sha256:aa")),
        "sha256:aa",
        Some("r-2"),
        None,
        now(),
    );
    assert_eq!(state_row(&rows).code, CheckCode::ApprovalSubjectMismatch);
}

/// **APPROVAL-KEY-WINDOW-UNPUBLISHED, deliverable 2.** The advisory row reads
/// the window the `Approval` publishes and warns ahead of time.
///
/// D2 §6.3's condition verbatim: `ApproverKeyExpiresBeforeDeadline` when
/// `notAfter` < now + `deadlineSeconds`. It is a WARNING — the row is gated `A`
/// and §6.4 says "Advisory `notReady` appears as warnings" — so the aggregate
/// over the same rows is not dragged to `notReady` by it. The refusal, when the
/// key actually expires, is the blocking row relaying `KeyIdExpired`.
///
/// KILLS: "compare the other way" (`notAfter > deadline`), which warns about
/// every key that OUTLASTS the restore and is the one row an operator learns to
/// ignore; "use `<=`", which warns about a key that is valid for exactly as
/// long as it needs to be; "make the warning blocking", which would refuse a
/// restore whose approval is valid now over a key that expires mid-run.
#[test]
fn a_key_that_expires_before_the_deadline_is_warned_about_and_not_refused() {
    let facts = windowed(
        found_approval(Some(true), Some("sha256:aa")),
        "approver-1",
        now() + Duration::minutes(20),
    );
    let deadline = now() + Duration::hours(1);
    let rows = pf::approval_rows(&facts, "sha256:aa", Some("r-1"), Some(deadline), now());

    let validity = validity_row(&rows);
    assert_eq!(validity.gating, Gating::Advisory);
    assert_eq!(validity.state, CheckState::NotReady);
    assert_eq!(validity.code, CheckCode::ApproverKeyExpiresBeforeDeadline);
    assert!(
        validity.message.contains("approver-1") && validity.message.contains("before this"),
        "the warning names the key and says what it is early for: {}",
        validity.message
    );
    assert!(
        !validity.remedy.is_empty(),
        "a warning an operator can act on names the action"
    );

    // ADVISORY MEANS THE AGGREGATE IS NOT DRAGGED DOWN BY IT (D2 §6.4).
    assert_eq!(state_row(&rows).state, CheckState::Ready);
    assert_eq!(
        logweir_core::check_contract::aggregate(&rows),
        OverallState::Ready,
        "an early-expiring key is a warning ahead of time, not a refusal: {rows:?}"
    );
    assert_eq!(
        logweir_core::check_contract::advisory_warnings(&rows).len(),
        1,
        "…and it is REPORTED, as a warning"
    );

    // THE BOUNDARY. `notAfter` exactly at the deadline is not early.
    let exact = pf::approval_rows(
        &windowed(
            found_approval(Some(true), Some("sha256:aa")),
            "approver-1",
            deadline,
        ),
        "sha256:aa",
        Some("r-1"),
        Some(deadline),
        now(),
    );
    assert_eq!(validity_row(&exact).code, CheckCode::ApproverKeyValid);

    // AND THE ABSENCE OF THE WARNING WHEN THE DEADLINE IS EARLIER.
    let outlasts = pf::approval_rows(
        &windowed(
            found_approval(Some(true), Some("sha256:aa")),
            "approver-1",
            now() + Duration::hours(6),
        ),
        "sha256:aa",
        Some("r-1"),
        Some(deadline),
        now(),
    );
    let row = validity_row(&outlasts);
    assert_eq!(
        (row.state, row.code),
        (CheckState::Ready, CheckCode::ApproverKeyValid)
    );
    assert!(
        row.message.contains("falls inside it"),
        "the ready sentence says what it compared: {}",
        row.message
    );
}

/// **Deliverable 2, second half: `min(10 m, notAfter)` on BOTH rows.**
///
/// D2 §6.3 gives `approval.state` `min(10 m, key notAfter)` and
/// `approval.keyValidity` `min(10 m, notAfter)`. A verdict about a key must not
/// outlive the key: a preflight that stayed fresh for the full ten minutes past
/// a `notAfter` four minutes away would let a restore be admitted on a record
/// that says `ready` about a window that has closed.
///
/// KILLS: "cap only the blocking row" — the advisory row would then outlive the
/// key it is entirely about; "cap only when the key expires before the
/// deadline", which leaves the common case uncapped; "take the catalogue entry
/// unconditionally", the shipped behaviour and the second half of the defect.
#[test]
fn both_approval_rows_re_check_inside_the_published_window() {
    let inside = now() + Duration::minutes(4);
    let rows = pf::approval_rows(
        &windowed(
            found_approval(Some(true), Some("sha256:aa")),
            "approver-1",
            inside,
        ),
        "sha256:aa",
        Some("r-1"),
        Some(now() + Duration::hours(1)),
        now(),
    );
    assert_eq!(rows.len(), 2);
    for row in &rows {
        assert_eq!(
            row.expires_at,
            Some(inside),
            "`{}` must be re-checked at the key's notAfter, not ten minutes later",
            row.id
        );
    }

    // …and the ten minutes is still the CEILING when the key outlasts it.
    let far = pf::approval_rows(
        &windowed(
            found_approval(Some(true), Some("sha256:aa")),
            "approver-1",
            now() + Duration::days(30),
        ),
        "sha256:aa",
        Some("r-1"),
        None,
        now(),
    );
    for row in &far {
        assert_eq!(
            row.expires_at,
            Some(now() + Duration::minutes(10)),
            "`{}` keeps its catalogue expiry when the window is further out",
            row.id
        );
    }
    // …and with NO deadline there was no comparison, so the ready sentence must
    // not claim one. Reachable in production: `deadlineSeconds` is an unbounded
    // `int64`, and an absurd value overflows `now + deadlineSeconds` to `None`.
    let no_deadline = validity_row(&far);
    assert_eq!(no_deadline.code, CheckCode::ApproverKeyValid);
    assert!(
        no_deadline
            .message
            .contains("names no deadline to compare it against"),
        "a green row must not describe work it did not do: {}",
        no_deadline.message
    );
    assert!(
        !no_deadline.message.contains("falls inside it"),
        "that is the sentence for a comparison that happened: {}",
        no_deadline.message
    );
}

/// **Deliverable 2, third half: no window is UNKNOWN, and never valid.**
///
/// PLAT-19.1's acceptance is explicit — unevaluated or stale expiry information
/// is unknown, not valid. Three shapes reach it: an `Approval` that matched no
/// key (nothing to publish), a controller image that predates
/// `status.approverKeyWindow` (an upgrade in progress), and a window that names
/// SOME OTHER key than the verdict does. The third is why
/// `controllers::approval` writes the key id inside the window.
///
/// The advisory row being `unknown` does not make the aggregate `unknown`: D2
/// §6.4 aggregates over blocking checks, so honesty here costs a reader nothing
/// but a green badge it was not entitled to.
///
/// KILLS: "treat an absent window as valid" — the row would read `ready` about
/// a key it knows nothing about, which is the whole defect; "trust any window
/// the status carries", which compares a deadline against another key's
/// rotation schedule; "make the unknown row blocking", which would stall every
/// restore through the upgrade that adds the field.
#[test]
fn an_unpublished_or_mismatched_window_is_unknown_and_never_valid() {
    let deadline = Some(now() + Duration::hours(1));

    // 1. nothing published at all.
    let none = pf::approval_rows(
        &found_approval(Some(true), Some("sha256:aa")),
        "sha256:aa",
        Some("r-1"),
        deadline,
        now(),
    );
    let row = validity_row(&none);
    assert_eq!(
        (row.state, row.code),
        (CheckState::Unknown, CheckCode::ApproverKeyWindowUnknown)
    );
    assert!(
        row.message.contains("approver-1") && row.message.contains("never valid"),
        "it names the key it could not ground and says what absence means: {}",
        row.message
    );
    assert_eq!(row.gating, Gating::Advisory);
    assert_eq!(
        logweir_core::check_contract::aggregate(&none),
        OverallState::Ready,
        "an advisory unknown does not stall a restore whose blocking rows are ready"
    );
    assert_eq!(
        none[0].expires_at,
        Some(now() + Duration::minutes(10)),
        "with no window there is nothing to cap at, so the catalogue entry stands"
    );

    // 2. a window about a DIFFERENT key than the one that verified.
    let other = pf::approval_rows(
        &windowed(
            found_approval(Some(true), Some("sha256:aa")),
            "approver-2",
            now() + Duration::minutes(1),
        ),
        "sha256:aa",
        Some("r-1"),
        deadline,
        now(),
    );
    let row = validity_row(&other);
    assert_eq!(
        (row.state, row.code),
        (CheckState::Unknown, CheckCode::ApproverKeyWindowUnknown),
        "a window naming approver-2 says nothing about approver-1, and a minute-long \
         window would otherwise have produced a very confident warning"
    );
    for row in &other {
        assert_eq!(
            row.expires_at,
            Some(now() + Duration::minutes(10)),
            "and no row is capped at another key's notAfter"
        );
    }
}

/// **The roster is not reachable from this path at all**, and the signature is
/// what makes that true rather than a comment.
///
/// `approval_rows` is not GIVEN a `RosterFacts`, so it cannot consult one
/// however the body is later edited. This reads the source for the shape,
/// because a type that is absent cannot be asserted against at run time.
#[test]
fn the_approval_rows_are_not_given_the_roster() {
    let source = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join("controllers")
            .join("preflight.rs"),
    )
    .expect("the controller source reads");

    for function in [
        "pub fn approval_rows(",
        "fn approval_verdict(",
        "fn key_validity_row(",
    ] {
        let at = source.find(function).unwrap_or_else(|| {
            panic!(
                "`{function}` is gone; this guard cannot hold a shape that \
                                       no longer exists"
            )
        });
        let body = &source[at..];
        let end = body.find(')').expect("the parameter list closes");
        let params = &body[..end];
        assert!(
            !params.contains("RosterFacts"),
            "`{function}` takes a RosterFacts again. Resolving the approver key here is \
             PREFLIGHT-APPROVAL-ROSTER: the roster is not the authority the Approval controller \
             resolves through, so a verdict derived from it can authorise what the controller \
             has already refused."
        );
    }

    // And nothing in the approval half reaches for the roster's approver keys
    // by another route.
    let from = source
        .find("pub fn approval_rows(")
        .expect("the row exists");
    let to = source
        .find("// ---------------------------------------------------------------------------\n// The pod-status rows")
        .expect("the approval section ends where the pod-status section begins");
    assert!(
        to > from,
        "the approval section precedes the pod-status one"
    );
    assert!(
        !source[from..to].contains("approver_keys"),
        "the approval rows read `RosterFacts::approver_keys` again"
    );
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

/// A source-connection check names NO destination, so it reports no
/// destination credential row.
///
/// MUTANT: put `destination.credentialProjected` back in `pod_rows`'
/// unconditional base. It is BLOCKING, nothing answers it for this operation,
/// and `assemble` renders an unanswered row as a blocking `unknown` — so every
/// connection test would be pinned at `unknown` for ever, with a row reading
/// "the check Job did not report this row" about a destination nobody named.
/// That is reviewer finding F1's exact shape in a new operation.
#[test]
fn a_source_connection_check_reports_no_destination_row_at_all() {
    let pod: BTreeSet<CheckId> = pf::pod_rows(PreflightOperation::SourceConnection)
        .iter()
        .map(|r| r.id)
        .collect();
    assert!(
        pod.contains(&CheckId::ConnectionCredentialProjected),
        "it dials, so the connection credential has to be projected"
    );
    assert!(!pod.contains(&CheckId::DestinationCredentialProjected));
    assert!(!pod.contains(&CheckId::TargetCredentialProjected));

    let all: BTreeSet<CheckId> = pf::controller_rows(PreflightOperation::SourceConnection)
        .iter()
        .map(|r| r.id)
        .collect();
    assert_eq!(
        all,
        [
            CheckId::ConnectionResolved,
            CheckId::ConnectionClusterIdentity,
            CheckId::ConfigurationPolicy,
            CheckId::ConfigurationEgress,
            CheckId::RunnerImage,
            CheckId::RunnerPod,
            CheckId::ConnectionCredentialProjected,
        ]
        .into_iter()
        .collect::<BTreeSet<CheckId>>(),
        "D2 §6.3's Backup catalogue minus every row about what the connection would be USED for"
    );
    for absent in [
        CheckId::DestinationResolved,
        CheckId::SignerRostered,
        CheckId::ConnectionTopicsReadable,
    ] {
        assert!(
            !all.contains(&absent),
            "`{absent}` is a claim about something this operation does not name"
        );
    }

    // REVIEW F3 / MUTANT R1. `unrendered_job_rows` is the OTHER list of
    // Job-sourced rows — the one used when no check plan could be rendered at
    // all, which is the console's commonest failure path (a connection that
    // does not resolve). The reviewer's mutant added
    // `connection.topicsDescribable` there and SURVIVED the whole suite: §3.2's
    // claim was held on three sides and not on this fourth one, so a future
    // edit could publish `topicsDescribable unknown/BlockedByPrerequisite` on a
    // check that named no topic and every other row would stay green.
    assert_eq!(
        pf::unrendered_job_rows(PreflightOperation::SourceConnection),
        [CheckId::RunnerContract, CheckId::ConnectionAuthenticated]
            .into_iter()
            .collect::<BTreeSet<CheckId>>(),
        "the rows a connectivity check would have asked the Job for are the two it emits, and \
         `connection.topicsDescribable` is not one of them on this path either"
    );
    // The unrendered list can never exceed what a rendered plan would ask for:
    // a row listed here and never emitted is a permanent blocking `unknown`.
    assert!(
        pf::unrendered_job_rows(PreflightOperation::SourceConnection)
            .is_subset(&job_rows(&runner_source_connection_request(), false))
    );
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
    let expected = unrendered_job_rows(PreflightOperation::Backup);
    let checks = assemble(
        pod,
        vec![runner_row(
            CheckId::ConnectionAuthenticated,
            CheckState::Ready,
            CheckCode::Authenticated,
            Gating::Blocking,
        )],
        blocked,
        &expected,
        &BTreeSet::new(),
        now(),
    );
    for id in expected.iter().copied() {
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
    let expected = unrendered_job_rows(PreflightOperation::Restore);
    let checks = assemble(
        Vec::new(),
        Vec::new(),
        true,
        &expected,
        &BTreeSet::new(),
        now(),
    );
    for id in expected.iter().copied() {
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
    let expected = unrendered_job_rows(PreflightOperation::Restore);
    let relayed: Vec<CheckOutcome> = expected
        .iter()
        .copied()
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
    let checks = assemble(Vec::new(), relayed, false, &expected, &skip, now());
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
        controller,
        relayed,
        false,
        &unrendered_job_rows(PreflightOperation::Restore),
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
    let (result, _) = result_for(&[soon, bytes], None);
    assert_eq!(result.expires_at, Some(now() + Duration::minutes(5)));
}

#[test]
fn a_facts_bearing_row_keeps_its_facts_in_the_message() {
    let row = signer_rostered_row(
        PreflightOperation::Backup,
        &approver_roster(None),
        Some(RUNNER_KEY_ID),
        now(),
    );
    let entry = entry_of(&row);
    assert!(
        entry
            .message
            .as_deref()
            .unwrap_or_default()
            .contains(&format!("signerKeyId={RUNNER_KEY_ID}")),
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
    assert_eq!(result_for(&[], None).0.state, "unknown");
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

/// One terminal `Preflight` in the LIST the collector issues — D2 §4.3's
/// `gc.rs`. `basis` is the instant retention counts from: `result.expiresAt`
/// when the check produced a verdict, else `observedAt`.
fn listed_preflight(name: &str, uid: &str, phase: &str, basis: DateTime<Utc>) -> Value {
    json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "Preflight",
        "metadata": {"name": name, "namespace": NS, "uid": uid, "resourceVersion": "9"},
        "spec": {"request": backup_request()},
        "status": {
            "phase": phase, "reason": "Valid",
            "observedAt": basis.to_rfc3339(),
            "result": {"state": "ready", "expiresAt": basis.to_rfc3339(), "checks": []}
        }
    })
}

/// The `GET …/preflights` route every TERMINAL pass now hits, because the
/// terminal branch runs the collector before it revalidates.
fn gc_list(items: Vec<Value>) -> Route {
    Route {
        method: "GET",
        path_suffix: "/preflights",
        status: 200,
        body: json!({
            "apiVersion": "logweir.dev/v1alpha1", "kind": "PreflightList",
            "metadata": {"resourceVersion": "9"}, "items": items
        })
        .to_string(),
    }
}

/// The collector's listing with nothing collectable in it — what a test about
/// something OTHER than garbage collection wants.
fn gc_list_empty() -> Route {
    gc_list(vec![listed_preflight("pf-1", PF_UID, "Completed", now())])
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
            roster(RUNNER_KEY_ID, allowed).to_string(),
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
    let relayed = relay_from(&runner_pinned_ids(
        "a_readiness_check_reports_every_row_it_owns",
    ));
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

    // …AND SCOPES, which this test is named for and did not assert. D2 §6.3's
    // acceptance sentence is "with check time and scope"; the live run
    // published six `notReady` rows with `scope: null`.
    let mut authorities: BTreeSet<&str> = BTreeSet::new();
    for c in status["result"]["checks"].as_array().expect("checks") {
        let scope = c["scope"]
            .as_object()
            .unwrap_or_else(|| panic!("`{}` carries no scope: {c}", c["id"]));
        assert!(
            scope.get("kind").and_then(Value::as_str).is_some(),
            "`{}`'s scope names no kind",
            c["id"]
        );
        assert!(
            scope.get("name").and_then(Value::as_str).is_some(),
            "`{}`'s scope names no object",
            c["id"]
        );
        assert!(
            scope.get("uid").and_then(Value::as_str).is_some(),
            "`{}`'s scope names no UID: {c}",
            c["id"]
        );
        authorities.insert(c["authority"].as_str().unwrap_or_default());
    }
    assert_eq!(
        authorities,
        BTreeSet::from(["checkJob", "controller", "podStatus"]),
        "one scoped row per authority, which is what the fix has to cover"
    );
    assert_eq!(
        check_entry(&status, "signer.rostered")["scope"]["kind"],
        "TrustRoster"
    );
    assert_eq!(
        check_entry(&status, "runner.pod")["scope"]["kind"],
        "Pod",
        "a pod-status row is about the Pod the kubelet reported on"
    );
    assert_eq!(
        check_entry(&status, "runner.contract")["scope"]["kind"],
        "Job",
        "a check-Job row that named no object of its own is about the Job"
    );
    // D2-SIGNERID-REDACTED, end to end: the key id the pod relayed reaches the
    // roster comparison and the published message as itself.
    assert!(
        check_entry(&status, "signer.rostered")["message"]
            .as_str()
            .unwrap_or_default()
            .contains(RUNNER_KEY_ID),
        "the key id an operator must roster was not published: {}",
        check_entry(&status, "signer.rostered")["message"]
    );
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
    // The row an operator reads names the object the kubelet reported on…
    assert_eq!(row["scope"]["kind"], "Pod");
    assert_eq!(row["scope"]["uid"], POD_UID);
    // …and the kubelet's own sentence keeps the Secret's NAME (D2 §6.5: a
    // Secret name is a public reference). The live S14a/S14b rows read
    // `secret [redacted] not found` and `key password [redacted]]] Secret
    // [redacted]`.
    assert!(
        row["message"]
            .as_str()
            .unwrap_or_default()
            .contains("kafka-src"),
        "the Secret an operator has to create is not named: {}",
        row["message"]
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
    // TWO ROUTES, AND THAT IS THE WHOLE COST. The collector's listing (D2
    // §4.3's `gc.rs`, which every terminal pass runs) and the status patch. A
    // revalidation that RE-RESOLVED the cluster would make an expired verdict
    // cost a full resolution pass — the `KafkaCluster`, the destination, the
    // roster — and the clock is enough; the double panics on any of them.
    let routes = vec![
        gc_list_empty(),
        route("PATCH", "/pf-1/status", echo("Preflight", "pf-1")),
    ];
    let (status, recorder) = reconcile_with(&object, routes).await;
    assert_eq!(status["result"]["state"], "unknown");
    assert!(
        status["message"]
            .as_str()
            .unwrap_or_default()
            .contains("expired"),
        "a stale flag with no reason is the defect PLAT-03.2 is about"
    );
    let seen = recorder.lock().expect("recorder");
    assert_eq!(
        seen.len(),
        2,
        "the GC listing and the status patch: {seen:?}"
    );
    assert!(
        seen.iter()
            .any(|r| r.method == "GET" && r.uri.contains("/preflights")),
        "the first call is the collector's listing: {seen:?}"
    );
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
    routes.push(gc_list_empty());
    routes.push(route("PATCH", "/pf-1/status", echo("Preflight", "pf-1")));
    let (status, _) = reconcile_with(&object, routes).await;
    assert_eq!(status["result"]["state"], "unknown");
    let message = status["message"].as_str().unwrap_or_default();
    assert!(
        message.contains("referentChanged:BackupDestination/primary"),
        "the reason names the object that moved; got {message}"
    );
}

// ===========================================================================
// D2 §4.3 `gc.rs` / §6.x retention — the collector, wired by W11
// ===========================================================================

/// A terminal `Preflight` that is only there to make the reconcile reach the
/// terminal branch. Its own verdict is `notReady`, which nothing revalidates,
/// so the ONLY calls a pass makes are the collector's.
fn terminal_preflight() -> Preflight {
    let mut object = preflight(backup_request());
    object.status = Some(
        serde_json::from_value(json!({
            "phase": "Completed",
            "reason": "NotReady",
            "binding": {"inputsDigest": "sha256:aa", "referents": []},
            "result": {"state": "notReady", "checks": []}
        }))
        .expect("the status fixture parses"),
    );
    object
}

/// **The age rule, and the UID precondition.** A terminal `Preflight` past
/// `expiresAt + retentionSeconds` (one hour by default) is deleted; one inside
/// the window is not; a `Running` one is not, however old it is.
///
/// NO COHORT RULE HERE, and that is deliberate: a `Preflight` is about ONE plan
/// of ONE operation, so there is no "newest five for this connection" to be
/// outside of, and inventing one would delete the only readiness verdict a
/// restore has.
///
/// MUTANTS: (a) drop the terminal guard in `gc_row` and the ancient `Running`
/// object goes; (b) drop `preconditions` from `DeleteParams` and the body
/// assertion fails; (c) use `observedAt` in preference to `result.expiresAt`
/// and the object that expired two hours ago but was observed three hours ago
/// is collected on the wrong basis.
#[tokio::test]
async fn the_collector_deletes_only_expired_terminal_preflights_and_names_their_uid() {
    let routes = vec![
        gc_list(vec![
            // Expired at `now - 2 h`: past the default 3 600 s window.
            listed_preflight("pf-old", "uid-old", "Completed", now() - Duration::hours(2)),
            // Expired a minute ago: INSIDE the window.
            listed_preflight("pf-1", PF_UID, "Completed", now() - Duration::minutes(1)),
            // A day old and still RUNNING. Its pod holds the only copy of a
            // relay nobody has read.
            listed_preflight(
                "pf-running",
                "uid-running",
                "Running",
                now() - Duration::hours(24),
            ),
        ]),
        Route {
            method: "DELETE",
            path_suffix: "/preflights/pf-old",
            status: 200,
            body: json!({"kind": "Status", "status": "Success"}).to_string(),
        },
        route("PATCH", "/pf-1/status", echo("Preflight", "pf-1")),
    ];
    let (client, recorder, bodies) = mock_client_recording_bodies(routes);
    let ctx = context(client);
    let cache = weirkeeper::check::policy::PolicyCache::new();
    pf::reconcile_preflight(&terminal_preflight(), &ctx, &cache, now())
        .await
        .expect("the reconcile completes");

    let deletes: Vec<String> = recorder
        .lock()
        .expect("recorder")
        .iter()
        .filter(|r| r.method == "DELETE")
        .map(|r| r.uri.clone())
        .collect();
    assert_eq!(
        deletes.len(),
        1,
        "the in-window verdict and the RUNNING check are untouched: {deletes:?}"
    );
    assert!(deletes[0].contains("/preflights/pf-old"));
    let body: Value = bodies
        .lock()
        .expect("bodies")
        .iter()
        .find(|b| b.method == "DELETE")
        .and_then(|b| serde_json::from_str(&b.body).ok())
        .unwrap_or_else(|| panic!("the DELETE carried a body"));
    assert_eq!(
        body["preconditions"]["uid"], "uid-old",
        "a name is not an identity: without the UID precondition a same-named replacement \
         created between the LIST and the DELETE is what goes. body={body}"
    );
}

/// **The per-pass cap.** Twenty-five expired checks, one pass, twenty deletes.
///
/// MUTANT: remove the `.take(GC_MAX_DELETES_PER_PASS)` and the twenty-first
/// DELETE has no route, so the double panics.
#[tokio::test]
async fn one_preflight_collection_pass_is_capped() {
    let mut items = Vec::new();
    let mut routes = Vec::new();
    for i in 0..25u32 {
        items.push(listed_preflight(
            &format!("pf-{i:02}"),
            &format!("uid-{i:02}"),
            "Completed",
            now() - Duration::hours(2) - Duration::minutes(i64::from(i)),
        ));
        routes.push(Route {
            method: "DELETE",
            path_suffix: leak(format!("/preflights/pf-{i:02}")),
            status: 200,
            body: json!({"kind": "Status", "status": "Success"}).to_string(),
        });
    }
    routes.insert(0, gc_list(items));
    routes.push(route("PATCH", "/pf-1/status", echo("Preflight", "pf-1")));
    let (client, recorder, _b) = mock_client_recording_bodies(routes);
    let ctx = context(client);
    let cache = weirkeeper::check::policy::PolicyCache::new();
    pf::reconcile_preflight(&terminal_preflight(), &ctx, &cache, now())
        .await
        .expect("the reconcile completes");
    let deletes = recorder
        .lock()
        .expect("recorder")
        .iter()
        .filter(|r| r.method == "DELETE")
        .count();
    assert_eq!(
        deletes,
        pf::GC_MAX_DELETES_PER_PASS,
        "one pass deletes at most twenty"
    );
}

/// **A refused DELETE is not a reconcile error.** Garbage collection is never
/// the reason a check's own reconcile fails: a 409 (the UID precondition
/// refusing a replacement) or a 404 (somebody got there first) is logged and
/// the pass continues.
#[tokio::test]
async fn a_refused_preflight_collection_is_not_a_reconcile_error() {
    let routes = vec![
        gc_list(vec![listed_preflight(
            "pf-old",
            "uid-old",
            "Completed",
            now() - Duration::hours(2),
        )]),
        Route {
            method: "DELETE",
            path_suffix: "/preflights/pf-old",
            status: 409,
            body: json!({"kind": "Status", "status": "Failure", "code": 409,
                         "message": "the UID in the precondition does not match"})
            .to_string(),
        },
    ];
    let (client, _r, _b) = mock_client_recording_bodies(routes);
    let ctx = context(client);
    let cache = weirkeeper::check::policy::PolicyCache::new();
    pf::reconcile_preflight(&terminal_preflight(), &ctx, &cache, now())
        .await
        .expect("a refused delete is not a reconcile error");
}

/// **The pure rule, on its own.** Both bounds of `expired_terminal`, and the
/// total order two passes over the same input have to agree on.
#[test]
fn expired_terminal_preflights_are_exactly_the_ones_past_the_window() {
    let row = |name: &str, uid: &str, age: Duration| pf::TerminalPreflight {
        name: name.to_string(),
        uid: uid.to_string(),
        basis: now() - age,
    };
    let all = vec![
        row("a", "uid-a", Duration::hours(2)),
        row("b", "uid-b", Duration::minutes(59)),
        row("c", "uid-c", Duration::hours(9)),
    ];
    assert_eq!(
        pf::expired_terminal(&all, 3600, now()),
        vec!["uid-a".to_string(), "uid-c".to_string()],
        "newest-first over the ones past the window (`a` expired 2 h ago, `c` 9 h ago), and \
         `b` is inside it"
    );
    // A window wide enough keeps everything — the rule is the window and not
    // the sort.
    assert!(pf::expired_terminal(&all, 86_400, now()).is_empty());
    // And the order is total: equal instants tie-break on the UID.
    let tied = vec![
        row("z", "uid-z", Duration::hours(2)),
        row("y", "uid-y", Duration::hours(2)),
    ];
    assert_eq!(
        pf::expired_terminal(&tied, 3600, now()),
        vec!["uid-y".to_string(), "uid-z".to_string()]
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
        "the weirkeeper ClusterRole grants `delete` only on the two transient check kinds, and \
         only from the collector (D2 §4.3); a CANCEL is a collapsed deadline and never a delete \
         of anything"
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
            roster(RUNNER_KEY_ID, vec!["target-id"]).to_string(),
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

/// PLAT-03.2's "expired approval", end to end — and the shape that proves
/// **PREFLIGHT-APPROVAL-ROSTER** is closed.
///
/// The old version of this test planted an expired approver key on the
/// `TrustRoster` and an `Approval` whose status said `verified: true`, and
/// asserted `ApprovalExpired`. That WAS the defect: the preflight was deciding
/// expiry from the roster while `controllers::approval` decides it from the
/// trust policy that governs the namespace, and the two can disagree — on
/// `lab-refresh-4` they did.
///
/// It is inverted here. The roster's approver key is **wide open** (no
/// `notAfter` at all), so the roster cannot produce this verdict; the
/// `Approval` carries what the controller writes under a `TrustPolicy` that
/// has expired the key — `Verified=False`, reason `KeyIdExpired`. The only way
/// the row can read `ApprovalExpired` is by consuming the Approval's own
/// verdict, and the only way the restore can be refused is if that row still
/// gates.
///
/// It also removes the fixture problem the harness hit: the row no longer
/// needs a roster carrying an expired approver key, so PLAT-03.2's live test
/// no longer needs the shared, immutable `TrustRoster/default` recreated.
#[tokio::test]
async fn an_approval_expired_under_a_policy_refuses_the_restore_before_it_is_submitted() {
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
        // WHAT THE APPROVAL CONTROLLER WRITES when the RESOLVED TRUST POLICY
        // has expired the key that verified this approval (D3 W10).
        "status": {
            "verified": false,
            "matchedKeyId": "approver-1",
            "conditions": [{
                "type": "Verified", "status": "False", "reason": "KeyIdExpired",
                "message": "the approval verified under key id approver-1, whose notAfter \
                            2026-09-16T11:00:00Z has passed; a key past notAfter does not \
                            authorise anything"
            }]
        }
    });
    // THE ROSTER'S APPROVER KEY IS WIDE OPEN. If this row were still deriving
    // expiry from the roster it would read `ready`, and this test would fail —
    // which is exactly what makes it a proof rather than a restatement.
    let open_roster = json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "TrustRoster",
        "metadata": {"name": "default", "uid": ROSTER_UID, "generation": 4},
        "spec": {
            "approverKeys": [{
                "keyId": "approver-1",
                "spkiPem": "-----BEGIN PUBLIC KEY-----\nAA\n-----END PUBLIC KEY-----"
            }],
            "signingKeys": [{"keyId": RUNNER_KEY_ID, "spkiPem": "-----BEGIN PUBLIC KEY-----\nBB\n-----END PUBLIC KEY-----"}],
            "allowedClusterIds": ["target-id"]
        }
    });

    let mut routes = vec![
        route("GET", "/trustrosters/default", open_roster.to_string()),
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
    let (status, recorder) = reconcile_with(&preflight(request), routes).await;

    // REVIEW P1 / MUTANT X3. THE APPROVAL IS READ IN THE PREFLIGHT'S OWN
    // NAMESPACE, AND THIS IS THE ONLY ROW THAT CAN SAY SO.
    //
    // `weirkeeper::testing::answer` matches a route by PATH SUFFIX, so
    // `/approvals/ap-1` registered here answers a request for ANY namespace's
    // `ap-1` just as well. The reviewer's mutant — `Api::namespaced(client,
    // "default")` in the Approval read — therefore passed the whole file. The
    // product code is right; nothing could tell.
    //
    // It matters here more than anywhere else in this file: this commit makes
    // the `Approval` the SOLE authority for a blocking row, so a namespace
    // substitution would let a restore be authorised by an approval in a
    // namespace the requester does not own. The double records full request
    // targets, so the namespace is recoverable from the log even though the
    // matcher ignores it.
    let seen = recorder.lock().expect("recorder");
    let approval_reads: Vec<&String> = seen
        .iter()
        .filter(|r| r.method == "GET" && r.uri.contains("/approvals/ap-1"))
        .map(|r| &r.uri)
        .collect();
    assert!(
        !approval_reads.is_empty(),
        "the reconcile read the Approval at all"
    );
    assert!(
        approval_reads
            .iter()
            .all(|uri| uri.contains(&format!("/namespaces/{NS}/approvals/ap-1"))),
        "the Approval is read in the Preflight's own namespace and no other: {approval_reads:?}"
    );
    drop(seen);

    assert_eq!(
        status["result"]["state"], "notReady",
        "a blocking approval row that says expired refuses the restore"
    );
    let approval = check_entry(&status, "approval.state");
    assert_eq!(approval["code"], "ApprovalExpired");
    assert_eq!(approval["state"], "notReady");
    assert_eq!(approval["gating"], "blocking");
    assert!(
        approval["message"]
            .as_str()
            .unwrap_or_default()
            .contains("notAfter"),
        "the controller's own sentence reaches the row: {approval}"
    );
    // THE ADVISORY ROW MAKES NO CLAIM ABOUT A WINDOW THAT IS NOT PUBLISHED.
    // The key expired, so the Approval matched none and published no
    // `status.approverKeyWindow`; `unknown` is the answer and `ready` would be
    // APPROVAL-KEY-WINDOW-UNPUBLISHED wearing a green badge.
    let validity = check_entry(&status, "approval.keyValidity");
    assert_eq!(validity["code"], "ApproverKeyWindowUnknown");
    assert_eq!(validity["state"], "unknown");
    assert_eq!(validity["gating"], "advisory");
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

/// **APPROVAL-KEY-WINDOW-UNPUBLISHED, end to end.** The window the `Approval`
/// publishes reaches the published rows: the warning, and the cap on both.
///
/// The pure rows above prove the arithmetic; this one proves the WIRING — that
/// `status.approverKeyWindow` is read off the object at all, and that the
/// capped expiry survives into `status.checks[]` and the aggregate's own
/// `expiresAt`. A reader of the pure tests alone could not tell a controller
/// that never looked at the field from one that did.
///
/// THE ROSTER IS WIDE OPEN HERE TOO, for the same reason the expiry row above
/// inverts it: nothing in this verdict can have come from `TrustRoster/default`.
///
/// KILLS: "never read `status.approverKeyWindow`" — the shipped behaviour;
/// "cap the rows and publish the uncapped `expiresAt`"; "let the advisory
/// warning drag the aggregate to `notReady`", which would refuse a restore over
/// a key that is valid right now.
#[tokio::test]
async fn a_published_key_window_reaches_the_warning_and_the_cap_on_both_rows() {
    let job = job_name(CheckPlanKind::RestorePreflight);
    let plan = plan_yaml("restore-", "s3-bucket");
    let plan_hash = logweir_core::ids::sha256_prefixed(plan.as_bytes());
    // The restore's own deadline is an hour out; the key's window closes in
    // twenty minutes. D2 §6.3: `notAfter` < now + `deadlineSeconds`.
    let not_after = now() + Duration::minutes(20);
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
        // WHAT THE APPROVAL CONTROLLER WRITES under a TrustPolicy whose
        // approver key is valid now and closes inside this restore's deadline.
        "status": {
            "verified": true,
            "matchedKeyId": "approver-1",
            "approverKeyWindow": {
                "keyId": "approver-1",
                "notBefore": (now() - Duration::days(30)).to_rfc3339(),
                "notAfter": not_after.to_rfc3339(),
            },
            "conditions": [{
                "type": "Verified", "status": "True", "reason": "Verified",
                "message": "the DSSE signature over spec.approvalBytes verified under \
                            GovernedApproval key approver-1 (trustSource=org-default)"
            }]
        }
    });
    let open_roster = json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "TrustRoster",
        "metadata": {"name": "default", "uid": ROSTER_UID, "generation": 4},
        "spec": {
            "approverKeys": [{
                "keyId": "approver-1",
                "spkiPem": "-----BEGIN PUBLIC KEY-----\nAA\n-----END PUBLIC KEY-----"
            }],
            "signingKeys": [{"keyId": RUNNER_KEY_ID, "spkiPem": "-----BEGIN PUBLIC KEY-----\nBB\n-----END PUBLIC KEY-----"}],
            "allowedClusterIds": ["target-id"]
        }
    });

    let mut routes = vec![
        route("GET", "/trustrosters/default", open_roster.to_string()),
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
    let (status, _recorder) = reconcile_with(&preflight(request), routes).await;

    let state = check_entry(&status, "approval.state");
    assert_eq!(state["code"], "ApprovalVerified");
    assert_eq!(state["state"], "ready");

    let validity = check_entry(&status, "approval.keyValidity");
    assert_eq!(
        validity["code"], "ApproverKeyExpiresBeforeDeadline",
        "the key closes inside the restore's own deadline: {validity}"
    );
    assert_eq!(validity["state"], "notReady");
    assert_eq!(
        validity["gating"], "advisory",
        "D2 §6.3 gates this row `A`, and §6.4: advisory notReady appears as warnings"
    );
    assert!(
        validity["remedy"]
            .as_str()
            .unwrap_or_default()
            .contains("Rotate"),
        "the warning names the action: {validity}"
    );

    // THE CAP, ON BOTH ROWS, IN THE PUBLISHED RECORD.
    for id in ["approval.state", "approval.keyValidity"] {
        let expires: DateTime<Utc> = check_entry(&status, id)["expiresAt"]
            .as_str()
            .and_then(|t| DateTime::parse_from_rfc3339(t).ok())
            .map(|t| t.with_timezone(&Utc))
            .unwrap_or_else(|| panic!("`{id}` publishes an expiresAt: {status}"));
        assert!(
            expires <= not_after,
            "`{id}` must not be re-checked after the key's notAfter ({not_after}): {expires}"
        );
        assert!(
            expires <= now() + Duration::minutes(10),
            "`{id}` keeps the ten-minute ceiling too"
        );
    }

    // AND THE WARNING IS A WARNING. An approval that is valid right now is not
    // refused because its key closes before a deadline the restore may never
    // reach.
    assert_ne!(
        status["result"]["state"], "notReady",
        "an advisory row never refuses: {}",
        status["result"]
    );
}

/// The `Backup` a restore points at: `Succeeded`, over the plan's own topic,
/// carrying `frozen` as its `status.destination.locationDigest` when it has
/// one. `None` is a LEGACY recovery point — an inline-`archive` run, or one an
/// older controller froze.
fn recovery_point_object(frozen: Option<&str>) -> Value {
    let mut object = json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "Backup",
        "metadata": {
            "name": "nightly-1", "namespace": NS, "uid": BACKUP_UID,
            "generation": 1, "resourceVersion": "700"
        },
        "spec": {
            "sourceRef": {"name": "source"},
            "topics": ["orders"],
            "triggeredBy": "manual",
            "deadlineSeconds": 3600,
            "archive": {"url": "s3://s3-bucket"}
        },
        "status": {"phase": "Succeeded"}
    });
    if let Some(digest) = frozen {
        object["status"]["destination"] = json!({
            "name": "primary",
            "uid": DEST_UID,
            "generation": 3,
            "locationDigest": digest
        });
    }
    object
}

/// The digest the `primary` fixture resolves to — what a destination-backed
/// restore check compares a recovery point against. Taken from the resolver,
/// never spelled out here: a literal would drift the day the canonical form
/// changes and this test would keep passing about the wrong thing.
fn primary_location_digest() -> String {
    let object = serde_json::from_value(backup_destination("primary"))
        .expect("the fixture is a BackupDestination");
    weirkeeper::destination::resolve(
        &object,
        DestinationRole::ArchiveRead,
        &weirkeeper::check::policy::Policy::defaults(),
    )
    .expect("the fixture resolves")
    .location_digest
}

/// A draft restore over `point`, reconciled in full.
///
/// The EVIDENCE destination sits in a different bucket from the source one, so
/// the two resolve to different `locationDigest`s. That is what makes "the
/// expected digest is the ARCHIVE destination's" a testable claim rather than
/// a coincidence of one shared fixture bucket.
async fn restore_over_recovery_point(point: Value) -> Value {
    let job = job_name(CheckPlanKind::RestorePreflight);
    let mut evidence = backup_destination("evidence");
    evidence["spec"]["storage"]["bucket"] = json!("evidence-bucket");
    let mut routes = vec![
        route(
            "GET",
            "/trustrosters/default",
            roster(RUNNER_KEY_ID, vec!["target-id"]).to_string(),
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
        route("GET", "/backupdestinations/evidence", evidence.to_string()),
    ];
    routes.push(route("GET", "/backups/nightly-1", point.to_string()));
    // The point's own `spec.sourceRef`: the check reads the cluster the
    // recovery point was captured from, to bind `plan.bindings` to it.
    routes.push(route(
        "GET",
        "/kafkaclusters/source",
        kafka_cluster("source", Some("prod-id")).to_string(),
    ));
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
    let request = restore_request(json!({
        "recoveryPointRef": {"name": "nightly-1", "uid": BACKUP_UID}
    }));
    let (status, _) = reconcile_with(&preflight(request), routes).await;
    status
}

/// **THE ROW IS WIRED TO THE OBJECT, NOT ONLY TO ITS OWN FACT TYPE.**
///
/// # The defect this closes
///
/// `recovery_point_row` could compare digests for a year and report nothing:
/// before `Backup.status.destination` existed the reconciler passed
/// `location_digest: None` and `expected_location_digest: None` with a comment
/// saying why, and every restore preflight answered `RecoveryPointSucceeded`.
/// This test reconciles three real recovery points through the routes and reads
/// the published verdict, so the wiring — a read of the point's frozen block
/// and a read of the destination THIS check resolved — is what is under test.
///
/// KILLS, one mutant each:
/// * `location_digest` back to `None` (every case becomes `ready`);
/// * `expected_location_digest` back to `None` (mismatch and legacy both
///   become `ready`);
/// * `expected_location_digest` taken from the EVIDENCE destination instead of
///   the archive one;
/// * the frozen digest re-resolved from the live `BackupDestination` rather
///   than read off the point, which makes the moved point look settled.
#[tokio::test]
async fn the_recovery_point_location_row_reads_the_frozen_destination_off_the_object() {
    let here = primary_location_digest();
    let there = "sha256:2222222222222222222222222222222222222222222222222222222222222222";
    assert_ne!(here, there, "the premise: two different locations");

    let same = restore_over_recovery_point(recovery_point_object(Some(&here))).await;
    let row = check_entry(&same, "recoveryPoint.state");
    assert_eq!(
        row["code"], "RecoveryPointSucceeded",
        "a point frozen at the destination this check resolved is the point"
    );
    assert_eq!(row["state"], "ready");

    let moved = restore_over_recovery_point(recovery_point_object(Some(there))).await;
    let row = check_entry(&moved, "recoveryPoint.state");
    assert_eq!(
        row["code"], "RecoveryPointLocationMismatch",
        "D2 §6.3's code is REACHABLE now, and this is the case that reaches it"
    );
    assert_eq!(row["state"], "notReady");
    assert_eq!(
        moved["result"]["state"], "notReady",
        "a blocking notReady row makes the whole verdict notReady"
    );
    let message = row["message"].as_str().unwrap_or_default();
    assert!(
        message.contains(&here) && message.contains(there),
        "the operator is told BOTH locations: {message}"
    );

    let legacy = restore_over_recovery_point(recovery_point_object(None)).await;
    let row = check_entry(&legacy, "recoveryPoint.state");
    assert_eq!(
        row["code"], "RecoveryPointLocationUnknown",
        "a point that publishes no location is not a point proved to be here"
    );
    assert_eq!(row["state"], "unknown");
    assert_ne!(
        legacy["result"]["state"], "ready",
        "the upgrade note in one assertion: an old recovery point never gets a \
         green verdict it did not earn"
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
/// say so about code that does not exist. The planted mutants are an `import`
/// of `Preflight` into `controllers/restore.rs` and one into
/// `backup_execution.rs`.
///
/// # It globs, and the exclusion list is the whole argument
///
/// Reviewer finding **F5**: it used to name four reconcilers and two runner
/// directories. `crates/weirkeeper/src/backup_execution.rs` — where W10 works —
/// was not scanned, nor were `verification.rs`, `retention.rs`, `slot.rs`,
/// `cadence.rs` or `controllers/kafka_cluster.rs`, and a file added in any of
/// them could have consulted a `Preflight` with the guard still green. So the
/// scan now covers **everything** under `crates/weirkeeper/src/` and
/// `crates/logweir/src/`, and the exclusions are enumerated here rather than
/// implied:
///
/// * `controllers/preflight.rs` — the controller itself;
/// * `crates/weirkeeper/src/check/` and `crates/logweir/src/check/` — the
///   shared check framework and the runner's own `check run`, which are what a
///   `Preflight` is EXECUTED BY and not what it is read from;
/// * `crates/weirkeeper/src/crds/` — the kind's own type has to be declarable.
///
/// The module path `preflight` is forbidden too, not only the type name: a
/// re-export (`pub type Pf = preflight::Preflight;`) would otherwise reach the
/// kind through a name the old list did not carry.
#[test]
fn no_execution_path_reads_preflight_or_discovery() {
    let root = repo_root();
    let mut files = rust_files_under(&root.join("crates/weirkeeper/src"));
    files.extend(rust_files_under(&root.join("crates/logweir/src")));
    let excluded = |p: &std::path::Path| {
        let text = p.to_string_lossy().replace('\\', "/");
        text.ends_with("crates/weirkeeper/src/controllers/preflight.rs")
            || text.contains("crates/weirkeeper/src/check/")
            || text.contains("crates/weirkeeper/src/crds/")
            || text.contains("crates/logweir/src/check/")
            // The REGISTRATION POINT and the module declarations. `main.rs`
            // pushes the reconciler and `controllers/mod.rs` / `lib.rs` declare
            // it; neither is an execution path, and a controller that could not
            // be registered could not exist.
            || text.ends_with("crates/weirkeeper/src/main.rs")
            || text.ends_with("crates/weirkeeper/src/controllers/mod.rs")
            || text.ends_with("crates/weirkeeper/src/lib.rs")
            // The `TopicDiscovery` reconciler is not an execution path either
            // — it is the OTHER interactive check controller, and D-SEAMS S2
            // is about a RUN reading a discovery result, not about the
            // controller that produces one.
            || text.ends_with("crates/weirkeeper/src/controllers/topic_discovery.rs")
    };
    let scanned: Vec<std::path::PathBuf> = files.into_iter().filter(|p| !excluded(p)).collect();
    assert!(
        scanned.len() >= 40,
        "the scan found only {} files; it has gone quiet and would pass vacuously",
        scanned.len()
    );
    // The four files the old guard named, by name, so a refactor that moved
    // them out from under the glob is a failure rather than a silent pass.
    for must in [
        "crates/weirkeeper/src/controllers/restore.rs",
        "crates/weirkeeper/src/controllers/backup.rs",
        "crates/weirkeeper/src/controllers/backup_schedule.rs",
        "crates/weirkeeper/src/controllers/approval.rs",
        "crates/weirkeeper/src/backup_execution.rs",
    ] {
        assert!(
            scanned
                .iter()
                .any(|p| p.to_string_lossy().replace('\\', "/").ends_with(must)),
            "{must} is not in the scan"
        );
    }

    const FORBIDDEN: [&str; 4] = [
        "Preflight",
        "TopicDiscovery",
        "preflights",
        "topicdiscoveries",
    ];
    for file in &scanned {
        let text = std::fs::read_to_string(file)
            .unwrap_or_else(|e| panic!("could not read {}: {e}", file.display()));
        for (n, line) in text.lines().enumerate() {
            let code = code_of(line);
            for needle in FORBIDDEN {
                assert!(
                    !names_word(&code, needle),
                    "{}:{} names `{needle}` in code: {line}\n\
                     D2 §6.8: the preflight replaces no execution-time guard, and an execution \
                     path that could read one is an execution path a green preview could bypass.",
                    file.display(),
                    n + 1
                );
            }
            // The MODULE PATH, so a re-export cannot smuggle the type in under
            // another name (`pub type Pf = preflight::Preflight;` then
            // `crds::Pf` at the call site). As a PATH SEGMENT and not a word:
            // `controllers/restore.rs` carries `TOPIC_PREFLIGHT_KEY_PREFIX =
            // "topic-preflight="`, phase 0's broker-config observation, which
            // has nothing to do with this kind.
            assert!(
                !names_module_path(&code, "preflight"),
                "{}:{} reaches the `preflight` MODULE: {line}",
                file.display(),
                n + 1
            );
        }
    }
}

/// A line with its trailing `//` comment removed — and never a string's
/// contents mistaken for one.
///
/// `line.split("//").next()` blanked everything after a `//` INSIDE a string
/// literal, which is a hole a forbidden name could sit in
/// (`let s = "https://x"; use crate::crds::preflight::Preflight;` on one line).
/// This walks the line and only treats `//` as a comment when it is outside a
/// double-quoted run.
fn code_of(line: &str) -> String {
    let bytes: Vec<char> = line.chars().collect();
    let mut out = String::new();
    let mut in_string = false;
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        if in_string {
            if c == '\\' {
                out.push(c);
                if i + 1 < bytes.len() {
                    out.push(bytes[i + 1]);
                    i += 2;
                    continue;
                }
            } else if c == '"' {
                in_string = false;
            }
            out.push(c);
        } else if c == '"' {
            in_string = true;
            out.push(c);
        } else if c == '/' && i + 1 < bytes.len() && bytes[i + 1] == '/' {
            break;
        } else {
            out.push(c);
        }
        i += 1;
    }
    out
}

/// Whether `code` reaches a Rust module by path — `…preflight::`,
/// `use …preflight;` or `use …preflight as alias;`.
///
/// The third form is not decoration: `use crate::crds::preflight as pf;` then
/// `pf::Preflight` at the call site reaches the kind through two names the
/// word scan does not carry, and it SURVIVED the first version of this guard.
fn names_module_path(code: &str, module: &str) -> bool {
    let bytes: Vec<char> = code.chars().collect();
    let mut from = 0usize;
    while let Some(at) = code[from..].find(module) {
        let start = from + at;
        let end = start + module.len();
        let before = start == 0 || !(is_ident(bytes[start - 1]) || bytes[start - 1] == '-');
        let tail = &code[end..];
        let after =
            tail.starts_with("::") || tail.starts_with(';') || tail.trim_start().starts_with("as ");
        if before && after {
            return true;
        }
        from = start + 1;
    }
    false
}

fn repo_root() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("the repository root")
        .to_path_buf()
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
        let code = code_of(line);
        for needle in ["TopicDiscovery", "topicdiscoveries"] {
            assert!(
                !names_word(&code, needle),
                "preflight.rs:{} names `{needle}` in code. D-SEAMS S2: a discovery result is \
                 never an input to anything that runs.",
                n + 1
            );
        }
    }
}

/// **The one destructive call this file makes is the collector's** — the
/// mirror of
/// `topic_discovery_controller::the_reconciler_names_no_grant_this_role_does_not_hold`,
/// and review finding **F2**, fix round 1.
///
/// # Why this did not exist, and what it cost
///
/// The discovery controller got a source-level pin when the `delete` grant
/// landed; this file did not, and `manifest_lint`'s own narrowing was by TYPE
/// (`Api<Preflight>` may be deleted through) rather than by CALL SITE. The
/// reviewer planted a second `Api<Preflight>::delete` above `collect_expired`
/// — `api.delete(name, &DeleteParams::default())`, with **no UID
/// precondition** — and it survived every guard in the tree: `manifest_lint`
/// 29/29, this suite 82/82, `topic_discovery_controller` 54/54, `linkage`
/// 17/17.
///
/// A delete by name removes whatever holds the name. A `Preflight` deleted
/// without its UID is, in the window between a LIST and a DELETE, somebody
/// else's verdict about somebody else's plan.
///
/// `manifest_lint::every_delete_in_the_control_plane_is_the_check_collectors`
/// is the authoritative version and walks every file in the crate, so a NEW
/// file cannot slip through the gap this one closes for this file. Both exist
/// because a claim about this reconciler belongs beside this reconciler.
///
/// MUTANT: re-plant the reviewer's stray delete — anywhere in this file,
/// inside `collect_expired` or not — and the count below fails.
#[test]
fn the_preflight_reconciler_deletes_only_from_its_collector_and_only_by_uid() {
    let source = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/controllers/preflight.rs"),
    )
    .expect("the controller source is readable");
    // Prose is not code: this file's own doc comments talk about deleting.
    let code: String = source
        .lines()
        .filter(|l| {
            let t = l.trim_start();
            !(t.starts_with("//") || t.starts_with("///") || t.starts_with("*"))
        })
        .collect::<Vec<_>>()
        .join("\n");

    assert!(
        !code.contains(".delete_opt("),
        "`delete_opt` takes no `DeleteParams`, so a delete written that way carries NO UID \
         precondition at all"
    );
    assert_eq!(
        code.matches(".delete(").count(),
        1,
        "this file makes EXACTLY ONE delete call — the check GC's. A second one is a second \
         capability nobody reviewed, and the reviewer's planted one carried no precondition"
    );

    // …and it is inside `collect_expired`, on the typed handle, with the UID.
    let from = code
        .find("async fn collect_expired(")
        .expect("the collector is in this file");
    let region = &code[from..];
    let to = region[1..].find("\n}").map_or(region.len(), |r| r + 2);
    let body = &region[..to];

    // EVERY mention of `DeleteParams` is inside that body. The count is two
    // and not one because the struct-update form names the type twice
    // (`DeleteParams { … ..DeleteParams::default() }`); what matters is that
    // the file's total and the collector's total are the same number, so a
    // `DeleteParams` built anywhere else is a diff.
    assert_eq!(
        code.matches("DeleteParams").count(),
        body.matches("DeleteParams").count(),
        "every `DeleteParams` in this file belongs to `collect_expired`; one built elsewhere is \
         a delete being prepared outside the collector"
    );
    assert!(
        body.contains("api.delete("),
        "the one delete call moved out of `collect_expired`, or changed receiver: `api` is the \
         spelling `manifest_lint`'s `Api<T>`-region scan can see and the one \
         `scripts/check-no-archive-write.sh`'s store-anchored token does not match"
    );
    assert!(
        body.contains("preconditions:") && body.contains("uid: Some(uid.clone())"),
        "the collector's delete must carry `DeleteParams {{ preconditions: {{ uid }} }}` — a \
         name is not an identity"
    );
    // AND THE LISTING IT DELETES FROM IS NAMESPACED. `Api::all` is legitimate
    // in this file — `controller()` builds the cluster-wide WATCH, which is
    // how a controller sees every namespace at all — so the assertion is
    // scoped to the collector: one built over `Api::all` would make a
    // per-namespace cohort rule reach every tenant's objects at once, and the
    // per-pass cap would then be the ONLY thing between a bad rule and the
    // whole cluster.
    assert!(
        !body.contains("Api::all"),
        "`collect_expired` must never build a cluster-wide handle: its caller passes the \
         `Api::namespaced` handle for the object's own namespace"
    );
    assert!(
        body.contains(".list("),
        "the collector LISTS and then deletes what the rule named; it never deletes by name \
         alone"
    );
    let terminal = code
        .find("if terminal {")
        .expect("the terminal branch is where the collector runs");
    let window = &code[terminal..terminal + 1400.min(code.len() - terminal)];
    assert!(
        window.contains("Api::namespaced(client.clone(), &namespace)"),
        "the handle the terminal branch hands `collect_expired` is namespaced to THIS object's \
         namespace"
    );
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
    assert_eq!(
        pf::plan_kind(PreflightOperation::SourceConnection),
        CheckPlanKind::SourceConnection
    );
    // Four operations, four Jobs for one subject: the discriminator is in the
    // name, so a connectivity test cannot land on a readiness check's Job.
    let names: BTreeSet<String> = [
        PreflightOperation::Backup,
        PreflightOperation::Restore,
        PreflightOperation::DestinationAccess,
        PreflightOperation::SourceConnection,
    ]
    .into_iter()
    .map(|op| check::job::check_job_name(pf::plan_kind(op), PF_UID))
    .collect();
    assert_eq!(names.len(), 4);
    assert!(
        check::job::check_job_name(CheckPlanKind::SourceConnection, PF_UID).starts_with("lwc-sc-")
    );
}

// ===========================================================================
// 10. F1 — the expected row set is the RUNNER's row set for the same plan
// ===========================================================================

/// The runner's own pinned expectation, read out of `crates/logweir/tests/check_cli.rs`.
///
/// # Why this reads another crate's SOURCE instead of calling it
///
/// `weirkeeper` and `logweir` share no dependency edge — the controller crate
/// links `logweir-core` and `logweir-verify`, never the runner — and adding one
/// to test a row set would be a dependency decision, not a test. What the two
/// crates DO share is this file: `a_readiness_check_reports_every_row_it_owns`
/// and `a_healthy_restore_preflight_reports_every_row_it_owns` each assert that
/// the runner emits **exactly** the ids in their `want` literal, so the literal
/// is a pinned statement of the runner's behaviour. Reading it here closes the
/// loop: the runner test pins runner ⟷ literal, and this one pins literal ⟷
/// [`job_rows`].
///
/// It is not a copy. A copied constant is what reviewer finding **F1** was: the
/// controller's idea of the runner's rows drifted from the runner's and nothing
/// could see it. Either side moving now breaks a test.
fn runner_pinned_ids(test_fn: &str) -> BTreeSet<String> {
    runner_pinned_ids_at_least(test_fn, 8)
}

/// The same, with the floor the caller's literal actually has.
///
/// THE FLOOR IS WHAT KEEPS THIS FROM PASSING VACUOUSLY: if the extractor stops
/// matching, an empty set would equal an empty set. Eight is right for the two
/// big catalogues; a `sourceConnection` check pins TWO rows, and a floor of
/// eight there would refuse a correct literal.
fn runner_pinned_ids_at_least(test_fn: &str, floor: usize) -> BTreeSet<String> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("the repository root")
        .join("crates/logweir/tests/check_cli.rs");
    let source = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("could not read {}: {e}", path.display()));
    let at = source
        .find(&format!("fn {test_fn}("))
        .unwrap_or_else(|| panic!("`{test_fn}` is gone from {}", path.display()));
    let rest = &source[at..];
    let want_at = rest
        .find("let want: BTreeSet<&str> = [")
        .unwrap_or_else(|| panic!("`{test_fn}` no longer pins a `want` id set"));
    let body = &rest[want_at..];
    let end = body.find(']').expect("the `want` literal closes");
    let ids: BTreeSet<String> = body[..end]
        .split('"')
        .filter(|t| t.contains('.') && !t.contains(' ') && !t.contains('['))
        .map(ToString::to_string)
        .collect();
    assert!(
        ids.len() >= floor,
        "only {} ids were extracted from `{test_fn}`; the literal's shape changed and this \
         guard would pass vacuously: {ids:?}",
        ids.len()
    );
    ids
}

fn ids_of(set: &BTreeSet<CheckId>) -> BTreeSet<String> {
    set.iter().map(|i| i.as_str().to_string()).collect()
}

/// The `readiness_plan` fixture `crates/logweir/tests/check_cli.rs` drives its
/// `a_readiness_check_reports_every_row_it_owns` with, as a `CheckRequest`.
fn runner_readiness_request() -> CheckRequest {
    CheckRequest::OperationReadiness(Box::new(
        logweir_core::check_contract::OperationReadinessRequest {
            operation: logweir_core::check_contract::CheckOperation::Backup,
            connection: fixture_connection_plan(),
            destination: Some(fixture_destination_plan()),
            roles: vec![
                DestinationRole::ArchiveRead,
                DestinationRole::EvidenceWrite,
                DestinationRole::EvidenceRead,
            ],
            topics: vec!["orders".to_string()],
            signer_path: Some("/signing/key.pem".to_string()),
            write_probe: true,
            skip_checks: Vec::new(),
        },
    ))
}

/// The `restore_plan` fixture the runner's healthy-restore test drives, as a
/// `CheckRequest`. `evidence_destination: None`, target mode `scratch`.
/// The plan `crates/logweir/tests/check_cli.rs` drives its
/// `a_source_connection_check_reports_every_row_it_owns` with.
fn runner_source_connection_request() -> CheckRequest {
    CheckRequest::SourceConnection(logweir_core::check_contract::SourceConnectionRequest {
        connection: fixture_connection_plan(),
    })
}

fn runner_restore_request(evidence: Option<()>) -> CheckRequest {
    CheckRequest::RestorePreflight(Box::new(
        logweir_core::check_contract::RestorePreflightRequest {
            plan_file: "/check/plan.yaml".to_string(),
            plan_sha256: PLAN_DIGEST.to_string(),
            target: fixture_connection_plan(),
            source_destination: fixture_destination_plan(),
            evidence_destination: evidence.map(|()| fixture_destination_plan()),
            backup_id: "bk-1".to_string(),
            manifest_key: "bk-1/manifest.json".to_string(),
            checks: Vec::new(),
            skip_checks: Vec::new(),
        },
    ))
}

fn fixture_connection_plan() -> logweir_core::check_contract::ConnectionPlan {
    logweir_core::check_contract::ConnectionPlan {
        bootstrap_servers: vec!["broker:9092".to_string()],
        auth_mode: "scramSha512".to_string(),
        username: Some("backup".to_string()),
        password_env: Some("LOGWEIR_SOURCE_PASSWORD".to_string()),
        tls: Some(true),
        ca_file: None,
        principal: "User:backup".to_string(),
    }
}

fn fixture_destination_plan() -> logweir_core::check_contract::DestinationPlan {
    logweir_core::check_contract::DestinationPlan {
        name: "primary".to_string(),
        uid: DEST_UID.to_string(),
        location: logweir_core::destination::DestinationLocation {
            provider: logweir_core::destination::StorageProvider::S3,
            bucket: "s3-bucket".to_string(),
            prefix: String::new(),
            region: None,
            endpoint: None,
            addressing: logweir_core::destination::Addressing::PathStyle,
            transport: logweir_core::destination::TransportSecurity::Tls,
        },
        location_digest: "sha256:aa".to_string(),
        ca_file: None,
        credentials: logweir_core::check_contract::CredentialMode::Static,
    }
}

/// **The guard finding F1 asks for.** The ids [`job_rows`] expects for a plan
/// are the ids the runner emits for that same plan.
#[test]
fn the_expected_rows_are_the_rows_the_runner_emits() {
    assert_eq!(
        ids_of(&job_rows(&runner_readiness_request(), false)),
        runner_pinned_ids("a_readiness_check_reports_every_row_it_owns"),
        "`job_rows` and the runner disagree about an `operationReadiness` plan. This is the \
         defect F1 named: every id in the difference becomes a BLOCKING `unknown` row reading \
         \"the check Job did not report this row\", and no Preflight can ever be `ready`."
    );
    assert_eq!(
        ids_of(&job_rows(&runner_restore_request(None), true)),
        runner_pinned_ids("a_healthy_restore_preflight_reports_every_row_it_owns"),
        "`job_rows` and the runner disagree about a `restorePreflight` plan"
    );
    assert_eq!(
        ids_of(&job_rows(&runner_source_connection_request(), false)),
        runner_pinned_ids_at_least("a_source_connection_check_reports_every_row_it_owns", 2),
        "`job_rows` and the runner disagree about a `sourceConnection` plan. The runner emits \
         two rows and `connection.topicsDescribable` is deliberately not one of them; a mirror \
         that listed it would pin every connection test at `unknown`."
    );
}

/// The two rows whose presence depends on the plan and not on the operation,
/// each derived from the runner source line that decides it.
#[test]
fn the_expected_rows_follow_the_plan_and_not_the_operation() {
    let base = job_rows(&runner_restore_request(None), true);
    // `restore.rs` pushes `destination.evidenceWritable` when, and only when,
    // the request names an evidence destination.
    let with_evidence = job_rows(&runner_restore_request(Some(())), true);
    assert_eq!(
        with_evidence
            .difference(&base)
            .copied()
            .collect::<BTreeSet<CheckId>>(),
        [CheckId::DestinationEvidenceWritable].into_iter().collect(),
        "naming an evidence destination adds exactly one row"
    );
    // `target.scratchMarker` is the one row decided by the mounted PLAN BYTES,
    // which is why `job_rows` takes the flag.
    let new_topic = job_rows(&runner_restore_request(None), false);
    assert!(!new_topic.contains(&CheckId::TargetScratchMarker));
    assert!(base.contains(&CheckId::TargetScratchMarker));

    // A readiness plan with no signer path emits no signer row...
    let CheckRequest::OperationReadiness(mut r) = runner_readiness_request() else {
        unreachable!()
    };
    r.signer_path = None;
    let rows = job_rows(&CheckRequest::OperationReadiness(r.clone()), false);
    assert!(!rows.contains(&CheckId::SignerPrivateKeyUsable));
    // ...and a role that is not requested produces no row for it.
    r.roles = vec![DestinationRole::ArchiveRead];
    let rows = job_rows(&CheckRequest::OperationReadiness(r), false);
    assert!(rows.contains(&CheckId::DestinationArchiveListable));
    assert!(!rows.contains(&CheckId::DestinationEvidenceWritable));
    assert!(!rows.contains(&CheckId::DestinationEvidenceReadable));
}

/// The role→row table is the runner's `access.rs` match, and all four roles are
/// distinct.
#[test]
fn every_destination_role_maps_to_its_own_row() {
    let rows: BTreeSet<CheckId> = DestinationRole::ALL
        .iter()
        .map(|r| pf::destination_row_for(*r))
        .collect();
    assert_eq!(rows.len(), DestinationRole::ALL.len());
    assert_eq!(
        pf::destination_row_for(DestinationRole::ArchiveRead),
        CheckId::DestinationArchiveListable,
        "the BLOCKING archive row comes from the READ grant; requesting only ArchiveWrite is \
         what made it unanswerable (F1)"
    );
}

/// The controller's own rendered backup plan requests the roles whose rows D2
/// §6.3's Backup catalogue gates on.
#[test]
fn the_rendered_backup_plan_requests_the_blocking_destination_rows() {
    let shape = rendered_backup_shape(true);
    let rows = job_rows(&shape.plan.request, false);
    for id in [
        CheckId::DestinationArchiveListable,
        CheckId::DestinationEvidenceWritable,
        CheckId::DestinationEvidenceReadable,
        CheckId::ConnectionAuthenticated,
        CheckId::ConnectionTopicsDescribable,
        CheckId::SignerPrivateKeyUsable,
        CheckId::RunnerContract,
    ] {
        assert!(rows.contains(&id), "the rendered plan cannot answer {id}");
    }
}

// ===========================================================================
// 11. Fix round 1: the rows the review's mutants asked for
// ===========================================================================

/// A destination that configures the evidence-read grant AND opts in to the
/// create-only readiness marker — the two spec facts the rendered plan reads.
fn rich_destination(name: &str) -> Value {
    let mut object = backup_destination(name);
    let spec = object["spec"].as_object_mut().expect("spec");
    spec["access"].as_object_mut().expect("access").insert(
        "evidenceRead".to_string(),
        json!({
            "mode": "SecretKeys",
            "secret": {
                "name": "logweir-s3",
                "accessKeyIdKey": "AWS_ACCESS_KEY_ID",
                "secretAccessKeyKey": "AWS_SECRET_ACCESS_KEY"
            }
        }),
    );
    spec.insert(
        "readiness".to_string(),
        json!({"writeProbe": "CreateOnlyMarker"}),
    );
    object
}

/// The `Inputs` a healthy backup readiness pass resolves to, built from the
/// REAL resolver over the fixtures.
fn backup_inputs(rich: bool) -> Inputs {
    let cluster: weirkeeper::crds::kafka_cluster::KafkaCluster =
        serde_json::from_value(kafka_cluster("source", Some("prod-id"))).expect("fixture");
    let object: weirkeeper::crds::backup_destination::BackupDestination =
        serde_json::from_value(if rich {
            rich_destination("primary")
        } else {
            backup_destination("primary")
        })
        .expect("fixture");
    let policy = weirkeeper::check::policy::Policy::defaults();
    let mut roles = vec![DestinationRole::ArchiveRead, DestinationRole::EvidenceWrite];
    let evidence_read_configured = !matches!(
        weirkeeper::destination::resolve(&object, DestinationRole::EvidenceRead, &policy)
            .map(|d| d.grant),
        Ok(weirkeeper::destination::ResolvedGrant::NotConfigured) | Err(_)
    );
    if evidence_read_configured {
        roles.push(DestinationRole::EvidenceRead);
    }
    Inputs {
        operation: PreflightOperation::Backup,
        namespace: NS.to_string(),
        timeout_seconds: 120,
        policy_digest: policy.digest(),
        cluster_uid: Some(CLUSTER_UID.to_string()),
        connection: Some(weirkeeper::connection::resolve(
            &cluster,
            weirkeeper::connection::ConnectionUse::PreflightSource,
        )),
        archive_name: Some("primary".to_string()),
        archive: Some(weirkeeper::destination::resolve(
            &object,
            DestinationRole::ArchiveWrite,
            &policy,
        )),
        roles,
        write_probe: weirkeeper::destination::write_probe_enabled(&object),
        topics: vec!["orders".to_string()],
        ..Inputs::default()
    }
}

fn rendered_backup_shape(rich: bool) -> weirkeeper::controllers::preflight::JobShape {
    weirkeeper::controllers::preflight::build_job_shape(
        &backup_inputs(rich),
        &weirkeeper::job::RunnerOwner {
            api_version: "logweir.dev/v1alpha1".to_string(),
            kind: "Preflight".to_string(),
            name: "pf-1".to_string(),
            uid: PF_UID.to_string(),
        },
        &weirkeeper::job::RunnerImage::default(),
    )
    .expect("the shape renders")
}

/// The rendered `sourceConnection` plan carries the resolved connection and
/// the pod is given no signing key and no destination env.
///
/// MUTANT: make `Inputs::signs()` true for `SourceConnection` (its previous
/// spelling, `self.operation != PreflightOperation::DestinationAccess`, does
/// exactly that). The check pod would mount the installation's signing Secret
/// to answer a question about a broker — a credential in a pod that has no use
/// for it, which is the blast-radius argument D2 §6.1 accepts the check Job on
/// in the first place.
#[test]
fn a_source_connection_plan_carries_the_connection_and_projects_no_signer() {
    let cluster: weirkeeper::crds::kafka_cluster::KafkaCluster =
        serde_json::from_value(kafka_cluster("source", Some("prod-id"))).expect("fixture");
    let policy = weirkeeper::check::policy::Policy::defaults();
    let inputs = Inputs {
        operation: PreflightOperation::SourceConnection,
        namespace: NS.to_string(),
        timeout_seconds: 120,
        policy_digest: policy.digest(),
        cluster_name: Some("source".to_string()),
        cluster_uid: Some(CLUSTER_UID.to_string()),
        connection: Some(weirkeeper::connection::resolve(
            &cluster,
            weirkeeper::connection::ConnectionUse::PreflightSource,
        )),
        ..Inputs::default()
    };
    assert!(!inputs.signs(), "nothing would be signed by a dial");

    let shape = weirkeeper::controllers::preflight::build_job_shape(
        &inputs,
        &weirkeeper::job::RunnerOwner {
            api_version: "logweir.dev/v1alpha1".to_string(),
            kind: "Preflight".to_string(),
            name: "pf-1".to_string(),
            uid: PF_UID.to_string(),
        },
        &weirkeeper::job::RunnerImage::default(),
    )
    .expect("the shape renders");

    let CheckRequest::SourceConnection(r) = &shape.plan.request else {
        panic!("a sourceConnection plan, got {:?}", shape.plan.kind())
    };
    assert_eq!(r.connection.principal, "User:backup");
    // THE NAME, NEVER THE VALUE — the same rule every rendered plan keeps.
    assert!(r.connection.password_env.is_some());
    let rendered = String::from_utf8(shape.documents.check_plan.clone()).expect("utf-8");
    assert!(!rendered.contains("destination"), "{rendered}");

    assert_eq!(shape.spec.kind, CheckPlanKind::SourceConnection);
    assert!(
        shape
            .spec
            .secret_mounts
            .iter()
            .all(|m| m.secret_name != "logweir-signing-key"),
        "no signing key in a pod that signs nothing: {:?}",
        shape.spec.secret_mounts
    );
    assert!(shape.documents.archive_ca.is_none());
    assert!(shape.documents.restore_plan.is_none());
}

/// **F3.** `spec.readiness.writeProbe: CreateOnlyMarker` is a shipped,
/// user-settable field. It used to be hard-`false` in every rendered plan, so
/// an operator who opted in got `WriteNotProbed` with a message that was FALSE
/// about their own object — the UI-FAKEPREFLIGHT pattern with a new spelling.
#[test]
fn an_opted_in_destination_gets_the_create_only_write_probe() {
    let CheckRequest::OperationReadiness(r) = rendered_backup_shape(true).plan.request else {
        panic!("a backup readiness plan")
    };
    assert!(
        r.write_probe,
        "`readiness.writeProbe: CreateOnlyMarker` must reach the plan; without it the runner \
         answers `WriteNotProbed` and says the destination configures no probe, which is false"
    );
    let CheckRequest::OperationReadiness(r) = rendered_backup_shape(false).plan.request else {
        panic!("a backup readiness plan")
    };
    assert!(
        !r.write_probe,
        "and a destination that did NOT opt in still writes nothing — Global Constraint 6's \
         create-only boundary is opt-in and stays so"
    );
}

/// The resolver's own half of F3, over the three spellings of the field.
#[test]
fn the_write_probe_is_read_from_the_spec_and_defaults_closed() {
    let of = |readiness: Option<Value>| {
        let mut object = backup_destination("primary");
        if let Some(r) = readiness {
            object["spec"]
                .as_object_mut()
                .expect("spec")
                .insert("readiness".to_string(), r);
        }
        let dest: weirkeeper::crds::backup_destination::BackupDestination =
            serde_json::from_value(object).expect("fixture");
        weirkeeper::destination::write_probe_enabled(&dest)
    };
    assert!(!of(None), "absent `readiness` is Disabled");
    assert!(!of(Some(json!({}))), "absent `writeProbe` is Disabled");
    assert!(!of(Some(json!({"writeProbe": "Disabled"}))));
    assert!(of(Some(json!({"writeProbe": "CreateOnlyMarker"}))));
}

/// **F2(a).** An unanswered BLOCKING row holds the verdict back. The mutant is
/// `result_for` filtering `unknown` rows out of the set it aggregates.
#[test]
fn a_blocking_unknown_or_skipped_row_keeps_the_verdict_unknown() {
    let ready = runner_row(
        CheckId::RunnerContract,
        CheckState::Ready,
        CheckCode::ContractSupported,
        Gating::Blocking,
    );
    let unanswered = CheckOutcome::new(
        CheckId::ArchiveSegments,
        CheckState::Unknown,
        Gating::Blocking,
        Authority::CheckJob,
        CheckCode::BlockedByPrerequisite,
    );
    assert_eq!(
        result_for(&[ready.clone(), unanswered], None).0.state,
        "unknown",
        "a blocking row nobody answered must not be filtered out of the aggregate: that is \
         UI-FAKEPREFLIGHT rewritten in the one function that decides the published verdict"
    );
    let skipped = CheckOutcome::new(
        CheckId::ArchiveSegments,
        CheckState::Skipped,
        Gating::Blocking,
        Authority::Controller,
        CheckCode::BlockedByPrerequisite,
    );
    assert_eq!(
        result_for(&[ready.clone(), skipped], None).0.state,
        "unknown",
        "skipping a question is not answering it (D2 §6.2)"
    );
    // And the control: every blocking row ready IS ready, so the two rows above
    // are the difference and not a test that can only ever say `unknown`.
    assert_eq!(result_for(&[ready], None).0.state, "ready");
}

/// **F8.** The published list is capped at the CRD's own `maxItems`, and the
/// verdict is computed over every row, so producing more rows can never turn a
/// verdict green.
#[test]
fn the_published_rows_are_capped_and_the_cap_cannot_change_the_verdict() {
    let mut checks: Vec<CheckOutcome> = (0..70)
        .map(|i| {
            let id = CheckId::ALL[i % CheckId::ALL.len()];
            runner_row(
                id,
                CheckState::Ready,
                CheckCode::Succeeded,
                Gating::Blocking,
            )
        })
        .collect();
    // The one row that decides the verdict, LAST — so a cap that took the first
    // N would drop it.
    checks.push(
        CheckOutcome::new(
            CheckId::ArchiveSegments,
            CheckState::NotReady,
            Gating::Blocking,
            Authority::CheckJob,
            CheckCode::SegmentMissing,
        )
        .with_message("a segment is missing"),
    );
    let (result, dropped) = result_for(&checks, None);
    let published = result.checks.as_ref().expect("checks");
    assert!(
        published.len() <= pf::MAX_PUBLISHED_CHECKS,
        "the shipped CRD declares maxItems: {}; a longer list is a 422 the reconciler can only \
         requeue on, and the verdict is then never published at all",
        pf::MAX_PUBLISHED_CHECKS
    );
    assert!(dropped > 0, "this fixture is over the cap on purpose");
    assert_eq!(
        result.state, "notReady",
        "the verdict is computed over ALL rows"
    );
    assert!(
        published.iter().any(|c| c.id == "archive.segments"),
        "the rows that EXPLAIN the verdict are the ones kept"
    );
}

/// **F9.** A restore whose EVIDENCE destination does not resolve gets the cause
/// and the remedy for that destination, scoped to it — not a green row about
/// the archive.
#[test]
fn the_destination_row_reports_whichever_destination_refused() {
    let ok = weirkeeper::destination::resolve(
        &serde_json::from_value::<weirkeeper::crds::backup_destination::BackupDestination>(
            backup_destination("primary"),
        )
        .expect("fixture"),
        DestinationRole::ArchiveRead,
        &weirkeeper::check::policy::Policy::defaults(),
    );
    let refused = Err(weirkeeper::destination::DestinationRefusal {
        code: CheckCode::CaBundleNotFound,
        field: "spec.transport.caBundle".to_string(),
        message: "the CA ConfigMap does not exist".to_string(),
    });
    let inputs = Inputs {
        operation: PreflightOperation::Restore,
        archive_name: Some("primary".to_string()),
        archive: Some(ok),
        evidence_name: Some("evidence".to_string()),
        evidence: Some(refused),
        ..Inputs::default()
    };
    let row = inputs
        .destination_verdict_row(now())
        .expect("a restore names destinations");
    assert_eq!(row.code, CheckCode::CaBundleNotFound);
    assert_eq!(
        row.scope.as_ref().map(|s| s.name.as_str()),
        Some("evidence"),
        "the scope names the object that actually failed"
    );
    assert!(!row.remedy.is_empty());
    assert!(
        !inputs.plan_blockers(&[row]).is_empty(),
        "and it blocks the plan, so no check Job is created against a destination that did not \
         resolve"
    );
}

/// **F7.** A topic name long enough to be illegal is a topic name long enough
/// for the redaction chokepoint to eat, so it goes on `scope`, which
/// `entry_of` copies verbatim.
#[test]
fn the_offending_topic_name_survives_redaction_on_the_scope() {
    let long = "x".repeat(250);
    let facts = PlanFacts::of(plan_yaml(&long, "s3-bucket").as_bytes(), None);
    let row = plan_names_row(&facts, now());
    assert_eq!(row.code, CheckCode::MappedTopicNameIllegal);
    let entry = entry_of(&row);
    let named = entry
        .scope
        .as_ref()
        .and_then(|s| s.name.clone())
        .unwrap_or_default();
    assert!(
        named.starts_with(&long),
        "the operator is told a name is illegal in exactly the case the name is long enough to \
         be redacted out of the prose; `scope` is where it survives. Got {named:?}"
    );
    assert_eq!(
        entry.scope.as_ref().and_then(|s| s.kind.clone()).as_deref(),
        Some("Topic")
    );
    // And the same for the bindings row's "the first being `…`".
    let short_plan = PlanFacts::of(plan_yaml("restore-", "s3-bucket").as_bytes(), None);
    let bindings = BindingFacts {
        recovery_point_topics: Some(vec!["payments".to_string()]),
        ..BindingFacts::default()
    };
    let row = plan_bindings_row(&short_plan, &bindings, now());
    assert_eq!(row.code, CheckCode::PlanTopicsNotInRecoveryPoint);
    assert_eq!(row.scope.as_ref().map(|s| s.name.as_str()), Some("orders"));
}

/// **Q1.** A verified relay is never discarded, whatever the pod is waiting for.
#[test]
fn a_verified_relay_is_never_discarded_for_a_waiting_state() {
    let (_, blocked) = pod_outcomes(
        PreflightOperation::Backup,
        Some(&waiting(CheckCode::DisruptedMidCheck, None)),
        &projections(),
        true,
        None,
        now(),
    );
    assert!(
        !blocked,
        "`blocked` is exactly `drop every relayed row`; a result that exists is worth more than \
         the placeholders that would replace it"
    );
    let (_, blocked) = pod_outcomes(
        PreflightOperation::Backup,
        Some(&waiting(CheckCode::DisruptedMidCheck, None)),
        &projections(),
        false,
        None,
        now(),
    );
    assert!(blocked, "and with no relay there is nothing to keep");
}

// ===========================================================================
// 12. Fix round 1: the happy paths, built from the RUNNER's own row set
// ===========================================================================

/// Turn the runner's pinned id set into a relay, with the two facts the J+C
/// rows need. **Never `job_rows`**: a fixture defined as the expectation is
/// what hid reviewer finding F1.
fn relay_from(ids: &BTreeSet<String>) -> Vec<CheckOutcome> {
    ids.iter()
        .map(|s| {
            let id = CheckId::parse(s).unwrap_or_else(|| panic!("`{s}` is not a CheckId"));
            let row = runner_row(
                id,
                CheckState::Ready,
                CheckCode::Succeeded,
                Gating::Blocking,
            );
            match id {
                CheckId::SignerPrivateKeyUsable => row.with_fact("signerKeyId", RUNNER_KEY_ID),
                CheckId::ConnectionAuthenticated | CheckId::TargetAuthenticated => {
                    row.with_fact("clusterId", "prod-id")
                }
                _ => row,
            }
        })
        .collect()
}

/// **F1, the backup half.** A healthy backup readiness check reports `ready`.
#[tokio::test]
async fn a_healthy_backup_readiness_reports_ready() {
    let job = job_name(CheckPlanKind::OperationReadiness);
    let ids = runner_pinned_ids("a_readiness_check_reports_every_row_it_owns");
    let log = relay_log(PLAN_DIGEST, relay_from(&ids), None);

    let mut routes = vec![
        route(
            "GET",
            "/trustrosters/default",
            roster(RUNNER_KEY_ID, vec![]).to_string(),
        ),
        route(
            "GET",
            "/kafkaclusters/source",
            kafka_cluster("source", Some("prod-id")).to_string(),
        ),
        route(
            "GET",
            "/backupdestinations/primary",
            rich_destination("primary").to_string(),
        ),
    ];
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

    let not_ready: Vec<&Value> = status["result"]["checks"]
        .as_array()
        .expect("checks")
        .iter()
        .filter(|c| c["state"] != "ready")
        .collect();
    assert_eq!(
        status["result"]["state"], "ready",
        "PLAT-03.1's whole point. Rows that are not ready: {not_ready:#?}"
    );
    assert_eq!(status["phase"], "Completed");
    assert_eq!(
        check_entry(&status, "destination.archiveListable")["state"],
        "ready",
        "the BLOCKING archive row is answered because the plan requests the ArchiveRead grant"
    );
    assert_eq!(
        check_entry(&status, "signer.rostered")["code"],
        "SignerRostered"
    );
    assert_eq!(
        check_entry(&status, "connection.clusterIdentity")["code"],
        "ClusterIdentityMatches"
    );
    // Every row the runner reported is in the verdict, and none is a
    // `BlockedByPrerequisite` placeholder.
    for id in &ids {
        let row = check_entry(&status, id);
        assert_ne!(
            row["code"], "BlockedByPrerequisite",
            "{id} was relayed and must not be replaced by a placeholder"
        );
    }
}

/// **F1, the restore half.** A healthy restore preflight over an approved
/// `Restore` reports `ready`.
#[tokio::test]
async fn a_healthy_restore_preflight_reports_ready() {
    let job = job_name(CheckPlanKind::RestorePreflight);
    let mut ids = runner_pinned_ids("a_healthy_restore_preflight_reports_every_row_it_owns");
    // The one id the fixture plan cannot carry: `restore.rs` pushes
    // `destination.evidenceWritable` when — and only when — the request names
    // an evidence destination, which the runner's own fixture leaves `None` and
    // this controller always sets. `the_expected_rows_follow_the_plan_and_not_the_operation`
    // is the row that proves that is the ONLY difference.
    ids.insert(CheckId::DestinationEvidenceWritable.as_str().to_string());
    let log = relay_log(PLAN_DIGEST, relay_from(&ids), None);

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

    let mut routes = vec![
        route(
            "GET",
            "/trustrosters/default",
            roster(RUNNER_KEY_ID, vec!["prod-id"]).to_string(),
        ),
        route("GET", "/restores/r-1", restore_object.to_string()),
        route("GET", "/approvals/ap-1", approval.to_string()),
        route(
            "GET",
            "/kafkaclusters/target",
            kafka_cluster("target", Some("prod-id")).to_string(),
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
    routes.extend(observation_routes(
        leak(job.clone()),
        owned_pod(&job, terminated(0)),
        log,
    ));

    let request = json!({"operation": "Restore", "restore": {
        "restoreRef": {"name": "r-1"},
        "sourceDestinationRef": {"name": "primary"},
        "evidenceDestinationRef": {"name": "evidence"}
    }, "timeoutSeconds": 120});
    let (status, _) = reconcile_with(&preflight(request), routes).await;

    let not_ready: Vec<&Value> = status["result"]["checks"]
        .as_array()
        .expect("checks")
        .iter()
        .filter(|c| c["state"] != "ready")
        .collect();
    assert_eq!(
        status["result"]["state"], "ready",
        "PLAT-03.2's whole point. Rows that are not ready: {not_ready:#?}"
    );
    assert_eq!(
        check_entry(&status, "approval.state")["code"],
        "ApprovalVerified"
    );
    assert_eq!(
        check_entry(&status, "target.clusterIdentity")["code"],
        "TargetAllowed"
    );
    assert_eq!(
        check_entry(&status, "signer.rostered")["gating"],
        "executionOnly",
        "a restore check plan projects no signing key, so the row says so rather than blocking \
         on an answer nobody can give"
    );
    assert_eq!(check_entry(&status, "archive.segments")["state"], "ready");
}

/// **F2(b).** The binding is recorded BEFORE the Job. The mutant is moving the
/// `Pending` status patch after `check_plan::ensure` / `check::create_job`.
#[tokio::test]
async fn the_binding_is_recorded_before_the_plan_and_the_job_are_created() {
    let job = job_name(CheckPlanKind::OperationReadiness);
    let routes = vec![
        route(
            "GET",
            "/trustrosters/default",
            roster(RUNNER_KEY_ID, vec![]).to_string(),
        ),
        route(
            "GET",
            "/kafkaclusters/source",
            kafka_cluster("source", Some("prod-id")).to_string(),
        ),
        route(
            "GET",
            "/backupdestinations/primary",
            rich_destination("primary").to_string(),
        ),
        not_found("GET", leak(job.clone())),
        route("GET", "/apis/batch/v1/jobs", list_of(vec![])),
        route("PATCH", "/pf-1/status", echo("Preflight", "pf-1")),
        route("POST", "/configmaps", echo("ConfigMap", "plan")),
        route("POST", "/jobs", echo("Job", &job)),
    ];
    let (client, recorder, bodies) = mock_client_recording_bodies(routes);
    let ctx = context(client);
    let cache = weirkeeper::check::policy::PolicyCache::new();
    pf::reconcile_preflight(&preflight(backup_request()), &ctx, &cache, now())
        .await
        .expect("the reconcile completes");

    let seen = recorder.lock().expect("recorder");
    let index =
        |pred: &dyn Fn(&weirkeeper::testing::SeenRequest) -> bool| seen.iter().position(pred);
    let status_at = index(&|r| r.method == "PATCH" && r.uri.contains("/preflights/pf-1/status"))
        .expect("the binding was written");
    let plan_at = index(&|r| r.method == "POST" && r.uri.contains("/configmaps"))
        .expect("the plan ConfigMap was created");
    let job_at =
        index(&|r| r.method == "POST" && r.uri.contains("/jobs")).expect("the Job was created");
    assert!(
        status_at < plan_at && status_at < job_at,
        "D2 §6.6: the binding is recorded BEFORE the Job. A verdict whose binding was written \
         afterwards cannot be told apart from a verdict about whatever the objects became in \
         between. Order seen: {:?}",
        seen.iter()
            .map(|r| format!("{} {}", r.method, r.uri))
            .collect::<Vec<_>>()
    );
    let first = bodies
        .lock()
        .expect("bodies")
        .iter()
        .find(|b| b.method == "PATCH" && b.uri.contains("/preflights/pf-1/status"))
        .expect("the first status patch")
        .body
        .clone();
    assert!(
        first.contains("\"inputsDigest\""),
        "and that first patch carries the binding, not just a phase: {first}"
    );
}

// ===========================================================================
// PLAT-15.2 — the catalog point is read THROUGH THE RECONCILER
// ===========================================================================

const CATALOG_POINT: &str = "lwp1-0123456789abcdef0123456789abcdef";
const CATALOG_PAGE: &str = "archive-g1-p0";

fn catalog_receipt_sha() -> String {
    format!("sha256:{}", "a1".repeat(32))
}

fn catalog_manifest_sha() -> String {
    format!("sha256:{}", "b2".repeat(32))
}

fn catalog_receipt_key() -> String {
    "logweir/backups/bk-1/01JB7Z00000000000000000000.receipt.json".to_string()
}

/// A draft plan in the runner's grammar, BOUND to the catalog point when
/// `bound` — the one fact that separates the ready case from the mismatch.
fn catalog_plan_yaml(bound: bool) -> String {
    let mut plan = plan_yaml("restore-", "s3-bucket");
    if bound {
        plan = plan.replace(
            "  topics:\n    - orders\n",
            &format!(
                "  topics:\n    - orders\n  point:\n    point_id: {CATALOG_POINT}\n    receipt_key: \
                 {}\n    receipt_sha256: '{}'\n    manifest_sha256: '{}'\n",
                catalog_receipt_key(),
                catalog_receipt_sha(),
                catalog_manifest_sha()
            ),
        );
    }
    plan
}

/// The catalog, its one immutable page, and the page's recorded digest.
fn catalog_objects(selectable: bool) -> (Value, Value) {
    let entry = json!({
        "pointId": CATALOG_POINT, "backupId": "bk-1", "runId": "01JB7Z00000000000000000000",
        "recoveryPointAtMs": 1_757_000_000_000_i64,
        "coveredFromMs": 1_756_990_000_000_i64, "coveredToMs": 1_757_000_000_000_i64,
        "receiptKey": catalog_receipt_key(), "receiptSha256": catalog_receipt_sha(),
        "manifestSha256": catalog_manifest_sha(),
        "availability": "Available",
        "verification": if selectable { "Verified" } else { "UntrustedSigner" },
        "selectable": selectable
    })
    .to_string();
    let digest = weirkeeper::catalog_view::page_digest(&[entry.as_str()]);
    let page = json!({
        "apiVersion": "v1", "kind": "ConfigMap",
        "metadata": {"name": CATALOG_PAGE, "namespace": NS},
        "immutable": true,
        "data": {(weirkeeper::catalog_view::PAGE_DATA_KEY): format!("{entry}\n")}
    });
    let catalog = json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "RecoveryCatalog",
        "metadata": {"name": "archive", "namespace": NS, "uid": "catalog-uid"},
        "spec": {
            "destinationRef": {"name": "primary"},
            "sync": {"intervalSeconds": 0, "mode": "Full", "maxObjectsPerRun": 100000,
                     "deepCheck": "ManifestDigest", "viewLimit": 2000}
        },
        "status": {
            "viewExpiresAt": (now() + Duration::hours(1)).to_rfc3339(),
            "pages": [{"configMapName": CATALOG_PAGE, "index": 0, "count": 1,
                       "sha256": format!("sha256:{digest}")}]
        }
    });
    (catalog, page)
}

async fn restore_over_catalog_point(bound: bool, selectable: bool, backups: Vec<Value>) -> Value {
    let job = job_name(CheckPlanKind::RestorePreflight);
    let mut evidence = backup_destination("evidence");
    evidence["spec"]["storage"]["bucket"] = json!("evidence-bucket");
    let (catalog, page) = catalog_objects(selectable);
    let routes = vec![
        route(
            "GET",
            "/trustrosters/default",
            roster(RUNNER_KEY_ID, vec!["target-id"]).to_string(),
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
        route("GET", "/backupdestinations/evidence", evidence.to_string()),
        route("GET", "/recoverycatalogs/archive", catalog.to_string()),
        route("GET", "/configmaps/archive-g1-p0", page.to_string()),
        route("GET", "/backups", list_of(backups)),
        route("GET", leak(job.clone()), finished_job(&job).to_string()),
        route(
            "GET",
            "/pods",
            list_of(vec![owned_pod(&job, terminated(0))]),
        ),
        route("GET", "/events", list_of(vec![])),
        route(
            "GET",
            "-plan",
            plan_config_map(&job, PLAN_DIGEST).to_string(),
        ),
        route("GET", "/log", relay_log(PLAN_DIGEST, vec![], None)),
        route("PATCH", "/pf-1/status", echo("Preflight", "pf-1")),
        route("PATCH", leak(job.clone()), echo("Job", &job)),
    ];
    let bytes = catalog_plan_yaml(bound);
    let mut request = restore_request(json!({
        "catalogPointRef": {"catalogRef": {"name": "archive"}, "pointId": CATALOG_POINT}
    }));
    request["restore"]["planBytes"] = json!(bytes);
    request["restore"]["planHash"] = json!(logweir_core::ids::sha256_prefixed(bytes.as_bytes()));
    let (status, _) = reconcile_with(&preflight(request), routes).await;
    status
}

fn refusing_backup(verdict: &str) -> Value {
    let mut object = recovery_point_object(None);
    object["status"]["backupId"] = json!("bk-1");
    object["status"]["evidence"] = json!({
        "receiptSha256": catalog_receipt_sha(),
        "verification": {"result": verdict}
    });
    object
}

/// **THE ROW IS WIRED TO THE OBJECTS.** `catalog_point_facts` is exercised on
/// its own in `preflight_catalog_point.rs`; this reconciles a request naming
/// `catalogPointRef` through the routes and reads the PUBLISHED verdict, so the
/// reads themselves — the catalog, its page by the name the status gives, the
/// namespace's `Backup`s — are what is under test.
///
/// KILLS: the reconciler never calling `read_catalog_point` (no row at all);
/// the Backup list not consulted (the refusal case turns `ready`); the plan not
/// passed in (the bound case turns `CatalogPointBindingMismatch`).
#[tokio::test]
async fn a_catalog_point_request_is_answered_from_the_catalog_row() {
    let ready = restore_over_catalog_point(true, true, vec![]).await;
    let row = check_entry(&ready, "recoveryPoint.state");
    assert_eq!(row["code"], "CatalogPointSelectable", "{row}");
    assert_eq!(row["state"], "ready");
    assert_eq!(row["scope"]["kind"], "RecoveryCatalog");

    let unbound = restore_over_catalog_point(false, true, vec![]).await;
    let row = check_entry(&unbound, "recoveryPoint.state");
    assert_eq!(row["code"], "CatalogPointBindingMismatch", "{row}");
    assert_eq!(unbound["result"]["state"], "notReady");

    let refused = restore_over_catalog_point(true, true, vec![refusing_backup("Invalid")]).await;
    let row = check_entry(&refused, "recoveryPoint.state");
    assert_eq!(row["code"], "CatalogPointRefusedByController", "{row}");
    assert_eq!(refused["result"]["state"], "notReady");

    // CONTROL: the controller could not look, so the catalog decides.
    let deferred =
        restore_over_catalog_point(true, true, vec![refusing_backup("NotAttempted")]).await;
    assert_eq!(
        check_entry(&deferred, "recoveryPoint.state")["code"],
        "CatalogPointSelectable"
    );

    let unselectable = restore_over_catalog_point(true, false, vec![]).await;
    assert_eq!(
        check_entry(&unselectable, "recoveryPoint.state")["code"],
        "CatalogPointNotSelectable"
    );
}
