//! **PROD-04.2 §3.8: `Preflight` operation `Switchover`**, the review before
//! a `Switchover`, at the controller: which `Restore` is admitted to a
//! review (the `Switchover` controller's own admission, read-only), what the
//! rendered `switchoverReadiness` plan carries, what the check pod is given,
//! and which rows the controller and the Job answer.

use logweir_core::check_contract::{
    CheckCode, CheckId, CheckPlanKind, CheckRequest, CheckState, SwitchoverReadinessGroup,
};
use logweir_core::destination::DestinationRole;
use logweir_core::execution_contract::{EvidenceKey, EvidenceKeyring};
use serde_json::{json, Value};
use std::collections::BTreeSet;
use weirkeeper::controllers::preflight::{
    self as pf, Inputs, RelayFacts, SwitchoverFacts, SwitchoverRestoreFacts,
};
use weirkeeper::crds::preflight::{
    PreflightOperation, SwitchoverPreflightGroup, SwitchoverPreflightRequest, UidRef,
};
use weirkeeper::crds::restore::Restore;

const NS: &str = "team-a";
/// The workspace every fixture below is in (PROD-19.1).
const WS: &str = "dev";

fn now() -> chrono::DateTime<chrono::Utc> {
    "2026-10-13T12:00:00Z".parse().expect("an instant")
}

fn keyring() -> EvidenceKeyring {
    EvidenceKeyring {
        format_version: "1.0.0".into(),
        keys: vec![EvidenceKey {
            public_key_pem: "-----BEGIN PUBLIC KEY-----\nAA==\n-----END PUBLIC KEY-----\n".into(),
            trust: logweir_core::trust::TrustedKey {
                key_id: "a".repeat(64),
                principal_id: "install:x".into(),
                usages: vec![logweir_core::trust::KeyUsage::EvidenceSigning],
                not_before: "2026-01-01T00:00:00Z".parse().expect("t"),
                not_after: "2027-01-01T00:00:00Z".parse().expect("t"),
                state: logweir_core::trust::KeyState::Active,
                retired_at: None,
                revoked_at: None,
                revocation_reason: None,
                revocation_effective_from: None,
            },
        }],
    }
}

fn ready_restore() -> SwitchoverRestoreFacts {
    SwitchoverRestoreFacts {
        name: "incident".into(),
        uid: "r-uid".into(),
        plan_hash: logweir_core::ids::sha256_prefixed(b"restore plan"),
        phase: Some("Succeeded".into()),
        verdict: Some("Valid".into()),
        scorecard_key: Some("logweir/drills/run-1.json".into()),
        scratch: false,
        evidence_destination: true,
    }
}

fn facts(restore: Option<SwitchoverRestoreFacts>) -> SwitchoverFacts {
    SwitchoverFacts {
        requested: "incident".into(),
        requested_uid: Some("r-uid".into()),
        restore,
        keyring: Ok(keyring()),
        groups: vec![SwitchoverReadinessGroup {
            source: "app".into(),
            target: Some("app-v2".into()),
        }],
    }
}

fn restore_object(mode: &str, phase: &str, verdict: &str, evidence_ref: bool) -> Restore {
    let mut spec = json!({
        "planBytes": "restore plan", "approvalRef": {"name": "a"},
        "sourceArchive": {"url": "s3://b/p"}, "backupSetRef": "b1",
        "pointInTime": "2026-10-13T08:00:00Z",
        "target": {"clusterRef": {"name": "target"}, "mode": mode,
                   "topicNaming": {"prefix": "r-"}},
        "deadlineSeconds": 600
    });
    if evidence_ref {
        spec["sourceDestinationRef"] = json!({"name": "archive"});
        spec["evidenceDestinationRef"] = json!({"name": "evidence"});
        spec["sourceArchive"] = json!({"url": "logweir-destination://archive"});
    }
    serde_json::from_value(json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "Restore",
        "metadata": {"labels": {"logweir.dev/workspace": WS}, "name": "incident", "namespace": NS, "uid": "r-uid", "generation": 1},
        "spec": spec,
        "status": {"phase": phase,
                   "evidence": {"scorecardKey": "logweir/drills/run-1.json",
                                "verification": {"result": verdict}}}
    }))
    .expect("a Restore")
}

fn row(f: Option<&SwitchoverFacts>) -> logweir_core::check_contract::CheckOutcome {
    pf::switchover_restore_row(f, now())
}

/// **A review is admitted on the `Switchover` controller's own facts**: the
/// `Restore` by name and UID, not scratch, `Succeeded` with a `Valid`
/// verdict and a scorecard key, evidence in a saved destination, and keys
/// to verify it under. Each refusal is `notReady` by name, and holds the Job
/// back (`plan_blockers`).
/// KILLS: a review rendered over a replaced, running, failed or scratch
/// restore, over an inline archive this build cannot read, or with no key to
/// verify the scorecard.
#[test]
fn a_review_is_admitted_on_the_switchover_controllers_own_facts() {
    let ready = row(Some(&facts(Some(ready_restore()))));
    assert_eq!(
        (ready.state, ready.code),
        (CheckState::Ready, CheckCode::RestoreSucceeded)
    );
    let refused = |f: SwitchoverFacts| row(Some(&f));
    let with = |edit: &dyn Fn(&mut SwitchoverRestoreFacts)| {
        let mut r = ready_restore();
        edit(&mut r);
        refused(facts(Some(r)))
    };
    let cases: Vec<(
        &str,
        logweir_core::check_contract::CheckOutcome,
        CheckCode,
        &str,
    )> = vec![
        (
            "absent",
            refused(facts(None)),
            CheckCode::RestoreNotFound,
            "no Restore named",
        ),
        (
            "replaced",
            with(&|r| r.uid = "other".into()),
            CheckCode::RestoreNotFound,
            "was replaced",
        ),
        (
            "scratch",
            with(&|r| r.scratch = true),
            CheckCode::RestoreNotSucceeded,
            "RestoreIsScratch",
        ),
        (
            "running",
            with(&|r| r.phase = Some("Running".into())),
            CheckCode::RestoreNotSucceeded,
            "RestoreNotSucceeded",
        ),
        (
            "unverified",
            with(&|r| r.verdict = Some("Invalid".into())),
            CheckCode::RestoreNotSucceeded,
            "Invalid",
        ),
        (
            "no scorecard",
            with(&|r| r.scorecard_key = None),
            CheckCode::RestoreNotSucceeded,
            "no scorecard key",
        ),
        (
            "inline archive",
            with(&|r| r.evidence_destination = false),
            CheckCode::ArchiveUrlUnreadable,
            "inline archive",
        ),
        (
            "no keys",
            refused(SwitchoverFacts {
                keyring: Err("two policies claim it".into()),
                ..facts(Some(ready_restore()))
            }),
            CheckCode::TrustRosterNotLoaded,
            "two policies claim it",
        ),
    ];
    for (name, out, code, words) in cases {
        assert_eq!(
            (out.state, out.code),
            (CheckState::NotReady, code),
            "{name}"
        );
        assert!(out.message.contains(words), "{name}: {}", out.message);
        let inputs = Inputs {
            operation: PreflightOperation::Switchover,
            ..Inputs::default()
        };
        assert_eq!(
            inputs
                .plan_blockers(std::slice::from_ref(&out))
                .iter()
                .map(|b| b.id)
                .collect::<Vec<_>>(),
            vec![CheckId::SwitchoverRestore],
            "{name}: no Job is rendered"
        );
    }
}

/// **The facts are read off the objects**, the way the `Switchover`
/// controller reads them: phase, verdict, scorecard key, scratch, the saved
/// evidence destination, and the requested groups.
#[test]
fn the_facts_are_read_off_the_restore_and_the_request() {
    let request = SwitchoverPreflightRequest {
        restore_ref: UidRef {
            name: "incident".into(),
            uid: Some("r-uid".into()),
        },
        groups: Some(vec![SwitchoverPreflightGroup {
            source: "app".into(),
            target: None,
        }]),
    };
    let unreadable = kube::Error::Api(kube::core::ErrorResponse {
        status: "Failure".into(),
        message: "forbidden".into(),
        reason: "Forbidden".into(),
        code: 403,
    });
    let restore = restore_object("newTopic", "Succeeded", "Valid", true);
    let f = SwitchoverFacts::of(&request, Some(&restore), Err(&unreadable));
    let r = f.restore.clone().expect("found");
    assert_eq!(r.uid, "r-uid");
    assert_eq!(r.phase.as_deref(), Some("Succeeded"));
    assert_eq!(r.verdict.as_deref(), Some("Valid"));
    assert_eq!(
        r.scorecard_key.as_deref(),
        Some("logweir/drills/run-1.json")
    );
    assert!(!r.scratch && r.evidence_destination);
    assert_eq!(
        r.plan_hash,
        logweir_core::ids::sha256_prefixed(b"restore plan")
    );
    assert_eq!(
        f.groups,
        vec![SwitchoverReadinessGroup {
            source: "app".into(),
            target: None
        }]
    );
    assert!(f
        .keyring
        .as_ref()
        .is_err_and(|e| e.contains("could not be read")));
    let inline = SwitchoverFacts::of(
        &request,
        Some(&restore_object("scratch", "Running", "", false)),
        Err(&unreadable),
    );
    let r = inline.restore.expect("found");
    assert!(r.scratch && !r.evidence_destination);
    assert!(SwitchoverFacts::of(&request, None, Err(&unreadable))
        .restore
        .is_none());
}

fn target_cluster() -> weirkeeper::crds::kafka_cluster::KafkaCluster {
    serde_json::from_value(json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "KafkaCluster",
        "metadata": {"labels": {"logweir.dev/workspace": WS}, "name": "target", "namespace": NS, "uid": "c-uid", "generation": 1},
        "spec": {
            "bootstrapServers": ["target-kafka:9092"],
            "role": "target",
            "auth": {"mode": "scramSha512", "username": "restorer", "tls": true,
                     "secretRef": {"name": "kafka-target", "passwordKey": "password"}}
        },
        "status": {"clusterId": "cluster-t"}
    }))
    .expect("a KafkaCluster")
}

fn evidence_destination() -> Value {
    let secret = |name: &str| {
        json!({"mode": "SecretKeys", "secret": {"name": name,
               "accessKeyIdKey": "AWS_ACCESS_KEY_ID",
               "secretAccessKeyKey": "AWS_SECRET_ACCESS_KEY"}})
    };
    json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "BackupDestination",
        "metadata": {"labels": {"logweir.dev/workspace": WS}, "name": "evidence", "namespace": NS, "uid": "d-uid", "generation": 1},
        "spec": {
            "storage": {"provider": "S3", "bucket": "evidence", "addressing": "PathStyle"},
            "transport": {"security": "TLS"},
            "access": {"archiveWrite": secret("evidence-writer"),
                       "evidenceRead": secret("evidence-reader")}
        },
        "status": {"observedGeneration": 1, "reason": "Valid",
                   "conditions": [{"type": "Valid", "status": "True", "observedGeneration": 1,
                                   "reason": "Valid", "message": "ok"}]}
    })
}

fn review_inputs() -> Inputs {
    let policy = weirkeeper::check::policy::Policy::defaults();
    let destination: weirkeeper::crds::backup_destination::BackupDestination =
        serde_json::from_value(evidence_destination()).expect("a destination");
    Inputs {
        operation: PreflightOperation::Switchover,
        namespace: NS.into(),
        timeout_seconds: 120,
        policy_digest: policy.digest(),
        policy: None,
        cluster_name: Some("target".into()),
        cluster_uid: Some("c-uid".into()),
        connection: Some(weirkeeper::connection::resolve(
            &target_cluster(),
            weirkeeper::connection::ConnectionUse::PreflightTarget,
        )),
        archive_name: Some("evidence".into()),
        archive: Some(weirkeeper::destination::resolve(
            &destination,
            DestinationRole::EvidenceRead,
            &policy,
        )),
        roles: vec![DestinationRole::EvidenceRead],
        switchover: Some(facts(Some(ready_restore()))),
        ..Inputs::default()
    }
}

/// **The rendered review**: a `switchoverReadiness` plan naming the
/// restore, its plan hash and scorecard key, the target as the restore's own
/// principal, the evidence destination with NO grant binding, the
/// namespace's keys and the requested groups; it validates. The pod is
/// given the target's password and the evidence-READ grant's keys, and no
/// signing key; the Job answers exactly four rows, the controller the
/// restore, the target and the destination, and no cluster-identity or signer
/// row.
/// KILLS: a review pod holding the signing key or the archive-write grant, a
/// plan the runner would refuse, a `job_rows` mirror that pins the verdict at
/// `unknown`.
#[test]
fn a_review_renders_its_plan_and_projects_the_target_and_the_evidence_read_grant() {
    let inputs = review_inputs();
    let owner = weirkeeper::job::RunnerOwner {
        api_version: "logweir.dev/v1alpha1".into(),
        kind: "Preflight".into(),
        name: "pf-1".into(),
        uid: "pf-uid".into(),
        workspace: None,
    };
    let shape = pf::build_job_shape(&inputs, &owner, &weirkeeper::job::RunnerImage::default())
        .expect("the shape renders");
    shape.plan.validate().expect("a plan the runner accepts");
    assert_eq!(shape.spec.kind, CheckPlanKind::SwitchoverReadiness);
    let CheckRequest::SwitchoverReadiness(r) = &shape.plan.request else {
        panic!("a switchoverReadiness plan, got {:?}", shape.plan.kind())
    };
    assert_eq!(
        (r.restore_name.as_str(), r.restore_uid.as_str()),
        ("incident", "r-uid")
    );
    assert_eq!(r.restore_plan_sha256, ready_restore().plan_hash);
    assert_eq!(r.scorecard_key, "logweir/drills/run-1.json");
    assert_eq!(r.target.principal, "User:restorer");
    assert!(r.target.password_env.is_some(), "the name, never the value");
    assert_eq!(r.evidence.name, "evidence");
    assert!(r.evidence.grant_bindings.is_empty());
    assert_eq!(r.keyring, keyring());
    assert_eq!(r.groups[0].target.as_deref(), Some("app-v2"));
    let secrets: BTreeSet<&str> = shape
        .spec
        .env_from_secret
        .iter()
        .map(|e| e.secret_name.as_str())
        .collect();
    assert!(secrets.contains("kafka-target"), "{secrets:?}");
    assert!(secrets.contains("evidence-reader"), "{secrets:?}");
    assert!(!secrets.contains("evidence-writer"), "{secrets:?}");
    assert!(
        shape
            .spec
            .secret_mounts
            .iter()
            .all(|m| m.secret_name != "logweir-signing-key"),
        "a review signs nothing"
    );
    assert_eq!(
        pf::job_rows(&shape.plan.request, false),
        pf::SWITCHOVER_JOB_ROWS.into_iter().collect()
    );
    assert_eq!(
        pf::unrendered_job_rows(PreflightOperation::Switchover),
        pf::SWITCHOVER_JOB_ROWS.into_iter().collect()
    );

    let (rows, blocked) =
        inputs.controller_outcomes(&RelayFacts::default(), None, true, None, now());
    assert!(!blocked);
    let ids: BTreeSet<CheckId> = rows.iter().map(|c| c.id).collect();
    for id in [
        CheckId::SwitchoverRestore,
        CheckId::TargetResolved,
        CheckId::DestinationResolved,
        CheckId::ConfigurationEgress,
        CheckId::TargetCredentialProjected,
    ] {
        assert!(ids.contains(&id), "{id} missing: {ids:?}");
    }
    for id in [
        CheckId::TargetClusterIdentity,
        CheckId::ConnectionClusterIdentity,
        CheckId::SignerRostered,
    ] {
        assert!(!ids.contains(&id), "{id} is not a review's: {ids:?}");
    }
    let restore = rows
        .iter()
        .find(|c| c.id == CheckId::SwitchoverRestore)
        .expect("the row");
    assert_eq!(restore.state, CheckState::Ready);
}
