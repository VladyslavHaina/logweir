//! **Theme A's PoC defect D3: a create answered `state_conflict` leaves
//! nothing behind.** On the PoC a `POST …/restores` whose approval name was
//! already another Restore's Approval answered `409 state_conflict` and still
//! created a Restore, which then failed `ApprovalSubjectMismatch`. The
//! taken name is now refused before the subject exists, for both kinds the
//! console signs for (`Restore`, `Switchover`); each row has its control, the
//! same request under a free approval name.
mod support;

use std::sync::Arc;

use logweir_api::approval::{ApprovalSettings, ConfirmationKey};
use logweir_core::approval_policy::ApprovalPolicySet;
use logweir_core::switchover::{Expected, PlanGroup, PlanTopic, RestoreRef, SwitchoverPlan};
use logweir_evidence::keys::SigningKey;
use serde_json::{json, Value};
use support::{approvals_posted, FakeKube, Options, TestApp, NS_A};

/// `team-a` under a `confirm` (Ordinary) policy, served by a localAdmin
/// console holding its confirmation key.
fn app() -> TestApp {
    let policies = ApprovalPolicySet::parse(&format!(
        "allowOrdinaryConfirmation: true\npolicies:\n  - name: team-ordinary\n    mode: \
         Ordinary\nnamespaces:\n  {NS_A}: team-ordinary\n"
    ))
    .expect("valid");
    TestApp::with(
        FakeKube::new(),
        Options {
            approval: Arc::new(ApprovalSettings {
                policies,
                confirmation: Some(
                    ConfirmationKey::from_key(SigningKey::generate_ed25519()).expect("key"),
                ),
                ..ApprovalSettings::default()
            }),
            ..Options::default()
        },
    )
}

/// Another object's Approval under `name`.
fn seed_foreign_approval(app: &TestApp, name: &str, kind: &str) {
    app.fake.seed(
        "approvals",
        NS_A,
        json!({
            "metadata": {"name": name},
            "spec": {"subjectRef": {"kind": kind, "name": "someone-else"},
                     "planHash": "sha256:x", "approvalBytes": "{}", "sidecarBytes": "{}"}
        }),
    );
}

fn sha(c: char) -> String {
    format!("sha256:{}", c.to_string().repeat(64))
}

fn switchover_body(approval: &str) -> Value {
    let plan = serde_yaml::to_string(&SwitchoverPlan {
        format_version: "1.0.0".into(),
        restore: RestoreRef {
            name: "incident".into(),
            uid: "r-uid".into(),
            plan_hash: sha('a'),
            scorecard_sha256: sha('b'),
            mapping_sha256: sha('c'),
        },
        rollback_of: None,
        groups: vec![PlanGroup {
            source: "app".into(),
            target: "app".into(),
            topics: [(
                "r-orders".to_string(),
                PlanTopic {
                    offsets: [(0, 40)].into(),
                    approximate: vec![],
                    expected: Expected::none(),
                },
            )]
            .into(),
        }],
    })
    .unwrap();
    json!({
        "planBytes": plan,
        "planHash": logweir_core::ids::sha256_prefixed(plan.as_bytes()),
        "approvalRef": {"name": approval},
        "restoreRef": {"name": "incident", "uid": "r-uid"},
        "deadlineSeconds": 900
    })
}

/// **A Restore whose approval name is taken creates nothing.** The 409 names
/// the Approval and says nothing was created; no Restore and no Approval is
/// written. CONTROL: the same request under a free name creates the Restore
/// and signs its confirmation.
///
/// KILLS: `refuse_taken_approval` removed from the restore create (the
/// Restore is created, then refused, as on the PoC).
#[tokio::test]
async fn a_restore_refused_for_a_taken_approval_name_creates_nothing() {
    let app = app();
    seed_foreign_approval(&app, "approval-1234abcd", "Restore");
    let path = format!("/api/v1/namespaces/{NS_A}/restores");
    let body = support::restore_body(&support::golden_plan());
    let refused = app
        .post(&path, Some("d3-restore-0001"), &body.to_string())
        .await;
    refused.assert_problem(409, "state_conflict");
    let text = refused.text();
    assert!(text.contains("approval-1234abcd"), "{text}");
    assert!(text.contains("Nothing was created"), "{text}");
    assert_eq!(app.fake.count("restores", NS_A), 0, "no Restore is left");
    assert!(approvals_posted(&app.fake).is_empty());
    assert!(
        !app.fake
            .requests()
            .iter()
            .any(|r| r.method == "POST" && r.path.ends_with("/restores")),
        "nothing was even attempted"
    );

    // CONTROL: a free approval name.
    let mut free = body;
    free["approvalRef"]["name"] = json!("approval-free-0001");
    let created = app
        .post(&path, Some("d3-restore-0002"), &free.to_string())
        .await;
    assert_eq!(created.status, 201, "{}", created.text());
    assert_eq!(created.json()["authorization"]["state"], "confirmed");
    assert_eq!(app.fake.count("restores", NS_A), 1);
    assert_eq!(approvals_posted(&app.fake).len(), 1);
}

/// **The same for a Switchover**, whose create shares the restore's gates
/// and routing. CONTROL: a free approval name creates it.
///
/// KILLS: `refuse_taken_approval` removed from the switchover create.
#[tokio::test]
async fn a_switchover_refused_for_a_taken_approval_name_creates_nothing() {
    let app = app();
    seed_foreign_approval(&app, "cutover", "Switchover");
    let path = format!("/api/v1/namespaces/{NS_A}/switchovers");
    let refused = app
        .post(
            &path,
            Some("d3-switchover-0001"),
            &switchover_body("cutover").to_string(),
        )
        .await;
    refused.assert_problem(409, "state_conflict");
    assert!(refused.text().contains("Nothing was created"));
    assert_eq!(
        app.fake.count("switchovers", NS_A),
        0,
        "no Switchover is left"
    );
    assert!(approvals_posted(&app.fake).is_empty());

    let created = app
        .post(
            &path,
            Some("d3-switchover-0002"),
            &switchover_body("cutover-free").to_string(),
        )
        .await;
    assert_eq!(created.status, 201, "{}", created.text());
    assert_eq!(app.fake.count("switchovers", NS_A), 1);
    assert_eq!(approvals_posted(&app.fake).len(), 1);
}
