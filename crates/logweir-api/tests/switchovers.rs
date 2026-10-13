//! PROD-04.2: the switchover routes in an unbound namespace (the legacy v1
//! approval path): a create that is held to its plan, the projection, and an
//! approval submission held to the subject kind.
mod support;

use logweir_core::switchover::{Expected, PlanGroup, PlanTopic, RestoreRef, SwitchoverPlan};
use serde_json::json;
use support::{TestApp, NS_A};

fn path(ns: &str, resource: &str) -> String {
    format!("/api/v1/namespaces/{ns}/{resource}")
}

fn sha(c: char) -> String {
    format!("sha256:{}", c.to_string().repeat(64))
}

fn plan() -> String {
    serde_yaml::to_string(&SwitchoverPlan {
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
    .unwrap()
}

fn body(plan: &str, restore_name: &str) -> serde_json::Value {
    json!({
        "planBytes": plan,
        "planHash": logweir_core::ids::sha256_prefixed(plan.as_bytes()),
        "approvalRef": {"name": "cutover"},
        "restoreRef": {"name": restore_name, "uid": "r-uid"},
        "deadlineSeconds": 900
    })
}

#[tokio::test]
async fn a_switchover_is_created_held_to_its_plan_and_approved_by_kind() {
    let app = TestApp::new();
    let p = plan();
    // Refused before anything exists: a restoreRef that is not the plan's.
    let wrong = app
        .post(
            &path(NS_A, "switchovers"),
            Some("key-wrong-0001"),
            &body(&p, "other").to_string(),
        )
        .await;
    assert_eq!(
        wrong.status.as_u16(),
        422,
        "{}",
        String::from_utf8_lossy(&wrong.body)
    );
    let created = app
        .post(
            &path(NS_A, "switchovers"),
            Some("key-create-0001"),
            &body(&p, "incident").to_string(),
        )
        .await;
    assert_eq!(
        created.status.as_u16(),
        201,
        "{}",
        String::from_utf8_lossy(&created.body)
    );
    let v = created.json();
    assert_eq!(v["authorization"]["state"], "awaitingApproval");
    assert_eq!(v["item"]["restore"]["name"], "incident");
    let name = v["item"]["name"].as_str().unwrap().to_string();
    let listed = app.get(&path(NS_A, "switchovers")).await;
    assert_eq!(listed.json()["items"][0]["name"], name.as_str());
    assert!(listed.json()["items"][0].get("planBytes").is_none());
    // An approval document for a Restore is refused by kind.
    let doc = json!({"approver": "a", "ticket": "t", "plan_hash": logweir_core::ids::sha256_prefixed(p.as_bytes()),
                     "approved_at": "2026-10-13T00:00:00Z", "subject_kind": "Restore"});
    let sidecar = json!({"payloadType": "application/vnd.logweir.drill-approval+json;version=1.0.0", "payload": "", "signatures": []});
    let submit = |doc: serde_json::Value| {
        json!({"approvalBytes": doc.to_string(), "sidecarBytes": sidecar.to_string()}).to_string()
    };
    let refused = app
        .post(
            &format!("{}/{name}/approval", path(NS_A, "switchovers")),
            None,
            &submit(doc.clone()),
        )
        .await;
    assert_eq!(
        refused.status.as_u16(),
        422,
        "{}",
        String::from_utf8_lossy(&refused.body)
    );
    let mut ok = doc;
    ok["subject_kind"] = json!("Switchover");
    let recorded = app
        .post(
            &format!("{}/{name}/approval", path(NS_A, "switchovers")),
            None,
            &submit(ok),
        )
        .await;
    assert_eq!(
        recorded.status.as_u16(),
        201,
        "{}",
        String::from_utf8_lossy(&recorded.body)
    );
}
