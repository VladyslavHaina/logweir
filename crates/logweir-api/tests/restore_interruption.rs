//! **PROD-07.2: cancel and clean up, over the API.** `POST
//! …/restores/{name}/cancel` and `…/cleanup` set one annotation each, once
//! (metadata only; the sealed spec is untouched), in a phase that allows it,
//! for whoever may create restores in the workspace; the view names the
//! interruption.

mod support;

use serde_json::{json, Value};
use support::{repo_root, FakeKube, TestApp, INSTALL_NS, NS_A};

const CANCEL: &str = "logweir.dev/cancel-requested-by";
const CLEANUP: &str = "logweir.dev/cleanup-requested-by";

fn seed(fake: &FakeKube, name: &str, status: Value) -> Value {
    let path =
        repo_root().join("crates/logweir-api/tests/fixtures/restore-legacy-no-progress.json");
    let mut r: Value = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    r["metadata"]["name"] = json!(name);
    r["metadata"]["uid"] = json!(format!("00000000-0000-4000-9000-{:012}", name.len()));
    r["metadata"].as_object_mut().unwrap().remove("annotations");
    r["status"] = status;
    fake.seed("restores", NS_A, r)
}

fn annotations(fake: &FakeKube, name: &str) -> Value {
    fake.object("restores", NS_A, name).expect("the restore")["metadata"]["annotations"].clone()
}

fn patches(fake: &FakeKube) -> usize {
    fake.requests()
        .iter()
        .filter(|r| r.method == "PATCH")
        .count()
}

/// A running restore's cancel sets the one annotation, naming the actor, in
/// the install namespace; a second cancel writes nothing (IR-8's two POSTs).
/// KILLS: a cancel that patches the spec (the fake refuses it), or one that
/// writes on every click.
#[tokio::test]
async fn a_cancel_sets_one_annotation_once() {
    let app = TestApp::new();
    seed(&app.fake, "rst-running", json!({"phase": "Running"}));
    let path = format!("/api/v1/workspaces/{NS_A}/restores/rst-running/cancel");
    let first = app.post(&path, None, "").await;
    assert_eq!(first.status, 200, "{}", first.text());
    assert!(
        annotations(&app.fake, "rst-running")[CANCEL]
            .as_str()
            .is_some_and(|by| !by.is_empty()),
        "{}",
        annotations(&app.fake, "rst-running")
    );
    assert_eq!(patches(&app.fake), 1);
    for r in app.fake.requests().iter().filter(|r| r.method == "PATCH") {
        assert!(
            r.path.starts_with(&format!(
                "/apis/logweir.dev/v1alpha1/namespaces/{INSTALL_NS}/"
            )),
            "{}",
            r.path
        );
    }
    let second = app.post(&path, None, "").await;
    assert_eq!(second.status, 200, "{}", second.text());
    assert_eq!(patches(&app.fake), 1, "a repeated cancel writes nothing");
    app.fake.assert_strict();
}

/// A restore that has ended cannot be cancelled: 409, nothing written.
#[tokio::test]
async fn an_ended_restore_is_not_cancelled() {
    let app = TestApp::new();
    for (name, phase) in [("rst-done", "Succeeded"), ("rst-stopped", "Interrupted")] {
        seed(&app.fake, name, json!({"phase": phase}));
        app.post(
            &format!("/api/v1/workspaces/{NS_A}/restores/{name}/cancel"),
            None,
            "",
        )
        .await
        .assert_problem(409, "state_conflict");
    }
    assert_eq!(patches(&app.fake), 0);
}

/// Clean up only on an `Interrupted` restore whose reconcile signed; before
/// that, 409 and nothing written. The view offers it exactly then.
#[tokio::test]
async fn cleanup_needs_a_signed_reconcile() {
    let app = TestApp::new();
    seed(
        &app.fake,
        "rst-verifying",
        json!({"phase": "Interrupted", "reason": "Cancelled",
               "interruption": {"cause": "Cancelled", "verification": "Running"}}),
    );
    seed(
        &app.fake,
        "rst-listed",
        json!({"phase": "Interrupted", "reason": "Cancelled",
               "interruption": {"cause": "Cancelled", "verification": "Done",
                                "reconcileRunId": "01RECONCILE"}}),
    );
    let early = app
        .post(
            &format!("/api/v1/workspaces/{NS_A}/restores/rst-verifying/cleanup"),
            None,
            "",
        )
        .await;
    early.assert_problem(409, "state_conflict");
    assert_eq!(patches(&app.fake), 0);

    let view = app
        .get(&format!("/api/v1/workspaces/{NS_A}/restores/rst-listed"))
        .await
        .json();
    assert_eq!(
        view["item"]["interruption"]["cleanupOffered"], true,
        "{view}"
    );
    assert_eq!(view["item"]["interruption"]["cause"], "Cancelled");
    assert_eq!(view["item"]["operation"]["state"], "interrupted", "{view}");

    let ok = app
        .post(
            &format!("/api/v1/workspaces/{NS_A}/restores/rst-listed/cleanup"),
            None,
            "",
        )
        .await;
    assert_eq!(ok.status, 200, "{}", ok.text());
    assert!(annotations(&app.fake, "rst-listed")[CLEANUP].is_string());
    assert!(annotations(&app.fake, "rst-listed")[CANCEL].is_null());
    app.fake.assert_strict();
}

/// A cancel before the Job reads as `cancelled`, never as a plain failure.
#[tokio::test]
async fn a_cancelled_restore_reads_as_cancelled() {
    let app = TestApp::new();
    seed(
        &app.fake,
        "rst-cancelled",
        json!({"phase": "Failed", "reason": "Cancelled"}),
    );
    let view = app
        .get(&format!("/api/v1/workspaces/{NS_A}/restores/rst-cancelled"))
        .await
        .json();
    assert_eq!(view["item"]["operation"]["state"], "cancelled", "{view}");
}
