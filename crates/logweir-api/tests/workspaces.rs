//! PROD-19.1: one namespace, workspaces by label. The adapter is the choke
//! point: a list carries the workspace selector, a read of another
//! workspace's object is `404`, and a create carries the path's workspace.

mod support;

use serde_json::json;
use support::{FakeKube, TestApp, INSTALL_NS, NS_A, NS_B, WORKSPACE_LABEL};

fn seed_backup(fake: &FakeKube, workspace: &str, name: &str) {
    fake.seed(
        "backups",
        workspace,
        json!({
            "metadata": {"name": name},
            "spec": {"sourceRef": {"name": "source"}, "topics": ["orders"], "archive": {"url": "s3://b/p"}, "triggeredBy": "schedule", "deadlineSeconds": 1800}
        }),
    );
}

/// A list in `team-a` returns only `team-a` objects, across pages, and every
/// page asks the API server for the workspace label in the install namespace.
#[tokio::test]
async fn a_list_holds_one_workspace_across_pages() {
    let app = TestApp::new();
    for i in 0..3 {
        seed_backup(&app.fake, NS_A, &format!("a-{i}"));
        seed_backup(&app.fake, NS_B, &format!("b-{i}"));
    }
    let base = format!("/api/v1/workspaces/{NS_A}/backups");
    let mut url = format!("{base}?limit=2");
    let mut seen = Vec::new();
    loop {
        let page = app.get(&url).await;
        assert_eq!(page.status, 200, "{}", page.text());
        let v = page.json();
        for item in v["items"].as_array().unwrap() {
            seen.push(item["name"].as_str().unwrap().to_string());
        }
        match v["page"]["nextCursor"].as_str() {
            Some(cursor) => url = format!("{base}?limit=2&cursor={cursor}"),
            None => break,
        }
    }
    assert_eq!(seen, ["a-0", "a-1", "a-2"]);
    for r in app.fake.requests() {
        assert!(
            r.path.starts_with(&format!(
                "/apis/logweir.dev/v1alpha1/namespaces/{INSTALL_NS}/"
            )),
            "{}",
            r.path
        );
        assert!(
            r.query
                .contains("labelSelector=logweir.dev%2Fworkspace%3Dteam-a"),
            "{}",
            r.query
        );
    }
    app.fake.assert_strict();
}

/// A read of another workspace's object is the not-found answer, exactly as
/// for a missing one; the same object read in its own workspace is found.
#[tokio::test]
async fn another_workspaces_object_is_not_found() {
    let app = TestApp::new();
    seed_backup(&app.fake, NS_B, "b-only");

    let crossed = app
        .get(&format!("/api/v1/workspaces/{NS_A}/backups/b-only"))
        .await;
    crossed.assert_problem(404, "not_found");
    let missing = app
        .get(&format!("/api/v1/workspaces/{NS_A}/backups/no-such-backup"))
        .await;
    missing.assert_problem(404, "not_found");

    // The control: in its own workspace it is there.
    let own = app
        .get(&format!("/api/v1/workspaces/{NS_B}/backups/b-only"))
        .await;
    assert_eq!(own.status, 200, "{}", own.text());
    app.fake.assert_strict();
}

/// A create lands in the install namespace with the path's workspace label.
/// The same Idempotency-Key in another workspace is another create: the name
/// hashes the workspace.
#[tokio::test]
async fn a_create_carries_the_paths_workspace() {
    let app = TestApp::new();
    let create = |workspace: &str| {
        let path = format!("/api/v1/workspaces/{workspace}/connections");
        let app = &app;
        async move {
            let created = app
                .post(
                    &path,
                    Some("workspace-create-0001"),
                    &support::connection_body().to_string(),
                )
                .await;
            assert_eq!(created.status, 201, "{}", created.text());
            created.json()["item"]["name"]
                .as_str()
                .expect("a name")
                .to_string()
        }
    };
    let in_b = create(NS_B).await;
    let stored = app
        .fake
        .object("kafkaclusters", NS_B, &in_b)
        .expect("stored in team-b");
    assert_eq!(stored["metadata"]["namespace"], INSTALL_NS);
    assert_eq!(stored["metadata"]["labels"][WORKSPACE_LABEL], NS_B);
    assert!(app.fake.object("kafkaclusters", NS_A, &in_b).is_none());

    let in_a = create(NS_A).await;
    assert_ne!(in_a, in_b);
    assert_eq!(app.fake.count("kafkaclusters", NS_A), 1);
    assert_eq!(app.fake.count("kafkaclusters", NS_B), 1);
    app.fake.assert_strict();
}
