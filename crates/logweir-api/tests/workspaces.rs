//! PROD-19.1: one namespace, workspaces by label. The adapter is the choke
//! point: a list carries the workspace selector, a read of another
//! workspace's object is `404`, and a create carries the path's workspace.

mod support;

use serde_json::json;
use support::{
    seed_discovery, seed_running_preflight, topic_line, FakeKube, TestApp, INSTALL_NS,
    LOCAL_ADMIN_ACTOR, NS_A, NS_B, WORKSPACE_LABEL,
};

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

/// A resourceVersion is a request field: one borrowed from another
/// workspace's object patches nothing there. The suspension command and a
/// check cancel, sent through `team-a` with the real version of a `team-b`
/// object, are `404` and send no PATCH; the same calls in `team-b` succeed.
#[tokio::test]
async fn a_borrowed_resource_version_patches_nothing_in_another_workspace() {
    let app = TestApp::new();
    let created = app
        .post(
            &format!("/api/v1/workspaces/{NS_B}/schedules"),
            Some("workspace-susp-0001"),
            &support::schedule_body().to_string(),
        )
        .await;
    assert_eq!(created.status, 201, "{}", created.text());
    let item = created.json()["item"].clone();
    let name = item["name"].as_str().unwrap().to_string();
    let rv = item["resourceVersion"].as_str().unwrap().to_string();
    seed_running_preflight(&app.fake, NS_B, "pf-b", LOCAL_ADMIN_ACTOR);
    let suspend = json!({"suspended": false, "expectedResourceVersion": rv}).to_string();

    app.fake.clear_requests();
    app.post(
        &format!("/api/v1/workspaces/{NS_A}/schedules/{name}:set-suspension"),
        None,
        &suspend,
    )
    .await
    .assert_problem(404, "not_found");
    app.post(
        &format!("/api/v1/workspaces/{NS_A}/preflights/pf-b:cancel"),
        None,
        "{}",
    )
    .await
    .assert_problem(404, "not_found");
    assert!(!app.fake.requests().iter().any(|r| r.method == "PATCH"));
    let schedule = app.fake.object("backupschedules", NS_B, &name).unwrap();
    assert_eq!(schedule["spec"]["suspend"], true);
    assert_eq!(schedule["metadata"]["resourceVersion"], rv.as_str());
    assert_eq!(
        app.fake.object("preflights", NS_B, "pf-b").unwrap()["spec"]["cancelRequested"],
        false
    );

    // The control: the owning workspace patches both.
    let own = app
        .post(
            &format!("/api/v1/workspaces/{NS_B}/schedules/{name}:set-suspension"),
            None,
            &suspend,
        )
        .await;
    assert_eq!(own.status, 200, "{}", own.text());
    assert_eq!(own.json()["item"]["suspended"], false);
    let cancel = app
        .post(
            &format!("/api/v1/workspaces/{NS_B}/preflights/pf-b:cancel"),
            None,
            "{}",
        )
        .await;
    assert_eq!(cancel.status, 200, "{}", cancel.text());
    assert_eq!(
        app.fake.object("preflights", NS_B, "pf-b").unwrap()["spec"]["cancelRequested"],
        true
    );
    app.fake.assert_strict();
}

/// A stored result document belongs to the object that owns it. A `team-a`
/// discovery whose status names a `team-b` discovery's chunk (digest
/// included) serves none of its rows; `team-b` reads its own chunk.
#[tokio::test]
async fn a_result_document_of_another_workspace_is_not_served() {
    let app = TestApp::new();
    let theirs = vec![topic_line("secret-b", 3, "-")];
    let b = seed_discovery(
        &app.fake,
        NS_B,
        "td-b",
        "source",
        Some(LOCAL_ADMIN_ACTOR),
        &[theirs],
    );
    let mut a = seed_discovery(
        &app.fake,
        NS_A,
        "td-a",
        "source",
        Some(LOCAL_ADMIN_ACTOR),
        &[vec![topic_line("mine-a", 1, "-")]],
    );
    a["status"]["result"] = b["status"]["result"].clone();
    app.fake.seed("topicdiscoveries", NS_A, a);

    let crossed = app
        .get(&format!(
            "/api/v1/workspaces/{NS_A}/topic-discoveries/td-a/topics"
        ))
        .await;
    crossed.assert_problem(410, "cursor_expired");
    assert!(!crossed.text().contains("secret-b"));

    let own = app
        .get(&format!(
            "/api/v1/workspaces/{NS_B}/topic-discoveries/td-b/topics"
        ))
        .await;
    assert_eq!(own.status, 200, "{}", own.text());
    assert!(own.text().contains("secret-b"));
    app.fake.assert_strict();
}

/// A create body that names another workspace's object is `422` on that
/// field, worded as for a missing one, and creates nothing. The same body in
/// the referent's own workspace is created.
#[tokio::test]
async fn a_reference_to_another_workspace_is_refused_on_create() {
    let app = TestApp::new();
    app.fake.seed(
        "kafkaclusters",
        NS_B,
        json!({
            "metadata": {"name": "source"},
            "spec": {
                "bootstrapServers": ["kafka:9092"],
                "auth": {"mode": "scramSha512", "username": "scram-user", "secretRef": {"name": "s"}, "tls": false},
                "role": "source"
            }
        }),
    );
    let body = support::schedule_body().to_string();

    let crossed = app
        .post(
            &format!("/api/v1/workspaces/{NS_A}/schedules"),
            Some("workspace-ref-0001"),
            &body,
        )
        .await;
    crossed.assert_problem(422, "validation_failed");
    let error = &crossed.json()["errors"][0];
    assert_eq!(error["field"], "sourceRef.name");
    assert_eq!(
        error["message"],
        format!("no connection named `source` in workspace `{NS_A}`")
    );
    assert_eq!(app.fake.count("backupschedules", NS_A), 0);

    // The control: the connection's own workspace creates the schedule.
    let own = app
        .post(
            &format!("/api/v1/workspaces/{NS_B}/schedules"),
            Some("workspace-ref-0002"),
            &body,
        )
        .await;
    assert_eq!(own.status, 201, "{}", own.text());
    app.fake.assert_strict();
}

/// Names are unique per install: a typed name another workspace holds is
/// refused with the name-in-use message and nothing is created; a free name
/// in the same workspace is created.
#[tokio::test]
async fn a_name_another_workspace_holds_is_in_use() {
    let app = TestApp::new();
    app.fake.seed(
        "recoverycatalogs",
        NS_B,
        json!({
            "metadata": {"name": "archive"},
            "spec": {"destinationRef": {"name": "dest-b"}, "sync": {"mode": "Index"}}
        }),
    );
    let connect = |name: &str| {
        json!({
            "name": name,
            "legacyArchive": {"url": "s3://old-archive/kafka", "credentialRef": {"name": "read-only"}},
            "syncMode": "full",
            "intervalSeconds": 0
        })
        .to_string()
    };
    let taken = app
        .post(
            &format!("/api/v1/workspaces/{NS_A}/catalogs"),
            Some("workspace-name-0001"),
            &connect("archive"),
        )
        .await;
    taken.assert_problem(409, "state_conflict");
    assert!(
        taken.text().contains(
            "The name `archive` is in use in this installation; names are shared by every workspace."
        ),
        "{}",
        taken.text()
    );
    assert_eq!(app.fake.count("recoverycatalogs", NS_A), 0);

    let free = app
        .post(
            &format!("/api/v1/workspaces/{NS_A}/catalogs"),
            Some("workspace-name-0002"),
            &connect("archive-a"),
        )
        .await;
    assert_eq!(free.status, 201, "{}", free.text());
    app.fake.assert_strict();
}
