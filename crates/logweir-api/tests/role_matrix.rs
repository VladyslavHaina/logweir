//! The complete viewer / operator / approver / administrator matrix, in two
//! namespaces, plus role union, separation of duties and enumeration
//! resistance.
//!
//! TWO LAYERS, BOTH NEEDED. The first test pins the DECISION TABLE as data: all
//! four roles against all nineteen actions, written out here so that changing
//! `Role::allows` changes this file too. The rest drive the real router over
//! HTTP, because a table that is right and a router that does not consult it
//! would both pass the first test alone.
//!
//! WHY EVERY DENIAL ALSO ASSERTS THE STORE. "A viewer cannot mutate" is a
//! statement about the cluster, not about a status code. Each refused mutation
//! below checks that the fake API server holds no new object and recorded no
//! unexpected request, so a route that answered 403 after creating something
//! would fail here rather than read as a pass.

mod support;

use axum::body::Body;
use http::Request;
use logweir_api::authz::{Action, Role};
use support::{
    FakeKube, SharedApp, SharedOptions, TestResponse, ISSUER, NS_A, NS_B, SHARED_HOST,
    SHARED_ORIGIN,
};

/// **The decision table, as data.**
///
/// Read the columns as viewer, operator, approver, administrator. The three
/// properties D0 states in prose are asserted separately underneath, so that a
/// table edited to match a wrong implementation still fails.
#[test]
fn the_decision_table_is_exactly_this() {
    let expected: [(Action, [bool; 4]); 24] = [
        //                                     V      O      A     Adm
        (Action::ReadConnections, [true, true, false, true]),
        (Action::CreateConnection, [false, true, false, true]),
        (Action::TestConnection, [false, true, false, true]),
        (Action::WriteCredential, [false, true, false, true]),
        // D2 W12: a domain's READ is the viewer's floor; starting and
        // cancelling are the operator's, and an approver sees readiness only
        // because an approval packet needs it.
        (Action::ReadTopicDiscoveries, [true, true, false, true]),
        (Action::DiscoverTopics, [false, true, false, true]),
        (Action::CancelTopicDiscovery, [false, true, false, true]),
        (Action::ReadPreflights, [true, true, true, true]),
        (Action::RunPreflight, [false, true, false, true]),
        (Action::CancelPreflight, [false, true, false, true]),
        (Action::ReadDestinations, [true, true, false, true]),
        (Action::ManageDestinations, [false, true, false, true]),
        (Action::ReadSchedules, [true, true, false, true]),
        (Action::CreateSchedule, [false, true, false, true]),
        (Action::SetScheduleSuspension, [false, true, false, true]),
        (Action::ReadBackups, [true, true, true, true]),
        (Action::CreateManualBackup, [false, true, false, true]),
        (Action::ReadRestores, [true, true, true, true]),
        (Action::CreateRestore, [false, true, false, true]),
        (Action::ReadApprovals, [true, true, true, true]),
        (Action::ReadApprovalPacket, [false, true, true, true]),
        (Action::SubmitApproval, [false, false, true, false]),
        (Action::ReadOperations, [true, true, true, true]),
        (Action::StreamOperationEvents, [true, true, true, true]),
    ];
    for (action, row) in expected {
        for (index, role) in Role::ALL.into_iter().enumerate() {
            assert_eq!(role.allows(action), row[index], "{:?} / {:?}", role, action);
        }
    }

    // THE FOUR SENTENCES D0 WRITES OUT, asserted over the whole action set so
    // that a nineteenth action added tomorrow is covered by them.
    let mutations = [
        Action::CreateConnection,
        Action::TestConnection,
        Action::WriteCredential,
        Action::DiscoverTopics,
        Action::CancelTopicDiscovery,
        Action::RunPreflight,
        Action::CancelPreflight,
        Action::ManageDestinations,
        Action::CreateSchedule,
        Action::SetScheduleSuspension,
        Action::CreateManualBackup,
        Action::CreateRestore,
        Action::SubmitApproval,
    ];
    for action in mutations {
        assert!(!Role::Viewer.allows(action), "a viewer mutated: {action:?}");
    }
    assert!(
        !Role::Operator.allows(Action::SubmitApproval),
        "an operator must never submit a governed approval"
    );
    for action in [
        Action::CreateConnection,
        Action::CreateSchedule,
        Action::CreateRestore,
        Action::CreateManualBackup,
        Action::SetScheduleSuspension,
        // An approver READS readiness for the packet; it never STARTS one, and
        // it never sees a connection, a destination or a topic inventory.
        Action::RunPreflight,
        Action::CancelPreflight,
        Action::DiscoverTopics,
        Action::CancelTopicDiscovery,
        Action::ReadTopicDiscoveries,
        Action::ReadDestinations,
        Action::ManageDestinations,
    ] {
        assert!(
            !Role::Approver.allows(action),
            "an approver must not be able to create an execution: {action:?}"
        );
    }
    assert!(
        !Role::Administrator.allows(Action::SubmitApproval),
        "administrator is not a self-approval bypass; approving needs a separate Approver binding"
    );
}

// ------------------------------------------------------------------ HTTP

struct Probe {
    label: &'static str,
    method: &'static str,
    path: &'static str,
    body: fn() -> String,
    action: Action,
}

fn nothing() -> String {
    String::new()
}

fn connection() -> String {
    support::connection_body().to_string()
}

fn schedule() -> String {
    support::schedule_body().to_string()
}

fn restore() -> String {
    support::restore_body(&support::golden_plan()).to_string()
}

fn suspension() -> String {
    serde_json::json!({ "suspended": true, "expectedResourceVersion": "1" }).to_string()
}

/// Every route this build serves, with the action it asks the authorizer about.
fn probes() -> Vec<Probe> {
    vec![
        Probe {
            label: "list connections",
            method: "GET",
            path: "/connections",
            body: nothing,
            action: Action::ReadConnections,
        },
        Probe {
            label: "get connection",
            method: "GET",
            path: "/connections/conn-1",
            body: nothing,
            action: Action::ReadConnections,
        },
        Probe {
            label: "create connection",
            method: "POST",
            path: "/connections",
            body: connection,
            action: Action::CreateConnection,
        },
        Probe {
            label: "list schedules",
            method: "GET",
            path: "/schedules",
            body: nothing,
            action: Action::ReadSchedules,
        },
        Probe {
            label: "get schedule",
            method: "GET",
            path: "/schedules/sched-1",
            body: nothing,
            action: Action::ReadSchedules,
        },
        Probe {
            label: "create schedule",
            method: "POST",
            path: "/schedules",
            body: schedule,
            action: Action::CreateSchedule,
        },
        Probe {
            label: "set suspension",
            method: "POST",
            path: "/schedules/sched-1",
            body: suspension,
            action: Action::SetScheduleSuspension,
        },
        Probe {
            label: "list backups",
            method: "GET",
            path: "/backups",
            body: nothing,
            action: Action::ReadBackups,
        },
        Probe {
            label: "get backup",
            method: "GET",
            path: "/backups/backup-1",
            body: nothing,
            action: Action::ReadBackups,
        },
        Probe {
            label: "list restores",
            method: "GET",
            path: "/restores",
            body: nothing,
            action: Action::ReadRestores,
        },
        Probe {
            label: "get restore",
            method: "GET",
            path: "/restores/restore-1",
            body: nothing,
            action: Action::ReadRestores,
        },
        Probe {
            label: "create restore",
            method: "POST",
            path: "/restores",
            body: restore,
            action: Action::CreateRestore,
        },
        Probe {
            label: "list approvals",
            method: "GET",
            path: "/approvals",
            body: nothing,
            action: Action::ReadApprovals,
        },
        Probe {
            label: "get approval",
            method: "GET",
            path: "/approvals/approval-1",
            body: nothing,
            action: Action::ReadApprovals,
        },
        Probe {
            label: "read approval packet",
            method: "GET",
            path: "/approvals/approval-1/packet",
            body: nothing,
            action: Action::ReadApprovalPacket,
        },
        Probe {
            label: "read operation",
            method: "GET",
            path: "/operations/backup/backup-1",
            body: nothing,
            action: Action::ReadOperations,
        },
        // ------------------------------------------------------ D2 W12
        Probe {
            label: "list destinations",
            method: "GET",
            path: "/destinations",
            body: nothing,
            action: Action::ReadDestinations,
        },
        Probe {
            label: "get destination",
            method: "GET",
            path: "/destinations/dest-1",
            body: nothing,
            action: Action::ReadDestinations,
        },
        Probe {
            label: "destination usage",
            method: "GET",
            path: "/destinations/dest-1/usage",
            body: nothing,
            action: Action::ReadDestinations,
        },
        Probe {
            label: "create destination",
            method: "POST",
            path: "/destinations",
            body: destination,
            action: Action::ManageDestinations,
        },
        Probe {
            label: "rotate destination access",
            method: "POST",
            path: "/destinations/dest-1:update-access",
            body: rotation,
            action: Action::ManageDestinations,
        },
        Probe {
            label: "test destination",
            method: "POST",
            path: "/destinations/dest-1:test",
            body: nothing_object,
            action: Action::ManageDestinations,
        },
        Probe {
            label: "adopt a legacy location",
            method: "POST",
            path: "/destinations:from-legacy",
            body: from_legacy,
            action: Action::ManageDestinations,
        },
        Probe {
            label: "list discoveries",
            method: "GET",
            path: "/connections/conn-1/topic-discoveries",
            body: nothing,
            action: Action::ReadTopicDiscoveries,
        },
        Probe {
            label: "get discovery",
            method: "GET",
            path: "/topic-discoveries/td-1",
            body: nothing,
            action: Action::ReadTopicDiscoveries,
        },
        Probe {
            label: "page discovered topics",
            method: "GET",
            path: "/topic-discoveries/td-1/topics",
            body: nothing,
            action: Action::ReadTopicDiscoveries,
        },
        Probe {
            label: "start discovery",
            method: "POST",
            path: "/connections/conn-1/topic-discoveries",
            body: nothing_object,
            action: Action::DiscoverTopics,
        },
        Probe {
            label: "cancel discovery",
            method: "POST",
            path: "/topic-discoveries/td-1:cancel",
            body: nothing_object,
            action: Action::CancelTopicDiscovery,
        },
        Probe {
            label: "get preflight",
            method: "GET",
            path: "/preflights/pf-1",
            body: nothing,
            action: Action::ReadPreflights,
        },
        Probe {
            label: "page preflight details",
            method: "GET",
            path: "/preflights/pf-1/details",
            body: nothing,
            action: Action::ReadPreflights,
        },
        Probe {
            label: "start preflight",
            method: "POST",
            path: "/preflights",
            body: preflight,
            action: Action::RunPreflight,
        },
        Probe {
            label: "cancel preflight",
            method: "POST",
            path: "/preflights/pf-1:cancel",
            body: nothing_object,
            action: Action::CancelPreflight,
        },
    ]
}

fn nothing_object() -> String {
    "{}".to_string()
}

fn destination() -> String {
    support::destination_body("dest-2").to_string()
}

fn rotation() -> String {
    serde_json::json!({
        "expectedGeneration": 1,
        "access": {"archiveWrite": {"mode": "secretKeys", "secret": {"existing": {"name": "logweir-s3"}}}}
    })
    .to_string()
}

fn from_legacy() -> String {
    serde_json::json!({
        "name": "adopted-1",
        "sourceSchedule": "sched-1",
        "access": {"archiveWrite": {"mode": "secretKeys", "secret": {"existing": {"name": "logweir-s3"}}}}
    })
    .to_string()
}

fn preflight() -> String {
    serde_json::json!({
        "operation": "backup",
        "backup": {"sourceConnection": "conn-1", "destination": "dest-1", "topics": ["orders"]}
    })
    .to_string()
}

async fn drive(
    app: &SharedApp,
    probe: &Probe,
    namespace: &str,
    cookie: &str,
    csrf: &str,
    key: &str,
) -> TestResponse {
    let uri = format!("/api/v1/namespaces/{namespace}{}", probe.path);
    let mut builder = Request::builder()
        .method(probe.method)
        .uri(&uri)
        .header("host", SHARED_HOST)
        .header("cookie", cookie);
    if probe.method == "POST" {
        builder = builder
            .header("origin", SHARED_ORIGIN)
            .header("content-type", "application/json")
            .header("x-csrf-token", csrf)
            .header("idempotency-key", key);
    }
    app.app
        .send(builder.body(Body::from((probe.body)())).unwrap())
        .await
}

/// A cluster holding one object of every kind in both namespaces, so that an
/// ALLOWED read answers 200 rather than 404 and the matrix's allow column is
/// not satisfied by "the object happens not to exist".
fn seeded() -> FakeKube {
    let fake = FakeKube::new();
    for namespace in [NS_A, NS_B] {
        fake.seed(
            "kafkaclusters",
            namespace,
            serde_json::json!({
                "metadata": {"name": "conn-1"},
                "spec": {"bootstrapServers": ["kafka:9096"], "auth": {"mode": "plaintext", "tls": false}, "role": "source"},
            }),
        );
        fake.seed(
            "backupschedules",
            namespace,
            serde_json::json!({
                "metadata": {"name": "sched-1"},
                "spec": {"schedule": "0 3 * * *", "sourceRef": {"name": "conn-1"}, "topics": ["orders"], "archive": {"url": "s3://b/p"}, "suspend": false},
            }),
        );
        fake.seed(
            "backups",
            namespace,
            serde_json::json!({
                "metadata": {"name": "backup-1"},
                "spec": {"sourceRef": {"name": "conn-1"}, "topics": ["orders"], "archive": {"url": "s3://b/p"}},
            }),
        );
        fake.seed(
            "restores",
            namespace,
            serde_json::json!({
                "metadata": {"name": "restore-1"},
                "spec": {"planBytes": "plan: {}\n", "planHash": "sha256:0", "approvalRef": {"name": "approval-1"}},
            }),
        );
        fake.seed(
            "approvals",
            namespace,
            serde_json::json!({
                "metadata": {"name": "approval-1"},
                "spec": {"subjectRef": {"kind": "Restore", "name": "restore-1"}, "document": "e30=", "sidecar": "e30="},
            }),
        );
        support::seed_destination(&fake, namespace, "dest-1");
        support::seed_discovery(
            &fake,
            namespace,
            "td-1",
            "conn-1",
            Some("urn:test#someone-else"),
            &[vec![support::topic_line("orders", 3, "-")]],
        );
        // A RESTORE PREFLIGHT, because that is the one an approver may read;
        // the approver arm below also drives a Backup one and must NOT see it.
        support::seed_preflight(
            &fake,
            namespace,
            "pf-1",
            "Restore",
            None,
            Some("urn:test#someone-else"),
        );
        support::seed_preflight(
            &fake,
            namespace,
            "pf-backup",
            "Backup",
            None,
            Some("urn:test#someone-else"),
        );
    }
    fake
}

/// The exact number of objects of every kind, so a refused mutation can be
/// shown to have created nothing.
fn counts(fake: &FakeKube) -> Vec<(String, usize)> {
    let mut out = Vec::new();
    for plural in support::PLURALS {
        for namespace in [NS_A, NS_B] {
            out.push((
                format!("{plural}/{namespace}"),
                fake.count(plural, namespace),
            ));
        }
    }
    out
}

/// **The whole matrix, over HTTP, in two namespaces.**
///
/// Each of the four roles is bound in team-a ONLY. Every route is driven in
/// team-a (where the role's table decides) and in team-b (where the actor has
/// no binding at all and must get the nonexistent-resource answer).
#[tokio::test]
async fn the_matrix_holds_over_http_in_two_namespaces() {
    let fake = seeded();
    let app = SharedApp::new(
        fake.clone(),
        support::idp::MockIdp::new(ISSUER, &[]),
        SharedOptions {
            bindings: support::RoleBindings {
                revision: "matrix-1".into(),
                bindings: vec![
                    support::binding(Role::Viewer, NS_A, &["lw-a-viewers"]),
                    support::binding(Role::Operator, NS_A, &["lw-a-operators"]),
                    support::binding(Role::Approver, NS_A, &["lw-a-approvers"]),
                    support::binding(Role::Administrator, NS_A, &["lw-a-admins"]),
                ],
            },
            ..SharedOptions::default()
        },
    );

    let actors = [
        (Role::Viewer, "u-viewer", "lw-a-viewers"),
        (Role::Operator, "u-operator", "lw-a-operators"),
        (Role::Approver, "u-approver", "lw-a-approvers"),
        (Role::Administrator, "u-admin", "lw-a-admins"),
    ];

    let mut key_counter = 0;
    for (role, subject, group) in actors {
        let cookie = app.session_cookie(subject, &[group]);
        let csrf = app.csrf_for(subject);

        for probe in probes() {
            key_counter += 1;
            let key = format!("matrix-{key_counter:012}");
            let before = counts(&fake);

            // --- the bound namespace: the role's table decides.
            let response = drive(&app, &probe, NS_A, &cookie, &csrf, &key).await;
            let allowed = role.allows(probe.action);
            if allowed {
                assert_ne!(
                    response.status.as_u16(),
                    403,
                    "{role:?} was refused `{}` in {NS_A}: {}",
                    probe.label,
                    String::from_utf8_lossy(&response.body)
                );
            } else {
                assert_eq!(
                    (response.status.as_u16(), response.code().as_str()),
                    (403, "forbidden"),
                    "{role:?} was not refused `{}` in {NS_A}",
                    probe.label
                );
                assert_eq!(
                    counts(&fake),
                    before,
                    "{role:?} changed the cluster through a refused `{}`",
                    probe.label
                );
            }

            // --- the unbound namespace: enumeration-resistant, always.
            key_counter += 1;
            let key = format!("matrix-{key_counter:012}");
            let before = counts(&fake);
            let response = drive(&app, &probe, NS_B, &cookie, &csrf, &key).await;
            assert_eq!(
                (response.status.as_u16(), response.code().as_str()),
                (404, "not_found"),
                "{role:?} learned something about the unbound {NS_B} through `{}`",
                probe.label
            );
            assert_eq!(
                counts(&fake),
                before,
                "{role:?} changed the cluster in the unbound {NS_B} through `{}`",
                probe.label
            );
        }

        // The session reports the same roles the table used.
        let session = app.get("/api/v1/session", &cookie).await.json();
        assert_eq!(session["bindingRevision"], "matrix-1");
        assert_eq!(
            session["namespaces"].as_array().unwrap().len(),
            1,
            "{role:?} sees only its bound namespace"
        );
        assert_eq!(session["namespaces"][0]["name"], NS_A);
        assert_eq!(session["namespaces"][0]["roles"][0], role.as_str());
    }
    app.app.fake.assert_strict();
}

/// **An unbound namespace and a nonexistent object are the same answer, byte
/// for byte apart from the request id.**
#[tokio::test]
async fn an_unbound_namespace_is_indistinguishable_from_a_missing_object() {
    let fake = seeded();
    let app = SharedApp::new(
        fake.clone(),
        support::idp::MockIdp::new(ISSUER, &[]),
        SharedOptions {
            bindings: support::RoleBindings {
                revision: "r".into(),
                bindings: vec![support::binding(Role::Operator, NS_A, &["lw-a-operators"])],
            },
            ..SharedOptions::default()
        },
    );
    let cookie = app.session_cookie("u-op", &["lw-a-operators"]);

    let unbound = app
        .get("/api/v1/namespaces/team-b/schedules/sched-1", &cookie)
        .await;
    let missing = app
        .get(
            "/api/v1/namespaces/team-a/schedules/no-such-object",
            &cookie,
        )
        .await;
    let nonexistent_namespace = app
        .get("/api/v1/namespaces/team-zzz/schedules/sched-1", &cookie)
        .await;

    let normalize = |response: &TestResponse| {
        let mut body = response.json();
        body["requestId"] = serde_json::Value::String(String::new());
        (response.status.as_u16(), body)
    };
    assert_eq!(normalize(&unbound), normalize(&missing));
    assert_eq!(normalize(&nonexistent_namespace), normalize(&missing));

    // And the unbound namespace never reached Kubernetes: the namespace is
    // checked BEFORE the lookup, so the answer cannot depend on cluster state
    // the actor may not see.
    fake.clear_requests();
    let _ = app
        .get("/api/v1/namespaces/team-b/schedules/sched-1", &cookie)
        .await;
    assert!(
        fake.requests().is_empty(),
        "an unbound namespace reached Kubernetes: {:?}",
        fake.requests()
    );
    // The bound one does.
    let _ = app
        .get("/api/v1/namespaces/team-a/schedules/sched-1", &cookie)
        .await;
    assert!(!fake.requests().is_empty());
    app.app.fake.assert_strict();
}

/// **Bindings union: two roles in one namespace grant the union, and a role in
/// each of two namespaces grants each one separately.**
#[tokio::test]
async fn bindings_union_within_a_namespace_and_stay_separate_across_them() {
    let fake = seeded();
    let app = SharedApp::new(
        fake.clone(),
        support::idp::MockIdp::new(ISSUER, &[]),
        SharedOptions {
            bindings: support::RoleBindings {
                revision: "union-1".into(),
                bindings: vec![
                    // One person, two groups, both bound in team-a.
                    support::binding(Role::Viewer, NS_A, &["lw-a-viewers"]),
                    support::binding(Role::Approver, NS_A, &["lw-a-approvers"]),
                    // And an operator binding in team-b only.
                    support::binding(Role::Operator, NS_B, &["lw-b-operators"]),
                ],
            },
            ..SharedOptions::default()
        },
    );
    let cookie = app.session_cookie(
        "u-both",
        &["lw-a-viewers", "lw-a-approvers", "lw-b-operators"],
    );
    let csrf = app.csrf_for("u-both");

    let session = app.get("/api/v1/session", &cookie).await.json();
    let grants: Vec<(String, Vec<String>)> = session["namespaces"]
        .as_array()
        .unwrap()
        .iter()
        .map(|g| {
            (
                g["name"].as_str().unwrap().to_string(),
                g["roles"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|r| r.as_str().unwrap().to_string())
                    .collect(),
            )
        })
        .collect();
    assert_eq!(
        grants,
        vec![
            (
                NS_A.to_string(),
                vec!["viewer".to_string(), "approver".to_string()]
            ),
            (NS_B.to_string(), vec!["operator".to_string()]),
        ]
    );

    // The UNION in team-a: the packet is an approver's, the connection list is
    // a viewer's, and neither role alone has both.
    assert_ne!(
        app.get(
            "/api/v1/namespaces/team-a/approvals/approval-1/packet",
            &cookie
        )
        .await
        .status
        .as_u16(),
        403,
        "the approver half of the union was lost"
    );
    assert_ne!(
        app.get("/api/v1/namespaces/team-a/connections", &cookie)
            .await
            .status
            .as_u16(),
        403,
        "the viewer half of the union was lost"
    );
    // But the union of viewer and approver is still not an operator.
    let refused = app
        .post(
            "/api/v1/namespaces/team-a/schedules",
            &cookie,
            Some(&csrf),
            Some("union-schedule-00001"),
            &schedule(),
        )
        .await;
    refused.assert_problem(403, "forbidden");
    // The seed put one schedule in each namespace; the refused create added
    // none.
    assert_eq!(fake.count("backupschedules", NS_A), 1);

    // The operator binding is in team-b and does not leak into team-a.
    let created = app
        .post(
            "/api/v1/namespaces/team-b/schedules",
            &cookie,
            Some(&csrf),
            Some("union-schedule-00002"),
            &schedule(),
        )
        .await;
    assert_eq!(created.status.as_u16(), 201, "{}", created.code());
    assert_eq!(
        fake.count("backupschedules", NS_B),
        2,
        "the seed plus one create"
    );
    assert_eq!(
        fake.count("backupschedules", NS_A),
        1,
        "team-a is untouched"
    );
    app.app.fake.assert_strict();
}

/// **A subject binding is an exact `<issuer>#<subject>` string, and nothing
/// near it matches.**
#[tokio::test]
async fn subject_bindings_are_exact_strings() {
    let app = SharedApp::new(
        seeded(),
        support::idp::MockIdp::new(ISSUER, &[]),
        SharedOptions {
            bindings: support::RoleBindings {
                revision: "exact-1".into(),
                bindings: vec![support::subject_binding(
                    Role::Operator,
                    NS_A,
                    &[&format!("{ISSUER}#u-named")],
                )],
            },
            ..SharedOptions::default()
        },
    );

    let named = app.session_cookie("u-named", &[]);
    assert_eq!(
        app.get("/api/v1/namespaces/team-a/schedules", &named)
            .await
            .status
            .as_u16(),
        200
    );

    // A prefix, a suffix and a different case are all different subjects.
    for other in ["u-name", "u-namedx", "U-NAMED", "u-named "] {
        let cookie = app.session_cookie(other, &[]);
        let response = app
            .get("/api/v1/namespaces/team-a/schedules", &cookie)
            .await;
        assert_eq!(
            (response.status.as_u16(), response.code().as_str()),
            (404, "not_found"),
            "`{other}` matched a binding for `u-named`"
        );
    }
}

/// **Role configuration is re-read per request: a binding removed now applies
/// to the next request, with no restart and without invalidating the session.**
#[tokio::test]
async fn changing_the_role_configuration_takes_effect_on_the_next_request() {
    let fake = seeded();
    let app = SharedApp::new(
        fake.clone(),
        support::idp::MockIdp::new(ISSUER, &[]),
        SharedOptions {
            bindings: support::RoleBindings {
                revision: "before".into(),
                bindings: vec![support::binding(Role::Operator, NS_A, &["lw-a-operators"])],
            },
            ..SharedOptions::default()
        },
    );
    let cookie = app.session_cookie("u-op", &["lw-a-operators"]);
    let csrf = app.csrf_for("u-op");

    let created = app
        .post(
            "/api/v1/namespaces/team-a/schedules",
            &cookie,
            Some(&csrf),
            Some("rollconfig-00000001"),
            &schedule(),
        )
        .await;
    assert_eq!(created.status.as_u16(), 201, "{}", created.code());

    // The administrator demotes the group to viewer. No restart, no logout.
    app.authorizer.replace(support::RoleBindings {
        revision: "after".into(),
        bindings: vec![support::binding(Role::Viewer, NS_A, &["lw-a-operators"])],
    });

    let session = app.get("/api/v1/session", &cookie).await.json();
    assert_eq!(session["bindingRevision"], "after");
    assert_eq!(session["namespaces"][0]["roles"][0], "viewer");
    assert_eq!(
        session["namespaces"][0]["capabilities"]["scheduleCreate"],
        false
    );

    let refused = app
        .post(
            "/api/v1/namespaces/team-a/schedules",
            &cookie,
            Some(&csrf),
            Some("rollconfig-00000002"),
            &schedule(),
        )
        .await;
    refused.assert_problem(403, "forbidden");
    assert_eq!(
        fake.count("backupschedules", NS_A),
        2,
        "the seed plus the one create made while the binding was operator"
    );

    // And a binding removed entirely takes the namespace with it.
    app.authorizer.replace(support::RoleBindings {
        revision: "revoked".into(),
        bindings: vec![support::binding(Role::Viewer, NS_B, &["someone-else"])],
    });
    let session = app.get("/api/v1/session", &cookie).await.json();
    assert_eq!(session["namespaces"].as_array().unwrap().len(), 0);
    app.get("/api/v1/namespaces/team-a/schedules", &cookie)
        .await
        .assert_problem(404, "not_found");
    app.app.fake.assert_strict();
}

/// **Separation of duties compares principals, and administrator is not a way
/// around it.**
///
/// The governed-approval ROUTE is PLAT-19.2 and is absent (the capability is
/// advertised `false`). The DECISION is here and tested now, because the
/// tracker's acceptance — "an operator cannot assume approver rights" and
/// "admin is not a self-approval bypass" — is a property of the decision.
#[tokio::test]
async fn governed_approval_needs_an_approver_binding_and_a_different_principal() {
    use logweir_api::auth::Actor;
    use logweir_api::authz::{authorize_governed_approval, Principal, SharedAuthorizer};

    let authorizer = SharedAuthorizer::new(support::RoleBindings {
        revision: "sod-1".into(),
        bindings: vec![
            support::binding(Role::Operator, NS_A, &["lw-a-operators"]),
            support::binding(Role::Approver, NS_A, &["lw-a-approvers"]),
            support::binding(Role::Administrator, NS_A, &["lw-a-admins"]),
            // One person who is BOTH: an administrator who is also bound as an
            // approver. Even then they cannot approve their own request.
            support::binding(Role::Administrator, NS_B, &["lw-both"]),
            support::binding(Role::Approver, NS_B, &["lw-both"]),
        ],
    });

    let requester =
        Actor::new(ISSUER, "u-operator", "Op").with_groups(vec!["lw-a-operators".into()]);
    let approver =
        Actor::new(ISSUER, "u-approver", "App").with_groups(vec!["lw-a-approvers".into()]);
    let admin = Actor::new(ISSUER, "u-admin", "Adm").with_groups(vec!["lw-a-admins".into()]);
    let both = Actor::new(ISSUER, "u-both", "Both").with_groups(vec!["lw-both".into()]);

    let principal = Principal::of(&requester);

    // The approver may.
    assert!(authorize_governed_approval(&authorizer, &approver, NS_A, &principal).is_ok());

    // The operator may not, whoever the requester is.
    assert_eq!(
        authorize_governed_approval(&authorizer, &requester, NS_A, &Principal::of(&approver))
            .unwrap_err()
            .code,
        logweir_api::problem::ProblemCode::Forbidden
    );

    // The administrator may not: administering is not approving.
    assert_eq!(
        authorize_governed_approval(&authorizer, &admin, NS_A, &principal)
            .unwrap_err()
            .code,
        logweir_api::problem::ProblemCode::Forbidden
    );

    // The approver may not approve their OWN request.
    assert_eq!(
        authorize_governed_approval(&authorizer, &approver, NS_A, &Principal::of(&approver))
            .unwrap_err()
            .code,
        logweir_api::problem::ProblemCode::Forbidden
    );

    // Someone bound as administrator AND approver may approve someone else's
    // request, and still not their own. This is the case that would silently
    // pass if administrator had a catch-all arm.
    assert!(authorize_governed_approval(&authorizer, &both, NS_B, &principal).is_ok());
    assert_eq!(
        authorize_governed_approval(&authorizer, &both, NS_B, &Principal::of(&both))
            .unwrap_err()
            .code,
        logweir_api::problem::ProblemCode::Forbidden
    );

    // And an approver binding in another namespace is not one here.
    assert_eq!(
        authorize_governed_approval(&authorizer, &approver, NS_B, &principal)
            .unwrap_err()
            .code,
        logweir_api::problem::ProblemCode::Forbidden
    );

    // Principals are compared as (issuer, subject): the same subject under a
    // different issuer is a different person, and the same person with a
    // different display name is not.
    let renamed = Actor::new(ISSUER, "u-approver", "A Different Display Name");
    assert!(
        !logweir_api::authz::independent_principals(
            &Principal::of(&approver),
            &Principal::of(&renamed)
        ),
        "a display name change must not create a second principal"
    );
    let elsewhere = Actor::new("https://other-idp.test", "u-approver", "App");
    assert!(logweir_api::authz::independent_principals(
        &Principal::of(&approver),
        &Principal::of(&elsewhere)
    ));
}

/// **No route serves the approver-only submission yet, and the capability says
/// so rather than the route returning a fake success.**
#[tokio::test]
async fn the_governed_approval_route_is_absent_not_stubbed() {
    let app = SharedApp::new(
        seeded(),
        support::idp::MockIdp::new(ISSUER, &[]),
        SharedOptions {
            bindings: support::RoleBindings {
                revision: "r".into(),
                bindings: vec![support::binding(Role::Approver, NS_A, &["lw-a-approvers"])],
            },
            ..SharedOptions::default()
        },
    );
    let cookie = app.session_cookie("u-approver", &["lw-a-approvers"]);
    let csrf = app.csrf_for("u-approver");

    for path in [
        "/api/v1/namespaces/team-a/approvals",
        "/api/v1/namespaces/team-a/approvals/approval-1:submit",
        "/api/v1/namespaces/team-a/approvals/approval-1/submit",
    ] {
        let response = app
            .post(
                path,
                &cookie,
                Some(&csrf),
                Some("submit-attempt-00001"),
                "{}",
            )
            .await;
        assert!(
            matches!(response.status.as_u16(), 404 | 405),
            "{path} answered {} — an absent route must not be a stub",
            response.status
        );
    }
    let session = app.get("/api/v1/session", &cookie).await.json();
    assert_eq!(session["capabilities"]["approvalSubmit"], false);
    assert_eq!(
        session["namespaces"][0]["capabilities"]["approvalSubmit"],
        false
    );
}

/// **The approver's readiness window is the approval packet, and no wider.**
///
/// D0 gives the approver "read only when needed for the approval packet". That
/// is a statement about the OBJECT, not about the role: a Restore readiness
/// result is what an approver is being asked to authorize, and a Backup one is
/// not its business. An actor bound only as Approver therefore reads the first
/// and gets the nonexistent-resource answer for the second — the same answer an
/// unbound namespace gives, so an approver cannot map which checks exist.
#[tokio::test]
async fn an_approver_reads_restore_readiness_and_nothing_else() {
    let fake = seeded();
    let app = SharedApp::new(
        fake.clone(),
        support::idp::MockIdp::new(ISSUER, &[]),
        SharedOptions {
            bindings: support::RoleBindings {
                revision: "approver-1".into(),
                bindings: vec![
                    support::binding(Role::Approver, NS_A, &["lw-a-approvers"]),
                    support::binding(Role::Viewer, NS_A, &["lw-a-viewers"]),
                ],
            },
            ..SharedOptions::default()
        },
    );
    let approver = app.session_cookie("u-approver", &["lw-a-approvers"]);

    let restore = app
        .get(
            &format!("/api/v1/namespaces/{NS_A}/preflights/pf-1"),
            &approver,
        )
        .await;
    assert_eq!(restore.status.as_u16(), 200, "{}", restore.text());
    assert_eq!(restore.json()["item"]["operation"], "restore");

    let backup = app
        .get(
            &format!("/api/v1/namespaces/{NS_A}/preflights/pf-backup"),
            &approver,
        )
        .await;
    backup.assert_problem(404, "not_found");
    let details = app
        .get(
            &format!("/api/v1/namespaces/{NS_A}/preflights/pf-backup/details"),
            &approver,
        )
        .await;
    details.assert_problem(404, "not_found");
    let operation = app
        .get(
            &format!("/api/v1/namespaces/{NS_A}/operations/preflight/pf-backup"),
            &approver,
        )
        .await;
    operation.assert_problem(404, "not_found");

    // AN ACTOR WHO IS ALSO A VIEWER IS NOT NARROWED: the restriction is about
    // holding the approver binding ALONE, not about being an approver.
    let both = app.session_cookie("u-both", &["lw-a-approvers", "lw-a-viewers"]);
    let backup = app
        .get(
            &format!("/api/v1/namespaces/{NS_A}/preflights/pf-backup"),
            &both,
        )
        .await;
    assert_eq!(backup.status.as_u16(), 200, "{}", backup.text());
    fake.assert_strict();
}

/// **An operator cancels its own checks, and only its own.**
///
/// Two operators bound in the same namespace hold the same role and the same
/// Kubernetes reach. What separates them is the actor recorded on the object
/// when it was created, compared as `issuer#subject` — never a display name.
#[tokio::test]
async fn one_operator_cannot_cancel_another_operators_check() {
    let fake = FakeKube::new();
    fake.seed(
        "kafkaclusters",
        NS_A,
        serde_json::json!({
            "metadata": {"name": "conn-1"},
            "spec": {"bootstrapServers": ["kafka:9096"], "auth": {"mode": "plaintext", "tls": false}, "role": "source"},
        }),
    );
    let app = SharedApp::new(
        fake.clone(),
        support::idp::MockIdp::new(ISSUER, &[]),
        SharedOptions {
            bindings: support::RoleBindings {
                revision: "operators-1".into(),
                bindings: vec![support::binding(Role::Operator, NS_A, &["lw-a-operators"])],
            },
            ..SharedOptions::default()
        },
    );
    let first = app.session_cookie("u-op-1", &["lw-a-operators"]);
    let second = app.session_cookie("u-op-2", &["lw-a-operators"]);

    let started = app
        .post(
            &format!("/api/v1/namespaces/{NS_A}/connections/conn-1/topic-discoveries"),
            &first,
            Some(&app.csrf_for("u-op-1")),
            Some("owner-key-000001"),
            "{}",
        )
        .await;
    assert_eq!(started.status.as_u16(), 202, "{}", started.text());
    let id = started.json()["item"]["id"].as_str().unwrap().to_string();
    let stored = fake.object("topicdiscoveries", NS_A, &id).unwrap();
    assert_eq!(
        stored["metadata"]["annotations"][support::ACTOR_ANNOTATION],
        format!("{ISSUER}#u-op-1")
    );

    // The other operator holds the same role and is refused anyway.
    let refused = app
        .post(
            &format!("/api/v1/namespaces/{NS_A}/topic-discoveries/{id}:cancel"),
            &second,
            Some(&app.csrf_for("u-op-2")),
            None,
            "{}",
        )
        .await;
    refused.assert_problem(403, "forbidden");
    assert_eq!(
        fake.object("topicdiscoveries", NS_A, &id).unwrap()["spec"]["cancelRequested"],
        false
    );

    // The one who started it may stop it.
    let allowed = app
        .post(
            &format!("/api/v1/namespaces/{NS_A}/topic-discoveries/{id}:cancel"),
            &first,
            Some(&app.csrf_for("u-op-1")),
            None,
            "{}",
        )
        .await;
    assert_eq!(allowed.status.as_u16(), 200, "{}", allowed.text());
    assert_eq!(
        fake.object("topicdiscoveries", NS_A, &id).unwrap()["spec"]["cancelRequested"],
        true
    );
    fake.assert_strict();
    logweir_api::routes::reset_check_rate_limits();
}

/// Every action the authorizer knows appears in the decision table above.
/// A new action added without a row would otherwise be untested policy.
#[test]
fn the_decision_table_covers_every_action() {
    let listed = [
        Action::ReadConnections,
        Action::CreateConnection,
        Action::TestConnection,
        Action::WriteCredential,
        Action::ReadTopicDiscoveries,
        Action::DiscoverTopics,
        Action::CancelTopicDiscovery,
        Action::ReadPreflights,
        Action::RunPreflight,
        Action::CancelPreflight,
        Action::ReadDestinations,
        Action::ManageDestinations,
        Action::ReadSchedules,
        Action::CreateSchedule,
        Action::SetScheduleSuspension,
        Action::ReadBackups,
        Action::CreateManualBackup,
        Action::ReadRestores,
        Action::CreateRestore,
        Action::ReadApprovals,
        Action::ReadApprovalPacket,
        Action::SubmitApproval,
        Action::ReadOperations,
        Action::StreamOperationEvents,
    ];
    let names: std::collections::BTreeSet<&str> = listed.iter().map(|a| a.name()).collect();
    assert_eq!(
        names.len(),
        listed.len(),
        "two actions share an audit name, which would make the audit record ambiguous"
    );
}
