//! Preflights: the create contract, the binding recomputed on every read, the
//! aggregate that never rounds up, the detail page and the approver narrowing.

mod support;

use logweir_api::contract::StaleReasonKind;
use serde_json::{json, Value};
use support::{
    seed_preflight, seed_running_preflight, TestApp, ACTOR_ANNOTATION, LOCAL_ADMIN_ACTOR, NS_A,
};

fn plan() -> String {
    support::golden_plan()
}

fn plan_hash(plan: &str) -> String {
    logweir_core::ids::sha256_prefixed(plan.as_bytes())
}

fn restore_request(plan: &str) -> Value {
    json!({
        "operation": "restore",
        "restore": {
            "planBytes": plan,
            "planHash": plan_hash(plan),
            "target": "target",
            "sourceDestination": "primary",
            "evidenceDestination": "evidence",
            "recoveryPoint": {"backupName": "logweir-backup-nightly-20260915-030000", "backupUid": "uid-b"}
        }
    })
}

fn legacy_restore_request(plan: &str, point: &str) -> Value {
    json!({
        "operation": "restore",
        "restore": {
            "planBytes": plan,
            "planHash": plan_hash(plan),
            "target": "target",
            "legacySourceArchive": {
                "url": "s3://archive/team-a",
                "credentialRef": {"name": "legacy-reader"}
            },
            "recoveryPoint": {"backupName": point, "backupUid": format!("uid-{point}")}
        }
    })
}

fn seed_recovery_point(app: &TestApp, name: &str, destination: Option<&str>) {
    let (archive, destination_ref) = match destination {
        Some(destination) => (
            json!({"url": format!("logweir-destination://{destination}")}),
            Some(json!({"name": destination})),
        ),
        None => (
            json!({
                "url": "s3://archive/team-a",
                "secretRef": {"name": "legacy-reader"}
            }),
            None,
        ),
    };
    let mut spec = json!({
        "sourceRef": {"name": "source"},
        "topics": ["orders"],
        "archive": archive,
        "triggeredBy": "manual",
        "deadlineSeconds": 1800
    });
    if let Some(destination_ref) = destination_ref {
        spec["destinationRef"] = destination_ref;
    }
    app.fake.seed(
        "backups",
        NS_A,
        json!({
            "metadata": {"name": name, "uid": format!("uid-{name}")},
            "spec": spec
        }),
    );
}

#[tokio::test]
async fn a_restore_preflight_forwards_its_plan_bytes_verbatim() {
    let app = TestApp::new();
    let plan = plan();
    let response = app
        .post(
            &format!("/api/v1/namespaces/{NS_A}/preflights"),
            Some("preflight-key-00001"),
            &restore_request(&plan).to_string(),
        )
        .await;
    assert_eq!(response.status.as_u16(), 202, "{}", response.text());
    let v = response.json();
    assert_eq!(v["item"]["operation"], "restore");
    assert_eq!(v["item"]["state"], "pending");
    let id = v["item"]["id"].as_str().unwrap().to_string();
    assert!(id.starts_with("pf-"), "{id}");

    let stored = app.fake.object("preflights", NS_A, &id).unwrap();
    // BYTE FOR BYTE. Nothing here parses or re-emits a plan document.
    assert_eq!(
        stored["spec"]["request"]["restore"]["planBytes"]
            .as_str()
            .unwrap(),
        plan
    );
    assert_eq!(
        stored["spec"]["request"]["restore"]["planHash"],
        plan_hash(&plan)
    );
    assert_eq!(stored["spec"]["request"]["timeoutSeconds"], 120);
    assert_eq!(
        stored["metadata"]["annotations"][ACTOR_ANNOTATION],
        LOCAL_ADMIN_ACTOR
    );
    app.fake.assert_strict();
}

#[tokio::test]
async fn a_plan_hash_that_is_not_the_hash_of_the_bytes_is_refused_without_echoing_either() {
    let app = TestApp::new();
    let plan = plan();
    let mut request = restore_request(&plan);
    request["restore"]["planHash"] = json!(format!("sha256:{}", "b".repeat(64)));
    let response = app
        .post(
            &format!("/api/v1/namespaces/{NS_A}/preflights"),
            Some("preflight-key-00002"),
            &request.to_string(),
        )
        .await;
    response.assert_problem(422, "validation_failed");
    let text = response.text();
    assert!(
        !text.contains(&"b".repeat(64)),
        "the refusal echoed the hash"
    );
    assert!(!text.contains("kafka"), "the refusal echoed the plan");
    assert_eq!(response.json()["errors"][0]["code"], "hash_mismatch");
    assert!(app.fake.requests().is_empty());
}

#[tokio::test]
async fn the_block_must_match_the_operation_and_the_exclusive_pairs_hold() {
    let app = TestApp::new();
    let path = format!("/api/v1/namespaces/{NS_A}/preflights");

    // P3: a block that is not the operation's.
    let response = app
        .post(
            &path,
            Some("preflight-bad-00001"),
            &json!({"operation": "backup", "restore": {"restoreName": "r"}}).to_string(),
        )
        .await;
    response.assert_problem(422, "validation_failed");
    let fields = field_names(&response.json());
    assert!(fields.contains(&"restore".to_string()), "{fields:?}");
    assert!(fields.contains(&"backup".to_string()), "{fields:?}");

    // P4: a backup names a destination or a legacy archive, never both.
    let response = app
        .post(
            &path,
            Some("preflight-bad-00002"),
            &json!({
                "operation": "backup",
                "backup": {"sourceConnection": "source", "destination": "primary", "legacyArchive": {"url": "s3://b/p"}, "topics": ["orders"]}
            })
            .to_string(),
        )
        .await;
    assert!(field_names(&response.json()).contains(&"backup.destination".to_string()));

    // P5: a draft or an existing restore, never both and never neither.
    let response = app
        .post(
            &path,
            Some("preflight-bad-00003"),
            &json!({"operation": "restore", "restore": {}}).to_string(),
        )
        .await;
    assert!(field_names(&response.json()).contains(&"restore.planBytes".to_string()));

    // P7: source and evidence destinations travel together.
    let plan = plan();
    let mut request = restore_request(&plan);
    request["restore"]["evidenceDestination"] = Value::Null;
    let response = app
        .post(&path, Some("preflight-bad-00004"), &request.to_string())
        .await;
    assert!(field_names(&response.json()).contains(&"restore.sourceDestination".to_string()));

    // A pattern is never a topic.
    let response = app
        .post(
            &path,
            Some("preflight-bad-00005"),
            &json!({
                "operation": "backup",
                "backup": {"sourceConnection": "source", "destination": "primary", "topics": ["orders*"]}
            })
            .to_string(),
        )
        .await;
    assert!(field_names(&response.json()).contains(&"backup.topics[0]".to_string()));

    // The budget is bounded.
    let response = app
        .post(
            &path,
            Some("preflight-bad-00006"),
            &json!({
                "operation": "backup",
                "backup": {"sourceConnection": "source", "destination": "primary", "topics": ["orders"]},
                "timeoutSeconds": 5
            })
            .to_string(),
        )
        .await;
    assert!(field_names(&response.json()).contains(&"timeoutSeconds".to_string()));

    assert_eq!(app.fake.count("preflights", NS_A), 0);
    assert!(app.fake.requests().is_empty());
}

/// BACKUP-PROJECTION-NO-DESTINATION's independent API rail.
///
/// MUTANT: remove `validate_legacy_source_for_point` from the create route (or
/// accept `Some(legacySourceArchive)` unconditionally). The first assertion
/// becomes 202 and a doomed `Preflight` is stored. The second half prevents
/// the guard from becoming a blanket ban that strands pre-destination runs.
#[tokio::test]
async fn legacy_source_archive_is_only_accepted_for_a_truly_legacy_point() {
    let path = format!("/api/v1/namespaces/{NS_A}/preflights");
    let plan = plan();

    let destination_backed = TestApp::new();
    seed_recovery_point(&destination_backed, "saved-point", Some("primary"));
    let refused = destination_backed
        .post(
            &path,
            Some("preflight-destination-point-0001"),
            &legacy_restore_request(&plan, "saved-point").to_string(),
        )
        .await;
    refused.assert_problem(422, "validation_failed");
    assert_eq!(
        refused.json()["errors"][0]["field"],
        "restore.legacySourceArchive"
    );
    assert_eq!(
        refused.json()["errors"][0]["code"],
        "destination_ref_required"
    );
    assert!(
        refused.text().contains("destinationRef `primary`"),
        "the refusal names the saved destination contract: {}",
        refused.text()
    );
    assert_eq!(destination_backed.fake.count("preflights", NS_A), 0);

    let legacy = TestApp::new();
    seed_recovery_point(&legacy, "legacy-point", None);
    let accepted = legacy
        .post(
            &path,
            Some("preflight-legacy-point-0001"),
            &legacy_restore_request(&plan, "legacy-point").to_string(),
        )
        .await;
    assert_eq!(accepted.status.as_u16(), 202, "{}", accepted.text());
    let id = accepted.json()["item"]["id"].as_str().unwrap().to_string();
    let stored = legacy.fake.object("preflights", NS_A, &id).unwrap();
    assert_eq!(
        stored["spec"]["request"]["restore"]["legacySourceArchive"],
        json!({
            "url": "s3://archive/team-a",
            "secretRef": {"name": "legacy-reader"}
        })
    );
}

/// A recovery point is a name plus its optional frozen UID, never the newest
/// object answering that name. The compatibility lookup may inspect the name,
/// but it must not recommend a replacement object's destination for the old
/// point the request actually identifies.
///
/// MUTANT: remove the UID comparison in
/// `validate_legacy_source_for_point`. This request becomes a misleading 422
/// naming `replacement-primary` instead of storing a preflight whose ordinary
/// binding check can report the recreated recovery point.
#[tokio::test]
async fn a_recreated_recovery_point_never_supplies_legacy_destination_advice() {
    let app = TestApp::new();
    let point = "saved-point-recreated";
    seed_recovery_point(&app, point, Some("original-primary"));

    // Delete/recreate under the same name in the fake apiserver: the request
    // below retains `uid-{point}`, while this replacement has another UID and
    // another destination.
    let mut replacement = app.fake.object("backups", NS_A, point).unwrap();
    replacement["metadata"]["uid"] = json!("uid-of-replacement");
    replacement["spec"]["destinationRef"]["name"] = json!("replacement-primary");
    replacement["spec"]["archive"]["url"] = json!("logweir-destination://replacement-primary");
    app.fake.seed("backups", NS_A, replacement);
    app.fake.clear_requests();

    let response = app
        .post(
            &format!("/api/v1/namespaces/{NS_A}/preflights"),
            Some("preflight-recreated-point01"),
            &legacy_restore_request(&plan(), point).to_string(),
        )
        .await;
    assert_eq!(response.status.as_u16(), 202, "{}", response.text());
    assert!(
        !response.text().contains("replacement-primary"),
        "no response may claim the replacement destination: {}",
        response.text()
    );
    let id = response.json()["item"]["id"].as_str().unwrap().to_string();
    let stored = app.fake.object("preflights", NS_A, &id).unwrap();
    assert_eq!(
        stored["spec"]["request"]["restore"]["recoveryPointRef"],
        json!({"name": point, "uid": format!("uid-{point}")}),
        "the stored binding remains the old point's identity"
    );
    assert_eq!(
        app.fake
            .requests()
            .iter()
            .filter(|request| request.path.ends_with(&format!("/backups/{point}")))
            .count(),
        1,
        "the guard performs only its one bounded Backup lookup"
    );
    assert!(
        app.fake
            .requests()
            .iter()
            .all(|request| !request.path.contains("backupdestinations")),
        "the replacement destination is never looked up"
    );
}

/// The cheap request-shape gate stays first, while the rate gate must be the
/// last synchronous gate before any recovery-point lookup. An over-limit
/// caller must not turn the legacy compatibility check into unbounded
/// Kubernetes reads.
///
/// MUTANT: move `check_create_rate` back below
/// `validate_legacy_source_for_point`. The final request records a GET for the
/// missing Backup and this row fails even though the HTTP response is still
/// 429.
#[tokio::test]
async fn an_over_limit_preflight_performs_no_kubernetes_read() {
    let app = TestApp::new();
    let namespace = support::NS_B;
    let path = format!("/api/v1/namespaces/{namespace}/preflights");
    let plan = plan();
    let request = legacy_restore_request(&plan, "missing-point");
    logweir_api::routes::reset_check_rate_limits();

    for i in 0..logweir_api::routes::PREFLIGHT_CREATES_PER_MINUTE {
        let response = app
            .post(
                &path,
                Some(&format!("preflight-rate-{i:010}")),
                &request.to_string(),
            )
            .await;
        assert_eq!(
            response.status.as_u16(),
            202,
            "create {i}: {}",
            response.text()
        );
    }

    app.fake.clear_requests();
    let malformed = app
        .post(
            &path,
            Some("preflight-rate-malformed1"),
            r#"{"operation":"restore","restore":{}}"#,
        )
        .await;
    malformed.assert_problem(422, "validation_failed");
    assert!(
        app.fake.requests().is_empty(),
        "shape validation is still first"
    );

    let limited = app
        .post(
            &path,
            Some("preflight-rate-overflow01"),
            &request.to_string(),
        )
        .await;
    limited.assert_problem(429, "rate_limited");
    assert!(limited.header("retry-after").is_some());
    assert!(
        app.fake.requests().is_empty(),
        "an over-limit request performed Kubernetes I/O: {:?}",
        app.fake.requests()
    );
    assert_eq!(
        app.fake.count("preflights", namespace),
        logweir_api::routes::PREFLIGHT_CREATES_PER_MINUTE as usize
    );
    logweir_api::routes::reset_check_rate_limits();
}

/// The guided schedule form's inline readiness payload must use the public
/// `legacyArchive` spelling, while a dynamic schedule must not send the empty
/// named-topic list that this route correctly refuses.
#[tokio::test]
async fn backup_preflight_accepts_an_inline_archive_and_refuses_empty_topics() {
    let app = TestApp::new();
    let path = format!("/api/v1/namespaces/{NS_A}/preflights");
    let inline = json!({
        "operation": "backup",
        "backup": {
            "sourceConnection": "source",
            "legacyArchive": {"url": "s3://b/p", "credentialRef": {"name": "s3-creds"}},
            "topics": ["orders"]
        }
    });
    let response = app
        .post(&path, Some("preflight-inline-0001"), &inline.to_string())
        .await;
    assert_eq!(response.status.as_u16(), 202, "{}", response.text());
    let response_body = response.json();
    let id = response_body["item"]["id"].as_str().unwrap();
    let stored = app.fake.object("preflights", NS_A, id).unwrap();
    assert_eq!(
        stored["spec"]["request"]["backup"]["legacyArchive"],
        json!({"url": "s3://b/p", "secretRef": {"name": "s3-creds"}})
    );

    let empty = json!({
        "operation": "backup",
        "backup": {"sourceConnection": "source", "legacyArchive": {"url": "s3://b/p"}, "topics": []}
    });
    let response = app
        .post(&path, Some("preflight-inline-0002"), &empty.to_string())
        .await;
    response.assert_problem(422, "validation_failed");
    assert!(field_names(&response.json()).contains(&"backup.topics".to_string()));
    app.fake.assert_strict();
}

/// D2-SOURCECHECK: the connectivity check the console's "Test connection"
/// creates, and the two ways of asking for it wrongly.
///
/// BOTH WRONG WAYS ARE A 422 NAMING A FIELD, and neither is a quietly wider
/// check. An omitted block is the route's own block/operation table; an
/// omitted `connectionRef` is `read_json`'s `Category::Data` arm, which is the
/// reason the field is REQUIRED on the DTO rather than an `Option` this
/// function would have to remember to look at.
///
/// MUTANT: give `SourceConnectionPreflightRequest.connection_ref` a
/// `#[serde(default)]` and an `Option<String>`. The second arm below stops
/// being a 422 and a `Preflight` is created whose `connectionRef.name` is
/// empty — an object the controller then reports `ConnectionNotFound` about,
/// which spends a reconcile to say what the API already knew.
#[tokio::test]
async fn a_source_connection_check_needs_a_connection_and_nothing_else() {
    let app = TestApp::new();
    let path = format!("/api/v1/namespaces/{NS_A}/preflights");

    // The block is missing entirely.
    let response = app
        .post(
            &path,
            Some("preflight-sc-00001"),
            &json!({"operation": "sourceConnection"}).to_string(),
        )
        .await;
    response.assert_problem(422, "validation_failed");
    assert!(field_names(&response.json()).contains(&"sourceConnection".to_string()));

    // The block is there and the one field it has is not.
    let response = app
        .post(
            &path,
            Some("preflight-sc-00002"),
            &json!({"operation": "sourceConnection", "sourceConnection": {}}).to_string(),
        )
        .await;
    response.assert_problem(422, "validation_failed");
    assert!(
        field_names(&response.json()).contains(&"connectionRef".to_string()),
        "{:?}",
        field_names(&response.json())
    );

    // A reference that is not a Kubernetes name.
    let response = app
        .post(
            &path,
            Some("preflight-sc-00003"),
            &json!({"operation": "sourceConnection", "sourceConnection": {"connectionRef": "Not A Name"}})
                .to_string(),
        )
        .await;
    assert!(field_names(&response.json()).contains(&"sourceConnection.connectionRef".to_string()));

    // A field this check does not ask about is a refusal, not a wider check.
    let response = app
        .post(
            &path,
            Some("preflight-sc-00004"),
            &json!({
                "operation": "sourceConnection",
                "sourceConnection": {"connectionRef": "source", "destination": "primary"}
            })
            .to_string(),
        )
        .await;
    response.assert_problem(422, "validation_failed");
    assert_eq!(response.json()["errors"][0]["code"], "unknown_field");

    // Another operation's block beside it.
    let response = app
        .post(
            &path,
            Some("preflight-sc-00005"),
            &json!({
                "operation": "sourceConnection",
                "sourceConnection": {"connectionRef": "source"},
                "destinationAccess": {"destination": "primary", "roles": ["archiveRead"]}
            })
            .to_string(),
        )
        .await;
    response.assert_problem(422, "validation_failed");
    assert!(field_names(&response.json()).contains(&"destinationAccess".to_string()));

    assert_eq!(app.fake.count("preflights", NS_A), 0, "nothing was created");

    // And the request the console actually sends.
    let response = app
        .post(
            &path,
            Some("preflight-sc-00006"),
            &json!({"operation": "sourceConnection", "sourceConnection": {"connectionRef": "source"}})
                .to_string(),
        )
        .await;
    assert_eq!(response.status.as_u16(), 202, "{}", response.text());
    let v = response.json();
    assert_eq!(v["item"]["operation"], "sourceConnection");
    assert_eq!(v["item"]["state"], "pending");
    let id = v["item"]["id"].as_str().unwrap().to_string();

    let stored = app.fake.object("preflights", NS_A, &id).unwrap();
    assert_eq!(stored["spec"]["request"]["operation"], "SourceConnection");
    assert_eq!(
        stored["spec"]["request"]["sourceConnection"]["connectionRef"]["name"],
        "source"
    );
    // THE ABSENCES REACH THE STORED OBJECT TOO: P3 refuses a second block at
    // admission, and nothing here writes one.
    assert!(stored["spec"]["request"]["backup"].is_null());
    assert!(stored["spec"]["request"]["destinationAccess"].is_null());
    assert_eq!(stored["spec"]["request"]["timeoutSeconds"], 120);
    app.fake.assert_strict();
}

#[tokio::test]
async fn a_duplicate_request_replays_and_a_changed_one_conflicts() {
    let app = TestApp::new();
    let plan = plan();
    let path = format!("/api/v1/namespaces/{NS_A}/preflights");
    let first = app
        .post(
            &path,
            Some("preflight-dup-00001"),
            &restore_request(&plan).to_string(),
        )
        .await;
    assert_eq!(first.status.as_u16(), 202);
    let replay = app
        .post(
            &path,
            Some("preflight-dup-00001"),
            &restore_request(&plan).to_string(),
        )
        .await;
    assert_eq!(replay.status.as_u16(), 200);
    assert_eq!(replay.json()["replayed"], true);
    assert_eq!(replay.json()["item"]["uid"], first.json()["item"]["uid"]);

    let mut changed = restore_request(&plan);
    changed["skipChecks"] = json!(["archive.segments"]);
    app.post(&path, Some("preflight-dup-00001"), &changed.to_string())
        .await
        .assert_problem(409, "idempotency_conflict");
    assert_eq!(app.fake.count("preflights", NS_A), 1);
    app.fake.assert_strict();
}

// ======================================================================
// The result, and what it is a result about
// ======================================================================

#[tokio::test]
async fn a_result_names_its_gating_and_never_rounds_the_aggregate_up() {
    let app = TestApp::new();
    seed_preflight(
        &app.fake,
        NS_A,
        "pf-0001",
        "Backup",
        None,
        Some(LOCAL_ADMIN_ACTOR),
    );
    let response = app
        .get(&format!("/api/v1/namespaces/{NS_A}/preflights/pf-0001"))
        .await;
    assert_eq!(response.status.as_u16(), 200, "{}", response.text());
    let v = response.json();
    let item = &v["item"];
    assert_eq!(item["state"], "notReady");
    assert_eq!(item["terminal"], true);

    // BLOCKING CHECKS, ADVISORY WARNINGS AND EXECUTION-ONLY NOTES ARE THREE
    // LISTS, because folding them into one is how "ready" comes to mean "every
    // permission is verified".
    let checks = item["checks"].as_array().unwrap();
    assert!(checks.iter().any(|c| c["id"] == "target.mappedTopics"));
    assert_eq!(item["warnings"][0]["id"], "archive.retention");
    assert_eq!(item["executionOnly"][0]["id"], "target.logAppendTime");
    assert!(item["executionOnly"][0]["note"]
        .as_str()
        .unwrap()
        .contains("run executes"));
    assert_eq!(item["detailsAvailable"], true);
    assert_eq!(item["checks"][0]["gating"], "blocking");
    assert_eq!(item["checks"][0]["remedy"], "Choose another prefix.");
    app.fake.assert_strict();
}

#[tokio::test]
async fn applicability_is_recomputed_against_the_callers_current_plan() {
    let app = TestApp::new();
    let bound = format!("sha256:{}", "a".repeat(64));
    seed_preflight(
        &app.fake,
        NS_A,
        "pf-0002",
        "Restore",
        Some(&bound),
        Some(LOCAL_ADMIN_ACTOR),
    );
    // The binding names `KafkaCluster/target` at uid-target/generation 1, and
    // the API now READS it back — so the fixture has to be internally
    // consistent to be applicable at all. That is the point of the
    // recomputation: a verdict about objects that are not there is not
    // applicable.
    seed_bound_referent(&app, "uid-target", 1);

    // Bound to the plan the caller is looking at: applicable.
    let same = app
        .get(&format!(
            "/api/v1/namespaces/{NS_A}/preflights/pf-0002?planHash={bound}"
        ))
        .await;
    assert_eq!(same.json()["item"]["applicable"], true);
    assert_eq!(same.json()["item"]["stale"], false);
    assert_eq!(same.json()["item"]["binding"]["planHash"], bound);

    // ONE EDIT TO THE PLAN AND THE RESULT IS ABOUT SOMETHING ELSE.
    let edited = format!("sha256:{}", "c".repeat(64));
    let changed = app
        .get(&format!(
            "/api/v1/namespaces/{NS_A}/preflights/pf-0002?planHash={edited}"
        ))
        .await;
    assert_eq!(changed.json()["item"]["applicable"], false);
    assert_eq!(changed.json()["item"]["stale"], true);
    assert_eq!(
        changed.json()["item"]["staleReasons"][0]["reason"],
        "planHashChanged"
    );

    // And an expiry is the other way a verdict stops describing now.
    app.clock.advance(3600);
    let expired = app
        .get(&format!(
            "/api/v1/namespaces/{NS_A}/preflights/pf-0002?planHash={bound}"
        ))
        .await;
    assert_eq!(expired.json()["item"]["applicable"], false);
    assert_eq!(
        expired.json()["item"]["staleReasons"][0]["reason"],
        "expired"
    );

    // A malformed hash is refused rather than silently treated as absent.
    app.get(&format!(
        "/api/v1/namespaces/{NS_A}/preflights/pf-0002?planHash=deadbeef"
    ))
    .await
    .assert_problem(422, "validation_failed");
    app.fake.assert_strict();
}

#[tokio::test]
async fn the_detail_page_is_verified_filtered_and_paged() {
    let app = TestApp::new();
    seed_preflight(
        &app.fake,
        NS_A,
        "pf-0003",
        "Restore",
        None,
        Some(LOCAL_ADMIN_ACTOR),
    );
    let all = app
        .get(&format!(
            "/api/v1/namespaces/{NS_A}/preflights/pf-0003/details"
        ))
        .await;
    assert_eq!(all.status.as_u16(), 200, "{}", all.text());
    assert_eq!(all.json()["items"].as_array().unwrap().len(), 2);
    assert_eq!(all.json()["items"][0]["check"], "target.mappedTopics");

    let filtered = app
        .get(&format!(
            "/api/v1/namespaces/{NS_A}/preflights/pf-0003/details?check=archive.segments"
        ))
        .await;
    assert_eq!(filtered.json()["items"].as_array().unwrap().len(), 1);
    assert_eq!(filtered.json()["items"][0]["entry"]["key"], "missing-0");

    let first = app
        .get(&format!(
            "/api/v1/namespaces/{NS_A}/preflights/pf-0003/details?limit=1"
        ))
        .await;
    assert_eq!(first.json()["items"].as_array().unwrap().len(), 1);
    let cursor = first.json()["page"]["nextCursor"]
        .as_str()
        .unwrap()
        .to_string();
    let next = app
        .get(&format!(
            "/api/v1/namespaces/{NS_A}/preflights/pf-0003/details?limit=1&cursor={cursor}"
        ))
        .await;
    assert_eq!(next.json()["items"][0]["check"], "archive.segments");

    // A tampered cursor is refused before anything is read.
    let mut bytes = cursor.into_bytes();
    bytes[0] = if bytes[0] == b'A' { b'B' } else { b'A' };
    app.get(&format!(
        "/api/v1/namespaces/{NS_A}/preflights/pf-0003/details?limit=1&cursor={}",
        String::from_utf8(bytes).unwrap()
    ))
    .await
    .assert_problem(400, "cursor_invalid");
    app.fake.assert_strict();
}

/// THE INTEGRITY MUTANT for the detail document: bytes whose digest does not
/// match the one the status indexes are refused, not rendered.
#[tokio::test]
async fn a_details_document_that_fails_its_digest_refuses_the_page() {
    let app = TestApp::new();
    seed_preflight(
        &app.fake,
        NS_A,
        "pf-0004",
        "Restore",
        None,
        Some(LOCAL_ADMIN_ACTOR),
    );
    let mut document = app
        .fake
        .object("configmaps", NS_A, "lwc-pf-0004-details")
        .unwrap();
    document["data"]["details.jsonl"] = json!("{\"check\":\"planted\"}\n");
    app.fake.seed("configmaps", NS_A, document);
    let response = app
        .get(&format!(
            "/api/v1/namespaces/{NS_A}/preflights/pf-0004/details"
        ))
        .await;
    response.assert_problem(409, "result_integrity_failed");
    assert!(!response.text().contains("planted"));
}

#[tokio::test]
async fn a_preflight_with_no_details_answers_an_empty_page_rather_than_404() {
    let app = TestApp::new();
    app.fake.seed(
        "preflights",
        NS_A,
        json!({
            "metadata": {"name": "pf-0005"},
            "spec": {"request": {"operation": "Backup", "backup": {"sourceRef": {"name": "s"}, "destinationRef": {"name": "d"}, "topics": ["o"]}, "timeoutSeconds": 120}, "cancelRequested": false},
            "status": {"phase": "Completed", "result": {"state": "ready"}}
        }),
    );
    let response = app
        .get(&format!(
            "/api/v1/namespaces/{NS_A}/preflights/pf-0005/details"
        ))
        .await;
    assert_eq!(response.status.as_u16(), 200);
    assert_eq!(response.json()["items"], json!([]));
}

#[tokio::test]
async fn cancel_is_idempotent_and_exact_owner() {
    let app = TestApp::new();
    seed_running_preflight(&app.fake, NS_A, "pf-mine", LOCAL_ADMIN_ACTOR);
    seed_running_preflight(&app.fake, NS_A, "pf-theirs", "urn:test#another-operator");
    let path = format!("/api/v1/namespaces/{NS_A}/preflights/pf-mine:cancel");

    let first = app.post(&path, None, "{}").await;
    assert_eq!(first.status.as_u16(), 200, "{}", first.text());
    assert_eq!(first.json()["alreadyTerminal"], false);
    assert_eq!(
        app.fake.object("preflights", NS_A, "pf-mine").unwrap()["spec"]["cancelRequested"],
        true
    );

    app.fake.clear_requests();
    let again = app.post(&path, None, "{}").await;
    assert_eq!(again.status.as_u16(), 200);
    assert!(!app.fake.requests().iter().any(|r| r.method == "PATCH"));

    app.post(
        &format!("/api/v1/namespaces/{NS_A}/preflights/pf-theirs:cancel"),
        None,
        "{}",
    )
    .await
    .assert_problem(403, "forbidden");
    assert_eq!(
        app.fake.object("preflights", NS_A, "pf-theirs").unwrap()["spec"]["cancelRequested"],
        false
    );
    app.fake.assert_strict();
}

#[tokio::test]
async fn the_operation_route_reports_a_preflight_without_an_evidence_verdict() {
    let app = TestApp::new();
    seed_preflight(
        &app.fake,
        NS_A,
        "pf-0006",
        "Restore",
        None,
        Some(LOCAL_ADMIN_ACTOR),
    );
    let response = app
        .get(&format!(
            "/api/v1/namespaces/{NS_A}/operations/preflight/pf-0006"
        ))
        .await;
    let v = response.json();
    assert_eq!(v["item"]["kind"], "preflight");
    // A CHECK THAT WORKED AND FOUND A PROBLEM `succeeded`. The VERDICT is the
    // preflight's own state; calling this `failed` would say the check broke.
    assert_eq!(v["item"]["state"], "succeeded");
    assert_eq!(v["item"]["terminal"], true);
    assert_eq!(v["item"]["cancellable"], false);
    for absent in ["verification", "result", "evidence", "verifiedSuccess"] {
        assert!(v["item"].get(absent).is_none(), "{absent} is published");
    }
    app.fake.assert_strict();
}

#[tokio::test]
async fn a_completed_check_with_no_recorded_aggregate_is_unknown_and_not_ready() {
    let app = TestApp::new();
    app.fake.seed(
        "preflights",
        NS_A,
        json!({
            "metadata": {"name": "pf-0007"},
            "spec": {"request": {"operation": "Backup", "backup": {"sourceRef": {"name": "s"}, "destinationRef": {"name": "d"}, "topics": ["o"]}, "timeoutSeconds": 120}, "cancelRequested": false},
            "status": {"phase": "Completed", "reason": "Completed"}
        }),
    );
    let response = app
        .get(&format!("/api/v1/namespaces/{NS_A}/preflights/pf-0007"))
        .await;
    assert_eq!(response.json()["item"]["state"], "unknown");
}

// ------------------------------------------------------------------ helpers

fn field_names(problem: &Value) -> Vec<String> {
    problem["errors"]
        .as_array()
        .map(|errors| {
            errors
                .iter()
                .map(|e| e["field"].as_str().unwrap_or_default().to_string())
                .collect()
        })
        .unwrap_or_default()
}

// ======================================================================
// The stale-reason vocabulary, and the recomputation that produces it
// ======================================================================

/// Seed the `KafkaCluster/target` that `seed_preflight`'s binding records, at
/// the revision it records.
fn seed_bound_referent(app: &TestApp, uid: &str, generation: i64) {
    app.fake.seed(
        "kafkaclusters",
        NS_A,
        json!({
            "metadata": {"name": "target", "uid": uid, "generation": generation},
            "spec": {"bootstrapServers": ["kafka:9096"], "auth": {"mode": "plaintext", "tls": false}, "role": "target"}
        }),
    );
}

/// **The DTO's reasons are core's vocabulary, plus exactly one of ours.**
///
/// Renamed from `…is_the_controllers_reason_set`, which is what it never was:
/// it pins the DTO against `logweir_core::check_contract::StaleReason`, the
/// VOCABULARY type, and the controller emits a strict subset of that. Saying
/// so in the name is the difference between a test a reader can trust and one
/// whose title promises coverage it does not have.
///
/// The `match` has NO WILDCARD, so a seventh reason in `logweir-core` stops
/// this file compiling. Both the LIVE Rust enum and the checked-in document
/// are compared: an earlier cut read only the document, and a reviewer's
/// variant-drop mutant passed this test while `just schema-check` caught it.
#[test]
fn the_dto_reason_set_is_the_core_vocabulary_plus_unverifiable() {
    use logweir_core::check_contract::StaleReason;

    let all = [
        StaleReason::Expired,
        StaleReason::PlanHashChanged,
        StaleReason::ReferentChanged("BackupDestination/primary".to_string()),
        StaleReason::CaBundleChanged,
        StaleReason::PolicyChanged,
        StaleReason::InputsDigestChanged,
    ];
    for reason in &all {
        let _: &str = match reason {
            StaleReason::Expired => "expired",
            StaleReason::PlanHashChanged => "planHashChanged",
            StaleReason::ReferentChanged(_) => "referentChanged",
            StaleReason::CaBundleChanged => "caBundleChanged",
            StaleReason::PolicyChanged => "policyChanged",
            StaleReason::InputsDigestChanged => "inputsDigestChanged",
        };
    }
    let mut expected: Vec<String> = all
        .iter()
        .map(|r| {
            let rendered = r.to_string();
            rendered
                .split_once(':')
                .map_or(rendered.clone(), |(head, _)| head.to_string())
        })
        .collect();
    // THE ONE VARIANT THAT IS NOT CORE'S, and the reason it exists: core has
    // no spelling for "I could not compare this".
    expected.push("unverifiable".to_string());
    expected.sort();
    expected.dedup();

    // The LIVE enum, through the serialization every response uses.
    let live: Vec<String> = [
        StaleReasonKind::Expired,
        StaleReasonKind::PlanHashChanged,
        StaleReasonKind::ReferentChanged,
        StaleReasonKind::CaBundleChanged,
        StaleReasonKind::PolicyChanged,
        StaleReasonKind::InputsDigestChanged,
        StaleReasonKind::Unverifiable,
    ]
    .iter()
    .map(|k| {
        serde_json::to_value(k)
            .unwrap()
            .as_str()
            .unwrap()
            .to_string()
    })
    .collect();
    let mut live_sorted = live.clone();
    live_sorted.sort();
    assert_eq!(
        live_sorted, expected,
        "the live enum is not core's set plus one"
    );

    // And the PUBLISHED enum, so the wire names are held too.
    let document: serde_json::Value =
        serde_json::from_str(include_str!("../../../schemas/logweir-api-v1.openapi.json"))
            .expect("the document is JSON");
    let mut published: Vec<String> = document["components"]["schemas"]["StaleReasonKind"]["oneOf"]
        .as_array()
        .expect("StaleReasonKind is an enumeration in the document")
        .iter()
        .flat_map(|option| option["enum"].as_array().cloned().unwrap_or_default())
        .map(|v| v.as_str().unwrap_or_default().to_string())
        .collect();
    published.sort();
    assert_eq!(
        published, expected,
        "the published enum drifted from the live one"
    );
    assert!(!published.iter().any(|r| r == "cancelRequested"));
}

/// **Two reasons the controller can emit have no producer, and are published
/// as reserved rather than as live.**
///
/// `caBundleChanged` needs the CA bundle list, and `inputsDigestChanged` needs
/// a digest taken over a wider document than `status.binding` records — so
/// neither the controller's `stale_against_status` nor this service can emit
/// either. Publishing them without saying so invited W13 to branch on a value
/// it will never receive; the doc comments and `docs/api.md` now say it, and
/// this test holds the claim by driving every reachable difference and
/// asserting neither appears.
#[tokio::test]
async fn the_two_reserved_reasons_are_never_emitted_by_any_reachable_path() {
    let app = TestApp::new();
    seed_preflight(
        &app.fake,
        NS_A,
        "pf-res",
        "Restore",
        None,
        Some(LOCAL_ADMIN_ACTOR),
    );
    // Every difference this build can produce at once: a replaced referent, a
    // passed expiry and a changed plan hash.
    seed_bound_referent(&app, "a-different-uid", 9);
    app.clock.advance(3600);
    let response = app
        .get(&format!(
            "/api/v1/namespaces/{NS_A}/preflights/pf-res?planHash=sha256:{}",
            "c".repeat(64)
        ))
        .await;
    let text = response.text();
    assert!(!text.contains("caBundleChanged"), "{text}");
    assert!(!text.contains("inputsDigestChanged"), "{text}");
}

/// **A referent whose revision moved is named, typed, from structured fields.**
#[tokio::test]
async fn a_referent_whose_revision_moved_is_reported_with_its_kind_and_name() {
    // The recorded binding says `KafkaCluster/target` at uid-target,
    // generation 1.
    for (uid, generation, why) in [
        ("uid-target", 2, "an edit bumped its generation"),
        ("a-new-uid", 1, "it was deleted and recreated"),
    ] {
        let app = TestApp::new();
        seed_preflight(
            &app.fake,
            NS_A,
            "pf-ref",
            "Restore",
            None,
            Some(LOCAL_ADMIN_ACTOR),
        );
        seed_bound_referent(&app, uid, generation);
        let response = app
            .get(&format!("/api/v1/namespaces/{NS_A}/preflights/pf-ref"))
            .await;
        let item = response.json()["item"].clone();
        assert_eq!(item["stale"], true, "{why}");
        assert_eq!(item["applicable"], false, "{why}");
        assert!(
            item["staleReasons"].as_array().unwrap().iter().any(|r| {
                r["reason"] == "referentChanged"
                    && r["kind"] == "KafkaCluster"
                    && r["name"] == "target"
            }),
            "{why}: {}",
            item["staleReasons"]
        );
        // The comparison says what it covered.
        assert!(item["staleBasis"]
            .as_array()
            .unwrap()
            .iter()
            .any(|b| b.as_str().unwrap().starts_with("referents:")));
        app.fake.assert_strict();
    }
}

/// A referent that was DELETED is a change too, and is named the same way.
#[tokio::test]
async fn a_referent_that_vanished_is_reported_as_changed() {
    let app = TestApp::new();
    seed_preflight(
        &app.fake,
        NS_A,
        "pf-gone",
        "Restore",
        None,
        Some(LOCAL_ADMIN_ACTOR),
    );
    // The `KafkaCluster/target` the binding names is never seeded.
    let response = app
        .get(&format!("/api/v1/namespaces/{NS_A}/preflights/pf-gone"))
        .await;
    let reasons = response.json()["item"]["staleReasons"].clone();
    assert!(
        reasons
            .as_array()
            .unwrap()
            .iter()
            .any(|r| { r["reason"] == "referentChanged" && r["name"] == "target" }),
        "{reasons}"
    );
    app.fake.assert_strict();
}

/// **A referent that could not be READ is `unverifiable`, never "unchanged".**
///
/// This is the fail-closed rule. A transient Kubernetes failure, or a kind
/// outside the sealed adapter, must not read as "I compared it and it
/// matches": that is `applicable: true` for a verdict nobody re-checked.
#[tokio::test]
async fn a_referent_that_cannot_be_read_fails_closed() {
    // (a) the read fails.
    let app = TestApp::new();
    seed_preflight(
        &app.fake,
        NS_A,
        "pf-unread",
        "Restore",
        None,
        Some(LOCAL_ADMIN_ACTOR),
    );
    seed_bound_referent(&app, "uid-target", 1);
    app.fake.inject(support::Fault {
        method: "GET",
        path_contains: "/kafkaclusters/target".to_string(),
        status: 500,
        reason: "InternalError",
        delay: None,
        remaining: 1,
    });
    let response = app
        .get(&format!("/api/v1/namespaces/{NS_A}/preflights/pf-unread"))
        .await;
    let item = response.json()["item"].clone();
    assert_eq!(item["stale"], true);
    assert_eq!(
        item["applicable"], false,
        "an unreadable referent failed OPEN"
    );
    let unverifiable: Vec<&serde_json::Value> = item["staleReasons"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|r| r["reason"] == "unverifiable")
        .collect();
    assert!(
        unverifiable
            .iter()
            .any(|r| r["kind"] == "KafkaCluster" && r["name"] == "target"),
        "{:?}",
        item["staleReasons"]
    );
    // A refusal to answer always says what it could not check.
    for reason in &unverifiable {
        assert!(reason["basis"].is_string(), "{reason}");
    }

    // (b) a kind this build has no read for. Both cluster-scoped trust kinds
    // ARE read now (PREFLIGHT-TRUSTROSTER-STALE, below); a kind a later
    // controller adds is not, and is still reported rather than skipped.
    let app = TestApp::new();
    seed_preflight(
        &app.fake,
        NS_A,
        "pf-future",
        "Restore",
        None,
        Some(LOCAL_ADMIN_ACTOR),
    );
    let mut object = app.fake.object("preflights", NS_A, "pf-future").unwrap();
    object["status"]["binding"]["referents"] = json!([
        {"kind": "FutureTrustKind", "name": "default", "uid": "uid-future", "generation": 1}
    ]);
    app.fake.seed("preflights", NS_A, object);
    let response = app
        .get(&format!("/api/v1/namespaces/{NS_A}/preflights/pf-future"))
        .await;
    let item = response.json()["item"].clone();
    assert_eq!(item["applicable"], false);
    assert!(item["staleReasons"]
        .as_array()
        .unwrap()
        .iter()
        .any(|r| { r["reason"] == "unverifiable" && r["kind"] == "FutureTrustKind" }));
    app.fake.assert_strict();
}

// ======================================================================
// PREFLIGHT-TRUSTROSTER-STALE: the cluster-scoped trust referents
// ======================================================================
//
// The controller records `TrustRoster/default` whenever it exists (since
// `4c4d2ed`) and the namespace's governing `TrustPolicy`. Until this fix the
// API could read neither, so EVERY readiness result on a cluster with a roster
// was served `unverifiable` → stale, and the wizard refused all of them. Now
// both are compared by uid and generation like every other referent — and a
// read that is refused stays `unverifiable`, which is the fail-closed half.

/// One trust kind under test: its binding kind, its RBAC plural, the name the
/// binding records, and a minimal object of that kind the typed read accepts.
struct TrustKind {
    kind: &'static str,
    plural: &'static str,
    name: &'static str,
    spec: fn() -> Value,
}

const ROSTER: TrustKind = TrustKind {
    kind: "TrustRoster",
    plural: "trustrosters",
    name: "default",
    spec: || json!({"approverKeys": [], "signingKeys": [], "allowedClusterIds": []}),
};

const POLICY: TrustKind = TrustKind {
    kind: "TrustPolicy",
    plural: "trustpolicies",
    name: "org-default",
    spec: || json!({"default": false, "namespaces": [NS_A], "keys": []}),
};

/// A completed restore check whose binding names the bound `KafkaCluster`
/// (unchanged, seeded here) and ONE trust referent recorded at `uid`/`generation`.
fn seed_trust_bound(app: &TestApp, id: &str, trust: &TrustKind, uid: &str, generation: i64) {
    seed_preflight(
        &app.fake,
        NS_A,
        id,
        "Restore",
        None,
        Some(LOCAL_ADMIN_ACTOR),
    );
    seed_bound_referent(app, "uid-target", 1);
    let mut object = app.fake.object("preflights", NS_A, id).unwrap();
    object["status"]["binding"]["referents"] = json!([
        {"kind": "KafkaCluster", "name": "target", "uid": "uid-target", "generation": 1},
        {"kind": trust.kind, "name": trust.name, "uid": uid, "generation": generation}
    ]);
    app.fake.seed("preflights", NS_A, object);
}

/// The live trust object, as it is NOW.
fn seed_trust_live(app: &TestApp, trust: &TrustKind, uid: &str, generation: i64) {
    app.fake.seed_cluster(
        trust.plural,
        json!({
            "metadata": {"name": trust.name, "uid": uid, "generation": generation},
            "spec": (trust.spec)(),
        }),
    );
}

async fn read_item(app: &TestApp, id: &str) -> Value {
    app.get(&format!("/api/v1/namespaces/{NS_A}/preflights/{id}"))
        .await
        .json()["item"]
        .clone()
}

/// And the request the recomputation made is the one read the grant allows:
/// `GET` of that object by name, cluster-scoped, and no list.
fn assert_read_by_name(app: &TestApp, trust: &TrustKind) {
    let wanted = format!("/apis/logweir.dev/v1alpha1/{}/{}", trust.plural, trust.name);
    let reads: Vec<String> = app
        .fake
        .requests()
        .into_iter()
        .filter(|r| r.path.contains(trust.plural))
        .map(|r| format!("{} {}", r.method, r.path))
        .collect();
    assert_eq!(reads, vec![format!("GET {wanted}")], "{}", trust.kind);
}

async fn an_unchanged_trust_referent_leaves_the_verdict_applicable(trust: &TrustKind) {
    let app = TestApp::new();
    seed_trust_bound(&app, "pf-trust-same", trust, "uid-trust", 3);
    seed_trust_live(&app, trust, "uid-trust", 3);
    let item = read_item(&app, "pf-trust-same").await;
    assert_eq!(
        item["staleReasons"],
        json!([]),
        "{}: an unchanged trust referent made the verdict stale",
        trust.kind
    );
    assert_eq!(item["stale"], false, "{}", trust.kind);
    assert_eq!(item["applicable"], true, "{}", trust.kind);
    assert!(
        item["staleBasis"]
            .as_array()
            .unwrap()
            .contains(&json!("referents:2")),
        "{}: both referents were compared: {}",
        trust.kind,
        item["staleBasis"]
    );
    assert_read_by_name(&app, trust);
    app.fake.assert_strict();
}

async fn a_trust_referent_whose_generation_moved_is_stale(trust: &TrustKind) {
    let app = TestApp::new();
    seed_trust_bound(&app, "pf-trust-edited", trust, "uid-trust", 3);
    seed_trust_live(&app, trust, "uid-trust", 4);
    let item = read_item(&app, "pf-trust-edited").await;
    assert_eq!(item["stale"], true, "{}", trust.kind);
    assert_eq!(item["applicable"], false, "{}", trust.kind);
    assert_eq!(
        item["staleReasons"],
        json!([{"reason": "referentChanged", "kind": trust.kind, "name": trust.name}]),
        "{}: an edited trust object must be named as the change",
        trust.kind
    );
    app.fake.assert_strict();
}

async fn a_recreated_trust_referent_is_stale(trust: &TrustKind) {
    let app = TestApp::new();
    seed_trust_bound(&app, "pf-trust-recreated", trust, "uid-trust", 3);
    // SAME generation, NEW uid: a delete and re-create of the same name.
    seed_trust_live(&app, trust, "uid-trust-recreated", 3);
    let item = read_item(&app, "pf-trust-recreated").await;
    assert_eq!(item["stale"], true, "{}", trust.kind);
    assert_eq!(item["applicable"], false, "{}", trust.kind);
    assert_eq!(
        item["staleReasons"],
        json!([{"reason": "referentChanged", "kind": trust.kind, "name": trust.name}]),
        "{}: a re-created trust object must be named as the change",
        trust.kind
    );
    app.fake.assert_strict();
}

async fn a_refused_trust_read_stays_unverifiable(trust: &TrustKind) {
    let app = TestApp::new();
    seed_trust_bound(&app, "pf-trust-refused", trust, "uid-trust", 3);
    // The object exists and is UNCHANGED — the only thing wrong is that this
    // service may not read it. That must not read as "unchanged".
    seed_trust_live(&app, trust, "uid-trust", 3);
    app.fake.inject(support::Fault {
        method: "GET",
        path_contains: format!("/{}/{}", trust.plural, trust.name),
        status: 403,
        reason: "Forbidden",
        delay: None,
        remaining: 1,
    });
    let item = read_item(&app, "pf-trust-refused").await;
    assert_eq!(item["stale"], true, "{}", trust.kind);
    assert_eq!(
        item["applicable"], false,
        "{}: a refused trust read failed OPEN",
        trust.kind
    );
    let reasons = item["staleReasons"].as_array().unwrap();
    assert_eq!(reasons.len(), 1, "{}: {reasons:?}", trust.kind);
    assert_eq!(reasons[0]["reason"], "unverifiable");
    assert_eq!(reasons[0]["kind"], trust.kind);
    assert_eq!(reasons[0]["name"], trust.name);
    assert!(
        reasons[0]["basis"]
            .as_str()
            .unwrap()
            .contains("could not be read"),
        "{}: {}",
        trust.kind,
        reasons[0]
    );
    app.fake.assert_strict();
}

/// A trust object that is gone is a change, named like any other vanished
/// referent — never "unchanged", never "unverifiable".
async fn a_vanished_trust_referent_is_stale(trust: &TrustKind) {
    let app = TestApp::new();
    seed_trust_bound(&app, "pf-trust-gone", trust, "uid-trust", 3);
    let item = read_item(&app, "pf-trust-gone").await;
    assert_eq!(item["stale"], true, "{}", trust.kind);
    assert_eq!(
        item["staleReasons"],
        json!([{"reason": "referentChanged", "kind": trust.kind, "name": trust.name}]),
        "{}",
        trust.kind
    );
    app.fake.assert_strict();
}

#[tokio::test]
async fn an_unchanged_trust_roster_leaves_the_verdict_applicable() {
    an_unchanged_trust_referent_leaves_the_verdict_applicable(&ROSTER).await;
}

#[tokio::test]
async fn a_trust_roster_whose_generation_moved_makes_the_verdict_stale() {
    a_trust_referent_whose_generation_moved_is_stale(&ROSTER).await;
}

#[tokio::test]
async fn a_recreated_trust_roster_makes_the_verdict_stale() {
    a_recreated_trust_referent_is_stale(&ROSTER).await;
}

#[tokio::test]
async fn a_refused_trust_roster_read_keeps_the_verdict_stale() {
    a_refused_trust_read_stays_unverifiable(&ROSTER).await;
}

#[tokio::test]
async fn a_deleted_trust_roster_makes_the_verdict_stale() {
    a_vanished_trust_referent_is_stale(&ROSTER).await;
}

#[tokio::test]
async fn an_unchanged_trust_policy_leaves_the_verdict_applicable() {
    an_unchanged_trust_referent_leaves_the_verdict_applicable(&POLICY).await;
}

#[tokio::test]
async fn a_trust_policy_whose_generation_moved_makes_the_verdict_stale() {
    a_trust_referent_whose_generation_moved_is_stale(&POLICY).await;
}

#[tokio::test]
async fn a_recreated_trust_policy_makes_the_verdict_stale() {
    a_recreated_trust_referent_is_stale(&POLICY).await;
}

#[tokio::test]
async fn a_refused_trust_policy_read_keeps_the_verdict_stale() {
    a_refused_trust_read_stays_unverifiable(&POLICY).await;
}

#[tokio::test]
async fn a_deleted_trust_policy_makes_the_verdict_stale() {
    a_vanished_trust_referent_is_stale(&POLICY).await;
}

/// **Only `TrustRoster/default` is read.** The controller records no other
/// roster name, and the grant is `get` with `resourceNames: ["default"]`. A
/// binding that names another roster is `unverifiable` WITHOUT a Kubernetes
/// call — `assert_strict` fails on any roster request but `GET .../default`.
#[tokio::test]
async fn a_roster_not_named_default_is_unverifiable_and_never_read() {
    let app = TestApp::new();
    let other = TrustKind {
        name: "logweir-trust",
        ..ROSTER
    };
    seed_trust_bound(&app, "pf-trust-other", &other, "uid-trust", 1);
    let item = read_item(&app, "pf-trust-other").await;
    assert_eq!(item["stale"], true);
    assert_eq!(item["applicable"], false);
    let reasons = item["staleReasons"].as_array().unwrap();
    assert_eq!(reasons.len(), 1, "{reasons:?}");
    assert_eq!(reasons[0]["reason"], "unverifiable");
    assert_eq!(reasons[0]["kind"], "TrustRoster");
    assert_eq!(reasons[0]["name"], "logweir-trust");
    assert!(reasons[0]["basis"]
        .as_str()
        .unwrap()
        .contains("only the TrustRoster named `default`"));
    assert!(
        !app.fake
            .requests()
            .iter()
            .any(|r| r.path.contains("trustrosters")),
        "a roster other than `default` must not be requested at all"
    );
    app.fake.assert_strict();
}

/// A completed verdict with no recorded binding cannot be compared with
/// anything, and says so rather than passing.
#[tokio::test]
async fn a_result_with_no_binding_is_unverifiable() {
    let app = TestApp::new();
    seed_preflight(
        &app.fake,
        NS_A,
        "pf-nobind",
        "Restore",
        None,
        Some(LOCAL_ADMIN_ACTOR),
    );
    let mut object = app.fake.object("preflights", NS_A, "pf-nobind").unwrap();
    object["status"].as_object_mut().unwrap().remove("binding");
    app.fake.seed("preflights", NS_A, object);
    let response = app
        .get(&format!("/api/v1/namespaces/{NS_A}/preflights/pf-nobind"))
        .await;
    let item = response.json()["item"].clone();
    assert_eq!(item["stale"], true);
    assert_eq!(item["applicable"], false);
    assert_eq!(item["staleReasons"][0]["reason"], "unverifiable");
    assert!(item["staleReasons"][0]["basis"]
        .as_str()
        .unwrap()
        .contains("no binding"));
}

/// **No code path reads `status.message` to decide staleness.**
///
/// The first cut recovered the reasons from the controller's prose. That prose
/// is redacted and capped at 512 characters, so a long referent list lost its
/// closing bracket and the parse returned NOTHING — reporting a downgraded
/// verdict as `applicable: true`. It failed OPEN, which is the one direction a
/// staleness check may never fail. The recomputation replaced it; this is the
/// row that stops it coming back.
#[test]
fn staleness_is_computed_from_structured_fields_and_never_from_a_message() {
    let source = include_str!("../src/routes/preflights.rs");
    let code: Vec<&str> = source
        .lines()
        .filter(|l| {
            let t = l.trim_start();
            !t.starts_with("//") && !t.starts_with("/*") && !t.starts_with('*')
        })
        .collect();
    let staleness_at = code
        .iter()
        .position(|l| l.contains("fn staleness("))
        .expect("`staleness` exists");
    let end = code[staleness_at..]
        .iter()
        .position(|l| l.trim_end() == "}")
        .map_or(code.len(), |n| staleness_at + n);
    let body = code[staleness_at..end].join("\n");
    assert!(
        !body.contains("message"),
        "`staleness` reads a message again:\n{body}"
    );
    // And nothing anywhere in the module parses the controller's sentence.
    for banned in [
        "no longer applies",
        "recorded_reasons",
        "reason_from_token",
        "DOWNGRADE_MESSAGE_PREFIX",
    ] {
        assert!(
            !code.join("\n").contains(banned),
            "the message recovery is back: `{banned}`"
        );
    }
    // The comparison is core's, not a second implementation of it.
    assert!(
        code.join("\n").contains("stale_reasons("),
        "staleness no longer calls `check_contract::stale_reasons`"
    );
}

/// The API's own two reasons and the recomputed ones are one list, in order,
/// and `staleBasis` names what was compared.
#[tokio::test]
async fn the_expiry_the_plan_hash_and_the_referents_are_one_list() {
    let app = TestApp::new();
    let bound = format!("sha256:{}", "a".repeat(64));
    seed_preflight(
        &app.fake,
        NS_A,
        "pf-merge",
        "Restore",
        Some(&bound),
        Some(LOCAL_ADMIN_ACTOR),
    );
    seed_bound_referent(&app, "a-replacement-uid", 1);
    app.clock.advance(3600);
    let response = app
        .get(&format!(
            "/api/v1/namespaces/{NS_A}/preflights/pf-merge?planHash=sha256:{}",
            "c".repeat(64)
        ))
        .await;
    let item = response.json()["item"].clone();
    let reasons: Vec<String> = item["staleReasons"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["reason"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(
        reasons,
        vec!["expired", "planHashChanged", "referentChanged"],
        "{}",
        item["staleReasons"]
    );
    let basis: Vec<String> = item["staleBasis"]
        .as_array()
        .unwrap()
        .iter()
        .map(|b| b.as_str().unwrap().to_string())
        .collect();
    assert_eq!(basis.len(), 4, "{basis:?}");
    assert_eq!(basis[0], "expiry");
    assert_eq!(basis[1], "referents:1");
    // THE POLICY IS NAMED, AND SO IS WHO COMPARED IT. An empty
    // `staleReasons` must not be mistakeable for "nothing was checked".
    assert!(
        basis[2].starts_with("policyDigest:byController"),
        "{basis:?}"
    );
    assert_eq!(basis[3], "planHash");
}

/// A verdict whose every comparable input still matches reports **no** named
/// change — only the honest gap. This is the arm that would catch a
/// recomputation that reported drift where there was none.
#[tokio::test]
async fn a_verdict_whose_inputs_still_match_reports_no_change() {
    let app = TestApp::new();
    let bound = format!("sha256:{}", "a".repeat(64));
    seed_preflight(
        &app.fake,
        NS_A,
        "pf-same",
        "Restore",
        Some(&bound),
        Some(LOCAL_ADMIN_ACTOR),
    );
    seed_bound_referent(&app, "uid-target", 1);
    let response = app
        .get(&format!(
            "/api/v1/namespaces/{NS_A}/preflights/pf-same?planHash={bound}"
        ))
        .await;
    let item = response.json()["item"].clone();
    let reasons: Vec<String> = item["staleReasons"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["reason"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(
        reasons,
        Vec::<String>::new(),
        "the recomputation invented a change: {}",
        item["staleReasons"]
    );
    assert_eq!(item["stale"], false);
    assert_eq!(
        item["applicable"], true,
        "a verdict whose inputs all match is applicable"
    );
    // AND THE BASIS SAYS WHAT THAT VERDICT RESTS ON, so an empty reason list
    // cannot be read as "nothing was compared".
    let basis: Vec<String> = item["staleBasis"]
        .as_array()
        .unwrap()
        .iter()
        .map(|b| b.as_str().unwrap().to_string())
        .collect();
    assert!(basis.contains(&"expiry".to_string()), "{basis:?}");
    assert!(basis.contains(&"referents:1".to_string()), "{basis:?}");
    assert!(
        basis.iter().any(|b| b.starts_with("policyDigest:")),
        "{basis:?}"
    );
    app.fake.assert_strict();
}

/// A cancelled check reports no stale reason: its verdict is absent, not out
/// of date, and `state` and `terminal` are what say so.
#[tokio::test]
async fn a_cancel_request_is_not_a_stale_reason() {
    let app = TestApp::new();
    seed_running_preflight(&app.fake, NS_A, "pf-cancelled", LOCAL_ADMIN_ACTOR);
    let mut object = app.fake.object("preflights", NS_A, "pf-cancelled").unwrap();
    object["spec"]["cancelRequested"] = json!(true);
    app.fake.seed("preflights", NS_A, object);
    let response = app
        .get(&format!(
            "/api/v1/namespaces/{NS_A}/preflights/pf-cancelled"
        ))
        .await;
    let item = &response.json()["item"];
    assert_eq!(item["staleReasons"], json!([]));
    assert_eq!(item["staleBasis"], json!([]));
    assert_eq!(item["stale"], false);
    assert_eq!(
        item["applicable"], false,
        "a running check applies to nothing"
    );
    assert_eq!(item["state"], "running");
}

// ======================================================================
// ui-conn-followups: finding a connectivity check again
// ======================================================================

fn seed_cluster(app: &TestApp, name: &str, uid: &str) {
    app.fake.seed(
        "kafkaclusters",
        NS_A,
        json!({
            "metadata": {"name": name, "uid": uid, "generation": 1},
            "spec": {
                "bootstrapServers": ["kafka:9092"],
                "auth": {"mode": "scramSha512", "username": "u", "secretRef": {"name": "s"}, "tls": false},
                "role": "source"
            }
        }),
    );
}

/// A connectivity check carries the one label that finds it again.
///
/// Until it did, a console reload found nothing at all for a connection and
/// the panel read as though no check had ever run — which invites a second one
/// for an answer that already exists.
#[tokio::test]
async fn a_source_connection_check_is_labelled_so_a_reload_can_find_it() {
    let app = TestApp::new();
    let response = app
        .post(
            &format!("/api/v1/namespaces/{NS_A}/preflights"),
            Some("preflight-label-00001"),
            &json!({"operation": "sourceConnection", "sourceConnection": {"connectionRef": "source"}})
                .to_string(),
        )
        .await;
    assert_eq!(response.status.as_u16(), 202, "{}", response.text());
    let id = response.json()["item"]["id"].as_str().unwrap().to_string();
    let labels = app.fake.object("preflights", NS_A, &id).unwrap()["metadata"]["labels"].clone();

    assert_eq!(labels["logweir.dev/connection-test"], "source");
    // ONE LABEL, AND NOT THE DESTINATION ONES. A connectivity check names no
    // destination, so a `logweir.dev/destination` on it would put it in the
    // "what uses this destination" view of a destination it never read.
    assert!(labels["logweir.dev/destination"].is_null());
    assert!(labels["logweir.dev/destination-test"].is_null());
    assert_eq!(
        labels.as_object().map(serde_json::Map::len),
        Some(1),
        "exactly the one label that finds it again: {labels}"
    );
}

/// `lastTest` finds the newest connectivity check, and NEVER one that belongs
/// to a `KafkaCluster` deleted and recreated under the same name.
///
/// The label carries the connection's NAME, because that is what a selector
/// can be built from without a second read at create time. A recreated cluster
/// is a different set of brokers reached with a different credential, so the
/// UID recorded in each check's own binding is what makes the answer exact —
/// the same substitution the console's list view refuses row by row.
#[tokio::test]
async fn last_test_finds_the_newest_check_and_refuses_a_predecessors() {
    let app = TestApp::new();
    seed_cluster(&app, "source", "uid-now");

    let seed_check = |name: &str, created: &str, uid: Option<&str>, state: &str| {
        let referents = match uid {
            Some(uid) => json!([{"kind": "KafkaCluster", "name": "source", "uid": uid}]),
            None => json!([]),
        };
        app.fake.seed(
            "preflights",
            NS_A,
            json!({
                "metadata": {
                    "name": name,
                    "labels": {"logweir.dev/connection-test": "source"},
                    "creationTimestamp": created
                },
                "spec": {"request": {"operation": "SourceConnection", "sourceConnection": {"connectionRef": {"name": "source"}}, "timeoutSeconds": 120}, "cancelRequested": false},
                "status": {
                    "phase": "Completed", "observedAt": created,
                    "binding": {"operation": "SourceConnection", "referents": referents},
                    "result": {"state": state}
                }
            }),
        );
    };
    // Sorts LAST by name, oldest by time, and belongs to the cluster that used
    // to answer to this name.
    seed_check(
        "pf-zzzz",
        "2026-09-18T23:00:00Z",
        Some("uid-before"),
        "ready",
    );
    seed_check(
        "pf-aaaa",
        "2026-09-18T21:00:00Z",
        Some("uid-now"),
        "notReady",
    );
    seed_check(
        "pf-bbbb",
        "2026-09-18T22:00:00Z",
        Some("uid-now"),
        "unknown",
    );
    // Never got far enough to resolve a referent: not this connection's test.
    seed_check("pf-cccc", "2026-09-18T23:30:00Z", None, "ready");

    let response = app
        .get(&format!("/api/v1/namespaces/{NS_A}/connections/source"))
        .await;
    assert_eq!(response.status.as_u16(), 200, "{}", response.text());
    let last = &response.json()["item"]["lastTest"];
    assert_eq!(
        last["preflightId"], "pf-bbbb",
        "the newest check bound to THIS uid, not the newest by name and not a predecessor's"
    );
    assert_eq!(last["state"], "unknown");
    assert_eq!(last["truncated"], false);

    // ONE SELECTOR, and it is the connection-test one.
    let selectors: Vec<String> = app
        .fake
        .requests()
        .iter()
        .filter(|r| r.path.ends_with("/preflights"))
        .map(|r| r.query.clone())
        .collect();
    assert!(
        !selectors.is_empty(),
        "the detail read looked for checks at all"
    );
    assert!(
        selectors
            .iter()
            .all(|q| q.contains("connection-test") || q.contains("connection-test%3Dsource")),
        "lastTest selected on something other than the connectivity-test label: {selectors:?}"
    );

    // A CONNECTION WITH NO CHECK SAYS SO, and a LIST never pays for the scan.
    seed_cluster(&app, "other", "uid-other");
    let none = app
        .get(&format!("/api/v1/namespaces/{NS_A}/connections/other"))
        .await;
    assert!(none.json()["item"]["lastTest"].is_null());
    let list = app
        .get(&format!("/api/v1/namespaces/{NS_A}/connections"))
        .await;
    for item in list.json()["items"].as_array().unwrap() {
        assert!(
            item["lastTest"].is_null(),
            "a page of connections must not be a page of label scans"
        );
    }
}

/// **A referent recorded WITHOUT a generation is bound by uid alone.**
///
/// The controller records the recovery-point `Backup` (and the `Approval`)
/// with no generation — `Referent::generation` is `None` "for a kind whose
/// generation is not meaningful". Comparing the live object's generation
/// against that `None` reported `referentChanged` for every restore check
/// naming a recovery point, on every `?planHash=` re-read, so the console's
/// pre-submit re-read refused every such submit (found by PLAT-08.2's live
/// journey). The same uid is unchanged; a recreated object is still a change.
#[tokio::test]
async fn a_referent_recorded_without_a_generation_is_compared_by_uid_alone() {
    for (live_uid, changed, why) in [
        (
            "uid-point",
            false,
            "the same Backup, whatever its generation",
        ),
        (
            "uid-recreated",
            true,
            "a Backup recreated under the same name",
        ),
    ] {
        let app = TestApp::new();
        let mut preflight = seed_preflight(
            &app.fake,
            NS_A,
            "pf-point",
            "Restore",
            None,
            Some(LOCAL_ADMIN_ACTOR),
        );
        preflight["status"]["binding"]["referents"] = json!([
            {"kind": "KafkaCluster", "name": "target", "uid": "uid-target", "generation": 1},
            {"kind": "Backup", "name": "point", "uid": "uid-point"}
        ]);
        app.fake.seed("preflights", NS_A, preflight);
        seed_bound_referent(&app, "uid-target", 1);
        app.fake.seed(
            "backups",
            NS_A,
            json!({
                "metadata": {"name": "point", "uid": live_uid, "generation": 3},
                "spec": {"sourceRef": {"name": "source"}, "topics": ["orders"],
                         "archive": {"url": "s3://archive/team-a"},
                         "triggeredBy": "manual", "deadlineSeconds": 1800}
            }),
        );
        let response = app
            .get(&format!("/api/v1/namespaces/{NS_A}/preflights/pf-point"))
            .await;
        let item = response.json()["item"].clone();
        let names_point = item["staleReasons"].as_array().unwrap().iter().any(|r| {
            r["reason"] == "referentChanged" && r["kind"] == "Backup" && r["name"] == "point"
        });
        assert_eq!(names_point, changed, "{why}: {}", item["staleReasons"]);
        app.fake.assert_strict();
    }
}

// ======================================================================
// PLAT-15.2 — a restore readiness check about a CATALOG point
// ======================================================================

const CATALOG_POINT_ID: &str = "lwp1-0123456789abcdef0123456789abcdef";

fn catalog_point_request(plan: &str) -> Value {
    let mut request = restore_request(plan);
    let restore = request["restore"]
        .as_object_mut()
        .expect("the restore block");
    restore.remove("recoveryPoint");
    restore.insert(
        "catalogPoint".to_string(),
        json!({"catalog": "archive", "pointId": CATALOG_POINT_ID}),
    );
    request
}

/// The catalog point reaches the stored object as `catalogPointRef`, by the
/// catalog's NAME and the point's content-derived id, and nothing else is
/// invented beside it: no `recoveryPointRef` (CRD rule P10), no Backup read.
///
/// MUTANT: drop the `catalog_point_ref` mapping in `build` — the stored
/// object carries no reference and the controller never re-reads the row.
#[tokio::test]
async fn a_catalog_point_is_stored_as_the_reference_the_controller_reads() {
    let app = TestApp::new();
    let plan = plan();
    let response = app
        .post(
            &format!("/api/v1/namespaces/{NS_A}/preflights"),
            Some("preflight-catalog-00001"),
            &catalog_point_request(&plan).to_string(),
        )
        .await;
    assert_eq!(response.status.as_u16(), 202, "{}", response.text());
    let id = response.json()["item"]["id"].as_str().unwrap().to_string();
    let stored = app.fake.object("preflights", NS_A, &id).unwrap();
    let restore = &stored["spec"]["request"]["restore"];
    assert_eq!(
        restore["catalogPointRef"],
        json!({"catalogRef": {"name": "archive"}, "pointId": CATALOG_POINT_ID})
    );
    assert!(
        restore.get("recoveryPointRef").is_none_or(Value::is_null),
        "one point per check: {restore}"
    );
    app.fake.assert_strict();
}

/// The two refusals the route makes itself, each with its control.
///
/// MUTANTS: accept both references (the first request stores a check whose
/// `recoveryPoint.state` has two answers — the CRD's P10 refuses it only at
/// the API server, with a message nobody wrote for this form); accept any
/// point id (the second stores a reference no view entry can ever match).
#[tokio::test]
async fn a_catalog_point_is_refused_beside_a_backup_point_or_with_a_malformed_id() {
    let app = TestApp::new();
    let path = format!("/api/v1/namespaces/{NS_A}/preflights");
    let plan = plan();

    let mut both = catalog_point_request(&plan);
    both["restore"]["recoveryPoint"] =
        json!({"backupName": "logweir-backup-nightly-20260915-030000"});
    let response = app
        .post(&path, Some("preflight-catalog-00002"), &both.to_string())
        .await;
    response.assert_problem(422, "validation_failed");
    assert!(
        field_names(&response.json()).contains(&"restore.catalogPoint".to_string()),
        "{}",
        response.text()
    );

    for bad in [
        "lwp1-0123",
        "lwp1-0123456789ABCDEF0123456789ABCDEF",
        "lwp2-0123456789abcdef0123456789abcdef",
        "",
    ] {
        let mut request = catalog_point_request(&plan);
        request["restore"]["catalogPoint"]["pointId"] = json!(bad);
        let response = app
            .post(&path, Some("preflight-catalog-00003"), &request.to_string())
            .await;
        response.assert_problem(422, "validation_failed");
        assert!(
            field_names(&response.json()).contains(&"restore.catalogPoint.pointId".to_string()),
            "{bad:?}: {}",
            response.text()
        );
    }

    let mut request = catalog_point_request(&plan);
    request["restore"]["catalogPoint"]["catalog"] = json!("Not_A_Name");
    let response = app
        .post(&path, Some("preflight-catalog-00004"), &request.to_string())
        .await;
    response.assert_problem(422, "validation_failed");
    assert!(
        field_names(&response.json()).contains(&"restore.catalogPoint.catalog".to_string()),
        "{}",
        response.text()
    );

    assert_eq!(app.fake.count("preflights", NS_A), 0);

    // CONTROL: the same request, well formed, is accepted.
    let response = app
        .post(
            &path,
            Some("preflight-catalog-00005"),
            &catalog_point_request(&plan).to_string(),
        )
        .await;
    assert_eq!(response.status.as_u16(), 202, "{}", response.text());
}
