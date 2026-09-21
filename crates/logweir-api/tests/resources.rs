//! The resource routes against the fake cluster: connections, schedules and
//! the one permitted mutation, restores with opaque plan bytes, approvals and
//! the packet route, and the session.

mod support;

use serde_json::{json, Value};
use support::{TestApp, NS_A, NS_B};

fn post_bodies(app: &TestApp, plural: &str) -> Vec<Value> {
    app.fake
        .requests()
        .into_iter()
        .filter(|r| r.method == "POST" && r.path.ends_with(plural))
        .map(|r| serde_json::from_str(&r.body).unwrap())
        .collect()
}

#[tokio::test]
async fn plan_bytes_are_stored_byte_for_byte_and_never_reserialized() {
    let app = TestApp::new();
    let golden = support::golden_plan();
    // The golden plus every shape a reserialiser would normalise: CRLF, a
    // tab, trailing spaces, a BOM-less non-ASCII name, a line separator, a
    // YAML comment, key order the runner does not require, a duplicate blank
    // line and no final newline.
    let tricky = format!(
        "{golden}# trailing comment  \r\nname: \"orders-\u{00e9}t\u{00e9}\"\t\r\n\n\nz_last: 1\nu2028: \"a\u{2028}b\"\n  "
    );
    for (i, plan) in [golden.clone(), tricky].into_iter().enumerate() {
        let key = format!("plan-bytes-key-{i:02}");
        let body = support::restore_body(&plan);
        let created = app
            .post(
                &format!("/api/v1/namespaces/{NS_A}/restores"),
                Some(&key),
                &body.to_string(),
            )
            .await;
        assert_eq!(
            created.status,
            201,
            "{}",
            String::from_utf8_lossy(&created.body)
        );
        let item = &created.json()["item"];
        assert_eq!(
            item["planBytes"].as_str().unwrap().as_bytes(),
            plan.as_bytes()
        );
        assert_eq!(
            item["planHash"],
            logweir_core::ids::sha256_prefixed(plan.as_bytes())
        );
        assert_eq!(item["planBytesLength"], plan.len());
        let name = item["name"].as_str().unwrap().to_string();
        assert!(name.starts_with("rst-") && name.len() == 30);

        // What Kubernetes received, decoded, is the exact bytes.
        let sent = post_bodies(&app, "/restores").pop().unwrap();
        assert_eq!(
            sent["spec"]["planBytes"].as_str().unwrap().as_bytes(),
            plan.as_bytes()
        );
        assert_eq!(sent["spec"]["approvalRef"]["name"], "approval-1234abcd");
        // What is stored, and read back through the API, is the exact bytes.
        let stored = app.fake.object("restores", NS_A, &name).unwrap();
        assert_eq!(
            stored["spec"]["planBytes"].as_str().unwrap().as_bytes(),
            plan.as_bytes()
        );
        let read = app
            .get(&format!("/api/v1/namespaces/{NS_A}/restores/{name}"))
            .await
            .json();
        assert_eq!(
            read["item"]["planBytes"].as_str().unwrap().as_bytes(),
            plan.as_bytes()
        );
        // Lists omit the bytes but keep the hash.
        let list = app
            .get(&format!("/api/v1/namespaces/{NS_A}/restores"))
            .await
            .json();
        let row = list["items"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["name"] == name.as_str())
            .unwrap();
        assert!(row.get("planBytes").is_none());
        assert_eq!(
            row["planHash"],
            logweir_core::ids::sha256_prefixed(plan.as_bytes())
        );
    }
    app.fake.assert_strict();
}

#[tokio::test]
async fn a_plan_hash_that_does_not_match_the_bytes_is_refused() {
    let app = TestApp::new();
    let plan = support::golden_plan();
    let mut body = support::restore_body(&plan);
    // The hash of the bytes with the final newline trimmed: a one-byte change.
    body["planHash"] = json!(logweir_core::ids::sha256_prefixed(
        plan.trim_end().as_bytes()
    ));
    let response = app
        .post(
            &format!("/api/v1/namespaces/{NS_A}/restores"),
            Some("plan-mismatch-01"),
            &body.to_string(),
        )
        .await;
    response.assert_problem(422, "validation_failed");
    assert_eq!(response.json()["errors"][0]["code"], "plan_hash_mismatch");
    for (field, value) in [
        ("planHash", json!("abc")),
        ("planHash", json!("SHA256:00")),
        ("deadlineSeconds", json!(5)),
        ("pointInTime", json!("yesterday")),
        ("backupSetRef", json!("../../etc")),
    ] {
        let mut b = support::restore_body(&plan);
        b[field] = value;
        app.post(
            &format!("/api/v1/namespaces/{NS_A}/restores"),
            Some("plan-invalid-001"),
            &b.to_string(),
        )
        .await
        .assert_problem(422, "validation_failed");
    }
    let mut b = support::restore_body(&plan);
    b["target"]["topicNaming"]["prefix"] = json!("");
    app.post(
        &format!("/api/v1/namespaces/{NS_A}/restores"),
        Some("plan-invalid-002"),
        &b.to_string(),
    )
    .await
    .assert_problem(422, "validation_failed");
    assert!(app.fake.requests().is_empty());
}

/// PLAT-11.2: the declared mapping is checked against the PREFIX THIS REQUEST
/// STORES, and a duplicate target names BOTH sources.
///
/// WHY THE API CAN CHECK THIS WITHOUT PARSING THE PLAN. The mapping rule is
/// prefix concatenation and nothing else (`logweir_core::spec::
/// target_topic_prefix`), and `target.topicNaming.prefix` is a stored field of
/// `Restore.spec`, so `prefix + source` is a pure function of the object being
/// created. A console that previewed one mapping and submitted another is
/// refused here rather than in phase 0, after an approver has signed.
///
/// KILLS: dropping the duplicate check (a repeated source is accepted);
/// naming only one side of a duplicate; accepting a target that is not
/// `prefix + source`; accepting an identity map; accepting a mapped name a
/// broker would refuse; and storing the declaration on the object.
#[tokio::test]
async fn a_declared_topic_mapping_is_checked_against_the_stored_prefix() {
    let app = TestApp::new();
    let plan = support::golden_plan();
    let prefix = "restore-20260907t140500z-";

    // The happy path: every row is exactly `prefix + source`, and NOTHING of
    // the declaration reaches the object.
    let mut ok = support::restore_body(&plan);
    ok["topicMapping"] = json!([
        {"source": "orders", "target": format!("{prefix}orders")},
        {"source": "payments", "target": format!("{prefix}payments")},
    ]);
    let created = app
        .post(
            &format!("/api/v1/namespaces/{NS_A}/restores"),
            Some("mapping-ok-000001"),
            &ok.to_string(),
        )
        .await;
    assert_eq!(
        created.status,
        201,
        "{}",
        String::from_utf8_lossy(&created.body)
    );
    let name = created.json()["item"]["name"].as_str().unwrap().to_string();
    let stored = app.fake.object("restores", NS_A, &name).unwrap();
    assert!(
        stored["spec"].get("topicMapping").is_none(),
        "the declaration is a rail, never a stored field: {}",
        stored["spec"]
    );
    assert_eq!(stored["spec"]["target"]["topicNaming"]["prefix"], prefix);

    // TWO SOURCE TOPICS ONTO ONE TARGET NAME. With an injective prefix map
    // that is a REPEATED source, and the message names both rows.
    let mut duplicate = support::restore_body(&plan);
    duplicate["topicMapping"] = json!([
        {"source": "orders", "target": format!("{prefix}orders")},
        {"source": "orders", "target": format!("{prefix}orders")},
    ]);
    let refused = app
        .post(
            &format!("/api/v1/namespaces/{NS_A}/restores"),
            Some("mapping-dup-000001"),
            &duplicate.to_string(),
        )
        .await;
    refused.assert_problem(422, "validation_failed");
    let error = &refused.json()["errors"][0];
    assert_eq!(error["code"], "duplicate_mapping");
    assert_eq!(error["field"], "topicMapping[1].target");
    let message = error["message"].as_str().unwrap();
    assert!(
        message.contains("`orders`") && message.contains(&format!("`{prefix}orders`")),
        "the refusal names both sources and the target they share: {message}"
    );

    // A row whose target is not `prefix + source`: the preview and the
    // submission disagree, and the expected name is in the message.
    let mut renamed = support::restore_body(&plan);
    renamed["topicMapping"] = json!([{"source": "orders", "target": "restore-elsewhere-orders"}]);
    let refused = app
        .post(
            &format!("/api/v1/namespaces/{NS_A}/restores"),
            Some("mapping-ren-000001"),
            &renamed.to_string(),
        )
        .await;
    refused.assert_problem(422, "validation_failed");
    assert_eq!(refused.json()["errors"][0]["code"], "mapping_mismatch");
    assert!(refused.json()["errors"][0]["message"]
        .as_str()
        .unwrap()
        .contains(&format!("{prefix}orders")));

    // An identity map -- a restore writing over the topic it came from.
    let mut identity = support::restore_body(&plan);
    identity["target"]["topicNaming"]["prefix"] = json!("orders");
    identity["topicMapping"] = json!([{"source": "orders", "target": "orders"}]);
    let refused = app
        .post(
            &format!("/api/v1/namespaces/{NS_A}/restores"),
            Some("mapping-idn-000001"),
            &identity.to_string(),
        )
        .await;
    refused.assert_problem(422, "validation_failed");
    assert_eq!(refused.json()["errors"][0]["code"], "mapping_identity");

    // A mapped name no broker would accept: 249 characters is the bound.
    let long_source = "o".repeat(240);
    let mut illegal = support::restore_body(&plan);
    illegal["topicMapping"] =
        json!([{"source": long_source, "target": format!("{prefix}{long_source}")}]);
    let refused = app
        .post(
            &format!("/api/v1/namespaces/{NS_A}/restores"),
            Some("mapping-ill-000001"),
            &illegal.to_string(),
        )
        .await;
    refused.assert_problem(422, "validation_failed");
    assert_eq!(refused.json()["errors"][0]["code"], "mapped_name_illegal");

    // A source that is not a topic name at all -- a glob, a space, an empty
    // string -- is refused on the SOURCE and never silently mapped.
    for bad in ["orders*", "two words", ""] {
        let mut b = support::restore_body(&plan);
        b["topicMapping"] = json!([{"source": bad, "target": format!("{prefix}{bad}")}]);
        let refused = app
            .post(
                &format!("/api/v1/namespaces/{NS_A}/restores"),
                Some("mapping-src-000001"),
                &b.to_string(),
            )
            .await;
        refused.assert_problem(422, "validation_failed");
        assert_eq!(
            refused.json()["errors"][0]["code"],
            "invalid_topic",
            "{bad} is not a topic name"
        );
    }

    // AND AN ABSENT DECLARATION IS EXACTLY WHAT THIS ROUTE DID BEFORE. No
    // field, no mapping errors, 201.
    let plain = app
        .post(
            &format!("/api/v1/namespaces/{NS_A}/restores"),
            Some("mapping-absent-0001"),
            &support::restore_body(&plan).to_string(),
        )
        .await;
    assert_eq!(
        plain.status,
        201,
        "{}",
        String::from_utf8_lossy(&plain.body)
    );
}

#[tokio::test]
async fn connections_project_secret_names_only_and_create_typed_objects() {
    let app = TestApp::new();
    app.fake.seed(
        "kafkaclusters",
        NS_A,
        json!({
            "metadata": {"name": "source", "annotations": {"internal": "x"}, "managedFields": [{"manager": "kubectl"}]},
            "spec": {"bootstrapServers": ["kafka:9096"], "auth": {"mode": "scramSha512", "username": "scram-user", "secretRef": {"name": "source-scram"}, "tls": false}, "role": "source"},
            "status": {"reachable": true, "clusterId": "M29I2S7FQPyHBEX12Vx7XA", "observedAt": "2026-09-15T11:00:00Z", "reason": "Reachable"}
        }),
    );
    let list = app
        .get(&format!("/api/v1/namespaces/{NS_A}/connections"))
        .await
        .json();
    let item = &list["items"][0];
    assert_eq!(item["auth"]["credentialRef"]["name"], "source-scram");
    assert_eq!(item["reachability"]["state"], "reachable");
    assert_eq!(item["reachability"]["clusterId"], "M29I2S7FQPyHBEX12Vx7XA");
    let text = list.to_string();
    for forbidden in [
        "managedFields",
        "annotations",
        "internal",
        "password",
        "secretRef",
    ] {
        assert!(!text.contains(forbidden), "{forbidden} leaked: {text}");
    }

    let created = app
        .post(
            &format!("/api/v1/namespaces/{NS_A}/connections"),
            Some("connection-key-9"),
            &support::connection_body().to_string(),
        )
        .await;
    assert_eq!(created.status, 201);
    let sent = post_bodies(&app, "/kafkaclusters").pop().unwrap();
    assert_eq!(sent["kind"], "KafkaCluster");
    assert_eq!(sent["spec"]["auth"]["secretRef"]["name"], "source-scram");
    assert_eq!(sent["spec"]["role"], "source");
    assert!(sent["status"].is_null());
    // The field manager is recorded on the write.
    let post = app
        .fake
        .requests()
        .into_iter()
        .find(|r| r.method == "POST")
        .unwrap();
    assert!(
        post.query.contains("fieldManager=logweir-api"),
        "{}",
        post.query
    );

    // Validation: scram needs a username and a credential name, plaintext takes neither.
    for body in [
        json!({"role": "source", "bootstrapServers": ["kafka:9092"], "auth": {"mode": "scramSha512", "tls": false}}),
        json!({"role": "source", "bootstrapServers": ["kafka:9092"], "auth": {"mode": "plaintext", "username": "u", "tls": false}}),
        json!({"role": "source", "bootstrapServers": [], "auth": {"mode": "plaintext", "tls": false}}),
        json!({"role": "source", "bootstrapServers": ["SASL_SSL://kafka:9092"], "auth": {"mode": "plaintext", "tls": false}}),
        json!({"role": "primary", "bootstrapServers": ["kafka:9092"], "auth": {"mode": "plaintext", "tls": false}}),
        json!({"role": "source", "bootstrapServers": ["kafka:9092"], "auth": {"mode": "plaintext", "tls": false, "password": "x"}}),
    ] {
        app.post(
            &format!("/api/v1/namespaces/{NS_A}/connections"),
            Some("connection-bad-1"),
            &body.to_string(),
        )
        .await
        .assert_problem(422, "validation_failed");
    }
    app.fake.assert_strict();
}

#[tokio::test]
async fn set_suspension_patches_only_spec_suspend_under_a_precondition() {
    let app = TestApp::new();
    let created = app
        .post(
            &format!("/api/v1/namespaces/{NS_A}/schedules"),
            Some("schedule-susp-01"),
            &support::schedule_body().to_string(),
        )
        .await
        .json();
    let name = created["item"]["name"].as_str().unwrap().to_string();
    let rv = created["item"]["resourceVersion"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(created["item"]["suspended"], true);
    let command = format!("/api/v1/namespaces/{NS_A}/schedules/{name}:set-suspension");

    // Stale version: 412, nothing changed.
    let stale = app
        .post(
            &command,
            None,
            &json!({"suspended": false, "expectedResourceVersion": "1"}).to_string(),
        )
        .await;
    stale.assert_problem(412, "precondition_failed");
    assert_eq!(
        app.fake.object("backupschedules", NS_A, &name).unwrap()["spec"]["suspend"],
        true
    );

    // Current version: 200 with a new resourceVersion.
    let ok = app
        .post(
            &command,
            None,
            &json!({"suspended": false, "expectedResourceVersion": rv}).to_string(),
        )
        .await;
    assert_eq!(ok.status, 200, "{}", String::from_utf8_lossy(&ok.body));
    assert_eq!(ok.json()["item"]["suspended"], false);
    assert_ne!(ok.json()["item"]["resourceVersion"], rv.as_str());

    // Replaying the same command with the old version is now stale.
    app.post(
        &command,
        None,
        &json!({"suspended": false, "expectedResourceVersion": rv}).to_string(),
    )
    .await
    .assert_problem(412, "precondition_failed");

    // The PATCH body was exactly {metadata.resourceVersion, spec.suspend}.
    let patches: Vec<Value> = app
        .fake
        .requests()
        .into_iter()
        .filter(|r| r.method == "PATCH")
        .map(|r| {
            let ct = r
                .headers
                .iter()
                .find(|(k, _)| k == "content-type")
                .unwrap()
                .1
                .clone();
            assert_eq!(ct, "application/merge-patch+json");
            serde_json::from_str(&r.body).unwrap()
        })
        .collect();
    assert_eq!(patches.len(), 3);
    for patch in &patches {
        let keys: Vec<&String> = patch.as_object().unwrap().keys().collect();
        assert_eq!(keys, vec!["metadata", "spec"]);
        assert_eq!(patch["spec"].as_object().unwrap().len(), 1);
        assert_eq!(patch["metadata"].as_object().unwrap().len(), 1);
    }

    // Strict body: no other field may ride along.
    app.post(
        &command,
        None,
        &json!({"suspended": true, "expectedResourceVersion": rv, "schedule": "* * * * *"})
            .to_string(),
    )
    .await
    .assert_problem(422, "validation_failed");
    app.post(&command, None, &json!({"suspended": true}).to_string())
        .await
        .assert_problem(422, "validation_failed");
    // Unknown commands and malformed names.
    app.post(
        &format!("/api/v1/namespaces/{NS_A}/schedules/{name}:delete"),
        None,
        "{}",
    )
    .await
    .assert_problem(404, "not_found");
    app.post(
        &format!("/api/v1/namespaces/{NS_A}/schedules/{name}"),
        None,
        "{}",
    )
    .await
    .assert_problem(404, "not_found");
    app.get(&format!(
        "/api/v1/namespaces/{NS_A}/schedules/{name}:set-suspension"
    ))
    .await
    .assert_problem(404, "not_found");
    app.post(
        &format!("/api/v1/namespaces/{NS_A}/schedules/absent:set-suspension"),
        None,
        &json!({"suspended": true, "expectedResourceVersion": "5"}).to_string(),
    )
    .await
    .assert_problem(404, "not_found");
    app.fake.assert_strict();
}

#[tokio::test]
async fn schedule_validation_uses_the_controller_cron_parser() {
    let app = TestApp::new();
    for (i, schedule) in ["@daily", "*/5 * * * *", "0 3 * * 1-5"]
        .into_iter()
        .enumerate()
    {
        let mut body = support::schedule_body();
        body["schedule"] = json!(schedule);
        let key = format!("cron-ok-key-{i}");
        let r = app
            .post(
                &format!("/api/v1/namespaces/{NS_A}/schedules"),
                Some(&key),
                &body.to_string(),
            )
            .await;
        assert!(
            r.status == 201 || r.status == 200,
            "{schedule}: {}",
            String::from_utf8_lossy(&r.body)
        );
    }
    for schedule in ["@yearly", "* * * *", "61 * * * *", "L * * * *", ""] {
        let mut body = support::schedule_body();
        body["schedule"] = json!(schedule);
        let r = app
            .post(
                &format!("/api/v1/namespaces/{NS_A}/schedules"),
                Some("cron-bad-key"),
                &body.to_string(),
            )
            .await;
        r.assert_problem(422, "validation_failed");
    }
}

#[tokio::test]
async fn approvals_hide_documents_except_on_the_packet_route() {
    let app = TestApp::new();
    let approval_bytes = "approver: ops\nplan_hash: sha256:aa\nsubject_kind: Restore\n";
    let sidecar_bytes = r#"{"payloadType":"application/vnd.logweir.approval+json","signatures":[{"keyid":"k","sig":"c2ln"}]}"#;
    app.fake.seed(
        "approvals",
        NS_A,
        json!({
            "metadata": {"name": "approval-1"},
            "spec": {"subjectRef": {"kind": "Restore", "name": "rst-x"}, "planHash": "sha256:aa", "approvalBytes": approval_bytes, "sidecarBytes": sidecar_bytes},
            "status": {"verified": true, "matchedKeyId": "k", "approver": "ops", "selfAttestedRisk": false,
                       "verifiedSubjectRef": {"apiVersion": "logweir.dev/v1alpha1", "kind": "Restore", "name": "rst-x", "namespace": "team-a", "uid": "u-1"}}
        }),
    );
    for path in [
        format!("/api/v1/namespaces/{NS_A}/approvals"),
        format!("/api/v1/namespaces/{NS_A}/approvals/approval-1"),
    ] {
        let response = app.get(&path).await;
        assert_eq!(response.status, 200);
        let text = String::from_utf8_lossy(&response.body);
        assert!(
            !text.contains("\"approvalBytes\"") && !text.contains("\"sidecarBytes\""),
            "{text}"
        );
        assert!(
            !text.contains("approver: ops") && !text.contains("c2ln"),
            "{text}"
        );
        assert!(!text.contains("payloadType"), "{text}");
    }
    let meta = app
        .get(&format!("/api/v1/namespaces/{NS_A}/approvals/approval-1"))
        .await
        .json();
    assert_eq!(meta["item"]["verified"], true);
    assert_eq!(meta["item"]["verifiedSubject"]["uid"], "u-1");
    assert_eq!(meta["item"]["approvalBytesLength"], approval_bytes.len());

    let packet = app
        .get(&format!(
            "/api/v1/namespaces/{NS_A}/approvals/approval-1/packet"
        ))
        .await
        .json();
    assert_eq!(
        packet["item"]["approvalBytes"].as_str().unwrap(),
        approval_bytes
    );
    assert_eq!(
        packet["item"]["sidecarBytes"].as_str().unwrap(),
        sidecar_bytes
    );
    app.fake.assert_strict();
}

#[tokio::test]
async fn session_and_namespaces_come_from_configuration_only() {
    let app = TestApp::new();
    let session = app.get("/api/v1/session").await;
    assert_eq!(session.status, 200);
    let v = session.json();
    assert_eq!(v["authenticationMode"], "localAdmin");
    assert_eq!(v["actor"]["issuer"], "urn:logweir:local-admin");
    assert_eq!(v["actor"]["subject"], "admin");
    assert!(v["expiresAt"].is_null() && v["csrfToken"].is_null());
    let names: Vec<&str> = v["namespaces"]
        .as_array()
        .unwrap()
        .iter()
        .map(|g| g["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, vec![NS_A, NS_B]);
    let caps = &v["capabilities"];
    for enabled in [
        "connectionsRead",
        "connectionCreate",
        "schedulesRead",
        "scheduleCreate",
        "scheduleSetSuspension",
        "backupsRead",
        "restoresRead",
        "restoreCreate",
        "approvalsRead",
        "approvalPacketRead",
        "operationsRead",
        // D2 W12: the three domains now have routes, so the local
        // administrator — who may do every implemented action — sees them.
        // Each flag is the domain's READ floor; whether the actor may also
        // start or cancel is the role table, published per grant in `roles`.
        "topicDiscovery",
        "preflight",
        "destinations",
        "credentialWrite",
        // D1 W6: `POST .../backups` has a route. `scheduleCreate` also covers
        // `PUT .../schedules/{name}`: the same authority, one flag, and
        // `role_matrix` pins that the two actions have the same role row.
        "manualBackupCreate",
        // D3 W11: the five read families, the catalog create and the event
        // stream have routes; the local administrator holds every implemented
        // action, so every one of them is on.
        "protection",
        "rehearsals",
        "catalogs",
        "catalogConnect",
        "retention",
        "trustPoliciesRead",
        "operationEvents",
    ] {
        assert_eq!(caps[enabled], true, "{enabled}");
    }
    for disabled in [
        "connectionTest",
        "approvalSubmit",
        // D3 §10 and §5.3 name these two surfaces and deliberately do not
        // serve them in v1. They are `false` for the LOCAL ADMINISTRATOR, who
        // holds every action there is, which is what makes them absent
        // capabilities rather than a role the console has not been given.
        "trustAdministration",
        "catalogWindowQuery",
    ] {
        assert_eq!(caps[disabled], false, "{disabled}");
    }
    let namespaces = app.get("/api/v1/namespaces").await.json();
    assert_eq!(namespaces["items"], v["namespaces"]);
    // Neither route asked Kubernetes anything: no core Namespace list.
    assert!(app.fake.requests().is_empty());
}
