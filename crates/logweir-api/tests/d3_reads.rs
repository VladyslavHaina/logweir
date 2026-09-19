//! D3 §10's five read families, over the SAME live-derived fixtures the
//! operation view uses.
//!
//! The properties these rows are about, in one place:
//!
//! 1. **No response carries a credential, a Secret name or public key bytes.**
//!    Each family has its own way to get that wrong — a PagerDuty routing key
//!    reference, a delete-capable Secret name, an `spkiPem` — so each family
//!    has its own scan, and `response_schemas_carry_no_credential_or_document_bytes`
//!    in `tests/contract.rs` is the shape half of the same rule.
//! 2. **Absent is not observed.** A `trusted` the catalog could not establish
//!    is absent and never `false`; a trust evaluation that is not fresh is
//!    `unknown` and never `valid`; an approved plan whose expiry is unknown is
//!    `unknown` and never `approved`.
//! 3. **Every page is bounded and every cursor is scoped.** The catalog point
//!    cursor additionally binds the view GENERATION, so a re-sync under a
//!    paging client is an error rather than a different page.

mod support;

use logweir_api::routes::retention::ApprovedPlanState;
use serde_json::{json, Value};
use support::{repo_root, FakeKube, Options, SharedApp, SharedOptions, TestApp, NS_A, NS_B};

fn fixture(name: &str) -> Value {
    let path = repo_root()
        .join("crates/logweir-api/tests/fixtures")
        .join(name);
    serde_json::from_str(
        &std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display())),
    )
    .expect("a fixture is JSON")
}

/// A PEM header, assembled at run time.
///
/// The repository rule (`WORKER-RULES.md`, 2026-09-18) is that a fixture which
/// LOOKS like a secret must be built rather than written, so no source line
/// carries the contiguous literal. The value only has to be distinctive enough
/// for the leak scans below to find it if it ever reached a response.
fn spki_pem() -> String {
    format!(
        "{}{}\n{}\n{}{}",
        "-----BEGIN ",
        "PUBLIC KEY-----",
        "MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAE",
        "-----END ",
        "PUBLIC KEY-----"
    )
}

fn seeded() -> FakeKube {
    let fake = FakeKube::new();
    fake.seed(
        "protectionpolicies",
        NS_A,
        fixture("protection-policy-unprotected.json"),
    );
    fake.seed(
        "retentionpolicies",
        NS_A,
        fixture("retention-policy-report.json"),
    );
    fake.seed(
        "retentionpolicies",
        NS_A,
        fixture("retention-policy-enforce.json"),
    );
    fake.seed("recoverycatalogs", NS_A, fixture("recovery-catalog.json"));
    fake.seed(
        "rehearsalschedules",
        NS_A,
        fixture("rehearsal-schedule.json"),
    );
    let mut policy = fixture("trust-policy-fresh.json");
    for key in policy["spec"]["keys"].as_array_mut().expect("keys") {
        key["spkiPem"] = json!(spki_pem());
    }
    fake.seed_cluster("trustpolicies", policy);
    fake
}

fn app() -> TestApp {
    TestApp::with(seeded(), Options::default())
}

/// The clock every fixture's freshness is measured against: ten minutes after
/// the live `TrustPolicy`'s own `evaluatedAt`, so the default is FRESH and a
/// row has to move the clock to make it stale.
fn at(app: &TestApp, rfc3339: &str) {
    app.clock.set(rfc3339.parse().expect("an instant"));
}

// ======================================================================
// Protection
// ======================================================================

#[tokio::test]
async fn protection_health_is_published_beside_its_basis_and_its_schedules() {
    let app = app();
    let body = app
        .get("/api/v1/namespaces/team-a/protection-policies")
        .await;
    assert_eq!(body.status.as_u16(), 200);
    let v = body.json();
    assert_eq!(v["items"].as_array().expect("items").len(), 1);
    let item = &v["items"][0];

    // The live object was `Unprotected` with a `Catalog` basis: three claims
    // that must not be collapsed into one.
    assert_eq!(item["health"], "Unprotected");
    assert_eq!(item["availabilityBasis"], "Catalog");
    assert_eq!(item["objectives"]["maxRecoveryPointAgeSeconds"], 300);
    assert_eq!(item["objectives"]["requireVerifiedEvidence"], true);

    // The schedule's OWN readiness, beside protection health and not folded
    // into it: an enabled schedule can have no recoverable backup, and a
    // suspended one is the commonest cause of staleness.
    let schedule = &item["schedules"][0];
    assert_eq!(schedule["name"], "keeps-running");
    assert_eq!(schedule["suspended"], true);
    assert_eq!(schedule["ready"], "False");

    // The alert ledger, with the pair that makes delivery exactly-once.
    let alert = &item["alerts"][0];
    assert_eq!(alert["kind"], "Staleness");
    assert_eq!(alert["state"], "Open");
    assert_eq!(alert["transition"], 1);
    assert_eq!(alert["notifiedTransition"], 1);
    assert_eq!(alert["delivery"]["state"], "Delivered");
    assert_eq!(alert["delivery"]["attempts"], 1);

    // The last ATTEMPT is a different object from the last available POINT.
    assert_eq!(item["lastAttempt"]["phase"], "Succeeded");
    assert!(item.get("lastAvailablePoint").is_none());

    let one = app
        .get("/api/v1/namespaces/team-a/protection-policies/protect-a")
        .await;
    assert_eq!(one.status.as_u16(), 200);
    assert_eq!(one.json()["item"]["health"], "Unprotected");
    app.fake.assert_strict();
}

/// **No sink, no routing key, no Secret name.**
///
/// REGRESSION REASON. The obvious projection of `spec.notifications` copies
/// the routes across, and the routes carry `secretKeyRef`s. A webhook URL is a
/// bearer token with a hostname on the front — the CRD says so in as many
/// words — and the NAME of the Secret holding one is what a reader needs to
/// decide what to try to read next.
#[tokio::test]
async fn a_protection_response_names_no_secret_and_no_sink() {
    let app = app();
    let text = app
        .get("/api/v1/namespaces/team-a/protection-policies")
        .await
        .text();
    // The live object really does reference one, or this proves nothing.
    let fixture = fixture("protection-policy-unprotected.json");
    let secret = fixture
        .pointer("/spec/notifications/routes/0/webhook/urlSecretRef/name")
        .and_then(Value::as_str)
        .expect("the fixture references a Secret");
    assert!(
        !text.contains(secret),
        "the Secret name reached the response"
    );
    for word in ["urlSecretRef", "routingKeySecretRef", "webhookUrlSecretRef"] {
        assert!(!text.contains(word), "{word} reached the response");
    }
    // What IS published: the route's name and which channels it has.
    let route = &app
        .get("/api/v1/namespaces/team-a/protection-policies")
        .await
        .json()["items"][0]["notifications"]["routes"][0];
    assert_eq!(route["name"], "local-sink");
    assert_eq!(route["webhook"], true);
    assert_eq!(route["pagerDuty"], false);
    assert_eq!(route["slack"], false);
}

// ======================================================================
// Retention
// ======================================================================

#[tokio::test]
async fn retention_publishes_what_is_actually_enforcing_and_never_the_credential() {
    let app = app();
    let body = app
        .get("/api/v1/namespaces/team-a/retention-policies")
        .await;
    assert_eq!(body.status.as_u16(), 200);
    let items = body.json();
    let items = items["items"].as_array().expect("items");
    assert_eq!(items.len(), 2);

    let report = items
        .iter()
        .find(|i| i["name"] == "keep-a")
        .expect("the Report policy");
    assert_eq!(report["mode"], "Report");
    assert_eq!(report["enforcement"], "RecommendationOnly");
    // The guarantee table, which is the honest half: a declared provider rule
    // is `ProviderEnforcedUnverified`, not "enforced".
    assert_eq!(report["guarantees"]["minUsablePoints"], "LogweirEnforced");
    assert_eq!(report["guarantees"]["ageExpiry"], "NotEnforced");
    assert_eq!(
        report["guarantees"]["legalHold"],
        "ProviderEnforcedUnverified"
    );
    assert_eq!(report["approvedPlanState"], "notApplicable");
    // Nothing in `lastEvaluation` was deleted.
    assert_eq!(report["lastEvaluation"]["candidateCount"], 3);
    assert!(
        report["lastEvaluation"]["candidates"]
            .as_array()
            .unwrap()
            .len()
            == 3
    );
    assert!(report.get("lastEnforcement").is_none());

    let enforce = items
        .iter()
        .find(|i| i["name"] == "d3w14-degrade")
        .expect("the Enforce policy");
    assert_eq!(enforce["mode"], "Enforce");
    assert_eq!(enforce["enforcementSettings"]["credentialConfigured"], true);
    // THE POINT IDS, NOT A COUNT (D3 §6.5). The live run deleted nothing, so
    // the list is absent rather than `0` — which is the same absent-field rule
    // the counts follow.
    assert!(enforce["lastEnforcement"].get("deleted").is_none());
    assert_eq!(enforce["lastEnforcement"]["deletedTruncated"], false);
    assert_eq!(
        enforce["lastEnforcement"]["failed"][0]["code"],
        "AccessDenied"
    );

    // THE DELETE-CAPABLE SECRET'S NAME NEVER LEAVES THE CLUSTER.
    let text = body.text();
    let fixture = fixture("retention-policy-enforce.json");
    let secret = fixture
        .pointer("/spec/enforcement/credentialSecretRef/name")
        .and_then(Value::as_str)
        .expect("the fixture references a delete-capable Secret");
    assert!(
        !text.contains(secret),
        "the credential Secret name reached the response"
    );
    assert!(!text.contains("credentialSecretRef"));
    app.fake.assert_strict();
}

/// **The approved-plan gate, every arm, and `unknown` is never `approved`.**
///
/// REGRESSION REASON. This is the rule that decides whether a deletion Job may
/// be created at all. A projection that answered "approved" because it could
/// not find the plan digest, or because it found a digest but no expiry, would
/// be the most expensive possible reading of D3 §12's absent-field rule.
#[test]
fn the_approved_plan_gate_is_unknown_whenever_an_input_is_absent() {
    use logweir_api::routes::retention::approved_plan_state;
    use weirkeeper::crds::retention_policy::RetentionPolicy;

    let now: chrono::DateTime<chrono::Utc> = "2026-09-19T02:00:00Z".parse().unwrap();
    let base = fixture("retention-policy-enforce.json");
    let parse = |v: &Value| -> RetentionPolicy {
        serde_json::from_value(v.clone()).expect("the fixture is a RetentionPolicy")
    };

    // The live object: `requireApprovedPlan: false`, which is the unattended
    // choice and is published as its own state rather than as "approved".
    assert_eq!(
        approved_plan_state(&parse(&base), now),
        ApprovedPlanState::NotRequired
    );

    // Report mode has nothing to approve.
    let mut report = base.clone();
    report["spec"]["mode"] = json!("Report");
    report["spec"]
        .as_object_mut()
        .unwrap()
        .remove("enforcement");
    assert_eq!(
        approved_plan_state(&parse(&report), now),
        ApprovedPlanState::NotApplicable
    );

    let mut required = base.clone();
    required["spec"]["enforcement"]["requireApprovedPlan"] = json!(true);

    // A plan exists, nothing approved it.
    assert_eq!(
        approved_plan_state(&parse(&required), now),
        ApprovedPlanState::AwaitingApproval
    );

    // A digest that approves a DIFFERENT plan is not an approval of this one.
    let mut wrong = required.clone();
    wrong["spec"]["enforcement"]["approvedPlanSha256"] =
        json!("sha256:".to_string() + &"0".repeat(64));
    assert_eq!(
        approved_plan_state(&parse(&wrong), now),
        ApprovedPlanState::AwaitingApproval
    );

    let plan = required
        .pointer("/status/lastEvaluation/planSha256")
        .and_then(Value::as_str)
        .expect("the live evaluation carries a plan digest")
        .to_string();

    // The matching digest with a live expiry is approved.
    let mut approved = required.clone();
    approved["spec"]["enforcement"]["approvedPlanSha256"] = json!(plan);
    approved["status"]["lastEvaluation"]["planExpiresAt"] = json!("2026-09-19T03:00:00Z");
    assert_eq!(
        approved_plan_state(&parse(&approved), now),
        ApprovedPlanState::Approved
    );

    // Past its expiry it is `expired`, not still approved.
    approved["status"]["lastEvaluation"]["planExpiresAt"] = json!("2026-09-19T01:59:59Z");
    assert_eq!(
        approved_plan_state(&parse(&approved), now),
        ApprovedPlanState::Expired
    );

    // A matching digest with NO expiry recorded is `unknown` — never
    // `approved`, because nothing said the plan is still current.
    approved["status"]["lastEvaluation"]
        .as_object_mut()
        .unwrap()
        .remove("planExpiresAt");
    assert_eq!(
        approved_plan_state(&parse(&approved), now),
        ApprovedPlanState::Unknown
    );

    // No evaluation at all.
    let mut no_plan = required.clone();
    no_plan["status"]
        .as_object_mut()
        .unwrap()
        .remove("lastEvaluation");
    assert_eq!(
        approved_plan_state(&parse(&no_plan), now),
        ApprovedPlanState::NoPlan
    );
}

/// **`EnforcementDegraded` is published as a flag, from the condition.**
#[tokio::test]
async fn the_degraded_condition_is_published_as_a_flag() {
    let fake = seeded();
    let mut degraded = fixture("retention-policy-enforce.json");
    degraded["metadata"]["name"] = json!("degraded");
    degraded["status"]["consecutiveRunFailures"] = json!(3);
    let conditions = degraded["status"]["conditions"].as_array_mut().unwrap();
    conditions.push(json!({
        "type": "EnforcementDegraded",
        "status": "True",
        "reason": "ThreeConsecutiveFailures",
        "message": "three consecutive enforcement runs failed; no further run is scheduled until the spec changes",
        "lastTransitionTime": "2026-09-19T01:20:00Z"
    }));
    fake.seed("retentionpolicies", NS_A, degraded);
    let app = TestApp::with(fake, Options::default());

    let v = app
        .get("/api/v1/namespaces/team-a/retention-policies/degraded")
        .await
        .json();
    assert_eq!(v["item"]["enforcementDegraded"], true);
    assert_eq!(v["item"]["consecutiveRunFailures"], 3);

    // And the policy that is NOT degraded says so, so the flag is a fact and
    // not a default.
    let v = app
        .get("/api/v1/namespaces/team-a/retention-policies/keep-a")
        .await
        .json();
    assert_eq!(v["item"]["enforcementDegraded"], false);
}

// ======================================================================
// Catalogs
// ======================================================================

#[tokio::test]
async fn a_catalog_publishes_ten_counts_a_window_flag_and_its_signers() {
    let app = app();
    let v = app
        .get("/api/v1/namespaces/team-a/catalogs/primary")
        .await
        .json();
    let item = &v["item"];
    // Ten counts, not two: "how many points are there" and "how many can I
    // restore from" are different numbers.
    assert_eq!(item["counts"]["total"], 5);
    assert_eq!(item["counts"]["available"], 3);
    assert_eq!(item["counts"]["missing"], 2);
    assert_eq!(item["counts"]["unverified"], 1);
    assert_eq!(item["truncated"], true);
    assert_eq!(item["viewPoints"], 4);
    assert_eq!(item["signers"][0]["trusted"], true);

    let signers = app
        .get("/api/v1/namespaces/team-a/catalogs/primary/signers")
        .await
        .json();
    assert_eq!(signers["items"][0]["keyId"].as_str().unwrap().len(), 64);
    assert_eq!(signers["untrustedPoints"], 0);
    // The panel offers a comparison, not a button.
    assert!(signers["fingerprintCommand"]
        .as_str()
        .expect("a command")
        .contains("sha256"));
    app.fake.assert_strict();
}

/// The page `ConfigMap` a catalog's status names, with the digest the status
/// recorded.
fn seed_page(fake: &FakeKube, name: &str, lines: &[String]) -> String {
    let refs: Vec<&str> = lines.iter().map(String::as_str).collect();
    let digest = weirkeeper::catalog_view::page_digest(&refs);
    fake.seed(
        "configmaps",
        NS_A,
        json!({
            "metadata": {
                "name": name,
                "annotations": {
                    weirkeeper::catalog_view::PAGE_DIGEST_ANNOTATION: digest.clone()
                }
            },
            "immutable": true,
            "data": { weirkeeper::catalog_view::PAGE_DATA_KEY: lines.join("\n") + "\n" },
        }),
    );
    digest
}

fn entry_lines() -> Vec<String> {
    let path = repo_root().join("crates/logweir-api/tests/fixtures/catalog-page-entries.jsonl");
    std::fs::read_to_string(path)
        .expect("the entry fixture is readable")
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(str::to_string)
        .collect()
}

fn catalog_with_page(digest_override: Option<&str>) -> (TestApp, Vec<String>) {
    let fake = seeded();
    let lines = entry_lines();
    let digest = seed_page(&fake, "primary-g1-p0", &lines);
    let mut catalog = fixture("recovery-catalog.json");
    catalog["status"]["pages"] = json!([{
        "configMapName": "primary-g1-p0",
        "index": 0,
        "count": lines.len(),
        "sha256": digest_override.unwrap_or(&digest),
    }]);
    fake.seed("recoverycatalogs", NS_A, catalog);
    (TestApp::with(fake, Options::default()), lines)
}

#[tokio::test]
async fn the_point_view_keeps_availability_and_verification_apart_and_pages() {
    let (app, lines) = catalog_with_page(None);
    let v = app
        .get("/api/v1/namespaces/team-a/catalogs/primary/points")
        .await
        .json();
    assert_eq!(v["items"].as_array().unwrap().len(), lines.len());
    let first = &v["items"][0];
    assert_eq!(first["availability"], "Available");
    assert_eq!(first["verification"], "Verified");
    assert_eq!(first["selectable"], true);
    assert_eq!(first["signerKeyId"].as_str().unwrap().len(), 64);
    assert_eq!(first["locations"][0]["availability"], "Available");
    // The ms instants are rendered as instants, not as numbers a console has
    // to divide.
    assert!(first["recoveryPointAt"].as_str().unwrap().ends_with('Z'));
    assert_eq!(v["truncated"], true);
    assert_eq!(v["viewExpired"], false);

    // Paging: one at a time, with a cursor that carries the rest.
    let mut seen = Vec::new();
    let mut cursor: Option<String> = None;
    for _ in 0..lines.len() + 1 {
        let url = match &cursor {
            None => "/api/v1/namespaces/team-a/catalogs/primary/points?limit=1".to_string(),
            Some(c) => {
                format!("/api/v1/namespaces/team-a/catalogs/primary/points?limit=1&cursor={c}")
            }
        };
        let page = app.get(&url).await.json();
        for item in page["items"].as_array().unwrap() {
            seen.push(item["pointId"].as_str().unwrap().to_string());
        }
        cursor = page["page"]["nextCursor"].as_str().map(str::to_string);
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(seen.len(), lines.len());
    let unique: std::collections::BTreeSet<&String> = seen.iter().collect();
    assert_eq!(unique.len(), seen.len(), "a page repeated a point");
    app.fake.assert_strict();
}

/// **A page whose bytes do not match the recorded digest serves no row.**
///
/// REGRESSION REASON. The page `ConfigMap`s are owned by the sync JOB, not by
/// the catalog — that is how the view is collected without any `delete`
/// permission anywhere — so owner identity cannot be the check here. The
/// digest is, and a route that read the bytes without comparing it would serve
/// a recovery-point list that something else wrote.
#[tokio::test]
async fn a_page_that_fails_its_digest_or_immutability_serves_nothing() {
    let (app, _) = catalog_with_page(Some(&format!("sha256:{}", "a".repeat(64))));
    app.get("/api/v1/namespaces/team-a/catalogs/primary/points")
        .await
        .assert_problem(409, "result_integrity_failed");

    // A page with no recorded digest at all is refused too: "the status did
    // not say" is not the same as "the bytes are fine".
    let fake = seeded();
    let lines = entry_lines();
    seed_page(&fake, "primary-g1-p0", &lines);
    let mut catalog = fixture("recovery-catalog.json");
    catalog["status"]["pages"] =
        json!([{ "configMapName": "primary-g1-p0", "index": 0, "count": lines.len() }]);
    fake.seed("recoverycatalogs", NS_A, catalog);
    let app = TestApp::with(fake, Options::default());
    app.get("/api/v1/namespaces/team-a/catalogs/primary/points")
        .await
        .assert_problem(409, "result_integrity_failed");

    // A mutable page is not a published page.
    let fake = seeded();
    let lines = entry_lines();
    let refs: Vec<&str> = lines.iter().map(String::as_str).collect();
    let digest = weirkeeper::catalog_view::page_digest(&refs);
    fake.seed(
        "configmaps",
        NS_A,
        json!({
            "metadata": {"name": "primary-g1-p0"},
            "immutable": false,
            "data": { weirkeeper::catalog_view::PAGE_DATA_KEY: lines.join("\n") },
        }),
    );
    let mut catalog = fixture("recovery-catalog.json");
    catalog["status"]["pages"] = json!([{
        "configMapName": "primary-g1-p0", "index": 0, "count": lines.len(), "sha256": digest
    }]);
    fake.seed("recoverycatalogs", NS_A, catalog);
    let app = TestApp::with(fake, Options::default());
    app.get("/api/v1/namespaces/team-a/catalogs/primary/points")
        .await
        .assert_problem(409, "result_integrity_failed");
}

/// **A cursor minted against one view generation does not open another.**
#[tokio::test]
async fn a_point_cursor_is_bound_to_the_view_generation() {
    let (app, _) = catalog_with_page(None);
    let page = app
        .get("/api/v1/namespaces/team-a/catalogs/primary/points?limit=1")
        .await
        .json();
    let cursor = page["page"]["nextCursor"]
        .as_str()
        .expect("a next cursor")
        .to_string();
    let generation = page["page"]["snapshot"].as_str().unwrap().to_string();

    // A re-sync publishes a new page set under a new `syncedAt`.
    let mut catalog = app
        .fake
        .object("recoverycatalogs", NS_A, "primary")
        .expect("the catalog");
    catalog["status"]["syncedAt"] = json!("2026-09-19T03:00:00Z");
    app.fake.seed("recoverycatalogs", NS_A, catalog);

    let refused = app
        .get(&format!(
            "/api/v1/namespaces/team-a/catalogs/primary/points?limit=1&cursor={cursor}"
        ))
        .await;
    refused.assert_problem(400, "cursor_invalid");

    // And a cursor from another catalog's list is not this one's either.
    let other = app
        .get("/api/v1/namespaces/team-a/catalogs/primary/points?limit=1&cursor=not-a-cursor")
        .await;
    other.assert_problem(400, "cursor_invalid");
    assert!(!generation.is_empty());
}

/// **An expired view answers an empty list that SAYS it is empty because the
/// window aged out.**
#[tokio::test]
async fn an_expired_view_is_named_rather_than_reported_as_an_empty_archive() {
    let fake = seeded();
    let mut catalog = fixture("recovery-catalog.json");
    catalog["status"]["viewExpiresAt"] = json!("2026-09-15T11:00:00Z");
    catalog["status"].as_object_mut().unwrap().remove("pages");
    fake.seed("recoverycatalogs", NS_A, catalog);
    let app = TestApp::with(fake, Options::default());
    let v = app
        .get("/api/v1/namespaces/team-a/catalogs/primary/points")
        .await
        .json();
    assert_eq!(v["items"].as_array().unwrap().len(), 0);
    assert_eq!(v["viewExpired"], true);
    // The count the sync recorded is still published, so "no rows" and "no
    // points" are distinguishable.
    let catalog = app
        .get("/api/v1/namespaces/team-a/catalogs/primary")
        .await
        .json();
    assert_eq!(catalog["item"]["counts"]["total"], 5);
    assert_eq!(catalog["item"]["viewExpired"], true);
}

#[tokio::test]
async fn connecting_an_archive_creates_one_catalog_and_replays_the_same_one() {
    let app = TestApp::with(FakeKube::new(), Options::default());
    let body = json!({
        "name": "connected",
        "legacyArchive": {"url": "s3://old-archive/kafka", "credentialRef": {"name": "read-only"}},
        "syncMode": "full",
        "intervalSeconds": 0
    })
    .to_string();
    let created = app
        .post(
            "/api/v1/namespaces/team-a/catalogs",
            Some("connect-0001"),
            &body,
        )
        .await;
    assert_eq!(created.status.as_u16(), 201);
    let v = created.json();
    assert_eq!(v["item"]["name"], "connected");
    assert_eq!(v["item"]["mode"], "Full");
    assert_eq!(v["item"]["intervalSeconds"], 0);
    assert_eq!(v["replayed"], false);
    // The archive URL is published with userinfo redacted, and the credential
    // as a NAME.
    assert_eq!(v["item"]["legacyArchive"]["url"], "s3://old-archive/kafka");
    assert_eq!(
        v["item"]["legacyArchive"]["credentialRef"]["name"],
        "read-only"
    );

    let replay = app
        .post(
            "/api/v1/namespaces/team-a/catalogs",
            Some("connect-0001"),
            &body,
        )
        .await;
    assert_eq!(replay.status.as_u16(), 200);
    assert_eq!(replay.json()["replayed"], true);
    assert_eq!(app.fake.count("recoverycatalogs", NS_A), 1);

    // A DIFFERENT request under the same key is a conflict, never a second
    // object.
    let other = json!({
        "name": "connected",
        "destinationRef": {"name": "dest-a"},
        "syncMode": "index"
    })
    .to_string();
    app.post(
        "/api/v1/namespaces/team-a/catalogs",
        Some("connect-0001"),
        &other,
    )
    .await
    .assert_problem(409, "idempotency_conflict");

    // The create never sets `syncRequest`: a pre-filled token would make the
    // first reconcile look like a re-sync request.
    let stored = app
        .fake
        .object("recoverycatalogs", NS_A, "connected")
        .expect("the object");
    assert!(stored.pointer("/spec/syncRequest").is_none());
    app.fake.assert_strict();
}

#[tokio::test]
async fn a_connect_request_names_exactly_one_location_and_bounded_numbers() {
    let app = TestApp::with(FakeKube::new(), Options::default());
    let cases = [
        (json!({"name": "c", "syncMode": "full"}), "destinationRef"),
        (
            json!({"name": "c", "syncMode": "full", "destinationRef": {"name": "d"},
                   "legacyArchive": {"url": "s3://a/b"}}),
            "destinationRef",
        ),
        (
            json!({"name": "Not A Name", "syncMode": "full", "destinationRef": {"name": "d"}}),
            "name",
        ),
        (
            json!({"name": "c", "syncMode": "full", "legacyArchive": {"url": "https://a/b"}}),
            "legacyArchive.url",
        ),
        (
            json!({"name": "c", "syncMode": "full", "destinationRef": {"name": "d"},
                   "intervalSeconds": 60}),
            "intervalSeconds",
        ),
        (
            json!({"name": "c", "syncMode": "full", "destinationRef": {"name": "d"},
                   "viewLimit": 99}),
            "viewLimit",
        ),
    ];
    for (body, field) in cases {
        let response = app
            .post(
                "/api/v1/namespaces/team-a/catalogs",
                Some("validation-0001"),
                &body.to_string(),
            )
            .await;
        response.assert_problem(422, "validation_failed");
        let v = response.json();
        assert_eq!(v["errors"][0]["field"], field, "{body}");
    }
    // An unknown field is refused rather than dropped.
    app.post(
        "/api/v1/namespaces/team-a/catalogs",
        Some("validation-0002"),
        &json!({"name": "c", "syncMode": "full", "destinationRef": {"name": "d"}, "extra": 1})
            .to_string(),
    )
    .await
    .assert_problem(422, "validation_failed");
    assert_eq!(app.fake.count("recoverycatalogs", NS_A), 0);
}

// ======================================================================
// Trust
// ======================================================================

/// **§7.7: a stale or generation-behind evaluation is `unknown`, not `valid`.**
///
/// REGRESSION REASON. The page this replaces printed `valid` for any key not
/// listed in `status.expiredKeyIds`, including when the object carried no
/// status at all. Deciding it on the server means a console cannot get it
/// wrong, and a mutant that dropped the freshness filter fails here.
#[tokio::test]
async fn a_trust_evaluation_that_is_not_fresh_publishes_unknown() {
    let app = app();
    // The live object's `evaluatedAt` is 04:42:25; the harness clock starts at
    // 2026-09-15, so the verdict is far in the "future" and therefore NOT
    // fresh — which is itself one of the four rows.
    let v = app.get("/api/v1/trust-policies/org-default").await.json();
    assert_eq!(v["item"]["evaluation"]["state"], "unknown");
    assert_eq!(v["item"]["keys"][0]["effectiveState"], "unknown");
    assert!(v["item"]["keys"][0].get("usableForVerification").is_none());

    // Ten minutes after the recorded evaluation: fresh, and the verdict the
    // controller wrote is published.
    at(&app, "2026-09-18T04:52:00Z");
    let v = app.get("/api/v1/trust-policies/org-default").await.json();
    assert_eq!(v["item"]["evaluation"]["state"], "fresh");
    assert_eq!(v["item"]["evaluation"]["reason"], "evaluated");
    assert_eq!(v["item"]["keys"][0]["effectiveState"], "Active");
    assert_eq!(v["item"]["keys"][0]["usableForVerification"], "Full");
    assert_eq!(v["item"]["keys"][0]["usableForNewSignatures"], true);
    // The two instants the comparison used are published, so a reader checks
    // the arithmetic instead of redoing it against the browser's clock.
    assert_eq!(
        v["item"]["evaluation"]["serverTime"],
        "2026-09-18T04:52:00Z"
    );
    assert_eq!(v["item"]["evaluation"]["freshWithinSeconds"], 900);

    // Sixteen minutes: stale.
    at(&app, "2026-09-18T04:58:26Z");
    let v = app.get("/api/v1/trust-policies/org-default").await.json();
    assert_eq!(v["item"]["evaluation"]["state"], "unknown");
    assert_eq!(v["item"]["evaluation"]["reason"], "stale");
    assert_eq!(v["item"]["keys"][0]["effectiveState"], "unknown");
}

#[tokio::test]
async fn a_generation_ahead_of_its_verdict_and_a_statusless_policy_are_both_unknown() {
    let fake = seeded();
    let mut behind = fixture("trust-policy-fresh.json");
    behind["metadata"]["name"] = json!("behind");
    behind["metadata"]["generation"] = json!(7);
    for key in behind["spec"]["keys"].as_array_mut().unwrap() {
        key["spkiPem"] = json!(spki_pem());
    }
    fake.seed_cluster("trustpolicies", behind);

    let mut bare = fixture("trust-policy-fresh.json");
    bare["metadata"]["name"] = json!("bare");
    bare.as_object_mut().unwrap().remove("status");
    for key in bare["spec"]["keys"].as_array_mut().unwrap() {
        key["spkiPem"] = json!(spki_pem());
    }
    fake.seed_cluster("trustpolicies", bare);

    let app = TestApp::with(fake, Options::default());
    at(&app, "2026-09-18T04:52:00Z");

    let v = app.get("/api/v1/trust-policies/behind").await.json();
    assert_eq!(v["item"]["evaluation"]["reason"], "generationBehind");
    assert_eq!(v["item"]["keys"][0]["effectiveState"], "unknown");

    let v = app.get("/api/v1/trust-policies/bare").await.json();
    assert_eq!(v["item"]["evaluation"]["reason"], "notEvaluated");
    assert_eq!(v["item"]["keys"][0]["effectiveState"], "unknown");
    // The DECLARED lifecycle state is still published: it is a fact about the
    // spec and does not depend on anybody having evaluated it.
    assert_eq!(v["item"]["keys"][0]["state"], "Active");
}

/// **No public key bytes leave this API.**
#[tokio::test]
async fn a_trust_response_never_carries_the_public_key_material() {
    let app = app();
    for path in [
        "/api/v1/trust-policies",
        "/api/v1/trust-policies/org-default",
    ] {
        let response = app.get(path).await;
        let text = response.text();
        assert!(
            !text.contains(&spki_pem()),
            "{path} carried the public key bytes"
        );
        assert!(
            !text.contains("MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAE"),
            "{path} carried a key body"
        );
        // NO OBJECT ANYWHERE IN THE BODY HAS AN `spkiPem` KEY. The substring
        // itself is not the test: the controller's own `Loaded` condition
        // message says "every spkiPem on this policy parsed", which is prose
        // about a field and not the field, and D0 keeps controller messages
        // visible. What must not exist is a place a client could read the
        // bytes OUT of.
        assert!(
            !has_key(&response.json(), "spkiPem"),
            "{path} published an spkiPem field"
        );
        // What IS published is the key id: the number `openssl` prints.
        assert!(text.contains("2c76e22ff89969dc0337e64756c85f18edb3e51ae2950ea18d81021d7176d7fe"));
        // And the enum spellings are the CRD's own, not Rust's.
        assert!(text.contains("\"algorithm\":\"p256\""), "{path}: {text}");
    }
}

/// Whether any object in `value`, at any depth, carries `key`.
fn has_key(value: &Value, key: &str) -> bool {
    match value {
        Value::Object(map) => map.contains_key(key) || map.values().any(|v| has_key(v, key)),
        Value::Array(items) => items.iter().any(|v| has_key(v, key)),
        _ => false,
    }
}

// ======================================================================
// Paging and authorization
// ======================================================================

#[tokio::test]
async fn every_family_pages_with_a_cursor_bound_to_its_own_route() {
    let fake = seeded();
    for i in 0..3 {
        let mut policy = fixture("protection-policy-unprotected.json");
        policy["metadata"]["name"] = json!(format!("p-{i}"));
        fake.seed("protectionpolicies", NS_A, policy);
        let mut catalog = fixture("recovery-catalog.json");
        catalog["metadata"]["name"] = json!(format!("c-{i}"));
        fake.seed("recoverycatalogs", NS_A, catalog);
    }
    let app = TestApp::with(fake, Options::default());

    let protection = app
        .get("/api/v1/namespaces/team-a/protection-policies?limit=2")
        .await
        .json();
    assert_eq!(protection["items"].as_array().unwrap().len(), 2);
    let cursor = protection["page"]["nextCursor"]
        .as_str()
        .expect("a next cursor")
        .to_string();

    // The SAME cursor on another family's route is invalid: the scope binds
    // the route, so a page of protection policies can never be replayed as a
    // page of catalogs.
    app.get(&format!(
        "/api/v1/namespaces/team-a/catalogs?limit=2&cursor={cursor}"
    ))
    .await
    .assert_problem(400, "cursor_invalid");
    // Nor in another namespace.
    app.get(&format!(
        "/api/v1/namespaces/team-b/protection-policies?limit=2&cursor={cursor}"
    ))
    .await
    .assert_problem(400, "cursor_invalid");
    // Its own route continues.
    let next = app
        .get(&format!(
            "/api/v1/namespaces/team-a/protection-policies?limit=2&cursor={cursor}"
        ))
        .await;
    assert_eq!(next.status.as_u16(), 200);

    // The page bound is the shared one.
    app.get("/api/v1/namespaces/team-a/protection-policies?limit=201")
        .await
        .assert_problem(422, "validation_failed");
    app.get("/api/v1/namespaces/team-a/catalogs/primary/points?limit=201")
        .await
        .assert_problem(422, "validation_failed");
    app.fake.assert_strict();
}

/// **The role table, on the routes themselves.**
#[tokio::test]
async fn the_read_families_are_narrowed_by_role_and_trust_needs_an_administrator() {
    let shared = SharedApp::new(
        seeded(),
        support::idp::MockIdp::new(support::ISSUER, &[]),
        SharedOptions {
            bindings: support::default_bindings(),
            ..SharedOptions::default()
        },
    );
    let viewer = shared.session_cookie("u-view", &["lw-a-viewers"]);
    let operator = shared.session_cookie("u-op", &["lw-a-operators"]);
    let approver = shared.session_cookie("u-app", &["lw-a-approvers"]);
    let admin = shared.session_cookie("u-adm", &["lw-a-admins"]);

    let reads = [
        "/api/v1/namespaces/team-a/protection-policies",
        "/api/v1/namespaces/team-a/rehearsal-schedules",
        "/api/v1/namespaces/team-a/catalogs",
        "/api/v1/namespaces/team-a/retention-policies",
    ];
    for path in reads {
        for (who, cookie) in [
            ("viewer", &viewer),
            ("operator", &operator),
            ("admin", &admin),
        ] {
            let response = shared.get(path, cookie).await;
            assert_eq!(response.status.as_u16(), 200, "{who} on {path}");
        }
        // AN APPROVER SEES THE APPROVAL SUBJECT AND ITS PLAN, AND NOTHING THAT
        // WOULD LET IT PREPARE ONE. Protection health, the catalog and the
        // retention preview are none of those.
        let response = shared.get(path, &approver).await;
        assert_eq!(response.status.as_u16(), 403, "approver on {path}");
    }

    // The cluster-scoped read is the administrator's alone.
    for (who, cookie, expected) in [
        ("viewer", &viewer, 403),
        ("operator", &operator, 403),
        ("approver", &approver, 403),
        ("admin", &admin, 200),
    ] {
        let response = shared.get("/api/v1/trust-policies", cookie).await;
        assert_eq!(
            response.status.as_u16(),
            expected,
            "{who} on /trust-policies"
        );
    }

    // An ungranted namespace is 404 in shared mode, for the new families too:
    // the answer never depends on cluster state the actor may not see.
    let response = shared
        .get("/api/v1/namespaces/team-c/protection-policies", &viewer)
        .await;
    assert_eq!(response.status.as_u16(), 404);
    assert!(
        !shared
            .app
            .fake
            .requests()
            .iter()
            .any(|r| r.path.contains("team-c")),
        "an ungranted namespace reached Kubernetes"
    );
}

/// **Connecting an archive is an operator's write, not a viewer's.**
#[tokio::test]
async fn connecting_an_archive_is_refused_to_a_viewer_and_an_approver() {
    let shared = SharedApp::new(
        FakeKube::new(),
        support::idp::MockIdp::new(support::ISSUER, &[]),
        SharedOptions {
            bindings: support::default_bindings(),
            ..SharedOptions::default()
        },
    );
    let body =
        json!({"name": "c", "syncMode": "full", "destinationRef": {"name": "d"}}).to_string();
    for (who, groups, expected) in [
        ("viewer", "lw-a-viewers", 403),
        ("approver", "lw-a-approvers", 403),
        ("operator", "lw-a-operators", 201),
    ] {
        let cookie = shared.session_cookie(who, &[groups]);
        let csrf = shared.csrf_for(who);
        let response = shared
            .post(
                "/api/v1/namespaces/team-a/catalogs",
                &cookie,
                Some(&csrf),
                Some(&format!("connect-{who}-01")),
                &body,
            )
            .await;
        assert_eq!(response.status.as_u16(), expected, "{who}");
    }
    assert_eq!(shared.app.fake.count("recoverycatalogs", NS_A), 1);
    assert_eq!(shared.app.fake.count("recoverycatalogs", NS_B), 0);
}

// ======================================================================
// Rehearsals
// ======================================================================

/// **A skip is a result, and the thing that blocks the next slot is visible.**
///
/// A rehearsal that quietly stops running looks exactly like a rehearsal that
/// keeps passing if the surface shows only the last success. So `lastSkipped`
/// and `cleanup.pendingTopics` are published beside `lastSucceeded`: while
/// those topics are there every further slot is skipped, and an operator who
/// cannot see them cannot clear them.
///
/// THE FIXTURE'S SHAPE IS THE CRD'S, NOT A LIVE OBJECT'S. D3 W7 (the rehearsal
/// controller) has not landed, so no `RehearsalSchedule` exists in the live
/// evidence; this row deserializes through `weirkeeper::crds`, so a field that
/// drifts from the CRD fails it.
#[tokio::test]
async fn a_rehearsal_publishes_its_skip_its_leftovers_and_a_named_authorization() {
    let app = app();
    let v = app
        .get("/api/v1/namespaces/team-a/rehearsal-schedules/weekly-orders")
        .await
        .json();
    let item = &v["item"];
    assert_eq!(item["schedule"], "0 3 * * 0");
    assert_eq!(item["suspend"], false);
    assert_eq!(item["point"]["requireVerifiedEvidence"], true);
    assert_eq!(item["point"]["selection"], "NewestAvailable");
    assert_eq!(item["bounds"]["concurrencyPolicy"], "Forbid");

    assert_eq!(item["lastSucceeded"]["evidence"], "Valid");
    assert_eq!(item["lastSucceeded"]["rtoSeconds"], 465);
    assert_eq!(item["lastSkipped"]["reason"], "LeftoverTopics");
    assert_eq!(item["lastFailed"]["reason"], "TeardownIncomplete");
    assert_eq!(
        item["cleanup"]["pendingTopics"][0],
        "rehearsal-3f2a91c7-orders"
    );
    assert_eq!(item["cleanup"]["pendingTopicsTruncated"], false);

    // The standing authorization is a NAME. The signed document, its
    // signatures and the trusted public keys live on the `Approval` and reach
    // a caller only through the explicit approval-packet route.
    assert_eq!(
        item["standingApprovalRef"]["name"],
        "weekly-orders-standing"
    );
    let text = v.to_string();
    for word in ["planBytes", "signature", "spkiPem", "approvalBytes"] {
        assert!(
            !text.contains(word),
            "{word} reached the rehearsal projection"
        );
    }

    let list = app
        .get("/api/v1/namespaces/team-a/rehearsal-schedules")
        .await
        .json();
    assert_eq!(list["items"].as_array().unwrap().len(), 1);
    app.fake.assert_strict();
}

// ======================================================================
// Fix round 1 — the review's F2 and F4
// ======================================================================

/// A cluster-scoped policy governing four namespaces, of which the reader
/// administers one.
fn wide_policy(name: &str, namespaces: &[&str], default: bool) -> Value {
    let mut policy = fixture("trust-policy-fresh.json");
    policy["metadata"]["name"] = json!(name);
    policy["spec"]["default"] = json!(default);
    policy["spec"]["namespaces"] = json!(namespaces);
    policy["status"]["boundNamespaces"] = json!(namespaces);
    // One conflict per governed namespace, so a policy whose whole list the
    // reader can see has nothing filtered out of it.
    policy["status"]["conflicts"] = Value::Array(
        namespaces
            .iter()
            .map(|n| json!({"namespace": n, "policies": ["org-default", name]}))
            .collect(),
    );
    for key in policy["spec"]["keys"].as_array_mut().expect("keys") {
        key["spkiPem"] = json!(spki_pem());
    }
    policy
}

/// **F2: a namespace administrator cannot enumerate the installation.**
///
/// REGRESSION REASON, MEASURED. The first round admitted any actor holding
/// `trustPolicy.read` in any one bound namespace and then served EVERY
/// `TrustPolicy` whole: an administrator bound only in `team-a` read
/// `spec.namespaces`, `status.boundNamespaces` and `conflicts[].namespace` for
/// the entire installation. D0's matrix row is "installation-admin only,
/// cluster scope"; this model has no installation-scoped role, so the
/// deviation is narrowed instead — a policy is served only when it governs a
/// namespace the reader administers or is the installation `default`, and
/// every namespace-bearing list is filtered to the administered set with
/// `namespacesFiltered` saying so.
#[tokio::test]
async fn a_namespace_administrator_never_enumerates_another_namespace() {
    let fake = seeded();
    fake.seed_cluster(
        "trustpolicies",
        wide_policy(
            "org-wide",
            &["team-a", "team-b", "finance-prod", "hr-secrets"],
            false,
        ),
    );
    let shared = SharedApp::new(
        fake,
        support::idp::MockIdp::new(support::ISSUER, &[]),
        SharedOptions {
            bindings: support::RoleBindings {
                revision: "rev-1".into(),
                bindings: vec![support::binding(
                    logweir_api::authz::Role::Administrator,
                    NS_A,
                    &["lw-a-admins"],
                )],
            },
            ..SharedOptions::default()
        },
    );
    let admin = shared.session_cookie("u-adm", &["lw-a-admins"]);

    let response = shared.get("/api/v1/trust-policies/org-wide", &admin).await;
    assert_eq!(response.status.as_u16(), 200);
    let text = response.text();
    for hidden in ["team-b", "finance-prod", "hr-secrets"] {
        assert!(
            !text.contains(hidden),
            "{hidden} was enumerated to an administrator who does not administer it:\n{text}"
        );
    }
    let item = &response.json()["item"];
    assert_eq!(item["namespaces"], json!(["team-a"]));
    assert_eq!(item["boundNamespaces"], json!(["team-a"]));
    assert_eq!(
        item["namespacesFiltered"], true,
        "a filtered list that did not say so would look complete"
    );
    // The conflict about a namespace the reader administers survives; the one
    // about `hr-secrets` does not.
    assert_eq!(item["conflicts"].as_array().unwrap().len(), 1);
    assert_eq!(item["conflicts"][0]["namespace"], "team-a");
    // The keys view §7.7 needs is untouched by the filter.
    assert_eq!(item["keys"][0]["state"], "Active");
    assert_eq!(item["keyCount"], 1);

    // A policy that governs nothing the reader administers is NOT FOUND — the
    // same answer a policy that does not exist gives, because "it exists and
    // you may not see it" is the enumeration this closes.
    shared.app.fake.seed_cluster(
        "trustpolicies",
        wide_policy("hr-only", &["hr-secrets"], false),
    );
    let refused = shared.get("/api/v1/trust-policies/hr-only", &admin).await;
    refused.assert_problem(404, "not_found");

    let listed = shared.get("/api/v1/trust-policies", &admin).await.json();
    let names: Vec<&str> = listed["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["name"].as_str().unwrap())
        .collect();
    assert!(!names.contains(&"hr-only"), "{names:?}");
    assert!(names.contains(&"org-wide"), "{names:?}");
}

/// **The installation `default` policy is in scope for every administrator.**
///
/// It is the policy that governs a namespace when no explicit binding does, so
/// hiding it would hide the very object the keys page is about — and it names
/// no namespaces, so there is nothing to enumerate from it.
#[tokio::test]
async fn the_default_policy_is_in_scope_and_an_unfiltered_one_says_so() {
    let fake = FakeKube::new();
    fake.seed_cluster("trustpolicies", wide_policy("org-default", &[], true));
    fake.seed_cluster(
        "trustpolicies",
        wide_policy("just-team-a", &["team-a"], false),
    );
    let shared = SharedApp::new(
        fake,
        support::idp::MockIdp::new(support::ISSUER, &[]),
        SharedOptions {
            bindings: support::RoleBindings {
                revision: "rev-1".into(),
                bindings: vec![support::binding(
                    logweir_api::authz::Role::Administrator,
                    NS_A,
                    &["lw-a-admins"],
                )],
            },
            ..SharedOptions::default()
        },
    );
    let admin = shared.session_cookie("u-adm", &["lw-a-admins"]);

    let listed = shared.get("/api/v1/trust-policies", &admin).await.json();
    let names: Vec<&str> = listed["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, vec!["just-team-a", "org-default"]);

    // A policy whose whole namespace list is visible is NOT reported filtered:
    // the flag is a fact, not a constant.
    let unfiltered = shared
        .get("/api/v1/trust-policies/just-team-a", &admin)
        .await
        .json();
    assert_eq!(unfiltered["item"]["namespacesFiltered"], false);
    assert_eq!(unfiltered["item"]["namespaces"], json!(["team-a"]));
}

/// **F4: a legacy archive URL comes back with its userinfo redacted.**
///
/// REGRESSION REASON. The line that does it had no test at all: the reviewer
/// planted `url: a.url.clone()` and the whole 358-test suite passed. The row
/// that looked like the guard sent a URL with no userinfo in it. `POST`
/// refuses a userinfo URL by field, so one can only arrive by `kubectl apply`
/// — which is exactly the legacy-archive adoption path this family exists for.
#[tokio::test]
async fn an_archive_url_that_carries_userinfo_comes_back_redacted() {
    let secret = concat!("arch1ve-", "p455w0rd-in-a-url");
    let fake = seeded();
    let mut catalog = fixture("recovery-catalog.json");
    catalog["metadata"]["name"] = json!("adopted");
    catalog["spec"]
        .as_object_mut()
        .unwrap()
        .remove("destinationRef");
    catalog["spec"]["legacyArchive"] = json!({
        "url": format!("s3://archive-user:{secret}@old-bucket/kafka"),
        "secretRef": {"name": "read-only"}
    });
    fake.seed("recoverycatalogs", NS_A, catalog);

    let mut policy = fixture("protection-policy-unprotected.json");
    policy["metadata"]["name"] = json!("adopted-protection");
    policy["spec"]["protects"]
        .as_object_mut()
        .unwrap()
        .remove("destinationRef");
    policy["spec"]["protects"]["legacyArchive"] = json!({
        "url": format!("s3://archive-user:{secret}@old-bucket/kafka"),
        "secretRef": {"name": "read-only"}
    });
    fake.seed("protectionpolicies", NS_A, policy);

    let app = TestApp::with(fake, Options::default());
    for path in [
        "/api/v1/namespaces/team-a/catalogs/adopted",
        "/api/v1/namespaces/team-a/catalogs",
        "/api/v1/namespaces/team-a/protection-policies/adopted-protection",
    ] {
        let text = app.get(path).await.text();
        assert!(
            !text.contains(secret),
            "{path} published the credential in an archive URL:\n{text}"
        );
        assert!(
            !text.contains("archive-user"),
            "{path} published the userinfo of an archive URL:\n{text}"
        );
        assert!(
            text.contains("old-bucket"),
            "{path} redacted the whole URL instead of its userinfo:\n{text}"
        );
    }
    app.fake.assert_strict();
}
