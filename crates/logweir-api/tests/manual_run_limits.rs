//! P10 — the API half: one person cannot queue manual runs faster than
//! `rateLimits.*` per namespace per minute, and a queued run is published as
//! `queued` with the ceiling it waits behind.
//!
//! THE DEFECT, MEASURED. On the PoC install one operator's hundred
//! `POST …/backups` were all answered `201` within 2.4 s
//! (`final/scale/seed.log`), and every one became a runner pod. The controller
//! now bounds how many RUN at once (`weirkeeper::run_pool`); these rows hold
//! the rate at which one principal can START them, D0's `rate_limited` (429
//! plus `Retry-After`), keyed exactly as the discovery and preflight limits
//! are: `(actor, namespace, route)`.
//!
//! EVERY ROW ASSERTS THE CLUSTER, NOT ONLY THE STATUS CODE: a 429 that still
//! created the object would pass on status codes alone.

mod support;

use std::sync::Arc;

use serde_json::{json, Value};
use support::{FakeKube, Options, Role, SharedApp, SharedOptions, TestApp, TestClock, NS_A};

fn seed_schedule(fake: &FakeKube) {
    fake.seed(
        "backupschedules",
        NS_A,
        json!({
            "metadata": {"name": "nightly", "generation": 7},
            "spec": {
                "schedule": "0 2 * * *",
                "sourceRef": {"name": "source"},
                "topics": ["orders", "payments"],
                "archive": {"url": "s3://kafka-backups/logweir", "secretRef": {"name": "logweir-s3"}},
                "activeDeadlineSeconds": 3600,
                "concurrencyPolicy": "Forbid",
                "suspend": false
            }
        }),
    );
}

fn back_up_now() -> String {
    json!({"scheduleRef": {"name": "nightly"}}).to_string()
}

fn backups_path() -> String {
    format!("/api/v1/namespaces/{NS_A}/backups")
}

fn shared(fake: &FakeKube, clock: Arc<TestClock>) -> SharedApp {
    SharedApp::with_clock(
        fake.clone(),
        support::idp::MockIdp::new(support::ISSUER, &[]),
        SharedOptions {
            bindings: support::RoleBindings {
                revision: "p10-1".into(),
                bindings: vec![support::binding(Role::Operator, NS_A, &["lw-a-operators"])],
            },
            ..SharedOptions::default()
        },
        clock,
    )
}

fn retry_after(response: &support::TestResponse) -> u64 {
    response
        .header("retry-after")
        .expect("a 429 carries Retry-After")
        .parse()
        .expect("Retry-After is delta-seconds")
}

/// **"Back up now" is limited per person: ten a minute, then `429` with
/// `Retry-After`, and the eleventh creates nothing.** A second operator in the
/// same namespace is a different window and is still admitted; the first is
/// admitted again once the window has passed.
///
/// KILLS: no limit on the route at all (the P10 defect: all eleven `201`); a
/// limit keyed by namespace or route alone (the second operator would be
/// refused for the first one's clicks); a 429 without `Retry-After`; a refused
/// request that still reaches the cluster; a window that never resets; an
/// off-by-one that admits eleven.
#[tokio::test]
async fn back_up_now_is_limited_per_person_with_retry_after() {
    let fake = FakeKube::new();
    seed_schedule(&fake);
    let clock = TestClock::new();
    let app = shared(&fake, clock.clone());
    let alice = app.session_cookie("u-alice", &["lw-a-operators"]);
    let alice_csrf = app.csrf_for("u-alice");
    for i in 0..10 {
        let key = format!("alice-back-up-now-{i:04}");
        let created = app
            .post(
                &backups_path(),
                &alice,
                Some(&alice_csrf),
                Some(&key),
                &back_up_now(),
            )
            .await;
        assert_eq!(
            created.status.as_u16(),
            201,
            "create {i}: {}",
            created.text()
        );
    }
    assert_eq!(fake.count("backups", NS_A), 10);

    let limited = app
        .post(
            &backups_path(),
            &alice,
            Some(&alice_csrf),
            Some("alice-back-up-now-0010"),
            &back_up_now(),
        )
        .await;
    limited.assert_problem(429, "rate_limited");
    let wait = retry_after(&limited);
    assert!(
        (1..=60).contains(&wait),
        "Retry-After {wait} is within the window"
    );
    assert_eq!(
        fake.count("backups", NS_A),
        10,
        "the refused request created nothing"
    );

    // ANOTHER PERSON, THE SAME NAMESPACE: a different window.
    let bob = app.session_cookie("u-bob", &["lw-a-operators"]);
    let bob_csrf = app.csrf_for("u-bob");
    let other = app
        .post(
            &backups_path(),
            &bob,
            Some(&bob_csrf),
            Some("bob-back-up-now-0000"),
            &back_up_now(),
        )
        .await;
    assert_eq!(
        other.status.as_u16(),
        201,
        "the limit is per principal: {}",
        other.text()
    );

    // THE WINDOW PASSES.
    clock.advance(61);
    let again = app
        .post(
            &backups_path(),
            &alice,
            Some(&alice_csrf),
            Some("alice-back-up-now-0011"),
            &back_up_now(),
        )
        .await;
    assert_eq!(
        again.status.as_u16(),
        201,
        "a new window admits again: {}",
        again.text()
    );
    assert_eq!(fake.count("backups", NS_A), 12);
}

/// **A manual restore is limited the same way** (five a minute by default),
/// and the ceiling is the configured one: the same app with
/// `manualRestoresPerMinute: 2` refuses the third.
///
/// KILLS: leaving the restore route unlimited; a limiter that ignores its
/// configuration.
#[tokio::test]
async fn a_manual_restore_is_limited_and_the_ceiling_is_configurable() {
    let plan = support::golden_plan();
    for (limit, options) in [
        (5_usize, Options::default()),
        (
            2,
            Options {
                run_rate_limits: logweir_api::routes::RunRateLimits {
                    manual_backups_per_minute: 10,
                    manual_restores_per_minute: 2,
                },
                ..Options::default()
            },
        ),
    ] {
        let app = TestApp::with(FakeKube::new(), options);
        let path = format!("/api/v1/namespaces/{NS_A}/restores");
        for i in 0..limit {
            let mut body = support::restore_body(&plan);
            body["approvalRef"]["name"] = json!(format!("approval-{i:08}"));
            let created = app
                .post(
                    &path,
                    Some(&format!("restore-key-{i:06}")),
                    &body.to_string(),
                )
                .await;
            assert_eq!(
                created.status.as_u16(),
                201,
                "restore {i}: {}",
                created.text()
            );
        }
        let mut body = support::restore_body(&plan);
        body["approvalRef"]["name"] = json!("approval-overflow");
        let limited = app
            .post(&path, Some("restore-key-overflow"), &body.to_string())
            .await;
        limited.assert_problem(429, "rate_limited");
        assert!(retry_after(&limited) >= 1);
        assert_eq!(
            app.fake.count("restores", NS_A),
            limit,
            "exactly {limit} restores exist"
        );
    }
}

/// **A request refused for what it says does not spend the window.** Ten
/// malformed "Back up now" bodies (a run policy field beside `scheduleRef`,
/// D1 §8.2's `422`) and then ten good ones: all ten good ones are created.
///
/// KILLS: counting before validation, which would let a typo lock a person out
/// for a minute.
#[tokio::test]
async fn a_malformed_request_does_not_spend_the_window() {
    let app = TestApp::new();
    seed_schedule(&app.fake);
    for i in 0..10 {
        let bad = json!({"scheduleRef": {"name": "nightly"}, "topicSelection": {"topics": ["x"]}});
        let refused = app
            .post(
                &backups_path(),
                Some(&format!("bad-key-{i:06}")),
                &bad.to_string(),
            )
            .await;
        assert_eq!(refused.status.as_u16(), 422, "{}", refused.text());
    }
    for i in 0..10 {
        let created = app
            .post(
                &backups_path(),
                Some(&format!("good-key-{i:06}")),
                &back_up_now(),
            )
            .await;
        assert_eq!(created.status.as_u16(), 201, "good {i}: {}", created.text());
    }
    assert_eq!(app.fake.count("backups", NS_A), 10);
}

/// **A queued run is published as `queued`, with the ceiling it waits
/// behind** — in the list projection, the single read and the operation view
/// — and `status.queue` is NOT published for a run that is no longer queued.
///
/// KILLS: mapping `phase: Queued` to `unknown`/`UnrecognizedPhase` (what an
/// older API does); inventing the `Queued` STAGE (a Job with no pod) for a run
/// that has no Job; publishing a stale `queue` block beside a running run.
#[tokio::test]
async fn a_queued_run_is_published_as_queued_with_its_ceiling() {
    let app = TestApp::new();
    let queued = json!({
        "metadata": {"name": "logweir-manual-queued", "creationTimestamp": "2026-09-24T16:00:00Z"},
        "spec": {
            "sourceRef": {"name": "source"},
            "topics": ["orders"],
            "archive": {"url": "s3://kafka-backups/logweir", "secretRef": {"name": "logweir-s3"}},
            "triggeredBy": "manual",
            "trigger": {"kind": "Manual", "attempt": 0},
            "deadlineSeconds": 3600
        },
        "status": {
            "phase": "Queued",
            "queue": {"limit": 4},
            "conditions": [{
                "type": "Admitted", "status": "False", "reason": "ConcurrencyLimited",
                "message": "this manual Backup is queued: namespace team-a already has 4 manual Backup(s) active or ahead of it in line",
                "lastTransitionTime": "2026-09-24T16:00:01Z"
            }]
        }
    });
    app.fake.seed("backups", NS_A, queued.clone());
    let mut running = queued.clone();
    running["metadata"]["name"] = json!("logweir-manual-running");
    running["status"]["phase"] = json!("Running");
    running["status"]["jobRef"] = json!({"name": "logweir-manual-running"});
    app.fake.seed("backups", NS_A, running);

    let one = app
        .get(&format!("{}/logweir-manual-queued", backups_path()))
        .await;
    assert_eq!(one.status.as_u16(), 200, "{}", one.text());
    let item = one.json();
    let item = if item.get("item").is_some() {
        item["item"].clone()
    } else {
        item
    };
    assert_eq!(item["operation"]["state"], "queued", "{item}");
    assert_eq!(item["operation"]["stateReason"], "ConcurrencyLimited");
    assert_eq!(item["queue"], json!({"limit": 4}));

    let list = app.get(&backups_path()).await.json();
    let rows: Vec<&Value> = list["items"].as_array().expect("items").iter().collect();
    let running_row = rows
        .iter()
        .find(|r| r["name"] == "logweir-manual-running")
        .expect("the running row");
    assert!(
        running_row.get("queue").is_none(),
        "a stale queue block is not published beside a running run: {running_row}"
    );

    let view = app
        .get(&format!(
            "/api/v1/namespaces/{NS_A}/operations/backup/logweir-manual-queued"
        ))
        .await;
    assert_eq!(view.status.as_u16(), 200, "{}", view.text());
    let view = view.json();
    let view = if view.get("item").is_some() {
        view["item"].clone()
    } else {
        view
    };
    assert_eq!(view["state"], "queued", "{view}");
    assert_eq!(
        view["stage"], "admission",
        "no Job exists: this is not the Job-queued stage"
    );
    assert_eq!(view["terminal"], false);
}

/// **The window is the AUTHENTICATED PRINCIPAL's — `issuer#subject` — and
/// nothing else a client can vary** (review L4, mutant R3).
///
/// One person in two sessions (a second browser, a re-login) shares ONE
/// window: ten from the first session, and the eleventh — from the second —
/// is `429`. Two different people who happen to share a display name are TWO
/// windows: both get their ten.
///
/// KILLS: keying the window on the session id (a re-login would reset it);
/// keying it on the display name (a colleague with the same name would spend
/// your window, and you theirs).
#[tokio::test]
async fn the_window_is_keyed_on_the_subject_not_the_session_or_the_display_name() {
    let fake = FakeKube::new();
    seed_schedule(&fake);
    let app = shared(&fake, TestClock::new());

    // ONE PERSON, TWO SESSIONS.
    let first = app.session_cookie_named(
        "sid-a1".into(),
        "u-alice",
        "Alice",
        &["lw-a-operators"],
        900,
    );
    let second = app.session_cookie_named(
        "sid-a2".into(),
        "u-alice",
        "Alice",
        &["lw-a-operators"],
        900,
    );
    let first_csrf = app.keys.csrf_token("sid-a1");
    let second_csrf = app.keys.csrf_token("sid-a2");
    for i in 0..10 {
        let created = app
            .post(
                &backups_path(),
                &first,
                Some(&first_csrf),
                Some(&format!("alice-s1-{i:04}")),
                &back_up_now(),
            )
            .await;
        assert_eq!(
            created.status.as_u16(),
            201,
            "session 1, create {i}: {}",
            created.text()
        );
    }
    let relogin = app
        .post(
            &backups_path(),
            &second,
            Some(&second_csrf),
            Some("alice-s2-0000"),
            &back_up_now(),
        )
        .await;
    relogin.assert_problem(429, "rate_limited");

    // TWO PEOPLE, ONE DISPLAY NAME.
    let fake = FakeKube::new();
    seed_schedule(&fake);
    let app = shared(&fake, TestClock::new());
    for (subject, sid) in [("u-alex-1", "sid-x1"), ("u-alex-2", "sid-x2")] {
        let cookie =
            app.session_cookie_named(sid.into(), subject, "Alex", &["lw-a-operators"], 900);
        let csrf = app.keys.csrf_token(sid);
        for i in 0..10 {
            let created = app
                .post(
                    &backups_path(),
                    &cookie,
                    Some(&csrf),
                    Some(&format!("{subject}-{i:04}")),
                    &back_up_now(),
                )
                .await;
            assert_eq!(
                created.status.as_u16(),
                201,
                "{subject} create {i}: {}",
                created.text()
            );
        }
    }
    assert_eq!(fake.count("backups", NS_A), 20, "two people, two windows");
}

/// **A queued restore publishes its approval's deadline** (review M2): the
/// item's `queue.authorizationExpiresAt` is the object's own
/// `status.queue.authorizationExpiresAt`, copied.
#[tokio::test]
async fn a_queued_restore_publishes_its_approval_deadline() {
    let app = TestApp::new();
    let plan = support::golden_plan();
    let mut body = support::restore_body(&plan);
    body["approvalRef"]["name"] = json!("approval-queued1");
    let created = app
        .post(
            &format!("/api/v1/namespaces/{NS_A}/restores"),
            Some("restore-key-queued"),
            &body.to_string(),
        )
        .await;
    assert_eq!(created.status.as_u16(), 201, "{}", created.text());
    let name = created.json()["item"]["name"]
        .as_str()
        .expect("named")
        .to_string();
    let mut object = app.fake.object("restores", NS_A, &name).expect("stored");
    object["status"] = json!({
        "phase": "Queued",
        "reason": "ConcurrencyLimited",
        "queue": {"limit": 2, "authorizationExpiresAt": "2026-09-24T17:00:00Z"},
        "conditions": [{"type": "Admitted", "status": "False", "reason": "ConcurrencyLimited",
                        "lastTransitionTime": "2026-09-24T16:45:00Z"}]
    });
    app.fake.seed("restores", NS_A, object);
    let got = app
        .get(&format!("/api/v1/namespaces/{NS_A}/restores/{name}"))
        .await;
    assert_eq!(got.status.as_u16(), 200, "{}", got.text());
    let item = got.json();
    let item = if item.get("item").is_some() {
        item["item"].clone()
    } else {
        item
    };
    assert_eq!(item["operation"]["state"], "queued", "{item}");
    assert_eq!(
        item["queue"],
        json!({"limit": 2, "authorizationExpiresAt": "2026-09-24T17:00:00Z"})
    );
}
