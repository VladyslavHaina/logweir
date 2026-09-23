//! `POST /api/v1/namespaces/{ns}/backups` (PLAT-06.2, D1 §8).
//!
//! THE SIX ROWS D1 §12 REQUIRES, and the API half of each: double click, lost
//! HTTP response, refresh, paused schedule, failed preflight, and a successful
//! scheduled-policy copy. The browser halves are W7's `ui/tests/*.spec.js`;
//! what is asserted here is that the API makes each of them POSSIBLE — one
//! object per intent, a replay that returns the original run, labels a reload
//! can select on, no gate on suspension or readiness, and a copied policy whose
//! digest the controller will accept.
//!
//! EVERY TEST ASSERTS THE CLUSTER, NOT ONLY THE STATUS CODE. "Exactly one run"
//! is a statement about `Backup` objects; a route that answered 200 twice and
//! created two archives would pass on status codes alone.

mod support;

use std::collections::BTreeSet;

use serde_json::{json, Value};
use support::{FakeKube, TestApp, NS_A};

const KEY: &str = "back-up-now-0001";

/// A schedule the console would offer a "Back up now" button for.
fn seed_schedule(fake: &FakeKube, name: &str, suspend: bool) -> Value {
    fake.seed(
        "backupschedules",
        NS_A,
        json!({
            "metadata": {"name": name, "generation": 7},
            "spec": {
                "schedule": "0 2 * * *",
                "timeZone": "Europe/Berlin",
                "sourceRef": {"name": "source"},
                "topics": ["orders", "payments"],
                "archive": {"url": "s3://kafka-backups/logweir", "secretRef": {"name": "logweir-s3"}},
                "activeDeadlineSeconds": 3600,
                "concurrencyPolicy": "Forbid",
                "suspend": suspend
            },
            "status": {
                "observedGeneration": 7,
                "policy": {
                    "generation": 7,
                    "runPolicySha256": schedule_digest(),
                    "timeZone": "Europe/Berlin",
                    "tzdb": weirkeeper::cadence::TZDB_SOURCE,
                    "effectiveSince": "2026-09-15T10:00:00Z",
                    "evaluatedAt": "2026-09-15T10:00:00Z"
                },
                "activeRuns": [
                    {"name": "logweir-backup-nightly-20260915-020000", "kind": "Scheduled", "attempt": 0}
                ]
            }
        }),
    )
}

/// The digest the CONTROLLER would record for the seeded schedule, computed by
/// the controller's own builder. Nothing in the API is allowed to disagree
/// with it.
fn schedule_digest() -> String {
    let spec: weirkeeper::crds::backup_schedule::BackupScheduleSpec =
        serde_json::from_value(json!({
            "schedule": "0 2 * * *",
            "timeZone": "Europe/Berlin",
            "sourceRef": {"name": "source"},
            "topics": ["orders", "payments"],
            "archive": {"url": "s3://kafka-backups/logweir", "secretRef": {"name": "logweir-s3"}},
            "activeDeadlineSeconds": 3600,
            "concurrencyPolicy": "Forbid",
            "suspend": false
        }))
        .expect("the seed is a BackupScheduleSpec");
    weirkeeper::controllers::backup_schedule::run_policy_digest(&spec)
}

fn from_schedule() -> String {
    json!({"scheduleRef": {"name": "nightly"}}).to_string()
}

fn backups(app: &TestApp) -> Vec<Value> {
    app.fake
        .requests()
        .iter()
        .filter(|r| r.method == "POST" && r.path.ends_with("/backups"))
        .map(|r| serde_json::from_str(&r.body).expect("a POST body is JSON"))
        .collect()
}

/// **The answer NAMES the run it made** — PLAT-18.2's live pass, whose console
/// success line read "Created Backup  (uid )".
///
/// The run's identity is in the answer and always was: `item` is the created
/// object's projection (`name`, `namespace`, `uid`, `resourceVersion`), in the
/// SAME `{requestId, replayed, item}` envelope every other create route answers
/// with (`RestoreResponse`, `DestinationResponse`, the schedule create …), plus
/// the one route-specific member `schedule`. The empty line was the console
/// reading `result.object.metadata` where its own client returns `result.run`;
/// that is a `ui/` fix, and this row is what keeps the API half of the
/// agreement from drifting while it is made.
///
/// So it holds the identity to the OBJECT THE CLUSTER STORED, on the create and
/// on the replay, and holds the envelope's member set.
///
/// KILLS: answering with the object as BUILT rather than as created (no uid, no
/// resourceVersion — the create's return value dropped); a replay that answers
/// with a fresh build; renaming or wrapping `item` so the envelope no longer
/// matches the other creates.
#[tokio::test]
async fn the_answer_names_the_created_run_in_the_create_envelope() {
    let app = TestApp::new();
    seed_schedule(&app.fake, "nightly", false);
    for (attempt, expected_status) in [("create", 201), ("replay", 200)] {
        let answer = app
            .post(
                &format!("/api/v1/namespaces/{NS_A}/backups"),
                Some(KEY),
                &from_schedule(),
            )
            .await;
        assert_eq!(
            answer.status,
            expected_status,
            "{attempt}: {}",
            answer.text()
        );
        let body = answer.json();
        let members: BTreeSet<&str> = body
            .as_object()
            .expect("an object")
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(
            members,
            BTreeSet::from(["requestId", "replayed", "item", "schedule"]),
            "{attempt}: the create envelope every create route answers with, plus `schedule`"
        );
        let item = &body["item"];
        let name = item["name"].as_str().unwrap_or_default();
        let stored = app
            .fake
            .object("backups", NS_A, name)
            .unwrap_or_else(|| panic!("{attempt}: the answer names `{name}`, which is not stored"));
        for (field, want) in [
            ("name", &stored["metadata"]["name"]),
            ("namespace", &stored["metadata"]["namespace"]),
            ("uid", &stored["metadata"]["uid"]),
            ("resourceVersion", &stored["metadata"]["resourceVersion"]),
        ] {
            let got = item[field].as_str().unwrap_or_default();
            assert!(
                !got.is_empty() && Some(got) == want.as_str(),
                "{attempt}: `item.{field}` is `{got}` and the stored object's is {want}. A \
                 console names the run from this answer, and a run it cannot name is one a \
                 person cannot follow"
            );
        }
    }
    assert_eq!(app.fake.count("backups", NS_A), 1);
    app.fake.assert_strict();
}

/// **`same_key_same_body_returns_200_and_uid`** (D1 §12, double click).
///
/// Two clicks 40 ms apart send the same key and the same body. The second is
/// `200`, carries `replayed: true`, and names the SAME object: one intent, one
/// run, one archive.
///
/// KILLS: a random name (two runs, two archives, double the broker load and
/// two receipts for one intention); reading process memory rather than the
/// cluster (a second replica would answer differently); answering 201 twice.
#[tokio::test]
async fn same_key_same_body_returns_200_and_uid() {
    let app = TestApp::new();
    seed_schedule(&app.fake, "nightly", false);

    let first = app
        .post(
            &format!("/api/v1/namespaces/{NS_A}/backups"),
            Some(KEY),
            &from_schedule(),
        )
        .await;
    assert_eq!(first.status, 201, "{}", first.text());
    let created = first.json();
    assert_eq!(created["replayed"], false);
    let name = created["item"]["name"].as_str().unwrap().to_string();
    let uid = created["item"]["uid"].as_str().unwrap().to_string();
    assert!(
        name.starts_with("logweir-manual-") && name.len() == 41,
        "D1 §8.1's deterministic name: {name}"
    );

    let second = app
        .post(
            &format!("/api/v1/namespaces/{NS_A}/backups"),
            Some(KEY),
            &from_schedule(),
        )
        .await;
    assert_eq!(second.status, 200, "{}", second.text());
    assert_eq!(second.json()["replayed"], true);
    assert_eq!(second.json()["item"]["uid"], uid);
    assert_eq!(second.json()["item"]["name"], name);
    assert_eq!(app.fake.count("backups", NS_A), 1, "two clicks, two runs");
    app.fake.assert_strict();
}

/// **The lost response replays the ORIGINAL run, even after the schedule was
/// edited in between** (D1 §12, lost HTTP response).
///
/// The request hash covers the body as sent, and the object's name covers the
/// key — so "check status" after a dropped `201` finds the run that was
/// created, not a new one under today's policy. A different body under the
/// same key is `409 idempotency_conflict`, because a key identifies an
/// intention and not a slot to overwrite.
///
/// KILLS: re-reading the schedule on replay and answering with a fresh copy;
/// keying the replay on the schedule's generation; adopting an object this
/// scope did not create.
#[tokio::test]
async fn a_lost_response_replays_the_original_run_after_an_edit() {
    let app = TestApp::new();
    seed_schedule(&app.fake, "nightly", false);
    // The response never reached the client: the object exists all the same.
    let first = app
        .post(
            &format!("/api/v1/namespaces/{NS_A}/backups"),
            Some(KEY),
            &from_schedule(),
        )
        .await;
    let uid = first.json()["item"]["uid"].as_str().unwrap().to_string();
    let original = first.json()["item"]["topics"].clone();
    assert_eq!(original, json!(["orders", "payments"]));

    // Somebody edits the schedule. The replay must not pick that up.
    app.put(
        &format!("/api/v1/namespaces/{NS_A}/schedules/nightly"),
        &json!({
            "expectedGeneration": 7,
            "schedule": "0 2 * * *",
            "topicSelection": {"topics": ["orders"]},
            "archive": {"url": "s3://kafka-backups/logweir", "credentialRef": {"name": "logweir-s3"}},
            "suspended": false
        })
        .to_string(),
    )
    .await;

    let replay = app
        .post(
            &format!("/api/v1/namespaces/{NS_A}/backups"),
            Some(KEY),
            &from_schedule(),
        )
        .await;
    assert_eq!(replay.status, 200);
    assert_eq!(replay.json()["item"]["uid"], uid);
    assert_eq!(replay.json()["item"]["topics"], original);
    assert_eq!(app.fake.count("backups", NS_A), 1);

    // Same key, different body: a conflict, and still one run.
    let conflict = app
        .post(
            &format!("/api/v1/namespaces/{NS_A}/backups"),
            Some(KEY),
            &json!({"scheduleRef": {"name": "nightly", "expectedGeneration": 8}}).to_string(),
        )
        .await;
    conflict.assert_problem(409, "idempotency_conflict");
    assert_eq!(app.fake.count("backups", NS_A), 1);

    // A NEW key is a new, deliberate run.
    let deliberate = app
        .post(
            &format!("/api/v1/namespaces/{NS_A}/backups"),
            Some("back-up-now-0002"),
            &from_schedule(),
        )
        .await;
    assert_eq!(deliberate.status, 201);
    assert_ne!(deliberate.json()["item"]["uid"], uid);
    assert_eq!(app.fake.count("backups", NS_A), 2);
    app.fake.assert_strict();
}

/// **After a reload the run is findable by label, without the browser's key**
/// (D1 §12, refresh).
///
/// D1 §8.5's "after refresh" row: the in-memory intent is gone, so the card
/// lists this schedule's recent manual runs instead — which is only possible
/// if the run carries the labels to select on and the list route honours the
/// selector. A second deliberate click is then a second run with a different
/// name, which is the right answer: the person meant it.
///
/// KILLS: dropping `logweir.dev/trigger` or `logweir.dev/schedule-uid` from
/// the created object; putting membership in a label the list cannot filter on.
#[tokio::test]
async fn a_reload_can_find_the_run_by_label() {
    let app = TestApp::new();
    let schedule = seed_schedule(&app.fake, "nightly", false);
    let uid = schedule["metadata"]["uid"].as_str().unwrap().to_string();
    app.post(
        &format!("/api/v1/namespaces/{NS_A}/backups"),
        Some(KEY),
        &from_schedule(),
    )
    .await;

    let posted = &backups(&app)[0];
    assert_eq!(
        posted["metadata"]["labels"]["logweir.dev/trigger"],
        "manual"
    );
    assert_eq!(posted["metadata"]["labels"]["logweir.dev/attempt"], "0");
    assert_eq!(
        posted["metadata"]["labels"]["logweir.dev/schedule"],
        "nightly"
    );
    assert_eq!(
        posted["metadata"]["labels"]["logweir.dev/schedule-uid"],
        uid
    );

    let listed = app
        .get(&format!(
            "/api/v1/namespaces/{NS_A}/backups?labelSelector=logweir.dev/trigger%3Dmanual,logweir.dev/schedule-uid%3D{uid}"
        ))
        .await
        .json();
    assert_eq!(listed["items"].as_array().unwrap().len(), 1);
    assert_eq!(listed["items"][0]["trigger"]["kind"], "Manual");
    assert_eq!(listed["items"][0]["scheduleRef"]["generation"], 7);
    app.fake.assert_strict();
}

/// **`run_from_suspended_schedule_is_allowed_and_reported`** (D1 §12, paused
/// schedule).
///
/// A suspended schedule stops FUTURE SLOTS. It does not stop a person. The run
/// is created, `spec.suspend` is untouched, and the response says the schedule
/// is still suspended so the console can show the notice rather than leaving
/// the person to wonder whether the button resumed anything.
///
/// KILLS: refusing a manual run on a suspended schedule; resuming the schedule
/// as a side effect; hiding the suspension from the answer.
#[tokio::test]
async fn run_from_suspended_schedule_is_allowed_and_reported() {
    let app = TestApp::new();
    seed_schedule(&app.fake, "nightly", true);
    let response = app
        .post(
            &format!("/api/v1/namespaces/{NS_A}/backups"),
            Some(KEY),
            &from_schedule(),
        )
        .await;
    assert_eq!(response.status, 201, "{}", response.text());
    assert_eq!(response.json()["schedule"]["suspended"], true);
    let stored = app.fake.object("backupschedules", NS_A, "nightly").unwrap();
    assert_eq!(
        stored["spec"]["suspend"], true,
        "the run resumed the schedule"
    );
    assert_eq!(
        stored["metadata"]["generation"], 7,
        "the schedule was written to"
    );
    // The schedule was READ and never written.
    assert!(
        !app.fake
            .requests()
            .iter()
            .any(|r| r.method != "GET" && r.path.contains("/backupschedules")),
        "{:#?}",
        app.fake.requests()
    );
    app.fake.assert_strict();
}

/// **A scheduled run already going neither blocks the manual run nor counts
/// it.**
///
/// D1 §8.3: `concurrencyPolicy` is about slots, not about people — the CronJob
/// "run now" precedent. The active run is REPORTED as a non-blocking notice.
#[tokio::test]
async fn an_active_forbid_run_does_not_block_a_manual_run() {
    let app = TestApp::new();
    seed_schedule(&app.fake, "nightly", false);
    let response = app
        .post(
            &format!("/api/v1/namespaces/{NS_A}/backups"),
            Some(KEY),
            &from_schedule(),
        )
        .await;
    assert_eq!(response.status, 201, "{}", response.text());
    let context = &response.json()["schedule"];
    assert_eq!(context["activeRuns"].as_array().unwrap().len(), 1);
    assert_eq!(context["activeRuns"][0]["kind"], "Scheduled");
    // The manual run is not a schedule-created run: no slot, attempt 0, and
    // `Manual` is not one of `concurrencyPolicy`'s kinds.
    let posted = &backups(&app)[0];
    assert!(posted["spec"].get("slot").is_none());
    assert_eq!(
        posted["spec"]["trigger"],
        json!({"kind": "Manual", "attempt": 0})
    );
}

/// **`api_never_gates_on_readiness_but_records_acknowledgement`** (D1 §12,
/// failed preflight).
///
/// D1 §8.4 and the UI-FAKEPREFLIGHT rail: this API does not call a preflight,
/// does not wait for one, and does not refuse on one. A `notReady`
/// acknowledgement is an annotation naming the check that said so and nothing
/// more; the run's own execution-time guards remain the authority. A route
/// that CONSULTED readiness would be a fake gate — it cannot see what the pod
/// will see — and a route that refused without one would block the direct
/// `kubectl` path for no gain.
///
/// KILLS: reading the named `Preflight` (the fake records every request, and
/// this asserts none reached `preflights`); refusing a `notReady`
/// acknowledgement; inventing an acknowledgement when none was sent.
#[tokio::test]
async fn the_api_never_gates_on_readiness_but_records_the_acknowledgement() {
    let app = TestApp::new();
    seed_schedule(&app.fake, "nightly", false);
    let body = json!({
        "scheduleRef": {"name": "nightly"},
        "readinessAcknowledgement": {"preflight": "pf-abcdef", "state": "notReady"}
    })
    .to_string();
    let response = app
        .post(
            &format!("/api/v1/namespaces/{NS_A}/backups"),
            Some(KEY),
            &body,
        )
        .await;
    assert_eq!(response.status, 201, "{}", response.text());
    let posted = &backups(&app)[0];
    assert_eq!(
        posted["metadata"]["annotations"]["logweir.dev/readiness-ack"],
        "pf-abcdef=notReady"
    );
    assert!(
        !app.fake
            .requests()
            .iter()
            .any(|r| r.path.contains("/preflights")),
        "the API consulted a readiness result: {:#?}",
        app.fake.requests()
    );

    // No acknowledgement, no annotation: the API never invents one.
    let plain = app
        .post(
            &format!("/api/v1/namespaces/{NS_A}/backups"),
            Some("back-up-now-0002"),
            &from_schedule(),
        )
        .await;
    assert_eq!(plain.status, 201);
    let second = &backups(&app)[1];
    assert!(second["metadata"]["annotations"]
        .get("logweir.dev/readiness-ack")
        .is_none());
    app.fake.assert_strict();
}

/// **`copied_policy_digest_equals_schedule_generation_digest`** (D1 §12,
/// successful scheduled-policy copy).
///
/// The copy has to be the scheduler's copy. The run records
/// `{uid, generation, runPolicySha256}`, and the digest it records must equal
/// the one the CONTROLLER computes for the same generation — otherwise D1
/// §3.1's rule 5 fails the run terminally the moment the Backup controller
/// looks at it.
///
/// KILLS: copying the fields by hand in this route (the absent
/// `activeDeadlineSeconds` alone would produce a different digest, because
/// D1 §3.2 puts the RESOLVED deadline inside the digested document); reading
/// the digest out of `status.policy` instead of computing it from the copy —
/// which would record a digest for a generation the run is not running.
#[tokio::test]
async fn the_copied_policy_digest_equals_the_schedules_own() {
    let app = TestApp::new();
    seed_schedule(&app.fake, "nightly", false);
    let response = app
        .post(
            &format!("/api/v1/namespaces/{NS_A}/backups"),
            Some(KEY),
            &from_schedule(),
        )
        .await;
    assert_eq!(response.status, 201, "{}", response.text());
    let item = &response.json()["item"];
    assert_eq!(item["scheduleRef"]["generation"], 7);
    assert_eq!(item["scheduleRef"]["runPolicySha256"], schedule_digest());
    assert_eq!(item["triggeredBy"], "manual");
    assert_eq!(item["trigger"]["kind"], "Manual");
    assert_eq!(item["trigger"]["attempt"], 0);
    assert_eq!(item["topics"], json!(["orders", "payments"]));
    assert_eq!(item["deadlineSeconds"], 3600);

    // And the object really carries it: the controller recomputes this from
    // the stored spec, not from the response.
    let posted = &backups(&app)[0];
    assert_eq!(
        posted["spec"]["scheduleRef"]["runPolicySha256"],
        schedule_digest()
    );
    let spec: weirkeeper::crds::backup::BackupSpec =
        serde_json::from_value(posted["spec"].clone()).expect("the POST body is a BackupSpec");
    assert_eq!(
        weirkeeper::policy::run_policy_sha256(&spec),
        schedule_digest(),
        "the digest recorded is not the digest of the fields recorded"
    );
}

/// **A schedule whose stored policy cannot run is refused with a CLEAN field
/// path.**
///
/// A schedule can be stored in a shape this build refuses — an older CRD, a
/// hand edit, `topics: []` with no `allUserTopics`. Copying it and only then
/// validating is what makes the answer a 422 about the RUN rather than a run
/// that dies at the guard rail.
///
/// `FieldError.field` IS A PATH. It used to be `"scheduleRef.name (topics)"`
/// — a path with a parenthetical glued on — and `ui/contract.js` types the
/// field as a plain string with no parser, so a console matching
/// `e.field === "scheduleRef.name"` to highlight the schedule picker found
/// nothing. The schedule's own offending field belongs in the message.
///
/// KILLS: decorating the path again; dropping the schedule field from the
/// message, which would leave the person a 422 that names the picker and not
/// the problem.
#[tokio::test]
async fn a_schedule_whose_policy_cannot_run_is_refused_by_a_clean_path() {
    let app = TestApp::new();
    app.fake.seed(
        "backupschedules",
        NS_A,
        json!({
            "metadata": {"name": "empty", "generation": 1},
            "spec": {
                "schedule": "0 2 * * *",
                "sourceRef": {"name": "source"},
                "topics": [],
                "archive": {"url": "s3://kafka-backups/logweir"},
                "suspend": false
            }
        }),
    );
    let response = app
        .post(
            &format!("/api/v1/namespaces/{NS_A}/backups"),
            Some(KEY),
            &json!({"scheduleRef": {"name": "empty"}}).to_string(),
        )
        .await;
    response.assert_problem(422, "validation_failed");
    let error = &response.json()["errors"][0];
    assert_eq!(
        error["field"], "scheduleRef.name",
        "the field is a path a console can match on, not a path with a note glued to it"
    );
    assert_eq!(error["code"], "selection_invalid");
    let message = error["message"].as_str().unwrap_or_default();
    assert!(
        message.contains("spec.topics"),
        "the schedule's own offending field is in the message: {message}"
    );
    assert_eq!(app.fake.count("backups", NS_A), 0);

    // The ad-hoc form keeps its OWN paths, because those inputs are on the
    // form the person is looking at.
    let ad_hoc = app
        .post(
            &format!("/api/v1/namespaces/{NS_A}/backups"),
            Some("back-up-now-7777"),
            &json!({
                "sourceRef": {"name": "source"},
                "topicSelection": {"topics": []},
                "legacyArchive": {"url": "s3://b/p"}
            })
            .to_string(),
        )
        .await;
    ad_hoc.assert_problem(422, "validation_failed");
    assert_eq!(ad_hoc.json()["errors"][0]["field"], "topicSelection");
    app.fake.assert_strict();
}

/// **A schedule that moved on is `409 policy_changed`, with the revision it
/// moved TO.**
///
/// D1 §8.2: `expectedGeneration` is the revision the person was LOOKING at.
/// Running a different one silently is not what the button promised, and a
/// bare 409 would leave the console with nothing to show.
#[tokio::test]
async fn an_expected_generation_that_moved_is_policy_changed() {
    let app = TestApp::new();
    seed_schedule(&app.fake, "nightly", false);
    let response = app
        .post(
            &format!("/api/v1/namespaces/{NS_A}/backups"),
            Some(KEY),
            &json!({"scheduleRef": {"name": "nightly", "expectedGeneration": 6}}).to_string(),
        )
        .await;
    response.assert_problem(409, "policy_changed");
    let body = response.json();
    assert_eq!(body["policy"]["currentGeneration"], 7);
    assert_eq!(body["policy"]["currentRunPolicySha256"], schedule_digest());
    assert_eq!(app.fake.count("backups", NS_A), 0);

    // The matching generation runs.
    let ok = app
        .post(
            &format!("/api/v1/namespaces/{NS_A}/backups"),
            Some(KEY),
            &json!({"scheduleRef": {"name": "nightly", "expectedGeneration": 7}}).to_string(),
        )
        .await;
    assert_eq!(ok.status, 201, "{}", ok.text());
    app.fake.assert_strict();
}

/// **A policy field beside `scheduleRef` is `422`, and an ad-hoc body without
/// one is complete or refused.**
///
/// "Back up now on this schedule" promises the run the schedule describes. A
/// body that also named topics would produce a run whose receipt says
/// `scheduleRef: nightly` and whose contents are something else.
#[tokio::test]
async fn the_two_bodies_do_not_mix() {
    let app = TestApp::new();
    seed_schedule(&app.fake, "nightly", false);
    let mixed = app
        .post(
            &format!("/api/v1/namespaces/{NS_A}/backups"),
            Some(KEY),
            &json!({
                "scheduleRef": {"name": "nightly"},
                "topicSelection": {"topics": ["something-else"]}
            })
            .to_string(),
        )
        .await;
    mixed.assert_problem(422, "validation_failed");
    assert_eq!(mixed.json()["errors"][0]["field"], "topicSelection");
    assert_eq!(app.fake.count("backups", NS_A), 0);

    for (label, body) in [
        ("neither", json!({})),
        (
            "no selection",
            json!({"sourceRef": {"name": "source"}, "legacyArchive": {"url": "s3://b/p"}}),
        ),
        (
            "no destination",
            json!({"sourceRef": {"name": "source"}, "topicSelection": {"topics": ["orders"]}}),
        ),
        (
            "empty selection",
            json!({
                "sourceRef": {"name": "source"},
                "topicSelection": {"topics": []},
                "legacyArchive": {"url": "s3://b/p"}
            }),
        ),
        (
            "a glob",
            json!({
                "sourceRef": {"name": "source"},
                "topicSelection": {"topics": ["orders*"]},
                "legacyArchive": {"url": "s3://b/p"}
            }),
        ),
    ] {
        let response = app
            .post(
                &format!("/api/v1/namespaces/{NS_A}/backups"),
                Some("back-up-now-9999"),
                &body.to_string(),
            )
            .await;
        response.assert_problem(422, "validation_failed");
        assert_eq!(app.fake.count("backups", NS_A), 0, "{label}");
    }
    // A schedule that is not there is `not_found`, not a run against nothing.
    app.post(
        &format!("/api/v1/namespaces/{NS_A}/backups"),
        Some("back-up-now-8888"),
        &json!({"scheduleRef": {"name": "absent"}}).to_string(),
    )
    .await
    .assert_problem(404, "not_found");
    app.fake.assert_strict();
}

/// **"Run first backup now": an ad-hoc run against a cluster with no schedule
/// at all.**
///
/// D1 §8.2's body B. It reads NOTHING — there is no schedule to read — and the
/// run it creates has no `scheduleRef`, so its execution id is its own UID and
/// nothing binds it to a policy nobody wrote down.
#[tokio::test]
async fn an_ad_hoc_run_reads_nothing_and_records_no_schedule() {
    let app = TestApp::new();
    let body = json!({
        "sourceRef": {"name": "source"},
        "topicSelection": {"topics": ["orders", "payments"]},
        "legacyArchive": {"url": "s3://kafka-backups/logweir", "credentialRef": {"name": "logweir-s3"}},
        "deadlineSeconds": 3600
    })
    .to_string();
    let response = app
        .post(
            &format!("/api/v1/namespaces/{NS_A}/backups"),
            Some(KEY),
            &body,
        )
        .await;
    assert_eq!(response.status, 201, "{}", response.text());
    assert!(response.json().get("schedule").is_none());
    assert!(response.json()["item"].get("scheduleRef").is_none());
    assert!(
        !app.fake
            .requests()
            .iter()
            .any(|r| r.path.contains("/backupschedules")),
        "an ad-hoc run read a schedule: {:#?}",
        app.fake.requests()
    );
    let posted = &backups(&app)[0];
    assert!(posted["metadata"]["labels"]
        .get("logweir.dev/schedule-uid")
        .is_none());
    assert_eq!(posted["spec"]["triggeredBy"], "manual");

    // A saved destination instead of an inline archive: the sentinel is built
    // here and never accepted from a body.
    let with_destination = app
        .post(
            &format!("/api/v1/namespaces/{NS_A}/backups"),
            Some("back-up-now-0002"),
            &json!({
                "sourceRef": {"name": "source"},
                "topicSelection": {"topics": ["orders"]},
                "destinationRef": {"name": "primary"}
            })
            .to_string(),
        )
        .await;
    assert_eq!(with_destination.status, 201, "{}", with_destination.text());
    let posted = &backups(&app)[1];
    assert_eq!(
        posted["spec"]["archive"],
        json!({"url": "logweir-destination://primary"})
    );
    assert_eq!(posted["spec"]["destinationRef"], json!({"name": "primary"}));
    app.post(
        &format!("/api/v1/namespaces/{NS_A}/backups"),
        Some("back-up-now-0003"),
        &json!({
            "sourceRef": {"name": "source"},
            "topicSelection": {"topics": ["orders"]},
            "legacyArchive": {"url": "logweir-destination://primary"}
        })
        .to_string(),
    )
    .await
    .assert_problem(422, "validation_failed");
    app.fake.assert_strict();
}

/// **Dynamic selection survives the copy, and the third shape is the API
/// SERVER's to refuse.**
///
/// A schedule in `allUserTopics` mode has `topics: []`; a manual run of it must
/// carry both halves or the runner's empty-list rail exits 3. The third shape
/// — a named allowlist AND a block — is the `Backup` CRD's own rule, not a copy
/// kept here.
#[tokio::test]
async fn dynamic_selection_is_copied_whole() {
    let app = TestApp::new();
    app.fake.seed(
        "backupschedules",
        NS_A,
        json!({
            "metadata": {"name": "dyn", "generation": 2},
            "spec": {
                "schedule": "0 2 * * *",
                "sourceRef": {"name": "source"},
                "topics": [],
                "allUserTopics": {
                    "exclude": {"topics": ["skip-me"], "prefixes": ["pfx-"]},
                    "incompleteDiscovery": "BackUpVisibleTopics"
                },
                "archive": {"url": "s3://kafka-backups/logweir"},
                "suspend": false
            }
        }),
    );
    let response = app
        .post(
            &format!("/api/v1/namespaces/{NS_A}/backups"),
            Some(KEY),
            &json!({"scheduleRef": {"name": "dyn"}}).to_string(),
        )
        .await;
    assert_eq!(response.status, 201, "{}", response.text());
    let posted = &backups(&app)[0];
    assert_eq!(posted["spec"]["topics"], json!([]));
    assert_eq!(
        posted["spec"]["allUserTopics"],
        json!({
            "exclude": {"topics": ["skip-me"], "prefixes": ["pfx-"]},
            "incompleteDiscovery": "BackUpVisibleTopics"
        })
    );
    // The ad-hoc form can ask for it too, and `incompleteDiscovery` is
    // REQUIRED with no default: both possible defaults are wrong in a way the
    // operator would not notice.
    app.post(
        &format!("/api/v1/namespaces/{NS_A}/backups"),
        Some("back-up-now-0002"),
        &json!({
            "sourceRef": {"name": "source"},
            "topicSelection": {"allUserTopics": {"exclude": {"prefixes": ["pfx-"]}}},
            "legacyArchive": {"url": "s3://b/p"}
        })
        .to_string(),
    )
    .await
    .assert_problem(422, "validation_failed");
    app.fake.assert_strict();
}

/// **The shipped sample is the object this route creates — driven through the
/// route and compared byte for byte.**
///
/// D1 §8.1 promises ONE CR path, and the sample's own header says the API
/// produces "exactly this shape". A test that only checked the file was
/// internally well-formed would let the two drift apart while still passing
/// under a name that promises a comparison — which is what the reviewer found:
/// the sample carried a decorative `app.kubernetes.io/name` label the route
/// never writes.
///
/// So this drives `POST .../backups` against a schedule seeded with the
/// sample's own copied policy, UID and generation, and asserts that the object
/// the fake API server STORED equals the sample's `metadata.labels` and `spec`
/// exactly. Only the name differs, and only because the route mints its own
/// deterministic one; the sample's name is a placeholder of the same shape.
///
/// KILLS: a label on either side the other does not have; a copied field that
/// drifts (the resolved `deadlineSeconds`, `allUserTopics`, the archive's
/// `secretRef`); a `trigger` block that stops being `{kind: Manual, attempt:
/// 0}`; a stale `runPolicySha256` in the sample, which
/// `identity::check_run_policy_digest` would refuse the run for.
#[tokio::test]
async fn the_shipped_sample_is_what_this_route_creates() {
    let path = support::repo_root().join("config/samples/backup-manual.yaml");
    let text = std::fs::read_to_string(&path).expect("the sample exists");
    let sample: weirkeeper::crds::backup::Backup =
        serde_yaml::from_str(&text).expect("the sample is a Backup");
    let sample_value: Value = serde_json::to_value(&sample).expect("a Backup serialises");
    let reference = sample
        .spec
        .schedule_ref
        .as_ref()
        .expect("the sample shows the copy");

    // The schedule the sample says it copied: the same UID, the same
    // generation, and the policy fields the sample carries. `FakeKube::seed`
    // keeps a `uid` that is already in the metadata, so the copy can be
    // compared literally rather than after a substitution.
    let app = TestApp::new();
    app.fake.seed(
        "backupschedules",
        NS_A,
        json!({
            "metadata": {
                "name": reference.name,
                "uid": reference.uid,
                "generation": reference.generation,
            },
            "spec": {
                "schedule": "0 2 * * *",
                "sourceRef": sample.spec.source_ref,
                "topics": sample.spec.topics,
                "archive": sample.spec.archive,
                "activeDeadlineSeconds": sample.spec.deadline_seconds,
                "suspend": false
            }
        }),
    );
    let response = app
        .post(
            &format!("/api/v1/namespaces/{NS_A}/backups"),
            Some(KEY),
            &json!({"scheduleRef": {"name": reference.name}}).to_string(),
        )
        .await;
    assert_eq!(response.status, 201, "{}", response.text());
    let name = response.json()["item"]["name"]
        .as_str()
        .expect("a name")
        .to_string();
    let stored = app
        .fake
        .object("backups", NS_A, &name)
        .expect("the route stored it");

    // THE COMPARISON. The spec is the whole policy and the whole identity.
    assert_eq!(
        stored["spec"], sample_value["spec"],
        "config/samples/backup-manual.yaml and POST .../backups disagree about the spec"
    );
    // The labels are compared WHOLE, not as a subset: a decorative label on
    // either side is a claim about the other that is not true.
    assert_eq!(
        stored["metadata"]["labels"], sample_value["metadata"]["labels"],
        "the sample's labels are not the ones the route writes"
    );

    // The name differs only because the route mints its own from the
    // idempotency scope; both are the same 41-character shape.
    let sample_name = sample.metadata.name.as_deref().unwrap_or_default();
    assert_ne!(sample_name, name, "the sample's name is a placeholder");
    for candidate in [sample_name, name.as_str()] {
        assert!(
            candidate.starts_with("logweir-manual-") && candidate.len() == 41,
            "D1 §8.1's deterministic name: {candidate}"
        );
    }

    // And the identity module agrees: the digest the SHIPPED FILE carries is
    // the digest of its own policy fields — the check the Backup controller
    // recomputes and refuses the run terminally on (D1 §3.1 rule 5) — and the
    // object the ROUTE created resolves to a manual identity that is its own
    // UID and holds no scheduled identity at all.
    //
    // The digest check runs on the file and the identity check on the stored
    // object, because a manual run's identity IS `metadata.uid` and a YAML
    // sample has none until an API server assigns one.
    weirkeeper::identity::check_run_policy_digest(&sample).expect("the digest checks out");
    let created: weirkeeper::crds::backup::Backup =
        serde_json::from_value(stored.clone()).expect("the stored object is a Backup");
    weirkeeper::identity::check_run_policy_digest(&created)
        .expect("the created run's digest checks out");
    let identity = weirkeeper::identity::run_identity(&created).expect("a manual identity");
    assert!(
        identity.schedule.is_none(),
        "a manual run holds no SCHEDULED identity, whatever schedule it copied"
    );
    assert_eq!(
        identity.execution_id,
        created.metadata.uid.clone().unwrap_or_default(),
        "a manual run's execution id is its own UID"
    );
    app.fake.assert_strict();
}

// ===========================================================================
// The name rule, pinned across two languages by ONE file
// ===========================================================================

/// **`ui/tests/fixtures/manual-backup-names.json` is THIS route's own rule, and
/// this is the Rust half of that pin (review F1).**
///
/// A manual `Backup` has no name until its idempotency scope is hashed, so the
/// name IS the replay guard, and the rule is implemented twice: here, in
/// [`logweir_api::idempotency::identity`], and in `ui/client.js`'s
/// `manualBackupName`. Two implementations of a hash agree until the day they
/// do not.
///
/// THE FIXTURE WAS PINNED ON ONE SIDE ONLY, WHICH IS NOT A PIN.
/// `ui/tests/d1.spec.js` drives the page over every recorded row, and
/// `scripts/live/d1/run.py`'s `L-06-2-cli` drives the real binary over ONE live
/// scope — so a change to `ROUTE_CREATE`, [`NAME_PREFIX`], the field order,
/// `push_field`'s width, `NAME_HASH_CHARS` or the base32 alphabet passed every
/// gate that runs without a cluster. This row closes that: the six recorded
/// names are now what BOTH implementations produce, checked in CI, with no
/// cluster and no browser.
///
/// KILLS: any change to the scope document that moves a name — the opener, a
/// field's position, a length prefix's width, the alphabet, the 26-character
/// cut, the prefix, or the route literal. Measured: reordering `issuer` and
/// `subject` in `identity`, widening the length prefix, and taking
/// `[1..27]` of the base32 instead of `[..26]` each fail this row.
#[test]
fn the_manual_run_name_fixture_is_this_routes_own_rule() {
    use logweir_api::auth::Actor;
    use logweir_api::idempotency::{self, IdempotencyKey, NAME_HASH_CHARS};
    use logweir_api::routes::backups::{NAME_PREFIX, ROUTE_CREATE};

    let path = support::repo_root().join("ui/tests/fixtures/manual-backup-names.json");
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!(
            "{} is missing ({e}); it is the pin between this route and ui/client.js",
            path.display()
        )
    });
    let fixture: Value = serde_json::from_str(&text).expect("the fixture is JSON");

    // THE `rule` BLOCK IS THIS CRATE'S OWN CONSTANTS, not prose beside them. A
    // fixture that described a rule the route does not implement would pass its
    // own rows and pin nothing.
    let rule = &fixture["rule"];
    assert_eq!(
        rule["prefix"].as_str(),
        Some(NAME_PREFIX),
        "the fixture's name prefix is not this route's"
    );
    assert_eq!(
        rule["route"].as_str(),
        Some(ROUTE_CREATE),
        "the fixture's route identifier is not this route's"
    );
    assert_eq!(
        rule["hashChars"].as_u64(),
        Some(NAME_HASH_CHARS as u64),
        "the fixture keeps a different number of base32 characters"
    );
    assert_eq!(
        rule["fieldOrder"].as_array().map(|f| f
            .iter()
            .map(|v| v.as_str().unwrap_or_default().to_string())
            .collect::<Vec<_>>()),
        Some(vec![
            "issuer".to_string(),
            "subject".to_string(),
            "namespace".to_string(),
            "route".to_string(),
            "key".to_string(),
        ]),
        "the fixture names a different scope than D1 §8.2's"
    );
    // AND THE ALPHABET IS THE ONE `base32_lower` ACTUALLY EMITS, read out of
    // the function rather than copied from the document: the 32 five-bit values
    // 0..31, in order, are exactly twenty bytes of input.
    let ramp: [u8; 20] = [
        0x00, 0x44, 0x32, 0x14, 0xc7, 0x42, 0x54, 0xb6, 0x35, 0xcf, 0x84, 0x65, 0x3a, 0x56, 0xd7,
        0xc6, 0x75, 0xbe, 0x77, 0xdf,
    ];
    assert_eq!(
        idempotency::base32_lower(&ramp),
        rule["alphabet"].as_str().unwrap_or_default(),
        "the fixture's alphabet is not the one base32_lower emits"
    );

    let rows = fixture["rows"].as_array().expect("the fixture has rows");
    assert!(
        rows.len() >= 6,
        "the fixture lost rows; it had six, one per property it is for"
    );
    let mut produced: BTreeSet<String> = BTreeSet::new();
    for row in rows {
        let scope = &row["scope"];
        let note = row["note"].as_str().unwrap_or_default();
        let key_text = scope["key"].as_str().expect("a row's key");
        let mut headers = http::HeaderMap::new();
        headers.insert(
            idempotency::HEADER,
            key_text.parse().expect("a row's key is a header value"),
        );
        // THROUGH `from_headers`, so a row whose key this route would REFUSE
        // (under 8 characters, over 128, non-visible ASCII) cannot sit in the
        // fixture claiming a name the route would never mint.
        let key = IdempotencyKey::from_headers(&headers)
            .unwrap_or_else(|_| panic!("this route would refuse the key in row: {note}"));
        let actor = Actor::new(
            scope["issuer"].as_str().expect("a row's issuer"),
            scope["subject"].as_str().expect("a row's subject"),
            "",
        );
        let identity = idempotency::identity(
            &actor,
            scope["namespace"].as_str().expect("a row's namespace"),
            scope["route"].as_str().expect("a row's route"),
            NAME_PREFIX,
            &key,
            b"",
        );
        assert_eq!(
            identity.name,
            row["name"].as_str().unwrap_or_default(),
            "this route derives a different name from the recorded scope ({note}). The other \
             implementation is ui/client.js::manualBackupName and ui/tests/d1.spec.js holds it \
             to the same file."
        );
        assert_eq!(
            identity.name.len(),
            41,
            "prefix plus 26 characters ({note})"
        );
        produced.insert(identity.name);
    }
    // THE LENGTH PREFIX, AS A PROPERTY AND NOT AS A COMMENT. Two of the rows
    // are the same characters cut in two places — (subject "admin", namespace
    // "lw-p062-demo") and (subject "", namespace "adminlw-p062-demo") — which
    // collide under a naive concatenation. Every name being distinct is what
    // says the 64-bit length prefixes are doing their job on THIS side too.
    assert_eq!(
        produced.len(),
        rows.len(),
        "two recorded scopes produced the same name: the scope document is ambiguous"
    );
}
