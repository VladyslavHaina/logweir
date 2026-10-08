//! PROD-08.1a: the product API carries a restore's coverage request in, and
//! says sampled or complete — with a complete verification's exact counts and
//! `covered` — on the way out.
//!
//! BOTH SIDES READ ONE FIXTURE. `ui/tests/fixtures/console/restore-complete-uncovered.json`
//! and `.../operation-restore-complete-uncovered.json` are this crate's
//! projections of the custom resource `ui/tests/fixtures/restore-complete-uncovered.json`;
//! `ui/tests/complete-coverage.spec.js` decodes and renders the same files, so
//! the field names cannot drift between the API and the console. Set
//! `LOGWEIR_WRITE_COVERAGE_FIXTURES=1` to rewrite them from the projection.

mod support;

use logweir_api::projection::restore;
use logweir_api::status::restore_view;
use serde_json::{json, Value};
use support::{TestApp, NS_A};
use weirkeeper::crds::restore::Restore as RestoreCr;

fn now() -> chrono::DateTime<chrono::Utc> {
    "2026-09-19T01:30:00Z".parse().expect("a fixed instant")
}

fn uncovered() -> Value {
    support::fixture("restore-complete-uncovered.json")
}

fn cr(value: &Value) -> RestoreCr {
    serde_json::from_value(value.clone()).expect("the fixture is a Restore")
}

/// The projection compared with the checked-in console fixture, or written to
/// it when `LOGWEIR_WRITE_COVERAGE_FIXTURES` is set.
fn golden(name: &str, projected: &Value) -> Value {
    let path = support::repo_root()
        .join("ui/tests/fixtures/console")
        .join(name);
    if std::env::var_os("LOGWEIR_WRITE_COVERAGE_FIXTURES").is_some() {
        let doc = json!({"requestId": "req-coverage-0001", "item": projected});
        std::fs::write(
            &path,
            format!("{}\n", serde_json::to_string_pretty(&doc).unwrap()),
        )
        .expect("the fixture is written");
    }
    support::fixture(&format!("console/{name}"))
}

/// **A `covered: false` complete run reads as not-passed in the API**, on the
/// list row and on the detail: the Restore says it asked for complete
/// coverage (with its bound), that the signed scorecard says complete, that it
/// did NOT cover, and why; `operation.verifiedSuccess` is `false` and the
/// outcome is the scorecard's own `fail-integrity`. The console fixture is
/// this projection.
///
/// KILLS: `restore_coverage` dropping `covered` or `incompleteReason`; reading
/// `requested` from the status instead of the spec.
#[test]
fn a_complete_run_that_did_not_cover_reads_complete_and_not_passed_on_list_and_detail() {
    let object = cr(&uncovered());
    for with_bytes in [false, true] {
        let projected = serde_json::to_value(restore(&object, with_bytes)).unwrap();
        let coverage = &projected["coverage"];
        assert_eq!(coverage["requested"], "complete", "{coverage}");
        assert_eq!(coverage["completeMaxRecords"], 300);
        assert_eq!(coverage["recorded"], "complete");
        assert_eq!(coverage["covered"], false);
        assert!(
            coverage["incompleteReason"]
                .as_str()
                .is_some_and(|r| r.contains("complete_max_records is 300")),
            "{coverage}"
        );
        assert_eq!(projected["operation"]["verifiedSuccess"], false);
        assert_eq!(projected["operation"]["outcome"], "fail-integrity");
        if with_bytes {
            let fixture = golden("restore-complete-uncovered.json", &projected);
            assert_eq!(
                projected, fixture["item"],
                "the console fixture is this crate's projection of the same object"
            );
        }
    }
}

/// **The operation view carries the per-partition exact counts.** The
/// `verificationScope` of a complete run names the coverage, `covered`, the
/// reason, the archive counts, the totals and one row per partition with its
/// own exact counts and `compared` — the console's detail renders these.
///
/// KILLS: `complete_view` dropping the partition rows, the reason or the
/// archive counts; `coverage` derived from `integrity.level`.
#[test]
fn the_operation_view_says_complete_with_the_exact_counts_per_partition() {
    let view = serde_json::to_value(restore_view(&cr(&uncovered()), now())).unwrap();
    let scope = &view["verificationScope"];
    assert_eq!(scope["coverage"], "complete", "{scope}");
    assert_eq!(
        scope["level"], "sampled",
        "the level is HOW records were compared, unchanged"
    );
    let complete = &scope["complete"];
    assert_eq!(complete["covered"], false);
    assert_eq!(complete["maxRecords"], 300);
    assert_eq!(complete["segments"], 4);
    assert_eq!(complete["segmentsUnverified"], 2);
    assert_eq!(complete["replay"]["expected"], 250);
    assert_eq!(complete["partitionCount"], 2);
    assert_eq!(complete["partitions"][0]["replay"]["matching"], 250);
    assert_eq!(complete["partitions"][1]["compared"], false);
    assert_eq!(view["result"]["outcome"], "fail-integrity");
    let fixture = golden("operation-restore-complete-uncovered.json", &view);
    assert_eq!(
        view, fixture["item"],
        "the console fixture is this crate's projection of the same object"
    );
}

/// **`covered: false` is never `verifiedSuccess`, even beside a forged pass.**
/// The same object with `outcome: pass`, exit 0 and a `pass` integrity result
/// — a status arm IV-6 keeps out of every accepted document — is still not a
/// verified success, because the controller's badge rule refuses green beside
/// `covered: false` and the API computes `verifiedSuccess` with that rule. The
/// control: `covered: true` beside the same status is a verified success.
///
/// KILLS: the badge's `covered` arm removed (the forged row reads green).
#[test]
fn a_forged_pass_beside_covered_false_is_never_a_verified_success() {
    let forged = |covered: bool| {
        let mut v = uncovered();
        v["status"]["phase"] = json!("Succeeded");
        v["status"]["exitCode"] = json!(0);
        v["status"]["exitReason"] = json!("ok");
        v["status"]["outcome"] = json!("pass");
        v["status"]["integrity"]["result"] = json!("pass");
        v["status"]["integrity"]["complete"]["covered"] = json!(covered);
        cr(&v)
    };
    let row = serde_json::to_value(restore(&forged(false), false)).unwrap();
    assert_eq!(row["operation"]["verifiedSuccess"], false, "{row}");
    assert_eq!(row["coverage"]["covered"], false);
    let control = serde_json::to_value(restore(&forged(true), false)).unwrap();
    assert_eq!(control["operation"]["verifiedSuccess"], true, "{control}");
}

/// **A sampled run never claims complete.** A Restore that asks for nothing
/// and whose scorecard predates format 1.4.0 reads `requested: sampled`, no
/// `recorded` coverage (not recorded) and no complete block anywhere; one
/// whose scorecard says `sampled` reads `recorded: sampled` and lists FX-23's
/// unsampled topics; and a status whose coverage says sampled but which
/// carries a complete block (no reader writes one) never serves the block.
///
/// KILLS: `recorded` defaulting to complete; serving a complete block beside a
/// sampled coverage; dropping `unsampledTopics`.
#[test]
fn a_sampled_run_never_claims_complete() {
    let plain = cr(&support::fixture("restore-valid-pass.json"));
    let row = serde_json::to_value(restore(&plain, false)).unwrap();
    assert_eq!(row["coverage"], json!({"requested": "sampled"}), "{row}");
    let view = serde_json::to_value(restore_view(&plain, now())).unwrap();
    assert!(view["verificationScope"].get("coverage").is_none());
    assert!(view["verificationScope"].get("complete").is_none());

    let mut sampled = support::fixture("restore-valid-pass.json");
    sampled["status"]["integrity"]["coverage"] = json!("sampled");
    sampled["status"]["integrity"]["unsampledTopics"] = json!(["audit", "payments"]);
    // A block beside a sampled coverage is not believed.
    sampled["status"]["integrity"]["complete"] =
        uncovered()["status"]["integrity"]["complete"].clone();
    let object = cr(&sampled);
    let row = serde_json::to_value(restore(&object, false)).unwrap();
    assert_eq!(row["coverage"]["recorded"], "sampled");
    assert!(row["coverage"].get("covered").is_none(), "{row}");
    let view = serde_json::to_value(restore_view(&object, now())).unwrap();
    let scope = &view["verificationScope"];
    assert_eq!(scope["coverage"], "sampled");
    assert!(scope.get("complete").is_none(), "{scope}");
    assert_eq!(scope["unsampledTopics"], json!(["audit", "payments"]));
}

/// **The create route carries the request into `spec`, and refuses a bound
/// on nothing.** `coverage: complete` with `completeMaxRecords` is stored on
/// `Restore.spec` exactly; a bound without complete coverage and a bound of 0
/// are refused by field; and a request that states no coverage stores no
/// field — the object every older client created.
///
/// KILLS: `build` dropping either field; the bound's checks removed.
#[tokio::test]
async fn the_create_route_stores_the_coverage_request_and_refuses_a_bound_on_nothing() {
    let app = TestApp::new();
    let plan = support::golden_plan().replace(
        "  anchor: \"head\"\n",
        "  anchor: \"head\"\n  coverage: \"complete\"\n  complete_max_records: 1000\n",
    );
    assert!(plan.contains("coverage: \"complete\""), "the golden moved");
    let mut body = support::restore_body(&plan);
    body["coverage"] = json!("complete");
    body["completeMaxRecords"] = json!(1000);
    let created = app
        .post(
            &format!("/api/v1/namespaces/{NS_A}/restores"),
            Some("coverage-complete-01"),
            &body.to_string(),
        )
        .await;
    assert_eq!(
        created.status,
        201,
        "{}",
        String::from_utf8_lossy(&created.body)
    );
    let item = created.json()["item"].clone();
    assert_eq!(item["coverage"]["requested"], "complete", "{item}");
    assert_eq!(item["coverage"]["completeMaxRecords"], 1000);
    let name = item["name"].as_str().unwrap().to_string();
    let stored = app.fake.object("restores", NS_A, &name).unwrap();
    assert_eq!(stored["spec"]["coverage"], "complete");
    assert_eq!(stored["spec"]["completeMaxRecords"], 1000);

    for (key, mutate, field, code) in [
        (
            "coverage-bound-sampled",
            json!({"completeMaxRecords": 5}),
            "completeMaxRecords",
            "requires_complete_coverage",
        ),
        (
            "coverage-bound-zero-1",
            json!({"coverage": "complete", "completeMaxRecords": 0}),
            "completeMaxRecords",
            "out_of_range",
        ),
    ] {
        let mut bad = support::restore_body(&support::golden_plan());
        for (k, v) in mutate.as_object().unwrap() {
            bad[k] = v.clone();
        }
        let refused = app
            .post(
                &format!("/api/v1/namespaces/{NS_A}/restores"),
                Some(key),
                &bad.to_string(),
            )
            .await;
        assert_eq!(refused.status, 422, "{key}");
        let errors = refused.json()["errors"].clone();
        assert!(
            errors
                .as_array()
                .unwrap()
                .iter()
                .any(|e| e["field"] == field && e["code"] == code),
            "{key}: {errors}"
        );
    }

    let plain = app
        .post(
            &format!("/api/v1/namespaces/{NS_A}/restores"),
            Some("coverage-absent-0001"),
            &support::restore_body(&support::golden_plan()).to_string(),
        )
        .await;
    assert_eq!(plain.status, 201);
    let name = plain.json()["item"]["name"].as_str().unwrap().to_string();
    let stored = app.fake.object("restores", NS_A, &name).unwrap();
    assert!(
        stored["spec"].get("coverage").is_none()
            && stored["spec"].get("completeMaxRecords").is_none(),
        "{}",
        stored["spec"]
    );
}

/// The rehearsal view says what each slot verifies: `sampled` for a schedule
/// that states nothing, `complete` (with its bound) for one that asks.
#[test]
fn a_rehearsal_schedule_view_says_sampled_or_complete() {
    let schedule =
        |bounds_extra: Value| -> weirkeeper::crds::rehearsal_schedule::RehearsalSchedule {
            let mut bounds = json!({"deadlineSeconds": 3600, "recordsPerPartition": 25,
                                "maxPartitions": 200});
            for (k, v) in bounds_extra.as_object().unwrap() {
                bounds[k] = v.clone();
            }
            serde_json::from_value(json!({
            "apiVersion": "logweir.dev/v1alpha1",
            "kind": "RehearsalSchedule",
            "metadata": {"name": "weekly", "namespace": NS_A, "uid": "u"},
            "spec": {
                "schedule": "0 3 * * 0",
                "point": {"scheduleRefs": [{"name": "nightly"}], "requireVerifiedEvidence": true},
                "target": {"clusterRef": {"name": "scratch"}, "topicPrefix": "rehearsal-",
                           "markerTopic": "logweir.scratch"},
                "bounds": bounds,
                "authorization": {"standingApprovalRef": {"name": "a"}}
            }
        }))
        .expect("a RehearsalSchedule")
        };
    let plain =
        serde_json::to_value(logweir_api::routes::rehearsals::view(&schedule(json!({})))).unwrap();
    assert_eq!(plain["bounds"]["coverage"], "sampled");
    assert!(plain["bounds"].get("completeMaxRecords").is_none());
    let complete = serde_json::to_value(logweir_api::routes::rehearsals::view(&schedule(
        json!({"coverage": "complete", "completeMaxRecords": 50000}),
    )))
    .unwrap();
    assert_eq!(complete["bounds"]["coverage"], "complete");
    assert_eq!(complete["bounds"]["completeMaxRecords"], 50000);
}

/// **The live chain, over scorecards a real run signed** (`#[ignore]`d: it
/// reads the outcome files `e2e/tests/record_semantics.rs`'s
/// `a_console_plan_asking_for_complete_coverage_verifies_every_record_on_the_stack`
/// writes on a compose slot). Each signed scorecard goes through the
/// controller's own reader (`scorecard_observation`, `integrity_block`, and
/// the badge rule, whose `Verified` condition replaces the fixture's and
/// whose reason is asserted), onto a `Restore` status beside a `Valid`
/// verdict, and through
/// this crate's list and operation projections; the projections are written to
/// `LOGWEIR_COVERAGE_OUT` for the console to render.
///
/// ```text
/// LOGWEIR_COVERAGE_SCORECARDS=.e2e/logweir-e2e-s4/record-semantics \
/// LOGWEIR_COVERAGE_OUT=/tmp/out \
///   cargo test -p logweir-api --test complete_coverage -- --ignored live
/// ```
#[test]
#[ignore = "reads the signed scorecards a compose-slot run left; see the doc comment"]
fn live_signed_scorecards_flow_through_the_controller_and_the_api() {
    let dir = std::path::PathBuf::from(
        std::env::var("LOGWEIR_COVERAGE_SCORECARDS")
            .expect("LOGWEIR_COVERAGE_SCORECARDS names the outcome directory"),
    );
    let out = std::path::PathBuf::from(
        std::env::var("LOGWEIR_COVERAGE_OUT").expect("LOGWEIR_COVERAGE_OUT names a directory"),
    );
    std::fs::create_dir_all(&out).expect("the output directory");
    for (label, exit, recorded, covered) in [
        ("cfull", 0, "complete", Some(true)),
        ("cbound", 2, "complete", Some(false)),
        ("sampled", 0, "sampled", None),
    ] {
        let bytes = std::fs::read(dir.join(format!("cc-console-{label}.scorecard.json")))
            .unwrap_or_else(|e| panic!("{label}: {e}"));
        let o =
            weirkeeper::controllers::restore::scorecard_observation(&bytes).expect("a scorecard");
        let mut doc = uncovered();
        doc["metadata"]["name"] = json!(format!("live-{label}"));
        if recorded == "sampled" {
            doc["spec"].as_object_mut().unwrap().remove("coverage");
            doc["spec"]
                .as_object_mut()
                .unwrap()
                .remove("completeMaxRecords");
        }
        let status = doc["status"].as_object_mut().unwrap();
        status.insert("exitCode".into(), json!(exit));
        status.insert("outcome".into(), json!(o.outcome.clone().unwrap()));
        status.insert(
            "phase".into(),
            json!(if exit == 0 { "Succeeded" } else { "Failed" }),
        );
        status.insert(
            "integrity".into(),
            Value::Object(weirkeeper::controllers::restore::integrity_block(&o)),
        );
        // The `Verified` condition the controller would write over THIS
        // status, from its own badge rule -- not the fixture's: a real
        // `covered: false` run is `CompleteNotCovered` (review M2), a covered
        // or sampled pass is `Verified`.
        let badge = weirkeeper::verification::restore_badge(&doc["status"]);
        let want = if covered == Some(false) {
            weirkeeper::conditions::REASON_COMPLETE_NOT_COVERED
        } else {
            weirkeeper::conditions::REASON_VERIFIED
        };
        assert_eq!(badge.reason, want, "{label}: {badge:?}");
        for condition in doc["status"]["conditions"]
            .as_array_mut()
            .expect("the fixture carries conditions")
            .iter_mut()
            .filter(|c| c["type"] == "Verified")
        {
            condition["status"] = json!(if badge.green { "True" } else { "False" });
            condition["reason"] = json!(badge.reason);
            condition["message"] = json!(badge.label);
        }
        let object = cr(&doc);
        let row = serde_json::to_value(restore(&object, false)).unwrap();
        let detail = serde_json::to_value(restore(&object, true)).unwrap();
        let view = serde_json::to_value(restore_view(&object, now())).unwrap();
        assert_eq!(row["coverage"]["recorded"], recorded, "{label}: {row}");
        assert_eq!(
            row["coverage"].get("covered").and_then(Value::as_bool),
            covered,
            "{label}"
        );
        assert_eq!(view["verificationScope"]["coverage"], recorded, "{label}");
        let green = row["operation"]["verifiedSuccess"] == true;
        assert_eq!(green, covered != Some(false) && exit == 0, "{label}: {row}");
        if covered == Some(false) {
            assert_eq!(view["verificationScope"]["complete"]["covered"], false);
            let rows = view["verificationScope"]["complete"]["partitions"]
                .as_array()
                .expect("rows");
            assert_eq!(
                rows.iter()
                    .map(|p| p["compared"] == true)
                    .collect::<Vec<_>>(),
                vec![true, false, false]
            );
        }
        for (name, doc) in [
            (
                format!("{label}.restore-list-row.json"),
                json!({"requestId": "live", "items": [row], "page": {"limit": 50, "nextCursor": null, "snapshot": null}}),
            ),
            (
                format!("{label}.restore.json"),
                json!({"requestId": "live", "item": detail}),
            ),
            (
                format!("{label}.operation.json"),
                json!({"requestId": "live", "item": view}),
            ),
            (format!("{label}.status.json"), doc["status"].clone()),
        ] {
            std::fs::write(
                out.join(name),
                format!("{}\n", serde_json::to_string_pretty(&doc).unwrap()),
            )
            .expect("written");
        }
    }
}
