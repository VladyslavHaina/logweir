//! The `BackupSchedule` cron reconciler, and guard **G-SLOT**.
//!
//! EVERY TEST HERE IS A PURE-FUNCTION TEST OR A `mock_client` TEST, WITH ONE
//! DELIBERATE EXCEPTION. Nothing dials a socket, nothing waits on a Job, and
//! nothing approaches Global Constraint 22's 15 s per-test bound — the
//! transport is a `tower` closure and the clock is an argument.
//!
//! The exception is `the_backup_runner_argv_is_one_the_cli_accepts`, which
//! SHELLS OUT to `target/debug/logweir` on purpose. Erratum **E20** is why:
//! this crate emitted a `Backup` argv the CLI refused, every scheduled
//! `Backup` in the tree exited 1 for six tasks, and three green reviews missed
//! it because **every assertion about the argv compared the reconciler's
//! output against a literal written in the same repository**. An argv a
//! controller emits is an interface with another binary, and it is tested only
//! when it is handed to that binary's real parser. Task 22's
//! `restore_controller.rs::the_restore_job_projects_every_unexpired_roster_key_id`
//! is the same shape, and it exists because erratum E10 caught the identical
//! class on the `Restore` side. It dials nothing: the run stops at the
//! credential projection, before any librdkafka handle exists.
//!
//! THE GUARD IS `a_crash_between_create_and_status_write_yields_exactly_one_backup`.
//! Read it first: everything else in this file exists to make its assertions
//! meaningful (the name is legal, the name is refused when it would not fit,
//! the name is not computed from a clock or a status) or to make the horizon
//! and `suspend` arms checkable.

use chrono::{DateTime, TimeZone, Utc};
use kube::CustomResourceExt as _;
use weirkeeper::backup_execution::{
    execution_identity, runner_argv, ExecutionTrigger, ALLOWED_CLUSTERS_PATH, OUT_PATH,
    RECEIPT_OUT_PATH, RUNNER_ARGV_ANNOTATION, SIGNING_KEY_PATH, SPEC_PATH, TRIGGER_SCHEDULE,
};
use weirkeeper::conditions::apply_merge_patch;
use weirkeeper::controllers::backup_schedule::{
    decide, reconcile_schedule, refine_against_status, scheduled_backup, status_patch,
    ScheduleOutcome, SlotDecision, MISSED_SLOT_HORIZON, REASON_CONCURRENCY_BLOCKED,
    REASON_SCHEDULED, REASON_SLOT_MISSED, REASON_SUSPENDED, REQUEUE_SECS, SCHEDULE_LABEL,
    SLOT_LABEL, TRIGGERED_BY_SCHEDULE,
};
use weirkeeper::crds::backup_schedule::{
    BackupSchedule, BackupScheduleStatus, ConcurrencyPolicy, SOURCE_REF_IMMUTABLE_RULE,
};
use weirkeeper::crds::LocalRef;
use weirkeeper::slot::{
    backup_id_for, scheduled_backup_name, slot_name, Cron, SlotError, NAME_LIMIT,
};
use weirkeeper::testing::{mock_client_recording, mock_client_recording_bodies, Route, SeenBody};

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

/// The task's namespace (STANDING RULE 13).
const NS: &str = "logweir-t18";

/// The schedule's UID, as the API server would have minted it.
const UID: &str = "3f1c8a5e-0000-4000-8000-000000000001";

/// A second UID, for the two-namespaces-one-name case.
const OTHER_UID: &str = "3f1c8a5e-0000-4000-8000-000000000002";

/// A `BackupSchedule` body the double answers a `GET` with, and that the pure
/// functions take by value.
///
/// `17 3 * * 1` — 03:17 UTC every Monday — is the brief's own expression, so
/// the instants in this file line up with `cron_last_fire_is_utc_and_stable`.
fn schedule_json(name: &str, uid: &str, cron: &str, suspend: bool) -> String {
    format!(
        r#"{{
  "apiVersion": "logweir.dev/v1alpha1",
  "kind": "BackupSchedule",
  "metadata": {{
    "name": "{name}",
    "namespace": "{NS}",
    "uid": "{uid}",
    "resourceVersion": "17",
    "generation": 4
  }},
  "spec": {{
    "schedule": "{cron}",
    "sourceRef": {{ "name": "prod" }},
    "topics": ["orders", "payments"],
    "archive": {{ "url": "s3://kafka-backups/logweir" }},
    "concurrencyPolicy": "Allow",
    "suspend": {suspend}
  }}
}}"#
    )
}

/// The fixture as a typed object.
fn schedule(name: &str, uid: &str, cron: &str, suspend: bool) -> BackupSchedule {
    serde_json::from_str(&schedule_json(name, uid, cron, suspend))
        .expect("the fixture is a BackupSchedule")
}

fn forbid_schedule(name: &str, uid: &str, cron: &str, suspend: bool) -> BackupSchedule {
    let mut schedule = schedule(name, uid, cron, suspend);
    schedule.spec.concurrency_policy = ConcurrencyPolicy::Forbid;
    schedule
}

fn backup_value(
    name: &str,
    owner_uid: &str,
    phase: Option<&str>,
    job_ref: Option<&str>,
) -> serde_json::Value {
    let mut status = serde_json::Map::new();
    if let Some(phase) = phase {
        status.insert("phase".to_string(), serde_json::json!(phase));
    }
    if let Some(job) = job_ref {
        status.insert("jobRef".to_string(), serde_json::json!({ "name": job }));
    }
    serde_json::json!({
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "Backup",
        "metadata": {
            "name": name,
            "namespace": NS,
            "uid": format!("backup-{name}"),
            "resourceVersion": "41",
            "labels": {
                "logweir.dev/schedule": "nightly",
                "logweir.dev/schedule-uid": owner_uid
            }
            // NO `ownerReferences` SINCE PLAT-05.2 (D1 §6.1). This fixture is
            // what THIS controller creates, and it stopped making its runs the
            // schedule's dependents: membership is `spec.scheduleRef {name,
            // uid}` below, which survives the schedule being deleted. The
            // LEGACY shape — a controller ownerReference and a `scheduleRef`
            // with no UID — is what `tests/schedule_history.rs` builds, because
            // migrating it is that file's subject.
        },
        "spec": {
            "sourceRef": { "name": "prod" },
            "topics": ["orders"],
            "archive": { "url": "s3://kafka-backups/logweir" },
            "scheduleRef": { "name": "nightly", "uid": owner_uid },
            "slot": name.get(name.len().saturating_sub(15)..).unwrap_or_default(),
            "triggeredBy": "schedule",
            "deadlineSeconds": 3600
        },
        "status": serde_json::Value::Object(status)
    })
}

fn backup_list_body(items: Vec<serde_json::Value>) -> String {
    serde_json::json!({
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "BackupList",
        "metadata": { "resourceVersion": "41" },
        "items": items
    })
    .to_string()
}

/// A UTC instant, spelled as five integers so a test reads like a calendar.
fn utc(y: i32, m: u32, d: u32, h: u32, min: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(y, m, d, h, min, 0)
        .single()
        .expect("the fixture instant exists")
}

/// The body a successful `POST …/backups` is answered with — the API server
/// echoing the created object.
///
/// IT CARRIES `spec.trigger` AND `spec.scheduleRef.uid`, AND THE RECONCILER
/// READS THEM. D1 §4.9's CRD-before-controller guard checks its own write
/// responses: the API server silently PRUNES a field an older CRD does not
/// declare, so a created `Backup` that comes back without its identity fields
/// means the CRD is behind the controller, and the schedule stops admitting
/// rather than creating runs that execute under an ambiguous identity. An echo
/// that dropped them would therefore be indistinguishable from that outage.
fn created_backup_body(name: &str) -> String {
    let slot = name
        .get(name.len().saturating_sub(15)..)
        .unwrap_or_default();
    format!(
        r#"{{"apiVersion":"logweir.dev/v1alpha1","kind":"Backup",
  "metadata":{{"name":"{name}","namespace":"{NS}","uid":"aaaaaaaa-0000-4000-8000-00000000000b"}},
  "spec":{{"sourceRef":{{"name":"prod"}},"topics":["orders"],
    "archive":{{"url":"s3://kafka-backups/logweir"}},"triggeredBy":"schedule",
    "scheduleRef":{{"name":"nightly","uid":"{UID}"}},"slot":"{slot}",
    "trigger":{{"kind":"Scheduled","attempt":0}},
    "deadlineSeconds":3600}}}}"#
    )
}

/// A 409 `AlreadyExists` `Status` body, in the shape the API server sends —
/// which is what `kube` parses `kube::Error::Api(e).code` out of.
fn already_exists_body(name: &str) -> String {
    format!(
        r#"{{"kind":"Status","apiVersion":"v1","status":"Failure",
  "message":"backups.logweir.dev \"{name}\" already exists",
  "reason":"AlreadyExists","code":409}}"#
    )
}

/// A 500 body, for the status write that fails in the guard's first reconcile.
const SERVER_ERROR_BODY: &str = r#"{"kind":"Status","apiVersion":"v1","status":"Failure",
  "message":"etcdserver: request timed out","reason":"InternalError","code":500}"#;

/// The `BackupSchedule` body a `PATCH …/status` is answered with.
fn patched_schedule_body() -> String {
    schedule_json("nightly", UID, "17 3 * * 1", false)
}

/// Midnight every day — the expression whose healthy steady state review
/// finding HIGH-1 is about.
const DAILY: &str = "0 0 * * *";

/// The two routes one reconcile of a [`DAILY`] schedule can need: the
/// `POST …/backups`, answered with `post_status`, and the `PATCH …/status`
/// that follows it either way.
///
/// `post_status` IS 409 FOR EVERY RECONCILE AFTER THE FIRST, because that is
/// what the API server really answers once the slot's `Backup` exists — the
/// mechanism working. A route is present even in the arms that assert zero
/// `POST`s, for the reason `a_missed_slot_older_than_the_horizon_creates_nothing_and_says_so`
/// spells out: a reconcile that DECLINED to create proves more than one that
/// could not.
fn daily_routes(name: &str, post_status: u16) -> Vec<Route> {
    daily_routes_phase(name, post_status, "Succeeded")
}

/// [`daily_routes`], with the phase the slot's existing `Backup` reports.
///
/// THE PHASE IS PART OF THE FIXTURE NOW BECAUSE THE RECONCILER READS IT. D1
/// §4.5 step 6 asks the slot's deterministic object what happened to it —
/// running, succeeded, failed retryably — and decides from the answer, so a
/// table that did not say which was a table describing no particular cluster.
fn daily_routes_phase(name: &str, post_status: u16, phase: &str) -> Vec<Route> {
    // THE FIXTURE HAS TO BE INTERNALLY CONSISTENT NOW, and it was not before.
    // D1 §4.5 step 6 discovers the attempt chain by `GET`ting the slot's
    // deterministic name, so a table that answers that `GET` with a Running
    // object AND expects a 201 from the `POST` describes a cluster in which the
    // run both does and does not exist. `post_status` picks which world this is:
    // 201 means "the slot has not run", 409 means "it has".
    let fired = post_status != 201;
    let existing = backup_value(name, UID, Some(phase), Some(name));
    vec![
        Route {
            method: "GET",
            path_suffix: "/namespaces/logweir-t18/backups",
            status: 200,
            body: backup_list_body(if fired {
                vec![existing.clone()]
            } else {
                vec![]
            }),
        },
        Route {
            method: "POST",
            path_suffix: "/namespaces/logweir-t18/backups",
            status: post_status,
            body: if post_status == 201 {
                created_backup_body(name)
            } else {
                already_exists_body(name)
            },
        },
        Route {
            method: "GET",
            path_suffix: Box::leak(format!("/backups/{name}").into_boxed_str()),
            status: if fired { 200 } else { 404 },
            body: if fired {
                existing.to_string()
            } else {
                not_found_body(name)
            },
        },
        Route {
            method: "PATCH",
            path_suffix: "/backupschedules/nightly/status",
            status: 200,
            body: reservation_echo(
                &schedule_json("nightly", UID, DAILY, false),
                name,
                name.get(name.len().saturating_sub(15)..)
                    .unwrap_or_default(),
                0,
            ),
        },
    ]
}

/// The body the API server answers a reservation `PATCH` with: the object, plus
/// the reservation it has just accepted.
///
/// D1 §4.9's CRD-BEFORE-CONTROLLER GUARD READS THIS BACK. The API server
/// silently PRUNES a field the installed CRD does not declare, so the
/// controller checks its own write response for `status.pendingRun` and admits
/// nothing when it is missing — a `Backup` created without its identity fields
/// would have neither `scheduleRef.uid` nor an ownerReference and would be
/// refused `ScheduledIdentityMismatch` before it ever ran. A double that echoed
/// the PRE-patch object therefore looks exactly like an outdated CRD, which is
/// the guard working, and is why this helper exists.
fn reservation_echo(schedule_body: &str, name: &str, slot: &str, attempt: i32) -> String {
    let mut v: serde_json::Value =
        serde_json::from_str(schedule_body).expect("a schedule fixture is JSON");
    let generation = v["metadata"]["generation"].clone();
    v["status"]["pendingRun"] = serde_json::json!({
        "name": name,
        "slot": slot,
        "attempt": attempt,
        "kind": if attempt == 0 { "Scheduled" } else { "Retry" },
        "generation": generation,
    });
    v.to_string()
}

/// The `Backup` name of the slot due at `now`, for the one-minute schedules
/// this file's concurrency tests use.
fn due_name(now: DateTime<Utc>) -> String {
    scheduled_backup_name("nightly", &slot_name(now)).expect("the fixture name fits")
}

/// A 404 `Status` body, in the shape the API server sends.
fn not_found_body(name: &str) -> String {
    format!(
        r#"{{"kind":"Status","apiVersion":"v1","status":"Failure",
  "message":"backups.logweir.dev \"{name}\" not found",
  "reason":"NotFound","code":404}}"#
    )
}

/// The `GET` that answers "this attempt does not exist".
///
/// D1 §4.5 STEP 6 DISCOVERS THE ATTEMPT CHAIN BY NAME, not by listing: it
/// `GET`s `name(S, 0)`, `name(S, 1)`, … and stops at the first 404. A 404 is a
/// real API-server answer and has to be recorded as one — the double PANICS on
/// a request it has no route for, precisely so that "the reconciler asked for
/// exactly these objects" stays a property a test can see.
fn absent_backup(name: &str) -> Route {
    Route {
        method: "GET",
        path_suffix: Box::leak(format!("/backups/{name}").into_boxed_str()),
        status: 404,
        body: not_found_body(name),
    }
}

/// The owned-`Backup` LIST that D1 §4.5 step 1 makes when `status.activeRuns`
/// has never been written — the bootstrap branch PLAT-05.2's inventory
/// replaces.
fn no_backups() -> Route {
    Route {
        method: "GET",
        path_suffix: "/namespaces/logweir-t18/backups",
        status: 200,
        body: backup_list_body(vec![]),
    }
}

/// Every `POST` the double was asked for, in order.
fn posts(bodies: &[SeenBody]) -> Vec<&SeenBody> {
    bodies.iter().filter(|b| b.method == "POST").collect()
}

/// `metadata.name` out of a recorded request body.
fn body_name(seen: &SeenBody) -> String {
    let v: serde_json::Value =
        serde_json::from_str(&seen.body).expect("a recorded POST body is JSON");
    v["metadata"]["name"]
        .as_str()
        .expect("a created object carries metadata.name")
        .to_string()
}

/// The one `status` object out of the recorded **finalization** `PATCH` bodies.
fn patched_status(bodies: &[SeenBody]) -> serde_json::Value {
    patched_status_opt(bodies).expect("the reconciler patches /status")
}

/// Whether a recorded `/status` patch body is a slot RESERVATION.
///
/// BOTH STATUS WRITES ARE NOW `PATCH`es — the admission reservation used to be
/// an `Api::replace_status`, which is a `PUT` the API server authorises as the
/// verb `update` on `backupschedules/status`, and the shipped ClusterRole
/// grants only `patch` there (W0). So a test can no longer tell the two apart
/// by method, and tells them apart by CONTENT instead: only a reservation
/// writes a `status.pendingBackupRef` OBJECT. A finalization either clears that
/// reference (explicit `null`, [`PendingRefUpdate::Clear`]) or leaves it alone
/// (absent key), and there is no third writer of the field in the reconciler.
fn is_reservation(status: &serde_json::Value) -> bool {
    status["pendingBackupRef"].is_object()
}

/// Every `/status` patch body's `status`, in the order the double saw them.
fn status_patches(bodies: &[SeenBody]) -> Vec<serde_json::Value> {
    bodies
        .iter()
        .filter(|b| b.method == "PATCH")
        .map(|b| {
            let v: serde_json::Value =
                serde_json::from_str(&b.body).expect("a recorded PATCH body is JSON");
            v["status"].clone()
        })
        .collect()
}

/// The reservation patch's `status`, or `None` when this reconcile made none.
fn reserved_status_opt(bodies: &[SeenBody]) -> Option<serde_json::Value> {
    status_patches(bodies).into_iter().find(is_reservation)
}

/// The reservation patch's `status`, which this reconcile is asserted to make.
fn reserved_status(bodies: &[SeenBody]) -> serde_json::Value {
    reserved_status_opt(bodies).expect("the reconciler reserves the slot before creating it")
}

/// The `status` object out of the recorded `PATCH` bodies, or `None` when the
/// reconciler sent NO patch.
///
/// TASK 16b. A reconcile whose computed status equals the one already on the
/// object now sends nothing at all (plan erratum E11(d), review findings
/// H-1/H-2/M-1) — a steady `BackupSchedule` on a 30 s requeue used to issue
/// 2,880 identical `PATCH`es a day, and one with an archive configured spun,
/// because `retentionReport.evaluatedAt = now` made each of them a change.
/// "No patch" is now an expected outcome, so it is a `None` a test can assert
/// on rather than a panic inside a helper.
/// W0: THE FINALIZATION AND NOT THE RESERVATION. A `Forbid` admission now
/// sends two `PATCH`es to the same path — the reservation, then the
/// finalization — so "the first PATCH" would silently become the reservation
/// for every admitting test, and every assertion about the settled status
/// would be about a body written before the child existed. [`is_reservation`]
/// is the discriminator, and the finalization is the last body that is not one.
fn patched_status_opt(bodies: &[SeenBody]) -> Option<serde_json::Value> {
    status_patches(bodies)
        .into_iter()
        .filter(|status| !is_reservation(status))
        .next_back()
}

/// How many `/status` `PATCH`es the double was asked for — reservations
/// included, because the property every caller of this helper has is "how many
/// times did this reconcile write status at all".
fn patch_count(bodies: &[SeenBody]) -> usize {
    bodies.iter().filter(|b| b.method == "PATCH").count()
}

/// This file's own source, for the source-reading assertions.
fn source_of(relative: &str) -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(relative);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

/// A file's text, read relative to the WORKSPACE ROOT rather than to this
/// crate: `crates/logweir/src/cli.rs` belongs to another crate and
/// `the_backup_id_override_is_passed_not_defined` asserts on its shape.
///
/// IT PANICS RATHER THAN SKIPPING. A source-shape assertion that cannot find
/// its source has not passed — it has not run. That is review finding HIGH-2's
/// second half: the assertion this helper replaces wrapped a `git` invocation
/// in `if let Ok(out) { if out.status.success() {`, so it passed SILENTLY
/// wherever git was absent and failed hard wherever git worked and the base
/// commit had moved.
fn workspace_source(relative: &str) -> String {
    let path = workspace_root().join(relative);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

/// The workspace root: two levels above this crate's manifest directory.
fn workspace_root() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("the workspace root is two levels above this crate")
        .to_path_buf()
}

/// Every `.rs` file under `crates/weirkeeper/src/`, as `(path relative to
/// `src/`, text)`.
///
/// READ OFF THE CHECKED-IN TREE, WITH NO `git` AND NO BASE COMMIT. What the
/// assertion needs to know is a property of the source as it stands — this
/// crate defines no CLI flag — and that property is true or false in the
/// working tree alone, whatever any commit before it did. A `git diff` against
/// a hard-coded SHA answers a different question and stops answering it the
/// moment the branch rebases (review finding HIGH-2).
fn weirkeeper_sources() -> Vec<(String, String)> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut out = Vec::new();
    let mut stack = vec![root.clone()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).unwrap_or_else(|e| panic!("{}: {e}", dir.display())) {
            let path = entry.expect("a readable directory entry").path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|e| e == "rs") {
                let text = std::fs::read_to_string(&path)
                    .unwrap_or_else(|e| panic!("{}: {e}", path.display()));
                let relative = path
                    .strip_prefix(&root)
                    .expect("every file came from under src/")
                    .to_string_lossy()
                    .into_owned();
                out.push((relative, text));
            }
        }
    }
    assert!(
        out.len() >= 5,
        "the walk found only {} source files under crates/weirkeeper/src — a walk that finds \
         nothing asserts nothing",
        out.len()
    );
    out
}

/// The body of the first `fn` whose signature contains `needle`, by brace
/// matching from the signature's opening `{`.
///
/// A HAND-WRITTEN BRACE MATCH AND NOT A REGEX, for the same reason
/// `scripts/check-no-oso.sh`'s check A is a Python paren matcher: brace
/// matching is not a regular language, and a regex that pretends otherwise
/// makes a source-reading gate report "ok" on a violation.
fn fn_body(src: &str, needle: &str) -> String {
    let at = src
        .find(needle)
        .unwrap_or_else(|| panic!("the source contains no {needle:?}"));
    let open = at + src[at..].find('{').expect("the fn has a body");
    let bytes = src.as_bytes();
    let mut depth = 0usize;
    for (i, b) in bytes.iter().enumerate().skip(open) {
        match b {
            b'{' => depth += 1,
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    return src[open..=i].to_string();
                }
            }
            _ => {}
        }
    }
    panic!("the body of {needle:?} is unbalanced");
}

// ---------------------------------------------------------------------------
// G-SLOT
// ---------------------------------------------------------------------------

/// **G-SLOT.** A crash between the `create` and the status write yields
/// EXACTLY ONE `Backup`.
///
/// Reconcile once with the `POST` answered 201 and the following `PATCH
/// …/status` answered **500** — the crash. Reconcile a second time at a
/// **later** `now` inside the same cron slot, with the `POST` answered **409
/// AlreadyExists**. Two `POST`s were made, both carried the identical
/// `metadata.name`, and the second reconcile returned `Ok`: the 409 is success,
/// so exactly one object exists.
///
/// WHY THE SECOND `now` IS LATER. It is what separates a name derived from the
/// trigger from a name derived from a reconcile-time clock. Both instants sit
/// inside the slot that fired at 2026-09-07T03:17:00Z, so a pure function of
/// the trigger returns one name for both while `slot_name(Utc::now())` returns
/// two.
#[tokio::test]
async fn a_crash_between_create_and_status_write_yields_exactly_one_backup() {
    let schedule = schedule("nightly", UID, "17 3 * * 1", false);
    let due = utc(2026, 9, 7, 3, 17);
    let slot = slot_name(due);
    let expected = scheduled_backup_name("nightly", &slot).expect("the fixture name fits");

    // ---- ARM 1: the RESERVATION is refused -----------------------------
    //
    // The admission is a resourceVersion-conditional status PATCH that happens
    // BEFORE the create, so a status write that does not land is a run that was
    // never created. This arm is stronger than the one it replaces: it is no
    // longer "the fire was recorded late", it is "nothing exists to record".
    let (client, _calls, bodies) = mock_client_recording_bodies(vec![
        no_backups(),
        absent_backup(&expected),
        Route {
            method: "POST",
            path_suffix: "/namespaces/logweir-t18/backups",
            status: 201,
            body: created_backup_body(&expected),
        },
        Route {
            method: "PATCH",
            path_suffix: "/backupschedules/nightly/status",
            status: 500,
            body: SERVER_ERROR_BODY.to_string(),
        },
    ]);
    let refused = reconcile_schedule(&schedule, &client, due).await;
    assert!(
        refused.is_err(),
        "a reservation answered 500 must be reported and requeued, never swallowed: {refused:?}"
    );
    assert!(
        posts(&bodies.lock().expect("readable")).is_empty(),
        "NOTHING is created when the reservation could not be recorded — a POST route is \
         present and was not used, so this is a choice and not an inability"
    );

    // ---- ARM 2: the create succeeds, the FINALIZATION is lost -----------
    //
    // The controller dies after the POST. Its next reconcile reads the same
    // pre-crash object (no `lastFireTime`, the reservation still outstanding)
    // and finds the slot's run by GETTING THE NAME IT WOULD HAVE MINTED. It
    // POSTs nothing, and reports the run it found.
    let (client, _calls, bodies) = mock_client_recording_bodies(admitting_routes(
        Box::leak(expected.clone().into_boxed_str()),
        201,
        serde_json::to_string(&schedule).expect("the fixture serialises"),
    ));
    let first = reconcile_schedule(&schedule, &client, due)
        .await
        .expect("the slot fires");
    let first_posts: Vec<String> = posts(&bodies.lock().expect("readable"))
        .iter()
        .map(|b| body_name(b))
        .collect();
    assert_eq!(
        first_posts,
        vec![expected.clone()],
        "the first reconcile POSTs exactly one Backup, named from the trigger"
    );
    assert_eq!(first.created, Some(expected.clone()));

    let mut crashed = schedule.clone();
    crashed.status = Some(BackupScheduleStatus {
        pending_backup_ref: Some(LocalRef {
            name: expected.clone(),
        }),
        ..BackupScheduleStatus::default()
    });
    let (client, calls, bodies) = mock_client_recording_bodies(vec![
        Route {
            method: "GET",
            path_suffix: "/namespaces/logweir-t18/backups",
            status: 200,
            body: backup_list_body(vec![backup_value(
                &expected,
                UID,
                Some("Running"),
                Some(&expected),
            )]),
        },
        Route {
            method: "GET",
            path_suffix: Box::leak(format!("/backups/{expected}").into_boxed_str()),
            status: 200,
            body: backup_value(&expected, UID, Some("Running"), Some(&expected)).to_string(),
        },
        Route {
            method: "POST",
            path_suffix: "/namespaces/logweir-t18/backups",
            status: 409,
            body: already_exists_body(&expected),
        },
        Route {
            method: "PATCH",
            path_suffix: "/backupschedules/nightly/status",
            status: 200,
            body: patched_schedule_body(),
        },
    ]);
    let resumed = reconcile_schedule(&crashed, &client, due + chrono::Duration::minutes(27))
        .await
        .expect("a resumed reconcile is not an error");
    let seen = calls.lock().expect("readable").clone();
    assert!(
        posts(&bodies.lock().expect("readable")).is_empty(),
        "the resumed reconcile creates NOTHING: the reservation names an object that exists, \
         so it is cleared rather than acted on. A POST route was available. Calls: {seen:?}"
    );
    assert_eq!(
        resumed.created,
        Some(expected.clone()),
        "and it reports the run this slot has, so the fire is recorded exactly once"
    );
    assert!(resumed.already_existed);
    assert_eq!(resumed.decision.reason(), REASON_SCHEDULED);
    let resumed_status = patched_status(&bodies.lock().expect("readable"));
    assert_cleared(&resumed_status, "pendingBackupRef");
    assert_cleared(&resumed_status, "pendingRun");

    // ---- ARM 3: the LIST is stale, so the 409 is the idempotence key -----
    //
    // The reservation is outstanding and the namespace listing has not caught
    // up with the object the crashed process created, so the resume path does
    // what it is for and POSTs the reserved name. That is the one case where a
    // second POST is still made, and it is the case the whole design turns on:
    // BOTH POSTs carry the identical `metadata.name`, so the API server answers
    // 409 and exactly one object exists. Two different names is a name derived
    // from a reconcile-time clock or from `status.lastFireTime`, and it produces
    // two partial archives under colliding backup_ids.
    let (client, _calls, bodies) = mock_client_recording_bodies(vec![
        no_backups(),
        Route {
            method: "GET",
            path_suffix: Box::leak(format!("/backups/{expected}").into_boxed_str()),
            status: 200,
            body: backup_value(&expected, UID, Some("Running"), Some(&expected)).to_string(),
        },
        Route {
            method: "POST",
            path_suffix: "/namespaces/logweir-t18/backups",
            status: 409,
            body: already_exists_body(&expected),
        },
        Route {
            method: "PATCH",
            path_suffix: "/backupschedules/nightly/status",
            status: 200,
            body: patched_schedule_body(),
        },
    ]);
    let stale = reconcile_schedule(&crashed, &client, due + chrono::Duration::minutes(31))
        .await
        .expect(
            "409 AlreadyExists IS the idempotence key, so a stale read must return Ok. An Err \
             here means a duplicate reconcile is reported as a failure and the schedule never \
             records its fire.",
        );
    let stale_posts: Vec<String> = posts(&bodies.lock().expect("readable"))
        .iter()
        .map(|b| body_name(b))
        .collect();
    assert_eq!(stale_posts, vec![expected.clone()]);
    assert_eq!(
        stale,
        ScheduleOutcome {
            decision: SlotDecision::Due {
                due,
                slot: slot.clone(),
                name: expected.clone(),
                next_fire_time: Some(utc(2026, 9, 14, 3, 17)),
            },
            created: Some(expected.clone()),
            already_existed: true,
        },
        "the collision is reported as `already_existed`, not as an error, and the decision \
         still names the slot and the object it resumed"
    );

    // THE PROPERTY. Both POSTs, made in two different reconciles at two
    // different instants inside one slot, carry the identical name.
    let all: Vec<String> = first_posts.into_iter().chain(stale_posts).collect();
    assert_eq!(all.len(), 2, "two POSTs across the two reconciles: {all:?}");
    assert_eq!(
        all[0], all[1],
        "BOTH POSTs must carry the identical metadata.name — that identity is what makes the \
         second one a 409 and therefore what makes exactly one object exist."
    );
    assert_eq!(
        all[0], expected,
        "the name is `logweir-backup-<schedule>-<slot>` computed from the fired slot"
    );
}

/// The object name is a DNS-1123 subdomain, asserted on the string that
/// actually becomes an object name.
///
/// TWO STRINGS, AND THE FIRST ONE IS THE POINT (critique B L6). The regex is
/// asserted on `scheduled_backup_name("nightly", &slot_name(t))?` — the value
/// that goes into `metadata.name` — and separately on `slot_name(t)`. A test
/// that checked only the slot would pass for a composed name that was illegal.
/// Parameterised over twelve instants including midnight, a leap day and a
/// second boundary.
#[test]
fn the_object_name_is_a_dns1123_subdomain() {
    let instants = [
        // midnight, and the second before and after it
        Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
        Utc.with_ymd_and_hms(2025, 12, 31, 23, 59, 59).unwrap(),
        Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 1).unwrap(),
        // a leap day, at both ends
        Utc.with_ymd_and_hms(2024, 2, 29, 0, 0, 0).unwrap(),
        Utc.with_ymd_and_hms(2024, 2, 29, 23, 59, 59).unwrap(),
        // a second boundary either side of noon
        Utc.with_ymd_and_hms(2026, 6, 30, 11, 59, 59).unwrap(),
        Utc.with_ymd_and_hms(2026, 6, 30, 12, 0, 0).unwrap(),
        // the brief's own slot
        Utc.with_ymd_and_hms(2026, 9, 7, 14, 5, 0).unwrap(),
        Utc.with_ymd_and_hms(2026, 9, 7, 3, 17, 0).unwrap(),
        // single-digit month and day, which is where a missing zero-pad shows
        Utc.with_ymd_and_hms(2026, 3, 4, 5, 6, 7).unwrap(),
        // the far ends of a plausible archive lifetime
        Utc.with_ymd_and_hms(1970, 1, 1, 0, 0, 0).unwrap(),
        Utc.with_ymd_and_hms(2099, 12, 31, 23, 59, 59).unwrap(),
    ];
    assert_eq!(
        instants.len(),
        12,
        "twelve instants, as the acceptance says"
    );

    for t in instants {
        let slot = slot_name(t);
        let name = scheduled_backup_name("nightly", &slot).expect("`nightly` fits inside 63");

        for (what, s) in [("the object name", &name), ("the slot", &slot)] {
            assert!(
                is_dns1123(s),
                "{what} {s:?} (for {t}) must match ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ — a \
                 Kubernetes object name is a DNS-1123 subdomain and an illegal one is refused \
                 by the API server, so the Backup would never be created at all"
            );
            assert!(
                !s.contains('T') && !s.contains('Z'),
                "{what} {s:?} must contain neither `T` nor `Z`: uppercase is rejected in a \
                 DNS-1123 subdomain. The `YYYYmmddTHHMMSSZ` form is for Kafka TOPIC names \
                 (Task 9b), which permit it."
            );
        }
        assert_eq!(
            slot.len(),
            15,
            "the slot is `yyyymmdd-hhmmss`: got {slot:?}"
        );
    }
}

/// `^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`, hand-written.
///
/// THE PATTERN IS SPELLED IN THIS DOC COMMENT AND IMPLEMENTED BELOW rather
/// than compiled by a crate: Global Constraint 38 closes the workspace graph
/// and `regex` is not in it, and this particular pattern is four lines of
/// character tests.
fn is_dns1123(s: &str) -> bool {
    let ok = |c: char| c.is_ascii_lowercase() || c.is_ascii_digit();
    let mut chars = s.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !ok(first) {
        return false;
    }
    let rest: Vec<char> = chars.collect();
    if let Some(last) = rest.last() {
        if !ok(*last) {
            return false;
        }
    }
    rest.iter().all(|&c| ok(c) || c == '-')
}

/// A schedule name that would push the object name past 63 characters is
/// refused, naming the limit.
///
/// WHY 63 AND NOT 253. A Kubernetes object *name* may be 253 characters; a
/// **label value** may be 63. The runner pod's `batch.kubernetes.io/job-name`
/// label is derived from this name, so a longer one yields pods that cannot be
/// labelled — pods Task 17's reconciler could never find by label selector, and
/// an exit code it could never read.
#[test]
fn a_schedule_name_that_would_exceed_sixty_three_characters_is_refused() {
    let long = "n".repeat(52);
    assert_eq!(long.len(), 52, "a 52-character schedule name");
    let slot = slot_name(utc(2026, 9, 7, 14, 5));

    let got = scheduled_backup_name(&long, &slot);
    // 15 (`logweir-backup-`) + 52 + 1 (`-`) + 15 (the slot) = 83.
    assert_eq!(
        got,
        Err(SlotError::NameTooLong { limit: 63, got: 83 }),
        "a 52-character schedule name must be REFUSED, naming the limit — returning the long \
         name instead produces a Backup whose pods cannot be labelled"
    );
    assert_eq!(NAME_LIMIT, 63, "the limit is the label-value cap");

    let message = got.expect_err("refused above").to_string();
    for needle in ["63", "83", "batch.kubernetes.io/job-name"] {
        assert!(
            message.contains(needle),
            "the refusal must name {needle:?} so an operator can act on it. Got: {message}"
        );
    }

    // The boundary, from the other side: 32 characters is the most that fits,
    // and 33 is the first that does not.
    assert_eq!(
        scheduled_backup_name(&"n".repeat(32), &slot)
            .expect("32 fits")
            .len(),
        63
    );
    assert!(scheduled_backup_name(&"n".repeat(33), &slot).is_err());
}

/// The name never reads a reconcile clock or a status.
///
/// A SOURCE-READING TEST, because the property is about what the code CANNOT
/// do. `decide` is where the name is minted, and it must compute it from
/// `slot_name(due)` — the fired slot — with no clock read anywhere between
/// `last_fire_at_or_before` and the `POST`.
#[test]
fn the_name_never_reads_a_reconcile_clock_or_a_status() {
    let src = source_of("src/controllers/backup_schedule.rs");

    let decide_body = fn_body(&src, "pub fn decide(");
    assert!(
        decide_body.contains("let slot = slot_name(due);"),
        "`decide` must compute the slot from the DUE instant — `slot_name(due)` — because the \
         name is a pure function of the trigger"
    );
    let slot_at = decide_body.find("slot_name(due)").expect("asserted above");
    let name_at = decide_body
        .find("scheduled_backup_name(name, &slot)")
        .expect("`decide` must mint the name with `scheduled_backup_name(name, &slot)`");
    assert!(
        slot_at < name_at,
        "the slot is computed from `due` BEFORE the name is minted from it"
    );
    // THE DUE SLOT COMES FROM THE CADENCE ENGINE, WHICH IS ALSO CLOCK-FREE.
    // PLAT-04.2 moved the walk behind `Cadence`, so that a zoned expression and
    // a UTC one answer the same question in the same place; the ORDER is what
    // this assertion is about and it is unchanged — the instant, then the slot
    // string, then the name.
    let due_at = decide_body
        .find("latest_due_slot(now)")
        .expect("`decide` must take the due slot from `Cadence::latest_due_slot`");
    assert!(
        due_at < slot_at,
        "the due slot comes from `latest_due_slot`, then the slot string, then the name"
    );
    for forbidden in ["Utc::now()", "lastFireTime", "last_fire_time", ".status"] {
        assert!(
            !decide_body.contains(forbidden),
            "`decide` must not name {forbidden:?}: a name derived from a reconcile-time clock \
             or from status.lastFireTime produces TWO objects when the controller crashes \
             between the create and the status write"
        );
    }

    // TASK 19 MOVED THIS BODY, AND THE NEEDLE FOLLOWED IT. The
    // API-server-facing half is now `reconcile_schedule_with_archive`, which
    // carries the controller's read-only archive handle for the retention
    // report; `reconcile_schedule` is that function with `None`, so Task 18's
    // three-argument contract is unchanged for every caller. The property this
    // test is about — no clock read, decision before the POST, status write
    // after it — is asserted over the body that now holds it, and the
    // delegation is PINNED below so the three-argument form cannot quietly
    // grow a second implementation.
    // D3 W9 MOVED IT AGAIN, AND THE NEEDLE FOLLOWED IT AGAIN. The
    // API-server-facing half is now `reconcile_schedule_with_archive_at`, which
    // takes the controller's own archive LOCATION beside its handle so that
    // "the handle points at a different destination" (D3 §6.3, defect
    // RET-WRONGBUCKET) is a comparison a test can construct rather than a
    // `std::env::var` behind an `async fn`. Both shorter forms are pinned below
    // as pure delegations, so the ordering this test asserts cannot come to hold
    // in only one of three implementations.
    let reconcile_body = fn_body(&src, "pub async fn reconcile_schedule_with_archive_at(");
    let delegating_body = fn_body(&src, "pub async fn reconcile_schedule(");
    assert!(
        delegating_body.contains("reconcile_schedule_with_archive(schedule, client, None, now)"),
        "`reconcile_schedule` must be `reconcile_schedule_with_archive` with no archive handle \
         and nothing else: two implementations of one reconcile is how the ordering this test \
         asserts comes to hold in only one of them. Got:\n{delegating_body}"
    );
    let archive_body = fn_body(&src, "pub async fn reconcile_schedule_with_archive(");
    assert!(
        archive_body.contains("reconcile_schedule_with_archive_at(schedule, client, archive,"),
        "`reconcile_schedule_with_archive` must be `…_at` with the configured location read \
         once and nothing else. Got:\n{archive_body}"
    );
    assert!(
        !reconcile_body.contains("Utc::now()"),
        "`reconcile_schedule` — the half that talks to the API server — must contain no \
         `Utc::now()` at all: the instant arrives as an argument, decided once before the \
         reconcile begins. A clock read between `decide` and the POST is the mutant G-SLOT \
         kills."
    );
    // THE ORDERING NOW SPANS THREE FUNCTIONS, AND THE PROPERTY IS THE SAME
    // ONE. D1 §4.5's algorithm has ten exits, so the create and the final
    // status write are named helpers rather than inline code — `admit` reserves
    // and then creates, `create_run` is the only `POST`, and `finish` is the
    // only final `/status` write. The assertions follow them there, because a
    // property asserted over a body the code no longer has is not asserted at
    // all.
    let decide_call = reconcile_body
        .find("decide(&name, &schedule.spec, now)")
        .expect(
            "`reconcile_schedule_with_archive` must call `decide` with the instant it was \
                 handed",
        );
    let first_admission = reconcile_body
        .find("admit(")
        .or_else(|| reconcile_body.find("create_run("))
        .expect("the reconcile must admit or resume a run somewhere");
    assert!(
        decide_call < first_admission,
        "the decision — and therefore the name — precedes every admission"
    );

    let create_body = fn_body(&src, "async fn create_run(");
    assert!(
        create_body.contains(".create(&PostParams::default(), &backup)"),
        "`create_run` is the ONE place a Backup is POSTed: a second `Api::create` anywhere in \
         this file is a second way for a slot to be named. Got:\n{create_body}"
    );
    assert!(
        !create_body.contains("patch_status("),
        "`create_run` writes no status: the status write happens AFTER the create, in `finish`"
    );

    let admit_body = fn_body(&src, "async fn admit(");
    let reserve_at = admit_body
        .find("patch_status(")
        .expect("`admit` reserves the slot with a status PATCH before creating anything");
    let create_at = admit_body
        .find("create_run(")
        .expect("`admit` creates the run after reserving it");
    assert!(
        reserve_at < create_at,
        "THE RESERVATION COMES FIRST, for every trigger kind and both concurrency policies. It \
         carries `metadata.resourceVersion`, so an edit that lands in the window answers 409 \
         and nothing is created under a policy the schedule no longer has."
    );

    let finish_body = fn_body(&src, "async fn finish(");
    assert!(
        finish_body.contains("patch_status("),
        "`finish` is the final status write, and every exit of the algorithm goes through it"
    );
    assert!(
        !finish_body.contains(".create(&PostParams::default()"),
        "the status write happens AFTER the create and never instead of it. That ordering is \
         what makes the crash window harmless: a crash between the two recomputes the same \
         name and collides, where a status-first order would record a fire that never happened."
    );

    // The one clock read in the whole file is in the `kube::runtime` wrapper,
    // before anything is decided. Counted over CODE lines only: this module's
    // own documentation names `Utc::now()` twice, to say where it is and where
    // it must not be, and a count that included prose would be a count of
    // comments.
    let code_reads = src
        .lines()
        .filter(|l| {
            let t = l.trim_start();
            !t.starts_with("//") && t.contains("Utc::now()")
        })
        .count();
    assert_eq!(
        code_reads, 1,
        "this file reads the clock exactly once, in the `kube::runtime` reconcile wrapper. A \
         second read is a second slot."
    );
    let wrapper = fn_body(
        &src,
        "async fn reconcile(\n    schedule: Arc<BackupSchedule>,",
    );
    assert!(
        wrapper.contains("let now = Utc::now();")
            && wrapper.contains(
                "reconcile_schedule_with_archive(&schedule, &ctx.client, ctx.archive.as_ref(), \
                 now)"
            ),
        "the one clock read is the wrapper's, it is bound ONCE, and it is handed straight to \
         `reconcile_schedule_with_archive` as an argument — beside the archive handle Task 19 \
         threads through, which is a value on the context and not a second clock. The binding \
         matters now that the requeue interval is also computed from it (D1 §4.5 step 8): two \
         `Utc::now()` calls in one reconcile would let the decision and the requeue disagree \
         about what time it is. Got:\n{wrapper}"
    );

    // AND `slot.rs` READS NO CLOCK AT ALL (review finding LOW-2). Both the
    // module header and the task report claim it; nothing tested it, and the
    // claim is the load-bearing half of "the name is a pure function of the
    // trigger" — `slot_name` is where a `Utc::now()` would be cheapest to add
    // and hardest to see. Counted over CODE lines, for the reason above.
    let slot_src = source_of("src/slot.rs");
    let slot_clock_reads = slot_src
        .lines()
        .filter(|l| {
            let t = l.trim_start();
            !t.starts_with("//") && t.contains("Utc::now()")
        })
        .count();
    assert_eq!(
        slot_clock_reads, 0,
        "`slot.rs` takes every instant as an argument: a clock read there would put a second \
         slot inside the pure half itself, where `decide`'s own source-reading assertions \
         above could never see it"
    );

    // AND THE STATUS-READING REFINEMENT CANNOT MINT A NAME. `refine_against_last_fire`
    // is the one function in this file that sees `status.lastFireTime` (review
    // finding HIGH-1). It exists so the missed-slot REASON can consult the
    // status while the NAME cannot, and that separation is only real if it
    // never touches the name: it must not call `scheduled_backup_name`, must
    // not call `slot_name`, and must not construct `SlotDecision::Due`.
    let refine_body = fn_body(&src, "pub fn refine_against_status(");
    for forbidden in [
        "scheduled_backup_name",
        "slot_name(",
        "SlotDecision::Due",
        "Utc::now()",
    ] {
        assert!(
            !refine_body.contains(forbidden),
            "`refine_against_status` must not name {forbidden:?}: it refines a REASON \
             against `status.lastFireTime` and `status.policy.effectiveSince`, and a name that \
             read the status would produce two objects when the controller crashes between the \
             create and the status write. It CARRIES a name `decide` already minted; it never \
             mints one."
        );
    }
    assert!(
        refine_body.contains("SlotDecision::AlreadyFired"),
        "the variants it produces are `AlreadyFired` (rows 14 and 19's first half) and \
         `Missed` with reason `BeforeRevision` (row 19), both out of a decision that already \
         holds its name"
    );
    assert!(
        refine_body.contains("MissedReason::BeforeRevision"),
        "and the catch-up half of the refinement is here, not in `decide`: `effectiveSince` is \
         a status field"
    );
    let decide_at = src
        .find("pub fn decide(")
        .expect("`decide` is in this file");
    let refine_at = src
        .find("pub fn refine_against_status(")
        .expect("asserted above");
    assert!(
        decide_at < refine_at,
        "the pure decision comes first and the refinement reads its result: the name is \
         minted before anything consults a status"
    );
}

// ---------------------------------------------------------------------------
// The cron parser
// ---------------------------------------------------------------------------

/// The parser refuses what it does not understand, naming the field.
///
/// A CRON PARSER WHOSE UNKNOWN TOKEN BECOMES `*` turns a typo into "every
/// minute of every day", which for a backup schedule is a broker read a minute
/// forever. Each refusal below names the part it refused.
#[test]
fn cron_parse_refuses_what_it_does_not_understand() {
    // `"L"` — Quartz's last-day-of-month, which this grammar does not have.
    let e = Cron::parse("L * * * *").expect_err("`L` is not a cron field this parser accepts");
    let m = e.to_string();
    assert!(
        m.contains("field 1 (minute)") && m.contains("\"L\"") && m.contains("is not a literal"),
        "the refusal names the FIELD and the term. Got: {m}"
    );

    // Six fields — the seconds-resolution form.
    let e = Cron::parse("* * * * * *").expect_err("six fields is not a five-field expression");
    let m = e.to_string();
    assert!(
        m.contains("five fields") && m.contains("got 6"),
        "the refusal names the five fields and the count it got. (A field COUNT error names \
         the fields rather than one field, because no single field was at fault.) Got: {m}"
    );

    // An `@` form outside the three that are accepted.
    let e = Cron::parse("@yearly").expect_err("@yearly is not accepted");
    let m = e.to_string();
    assert!(
        m.contains("@yearly")
            && m.contains("@hourly")
            && m.contains("@daily")
            && m.contains("@weekly"),
        "the refusal names what was written and the three forms that ARE accepted — reading \
         @yearly as @daily would be 365 unwanted backups a year. Got: {m}"
    );

    // A zero step, which must never be read as `*`.
    let e = Cron::parse("*/0 * * * *").expect_err("a step of 0 is not a step");
    let m = e.to_string();
    assert!(
        m.contains("field 1 (minute)") && m.contains("step of 0") && m.contains("never `*`"),
        "the refusal names the field and says a 0 step is not `*` — making `*/0` parse as `*` \
         is a schedule that fires every minute. Got: {m}"
    );

    // And the four that parse.
    for expr in ["17 3 * * 1", "*/5 * * * *", "0 0,12 * * *", "@daily"] {
        assert!(
            Cron::parse(expr).is_ok(),
            "{expr:?} is a legal expression this parser must accept"
        );
    }

    // A few more refusals, each naming its own field, so the message shape is
    // not an accident of the first case.
    for (expr, needle) in [
        ("60 * * * *", "field 1 (minute)"),
        ("0 24 * * *", "field 2 (hour)"),
        ("0 0 32 * *", "field 3 (day-of-month)"),
        ("0 0 0 * *", "field 3 (day-of-month)"),
        ("0 0 * 13 *", "field 4 (month)"),
        ("0 0 * * 7", "field 5 (day-of-week)"),
        ("0 0 5-1 * *", "field 3 (day-of-month)"),
        ("1,,2 * * * *", "field 1 (minute)"),
        ("1-30/5 * * * *", "field 3"),
        // A LEADING `+` IS NOT A CRON NUMBER (review finding LOW-1). Rust's
        // integer `FromStr` accepts one, so all four of these PARSED before
        // the fix — each to the obvious intent, which is why no firing set was
        // wrong and why it was a LOW — while Vixie refuses all four. A parser
        // documented as five forms must not accept a sixth spelling by
        // accident of the standard library.
        ("+5 * * * *", "field 1 (minute)"),
        ("*/+5 * * * *", "field 1 (minute)"),
        ("+0-+5 * * * *", "field 1 (minute)"),
        ("0 0 * * +1", "field 5 (day-of-week)"),
    ] {
        let e = Cron::parse(expr).map(|_| ()).expect_err(expr);
        let m = e.to_string();
        if expr == "1-30/5 * * * *" {
            // A stepped range is a sixth form this grammar does not have; the
            // term lands in field 1, so the message names field 1.
            assert!(m.contains("field 1 (minute)"), "{expr}: got {m}");
        } else {
            assert!(m.contains(needle), "{expr}: expected {needle:?}, got {m}");
        }
    }
}

/// `last_fire_at_or_before` is UTC and stable.
/// Every day of September 2026 on which `expr` fires, by walking the public
/// `next_fire_after` from the last instant of August.
///
/// A WALK AND NOT A HAND-WRITTEN SET, so the count is the parser's own answer
/// rather than the test's opinion of it.
fn firing_days(expr: &str) -> Vec<u32> {
    use chrono::Datelike as _;
    let cron = Cron::parse(expr).unwrap_or_else(|e| panic!("{expr}: {e}"));
    let end = utc(2026, 10, 1, 0, 0);
    let mut days = Vec::new();
    let mut at = Utc
        .with_ymd_and_hms(2026, 8, 31, 23, 59, 59)
        .single()
        .expect("the walk's start instant exists");
    // Bounded: 44,640 minutes in September, and every step advances.
    for _ in 0..2000 {
        let Some(next) = cron.next_fire_after(at) else {
            break;
        };
        if next >= end {
            break;
        }
        if days.last() != Some(&next.day()) {
            days.push(next.day());
        }
        at = next;
    }
    days
}

/// The day rule reads the FIRST CHARACTER of the day fields, and is cronie's
/// predicate exactly.
///
/// REVIEW FINDING HIGH-3, RULED cronie-EXACT. `restricted` was `field != "*"`,
/// so `*/2` in day-of-month counted as narrow, the union arm of the day rule
/// engaged, and `0 0 */2 * 1` fired on 17 days of September 2026. cronie's
/// `find_jobs` is `(DOM_STAR || DOW_STAR) ? (dom && dow) : (dom || dow)` with
/// the star flags taken from each field's FIRST CHARACTER, which makes that
/// expression the odd-numbered Mondays: **2** days. Fix round 1 got the star
/// flag right and then let a starred-and-narrowed field stand aside, which
/// gave 4; the ruling is that a starred field still restricts, because its
/// step is in the bitset and cronie intersects the bitset in. An adopter's
/// migrated crontab means here what it meant there only if this holds.
///
/// THE COUNTS ARE OVER SEPTEMBER 2026 — 30 days, the 1st a Tuesday, the
/// Mondays the 7th, 14th, 21st and 28th — and the eight rows are the ruling's
/// own eight. `*/2 * 1` (2) and `*/7 * 1` (0) are the two that separate this
/// reading from fix round 1's 4 and 4. `* * 1` (4) and `1-31 * 1` (30) are the
/// controls that agree under both readings and fail if the day rule is simply
/// switched off; `15 * 1` (5) is the union arm, which only two narrow fields
/// reach; `*/2 * *` (15) is a starred field's bits deciding against an
/// all-ones partner; `* * *` (30) and `*/1 * 1` (4) are the ends of the range.
/// The ninth row is retained from fix round 1 because the ruling's eight never
/// pair a narrow day-of-month with a starred day-of-week, and that arm needs a
/// witness too.
#[test]
fn the_day_rule_reads_the_first_character_of_the_field() {
    // September 2026: 30 days, the 1st is a Tuesday, the Mondays are 7, 14, 21
    // and 28, `*/2` in day-of-month is {1, 3, …, 29} (fifteen days, the step
    // counting from the range's first value, 1) and `*/7` is {1, 8, 15, 22,
    // 29} — none of which is a Monday, which is why one row expects nothing.
    let mondays: Vec<u32> = vec![7, 14, 21, 28];
    let every_day: Vec<u32> = (1..=30).collect();
    let odd_days: Vec<u32> = (1..=30).filter(|d| d % 2 == 1).collect();

    for (expr, expected, why) in [
        (
            "0 0 */2 * 1",
            vec![7, 21],
            "`*/2` is STARRED (first character `*`), so cronie intersects: the odd-numbered \
             Mondays. `field != \"*\"` made this 17 days; fix round 1 made it 4",
        ),
        (
            "0 0 */7 * 1",
            vec![],
            "same arm, and the intersection is empty — {1, 8, 15, 22, 29} contains no Monday \
             in this month. `field != \"*\"` made this 9 days; fix round 1 made it 4. A rule \
             that cannot answer NO DAYS is not cronie's",
        ),
        (
            "0 0 */1 * 1",
            mondays.clone(),
            "`*/1` is starred and its bits are every day, so the intersection is the Mondays. \
             `field != \"*\"` made this all 30 — a weekly backup running daily",
        ),
        (
            "0 0 */2 * *",
            odd_days,
            "BOTH fields starred, so still the intersection, and a starred field's bits still \
             decide: fifteen days. Answering `true` for two starred fields — which the \
             pre-fix-round-1 arm did — would fire an every-second-day schedule on all thirty",
        ),
        (
            "0 0 * * 1",
            mondays,
            "a bare `*` day-of-month is starred, so the intersection engages and an all-ones \
             bitset leaves the Mondays alone. Both readings agree",
        ),
        (
            "0 0 1-31 * 1",
            every_day.clone(),
            "NEITHER field is starred — `1-31` begins with `1` — so this is the union arm: \
             every day of the month OR any Monday, which is all 30. This is the row that \
             fails if the union arm is deleted along with fix round 1's predicate",
        ),
        (
            "0 0 15 * 1",
            vec![7, 14, 15, 21, 28],
            "the union arm again, with a count that is neither 4 nor 30: the 15th or any \
             Monday, five days. This is the classic `0 0 1 * 1` shape, which every cron \
             reads as OR",
        ),
        (
            "0 0 * * *",
            every_day,
            "two bare `*`s are still every day: starred, so intersected, and both bitsets \
             are all ones",
        ),
        (
            "0 0 1 * *",
            vec![1],
            "RETAINED CONTROL, not one of the ruling's eight: a narrow day-of-month against \
             a starred day-of-week, the one arm combination the eight never reach. The \
             intersection with an all-ones day-of-week is the 1st. Both readings agree",
        ),
    ] {
        assert_eq!(
            firing_days(expr),
            expected,
            "{expr}: {why}. Got {} days.",
            firing_days(expr).len()
        );
    }
}

#[test]
fn cron_last_fire_is_utc_and_stable() {
    let cron = Cron::parse("17 3 * * 1").expect("the brief's expression parses");
    assert_eq!(
        cron.last_fire_at_or_before(utc(2026, 9, 9, 10, 0)),
        Some(utc(2026, 9, 7, 3, 17)),
        "03:17 UTC on Monday 2026-09-07 is the last firing at or before 2026-09-09T10:00:00Z"
    );

    // STABLE ACROSS THE WHOLE SLOT, which is the property G-SLOT rests on:
    // every instant from the fire until the next one yields the same `due`.
    for t in [
        utc(2026, 9, 7, 3, 17),
        utc(2026, 9, 7, 3, 18),
        utc(2026, 9, 9, 10, 0),
        utc(2026, 9, 14, 3, 16),
    ] {
        assert_eq!(
            cron.last_fire_at_or_before(t),
            Some(utc(2026, 9, 7, 3, 17)),
            "the due slot is stable for every instant inside it ({t})"
        );
    }
    // Seconds are dropped, not rounded up.
    assert_eq!(
        cron.last_fire_at_or_before(
            Utc.with_ymd_and_hms(2026, 9, 7, 3, 17, 59)
                .single()
                .unwrap()
        ),
        Some(utc(2026, 9, 7, 3, 17))
    );
    // `next_fire_after` is STRICTLY after, including within the fired minute.
    assert_eq!(
        cron.next_fire_after(utc(2026, 9, 7, 3, 17)),
        Some(utc(2026, 9, 14, 3, 17))
    );
    // The three `@` forms expand to what they say.
    assert_eq!(
        Cron::parse("@daily")
            .unwrap()
            .next_fire_after(utc(2026, 9, 9, 10, 0)),
        Some(utc(2026, 9, 10, 0, 0))
    );
    assert_eq!(
        Cron::parse("@hourly")
            .unwrap()
            .next_fire_after(utc(2026, 9, 9, 10, 30)),
        Some(utc(2026, 9, 9, 11, 0))
    );
    assert_eq!(
        Cron::parse("@weekly")
            .unwrap()
            .last_fire_at_or_before(utc(2026, 9, 9, 10, 0)),
        Some(utc(2026, 9, 6, 0, 0)),
        "@weekly is Sunday 00:00, and 2026-09-06 is a Sunday"
    );
}

// ---------------------------------------------------------------------------
// The horizon, and `suspend`
// ---------------------------------------------------------------------------

/// A missed slot older than the horizon creates nothing — AND says so.
///
/// TWO HALVES, AND THE SECOND IS CRITIQUE B M20'S. Zero `POST`s is the horizon
/// working; `lastMissedSlot` plus a `SlotMissed` condition is what makes a
/// correct implementation distinguishable from a broken schedule. Skipping the
/// slot without recording it fails the second half.
#[tokio::test]
async fn a_missed_slot_older_than_the_horizon_creates_nothing_and_says_so() {
    let schedule = schedule("nightly", UID, "17 3 * * 1", false);
    // Two hours after the slot that fired at 03:17.
    let now = utc(2026, 9, 7, 5, 17);
    let slot = slot_name(utc(2026, 9, 7, 3, 17));

    let (client, calls, bodies) = mock_client_recording_bodies(vec![
        no_backups(),
        absent_backup(&scheduled_backup_name("nightly", &slot).expect("the fixture name fits")),
        // A POST ROUTE IS PRESENT ON PURPOSE, even though this test asserts that
        // ZERO POSTs are made. Without it the double would PANIC on the
        // unrouted request (see `src/testing.rs`), which fails the test for the
        // right reason but at the wrong place: the assertion that must fire is
        // the zero-POST count, and a test that only proves the reconciler COULD
        // NOT create proves less than one that proves it CHOSE not to.
        Route {
            method: "POST",
            path_suffix: "/namespaces/logweir-t18/backups",
            status: 201,
            body: created_backup_body(
                &scheduled_backup_name("nightly", &slot).expect("the fixture name fits"),
            ),
        },
        Route {
            method: "PATCH",
            path_suffix: "/backupschedules/nightly/status",
            status: 200,
            body: patched_schedule_body(),
        },
    ]);
    let outcome = reconcile_schedule(&schedule, &client, now)
        .await
        .expect("a missed slot is a decision, not an error");

    let seen = calls.lock().expect("the recorder is readable").clone();
    assert_eq!(
        seen.iter().filter(|c| c.method == "POST").count(),
        0,
        "ZERO POST requests, with a POST route available to take one: a controller restarted \
         after a week must not fire the backlog. Dropping the horizon makes this count 1. \
         Calls: {seen:?}"
    );
    assert_eq!(outcome.created, None);

    let status = patched_status(&bodies.lock().expect("the body recorder is readable"));
    assert_eq!(
        status["lastMissedSlot"],
        serde_json::json!(slot),
        "the skipped slot is RECORDED. Without it an adopter cannot tell a schedule that \
         correctly skipped a stale slot from one that is simply not firing (critique B M20). \
         Got: {status}"
    );
    let condition = &status["conditions"][0];
    assert_eq!(condition["type"], serde_json::json!("Ready"));
    assert_eq!(condition["reason"], serde_json::json!(REASON_SLOT_MISSED));
    assert_eq!(
        condition["message"],
        serde_json::json!(format!(
            "slot {slot} is past its starting deadline and was not fired; catchUpPolicy is \
             None, so it is counted in status.missedSlots and the next firing is \
             2026-09-14T03:17:00Z"
        )),
        "the message names the slot, the field that set the deadline, the fact it was NOT \
         fired, where the skip is counted, and when the schedule resumes. Got: {condition}"
    );
    assert_eq!(
        MISSED_SLOT_HORIZON, 3600,
        "the horizon is one hour, and the CRD field description says so"
    );
    assert_eq!(
        status["nextFireTime"],
        serde_json::json!("2026-09-14T03:17:00Z"),
        "the next firing is recorded even for a skipped slot"
    );

    // THE BOUNDARY, from both sides: exactly one hour after the slot still
    // fires; a second past it does not.
    assert!(matches!(
        decide("nightly", &schedule.spec, utc(2026, 9, 7, 4, 17)),
        SlotDecision::Due { .. }
    ));
    assert!(matches!(
        decide(
            "nightly",
            &schedule.spec,
            Utc.with_ymd_and_hms(2026, 9, 7, 4, 17, 1).single().unwrap()
        ),
        SlotDecision::Missed { .. }
    ));

    // A STALE SLOT WITH AN OLDER `lastFireTime` IS STILL MISSED. The
    // refinement that keeps a healthy schedule out of `SlotMissed` (see
    // `a_healthy_schedule_is_not_reported_as_missing_the_slot_it_fired`) must
    // not swallow a real skip: last week's fire is not this week's slot.
    let mut stale = schedule.clone();
    stale.status = Some(BackupScheduleStatus {
        last_fire_time: Some(utc(2026, 8, 31, 3, 17)),
        ..BackupScheduleStatus::default()
    });
    let decision = refine_against_status(
        decide("nightly", &stale.spec, now),
        stale.status.as_ref().and_then(|s| s.last_fire_time),
        None,
    );
    assert!(
        matches!(decision, SlotDecision::Missed { .. }),
        "a slot LATER than `status.lastFireTime` was never fired and stays missed: {decision:?}"
    );
    assert_eq!(decision.reason(), REASON_SLOT_MISSED);
}

/// `lastTransitionTime` MOVES ONLY WHEN THE CONDITION TRANSITIONS.
///
/// REVIEW FINDING MED-1. This reconciler has no watch to wake it for a clock
/// tick, so it requeues every `REQUEUE_SECS` = 30 seconds — 2,880 status
/// writes a day per schedule. Every one of them used to carry
/// `lastTransitionTime: now`, so an operator asking "how long has this
/// schedule been Ready?" was told "thirty seconds" about a schedule that had
/// been Ready for a month, and every write bumped a `resourceVersion` that
/// every watcher in the cluster then had to receive.
///
/// The Kubernetes condition contract is that the field moves when the
/// condition transitions. The four arms below are: the first write (it is a
/// transition — there was no condition), the 30 s requeue (not a transition),
/// a later reconcile whose MESSAGE differs while `status` and `reason` do not
/// (not a transition — the message carries the next firing, which moves at
/// every slot boundary by design), and a real change of state (a transition).
#[tokio::test]
async fn two_reconciles_with_no_change_do_not_move_last_transition_time() {
    let fire = utc(2026, 9, 10, 0, 0);
    let slot = slot_name(fire);
    let name = scheduled_backup_name("nightly", &slot).expect("the fixture name fits");

    // ARM 1 — the first write. There is no previous condition, so `now` IS the
    // transition instant.
    let first = schedule("nightly", UID, DAILY, false);
    let (client, _calls, bodies) = mock_client_recording_bodies(daily_routes(&name, 201));
    reconcile_schedule(&first, &client, fire)
        .await
        .expect("the midnight slot fires");
    let patched = patched_status(&bodies.lock().expect("readable"));
    assert_eq!(
        patched["conditions"][0]["lastTransitionTime"],
        serde_json::json!(fire),
        "the FIRST condition transitions from nothing to Ready=True, so it carries `now`: \
         {patched}"
    );
    let stored: BackupScheduleStatus =
        serde_json::from_value(patched).expect("the patched status is a BackupScheduleStatus");

    // ARMS 2 AND 3 — nothing transitioned. `Due` at +30 s and `AlreadyFired`
    // at +12 h are both `Ready=True` with reason `Scheduled`; only the message
    // and `nextFireTime` differ.
    let requeue = fire
        + chrono::Duration::seconds(
            i64::try_from(REQUEUE_SECS).expect("the requeue interval is 30 seconds"),
        );
    //
    // `expected_patches` IS TASK 16b's ADDITION, and it is the sharper form of
    // this test's own property, now measured over the three states a slot
    // really passes through. At the 30 s requeue the run created at midnight is
    // RUNNING, which is a different message from "is due" — so one patch. At
    // +60 s nothing at all has moved, and the reconcile sends NOTHING (plan
    // erratum E11(d)). At +12 h the run has Succeeded, whose message differs
    // again while `status` and `reason` do not, so one patch is sent and
    // `lastTransitionTime` must still be the original instant — which is the
    // MED-1 assertion, unchanged.
    //
    // THE ZERO-PATCH ARM IS THE ONE THAT MATTERS, and it is the middle one:
    // this reconciler wakes every 30 s forever, so "the state did not move" has
    // to cost nothing. Its `stored` is the arm before it, threaded through.
    let mut carried = stored.clone();
    for (label, now, phase, expected_patches) in [
        ("the 30 s requeue", requeue, "Running", 1),
        (
            "+60 s, nothing moved",
            requeue + chrono::Duration::seconds(30),
            "Running",
            0,
        ),
        (
            "+12 h, a different message",
            utc(2026, 9, 10, 12, 0),
            "Succeeded",
            1,
        ),
    ] {
        let mut again = schedule("nightly", UID, DAILY, false);
        again.status = Some(carried.clone());
        let (client, _calls, bodies) =
            mock_client_recording_bodies(daily_routes_phase(&name, 409, phase));
        reconcile_schedule(&again, &client, now)
            .await
            .unwrap_or_else(|e| panic!("{label}: {e}"));
        let recorded = bodies.lock().expect("readable").clone();
        assert_eq!(
            patch_count(&recorded),
            expected_patches,
            "{label}: a reconcile that changes nothing writes nothing (plan erratum E11(d)); a \
             reconcile that changes the message writes once"
        );
        if let Some(patched) = patched_status_opt(&recorded) {
            carried = serde_json::from_value(
                serde_json::to_value(stored_after(Some(&carried), &patched))
                    .expect("the merged status serialises"),
            )
            .expect("the merged status is a BackupScheduleStatus");
        }
        let status = patched_status_opt(&recorded).unwrap_or_else(|| {
            serde_json::to_value(&carried).expect("the stored status serialises")
        });
        let condition = status["conditions"][0].clone();
        assert_eq!(
            condition["reason"],
            serde_json::json!(REASON_SCHEDULED),
            "{label}: the reason is unchanged, which is the premise of this arm: {condition}"
        );
        assert_eq!(
            condition["lastTransitionTime"],
            serde_json::json!(fire),
            "{label}: NOTHING transitioned, so `lastTransitionTime` still names the instant \
             the schedule became Ready. Bumping it on every reconcile writes 2,880 false \
             transitions a day (review finding MED-1): {condition}"
        );
    }

    // ARM 4 — a real transition. Suspending the schedule changes both the
    // status and the reason, and the timestamp moves to the instant it did.
    let mut suspended = schedule("nightly", UID, DAILY, true);
    suspended.status = Some(stored.clone());
    let transition = utc(2026, 9, 10, 9, 30);
    let (client, _calls, bodies) = mock_client_recording_bodies(daily_routes(&name, 409));
    reconcile_schedule(&suspended, &client, transition)
        .await
        .expect("a suspended schedule is a decision, not an error");
    let condition = patched_status(&bodies.lock().expect("readable"))["conditions"][0].clone();
    assert_eq!(condition["reason"], serde_json::json!(REASON_SUSPENDED));
    assert_eq!(condition["status"], serde_json::json!("False"));
    assert_eq!(
        condition["lastTransitionTime"],
        serde_json::json!(transition),
        "Ready=True/Scheduled -> Ready=False/Suspended IS a transition, and the timestamp \
         names when it happened: {condition}"
    );

    // AND THE PURE FUNCTION SAYS THE SAME THING, so the property is readable
    // without a client: the same decision against the same stored condition
    // keeps the old instant, whatever `now` is.
    let decision = decide("nightly", &first.spec, fire);
    let mut carrying = schedule("nightly", UID, DAILY, false);
    carrying.status = Some(stored);
    let patch = status_patch(&carrying, &decision, Some(&name), utc(2027, 5, 1, 4, 4));
    assert_eq!(
        patch["status"]["conditions"][0]["lastTransitionTime"],
        serde_json::json!(fire),
        "`status_patch` reads the condition it is replacing: {patch}"
    );
}

/// A HEALTHY schedule is never reported as having missed the slot it fired.
///
/// REVIEW FINDING HIGH-1, AND THE MEASUREMENT THAT MADE IT ONE. Before the fix
/// a `0 0 * * *` schedule fired at midnight and then, from 01:00 until the next
/// midnight, found its own 00:00 slot outside the one-hour horizon and wrote
/// **that slot** into `status.lastMissedSlot` with the message "was not fired":
/// `SlotMissed` for **1379 of 1440 minutes a day**. The field critique B M20
/// added so that a correct implementation is distinguishable from a broken
/// schedule was therefore written by normal operation on every schedule slower
/// than hourly — the remedy inverted.
///
/// The fix is that a slot is missed only if it was NEVER FIRED, decided by
/// `refine_against_last_fire` from `status.lastFireTime` AFTER the name has
/// been minted. This test drives the real reconciler over the real status: it
/// fires the midnight slot, feeds the patched status back as the API server
/// would, and reconciles again at +5 min, +12 h and +23 h 59 min.
#[tokio::test]
async fn a_healthy_schedule_is_not_reported_as_missing_the_slot_it_fired() {
    let fire = utc(2026, 9, 10, 0, 0);
    let slot = slot_name(fire);
    let name = scheduled_backup_name("nightly", &slot).expect("the fixture name fits");

    // The midnight reconcile: the slot is due, the Backup is created, the
    // status records the fire.
    let midnight = schedule("nightly", UID, DAILY, false);
    let (client, _calls, bodies) = mock_client_recording_bodies(daily_routes(&name, 201));
    reconcile_schedule(&midnight, &client, fire)
        .await
        .expect("the midnight slot fires");
    let stored: BackupScheduleStatus =
        serde_json::from_value(patched_status(&bodies.lock().expect("readable")))
            .expect("the patched status is a BackupScheduleStatus — the API server stores it");
    assert_eq!(
        stored.last_fire_time,
        Some(fire),
        "the fire is recorded, which is what the later reconciles read"
    );

    // Every instant of the rest of the day, at the three points that matter:
    // just after the fire (still inside the horizon), the middle of the day,
    // and one minute before the next slot.
    //
    // `expected_patches` IS TASK 16b's ADDITION. At +5 min the slot is still
    // inside the horizon, so the decision, the name, `nextFireTime` and the
    // whole condition are byte-identical to what the midnight pass already
    // wrote — and a reconcile that computes the status the object already
    // carries now sends NO patch (plan erratum E11(d)). At +12 h and
    // +23 h 59 min the decision has become `AlreadyFired`, whose message
    // differs, so one patch IS sent.
    for (label, now, expected_posts, expected_patches) in [
        ("+5 min", utc(2026, 9, 10, 0, 5), 0, 1),
        ("+12 h", utc(2026, 9, 10, 12, 0), 0, 1),
        ("+23 h 59 min", utc(2026, 9, 10, 23, 59), 0, 1),
    ] {
        let mut later = schedule("nightly", UID, DAILY, false);
        later.status = Some(stored.clone());
        // 409, because the midnight Backup EXISTS. A POST route is present in
        // every arm, including all three that make none: a reconcile that
        // declined to create proves more than one that could not.
        //
        // ZERO POSTs IN EVERY ARM, WHICH IS STRICTER THAN BEFORE. D1 §4.5 step
        // 6 asks the slot's deterministic object what happened to it before
        // deciding anything, so a slot whose run already succeeded is answered
        // without a `POST` at all — the 409 that used to be the idempotence key
        // is now a fallback for the race, not the steady state.
        let (client, calls, bodies) = mock_client_recording_bodies(daily_routes(&name, 409));
        let outcome = reconcile_schedule(&later, &client, now)
            .await
            .unwrap_or_else(|e| panic!("{label}: a healthy schedule is not an error: {e}"));

        let seen = calls.lock().expect("the recorder is readable").clone();
        assert_eq!(
            seen.iter().filter(|c| c.method == "POST").count(),
            expected_posts,
            "{label}: the slot's attempt chain already shows a Succeeded run, so nothing is \
             admitted and no POST is made at all. Calls: {seen:?}"
        );
        assert_eq!(
            outcome.decision.reason(),
            REASON_SCHEDULED,
            "{label}: a schedule that fired its slot is Scheduled, not SlotMissed: {:?}",
            outcome.decision
        );

        let recorded = bodies
            .lock()
            .expect("the body recorder is readable")
            .clone();
        assert_eq!(
            patch_count(&recorded),
            expected_patches,
            "{label}: a reconcile that changes nothing writes nothing (plan erratum E11(d))"
        );
        // The status the object CARRIES after this pass: the patch when one was
        // sent, and otherwise the one it already had — which is the same
        // object either way, and is what the assertions below are about.
        let status = patched_status_opt(&recorded).unwrap_or_else(|| {
            serde_json::to_value(&stored).expect("the stored status serialises")
        });
        assert!(
            status.get("lastMissedSlot").is_none(),
            "{label}: NOTHING was missed, so `lastMissedSlot` is not written. This assertion \
             is review finding HIGH-1: the slot this schedule successfully fired must never \
             appear in the field that records skips. Got: {status}"
        );
        let condition = &status["conditions"][0];
        assert_eq!(
            condition["reason"],
            serde_json::json!(REASON_SCHEDULED),
            "{label}: got {condition}"
        );
        assert_eq!(
            condition["status"],
            serde_json::json!("True"),
            "{label}: got {condition}"
        );
        assert!(
            !condition["message"]
                .as_str()
                .expect("the condition carries a message")
                .contains("was not fired"),
            "{label}: the message must not say a fired slot was not fired. Got: {condition}"
        );
    }

    // AND THE REFINEMENT CANNOT REACH A NAME. `Due` — the one variant that
    // carries an object name — is returned unchanged for every possible
    // `lastFireTime`, including one in the future, so the name stays a pure
    // function of the trigger no matter what the status says.
    let due = decide("nightly", &midnight.spec, fire);
    for last in [
        None,
        Some(utc(2026, 9, 10, 0, 0)),
        Some(utc(2026, 9, 9, 0, 0)),
        Some(utc(2027, 1, 1, 0, 0)),
    ] {
        assert_eq!(
            refine_against_status(due.clone(), last, None),
            due,
            "the refinement touches the reason of a MISSED slot and nothing else; a `Due` \
             decision — the only one carrying a name — is returned verbatim (last = {last:?})"
        );
    }
}

/// `spec.suspend` creates nothing, and the `Ready` reason says why.
#[tokio::test]
async fn suspend_creates_nothing() {
    let schedule = schedule("nightly", UID, "17 3 * * 1", true);

    let (client, calls, bodies) = mock_client_recording_bodies(vec![
        no_backups(),
        // A POST ROUTE IS PRESENT ON PURPOSE, even though this test asserts that
        // ZERO POSTs are made. Without it the double would PANIC on the
        // unrouted request (see `src/testing.rs`), which fails the test for the
        // right reason but at the wrong place: the assertion that must fire is
        // the zero-POST count, and a test that only proves the reconciler COULD
        // NOT create proves less than one that proves it CHOSE not to.
        Route {
            method: "POST",
            path_suffix: "/namespaces/logweir-t18/backups",
            status: 201,
            body: created_backup_body("logweir-backup-nightly-20260907-031700"),
        },
        Route {
            method: "PATCH",
            path_suffix: "/backupschedules/nightly/status",
            status: 200,
            body: patched_schedule_body(),
        },
    ]);
    let outcome = reconcile_schedule(&schedule, &client, utc(2026, 9, 7, 3, 17))
        .await
        .expect("a suspended schedule is a decision, not an error");

    let seen = calls.lock().expect("the recorder is readable").clone();
    assert_eq!(
        seen.iter().filter(|c| c.method == "POST").count(),
        0,
        "ZERO POST requests while `spec.suspend` is true, even at the exact instant a slot \
         comes due. Calls: {seen:?}"
    );
    assert_eq!(outcome.decision, SlotDecision::Suspended);
    assert_eq!(outcome.created, None);

    let status = patched_status(&bodies.lock().expect("the body recorder is readable"));
    assert_eq!(
        status["conditions"][0]["reason"],
        serde_json::json!(REASON_SUSPENDED),
        "the Ready condition's reason is `Suspended`. Got: {status}"
    );
    assert_eq!(
        status["nextFireTime"],
        serde_json::Value::Null,
        "`nextFireTime` is CLEARED — an explicit JSON null, not an omitted key, because an \
         omitted key in a merge patch leaves the old value in kubectl's NEXT column for a \
         schedule that is not going to fire. Got: {status}"
    );
    assert!(
        status.get("lastMissedSlot").is_none() && status.get("lastFireTime").is_none(),
        "suspending a schedule neither records a missed slot nor rewrites when it last fired"
    );
}

// ---------------------------------------------------------------------------
// The backup id, and interface I10
// ---------------------------------------------------------------------------

/// `backup_id` includes the schedule UID, so two same-named schedules in two
/// namespaces cannot collide in one archive.
#[test]
fn backup_id_includes_the_schedule_uid() {
    let slot = slot_name(utc(2026, 9, 7, 3, 17));
    let a = backup_id_for(UID, &slot);
    let b = backup_id_for(OTHER_UID, &slot);

    assert_ne!(
        a, b,
        "two BackupSchedules named `nightly` in two namespaces are two schedules, and a \
         backup_id built from their NAMES would put both of their 03:17 runs under one archive \
         prefix — the colliding-backup_id case that does not accumulate and leaves a partial \
         archive behind"
    );
    assert_eq!(a, format!("{UID}-{slot}"));
    assert!(a.contains(UID) && b.contains(OTHER_UID));

    // And through the whole object, which is where it actually reaches the
    // archive: the same name, the same slot, two namespaces. The Backup
    // reconciler derives the run identity from the object this reconciler
    // creates — with the UID the API server gives it — and never from an
    // annotation, so the property is asserted over that derivation.
    let one = schedule("nightly", UID, "17 3 * * 1", false);
    let two = schedule("nightly", OTHER_UID, "17 3 * * 1", false);
    let name = scheduled_backup_name("nightly", &slot).unwrap();
    let id_of = |s: &BackupSchedule, uid: &str| {
        let mut created = scheduled_backup(s, uid, &slot, &name);
        created.metadata.uid = Some(format!("api-server-assigned-{uid}"));
        execution_identity(&created)
            .expect("a Backup the schedule creates states a scheduled run identity")
            .id
    };
    assert_eq!(id_of(&one, UID), a);
    assert_eq!(id_of(&two, OTHER_UID), b);
    assert_ne!(id_of(&one, UID), id_of(&two, OTHER_UID));
}

/// The `--backup-id-override` flag is PASSED by this crate, not defined here —
/// and since PLAT-06.1 it is passed by the `Backup` reconciler's derived argv,
/// never by an annotation the schedule writes.
///
/// INTERFACE **I10** IS TASK 4'S. The flag lives on `logweir backup run`; this
/// crate writes it into the runner argv `backup_execution::runner_argv`
/// derives, and adds no file under `crates/logweir/` at all (critique B
/// H10(c): the first draft mandated a new CLI flag from a task whose Files
/// block names no `crates/logweir/` file, on a chain it does not own).
///
/// KILLS: re-adding the `logweir.dev/runner-argv` annotation to the scheduled
/// Backup; deriving the scheduled backup id from anything but the schedule UID
/// and the slot; a second code-line spelling of the flag anywhere in the crate.
#[test]
fn the_backup_id_override_is_passed_not_defined() {
    let slot = slot_name(utc(2026, 9, 7, 14, 5));
    let name = scheduled_backup_name("nightly", &slot).unwrap();
    let schedule = schedule("nightly", UID, "5 14 * * *", false);
    let backup = scheduled_backup(&schedule, UID, &slot, &name);

    assert!(
        backup
            .metadata
            .annotations
            .as_ref()
            .is_none_or(|annotations| !annotations.contains_key(RUNNER_ARGV_ANNOTATION)),
        "a scheduled Backup carries NO `{RUNNER_ARGV_ANNOTATION}` annotation: the Backup \
         reconciler derives the argv from the typed spec and the server-generated run identity, \
         and an annotation is never executable input. Got {:?}",
        backup.metadata.annotations
    );

    // The identity the Backup reconciler derives from exactly this object, with
    // the UID the API server would give it.
    let mut stored = backup.clone();
    stored.metadata.uid = Some("7d1b5b6e-0000-4000-8000-00000000b001".to_string());
    let identity = execution_identity(&stored)
        .expect("the object this reconciler creates states a complete scheduled run identity");
    assert_eq!(identity.trigger, ExecutionTrigger::Schedule);
    assert_eq!(identity.id, backup_id_for(UID, &slot));
    let argv = runner_argv(identity.trigger, &identity.id);

    let flag = argv
        .iter()
        .position(|a| a == "--backup-id-override")
        .expect("the runner argv must carry --backup-id-override (interface I10)");
    assert_eq!(
        argv[flag + 1],
        backup_id_for(UID, &slot),
        "the flag's value is `backup_id_for(<schedule uid>, <slot>)`, so the CLI does not have \
         to know that schedules exist"
    );
    assert_eq!(
        argv,
        runner_argv(ExecutionTrigger::Schedule, &backup_id_for(UID, &slot))
    );
    assert_eq!(
        argv[0], "backup",
        "the argv's first token names the `logweir` subcommand `backup`, which Global \
         Constraint 3 as revised by Task 1 admits"
    );

    // The rest of the object the Backup reconciler reads by name.
    assert_eq!(backup.metadata.name.as_deref(), Some(name.as_str()));
    assert_eq!(backup.spec.slot.as_deref(), Some(slot.as_str()));
    assert_eq!(backup.spec.triggered_by, TRIGGERED_BY_SCHEDULE);
    assert_eq!(TRIGGERED_BY_SCHEDULE, TRIGGER_SCHEDULE);
    assert_eq!(
        backup.spec.schedule_ref.as_ref().map(|r| r.name.as_str()),
        Some("nightly")
    );
    // NO ownerReference TO THE SCHEDULE — PLAT-05.2, D1 §6.1. This assertion
    // used to require one, `controller: true` and `blockOwnerDeletion: true`;
    // that reference is what made `kubectl delete backupschedule` delete the
    // history, and removing it is the whole of PLAT-05.2. The identity this
    // test is about — `backup_id_for(scheduleUid, slot)` — is unchanged,
    // because it is built from `spec.scheduleRef.uid`, asserted above, and
    // never from an owner entry.
    assert_eq!(
        backup
            .metadata
            .owner_references
            .as_deref()
            .unwrap_or_default(),
        &[],
        "a scheduled Backup is NOT a dependent of its schedule (D1 §6.1); deleting the          schedule must leave this object, its plan ConfigMap and its Job in place"
    );
    assert_eq!(
        backup.spec.schedule_ref.as_ref().and_then(|r| r.uid.as_deref()),
        Some(UID),
        "and the UID membership is built on is on the SPEC, where deleting the owner cannot          reach it"
    );
    let labels = backup.metadata.labels.expect("labels");
    assert_eq!(labels[SCHEDULE_LABEL], "nightly");
    assert_eq!(labels[SLOT_LABEL], slot);

    // -----------------------------------------------------------------------
    // THE SOURCE-SHAPE HALF: the flag is DEFINED in Task 4's crate and only
    // PASSED here.
    //
    // ASSERTED OVER THE CHECKED-IN TREE, WITH NO `git` AND NO BASE COMMIT.
    // This half used to shell `git diff --name-only e2eb5b7 -- crates/logweir/`
    // — a hard-coded base SHA. main moved six files under `crates/logweir/`
    // after `e2eb5b7`, so the assertion went red on a conflict-free rebase and
    // the branch could not land (review finding HIGH-2); and because the git
    // call was wrapped in `if let Ok(out) { if out.status.success() {`, it
    // passed silently wherever git was absent. "Which files did this commit
    // touch" was always a proxy anyway. What the brief actually asks — the flag
    // belongs to `logweir backup run` and this crate merely writes it into an
    // argv — is a property of the SOURCE, true or false in the working tree
    // whatever any earlier commit did, and that is what is asserted below.
    // -----------------------------------------------------------------------

    // 1. TASK 4'S CRATE DEFINES IT, and defines it as a clap long flag. The
    //    flag's spelling is DERIVED from the field name rather than repeated,
    //    because clap derives it the same way: rename the field there and this
    //    assertion fails here, which is exactly the coupling that matters —
    //    the runner would otherwise be handed a flag it does not know.
    let cli = workspace_source("crates/logweir/src/cli.rs");
    let field = "backup_id_override";
    let at = cli.find(&format!("{field}:")).unwrap_or_else(|| {
        panic!(
            "crates/logweir/src/cli.rs must declare `{field}` — interface I10 is Task 4's \
             flag on Task 4's `logweir backup run`, and this task passes it"
        )
    });
    assert!(
        cli[..at].trim_end().ends_with("#[arg(long)]"),
        "`{field}` must be a clap LONG flag in crates/logweir/src/cli.rs: it is what \
         `--backup-id-override` in this crate's argv resolves to"
    );
    let flag = format!("--{}", field.replace('_', "-"));
    assert_eq!(
        flag, "--backup-id-override",
        "clap spells a long flag as the kebab-case of its field name, so the token this \
         crate writes and the field Task 4 declares are one decision"
    );
    assert!(
        argv.contains(&flag),
        "the argv carries the flag Task 4 declared: {argv:?}"
    );

    // 2. THIS CRATE DEFINES NO CLI FLAG AT ALL. No `#[arg(…)]` anywhere under
    //    `crates/weirkeeper/src/`, and no `clap` in its manifest — so there is
    //    nowhere for a second definition of I10 to hide, which is what critique
    //    B H10(c) was about.
    let sources = weirkeeper_sources();
    for (path, text) in &sources {
        assert!(
            !text.contains("#[arg("),
            "crates/weirkeeper/src/{path} declares a clap argument: this crate defines no CLI \
             surface, it writes an argv for Task 4's"
        );
    }
    let manifest = source_of("Cargo.toml");
    assert!(
        !manifest.contains("clap"),
        "weirkeeper's manifest must not take `clap`: the flag set it passes is another \
         crate's"
    );

    // 3. AND THE FLAG IS NAMED IN EXACTLY ONE PLACE IN THIS CRATE'S CODE —
    //    `backup_execution::runner_argv`'s token array. Counted over CODE
    //    lines only, the way `the_name_never_reads_a_reconcile_clock_or_a_status`
    //    counts clock reads: documentation names the flag to say whose it is,
    //    and a count including prose would be a count of comments. A second
    //    code line — an annotation writer, a message quoting the token, a
    //    second argv builder — is a second place the executed identity could
    //    come from.
    for (path, text) in &sources {
        let code = text
            .lines()
            .filter(|l| {
                let t = l.trim_start();
                !t.starts_with("//") && t.contains(&flag)
            })
            .count();
        let expected = usize::from(path == "backup_execution.rs");
        assert_eq!(
            code, expected,
            "crates/weirkeeper/src/{path} names {flag} on {code} code line(s); expected \
             {expected}. The one permitted occurrence is the token in `runner_argv`"
        );
    }
    let src = source_of("src/backup_execution.rs");
    assert!(
        fn_body(&src, "pub fn runner_argv(").contains(&flag),
        "that one occurrence is inside `runner_argv`, where it is an argv TOKEN"
    );

    // 4. And neither reconciler reaches the CLI through a Rust path: the
    //    runner is another binary, reached through an argv string.
    for file in [
        "src/backup_execution.rs",
        "src/controllers/backup_schedule.rs",
    ] {
        let text = source_of(file);
        assert!(
            !text.contains("logweir::backup"),
            "{file} reaches the CLI through an argv string and never through a Rust path"
        );
    }
    assert!(
        !source_of("src/controllers/backup_schedule.rs").contains(RUNNER_ARGV_ANNOTATION),
        "the schedule reconciler writes no runner-argv annotation, and does not even name one"
    );
}

/// **The runner argv is one `logweir backup run` ACCEPTS** — Task 24, found by
/// execution on a live cluster.
///
/// `logweir backup run` writes exactly ONE document and `--receipt-out` takes
/// precedence over `--out`, so the CLI refuses two flags naming DIFFERENT
/// paths with exit **1**, before the engine is spawned and before any broker
/// client exists (`refuse_two_receipt_paths`, in the CLI's own
/// `backup/phase_minus1_admit.rs`). This argv passed both —
/// `--out /work/backup.json --receipt-out /work/receipt.json` — so **every
/// scheduled `Backup` in the shipped tree exited 1 and archived nothing**,
/// with the message
///
/// > operational: --receipt-out /work/receipt.json and --out /work/backup.json
/// > name DIFFERENT paths … NO backup was taken
///
/// measured in `logweir-t24` during the Phase B demo. The stub could not see
/// it and no unit test asserted it, because the argv was only ever compared
/// against itself.
///
/// KILLS: re-adding `--out` beside `--receipt-out`, in either order.
#[test]
fn the_runner_argv_names_at_most_one_output_path() {
    let argv = runner_argv(ExecutionTrigger::Schedule, "b1");
    let out = argv.iter().filter(|a| *a == "--out").count();
    let receipt = argv.iter().filter(|a| *a == "--receipt-out").count();
    assert_eq!(
        receipt, 1,
        "the signed receipt is the one document this command writes, and the flag that names it \
         is the one that wins; got {argv:?}"
    );
    assert_eq!(
        out, 0,
        "`--out` beside `--receipt-out` at a different path is exit 1 before anything runs — the \
         defect that made every scheduled Backup in this tree fail. Got {argv:?}"
    );
    // AND THE TWO PATHS ARE STILL DIFFERENT CONSTANTS, so a future editor who
    // re-adds the flag cannot do it "safely" by pointing both at one file and
    // then have the two drift apart again.
    assert_ne!(
        OUT_PATH, RECEIPT_OUT_PATH,
        "if these ever became equal the CLI would accept both flags, and this test would stop \
         meaning anything"
    );
}

// ---------------------------------------------------------------------------
// The argv, handed to the parser that has to accept it — erratum E20
// ---------------------------------------------------------------------------

/// `target/debug/logweir`, the runner's own binary (interface **E9**), located
/// from this crate's manifest directory.
///
/// A weirkeeper test cannot use `env!("CARGO_BIN_EXE_logweir")`: that variable
/// exists only for targets of the package that declares the binary, and
/// `weirkeeper` must NOT declare `logweir` as a dependency of any kind —
/// `scripts/check-one-signer.sh`'s check 1 counts normal, build **and** dev
/// edges, and the set of crates from which `logweir-evidence` is reachable is
/// pinned at exactly `{logweir, e2e}`. Taking even a dev-dependency here would
/// put `weirkeeper` in that set and turn the link-time single-signer gate red.
/// So the two sides meet through the filesystem and a process, which is also
/// what they do in a cluster. Task 22's `restore_controller.rs` carries the
/// identical helper for the identical reason.
///
/// AN ABSENT BINARY IS A LOUD, NAMED FAILURE AND NEVER A SILENT SKIP. A row
/// that quietly passes when its subject is missing is the defect this whole
/// file exists to avoid.
fn runner_binary() -> std::path::PathBuf {
    let target = std::env::var_os("CARGO_TARGET_DIR").map_or_else(
        || {
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../target")
                .to_path_buf()
        },
        std::path::PathBuf::from,
    );
    let bin = target.join("debug").join("logweir");
    assert!(
        bin.exists(),
        "the runner binary is not at {}. This row is END-TO-END on purpose: it feeds the argv \
         THIS crate emits to the parser that has to accept it. `cargo test --workspace` builds \
         it; `cargo test -p weirkeeper` alone does not — run `cargo build -p logweir` first.",
        bin.display()
    );
    bin
}

/// A scratch directory under the system temp dir, made by hand.
///
/// `tempfile` is not a dependency of this crate and is not being added for one
/// test: `tests/linkage.rs` measures this crate's declared entries, and a new
/// one would have to justify itself there. The same decision
/// `tests/restore_controller.rs` and `tests/verification.rs` record.
fn scratch_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("logweir-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("a scratch directory under the temp dir");
    dir
}

/// The argv with its four IN-POD paths rewritten to files that exist on this
/// machine, and NOTHING ELSE TOUCHED.
///
/// Flag tokens, their order, and every non-path value are the reconciler's
/// own. The substitution is by EXACT CONSTANT — `SPEC_PATH`,
/// `ALLOWED_CLUSTERS_PATH`, `SIGNING_KEY_PATH`, `RECEIPT_OUT_PATH` — and the
/// caller asserts afterwards that no absolute path survived it, so a fifth
/// path flag added to `runner_argv` cannot slip through this row untested: it
/// would still be `/…` and the assertion would name it.
fn argv_against(dir: &std::path::Path, argv: &[String]) -> Vec<String> {
    argv.iter()
        .map(|a| match a.as_str() {
            SPEC_PATH => dir.join("backup.yaml").display().to_string(),
            ALLOWED_CLUSTERS_PATH => dir.join("allowed-clusters.json").display().to_string(),
            SIGNING_KEY_PATH => dir.join("key.pem").display().to_string(),
            RECEIPT_OUT_PATH => dir.join("receipt.json").display().to_string(),
            other => other.to_string(),
        })
        .collect()
}

/// **THE `Backup` ARGV IS ONE `logweir backup run` ACCEPTS** — erratum
/// **E20**, and this row is end-to-end for exactly that reason.
///
/// # What a shape assertion could not see
///
/// `backup_schedule::runner_argv` emitted `--out /work/backup.json` beside
/// `--receipt-out /work/receipt.json`. `logweir backup run` writes ONE
/// document and refuses two flags naming different paths with exit **1**,
/// before the engine is spawned — so **every scheduled `Backup` in the shipped
/// tree exited 1 from Task 18 to Task 24 and nothing was ever archived**.
/// Tasks 17, 18 and 19 and all three reviews were green, because every
/// assertion about this argv compared the reconciler's output against a
/// literal written in the same repository. The rule the plan carries forward:
/// **an argv a controller emits is an interface with another binary, and it is
/// tested only when it is handed to that binary's real parser.**
///
/// # What is asserted, and why the run fails where it does
///
/// The argv is `runner_argv`'s own, with only its four in-pod paths pointed at
/// stub files in a scratch directory. The stub spec is a real `BackupSpec`
/// with `auth.mode: scramSha512`, so the run passes clap, passes every local
/// guard — including the `--out`/`--receipt-out` refusal, which is phase −1's
/// step 4 — and stops at the credential projection with `$LOGWEIR_SOURCE_PASSWORD`
/// unset: exit 1, named, **before any librdkafka handle exists**, so this row
/// dials nothing and takes milliseconds. That is the first point at which a
/// real broker or archive would be needed.
///
/// The negative twin appends `--out` at a different path and requires the E20
/// refusal, so the positive half is not vacuous: a build that accepted
/// anything would fail it.
///
/// KILLS: putting `--out` back beside `--receipt-out` in `runner_argv`; any
/// flag this crate spells differently from the CLI (`unexpected argument`);
/// any value the CLI's parser rejects (`unexpected value`).
#[test]
fn the_backup_runner_argv_is_one_the_cli_accepts() {
    let dir = scratch_dir("t24-backup-argv");
    // A REAL `BackupSpec`, built through the type the CLI parses rather than
    // written as text, for `plan_backup_spec`'s own reason: `storage` is an
    // internally tagged enum whose variants have incompatible required fields.
    let spec = logweir_core::spec::BackupSpec {
        source: logweir_core::spec::BackupSourceSpec {
            bootstrap_servers: vec!["broker-0.prod:9093".to_string()],
            auth: logweir_core::spec::AuthSpec::ScramSha512 {
                username: "logweir".to_string(),
                tls: true,
            },
            topics: vec!["orders".to_string()],
        },
        storage: logweir_core::engine::StorageUrl::S3 {
            bucket: "kafka-backups".to_string(),
            prefix: "logweir".to_string(),
            region: None,
            endpoint: None,
            path_style: true,
            allow_http: false,
        },
        backup_id: "b1".to_string(),
        backup: logweir_core::spec::BackupSettings::default(),
    };
    std::fs::write(
        dir.join("backup.yaml"),
        serde_yaml::to_string(&spec).expect("the stub spec serialises"),
    )
    .expect("the scratch spec is writable");
    std::fs::write(
        dir.join("allowed-clusters.json"),
        serde_json::to_string(&logweir_core::spec::AllowedClusters {
            allowed_cluster_ids: Vec::new(),
            source_cluster_id: None,
        })
        .expect("the stub allowlist serialises"),
    )
    .expect("the scratch allowlist is writable");
    // A valid PKCS#8 signer in the SCRATCH path. Signing readiness is checked
    // before credential projection, so a malformed placeholder would stop at
    // exit 4 and cease to prove that the controller's complete argv reaches
    // the expected missing-password boundary. The checked-in key is test-only
    // fixture material; copying it keeps the runner path identical to its pod
    // mount without adding a signer dependency to this controller crate.
    std::fs::copy(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../e2e/fixtures/signed/signing.pem"),
        dir.join("key.pem"),
    )
    .expect("a valid PKCS#8 signer is copied into the scratch mount path");

    let emitted = runner_argv(ExecutionTrigger::Schedule, "b1");
    let argv = argv_against(&dir, &emitted);
    let root = dir.display().to_string();
    let unmapped: Vec<&String> = argv
        .iter()
        .filter(|a| a.starts_with('/') && !a.starts_with(&root))
        .collect();
    assert!(
        unmapped.is_empty(),
        "every IN-POD path in the emitted argv is accounted for by `argv_against`; a flag naming \
         a fifth mount path would reach the CLI as a path that does not exist on this machine \
         and this row would stop meaning anything. Left unmapped: {unmapped:?}"
    );
    assert_eq!(
        argv.len(),
        emitted.len(),
        "the substitution rewrites values and adds and removes nothing"
    );

    let out = std::process::Command::new(runner_binary())
        .args(&argv)
        // `LOGWEIR_SOURCE_PASSWORD` is deliberately REMOVED rather than left
        // to the ambient environment: the assertion below is that the run
        // reached the credential projection, and an inherited value would send
        // it on to a broker that is not there.
        .env_remove("LOGWEIR_SOURCE_PASSWORD")
        .output()
        .expect("the runner binary runs");
    let stderr = String::from_utf8_lossy(&out.stderr).to_string();
    let first = stderr.lines().next().unwrap_or_default().to_string();

    assert!(
        !stderr.contains("unexpected argument"),
        "THE ERRATUM E20 FAILURE CLASS: the runner refused a flag this crate emits. argv \
         {argv:?}\nstderr: {stderr}"
    );
    assert!(
        !stderr.contains("unexpected value") && !stderr.contains("invalid value"),
        "…and every VALUE this crate emits is one the parser takes. argv {argv:?}\n\
         stderr: {stderr}"
    );
    assert!(
        !stderr.contains("name DIFFERENT paths"),
        "THE E20 DEFECT ITSELF: `logweir backup run` writes exactly one document, and this argv \
         named two output paths. Every scheduled Backup in the tree exited 1 on this message. \
         argv {argv:?}\nstderr: {stderr}"
    );
    assert_eq!(
        out.status.code(),
        Some(1),
        "the run gets as far as the credential projection and stops there. stderr: {stderr}"
    );
    assert!(
        first.contains("$LOGWEIR_SOURCE_PASSWORD is unset"),
        "…and THAT is where it stops — the first point at which a real broker or archive would \
         be needed, reached before any librdkafka handle exists. First stderr line: {first}"
    );

    // THE NEGATIVE TWIN. Re-add the flag that caused E20 and the same argv is
    // refused, so the assertions above are about this build and not about a
    // parser that accepts everything.
    let mut mutated = argv.clone();
    mutated.push("--out".to_string());
    mutated.push(dir.join("backup.json").display().to_string());
    let out = std::process::Command::new(runner_binary())
        .args(&mutated)
        .env_remove("LOGWEIR_SOURCE_PASSWORD")
        .output()
        .expect("the runner binary runs");
    let stderr = String::from_utf8_lossy(&out.stderr).to_string();
    assert_eq!(out.status.code(), Some(1), "{stderr}");
    assert!(
        stderr.contains("name DIFFERENT paths"),
        "`--out` beside `--receipt-out` at a different path IS refused by this build, so the \
         positive half above is a real acceptance and not a vacuous one: {stderr}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// The status write
// ---------------------------------------------------------------------------

/// A due slot records `lastFireTime`, `nextFireTime` and `activeBackupRef` —
/// and the `Ready` reason is `Scheduled`.
#[test]
fn a_fired_slot_records_what_it_fired() {
    let schedule = schedule("nightly", UID, "17 3 * * 1", false);
    let now = utc(2026, 9, 7, 3, 17);
    let decision = decide("nightly", &schedule.spec, now);
    let name = match &decision {
        SlotDecision::Due { name, .. } => name.clone(),
        other => panic!("the slot is due: {other:?}"),
    };

    let patch = status_patch(&schedule, &decision, Some(&name), now);
    let status = &patch["status"];
    assert_eq!(
        status["lastFireTime"],
        serde_json::json!("2026-09-07T03:17:00Z"),
        "`lastFireTime` is the DUE instant, not the reconcile clock: got {status}"
    );
    assert_eq!(
        status["nextFireTime"],
        serde_json::json!("2026-09-14T03:17:00Z")
    );
    assert_eq!(status["activeBackupRef"], serde_json::json!({"name": name}));
    assert_eq!(
        status["conditions"][0]["reason"],
        serde_json::json!(REASON_SCHEDULED)
    );
    assert_eq!(status["conditions"][0]["status"], serde_json::json!("True"));
    assert_eq!(
        status["conditions"][0]["observedGeneration"],
        serde_json::json!(4),
        "the condition names the generation it was computed from"
    );
    assert!(
        status.get("lastMissedSlot").is_none(),
        "a slot that FIRED records no missed slot"
    );
}

/// The reconciler patches only `/status`, and never DELETEs.
#[tokio::test]
async fn the_reconciler_patches_only_status_and_never_deletes() {
    let schedule = schedule("nightly", UID, "17 3 * * 1", false);
    let slot = slot_name(utc(2026, 9, 7, 3, 17));
    let name = scheduled_backup_name("nightly", &slot).unwrap();

    // The route table has NO DELETE route and no route for any object this
    // reconciler does not name, so the double PANICS on either — see
    // `src/testing.rs` for why a panic and not a 404. The two reads it does
    // make are the bootstrap listing and the slot's own attempt-0 GET, and both
    // are reads.
    let (client, calls) = mock_client_recording(vec![
        no_backups(),
        absent_backup(&name),
        Route {
            method: "POST",
            path_suffix: "/namespaces/logweir-t18/backups",
            status: 201,
            body: created_backup_body(&name),
        },
        Route {
            method: "PATCH",
            path_suffix: "/backupschedules/nightly/status",
            status: 200,
            body: reservation_echo(&patched_schedule_body(), &name, &slot, 0),
        },
    ]);
    reconcile_schedule(&schedule, &client, utc(2026, 9, 7, 3, 17))
        .await
        .expect("the slot fires");

    let seen = calls.lock().expect("the recorder is readable").clone();
    let pairs: Vec<(String, String)> = seen
        .iter()
        .map(|c| {
            (
                c.method.clone(),
                c.uri.split('?').next().unwrap_or(&c.uri).to_string(),
            )
        })
        .collect();
    assert_eq!(
        pairs,
        vec![
            (
                "GET".to_string(),
                "/apis/logweir.dev/v1alpha1/namespaces/logweir-t18/backups".to_string()
            ),
            (
                "GET".to_string(),
                format!("/apis/logweir.dev/v1alpha1/namespaces/logweir-t18/backups/{name}")
            ),
            (
                "PATCH".to_string(),
                "/apis/logweir.dev/v1alpha1/namespaces/logweir-t18/backupschedules/nightly/status"
                    .to_string()
            ),
            (
                "POST".to_string(),
                "/apis/logweir.dev/v1alpha1/namespaces/logweir-t18/backups".to_string()
            ),
            (
                "PATCH".to_string(),
                "/apis/logweir.dev/v1alpha1/namespaces/logweir-t18/backupschedules/nightly/status"
                    .to_string()
            ),
        ],
        "exactly two calls, in this order: the create, then the status patch. No GET (the \
         object arrived from the watch), no DELETE ever, and no patch of the bare object — \
         `spec` is sealed by CEL and `suspend` is the operator's field, not the controller's."
    );
}

/// An unparseable expression is a condition, not a crash — and creates nothing.
#[tokio::test]
async fn an_unparseable_schedule_creates_nothing_and_names_the_field() {
    let schedule = schedule("nightly", UID, "L * * * *", false);
    let (client, calls, bodies) = mock_client_recording_bodies(vec![
        no_backups(),
        // A POST ROUTE IS PRESENT ON PURPOSE, even though this test asserts that
        // ZERO POSTs are made. Without it the double would PANIC on the
        // unrouted request (see `src/testing.rs`), which fails the test for the
        // right reason but at the wrong place: the assertion that must fire is
        // the zero-POST count, and a test that only proves the reconciler COULD
        // NOT create proves less than one that proves it CHOSE not to.
        Route {
            method: "POST",
            path_suffix: "/namespaces/logweir-t18/backups",
            status: 201,
            body: created_backup_body("logweir-backup-nightly-20260907-031700"),
        },
        Route {
            method: "PATCH",
            path_suffix: "/backupschedules/nightly/status",
            status: 200,
            body: patched_schedule_body(),
        },
    ]);
    reconcile_schedule(&schedule, &client, utc(2026, 9, 7, 3, 17))
        .await
        .expect("an unparseable expression is a decision, not an error");

    let seen = calls.lock().expect("the recorder is readable").clone();
    assert_eq!(seen.iter().filter(|c| c.method == "POST").count(), 0);

    let status = patched_status(&bodies.lock().expect("the body recorder is readable"));
    assert_eq!(
        status["conditions"][0]["reason"],
        serde_json::json!("UnparseableSchedule")
    );
    assert_eq!(
        status["conditions"][0]["status"],
        serde_json::json!("False")
    );
    let message = status["conditions"][0]["message"]
        .as_str()
        .expect("the condition carries a message");
    assert!(
        message.contains("field 1 (minute)") && message.contains("\"L\""),
        "the condition names the field an operator has to fix. Got: {message}"
    );
    assert_eq!(status["nextFireTime"], serde_json::Value::Null);
}

/// A due slot whose name would not fit is a condition, not a silent skip.
#[tokio::test]
async fn a_name_that_does_not_fit_is_reported_rather_than_dropped() {
    let long = "n".repeat(52);
    let json = schedule_json(&long, UID, "17 3 * * 1", false);
    let schedule: BackupSchedule = serde_json::from_str(&json).expect("the fixture parses");

    let (client, calls, bodies) = mock_client_recording_bodies(vec![
        no_backups(),
        // A POST ROUTE IS PRESENT ON PURPOSE, even though this test asserts that
        // ZERO POSTs are made. Without it the double would PANIC on the
        // unrouted request (see `src/testing.rs`), which fails the test for the
        // right reason but at the wrong place: the assertion that must fire is
        // the zero-POST count, and a test that only proves the reconciler COULD
        // NOT create proves less than one that proves it CHOSE not to.
        Route {
            method: "POST",
            path_suffix: "/namespaces/logweir-t18/backups",
            status: 201,
            body: created_backup_body("logweir-backup-would-not-fit"),
        },
        Route {
            method: "PATCH",
            path_suffix: "/status",
            status: 200,
            body: json.clone(),
        },
    ]);
    reconcile_schedule(&schedule, &client, utc(2026, 9, 7, 3, 17))
        .await
        .expect("an unnameable slot is a decision, not an error");

    let seen = calls.lock().expect("the recorder is readable").clone();
    assert_eq!(
        seen.iter().filter(|c| c.method == "POST").count(),
        0,
        "nothing is created for a slot whose object name would be refused by the API server"
    );
    let status = patched_status(&bodies.lock().expect("the body recorder is readable"));
    assert_eq!(
        status["conditions"][0]["reason"],
        serde_json::json!("NameTooLong")
    );
    let message = status["conditions"][0]["message"]
        .as_str()
        .expect("a message");
    assert!(
        message.contains("63") && message.contains("83"),
        "the condition names the limit and the length, so the operator can shorten the \
         schedule's name by the right amount. Got: {message}"
    );
}

/// The CRD field descriptions state the missed-slot horizon, the field that now
/// sets it, and what an absent field means.
///
/// # What PLAT-04.2 changed about this obligation
///
/// The horizon used to be a constant nobody could see from the cluster, so the
/// obligation was that its description said "one hour". It is now
/// `spec.startingDeadlineSeconds`, and the obligation grew a second half: the
/// DEFAULT has to be discoverable too, because the whole compatibility promise
/// is that an absent field reproduces the old constant. A description that
/// named the field but not the default would leave every existing schedule's
/// behaviour undocumented at exactly the moment it became configurable.
#[test]
fn the_crd_states_the_missed_slot_horizon() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("the workspace root is two levels above this crate")
        .join("config/crd/backupschedules.yaml");
    let yaml = std::fs::read_to_string(&root).unwrap_or_else(|e| panic!("{}: {e}", root.display()));
    // The rendered YAML wraps long descriptions, so the assertion is on the
    // whitespace-collapsed text rather than on a line.
    let flat = yaml.split_whitespace().collect::<Vec<_>>().join(" ");
    for needle in [
        "THE MISSED-SLOT HORIZON IS ONE HOUR",
        "reason is `SlotMissed`",
        // The field that sets it, named from the cron field an operator reads
        // first...
        "more than `startingDeadlineSeconds` before the controller looks",
        // ...and the default, on the field itself.
        "How long after a slot came due it may still start. **Absent means 3600**",
    ] {
        assert!(
            flat.contains(needle),
            "config/crd/backupschedules.yaml must state {needle:?} so `kubectl explain` does \
             too — re-render with `just crds` after editing crds/backup_schedule.rs"
        );
    }
}

// ===========================================================================
// TASK 16b — THE STEADY-OBJECT ROW, plan erratum E11(d)
// ===========================================================================

/// The status the API server would hold after applying `patch` to `previous`.
///
/// ONTO THE PREVIOUS STATUS, NOT ONTO NOTHING, and that is the whole point of
/// using a real merge. `SlotDecision::AlreadyFired` deliberately writes NEITHER
/// `lastFireTime` NOR `activeBackupRef` — omitting a key in a merge patch means
/// "leave it alone" — so a test that rebuilt the object from the patch alone
/// would silently drop the record of the fire and turn a healthy schedule into
/// `SlotMissed` on the next pass. The API server does not do that, and neither
/// does this.
fn stored_after(
    previous: Option<&BackupScheduleStatus>,
    patch: &serde_json::Value,
) -> BackupScheduleStatus {
    let mut stored = previous.map_or(serde_json::Value::Null, |p| {
        serde_json::to_value(p).expect("a BackupScheduleStatus serialises")
    });
    apply_merge_patch(&mut stored, patch);
    serde_json::from_value(stored).expect("the patched status is a BackupScheduleStatus")
}

/// **Task 16b.** A steady `BackupSchedule` is patched once per real change and
/// never otherwise; a `suspend` flip is a real change.
///
/// # Why a route-table count and not another byte comparison
///
/// `two_reconciles_with_no_change_do_not_move_last_transition_time` already
/// pins the timestamp. What it could not see is that the reconciler used to
/// send the identical patch anyway, 2,880 times a day per schedule — invisible
/// on the wire only because the API server answers "this changed nothing".
/// With an archive configured it was not even invisible: `evaluatedAt = now`
/// made every one of those a real write, and the schedule spun. This row counts
/// REQUESTS.
///
/// Each pass's object is the previous pass's own patch applied as the API
/// server would apply it (`conditions::apply_merge_patch`, RFC 7386).
#[tokio::test]
async fn a_steady_backup_schedule_is_patched_once_and_a_suspend_flip_once_more() {
    let fire = utc(2026, 9, 10, 0, 0);
    let slot = slot_name(fire);
    let name = scheduled_backup_name("nightly", &slot).expect("the fixture name fits");

    // PASS 1 — midnight. The slot is due, the Backup is created, the status
    // records the fire.
    let (client, _calls, bodies) = mock_client_recording_bodies(daily_routes(&name, 201));
    reconcile_schedule(&schedule("nightly", UID, DAILY, false), &client, fire)
        .await
        .expect("the midnight slot fires");
    let stored = stored_after(None, &patched_status(&bodies.lock().expect("readable")));

    // PASS 2 — midday. `AlreadyFired`: the message changes, so one patch.
    let mut later = schedule("nightly", UID, DAILY, false);
    later.status = Some(stored);
    let (client, _calls, bodies) = mock_client_recording_bodies(daily_routes(&name, 409));
    reconcile_schedule(&later, &client, utc(2026, 9, 10, 12, 0))
        .await
        .expect("the midday reconcile completes");
    let midday = bodies.lock().expect("readable").clone();
    assert_eq!(
        patch_count(&midday),
        1,
        "the decision became AlreadyFired, whose message differs: {midday:?}"
    );
    let settled = stored_after(
        Some(&later.status.clone().expect("pass 2 carried a status")),
        &patched_status(&midday),
    );
    let settled_transition = settled
        .conditions
        .as_ref()
        .and_then(|c| c.first())
        .and_then(|c| c.last_transition_time)
        .expect("the settled condition carries a transition time");

    // PASS 3 — half an hour later, nothing about the schedule or the slot has
    // moved. ZERO requests to `/status`.
    //
    // HALF AN HOUR AND NOT AN HOUR SINCE PLAT-05.2, and the difference is a
    // fact and not a fudge. D1 §6.7 re-inventories a schedule every 60 minutes,
    // and `status.history.inventoriedAt` is when that last happened — so a pass
    // exactly 3 600 s after pass 2 DOES move the status, once, because it took
    // a real inventory. That is 24 writes a day, not the 2 880 this test is
    // about, and it is guarded on its own in `tests/schedule_history.rs`
    // (`the_hourly_inventory_is_the_only_thing_that_moves_a_settled_status`).
    // Pass 3 therefore sits INSIDE the inventory window, where the property it
    // names — a recomputed status equal to the stored one writes nothing — is
    // the only thing in play.
    let mut steady = schedule("nightly", UID, DAILY, false);
    steady.status = Some(settled.clone());
    let (client, _calls, bodies) = mock_client_recording_bodies(daily_routes(&name, 409));
    let outcome = reconcile_schedule(&steady, &client, utc(2026, 9, 10, 12, 30))
        .await
        .expect("the steady reconcile completes");
    let third = bodies.lock().expect("readable").clone();
    assert_eq!(
        outcome.decision.reason(),
        REASON_SCHEDULED,
        "the decision is still computed and returned: {:?}",
        outcome.decision
    );
    assert_eq!(
        patch_count(&third),
        0,
        "a pass that computes the status the object already carries writes NOTHING. On a 30 s \
         requeue that is 2,880 API writes a day per schedule that this reconciler no longer \
         makes: {third:?}"
    );

    // PASS 4 — `suspend` flips. The ONE mutable field on this spec, and a real
    // state change: Ready=True/Scheduled becomes Ready=False/Suspended.
    let mut suspended = schedule("nightly", UID, DAILY, true);
    suspended.status = Some(settled);
    let (client, _calls, bodies) = mock_client_recording_bodies(daily_routes(&name, 409));
    reconcile_schedule(&suspended, &client, utc(2026, 9, 10, 14, 0))
        .await
        .expect("the suspend reconcile completes");
    let fourth = bodies.lock().expect("readable").clone();
    assert_eq!(
        patch_count(&fourth),
        1,
        "a real state change is written exactly once: {fourth:?}"
    );
    let status = patched_status(&fourth);
    assert_eq!(
        status["conditions"][0]["status"],
        serde_json::json!("False")
    );
    assert_eq!(
        status["conditions"][0]["lastTransitionTime"],
        serde_json::json!(utc(2026, 9, 10, 14, 0)),
        "and the transition time MOVES, because the condition transitioned: {status}"
    );
    assert_ne!(
        status["conditions"][0]["lastTransitionTime"],
        serde_json::json!(settled_transition),
        "a comparison that never moved the field would be as wrong as one that always did"
    );
}

// ===========================================================================
// PLAT-04.1 — ACTUAL-RUN CONCURRENCY
// ===========================================================================

#[test]
fn omitted_concurrency_policy_defaults_to_forbid_and_the_schema_validates_both_values() {
    let mut value: serde_json::Value =
        serde_json::from_str(&schedule_json("nightly", UID, DAILY, false)).unwrap();
    value["spec"]
        .as_object_mut()
        .expect("spec is an object")
        .remove("concurrencyPolicy");
    let old: BackupSchedule = serde_json::from_value(value).expect("an old schedule still parses");
    assert_eq!(old.spec.concurrency_policy, ConcurrencyPolicy::Forbid);

    let mut invalid: serde_json::Value =
        serde_json::from_str(&schedule_json("nightly", UID, DAILY, false)).unwrap();
    invalid["spec"]["concurrencyPolicy"] = serde_json::json!("Replace");
    assert!(
        serde_json::from_value::<BackupSchedule>(invalid).is_err(),
        "the typed model refuses policy values outside Forbid and Allow"
    );

    let crd = serde_json::to_value(BackupSchedule::crd()).expect("the CRD serializes");
    let policy = &crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]
        ["properties"]["concurrencyPolicy"];
    assert_eq!(policy["default"], serde_json::json!("Forbid"));
    assert_eq!(policy["enum"], serde_json::json!(["Forbid", "Allow"]));
    // PLAT-05.1 MADE IT EDITABLE, AND THAT IS THE ASSERTION NOW. Before D1 the
    // policy was sealed with the rest of the spec, so an old omitted-field
    // schedule needed a replacement object to opt into `Allow`; it is now a
    // one-field edit, and the seal names `sourceRef` and nothing else.
    assert!(
        !SOURCE_REF_IMMUTABLE_RULE.contains("concurrencyPolicy"),
        "concurrencyPolicy is editable policy (D1 §5.1) and must not appear in the seal"
    );
    assert!(
        SOURCE_REF_IMMUTABLE_RULE.contains("self.sourceRef == oldSelf.sourceRef"),
        "the seal still pins the one immutable field"
    );
}

#[tokio::test]
async fn a_long_running_previous_slot_blocks_the_next_slot_under_forbid() {
    let now = utc(2026, 9, 10, 12, 1);
    let previous = scheduled_backup_name("nightly", &slot_name(utc(2026, 9, 10, 12, 0))).unwrap();
    let mut schedule = forbid_schedule("nightly", UID, "* * * * *", false);
    schedule.status = Some(BackupScheduleStatus {
        active_backup_ref: Some(LocalRef {
            name: previous.clone(),
        }),
        ..BackupScheduleStatus::default()
    });
    let (client, calls, bodies) = mock_client_recording_bodies(vec![
        Route {
            method: "GET",
            path_suffix: "/namespaces/logweir-t18/backups",
            status: 200,
            body: backup_list_body(vec![backup_value(
                &previous,
                UID,
                Some("Running"),
                Some(&previous),
            )]),
        },
        absent_backup(&due_name(now)),
        Route {
            method: "PATCH",
            path_suffix: "/backupschedules/nightly/status",
            status: 200,
            body: serde_json::to_string(&schedule).unwrap(),
        },
        Route {
            method: "POST",
            path_suffix: "/namespaces/logweir-t18/backups",
            status: 201,
            body: created_backup_body("must-not-be-created"),
        },
    ]);

    let outcome = reconcile_schedule(&schedule, &client, now).await.unwrap();
    assert!(matches!(
        outcome.decision,
        SlotDecision::ConcurrencyBlocked { .. }
    ));
    assert_eq!(outcome.decision.reason(), REASON_CONCURRENCY_BLOCKED);
    assert_eq!(outcome.created, None);
    let seen = calls.lock().unwrap().clone();
    assert_eq!(seen.iter().filter(|call| call.method == "POST").count(), 0);
    let status = patched_status(&bodies.lock().unwrap());
    assert_eq!(status["activeBackupRef"]["name"], previous);
    assert_eq!(status["lastMissedSlot"], slot_name(now));
    assert!(status["conditions"][0]["message"]
        .as_str()
        .unwrap()
        .contains("was not fired because concurrencyPolicy Forbid"));
}

#[tokio::test]
async fn a_deleted_job_keeps_its_nonterminal_backup_conservatively_active() {
    let now = utc(2026, 9, 10, 12, 1);
    let previous = scheduled_backup_name("nightly", &slot_name(utc(2026, 9, 10, 12, 0))).unwrap();
    let schedule = forbid_schedule("nightly", UID, "* * * * *", false);
    let (client, calls) = mock_client_recording(vec![
        Route {
            method: "GET",
            path_suffix: "/namespaces/logweir-t18/backups",
            status: 200,
            // The referenced Job is deliberately absent. The Backup controller
            // may recreate it, so the schedule must not infer completion.
            body: backup_list_body(vec![backup_value(
                &previous,
                UID,
                Some("Running"),
                Some("deleted-job"),
            )]),
        },
        absent_backup(&due_name(now)),
        Route {
            method: "PATCH",
            path_suffix: "/backupschedules/nightly/status",
            status: 200,
            body: serde_json::to_string(&schedule).unwrap(),
        },
    ]);
    let outcome = reconcile_schedule(&schedule, &client, now).await.unwrap();
    assert_eq!(outcome.decision.reason(), REASON_CONCURRENCY_BLOCKED);
    assert!(calls
        .lock()
        .unwrap()
        .iter()
        .all(|call| !call.uri.contains("/jobs")));
}

#[tokio::test]
async fn a_terminal_backup_clears_the_old_ref_and_admits_the_new_slot() {
    let now = utc(2026, 9, 10, 12, 1);
    let previous = scheduled_backup_name("nightly", &slot_name(utc(2026, 9, 10, 12, 0))).unwrap();
    let current = scheduled_backup_name("nightly", &slot_name(now)).unwrap();
    let mut schedule = forbid_schedule("nightly", UID, "* * * * *", false);
    schedule.status = Some(BackupScheduleStatus {
        active_backup_ref: Some(LocalRef {
            name: previous.clone(),
        }),
        ..BackupScheduleStatus::default()
    });
    let (client, calls, bodies) = mock_client_recording_bodies(vec![
        Route {
            method: "GET",
            path_suffix: "/namespaces/logweir-t18/backups",
            status: 200,
            body: backup_list_body(vec![backup_value(
                &previous,
                UID,
                Some("Succeeded"),
                Some(&previous),
            )]),
        },
        absent_backup(&current),
        Route {
            // W0: THE RESERVATION IS A `PATCH` — see `is_reservation`. This
            // route and the finalization one below answer the same method and
            // path, which is exactly what the shipped RBAC grants.
            method: "PATCH",
            path_suffix: "/backupschedules/nightly/status",
            status: 200,
            body: reservation_echo(
                &serde_json::to_string(&schedule).unwrap(),
                &current,
                &slot_name(now),
                0,
            ),
        },
        Route {
            method: "POST",
            path_suffix: "/namespaces/logweir-t18/backups",
            status: 201,
            body: created_backup_body(&current),
        },
        Route {
            method: "PATCH",
            path_suffix: "/backupschedules/nightly/status",
            status: 200,
            body: serde_json::to_string(&schedule).unwrap(),
        },
    ]);
    let outcome = reconcile_schedule(&schedule, &client, now).await.unwrap();
    assert_eq!(outcome.created.as_deref(), Some(current.as_str()));
    let methods: Vec<String> = calls
        .lock()
        .unwrap()
        .iter()
        .map(|call| call.method.clone())
        .collect();
    assert_eq!(
        methods,
        ["GET", "GET", "PATCH", "POST", "PATCH"],
        "the bootstrap LIST, the slot's own attempt-0 GET (D1 §4.5 step 6), the reservation, \
         the create and the finalization — in that order and nothing else"
    );
    let status = patched_status(&bodies.lock().unwrap());
    assert_eq!(status["activeBackupRef"]["name"], current);
    assert!(status["pendingBackupRef"].is_null());
}

#[tokio::test]
async fn a_stale_active_reference_is_cleared_before_the_new_slot_is_admitted() {
    let now = utc(2026, 9, 10, 12, 1);
    let current = scheduled_backup_name("nightly", &slot_name(now)).unwrap();
    let mut schedule = forbid_schedule("nightly", UID, "* * * * *", false);
    schedule.status = Some(BackupScheduleStatus {
        active_backup_ref: Some(LocalRef {
            name: "deleted-backup".to_string(),
        }),
        ..BackupScheduleStatus::default()
    });
    let (client, _calls, bodies) = mock_client_recording_bodies(vec![
        Route {
            method: "GET",
            path_suffix: "/namespaces/logweir-t18/backups",
            status: 200,
            body: backup_list_body(vec![]),
        },
        absent_backup(&current),
        Route {
            // W0: THE RESERVATION IS A `PATCH` — see `is_reservation`. This
            // route and the finalization one below answer the same method and
            // path, which is exactly what the shipped RBAC grants.
            method: "PATCH",
            path_suffix: "/backupschedules/nightly/status",
            status: 200,
            body: reservation_echo(
                &serde_json::to_string(&schedule).unwrap(),
                &current,
                &slot_name(now),
                0,
            ),
        },
        Route {
            method: "POST",
            path_suffix: "/namespaces/logweir-t18/backups",
            status: 201,
            body: created_backup_body(&current),
        },
        Route {
            method: "PATCH",
            path_suffix: "/backupschedules/nightly/status",
            status: 200,
            body: serde_json::to_string(&schedule).unwrap(),
        },
    ]);
    reconcile_schedule(&schedule, &client, now).await.unwrap();
    let recorded = bodies.lock().unwrap().clone();
    let reservation = reserved_status(&recorded);
    // AN EXPLICIT `null` AND NOT AN OMITTED KEY. The reservation is a merge
    // patch now, so the stale reference is cleared only if the body SAYS null.
    assert_eq!(reservation["activeBackupRef"], serde_json::Value::Null);
    assert_eq!(reservation["pendingBackupRef"]["name"], current);
    // And the CAS token the reservation is conditional on.
    let raw: serde_json::Value = serde_json::from_str(
        &recorded
            .iter()
            .find(|body| body.method == "PATCH")
            .unwrap()
            .body,
    )
    .unwrap();
    assert_eq!(raw["metadata"]["resourceVersion"], serde_json::json!("17"));
    assert_eq!(raw["metadata"]["name"], serde_json::json!("nightly"));
}

#[tokio::test]
async fn restart_after_child_creation_adopts_the_owned_child_and_clears_the_reservation() {
    let now = utc(2026, 9, 10, 12, 1);
    let current = scheduled_backup_name("nightly", &slot_name(now)).unwrap();
    let mut schedule = forbid_schedule("nightly", UID, "* * * * *", false);
    schedule.status = Some(BackupScheduleStatus {
        pending_backup_ref: Some(LocalRef {
            name: current.clone(),
        }),
        ..BackupScheduleStatus::default()
    });
    let (client, calls, bodies) = mock_client_recording_bodies(vec![
        Route {
            method: "GET",
            path_suffix: "/namespaces/logweir-t18/backups",
            status: 200,
            body: backup_list_body(vec![backup_value(&current, UID, None, None)]),
        },
        Route {
            method: "GET",
            path_suffix: Box::leak(format!("/backups/{current}").into_boxed_str()),
            status: 200,
            body: backup_value(&current, UID, None, None).to_string(),
        },
        Route {
            method: "PATCH",
            path_suffix: "/backupschedules/nightly/status",
            status: 200,
            body: serde_json::to_string(&schedule).unwrap(),
        },
    ]);
    let outcome = reconcile_schedule(&schedule, &client, now).await.unwrap();
    assert!(outcome.already_existed);
    assert_eq!(outcome.created.as_deref(), Some(current.as_str()));
    assert_eq!(
        calls
            .lock()
            .unwrap()
            .iter()
            .filter(|call| call.method == "POST")
            .count(),
        0
    );
    let status = patched_status(&bodies.lock().unwrap());
    assert_eq!(status["activeBackupRef"]["name"], current);
    assert!(status["pendingBackupRef"].is_null());
}

#[tokio::test]
async fn allow_explicitly_permits_a_new_slot_without_active_run_admission() {
    let now = utc(2026, 9, 10, 12, 1);
    let current = scheduled_backup_name("nightly", &slot_name(now)).unwrap();
    let previous = scheduled_backup_name("nightly", &slot_name(utc(2026, 9, 10, 12, 0))).unwrap();
    let mut schedule = schedule("nightly", UID, "* * * * *", false);
    schedule.status = Some(BackupScheduleStatus {
        active_backup_ref: Some(LocalRef { name: previous }),
        ..BackupScheduleStatus::default()
    });
    assert_eq!(schedule.spec.concurrency_policy, ConcurrencyPolicy::Allow);
    let (client, calls) = mock_client_recording(vec![
        // `Allow` RESERVES TOO NOW (D1 §4.5 ADMIT). The reservation used to be
        // `Forbid`-only, which left `Allow` runs unrecorded between the create
        // and the status write and gave the two policies different crash
        // semantics. It is uniform, so `status.activeRuns` is complete by
        // construction for both.
        no_backups(),
        absent_backup(&current),
        Route {
            method: "POST",
            path_suffix: "/namespaces/logweir-t18/backups",
            status: 201,
            body: created_backup_body(&current),
        },
        Route {
            method: "PATCH",
            path_suffix: "/backupschedules/nightly/status",
            status: 200,
            body: reservation_echo(
                &serde_json::to_string(&schedule).unwrap(),
                &current,
                &slot_name(now),
                0,
            ),
        },
    ]);
    reconcile_schedule(&schedule, &client, now).await.unwrap();
    let methods: Vec<String> = calls
        .lock()
        .unwrap()
        .iter()
        .map(|call| call.method.clone())
        .collect();
    assert_eq!(
        methods,
        ["GET", "GET", "PATCH", "POST", "PATCH"],
        "the bootstrap LIST, the slot's attempt-0 GET, the reservation, the create and the \
         finalization: `Allow` takes exactly the same path as `Forbid`, and admits where \
         `Forbid` would have been blocked"
    );
}

#[tokio::test]
async fn an_unknown_previous_run_state_blocks_conservatively_under_forbid() {
    let now = utc(2026, 9, 10, 12, 1);
    let previous = scheduled_backup_name("nightly", &slot_name(utc(2026, 9, 10, 12, 0))).unwrap();
    let schedule = forbid_schedule("nightly", UID, "* * * * *", false);
    let (client, calls) = mock_client_recording(vec![
        Route {
            method: "GET",
            path_suffix: "/namespaces/logweir-t18/backups",
            status: 200,
            body: backup_list_body(vec![backup_value(&previous, UID, None, None)]),
        },
        absent_backup(&due_name(now)),
        Route {
            method: "PATCH",
            path_suffix: "/backupschedules/nightly/status",
            status: 200,
            body: serde_json::to_string(&schedule).unwrap(),
        },
    ]);
    let outcome = reconcile_schedule(&schedule, &client, now).await.unwrap();
    assert_eq!(outcome.decision.reason(), REASON_CONCURRENCY_BLOCKED);
    assert_eq!(
        calls
            .lock()
            .unwrap()
            .iter()
            .filter(|call| call.method == "POST")
            .count(),
        0
    );
}

#[tokio::test]
async fn allow_still_clears_a_completed_singular_active_reference_between_slots() {
    let due = utc(2026, 9, 10, 0, 0);
    let previous = scheduled_backup_name("nightly", &slot_name(due)).unwrap();
    let mut schedule = schedule("nightly", UID, DAILY, false);
    schedule.status = Some(BackupScheduleStatus {
        last_fire_time: Some(due),
        active_backup_ref: Some(LocalRef {
            name: previous.clone(),
        }),
        ..BackupScheduleStatus::default()
    });
    let (client, _calls, bodies) = mock_client_recording_bodies(vec![
        Route {
            method: "GET",
            path_suffix: "/namespaces/logweir-t18/backups",
            status: 200,
            body: backup_list_body(vec![backup_value(
                &previous,
                UID,
                Some("Succeeded"),
                Some(&previous),
            )]),
        },
        Route {
            method: "GET",
            path_suffix: Box::leak(format!("/backups/{previous}").into_boxed_str()),
            status: 200,
            body: backup_value(&previous, UID, Some("Succeeded"), Some(&previous)).to_string(),
        },
        Route {
            method: "PATCH",
            path_suffix: "/backupschedules/nightly/status",
            status: 200,
            body: serde_json::to_string(&schedule).unwrap(),
        },
    ]);
    reconcile_schedule(&schedule, &client, utc(2026, 9, 10, 12, 0))
        .await
        .unwrap();
    assert!(patched_status(&bodies.lock().unwrap())["activeBackupRef"].is_null());
}

#[tokio::test]
async fn a_wrong_owner_uid_at_the_deterministic_name_is_not_adopted() {
    let now = utc(2026, 9, 10, 12, 1);
    let current = scheduled_backup_name("nightly", &slot_name(now)).unwrap();
    let schedule = forbid_schedule("nightly", UID, "* * * * *", false);
    let foreign = backup_value(&current, OTHER_UID, Some("Running"), None);
    let (client, calls, bodies) = mock_client_recording_bodies(vec![
        Route {
            method: "GET",
            path_suffix: "/namespaces/logweir-t18/backups",
            status: 200,
            body: backup_list_body(vec![foreign.clone()]),
        },
        Route {
            method: "GET",
            path_suffix: Box::leak(format!("/backups/{current}").into_boxed_str()),
            status: 200,
            body: foreign.to_string(),
        },
        // PRESENT AND NOT USED, so the zero-POST assertion below is a choice
        // rather than an inability.
        Route {
            method: "POST",
            path_suffix: "/namespaces/logweir-t18/backups",
            status: 201,
            body: created_backup_body(&current),
        },
        Route {
            method: "PATCH",
            path_suffix: "/backupschedules/nightly/status",
            status: 200,
            body: serde_json::to_string(&schedule).unwrap(),
        },
    ]);
    let outcome = reconcile_schedule(&schedule, &client, now).await.unwrap();
    // D1 §4.7 ROW 6, AND IT IS A REPORT RATHER THAN AN ERROR NOW. A same-named
    // schedule recreated under a new UID leaves the old schedule's objects
    // sitting on the deterministic names of the new one's slots; treating that
    // as a reconcile failure would requeue forever and say nothing an operator
    // can act on. The slot is skipped, RECORDED, and never re-run under a
    // different name — the identity of a scheduled run is its name.
    assert_eq!(outcome.decision.reason(), "SlotNameUnavailable");
    assert_eq!(outcome.created, None);
    assert!(
        posts(&bodies.lock().unwrap()).is_empty(),
        "a foreign object at the deterministic name is never overwritten or worked around"
    );
    let status = patched_status(&bodies.lock().unwrap());
    assert_eq!(
        status["lastMissedSlot"],
        slot_name(now),
        "the skip is recorded, so a correct implementation is distinguishable from a schedule \
         that is simply not firing: {status}"
    );
    assert_eq!(
        status["lastSlot"]["disposition"],
        serde_json::json!("NameUnavailable")
    );
    assert!(status["conditions"][0]["message"]
        .as_str()
        .unwrap()
        .contains("held by an object this schedule does not own"));
    assert_eq!(calls.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn a_backup_list_api_failure_never_admits_or_creates_work() {
    let schedule = forbid_schedule("nightly", UID, "* * * * *", false);
    let (client, calls) = mock_client_recording(vec![Route {
        method: "GET",
        path_suffix: "/namespaces/logweir-t18/backups",
        status: 500,
        body: SERVER_ERROR_BODY.to_string(),
    }]);
    assert!(
        reconcile_schedule(&schedule, &client, utc(2026, 9, 10, 12, 1))
            .await
            .is_err()
    );
    assert_eq!(calls.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn a_child_create_failure_leaves_the_atomic_reservation_for_restart() {
    let now = utc(2026, 9, 10, 12, 1);
    let current = scheduled_backup_name("nightly", &slot_name(now)).unwrap();
    let schedule = forbid_schedule("nightly", UID, "* * * * *", false);
    let (client, calls, bodies) = mock_client_recording_bodies(vec![
        Route {
            method: "GET",
            path_suffix: "/namespaces/logweir-t18/backups",
            status: 200,
            body: backup_list_body(vec![]),
        },
        absent_backup(&current),
        Route {
            // W0: THE RESERVATION IS A `PATCH` — see `is_reservation`. This
            // route and the finalization one below answer the same method and
            // path, which is exactly what the shipped RBAC grants.
            method: "PATCH",
            path_suffix: "/backupschedules/nightly/status",
            status: 200,
            body: reservation_echo(
                &serde_json::to_string(&schedule).unwrap(),
                &current,
                &slot_name(now),
                0,
            ),
        },
        Route {
            method: "POST",
            path_suffix: "/namespaces/logweir-t18/backups",
            status: 500,
            body: SERVER_ERROR_BODY.to_string(),
        },
    ]);
    assert!(reconcile_schedule(&schedule, &client, now).await.is_err());
    let methods: Vec<String> = calls
        .lock()
        .unwrap()
        .iter()
        .map(|call| call.method.clone())
        .collect();
    assert_eq!(methods, ["GET", "GET", "PATCH", "POST"]);
    let reservation = reserved_status(&bodies.lock().unwrap());
    assert_eq!(reservation["pendingBackupRef"]["name"], current);
}

#[tokio::test]
async fn restart_after_reservation_resumes_the_accepted_slot_without_readmitting_it() {
    let now = utc(2026, 9, 10, 12, 1);
    let current = scheduled_backup_name("nightly", &slot_name(now)).unwrap();
    let mut schedule = forbid_schedule("nightly", UID, "* * * * *", false);
    schedule.status = Some(BackupScheduleStatus {
        pending_backup_ref: Some(LocalRef {
            name: current.clone(),
        }),
        ..BackupScheduleStatus::default()
    });
    let (client, calls, bodies) = mock_client_recording_bodies(vec![
        Route {
            method: "GET",
            path_suffix: "/namespaces/logweir-t18/backups",
            status: 200,
            body: backup_list_body(vec![]),
        },
        Route {
            method: "POST",
            path_suffix: "/namespaces/logweir-t18/backups",
            status: 201,
            body: created_backup_body(&current),
        },
        Route {
            method: "PATCH",
            path_suffix: "/backupschedules/nightly/status",
            status: 200,
            body: serde_json::to_string(&schedule).unwrap(),
        },
    ]);
    let outcome = reconcile_schedule(&schedule, &client, now).await.unwrap();
    assert_eq!(outcome.created.as_deref(), Some(current.as_str()));
    let methods: Vec<String> = calls
        .lock()
        .unwrap()
        .iter()
        .map(|call| call.method.clone())
        .collect();
    assert_eq!(methods, ["GET", "POST", "PATCH"]);
    let status = patched_status(&bodies.lock().unwrap());
    assert_eq!(status["activeBackupRef"]["name"], current);
    assert!(status["pendingBackupRef"].is_null());
}

#[tokio::test]
async fn an_accepted_reservation_survives_past_the_missed_slot_horizon() {
    let due = utc(2026, 9, 10, 0, 0);
    let now = utc(2026, 9, 10, 12, 0);
    let reserved = scheduled_backup_name("nightly", &slot_name(due)).unwrap();
    let mut schedule = forbid_schedule("nightly", UID, DAILY, false);
    schedule.status = Some(BackupScheduleStatus {
        pending_backup_ref: Some(LocalRef {
            name: reserved.clone(),
        }),
        ..BackupScheduleStatus::default()
    });
    let (client, calls, bodies) = mock_client_recording_bodies(vec![
        Route {
            method: "GET",
            path_suffix: "/namespaces/logweir-t18/backups",
            status: 200,
            body: backup_list_body(vec![]),
        },
        Route {
            method: "POST",
            path_suffix: "/namespaces/logweir-t18/backups",
            status: 201,
            body: created_backup_body(&reserved),
        },
        Route {
            method: "PATCH",
            path_suffix: "/backupschedules/nightly/status",
            status: 200,
            body: serde_json::to_string(&schedule).unwrap(),
        },
    ]);
    let outcome = reconcile_schedule(&schedule, &client, now).await.unwrap();
    assert_eq!(outcome.created.as_deref(), Some(reserved.as_str()));
    assert_eq!(
        calls
            .lock()
            .unwrap()
            .iter()
            .filter(|call| call.method == "POST")
            .count(),
        1
    );
    let status = patched_status(&bodies.lock().unwrap());
    assert_eq!(status["lastFireTime"], serde_json::json!(due));
    assert_eq!(status["activeBackupRef"]["name"], reserved);
    assert!(status["pendingBackupRef"].is_null());
}

#[tokio::test]
async fn a_newer_due_slot_cannot_overtake_an_accepted_previous_reservation() {
    let accepted_due = utc(2026, 9, 10, 12, 0);
    let now = utc(2026, 9, 10, 12, 1);
    let accepted = scheduled_backup_name("nightly", &slot_name(accepted_due)).unwrap();
    let mut schedule = forbid_schedule("nightly", UID, "* * * * *", false);
    schedule.status = Some(BackupScheduleStatus {
        pending_backup_ref: Some(LocalRef {
            name: accepted.clone(),
        }),
        ..BackupScheduleStatus::default()
    });
    let (client, _calls, bodies) = mock_client_recording_bodies(vec![
        Route {
            method: "GET",
            path_suffix: "/namespaces/logweir-t18/backups",
            status: 200,
            body: backup_list_body(vec![]),
        },
        Route {
            method: "POST",
            path_suffix: "/namespaces/logweir-t18/backups",
            status: 201,
            body: created_backup_body(&accepted),
        },
        Route {
            method: "PATCH",
            path_suffix: "/backupschedules/nightly/status",
            status: 200,
            body: serde_json::to_string(&schedule).unwrap(),
        },
    ]);
    let outcome = reconcile_schedule(&schedule, &client, now).await.unwrap();
    // THE ACCEPTED RESERVATION IS RESUMED AND THE NEWER SLOT IS NOT CONSIDERED
    // AT ALL. D1 §4.5 step 2 goes straight to the status write after creating a
    // reserved child, which is what makes the boundary atomic: one admission
    // per reconcile, and the newer slot is decided on the next pass — where the
    // run just created is exactly what blocks it under `Forbid`.
    assert_eq!(outcome.created.as_deref(), Some(accepted.as_str()));
    assert_eq!(outcome.decision.reason(), REASON_SCHEDULED);
    assert_eq!(
        posts(&bodies.lock().unwrap())
            .iter()
            .map(|b| body_name(b))
            .collect::<Vec<_>>(),
        vec![accepted.clone()],
        "exactly one POST, and it is the RESERVED name and not the newer slot's"
    );
    let status = patched_status(&bodies.lock().unwrap());
    assert_eq!(status["lastFireTime"], serde_json::json!(accepted_due));
    assert_eq!(status["activeBackupRef"]["name"], accepted);
    assert_cleared(&status, "pendingBackupRef");
    assert_cleared(&status, "pendingRun");
}

#[tokio::test]
async fn two_controller_replicas_cannot_admit_different_slots_from_the_same_resource_version() {
    use http::{Request, Response};
    use http_body_util::BodyExt as _;
    use kube::client::Body;
    use std::sync::{Arc, Mutex};
    use tower::service_fn;

    struct ApiState {
        /// The stored object, whose `metadata.resourceVersion` is the CAS
        /// token — moved by every accepted status write, exactly as the API
        /// server moves it.
        stored: serde_json::Value,
        /// What each RESERVATION patch offered as its precondition, in order:
        /// `None` for a body that carried no `metadata.resourceVersion` at all.
        /// Reservations only, because a winner's finalization legitimately
        /// offers the token it was just handed, and the two interleave.
        reservation_tokens: Vec<Option<String>>,
        reservations_accepted: usize,
        posted_names: Vec<String>,
    }

    let schedule = forbid_schedule("nightly", UID, "* * * * *", false);
    let state = Arc::new(Mutex::new(ApiState {
        stored: serde_json::to_value(&schedule).unwrap(),
        reservation_tokens: Vec::new(),
        reservations_accepted: 0,
        posted_names: Vec::new(),
    }));
    let service = {
        let state = Arc::clone(&state);
        service_fn(move |request: Request<Body>| {
            let state = Arc::clone(&state);
            async move {
                let method = request.method().as_str().to_string();
                let path = request.uri().path().to_string();
                let body = request
                    .into_body()
                    .collect()
                    .await
                    .map(|collected| collected.to_bytes())
                    .unwrap_or_default();
                let (status, response_body) = if method == "GET" && path.ends_with("/backups") {
                    // Both replicas are allowed to observe the same empty list;
                    // the status resourceVersion is the actual admission CAS.
                    (200, backup_list_body(vec![]))
                } else if method == "GET" && path.contains("/backups/") {
                    // AND THE SAME 404 FOR THE ATTEMPT CHAIN. Both replicas ask
                    // the slot's deterministic name whether it has a run, and
                    // both are told it does not — which is the whole premise of
                    // this race: two replicas that agree about the world and
                    // disagree only about who gets to act on it.
                    let name = path.rsplit('/').next().unwrap_or_default().to_string();
                    (404, not_found_body(&name))
                } else if method == "PATCH" && path.ends_with("/backupschedules/nightly/status") {
                    // THE API SERVER'S OWN RULE, MODELLED AND NOT ASSERTED.
                    // A merge patch is applied to the CURRENT object, so a body
                    // that carries no `metadata.resourceVersion` inherits the
                    // stored one and can never conflict — which is precisely
                    // what dropping the precondition would do. Modelling that
                    // (rather than asserting the token inside the double) is
                    // what makes the mutant show up as TWO winners and TWO
                    // POSTs instead of as a panic on a spawned task.
                    let patch: serde_json::Value = serde_json::from_slice(&body).unwrap();
                    let mut state = state.lock().unwrap();
                    let stored_version = state.stored["metadata"]["resourceVersion"]
                        .as_str()
                        .unwrap()
                        .to_string();
                    let offered = patch["metadata"]["resourceVersion"]
                        .as_str()
                        .map(str::to_string);
                    let reserving = is_reservation(&patch["status"]);
                    if reserving {
                        state.reservation_tokens.push(offered.clone());
                    }
                    if offered.as_ref().is_some_and(|v| *v != stored_version) {
                        (
                            409,
                            r#"{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Conflict","message":"the object has been modified","code":409}"#.to_string(),
                        )
                    } else {
                        if reserving {
                            state.reservations_accepted += 1;
                        }
                        let mut stored = state.stored.clone();
                        weirkeeper::conditions::apply_merge_patch(&mut stored, &patch);
                        stored["metadata"]["resourceVersion"] = serde_json::json!(stored_version
                            .parse::<u64>()
                            .map(|v| (v + 1).to_string())
                            .unwrap());
                        state.stored = stored;
                        (200, state.stored.to_string())
                    }
                } else if method == "POST" && path.ends_with("/backups") {
                    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
                    state
                        .lock()
                        .unwrap()
                        .posted_names
                        .push(value["metadata"]["name"].as_str().unwrap().to_string());
                    (201, String::from_utf8(body.to_vec()).unwrap())
                } else {
                    panic!("unexpected replica-race request: {method} {path}")
                };
                Ok::<_, std::convert::Infallible>(
                    Response::builder()
                        .status(status)
                        .body(Body::from(response_body.into_bytes()))
                        .unwrap(),
                )
            }
        })
    };
    let client = kube::Client::new(service, "default");
    let a = schedule.clone();
    let b = schedule;
    let (first, second) = tokio::join!(
        reconcile_schedule(&a, &client, utc(2026, 9, 10, 12, 0)),
        reconcile_schedule(&b, &client, utc(2026, 9, 10, 12, 1)),
    );
    assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
    assert_eq!(
        usize::from(first.is_err()) + usize::from(second.is_err()),
        1
    );
    let state = state.lock().unwrap();
    assert_eq!(
        state.reservations_accepted, 1,
        "exactly one replica may take the slot reservation: {:?}",
        state.reservation_tokens
    );
    assert_eq!(
        state.posted_names.len(),
        1,
        "only the resourceVersion winner may create its deterministic slot child"
    );
    // THE MUTANT THIS ROW IS AGAINST. Both replicas hold the watched object,
    // so both MUST offer `17` as their precondition; a reservation patch sent
    // without one (`None` here) is applied to whatever the object has become,
    // both replicas win, and the two assertions above both see 2.
    assert_eq!(
        state.reservation_tokens,
        vec![Some("17".to_string()), Some("17".to_string())],
        "both stale replicas must submit the watched resourceVersion as the CAS token"
    );
}

#[tokio::test]
async fn failed_and_refused_children_each_release_forbid_admission() {
    let now = utc(2026, 9, 10, 12, 1);
    let previous = scheduled_backup_name("nightly", &slot_name(utc(2026, 9, 10, 12, 0)))
        .expect("the previous name fits");
    let current = scheduled_backup_name("nightly", &slot_name(now)).expect("the current name fits");

    for phase in ["Failed", "Refused"] {
        let mut schedule = forbid_schedule("nightly", UID, "* * * * *", false);
        schedule.status = Some(BackupScheduleStatus {
            active_backup_ref: Some(LocalRef {
                name: previous.clone(),
            }),
            ..BackupScheduleStatus::default()
        });
        let (client, calls) = mock_client_recording(vec![
            Route {
                method: "GET",
                path_suffix: "/namespaces/logweir-t18/backups",
                status: 200,
                body: backup_list_body(vec![backup_value(
                    &previous,
                    UID,
                    Some(phase),
                    Some(&previous),
                )]),
            },
            absent_backup(&current),
            Route {
                // W0: the reservation is a `PATCH`; see `is_reservation`.
                method: "PATCH",
                path_suffix: "/backupschedules/nightly/status",
                status: 200,
                body: reservation_echo(
                    &serde_json::to_string(&schedule).unwrap(),
                    &current,
                    &slot_name(now),
                    0,
                ),
            },
            Route {
                method: "POST",
                path_suffix: "/namespaces/logweir-t18/backups",
                status: 201,
                body: created_backup_body(&current),
            },
            Route {
                method: "PATCH",
                path_suffix: "/backupschedules/nightly/status",
                status: 200,
                body: serde_json::to_string(&schedule).unwrap(),
            },
        ]);

        let outcome = reconcile_schedule(&schedule, &client, now)
            .await
            .unwrap_or_else(|e| panic!("terminal phase {phase} releases admission: {e}"));
        assert_eq!(outcome.created.as_deref(), Some(current.as_str()));
        let methods: Vec<String> = calls
            .lock()
            .unwrap()
            .iter()
            .map(|call| call.method.clone())
            .collect();
        assert_eq!(
            methods,
            ["GET", "GET", "PATCH", "POST", "PATCH"],
            "{phase}: the bootstrap listing, the slot's own attempt-0 GET, the reservation, the \
             create and the finalization"
        );
    }
}

#[tokio::test]
async fn an_invalid_pending_reference_is_cleared_with_a_resource_version_precondition() {
    let mut schedule = forbid_schedule("nightly", UID, DAILY, true);
    schedule.status = Some(BackupScheduleStatus {
        pending_backup_ref: Some(LocalRef {
            name: "not-a-deterministic-slot-name".to_string(),
        }),
        ..BackupScheduleStatus::default()
    });
    let (client, calls, bodies) = mock_client_recording_bodies(vec![
        Route {
            method: "GET",
            path_suffix: "/namespaces/logweir-t18/backups",
            status: 200,
            body: backup_list_body(vec![]),
        },
        Route {
            method: "PATCH",
            path_suffix: "/backupschedules/nightly/status",
            status: 200,
            body: serde_json::to_string(&schedule).unwrap(),
        },
    ]);

    reconcile_schedule(&schedule, &client, utc(2026, 9, 10, 12, 0))
        .await
        .expect("invalid pending status is stale, not accepted work");
    let recorded = bodies.lock().unwrap().clone();
    let patch: serde_json::Value = serde_json::from_str(
        &recorded
            .iter()
            .find(|body| body.method == "PATCH")
            .expect("stale status is cleared")
            .body,
    )
    .unwrap();
    // BOTH SPELLINGS, AND EXPLICITLY. `is_null()` passes for a key the patch
    // simply did not carry, and an absent key in a merge patch means "leave it
    // alone" — so the weaker form was green for a controller that had stopped
    // clearing the field at all. This schedule is SUSPENDED, so there is no
    // admission to clear it as a side effect: the clear has to be a decision of
    // its own, or the stale reservation stands forever and every reader shows
    // accepted work that does not exist.
    assert_cleared(&patch["status"], "pendingBackupRef");
    assert_cleared(&patch["status"], "pendingRun");
    assert_eq!(patch["metadata"]["name"], serde_json::json!("nightly"));
    assert_eq!(
        patch["metadata"]["resourceVersion"],
        serde_json::json!("17")
    );
    assert_eq!(
        calls
            .lock()
            .unwrap()
            .iter()
            .filter(|call| call.method == "POST")
            .count(),
        0,
        "an invalid reservation is cleared, never created"
    );
}

#[tokio::test]
async fn allow_rejects_foreign_ownerless_and_old_generation_409_winners() {
    let now = utc(2026, 9, 10, 12, 1);
    let current = scheduled_backup_name("nightly", &slot_name(now)).unwrap();
    // THE THREE WAYS TO FAIL MEMBERSHIP, RESPELLED FOR PLAT-05.2. They used to
    // be three shapes of ownerReference, because that was the authority; D1 §6.1
    // removed the reference, so `spec.scheduleRef` is. The cases are the same
    // three facts — a different schedule UID, no reference at all, and a
    // reference naming another schedule — plus a fourth that the old spelling
    // could not express: a LEGACY object, with no `scheduleRef.uid`, whose
    // surviving controller ownerReference names somebody else. Membership rule
    // 2 still reads that one, so it still has to be refused.
    let mut wrong_name = backup_value(&current, UID, Some("Running"), None);
    wrong_name["spec"]["scheduleRef"]["name"] = serde_json::json!("retired-nightly");
    let mut ownerless = backup_value(&current, UID, Some("Running"), None);
    ownerless["spec"]
        .as_object_mut()
        .unwrap()
        .remove("scheduleRef");
    let mut legacy_other_owner = backup_value(&current, UID, Some("Running"), None);
    legacy_other_owner["spec"]["scheduleRef"] = serde_json::json!({ "name": "nightly" });
    legacy_other_owner["metadata"]["ownerReferences"] = serde_json::json!([{
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "BackupSchedule",
        "name": "retired-nightly",
        "uid": OTHER_UID,
        "controller": true,
        "blockOwnerDeletion": true
    }]);

    for (case, holder) in [
        (
            "old UID",
            backup_value(&current, OTHER_UID, Some("Running"), None),
        ),
        ("no schedule reference", ownerless),
        ("wrong schedule name", wrong_name),
        ("legacy owner naming another schedule", legacy_other_owner),
    ] {
        let schedule = schedule("nightly", UID, "* * * * *", false);
        let (client, calls, bodies) = mock_client_recording_bodies(vec![
            no_backups(),
            Route {
                method: "GET",
                path_suffix: "/backups/logweir-backup-nightly-20260910-120100",
                status: 200,
                body: holder.to_string(),
            },
            // PRESENT AND NEVER USED in any of the three cases.
            Route {
                method: "POST",
                path_suffix: "/namespaces/logweir-t18/backups",
                status: 201,
                body: created_backup_body(&current),
            },
            Route {
                method: "PATCH",
                path_suffix: "/backupschedules/nightly/status",
                status: 200,
                body: serde_json::to_string(&schedule).unwrap(),
            },
        ]);
        // MEMBERSHIP IS CHECKED WHERE THE OBJECT IS FIRST SEEN, WHICH IS NOW
        // THE ATTEMPT-CHAIN GET RATHER THAN THE 409 WINNER. The three ways to
        // fail it are unchanged — a different schedule UID, no controller
        // ownerReference at all, and a controller reference naming another
        // schedule — and none of them is adopted, overwritten or worked around.
        let outcome = reconcile_schedule(&schedule, &client, now)
            .await
            .unwrap_or_else(|e| panic!("{case}: a held name is a decision, not an error: {e}"));
        assert_eq!(
            outcome.decision.reason(),
            "SlotNameUnavailable",
            "{case}: {:?}",
            outcome.decision
        );
        assert_eq!(outcome.created, None, "{case}");
        assert!(
            posts(&bodies.lock().unwrap()).is_empty(),
            "{case}: a POST route was available and was not used"
        );
        let methods: Vec<String> = calls
            .lock()
            .unwrap()
            .iter()
            .map(|call| call.method.clone())
            .collect();
        assert_eq!(methods, ["GET", "GET", "PATCH"], "{case}");
    }
}

#[tokio::test]
async fn allow_accepts_only_the_owned_409_winner_and_observes_its_terminal_state() {
    let now = utc(2026, 9, 10, 12, 1);
    let current = scheduled_backup_name("nightly", &slot_name(now)).unwrap();
    let schedule = schedule("nightly", UID, "* * * * *", false);
    let (client, calls, bodies) = mock_client_recording_bodies(vec![
        no_backups(),
        Route {
            method: "GET",
            path_suffix: "/backups/logweir-backup-nightly-20260910-120100",
            status: 200,
            body: backup_value(&current, UID, Some("Failed"), None).to_string(),
        },
        // PRESENT AND UNUSED: the slot's own object answers the question the
        // POST used to.
        Route {
            method: "POST",
            path_suffix: "/namespaces/logweir-t18/backups",
            status: 409,
            body: already_exists_body(&current),
        },
        Route {
            method: "PATCH",
            path_suffix: "/backupschedules/nightly/status",
            status: 200,
            body: serde_json::to_string(&schedule).unwrap(),
        },
    ]);

    // THE OWNED OBJECT IS OBSERVED, NOT ADOPTED THROUGH A COLLISION. D1 §4.5
    // step 6 asks the slot's deterministic name what happened to it before
    // deciding anything, so the terminal state of an owned run reaches the
    // status without a `POST` having to fail first. The `Failed` run has no
    // exit code and no terminal condition, so it is not retryable — a failure
    // nobody classified is never re-run.
    let outcome = reconcile_schedule(&schedule, &client, now).await.unwrap();
    assert!(outcome.already_existed);
    assert_eq!(outcome.created.as_deref(), Some(current.as_str()));
    assert!(
        posts(&bodies.lock().unwrap()).is_empty(),
        "a POST route was available and was not used"
    );
    let status = patched_status(&bodies.lock().unwrap());
    assert!(
        status["activeBackupRef"].is_null(),
        "a terminal run is not active: {status}"
    );
    assert_eq!(
        status["activeRuns"],
        serde_json::json!([]),
        "and the typed list says so explicitly, which is what turns the next reconcile into \
         the O(active) branch instead of a namespace LIST: {status}"
    );
    assert_eq!(outcome.decision.reason(), "RunFailed");
    let methods: Vec<String> = calls
        .lock()
        .unwrap()
        .iter()
        .map(|call| call.method.clone())
        .collect();
    assert_eq!(methods, ["GET", "GET", "PATCH"]);
}

#[tokio::test]
async fn allow_409_followed_by_get_404_is_transient_not_owned_success() {
    let now = utc(2026, 9, 10, 12, 1);
    let current = scheduled_backup_name("nightly", &slot_name(now)).unwrap();
    let schedule = schedule("nightly", UID, "* * * * *", false);
    let not_found = r#"{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"NotFound","message":"the winner is not observable yet","code":404}"#;
    let (client, calls) = mock_client_recording(vec![
        no_backups(),
        Route {
            method: "GET",
            path_suffix: "/backups/logweir-backup-nightly-20260910-120100",
            status: 404,
            body: not_found.to_string(),
        },
        Route {
            method: "PATCH",
            path_suffix: "/backupschedules/nightly/status",
            status: 200,
            body: reservation_echo(
                &serde_json::to_string(&schedule).unwrap(),
                &current,
                &slot_name(now),
                0,
            ),
        },
        Route {
            method: "POST",
            path_suffix: "/namespaces/logweir-t18/backups",
            status: 409,
            body: already_exists_body(&current),
        },
    ]);

    // THE READ THAT HAS NOT CAUGHT UP. The attempt-chain GET says the slot has
    // no run, so the slot is admitted and the create collides — and the GET
    // that follows the 409 still says 404. That is a transient disagreement
    // between two reads, not ownership: the reconcile requeues rather than
    // writing a success status for an object it has never seen.
    let error = reconcile_schedule(&schedule, &client, now)
        .await
        .expect_err("a 404 after 409 must requeue rather than claim ownership");
    assert!(error.to_string().contains("404"));
    let methods: Vec<String> = calls
        .lock()
        .unwrap()
        .iter()
        .map(|call| call.method.clone())
        .collect();
    assert_eq!(methods, ["GET", "GET", "PATCH", "POST", "GET"]);
}

#[tokio::test]
async fn safe_replacement_drains_an_old_omitted_policy_schedule_and_retains_its_history() {
    let old_run = scheduled_backup_name("nightly", &slot_name(utc(2026, 9, 10, 12, 0))).unwrap();
    let mut old_value: serde_json::Value =
        serde_json::from_str(&schedule_json("nightly", UID, "* * * * *", true)).unwrap();
    old_value["spec"]
        .as_object_mut()
        .unwrap()
        .remove("concurrencyPolicy");
    let mut old: BackupSchedule = serde_json::from_value(old_value).unwrap();
    old.status = Some(BackupScheduleStatus {
        active_backup_ref: Some(LocalRef {
            name: old_run.clone(),
        }),
        ..BackupScheduleStatus::default()
    });
    assert_eq!(old.spec.concurrency_policy, ConcurrencyPolicy::Forbid);

    let (client, calls) = mock_client_recording(vec![
        Route {
            method: "GET",
            path_suffix: "/namespaces/logweir-t18/backups",
            status: 200,
            body: backup_list_body(vec![backup_value(
                &old_run,
                UID,
                Some("Running"),
                Some(&old_run),
            )]),
        },
        Route {
            method: "PATCH",
            path_suffix: "/backupschedules/nightly/status",
            status: 200,
            body: serde_json::to_string(&old).unwrap(),
        },
    ]);
    let outcome = reconcile_schedule(&old, &client, utc(2026, 9, 10, 12, 1))
        .await
        .expect("the suspended old schedule observes its active child without firing");
    assert!(matches!(outcome.decision, SlotDecision::Suspended));
    assert!(calls
        .lock()
        .unwrap()
        .iter()
        .all(|call| call.method != "POST" && call.method != "DELETE"));

    // The old child reaches terminal while the old schedule stays suspended.
    // Reconciliation clears only the display reference; it does not delete the
    // terminal Backup or the schedule that anchors its history.
    let (client, drained_calls) = mock_client_recording(vec![
        Route {
            method: "GET",
            path_suffix: "/namespaces/logweir-t18/backups",
            status: 200,
            body: backup_list_body(vec![backup_value(
                &old_run,
                UID,
                Some("Succeeded"),
                Some(&old_run),
            )]),
        },
        Route {
            method: "PATCH",
            path_suffix: "/backupschedules/nightly/status",
            status: 200,
            body: serde_json::to_string(&old).unwrap(),
        },
    ]);
    reconcile_schedule(&old, &client, utc(2026, 9, 10, 12, 2))
        .await
        .expect("the suspended old schedule observes that every child is terminal");
    assert!(drained_calls
        .lock()
        .unwrap()
        .iter()
        .all(|call| call.method != "POST" && call.method != "DELETE"));

    // Only after that drain, a differently named schedule starts its own UID
    // generation while the old terminal Backup remains in the list as history.
    let replacement = forbid_schedule("nightly-v2", OTHER_UID, "* * * * *", false);
    let replacement_name =
        scheduled_backup_name("nightly-v2", &slot_name(utc(2026, 9, 10, 12, 3))).unwrap();
    let (client, replacement_calls) = mock_client_recording(vec![
        Route {
            method: "GET",
            path_suffix: "/namespaces/logweir-t18/backups",
            status: 200,
            body: backup_list_body(vec![backup_value(
                &old_run,
                UID,
                Some("Succeeded"),
                Some(&old_run),
            )]),
        },
        absent_backup(&replacement_name),
        Route {
            // W0: the reservation is a `PATCH`; see `is_reservation`.
            method: "PATCH",
            path_suffix: "/backupschedules/nightly-v2/status",
            status: 200,
            body: reservation_echo(
                &serde_json::to_string(&replacement).unwrap(),
                &replacement_name,
                &slot_name(utc(2026, 9, 10, 12, 3)),
                0,
            ),
        },
        Route {
            method: "POST",
            path_suffix: "/namespaces/logweir-t18/backups",
            status: 201,
            body: created_backup_body(&replacement_name),
        },
        Route {
            method: "PATCH",
            path_suffix: "/backupschedules/nightly-v2/status",
            status: 200,
            body: serde_json::to_string(&replacement).unwrap(),
        },
    ]);
    let replacement_outcome = reconcile_schedule(&replacement, &client, utc(2026, 9, 10, 12, 3))
        .await
        .expect("the differently named replacement starts after the old generation drains");
    assert_eq!(
        replacement_outcome.created.as_deref(),
        Some(replacement_name.as_str())
    );
    assert!(replacement_calls
        .lock()
        .unwrap()
        .iter()
        .all(|call| call.method != "DELETE"));

    // THE DOCUMENTED PROCEDURE THIS TEST IS PAIRED WITH CHANGED WITH PLAT-05.2,
    // and it changed because the reason for it went away. The old §9 said
    // "retain the old, suspended schedule, because deleting it can let
    // Kubernetes garbage collection delete that history" — true while every
    // run carried a controller ownerReference to its schedule, and false now
    // that none does (D1 §6.1). A test that still demanded that paragraph
    // would be holding the documentation to a hazard the code no longer has.
    //
    // What this test's BEHAVIOUR asserts is unchanged and still passes: the old
    // schedule is suspended, its running child is observed, the differently
    // named replacement fires its own slot, and NOTHING is deleted. What the
    // documentation must now say is the half an operator acts on.
    let docs = workspace_source("docs/kubernetes.md");
    let retained_history = docs
        .split("### Deleting a schedule keeps its history (PLAT-05.2)")
        .nth(1)
        .expect("§9 documents what deleting a schedule now does");
    for required in [
        "leaves every run",
        "--cascade=orphan",
        "`ScheduleNotFound`",
        "`HistoryRetained`",
        "`ActiveLegacyRunsOwned`",
        "Recreating a schedule under the same name",
        "new UID",
        "`SlotNameUnavailable`",
    ] {
        assert!(
            retained_history.contains(required),
            "the retained-history guidance must contain {required:?}"
        );
    }
    assert!(
        !docs.contains("Until PLAT-05.2 decouples retained history"),
        "and the drain-and-retain procedure, whose whole reason was the ownerReference this \
         task removed, must not still be standing beside it"
    );
}

#[tokio::test]
async fn an_old_finalizer_cannot_clear_a_newer_reservation_and_that_reservation_recovers() {
    use http::{Request, Response};
    use http_body_util::BodyExt as _;
    use kube::client::Body;
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use tokio::sync::Notify;
    use tower::service_fn;

    struct ApiState {
        schedule: serde_json::Value,
        backups: BTreeMap<String, serde_json::Value>,
        posted_names: Vec<String>,
        first_b_create_failed: bool,
    }

    let slot_a = scheduled_backup_name("nightly", &slot_name(utc(2026, 9, 10, 12, 0))).unwrap();
    let slot_b = scheduled_backup_name("nightly", &slot_name(utc(2026, 9, 10, 12, 1))).unwrap();
    let initial = forbid_schedule("nightly", UID, "* * * * *", false);
    let state = Arc::new(Mutex::new(ApiState {
        schedule: serde_json::to_value(&initial).unwrap(),
        backups: BTreeMap::new(),
        posted_names: Vec::new(),
        first_b_create_failed: false,
    }));
    let a_created = Arc::new(AtomicBool::new(false));
    let a_created_notify = Arc::new(Notify::new());
    let b_reserved = Arc::new(AtomicBool::new(false));
    let b_reserved_notify = Arc::new(Notify::new());

    let service = {
        let state = Arc::clone(&state);
        let a_created = Arc::clone(&a_created);
        let a_created_notify = Arc::clone(&a_created_notify);
        let b_reserved = Arc::clone(&b_reserved);
        let b_reserved_notify = Arc::clone(&b_reserved_notify);
        let slot_a = slot_a.clone();
        let slot_b = slot_b.clone();
        service_fn(move |request: Request<Body>| {
            let state = Arc::clone(&state);
            let a_created = Arc::clone(&a_created);
            let a_created_notify = Arc::clone(&a_created_notify);
            let b_reserved = Arc::clone(&b_reserved);
            let b_reserved_notify = Arc::clone(&b_reserved_notify);
            let slot_a = slot_a.clone();
            let slot_b = slot_b.clone();
            async move {
                let method = request.method().as_str().to_string();
                let path = request.uri().path().to_string();
                let body = request
                    .into_body()
                    .collect()
                    .await
                    .map(|collected| collected.to_bytes())
                    .unwrap_or_default();

                if method == "PATCH" && path.ends_with("/backupschedules/nightly/status") {
                    let patch: serde_json::Value = serde_json::from_slice(&body).unwrap();
                    // A's OLD FINALIZER IS HELD, AND ONLY IT. W0 made the
                    // reservation a `PATCH` too, so `resourceVersion == 18`
                    // alone now also matches B's reservation — which is the one
                    // write this gate is waiting FOR, and gating it would
                    // deadlock. `is_reservation` separates them.
                    if patch["metadata"]["resourceVersion"] == serde_json::json!("18")
                        && !is_reservation(&patch["status"])
                    {
                        while !b_reserved.load(Ordering::SeqCst) {
                            b_reserved_notify.notified().await;
                        }
                    }
                }

                let (status, response_body) = {
                    let mut state = state.lock().unwrap();
                    if method == "GET" && path.ends_with("/backups") {
                        (
                            200,
                            backup_list_body(state.backups.values().cloned().collect()),
                        )
                    } else if method == "GET" && path.contains("/backups/") {
                        // D1 §4.5 STEP 6 ASKS FOR A SLOT'S ATTEMPT BY NAME.
                        // Answering it out of the same map the POSTs write
                        // keeps this fake API server internally consistent —
                        // the chain and the listing cannot disagree about what
                        // exists.
                        let name = path.rsplit('/').next().unwrap_or_default().to_string();
                        match state.backups.get(&name) {
                            Some(existing) => (200, existing.to_string()),
                            None => (404, not_found_body(&name)),
                        }
                    } else if method == "POST" && path.ends_with("/backups") {
                        let posted: serde_json::Value = serde_json::from_slice(&body).unwrap();
                        let posted_name = posted["metadata"]["name"].as_str().unwrap().to_string();
                        state.posted_names.push(posted_name.clone());
                        if posted_name == slot_b && !state.first_b_create_failed {
                            state.first_b_create_failed = true;
                            (500, SERVER_ERROR_BODY.to_string())
                        } else {
                            let phase = if posted_name == slot_a {
                                "Succeeded"
                            } else {
                                "Running"
                            };
                            state.backups.insert(
                                posted_name.clone(),
                                backup_value(&posted_name, UID, Some(phase), Some(&posted_name)),
                            );
                            if posted_name == slot_a {
                                a_created.store(true, Ordering::SeqCst);
                                a_created_notify.notify_waiters();
                            }
                            (201, posted.to_string())
                        }
                    } else if method == "PATCH" && path.ends_with("/backupschedules/nightly/status")
                    {
                        let patch: serde_json::Value = serde_json::from_slice(&body).unwrap();
                        let expected = state.schedule["metadata"]["resourceVersion"]
                            .as_str()
                            .unwrap()
                            .parse::<u64>()
                            .unwrap();
                        // A MERGE PATCH WITH NO `resourceVersion` INHERITS THE
                        // STORED ONE and can never conflict — the API server's
                        // own rule, modelled rather than `unwrap`ed, so that a
                        // reservation sent without a precondition shows up as a
                        // wrong OUTCOME here instead of as a hang.
                        let offered = patch["metadata"]["resourceVersion"]
                            .as_str()
                            .and_then(|v| v.parse::<u64>().ok());
                        if offered.is_some_and(|offered| offered != expected) {
                            (
                                409,
                                r#"{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Conflict","message":"the object has been modified","code":409}"#.to_string(),
                            )
                        } else {
                            weirkeeper::conditions::apply_merge_patch(&mut state.schedule, &patch);
                            state.schedule["metadata"]["resourceVersion"] =
                                serde_json::json!((expected + 1).to_string());
                            // B's RESERVATION IS THE EVENT A's FINALIZER WAITS
                            // FOR — one merge patch away from the write that
                            // used to be a replace.
                            let pending = state.schedule["status"]["pendingBackupRef"]["name"]
                                .as_str()
                                .unwrap_or_default()
                                .to_string();
                            if pending == slot_b {
                                b_reserved.store(true, Ordering::SeqCst);
                                b_reserved_notify.notify_waiters();
                            }
                            (200, state.schedule.to_string())
                        }
                    } else {
                        panic!("unexpected stale-finalizer request: {method} {path}")
                    }
                };
                Ok::<_, std::convert::Infallible>(
                    Response::builder()
                        .status(status)
                        .body(Body::from(response_body.into_bytes()))
                        .unwrap(),
                )
            }
        })
    };
    let client = kube::Client::new(service, "default");

    let a_client = client.clone();
    let a_schedule = initial.clone();
    let old_finalizer = tokio::spawn(async move {
        reconcile_schedule(&a_schedule, &a_client, utc(2026, 9, 10, 12, 0)).await
    });
    while !a_created.load(Ordering::SeqCst) {
        a_created_notify.notified().await;
    }

    let schedule_after_a: BackupSchedule =
        serde_json::from_value(state.lock().unwrap().schedule.clone()).unwrap();
    assert_eq!(
        schedule_after_a
            .status
            .as_ref()
            .and_then(|status| status.pending_backup_ref.as_ref())
            .map(|reference| reference.name.as_str()),
        Some(slot_a.as_str())
    );
    let b = reconcile_schedule(&schedule_after_a, &client, utc(2026, 9, 10, 12, 1)).await;
    assert!(b.is_err(), "slot B's first create simulates a worker crash");
    let a = old_finalizer.await.unwrap();
    assert!(a.is_err(), "A's stale final status CAS must conflict");

    let after_race: BackupSchedule =
        serde_json::from_value(state.lock().unwrap().schedule.clone()).unwrap();
    assert_eq!(
        after_race
            .status
            .as_ref()
            .and_then(|status| status.pending_backup_ref.as_ref())
            .map(|reference| reference.name.as_str()),
        Some(slot_b.as_str()),
        "A's old finalizer cannot clear B's newer accepted reservation"
    );
    assert!(
        after_race
            .status
            .as_ref()
            .and_then(|status| status.conditions.as_ref())
            .and_then(|conditions| conditions.first())
            .and_then(|condition| condition.message.as_deref())
            .is_some_and(|message| message.contains(&slot_b)),
        "A's old finalizer cannot overwrite B's newer scheduling condition"
    );

    let recovered = reconcile_schedule(&after_race, &client, utc(2026, 9, 10, 12, 2))
        .await
        .expect("restart resumes B from the surviving reservation");
    assert_eq!(recovered.created.as_deref(), Some(slot_b.as_str()));
    let state = state.lock().unwrap();
    assert_eq!(
        state
            .posted_names
            .iter()
            .filter(|name| name.as_str() == slot_b)
            .count(),
        2,
        "B is attempted once before the crash and exactly once on recovery"
    );
    assert!(state.schedule["status"]["pendingBackupRef"].is_null());
    assert_eq!(
        state.schedule["status"]["activeBackupRef"]["name"],
        serde_json::json!(slot_b)
    );
}

// ===========================================================================
// D1 §12 — PLAT-05.1: an editable policy with immutable run snapshots
// ===========================================================================

/// Whether a recorded request targets the `backups` resource.
///
/// A PATH SEGMENT AND NOT A SUBSTRING. `/namespaces/x/backupschedules/nightly/status`
/// contains the text `/backups`, so a substring test would report every status
/// patch this reconciler makes as a write against a run — a green assertion
/// about the wrong thing, or a red one about nothing.
fn targets_backups(uri: &str) -> bool {
    uri.split('?')
        .next()
        .unwrap_or(uri)
        .split('/')
        .any(|segment| segment == "backups")
}

/// A schedule fixture with an arbitrary spec body, so an edit is a fixture
/// difference rather than a mutation helper.
fn schedule_with(uid: &str, generation: i64, resource_version: &str, spec: &str) -> BackupSchedule {
    serde_json::from_str(&format!(
        r#"{{
  "apiVersion": "logweir.dev/v1alpha1",
  "kind": "BackupSchedule",
  "metadata": {{
    "name": "nightly",
    "namespace": "{NS}",
    "uid": "{uid}",
    "resourceVersion": "{resource_version}",
    "generation": {generation}
  }},
  "spec": {spec}
}}"#
    ))
    .expect("the fixture is a BackupSchedule")
}

/// The spec every PLAT-05.1 fixture starts from: `Forbid`, daily, two topics.
const EDITABLE_SPEC: &str = r#"{
    "schedule": "0 0 * * *",
    "sourceRef": { "name": "prod" },
    "topics": ["orders", "payments"],
    "archive": { "url": "s3://kafka-backups/logweir" },
    "concurrencyPolicy": "Forbid",
    "suspend": false
  }"#;

/// A `Forbid` route table for a slot with no prior run: the owned-Backup list
/// is empty, the reservation and the finalization are answered, and the create
/// is answered with `post_status`.
fn admitting_routes(name: &'static str, post_status: u16, schedule_body: String) -> Vec<Route> {
    vec![
        Route {
            method: "GET",
            path_suffix: "/namespaces/logweir-t18/backups",
            status: 200,
            body: backup_list_body(vec![]),
        },
        Route {
            method: "POST",
            path_suffix: "/namespaces/logweir-t18/backups",
            status: post_status,
            body: if post_status == 201 {
                created_backup_body(name)
            } else {
                already_exists_body(name)
            },
        },
        absent_backup(name),
        Route {
            method: "PATCH",
            path_suffix: "/backupschedules/nightly/status",
            status: 200,
            body: reservation_echo(
                &schedule_body,
                name,
                name.get(name.len().saturating_sub(15)..)
                    .unwrap_or_default(),
                0,
            ),
        },
    ]
}

/// The `spec` of the one `Backup` this reconcile POSTed.
fn posted_spec(bodies: &[SeenBody]) -> serde_json::Value {
    let posts = posts(bodies);
    assert_eq!(posts.len(), 1, "exactly one Backup is created: {posts:?}");
    let v: serde_json::Value =
        serde_json::from_str(&posts[0].body).expect("a recorded POST body is JSON");
    v["spec"].clone()
}

/// D1 §5.3 and §5.4's invariant: **a Backup's copied policy equals the schedule
/// spec at the generation recorded in it.**
///
/// The run records `uid`, `generation` and `runPolicySha256`, and the digest it
/// records is the digest of the fields it actually carries — which is exactly
/// what `identity::check_run_policy_digest` recomputes before the Backup
/// controller freezes anything. A scheduler that copied the policy of one
/// generation and stamped the number of another would produce a run the Backup
/// controller refuses terminally with `RunPolicyDigestMismatch`, so this is the
/// assertion that keeps that refusal unreachable in normal operation.
#[tokio::test]
async fn a_created_run_records_the_revision_whose_policy_it_copied() {
    let fire = utc(2026, 9, 10, 0, 0);
    let slot = slot_name(fire);
    let name: &'static str = Box::leak(
        scheduled_backup_name("nightly", &slot)
            .expect("fits")
            .into_boxed_str(),
    );

    let schedule = schedule_with(UID, 7, "17", EDITABLE_SPEC);
    let (client, _calls, bodies) = mock_client_recording_bodies(admitting_routes(
        name,
        201,
        serde_json::to_string(&schedule).expect("the fixture serialises"),
    ));
    reconcile_schedule(&schedule, &client, fire)
        .await
        .expect("the slot fires");

    let bodies = bodies.lock().expect("readable").clone();
    let spec = posted_spec(&bodies);
    assert_eq!(spec["scheduleRef"]["name"], serde_json::json!("nightly"));
    assert_eq!(spec["scheduleRef"]["uid"], serde_json::json!(UID));
    assert_eq!(
        spec["scheduleRef"]["generation"],
        serde_json::json!(7),
        "the run records the generation it was admitted under: {spec}"
    );

    // The digest is the digest of the POSTed object's own fields, recomputed
    // the way the Backup controller recomputes it.
    let posted: weirkeeper::crds::backup::Backup =
        serde_json::from_str(&posts(&bodies)[0].body).expect("the POST body is a Backup");
    assert_eq!(
        spec["scheduleRef"]["runPolicySha256"].as_str(),
        Some(weirkeeper::policy::run_policy_sha256(&posted.spec).as_str()),
        "the recorded digest must equal the digest of the fields the run carries; a mismatch \
         is what `identity::check_run_policy_digest` refuses terminally: {spec}"
    );
    assert_eq!(
        spec["scheduleRef"]["runPolicySha256"].as_str(),
        Some(weirkeeper::controllers::backup_schedule::run_policy_digest(&schedule.spec).as_str()),
        "and it must equal the schedule's own digest for that generation: {spec}"
    );
    weirkeeper::identity::check_run_policy_digest(&posted)
        .expect("the created run passes the digest check the Backup controller performs");

    // The reservation carried the same generation, BEFORE the create: that is
    // the §5.4 boundary, and it is what a 409 protects.
    let reserved = reserved_status(&bodies);
    assert_eq!(
        reserved["pendingRun"]["generation"],
        serde_json::json!(7),
        "the reservation records the generation the child will be created under: {reserved}"
    );
    assert_eq!(reserved["pendingRun"]["name"], serde_json::json!(name));
    assert_eq!(reserved["pendingRun"]["slot"], serde_json::json!(slot));
    assert_eq!(reserved["pendingRun"]["attempt"], serde_json::json!(0));
    assert_eq!(
        reserved["pendingRun"]["kind"],
        serde_json::json!("Scheduled")
    );

    // And the settled status records the revision an operator reads.
    let status = patched_status(&bodies);
    assert_eq!(status["observedGeneration"], serde_json::json!(7));
    assert_eq!(status["policy"]["generation"], serde_json::json!(7));
    assert_eq!(
        status["policy"]["runPolicySha256"],
        spec["scheduleRef"]["runPolicySha256"]
    );
    assert_eq!(status["policy"]["timeZone"], serde_json::json!("UTC"));
    assert_eq!(
        status["policy"]["tzdb"],
        serde_json::json!(weirkeeper::cadence::TZDB_SOURCE)
    );
}

/// The run policy digest covers WHAT a run does and not WHEN it runs.
///
/// This is the whole value of the field: an operator who suspends and resumes a
/// schedule sees `metadata.generation` move twice and `runPolicySha256` stand
/// still, so "did anything about my backups change?" has an answer that is not
/// "diff two YAML documents".
#[test]
fn run_policy_digest_ignores_cadence_suspension_and_topic_order() {
    let base = schedule_with(UID, 1, "1", EDITABLE_SPEC);
    let digest = weirkeeper::controllers::backup_schedule::run_policy_digest(&base.spec);

    // WHEN — every one of these leaves the digest alone.
    for (what, spec) in [
        (
            "the cron expression",
            EDITABLE_SPEC.replace("0 0 * * *", "30 2 * * 1"),
        ),
        (
            "suspension",
            EDITABLE_SPEC.replace("\"suspend\": false", "\"suspend\": true"),
        ),
        (
            "the concurrency policy",
            EDITABLE_SPEC.replace("\"Forbid\"", "\"Allow\""),
        ),
        (
            "a time zone",
            EDITABLE_SPEC.replace(
                "\"suspend\": false",
                "\"suspend\": false, \"timeZone\": \"Europe/Berlin\"",
            ),
        ),
        (
            "a retry policy",
            EDITABLE_SPEC.replace(
                "\"suspend\": false",
                "\"suspend\": false, \"retry\": { \"maxRetries\": 2 }",
            ),
        ),
        (
            "a catch-up policy",
            EDITABLE_SPEC.replace(
                "\"suspend\": false",
                "\"suspend\": false, \"catchUpPolicy\": \"Latest\"",
            ),
        ),
        (
            "the starting deadline",
            EDITABLE_SPEC.replace(
                "\"suspend\": false",
                "\"suspend\": false, \"startingDeadlineSeconds\": 600",
            ),
        ),
        (
            "the retention report policy",
            EDITABLE_SPEC.replace(
                "\"suspend\": false",
                "\"suspend\": false, \"retention\": { \"keepLast\": 3 }",
            ),
        ),
        (
            "the ORDER of the topic list",
            EDITABLE_SPEC.replace("[\"orders\", \"payments\"]", "[\"payments\", \"orders\"]"),
        ),
    ] {
        let edited = schedule_with(UID, 2, "2", &spec);
        assert_eq!(
            weirkeeper::controllers::backup_schedule::run_policy_digest(&edited.spec),
            digest,
            "{what} decides WHEN a run happens, not WHAT it does, so it must not move \
             runPolicySha256"
        );
    }

    // WHAT — every one of these moves it.
    for (what, spec) in [
        (
            "the topic SET",
            EDITABLE_SPEC.replace("\"payments\"", "\"shipments\""),
        ),
        (
            "the archive URL",
            EDITABLE_SPEC.replace("kafka-backups/logweir", "kafka-backups/elsewhere"),
        ),
        (
            "the archive credential",
            EDITABLE_SPEC.replace(
                "\"url\": \"s3://kafka-backups/logweir\"",
                "\"url\": \"s3://kafka-backups/logweir\", \"secretRef\": { \"name\": \"s3\" }",
            ),
        ),
        (
            "the run deadline",
            EDITABLE_SPEC.replace(
                "\"suspend\": false",
                "\"suspend\": false, \"activeDeadlineSeconds\": 7200",
            ),
        ),
    ] {
        let edited = schedule_with(UID, 2, "2", &spec);
        assert_ne!(
            weirkeeper::controllers::backup_schedule::run_policy_digest(&edited.spec),
            digest,
            "{what} changes what a run does and must move runPolicySha256"
        );
    }
}

/// D1 §5.4: an edit that lands between the read and the reservation gets a
/// **409**, and the reconcile creates nothing.
///
/// The reservation is a merge PATCH carrying `metadata.resourceVersion`, which
/// Kubernetes applies as an update precondition. The 409 is the whole
/// mechanism: the reconcile aborts, requeues, re-reads the newer object and
/// decides again under the new generation — so a run is never created from a
/// policy the reservation did not agree with.
#[tokio::test]
async fn an_edit_between_the_read_and_the_reservation_gets_409_and_creates_nothing() {
    let fire = utc(2026, 9, 10, 0, 0);
    let slot = slot_name(fire);
    let name: &'static str = Box::leak(
        scheduled_backup_name("nightly", &slot)
            .expect("fits")
            .into_boxed_str(),
    );
    let schedule = schedule_with(UID, 7, "17", EDITABLE_SPEC);

    let mut routes = admitting_routes(
        name,
        201,
        serde_json::to_string(&schedule).expect("the fixture serialises"),
    );
    routes.retain(|r| r.method != "PATCH");
    routes.push(Route {
        method: "PATCH",
        path_suffix: "/backupschedules/nightly/status",
        status: 409,
        body: r#"{"kind":"Status","apiVersion":"v1","status":"Failure",
  "message":"Operation cannot be fulfilled on backupschedules.logweir.dev \"nightly\": the object has been modified",
  "reason":"Conflict","code":409}"#
            .to_string(),
    });
    let (client, _calls, bodies) = mock_client_recording_bodies(routes);

    let err = reconcile_schedule(&schedule, &client, fire)
        .await
        .expect_err("a rejected reservation aborts the reconcile");
    assert!(
        format!("{err}").contains("409") || format!("{err}").to_lowercase().contains("conflict"),
        "the error names the conflict so the requeue is legible: {err}"
    );

    let bodies = bodies.lock().expect("readable").clone();
    assert!(
        posts(&bodies).is_empty(),
        "NOTHING is created when the reservation was refused — that is the atomic boundary, \
         and a create after a 409 would run a policy the schedule no longer has: {bodies:?}"
    );
    let reserved = reserved_status(&bodies);
    assert_eq!(
        reserved["pendingRun"]["generation"],
        serde_json::json!(7),
        "the reservation that was refused carried the generation it was decided under"
    );
}

/// D1 §5.4, the other half: a restart after an accepted reservation creates the
/// child **with the generation it reads now**, and records that generation.
///
/// The decision tree says so in one line — "crash before POST → restart reads
/// G_m ≥ G_n → step 2: create with G_m, recording G_m" — and the reason is that
/// there is nothing else to be truthful about. The reservation named a slot and
/// an object name, not a policy; the object the controller can see is the
/// current one; and recording the current generation keeps the invariant that a
/// run's copied policy equals the spec at the generation written inside it.
#[tokio::test]
async fn a_restart_after_a_reservation_creates_under_the_generation_it_reads() {
    let fire = utc(2026, 9, 10, 0, 0);
    let slot = slot_name(fire);
    let name: &'static str = Box::leak(
        scheduled_backup_name("nightly", &slot)
            .expect("fits")
            .into_boxed_str(),
    );

    // The reservation was made at generation 7; the operator then edited the
    // topic list, so the object the restarted controller reads is generation 8.
    let edited_spec = EDITABLE_SPEC.replace("\"payments\"", "\"shipments\"");
    let mut schedule = schedule_with(UID, 8, "23", &edited_spec);
    schedule.status = Some(BackupScheduleStatus {
        pending_backup_ref: Some(LocalRef {
            name: name.to_string(),
        }),
        ..BackupScheduleStatus::default()
    });

    let (client, _calls, bodies) = mock_client_recording_bodies(admitting_routes(
        name,
        201,
        serde_json::to_string(&schedule).expect("serialises"),
    ));
    // An hour past the slot, so the cron decision is no longer `Due`: the
    // reservation is resumed on its own account.
    reconcile_schedule(&schedule, &client, fire + chrono::Duration::hours(2))
        .await
        .expect("the accepted reservation is resumed");

    let bodies = bodies.lock().expect("readable").clone();
    assert!(
        reserved_status_opt(&bodies).is_none(),
        "a resumed reservation is not re-reserved: {bodies:?}"
    );
    let spec = posted_spec(&bodies);
    assert_eq!(
        spec["scheduleRef"]["generation"],
        serde_json::json!(8),
        "the resumed child records the generation the controller could actually read: {spec}"
    );
    assert_eq!(
        spec["topics"],
        serde_json::json!(["orders", "shipments"]),
        "and it copies THAT generation's policy, so the digest it records is true: {spec}"
    );
    let posted: weirkeeper::crds::backup::Backup =
        serde_json::from_str(&posts(&bodies)[0].body).expect("the POST body is a Backup");
    weirkeeper::identity::check_run_policy_digest(&posted)
        .expect("the resumed run's recorded digest matches the policy it copied");

    let status = patched_status(&bodies);
    // BOTH SPELLINGS, EXPLICITLY. `pendingRun` is the typed reservation and
    // `pendingBackupRef` is the mirror older readers use; clearing one and
    // leaving the other presents an outstanding reservation to whichever reader
    // looked at the wrong field.
    assert_cleared(&status, "pendingBackupRef");
    assert_cleared(&status, "pendingRun");
}

/// D1 §12: editing a schedule never touches a running `Backup`.
///
/// The reconciler's whole write surface against `backups` is a `POST`; there is
/// no PUT, no PATCH and no DELETE. So an edit cannot reach a run's spec, its
/// frozen inputs or its Job — which is what makes "immutable run snapshots"
/// true by construction rather than by care.
#[tokio::test]
async fn editing_a_schedule_never_writes_to_a_running_backup() {
    let fire = utc(2026, 9, 10, 0, 0);
    let slot = slot_name(fire);
    let running: &'static str = Box::leak(
        scheduled_backup_name("nightly", &slot)
            .expect("fits")
            .into_boxed_str(),
    );

    // Generation 9: the topic list was edited while the midnight run is still
    // Running. The next slot is not due yet.
    let edited_spec = EDITABLE_SPEC.replace("\"payments\"", "\"shipments\"");
    let schedule = schedule_with(UID, 9, "31", &edited_spec);
    let (client, calls, bodies) = mock_client_recording_bodies(vec![
        Route {
            method: "GET",
            path_suffix: "/namespaces/logweir-t18/backups",
            status: 200,
            body: backup_list_body(vec![backup_value(
                running,
                UID,
                Some("Running"),
                Some(running),
            )]),
        },
        absent_backup(&due_name(fire)),
        Route {
            method: "PATCH",
            path_suffix: "/backupschedules/nightly/status",
            status: 200,
            body: serde_json::to_string(&schedule).expect("serialises"),
        },
    ]);
    reconcile_schedule(&schedule, &client, fire + chrono::Duration::minutes(30))
        .await
        .expect("the reconcile completes");

    for seen in calls.lock().expect("readable").iter() {
        assert!(
            !(targets_backups(&seen.uri)
                && matches!(seen.method.as_str(), "PATCH" | "PUT" | "DELETE")),
            "the schedule reconciler never writes to a Backup: an edit must not be able to \
             reach a run that already exists. Saw {} {}",
            seen.method,
            seen.uri
        );
    }
    assert!(
        posts(&bodies.lock().expect("readable")).is_empty(),
        "and no new run is created while the previous one is active under Forbid"
    );
}

/// D1 §5.5: an edit the schema accepts but the controller cannot run stops
/// admissions, says why, and leaves running work alone.
///
/// The converse of R2 — "`topics: []` requires `allUserTopics`" — is not
/// expressible in CEL on the 1.29 floor without stranding every object already
/// stored with an empty list, and cron and time-zone validity are not
/// expressible at all. So the controller is the gate, and this is the table of
/// what it refuses.
#[tokio::test]
async fn semantic_invalid_edits_fail_closed_with_reasons() {
    let fire = utc(2026, 9, 10, 0, 0);

    let cases: [(&str, String, &str); 4] = [
        (
            "an empty topic list with no dynamic block",
            EDITABLE_SPEC.replace("[\"orders\", \"payments\"]", "[]"),
            "InvalidTopicSelection",
        ),
        (
            "a glob metacharacter in the allowlist",
            EDITABLE_SPEC.replace("\"payments\"", "\"orders-*\""),
            "InvalidTopicSelection",
        ),
        (
            "a dynamic block beside a named allowlist",
            EDITABLE_SPEC.replace(
                "\"suspend\": false",
                "\"suspend\": false, \"allUserTopics\": { \"incompleteDiscovery\": \"Refuse\" }",
            ),
            "InvalidTopicSelection",
        ),
        (
            "an unparseable cron expression",
            EDITABLE_SPEC.replace("0 0 * * *", "61 * * * *"),
            "UnparseableSchedule",
        ),
    ];

    for (what, spec, reason) in cases {
        let schedule = schedule_with(UID, 11, "41", &spec);
        let (client, _calls, bodies) = mock_client_recording_bodies(vec![
            Route {
                method: "GET",
                path_suffix: "/namespaces/logweir-t18/backups",
                status: 200,
                body: backup_list_body(vec![]),
            },
            Route {
                method: "PATCH",
                path_suffix: "/backupschedules/nightly/status",
                status: 200,
                body: serde_json::to_string(&schedule).expect("serialises"),
            },
        ]);
        let outcome = reconcile_schedule(&schedule, &client, fire)
            .await
            .expect("an invalid policy is a decision, not a reconcile failure");
        assert_eq!(
            outcome.decision.reason(),
            reason,
            "{what}: the reason names what the operator has to fix. Got {:?}",
            outcome.decision
        );
        let bodies = bodies.lock().expect("readable").clone();
        assert!(
            posts(&bodies).is_empty(),
            "{what}: nothing is admitted while the policy is unusable: {bodies:?}"
        );
        let status = patched_status(&bodies);
        assert_eq!(
            status["conditions"][0]["status"],
            serde_json::json!("False"),
            "{what}: Ready=False is what PLAT-14.2's staleness alert reads: {status}"
        );
        assert_eq!(status["conditions"][0]["reason"], serde_json::json!(reason));
        assert_eq!(
            status["nextFireTime"],
            serde_json::Value::Null,
            "{what}: a schedule that will not fire has no next firing to advertise: {status}"
        );
        assert_eq!(
            status["observedGeneration"],
            serde_json::json!(11),
            "{what}: the controller still records which revision it judged: {status}"
        );
    }
}

/// D1 §4.5 step 2 / §4.7 row 2: an invalid policy RELEASES a pending
/// reservation instead of resuming it.
///
/// Resuming it would POST a `Backup` whose copied policy the Backup controller
/// refuses terminally (`InvalidTopicSelection`), which costs an object, a
/// condition and an operator's attention to reach the same conclusion the
/// scheduler already had. Releasing it means that fixing the spec resumes the
/// schedule within one reconcile, and the slot is accounted rather than run.
#[tokio::test]
async fn an_invalid_policy_releases_a_pending_reservation_and_leaves_running_work_alone() {
    let fire = utc(2026, 9, 10, 0, 0);
    let slot = slot_name(fire);
    let reserved: &'static str = Box::leak(
        scheduled_backup_name("nightly", &slot)
            .expect("fits")
            .into_boxed_str(),
    );
    let earlier: &'static str = Box::leak(
        scheduled_backup_name("nightly", &slot_name(utc(2026, 9, 9, 0, 0)))
            .expect("fits")
            .into_boxed_str(),
    );

    let spec = EDITABLE_SPEC.replace("[\"orders\", \"payments\"]", "[]");
    let mut schedule = schedule_with(UID, 12, "43", &spec);
    schedule.status = Some(BackupScheduleStatus {
        pending_backup_ref: Some(LocalRef {
            name: reserved.to_string(),
        }),
        ..BackupScheduleStatus::default()
    });

    let (client, calls, bodies) = mock_client_recording_bodies(vec![
        Route {
            method: "GET",
            path_suffix: "/namespaces/logweir-t18/backups",
            status: 200,
            body: backup_list_body(vec![backup_value(
                earlier,
                UID,
                Some("Running"),
                Some(earlier),
            )]),
        },
        Route {
            method: "PATCH",
            path_suffix: "/backupschedules/nightly/status",
            status: 200,
            body: serde_json::to_string(&schedule).expect("serialises"),
        },
    ]);
    reconcile_schedule(&schedule, &client, fire + chrono::Duration::minutes(5))
        .await
        .expect("an invalid policy is a decision");

    let bodies = bodies.lock().expect("readable").clone();
    assert!(
        posts(&bodies).is_empty(),
        "the reservation is NOT resumed into a run the Backup controller would refuse: \
         {bodies:?}"
    );
    let status = patched_status(&bodies);
    assert_cleared(&status, "pendingBackupRef");
    assert_cleared(&status, "pendingRun");
    assert_eq!(
        status["conditions"][0]["reason"],
        serde_json::json!("InvalidTopicSelection")
    );
    // The run that was already going is untouched — no write of any kind
    // against `backups`.
    for seen in calls.lock().expect("readable").iter() {
        assert!(
            !(targets_backups(&seen.uri)
                && matches!(seen.method.as_str(), "PATCH" | "PUT" | "DELETE" | "POST")),
            "a released reservation writes nothing against backups; saw {} {}",
            seen.method,
            seen.uri
        );
    }
}

/// D1 §5.7: a schedule stored before PLAT-05.1 reconciles with no rewrite, and
/// its absent fields mean what they meant.
///
/// The object here is byte-for-byte what an older controller stored: no
/// `timeZone`, no deadlines, no catch-up, no retry, no dynamic selection, and a
/// status that has never held `observedGeneration` or `policy`. Nothing about
/// it is converted; the controller reads the defaults, fires the slot it would
/// always have fired, under the name it would always have used, and ADDS the
/// revision block.
#[tokio::test]
async fn a_pre_upgrade_schedule_reconciles_without_a_rewrite() {
    let fire = utc(2026, 9, 10, 0, 0);
    let slot = slot_name(fire);
    let name: &'static str = Box::leak(
        scheduled_backup_name("nightly", &slot)
            .expect("fits")
            .into_boxed_str(),
    );

    // The exact spec an older controller would have stored: five fields.
    let legacy_spec = r#"{
    "schedule": "0 0 * * *",
    "sourceRef": { "name": "prod" },
    "topics": ["orders"],
    "archive": { "url": "s3://kafka-backups/logweir" },
    "suspend": false
  }"#;
    let schedule = schedule_with(UID, 1, "5", legacy_spec);
    assert!(schedule.spec.time_zone.is_none());
    assert!(schedule.spec.starting_deadline_seconds.is_none());
    assert!(schedule.spec.catch_up_policy.is_none());
    assert!(schedule.spec.retry.is_none());
    assert!(schedule.spec.active_deadline_seconds.is_none());
    assert!(schedule.spec.all_user_topics.is_none());

    let (client, _calls, bodies) = mock_client_recording_bodies(admitting_routes(
        name,
        201,
        serde_json::to_string(&schedule).expect("serialises"),
    ));
    reconcile_schedule(&schedule, &client, fire)
        .await
        .expect("a legacy schedule still fires");

    let bodies = bodies.lock().expect("readable").clone();
    let spec = posted_spec(&bodies);
    assert_eq!(
        body_name(posts(&bodies)[0]),
        name,
        "the slot name is the one the old controller would have computed"
    );
    assert_eq!(
        spec["deadlineSeconds"],
        serde_json::json!(3600),
        "an absent activeDeadlineSeconds is the constant the old controller used: {spec}"
    );
    assert!(
        spec["trigger"]["timeZone"].is_null(),
        "no zone was configured, so none is recorded: {spec}"
    );
    assert_eq!(spec["trigger"]["kind"], serde_json::json!("Scheduled"));
    assert_eq!(spec["trigger"]["attempt"], serde_json::json!(0));

    let status = patched_status(&bodies);
    assert_eq!(
        status["policy"]["timeZone"],
        serde_json::json!("UTC"),
        "the EFFECTIVE zone is reported, so a reader never has to know the default: {status}"
    );
    assert_eq!(status["observedGeneration"], serde_json::json!(1));
    assert_eq!(
        status["policy"]["effectiveSince"],
        serde_json::json!(fire),
        "the first observation of a revision is when this controller first saw it: {status}"
    );
}

/// D1 §5.7 rollback shape: a status written by an older controller — a
/// `pendingBackupRef` with no `pendingRun` beside it — is resumed, not
/// discarded.
///
/// This is the state a roll-forward finds after an older controller made a
/// reservation and stopped. The typed block is the newer spelling; the mirror
/// is the one the old controller wrote, and it is still the authority for
/// resuming, so no accepted slot is lost across a version boundary in either
/// direction.
#[tokio::test]
async fn an_old_style_reservation_without_pending_run_is_resumed() {
    let fire = utc(2026, 9, 10, 0, 0);
    let slot = slot_name(fire);
    let name: &'static str = Box::leak(
        scheduled_backup_name("nightly", &slot)
            .expect("fits")
            .into_boxed_str(),
    );

    let mut schedule = schedule_with(UID, 3, "9", EDITABLE_SPEC);
    schedule.status = Some(BackupScheduleStatus {
        pending_backup_ref: Some(LocalRef {
            name: name.to_string(),
        }),
        ..BackupScheduleStatus::default()
    });
    assert!(
        schedule
            .status
            .as_ref()
            .expect("a status")
            .pending_run
            .is_none(),
        "the fixture is the OLD spelling: a mirror with no typed block beside it"
    );

    let (client, _calls, bodies) = mock_client_recording_bodies(admitting_routes(
        name,
        201,
        serde_json::to_string(&schedule).expect("serialises"),
    ));
    reconcile_schedule(&schedule, &client, fire + chrono::Duration::hours(3))
        .await
        .expect("the old-style reservation is resumed");

    let bodies = bodies.lock().expect("readable").clone();
    assert_eq!(
        body_name(posts(&bodies)[0]),
        name,
        "the accepted slot is created, not abandoned: {bodies:?}"
    );
    let status = patched_status(&bodies);
    assert_cleared(&status, "pendingBackupRef");
    assert_cleared(&status, "pendingRun");
}

// ===========================================================================
// D1 §12 — PLAT-04.2: zones, deadlines, catch-up, retries and the truth table
// ===========================================================================

/// Assert that a status patch CLEARS `key` — an explicit JSON `null`, not an
/// absent key.
///
/// THE DIFFERENCE IS THE WHOLE OF MERGE-PATCH SEMANTICS, and `Value::Index`
/// hides it: `status["pendingRun"]` returns `Null` both for a key set to null
/// and for a key that is not there at all. An absent key in a merge patch means
/// "leave it alone", so a test written with `assert_eq!(status["x"], Null)`
/// passes for a controller that stopped clearing the field — which is exactly
/// how a released reservation comes to still look outstanding to whichever
/// reader looked at the field the patch forgot.
fn assert_cleared(status: &serde_json::Value, key: &str) {
    assert_eq!(
        status.get(key),
        Some(&serde_json::Value::Null),
        "`status.{key}` must be cleared with an EXPLICIT null: an absent key in a merge patch \
         means `leave it alone`, so omitting it leaves the stale value standing. Got: {status}"
    );
}

/// A `Backup` that reached a terminal phase, with the terminal condition the
/// retry delay is measured from.
///
/// THE CONDITION IS NOT DECORATION. D1 §4.6 measures `delaySeconds` from
/// `finishedAt(k)` — the `Failed` condition's `lastTransitionTime`, written
/// once by the terminal patch — so a fixture without one describes a run whose
/// retry could not be scheduled at any particular instant, and the controller
/// treats it as not retryable for exactly that reason.
fn terminal_backup(
    name: &str,
    phase: &str,
    exit_code: Option<i32>,
    exit_reason: Option<&str>,
    finished_at: DateTime<Utc>,
) -> serde_json::Value {
    let mut value = backup_value(name, UID, Some(phase), Some(name));
    let status = value["status"].as_object_mut().expect("a status object");
    if let Some(code) = exit_code {
        status.insert("exitCode".to_string(), serde_json::json!(code));
    }
    if let Some(reason) = exit_reason {
        status.insert("exitReason".to_string(), serde_json::json!(reason));
    }
    status.insert(
        "conditions".to_string(),
        serde_json::json!([{
            "type": if phase == "Succeeded" { "Complete" } else { "Failed" },
            "status": "True",
            "lastTransitionTime": finished_at,
            "reason": "Operational",
        }]),
    );
    value
}

/// A one-minute `Forbid` schedule with an arbitrary extra policy block.
fn cadence_schedule(extra: &str) -> BackupSchedule {
    cadence_schedule_cron("* * * * *", extra)
}

/// [`cadence_schedule`] on an arbitrary expression.
///
/// THE DEADLINE TESTS NEED SLOTS FURTHER APART THAN THE DEADLINE. With
/// one-minute slots and a 60-second starting deadline a slot is never past its
/// deadline while it is still the latest one — the next slot arrives at the
/// same instant the deadline expires — so "past the deadline" is not a state a
/// `* * * * *` schedule can be observed in. `*/5` with a 60-second deadline can.
fn cadence_schedule_cron(cron: &str, extra: &str) -> BackupSchedule {
    schedule_with(
        UID,
        3,
        "17",
        &format!(
            r#"{{
    "schedule": "{cron}",
    "sourceRef": {{ "name": "prod" }},
    "topics": ["orders"],
    "archive": {{ "url": "s3://kafka-backups/logweir" }},
    "concurrencyPolicy": "Forbid",
    "suspend": false{extra}
  }}"#
        ),
    )
}

/// D1 §4.3 through the controller: a zoned schedule fires at the UTC instant
/// its local expression names, and the run records the zone it was computed in.
///
/// `Europe/Berlin`, `30 2 * * *`, 2026-10-25 — the night the local hour 02:00
/// happens twice. The decision names the FIRST occurrence's UTC instant
/// (00:30Z), the object is named from that instant, and `spec.trigger.timeZone`
/// carries `Europe/Berlin` so a history row keeps its local time after somebody
/// edits the schedule's zone. The second occurrence (01:30Z) is a slot of its
/// own and appears in the previews.
#[tokio::test]
async fn a_time_zone_schedule_fires_at_the_utc_slot_and_records_the_zone() {
    let first = utc(2026, 10, 25, 0, 30);
    let second = utc(2026, 10, 25, 1, 30);
    let schedule = schedule_with(
        UID,
        3,
        "17",
        r#"{
    "schedule": "30 2 * * *",
    "timeZone": "Europe/Berlin",
    "sourceRef": { "name": "prod" },
    "topics": ["orders"],
    "archive": { "url": "s3://kafka-backups/logweir" },
    "concurrencyPolicy": "Forbid",
    "suspend": false
  }"#,
    );
    let name: &'static str = Box::leak(
        scheduled_backup_name("nightly", &slot_name(first))
            .expect("fits")
            .into_boxed_str(),
    );
    assert_eq!(
        name, "logweir-backup-nightly-20261025-003000",
        "the slot identity is the UTC instant, whatever the zone: names stay unique, monotonic \
         and DNS-1123"
    );

    let (client, _calls, bodies) = mock_client_recording_bodies(admitting_routes(
        name,
        201,
        serde_json::to_string(&schedule).expect("serialises"),
    ));
    reconcile_schedule(&schedule, &client, first + chrono::Duration::seconds(10))
        .await
        .expect("the zoned slot fires");

    let bodies = bodies.lock().expect("readable").clone();
    let spec = posted_spec(&bodies);
    assert_eq!(spec["slot"], serde_json::json!(slot_name(first)));
    assert_eq!(
        spec["trigger"]["timeZone"],
        serde_json::json!("Europe/Berlin"),
        "the zone the slot was computed in travels with the run, so a history row keeps its \
         local time after an edit: {spec}"
    );

    let status = patched_status(&bodies);
    assert_eq!(
        status["policy"]["timeZone"],
        serde_json::json!("Europe/Berlin")
    );
    // THE REPEATED HOUR FIRES TWICE, AND THE PREVIEWS SAY SO. Both occurrences
    // of local 02:30 are real instants whose local wall time matches, so each is
    // its own slot; the markers are what make that predictable instead of
    // surprising.
    let previews = status["nextRuns"].as_array().expect("nextRuns is a list");
    assert_eq!(
        previews[0]["at"],
        serde_json::json!(second),
        "the second occurrence of the repeated local hour is the next firing: {status}"
    );
    assert_eq!(
        previews[0]["adjustment"],
        serde_json::json!("RepeatedLocalTimeSecond")
    );
    assert!(previews[0]["localTime"]
        .as_str()
        .expect("a rendered local time")
        .ends_with("+01:00"));
}

/// D1 §4.3: a zone this build's database does not have is `Ready=False` and
/// admits nothing. **Never silently UTC.**
#[tokio::test]
async fn an_unknown_timezone_is_ready_false_and_creates_nothing() {
    let schedule = cadence_schedule(r#", "timeZone": "Mars/Olympus""#);
    let now = utc(2026, 9, 10, 12, 1);
    let (client, _calls, bodies) = mock_client_recording_bodies(vec![
        no_backups(),
        // PRESENT AND UNUSED.
        Route {
            method: "POST",
            path_suffix: "/namespaces/logweir-t18/backups",
            status: 201,
            body: created_backup_body(&due_name(now)),
        },
        Route {
            method: "PATCH",
            path_suffix: "/backupschedules/nightly/status",
            status: 200,
            body: serde_json::to_string(&schedule).expect("serialises"),
        },
    ]);
    let outcome = reconcile_schedule(&schedule, &client, now)
        .await
        .expect("an unknown zone is a decision, not an error");
    assert_eq!(outcome.decision.reason(), "UnknownTimeZone");
    let bodies = bodies.lock().expect("readable").clone();
    assert!(posts(&bodies).is_empty());
    let status = patched_status(&bodies);
    assert_eq!(
        status["conditions"][0]["status"],
        serde_json::json!("False")
    );
    assert_eq!(
        status["nextRuns"],
        serde_json::json!([]),
        "a schedule that cannot compute a slot advertises no firings: {status}"
    );
    assert_eq!(status["nextFireTime"], serde_json::Value::Null);
    assert!(status["conditions"][0]["message"]
        .as_str()
        .expect("a message")
        .contains("is never read as UTC"));
}

/// D1 §4.7 rows 18 and 22: a week of downtime with `catchUpPolicy: None` runs
/// nothing and counts what it skipped.
#[tokio::test]
async fn downtime_with_catch_up_none_records_missed_and_creates_nothing() {
    // The controller was last here at 12:00 and comes back at 12:30: thirty
    // one-minute slots came due while it was away, and the latest of them is
    // itself past a 60-second starting deadline.
    let away_since = utc(2026, 9, 10, 12, 0);
    let back = utc(2026, 9, 10, 12, 30);
    let mut schedule = cadence_schedule_cron("*/5 * * * *", r#", "startingDeadlineSeconds": 60"#);
    schedule.status = Some(BackupScheduleStatus {
        missed_slots: Some(weirkeeper::crds::backup_schedule::MissedSlots {
            count: 0,
            count_capped: false,
            last_evaluated_slot: Some(slot_name(away_since)),
            recent: None,
        }),
        active_runs: Some(Vec::new()),
        ..BackupScheduleStatus::default()
    });
    let latest = utc(2026, 9, 10, 12, 30);
    let (client, _calls, bodies) = mock_client_recording_bodies(vec![
        // PLAT-05.2: step 1's inventory (D1 §6.7) replaced the bootstrap
        // LIST, and it runs on the FIRST pass over a schedule whose
        // `status.history` is absent. This schedule has no runs, so the
        // one page it reads is empty.
        no_backups(),
        absent_backup(&due_name(latest)),
        Route {
            method: "POST",
            path_suffix: "/namespaces/logweir-t18/backups",
            status: 201,
            body: created_backup_body(&due_name(latest)),
        },
        Route {
            method: "PATCH",
            path_suffix: "/backupschedules/nightly/status",
            status: 200,
            body: serde_json::to_string(&schedule).expect("serialises"),
        },
    ]);
    let outcome = reconcile_schedule(&schedule, &client, back + chrono::Duration::seconds(90))
        .await
        .expect("a missed slot is a decision");
    assert_eq!(outcome.decision.reason(), REASON_SLOT_MISSED);
    let bodies = bodies.lock().expect("readable").clone();
    assert!(
        posts(&bodies).is_empty(),
        "NOTHING is fired for a backlog nobody is waiting for: {bodies:?}"
    );
    let status = patched_status(&bodies);
    assert_eq!(
        status["missedSlots"]["count"],
        serde_json::json!(6),
        "five slots strictly between the last evaluated one and this one, plus this one: \
         {status}"
    );
    assert_eq!(
        status["missedSlots"]["lastEvaluatedSlot"],
        serde_json::json!(slot_name(latest)),
        "and the marker advances, so the next reconcile does not count the same gap again"
    );
    assert_eq!(
        status["missedSlots"]["recent"][0]["reason"],
        serde_json::json!("PastStartingDeadline")
    );
    assert_eq!(
        status["lastSlot"]["disposition"],
        serde_json::json!("Missed")
    );
    // NO STEADY-STATE DOUBLE COUNTING. A second reconcile a minute later has a
    // newer latest slot, so it counts that one and not the thirty already
    // accounted for.
    let mut second = schedule.clone();
    second.status = Some(
        serde_json::from_value(status.clone()).expect("the patched status is a schedule status"),
    );
    let next = latest + chrono::Duration::minutes(5);
    let (client, _calls, bodies) = mock_client_recording_bodies(vec![
        absent_backup(&due_name(next)),
        Route {
            method: "PATCH",
            path_suffix: "/backupschedules/nightly/status",
            status: 200,
            body: serde_json::to_string(&second).expect("serialises"),
        },
    ]);
    reconcile_schedule(&second, &client, next + chrono::Duration::seconds(90))
        .await
        .expect("the next missed slot is a decision");
    assert_eq!(
        patched_status(&bodies.lock().expect("readable"))["missedSlots"]["count"],
        serde_json::json!(7),
        "one more, not six more: the interval is bounded by the marker the previous pass \
         advanced"
    );
}

/// D1 §4.7 row 21: the same downtime with `catchUpPolicy: Latest` runs
/// **exactly one** `CatchUp`, for the most recent slot only.
#[tokio::test]
async fn downtime_with_catch_up_latest_creates_exactly_one_catch_up() {
    let away_since = utc(2026, 9, 10, 12, 0);
    let latest = utc(2026, 9, 10, 12, 30);
    let mut schedule = cadence_schedule_cron(
        "*/5 * * * *",
        r#", "startingDeadlineSeconds": 60, "catchUpPolicy": "Latest""#,
    );
    schedule.status = Some(BackupScheduleStatus {
        missed_slots: Some(weirkeeper::crds::backup_schedule::MissedSlots {
            count: 0,
            count_capped: false,
            last_evaluated_slot: Some(slot_name(away_since)),
            recent: None,
        }),
        active_runs: Some(Vec::new()),
        policy: Some(weirkeeper::crds::backup_schedule::PolicyStatus {
            generation: 3,
            run_policy_sha256: weirkeeper::controllers::backup_schedule::run_policy_digest(
                &schedule.spec,
            ),
            time_zone: "UTC".to_string(),
            tzdb: weirkeeper::cadence::TZDB_SOURCE.to_string(),
            // OBSERVED BEFORE THE DOWNTIME, so the catch-up slot is inside this
            // revision and row 19 does not refuse it.
            effective_since: away_since - chrono::Duration::hours(1),
            evaluated_at: away_since,
        }),
        ..BackupScheduleStatus::default()
    });
    let name: &'static str = Box::leak(due_name(latest).into_boxed_str());
    let (client, _calls, bodies) = mock_client_recording_bodies(vec![
        // PLAT-05.2: step 1's inventory (D1 §6.7) replaced the bootstrap
        // LIST, and it runs on the FIRST pass over a schedule whose
        // `status.history` is absent. This schedule has no runs, so the
        // one page it reads is empty.
        no_backups(),
        absent_backup(name),
        Route {
            method: "POST",
            path_suffix: "/namespaces/logweir-t18/backups",
            status: 201,
            body: created_backup_body(name),
        },
        Route {
            method: "PATCH",
            path_suffix: "/backupschedules/nightly/status",
            status: 200,
            body: reservation_echo(
                &serde_json::to_string(&schedule).expect("serialises"),
                name,
                &slot_name(latest),
                0,
            ),
        },
    ]);
    let outcome = reconcile_schedule(&schedule, &client, latest + chrono::Duration::seconds(90))
        .await
        .expect("a catch-up is a decision");
    assert_eq!(outcome.decision.reason(), "CaughtUp");
    let bodies = bodies.lock().expect("readable").clone();
    let spec = posted_spec(&bodies);
    assert_eq!(
        spec["trigger"]["kind"],
        serde_json::json!("CatchUp"),
        "the run says what it is, so a history row does not claim to have fired on time: {spec}"
    );
    assert_eq!(
        spec["trigger"]["attempt"],
        serde_json::json!(0),
        "a catch-up IS slot S, started late — not a retry of it"
    );
    assert_eq!(
        spec["slot"],
        serde_json::json!(slot_name(latest)),
        "and only the LATEST slot is caught up; the five before it are counted, not run"
    );
    assert_eq!(body_name(posts(&bodies)[0]), name);
    let status = patched_status(&bodies);
    assert_eq!(
        status["lastSlot"]["disposition"],
        serde_json::json!("CaughtUp")
    );
    assert_eq!(
        status["missedSlots"]["count"],
        serde_json::json!(5),
        "the five slots the catch-up did not cover are still counted: {status}"
    );
    // AND THE COUNT COMES WITH ONE ENTRY AN OPERATOR CAN READ. This slot was
    // not itself skipped — it ran, as a catch-up — so without a boundary entry
    // `recent` would be empty beside a `count` of five, and a number with no
    // sample is a number nobody can act on. The names of the gap itself are
    // deliberately not enumerated (a week of one-minute slots is ten thousand
    // of them); one entry naming the slot the accounting caught up AT, with the
    // reason those slots were skipped, is what makes the count legible.
    assert_eq!(
        status["missedSlots"]["recent"],
        serde_json::json!([{
            "slot": slot_name(latest),
            "reason": "ControllerUnavailable",
            "recordedAt": latest + chrono::Duration::seconds(90),
        }]),
        "the gap moved the count, so `recent` carries the boundary entry: {status}"
    );
}

/// D1 §4.7 row 19: a catch-up never runs a slot older than the revision in
/// force.
///
/// Editing a schedule at noon must not retroactively back up the morning under
/// the new policy — the run would carry the new topic list and the new archive
/// and claim to be a backup of a window that policy never covered.
#[tokio::test]
async fn catch_up_never_runs_a_slot_before_the_observed_revision() {
    let latest = utc(2026, 9, 10, 12, 30);
    let mut schedule = cadence_schedule_cron(
        "*/5 * * * *",
        r#", "startingDeadlineSeconds": 60, "catchUpPolicy": "Latest""#,
    );
    schedule.status = Some(BackupScheduleStatus {
        active_runs: Some(Vec::new()),
        policy: Some(weirkeeper::crds::backup_schedule::PolicyStatus {
            generation: 3,
            run_policy_sha256: weirkeeper::controllers::backup_schedule::run_policy_digest(
                &schedule.spec,
            ),
            time_zone: "UTC".to_string(),
            tzdb: weirkeeper::cadence::TZDB_SOURCE.to_string(),
            // THE EDIT LANDED AFTER THE SLOT CAME DUE.
            effective_since: latest + chrono::Duration::seconds(1),
            evaluated_at: latest + chrono::Duration::seconds(1),
        }),
        ..BackupScheduleStatus::default()
    });
    let (client, _calls, bodies) = mock_client_recording_bodies(vec![
        // PLAT-05.2: step 1's inventory (D1 §6.7) replaced the bootstrap
        // LIST, and it runs on the FIRST pass over a schedule whose
        // `status.history` is absent. This schedule has no runs, so the
        // one page it reads is empty.
        no_backups(),
        absent_backup(&due_name(latest)),
        // PRESENT AND UNUSED.
        Route {
            method: "POST",
            path_suffix: "/namespaces/logweir-t18/backups",
            status: 201,
            body: created_backup_body(&due_name(latest)),
        },
        Route {
            method: "PATCH",
            path_suffix: "/backupschedules/nightly/status",
            status: 200,
            body: serde_json::to_string(&schedule).expect("serialises"),
        },
    ]);
    let outcome = reconcile_schedule(&schedule, &client, latest + chrono::Duration::seconds(90))
        .await
        .expect("a skipped catch-up is a decision");
    assert_eq!(outcome.decision.reason(), REASON_SLOT_MISSED);
    let bodies = bodies.lock().expect("readable").clone();
    assert!(posts(&bodies).is_empty());
    let status = patched_status(&bodies);
    assert_eq!(
        status["missedSlots"]["recent"][0]["reason"],
        serde_json::json!("BeforeRevision"),
        "the reason distinguishes `I chose not to` from `I was not running`: {status}"
    );
}

/// D1 §4.7 row 13: a **retryable** failure is retried once its delay has
/// elapsed, under a new name, a new execution id and the CURRENT generation.
#[tokio::test]
async fn a_retryable_failure_creates_r1_after_the_delay() {
    let slot = utc(2026, 9, 10, 12, 30);
    let failed_at = slot + chrono::Duration::seconds(30);
    let schedule = cadence_schedule_cron(
        "*/30 * * * *",
        r#", "retry": { "maxRetries": 2, "delaySeconds": 60 }"#,
    );
    let attempt0: &'static str = Box::leak(due_name(slot).into_boxed_str());
    let attempt1: &'static str = Box::leak(
        weirkeeper::slot::scheduled_backup_name_for_attempt("nightly", &slot_name(slot), 1)
            .expect("fits")
            .into_boxed_str(),
    );
    assert_eq!(
        attempt1,
        format!("{attempt0}-r1"),
        "a retry's name is attempt 0's plus `-r<k>`, so the chain is discoverable by GET"
    );

    let routes = vec![
        no_backups(),
        Route {
            method: "GET",
            path_suffix: Box::leak(format!("/backups/{attempt0}").into_boxed_str()),
            status: 200,
            // exit 1 — `operational`, no artifact. THE one exit code D1 §4.6
            // retries by number.
            body: terminal_backup(attempt0, "Failed", Some(1), Some("operational"), failed_at)
                .to_string(),
        },
        absent_backup(attempt1),
        Route {
            method: "POST",
            path_suffix: "/namespaces/logweir-t18/backups",
            status: 201,
            body: created_backup_body(attempt1),
        },
        Route {
            method: "PATCH",
            path_suffix: "/backupschedules/nightly/status",
            status: 200,
            body: reservation_echo(
                &serde_json::to_string(&schedule).expect("serialises"),
                attempt1,
                &slot_name(slot),
                1,
            ),
        },
    ];

    // BEFORE THE DELAY ELAPSES: nothing, and the status says when.
    let (client, _calls, bodies) = mock_client_recording_bodies(routes.clone());
    let early = reconcile_schedule(
        &schedule,
        &client,
        failed_at + chrono::Duration::seconds(30),
    )
    .await
    .expect("a pending retry is a decision");
    assert_eq!(early.decision.reason(), "RetryPending");
    assert!(
        posts(&bodies.lock().expect("readable")).is_empty(),
        "a retry route was available 30 seconds into a 60-second delay and was not used"
    );
    assert_eq!(
        early
            .decision
            .requeue_after(failed_at + chrono::Duration::seconds(30)),
        std::time::Duration::from_secs(30),
        "and the reconcile asks to be woken when the delay expires, not merely at the next \
         poll: D1 §4.5 step 8's min(30 s, next retry due)"
    );

    // AFTER IT: exactly one retry, named and numbered.
    let (client, _calls, bodies) = mock_client_recording_bodies(routes);
    let outcome = reconcile_schedule(
        &schedule,
        &client,
        failed_at + chrono::Duration::seconds(61),
    )
    .await
    .expect("the retry is admitted");
    assert_eq!(outcome.decision.reason(), "RetryScheduled");
    let bodies = bodies.lock().expect("readable").clone();
    let spec = posted_spec(&bodies);
    assert_eq!(body_name(posts(&bodies)[0]), attempt1);
    assert_eq!(spec["trigger"]["kind"], serde_json::json!("Retry"));
    assert_eq!(spec["trigger"]["attempt"], serde_json::json!(1));
    assert_eq!(
        spec["trigger"]["retryOf"]["name"],
        serde_json::json!(attempt0),
        "the retry names the attempt it retries, and `identity::run_identity` checks it rather \
         than trusting it: {spec}"
    );
    assert_eq!(
        spec["slot"],
        serde_json::json!(slot_name(slot)),
        "a retry is the SAME slot: the window it covers did not move"
    );
    // A NEW EXECUTION ID, NEVER A SECOND WRITE UNDER THE OLD ONE. A failed
    // attempt may have written part of an archive, and reusing its `backup_id`
    // would append into that partial prefix.
    assert_eq!(
        weirkeeper::slot::backup_id_for_attempt(UID, &slot_name(slot), 1),
        format!(
            "{}-r1",
            weirkeeper::slot::backup_id_for_attempt(UID, &slot_name(slot), 0)
        )
    );
    let status = patched_status(&bodies);
    assert_eq!(
        status["lastSlot"]["disposition"],
        serde_json::json!("Retried")
    );
    assert_eq!(status["lastSlot"]["attempt"], serde_json::json!(1));
    assert_eq!(status["activeRuns"][0]["kind"], serde_json::json!("Retry"));
}

/// D1 §4.6 and §4.7 row 9: a **decision** is never retried.
///
/// A guard refusal, a drill that did not pass and a signing failure are all
/// answers the product already gave. Re-running them asks the same question of
/// the same cluster and gets the same answer, at the cost of another broker
/// read — and, worse, tells an operator watching the schedule that something
/// might still change.
#[tokio::test]
async fn a_guard_refusal_is_never_retried() {
    let slot = utc(2026, 9, 10, 12, 30);
    let failed_at = slot + chrono::Duration::seconds(30);
    let schedule = cadence_schedule_cron(
        "*/30 * * * *",
        r#", "retry": { "maxRetries": 3, "delaySeconds": 60 }"#,
    );
    let attempt0: &'static str = Box::leak(due_name(slot).into_boxed_str());

    for (label, exit_code, exit_reason) in [
        ("a guard refusal", Some(3), Some("guard-refused")),
        ("a drill that did not pass", Some(2), Some("drill-not-pass")),
        ("a signing failure", Some(4), Some("SigningOrLock")),
        (
            "a terminal state nobody has classified",
            None,
            Some("SomethingNewAndUnclassified"),
        ),
    ] {
        let (client, _calls, bodies) = mock_client_recording_bodies(vec![
            no_backups(),
            Route {
                method: "GET",
                path_suffix: Box::leak(format!("/backups/{attempt0}").into_boxed_str()),
                status: 200,
                body: terminal_backup(attempt0, "Failed", exit_code, exit_reason, failed_at)
                    .to_string(),
            },
            absent_backup(&format!("{attempt0}-r1")),
            // PRESENT AND UNUSED IN EVERY ARM.
            Route {
                method: "POST",
                path_suffix: "/namespaces/logweir-t18/backups",
                status: 201,
                body: created_backup_body(&format!("{attempt0}-r1")),
            },
            Route {
                method: "PATCH",
                path_suffix: "/backupschedules/nightly/status",
                status: 200,
                body: serde_json::to_string(&schedule).expect("serialises"),
            },
        ]);
        // TWENTY MINUTES PAST A SIXTY-SECOND DELAY, AND STILL INSIDE SLOT
        // 12:30's window (the next `*/30` slot is 13:00). The delay is not what
        // is stopping the retry here; the classification is.
        let outcome = reconcile_schedule(
            &schedule,
            &client,
            failed_at + chrono::Duration::minutes(20),
        )
        .await
        .unwrap_or_else(|e| panic!("{label}: {e}"));
        assert_eq!(
            outcome.decision.reason(),
            "RunFailed",
            "{label} is a decision, not a blip: {:?}",
            outcome.decision
        );
        let bodies = bodies.lock().expect("readable").clone();
        assert!(
            posts(&bodies).is_empty(),
            "{label}: twenty minutes past a 60-second delay, with a POST route available: \
             {bodies:?}"
        );
        assert_eq!(
            patched_status(&bodies)["lastSlot"]["disposition"],
            serde_json::json!("Failed"),
            "{label}"
        );
    }
}

/// D1 §4.7 row 10: the chain stops at `maxRetries` and says so, and a
/// `maxRetries` LOWERED by an edit still sees the attempts that exist.
#[tokio::test]
async fn retries_stop_at_max_retries_and_report_exhausted() {
    let slot = utc(2026, 9, 10, 12, 30);
    let failed_at = slot + chrono::Duration::seconds(30);
    let attempt0: &'static str = Box::leak(due_name(slot).into_boxed_str());
    let attempt1: &'static str = Box::leak(format!("{attempt0}-r1").into_boxed_str());

    // THE THIRD ARM IS THE ONE D1 §4.7 ROW 10 CALLS OUT. `RetryExhausted` on a
    // schedule that never configured retries reads as "your retries ran out" to
    // an operator who asked for none, so an ABSENT `spec.retry` reports
    // `RunFailed` instead. `maxRetries: 0` is different: the operator DID
    // consider retries and chose zero, and the ceiling really was reached.
    for (label, retry, expected_reason) in [
        (
            "the ceiling is reached",
            r#", "retry": { "maxRetries": 1, "delaySeconds": 60 }"#,
            "RetryExhausted",
        ),
        (
            "the ceiling was LOWERED to zero by an edit",
            r#", "retry": { "maxRetries": 0, "delaySeconds": 60 }"#,
            "RetryExhausted",
        ),
        ("the retry block was REMOVED by an edit", "", "RunFailed"),
    ] {
        let mut schedule = cadence_schedule_cron("*/30 * * * *", retry);
        // THE STORED `lastSlot` IS WHAT KEEPS AN EXISTING CHAIN VISIBLE AFTER
        // THE POLICY THAT CREATED IT IS REMOVED. The attempt-chain walk stops
        // at attempt 0 on a schedule that has no retry policy and no record of
        // one — this controller is the only writer of `-r<k>` names, so walking
        // further could not find anything — and `status.lastSlot.attempt`, plus
        // any `-r<k>` in `status.activeRuns`, is how a schedule whose retry
        // block was just deleted still sees the attempts it already made. That
        // is D1 §3.1 rule 7's "a lowered maxRetries still sees existing
        // attempts", bounded so the steady-state reconcile costs one GET.
        schedule.status = Some(BackupScheduleStatus {
            last_slot: Some(weirkeeper::crds::backup_schedule::LastSlot {
                slot: slot_name(slot),
                due_at: slot,
                attempt: 1,
                disposition: "Retried".to_string(),
                backup_ref: Some(LocalRef {
                    name: attempt1.to_string(),
                }),
                reason: "RetryScheduled".to_string(),
                decided_at: failed_at,
            }),
            ..BackupScheduleStatus::default()
        });
        let (client, _calls, bodies) = mock_client_recording_bodies(vec![
            no_backups(),
            Route {
                method: "GET",
                path_suffix: Box::leak(format!("/backups/{attempt0}").into_boxed_str()),
                status: 200,
                body: terminal_backup(attempt0, "Failed", Some(1), Some("operational"), failed_at)
                    .to_string(),
            },
            Route {
                method: "GET",
                path_suffix: Box::leak(format!("/backups/{attempt1}").into_boxed_str()),
                status: 200,
                body: terminal_backup(
                    attempt1,
                    "Failed",
                    Some(1),
                    Some("operational"),
                    failed_at + chrono::Duration::minutes(2),
                )
                .to_string(),
            },
            absent_backup(&format!("{attempt0}-r2")),
            // PRESENT AND UNUSED.
            Route {
                method: "POST",
                path_suffix: "/namespaces/logweir-t18/backups",
                status: 201,
                body: created_backup_body(&format!("{attempt0}-r2")),
            },
            Route {
                method: "PATCH",
                path_suffix: "/backupschedules/nightly/status",
                status: 200,
                body: serde_json::to_string(&schedule).expect("serialises"),
            },
        ]);
        // TWENTY MINUTES PAST A SIXTY-SECOND DELAY, AND STILL INSIDE SLOT
        // 12:30's window (the next `*/30` slot is 13:00). The delay is not what
        // is stopping the retry here; the classification is.
        let outcome = reconcile_schedule(
            &schedule,
            &client,
            failed_at + chrono::Duration::minutes(20),
        )
        .await
        .unwrap_or_else(|e| panic!("{label}: {e}"));
        assert_eq!(
            outcome.decision.reason(),
            expected_reason,
            "{label}: {:?}",
            outcome.decision
        );
        let bodies = bodies.lock().expect("readable").clone();
        assert!(
            posts(&bodies).is_empty(),
            "{label}: no `-r2` is ever created: {bodies:?}"
        );
        let status = patched_status(&bodies);
        assert_eq!(
            status["lastSlot"]["disposition"],
            serde_json::json!("Exhausted"),
            "{label}: the slot is done either way; only the REASON differs, because a chain \
             that was never configured to retry exhausted nothing: {status}"
        );
        assert_eq!(
            status["lastSlot"]["attempt"],
            serde_json::json!(1),
            "{label}"
        );
    }
}

/// D1 §4.7 row 12: a retry that is due waits for concurrency like any other
/// admission, and the slot stays admissible while it waits.
#[tokio::test]
async fn a_retry_is_blocked_by_an_active_forbid_run() {
    let slot = utc(2026, 9, 10, 12, 30);
    let failed_at = slot + chrono::Duration::seconds(30);
    let schedule = cadence_schedule_cron(
        "*/30 * * * *",
        r#", "retry": { "maxRetries": 2, "delaySeconds": 60 }"#,
    );
    let attempt0: &'static str = Box::leak(due_name(slot).into_boxed_str());
    let other = "logweir-backup-nightly-20260910-120000";

    let (client, _calls, bodies) = mock_client_recording_bodies(vec![
        Route {
            method: "GET",
            path_suffix: "/namespaces/logweir-t18/backups",
            status: 200,
            body: backup_list_body(vec![backup_value(other, UID, Some("Running"), Some(other))]),
        },
        Route {
            method: "GET",
            path_suffix: Box::leak(format!("/backups/{attempt0}").into_boxed_str()),
            status: 200,
            body: terminal_backup(attempt0, "Failed", Some(1), Some("operational"), failed_at)
                .to_string(),
        },
        absent_backup(&format!("{attempt0}-r1")),
        Route {
            method: "POST",
            path_suffix: "/namespaces/logweir-t18/backups",
            status: 201,
            body: created_backup_body(&format!("{attempt0}-r1")),
        },
        Route {
            method: "PATCH",
            path_suffix: "/backupschedules/nightly/status",
            status: 200,
            body: serde_json::to_string(&schedule).expect("serialises"),
        },
    ]);
    let outcome = reconcile_schedule(&schedule, &client, failed_at + chrono::Duration::minutes(5))
        .await
        .expect("a blocked retry is a decision");
    assert_eq!(outcome.decision.reason(), "RetryBlocked");
    let bodies = bodies.lock().expect("readable").clone();
    assert!(posts(&bodies).is_empty());
    assert!(patched_status(&bodies)["conditions"][0]["message"]
        .as_str()
        .expect("a message")
        .contains("is still unfinished"));
}

/// D1 §4.7 row 7: a succeeded attempt ends the slot, whatever the retry policy
/// says.
#[tokio::test]
async fn a_succeeded_attempt_is_never_retried() {
    let slot = utc(2026, 9, 10, 12, 30);
    let schedule = cadence_schedule_cron(
        "*/30 * * * *",
        r#", "retry": { "maxRetries": 3, "delaySeconds": 60 }"#,
    );
    let attempt0: &'static str = Box::leak(due_name(slot).into_boxed_str());
    let (client, _calls, bodies) = mock_client_recording_bodies(vec![
        no_backups(),
        Route {
            method: "GET",
            path_suffix: Box::leak(format!("/backups/{attempt0}").into_boxed_str()),
            status: 200,
            body: terminal_backup(attempt0, "Succeeded", Some(0), Some("ok"), slot).to_string(),
        },
        absent_backup(&format!("{attempt0}-r1")),
        Route {
            method: "POST",
            path_suffix: "/namespaces/logweir-t18/backups",
            status: 201,
            body: created_backup_body(&format!("{attempt0}-r1")),
        },
        Route {
            method: "PATCH",
            path_suffix: "/backupschedules/nightly/status",
            status: 200,
            body: serde_json::to_string(&schedule).expect("serialises"),
        },
    ]);
    let outcome = reconcile_schedule(&schedule, &client, slot + chrono::Duration::minutes(20))
        .await
        .expect("a finished slot is a decision");
    assert_eq!(outcome.decision.reason(), REASON_SCHEDULED);
    assert!(posts(&bodies.lock().expect("readable")).is_empty());
}

/// D1 §4.7's "blocked" definition for `Allow`: ten concurrent schedule-created
/// runs is the ceiling, and reaching it is reported rather than silently
/// obeyed.
#[tokio::test]
async fn the_active_run_limit_caps_allow_at_ten() {
    let now = utc(2026, 9, 10, 12, 10);
    let running: Vec<serde_json::Value> = (0..10)
        .map(|minute| {
            let name = due_name(utc(2026, 9, 10, 12, minute));
            backup_value(&name, UID, Some("Running"), Some(&name))
        })
        .collect();
    let schedule = schedule("nightly", UID, "* * * * *", false);
    assert_eq!(schedule.spec.concurrency_policy, ConcurrencyPolicy::Allow);
    let (client, _calls, bodies) = mock_client_recording_bodies(vec![
        Route {
            method: "GET",
            path_suffix: "/namespaces/logweir-t18/backups",
            status: 200,
            body: backup_list_body(running),
        },
        absent_backup(&due_name(now)),
        // PRESENT AND UNUSED: the cap is a decision, not an inability.
        Route {
            method: "POST",
            path_suffix: "/namespaces/logweir-t18/backups",
            status: 201,
            body: created_backup_body(&due_name(now)),
        },
        Route {
            method: "PATCH",
            path_suffix: "/backupschedules/nightly/status",
            status: 200,
            body: serde_json::to_string(&schedule).expect("serialises"),
        },
    ]);
    let outcome = reconcile_schedule(&schedule, &client, now)
        .await
        .expect("the cap is a decision");
    assert_eq!(outcome.decision.reason(), "ActiveRunLimit");
    let bodies = bodies.lock().expect("readable").clone();
    assert!(posts(&bodies).is_empty());
    let status = patched_status(&bodies);
    assert_eq!(
        status["activeRuns"].as_array().map(Vec::len),
        Some(10),
        "and the ten are reported, so an operator can see what they are: {status}"
    );
    assert_eq!(status["lastMissedSlot"], slot_name(now));
}

/// D1 §4.9's CRD-before-controller guard: a reservation the API server pruned
/// stops admission instead of creating a run without an identity.
#[tokio::test]
async fn a_pruned_reservation_response_stops_admission_as_crd_outdated() {
    let now = utc(2026, 9, 10, 12, 1);
    let schedule = cadence_schedule("");
    let name: &'static str = Box::leak(due_name(now).into_boxed_str());
    let (client, _calls, bodies) = mock_client_recording_bodies(vec![
        no_backups(),
        absent_backup(name),
        Route {
            method: "POST",
            path_suffix: "/namespaces/logweir-t18/backups",
            status: 201,
            body: created_backup_body(name),
        },
        Route {
            method: "PATCH",
            path_suffix: "/backupschedules/nightly/status",
            status: 200,
            // AN OLDER CRD PRUNES WHAT IT DOES NOT DECLARE, SILENTLY. The echo
            // comes back without `status.pendingRun`, which is exactly what a
            // 1.29 API server does with a field the installed schema has never
            // heard of — no error, no warning, just an absent key.
            body: serde_json::to_string(&schedule).expect("serialises"),
        },
    ]);
    let error = reconcile_schedule(&schedule, &client, now)
        .await
        .expect_err("a pruned reservation is an error, not a silent admission");
    assert!(
        error.to_string().contains("older than this controller"),
        "the error says what to do about it: {error}"
    );
    assert!(
        posts(&bodies.lock().expect("readable")).is_empty(),
        "and NOTHING is created: a Backup written without `spec.trigger` or \
         `spec.scheduleRef.uid` has no identity, and the Backup controller would refuse it \
         terminally with ScheduledIdentityMismatch"
    );
}

/// D1 §3.1 rule 6: a retry policy whose names cannot fit is an INVALID POLICY,
/// not a schedule that silently never retries.
#[test]
fn a_retry_policy_that_cannot_be_named_is_refused_as_a_policy() {
    let thirty = "n".repeat(30);
    let spec = serde_json::from_str::<BackupSchedule>(&format!(
        r#"{{
  "apiVersion": "logweir.dev/v1alpha1",
  "kind": "BackupSchedule",
  "metadata": {{ "name": "{thirty}", "namespace": "{NS}", "uid": "{UID}", "generation": 1,
    "resourceVersion": "1" }},
  "spec": {{
    "schedule": "* * * * *",
    "sourceRef": {{ "name": "prod" }},
    "topics": ["orders"],
    "archive": {{ "url": "s3://kafka-backups/logweir" }},
    "retry": {{ "maxRetries": 1 }},
    "suspend": false
  }}
}}"#
    ))
    .expect("the fixture parses");

    let decision = decide(&thirty, &spec.spec, utc(2026, 9, 10, 12, 1));
    assert_eq!(decision.reason(), "NameTooLong");
    assert!(!decision.ready());
    assert!(
        decision.message().contains("silently never retries"),
        "the message says why this is refused rather than degraded: {}",
        decision.message()
    );

    // AND `maxRetries: 0` ON THE SAME NAME IS FINE, because no `-r<N>` name is
    // ever composed. That is the exemption the CRD's root rule R3 carries too.
    let mut zero = spec.spec.clone();
    zero.retry = Some(weirkeeper::crds::backup_schedule::RetrySpec {
        max_retries: 0,
        delay_seconds: None,
    });
    assert_eq!(
        decide(&thirty, &zero, utc(2026, 9, 10, 12, 1)).reason(),
        REASON_SCHEDULED
    );
}

/// D1 §2 and §8.3: a **manual** run of this schedule is part of its history and
/// is **not** a schedule-created run — it neither occupies a `Forbid` slot nor
/// is blocked by one.
///
/// # The trap this closes, which the W3b review found before it bit
///
/// Membership used to be the complete `BackupSchedule` controller
/// ownerReference, and a manual `Backup` has none — so "not counted" held by
/// accident. PLAT-05.1 moves membership onto `spec.scheduleRef {name, uid}`,
/// which a manual run created from a schedule DOES carry (the API copies the
/// revision so the run records what policy it ran), and PLAT-05.2 removes the
/// ownerReference outright. Without a trigger-kind clause, "Back up now" would
/// silently start blocking the next scheduled slot — and an operator would see
/// `ConcurrencyBlocked` on a schedule that is working exactly as designed.
///
/// `participates_in_concurrency` is where the two questions part:
/// history-membership stays `is_run_of_schedule` (PLAT-05.2's inventory and
/// migration need the manual run), and ACCOUNTING adds the kind.
#[tokio::test]
async fn a_manual_run_of_this_schedule_is_neither_counted_nor_blocked() {
    let now = utc(2026, 9, 10, 12, 1);
    let current = due_name(now);
    let schedule = forbid_schedule("nightly", UID, "* * * * *", false);

    // A manual `Backup` created FROM this schedule: it names the schedule, it
    // carries its UID, and it is running right now.
    let mut manual = backup_value("logweir-manual-abc123", UID, Some("Running"), None);
    manual["metadata"]
        .as_object_mut()
        .expect("metadata is an object")
        .remove("ownerReferences");
    manual["spec"]["scheduleRef"] = serde_json::json!({ "name": "nightly", "uid": UID });
    manual["spec"]["triggeredBy"] = serde_json::json!("manual");
    manual["spec"]["trigger"] = serde_json::json!({ "kind": "Manual", "attempt": 0 });
    manual["spec"]
        .as_object_mut()
        .expect("spec is an object")
        .remove("slot");

    // It IS a member of the schedule for history purposes...
    let typed: weirkeeper::crds::backup::Backup =
        serde_json::from_value(manual.clone()).expect("the fixture is a Backup");
    assert!(
        weirkeeper::identity::is_run_of_schedule(&typed, "nightly", UID),
        "a manual run created from a schedule is part of that schedule's history — PLAT-05.2's \
         inventory and migration have to see it"
    );
    // ...and it is NOT a schedule-created run.
    assert!(
        !weirkeeper::controllers::backup_schedule::participates_in_concurrency(
            &typed, "nightly", UID
        ),
        "but it does not participate in concurrencyPolicy: `Back up now` follows the CronJob \
         precedent and is neither blocked nor blocking"
    );

    let (client, _calls, bodies) = mock_client_recording_bodies(vec![
        Route {
            method: "GET",
            path_suffix: "/namespaces/logweir-t18/backups",
            status: 200,
            body: backup_list_body(vec![manual]),
        },
        absent_backup(&current),
        Route {
            method: "POST",
            path_suffix: "/namespaces/logweir-t18/backups",
            status: 201,
            body: created_backup_body(&current),
        },
        Route {
            method: "PATCH",
            path_suffix: "/backupschedules/nightly/status",
            status: 200,
            body: reservation_echo(
                &serde_json::to_string(&schedule).expect("serialises"),
                &current,
                &slot_name(now),
                0,
            ),
        },
    ]);
    let outcome = reconcile_schedule(&schedule, &client, now)
        .await
        .expect("the slot fires beside a running manual backup");
    assert_eq!(
        outcome.decision.reason(),
        REASON_SCHEDULED,
        "under Forbid, with a manual run of this very schedule still running: {:?}",
        outcome.decision
    );
    assert_eq!(outcome.created.as_deref(), Some(current.as_str()));
    let bodies = bodies.lock().expect("readable").clone();
    let status = patched_status(&bodies);
    assert_eq!(
        status["activeRuns"],
        serde_json::json!([{ "name": current, "kind": "Scheduled", "attempt": 0 }]),
        "and the manual run is not in `activeRuns`, which is the schedule-created accounting \
         list: {status}"
    );
    assert_eq!(
        status["activeBackupRef"]["name"],
        serde_json::json!(current)
    );
}

/// D1 §3.1 rule 1, R8: a **non-scheduled** object sitting on a slot's
/// deterministic name is a foreign occupant, not an attempt of that slot.
///
/// # Why it is not read as the slot's run
///
/// Rule 1 constrains the names of scheduled kinds only, so nothing refuses a
/// `Manual` Backup called `logweir-backup-<schedule>-<slot>` — the repository's
/// own `pre-connection-contract-inputs.json` fixture is exactly that shape.
/// D1 §2 discovers attempt chains by GETTING those names, so such an object
/// would otherwise look like attempt 0 of the slot.
///
/// It is not. A manual run executes under its OWN UID and therefore its own
/// execution id, so the archive it writes is not the archive slot S's run would
/// have written; adopting it would make the scheduler report a window as
/// covered by a run that covered a different one. The slot is reported
/// `SlotNameUnavailable`, the skip is recorded, and nothing is created under a
/// different name — the same answer another schedule's object gets, for the
/// same reason.
#[tokio::test]
async fn a_manual_run_squatting_a_slot_name_is_a_foreign_occupant() {
    let now = utc(2026, 9, 10, 12, 1);
    let current: &'static str = Box::leak(due_name(now).into_boxed_str());
    let schedule = forbid_schedule("nightly", UID, "* * * * *", false);

    let mut squatter = backup_value(current, UID, Some("Running"), None);
    squatter["metadata"]
        .as_object_mut()
        .expect("metadata is an object")
        .remove("ownerReferences");
    squatter["spec"]["scheduleRef"] = serde_json::json!({ "name": "nightly", "uid": UID });
    squatter["spec"]["triggeredBy"] = serde_json::json!("manual");
    squatter["spec"]["trigger"] = serde_json::json!({ "kind": "Manual", "attempt": 0 });

    let (client, _calls, bodies) = mock_client_recording_bodies(vec![
        no_backups(),
        Route {
            method: "GET",
            path_suffix: Box::leak(format!("/backups/{current}").into_boxed_str()),
            status: 200,
            body: squatter.to_string(),
        },
        // PRESENT AND UNUSED: a POST would collide with the squatter, and
        // adopting it would be worse than colliding.
        Route {
            method: "POST",
            path_suffix: "/namespaces/logweir-t18/backups",
            status: 201,
            body: created_backup_body(current),
        },
        Route {
            method: "PATCH",
            path_suffix: "/backupschedules/nightly/status",
            status: 200,
            body: serde_json::to_string(&schedule).expect("serialises"),
        },
    ]);
    let outcome = reconcile_schedule(&schedule, &client, now)
        .await
        .expect("a held name is a decision, not an error");
    assert_eq!(outcome.decision.reason(), "SlotNameUnavailable");
    assert_eq!(outcome.created, None);
    let bodies = bodies.lock().expect("readable").clone();
    assert!(posts(&bodies).is_empty());
    let status = patched_status(&bodies);
    assert_eq!(
        status["lastSlot"]["disposition"],
        serde_json::json!("NameUnavailable")
    );
    assert_eq!(status["lastMissedSlot"], slot_name(now));
    assert!(status["conditions"][0]["message"]
        .as_str()
        .expect("a message")
        .contains("held by an object this schedule does not own"));
}

/// D1 §4.5 step 5: a slot that is only WAITING is not counted, and waiting
/// longer does not count it again.
///
/// # The double-counting this closes
///
/// The skipped-slot accounting walks the open interval
/// `(lastEvaluatedSlot, S)` and adds what it finds. That is idempotent only
/// because `lastEvaluatedSlot` advances when — and only when — `S` reaches a
/// disposition it cannot come back from. A blocked slot has not: it stays
/// admissible while it is the latest due one, and it may yet run. Advancing the
/// marker there would count the same slot on every 30-second reconcile for as
/// long as the block lasts (a schedule blocked for an hour would report 120
/// missed slots that never came due), and it would also skip the slots BEFORE
/// it over — the interval they were in has been closed behind them.
///
/// So this drives the same blocked slot twice and asserts the accounting stands
/// still, then lets the block clear and asserts the slot RUNS — proving the
/// wait was a wait and not a skip.
#[tokio::test]
async fn a_blocked_slot_is_not_counted_and_waiting_longer_does_not_count_it_again() {
    let now = utc(2026, 9, 10, 12, 1);
    let blocker = due_name(utc(2026, 9, 10, 12, 0));
    let current = due_name(now);
    let mut schedule = forbid_schedule("nightly", UID, "* * * * *", false);
    schedule.status = Some(BackupScheduleStatus {
        active_runs: Some(vec![weirkeeper::crds::backup_schedule::ActiveRun {
            name: blocker.clone(),
            kind: "Scheduled".to_string(),
            attempt: 0,
        }]),
        missed_slots: Some(weirkeeper::crds::backup_schedule::MissedSlots {
            count: 4,
            count_capped: false,
            last_evaluated_slot: Some(slot_name(utc(2026, 9, 10, 11, 55))),
            recent: None,
        }),
        ..BackupScheduleStatus::default()
    });

    let routes = vec![
        Route {
            method: "GET",
            path_suffix: Box::leak(format!("/backups/{blocker}").into_boxed_str()),
            status: 200,
            body: backup_value(&blocker, UID, Some("Running"), Some(&blocker)).to_string(),
        },
        // PLAT-05.2: step 1's inventory (D1 §6.7) replaced the bootstrap
        // LIST, and it runs on the FIRST pass over a schedule whose
        // `status.history` is absent. IT SEES THE BLOCKER, because that is
        // what the inventory is for: a repaired `activeRuns` has to contain
        // the run that is blocking, or the slot this test says is blocked
        // would be admitted instead.
        Route {
            method: "GET",
            path_suffix: "/namespaces/logweir-t18/backups",
            status: 200,
            body: backup_list_body(vec![backup_value(
                &blocker,
                UID,
                Some("Running"),
                Some(&blocker),
            )]),
        },
        absent_backup(&current),
        // PRESENT AND UNUSED.
        Route {
            method: "POST",
            path_suffix: "/namespaces/logweir-t18/backups",
            status: 201,
            body: created_backup_body(&current),
        },
        Route {
            method: "PATCH",
            path_suffix: "/backupschedules/nightly/status",
            status: 200,
            body: serde_json::to_string(&schedule).expect("serialises"),
        },
    ];

    let mut carried = schedule.clone();
    for (label, at) in [
        ("the first reconcile", now),
        ("thirty seconds later", now + chrono::Duration::seconds(30)),
        ("a minute later still", now + chrono::Duration::seconds(59)),
    ] {
        let (client, _calls, bodies) = mock_client_recording_bodies(routes.clone());
        let outcome = reconcile_schedule(&carried, &client, at)
            .await
            .unwrap_or_else(|e| panic!("{label}: {e}"));
        assert_eq!(
            outcome.decision.reason(),
            REASON_CONCURRENCY_BLOCKED,
            "{label}: {:?}",
            outcome.decision
        );
        let bodies = bodies.lock().expect("readable").clone();
        assert!(posts(&bodies).is_empty(), "{label}");
        if let Some(status) = patched_status_opt(&bodies) {
            assert_eq!(
                status["missedSlots"]["count"],
                serde_json::json!(4),
                "{label}: a slot that is WAITING has not been skipped, and the four slots \
                 already accounted for are not re-counted: {status}"
            );
            assert_eq!(
                status["missedSlots"]["lastEvaluatedSlot"],
                serde_json::json!(slot_name(utc(2026, 9, 10, 11, 55))),
                "{label}: the marker does not advance past a slot that may yet run — if it \
                 did, the slots before it would be closed behind them and never counted: \
                 {status}"
            );
            carried.status = Some(
                serde_json::from_value(
                    serde_json::to_value(stored_after(carried.status.as_ref(), &status))
                        .expect("the merged status serialises"),
                )
                .expect("the merged status is a schedule status"),
            );
        }
    }

    // AND THE WAIT WAS A WAIT. The blocker finishes, and the same slot — still
    // the latest due one, still inside its starting deadline — runs.
    let (client, _calls, bodies) = mock_client_recording_bodies(vec![
        Route {
            method: "GET",
            path_suffix: Box::leak(format!("/backups/{blocker}").into_boxed_str()),
            status: 200,
            body: terminal_backup(
                &blocker,
                "Succeeded",
                Some(0),
                Some("ok"),
                utc(2026, 9, 10, 12, 1),
            )
            .to_string(),
        },
        absent_backup(&current),
        Route {
            method: "POST",
            path_suffix: "/namespaces/logweir-t18/backups",
            status: 201,
            body: created_backup_body(&current),
        },
        Route {
            method: "PATCH",
            path_suffix: "/backupschedules/nightly/status",
            status: 200,
            body: reservation_echo(
                &serde_json::to_string(&carried).expect("serialises"),
                &current,
                &slot_name(now),
                0,
            ),
        },
    ]);
    let outcome = reconcile_schedule(&carried, &client, now + chrono::Duration::seconds(59))
        .await
        .expect("the unblocked slot fires");
    assert_eq!(outcome.decision.reason(), REASON_SCHEDULED);
    assert_eq!(outcome.created.as_deref(), Some(current.as_str()));
    let status = patched_status(&bodies.lock().expect("readable"));
    assert_eq!(
        status["missedSlots"]["count"],
        serde_json::json!(9),
        "the ADMISSION is what closes the interval: the five slots that came due between the \
         marker at 11:55 and this one — 11:56 through 12:00 — were genuinely never run and are \
         counted once, here. The slot that was admitted is not among them, and neither is it \
         counted for the three passes it spent waiting: {status}"
    );
    assert_eq!(
        status["missedSlots"]["lastEvaluatedSlot"],
        serde_json::json!(slot_name(now)),
        "and the marker advances now, on the disposition it cannot come back from: {status}"
    );
}

// ===========================================================================
// Fix round 1 — the reviewer's findings, each with the row that guards it
// ===========================================================================

/// **H-1.** A slot that has already received a FINAL disposition is not counted
/// again on the next pass.
///
/// # Why the existing guard could not catch this
///
/// `a_blocked_slot_is_not_counted_and_waiting_longer_does_not_count_it_again`
/// drives the NON-final branch, where `account_skipped` returns before it
/// touches anything. This is the other branch, and it is the steady state of a
/// missed slot: a slot that was missed stays the latest due slot until the next
/// one comes due, so the controller reaches the same final disposition for it
/// on every pass in between. On a 30-second requeue a daily schedule that
/// misses one slot would inflate `count` by roughly 2,880 for that single slot,
/// fill `recent` with ten copies of it, and bump `resourceVersion` every time —
/// the churn erratum E11(d) exists to prevent, reached through a third field.
#[tokio::test]
async fn a_final_disposition_is_not_counted_twice_for_the_same_slot() {
    let away_since = utc(2026, 9, 10, 12, 0);
    let latest = utc(2026, 9, 10, 12, 30);
    let mut schedule = cadence_schedule_cron("*/5 * * * *", r#", "startingDeadlineSeconds": 60"#);
    schedule.status = Some(BackupScheduleStatus {
        missed_slots: Some(weirkeeper::crds::backup_schedule::MissedSlots {
            count: 0,
            count_capped: false,
            last_evaluated_slot: Some(slot_name(away_since)),
            recent: None,
        }),
        active_runs: Some(Vec::new()),
        ..BackupScheduleStatus::default()
    });

    let routes = vec![
        // PLAT-05.2: step 1's inventory (D1 §6.7) replaced the bootstrap
        // LIST, and it runs on the FIRST pass over a schedule whose
        // `status.history` is absent. This schedule has no runs, so the
        // one page it reads is empty.
        no_backups(),
        absent_backup(&due_name(latest)),
        // PRESENT AND UNUSED in both passes.
        Route {
            method: "POST",
            path_suffix: "/namespaces/logweir-t18/backups",
            status: 201,
            body: created_backup_body(&due_name(latest)),
        },
        Route {
            method: "PATCH",
            path_suffix: "/backupschedules/nightly/status",
            status: 200,
            body: serde_json::to_string(&schedule).expect("serialises"),
        },
    ];

    // PASS 1 — the slot is past its deadline: five slots in the gap, plus this
    // one.
    let (client, _calls, bodies) = mock_client_recording_bodies(routes.clone());
    reconcile_schedule(&schedule, &client, latest + chrono::Duration::seconds(90))
        .await
        .expect("a missed slot is a decision");
    let first = patched_status(&bodies.lock().expect("readable"));
    assert_eq!(first["missedSlots"]["count"], serde_json::json!(6));
    assert_eq!(
        first["missedSlots"]["recent"].as_array().map(Vec::len),
        Some(1)
    );

    // PASS 2 — thirty seconds later. No newer slot is due (`*/5`), so the
    // controller reaches the SAME final disposition for the SAME slot.
    let mut again = schedule.clone();
    again.status = Some(serde_json::from_value(first).expect("the patch is a schedule status"));
    let (client, _calls, bodies) = mock_client_recording_bodies(routes);
    let outcome = reconcile_schedule(&again, &client, latest + chrono::Duration::seconds(120))
        .await
        .expect("the same missed slot is still a decision");
    assert_eq!(outcome.decision.reason(), REASON_SLOT_MISSED);

    let recorded = bodies.lock().expect("readable").clone();
    match patched_status_opt(&recorded) {
        // The ideal outcome: nothing about the status moved, so nothing is
        // written at all.
        None => {}
        Some(second) => {
            assert_eq!(
                second["missedSlots"]["count"],
                serde_json::json!(6),
                "the same slot with the same final disposition is counted ONCE: {second}"
            );
            assert_eq!(
                second["missedSlots"]["recent"].as_array().map(Vec::len),
                Some(1),
                "and `recent` does not fill with copies of it: {second}"
            );
            assert_eq!(
                second["missedSlots"]["lastEvaluatedSlot"],
                serde_json::json!(slot_name(latest))
            );
        }
    }

    // PASS 3 — the NEXT slot, and it runs. The marker is the slot immediately
    // before it, so the gap between them is EMPTY: nothing was skipped, and
    // `recent` must not gain a boundary entry saying otherwise. A boundary
    // entry pushed whenever a marker exists would put a `ControllerUnavailable`
    // row into `recent` on every successful admission of every healthy
    // schedule — a sample that contradicts its own count.
    let next = latest + chrono::Duration::minutes(5);
    let admitted: &'static str = Box::leak(due_name(next).into_boxed_str());
    let mut healthy = schedule.clone();
    healthy.status = Some(BackupScheduleStatus {
        missed_slots: Some(weirkeeper::crds::backup_schedule::MissedSlots {
            count: 6,
            count_capped: false,
            last_evaluated_slot: Some(slot_name(latest)),
            recent: Some(vec![weirkeeper::crds::backup_schedule::MissedSlot {
                slot: slot_name(latest),
                reason: "PastStartingDeadline".to_string(),
                recorded_at: latest,
            }]),
        }),
        active_runs: Some(Vec::new()),
        ..BackupScheduleStatus::default()
    });
    let (client, _calls, bodies) = mock_client_recording_bodies(vec![
        // PLAT-05.2: this pass's status carries no `history` block either, so
        // step 1 inventories before it admits. The schedule has no runs yet.
        no_backups(),
        absent_backup(admitted),
        Route {
            method: "POST",
            path_suffix: "/namespaces/logweir-t18/backups",
            status: 201,
            body: created_backup_body(admitted),
        },
        Route {
            method: "PATCH",
            path_suffix: "/backupschedules/nightly/status",
            status: 200,
            body: reservation_echo(
                &serde_json::to_string(&healthy).expect("serialises"),
                admitted,
                &slot_name(next),
                0,
            ),
        },
    ]);
    let outcome = reconcile_schedule(&healthy, &client, next + chrono::Duration::seconds(10))
        .await
        .expect("the next slot fires");
    assert_eq!(outcome.decision.reason(), REASON_SCHEDULED);
    let third = patched_status(&bodies.lock().expect("readable"));
    assert_eq!(
        third["missedSlots"]["count"],
        serde_json::json!(6),
        "an admitted slot adds nothing to the count: {third}"
    );
    assert_eq!(
        third["missedSlots"]["recent"].as_array().map(Vec::len),
        Some(1),
        "and adds no boundary entry, because the gap it closed was empty: {third}"
    );
    assert_eq!(
        third["missedSlots"]["recent"][0]["reason"],
        serde_json::json!("PastStartingDeadline"),
        "the one entry is still pass 1's, not a `ControllerUnavailable` row for a slot that \
         was never skipped: {third}"
    );
}

/// **M-1.** A foreign object at a LATER attempt does not erase what the earlier
/// attempts proved about the window.
///
/// # The history this keeps honest
///
/// The walk used to abandon the whole chain at the first name this schedule
/// does not own, so a `Succeeded` attempt 0 sitting beside a squatted `-r1`
/// was reported `SlotNameUnavailable`: `lastFireTime` did not advance,
/// `lastSlot.backupRef` was `None`, and `missedSlots` counted a window that a
/// successful run had in fact covered — a false entry in the one record an
/// operator reads to tell a correct implementation from a broken schedule.
///
/// A held name matters only when something would be created under it. Attempt 0
/// succeeded, so nothing would be; the honest answer is what the attempts say.
#[tokio::test]
async fn a_foreign_object_at_a_later_attempt_does_not_erase_a_succeeded_one() {
    let slot = utc(2026, 9, 10, 12, 30);
    let schedule = cadence_schedule_cron(
        "*/30 * * * *",
        r#", "retry": { "maxRetries": 2, "delaySeconds": 60 }"#,
    );
    let attempt0: &'static str = Box::leak(due_name(slot).into_boxed_str());
    let squatted: &'static str = Box::leak(format!("{attempt0}-r1").into_boxed_str());

    // Someone's manual Backup is sitting on the retry name. Nothing refuses
    // that — it is R8's own premise.
    let mut squatter = backup_value(squatted, UID, Some("Running"), None);
    squatter["metadata"]
        .as_object_mut()
        .expect("metadata is an object")
        .remove("ownerReferences");
    squatter["spec"]["scheduleRef"] = serde_json::json!({ "name": "nightly", "uid": UID });
    squatter["spec"]["triggeredBy"] = serde_json::json!("manual");
    squatter["spec"]["trigger"] = serde_json::json!({ "kind": "Manual", "attempt": 0 });

    let (client, _calls, bodies) = mock_client_recording_bodies(vec![
        no_backups(),
        Route {
            method: "GET",
            path_suffix: Box::leak(format!("/backups/{attempt0}").into_boxed_str()),
            status: 200,
            body: terminal_backup(attempt0, "Succeeded", Some(0), Some("ok"), slot).to_string(),
        },
        Route {
            method: "GET",
            path_suffix: Box::leak(format!("/backups/{squatted}").into_boxed_str()),
            status: 200,
            body: squatter.to_string(),
        },
        // PRESENT AND UNUSED.
        Route {
            method: "POST",
            path_suffix: "/namespaces/logweir-t18/backups",
            status: 201,
            body: created_backup_body(squatted),
        },
        Route {
            method: "PATCH",
            path_suffix: "/backupschedules/nightly/status",
            status: 200,
            body: serde_json::to_string(&schedule).expect("serialises"),
        },
    ]);
    let outcome = reconcile_schedule(&schedule, &client, slot + chrono::Duration::minutes(20))
        .await
        .expect("a covered window is a decision");

    assert_eq!(
        outcome.decision.reason(),
        REASON_SCHEDULED,
        "the slot RAN and succeeded; a squatter on the retry name does not unmake that: {:?}",
        outcome.decision
    );
    assert_eq!(outcome.created.as_deref(), Some(attempt0));
    let bodies = bodies.lock().expect("readable").clone();
    assert!(posts(&bodies).is_empty());
    let status = patched_status(&bodies);
    assert_eq!(
        status["lastFireTime"],
        serde_json::json!(slot),
        "the fire is recorded, which is what a history reader needs: {status}"
    );
    assert_eq!(
        status["lastSlot"]["disposition"],
        serde_json::json!("Admitted")
    );
    assert_eq!(
        status["lastSlot"]["backupRef"]["name"],
        serde_json::json!(attempt0)
    );
    assert!(
        status["lastMissedSlot"].is_null(),
        "and the window is NOT recorded as missed: {status}"
    );

    // THE CONVERSE STILL HOLDS. With attempt 0 absent, the squatter holds the
    // name the next admission would use, and the slot IS unavailable.
    let held: &'static str = attempt0;
    let mut squatter0 = backup_value(held, UID, Some("Running"), None);
    squatter0["metadata"]
        .as_object_mut()
        .expect("metadata is an object")
        .remove("ownerReferences");
    squatter0["spec"]["scheduleRef"] = serde_json::json!({ "name": "nightly", "uid": UID });
    squatter0["spec"]["triggeredBy"] = serde_json::json!("manual");
    squatter0["spec"]["trigger"] = serde_json::json!({ "kind": "Manual", "attempt": 0 });
    let (client, _calls, bodies) = mock_client_recording_bodies(vec![
        no_backups(),
        Route {
            method: "GET",
            path_suffix: Box::leak(format!("/backups/{held}").into_boxed_str()),
            status: 200,
            body: squatter0.to_string(),
        },
        Route {
            method: "POST",
            path_suffix: "/namespaces/logweir-t18/backups",
            status: 201,
            body: created_backup_body(held),
        },
        Route {
            method: "PATCH",
            path_suffix: "/backupschedules/nightly/status",
            status: 200,
            body: serde_json::to_string(&schedule).expect("serialises"),
        },
    ]);
    let outcome = reconcile_schedule(&schedule, &client, slot + chrono::Duration::minutes(20))
        .await
        .expect("a held name is a decision");
    assert_eq!(
        outcome.decision.reason(),
        "SlotNameUnavailable",
        "the occupant holds the name attempt 0 would use, so the slot cannot run: {:?}",
        outcome.decision
    );
    assert!(posts(&bodies.lock().expect("readable")).is_empty());
}

/// **M-2.** Every schedule-created kind is counted, not just `Scheduled`.
///
/// # What the reviewer's mutant walked through
///
/// `participates_in_concurrency`'s trigger-kind clause was guarded only at its
/// `Manual` boundary: `a_manual_run_of_this_schedule_is_neither_counted_nor_blocked`
/// proves `Manual` is OUT, and nothing proved `CatchUp` and `Retry` are IN.
/// Narrowing the clause to `Scheduled | Retry` survived the whole suite — and
/// with that mutation live, a running catch-up does not block the next
/// scheduled slot (two concurrent runs of one schedule, which is precisely what
/// `Forbid` promises not to do), never appears in `status.activeRuns`, and is
/// classified `ForeignBackup` by `create_run`'s own 409 adoption.
#[tokio::test]
async fn every_schedule_created_kind_blocks_a_later_slot_under_forbid() {
    let now = utc(2026, 9, 10, 12, 1);
    let current = due_name(now);
    let earlier = due_name(utc(2026, 9, 10, 12, 0));

    for (label, kind, attempt) in [("a catch-up", "CatchUp", 0), ("a retry", "Retry", 1)] {
        // A nonterminal run of THIS schedule, of a kind that is not
        // `Scheduled`. A retry is named `…-r1`, and its name is not what makes
        // it count — its trigger kind is.
        let running = if attempt == 0 {
            earlier.clone()
        } else {
            format!("{earlier}-r1")
        };
        let mut run = backup_value(&running, UID, Some("Running"), Some(&running));
        run["spec"]["trigger"] = serde_json::json!({ "kind": kind, "attempt": attempt });
        run["spec"]["scheduleRef"] = serde_json::json!({ "name": "nightly", "uid": UID });

        let schedule = forbid_schedule("nightly", UID, "* * * * *", false);
        let (client, _calls, bodies) = mock_client_recording_bodies(vec![
            Route {
                method: "GET",
                path_suffix: "/namespaces/logweir-t18/backups",
                status: 200,
                body: backup_list_body(vec![run]),
            },
            absent_backup(&current),
            // PRESENT AND UNUSED IN BOTH ARMS.
            Route {
                method: "POST",
                path_suffix: "/namespaces/logweir-t18/backups",
                status: 201,
                body: created_backup_body(&current),
            },
            Route {
                method: "PATCH",
                path_suffix: "/backupschedules/nightly/status",
                status: 200,
                body: serde_json::to_string(&schedule).expect("serialises"),
            },
        ]);
        let outcome = reconcile_schedule(&schedule, &client, now)
            .await
            .unwrap_or_else(|e| panic!("{label}: {e}"));

        assert_eq!(
            outcome.decision.reason(),
            REASON_CONCURRENCY_BLOCKED,
            "{label} of this schedule is a schedule-created run and blocks the next slot under \
             Forbid: {:?}",
            outcome.decision
        );
        let bodies = bodies.lock().expect("readable").clone();
        assert!(
            posts(&bodies).is_empty(),
            "{label}: a POST route was available and was not used"
        );
        let status = patched_status(&bodies);
        assert_eq!(
            status["activeRuns"],
            serde_json::json!([{ "name": running, "kind": kind, "attempt": attempt }]),
            "{label}: and it is reported with its own kind, so an operator can see WHAT is \
             running: {status}"
        );
        assert_eq!(
            status["activeBackupRef"]["name"],
            serde_json::json!(running),
            "{label}"
        );
    }
}

/// A reservation naming a slot the calendar does not have is **stale status**,
/// not work to resume.
///
/// # Why the composed-name check cannot catch this on its own
///
/// `reserved_slot` re-derives `name(schedule, slot, attempt)` and compares it
/// with the recorded name — but the name carries the slot string VERBATIM, so a
/// non-calendar slot re-composes exactly and passes. The calendar is a separate
/// question, and `identity::run_identity` now asks it of `spec.slot`: a slot IS
/// a UTC instant, half of the run's name and half of its archive prefix, so an
/// object named after an instant that never occurs takes a prefix no schedule
/// can ever mint and no later run can collide with to reveal the mistake.
///
/// Resuming such a reservation would create a `Backup` only for the Backup
/// controller to refuse it terminally. The scheduler clears it instead — and
/// the schedule still fires its real due slot, which is what an operator needs.
#[tokio::test]
async fn a_reservation_naming_an_impossible_slot_is_cleared_rather_than_resumed() {
    let now = utc(2026, 9, 10, 12, 1);
    let current: &'static str = Box::leak(due_name(now).into_boxed_str());

    for bad in [
        "20261309-031700", // month 13
        "20260230-000000", // 30 February
        "20260915-256100", // hour 25, minute 61
        "00000000-000000", // month 0, day 0 — NORMALISED by chrono, not refused
    ] {
        let impossible = format!("logweir-backup-nightly-{bad}");
        assert_eq!(
            weirkeeper::slot::scheduled_backup_name("nightly", bad).expect("it composes"),
            impossible,
            "{bad}: the fixture's name composes, so a refusal is the calendar and not the name"
        );

        let mut schedule = forbid_schedule("nightly", UID, "* * * * *", false);
        schedule.status = Some(BackupScheduleStatus {
            pending_backup_ref: Some(LocalRef {
                name: impossible.clone(),
            }),
            ..BackupScheduleStatus::default()
        });

        let (client, calls, bodies) = mock_client_recording_bodies(vec![
            no_backups(),
            absent_backup(current),
            Route {
                method: "POST",
                path_suffix: "/namespaces/logweir-t18/backups",
                status: 201,
                body: created_backup_body(current),
            },
            Route {
                method: "PATCH",
                path_suffix: "/backupschedules/nightly/status",
                status: 200,
                body: reservation_echo(
                    &serde_json::to_string(&schedule).expect("serialises"),
                    current,
                    &slot_name(now),
                    0,
                ),
            },
        ]);
        let outcome = reconcile_schedule(&schedule, &client, now)
            .await
            .unwrap_or_else(|e| panic!("{bad}: stale status is not an error: {e}"));

        // NOTHING WAS ASKED ABOUT THE IMPOSSIBLE NAME. No route answers it and
        // the double panics on a request it has no route for, so this is a
        // property the table can see rather than an assertion about a result.
        for seen in calls.lock().expect("readable").iter() {
            assert!(
                !seen.uri.contains(bad),
                "{bad}: the reservation was not even looked up: {} {}",
                seen.method,
                seen.uri
            );
        }
        let bodies = bodies.lock().expect("readable").clone();
        assert_eq!(
            body_name(posts(&bodies)[0]),
            current,
            "{bad}: nothing is created UNDER the impossible slot, and the schedule gets on \
             with its real due slot"
        );
        assert_eq!(outcome.decision.reason(), REASON_SCHEDULED, "{bad}");
        assert_eq!(outcome.created.as_deref(), Some(current), "{bad}");
        let status = patched_status(&bodies);
        assert_cleared(&status, "pendingBackupRef");
        assert_cleared(&status, "pendingRun");
    }
}

/// **L-1.** The reservation's `Ready` message names the schedule's ACTUAL
/// concurrency policy.
///
/// `admit` is the uniform path for both policies, so the hard-coded `Forbid`
/// this replaces made an `Allow` schedule publish a condition naming the wrong
/// one. The final write replaces it moments later — but a watcher,
/// `kubectl get -w` or the console reads what is on the object at the time.
#[tokio::test]
async fn the_reservation_message_names_the_policy_that_admitted_the_slot() {
    let now = utc(2026, 9, 10, 12, 1);
    let current: &'static str = Box::leak(due_name(now).into_boxed_str());

    for (policy, schedule) in [
        (
            "Forbid",
            forbid_schedule("nightly", UID, "* * * * *", false),
        ),
        ("Allow", schedule("nightly", UID, "* * * * *", false)),
    ] {
        let (client, _calls, bodies) = mock_client_recording_bodies(vec![
            no_backups(),
            absent_backup(current),
            Route {
                method: "POST",
                path_suffix: "/namespaces/logweir-t18/backups",
                status: 201,
                body: created_backup_body(current),
            },
            Route {
                method: "PATCH",
                path_suffix: "/backupschedules/nightly/status",
                status: 200,
                body: reservation_echo(
                    &serde_json::to_string(&schedule).expect("serialises"),
                    current,
                    &slot_name(now),
                    0,
                ),
            },
        ]);
        reconcile_schedule(&schedule, &client, now)
            .await
            .unwrap_or_else(|e| panic!("{policy}: {e}"));
        let reserved = reserved_status(&bodies.lock().expect("readable"));
        let message = reserved["conditions"][0]["message"]
            .as_str()
            .expect("the reservation carries a message");
        assert!(
            message.contains(&format!("concurrencyPolicy {policy} atomically admitted")),
            "{policy}: the transient reservation condition must name the policy that actually \
             admitted the slot. Got: {message}"
        );
    }
}

/// **THE DESTINATION-BACKED `Backup` ARGV IS ONE `logweir backup run` ACCEPTS,
/// AND THE HANDSHAKE IT CARRIES IS ENFORCED AT BOTH ENDS** — erratum **E20**,
/// applied to D2 §3.5's store contract.
///
/// # Why this is end-to-end and not a literal compared against a literal
///
/// `weirkeeper::destination::STORE_CONTRACT_VERSION_ARG` and
/// `logweir::backup::store_contract::VERSION_ARG` are two declarations of one
/// string, in two crates that deliberately do not depend on each other (the
/// `logweir` binary ships without a Kubernetes client). Every assertion that
/// compared them would compare two literals in the same repository — which is
/// exactly how E20 shipped a `Backup` argv the CLI refused and three green
/// reviews missed it. An argv a controller emits is an interface with another
/// binary, and it is tested only when it is handed to that binary's real
/// parser.
///
/// KILLS: renaming either constant on one side; and dropping the version check
/// in `store_contract::admit`, which the negative arm below catches by handing
/// the real parser a version this build does not implement.
#[test]
fn the_destination_backed_argv_is_one_the_cli_accepts_and_the_version_is_enforced() {
    let dir = scratch_dir("d2w10-store-contract-argv");
    let spec = logweir_core::spec::BackupSpec {
        source: logweir_core::spec::BackupSourceSpec {
            bootstrap_servers: vec!["broker-0.prod:9093".to_string()],
            auth: logweir_core::spec::AuthSpec::ScramSha512 {
                username: "logweir".to_string(),
                tls: true,
            },
            topics: vec!["orders".to_string()],
        },
        storage: logweir_core::engine::StorageUrl::S3 {
            bucket: "lw-a".to_string(),
            prefix: "team-a/prod".to_string(),
            region: Some("us-east-1".to_string()),
            endpoint: Some("https://minio-a.storage.svc:9000".to_string()),
            path_style: true,
            allow_http: false,
        },
        backup_id: "b1".to_string(),
        backup: logweir_core::spec::BackupSettings::default(),
    };
    std::fs::write(
        dir.join("backup.yaml"),
        serde_yaml::to_string(&spec).expect("the stub spec serialises"),
    )
    .expect("the scratch spec is writable");
    std::fs::write(
        dir.join("allowed-clusters.json"),
        serde_json::to_string(&logweir_core::spec::AllowedClusters {
            allowed_cluster_ids: Vec::new(),
            source_cluster_id: None,
        })
        .expect("the stub allowlist serialises"),
    )
    .expect("the scratch allowlist is writable");
    std::fs::copy(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../e2e/fixtures/signed/signing.pem"),
        dir.join("key.pem"),
    )
    .expect("a valid PKCS#8 signer is copied into the scratch mount path");

    let emitted = weirkeeper::backup_execution::runner_argv_with_store_contract(
        ExecutionTrigger::Schedule,
        "b1",
    );
    let argv = argv_against(&dir, &emitted);
    let root = dir.display().to_string();
    let unmapped: Vec<&String> = argv
        .iter()
        .filter(|a| a.starts_with('/') && !a.starts_with(&root))
        .collect();
    assert!(
        unmapped.is_empty(),
        "every IN-POD path in the emitted argv is accounted for: {unmapped:?}"
    );

    // ---- THE POSITIVE ARM: the flag AND the variable, both `1` ----------
    let out = std::process::Command::new(runner_binary())
        .args(&argv)
        .env(
            weirkeeper::destination::STORE_CONTRACT_VERSION_ENV,
            weirkeeper::destination::STORE_CONTRACT_VERSION,
        )
        .env("LOGWEIR_ARCHIVE_CREDENTIALS", "static")
        .env_remove("LOGWEIR_SOURCE_PASSWORD")
        .output()
        .expect("the runner binary runs");
    let stderr = String::from_utf8_lossy(&out.stderr).to_string();
    assert!(
        !stderr.contains("unexpected argument"),
        "THE E20 FAILURE CLASS: the runner refused a flag this crate emits. argv {argv:?}\n\
         stderr: {stderr}"
    );
    assert_eq!(
        out.status.code(),
        Some(1),
        "the run passes the store contract and gets as far as the credential projection, which \
         is the first point a real broker would be needed. stderr: {stderr}"
    );
    assert!(
        stderr
            .lines()
            .next()
            .unwrap_or_default()
            .contains("$LOGWEIR_SOURCE_PASSWORD is unset"),
        "…and THAT is where it stops. stderr: {stderr}"
    );

    // ---- NEGATIVE ARM 1: a version this build does not implement --------
    let mut wrong: Vec<String> = argv.clone();
    let at = wrong
        .iter()
        .position(|a| a == weirkeeper::destination::STORE_CONTRACT_VERSION_ARG)
        .expect("the flag is on the argv");
    wrong[at + 1] = "2".to_string();
    let out = std::process::Command::new(runner_binary())
        .args(&wrong)
        .env(weirkeeper::destination::STORE_CONTRACT_VERSION_ENV, "2")
        .env_remove("LOGWEIR_SOURCE_PASSWORD")
        .output()
        .expect("the runner binary runs");
    let stderr = String::from_utf8_lossy(&out.stderr).to_string();
    assert_eq!(
        out.status.code(),
        Some(3),
        "a contract this build does not implement is REFUSED, not approximated: a runner \
         improvising its store configuration is how an approved plan reaches a different bucket. \
         stderr: {stderr}"
    );

    // ---- NEGATIVE ARM 2: the flag and the variable disagree -------------
    let out = std::process::Command::new(runner_binary())
        .args(&argv)
        .env(weirkeeper::destination::STORE_CONTRACT_VERSION_ENV, "7")
        .env_remove("LOGWEIR_SOURCE_PASSWORD")
        .output()
        .expect("the runner binary runs");
    assert_eq!(
        out.status.code(),
        Some(3),
        "two answers to which store contract is in force is no contract: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// **A RETENTION REPORT IS NEVER ABOUT ANOTHER DESTINATION'S CATALOGUE** —
/// defect RET-WRONGBUCKET, grounding G14.
///
/// The reconciler LISTS through the controller's one global handle and RENDERS
/// `aws s3 rm` commands naming the SCHEDULE's own URL. On two buckets that is a
/// report about bucket A printed as if it described bucket B, with commands
/// naming keys in B that were listed in A.
///
/// KILLS: `retention_scope` returning `GlobalHandleApplies` unconditionally,
/// and the `destination_backed` arm dropped (a destination-backed schedule then
/// gets a report computed from a handle that cannot see its bucket).
#[test]
fn a_retention_report_is_withheld_when_it_would_describe_another_bucket() {
    use weirkeeper::destination::{retention_scope, RetentionScope};

    assert!(
        retention_scope("s3://kb/team-a", Some("s3://kb"), false).reports(),
        "two prefixes of ONE bucket are both describable by a handle on that bucket — the \
         listing is prefix-scoped by the report itself"
    );
    match retention_scope("s3://team-b-bucket/x", Some("s3://kb"), false) {
        RetentionScope::WrongBucket {
            schedule_bucket,
            handle_bucket,
        } => {
            assert_eq!(schedule_bucket, "team-b-bucket");
            assert_eq!(handle_bucket, "kb");
        }
        other => panic!("a different bucket withholds the report; got {other:?}"),
    }
    assert!(
        !retention_scope("s3://kb/team-a", Some("s3://kb"), true).reports(),
        "a destination-backed schedule gets NO report until PLAT-16.1's per-destination \
         archive-inventory check: the global handle is for objects without a destinationRef"
    );
    assert!(
        !retention_scope("s3://kb/team-a", None, false).reports(),
        "and a controller with no handle reports nothing at all"
    );
    assert!(
        !retention_scope("logweir-destination://dest-a", Some("s3://kb"), true).reports(),
        "the sentinel URL is not a bucket, and an unreadable URL is never the same bucket as \
         anything — the safe direction"
    );
}
