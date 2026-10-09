//! **FX-29.** A `BackupSchedule` whose latest slot was `Missed` rewrote its
//! status on every reconcile, and every write woke the next reconcile.
//!
//! # What the PoC showed
//!
//! PoC batch 4 (2026-10-09, `claude/artifacts/poc-batch-4/findings/`): both
//! PoC schedules' `resourceVersion` rose ~120 a second each from the moment
//! they were resumed, with `status.lastSlot.decidedAt` and
//! `status.policy.evaluatedAt` restamped to the read time on every pass — 323 MB
//! of controller log in 67 minutes and API-server 429s on a shared cluster. The
//! old controller did the same. `fixtures/fx-29/poc-schedule.json` is one of
//! those two objects as the API server held it (annotations dropped).
//!
//! # The two causes, and the row each has here
//!
//! 1. **A merge patch cannot clear a field that became `None`.** The 10-08 slot
//!    was `Admitted` with a `Backup`; the 10-09 slot was `Missed` with none.
//!    `lastSlot.backupRef: None` serialises as an absent key, so the stored
//!    10-08 reference stayed on the 10-09 record; the typed "same decision?"
//!    comparison saw stored `Some` against computed `None`, stamped
//!    `decidedAt: now`, and the write that followed could never make the two
//!    agree. Rows: `the_poc_schedule_settles_after_one_write`,
//!    `a_missed_slot_after_an_admitted_one_settles_and_names_no_backup`, and
//!    the twins of the same class in `retentionReport` and `history`.
//! 2. **The reconciler woke on its own `/status` writes.** `Controller::new`
//!    triggers on every event of the object. Row:
//!    `the_watch_triggers_on_identity_and_spec_revision_only`.
//!
//! Each row's control is stated beside it: the pre-fix code fails the
//! assertion it names.
//!
//! The fake API is `weirkeeper::testing::ObjectStore`: it applies each `/status`
//! merge patch exactly as RFC 7386 says, enforces the `resourceVersion`
//! precondition, bumps the version and records every write — so "how many
//! times did the schedule's status change" is a count, and each pass is handed
//! the object as a watch would hand it.

use chrono::{DateTime, Duration, TimeZone, Utc};
use futures::StreamExt as _;
use weirkeeper::conditions::{
    apply_merge_patch, keep_instant_unless_changed, replacing, status_unchanged,
};
use weirkeeper::controllers::backup_schedule::{
    decide, reconcile_schedule, reconcile_trigger, schedule_triggers, status_patch_with_retention,
    SlotDecision,
};
use weirkeeper::crds::backup_schedule::BackupSchedule;
use weirkeeper::retention::RetentionReport;
use weirkeeper::slot::{scheduled_backup_name, slot_name};
use weirkeeper::testing::{mock_client_with_store, ObjectStore, Route, SharedStore};

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

/// The PoC object, as `kubectl get -o json` returned it at 13:15:56Z on
/// 2026-10-09, mid-loop.
const POC_SCHEDULE: &str = include_str!("fixtures/fx-29/poc-schedule.json");
const POC_NAME: &str = "sch-klp5kny4fbji4r2clgj4l4bdaf";
const POC_KEY: &str = "/namespaces/logweir-poc/backupschedules/sch-klp5kny4fbji4r2clgj4l4bdaf";

const NS: &str = "logweir-fx29";
const UID: &str = "5c0f2e29-0000-4000-8000-000000000029";
const SCHEDULE_KEY: &str = "/namespaces/logweir-fx29/backupschedules/nightly";
const BACKUPS: &str = "/namespaces/logweir-fx29/backups";
const DAILY: &str = "0 0 * * *";

fn utc(y: i32, m: u32, d: u32, h: u32, min: u32, s: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(y, m, d, h, min, s)
        .single()
        .expect("the fixture instant exists")
}

/// A daily `Forbid` schedule with no status: what `kubectl apply` creates.
fn nightly(suspend: bool, generation: i64) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "BackupSchedule",
        "metadata": {
            "name": "nightly",
            "namespace": NS,
            "uid": UID,
            "resourceVersion": "100",
            "generation": generation,
            "creationTimestamp": "2026-09-01T00:00:00Z"
        },
        "spec": {
            "schedule": DAILY,
            "sourceRef": { "name": "prod" },
            "topics": ["orders", "payments"],
            "archive": { "url": "s3://kafka-backups/logweir" },
            "concurrencyPolicy": "Forbid",
            "suspend": suspend
        }
    })
}

/// The object under `key`, typed, as the next watch event would carry it.
fn current(store: &SharedStore, key: &str) -> BackupSchedule {
    let value = store
        .lock()
        .expect("the store is readable")
        .get(key)
        .expect("the schedule is in the store");
    serde_json::from_value(value).expect("the stored object is a BackupSchedule")
}

fn stored_value(store: &SharedStore, key: &str) -> serde_json::Value {
    store
        .lock()
        .expect("the store is readable")
        .get(key)
        .expect("the object is in the store")
}

/// Accepted `/status` writes to the schedule — what moves its resourceVersion
/// and what its watch delivers.
fn status_writes(store: &SharedStore, name: &str) -> usize {
    let suffix = format!("/backupschedules/{name}/status");
    store
        .lock()
        .expect("the store is readable")
        .writes()
        .iter()
        .filter(|w| w.method == "PATCH" && w.path.ends_with(&suffix) && w.status == 200)
        .count()
}

/// `POST`s that created a `Backup`.
fn creates(store: &SharedStore) -> usize {
    store
        .lock()
        .expect("the store is readable")
        .writes()
        .iter()
        .filter(|w| w.method == "POST" && w.status == 201)
        .count()
}

/// The scheduled `Backup` name of `slot`, in the collection's own key, so the
/// store answers its `GET` and a later `POST` replaces the tombstone.
fn run_key(schedule: &str, slot: &str) -> String {
    format!(
        "{BACKUPS}/{}",
        scheduled_backup_name(schedule, slot).expect("the fixture name fits")
    )
}

/// A namespace `LIST` of `Backup`s, answered with `items`.
fn list_route(items: Vec<serde_json::Value>) -> Route {
    Route {
        method: "GET",
        path_suffix: BACKUPS,
        status: 200,
        body: serde_json::json!({
            "apiVersion": "logweir.dev/v1alpha1",
            "kind": "BackupList",
            "metadata": { "resourceVersion": "41" },
            "items": items
        })
        .to_string(),
    }
}

/// A fresh store holding `schedule` under `key`, with the `Backup` collection
/// registered so a `POST` creates.
fn store_with(key: &str, schedule: serde_json::Value) -> SharedStore {
    let store = ObjectStore::shared();
    {
        let mut s = store.lock().expect("the store is writable");
        s.put(key, schedule);
        s.collection(BACKUPS);
    }
    store
}

/// Reconcile the object under `key` at `now`, against `store` and `routes`.
async fn pass(
    store: &SharedStore,
    key: &str,
    routes: Vec<Route>,
    now: DateTime<Utc>,
) -> SlotDecision {
    let (client, _calls, _bodies) = mock_client_with_store(routes, store.clone());
    reconcile_schedule(&current(store, key), &client, now)
        .await
        .unwrap_or_else(|e| panic!("the reconcile at {now} is a decision, not an error: {e}"))
        .decision
}

/// Fire the 2026-10-08 00:00 slot of `nightly` and let its run succeed: the
/// state the PoC's schedules were in on the morning of 10-08.
async fn admitted_yesterday(store: &SharedStore) -> String {
    let fire = utc(2026, 10, 8, 0, 0, 0);
    let key = run_key("nightly", &slot_name(fire));
    store.lock().expect("writable").remove(&key);
    let decision = pass(store, SCHEDULE_KEY, vec![list_route(vec![])], fire).await;
    assert!(
        matches!(decision, SlotDecision::Due { .. }),
        "the 10-08 slot is due at its own instant: {decision:?}"
    );
    assert_eq!(creates(store), 1, "the 10-08 run was created");
    let mut run = stored_value(store, &key);
    run["status"] = serde_json::json!({ "phase": "Succeeded" });
    store.lock().expect("writable").put(&key, run);
    key.rsplit('/')
        .next()
        .expect("a key names the run")
        .to_string()
}

// ---------------------------------------------------------------------------
// The rows
// ---------------------------------------------------------------------------

/// **The PoC object itself, reconciled forty times in 0.4 s** — the cadence of
/// the loop the PoC measured. It is written ONCE, and that write is the
/// correction: the 10-08 `backupRef` leaves the 10-09 `Missed` record.
///
/// CONTROL: at `main` `a8a30428` (before this fix) the count is 40 — every pass
/// stamps `decidedAt: now`, which is the loop. Mutants: dropping `replacing`
/// keeps the stale reference (the `backupRef` assertion fails); comparing the
/// block including its instant, or always writing, makes the count 40.
#[tokio::test]
async fn the_poc_schedule_settles_after_one_write() {
    let poc: serde_json::Value = serde_json::from_str(POC_SCHEDULE).expect("the fixture is JSON");
    assert_eq!(
        poc["status"]["lastSlot"]["backupRef"]["name"],
        serde_json::json!("logweir-backup-sch-klp5kny4fbji4r2clgj4l4bdaf-20261008-020000"),
        "the premise: the live 10-09 Missed record names the 10-08 run"
    );
    let store = store_with(POC_KEY, poc.clone());
    let missed = format!(
        "/namespaces/logweir-poc/backups/{}",
        scheduled_backup_name(POC_NAME, "20261009-020000").expect("fits")
    );
    store.lock().expect("writable").remove(&missed);

    let t0 = utc(2026, 10, 9, 13, 16, 29);
    let passes = 40;
    for k in 0..passes {
        let now = t0 + Duration::milliseconds(10 * k);
        let decision = pass(&store, POC_KEY, vec![], now).await;
        assert!(
            matches!(decision, SlotDecision::Missed { .. }),
            "pass {k}: the 10-09 slot stays Missed until 10-10 02:00: {decision:?}"
        );
    }

    assert_eq!(
        status_writes(&store, POC_NAME),
        1,
        "{passes} reconciles of an unchanged Missed schedule write its status at most once"
    );
    let after = stored_value(&store, POC_KEY);
    let last = &after["status"]["lastSlot"];
    assert!(
        last.get("backupRef").is_none(),
        "a Missed slot names no Backup; the 10-08 reference is cleared, not carried: {last}"
    );
    assert_eq!(last["slot"], serde_json::json!("20261009-020000"));
    assert_eq!(last["disposition"], serde_json::json!("Missed"));
    assert_eq!(
        last["decidedAt"],
        serde_json::json!(t0),
        "the one write moved decidedAt to the pass that corrected the record, and no pass \
         after it moved it again: {last}"
    );
    assert_eq!(
        after["status"]["policy"]["evaluatedAt"],
        serde_json::json!(t0),
        "policy.evaluatedAt is when the status last moved: the one write"
    );
    assert_eq!(
        after["metadata"]["resourceVersion"],
        serde_json::json!("22669099"),
        "one write, one resourceVersion bump"
    );
    for kept in [
        "missedSlots",
        "nextRuns",
        "conditions",
        "history",
        "retentionReport",
    ] {
        assert_eq!(
            after["status"][kept], poc["status"][kept],
            "`{kept}` did not change, so the correction left it byte-for-byte"
        );
    }
}

/// **A fresh controller, the PoC's sequence from the start.** The 10-08 slot
/// is admitted and succeeds; the controller is away at 10-09 00:00 and comes
/// back at 02:00, past the one-hour deadline. The `Missed` decision is written
/// once — without a `backupRef` — and then nothing, for twenty passes.
///
/// CONTROL: before this fix the first `Missed` write could not remove the
/// 10-08 `backupRef` (absent key in a merge patch), so the record was wrong
/// from its first write and every later pass wrote again: 20 writes.
#[tokio::test]
async fn a_missed_slot_after_an_admitted_one_settles_and_names_no_backup() {
    let store = store_with(SCHEDULE_KEY, nightly(false, 4));
    let yesterday = admitted_yesterday(&store).await;
    let before = status_writes(&store, "nightly");
    let admitted = stored_value(&store, SCHEDULE_KEY)["status"]["lastSlot"].clone();
    assert_eq!(admitted["backupRef"]["name"], serde_json::json!(yesterday));

    let today = run_key("nightly", &slot_name(utc(2026, 10, 9, 0, 0, 0)));
    store.lock().expect("writable").remove(&today);
    let finished = stored_value(&store, &format!("{BACKUPS}/{yesterday}"));
    let back = utc(2026, 10, 9, 2, 0, 0);
    let passes = 20;
    for k in 0..passes {
        let now = back + Duration::seconds(k);
        let decision = pass(
            &store,
            SCHEDULE_KEY,
            vec![list_route(vec![finished.clone()])],
            now,
        )
        .await;
        assert!(
            matches!(decision, SlotDecision::Missed { .. }),
            "pass {k}: {decision:?}"
        );
    }
    assert_eq!(
        status_writes(&store, "nightly") - before,
        1,
        "the Missed decision is written once, and {passes} passes later it has not been \
         written again"
    );
    let last = stored_value(&store, SCHEDULE_KEY)["status"]["lastSlot"].clone();
    assert_eq!(last["disposition"], serde_json::json!("Missed"));
    assert_eq!(last["slot"], serde_json::json!("20261009-000000"));
    assert!(
        last.get("backupRef").is_none(),
        "the 10-09 record does not name the 10-08 run: {last}"
    );
    assert_eq!(
        last["decidedAt"],
        serde_json::json!(back),
        "decidedAt is the instant the slot was decided, not the last reconcile: {last}"
    );
}

/// **A real new slot still fires, writes and decides** — the control that the
/// rows above did not get their quiet by deciding nothing. From the settled
/// `Missed` state, 10-10 00:00 comes due: one `Backup` is created, the status
/// is written, and `lastSlot` is the new decision with its own `decidedAt`.
#[tokio::test]
async fn a_new_slot_after_a_settled_missed_slot_still_writes_and_decides() {
    let store = store_with(SCHEDULE_KEY, nightly(false, 4));
    let yesterday = admitted_yesterday(&store).await;
    let finished = stored_value(&store, &format!("{BACKUPS}/{yesterday}"));
    let missed = run_key("nightly", &slot_name(utc(2026, 10, 9, 0, 0, 0)));
    store.lock().expect("writable").remove(&missed);
    for k in 0..3 {
        pass(
            &store,
            SCHEDULE_KEY,
            vec![list_route(vec![finished.clone()])],
            utc(2026, 10, 9, 2, 0, k),
        )
        .await;
    }
    let settled_writes = status_writes(&store, "nightly");
    let settled_creates = creates(&store);

    let due = utc(2026, 10, 10, 0, 0, 0);
    let next = run_key("nightly", &slot_name(due));
    store.lock().expect("writable").remove(&next);
    let decision = pass(
        &store,
        SCHEDULE_KEY,
        vec![list_route(vec![finished.clone()])],
        due,
    )
    .await;
    assert!(
        matches!(decision, SlotDecision::Due { .. }),
        "the 10-10 slot is due: {decision:?}"
    );
    assert_eq!(creates(&store) - settled_creates, 1, "its run was created");
    assert!(
        status_writes(&store, "nightly") > settled_writes,
        "and the status was written: a new decision is a change"
    );
    let last = stored_value(&store, SCHEDULE_KEY)["status"]["lastSlot"].clone();
    assert_eq!(last["slot"], serde_json::json!("20261010-000000"));
    assert_eq!(last["disposition"], serde_json::json!("Admitted"));
    assert_eq!(
        last["backupRef"]["name"],
        serde_json::json!(next.rsplit('/').next().expect("a name")),
        "an admitted slot names its run: {last}"
    );
    assert_eq!(last["decidedAt"], serde_json::json!(due));
}

/// **A spec change still writes.** `suspend` flips on the settled `Missed`
/// schedule (generation 4 → 5): one write, the `Ready` condition says
/// `Suspended` with a new transition time, `policy.generation` follows — and
/// the pass after it writes nothing.
#[tokio::test]
async fn a_spec_change_on_a_settled_schedule_still_writes_once() {
    let store = store_with(SCHEDULE_KEY, nightly(false, 4));
    let yesterday = admitted_yesterday(&store).await;
    let finished = stored_value(&store, &format!("{BACKUPS}/{yesterday}"));
    let missed = run_key("nightly", &slot_name(utc(2026, 10, 9, 0, 0, 0)));
    store.lock().expect("writable").remove(&missed);
    for k in 0..3 {
        pass(
            &store,
            SCHEDULE_KEY,
            vec![list_route(vec![finished.clone()])],
            utc(2026, 10, 9, 2, 0, k),
        )
        .await;
    }
    let settled = status_writes(&store, "nightly");

    // The edit, as the API server stores it: spec and generation move, the
    // status is untouched.
    let mut edited = stored_value(&store, SCHEDULE_KEY);
    edited["spec"]["suspend"] = serde_json::json!(true);
    edited["metadata"]["generation"] = serde_json::json!(5);
    store.lock().expect("writable").put(SCHEDULE_KEY, edited);

    let edit_at = utc(2026, 10, 9, 9, 30, 0);
    for k in 0..5 {
        let decision = pass(
            &store,
            SCHEDULE_KEY,
            vec![list_route(vec![finished.clone()])],
            edit_at + Duration::seconds(k),
        )
        .await;
        assert!(matches!(decision, SlotDecision::Suspended), "{decision:?}");
    }
    assert_eq!(
        status_writes(&store, "nightly") - settled,
        1,
        "the spec change is written once, and the four passes after it write nothing"
    );
    let status = stored_value(&store, SCHEDULE_KEY)["status"].clone();
    assert_eq!(status["policy"]["generation"], serde_json::json!(5));
    assert_eq!(status["observedGeneration"], serde_json::json!(5));
    assert_eq!(
        status["conditions"][0]["reason"],
        serde_json::json!("Suspended")
    );
    assert_eq!(
        status["conditions"][0]["lastTransitionTime"],
        serde_json::json!(edit_at)
    );
    assert_eq!(status["policy"]["evaluatedAt"], serde_json::json!(edit_at));
}

/// **The watch wakes the reconciler for a new object or a new spec revision,
/// and never for a status write** (`schedule_triggers`, cause 2).
///
/// Seven watch events of one name: the first sighting, two of the
/// reconciler's own status writes, the schedule deleted and RE-CREATED under
/// the same name (generation 1 again, because the old one was never edited), a
/// status write on the new one, a spec edit, and a status write after it.
/// Three of them trigger.
///
/// Mutants: an identity filter (or a `resourceVersion` one) triggers all
/// seven; hashing the generation alone drops the re-created schedule, which
/// would then never be reconciled until somebody edited it.
#[tokio::test]
async fn the_watch_triggers_on_identity_and_spec_revision_only() {
    let event = |uid: &str, generation: i64, rv: &str, decided: &str| {
        let mut v = nightly(false, generation);
        v["metadata"]["uid"] = serde_json::json!(uid);
        v["metadata"]["resourceVersion"] = serde_json::json!(rv);
        v["status"] = serde_json::json!({
            "lastSlot": {
                "slot": "20261009-000000", "dueAt": "2026-10-09T00:00:00Z", "attempt": 0,
                "disposition": "Missed", "reason": "SlotMissed", "decidedAt": decided
            }
        });
        Ok::<BackupSchedule, kube::runtime::watcher::Error>(
            serde_json::from_value(v).expect("a BackupSchedule"),
        )
    };
    let other = "5c0f2e29-0000-4000-8000-0000000000ff";
    let events = vec![
        event(UID, 1, "1", "2026-10-09T02:00:00Z"),
        event(UID, 1, "2", "2026-10-09T02:00:00.010Z"),
        event(UID, 1, "3", "2026-10-09T02:00:00.020Z"),
        event(other, 1, "4", "2026-10-09T03:00:00Z"),
        event(other, 1, "5", "2026-10-09T03:00:00.010Z"),
        event(other, 2, "6", "2026-10-09T03:00:00.010Z"),
        event(other, 2, "7", "2026-10-09T03:00:00.020Z"),
    ];
    let triggered: Vec<String> = schedule_triggers(futures::stream::iter(events))
        .map(|e| {
            e.expect("no watch error")
                .metadata
                .resource_version
                .expect("an event carries a version")
        })
        .collect()
        .await;
    assert_eq!(
        triggered,
        vec!["1", "4", "6"],
        "the first sighting, the re-created object and the spec edit wake the reconciler; \
         the four status writes do not"
    );

    // The hash is a function of identity and revision ONLY.
    let a: BackupSchedule = serde_json::from_value(nightly(false, 3)).expect("typed");
    let mut b = a.clone();
    b.metadata.resource_version = Some("999".to_string());
    b.status = None;
    assert_eq!(reconcile_trigger(&a), reconcile_trigger(&b));
}

/// **The controller is built on that filter.** A source-shape row, because the
/// `Controller` itself needs a cluster to run: `controller_in` constructs the
/// controller from `schedule_triggers` and never through `Controller::new`,
/// which triggers on every event. Reverting to `Controller::new` (the
/// "requeue on own status" mutant) fails here.
#[test]
fn the_schedule_controller_is_built_on_the_filtered_trigger() {
    let path =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/controllers/backup_schedule.rs");
    let source = std::fs::read_to_string(&path).expect("the controller source is readable");
    let at = source
        .find("fn controller_in(")
        .expect("the source defines controller_in");
    let body = &source[at..];
    let body = &body[..body.find("\n}\n").expect("controller_in ends")];
    assert!(
        body.contains("schedule_triggers(") && body.contains("Controller::for_stream("),
        "controller_in must build the controller from the filtered trigger: {body}"
    );
    assert!(
        !body.contains("Controller::new("),
        "Controller::new wakes the reconciler on its own status writes: {body}"
    );
}

/// **The same class in `retentionReport`.** A rule removed from the spec
/// (`keepDays: 7` → none) yields a report with no `keepDays`; sent as an absent
/// key it would stay on the object, the findings would differ from it on every
/// pass, and `evaluatedAt` would be restamped on every pass — the same loop on
/// a schedule with an archive handle (Task 16b measured ~3,850 bumps in 90 s
/// for its own variant). Pure-function passes, each fed the object the last
/// one produced.
///
/// CONTROL: before this fix the second pass's patch carried no `keepDays`, and
/// passes 3..12 each wrote a new `evaluatedAt`.
#[test]
fn a_cleared_retention_rule_is_cleared_on_the_object_and_settles() {
    let report = |at: DateTime<Utc>, keep_days: Option<u32>| RetentionReport {
        evaluated_at: at,
        keep_last: None,
        keep_days,
        sets_kept: vec!["set-a".to_string()],
        sets_that_would_be_removed: vec![],
        aws_cli: vec![],
        mc_cli: vec![],
        skipped: vec![],
        note: keep_days
            .is_none()
            .then(|| "no retention rule is configured".to_string()),
    };
    let mut schedule: BackupSchedule = serde_json::from_value(nightly(true, 4)).expect("typed");
    let decision = decide("nightly", &schedule.spec, utc(2026, 10, 9, 0, 0, 0));
    let mut stored = serde_json::Value::Null;
    let mut store_patch = |schedule: &mut BackupSchedule, patch: &serde_json::Value| -> bool {
        let current = schedule
            .status
            .as_ref()
            .map(|s| serde_json::to_value(s).expect("serialises"));
        let unchanged = status_unchanged(current.as_ref(), patch);
        apply_merge_patch(&mut stored, &patch["status"]);
        schedule.status = Some(serde_json::from_value(stored.clone()).expect("a status"));
        !unchanged
    };

    let t0 = utc(2026, 10, 9, 0, 0, 0);
    let first =
        status_patch_with_retention(&schedule, &decision, None, Some(&report(t0, Some(7))), t0);
    assert!(store_patch(&mut schedule, &first));

    let t1 = t0 + Duration::hours(1);
    let second =
        status_patch_with_retention(&schedule, &decision, None, Some(&report(t1, None)), t1);
    assert_eq!(
        second["status"]["retentionReport"]["keepDays"],
        serde_json::Value::Null,
        "the cleared rule is sent as an explicit null"
    );
    assert!(
        second["status"]["retentionReport"]
            .as_object()
            .expect("an object")
            .contains_key("keepDays"),
        "…and not omitted, which a merge patch reads as \"leave it alone\""
    );
    assert!(
        store_patch(&mut schedule, &second),
        "the findings changed: one write"
    );
    assert!(
        schedule
            .status
            .as_ref()
            .and_then(|s| s.retention_report.as_ref())
            .and_then(|r| r.keep_days)
            .is_none(),
        "the object no longer claims keepDays"
    );

    let mut writes = 0;
    for k in 2..12 {
        let now = t0 + Duration::hours(k);
        let patch =
            status_patch_with_retention(&schedule, &decision, None, Some(&report(now, None)), now);
        if store_patch(&mut schedule, &patch) {
            writes += 1;
        }
    }
    assert_eq!(
        writes, 0,
        "the same findings write nothing, ten passes running"
    );
    assert_eq!(
        stored["retentionReport"]["evaluatedAt"],
        serde_json::json!(t1),
        "evaluatedAt names the evaluation whose findings these are"
    );
}

/// **The same class in `history`.** A migration that WAS blocked and no longer
/// is: the inventory finds nothing blocked, so `migrationBlocked` must leave
/// the object — or the `HistoryRetained` condition, computed from the stored
/// block on every pass that takes no inventory, goes back to
/// `MigrationBlocked` on the very next pass.
///
/// CONTROL: before this fix the inventory pass's patch omitted the key, the
/// stale sample stayed, and the second pass below wrote
/// `HistoryRetained=False/MigrationBlocked`.
#[tokio::test]
async fn a_cleared_migration_block_is_cleared_on_the_object() {
    let mut schedule = nightly(true, 4);
    let old = utc(2026, 10, 9, 0, 0, 0);
    schedule["status"] = serde_json::json!({
        "activeRuns": [],
        "history": {
            "runCount": 1, "runCountCapped": false, "estimatedBytes": 4096,
            "legacyOwnedRuns": 1, "legacyMigratableRuns": 0,
            "migrationBlocked": [{ "name": "logweir-backup-nightly-20261001-000000",
                                   "reason": "ApiForbidden" }],
            "ownershipScanComplete": true, "inventoriedAt": old
        }
    });
    let store = store_with(SCHEDULE_KEY, schedule);

    // Inventory due (inventoriedAt is two hours old); the blocked run is gone.
    pass(
        &store,
        SCHEDULE_KEY,
        vec![list_route(vec![])],
        old + Duration::hours(2),
    )
    .await;
    let after = stored_value(&store, SCHEDULE_KEY)["status"].clone();
    assert!(
        after["history"].get("migrationBlocked").is_none(),
        "the inventory found nothing blocked, and the object says so: {}",
        after["history"]
    );
    let retained = |status: &serde_json::Value| {
        status["conditions"]
            .as_array()
            .expect("conditions")
            .iter()
            .find(|c| c["type"] == "HistoryRetained")
            .cloned()
            .expect("a HistoryRetained condition")
    };
    assert_eq!(retained(&after)["status"], serde_json::json!("True"));

    // The next pass takes no inventory and reads the stored block.
    let writes = status_writes(&store, "nightly");
    pass(
        &store,
        SCHEDULE_KEY,
        vec![],
        old + Duration::hours(2) + Duration::seconds(30),
    )
    .await;
    assert_eq!(
        status_writes(&store, "nightly"),
        writes,
        "nothing moved, so nothing is written"
    );
    let later = stored_value(&store, SCHEDULE_KEY)["status"].clone();
    assert_eq!(
        retained(&later)["status"],
        serde_json::json!("True"),
        "and the condition does not fall back to a block that no longer exists: {}",
        retained(&later)
    );
}

// ---------------------------------------------------------------------------
// The two helpers, as pure functions
// ---------------------------------------------------------------------------

#[test]
fn replacing_sends_null_for_every_key_the_stored_block_has_and_the_new_one_lacks() {
    let stored = serde_json::json!({
        "slot": "a", "backupRef": { "name": "old" }, "nested": { "keep": 1, "gone": 2 },
        "list": [1, 2, 3]
    });
    let next = serde_json::json!({ "slot": "b", "nested": { "keep": 1 }, "list": [1] });
    let patch = replacing(next, Some(&stored));
    assert_eq!(
        patch,
        serde_json::json!({
            "slot": "b", "backupRef": null, "nested": { "keep": 1, "gone": null }, "list": [1]
        })
    );
    let mut merged = stored.clone();
    apply_merge_patch(&mut merged, &patch);
    assert_eq!(
        merged,
        serde_json::json!({ "slot": "b", "nested": { "keep": 1 }, "list": [1] }),
        "merged, the block IS the new one"
    );
    // Nothing stored, or nothing to clear: unchanged.
    let next = serde_json::json!({ "slot": "b" });
    assert_eq!(replacing(next.clone(), None), next);
    assert_eq!(
        replacing(serde_json::json!(3), Some(&stored)),
        serde_json::json!(3)
    );
}

#[test]
fn an_instant_is_kept_exactly_when_nothing_else_in_its_block_moved() {
    let stored = serde_json::json!({ "slot": "a", "decidedAt": "2026-10-09T02:00:00Z" });

    let mut same = serde_json::json!({ "slot": "a", "decidedAt": "2026-10-09T14:00:00Z" });
    assert!(keep_instant_unless_changed(
        Some(&stored),
        &mut same,
        "decidedAt"
    ));
    assert_eq!(same["decidedAt"], serde_json::json!("2026-10-09T02:00:00Z"));

    let mut moved = serde_json::json!({ "slot": "b", "decidedAt": "2026-10-09T14:00:00Z" });
    assert!(!keep_instant_unless_changed(
        Some(&stored),
        &mut moved,
        "decidedAt"
    ));
    assert_eq!(
        moved["decidedAt"],
        serde_json::json!("2026-10-09T14:00:00Z")
    );

    // Compared AS STORED: a key the patch omits is not a difference (the merge
    // keeps it), and a key it nulls is.
    let with_ref = serde_json::json!({
        "slot": "a", "backupRef": { "name": "x" }, "decidedAt": "2026-10-09T02:00:00Z"
    });
    let mut omitted = serde_json::json!({ "slot": "a", "decidedAt": "2026-10-09T14:00:00Z" });
    assert!(keep_instant_unless_changed(
        Some(&with_ref),
        &mut omitted,
        "decidedAt"
    ));
    let mut nulled = replacing(
        serde_json::json!({ "slot": "a", "decidedAt": "2026-10-09T14:00:00Z" }),
        Some(&with_ref),
    );
    assert!(!keep_instant_unless_changed(
        Some(&with_ref),
        &mut nulled,
        "decidedAt"
    ));

    // No stored instant: nothing to keep.
    let mut first = serde_json::json!({ "slot": "a", "decidedAt": "2026-10-09T14:00:00Z" });
    assert!(!keep_instant_unless_changed(
        Some(&serde_json::json!({ "slot": "a" })),
        &mut first,
        "decidedAt"
    ));
    assert!(!keep_instant_unless_changed(None, &mut first, "decidedAt"));
}
