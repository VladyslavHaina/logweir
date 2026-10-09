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
use kube::runtime::controller::Action;
use weirkeeper::conditions::{
    apply_merge_patch, keep_instant_unless_changed, replacing, status_unchanged,
};
use weirkeeper::controllers::backup_schedule::{
    decide, error_action, reconcile_pass, reconcile_schedule, reconcile_trigger, schedule_action,
    schedule_triggers, status_patch_with_retention, SlotDecision, REQUEUE_SECS,
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

/// The text of a controller's source file under `src/controllers/`.
fn controller_source(file: &str) -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src/controllers")
        .join(file);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

/// The body of the `fn` whose signature contains `needle`, from its opening
/// brace to the matching one, with every comment line dropped and all
/// whitespace removed — so a row can assert the CODE's shape, and a comment
/// that quotes a call cannot satisfy it.
fn code_of_fn(source: &str, needle: &str) -> String {
    let at = source
        .find(needle)
        .unwrap_or_else(|| panic!("the source has no {needle:?}"));
    let open = at + source[at..].find('{').expect("the fn has a body");
    let mut depth = 0usize;
    let mut close = None;
    for (i, c) in source[open..].char_indices() {
        match c {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    close = Some(open + i);
                    break;
                }
            }
            _ => {}
        }
    }
    let body = &source[open..=close.expect("the body is balanced")];
    code_only(body)
}

/// `text` with comment lines dropped and all whitespace removed.
fn code_only(text: &str) -> String {
    text.lines()
        .filter(|line| !line.trim_start().starts_with("//"))
        .collect::<String>()
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect()
}

/// **The controller is built on that filter, and on nothing else.** A
/// source-shape row, because the `Controller` itself needs a cluster to run.
///
/// The call shape is asserted, not the presence of two names (FX-29 review
/// L-1): `controller_in` hands `Controller::for_stream` the result of
/// `schedule_trigger_stream(…)` directly, and that function's body is the one
/// expression `schedule_triggers(watcher(api, …)…)`. Mutants that fail here:
/// reverting to `Controller::new` (the "requeue on own status" mutant), and
/// keeping a dead `schedule_triggers(…)` call while handing the UNFILTERED
/// stream to the controller (the reviewer's R3c).
#[test]
fn the_schedule_controller_is_built_on_the_filtered_trigger() {
    let source = controller_source("backup_schedule.rs");
    let wiring = code_of_fn(&source, "fn controller_in(");
    assert!(
        wiring.contains("Controller::for_stream(schedule_trigger_stream(api,writer),reader)"),
        "controller_in must hand for_stream the filtered trigger stream and nothing else: \
         {wiring}"
    );
    assert!(
        !wiring.contains("Controller::new(") && !wiring.contains("watcher("),
        "no second watch, and no Controller::new, which wakes on every event: {wiring}"
    );
    assert_eq!(
        code_of_fn(&source, "pub fn schedule_trigger_stream("),
        "{schedule_triggers(watcher(api,watcher::Config::default()).default_backoff()\
         .reflect(writer).applied_objects(),)}",
        "the trigger stream is the reflected watch, filtered — one expression, so the \
         unfiltered stream cannot be what is returned"
    );
}

/// **Every reconcile path returns a timed requeue — the schedule's only clock**
/// (FX-29 review M-1). Since the watch stopped delivering the schedule's own
/// status writes, a pass that returned `await_change()` would leave a steady
/// schedule asleep until somebody edited it. A source-shape row over the four
/// controllers whose time-based work rides on the requeue: each `reconcile`
/// returns its named action function, each `error_policy` its error action,
/// and no code line in the file says `await_change`. The behaviour of each
/// action is pinned by its own row.
#[test]
fn every_timed_controller_returns_its_requeue_on_every_path() {
    for (file, reconcile_returns, error_returns) in [
        (
            "backup_schedule.rs",
            vec![
                "reconcile_pass(&schedule,&ctx.client,ctx.archive.as_ref(),Utc::now()).await\
                 .map(|(_,action)|action)",
            ],
            "error_action()}",
        ),
        (
            "backup_destination.rs",
            vec!["Ok(destination_action())"],
            "destination_error_action()}",
        ),
        (
            "protection_policy.rs",
            vec!["Ok(policy_action(&outcome))"],
            "policy_error_action()}",
        ),
        (
            "retention_policy.rs",
            vec!["Ok(policy_action(&outcome))"],
            "policy_error_action()}",
        ),
    ] {
        let source = controller_source(file);
        let reconcile = code_of_fn(&source, "async fn reconcile(");
        for expected in reconcile_returns {
            assert!(
                reconcile.ends_with(&format!("{expected}}}")),
                "{file}: reconcile must return {expected}: {reconcile}"
            );
        }
        let error = code_of_fn(&source, "fn error_policy(");
        assert!(
            error.ends_with(error_returns),
            "{file}: error_policy must return {error_returns}: {error}"
        );
        assert!(
            !code_only(&source).contains("await_change"),
            "{file}: a time-driven controller never parks a pass on await_change()"
        );
    }
    let schedule = controller_source("backup_schedule.rs");
    assert!(
        code_of_fn(&schedule, "pub async fn reconcile_pass(")
            .contains("letaction=schedule_action(&outcome.decision,now);"),
        "reconcile_pass returns schedule_action of the decision"
    );
}

/// **The requeue every decision returns is a timed one, at most the poll.**
/// Every `SlotDecision` variant, by an exhaustive `match` (a new variant does
/// not compile until it is added here), plus the error path.
///
/// Mutants that fail here: `schedule_action` returning `await_change()`
/// (the reviewer's R5b, wherever it is put), and a poll longer than
/// `REQUEUE_SECS` (R5c).
#[test]
fn every_reconcile_exit_requeues_within_the_poll() {
    use weirkeeper::cadence::MissedReason;
    let now = utc(2026, 10, 9, 14, 0, 0);
    let poll = std::time::Duration::from_secs(REQUEUE_SECS);
    let unparseable = {
        let mut spec: BackupSchedule = serde_json::from_value(nightly(false, 4)).expect("typed");
        spec.spec.schedule = "not a cron".to_string();
        decide("nightly", &spec.spec, now)
    };
    assert!(matches!(unparseable, SlotDecision::Unparseable(_)));
    let slot_error = || {
        scheduled_backup_name(&"x".repeat(80), "20261009-020000")
            .expect_err("a name that does not fit")
    };
    let next = Some(now + Duration::hours(12));
    let s = || "20261009-020000".to_string();
    let decisions = vec![
        SlotDecision::Suspended,
        unparseable,
        SlotDecision::UnknownTimeZone {
            got: "Mars/Olympus".to_string(),
        },
        SlotDecision::RetryNamesTooLong {
            error: slot_error(),
        },
        SlotDecision::InvalidTopicSelection { errors: vec![] },
        SlotDecision::InvalidRunPolicy { errors: vec![] },
        SlotDecision::NoDueSlot {
            next_fire_time: next,
        },
        SlotDecision::BeforeCreation {
            due: now,
            slot: s(),
            created_at: now,
            fired_at: None,
            next_fire_time: next,
        },
        SlotDecision::AlreadyFired {
            due: now,
            slot: s(),
            last_fire_time: now,
            next_fire_time: next,
        },
        SlotDecision::Missed {
            due: now,
            slot: s(),
            reason: MissedReason::PastStartingDeadline,
            next_fire_time: next,
        },
        SlotDecision::CatchUpDue {
            due: now,
            slot: s(),
            name: "n".to_string(),
            next_fire_time: next,
        },
        SlotDecision::CaughtUp {
            due: now,
            slot: s(),
            name: "n".to_string(),
            next_fire_time: next,
        },
        SlotDecision::CatchUpBlocked {
            slot: s(),
            active_backups: vec![],
            next_fire_time: next,
        },
        SlotDecision::InProgress {
            slot: s(),
            attempt: 0,
            name: "n".to_string(),
            next_fire_time: next,
        },
        SlotDecision::Retried {
            due: now,
            slot: s(),
            name: "n".to_string(),
            attempt: 1,
            next_fire_time: next,
        },
        SlotDecision::RetryPending {
            slot: s(),
            attempt: 1,
            due_at: now + Duration::hours(1),
            next_fire_time: next,
        },
        SlotDecision::RetryBlocked {
            slot: s(),
            attempt: 1,
            active_backups: vec![],
            next_fire_time: next,
        },
        SlotDecision::RetryExhausted {
            slot: s(),
            attempt: 3,
            max_retries: 3,
            retry_configured: true,
            next_fire_time: next,
        },
        SlotDecision::RunFailed {
            slot: s(),
            attempt: 0,
            name: "n".to_string(),
            next_fire_time: next,
        },
        SlotDecision::SlotNameUnavailable {
            slot: s(),
            name: "n".to_string(),
            next_fire_time: next,
        },
        SlotDecision::ActiveRunLimit {
            slot: s(),
            active_backups: vec![],
            next_fire_time: next,
        },
        SlotDecision::CrdOutdated {
            detail: "d".to_string(),
            next_fire_time: next,
        },
        SlotDecision::ConcurrencyBlocked {
            slot: s(),
            active_backups: vec![],
            next_fire_time: next,
        },
        SlotDecision::NameTooLong {
            slot: s(),
            error: slot_error(),
            next_fire_time: next,
        },
        SlotDecision::Due {
            due: now,
            slot: s(),
            name: "n".to_string(),
            next_fire_time: next,
        },
    ];
    // EXHAUSTIVE: a variant added to `SlotDecision` stops this compiling.
    let named = |d: &SlotDecision| match d {
        SlotDecision::Suspended => "Suspended",
        SlotDecision::Unparseable(_) => "Unparseable",
        SlotDecision::UnknownTimeZone { .. } => "UnknownTimeZone",
        SlotDecision::RetryNamesTooLong { .. } => "RetryNamesTooLong",
        SlotDecision::InvalidTopicSelection { .. } => "InvalidTopicSelection",
        SlotDecision::InvalidRunPolicy { .. } => "InvalidRunPolicy",
        SlotDecision::NoDueSlot { .. } => "NoDueSlot",
        SlotDecision::BeforeCreation { .. } => "BeforeCreation",
        SlotDecision::AlreadyFired { .. } => "AlreadyFired",
        SlotDecision::Missed { .. } => "Missed",
        SlotDecision::CatchUpDue { .. } => "CatchUpDue",
        SlotDecision::CaughtUp { .. } => "CaughtUp",
        SlotDecision::CatchUpBlocked { .. } => "CatchUpBlocked",
        SlotDecision::InProgress { .. } => "InProgress",
        SlotDecision::Retried { .. } => "Retried",
        SlotDecision::RetryPending { .. } => "RetryPending",
        SlotDecision::RetryBlocked { .. } => "RetryBlocked",
        SlotDecision::RetryExhausted { .. } => "RetryExhausted",
        SlotDecision::RunFailed { .. } => "RunFailed",
        SlotDecision::SlotNameUnavailable { .. } => "SlotNameUnavailable",
        SlotDecision::ActiveRunLimit { .. } => "ActiveRunLimit",
        SlotDecision::CrdOutdated { .. } => "CrdOutdated",
        SlotDecision::ConcurrencyBlocked { .. } => "ConcurrencyBlocked",
        SlotDecision::NameTooLong { .. } => "NameTooLong",
        SlotDecision::Due { .. } => "Due",
    };
    let covered: std::collections::BTreeSet<&str> = decisions.iter().map(named).collect();
    assert_eq!(covered.len(), 25, "every variant appears once: {covered:?}");
    for decision in &decisions {
        let wait = decision.requeue_after(now);
        assert!(
            wait > std::time::Duration::ZERO && wait <= poll,
            "{}: the requeue is a real wait no longer than the {}-second poll, got {wait:?}",
            named(decision),
            REQUEUE_SECS
        );
        assert_eq!(
            schedule_action(decision, now),
            Action::requeue(wait),
            "{}: the pass returns that timed requeue, never await_change()",
            named(decision)
        );
    }
    // A retry due sooner than the poll wakes the schedule at the retry.
    let soon = SlotDecision::RetryPending {
        slot: s(),
        attempt: 1,
        due_at: now + Duration::seconds(5),
        next_fire_time: next,
    };
    assert_eq!(
        schedule_action(&soon, now),
        Action::requeue(std::time::Duration::from_secs(5))
    );
    // And a failed pass is requeued on the same poll.
    assert_eq!(error_action(), Action::requeue(poll));
    assert_eq!(REQUEUE_SECS, 30, "the poll this row and the docs state");
}

/// **The returned `Action`, on the real paths a pass takes:** decided (a slot
/// fired), missed, suspended, and an error (a status write the API server
/// refused). Each decided path returns `Action::requeue(30 s)` from
/// `reconcile_pass`; the error path returns `Err`, which `error_policy` turns
/// into `error_action()`.
#[tokio::test]
async fn the_reconcile_exit_paths_return_the_timed_requeue() {
    let poll = Action::requeue(std::time::Duration::from_secs(REQUEUE_SECS));
    let run = |store: SharedStore, routes: Vec<Route>, now: DateTime<Utc>| async move {
        let (client, _calls, _bodies) = mock_client_with_store(routes, store.clone());
        reconcile_pass(&current(&store, SCHEDULE_KEY), &client, None, now).await
    };

    // DECIDED: the 10-08 slot fires.
    let store = store_with(SCHEDULE_KEY, nightly(false, 4));
    let fire = utc(2026, 10, 8, 0, 0, 0);
    store
        .lock()
        .expect("writable")
        .remove(&run_key("nightly", &slot_name(fire)));
    let (outcome, action) = run(store.clone(), vec![list_route(vec![])], fire)
        .await
        .expect("decided");
    assert!(matches!(outcome.decision, SlotDecision::Due { .. }));
    assert_eq!(action, poll, "decided: {:?}", outcome.decision);

    // MISSED: the controller is back at 02:00 the next day.
    let today = run_key("nightly", &slot_name(utc(2026, 10, 9, 0, 0, 0)));
    store.lock().expect("writable").remove(&today);
    let (outcome, action) = run(
        store.clone(),
        vec![list_route(vec![])],
        utc(2026, 10, 9, 2, 0, 0),
    )
    .await
    .expect("missed");
    assert!(matches!(outcome.decision, SlotDecision::Missed { .. }));
    assert_eq!(action, poll, "missed");

    // SUSPENDED.
    let suspended = store_with(SCHEDULE_KEY, nightly(true, 4));
    let (outcome, action) = run(
        suspended,
        vec![list_route(vec![])],
        utc(2026, 10, 9, 2, 0, 0),
    )
    .await
    .expect("suspended");
    assert!(matches!(outcome.decision, SlotDecision::Suspended));
    assert_eq!(action, poll, "suspended");

    // ERROR: the status write is refused (no route answers it, so the double
    // returns an error), the pass is an `Err`, and the error action is the poll.
    let (client, _calls, _bodies) = weirkeeper::testing::mock_client_recording_bodies(vec![
        list_route(vec![]),
        Route {
            method: "PATCH",
            path_suffix: "/backupschedules/nightly/status",
            status: 500,
            body: serde_json::json!({"kind": "Status", "apiVersion": "v1", "status": "Failure",
                "message": "etcdserver: request timed out", "reason": "InternalError",
                "code": 500})
            .to_string(),
        },
    ]);
    let object: BackupSchedule = serde_json::from_value(nightly(true, 4)).expect("typed");
    let failed = reconcile_pass(&object, &client, None, utc(2026, 10, 9, 2, 0, 0)).await;
    assert!(
        failed.is_err(),
        "a refused status write is an error, not a decision"
    );
    assert_eq!(error_action(), poll, "error");
}

/// **A slot that comes due with NO watch event still fires, within the poll**
/// (FX-29 review M-1). The controller is driven exactly as kube drives it with
/// no event: reconcile, wait the requeue the pass returned, reconcile again.
/// It starts at 23:58:47 the day after an admitted slot; the first pass
/// records the missed 10-09 slot, and from then on the schedule is in the
/// settled state the watch filter leaves completely quiet. The 10-10 00:00
/// slot must fire on the first pass at or after it, and that pass may be at
/// most one poll (plus one second of slack) late.
///
/// Mutants that fail here: `await_change()` (no requeue to wait), and an
/// hourly poll (the slot fires an hour late — or, past the starting deadline,
/// never).
#[tokio::test]
async fn a_due_slot_fires_from_the_requeue_alone() {
    let store = store_with(SCHEDULE_KEY, nightly(false, 4));
    let yesterday = admitted_yesterday(&store).await;
    let finished = stored_value(&store, &format!("{BACKUPS}/{yesterday}"));
    let missed = run_key("nightly", &slot_name(utc(2026, 10, 9, 0, 0, 0)));
    store.lock().expect("writable").remove(&missed);
    let due = utc(2026, 10, 10, 0, 0, 0);
    store
        .lock()
        .expect("writable")
        .remove(&run_key("nightly", &slot_name(due)));

    let mut now = utc(2026, 10, 9, 23, 58, 47);
    let created_before = creates(&store);
    let mut fired_at = None;
    for _ in 0..20 {
        let (client, _calls, _bodies) =
            mock_client_with_store(vec![list_route(vec![finished.clone()])], store.clone());
        let (outcome, action) = reconcile_pass(&current(&store, SCHEDULE_KEY), &client, None, now)
            .await
            .expect("a pass is a decision");
        if creates(&store) > created_before {
            fired_at = Some(now);
            break;
        }
        let wait = outcome.decision.requeue_after(now);
        assert_eq!(
            action,
            Action::requeue(wait),
            "the pass at {now} hands kube a timed requeue; with no watch event that requeue is \
             the only thing that ever runs the next pass"
        );
        now += Duration::from_std(wait).expect("a requeue is a chrono duration");
    }
    let fired_at = fired_at.expect("the slot fired from the requeue alone, with no watch event");
    let late = fired_at - due;
    assert!(
        late >= Duration::zero()
            && late <= Duration::seconds(i64::try_from(REQUEUE_SECS).expect("small") + 1),
        "the 10-10 00:00 slot fired {late} after it came due; the bound is one poll"
    );
    assert_eq!(
        creates(&store) - created_before,
        1,
        "and it fired exactly once"
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
        "items": [1, 2, 3]
    });
    let next = serde_json::json!({ "slot": "b", "nested": { "keep": 1 }, "items": [1] });
    let patch = replacing(next, Some(&stored));
    assert_eq!(
        patch,
        serde_json::json!({
            "slot": "b", "backupRef": null, "nested": { "keep": 1, "gone": null }, "items": [1]
        })
    );
    let mut merged = stored.clone();
    apply_merge_patch(&mut merged, &patch);
    assert_eq!(
        merged,
        serde_json::json!({ "slot": "b", "nested": { "keep": 1 }, "items": [1] }),
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
