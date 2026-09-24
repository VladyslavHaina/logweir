//! MCP-13 and MCP-17 (console-ux-1): two ADDITIVE projections the shared
//! console needed and the product API did not publish.
//!
//! * `ScheduleStatusView.lastSlot` / `missedSlots` — what happened to the most
//!   recent slot and how many were skipped. The console used to tell an
//!   operator to "read the schedule with kubectl".
//! * `OperationSummary.exitCode` / `outcome` on a LIST row — the Backups
//!   table's EXIT and the History table's RESULT read `-` on every row.
//!
//! BOTH SIDES READ ONE FIXTURE. `ui/tests/fixtures/console/schedule-last-slot.json`
//! is this crate's projection of `ui/tests/fixtures/schedule-policy.json`, and
//! `ui/tests/console-ux.spec.js` decodes and renders the same file, so the
//! field names cannot drift between the API and the console.

mod support;

use logweir_api::projection::{backup, restore, schedule};
use serde_json::Value;
use weirkeeper::crds::backup::Backup as BackupCr;
use weirkeeper::crds::backup_schedule::BackupSchedule;
use weirkeeper::crds::restore::Restore as RestoreCr;

#[test]
fn a_schedule_projects_its_last_slot_and_its_missed_slots_verbatim() {
    let cr: BackupSchedule = serde_json::from_value(support::fixture("schedule-policy.json"))
        .expect("the schedule fixture deserialises");
    let projected = serde_json::to_value(schedule(&cr)).expect("the projection serialises");
    let status = &projected["status"];
    let source = &support::fixture("schedule-policy.json")["status"];

    assert_eq!(status["lastSlot"]["slot"], source["lastSlot"]["slot"]);
    assert_eq!(status["lastSlot"]["disposition"], "Admitted");
    assert_eq!(status["lastSlot"]["reason"], source["lastSlot"]["reason"]);
    assert_eq!(status["lastSlot"]["attempt"], 0);
    assert_eq!(
        status["lastSlot"]["backupRef"]["name"],
        source["lastSlot"]["backupRef"]["name"]
    );
    assert_eq!(status["missedSlots"]["count"], 1);
    assert_eq!(status["missedSlots"]["countCapped"], false);
    assert_eq!(
        status["missedSlots"]["recent"][0]["reason"],
        "PastStartingDeadline"
    );

    // THE SHARED FIXTURE IS THIS PROJECTION. The console's row decodes it.
    let golden = support::fixture("console/schedule-last-slot.json");
    assert_eq!(
        golden["item"]["status"]["lastSlot"], status["lastSlot"],
        "the console fixture is this crate's projection of the same CR"
    );
    assert_eq!(
        golden["item"]["status"]["missedSlots"],
        status["missedSlots"]
    );

    // ABSENT STAYS ABSENT: a schedule the controller has decided nothing about
    // publishes neither key, never an empty block.
    let mut bare = support::fixture("schedule-policy.json");
    let map = bare["status"].as_object_mut().unwrap();
    map.remove("lastSlot");
    map.remove("missedSlots");
    let bare: BackupSchedule = serde_json::from_value(bare).unwrap();
    let projected = serde_json::to_value(schedule(&bare)).unwrap();
    assert!(projected["status"].get("lastSlot").is_none());
    assert!(projected["status"].get("missedSlots").is_none());
}

#[test]
fn a_list_row_carries_the_runs_exit_code_and_a_restores_outcome() {
    let failed: BackupCr =
        serde_json::from_value(support::fixture("backup-valid-exit2.json")).unwrap();
    let row = serde_json::to_value(backup(&failed)).unwrap();
    assert_eq!(
        row["operation"]["exitCode"], 2,
        "BEFORE: the list row carried no exit code, and EXIT read `-` on every row"
    );
    assert_eq!(row["operation"]["verifiedSuccess"], false);

    let passed: BackupCr =
        serde_json::from_value(support::fixture("backup-valid-exit0.json")).unwrap();
    assert_eq!(
        serde_json::to_value(backup(&passed)).unwrap()["operation"]["exitCode"],
        0
    );

    let restored: RestoreCr =
        serde_json::from_value(support::fixture("restore-valid-pass.json")).unwrap();
    let row = serde_json::to_value(restore(&restored, false)).unwrap();
    assert_eq!(row["operation"]["outcome"], "pass");

    // A run with no recovered result publishes no key, never a `0`.
    let mut running = support::fixture("backup-valid-exit0.json");
    running["status"]
        .as_object_mut()
        .unwrap()
        .remove("exitCode");
    let running: BackupCr = serde_json::from_value(running).unwrap();
    let row = serde_json::to_value(backup(&running)).unwrap();
    assert_eq!(row["operation"].get("exitCode"), None::<&Value>);
}
