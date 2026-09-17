//! PLAT-05.2 — deleting a schedule stops future work and keeps every run.
//!
//! D1 §6. The rows named in §12's PLAT-05.2 table live here, plus the four the
//! brief adds: the migration is idempotent across two passes and resumable
//! after a crash mid-list, a manual run of the schedule survives its deletion
//! and is inventoried as history, and the inventory never issues an unbounded
//! LIST.
//!
//! # What a route table can and cannot prove here
//!
//! It can prove exactly what this controller ASKED the API server for, which is
//! the whole of the claim on this side: which lists, with which `limit` and
//! which selector, which PATCH bodies, and — because the double PANICS on a
//! request it has no route for — that nothing else was asked at all. That last
//! property is what makes "never deletes" assertable: there is no DELETE route
//! anywhere in this file.
//!
//! It cannot prove that the API server's garbage collector then leaves the
//! objects alone. That is `L-05.2-2`, `L-05.2-3` and `L-05.2-5` in D1 §13, and
//! it is W8's.

use chrono::{DateTime, TimeZone, Utc};
use kube::Api;
use weirkeeper::conditions::CONDITION_HISTORY_RETAINED;
use weirkeeper::controllers::backup_schedule::{reconcile_schedule, scheduled_backup};
use weirkeeper::controllers::schedule_history::{
    blocked_reason_clears_itself, history_condition, inventory_due, may_use_label_selector,
    migration_patch, migration_refusal, observe, MigrationRefusal, INVENTORY_PAGE_SIZE,
    MAX_INVENTORY_PAGES, MAX_MIGRATIONS_PER_PASS, MIGRATION_BLOCKED_SAMPLE,
};
use weirkeeper::crds::backup::Backup;
use weirkeeper::crds::backup_schedule::{
    BackupSchedule, BackupScheduleStatus, MigrationBlocked, ScheduleHistory,
};
use weirkeeper::identity::{
    is_run_of_schedule, RETAINED_FROM_OWNER_ANNOTATION, SCHEDULE_UID_LABEL,
};
use weirkeeper::slot::{scheduled_backup_name, slot_name};
use weirkeeper::testing::{mock_client_recording_bodies, Route, SeenBody};

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

/// The task's namespace (STANDING RULE 13).
const NS: &str = "logweir-t18";

/// The schedule's UID.
const UID: &str = "3f1c8a5e-0000-4000-8000-000000000001";

/// A second schedule's UID — and, in the recreation rows, the SAME NAME under a
/// new UID, which is what D1 §6.4 says a recreated schedule is.
const OTHER_UID: &str = "3f1c8a5e-0000-4000-8000-000000000002";

/// Daily at midnight, so a slot instant reads like a date.
const DAILY: &str = "0 0 * * *";

fn utc(y: i32, m: u32, d: u32, h: u32, min: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(y, m, d, h, min, 0)
        .single()
        .expect("the fixture instant exists")
}

fn schedule_json(name: &str, uid: &str, status: serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "BackupSchedule",
        "metadata": {
            "name": name,
            "namespace": NS,
            "uid": uid,
            "resourceVersion": "17",
            "generation": 4
        },
        "spec": {
            "schedule": DAILY,
            "sourceRef": { "name": "prod" },
            "topics": ["orders", "payments"],
            "archive": { "url": "s3://kafka-backups/logweir" },
            "concurrencyPolicy": "Allow",
            "suspend": false
        },
        "status": status
    })
}

fn schedule_with(name: &str, uid: &str, status: serde_json::Value) -> BackupSchedule {
    serde_json::from_value(schedule_json(name, uid, status))
        .expect("the fixture is a BackupSchedule")
}

/// A `Backup` as the controller BEFORE PLAT-05.2 created it: a controller
/// ownerReference to the schedule, a `spec.scheduleRef` with a name and no UID,
/// no `logweir.dev/schedule-uid` label and no `spec.trigger`.
///
/// THIS SHAPE IS THE WHOLE SUBJECT OF THE MIGRATION. Every assertion about
/// "before" in this file is an assertion about an object of exactly this shape,
/// and the ones about "after" are about what one merge PATCH turns it into.
fn legacy_backup(
    name: &str,
    schedule_name: &str,
    owner_uid: &str,
    phase: &str,
) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "Backup",
        "metadata": {
            "name": name,
            "namespace": NS,
            "uid": format!("uid-{name}"),
            "resourceVersion": "101",
            "ownerReferences": [{
                "apiVersion": "logweir.dev/v1alpha1",
                "kind": "BackupSchedule",
                "name": schedule_name,
                "uid": owner_uid,
                "controller": true,
                "blockOwnerDeletion": true
            }]
        },
        "spec": {
            "sourceRef": { "name": "prod" },
            "topics": ["orders", "payments"],
            "archive": { "url": "s3://kafka-backups/logweir" },
            "scheduleRef": { "name": schedule_name },
            "slot": name.get(name.len().saturating_sub(15)..).unwrap_or_default(),
            "triggeredBy": "schedule",
            "deadlineSeconds": 3600
        },
        "status": { "phase": phase }
    })
}

/// A `Backup` as THIS controller creates it: no ownerReference, membership on
/// `spec.scheduleRef {name, uid}`.
fn retained_backup(
    name: &str,
    schedule_name: &str,
    schedule_uid: &str,
    phase: &str,
) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "Backup",
        "metadata": {
            "name": name,
            "namespace": NS,
            "uid": format!("uid-{name}"),
            "resourceVersion": "101",
            "labels": {
                "logweir.dev/schedule": schedule_name,
                "logweir.dev/schedule-uid": schedule_uid,
                "logweir.dev/trigger": "scheduled"
            }
        },
        "spec": {
            "sourceRef": { "name": "prod" },
            "topics": ["orders", "payments"],
            "archive": { "url": "s3://kafka-backups/logweir" },
            "scheduleRef": { "name": schedule_name, "uid": schedule_uid },
            "slot": name.get(name.len().saturating_sub(15)..).unwrap_or_default(),
            "triggeredBy": "schedule",
            "trigger": { "kind": "Scheduled", "attempt": 0 },
            "deadlineSeconds": 3600
        },
        "status": { "phase": phase }
    })
}

/// A "Back up now" run OF a schedule: `spec.scheduleRef`, `trigger.kind:
/// Manual`, no slot, and never an ownerReference to the schedule.
fn manual_backup(
    name: &str,
    schedule_name: &str,
    schedule_uid: &str,
    phase: &str,
) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "Backup",
        "metadata": {
            "name": name,
            "namespace": NS,
            "uid": format!("uid-{name}"),
            "resourceVersion": "101",
            "labels": {
                "logweir.dev/schedule": schedule_name,
                "logweir.dev/schedule-uid": schedule_uid,
                "logweir.dev/trigger": "manual"
            }
        },
        "spec": {
            "sourceRef": { "name": "prod" },
            "topics": ["orders", "payments"],
            "archive": { "url": "s3://kafka-backups/logweir" },
            "scheduleRef": { "name": schedule_name, "uid": schedule_uid },
            "triggeredBy": "manual",
            "trigger": { "kind": "Manual", "attempt": 0 },
            "deadlineSeconds": 3600
        },
        "status": { "phase": phase }
    })
}

fn list_body(items: Vec<serde_json::Value>, continue_token: Option<&str>) -> String {
    let mut metadata = serde_json::json!({ "resourceVersion": "41" });
    if let Some(token) = continue_token {
        metadata["continue"] = serde_json::json!(token);
    }
    serde_json::json!({
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "BackupList",
        "metadata": metadata,
        "items": items
    })
    .to_string()
}

fn list_route(items: Vec<serde_json::Value>, continue_token: Option<&str>) -> Route {
    Route {
        method: "GET",
        path_suffix: "/namespaces/logweir-t18/backups",
        status: 200,
        body: list_body(items, continue_token),
    }
}

/// A `PATCH` on one `Backup`, answered with the object it was sent.
fn patch_route(name: &str, status: u16, body: serde_json::Value) -> Route {
    Route {
        method: "PATCH",
        path_suffix: Box::leak(format!("/backups/{name}").into_boxed_str()),
        status,
        body: body.to_string(),
    }
}

fn api_error(code: u16, reason: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "v1",
        "kind": "Status",
        "status": "Failure",
        "code": code,
        "reason": reason,
        "message": reason
    })
}

fn backups(client: &kube::Client) -> Api<Backup> {
    Api::namespaced(client.clone(), NS)
}

fn typed(value: &serde_json::Value) -> Backup {
    serde_json::from_value(value.clone()).expect("the fixture is a Backup")
}

/// Every request the double was asked for, as `(method, path-without-query)`.
fn calls(seen: &[SeenBody]) -> Vec<(String, String)> {
    seen.iter()
        .map(|b| {
            (
                b.method.clone(),
                b.uri.split('?').next().unwrap_or(&b.uri).to_string(),
            )
        })
        .collect()
}

/// The bodies of every `PATCH` whose path names a `Backup`.
fn backup_patches(seen: &[SeenBody]) -> Vec<serde_json::Value> {
    seen.iter()
        .filter(|b| {
            b.method == "PATCH"
                && b.uri.contains("/backups/")
                && !b.uri.contains("/backupschedules/")
        })
        .map(|b| serde_json::from_str(&b.body).expect("a patch body is JSON"))
        .collect()
}

/// A status carrying a settled `HistoryRetained=True`, which is what unlocks
/// the label-selected inventory (D1 §6.7).
fn retained_status(inventoried_at: DateTime<Utc>, run_count: i64) -> serde_json::Value {
    serde_json::json!({
        "activeRuns": [],
        "history": {
            "runCount": run_count,
            "runCountCapped": false,
            "estimatedBytes": 4096,
            "legacyOwnedRuns": 0,
            "legacyMigratableRuns": 0,
            "ownershipScanComplete": true,
            "inventoriedAt": inventoried_at
        },
        "conditions": [{
            "type": "HistoryRetained",
            "status": "True",
            "reason": "Retained",
            "message": "History is retained.",
            "lastTransitionTime": inventoried_at
        }]
    })
}

// ---------------------------------------------------------------------------
// §12 unit rows — the PATCH body is a pure function, and it is asserted as one
// ---------------------------------------------------------------------------

/// **§12 PLAT-05.2, "Migration interruption" (unit).**
///
/// One entry out, nothing else touched, and the precondition present.
#[test]
fn migration_patch_removes_only_the_matching_owner_entry() {
    let name = "logweir-backup-hist-20260910-000000";
    let mut object = legacy_backup(name, "hist", UID, "Succeeded");
    // A SECOND OWNER, AND A THIRD THAT IS ANOTHER SCHEDULE. The first is the
    // `anchor` ConfigMap D1 §13's L-05.2-1 pre-adds; the third is the UID this
    // patch must not confuse with its own.
    object["metadata"]["ownerReferences"] = serde_json::json!([
        {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "name": "anchor",
            "uid": "anchor-uid",
            "controller": false
        },
        {
            "apiVersion": "logweir.dev/v1alpha1",
            "kind": "BackupSchedule",
            "name": "hist",
            "uid": UID,
            "controller": true,
            "blockOwnerDeletion": true
        },
        {
            "apiVersion": "logweir.dev/v1alpha1",
            "kind": "BackupSchedule",
            "name": "other",
            "uid": OTHER_UID,
            "controller": false
        }
    ]);

    let patch = migration_patch(&typed(&object), "hist", UID).expect("the entry is there");
    let owners = patch["metadata"]["ownerReferences"]
        .as_array()
        .expect("two entries survive");
    assert_eq!(owners.len(), 2, "exactly one entry is removed: {patch}");
    assert_eq!(owners[0]["name"], serde_json::json!("anchor"));
    assert_eq!(owners[1]["uid"], serde_json::json!(OTHER_UID));
    assert_eq!(
        patch["metadata"]["resourceVersion"],
        serde_json::json!("101"),
        "EVERY migration PATCH carries the observed resourceVersion as the API server's \
         update precondition, so a racing edit loses with a 409 instead of having its \
         ownerReferences array silently rewritten from a stale read: {patch}"
    );
    assert_eq!(
        patch["metadata"]["labels"][SCHEDULE_UID_LABEL],
        serde_json::json!(UID)
    );
    assert_eq!(
        patch["metadata"]["annotations"][RETAINED_FROM_OWNER_ANNOTATION],
        serde_json::json!(UID),
        "membership rule 3 reads this annotation; without it the detached run belongs to \
         nothing: {patch}"
    );
    assert_eq!(
        patch["metadata"].as_object().map(|m| m.len()),
        Some(4),
        "four keys and no fifth. A patch that reached `spec` would be refused by CEL, and \
         one that reached `status` would be a different subresource: {patch}"
    );
    assert!(
        patch.get("spec").is_none() && patch.get("status").is_none(),
        "the migration writes METADATA and nothing else: {patch}"
    );

    // THE WHOLE TRIPLE, AND EACH THIRD OF IT ON ITS OWN.
    //
    // `controller: true` — the `other` entry above carries a real schedule UID
    // and a real kind and is NOT a controller reference, so it is not an
    // ownership claim and nothing may offer to remove it.
    assert!(
        migration_patch(&typed(&object), "other", OTHER_UID).is_none(),
        "a non-controller entry is not a legacy ownership claim"
    );

    // `name` — this is the case `identity::legacy_owner_uid` calls an archive
    // bug in capitals: a `Backup` whose `spec.scheduleRef` names `hist` while
    // its controller ownerReference names `retired-nightly`. It is NOT a member
    // of `hist` (membership rule 2 requires the name to match), so `hist`'s
    // migration must not reach into it — detaching another schedule's claim is
    // how one schedule comes to delete the other's garbage-collection anchor.
    let mut other_name = legacy_backup(name, "hist", UID, "Succeeded");
    other_name["metadata"]["ownerReferences"][0]["name"] = serde_json::json!("retired-nightly");
    assert!(
        migration_patch(&typed(&other_name), "hist", UID).is_none(),
        "the owner entry names another schedule, so it is not this schedule's to remove"
    );
    assert!(
        !is_run_of_schedule(&typed(&other_name), "hist", UID),
        "and the object is not this schedule's run either — which is exactly why removing \
         the entry would be reaching into somebody else's object"
    );

    // `kind` and `apiVersion` — a controller reference from another API, with
    // the same name and the same UID string. Kubernetes UIDs do not collide,
    // but this function is what decides whether an ownerReference is deleted
    // from an object, and "the UID looked familiar" is not a reason to.
    let mut other_kind = legacy_backup(name, "hist", UID, "Succeeded");
    other_kind["metadata"]["ownerReferences"][0]["kind"] = serde_json::json!("RehearsalSchedule");
    assert!(
        migration_patch(&typed(&other_kind), "hist", UID).is_none(),
        "a different kind is a different owner"
    );
    let mut other_group = legacy_backup(name, "hist", UID, "Succeeded");
    other_group["metadata"]["ownerReferences"][0]["apiVersion"] =
        serde_json::json!("example.com/v1");
    assert!(
        migration_patch(&typed(&other_group), "hist", UID).is_none(),
        "and a different API group is a different owner"
    );
}

/// **§12 PLAT-05.2, "Unrelated-resource preservation" (unit).**
///
/// The surviving entries, and every label and annotation the object already
/// had, come through the patch unchanged — the second half by merge-patch
/// semantics, which is why the patch must NOT name them.
#[test]
fn foreign_owner_references_labels_and_annotations_are_preserved_byte_for_byte() {
    let name = "logweir-backup-hist-20260910-000000";
    let mut object = legacy_backup(name, "hist", UID, "Succeeded");
    let anchor = serde_json::json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "name": "anchor",
        "uid": "0d2f-anchor",
        "controller": false,
        "blockOwnerDeletion": false
    });
    object["metadata"]["ownerReferences"] =
        serde_json::json!([anchor.clone(), object["metadata"]["ownerReferences"][0]]);
    object["metadata"]["labels"] = serde_json::json!({ "team": "x" });
    object["metadata"]["annotations"] = serde_json::json!({ "note": "y" });

    let patch = migration_patch(&typed(&object), "hist", UID).expect("the entry is there");
    assert_eq!(
        patch["metadata"]["ownerReferences"],
        serde_json::json!([anchor]),
        "the foreign entry survives BYTE FOR BYTE — every field of it, including the two \
         booleans, is rewritten exactly as observed: {patch}"
    );
    assert!(
        patch["metadata"]["labels"].get("team").is_none(),
        "`team=x` is NOT in the patch, and that is how it survives: a JSON merge patch \
         merges objects key by key, so naming only the one key this task adds leaves every \
         other label alone. A patch that spelled the whole map out would delete any label \
         written between the read and the write: {patch}"
    );
    assert!(
        patch["metadata"]["annotations"].get("note").is_none(),
        "and the same for annotations: {patch}"
    );

    // AND THE EMPTY CASE IS `null`, not `[]` — D1 §6.2 step 3.
    let alone = legacy_backup(name, "hist", UID, "Succeeded");
    let patch = migration_patch(&typed(&alone), "hist", UID).expect("the entry is there");
    assert_eq!(
        patch["metadata"]["ownerReferences"],
        serde_json::Value::Null,
        "the last entry removed leaves no `ownerReferences` key at all: {patch}"
    );
}

/// **The two refusals, as a closed set.** A run that is still going keeps its
/// owner (D1 §6.2 step 2); a run whose `spec.scheduleRef` does not name this
/// schedule keeps it too, because detaching it would orphan it.
#[test]
fn only_a_terminal_run_that_names_this_schedule_is_detached() {
    let name = "logweir-backup-hist-20260910-000000";
    for phase in ["Succeeded", "Failed", "Refused"] {
        assert_eq!(
            migration_refusal(&typed(&legacy_backup(name, "hist", UID, phase)), "hist"),
            None,
            "`{phase}` is terminal, so the run is history and may be detached"
        );
    }
    for phase in [
        "Pending",
        "Running",
        "Resolving",
        "SomethingNewerControllersWrite",
    ] {
        assert_eq!(
            migration_refusal(&typed(&legacy_backup(name, "hist", UID, phase)), "hist"),
            Some(MigrationRefusal::NotTerminal),
            "`{phase}` is not terminal. PLAT-06.1's scheduled identity for an UNFROZEN legacy \
             object still derives from the owner UID, so removing the entry mid-flight would \
             leave the run with no identity at all and the Backup reconciler would refuse it \
             `ScheduledIdentityMismatch` before its POST."
        );
    }

    // THE HAZARD D1 §6.2 DOES NOT NAME. Membership rule 3 is "the annotation
    // carries the UID AND `spec.scheduleRef.name` matches". An object that is a
    // member only through its ownerReference would, after the patch, be a
    // member of nothing: not inventoried, not in any history view, not
    // restorable through the schedule.
    let mut mismatched = legacy_backup(name, "hist", UID, "Succeeded");
    mismatched["spec"]["scheduleRef"] = serde_json::json!({ "name": "somebody-else" });
    assert_eq!(
        migration_refusal(&typed(&mismatched), "hist"),
        Some(MigrationRefusal::NoScheduleReference),
        "detaching this would orphan it, so it is reported instead"
    );
    let mut absent = legacy_backup(name, "hist", UID, "Succeeded");
    absent["spec"]
        .as_object_mut()
        .expect("spec")
        .remove("scheduleRef");
    assert_eq!(
        migration_refusal(&typed(&absent), "hist"),
        Some(MigrationRefusal::NoScheduleReference)
    );
}

/// A migrated object is still a member — the property the whole migration turns
/// on, asserted over the patch it actually sends.
#[test]
fn a_migrated_run_is_still_a_run_of_its_schedule() {
    let name = "logweir-backup-hist-20260910-000000";
    let object = legacy_backup(name, "hist", UID, "Succeeded");
    assert!(
        is_run_of_schedule(&typed(&object), "hist", UID),
        "before: a member through its ownerReference (rule 2)"
    );

    let patch = migration_patch(&typed(&object), "hist", UID).expect("the entry is there");
    let mut after = object.clone();
    weirkeeper::conditions::apply_merge_patch(&mut after, &patch);
    assert!(
        after["metadata"]["ownerReferences"].is_null(),
        "after: no owner entry at all, so the garbage collector has nothing to follow"
    );
    assert!(
        is_run_of_schedule(&typed(&after), "hist", UID),
        "after: STILL a member, through rule 3 — the annotation plus the matching \
         `spec.scheduleRef.name`. If this were false the migration would orphan exactly the \
         history it exists to keep."
    );
    assert!(
        !is_run_of_schedule(&typed(&after), "hist", OTHER_UID),
        "and it is not a member of a DIFFERENT UID under the same name, which is what D1 \
         §6.4 makes a recreated schedule"
    );
}

// ---------------------------------------------------------------------------
// The condition is a pure map from the recorded block
// ---------------------------------------------------------------------------

fn history(
    run_count: i64,
    estimated: i64,
    owned: i64,
    migratable: i64,
    blocked: Option<Vec<MigrationBlocked>>,
) -> ScheduleHistory {
    ScheduleHistory {
        run_count,
        run_count_capped: false,
        estimated_bytes: estimated,
        legacy_owned_runs: owned,
        legacy_migratable_runs: migratable,
        migration_blocked: blocked,
        // The arms below are about a walk that FINISHED. The arm about one that
        // did not has its own row, `a_capped_namespace_wide_walk_…`.
        ownership_scan_complete: true,
        inventoried_at: utc(2026, 9, 10, 0, 0),
    }
}

/// **`HistoryRetained` is computed, and every arm of §3.4's closed reason set
/// is reachable from a block this controller can record.**
#[test]
fn the_history_condition_is_a_pure_map_from_the_recorded_block() {
    let now = utc(2026, 9, 10, 1, 0);
    let cases: Vec<(ScheduleHistory, &str, &str)> = vec![
        (history(12, 4096, 0, 0, None), "True", "Retained"),
        (history(0, 0, 0, 0, None), "True", "Retained"),
        (history(2_001, 4096, 0, 0, None), "True", "HistoryLarge"),
        (
            history(12, 64 * 1024 * 1024 + 1, 0, 0, None),
            "True",
            "HistoryLarge",
        ),
        (
            history(12, 4096, 3, 3, None),
            "False",
            "LegacyOwnerReferencesRemain",
        ),
        (
            history(12, 4096, 2, 0, None),
            "False",
            "ActiveLegacyRunsOwned",
        ),
        (
            history(
                12,
                4096,
                1,
                0,
                Some(vec![MigrationBlocked {
                    name: "logweir-backup-hist-20260910-000000".to_string(),
                    reason: "422".to_string(),
                }]),
            ),
            "False",
            "MigrationBlocked",
        ),
        // A BLOCKED RUN WINS OVER A LARGE HISTORY, because one needs a human
        // and the other is advice.
        (
            history(
                9_000,
                4096,
                1,
                0,
                Some(vec![MigrationBlocked {
                    name: "b".to_string(),
                    reason: "403".to_string(),
                }]),
            ),
            "False",
            "MigrationBlocked",
        ),
    ];
    for (block, status, reason) in cases {
        let condition = history_condition(&block, None, Some(4), now);
        assert_eq!(condition.status, status, "{block:?}");
        assert_eq!(condition.reason.as_deref(), Some(reason), "{block:?}");
        assert_eq!(condition.r#type, CONDITION_HISTORY_RETAINED);
        assert_eq!(condition.observed_generation, Some(4));
        assert!(
            condition.message.is_some_and(|m| !m.is_empty()),
            "every arm says WHY in words an operator can act on"
        );
    }

    // THE BLOCKED SAMPLE IS BOUNDED AND NAMED (D1 §6.2 step 4: "naming up to
    // 10"). A status that named every blocked run on a 9 000-run schedule
    // would be a status nobody can read.
    let many: Vec<MigrationBlocked> = (0..25)
        .map(|i| MigrationBlocked {
            name: format!("b{i}"),
            reason: "422".to_string(),
        })
        .collect();
    let condition = history_condition(&history(30, 4096, 25, 0, Some(many)), None, Some(4), now);
    let message = condition.message.expect("a message");
    assert!(message.contains("b0") && message.contains("b9"));
    assert!(!message.contains("b10"), "at most ten are named: {message}");

    // AND `lastTransitionTime` OBEYS THE metav1 RULE — plan erratum E11(d).
    // This reconciler requeues every 30 s; a condition that moved its instant
    // on every pass would be 2,880 lies a day about when the state changed.
    let settled = history_condition(&history(12, 4096, 0, 0, None), None, Some(4), now);
    let again = history_condition(
        &history(13, 5000, 0, 0, None),
        Some(&settled),
        Some(4),
        now + chrono::Duration::hours(2),
    );
    assert_eq!(
        again.last_transition_time, settled.last_transition_time,
        "the run count moved and the STATE did not, so the instant stands still"
    );
    let moved = history_condition(
        &history(13, 5000, 1, 1, None),
        Some(&settled),
        Some(4),
        now + chrono::Duration::hours(2),
    );
    assert_ne!(
        moved.last_transition_time, settled.last_transition_time,
        "True/Retained became False/LegacyOwnerReferencesRemain, which IS a transition"
    );
}

/// **When the inventory runs, and when it does not.**
#[test]
fn the_inventory_is_due_only_for_the_four_recorded_reasons() {
    let now = utc(2026, 9, 10, 12, 0);
    assert!(
        inventory_due(None, now),
        "no status at all: the upgrade case"
    );

    let no_active: BackupScheduleStatus = serde_json::from_value(serde_json::json!({
        "history": {
            "runCount": 3, "runCountCapped": false, "estimatedBytes": 10,
            "legacyOwnedRuns": 0, "legacyMigratableRuns": 0,
            "ownershipScanComplete": true,
            "inventoriedAt": now
        }
    }))
    .expect("a status");
    assert!(
        inventory_due(Some(&no_active), now),
        "an ABSENT `activeRuns` is `never computed`, not `computed and empty`, so the \
         O(active) branch has nothing to read"
    );

    let fresh: BackupScheduleStatus =
        serde_json::from_value(retained_status(now, 3)).expect("a status");
    assert!(
        !inventory_due(Some(&fresh), now + chrono::Duration::minutes(59)),
        "59 minutes is inside the window"
    );
    assert!(
        inventory_due(Some(&fresh), now + chrono::Duration::minutes(60)),
        "60 minutes is D1 §6.7's interval"
    );

    let mut migrating = retained_status(now, 3);
    migrating["history"]["legacyMigratableRuns"] = serde_json::json!(4);
    let migrating: BackupScheduleStatus = serde_json::from_value(migrating).expect("a status");
    assert!(
        inventory_due(Some(&migrating), now),
        "there is migration work a pass can do NOW, so the next reconcile does it rather \
         than waiting an hour"
    );

    // AND A RUN THAT IS MERELY STILL GOING DOES NOT RE-LIST EVERY 30 s. D1 §6.2:
    // "never retried faster than the inventory interval".
    let mut waiting = retained_status(now, 3);
    waiting["history"]["legacyOwnedRuns"] = serde_json::json!(2);
    let waiting: BackupScheduleStatus = serde_json::from_value(waiting).expect("a status");
    assert!(
        !inventory_due(Some(&waiting), now + chrono::Duration::minutes(5)),
        "the two owned runs are NONTERMINAL; nothing this controller does moves them, so \
         re-listing the namespace every 30 s until a Job finishes buys nothing"
    );
}

/// The label selector is unlocked by the condition and by nothing else.
#[test]
fn the_label_selector_waits_for_the_condition_to_say_true() {
    let now = utc(2026, 9, 10, 12, 0);
    assert!(!may_use_label_selector(None));
    let retained: BackupScheduleStatus =
        serde_json::from_value(retained_status(now, 3)).expect("a status");
    assert!(may_use_label_selector(Some(&retained)));

    let mut not_yet = retained_status(now, 3);
    not_yet["conditions"][0]["status"] = serde_json::json!("False");
    not_yet["conditions"][0]["reason"] = serde_json::json!("LegacyOwnerReferencesRemain");
    let not_yet: BackupScheduleStatus = serde_json::from_value(not_yet).expect("a status");
    assert!(
        !may_use_label_selector(Some(&not_yet)),
        "A LEGACY OBJECT CARRIES NO UID LABEL. Selecting on it while any run might still be \
         legacy would make the migration invisible to its own inventory, and the schedule \
         would report `Retained` forever while its history stayed collectable."
    );
}

// ---------------------------------------------------------------------------
// §12 double rows — the migration against a route table
// ---------------------------------------------------------------------------

/// **§12 PLAT-05.2, "Migration interruption" (double).**
///
/// Every PATCH carries the precondition, and a pass that loses one still
/// migrates the rest — the interruption-safety property, which is that progress
/// is derived from OBSERVATION and never from a cursor.
#[tokio::test]
async fn migration_patches_carry_resource_version_and_resume_after_a_failed_patch() {
    let one = "logweir-backup-hist-20260910-000000";
    let two = "logweir-backup-hist-20260911-000000";
    let three = "logweir-backup-hist-20260912-000000";
    let now = utc(2026, 9, 13, 0, 0);

    let (client, _rec, bodies) = mock_client_recording_bodies(vec![
        list_route(
            vec![
                legacy_backup(one, "hist", UID, "Succeeded"),
                legacy_backup(two, "hist", UID, "Failed"),
                legacy_backup(three, "hist", UID, "Succeeded"),
            ],
            None,
        ),
        // THE MIDDLE ONE FAILS WITH A 500 — a transport-shaped failure, not a
        // refusal. The pass must not stop at it.
        patch_route(two, 500, api_error(500, "InternalError")),
        patch_route(one, 200, legacy_backup(one, "hist", UID, "Succeeded")),
        patch_route(three, 200, legacy_backup(three, "hist", UID, "Succeeded")),
    ]);
    let observed = observe(&backups(&client), None, "hist", UID, None, now)
        .await
        .expect("an inventory is a decision");

    let seen = bodies.lock().expect("readable").clone();
    let patches = backup_patches(&seen);
    assert_eq!(patches.len(), 3, "one PATCH per legacy object: {seen:?}");
    for patch in &patches {
        assert_eq!(
            patch["metadata"]["resourceVersion"],
            serde_json::json!("101"),
            "EVERY migration PATCH is a compare-and-set: {patch}"
        );
    }
    assert_eq!(observed.history.run_count, 3);
    assert_eq!(
        observed.history.legacy_migratable_runs, 1,
        "the one that failed is still owned and still migratable, so the next pass retries \
         it — and the two that succeeded are not counted again"
    );
    assert_eq!(observed.history.legacy_owned_runs, 1);
    assert!(
        observed.history.migration_blocked.is_none(),
        "a 500 is not a refusal: it does not name a run for a human to fix"
    );
    assert!(
        seen.iter().all(|b| b.method != "DELETE"),
        "nothing is ever deleted: {seen:?}"
    );
}

/// **§12 PLAT-05.2, "Migration interruption" (double).** A `409` is a lost race,
/// not a failure: the object is re-read on the next inventory.
#[tokio::test]
async fn a_409_migration_patch_is_retried_next_inventory() {
    let name = "logweir-backup-hist-20260910-000000";
    let now = utc(2026, 9, 11, 0, 0);

    // PASS 1 — the patch loses a 409.
    let (client, _rec, bodies) = mock_client_recording_bodies(vec![
        list_route(vec![legacy_backup(name, "hist", UID, "Succeeded")], None),
        patch_route(name, 409, api_error(409, "Conflict")),
    ]);
    let first = observe(&backups(&client), None, "hist", UID, None, now)
        .await
        .expect("a lost race is a decision");
    assert_eq!(first.history.legacy_migratable_runs, 1);
    assert_eq!(backup_patches(&bodies.lock().expect("readable")).len(), 1);

    // The status the schedule now carries, and the next reconcile's decision.
    let status: BackupScheduleStatus = serde_json::from_value(serde_json::json!({
        "activeRuns": [],
        "history": serde_json::to_value(&first.history).expect("serialises")
    }))
    .expect("a status");
    assert!(
        inventory_due(Some(&status), now + chrono::Duration::seconds(30)),
        "the very next reconcile re-inventories, because there is work it can do"
    );
    assert!(
        !may_use_label_selector(Some(&status)),
        "and it re-lists NAMESPACE-WIDE, because the object it has to find carries no UID \
         label — that is what the failed patch would have given it"
    );

    // PASS 2 — the object comes back with a newer resourceVersion and lands.
    let mut newer = legacy_backup(name, "hist", UID, "Succeeded");
    newer["metadata"]["resourceVersion"] = serde_json::json!("204");
    let (client, _rec, bodies) = mock_client_recording_bodies(vec![
        list_route(vec![newer.clone()], None),
        patch_route(name, 200, newer),
    ]);
    let second = observe(
        &backups(&client),
        Some(&status),
        "hist",
        UID,
        None,
        now + chrono::Duration::seconds(30),
    )
    .await
    .expect("the retry lands");
    assert_eq!(
        second.history.legacy_migratable_runs, 0,
        "and the schedule reaches HistoryRetained=True"
    );
    let patches = backup_patches(&bodies.lock().expect("readable"));
    assert_eq!(
        patches[0]["metadata"]["resourceVersion"],
        serde_json::json!("204"),
        "the RE-READ version, not the one the first pass held. A retry that replayed the \
         stale precondition would 409 forever: {patches:?}"
    );
}

/// **§12 PLAT-05.2, "Migration interruption" (double).**
#[tokio::test]
async fn nonterminal_legacy_runs_keep_their_owner_until_terminal() {
    let running = "logweir-backup-hist-20260910-000000";
    let done = "logweir-backup-hist-20260909-000000";
    let now = utc(2026, 9, 10, 1, 0);

    let (client, _rec, bodies) = mock_client_recording_bodies(vec![
        list_route(
            vec![
                legacy_backup(running, "hist", UID, "Running"),
                legacy_backup(done, "hist", UID, "Succeeded"),
            ],
            None,
        ),
        patch_route(done, 200, legacy_backup(done, "hist", UID, "Succeeded")),
    ]);
    let observed = observe(&backups(&client), None, "hist", UID, None, now)
        .await
        .expect("an inventory is a decision");

    let patches = backup_patches(&bodies.lock().expect("readable"));
    assert_eq!(patches.len(), 1, "only the terminal one is patched");
    let patched: Vec<String> = bodies
        .lock()
        .expect("readable")
        .iter()
        .filter(|b| b.method == "PATCH")
        .map(|b| b.uri.clone())
        .collect();
    assert!(
        patched.iter().all(|uri| !uri.contains(running)),
        "the RUNNING one is never patched: its scheduled identity still derives from the \
         owner UID, so detaching it mid-flight would make the Backup reconciler refuse it \
         `ScheduledIdentityMismatch`: {patched:?}"
    );
    assert_eq!(observed.history.legacy_owned_runs, 1);
    assert_eq!(
        observed.history.legacy_migratable_runs, 0,
        "there is nothing this controller can do until the Job finishes"
    );
    let condition = history_condition(&observed.history, None, Some(4), now);
    assert_eq!(condition.status, "False");
    assert_eq!(
        condition.reason.as_deref(),
        Some("ActiveLegacyRunsOwned"),
        "and the condition says exactly that, rather than blaming a blocked migration"
    );
    assert_eq!(
        observed.active.len(),
        1,
        "the running one is also what repairs `status.activeRuns`"
    );
    assert_eq!(observed.active[0].name, running);
}

/// **§12 PLAT-05.2, "Migration interruption" (double).** A `422` and a `403` are
/// refusals a human has to clear, and the status names the object.
#[tokio::test]
async fn a_422_legacy_object_reports_migration_blocked() {
    let name = "logweir-backup-hist-20260910-000000";
    let now = utc(2026, 9, 11, 0, 0);

    for (code, reason, recorded) in [
        (422u16, "Invalid", "ApiInvalid"),
        (403, "Forbidden", "ApiForbidden"),
    ] {
        let (client, _rec, _bodies) = mock_client_recording_bodies(vec![
            list_route(vec![legacy_backup(name, "hist", UID, "Succeeded")], None),
            patch_route(name, code, api_error(code, reason)),
        ]);
        let observed = observe(&backups(&client), None, "hist", UID, None, now)
            .await
            .unwrap_or_else(|e| panic!("{code}: a refusal is a condition, not an error: {e}"));
        let blocked = observed
            .history
            .migration_blocked
            .as_deref()
            .unwrap_or_default();
        assert_eq!(blocked.len(), 1, "{code}");
        assert_eq!(blocked[0].name, name, "{code}: the status NAMES the object");
        assert_eq!(
            blocked[0].reason, recorded,
            "{code}: ONE vocabulary. The field used to carry the bare digits beside the \
             CamelCase `NoScheduleReference`, so a console could not branch on it."
        );
        assert_eq!(
            observed.history.legacy_migratable_runs, 0,
            "{code}: a blocked run is NOT migratable, which is what keeps the next reconcile \
             from re-listing the namespace every 30 s against an API server that will keep \
             refusing (D1 §6.2: never retried faster than the inventory interval)"
        );
        assert_eq!(observed.history.legacy_owned_runs, 1, "{code}");

        let status: BackupScheduleStatus = serde_json::from_value(serde_json::json!({
            "activeRuns": [],
            "history": serde_json::to_value(&observed.history).expect("serialises")
        }))
        .expect("a status");
        assert!(
            !inventory_due(Some(&status), now + chrono::Duration::minutes(30)),
            "{code}: half an hour later, nothing is re-attempted"
        );
        assert!(
            inventory_due(Some(&status), now + chrono::Duration::minutes(60)),
            "{code}: and the ordinary interval DOES retry it — a 403 cleared by an RBAC fix \
             must not need a controller restart"
        );
        let condition = history_condition(&observed.history, None, Some(4), now);
        assert_eq!(
            condition.reason.as_deref(),
            Some("MigrationBlocked"),
            "{code}"
        );
        assert!(
            condition.message.expect("a message").contains(name),
            "{code}: and the condition names it too"
        );
    }

    // THE SAMPLE IS BOUNDED; THE COUNT IS NOT. `migrationBlocked` names at most
    // ten (D1 §6.2 step 4), but `legacyOwnedRuns` is the TOTAL an operator reads
    // before deciding whether an ordinary `kubectl delete` is safe. Reading it
    // off the truncated sample would report ten owned runs on a schedule with
    // twenty-five — an undercount of exactly the number the decision turns on.
    let names: Vec<String> = (0..25)
        .map(|i| format!("logweir-backup-hist-2026091{}-00000{}", i / 10, i % 10))
        .collect();
    let mut routes = vec![list_route(
        names
            .iter()
            .map(|n| legacy_backup(n, "hist", UID, "Succeeded"))
            .collect(),
        None,
    )];
    routes.extend(
        names
            .iter()
            .map(|n| patch_route(n, 422, api_error(422, "Invalid"))),
    );
    let (client, _rec, _bodies) = mock_client_recording_bodies(routes);
    let observed = observe(&backups(&client), None, "hist", UID, None, now)
        .await
        .expect("twenty-five refusals are a condition, not an error");
    assert_eq!(
        observed
            .history
            .migration_blocked
            .as_deref()
            .unwrap_or_default()
            .len(),
        MIGRATION_BLOCKED_SAMPLE,
        "the SAMPLE is ten"
    );
    assert_eq!(
        observed.history.legacy_owned_runs, 25,
        "and the COUNT is all of them"
    );
    assert_eq!(
        observed.history.legacy_migratable_runs, 0,
        "none of which the next pass may retry faster than the inventory interval"
    );
}

/// **§12 PLAT-05.2, "Unrelated-resource preservation" (double).**
#[tokio::test]
async fn migration_never_patches_backups_of_other_schedules_or_manual_runs() {
    let mine = "logweir-backup-hist-20260910-000000";
    let theirs = "logweir-backup-other-20260910-000000";
    let manual = "logweir-manual-abcdefghijklmnopqrstuvwxyz";
    let now = utc(2026, 9, 11, 0, 0);

    let (client, _rec, bodies) = mock_client_recording_bodies(vec![
        list_route(
            vec![
                legacy_backup(mine, "hist", UID, "Succeeded"),
                legacy_backup(theirs, "other", OTHER_UID, "Succeeded"),
                manual_backup(manual, "hist", UID, "Succeeded"),
            ],
            None,
        ),
        patch_route(mine, 200, legacy_backup(mine, "hist", UID, "Succeeded")),
    ]);
    let observed = observe(&backups(&client), None, "hist", UID, None, now)
        .await
        .expect("an inventory is a decision");

    let seen = bodies.lock().expect("readable").clone();
    let patched: Vec<String> = seen
        .iter()
        .filter(|b| b.method == "PATCH")
        .map(|b| b.uri.clone())
        .collect();
    assert_eq!(
        patched.len(),
        1,
        "exactly one object is touched: {patched:?}"
    );
    assert!(patched[0].contains(mine));
    assert!(
        seen.iter().all(|b| b.method != "DELETE"),
        "and nothing is deleted: {seen:?}"
    );

    // AND THE MANUAL RUN IS HISTORY (D1 §3.1's membership note): it is counted,
    // it has no owner entry to remove, and it is not something to migrate.
    assert_eq!(
        observed.history.run_count, 2,
        "MY schedule's two runs — the scheduled one and the MANUAL one, which is part of this \
         schedule's history even though it never counted for concurrency. The other \
         schedule's run is not mine and is not counted."
    );
    assert_eq!(observed.history.legacy_owned_runs, 0);
}

/// **§12 PLAT-05.2, "Same-name recreation" (double).**
///
/// A recreated `hist` has a new UID, and D1 §6.4's consequence is that it sees
/// none of the old runs as its own.
#[tokio::test]
async fn a_recreated_schedule_does_not_count_old_uid_runs() {
    let old_run = "logweir-backup-hist-20260910-000000";
    let old_running = "logweir-backup-hist-20260911-000000";
    let new_run = "logweir-backup-hist-20260912-000000";
    let now = utc(2026, 9, 12, 1, 0);

    let page = vec![
        // The previous generation's history, already migrated: same NAME, other
        // UID.
        {
            let mut b = retained_backup(old_run, "hist", OTHER_UID, "Succeeded");
            b["metadata"]["annotations"] =
                serde_json::json!({ RETAINED_FROM_OWNER_ANNOTATION: OTHER_UID });
            b
        },
        // And one of them is STILL RUNNING when the schedule is recreated.
        retained_backup(old_running, "hist", OTHER_UID, "Running"),
        retained_backup(new_run, "hist", UID, "Running"),
    ];
    let (client, _rec, bodies) = mock_client_recording_bodies(vec![list_route(page, None)]);
    let observed = observe(&backups(&client), None, "hist", UID, None, now)
        .await
        .expect("an inventory is a decision");

    assert_eq!(
        observed.history.run_count, 1,
        "`status.history.runCount` counts only the NEW generation's runs. History queries \
         are by UID, so a recreated schedule inherits nothing — D1 §6.4."
    );
    assert_eq!(
        observed
            .active
            .iter()
            .map(|r| r.name.as_str())
            .collect::<Vec<_>>(),
        vec![new_run],
        "and an OLD run that is still running does not appear in the new `activeRuns` and \
         therefore does not block the new schedule's admissions"
    );
    assert!(
        bodies
            .lock()
            .expect("readable")
            .iter()
            .all(|b| b.method == "GET"),
        "the old generation's objects are READ and nothing else: not patched, not adopted, \
         not deleted"
    );
}

/// **§12 PLAT-05.2, "Same-name recreation" (double).**
///
/// A slot whose deterministic name is held by the PREVIOUS generation is
/// recorded and skipped, never re-run under a different name.
#[tokio::test]
async fn a_held_deterministic_name_is_recorded_as_slot_name_unavailable() {
    let fire = utc(2026, 9, 10, 0, 0);
    let slot = slot_name(fire);
    let name = scheduled_backup_name("hist", &slot).expect("the fixture name fits");
    let schedule = schedule_with("hist", UID, serde_json::json!({}));

    // The object on the name belongs to the PREVIOUS generation of `hist`.
    let squatter = retained_backup(&name, "hist", OTHER_UID, "Running");
    let (client, _rec, bodies) = mock_client_recording_bodies(vec![
        list_route(vec![squatter.clone()], None),
        Route {
            method: "GET",
            path_suffix: Box::leak(format!("/backups/{name}").into_boxed_str()),
            status: 200,
            body: squatter.to_string(),
        },
        Route {
            method: "PATCH",
            path_suffix: "/backupschedules/hist/status",
            status: 200,
            body: schedule_json("hist", UID, serde_json::json!({})).to_string(),
        },
    ]);
    let outcome = reconcile_schedule(&schedule, &client, fire)
        .await
        .expect("a held name is a decision, not an error");
    assert_eq!(outcome.decision.reason(), "SlotNameUnavailable");
    assert_eq!(outcome.created, None);
    let seen = bodies.lock().expect("readable").clone();
    assert!(
        seen.iter().all(|b| b.method != "POST"),
        "nothing is created under ANOTHER name: an object name that is a pure function of \
         the trigger is what makes a duplicate reconcile a 409 instead of a second, partial \
         archive: {seen:?}"
    );
}

// ---------------------------------------------------------------------------
// §12 scale rows, and the read cost §6.7 exists to bound
// ---------------------------------------------------------------------------

/// **§12 PLAT-05.2, "Scale" (double).** Steady state is O(active), not
/// O(history).
#[tokio::test]
async fn steady_state_reconcile_makes_no_list_request() {
    let now = utc(2026, 9, 10, 12, 0);
    let running = "logweir-backup-hist-20260910-000000";
    let mut status = retained_status(now - chrono::Duration::minutes(10), 500);
    status["activeRuns"] =
        serde_json::json!([{ "name": running, "kind": "Scheduled", "attempt": 0 }]);
    let status: BackupScheduleStatus = serde_json::from_value(status).expect("a status");

    // NO LIST ROUTE AT ALL. The double panics on a request it has no route for,
    // so a reconcile that listed would fail loudly rather than quietly costing
    // O(history) — which is the defect D1 §6.7 exists to remove.
    let (client, _rec, bodies) = mock_client_recording_bodies(vec![Route {
        method: "GET",
        path_suffix: Box::leak(format!("/backups/{running}").into_boxed_str()),
        status: 200,
        body: retained_backup(running, "hist", UID, "Running").to_string(),
    }]);
    let observed = observe(&backups(&client), Some(&status), "hist", UID, None, now)
        .await
        .expect("a steady pass is a decision");

    assert!(!observed.inventoried);
    assert_eq!(
        calls(&bodies.lock().expect("readable")),
        vec![(
            "GET".to_string(),
            format!("/apis/logweir.dev/v1alpha1/namespaces/{NS}/backups/{running}")
        )],
        "ONE GET, for the one name the status records. A schedule with 500 runs of history \
         costs exactly what a schedule with none does."
    );
    assert_eq!(
        observed.history.run_count, 500,
        "and the history block is CARRIED, not recomputed: a pass that observed nothing new \
         must write back what the last inventory found, or `status_unchanged` would be false \
         on every steady pass"
    );
}

/// **§12 PLAT-05.2, "Scale" (double).** The inventory is paginated, `limit`ed,
/// and label-selected once the condition allows it.
#[tokio::test]
async fn inventory_is_paginated_and_label_selected_after_migration() {
    let now = utc(2026, 9, 10, 12, 0);
    let status: BackupScheduleStatus =
        serde_json::from_value(retained_status(now - chrono::Duration::hours(2), 3))
            .expect("a status");

    // THE ROUTE ANSWERS EVERY PAGE WITH THE SAME `continue` TOKEN, so the only
    // thing that can stop this loop is the controller's own page cap. A double
    // that answered one page could not tell a bounded walk from an unbounded
    // one.
    let (client, _rec, bodies) = mock_client_recording_bodies(vec![list_route(
        vec![retained_backup(
            "logweir-backup-hist-20260910-000000",
            "hist",
            UID,
            "Succeeded",
        )],
        Some("next-page"),
    )]);
    let observed = observe(&backups(&client), Some(&status), "hist", UID, None, now)
        .await
        .expect("an inventory is a decision");

    let seen = bodies.lock().expect("readable").clone();
    assert_eq!(
        seen.len(),
        MAX_INVENTORY_PAGES,
        "the walk stops at its page cap instead of following `continue` forever: {seen:?}"
    );
    for (i, request) in seen.iter().enumerate() {
        assert!(
            request
                .uri
                .contains(&format!("limit={INVENTORY_PAGE_SIZE}")),
            "page {i} carries a `limit`. A LIST without one streams every matching object \
             into one response body: {}",
            request.uri
        );
        assert!(
            request
                .uri
                .contains("labelSelector=logweir.dev%2Fschedule-uid%3D"),
            "page {i} is narrowed to this schedule's UID, because the condition already says \
             `HistoryRetained=True` and every remaining run therefore carries the label: {}",
            request.uri
        );
        if i > 0 {
            assert!(
                request.uri.contains("continue=next-page"),
                "page {i} resumes from the token the previous page returned: {}",
                request.uri
            );
        }
    }
    assert!(
        observed.history.run_count_capped,
        "and the count it reports is a FLOOR, said so rather than passed off as a total"
    );
    let condition = history_condition(&observed.history, None, Some(4), now);
    assert_eq!(condition.status, "True");
    assert_eq!(
        condition.reason.as_deref(),
        Some("HistoryLarge"),
        "20 pages of 500 is past D1 §6.6's 2,000-run advisory, so the condition carries the \
         warning while still saying the history IS retained"
    );
}

/// **The brief's row: the inventory never issues an unbounded LIST.** Before
/// the condition says `True` the list is namespace-wide — it has to be, because
/// a legacy object carries no UID label — but it is still `limit`ed and still
/// bounded.
#[tokio::test]
async fn the_first_inventory_is_namespace_wide_but_never_unbounded() {
    let now = utc(2026, 9, 10, 12, 0);
    let (client, _rec, bodies) = mock_client_recording_bodies(vec![list_route(
        vec![legacy_backup(
            "logweir-backup-hist-20260909-000000",
            "hist",
            UID,
            "Running",
        )],
        None,
    )]);
    observe(&backups(&client), None, "hist", UID, None, now)
        .await
        .expect("an inventory is a decision");

    let seen = bodies.lock().expect("readable").clone();
    assert_eq!(
        seen.len(),
        1,
        "one page, because one page was all there was"
    );
    assert!(
        seen[0]
            .uri
            .contains(&format!("limit={INVENTORY_PAGE_SIZE}")),
        "still `limit`ed: {}",
        seen[0].uri
    );
    assert!(
        !seen[0].uri.contains("labelSelector"),
        "and NOT label-selected: the object it has to find is exactly the one that carries \
         no `logweir.dev/schedule-uid` label. Selecting on it here is how a migration comes \
         to be invisible to its own inventory: {}",
        seen[0].uri
    );
}

// ---------------------------------------------------------------------------
// The brief's remaining rows
// ---------------------------------------------------------------------------

/// **The brief's row: the migration is idempotent across two passes.**
#[tokio::test]
async fn a_second_pass_over_migrated_history_patches_nothing() {
    let name = "logweir-backup-hist-20260910-000000";
    let now = utc(2026, 9, 11, 0, 0);

    // PASS 1 — one legacy object, one patch.
    let (client, _rec, bodies) = mock_client_recording_bodies(vec![
        list_route(vec![legacy_backup(name, "hist", UID, "Succeeded")], None),
        patch_route(name, 200, legacy_backup(name, "hist", UID, "Succeeded")),
    ]);
    let first = observe(&backups(&client), None, "hist", UID, None, now)
        .await
        .expect("an inventory is a decision");
    assert_eq!(backup_patches(&bodies.lock().expect("readable")).len(), 1);
    assert_eq!(first.history.legacy_owned_runs, 0);

    // The object as the API server now holds it, and the status as the schedule
    // now carries it.
    let mut migrated = legacy_backup(name, "hist", UID, "Succeeded");
    let patch = migration_patch(&typed(&migrated), "hist", UID).expect("the entry was there");
    weirkeeper::conditions::apply_merge_patch(&mut migrated, &patch);
    let status: BackupScheduleStatus = serde_json::from_value(serde_json::json!({
        "activeRuns": [],
        "history": serde_json::to_value(&first.history).expect("serialises"),
        "conditions": [serde_json::to_value(history_condition(&first.history, None, Some(4), now))
            .expect("serialises")]
    }))
    .expect("a status");

    // PASS 2 — an hour later. THE ROUTE TABLE HAS NO PATCH ROUTE, so a second
    // patch would fail loudly. Idempotence is not "the same patch twice is
    // harmless"; it is "there is no second patch".
    let (client, _rec, bodies) =
        mock_client_recording_bodies(vec![list_route(vec![migrated], None)]);
    let second = observe(
        &backups(&client),
        Some(&status),
        "hist",
        UID,
        None,
        now + chrono::Duration::hours(1),
    )
    .await
    .expect("a second inventory is a decision");

    let seen = bodies.lock().expect("readable").clone();
    assert!(
        seen.iter().all(|b| b.method == "GET"),
        "the second pass READS and writes nothing: {seen:?}"
    );
    assert_eq!(second.history.run_count, 1, "and still sees the run");
    assert_eq!(second.history.legacy_owned_runs, 0);
    assert_eq!(
        history_condition(&second.history, None, Some(4), now)
            .reason
            .as_deref(),
        Some("Retained")
    );
}

/// **The brief's row: resumable after a crash mid-list.**
///
/// A pass that died after patching the first page is indistinguishable, to the
/// pass that follows it, from one that never ran — because progress is
/// OBSERVED, not recorded. The second pass sees a half-migrated namespace and
/// finishes it, patching each object exactly once in its whole life.
#[tokio::test]
async fn a_crash_mid_list_leaves_every_object_migrated_or_untouched() {
    let done = "logweir-backup-hist-20260910-000000";
    let not_yet = "logweir-backup-hist-20260911-000000";
    let now = utc(2026, 9, 12, 0, 0);

    // What the first pass got as far as: `done` is migrated, `not_yet` is not.
    let mut migrated = legacy_backup(done, "hist", UID, "Succeeded");
    let patch = migration_patch(&typed(&migrated), "hist", UID).expect("the entry was there");
    weirkeeper::conditions::apply_merge_patch(&mut migrated, &patch);

    // THE RESTARTED PROCESS HAS NO STATUS AT ALL — the crash lost the status
    // write too, which is the worst case and the one a cursor-based design
    // cannot recover from. The route table offers a PATCH for `not_yet` only,
    // so a pass that re-patched `done` would fail loudly.
    let (client, _rec, bodies) = mock_client_recording_bodies(vec![
        list_route(
            vec![
                migrated.clone(),
                legacy_backup(not_yet, "hist", UID, "Succeeded"),
            ],
            None,
        ),
        patch_route(
            not_yet,
            200,
            legacy_backup(not_yet, "hist", UID, "Succeeded"),
        ),
    ]);
    let observed = observe(&backups(&client), None, "hist", UID, None, now)
        .await
        .expect("a restart re-inventories");

    let patched: Vec<String> = bodies
        .lock()
        .expect("readable")
        .iter()
        .filter(|b| b.method == "PATCH")
        .map(|b| b.uri.clone())
        .collect();
    assert_eq!(patched.len(), 1, "exactly one object still needed it");
    assert!(patched[0].contains(not_yet));
    assert_eq!(
        observed.history.run_count, 2,
        "and BOTH are still history — the migrated one through membership rule 3"
    );
    assert_eq!(observed.history.legacy_owned_runs, 0);
}

/// **The brief's row: a manual run of the schedule survives deletion and is
/// inventoried as history.**
///
/// D1 §3.1's membership note and §8.3: a manual run IS the schedule's history
/// and is NEVER its concurrency.
#[tokio::test]
async fn a_manual_run_is_history_and_never_concurrency() {
    let now = utc(2026, 9, 10, 12, 0);
    let manual = "logweir-manual-abcdefghijklmnopqrstuvwxyz";
    let scheduled = "logweir-backup-hist-20260910-000000";

    let (client, _rec, _bodies) = mock_client_recording_bodies(vec![list_route(
        vec![
            manual_backup(manual, "hist", UID, "Running"),
            retained_backup(scheduled, "hist", UID, "Succeeded"),
        ],
        None,
    )]);
    let observed = observe(&backups(&client), None, "hist", UID, None, now)
        .await
        .expect("an inventory is a decision");

    assert_eq!(
        observed.history.run_count, 2,
        "BOTH runs are this schedule's history. `identity::is_run_of_schedule` is the \
         membership question and a manual run of a schedule answers it yes."
    );
    assert!(
        observed.active.is_empty(),
        "and NEITHER is in `activeRuns`: the manual one is running but does not participate \
         in `concurrencyPolicy` (D1 §8.3, the CronJob precedent), and the scheduled one is \
         terminal. A manual run that occupied a `Forbid` slot would stop the schedule."
    );
    assert!(
        observed.history.estimated_bytes > 0,
        "and D1 §6.6's cost report counts it: an operator deciding whether to prune needs \
         the whole figure"
    );
}

/// **The hourly inventory is the one thing that moves a settled status**, and it
/// is 24 writes a day rather than the 2,880 the no-write-when-nothing-changed
/// rule exists to prevent.
///
/// This is the cost PLAT-05.2 adds, measured rather than assumed — and the
/// reason `schedule_controller.rs`'s and `retention.rs`'s steady-state rows now
/// take their second pass inside the inventory window.
#[tokio::test]
async fn the_hourly_inventory_is_the_only_thing_that_moves_a_settled_status() {
    let fire = utc(2026, 9, 10, 0, 0);
    let slot = slot_name(fire);
    let name = scheduled_backup_name("hist", &slot).expect("the fixture name fits");
    let run = retained_backup(&name, "hist", UID, "Succeeded");

    let routes = || {
        vec![
            list_route(vec![run.clone()], None),
            Route {
                method: "GET",
                path_suffix: Box::leak(format!("/backups/{name}").into_boxed_str()),
                status: 200,
                body: run.to_string(),
            },
            Route {
                method: "PATCH",
                path_suffix: "/backupschedules/hist/status",
                status: 200,
                body: schedule_json("hist", UID, serde_json::json!({})).to_string(),
            },
        ]
    };
    let writes = |seen: &[SeenBody]| seen.iter().filter(|b| b.method != "GET").count();

    // PASS 1 — the first reconcile inventories, because `status.history` is
    // absent, and writes.
    let schedule = schedule_with("hist", UID, serde_json::json!({}));
    let (client, _rec, bodies) = mock_client_recording_bodies(routes());
    reconcile_schedule(&schedule, &client, fire + chrono::Duration::hours(1))
        .await
        .expect("the first reconcile completes");
    let first = bodies.lock().expect("readable").clone();
    assert_eq!(writes(&first), 1, "the first pass writes once: {first:?}");
    let patched: serde_json::Value = serde_json::from_str(
        &first
            .iter()
            .find(|b| b.method == "PATCH")
            .expect("a status patch")
            .body,
    )
    .expect("the patch is JSON");
    assert!(
        patched["status"]["history"]["inventoriedAt"].is_string(),
        "and it records WHEN it inventoried, which is what the next pass reads: {patched}"
    );
    // AND THE CONDITION IS COMPUTED, NOT COPIED. There was no stored
    // `HistoryRetained` to copy on this pass, so a builder that carried one
    // forward — W2's documented no-op, which this task replaces — would emit
    // `Ready` alone and leave every reader unable to tell whether deleting the
    // schedule is safe.
    let conditions = patched["status"]["conditions"]
        .as_array()
        .expect("the status carries conditions");
    let retained = conditions
        .iter()
        .find(|c| c["type"] == serde_json::json!(CONDITION_HISTORY_RETAINED))
        .unwrap_or_else(|| panic!("the first status write computes HistoryRetained: {patched}"));
    assert_eq!(retained["status"], serde_json::json!("True"));
    assert_eq!(retained["reason"], serde_json::json!("Retained"));
    assert!(
        conditions
            .iter()
            .any(|c| c["type"] == serde_json::json!("Ready")),
        "and `Ready` is still there — a merge patch REPLACES `status.conditions`, so a          builder that emitted only the condition it owns deletes the other: {patched}"
    );

    let settled = schedule_with("hist", UID, patched["status"].clone());

    // PASS 2 — half an hour later, inside the window. NOTHING is written.
    let (client, _rec, bodies) = mock_client_recording_bodies(routes());
    reconcile_schedule(
        &settled,
        &client,
        fire + chrono::Duration::hours(1) + chrono::Duration::minutes(30),
    )
    .await
    .expect("the steady reconcile completes");
    let second = bodies.lock().expect("readable").clone();
    assert_eq!(
        writes(&second),
        0,
        "inside the inventory window a settled schedule writes NOTHING: {second:?}"
    );
    assert!(
        second.iter().all(|b| !b.uri.ends_with("/backups")),
        "and it does not LIST: {second:?}"
    );

    // PASS 3 — an hour after pass 1. ONE write, because a real inventory
    // happened and `inventoriedAt` is a real fact about it.
    let (client, _rec, bodies) = mock_client_recording_bodies(routes());
    reconcile_schedule(&settled, &client, fire + chrono::Duration::hours(2))
        .await
        .expect("the hourly reconcile completes");
    let third = bodies.lock().expect("readable").clone();
    assert_eq!(
        writes(&third),
        1,
        "once an hour, and once an hour only — 24 writes a day per schedule, against the \
         2,880 a rewrite-on-every-pass would make: {third:?}"
    );
}

/// **§12 PLAT-05.2, "Delete with active/completed runs" (double).**
///
/// The object this controller POSTs carries no ownerReference to its schedule,
/// so the API server has nothing to garbage-collect when the schedule goes.
#[test]
fn a_new_scheduled_backup_carries_no_schedule_owner_reference() {
    let fire = utc(2026, 9, 10, 0, 0);
    let slot = slot_name(fire);
    let name = scheduled_backup_name("hist", &slot).expect("the fixture name fits");
    let schedule = schedule_with("hist", UID, serde_json::json!({}));
    let backup = scheduled_backup(&schedule, UID, &slot, &name);

    assert_eq!(
        backup
            .metadata
            .owner_references
            .as_deref()
            .unwrap_or_default(),
        &[],
        "D1 §6.1. This is the whole of PLAT-05.2's write half: a run is not its schedule's \
         dependent, so `kubectl delete backupschedule hist` — with ANY propagation policy — \
         leaves the run, its immutable plan ConfigMap and its Job in place."
    );
    assert_eq!(
        backup
            .spec
            .schedule_ref
            .as_ref()
            .and_then(|r| r.uid.as_deref()),
        Some(UID),
        "membership moved onto the SPEC, which deletion of the owner cannot reach"
    );
    assert!(
        is_run_of_schedule(&backup, "hist", UID),
        "and it answers the membership question without any help from an owner entry"
    );
    let labels = backup.metadata.labels.clone().expect("labels");
    assert_eq!(
        labels[SCHEDULE_UID_LABEL], UID,
        "which is what the §6.7 inventory selects on"
    );
}

/// **§12 PLAT-05.2, "Unrelated-resource preservation" (double), the extended
/// half.** The reconciler patches status and the migration metadata, and
/// nothing else, ever.
///
/// The route table below has NO DELETE route and no route for any object this
/// reconciler does not name, so the double panics on either — see
/// `src/testing.rs` for why a panic and not a 404.
#[tokio::test]
async fn the_reconciler_patches_only_status_and_the_migration_metadata() {
    let fire = utc(2026, 9, 10, 0, 0);
    let slot = slot_name(fire);
    let name = scheduled_backup_name("hist", &slot).expect("the fixture name fits");
    let old = "logweir-backup-hist-20260909-000000";
    let schedule = schedule_with("hist", UID, serde_json::json!({}));

    let (client, _rec, bodies) = mock_client_recording_bodies(vec![
        list_route(vec![legacy_backup(old, "hist", UID, "Succeeded")], None),
        patch_route(old, 200, legacy_backup(old, "hist", UID, "Succeeded")),
        Route {
            method: "GET",
            path_suffix: Box::leak(format!("/backups/{name}").into_boxed_str()),
            status: 404,
            body: serde_json::json!({
                "apiVersion": "v1", "kind": "Status", "status": "Failure",
                "code": 404, "reason": "NotFound", "message": "not found"
            })
            .to_string(),
        },
        Route {
            method: "POST",
            path_suffix: "/namespaces/logweir-t18/backups",
            status: 201,
            body: serde_json::json!({
                "apiVersion": "logweir.dev/v1alpha1", "kind": "Backup",
                "metadata": { "name": name, "namespace": NS, "uid": "created" },
                "spec": {
                    "sourceRef": { "name": "prod" },
                    "topics": ["orders", "payments"],
                    "archive": { "url": "s3://kafka-backups/logweir" },
                    "scheduleRef": { "name": "hist", "uid": UID },
                    "slot": slot,
                    "triggeredBy": "schedule",
                    "trigger": { "kind": "Scheduled", "attempt": 0 },
                    "deadlineSeconds": 3600
                }
            })
            .to_string(),
        },
        Route {
            method: "PATCH",
            path_suffix: "/backupschedules/hist/status",
            status: 200,
            body: {
                let mut v = schedule_json("hist", UID, serde_json::json!({}));
                v["status"]["pendingRun"] = serde_json::json!({
                    "name": name, "slot": slot, "attempt": 0,
                    "kind": "Scheduled", "generation": 4
                });
                v["status"]["pendingBackupRef"] = serde_json::json!({ "name": name });
                v.to_string()
            },
        },
    ]);
    reconcile_schedule(&schedule, &client, fire)
        .await
        .expect("the slot fires");

    let seen = bodies.lock().expect("readable").clone();
    for request in &seen {
        assert_ne!(request.method, "DELETE", "never, on anything: {seen:?}");
        assert_ne!(request.method, "PUT", "and no `replace`: {seen:?}");
    }
    // The ONE patch that is not a `/status` write, and everything it may touch.
    let metadata_patches = backup_patches(&seen);
    assert_eq!(metadata_patches.len(), 1, "{seen:?}");
    assert_eq!(
        metadata_patches[0]
            .as_object()
            .map(|m| m.keys().cloned().collect::<Vec<_>>()),
        Some(vec!["metadata".to_string()]),
        "a migration patch is metadata and nothing else. It cannot reach the sealed `spec` \
         and it cannot reach a `status`, which is a subresource with its own RBAC rule: {:?}",
        metadata_patches[0]
    );
    assert!(
        calls(&seen)
            .iter()
            .any(|(method, path)| method == "POST" && path.ends_with("/backups")),
        "and the slot still fires while the migration runs — a schedule that stopped \
         admitting during its upgrade would be an outage: {seen:?}"
    );
}

// ---------------------------------------------------------------------------
// Fix round 1 — the review's findings, each with the row that would have caught it
// ---------------------------------------------------------------------------

/// **H-1 / L-1.** A walk that did not finish may not claim `Retained`, and may
/// not narrow the next walk to a label the objects it is hunting do not carry.
///
/// # The latch, and why this needs its own row
///
/// The only capped-walk row in this file before fix round 1
/// (`inventory_is_paginated_and_label_selected_after_migration`) is
/// **label-selected**, and a capped label-selected walk is fine: every object
/// past its bound carries this schedule's UID label, so none of them can be a
/// pre-upgrade run. The dangerous walk is the **namespace-wide** one, and
/// nothing exercised it. The reviewer's mutant — report `runCountCapped` as
/// `capped = label_selected` — survived all 23 rows for exactly that reason.
///
/// What the latch did: a capped namespace-wide walk set `runCountCapped`, which
/// the `HistoryLarge` arm turned into `HistoryRetained=True`, which unlocked the
/// `logweir.dev/schedule-uid` selector — a label no unmigrated legacy object
/// carries. Every later walk then looked only where the answer could not be,
/// the condition stayed `True` forever, and D1 §6.9's upgrade gate ("wait for
/// `HistoryRetained=True`") passed on a schedule whose history a
/// default-propagation `kubectl delete` would still collect. D1 §6.6's own
/// worst-case schedule — 35 040 runs a year — reaches it.
#[tokio::test]
async fn a_capped_namespace_wide_walk_never_claims_retained_and_never_latches_the_selector() {
    let now = utc(2026, 9, 10, 12, 0);

    // The first inventory: no stored status, so the walk is namespace-wide. The
    // double answers every page with the same `continue` token, so it is
    // capped. NOTHING IT SAW IS LEGACY-OWNED — the legacy runs are past the
    // bound, which is the whole scenario.
    let (client, _rec, bodies) = mock_client_recording_bodies(vec![list_route(
        vec![retained_backup(
            "logweir-backup-hist-20260910-000000",
            "hist",
            UID,
            "Succeeded",
        )],
        Some("next-page"),
    )]);
    let observed = observe(&backups(&client), None, "hist", UID, None, now)
        .await
        .expect("an inventory is a decision");

    let seen = bodies.lock().expect("readable").clone();
    assert!(
        !seen[0].uri.contains("labelSelector"),
        "premise: the first walk is namespace-wide, because a legacy object carries no UID \
         label: {}",
        seen[0].uri
    );
    assert!(observed.history.run_count_capped, "premise: it is capped");
    assert_eq!(
        observed.history.legacy_owned_runs, 0,
        "premise: it saw no legacy owner — the ones that exist are past the bound"
    );

    assert!(
        !observed.history.ownership_scan_complete,
        "THE FIX. A namespace-wide walk that stopped at a bound has not established that no \
         run is owned by this schedule, and the status says so in a field a console can read."
    );

    let condition = history_condition(&observed.history, None, Some(4), now);
    assert_eq!(
        condition.status, "False",
        "so the condition must NOT claim the history is retained: {condition:?}"
    );
    assert_eq!(
        condition.reason.as_deref(),
        Some("MigrationBlocked"),
        "reported through §3.4's existing closed reason set — the machine-readable `why` is \
         `status.history.ownershipScanComplete`, not a sixth condition reason"
    );
    let message = condition.message.clone().expect("a message");
    assert!(
        message.contains("--cascade=orphan"),
        "and it names the spelling that is always safe: {message}"
    );
    assert!(
        message.contains("ownershipScanComplete"),
        "and the field to look at: {message}"
    );

    // THE LATCH ITSELF. This is the assertion the reviewer's reproduction made
    // in the opposite direction, and it is the one that makes the defect
    // permanent rather than merely wrong for one pass.
    let next: BackupScheduleStatus = serde_json::from_value(serde_json::json!({
        "activeRuns": [],
        "history": serde_json::to_value(&observed.history).expect("serialises"),
        "conditions": [serde_json::to_value(&condition).expect("serialises")],
    }))
    .expect("a status");
    assert!(
        !may_use_label_selector(Some(&next)),
        "the next walk stays NAMESPACE-WIDE. Narrowing here is irreversible in practice: a \
         selector that excludes the objects the migration is looking for guarantees the next \
         walk finds none, which keeps the condition True, which keeps the selector on."
    );

    // AND BOTH HALVES OF THE GATE ARE REQUIRED. A stored `True` beside an
    // incomplete scan must still not unlock it — that is precisely the state
    // the old code produced and would produce again if either half were
    // dropped.
    let mut forged = serde_json::to_value(&next).expect("serialises");
    forged["conditions"][0]["status"] = serde_json::json!("True");
    forged["conditions"][0]["reason"] = serde_json::json!("HistoryLarge");
    let forged: BackupScheduleStatus = serde_json::from_value(forged).expect("a status");
    assert!(
        !may_use_label_selector(Some(&forged)),
        "`ownershipScanComplete` alone vetoes it"
    );

    // The complete-walk case still unlocks, or the selector would never be used
    // at all and §6.7's whole point would be lost.
    let mut complete = serde_json::to_value(&forged).expect("serialises");
    complete["history"]["ownershipScanComplete"] = serde_json::json!(true);
    let complete: BackupScheduleStatus = serde_json::from_value(complete).expect("a status");
    assert!(
        may_use_label_selector(Some(&complete)),
        "a walk that finished AND concluded True is what the selector waits for"
    );
}

/// **M-1.** A migration that fits in one budget finishes in one pass, and
/// nothing re-arms after it — against one full namespace-wide walk every thirty
/// seconds until it finished.
///
/// This row covers the single-pass case. The multi-pass cost, which is
/// quadratic in the page count and which no route-table row can see, is
/// `a_multi_page_migration_costs_a_quadratic_number_of_lists`.
///
/// # What it cost when it was wrong
///
/// The patches used to be sent after the WHOLE walk, under a per-pass budget,
/// with `inventory_due` re-arming while work remained. So each pass listed the
/// whole namespace and then patched at most two hundred objects, and the next
/// pass thirty seconds later listed it all over again. Migrating 10 000 legacy
/// runs was fifty passes × twenty pages × five hundred objects — about
/// **1 000 LISTs and 500 000 object reads**, the exact read cost D1 §6.7 exists
/// to remove, re-incurred at its maximum for the whole upgrade window, and
/// satisfying L-05.2-6's "zero LIST requests except inventories at the
/// documented interval" only by calling every one of them an inventory.
///
/// Patching inside the page loop means a pass migrates what it sees as it goes,
/// so a migration that fits inside one budget is DONE after one pass and the
/// 30 s re-arm stops immediately.
#[tokio::test]
async fn the_migration_window_is_bounded_in_lists_not_in_passes() {
    let now = utc(2026, 9, 12, 0, 0);
    let names: Vec<String> = (0..12)
        .map(|i| format!("logweir-backup-hist-202609{:02}-000000", i + 1))
        .collect();
    let routes = || {
        let mut routes = vec![list_route(
            names
                .iter()
                .map(|n| legacy_backup(n, "hist", UID, "Succeeded"))
                .collect(),
            None,
        )];
        routes.extend(
            names
                .iter()
                .map(|n| patch_route(n, 200, legacy_backup(n, "hist", UID, "Succeeded"))),
        );
        routes
    };
    let lists = |seen: &[SeenBody]| {
        calls(seen)
            .into_iter()
            .filter(|(method, path)| method == "GET" && path.ends_with("/backups"))
            .count()
    };

    // PASS 1 — one LIST, and every candidate on the page is detached in the
    // same pass rather than two hundred of them.
    let (client, _rec, bodies) = mock_client_recording_bodies(routes());
    let first = observe(&backups(&client), None, "hist", UID, None, now)
        .await
        .expect("a migrating pass is a decision");
    let seen = bodies.lock().expect("readable").clone();
    assert_eq!(lists(&seen), 1, "{seen:?}");
    assert_eq!(backup_patches(&seen).len(), 12, "all twelve, in one pass");
    assert_eq!(
        first.history.legacy_owned_runs, 0,
        "so the migration is FINISHED after one pass"
    );
    assert!(first.history.ownership_scan_complete);

    // PASS 2, thirty seconds later — and the whole point: ZERO lists, because
    // there is nothing left to re-arm on. The old shape took a full
    // namespace-wide walk here, and again every thirty seconds after it.
    let migrated: Vec<serde_json::Value> = names
        .iter()
        .map(|n| {
            let mut object = legacy_backup(n, "hist", UID, "Succeeded");
            let patch = migration_patch(&typed(&object), "hist", UID).expect("the entry was there");
            weirkeeper::conditions::apply_merge_patch(&mut object, &patch);
            object
        })
        .collect();
    let condition = history_condition(&first.history, None, Some(4), now);
    let status: BackupScheduleStatus = serde_json::from_value(serde_json::json!({
        "activeRuns": [],
        "history": serde_json::to_value(&first.history).expect("serialises"),
        "conditions": [serde_json::to_value(&condition).expect("serialises")],
    }))
    .expect("a status");
    assert_eq!(condition.status, "True");
    assert!(
        !inventory_due(Some(&status), now + chrono::Duration::seconds(30)),
        "nothing re-arms, so the next reconcile takes the O(active) branch"
    );

    // And when the hourly one does come round, it is ONE list and no patch.
    let (client, _rec, bodies) = mock_client_recording_bodies(vec![list_route(migrated, None)]);
    observe(
        &backups(&client),
        Some(&status),
        "hist",
        UID,
        None,
        now + chrono::Duration::hours(1),
    )
    .await
    .expect("the hourly inventory is a decision");
    let seen = bodies.lock().expect("readable").clone();
    assert_eq!(lists(&seen), 1);
    assert!(backup_patches(&seen).is_empty());
    assert_eq!(
        lists(&seen),
        1,
        "TWO lists for the whole migration and its confirmation, against fifty for the same \
         work before this fix"
    );
}

/// **M-1, the other half.** A pass whose detach budget IS spent stops the walk
/// there, says its counts are floors, and re-arms the next reconcile — so a
/// migration bigger than one budget costs ONE page per pass, not twenty.
#[tokio::test]
async fn a_pass_that_spends_its_detach_budget_stops_and_re_arms() {
    let now = utc(2026, 9, 12, 0, 0);
    // One more candidate than the budget allows, all on a single page, and the
    // page carries a `continue` token so that a walk which did NOT stop would
    // go on asking.
    let names: Vec<String> = (0..=MAX_MIGRATIONS_PER_PASS)
        .map(|i| format!("logweir-backup-hist-{i:08}-000000"))
        .collect();
    let mut routes = vec![list_route(
        names
            .iter()
            .map(|n| legacy_backup(n, "hist", UID, "Succeeded"))
            .collect(),
        Some("page-2"),
    )];
    // One route for every name: they all end in `-000000`, and the double
    // matches a route by path SUFFIX.
    routes.push(Route {
        method: "PATCH",
        path_suffix: "-000000",
        status: 200,
        body: legacy_backup(&names[0], "hist", UID, "Succeeded").to_string(),
    });
    let (client, _rec, bodies) = mock_client_recording_bodies(routes);
    let observed = observe(&backups(&client), None, "hist", UID, None, now)
        .await
        .expect("a budgeted pass is a decision");

    let seen = bodies.lock().expect("readable").clone();
    assert_eq!(
        backup_patches(&seen).len(),
        MAX_MIGRATIONS_PER_PASS,
        "exactly the budget and not one more: a reconcile that sent 35 040 sequential PATCHes \
         would hold this schedule's slot evaluation for minutes"
    );
    assert_eq!(
        calls(&seen)
            .into_iter()
            .filter(|(m, p)| m == "GET" && p.ends_with("/backups"))
            .count(),
        1,
        "and it did NOT page on to count objects it was not going to touch this pass"
    );
    // The budget is one page's worth on purpose, so a pass detaches at most one
    // page. What the WHOLE migration costs is a different question and is
    // measured in `a_multi_page_migration_costs_a_quadratic_number_of_lists`:
    // this row, like every other route-table row here, only ever exercises
    // pass 1.
    assert_eq!(
        MAX_MIGRATIONS_PER_PASS, INVENTORY_PAGE_SIZE as usize,
        "the budget and the page size are one number; splitting them re-opens M-1 by degrees"
    );
    assert!(
        observed.history.run_count_capped,
        "the counts are floors, because the walk stopped early"
    );
    assert!(
        !observed.history.ownership_scan_complete,
        "and a pass that stopped early has established nothing about ownership — H-1's gate \
         covers the budget case as well as the page-bound case"
    );
    assert!(
        observed.history.legacy_migratable_runs > 0,
        "so the very next reconcile continues, rather than waiting an hour"
    );
    let status: BackupScheduleStatus = serde_json::from_value(serde_json::json!({
        "activeRuns": [],
        "history": serde_json::to_value(&observed.history).expect("serialises")
    }))
    .expect("a status");
    assert!(inventory_due(
        Some(&status),
        now + chrono::Duration::seconds(30)
    ));
    assert!(
        !may_use_label_selector(Some(&status)),
        "and it continues NAMESPACE-WIDE"
    );
}

/// **M-2.** `NoScheduleReference` is terminal, and the condition says so and
/// names the only two things that work.
///
/// `Backup.spec` carries `self == oldSelf`, so the `spec.scheduleRef` that
/// would keep the run a member of this schedule ([`is_run_of_schedule`] rule 3)
/// cannot be added to an object that already exists — by this controller, by an
/// operator, by anyone. D1 §6.9 tells an operator to wait for
/// `HistoryRetained=True` before deleting a schedule without
/// `--cascade=orphan`; on a schedule holding one such run that wait never ends,
/// and a message that reads like a transient refusal is what makes it a trap.
#[tokio::test]
async fn a_terminally_blocked_run_says_it_will_never_clear_and_names_the_remedies() {
    let now = utc(2026, 9, 12, 0, 0);
    let orphanable = "logweir-backup-hist-20260910-000000";
    let mut mismatched = legacy_backup(orphanable, "hist", UID, "Succeeded");
    mismatched["spec"]["scheduleRef"] = serde_json::json!({ "name": "somebody-else" });

    let (client, _rec, bodies) =
        mock_client_recording_bodies(vec![list_route(vec![mismatched], None)]);
    let observed = observe(&backups(&client), None, "hist", UID, None, now)
        .await
        .expect("a terminal refusal is a condition, not an error");

    assert!(
        backup_patches(&bodies.lock().expect("readable")).is_empty(),
        "it is never patched — detaching it would leave it a member of nothing"
    );
    let blocked = observed
        .history
        .migration_blocked
        .as_deref()
        .unwrap_or_default();
    assert_eq!(blocked.len(), 1);
    assert_eq!(blocked[0].name, orphanable);
    assert_eq!(blocked[0].reason, "NoScheduleReference");

    let condition = history_condition(&observed.history, None, Some(4), now);
    assert_eq!(condition.status, "False");
    assert_eq!(condition.reason.as_deref(), Some("MigrationBlocked"));
    let message = condition.message.clone().expect("a message");
    assert!(
        message.contains("NEVER"),
        "the operator is told this does not clear, rather than being left to wait: {message}"
    );
    assert!(
        message.contains("--cascade=orphan"),
        "remedy one: {message}"
    );
    assert!(
        message.contains("deleting those runs"),
        "remedy two: {message}"
    );
    assert!(
        message.contains(orphanable),
        "and it names the run: {message}"
    );

    // AND IT REALLY NEVER CLEARS: no interval retries it, because there is
    // nothing to retry.
    let status: BackupScheduleStatus = serde_json::from_value(serde_json::json!({
        "activeRuns": [],
        "history": serde_json::to_value(&observed.history).expect("serialises"),
        "conditions": [serde_json::to_value(&condition).expect("serialises")],
    }))
    .expect("a status");
    assert!(
        !inventory_due(Some(&status), now + chrono::Duration::minutes(30)),
        "half an hour later, nothing is re-attempted"
    );
    assert!(
        !may_use_label_selector(Some(&status)),
        "and the walk stays namespace-wide, because the run it cannot detach carries no UID \
         label either"
    );

    // THE TWO SHAPES ARE DISTINGUISHED, not conflated. A `403` says NOT NOW.
    assert!(!blocked_reason_clears_itself("NoScheduleReference"));
    assert!(blocked_reason_clears_itself("ApiForbidden"));
    assert!(blocked_reason_clears_itself("ApiInvalid"));
    assert!(
        !blocked_reason_clears_itself("SomethingAFutureVersionWrote"),
        "and an unknown reason is read as terminal, which is the safe direction: the message \
         then tells the operator to use `--cascade=orphan`, which is correct either way"
    );

    // A schedule blocked only on transient refusals gets the other sentence.
    let transient = ScheduleHistory {
        run_count: 1,
        run_count_capped: false,
        estimated_bytes: 4096,
        legacy_owned_runs: 1,
        legacy_migratable_runs: 0,
        migration_blocked: Some(vec![MigrationBlocked {
            name: "b".to_string(),
            reason: "ApiForbidden".to_string(),
        }]),
        ownership_scan_complete: true,
        inventoried_at: now,
    };
    let message = history_condition(&transient, None, Some(4), now)
        .message
        .expect("a message");
    assert!(
        message.contains("clear themselves"),
        "a 403 is the API server saying NOT NOW: {message}"
    );
    assert!(
        !message.contains("NEVER"),
        "and must not be reported as permanent: {message}"
    );
}

/// **L-2.** The blocked sample is sorted before it is truncated, so two passes
/// over the same namespace report the same ten.
#[tokio::test]
async fn the_blocked_sample_is_deterministic_across_passes() {
    let now = utc(2026, 9, 12, 0, 0);
    let mut forward: Vec<String> = (0..25)
        .map(|i| format!("logweir-backup-hist-b{i:02}"))
        .collect();
    let mut reverse = forward.clone();
    reverse.reverse();

    let mut samples = Vec::new();
    for order in [&mut forward, &mut reverse] {
        let mut routes = vec![list_route(
            order
                .iter()
                .map(|n| legacy_backup(n, "hist", UID, "Succeeded"))
                .collect(),
            None,
        )];
        routes.extend(
            order
                .iter()
                .map(|n| patch_route(n, 403, api_error(403, "Forbidden"))),
        );
        let (client, _rec, _bodies) = mock_client_recording_bodies(routes);
        let observed = observe(&backups(&client), None, "hist", UID, None, now)
            .await
            .expect("an inventory is a decision");
        assert_eq!(
            observed.history.legacy_owned_runs, 25,
            "the COUNT is all of them, whatever order the API server paged them in"
        );
        samples.push(
            observed
                .history
                .migration_blocked
                .unwrap_or_default()
                .into_iter()
                .map(|b| b.name)
                .collect::<Vec<_>>(),
        );
    }
    assert_eq!(samples[0].len(), MIGRATION_BLOCKED_SAMPLE);
    assert_eq!(
        samples[0], samples[1],
        "the same ten, in the same order, from the same set in two different paging orders. \
         The CRD used to promise `newest first` and deliver whichever ten came back first."
    );
    assert!(
        samples[0].windows(2).all(|w| w[0] <= w[1]),
        "sorted by name: {:?}",
        samples[0]
    );
}

/// **R-1.** What a whole multi-pass migration actually costs, measured against
/// a double that can tell one page from another.
///
/// # Why the rest of this file could not measure it
///
/// `weirkeeper::testing`'s route table matches on the request path with the
/// query string stripped, so no route can answer page 0 and page 1 with
/// different bodies. Every other row here therefore exercises **pass 1 only** —
/// the one pass for which "a migrating pass reads one page" is true. It is not
/// true of pass 2: the walk always restarts at page 0, and a page whose
/// candidates have already been detached does not stop it, because the budget
/// is spent only when a candidate is *skipped* and a candidate-free page skips
/// nothing. So pass *k* reads *k* + 1 pages, and the published cost of a
/// migration was about ten times too optimistic (review finding R-1).
///
/// This row uses a stateful double — one that pages on `continue=` and
/// remembers the patches — so the number below is measured rather than derived,
/// and `docs/kubernetes.md` §9 quotes the closed form this asserts.
#[tokio::test]
async fn a_multi_page_migration_costs_a_quadratic_number_of_lists() {
    use std::sync::{Arc, Mutex};

    const PAGES: usize = 3;
    let page_size = INVENTORY_PAGE_SIZE as usize;
    let now = utc(2026, 9, 12, 0, 0);

    // Every object is a legacy-owned terminal run of `hist`.
    let objects: Arc<Mutex<Vec<serde_json::Value>>> = Arc::new(Mutex::new(
        (0..PAGES * page_size)
            .map(|i| {
                legacy_backup(
                    &format!("logweir-backup-hist-{i:08}-000000"),
                    "hist",
                    UID,
                    "Succeeded",
                )
            })
            .collect(),
    ));
    let lists = Arc::new(Mutex::new(0usize));
    let patches = Arc::new(Mutex::new(0usize));

    let client = {
        let objects = Arc::clone(&objects);
        let lists = Arc::clone(&lists);
        let patches = Arc::clone(&patches);
        let service = tower::service_fn(move |req: http::Request<kube::client::Body>| {
            let objects = Arc::clone(&objects);
            let lists = Arc::clone(&lists);
            let patches = Arc::clone(&patches);
            async move {
                let method = req.method().clone();
                let uri = req.uri().to_string();
                let body = {
                    use http_body_util::BodyExt as _;
                    req.into_body()
                        .collect()
                        .await
                        .map(|c| String::from_utf8_lossy(&c.to_bytes()).into_owned())
                        .unwrap_or_default()
                };
                let (path, query) = match uri.split_once('?') {
                    Some((p, q)) => (p, q),
                    None => (uri.as_str(), ""),
                };
                let reply = |value: serde_json::Value| {
                    Ok::<_, std::convert::Infallible>(
                        http::Response::builder()
                            .status(200)
                            .body(kube::client::Body::from(value.to_string().into_bytes()))
                            .expect("a response builds"),
                    )
                };

                if method == http::Method::GET && path.ends_with("/backups") {
                    *lists.lock().expect("readable") += 1;
                    // THE PAGE THE `continue` TOKEN NAMES. This is the whole
                    // point of the stateful double: the route table cannot do
                    // it, so the multi-pass shape was invisible.
                    let page: usize = query
                        .split('&')
                        .find_map(|kv| kv.strip_prefix("continue="))
                        .and_then(|token| token.parse().ok())
                        .unwrap_or(0);
                    assert!(
                        query.contains(&format!("limit={INVENTORY_PAGE_SIZE}")),
                        "every page still carries a `limit`: {query}"
                    );
                    let held = objects.lock().expect("readable");
                    let start = page * page_size;
                    let items: Vec<serde_json::Value> =
                        held[start..(start + page_size).min(held.len())].to_vec();
                    let mut metadata = serde_json::json!({ "resourceVersion": "41" });
                    if start + page_size < held.len() {
                        metadata["continue"] = serde_json::json!((page + 1).to_string());
                    }
                    return reply(serde_json::json!({
                        "apiVersion": "logweir.dev/v1alpha1",
                        "kind": "BackupList",
                        "metadata": metadata,
                        "items": items
                    }));
                }

                if method == http::Method::PATCH && path.contains("/backups/") {
                    *patches.lock().expect("readable") += 1;
                    let name = path.rsplit('/').next().unwrap_or_default().to_string();
                    let patch: serde_json::Value =
                        serde_json::from_str(&body).expect("a patch body is JSON");
                    let mut held = objects.lock().expect("readable");
                    let object = held
                        .iter_mut()
                        .find(|o| o["metadata"]["name"] == serde_json::json!(name))
                        .expect("the controller patched an object it listed");
                    weirkeeper::conditions::apply_merge_patch(object, &patch);
                    return reply(object.clone());
                }

                panic!("the stateful double was asked for {method} {path}");
            }
        });
        kube::Client::new(service, NS)
    };

    let mut stored: Option<BackupScheduleStatus> = None;
    let mut passes = 0usize;
    loop {
        let due = inventory_due(stored.as_ref(), now);
        if !due {
            break;
        }
        passes += 1;
        assert!(passes <= 16, "the migration must terminate");
        let observed = observe(&backups(&client), stored.as_ref(), "hist", UID, None, now)
            .await
            .expect("a migrating pass is a decision");
        let condition = history_condition(&observed.history, None, Some(4), now);
        stored = Some(
            serde_json::from_value(serde_json::json!({
                "activeRuns": [],
                "history": serde_json::to_value(&observed.history).expect("serialises"),
                "conditions": [serde_json::to_value(&condition).expect("serialises")],
            }))
            .expect("a status"),
        );
    }

    let lists = *lists.lock().expect("readable");
    let patches = *patches.lock().expect("readable");
    assert_eq!(
        patches,
        PAGES * page_size,
        "every object is detached exactly once over the whole migration"
    );
    assert_eq!(passes, PAGES, "one pass per page of candidates");

    // THE CLOSED FORM, AND IT IS QUADRATIC. Pass k reads pages 0..k because the
    // walk restarts at page 0 every time and an already-detached page does not
    // stop it; the last pass reads every page and then runs out. So
    //     LISTs = Σ(k=1..P-1)(k+1) + P = P(P+1)/2 + P − 1.
    // `docs/kubernetes.md` §9 quotes this, and 10 000 pre-upgrade runs are
    // P = 20 ⇒ 229 LIST requests and ≈ 114 500 objects read — NOT the 21 and
    // 10 000 the table claimed before this row existed.
    let p = PAGES;
    assert_eq!(
        lists,
        p * (p + 1) / 2 + p - 1,
        "measured LIST count must equal the closed form the documentation quotes"
    );
    assert_eq!(lists, 8, "and for three pages that is eight");

    // It is still BOUNDED, which is what makes it a cost and not a hazard: no
    // pass exceeds the page bound, whatever the namespace holds.
    assert!(lists <= passes * MAX_INVENTORY_PAGES);

    // And the migration really finished: the condition is True and the selector
    // is unlocked, so the steady state is the cheap one.
    let settled = stored.expect("the loop ran at least once");
    let history = settled.history.clone().expect("a history block");
    assert!(history.ownership_scan_complete);
    assert_eq!(history.legacy_owned_runs, 0);
    assert_eq!(history.run_count, (PAGES * page_size) as i64);
    assert!(
        may_use_label_selector(Some(&settled)),
        "so every later inventory is O(this schedule) — §6.7's saving is not lost"
    );
}

/// **R-3, the invariant the arm reordering must not break.**
///
/// An incomplete ownership scan can never produce `True`, whichever other facts
/// the block carries. The reordering that restored D1 §6.2 step 5's
/// `LegacyOwnerReferencesRemain` moved three `False` arms in front of the
/// incomplete-scan arm; this asserts over the whole cross-product rather than
/// over a reading of the code, because the reading is exactly what a later edit
/// changes.
#[test]
fn an_incomplete_scan_is_never_reported_as_true() {
    let now = utc(2026, 9, 12, 0, 0);
    let blocked_sets = [
        None,
        Some(vec![MigrationBlocked {
            name: "b".to_string(),
            reason: "ApiForbidden".to_string(),
        }]),
        Some(vec![MigrationBlocked {
            name: "b".to_string(),
            reason: "NoScheduleReference".to_string(),
        }]),
    ];
    for complete in [false, true] {
        for capped in [false, true] {
            for owned in [0i64, 3] {
                for migratable in [0i64, 2] {
                    for runs in [0i64, 9_000] {
                        for blocked in &blocked_sets {
                            let history = ScheduleHistory {
                                run_count: runs,
                                run_count_capped: capped,
                                estimated_bytes: 4096,
                                legacy_owned_runs: owned.max(migratable),
                                legacy_migratable_runs: migratable,
                                migration_blocked: blocked.clone(),
                                ownership_scan_complete: complete,
                                inventoried_at: now,
                            };
                            let condition = history_condition(&history, None, Some(4), now);
                            if !complete {
                                assert_eq!(
                                    condition.status, "False",
                                    "an incomplete scan may never claim the history is \
                                     retained: {history:?}"
                                );
                                assert!(
                                    condition
                                        .message
                                        .as_deref()
                                        .is_some_and(|m| m.contains("did not finish")),
                                    "and it must SAY the scan did not finish, whichever arm \
                                     reported it: {history:?}"
                                );
                            }
                            assert!(
                                ["True", "False"].contains(&condition.status.as_str()),
                                "no third status"
                            );
                        }
                    }
                }
            }
        }
    }

    // AND THE ORDINARY MID-MIGRATION STATE IS `LegacyOwnerReferencesRemain`
    // AGAIN (finding R-3). Before the reorder, any migration spanning more than
    // one page reported `MigrationBlocked` — because a budget-spent pass sets
    // `runCountCapped` — where D1 §6.2 step 5 assigns
    // `LegacyOwnerReferencesRemain` to "terminal runs still being detached", so
    // an operator watching a healthy upgrade saw a reason that reads like
    // something is stuck.
    let mid_migration = ScheduleHistory {
        run_count: 1_500,
        run_count_capped: true,
        estimated_bytes: 4096,
        legacy_owned_runs: 1_000,
        legacy_migratable_runs: 1_000,
        migration_blocked: None,
        ownership_scan_complete: false,
        inventoried_at: now,
    };
    let condition = history_condition(&mid_migration, None, Some(4), now);
    assert_eq!(condition.status, "False");
    assert_eq!(
        condition.reason.as_deref(),
        Some("LegacyOwnerReferencesRemain"),
        "the reason D1 §6.2 step 5 names for exactly this state"
    );
    let message = condition.message.expect("a message");
    assert!(
        message.contains("did not finish"),
        "and the unfinished scan is still reported, as a suffix rather than by replacing the \
         reason: {message}"
    );

    // The incomplete-scan arm still owns the case nothing else explains: a
    // namespace bigger than the walk's bound, with no migration under way.
    let too_big = ScheduleHistory {
        legacy_owned_runs: 0,
        legacy_migratable_runs: 0,
        ..mid_migration.clone()
    };
    let condition = history_condition(&too_big, None, Some(4), now);
    assert_eq!(condition.status, "False");
    assert_eq!(condition.reason.as_deref(), Some("MigrationBlocked"));
    assert!(condition
        .message
        .expect("a message")
        .contains("Prune terminal runs"));
}
