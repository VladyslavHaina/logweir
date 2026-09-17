//! D1 §3.1 and §3.2: what a `Backup` IS, and whether two runs were asked to do
//! the same thing.
//!
//! EVERY TEST HERE IS PURE. `run_identity` reads the object and nothing else —
//! no clock, no status field, no annotation — so these are table tests over
//! JSON fixtures with no API server, no Job and no subprocess.
//!
//! THE FIXTURES ARE MUTATED, NOT RE-TEMPLATED. One base object per shape, each
//! case changing exactly the field it is about, so a case cannot pass because
//! the fixture it typed happened to differ somewhere else too.

use serde_json::{json, Value};
use weirkeeper::crds::backup::{Backup, TriggerKind};
use weirkeeper::identity::{self, IdentityError};
use weirkeeper::policy;

const NS: &str = "team-a";
const UID: &str = "1d0f2c3b-4a59-4c6e-8f10-2b7c9d4e5f60";
const SCHEDULE_UID: &str = "3f0c7a11-9b2d-4e8f-a0c1-5d6e7f809a1b";
const SLOT: &str = "20260915-020000";

/// A manual `Backup`, as a person or the console creates one.
fn manual_json() -> Value {
    json!({
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "Backup",
        "metadata": { "name": "logweir-manual-abc", "namespace": NS, "uid": UID },
        "spec": {
            "sourceRef": { "name": "prod" },
            "topics": ["orders", "payments"],
            "archive": { "url": "s3://kafka-backups/logweir", "secretRef": { "name": "logweir-s3" } },
            "triggeredBy": "manual",
            "deadlineSeconds": 3600
        }
    })
}

/// Attempt 0 of a slot, with the modern `scheduleRef.uid` and `trigger`.
fn scheduled_json() -> Value {
    let mut v = manual_json();
    v["metadata"]["name"] = json!(format!("logweir-backup-nightly-{SLOT}"));
    v["spec"]["scheduleRef"] = json!({ "name": "nightly", "uid": SCHEDULE_UID });
    v["spec"]["slot"] = json!(SLOT);
    v["spec"]["triggeredBy"] = json!("schedule");
    v["spec"]["trigger"] = json!({ "kind": "Scheduled", "attempt": 0 });
    v
}

fn backup(value: &Value) -> Backup {
    serde_json::from_value(value.clone()).expect("the fixture is a Backup")
}

fn identity_of(value: &Value) -> Result<weirkeeper::identity::RunIdentity, IdentityError> {
    identity::run_identity(&backup(value))
}

// ---------------------------------------------------------------------------
// Rule 4: the legacy reading, which is the whole upgrade story
// ---------------------------------------------------------------------------

/// A `Backup` created before `spec.trigger` existed resolves exactly as the
/// controller that created it resolved it.
///
/// THE UPGRADE IS A NO-OP, AND THIS IS WHERE THAT IS TRUE OR NOT. Every stored
/// `Backup` in every cluster has no `trigger`; if rule 4 read one of them
/// differently, upgrading the controller would change the execution id of a
/// run that is mid-flight and point it at a different archive prefix.
#[test]
fn a_backup_with_no_trigger_reads_as_the_earlier_controller_read_it() {
    let mut legacy = scheduled_json();
    legacy["spec"].as_object_mut().unwrap().remove("trigger");
    let id = identity_of(&legacy).expect("a legacy scheduled Backup has an identity");
    assert_eq!(id.kind, TriggerKind::Scheduled);
    assert_eq!(id.attempt, 0);
    assert_eq!(
        id.execution_id,
        format!("{SCHEDULE_UID}-{SLOT}"),
        "the execution id of a legacy scheduled run is <scheduleUid>-<slot>, byte for byte \
         what `slot::backup_id_for` produced before this module existed"
    );

    let mut legacy_manual = manual_json();
    legacy_manual["spec"]
        .as_object_mut()
        .unwrap()
        .remove("trigger");
    let id = identity_of(&legacy_manual).expect("a legacy manual Backup has an identity");
    assert_eq!(id.kind, TriggerKind::Manual);
    assert_eq!(
        id.execution_id, UID,
        "a manual run executes under its own UID"
    );
    assert!(id.schedule.is_none());

    // And a declared trigger that says what rule 4 would have inferred gives
    // the identical identity — the field is finer, not different.
    assert_eq!(
        identity_of(&legacy).expect("legacy").execution_id,
        identity_of(&scheduled_json())
            .expect("declared")
            .execution_id
    );
}

/// The legacy UID source is the controller ownerReference, and it still works
/// with no `scheduleRef.uid` at all.
#[test]
fn a_legacy_scheduled_backup_takes_its_uid_from_the_owner_reference() {
    let mut legacy = scheduled_json();
    legacy["spec"]["scheduleRef"] = json!({ "name": "nightly" });
    legacy["metadata"]["ownerReferences"] = json!([{
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "BackupSchedule",
        "name": "nightly",
        "uid": SCHEDULE_UID,
        "controller": true,
        "blockOwnerDeletion": true,
    }]);
    let id = identity_of(&legacy).expect("the owner reference carries the UID");
    let schedule = id.schedule.expect("a scheduled run names its schedule");
    assert_eq!(schedule.uid, SCHEDULE_UID);
    assert!(
        schedule.from_owner_reference,
        "the caller needs to know the UID came from the ownerReference, because PLAT-05.2 \
         removes that reference and the annotation takes over"
    );

    // With NEITHER, there is no identity at all: two same-named schedules'
    // runs would otherwise share one archive prefix.
    let mut orphan = legacy.clone();
    orphan["metadata"]
        .as_object_mut()
        .unwrap()
        .remove("ownerReferences");
    assert!(matches!(
        identity_of(&orphan),
        Err(IdentityError::ScheduledIdentityMismatch { .. })
    ));
}

// ---------------------------------------------------------------------------
// Rule 1: the name is a pure function of the trigger
// ---------------------------------------------------------------------------

/// A scheduled-kind object whose name is not the name its fields compose is
/// refused, and is NEVER re-read as a manual run.
#[test]
fn a_misnamed_scheduled_backup_is_a_mismatch_and_not_a_manual_run() {
    let mut wrong = scheduled_json();
    wrong["metadata"]["name"] = json!("logweir-backup-nightly-20260915-999999");
    let err = identity_of(&wrong).expect_err("the name does not compose");
    assert!(
        matches!(err, IdentityError::ScheduledIdentityMismatch { .. }),
        "got {err:?}"
    );
    assert_eq!(err.terminal_state(), "ScheduledIdentityMismatch");
    assert!(
        format!("{err}").contains("never re-read as a manual run"),
        "the message must say what the refusal PREVENTS: a manual run executes under its own \
         UID and would write a second archive of a window a scheduled run already owns. Got: \
         {err}"
    );

    // The same object renamed to the composed name resolves — so the case
    // above failed on the name and on nothing else.
    let mut right = wrong;
    right["metadata"]["name"] = json!(format!("logweir-backup-nightly-{SLOT}"));
    assert!(identity_of(&right).is_ok());
}

/// A retry is named and identified by its attempt, and the attempt is checked
/// against `retryOf` rather than trusted.
#[test]
fn a_retry_is_a_new_execution_id_and_names_the_attempt_it_retries() {
    let retry = |attempt: i64, retry_of: Option<&str>, name: &str| {
        let mut v = scheduled_json();
        v["metadata"]["name"] = json!(name);
        v["spec"]["trigger"] = match retry_of {
            Some(r) => json!({ "kind": "Retry", "attempt": attempt, "retryOf": { "name": r } }),
            None => json!({ "kind": "Retry", "attempt": attempt }),
        };
        v
    };

    let attempt0 = format!("logweir-backup-nightly-{SLOT}");
    let attempt1 = format!("{attempt0}-r1");
    let attempt2 = format!("{attempt0}-r2");

    let id = identity_of(&retry(1, Some(&attempt0), &attempt1)).expect("attempt 1 resolves");
    assert_eq!(id.kind, TriggerKind::Retry);
    assert_eq!(id.attempt, 1);
    assert_eq!(
        id.execution_id,
        format!("{SCHEDULE_UID}-{SLOT}-r1"),
        "A RETRY IS A NEW EXECUTION ID. Reusing attempt 0's would append into the prefix a \
         failed attempt may have half-written — the manifest-says-2048-broker-holds-6000 \
         false pass"
    );
    assert_ne!(
        id.execution_id,
        identity_of(&scheduled_json())
            .expect("attempt 0")
            .execution_id
    );

    let id = identity_of(&retry(2, Some(&attempt1), &attempt2)).expect("attempt 2 resolves");
    assert_eq!(id.execution_id, format!("{SCHEDULE_UID}-{SLOT}-r2"));

    // `retryOf` naming the wrong attempt, or naming nothing, is a mismatch:
    // a chain that can skip a link is a chain nobody can audit.
    assert!(matches!(
        identity_of(&retry(2, Some(&attempt0), &attempt2)),
        Err(IdentityError::ScheduledIdentityMismatch { .. })
    ));
    assert!(matches!(
        identity_of(&retry(1, None, &attempt1)),
        Err(IdentityError::ScheduledIdentityMismatch { .. })
    ));
    // And a retry whose NAME is attempt 0's.
    assert!(matches!(
        identity_of(&retry(1, Some(&attempt0), &attempt0)),
        Err(IdentityError::ScheduledIdentityMismatch { .. })
    ));
}

/// `CatchUp` is slot S started late: the SAME name and the SAME execution id
/// as `Scheduled`.
///
/// A CATCH-UP THAT MINTED ITS OWN IDENTITY WOULD BE A SECOND ARCHIVE OF ONE
/// WINDOW. A controller restarted after an outage runs the slot it missed;
/// giving that run a distinct id would let a second restart produce a third.
#[test]
fn a_catch_up_is_the_same_run_as_the_slot_it_is_catching_up() {
    let mut catch_up = scheduled_json();
    catch_up["spec"]["trigger"] = json!({ "kind": "CatchUp", "attempt": 0 });
    let caught = identity_of(&catch_up).expect("a catch-up resolves");
    let on_time = identity_of(&scheduled_json()).expect("attempt 0 resolves");
    assert_eq!(caught.execution_id, on_time.execution_id);
    assert_eq!(caught.kind, TriggerKind::CatchUp);
    assert_eq!(
        caught.slot, on_time.slot,
        "the two differ only in what they say about WHY the run started"
    );
}

// ---------------------------------------------------------------------------
// Rule 3: the shapes that are refused
// ---------------------------------------------------------------------------

/// Every illegal `(kind, attempt, slot)` combination is a mismatch.
#[test]
fn the_illegal_trigger_shapes_are_each_refused() {
    let cases: Vec<(&str, Value)> = vec![
        ("Scheduled with an attempt", {
            let mut v = scheduled_json();
            v["spec"]["trigger"] = json!({ "kind": "Scheduled", "attempt": 1 });
            v
        }),
        ("CatchUp with an attempt", {
            let mut v = scheduled_json();
            v["spec"]["trigger"] = json!({ "kind": "CatchUp", "attempt": 2 });
            v
        }),
        ("Retry with attempt 0", {
            let mut v = scheduled_json();
            v["spec"]["trigger"] = json!({ "kind": "Retry", "attempt": 0 });
            v
        }),
        ("Manual carrying a slot", {
            let mut v = manual_json();
            v["spec"]["slot"] = json!(SLOT);
            v["spec"]["trigger"] = json!({ "kind": "Manual", "attempt": 0 });
            v
        }),
        ("Manual with an attempt", {
            let mut v = manual_json();
            v["spec"]["trigger"] = json!({ "kind": "Manual", "attempt": 1 });
            v
        }),
        ("a scheduled kind with no scheduleRef", {
            let mut v = scheduled_json();
            v["spec"].as_object_mut().unwrap().remove("scheduleRef");
            v
        }),
        ("a scheduled kind with no slot", {
            let mut v = scheduled_json();
            v["spec"].as_object_mut().unwrap().remove("slot");
            v
        }),
        ("a slot that is not yyyymmdd-hhmmss", {
            let mut v = scheduled_json();
            v["spec"]["slot"] = json!("2026-09-15T02:00");
            v["metadata"]["name"] = json!("logweir-backup-nightly-2026-09-15T02:00");
            v
        }),
    ];
    for (name, value) in cases {
        let err = match identity_of(&value) {
            Err(err) => err,
            Ok(id) => unreachable(name, &id),
        };
        assert!(
            matches!(err, IdentityError::ScheduledIdentityMismatch { .. }),
            "case `{name}` must be a mismatch; got {err:?}"
        );
    }
}

/// `Result::expect_err` with a case name, as a function so the table above
/// reads as a table.
fn unreachable(case: &str, id: &weirkeeper::identity::RunIdentity) -> IdentityError {
    panic!("case `{case}` resolved to {id:?}; it must not resolve at all")
}

// ---------------------------------------------------------------------------
// Membership, including the case the history migration creates
// ---------------------------------------------------------------------------

/// Membership has three sources, and the third is the one PLAT-05.2 needs.
#[test]
fn membership_survives_the_history_migration_removing_the_owner_reference() {
    let modern = backup(&scheduled_json());
    assert!(identity::is_run_of_schedule(
        &modern,
        "nightly",
        SCHEDULE_UID
    ));
    assert!(
        !identity::is_run_of_schedule(&modern, "nightly", "a-different-uid"),
        "a schedule deleted and recreated under the same name must not adopt the previous \
         object's work"
    );
    assert!(!identity::is_run_of_schedule(
        &modern,
        "weekly",
        SCHEDULE_UID
    ));

    let mut legacy = scheduled_json();
    legacy["spec"]["scheduleRef"] = json!({ "name": "nightly" });
    legacy["metadata"]["ownerReferences"] = json!([{
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "BackupSchedule",
        "name": "nightly",
        "uid": SCHEDULE_UID,
        "controller": true,
        "blockOwnerDeletion": true,
    }]);
    assert!(identity::is_run_of_schedule(
        &backup(&legacy),
        "nightly",
        SCHEDULE_UID
    ));

    // The migration REMOVES that ownerReference so deleting the schedule stops
    // deleting its history, and writes the annotation in its place. Without
    // the third rule this run would be orphaned by the very migration that
    // exists to keep it.
    let mut migrated = legacy;
    migrated["metadata"]
        .as_object_mut()
        .unwrap()
        .remove("ownerReferences");
    migrated["metadata"]["annotations"] =
        json!({ "logweir.dev/history-retained-from-owner": SCHEDULE_UID });
    assert!(
        identity::is_run_of_schedule(&backup(&migrated), "nightly", SCHEDULE_UID),
        "a migrated legacy run is still a run of its schedule"
    );
    assert!(
        !identity::is_run_of_schedule(&backup(&migrated), "nightly", "a-different-uid"),
        "and the annotation is checked against the UID, not merely present"
    );
}

// ---------------------------------------------------------------------------
// D1 §3.2 — the run policy digest
// ---------------------------------------------------------------------------

/// The digest covers WHAT a run does and nothing about WHEN.
#[test]
fn the_run_policy_digest_ignores_cadence_and_canonicalises_the_topic_order() {
    let base = backup(&scheduled_json());
    let digest = policy::run_policy_sha256(&base.spec);
    assert!(
        digest.starts_with("sha256:") && digest.len() == 71,
        "the digest is `sha256:<64 lowercase hex>`, the one spelling this corpus uses: {digest}"
    );

    // Reordering the topic list is not a policy change: the digest
    // canonicalises, while `spec.topics` keeps the user's order verbatim.
    let mut reordered = scheduled_json();
    reordered["spec"]["topics"] = json!(["payments", "orders"]);
    let reordered = backup(&reordered);
    assert_eq!(
        policy::run_policy_sha256(&reordered.spec),
        digest,
        "two schedules naming the same topics in a different order are the same policy"
    );
    assert_eq!(
        reordered.spec.topics,
        vec!["payments".to_string(), "orders".to_string()],
        "and the SPEC keeps the order the user typed — only the digest canonicalises"
    );

    // A slot, a trigger and a schedule revision are all about WHEN, or about
    // WHICH run, and none of them moves the digest.
    let mut other_slot = scheduled_json();
    other_slot["spec"]["slot"] = json!("20260916-020000");
    other_slot["metadata"]["name"] = json!("logweir-backup-nightly-20260916-020000");
    other_slot["spec"]["trigger"] = json!({ "kind": "CatchUp", "attempt": 0 });
    other_slot["spec"]["scheduleRef"] =
        json!({ "name": "nightly", "uid": SCHEDULE_UID, "generation": 9 });
    assert_eq!(
        policy::run_policy_sha256(&backup(&other_slot).spec),
        digest,
        "a different slot, trigger and schedule generation are the same POLICY"
    );

    // Each field that decides what a run DOES moves it.
    for (name, mutate) in [
        (
            "a topic added",
            json!(["orders", "payments", "audit"]) as Value,
        ),
        ("a topic removed", json!(["orders"])),
    ] {
        let mut changed = scheduled_json();
        changed["spec"]["topics"] = mutate;
        assert_ne!(
            policy::run_policy_sha256(&backup(&changed).spec),
            digest,
            "case `{name}` must change the policy digest"
        );
    }
    let mut elsewhere = scheduled_json();
    elsewhere["spec"]["archive"]["url"] = json!("s3://other-bucket/logweir");
    assert_ne!(policy::run_policy_sha256(&backup(&elsewhere).spec), digest);
    let mut longer = scheduled_json();
    longer["spec"]["deadlineSeconds"] = json!(7200);
    assert_ne!(policy::run_policy_sha256(&backup(&longer).spec), digest);
}

/// Rule 5: a copied digest that disagrees with the object's own fields is
/// terminal.
#[test]
fn a_copied_policy_digest_that_disagrees_with_the_object_is_refused() {
    let mut carrying = scheduled_json();
    let want = policy::run_policy_sha256(&backup(&scheduled_json()).spec);
    carrying["spec"]["scheduleRef"] = json!({
        "name": "nightly", "uid": SCHEDULE_UID, "runPolicySha256": want,
    });
    assert!(
        identity::check_run_policy_digest(&backup(&carrying)).is_ok(),
        "a digest that matches is accepted"
    );

    let mut wrong = carrying;
    wrong["spec"]["scheduleRef"]["runPolicySha256"] = json!(format!("sha256:{}", "0".repeat(64)));
    let err = identity::check_run_policy_digest(&backup(&wrong))
        .expect_err("a digest that disagrees is refused");
    assert!(matches!(err, IdentityError::RunPolicyDigestMismatch { .. }));
    assert_eq!(err.terminal_state(), "RunPolicyDigestMismatch");

    // Absent is not a mismatch: every Backup created before the field existed
    // has none, and refusing them would be an upgrade that breaks history.
    assert!(identity::check_run_policy_digest(&backup(&scheduled_json())).is_ok());
}

// ---------------------------------------------------------------------------
// D1 §7.1 — the two legal selection shapes
// ---------------------------------------------------------------------------

/// Two shapes are accepted and the other two are named, each with the field
/// that is wrong.
#[test]
fn the_selection_shape_is_one_of_exactly_two() {
    use weirkeeper::policy::SelectionShape;

    let named = backup(&scheduled_json());
    assert_eq!(
        policy::validate_run_policy(&named.spec),
        Ok(SelectionShape::SelectedTopics)
    );

    let mut dynamic = scheduled_json();
    dynamic["spec"]["topics"] = json!([]);
    dynamic["spec"]["allUserTopics"] = json!({
        "exclude": { "prefixes": ["tmp-"] },
        "incompleteDiscovery": "BackUpVisibleTopics"
    });
    assert_eq!(
        policy::validate_run_policy(&backup(&dynamic).spec),
        Ok(SelectionShape::AllUserTopics)
    );

    // Both at once: two answers to one question.
    let mut both = dynamic.clone();
    both["spec"]["topics"] = json!(["orders"]);
    let errs = policy::validate_run_policy(&backup(&both).spec).expect_err("both is refused");
    assert!(
        errs.iter().any(|e| e.field == "spec.allUserTopics"),
        "the error names the field: {errs:?}"
    );

    // Neither: `topics: []` alone is the shape that used to mean "everything"
    // to the engine, which is the defect guard G-GLOB exists for.
    let mut neither = scheduled_json();
    neither["spec"]["topics"] = json!([]);
    let errs = policy::validate_run_policy(&backup(&neither).spec).expect_err("neither is refused");
    assert!(
        errs.iter()
            .any(|e| e.field == "spec.topics" && e.message.contains("G-GLOB")),
        "an empty allowlist with no dynamic block must name the guard it violates: {errs:?}"
    );

    // THE TWO ENTRY POINTS AGREE ABOUT THE SHAPE, and differ only about
    // `deadlineSeconds` — which belongs to PLAT-06.1's `ExecutionSpecInvalid`,
    // not to D1's `InvalidTopicSelection`. The Backup reconciler calls the
    // selection half so it does not take over a refusal that already has an
    // owner.
    let mut zero_deadline = dynamic.clone();
    zero_deadline["spec"]["deadlineSeconds"] = json!(0);
    let zero_deadline = backup(&zero_deadline);
    assert_eq!(
        policy::validate_topic_selection(&zero_deadline.spec),
        Ok(SelectionShape::AllUserTopics),
        "a zero deadline is not a selection problem"
    );
    let errs = policy::validate_run_policy(&zero_deadline.spec)
        .expect_err("but it IS a run-policy problem");
    assert!(
        errs.iter().all(|e| e.field == "spec.deadlineSeconds"),
        "and the only error is the deadline: {errs:?}"
    );

    // And the digest distinguishes the two dynamic policies, so switching
    // `incompleteDiscovery` is visibly a policy change.
    let mut refusing = dynamic.clone();
    refusing["spec"]["allUserTopics"]["incompleteDiscovery"] = json!("Refuse");
    assert_ne!(
        policy::run_policy_sha256(&backup(&refusing).spec),
        policy::run_policy_sha256(&backup(&dynamic).spec),
        "Refuse and BackUpVisibleTopics are different policies and must digest differently"
    );
}

/// A glob metacharacter is refused wherever a topic name can be written.
///
/// GUARD **G-GLOB**, EXTENDED TO THE EXCLUSIONS. The allowlist rail has always
/// existed; a dynamic policy adds two more places a pattern could be typed,
/// and `orders*` excluding everything beginning with `orders` is exactly the
/// misreading a literal-prefix field has to prevent.
#[test]
fn a_glob_metacharacter_is_refused_in_every_place_a_topic_can_be_named() {
    for (field, mutate) in [
        ("spec.topics[0]", json!(["orders*"]) as Value),
        ("spec.topics[1]", json!(["orders", "pay?ents"])),
    ] {
        let mut v = scheduled_json();
        v["spec"]["topics"] = mutate;
        let errs = policy::validate_run_policy(&backup(&v).spec).expect_err("a glob is refused");
        assert!(
            errs.iter().any(|e| e.field == field),
            "expected an error on {field}; got {errs:?}"
        );
    }

    let mut v = scheduled_json();
    v["spec"]["topics"] = json!([]);
    v["spec"]["allUserTopics"] = json!({
        "exclude": { "topics": ["ok"], "prefixes": ["tmp*"] },
        "incompleteDiscovery": "Refuse"
    });
    let errs = policy::validate_run_policy(&backup(&v).spec).expect_err("a glob prefix is refused");
    assert!(
        errs.iter()
            .any(|e| e.field == "spec.allUserTopics.exclude.prefixes[0]"),
        "a prefix is LITERAL: `tmp*` must be refused rather than read as a pattern. Got {errs:?}"
    );

    // The control: the same object with a literal prefix is accepted, so the
    // case above failed on the metacharacter and not on the shape.
    v["spec"]["allUserTopics"]["exclude"]["prefixes"] = json!(["tmp-"]);
    assert!(policy::validate_run_policy(&backup(&v).spec).is_ok());
}

// ---------------------------------------------------------------------------
// The name budget
// ---------------------------------------------------------------------------

/// A schedule name that fits attempt 0 but not `-r1` is a name budget problem
/// and is reported as one.
#[test]
fn a_retry_name_that_does_not_fit_is_name_too_long_and_not_a_mismatch() {
    // 31 characters: attempt 0 fits the 32-character budget, and `-r1` does
    // not fit the 29-character one.
    let long = "n".repeat(31);
    let mut v = scheduled_json();
    v["spec"]["scheduleRef"] = json!({ "name": long, "uid": SCHEDULE_UID });
    v["metadata"]["name"] = json!(format!("logweir-backup-{long}-{SLOT}"));
    assert!(
        identity_of(&v).is_ok(),
        "attempt 0 of a 31-character schedule still fits"
    );

    v["metadata"]["name"] = json!(format!("logweir-backup-{long}-{SLOT}-r1"));
    v["spec"]["trigger"] = json!({
        "kind": "Retry",
        "attempt": 1,
        "retryOf": { "name": format!("logweir-backup-{long}-{SLOT}") }
    });
    let err = identity_of(&v).expect_err("the retry name does not fit");
    assert!(
        matches!(err, IdentityError::NameTooLong { .. }),
        "a name that does not fit is NameTooLong, not a mismatch — the difference is whether \
         an operator should rename the schedule or fix the object. Got {err:?}"
    );
    assert_eq!(err.terminal_state(), "NameTooLong");
}

// ---------------------------------------------------------------------------
// The vocabulary is in one place
// ---------------------------------------------------------------------------

/// Every terminal state this module can produce is in
/// `conditions::TERMINAL_STATES`, and the cadence retry classification names
/// the constant rather than a literal.
#[test]
fn the_identity_terminal_states_are_in_the_one_closed_list() {
    for state in [
        "ScheduledIdentityMismatch",
        "ScheduleNotFound",
        "RunPolicyDigestMismatch",
        "InvalidTopicSelection",
        "DiscoveryFailed",
        "DiscoveryIncomplete",
        "DiscoveryResultUnreadable",
        "SelectionEmpty",
        "SelectionTooLarge",
        "SourceChangedDuringResolution",
    ] {
        assert!(
            weirkeeper::conditions::TERMINAL_STATES.contains(&state),
            "`{state}` must be in the one closed list the metav1-reason regex test walks; a \
             state that is not in it is a state nothing validates"
        );
    }

    assert_eq!(
        weirkeeper::cadence::RETRYABLE_TERMINAL_STATES,
        [
            "DisruptedMidDrill",
            "PodUnschedulable",
            "NoExitCode",
            weirkeeper::conditions::TERMINAL_STATE_DISCOVERY_FAILED,
        ],
        "the retry allowlist is unchanged in VALUE; only the spelling of its fourth entry moved \
         from a literal to the constant D1 §3.4 assigns to conditions.rs"
    );
    assert!(
        !weirkeeper::cadence::RETRYABLE_TERMINAL_STATES
            .contains(&weirkeeper::conditions::TERMINAL_STATE_DISCOVERY_INCOMPLETE),
        "DiscoveryIncomplete is NOT retryable: re-running asks the same principal the same \
         question and gets the same partial answer"
    );
}

// ---------------------------------------------------------------------------
// Rule 1, the other half: the slot is an INSTANT, and the legacy owner
// reference has to name the schedule the run claims
// ---------------------------------------------------------------------------

/// **A SLOT THAT IS FIFTEEN DIGITS AND NOT A DATE IS REFUSED**, even when
/// `metadata.name` composes from it perfectly.
///
/// The shape check — fifteen bytes, digits, a hyphen at 8 — admits
/// `20261309-031700` (month 13), `20260230-000000` (30 February) and
/// `20260915-256100` (hour 25). Each of those composes a name that rule 1
/// accepts, so rule 1 does NOT catch them: the object is internally consistent
/// and simply names an instant that never occurs. A slot is half of the archive
/// prefix, so such a run takes a prefix no schedule can ever mint and no later
/// run of that slot can collide with to reveal the mistake.
///
/// KILLS: replacing the `chrono` round-trip with a shape test; dropping the
/// `format(...) == slot` comparison, which is what rejects the fields
/// `parse_from_str` silently normalises.
#[test]
fn a_slot_that_is_not_a_calendar_instant_is_refused_even_when_the_name_composes() {
    for bad in [
        "20261309-031700", // month 13
        "20260230-000000", // 30 February
        "20260915-256100", // hour 25, minute 61
        "00000000-000000", // month 0, day 0
    ] {
        let mut v = scheduled_json();
        v["spec"]["slot"] = json!(bad);
        v["metadata"]["name"] = json!(format!("logweir-backup-nightly-{bad}"));

        // The premise: rule 1 is satisfied, so this case is ONLY about the date.
        assert_eq!(
            weirkeeper::slot::scheduled_backup_name("nightly", bad).expect("it composes"),
            v["metadata"]["name"].as_str().expect("a name"),
            "{bad}: the fixture's name composes, so a refusal here is the calendar check and \
             not rule 1"
        );

        match identity_of(&v) {
            Err(IdentityError::ScheduledIdentityMismatch { detail }) => assert!(
                detail.contains(bad) && detail.contains("UTC instant"),
                "{bad}: the refusal names the slot and says what is wrong with it: {detail}"
            ),
            other => panic!("{bad}: a slot that is not a date has no identity: {other:?}"),
        }
    }

    // And the real instants around them still resolve.
    for good in ["20280229-000000", "20260915-235959", "20261231-000000"] {
        let mut v = scheduled_json();
        v["spec"]["slot"] = json!(good);
        v["metadata"]["name"] = json!(format!("logweir-backup-nightly-{good}"));
        assert_eq!(
            identity_of(&v)
                .expect("a real instant resolves")
                .slot
                .as_deref(),
            Some(good),
            "{good} is a real instant (2028 is a leap year, 2026 is not) and must not be refused"
        );
    }
}

/// **A LEGACY OWNER REFERENCE SUPPLIES THE UID ONLY WHEN IT NAMES THE SCHEDULE
/// `spec.scheduleRef` NAMES.**
///
/// The legacy identity IS the owner's UID. An object whose `spec.scheduleRef`
/// says `nightly` while its controller ownerReference says `hourly` would, if
/// the name were not part of the match, execute under `hourly`'s archive prefix
/// — a run filed under a schedule that never asked for it, with two schedules'
/// history mixed in one bucket. Rule 1 cannot catch it: the name composes from
/// `scheduleRef.name`, which is the one this object states.
///
/// KILLS: matching the ownerReference on controller + kind + apiVersion alone;
/// checking the name in a separate `any()` that could be satisfied by a
/// different owner than the one the UID came from.
#[test]
fn a_legacy_owner_reference_for_another_schedule_supplies_no_identity() {
    let other_uid = "9a8b7c6d-0000-4000-8000-0000000000ff";
    let mut v = scheduled_json();
    v["spec"]["scheduleRef"] = json!({ "name": "nightly" });
    v["metadata"]["ownerReferences"] = json!([{
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "BackupSchedule",
        "name": "hourly",
        "uid": other_uid,
        "controller": true,
        "blockOwnerDeletion": true,
    }]);

    // The premise: the NAME composes, so rule 1 does not fire first.
    assert_eq!(
        weirkeeper::slot::scheduled_backup_name("nightly", SLOT).expect("it composes"),
        v["metadata"]["name"].as_str().expect("a name"),
        "the fixture is named for `nightly`, the schedule it claims"
    );

    match identity_of(&v) {
        Err(IdentityError::ScheduledIdentityMismatch { detail }) => assert!(
            detail.contains("nightly"),
            "the refusal names the schedule whose reference is missing: {detail}"
        ),
        other => panic!(
            "an ownerReference to `hourly` must not become `nightly`'s archive prefix: {other:?}"
        ),
    }

    // The same object with the owner renamed resolves, and to that owner's UID.
    let mut matching = v.clone();
    matching["metadata"]["ownerReferences"][0]["name"] = json!("nightly");
    let id = identity_of(&matching).expect("the owner now names the schedule");
    assert_eq!(
        id.schedule.expect("a schedule").uid,
        other_uid,
        "and the identity is that owner's UID, which is exactly why the name has to match"
    );

    // Membership asks the same question the same way.
    assert!(
        !identity::is_run_of_schedule(&backup(&v), "nightly", other_uid),
        "a run owned by `hourly` is not a run of `nightly`, whatever UID is quoted"
    );
    assert!(identity::is_run_of_schedule(
        &backup(&matching),
        "nightly",
        other_uid
    ));
}
