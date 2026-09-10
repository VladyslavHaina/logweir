//! The `BackupSchedule` cron reconciler, and guard **G-SLOT**.
//!
//! EVERY TEST HERE IS A PURE-FUNCTION TEST OR A `mock_client` TEST. Nothing
//! dials a socket, nothing waits on a Job, nothing shells out, and nothing
//! approaches Global Constraint 22's 15 s per-test bound — the transport is a
//! `tower` closure and the clock is an argument.
//!
//! THE GUARD IS `a_crash_between_create_and_status_write_yields_exactly_one_backup`.
//! Read it first: everything else in this file exists to make its assertions
//! meaningful (the name is legal, the name is refused when it would not fit,
//! the name is not computed from a clock or a status) or to make the horizon
//! and `suspend` arms checkable.

use chrono::{DateTime, TimeZone, Utc};
use weirkeeper::controllers::backup_schedule::{
    decide, reconcile_schedule, refine_against_last_fire, runner_argv, scheduled_backup,
    status_patch, ScheduleOutcome, SlotDecision, MISSED_SLOT_HORIZON, REASON_SCHEDULED,
    REASON_SLOT_MISSED, REASON_SUSPENDED, REQUEUE_SECS, RUNNER_ARGV_ANNOTATION, SCHEDULE_LABEL,
    SLOT_LABEL, TRIGGERED_BY_SCHEDULE,
};
use weirkeeper::crds::backup_schedule::{BackupSchedule, BackupScheduleStatus};
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
    "generation": 4
  }},
  "spec": {{
    "schedule": "{cron}",
    "sourceRef": {{ "name": "prod" }},
    "topics": ["orders", "payments"],
    "archive": {{ "url": "s3://kafka-backups/logweir" }},
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

/// A UTC instant, spelled as five integers so a test reads like a calendar.
fn utc(y: i32, m: u32, d: u32, h: u32, min: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(y, m, d, h, min, 0)
        .single()
        .expect("the fixture instant exists")
}

/// The body a successful `POST …/backups` is answered with. The API server
/// echoes the created object; nothing in the reconciler reads it, and it is
/// present because a 201 with no body is not a shape the client expects.
fn created_backup_body(name: &str) -> String {
    format!(
        r#"{{"apiVersion":"logweir.dev/v1alpha1","kind":"Backup",
  "metadata":{{"name":"{name}","namespace":"{NS}","uid":"aaaaaaaa-0000-4000-8000-00000000000b"}},
  "spec":{{"sourceRef":{{"name":"prod"}},"topics":["orders"],
    "archive":{{"url":"s3://kafka-backups/logweir"}},"triggeredBy":"schedule",
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
    vec![
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
            method: "PATCH",
            path_suffix: "/backupschedules/nightly/status",
            status: 200,
            body: schedule_json("nightly", UID, DAILY, false),
        },
    ]
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

/// The one `status` object out of the recorded `PATCH` bodies.
fn patched_status(bodies: &[SeenBody]) -> serde_json::Value {
    let patch = bodies
        .iter()
        .find(|b| b.method == "PATCH")
        .expect("the reconciler patches /status");
    let v: serde_json::Value =
        serde_json::from_str(&patch.body).expect("a recorded PATCH body is JSON");
    v["status"].clone()
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
    let slot = slot_name(utc(2026, 9, 7, 3, 17));
    let expected = scheduled_backup_name("nightly", &slot).expect("the fixture name fits");

    // FIRST RECONCILE: the create succeeds, the status write does not.
    let (client, _calls, bodies) = mock_client_recording_bodies(vec![
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
    let first = reconcile_schedule(&schedule, &client, utc(2026, 9, 7, 3, 17)).await;
    assert!(
        first.is_err(),
        "the status write was answered 500, so this reconcile must report a failure and be \
         requeued — swallowing it would lose the record of the fire. Got: {first:?}"
    );
    let first_posts: Vec<String> = posts(&bodies.lock().expect("the body recorder is readable"))
        .iter()
        .map(|b| body_name(b))
        .collect();
    assert_eq!(
        first_posts,
        vec![expected.clone()],
        "the first reconcile POSTs exactly one Backup, named from the trigger"
    );

    // SECOND RECONCILE, at a LATER instant inside the same slot: the create
    // collides.
    let (client, _calls, bodies) = mock_client_recording_bodies(vec![
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
    let second = reconcile_schedule(&schedule, &client, utc(2026, 9, 7, 3, 44))
        .await
        .expect(
            "409 AlreadyExists IS the idempotence key, so the second reconcile must return Ok. \
             An Err here means a duplicate reconcile is reported as a failure and the schedule \
             never records its fire.",
        );
    let second_posts: Vec<String> = posts(&bodies.lock().expect("the body recorder is readable"))
        .iter()
        .map(|b| body_name(b))
        .collect();
    assert_eq!(
        second_posts,
        vec![expected.clone()],
        "the second reconcile POSTs exactly one Backup"
    );

    // THE PROPERTY. Two POSTs across the two reconciles, both carrying the
    // identical metadata.name.
    let all: Vec<String> = first_posts.into_iter().chain(second_posts).collect();
    assert_eq!(
        all.len(),
        2,
        "exactly two POST …/backups requests were made across the two reconciles; got {all:?}"
    );
    assert_eq!(
        all[0], all[1],
        "BOTH POSTs must carry the identical metadata.name — that identity is what makes the \
         second one a 409 and therefore what makes exactly one object exist. Two different \
         names is a name derived from a reconcile-time clock or from status.lastFireTime, and \
         it produces TWO partial archives under colliding backup_ids."
    );
    assert_eq!(
        all[0], expected,
        "the name is `logweir-backup-<schedule>-<slot>` computed from the fired slot"
    );
    assert_eq!(
        second,
        ScheduleOutcome {
            decision: SlotDecision::Due {
                due: utc(2026, 9, 7, 3, 17),
                slot: slot.clone(),
                name: expected.clone(),
                next_fire_time: Some(utc(2026, 9, 14, 3, 17)),
            },
            created: Some(expected),
            already_existed: true,
        },
        "the second reconcile reports the collision as `already_existed`, not as an error"
    );
}

// ---------------------------------------------------------------------------
// The name
// ---------------------------------------------------------------------------

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
    let due_at = decide_body
        .find("last_fire_at_or_before(now)")
        .expect("`decide` must take the due slot from `last_fire_at_or_before`");
    assert!(
        due_at < slot_at,
        "the due slot comes from `last_fire_at_or_before`, then the slot string, then the name"
    );
    for forbidden in ["Utc::now()", "lastFireTime", "last_fire_time", ".status"] {
        assert!(
            !decide_body.contains(forbidden),
            "`decide` must not name {forbidden:?}: a name derived from a reconcile-time clock \
             or from status.lastFireTime produces TWO objects when the controller crashes \
             between the create and the status write"
        );
    }

    let reconcile_body = fn_body(&src, "pub async fn reconcile_schedule(");
    assert!(
        !reconcile_body.contains("Utc::now()"),
        "`reconcile_schedule` — the half that talks to the API server — must contain no \
         `Utc::now()` at all: the instant arrives as an argument, decided once before the \
         reconcile begins. A clock read between `decide` and the POST is the mutant G-SLOT \
         kills."
    );
    let decide_call = reconcile_body
        .find("decide(&name, &schedule.spec, now)")
        .expect("`reconcile_schedule` must call `decide` with the instant it was handed");
    let post = reconcile_body
        .find(".create(&PostParams::default(), &backup)")
        .expect("`reconcile_schedule` must POST the Backup with `Api::create`");
    assert!(
        decide_call < post,
        "the decision — and therefore the name — precedes the POST"
    );
    let status_write = reconcile_body
        .find("patch_status(")
        .expect("`reconcile_schedule` must patch /status");
    assert!(
        post < status_write,
        "the status write happens AFTER the create. That ordering is what makes the crash \
         window harmless: a crash between the two recomputes the same name and collides, \
         where a status-first order would record a fire that never happened."
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
        wrapper.contains("reconcile_schedule(&schedule, &ctx.client, Utc::now())"),
        "the one clock read is the wrapper's, and it is handed straight to \
         `reconcile_schedule` as an argument"
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
    let refine_body = fn_body(&src, "pub fn refine_against_last_fire(");
    for forbidden in [
        "scheduled_backup_name",
        "slot_name",
        "SlotDecision::Due",
        "Utc::now()",
    ] {
        assert!(
            !refine_body.contains(forbidden),
            "`refine_against_last_fire` must not name {forbidden:?}: it refines a REASON \
             against `status.lastFireTime`, and a name that read the status would produce \
             two objects when the controller crashes between the create and the status write"
        );
    }
    assert!(
        refine_body.contains("SlotDecision::AlreadyFired"),
        "the one variant it produces is `AlreadyFired`, out of `Missed`"
    );
    let decide_at = src
        .find("pub fn decide(")
        .expect("`decide` is in this file");
    let refine_at = src
        .find("pub fn refine_against_last_fire(")
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
            "slot {slot} is older than the one-hour missed-slot horizon and was not fired; the \
             next firing is 2026-09-14T03:17:00Z"
        )),
        "the message names the slot, the horizon, the fact it was NOT fired, and when the \
         schedule resumes. Got: {condition}"
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
    let decision = refine_against_last_fire(
        decide("nightly", &stale.spec, now),
        stale.status.as_ref().and_then(|s| s.last_fire_time),
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
    for (label, now) in [
        ("the 30 s requeue", requeue),
        ("+12 h, a different message", utc(2026, 9, 10, 12, 0)),
    ] {
        let mut again = schedule("nightly", UID, DAILY, false);
        again.status = Some(stored.clone());
        let (client, _calls, bodies) = mock_client_recording_bodies(daily_routes(&name, 409));
        reconcile_schedule(&again, &client, now)
            .await
            .unwrap_or_else(|e| panic!("{label}: {e}"));
        let condition = patched_status(&bodies.lock().expect("readable"))["conditions"][0].clone();
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
    for (label, now, expected_posts) in [
        ("+5 min", utc(2026, 9, 10, 0, 5), 1),
        ("+12 h", utc(2026, 9, 10, 12, 0), 0),
        ("+23 h 59 min", utc(2026, 9, 10, 23, 59), 0),
    ] {
        let mut later = schedule("nightly", UID, DAILY, false);
        later.status = Some(stored.clone());
        // 409, because the midnight Backup EXISTS. A POST route is present in
        // every arm, including the two that make none: a reconcile that
        // declined to create proves more than one that could not.
        let (client, calls, bodies) = mock_client_recording_bodies(daily_routes(&name, 409));
        let outcome = reconcile_schedule(&later, &client, now)
            .await
            .unwrap_or_else(|e| panic!("{label}: a healthy schedule is not an error: {e}"));

        let seen = calls.lock().expect("the recorder is readable").clone();
        assert_eq!(
            seen.iter().filter(|c| c.method == "POST").count(),
            expected_posts,
            "{label}: the slot was already fired, so the only POST is the one still inside \
             the horizon (whose 409 is the idempotence key). Calls: {seen:?}"
        );
        assert_eq!(
            outcome.decision.reason(),
            REASON_SCHEDULED,
            "{label}: a schedule that fired its slot is Scheduled, not SlotMissed: {:?}",
            outcome.decision
        );

        let status = patched_status(&bodies.lock().expect("the body recorder is readable"));
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
            refine_against_last_fire(due.clone(), last),
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
    // archive: the same name, the same slot, two namespaces.
    let one = schedule("nightly", UID, "17 3 * * 1", false);
    let two = schedule("nightly", OTHER_UID, "17 3 * * 1", false);
    let name = scheduled_backup_name("nightly", &slot).unwrap();
    let argv_of = |s: &BackupSchedule, uid: &str| {
        scheduled_backup(s, uid, &slot, &name)
            .metadata
            .annotations
            .expect("the created Backup carries the runner argv")[RUNNER_ARGV_ANNOTATION]
            .clone()
    };
    assert_ne!(argv_of(&one, UID), argv_of(&two, OTHER_UID));
}

/// The `--backup-id-override` flag is PASSED here, not defined here.
///
/// INTERFACE **I10** IS TASK 4'S. The flag lives on `logweir backup run`; this
/// task writes it into the runner argv it puts on the `Backup` it creates, and
/// adds no file under `crates/logweir/` at all (critique B H10(c): the first
/// draft mandated a new CLI flag from a task whose Files block names no
/// `crates/logweir/` file, on a chain it does not own).
#[test]
fn the_backup_id_override_is_passed_not_defined() {
    let slot = slot_name(utc(2026, 9, 7, 14, 5));
    let name = scheduled_backup_name("nightly", &slot).unwrap();
    let schedule = schedule("nightly", UID, "5 14 * * *", false);
    let backup = scheduled_backup(&schedule, UID, &slot, &name);

    let annotations = backup
        .metadata
        .annotations
        .clone()
        .expect("the created Backup carries annotations");
    let argv: Vec<String> = serde_json::from_str(&annotations[RUNNER_ARGV_ANNOTATION])
        .expect("the runner argv annotation is a JSON array of argv tokens");

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
    assert_eq!(argv, runner_argv(&backup_id_for(UID, &slot)));
    assert_eq!(
        argv[0], "backup",
        "the argv's first token names the `logweir` subcommand `backup`, which Global \
         Constraint 3 as revised by Task 1 admits"
    );

    // The rest of the object Task 17 reads by name.
    assert_eq!(backup.metadata.name.as_deref(), Some(name.as_str()));
    assert_eq!(backup.spec.slot.as_deref(), Some(slot.as_str()));
    assert_eq!(backup.spec.triggered_by, TRIGGERED_BY_SCHEDULE);
    assert_eq!(
        backup.spec.schedule_ref.as_ref().map(|r| r.name.as_str()),
        Some("nightly")
    );
    let owner = &backup
        .metadata
        .owner_references
        .as_ref()
        .expect("a scheduled Backup is owned by its schedule")[0];
    assert_eq!(owner.kind, "BackupSchedule");
    assert_eq!(owner.api_version, "logweir.dev/v1alpha1");
    assert_eq!(owner.uid, UID);
    assert_eq!(owner.controller, Some(true));
    assert_eq!(owner.block_owner_deletion, Some(true));
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
    //    `runner_argv`'s token array. Counted over CODE lines only, the way
    //    `the_name_never_reads_a_reconcile_clock_or_a_status` counts clock
    //    reads: this module's documentation names the flag three times, to say
    //    whose it is, and a count including prose would be a count of comments.
    for (path, text) in &sources {
        let code = text
            .lines()
            .filter(|l| {
                let t = l.trim_start();
                !t.starts_with("//") && t.contains(&flag)
            })
            .count();
        let expected = usize::from(path == "controllers/backup_schedule.rs");
        assert_eq!(
            code, expected,
            "crates/weirkeeper/src/{path} names {flag} on {code} code line(s); expected \
             {expected}. The one permitted occurrence is the token in `runner_argv`"
        );
    }
    let src = source_of("src/controllers/backup_schedule.rs");
    assert!(
        fn_body(&src, "pub fn runner_argv(").contains(&flag),
        "that one occurrence is inside `runner_argv`, where it is an argv TOKEN"
    );

    // 4. And the reconciler reaches the CLI through that argv string, never
    //    through a Rust path.
    assert!(
        !src.contains("logweir::backup") && !src.contains("crate::backup"),
        "the reconciler reaches the CLI through an argv string and never through a Rust path"
    );
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

    // The route table has no bare-object route and no DELETE route, so the
    // double PANICS on either — see `src/testing.rs` for why a panic and not a
    // 404.
    let (client, calls) = mock_client_recording(vec![
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
            body: patched_schedule_body(),
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

/// The CRD field descriptions state the missed-slot horizon.
///
/// The horizon reaches `docs/kubernetes.md` through these descriptions and
/// through Task 19's chain-W slot; this task's obligation is that
/// `kubectl explain` says it.
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
        "ONE-HOUR missed-slot horizon",
    ] {
        assert!(
            flat.contains(needle),
            "config/crd/backupschedules.yaml must state {needle:?} so `kubectl explain` does \
             too — re-render with `just crds` after editing crds/backup_schedule.rs"
        );
    }
}
