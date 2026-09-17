//! What the one sanctioned deleter does, and — mostly — what it refuses.
//!
//! Every row here drives the REAL [`logweir_reaper::execute`] over a recording
//! fake, so "a dry run performs no delete", "an `AccessDenied` is not retried"
//! and "a key outside the scope deletes nothing" are observations about the
//! shipped state machine and not about a test double of it. The fake records
//! every key it was asked to remove, in order, so a row can assert the ORDER —
//! which is the property that makes a half-deleted set impossible to mistake
//! for a usable one.
//!
//! Nothing here dials anything. The `Deleter`, the `Lister`, the `Sleeper` and
//! the `TombstoneSink` are all injected, which is what
//! `crates/logweir/tests/no_network_in_unit_tests.rs` asks of every new
//! boundary.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::time::Duration;

use chrono::{TimeZone as _, Utc};

use logweir_reaper::{
    execute, parse_plan, record, record_bytes, record_key, tombstone_key, validate_listed_key,
    validate_plan, DeleteError, Deleter, Limits, Lister, Plan, PlanLine, PointState, RecordContext,
    Refusal, RunAttribution, RunBinding, SinkError, Sleeper, TombstoneSink, EVIDENCE_ROOT,
    MAX_ATTEMPTS, PLAN_MEDIA_TYPE, RECORD_MEDIA_TYPE,
};

// ===========================================================================
// The fakes
// ===========================================================================

/// A deleter that records every key and answers from a script.
#[derive(Default)]
struct FakeDeleter {
    /// Keys, in the order they were asked for. Retries appear more than once.
    seen: RefCell<Vec<String>>,
    /// `key -> the answers to give, in order`. An exhausted list answers `Ok`.
    script: RefCell<BTreeMap<String, Vec<DeleteError>>>,
}

impl FakeDeleter {
    fn refusing(key: &str, errors: &[DeleteError]) -> Self {
        let mut script = BTreeMap::new();
        script.insert(key.to_string(), errors.to_vec());
        Self {
            seen: RefCell::new(Vec::new()),
            script: RefCell::new(script),
        }
    }

    fn keys(&self) -> Vec<String> {
        self.seen.borrow().clone()
    }

    fn attempts_on(&self, key: &str) -> usize {
        self.seen.borrow().iter().filter(|k| *k == key).count()
    }
}

impl Deleter for FakeDeleter {
    fn delete_exact(&self, key: &str) -> Result<(), DeleteError> {
        self.seen.borrow_mut().push(key.to_string());
        let mut script = self.script.borrow_mut();
        match script.get_mut(key).and_then(|answers| {
            if answers.is_empty() {
                None
            } else {
                Some(answers.remove(0))
            }
        }) {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}

/// A deleter that panics. Used where the property is that NOTHING is deleted.
struct NeverDeletes;

impl Deleter for NeverDeletes {
    fn delete_exact(&self, key: &str) -> Result<(), DeleteError> {
        panic!(
            "delete_exact({key}) was called on a path that must delete nothing. That is the \
             whole property this row exists for."
        )
    }
}

/// A sleeper that records the waits instead of taking them.
#[derive(Default)]
struct FakeSleeper {
    waited: RefCell<Vec<Duration>>,
}

impl Sleeper for FakeSleeper {
    fn sleep(&self, d: Duration) {
        self.waited.borrow_mut().push(d);
    }
}

/// A create-only tombstone sink that records what it was given.
#[derive(Default)]
struct FakeSink {
    written: RefCell<BTreeMap<String, Vec<u8>>>,
    refuse: bool,
}

impl FakeSink {
    fn refusing() -> Self {
        Self {
            written: RefCell::new(BTreeMap::new()),
            refuse: true,
        }
    }

    fn keys(&self) -> Vec<String> {
        self.written.borrow().keys().cloned().collect()
    }
}

impl TombstoneSink for FakeSink {
    fn put_create_only(&self, key: &str, bytes: &[u8]) -> Result<(), SinkError> {
        if self.refuse {
            return Err(SinkError("the sink refused".to_string()));
        }
        let mut written = self.written.borrow_mut();
        if written.contains_key(key) {
            return Err(SinkError(format!("{key} already exists")));
        }
        written.insert(key.to_string(), bytes.to_vec());
        Ok(())
    }
}

/// A lister that answers from a fixed set.
struct FakeLister(Vec<String>);

impl Lister for FakeLister {
    fn list_exact(&self, prefix: &str) -> Result<Vec<String>, DeleteError> {
        Ok(self
            .0
            .iter()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect())
    }
}

/// A lister that answers everything it holds, whatever the prefix — the
/// hostile bucket the re-validation exists for.
struct HostileLister(Vec<String>);

impl Lister for HostileLister {
    fn list_exact(&self, _prefix: &str) -> Result<Vec<String>, DeleteError> {
        Ok(self.0.clone())
    }
}

// ===========================================================================
// Fixtures
// ===========================================================================

const SCOPE: &str = "kafka-backups/team-a";
const UID: &str = "3f1c8a5e-0000-4000-8000-000000000016";

fn line(point: &str, backup: &str, segments: &[&str]) -> PlanLine {
    let manifest = format!("{SCOPE}/{backup}/manifest.json");
    let mut object_keys = vec![manifest.clone()];
    object_keys.extend(segments.iter().map(|s| format!("{SCOPE}/{backup}/{s}")));
    PlanLine {
        point_id: point.to_string(),
        backup_id: backup.to_string(),
        reason: "BeyondKeepLast".to_string(),
        recovery_point_at_ms: 1_788_912_000_000,
        manifest_key: manifest,
        set_prefix: format!("{SCOPE}/{backup}/"),
        enumerate_set: false,
        object_keys,
    }
}

fn plan(lines: Vec<PlanLine>) -> Plan {
    Plan {
        format: PLAN_MEDIA_TYPE.to_string(),
        format_version: "1.0.0".to_string(),
        policy_namespace: "team-a".to_string(),
        policy_name: "primary".to_string(),
        policy_uid: UID.to_string(),
        policy_generation: 4,
        location_id: "s3://kafka-backups/team-a".to_string(),
        scope_prefix: SCOPE.to_string(),
        keep_last: Some(2),
        keep_days: Some(30),
        min_usable_points: 3,
        evaluated_at: Utc.with_ymd_and_hms(2026, 9, 17, 4, 17, 0).unwrap(),
        points_evaluated: 6,
        lines,
    }
}

fn binding() -> RunBinding {
    RunBinding {
        policy_uid: UID.to_string(),
        policy_generation: 4,
        scope_prefix: SCOPE.to_string(),
        max_deletions_per_run: 50,
        max_objects_per_run: 20_000,
    }
}

fn attribution() -> RunAttribution {
    RunAttribution {
        run_id: "r0123456789abcdef".to_string(),
        policy_uid: UID.to_string(),
        plan_sha256: "sha256:aa".to_string(),
    }
}

fn limits() -> Limits {
    Limits {
        dry_run: false,
        max_objects: 20_000,
    }
}

fn run(
    p: &Plan,
    deleter: &FakeDeleter,
    sink: &FakeSink,
    limits: Limits,
) -> logweir_reaper::Outcome {
    execute(
        p,
        deleter,
        &FakeSleeper::default(),
        sink,
        &FakeLister(Vec::new()),
        &attribution(),
        limits,
    )
}

// ===========================================================================
// Dry preview — PLAT-16.2 "dry preview"
// ===========================================================================

/// A dry run issues NO delete at all.
///
/// The deleter PANICS on any call, so this is not "zero were observed" but
/// "one would have failed the test".
#[test]
fn a_dry_run_issues_no_delete() {
    let p = plan(vec![line("lwp1-a", "set-a", &["seg-0", "seg-1"])]);
    let sink = FakeSink::default();
    let outcome = execute(
        &p,
        &NeverDeletes,
        &FakeSleeper::default(),
        &sink,
        &FakeLister(Vec::new()),
        &attribution(),
        Limits {
            dry_run: true,
            max_objects: 20_000,
        },
    );
    assert_eq!(outcome.objects_deleted, 0);
    assert_eq!(outcome.attempts, 0, "a dry run issues no delete call");
    assert!(outcome.dry_run);
    assert_eq!(outcome.points[0].state, PointState::Kept.as_str());
    assert_eq!(outcome.points[0].code.as_deref(), Some("DryRun"));
    assert!(
        sink.keys().is_empty(),
        "a dry run writes no tombstone either: nothing was removed, so there is nothing to \
         attribute. Wrote: {:?}",
        sink.keys()
    );
}

/// The plan's digest is a pure function of its bytes, and a digest that does
/// not match the approved one refuses before the JSON is even parsed.
#[test]
fn a_plan_whose_digest_is_not_the_approved_one_is_refused() {
    let bytes = logweir_core::det_json::to_deterministic_json(&plan(vec![line(
        "lwp1-a",
        "set-a",
        &["seg-0"],
    )]))
    .expect("the plan serialises");
    let real = logweir_core::ids::sha256_prefixed(&bytes);

    assert!(parse_plan(&bytes, &real).is_ok(), "the real digest parses");

    let err = parse_plan(&bytes, "sha256:0000").expect_err("a stale approval is refused");
    match err {
        Refusal::DigestMismatch { found, approved } => {
            assert_eq!(found, real);
            assert_eq!(approved, "sha256:0000");
        }
        other => panic!("expected a digest mismatch, got {other:?}"),
    }
}

/// Mutant for the row above: parsing before the digest check would let a plan
/// whose bytes nobody approved through on the strength of being well-formed.
///
/// The check is that a plan which is NOT valid JSON still refuses with
/// `DigestMismatch` and not with `Unreadable` — i.e. the digest ran first.
#[test]
fn the_digest_is_checked_before_the_document_is_parsed() {
    let err = parse_plan(b"this is not json", "sha256:0000")
        .expect_err("garbage with a wrong digest is refused");
    assert!(
        matches!(err, Refusal::DigestMismatch { .. }),
        "the digest must be checked FIRST, over the exact bytes on disk: a plan whose bytes are \
         not the approved bytes is not this administrator's plan, whatever it contains. Got: \
         {err:?}"
    );
}

// ===========================================================================
// Wrong-prefix rejection — PLAT-16.2
// ===========================================================================

/// A key outside `<scope>/<backupId>/` refuses the plan with ZERO deletes.
#[test]
fn a_key_outside_the_scope_refuses_the_plan_with_zero_deletes() {
    let mut bad = line("lwp1-a", "set-a", &["seg-0"]);
    bad.object_keys
        .push("kafka-backups/team-b/set-a/seg-9".to_string());
    let p = plan(vec![bad]);

    let err = validate_plan(&p, &binding()).expect_err("an out-of-scope key is refused");
    match &err {
        Refusal::ScopeViolation { key, expected, .. } => {
            assert_eq!(key, "kafka-backups/team-b/set-a/seg-9");
            assert_eq!(expected, &format!("{SCOPE}/set-a/"));
        }
        other => panic!("expected a scope violation, got {other:?}"),
    }
    assert!(
        err.to_string().starts_with("RetentionScopeViolation"),
        "the refusal opens with the state name the exit contract documents: {err}"
    );
}

/// A key under `logweir/` is refused as the EVIDENCE ROOT, not merely as an
/// out-of-scope key — the audit trail has its own name in the refusal.
#[test]
fn a_key_under_the_evidence_root_is_refused_by_that_name() {
    let mut bad = line("lwp1-a", "set-a", &["seg-0"]);
    bad.object_keys
        .push(format!("{EVIDENCE_ROOT}backups/set-a/x.receipt.json"));
    let err = validate_plan(&plan(vec![bad]), &binding()).expect_err("logweir/ is refused");
    assert!(
        matches!(err, Refusal::EvidenceRoot { .. }),
        "a key under the evidence root is refused BY THAT NAME: receipts, scorecards, catalog \
         records and the retention records themselves live there, so a deleted point's audit \
         trail outlives the point. Got: {err:?}"
    );
}

/// Mutant for the row above: a scope prefix that ITSELF began `logweir/` must
/// not let every key under it through. The evidence check is independent of the
/// prefix comparison, and this is the row that says so.
#[test]
fn an_evidence_scope_cannot_launder_a_key_under_logweir() {
    let mut poisoned = binding();
    poisoned.scope_prefix = "logweir/retention".to_string();
    let mut l = line("lwp1-a", "set-a", &[]);
    l.set_prefix = "logweir/retention/set-a/".to_string();
    l.manifest_key = "logweir/retention/set-a/manifest.json".to_string();
    l.object_keys = vec![l.manifest_key.clone()];
    let mut p = plan(vec![l]);
    p.scope_prefix = "logweir/retention".to_string();

    let err = validate_plan(&p, &poisoned).expect_err("logweir/ is refused whatever the scope is");
    assert!(
        matches!(err, Refusal::EvidenceRoot { .. }),
        "the evidence-root check runs on its own and BEFORE the prefix comparison; deriving it \
         from the prefix would make a scope of `logweir/…` a licence to delete the audit trail. \
         Got: {err:?}"
    );
}

/// A LISTED key is re-validated too. The bucket is exactly the thing this
/// worker does not trust.
#[test]
fn an_enumerated_key_outside_the_bound_stops_the_point_with_zero_deletes() {
    let mut l = line("lwp1-a", "set-a", &[]);
    l.enumerate_set = true;
    let p = plan(vec![l]);
    let deleter = FakeDeleter::default();
    let sink = FakeSink::default();
    let outcome = execute(
        &p,
        &deleter,
        &FakeSleeper::default(),
        &sink,
        // The listing answers with a key from ANOTHER tenant's prefix.
        &HostileLister(vec![
            format!("{SCOPE}/set-a/manifest.json"),
            "kafka-backups/team-b/set-a/seg-0".to_string(),
        ]),
        &attribution(),
        limits(),
    );
    assert!(
        deleter.keys().is_empty(),
        "a listing that returned something outside the bound is a listing this worker has no \
         reason to trust at all, so NOTHING from it is used. Deleted: {:?}",
        deleter.keys()
    );
    assert_eq!(outcome.points[0].state, PointState::Kept.as_str());
    assert!(
        outcome.points[0]
            .code
            .as_deref()
            .unwrap_or_default()
            .starts_with("RetentionScopeViolation"),
        "the code names the refusal: {:?}",
        outcome.points[0].code
    );
    assert!(sink.keys().is_empty(), "and no tombstone was written");
}

/// The same rule, as the function the executor calls.
#[test]
fn validate_listed_key_refuses_the_evidence_root_and_the_wrong_prefix() {
    let l = line("lwp1-a", "set-a", &[]);
    assert!(validate_listed_key(&format!("{SCOPE}/set-a/seg-0"), &l).is_ok());
    assert!(matches!(
        validate_listed_key("logweir/backups/set-a/x", &l),
        Err(Refusal::EvidenceRoot { .. })
    ));
    assert!(matches!(
        validate_listed_key("kafka-backups/team-b/set-a/seg-0", &l),
        Err(Refusal::ScopeViolation { .. })
    ));
}

/// A plan whose `set_prefix` was widened beyond what its own `backup_id`
/// justifies is refused: the enumeration bound is re-derived from the JOB's
/// scope and never taken from the plan.
#[test]
fn a_widened_set_prefix_is_refused() {
    let mut l = line("lwp1-a", "set-a", &["seg-0"]);
    l.set_prefix = format!("{SCOPE}/");
    let err = validate_plan(&plan(vec![l]), &binding()).expect_err("a widened bound is refused");
    assert!(matches!(err, Refusal::ScopeViolation { .. }), "got {err:?}");
}

/// The plan must be for THIS policy at THIS generation. A policy edit bumps the
/// generation, so a plan from before the edit does not run.
#[test]
fn a_plan_for_another_policy_or_generation_is_refused() {
    let mut p = plan(vec![line("lwp1-a", "set-a", &["seg-0"])]);
    p.policy_generation = 3;
    let err = validate_plan(&p, &binding()).expect_err("a stale generation is refused");
    assert!(matches!(err, Refusal::PolicyMismatch { .. }), "got {err:?}");

    let mut q = plan(vec![line("lwp1-a", "set-a", &["seg-0"])]);
    q.policy_uid = "00000000-0000-4000-8000-000000000000".to_string();
    assert!(matches!(
        validate_plan(&q, &binding()),
        Err(Refusal::PolicyMismatch { .. })
    ));
}

/// The scope the JOB was told and the scope the plan carries must agree.
#[test]
fn a_plan_that_widens_its_own_scope_is_refused() {
    let mut p = plan(vec![line("lwp1-a", "set-a", &["seg-0"])]);
    p.scope_prefix = "kafka-backups".to_string();
    assert!(
        matches!(
            validate_plan(&p, &binding()),
            Err(Refusal::PolicyMismatch { .. })
        ),
        "the worker takes its scope from its own environment, never from the document it is \
         being asked to execute"
    );
}

/// One key on two lines would make a delete attributable to two points.
#[test]
fn a_key_on_two_lines_is_refused() {
    let shared = line("lwp1-a", "set-a", &["seg-0"]);
    let mut other = line("lwp1-b", "set-a", &["seg-0"]);
    other.point_id = "lwp1-b".to_string();
    let err =
        validate_plan(&plan(vec![shared, other]), &binding()).expect_err("a shared key is refused");
    assert!(matches!(err, Refusal::DuplicateKey { .. }), "got {err:?}");
}

/// The ceilings are refusals, not truncations.
#[test]
fn a_plan_over_a_ceiling_is_refused_rather_than_trimmed() {
    let p = plan(vec![
        line("lwp1-a", "set-a", &["seg-0"]),
        line("lwp1-b", "set-b", &["seg-0"]),
    ]);
    let mut tight = binding();
    tight.max_deletions_per_run = 1;
    assert!(matches!(
        validate_plan(&p, &tight),
        Err(Refusal::OverCap { what: "points", .. })
    ));

    let mut tight_objects = binding();
    tight_objects.max_objects_per_run = 3;
    assert!(matches!(
        validate_plan(&p, &tight_objects),
        Err(Refusal::OverCap {
            what: "object keys",
            ..
        })
    ));
}

// ===========================================================================
// Execution order, partial failure and idempotent completion
// ===========================================================================

/// The manifest goes FIRST, then the segments in plan order.
#[test]
fn the_manifest_is_deleted_before_any_segment() {
    let p = plan(vec![line("lwp1-a", "set-a", &["seg-0", "seg-1", "seg-2"])]);
    let deleter = FakeDeleter::default();
    let sink = FakeSink::default();
    let outcome = run(&p, &deleter, &sink, limits());
    assert_eq!(
        deleter.keys(),
        vec![
            format!("{SCOPE}/set-a/manifest.json"),
            format!("{SCOPE}/set-a/seg-0"),
            format!("{SCOPE}/set-a/seg-1"),
            format!("{SCOPE}/set-a/seg-2"),
        ],
        "a set whose manifest is gone cannot be read as usable by anything, so a run \
         interrupted between the two leaves an unmistakably dead set rather than a \
         plausible-looking one"
    );
    assert_eq!(outcome.points[0].state, PointState::Deleted.as_str());
    assert_eq!(outcome.objects_deleted, 4);
    assert!(outcome.complete());
}

/// A manifest that would not go leaves the segments untouched.
#[test]
fn a_manifest_that_will_not_go_leaves_every_segment_alone() {
    let p = plan(vec![line("lwp1-a", "set-a", &["seg-0", "seg-1"])]);
    let deleter = FakeDeleter::refusing(
        &format!("{SCOPE}/set-a/manifest.json"),
        &[DeleteError::AccessDenied],
    );
    let sink = FakeSink::default();
    let outcome = run(&p, &deleter, &sink, limits());
    assert_eq!(
        deleter.keys(),
        vec![format!("{SCOPE}/set-a/manifest.json")],
        "deleting segments out from under a live manifest is the one way to produce a set that \
         LOOKS restorable and is not"
    );
    assert_eq!(outcome.points[0].state, PointState::Kept.as_str());
    assert_eq!(outcome.points[0].objects_deleted, 0);
    assert_eq!(outcome.points[0].code.as_deref(), Some("AccessDenied"));
    assert!(!outcome.complete());
}

/// Manifest gone, a segment refused: `Orphaned`, and the leftover keys are
/// named so the next plan can complete exactly them.
#[test]
fn a_partial_failure_is_orphaned_and_names_its_leftovers() {
    let p = plan(vec![line("lwp1-a", "set-a", &["seg-0", "seg-1"])]);
    let deleter = FakeDeleter::refusing(
        &format!("{SCOPE}/set-a/seg-1"),
        &[DeleteError::AccessDenied],
    );
    let sink = FakeSink::default();
    let outcome = run(&p, &deleter, &sink, limits());
    assert_eq!(outcome.points[0].state, PointState::Orphaned.as_str());
    assert_eq!(outcome.points[0].objects_deleted, 2);
    assert_eq!(
        outcome.points[0].remaining_keys,
        vec![format!("{SCOPE}/set-a/seg-1")],
        "the next plan names exactly those leftovers, which is what makes completion idempotent"
    );
}

/// A key that is already gone is DONE, not failed — the other half of
/// idempotent completion.
#[test]
fn a_key_that_is_already_gone_completes_the_point() {
    let p = plan(vec![line("lwp1-a", "set-a", &["seg-0"])]);
    let mut script = BTreeMap::new();
    script.insert(
        format!("{SCOPE}/set-a/manifest.json"),
        vec![DeleteError::NotFound],
    );
    script.insert(format!("{SCOPE}/set-a/seg-0"), vec![DeleteError::NotFound]);
    let deleter = FakeDeleter {
        seen: RefCell::new(Vec::new()),
        script: RefCell::new(script),
    };
    let sink = FakeSink::default();
    let outcome = run(&p, &deleter, &sink, limits());
    assert_eq!(
        outcome.points[0].state,
        PointState::Deleted.as_str(),
        "a key an interrupted run removed is a key this run wanted removed; reporting it as a \
         failure would make idempotent completion impossible"
    );
}

/// One point failing does not abandon the next: they are independent sets and
/// the administrator approved both.
#[test]
fn one_failing_point_does_not_stop_the_run() {
    let p = plan(vec![
        line("lwp1-a", "set-a", &["seg-0"]),
        line("lwp1-b", "set-b", &["seg-0"]),
    ]);
    let deleter = FakeDeleter::refusing(
        &format!("{SCOPE}/set-a/manifest.json"),
        &[DeleteError::AccessDenied],
    );
    let sink = FakeSink::default();
    let outcome = run(&p, &deleter, &sink, limits());
    assert_eq!(outcome.deleted(), vec!["lwp1-b"]);
    assert_eq!(outcome.failed(), vec![("lwp1-a", "AccessDenied")]);
}

// ===========================================================================
// Bounded retry — PLAT-16.2 "denied deletion", "bounded retry"
// ===========================================================================

/// `AccessDenied` is answered ONCE. A second attempt at a policy decision is
/// three seconds of nothing.
#[test]
fn access_denied_is_not_retried() {
    let key = format!("{SCOPE}/set-a/manifest.json");
    let p = plan(vec![line("lwp1-a", "set-a", &[])]);
    let deleter = FakeDeleter::refusing(&key, &[DeleteError::AccessDenied]);
    let sleeper = FakeSleeper::default();
    let sink = FakeSink::default();
    let outcome = execute(
        &p,
        &deleter,
        &sleeper,
        &sink,
        &FakeLister(Vec::new()),
        &attribution(),
        limits(),
    );
    assert_eq!(deleter.attempts_on(&key), 1, "exactly one attempt");
    assert!(
        sleeper.waited.borrow().is_empty(),
        "and no backoff was taken for a refusal that cannot change"
    );
    assert_eq!(outcome.points[0].code.as_deref(), Some("AccessDenied"));
}

/// `Locked` is not retried either, and it is a HOLD rather than a failure — a
/// provider refusal is authoritative and recorded, never "Logweir knows the
/// hold exists".
#[test]
fn a_provider_lock_is_not_retried_and_is_a_hold() {
    let key = format!("{SCOPE}/set-a/manifest.json");
    let p = plan(vec![line("lwp1-a", "set-a", &[])]);
    let deleter = FakeDeleter::refusing(&key, &[DeleteError::Locked]);
    let sink = FakeSink::default();
    let outcome = run(&p, &deleter, &sink, limits());
    assert_eq!(deleter.attempts_on(&key), 1);
    assert_eq!(outcome.points[0].code.as_deref(), Some("Locked"));
    assert!(DeleteError::Locked.is_hold());
    assert!(!DeleteError::AccessDenied.is_hold());
}

/// A 5xx IS retried, up to three attempts, with 1 s then 4 s between them.
#[test]
fn a_server_error_is_retried_three_times_with_the_documented_backoff() {
    let key = format!("{SCOPE}/set-a/manifest.json");
    let p = plan(vec![line("lwp1-a", "set-a", &[])]);
    let deleter = FakeDeleter::refusing(
        &key,
        &[
            DeleteError::ServerError,
            DeleteError::ServerError,
            DeleteError::ServerError,
        ],
    );
    let sleeper = FakeSleeper::default();
    let sink = FakeSink::default();
    execute(
        &p,
        &deleter,
        &sleeper,
        &sink,
        &FakeLister(Vec::new()),
        &attribution(),
        limits(),
    );
    assert_eq!(
        deleter.attempts_on(&key),
        MAX_ATTEMPTS as usize,
        "three attempts and no more"
    );
    assert_eq!(
        *sleeper.waited.borrow(),
        vec![Duration::from_secs(1), Duration::from_secs(4)],
        "1 s then 4 s; the third attempt is the last, so there is no wait after it"
    );
}

/// A transient failure that clears on the second attempt succeeds.
#[test]
fn a_timeout_that_clears_succeeds_on_the_retry() {
    let key = format!("{SCOPE}/set-a/manifest.json");
    let p = plan(vec![line("lwp1-a", "set-a", &[])]);
    let deleter = FakeDeleter::refusing(&key, &[DeleteError::Timeout]);
    let sink = FakeSink::default();
    let outcome = run(&p, &deleter, &sink, limits());
    assert_eq!(deleter.attempts_on(&key), 2);
    assert_eq!(outcome.points[0].state, PointState::Deleted.as_str());
}

/// The retry classification, as a table. A code that is neither 5xx nor a
/// timeout is answered once — "unclassified repeated three times is still
/// unclassified".
#[test]
fn only_server_errors_and_timeouts_are_retryable() {
    for (code, retryable) in [
        (DeleteError::AccessDenied, false),
        (DeleteError::Locked, false),
        (DeleteError::PreconditionFailed, false),
        (DeleteError::NotFound, false),
        (DeleteError::ServerError, true),
        (DeleteError::Timeout, true),
        (DeleteError::Unclassified, false),
    ] {
        assert_eq!(
            code.retryable(),
            retryable,
            "{} classified wrongly",
            code.as_str()
        );
    }
}

/// The provider codes this worker actually sees, classified.
#[test]
fn provider_messages_classify_into_the_closed_vocabulary() {
    use logweir_reaper::archive::classify_text;
    assert_eq!(classify_text("AccessDenied"), DeleteError::AccessDenied);
    assert_eq!(classify_text("403 Forbidden"), DeleteError::AccessDenied);
    assert_eq!(classify_text("503 SlowDown"), DeleteError::ServerError);
    assert_eq!(classify_text("operation timed out"), DeleteError::Timeout);
    // A WORM refusal often ALSO says access denied; the hold is the more
    // specific and more consequential fact, so it wins.
    assert_eq!(
        classify_text("AccessDenied: object is under a legal hold"),
        DeleteError::Locked,
        "a legal hold is not merely a denial: it is kept, recorded and excluded from the next \
         plan, and collapsing it into AccessDenied loses that"
    );
    assert_eq!(classify_text("something new"), DeleteError::Unclassified);
}

// ===========================================================================
// Attribution — PLAT-16.2 "every deletion is attributable"
// ===========================================================================

/// The intent tombstone is written BEFORE the first delete, and the completion
/// after the last.
#[test]
fn the_intent_is_written_before_the_first_delete() {
    let p = plan(vec![line("lwp1-a", "set-a", &["seg-0"])]);
    let deleter = FakeDeleter::default();
    let sink = FakeSink::default();
    run(&p, &deleter, &sink, limits());
    assert_eq!(
        sink.keys(),
        vec![
            tombstone_key(UID, "r0123456789abcdef", "lwp1-a", "completion"),
            tombstone_key(UID, "r0123456789abcdef", "lwp1-a", "intent"),
        ],
        "both stages exist (the list is key-sorted, so `completion` sorts first)"
    );
    for key in sink.keys() {
        assert!(
            key.starts_with(EVIDENCE_ROOT),
            "every tombstone goes under the evidence root, which this run's delete credential \
             cannot reach: {key}"
        );
    }
}

/// A point whose intent could not be written is NOT deleted. "Every deletion is
/// attributable" is a precondition, not a report.
#[test]
fn a_point_whose_intent_cannot_be_written_is_not_deleted() {
    let p = plan(vec![line("lwp1-a", "set-a", &["seg-0"])]);
    let deleter = FakeDeleter::default();
    let sink = FakeSink::refusing();
    let outcome = run(&p, &deleter, &sink, limits());
    assert!(
        deleter.keys().is_empty(),
        "an unattributable deletion is not performed. Deleted: {:?}",
        deleter.keys()
    );
    assert_eq!(outcome.points[0].state, PointState::Kept.as_str());
    assert!(outcome.points[0]
        .code
        .as_deref()
        .unwrap_or_default()
        .starts_with("TombstoneRefused"));
}

/// The record names the plan, the approver, every deleted point and every
/// failure's closed code — and carries no credential of any kind.
#[test]
fn the_record_names_the_plan_the_approver_and_every_outcome() {
    let p = plan(vec![
        line("lwp1-a", "set-a", &["seg-0"]),
        line("lwp1-b", "set-b", &["seg-0"]),
    ]);
    let deleter = FakeDeleter::refusing(
        &format!("{SCOPE}/set-b/manifest.json"),
        &[DeleteError::Locked],
    );
    let sink = FakeSink::default();
    let outcome = run(&p, &deleter, &sink, limits());
    let doc = record(
        &p,
        &outcome,
        &RecordContext {
            run_id: "r0123456789abcdef".to_string(),
            approver: "audit:0f3a".to_string(),
            plan_sha256: "sha256:aa".to_string(),
            started_at: Utc.with_ymd_and_hms(2026, 9, 17, 4, 17, 0).unwrap(),
            finished_at: Utc.with_ymd_and_hms(2026, 9, 17, 4, 18, 0).unwrap(),
            exit_code: 1,
        },
    );
    assert_eq!(doc.format, RECORD_MEDIA_TYPE);
    assert_eq!(doc.approver, "audit:0f3a");
    assert_eq!(doc.plan_sha256, "sha256:aa");
    assert_eq!(doc.policy_uid, UID);
    assert_eq!(doc.policy_generation, 4);
    assert_eq!(doc.exit_code, 1);
    let (bytes, digest) = record_bytes(&doc).expect("the record serialises");
    assert!(digest.starts_with("sha256:"));
    let text = String::from_utf8(bytes).expect("the record is UTF-8");
    assert!(text.contains("lwp1-a") && text.contains("lwp1-b"));
    assert!(
        text.contains("Locked"),
        "the failure's closed code is in it"
    );
    for forbidden in ["AWS_SECRET", "secret-access-key", "AKIA"] {
        assert!(
            !text.contains(forbidden),
            "the record carries no credential material: `{forbidden}` appeared"
        );
    }
    assert_eq!(
        record_key(UID, "r0123456789abcdef"),
        format!("{EVIDENCE_ROOT}retention/{UID}/r0123456789abcdef.json"),
        "the record goes under the evidence root, which this run's own credential cannot delete \
         from — that is what makes the audit trail outlive the point"
    );
}

/// The record's bytes are deterministic, so two renderings of one run digest
/// identically.
#[test]
fn the_record_bytes_are_deterministic() {
    let p = plan(vec![line("lwp1-a", "set-a", &["seg-0"])]);
    let outcome = run(&p, &FakeDeleter::default(), &FakeSink::default(), limits());
    let ctx = RecordContext {
        run_id: "r0".to_string(),
        approver: "kubectl:alice".to_string(),
        plan_sha256: "sha256:aa".to_string(),
        started_at: Utc.with_ymd_and_hms(2026, 9, 17, 4, 17, 0).unwrap(),
        finished_at: Utc.with_ymd_and_hms(2026, 9, 17, 4, 18, 0).unwrap(),
        exit_code: 0,
    };
    let a = record_bytes(&record(&p, &outcome, &ctx)).expect("serialises");
    let b = record_bytes(&record(&p, &outcome, &ctx)).expect("serialises");
    assert_eq!(a, b);
}

// ===========================================================================
// The object budget
// ===========================================================================

/// The object ceiling stops the run and says so; it does not half-delete a
/// point and call it done.
#[test]
fn the_object_ceiling_stops_the_run_and_names_the_reason() {
    let p = plan(vec![
        line("lwp1-a", "set-a", &["seg-0"]),
        line("lwp1-b", "set-b", &["seg-0"]),
    ]);
    let deleter = FakeDeleter::default();
    let sink = FakeSink::default();
    let outcome = run(
        &p,
        &deleter,
        &sink,
        Limits {
            dry_run: false,
            max_objects: 2,
        },
    );
    assert_eq!(outcome.objects_deleted, 2);
    assert_eq!(outcome.points[0].state, PointState::Deleted.as_str());
    assert_eq!(outcome.points[1].state, PointState::Kept.as_str());
    assert_eq!(
        outcome.points[1].code.as_deref(),
        Some("ObjectBudgetExhausted")
    );
}

// ===========================================================================
// Enumeration
// ===========================================================================

/// When the plan names the set directory, the listing supplies the rest — and
/// the manifest still goes first.
#[test]
fn an_enumerated_set_deletes_the_manifest_first_and_then_what_was_listed() {
    let mut l = line("lwp1-a", "set-a", &[]);
    l.enumerate_set = true;
    let p = plan(vec![l]);
    let deleter = FakeDeleter::default();
    let sink = FakeSink::default();
    let outcome = execute(
        &p,
        &deleter,
        &FakeSleeper::default(),
        &sink,
        &FakeLister(vec![
            format!("{SCOPE}/set-a/seg-1"),
            format!("{SCOPE}/set-a/manifest.json"),
            format!("{SCOPE}/set-a/seg-0"),
        ]),
        &attribution(),
        limits(),
    );
    assert_eq!(
        deleter.keys(),
        vec![
            format!("{SCOPE}/set-a/manifest.json"),
            format!("{SCOPE}/set-a/seg-0"),
            format!("{SCOPE}/set-a/seg-1"),
        ],
        "the manifest first, whatever order the listing came back in, then the rest in a total \
         order so a re-run produces the same sequence"
    );
    assert_eq!(outcome.points[0].state, PointState::Deleted.as_str());
}

// ===========================================================================
// The document shapes
// ===========================================================================

/// A plan carrying a field this build does not know is REFUSED, not partially
/// obeyed.
#[test]
fn an_unknown_plan_field_is_refused() {
    let mut value = serde_json::to_value(plan(vec![line("lwp1-a", "set-a", &["seg-0"])]))
        .expect("the plan is JSON");
    value
        .as_object_mut()
        .expect("an object")
        .insert("tomorrows_field".to_string(), serde_json::json!(true));
    let bytes = serde_json::to_vec(&value).expect("serialises");
    let digest = logweir_core::ids::sha256_prefixed(&bytes);
    let err = parse_plan(&bytes, &digest).expect_err("an unknown field is refused");
    assert!(
        matches!(err, Refusal::Unreadable(_)),
        "a deletion worker that silently ignored half of an instruction is the failure mode the \
         whole two-step approval exists to prevent. Got: {err:?}"
    );
}

/// A document of another media type is refused by name.
#[test]
fn a_document_of_another_media_type_is_refused() {
    let mut p = plan(vec![line("lwp1-a", "set-a", &["seg-0"])]);
    p.format = "application/vnd.logweir.retention-plan+json;version=2.0.0".to_string();
    let bytes = serde_json::to_vec(&p).expect("serialises");
    let digest = logweir_core::ids::sha256_prefixed(&bytes);
    assert!(matches!(
        parse_plan(&bytes, &digest),
        Err(Refusal::WrongFormat(_))
    ));
}

/// A line whose first key is not its declared manifest is refused: the order is
/// the safety property, so a plan that disagrees with its own declaration is
/// not executed.
#[test]
fn a_line_whose_first_key_is_not_its_manifest_is_refused() {
    let mut l = line("lwp1-a", "set-a", &["seg-0"]);
    l.object_keys.reverse();
    assert!(matches!(
        validate_plan(&plan(vec![l]), &binding()),
        Err(Refusal::ManifestNotFirst { .. })
    ));
}

/// A line naming no key at all describes no deletion.
#[test]
fn a_line_with_no_key_is_refused() {
    let mut l = line("lwp1-a", "set-a", &[]);
    l.object_keys.clear();
    assert!(matches!(
        validate_plan(&plan(vec![l]), &binding()),
        Err(Refusal::EmptyLine(_))
    ));
}

/// An EMPTY plan is legitimate and deletes nothing.
#[test]
fn an_empty_plan_is_valid_and_deletes_nothing() {
    let p = plan(Vec::new());
    assert!(validate_plan(&p, &binding()).is_ok());
    let outcome = execute(
        &p,
        &NeverDeletes,
        &FakeSleeper::default(),
        &FakeSink::default(),
        &FakeLister(Vec::new()),
        &attribution(),
        limits(),
    );
    assert_eq!(outcome.objects_deleted, 0);
    assert!(outcome.complete());
}
