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
    Refusal, RunAttribution, RunBinding, SinkError, Sleeper, TombstoneSink, Versioning,
    EVIDENCE_ROOT, MAX_ATTEMPTS, PLAN_MEDIA_TYPE, RECORD_MEDIA_TYPE,
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

    /// An unversioned bucket: the provider this fake models removes what it
    /// is asked to remove.
    fn probe_versioning(&self, _key: &str) -> Result<Versioning, DeleteError> {
        Ok(Versioning::Unversioned)
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

    fn probe_versioning(&self, _key: &str) -> Result<Versioning, DeleteError> {
        Ok(Versioning::Unversioned)
    }
}

/// A bucket's versioning state, as S3 names it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    /// Never versioned.
    Unversioned,
    /// Versioning (and Object Lock, which requires it) on.
    Enabled,
    /// Versioning on once, now suspended.
    Suspended,
}

/// One stored version: `id` is `None` for a NULL version; `marker` for a
/// delete marker.
#[derive(Clone, Debug)]
struct Ver {
    id: Option<String>,
    marker: bool,
}

/// An S3-compatible bucket, modelled the way MinIO behaves — the lab's
/// (harness-rows-11 `object-lock`, ctl-batch-2's probe) and the reviewer's
/// local one (ctl-batch-2 review H1):
///
/// * a PUT answers a version id only while versioning is ENABLED;
/// * a HEAD answers the current version's id, and nothing for a null version
///   — **including an object written BEFORE versioning was enabled** (H1);
///   a key whose latest version is a marker answers 404;
/// * a DELETE with no version id is ANSWERED SUCCESS: it removes the object
///   on an unversioned bucket, pushes a marker over everything under Enabled
///   versioning, and under Suspended replaces the null version with a null
///   marker (older, Enabled-era versions survive). No legal hold is consulted
///   by a delete that names no version — which is the defect.
///
/// The worker cannot delete a specific version (`object_store` 0.14 has no
/// such call), so neither can this. What a row reads afterwards is the truth:
/// whether any DATA version of a key survives, and how many markers exist.
/// It is also the run's tombstone sink, because the real sink writes into the
/// same bucket.
struct ModelBucket {
    mode: std::cell::Cell<Mode>,
    versions: RefCell<BTreeMap<String, Vec<Ver>>>,
    next_id: std::cell::Cell<u32>,
    /// An operator's hand: after this many more tombstone PUTs, the bucket's
    /// versioning becomes `Enabled` (re-check RH1 — versioning switched on
    /// while a point's deletes are in flight).
    enable_after_sink_puts: std::cell::Cell<Option<usize>>,
}

impl ModelBucket {
    fn new(mode: Mode) -> Self {
        Self {
            mode: std::cell::Cell::new(mode),
            versions: RefCell::new(BTreeMap::new()),
            next_id: std::cell::Cell::new(1),
            enable_after_sink_puts: std::cell::Cell::new(None),
        }
    }

    fn fresh_id(&self) -> String {
        let n = self.next_id.get();
        self.next_id.set(n + 1);
        format!("v{n}")
    }

    /// Store keys under the bucket's CURRENT mode.
    fn put_all(&self, keys: &[String]) {
        for key in keys {
            self.put(key);
        }
    }

    fn put(&self, key: &str) -> Option<String> {
        let mut versions = self.versions.borrow_mut();
        let entry = versions.entry(key.to_string()).or_default();
        match self.mode.get() {
            Mode::Unversioned => {
                *entry = vec![Ver {
                    id: None,
                    marker: false,
                }];
                None
            }
            Mode::Enabled => {
                let id = self.fresh_id();
                entry.push(Ver {
                    id: Some(id.clone()),
                    marker: false,
                });
                Some(id)
            }
            Mode::Suspended => {
                entry.retain(|v| v.id.is_some());
                entry.push(Ver {
                    id: None,
                    marker: false,
                });
                None
            }
        }
    }

    fn data_survives(&self, key: &str) -> bool {
        self.versions
            .borrow()
            .get(key)
            .is_some_and(|v| v.iter().any(|x| !x.marker))
    }

    fn latest_is_data(&self, key: &str) -> bool {
        self.versions
            .borrow()
            .get(key)
            .and_then(|v| v.last())
            .is_some_and(|x| !x.marker)
    }

    fn markers(&self) -> usize {
        self.versions
            .borrow()
            .values()
            .flatten()
            .filter(|v| v.marker)
            .count()
    }
}

impl Deleter for ModelBucket {
    fn delete_exact(&self, key: &str) -> Result<(), DeleteError> {
        let mode = self.mode.get();
        let id = (mode == Mode::Enabled).then(|| self.fresh_id());
        let mut versions = self.versions.borrow_mut();
        match mode {
            Mode::Unversioned => {
                versions.remove(key);
            }
            Mode::Enabled => versions
                .entry(key.to_string())
                .or_default()
                .push(Ver { id, marker: true }),
            Mode::Suspended => {
                let entry = versions.entry(key.to_string()).or_default();
                entry.retain(|v| v.id.is_some());
                entry.push(Ver {
                    id: None,
                    marker: true,
                });
            }
        }
        Ok(())
    }

    fn probe_versioning(&self, key: &str) -> Result<Versioning, DeleteError> {
        match self.versions.borrow().get(key).and_then(|v| v.last()) {
            None => Err(DeleteError::NotFound),
            Some(v) if v.marker => Err(DeleteError::NotFound),
            Some(v) if v.id.is_some() => Ok(Versioning::Versioned),
            Some(_) => Ok(Versioning::Unversioned),
        }
    }
}

impl TombstoneSink for ModelBucket {
    fn put_create_only(&self, key: &str, _bytes: &[u8]) -> Result<Option<String>, SinkError> {
        if self.latest_is_data(key) {
            return Err(SinkError(format!("{key} already exists")));
        }
        let version = self.put(key);
        if let Some(n) = self.enable_after_sink_puts.get() {
            if n <= 1 {
                self.mode.set(Mode::Enabled);
                self.enable_after_sink_puts.set(None);
            } else {
                self.enable_after_sink_puts.set(Some(n - 1));
            }
        }
        Ok(version)
    }
}

/// A deleter whose PROBE answers from a script, and which records deletes.
struct ScriptedProbe {
    answers: RefCell<Vec<Result<Versioning, DeleteError>>>,
    deleted: RefCell<Vec<String>>,
}

impl ScriptedProbe {
    fn answering(answers: Vec<Result<Versioning, DeleteError>>) -> Self {
        Self {
            answers: RefCell::new(answers),
            deleted: RefCell::new(Vec::new()),
        }
    }
}

impl Deleter for ScriptedProbe {
    fn delete_exact(&self, key: &str) -> Result<(), DeleteError> {
        self.deleted.borrow_mut().push(key.to_string());
        Ok(())
    }

    fn probe_versioning(&self, _key: &str) -> Result<Versioning, DeleteError> {
        let mut answers = self.answers.borrow_mut();
        if answers.is_empty() {
            Ok(Versioning::Unversioned)
        } else {
            answers.remove(0)
        }
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
    fn put_create_only(&self, key: &str, bytes: &[u8]) -> Result<Option<String>, SinkError> {
        if self.refuse {
            return Err(SinkError("the sink refused".to_string()));
        }
        let mut written = self.written.borrow_mut();
        if written.contains_key(key) {
            return Err(SinkError(format!("{key} already exists")));
        }
        written.insert(key.to_string(), bytes.to_vec());
        Ok(None)
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
        co_point_ids: Vec::new(),
    }
}

fn plan(lines: Vec<PlanLine>) -> Plan {
    Plan {
        format: PLAN_MEDIA_TYPE.to_string(),
        format_version: "1.0.0".to_string(),
        policy_namespace: "team-a".to_string(),
        policy_name: "primary".to_string(),
        policy_uid: UID.to_string(),
        location_id: "s3://kafka-backups/team-a".to_string(),
        scope_prefix: SCOPE.to_string(),
        keep_last: Some(2),
        keep_days: Some(30),
        min_usable_points: 3,
        lines,
    }
}

fn binding() -> RunBinding {
    RunBinding {
        policy_uid: UID.to_string(),
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

/// The plan must be for THIS policy.
///
/// **By UID and not by generation** (review `d3w9` C1): approving a plan is a
/// spec patch and a spec patch bumps the generation, so a plan bound to one
/// could never be approved. What binds the plan to the run is the policy's
/// identity, its scope, and the approved digest `parse_plan` checked over the
/// exact bytes.
#[test]
fn a_plan_for_another_policy_is_refused() {
    let mut q = plan(vec![line("lwp1-a", "set-a", &["seg-0"])]);
    q.policy_uid = "00000000-0000-4000-8000-000000000000".to_string();
    assert!(matches!(
        validate_plan(&q, &binding()),
        Err(Refusal::PolicyMismatch { .. })
    ));
    // And the plan carries no generation at all, so nothing here can compare
    // one: `deny_unknown_fields` refuses a document that still has it.
    let mut with_generation =
        serde_json::to_value(plan(vec![line("lwp1-a", "set-a", &["seg-0"])])).expect("JSON");
    with_generation
        .as_object_mut()
        .expect("an object")
        .insert("policy_generation".to_string(), serde_json::json!(4));
    let bytes = serde_json::to_vec(&with_generation).expect("serialises");
    let digest = logweir_core::ids::sha256_prefixed(&bytes);
    assert!(matches!(
        parse_plan(&bytes, &digest),
        Err(Refusal::Unreadable(_))
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
            tombstone_key(UID, "r0123456789abcdef", "lwp1-a", "check"),
            tombstone_key(UID, "r0123456789abcdef", "lwp1-a", "completion"),
            tombstone_key(UID, "r0123456789abcdef", "lwp1-a", "intent"),
        ],
        "both stages exist, and the post-delete versioning check (re-check RH1); the list \
         is key-sorted"
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
            policy_generation: 4,
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
        policy_generation: 4,
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
        Some("BudgetExhausted"),
        "the run's own ceiling has its own code (review `d3w9` M2), so the controller can \
         decline to count it toward consecutiveRunFailures"
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

// ===========================================================================
// FIX ROUND 1
// ===========================================================================

/// **M1.** A dry run over an enumerating plan reports the REAL object count.
///
/// Every plan this build writes sets `enumerate_set: true` — the catalog view
/// carries no segment keys — so a preview that reported the plan's own key list
/// reported `1`, the manifest and nothing else, for a point whose removal takes
/// thousands of objects.
#[test]
fn a_dry_run_enumerates_and_reports_the_real_count() {
    let mut l = line("lwp1-a", "set-a", &[]);
    l.enumerate_set = true;
    let p = plan(vec![l]);
    let listed: Vec<String> = (0..5)
        .map(|i| format!("{SCOPE}/set-a/seg-{i}"))
        .chain(std::iter::once(format!("{SCOPE}/set-a/manifest.json")))
        .collect();
    let sink = FakeSink::default();
    let outcome = execute(
        &p,
        // The deleter PANICS: a preview that enumerates must still delete
        // nothing at all.
        &NeverDeletes,
        &FakeSleeper::default(),
        &sink,
        &FakeLister(listed),
        &attribution(),
        Limits {
            dry_run: true,
            max_objects: 20_000,
        },
    );
    assert_eq!(outcome.objects_deleted, 0);
    assert_eq!(outcome.attempts, 0, "a dry run issues no delete call");
    assert!(sink.keys().is_empty(), "and writes no tombstone");
    assert_eq!(
        outcome.points[0].remaining_keys.len(),
        6,
        "the manifest plus its five segments — the number an administrator needs, not the \
         plan's own 1. Got: {:?}",
        outcome.points[0].remaining_keys
    );
    assert_eq!(
        outcome.points[0].remaining_keys[0],
        format!("{SCOPE}/set-a/manifest.json"),
        "and the manifest is still first, which is the order the real run would use"
    );
}

/// **M1, the fall-back.** A preview by a caller holding no list grant reports
/// the plan's own keys rather than failing.
#[test]
fn a_dry_run_without_a_lister_falls_back_to_the_plans_own_keys() {
    let mut l = line("lwp1-a", "set-a", &[]);
    l.enumerate_set = true;
    let p = plan(vec![l]);
    let outcome = execute(
        &p,
        &NeverDeletes,
        &FakeSleeper::default(),
        &FakeSink::default(),
        &logweir_reaper::NoListing,
        &attribution(),
        Limits {
            dry_run: true,
            max_objects: 20_000,
        },
    );
    assert_eq!(outcome.points[0].remaining_keys.len(), 1);
    assert_eq!(outcome.points[0].code.as_deref(), Some("DryRun"));
}

/// **M2.** A run stopped by its own ceiling mid-point is named
/// `BudgetExhausted`, not `Unclassified`.
#[test]
fn a_mid_point_budget_stop_is_named_and_is_not_a_failure() {
    let p = plan(vec![line("lwp1-a", "set-a", &["seg-0", "seg-1", "seg-2"])]);
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
    assert_eq!(outcome.points[0].state, PointState::Orphaned.as_str());
    assert_eq!(
        outcome.points[0].code.as_deref(),
        Some("BudgetExhausted"),
        "a run that stopped on its own ceiling and a run that could not read the bucket are \
         different findings, fixed in different places"
    );
    assert!(
        outcome.bounded_only(),
        "and the run as a whole says so, so the controller can decline to count it toward \
         consecutiveRunFailures"
    );
    // The negative control: one real failure and it is no longer `bounded_only`.
    let failing = FakeDeleter::refusing(
        &format!("{SCOPE}/set-a/seg-0"),
        &[DeleteError::AccessDenied],
    );
    let mixed = run(&p, &failing, &FakeSink::default(), limits());
    assert!(!mixed.bounded_only());
}

/// **M7.** The deleter refuses Azure and GCS rather than reading the process
/// environment for a delete-capable credential.
#[test]
fn the_deleter_refuses_azure_and_gcs_rather_than_reading_the_environment() {
    use logweir_core::engine::StorageUrl;
    use logweir_reaper::archive::BuildError;
    use logweir_reaper::ArchiveReaper;

    let azure = ArchiveReaper::new(
        &StorageUrl::Azure {
            account_name: "acct".to_string(),
            container_name: "c".to_string(),
            prefix: String::new(),
        },
        None,
        false,
    );
    assert!(
        matches!(
            azure,
            Err(BuildError::UnsupportedProvider("Azure Blob Storage"))
        ),
        "the S3 arm is written with `AmazonS3Builder::new()` precisely so no ambient variable \
         can relocate a deletion; a `from_env()` on another provider is the same defect through \
         the other door. Got: {azure:?}"
    );

    let gcs = ArchiveReaper::new(
        &StorageUrl::Gcs {
            bucket: "b".to_string(),
            prefix: String::new(),
        },
        None,
        false,
    );
    assert!(
        matches!(
            gcs,
            Err(BuildError::UnsupportedProvider("Google Cloud Storage"))
        ),
        "got {gcs:?}"
    );
    assert!(
        format!("{}", azure.expect_err("an error")).contains("mode: Report"),
        "and the refusal says what an operator can still do"
    );
}

/// **M8.** A key whose normalised form differs from the plan's is refused, not
/// deleted in its normalised form.
///
/// `object_store::path::Path::from` normalises AFTER every rail above has run:
/// it drops empty segments and percent-encodes `.`, `..`, `%`, `#`, `<`, `>`,
/// `?`, `*` and the control set. Two consequences, both closed here.
#[test]
fn a_key_whose_normalised_form_differs_is_refused() {
    use logweir_reaper::archive::normalise;

    // The ordinary case still works.
    assert!(normalise(&format!("{SCOPE}/set-a/seg-0")).is_ok());

    // (a) A LEADING SLASH NORMALISES INTO THE EVIDENCE ROOT. With an empty
    //     scope prefix, `/logweir/x` passes `validate_key` — it starts with
    //     neither `logweir/` nor anything disallowed — and `Path::from` turns
    //     it into `logweir/x`.
    let laundered = normalise("/logweir/x");
    assert!(
        laundered.is_err(),
        "the string validated must be the path deleted; `{:?}` normalises into the evidence \
         root",
        laundered.map(|p| p.to_string())
    );

    // (b) A KEY CARRYING `?`, `#` OR `%` IS PERCENT-ENCODED INTO A DIFFERENT
    //     OBJECT. The delete would then hit nothing, the backend would answer
    //     `NotFound`, and `attempt` treats that as success — reporting a point
    //     `Deleted` with its objects still in the bucket.
    for key in [
        format!("{SCOPE}/set-a/seg?0"),
        format!("{SCOPE}/set-a/seg#0"),
        format!("{SCOPE}/set-a/seg%200"),
        format!("{SCOPE}/set-a//seg-0"),
        format!("{SCOPE}/set-a/../seg-0"),
    ] {
        assert!(
            normalise(&key).is_err(),
            "`{key}` normalises to something else, so deleting the normalised form would be \
             deleting a path nothing validated"
        );
    }
}

// ===========================================================================
// Defect OBJECT-LOCK-DELETE-MARKER — a delete marker is not a deletion
// ===========================================================================

fn run_over<D: Deleter, T: TombstoneSink>(
    p: &Plan,
    deleter: &D,
    sink: &T,
) -> logweir_reaper::Outcome {
    execute(
        p,
        deleter,
        &FakeSleeper::default(),
        sink,
        &FakeLister(Vec::new()),
        &attribution(),
        limits(),
    )
}

/// The four bucket states, each against one plan line. `before` is the mode the
/// set was WRITTEN under, `now` the mode the run meets.
fn bucket_run(before: Mode, now: Mode) -> (ModelBucket, PlanLine, logweir_reaper::Outcome) {
    let point = line("lwp1-a", "set-a", &["seg-0", "seg-1"]);
    let bucket = ModelBucket::new(before);
    bucket.put_all(&point.object_keys);
    bucket.mode.set(now);
    let outcome = run_over(&plan(vec![point.clone()]), &bucket, &bucket);
    (bucket, point, outcome)
}

fn assert_refused_and_intact(
    label: &str,
    bucket: &ModelBucket,
    point: &PlanLine,
    outcome: &logweir_reaper::Outcome,
) {
    assert!(
        outcome.deleted().is_empty(),
        "{label}: {:?}",
        outcome.points
    );
    assert_eq!(
        outcome.points[0].state,
        PointState::Kept.as_str(),
        "{label}"
    );
    assert_eq!(
        outcome.points[0].code.as_deref(),
        Some("VersionedBucket"),
        "{label}"
    );
    assert_eq!(
        outcome.attempts, 0,
        "{label}: no delete call was issued at all"
    );
    assert_eq!(bucket.markers(), 0, "{label}: no delete marker was written");
    assert!(
        point.object_keys.iter().all(|k| bucket.latest_is_data(k)),
        "{label}: every object is still the latest version"
    );
}

/// **REVIEW H1, THE CASE THE FIRST FIX MISSED.** The set was written BEFORE
/// versioning was enabled, so every object is a NULL version and its HEAD
/// carries no version id — the per-key probe says `Unversioned`. Under Enabled
/// versioning a delete by key still writes a marker over it (measured on MinIO
/// by the review). The run's own intent tombstone, PUT into the same bucket,
/// comes back WITH a version id, and that refuses the line.
///
/// MUTANT: ignore the intent tombstone's version id in `execute`. The point
/// is recorded `Deleted`, markers are written, the data survives, and this row
/// fails.
#[test]
fn an_object_written_before_versioning_was_enabled_is_not_recorded_deleted() {
    let (bucket, point, outcome) = bucket_run(Mode::Unversioned, Mode::Enabled);
    assert_refused_and_intact("pre-versioning, now Enabled", &bucket, &point, &outcome);
}

/// Versioning (or Object Lock) ENABLED throughout — the live lab shape
/// (harness-rows-11 `object-lock`). Both signals fire; nothing is deleted.
///
/// MUTANT: drop BOTH the intent-version refusal and the per-key HEAD refusal.
/// Markers are written, the point is `Deleted`, and this row fails. (Either
/// check alone keeps it green — that is the defence in depth.)
#[test]
fn a_versioned_bucket_is_refused_and_nothing_is_recorded_deleted() {
    let (bucket, point, outcome) = bucket_run(Mode::Enabled, Mode::Enabled);
    assert_refused_and_intact("Enabled throughout", &bucket, &point, &outcome);
    assert!(!DeleteError::VersionedBucket.retryable());
    assert!(
        !DeleteError::VersionedBucket.is_hold(),
        "a refusal to delete on a versioned bucket is not a provider's hold verdict"
    );
}

/// Versioning SUSPENDED after the set was written under Enabled: a PUT now
/// answers no version id, so the bucket-level signal is silent — and a delete
/// by key would put a null marker over the Enabled-era versions and remove
/// nothing. The per-key HEAD sees their version ids and refuses.
///
/// MUTANT: drop the per-key HEAD refusal in `attempt`. This row fails.
#[test]
fn enabled_era_objects_in_a_suspended_bucket_are_not_recorded_deleted() {
    let (bucket, point, outcome) = bucket_run(Mode::Enabled, Mode::Suspended);
    assert!(outcome.deleted().is_empty(), "{:?}", outcome.points);
    assert_eq!(outcome.points[0].code.as_deref(), Some("VersionedBucket"));
    assert_eq!(bucket.markers(), 0);
    assert!(point.object_keys.iter().all(|k| bucket.latest_is_data(k)));
}

/// NEGATIVE CONTROLS: a bucket that was never versioned, and a Suspended one
/// whose set was written while suspended (null versions, which a delete really
/// replaces), are deleted — and no data version of any key survives. A worker
/// that refused every bucket would pass the three rows above and fail these.
///
/// MUTANT: refuse the line whenever the intent tombstone was written at all
/// (read `None` as versioned). Both arms fail.
#[test]
fn unversioned_and_suspended_era_objects_are_really_deleted() {
    for (label, before, now) in [
        ("never versioned", Mode::Unversioned, Mode::Unversioned),
        ("written while suspended", Mode::Suspended, Mode::Suspended),
    ] {
        let (bucket, point, outcome) = bucket_run(before, now);
        assert_eq!(outcome.deleted(), vec!["lwp1-a"], "{label}");
        assert_eq!(outcome.objects_deleted, 3, "{label}");
        assert!(
            point.object_keys.iter().all(|k| !bucket.data_survives(k)),
            "{label}: `Deleted` means the data is gone"
        );
    }
}

/// A set written across a suspension: the manifest is a null version and goes,
/// a segment carries a version id and is NOT deleted. The point is `Orphaned`
/// with the versioned key named — never `Deleted`.
#[test]
fn a_versioned_segment_is_left_and_named() {
    let point = line("lwp1-a", "set-a", &["seg-0", "seg-1"]);
    let versioned_segment = point.object_keys[2].clone();
    let point_keys = point.object_keys.clone();
    let bucket = ModelBucket::new(Mode::Enabled);
    bucket.put_all(std::slice::from_ref(&versioned_segment));
    bucket.mode.set(Mode::Suspended);
    bucket.put_all(&point.object_keys[..2]);
    let outcome = run_over(&plan(vec![point]), &bucket, &bucket);
    let only = &outcome.points[0];
    assert_eq!(only.state, PointState::Orphaned.as_str());
    assert_eq!(only.code.as_deref(), Some("VersionedBucket"));
    assert_eq!(only.remaining_keys, vec![versioned_segment.clone()]);
    assert!(bucket.latest_is_data(&versioned_segment));
    assert_eq!(
        bucket.markers(),
        2,
        "only the two null versions' own null markers, each a real removal"
    );
    assert!(!bucket.data_survives(&point_keys[0]) && !bucket.data_survives(&point_keys[1]));
}

/// A probe that cannot be answered — `AccessDenied` on the HEAD, a credential
/// without `s3:GetObject` — deletes nothing: "could not tell" never
/// authorises a delete.
///
/// MUTANT: treat a refused probe as `Unversioned`. The delete is issued and
/// this row fails.
#[test]
fn a_refused_probe_deletes_nothing_and_names_itself() {
    let deleter = ScriptedProbe::answering(vec![Err(DeleteError::AccessDenied)]);
    let outcome = run_over(
        &plan(vec![line("lwp1-a", "set-a", &["seg-0"])]),
        &deleter,
        &FakeSink::default(),
    );
    assert!(deleter.deleted.borrow().is_empty());
    assert_eq!(outcome.points[0].state, PointState::Kept.as_str());
    assert_eq!(
        outcome.points[0].code.as_deref(),
        Some("VersionProbeRefused")
    );
}

/// The probe gets the bounded retry a delete gets: a 5xx that clears is
/// retried and the key is then deleted; one that does not is named as itself.
#[test]
fn a_probe_server_error_is_retried_and_then_named() {
    let clears = ScriptedProbe::answering(vec![
        Err(DeleteError::ServerError),
        Ok(Versioning::Unversioned),
    ]);
    let outcome = run_over(
        &plan(vec![line("lwp1-a", "set-a", &[])]),
        &clears,
        &FakeSink::default(),
    );
    assert_eq!(outcome.deleted(), vec!["lwp1-a"]);
    assert_eq!(
        outcome.attempts, 1,
        "probes are not counted as delete calls"
    );

    let stuck =
        ScriptedProbe::answering(vec![Err(DeleteError::ServerError); MAX_ATTEMPTS as usize]);
    let outcome = run_over(
        &plan(vec![line("lwp1-a", "set-a", &[])]),
        &stuck,
        &FakeSink::default(),
    );
    assert!(stuck.deleted.borrow().is_empty());
    assert_eq!(outcome.points[0].code.as_deref(), Some("ServerError"));
}

/// A key the probe finds gone is done, and NO delete is sent for it: on a
/// versioned bucket a DELETE of an absent key would itself write a marker.
#[test]
fn a_key_the_probe_finds_gone_is_done_without_a_delete() {
    let deleter = ScriptedProbe::answering(vec![Err(DeleteError::NotFound)]);
    let point = line("lwp1-a", "set-a", &["seg-0"]);
    let outcome = run_over(&plan(vec![point.clone()]), &deleter, &FakeSink::default());
    assert_eq!(outcome.deleted(), vec!["lwp1-a"]);
    assert_eq!(
        *deleter.deleted.borrow(),
        vec![point.object_keys[1].clone()]
    );
}

/// The one rule that reads a provider's answer: any version id is versioned,
/// including a literal `null`; absent (or empty) is not.
#[test]
fn a_version_id_on_the_head_is_what_versioned_means() {
    let meta = |version: Option<&str>| object_store::ObjectMeta {
        location: object_store::path::Path::from("p/k"),
        last_modified: Utc.with_ymd_and_hms(2026, 9, 23, 0, 0, 0).unwrap(),
        size: 5,
        e_tag: None,
        version: version.map(str::to_string),
    };
    use logweir_reaper::archive::versioning_of;
    assert_eq!(
        versioning_of(&meta(Some("54734dd9"))),
        Versioning::Versioned
    );
    assert_eq!(versioning_of(&meta(Some("null"))), Versioning::Versioned);
    assert_eq!(versioning_of(&meta(None)), Versioning::Unversioned);
    assert_eq!(versioning_of(&meta(Some(""))), Versioning::Unversioned);
}

// ===========================================================================
// Review M2 — one line per shared set, every point attributed
// ===========================================================================

/// Two receipts over ONE set, both due: ONE line, the second receipt a
/// co-point. The plan validates (it used to be refused whole as
/// `DuplicateKey` on every run), the set is removed ONCE, the objects are
/// counted once, and BOTH points get an outcome and a pair of tombstones.
///
/// MUTANT: `push_line` records only the line's own point. `lwp1-b` has no
/// outcome and this row fails. MUTANT: tombstones for the line's own point
/// only. The co-point's intent is missing and this row fails.
#[test]
fn a_shared_set_line_removes_the_set_once_and_attributes_every_point() {
    let mut shared = line("lwp1-a", "set-a", &["seg-0", "seg-1"]);
    shared.co_point_ids = vec!["lwp1-b".to_string()];
    let p = plan(vec![shared, line("lwp1-c", "set-c", &[])]);
    validate_plan(&p, &binding()).expect("one line per set is a valid plan");
    let deleter = FakeDeleter::default();
    let sink = FakeSink::default();
    let outcome = run(&p, &deleter, &sink, limits());
    assert_eq!(outcome.deleted(), vec!["lwp1-a", "lwp1-b", "lwp1-c"]);
    assert_eq!(
        outcome.objects_deleted, 4,
        "the shared set's 3 keys once, plus 1"
    );
    assert_eq!(deleter.keys().len(), 4, "no key is deleted twice");
    let b = outcome
        .points
        .iter()
        .find(|p| p.point_id == "lwp1-b")
        .expect("the co-point has its own outcome");
    assert_eq!(
        b.objects_deleted, 0,
        "its objects are counted on the line's point"
    );
    for point in ["lwp1-a", "lwp1-b"] {
        for stage in ["intent", "completion"] {
            let key = tombstone_key(UID, "r0123456789abcdef", point, stage);
            assert!(
                sink.keys().contains(&key),
                "{point} {stage}: {:?}",
                sink.keys()
            );
        }
    }
}

/// A point named twice — a line's own point repeated as a co-point elsewhere —
/// is refused before any delete: its removal would be recorded twice.
#[test]
fn a_point_named_twice_is_refused() {
    let mut first = line("lwp1-a", "set-a", &[]);
    first.co_point_ids = vec!["lwp1-c".to_string()];
    let p = plan(vec![first, line("lwp1-c", "set-c", &[])]);
    assert_eq!(
        validate_plan(&p, &binding()),
        Err(Refusal::DuplicatePoint("lwp1-c".to_string()))
    );
}

/// `maxDeletionsPerRun` counts POINTS, co-points included.
#[test]
fn the_point_ceiling_counts_co_points() {
    let mut shared = line("lwp1-a", "set-a", &[]);
    shared.co_point_ids = vec!["lwp1-b".to_string()];
    let mut tight = binding();
    tight.max_deletions_per_run = 1;
    assert!(matches!(
        validate_plan(&plan(vec![shared]), &tight),
        Err(Refusal::OverCap {
            what: "points",
            found: 2,
            cap: 1
        })
    ));
}

/// Review L9: the dry run says when the run would refuse a versioned manifest.
#[test]
fn a_dry_run_names_a_versioned_manifest() {
    let bucket = ModelBucket::new(Mode::Enabled);
    let point = line("lwp1-a", "set-a", &[]);
    bucket.put_all(&point.object_keys);
    let outcome = execute(
        &plan(vec![point]),
        &bucket,
        &FakeSleeper::default(),
        &logweir_reaper::NoTombstones,
        &FakeLister(Vec::new()),
        &attribution(),
        Limits {
            dry_run: true,
            max_objects: 20_000,
        },
    );
    assert_eq!(
        outcome.points[0].code.as_deref(),
        Some("DryRun:VersionedBucket")
    );
    assert_eq!(bucket.markers(), 0);
}

// ===========================================================================
// Re-check RH1 — versioning switched on WHILE a point's deletes run
// ===========================================================================

/// A plain bucket; the operator enables versioning right after the point's
/// intent tombstone is written. The intent came back unversioned, every key's
/// HEAD answers no version id (null versions), and every DELETE is answered
/// success — and wrote a marker. The post-delete check object comes back
/// VERSIONED, so the point is NOT `Deleted`: `Orphaned` with
/// `VersionedBucket`, every planned key named, nothing counted as removed.
///
/// MUTANT: ignore the check object's version id. The point is recorded
/// `Deleted` with 3 objects while all three survive behind markers, and this
/// row fails.
#[test]
fn versioning_enabled_during_a_points_deletes_is_not_recorded_deleted() {
    let point = line("lwp1-a", "set-a", &["seg-0", "seg-1"]);
    let bucket = ModelBucket::new(Mode::Unversioned);
    bucket.put_all(&point.object_keys);
    bucket.enable_after_sink_puts.set(Some(1));
    let outcome = run_over(&plan(vec![point.clone()]), &bucket, &bucket);
    assert!(outcome.deleted().is_empty(), "{:?}", outcome.points);
    let only = &outcome.points[0];
    assert_eq!(only.state, PointState::Orphaned.as_str());
    assert_eq!(only.code.as_deref(), Some("VersionedBucket"));
    assert_eq!(only.objects_deleted, 0);
    assert_eq!(
        outcome.objects_deleted, 0,
        "no object is claimed as removed"
    );
    assert_eq!(only.remaining_keys, point.object_keys);
    assert!(
        point.object_keys.iter().all(|k| bucket.data_survives(k)),
        "the model confirms the premise: every object survives behind a marker"
    );
    assert_eq!(bucket.markers(), 3);
}

/// NEGATIVE CONTROL: the same run on a bucket that stays plain is `Deleted`,
/// its check object answered with no version id.
#[test]
fn a_bucket_that_stays_plain_during_the_deletes_is_deleted() {
    let point = line("lwp1-a", "set-a", &["seg-0", "seg-1"]);
    let bucket = ModelBucket::new(Mode::Unversioned);
    bucket.put_all(&point.object_keys);
    let outcome = run_over(&plan(vec![point.clone()]), &bucket, &bucket);
    assert_eq!(outcome.deleted(), vec!["lwp1-a"]);
    assert!(point.object_keys.iter().all(|k| !bucket.data_survives(k)));
}

/// A check object that could not be written is "could not tell": the point is
/// not recorded `Deleted`.
#[test]
fn a_refused_versioning_check_is_not_recorded_deleted() {
    struct RefusesTheCheck(FakeSink);
    impl TombstoneSink for RefusesTheCheck {
        fn put_create_only(&self, key: &str, bytes: &[u8]) -> Result<Option<String>, SinkError> {
            if key.ends_with(".check.json") {
                return Err(SinkError("refused".to_string()));
            }
            self.0.put_create_only(key, bytes)
        }
    }
    let outcome = run_over(
        &plan(vec![line("lwp1-a", "set-a", &["seg-0"])]),
        &FakeDeleter::default(),
        &RefusesTheCheck(FakeSink::default()),
    );
    assert!(outcome.deleted().is_empty());
    assert!(outcome.points[0]
        .code
        .as_deref()
        .is_some_and(|c| c.starts_with("VersionCheckRefused:")));
}
