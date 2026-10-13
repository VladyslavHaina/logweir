//! The retention worker's own rows: the one executable in this product that
//! deletes archive data. `a_dry_run_is_a_dry_run` kills a `--dry-run` flipped
//! to delete for real.
//!
//! Everything here drives the shipped [`logweir_retention::admit`] and
//! [`logweir_retention::execute`]. `admit` is pure: it takes the argv, an
//! environment map and a plan-reading closure, so **every exit-3 refusal is
//! a table row** with no socket, no bucket and no process
//! environment. `execute` takes the reaper's four injected ports, so the
//! dry-run arm, the key lines and the exit code are observable without
//! deleting anything.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::time::Duration;

use logweir_reaper::{
    DeleteError, Deleter, Lister, PlanLine, SinkError, Sleeper, TombstoneSink, Versioning,
};
use logweir_retention::{
    admit, env, env_map, execute, parse_args, result_line, Refusal, EXIT_INCOMPLETE, EXIT_OK,
    EXIT_REFUSED, REFUSAL_CODES,
};

const SCOPE: &str = "kafka-backups/team-a";
const UID: &str = "16161616-0000-4000-8000-000000000016";

// Fixtures.

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

fn plan_value(lines: Vec<PlanLine>) -> serde_json::Value {
    serde_json::json!({
        "format": logweir_reaper::PLAN_MEDIA_TYPE,
        "format_version": "1.0.0",
        "policy_namespace": "team-a",
        "policy_name": "primary",
        "policy_uid": UID,
        "location_id": "s3://kafka-backups/team-a",
        "scope_prefix": SCOPE,
        "keep_last": 2,
        "keep_days": 30,
        "min_usable_points": 3,
        "lines": lines,
    })
}

fn plan_bytes(lines: Vec<PlanLine>) -> Vec<u8> {
    serde_json::to_vec(&plan_value(lines)).expect("the plan serialises")
}

fn digest_of(bytes: &[u8]) -> String {
    logweir_core::ids::sha256_prefixed(bytes)
}

/// A `DestinationLocation` as the controller freezes it into the Job.
fn location_json() -> String {
    serde_json::json!({
        "provider": "S3",
        "bucket": "kafka-backups",
        "prefix": SCOPE,
        "region": "us-east-1",
        "endpoint": "http://minio.storage.svc:9000",
        "addressing": "PathStyle",
        "transport": "InsecureHTTP"
    })
    .to_string()
}

/// The complete, valid environment a real Job carries.
fn full_env(digest: &str) -> BTreeMap<String, String> {
    env_map(&[
        ("LOGWEIR_RETENTION_PLAN_SHA256", digest),
        ("LOGWEIR_RETENTION_POLICY_UID", UID),
        ("LOGWEIR_RETENTION_POLICY_GENERATION", "4"),
        ("LOGWEIR_RETENTION_SCOPE_PREFIX", SCOPE),
        ("LOGWEIR_RETENTION_RUN_ID", "r0123456789abcdef"),
        ("LOGWEIR_RETENTION_APPROVER", "audit:0f3a"),
        ("LOGWEIR_RETENTION_MAX_DELETIONS", "50"),
        ("LOGWEIR_RETENTION_MAX_OBJECTS", "20000"),
        ("LOGWEIR_RETENTION_LOCATION", &location_json()),
        ("AWS_ACCESS_KEY_ID", "AKIADELETE"),
        ("AWS_SECRET_ACCESS_KEY", "delete-secret"),
        ("LOGWEIR_EVIDENCE_AWS_ACCESS_KEY_ID", "AKIAEVIDENCE"),
        ("LOGWEIR_EVIDENCE_AWS_SECRET_ACCESS_KEY", "evidence-secret"),
    ])
}

fn argv() -> Vec<&'static str> {
    vec![
        "run",
        "--plan",
        "/retention/plan.json",
        "--retention-contract-version",
        "1",
    ]
}

fn dry_argv() -> Vec<&'static str> {
    let mut v = argv();
    v.push("--dry-run");
    v
}

// Ports.

#[derive(Default)]
struct FakeDeleter {
    seen: RefCell<Vec<String>>,
}

impl Deleter for FakeDeleter {
    fn delete_exact(&self, key: &str) -> Result<(), DeleteError> {
        self.seen.borrow_mut().push(key.to_string());
        Ok(())
    }

    fn probe_versioning(&self, _key: &str) -> Result<Versioning, DeleteError> {
        Ok(Versioning::Unversioned)
    }
}

/// A deleter that PANICS. Used wherever the property is that nothing is
/// deleted, so a zero count is "one call would have failed the test" rather
/// than "no call was observed".
struct NeverDeletes;

impl Deleter for NeverDeletes {
    fn delete_exact(&self, key: &str) -> Result<(), DeleteError> {
        panic!("delete_exact({key}) on a path that must delete nothing")
    }

    fn probe_versioning(&self, _key: &str) -> Result<Versioning, DeleteError> {
        Ok(Versioning::Unversioned)
    }
}

#[derive(Default)]
struct FakeSink {
    written: RefCell<Vec<String>>,
}

impl TombstoneSink for FakeSink {
    fn put_create_only(&self, key: &str, _bytes: &[u8]) -> Result<Option<String>, SinkError> {
        self.written.borrow_mut().push(key.to_string());
        Ok(None)
    }
}

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

#[derive(Default)]
struct NoSleep;

impl Sleeper for NoSleep {
    fn sleep(&self, _d: Duration) {}
}

// The argv.

/// The subcommand, the contract version and `--plan`, as a table.
#[test]
fn the_argv_refusals_are_each_named() {
    assert!(matches!(
        parse_args(&["retention", "run"]),
        Err(Refusal::NotRun(_))
    ));
    assert!(matches!(parse_args(&[]), Err(Refusal::NotRun(_))));
    assert!(matches!(
        parse_args(&[
            "run",
            "--plan",
            "p",
            "--retention-contract-version",
            "1",
            "--wat"
        ]),
        Err(Refusal::UnknownArgument(_))
    ));
    assert!(matches!(
        parse_args(&["run", "--plan", "p"]),
        Err(Refusal::ContractVersion(_))
    ));
    assert!(matches!(
        parse_args(&["run", "--plan", "p", "--retention-contract-version", "2"]),
        Err(Refusal::ContractVersion(_))
    ));
    assert!(matches!(
        parse_args(&["run", "--retention-contract-version", "1"]),
        Err(Refusal::NoPlanPath)
    ));
    let ok = parse_args(&argv()).expect("the real argv parses");
    assert_eq!(ok.plan_path, "/retention/plan.json");
    assert!(!ok.dry_run);
    assert!(parse_args(&dry_argv()).expect("parses").dry_run);
}

/// **The contract version is answered BEFORE the plan path**, so a newer
/// controller's Job is refused by name rather than for the wrong reason.
#[test]
fn the_contract_version_is_checked_before_the_plan_path() {
    let err = parse_args(&["run", "--retention-contract-version", "9"])
        .expect_err("a wrong version with no plan is refused");
    assert!(
        matches!(err, Refusal::ContractVersion(_)),
        "an older worker must refuse a newer plan rather than executing the half of it that it \
         understands; reporting the missing path instead would send the operator to the wrong \
         place. Got: {err:?}"
    );
}

/// Every refusal is exit 3 — "nothing ran", which is what separates it from
/// exit 1.
#[test]
fn every_refusal_is_exit_three() {
    for refusal in [
        Refusal::NotRun("x".to_string()),
        Refusal::UnknownArgument("--x".to_string()),
        Refusal::ContractVersion("2".to_string()),
        Refusal::NoPlanPath,
        Refusal::BindingIncomplete("LOGWEIR_RETENTION_RUN_ID"),
        Refusal::GenerationNotANumber("x".to_string()),
        Refusal::CapUnreadable {
            name: logweir_retention::env::MAX_OBJECTS,
            value: "x".to_string(),
        },
        Refusal::LocationUnreadable("x".to_string()),
        Refusal::NoRecordCredential,
        Refusal::Port("x".to_string()),
    ] {
        assert_eq!(
            refusal.exit_code(),
            EXIT_REFUSED,
            "{refusal:?} must be exit 3"
        );
    }
    // And 2 and 4 are never returned: Global Constraint 11 reserves them for a
    // drill result and a signing failure, neither of which this binary
    // produces.
    assert_ne!(EXIT_REFUSED, 2);
    assert_ne!(EXIT_REFUSED, 4);
    assert_ne!(EXIT_INCOMPLETE, 2);
}

// Admission: every exit-3 path, with no port built.

fn admit_with(
    argv: &[&str],
    env: &BTreeMap<String, String>,
    bytes: Vec<u8>,
) -> Result<logweir_retention::Admitted, Refusal> {
    admit(argv, env, |_| Ok(bytes))
}

/// The happy path admits, and carries both credentials.
#[test]
fn a_complete_binding_admits() {
    let bytes = plan_bytes(vec![line("lwp1-a", "set-a", &["seg-0"])]);
    let digest = digest_of(&bytes);
    let admitted =
        admit_with(&argv(), &full_env(&digest), bytes).expect("a complete binding admits");
    assert_eq!(admitted.attribution.plan_sha256, digest);
    assert_eq!(admitted.attribution.policy_uid, UID);
    assert_eq!(admitted.policy_generation, 4);
    assert_eq!(admitted.approver, "audit:0f3a");
    assert!(!admitted.dry_run);
    assert!(admitted.archive_keys.is_some());
    assert!(admitted.evidence_keys.is_some());
}

/// Each of the seven required variables, missing in turn, is named: an absent
/// ceiling is never replaced by a default.
#[test]
fn each_missing_binding_variable_is_named() {
    let bytes = plan_bytes(vec![line("lwp1-a", "set-a", &["seg-0"])]);
    let digest = digest_of(&bytes);
    for missing in [
        "LOGWEIR_RETENTION_PLAN_SHA256",
        "LOGWEIR_RETENTION_POLICY_UID",
        "LOGWEIR_RETENTION_POLICY_GENERATION",
        "LOGWEIR_RETENTION_SCOPE_PREFIX",
        "LOGWEIR_RETENTION_RUN_ID",
        "LOGWEIR_RETENTION_MAX_DELETIONS",
        "LOGWEIR_RETENTION_MAX_OBJECTS",
    ] {
        let mut env = full_env(&digest);
        env.remove(missing);
        let err =
            admit_with(&argv(), &env, bytes.clone()).expect_err("an incomplete binding is refused");
        assert!(
            matches!(&err, Refusal::BindingIncomplete(name) if *name == missing),
            "removing {missing} must be refused by that name; got {err:?}"
        );
        // AND PRESENT-AND-BLANK IS THE SAME THING. `std::env::var` returns
        // `Ok("")` for a Kubernetes `env:` entry with an empty `value:`, and a
        // `secretKeyRef` to a blank key projects the same.
        let mut blank = full_env(&digest);
        blank.insert(missing.to_string(), String::new());
        assert!(
            matches!(
                admit_with(&argv(), &blank, bytes.clone()),
                Err(Refusal::BindingIncomplete(name)) if name == missing
            ),
            "a blank {missing} is not a configured {missing}"
        );
    }
}

/// The env with both per-run ceilings set to `deletions` and `objects`.
fn env_with_caps(digest: &str, deletions: &str, objects: &str) -> BTreeMap<String, String> {
    let mut env = full_env(digest);
    env.insert(
        logweir_retention::env::MAX_DELETIONS.to_string(),
        deletions.to_string(),
    );
    env.insert(
        logweir_retention::env::MAX_OBJECTS.to_string(),
        objects.to_string(),
    );
    env
}

/// `n` one-segment lines, each its own point and its own backup set.
fn lines(n: usize) -> Vec<PlanLine> {
    (0..n)
        .map(|i| line(&format!("lwp1-{i}"), &format!("set-{i}"), &["seg-0"]))
        .collect()
}

/// **The per-run ceilings are the Job's own, at values their defaults cannot
/// imitate.** The Job says **7** points and **1234** object keys:
///
/// * the binding and the execution limit carry 7 and 1234;
/// * a plan of 8 points is refused `OverCap` naming 7, and a plan of 7 is not;
/// * one point of 1235 object keys is refused `OverCap` naming 1234, and one of
///   1234 is not.
///
/// NEGATIVE CONTROL, in the row: the SAME 8-point and 1235-key plans are
/// ADMITTED under the old defaults (50 / 20 000), so a worker that ignored the
/// environment, or read the two names crossed, fails this row. MUTANTS:
/// `max_deletions_per_run: 50` or `max_objects_per_run: 20_000` hard-coded in
/// `admit`; the two names swapped.
#[test]
fn the_per_run_ceilings_are_the_jobs_own_at_non_default_values() {
    // The binding, and the limit `execute` stops at.
    let one = plan_bytes(lines(1));
    let admitted = admit_with(&argv(), &env_with_caps(&digest_of(&one), "7", "1234"), one)
        .expect("a binding with non-default ceilings admits");
    assert_eq!(admitted.binding.max_deletions_per_run, 7);
    assert_eq!(admitted.binding.max_objects_per_run, 1234);
    assert_eq!(admitted.limits().max_objects, 1234);

    // POINTS: 7 admits and 8 is refused naming the Job's 7 …
    let seven = plan_bytes(lines(7));
    admit_with(
        &argv(),
        &env_with_caps(&digest_of(&seven), "7", "1234"),
        seven,
    )
    .expect("exactly the ceiling admits");
    let eight = plan_bytes(lines(8));
    let err = admit_with(
        &argv(),
        &env_with_caps(&digest_of(&eight), "7", "1234"),
        eight.clone(),
    )
    .expect_err("one point over the Job's ceiling is refused");
    assert!(
        matches!(
            &err,
            Refusal::Plan(logweir_reaper::Refusal::OverCap {
                what: "points",
                found: 8,
                cap: 7
            })
        ),
        "the refusal names the Job's own ceiling: {err:?}"
    );
    assert_eq!(err.exit_code(), EXIT_REFUSED);
    // … and the CONTROL: the same plan under the old defaults is admitted.
    admit_with(
        &argv(),
        &env_with_caps(&digest_of(&eight), "50", "20000"),
        eight,
    )
    .expect("CONTROL: at the defaults this plan admits, so the refusal above is the 7's");

    // OBJECTS: one point whose set is its manifest plus 1233 / 1234 segments.
    let segments = |n: usize| (0..n).map(|i| format!("seg-{i}")).collect::<Vec<_>>();
    let wide = |n: usize| {
        let names = segments(n);
        let refs: Vec<&str> = names.iter().map(String::as_str).collect();
        plan_bytes(vec![line("lwp1-wide", "set-wide", &refs)])
    };
    let at = wide(1233); // 1234 keys
    admit_with(&argv(), &env_with_caps(&digest_of(&at), "7", "1234"), at)
        .expect("exactly the object ceiling admits");
    let over = wide(1234); // 1235 keys
    let err = admit_with(
        &argv(),
        &env_with_caps(&digest_of(&over), "7", "1234"),
        over.clone(),
    )
    .expect_err("one object key over the Job's ceiling is refused");
    assert!(
        matches!(
            &err,
            Refusal::Plan(logweir_reaper::Refusal::OverCap {
                what: "object keys",
                found: 1235,
                cap: 1234
            })
        ),
        "the refusal names the Job's own object ceiling: {err:?}"
    );
    admit_with(
        &argv(),
        &env_with_caps(&digest_of(&over), "50", "20000"),
        over,
    )
    .expect("CONTROL: at the defaults 1235 keys admit, so the refusal above is the 1234's");
}

/// **A RAISED ceiling reaches the worker too, at admission AND at execution.**
/// The row above sets the ceilings BELOW the old fallbacks, so a worker that
/// bounded the environment by the old default (`cap(…)?.min(50)`) would pass it
/// while an administrator's raised `maxDeletionsPerRun` was silently cut to 50.
/// Here the Job says **75** points and **30 000** object keys, both above 50 /
/// 20 000 and inside the CRD's 1..500 / 1..200 000:
///
/// * the binding and the execution limit carry 75 and 30 000;
/// * a plan of 75 points is admitted and one of 76 is refused `OverCap` naming
///   75;
/// * one point of 30 000 object keys is admitted and one of 30 001 is refused
///   `OverCap` naming 30 000;
/// * the 30 000-key point RUNS: all 30 000 keys are deleted, manifest first,
///   and the run exits 0, so the raised number reaches `execute`'s budget too.
///
/// NEGATIVE CONTROL, in the row: the SAME plans are REFUSED at the old 50 / 20
/// 000, naming those.
///
/// MUTANTS: `cap(…)?.min(50)` / `.min(20_000)` in `admit`;
/// `max_objects: self.binding.max_objects_per_run.min(20_000)` in
/// `Admitted::limits` (admission passes, execution stops at 20 000).
#[test]
fn a_raised_ceiling_reaches_admission_and_execution_above_the_old_defaults() {
    // The binding, and the limit `execute` stops at.
    let one = plan_bytes(lines(1));
    let admitted = admit_with(
        &argv(),
        &env_with_caps(&digest_of(&one), "75", "30000"),
        one,
    )
    .expect("a binding with raised ceilings admits");
    assert_eq!(admitted.binding.max_deletions_per_run, 75);
    assert_eq!(admitted.binding.max_objects_per_run, 30_000);
    assert_eq!(admitted.limits().max_objects, 30_000);

    // POINTS: 75 admits and 76 is refused naming the Job's 75 …
    let seventy_five = plan_bytes(lines(75));
    admit_with(
        &argv(),
        &env_with_caps(&digest_of(&seventy_five), "75", "30000"),
        seventy_five.clone(),
    )
    .expect("75 points, above the old 50, admit under a raised ceiling");
    let seventy_six = plan_bytes(lines(76));
    let err = admit_with(
        &argv(),
        &env_with_caps(&digest_of(&seventy_six), "75", "30000"),
        seventy_six,
    )
    .expect_err("one point over the raised ceiling is refused");
    assert!(
        matches!(
            &err,
            Refusal::Plan(logweir_reaper::Refusal::OverCap {
                what: "points",
                found: 76,
                cap: 75
            })
        ),
        "the refusal names the Job's raised ceiling: {err:?}"
    );
    // … and the CONTROL: the 75-point plan is refused at the old 50.
    let err = admit_with(
        &argv(),
        &env_with_caps(&digest_of(&seventy_five), "50", "20000"),
        seventy_five,
    )
    .expect_err("CONTROL: at the old default the 75-point plan is refused");
    assert!(
        matches!(
            &err,
            Refusal::Plan(logweir_reaper::Refusal::OverCap {
                what: "points",
                found: 75,
                cap: 50
            })
        ),
        "CONTROL names 50: {err:?}"
    );

    // OBJECTS: one point whose set is its manifest plus 29 999 / 30 000 segments.
    let wide = |segments: usize| {
        let names: Vec<String> = (0..segments).map(|i| format!("seg-{i}")).collect();
        let refs: Vec<&str> = names.iter().map(String::as_str).collect();
        plan_bytes(vec![line("lwp1-wide", "set-wide", &refs)])
    };
    let at = wide(29_999); // 30 000 keys
    let admitted = admit_with(
        &argv(),
        &env_with_caps(&digest_of(&at), "75", "30000"),
        at.clone(),
    )
    .expect("30 000 keys, above the old 20 000, admit under a raised ceiling");
    let over = wide(30_000); // 30 001 keys
    let err = admit_with(
        &argv(),
        &env_with_caps(&digest_of(&over), "75", "30000"),
        over,
    )
    .expect_err("one object key over the raised ceiling is refused");
    assert!(
        matches!(
            &err,
            Refusal::Plan(logweir_reaper::Refusal::OverCap {
                what: "object keys",
                found: 30_001,
                cap: 30_000
            })
        ),
        "the refusal names the Job's raised object ceiling: {err:?}"
    );
    // CONTROL: the 30 000-key point is refused at the old 20 000.
    let err = admit_with(&argv(), &env_with_caps(&digest_of(&at), "50", "20000"), at)
        .expect_err("CONTROL: at the old default 30 000 keys are refused");
    assert!(
        matches!(
            &err,
            Refusal::Plan(logweir_reaper::Refusal::OverCap {
                what: "object keys",
                found: 30_000,
                cap: 20_000
            })
        ),
        "CONTROL names 20 000: {err:?}"
    );

    // EXECUTION: the admitted 30 000-key point is deleted whole. A budget cut
    // back to 20 000 would stop at 20 000 with `BudgetExhausted` and exit 1.
    let deleter = FakeDeleter::default();
    let report = execute(
        &admitted,
        &deleter,
        &NoSleep,
        &FakeSink::default(),
        &FakeLister(Vec::new()),
    );
    let seen = deleter.seen.borrow();
    assert_eq!(
        seen.len(),
        30_000,
        "every key of the admitted point is deleted under the raised budget"
    );
    assert_eq!(seen[0], format!("{SCOPE}/set-wide/manifest.json"));
    assert_eq!(report.outcome.objects_deleted, 30_000);
    assert_eq!(report.exit_code, EXIT_OK, "{:?}", report.lines);
    assert!(report
        .lines
        .iter()
        .any(|l| l.contains("retention-point=lwp1-wide state=Deleted objects=30000")));
}

/// **A ceiling that is not a whole number of at least 1 is REFUSED (exit 3,
/// nothing deleted) and never replaced by a default**: `5O` (a letter O), `7.5`
/// or `-1` must not run with 50 points. Absent and blank are
/// `BindingIncomplete` ([`each_missing_binding_variable_is_named`]); everything
/// else is `CapUnreadable`, naming the variable and the value.
///
/// MUTANT: restore the `unwrap_or` fallback and every case here admits.
#[test]
fn a_ceiling_the_worker_cannot_read_is_refused_never_defaulted() {
    let bytes = plan_bytes(lines(1));
    let digest = digest_of(&bytes);
    for name in [
        logweir_retention::env::MAX_DELETIONS,
        logweir_retention::env::MAX_OBJECTS,
    ] {
        for bad in [
            "5O",
            "7.5",
            "-1",
            "0",
            "1e3",
            "seven",
            "9223372036854775808",
        ] {
            let mut env = full_env(&digest);
            env.insert(name.to_string(), bad.to_string());
            let err = admit_with(&argv(), &env, bytes.clone())
                .expect_err("an unreadable ceiling is refused");
            assert_eq!(
                err,
                Refusal::CapUnreadable {
                    name,
                    value: bad.to_string()
                },
                "{name}={bad}"
            );
            assert_eq!(err.exit_code(), EXIT_REFUSED);
            assert!(
                format!("{err}").contains(name) && format!("{err}").contains("nothing is deleted"),
                "the message names the variable and says nothing is deleted: {err}"
            );
        }
        // Surrounding whitespace is not a different number.
        let mut env = full_env(&digest);
        env.insert(name.to_string(), " 12 ".to_string());
        admit_with(&argv(), &env, bytes.clone()).expect("` 12 ` is twelve");
    }
    // The pure reader, at its boundary.
    assert_eq!(
        logweir_retention::cap(logweir_retention::env::MAX_OBJECTS, "1"),
        Ok(1)
    );
    assert!(logweir_retention::cap(logweir_retention::env::MAX_OBJECTS, "0").is_err());
}

/// **The binding variable names are the ones the controller projects.**
///
/// `weirkeeper` writes the enforcement Job's environment from its own constants
/// (`controllers::retention_policy::env`), this worker reads its own
/// (`logweir_retention::env`), and the two crates share no dependency edge on
/// purpose. Every binding variable is required, so a rename fails closed at run
/// time, and this row fails before that, reading the controller's source.
/// `weirkeeper/tests/configured_values.rs` is the same comparison from the
/// other side.
///
/// MUTANT: rename `MAX_OBJECTS` on either side and this fails naming it.
#[test]
fn the_binding_names_are_the_ones_the_controller_projects() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../weirkeeper/src/controllers/retention_policy.rs");
    let source =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    let start = source
        .find("pub mod env {")
        .expect("the controller names its Job's environment in `pub mod env`");
    let block = &source[start..];
    let block = &block[..block.find("\n}").expect("the module closes")];
    let projected: std::collections::BTreeSet<String> = block
        .lines()
        .filter_map(|l| {
            let l = l.trim();
            let rest = l.strip_prefix("pub const ")?;
            let value = rest.split('"').nth(1)?;
            Some(value.to_string())
        })
        .collect();
    let read: std::collections::BTreeSet<String> = logweir_retention::env::BINDING
        .iter()
        .map(|s| (*s).to_string())
        .collect();
    assert_eq!(
        projected, read,
        "the variables weirkeeper projects onto an enforcement Job and the ones this worker \
         reads must be the same set; a name on one side only is a binding that never arrives"
    );
    assert_eq!(read.len(), 9, "nine binding variables, each spelt once");
}

/// A generation that is not a number, and a location that does not parse.
#[test]
fn a_malformed_generation_or_location_is_refused() {
    let bytes = plan_bytes(vec![line("lwp1-a", "set-a", &["seg-0"])]);
    let digest = digest_of(&bytes);

    let mut env = full_env(&digest);
    env.insert(
        "LOGWEIR_RETENTION_POLICY_GENERATION".to_string(),
        "four".to_string(),
    );
    assert!(matches!(
        admit_with(&argv(), &env, bytes.clone()),
        Err(Refusal::GenerationNotANumber(_))
    ));

    let mut env = full_env(&digest);
    env.insert(
        "LOGWEIR_RETENTION_LOCATION".to_string(),
        "{not json".to_string(),
    );
    assert!(matches!(
        admit_with(&argv(), &env, bytes.clone()),
        Err(Refusal::LocationUnreadable(_))
    ));

    let mut env = full_env(&digest);
    env.remove("LOGWEIR_RETENTION_LOCATION");
    assert!(matches!(
        admit_with(&argv(), &env, bytes),
        Err(Refusal::BindingIncomplete("LOGWEIR_RETENTION_LOCATION"))
    ));
}

/// A plan file that cannot be read.
#[test]
fn an_unreadable_plan_file_is_refused() {
    let digest = digest_of(b"anything");
    let err = admit(&argv(), &full_env(&digest), |path| {
        Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("no such file: {path}"),
        ))
    })
    .expect_err("an unreadable plan is refused");
    assert!(matches!(err, Refusal::PlanUnreadable { .. }), "got {err:?}");
}

/// The reaper's own refusals reach the exit code through `admit`: a digest that
/// is not the approved one, and a key outside the scope.
#[test]
fn the_plans_own_refusals_reach_admission() {
    let bytes = plan_bytes(vec![line("lwp1-a", "set-a", &["seg-0"])]);

    // A digest nobody approved.
    let err = admit_with(&argv(), &full_env("sha256:0000"), bytes.clone())
        .expect_err("a stale approval is refused");
    assert!(matches!(err, Refusal::Plan(_)), "got {err:?}");
    assert_eq!(err.exit_code(), EXIT_REFUSED);

    // A key outside `<scope>/<backupId>/`.
    let mut rogue = line("lwp1-a", "set-a", &["seg-0"]);
    rogue
        .object_keys
        .push("kafka-backups/team-b/set-a/seg-9".to_string());
    let bytes = plan_bytes(vec![rogue]);
    let digest = digest_of(&bytes);
    let err = admit_with(&argv(), &full_env(&digest), bytes).expect_err("out of scope is refused");
    assert!(
        format!("{err}").contains("RetentionScopeViolation"),
        "the refusal opens with the state name the exit contract documents: {err}"
    );

    // A scope the Job was not told.
    let bytes = plan_bytes(vec![line("lwp1-a", "set-a", &["seg-0"])]);
    let digest = digest_of(&bytes);
    let mut env = full_env(&digest);
    env.insert(
        "LOGWEIR_RETENTION_SCOPE_PREFIX".to_string(),
        "kafka-backups".to_string(),
    );
    assert!(matches!(
        admit_with(&argv(), &env, bytes),
        Err(Refusal::Plan(_))
    ));
}

/// **The two credentials never fall back to each other.** A real run with no
/// `evidenceWrite` grant is refused BEFORE any handle is built.
#[test]
fn a_run_with_no_record_credential_is_refused_before_any_port_exists() {
    let bytes = plan_bytes(vec![line("lwp1-a", "set-a", &["seg-0"])]);
    let digest = digest_of(&bytes);
    let mut env = full_env(&digest);
    env.remove("LOGWEIR_EVIDENCE_AWS_ACCESS_KEY_ID");
    let err = admit_with(&argv(), &env, bytes.clone()).expect_err("no record sink, no deletion");
    assert!(matches!(err, Refusal::NoRecordCredential), "got {err:?}");
    assert!(
        format!("{err}").contains("not a fall-back"),
        "the message says the delete credential is deliberately not one: {err}"
    );

    // The blank case is the same case.
    let mut blank = full_env(&digest);
    blank.insert(
        "LOGWEIR_EVIDENCE_AWS_SECRET_ACCESS_KEY".to_string(),
        String::new(),
    );
    assert!(matches!(
        admit_with(&argv(), &blank, bytes.clone()),
        Err(Refusal::NoRecordCredential)
    ));

    // AND A DRY RUN NEEDS NO SINK, because it deletes nothing to attribute.
    let admitted = admit_with(&dry_argv(), &env, bytes).expect("a preview needs no record sink");
    assert!(admitted.dry_run);
    assert!(admitted.evidence_keys.is_none());
}

/// The archive credential is read from `AWS_*` and the record credential from
/// `LOGWEIR_EVIDENCE_AWS_*`, and neither reads the other's variables.
#[test]
fn the_two_credentials_are_read_from_their_own_variables() {
    let bytes = plan_bytes(vec![line("lwp1-a", "set-a", &["seg-0"])]);
    let digest = digest_of(&bytes);
    let admitted = admit_with(&argv(), &full_env(&digest), bytes).expect("admits");
    let archive = admitted.archive_keys.expect("an archive credential");
    let evidence = admitted.evidence_keys.expect("a record credential");
    assert_eq!(archive.access_key_id, "AKIADELETE");
    assert_eq!(evidence.access_key_id, "AKIAEVIDENCE");
    assert_ne!(
        archive.secret_access_key, evidence.secret_access_key,
        "the delete-capable principal must not be able to write the record that attributes its \
         own deletes"
    );
    // And neither prints its secret.
    let printed = format!("{archive:?} {evidence:?}");
    assert!(!printed.contains("delete-secret"));
    assert!(!printed.contains("evidence-secret"));
    assert!(printed.contains("access_key_id_len"));
}

// Execution.

/// **`--dry-run` deletes nothing.** Flip `"--dry-run" => { dry_run = true }`
/// to `false` and the deleter panics on the first call.
#[test]
fn a_dry_run_is_a_dry_run() {
    let bytes = plan_bytes(vec![line("lwp1-a", "set-a", &["seg-0", "seg-1"])]);
    let digest = digest_of(&bytes);
    let admitted = admit_with(&dry_argv(), &full_env(&digest), bytes).expect("admits");
    assert!(admitted.dry_run, "the flag reached the run");

    let sink = FakeSink::default();
    let report = execute(
        &admitted,
        &NeverDeletes,
        &NoSleep,
        &sink,
        &FakeLister(Vec::new()),
    );
    assert_eq!(report.exit_code, EXIT_OK);
    assert_eq!(report.outcome.objects_deleted, 0);
    assert_eq!(report.outcome.attempts, 0);
    assert!(
        sink.written.borrow().is_empty(),
        "and no tombstone: nothing was removed, so there is nothing to attribute"
    );
    assert!(report.lines.iter().any(|l| l.contains("code=DryRun")));
    assert!(result_line(&report, true).contains("dryRun=true"));
}

/// **Through the worker.** A preview over an enumerating plan prints the
/// real object count per point.
#[test]
fn a_preview_prints_the_real_object_count() {
    let mut l = line("lwp1-a", "set-a", &[]);
    l.enumerate_set = true;
    let bytes = plan_bytes(vec![l]);
    let digest = digest_of(&bytes);
    let admitted = admit_with(&dry_argv(), &full_env(&digest), bytes).expect("admits");
    let listed: Vec<String> = (0..7)
        .map(|i| format!("{SCOPE}/set-a/seg-{i}"))
        .chain(std::iter::once(format!("{SCOPE}/set-a/manifest.json")))
        .collect();
    let report = execute(
        &admitted,
        &NeverDeletes,
        &NoSleep,
        &FakeSink::default(),
        &FakeLister(listed),
    );
    let point_line = report
        .lines
        .iter()
        .find(|l| l.starts_with("retention-point="))
        .expect("a point line");
    assert!(
        point_line.contains("objects=8"),
        "the manifest plus its seven segments, not the plan's own 1: {point_line}"
    );
}

/// A real run deletes, writes both tombstone stages, and exits 0.
#[test]
fn a_complete_run_exits_zero_and_attributes_every_point() {
    let bytes = plan_bytes(vec![line("lwp1-a", "set-a", &["seg-0"])]);
    let digest = digest_of(&bytes);
    let admitted = admit_with(&argv(), &full_env(&digest), bytes).expect("admits");
    let deleter = FakeDeleter::default();
    let sink = FakeSink::default();
    let report = execute(
        &admitted,
        &deleter,
        &NoSleep,
        &sink,
        &FakeLister(Vec::new()),
    );

    assert_eq!(report.exit_code, EXIT_OK);
    assert_eq!(
        deleter.seen.borrow().as_slice(),
        &[
            format!("{SCOPE}/set-a/manifest.json"),
            format!("{SCOPE}/set-a/seg-0"),
        ],
        "manifest first, then the segments"
    );
    let written = sink.written.borrow().clone();
    assert_eq!(
        written.len(),
        3,
        "intent, the post-delete versioning check, completion: {written:?}"
    );
    assert!(written.iter().all(|k| k.starts_with("logweir/retention/")));
    assert!(report
        .lines
        .iter()
        .any(|l| l.contains("retention-point=lwp1-a state=Deleted objects=2")));
    assert!(report.lines[0].starts_with(&format!("retention-plan={digest}")));
}

/// A point that did not complete is exit 1, with its closed code on the line.
#[test]
fn an_incomplete_run_exits_one_and_names_the_code() {
    struct Denies;
    impl Deleter for Denies {
        fn delete_exact(&self, _key: &str) -> Result<(), DeleteError> {
            Err(DeleteError::AccessDenied)
        }

        fn probe_versioning(&self, _key: &str) -> Result<Versioning, DeleteError> {
            Ok(Versioning::Unversioned)
        }
    }
    let bytes = plan_bytes(vec![line("lwp1-a", "set-a", &["seg-0"])]);
    let digest = digest_of(&bytes);
    let admitted = admit_with(&argv(), &full_env(&digest), bytes).expect("admits");
    let report = execute(
        &admitted,
        &Denies,
        &NoSleep,
        &FakeSink::default(),
        &FakeLister(Vec::new()),
    );
    assert_eq!(report.exit_code, EXIT_INCOMPLETE);
    assert!(report
        .lines
        .iter()
        .any(|l| l.contains("state=Kept") && l.contains("code=AccessDenied")));
    assert!(result_line(&report, false).contains("failed=1"));
}

/// A delete marker is not a deletion, at the process boundary: on a versioned
/// bucket the run issues no delete, exits 1, and the key line the controller
/// harvests into `status.lastEnforcement.failed[]` names `VersionedBucket`,
/// never `state=Deleted`.
///
/// MUTANT: skip the probe in `logweir_reaper::attempt`. `NeverDeletes` panics
/// on the first delete and this row fails.
#[test]
fn a_versioned_bucket_exits_one_naming_the_refusal_and_deletes_nothing() {
    struct Versioned;
    impl Deleter for Versioned {
        fn delete_exact(&self, key: &str) -> Result<(), DeleteError> {
            NeverDeletes.delete_exact(key)
        }

        fn probe_versioning(&self, _key: &str) -> Result<Versioning, DeleteError> {
            Ok(Versioning::Versioned)
        }
    }
    let bytes = plan_bytes(vec![line("lwp1-a", "set-a", &["seg-0"])]);
    let digest = digest_of(&bytes);
    let admitted = admit_with(&argv(), &full_env(&digest), bytes).expect("admits");
    let report = execute(
        &admitted,
        &Versioned,
        &NoSleep,
        &FakeSink::default(),
        &FakeLister(Vec::new()),
    );
    assert_eq!(report.exit_code, EXIT_INCOMPLETE);
    assert!(
        report
            .lines
            .iter()
            .any(|l| l == "retention-point=lwp1-a state=Kept objects=0 code=VersionedBucket"),
        "{:?}",
        report.lines
    );
    assert!(result_line(&report, false).contains("deleted=0 failed=1 objects=0"));
}

/// An empty plan is a legitimate plan and exits 0 having removed nothing.
#[test]
fn an_empty_plan_exits_zero_and_removes_nothing() {
    let bytes = plan_bytes(Vec::new());
    let digest = digest_of(&bytes);
    let admitted = admit_with(&argv(), &full_env(&digest), bytes).expect("admits");
    let report = execute(
        &admitted,
        &NeverDeletes,
        &NoSleep,
        &FakeSink::default(),
        &FakeLister(Vec::new()),
    );
    assert_eq!(report.exit_code, EXIT_OK);
    assert_eq!(report.outcome.objects_deleted, 0);
}

/// The key lines are read by NAME, so their order is not a contract — but each
/// one is present and shaped as `docs/stability.md` prints it.
#[test]
fn the_key_lines_are_shaped_as_the_contract_prints_them() {
    let bytes = plan_bytes(vec![line("lwp1-a", "set-a", &["seg-0"])]);
    let digest = digest_of(&bytes);
    let admitted = admit_with(&argv(), &full_env(&digest), bytes).expect("admits");
    let report = execute(
        &admitted,
        &FakeDeleter::default(),
        &NoSleep,
        &FakeSink::default(),
        &FakeLister(Vec::new()),
    );
    let plan_line = &report.lines[0];
    assert!(plan_line.starts_with("retention-plan=sha256:"));
    assert!(plan_line.contains(" points=1 objects=2"));
    assert_eq!(
        logweir_retention::record_line("logweir/retention/u/r.json", "sha256:aa"),
        "retention-record=logweir/retention/u/r.json sha256=sha256:aa"
    );
    assert_eq!(
        result_line(&report, false),
        "retention-result=deleted=1 failed=0 objects=2"
    );
}

// The delete-capable key is used only when it is bound to this policy.

/// **An unbound or foreign delete key — or record key — is refused at
/// admission, before the plan file is even read; its own binding admits.**
///
/// The confused deputy: a `RetentionPolicy` author who cannot read Secrets
/// names another policy's delete-capable Secret, or keeps theirs while the
/// destination of that NAME is re-created at an endpoint they control. The
/// worker signs requests (and deletes) with whatever Secret the Job projected,
/// so the binding is checked first.
///
/// KILLS: the admission check dropped; only one of the two pairs checked; the
/// check placed after the plan read.
#[test]
fn fx20_an_unbound_or_foreign_credential_is_refused_before_anything_is_read() {
    let bytes = plan_bytes(vec![line("lwp1-a", "set-a", &["seg-0"])]);
    let digest = digest_of(&bytes);
    let route = logweir_core::engine::StorageUrl::S3 {
        bucket: "kafka-backups".to_string(),
        prefix: SCOPE.to_string(),
        region: Some("us-east-1".to_string()),
        endpoint: Some("http://minio.storage.svc:9000".to_string()),
        path_style: true,
        allow_http: true,
    };
    let delete_binding = logweir_core::credential_binding::retention_binding(UID, &route, SCOPE);
    let record_binding = logweir_core::credential_binding::destination_binding("dest-uid", &route);
    let bound = |archive: Option<&str>, evidence: Option<&str>| {
        let mut env = full_env(&digest);
        env.insert(
            "LOGWEIR_ARCHIVE_CREDENTIAL_BINDING_EXPECTED".to_string(),
            delete_binding.clone(),
        );
        env.insert(
            "LOGWEIR_EVIDENCE_CREDENTIAL_BINDING_EXPECTED".to_string(),
            record_binding.clone(),
        );
        if let Some(v) = archive {
            env.insert(
                "LOGWEIR_ARCHIVE_CREDENTIAL_BINDING".to_string(),
                v.to_string(),
            );
        }
        if let Some(v) = evidence {
            env.insert(
                "LOGWEIR_EVIDENCE_CREDENTIAL_BINDING".to_string(),
                v.to_string(),
            );
        }
        env
    };
    let other_policy = logweir_core::credential_binding::retention_binding(
        "99999999-0000-4000-8000-000000000099",
        &route,
        SCOPE,
    );
    for (row, env, variable) in [
        (
            "delete key unbound",
            bound(None, Some(&record_binding)),
            "LOGWEIR_ARCHIVE_CREDENTIAL_BINDING",
        ),
        (
            "delete key bound to another policy",
            bound(Some(&other_policy), Some(&record_binding)),
            "LOGWEIR_ARCHIVE_CREDENTIAL_BINDING",
        ),
        (
            "record key bound elsewhere",
            bound(Some(&delete_binding), Some(&other_policy)),
            "LOGWEIR_EVIDENCE_CREDENTIAL_BINDING",
        ),
    ] {
        let read = std::cell::Cell::new(false);
        let err = admit(&argv(), &env, |_| {
            read.set(true);
            Ok(bytes.clone())
        })
        .expect_err(row);
        assert!(
            matches!(err, Refusal::CredentialBinding(_)),
            "{row}: {err:?}"
        );
        assert_eq!(err.exit_code(), EXIT_REFUSED, "{row}");
        assert_eq!(err.code(), "CredentialBindingMismatch", "{row}");
        let message = err.to_string();
        assert!(
            message.starts_with("CredentialBindingMismatch: ") && message.contains(variable),
            "{row}: {message}"
        );
        assert!(!message.contains("delete-secret") && !message.contains(&other_policy));
        assert!(
            !read.get(),
            "{row}: the plan was read before the binding refusal"
        );
    }
    // CONTROL: both bound — admitted, and both keys carried.
    let admitted = admit_with(
        &argv(),
        &bound(Some(&delete_binding), Some(&record_binding)),
        bytes.clone(),
    )
    .expect("both credentials bound to this run admit");
    assert!(admitted.archive_keys.is_some() && admitted.evidence_keys.is_some());
}

/// Run the shipped binary once over `full_env` plus `extra`, with a deadline:
/// its exit code, stdout and stderr.
fn run_shipped(extra: &[(&str, &str)]) -> (Option<i32>, String, String) {
    run_shipped_with(extra, &[])
}

/// [`run_shipped`] with arguments appended to the argv.
fn run_shipped_with(extra: &[(&str, &str)], args: &[&str]) -> (Option<i32>, String, String) {
    // No `tempfile` dev-dependency in this crate: a unique scratch directory
    // under the system temp dir, removed at the end.
    let dir = std::env::temp_dir().join(format!(
        "fx20-retention-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos())
    ));
    std::fs::create_dir_all(&dir).expect("scratch");
    let plan = dir.join("plan.json");
    let bytes = plan_bytes(vec![line("lwp1-a", "set-a", &["seg-0"])]);
    std::fs::write(&plan, &bytes).expect("plan written");
    let digest = digest_of(&bytes);
    let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_logweir-retention"));
    cmd.args([
        "run",
        "--plan",
        plan.to_str().expect("utf-8"),
        "--retention-contract-version",
        "1",
    ])
    .args(args)
    .env_clear()
    .stdin(std::process::Stdio::null())
    .stdout(std::process::Stdio::piped())
    .stderr(std::process::Stdio::piped());
    for (k, v) in full_env(&digest) {
        cmd.env(k, v);
    }
    for (k, v) in extra {
        cmd.env(k, v);
    }
    let mut child = cmd.spawn().expect("the worker spawns");
    let started = std::time::Instant::now();
    let status = loop {
        if let Some(status) = child.try_wait().expect("try_wait") {
            break status;
        }
        if started.elapsed() > Duration::from_secs(60) {
            let _ = child.kill();
            let _ = child.wait();
            panic!("the worker did not exit within 60 s");
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    let output = child.wait_with_output().expect("output");
    let _ = std::fs::remove_dir_all(&dir);
    (
        status.code(),
        String::from_utf8_lossy(&output.stdout).into_owned(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

/// **The shipped binary prints the closed `retention-refusal=` code its
/// controller publishes, exits 3, and deletes nothing.** A process row, with a
/// deadline, because the line is written by `main` and not by the library.
#[test]
fn fx20_the_shipped_worker_names_a_binding_refusal_on_stdout() {
    let (status, stdout, stderr) = run_shipped(&[
        (
            "LOGWEIR_ARCHIVE_CREDENTIAL_BINDING_EXPECTED",
            "v1:mine:sha256:00",
        ),
        ("LOGWEIR_ARCHIVE_CREDENTIAL_BINDING", "v1:theirs:sha256:11"),
    ]);
    assert_eq!(status, Some(EXIT_REFUSED), "stderr: {stderr}");
    assert_eq!(
        stdout.lines().collect::<Vec<_>>(),
        vec!["retention-refusal=CredentialBindingMismatch"],
        "the closed code and nothing else; stderr: {stderr}"
    );
    assert!(stderr.contains("nothing is deleted"), "{stderr}");
    assert!(!stderr.contains("delete-secret") && !stdout.contains("delete-secret"));
}

/// **Every refusal is named, by a code from the closed set the
/// controller reads, and a refusal about one binding variable names it.**
/// One case per variant (the reaper's plan refusals each by its own), so the
/// 22 codes are each reached once and no two variants share a code except the
/// two scope violations, which are one refusal to an operator.
///
/// KILLS: a variant mapped to a code outside [`REFUSAL_CODES`]; a variable
/// dropped from the line (CONTROL: a refusal without one prints none).
#[test]
fn fx15_every_refusal_has_a_closed_code_and_names_its_variable() {
    use logweir_reaper::Refusal as Plan;
    let s = String::new;
    let cases: Vec<(Refusal, &str, Option<&str>)> = vec![
        (Refusal::NotRun(s()), "NotRun", None),
        (Refusal::UnknownArgument(s()), "UnknownArgument", None),
        (Refusal::ContractVersion(s()), "ContractVersion", None),
        (Refusal::NoPlanPath, "NoPlanPath", None),
        (
            Refusal::PlanUnreadable {
                path: s(),
                error: s(),
            },
            "PlanUnreadable",
            None,
        ),
        (
            Refusal::BindingIncomplete("LOGWEIR_RETENTION_RUN_ID"),
            "BindingIncomplete",
            Some("LOGWEIR_RETENTION_RUN_ID"),
        ),
        (
            Refusal::GenerationNotANumber(s()),
            "GenerationNotANumber",
            Some("LOGWEIR_RETENTION_POLICY_GENERATION"),
        ),
        (
            Refusal::CapUnreadable {
                name: "LOGWEIR_RETENTION_MAX_DELETIONS",
                value: "0".to_string(),
            },
            "CapUnreadable",
            Some("LOGWEIR_RETENTION_MAX_DELETIONS"),
        ),
        (
            Refusal::LocationUnreadable(s()),
            "LocationUnreadable",
            Some("LOGWEIR_RETENTION_LOCATION"),
        ),
        (Refusal::NoRecordCredential, "NoRecordCredential", None),
        (
            Refusal::CredentialBinding(s()),
            "CredentialBindingMismatch",
            None,
        ),
        (Refusal::Port(s()), "PortUnavailable", None),
        (Plan::Unreadable(s()).into(), "PlanUnparseable", None),
        (Plan::WrongFormat(s()).into(), "PlanWrongFormat", None),
        (
            Plan::DigestMismatch {
                found: s(),
                approved: s(),
            }
            .into(),
            "PlanDigestMismatch",
            None,
        ),
        (
            Plan::PolicyMismatch {
                found: s(),
                expected: s(),
            }
            .into(),
            "PlanPolicyMismatch",
            None,
        ),
        (
            Plan::ScopeViolation {
                key: s(),
                point_id: s(),
                expected: s(),
            }
            .into(),
            "RetentionScopeViolation",
            None,
        ),
        (
            Plan::EvidenceRoot {
                key: s(),
                point_id: s(),
            }
            .into(),
            "RetentionScopeViolation",
            None,
        ),
        (
            Plan::ManifestNotFirst {
                point_id: s(),
                first: s(),
                manifest_key: s(),
            }
            .into(),
            "PlanManifestNotFirst",
            None,
        ),
        (
            Plan::OverCap {
                what: "points",
                found: 2,
                cap: 1,
            }
            .into(),
            "PlanOverCap",
            None,
        ),
        (Plan::EmptyLine(s()).into(), "PlanEmptyLine", None),
        (Plan::DuplicatePoint(s()).into(), "PlanDuplicatePoint", None),
        (
            Plan::DuplicateKey {
                key: s(),
                first: s(),
                second: s(),
            }
            .into(),
            "PlanDuplicateKey",
            None,
        ),
    ];
    let mut reached = std::collections::BTreeSet::new();
    for (refusal, code, variable) in &cases {
        assert_eq!(refusal.code(), *code, "{refusal:?}");
        assert!(
            REFUSAL_CODES.contains(code),
            "{code} is not in REFUSAL_CODES"
        );
        assert_eq!(refusal.variable(), *variable, "{refusal:?}");
        if let Some(name) = variable {
            assert!(env::BINDING.contains(name), "{name} is a binding name");
        }
        let want = match variable {
            Some(name) => format!("retention-refusal={code} variable={name}"),
            None => format!("retention-refusal={code}"),
        };
        assert_eq!(refusal.refusal_line(), want);
        reached.insert(*code);
    }
    let all: std::collections::BTreeSet<&str> = REFUSAL_CODES.iter().copied().collect();
    assert_eq!(
        reached, all,
        "every code is some refusal's, and no code is spare"
    );
    assert_eq!(all.len(), REFUSAL_CODES.len(), "the codes are distinct");
}

/// **The shipped binary names a ceiling it cannot read, by code and
/// variable, on stdout.** The process half of the row above: the line is
/// written by `main`, through the one exit-3 helper every refusal takes.
///
/// KILLS: `main` printing the code without the variable, or nothing.
#[test]
fn fx15_the_shipped_worker_names_an_unreadable_ceiling_and_its_variable() {
    let (status, stdout, stderr) = run_shipped(&[("LOGWEIR_RETENTION_MAX_OBJECTS", "0")]);
    assert_eq!(status, Some(EXIT_REFUSED), "stderr: {stderr}");
    assert_eq!(
        stdout.lines().collect::<Vec<_>>(),
        vec!["retention-refusal=CapUnreadable variable=LOGWEIR_RETENTION_MAX_OBJECTS"],
        "stderr: {stderr}"
    );
    assert!(stderr.contains("nothing is deleted"), "{stderr}");
}

/// A token for the FX-43 rows, assembled so that no source line holds a
/// secret-shaped literal.
fn fx43_token() -> logweir_core::refusal_detail::LineToken {
    logweir_core::refusal_detail::LineToken::parse(&"7f".repeat(20)).expect("forty hex digits")
}

/// **FX-43: the shipped worker given `--runner-line-token` seals its lines and
/// its end, and the plain line stays last.** A binding refusal (exit 3): the
/// controller reads only the sealed `retention-refusal=`, an older controller
/// the plain one, and the human line on stderr is one line.
///
/// KILLS: the block printed without its seal or its end line; the token flag
/// refused as an unknown argument; the stderr text printed raw.
#[test]
fn fx43_the_shipped_worker_seals_its_lines_and_its_end() {
    let t = fx43_token();
    let (status, stdout, stderr) = run_shipped_with(
        &[
            (
                "LOGWEIR_ARCHIVE_CREDENTIAL_BINDING_EXPECTED",
                "v1:mine:sha256:00",
            ),
            ("LOGWEIR_ARCHIVE_CREDENTIAL_BINDING", "v1:theirs:sha256:11"),
        ],
        &[
            logweir_core::runner_line::RUNNER_LINE_TOKEN_ARG,
            t.expose_token(),
        ],
    );
    assert_eq!(status, Some(EXIT_REFUSED), "stderr: {stderr}");
    let sealed: Vec<&str> = stdout
        .lines()
        .filter_map(|l| logweir_core::runner_line::unseal(&t, l))
        .collect();
    assert_eq!(
        sealed,
        vec![
            "retention-refusal=CredentialBindingMismatch",
            "runner-exit=3"
        ],
        "{stdout}"
    );
    assert_eq!(
        stdout.lines().last(),
        Some("retention-refusal=CredentialBindingMismatch"),
        "the plain line last, for an older controller: {stdout}"
    );
    assert_eq!(stderr.lines().count(), 1, "{stderr}");
    // NEGATIVE CONTROL: without the flag, the bytes it always printed.
    let (_, plain, _) = run_shipped(&[
        (
            "LOGWEIR_ARCHIVE_CREDENTIAL_BINDING_EXPECTED",
            "v1:mine:sha256:00",
        ),
        ("LOGWEIR_ARCHIVE_CREDENTIAL_BINDING", "v1:theirs:sha256:11"),
    ]);
    assert_eq!(plain, "retention-refusal=CredentialBindingMismatch\n");
}

/// The token pair is taken off the argv wherever it stands; a malformed
/// value or a second pair is left for the argv grammar, which refuses it.
#[test]
fn fx43_the_line_token_is_taken_off_the_argv() {
    let t = fx43_token();
    let flag = logweir_core::runner_line::RUNNER_LINE_TOKEN_ARG;
    let argv = ["run", flag, t.expose_token(), "--dry-run"];
    let (rest, token) = logweir_retention::take_line_token(&argv);
    assert_eq!(rest, vec!["run", "--dry-run"]);
    assert_eq!(token, Some(t.clone()));
    let (rest, token) = logweir_retention::take_line_token(&["run", flag, "not-hex"]);
    assert_eq!(rest, vec!["run", flag, "not-hex"]);
    assert!(token.is_none());
    assert!(matches!(
        logweir_retention::parse_args(&rest),
        Err(logweir_retention::Refusal::UnknownArgument(_))
    ));
    let (rest, _) = logweir_retention::take_line_token(&[
        "run",
        flag,
        t.expose_token(),
        flag,
        t.expose_token(),
    ]);
    assert_eq!(rest, vec!["run", flag, t.expose_token()]);
}

/// **FX-43: the worker's error text is printed on ONE line.** A plan path
/// holding a line break and a forged `retention-refusal=` (an unreadable
/// plan's error repeats the path): exit 3 `PlanUnreadable`, and no line of
/// stdout or stderr starts with the forged key; the genuine refusal is the one
/// stdout line.
///
/// KILLS: `refuse` printing the refusal's text raw.
#[test]
fn fx43_the_workers_error_text_never_starts_a_line() {
    let forged = "retention-refusal=CredentialBindingMismatch";
    let dir = std::env::temp_dir().join(format!("fx43-retention-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("scratch");
    let plan = format!("{}/absent\n{forged}", dir.display());
    let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_logweir-retention"));
    cmd.args(["run", "--plan", &plan, "--retention-contract-version", "1"])
        .env_clear()
        .stdin(std::process::Stdio::null());
    for (k, v) in full_env(&digest_of(b"x")) {
        cmd.env(k, v);
    }
    let out = cmd.output().expect("the worker runs");
    let _ = std::fs::remove_dir_all(&dir);
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(out.status.code(), Some(EXIT_REFUSED), "{stderr}");
    assert_eq!(stdout, "retention-refusal=PlanUnreadable\n");
    assert!(
        stderr.contains("\\n"),
        "the path is shown, escaped: {stderr}"
    );
    for line in stdout.lines().chain(stderr.lines()) {
        assert!(!line.starts_with(forged), "{line}");
    }
}
