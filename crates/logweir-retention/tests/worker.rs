//! The retention worker's own rows — review `d3w9` **H6**.
//!
//! Before this file, `cargo test -p logweir-retention` ran **zero tests**
//! against the one executable in this product that deletes archive data: no
//! `tests/` directory, no `#[cfg(test)]`, and no other crate can link a
//! `[[bin]]`. The reviewer proved what that costs by flipping
//! `"--dry-run" => dry_run = true` to `false` — so a preview deletes for real —
//! and re-running every suite that could observe the file: 41 suites, all
//! green. `a_dry_run_is_a_dry_run` below is the row that kills it.
//!
//! Everything here drives the shipped [`logweir_retention::admit`] and
//! [`logweir_retention::execute`]. `admit` is pure: it takes the argv, an
//! environment map and a plan-reading closure, so **all thirteen exit-3
//! refusals are table rows** with no socket, no bucket and no process
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
    admit, env_map, execute, parse_args, result_line, Refusal, EXIT_INCOMPLETE, EXIT_OK,
    EXIT_REFUSED,
};

const SCOPE: &str = "kafka-backups/team-a";
const UID: &str = "16161616-0000-4000-8000-000000000016";

// ===========================================================================
// Fixtures
// ===========================================================================

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

// ===========================================================================
// Ports
// ===========================================================================

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

// ===========================================================================
// The argv
// ===========================================================================

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

// ===========================================================================
// Admission — every exit-3 path, with no port built
// ===========================================================================

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

/// Each of the five required variables, missing in turn, is named.
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

// ===========================================================================
// Execution
// ===========================================================================

/// **The mutant the reviewer planted, killed.** `--dry-run` deletes nothing.
///
/// Flip `"--dry-run" => { dry_run = true }` to `false` and this row fails: the
/// deleter panics on the first call.
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

/// **M1 through the worker.** A preview over an enumerating plan prints the
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
    assert_eq!(written.len(), 2, "intent and completion: {written:?}");
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

/// Defect OBJECT-LOCK-DELETE-MARKER at the process boundary: on a versioned
/// bucket the run issues no delete, exits 1, and the key line the controller
/// harvests into `status.lastEnforcement.failed[]` names `VersionedBucket` —
/// the line that used to read `state=Deleted` over a held point's markers.
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
