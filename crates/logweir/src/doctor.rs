//! `logweir doctor` — preflight checks, run before `logweir drill run` is
//! attempted. Task 14b.
//!
//! Seven checks, each with its own gate: engine binary present, engine
//! version pinned, drill spec parses, allowed-clusters parses, approver key
//! loads, source storage reachable, target cluster reachable/allowed/marked.
//! The list short-circuits on the first FAILURE (a reviewer sees all seven
//! checks in one place and the first hard stop wins), but a check that
//! genuinely could not be exercised (no live broker, no live bucket) reports
//! `Skipped` and the run continues — it is never silently reported `ok` on
//! the strength of not having looked, and it never blocks a run that had no
//! way to look. See `CheckResult` below.
use crate::exit::ExitCode;
use logweir_kafka::reader::{AuthConfig, ClusterReader};
use std::path::PathBuf;

pub struct DoctorArgs {
    pub spec: PathBuf,
    pub allowed_clusters: PathBuf,
    pub approver_key: PathBuf,
}

/// The outcome of one check. `Skipped` exists so a check that needs a live
/// dependency (`just e2e-up`, Task 7b) is never reported green on the
/// strength of not having looked, and never fails a run that had no way to
/// look.
pub enum CheckResult {
    Passed(String),
    Skipped(String),
    Failed(String),
}

impl From<Result<String, String>> for CheckResult {
    fn from(r: Result<String, String>) -> Self {
        match r {
            Ok(detail) => CheckResult::Passed(detail),
            Err(why) => CheckResult::Failed(why),
        }
    }
}

pub type Check<'a> = (&'static str, Box<dyn Fn() -> CheckResult + 'a>);

pub fn run(args: &DoctorArgs) -> ExitCode {
    let checks: Vec<Check> = vec![
        ("engine binary", Box::new(|| check_engine_present().into())),
        ("engine version", Box::new(|| check_engine_version().into())),
        ("drill spec", Box::new(|| check_spec(&args.spec).into())),
        (
            "allowed-clusters",
            Box::new(|| check_allowed(&args.allowed_clusters).into()),
        ),
        (
            "approver key",
            Box::new(|| check_approver(&args.approver_key).into()),
        ),
        ("storage", Box::new(|| check_storage(&args.spec))),
        (
            "target",
            Box::new(|| check_target(&args.spec, &args.allowed_clusters)),
        ),
    ];
    let mut skipped = 0usize;
    for (name, f) in checks {
        match f() {
            CheckResult::Passed(detail) => println!("ok    {name:<18} {detail}"),
            CheckResult::Skipped(why) => {
                skipped += 1;
                println!("skip  {name:<18} {why}");
            }
            CheckResult::Failed(why) => {
                eprintln!("FAIL  {name:<18} {why}");
                return ExitCode::Operational;
            }
        }
    }
    if skipped > 0 {
        println!("\n{skipped} check(s) skipped — run `just e2e-up` to exercise them.");
    } else {
        println!(
            "\nAll checks passed. `logweir drill run` should work against this configuration."
        );
    }
    ExitCode::Ok
}

/// 1. `$LOGWEIR_ENGINE_BIN`, else /usr/local/bin/kafka-backup, else `kafka-backup`
///    on $PATH. On failure name the pinned digest and the glibc floor, because
///    the standalone binary carries no engine and that is the usual cause.
fn check_engine_present() -> Result<String, String> {
    let p = engine_path();
    if p.exists() {
        return Ok(p.display().to_string());
    }
    let digest = std::fs::read_to_string("third_party/kafka-backup-binary.digest")
        .unwrap_or_else(|_| "sha256:<run `just engine`>".into());
    Err(format!(
        "no engine at {}. Logweir shells out to kafka-backup pinned at {} \
         (Global Constraint 7). The standalone `logweir` binary carries NO engine: \
         install that digest on $PATH, or use the container image. The engine is \
         dynamically linked and needs glibc >= 2.36, libssl3 and a CA bundle \
         (docs/stability.md), so it cannot run in a musl or bare-distroless image.",
        p.display(),
        digest.trim()
    ))
}

/// 2. `--version` matches the pin.
fn check_engine_version() -> Result<String, String> {
    let out = std::process::Command::new(engine_path())
        .arg("--version")
        .output()
        .map_err(|e| format!("cannot execute the engine: {e}"))?;
    let s =
        String::from_utf8_lossy(&out.stdout).to_string() + &String::from_utf8_lossy(&out.stderr);
    if s.contains("0.21.0") {
        Ok(s.trim().to_string())
    } else {
        Err(format!(
            "version mismatch: expected 0.21.0, engine reports `{}`",
            s.trim()
        ))
    }
}

fn check_spec(p: &std::path::Path) -> Result<String, String> {
    let t = std::fs::read_to_string(p).map_err(|e| format!("drill spec {}: {e}", p.display()))?;
    let sp: logweir_core::spec::DrillSpec = serde_yaml::from_str(&t)
        .map_err(|e| format!("drill spec {} does not parse: {e}", p.display()))?;
    Ok(format!(
        "{} topic(s), prefix `{}`",
        sp.source.topics.len(),
        sp.target.topic_mapping_prefix
    ))
}

fn check_allowed(p: &std::path::Path) -> Result<String, String> {
    let t =
        std::fs::read_to_string(p).map_err(|e| format!("allowed-clusters {}: {e}", p.display()))?;
    let a: logweir_core::spec::AllowedClusters = serde_json::from_str(&t)
        .map_err(|e| format!("allowed-clusters {} does not parse: {e}", p.display()))?;
    Ok(format!(
        "{} allowed cluster id(s)",
        a.allowed_cluster_ids.len()
    ))
}

fn check_approver(p: &std::path::Path) -> Result<String, String> {
    logweir_evidence::keys::VerifyingKey::from_pem_file(p)
        .map(|k| format!("key {}", k.key_id()))
        .map_err(|e| format!("approver key {}: {e}", p.display()))
}

/// 6. One `list` against the configured prefix — proves credentials, endpoint
///    and bucket in a single call. A storage configuration that will not
///    construct is a FAILURE; a well-formed one this process cannot reach is a
///    SKIP, because `cargo test` and CI run with no MinIO (`just e2e-up`,
///    Task 7b).
///
/// Uses `Store::read_only_from_url`, never `Store::from_url`: `source.storage`
/// names the OSO archive being READ, not the evidence bucket that
/// `from_url`'s Global-Constraint-6 guard protects (it refuses any prefix not
/// under `logweir/`), and an archive prefix is never under that root — every
/// real drill spec's `source.storage`, not just a malformed one, would trip
/// that guard. `crate::drill::mod::context` makes the identical distinction
/// for the orchestrator's own two `Store` handles (`from_url` for
/// `spec.evidence`, `read_only_from_url` for `spec.source.storage`), and this
/// check must agree with it rather than invent a second reading of the type.
fn check_storage(spec: &std::path::Path) -> CheckResult {
    let sp = match parse_spec(spec) {
        Ok(sp) => sp,
        Err(e) => return CheckResult::Failed(format!("storage: cannot read the drill spec: {e}")),
    };
    let st = match logweir_engine_oso::storage::Store::read_only_from_url(&sp.source.storage) {
        Ok(st) => st,
        Err(e) => return CheckResult::Failed(format!("storage: {e}")),
    };
    match st.list_manifest_keys(sp.source.storage.prefix()) {
        Ok(keys) => CheckResult::Passed(format!(
            "{} backup set(s) under {}",
            keys.len(),
            sp.source.storage.prefix()
        )),
        Err(e) => CheckResult::Skipped(format!(
            "storage at {} not reachable ({e}); run `just e2e-up` to exercise this check",
            sp.source.storage.prefix()
        )),
    }
}

/// 7. Reachability, cluster_id membership and the marker topic — the three
///    facts phase 0 refuses on, checked before a drill is attempted. Always a
///    FAILURE, never a skip: an adopter with a firewall problem must read a
///    NAMED `target` failure, not "doctor failed" and nothing else.
fn check_target(spec: &std::path::Path, allowed: &std::path::Path) -> CheckResult {
    let sp = match parse_spec(spec) {
        Ok(sp) => sp,
        Err(e) => return CheckResult::Failed(e),
    };
    let t = match std::fs::read_to_string(allowed) {
        Ok(t) => t,
        Err(e) => return CheckResult::Failed(e.to_string()),
    };
    let al: logweir_core::spec::AllowedClusters = match serde_json::from_str(&t) {
        Ok(al) => al,
        Err(e) => return CheckResult::Failed(e.to_string()),
    };
    let r = match logweir_kafka::rdkafka_reader::RdKafkaReader::connect(
        &sp.target.bootstrap_servers,
        AuthConfig::Plaintext,
    ) {
        Ok(r) => r,
        Err(e) => {
            return CheckResult::Failed(format!(
                "target unreachable at {:?}: {e}",
                sp.target.bootstrap_servers
            ))
        }
    };
    evaluate_target(&sp, &al, &r)
}

/// The reachability/allow-list/marker-topic evaluation, factored out of
/// `check_target` so it can be exercised directly against a stub
/// `ClusterReader` — no broker needed — in `tests::` below. Mirrors
/// `drill::phase0_admit::run`'s identical split for the identical reason
/// (see that function's own doc comment): the marker topic must be
/// CONFIRMED HEALTHY, not merely named in the list. Fix round 1: this used
/// to check presence by name alone (`topics.iter().any(...)`), which is the
/// same defect Task 14 fixed in phase 0 — a topic mid-leader-election, or
/// one this principal cannot describe, still appears in `list_topics`'s
/// output (by `TopicMeta`'s own contract) carrying `partitions: 0` and an
/// `error`, and reporting "ok" on name alone would send an adopter into a
/// drill phase 0 immediately refuses — the worst possible ordering for
/// trust in a command whose one job is catching this before a drill runs.
fn evaluate_target(
    sp: &logweir_core::spec::DrillSpec,
    al: &logweir_core::spec::AllowedClusters,
    r: &dyn ClusterReader,
) -> CheckResult {
    let id = match r.cluster_id() {
        Ok(id) => id,
        Err(e) => return CheckResult::Failed(format!("target unreachable: {e}")),
    };
    if !al.allowed_cluster_ids.contains(&id) {
        return CheckResult::Failed(format!(
            "target cluster_id {id} is not in allowedClusterIds"
        ));
    }
    let topics = match r.list_topics() {
        Ok(topics) => topics,
        Err(e) => return CheckResult::Failed(format!("target: {e}")),
    };
    match topics.iter().find(|t| t.name == sp.target.marker_topic) {
        None => CheckResult::Failed(format!(
            "target marker topic `{}` does not exist. Create it on the SCRATCH \
             cluster only — its existence is the v0.1 segregation proof.",
            sp.target.marker_topic
        )),
        Some(t) if t.error.is_some() => CheckResult::Failed(format!(
            "target marker topic `{}` exists but its metadata carried an error, so its \
             presence cannot be confirmed healthy: {}. Recreate it healthy on the SCRATCH \
             cluster only — its existence is the v0.1 segregation proof.",
            sp.target.marker_topic,
            t.error.as_deref().unwrap_or("<no detail>")
        )),
        Some(_) => CheckResult::Passed(format!("cluster {id}, marker topic present")),
    }
}

fn parse_spec(p: &std::path::Path) -> Result<logweir_core::spec::DrillSpec, String> {
    let t = std::fs::read_to_string(p).map_err(|e| e.to_string())?;
    serde_yaml::from_str(&t).map_err(|e| e.to_string())
}

fn engine_path() -> PathBuf {
    if let Ok(p) = std::env::var("LOGWEIR_ENGINE_BIN") {
        return PathBuf::from(p);
    }
    let d = PathBuf::from("/usr/local/bin/kafka-backup");
    if d.exists() {
        d
    } else {
        PathBuf::from("kafka-backup")
    }
}

/// Fix round 1 (coordinator ruling on the task-14b review): the CLI-level
/// tests in `crates/logweir/tests/doctor.rs` assert on `run()`'s AGGREGATE
/// output. That is blind to one specific mutation: gutting a check's
/// function body to a fake `Ok`/`Passed` while leaving its entry in the
/// `checks` Vec untouched, because the entry's own static label still
/// prints on an "ok" line, and a later check's real, unrelated failure
/// (`storage`'s unreachable-endpoint skip, or `target`'s ~20s
/// unreachable-broker timeout) still keeps the overall exit code at 1 —
/// see task-14b-report.md's "mutant table" for the four survivors this
/// found (mutants 3-6). These unit tests call each check function DIRECTLY
/// and assert on ITS OWN return value, so they die at assertion time
/// regardless of what any other check does. The existing named tests in
/// `tests/doctor.rs` are unmodified; these are additive.
#[cfg(test)]
mod tests {
    use super::*;
    use logweir_core::spec::AllowedClusters;
    use logweir_kafka::reader::{ConsumedRecord, KafkaError, TopicMeta};
    use std::collections::BTreeMap;
    use std::path::Path;

    #[test]
    fn check_spec_names_an_unparsable_file_as_a_drill_spec_problem() {
        let e = check_spec(Path::new("../../Cargo.toml")).unwrap_err();
        assert!(e.contains("drill spec"), "got: {e}");
    }

    #[test]
    fn check_allowed_names_an_unparsable_file_as_an_allowed_clusters_problem() {
        let e = check_allowed(Path::new("../../Cargo.toml")).unwrap_err();
        assert!(e.contains("allowed-clusters"), "got: {e}");
    }

    #[test]
    fn check_approver_names_an_unloadable_file_as_an_approver_key_problem() {
        let e = check_approver(Path::new("../../Cargo.toml")).unwrap_err();
        assert!(e.contains("approver key"), "got: {e}");
    }

    #[test]
    fn check_storage_names_an_unconstructable_config_as_a_storage_problem() {
        match check_storage(Path::new("../../e2e/fixtures/drill-bad-storage.yaml")) {
            CheckResult::Failed(why) => assert!(why.contains("storage"), "got: {why}"),
            CheckResult::Passed(d) => panic!("expected Failed, got Passed({d})"),
            CheckResult::Skipped(w) => panic!("expected Failed, got Skipped({w})"),
        }
    }

    /// "Could not determine" must be distinguishable from "verified fine".
    /// `examples/drill.yaml`'s source storage constructs fine (a well-formed
    /// S3 config) but this process has no MinIO to reach (`just e2e-up`,
    /// Task 7b, is not running under default `cargo test`) — that MUST be a
    /// `Skipped`, structurally distinct from `Passed`, never folded into a
    /// bare `Ok`/"ok" that a reader can't tell from a genuine pass.
    #[test]
    fn check_storage_reports_skipped_not_passed_when_it_cannot_reach_a_well_formed_target() {
        match check_storage(Path::new("../../examples/drill.yaml")) {
            CheckResult::Skipped(why) => assert!(!why.is_empty()),
            CheckResult::Passed(d) => panic!(
                "a storage this process cannot reach must never report Passed \
                 (undetectable false confidence): got Passed({d})"
            ),
            CheckResult::Failed(why) => panic!(
                "a well-formed but unreachable storage must Skip, not Fail: got Failed({why})"
            ),
        }
    }

    struct StubReader {
        cluster_id: Result<String, KafkaError>,
        topics: Result<Vec<TopicMeta>, KafkaError>,
    }

    impl ClusterReader for StubReader {
        fn cluster_id(&self) -> Result<String, KafkaError> {
            self.cluster_id.clone()
        }
        fn list_topics(&self) -> Result<Vec<TopicMeta>, KafkaError> {
            self.topics.clone()
        }
        fn end_offsets(&self, _topic: &str) -> Result<Vec<(i32, i64)>, KafkaError> {
            Ok(vec![])
        }
        fn topic_configs(&self, _topic: &str) -> Result<BTreeMap<String, String>, KafkaError> {
            Ok(BTreeMap::new())
        }
        fn consume_range(
            &self,
            _topic: &str,
            _partition: i32,
            _from: i64,
            _max: usize,
        ) -> Result<Vec<ConsumedRecord>, KafkaError> {
            Ok(vec![])
        }
    }

    fn spec_with_marker_topic(marker: &str) -> logweir_core::spec::DrillSpec {
        let yaml = format!(
            "source:\n  storage:\n    backend: filesystem\n    path: /tmp\n  topics: [orders]\n\
             target:\n  bootstrap_servers: [localhost:9092]\n  marker_topic: \"{marker}\"\n  \
             topic_mapping_prefix: \"drill-\"\n\
             sample:\n  window_start: \"2026-01-01T00:00:00Z\"\n  window_end: \"2026-01-02T00:00:00Z\"\n\
             objectives: {{}}\n\
             evidence:\n  backend: filesystem\n  path: /tmp\n"
        );
        serde_yaml::from_str(&yaml).expect("fixture spec must parse")
    }

    fn allowed(ids: &[&str]) -> AllowedClusters {
        AllowedClusters {
            allowed_cluster_ids: ids.iter().map(|s| s.to_string()).collect(),
            source_cluster_id: None,
        }
    }

    #[test]
    fn evaluate_target_passes_when_the_marker_topic_is_healthy() {
        let sp = spec_with_marker_topic("logweir.scratch");
        let al = allowed(&["ALLOWED0000000000000000"]);
        let r = StubReader {
            cluster_id: Ok("ALLOWED0000000000000000".into()),
            topics: Ok(vec![TopicMeta::new("logweir.scratch", 1)]),
        };
        match evaluate_target(&sp, &al, &r) {
            CheckResult::Passed(_) => {}
            other => panic!(
                "expected Passed, got a Failed/Skipped instead: {}",
                match other {
                    CheckResult::Failed(w) => w,
                    CheckResult::Skipped(w) => w,
                    CheckResult::Passed(_) => unreachable!(),
                }
            ),
        }
    }

    /// Fix round 1, item 2: the identical defect Task 14 fixed in phase 0
    /// (`drill::phase0_admit`) — a marker topic present in `list_topics`'s
    /// output but carrying an `error` (leader election, an authorization
    /// gap) must be a NAMED failure, distinguishable from the topic being
    /// absent, never read as healthy on name alone.
    #[test]
    fn evaluate_target_fails_when_the_marker_topic_is_present_but_errored() {
        let sp = spec_with_marker_topic("logweir.scratch");
        let al = allowed(&["ALLOWED0000000000000000"]);
        let r = StubReader {
            cluster_id: Ok("ALLOWED0000000000000000".into()),
            topics: Ok(vec![TopicMeta::errored(
                "logweir.scratch",
                "leader election in progress",
            )]),
        };
        match evaluate_target(&sp, &al, &r) {
            CheckResult::Failed(why) => {
                assert!(why.contains("logweir.scratch"), "got: {why}");
                assert!(why.contains("leader election in progress"), "got: {why}");
            }
            CheckResult::Passed(d) => panic!(
                "an errored marker topic must never report Passed (the exact defect Task 14 \
                 fixed in phase 0): got Passed({d})"
            ),
            CheckResult::Skipped(w) => panic!("expected Failed, got Skipped({w})"),
        }
    }

    #[test]
    fn evaluate_target_fails_when_the_marker_topic_is_absent() {
        let sp = spec_with_marker_topic("logweir.scratch");
        let al = allowed(&["ALLOWED0000000000000000"]);
        let r = StubReader {
            cluster_id: Ok("ALLOWED0000000000000000".into()),
            topics: Ok(vec![]),
        };
        match evaluate_target(&sp, &al, &r) {
            CheckResult::Failed(why) => {
                assert!(why.contains("logweir.scratch"), "got: {why}");
                assert!(why.contains("does not exist"), "got: {why}");
            }
            other => panic!(
                "expected Failed, got: {}",
                match other {
                    CheckResult::Passed(d) => d,
                    CheckResult::Skipped(w) => w,
                    CheckResult::Failed(_) => unreachable!(),
                }
            ),
        }
    }

    #[test]
    fn evaluate_target_fails_when_the_cluster_is_not_in_the_allow_list() {
        let sp = spec_with_marker_topic("logweir.scratch");
        let al = allowed(&["SOMETHING_ELSE0000000000"]);
        let r = StubReader {
            cluster_id: Ok("ALLOWED0000000000000000".into()),
            topics: Ok(vec![TopicMeta::new("logweir.scratch", 1)]),
        };
        match evaluate_target(&sp, &al, &r) {
            CheckResult::Failed(why) => assert!(why.contains("not in allowedClusterIds"), "{why}"),
            other => panic!(
                "expected Failed, got: {}",
                match other {
                    CheckResult::Passed(d) => d,
                    CheckResult::Skipped(w) => w,
                    CheckResult::Failed(_) => unreachable!(),
                }
            ),
        }
    }
}
