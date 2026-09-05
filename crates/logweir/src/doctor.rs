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
    if !topics.iter().any(|t| t.name == sp.target.marker_topic) {
        return CheckResult::Failed(format!(
            "target marker topic `{}` does not exist. Create it on the SCRATCH \
             cluster only — its existence is the v0.1 segregation proof.",
            sp.target.marker_topic
        ));
    }
    CheckResult::Passed(format!("cluster {id}, marker topic present"))
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
