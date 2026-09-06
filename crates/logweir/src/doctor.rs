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
use crate::engine_bin::engine_path;
use crate::exit::ExitCode;
use logweir_kafka::reader::{AuthConfig, ClusterReader};
use std::path::PathBuf;

pub struct DoctorArgs {
    pub spec: PathBuf,
    pub allowed_clusters: PathBuf,
    pub approver_key: PathBuf,
    /// Fix round 2, M5: a plain `0` exit does not distinguish "all seven
    /// checks verified" from "storage skipped, the rest verified" — both are
    /// silently the same to a script keying on exit code alone. GC11 leaves
    /// no third exit code to add, so this is the non-breaking way: an
    /// opt-in flag that maps `skipped > 0` onto `ExitCode::Operational`
    /// instead of leaving it at `Ok`. Default `false` keeps today's
    /// behaviour (a skip never fails a run that had no way to look).
    pub strict: bool,
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
        if args.strict {
            eprintln!(
                "--strict: {skipped} check(s) were never actually looked at, which counts as \
                 not verified."
            );
        }
    } else {
        println!(
            "\nAll checks passed. `logweir drill run` should work against this configuration."
        );
    }
    skip_verdict(skipped, args.strict)
}

/// The `--strict`/skip-count decision (fix round 2, M5), factored out as a
/// pure function so it is unit-testable on its own: reaching the "all seven
/// genuinely passed vs. one skipped" branch of `run()` end-to-end needs a
/// live, reachable target (`just e2e-up`, Task 7b), which the default
/// `cargo test` run does not have (GR2) — this needs neither.
fn skip_verdict(skipped: usize, strict: bool) -> ExitCode {
    if skipped > 0 && strict {
        ExitCode::Operational
    } else {
        ExitCode::Ok
    }
}

/// 1. Resolved by `crate::engine_bin::engine_path` — `$LOGWEIR_ENGINE_BIN`,
///    else `.engine/kafka-backup`, else `/usr/local/bin/kafka-backup`, else a
///    `$PATH` scan — which is the SAME chain `drill run` walks, so a green line
///    here names the binary the drill will actually execute. On failure name
///    the pinned digest and the glibc floor, because the standalone binary
///    carries no engine and that is the usual cause.
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
///
/// Fix round 2, M1: `out.status` used to be discarded entirely, so an engine
/// that could not run AT ALL (wrong architecture, missing interpreter, not
/// executable) fell through to the same "version mismatch" text as an engine
/// that ran fine and printed a different number. On darwin — the demo
/// platform — `Command::output()` for the pinned linux/amd64 binary does not
/// return an `Err` (unlike, say, a plain ENOENT for a missing file): the
/// process runs (traditional Unix execve-without-a-recognised-magic falls
/// back to `/bin/sh`), and `/bin/sh` itself prints "cannot execute binary
/// file" to stderr and exits non-zero (conventionally 126) — so the SECOND
/// line of the first command in the demo told a Mac user to go hunt a
/// version problem that did not exist. `out.status.success()` is now
/// consulted before anything else.
fn check_engine_version() -> Result<String, String> {
    let path = engine_path();
    let out = std::process::Command::new(&path)
        .arg("--version")
        .output()
        .map_err(|e| {
            format!(
                "the engine at {} could not be executed: {e}. This usually means the wrong \
                 architecture (the pinned kafka-backup is linux/amd64 only) or a missing \
                 interpreter/dynamic linker — not a version mismatch.",
                path.display()
            )
        })?;
    evaluate_engine_version(&path, &out)
}

/// Split out of `check_engine_version` (fix round 2) so the exec-failure /
/// version-mismatch distinction is directly unit-testable against a
/// hand-built `std::process::Output` — no subprocess, no platform
/// dependence, so the darwin-specific failure mode above is pinned on every
/// CI runner regardless of what engine (if any) is actually installed there.
fn evaluate_engine_version(
    path: &std::path::Path,
    out: &std::process::Output,
) -> Result<String, String> {
    let s =
        String::from_utf8_lossy(&out.stdout).to_string() + &String::from_utf8_lossy(&out.stderr);
    if !out.status.success() {
        return Err(format!(
            "the engine at {} exited {} without producing version output: `{}`. This is an \
             EXECUTION failure, not a version mismatch — on darwin/arm64 the pinned linux/amd64 \
             binary cannot run natively (see check 1's glibc >= 2.36 / libssl3 / CA-bundle note); \
             use the container image or a linux/amd64 host.",
            path.display(),
            out.status
                .code()
                .map(|c| c.to_string())
                .unwrap_or_else(|| "via a signal".into()),
            s.trim()
        ));
    }
    if version_matches(&s) {
        Ok(s.trim().to_string())
    } else {
        Err(format!(
            "version mismatch: expected 0.21.0, engine reports `{}`",
            s.trim()
        ))
    }
}

/// Fix round 2, M6: `s.contains("0.21.0")` accepted `0.21.01`, `10.21.0` and
/// `0.21.0-rc1` — Global Constraint 8 pins an EXACT version. Matches the pin
/// only when it is not immediately adjacent to another digit or `.` (rules
/// out `10.21.0` and `0.21.01`) and not immediately followed by `-` (rules
/// out a `0.21.0-rc1` prerelease suffix), i.e. as a whole version token.
fn version_matches(s: &str) -> bool {
    const PIN: &str = "0.21.0";
    let bytes = s.as_bytes();
    let mut start = 0;
    while let Some(rel) = s.get(start..).and_then(|s| s.find(PIN)) {
        let idx = start + rel;
        let before_ok = idx == 0 || {
            let c = bytes[idx - 1];
            !(c.is_ascii_digit() || c == b'.')
        };
        let after = idx + PIN.len();
        let after_ok = after >= bytes.len() || {
            let c = bytes[after];
            !(c.is_ascii_digit() || c == b'.' || c == b'-')
        };
        if before_ok && after_ok {
            return true;
        }
        start = idx + 1;
    }
    false
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
///    and bucket in a single call, AND that the archive holds something a
///    drill could restore from (fix round 2, M3). A storage configuration
///    that will not construct, or that constructs but is EMPTY, is a
///    FAILURE. A credentials/not-found error is also a FAILURE, distinct
///    from a genuinely unreachable endpoint (fix round 2, M4), which is the
///    only case that SKIPs — `cargo test` and CI run with no MinIO (`just
///    e2e-up`, Task 7b).
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
    let loc = storage_location(&sp.source.storage);
    let st = match logweir_engine_oso::storage::Store::read_only_from_url(&sp.source.storage) {
        Ok(st) => st,
        Err(e) => return CheckResult::Failed(format!("storage: {e}")),
    };
    let keys = st.list_manifest_keys(sp.source.storage.prefix());
    evaluate_storage(&loc, &sp.source.backup, keys)
}

/// The classification of a listing's RESULT, factored out of `check_storage`
/// so it can be exercised directly against a synthesised `Result` — no
/// bucket, no endpoint, no network — in `tests::` below. Task 5b.
///
/// PURE. It takes the result of a listing and never performs one. This is
/// the exact split `check_target`/`evaluate_target` already made a few
/// functions down, for the identical reason and stated in the identical
/// words: the interesting behaviour is a decision about an outcome, and a
/// decision about an outcome does not need the machine that produced it.
/// `check_storage` never got that treatment when `check_target` did, so the
/// one test that pins "unreachable must SKIP, never PASS" had to reach a
/// real endpoint to get an unreachable answer — and paid `object_store`
/// 0.14.1's unconfigured retry budget, ~12 s, on every `cargo test` run on
/// every machine with no MinIO. Which is every machine, by default.
///
/// Only the ERROR half is a judgement call, and it is delegated to
/// `classify_storage_error`; the two `Ok` arms are here because
/// reachable-and-empty vs reachable-with-backups is the same kind of
/// decision and belongs beside it.
fn evaluate_storage(
    loc: &str,
    backup: &str,
    keys: Result<Vec<String>, logweir_core::engine::EngineError>,
) -> CheckResult {
    match keys {
        // Fix round 2, M3: a reachable archive holding ZERO backup sets used
        // to print `ok`. `source.backup: latestCompleted` has nothing to
        // select from an empty archive and a drill against it fails in
        // phase 1 — this is the build's signature defect ("a check that
        // cannot fail") arriving in doctor: a green line in front of a
        // guaranteed-failing drill. Reachable-and-empty is now its own
        // FAILURE, distinct from reachable-with-backups (`Passed`) and from
        // unreachable (`Skipped`, below).
        Ok(keys) if keys.is_empty() => CheckResult::Failed(format!(
            "storage at {loc} is reachable but holds zero backup sets. `source.backup: {backup}` \
             has nothing to select, and a drill against this configuration would fail in phase 1 \
             — point --spec at an archive that has completed at least one backup."
        )),
        Ok(keys) => CheckResult::Passed(format!("{} backup set(s) under {loc}", keys.len())),
        Err(e) => classify_storage_error(loc, &e.to_string()),
    }
}

/// Fix round 2, M4: every `Err` from `list_manifest_keys` used to become an
/// unconditional `Skipped` naming `just e2e-up` as the remedy. An S3
/// `AccessDenied` (wrong credentials) or a not-found bucket/prefix (a typo)
/// landed there too, alongside a genuinely down MinIO — telling someone with
/// a credentials problem to go start a local compose stack wastes their
/// time, and contradicts this check's own doc comment ("proves credentials,
/// endpoint and bucket"). Classified against `object_store::Error`'s own
/// Display text [VERIFIED object_store-0.14.1/src/lib.rs:2233-2331:
/// `NotFound` renders "... not found: ...", `PermissionDenied` renders
/// "...necessary privileges...", `Unauthenticated` renders "...valid
/// authentication credentials..."] — `list_keys`
/// (`crates/logweir-engine-oso/src/storage.rs:289`) stringifies the
/// underlying `object_store::Error` directly via `.to_string()`, so these
/// exact phrases survive into the `EngineError` this function receives.
/// Anything that does not match one of those three (connection refused, DNS
/// failure, a timeout) is a genuine reachability gap and still SKIPs.
fn classify_storage_error(loc: &str, e: &str) -> CheckResult {
    let el = e.to_ascii_lowercase();
    if el.contains("necessary privileges") || el.contains("valid authentication credentials") {
        CheckResult::Failed(format!(
            "storage credentials at {loc} were rejected: {e}. This is a credentials/permissions \
             problem — `just e2e-up` will not fix it. Check the access key, secret and bucket \
             policy."
        ))
    } else if el.contains("not found") {
        CheckResult::Failed(format!(
            "storage location {loc} was not found: {e}. This looks like a configuration problem \
             (a typo'd bucket, container or prefix) — `just e2e-up` will not fix it."
        ))
    } else {
        CheckResult::Skipped(format!(
            "storage at {loc} not reachable ({e}); run `just e2e-up` to exercise this check"
        ))
    }
}

/// A human-readable location for `check_storage`'s messages. `StorageUrl`'s
/// own `prefix()` method returns `""` for `Filesystem` (it has no such
/// field), so interpolating "under {prefix}" alone (the round-1 shape) left
/// a message ending in a dangling "under " for that backend (fix round 2,
/// M3, second half) — this names the backend's own bucket/container/path,
/// appending the prefix only when there is one to show.
fn storage_location(u: &logweir_core::engine::StorageUrl) -> String {
    use logweir_core::engine::StorageUrl;
    match u {
        StorageUrl::S3 { bucket, prefix, .. } => with_prefix(format!("s3://{bucket}"), prefix),
        StorageUrl::Azure {
            account_name,
            container_name,
            prefix,
        } => with_prefix(format!("azure://{account_name}/{container_name}"), prefix),
        StorageUrl::Gcs { bucket, prefix } => with_prefix(format!("gcs://{bucket}"), prefix),
        StorageUrl::Filesystem { path } => path.display().to_string(),
    }
}

fn with_prefix(base: String, prefix: &str) -> String {
    if prefix.is_empty() {
        base
    } else {
        format!("{base}/{prefix}")
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

// The engine resolution that lived here — `engine_path`, `which_on_path`,
// `find_on_path`, `is_executable` — moved to `crate::engine_bin`, which
// `crate::drill` now calls too. They used to disagree: this module walked
// `$LOGWEIR_ENGINE_BIN` -> `/usr/local/bin/kafka-backup` -> `$PATH`, while
// `crate::drill`'s default was the literal `.engine/kafka-backup`, and
// `Command::new` never searches `$PATH` for a path containing a slash. So the
// install `README.md` documents — the engine on `$PATH` — produced a green
// `doctor` and a drill that died at phase 5.

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

    /// Test-only formatter for a `CheckResult` a test did NOT expect, so a
    /// failing `match`'s `panic!` names the payload it actually got instead
    /// of just "a different variant".
    fn describe(r: &CheckResult) -> String {
        match r {
            CheckResult::Passed(d) => format!("Passed({d})"),
            CheckResult::Skipped(w) => format!("Skipped({w})"),
            CheckResult::Failed(w) => format!("Failed({w})"),
        }
    }

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
    /// A well-formed S3 config this process cannot reach MUST be a `Skipped`,
    /// structurally distinct from `Passed`, never folded into a bare
    /// `Ok`/"ok" that a reader can't tell from a genuine pass.
    ///
    /// Task 5b, addendum ruling B1(b): this used to run `check_storage`
    /// against `examples/drill.yaml` and get its unreachable answer from the
    /// network. **It cost 6.7 s of every `cargo test` run** — and not, as
    /// everyone assumed, waiting on `localhost:9000`. MEASURED: with no AWS
    /// credentials in the environment, `AmazonS3Builder::from_env()` falls
    /// through to the EC2 instance-metadata credential provider and the
    /// retries are against the LINK-LOCAL address `169.254.169.254`, ten of
    /// them, before the endpoint is ever contacted. A unit test was reaching
    /// off the loopback interface (GC17) to prove a classification.
    ///
    /// The property is about `check_storage`'s CLASSIFICATION of an
    /// unreachable result, and classification is pure, so it is asserted
    /// against `evaluate_storage` with a synthesised `Err` — the identical
    /// discipline the three `classify_storage_error` tests below already use.
    /// The dialling form is NOT deleted: see
    /// `check_storage_skips_a_genuinely_unreachable_endpoint`, which runs the
    /// original body under `--features e2e`.
    ///
    /// The input strings are `object_store` 0.14.1's own Display text,
    /// CAPTURED VERBATIM from a real run of this check on this workstation
    /// with no stack up (the first) and from the same generic form with the
    /// configured endpoint in it (the second) — a synthesised error that does
    /// not look like the real one proves nothing about the real one.
    #[test]
    fn check_storage_reports_skipped_not_passed_when_it_cannot_reach_a_well_formed_target() {
        const UNREACHABLE: [&str; 2] = [
            // Captured from `logweir doctor --spec examples/drill.yaml` with
            // the compose stack down and no AWS credentials set.
            "operational: Generic S3 error: Error performing PUT \
             http://169.254.169.254/latest/api/token in 6.510238083s, after 10 retries, \
             max_retries: 10, retry_timeout: 180s  - HTTP error: error sending request",
            // The same shape once credentials resolve and the configured
            // endpoint itself is the thing that is not there.
            "operational: Generic S3 error: Error performing GET \
             http://localhost:9000/kafka-backups?list-type=2 in 1.204s, after 10 retries, \
             max_retries: 10, retry_timeout: 180s  - HTTP error: error sending request",
        ];
        for e in UNREACHABLE {
            let keys = Err(logweir_core::engine::EngineError::Operational(e.into()));
            match evaluate_storage("s3://kafka-backups/drill-demo", "latestCompleted", keys) {
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
    }

    /// The dialling half of the test above, RETAINED rather than deleted
    /// (Task 5b, addendum ruling B1(b)). Trading an end-to-end proof for a
    /// unit proof would quietly lose the guarantee that `object_store`'s real
    /// unreachable error still classifies as `Skipped` — the synthesised
    /// strings above are only as good as the day they were captured, and a
    /// dependency bump can reword them.
    ///
    /// It runs under `just e2e` and never in the default suite. That is where
    /// a genuinely dead endpoint is arrangeable and honest: the compose stack
    /// is up, so port 1 on loopback is dead because nothing listens there,
    /// not because the whole machine has no object storage.
    ///
    /// The `StorageUrl` is built from a YAML literal rather than from a new
    /// fixture file, the same way `spec_with_marker_topic` below builds a
    /// `DrillSpec` — the subject is one storage config, not a whole drill.
    #[cfg(feature = "e2e")]
    #[test]
    fn check_storage_skips_a_genuinely_unreachable_endpoint() {
        let u: logweir_core::engine::StorageUrl = serde_yaml::from_str(
            "backend: s3\nbucket: kafka-backups\nprefix: drill-demo\nregion: us-east-1\n\
             endpoint: http://127.0.0.1:1\npath_style: true\nallow_http: true\n",
        )
        .expect("the storage fixture must parse");
        let st = logweir_engine_oso::storage::Store::read_only_from_url(&u)
            .expect("a well-formed S3 config must CONSTRUCT even when nothing answers");
        match evaluate_storage(
            "s3://kafka-backups/drill-demo",
            "latestCompleted",
            st.list_manifest_keys(u.prefix()),
        ) {
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

    /// Task 5b: the DEFAULT-SUITE half of check 7's property. An unreachable
    /// target must produce a NAMED `target` failure, never a generic "doctor
    /// failed" — an adopter with a firewall problem who reads only the latter
    /// has been told nothing.
    ///
    /// `crates/logweir/tests/doctor.rs`'s
    /// `check_7_an_unreachable_target_is_named_as_a_target_problem` proves
    /// the same property through the compiled binary, and MEASURED 26.6 s to
    /// do it: 20 s of `rdkafka_reader.rs:16`'s `const T` (a refused connect
    /// does NOT shorten it — librdkafka retries the connection and the
    /// metadata REQUEST waits out `T`, so the fixture's unroutable
    /// `127.0.0.1:1` buys nothing) plus ~6.5 s of object_store retries on the
    /// storage check ahead of it. It is now `--features e2e`; this is what
    /// the default suite runs instead, in microseconds, against the stub
    /// reader the file already has.
    ///
    /// The two halves together are the whole property: this one pins the
    /// wording and the `Failed` verdict, and `run`'s check table — where
    /// `("target", ...)` is the name printed beside it — is what makes it
    /// reach the operator as `FAIL  target`. Shortening `T` to make the CLI
    /// form fast is NOT the alternative: that constant is open decision O18's.
    #[test]
    fn evaluate_target_fails_and_names_the_target_when_the_cluster_is_unreachable() {
        let sp = spec_with_marker_topic("logweir.scratch");
        let al = allowed(&["ALLOWED0000000000000000"]);
        let r = StubReader {
            cluster_id: Err(KafkaError::Unreachable(
                "Meta data fetch error: BrokerTransportFailure".into(),
            )),
            topics: Ok(vec![]),
        };
        match evaluate_target(&sp, &al, &r) {
            CheckResult::Failed(why) => {
                assert!(
                    why.contains("target"),
                    "the failure must NAME the target — an adopter behind a firewall reads this \
                     line and nothing else: {why}"
                );
                assert!(why.contains("unreachable"), "got: {why}");
            }
            CheckResult::Passed(d) => {
                panic!("a target that cannot be reached must never report Passed: got Passed({d})")
            }
            CheckResult::Skipped(w) => panic!(
                "check 7 is ALWAYS a failure, never a skip — that is its whole contract \
                 (`check_target`'s doc comment): got Skipped({w})"
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

    // ---- Fix round 2 -----------------------------------------------------

    /// Fix round 2, M5's regression test: `--strict` must turn a skip into a
    /// failure, and must never fail a run with zero skips even under
    /// `--strict`. Pure-function unit test — reaching this branch of
    /// `run()` for real needs a live, reachable target (`just e2e-up`), which
    /// the default `cargo test` run does not have (GR2).
    #[test]
    fn skip_verdict_is_ok_without_strict_regardless_of_skips() {
        assert_eq!(skip_verdict(0, false), ExitCode::Ok);
        assert_eq!(skip_verdict(3, false), ExitCode::Ok);
    }

    #[test]
    fn skip_verdict_is_operational_under_strict_only_when_something_was_skipped() {
        assert_eq!(skip_verdict(0, true), ExitCode::Ok);
        assert_eq!(skip_verdict(1, true), ExitCode::Operational);
    }

    #[test]
    fn version_matches_accepts_only_the_exact_pin() {
        assert!(version_matches("kafka-backup 0.21.0"));
        assert!(version_matches("kafka-backup 0.21.0\n"));
        assert!(version_matches("0.21.0"));
        assert!(!version_matches("kafka-backup 0.19.1"));
        assert!(
            !version_matches("kafka-backup 0.21.01"),
            "must reject a longer patch number"
        );
        assert!(
            !version_matches("kafka-backup 10.21.0"),
            "must reject a different major version"
        );
        assert!(
            !version_matches("kafka-backup 0.21.0-rc1"),
            "must reject a prerelease suffix"
        );
    }

    fn fake_output(exit_code: i32, stdout: &str, stderr: &str) -> std::process::Output {
        use std::os::unix::process::ExitStatusExt;
        std::process::Output {
            status: std::process::ExitStatus::from_raw(exit_code << 8),
            stdout: stdout.as_bytes().to_vec(),
            stderr: stderr.as_bytes().to_vec(),
        }
    }

    /// Fix round 2, M1's regression test: an engine that could not execute
    /// at all must never be reported as a version mismatch. Built from a
    /// hand-crafted `std::process::Output` (exit 126, the conventional shell
    /// "command found but not executable" code) rather than a real
    /// subprocess, so this is pinned on every platform regardless of what
    /// engine binary (if any) happens to be installed on the CI runner.
    #[test]
    fn evaluate_engine_version_names_an_exec_failure_distinctly_from_a_version_mismatch() {
        let out = fake_output(126, "", "kafka-backup: ...: cannot execute binary file\n");
        let e = evaluate_engine_version(Path::new("/fake/kafka-backup"), &out).unwrap_err();
        assert!(
            !e.contains("version mismatch: expected"),
            "must not claim a version mismatch for a binary that never ran: {e}"
        );
        assert!(
            e.contains("EXECUTION failure") || e.contains("architecture"),
            "must name the real problem: {e}"
        );
    }

    #[test]
    fn evaluate_engine_version_names_a_real_version_mismatch_when_the_engine_ran_successfully() {
        let out = fake_output(0, "kafka-backup 0.19.1\n", "");
        let e = evaluate_engine_version(Path::new("/fake/kafka-backup"), &out).unwrap_err();
        assert!(e.contains("version mismatch: expected 0.21.0"), "got: {e}");
    }

    #[test]
    fn evaluate_engine_version_passes_on_an_exact_pin_match() {
        let out = fake_output(0, "kafka-backup 0.21.0\n", "");
        assert!(evaluate_engine_version(Path::new("/fake/kafka-backup"), &out).is_ok());
    }

    // The three `find_on_path_*` tests that lived here moved to
    // `crate::engine_bin::tests` with the function itself, unrenamed, when
    // `doctor` and `drill run` were unified onto one resolution.

    /// Fix round 2, M3's regression test: a reachable archive holding zero
    /// backup sets must FAIL, never `Passed`. `e2e/fixtures/empty-archive/`
    /// is a real, checked-in, genuinely empty directory (tracked via its
    /// `.gitkeep`, which does not end in `/manifest.json` and so never
    /// counts as a backup set itself).
    #[test]
    fn check_storage_fails_on_a_reachable_but_empty_archive() {
        match check_storage(Path::new("../../e2e/fixtures/drill-empty-archive.yaml")) {
            CheckResult::Failed(why) => assert!(why.contains("zero backup sets"), "got: {why}"),
            other => panic!("expected Failed, got: {}", describe(&other)),
        }
    }

    /// Fix round 2, M4: credentials/permission and not-found errors must be
    /// named as FAILURES, distinct from a genuine reachability gap, which
    /// alone SKIPs. The input strings mirror `object_store::Error`'s own
    /// Display text verbatim (verified against
    /// object_store-0.14.1/src/lib.rs:2233-2331), which is exactly the text
    /// that reaches `classify_storage_error` in production, since
    /// `list_keys` stringifies that error directly.
    #[test]
    fn classify_storage_error_treats_permission_denied_as_failed_not_skipped() {
        let e = "operational: The operation lacked the necessary privileges to complete for \
                 path logweir/x: 403 Forbidden";
        match classify_storage_error("s3://bucket", e) {
            CheckResult::Failed(why) => assert!(why.contains("credentials"), "got: {why}"),
            other => panic!("expected Failed, got: {}", describe(&other)),
        }
    }

    #[test]
    fn classify_storage_error_treats_unauthenticated_as_failed_not_skipped() {
        let e = "operational: The operation lacked valid authentication credentials for path \
                 logweir/x: InvalidAccessKeyId";
        match classify_storage_error("s3://bucket", e) {
            CheckResult::Failed(why) => assert!(why.contains("credentials"), "got: {why}"),
            other => panic!("expected Failed, got: {}", describe(&other)),
        }
    }

    #[test]
    fn classify_storage_error_treats_not_found_as_failed_not_skipped() {
        let e = "operational: Object at location logweir/x not found: 404 Not Found";
        match classify_storage_error("s3://bucket", e) {
            CheckResult::Failed(why) => assert!(why.contains("configuration"), "got: {why}"),
            other => panic!("expected Failed, got: {}", describe(&other)),
        }
    }

    #[test]
    fn classify_storage_error_treats_a_generic_transport_error_as_skipped_not_failed() {
        let e = "operational: Generic S3 error: error sending request for url \
                 (http://127.0.0.1:1/): connection refused";
        match classify_storage_error("s3://bucket", e) {
            CheckResult::Skipped(_) => {}
            other => panic!("expected Skipped, got: {}", describe(&other)),
        }
    }

    /// Fix round 2, M3's second half: `Filesystem` has no `prefix()`, so
    /// naming it must not end in a dangling "under " with nothing after it.
    #[test]
    fn storage_location_never_ends_in_a_dangling_under_for_filesystem() {
        let u = logweir_core::engine::StorageUrl::Filesystem {
            path: std::path::PathBuf::from("/tmp/archive"),
        };
        let loc = storage_location(&u);
        assert!(!loc.ends_with("under "), "got: {loc}");
        assert_eq!(loc, "/tmp/archive");
    }
}
