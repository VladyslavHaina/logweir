use std::ffi::OsString;
use std::path::Path;
use std::process::Command;

/// `--approval`, `--approver-key`, `--allowed-clusters` and `--signing-key` for
/// a run over `spec`, with an approval and detached sidecar minted for those
/// exact bytes and written into `dir`.
///
/// Startup verifies the whole approval bundle before phase 0 runs, so a row
/// that means to be REFUSED by a phase-0 guard has to be genuinely approved
/// first. With the shipped example approval, which has no sidecar beside it and
/// was not minted for these modified specs, every row here would measure that
/// exit-1 operational failure instead of the guard. Minted the same way as
/// `cli_exit_codes.rs`'s `drill_run_capturing_stdout`.
fn approved_bundle_args(dir: &Path, spec: &str) -> Vec<OsString> {
    let key = logweir_evidence::keys::SigningKey::generate_p256();
    let approver_key = dir.join("approver.pub.pem");
    let signing_key = dir.join("signing.pem");
    std::fs::write(
        &approver_key,
        key.verifying_key().to_public_key_pem().unwrap(),
    )
    .unwrap();
    std::fs::write(&signing_key, key.to_pkcs8_pem().unwrap()).unwrap();
    let approval = dir.join("approval.json");
    let approval_doc = logweir_core::spec::ApprovalDoc {
        approver: "guard-cli-test@example.com".into(),
        ticket: "GUARD-CLI-PHASE0".into(),
        plan_hash: logweir_core::ids::sha256_prefixed(spec.as_bytes()),
        approved_at: chrono::Utc::now(),
        subject_kind: logweir_core::spec::SUBJECT_KIND_RESTORE.into(),
    };
    let approval_bytes = serde_json::to_vec(&approval_doc).unwrap();
    std::fs::write(&approval, &approval_bytes).unwrap();
    let sidecar = logweir_evidence::sign::sign_detached(
        &key,
        logweir::drill::phase1_approval::PAYLOAD_TYPE_APPROVAL,
        &approval_bytes,
    )
    .unwrap();
    std::fs::write(
        approval.with_extension("sig"),
        serde_json::to_vec(&sidecar).unwrap(),
    )
    .unwrap();
    vec![
        "--approval".into(),
        approval.into(),
        "--approver-key".into(),
        approver_key.into(),
        "--allowed-clusters".into(),
        "../../examples/allowed-clusters.json".into(),
        "--signing-key".into(),
        signing_key.into(),
    ]
}

fn run_with_spec(spec: &str) -> std::process::Output {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("drill.yaml");
    std::fs::write(&p, spec).unwrap();
    Command::new(env!("CARGO_BIN_EXE_logweir"))
        .args(["drill", "run", "--spec"])
        .arg(&p)
        .args(approved_bundle_args(dir.path(), spec))
        .output()
        .unwrap()
}

#[test]
fn purge_topics_in_the_spec_exits_3_at_either_value() {
    for v in ["true", "false"] {
        let spec = std::fs::read_to_string("../../examples/drill.yaml").unwrap()
            + &format!("\nengine_overrides:\n  purge_topics: {v}\n");
        let out = run_with_spec(&spec);
        assert_eq!(out.status.code(), Some(3), "purge_topics: {v}");
        let e = String::from_utf8(out.stderr).unwrap();
        assert!(
            e.contains("purge_topics"),
            "the offending path must be printed: {e}"
        );
    }
}

/// This asserts on the MESSAGE, not the code alone. `phase0_admit::run` performs
/// the two purely-local checks first (step 5 reorders it so), but this test runs
/// with no broker up, so a bare `assert_eq!(code, 3)` would pass even if
/// `check_topic_mapping_coverage` were deleted — a false green on the guard the
/// task exists to prove.
#[test]
fn an_unmapped_selected_topic_exits_3_on_the_mapping_check_not_on_a_dead_broker() {
    let spec = std::fs::read_to_string("../../examples/drill.yaml")
        .unwrap()
        .replace(
            "topic_mapping_prefix: \"drill-\"",
            "topic_mapping_prefix: \"\"",
        );
    let out = run_with_spec(&spec);
    let e = String::from_utf8(out.stderr).unwrap();
    assert_eq!(out.status.code(), Some(3), "{e}");
    assert!(
        e.contains("topic_mapping entry") || e.contains("onto itself"),
        "must fail on the mapping check, not on an unreachable broker: {e}"
    );
}

// ------------------------------------------------- Task 2 review carry F2
// **G-GLOB and G-EXP at phase 0.**
//
// Both properties were already enforced by `render_restore::render`. That is
// not where a plan gets REFUSED. A wildcard such as `orders*` in a spec was
// admitted at phase 0 and refused only in `preflight` — at phase 5, as
// `EngineError::Operational`, which is exit **1** with no `refusal-reason=`
// line — where Global Constraint 11 reserves exit 3 for "refused by a guard,
// before anything runs" and requires the reason line on every guard refusal.
//
// Ruling R-E (`logweir-engine-oso/src/engine.rs:243-250`) is not reopened: a
// renderer refusal REACHED AT PHASE 5 is exit 1, because by then phases 0-5
// have run. The defect was the missing phase-0 arm, and these two tests are
// what stop it being deleted again.

/// `(exit_code, stdout_text, stderr_text)`, with the code read from
/// `Command::status()` and both streams captured into real FILES — no pipe
/// carries any of the three (STANDING RULE 20).
fn run_with_spec_capturing_streams(spec: &str) -> (Option<i32>, String, String) {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("drill.yaml");
    std::fs::write(&p, spec).unwrap();
    let out_path = dir.path().join("stdout.txt");
    let err_path = dir.path().join("stderr.txt");
    let out_file = std::fs::File::create(&out_path).unwrap();
    let err_file = std::fs::File::create(&err_path).unwrap();
    let status = Command::new(env!("CARGO_BIN_EXE_logweir"))
        .args(["drill", "run", "--spec"])
        .arg(&p)
        .args(approved_bundle_args(dir.path(), spec))
        .env_remove("LOGWEIR_SOURCE_PASSWORD")
        .env_remove("LOGWEIR_TARGET_PASSWORD")
        .stdout(std::process::Stdio::from(out_file))
        .stderr(std::process::Stdio::from(err_file))
        .status()
        .unwrap();
    (
        status.code(),
        std::fs::read_to_string(&out_path).unwrap(),
        std::fs::read_to_string(&err_path).unwrap(),
    )
}

/// The last non-empty stdout line — what a controller tailing `pods/log`
/// reads.
fn last_line(stdout: &str) -> &str {
    stdout
        .lines()
        .filter(|l| !l.trim().is_empty())
        .next_back()
        .unwrap_or("")
}

fn spec_with_topics(topics: &str) -> String {
    std::fs::read_to_string("../../examples/drill.yaml")
        .unwrap()
        .replace("topics: [orders, payments]", topics)
}

/// **G-GLOB at phase 0.** A topic carrying a glob metacharacter is refused
/// with no broker, no bucket and no engine — the fact is knowable from the
/// spec text alone, which is exactly what exit 3 means.
///
/// Upstream's `TopicSelection.include` "supports glob patterns"
/// [U/kafka-backup/crates/kafka-backup-core/src/config.rs:334-343], so one
/// named entry silently widens to a set and the drill restores topics the
/// approved plan never named. All six metacharacters, closing halves included.
#[test]
fn a_globbed_topic_in_the_spec_exits_3_at_phase_0_with_its_reason_line() {
    for bad in ["orders*", "orders?", "events[1]", "a]b"] {
        let (code, stdout, stderr) =
            run_with_spec_capturing_streams(&spec_with_topics(&format!("topics: [\"{bad}\"]")));
        assert_eq!(
            code,
            Some(3),
            "`{bad}` must be refused at phase 0 (exit 3), not carried to phase 5's exit 1: \
             stdout:\n{stdout}\nstderr:\n{stderr}"
        );
        assert_eq!(
            last_line(&stdout),
            "refusal-reason=GuardRefused",
            "`{bad}`: a guard refusal owes its reason line:\n{stdout}"
        );
        // The REASON, not only the code: with no broker up, a bare
        // `assert_eq!(code, 3)` would also pass if this arm were deleted and
        // some later guard happened to refuse. The message must name the glob.
        assert!(
            stderr.contains("glob metacharacter") && stderr.contains(bad),
            "`{bad}`: the refusal must name the glob and the entry:\n{stderr}"
        );
    }
    // The mapped-target side too: `target.topic_mapping_prefix` is the half an
    // operator is more likely to template, and it reaches an include-style
    // position through `restore.topic_mapping`.
    let spec = std::fs::read_to_string("../../examples/drill.yaml")
        .unwrap()
        .replace(
            "topic_mapping_prefix: \"drill-\"",
            "topic_mapping_prefix: \"drill-*\"",
        );
    let (code, stdout, stderr) = run_with_spec_capturing_streams(&spec);
    assert_eq!(code, Some(3), "stdout:\n{stdout}\nstderr:\n{stderr}");
    assert_eq!(
        last_line(&stdout),
        "refusal-reason=GuardRefused",
        "{stdout}"
    );
    assert!(
        stderr.contains("glob metacharacter") && stderr.contains("mapped target"),
        "the prefix side must be named as the MAPPED TARGET, so the operator knows \
         which of the two to fix:\n{stderr}"
    );
}

/// **G-EXP at phase 0.** A `${` in a spec topic is refused before anything
/// runs, and refused as an EXPANSION rather than as a glob — `${` contains two
/// glob metacharacters, so the order of the two arms decides which fix the
/// operator attempts.
///
/// The hazard needs no attacker: the engine expands `${NAME}` over the whole
/// config as raw text before parsing and replaces an UNSET name with the empty
/// string behind a warning
/// [U:crates/kafka-backup-cli/src/commands/config.rs:1-34], so `orders${X}`
/// becomes `orders` and the drill restores a different topic than the plan
/// named — while `plan_hash` still matches, because the hashed bytes are a
/// template and the executed bytes are its expansion.
#[test]
fn a_dollar_brace_in_a_spec_topic_exits_3_at_phase_0_naming_the_expansion() {
    let (code, stdout, stderr) =
        run_with_spec_capturing_streams(&spec_with_topics("topics: [\"orders${X}\"]"));
    assert_eq!(
        code,
        Some(3),
        "a `${{` in a spec topic must be refused before anything runs: stdout:\n{stdout}\n\
         stderr:\n{stderr}"
    );
    assert_eq!(
        last_line(&stdout),
        "refusal-reason=GuardRefused",
        "{stdout}"
    );
    // THE REASON IS LOAD-BEARING HERE, and this assertion is what makes the
    // arm's own mutant killable. `orders${X}` also trips the G-GLOB arm two
    // lines below it — `{` and `}` are glob metacharacters — so deleting the
    // G-EXP arm still yields exit 3 and still prints
    // `refusal-reason=GuardRefused`. Only the message distinguishes them, and
    // the message is what the operator acts on: escaping a brace does not
    // stop an expansion.
    assert!(
        stderr.contains("expands textually BEFORE the"),
        "the refusal must name the EXPANSION, not the glob:\n{stderr}"
    );
    assert!(
        !stderr.contains("glob metacharacter"),
        "G-EXP must be checked before G-GLOB, or the operator is sent to escape a brace:\n\
         {stderr}"
    );
    assert!(stderr.contains("orders${X}"), "{stderr}");
}
