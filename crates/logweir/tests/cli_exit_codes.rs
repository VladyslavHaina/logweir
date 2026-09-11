//! Exit-code contract tests beyond `cli_verify.rs`'s four brief-mandated
//! cases. Global Constraint 11 reserves exit code 2 for "a drill result that
//! is not a pass — a scorecard IS written and signed", and `2` is exactly
//! the code clap's `Parser::parse()` hardcodes for EVERY usage error
//! (`Error::exit()` in `clap_builder`). Left unhandled, a typo'd flag or a
//! flag a later chart bump drops would report "the drill ran and did not
//! pass — go fetch the signed scorecard" when no drill ran and nothing was
//! written. `main.rs` now uses `try_parse` and maps any stderr-routed clap
//! error to `ExitCode::Operational` (1) instead, while `--help`/`--version`
//! (routed to stdout) keep exiting 0. These assert the real process exit
//! status for each shape of usage error, plus the two Global-Constraint-13
//! branches (`snapshot`/`diff` vs `plan`) that only `plan` had process-level
//! coverage for before.

use std::process::Command;

fn bin() -> Command {
    Command::new(env!("CARGO_BIN_EXE_logweir"))
}

#[test]
fn no_arguments_exits_operational_not_a_drill_result() {
    let out = bin().output().unwrap();
    assert_eq!(
        out.status.code(),
        Some(1),
        "a bare invocation is a usage error, not a drill result: stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn drill_verify_with_no_arguments_exits_operational_not_a_drill_result() {
    // The exact case the finding names: missing required flags on a real
    // subcommand must not be reported as exit code 2.
    let out = bin().args(["drill", "verify"]).output().unwrap();
    assert_eq!(
        out.status.code(),
        Some(1),
        "missing required flags is a usage error, not a drill result: stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn an_unknown_flag_exits_operational() {
    let out = bin().args(["drill", "verify", "--nope"]).output().unwrap();
    assert_eq!(
        out.status.code(),
        Some(1),
        "an unrecognised flag is a usage error, not a drill result: stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn an_unknown_subcommand_exits_operational() {
    let out = bin().args(["frobnicate"]).output().unwrap();
    assert_eq!(
        out.status.code(),
        Some(1),
        "an unrecognised subcommand is a usage error, not a drill result: stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn help_still_exits_ok() {
    let out = bin().args(["--help"]).output().unwrap();
    assert_eq!(out.status.code(), Some(0));
    assert!(!out.stdout.is_empty());
}

#[test]
fn version_still_exits_ok() {
    let out = bin().args(["--version"]).output().unwrap();
    assert_eq!(out.status.code(), Some(0));
    assert!(!out.stdout.is_empty());
}

#[test]
fn schema_snapshot_exits_1_naming_the_sub_project() {
    // Global Constraint 13 names `snapshot` and `diff` explicitly, sharing a
    // branch distinct from `plan`'s; only `plan` had process-level coverage.
    let out = bin().args(["schema", "snapshot"]).output().unwrap();
    assert_eq!(out.status.code(), Some(1));
    let err = String::from_utf8(out.stderr).unwrap();
    assert!(
        err.contains("SP2"),
        "must name the sub-project that introduces it, got: {err}"
    );
}

#[test]
fn drill_verify_exits_signing_or_lock_on_a_payload_type_mismatch() {
    // Controller ruling on the deferred finding: a payload_type mismatch is
    // evidence of SUBSTITUTION (a genuinely-signed sidecar for a different
    // kind of document, handed over in place of a scorecard), not
    // corruption — so it must map to exit 4, the same code a bad signature
    // does, not exit 1.
    let dir = tempfile::tempdir().unwrap();
    let sc = dir.path().join("s.json");
    let sig = dir.path().join("s.sig");
    let pubk = dir.path().join("pub.pem");
    std::fs::copy("../../e2e/fixtures/signed/scorecard.json", &sc).unwrap();
    std::fs::copy("../../e2e/fixtures/signed/public.pem", &pubk).unwrap();

    let good_sig = std::fs::read_to_string("../../e2e/fixtures/signed/scorecard.sig").unwrap();
    let mut sidecar: serde_json::Value = serde_json::from_str(&good_sig).unwrap();
    sidecar["payloadType"] = serde_json::Value::String(
        "application/vnd.logweir.drill-teardown+json;version=1.0.0".to_string(),
    );
    std::fs::write(&sig, serde_json::to_vec(&sidecar).unwrap()).unwrap();

    let out = bin()
        .args(["drill", "verify", "--scorecard"])
        .arg(&sc)
        .arg("--signature")
        .arg(&sig)
        .arg("--public-key")
        .arg(&pubk)
        .output()
        .unwrap();
    assert_eq!(
        out.status.code(),
        Some(4),
        "a payload_type mismatch must exit 4 (substitution), not 1: stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

// ------------------------------------------------------------------- [I9]
// The refusal-reason contract, at the process level.
//
// Global Constraint 11: every guard refusal prints
// `refusal-reason=<TerminalState>` as its **final stdout line**. Stdout and
// not stderr because the pod log API has no stream selector — it returns the
// container's two streams interleaved with no marker saying which byte came
// from which, so nothing a runner writes on stderr is distinguishable by a
// controller (spec §7 amendment 4). Last because a controller tailing the log
// reads the final line.
//
// Both arms read the exit code DIRECTLY from `Command::status()` (STANDING
// RULE 20 / GC11: never through a pipe) and capture stdout by handing the
// child a real FILE, so there is no pipe anywhere in either assertion.

/// Runs `logweir drill run` over `spec_text` with the shipped example
/// approval, key and allowed-clusters files, and returns
/// `(exit_code, stdout_text)`.
///
/// `env_remove` on the two password variables is not decoration: a developer
/// with `LOGWEIR_SOURCE_PASSWORD` exported would otherwise see the credential
/// guard fire in the spec arm, and the failure would look like a bug in the
/// spec arm.
fn drill_run_capturing_stdout(spec_text: &str, env: &[(&str, &str)]) -> (Option<i32>, String) {
    let dir = tempfile::tempdir().unwrap();
    let spec = dir.path().join("drill.yaml");
    std::fs::write(&spec, spec_text).unwrap();
    let out_path = dir.path().join("stdout.txt");
    let out_file = std::fs::File::create(&out_path).unwrap();

    let mut cmd = bin();
    cmd.args(["drill", "run", "--spec"])
        .arg(&spec)
        .args([
            "--approval",
            "../../examples/approval.json",
            "--approver-key",
            "../../e2e/fixtures/signed/public.pem",
            "--allowed-clusters",
            "../../examples/allowed-clusters.json",
            "--signing-key",
            "../../e2e/fixtures/signed/signing.pem",
        ])
        .env_remove("LOGWEIR_SOURCE_PASSWORD")
        .env_remove("LOGWEIR_TARGET_PASSWORD")
        .stdout(std::process::Stdio::from(out_file));
    for (k, v) in env {
        cmd.env(k, v);
    }
    // The exit code, read from `status()` itself.
    let status = cmd.status().unwrap();
    let stdout = std::fs::read_to_string(&out_path).unwrap();
    (status.code(), stdout)
}

/// The last non-empty line of a captured stdout, which is what a controller
/// tailing `pods/log` reads.
fn final_stdout_line(stdout: &str) -> &str {
    stdout
        .lines()
        .filter(|l| !l.trim().is_empty())
        .next_back()
        .unwrap_or("")
}

/// [I9] A plain guard refusal — a forbidden key in the spec, refused by
/// `phase0_admit::run` before anything runs — exits 3 and says so on stdout.
///
/// `dry_run: true` is the payload because it is the refusal whose ABSENCE is
/// most expensive: an ordinary YAML `dry_run: true` reaching phase 6 makes the
/// restore a no-op that exits 0, and the drill then signs a scorecard whose
/// RTO was measured around nothing (Global Constraint 4).
#[test]
fn a_guard_refusal_prints_its_terminal_state_as_the_final_stdout_line() {
    let spec = std::fs::read_to_string("../../examples/drill.yaml").unwrap()
        + "\nengine_overrides:\n  dry_run: true\n";
    let (code, stdout) = drill_run_capturing_stdout(&spec, &[]);
    assert_eq!(
        code,
        Some(3),
        "a forbidden key in the spec is a guard refusal (exit 3): stdout:\n{stdout}"
    );
    assert_eq!(
        final_stdout_line(&stdout),
        "refusal-reason=GuardRefused",
        "the reason must be the FINAL stdout line — stderr is not distinguishable \
         through the pod log API:\n{stdout}"
    );
    // Exactly one such line, and it is not repeated on any other exit path.
    assert_eq!(
        stdout
            .lines()
            .filter(|l| l.starts_with("refusal-reason="))
            .count(),
        1,
        "{stdout}"
    );
}

/// [I9]/[I11] A projected password that cannot be substituted into the
/// engine's pre-parse config text is a GUARD REFUSAL — exit 3 — and names the
/// terminal state `CredentialNotRenderable`.
///
/// The value carries a newline, which ends the physical line the placeholder
/// sits on and turns whatever follows into a new YAML key at whatever
/// indentation it has. The refusal happens on the RUNNER, at the moment it
/// reads the variable, before `context` builds any client: `weirkeeper` holds
/// no `get` on Secrets anywhere (spec §9), so the controller never sees the
/// projected value and cannot be the one to refuse it (spec §7 amendment 4).
///
/// The spec here is the CLEAN shipped example, deliberately: it would be
/// admitted, so an exit 3 can only have come from the credential read. That is
/// also what makes the "map the credential refusal to exit 1" mutant
/// attributable — with the mapping changed, this arm fails on the code while
/// the spec arm above still passes.
#[test]
fn a_credential_refusal_is_a_guard_refusal() {
    let spec = std::fs::read_to_string("../../examples/drill.yaml").unwrap();
    let (code, stdout) = drill_run_capturing_stdout(
        &spec,
        &[("LOGWEIR_SOURCE_PASSWORD", "hunter2\n bootstrap_servers:")],
    );
    assert_eq!(
        code,
        Some(3),
        "an unrenderable projected credential is refused before anything runs \
         (exit 3, not 1): stdout:\n{stdout}"
    );
    assert_eq!(
        final_stdout_line(&stdout),
        "refusal-reason=CredentialNotRenderable",
        "the controller's only machine-readable channel is the final stdout line:\n{stdout}"
    );
    // THE VALUE NEVER APPEARS. Not in the reason line, not in the structured
    // logs the same stream carries.
    assert!(
        !stdout.contains("hunter2"),
        "no fragment of the projected credential may reach any stream:\n{stdout}"
    );
    assert!(
        !stdout.contains("bootstrap_servers:"),
        "no fragment of the projected credential may reach any stream:\n{stdout}"
    );

    // The TARGET variable is checked too, and to the same state.
    let (code, stdout) = drill_run_capturing_stdout(
        &spec,
        &[("LOGWEIR_TARGET_PASSWORD", "hunter2\"and-a-quote")],
    );
    assert_eq!(code, Some(3), "stdout:\n{stdout}");
    assert_eq!(
        final_stdout_line(&stdout),
        "refusal-reason=CredentialNotRenderable",
        "{stdout}"
    );

    // And a RENDERABLE password is NOT refused by this guard. Proved by
    // pairing it with a spec that phase 0 refuses for its own reason: the run
    // still exits 3, but the state is `GuardRefused` and not
    // `CredentialNotRenderable`. A guard that refused every password would
    // pass the two assertions above while being useless, and this is the
    // discrimination that catches it.
    //
    // It is deliberately NOT proved by letting a renderable password run on
    // into the drill: that path reaches `context`, dials the target cluster
    // and spends `rdkafka_reader.rs:16`'s 20 s `const T` waiting for a broker
    // that is not there — measured at 20.8 s, over Global Constraint 22's 15 s
    // per-test bound, in a suite whose whole premise is that it dials nothing.
    let refused_spec = format!("{spec}\nengine_overrides:\n  dry_run: true\n");
    let (code, stdout) = drill_run_capturing_stdout(
        &refused_spec,
        &[("LOGWEIR_SOURCE_PASSWORD", "hunter2-Aa1!")],
    );
    assert_eq!(code, Some(3), "stdout:\n{stdout}");
    assert_eq!(
        final_stdout_line(&stdout),
        "refusal-reason=GuardRefused",
        "a renderable password must not colour another guard's refusal as a \
         credential problem:\n{stdout}"
    );
}

/// **Task 6.** The two failure modes of a SCRAM spec's password are two
/// different exit codes, and the difference is what a reconciler acts on.
///
/// * The variable is **UNSET** while `auth.mode` is `scramSha512` → **exit 1**,
///   operational, and **no** `refusal-reason=` line. Nothing was refused: the
///   plan is probably fine and the fix is to project the Secret, which is
///   exactly what "retry" means to Task 18's cron reconciler. A guard refusal
///   here would tell it to stop retrying a condition an operator is about to
///   fix.
/// * The variable holds a value that **cannot be substituted** into the
///   engine's pre-parse config text → **exit 3**, with
///   `refusal-reason=CredentialNotRenderable`. No projection of that value
///   will ever work.
///
/// Both codes are read directly from `Command::status()` (STANDING RULE 20),
/// and both refuse before a client is constructed — `AuthConfig::from_spec` is
/// reached in `drill::context` before `RdKafkaReader::connect`, so neither arm
/// waits on `rdkafka_reader.rs`'s 20 s `const T` and neither reaches Global
/// Constraint 22's 15 s per-test bound.
#[test]
fn a_missing_password_is_operational_and_a_broken_one_is_a_guard_refusal() {
    // The shipped example with a SCRAM target bolted on. It is otherwise the
    // CLEAN example — it would be admitted — so the exit code can only have
    // come from the credential path.
    // The auth block is spliced in ahead of `marker_topic` — NOT by rewriting
    // the `bootstrap_servers` line. Naming a loopback endpoint here, even
    // inside a `replace` pattern, trips
    // `tests/no_network_in_unit_tests.rs`'s source grep, which is a plain
    // `contains` over the file by design: an entry in its `ALLOWED` is meant
    // to be argued for, and this file has no need of one.
    const MARKER: &str = "  marker_topic: logweir.scratch\n";
    let example = std::fs::read_to_string("../../examples/drill.yaml").unwrap();
    assert!(example.contains(MARKER), "the shipped example moved");
    let spec = example.replace(
        MARKER,
        &format!("  auth:\n    mode: scramSha512\n    username: logweir\n{MARKER}"),
    );
    assert!(
        spec.contains("mode: scramSha512"),
        "the fixture must actually ask for SCRAM, or this test asserts nothing"
    );

    // ---- UNSET: exit 1, and no refusal-reason line at all ----
    // `drill_run_capturing_stdout` removes both variables itself, so "unset"
    // here is unset regardless of the developer's own environment.
    let (code, stdout) = drill_run_capturing_stdout(&spec, &[]);
    assert_eq!(
        code,
        Some(1),
        "an absent Secret is operational, not a refused plan: stdout:\n{stdout}"
    );
    assert!(
        !stdout.contains("refusal-reason="),
        "exit 1 prints no refusal-reason line — the contract is exit-3-only:\n{stdout}"
    );

    // ---- UNRENDERABLE: exit 3, and the credential terminal state ----
    let (code, stdout) = drill_run_capturing_stdout(
        &spec,
        &[("LOGWEIR_TARGET_PASSWORD", "x\"\n bootstrap_servers:")],
    );
    assert_eq!(
        code,
        Some(3),
        "a value that cannot be substituted into pre-parse config text is refused before \
         anything runs: stdout:\n{stdout}"
    );
    assert_eq!(
        final_stdout_line(&stdout),
        "refusal-reason=CredentialNotRenderable",
        "{stdout}"
    );
    // And no fragment of it reaches any stream.
    for fragment in ["bootstrap_servers:", "x\""] {
        assert!(!stdout.contains(fragment), "{stdout}");
    }

    // ---- The SOURCE variable is not consulted by a drill ----
    // A drill dials the TARGET. Projecting the source variable instead leaves
    // the target's still unset, so this is exit 1 again — not a silent
    // plaintext downgrade, and not a success.
    let (code, stdout) =
        drill_run_capturing_stdout(&spec, &[("LOGWEIR_SOURCE_PASSWORD", "renderable-Aa1")]);
    assert_eq!(
        code,
        Some(1),
        "the drill reads $LOGWEIR_TARGET_PASSWORD; the source variable cannot stand in for \
         it: stdout:\n{stdout}"
    );
}

// ---------------------------------------------------------------------------
// Task 22 — the pinned-approver refusal's exit code, and the flag's shape
// ---------------------------------------------------------------------------

/// The five flags a `restore run` needs, over the shipped example plan.
fn restore_base() -> Vec<String> {
    [
        "restore",
        "run",
        "--spec",
        "../../examples/drill.yaml",
        "--approval",
        "../../examples/approval.json",
        "--approver-key",
        "../../e2e/fixtures/signed/public.pem",
        "--allowed-clusters",
        "../../examples/allowed-clusters.json",
        "--signing-key",
        "../../e2e/fixtures/signed/signing.pem",
    ]
    .iter()
    .map(|s| (*s).to_string())
    .collect()
}

/// The approver refusal is a GUARD refusal: exit **3**, never 1.
///
/// The distinction is the whole exit-code contract (Global Constraint 11,
/// `crates/logweir/src/exit.rs`): 1 says "Logweir could not do its job, retry",
/// 3 says "the plan is refused and nothing ran". A pinned-set miss is the
/// second: the approval is authentic, it is simply not one this run accepts.
/// Exit 1 would tell a CronJob to retry a plan that will never be admitted.
#[test]
fn approver_refusal_is_a_guard_refusal() {
    let mut args = restore_base();
    args.push("--approver-key-ids".to_string());
    args.push("sha256:not-this-one".to_string());
    let out = bin().args(&args).output().unwrap();
    let stderr = String::from_utf8_lossy(&out.stderr).to_string();
    assert_eq!(
        out.status.code(),
        Some(3),
        "a pinned-set miss is exit 3 (GuardRefused), never 1 (Operational): {stderr}"
    );
    assert!(
        stderr.contains("is not in the pinned set"),
        "and it is THIS refusal, not some other exit-3 guard: {stderr}"
    );
    assert!(
        String::from_utf8_lossy(&out.stdout)
            .trim_end()
            .ends_with("refusal-reason=GuardRefused"),
        "interface I9: the reason line, on stdout, last"
    );
}

/// The `subject_kind` refusal SHARES the exit-3 mapping, and this arm reads a
/// real process's code to prove the mapping is 3 and not 1.
///
/// # Why the mutation itself is not driven through the binary
///
/// `drill/mod.rs` runs phase 0 before phase 1, and phase 0 DIALS the target
/// cluster. A `restore run` over a plan good enough to reach phase 1 needs a
/// live broker, so a process-level assertion on the mutation would be green
/// only while somebody else's compose stack happened to be up — measured:
/// with the stack up it exits 3 on the signature, with it down it exits 1 at
/// phase 0 and never observes the refusal at all. Global Constraint 22 keeps
/// dialling tests out of the default suite, so this file asserts the two
/// halves that ARE deterministic:
///
/// 1. the mutation produces `DrillError::Guard(GuardRefusal(_))` — the same
///    variant, from the same module, asserted at the `phase1_approval::verify`
///    seam in `approval.rs::subject_kind_is_inside_the_signed_bytes`; and
/// 2. that variant exits **3** in a real process, read from the process
///    status directly, via the one phase-1 guard reachable with no broker —
///    the pinned-approver refusal this task lands.
#[test]
fn the_subject_kind_refusal_shares_the_exit_3_mapping() {
    use logweir::drill::{phase1_approval, DrillError};
    use logweir_core::guard::GuardRefusal;

    let dir = tempfile::tempdir().unwrap();
    let spec_text = std::fs::read_to_string("../../examples/drill.yaml").unwrap();
    let spec = dir.path().join("drill.yaml");
    std::fs::write(&spec, &spec_text).unwrap();
    let key = dir.path().join("approver.pem");
    let sk = logweir_evidence::keys::SigningKey::generate_p256();
    std::fs::write(&key, sk.to_pkcs8_pem().unwrap()).unwrap();
    let approver_pub = dir.path().join("approver.pub.pem");
    std::fs::write(
        &approver_pub,
        sk.verifying_key().to_public_key_pem().unwrap(),
    )
    .unwrap();
    let approval = dir.path().join("approval.json");

    let minted = bin()
        .args(["drill", "approve"])
        .arg("--spec")
        .arg(&spec)
        .arg("--key")
        .arg(&key)
        .args(["--approver", "sre-oncall@example.com"])
        .args(["--ticket", "CHG-40881"])
        .args(["--subject-kind", "Backup"])
        .arg("--out")
        .arg(&approval)
        .output()
        .unwrap();
    assert_eq!(minted.status.code(), Some(0));

    let doc = std::fs::read_to_string(&approval).unwrap();
    let mutated = doc.replace(
        r#""subject_kind": "Backup""#,
        r#""subject_kind": "Restore""#,
    );
    assert_ne!(doc, mutated, "the mutation must change the file");
    std::fs::write(&approval, mutated).unwrap();

    // 1 — the variant.
    let signing = logweir_evidence::keys::SigningKey::generate_p256().verifying_key();
    let err = phase1_approval::verify(&spec_text, &approval, &approver_pub, &signing).unwrap_err();
    assert!(
        matches!(err, DrillError::Guard(GuardRefusal(_))),
        "a mutated field inside the signed bytes is a guard refusal, got {err:?}"
    );

    // 2 — that variant's code, from a real process.
    let mut args = restore_base();
    args.push("--approver-key-ids".to_string());
    args.push("sha256:not-this-one".to_string());
    let out = bin().args(&args).output().unwrap();
    assert_eq!(
        out.status.code(),
        Some(3),
        "DrillError::Guard maps to 3, never to 1: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// **THE FLAG SHAPE, PINNED.** `--approver-key-ids` is REPEATABLE and is NOT
/// comma-separated: a comma-joined value is ONE id, and since no key id
/// contains a comma it matches nothing and refuses, naming the whole string.
///
/// This is a decision, not an accident. The operator emits one flag per id
/// (`weirkeeper::controllers::restore::runner_argv`, asserted by
/// `restore_controller.rs::only_unexpired_roster_key_ids_reach_the_argv`:
/// "repeated flags, never one comma-joined value"). Accepting BOTH shapes
/// would mean two spellings of the same set, one of which nothing in this
/// workspace produces — and a `value_delimiter` would silently reinterpret an
/// id that somehow contained a comma instead of refusing it.
#[test]
fn a_comma_joined_approver_key_id_is_one_id_not_two() {
    let mut args = restore_base();
    args.push("--approver-key-ids".to_string());
    args.push("sha256:aaaa,sha256:bbbb".to_string());
    let out = bin().args(&args).output().unwrap();
    let stderr = String::from_utf8_lossy(&out.stderr).to_string();
    assert_eq!(out.status.code(), Some(3), "{stderr}");
    assert!(
        stderr.contains("the pinned set {sha256:aaaa,sha256:bbbb}"),
        "the comma-joined value is ONE member of the set, printed whole: {stderr}"
    );
}
