//! **FX-34**, the RUNNER's half: a guard refusal prints its reason code and
//! its sentence as ONE `refusal-detail=` line, immediately before I9's
//! `refusal-reason=` line, at exit 3 and at no other exit.
//!
//! Every row runs the shipped `logweir` binary against a loopback sentinel
//! that must never be dialled, and reads the line back through
//! `logweir_core::refusal_detail::RefusalDetail::from_line`: the reader a
//! controller uses, so "the runner printed it" and "a controller accepts it"
//! are one assertion. The controller's own rows are
//! `crates/weirkeeper/tests/refusal_detail.rs`.

use std::ffi::OsString;
use std::io::ErrorKind;
use std::net::TcpListener;
use std::path::Path;
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

use logweir_core::refusal_detail::{
    clean_message, split_reason_code, RefusalDetail, REASON_MESSAGE_MAX_BYTES,
    REFUSAL_DETAIL_LINE_MAX_BYTES, REFUSAL_DETAIL_PREFIX, REPLACEMENT, TRUNCATION_MARKER,
};

const LIMIT: Duration = Duration::from_secs(20);
/// A password no stream may ever carry.
const SEEDED: &str = "seeded-refusal-detail-password-51c9";
const EXPECTED: &str = "v1:00000000-0000-0000-0000-000000000001:sha256:abc";
const FOREIGN: &str = "v1:99999999-9999-9999-9999-999999999999:sha256:def";
/// What `DrillError::Guard` and `BackupError::Guard` put before the guard's
/// own sentence on the human line.
const HUMAN_PREFIX: &str = "guard: plan refused by the admission guard: ";

fn sentinel() -> (TcpListener, String) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let address = listener.local_addr().unwrap().to_string();
    (listener, address)
}

fn assert_no_connection(listener: &TcpListener, label: &str) {
    match listener.accept() {
        Err(error) if error.kind() == ErrorKind::WouldBlock => {}
        Ok((_, peer)) => panic!("{label}: the sentinel was dialled from {peer}"),
        Err(error) => panic!("{label}: inspect the sentinel: {error}"),
    }
}

fn base(root: &Path) -> Command {
    let tmp = root.join("tmp");
    std::fs::create_dir_all(&tmp).unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_logweir"));
    command
        .env_clear()
        .env("TMPDIR", &tmp)
        .env("RUST_LOG", "info")
        .env("LOGWEIR_ENGINE_BIN", root.join("no-engine"))
        .env("LOGWEIR_ENGINE_VERSION", "sentinel-version")
        .env("LOGWEIR_ENGINE_DIGEST", "sha256:sentinel-digest");
    command
}

/// Runs to completion or panics at [`LIMIT`]: a hung child must not block
/// the suite.
fn run(mut command: Command, label: &str) -> (Option<i32>, String, String) {
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let started = Instant::now();
    let mut child = command.spawn().expect("spawn logweir");
    let out: Output = loop {
        if child.try_wait().unwrap().is_some() {
            break child.wait_with_output().unwrap();
        }
        if started.elapsed() >= LIMIT {
            let _ = child.kill();
            let out = child.wait_with_output().unwrap();
            panic!(
                "{label}: exceeded {LIMIT:?}; stdout {} stderr {}",
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            );
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    (
        out.status.code(),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

/// An approval, its sidecar and the two keys for exactly `spec`'s bytes.
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
        approver: "refusal-detail-test@example.com".into(),
        ticket: "FX-34".into(),
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

/// `logweir restore run` over `spec`, with `env` added.
fn restore(spec: &str, env: &[(&str, &str)], label: &str) -> (Option<i32>, String, String) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("restore.yaml");
    std::fs::write(&path, spec).unwrap();
    let mut command = base(dir.path());
    command
        .args(["restore", "run", "--spec"])
        .arg(&path)
        .args(approved_bundle_args(dir.path(), spec));
    for (k, v) in env {
        command.env(k, v);
    }
    run(command, label)
}

/// `logweir backup run` over a one-topic spec whose source auth is `auth`.
fn backup(
    root: &Path,
    bootstrap: &str,
    auth: &str,
    env: &[(&str, &str)],
    label: &str,
) -> (Option<i32>, String, String) {
    let spec = format!(
        "backup_id: refusal-detail-row\nsource:\n  bootstrap_servers: [\"{bootstrap}\"]\n  \
         auth:\n{auth}  topics: [orders]\nstorage:\n  backend: filesystem\n  path: {}\n",
        root.join("archive").display()
    );
    std::fs::create_dir_all(root.join("archive")).unwrap();
    let spec_path = root.join("backup.yaml");
    std::fs::write(&spec_path, spec).unwrap();
    let allowed = root.join("allowed.json");
    std::fs::write(&allowed, r#"{"allowed_cluster_ids":[]}"#).unwrap();
    let mut command = base(root);
    command
        .args(["backup", "run", "--spec"])
        .arg(&spec_path)
        .arg("--allowed-clusters")
        .arg(&allowed)
        .arg("--signing-key")
        .arg(root.join("absent-signing-key.pem"));
    for (k, v) in env {
        command.env(k, v);
    }
    run(command, label)
}

fn example_restore_spec() -> String {
    std::fs::read_to_string("../../examples/drill.yaml").unwrap()
}

/// The non-empty stdout lines: what a controller's bounded tail holds.
fn lines(stdout: &str) -> Vec<&str> {
    stdout.lines().filter(|l| !l.trim().is_empty()).collect()
}

fn detail_lines(stdout: &str) -> Vec<&str> {
    stdout
        .lines()
        .filter(|l| l.starts_with(REFUSAL_DETAIL_PREFIX))
        .collect()
}

/// The ONE detail line of an exit-3 run, read as a controller reads it, with
/// the shape every such run owes: it is the line before `refusal-reason=`,
/// which is still last.
fn the_detail(stdout: &str, label: &str) -> RefusalDetail {
    let all = lines(stdout);
    assert!(all.len() >= 2, "{label}: {stdout}");
    let last = all[all.len() - 1];
    let before = all[all.len() - 2];
    assert!(
        last.starts_with("refusal-reason="),
        "{label}: I9's state line is still the FINAL stdout line: {stdout}"
    );
    assert!(
        before.starts_with(REFUSAL_DETAIL_PREFIX),
        "{label}: the detail line is the one before it: {stdout}"
    );
    assert_eq!(
        detail_lines(stdout).len(),
        1,
        "{label}: exactly one: {stdout}"
    );
    assert!(
        before.len() <= REFUSAL_DETAIL_LINE_MAX_BYTES,
        "{label}: {} bytes",
        before.len()
    );
    RefusalDetail::from_line(before)
        .unwrap_or_else(|| panic!("{label}: a controller must accept the runner's line: {before}"))
}

/// The guard's own sentence off the human line on stderr.
fn human_sentence<'a>(stderr: &'a str, label: &str) -> &'a str {
    stderr
        .lines()
        .find_map(|l| l.strip_prefix(HUMAN_PREFIX))
        .unwrap_or_else(|| panic!("{label}: the human line is unchanged and present: {stderr}"))
}

/// A restore plan a guard refuses with NO named reason: the detail line
/// carries `GuardRefused` and the sentence the human line carries, which is
/// kept as it was.
#[test]
fn a_refused_restore_prints_one_detail_line_before_its_state_line() {
    let spec =
        example_restore_spec().replace("topics: [orders, payments]", "topics: [\"orders*\"]");
    let (code, stdout, stderr) = restore(&spec, &[], "glob");
    assert_eq!(code, Some(3), "{stdout}\n{stderr}");
    let detail = the_detail(&stdout, "glob");
    assert_eq!(detail.code(), "GuardRefused");
    let human = human_sentence(&stderr, "glob");
    assert!(
        human.contains("`orders*` contains a glob metacharacter")
            && human.ends_with("this build cannot restore it."),
        "{human}"
    );
    assert_eq!(split_reason_code(human).0, "GuardRefused");
    // THE SAME SENTENCE, WHOLE: it is under the bound, so it ends as the
    // human line ends. What differs is one run the credential rules read as
    // key-shaped, the upstream source path the sentence cites: a relayed
    // sentence passes the same rules every relayed message does, and they
    // err towards removing.
    assert_eq!(detail.message(), clean_message(human));
    assert!(
        detail
            .message()
            .starts_with("source topic `orders*` contains a glob metacharacter (one of *?[]{});")
            && detail.message().ends_with("this build cannot restore it."),
        "{detail}"
    );
    assert_eq!(
        detail.message().replace(
            "[[redacted].rs:",
            "[U/kafka-backup/crates/kafka-backup-core/src/config.rs:"
        ),
        human
    );
}

/// A sentence over the bound is carried cut, with the marker; the human line
/// carries all of it. Four forbidden keys, one of them nested, make the
/// forbidden-key refusal longer than 512 bytes.
#[test]
fn a_sentence_over_the_bound_is_cut_with_a_marker() {
    let spec = example_restore_spec()
        + "\nengine_overrides:\n  dry_run: true\n  purge_topics: false\n  \
           header_preflight_external: true\n  nested:\n    dry_run: true\n";
    let (code, stdout, stderr) = restore(&spec, &[], "forbidden keys");
    assert_eq!(code, Some(3), "{stdout}\n{stderr}");
    let detail = the_detail(&stdout, "forbidden keys");
    assert_eq!(detail.code(), "GuardRefused");
    let human = human_sentence(&stderr, "forbidden keys");
    assert!(
        human.len() > REASON_MESSAGE_MAX_BYTES,
        "the row needs a sentence over the bound: {} bytes",
        human.len()
    );
    assert_eq!(detail.message(), clean_message(human));
    assert!(detail.message().len() <= REASON_MESSAGE_MAX_BYTES);
    assert!(detail.message().ends_with(TRUNCATION_MARKER), "{detail}");
    assert!(human.starts_with(detail.message().trim_end_matches(TRUNCATION_MARKER)));
    assert!(
        detail
            .message()
            .starts_with("forbidden key(s) present in the drill spec, at any value: "),
        "{detail}"
    );
}

/// A backup plan refused with a NAMED reason: the code is that name, the
/// state line names it too, and nothing was dialled. Control: the same plan
/// over TLS is not refused by this guard, stops later on another exit code,
/// and prints no detail line at all.
#[test]
fn a_refused_backup_names_its_reason_code_and_another_exit_prints_no_detail() {
    for (tls, refused) in [(false, true), (true, false)] {
        let root = tempfile::tempdir().unwrap();
        let (listener, address) = sentinel();
        let auth = format!("    mode: plain\n    username: logweir\n    tls: {tls}\n");
        let (code, stdout, stderr) = backup(
            root.path(),
            &address,
            &auth,
            &[("LOGWEIR_SOURCE_PASSWORD", SEEDED)],
            "plain",
        );
        if refused {
            assert_eq!(code, Some(3), "{stdout}\n{stderr}");
            let detail = the_detail(&stdout, "plain");
            assert_eq!(detail.code(), "PlainWithoutTls");
            assert_eq!(
                lines(&stdout).last().copied(),
                Some("refusal-reason=PlainWithoutTls")
            );
            let human = human_sentence(&stderr, "plain");
            assert_eq!(
                detail.to_string(),
                human,
                "short enough to be carried whole"
            );
            assert_no_connection(&listener, "plain without TLS");
        } else {
            assert_ne!(code, Some(3), "{stdout}\n{stderr}");
            assert_ne!(code, Some(0), "{stdout}\n{stderr}");
            assert!(
                detail_lines(&stdout).is_empty() && !stderr.contains(REFUSAL_DETAIL_PREFIX),
                "only exit 3 prints a detail line (exit {code:?}): {stdout}\n{stderr}"
            );
        }
        assert!(!stdout.contains(SEEDED) && !stderr.contains(SEEDED));
    }
}

/// Exit 1 prints no detail line: a plan file that does not exist is an
/// operational failure, and nothing was refused.
#[test]
fn an_operational_failure_prints_no_detail_line() {
    let root = tempfile::tempdir().unwrap();
    let mut command = base(root.path());
    command
        .args(["backup", "run", "--spec"])
        .arg(root.path().join("no-such-plan.yaml"))
        .arg("--allowed-clusters")
        .arg(root.path().join("no-such-allowlist.json"))
        .arg("--signing-key")
        .arg(root.path().join("absent-signing-key.pem"));
    let (code, stdout, stderr) = run(command, "exit 1");
    assert_eq!(code, Some(1), "{stdout}\n{stderr}");
    assert!(
        !stdout.contains(REFUSAL_DETAIL_PREFIX) && !stdout.contains("refusal-reason="),
        "{stdout}"
    );
}

/// **No secret rides along.** The two refusals that are ABOUT a credential
/// name the variable and the mechanism, and the detail line, like every other
/// line, carries no byte of the value or of either binding.
#[test]
fn a_refusal_about_a_credential_never_carries_its_value() {
    // The projected Secret's binding names another connection.
    let root = tempfile::tempdir().unwrap();
    let (listener, address) = sentinel();
    let auth = "    mode: scramSha512\n    username: logweir\n    tls: false\n";
    let (code, stdout, stderr) = backup(
        root.path(),
        &address,
        auth,
        &[
            ("LOGWEIR_SOURCE_PASSWORD", SEEDED),
            ("LOGWEIR_SOURCE_CREDENTIAL_BINDING_EXPECTED", EXPECTED),
            ("LOGWEIR_SOURCE_CREDENTIAL_BINDING", FOREIGN),
        ],
        "binding",
    );
    assert_eq!(code, Some(3), "{stdout}\n{stderr}");
    let detail = the_detail(&stdout, "binding");
    assert_eq!(detail.code(), "CredentialBindingMismatch");
    for secret in [SEEDED, EXPECTED, FOREIGN] {
        assert!(
            !stdout.contains(secret) && !stderr.contains(secret),
            "`{secret}` reached a stream: {stdout}\n{stderr}"
        );
    }
    assert_no_connection(&listener, "binding");

    // A projected password that cannot be rendered: the refusal names the
    // variable and the character CLASS.
    let password = format!("{SEEDED}\n bootstrap_servers:");
    let (code, stdout, stderr) = restore(
        &example_restore_spec(),
        &[("LOGWEIR_SOURCE_PASSWORD", &password)],
        "unrenderable",
    );
    assert_eq!(code, Some(3), "{stdout}\n{stderr}");
    let detail = the_detail(&stdout, "unrenderable");
    assert_eq!(detail.code(), "CredentialNotRenderable");
    assert!(
        detail.message().contains("LOGWEIR_SOURCE_PASSWORD"),
        "{detail}"
    );
    assert!(
        !stdout.contains(SEEDED) && !stderr.contains(SEEDED),
        "{stdout}\n{stderr}"
    );
}

/// A sentence that interpolates plan text is printed CLEANED: a topic name
/// carrying a bidi override and an ANSI escape reaches the detail line as
/// printable text, so the runner's own line already holds what the reader
/// would reduce it to.
#[test]
fn a_hostile_name_in_the_plan_is_cleaned_in_the_detail_line() {
    // YAML double-quoted escapes: U+202E RIGHT-TO-LEFT OVERRIDE and ESC.
    let spec = example_restore_spec().replace(
        "topics: [orders, payments]",
        "topics: [\"or\\u202Eders*\\e[2J\"]",
    );
    let (code, stdout, stderr) = restore(&spec, &[], "hostile");
    assert_eq!(code, Some(3), "{stdout}\n{stderr}");
    let line = detail_lines(&stdout)[0];
    assert!(
        line.chars().all(|c| c == ' '
            || c.is_ascii_graphic()
            || c == REPLACEMENT
            || c == TRUNCATION_MARKER),
        "the line itself is printable: {line:?}"
    );
    let detail = the_detail(&stdout, "hostile");
    assert!(
        detail
            .message()
            .contains(&format!("`or{REPLACEMENT}ders* [2J` contains a glob")),
        "{detail}"
    );
    // The control: the human line still carries what the plan said, raw.
    assert!(human_sentence(&stderr, "hostile").contains('\u{202E}'));
}

/// The writer seam, byte for byte, over the sentence PoC batch 5's runner
/// printed (`claude/artifacts/poc-batch-5/prod111/k3-runner.log`, an earlier
/// build's named reason): one line and its newline, and nothing for a refusal
/// with nothing to say.
#[test]
fn the_detail_writer_prints_exactly_one_json_line() {
    let sentence = "PartitionSubsetsAwaitOwnerDecision: restore.partitions names a partition \
        subset of orders; a restore of a partition subset is refused until the owner decides \
        how its scorecard is versioned (OD-9), because a verifier that predates it would read \
        the narrowed restore as a full one. Remove restore.partitions to restore every \
        partition (a window start, restore.point_in_time: \"<start>/<end>\", is accepted)";
    let mut out = Vec::new();
    logweir::exit::print_refusal_detail_to(&mut out, sentence).unwrap();
    let text = String::from_utf8(out).unwrap();
    assert_eq!(
        text,
        "refusal-detail={\"code\":\"PartitionSubsetsAwaitOwnerDecision\",\"message\":\"\
         restore.partitions names a partition subset of orders; a restore of a partition \
         subset is refused until the owner decides how its scorecard is versioned (OD-9), \
         because a verifier that predates it would read the narrowed restore as a full one. \
         Remove restore.partitions to restore every partition (a window start, \
         restore.point_in_time: \\\"<start>/<end>\\\", is accepted)\"}\n"
    );
    let mut nothing = Vec::new();
    logweir::exit::print_refusal_detail_to(&mut nothing, " \n").unwrap();
    assert!(nothing.is_empty());
}
