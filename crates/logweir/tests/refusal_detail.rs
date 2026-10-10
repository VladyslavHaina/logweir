//! **FX-34**, the RUNNER's half: a guard refusal prints its reason code and
//! its sentence as ONE `refusal-detail=` line, immediately before I9's
//! `refusal-reason=` line, at EVERY exit 3 and at no other exit; and the line
//! carries the Job's line token when the run was given one (`--line-token`),
//! and no token when it was not.
//!
//! Most rows run the shipped `logweir` binary against a loopback sentinel
//! that must never be dialled. They run it the way the controller does, WITH
//! a line token, and read the line back through
//! `logweir_core::refusal_detail::RefusalDetail::read_line`: the reader a
//! controller uses, so "the runner printed it" and "a controller accepts it"
//! are one assertion. The controller's own rows are in
//! `crates/weirkeeper/tests/{restore,backup}_controller.rs`,
//! `crates/weirkeeper/tests/line_token.rs` and `crates/weirkeeper/src/refusal.rs`.
//!
//! # Why the token, and not where the line stands
//!
//! A plan can start a line of its own in a pod log. This build's runner
//! escapes the line breaks in the error text it prints itself (PROD-15.1's
//! `one_line`); the Kafka client inside it logs to the same stderr by itself,
//! unescaped, and an error may repeat a plan value that holds a line break
//! (FX-43). Both streams reach a controller as one log, so
//! nothing about a line's place tells the runner's line from the plan's. The
//! token does: the controller makes it when it builds the Job, and a plan is
//! older than its Job. The last rows of this file run a plan that writes
//! marker lines of its own and show which line carries the token.

use std::ffi::OsString;
use std::io::ErrorKind;
use std::net::TcpListener;
use std::path::Path;
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

use logweir_core::refusal_detail::{
    clean_message, split_reason_code, LineRead, LineToken, RefusalDetail, RefusingRun,
    BACKUP_REASON_CODES, LINE_TOKEN_ARG, NO_SENTENCE, REASON_MESSAGE_MAX_BYTES,
    REFUSAL_DETAIL_LINE_MAX_BYTES, REFUSAL_DETAIL_PREFIX, REPLACEMENT, RESTORE_REASON_CODES,
    TRUNCATION_MARKER,
};
use RefusingRun::{Backup, Restore};

const LIMIT: Duration = Duration::from_secs(20);
/// A password no stream may ever carry.
const SEEDED: &str = "seeded-refusal-detail-password-51c9";
const EXPECTED: &str = "v1:00000000-0000-0000-0000-000000000001:sha256:abc";
const FOREIGN: &str = "v1:99999999-9999-9999-9999-999999999999:sha256:def";
/// What `DrillError::Guard` and `BackupError::Guard` put before the guard's
/// own sentence on the human line.
const HUMAN_PREFIX: &str = "guard: plan refused by the admission guard: ";

/// The line token these rows give the runner, as a controller would.
/// Assembled at run time, so no source line holds a secret-shaped literal.
fn token() -> LineToken {
    LineToken::parse(&"7f".repeat(20)).expect("forty hex digits")
}

/// The reader a controller uses, for a Job whose token is [`token`]: `Some`
/// when the line carries that token and validates.
fn read_as_the_controller(run: RefusingRun, line: &str) -> Option<RefusalDetail> {
    match RefusalDetail::read_line(run, &token(), line) {
        LineRead::Valid(detail) => Some(detail),
        LineRead::Invalid | LineRead::NotThisJobs => None,
    }
}

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
        approval_subject: None,
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
        .args(approved_bundle_args(dir.path(), spec))
        .args([LINE_TOKEN_ARG, token().expose_token()]);
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
    run(backup_command(root, bootstrap, auth, env), label)
}

/// The command [`backup`] runs.
fn backup_command(root: &Path, bootstrap: &str, auth: &str, env: &[(&str, &str)]) -> Command {
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
        .arg(root.join("absent-signing-key.pem"))
        .args([LINE_TOKEN_ARG, token().expose_token()]);
    for (k, v) in env {
        command.env(k, v);
    }
    command
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
fn the_detail(run: RefusingRun, stdout: &str, label: &str) -> RefusalDetail {
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
    let detail = read_as_the_controller(run, before)
        .unwrap_or_else(|| panic!("{label}: a controller must accept the runner's line: {before}"));
    assert!(
        detail.agrees_with_state(&last["refusal-reason=".len()..]),
        "{label}: the two lines agree: {before} / {last}"
    );
    detail
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
    let detail = the_detail(Restore, &stdout, "glob");
    assert_eq!(detail.code(), "GuardRefused");
    let human = human_sentence(&stderr, "glob");
    assert!(
        human.contains("`orders*` contains a glob metacharacter")
            && human.ends_with("this build cannot restore it."),
        "{human}"
    );
    assert_eq!(split_reason_code(Restore, human).0, "GuardRefused");
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
/// carries all of it. The forbidden-key refusal lists every key it found, so
/// sixteen of them make it longer than the bound.
#[test]
fn a_sentence_over_the_bound_is_cut_with_a_marker() {
    let nested: String = (1..=16)
        .map(|i| format!("  level{i:02}:\n    dry_run: true\n"))
        .collect();
    let spec = example_restore_spec() + "\nengine_overrides:\n" + &nested;
    let (code, stdout, stderr) = restore(&spec, &[], "forbidden keys");
    assert_eq!(code, Some(3), "{stdout}\n{stderr}");
    let detail = the_detail(Restore, &stdout, "forbidden keys");
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
            let detail = the_detail(Backup, &stdout, "plain");
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
    let detail = the_detail(Backup, &stdout, "binding");
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
    let detail = the_detail(Restore, &stdout, "unrenderable");
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
    let detail = the_detail(Restore, &stdout, "hostile");
    assert!(
        detail
            .message()
            .contains(&format!("`or{REPLACEMENT}ders* [2J` contains a glob")),
        "{detail}"
    );
    // The control: the human line still carries what the plan said, raw.
    assert!(human_sentence(&stderr, "hostile").contains('\u{202E}'));
}

/// A writer that records every `write` call it is given, whole.
#[derive(Default)]
struct Writes(Vec<Vec<u8>>);

impl std::io::Write for Writes {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.push(buf.to_vec());
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// **The writer seam, byte for byte**: the detail line and the state line,
/// each with its newline, in ONE write, so nothing this process prints can
/// come between them. Over the sentence PoC batch 5's runner printed
/// (`claude/artifacts/poc-batch-5/prod111/k3-runner.log`): its opening word
/// was a reason code then and is in no closed set now, so it stays in the
/// sentence.
///
/// KILLS: two writes; the detail line skipped for a refusal that says
/// nothing; a word outside the closed set printed as the code; the token
/// left out of the line, or written anywhere but its member.
#[test]
fn the_refusal_writer_prints_both_lines_in_one_write() {
    let sentence = "PartitionSubsetsAwaitOwnerDecision: restore.partitions names a partition \
        subset of orders; a restore of a partition subset is refused until the owner decides \
        how its scorecard is versioned (OD-9), because a verifier that predates it would read \
        the narrowed restore as a full one. Remove restore.partitions to restore every \
        partition (a window start, restore.point_in_time: \"<start>/<end>\", is accepted)";
    let mut out = Writes::default();
    logweir::exit::print_refusal_to(&mut out, Restore, None, sentence).unwrap();
    assert_eq!(out.0.len(), 1, "one write holds both lines");
    assert_eq!(
        String::from_utf8(out.0.remove(0)).unwrap(),
        "refusal-detail={\"code\":\"GuardRefused\",\"message\":\"\
         PartitionSubsetsAwaitOwnerDecision: restore.partitions names a partition subset of \
         orders; a restore of a partition subset is refused until the owner decides how its \
         scorecard is versioned (OD-9), because a verifier that predates it would read the \
         narrowed restore as a full one. Remove restore.partitions to restore every partition \
         (a window start, restore.point_in_time: \\\"<start>/<end>\\\", is accepted)\"}\n\
         refusal-reason=GuardRefused\n"
    );

    // A NAMED reason of this kind of run: the code, and the state line too
    // when it is one of I9's states.
    for (run, message, want) in [
        (
            Restore,
            "TargetTopicConfigRefused: cleanup.policy is `compact`",
            "refusal-detail={\"code\":\"TargetTopicConfigRefused\",\"message\":\"cleanup.policy is \
             `compact`\"}\nrefusal-reason=TargetTopicConfigRefused\n",
        ),
        (
            Restore,
            "PointUntrusted. The receipt is not signed by a pinned key",
            "refusal-detail={\"code\":\"PointUntrusted\",\"message\":\"The receipt is not signed \
             by a pinned key\"}\nrefusal-reason=GuardRefused\n",
        ),
        // The same sentence from a BACKUP: not a code of that kind of run.
        (
            Backup,
            "PointUntrusted. The receipt is not signed by a pinned key",
            "refusal-detail={\"code\":\"GuardRefused\",\"message\":\"PointUntrusted. The receipt \
             is not signed by a pinned key\"}\nrefusal-reason=GuardRefused\n",
        ),
        // A refusal that says nothing STILL prints both lines.
        (
            Backup,
            " \n",
            "refusal-detail={\"code\":\"GuardRefused\",\"message\":\"the refusal carried no \
             sentence\"}\nrefusal-reason=GuardRefused\n",
        ),
    ] {
        let mut out = Writes::default();
        logweir::exit::print_refusal_to(&mut out, run, None, message).unwrap();
        assert_eq!(out.0.len(), 1, "{message:?}");
        assert_eq!(String::from_utf8(out.0.remove(0)).unwrap(), want, "{message:?}");
    }
    assert_eq!(NO_SENTENCE, "the refusal carried no sentence");

    // WITH A TOKEN: the same two lines in one write, the token the FIRST
    // member of the detail line and nowhere in the state line.
    let own = token();
    let mut out = Writes::default();
    logweir::exit::print_refusal_to(
        &mut out,
        Restore,
        Some(&own),
        "PointUntrusted. The receipt is not signed by a pinned key",
    )
    .unwrap();
    assert_eq!(out.0.len(), 1, "one write holds both lines");
    assert_eq!(
        String::from_utf8(out.0.remove(0)).unwrap(),
        format!(
            "refusal-detail={{\"token\":\"{}\",\"code\":\"PointUntrusted\",\"message\":\"The \
             receipt is not signed by a pinned key\"}}\nrefusal-reason=GuardRefused\n",
            own.expose_token()
        )
    );
}

/// **The closed sets name the constants the runner's refusals open with.**
/// `logweir-core` cannot name this crate's constants or the engine crate's,
/// so its sets spell five codes as literals; this row holds each literal to
/// its constant, and holds every code to the kind of run that can print it.
#[test]
fn the_closed_sets_hold_the_constants_the_refusals_open_with() {
    use logweir::drill::binding;
    for code in [
        binding::POINT_BINDING_MISMATCH,
        binding::POINT_BINDING_SET_MISMATCH,
        binding::POINT_UNTRUSTED,
        binding::REHEARSAL_SCOPE_VIOLATION,
        logweir_core::execution_contract::AUTHORIZATION_INVALID,
        logweir_core::execution_contract::AUTHORIZATION_EXPIRED,
        logweir_core::guard::STORAGE_REGION_INVALID,
    ] {
        assert!(RESTORE_REASON_CODES.contains(&code), "restore: {code}");
    }
    for state in logweir_core::guard::TERMINAL_STATES {
        assert!(RESTORE_REASON_CODES.contains(&state), "restore: {state}");
    }
    for code in [
        logweir_engine_oso::storage::WORKLOAD_IDENTITY_NOT_INJECTED,
        logweir_core::consumer_positions::SELECTION_TOO_LARGE,
        logweir_core::consumer_positions::SELECTION_ID_INVALID,
        logweir_core::consumer_positions::SELECTION_REPEATED,
        logweir_core::guard::STORAGE_REGION_INVALID,
        logweir_core::guard::TERMINAL_STATE_CREDENTIAL_NOT_RENDERABLE,
        logweir_core::guard::TERMINAL_STATE_PLAIN_WITHOUT_TLS,
        logweir_core::guard::TERMINAL_STATE_CREDENTIAL_BINDING_MISMATCH,
    ] {
        assert!(BACKUP_REASON_CODES.contains(&code), "backup: {code}");
    }
    // PER KIND: a restore-only code is not a backup's, and the reverse.
    for code in [
        binding::POINT_UNTRUSTED,
        logweir_core::guard::TERMINAL_STATE_TARGET_TOPIC_CONFIG_REFUSED,
        logweir_core::guard::TERMINAL_STATE_POINT_IN_TIME_BY_PRODUCER_TIME,
        logweir_core::execution_contract::AUTHORIZATION_INVALID,
    ] {
        assert!(!BACKUP_REASON_CODES.contains(&code), "{code}");
    }
    for code in [
        logweir_engine_oso::storage::WORKLOAD_IDENTITY_NOT_INJECTED,
        logweir_core::consumer_positions::SELECTION_TOO_LARGE,
    ] {
        assert!(!RESTORE_REASON_CODES.contains(&code), "{code}");
    }
}

/// The example `docs/kubernetes.md` §10 quotes is what the runner prints for
/// that plan, word for word: an `http://` archive endpoint with
/// `allow_http: false` (C15). The refusal names the field and never the
/// endpoint, so the detail line does not carry it either.
#[test]
fn the_documented_example_is_what_the_runner_prints() {
    let example = example_restore_spec();
    let spec = example.replacen("    allow_http: true\n", "    allow_http: false\n", 1);
    assert_ne!(
        spec, example,
        "the row edits the source storage's allow_http"
    );
    let (code, stdout, stderr) = restore(&spec, &[], "c15");
    assert_eq!(code, Some(3), "{stdout}\n{stderr}");
    let detail = the_detail(Restore, &stdout, "c15");
    let documented = "GuardRefused: source.storage.endpoint is a plain http:// endpoint but \
        source.storage.allow_http is false. The pinned engine (kafka-backup 0.22.0 and later) \
        derives plaintext transport from an http:// endpoint whatever allow_http says, so it \
        would dial the archive in the clear although the spec asked for no plaintext. Set \
        allow_http: true to state plaintext explicitly, or use an https:// endpoint.";
    assert_eq!(detail.to_string(), documented);
    assert_eq!(
        format!("GuardRefused: {}", human_sentence(&stderr, "c15")),
        documented,
        "and it is the human line's sentence, whole"
    );
    let doc = std::fs::read_to_string("../../docs/kubernetes.md").expect("the operator doc ships");
    assert!(
        doc.contains(&format!(
            "; the runner's own reason, cleaned and bounded: {documented}\n"
        )),
        "docs/kubernetes.md quotes this refusal as its example; keep the two in step"
    );
    assert!(
        !detail_lines(&stdout)[0].contains(":9000"),
        "the endpoint's value is not in the line: {stdout}"
    );
}

// ---------------------------------------------------------------------------
// What the process writes, in the order it writes it, at every exit 3
// ---------------------------------------------------------------------------
//
// A controller does not trust a line for where it stands (the token decides).
// These rows still hold the runner to its own order, because it is the
// contract `docs/stability.md` states and the state line's reader relies on:
// the pair is the last thing the process writes, in one write.

/// Runs `command` with stdout AND stderr on ONE open file, so the file holds
/// both streams in the order the process wrote them. A pod log merges the two
/// in no promised order; this is the order the runner is answerable for.
fn run_merged(mut command: Command, label: &str) -> (Option<i32>, String) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("merged.log");
    let file = std::fs::File::create(&path).unwrap();
    command
        .stdin(Stdio::null())
        .stdout(Stdio::from(file.try_clone().unwrap()))
        .stderr(Stdio::from(file));
    let started = Instant::now();
    let mut child = command.spawn().expect("spawn logweir");
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if started.elapsed() >= LIMIT {
            let _ = child.kill();
            let _ = child.wait();
            panic!(
                "{label}: exceeded {LIMIT:?}: {}",
                std::fs::read_to_string(&path).unwrap_or_default()
            );
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    (
        status.code(),
        String::from_utf8_lossy(&std::fs::read(&path).unwrap()).into_owned(),
    )
}

fn restore_command(dir: &Path, spec: &str, env: &[(&str, &str)]) -> Command {
    let mut command = restore_command_without_a_token(dir, spec, env);
    command.args([LINE_TOKEN_ARG, token().expose_token()]);
    command
}

/// `logweir restore run` as a person starts it: no `--line-token`.
fn restore_command_without_a_token(dir: &Path, spec: &str, env: &[(&str, &str)]) -> Command {
    let path = dir.join("restore.yaml");
    std::fs::write(&path, spec).unwrap();
    let mut command = base(dir);
    command
        .args(["restore", "run", "--spec"])
        .arg(&path)
        .args(approved_bundle_args(dir, spec));
    for (k, v) in env {
        command.env(k, v);
    }
    command
}

/// What every exit-3 run owes over both streams in written order: the pair is
/// the last two non-empty lines, each key appears once at the start of a line
/// the runner wrote last, and the human line came before.
fn assert_the_pair_ends_the_log(run: RefusingRun, merged: &str, label: &str) -> RefusalDetail {
    let all = lines(merged);
    assert!(all.len() >= 3, "{label}: {merged}");
    let (before, last) = (all[all.len() - 2], all[all.len() - 1]);
    assert!(
        before.starts_with(REFUSAL_DETAIL_PREFIX) && last.starts_with("refusal-reason="),
        "{label}: the last two lines the process wrote are the pair:\n{merged}"
    );
    let human_at = all
        .iter()
        .position(|l| l.starts_with(HUMAN_PREFIX))
        .unwrap_or_else(|| panic!("{label}: the human line is present:\n{merged}"));
    assert!(
        human_at < all.len() - 2,
        "{label}: the error text is written BEFORE the pair:\n{merged}"
    );
    let detail = read_as_the_controller(run, before)
        .unwrap_or_else(|| panic!("{label}: a controller accepts the line: {before}"));
    assert!(detail.agrees_with_state(&last["refusal-reason=".len()..]));
    detail
}

/// One refused restore of the row below: its label, its plan, the variables
/// added to the runner's environment, and the reason code it must print.
type RefusedRestore<'a> = (&'a str, String, Vec<(&'a str, &'a str)>, &'a str);

/// **Every exit-3 path prints the pair after the error text, as its last two
/// lines.** Six refusals, from the plan scan to the credential checks, on
/// both runners: each ends the same way, because every exit 3 of a run leaves
/// through `exiting` (the source-level row below holds that).
///
/// KILLS: the pair printed before the human line; a line printed after it; a
/// refusal path that skips the detail line.
#[test]
fn every_refused_run_writes_the_pair_last_after_its_error_text() {
    let example = example_restore_spec();
    let unrenderable = format!("{SEEDED}\n bootstrap_servers:");
    let restores: Vec<RefusedRestore<'_>> = vec![
        (
            "a glob topic (the admission guard)",
            example.replace("topics: [orders, payments]", "topics: [\"orders*\"]"),
            vec![],
            "GuardRefused",
        ),
        (
            "a forbidden key (the plan scan)",
            example.clone() + "\nengine_overrides:\n  x:\n    dry_run: true\n",
            vec![],
            "GuardRefused",
        ),
        (
            "an http endpoint without allow_http (C15)",
            example.replacen("    allow_http: true\n", "    allow_http: false\n", 1),
            vec![],
            "GuardRefused",
        ),
        (
            "an unrenderable projected password",
            example.clone(),
            vec![("LOGWEIR_SOURCE_PASSWORD", unrenderable.as_str())],
            "CredentialNotRenderable",
        ),
    ];
    for (label, spec, env, code) in restores {
        let dir = tempfile::tempdir().unwrap();
        let (exit, merged) = run_merged(restore_command(dir.path(), &spec, &env), label);
        assert_eq!(exit, Some(3), "{label}:\n{merged}");
        let detail = assert_the_pair_ends_the_log(Restore, &merged, label);
        assert_eq!(detail.code(), code, "{label}");
        assert!(!merged.contains(SEEDED), "{label}");
    }

    // The backup runner, two refusals.
    for (label, auth, env, code) in [
        (
            "SASL/PLAIN without TLS",
            "    mode: plain\n    username: logweir\n    tls: false\n",
            vec![("LOGWEIR_SOURCE_PASSWORD", SEEDED)],
            "PlainWithoutTls",
        ),
        (
            "a credential bound to another connection",
            "    mode: scramSha512\n    username: logweir\n    tls: false\n",
            vec![
                ("LOGWEIR_SOURCE_PASSWORD", SEEDED),
                ("LOGWEIR_SOURCE_CREDENTIAL_BINDING_EXPECTED", EXPECTED),
                ("LOGWEIR_SOURCE_CREDENTIAL_BINDING", FOREIGN),
            ],
            "CredentialBindingMismatch",
        ),
    ] {
        let root = tempfile::tempdir().unwrap();
        let (listener, address) = sentinel();
        let (exit, merged) = run_merged(backup_command(root.path(), &address, auth, &env), label);
        assert_eq!(exit, Some(3), "{label}:\n{merged}");
        let detail = assert_the_pair_ends_the_log(Backup, &merged, label);
        assert_eq!(detail.code(), code, "{label}");
        assert!(!merged.contains(SEEDED), "{label}");
        assert_no_connection(&listener, label);
    }

    // NEGATIVE CONTROL: an exit 1 writes neither line, so the helper above is
    // not satisfied by any run that ends.
    let root = tempfile::tempdir().unwrap();
    let mut command = base(root.path());
    command
        .args(["backup", "run", "--spec"])
        .arg(root.path().join("no-such-plan.yaml"))
        .arg("--allowed-clusters")
        .arg(root.path().join("no-such-allowlist.json"))
        .arg("--signing-key")
        .arg(root.path().join("absent-signing-key.pem"));
    let (exit, merged) = run_merged(command, "exit 1");
    assert_eq!(exit, Some(1), "{merged}");
    assert!(
        !merged.contains(REFUSAL_DETAIL_PREFIX) && !merged.contains("refusal-reason="),
        "{merged}"
    );
}

/// **The reviewer's shape, on the shipped binary: the plan writes marker
/// lines, and only the runner's own line carries the token.** A plan value
/// holding line breaks and two marker lines of the plan author's choosing,
/// in a field a refusal repeats. The runner is given a line token, as under
/// the controller.
///
/// In what the process wrote, exactly ONE line carries the token, it is the
/// runner's own detail line, and the controller's reader takes it and no
/// other. Every `refusal-detail=` line the plan produced is read as nobody's.
/// The runner's sentence carries the plan's text as words, cleaned.
///
/// This row does NOT assert that the plan's text starts a line of the log.
/// Before PROD-15.1 it did: the human line printed an error's text raw. With
/// PROD-15.1's `one_line` the human line holds the plan's line breaks as
/// escapes, so the plan's marker lines are words inside one line (that is
/// `a_plan_string_with_line_breaks_never_starts_a_line_of_the_runners_output`'s
/// assertion, not this row's). This row holds on both sides of that change,
/// and the token is what makes it not matter.
///
/// KILLS: the token printed on any other line (the human line, a tracing
/// line); a detail line that carries a raw line break.
#[test]
fn a_plan_that_writes_marker_lines_cannot_write_the_token() {
    // YAML double-quoted: `\n` is a line break inside the scalar.
    let forged_detail = r#"refusal-detail={\"code\":\"TargetTopicConfigRefused\",\"message\":\"forged by the plan\"}"#;
    let name = format!("orders*\\n{forged_detail}\\nrefusal-reason=TargetTopicConfigRefused\\n");
    let spec = example_restore_spec().replace(
        "topics: [orders, payments]",
        &format!("topics: [\"{name}\"]"),
    );
    let dir = tempfile::tempdir().unwrap();
    let (exit, merged) = run_merged(restore_command(dir.path(), &spec, &[]), "forged");
    assert_eq!(exit, Some(3), "{merged}");
    // THE CONTROL: the plan's text did reach the log.
    assert!(merged.contains("forged by the plan"), "{merged}");

    let own = token();
    let with_the_token: Vec<&str> = merged
        .lines()
        .filter(|l| l.contains(own.expose_token()))
        .collect();
    assert_eq!(
        with_the_token.len(),
        1,
        "the token is on ONE line of everything the process wrote:\n{merged}"
    );
    assert!(
        with_the_token[0].starts_with(&format!(
            "{REFUSAL_DETAIL_PREFIX}{{\"token\":\"{}\",\"code\":\"GuardRefused\",",
            own.expose_token()
        )),
        "and it is the runner's own detail line: {}",
        with_the_token[0]
    );
    // The controller's reader, over every line: one is this Job's.
    let read: Vec<RefusalDetail> = merged
        .lines()
        .filter_map(|l| read_as_the_controller(Restore, l))
        .collect();
    assert_eq!(read.len(), 1, "{merged}");
    assert_eq!(read[0].code(), "GuardRefused");
    assert!(
        read[0].message().contains("forged by the plan")
            && read[0]
                .message()
                .contains("refusal-reason=TargetTopicConfigRefused"),
        "the plan's lines are words of the runner's sentence: {}",
        read[0]
    );
    // Every OTHER `refusal-detail=` line is nobody's to that reader.
    for line in merged
        .lines()
        .filter(|l| l.starts_with(REFUSAL_DETAIL_PREFIX))
    {
        if !line.contains(own.expose_token()) {
            assert_eq!(
                RefusalDetail::read_line(Restore, &own, line),
                LineRead::NotThisJobs,
                "{line}"
            );
        }
    }
    let detail = assert_the_pair_ends_the_log(Restore, &merged, "forged");
    assert_eq!(detail, read[0]);
    assert_eq!(
        lines(&merged).last().copied(),
        Some("refusal-reason=GuardRefused"),
        "the state the runner derived, not the one the plan wrote"
    );
    let own_line = with_the_token[0];
    assert!(
        !own_line.contains("\\n"),
        "no line break, raw or escaped: {own_line}"
    );
}

/// **A run given a token prints it in its one line and nowhere else; a run
/// started by hand prints the line with no token.**
///
/// * With `--line-token`: the token appears once on stdout, as the first
///   member of the detail line, and not at all on stderr.
/// * Without it: the detail line has no `token` member, and is byte for byte
///   the two-member line; a controller's reader takes it as nobody's.
/// * An exit 1 given a token prints the token nowhere.
/// * A malformed token is a usage error (exit 1) before anything runs, and
///   the message does not repeat a well-formed token it was not given.
///
/// KILLS: the token logged (a tracing line or the human line carrying it);
/// the token printed when none was given; the token printed at exit 1.
#[test]
fn a_run_prints_the_token_it_was_given_in_one_line_and_none_otherwise() {
    let own = token();
    let spec =
        example_restore_spec().replace("topics: [orders, payments]", "topics: [\"orders*\"]");

    // WITH the token.
    let dir = tempfile::tempdir().unwrap();
    let mut command = restore_command(dir.path(), &spec, &[]);
    command.env("RUST_LOG", "trace");
    let (code, stdout, stderr) = run(command, "with a token");
    assert_eq!(code, Some(3), "{stdout}\n{stderr}");
    assert_eq!(
        stdout.matches(own.expose_token()).count(),
        1,
        "once on stdout, at the most verbose log level: {stdout}"
    );
    assert!(
        !stderr.contains(own.expose_token()),
        "never on stderr: {stderr}"
    );
    let detail = the_detail(Restore, &stdout, "with a token");
    let line = detail_lines(&stdout)[0];
    assert!(line.starts_with(&format!(
        "{REFUSAL_DETAIL_PREFIX}{{\"token\":\"{}\",\"code\":\"",
        own.expose_token()
    )));

    // WITHOUT it: the same refusal, the same sentence, no token member.
    let dir = tempfile::tempdir().unwrap();
    let (code, stdout, stderr) = run(
        restore_command_without_a_token(dir.path(), &spec, &[]),
        "by hand",
    );
    assert_eq!(code, Some(3), "{stdout}\n{stderr}");
    let bare = detail_lines(&stdout)[0];
    assert!(!bare.contains("token"), "{bare}");
    assert_eq!(
        bare,
        line.replacen(&format!("\"token\":\"{}\",", own.expose_token()), "", 1),
        "the line a person's run prints is the tokened line without its first member"
    );
    assert!(bare.starts_with(&format!(
        "{REFUSAL_DETAIL_PREFIX}{{\"code\":\"GuardRefused\",\"message\":\""
    )));
    assert_eq!(
        RefusalDetail::read_line(Restore, &own, bare),
        LineRead::NotThisJobs,
        "and a controller's reader takes it as nobody's"
    );
    assert_eq!(
        lines(&stdout).last().copied(),
        Some("refusal-reason=GuardRefused")
    );
    let _ = detail;

    // An exit 1 given a token: neither line, and the token nowhere.
    let root = tempfile::tempdir().unwrap();
    let mut command = base(root.path());
    command
        .args(["backup", "run", "--spec"])
        .arg(root.path().join("no-such-plan.yaml"))
        .arg("--allowed-clusters")
        .arg(root.path().join("no-such-allowlist.json"))
        .arg("--signing-key")
        .arg(root.path().join("absent-signing-key.pem"))
        .args([LINE_TOKEN_ARG, own.expose_token()])
        .env("RUST_LOG", "trace");
    let (code, stdout, stderr) = run(command, "exit 1 with a token");
    assert_eq!(code, Some(1), "{stdout}\n{stderr}");
    assert!(
        !stdout.contains(own.expose_token()) && !stderr.contains(own.expose_token()),
        "{stdout}\n{stderr}"
    );
    assert!(!stdout.contains(REFUSAL_DETAIL_PREFIX));

    // A value that is not a token: a usage error before anything runs.
    for bad in ["not-a-token", "ABCDEF0123456789ABCDEF0123456789", "abc"] {
        let dir = tempfile::tempdir().unwrap();
        let mut command = restore_command_without_a_token(dir.path(), &spec, &[]);
        command.args([LINE_TOKEN_ARG, bad]);
        let (code, stdout, stderr) = run(command, "a malformed token");
        assert_eq!(code, Some(1), "{bad}: {stdout}\n{stderr}");
        assert!(
            stderr.contains("a line token is 32 to 128 lower-case hex digits"),
            "{bad}: {stderr}"
        );
        assert!(
            !stdout.contains(REFUSAL_DETAIL_PREFIX) && !stdout.contains("refusal-reason="),
            "{bad}: nothing ran: {stdout}"
        );
    }
    // And the flag given twice is refused too: a second one cannot be added
    // to a Job's arguments and win.
    let dir = tempfile::tempdir().unwrap();
    let mut command = restore_command(dir.path(), &spec, &[]);
    command.args([LINE_TOKEN_ARG, &"3c".repeat(20)]);
    let (code, stdout, stderr) = run(command, "two tokens");
    assert_eq!(code, Some(1), "{stdout}\n{stderr}");
    assert!(!stdout.contains(REFUSAL_DETAIL_PREFIX), "{stdout}");
}

/// **Source-level: every exit 3 of a run leaves through the one printer.**
///
/// Read off the two runners' sources, test modules excluded:
///
/// * exit code 3 is PRODUCED in one place per runner, the `Guard` arm of its
///   error's `exit_code`;
/// * `exiting` is called from one place per runner, and it is where
///   `crate::exit::print_refusal(` is called, once, directly under
///   `if code == ExitCode::GuardRefused {`;
/// * nothing in either runner prints the state line by itself, which would
///   leave the detail line's position to whatever came before; and
///   `src/exit.rs` has no printer of the state line alone to reach for (the
///   old `print_refusal_reason` and its writer seam are removed, and
///   `refusal_reason_line` is called in one place, inside `print_refusal_to`).
///
/// KILLS: a second exit-3 path that returns the code without `exiting`; the
/// state line printed alone; a printer of the state line alone put back.
#[test]
fn every_exit_three_of_a_run_leaves_through_the_one_printer() {
    fn production(path: &str) -> String {
        let text = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{path}: {e}"));
        let cut = text.find("\n#[cfg(test)]").unwrap_or(text.len());
        text[..cut]
            .lines()
            .filter(|l| !l.trim_start().starts_with("//"))
            .collect::<Vec<_>>()
            .join("\n")
    }
    fn count(haystack: &str, needle: &str) -> usize {
        haystack.matches(needle).count()
    }
    for (path, guard_arm, run) in [
        (
            "src/drill/mod.rs",
            "DrillError::Guard(_) => ExitCode::GuardRefused,",
            "Restore",
        ),
        (
            "src/backup/mod.rs",
            "BackupError::Guard(_) => ExitCode::GuardRefused,",
            "Backup",
        ),
    ] {
        let text = production(path);
        assert_eq!(
            count(&text, guard_arm),
            1,
            "{path}: the one producer of exit 3"
        );
        assert_eq!(
            count(&text, "=> ExitCode::GuardRefused"),
            1,
            "{path}: no other arm produces exit 3"
        );
        assert_eq!(
            count(&text, "return ExitCode::GuardRefused"),
            0,
            "{path}: and nothing returns it directly"
        );
        assert_eq!(count(&text, "\nfn exiting("), 1, "{path}");
        assert_eq!(
            count(&text, "    exiting(\n"),
            1,
            "{path}: `exiting` has one caller, the one terminal path"
        );
        let printer = format!(
            "    if code == ExitCode::GuardRefused {{\n        crate::exit::print_refusal(\n            \
             logweir_core::refusal_detail::RefusingRun::{run},\n"
        );
        assert_eq!(
            count(&text, &printer),
            1,
            "{path}: the pair is printed under the exit-3 gate, for this kind of run"
        );
        assert_eq!(
            count(&text, "print_refusal("),
            1,
            "{path}: and nowhere else"
        );
        assert_eq!(
            count(&text, "print_refusal_reason"),
            0,
            "{path}: the state line is never printed by itself"
        );
    }
    // No other file of either runner produces the code or prints the lines.
    for dir in ["src/drill", "src/backup"] {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            let name = path.to_string_lossy().into_owned();
            if !name.ends_with(".rs") || name.ends_with("/mod.rs") {
                continue;
            }
            let text = production(&name);
            for needle in [
                "ExitCode::GuardRefused",
                "print_refusal",
                "refusal-reason=",
                "refusal-detail=",
            ] {
                assert_eq!(count(&text, needle), 0, "{name}: `{needle}`");
            }
        }
    }
    // NEGATIVE CONTROL: the reader of sources sees code and skips comments.
    let exit = production("src/exit.rs");
    assert!(exit.contains("pub fn print_refusal_to<W: std::io::Write>("));
    assert!(!exit.contains("/// # Why a second line, and why it is first (FX-34)"));
    // ONE PRINTER IN `exit.rs` TOO: no function prints the state line alone
    // (its name appears in comments only, which `production` drops), and the
    // state line's text is built in one place, beside the detail line's.
    assert_eq!(count(&exit, "print_refusal_reason"), 0);
    assert_eq!(count(&exit, "refusal_reason_line("), 1);
    assert_eq!(count(&exit, "refusal_detail_line("), 1);
    assert_eq!(
        count(&exit, "pub fn print_refusal"),
        2,
        "`print_refusal` and its writer seam, and no third"
    );
}
