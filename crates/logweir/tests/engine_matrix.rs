//! `.github/workflows/engine-matrix.yml` and the three scripts it calls.
//!
//! PROD-00.1 (`docs/to-do/decisions/PROD-00-engine-route.md`) found every
//! scheduled run red (2026-09-14, 2026-09-21, 2026-09-28) for independent
//! reasons. Each group of tests below pins one of them:
//!
//! 1. `scripts/run-named-tests.sh` reported an EXISTING test missing: under
//!    `pipefail`, `printf "$listing" | grep -q` fails once the listing outgrows
//!    the pipe buffer, because `grep -q` exits at its first match and `printf`
//!    takes EPIPE.
//! 2. `scripts/e2e-seed.sh` refused every engine below 0.21, which writes no
//!    segment sha256, before a single row test ran.
//! 3. The matrix's full-drill step drifted from the CI e2e job: no
//!    `scram-setup`, and a docker-desktop-only test.
//! 4. `upload-artifact@v4` skips hidden paths, so the rows written to
//!    `.matrix/` were never uploaded and `publish` found none.
//! 5. `publish` would have rewritten the whole hand-written `## Rows` table.
//!
//! The review of PROD-00.1 (M1, M2, L6) added the steps that decide whether a
//! green job means anything:
//!
//! 6. the row's outcome is derived from what every step did
//!    (`scripts/engine-matrix-outcome.sh`), never assumed from the floor;
//! 7. the last verdict step fails a row that records anything but its
//!    declaration;
//! 8. the seed's digest mode follows the row's floor;
//! 9. a pull request is opened only from `main`, and only when opted in;
//! 10. the broker is read back from the running container
//!     (`scripts/engine-matrix-broker.sh`), and the row records that version.
use serde_yaml::Value;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus};
use std::time::{Duration, Instant};

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn read(rel: &str) -> String {
    std::fs::read_to_string(root().join(rel)).unwrap_or_else(|e| panic!("{rel}: {e}"))
}

struct Ran {
    status: ExitStatus,
    stdout: String,
    stderr: String,
}

impl Ran {
    fn transcript(&self) -> String {
        format!("stdout:\n{}\nstderr:\n{}", self.stdout, self.stderr)
    }
}

/// Every child is bounded: polled against a deadline and killed on expiry. Its
/// output goes to files rather than pipes, so a transcript larger than a pipe
/// buffer cannot stall the poll.
fn run_bounded(mut cmd: Command, secs: u64) -> Ran {
    let dir = tempfile::tempdir().expect("a temp dir");
    let out_path = dir.path().join("stdout");
    let err_path = dir.path().join("stderr");
    cmd.stdout(File::create(&out_path).unwrap())
        .stderr(File::create(&err_path).unwrap());
    let mut child = cmd.spawn().expect("the child starts");
    let deadline = Instant::now() + Duration::from_secs(secs);
    let status = loop {
        if let Some(status) = child.try_wait().expect("poll the child") {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("the child ran past its {secs}s bound");
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    Ran {
        status,
        stdout: std::fs::read_to_string(&out_path).unwrap_or_default(),
        stderr: std::fs::read_to_string(&err_path).unwrap_or_default(),
    }
}

fn with_path(cmd: &mut Command, bin: &Path) {
    cmd.env(
        "PATH",
        format!(
            "{}:{}",
            bin.display(),
            std::env::var("PATH").unwrap_or_default()
        ),
    );
}

fn write_executable(path: &Path, body: &str) {
    std::fs::write(path, body).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
}

// ---------------------------------------------------------------------------
// 1. run-named-tests.sh
// ---------------------------------------------------------------------------

/// A `cargo` whose `--list` prints `first` and then 30,000 more names (about
/// 1.7 MB, far past any pipe buffer), and which otherwise echoes its argv.
fn fake_cargo(first: &str) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("bin")).unwrap();
    write_executable(
        &dir.path().join("bin/cargo"),
        &format!(
            r#"#!/usr/bin/env bash
for a in "$@"; do
  if [ "$a" = "--list" ]; then
    echo "{first}: test"
    awk 'BEGIN {{ for (i = 0; i < 30000; i++) printf "some_module::a_rather_long_test_name_number_%06d: test\n", i }}'
    exit 0
  fi
done
echo "ran: $*"
"#
        ),
    );
    dir
}

fn run_named(fake: &tempfile::TempDir, names: &[&str]) -> Ran {
    let mut cmd = Command::new("bash");
    cmd.arg(root().join("scripts/run-named-tests.sh"))
        .args(names)
        .env_remove("CARGO_TEST_ARGS")
        .env_remove("LIBTEST_ARGS");
    with_path(&mut cmd, &fake.path().join("bin"));
    run_bounded(cmd, 60)
}

/// The weekly run of 2026-09-28 recorded `fail(lever-not-honoured)` for the
/// pinned engine because this lookup failed for a test that existed.
#[test]
fn a_named_test_is_found_in_a_listing_larger_than_a_pipe_buffer() {
    let fake = fake_cargo("wanted_test");
    let ran = run_named(&fake, &["wanted_test"]);
    assert!(
        ran.status.success() && ran.stdout.contains("ok: wanted_test"),
        "an existing test was reported missing:\n{}",
        ran.transcript()
    );
    assert!(
        ran.stdout.contains(
            "ran: test --workspace --features e2e -- --test-threads=1 --exact wanted_test"
        ),
        "the named test must then be run with --exact:\n{}",
        ran.transcript()
    );
}

/// The fix must not make the lookup vacuous: a name that matches nothing still
/// fails before anything runs.
#[test]
fn a_missing_name_still_fails_before_anything_runs() {
    let fake = fake_cargo("wanted_test");
    let ran = run_named(&fake, &["not_a_test_anywhere"]);
    assert_eq!(ran.status.code(), Some(1), "{}", ran.transcript());
    assert!(
        ran.stderr
            .contains("no test named `not_a_test_anywhere` exists"),
        "{}",
        ran.transcript()
    );
    assert!(!ran.stdout.contains("ran:"), "{}", ran.transcript());
}

// ---------------------------------------------------------------------------
// 2. seed-manifest-check.py and e2e-seed.sh's digest mode
// ---------------------------------------------------------------------------

const PRE_021_MANIFEST: &str = "e2e/fixtures/manifests/0.19.2.json";
const PRE_021_KEY: &str = "orders/0/00000000000000000000.seg";

fn manifest_check(mode: Option<&str>, manifest: &str, records: u64, key: &str, sha: &str) -> Ran {
    let mut cmd = Command::new("python3");
    cmd.arg(root().join("scripts/seed-manifest-check.py"));
    if let Some(mode) = mode {
        cmd.args(["--segment-sha256", mode]);
    }
    cmd.arg(root().join(manifest))
        .arg(records.to_string())
        .arg(key)
        .arg(sha);
    run_bounded(cmd, 60)
}

/// The committed upstream pair: `upstream-0.21.0.kbak` and the key the 0.21
/// manifest records for it.
fn real_021_pair() -> (String, String) {
    let bytes = std::fs::read(root().join("e2e/fixtures/segments/upstream-0.21.0.kbak")).unwrap();
    let sha = logweir_core::ids::sha256_hex(&bytes);
    let manifest: serde_json::Value =
        serde_json::from_str(&read("e2e/fixtures/manifests/0.21.json")).unwrap();
    let key = manifest["topics"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|t| t["partitions"].as_array().unwrap())
        .flat_map(|p| p["segments"].as_array().unwrap())
        .find(|s| s["sha256"] == serde_json::Value::String(sha.clone()))
        .expect("the 0.21 manifest records the committed segment's digest")["key"]
        .as_str()
        .unwrap()
        .to_string();
    (key, sha)
}

#[test]
fn the_default_mode_accepts_the_real_021_pair() {
    let (key, sha) = real_021_pair();
    let ran = manifest_check(None, "e2e/fixtures/manifests/0.21.json", 2000, &key, &sha);
    assert!(ran.status.success(), "{}", ran.transcript());
    assert!(
        ran.stdout.contains("every sha256 present"),
        "{}",
        ran.transcript()
    );
    assert!(ran.stdout.contains("matches the manifest byte for byte"));
}

#[test]
fn the_default_mode_refuses_a_manifest_without_segment_digests() {
    let ran = manifest_check(None, PRE_021_MANIFEST, 150, PRE_021_KEY, "00");
    assert_eq!(ran.status.code(), Some(1), "{}", ran.transcript());
    assert!(
        ran.stderr.contains("segments with an empty sha256"),
        "{}",
        ran.transcript()
    );
}

#[test]
fn optional_digests_accept_a_manifest_that_carries_none() {
    let ran = manifest_check(Some("optional"), PRE_021_MANIFEST, 150, PRE_021_KEY, "00");
    assert!(ran.status.success(), "{}", ran.transcript());
    assert!(
        ran.stdout.contains("2 of 2 segments carry no sha256"),
        "the relaxed check must say what it did not check:\n{}",
        ran.transcript()
    );
}

/// `optional` relaxes the ABSENCE of a digest only: one that is present must
/// still match the bytes.
#[test]
fn optional_digests_still_refuse_a_digest_that_does_not_match() {
    let (key, _) = real_021_pair();
    let wrong = "0".repeat(64);
    let ran = manifest_check(
        Some("optional"),
        "e2e/fixtures/manifests/0.21.json",
        2000,
        &key,
        &wrong,
    );
    assert_eq!(ran.status.code(), Some(1), "{}", ran.transcript());
    assert!(ran.stderr.contains("!= manifest's"), "{}", ran.transcript());
}

#[test]
fn both_modes_refuse_a_count_the_broker_does_not_hold() {
    for mode in [None, Some("optional")] {
        let ran = manifest_check(mode, PRE_021_MANIFEST, 151, PRE_021_KEY, "00");
        assert_eq!(ran.status.code(), Some(1), "{mode:?}: {}", ran.transcript());
        assert!(
            ran.stderr
                .contains("manifest holds 150 records but the broker holds 151"),
            "{mode:?}: {}",
            ran.transcript()
        );
    }
}

/// The relaxed mode never refreshes the tracked fixtures, and a mode typo is
/// refused: both BEFORE the seed touches Docker.
#[test]
fn the_seed_refuses_a_bad_digest_mode_before_any_work() {
    for (mode, refresh, needle) in [
        (
            "optional",
            None,
            "LOGWEIR_SEED_SEGMENT_SHA256=optional needs LOGWEIR_SEED_REFRESH_FIXTURES=0",
        ),
        (
            "optional",
            Some("1"),
            "LOGWEIR_SEED_SEGMENT_SHA256=optional needs LOGWEIR_SEED_REFRESH_FIXTURES=0",
        ),
        ("sometimes", Some("0"), "must be `required` or `optional`"),
    ] {
        let mut env = vec![("LOGWEIR_SEED_SEGMENT_SHA256".to_string(), mode.to_string())];
        if let Some(refresh) = refresh {
            env.push(("LOGWEIR_SEED_REFRESH_FIXTURES".into(), refresh.into()));
        }
        let (ran, docker_ran) = seed_with(&env);
        assert_eq!(
            ran.status.code(),
            Some(1),
            "{mode}/{refresh:?}: {}",
            ran.transcript()
        );
        assert!(
            ran.stderr.contains(needle),
            "{mode}/{refresh:?}: {}",
            ran.transcript()
        );
        assert!(
            !docker_ran,
            "{mode}/{refresh:?}: docker ran before the mode was checked"
        );
    }
}

/// Runs `scripts/e2e-seed.sh` in a clean environment holding only `env`
/// (a stack is chosen by its variables, so an inherited slot must not leak in),
/// with a `docker` that records being called and fails. Returns the run and
/// whether docker ran.
fn seed_with(env: &[(String, String)]) -> (Ran, bool) {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("bin")).unwrap();
    let marker = dir.path().join("docker-was-called");
    write_executable(
        &dir.path().join("bin/docker"),
        &format!(
            "#!/usr/bin/env bash\ntouch '{}'\nexit 1\n",
            marker.display()
        ),
    );
    let mut cmd = Command::new("bash");
    cmd.arg(root().join("scripts/e2e-seed.sh"))
        .env_clear()
        .env("HOME", std::env::var("HOME").unwrap_or_default())
        .envs(env.iter().cloned());
    with_path(&mut cmd, &dir.path().join("bin"));
    let ran = run_bounded(cmd, 60);
    (ran, marker.exists())
}

/// `export K=V` lines of `e2e/compose/stack-env.sh --slot <n>`, PROD-01.5's
/// one source of a slot's variables.
fn slot_env(slot: u32) -> Vec<(String, String)> {
    let mut cmd = Command::new("bash");
    cmd.arg(root().join("e2e/compose/stack-env.sh"))
        .args(["--slot", &slot.to_string()]);
    let ran = run_bounded(cmd, 60);
    assert!(ran.status.success(), "{}", ran.transcript());
    ran.stdout
        .lines()
        .filter_map(|l| l.strip_prefix("export "))
        .filter_map(|kv| kv.split_once('='))
        .map(|(k, v)| {
            (
                k.to_string(),
                v.trim_matches('\'').trim_matches('"').to_string(),
            )
        })
        .collect()
}

/// PROD-01.5 made the fixture refresh default to 0 on a slot and refuse 1
/// there; the digest-mode guard tests that COMPUTED value, not the raw
/// variable (review L5 of PROD-00.1: a guard reading
/// `${LOGWEIR_SEED_REFRESH_FIXTURES:-1}` refused `optional` on every slot).
/// `optional` must pass both stacks' checks and reach the stack; each stack's
/// own refusal is the negative control.
#[test]
fn optional_digests_work_on_a_slot_and_on_the_default_stack() {
    const MODE_REFUSAL: &str = "optional needs LOGWEIR_SEED_REFRESH_FIXTURES=0";
    const SLOT_REFUSAL: &str = "refreshed from the DEFAULT stack only";
    let optional = (
        "LOGWEIR_SEED_SEGMENT_SHA256".to_string(),
        "optional".to_string(),
    );
    let refresh = |v: &str| ("LOGWEIR_SEED_REFRESH_FIXTURES".to_string(), v.to_string());
    let slot2 = slot_env(2);
    assert!(
        slot2
            .iter()
            .any(|(k, v)| k == "COMPOSE_PROJECT_NAME" && v == "logweir-e2e-s2"),
        "stack-env.sh --slot 2 names project logweir-e2e-s2: {slot2:?}"
    );
    let on_slot = |extra: Vec<(String, String)>| {
        let mut env = slot2.clone();
        env.extend(extra);
        env
    };
    // Passes the checks: the script goes on to the stack (docker, or the
    // `.env` precondition when this checkout has none).
    for (label, env) in [
        ("slot 2, refresh unset", on_slot(vec![optional.clone()])),
        (
            "slot 2, refresh 0",
            on_slot(vec![optional.clone(), refresh("0")]),
        ),
        (
            "default stack, refresh 0",
            vec![optional.clone(), refresh("0")],
        ),
    ] {
        let (ran, docker_ran) = seed_with(&env);
        assert!(
            !ran.stderr.contains(MODE_REFUSAL) && !ran.stderr.contains(SLOT_REFUSAL),
            "{label}: optional was refused: {}",
            ran.transcript()
        );
        assert!(
            docker_ran || ran.stderr.contains("e2e/compose/.env is missing"),
            "{label}: the seed did not get past its checks: {}",
            ran.transcript()
        );
    }
    // Refused, before docker: each stack's own rule.
    for (label, env, needle) in [
        (
            "default stack, refresh unset (defaults to 1)",
            vec![optional.clone()],
            MODE_REFUSAL,
        ),
        (
            "slot 2, refresh 1",
            on_slot(vec![optional.clone(), refresh("1")]),
            SLOT_REFUSAL,
        ),
    ] {
        let (ran, docker_ran) = seed_with(&env);
        assert_eq!(ran.status.code(), Some(1), "{label}: {}", ran.transcript());
        assert!(ran.stderr.contains(needle), "{label}: {}", ran.transcript());
        assert!(!docker_ran, "{label}: docker ran before the refusal");
    }
}

// ---------------------------------------------------------------------------
// 5. engine-matrix-rows.py
// ---------------------------------------------------------------------------

const BEGIN: &str = "<!-- engine-matrix:rows:begin -->";
const END: &str = "<!-- engine-matrix:rows:end -->";

fn row(tag: &str, kafka: &str, outcome: &str) -> String {
    format!(
        "| {tag} | {kafka} | `sha256:{}` | `{outcome}` | evidence for {tag} on {kafka} |\n",
        "a".repeat(64)
    )
}

fn render(doc: &str, rows: &[String], expect: Option<usize>) -> (Ran, String) {
    let dir = tempfile::tempdir().unwrap();
    let doc_path = dir.path().join("support-matrix.md");
    std::fs::write(&doc_path, doc).unwrap();
    let rows_dir = dir.path().join("rows/matrix-row-x");
    std::fs::create_dir_all(&rows_dir).unwrap();
    for (i, row) in rows.iter().enumerate() {
        std::fs::write(rows_dir.join(format!("{i}.row")), row).unwrap();
    }
    let mut cmd = Command::new("python3");
    cmd.arg(root().join("scripts/engine-matrix-rows.py"))
        .arg(&doc_path)
        .arg(dir.path().join("rows"))
        .args(["--note", "Generated by a test."]);
    if let Some(n) = expect {
        cmd.args(["--expect", &n.to_string()]);
    }
    let ran = run_bounded(cmd, 60);
    let after = std::fs::read_to_string(&doc_path).unwrap();
    (ran, after)
}

/// Against the REAL page: only the marked section changes, rows are ordered
/// newest engine first, and the hand-written `## Rows` table survives.
#[test]
fn the_renderer_rewrites_only_the_generated_section_of_the_real_page() {
    let doc = read("docs/support-matrix.md");
    assert_eq!(
        doc.matches(BEGIN).count(),
        1,
        "the page carries one begin marker"
    );
    assert_eq!(
        doc.matches(END).count(),
        1,
        "the page carries one end marker"
    );
    let rows = [
        row("v0.21.0", "3.7.1", "pass"),
        row("v0.22.0", "3.7.1", "pass"),
        row("v0.19.1", "3.7.1", "unsupported(lever-absent)"),
    ];
    let (ran, after) = render(&doc, &rows, Some(3));
    assert!(ran.status.success(), "{}", ran.transcript());
    let (before_head, before_rest) = doc.split_once(BEGIN).unwrap();
    let (_, before_tail) = before_rest.split_once(END).unwrap();
    let (after_head, after_rest) = after.split_once(BEGIN).unwrap();
    let (section, after_tail) = after_rest.split_once(END).unwrap();
    assert_eq!(before_head, after_head, "text above the markers changed");
    assert_eq!(before_tail, after_tail, "text below the markers changed");
    let v22 = section
        .find("| v0.22.0 |")
        .expect("the v0.22.0 row is rendered");
    let v21 = section
        .find("| v0.21.0 |")
        .expect("the v0.21.0 row is rendered");
    let v19 = section
        .find("| v0.19.1 |")
        .expect("the v0.19.1 row is rendered");
    assert!(v22 < v21 && v21 < v19, "newest engine first:\n{section}");
    assert!(
        section.contains("| Engine version | Kafka broker | Image digest | Outcome | Evidence |")
    );
    assert!(section.contains("Generated by a test."));
}

#[test]
fn the_renderer_refuses_and_writes_nothing_on_bad_input() {
    let doc = format!("# page\n\n{BEGIN}\nold\n{END}\n\ntail\n");
    let good = row("v0.22.0", "3.7.1", "pass");
    let cases: Vec<(&str, String, Vec<String>, Option<usize>)> = vec![
        (
            "no markers",
            "# page\n".to_string(),
            vec![good.clone()],
            None,
        ),
        ("no rows", doc.clone(), vec![], None),
        ("a row missing", doc.clone(), vec![good.clone()], Some(2)),
        (
            "a malformed row",
            doc.clone(),
            vec!["| v0.22.0 | 3.7.1 | not a digest | `pass` | x |\n".to_string()],
            None,
        ),
        (
            "an unknown outcome",
            doc.clone(),
            vec![row("v0.22.0", "3.7.1", "green")],
            None,
        ),
        (
            "a row recorded twice",
            doc.clone(),
            vec![good.clone(), good.clone()],
            None,
        ),
    ];
    for (label, page, rows, expect) in cases {
        let (ran, after) = render(&page, &rows, expect);
        assert_eq!(ran.status.code(), Some(1), "{label}: {}", ran.transcript());
        assert_eq!(
            after, page,
            "{label}: the page was written despite the refusal"
        );
    }
}

// ---------------------------------------------------------------------------
// 3, 4. The workflow itself
// ---------------------------------------------------------------------------

fn workflow(name: &str) -> Value {
    serde_yaml::from_str(&read(&format!(".github/workflows/{name}"))).unwrap()
}

fn steps(job: &Value) -> Vec<&Value> {
    job["steps"]
        .as_sequence()
        .expect("a job with steps")
        .iter()
        .collect()
}

/// Every non-comment line of every `run:` block in a job.
fn run_lines(job: &Value) -> Vec<String> {
    steps(job)
        .iter()
        .filter_map(|s| s["run"].as_str())
        .flat_map(|r| r.lines().map(str::trim).map(str::to_string))
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .collect()
}

fn matrix_rows() -> Vec<Value> {
    workflow("engine-matrix.yml")["jobs"]["matrix"]["strategy"]["matrix"]["include"]
        .as_sequence()
        .expect("declared rows")
        .clone()
}

fn version(text: &str) -> (u64, u64, u64) {
    let parts: Vec<u64> = text
        .trim_start_matches('v')
        .split('.')
        .map(|p| p.parse().unwrap())
        .collect();
    (parts[0], parts[1], parts[2])
}

/// The matrix sets the stack up, and runs the suite, exactly as the CI e2e job
/// does, so the two cannot drift apart again.
#[test]
fn the_matrix_runs_the_ci_e2e_setup_and_command() {
    let matrix = run_lines(&workflow("engine-matrix.yml")["jobs"]["matrix"]);
    let ci = run_lines(&workflow("ci.yml")["jobs"]["e2e"]);
    for needed in ["just e2e-up", "just e2e-down"] {
        assert!(
            matrix.iter().any(|l| l == needed),
            "engine-matrix does not run `{needed}`"
        );
    }
    let e2e_command = |lines: &[String]| {
        lines
            .iter()
            .find(|l| l.starts_with("cargo test --locked -p e2e --features e2e"))
            .cloned()
    };
    let ci_command = e2e_command(&ci).expect("the CI e2e job runs the e2e suite");
    assert_eq!(
        e2e_command(&matrix).as_deref(),
        Some(ci_command.as_str()),
        "the matrix's full drill must be the CI e2e job's command"
    );
}

/// A test the workflow names must exist, or its step is vacuous.
#[test]
fn every_test_the_matrix_names_exists() {
    let mut sources = String::new();
    for entry in std::fs::read_dir(root().join("e2e/tests")).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_some_and(|e| e == "rs") {
            sources.push_str(&std::fs::read_to_string(path).unwrap());
        }
    }
    let lines = run_lines(&workflow("engine-matrix.yml")["jobs"]["matrix"]);
    // The names end at the first shell token (a redirection, `||`, ...).
    let named: Vec<&str> = lines
        .iter()
        .filter_map(|l| l.split_once("run-named-tests.sh "))
        .flat_map(|(_, names)| {
            names.split_whitespace().take_while(|w| {
                w.chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
            })
        })
        .collect();
    assert!(
        named.len() >= 2,
        "the matrix names its reduced row and its control"
    );
    for name in named {
        assert!(
            sources.contains(&format!("fn {name}(")),
            "engine-matrix names `{name}`, which no e2e test defines"
        );
    }
}

/// `upload-artifact@v4` skips hidden files unless told otherwise; an upload
/// from a hidden path uploads nothing. Swept across every workflow.
#[test]
fn no_workflow_uploads_an_artifact_from_a_hidden_path() {
    for entry in std::fs::read_dir(root().join(".github/workflows")).unwrap() {
        let name = entry.unwrap().file_name().to_string_lossy().into_owned();
        let doc = workflow(&name);
        for (job_name, job) in doc["jobs"].as_mapping().unwrap() {
            // A job that calls a reusable workflow (`uses:`) has no steps of
            // its own; the called workflow is swept as a file of its own.
            if job["steps"].is_null() {
                continue;
            }
            for step in steps(job) {
                let uses = step["uses"].as_str().unwrap_or_default();
                if !uses.starts_with("actions/upload-artifact") {
                    continue;
                }
                let hidden_ok = step["with"]["include-hidden-files"].as_bool() == Some(true);
                let path = step["with"]["path"].as_str().unwrap_or_default();
                for p in path.lines().map(str::trim).filter(|p| !p.is_empty()) {
                    let hidden = p
                        .split('/')
                        .any(|seg| seg.starts_with('.') && seg != "." && seg != "..");
                    assert!(
                        !hidden || hidden_ok,
                        "{name} job {job_name:?} uploads `{p}`, a hidden path upload-artifact skips"
                    );
                }
            }
        }
    }
}

/// Rows at or above the documented full-drill floor run the full drill; rows
/// below it are declared `unsupported(lever-absent)`. The pinned tag has a
/// full row on the compose stack's default broker, and at least one row runs a
/// newer broker line than that default.
#[test]
fn the_declared_rows_follow_the_documented_floor() {
    let matrix_doc = read("docs/support-matrix.md");
    let floor = matrix_doc
        .lines()
        .find(|l| l.starts_with("| **Full-drill floor** |"))
        .and_then(|l| l.split("**").nth(3))
        .map(version)
        .expect("docs/support-matrix.md states the full-drill floor");
    let pin_tag = read("scripts/extract-engine.sh")
        .lines()
        .find_map(|l| l.strip_prefix("TAG=\"${OSO_TAG:-"))
        .and_then(|l| l.strip_suffix("}\""))
        .expect("extract-engine.sh names the pinned tag")
        .to_string();
    // The broker a row's `KAFKA_VERSION` selects is kafka-broker-1's image
    // (the container scripts/engine-matrix-broker.sh reads back). PROD-01.5
    // wrapped it in `${KAFKA_IMAGE:-…}`, a digest-pinned line that wins over
    // `KAFKA_VERSION` when set, so the matrix sets the one and never the other.
    let compose: Value = serde_yaml::from_str(&read("e2e/compose/docker-compose.yml")).unwrap();
    let image = compose["services"]["kafka-broker-1"]["image"]
        .as_str()
        .expect("kafka-broker-1 names its image");
    let default_broker = image
        .strip_prefix("${KAFKA_IMAGE:-")
        .and_then(|inner| inner.strip_suffix('}'))
        .unwrap_or(image)
        .strip_prefix("apache/kafka:${KAFKA_VERSION:-")
        .and_then(|v| v.strip_suffix('}'))
        .unwrap_or_else(|| {
            panic!("kafka-broker-1's image `{image}` is not chosen by KAFKA_VERSION")
        })
        .to_string();
    let job = &workflow("engine-matrix.yml")["jobs"]["matrix"];
    assert_eq!(
        job["env"]["KAFKA_VERSION"].as_str(),
        Some("${{ matrix.kafka }}"),
        "each row asks the stack for its broker through KAFKA_VERSION"
    );
    assert!(
        !serde_yaml::to_string(job).unwrap().contains("KAFKA_IMAGE"),
        "the matrix sets KAFKA_IMAGE, which would override every row's KAFKA_VERSION"
    );

    let rows = matrix_rows();
    let mut pin_on_default = false;
    let mut newer_broker = false;
    // PROD-00.3f: the C8 tripwire (the engine's fixed protocol versions on the
    // newest broker line) is a row for THE PIN, not for whichever engine
    // happened to carry it. Moving the pin without moving that row would leave
    // the newest broker exercised only by an engine Logweir no longer ships.
    let mut pin_on_newer_broker = false;
    for row in &rows {
        let tag = row["tag"].as_str().expect("tag");
        let kafka = row["kafka"].as_str().expect("kafka, as a string");
        let floor_kind = row["floor"].as_str().expect("floor");
        let expect = row["expect"].as_str().expect("expect");
        if version(tag) >= floor {
            assert_eq!(floor_kind, "full", "{tag} is at or above the floor");
            assert!(
                expect == "pass" || expect == "pass-degraded" || expect.starts_with("fail("),
                "{tag}: {expect}"
            );
        } else {
            assert_eq!(floor_kind, "below", "{tag} is below the floor");
            assert_eq!(expect, "unsupported(lever-absent)", "{tag}");
        }
        pin_on_default |= tag == pin_tag && kafka == default_broker;
        newer_broker |= version(kafka) > version(&default_broker);
        pin_on_newer_broker |= tag == pin_tag && version(kafka) > version(&default_broker);
    }
    assert!(
        pin_on_default,
        "no row runs the pin {pin_tag} on Kafka {default_broker}"
    );
    assert!(
        newer_broker,
        "no row runs a broker newer than {default_broker}"
    );
    assert!(
        pin_on_newer_broker,
        "no row runs the pin {pin_tag} on a broker newer than {default_broker} (the C8 tripwire)"
    );
}

/// The `.env` the matrix generates names the engine and never the broker; the
/// row's broker is the job's `KAFKA_VERSION` alone. The full drill runs
/// PROD-01.5's `a_slot_moves_every_host_port_and_the_default_render_does_not`,
/// which renders the stack with every stack variable removed and requires the
/// compose file's own default broker. A `.env` that carried the row's
/// `KAFKA_VERSION` turned that row red on the 4.3.1 row (reproduced locally
/// after the merge of PROD-01.5).
#[test]
fn the_generated_env_names_the_engine_and_leaves_the_broker_to_the_row() {
    let writes: Vec<String> = run_lines(&workflow("engine-matrix.yml")["jobs"]["matrix"])
        .into_iter()
        .filter(|l| l.contains("> e2e/compose/.env"))
        .collect();
    assert_eq!(
        writes.len(),
        1,
        "exactly one line writes e2e/compose/.env: {writes:?}"
    );
    assert!(
        writes[0].contains("OSO_DIGEST="),
        "the generated .env names the row's engine: {}",
        writes[0]
    );
    assert!(
        !writes[0].contains("KAFKA_VERSION") && !writes[0].contains("KAFKA_IMAGE"),
        "the generated .env pins the broker, which the default render then inherits: {}",
        writes[0]
    );
}

/// `publish` refuses a table with a row missing (its `--expect` is the number
/// of declared rows), is read-only, and a pull request is opt-in.
#[test]
fn publish_is_read_only_expects_every_row_and_the_pr_is_opt_in() {
    let doc = workflow("engine-matrix.yml");
    let publish = &doc["jobs"]["publish"];
    let rendered = run_lines(publish)
        .into_iter()
        .find(|l| l.contains("scripts/engine-matrix-rows.py"))
        .expect("publish renders through scripts/engine-matrix-rows.py");
    let expect: usize = rendered
        .split("--expect ")
        .nth(1)
        .and_then(|rest| rest.split_whitespace().next())
        .and_then(|n| n.parse().ok())
        .expect("publish passes --expect");
    assert_eq!(
        expect,
        matrix_rows().len(),
        "--expect must equal the declared rows"
    );
    assert!(
        publish["permissions"].is_null(),
        "publish inherits the workflow's read-only token"
    );
    let open_pr = &doc["jobs"]["open-pr"];
    assert!(
        open_pr["if"]
            .as_str()
            .is_some_and(|c| c.contains("vars.ENGINE_MATRIX_OPEN_PR == 'true'")),
        "opening a pull request is opt-in"
    );
}

/// The seed in a matrix row never rewrites the tracked fixtures.
#[test]
fn the_matrix_seed_never_refreshes_the_tracked_fixtures() {
    let job = &workflow("engine-matrix.yml")["jobs"]["matrix"];
    let seed = steps(job)
        .into_iter()
        .find(|s| s["run"].as_str() == Some("./scripts/e2e-seed.sh"))
        .expect("the matrix seeds the stack");
    assert_eq!(
        seed["env"]["LOGWEIR_SEED_REFRESH_FIXTURES"].as_str(),
        Some("0")
    );
}

// ---------------------------------------------------------------------------
// 6-10. The steps that decide whether a green job means anything (review M1,
// M2 and L6 of PROD-00.1)
// ---------------------------------------------------------------------------

const REFUSAL_LOG: &str = "engine: operational: engine 0.19.2 ignored the config key \
     `restore.header_preflight` that logweir rendered; this tag is below the declared floor";

/// The step outcomes of one row, as the "Record this row" step passes them.
#[derive(Clone)]
struct Steps {
    floor: &'static str,
    digest: &'static str,
    up: &'static str,
    declared: &'static str,
    measured: &'static str,
    seed: &'static str,
    build: &'static str,
    full: &'static str,
    reduced: &'static str,
    control: &'static str,
    reduced_log: Option<&'static str>,
    control_log: Option<&'static str>,
    retention_deletions: &'static str,
    retention_topics: &'static str,
}

impl Steps {
    /// A full row whose every step succeeded.
    fn full() -> Steps {
        Steps {
            floor: "full",
            digest: "sha256:8ff5be71f92a118cde64c082a86d188a4187d8f8f64311458081b8727e99c317",
            up: "success",
            declared: "3.7.1",
            measured: "3.7.1",
            seed: "success",
            build: "success",
            full: "success",
            reduced: "skipped",
            control: "success",
            reduced_log: None,
            control_log: None,
            retention_deletions: "0",
            retention_topics: "",
        }
    }

    /// A below-floor row where Logweir refused the engine in both drills.
    fn below() -> Steps {
        Steps {
            floor: "below",
            full: "skipped",
            reduced: "failure",
            control: "failure",
            reduced_log: Some(REFUSAL_LOG),
            control_log: Some(REFUSAL_LOG),
            ..Steps::full()
        }
    }

    /// The environment the Record step and the outcome script read, with the
    /// two transcripts written under `dir/matrix-logs/`.
    fn env(&self, dir: &Path) -> Vec<(String, String)> {
        let logs = dir.join("matrix-logs");
        std::fs::create_dir_all(&logs).unwrap();
        for (name, text) in [("reduced", self.reduced_log), ("control", self.control_log)] {
            if let Some(text) = text {
                std::fs::write(logs.join(format!("{name}.log")), text).unwrap();
            }
        }
        [
            ("FLOOR", self.floor),
            ("DIGEST", self.digest),
            ("UP", self.up),
            ("KAFKA_DECLARED", self.declared),
            ("BROKER_VERSION", self.measured),
            ("SEED", self.seed),
            ("BUILD", self.build),
            ("FULL", self.full),
            ("REDUCED", self.reduced),
            ("CONTROL", self.control),
            ("REDUCED_LOG", "matrix-logs/reduced.log"),
            ("CONTROL_LOG", "matrix-logs/control.log"),
            ("RETENTION_DELETIONS", self.retention_deletions),
            ("RETENTION_TOPICS", self.retention_topics),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
    }
}

/// `(outcome, reason)` from `scripts/engine-matrix-outcome.sh`, run in a clean
/// environment holding only `steps`.
fn classify(steps: &Steps) -> (String, String) {
    let dir = tempfile::tempdir().unwrap();
    let mut cmd = Command::new("bash");
    cmd.arg(root().join("scripts/engine-matrix-outcome.sh"))
        .current_dir(dir.path())
        .env_clear()
        .env("PATH", std::env::var("PATH").unwrap_or_default())
        .envs(steps.env(dir.path()));
    let ran = run_bounded(cmd, 60);
    assert!(ran.status.success(), "{}", ran.transcript());
    let field = |key: &str| {
        ran.stdout
            .lines()
            .find_map(|l| l.strip_prefix(&format!("{key}=")))
            .unwrap_or_else(|| panic!("no {key}= line: {}", ran.transcript()))
            .to_string()
    };
    (field("outcome"), field("reason"))
}

/// Review M1: the outcome follows what ran. A, B and C are the reviewer's
/// three combinations the old inline classifier got wrong.
#[test]
fn the_outcome_is_derived_from_what_every_step_did() {
    let s = Steps::full;
    let b = Steps::below;
    let cases: Vec<(&str, Steps, &str)> = vec![
        ("full row, every step green", s(), "pass"),
        (
            "full row, the suite failed",
            Steps {
                full: "failure",
                ..s()
            },
            "fail(e2e suite)",
        ),
        (
            "full row, the control failed",
            Steps {
                control: "failure",
                ..s()
            },
            "fail(lever-not-honoured)",
        ),
        (
            "A: the build failed, so the suite and the control were skipped",
            Steps {
                build: "failure",
                full: "skipped",
                control: "skipped",
                ..s()
            },
            "fail(build)",
        ),
        (
            "full row, the control was cancelled",
            Steps {
                control: "cancelled",
                ..s()
            },
            "fail(setup)",
        ),
        (
            "full row, the suite was skipped",
            Steps {
                full: "skipped",
                ..s()
            },
            "fail(setup)",
        ),
        (
            "full row, the seed failed",
            Steps {
                seed: "failure",
                full: "skipped",
                control: "skipped",
                ..s()
            },
            "fail(seed)",
        ),
        (
            "the tag did not resolve",
            Steps { digest: "", ..s() },
            "fail(setup)",
        ),
        (
            "the stack did not come up",
            Steps {
                up: "failure",
                ..s()
            },
            "fail(setup)",
        ),
        (
            "the broker was not read back",
            Steps {
                measured: "",
                ..s()
            },
            "fail(setup)",
        ),
        (
            "the stack ran another broker than declared",
            Steps {
                declared: "4.3.1",
                measured: "3.7.1",
                ..s()
            },
            "fail(setup)",
        ),
        (
            "below row, refused in both drills",
            b(),
            "unsupported(lever-absent)",
        ),
        (
            "B: below row, the seed failed",
            Steps {
                seed: "failure",
                reduced: "skipped",
                control: "skipped",
                ..b()
            },
            "fail(seed)",
        ),
        (
            "C: below row, both drills passed",
            Steps {
                reduced: "success",
                control: "success",
                ..b()
            },
            "fail(floor-not-enforced)",
        ),
        (
            "below row, the reduced row passed",
            Steps {
                reduced: "success",
                ..b()
            },
            "fail(floor-not-enforced)",
        ),
        (
            "below row, both failed without the floor refusal",
            Steps {
                reduced_log: Some("connection refused"),
                ..b()
            },
            "fail(floor-not-enforced)",
        ),
        (
            "below row, the control's transcript is missing",
            Steps {
                control_log: None,
                ..b()
            },
            "fail(floor-not-enforced)",
        ),
        (
            "below row, the control did not run",
            Steps {
                control: "skipped",
                ..b()
            },
            "fail(setup)",
        ),
        (
            "below row, the build failed",
            Steps {
                build: "failure",
                reduced: "skipped",
                control: "skipped",
                ..b()
            },
            "fail(build)",
        ),
        (
            "an unknown floor",
            Steps {
                floor: "sideways",
                ..s()
            },
            "fail(setup)",
        ),
    ];
    for (label, steps, want) in cases {
        let (got, reason) = classify(&steps);
        assert_eq!(got, want, "{label}: recorded `{got}` ({reason})");
        assert!(
            !reason.contains('|'),
            "{label}: a `|` would break the row: {reason}"
        );
    }
}

fn matrix_step<'a>(job: &'a Value, name: &str) -> (usize, &'a Value) {
    steps(job)
        .into_iter()
        .enumerate()
        .find(|(_, s)| s["name"].as_str() == Some(name))
        .unwrap_or_else(|| panic!("the matrix job has no step named {name:?}"))
}

fn step_by_id<'a>(job: &'a Value, id: &str) -> (usize, &'a Value) {
    steps(job)
        .into_iter()
        .enumerate()
        .find(|(_, s)| s["id"].as_str() == Some(id))
        .unwrap_or_else(|| panic!("the matrix job has no step with id {id:?}"))
}

/// Runs a step's `run:` text the way GitHub's default shell does (`bash -e`),
/// in `dir`, with `env`.
fn run_step_text(step: &Value, dir: &Path, env: &[(String, String)]) -> Ran {
    let script = dir.join("step.sh");
    std::fs::write(&script, step["run"].as_str().expect("a run: block")).unwrap();
    let mut cmd = Command::new("bash");
    cmd.arg("-e")
        .arg(&script)
        .current_dir(dir)
        .env_clear()
        .env("PATH", std::env::var("PATH").unwrap_or_default())
        .envs(env.iter().cloned());
    run_bounded(cmd, 60)
}

/// Review M2, R3: the Record step classifies through the outcome script, fed
/// by the steps that actually ran, and writes the row from its answer, with
/// the MEASURED broker in the Kafka column (L6).
#[test]
fn the_record_step_writes_the_row_the_steps_earned() {
    let doc = workflow("engine-matrix.yml");
    let job = &doc["jobs"]["matrix"];
    let (_, record) = matrix_step(job, "Record this row");
    assert_eq!(record["id"].as_str(), Some("record"));
    assert_eq!(record["if"].as_str(), Some("always()"));
    let wired = [
        ("TAG", "${{ matrix.tag }}"),
        ("FLOOR", "${{ matrix.floor }}"),
        ("KAFKA_DECLARED", "${{ matrix.kafka }}"),
        ("DIGEST", "${{ steps.pin.outputs.digest }}"),
        ("UP", "${{ steps.up.outcome }}"),
        ("BROKER_IMAGE", "${{ steps.broker.outputs.image }}"),
        ("BROKER_IMAGE_ID", "${{ steps.broker.outputs.image_id }}"),
        ("BROKER_VERSION", "${{ steps.broker.outputs.version }}"),
        ("SEED", "${{ steps.seed.outcome }}"),
        ("BUILD", "${{ steps.build.outcome }}"),
        ("FULL", "${{ steps.full.outcome }}"),
        ("REDUCED", "${{ steps.reduced.outcome }}"),
        ("CONTROL", "${{ steps.control.outcome }}"),
        ("REDUCED_LOG", "matrix-logs/reduced.log"),
        ("CONTROL_LOG", "matrix-logs/control.log"),
        (
            "RETENTION_DELETIONS",
            "${{ steps.retention.outputs.retention_deletions }}",
        ),
        (
            "RETENTION_TOPICS",
            "${{ steps.retention.outputs.retention_topics }}",
        ),
    ];
    for (key, value) in wired {
        assert_eq!(
            record["env"][key].as_str(),
            Some(value),
            "the Record step's {key} must be {value}"
        );
    }
    for id in [
        "pin",
        "up",
        "broker",
        "seed",
        "build",
        "full",
        "reduced",
        "control",
        "retention",
    ] {
        step_by_id(job, id);
    }

    let cases: Vec<(Steps, &str, &str)> = vec![
        (Steps::full(), "pass", "3.7.1"),
        (
            Steps {
                build: "failure",
                full: "skipped",
                control: "skipped",
                ..Steps::full()
            },
            "fail(build)",
            "3.7.1",
        ),
        (
            Steps {
                reduced: "success",
                control: "success",
                ..Steps::below()
            },
            "fail(floor-not-enforced)",
            "3.7.1",
        ),
        (Steps::below(), "unsupported(lever-absent)", "3.7.1"),
        (
            Steps {
                declared: "4.3.1",
                measured: "4.3.1",
                ..Steps::full()
            },
            "pass",
            "4.3.1",
        ),
        (
            Steps {
                measured: "",
                ..Steps::full()
            },
            "fail(setup)",
            "unmeasured",
        ),
    ];
    for (steps, want, kafka_column) in cases {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("scripts")).unwrap();
        std::fs::copy(
            root().join("scripts/engine-matrix-outcome.sh"),
            dir.path().join("scripts/engine-matrix-outcome.sh"),
        )
        .unwrap();
        let output = dir.path().join("github-output");
        let mut env = steps.env(dir.path());
        env.extend(
            [
                ("TAG", "v0.21.0"),
                ("BROKER_IMAGE", "apache/kafka:4.3.1"),
                (
                    "BROKER_IMAGE_ID",
                    "sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837",
                ),
                ("RUN_URL", "https://github.com/o/r/actions/runs/1"),
            ]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string())),
        );
        env.push(("GITHUB_OUTPUT".into(), output.display().to_string()));
        let ran = run_step_text(record, dir.path(), &env);
        assert!(ran.status.success(), "{want}: {}", ran.transcript());
        let row_file = dir
            .path()
            .join(format!("matrix-rows/v0.21.0-kafka-{}.row", steps.declared));
        let row = std::fs::read_to_string(&row_file).unwrap_or_else(|e| panic!("{want}: {e}"));
        assert!(
            row.starts_with(&format!("| v0.21.0 | {kafka_column} | `")),
            "{want}: the Kafka column must be the measured broker: {row}"
        );
        assert!(row.contains(&format!("| `{want}` |")), "{want}: {row}");
        assert_eq!(
            std::fs::read_to_string(&output).unwrap(),
            format!("outcome={want}\n")
        );
        if !steps.measured.is_empty() {
            assert!(
                row.contains(&format!("logged Kafka {}", steps.measured)),
                "the evidence names the measured broker: {row}"
            );
        }
    }
}

/// Review M2, R2: the verdict step exists, always runs after the Record step,
/// compares what the row recorded with what it declares, and fails the job on
/// a mismatch. Every test step is `continue-on-error`, so without it every row
/// would be green whatever it recorded.
#[test]
fn the_verdict_step_fails_a_row_that_records_other_than_it_declares() {
    let doc = workflow("engine-matrix.yml");
    let job = &doc["jobs"]["matrix"];
    let (record_at, _) = step_by_id(job, "record");
    let (verdict_at, verdict) = steps(job)
        .into_iter()
        .enumerate()
        .find(|(_, s)| {
            s["env"]["GOT"].as_str() == Some("${{ steps.record.outputs.outcome }}")
                && s["env"]["WANT"].as_str() == Some("${{ matrix.expect }}")
        })
        .expect("a step compares steps.record.outputs.outcome with matrix.expect");
    assert!(
        verdict_at > record_at,
        "the verdict runs after the row is recorded"
    );
    assert_eq!(verdict["if"].as_str(), Some("always()"));
    assert!(
        verdict["continue-on-error"].is_null(),
        "the verdict may not be softened"
    );
    for (got, want, ok) in [
        ("pass", "pass", true),
        (
            "unsupported(lever-absent)",
            "unsupported(lever-absent)",
            true,
        ),
        ("fail(e2e suite)", "pass", false),
        ("unsupported(lever-absent)", "pass", false),
        ("", "pass", false),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let env = [
            ("GOT", got),
            ("WANT", want),
            ("TAG", "v0.21.0"),
            ("KAFKA_VERSION", "3.7.1"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect::<Vec<_>>();
        let ran = run_step_text(verdict, dir.path(), &env);
        assert_eq!(
            ran.status.success(),
            ok,
            "recorded {got:?}, declared {want:?}: {}",
            ran.transcript()
        );
    }
}

/// Review M2, R4: below the floor the seed accepts segments without a digest,
/// at or above it the seed requires them.
#[test]
fn the_seed_digest_mode_follows_the_row_floor() {
    let job = &workflow("engine-matrix.yml")["jobs"]["matrix"];
    let (_, seed) = step_by_id(job, "seed");
    assert_eq!(seed["run"].as_str(), Some("./scripts/e2e-seed.sh"));
    assert_eq!(
        seed["env"]["LOGWEIR_SEED_SEGMENT_SHA256"].as_str(),
        Some("${{ matrix.floor == 'full' && 'required' || 'optional' }}")
    );
}

/// Review M2, R6: a pull request is opened only from `main`, and only when the
/// repository opts in.
#[test]
fn a_pull_request_is_opened_only_from_main_and_only_when_opted_in() {
    let open_pr = &workflow("engine-matrix.yml")["jobs"]["open-pr"];
    let condition = open_pr["if"].as_str().expect("open-pr is conditional");
    assert_eq!(
        condition,
        "github.ref == 'refs/heads/main' && vars.ENGINE_MATRIX_OPEN_PR == 'true'"
    );
}

/// The reduced row and the control keep their transcripts for the classifier
/// and do not mask their own exit status.
#[test]
fn the_drill_steps_keep_their_transcript_and_their_exit_status() {
    let job = &workflow("engine-matrix.yml")["jobs"]["matrix"];
    for (id, log) in [
        ("reduced", "matrix-logs/reduced.log"),
        ("control", "matrix-logs/control.log"),
    ] {
        let (_, step) = step_by_id(job, id);
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("scripts")).unwrap();
        write_executable(
            &dir.path().join("scripts/run-named-tests.sh"),
            &format!("#!/usr/bin/env bash\necho '{REFUSAL_LOG}'\nexit 101\n"),
        );
        let ran = run_step_text(step, dir.path(), &[]);
        assert_eq!(
            ran.status.code(),
            Some(101),
            "{id}: the step must exit with the test runner's status: {}",
            ran.transcript()
        );
        let kept = std::fs::read_to_string(dir.path().join(log)).unwrap_or_default();
        assert!(
            kept.contains("below the declared floor"),
            "{id}: transcript not kept"
        );
        assert!(
            ran.stdout.contains("below the declared floor"),
            "{id}: transcript not shown"
        );
    }
}

/// A `docker` that answers the three questions the readback asks, with the
/// broker's log given by `log`.
fn fake_docker(log: &str) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("bin")).unwrap();
    std::fs::write(dir.path().join("broker.log"), log).unwrap();
    write_executable(
        &dir.path().join("bin/docker"),
        &format!(
            r#"#!/usr/bin/env bash
case "$*" in
  "compose -f e2e/compose/docker-compose.yml ps -q kafka-broker-1") echo cid-broker ;;
  "inspect --format {{{{.Config.Image}}}} cid-broker") echo apache/kafka:4.3.1 ;;
  "inspect --format {{{{.Image}}}} cid-broker") echo sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837 ;;
  "logs cid-broker") cat '{}' ;;
  *) echo "unexpected docker $*" >&2; exit 2 ;;
esac
"#,
            dir.path().join("broker.log").display()
        ),
    );
    dir
}

fn broker_readback(fake: &tempfile::TempDir) -> Ran {
    broker_readback_with(fake, &[])
}

fn broker_readback_with(fake: &tempfile::TempDir, args: &[&str]) -> Ran {
    let mut cmd = Command::new("bash");
    cmd.arg(root().join("scripts/engine-matrix-broker.sh"))
        .args(args);
    with_path(&mut cmd, &fake.path().join("bin"));
    run_bounded(cmd, 60)
}

/// Review L6: the row records the broker the stack actually runs.
#[test]
fn the_broker_is_read_back_from_the_running_container() {
    let fake = fake_docker(
        "[2026-09-29 06:12:36,205] INFO Kafka version: 4.3.1 (org.apache.kafka.common.utils.AppInfoParser)\n\
         [2026-09-29 06:12:36,206] INFO Kafka commitId: 26b251a451ce941d (org.apache.kafka.common.utils.AppInfoParser)\n\
         [2026-09-29 06:12:37,001] INFO Kafka version: 4.3.1 (org.apache.kafka.common.utils.AppInfoParser)\n",
    );
    let ran = broker_readback(&fake);
    assert!(ran.status.success(), "{}", ran.transcript());
    assert_eq!(
        ran.stdout,
        "image=apache/kafka:4.3.1\n\
         image_id=sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837\n\
         version=4.3.1\n"
    );
    // A broker that logged no version is not a measurement.
    let silent = fake_docker("[2026-09-29 06:12:36,205] INFO starting\n");
    let ran = broker_readback(&silent);
    assert_eq!(ran.status.code(), Some(1), "{}", ran.transcript());
    assert!(
        ran.stdout.is_empty(),
        "nothing may be reported: {}",
        ran.stdout
    );
    assert!(ran.stderr.contains("no 'INFO Kafka version:' line"));

    // The workflow reads it back after the stack is up and feeds the Record
    // step with it (wiring pinned in the_record_step_writes_the_row_the_steps_earned).
    let job = &workflow("engine-matrix.yml")["jobs"]["matrix"];
    let (up_at, _) = step_by_id(job, "up");
    let (broker_at, broker) = step_by_id(job, "broker");
    let (seed_at, _) = step_by_id(job, "seed");
    assert!(up_at < broker_at && broker_at < seed_at);
    assert_eq!(broker["if"].as_str(), Some("steps.up.outcome == 'success'"));
    assert!(broker["run"].as_str().is_some_and(|r| r
        .contains("./scripts/engine-matrix-broker.sh")
        && r.contains("\"$GITHUB_OUTPUT\"")));
}

/// Run 36542777892: a fixture stamped older than the broker's retention lost
/// its records to the broker's time-retention check between its produce and
/// the engine's capture. The row reads back what that check deleted.
#[test]
fn the_brokers_retention_deletions_are_read_back() {
    // The three retention lines run 36542777892's broker logged, among the
    // lines a broker also logs for other deletions: the log start moving,
    // a topic deleted by the test's cleanup, the files removed later.
    const CI_LINES: &str = "\
[2026-09-29 09:07:21,152] INFO [LocalLog partition=other-topic-1, dir=/tmp/kafka-logs] Deleting segments as the log has been deleted: LogSegment(baseOffset=0, size=121, lastModifiedTime=1790672745429, largestRecordTimestamp=1760000000030) (kafka.log.LocalLog)
[2026-09-29 09:07:21,164] INFO [LocalLog partition=other-topic-1, dir=/tmp/kafka-logs] Deleting segment files LogSegment(baseOffset=0, size=121, lastModifiedTime=1790672745429, largestRecordTimestamp=1760000000030) (kafka.log.LocalLog$)
[2026-09-29 08:39:55,756] INFO [UnifiedLog partition=recsem-1463674564-shapes-0, dir=/tmp/kafka-logs] Incremented log start offset to 8 due to segment deletion (kafka.log.UnifiedLog)
[2026-09-29 08:39:55,757] INFO [UnifiedLog partition=recsem-1463674564-shapes-0, dir=/tmp/kafka-logs] Deleting segment LogSegment(baseOffset=0, size=253, lastModifiedTime=1790671194338, largestRecordTimestamp=1760000000070) due to log retention time 604800000ms breach based on the largest record timestamp in the segment (kafka.log.UnifiedLog)
[2026-09-29 08:39:55,759] INFO [UnifiedLog partition=recsem-1463674564-shapes-1, dir=/tmp/kafka-logs] Deleting segment LogSegment(baseOffset=0, size=121, lastModifiedTime=1790671194337, largestRecordTimestamp=1760000000030) due to log retention time 604800000ms breach based on the largest record timestamp in the segment (kafka.log.UnifiedLog)
[2026-09-29 08:39:55,760] INFO [UnifiedLog partition=recsem-1463674564-shapes-2, dir=/tmp/kafka-logs] Deleting segment LogSegment(baseOffset=0, size=154, lastModifiedTime=1790671194337, largestRecordTimestamp=1760000000500) due to log retention time 604800000ms breach based on the largest record timestamp in the segment (kafka.log.UnifiedLog)
";
    let ran = broker_readback_with(&fake_docker(CI_LINES), &["--retention"]);
    assert!(ran.status.success(), "{}", ran.transcript());
    assert_eq!(
        ran.stdout,
        "retention_deletions=3\nretention_topics=recsem-1463674564-shapes\n"
    );

    // Many topics: five are named and the rest counted, so the row stays one line.
    let many: String = (1..=7)
        .map(|i| {
            format!(
                "[t] INFO [UnifiedLog partition=t{i}-0, dir=/d] Deleting segment LogSegment(baseOffset=0) \
                 due to log retention time 604800000ms breach based on the largest record timestamp \
                 in the segment (kafka.log.UnifiedLog)\n"
            )
        })
        .collect();
    let ran = broker_readback_with(&fake_docker(&many), &["--retention"]);
    assert!(ran.status.success(), "{}", ran.transcript());
    assert_eq!(
        ran.stdout,
        "retention_deletions=7\nretention_topics=t1 t2 t3 t4 t5 (+2 more)\n"
    );

    // No deletion is a measurement of zero, not a failure.
    let ran = broker_readback_with(
        &fake_docker("[t] INFO Kafka version: 3.7.1 (x)\n"),
        &["--retention"],
    );
    assert!(ran.status.success(), "{}", ran.transcript());
    assert_eq!(ran.stdout, "retention_deletions=0\nretention_topics=\n");

    // An unknown option is refused before docker runs.
    let ran = broker_readback_with(&fake_docker(""), &["--retain"]);
    assert_eq!(ran.status.code(), Some(1), "{}", ran.transcript());
    assert!(ran.stdout.is_empty(), "{}", ran.stdout);

    // Read after every drill, before the row is recorded, and never fatal.
    let job = &workflow("engine-matrix.yml")["jobs"]["matrix"];
    let (retention_at, retention) = step_by_id(job, "retention");
    for id in ["full", "reduced", "control"] {
        let (at, _) = step_by_id(job, id);
        assert!(
            at < retention_at,
            "{id} must run before the retention readback"
        );
    }
    let (record_at, _) = step_by_id(job, "record");
    assert!(
        retention_at < record_at,
        "the readback feeds the Record step"
    );
    assert_eq!(
        retention["if"].as_str(),
        Some("always() && steps.up.outcome == 'success'")
    );
    assert_eq!(retention["continue-on-error"].as_bool(), Some(true));
    assert!(retention["run"].as_str().is_some_and(|r| r
        .contains("./scripts/engine-matrix-broker.sh --retention")
        && r.contains("\"$GITHUB_OUTPUT\"")));
}

/// What the broker's retention deleted is named in a failed suite's reason,
/// and never changes an outcome: the suite's failure stays a failure.
#[test]
fn a_failed_suite_names_what_the_brokers_retention_deleted() {
    let failed = |deletions: &'static str, topics: &'static str| Steps {
        full: "failure",
        retention_deletions: deletions,
        retention_topics: topics,
        ..Steps::full()
    };
    let (outcome, reason) = classify(&failed("3", "recsem-1463674564-shapes"));
    assert_eq!(outcome, "fail(e2e suite)");
    assert!(
        reason.contains("time-retention check deleted 3 segment(s) of recsem-1463674564-shapes"),
        "{reason}"
    );
    assert!(!reason.contains('|'), "a row is one table line: {reason}");
    for (deletions, why) in [
        ("0", "none deleted"),
        ("", "not read back"),
        ("3x", "not a count"),
    ] {
        let (outcome, reason) = classify(&failed(deletions, "t"));
        assert_eq!(outcome, "fail(e2e suite)", "{why}");
        assert_eq!(
            reason, "the full e2e suite failed; see the job log",
            "{why}"
        );
    }
    let deleted = |s: Steps| Steps {
        retention_deletions: "3",
        retention_topics: "t",
        ..s
    };
    for (steps, want) in [
        (deleted(Steps::full()), "pass"),
        (deleted(Steps::below()), "unsupported(lever-absent)"),
        (
            deleted(Steps {
                control: "failure",
                ..Steps::full()
            }),
            "fail(lever-not-honoured)",
        ),
    ] {
        let (got, reason) = classify(&steps);
        assert_eq!(got, want, "{reason}");
    }
}

/// A row whose broker could not be read back still renders, as `unmeasured`.
#[test]
fn the_renderer_accepts_an_unmeasured_broker() {
    let doc = format!("# page\n\n{BEGIN}\nold\n{END}\n");
    let rows = [
        row("v0.21.0", "unmeasured", "fail(setup)"),
        row("v0.21.0", "3.7.1", "pass"),
    ];
    let (ran, after) = render(&doc, &rows, Some(2));
    assert!(ran.status.success(), "{}", ran.transcript());
    assert!(after.contains("| v0.21.0 | unmeasured |"), "{after}");
}
