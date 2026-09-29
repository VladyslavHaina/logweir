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
            .env("LOGWEIR_SEED_SEGMENT_SHA256", mode)
            .env_remove("LOGWEIR_SEED_REFRESH_FIXTURES");
        if let Some(refresh) = refresh {
            cmd.env("LOGWEIR_SEED_REFRESH_FIXTURES", refresh);
        }
        with_path(&mut cmd, &dir.path().join("bin"));
        let ran = run_bounded(cmd, 60);
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
            !marker.exists(),
            "{mode}/{refresh:?}: docker ran before the mode was checked"
        );
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
    let named: Vec<&str> = lines
        .iter()
        .filter_map(|l| l.split_once("run-named-tests.sh "))
        .flat_map(|(_, names)| names.split_whitespace())
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
    let compose = read("e2e/compose/docker-compose.yml");
    let default_broker = compose
        .split("apache/kafka:${KAFKA_VERSION:-")
        .nth(1)
        .and_then(|rest| rest.split('}').next())
        .expect("the compose stack names its default broker")
        .to_string();

    let rows = matrix_rows();
    let mut pin_on_default = false;
    let mut newer_broker = false;
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
    }
    assert!(
        pin_on_default,
        "no row runs the pin {pin_tag} on Kafka {default_broker}"
    );
    assert!(
        newer_broker,
        "no row runs a broker newer than {default_broker}"
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
