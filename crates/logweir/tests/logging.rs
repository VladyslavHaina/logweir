//! T0-10: at the SHIPPED default log level — `RUST_LOG` absent from the
//! environment entirely — every terminal path of `drill run` must put a
//! non-empty `run_id` on a machine-parseable stdout line, and `RUST_LOG` must
//! still win when it is set.
//!
//! Both halves are load-bearing, and neither alone is the fix.
//!
//! `crates/logweir/src/drill/mod.rs` carried the comment "Spec §13: structured
//! JSON logs on stdout with run_id on every line" nine lines above
//! `.with_env_filter(EnvFilter::from_default_env())`, whose default level is
//! ERROR when `RUST_LOG` is unset. The span carrying the id is an INFO span, so
//! at ERROR it is never entered and renders nothing, and `exiting()`'s INFO
//! line — which its own doc comment calls "the one place [the exit-code]
//! distinction survives into a log aggregator" — was not emitted at all. A
//! CronJob failing operationally at 03:17 on a Monday left an operator a bare
//! error string with nothing to correlate against the bucket, the metrics
//! textfile, or a sibling pod.
//!
//! WHERE `run_id` LIVES IN THE JSON. `tracing_subscriber`'s JSON formatter
//! nests an event's own fields under `"fields"` and renders the current span as
//! a sibling object `"span":{"run_id":…,"name":"drill"}`. T0-10's fix text says
//! "add `run_id` as a field on the error event itself", and that is
//! `fields.run_id` — as opposed to `span.run_id`, which is what it excludes.
//! These tests therefore assert `fields.run_id`, NOT a root-level `run_id`:
//! a root-level key is unreachable without `.flatten_event(true)`, which would
//! change the shape of every log line the product emits.
//!
//! Every test here spawns the real binary against paths that do not exist. No
//! broker, no engine binary, no cluster, no Docker, no network (GC17, GR2).

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

/// Consumed verbatim from `crates/logweir/tests/cli_exit_codes.rs`.
fn bin() -> Command {
    Command::new(env!("CARGO_BIN_EXE_logweir"))
}

/// A `drill run` invocation that fails operationally (exit 1) before it can
/// reach a broker, the engine binary or a cluster: the spec is read first, and
/// it is not there.
///
/// The paths are built under a fresh `tempfile::tempdir()` and the directory is
/// then dropped, so nothing absolute about this host is baked into the test and
/// the paths are guaranteed absent.
struct FailingRun {
    argv: Vec<String>,
    /// Kept alive for the tests that need a WRITABLE directory (the metrics
    /// ones); `None` for the plain ones, whose temp dir is gone by construction.
    dir: Option<tempfile::TempDir>,
}

impl FailingRun {
    fn new() -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let base = dir.path().to_path_buf();
        // Dropped here on purpose: every path below is now absent.
        drop(dir);
        Self {
            argv: Self::argv(&base),
            dir: None,
        }
    }

    /// Same invocation, but the temp dir stays alive so a caller can point
    /// `--metrics-file` somewhere real. The five `--*-key`/spec paths are still
    /// absent, because nothing creates them.
    fn with_live_dir() -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let argv = Self::argv(&dir.path().join("absent"));
        Self {
            argv,
            dir: Some(dir),
        }
    }

    fn argv(base: &Path) -> Vec<String> {
        let p = |n: &str| base.join(n).to_string_lossy().into_owned();
        vec![
            "drill".into(),
            "run".into(),
            format!("--spec={}", p("spec.yaml")),
            format!("--approval={}", p("a.json")),
            format!("--approver-key={}", p("k.pem")),
            format!("--allowed-clusters={}", p("c.json")),
            format!("--signing-key={}", p("s.pem")),
        ]
    }

    fn dir(&self) -> &Path {
        self.dir.as_ref().expect("live dir").path()
    }

    fn arg(mut self, a: String) -> Self {
        self.argv.push(a);
        self
    }

    /// Runs with `RUST_LOG` REMOVED from the child's environment — the shipped
    /// default configuration, and the case this task exists for. `env_remove`
    /// rather than `env("RUST_LOG", "")`: an inherited value from the developer
    /// shell or from CI would otherwise decide the answer.
    fn unset(&self) -> Output {
        bin()
            .env_remove("RUST_LOG")
            .args(&self.argv)
            .output()
            .expect("spawn logweir")
    }

    fn with_rust_log(&self, v: &str) -> Output {
        bin()
            .env("RUST_LOG", v)
            .args(&self.argv)
            .output()
            .expect("spawn logweir")
    }
}

/// Every stdout line that parses as JSON. Parsed with `serde_json`, never
/// substring-matched: a substring hit would pass on `run_id` appearing anywhere
/// at any nesting level, which is precisely the distinction under test.
fn stdout_json(out: &Output) -> Vec<serde_json::Value> {
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .collect()
}

fn rendered(out: &Output) -> String {
    format!(
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

fn repo_root() -> PathBuf {
    // CARGO_MANIFEST_DIR is `crates/logweir`.
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

// ---------------------------------------------------------------------------
// The five tests T0-10 names.
// ---------------------------------------------------------------------------

#[test]
fn error_event_carries_run_id_with_rust_log_unset() {
    let r = FailingRun::new();
    let out = r.unset();
    assert_eq!(
        out.status.code(),
        Some(1),
        "a missing spec is an operational failure: {}",
        rendered(&out)
    );
    let lines = stdout_json(&out);
    let err_line = lines
        .iter()
        .find(|v| v["level"] == "ERROR")
        .unwrap_or_else(|| {
            panic!(
                "an ERROR JSON line on stdout with RUST_LOG unset: {}",
                rendered(&out)
            )
        });
    assert!(
        err_line["fields"]["run_id"]
            .as_str()
            .is_some_and(|s| !s.is_empty()),
        "the ERROR event itself must carry a non-empty run_id, not only its span: {err_line}"
    );
}

#[test]
fn default_level_is_info_when_rust_log_is_unset() {
    let r = FailingRun::new();
    let out = r.unset();
    assert_eq!(out.status.code(), Some(1), "{}", rendered(&out));
    let lines = stdout_json(&out);
    let info = lines
        .iter()
        .find(|v| v["level"] == "INFO" && v["fields"]["message"] == "drill finished")
        .unwrap_or_else(|| {
            panic!(
                "`exiting()`'s INFO line must be visible at the shipped default level: {}",
                rendered(&out)
            )
        });
    assert_eq!(info["fields"]["exit_code"], 1, "{info}");
    assert!(
        info["fields"]["run_id"]
            .as_str()
            .is_some_and(|s| !s.is_empty()),
        "{info}"
    );
}

#[test]
fn rust_log_still_overrides_the_default() {
    let r = FailingRun::new();
    let out = r.with_rust_log("warn");
    assert_eq!(out.status.code(), Some(1), "{}", rendered(&out));
    let lines = stdout_json(&out);
    assert!(
        !lines.iter().any(|v| v["level"] == "INFO"),
        "RUST_LOG=warn must suppress the INFO lines; the default must not be hard-coded: {}",
        rendered(&out)
    );
    assert!(
        lines.iter().any(|v| v["level"] == "ERROR"
            && v["fields"]["run_id"]
                .as_str()
                .is_some_and(|s| !s.is_empty())),
        "the error line and its run_id survive at WARN: {}",
        rendered(&out)
    );
}

#[test]
fn rust_log_set_but_empty_uses_the_default() {
    // `env: - name: RUST_LOG` with an empty `value:` is a Kubernetes-manifest
    // reality. An empty string must be treated as unset, not as "the empty
    // directive set", whose own default level is ERROR.
    let r = FailingRun::new();
    let out = r.with_rust_log("");
    assert_eq!(out.status.code(), Some(1), "{}", rendered(&out));
    let lines = stdout_json(&out);
    let info = lines
        .iter()
        .find(|v| v["level"] == "INFO" && v["fields"]["message"] == "drill finished")
        .unwrap_or_else(|| panic!("an empty RUST_LOG is unset, not ERROR: {}", rendered(&out)));
    assert_eq!(info["fields"]["exit_code"], 1, "{info}");
    assert!(
        info["fields"]["run_id"]
            .as_str()
            .is_some_and(|s| !s.is_empty()),
        "{info}"
    );
}

#[test]
fn cronjob_manifest_sets_rust_log() {
    let p = repo_root().join("examples/cronjob-drill.yaml");
    let t = std::fs::read_to_string(&p).unwrap_or_else(|e| panic!("{}: {e}", p.display()));
    // `grep -c` counts matching LINES; `str::matches().count()` counts
    // OCCURRENCES. This is the stricter of the two — two occurrences on one
    // line would satisfy the grep and still fail here.
    assert_eq!(
        t.matches("RUST_LOG").count(),
        1,
        "examples/cronjob-drill.yaml must set RUST_LOG exactly once, with the reason in a comment"
    );
    assert!(
        t.contains("name: RUST_LOG"),
        "it must be a container env var, not only a comment"
    );
}

// ---------------------------------------------------------------------------
// Additions (addendum A7.3). None needs a broker, an engine binary, a cluster
// or Docker, and none needs a new dependency.
// ---------------------------------------------------------------------------

#[test]
fn a_malformed_rust_log_does_not_panic_and_still_reaches_a_terminal_path() {
    // The filter is now computed from `RUST_LOG` by hand, so a directive string
    // that does not parse must not take the process down before the drill can
    // report. `EnvFilter::new` is `parse_lossy` (VERIFIED
    // tracing-subscriber-0.3.23 `filter/env/mod.rs:350` and
    // `filter/env/builder.rs:146`), so an unparseable directive is dropped with
    // a note on stderr rather than unwrapped; a future edit that reaches for
    // `try_new(..).unwrap()` instead is what this test refuses.
    let r = FailingRun::new();
    let out = r.with_rust_log("this is not a filter directive!!");
    assert_eq!(
        out.status.code(),
        Some(1),
        "a malformed RUST_LOG must not change the exit code, and must not panic: {}",
        rendered(&out)
    );
    assert!(
        !String::from_utf8_lossy(&out.stderr).contains("panicked at"),
        "a malformed RUST_LOG must not panic: {}",
        rendered(&out)
    );
}

#[test]
fn whitespace_only_rust_log_uses_the_default() {
    // The sibling of the empty-string case: a manifest with `value: " "`.
    let r = FailingRun::new();
    let out = r.with_rust_log("   ");
    assert_eq!(out.status.code(), Some(1), "{}", rendered(&out));
    assert!(
        stdout_json(&out)
            .iter()
            .any(|v| v["level"] == "INFO" && v["fields"]["message"] == "drill finished"),
        "a blank RUST_LOG is unset: {}",
        rendered(&out)
    );
}

#[test]
fn every_default_level_line_carries_the_run_id_on_the_event_or_on_its_span() {
    // The property in full, not just on the two lines the named tests pick out:
    // at the shipped default, EVERY JSON line the process emits is
    // correlatable. A line qualifies either by carrying `fields.run_id` (the
    // error event, `exiting()`, `PhaseLogger`'s three) or by rendering the
    // entered `drill` span, which `.with_current_span(true)` puts at
    // `span.run_id` — and which only exists at all because the default level is
    // now INFO, since the span itself is an INFO span.
    //
    // `--metrics-file` points inside a directory that does not exist, so the
    // textfile write fails and `publish()` emits its WARN line. That line
    // carries NO `run_id` field of its own, so it is the one that proves the
    // span half. GC11: the failed write does not move the exit code.
    let r = FailingRun::with_live_dir();
    let metrics = r.dir().join("no-such-dir").join("logweir.prom");
    let r = r.arg(format!("--metrics-file={}", metrics.display()));
    let out = r.unset();
    assert_eq!(
        out.status.code(),
        Some(1),
        "a failed metrics write never moves the exit code (GC11): {}",
        rendered(&out)
    );
    let lines = stdout_json(&out);
    assert!(
        lines.len() >= 3,
        "expected the error line, the metrics WARN line and `drill finished`: {}",
        rendered(&out)
    );
    let ids: Vec<&str> = lines
        .iter()
        .map(|v| {
            v["fields"]["run_id"]
                .as_str()
                .or_else(|| v["span"]["run_id"].as_str())
                .unwrap_or_else(|| panic!("a stdout line with no run id anywhere: {v}"))
        })
        .collect();
    assert!(
        ids.iter().all(|id| !id.is_empty() && *id == ids[0]),
        "every line must carry the SAME non-empty run id: {ids:?}"
    );
    assert!(
        lines
            .iter()
            .any(|v| v["level"] == "WARN" && v["fields"]["run_id"].is_null()),
        "the metrics WARN line is the one with no run_id field of its own; \
         without it this test does not exercise the span half: {}",
        rendered(&out)
    );
}

#[test]
fn the_metrics_record_and_the_log_carry_the_same_run_id() {
    // Cross-stream correlation, which is the whole point: the id an operator
    // greps out of the log aggregator is the id in the Prometheus textfile
    // node_exporter scraped off the same pod. The textfile carries it as a
    // leading COMMENT, never as a label (cardinality) — that shape is not this
    // task's and is not changed here, only asserted against the log.
    let r = FailingRun::with_live_dir();
    let metrics = r.dir().join("logweir.prom");
    let arg = format!("--metrics-file={}", metrics.display());
    let r = r.arg(arg);
    let out = r.unset();
    assert_eq!(out.status.code(), Some(1), "{}", rendered(&out));
    let body = std::fs::read_to_string(&metrics)
        .unwrap_or_else(|e| panic!("{}: {e}\n{}", metrics.display(), rendered(&out)));
    let from_file = body
        .lines()
        .find_map(|l| l.strip_prefix("# logweir run_id="))
        .unwrap_or_else(|| panic!("no run id comment in the textfile:\n{body}"))
        .to_string();
    let from_log = stdout_json(&out)
        .iter()
        .find_map(|v| {
            (v["level"] == "ERROR")
                .then(|| v["fields"]["run_id"].as_str().map(str::to_string))
                .flatten()
        })
        .unwrap_or_else(|| panic!("no run id on the ERROR line: {}", rendered(&out)));
    assert!(!from_file.is_empty(), "the textfile's run id is empty");
    assert_eq!(
        from_file, from_log,
        "the metrics textfile and the log must name the SAME run"
    );
}

// ---------------------------------------------------------------------------
// The engine subprocess's captured output, and the phase lines.
//
// These are the streams a run only produces against a live broker and the real
// engine binary, so they cannot be exercised by spawning the binary against
// absent paths. What they need from this task is the DEFAULT LEVEL: the lines
// are emitted at INFO by `logweir::metrics::PhaseLogger`, which the
// orchestrator constructs with the run id and hands to phase 6, and the engine
// child is never told the run id (GC3: its argv is exactly `restore` /
// `validate-restore` / `validation run` and their flags — the parent
// re-emits the child's captured lines under its own identity).
//
// So the test drives `PhaseLogger` directly under a subscriber built at the
// two levels that matter: the one this task now defaults to, and the one it
// replaced.
// ---------------------------------------------------------------------------

/// A thread-local `tracing` sink, so this never touches the global subscriber.
#[derive(Clone, Default)]
struct Captured(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

impl Captured {
    fn json(&self) -> Vec<serde_json::Value> {
        String::from_utf8_lossy(&self.0.lock().expect("buffer"))
            .lines()
            .filter_map(|l| serde_json::from_str(l).ok())
            .collect()
    }
}

impl std::io::Write for Captured {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().expect("buffer").extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Captured {
    type Writer = Captured;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

fn observe(directive: &str) -> Vec<serde_json::Value> {
    use logweir_core::engine::PhaseObserver;
    let logs = Captured::default();
    let subscriber = tracing_subscriber::fmt()
        .json()
        .with_writer(logs.clone())
        .with_env_filter(tracing_subscriber::EnvFilter::new(directive))
        .finish();
    tracing::subscriber::with_default(subscriber, || {
        let mut obs = logweir::metrics::PhaseLogger::new("01TESTRUNIDFORTHELOGGING1");
        obs.phase_started(1, "approval");
        obs.phase_finished(1, "ok");
        obs.engine_line("stdout", "Restoring topic orders-restored partition 0");
    });
    logs.json()
}

#[test]
fn the_engine_output_and_phase_lines_carry_the_run_id_at_the_default_level() {
    let lines = observe("info");
    let messages: Vec<&str> = lines
        .iter()
        .filter_map(|v| v["fields"]["message"].as_str())
        .collect();
    assert_eq!(
        messages,
        vec!["phase started", "phase finished", "engine output"],
        "all three PhaseObserver lines must be visible at the default level: {lines:?}"
    );
    for v in &lines {
        assert_eq!(
            v["fields"]["run_id"].as_str(),
            Some("01TESTRUNIDFORTHELOGGING1"),
            "every observer line carries the run id on the EVENT, so a single-line \
             consumer of the engine's captured output can correlate it: {v}"
        );
    }
    // The engine's own line is reproduced verbatim beside the id, on the
    // stream it came from, so the child never needs to know the run id.
    let engine = lines
        .iter()
        .find(|v| v["fields"]["message"] == "engine output")
        .expect("an engine output line");
    assert_eq!(engine["fields"]["stream"], "stdout", "{engine}");
    assert_eq!(
        engine["fields"]["line"], "Restoring topic orders-restored partition 0",
        "{engine}"
    );
}

#[test]
fn the_old_default_level_hid_every_one_of_those_lines() {
    // `EnvFilter::from_default_env()` with RUST_LOG unset was ERROR
    // (tracing-subscriber-0.3.23 `filter/env/mod.rs:289-293`). This pins WHY
    // the level change is the fix and not a cosmetic default: at ERROR the
    // engine's captured output and both phase lines do not exist at all, so no
    // amount of `run_id`-on-the-event work would have made them correlatable.
    assert!(
        observe("error").is_empty(),
        "these lines are INFO; at ERROR there is nothing to put a run id on"
    );
}
