//! A drill that leaves scratch topics on the operator's cluster must
//! SAY SO, on every channel an operator actually watches, on the exact run that
//! left them, and without changing the exit code.
//!
//! Phase 9's attestation is honest: `phase9_teardown::run` puts every topic
//! the broker refused into `topics_failed` and never lists it in
//! `topics_deleted`. This file holds what an operator can SEE: a drill that
//! left scratch topics on a production broker must not print the same stdout
//! line or write the same Prometheus textfile as one that cleaned up.
//!
//! The three channels are one test each, plus the two that hold the boundary:
//! a clean run's console line stays byte-identical, and the exit code does not
//! move in either direction (stage-2 ruling R-C).
mod fixtures;
mod support;

use fixtures::Drill;
use logweir::drill::{execute_with, phase9_teardown, summary_line};
use logweir_core::outcome::Outcome;
use logweir_kafka::reader::{KafkaError, TopicDeleter};
use support::doubles::Deleter;

/// The name the fixture spec maps `orders` onto (`topic_mapping_prefix:
/// "drill-"`), and therefore the one scratch topic a fixture drill creates.
const SCRATCH_TOPIC: &str = "drill-orders";
/// The broker's refusal, verbatim, as `Drill::LeavesATopicBehind` reports it.
const REFUSAL: &str = "BROKER: TOPIC_DELETION_DISABLED";

// ---------------------------------------------------------------- log capture

// Where the rendered JSON goes, per THREAD. `None` on a thread that is not
// capturing, and every line written there is discarded.
//
// Not `tracing::subscriber::with_default`: that installs a THREAD-LOCAL
// dispatcher, but `tracing` caches each callsite's `Interest` and the global
// maximum level PROCESS-wide, so a callsite first reached on a thread with no
// dispatcher caches `Interest::never` and the parallel drills in this binary
// poison it. So the subscriber is installed ONCE, GLOBALLY (below; that
// rebuilds the interest cache), and the per-thread routing here keeps the
// other tests' events out of this one's buffer. `drill::run` installs a
// global subscriber too.
thread_local! {
    static SINK: std::cell::RefCell<Option<std::sync::Arc<std::sync::Mutex<Vec<u8>>>>> =
        const { std::cell::RefCell::new(None) };
}

/// A `MakeWriter` that keeps every JSON line the subscriber renders *on the
/// capturing thread*, so a test can assert on what an operator's log
/// aggregator would receive.
#[derive(Clone, Copy, Default)]
struct Captured;

impl std::io::Write for Captured {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        SINK.with(|s| {
            if let Some(sink) = s.borrow().as_ref() {
                sink.lock().expect("buffer").extend_from_slice(buf);
            }
        });
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Captured {
    type Writer = Captured;
    fn make_writer(&'a self) -> Self::Writer {
        *self
    }
}

/// Runs `body` under the binary's one JSON subscriber, at `info`, and returns
/// the lines `body`'s own thread emitted.
///
/// `info` is what `DEFAULT_LOG_DIRECTIVE` pins Logweir's own targets at, so a
/// line visible here is a line visible to an operator who set no `RUST_LOG` at
/// all: the warning must not need a raised level to be seen.
fn under_a_capturing_subscriber(body: impl FnOnce()) -> Vec<serde_json::Value> {
    static INSTALLED: std::sync::OnceLock<()> = std::sync::OnceLock::new();
    INSTALLED.get_or_init(|| {
        let subscriber = tracing_subscriber::fmt()
            .json()
            .with_writer(Captured)
            .with_env_filter(tracing_subscriber::EnvFilter::new("info"))
            .finish();
        tracing::subscriber::set_global_default(subscriber)
            .expect("no other test in this binary installs a global subscriber");
    });

    let sink = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    SINK.with(|s| *s.borrow_mut() = Some(sink.clone()));
    body();
    SINK.with(|s| *s.borrow_mut() = None);

    let buf = sink.lock().expect("buffer");
    String::from_utf8_lossy(&buf)
        .lines()
        .filter_map(|l| serde_json::from_str(l).ok())
        .collect()
}

/// A deleter that refuses exactly the one scratch topic, so an attestation
/// identical to the fixture drill's can be built without running a drill.
/// Deliberately the PARTIAL failure (`Ok(vec![(name, Err(..))])`), which is
/// `phase9_teardown::run`'s per-topic branch — not `score.rs`'s refusing
/// deleter, whose whole call fails and exercises the other branch.
fn refuses_the_one_topic() -> Deleter {
    Deleter::default().refusing(SCRATCH_TOPIC, REFUSAL)
}

// ---------------------------------------------------------------- the metric

/// Channel 1 of 3: the Prometheus textfile.
///
/// The assertion is on the WHOLE line including the trailing ` 1`, never on the
/// metric name alone: a name-only assertion cannot tell 1 from 0 and would
/// survive a writer that emits the gauge and always reports zero.
#[test]
fn metrics_textfile_reports_teardown_failures() {
    let f = fixtures::orchestrator_fixture(Drill::LeavesATopicBehind);
    let sc = execute_with(&f.args, &f.run_id, &f.ctx)
        .expect("a refused DELETION is not a failed drill: the restore itself passed");
    logweir::metrics::write_textfile(&f.metrics, &sc).expect("the textfile is written");
    let text = std::fs::read_to_string(&f.metrics).expect("the textfile is readable");
    let want = format!(
        "logweir_drill_teardown_topics_failed{{cluster=\"{}\"}} 1\n",
        sc.target.cluster_id
    );
    assert!(
        text.contains(&want),
        "a drill whose broker refused `{SCRATCH_TOPIC}` wrote a textfile that does not say so. \
         Expected the line {want:?}. A dashboard over this file renders green while the scratch \
         topic is still on the operator's cluster.\nwritten:\n{text}"
    );
}

/// The series is emitted UNCONDITIONALLY, `0` included, and that is what makes
/// it safe to alert on with `> 0`: to PromQL an absent series and a clean
/// teardown are the same thing, so absence has to keep meaning "no drill
/// result was produced" and never "the teardown was clean".
#[test]
fn metrics_textfile_reports_zero_teardown_failures_on_a_clean_drill() {
    let f = fixtures::orchestrator_fixture(Drill::Passes);
    let sc = execute_with(&f.args, &f.run_id, &f.ctx).expect("the fixture drill passes");
    logweir::metrics::write_textfile(&f.metrics, &sc).expect("the textfile is written");
    let text = std::fs::read_to_string(&f.metrics).expect("the textfile is readable");
    let want = format!(
        "logweir_drill_teardown_topics_failed{{cluster=\"{}\"}} 0\n",
        sc.target.cluster_id
    );
    assert!(
        text.contains(&want),
        "a clean teardown must still emit the series, at 0. Expected {want:?}; a series that \
         appears only when it is non-zero cannot be alerted on, because an absent series and a \
         clean run are indistinguishable.\nwritten:\n{text}"
    );
}

// ---------------------------------------------------------------- the log line

/// Channel 2 of 3: the log.
///
/// The assertion is on the topic NAME, not on a count. An operator who reads
/// "teardown failed for 1 topic" still has to go and find out which one, on a
/// cluster whose topic list is not theirs to guess at — and the count is
/// already the metric's job.
#[test]
fn teardown_failure_names_the_topics() {
    let f = fixtures::orchestrator_fixture(Drill::LeavesATopicBehind);
    let lines = under_a_capturing_subscriber(|| {
        execute_with(&f.args, &f.run_id, &f.ctx).expect("a refused deletion is not a failed drill");
    });

    let warnings: Vec<&serde_json::Value> = lines
        .iter()
        .filter(|v| {
            v["level"] == "WARN"
                && v["fields"]["message"]
                    .as_str()
                    .is_some_and(|m| m.contains(SCRATCH_TOPIC))
        })
        .collect();
    assert_eq!(
        warnings.len(),
        1,
        "exactly one WARN naming `{SCRATCH_TOPIC}` is owed — no warning at all is the T0-11 \
         defect, and two is a duplicate an operator will learn to ignore.\nlines: {lines:#?}"
    );
    let warning = warnings[0];

    // The names travel in a FIELD as well as in the message: a JSON-line
    // consumer reads `fields.topics`, a plain-text consumer reads the message,
    // and neither may be the only carrier.
    assert_eq!(
        warning["fields"]["topics"].as_str(),
        Some(SCRATCH_TOPIC),
        "the failed topic names must be a field on the EVENT, not only inside the rendered \
         message: {warning}"
    );

    // The run id is on the EVENT, not
    // only on the entered span. A warning an operator cannot correlate to a
    // run is a warning nobody can act on, and a single-line consumer
    // (`jq '.fields.run_id'`) never reads the span object.
    assert_eq!(
        warning["fields"]["run_id"].as_str(),
        Some(f.run_id.as_str()),
        "the leftover warning must carry the run id as a FIELD on the event: {warning}"
    );

    // The count travels as a FIELD too (`fields.count`): a claimed field
    // nobody asserts is a field that can vanish. One refused topic ⇒ count 1.
    assert_eq!(
        warning["fields"]["count"].as_u64(),
        Some(1),
        "the leftover warning must carry the failed-topic count as a FIELD on the event: {warning}"
    );

    // The emitted message and the pure function that produces it can never
    // drift: the function is testable without a subscriber, the event is
    // testable with one, and these are asserted to be the same string.
    let att = phase9_teardown::run(
        &refuses_the_one_topic(),
        &fixtures::mapping("orders", SCRATCH_TOPIC),
        "delete",
        logweir_core::spec::TargetMode::Scratch,
        "RUN",
        "SC",
    );
    assert_eq!(
        warning["fields"]["message"].as_str(),
        Some(
            phase9_teardown::teardown_warning(&att)
                .expect("an attestation with one failure has a warning")
                .as_str()
        ),
        "the WARN the orchestrator emits and `teardown_warning`'s message must be one string, or \
         a reader of either has no reason to trust the other: {warning}"
    );
}

// ---------------------------------------------------------------- the console line

/// Channel 3 of 3: the one line `drill run` prints on stdout.
#[test]
fn summary_line_reports_teardown_failures() {
    let f = fixtures::orchestrator_fixture(Drill::LeavesATopicBehind);
    let sc = execute_with(&f.args, &f.run_id, &f.ctx).expect("a refused deletion is not a failure");
    let line = summary_line(&sc);
    assert!(
        line.contains("teardown left 1 scratch topic behind"),
        "the console line is the only output an operator running `drill run` by hand is \
         guaranteed to read: {line}"
    );
    assert!(
        line.contains(SCRATCH_TOPIC),
        "and it names the topic, not just the count: {line}"
    );
}

/// The no-regression half, and it is load-bearing: the clause is CONDITIONAL,
/// so a clean run's console line is byte-identical to the one every existing
/// test asserts on (`orchestrator.rs`'s
/// `the_stdout_line_quotes_the_signed_artifact_never_the_in_memory_copy`). An
/// unconditional suffix would change every clean run's output, which is churn a
/// reviewer cannot tell from a regression.
#[test]
fn summary_line_is_unchanged_when_teardown_is_clean() {
    let f = fixtures::orchestrator_fixture(Drill::Passes);
    let sc = execute_with(&f.args, &f.run_id, &f.ctx).expect("the fixture drill passes");
    let line = summary_line(&sc);
    assert!(
        !line.contains("teardown left"),
        "a clean drill's summary line must not grow a teardown clause: {line}"
    );
}

// ---------------------------------------------------------------- R-C

/// **This test encodes a RULING, not an accident.** Stage-2 ruling R-C, verbatim:
///
/// > **R-C (T0-11 scope).** The gauge, the `warn!`, the `summary_line()` count
/// > and the deletion of the false doc sentence are scheduled. Making
/// > `phase9_teardown::persist`'s failure propagate to exit 4 is a behaviour
/// > change after phase 8 has signed and uploaded; it is **deferred pending a
/// > ruling** and no task may make it. Task 13.
///
/// So the exit code must not move in EITHER direction. `report` is private,
/// so the code is asserted through the one public path that carries it:
/// `execute_with` returning `Ok`, which `report`'s `Ok(_) => ExitCode::Ok` arm
/// turns into exit 0, and `DrillError::exit_code` never being reached.
#[test]
fn teardown_failure_does_not_change_the_exit_code() {
    let f = fixtures::orchestrator_fixture(Drill::LeavesATopicBehind);

    // (a) No `DrillError` is produced, so no non-zero code can be derived from
    //     one. A failed DELETION is not a failed DRILL.
    let sc = match execute_with(&f.args, &f.run_id, &f.ctx) {
        Ok(sc) => sc,
        Err(e) => {
            let rendered = e.to_string();
            panic!(
                "R-C: a teardown that left a scratch topic behind produced a DrillError, which \
                 `DrillError::exit_code` turns into exit {}. Phase 8 has already signed and \
                 uploaded by the time phase 9 runs; escalating here is the behaviour change R-C \
                 defers. error: {rendered}",
                logweir::exit::ExitCode::from(e) as u8
            )
        }
    };

    // (b) …and the outcome is the one `report` maps to `ExitCode::Ok`.
    assert_eq!(
        sc.outcome,
        Outcome::Pass,
        "R-C: the drill itself passed; a residue on the cluster is not a drill result"
    );

    // (c) …and the failure was RECORDED rather than escalated: an "ok" phase
    //     record with empty notes is silence, and a non-"ok" one is the
    //     escalation R-C forbids.
    let p9 = sc
        .phases
        .iter()
        .find(|p| p.phase == 9)
        .expect("phase 9 ran");
    assert_eq!(
        p9.outcome, "ok",
        "R-C: phase 9's record must stay `ok` — a refused deletion is not a phase failure: {p9:?}"
    );
    assert!(
        !p9.notes.is_empty(),
        "the failure has to be recorded somewhere the metric and the summary line can read it; \
         an empty note list on an `ok` phase 9 is exactly the silence T0-11 names: {p9:?}"
    );
}

/// R-C in its OTHER direction: propagating `phase9_teardown::persist`'s `Err`
/// at the call site changes nothing on a fixture where `persist` SUCCEEDS, so
/// this one makes `persist` actually fail.
///
/// It fails the way it would in production: the evidence store's puts are
/// create-only (Global Constraint 6's segregation), so an object already
/// sitting at the teardown key makes both puts refuse. Squatting the key before
/// the run needs no fixture shape, no `#[cfg]` hook in `src/` and no second
/// store — the run id is known before `execute_with` is called, because the
/// caller mints it.
///
/// What must hold: the drill still returns `Ok`, phase 9's record still reads
/// `ok`, and the operator is nonetheless TOLD. The attestation's absence from
/// the bucket is the durable signal (`docs/stability.md`), and the warning is
/// the only live one.
#[test]
fn a_teardown_attestation_that_cannot_be_persisted_does_not_change_the_exit_code() {
    let f = fixtures::orchestrator_fixture(Drill::Passes);
    f.ctx
        .store
        .put_create_only(
            &format!("logweir/drills/{}.teardown.json", f.run_id),
            b"an object is already here",
        )
        .expect("the key is free before the drill runs");

    let lines = under_a_capturing_subscriber(|| {
        let sc = execute_with(&f.args, &f.run_id, &f.ctx).expect(
            "R-C: a teardown attestation that could not be persisted is not a failed drill",
        );
        assert_eq!(
            sc.outcome,
            Outcome::Pass,
            "R-C: phase 8 signed and uploaded the drill result before phase 9 ran"
        );
        let p9 = sc
            .phases
            .iter()
            .find(|p| p.phase == 9)
            .expect("phase 9 ran");
        assert_eq!(
            p9.outcome, "ok",
            "R-C: a failed teardown attestation is a WARNING at the call site, never an \
             outcome: {p9:?}"
        );
    });

    let warned = lines.iter().any(|v| {
        v["level"] == "WARN"
            && v["fields"]["message"]
                .as_str()
                .is_some_and(|m| m.contains("teardown attestation not persisted"))
    });
    assert!(
        warned,
        "the exit code is unchanged, so the WARN is the only live signal that the teardown \
         document never reached the bucket. Losing it makes the failure silent.\nlines: {lines:#?}"
    );
}

// ------------------------------------------- what a clean phase 9 looks like

/// The answer to "what does an operator see when phase 9 has NOTHING to
/// report", and the caller-side test for `PhaseObserver`.
///
/// On a clean teardown the gauge reads `0`, the summary line is unchanged, and
/// the engine child — spawned with `RUST_LOG=warn` pinned — contributes nothing
/// to the stream. What is left is phase 9's own `phase started` / `phase
/// finished` pair, and that pair is the whole of it: without it a clean phase 9
/// would be indistinguishable in the log from a phase 9 that never ran.
///
/// It keeps `PhaseObserver::phase_started` / `phase_finished` wired from the
/// orchestrator: deleting the two calls from `record` fails the closed
/// arithmetic below (EVERY phase record has a pair, not just phase 9).
#[test]
fn every_phase_the_orchestrator_runs_says_so_including_a_clean_teardown() {
    let f = fixtures::orchestrator_fixture(Drill::Passes);
    let mut scorecard = None;
    let lines = under_a_capturing_subscriber(|| {
        scorecard =
            Some(execute_with(&f.args, &f.run_id, &f.ctx).expect("the fixture drill passes"));
    });
    let sc = scorecard.expect("the drill produced a scorecard");

    let pairs = |message: &str| -> Vec<i64> {
        lines
            .iter()
            .filter(|v| {
                v["level"] == "INFO"
                    && v["fields"]["message"].as_str() == Some(message)
                    && v["fields"]["run_id"].as_str() == Some(f.run_id.as_str())
            })
            .filter_map(|v| v["fields"]["phase"].as_i64())
            .collect()
    };
    let started = pairs("phase started");
    let finished = pairs("phase finished");

    for p in &sc.phases {
        let n = i64::from(p.phase);
        assert!(
            started.contains(&n),
            "phase {n} ({}) is in the scorecard and said nothing when it started. The observer \
             hooks carry the run id and are the only per-phase progress an operator has; \
             unwired, they are live code with no caller.\nstarted: {started:?}",
            p.name
        );
        assert!(
            finished.contains(&n),
            "phase {n} ({}) is in the scorecard and said nothing when it finished.\nfinished: \
             {finished:?}",
            p.name
        );
    }
    assert!(
        started.contains(&9) && finished.contains(&9),
        "phase 9 above all: on a CLEAN teardown these two lines are the entire log record that \
         it ran. started: {started:?}, finished: {finished:?}"
    );

    // …and nothing more. A clean teardown says nothing special anywhere: no
    // WARN, and (asserted in its own test) no clause on the summary line.
    assert!(
        !lines.iter().any(|v| v["level"] == "WARN"
            && v["fields"]["message"]
                .as_str()
                .is_some_and(|m| m.contains("teardown left"))),
        "a clean teardown must not warn about leftovers.\nlines: {lines:#?}"
    );
}

// ---------------------------------------------------------------- the false sentence

/// Cleanup failure is a warning after the outcome has been signed, so the
/// function's documentation must not promise a different process exit code.
#[test]
fn teardown_docs_do_not_promise_a_false_exit_code() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let source = std::fs::read_to_string(root.join("crates/logweir/src/drill/phase9_teardown.rs"))
        .expect("phase9_teardown.rs is checked in");
    assert!(!source.contains("exit 4 rather than a silent success"));
}

// ---------------------------------------------------------------- the dashboard

/// The panel exists, and nothing that existed before this task disappeared.
#[test]
fn dashboard_has_a_teardown_panel() {
    // The panel ids that must not disappear, from:
    //   python3 -c "import json;print(sorted(p['id'] for p in json.load(open('dashboards/logweir.json'))['panels']))"
    const IDS_BEFORE_TASK_13: &[i64] = &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];

    let text = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../dashboards/logweir.json"
    ))
    .expect("dashboards/logweir.json is checked in");
    let doc: serde_json::Value = serde_json::from_str(&text).expect("the dashboard is JSON");
    let panels = doc["panels"].as_array().expect("panels is an array");

    assert!(
        panels.iter().any(|p| p["targets"][0]["expr"]
            == "logweir_drill_teardown_topics_failed{cluster=~\"$cluster\"}"),
        "no panel queries the teardown gauge, so the one channel an operator watches without \
         being asked to renders nothing at all"
    );

    let ids: Vec<i64> = panels
        .iter()
        .map(|p| p["id"].as_i64().expect("every panel has an integer id"))
        .collect();
    for want in IDS_BEFORE_TASK_13 {
        assert!(
            ids.contains(want),
            "panel id {want} existed before this task and is gone; this task adds panels and \
             deletes none. ids now: {ids:?}"
        );
    }
    let mut sorted = ids.clone();
    sorted.sort_unstable();
    let before = sorted.len();
    sorted.dedup();
    assert_eq!(
        sorted.len(),
        before,
        "duplicate panel ids in dashboards/logweir.json: {ids:?}"
    );
}

// ---------------------------------------------------------------- a failed run's leftovers

/// The broker's per-topic answer to a delete the principal may not make.
const AUTHZ: &str = "TopicAuthorizationFailed (Broker: Topic authorization failed)";

/// A deleter that records every name it is handed and deletes it.
#[derive(Default)]
struct Recording(std::sync::Mutex<Vec<String>>);
impl TopicDeleter for Recording {
    fn delete_topics(
        &self,
        names: &[String],
    ) -> Result<Vec<(String, Result<(), String>)>, KafkaError> {
        self.0.lock().unwrap().extend_from_slice(names);
        Ok(names.iter().map(|n| (n.clone(), Ok(()))).collect())
    }
}

/// A run that fails after its creation step created
/// `drill-orders` (the engine's restore fails, exit 1, nothing signed) removes
/// the topic and says so. Control: a passing drill deletes it in phase 9, once,
/// as a pass does. KILLS: the failure-path teardown skipped.
#[test]
fn a_run_that_fails_after_creating_its_topic_removes_it_and_says_so() {
    let f = fixtures::orchestrator_fixture(Drill::FailsAfterCreation);
    let mut result = None;
    let lines = under_a_capturing_subscriber(|| {
        result = Some(execute_with(&f.args, &f.run_id, &f.ctx));
    });
    let err = result.unwrap().expect_err("the engine's restore failed");
    assert_eq!(logweir::exit::ExitCode::from(err) as u8, 1);
    assert_eq!(*f.deleted_topics.lock().unwrap(), vec![SCRATCH_TOPIC]);
    assert!(
        lines.iter().any(|v| v["level"] == "INFO"
            && v["fields"]["message"]
                == "the restore failed after it created its target topics; teardown removed 1 \
                    scratch topic: drill-orders"),
        "{lines:#?}"
    );
    assert!(
        !lines.iter().any(|v| v["level"] == "WARN"
            && v["fields"]["message"]
                .as_str()
                .is_some_and(|m| m.contains("teardown left"))),
        "nothing was left: {lines:#?}"
    );

    let f = fixtures::orchestrator_fixture(Drill::Passes);
    execute_with(&f.args, &f.run_id, &f.ctx).expect("the control passes");
    assert_eq!(*f.deleted_topics.lock().unwrap(), vec![SCRATCH_TOPIC]);
}

/// **At phase 7.** The restore ran and phase 7 cannot read the
/// target back: exit 1, nothing signed, and the topic is removed. KILLS: the
/// phase-7 error return not reaching the failure teardown.
#[test]
fn a_run_that_fails_at_verify_removes_its_topic() {
    let f = fixtures::orchestrator_fixture(Drill::FailsAtVerify);
    let err = execute_with(&f.args, &f.run_id, &f.ctx).expect_err("phase 7 cannot read");
    assert!(
        !matches!(err, logweir::drill::DrillError::NotPass(..)),
        "a failure, not a signed result: {err}"
    );
    assert_eq!(logweir::exit::ExitCode::from(err) as u8, 1);
    assert_eq!(*f.deleted_topics.lock().unwrap(), vec![SCRATCH_TOPIC]);
}

/// The same failed run, and the broker refuses the
/// deletion for authorization: the WARN names the topic AND the grant. KILLS:
/// the leftover not named; the grant not named.
#[test]
fn a_failed_run_that_cannot_delete_names_the_topic_and_the_grant() {
    let f = fixtures::orchestrator_fixture(Drill::FailsAfterCreationAndCannotDelete);
    let mut result = None;
    let lines = under_a_capturing_subscriber(|| {
        result = Some(execute_with(&f.args, &f.run_id, &f.ctx));
    });
    assert!(result.unwrap().is_err());
    assert_eq!(*f.deleted_topics.lock().unwrap(), vec![SCRATCH_TOPIC]);
    let warning = lines
        .iter()
        .find(|v| v["level"] == "WARN" && v["fields"]["topics"] == SCRATCH_TOPIC)
        .unwrap_or_else(|| panic!("no WARN naming the topic: {lines:#?}"));
    assert_eq!(
        warning["fields"]["message"],
        "teardown left 1 scratch topic behind on the target cluster: drill-orders; the restore \
         principal lacks `Delete` on topic `drill-orders`",
        "{warning}"
    );
    assert_eq!(warning["fields"]["run_id"], f.run_id.as_str());
}

/// Only the topics THIS run created are handed to the
/// deleter: a mapped name it did not create (one that existed before it,
/// which phase 0 refuses today) is never deleted. With nothing created,
/// nothing is attempted. KILLS: tearing down a topic the run did not create.
#[test]
fn a_failed_run_deletes_only_the_topics_it_created() {
    let mapping = std::collections::BTreeMap::from([
        ("orders".to_string(), "drill-orders".to_string()),
        ("payments".to_string(), "drill-payments".to_string()),
    ]);
    let scratch = logweir_core::spec::TargetMode::Scratch;
    let deleter = Recording::default();
    let mut out = Vec::new();
    let (deleted, failed) = phase9_teardown::after_failure(
        &deleter,
        &mapping,
        &["drill-orders".to_string()],
        "delete",
        scratch,
        "RUN",
        &mut out,
    );
    assert_eq!(*deleter.0.lock().unwrap(), vec!["drill-orders"]);
    assert_eq!(deleted, vec!["drill-orders"]);
    assert!(failed.is_empty());
    assert!(out.is_empty(), "nothing was left, so no line");

    let deleter = Recording::default();
    let (deleted, failed) = phase9_teardown::after_failure(
        &deleter,
        &mapping,
        &[],
        "delete",
        scratch,
        "RUN",
        &mut std::io::sink(),
    );
    assert!(deleter.0.lock().unwrap().is_empty());
    assert!(deleted.is_empty() && failed.is_empty());
}

/// **A failed run's leftover is printed as the `teardown-failed=`
/// line** the controller copies to `Restore.status.teardown.failed`, with the
/// grant. Control: the run in the row above, which removed its topic, wrote
/// no line. KILLS: the line not printed on the failure path.
#[test]
fn a_failed_run_prints_what_it_left_for_the_controller() {
    let mut out = Vec::new();
    // Every name refused for authorization, as a broker does for a
    // principal without `Delete`.
    let (deleted, failed) = phase9_teardown::after_failure(
        &Deleter::default().refusing_all(AUTHZ),
        &fixtures::mapping("orders", SCRATCH_TOPIC),
        &[SCRATCH_TOPIC.to_string()],
        "delete",
        logweir_core::spec::TargetMode::Scratch,
        "RUN",
        &mut out,
    );
    assert!(deleted.is_empty());
    assert_eq!(failed.len(), 1);
    assert_eq!(
        String::from_utf8(out).unwrap(),
        "teardown-failed=[{\"topic\":\"drill-orders\",\"error\":\"the restore principal lacks \
         `Delete` on topic `drill-orders`: TopicAuthorizationFailed (Broker: Topic authorization \
         failed)\"}]\n"
    );
}

/// A failed `newTopic` restore, or a scratch one under
/// `teardown: keep`, deletes nothing it created, at any policy. Control: the
/// same call in scratch mode under `delete` deletes.
#[test]
fn a_failed_new_topic_restore_deletes_nothing() {
    let mapping = fixtures::mapping("orders", "restored-orders");
    let created = ["restored-orders".to_string()];
    for (mode, policy) in [
        (logweir_core::spec::TargetMode::NewTopic, "delete"),
        (logweir_core::spec::TargetMode::Scratch, "keep"),
    ] {
        let deleter = Recording::default();
        let (deleted, failed) = phase9_teardown::after_failure(
            &deleter,
            &mapping,
            &created,
            policy,
            mode,
            "RUN",
            &mut std::io::sink(),
        );
        assert!(deleter.0.lock().unwrap().is_empty(), "{mode:?} {policy}");
        assert!(deleted.is_empty() && failed.is_empty());
    }
    let deleter = Recording::default();
    phase9_teardown::after_failure(
        &deleter,
        &mapping,
        &created,
        "delete",
        logweir_core::spec::TargetMode::Scratch,
        "RUN",
        &mut std::io::sink(),
    );
    assert_eq!(*deleter.0.lock().unwrap(), vec!["restored-orders"]);
}

/// **The grant.** An authorization refusal is named as the missing
/// `Delete` grant, in the warning and in what the `teardown-failed=` line
/// carries; any other refusal is not. A pass's warning reads the same.
#[test]
fn the_teardown_warning_names_the_grant_only_for_an_authorization_refusal() {
    let refused = vec![("drill-orders".to_string(), AUTHZ.to_string())];
    assert_eq!(
        phase9_teardown::left_warning(&refused).unwrap(),
        "teardown left 1 scratch topic behind on the target cluster: drill-orders; the restore \
         principal lacks `Delete` on topic `drill-orders`"
    );
    assert_eq!(
        phase9_teardown::left_reasons(&refused),
        vec![(
            "drill-orders".to_string(),
            "the restore principal lacks `Delete` on topic `drill-orders`: \
             TopicAuthorizationFailed (Broker: Topic authorization failed)"
                .to_string()
        )]
    );
    let other = vec![("drill-orders".to_string(), REFUSAL.to_string())];
    assert_eq!(
        phase9_teardown::left_warning(&other).unwrap(),
        "teardown left 1 scratch topic behind on the target cluster: drill-orders"
    );
    assert_eq!(phase9_teardown::left_reasons(&other), other);
}
