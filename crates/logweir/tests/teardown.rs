//! T0-11 — a drill that leaves scratch topics on the operator's cluster must
//! SAY SO, on every channel an operator actually watches, on the exact run that
//! left them, and without changing the exit code.
//!
//! Phase 9's attestation has always been honest: `phase9_teardown::run` puts
//! every topic the broker refused into `topics_failed` and never lists it in
//! `topics_deleted`, so the signed teardown document cannot assert a clean
//! state the cluster is not in. What did not hold was anything an operator can
//! SEE. Before this file existed, `grep -rn "topics_failed" crates/logweir/src`
//! found five hits — three doc comments, the struct field and its construction
//! — and zero in a `tracing::` call, zero in `metrics.rs`, zero in
//! `summary_line()`. A drill that left five scratch topics on a production
//! broker printed the same stdout line, wrote the same Prometheus textfile and
//! exited 0 as one that cleaned up.
//!
//! The three channels are one test each, plus the two that hold the boundary:
//! a clean run's console line stays byte-identical, and the exit code does not
//! move in either direction (stage-2 ruling R-C).
mod fixtures;

use fixtures::Drill;
use logweir::drill::{execute_with, phase9_teardown, summary_line};
use logweir_core::outcome::Outcome;
use logweir_kafka::reader::{KafkaError, TopicDeleter};

/// The name the fixture spec maps `orders` onto (`topic_mapping_prefix:
/// "drill-"`), and therefore the one scratch topic a fixture drill creates.
const SCRATCH_TOPIC: &str = "drill-orders";
/// The broker's refusal, verbatim, as `Drill::LeavesATopicBehind` reports it.
const REFUSAL: &str = "BROKER: TOPIC_DELETION_DISABLED";

// ---------------------------------------------------------------- log capture

// Where the rendered JSON goes, per THREAD. `None` on a thread that is not
// capturing, and every line written there is discarded.
//
// This is one half of the answer to a real defect, and the reason it is not
// the obvious `tracing::subscriber::with_default` the sibling test files use.
// `with_default` installs a THREAD-LOCAL dispatcher, but `tracing` caches two
// things PROCESS-wide: each callsite's `Interest`, and the global maximum
// level. A callsite first reached on a thread where no dispatcher exists at
// all caches `Interest::never`, and from then on the macro skips the event
// before it ever consults the current thread's dispatcher. This binary runs
// seven tests in parallel and six of them drive a whole drill with no
// subscriber, so they reach `drill::teardown`'s `warn!` first and poison it.
// MEASURED on the inherited code: `teardown_failure_names_the_topics` passed
// alone and under `--test-threads=1`, and failed under a plain `cargo test`
// with a captured stream that held the phase lines and not one `WARN`.
//
// So the subscriber is installed ONCE, GLOBALLY (below), which is what fixes
// the interest cache — `set_global_default` rebuilds it — and the per-thread
// routing here is what keeps the six other tests' events out of this one's
// buffer. It is also closer to production: `drill::run` installs a global
// subscriber too, and never a scoped one.
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
/// all. That is the property T0-11 is about: the warning must not need a raised
/// level to be seen.
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
/// `phase9_teardown::run`'s per-topic branch — not `score.rs`'s
/// `RefusingDeleter`, whose whole call fails and exercises the other branch.
struct RefusesTheOneTopic;
impl TopicDeleter for RefusesTheOneTopic {
    fn delete_topics(
        &self,
        names: &[String],
    ) -> Result<Vec<(String, Result<(), String>)>, KafkaError> {
        Ok(names
            .iter()
            .map(|n| {
                if n == SCRATCH_TOPIC {
                    (n.clone(), Err(REFUSAL.to_string()))
                } else {
                    (n.clone(), Ok(()))
                }
            })
            .collect())
    }
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

    // Task 12's property, on this line too: the run id is on the EVENT, not
    // only on the entered span. A warning an operator cannot correlate to a
    // run is a warning nobody can act on, and a single-line consumer
    // (`jq '.fields.run_id'`) never reads the span object.
    assert_eq!(
        warning["fields"]["run_id"].as_str(),
        Some(f.run_id.as_str()),
        "the leftover warning must carry the run id as a FIELD on the event: {warning}"
    );

    // The count travels as a FIELD too (`fields.count`), and it is asserted
    // here because the review found that deleting the `count = …` field from
    // the `warn!` left every test green: a claimed field nobody asserts is a
    // field that can vanish. One refused topic ⇒ count 1.
    assert_eq!(
        warning["fields"]["count"].as_u64(),
        Some(1),
        "the leftover warning must carry the failed-topic count as a FIELD on the event: {warning}"
    );

    // The emitted message and the pure function that produces it can never
    // drift: the function is testable without a subscriber, the event is
    // testable with one, and these are asserted to be the same string.
    let att = phase9_teardown::run(
        &RefusesTheOneTopic,
        &fixtures::mapping("orders", SCRATCH_TOPIC),
        "delete",
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
/// So the exit code must not move in EITHER direction, and this fails if it
/// does. `report` is private (`fn report`, not `pub fn`) and widening a private
/// function's visibility for a test is a change a reviewer cannot distinguish
/// from the behaviour change R-C forbids — so the code is asserted through the
/// one public path that carries it: `execute_with` returning `Ok`, which is
/// what `report`'s `Ok(_) => ExitCode::Ok` arm turns into exit 0, and
/// `DrillError::exit_code` (which `impl From<DrillError> for ExitCode`
/// delegates to) never being reached at all.
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

    // (c) …and the failure was RECORDED rather than escalated. Both halves
    //     matter: an "ok" phase record with empty notes is the silence T0-11
    //     is about, and a non-"ok" one is the escalation R-C forbids.
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

/// R-C in its OTHER direction, and the reason it is a separate test: mutant M8
/// is "make `phase9_teardown::persist`'s `Err` at the call site propagate
/// instead of warning", and on a fixture where `persist` SUCCEEDS that mutant
/// changes nothing and survives. The brief says to strengthen the test rather
/// than accept the survivor, so this one makes `persist` actually fail.
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
/// Which makes this the test that keeps `PhaseObserver::phase_started` /
/// `phase_finished` wired. Before Task 13 they were live code with no caller
/// from the orchestrator — `PhaseLogger` implemented them and only a test in
/// `logging.rs` invoked them directly, so what the pair proves about a
/// production run was nothing at all. Deleting the two calls from `record` is
/// the mutant; the closed arithmetic below (EVERY phase record has a pair, not
/// just phase 9) is what fails.
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

/// The documented guarantee the code does not deliver, deleted and kept deleted.
///
/// `phase9_teardown::persist`'s doc comment claimed "a teardown that cannot be
/// attested is exit 4 rather than a silent success" in the SAME paragraph as
/// "A failure here is a WARNING at the call site, never an outcome", and
/// `drill::teardown` proved the first sentence false. That is the signature
/// defect of stage 1, sitting inside the exact function this task touches.
///
/// A doc comment is the one kind of claim no other test in this repository
/// reads, so it is asserted here and, for the operator who never runs
/// `cargo test`, in `just lint` — and the second half of this test is what
/// keeps the `lint` membership honest, in the idiom
/// `one_signer_gate.rs::just_lint_runs_the_one_signer_gate` established.
#[test]
fn the_false_exit_4_guarantee_is_gone_and_the_gate_keeps_it_gone() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .canonicalize()
        .expect("the repository root resolves from CARGO_MANIFEST_DIR");

    let source = std::fs::read_to_string(root.join("crates/logweir/src/drill/phase9_teardown.rs"))
        .expect("phase9_teardown.rs is checked in");
    assert!(
        !source.contains("exit 4 rather than a silent success"),
        "`persist`'s doc comment promises an exit code the call site does not produce. The \
         drill result is signed and uploaded by phase 8 before phase 9 runs, so a failure here \
         is a WARNING and nothing else — which the same paragraph already said."
    );

    let justfile = std::fs::read_to_string(root.join("justfile")).expect("justfile is checked in");
    let mut body = String::new();
    let mut inside = false;
    for line in justfile.lines() {
        if line.starts_with("lint:") {
            inside = true;
            continue;
        }
        if inside {
            if line.starts_with(|c: char| c.is_ascii_lowercase()) {
                break;
            }
            body.push_str(line);
            body.push('\n');
        }
    }
    assert!(inside, "the justfile must still declare a `lint` recipe");
    assert!(
        body.contains("exit 4 rather than a silent success"),
        "`just lint` no longer greps for the deleted sentence, so restoring it to the doc \
         comment would go unnoticed by everyone who does not run `cargo test`. The `lint` body \
         was:\n{body}"
    );
}

// ---------------------------------------------------------------- the dashboard

/// The panel exists, and nothing that existed before this task disappeared.
#[test]
fn dashboard_has_a_teardown_panel() {
    // Captured after rebasing on Task 11/12, RAN:
    //   python3 -c "import json;print(sorted(p['id'] for p in json.load(open('dashboards/logweir.json'))['panels']))"
    // These ids existed before this task ran; none of them may disappear.
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
