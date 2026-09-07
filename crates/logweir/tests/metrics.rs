//! T0-7. What a terminal path leaves behind on the ONE observable surface v0.1
//! has: the Prometheus textfile at `--metrics-file`.
//!
//! The split is deliberate. This file drives the public seam
//! `logweir::metrics::write_minimal_textfile` (and `write_textfile`) and pins
//! the CONTENT of what a run leaves behind. The ROUTING — which terminal path
//! reaches which writer — is pinned by the inline `mod tests` in
//! `crates/logweir/src/drill/mod.rs`, because `report` is crate-private and an
//! integration test cannot name it. Re-testing the routing here through some
//! public stand-in would be a second, weaker copy of a contract that already
//! has an exact one.

/// The value of the first SAMPLE line for `name` — a line that is not a
/// `# HELP` / `# TYPE` comment. Returns `None` when the metric appears only in
/// a comment, which is exactly the "emit the HELP block and nothing else"
/// mutant.
fn sample_value(text: &str, name: &str) -> Option<i64> {
    text.lines()
        .filter(|l| !l.starts_with('#') && !l.trim().is_empty())
        .find(|l| l.starts_with(name))
        .and_then(|l| l.rsplit_once(' '))
        .and_then(|(_, v)| v.trim().parse::<i64>().ok())
}

fn a_scorecard() -> logweir_core::scorecard::Scorecard {
    serde_json::from_str(include_str!("../../../e2e/fixtures/scorecard-pass.json"))
        .expect("the checked-in fixture parses")
}

/// T0-7 rung 2. `logweir_drill_last_run_timestamp_seconds` is rendered at
/// `docs/kubernetes.md:202` inside a "Verified live" transcript and was emitted
/// by nothing anywhere in the tree. This is the test that makes the
/// documentation true — and the `±60 s` window is what stops it being made true
/// by a constant.
#[test]
fn metrics_textfile_contains_last_run_timestamp() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("logweir.prom");
    logweir::metrics::write_textfile(&p, &a_scorecard()).unwrap();
    let t = std::fs::read_to_string(&p).unwrap();

    let v = sample_value(&t, "logweir_drill_last_run_timestamp_seconds").unwrap_or_else(|| {
        panic!(
            "no `logweir_drill_last_run_timestamp_seconds` SAMPLE line (a HELP/TYPE \
             block alone is not a metric):\n{t}"
        )
    });
    let now = chrono::Utc::now().timestamp();
    assert!(
        (now - v).abs() <= 60,
        "the timestamp must be THIS run's wall clock, never a constant: emitted {v}, \
         now {now}, drift {}s\n{t}",
        (now - v).abs()
    );
}

/// R-11a. `run_id` is a ULID — strictly higher cardinality than `triggered_by`,
/// which `crates/logweir/src/metrics.rs:1-4` already refuses to make a label. So
/// it rides as a comment line node_exporter's textfile collector passes over and
/// `cat` shows, on both shapes of the file, and it is a label on nothing.
#[test]
fn metrics_minimal_textfile_carries_the_run_id_as_a_comment_not_a_label() {
    let dir = tempfile::tempdir().unwrap();

    let p = dir.path().join("minimal.prom");
    logweir::metrics::write_minimal_textfile(
        &p,
        None,
        "01TESTRUNID",
        logweir::exit::ExitCode::GuardRefused,
    )
    .unwrap();
    let t = std::fs::read_to_string(&p).unwrap();
    assert!(
        t.contains("# logweir run_id=01TESTRUNID\n"),
        "the minimal textfile is the only thing an operator has on the failure \
         paths; it has to say WHICH run:\n{t}"
    );
    assert_samples_carry_no_run_id(&t);

    // The two shapes stay consistent: the full file carries the same line, from
    // the scorecard's own `run_id`, and it is not a label there either.
    let full = dir.path().join("full.prom");
    let sc = a_scorecard();
    logweir::metrics::write_textfile(&full, &sc).unwrap();
    let t = std::fs::read_to_string(&full).unwrap();
    assert!(
        t.contains(&format!("# logweir run_id={}\n", sc.run_id)),
        "{t}"
    );
    assert_samples_carry_no_run_id(&t);
}

fn assert_samples_carry_no_run_id(t: &str) {
    for l in t
        .lines()
        .filter(|l| !l.starts_with('#') && !l.trim().is_empty())
    {
        assert!(
            !l.contains("run_id"),
            "R-11a: a ULID as a label is unbounded cardinality. `{l}`\n{t}"
        );
    }
}

/// T0-7 rung 1. The dashboard must carry the freshness panel, and its
/// expression must be the query `docs/metrics.md` tells an operator to alert on
/// — a documented query and a shipped panel that disagree is how a dashboard
/// silently stops meaning anything.
///
/// The series is deliberately node_exporter's own, not a `logweir_*` one: a
/// metric Logweir writes cannot report that Logweir did not run, and
/// `node_textfile_mtime_seconds` carries no `cluster` label, so absence
/// detection works identically on the paths where the cluster id was never
/// learned (R-11b).
#[test]
fn dashboard_has_freshness_panel() {
    const SERIES: &str = "node_textfile_mtime_seconds";
    const MATCHER: &str = r#"file=~".*logweir.*""#;

    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let raw = std::fs::read_to_string(root.join("../../dashboards/logweir.json"))
        .expect("dashboards/logweir.json is checked in");
    let dashboard: serde_json::Value =
        serde_json::from_str(&raw).expect("dashboards/logweir.json is valid JSON");

    let found = dashboard["panels"]
        .as_array()
        .expect("the dashboard has panels")
        .iter()
        .any(|p| {
            p["targets"].as_array().is_some_and(|ts| {
                ts.iter().any(|t| {
                    let e = t["expr"].as_str().unwrap_or_default();
                    e.contains(SERIES) && e.contains(MATCHER)
                })
            })
        });
    assert!(
        found,
        "no panel queries `{SERIES}` with `{MATCHER}`. Without it the dashboard \
         cannot say the drill stopped running at all, which is the one thing an \
         absence-based operational story has to be able to say."
    );

    let doc = std::fs::read_to_string(root.join("../../docs/metrics.md"))
        .expect("docs/metrics.md is checked in");
    for needle in [SERIES, MATCHER] {
        assert!(
            doc.contains(needle),
            "docs/metrics.md does not contain `{needle}`, so the documented \
             freshness query and the shipped panel have drifted apart"
        );
    }
}
