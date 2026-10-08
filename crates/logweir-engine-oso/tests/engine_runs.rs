//! **PROD-11.1 — one engine run per distinct partition subset**, through
//! `impl DataEngine for OsoCliEngine` against `e2e/fixtures/fake-engine-runs.sh`,
//! which logs every invocation (subcommand, document, `source_partitions`,
//! target topics) and writes an offset report per run.
use logweir_core::engine::{
    BackupSetRef, CoverageState, DataEngine, EngineReport, PartitionCoverage, PhaseObserver,
    PreflightReport, RestorePlan, StorageUrl, WindowFloorSource,
};
use logweir_engine_oso::engine::{
    compose_offset_reports, merge_engine_reports, merge_preflight_reports, OsoCliEngine,
};
use logweir_engine_oso::storage::Store;
use std::path::{Path, PathBuf};

const FAKE: &str = "../../e2e/fixtures/fake-engine-runs.sh";

fn unique_dir(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!(
        "logweir-engine-runs-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&d).unwrap();
    d
}

struct Quiet;
impl PhaseObserver for Quiet {
    fn phase_started(&mut self, _: i8, _: &str) {}
    fn phase_finished(&mut self, _: i8, _: &str) {}
    fn engine_line(&mut self, _: &str, _: &str) {}
}

/// `audit` (no subset), `orders: [0, 2]`, `payments: [1]`: three runs.
fn plan(dir: &Path, topics: &[(&str, Option<&[i32]>)]) -> RestorePlan {
    RestorePlan {
        set: BackupSetRef {
            backup_id: "b".into(),
            manifest_key: "b/manifest.json".into(),
        },
        storage: StorageUrl::Filesystem {
            path: "/archive".into(),
        },
        target_bootstrap: vec!["k:9092".into()],
        target_auth: logweir_core::engine::AuthRender::Plaintext,
        topic_mapping: topics
            .iter()
            .map(|(t, _)| (t.to_string(), format!("r-{t}")))
            .collect(),
        time_window: (
            "2026-08-29T00:00:00Z".parse().unwrap(),
            "2026-08-30T02:00:00Z".parse().unwrap(),
        ),
        window_floor_source: WindowFloorSource::InheritedFromSpec,
        source_partitions: topics
            .iter()
            .filter_map(|(t, ps)| ps.map(|ps| (t.to_string(), ps.to_vec())))
            .collect(),
        default_replication_factor: 1,
        checkpoint_state: dir.join("checkpoint.json"),
        checkpoint_interval_secs: 30,
        offset_report: dir.join("offsets.json"),
    }
}

fn three(dir: &Path) -> RestorePlan {
    plan(
        dir,
        &[
            ("audit", None),
            ("orders", Some(&[0, 2])),
            ("payments", Some(&[1])),
        ],
    )
}

fn engine(workdir: &Path) -> OsoCliEngine {
    OsoCliEngine::new(
        PathBuf::from(FAKE),
        "v0.23.3-test".into(),
        "sha256:test".into(),
        workdir.to_path_buf(),
        Store::in_memory("logweir"),
    )
}

fn log(workdir: &Path) -> Vec<String> {
    std::fs::read_to_string(workdir.join("engine-runs.log"))
        .unwrap_or_default()
        .lines()
        .map(|l| l.trim_end().to_string())
        .collect()
}

/// **Three runs, validated and restored in order, each its own document,
/// filter and report; the uploaded report is every run's, in order.** KILLS:
/// one document for every topic (the engine would filter `audit` by a
/// subset), a run skipped, the composed report missing a run, the engine
/// report not the union.
#[test]
fn a_plan_with_different_subsets_is_one_engine_run_per_subset() {
    let (work, out) = (unique_dir("work"), unique_dir("out"));
    let p = three(&out);
    let e = engine(&work);
    let report = e.preflight(&p).expect("every run validates");
    assert!(report.valid && report.header_preflight_honoured);
    assert_eq!(report.records_to_restore, 30, "three runs' counts, summed");
    assert_eq!(report.rendered_restore_sha256.split(',').count(), 3);
    let facts = e.restore(&p, &mut Quiet).expect("every run restores");
    assert_eq!(
        log(&work),
        vec![
            "validate-restore restore.run-0.yaml - r-audit",
            "validate-restore restore.run-1.yaml 0, 2 r-orders",
            "validate-restore restore.run-2.yaml 1 r-payments",
            "restore restore.run-0.yaml - r-audit",
            "restore restore.run-1.yaml 0, 2 r-orders",
            "restore restore.run-2.yaml 1 r-payments",
        ]
    );
    assert_eq!(
        facts.engine_report,
        EngineReport::Read(
            [
                ("r-audit".to_string(), 0),
                ("r-orders".to_string(), 0),
                ("r-orders".to_string(), 2),
                ("r-payments".to_string(), 1),
            ]
            .into_iter()
            .collect()
        )
    );
    let composed: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&p.offset_report).unwrap()).unwrap();
    let runs = composed.as_array().expect("an array, one element per run");
    assert_eq!(runs.len(), 3);
    assert!(runs[1]["entries"]["r-orders/2"].is_object());
    for i in 0..3 {
        assert!(out.join(format!("offsets.run-{i}.json")).exists());
    }
}

/// A plan with no subset is ONE run over `restore.yaml`, its report the
/// engine's own object at the plan's path — the behaviour before PROD-11.1.
#[test]
fn a_plan_without_a_subset_is_one_run_as_before() {
    let (work, out) = (unique_dir("work1"), unique_dir("out1"));
    let p = plan(&out, &[("audit", None), ("orders", None)]);
    let e = engine(&work);
    e.preflight(&p).unwrap();
    e.restore(&p, &mut Quiet).unwrap();
    assert_eq!(
        log(&work),
        vec![
            "validate-restore restore.yaml - r-audit r-orders",
            "restore restore.yaml - r-audit r-orders",
        ]
    );
    let report: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&p.offset_report).unwrap()).unwrap();
    assert!(report.is_object(), "the engine's own report, not an array");
}

/// **Phase 6 restores exactly the documents phase 5 validated.** A plan
/// whose subsets changed after preflight (one subset moved between topics)
/// is refused before any engine run. KILLS: comparing only the first run's
/// digest, or the count of runs.
#[test]
fn a_run_set_that_diverged_after_preflight_is_refused() {
    let (work, out) = (unique_dir("work2"), unique_dir("out2"));
    let p = three(&out);
    let e = engine(&work);
    e.preflight(&p).unwrap();
    let swapped = plan(
        &out,
        &[
            ("audit", None),
            ("orders", Some(&[1])),
            ("payments", Some(&[0, 2])),
        ],
    );
    let err = e.restore(&swapped, &mut Quiet).unwrap_err().to_string();
    assert!(
        err.contains("diverged between phase 5 and phase 6"),
        "{err}"
    );
    assert!(
        log(&work).iter().all(|l| !l.starts_with("restore ")),
        "no engine run started"
    );
}

/// A run that fails stops the restore and is named, with its topics; the
/// later runs never start.
#[test]
fn a_failing_run_is_named_and_stops_the_rest() {
    let (work, out) = (unique_dir("work3"), unique_dir("out3"));
    let p = plan(
        &out,
        &[
            ("audit", None),
            ("fail-me", Some(&[0])),
            ("payments", Some(&[1])),
        ],
    );
    let e = engine(&work);
    e.preflight(&p).unwrap();
    let err = e.restore(&p, &mut Quiet).unwrap_err().to_string();
    assert!(err.contains("engine run 2 of 3, topics fail-me"), "{err}");
    assert!(
        !log(&work)
            .iter()
            .any(|l| l.starts_with("restore restore.run-2.yaml")),
        "the third run never started: {:?}",
        log(&work)
    );
}

fn report(valid: bool, honoured: bool, state: CoverageState) -> PreflightReport {
    PreflightReport {
        valid,
        errors: if valid { vec![] } else { vec!["bad".into()] },
        warnings: vec![],
        segments_to_process: 1,
        records_to_restore: 2,
        time_range: Some((5, 9)),
        partitions: vec![PartitionCoverage {
            topic: "t".into(),
            partition: 0,
            state,
            detail: String::new(),
        }],
        header_preflight_honoured: honoured,
        unknown_key_warnings: vec![],
        rendered_restore_sha256: "sha256:x".into(),
    }
}

/// Merging can only make the preflight WORSE: one invalid run, one ignored
/// lever, one blocking coverage finding survives the merge. KILLS: `||` for
/// `&&`, dropping a run's findings.
#[test]
fn merged_preflight_reports_keep_every_runs_findings() {
    let m = merge_preflight_reports(vec![
        report(true, true, CoverageState::Full),
        report(false, false, CoverageState::Missing),
    ]);
    assert!(!m.valid && !m.header_preflight_honoured);
    assert_eq!(m.errors, vec!["bad".to_string()]);
    assert_eq!(m.partitions.len(), 2);
    assert!(m
        .partitions
        .iter()
        .any(|p| p.state == CoverageState::Missing));
    assert_eq!(m.segments_to_process, 2);
    let one = merge_preflight_reports(vec![report(true, true, CoverageState::Full)]);
    assert!(one.valid && one.header_preflight_honoured);
}

/// The union is checked only when every run's report was read.
#[test]
fn engine_reports_merge_to_a_union_only_when_every_run_was_read() {
    let read = |t: &str| EngineReport::Read([(t.to_string(), 0)].into_iter().collect());
    assert_eq!(
        merge_engine_reports(vec![read("a"), read("b")]),
        EngineReport::Read(
            [("a".to_string(), 0), ("b".to_string(), 0)]
                .into_iter()
                .collect()
        )
    );
    assert_eq!(
        merge_engine_reports(vec![read("a"), EngineReport::Absent]),
        EngineReport::Absent
    );
    assert_eq!(
        merge_engine_reports(vec![
            EngineReport::Absent,
            EngineReport::Unreadable("x".into())
        ]),
        EngineReport::Unreadable("x".into())
    );
}

/// Review L5: a run's report path equal to the composed report's is refused,
/// never copied into itself (which never ends). The control composes two
/// distinct paths. KILLS: deleting the guard (the test would not finish).
#[test]
fn composing_a_report_into_itself_is_refused() {
    let dir = unique_dir("self");
    let out = dir.join("offsets.json");
    std::fs::write(&out, b"{}").unwrap();
    let err = compose_offset_reports(std::slice::from_ref(&out), &out)
        .unwrap_err()
        .to_string();
    assert!(err.contains("copied into itself"), "{err}");
    let a = dir.join("offsets.run-0.json");
    std::fs::write(&a, b"{\"entries\": {}}").unwrap();
    compose_offset_reports(&[a, dir.join("offsets.run-1.json")], &out).unwrap();
    assert_eq!(
        std::fs::read_to_string(&out).unwrap(),
        "[{\"entries\": {}},\nnull]\n"
    );
}
