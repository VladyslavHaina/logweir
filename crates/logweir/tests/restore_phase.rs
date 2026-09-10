mod fixtures; // crates/logweir/tests/fixtures/mod.rs — Task 14 step 5c
use logweir::drill::phase6_restore::assert_post_condition;
use std::collections::BTreeMap;

// ---------------------------------------------------------------------------
// Task 18 fix round 1 (2026-09-04), per task-18-review.md. Everything below
// this line is ADDITIVE: none of the four tests above are modified, per this
// round's explicit instruction. New imports needed only by the tests below.
use logweir::drill::DrillError;
use logweir_core::engine::{
    BackupSetFacts, BackupSetRef, DataEngine, EngineError, EngineId, PhaseObserver,
    PreflightReport, RecordFingerprint, RestoreFacts, RestorePlan, SampleSelection, StorageUrl,
};
use logweir_kafka::reader::{ClusterReader, ConsumedRecord, KafkaError, TopicMeta};
use std::sync::atomic::{AtomicBool, Ordering};

/// The backstop for a `dry_run` that arrives by any route the phase-0 guard did
/// not see. A no-op restore exits 0 and would otherwise score, producing a
/// signed scorecard whose RTO was measured around nothing.
#[test]
fn all_zero_end_offsets_fail_the_post_condition() {
    let mut m = BTreeMap::new();
    m.insert("drill-orders".to_string(), vec![(0, 0i64), (1, 0), (2, 0)]);
    let e = assert_post_condition(&m).unwrap_err();
    assert!(e.to_string().contains("no-op"), "{e}");
}

#[test]
fn one_non_zero_partition_satisfies_the_post_condition() {
    let mut m = BTreeMap::new();
    m.insert("drill-orders".to_string(), vec![(0, 0i64), (1, 41), (2, 0)]);
    assert!(assert_post_condition(&m).is_ok());
}

#[test]
fn an_empty_map_fails_rather_than_vacuously_passing() {
    assert!(assert_post_condition(&BTreeMap::new()).is_err());
}

#[test]
fn the_measured_window_brackets_the_subprocess_and_nothing_else() {
    let (r, engine) = fixtures::engine_that_sleeps_ms(300);
    let out = logweir::drill::phase6_restore::run(
        &engine,
        &fixtures::plan(),
        &r,
        &fixtures::mapping("orders", "drill-orders"),
        &mut fixtures::NullObserver,
    )
    .unwrap();
    let ms = (out.finished_at - out.started_at).num_milliseconds();
    assert!(
        (250..2_000).contains(&ms),
        "measured {ms}ms — it must bracket the subprocess only"
    );
}

// ---------------------------------------------------------------------------
// Fix round 1 additions. Doubles local to this file — no shared fixture is
// touched, no injection hook, no env var, no cfg flag; GC4/GR7 are unaffected.

/// Mutant M4 (review): `run()`'s call to `assert_post_condition` could be
/// deleted and every test above still passed, because `engine_that_sleeps_ms`'s
/// `FakeReader` target holds a non-zero end offset (500). This test uses a
/// target still at end offset 0 instead, so the backstop is load-bearing.
#[test]
fn run_refuses_a_restore_that_left_the_target_empty() {
    let engine = fixtures::SleepEngine { ms: 1 };
    let reader = fixtures::FakeReader {
        state: fixtures::target_with("MkU3OEVBNTcwNTJENDM2Qk", "drill-orders", 3, 0, &[]),
    };
    let r = logweir::drill::phase6_restore::run(
        &engine,
        &fixtures::plan(),
        &reader,
        &fixtures::mapping("orders", "drill-orders"),
        &mut fixtures::NullObserver,
    );
    let Err(e) = r else {
        panic!("run() returned Ok for a target still at end offset 0")
    };
    // The variant is what decides the exit code (Task 18 fix round 1, F1/F6):
    // it must be the not-pass finding, never Operational/Kafka/Engine.
    assert!(
        matches!(e, DrillError::RestoreNoOp(_)),
        "the no-op backstop must surface as RestoreNoOp (exit 2, a drill result), not {e:?}"
    );
    assert!(e.to_string().contains("no-op"), "{e}");
}

/// Review finding F5: an empty topic mapping and an all-zero restore are NOT
/// the same finding, and only one of them says anything about a restore that
/// ran. This pins the empty-map branch's classification and its wording.
#[test]
fn an_empty_topic_mapping_is_operational_not_a_restore_finding() {
    let e = assert_post_condition(&BTreeMap::new()).unwrap_err();
    assert!(
        matches!(e, DrillError::Operational(_)),
        "an empty selection says nothing about the archive; got {e:?}"
    );
    assert!(
        !e.to_string().contains("no-op"),
        "an empty selection must not claim a restore ran and did nothing: {e}"
    );
}

/// The other half of F5: an all-zero restore over a NON-EMPTY selection is a
/// positively established fact about the archive, so it must NOT share the
/// empty-map's `Operational` classification.
#[test]
fn all_zero_offsets_over_a_nonempty_selection_is_a_restore_finding_not_operational() {
    let mut m = BTreeMap::new();
    m.insert("drill-orders".to_string(), vec![(0, 0i64), (1, 0), (2, 0)]);
    let e = assert_post_condition(&m).unwrap_err();
    assert!(
        matches!(e, DrillError::RestoreNoOp(_)),
        "a non-empty selection that restored nothing is a drill result, not {e:?}"
    );
}

/// A minimal `DataEngine` double, local to this file: it never sleeps and
/// never reads a real clock — it returns FIXED sentinel timestamps and
/// records whether it was actually invoked. Review findings F3/M6/N6/N7/N18:
/// a duration-tolerance assertion cannot tell "measured the subprocess" apart
/// from "invented a plausible number" or "drifted by ~1s in either
/// direction". Comparing for exact identity against the engine's own
/// `RestoreFacts`, and asserting the engine was actually called, closes all
/// four at once with zero tolerance.
struct SentinelEngine {
    called: AtomicBool,
}
impl DataEngine for SentinelEngine {
    fn id(&self) -> EngineId {
        EngineId {
            id: "sentinel".into(),
            version: "v0.0.0".into(),
            digest: format!("sha256:{}", "0".repeat(64)),
        }
    }
    fn list_backup_sets(&self, _l: &StorageUrl) -> Result<Vec<BackupSetRef>, EngineError> {
        Ok(vec![])
    }
    fn describe(&self, _s: &BackupSetRef) -> Result<BackupSetFacts, EngineError> {
        Ok(fixtures::backup_facts_orders(3))
    }
    fn preflight(&self, _p: &RestorePlan) -> Result<PreflightReport, EngineError> {
        Err(EngineError::Operational("not used by this fixture".into()))
    }
    fn restore(
        &self,
        _p: &RestorePlan,
        _o: &mut dyn PhaseObserver,
    ) -> Result<RestoreFacts, EngineError> {
        self.called.store(true, Ordering::SeqCst);
        Ok(RestoreFacts {
            started_at: fixtures::ts("2026-01-01T00:00:00Z"),
            finished_at: fixtures::ts("2026-01-01T00:00:05Z"),
            exit_code: 0,
            unknown_key_warnings: vec!["restore.some_key".to_string()],
        })
    }
    fn fingerprints(&self, _s: &SampleSelection) -> Result<Vec<RecordFingerprint>, EngineError> {
        Ok(vec![])
    }
}

#[test]
fn the_measured_span_is_exactly_the_engines_own_facts_and_the_engine_is_actually_invoked() {
    let engine = SentinelEngine {
        called: AtomicBool::new(false),
    };
    let reader = fixtures::FakeReader {
        state: fixtures::target_with("MkU3OEVBNTcwNTJENDM2Qk", "drill-orders", 3, 500, &[]),
    };
    let out = logweir::drill::phase6_restore::run(
        &engine,
        &fixtures::plan(),
        &reader,
        &fixtures::mapping("orders", "drill-orders"),
        &mut fixtures::NullObserver,
    )
    .unwrap();
    assert!(
        engine.called.load(Ordering::SeqCst),
        "the engine must actually be invoked, not bypassed with a fabricated window"
    );
    assert_eq!(out.started_at, fixtures::ts("2026-01-01T00:00:00Z"));
    assert_eq!(out.finished_at, fixtures::ts("2026-01-01T00:00:05Z"));
    // F8: the engine's unknown-key warnings must survive phase 6, not be
    // silently dropped — nothing downstream can recover them otherwise.
    assert_eq!(
        out.unknown_key_warnings,
        vec!["restore.some_key".to_string()]
    );
}

/// A `ClusterReader` double whose `end_offsets` always fails, local to this
/// file. Review finding F7/N4: a broker read failure during the post-restore
/// check must surface as `DrillError::Kafka`, never be swallowed into a
/// false "no-op" finding about the archive.
struct FailingReader;
impl ClusterReader for FailingReader {
    fn cluster_id(&self) -> Result<String, KafkaError> {
        Ok("MkU3OEVBNTcwNTJENDM2Qk".into())
    }
    fn list_topics(&self) -> Result<Vec<TopicMeta>, KafkaError> {
        Ok(vec![])
    }
    fn end_offsets(&self, _topic: &str) -> Result<Vec<(i32, i64)>, KafkaError> {
        Err(KafkaError::Client(
            "broker unreachable during post-condition read".into(),
        ))
    }
    fn topic_configs(&self, _topic: &str) -> Result<BTreeMap<String, String>, KafkaError> {
        Ok(BTreeMap::new())
    }
    /// Task 8 (guard **G-TS**) added this to `ClusterReader`. An empty map is
    /// a broker that surfaces neither `log.message.timestamp.type` nor a
    /// timestamp bound, which is the harmless case: phase 0's preflight then
    /// treats the broker as the Apache default (`CreateTime`) and refuses
    /// nothing. G-TS's own arms live in
    /// `crates/logweir/tests/topic_preflight.rs`.
    fn broker_configs(&self) -> Result<BTreeMap<String, String>, KafkaError> {
        Ok(BTreeMap::new())
    }
    fn consume_range(
        &self,
        _t: &str,
        _p: i32,
        _from: i64,
        _max: usize,
    ) -> Result<Vec<ConsumedRecord>, KafkaError> {
        Ok(vec![])
    }
}

#[test]
fn a_broker_failure_during_the_post_restore_read_surfaces_as_kafka_not_a_no_op() {
    let engine = fixtures::SleepEngine { ms: 1 };
    let reader = FailingReader;
    let r = logweir::drill::phase6_restore::run(
        &engine,
        &fixtures::plan(),
        &reader,
        &fixtures::mapping("orders", "drill-orders"),
        &mut fixtures::NullObserver,
    );
    let Err(e) = r else {
        panic!("run() returned Ok despite the reader failing on every call")
    };
    assert!(
        matches!(e, DrillError::Kafka(_)),
        "a broker failure reading the target is operational-by-way-of-Kafka, not {e:?}"
    );
}

// ---------------------------------------------------------------------------
// Task 6 (T0-14). ADDITIVE: nothing above this line is modified.

/// Ruling **R-E**. A phase-5 / phase-6 `restore.yaml` divergence is detected
/// inside `OsoCliEngine::restore`, i.e. AFTER phases 0-5 have run — the
/// admission guard passed, the plan was rendered, and the engine's own
/// `validate-restore` already executed. Global Constraint 11 reserves exit `3`
/// for "plan refused by a guard, **before anything runs**", so it does not
/// describe this at all. The refusal is `EngineError::Operational`, which the
/// existing route at `crates/logweir/src/drill/mod.rs:105` maps through
/// `DrillError::Engine` to exit `1` — operational error, no artifact. This
/// test is the machine check on that route, and it is the kill for a mutant
/// that re-points `DrillError::Engine(_)` at `ExitCode::GuardRefused`.
///
/// It lives here rather than beside the other three T0-14 tests in
/// `crates/logweir-engine-oso/tests/render_equality.rs` because
/// `logweir-engine-oso` does not depend on `logweir` and must not: adding the
/// dependency would invert the crate graph.
#[test]
fn render_mismatch_is_operational_not_guard() {
    let e = logweir::drill::DrillError::Engine(logweir_core::engine::EngineError::Operational(
        "rendered restore.yaml diverged between phase 5 and phase 6: phase 5 validated \
         sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa, phase 6 \
         would restore sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb \
         — refusing"
            .into(),
    ));
    let code = logweir::exit::ExitCode::from(e);
    assert_eq!(code as u8, 1, "R-E: operational (1), no artifact");
    assert_ne!(
        code as u8, 3,
        "GC11's 3 is a guard refusal BEFORE anything runs; phases 0-5 have run"
    );
}
