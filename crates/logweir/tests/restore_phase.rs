mod fixtures; // crates/logweir/tests/fixtures/mod.rs — Task 14 step 5c
use logweir::drill::phase6_restore::assert_post_condition;
use std::collections::BTreeMap;

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
