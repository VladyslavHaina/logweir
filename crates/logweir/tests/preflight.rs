use logweir::drill::phase5_preflight::{adjudicate, Verdict};
use logweir_core::engine::{CoverageState, PartitionCoverage, PreflightReport};

fn report(states: &[CoverageState], honoured: bool, valid: bool) -> PreflightReport {
    PreflightReport {
        valid,
        errors: vec![],
        warnings: vec![],
        segments_to_process: 4,
        records_to_restore: 100,
        time_range: Some((0, 1)),
        header_preflight_honoured: honoured,
        unknown_key_warnings: vec![],
        partitions: states
            .iter()
            .enumerate()
            .map(|(i, s)| PartitionCoverage {
                topic: "orders".into(),
                partition: i as i32,
                state: s.clone(),
                detail: String::new(),
            })
            .collect(),
    }
}

#[test]
fn full_coverage_proceeds() {
    // NOT `[CoverageState::Full; 3]`: the array-repeat expression requires
    // `Copy`, and `CoverageState` carries an `Unknown(String)` variant, so it
    // can never be Copy (E0277).
    assert!(matches!(
        adjudicate(&report(
            &[
                CoverageState::Full,
                CoverageState::Full,
                CoverageState::Full
            ],
            true,
            true
        )),
        Verdict::Proceed { .. }
    ));
}

/// The three states Logweir blocks on. `Missing` here is header coverage, not
/// data — it is a WARNING, because v0.1 never requests offset recovery.
#[test]
fn data_missing_and_corrupt_block() {
    for s in [CoverageState::DataMissing, CoverageState::Corrupt] {
        match adjudicate(&report(&[s.clone(), CoverageState::Full], true, false)) {
            Verdict::Block { findings } => assert_eq!(findings.len(), 1, "{s:?}"),
            v => panic!("{s:?} must block, got {v:?}"),
        }
    }
}

#[test]
fn empty_and_indeterminate_block_because_neither_is_a_positive_pass() {
    for s in [CoverageState::Empty, CoverageState::Indeterminate] {
        assert!(matches!(
            adjudicate(&report(&[s], true, true)),
            Verdict::Block { .. }
        ));
    }
}

#[test]
fn missing_header_coverage_is_a_warning_not_a_block() {
    match adjudicate(&report(
        &[CoverageState::Missing, CoverageState::Full],
        true,
        true,
    )) {
        Verdict::Proceed { warnings } => assert_eq!(warnings.len(), 1),
        v => panic!("header coverage is advisory when offset recovery is not requested, got {v:?}"),
    }
}

#[test]
fn an_unknown_state_blocks_and_carries_the_raw_string() {
    match adjudicate(&report(
        &[CoverageState::Unknown("quantum".into())],
        true,
        true,
    )) {
        Verdict::Block { findings } => assert!(findings[0].detail.contains("quantum")),
        v => panic!("an unrecognised state must block, got {v:?}"),
    }
}

#[test]
fn a_lever_the_engine_ignored_blocks() {
    match adjudicate(&report(&[CoverageState::Full], false, true)) {
        Verdict::Block { findings } => assert!(findings[0].detail.contains("header_preflight")),
        v => panic!("an ignored lever must block, got {v:?}"),
    }
}

#[test]
fn engine_reported_errors_block_even_when_every_partition_is_full() {
    let mut r = report(&[CoverageState::Full], true, false);
    r.errors
        .push("segment .../000000000100.kbak is missing from storage".into());
    assert!(
        matches!(adjudicate(&r), Verdict::Block { .. }),
        "dry_run_check_segments errors are the whole reason we set the lever"
    );
}
