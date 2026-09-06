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
        // T0-14: `PreflightReport` carries the SHA-256 of the `restore.yaml`
        // phase 5 wrote. These adjudication tests render no document, so the
        // honest value is the all-zero placeholder; nothing here reads it.
        rendered_restore_sha256:
            "sha256:0000000000000000000000000000000000000000000000000000000000000000".into(),
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

// --- Fix round 1 (coordinator-ruled): the seven tests above never read
// `Finding.state`, so a mislabelled or relabelled reason (the exact property
// this build cares about — "the stated reason must be the ground on which
// the code actually refused") could regress undetected, and `Partial` was
// never constructed by any test at all. These tests are additive only — none
// of the seven tests above are modified — per the coordinator's ruling that
// "the seven tests in Step 1 verbatim" freezes their CONTENT, not the file.

/// A single-partition report with a settable per-partition `detail`, so a
/// blocking arm's ground can be isolated (no other arm can also fire) and its
/// carried-through detail text can be asserted distinctively, rather than the
/// empty string every `report(...)`-built partition above carries.
fn single(state: CoverageState, honoured: bool, valid: bool, detail: &str) -> PreflightReport {
    PreflightReport {
        valid,
        errors: vec![],
        warnings: vec![],
        segments_to_process: 4,
        records_to_restore: 100,
        time_range: Some((0, 1)),
        header_preflight_honoured: honoured,
        unknown_key_warnings: vec![],
        // T0-14: `PreflightReport` carries the SHA-256 of the `restore.yaml`
        // phase 5 wrote. These adjudication tests render no document, so the
        // honest value is the all-zero placeholder; nothing here reads it.
        rendered_restore_sha256:
            "sha256:0000000000000000000000000000000000000000000000000000000000000000".into(),
        partitions: vec![PartitionCoverage {
            topic: "orders".into(),
            partition: 0,
            state,
            detail: detail.into(),
        }],
    }
}

#[test]
fn each_blocking_ground_names_itself() {
    // (state, expected Finding.state tag, a distinctive detail marker to
    // check is carried through -- only for the arms whose detail actually
    // depends on `p.detail`; `Empty`'s detail is a fixed string and carries
    // no partition detail, so it has none).
    let cases: Vec<(CoverageState, &str, Option<&str>)> = vec![
        (
            CoverageState::DataMissing,
            "data_missing",
            Some("marker-data-missing"),
        ),
        (CoverageState::Corrupt, "corrupt", Some("marker-corrupt")),
        (CoverageState::Empty, "empty", None),
        (
            CoverageState::Indeterminate,
            "indeterminate",
            Some("marker-indeterminate"),
        ),
        (CoverageState::Unknown("quantum".into()), "unknown", None),
    ];
    for (state, expected_tag, marker) in cases {
        let detail = marker.unwrap_or("");
        // valid: true, honoured: true, errors: [] -- so ONLY this arm can
        // possibly produce a finding; a mislabelled or dropped arm cannot
        // hide behind the generic `engine-invalid` fallback.
        let r = single(state.clone(), true, true, detail);
        match adjudicate(&r) {
            Verdict::Block { findings } => {
                assert_eq!(findings.len(), 1, "{state:?}: {findings:?}");
                assert_eq!(findings[0].state, expected_tag, "{state:?}");
                if let Some(m) = marker {
                    assert!(
                        findings[0].detail.contains(m),
                        "{state:?}: detail {:?} does not carry the partition's own detail",
                        findings[0].detail
                    );
                }
            }
            v => panic!("{state:?} must block, got {v:?}"),
        }
    }
}

#[test]
fn an_engine_error_alone_blocks_and_is_tagged_engine_error() {
    let mut r = report(&[CoverageState::Full], true, true);
    r.errors
        .push("segment .../000000000100.kbak is missing from storage".into());
    match adjudicate(&r) {
        Verdict::Block { findings } => {
            assert_eq!(findings.len(), 1);
            assert_eq!(findings[0].state, "engine-error");
            assert_eq!(
                findings[0].detail,
                "segment .../000000000100.kbak is missing from storage"
            );
            assert_eq!(findings[0].topic, "*");
            assert_eq!(findings[0].partition, -1);
        }
        v => panic!("an engine error must block, got {v:?}"),
    }
}

#[test]
fn an_ignored_lever_is_tagged_lever_ignored() {
    match adjudicate(&report(&[CoverageState::Full], false, true)) {
        Verdict::Block { findings } => assert_eq!(findings[0].state, "lever-ignored"),
        v => panic!("an ignored lever must block, got {v:?}"),
    }
}

#[test]
fn a_bare_invalid_report_blocks_with_the_generic_tag_and_only_then() {
    // (a) nothing else is wrong: the generic fallback fires, and is
    // correctly labelled rather than mimicking a specific ground.
    match adjudicate(&report(&[CoverageState::Full], true, false)) {
        Verdict::Block { findings } => {
            assert_eq!(findings.len(), 1);
            assert_eq!(findings[0].state, "engine-invalid");
        }
        v => panic!("a bare invalid report must still block, got {v:?}"),
    }
    // (b) a specific ground ALSO fired: the fallback must not add a second,
    // vaguer finding on top of the specific one -- proving the generic tag
    // can never mask or duplicate a real ground.
    match adjudicate(&report(&[CoverageState::DataMissing], true, false)) {
        Verdict::Block { findings } => {
            assert_eq!(findings.len(), 1, "{findings:?}");
            assert_eq!(findings[0].state, "data_missing");
        }
        v => panic!("must still block on the specific ground, got {v:?}"),
    }
}

#[test]
fn partial_coverage_is_a_warning_not_a_block() {
    match adjudicate(&report(&[CoverageState::Partial], true, true)) {
        Verdict::Proceed { warnings } => {
            assert_eq!(warnings.len(), 1);
            assert_eq!(warnings[0].state, "header-coverage");
            assert!(
                warnings[0].detail.contains("Partial"),
                "{:?}",
                warnings[0].detail
            );
        }
        v => panic!("Partial coverage is advisory in v0.1, got {v:?}"),
    }
}

#[test]
fn every_ground_is_recorded_when_several_fire_together() {
    let mut r = report(
        &[
            CoverageState::DataMissing,
            CoverageState::Corrupt,
            CoverageState::Empty,
        ],
        false,
        true,
    );
    r.errors.push("segment missing".into());
    match adjudicate(&r) {
        Verdict::Block { findings } => {
            let tags: Vec<&str> = findings.iter().map(|f| f.state.as_str()).collect();
            assert_eq!(
                tags,
                vec![
                    "lever-ignored",
                    "data_missing",
                    "corrupt",
                    "empty",
                    "engine-error"
                ]
            );
        }
        v => panic!("expected Block, got {v:?}"),
    }
}
