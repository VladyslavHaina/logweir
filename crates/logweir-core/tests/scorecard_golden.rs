use logweir_core::det_json::to_deterministic_json;
use logweir_core::scorecard::Scorecard;

/// The fixture is the specification's example scorecard with its `//`
/// annotations stripped. Round-tripping it byte-for-byte is what makes the
/// format the public API: any field rename, reorder or default change turns
/// this test red.
#[test]
fn spec_example_round_trips_byte_for_byte() {
    let raw = include_str!("../../../e2e/fixtures/scorecard-pass.json");
    let parsed: Scorecard = serde_json::from_str(raw).expect("fixture parses");
    let out = to_deterministic_json(&parsed).expect("serialises");
    assert_eq!(
        String::from_utf8(out).unwrap().trim_end(),
        raw.trim_end(),
        "deterministic serialisation must reproduce the fixture exactly"
    );
}

#[test]
fn unknown_fields_are_ignored_not_rejected() {
    let raw = include_str!("../../../e2e/fixtures/scorecard-pass.json");
    let mut v: serde_json::Value = serde_json::from_str(raw).unwrap();
    v.as_object_mut()
        .unwrap()
        .insert("a_future_minor_field".into(), serde_json::json!(42));
    let parsed: Result<Scorecard, _> = serde_json::from_value(v);
    assert!(
        parsed.is_ok(),
        "readers must ignore unknown fields (Global Constraint 12)"
    );
}

#[test]
fn integrity_partial_requires_a_reason() {
    let raw = include_str!("../../../e2e/fixtures/scorecard-pass.json");
    let mut sc: Scorecard = serde_json::from_str(raw).unwrap();
    sc.integrity.result = logweir_core::outcome::IntegrityResult::Partial;
    sc.integrity.partial_reason = None;
    assert!(
        sc.validate_invariants().is_err(),
        "partial without a reason is invalid"
    );
}

#[test]
fn source_relative_rpo_and_its_reason_are_mutually_exclusive() {
    let raw = include_str!("../../../e2e/fixtures/scorecard-pass.json");
    let mut sc: Scorecard = serde_json::from_str(raw).unwrap();
    sc.source.captured_by_logweir = true;
    // reason is still Some(...) while captured_by_logweir is true -> invalid
    assert!(sc.validate_invariants().is_err());
}

/// **An older reader ignores a block a later minor adds inside `target`.**
/// `target.schemas` (1.13.0, 2.5.0) is nested, so the top-level row above does
/// not cover it: a reader built before 1.13.0 meets it as an unknown field of
/// `target`. Modelled by an unknown block there; the mutant this kills is
/// `deny_unknown_fields` on `TargetInfo`. Measured on the real older readers
/// too: `docs/formats/drill-scorecard.md`, *What an older reader does*, row
/// 1.13.0, 2.5.0.
#[test]
fn a_block_a_later_minor_adds_inside_target_is_ignored() {
    let raw = include_str!("../../../e2e/fixtures/scorecard-pass.json");
    let mut v: serde_json::Value = serde_json::from_str(raw).unwrap();
    v["target"]["a_block_a_later_minor_adds"] = serde_json::json!({"not_imported": "noSnapshot"});
    let parsed: Scorecard = serde_json::from_value(v).expect("an unknown block is ignored");
    assert!(parsed.target.schemas.is_none());
    assert!(parsed.validate_invariants().is_ok());
    // CONTROL: the block under its own name is read.
    let mut known: serde_json::Value = serde_json::from_str(raw).unwrap();
    known["format_version"] = serde_json::json!("1.13.0");
    known["target"]["schemas"] = serde_json::json!({"not_imported": "noSnapshot"});
    let read: Scorecard = serde_json::from_value(known).unwrap();
    assert!(read.target.schemas.is_some());
}
