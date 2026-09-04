use logweir_core::det_json::to_deterministic_json;
use logweir_core::scorecard::Scorecard;

/// The fixture is the spec §6.1 example with its `//` annotations stripped.
/// Round-tripping it byte-for-byte is what makes the format the public API:
/// any field rename, reorder or default change turns this test red.
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
    // reason is still Some(...) while captured_by_logweir is true -> invalid (spec §9.3 phase 8)
    assert!(sc.validate_invariants().is_err());
}
