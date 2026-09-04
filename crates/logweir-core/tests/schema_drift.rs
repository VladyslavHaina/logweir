/// The schema is checked in, not generated at build time, so a reviewer sees a
/// format change as a diff. This test is the gate (spec §12).
#[test]
fn checked_in_schema_matches_the_types() {
    let generated = logweir_core::schema::scorecard_schema();
    let checked_in = include_str!("../../../schemas/logweir-drill-scorecard-1.0.0.json");
    assert_eq!(
        generated.trim_end(),
        checked_in.trim_end(),
        "schemas/logweir-drill-scorecard-1.0.0.json is stale. \
         Run `just schema` and review the diff — a field added is a MINOR bump, \
         a field removed or retyped is a MAJOR bump (Global Constraint 12)."
    );
}

#[test]
fn schema_declares_the_format_version_const() {
    let v: serde_json::Value =
        serde_json::from_str(&logweir_core::schema::scorecard_schema()).unwrap();
    assert_eq!(v["title"], "Scorecard");
    assert!(v["properties"]["format_version"].is_object());
}
