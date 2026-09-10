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

/// Global Constraint 12, as amended: the scorecard stays frozen at
/// `format_version 1.0.0` with **21 top-level properties and 17 required
/// ones**. New top-level information ships as a NEW DOCUMENT — the backup
/// receipt, at its own `format_version` — and never as a new scorecard field.
///
/// # A SHAPE assertion, deliberately, and not a byte diff against a commit
///
/// GC12 as amended permits tag 1 to add NESTED OPTIONAL fields (`target.auth`,
/// `evidence.offset_report_key`/`_sha256`), each paying the full price the
/// constraint sets. Those change the schema's bytes and must not change these
/// two counts. A `diff` against a frozen blob here would go red the first time
/// a sanctioned nested field landed, and the cheapest way to make it green
/// again would be to delete the check — so the check would be gone at exactly
/// the moment it started mattering. `checked_in_schema_matches_the_types`
/// above is the byte-level gate, against the TYPES rather than against a
/// commit; this is the gate on the FREEZE.
///
/// # Both the GENERATED schema and the CHECKED-IN file
///
/// Two sources, one assertion, because each alone leaves a step in which a new
/// top-level field exists and nothing has said so. Against the checked-in file
/// only, adding a field to `Scorecard` fails `checked_in_schema_matches_the_
/// types` (stale) and this test still passes; the obvious next move is `just
/// schema`, and only THEN does the freeze complain. Against the generated
/// schema only, a hand-edit of the published file goes unnoticed here. Checked
/// together, a field added to the type fails this test in the same run that
/// adds it, at assertion time, with the count in the message.
#[test]
fn the_scorecard_top_level_shape_is_unchanged() {
    let checked_in: serde_json::Value = serde_json::from_str(include_str!(
        "../../../schemas/logweir-drill-scorecard-1.0.0.json"
    ))
    .expect("the checked-in scorecard schema parses");
    let generated: serde_json::Value =
        serde_json::from_str(&logweir_core::schema::scorecard_schema())
            .expect("the generated scorecard schema parses");
    for (source, v) in [("the checked-in file", checked_in), ("the type", generated)] {
        let properties = v["properties"]
            .as_object()
            .expect("the scorecard schema has a top-level `properties` object");
        let required = v["required"]
            .as_array()
            .expect("the scorecard schema has a top-level `required` array");
        assert_eq!(
            (properties.len(), required.len()),
            (21, 17),
            "the scorecard is FROZEN at 21 top-level properties and 17 required ones \
             (Global Constraint 12 as amended), and {source} disagrees. Top-level \
             information about something the scorecard did not observe is a NEW \
             DOCUMENT, not a new field here — \
             `schemas/logweir-backup-receipt-1.0.0.json` is the worked example. \
             Nested optional fields are permitted and do not change these counts.\n  \
             properties: {:?}\n  required:   {:?}",
            properties.keys().collect::<Vec<_>>(),
            required
        );
    }
}

/// The checked-in backup-receipt schema is EXACTLY what the generator emits.
///
/// The named test behind the CI drift arm and behind the by-hand acceptance
/// (`cargo run -p logweir-core --example emit_backup_receipt_schema > /tmp/r.json`
/// then `diff -u schemas/logweir-backup-receipt-1.0.0.json /tmp/r.json`). It
/// fails in both directions: a type change nobody regenerated, and a
/// hand-edit of the published file.
#[test]
fn backup_receipt_schema_has_no_drift() {
    let generated = logweir_core::schema::backup_receipt_schema();
    let checked_in = include_str!("../../../schemas/logweir-backup-receipt-1.0.0.json");
    assert_eq!(
        generated.trim_end(),
        checked_in.trim_end(),
        "schemas/logweir-backup-receipt-1.0.0.json is stale. Run `just schema` and \
         review the diff — a field added is a MINOR bump, a field removed or \
         retyped is a MAJOR bump (Global Constraint 12), and the receipt's \
         format_version is its own and not the scorecard's."
    );
}

/// The receipt schema names itself, pins its major with a pattern, and types
/// the covered window as two INTEGERS.
///
/// The pattern is the half of Global Constraint 12 a schema-only validator
/// can enforce: without it a `"9.9.9"` document validated cleanly against a
/// file named `…-1.0.0.json`. The integer types are interface **I22** — the
/// operator's `Backup.status.windowCovered{fromMs,toMs}` mirrors this shape as
/// two `int64`s, and a status subresource has no date-time type to mirror a
/// string into.
#[test]
fn backup_receipt_schema_pins_its_major_and_types_the_window_as_integers() {
    let v: serde_json::Value =
        serde_json::from_str(&logweir_core::schema::backup_receipt_schema()).unwrap();
    assert_eq!(v["title"], "BackupReceipt");
    assert_eq!(
        v["$id"],
        "https://logweir.dev/schemas/logweir-backup-receipt-1.0.0.json"
    );
    assert_eq!(
        v["properties"]["format_version"]["pattern"], r"^1\.[0-9]+\.[0-9]+$",
        "a schema-only validator is the one route that does not go through \
         validate_invariants; without this pattern it accepts a 9.9.9 document"
    );
    let covered = &v["definitions"]["ReceiptCovered"]["properties"];
    for field in ["from_ms", "to_ms"] {
        assert_eq!(
            covered[field]["type"], "integer",
            "covered.{field} is EPOCH MILLISECONDS (I22); an RFC 3339 string here \
             would need a conversion nobody owns on the operator's side"
        );
        assert_eq!(covered[field]["format"], "int64");
    }
}
