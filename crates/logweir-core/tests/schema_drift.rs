/// The checked-in schema file of the CURRENT version of `name`, whose version
/// is the writer's own constant (FX-4): a renumber moves the constant and
/// `just schema`, and every test below follows it.
fn current_schema(name: &str, version: &str) -> String {
    let rel = format!("schemas/logweir-{name}-{version}.json");
    std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join(&rel),
    )
    .unwrap_or_else(|e| panic!("{rel}: {e}"))
}

/// The justfile's `<name>_schema_version := "X"` value.
fn justfile_schema_version(name: &str) -> String {
    let justfile = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../justfile"),
    )
    .expect("read the justfile");
    let prefix = format!("{name}_schema_version := \"");
    justfile
        .lines()
        .find_map(|l| l.strip_prefix(prefix.as_str()))
        .and_then(|r| r.strip_suffix('"'))
        .unwrap_or_else(|| panic!("the justfile declares no {name}_schema_version"))
        .to_string()
}

/// **One version per document** (FX-7 fix round, review M-2). `just
/// schema`/`schema-check` write and compare the file their justfile variable
/// names; the writer's constant names the `$id` and the `format_version` a
/// pinned receipt carries. They must be one number, or a renumber would
/// regenerate one file and sign documents naming another.
#[test]
fn the_justfile_schema_versions_are_the_writers_constants() {
    assert_eq!(
        justfile_schema_version("receipt"),
        logweir_core::backup_receipt::FORMAT_VERSION_WITH_MANIFEST_VERSION
    );
}

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
/// then `diff -u` against the current schema file, which `just schema-check`
/// names). It fails in both directions: a type change nobody regenerated, and a
/// hand-edit of the published file. The CURRENT file is the newest MINOR's,
/// named by the writer's constant (FX-7 fix round): the 1.0.0 file is frozen.
#[test]
fn backup_receipt_schema_has_no_drift() {
    let generated = logweir_core::schema::backup_receipt_schema();
    let checked_in = current_schema(
        "backup-receipt",
        logweir_core::backup_receipt::FORMAT_VERSION_WITH_MANIFEST_VERSION,
    );
    assert_eq!(
        generated.trim_end(),
        checked_in.trim_end(),
        "schemas/logweir-backup-receipt-{}.json is stale. Run `just schema` and \
         review the diff — a field added is a MINOR bump, a field removed or \
         retyped is a MAJOR bump (Global Constraint 12), and the receipt's \
         format_version is its own and not the scorecard's.",
        logweir_core::backup_receipt::FORMAT_VERSION_WITH_MANIFEST_VERSION
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
        format!(
            "https://logweir.dev/schemas/logweir-backup-receipt-{}.json",
            logweir_core::backup_receipt::FORMAT_VERSION_WITH_MANIFEST_VERSION
        )
    );
    let archive = &v["definitions"]["ReceiptArchive"];
    assert!(archive["properties"]["manifest_version_id"].is_object());
    assert!(
        !archive["required"]
            .as_array()
            .expect("ReceiptArchive has required fields")
            .iter()
            .any(|r| r == "manifest_version_id"),
        "manifest_version_id is OPTIONAL: every 1.0.0 receipt lacks it and must still validate"
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

/// **FX-7 fix round (review M-2): the 1.0.0 receipt schema is FROZEN** beside
/// the current one and still describes every receipt written before the pin:
/// it names itself 1.0.0 and its `ReceiptArchive` has no
/// `manifest_version_id`. It is no longer regenerated (`just schema` writes
/// the current file only), so this is the gate that it stays what adopters
/// downloaded under that name — `docs/stability.md`'s "a new schema file
/// beside the old one".
#[test]
fn the_frozen_1_0_0_receipt_schema_is_still_the_1_0_0_schema() {
    let frozen: serde_json::Value = serde_json::from_str(include_str!(
        "../../../schemas/logweir-backup-receipt-1.0.0.json"
    ))
    .expect("the frozen 1.0.0 receipt schema parses");
    assert_eq!(
        frozen["$id"],
        "https://logweir.dev/schemas/logweir-backup-receipt-1.0.0.json"
    );
    let archive = &frozen["definitions"]["ReceiptArchive"]["properties"];
    assert!(archive["manifest_sha256"].is_object());
    assert!(
        archive.get("manifest_version_id").is_none(),
        "the frozen 1.0.0 schema must not describe the newer MINOR's field"
    );
    assert_ne!(
        logweir_core::backup_receipt::FORMAT_VERSION_WITH_MANIFEST_VERSION,
        "1.0.0",
        "the pin is a MINOR bump, so its schema is a NEW file"
    );
}
