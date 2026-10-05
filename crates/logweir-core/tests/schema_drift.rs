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

/// **One version per document.** `just schema`/`schema-check` write and
/// compare the file their justfile variable names; the writer's constant names
/// the `$id` and the `format_version` it writes. They must be one number, or a
/// renumber would regenerate one file and sign documents naming another.
#[test]
fn the_justfile_schema_versions_are_the_writers_constants() {
    assert_eq!(
        justfile_schema_version("scorecard"),
        logweir_core::FORMAT_VERSION
    );
    assert_eq!(
        justfile_schema_version("receipt"),
        logweir_core::backup_receipt::RECEIPT_FORMAT_VERSION
    );
}

/// The schema is checked in, not generated at build time, so a reviewer sees a
/// format change as a diff. This test is the gate (spec §12).
#[test]
fn checked_in_schema_matches_the_types() {
    let generated = logweir_core::schema::scorecard_schema();
    let checked_in = current_schema("drill-scorecard", logweir_core::FORMAT_VERSION);
    assert_eq!(
        generated.trim_end(),
        checked_in.trim_end(),
        "schemas/logweir-drill-scorecard-{}.json is stale. \
         Run `just schema` and review the diff — a field added is a MINOR bump, \
         a field removed or retyped is a MAJOR bump (Global Constraint 12).",
        logweir_core::FORMAT_VERSION
    );
}

/// FX-4: the 1.0.0 scorecard schema is FROZEN beside the 1.1.0 one, and still
/// describes the documents written before the bump — `docs/stability.md`'s "a
/// new schema file beside the old one". It is no longer regenerated, so this is
/// the gate that it stays what it was: it names itself 1.0.0 and it has neither
/// `topic_parity.not_assessed` nor `target_diff.not_assessed`.
#[test]
fn the_frozen_1_0_0_scorecard_schema_is_still_the_1_0_0_schema() {
    let frozen: serde_json::Value = serde_json::from_str(include_str!(
        "../../../schemas/logweir-drill-scorecard-1.0.0.json"
    ))
    .expect("the frozen 1.0.0 scorecard schema parses");
    assert_eq!(
        frozen["$id"],
        "https://logweir.dev/schemas/logweir-drill-scorecard-1.0.0.json"
    );
    let parity = &frozen["definitions"]["TopicParity"]["properties"];
    assert!(parity["intentionally_deviated"].is_object());
    assert!(
        parity.get("not_assessed").is_none(),
        "the frozen 1.0.0 schema must not describe the 1.1.0 field"
    );
    let diff = &frozen["definitions"]["TargetDiffSummary"]["properties"];
    assert!(diff["collisions"].is_object());
    assert!(
        diff.get("not_assessed").is_none(),
        "the frozen 1.0.0 schema must not describe the 1.1.0 target_diff field"
    );
    let current: serde_json::Value =
        serde_json::from_str(&logweir_core::schema::scorecard_schema()).unwrap();
    assert_eq!(
        current["$id"],
        format!(
            "https://logweir.dev/schemas/logweir-drill-scorecard-{}.json",
            logweir_core::FORMAT_VERSION
        )
    );
    let parity = &current["definitions"]["TopicParity"];
    assert!(parity["properties"]["not_assessed"].is_object());
    assert!(
        !parity["required"]
            .as_array()
            .expect("TopicParity has required fields")
            .iter()
            .any(|r| r == "not_assessed"),
        "not_assessed is OPTIONAL: a 1.0.0 document without it must still validate"
    );
    let diff = &current["definitions"]["TargetDiffSummary"];
    assert!(diff["properties"]["not_assessed"].is_object());
    assert!(
        !diff["required"]
            .as_array()
            .expect("TargetDiffSummary has required fields")
            .iter()
            .any(|r| r == "not_assessed"),
        "target_diff.not_assessed is OPTIONAL: a 1.0.0 document without it must still validate"
    );
}

/// FX-3: the 1.1.0 scorecard schema is FROZEN beside the 1.2.0 one, the way
/// FX-4 froze the 1.0.0 one. It still describes every document written between
/// the two bumps: it names itself 1.1.0, carries FX-4's two `not_assessed`
/// fields and does NOT describe `topic_parity.not_reconstructed`. The current
/// schema does, as an OPTIONAL field, so a 1.0.0 or 1.1.0 document without it
/// still validates against it.
#[test]
fn the_frozen_1_1_0_scorecard_schema_is_still_the_1_1_0_schema() {
    let frozen: serde_json::Value = serde_json::from_str(include_str!(
        "../../../schemas/logweir-drill-scorecard-1.1.0.json"
    ))
    .expect("the frozen 1.1.0 scorecard schema parses");
    assert_eq!(
        frozen["$id"],
        "https://logweir.dev/schemas/logweir-drill-scorecard-1.1.0.json"
    );
    let parity = &frozen["definitions"]["TopicParity"]["properties"];
    assert!(parity["not_assessed"].is_object());
    assert!(
        parity.get("not_reconstructed").is_none(),
        "the frozen 1.1.0 schema must not describe the 1.2.0 field"
    );
    let current: serde_json::Value =
        serde_json::from_str(&logweir_core::schema::scorecard_schema()).unwrap();
    assert_ne!(
        current["$id"], frozen["$id"],
        "the current schema is a NEW file beside the frozen one, never the 1.1.0 file regenerated"
    );
    let parity = &current["definitions"]["TopicParity"];
    assert!(parity["properties"]["not_reconstructed"].is_object());
    assert!(
        !parity["required"]
            .as_array()
            .expect("TopicParity has required fields")
            .iter()
            .any(|r| r == "not_reconstructed"),
        "not_reconstructed is OPTIONAL: a document before 1.2.0 without it must still validate"
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
    let frozen: serde_json::Value = serde_json::from_str(include_str!(
        "../../../schemas/logweir-drill-scorecard-1.0.0.json"
    ))
    .expect("the frozen 1.0.0 scorecard schema parses");
    let frozen_1_1: serde_json::Value = serde_json::from_str(include_str!(
        "../../../schemas/logweir-drill-scorecard-1.1.0.json"
    ))
    .expect("the frozen 1.1.0 scorecard schema parses");
    let checked_in: serde_json::Value = serde_json::from_str(&current_schema(
        "drill-scorecard",
        logweir_core::FORMAT_VERSION,
    ))
    .expect("the checked-in scorecard schema parses");
    let generated: serde_json::Value =
        serde_json::from_str(&logweir_core::schema::scorecard_schema())
            .expect("the generated scorecard schema parses");
    for (source, v) in [
        ("the frozen 1.0.0 file", frozen),
        ("the frozen 1.1.0 file", frozen_1_1),
        ("the checked-in current file", checked_in),
        ("the type", generated),
    ] {
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
    let checked_in = current_schema(
        "backup-receipt",
        logweir_core::backup_receipt::RECEIPT_FORMAT_VERSION,
    );
    assert_eq!(
        generated.trim_end(),
        checked_in.trim_end(),
        "schemas/logweir-backup-receipt-{}.json is stale. Run `just schema` and \
         review the diff — a field added is a MINOR bump, a field removed or \
         retyped is a MAJOR bump (Global Constraint 12), and the receipt's \
         format_version is its own and not the scorecard's.",
        logweir_core::backup_receipt::RECEIPT_FORMAT_VERSION
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
            logweir_core::backup_receipt::RECEIPT_FORMAT_VERSION
        )
    );
    assert!(
        !v["required"]
            .as_array()
            .expect("the receipt schema has a required array")
            .iter()
            .any(|r| r == "config_coverage"),
        "config_coverage is OPTIONAL: every 1.0.0 receipt lacks it and must still validate"
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

/// FX-4: the 1.0.0 receipt schema is FROZEN beside the 1.1.0 one and still
/// describes every receipt written before the bump: it names itself 1.0.0 and
/// has no `config_coverage`.
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
    assert!(frozen["properties"]["records"].is_object());
    assert!(
        frozen["properties"].get("config_coverage").is_none(),
        "the frozen 1.0.0 schema must not describe the 1.1.0 field"
    );
}
