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
/// renumber would regenerate one file and sign documents naming another. The
/// CURRENT file is each document's newest MINOR's — since PROD-01.3 the one a
/// document naming a new auth mode carries (scorecard 1.5.0, receipt 1.4.0, on
/// top of PROD-05.1's receipt 1.3.0, which every other receipt this build
/// signs carries); the older files are frozen beside it.
#[test]
fn the_justfile_schema_versions_are_the_writers_constants() {
    assert_eq!(
        justfile_schema_version("scorecard"),
        logweir_core::scorecard::FORMAT_VERSION_WITH_ORIGINAL_NAME
    );
    assert_eq!(
        justfile_schema_version("receipt"),
        logweir_core::backup_receipt::FORMAT_VERSION_WITH_AUTH_MODES
    );
}

/// The schema is checked in, not generated at build time, so a reviewer sees a
/// format change as a diff. This test is the gate (spec §12).
#[test]
fn checked_in_schema_matches_the_types() {
    let generated = logweir_core::schema::scorecard_schema();
    let checked_in = current_schema(
        "drill-scorecard",
        logweir_core::scorecard::FORMAT_VERSION_WITH_ORIGINAL_NAME,
    );
    assert_eq!(
        generated.trim_end(),
        checked_in.trim_end(),
        "schemas/logweir-drill-scorecard-{}.json is stale. \
         Run `just schema` and review the diff — a field added is a MINOR bump, \
         a field removed or retyped is a MAJOR bump (Global Constraint 12).",
        logweir_core::scorecard::FORMAT_VERSION_WITH_ORIGINAL_NAME
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
            logweir_core::scorecard::FORMAT_VERSION_WITH_ORIGINAL_NAME
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

/// FX-8: the 1.1.0 scorecard schema is FROZEN beside the 1.3.0 one, the way
/// FX-4 froze the 1.0.0 one. It still describes every document written before
/// the bump: it names itself 1.1.0, carries FX-4's `not_assessed` and does NOT
/// describe `source.time_basis`. The current schema does, as an OPTIONAL field
/// whose two lists are required inside it, so a document before 1.3.0 without
/// the block still validates against it.
#[test]
fn the_frozen_1_1_0_scorecard_schema_does_not_describe_the_time_basis() {
    let frozen: serde_json::Value = serde_json::from_str(include_str!(
        "../../../schemas/logweir-drill-scorecard-1.1.0.json"
    ))
    .expect("the frozen 1.1.0 scorecard schema parses");
    assert_eq!(
        frozen["$id"],
        "https://logweir.dev/schemas/logweir-drill-scorecard-1.1.0.json"
    );
    assert!(frozen["definitions"]["TopicParity"]["properties"]["not_assessed"].is_object());
    let source = &frozen["definitions"]["SourceInfo"]["properties"];
    assert!(source["backup_id"].is_object());
    assert!(
        source.get("time_basis").is_none(),
        "the frozen 1.1.0 schema must not describe the 1.3.0 field"
    );
    let current: serde_json::Value =
        serde_json::from_str(&logweir_core::schema::scorecard_schema()).unwrap();
    assert_ne!(
        current["$id"], frozen["$id"],
        "the current schema is a NEW file beside the frozen one, never the 1.1.0 file regenerated"
    );
    let source = &current["definitions"]["SourceInfo"];
    assert!(source["properties"]["time_basis"].is_object());
    assert!(
        !source["required"]
            .as_array()
            .expect("SourceInfo has required fields")
            .iter()
            .any(|r| r == "time_basis"),
        "time_basis is OPTIONAL: a document before 1.3.0 without it must still validate"
    );
    let label = &current["definitions"]["TimeBasisLabel"];
    let required: Vec<&str> = label["required"]
        .as_array()
        .expect("TimeBasisLabel has required fields")
        .iter()
        .filter_map(|r| r.as_str())
        .collect();
    assert_eq!(
        required,
        vec!["not_recorded", "producer_time"],
        "both lists are REQUIRED inside the block and `plan` is not: an absent list is never \
         read as empty"
    );
}

/// FX-8: the 1.2.0 scorecard schema (FX-3's) is FROZEN beside the 1.3.0 one.
/// It names itself 1.2.0, carries FX-3's `not_reconstructed` and does NOT
/// describe `source.time_basis`.
#[test]
fn the_frozen_1_2_0_scorecard_schema_does_not_describe_the_time_basis() {
    let frozen: serde_json::Value = serde_json::from_str(include_str!(
        "../../../schemas/logweir-drill-scorecard-1.2.0.json"
    ))
    .expect("the frozen 1.2.0 scorecard schema parses");
    assert_eq!(
        frozen["$id"],
        "https://logweir.dev/schemas/logweir-drill-scorecard-1.2.0.json"
    );
    assert!(frozen["definitions"]["TopicParity"]["properties"]["not_reconstructed"].is_object());
    assert!(
        frozen["definitions"]["SourceInfo"]["properties"]
            .get("time_basis")
            .is_none(),
        "the frozen 1.2.0 schema must not describe the 1.3.0 field"
    );
    let current: serde_json::Value =
        serde_json::from_str(&logweir_core::schema::scorecard_schema()).unwrap();
    assert_ne!(current["$id"], frozen["$id"]);
    assert!(
        current["definitions"]["TopicParity"]["properties"]["not_reconstructed"].is_object(),
        "the current schema keeps FX-3's field"
    );
}

/// PROD-08.1: FX-8's 1.3.0 scorecard schema is FROZEN beside the 1.4.0 one.
/// It names itself 1.3.0, carries FX-8's `source.time_basis` and does NOT
/// describe `integrity.verification`. The current schema does, as an OPTIONAL
/// field (a document before 1.4.0 without it still validates), whose four
/// descriptive strings and two range lists are required inside it and whose
/// `complete` block is optional.
#[test]
fn the_frozen_1_3_0_scorecard_schema_does_not_describe_the_verification() {
    let frozen: serde_json::Value = serde_json::from_str(include_str!(
        "../../../schemas/logweir-drill-scorecard-1.3.0.json"
    ))
    .expect("the frozen 1.3.0 scorecard schema parses");
    assert_eq!(
        frozen["$id"],
        "https://logweir.dev/schemas/logweir-drill-scorecard-1.3.0.json"
    );
    assert!(frozen["definitions"]["SourceInfo"]["properties"]["time_basis"].is_object());
    assert!(
        frozen["definitions"]["Integrity"]["properties"]
            .get("verification")
            .is_none(),
        "the frozen 1.3.0 schema must not describe the 1.4.0 field"
    );
    let current: serde_json::Value =
        serde_json::from_str(&logweir_core::schema::scorecard_schema()).unwrap();
    assert_ne!(current["$id"], frozen["$id"]);
    let integrity = &current["definitions"]["Integrity"];
    assert!(integrity["properties"]["verification"].is_object());
    assert!(
        !integrity["required"]
            .as_array()
            .expect("Integrity has required fields")
            .iter()
            .any(|r| r == "verification"),
        "verification is OPTIONAL: a document before 1.4.0 without it must still validate"
    );
    let required = |def: &str| -> Vec<String> {
        let mut r: Vec<String> = current["definitions"][def]["required"]
            .as_array()
            .unwrap_or_else(|| panic!("{def} has required fields"))
            .iter()
            .filter_map(|r| r.as_str().map(str::to_string))
            .collect();
        r.sort();
        r
    };
    assert_eq!(
        required("Verification"),
        vec![
            "application",
            "comparison_basis",
            "coverage",
            "gaps",
            "header_order",
            "pruned"
        ],
        "an absent coverage or range list is never read as sampled-and-clean; `complete` is \
         optional (present exactly with coverage complete, arm IV-4)"
    );
    assert!(
        required("CompleteVerification").contains(&"covered".to_string())
            && required("CompleteVerification").contains(&"partitions".to_string()),
        "a complete block always says whether it covered every partition, and which"
    );
    assert!(
        current["definitions"]["SourceInfo"]["properties"]["time_basis"].is_object(),
        "the current schema keeps FX-8's field"
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
        logweir_core::scorecard::FORMAT_VERSION_WITH_ORIGINAL_NAME,
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
/// then `diff -u` against the current schema file, which `just schema-check`
/// names). It fails in both directions: a type change nobody regenerated, and a
/// hand-edit of the published file. The CURRENT file is the newest MINOR's,
/// named by the writer's constant (FX-7 fix round): the 1.0.0 file is frozen.
#[test]
fn backup_receipt_schema_has_no_drift() {
    let generated = logweir_core::schema::backup_receipt_schema();
    let checked_in = current_schema(
        "backup-receipt",
        logweir_core::backup_receipt::FORMAT_VERSION_WITH_AUTH_MODES,
    );
    assert_eq!(
        generated.trim_end(),
        checked_in.trim_end(),
        "schemas/logweir-backup-receipt-{}.json is stale. Run `just schema` and \
         review the diff — a field added is a MINOR bump, a field removed or \
         retyped is a MAJOR bump (Global Constraint 12), and the receipt's \
         format_version is its own and not the scorecard's.",
        logweir_core::backup_receipt::FORMAT_VERSION_WITH_AUTH_MODES
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
            logweir_core::backup_receipt::FORMAT_VERSION_WITH_AUTH_MODES
        )
    );
    assert!(
        !v["required"]
            .as_array()
            .expect("the receipt schema has a required array")
            .iter()
            .any(|r| r == "topic_configuration"),
        "topic_configuration is OPTIONAL: every receipt before 1.3.0 lacks it and must still validate"
    );
    assert!(v["definitions"]["TopicConfiguration"]["properties"]["entries"].is_object());
    let archive = &v["definitions"]["ReceiptArchive"];
    assert!(archive["properties"]["manifest_version_id"].is_object());
    assert!(
        !archive["required"]
            .as_array()
            .expect("ReceiptArchive has required fields")
            .iter()
            .any(|r| r == "manifest_version_id"),
        "manifest_version_id is OPTIONAL: every 1.0.0 and 1.1.0 receipt lacks it and must still validate"
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

/// **The 1.0.0 receipt schema is FROZEN** (FX-4, and FX-7's fix round,
/// review M-2) beside the newer ones and still describes every receipt written
/// before FX-4: it names itself 1.0.0, it has no `config_coverage`, and its
/// `ReceiptArchive` has no `manifest_version_id`. It is no longer regenerated
/// (`just schema` writes the current file only), so this is the gate that it
/// stays what adopters downloaded under that name — `docs/stability.md`'s "a
/// new schema file beside the old one".
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
    let archive = &frozen["definitions"]["ReceiptArchive"]["properties"];
    assert!(archive["manifest_sha256"].is_object());
    assert!(
        archive.get("manifest_version_id").is_none(),
        "the frozen 1.0.0 schema must not describe the 1.2.0 field"
    );
    assert_ne!(
        logweir_core::backup_receipt::FORMAT_VERSION_WITH_MANIFEST_VERSION,
        "1.0.0",
        "the pin is a MINOR bump, so its schema is a NEW file"
    );
}

/// **FX-4's 1.1.0 receipt schema is FROZEN** beside the 1.2.0 one (FX-7 merged
/// after FX-4 and took the next MINOR). It still describes every receipt this
/// build writes WITHOUT a pin — the [`RECEIPT_FORMAT_VERSION`] document: it
/// names itself 1.1.0, it carries `config_coverage`, and its `ReceiptArchive`
/// has no `manifest_version_id`. `just schema` no longer regenerates it, so
/// this is the gate that it stays the file FX-4 published.
///
/// [`RECEIPT_FORMAT_VERSION`]: logweir_core::backup_receipt::RECEIPT_FORMAT_VERSION
#[test]
fn the_frozen_1_1_0_receipt_schema_is_still_fx4s() {
    let frozen: serde_json::Value = serde_json::from_str(include_str!(
        "../../../schemas/logweir-backup-receipt-1.1.0.json"
    ))
    .expect("the frozen 1.1.0 receipt schema parses");
    assert_eq!(
        frozen["$id"],
        "https://logweir.dev/schemas/logweir-backup-receipt-1.1.0.json"
    );
    assert!(
        frozen["properties"]["config_coverage"].is_object(),
        "the 1.1.0 schema is FX-4's: it describes config_coverage"
    );
    let archive = &frozen["definitions"]["ReceiptArchive"]["properties"];
    assert!(archive["manifest_sha256"].is_object());
    assert!(
        archive.get("manifest_version_id").is_none(),
        "the frozen 1.1.0 schema must not describe the 1.2.0 field"
    );
    assert_eq!(
        logweir_core::backup_receipt::RECEIPT_FORMAT_VERSION,
        "1.1.0",
        "an unpinned receipt is FX-4's 1.1.0 document, which this file describes"
    );
    assert_ne!(
        logweir_core::backup_receipt::FORMAT_VERSION_WITH_MANIFEST_VERSION,
        logweir_core::backup_receipt::RECEIPT_FORMAT_VERSION,
        "the pin is a MINOR bump over FX-4's 1.1.0, so its schema is a NEW file"
    );
}

/// **PROD-01.3: PROD-08.1's 1.4.0 scorecard schema is FROZEN** beside the
/// 1.5.0 one, and still describes every scorecard of a `plaintext` or
/// `scramSha512` target, which this build writes as 1.4.0: it names itself
/// 1.4.0, it carries PROD-08.1's `integrity.verification`, and its
/// `target.auth.mode` description is the closed set of two. The current file
/// names the five.
#[test]
fn the_frozen_1_4_0_scorecard_schema_is_still_prod_08_1s() {
    let frozen: serde_json::Value = serde_json::from_str(include_str!(
        "../../../schemas/logweir-drill-scorecard-1.4.0.json"
    ))
    .expect("the frozen 1.4.0 scorecard schema parses");
    assert_eq!(
        frozen["$id"],
        "https://logweir.dev/schemas/logweir-drill-scorecard-1.4.0.json"
    );
    assert!(frozen["definitions"]["Integrity"]["properties"]["verification"].is_object());
    let mode = frozen["definitions"]["AuthSummary"]["properties"]["mode"]["description"]
        .as_str()
        .expect("AuthSummary.mode has a description");
    assert!(mode.contains("A CLOSED SET OF TWO"), "{mode}");
    assert!(!mode.contains("mtls"), "{mode}");
    let current: serde_json::Value =
        serde_json::from_str(&logweir_core::schema::scorecard_schema()).unwrap();
    assert_ne!(current["$id"], frozen["$id"]);
    let mode = current["definitions"]["AuthSummary"]["properties"]["mode"]["description"]
        .as_str()
        .expect("AuthSummary.mode has a description");
    assert!(
        mode.contains("from 1.5.0") && mode.contains("mtls"),
        "{mode}"
    );
    assert_eq!(logweir_core::FORMAT_VERSION, "1.4.0");
    assert_eq!(
        logweir_core::scorecard::FORMAT_VERSION_WITH_AUTH_MODES,
        "1.5.0"
    );
}

/// **FX-23: PROD-01.3's 1.5.0 scorecard schema is FROZEN** beside the 1.6.0
/// one, and still describes every scorecard of a PROD-01.3 auth mode that
/// names no unsampled topic, which this build writes as 1.5.0: it names itself
/// 1.5.0, its `target.auth.mode` description is the five, and its `sample`
/// does not describe `unsampled_topics`. The current file does.
#[test]
fn the_frozen_1_5_0_scorecard_schema_does_not_describe_unsampled_topics() {
    let frozen: serde_json::Value = serde_json::from_str(include_str!(
        "../../../schemas/logweir-drill-scorecard-1.5.0.json"
    ))
    .expect("the frozen 1.5.0 scorecard schema parses");
    assert_eq!(
        frozen["$id"],
        "https://logweir.dev/schemas/logweir-drill-scorecard-1.5.0.json"
    );
    let mode = frozen["definitions"]["AuthSummary"]["properties"]["mode"]["description"]
        .as_str()
        .expect("AuthSummary.mode has a description");
    assert!(mode.contains("mtls"), "{mode}");
    assert!(frozen["definitions"]["SampleInfo"]["properties"]["coverage_note"].is_object());
    assert!(
        frozen["definitions"]["SampleInfo"]["properties"]
            .get("unsampled_topics")
            .is_none(),
        "the frozen 1.5.0 schema must not describe the 1.6.0 field"
    );
    let current: serde_json::Value =
        serde_json::from_str(&logweir_core::schema::scorecard_schema()).unwrap();
    assert_ne!(current["$id"], frozen["$id"]);
    assert!(current["definitions"]["SampleInfo"]["properties"]["unsampled_topics"].is_object());
    assert!(
        !current["definitions"]["SampleInfo"]["required"]
            .as_array()
            .expect("SampleInfo has required fields")
            .iter()
            .any(|r| r == "unsampled_topics"),
        "sample.unsampled_topics is OPTIONAL: absent on every document that names none"
    );
    assert_eq!(
        logweir_core::scorecard::FORMAT_VERSION_WITH_UNSAMPLED_TOPICS,
        "1.6.0"
    );
    assert_eq!(logweir_core::scorecard::UNSAMPLED_TOPICS_SINCE_MINOR, 6);
}

/// **PROD-11.1: FX-23's 1.6.0 scorecard schema is FROZEN** beside the 1.7.0
/// one, and still describes every scorecard of a restore that states no
/// replay selection, which this build writes as 1.6.0 (sampled) or 1.4.0/1.5.0
/// (complete): it names itself 1.6.0, describes `sample.unsampled_topics`, and
/// its `source` does not describe `selection`. The current file does, as an
/// optional block.
#[test]
fn the_frozen_1_6_0_scorecard_schema_does_not_describe_the_selection() {
    let frozen: serde_json::Value = serde_json::from_str(include_str!(
        "../../../schemas/logweir-drill-scorecard-1.6.0.json"
    ))
    .expect("the frozen 1.6.0 scorecard schema parses");
    assert_eq!(
        frozen["$id"],
        "https://logweir.dev/schemas/logweir-drill-scorecard-1.6.0.json"
    );
    assert!(frozen["definitions"]["SampleInfo"]["properties"]["unsampled_topics"].is_object());
    assert!(
        frozen["definitions"]["SourceInfo"]["properties"]
            .get("selection")
            .is_none(),
        "the frozen 1.6.0 schema must not describe the 1.7.0 block"
    );
    let current: serde_json::Value =
        serde_json::from_str(&logweir_core::schema::scorecard_schema()).unwrap();
    assert_ne!(current["$id"], frozen["$id"]);
    assert!(current["definitions"]["SourceInfo"]["properties"]["selection"].is_object());
    assert!(current["definitions"]["SelectionLabel"].is_object());
    assert!(
        !current["definitions"]["SourceInfo"]["required"]
            .as_array()
            .expect("SourceInfo has required fields")
            .iter()
            .any(|r| r == "selection"),
        "source.selection is OPTIONAL: absent on every document that states no selection"
    );
    assert_eq!(
        logweir_core::scorecard::FORMAT_VERSION_WITH_SELECTION,
        "1.7.0"
    );
    assert_eq!(logweir_core::scorecard::SELECTION_SINCE_MINOR, 7);
}

/// **PROD-15.1: PROD-11.1's 1.7.0 scorecard schema is FROZEN** beside the
/// 1.8.0 one, and still describes every scorecard of a restore that states a
/// window start and is not an original-name restore, which this build writes
/// as 1.7.0: it names itself 1.7.0, describes `source.selection`, and its
/// `target` does not describe `original_name`. The current file does, as an
/// optional block.
#[test]
fn the_frozen_1_7_0_scorecard_schema_does_not_describe_the_original_name() {
    let frozen: serde_json::Value = serde_json::from_str(include_str!(
        "../../../schemas/logweir-drill-scorecard-1.7.0.json"
    ))
    .expect("the frozen 1.7.0 scorecard schema parses");
    assert_eq!(
        frozen["$id"],
        "https://logweir.dev/schemas/logweir-drill-scorecard-1.7.0.json"
    );
    assert!(frozen["definitions"]["SourceInfo"]["properties"]["selection"].is_object());
    assert!(
        frozen["definitions"]["TargetInfo"]["properties"]
            .get("original_name")
            .is_none(),
        "the frozen 1.7.0 schema must not describe the 1.8.0 block"
    );
    let current: serde_json::Value =
        serde_json::from_str(&logweir_core::schema::scorecard_schema()).unwrap();
    assert_ne!(current["$id"], frozen["$id"]);
    assert!(current["definitions"]["TargetInfo"]["properties"]["original_name"].is_object());
    assert!(current["definitions"]["OriginalNameInfo"].is_object());
    assert!(
        !current["definitions"]["TargetInfo"]["required"]
            .as_array()
            .expect("TargetInfo has required fields")
            .iter()
            .any(|r| r == "original_name"),
        "target.original_name is OPTIONAL: absent on every document that is not an \
         original-name restore"
    );
    assert_eq!(
        logweir_core::scorecard::FORMAT_VERSION_WITH_ORIGINAL_NAME,
        "1.8.0"
    );
    assert_eq!(logweir_core::scorecard::ORIGINAL_NAME_SINCE_MINOR, 8);
}

/// **FX-7's 1.2.0 receipt schema is FROZEN** beside PROD-05.1's 1.3.0 one. It
/// still describes every PINNED receipt written before PROD-05.1: it names
/// itself 1.2.0, carries `config_coverage` and `archive.manifest_version_id`,
/// and does NOT describe `topic_configuration`. `just schema` no longer
/// regenerates it, so this is the gate that it stays the file FX-7 published.
#[test]
fn the_frozen_1_2_0_receipt_schema_is_still_fx7s() {
    let frozen: serde_json::Value = serde_json::from_str(include_str!(
        "../../../schemas/logweir-backup-receipt-1.2.0.json"
    ))
    .expect("the frozen 1.2.0 receipt schema parses");
    assert_eq!(
        frozen["$id"],
        "https://logweir.dev/schemas/logweir-backup-receipt-1.2.0.json"
    );
    assert!(frozen["properties"]["config_coverage"].is_object());
    assert!(
        frozen["definitions"]["ReceiptArchive"]["properties"]["manifest_version_id"].is_object()
    );
    assert!(
        frozen["properties"].get("topic_configuration").is_none(),
        "the frozen 1.2.0 schema must not describe the 1.3.0 field"
    );
    let current: serde_json::Value =
        serde_json::from_str(&logweir_core::schema::backup_receipt_schema()).unwrap();
    assert_ne!(
        current["$id"], frozen["$id"],
        "the current schema is a NEW file beside the frozen one, never the 1.2.0 file regenerated"
    );
    assert!(current["properties"]["topic_configuration"].is_object());
}

/// **PROD-01.3: PROD-05.1's 1.3.0 receipt schema is FROZEN** beside the 1.4.0
/// one, and still describes every receipt of a `plaintext` or `scramSha512`
/// backup, which this build writes as 1.3.0: it names itself 1.3.0, carries
/// `topic_configuration`, and its `source.auth.mode` description is the closed
/// set of two. The current file names the five, from 1.4.0.
#[test]
fn the_frozen_1_3_0_receipt_schema_is_still_prod_05_1s() {
    let frozen: serde_json::Value = serde_json::from_str(include_str!(
        "../../../schemas/logweir-backup-receipt-1.3.0.json"
    ))
    .expect("the frozen 1.3.0 receipt schema parses");
    assert_eq!(
        frozen["$id"],
        "https://logweir.dev/schemas/logweir-backup-receipt-1.3.0.json"
    );
    assert!(frozen["properties"]["topic_configuration"].is_object());
    let mode = frozen["definitions"]["ReceiptAuth"]["properties"]["mode"]["description"]
        .as_str()
        .expect("ReceiptAuth.mode has a description");
    assert!(mode.contains("A CLOSED SET OF TWO"), "{mode}");
    let current: serde_json::Value =
        serde_json::from_str(&logweir_core::schema::backup_receipt_schema()).unwrap();
    assert_ne!(current["$id"], frozen["$id"]);
    assert!(
        current["properties"]["topic_configuration"].is_object(),
        "the current file keeps PROD-05.1's field"
    );
    let mode = current["definitions"]["ReceiptAuth"]["properties"]["mode"]["description"]
        .as_str()
        .expect("ReceiptAuth.mode has a description");
    assert!(
        mode.contains("from 1.4.0") && mode.contains("mtls"),
        "{mode}"
    );
    assert_eq!(
        logweir_core::backup_receipt::FORMAT_VERSION_WITH_TOPIC_CONFIGURATION,
        "1.3.0"
    );
    assert_eq!(
        logweir_core::backup_receipt::FORMAT_VERSION_WITH_AUTH_MODES,
        "1.4.0"
    );
}
