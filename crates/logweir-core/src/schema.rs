use crate::backup_receipt::BackupReceipt;
use crate::scorecard::Scorecard;

/// Pretty-printed JSON Schema for the scorecard. `$id` pins the published
/// URL so a downloaded scorecard names the schema that validates it.
///
/// Each MINOR is a new file, the older ones FROZEN beside it and never
/// regenerated (`docs/stability.md`): 1.1.0 `topic_parity.not_assessed`, 1.2.0
/// `topic_parity.not_reconstructed`, 1.3.0 `source.time_basis`, 1.4.0
/// `integrity.verification`, 1.5.0 three more `target.auth.mode` values, 1.6.0
/// `sample.unsampled_topics` (every SAMPLED scorecard; a complete one is still
/// 1.4.0 or 1.5.0), 1.7.0 `source.selection` (a narrowed restore).
///
/// **2.0.0**, the format's first MAJOR, is written ONLY for a restore that
/// states a partition subset: 1.7.0's fields, with `source.selection` REQUIRED
/// and its `partitions` (a non-empty list of non-empty lists) and `engine_runs`
/// (at least one) required in it, and `format_version` pinned to `2.x.y`. The
/// Rust type reads both majors, so the requirements that make a document
/// 2.0.0's are added here, on top of what the type derives.
///
/// **2.1.0**, this file, adds `approval.console` (a partition-subset restore a
/// second person approved in the console): 2.0.0's fields plus that optional
/// block and its two definitions, exactly as the type derives them; the frozen
/// 2.0.0 file is this file without the block
/// (`the_frozen_2_0_0_scorecard_schema_is_the_2_1_0_one_without_the_console_approval`).
/// The `$id` is built from
/// [`crate::scorecard::FORMAT_VERSION_SUBSET_WITH_CONSOLE_APPROVAL`].
///
/// **2.2.0** (PROD-04.2) adds `integrity.verification.positions_mapping` the
/// same way; the 2.1.0 file is frozen beside it, and the `$id` is built from
/// [`crate::scorecard::FORMAT_VERSION_SUBSET_WITH_POSITIONS_MAPPING`].
///
/// **`target.original_name` is NOT in this file, on purpose**: an original-name
/// restore restores whole topics (arm ON-14), so the block never appears in a
/// 2.x document. It is format 1's, from 1.8.0, and
/// [`scorecard_format_1_schema`] writes format 1's newest file.
pub fn scorecard_schema() -> String {
    use schemars::schema::{Schema, SchemaObject};
    let settings = schemars::gen::SchemaSettings::draft07().with(|s| {
        s.option_nullable = true;
        s.option_add_null_type = false;
    });
    let mut root = settings
        .into_generator()
        .into_root_schema_for::<Scorecard>();
    root.schema.metadata().id = Some(format!(
        "https://logweir.dev/schemas/logweir-drill-scorecard-{}.json",
        crate::scorecard::FORMAT_VERSION_SUBSET_WITH_POSITIONS_MAPPING
    ));
    fn def<'a>(
        definitions: &'a mut schemars::Map<String, Schema>,
        name: &str,
    ) -> &'a mut SchemaObject {
        match definitions.get_mut(name) {
            Some(Schema::Object(o)) => o,
            _ => panic!("the scorecard schema defines {name}"),
        }
    }
    fn property<'a>(o: &'a mut SchemaObject, name: &str) -> &'a mut SchemaObject {
        match o.object().properties.get_mut(name) {
            Some(Schema::Object(p)) => p,
            _ => panic!("the scorecard schema's object has a {name} property"),
        }
    }
    // `target.original_name` is format 1's (1.8.0) and never a
    // 2.x document's, so this file does not describe it (see the note above).
    let target = def(&mut root.definitions, "TargetInfo");
    assert!(
        target
            .object()
            .properties
            .remove(ORIGINAL_NAME_PROPERTY)
            .is_some(),
        "the scorecard type derives target.original_name"
    );
    for name in ORIGINAL_NAME_DEFINITIONS {
        assert!(
            root.definitions.remove(name).is_some(),
            "the scorecard type derives {name}"
        );
    }
    // A 2.0.0 document is a partition-subset restore's: it carries the
    // selection block (arm PS-1), with its subsets and its engine runs.
    let source = def(&mut root.definitions, "SourceInfo");
    source.object().required.insert("selection".into());
    property(source, "selection").extensions.remove("nullable");
    let selection = def(&mut root.definitions, "SelectionLabel");
    for name in ["partitions", "engine_runs"] {
        selection.object().required.insert(name.into());
        property(selection, name).extensions.remove("nullable");
    }
    property(selection, "partitions").array().min_items = Some(1);
    property(selection, "engine_runs").number().minimum = Some(1.0);
    // PS-3 in the schema too: each list non-empty, distinct and
    // not negative, and the topic not empty. Ascending order is the readers'.
    let topic = def(&mut root.definitions, "TopicPartitions");
    property(topic, "topic").string().min_length = Some(1);
    let partitions = property(topic, "partitions").array();
    partitions.min_items = Some(1);
    partitions.unique_items = Some(true);
    match partitions.items.as_mut() {
        Some(schemars::schema::SingleOrVec::Single(item)) => match item.as_mut() {
            Schema::Object(o) => o.number().minimum = Some(0.0),
            Schema::Bool(_) => panic!("TopicPartitions.partitions' items are a schema object"),
        },
        _ => panic!("TopicPartitions.partitions has one item schema"),
    }
    let mut out = serde_json::to_string_pretty(&root).expect("schema serialises");
    out.push('\n');
    out
}

/// `TargetInfo`'s property for the `original_name` block.
const ORIGINAL_NAME_PROPERTY: &str = "original_name";

/// The two definitions the `original_name` block brings with it.
const ORIGINAL_NAME_DEFINITIONS: [&str; 2] = ["OriginalNameInfo", "OriginalNameOwner"];

/// `ApprovalInfo`'s property for the `approval.console` block.
const CONSOLE_APPROVAL_PROPERTY: &str = "console";

/// The two definitions the `approval.console` block brings with it.
const CONSOLE_APPROVAL_DEFINITIONS: [&str; 2] = ["ConsoleApprovalInfo", "ConsolePrincipal"];

/// Pretty-printed JSON Schema of the NEWEST MINOR OF SCORECARD FORMAT 1:
/// **1.10.0** (`integrity.verification.positions_mapping`, PROD-04.2): the
/// frozen 1.9.0 file plus that optional block and its three definitions. It
/// describes every 1.x document this build writes; the 1.9.0 file
/// (`approval.console`, a restore a second person approved in the console) is
/// frozen beside it and was built the same way from 1.8.0
/// ([`scorecard_format_1_9_0_schema`]).
///
/// The Rust type reads both majors, so what it derives on its own is no longer
/// format 1's selection block. A MINOR adds optional fields only, so this file
/// is BUILT AS WHAT IT IS: `frozen_predecessor` (the text of the frozen 1.8.0
/// file), plus the optional `approval.console` property and its two definitions
/// exactly as the type derives them, under the `$id` built from
/// [`crate::scorecard::FORMAT_VERSION_WITH_CONSOLE_APPROVAL`]. So a change to
/// [`crate::scorecard::ConsoleApprovalInfo`] is a diff of this file
/// (`just schema-check`), and nothing else of format 1 can move
/// (`the_frozen_1_8_0_scorecard_schema_is_the_frozen_1_7_0_plus_the_original_name_block`).
///
/// # Panics
///
/// When `frozen_predecessor` is not a JSON Schema that defines `ApprovalInfo`,
/// or already describes the block: the caller handed the wrong file.
pub fn scorecard_format_1_schema(frozen_predecessor: &str) -> String {
    format_1_minor(
        frozen_predecessor,
        crate::scorecard::FORMAT_VERSION_WITH_POSITIONS_MAPPING,
        "Verification",
        POSITIONS_MAPPING_PROPERTY,
        &POSITIONS_MAPPING_DEFINITIONS,
    )
}

/// `Verification`'s property for the `positions_mapping` block (PROD-04.2).
pub const POSITIONS_MAPPING_PROPERTY: &str = "positions_mapping";

/// The three definitions the `positions_mapping` block brings with it.
pub const POSITIONS_MAPPING_DEFINITIONS: [&str; 3] =
    ["PositionsMapping", "MappingCounts", "DocumentRef"];

/// The 1.9.0 file as it was generated: the frozen 1.8.0 file plus
/// `approval.console` (frozen since 1.10.0; `schema_drift` holds it to this).
pub fn scorecard_format_1_9_0_schema(frozen_1_8_0: &str) -> String {
    format_1_minor(
        frozen_1_8_0,
        crate::scorecard::FORMAT_VERSION_WITH_CONSOLE_APPROVAL,
        "ApprovalInfo",
        CONSOLE_APPROVAL_PROPERTY,
        &CONSOLE_APPROVAL_DEFINITIONS,
    )
}

/// One MINOR of format 1: `frozen_predecessor` plus one optional `property` of
/// `owner` and its `definitions`, exactly as the type derives them, under the
/// `$id` of `version`.
fn format_1_minor(
    frozen_predecessor: &str,
    version: &str,
    owner: &str,
    block: &str,
    definitions: &[&str],
) -> String {
    use schemars::schema::{RootSchema, Schema};
    let settings = schemars::gen::SchemaSettings::draft07().with(|s| {
        s.option_nullable = true;
        s.option_add_null_type = false;
    });
    let mut derived = settings
        .into_generator()
        .into_root_schema_for::<Scorecard>();
    let mut root: RootSchema =
        serde_json::from_str(frozen_predecessor).expect("the frozen scorecard schema parses");
    root.schema.metadata().id = Some(format!(
        "https://logweir.dev/schemas/logweir-drill-scorecard-{version}.json"
    ));
    let property = match derived.definitions.get_mut(owner) {
        Some(Schema::Object(o)) => o
            .object()
            .properties
            .remove(block)
            .unwrap_or_else(|| panic!("the scorecard type derives {owner}.{block}")),
        _ => panic!("the scorecard type derives {owner}"),
    };
    match root.definitions.get_mut(owner) {
        Some(Schema::Object(o)) => {
            assert!(
                o.object()
                    .properties
                    .insert(block.into(), property)
                    .is_none(),
                "the frozen predecessor must not describe {owner}.{block}"
            );
        }
        _ => panic!("the frozen scorecard schema defines {owner}"),
    }
    for &name in definitions {
        let definition = derived
            .definitions
            .remove(name)
            .unwrap_or_else(|| panic!("the scorecard type derives {name}"));
        assert!(
            root.definitions.insert(name.into(), definition).is_none(),
            "the frozen predecessor must not define {name}"
        );
    }
    let mut out = serde_json::to_string_pretty(&root).expect("schema serialises");
    out.push('\n');
    out
}

/// Pretty-printed JSON Schema for the backup receipt, with the scorecard's
/// generator settings. `$id` pins the published URL, and CI regenerates it and
/// `diff -u`s it against the CURRENT schema file on every build.
///
/// **The current file is the newest MINOR**:
/// `schemas/logweir-backup-receipt-<FORMAT_VERSION_WITH_SOURCE_WATERMARKS>.json`,
/// its `$id` built from that ONE constant, so a renumber is the constant and a
/// file name. The older files (1.0.0 to 1.7.0) are FROZEN beside it and never
/// regenerated; `tests/schema_drift.rs` holds each to what it published.
pub fn backup_receipt_schema() -> String {
    let settings = schemars::gen::SchemaSettings::draft07().with(|s| {
        s.option_nullable = true;
        s.option_add_null_type = false;
    });
    let mut root = settings
        .into_generator()
        .into_root_schema_for::<BackupReceipt>();
    root.schema.metadata().id = Some(format!(
        "https://logweir.dev/schemas/logweir-backup-receipt-{}.json",
        crate::backup_receipt::FORMAT_VERSION_WITH_SOURCE_WATERMARKS
    ));
    let mut out = serde_json::to_string_pretty(&root).expect("schema serialises");
    out.push('\n');
    out
}

/// The JSON Schema of the consumer positions DOCUMENT
/// (`<run_id>.consumer-positions.json`, format
/// [`crate::consumer_positions::DOCUMENT_FORMAT_VERSION`]) a 1.7.0 receipt
/// binds by digest: `schemas/logweir-consumer-positions-1.0.0.json`, its `$id`
/// built from that one constant.
pub fn consumer_positions_document_schema() -> String {
    let settings = schemars::gen::SchemaSettings::draft07().with(|s| {
        s.option_nullable = true;
        s.option_add_null_type = false;
    });
    let mut root = settings
        .into_generator()
        .into_root_schema_for::<crate::consumer_positions::PositionsDocument>();
    root.schema.metadata().id = Some(format!(
        "https://logweir.dev/schemas/logweir-consumer-positions-{}.json",
        crate::consumer_positions::DOCUMENT_FORMAT_VERSION
    ));
    let mut out = serde_json::to_string_pretty(&root).expect("schema serialises");
    out.push('\n');
    out
}

/// **PROD-04.2.** The positions-mapping document's schema,
/// `schemas/logweir-positions-mapping-1.0.0.json` (`logweir schema
/// positions-mapping`).
pub fn positions_mapping_document_schema() -> String {
    document_schema::<crate::positions_mapping::MappingDocument>(
        "positions-mapping",
        crate::positions_mapping::DOCUMENT_FORMAT_VERSION,
    )
}

/// **PROD-04.2.** The switchover record's schema,
/// `schemas/logweir-switchover-record-1.0.0.json` (`logweir schema
/// switchover-record`).
pub fn switchover_record_schema() -> String {
    document_schema::<crate::switchover::SwitchoverRecord>(
        "switchover-record",
        crate::switchover::RECORD_FORMAT_VERSION,
    )
}

/// A document's schema under `https://logweir.dev/schemas/logweir-<name>-<version>.json`,
/// with the scorecard's generator settings.
fn document_schema<T: schemars::JsonSchema>(name: &str, version: &str) -> String {
    let settings = schemars::gen::SchemaSettings::draft07().with(|s| {
        s.option_nullable = true;
        s.option_add_null_type = false;
    });
    let mut root = settings.into_generator().into_root_schema_for::<T>();
    root.schema.metadata().id = Some(format!(
        "https://logweir.dev/schemas/logweir-{name}-{version}.json"
    ));
    let mut out = serde_json::to_string_pretty(&root).expect("schema serialises");
    out.push('\n');
    out
}
