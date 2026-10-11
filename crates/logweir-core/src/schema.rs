use crate::backup_receipt::BackupReceipt;
use crate::scorecard::Scorecard;

/// Pretty-printed JSON Schema for the scorecard. `$id` pins the published
/// URL so a downloaded scorecard names the schema that validates it.
///
/// **1.1.0 since FX-4** (`topic_parity.not_assessed`). The 1.0.0 file,
/// `schemas/logweir-drill-scorecard-1.0.0.json`, is FROZEN beside it: it
/// describes every document written before the bump and is no longer
/// regenerated (`docs/stability.md`: a new optional field is a MINOR bump
/// "with a new schema file beside the old one").
///
/// **1.2.0 since FX-3** (`topic_parity.not_reconstructed`), and **1.3.0 since
/// FX-8** (`source.time_basis`), **1.4.0 since PROD-08.1**
/// (`integrity.verification`), and **1.5.0 since PROD-01.3**: the closed set of
/// `target.auth.mode` grows by three values, which a scorecard declares by
/// being 1.5.0; the 1.1.0 to 1.4.0 files are frozen beside the current one the
/// same way. **1.6.0 since FX-23** (`sample.unsampled_topics`), which this
/// build writes for every SAMPLED scorecard; the 1.5.0 file is frozen beside
/// it. A complete verification's scorecard is still written as 1.4.0 (or 1.5.0
/// for a PROD-01.3 mode), which those frozen files describe. **1.7.0 since
/// PROD-11.1** (`source.selection`), written only for a narrowed restore; the
/// 1.6.0 file is frozen beside it.
///
/// **2.0.0 since PROD-11.1b** (the owner's decision OD-9 (a)), the format's
/// first MAJOR, written ONLY for a restore that states a partition subset:
/// 1.7.0's fields, with `source.selection` REQUIRED and its `partitions` (a
/// non-empty list of non-empty lists) and `engine_runs` (at least one)
/// required in it, and `format_version` pinned to `2.x.y`. The 1.7.0 file is
/// FROZEN beside it. The generator emits the 2.0.0 file: the Rust type
/// reads both majors, so the requirements that make a document 2.0.0's are
/// added here, on top of what the type derives.
///
/// **2.1.0 since PROD-16.2** (`approval.console`), format 2's first MINOR,
/// written ONLY for a partition-subset restore that a second person approved
/// in the console: 2.0.0's fields plus that one optional block and its two
/// definitions, exactly as the type derives them. The 2.0.0 file is FROZEN
/// beside it and is this file without the block
/// (`the_frozen_2_0_0_scorecard_schema_is_the_2_1_0_one_without_the_console_approval`).
/// The `$id` is built from
/// [`crate::scorecard::FORMAT_VERSION_SUBSET_WITH_CONSOLE_APPROVAL`], the
/// newest version.
///
/// **PROD-15.1's `target.original_name` is NOT in this file, on purpose.** An
/// original-name restore restores whole topics (`crate::original_name`,
/// `OriginalNameNeedsWholeTopics`; arm ON-14), so the block never appears in a
/// 2.x document. The block is format 1's, from 1.8.0:
/// [`scorecard_format_1_schema`] writes format 1's newest file, which describes
/// every 1.x document this build writes.
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
        crate::scorecard::FORMAT_VERSION_SUBSET_WITH_CONSOLE_APPROVAL
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
    // PROD-15.1: `target.original_name` is format 1's (1.8.0) and never a
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
    // PS-3 in the schema too (review L5): each list non-empty, distinct and
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

/// `TargetInfo`'s property for PROD-15.1's block.
const ORIGINAL_NAME_PROPERTY: &str = "original_name";

/// The two definitions PROD-15.1's block brings with it.
const ORIGINAL_NAME_DEFINITIONS: [&str; 2] = ["OriginalNameInfo", "OriginalNameOwner"];

/// `ApprovalInfo`'s property for PROD-16.2's block.
const CONSOLE_APPROVAL_PROPERTY: &str = "console";

/// The two definitions PROD-16.2's block brings with it.
const CONSOLE_APPROVAL_DEFINITIONS: [&str; 2] = ["ConsoleApprovalInfo", "ConsolePrincipal"];

/// Pretty-printed JSON Schema of the NEWEST MINOR OF SCORECARD FORMAT 1:
/// **1.9.0 since PROD-16.2** (`approval.console`), written only for a restore
/// that a second person approved in the console. It describes every 1.x
/// document this build writes; PROD-15.1's 1.8.0 file (`target.original_name`)
/// is frozen beside it.
///
/// The Rust type reads both majors, so what it derives on its own is no longer
/// format 1's selection block (a start and an end; [`scorecard_schema`] turns
/// it into format 2's). A MINOR adds optional fields only, so this file is
/// BUILT AS WHAT IT IS: `frozen_predecessor` (the text of the frozen 1.8.0
/// file), plus the optional `approval.console` property and its two
/// definitions exactly as the type derives them today, under the `$id` built
/// from [`crate::scorecard::FORMAT_VERSION_WITH_CONSOLE_APPROVAL`]. So a
/// change to [`crate::scorecard::ConsoleApprovalInfo`] is a diff of this file
/// (`just schema-check`), and nothing else of format 1 can move: PROD-15.1's
/// block is the frozen file's, as that row published it
/// (`the_frozen_1_8_0_scorecard_schema_is_the_frozen_1_7_0_plus_the_original_name_block`).
///
/// # Panics
///
/// When `frozen_predecessor` is not a JSON Schema that defines `ApprovalInfo`,
/// or already describes the block: the caller handed the wrong file.
pub fn scorecard_format_1_schema(frozen_predecessor: &str) -> String {
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
        "https://logweir.dev/schemas/logweir-drill-scorecard-{}.json",
        crate::scorecard::FORMAT_VERSION_WITH_CONSOLE_APPROVAL
    ));
    let property = match derived.definitions.get_mut("ApprovalInfo") {
        Some(Schema::Object(o)) => o
            .object()
            .properties
            .remove(CONSOLE_APPROVAL_PROPERTY)
            .expect("the scorecard type derives approval.console"),
        _ => panic!("the scorecard type derives ApprovalInfo"),
    };
    match root.definitions.get_mut("ApprovalInfo") {
        Some(Schema::Object(o)) => {
            assert!(
                o.object()
                    .properties
                    .insert(CONSOLE_APPROVAL_PROPERTY.into(), property)
                    .is_none(),
                "the frozen predecessor must not describe approval.console"
            );
        }
        _ => panic!("the frozen scorecard schema defines ApprovalInfo"),
    }
    for name in CONSOLE_APPROVAL_DEFINITIONS {
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

/// Pretty-printed JSON Schema for the backup receipt (Task 5). Same
/// generator settings as the scorecard's, so the two files are comparable by
/// eye and a reviewer reading one drift diff has learnt to read the other.
///
/// `$id` pins the published URL, and the CI drift arm at
/// `.github/workflows/ci.yml` regenerates this and `diff -u`s it against the
/// CURRENT schema file on every build — so the checked-in file cannot silently
/// stop describing the type.
///
/// **The current file is the newest MINOR** (FX-7 fix round, review M-2):
/// `schemas/logweir-backup-receipt-<FORMAT_VERSION_WITH_CONSUMER_POSITIONS>.json`
/// (`1.7.0`, PROD-04.1's `consumer_positions`; PROD-01.4a's 1.6.0
/// `generations` file, PROD-03.0's 1.5.0
/// `schema_dependency` file, PROD-01.3's 1.4.0 file with its three new
/// `source.auth.mode` values, PROD-05.1's 1.3.0 `topic_configuration` file and
/// FX-7's 1.2.0 `archive.manifest_version_id` file are frozen beside it),
/// its `$id` built
/// from that ONE constant, so a renumber is the constant and a file name
/// (`docs/stability.md`: a MINOR bump is "a new schema file beside the old
/// one"). The older files are FROZEN beside it and never regenerated:
/// `schemas/logweir-backup-receipt-1.0.0.json` describes every receipt written
/// before FX-4 (`the_frozen_1_0_0_receipt_schema_is_still_the_1_0_0_schema`),
/// FX-4's `-1.1.0.json` (`config_coverage`) every receipt written without a
/// pin before PROD-05.1 (`the_frozen_1_1_0_receipt_schema_is_still_fx4s`), and
/// FX-7's `-1.2.0.json` (`archive.manifest_version_id`) every pinned one
/// before it (`the_frozen_1_2_0_receipt_schema_is_still_fx7s`).
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
        crate::backup_receipt::FORMAT_VERSION_WITH_CONSUMER_POSITIONS
    ));
    let mut out = serde_json::to_string_pretty(&root).expect("schema serialises");
    out.push('\n');
    out
}

/// **PROD-04.1.** The JSON Schema of the consumer positions DOCUMENT
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
