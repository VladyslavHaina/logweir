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
/// FX-8** (`source.time_basis`); the 1.1.0 and 1.2.0 files are frozen beside
/// the current one the same way. The `$id` is built from
/// [`crate::FORMAT_VERSION`], so the file this writes is always the one the
/// writer's documents name.
pub fn scorecard_schema() -> String {
    let settings = schemars::gen::SchemaSettings::draft07().with(|s| {
        s.option_nullable = true;
        s.option_add_null_type = false;
    });
    let mut root = settings
        .into_generator()
        .into_root_schema_for::<Scorecard>();
    root.schema.metadata().id = Some(format!(
        "https://logweir.dev/schemas/logweir-drill-scorecard-{}.json",
        crate::FORMAT_VERSION
    ));
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
/// `schemas/logweir-backup-receipt-<FORMAT_VERSION_WITH_MANIFEST_VERSION>.json`
/// (`1.2.0`, FX-7's `archive.manifest_version_id`), its `$id` built from that
/// ONE constant, so a renumber is the constant and a file name
/// (`docs/stability.md`: a MINOR bump is "a new schema file beside the old
/// one"). The older files are FROZEN beside it and never regenerated:
/// `schemas/logweir-backup-receipt-1.0.0.json` describes every receipt written
/// before FX-4 (`the_frozen_1_0_0_receipt_schema_is_still_the_1_0_0_schema`),
/// and FX-4's `-1.1.0.json` (`config_coverage`) every receipt written without
/// a pin (`the_frozen_1_1_0_receipt_schema_is_still_fx4s`).
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
        crate::backup_receipt::FORMAT_VERSION_WITH_MANIFEST_VERSION
    ));
    let mut out = serde_json::to_string_pretty(&root).expect("schema serialises");
    out.push('\n');
    out
}
