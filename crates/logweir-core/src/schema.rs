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
/// `.github/workflows/ci.yml` regenerates this and `diff -u`s it against
/// `schemas/logweir-backup-receipt-1.1.0.json` on every build — so the
/// checked-in file cannot silently stop describing the type.
///
/// **1.1.0 since FX-4** (`config_coverage`). The 1.0.0 file is FROZEN beside
/// it and describes every receipt written before the bump.
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
        crate::backup_receipt::RECEIPT_FORMAT_VERSION
    ));
    let mut out = serde_json::to_string_pretty(&root).expect("schema serialises");
    out.push('\n');
    out
}
