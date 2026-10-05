use crate::backup_receipt::BackupReceipt;
use crate::scorecard::Scorecard;

/// Pretty-printed JSON Schema for the scorecard. `$id` pins the published
/// URL so a downloaded scorecard names the schema that validates it.
pub fn scorecard_schema() -> String {
    let settings = schemars::gen::SchemaSettings::draft07().with(|s| {
        s.option_nullable = true;
        s.option_add_null_type = false;
    });
    let mut root = settings
        .into_generator()
        .into_root_schema_for::<Scorecard>();
    root.schema.metadata().id =
        Some("https://logweir.dev/schemas/logweir-drill-scorecard-1.0.0.json".to_string());
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
/// `schemas/logweir-backup-receipt-<FORMAT_VERSION_WITH_MANIFEST_VERSION>.json`,
/// its `$id` built from that ONE constant, so a renumber is the constant and a
/// file name (`docs/stability.md`: a MINOR bump is "a new schema file beside
/// the old one"). `schemas/logweir-backup-receipt-1.0.0.json` is FROZEN beside
/// it, describes every receipt written before the pin, and is never
/// regenerated (`the_frozen_1_0_0_receipt_schema_is_still_the_1_0_0_schema`).
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
