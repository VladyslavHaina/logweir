//! The JSON Schema for the catalog point record.
//!
//! Generated FROM THE RUST TYPE with the same generator settings the
//! scorecard's and the receipt's use (`logweir_core::schema`), so the three
//! files are comparable by eye and a reviewer reading one drift diff has
//! learnt to read the next. `just schema` writes it and `just schema-check`
//! regenerates it into a temporary directory and `diff -u`s it against the
//! checked-in bytes, so the published schema cannot silently stop describing
//! the type.

use crate::catalog::record::CatalogPoint;

/// Pretty-printed JSON Schema for [`CatalogPoint`], `$id` pinned to the
/// published URL so a downloaded record names the schema that validates it.
///
/// **The newest MINOR** (FX-7 fix round, review M-2): the `$id` and the file
/// name come from ONE constant,
/// [`FORMAT_VERSION_WITH_AUTH_MODES`](crate::catalog::record::FORMAT_VERSION_WITH_AUTH_MODES)
/// (`1.3.0`, PROD-01.3's three new `source.auth_mode` values). The older files
/// are FROZEN beside it: FX-7's `-1.2.0.json` (`archive.manifest_version_id`)
/// for a pinned record of a `plaintext`/`scramSha512` source, `schemas/logweir-catalog-point-1.0.0.json` for the records
/// written before FX-4, and FX-4's `-1.1.0.json` (`topics[].config_coverage`)
/// for the records written without a pin.
///
/// The `format_version` pattern (`^1\.[0-9]+\.[0-9]+$`) is on the type, not
/// added here: a schema-only validator — the one route that does not go
/// through [`crate::catalog::reader::read_record`] — must refuse a `9.9.9`
/// document against a file called `logweir-catalog-point-1.2.0.json` for the
/// same reason the scorecard and the receipt pin theirs.
#[must_use]
pub fn catalog_point_schema() -> String {
    let settings = schemars::gen::SchemaSettings::draft07().with(|s| {
        s.option_nullable = true;
        s.option_add_null_type = false;
    });
    let mut root = settings
        .into_generator()
        .into_root_schema_for::<CatalogPoint>();
    root.schema.metadata().id = Some(format!(
        "https://logweir.dev/schemas/logweir-catalog-point-{}.json",
        crate::catalog::record::FORMAT_VERSION_WITH_AUTH_MODES
    ));
    let mut out = serde_json::to_string_pretty(&root).expect("schema serialises");
    out.push('\n');
    out
}
