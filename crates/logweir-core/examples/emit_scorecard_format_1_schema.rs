// crates/logweir-core/examples/emit_scorecard_format_1_schema.rs
#![forbid(unsafe_code)]
//! Prints the newest MINOR of scorecard format 1 (`just schema`): the frozen
//! predecessor's file (1.10.0) plus the optional `target.topic_configuration`
//! block (1.11.0), as `logweir_core::schema::scorecard_format_1_schema` builds
//! it.
fn main() {
    let frozen = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(format!(
        "../../schemas/logweir-drill-scorecard-{}.json",
        logweir_core::scorecard::FORMAT_VERSION_WITH_TARGET_CREATION
    ));
    let frozen =
        std::fs::read_to_string(&frozen).unwrap_or_else(|e| panic!("{}: {e}", frozen.display()));
    print!(
        "{}",
        logweir_core::schema::scorecard_format_1_schema(&frozen)
    );
}
