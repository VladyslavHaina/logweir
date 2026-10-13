// crates/logweir-core/examples/emit_positions_mapping_schema.rs
//
// The generator behind `just schema`'s positions_mapping line and its drift arm
// (PROD-04.2). Prints and nothing else.
#![forbid(unsafe_code)]
fn main() {
    print!(
        "{}",
        logweir_core::schema::positions_mapping_document_schema()
    );
}
