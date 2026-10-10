// crates/logweir-core/examples/emit_consumer_positions_schema.rs
//
// PROD-04.1: the generator behind `just schema`'s consumer positions document
// line and its drift arm. Prints and nothing else.
#![forbid(unsafe_code)]
fn main() {
    print!(
        "{}",
        logweir_core::schema::consumer_positions_document_schema()
    );
}
