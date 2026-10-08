// crates/logweir-core/examples/emit_schema.rs
#![forbid(unsafe_code)]
fn main() {
    print!("{}", logweir_core::schema::scorecard_schema());
}
