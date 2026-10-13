// crates/logweir-core/examples/emit_switchover_record_schema.rs
//
// The generator behind `just schema`'s switchover_record line and its drift arm
// (PROD-04.2). Prints and nothing else.
#![forbid(unsafe_code)]
fn main() {
    print!("{}", logweir_core::schema::switchover_record_schema());
}
