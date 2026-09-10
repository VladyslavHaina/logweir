// crates/logweir-core/examples/emit_backup_receipt_schema.rs
//
// The generator behind `just schema`'s second line and behind the CI drift
// arm. Prints and nothing else: the redirect is the recipe's business, and a
// generator that wrote its own target could not be diffed against it.
fn main() {
    print!("{}", logweir_core::schema::backup_receipt_schema());
}
