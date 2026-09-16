// crates/logweir/examples/emit_catalog_point_schema.rs
//
// The generator behind `just schema`'s catalog line and behind the
// `just schema-check` drift arm (PLAT-15.1, decision D3 §5.2). Prints and
// nothing else: the redirect is the recipe's business, and a generator that
// wrote its own target could not be diffed against it.
//
// It lives in `crates/logweir` rather than beside the scorecard's and the
// receipt's in `crates/logweir-core` because the type does: a catalog point
// record is built from a signed receipt, a store location and a signing key,
// which is runner vocabulary and not the pure layer's (Global Constraint 1).
fn main() {
    print!("{}", logweir::catalog::schema::catalog_point_schema());
}
