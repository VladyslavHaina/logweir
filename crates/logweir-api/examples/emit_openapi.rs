//! Print the generated OpenAPI document. `just schema` redirects it into
//! `schemas/logweir-api-v1.openapi.json`; `just schema-check` and
//! `tests/contract.rs` compare it with the checked-in copy.
fn main() {
    print!("{}", logweir_api::openapi::openapi_document());
}
