//! The re-export, pinned. `crates/logweir-engine-oso/src/lib.rs` now reads
//! `pub use logweir_store as storage;` where it read `pub mod storage;`, and
//! the whole claim that the extraction "changed no call site" rests on that one
//! line: `crates/logweir/src/drill/*`, `doctor.rs`, `engine.rs` and `kbak.rs`
//! all still name `…::storage::Store`.
//!
//! COMPILE-ONLY BY DESIGN. There is nothing to execute — the property is that
//! the two paths RESOLVE, and a path that stops resolving is a build failure in
//! this file. The `assert!`s below exist so the test is a real test rather than
//! an empty body, not because the values are in doubt; `tests/storage.rs`, now
//! in `logweir-store`, is what exercises the behaviour.

#[test]
fn logweir_engine_oso_storage_path_still_resolves() {
    // The type, through the re-export.
    let s: logweir_engine_oso::storage::Store =
        logweir_engine_oso::storage::Store::in_memory(logweir_engine_oso::storage::LOGWEIR_ROOT);

    // The constant, through the re-export, with its Global Constraint 6 value.
    assert_eq!(
        logweir_engine_oso::storage::LOGWEIR_ROOT,
        "logweir/",
        "Global Constraint 6's key root is fixed in code and travels with the \
         crate it was extracted into"
    );

    // And the re-export is the same item, not a copy: a `logweir_store::Store`
    // is accepted where a `logweir_engine_oso::storage::Store` is expected.
    let _same_item: logweir_store::Store = s;
}
