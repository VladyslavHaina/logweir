//! Vendored wire types for OSO's (`kafka-backup`) on-bucket artifacts.
//!
//! This is the only crate in the Logweir workspace permitted to know that the
//! upstream `kafka-backup` engine exists (Global Constraint 2). It does not
//! depend on `kafka-backup-core` — every type below is a hand-ported serde
//! struct that mirrors a pinned upstream shape, so an upstream release can
//! break parsing but never the build. See `scripts/check-no-oso.sh`.

pub mod engine;
pub mod kbak;
// The object-store half of this crate now lives in `logweir-store`, so a
// component that must not link the OSO engine wrapper can still hold a
// bucket handle (ADR 0008 §E; `02-k8s-transition-plan.md:963`). The
// re-export keeps `logweir_engine_oso::storage::…` and `crate::storage::…`
// resolving, so the extraction changed no call site.
pub use logweir_store as storage;
pub mod subprocess;
pub mod vendored;

/// Global Constraint 4: never emit these three keys, at any value, on any
/// argv or in any rendered YAML. `render_restore` and `render_validation` are
/// structurally incapable of writing them; `tests/render.rs` pins the
/// property.
pub const FORBIDDEN_KEYS: [&str; 3] = ["purge_topics", "dry_run", "header_preflight_external"];
pub mod render_restore;
pub mod render_validation;
