//! Vendored wire types for OSO's (`kafka-backup`) on-bucket artifacts.
//!
//! This is the only crate in the Logweir workspace permitted to know that the
//! upstream `kafka-backup` engine exists (Global Constraint 2). It does not
//! depend on `kafka-backup-core` — every type below is a hand-ported serde
//! struct that mirrors a pinned upstream shape, so an upstream release can
//! break parsing but never the build. See `scripts/check-no-oso.sh`.

pub mod engine;
pub mod kbak;
pub mod storage;
pub mod subprocess;
pub mod vendored;

/// Global Constraint 4: never emit these three keys, at any value, on any
/// argv or in any rendered YAML. `render_restore` and `render_validation` are
/// structurally incapable of writing them; `tests/render.rs` pins the
/// property.
pub const FORBIDDEN_KEYS: [&str; 3] = ["purge_topics", "dry_run", "header_preflight_external"];
pub mod render_restore;
pub mod render_validation;
