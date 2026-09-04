//! Vendored wire types for OSO's (`kafka-backup`) on-bucket artifacts.
//!
//! This is the only crate in the Logweir workspace permitted to know that the
//! upstream `kafka-backup` engine exists (Global Constraint 2). It does not
//! depend on `kafka-backup-core` — every type below is a hand-ported serde
//! struct that mirrors a pinned upstream shape, so an upstream release can
//! break parsing but never the build. See `scripts/check-no-oso.sh`.

pub mod vendored;
