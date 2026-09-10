#![forbid(unsafe_code)]
//! `weirkeeper` — the Logweir control plane.
//!
//! This half of the crate is deliberately almost empty. It exists so that the
//! three risks that come with introducing a Kubernetes client — the dependency
//! decision, the one network fetch this plan performs, and the linkage
//! property that keeps the controller away from the signer — are settled and
//! tested on their own, before a single CRD field or reconciler arrives. The
//! six kinds land in Task 15b and the first reconciler in Task 16; both append
//! to what is here rather than reshaping it.
//!
//! WHAT THIS CRATE LINKS, AND WHAT IT MUST NOT. `logweir-verify` — the
//! verifying half of the DSSE machinery — and never `logweir-evidence`, which
//! keeps `SigningKey` and `sign_detached`. Global Constraint 27 states the
//! narrowed position exactly: no control-plane CRATE LINKS the signer, and the
//! CAPABILITY to sign is unbroken while this controller holds Job CRUD over
//! the signing key's namespace. Both halves of that sentence matter, and
//! `tests/linkage.rs` tests the half a test can reach.
//!
//! WHY THE DOUBLE IS IN `src/` AND NOT IN `tests/`. Every reconciler from
//! Task 16 onward is tested against [`testing::mock_client`], and a
//! `tests/`-local helper cannot be shared across test binaries, let alone
//! across crates. Shipping it in the library is what makes "the reconciler
//! called nothing its test did not record" a property every later task
//! inherits instead of re-implements — and it is why `tower`, `http` and
//! `http-body-util` are normal dependencies here rather than
//! `[dev-dependencies]`.

pub mod testing;
