//! The runner image, and nothing else yet.
//!
//! Task 17 grows `job::build` around [`RUNNER_IMAGE`] — the `Job` with
//! `restartPolicy: Never`, `backoffLimit: 0`, `activeDeadlineSeconds` from
//! `spec.deadlineSeconds` and a `podFailurePolicy` of `FailJob` on exit codes
//! `[2, 3, 4]`. None of that is here. What is here is the one string that must
//! exist in exactly one place before anything builds a Job at all.

/// The image the runner Jobs use — **interface I15**.
///
/// THE ONLY PLACE UNDER `crates/` THE RUNNER IMAGE IS NAMED. Task 17's
/// `job::build` reads this constant, and
/// `tests/crd_shape.rs::the_runner_image_is_named_once` asserts the registry
/// path appears exactly once in this file and nowhere else under `crates/`.
/// The property is defended from the task that first states it, rather than
/// from the task that finally pins the digest: a second occurrence is how a
/// digest bump comes to update one call site and miss another.
///
/// THE NAMESPACE IS THE LITERAL `ghcr.io/logweir/…`, NOT A PLACEHOLDER
/// (Global Constraint 24). A shipped `logweir.yaml` carrying `ghcr.io/<org>/…`
/// is not applyable, which would make spec §16's first clause unsatisfiable. A
/// trademark answer changes one string, here.
///
/// WHY A TAG TODAY, AND WHAT IS OWED. Global Constraint 7 pins by digest and
/// never by tag, and this value is a tag. That is not a relaxation of GC7; it
/// is GC37's `blocked: no remote` state written into the one place that has to
/// hold a value: no remote exists, `release.yml` has never run against one, so
/// no digest for this image exists to pin, and a fabricated `@sha256:…` would
/// be strictly worse than a tag — it would look pinned and resolve to nothing.
/// **Task 23 replaces this with the digest reference** and tightens
/// `the_runner_image_is_named_once` to the digest form, at which point the tag
/// is gone from the tree. Until then the obligation is a recorded one, not an
/// invisible one.
pub const RUNNER_IMAGE: &str = "ghcr.io/logweir/logweir:v0.1.0";
