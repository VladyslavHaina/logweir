//! The scope a standing rehearsal authorization signs over (decision D3 §4.3).
//!
//! # Types only, and the reason the check is not here yet
//!
//! D3 §14 gives this file to W1 with "only the scope types §4.3 names". The
//! predicate those types exist for —
//! `rehearsal_scope::plan_within_scope(plan, scope)` — reads a rendered
//! restore plan, whose shape is W5's execution contract v2, and is W7's to
//! write against it. Landing the check here against today's plan shape would
//! be a second definition of "the plan" that W5 would then have to keep in
//! step.
//!
//! What lands now is the **document half**: the scope is part of a signed
//! authorization, so its field names and its serialisation are an interface
//! the approver's `logweir approve rehearsal` and the controller's each-slot
//! re-check must already agree on, long before the predicate runs.
//!
//! # Why a signed scope at all
//!
//! Per-slot human approval would defeat an unattended rehearsal, and a
//! controller that could mint its own authorization would be the bypass
//! PLAT-19.2 exists to prevent. So the human signs a scope ONCE, and the
//! controller proves each slot's rendered plan falls inside it — twice, once
//! before creating the `Restore` and once in the runner against the mounted
//! bundle. [`RehearsalScope::template_digest`] is what binds the scope to one
//! schedule object: the digest is recomputed from the schedule's own sealed
//! spec each slot, so a scope cannot outlive the thing it was written for.
//!
//! # Pure
//!
//! No clock, no I/O. `issuedAt`/`expiresAt` live on the enclosing
//! authorization document, not here: this type is the SCOPE, and an expiry is
//! a property of the grant.

use serde::{Deserialize, Serialize};

/// The only target mode a rehearsal may run in.
///
/// `scratch` and nothing else. A rehearsal that could run in the new-topic
/// mode would restore into names an application might be reading, and the
/// runner's own prefix-scoped deletion guard — which is what makes teardown
/// safe — only applies to scratch names.
pub const MODE_SCRATCH: &str = "scratch";

/// What a standing rehearsal authorization permits (D3 §4.3).
///
/// EXACT VALUES, NOT PATTERNS, except for the one prefix the mode requires.
/// A scope written with wildcards is a scope nobody can read back and check,
/// and the whole purpose of this document is that a slot's plan can be proven
/// to fall inside something a human signed.
#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RehearsalScope {
    /// `sha256` over the canonical JSON of the `RehearsalSchedule`'s `spec`
    /// minus `suspend`.
    ///
    /// THE BINDING TO ONE SCHEDULE OBJECT. The spec is sealed except
    /// `suspend`, so the digest cannot drift under a running authorization; a
    /// NEW schedule needs a new authorization, which is the property that
    /// makes "approve this rehearsal" mean a specific rehearsal.
    pub template_digest: String,
    /// The cluster id a rehearsal restore may target. Compared against the
    /// cluster's own reported id, never against a spec field: `spec.role` is
    /// free-form and is not authority.
    pub target_cluster_id: String,
    /// The prefix every restored topic name must carry. Rendered per schedule
    /// as `<prefix><schedule-uid-first-8>-`, so two schedules can never map to
    /// the same topic name.
    pub topic_prefix: String,
    /// The source topics this authorization covers, by exact name.
    pub topics: Vec<String>,
    /// The most partitions any one restored topic may have.
    pub max_partitions: u32,
    /// The most records per partition the rehearsal may write.
    pub records_per_partition: u32,
    /// The wall-clock bound on one rehearsal run.
    pub deadline_seconds: u32,
    /// The target modes permitted — [`MODE_SCRATCH`] and nothing else today.
    ///
    /// A LIST RATHER THAN A CONSTANT, because it is a field of a SIGNED
    /// document: an authorization signed today must still be readable by a
    /// build that has learned a second mode, and a scope that carried no mode
    /// at all would silently widen when one arrived.
    pub modes: Vec<String>,
}

impl RehearsalScope {
    /// Whether this scope permits the scratch mode and nothing else.
    ///
    /// The check a reader performs BEFORE trusting any other field: a scope
    /// naming a mode this build does not implement is a scope this build must
    /// not act on, and the honest answer is a refusal rather than an
    /// intersection nobody signed.
    #[must_use]
    pub fn is_scratch_only(&self) -> bool {
        !self.modes.is_empty() && self.modes.iter().all(|m| m == MODE_SCRATCH)
    }
}
