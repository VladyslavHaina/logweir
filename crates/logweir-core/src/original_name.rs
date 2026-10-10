//! **PROD-15.1 — restore under the ORIGINAL topic name, into a topic that does
//! not exist: the conditions contract.** Pure: no clock, no I/O, no client.
//!
//! # What the owner decided, and what stays refused
//!
//! OD-2 (2026-10-05) kept `docs/stability.md`'s Never #1 — a restore into a
//! LIVE topic stays refused — and narrowed it so that this one path is allowed:
//! a restore that writes under the source's own topic names into topics that
//! are ABSENT, behind its own approval subject. Everything else about an
//! identity mapping stays refused exactly as before: in `scratch` mode, for any
//! target name that exists, and for an empty prefix nobody opted into.
//!
//! # The conditions, and where each one is checked
//!
//! | # | condition | where | refusal |
//! |---|---|---|---|
//! | 1 | the plan opts in explicitly: `target.topic_naming: {prefix: "", original_name: {…}}` in `newTopic` mode | phase 0, local ([`refuse_shape`]) | [`ORIGINAL_NAME_NOT_NEW_TOPIC`], [`ORIGINAL_NAME_PREFIX_NOT_EMPTY`]; an empty prefix without the block keeps the old "onto itself" refusal |
//! | 1b | the plan asks for COMPLETE verification, `sample.coverage: complete` (the orchestrator's ruling of 2026-10-09) | phase 0, local ([`refuse_shape`]); runner and controller readiness; controller admission; the product API and the console | [`ORIGINAL_NAME_NEEDS_COMPLETE_COVERAGE`] |
//! | 1c | the plan restores WHOLE topics: no `restore.partitions` (the orchestrator's ruling of 2026-10-09, after PROD-11.1b allowed partition subsets). A stated window start or end stays allowed: whole partitions, bounded in time | phase 0, local ([`refuse_shape`]); runner startup, `drill approve`, runner and controller readiness; controller admission and reconcile. No CEL rule: the `Restore` CRD declares no partitions | [`ORIGINAL_NAME_NEEDS_WHOLE_TOPICS`] |
//! | 2 | every restored name is absent on the target | phase 0 (the existing absence refusal, both modes) | "already exists" |
//! | 3 | the target is not the source cluster, OR every broker reports `auto.create.topics.enable=false` | phase 0 ([`source_relation`], [`require_auto_create_disabled`]) | [`ORIGINAL_NAME_AUTO_CREATE_ENABLED`], [`ORIGINAL_NAME_AUTO_CREATE_UNKNOWN`] |
//! | 4 | somewhere was looked for a declarative owner, and none was found unless the owner path is chosen | phase 0 ([`owner_verdict`]) | [`ORIGINAL_NAME_OWNER_NOT_CHECKED`], [`ORIGINAL_NAME_OWNER_PRESENT`], [`ORIGINAL_NAME_OWNERS_INVALID`] |
//! | 5 | the approval names the separate subject `originalName` | runner startup and phase 1, controller admission ([`check_approval_subject`]) | [`APPROVAL_SUBJECT_MISMATCH`] |
//! | 5b | on a one-person-confirmation (`Ordinary`) authorization, the requester RE-TYPED every original topic name, exactly, and the console signed what was typed (the owner's decision OD-10) | the product API before it signs, controller admission, runner startup and phase 1 ([`check_typed_confirmation`]) | [`ORIGINAL_NAME_CONFIRMATION_MISSING`], [`ORIGINAL_NAME_CONFIRMATION_MISMATCH`], [`ORIGINAL_NAME_CONFIRMATION_NOT_ACCEPTED`] |
//! | 6 | creation is exclusive: `CreateTopics` fails on an existing name, and a name that appears after phase 0 loses the race by name | the creation step | [`TARGET_TOPIC_APPEARED`] |
//! | 7 | the `LogAppendTime` probe and teardown never touch an original name, and NO code path deletes a topic under one — not a topic this run created, not after a lost race (the orchestrator's ruling of 2026-10-09: Kafka has no conditional delete) | phase 0 ([`probe_topic_name`]), the creation step and phase 9 | — |
//!
//! Every refusal before the creation step is exit 3 before anything is written
//! (the probe, the one documented exception, writes only under the scratch
//! prefix — condition 7). The creation-step race is exit 1: phases 0–5 have
//! run by then (`docs/stability.md`, "A phase-5 / phase-6 … divergence is exit
//! 1"), and nothing was written into the name that appeared.
//!
//! Every refusal MESSAGE opens with its token and a colon, as FX-16's
//! `PointBindingSetMismatch` does; the runner's `refusal-reason=` line stays
//! `GuardRefused`, so no controller condition vocabulary changes.
//!
//! # Why the owner is looked for and never assumed absent
//!
//! A Strimzi `KafkaTopic` (or a GitOps tool driving one) recreates a deleted
//! name on its own, and reverts a restored topic's settings — the pinned
//! `retention.ms=-1` included — to its desired state, which can delete the
//! restored records. The runner cannot read Kubernetes or a Git repository, so
//! it can never OBSERVE that no owner exists. It looks where it can (the
//! approved plan's statement, Strimzi `KafkaTopic` resources it is given, the
//! bound point's verified receipt) and refuses when it looked nowhere — the
//! same rule PROD-05.1 applies to `owner_detection: []` ("owner not checked",
//! never "no owner"). The controller does not list `KafkaTopic` resources yet
//! (PROD-05.1a, which needs a `kafka.strimzi.io` grant), so on Kubernetes the
//! approved plan's statement is the place.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::backup_receipt::TopicOwner;
use crate::spec::{DrillSpec, TargetMode};
use crate::topic_configuration::DeclaredOwner;

// ---------------------------------------------------------------------------
// The refusal tokens
// ---------------------------------------------------------------------------

/// An `original_name` block in `scratch` mode: the identity ban stays in
/// scratch mode, whatever the block says.
pub const ORIGINAL_NAME_NOT_NEW_TOPIC: &str = "OriginalNameNotNewTopic";
/// An `original_name` block beside a non-empty `topic_naming.prefix`: the
/// plan asks for two names at once.
pub const ORIGINAL_NAME_PREFIX_NOT_EMPTY: &str = "OriginalNamePrefixNotEmpty";
/// An `original_name` block in a plan whose `sample.coverage` is not
/// `complete`: a restore under the original topic names is verified
/// completely — every restored record compared with the archive — or not
/// run at all. A sampled check reads the first records of each partition and
/// a count bound, which a record another producer wrote into the restored
/// name can pass.
pub const ORIGINAL_NAME_NEEDS_COMPLETE_COVERAGE: &str = "OriginalNameNeedsCompleteCoverage";
/// An `original_name` block in a plan that states `restore.partitions`: a
/// restore under the original topic names restores WHOLE topics. The creation
/// step creates each topic under its production name with every partition the
/// archive lists, so a partition subset would leave the other partitions of
/// that name empty, and they could never be restored under it afterwards
/// (the name exists, and a restore into an existing topic is refused).
pub const ORIGINAL_NAME_NEEDS_WHOLE_TOPICS: &str = "OriginalNameNeedsWholeTopics";
/// The target is (or may be) the source cluster and a broker reports
/// `auto.create.topics.enable=true`: a producer still pointed at the name
/// would create it under the restore.
pub const ORIGINAL_NAME_AUTO_CREATE_ENABLED: &str = "OriginalNameAutoCreateEnabled";
/// The target is (or may be) the source cluster and a broker's
/// `auto.create.topics.enable` could not be read: refuse when unsure.
pub const ORIGINAL_NAME_AUTO_CREATE_UNKNOWN: &str = "OriginalNameAutoCreateUnknown";
/// Nowhere was looked for a declarative owner of the restored names.
pub const ORIGINAL_NAME_OWNER_NOT_CHECKED: &str = "OriginalNameOwnerNotChecked";
/// A declarative owner manages a restored name and the plan did not choose
/// the owner path.
pub const ORIGINAL_NAME_OWNER_PRESENT: &str = "OriginalNameOwnerPresent";
/// The plan's own owner statement is malformed (an unplanned topic, another
/// kind, an unusable reference, a topic twice).
pub const ORIGINAL_NAME_OWNERS_INVALID: &str = "OriginalNameOwnersInvalid";
/// The name phase 0 would create the `LogAppendTime` probe under is taken, or
/// is not a name the probe may use.
pub const ORIGINAL_NAME_PROBE_UNUSABLE: &str = "OriginalNameProbeUnusable";
/// The approval's signed subject is not the plan's.
pub const APPROVAL_SUBJECT_MISMATCH: &str = "ApprovalSubjectMismatch";
/// A mapped target name that phase 0 proved absent exists when the run comes
/// to create it: the race is lost by name, and nothing is written into it.
/// Both modes: the creation step is one step.
pub const TARGET_TOPIC_APPEARED: &str = "TargetTopicAppeared";
/// OD-10: a one-person confirmation of an original-name restore that carries
/// no typed topic names.
pub const ORIGINAL_NAME_CONFIRMATION_MISSING: &str = "OriginalNameConfirmationMissing";
/// OD-10: the typed topic names are not exactly the plan's source topics.
pub const ORIGINAL_NAME_CONFIRMATION_MISMATCH: &str = "OriginalNameConfirmationMismatch";
/// OD-10: typed topic names on an authorization that is not a one-person
/// confirmation of an original-name restore — a field nothing else reads.
pub const ORIGINAL_NAME_CONFIRMATION_NOT_ACCEPTED: &str = "OriginalNameConfirmationNotAccepted";
/// A `KafkaTopic` resource the runner was given that it cannot read, or whose
/// reference it cannot record: an owner it cannot see is never dropped.
pub const ORIGINAL_NAME_OWNER_UNREADABLE: &str = "OriginalNameOwnerUnreadable";

// ---------------------------------------------------------------------------
// The approval subject
// ---------------------------------------------------------------------------

/// The wire value of the separate approval subject an original-name restore
/// needs, inside the SIGNED approval document: v1's `approval_subject`, v2's
/// `approvalSubject`. An ordinary approval carries no such key, so every
/// document written before PROD-15.1 is an ordinary approval byte for byte.
pub const APPROVAL_SUBJECT_ORIGINAL_NAME: &str = "originalName";

/// The label an ordinary approval is SHOWN with (console, API, scorecard
/// readers). It is never written into a signed document.
pub const APPROVAL_SUBJECT_ORDINARY: &str = "ordinary";

/// What an approval authorises beyond its plan hash: an ordinary restore, or
/// a restore under the original topic names.
///
/// THE PLAN HASH ALREADY BINDS THE PLAN, and this is not a second copy of it.
/// It is the approver's separate, explicit statement that they are approving
/// a write under the production names — so an approval minted for "this plan"
/// without reading what the plan restores into cannot authorise the one path
/// that does, and the console and API can show the two apart before anyone
/// signs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApprovalSubject {
    /// Every restore that is not an original-name restore.
    Ordinary,
    /// A restore under the original topic names (PROD-15.1).
    OriginalName,
}

impl ApprovalSubject {
    /// The subject a plan needs: `OriginalName` exactly when it carries
    /// `target.topic_naming.original_name`, in any mode — a scratch plan that
    /// carries the block is refused by [`refuse_shape`], and asking an
    /// approver for the original-name subject first is the safe order.
    #[must_use]
    pub fn of_plan(spec: &DrillSpec) -> Self {
        if spec.target.original_name().is_some() {
            Self::OriginalName
        } else {
            Self::Ordinary
        }
    }

    /// The value written into a signed document: `None` for an ordinary
    /// approval (no key at all), `Some("originalName")` otherwise.
    #[must_use]
    pub const fn wire(self) -> Option<&'static str> {
        match self {
            Self::Ordinary => None,
            Self::OriginalName => Some(APPROVAL_SUBJECT_ORIGINAL_NAME),
        }
    }

    /// The label shown for it.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Ordinary => APPROVAL_SUBJECT_ORDINARY,
            Self::OriginalName => APPROVAL_SUBJECT_ORIGINAL_NAME,
        }
    }

    /// Read a signed document's value. ABSENT is an ordinary approval; the one
    /// legal present value is `originalName`. Anything else is refused rather
    /// than read as either: a subject this build does not know is a subject
    /// whose meaning it cannot enforce.
    ///
    /// # Errors
    ///
    /// A sentence naming the value.
    pub fn from_wire(value: Option<&str>) -> Result<Self, String> {
        match value {
            None => Ok(Self::Ordinary),
            Some(APPROVAL_SUBJECT_ORIGINAL_NAME) => Ok(Self::OriginalName),
            Some(other) => Err(format!(
                "{APPROVAL_SUBJECT_MISMATCH}: the approval document's approval subject is \
                 {other:?}; the one value this build knows is {APPROVAL_SUBJECT_ORIGINAL_NAME:?} \
                 (an ordinary approval carries no subject at all)"
            )),
        }
    }
}

impl std::fmt::Display for ApprovalSubject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.label())
    }
}

/// The approval's subject must BE the plan's — in both directions.
///
/// An ordinary approval never authorises an original-name restore: that is
/// the separate approval OD-2 and the tracker's rule 5 require. And an
/// original-name approval never authorises an ordinary plan either: the plan
/// hash would already refuse a different plan, so a mismatch here means the
/// approver and the plan disagree about what is being restored, and running it
/// would act on one of two readings.
///
/// # Errors
///
/// A refusal opening with [`APPROVAL_SUBJECT_MISMATCH`] that names both.
pub fn check_approval_subject(
    plan: ApprovalSubject,
    approved: ApprovalSubject,
) -> Result<(), String> {
    if plan == approved {
        return Ok(());
    }
    Err(match plan {
        ApprovalSubject::OriginalName => format!(
            "{APPROVAL_SUBJECT_MISMATCH}: this plan restores under the ORIGINAL topic names \
             (target.topic_naming.original_name), and its approval is an ordinary one. An \
             original-name restore needs its own approval, whose signed approval subject is \
             {APPROVAL_SUBJECT_ORIGINAL_NAME:?} (`logweir drill approve --approval-subject \
             original-name`, or the console's original-name confirmation); no data operation was \
             started"
        ),
        ApprovalSubject::Ordinary => format!(
            "{APPROVAL_SUBJECT_MISMATCH}: the approval's signed approval subject is \
             {APPROVAL_SUBJECT_ORIGINAL_NAME:?} and this plan does not restore under the original \
             topic names; approve the plan as an ordinary restore; no data operation was started"
        ),
    })
}

// ---------------------------------------------------------------------------
// Condition 5b: the typed confirmation (OD-10, 2026-10-09)
// ---------------------------------------------------------------------------

/// The scorecard's `target.original_name.confirmation` for an authorization
/// the requester confirmed alone by re-typing every original topic name.
pub const CONFIRMATION_TYPED_TOPIC_NAMES: &str = "typedTopicNames";

/// The most typed names a confirmation may carry: the product API's own
/// `topicMapping` bound.
pub const MAX_TYPED_TOPICS: usize = 1000;

/// **OD-10 (a), decided by the owner on 2026-10-09.** On an install using
/// one-person confirmation (an `Ordinary` policy), the requester may confirm
/// an original-name restore alone, but only after RE-TYPING every original
/// topic name, exactly; the console signs what was typed into the
/// authorization document v2, beside the approval subject. Two-person and
/// strict namespaces still need the second person, and carry no typed names.
///
/// INSIDE THE SIGNED BYTES (`RestoreAuthorization::original_name_confirmation`,
/// wire `originalNameConfirmation`), so the runner and the controller can hold
/// the typed names to the plan they execute — not to a request field the
/// console could have filled in itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct OriginalNameConfirmation {
    /// The original topic names as the requester typed them, in the order
    /// typed. They must be EXACTLY the plan's `source.topics`: each once,
    /// nothing else, byte for byte.
    pub typed_topics: Vec<String>,
}

/// What a typed list gets wrong against the plan's topics, in words, or
/// `None` when it is exactly them. Names are quoted (`{:?}`) and each list is
/// bounded, so a hostile typed name cannot reach a log or a status unescaped.
#[must_use]
pub fn typed_topics_mismatch(plan_topics: &[String], typed: &[String]) -> Option<String> {
    let expected: BTreeSet<&str> = plan_topics.iter().map(String::as_str).collect();
    let mut seen: BTreeSet<&str> = BTreeSet::new();
    let mut repeated: BTreeSet<&str> = BTreeSet::new();
    for t in typed {
        if !seen.insert(t.as_str()) {
            repeated.insert(t.as_str());
        }
    }
    let missing: Vec<&str> = expected.difference(&seen).copied().collect();
    let extra: Vec<&str> = seen.difference(&expected).copied().collect();
    if missing.is_empty()
        && extra.is_empty()
        && repeated.is_empty()
        && typed.len() <= MAX_TYPED_TOPICS
    {
        return None;
    }
    let shown = |names: &[&str]| {
        let mut out: Vec<String> = names.iter().take(5).map(|n| format!("{n:?}")).collect();
        if names.len() > 5 {
            out.push(format!("and {} more", names.len() - 5));
        }
        out.join(", ")
    };
    let mut parts = Vec::new();
    if !missing.is_empty() {
        parts.push(format!("not typed: {}", shown(&missing)));
    }
    if !extra.is_empty() {
        parts.push(format!(
            "typed but not restored by this plan: {}",
            shown(&extra)
        ));
    }
    if !repeated.is_empty() {
        let repeated: Vec<&str> = repeated.into_iter().collect();
        parts.push(format!("typed more than once: {}", shown(&repeated)));
    }
    if typed.len() > MAX_TYPED_TOPICS {
        parts.push(format!(
            "{} names typed, at most {MAX_TYPED_TOPICS}",
            typed.len()
        ));
    }
    Some(parts.join("; "))
}

/// Condition 5b, at every boundary that reads an authorization: the API
/// before it signs, the controller's admission, the runner at startup and
/// phase 1.
///
/// `one_person` is whether the authorization is a one-person confirmation (an
/// authorization document v2 under an `Ordinary` policy). A v1 approval or a
/// `Governed` document has a second person — the approver's own key — and
/// carries no typed names.
///
/// # Errors
///
/// [`ORIGINAL_NAME_CONFIRMATION_MISSING`] — a one-person confirmation of an
/// original-name plan without typed names; [`ORIGINAL_NAME_CONFIRMATION_MISMATCH`]
/// — typed names that are not exactly the plan's topics, naming the
/// difference; [`ORIGINAL_NAME_CONFIRMATION_NOT_ACCEPTED`] — typed names on
/// any other authorization.
pub fn check_typed_confirmation(
    plan_topics: &[String],
    subject: ApprovalSubject,
    one_person: bool,
    confirmation: Option<&OriginalNameConfirmation>,
) -> Result<(), String> {
    let needed = subject == ApprovalSubject::OriginalName && one_person;
    match (needed, confirmation) {
        (true, Some(c)) => match typed_topics_mismatch(plan_topics, &c.typed_topics) {
            None => Ok(()),
            Some(why) => Err(format!(
                "{ORIGINAL_NAME_CONFIRMATION_MISMATCH}: the topic names the requester typed to \
                 confirm this restore under the ORIGINAL topic names are not exactly the ones \
                 the plan restores ({why}). A one-person confirmation of an original-name \
                 restore needs every original topic name re-typed, exactly (the owner's decision \
                 OD-10); no data operation was started"
            )),
        },
        (true, None) => Err(format!(
            "{ORIGINAL_NAME_CONFIRMATION_MISSING}: this restore under the ORIGINAL topic names \
             was confirmed by its requester alone (a one-person confirmation), and the \
             confirmation carries no typed topic names. On such an install the requester must \
             re-type every original topic name, exactly, before confirming (the owner's \
             decision OD-10); no data operation was started"
        )),
        (false, Some(_)) => Err(format!(
            "{ORIGINAL_NAME_CONFIRMATION_NOT_ACCEPTED}: the authorization carries typed topic \
             names, which only a one-person confirmation of a restore under the ORIGINAL topic \
             names carries; no data operation was started"
        )),
        (false, None) => Ok(()),
    }
}

// ---------------------------------------------------------------------------
// Condition 1: the plan's shape
// ---------------------------------------------------------------------------

/// Whether this plan is an original-name restore the runner may consider at
/// all: the block, `newTopic`, and the empty prefix.
#[must_use]
pub fn is_original_name_restore(spec: &DrillSpec) -> bool {
    spec.target.mode == TargetMode::NewTopic
        && spec.target.original_name().is_some()
        && spec
            .target
            .topic_naming
            .as_ref()
            .is_some_and(|naming| naming.prefix.is_empty())
}

/// Conditions 1, 1b and 1c, purely local: an `original_name` block is legal
/// only in `newTopic` mode, only beside `prefix: ""`, only in a plan that asks
/// for COMPLETE verification (`sample.coverage: complete`), and only in a plan
/// that restores WHOLE topics (no `restore.partitions`). `None` for every
/// plan that carries no block (an empty prefix without one is the mapping
/// guard's, and it keeps refusing it).
///
/// **Why complete, and never sampled.** Under a production name another
/// writer is possible: a producer nobody stopped, pointed at the name the
/// restore is filling. A sampled check compares the first
/// `records_per_partition` records of each sampled partition and holds the
/// partition to the manifest's count BOUND, which is loose whenever the
/// window does not cover whole segments — so a foreign record inside that
/// bound can pass. The complete check compares EVERY restored record with the
/// archive by its `x-original-offset` and reports a record the archive does
/// not hold as unexpected, by offset. Only that is acceptable under an
/// original name, so the plan must ask for it.
///
/// **Why whole topics, and never a partition subset** (PROD-11.1b lifted the
/// general refusal of `restore.partitions`; this is the rule that stays for
/// an original name). The creation step creates each topic under its
/// production name with EVERY partition the archive lists, and the engine
/// fills only the selected ones. The result is a production-named topic whose
/// other partitions are empty, signed as covered, because "covered" then
/// means every SELECTED partition. And it cannot be finished later: the name
/// now exists, and a restore into an existing topic is refused. So the plan
/// is refused before anything is created. A stated window (a start, or an
/// end) restores whole partitions bounded in time, and stays allowed; a
/// subset under a PREFIX is PROD-11.1b's and is untouched.
#[must_use]
pub fn refuse_shape(spec: &DrillSpec) -> Option<String> {
    spec.target.original_name()?;
    if spec.target.mode == TargetMode::Scratch {
        return Some(format!(
            "{ORIGINAL_NAME_NOT_NEW_TOPIC}: target.topic_naming.original_name is set and \
             target.mode is scratch. A scratch drill maps through target.topic_mapping_prefix and \
             tears down what it created, so it never restores under the original names; set \
             target.mode: newTopic for an original-name restore, or remove the block"
        ));
    }
    let prefix = spec
        .target
        .topic_naming
        .as_ref()
        .map_or("", |naming| naming.prefix.as_str());
    if !prefix.is_empty() {
        return Some(format!(
            "{ORIGINAL_NAME_PREFIX_NOT_EMPTY}: target.topic_naming.original_name is set and \
             target.topic_naming.prefix is {prefix:?}. An original-name restore maps every topic \
             onto its own name, which is `prefix: \"\"`; a plan that states both asks for two \
             names at once"
        ));
    }
    if spec.sample.coverage != crate::spec::Coverage::Complete {
        return Some(format!(
            "{ORIGINAL_NAME_NEEDS_COMPLETE_COVERAGE}: target.topic_naming.original_name is set \
             and sample.coverage is `{}`{}. A restore under the original topic names requires \
             complete verification, where every restored record is compared with the archive and \
             a record the archive does not hold is reported by its offset; a sampled check reads \
             only the first records of each partition and a count bound, which a record another \
             producer wrote into the restored name can pass. Set sample.coverage: complete",
            spec.sample.coverage.as_str(),
            if spec.sample.coverage.is_sampled() {
                " (its default)"
            } else {
                ""
            }
        ));
    }
    if !spec.restore.partitions.is_empty() {
        let narrowed = spec
            .restore
            .partitions
            .keys()
            .map(|topic| format!("`{topic}`"))
            .collect::<Vec<_>>()
            .join(", ");
        return Some(format!(
            "{ORIGINAL_NAME_NEEDS_WHOLE_TOPICS}: target.topic_naming.original_name is set and \
             restore.partitions selects a partition subset of {narrowed}. A restore under the \
             original topic names restores whole topics: the run would create each topic under \
             its own name with every partition and fill only the selected ones, and the \
             partitions left out could never be restored under that name afterwards (a restore \
             into an existing topic is refused). Remove restore.partitions to restore every \
             partition (a window start or end may stay), or restore the subset under a prefix \
             (target.topic_naming.prefix) and no original_name block"
        ));
    }
    None
}

// ---------------------------------------------------------------------------
// Condition 3: which cluster, and auto-creation
// ---------------------------------------------------------------------------

/// `cluster_condition`'s value when the target is proven not to be the source
/// cluster.
pub const CLUSTER_CONDITION_TARGET_IS_NOT_SOURCE: &str = "targetIsNotSource";
/// `cluster_condition`'s value when the target is, or may be, the source
/// cluster and every broker reported `auto.create.topics.enable=false`.
pub const CLUSTER_CONDITION_AUTO_CREATE_DISABLED: &str = "autoCreateDisabled";
/// The closed set the scorecard's `target.original_name.cluster_condition`
/// takes (arm ON-5).
pub const CLUSTER_CONDITIONS: [&str; 2] = [
    CLUSTER_CONDITION_TARGET_IS_NOT_SOURCE,
    CLUSTER_CONDITION_AUTO_CREATE_DISABLED,
];

/// The broker setting condition 3 reads on every broker.
pub const AUTO_CREATE_TOPICS_KEY: &str = "auto.create.topics.enable";

/// How the target relates to the cluster the archive was taken from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceRelation {
    /// Every known source cluster id differs from the target's.
    TargetIsNotSource {
        /// The source cluster id compared.
        source: String,
    },
    /// A known source cluster id IS the target's.
    SameCluster {
        /// That id.
        source: String,
    },
    /// No source cluster id is known: no verified receipt, no allowlist entry.
    SourceUnknown,
}

impl SourceRelation {
    /// The source cluster id this relation was decided against, if any.
    #[must_use]
    pub fn source(&self) -> Option<&str> {
        match self {
            Self::TargetIsNotSource { source } | Self::SameCluster { source } => Some(source),
            Self::SourceUnknown => None,
        }
    }
}

/// Condition 3's first half. `known` are the source cluster ids the runner
/// holds as MEASURED facts — the bound point's verified receipt
/// (`source.cluster_id`, read from the broker at backup). Never a value from
/// the plan, and never the allowlist file's `source_cluster_id`: that is
/// unsigned runner input, and a wrong value would skip the auto-creation read
/// (PROD-15.1 review L3).
///
/// "Not the source" needs a known id and every known id to differ: one that
/// equals the target makes it the same cluster, and none at all is unknown.
/// Both of the latter need condition 3's second half.
#[must_use]
pub fn source_relation(known: &[String], target: &str) -> SourceRelation {
    let known: Vec<&String> = known.iter().filter(|id| !id.trim().is_empty()).collect();
    if let Some(same) = known.iter().find(|id| id.as_str() == target) {
        return SourceRelation::SameCluster {
            source: (*same).clone(),
        };
    }
    match known.first() {
        Some(source) => SourceRelation::TargetIsNotSource {
            source: (*source).clone(),
        },
        None => SourceRelation::SourceUnknown,
    }
}

/// Condition 3's second half, over every broker's answer for
/// [`AUTO_CREATE_TOPICS_KEY`] (`(broker id, value)`, `None` where the key was
/// absent from that broker's answer).
///
/// Disabled means EVERY broker says `false`. A single `true` refuses — one
/// broker that auto-creates is enough for a producer to recreate the name —
/// and so does any broker that did not say, and an answer with no broker at
/// all: refuse when unsure.
///
/// # Errors
///
/// [`ORIGINAL_NAME_AUTO_CREATE_ENABLED`] or [`ORIGINAL_NAME_AUTO_CREATE_UNKNOWN`],
/// naming the brokers and the relation.
pub fn require_auto_create_disabled(
    relation: &SourceRelation,
    target: &str,
    answers: &[(i32, Option<String>)],
) -> Result<(), String> {
    let why = match relation {
        SourceRelation::SameCluster { source } => format!(
            "the target cluster {target} is the cluster the archive was taken from ({source})"
        ),
        SourceRelation::SourceUnknown => format!(
            "no source cluster id is known for this archive (the plan is bound to no recovery \
             point, whose verified receipt is the only source id that counts), so the target \
             cluster {target} may be the cluster it was taken from"
        ),
        SourceRelation::TargetIsNotSource { .. } => return Ok(()),
    };
    let enabled: Vec<String> = answers
        .iter()
        .filter(|(_, value)| {
            value
                .as_deref()
                .is_some_and(|v| v.trim().eq_ignore_ascii_case("true"))
        })
        .map(|(broker, _)| broker.to_string())
        .collect();
    if !enabled.is_empty() {
        return Err(format!(
            "{ORIGINAL_NAME_AUTO_CREATE_ENABLED}: {why}, and broker(s) {} report \
             {AUTO_CREATE_TOPICS_KEY}=true. A producer still pointed at a restored name would \
             create it the moment it sends, so the name cannot be held absent until the restore \
             creates it. Restore into a different cluster, or set {AUTO_CREATE_TOPICS_KEY}=false \
             on every broker of the target first; nothing was written",
            enabled.join(", ")
        ));
    }
    let unread: Vec<String> = answers
        .iter()
        .filter(|(_, value)| {
            !value
                .as_deref()
                .is_some_and(|v| v.trim().eq_ignore_ascii_case("false"))
        })
        .map(|(broker, value)| match value {
            Some(v) => format!("{broker} ({v:?})"),
            None => format!("{broker} (not reported)"),
        })
        .collect();
    if answers.is_empty() || !unread.is_empty() {
        return Err(format!(
            "{ORIGINAL_NAME_AUTO_CREATE_UNKNOWN}: {why}, and {} did not report \
             {AUTO_CREATE_TOPICS_KEY}=false. A restore under the original names into the source \
             cluster needs auto-creation PROVEN disabled on every broker; refusing when unsure. \
             Grant DescribeConfigs on the cluster to this principal, or restore into a different \
             cluster; nothing was written",
            if answers.is_empty() {
                "no broker".to_string()
            } else {
                format!("broker(s) {}", unread.join(", "))
            }
        ));
    }
    Ok(())
}

/// The scorecard's `cluster_condition` for a relation that passed condition
/// 3.
#[must_use]
pub fn cluster_condition(relation: &SourceRelation) -> &'static str {
    match relation {
        SourceRelation::TargetIsNotSource { .. } => CLUSTER_CONDITION_TARGET_IS_NOT_SOURCE,
        SourceRelation::SameCluster { .. } | SourceRelation::SourceUnknown => {
            CLUSTER_CONDITION_AUTO_CREATE_DISABLED
        }
    }
}

// ---------------------------------------------------------------------------
// Condition 4: declarative owners
// ---------------------------------------------------------------------------

/// The plan's `original_name.owners`: the approver's statement.
pub const OWNER_FOUND_IN_PLAN: &str = "plan";
/// Strimzi `KafkaTopic` resources the runner was given
/// (`--kafka-topic-resources`), read with
/// `crate::topic_configuration::strimzi_owners`.
pub const OWNER_FOUND_IN_KAFKA_TOPIC_RESOURCES: &str = "kafkaTopicResources";
/// The bound recovery point's verified receipt: the owners its backup
/// recorded for the SOURCE topics (PROD-05.1), counted only where the target
/// may be the source cluster.
pub const OWNER_FOUND_IN_POINT_RECEIPT: &str = "pointReceipt";
/// The closed set of places the runner looks (arm ON-6), in the order a
/// writer lists them.
pub const OWNER_DETECTION_PLACES: [&str; 3] = [
    OWNER_FOUND_IN_PLAN,
    OWNER_FOUND_IN_KAFKA_TOPIC_RESOURCES,
    OWNER_FOUND_IN_POINT_RECEIPT,
];

/// The owners the bound point's verified receipt recorded: its
/// `owner_detection` and each source topic's `topic_configuration[t].owner`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReceiptOwners {
    /// The receipt's `owner_detection` (absent reads as empty: "not checked").
    pub owner_detection: Vec<String>,
    /// The recorded owner per source topic that had one.
    pub owners: BTreeMap<String, TopicOwner>,
}

/// One owner found for a restored name.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct FoundOwner {
    /// The restored (original) topic name.
    pub topic: String,
    /// `strimzi` or `external`.
    pub kind: String,
    /// Where the desired state lives.
    pub reference: String,
    /// Which place named it ([`OWNER_DETECTION_PLACES`]).
    pub found_in: String,
}

/// Where the runner may look, for one run.
#[derive(Debug, Clone, Copy, Default)]
pub struct OwnerInputs<'a> {
    /// The plan's `original_name.owners` (`None`: not declared).
    pub declared: Option<&'a [DeclaredOwner]>,
    /// Strimzi owners found in the `KafkaTopic` resources the runner was given
    /// (`None`: none given; `Some(empty)`: looked, none names a restored
    /// topic).
    pub kafka_topic_resources: Option<&'a BTreeMap<String, TopicOwner>>,
    /// The verified receipt's owners (`None`: no bound point).
    pub receipt: Option<&'a ReceiptOwners>,
}

/// What condition 4 established.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnerVerdict {
    /// Where the run looked, in [`OWNER_DETECTION_PLACES`] order; never empty.
    pub owner_detection: Vec<String>,
    /// Every owner found, sorted.
    pub owners: Vec<FoundOwner>,
    /// Whether the plan chose the owner path.
    pub owner_path: bool,
}

/// Condition 4.
///
/// * The plan's own statement is validated as a backup's `source.topic_owners`
///   is (`refuse_declarations`): each names a restored topic, a known kind and
///   a usable reference, no topic twice.
/// * The receipt's owners count only where its backup LOOKED (a non-empty
///   `owner_detection`) and only where the target is not proven to be another
///   cluster: they describe the SOURCE cluster's owners.
/// * Nowhere looked: refused, and the message says where to look.
/// * An owner found without the owner path: refused, naming every owner.
///
/// # Errors
///
/// A refusal opening with [`ORIGINAL_NAME_OWNERS_INVALID`],
/// [`ORIGINAL_NAME_OWNER_NOT_CHECKED`] or [`ORIGINAL_NAME_OWNER_PRESENT`].
pub fn owner_verdict(
    names: &[String],
    inputs: OwnerInputs<'_>,
    owner_path: bool,
    relation: &SourceRelation,
) -> Result<OwnerVerdict, String> {
    let mut detection: Vec<String> = Vec::new();
    let mut owners: Vec<FoundOwner> = Vec::new();
    if let Some(declared) = inputs.declared {
        if let Some(why) = crate::topic_configuration::refuse_declarations(declared, names) {
            return Err(format!(
                "{ORIGINAL_NAME_OWNERS_INVALID}: target.topic_naming.original_name.owners: {}",
                why.replace("source.topic_owners", "original_name.owners")
                    .replace("source.topics", "the restored topics")
            ));
        }
        detection.push(OWNER_FOUND_IN_PLAN.to_string());
        owners.extend(declared.iter().map(|owner| FoundOwner {
            topic: owner.topic.clone(),
            kind: owner.kind.clone(),
            reference: owner.reference.clone(),
            found_in: OWNER_FOUND_IN_PLAN.to_string(),
        }));
    }
    if let Some(found) = inputs.kafka_topic_resources {
        detection.push(OWNER_FOUND_IN_KAFKA_TOPIC_RESOURCES.to_string());
        owners.extend(found.iter().filter(|(topic, _)| names.contains(topic)).map(
            |(topic, owner)| FoundOwner {
                topic: topic.clone(),
                kind: owner.kind.clone(),
                reference: owner.reference.clone(),
                found_in: OWNER_FOUND_IN_KAFKA_TOPIC_RESOURCES.to_string(),
            },
        ));
    }
    let receipt_applies = !matches!(relation, SourceRelation::TargetIsNotSource { .. });
    if let Some(receipt) = inputs.receipt {
        if receipt_applies && !receipt.owner_detection.is_empty() {
            detection.push(OWNER_FOUND_IN_POINT_RECEIPT.to_string());
            owners.extend(
                receipt
                    .owners
                    .iter()
                    .filter(|(topic, _)| names.contains(topic))
                    .map(|(topic, owner)| FoundOwner {
                        topic: topic.clone(),
                        kind: owner.kind.clone(),
                        reference: owner.reference.clone(),
                        found_in: OWNER_FOUND_IN_POINT_RECEIPT.to_string(),
                    }),
            );
        }
    }
    owners.sort();
    owners.dedup();
    // **THE RECEIPT ADDS OWNERS; IT NEVER STANDS IN FOR LOOKING** (the fix
    // round's sweep of review M2). A backup records a `KafkaTopic` whose
    // reference it cannot record as NO owner (PROD-05.1 warns and goes on), so
    // "the receipt looked and found none" is not proof for a restore that
    // recreates a production name. An owner the receipt names still blocks
    // (below); "none found" needs the approved plan's statement or the
    // target's `KafkaTopic` resources.
    let looked = detection
        .iter()
        .any(|place| place != OWNER_FOUND_IN_POINT_RECEIPT);
    if !looked && (owners.is_empty() || owner_path) {
        return Err(format!(
            "{ORIGINAL_NAME_OWNER_NOT_CHECKED}: nothing was checked for a declarative owner of \
             the restored names (a Strimzi KafkaTopic, a GitOps or Terraform definition){}. Such \
             an owner recreates a deleted name on its own and reverts the restored topic's \
             settings, and this runner cannot read Kubernetes or a repository, so it never \
             assumes there is none. State it in the plan — \
             target.topic_naming.original_name.owners: [] when no owner manages any restored \
             name, or each owner as {{topic, kind, reference}} — or give the runner the \
             target's KafkaTopic resources (--kafka-topic-resources, `kubectl get kafkatopics \
             -A -o yaml`); nothing was written",
            if detection.is_empty() {
                ""
            } else {
                "; the bound point's receipt alone is not a look, because a backup records an \
                 owner it cannot record as none"
            }
        ));
    }
    if !owners.is_empty() && !owner_path {
        let listed: Vec<String> = owners
            .iter()
            .map(|o| {
                format!(
                    "`{}` ({} {}, from {})",
                    o.topic, o.kind, o.reference, o.found_in
                )
            })
            .collect();
        return Err(format!(
            "{ORIGINAL_NAME_OWNER_PRESENT}: a declarative owner manages restored name(s): {}. An \
             owner recreates a deleted name on its own and reverts a restored topic's settings \
             to its desired state, which can delete the restored records. Pause the owner's \
             reconciliation for the restore and choose the owner path explicitly \
             (target.topic_naming.original_name.owner_path: true), or restore under a new name; \
             nothing was written",
            listed.join(", ")
        ));
    }
    Ok(OwnerVerdict {
        owner_detection: detection,
        owners,
        owner_path,
    })
}

// ---------------------------------------------------------------------------
// Condition 7: the probe
// ---------------------------------------------------------------------------

/// The stem of the `LogAppendTime` probe's name in an original-name restore.
pub const PROBE_TOPIC_STEM: &str = "logweir-probe-";

/// The name phase 0's `LogAppendTime` override probe creates and deletes in
/// an original-name restore: `<target.topic_mapping_prefix>logweir-probe-<the
/// plan hash's first twelve hex digits>`.
///
/// NEVER an original name. In every other restore the probe borrows the first
/// mapped target name, which phase 0 has just proved absent and which the
/// restore will create anyway; here that name is a production name, and a
/// probe that could not be deleted would leave a one-partition topic under it.
/// So the probe is moved INTO the scratch namespace the runner's deleter is
/// scoped to (`topic_mapping_prefix`), under a name derived from the approved
/// bytes and nothing else.
#[must_use]
pub fn probe_topic_name(scratch_prefix: &str, plan_hash: &str) -> String {
    let hex: String = plan_hash
        .strip_prefix("sha256:")
        .unwrap_or(plan_hash)
        .chars()
        .filter(char::is_ascii_hexdigit)
        .take(12)
        .collect();
    format!("{scratch_prefix}{PROBE_TOPIC_STEM}{hex}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn owner(topic: &str, kind: &str, reference: &str) -> DeclaredOwner {
        DeclaredOwner {
            topic: topic.into(),
            kind: kind.into(),
            reference: reference.into(),
        }
    }

    fn names() -> Vec<String> {
        vec!["orders".into(), "payments".into()]
    }

    /// OD-10. A one-person confirmation of an original-name plan needs every
    /// original topic name typed, exactly; anything else is refused by name.
    /// KILLS: accepting a missing list; accepting a list missing a name, with
    /// an extra name, a repeated name, a name differing in case or padding;
    /// requiring names of an authorization with a second person; accepting
    /// names on one.
    #[test]
    fn a_one_person_confirmation_needs_every_original_name_typed_exactly() {
        let plan = vec!["orders".to_string(), "payments".to_string()];
        let typed = |names: &[&str]| OriginalNameConfirmation {
            typed_topics: names.iter().map(|n| (*n).to_string()).collect(),
        };
        let on = ApprovalSubject::OriginalName;
        assert!(
            check_typed_confirmation(&plan, on, true, Some(&typed(&["payments", "orders"])))
                .is_ok()
        );
        let refused = |r: Result<(), String>, token: &str| {
            let e = r.expect_err("refused");
            assert!(e.starts_with(&format!("{token}: ")), "{e}");
            e
        };
        refused(
            check_typed_confirmation(&plan, on, true, None),
            ORIGINAL_NAME_CONFIRMATION_MISSING,
        );
        for (bad, says) in [
            (vec!["orders"], "not typed: \"payments\""),
            (
                vec!["orders", "payments", "audit"],
                "typed but not restored by this plan: \"audit\"",
            ),
            (
                vec!["orders", "payments", "orders"],
                "typed more than once: \"orders\"",
            ),
            (vec!["orders", "Payments"], "not typed: \"payments\""),
            (vec!["orders", "payments "], "not typed: \"payments\""),
        ] {
            let e = refused(
                check_typed_confirmation(&plan, on, true, Some(&typed(&bad))),
                ORIGINAL_NAME_CONFIRMATION_MISMATCH,
            );
            assert!(e.contains(says), "{bad:?}: {e}");
        }
        // A second person (v1, Governed, standing): no names, and none accepted.
        assert!(check_typed_confirmation(&plan, on, false, None).is_ok());
        refused(
            check_typed_confirmation(&plan, on, false, Some(&typed(&["orders", "payments"]))),
            ORIGINAL_NAME_CONFIRMATION_NOT_ACCEPTED,
        );
        // An ordinary restore never carries them.
        assert!(check_typed_confirmation(&plan, ApprovalSubject::Ordinary, true, None).is_ok());
        refused(
            check_typed_confirmation(
                &plan,
                ApprovalSubject::Ordinary,
                true,
                Some(&typed(&["orders", "payments"])),
            ),
            ORIGINAL_NAME_CONFIRMATION_NOT_ACCEPTED,
        );
        // The wire shape is closed.
        assert!(serde_json::from_str::<OriginalNameConfirmation>(
            r#"{"typedTopics":["orders"],"clicked":true}"#
        )
        .is_err());
    }

    /// KILLS: an ordinary approval admitted for an original-name plan (the
    /// separate subject), an original-name approval admitted for an ordinary
    /// plan, and a comparison that always passes.
    #[test]
    fn the_approval_subject_must_be_the_plans_in_both_directions() {
        use ApprovalSubject::{Ordinary, OriginalName};
        assert!(check_approval_subject(Ordinary, Ordinary).is_ok());
        assert!(check_approval_subject(OriginalName, OriginalName).is_ok());
        let ordinary_for_original = check_approval_subject(OriginalName, Ordinary).unwrap_err();
        assert!(
            ordinary_for_original.starts_with("ApprovalSubjectMismatch: this plan restores"),
            "{ordinary_for_original}"
        );
        let original_for_ordinary = check_approval_subject(Ordinary, OriginalName).unwrap_err();
        assert!(
            original_for_ordinary.starts_with("ApprovalSubjectMismatch: the approval's"),
            "{original_for_ordinary}"
        );
    }

    /// KILLS: reading an unknown subject as ordinary (or as original-name),
    /// and writing a key for an ordinary approval.
    #[test]
    fn the_wire_value_is_absent_for_ordinary_and_closed_otherwise() {
        assert_eq!(
            ApprovalSubject::from_wire(None),
            Ok(ApprovalSubject::Ordinary)
        );
        assert_eq!(
            ApprovalSubject::from_wire(Some("originalName")),
            Ok(ApprovalSubject::OriginalName)
        );
        for bad in ["ordinary", "OriginalName", "original-name", ""] {
            assert!(
                ApprovalSubject::from_wire(Some(bad)).is_err(),
                "{bad:?} is not a subject"
            );
        }
        assert_eq!(ApprovalSubject::Ordinary.wire(), None);
        assert_eq!(ApprovalSubject::OriginalName.wire(), Some("originalName"));
    }

    /// KILLS: a relation that calls an unknown source "not the source", one
    /// that lets a second, differing id hide an equal one, and a blank id
    /// counted as known.
    #[test]
    fn the_target_is_not_the_source_only_when_a_known_id_differs_and_none_equals() {
        assert_eq!(source_relation(&[], "T"), SourceRelation::SourceUnknown);
        assert_eq!(
            source_relation(&["  ".into()], "T"),
            SourceRelation::SourceUnknown
        );
        assert_eq!(
            source_relation(&["S".into()], "T"),
            SourceRelation::TargetIsNotSource { source: "S".into() }
        );
        assert_eq!(
            source_relation(&["S".into(), "T".into()], "T"),
            SourceRelation::SameCluster { source: "T".into() }
        );
        assert_eq!(
            source_relation(&["T".into()], "T"),
            SourceRelation::SameCluster { source: "T".into() }
        );
    }

    /// KILLS: skipping the read on the same cluster, accepting one broker's
    /// `true` beside another's `false`, accepting an absent value, accepting
    /// an empty answer, and requiring the read on a different cluster.
    #[test]
    fn auto_creation_must_be_proven_disabled_on_every_broker_unless_the_cluster_differs() {
        let same = SourceRelation::SameCluster { source: "T".into() };
        let unknown = SourceRelation::SourceUnknown;
        let other = SourceRelation::TargetIsNotSource { source: "S".into() };
        let f = |v: &str| Some(v.to_string());
        assert!(
            require_auto_create_disabled(&same, "T", &[(1, f("false")), (2, f("false"))]).is_ok()
        );
        assert!(require_auto_create_disabled(&unknown, "T", &[(1, f("FALSE"))]).is_ok());
        let enabled = require_auto_create_disabled(&same, "T", &[(1, f("false")), (2, f("true"))])
            .unwrap_err();
        assert!(
            enabled.starts_with(ORIGINAL_NAME_AUTO_CREATE_ENABLED),
            "{enabled}"
        );
        assert!(enabled.contains("broker(s) 2 report"), "{enabled}");
        let absent =
            require_auto_create_disabled(&same, "T", &[(1, f("false")), (2, None)]).unwrap_err();
        assert!(
            absent.starts_with(ORIGINAL_NAME_AUTO_CREATE_UNKNOWN),
            "{absent}"
        );
        let none = require_auto_create_disabled(&unknown, "T", &[]).unwrap_err();
        assert!(
            none.starts_with(ORIGINAL_NAME_AUTO_CREATE_UNKNOWN),
            "{none}"
        );
        let odd = require_auto_create_disabled(&same, "T", &[(1, f("maybe"))]).unwrap_err();
        assert!(odd.starts_with(ORIGINAL_NAME_AUTO_CREATE_UNKNOWN), "{odd}");
        // A proven different cluster needs no read at all.
        assert!(require_auto_create_disabled(&other, "T", &[(1, f("true"))]).is_ok());
        assert!(require_auto_create_disabled(&other, "T", &[]).is_ok());
        assert_eq!(cluster_condition(&other), "targetIsNotSource");
        assert_eq!(cluster_condition(&same), "autoCreateDisabled");
        assert_eq!(cluster_condition(&unknown), "autoCreateDisabled");
    }

    /// KILLS: reading "looked nowhere" as "no owner", an owner found without
    /// the owner path admitted, a declared owner the plan's topics do not
    /// contain admitted, and the receipt's source owners counted against a
    /// proven different cluster.
    #[test]
    fn an_owner_is_looked_for_and_blocks_unless_the_owner_path_is_chosen() {
        let same = SourceRelation::SameCluster { source: "T".into() };
        let other = SourceRelation::TargetIsNotSource { source: "S".into() };
        // Nowhere looked.
        let none = owner_verdict(&names(), OwnerInputs::default(), false, &same).unwrap_err();
        assert!(none.starts_with(ORIGINAL_NAME_OWNER_NOT_CHECKED), "{none}");
        // The plan's empty statement is a place looked.
        let empty: Vec<DeclaredOwner> = Vec::new();
        let ok = owner_verdict(
            &names(),
            OwnerInputs {
                declared: Some(&empty),
                ..OwnerInputs::default()
            },
            false,
            &same,
        )
        .unwrap();
        assert_eq!(ok.owner_detection, vec!["plan".to_string()]);
        assert!(ok.owners.is_empty());
        // A declared owner blocks without the owner path, and passes with it.
        let declared = vec![owner("orders", "strimzi", "kafka/orders")];
        let blocked = owner_verdict(
            &names(),
            OwnerInputs {
                declared: Some(&declared),
                ..OwnerInputs::default()
            },
            false,
            &same,
        )
        .unwrap_err();
        assert!(
            blocked.starts_with(ORIGINAL_NAME_OWNER_PRESENT),
            "{blocked}"
        );
        assert!(
            blocked.contains("`orders` (strimzi kafka/orders, from plan)"),
            "{blocked}"
        );
        let path = owner_verdict(
            &names(),
            OwnerInputs {
                declared: Some(&declared),
                ..OwnerInputs::default()
            },
            true,
            &same,
        )
        .unwrap();
        assert_eq!(path.owners.len(), 1);
        assert!(path.owner_path);
        // A declaration naming a topic the restore does not restore.
        let stray = vec![owner("ledger", "strimzi", "kafka/ledger")];
        let invalid = owner_verdict(
            &names(),
            OwnerInputs {
                declared: Some(&stray),
                ..OwnerInputs::default()
            },
            true,
            &same,
        )
        .unwrap_err();
        assert!(
            invalid.starts_with(ORIGINAL_NAME_OWNERS_INVALID),
            "{invalid}"
        );
        // KafkaTopic resources: looked, and found.
        let mut found = BTreeMap::new();
        found.insert(
            "payments".to_string(),
            TopicOwner {
                kind: "strimzi".into(),
                basis: "kafkaTopicResource".into(),
                reference: "kafka/payments".into(),
            },
        );
        found.insert(
            "unrelated".to_string(),
            TopicOwner {
                kind: "strimzi".into(),
                basis: "kafkaTopicResource".into(),
                reference: "kafka/unrelated".into(),
            },
        );
        let by_resource = owner_verdict(
            &names(),
            OwnerInputs {
                kafka_topic_resources: Some(&found),
                ..OwnerInputs::default()
            },
            false,
            &other,
        )
        .unwrap_err();
        assert!(
            by_resource.starts_with(ORIGINAL_NAME_OWNER_PRESENT),
            "{by_resource}"
        );
        assert!(!by_resource.contains("unrelated"), "{by_resource}");
        // The receipt: counted on the same cluster, not on a different one,
        // and never where its backup looked nowhere.
        let receipt = ReceiptOwners {
            owner_detection: vec!["kafkaTopicResources".into()],
            owners: found.clone(),
        };
        let on_same = owner_verdict(
            &names(),
            OwnerInputs {
                receipt: Some(&receipt),
                ..OwnerInputs::default()
            },
            false,
            &same,
        )
        .unwrap_err();
        assert!(on_same.contains("from pointReceipt"), "{on_same}");
        let on_other = owner_verdict(
            &names(),
            OwnerInputs {
                receipt: Some(&receipt),
                ..OwnerInputs::default()
            },
            false,
            &other,
        )
        .unwrap_err();
        assert!(
            on_other.starts_with(ORIGINAL_NAME_OWNER_NOT_CHECKED),
            "{on_other}"
        );
        let unchecked = ReceiptOwners::default();
        assert!(owner_verdict(
            &names(),
            OwnerInputs {
                receipt: Some(&unchecked),
                ..OwnerInputs::default()
            },
            false,
            &same,
        )
        .unwrap_err()
        .starts_with(ORIGINAL_NAME_OWNER_NOT_CHECKED));
    }

    /// KILLS: a probe named after an original topic, and one outside the
    /// scratch namespace the deleter is scoped to.
    #[test]
    fn the_probe_lives_under_the_scratch_prefix_and_never_under_an_original_name() {
        let name = probe_topic_name("drill-", "sha256:0123456789abcdef0123");
        assert_eq!(name, "drill-logweir-probe-0123456789ab");
        assert!(name.starts_with("drill-"));
        assert!(crate::guard::topic_name_is_kafka_legal(&name));
    }
}
