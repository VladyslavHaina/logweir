//! **PROD-04.0b: which consumer groups Logweir may capture, and why each other
//! one is excluded or failed** (`docs/to-do/decisions/PROD-04.0-admin-path.md`
//! §4 #1–#2, §5, §6). Pure: the values arrive from librdkafka through
//! `logweir-rdkafka-ffi` and `crate::rdkafka_admin` (feature `client`), and
//! every rule below is unit-tested with no broker.
//!
//! # The rules, as the record says them
//!
//! Every selected id gets exactly ONE [`GroupVerdict`]. Nothing is dropped,
//! and no verdict carries a position: absence is never offset 0.
//!
//! | Where the id was found | Verdict |
//! |---|---|
//! | typed listing (ListConsumerGroups), type Classic | [`GroupVerdict::Capture`], `classic` |
//! | typed listing, type Consumer (KIP-848) | [`GroupVerdict::Capture`], `consumer` |
//! | typed listing, type Unknown or beyond librdkafka's enum (T4) | `excluded: GroupTypeNotCaptured` |
//! | name listing only (share, streams, a non-consumer protocol: T3) | `excluded: GroupTypeNotCaptured` |
//! | name listing only, but the typed listing lost a broker | `failed: TypeUnproven` |
//! | neither, and the listings are COMPLETE ([`ListingCompleteness`]) | `excluded: GroupNotFound` |
//! | neither, listings not complete, targeted describe refused 30 | `failed: NotVisibleToPrincipal` (T14) |
//! | neither, listings not complete ONLY through this principal's visibility, targeted describe answers the classic stand-in "Classic, Dead, no members" with no error | `excluded: GroupNotFound` |
//! | the same answer, but the listings lost a broker | `failed: AbsenceUnproven` |
//! | neither, listings not complete, anything else from the targeted describe | `failed`, with what it said |
//!
//! # The traps guarded here
//!
//! - **T2.** The classic describe answers a non-classic group and an absent id
//!   alike as "simple, Classic, Dead". A group's state and members are
//!   therefore read only for a [`CapturableGroup`], which only
//!   [`GroupListings::classify`] can make, and only when the description's
//!   type agrees with the listing's ([`describe_answer`]).
//! - **T3.** ListConsumerGroups drops every group whose protocol type is not
//!   empty or `consumer`: the name listing is joined in so they are excluded
//!   with a reason, never dropped.
//! - **T4.** A state librdkafka maps to Unknown (`Assigning`, `Reconciling`,
//!   any future one) is [`GroupState::UnknownToClient`] and counts as ACTIVE;
//!   an Unknown group type is "other".
//! - **T6.** One group can be listed more than once (several brokers answering
//!   for it, before librdkafka 2.14.2): entries are merged by id; differing
//!   states merge to `UnknownToClient` (active), differing types fail the id.
//! - **T14.** A listing is complete only with Describe on the cluster
//!   REPORTED, and an id missing from it is "not found" only then, or when a
//!   targeted describe of it answers without error.
//! - **T16's class (a summary that hides a per-item failure).** A typed listing
//!   whose result carries per-broker errors is NOT complete, even though the
//!   call as a whole succeeded (measured: with no broker reachable, librdkafka
//!   answers `Ok` with no group and one `_TIMED_OUT` entry).
//! - **T19 (found by PROD-04.0b).** librdkafka's legacy name listing keeps
//!   only the LAST broker's error (`rdkafka.c:5028`, `:5119` in rdkafka-sys
//!   4.10.0+2.12.1), so on a cluster of several brokers it can lose a broker's
//!   groups and still answer success. Every id of the typed listing must
//!   therefore appear in the name listing, or the listings are not complete.
//!   It cannot catch a lost broker that coordinates only share or streams
//!   groups: a limit stated in the decision record.
//! - **T18's class (bytes that are not text).** An id that is not UTF-8 is
//!   never matched, never lossily converted: it is counted
//!   ([`GroupListings::unreadable_ids`]) and reported.
use crate::access::ClusterAccess;
use crate::positions::TopicPartition;
use std::collections::{BTreeMap, BTreeSet};

/// librdkafka's group-state and group-type enums, and the Kafka codes these
/// rules decide on, as integers (T12).
pub mod code {
    /// `RD_KAFKA_CONSUMER_GROUP_STATE_UNKNOWN`.
    pub const STATE_UNKNOWN: u32 = 0;
    /// `RD_KAFKA_CONSUMER_GROUP_STATE_PREPARING_REBALANCE`.
    pub const STATE_PREPARING_REBALANCE: u32 = 1;
    /// `RD_KAFKA_CONSUMER_GROUP_STATE_COMPLETING_REBALANCE`.
    pub const STATE_COMPLETING_REBALANCE: u32 = 2;
    /// `RD_KAFKA_CONSUMER_GROUP_STATE_STABLE`.
    pub const STATE_STABLE: u32 = 3;
    /// `RD_KAFKA_CONSUMER_GROUP_STATE_DEAD`.
    pub const STATE_DEAD: u32 = 4;
    /// `RD_KAFKA_CONSUMER_GROUP_STATE_EMPTY`.
    pub const STATE_EMPTY: u32 = 5;
    /// `RD_KAFKA_CONSUMER_GROUP_TYPE_UNKNOWN`: also what librdkafka reports
    /// for every group on a broker that serves ListGroups below v5.
    pub const TYPE_UNKNOWN: u32 = 0;
    /// `RD_KAFKA_CONSUMER_GROUP_TYPE_CONSUMER` (KIP-848).
    pub const TYPE_CONSUMER: u32 = 1;
    /// `RD_KAFKA_CONSUMER_GROUP_TYPE_CLASSIC`.
    pub const TYPE_CLASSIC: u32 = 2;
    /// `GROUP_AUTHORIZATION_FAILED`.
    pub const GROUP_AUTHORIZATION_FAILED: i32 = 30;
}

/// The group types Logweir captures (§5).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum GroupType {
    /// A classic group: protocol type `consumer`, or empty (a simple group).
    Classic,
    /// A KIP-848 consumer group.
    Consumer,
}

impl GroupType {
    /// The spelling the snapshot carries (`groupType`).
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            GroupType::Classic => "classic",
            GroupType::Consumer => "consumer",
        }
    }

    /// librdkafka's integer, when it is one of the captured types.
    #[must_use]
    pub fn from_raw(raw: u32) -> Option<GroupType> {
        match raw {
            code::TYPE_CLASSIC => Some(GroupType::Classic),
            code::TYPE_CONSUMER => Some(GroupType::Consumer),
            _ => None,
        }
    }
}

/// A group's state as librdkafka named it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum GroupState {
    /// `PreparingRebalance`.
    PreparingRebalance,
    /// `CompletingRebalance`.
    CompletingRebalance,
    /// `Stable`.
    Stable,
    /// `Dead`.
    Dead,
    /// `Empty`.
    Empty,
    /// librdkafka's Unknown (0), or a value beyond its enum. KIP-848's
    /// `Assigning` and `Reconciling` arrive as this (C6). Treated as ACTIVE
    /// (T4, AP-04.1-5, AP-04.2-5).
    UnknownToClient {
        /// The integer librdkafka returned.
        raw: u32,
    },
}

impl GroupState {
    /// The state from librdkafka's integer.
    #[must_use]
    pub fn from_raw(raw: u32) -> GroupState {
        match raw {
            code::STATE_PREPARING_REBALANCE => GroupState::PreparingRebalance,
            code::STATE_COMPLETING_REBALANCE => GroupState::CompletingRebalance,
            code::STATE_STABLE => GroupState::Stable,
            code::STATE_DEAD => GroupState::Dead,
            code::STATE_EMPTY => GroupState::Empty,
            raw => GroupState::UnknownToClient { raw },
        }
    }

    /// Whether the group may have members, so that applying positions to it
    /// must be refused. Only `Empty` and `Dead` say it has none; a state the
    /// client cannot name is active (T4).
    #[must_use]
    pub fn is_active(self) -> bool {
        !matches!(self, GroupState::Empty | GroupState::Dead)
    }

    /// The spelling the snapshot carries.
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            GroupState::PreparingRebalance => "PreparingRebalance",
            GroupState::CompletingRebalance => "CompletingRebalance",
            GroupState::Stable => "Stable",
            GroupState::Dead => "Dead",
            GroupState::Empty => "Empty",
            GroupState::UnknownToClient { .. } => "stateUnknownToClient",
        }
    }
}

/// One entry of the typed listing (ListConsumerGroups), with a UTF-8 id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypedEntry {
    /// The group id.
    pub group_id: String,
    /// Protocol type empty: a group made by commits alone.
    pub is_simple: bool,
    /// librdkafka's state integer.
    pub state: u32,
    /// librdkafka's type integer.
    pub group_type: u32,
}

/// One entry of the name listing (ListGroups of every type), with a UTF-8 id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NameEntry {
    /// The group id.
    pub group_id: String,
    /// The legacy describe's per-group error code, `0` for none.
    pub error: i32,
}

/// What a targeted DescribeConsumerGroups said about ONE id that no listing
/// shows (§5, T14).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TargetedAnswer {
    /// The describe answered with no error for the id.
    Described {
        /// librdkafka's state integer.
        state: u32,
        /// librdkafka's type integer.
        group_type: u32,
        /// How many members the answer listed.
        members: usize,
    },
    /// The describe answered the id with an error.
    Refused {
        /// The code.
        code: i32,
        /// librdkafka's message.
        message: String,
    },
    /// The call failed as a whole, or answered nothing for this id.
    NoAnswer(String),
}

/// Everything one classification read, before any rule is applied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupListings {
    /// The typed listing's entries, duplicates included.
    pub typed: Vec<TypedEntry>,
    /// The typed listing's per-broker errors, as `(code, message)`; or the
    /// whole call's failure as one entry.
    pub typed_errors: Vec<(i32, String)>,
    /// The name listing's entries.
    pub names: Vec<NameEntry>,
    /// The name listing failed as a whole, or answered `_PARTIAL`.
    pub names_incomplete: Option<String>,
    /// What DescribeCluster said the principal may do on the cluster.
    pub access: ClusterAccess,
    /// Ids that are NULL or not UTF-8, in either listing: counted, never
    /// matched against a selection, never converted lossily.
    pub unreadable_ids: usize,
}

/// Why a classification's listings are not a complete inventory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IncompleteReason {
    /// The broker REPORTED the principal's cluster operations, without
    /// Describe: ListGroups shows it only the groups it may Describe (T14).
    NoDescribeOnCluster,
    /// The broker reported no cluster operations at all.
    ClusterOperationsNotReported,
    /// DescribeCluster failed.
    ClusterOperationsUnread(String),
    /// The typed listing carried per-broker errors (codes).
    TypedListingErrors(Vec<i32>),
    /// The name listing failed or answered `_PARTIAL`.
    NameListingIncomplete(String),
    /// Ids the typed listing shows and the name listing does not (T19).
    NameListingMissesTypedGroups(Vec<String>),
}

impl IncompleteReason {
    /// Whether the reason is about what THIS principal may see (T14), rather
    /// than a broker the listings lost.
    #[must_use]
    pub fn is_visibility(&self) -> bool {
        matches!(
            self,
            IncompleteReason::NoDescribeOnCluster
                | IncompleteReason::ClusterOperationsNotReported
                | IncompleteReason::ClusterOperationsUnread(_)
        )
    }
}

/// Whether an id missing from both listings can be called absent without a
/// targeted call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListingCompleteness {
    /// Describe on the cluster was reported, no listing carried an error, and
    /// the two listings agree (T14, T16, T19).
    Complete,
    /// Every reason the listings are not complete.
    NotComplete(Vec<IncompleteReason>),
}

/// A group Logweir may capture: only [`GroupListings::classify`] makes one,
/// so a state or a member list is never read for a group the typed listing
/// did not classify (T2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapturableGroup {
    group_id: String,
    group_type: GroupType,
    state: GroupState,
    is_simple: bool,
}

impl CapturableGroup {
    /// The group id.
    #[must_use]
    pub fn group_id(&self) -> &str {
        &self.group_id
    }
    /// The type the typed listing gave.
    #[must_use]
    pub fn group_type(&self) -> GroupType {
        self.group_type
    }
    /// The state the typed listing gave (merged over duplicates, T6).
    #[must_use]
    pub fn state(&self) -> GroupState {
        self.state
    }
    /// Whether the protocol type is empty (a simple group).
    #[must_use]
    pub fn is_simple(&self) -> bool {
        self.is_simple
    }
}

/// Why a group is not captured, though nothing failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Excluded {
    /// A share group, a streams group, a classic group of a non-consumer
    /// protocol (only the name listing shows these, T3), or a group whose type
    /// librdkafka could not name (T4). `groupType: other`.
    GroupTypeNotCaptured {
        /// Which of the two cases.
        why: OtherType,
    },
    /// The id names no group.
    GroupNotFound {
        /// What proved it.
        evidence: Absence,
    },
}

/// Why a group's type is "other".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OtherType {
    /// The name listing shows it and the typed listing does not.
    NotInTypedListing,
    /// The typed listing gave a type librdkafka could not name (raw value),
    /// e.g. every group of a broker below ListGroups v5.
    UnknownType {
        /// librdkafka's integer.
        raw: u32,
    },
}

/// What proved an id absent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Absence {
    /// Neither listing shows it, and they are complete.
    CompleteListing,
    /// A targeted describe answered it, without error, with the classic
    /// stand-in for "no such group": Classic, Dead, no members.
    TargetedDescribe,
}

/// Why a group could not be classified.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupFailure {
    /// No listing shows it and a targeted describe was refused with
    /// GROUP_AUTHORIZATION_FAILED: it may exist, and this principal may not
    /// see it. Never "not found" (T14, AP-04.1-6).
    NotVisibleToPrincipal,
    /// The listings contradict themselves or the targeted describe: the same
    /// id typed two ways (T6), or a live group no listing showed (it appeared
    /// during classification).
    ListingInconsistent(String),
    /// No listing shows it, the targeted describe answered the classic
    /// stand-in, but the listings are incomplete because a BROKER was lost
    /// (a typed-listing error, `_PARTIAL`, or T19's mismatch), not only
    /// because of this principal's filtering. On a lost broker the stand-in
    /// cannot tell an absent id from a share or streams group that broker
    /// coordinates (both answer "Classic, Dead", §3.1), so absence is not
    /// proven (PROD-04.0b review L1).
    AbsenceUnproven(String),
    /// Only the name listing shows it, but the TYPED listing lost a broker:
    /// a classic or consumer group that broker coordinates is then missing
    /// from the typed listing exactly as a share or streams group always is
    /// (T3), so "other type" is not proven (the type-side twin of
    /// [`GroupFailure::AbsenceUnproven`]).
    TypeUnproven(String),
    /// The targeted describe failed for another reason, or answered nothing.
    Unreachable {
        /// The code, when there was one.
        code: Option<i32>,
        /// What was said.
        message: String,
    },
}

/// One selected id's verdict.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupVerdict {
    /// §5's captured types: positions may be read for it.
    Capture(CapturableGroup),
    /// Not captured, with a reason.
    Excluded(Excluded),
    /// Not classified, with a reason.
    Failed(GroupFailure),
}

/// The default bound of one admin call (DescribeCluster, a listing, a
/// description, DescribeAcls): the 15 s PROD-04.0 measured its calls with.
pub const DEFAULT_ADMIN_BOUND: std::time::Duration = std::time::Duration::from_secs(15);

/// One classification: a verdict per selected id, and the evidence behind
/// them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupClassification {
    /// One verdict per selected id, in selection order.
    pub verdicts: Vec<(String, GroupVerdict)>,
    /// Whether the listings were complete, and if not, why.
    pub completeness: ListingCompleteness,
    /// The ids a targeted describe was sent for.
    pub targeted: Vec<String>,
    /// Listed ids that were NULL or not UTF-8 (never matched).
    pub unreadable_ids: usize,
}

/// The selected ids, refused before any call when blank; duplicates collapse
/// to one entry, in first-seen order ("exactly one entry per id").
pub fn selected_ids(selected: &[String]) -> Result<Vec<String>, String> {
    let mut seen = BTreeSet::new();
    let mut out = Vec::with_capacity(selected.len());
    for g in selected {
        if g.trim().is_empty() {
            return Err("a selected group id is never blank".to_string());
        }
        if seen.insert(g.as_str()) {
            out.push(g.clone());
        }
    }
    Ok(out)
}

/// What a description call answered for one id: the input [`classify_with`]
/// maps to a [`TargetedAnswer`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DescribeAnswer {
    /// The call answered the id: its per-group error, or its description.
    Answered {
        /// The per-group error, as `(code, message)`.
        error: Option<(i32, String)>,
        /// librdkafka's state integer.
        state: u32,
        /// librdkafka's type integer.
        group_type: u32,
        /// How many members the answer listed.
        members: usize,
    },
    /// The call failed as a whole for the id.
    CallFailed(String),
}

/// **§5 for every selected id, given the listings and a describe call**: the
/// whole decision, pure, so it is unit-tested with a fake `describe`
/// (PROD-04.0b review L3). `describe` is called once, with exactly the ids
/// [`GroupListings::needs_targeted_describe`] names, and only when there are
/// any; an id it does not answer fails, it is never assumed absent.
///
/// # Errors
///
/// A blank selected id, before `describe` is called.
pub fn classify_with(
    listings: &GroupListings,
    selected: &[String],
    describe: impl FnOnce(&[String]) -> BTreeMap<String, DescribeAnswer>,
) -> Result<GroupClassification, String> {
    let selected = selected_ids(selected)?;
    let targeted_ids = listings.needs_targeted_describe(&selected);
    let mut targeted = BTreeMap::new();
    if !targeted_ids.is_empty() {
        let answers = describe(&targeted_ids);
        for g in &targeted_ids {
            let answer = match answers.get(g) {
                None => TargetedAnswer::NoAnswer(format!("the describe answered nothing for {g}")),
                Some(DescribeAnswer::CallFailed(e)) => TargetedAnswer::NoAnswer(e.clone()),
                Some(DescribeAnswer::Answered {
                    error: Some((code, message)),
                    ..
                }) => TargetedAnswer::Refused {
                    code: *code,
                    message: message.clone(),
                },
                Some(DescribeAnswer::Answered {
                    error: None,
                    state,
                    group_type,
                    members,
                }) => TargetedAnswer::Described {
                    state: *state,
                    group_type: *group_type,
                    members: *members,
                },
            };
            targeted.insert(g.clone(), answer);
        }
    }
    Ok(GroupClassification {
        verdicts: listings.classify(&selected, &targeted),
        completeness: listings.completeness(),
        targeted: targeted_ids,
        unreadable_ids: listings.unreadable_ids,
    })
}

/// The typed listing merged by id (T6): one `(type, state, simple)` per id,
/// or why the entries contradict each other.
type Merged = Result<(u32, GroupState, bool), String>;

impl GroupListings {
    /// The typed listing, merged by id (T6).
    fn typed_by_id(&self) -> BTreeMap<&str, Merged> {
        let mut out: BTreeMap<&str, Merged> = BTreeMap::new();
        for e in &self.typed {
            let state = GroupState::from_raw(e.state);
            match out.get_mut(e.group_id.as_str()) {
                None => {
                    out.insert(&e.group_id, Ok((e.group_type, state, e.is_simple)));
                }
                Some(Err(_)) => {}
                Some(Ok((ty, st, simple))) => {
                    if *ty != e.group_type || *simple != e.is_simple {
                        let why = format!(
                            "{}: the typed listing names it twice, as type {} and {} (T6)",
                            e.group_id, ty, e.group_type
                        );
                        out.insert(&e.group_id, Err(why));
                    } else if *st != state {
                        // Two brokers disagree on the state: the client cannot
                        // say which is current, so it is unknown, and active.
                        *st = GroupState::UnknownToClient {
                            raw: code::STATE_UNKNOWN,
                        };
                    }
                }
            }
        }
        out
    }

    /// Whether an id missing from both listings is absent (T14, T16, T19).
    #[must_use]
    pub fn completeness(&self) -> ListingCompleteness {
        let mut why = Vec::new();
        match &self.access {
            ClusterAccess::Reported(_) if self.access.describe() == Some(true) => {}
            ClusterAccess::Reported(_) => why.push(IncompleteReason::NoDescribeOnCluster),
            ClusterAccess::NotReported => why.push(IncompleteReason::ClusterOperationsNotReported),
            ClusterAccess::Unread(e) => {
                why.push(IncompleteReason::ClusterOperationsUnread(e.clone()))
            }
        }
        if !self.typed_errors.is_empty() {
            why.push(IncompleteReason::TypedListingErrors(
                self.typed_errors.iter().map(|(c, _)| *c).collect(),
            ));
        }
        if let Some(e) = &self.names_incomplete {
            why.push(IncompleteReason::NameListingIncomplete(e.clone()));
        }
        let names: BTreeSet<&str> = self.names.iter().map(|n| n.group_id.as_str()).collect();
        let missing: Vec<String> = self
            .typed
            .iter()
            .map(|t| t.group_id.as_str())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .filter(|id| !names.contains(id))
            .map(str::to_string)
            .collect();
        if !missing.is_empty() {
            why.push(IncompleteReason::NameListingMissesTypedGroups(missing));
        }
        if why.is_empty() {
            ListingCompleteness::Complete
        } else {
            ListingCompleteness::NotComplete(why)
        }
    }

    /// The selected ids that need a targeted describe: those no listing shows,
    /// when the listings are not complete.
    #[must_use]
    pub fn needs_targeted_describe(&self, selected: &[String]) -> Vec<String> {
        if self.completeness() == ListingCompleteness::Complete {
            return Vec::new();
        }
        let typed = self.typed_by_id();
        let names: BTreeSet<&str> = self.names.iter().map(|n| n.group_id.as_str()).collect();
        selected
            .iter()
            .filter(|g| !typed.contains_key(g.as_str()) && !names.contains(g.as_str()))
            .cloned()
            .collect()
    }

    /// **§5, for every selected id**: one verdict each, in `selected`'s order
    /// (duplicates already collapsed by [`selected_ids`]). `targeted` holds
    /// the answer for each id [`Self::needs_targeted_describe`] named; an id
    /// that needed one and has none fails, it is never assumed absent.
    #[must_use]
    pub fn classify(
        &self,
        selected: &[String],
        targeted: &BTreeMap<String, TargetedAnswer>,
    ) -> Vec<(String, GroupVerdict)> {
        let typed = self.typed_by_id();
        let names: BTreeSet<&str> = self.names.iter().map(|n| n.group_id.as_str()).collect();
        let completeness = self.completeness();
        let complete = completeness == ListingCompleteness::Complete;
        // The stand-in proves absence only against FILTERING (§3.9): when
        // every reason the listings are incomplete is about this principal's
        // visibility, a describable id would have been listed.
        let lost: Vec<String> = match &completeness {
            ListingCompleteness::Complete => Vec::new(),
            ListingCompleteness::NotComplete(why) => why
                .iter()
                .filter(|r| !r.is_visibility())
                .map(|r| format!("{r:?}"))
                .collect(),
        };
        selected
            .iter()
            .map(|id| {
                let verdict =
                    match typed.get(id.as_str()) {
                        Some(Err(why)) => {
                            GroupVerdict::Failed(GroupFailure::ListingInconsistent(why.clone()))
                        }
                        Some(Ok((raw_type, state, is_simple))) => {
                            match GroupType::from_raw(*raw_type) {
                                Some(group_type) => GroupVerdict::Capture(CapturableGroup {
                                    group_id: id.clone(),
                                    group_type,
                                    state: *state,
                                    is_simple: *is_simple,
                                }),
                                None => GroupVerdict::Excluded(Excluded::GroupTypeNotCaptured {
                                    why: OtherType::UnknownType { raw: *raw_type },
                                }),
                            }
                        }
                        None if names.contains(id.as_str()) && self.typed_errors.is_empty() => {
                            GroupVerdict::Excluded(Excluded::GroupTypeNotCaptured {
                                why: OtherType::NotInTypedListing,
                            })
                        }
                        None if names.contains(id.as_str()) => {
                            GroupVerdict::Failed(GroupFailure::TypeUnproven(format!(
                            "only the name listing shows it, and the typed listing lost a broker \
                             (codes {:?}): a classic or consumer group that broker coordinates \
                             would be missing from it too",
                            self.typed_errors.iter().map(|(c, _)| *c).collect::<Vec<_>>()
                        )))
                        }
                        None if complete => GroupVerdict::Excluded(Excluded::GroupNotFound {
                            evidence: Absence::CompleteListing,
                        }),
                        None => targeted_verdict(targeted.get(id), &lost),
                    };
                (id.clone(), verdict)
            })
            .collect()
    }
}

/// What a targeted describe of an unlisted id means (§5, T14).
fn targeted_verdict(answer: Option<&TargetedAnswer>, lost: &[String]) -> GroupVerdict {
    match answer {
        None => GroupVerdict::Failed(GroupFailure::Unreachable {
            code: None,
            message: "no listing shows the id and no targeted describe answered for it".to_string(),
        }),
        Some(TargetedAnswer::Refused { code, .. }) if *code == code::GROUP_AUTHORIZATION_FAILED => {
            GroupVerdict::Failed(GroupFailure::NotVisibleToPrincipal)
        }
        Some(TargetedAnswer::Refused { code, message }) => {
            GroupVerdict::Failed(GroupFailure::Unreachable {
                code: Some(*code),
                message: message.clone(),
            })
        }
        Some(TargetedAnswer::NoAnswer(why)) => GroupVerdict::Failed(GroupFailure::Unreachable {
            code: None,
            message: why.clone(),
        }),
        // The classic stand-in, answered without error: the id is describable,
        // so a FILTERED listing would have shown a group of that id (§3.9).
        // A listing that lost a broker would not have: no proof (review L1).
        Some(TargetedAnswer::Described {
            state: code::STATE_DEAD,
            group_type: code::TYPE_CLASSIC,
            members: 0,
        }) if lost.is_empty() => GroupVerdict::Excluded(Excluded::GroupNotFound {
            evidence: Absence::TargetedDescribe,
        }),
        Some(TargetedAnswer::Described {
            state: code::STATE_DEAD,
            group_type: code::TYPE_CLASSIC,
            members: 0,
        }) => GroupVerdict::Failed(GroupFailure::AbsenceUnproven(format!(
            "the targeted describe answered the classic stand-in, but the listings lost a broker              ({}): there it cannot tell an absent id from a share or streams group",
            lost.join(", ")
        ))),
        Some(TargetedAnswer::Described {
            state,
            group_type,
            members,
        }) => GroupVerdict::Failed(GroupFailure::ListingInconsistent(format!(
            "no listing showed the id, and a targeted describe found a group (type {group_type}, \
             state {state}, {members} member(s)): it appeared during classification"
        ))),
    }
}

/// One member of a described group. Descriptive text that is NULL or not
/// UTF-8 is withheld (`None`), never converted lossily (T18's class).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberDescription {
    /// The member's `client.id`.
    pub client_id: Option<String>,
    /// The member id the coordinator gave it.
    pub consumer_id: Option<String>,
    /// Its `group.instance.id`, for a static member.
    pub group_instance_id: Option<String>,
    /// The host it connected from.
    pub host: Option<String>,
    /// Its current assignment.
    pub assignment: Vec<TopicPartition>,
    /// Its target assignment (KIP-848 groups only).
    pub target_assignment: Option<Vec<TopicPartition>>,
}

/// A captured group's description.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupDescription {
    /// The group id.
    pub group_id: String,
    /// The type, agreed by the listing and the description.
    pub group_type: GroupType,
    /// The DESCRIBED state (fresher than the listing's).
    pub state: GroupState,
    /// Whether the protocol type is empty.
    pub is_simple: bool,
    /// The partition assignor.
    pub partition_assignor: Option<String>,
    /// The coordinator's broker id.
    pub coordinator: Option<i32>,
    /// The members.
    pub members: Vec<MemberDescription>,
}

/// A description as librdkafka returned it, in this crate's terms: the input
/// of [`describe_answer`]. Identifier text that is not UTF-8 is `Err(bytes)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribedObservation {
    /// The per-group error, as `(code, message)`.
    pub error: Option<(i32, String)>,
    /// librdkafka's state integer.
    pub state: u32,
    /// librdkafka's type integer.
    pub group_type: u32,
    /// Whether the protocol type is empty.
    pub is_simple: bool,
    /// The assignor.
    pub partition_assignor: Option<String>,
    /// The coordinator.
    pub coordinator: Option<i32>,
    /// The members.
    pub members: Vec<MemberDescription>,
    /// Topic names of an assignment that were NULL or not UTF-8.
    pub unreadable_topics: usize,
}

/// Why a capturable group's description was not taken.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DescribeFailure {
    /// GROUP_AUTHORIZATION_FAILED: this principal may not Describe it.
    NotAuthorized,
    /// The description's type is not the listing's (T2: the classic stand-in
    /// for a group that vanished or is not classic).
    TypeDisagrees {
        /// What the typed listing said.
        listed: GroupType,
        /// librdkafka's type integer in the description.
        described: u32,
    },
    /// An assignment named a topic that is not UTF-8 text.
    NotRepresentable(String),
    /// Another error, or no answer for the id.
    Failed {
        /// The code, when there was one.
        code: Option<i32>,
        /// What was said.
        message: String,
    },
}

/// **The description of a captured group, refused unless it agrees with the
/// listing (T2).** `observed` is `None` when the call answered nothing for
/// the id.
pub fn describe_answer(
    group: &CapturableGroup,
    observed: Option<&DescribedObservation>,
) -> Result<GroupDescription, DescribeFailure> {
    let Some(d) = observed else {
        return Err(DescribeFailure::Failed {
            code: None,
            message: format!(
                "{}: the description answered nothing for it",
                group.group_id
            ),
        });
    };
    if let Some((c, message)) = &d.error {
        return Err(if *c == code::GROUP_AUTHORIZATION_FAILED {
            DescribeFailure::NotAuthorized
        } else {
            DescribeFailure::Failed {
                code: Some(*c),
                message: message.clone(),
            }
        });
    }
    if GroupType::from_raw(d.group_type) != Some(group.group_type) {
        return Err(DescribeFailure::TypeDisagrees {
            listed: group.group_type,
            described: d.group_type,
        });
    }
    if d.unreadable_topics > 0 {
        return Err(DescribeFailure::NotRepresentable(format!(
            "{}: {} assigned topic name(s) are not UTF-8 text",
            group.group_id, d.unreadable_topics
        )));
    }
    Ok(GroupDescription {
        group_id: group.group_id.clone(),
        group_type: group.group_type,
        state: GroupState::from_raw(d.state),
        is_simple: d.is_simple,
        partition_assignor: d.partition_assignor.clone(),
        coordinator: d.coordinator,
        members: d.members.clone(),
    })
}

#[cfg(test)]
mod tests {
    //! One row per trap; each names the mutant it kills.
    use super::*;

    fn typed(id: &str, ty: u32, state: u32) -> TypedEntry {
        TypedEntry {
            group_id: id.to_string(),
            is_simple: false,
            state,
            group_type: ty,
        }
    }

    fn name(id: &str) -> NameEntry {
        NameEntry {
            group_id: id.to_string(),
            error: 0,
        }
    }

    fn ids(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    /// §3.1's 4.3.1 fixture, as the two listings saw it for a super user.
    fn fixture(access: ClusterAccess) -> GroupListings {
        use code::*;
        GroupListings {
            typed: vec![
                typed("pa-classic-empty", TYPE_CLASSIC, STATE_EMPTY),
                typed("pa-classic-live", TYPE_CLASSIC, STATE_STABLE),
                typed("pa-consumer-empty", TYPE_CONSUMER, STATE_EMPTY),
                typed("pa-consumer-live", TYPE_CONSUMER, STATE_STABLE),
            ],
            typed_errors: vec![],
            names: [
                "pa-classic-empty",
                "pa-classic-live",
                "pa-consumer-empty",
                "pa-consumer-live",
                "pa-share-idle",
                "pa-share-live",
                "logweir-e2e-streams-protocol",
            ]
            .iter()
            .map(|n| name(n))
            .collect(),
            names_incomplete: None,
            access,
            unreadable_ids: 0,
        }
    }

    fn all_ops() -> ClusterAccess {
        ClusterAccess::Reported(vec![3, 4, 5, 6, 7, 8, 9, 10, 11, 12])
    }

    fn capture(v: &GroupVerdict) -> &CapturableGroup {
        match v {
            GroupVerdict::Capture(c) => c,
            other => panic!("expected Capture, got {other:?}"),
        }
    }

    /// AP-04.1-1 over recorded rows. Mutants killed: classifying from the
    /// typed listing alone (drops share and streams, T3); typing an absent id
    /// from a describe (T2); a verdict count that is not one per id.
    #[test]
    fn every_selected_id_gets_exactly_one_verdict_and_each_type_its_rule() {
        let l = fixture(all_ops());
        let selected = ids(&[
            "pa-classic-empty",
            "pa-classic-live",
            "pa-consumer-empty",
            "pa-consumer-live",
            "pa-share-idle",
            "pa-share-live",
            "logweir-e2e-streams-protocol",
            "pa-absent",
        ]);
        assert_eq!(l.completeness(), ListingCompleteness::Complete);
        assert!(l.needs_targeted_describe(&selected).is_empty());
        let v = l.classify(&selected, &BTreeMap::new());
        assert_eq!(
            v.iter().map(|(id, _)| id.clone()).collect::<Vec<_>>(),
            selected
        );
        let c = capture(&v[0].1);
        assert_eq!(
            (c.group_type(), c.state()),
            (GroupType::Classic, GroupState::Empty)
        );
        assert_eq!(capture(&v[1].1).state(), GroupState::Stable);
        assert_eq!(capture(&v[2].1).group_type(), GroupType::Consumer);
        assert_eq!(capture(&v[3].1).group_type(), GroupType::Consumer);
        for (id, verdict) in &v[4..7] {
            assert_eq!(
                verdict,
                &GroupVerdict::Excluded(Excluded::GroupTypeNotCaptured {
                    why: OtherType::NotInTypedListing
                }),
                "{id}"
            );
        }
        assert_eq!(
            v[7].1,
            GroupVerdict::Excluded(Excluded::GroupNotFound {
                evidence: Absence::CompleteListing
            })
        );
    }

    /// T14 and AP-04.1-6. Mutants killed: calling an unlisted id absent on a
    /// filtered listing; reading a targeted 30 as absent; reading "not
    /// reported" as complete; assuming absence when the targeted answer is
    /// missing.
    #[test]
    fn an_unlisted_id_is_absent_only_on_a_complete_listing_or_a_describable_answer() {
        let selected = ids(&["pa-hidden", "pa-absent", "pa-odd", "pa-lost", "pa-new"]);
        for access in [
            ClusterAccess::Reported(vec![]),
            ClusterAccess::Reported(vec![10]),
            ClusterAccess::NotReported,
            ClusterAccess::Unread("timed out".into()),
        ] {
            let mut l = fixture(access.clone());
            l.names.retain(|n| n.group_id != "pa-hidden");
            assert!(
                matches!(l.completeness(), ListingCompleteness::NotComplete(_)),
                "{access:?}"
            );
            assert_eq!(l.needs_targeted_describe(&selected), selected, "{access:?}");
            let mut t = BTreeMap::new();
            t.insert(
                "pa-hidden".to_string(),
                TargetedAnswer::Refused {
                    code: 30,
                    message: "Broker: Group authorization failed".into(),
                },
            );
            t.insert(
                "pa-absent".to_string(),
                TargetedAnswer::Described {
                    state: code::STATE_DEAD,
                    group_type: code::TYPE_CLASSIC,
                    members: 0,
                },
            );
            t.insert(
                "pa-odd".to_string(),
                TargetedAnswer::Refused {
                    code: 15,
                    message: "coordinator not available".into(),
                },
            );
            t.insert(
                "pa-new".to_string(),
                TargetedAnswer::Described {
                    state: code::STATE_STABLE,
                    group_type: code::TYPE_CONSUMER,
                    members: 1,
                },
            );
            let v: BTreeMap<String, GroupVerdict> = l.classify(&selected, &t).into_iter().collect();
            assert_eq!(
                v["pa-hidden"],
                GroupVerdict::Failed(GroupFailure::NotVisibleToPrincipal)
            );
            assert_eq!(
                v["pa-absent"],
                GroupVerdict::Excluded(Excluded::GroupNotFound {
                    evidence: Absence::TargetedDescribe
                })
            );
            assert!(matches!(
                v["pa-odd"],
                GroupVerdict::Failed(GroupFailure::Unreachable { code: Some(15), .. })
            ));
            assert!(matches!(
                v["pa-lost"],
                GroupVerdict::Failed(GroupFailure::Unreachable { code: None, .. })
            ));
            assert!(matches!(
                v["pa-new"],
                GroupVerdict::Failed(GroupFailure::ListingInconsistent(_))
            ));
        }
    }

    /// The classic stand-in is exactly Classic + Dead + no members. Mutants
    /// killed: dropping any one of the three from the match.
    #[test]
    fn only_the_classic_stand_in_proves_absence() {
        for (state, ty, members) in [
            (code::STATE_EMPTY, code::TYPE_CLASSIC, 0),
            (code::STATE_DEAD, code::TYPE_CONSUMER, 0),
            (code::STATE_DEAD, code::TYPE_CLASSIC, 1),
        ] {
            let v = targeted_verdict(
                Some(&TargetedAnswer::Described {
                    state,
                    group_type: ty,
                    members,
                }),
                &[],
            );
            assert!(
                matches!(
                    v,
                    GroupVerdict::Failed(GroupFailure::ListingInconsistent(_))
                ),
                "{state} {ty} {members}: {v:?}"
            );
        }
    }

    /// T4 and AP-04.1-5 / AP-04.2-5. Mutants killed: Unknown read as Empty
    /// (inactive); a raw state beyond the enum panicking or mapping to a
    /// named state; an Unknown type captured.
    #[test]
    fn an_unknown_state_is_active_and_an_unknown_type_is_other() {
        assert_eq!(
            GroupState::from_raw(0),
            GroupState::UnknownToClient { raw: 0 }
        );
        assert!(GroupState::from_raw(0).is_active());
        assert_eq!(GroupState::from_raw(0).wire_name(), "stateUnknownToClient");
        assert_eq!(
            GroupState::from_raw(9),
            GroupState::UnknownToClient { raw: 9 }
        );
        assert!(GroupState::from_raw(u32::MAX).is_active());
        assert!(!GroupState::Empty.is_active());
        assert!(!GroupState::Dead.is_active());
        for s in [
            GroupState::Stable,
            GroupState::PreparingRebalance,
            GroupState::CompletingRebalance,
        ] {
            assert!(s.is_active(), "{s:?}");
        }
        let mut l = fixture(all_ops());
        l.typed.push(typed(
            "pa-assigning",
            code::TYPE_CONSUMER,
            code::STATE_UNKNOWN,
        ));
        l.typed.push(typed(
            "pa-old-broker",
            code::TYPE_UNKNOWN,
            code::STATE_EMPTY,
        ));
        l.typed.push(typed("pa-future", 3, code::STATE_EMPTY));
        for n in ["pa-assigning", "pa-old-broker", "pa-future"] {
            l.names.push(name(n));
        }
        let v: BTreeMap<String, GroupVerdict> = l
            .classify(
                &ids(&["pa-assigning", "pa-old-broker", "pa-future"]),
                &BTreeMap::new(),
            )
            .into_iter()
            .collect();
        let a = capture(&v["pa-assigning"]);
        assert_eq!(a.state(), GroupState::UnknownToClient { raw: 0 });
        assert!(a.state().is_active());
        assert_eq!(
            v["pa-old-broker"],
            GroupVerdict::Excluded(Excluded::GroupTypeNotCaptured {
                why: OtherType::UnknownType { raw: 0 }
            })
        );
        assert_eq!(
            v["pa-future"],
            GroupVerdict::Excluded(Excluded::GroupTypeNotCaptured {
                why: OtherType::UnknownType { raw: 3 }
            })
        );
    }

    /// T6. Mutants killed: keeping the first duplicate's state; keeping
    /// either duplicate's type; emitting two verdicts for one id.
    #[test]
    fn duplicate_listings_merge_by_id() {
        let mut l = fixture(all_ops());
        l.typed.push(typed(
            "pa-classic-empty",
            code::TYPE_CLASSIC,
            code::STATE_EMPTY,
        ));
        l.typed.push(typed(
            "pa-classic-live",
            code::TYPE_CLASSIC,
            code::STATE_EMPTY,
        ));
        l.typed.push(typed(
            "pa-consumer-empty",
            code::TYPE_CLASSIC,
            code::STATE_EMPTY,
        ));
        let v = l.classify(
            &ids(&["pa-classic-empty", "pa-classic-live", "pa-consumer-empty"]),
            &BTreeMap::new(),
        );
        assert_eq!(v.len(), 3);
        assert_eq!(capture(&v[0].1).state(), GroupState::Empty);
        let live = capture(&v[1].1);
        assert_eq!(live.state(), GroupState::UnknownToClient { raw: 0 });
        assert!(live.state().is_active());
        assert!(
            matches!(&v[2].1, GroupVerdict::Failed(GroupFailure::ListingInconsistent(w)) if w.contains("T6"))
        );
    }

    /// T16's class and T19. Mutants killed: ignoring per-broker errors of an
    /// `Ok` typed listing; ignoring `_PARTIAL`; trusting a name listing that
    /// misses a typed group.
    #[test]
    fn a_listing_that_lost_a_broker_is_never_complete() {
        let mut l = fixture(all_ops());
        l.typed_errors.push((-185, "Local: Timed out".into()));
        assert_eq!(
            l.completeness(),
            ListingCompleteness::NotComplete(vec![IncompleteReason::TypedListingErrors(vec![
                -185
            ])])
        );
        let mut l = fixture(all_ops());
        l.names_incomplete = Some("rd_kafka_list_groups answered _PARTIAL".into());
        assert!(matches!(
            l.completeness(),
            ListingCompleteness::NotComplete(r) if matches!(r[0], IncompleteReason::NameListingIncomplete(_))
        ));
        let mut l = fixture(all_ops());
        l.names.retain(|n| n.group_id != "pa-consumer-live");
        assert_eq!(
            l.completeness(),
            ListingCompleteness::NotComplete(vec![IncompleteReason::NameListingMissesTypedGroups(
                vec!["pa-consumer-live".into()]
            )])
        );
        // ... and an id then missing from both needs a targeted describe.
        assert_eq!(
            l.needs_targeted_describe(&ids(&["pa-absent"])),
            ids(&["pa-absent"])
        );
    }

    /// L1 of the PROD-04.0b review. Mutant killed: reading the classic
    /// stand-in as absence when the listings lost a broker.
    #[test]
    fn the_stand_in_proves_absence_only_against_filtering() {
        let stand_in = TargetedAnswer::Described {
            state: code::STATE_DEAD,
            group_type: code::TYPE_CLASSIC,
            members: 0,
        };
        let mut t = BTreeMap::new();
        t.insert("pa-maybe".to_string(), stand_in);
        // Filtering only: absence is proven.
        let filtered = fixture(ClusterAccess::Reported(vec![]));
        assert_eq!(
            filtered.classify(&ids(&["pa-maybe"]), &t)[0].1,
            GroupVerdict::Excluded(Excluded::GroupNotFound {
                evidence: Absence::TargetedDescribe
            })
        );
        // A lost broker, alone or beside filtering: not proven.
        for access in [all_ops(), ClusterAccess::Reported(vec![])] {
            let mut lost = fixture(access);
            lost.typed_errors.push((-185, "Local: Timed out".into()));
            assert!(
                matches!(
                    &lost.classify(&ids(&["pa-maybe"]), &t)[0].1,
                    GroupVerdict::Failed(GroupFailure::AbsenceUnproven(w)) if w.contains("TypedListingErrors")
                ),
                "{:?}",
                lost.classify(&ids(&["pa-maybe"]), &t)
            );
        }
        let mut partial = fixture(ClusterAccess::NotReported);
        partial.names_incomplete = Some("_PARTIAL".into());
        assert!(matches!(
            partial.classify(&ids(&["pa-maybe"]), &t)[0].1,
            GroupVerdict::Failed(GroupFailure::AbsenceUnproven(_))
        ));
        assert!(IncompleteReason::NoDescribeOnCluster.is_visibility());
        assert!(IncompleteReason::ClusterOperationsUnread("x".into()).is_visibility());
        assert!(!IncompleteReason::TypedListingErrors(vec![-185]).is_visibility());
        assert!(!IncompleteReason::NameListingMissesTypedGroups(vec![]).is_visibility());
    }

    /// The type-side twin of L1. Mutant killed: reading "names only" as
    /// "other type" when the typed listing lost a broker.
    #[test]
    fn other_type_is_proven_only_by_a_typed_listing_that_lost_no_broker() {
        let mut lost = fixture(all_ops());
        lost.typed_errors.push((-185, "Local: Timed out".into()));
        let v = lost.classify(
            &ids(&["pa-share-idle", "pa-classic-live"]),
            &BTreeMap::new(),
        );
        assert!(
            matches!(&v[0].1, GroupVerdict::Failed(GroupFailure::TypeUnproven(w)) if w.contains("-185")),
            "{v:?}"
        );
        assert!(matches!(v[1].1, GroupVerdict::Capture(_)));
    }

    /// L3 of the PROD-04.0b review: the glue's whole decision, with a fake
    /// describe. Mutants killed: skipping the describe; calling it on a
    /// complete listing; describing listed ids; reading a missing answer or
    /// a failed call as absence.
    #[test]
    fn classify_with_describes_exactly_the_unlisted_ids_and_maps_every_answer() {
        // Complete: the describe is never called.
        let complete = fixture(all_ops());
        let c = classify_with(&complete, &ids(&["pa-classic-live", "pa-gone"]), |_| {
            panic!("a complete listing needs no targeted describe")
        })
        .expect("valid");
        assert!(c.targeted.is_empty());
        assert_eq!(
            c.verdicts[1].1,
            GroupVerdict::Excluded(Excluded::GroupNotFound {
                evidence: Absence::CompleteListing
            })
        );
        // Filtered: exactly the unlisted ids, in selection order.
        let filtered = fixture(ClusterAccess::Reported(vec![]));
        let mut asked = Vec::new();
        let c = classify_with(
            &filtered,
            &ids(&[
                "pa-hidden",
                "pa-classic-live",
                "pa-absent",
                "pa-odd",
                "pa-silent",
                "pa-hidden",
            ]),
            |unlisted| {
                asked = unlisted.to_vec();
                let mut m = BTreeMap::new();
                m.insert(
                    "pa-hidden".to_string(),
                    DescribeAnswer::Answered {
                        error: Some((30, "Broker: Group authorization failed".into())),
                        state: 0,
                        group_type: 0,
                        members: 0,
                    },
                );
                m.insert(
                    "pa-absent".to_string(),
                    DescribeAnswer::Answered {
                        error: None,
                        state: code::STATE_DEAD,
                        group_type: code::TYPE_CLASSIC,
                        members: 0,
                    },
                );
                m.insert(
                    "pa-odd".to_string(),
                    DescribeAnswer::CallFailed("timed out".into()),
                );
                m
            },
        )
        .expect("valid");
        assert_eq!(
            asked,
            ids(&["pa-hidden", "pa-absent", "pa-odd", "pa-silent"])
        );
        assert_eq!(c.targeted, asked);
        let v: BTreeMap<String, GroupVerdict> = c.verdicts.into_iter().collect();
        assert_eq!(v.len(), 5, "one verdict per distinct id");
        assert_eq!(
            v["pa-hidden"],
            GroupVerdict::Failed(GroupFailure::NotVisibleToPrincipal)
        );
        assert_eq!(
            v["pa-absent"],
            GroupVerdict::Excluded(Excluded::GroupNotFound {
                evidence: Absence::TargetedDescribe
            })
        );
        assert!(
            matches!(&v["pa-odd"], GroupVerdict::Failed(GroupFailure::Unreachable { message, .. }) if message == "timed out")
        );
        assert!(matches!(
            &v["pa-silent"],
            GroupVerdict::Failed(GroupFailure::Unreachable { code: None, .. })
        ));
        assert!(matches!(v["pa-classic-live"], GroupVerdict::Capture(_)));
        assert!(classify_with(&filtered, &ids(&[" "]), |_| BTreeMap::new()).is_err());
    }

    /// Mutants killed: a blank id passing; duplicates producing two entries.
    #[test]
    fn selected_ids_collapse_duplicates_and_refuse_blanks() {
        assert_eq!(
            selected_ids(&ids(&["b", "a", "b"])).expect("valid"),
            ids(&["b", "a"])
        );
        assert!(selected_ids(&ids(&["a", " "])).is_err());
    }

    /// T2 on the description. Mutants killed: accepting a description whose
    /// type is not the listing's (the stand-in); reading a 30 as another
    /// failure; accepting an assignment whose topic is not text.
    #[test]
    fn a_description_is_taken_only_when_it_agrees_with_the_listing() {
        let l = fixture(all_ops());
        let v = l.classify(
            &ids(&["pa-consumer-empty", "pa-classic-live"]),
            &BTreeMap::new(),
        );
        let consumer = capture(&v[0].1).clone();
        let classic = capture(&v[1].1).clone();
        let stand_in = DescribedObservation {
            error: None,
            state: code::STATE_DEAD,
            group_type: code::TYPE_CLASSIC,
            is_simple: true,
            partition_assignor: None,
            coordinator: Some(1),
            members: vec![],
            unreadable_topics: 0,
        };
        assert_eq!(
            describe_answer(&consumer, Some(&stand_in)),
            Err(DescribeFailure::TypeDisagrees {
                listed: GroupType::Consumer,
                described: code::TYPE_CLASSIC
            })
        );
        let live = DescribedObservation {
            state: code::STATE_STABLE,
            is_simple: false,
            partition_assignor: Some("range".into()),
            members: vec![MemberDescription {
                client_id: Some("classic-live".into()),
                consumer_id: Some("m-1".into()),
                group_instance_id: None,
                host: Some("/10.0.0.1".into()),
                assignment: vec![TopicPartition::new("pa-orders", 0)],
                target_assignment: None,
            }],
            ..stand_in.clone()
        };
        let d = describe_answer(&classic, Some(&live)).expect("agrees");
        assert_eq!((d.state, d.members.len()), (GroupState::Stable, 1));
        let refused = DescribedObservation {
            error: Some((30, "Broker: Group authorization failed".into())),
            ..live.clone()
        };
        assert_eq!(
            describe_answer(&classic, Some(&refused)),
            Err(DescribeFailure::NotAuthorized)
        );
        let other = DescribedObservation {
            error: Some((15, "coordinator not available".into())),
            ..live.clone()
        };
        assert!(matches!(
            describe_answer(&classic, Some(&other)),
            Err(DescribeFailure::Failed { code: Some(15), .. })
        ));
        let odd = DescribedObservation {
            unreadable_topics: 1,
            ..live.clone()
        };
        assert!(matches!(
            describe_answer(&classic, Some(&odd)),
            Err(DescribeFailure::NotRepresentable(_))
        ));
        assert!(matches!(
            describe_answer(&classic, None),
            Err(DescribeFailure::Failed { code: None, .. })
        ));
    }
}
