//! **The APPROVAL SCOPE: what a second person must be shown before they
//! approve, defined once.**
//!
//! Under a two-person policy the second person is the whole control. They
//! approve what they were shown, so what they are shown must be the WHOLE of
//! what they approve, and it must come from nowhere but the bytes the console
//! signed for the requester:
//!
//! * the request document (the authorization document v2 the console's
//!   `ConsoleConfirmation` key signed), and
//! * the plan those bytes name by hash ([`approval_scope`] recomputes the
//!   hash itself and refuses a plan that is not the request's).
//!
//! Nothing here reads a `Restore` object, a `KafkaCluster`, a status, a label
//! or an annotation: an object field can be edited after the request was
//! signed, and the plan bytes cannot be without changing the hash the
//! signature covers. [`approval_scope`] is a PURE function of those two byte
//! strings — no clock, no environment, no I/O — so two different scopes can
//! never stand behind one request hash.
//!
//! **Complete, or nothing.** The function returns the whole scope or a
//! [`ScopeIncomplete`] that says why not. There is no partial scope: no first
//! N topics, no "and 40 more". The console offers no approval for a request
//! whose scope is incomplete, the approve route refuses one by name when it
//! is called directly, and the create route refuses to make such a request
//! in a two-person namespace at all — so the largest restore that can be
//! requested under this mode is the largest one that can be shown in full
//! ([`MAX_SCOPE_TOPICS`]). A larger restore is split, or the namespace is
//! bound `strict`.
//!
//! **Every value is one a page can show faithfully.** A topic is a name a
//! Kafka broker accepts ([`crate::guard::topic_name_is_kafka_legal`]), before
//! and after its mapping; every other text is printable ASCII of bounded
//! length. A plan carrying anything else — a control character, a
//! right-to-left override, a megabyte of bucket name — has no scope, because
//! what a reviewer would see is not what the runner would read.

use std::collections::BTreeSet;

use chrono::{DateTime, Utc};

use crate::approval_policy::RestoreAuthorization;
use crate::original_name::ApprovalSubject;
use crate::spec::{DrillSpec, TargetMode};

/// The most topics one approval scope shows — and therefore the most a
/// restore may name to be requested in a two-person namespace.
///
/// No lower than any restore the product makes on its own: a schedule names
/// at most 256 topics (`logweir-api`'s `schedules::MAX_TOPICS`), and a
/// catalog point's target is 500. A hand-written plan may name more (the
/// plan document is bounded at 256 KiB, not by a count); such a restore is
/// split into several of at most this many topics, each requested and
/// approved on its own, or approved under a `strict` policy.
pub const MAX_SCOPE_TOPICS: usize = 1024;

/// The most partition numbers one scope lists across all of its subsets
/// (`restore.partitions`). A subset is shown number by number, never as a
/// count.
pub const MAX_SCOPE_PARTITIONS: usize = 8192;

/// The most bootstrap servers a scope lists for the target cluster.
pub const MAX_SCOPE_BOOTSTRAP_SERVERS: usize = 32;

/// The longest any single text value of a scope may be (a bucket, a prefix,
/// an endpoint, a bootstrap server, a backup id). A topic name has Kafka's
/// own bound, [`crate::guard::MAX_TOPIC_NAME_CHARS`].
pub const MAX_SCOPE_TEXT_CHARS: usize = 1024;

/// One of the two people of a request, as the console attested them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopePrincipal {
    /// The identity issuer.
    pub issuer: String,
    /// The subject within it.
    pub subject: String,
}

/// An object-store location a plan names, in the parts a reviewer reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopeStorage {
    /// `s3`, `azure`, `gcs` or `filesystem`.
    pub backend: &'static str,
    /// The location in one line: `s3://bucket/prefix`,
    /// `azure://account/container/prefix`, `gs://bucket/prefix` or
    /// `file://path`.
    pub location: String,
    /// The S3 endpoint the plan states, when it states one.
    pub endpoint: Option<String>,
    /// The S3 region the plan states, when it states one.
    pub region: Option<String>,
    /// Whether the plan allows this location to be dialled over plain HTTP.
    pub plaintext_http: bool,
}

/// The recovery point a plan is bound to by digest (execution contract v2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopePoint {
    /// The catalog point's id.
    pub point_id: String,
    /// The digest of that point's signed receipt.
    pub receipt_sha256: String,
}

/// Where the records come from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopeSource {
    /// The archive.
    pub storage: ScopeStorage,
    /// The backup set: an id, or `latestCompleted`.
    pub backup: String,
    /// The bound recovery point, when the plan names one.
    pub point: Option<ScopePoint>,
}

/// The instant restored to, and the window restored from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopeRecovery {
    /// The recovery point: `restore.point_in_time` when the plan states one,
    /// else `sample.window_end` — the pairing
    /// [`crate::spec::target_topic_prefix`] and the runner make.
    pub point_in_time: DateTime<Utc>,
    /// Whether the plan states `restore.point_in_time` itself.
    pub point_in_time_stated: bool,
    /// The inclusive start of the replayed window, when the plan narrows it.
    /// Absent restores from the archive's floor.
    pub window_start: Option<DateTime<Utc>>,
    /// `producerTime` when the plan accepts producer time for its selection.
    pub time_basis: Option<&'static str>,
}

/// The cluster written into.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopeTarget {
    /// The bootstrap servers the runner dials: the plan's own.
    pub bootstrap_servers: Vec<String>,
    /// How it authenticates there: `plaintext`, `scramSha512`, …
    pub auth_mode: &'static str,
    /// The SASL principal the plan authenticates as, when it names one.
    pub auth_username: Option<String>,
    /// `target.default_replication_factor`: the factor of every topic the run
    /// creates, except with `topic_configuration`, where a topic the point
    /// records a factor for is created at that factor, bounded by the
    /// target's brokers, and only one without gets this.
    pub replication_factor: i16,
    /// `target.teardown`: what phase 9 does with a SCRATCH run's targets
    /// (`delete` removes the topics the run created, never an original name;
    /// a `newTopic` restore deletes nothing whatever it says).
    pub teardown: String,
    /// PROD-11.1a: a TTL clone's lifetime in seconds (`target.teardown:
    /// {ttl_seconds}`), after which its topics are deleted; absent for every
    /// other plan.
    pub teardown_after_seconds: Option<u64>,
    /// PROD-11.1a: what restored records carry of their source, always
    /// [`crate::target_creation::LINEAGE_HEADERS_KEPT`]: every record keeps
    /// `x-original-offset` and `x-original-timestamp`.
    pub lineage_headers: &'static str,
    /// `scratch` or `newTopic`.
    pub mode: &'static str,
    /// The prefix every source topic is mapped through
    /// ([`crate::spec::target_topic_prefix`]); empty for a restore under the
    /// original topic names.
    pub topic_prefix: String,
    /// **For a restore under the original topic names only**:
    /// the requester's statement about declarative owners
    /// (`target.topic_naming.original_name`), which alone decides whether
    /// such a run writes — the console gives the runner nothing else to look
    /// in. Absent for every other restore.
    pub owner_statement: Option<ScopeOwnerStatement>,
    /// `restore.allow_transaction_markers`: the run restores an
    /// archive's transaction markers, and the records of aborted
    /// transactions, as ordinary data instead of being refused.
    pub allow_transaction_markers: bool,
    /// **PROD-05.2.** `target.topic_configuration`: the topics are created
    /// from the bound point's configuration model and, with
    /// `afterVerification`, the source's compaction, retention and timestamp
    /// settings are applied after a passing verdict. Absent: created as
    /// always.
    pub topic_configuration: Option<ScopeTopicConfiguration>,
}

/// The plan's configuration choice, as a second person approves it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopeTopicConfiguration {
    /// `afterVerification` or `none`.
    pub transition: &'static str,
    /// The requester's owner statement, keyed by SOURCE topic: `None` when
    /// the plan states none (the point's own detection decides).
    pub owners: Option<Vec<ScopeOwner>>,
}

/// One declarative owner the requester states (`{topic, kind, reference}`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopeOwner {
    /// The restored name it manages.
    pub topic: String,
    /// `strimzi` or `external`.
    pub kind: String,
    /// Where it is defined (a `KafkaTopic`, a repository path, …).
    pub reference: String,
}

/// The requester's owner statement for an original-name restore, as signed
/// in the plan: three cases for `owners` — NOT DECLARED (`None`: the run has
/// nowhere to look from the console and refuses), DECLARED EMPTY (`Some([])`:
/// the requester states no owner manages any name), or each owner — and
/// whether the owner path is chosen (restore although an owner manages a
/// name).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopeOwnerStatement {
    /// The owners stated, or `None` when the plan states nothing.
    pub owners: Option<Vec<ScopeOwner>>,
    /// Whether the plan chooses the owner path.
    pub owner_path: bool,
}

/// One source topic, and the name it is restored under.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopeTopic {
    /// The topic in the archive.
    pub source: String,
    /// The topic written on the target.
    pub target: String,
    /// Whether this is a write under the source's ORIGINAL name:
    /// the plan opts in and the two names are the same.
    pub original_name: bool,
    /// The partitions restored, when the plan narrows this topic to a subset;
    /// absent is every partition.
    pub partitions: Option<Vec<i32>>,
}

/// How the restore is checked afterwards.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopeVerification {
    /// `sampled` or `complete`.
    pub coverage: &'static str,
    /// The window the check reads.
    pub window_start: DateTime<Utc>,
    /// Its end.
    pub window_end: DateTime<Utc>,
}

/// **Everything a second person approves**, from the request and the plan it
/// hashes. See the module documentation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApprovalScope {
    /// Who asked.
    pub requester: ScopePrincipal,
    /// The namespace of the Restore.
    pub namespace: String,
    /// The Restore.
    pub restore: String,
    /// Its UID.
    pub restore_uid: String,
    /// The plan's hash, which the request names.
    pub plan_hash: String,
    /// What is approved: an ordinary restore, or one under the original
    /// topic names.
    pub approval_subject: ApprovalSubject,
    /// The policy the request was made under.
    pub policy_name: String,
    /// That policy's snapshot digest.
    pub policy_digest: String,
    /// The change ticket.
    pub ticket: Option<String>,
    /// When the console signed the request.
    pub requested_at: DateTime<Utc>,
    /// When it stops authorising anything.
    pub expires_at: DateTime<Utc>,
    /// The plan's own name, when it states one.
    pub plan_name: Option<String>,
    /// Where the records come from.
    pub source: ScopeSource,
    /// The instant restored to and the window restored from.
    pub recovery: ScopeRecovery,
    /// The cluster written into.
    pub target: ScopeTarget,
    /// Every source topic and the name it is restored under, in the plan's
    /// order. Never a slice.
    pub topics: Vec<ScopeTopic>,
    /// How the restore is checked.
    pub verification: ScopeVerification,
    /// Where the evidence is written.
    pub evidence: ScopeStorage,
}

/// The part of the scope a PLAN decides on its own: what the create route can
/// judge before any request exists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanScope {
    /// The plan's own name.
    pub plan_name: Option<String>,
    /// Where the records come from.
    pub source: ScopeSource,
    /// The instant restored to and the window restored from.
    pub recovery: ScopeRecovery,
    /// The cluster written into.
    pub target: ScopeTarget,
    /// Every topic, in the plan's order.
    pub topics: Vec<ScopeTopic>,
    /// How the restore is checked.
    pub verification: ScopeVerification,
    /// Where the evidence is written.
    pub evidence: ScopeStorage,
    /// The approval subject this plan needs.
    pub approval_subject: ApprovalSubject,
}

/// **Why a request has no scope a second person can be shown in full** — a
/// closed set, each with a stable word ([`Self::code`]) and one sentence
/// ([`Self::sentence`]) that says why and what to do. No sentence carries a
/// value from the plan or the request: they are text somebody else chose.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ScopeIncomplete {
    /// The plan bytes do not hash to the request's `planHash`.
    PlanNotTheRequests,
    /// The plan bytes are not a restore plan this build reads.
    PlanUnreadable,
    /// The request's approval subject is not the one the plan needs.
    SubjectNotThePlans,
    /// The plan names no topic.
    NoTopics,
    /// The plan names more topics than a scope shows.
    TooManyTopics {
        /// How many it names.
        count: usize,
    },
    /// The plan names a topic twice.
    TopicRepeated,
    /// A source topic, or the name it is restored under, is not a name a
    /// Kafka broker accepts.
    TopicNotAName,
    /// The plan's partition subsets list more numbers than a scope shows.
    TooManyPartitions {
        /// How many they list.
        count: usize,
    },
    /// A partition subset is empty, repeats a number, lists a negative one,
    /// or belongs to a topic the plan does not restore.
    PartitionsNotShowable,
    /// The plan names no bootstrap server, or more than a scope lists.
    BootstrapServersNotShowable,
    /// The plan states more declarative owners than it restores topics.
    OwnersNotShowable,
    /// The plan names a notification sink (a webhook, Slack or PagerDuty):
    /// the run would send its summary there, and the scope does not show
    /// where.
    NotificationsNotShown,
    /// A text value is longer than a scope shows, or is not printable ASCII.
    ValueNotShowable {
        /// Which value, in fixed words.
        what: &'static str,
    },
    /// PROD-04.2: the switchover plan names more consumer groups than a
    /// scope shows ([`crate::switchover::MAX_GROUPS`]).
    TooManyGroups {
        /// How many it names.
        count: usize,
    },
    /// PROD-04.2: the switchover plan commits more positions than a scope
    /// shows ([`crate::switchover::MAX_POSITIONS`]).
    TooManyPositions {
        /// How many it commits.
        count: usize,
    },
}

/// What a scope is the scope of: the sentences name it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScopeOf {
    /// A `Restore` ([`approval_scope`]).
    Restore,
    /// A `Switchover` ([`switchover_scope`]).
    Switchover,
}

impl ScopeIncomplete {
    /// The stable word for this reason.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::PlanNotTheRequests => "planNotTheRequests",
            Self::PlanUnreadable => "planUnreadable",
            Self::SubjectNotThePlans => "subjectNotThePlans",
            Self::NoTopics => "noTopics",
            Self::TooManyTopics { .. } => "tooManyTopics",
            Self::TopicRepeated => "topicRepeated",
            Self::TopicNotAName => "topicNotAName",
            Self::TooManyPartitions { .. } => "tooManyPartitions",
            Self::PartitionsNotShowable => "partitionsNotShowable",
            Self::BootstrapServersNotShowable => "bootstrapServersNotShowable",
            Self::OwnersNotShowable => "ownersNotShowable",
            Self::NotificationsNotShown => "notificationsNotShown",
            Self::ValueNotShowable { .. } => "valueNotShowable",
            Self::TooManyGroups { .. } => "tooManyGroups",
            Self::TooManyPositions { .. } => "tooManyPositions",
        }
    }

    /// [`Self::sentence`], naming the kind the scope is of. A `Restore`'s is
    /// [`Self::sentence`] itself, word for word.
    #[must_use]
    pub fn sentence_for(self, of: ScopeOf) -> String {
        const WAY_OUT: &str = "A second person approves only what they are shown in full";
        if of == ScopeOf::Restore {
            return self.sentence();
        }
        match self {
            Self::PlanNotTheRequests => format!(
                "{WAY_OUT}, and the plan stored on this Switchover is not the plan the request \
                 names by hash. Submit the Switchover again"
            ),
            Self::PlanUnreadable => format!(
                "{WAY_OUT}, and this Switchover's plan is not a switchover plan this console can \
                 read (format 1, 1 to {} groups, 1 to {} positions), so its groups and positions \
                 cannot be shown. Write it again with `logweir switchover plan`",
                crate::switchover::MAX_GROUPS,
                crate::switchover::MAX_POSITIONS
            ),
            Self::SubjectNotThePlans => format!(
                "{WAY_OUT}, and the request names an approval subject or a typed confirmation, \
                 which a switchover never carries. Submit the Switchover again"
            ),
            Self::TooManyGroups { count } => format!(
                "{WAY_OUT}, and this switchover names {count} consumer groups, more than the {} a \
                 request shows. Split it into switchovers of at most {} groups each, requested \
                 and approved one by one, or approve it under a strict (personal-key) policy",
                crate::switchover::MAX_GROUPS,
                crate::switchover::MAX_GROUPS
            ),
            Self::TooManyPositions { count } => format!(
                "{WAY_OUT}, and this switchover commits {count} positions, more than the {} a \
                 request shows. Split it into switchovers over fewer groups, or approve it under \
                 a strict (personal-key) policy",
                crate::switchover::MAX_POSITIONS
            ),
            Self::TopicNotAName => format!(
                "{WAY_OUT}, and a topic in this Switchover's plan is not a name a Kafka broker \
                 accepts (letters, digits, `.`, `_` and `-`, at most {} characters), so it cannot \
                 be shown as it would be committed. Write the plan again with `logweir switchover \
                 plan`",
                crate::guard::MAX_TOPIC_NAME_CHARS
            ),
            Self::ValueNotShowable { what } => format!(
                "{WAY_OUT}, and {what} in this {} is longer than {MAX_SCOPE_TEXT_CHARS} \
                 characters or is not printable ASCII, so what a reviewer would see is not what \
                 was signed. Correct it, or approve the switchover under a strict (personal-key) \
                 policy",
                if REQUEST_VALUES.contains(&what) {
                    "request"
                } else {
                    "Switchover's plan"
                }
            ),
            other => other.sentence(),
        }
    }

    /// Why, and what to do. Fixed words and numbers only.
    #[must_use]
    pub fn sentence(self) -> String {
        const WAY_OUT: &str = "A second person approves only what they are shown in full";
        match self {
            Self::PlanNotTheRequests => format!(
                "{WAY_OUT}, and the plan stored on this Restore is not the plan the request \
                 names by hash. Submit the Restore again"
            ),
            Self::PlanUnreadable => format!(
                "{WAY_OUT}, and this Restore's plan is not a restore plan this console can \
                 read, so its source, its target and its topics cannot be shown. Submit the \
                 Restore again from the wizard"
            ),
            Self::SubjectNotThePlans => format!(
                "{WAY_OUT}, and the request's approval subject is not the one its plan needs \
                 (a restore under the original topic names is requested as exactly that). \
                 Submit the Restore again"
            ),
            Self::NoTopics => format!(
                "{WAY_OUT}, and this Restore's plan names no topic, so there is nothing to show"
            ),
            Self::TooManyTopics { count } => format!(
                "{WAY_OUT}, and this restore names {count} topics, more than the \
                 {MAX_SCOPE_TOPICS} a request shows. Split it into restores of at most \
                 {MAX_SCOPE_TOPICS} topics each, requested and approved one by one, or approve \
                 it under a strict (personal-key) policy"
            ),
            Self::TopicRepeated => format!(
                "{WAY_OUT}, and this Restore's plan names a topic twice, so the list of what \
                 is restored is not one a reviewer can rely on. Name each topic once"
            ),
            Self::TopicNotAName => format!(
                "{WAY_OUT}, and a topic in this Restore's plan, or the name it would be \
                 restored under, is not a name a Kafka broker accepts (letters, digits, `.`, \
                 `_` and `-`, at most {} characters), so it cannot be shown as it would be \
                 written. Correct the topic names or the prefix",
                crate::guard::MAX_TOPIC_NAME_CHARS
            ),
            Self::TooManyPartitions { count } => format!(
                "{WAY_OUT}, and this restore's partition subsets list {count} partitions, more \
                 than the {MAX_SCOPE_PARTITIONS} a request shows. Split it into restores with \
                 smaller subsets, or approve it under a strict (personal-key) policy"
            ),
            Self::PartitionsNotShowable => format!(
                "{WAY_OUT}, and a partition subset in this Restore's plan is empty, repeats a \
                 partition, lists a negative one or belongs to a topic the plan does not \
                 restore. Correct restore.partitions"
            ),
            Self::BootstrapServersNotShowable => format!(
                "{WAY_OUT}, and this Restore's plan names no target bootstrap server, or more \
                 than the {MAX_SCOPE_BOOTSTRAP_SERVERS} a request shows, so the cluster it \
                 writes into cannot be shown"
            ),
            Self::OwnersNotShowable => format!(
                "{WAY_OUT}, and this Restore's plan states more declarative owners than it \
                 restores topics, so its owner statement cannot be shown as a statement about \
                 these names. Correct target.topic_naming.original_name.owners"
            ),
            Self::NotificationsNotShown => format!(
                "{WAY_OUT}, and this Restore's plan names a notification sink (a webhook, Slack \
                 or PagerDuty) the run would send its result to, which a request does not show. \
                 Submit it from the wizard, which names none, or approve it under a strict \
                 (personal-key) policy"
            ),
            Self::ValueNotShowable { what } => format!(
                "{WAY_OUT}, and {what} in this {} is longer than {MAX_SCOPE_TEXT_CHARS} \
                 characters or is not printable ASCII, so what a reviewer would see is not what \
                 was signed. Correct it, or approve the restore under a strict (personal-key) \
                 policy",
                if REQUEST_VALUES.contains(&what) {
                    "request"
                } else {
                    "Restore's plan"
                }
            ),
            // A restore plan never yields these; their words are the
            // switchover's.
            Self::TooManyGroups { .. } | Self::TooManyPositions { .. } => {
                self.sentence_for(ScopeOf::Switchover)
            }
        }
    }
}

impl std::fmt::Display for ScopeIncomplete {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.sentence())
    }
}

/// The values of a scope that come from the REQUEST, not from its plan.
const REQUEST_VALUES: [&str; 10] = [
    "the requester's issuer",
    "the requester's subject",
    "the namespace",
    "the Restore's name",
    "the Restore's UID",
    "the Switchover's name",
    "the Switchover's UID",
    "the policy's name",
    "the policy's digest",
    "the change ticket",
];

/// Whether `text` is printable ASCII (space to `~`) of at most
/// [`MAX_SCOPE_TEXT_CHARS`] characters: no control character, no
/// right-to-left override, no look-alike outside ASCII. The create holds a
/// two-person request's ticket to it, so no request is made
/// that its scope would refuse.
#[must_use]
pub fn showable(text: &str) -> bool {
    text.len() <= MAX_SCOPE_TEXT_CHARS && text.bytes().all(|b| (0x20..=0x7e).contains(&b))
}

fn shown(text: &str, what: &'static str) -> Result<String, ScopeIncomplete> {
    if showable(text) {
        Ok(text.to_string())
    } else {
        Err(ScopeIncomplete::ValueNotShowable { what })
    }
}

fn shown_opt(text: Option<&str>, what: &'static str) -> Result<Option<String>, ScopeIncomplete> {
    text.map(|t| shown(t, what)).transpose()
}

fn storage(
    url: &crate::engine::StorageUrl,
    what: &'static str,
) -> Result<ScopeStorage, ScopeIncomplete> {
    use crate::engine::StorageUrl;
    let join = |scheme: &str, parts: &[&str]| -> String {
        let mut out = format!("{scheme}://");
        out.push_str(
            &parts
                .iter()
                .filter(|p| !p.is_empty())
                .copied()
                .collect::<Vec<_>>()
                .join("/"),
        );
        out
    };
    let (backend, location, endpoint, region, plaintext_http) = match url {
        StorageUrl::S3 {
            bucket,
            prefix,
            region,
            endpoint,
            allow_http,
            ..
        } => (
            "s3",
            join("s3", &[bucket, prefix]),
            endpoint.as_deref(),
            region.as_deref(),
            *allow_http,
        ),
        StorageUrl::Azure {
            account_name,
            container_name,
            prefix,
        } => (
            "azure",
            join("azure", &[account_name, container_name, prefix]),
            None,
            None,
            false,
        ),
        StorageUrl::Gcs { bucket, prefix } => {
            ("gcs", join("gs", &[bucket, prefix]), None, None, false)
        }
        StorageUrl::Filesystem { path } => {
            let text = path
                .to_str()
                .ok_or(ScopeIncomplete::ValueNotShowable { what })?;
            ("filesystem", format!("file://{text}"), None, None, false)
        }
    };
    Ok(ScopeStorage {
        backend,
        location: shown(&location, what)?,
        endpoint: shown_opt(endpoint, what)?,
        region: shown_opt(region, what)?,
        plaintext_http,
    })
}

/// The owner statement of an original-name plan, each value by the showable
/// rule and the list bounded by the topic count.
fn owner_statement(
    statement: &crate::spec::OriginalNameSpec,
    topics: usize,
) -> Result<ScopeOwnerStatement, ScopeIncomplete> {
    let owners = match &statement.owners {
        None => None,
        Some(owners) if owners.len() > topics => return Err(ScopeIncomplete::OwnersNotShowable),
        Some(owners) => Some(
            owners
                .iter()
                .map(|o| {
                    Ok(ScopeOwner {
                        topic: shown(&o.topic, "a declared owner")?,
                        kind: shown(&o.kind, "a declared owner")?,
                        reference: shown(&o.reference, "a declared owner")?,
                    })
                })
                .collect::<Result<Vec<_>, ScopeIncomplete>>()?,
        ),
    };
    Ok(ScopeOwnerStatement {
        owners,
        owner_path: statement.owner_path,
    })
}

/// The plan's `target.topic_configuration` as a scope shows it: every owner
/// it states, each value showable, no more owners than topics.
fn topic_configuration_scope(
    block: &crate::topic_transition::TopicConfigurationPlan,
    topics: usize,
) -> Result<ScopeTopicConfiguration, ScopeIncomplete> {
    let owners = match &block.owners {
        None => None,
        Some(owners) if owners.len() > topics => return Err(ScopeIncomplete::OwnersNotShowable),
        Some(owners) => Some(
            owners
                .iter()
                .map(|o| {
                    Ok(ScopeOwner {
                        topic: shown(&o.topic, "a declared owner")?,
                        kind: shown(&o.kind, "a declared owner")?,
                        reference: shown(&o.reference, "a declared owner")?,
                    })
                })
                .collect::<Result<Vec<_>, ScopeIncomplete>>()?,
        ),
    };
    Ok(ScopeTopicConfiguration {
        transition: block.transition.wire_name(),
        owners,
    })
}

/// **The part of the scope a plan decides**, from the plan bytes alone — or
/// why it cannot be shown in full.
///
/// The mapped names come from [`crate::spec::target_topic_prefix`], the one
/// place that rule lives, so what a reviewer is shown is what phase 0 maps.
///
/// # Errors
///
/// [`ScopeIncomplete`]: every reason but the two that need the request.
pub fn plan_scope(plan_bytes: &[u8]) -> Result<PlanScope, ScopeIncomplete> {
    let text = std::str::from_utf8(plan_bytes).map_err(|_| ScopeIncomplete::PlanUnreadable)?;
    let plan: DrillSpec =
        serde_yaml::from_str(text).map_err(|_| ScopeIncomplete::PlanUnreadable)?;

    // The topics, every one.
    let count = plan.source.topics.len();
    if count == 0 {
        return Err(ScopeIncomplete::NoTopics);
    }
    if count > MAX_SCOPE_TOPICS {
        return Err(ScopeIncomplete::TooManyTopics { count });
    }
    let prefix = crate::spec::target_topic_prefix(&plan);
    let original = plan.target.original_name().is_some();
    let mut seen = BTreeSet::new();
    let mut listed_partitions = 0usize;
    let mut topics = Vec::with_capacity(count);
    for source in &plan.source.topics {
        let target = format!("{prefix}{source}");
        if !crate::guard::topic_name_is_kafka_legal(source)
            || !crate::guard::topic_name_is_kafka_legal(&target)
        {
            return Err(ScopeIncomplete::TopicNotAName);
        }
        if !seen.insert(source.as_str()) {
            return Err(ScopeIncomplete::TopicRepeated);
        }
        let partitions = match plan.restore.partitions.get(source) {
            None => None,
            Some(subset) => {
                let distinct: BTreeSet<i32> = subset.iter().copied().collect();
                if subset.is_empty()
                    || distinct.len() != subset.len()
                    || subset.iter().any(|p| *p < 0)
                {
                    return Err(ScopeIncomplete::PartitionsNotShowable);
                }
                listed_partitions += subset.len();
                Some(subset.clone())
            }
        };
        topics.push(ScopeTopic {
            original_name: original && target == *source,
            source: source.clone(),
            target,
            partitions,
        });
    }
    if plan
        .restore
        .partitions
        .keys()
        .any(|topic| !seen.contains(topic.as_str()))
    {
        return Err(ScopeIncomplete::PartitionsNotShowable);
    }
    if listed_partitions > MAX_SCOPE_PARTITIONS {
        return Err(ScopeIncomplete::TooManyPartitions {
            count: listed_partitions,
        });
    }

    // Where the result goes. The scope does not show notification sinks, so a
    // plan that names one is not approvable in the console; the wizard's plans
    // name none (`webhooks: []`).
    let sinks = &plan.notifications;
    if !sinks.webhooks.is_empty()
        || sinks.slack_webhook.is_some()
        || sinks.pagerduty_routing_key.is_some()
        || sinks.pagerduty_endpoint.is_some()
    {
        return Err(ScopeIncomplete::NotificationsNotShown);
    }

    // The target cluster.
    let servers = &plan.target.bootstrap_servers;
    if servers.is_empty()
        || servers.len() > MAX_SCOPE_BOOTSTRAP_SERVERS
        || servers.iter().any(|s| s.trim().is_empty())
    {
        return Err(ScopeIncomplete::BootstrapServersNotShowable);
    }
    let bootstrap_servers = servers
        .iter()
        .map(|s| shown(s, "a target bootstrap server"))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(PlanScope {
        plan_name: shown_opt(plan.name.as_deref(), "the plan's name")?,
        source: ScopeSource {
            storage: storage(&plan.source.storage, "the source archive's location")?,
            backup: shown(&plan.source.backup, "the backup set")?,
            point: plan
                .source
                .point
                .as_ref()
                .map(|point| {
                    Ok(ScopePoint {
                        point_id: shown(&point.point_id, "the recovery point's id")?,
                        receipt_sha256: shown(
                            &point.receipt_sha256,
                            "the recovery point's receipt digest",
                        )?,
                    })
                })
                .transpose()?,
        },
        recovery: ScopeRecovery {
            point_in_time: plan.restore.point_in_time.unwrap_or(plan.sample.window_end),
            point_in_time_stated: plan.restore.point_in_time.is_some(),
            window_start: plan.restore.window_start,
            time_basis: plan.restore.time_basis.map(crate::spec::TimeBasis::as_str),
        },
        target: ScopeTarget {
            bootstrap_servers,
            auth_mode: plan.target.auth.mode_str(),
            auth_username: shown_opt(plan.target.auth.username(), "the target's SASL principal")?,
            replication_factor: plan.target.default_replication_factor,
            teardown: shown(plan.target.teardown.policy(), "the target's teardown")?,
            teardown_after_seconds: plan.target.teardown.ttl_seconds(),
            lineage_headers: crate::target_creation::LINEAGE_HEADERS_KEPT,
            owner_statement: plan
                .target
                .original_name()
                .map(|statement| owner_statement(statement, count))
                .transpose()?,
            mode: match plan.target.mode {
                TargetMode::Scratch => "scratch",
                TargetMode::NewTopic => "newTopic",
            },
            topic_prefix: prefix,
            allow_transaction_markers: plan.restore.allow_transaction_markers,
            topic_configuration: plan
                .target
                .topic_configuration
                .as_ref()
                .map(|block| topic_configuration_scope(block, count))
                .transpose()?,
        },
        topics,
        verification: ScopeVerification {
            coverage: plan.sample.coverage.as_str(),
            window_start: plan.sample.window_start,
            window_end: plan.sample.window_end,
        },
        evidence: storage(&plan.evidence, "the evidence location")?,
        approval_subject: ApprovalSubject::of_plan(&plan),
    })
}

/// **The approval scope of one request**: everything a second person
/// approves, from the request document the console signed and the plan it
/// names by hash — or why it cannot be shown in full.
///
/// `plan_bytes` are held to the request FIRST: their SHA-256 must be the
/// request's `planHash`. So every plan-derived fact below stands behind the
/// console's signature, and a caller cannot be handed a scope for bytes the
/// signature does not cover. Pure: no clock, no I/O.
///
/// The caller verified the console's signature over `request`'s bytes and
/// held them to the Restore and the policy
/// ([`crate::approval_policy::check_request_binding`]); this function does
/// not, and reads nothing else.
pub fn approval_scope(
    request: &RestoreAuthorization,
    plan_bytes: &[u8],
) -> Result<ApprovalScope, ScopeIncomplete> {
    if crate::ids::sha256_prefixed(plan_bytes) != request.plan_hash {
        return Err(ScopeIncomplete::PlanNotTheRequests);
    }
    let plan = plan_scope(plan_bytes)?;
    let subject = ApprovalSubject::from_wire(request.approval_subject.as_deref())
        .map_err(|_| ScopeIncomplete::SubjectNotThePlans)?;
    if subject != plan.approval_subject {
        return Err(ScopeIncomplete::SubjectNotThePlans);
    }
    // THE REQUEST'S OWN TEXT, BY THE SAME RULE. Who asked is the fact the
    // second person decides on ("I am not the requester"): a right-to-left
    // override or a control character in it could make them misread it. The
    // console's identity rule already keeps such a requester from being
    // compared at all; this keeps it from being SHOWN, whatever made the
    // request.
    Ok(ApprovalScope {
        requester: ScopePrincipal {
            issuer: shown(&request.requester.issuer, "the requester's issuer")?,
            subject: shown(&request.requester.subject, "the requester's subject")?,
        },
        namespace: shown(&request.subject.namespace, "the namespace")?,
        restore: shown(&request.subject.name, "the Restore's name")?,
        restore_uid: shown(&request.subject.uid, "the Restore's UID")?,
        plan_hash: request.plan_hash.clone(),
        approval_subject: subject,
        policy_name: shown(&request.policy.name, "the policy's name")?,
        policy_digest: shown(&request.policy.digest, "the policy's digest")?,
        ticket: shown_opt(request.ticket.as_deref(), "the change ticket")?,
        requested_at: request.issued_at,
        expires_at: request.expires_at,
        plan_name: plan.plan_name,
        source: plan.source,
        recovery: plan.recovery,
        target: plan.target,
        topics: plan.topics,
        verification: plan.verification,
        evidence: plan.evidence,
    })
}

// ---------------------------------------------------------------------------
// PROD-04.2: the scope of a SWITCHOVER request.
// ---------------------------------------------------------------------------

/// The verified restore a switchover plan applies, as its plan binds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopeSwitchoverRestore {
    /// The `Restore`'s name.
    pub name: String,
    /// Its UID.
    pub uid: String,
    /// The restore plan's hash: the target cluster and its credentials.
    pub plan_hash: String,
    /// The signed scorecard's digest.
    pub scorecard_sha256: String,
    /// The positions-mapping document's digest.
    pub mapping_sha256: String,
}

/// One position a switchover commits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopePosition {
    /// The restored partition.
    pub partition: u32,
    /// The target offset committed.
    pub offset: i64,
    /// Whether the mapping class is `approximate` (else `exact`).
    pub approximate: bool,
    /// The committed position the group must hold first; `None`: none.
    pub expected: Option<i64>,
}

/// One restored topic of a scope group, every partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopeGroupTopic {
    /// The restored topic.
    pub topic: String,
    /// Every planned partition, in order.
    pub positions: Vec<ScopePosition>,
}

/// One consumer group a switchover commits for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopeGroup {
    /// The captured (source) group.
    pub source: String,
    /// The target group committed for.
    pub target: String,
    /// Every topic, in order.
    pub topics: Vec<ScopeGroupTopic>,
}

/// The part of a switchover's scope its PLAN decides.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SwitchoverPlanScope {
    /// The restore it applies.
    pub restore: ScopeSwitchoverRestore,
    /// A rollback plan's record `(key, sha256)`.
    pub rollback_of: Option<(String, String)>,
    /// Every group, in the plan's order. Never a slice.
    pub groups: Vec<ScopeGroup>,
    /// How many positions `groups` lists.
    pub positions: usize,
}

/// **Everything a second person approves for a switchover**, from the
/// request and the plan it hashes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SwitchoverScope {
    /// Who asked.
    pub requester: ScopePrincipal,
    /// The namespace of the Switchover.
    pub namespace: String,
    /// The Switchover.
    pub switchover: String,
    /// Its UID.
    pub switchover_uid: String,
    /// The plan's hash, which the request names.
    pub plan_hash: String,
    /// The policy the request was made under.
    pub policy_name: String,
    /// That policy's snapshot digest.
    pub policy_digest: String,
    /// The change ticket.
    pub ticket: Option<String>,
    /// When the console signed the request.
    pub requested_at: DateTime<Utc>,
    /// When it stops authorising anything.
    pub expires_at: DateTime<Utc>,
    /// The plan's part.
    pub plan: SwitchoverPlanScope,
}

/// **The part of a switchover's scope its plan decides**, from the plan
/// bytes alone — or why it cannot be shown in full. The counts come BEFORE
/// the plan's own shape check, so a plan too large to show is refused for
/// that, by name (`tooManyGroups`, `tooManyPositions`), and not as unreadable.
///
/// # Errors
///
/// [`ScopeIncomplete`]: every reason but the two that need the request.
pub fn switchover_plan_scope(plan_bytes: &[u8]) -> Result<SwitchoverPlanScope, ScopeIncomplete> {
    use crate::switchover::{validate, Expected, SwitchoverPlan, MAX_GROUPS, MAX_PLAN_BYTES};
    if plan_bytes.len() > MAX_PLAN_BYTES {
        return Err(ScopeIncomplete::PlanUnreadable);
    }
    let plan: SwitchoverPlan =
        serde_yaml::from_slice(plan_bytes).map_err(|_| ScopeIncomplete::PlanUnreadable)?;
    if plan.groups.len() > MAX_GROUPS {
        return Err(ScopeIncomplete::TooManyGroups {
            count: plan.groups.len(),
        });
    }
    let count = plan.positions();
    if count > crate::switchover::MAX_POSITIONS {
        return Err(ScopeIncomplete::TooManyPositions { count });
    }
    validate(&plan).map_err(|_| ScopeIncomplete::PlanUnreadable)?;
    let restore = ScopeSwitchoverRestore {
        name: shown(&plan.restore.name, "the restore's name")?,
        uid: shown(&plan.restore.uid, "the restore's UID")?,
        plan_hash: shown(&plan.restore.plan_hash, "a digest")?,
        scorecard_sha256: shown(&plan.restore.scorecard_sha256, "a digest")?,
        mapping_sha256: shown(&plan.restore.mapping_sha256, "a digest")?,
    };
    let rollback_of = match &plan.rollback_of {
        None => None,
        Some(record) => Some((
            shown(&record.key, "the rollback record's key")?,
            shown(&record.sha256, "a digest")?,
        )),
    };
    let mut groups = Vec::with_capacity(plan.groups.len());
    let mut listed = 0usize;
    for group in &plan.groups {
        let mut topics = Vec::with_capacity(group.topics.len());
        for (topic, planned) in &group.topics {
            if !crate::guard::topic_name_is_kafka_legal(topic) {
                return Err(ScopeIncomplete::TopicNotAName);
            }
            let positions: Vec<ScopePosition> = planned
                .offsets
                .iter()
                .map(|(partition, offset)| ScopePosition {
                    partition: *partition,
                    offset: *offset,
                    approximate: planned.approximate.contains(partition),
                    expected: match &planned.expected {
                        Expected::Word(_) => None,
                        Expected::Positions(_) => planned.expected.at(*partition),
                    },
                })
                .collect();
            listed += positions.len();
            topics.push(ScopeGroupTopic {
                topic: topic.clone(),
                positions,
            });
        }
        groups.push(ScopeGroup {
            source: shown(&group.source, "a consumer group id")?,
            target: shown(&group.target, "a consumer group id")?,
            topics,
        });
    }
    Ok(SwitchoverPlanScope {
        restore,
        rollback_of,
        groups,
        positions: listed,
    })
}

/// **The approval scope of one switchover request** ([`approval_scope`]'s
/// rule for subject kind `Switchover`): the request the console signed and
/// the plan it names by hash, held to that hash FIRST. Pure.
///
/// # Errors
///
/// Why it cannot be shown in full.
pub fn switchover_scope(
    request: &RestoreAuthorization,
    plan_bytes: &[u8],
) -> Result<SwitchoverScope, ScopeIncomplete> {
    if crate::ids::sha256_prefixed(plan_bytes) != request.plan_hash {
        return Err(ScopeIncomplete::PlanNotTheRequests);
    }
    if request.subject.kind != crate::spec::SUBJECT_KIND_SWITCHOVER
        || request.approval_subject.is_some()
        || request.original_name_confirmation.is_some()
    {
        return Err(ScopeIncomplete::SubjectNotThePlans);
    }
    let plan = switchover_plan_scope(plan_bytes)?;
    Ok(SwitchoverScope {
        requester: ScopePrincipal {
            issuer: shown(&request.requester.issuer, "the requester's issuer")?,
            subject: shown(&request.requester.subject, "the requester's subject")?,
        },
        namespace: shown(&request.subject.namespace, "the namespace")?,
        switchover: shown(&request.subject.name, "the Switchover's name")?,
        switchover_uid: shown(&request.subject.uid, "the Switchover's UID")?,
        plan_hash: request.plan_hash.clone(),
        policy_name: shown(&request.policy.name, "the policy's name")?,
        policy_digest: shown(&request.policy.digest, "the policy's digest")?,
        ticket: shown_opt(request.ticket.as_deref(), "the change ticket")?,
        requested_at: request.issued_at,
        expires_at: request.expires_at,
        plan,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::approval_policy::{
        ApprovalMode, AuthorizedSubject, PolicyRef, Requester, RESTORE_AUTHORIZATION_KIND,
    };

    fn at(text: &str) -> DateTime<Utc> {
        text.parse().expect("an instant")
    }

    /// A plan over `topics`, with `restore` as the lines of its `restore`
    /// block and `naming` as its `target.topic_naming`.
    fn plan_with(topics: &[String], naming: &str, restore: &str, bucket: &str) -> String {
        let list: String = topics.iter().map(|t| format!("    - \"{t}\"\n")).collect();
        format!(
            "name: \"orders-pitr\"\nsource:\n  storage:\n    backend: \"s3\"\n    bucket: \
             \"{bucket}\"\n    prefix: \"drill-demo\"\n    region: \"us-east-1\"\n    endpoint: \
             \"http://minio:9000\"\n    path_style: true\n    allow_http: true\n  backup: \
             \"01JB7Z0000000000000000000B\"\n  topics:\n{list}target:\n  bootstrap_servers:\n    \
             - \"kafka-0.target:9092\"\n    - \"kafka-1.target:9092\"\n  mode: \"newTopic\"\n\
             {naming}  topic_mapping_prefix: \"\"\nrestore:\n{restore}sample:\n  window_start: \
             \"2026-09-07T12:00:00Z\"\n  window_end: \"2026-09-07T15:00:00Z\"\n  \
             records_per_partition: 25\nobjectives:\n  rto_seconds: 900\n  pass_rate: 1.0\n\
             evidence:\n  backend: \"s3\"\n  bucket: \"logweir-evidence\"\n  prefix: \
             \"logweir/\"\n"
        )
    }

    const PREFIXED: &str = "  topic_naming:\n    prefix: \"restore-20260907T140500Z-\"\n";
    /// The `restore` block of a plan that restores every partition to one
    /// instant.
    const AT: &str = "  point_in_time: \"2026-09-07T14:05:00Z\"\n";
    /// The opening of a `restore` block that names partition subsets: the
    /// plan grammar writes those beside the interval form only.
    const FROM_THE_FLOOR: &str = "  point_in_time: \"../2026-09-07T14:05:00Z\"\n";
    const ORIGINAL: &str =
        "  topic_naming:\n    prefix: \"\"\n    original_name:\n      owners: []\n";

    fn names(n: usize) -> Vec<String> {
        (0..n).map(|i| format!("topic-{i:05}")).collect()
    }

    fn plan(topics: &[&str]) -> String {
        let topics: Vec<String> = topics.iter().map(|t| (*t).to_string()).collect();
        plan_with(&topics, PREFIXED, AT, "kafka-backups")
    }

    fn request(plan: &str, subject: Option<&str>) -> RestoreAuthorization {
        RestoreAuthorization {
            format_version: "2.0.0".into(),
            kind: RESTORE_AUTHORIZATION_KIND.into(),
            authorization_mode: ApprovalMode::Governed,
            subject: AuthorizedSubject {
                api_version: "logweir.dev/v1alpha1".into(),
                kind: "Restore".into(),
                namespace: "team-a".into(),
                name: "rst-1".into(),
                uid: "uid-1".into(),
            },
            plan_hash: crate::ids::sha256_prefixed(plan.as_bytes()),
            requester: Requester {
                issuer: "https://idp.example".into(),
                subject: "alice".into(),
            },
            policy: PolicyRef {
                name: "prod-pair".into(),
                digest: format!("sha256:{}", "c".repeat(64)),
            },
            issued_at: at("2026-10-10T12:00:00Z"),
            expires_at: at("2026-10-10T13:00:00Z"),
            ticket: Some("CHG-4711".into()),
            approval_subject: subject.map(str::to_string),
            original_name_confirmation: None,
            approver: None,
            approved_at: None,
        }
    }

    /// **The scope is every fact of the request and of the plan it hashes.**
    /// KILLS: a scope that leaves out the target cluster, the mapping, the
    /// source, the window or the coverage; a mapping derived any other way
    /// than the runner's.
    #[test]
    fn the_scope_is_every_fact_of_the_request_and_its_plan() {
        let text = plan(&["orders", "payments"]);
        let scope = approval_scope(&request(&text, None), text.as_bytes()).expect("complete");
        assert_eq!(scope.requester.issuer, "https://idp.example");
        assert_eq!(scope.requester.subject, "alice");
        assert_eq!(
            (
                scope.namespace.as_str(),
                scope.restore.as_str(),
                scope.restore_uid.as_str()
            ),
            ("team-a", "rst-1", "uid-1")
        );
        assert_eq!(
            scope.plan_hash,
            crate::ids::sha256_prefixed(text.as_bytes())
        );
        assert_eq!(scope.approval_subject, ApprovalSubject::Ordinary);
        assert_eq!(scope.policy_name, "prod-pair");
        assert_eq!(scope.ticket.as_deref(), Some("CHG-4711"));
        assert_eq!(scope.requested_at, at("2026-10-10T12:00:00Z"));
        assert_eq!(scope.expires_at, at("2026-10-10T13:00:00Z"));
        assert_eq!(scope.plan_name.as_deref(), Some("orders-pitr"));
        assert_eq!(scope.source.storage.backend, "s3");
        assert_eq!(
            scope.source.storage.location,
            "s3://kafka-backups/drill-demo"
        );
        assert_eq!(
            scope.source.storage.endpoint.as_deref(),
            Some("http://minio:9000")
        );
        assert_eq!(scope.source.storage.region.as_deref(), Some("us-east-1"));
        assert!(scope.source.storage.plaintext_http);
        assert_eq!(scope.source.backup, "01JB7Z0000000000000000000B");
        assert_eq!(scope.source.point, None);
        assert_eq!(scope.recovery.point_in_time, at("2026-09-07T14:05:00Z"));
        assert!(scope.recovery.point_in_time_stated);
        assert_eq!(scope.recovery.window_start, None);
        assert_eq!(
            scope.target.bootstrap_servers,
            vec!["kafka-0.target:9092", "kafka-1.target:9092"]
        );
        assert_eq!(scope.target.mode, "newTopic");
        assert_eq!(scope.target.auth_mode, "plaintext");
        assert_eq!(scope.target.auth_username, None);
        assert_eq!(scope.target.replication_factor, 1);
        assert_eq!(scope.target.teardown, "delete");
        assert_eq!(scope.target.topic_prefix, "restore-20260907T140500Z-");
        assert_eq!(
            scope.topics,
            vec![
                ScopeTopic {
                    source: "orders".into(),
                    target: "restore-20260907T140500Z-orders".into(),
                    original_name: false,
                    partitions: None,
                },
                ScopeTopic {
                    source: "payments".into(),
                    target: "restore-20260907T140500Z-payments".into(),
                    original_name: false,
                    partitions: None,
                },
            ]
        );
        assert_eq!(scope.verification.coverage, "sampled");
        assert_eq!(scope.verification.window_end, at("2026-09-07T15:00:00Z"));
        assert_eq!(scope.evidence.location, "s3://logweir-evidence/logweir/");
        // The mapping is the runner's own rule, name for name.
        let parsed: DrillSpec = serde_yaml::from_str(&text).unwrap();
        let prefix = crate::spec::target_topic_prefix(&parsed);
        for topic in &scope.topics {
            assert_eq!(topic.target, format!("{prefix}{}", topic.source));
        }
    }

    /// **A plan that allows transaction markers says so in its scope**: the
    /// override changes what the run writes, so the second person sees it.
    /// Control: the same plan without it. KILLS: the field dropped from the
    /// scope.
    #[test]
    fn a_plan_allowing_transaction_markers_shows_it_in_its_scope() {
        let topics: Vec<String> = vec!["orders".into()];
        let allowed = plan_with(
            &topics,
            PREFIXED,
            &format!("{AT}  allow_transaction_markers: true\n"),
            "kafka-backups",
        );
        let scope = approval_scope(&request(&allowed, None), allowed.as_bytes()).expect("complete");
        assert!(scope.target.allow_transaction_markers);
        let plain = plan(&["orders"]);
        let scope = approval_scope(&request(&plain, None), plain.as_bytes()).expect("complete");
        assert!(!scope.target.allow_transaction_markers);
    }

    /// **PROD-05.2: the plan's configuration choice is in its scope**, the
    /// transition and every owner it states; a plan without the block shows
    /// none, and more owners than topics is not showable. KILLS: the field
    /// dropped from the scope.
    #[test]
    fn the_topic_configuration_choice_is_shown_as_signed() {
        let topics: Vec<String> = vec!["orders".into()];
        let target = format!(
            "{PREFIXED}  topic_configuration:\n    transition: afterVerification\n    owners:\n      - {{topic: orders, kind: external, reference: tf-orders}}\n"
        );
        let text = plan_with(&topics, &target, AT, "kafka-backups");
        let scope = approval_scope(&request(&text, None), text.as_bytes()).expect("complete");
        let shown = scope
            .target
            .topic_configuration
            .expect("the block is shown");
        assert_eq!(shown.transition, "afterVerification");
        assert_eq!(
            shown.owners,
            Some(vec![ScopeOwner {
                topic: "orders".into(),
                kind: "external".into(),
                reference: "tf-orders".into(),
            }])
        );
        let plain = plan(&["orders"]);
        let scope = approval_scope(&request(&plain, None), plain.as_bytes()).expect("complete");
        assert_eq!(scope.target.topic_configuration, None);
        let two = format!(
            "{PREFIXED}  topic_configuration:\n    transition: none\n    owners:\n      - {{topic: orders, kind: external, reference: a}}\n      - {{topic: orders, kind: external, reference: b}}\n"
        );
        let text = plan_with(&topics, &two, AT, "kafka-backups");
        assert_eq!(
            approval_scope(&request(&text, None), text.as_bytes()),
            Err(ScopeIncomplete::OwnersNotShowable)
        );
    }

    /// **A plan that is not the request's has no scope.** The function holds
    /// the plan bytes to the hash the console signed before it reads one of
    /// them, so nothing shown can come from bytes the signature does not
    /// cover — and two different scopes cannot stand behind one request.
    /// KILLS: a scope built from a plan without the hash check.
    #[test]
    fn the_scope_is_a_function_of_the_signed_bytes_and_of_nothing_else() {
        let asked = plan(&["orders", "payments"]);
        let other = plan(&["orders", "payroll"]);
        let request = request(&asked, None);
        assert_eq!(
            approval_scope(&request, other.as_bytes()),
            Err(ScopeIncomplete::PlanNotTheRequests)
        );
        // One byte of the plan, anywhere: another hash, so not this request's.
        let mut edited = asked.clone().into_bytes();
        let at = edited.len() - 2;
        edited[at] ^= 1;
        assert_eq!(
            approval_scope(&request, &edited),
            Err(ScopeIncomplete::PlanNotTheRequests)
        );
        // The same two byte strings give the same scope, every time.
        let once = approval_scope(&request, asked.as_bytes()).unwrap();
        assert_eq!(once, approval_scope(&request, asked.as_bytes()).unwrap());
        // Two plans that differ in what is shown differ in their hash.
        let theirs = approval_scope(&self::request(&other, None), other.as_bytes()).unwrap();
        assert_ne!(once.topics, theirs.topics);
        assert_ne!(once.plan_hash, theirs.plan_hash);
        // NEGATIVE CONTROL: the request's own plan has a scope.
        assert!(approval_scope(&request, asked.as_bytes()).is_ok());
    }

    /// **At the bound every topic is in the scope; one over, there is no
    /// scope at all** — never the first 1,024.
    /// KILLS: a scope that truncates; a bound applied to what is shown and
    /// not to what is approvable.
    #[test]
    fn a_scope_shows_every_topic_or_none() {
        let at_bound = plan_with(&names(MAX_SCOPE_TOPICS), PREFIXED, AT, "kafka-backups");
        let scope = plan_scope(at_bound.as_bytes()).expect("at the bound");
        assert_eq!(scope.topics.len(), MAX_SCOPE_TOPICS);
        for index in [0, MAX_SCOPE_TOPICS / 2, MAX_SCOPE_TOPICS - 1] {
            assert_eq!(scope.topics[index].source, format!("topic-{index:05}"));
            assert_eq!(
                scope.topics[index].target,
                format!("restore-20260907T140500Z-topic-{index:05}")
            );
        }
        let over = plan_with(&names(MAX_SCOPE_TOPICS + 1), PREFIXED, AT, "kafka-backups");
        assert_eq!(
            plan_scope(over.as_bytes()),
            Err(ScopeIncomplete::TooManyTopics {
                count: MAX_SCOPE_TOPICS + 1
            })
        );
        let sentence = ScopeIncomplete::TooManyTopics {
            count: MAX_SCOPE_TOPICS + 1,
        }
        .sentence();
        assert!(
            sentence.contains("1025 topics")
                && sentence.contains("at most 1024 topics each")
                && sentence.contains("strict"),
            "{sentence}"
        );
        assert_eq!(
            plan_scope(plan_with(&[], PREFIXED, AT, "kafka-backups").as_bytes()),
            Err(ScopeIncomplete::NoTopics)
        );
    }

    /// **A restore under the original topic names is marked, topic by
    /// topic**, and its request must say so. KILLS: an original-name plan
    /// shown as an ordinary one; a request whose subject is not the plan's.
    #[test]
    fn original_names_are_marked_and_the_requests_subject_must_be_the_plans() {
        let topics = vec!["orders".to_string(), "payments".to_string()];
        let text = plan_with(&topics, ORIGINAL, AT, "kafka-backups");
        let scope = approval_scope(&request(&text, Some("originalName")), text.as_bytes()).unwrap();
        assert_eq!(scope.approval_subject, ApprovalSubject::OriginalName);
        assert_eq!(scope.target.topic_prefix, "");
        for topic in &scope.topics {
            assert!(topic.original_name, "{topic:?}");
            assert_eq!(topic.source, topic.target);
        }
        // The plan writes under original names and the request says ordinary:
        // a reviewer would be shown the wrong subject.
        assert_eq!(
            approval_scope(&request(&text, None), text.as_bytes()),
            Err(ScopeIncomplete::SubjectNotThePlans)
        );
        // And the reverse.
        let prefixed = plan(&["orders"]);
        assert_eq!(
            approval_scope(
                &request(&prefixed, Some("originalName")),
                prefixed.as_bytes()
            ),
            Err(ScopeIncomplete::SubjectNotThePlans)
        );
        assert_eq!(
            approval_scope(
                &request(&prefixed, Some("somethingElse")),
                prefixed.as_bytes()
            ),
            Err(ScopeIncomplete::SubjectNotThePlans)
        );
        // NEGATIVE CONTROL: a prefixed restore marks nothing.
        let scope = approval_scope(&request(&prefixed, None), prefixed.as_bytes()).unwrap();
        assert!(scope.topics.iter().all(|t| !t.original_name));
    }

    /// **A partition subset is shown number by number.** KILLS: a subset
    /// summarised as a count; a subset of a topic the plan does not restore
    /// left out of the scope.
    #[test]
    fn partition_subsets_are_listed_or_the_scope_is_incomplete() {
        let topics = vec!["orders".to_string(), "payments".to_string()];
        let subset = format!("{FROM_THE_FLOOR}  partitions:\n    orders: [0, 2, 5]\n");
        let text = plan_with(&topics, PREFIXED, &subset, "kafka-backups");
        let scope = plan_scope(text.as_bytes()).unwrap();
        assert_eq!(scope.topics[0].partitions, Some(vec![0, 2, 5]));
        assert_eq!(scope.topics[1].partitions, None, "every partition");
        for (bad, why) in [
            ("  partitions:\n    orders: []\n", "empty"),
            ("  partitions:\n    orders: [1, 1]\n", "repeated"),
            ("  partitions:\n    orders: [-1]\n", "negative"),
            ("  partitions:\n    refunds: [0]\n", "another topic"),
        ] {
            let text = plan_with(
                &topics,
                PREFIXED,
                &format!("{FROM_THE_FLOOR}{bad}"),
                "kafka-backups",
            );
            assert_eq!(
                plan_scope(text.as_bytes()),
                Err(ScopeIncomplete::PartitionsNotShowable),
                "{why}"
            );
        }
        let many: Vec<String> = (0..=MAX_SCOPE_PARTITIONS).map(|p| p.to_string()).collect();
        let over = format!(
            "{FROM_THE_FLOOR}  partitions:\n    orders: [{}]\n",
            many.join(", ")
        );
        assert_eq!(
            plan_scope(plan_with(&topics, PREFIXED, &over, "kafka-backups").as_bytes()),
            Err(ScopeIncomplete::TooManyPartitions {
                count: MAX_SCOPE_PARTITIONS + 1
            })
        );
        let at_bound = format!(
            "{FROM_THE_FLOOR}  partitions:\n    orders: [{}]\n",
            many[..MAX_SCOPE_PARTITIONS].join(", ")
        );
        let scope = plan_scope(plan_with(&topics, PREFIXED, &at_bound, "kafka-backups").as_bytes())
            .unwrap();
        assert_eq!(
            scope.topics[0].partitions.as_ref().map(Vec::len),
            Some(MAX_SCOPE_PARTITIONS)
        );
    }

    /// **A value a page cannot show faithfully is not shown at all.** A
    /// topic that is not a Kafka name before or after its mapping, a name of
    /// more than Kafka's 249 characters, a control character or a
    /// right-to-left override in a bucket, a megabyte of backup id: no scope.
    /// And the sentence that says so repeats none of it.
    /// KILLS: a scope that carries such a value; a sentence that echoes it.
    #[test]
    fn a_value_that_cannot_be_shown_faithfully_leaves_no_scope() {
        let longest = "t".repeat(crate::guard::MAX_TOPIC_NAME_CHARS);
        // THE CONTROL: a name of the greatest legal length, restored under
        // itself, is in the scope whole.
        let text = plan_with(
            std::slice::from_ref(&longest),
            ORIGINAL,
            AT,
            "kafka-backups",
        );
        let scope = plan_scope(text.as_bytes()).expect("a maximal name is a name");
        assert_eq!(scope.topics[0].source, longest);
        assert_eq!(scope.topics[0].target.len(), 249);
        // The same name behind a prefix is longer than a broker accepts.
        let text = plan_with(
            std::slice::from_ref(&longest),
            PREFIXED,
            AT,
            "kafka-backups",
        );
        assert_eq!(
            plan_scope(text.as_bytes()),
            Err(ScopeIncomplete::TopicNotAName)
        );
        for hostile in [
            "orders eu",
            "orders\\u202Epayments",
            "orders\\nrefunds",
            "..",
            "orders/eu",
        ] {
            let text = plan_with(&[hostile.to_string()], PREFIXED, AT, "kafka-backups");
            assert_eq!(
                plan_scope(text.as_bytes()),
                Err(ScopeIncomplete::TopicNotAName),
                "{hostile}"
            );
        }
        assert_eq!(
            plan_scope(
                plan_with(
                    &["orders".to_string(), "orders".to_string()],
                    PREFIXED,
                    "",
                    "kafka-backups"
                )
                .as_bytes()
            ),
            Err(ScopeIncomplete::TopicRepeated)
        );
        let big = "b".repeat(MAX_SCOPE_TEXT_CHARS + 1);
        for bucket in [
            "kafka\\u202Ebackups",
            "kafka\\tbackups",
            "sauvegardes-\\u00E9t\\u00E9",
            &big,
        ] {
            let text = plan_with(&["orders".to_string()], PREFIXED, AT, bucket);
            let refused = plan_scope(text.as_bytes()).expect_err(bucket);
            assert_eq!(
                refused,
                ScopeIncomplete::ValueNotShowable {
                    what: "the source archive's location"
                }
            );
            let sentence = refused.sentence();
            assert!(
                sentence.chars().all(|c| matches!(c, ' '..='~')),
                "{sentence}"
            );
            assert!(!sentence.contains("kafka"), "{sentence}");
            assert!(sentence.len() < 600, "{}", sentence.len());
        }
        // Not a plan at all.
        for bytes in [&b"\xff\xfe"[..], b"not: [a, plan", b"topics: 7"] {
            assert_eq!(plan_scope(bytes), Err(ScopeIncomplete::PlanUnreadable));
        }
        // No bootstrap server: the cluster written into cannot be shown.
        let text = plan(&["orders"]).replace(
            "  bootstrap_servers:\n    - \"kafka-0.target:9092\"\n    - \"kafka-1.target:9092\"\n",
            "  bootstrap_servers: []\n",
        );
        assert_eq!(
            plan_scope(text.as_bytes()),
            Err(ScopeIncomplete::BootstrapServersNotShowable)
        );
    }

    /// **Who asked is shown only as it is.** A requester subject carrying a
    /// right-to-left override could make the approver misread who asked, which
    /// is the fact they decide on: the scope is incomplete. The same for the
    /// ticket and the policy's name. NEGATIVE CONTROL: the plain request is
    /// complete. KILLS: a scope that copies a request field without the rule.
    #[test]
    fn request_text_that_cannot_be_shown_faithfully_leaves_no_scope() {
        let text = plan(&["orders"]);
        assert!(approval_scope(&request(&text, None), text.as_bytes()).is_ok());
        let mut bidi = request(&text, None);
        bidi.requester.subject = "bob\u{202e}ecila".into();
        assert_eq!(
            approval_scope(&bidi, text.as_bytes()),
            Err(ScopeIncomplete::ValueNotShowable {
                what: "the requester's subject"
            })
        );
        let mut ticket = request(&text, None);
        ticket.ticket = Some("CHG-1\nApproved by: security".into());
        assert_eq!(
            approval_scope(&ticket, text.as_bytes()),
            Err(ScopeIncomplete::ValueNotShowable {
                what: "the change ticket"
            })
        );
        let mut policy = request(&text, None);
        policy.policy.name = "prod-pair\u{200b}".into();
        assert!(approval_scope(&policy, text.as_bytes()).is_err());
    }

    /// **The requester's owner statement is part of what the second person
    /// approves**. For an original-name restore the
    /// console gives the runner nowhere else to look, so the plan's
    /// `original_name.owners` / `owner_path` alone decide whether it writes:
    /// the scope carries them as signed, in their three cases, and an owner
    /// that cannot be shown leaves no scope.
    ///
    /// NEGATIVE CONTROLS: a plan with `owner_path: true` shows it, with its
    /// owners; `owners: []` shows the empty statement (not "not declared");
    /// a prefixed restore carries no statement at all.
    /// KILLS: a scope that drops the statement.
    #[test]
    fn the_owner_statement_of_an_original_name_plan_is_shown_as_signed() {
        let topics = vec!["orders".to_string(), "payments".to_string()];
        let statement = |naming: &str| {
            plan_scope(plan_with(&topics, naming, AT, "kafka-backups").as_bytes())
                .map(|scope| scope.target.owner_statement)
        };
        // `owners: []`: the requester states no owner manages any name.
        assert_eq!(
            statement(ORIGINAL),
            Ok(Some(ScopeOwnerStatement {
                owners: Some(vec![]),
                owner_path: false
            }))
        );
        // Not declared: nothing stated (the run will refuse from the console).
        let undeclared = "  topic_naming:\n    prefix: \"\"\n    original_name: {}\n";
        assert_eq!(
            statement(undeclared),
            Ok(Some(ScopeOwnerStatement {
                owners: None,
                owner_path: false
            }))
        );
        // The owner path, with its owner: shown as signed.
        let path =
            "  topic_naming:\n    prefix: \"\"\n    original_name:\n      owners:\n        - \
                    topic: \"orders\"\n          kind: \"strimzi\"\n          reference: \
                    \"kafka/orders\"\n      owner_path: true\n";
        assert_eq!(
            statement(path),
            Ok(Some(ScopeOwnerStatement {
                owners: Some(vec![ScopeOwner {
                    topic: "orders".into(),
                    kind: "strimzi".into(),
                    reference: "kafka/orders".into(),
                }]),
                owner_path: true
            }))
        );
        // An owner reference that cannot be shown faithfully: no scope.
        let hostile = path.replace("kafka/orders", "kafka/\\u202Esredro");
        assert_eq!(
            statement(&hostile),
            Err(ScopeIncomplete::ValueNotShowable {
                what: "a declared owner"
            })
        );
        // More owners than topics: no scope.
        let many = "  topic_naming:\n    prefix: \"\"\n    original_name:\n      owners:\n\
                    {o}{o}{o}      owner_path: true\n"
            .replace(
                "{o}",
                "        - topic: \"orders\"\n          kind: \"external\"\n          reference: \"r\"\n",
            );
        assert_eq!(statement(&many), Err(ScopeIncomplete::OwnersNotShowable));
        // A prefixed restore carries no statement.
        assert_eq!(statement(PREFIXED), Ok(None));
    }

    /// **A plan that names a notification sink is not approvable in the
    /// console**: the run would send its result there and
    /// the scope does not show where. Each kind of sink leaves no scope.
    /// NEGATIVE CONTROL: `webhooks: []` (what the wizard writes) and no
    /// block at all are complete.
    /// KILLS: a scope that ignores `notifications`.
    #[test]
    fn a_plan_that_names_a_notification_sink_is_not_approvable_in_the_console() {
        let base = plan(&["orders"]);
        assert!(plan_scope(base.as_bytes()).is_ok(), "no block");
        let with = |block: &str| format!("{base}notifications:\n{block}");
        assert!(
            plan_scope(with("  webhooks: []\n").as_bytes()).is_ok(),
            "the wizard's"
        );
        for sink in [
            "  webhooks: [\"http://10.0.3.7:8080/hook\"]\n",
            "  slack_webhook: \"https://hooks.slack.test/x\"\n",
            "  pagerduty_routing_key: \"k\"\n",
            "  pagerduty_endpoint: \"https://events.pagerduty.test\"\n",
        ] {
            assert_eq!(
                plan_scope(with(sink).as_bytes()),
                Err(ScopeIncomplete::NotificationsNotShown),
                "{sink}"
            );
        }
        let sentence = ScopeIncomplete::NotificationsNotShown.sentence();
        assert!(
            !sentence.contains("10.0.3.7") && sentence.contains("wizard"),
            "{sentence}"
        );
    }

    /// Every reason has a stable word and a sentence of fixed words that
    /// says what to do.
    #[test]
    fn every_reason_has_a_word_and_a_sentence_of_its_own() {
        let reasons = [
            ScopeIncomplete::PlanNotTheRequests,
            ScopeIncomplete::PlanUnreadable,
            ScopeIncomplete::SubjectNotThePlans,
            ScopeIncomplete::NoTopics,
            ScopeIncomplete::TooManyTopics { count: 2000 },
            ScopeIncomplete::TopicRepeated,
            ScopeIncomplete::TopicNotAName,
            ScopeIncomplete::TooManyPartitions { count: 9000 },
            ScopeIncomplete::PartitionsNotShowable,
            ScopeIncomplete::BootstrapServersNotShowable,
            ScopeIncomplete::OwnersNotShowable,
            ScopeIncomplete::NotificationsNotShown,
            ScopeIncomplete::ValueNotShowable {
                what: "the backup set",
            },
            ScopeIncomplete::TooManyGroups { count: 101 },
            ScopeIncomplete::TooManyPositions { count: 8193 },
        ];
        let words: BTreeSet<&str> = reasons.iter().map(|r| r.code()).collect();
        let sentences: BTreeSet<String> = reasons.iter().map(|r| r.sentence()).collect();
        assert_eq!(words.len(), reasons.len());
        assert_eq!(sentences.len(), reasons.len());
        for reason in reasons {
            let sentence = reason.to_string();
            assert!(
                sentence.starts_with("A second person approves only what they are shown in full"),
                "{sentence}"
            );
            assert!(
                sentence.chars().all(|c| matches!(c, ' '..='~')),
                "{sentence}"
            );
        }
    }

    // ---- PROD-04.2: the switchover scope -----------------------------------

    use crate::switchover::{Expected, PlanGroup, PlanTopic, RestoreRef, SwitchoverPlan};

    fn sha(c: char) -> String {
        format!("sha256:{}", c.to_string().repeat(64))
    }

    /// A plan of `groups` groups, each over one topic of `per` partitions.
    fn switchover_plan(groups: usize, per: u32) -> SwitchoverPlan {
        SwitchoverPlan {
            format_version: "1.0.0".into(),
            restore: RestoreRef {
                name: "incident".into(),
                uid: "r-uid".into(),
                plan_hash: sha('a'),
                scorecard_sha256: sha('b'),
                mapping_sha256: sha('c'),
            },
            rollback_of: None,
            groups: (0..groups)
                .map(|g| PlanGroup {
                    source: format!("app-{g}"),
                    target: format!("app-{g}"),
                    topics: [(
                        "r-orders".to_string(),
                        PlanTopic {
                            offsets: (0..per).map(|p| (p, 40 + i64::from(p))).collect(),
                            approximate: vec![],
                            expected: Expected::none(),
                        },
                    )]
                    .into(),
                })
                .collect(),
        }
    }

    fn bytes(plan: &SwitchoverPlan) -> Vec<u8> {
        serde_yaml::to_string(plan).expect("yaml").into_bytes()
    }

    fn switchover_request(plan: &[u8]) -> RestoreAuthorization {
        let mut r = request("", None);
        r.subject.kind = crate::spec::SUBJECT_KIND_SWITCHOVER.into();
        r.subject.name = "swo-1".into();
        r.subject.uid = "s-uid".into();
        r.plan_hash = crate::ids::sha256_prefixed(plan);
        r
    }

    /// **A switchover's scope is every group, topic, partition and position,
    /// with each one's class and the position it must hold first.**
    /// KILLS: a scope that drops a group or a position, shows a position as
    /// exact that the plan marks approximate, or loses a stated `expected`.
    #[test]
    fn a_switchover_scope_is_every_position_of_the_request_and_its_plan() {
        let mut plan = switchover_plan(2, 3);
        let topic = plan.groups[1].topics.get_mut("r-orders").expect("topic");
        topic.approximate = vec![2];
        topic.expected = Expected::Positions([(0, 7), (1, 8), (2, 9)].into());
        let text = bytes(&plan);
        let scope = switchover_scope(&switchover_request(&text), &text).expect("complete");
        assert_eq!(
            (scope.switchover.as_str(), scope.switchover_uid.as_str()),
            ("swo-1", "s-uid")
        );
        assert_eq!(scope.requester.subject, "alice");
        assert_eq!(scope.ticket.as_deref(), Some("CHG-4711"));
        assert_eq!(scope.plan.restore.name, "incident");
        assert_eq!(scope.plan.restore.mapping_sha256, sha('c'));
        assert_eq!(scope.plan.groups.len(), 2);
        assert_eq!(scope.plan.positions, 6);
        let first = &scope.plan.groups[0].topics[0];
        assert_eq!(first.topic, "r-orders");
        assert_eq!(
            first.positions[1],
            ScopePosition {
                partition: 1,
                offset: 41,
                approximate: false,
                expected: None
            }
        );
        let second = &scope.plan.groups[1].topics[0].positions;
        assert_eq!(
            second[2],
            ScopePosition {
                partition: 2,
                offset: 42,
                approximate: true,
                expected: Some(9)
            }
        );
        assert_eq!(second[0].expected, Some(7));
    }

    /// **Complete, or nothing**, for a switchover: the plan hash first, then
    /// the counts BEFORE the plan's own shape (so a plan too large to show is
    /// refused for that, by name), then each value by the showable rule, and
    /// a request that is not a switchover's.
    /// KILLS: a scope over bytes the request does not hash; a count checked
    /// after the grammar (`planUnreadable` for an 8193-position plan); a
    /// group id that is not printable ASCII shown anyway; a restore's
    /// request read as a switchover's.
    #[test]
    fn a_switchover_scope_is_complete_or_names_why_not() {
        let ok = bytes(&switchover_plan(1, 1));
        let other = bytes(&switchover_plan(1, 2));
        assert_eq!(
            switchover_scope(&switchover_request(&ok), &other),
            Err(ScopeIncomplete::PlanNotTheRequests)
        );
        // 8192 positions: shown whole. 8193: refused by count, not grammar.
        let full = bytes(&switchover_plan(2, 4096));
        assert_eq!(
            switchover_scope(&switchover_request(&full), &full)
                .expect("8192 positions are shown")
                .plan
                .positions,
            8192
        );
        let mut over = switchover_plan(2, 4096);
        over.groups[1]
            .topics
            .get_mut("r-orders")
            .expect("topic")
            .offsets
            .insert(5000, 1);
        let over = bytes(&over);
        assert_eq!(
            switchover_scope(&switchover_request(&over), &over),
            Err(ScopeIncomplete::TooManyPositions { count: 8193 })
        );
        let many = bytes(&switchover_plan(101, 1));
        assert_eq!(
            switchover_scope(&switchover_request(&many), &many),
            Err(ScopeIncomplete::TooManyGroups { count: 101 })
        );
        let mut accented = switchover_plan(1, 1);
        accented.groups[0].target = "caf\u{e9}".into();
        let accented = bytes(&accented);
        assert_eq!(
            switchover_scope(&switchover_request(&accented), &accented),
            Err(ScopeIncomplete::ValueNotShowable {
                what: "a consumer group id"
            })
        );
        let unreadable = b"format_version: 1.0.0\n".to_vec();
        assert_eq!(
            switchover_scope(&switchover_request(&unreadable), &unreadable),
            Err(ScopeIncomplete::PlanUnreadable)
        );
        let mut restore_kind = switchover_request(&ok);
        restore_kind.subject.kind = "Restore".into();
        assert_eq!(
            switchover_scope(&restore_kind, &ok),
            Err(ScopeIncomplete::SubjectNotThePlans)
        );
        let mut with_subject = switchover_request(&ok);
        with_subject.approval_subject = Some("originalName".into());
        assert_eq!(
            switchover_scope(&with_subject, &ok),
            Err(ScopeIncomplete::SubjectNotThePlans)
        );
        // A switchover's sentences name the switchover; a restore's are
        // word for word what they were.
        for reason in [
            ScopeIncomplete::PlanNotTheRequests,
            ScopeIncomplete::PlanUnreadable,
            ScopeIncomplete::TooManyPositions { count: 8193 },
            ScopeIncomplete::ValueNotShowable {
                what: "a consumer group id",
            },
        ] {
            let words = reason.sentence_for(ScopeOf::Switchover);
            assert!(
                words.contains("witchover") && !words.contains("Restore"),
                "{words}"
            );
            assert_eq!(reason.sentence_for(ScopeOf::Restore), reason.sentence());
        }
    }
}
