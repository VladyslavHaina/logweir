//! **PROD-16.2 — the APPROVAL SCOPE: what a second person must be shown
//! before they approve, defined once.**
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
/// at most 256 topics (`logweir-api`'s `schedules::MAX_TOPICS`), and FX-33's
/// target for a catalog point is 500. A hand-written plan may name more (the
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
    /// The inclusive start of the replayed window, when the plan narrows it
    /// (PROD-11.1). Absent restores from the archive's floor.
    pub window_start: Option<DateTime<Utc>>,
    /// `producerTime` when the plan accepts producer time for its selection
    /// (FX-8).
    pub time_basis: Option<&'static str>,
}

/// The cluster written into.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopeTarget {
    /// The bootstrap servers the runner dials: the plan's own.
    pub bootstrap_servers: Vec<String>,
    /// How it authenticates there: `plaintext`, `scramSha512`, …
    pub auth_mode: &'static str,
    /// `scratch` or `newTopic`.
    pub mode: &'static str,
    /// The prefix every source topic is mapped through
    /// ([`crate::spec::target_topic_prefix`]); empty for a restore under the
    /// original topic names.
    pub topic_prefix: String,
}

/// One source topic, and the name it is restored under.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopeTopic {
    /// The topic in the archive.
    pub source: String,
    /// The topic written on the target.
    pub target: String,
    /// Whether this is a write under the source's ORIGINAL name (PROD-15.1):
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
    /// A text value is longer than a scope shows, or is not printable ASCII.
    ValueNotShowable {
        /// Which value, in fixed words.
        what: &'static str,
    },
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
            Self::ValueNotShowable { .. } => "valueNotShowable",
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
            Self::ValueNotShowable { what } => format!(
                "{WAY_OUT}, and {what} in this Restore's plan is longer than \
                 {MAX_SCOPE_TEXT_CHARS} characters or is not printable ASCII, so what a \
                 reviewer would see is not what the runner would read. Correct it, or approve \
                 the restore under a strict (personal-key) policy"
            ),
        }
    }
}

impl std::fmt::Display for ScopeIncomplete {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.sentence())
    }
}

/// Whether `text` is printable ASCII (space to `~`) of at most
/// [`MAX_SCOPE_TEXT_CHARS`] characters.
fn showable(text: &str) -> bool {
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

    // ---- the topics, every one --------------------------------------------
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

    // ---- the target cluster -----------------------------------------------
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
            mode: match plan.target.mode {
                TargetMode::Scratch => "scratch",
                TargetMode::NewTopic => "newTopic",
            },
            topic_prefix: prefix,
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
///
/// # Errors
///
/// [`ScopeIncomplete`].
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
    Ok(ApprovalScope {
        requester: ScopePrincipal {
            issuer: request.requester.issuer.clone(),
            subject: request.requester.subject.clone(),
        },
        namespace: request.subject.namespace.clone(),
        restore: request.subject.name.clone(),
        restore_uid: request.subject.uid.clone(),
        plan_hash: request.plan_hash.clone(),
        approval_subject: subject,
        policy_name: request.policy.name.clone(),
        policy_digest: request.policy.digest.clone(),
        ticket: request.ticket.clone(),
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
            ScopeIncomplete::ValueNotShowable {
                what: "the backup set",
            },
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
}
