//! The run policy digest: one number that says whether two runs were asked to
//! do the same thing.
//!
//! # What is in it, and what is deliberately not
//!
//! `runPolicySha256` covers the fields that decide **WHAT** a run does — its
//! source, its topic selection, where the archive goes and how long the Job
//! may take. It excludes everything that decides **WHEN**: cadence, time zone,
//! starting deadline, catch-up, retry, concurrency, retention and `suspend`.
//!
//! That split is the whole value of the field. PLAT-05.1 makes a schedule's
//! spec editable, so `metadata.generation` moves whenever anything changes —
//! including a `suspend` flip, which is the commonest edit there is. An
//! operator looking at "revision g7 · policy sha256:ab12…" can see at a glance
//! that suspending and resuming a schedule left the policy digest alone, and
//! that a topic-list edit did not. A digest over the whole spec could not say
//! that.
//!
//! # One implementation, three callers
//!
//! The scheduler fails closed on an invalid policy, the `Backup` controller
//! refuses terminally, and the API answers 422 — from
//! [`validate_run_policy`], so the three cannot disagree about what a valid
//! policy is. [`run_policy_sha256`] is likewise the only place the digest is
//! computed; a second implementation is how a stored digest and a recomputed
//! one come to differ for a run nobody edited.

use logweir_core::destination::FieldError;
use serde::Serialize;

use crate::crds::backup::BackupSpec;
use crate::crds::selection::{AllUserTopics, IncompleteDiscovery};

/// The format version inside the digested document.
///
/// INSIDE THE BYTES, not beside them. A digest whose input format could change
/// without the value changing is a digest that silently compares two different
/// questions; carrying the version in the canonical JSON makes a format change
/// a digest change, which is what a mismatch is supposed to report.
pub const RUN_POLICY_FORMAT: &str = "run-policy/v1";

/// The archive half of a run policy.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ArchivePolicy<'a> {
    url: &'a str,
    secret_ref: Option<NameOnly<'a>>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct NameOnly<'a> {
    name: &'a str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ExclusionsPolicy {
    topics: Vec<String>,
    prefixes: Vec<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AllUserTopicsPolicy {
    exclude: ExclusionsPolicy,
    incomplete_discovery: &'static str,
}

/// The document `runPolicySha256` is the digest of (D1 §3.2).
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RunPolicyV1<'a> {
    format_version: &'static str,
    source_ref: NameOnly<'a>,
    /// **Sorted and deduplicated**, and `[]` in dynamic mode.
    ///
    /// The digest canonicalises; `Backup.spec.topics` keeps the user's order
    /// verbatim. Two schedules that name the same topics in a different order
    /// are the same policy, and an operator who reorders a list has not
    /// changed what the run does.
    topics: Vec<String>,
    all_user_topics: Option<AllUserTopicsPolicy>,
    archive: ArchivePolicy<'a>,
    active_deadline_seconds: i64,
}

fn spelling(mode: IncompleteDiscovery) -> &'static str {
    match mode {
        IncompleteDiscovery::Refuse => "Refuse",
        IncompleteDiscovery::BackUpVisibleTopics => "BackUpVisibleTopics",
    }
}

fn sorted_unique(values: Option<&Vec<String>>) -> Vec<String> {
    let mut out: Vec<String> = values.cloned().unwrap_or_default();
    out.sort();
    out.dedup();
    out
}

fn all_user_topics_policy(block: &AllUserTopics) -> AllUserTopicsPolicy {
    let exclude = block.exclude.as_ref();
    AllUserTopicsPolicy {
        exclude: ExclusionsPolicy {
            topics: sorted_unique(exclude.and_then(|e| e.topics.as_ref())),
            prefixes: sorted_unique(exclude.and_then(|e| e.prefixes.as_ref())),
        },
        incomplete_discovery: spelling(block.incomplete_discovery),
    }
}

/// `sha256:<lowercase hex>` over the canonical JSON of this spec's run policy.
///
/// # Errors
///
/// Never, in practice: the document is a fixed struct of strings and integers,
/// and `to_deterministic_json` only fails on a value it cannot canonicalise
/// (a non-finite float, a non-string map key). The `expect` names that, rather
/// than propagating an error every caller would have to invent a reason for.
#[must_use]
pub fn run_policy_sha256(spec: &BackupSpec) -> String {
    let mut topics = spec.topics.clone();
    topics.sort();
    topics.dedup();
    let policy = RunPolicyV1 {
        format_version: RUN_POLICY_FORMAT,
        source_ref: NameOnly {
            name: &spec.source_ref.name,
        },
        topics,
        all_user_topics: spec.all_user_topics.as_ref().map(all_user_topics_policy),
        archive: ArchivePolicy {
            url: &spec.archive.url,
            secret_ref: spec
                .archive
                .secret_ref
                .as_ref()
                .map(|r| NameOnly { name: &r.name }),
        },
        active_deadline_seconds: spec.deadline_seconds,
    };
    let bytes = logweir_core::det_json::to_deterministic_json(&policy)
        .expect("a RunPolicyV1 is strings and integers and always canonicalises");
    logweir_core::ids::sha256_prefixed(&bytes)
}

/// The `spec.topics` / `spec.allUserTopics` combination this spec declares.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SelectionShape {
    /// A non-empty named allowlist, and no dynamic block.
    SelectedTopics,
    /// `topics: []` with a dynamic block.
    AllUserTopics,
}

/// Every field-level problem with this spec's run policy, or the shape it
/// declares.
///
/// EVERY PROBLEM, NOT THE FIRST, for the reason
/// `logweir_core::destination::validate` returns every one: a 422 that names
/// one of three mistakes makes the operator submit three times.
///
/// # Errors
///
/// A [`FieldError`] per problem, each naming a dotted path rooted at `spec`.
pub fn validate_run_policy(spec: &BackupSpec) -> Result<SelectionShape, Vec<FieldError>> {
    let mut errs: Vec<FieldError> = Vec::new();

    // THE THIRD SHAPE IS THE ONE THAT MATTERS. `topics` non-empty AND a
    // dynamic block is two answers to one question; `topics: []` with no
    // dynamic block is the shape that used to mean "everything" to the engine,
    // which is the defect guard G-GLOB exists for. Neither is expressible in
    // CEL at the 1.29 floor without blocking `suspend` updates on objects
    // stored with `topics: []`, so both are refused here — by the scheduler
    // before it admits, by the Backup controller before it POSTs, and by the
    // API as a 422.
    let shape = match (spec.topics.is_empty(), spec.all_user_topics.is_some()) {
        (false, false) => Some(SelectionShape::SelectedTopics),
        (true, true) => Some(SelectionShape::AllUserTopics),
        (false, true) => {
            errs.push(field_error(
                "spec.allUserTopics",
                "spec.allUserTopics requires spec.topics to be empty: a named allowlist and a \
                 dynamic selection are two answers to one question",
            ));
            None
        }
        (true, false) => {
            errs.push(field_error(
                "spec.topics",
                "spec.topics is empty and spec.allUserTopics is absent: a mandatory allowlist \
                 whose absence means `all topics` is not an allowlist (guard G-GLOB)",
            ));
            None
        }
    };

    for (index, topic) in spec.topics.iter().enumerate() {
        if !is_kafka_topic_name(topic) {
            errs.push(field_error(
                &format!("spec.topics[{index}]"),
                format!(
                    "`{}` is not a Kafka topic name: 1-249 characters matching \
                     ^[a-zA-Z0-9._-]+, and never `.` or `..`",
                    shown(topic)
                ),
            ));
        }
    }

    if let Some(block) = spec.all_user_topics.as_ref() {
        let exclude = block.exclude.as_ref();
        for (index, topic) in exclude
            .and_then(|e| e.topics.as_ref())
            .into_iter()
            .flatten()
            .enumerate()
        {
            if !is_kafka_topic_name(topic) {
                errs.push(field_error(
                    &format!("spec.allUserTopics.exclude.topics[{index}]"),
                    format!("`{}` is not a Kafka topic name", shown(topic)),
                ));
            }
        }
        for (index, prefix) in exclude
            .and_then(|e| e.prefixes.as_ref())
            .into_iter()
            .flatten()
            .enumerate()
        {
            if prefix.is_empty() || !prefix.chars().all(is_topic_char) {
                errs.push(field_error(
                    &format!("spec.allUserTopics.exclude.prefixes[{index}]"),
                    format!(
                        "`{}` is not a literal topic prefix: the same characters a topic name \
                         may use, and never a pattern",
                        shown(prefix)
                    ),
                ));
            }
        }
    }

    if spec.deadline_seconds <= 0 {
        errs.push(field_error(
            "spec.deadlineSeconds",
            format!(
                "deadlineSeconds is {}; a Job needs a positive activeDeadlineSeconds",
                spec.deadline_seconds
            ),
        ));
    }

    match (errs.is_empty(), shape) {
        (true, Some(shape)) => Ok(shape),
        _ => Err(errs),
    }
}

fn field_error(field: &str, message: impl Into<String>) -> FieldError {
    FieldError {
        field: field.to_string(),
        rule: "run-policy",
        message: message.into(),
    }
}

fn is_topic_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-'
}

/// A Kafka-legal topic name: `^[a-zA-Z0-9._-]{1,249}$`, and never `.` or `..`.
#[must_use]
pub fn is_kafka_topic_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 249
        && name != "."
        && name != ".."
        && name.chars().all(is_topic_char)
}

/// A value rendered into a message, bounded and with control characters
/// stripped — the same treatment `backup_execution::shown` gives a spec value,
/// for the same reason: a message is written into a condition an operator
/// reads, and a topic name is adopter-supplied text.
fn shown(value: &str) -> String {
    let cleaned: String = value
        .chars()
        .filter(|c| !c.is_control())
        .take(120)
        .collect();
    if value.chars().count() > 120 {
        format!("{cleaned}…")
    } else {
        cleaned
    }
}
