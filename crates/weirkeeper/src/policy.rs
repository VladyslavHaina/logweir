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
//! That split is the field's value: a schedule's spec is editable, so
//! `metadata.generation` moves on any edit, including the commonest, a
//! `suspend` flip. "revision g7 · policy sha256:ab12…" shows that suspending a
//! schedule left the policy alone and a topic-list edit did not.
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
/// Inside the bytes, not beside them, so a format change is a digest change.
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
    /// The selected consumer groups, sorted and deduplicated: what the run
    /// records positions for is part of what it does. Absent (and skipped) when
    /// none is selected, so every policy that selects none digests exactly as
    /// it did before the field existed.
    #[serde(skip_serializing_if = "Option::is_none")]
    consumer_groups: Option<Vec<String>>,
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

/// The consumer groups a spec selects: `None` when absent or empty, so an empty
/// list is no selection, in the digest, the frozen plan and the runner's
/// document alike.
#[must_use]
pub fn selected_groups(spec: &BackupSpec) -> Option<&[String]> {
    spec.consumer_groups
        .as_deref()
        .filter(|groups| !groups.is_empty())
}

/// `sha256:<lowercase hex>` over the canonical JSON of this spec's run policy.
/// It cannot fail in practice (the document is a fixed struct of strings and
/// integers), so the `expect` in `run_policy_document` names that rather than
/// handing every caller an error to invent a reason for.
#[must_use]
pub fn run_policy_sha256(spec: &BackupSpec) -> String {
    logweir_core::ids::sha256_prefixed(&run_policy_document(spec))
}

/// The canonical run-policy document [`run_policy_sha256`] digests.
fn run_policy_document(spec: &BackupSpec) -> Vec<u8> {
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
        consumer_groups: selected_groups(spec).map(|groups| {
            let mut groups = groups.to_vec();
            groups.sort();
            groups.dedup();
            groups
        }),
    };
    logweir_core::det_json::to_deterministic_json(&policy)
        .expect("a RunPolicyV1 is strings and integers and always canonicalises")
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
/// Every problem, not the first: a 422 that names one of three mistakes makes
/// the operator submit three times. This is [`validate_topic_selection`] plus
/// the non-topic bounds, kept separate because they are refused in different
/// places: the selection half is the `Backup` reconciler's terminal
/// `InvalidTopicSelection` (D1 §3.4), while `deadlineSeconds` belongs to
/// `ExecutionSpecInvalid`.
///
/// # Errors
///
/// A [`FieldError`] per problem, each naming a dotted path rooted at `spec`.
pub fn validate_run_policy(spec: &BackupSpec) -> Result<SelectionShape, Vec<FieldError>> {
    let mut errs: Vec<FieldError> = match validate_topic_selection(spec) {
        Ok(_) => Vec::new(),
        Err(errs) => errs,
    };
    let shape = validate_topic_selection(spec).ok();

    if spec.deadline_seconds <= 0 {
        errs.push(field_error(
            "spec.deadlineSeconds",
            format!(
                "deadlineSeconds is {}; a Job needs a positive activeDeadlineSeconds",
                spec.deadline_seconds
            ),
        ));
    }
    // The runner refuses such a selection at phase −1 (exit 3); refusing it
    // here keeps a schedule from admitting a policy every run of which would be
    // refused.
    if let Some(why) =
        selected_groups(spec).and_then(logweir_core::consumer_positions::refuse_selection)
    {
        errs.push(field_error("spec.consumerGroups", why));
    }
    // And so does a named allowlist over the maximum one backup may name
    // (`logweir_core::topic_budget`): its receipt could not be listed by the
    // recovery catalog or verified by this controller. The runner refuses at
    // phase −1, the freeze before any Job exists, and this refuses the policy,
    // so a schedule is `Ready=False` by name and the API answers 422.
    if let Some(why) = logweir_core::topic_budget::refuse_topic_count(spec.topics.len()) {
        errs.push(field_error("spec.topics", why));
    }

    match (errs.is_empty(), shape) {
        (true, Some(shape)) => Ok(shape),
        _ => Err(errs),
    }
}

/// The TOPIC half of the run policy: which of D1 §7.1's two shapes this spec
/// declares, or every reason it declares neither.
///
/// # Errors
///
/// A [`FieldError`] per problem, each naming a dotted path rooted at `spec`.
pub fn validate_topic_selection(spec: &BackupSpec) -> Result<SelectionShape, Vec<FieldError>> {
    let mut errs: Vec<FieldError> = Vec::new();

    // The third shape matters most: `topics` non-empty and a dynamic block is
    // two answers to one question, and `topics: []` with no dynamic block is
    // the shape that meant "everything" to the engine (guard G-GLOB). Neither
    // is expressible in CEL at the 1.29 floor without blocking `suspend`
    // updates on objects stored with `topics: []`, so both are refused here, by
    // the scheduler, the Backup controller and the API alike.
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

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(consumer_groups: Option<Vec<&str>>) -> BackupSpec {
        let mut v = serde_json::json!({
            "sourceRef": {"name": "prod"},
            "topics": ["payments", "orders"],
            "archive": {"url": "s3://kafka-backups/logweir", "secretRef": {"name": "logweir-s3"}},
            "triggeredBy": "manual",
            "deadlineSeconds": 3600
        });
        if let Some(groups) = consumer_groups {
            v["consumerGroups"] = serde_json::json!(groups);
        }
        serde_json::from_value(v).expect("a BackupSpec")
    }

    /// A policy that selects no consumer group is the document it was before
    /// the field existed, byte for byte, so every existing `runPolicySha256` is
    /// unchanged. The literal is the original `run-policy/v1` document. KILLS:
    /// serialising an absent selection (`consumerGroups: null`), or an empty
    /// one (`[]`).
    #[test]
    fn a_policy_that_selects_no_group_is_the_document_it_was() {
        let before = serde_json::json!({
            "formatVersion": "run-policy/v1",
            "sourceRef": {"name": "prod"},
            "topics": ["orders", "payments"],
            "allUserTopics": null,
            "archive": {"url": "s3://kafka-backups/logweir", "secretRef": {"name": "logweir-s3"}},
            "activeDeadlineSeconds": 3600
        });
        for groups in [None, Some(vec![])] {
            let doc = String::from_utf8(run_policy_document(&spec(groups.clone()))).unwrap();
            assert!(!doc.contains("consumerGroups"), "{groups:?}: {doc}");
            let parsed: serde_json::Value = serde_json::from_str(&doc).unwrap();
            assert_eq!(parsed, before, "{groups:?}");
            assert_eq!(
                doc,
                String::from_utf8(logweir_core::det_json::to_deterministic_json(&before).unwrap())
                    .unwrap(),
                "{groups:?}: the same bytes, in the same order"
            );
        }
        let selected: serde_json::Value =
            serde_json::from_slice(&run_policy_document(&spec(Some(vec!["b", "a", "b"])))).unwrap();
        assert_eq!(selected["consumerGroups"], serde_json::json!(["a", "b"]));
    }
}
