//! Stored objects to product DTOs. Pure functions, no I/O.
//!
//! WHAT A PROJECTION DROPS: every annotation and label (including this
//! service's own idempotency annotations), managed fields, owner references,
//! finalizers, the approval and sidecar documents outside the packet route,
//! and URL userinfo. What it keeps is named in `crate::contract`.

use kube::ResourceExt;
use weirkeeper::crds::approval::Approval as ApprovalCr;
use weirkeeper::crds::backup::Backup as BackupCr;
use weirkeeper::crds::backup_schedule::{BackupSchedule, ConcurrencyPolicy as CrdConcurrency};
use weirkeeper::crds::kafka_cluster::{AuthMode, KafkaCluster};
use weirkeeper::crds::restore::{Restore as RestoreCr, TargetMode};
use weirkeeper::crds::{ArchiveRef, LocalRef};

use crate::contract::{
    Approval, ApprovalPacket, ArchiveView, Backup, ConcurrencyPolicy, Connection,
    ConnectionAuthMode, ConnectionAuthView, NameRef, ObservedAuthView, ReachabilityState,
    ReachabilityView, RemovableSetView, Restore, RestoreMode, RestoreTargetView,
    RetentionReportView, RetentionView, Schedule, ScheduleStatusView, SubjectRefView,
    VerifiedSubjectView, WindowCoveredView,
};
use crate::status::{backup_operation, condition_view, restore_operation, summary, MAX_CONDITIONS};
use crate::validate::redact_url_userinfo;

/// The most entries a bounded list in a projection carries.
pub const MAX_LIST_ENTRIES: usize = 100;

fn name_ref(r: &LocalRef) -> NameRef {
    NameRef {
        name: r.name.clone(),
    }
}

fn archive_view(a: &ArchiveRef) -> ArchiveView {
    ArchiveView {
        url: redact_url_userinfo(&a.url),
        credential_ref: a.secret_ref.as_ref().map(name_ref),
    }
}

fn created_at<K: kube::Resource>(object: &K) -> Option<chrono::DateTime<chrono::Utc>> {
    object.meta().creation_timestamp.as_ref().map(|t| t.0)
}

/// A `KafkaCluster` as a connection.
#[must_use]
pub fn connection(cluster: &KafkaCluster) -> Connection {
    let status = cluster.status.as_ref();
    let reachable = status.and_then(|s| s.reachable);
    Connection {
        name: cluster.name_any(),
        namespace: cluster.namespace().unwrap_or_default(),
        uid: cluster.uid().unwrap_or_default(),
        resource_version: cluster.resource_version().unwrap_or_default(),
        created_at: created_at(cluster),
        role: cluster.spec.role.clone(),
        bootstrap_servers: cluster.spec.bootstrap_servers.clone(),
        auth: ConnectionAuthView {
            mode: match cluster.spec.auth.mode {
                AuthMode::Plaintext => ConnectionAuthMode::Plaintext,
                AuthMode::ScramSha512 => ConnectionAuthMode::ScramSha512,
            },
            username: cluster.spec.auth.username.clone(),
            // PLAT-07.1 gave the credential reference a `passwordKey`. The
            // projection stays NAME-ONLY: a data key is not a value, but which
            // key a Secret holds a password under is still a detail of the
            // reference, and widening this view is PLAT-17's decision to make
            // deliberately rather than a consequence of a rebase.
            credential_ref: cluster.spec.auth.secret_ref.as_ref().map(|r| NameRef {
                name: r.name.clone(),
            }),
            tls: cluster.spec.auth.tls,
        },
        marker_topic: cluster.spec.marker_topic.clone(),
        reachability: ReachabilityView {
            state: match reachable {
                Some(true) => ReachabilityState::Reachable,
                Some(false) => ReachabilityState::Unreachable,
                None => ReachabilityState::Unknown,
            },
            reason: status.and_then(|s| s.reason.clone()),
            cluster_id: status.and_then(|s| s.cluster_id.clone()),
            observed_at: status.and_then(|s| s.observed_at),
        },
    }
}

/// A `BackupSchedule` as a schedule.
#[must_use]
pub fn schedule(object: &BackupSchedule) -> Schedule {
    let status = object.status.as_ref();
    let report = status.and_then(|s| s.retention_report.as_ref()).map(|r| {
        let mut truncated = false;
        let mut cut = |len: usize| {
            if len > MAX_LIST_ENTRIES {
                truncated = true;
            }
            len.min(MAX_LIST_ENTRIES)
        };
        let kept = r.sets_kept.clone().unwrap_or_default();
        let kept_len = cut(kept.len());
        let removable = r.sets_that_would_be_removed.clone().unwrap_or_default();
        let removable_len = cut(removable.len());
        let aws = r.aws_cli.clone().unwrap_or_default();
        let aws_len = cut(aws.len());
        let mc = r.mc_cli.clone().unwrap_or_default();
        let mc_len = cut(mc.len());
        RetentionReportView {
            evaluated_at: r.evaluated_at,
            keep_last: r.keep_last,
            keep_days: r.keep_days,
            sets_kept: kept.into_iter().take(kept_len).collect(),
            sets_that_would_be_removed: removable
                .into_iter()
                .take(removable_len)
                .map(|s| RemovableSetView {
                    backup_id: s.backup_id,
                    newest_record_at: s.newest_record_at,
                    reason: s.reason,
                    days: s.days,
                    rank: s.rank,
                })
                .collect(),
            removal_commands: aws
                .into_iter()
                .take(aws_len)
                .map(|c| redact_url_userinfo_in(&c))
                .collect(),
            mc_removal_commands: mc
                .into_iter()
                .take(mc_len)
                .map(|c| redact_url_userinfo_in(&c))
                .collect(),
            skipped_manifests: r.skipped.as_ref().map_or(0, Vec::len),
            note: r.note.clone(),
            truncated,
        }
    });
    Schedule {
        name: object.name_any(),
        namespace: object.namespace().unwrap_or_default(),
        uid: object.uid().unwrap_or_default(),
        resource_version: object.resource_version().unwrap_or_default(),
        created_at: created_at(object),
        schedule: object.spec.schedule.clone(),
        source_ref: name_ref(&object.spec.source_ref),
        topics: object.spec.topics.clone(),
        archive: archive_view(&object.spec.archive),
        concurrency_policy: match object.spec.concurrency_policy {
            CrdConcurrency::Forbid => ConcurrencyPolicy::Forbid,
            CrdConcurrency::Allow => ConcurrencyPolicy::Allow,
        },
        retention: object.spec.retention.as_ref().map(|r| RetentionView {
            keep_last: r.keep_last,
            keep_days: r.keep_days,
        }),
        suspended: object.spec.suspend,
        status: ScheduleStatusView {
            last_fire_time: status.and_then(|s| s.last_fire_time),
            next_fire_time: status.and_then(|s| s.next_fire_time),
            active_backup: status
                .and_then(|s| s.active_backup_ref.as_ref())
                .map(|r| r.name.clone()),
            pending_backup: status
                .and_then(|s| s.pending_backup_ref.as_ref())
                .map(|r| r.name.clone()),
            last_missed_slot: status.and_then(|s| s.last_missed_slot.clone()),
            ready: status
                .and_then(|s| s.conditions.as_ref())
                .and_then(|cs| cs.iter().find(|c| c.r#type == "Ready"))
                .map(condition_view),
            retention_report: report,
        },
    }
}

fn redact_url_userinfo_in(command: &str) -> String {
    command
        .split(' ')
        .map(|word| {
            let quoted = word.trim_matches('\'');
            if quoted.contains("://") && quoted.contains('@') {
                word.replace(quoted, &redact_url_userinfo(quoted))
            } else {
                word.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// A `Backup`.
#[must_use]
pub fn backup(object: &BackupCr) -> Backup {
    let status = object.status.as_ref();
    let operation = backup_operation(object);
    Backup {
        name: object.name_any(),
        namespace: object.namespace().unwrap_or_default(),
        uid: object.uid().unwrap_or_default(),
        resource_version: object.resource_version().unwrap_or_default(),
        created_at: created_at(object),
        source_ref: name_ref(&object.spec.source_ref),
        topics: object.spec.topics.clone(),
        archive: archive_view(&object.spec.archive),
        schedule: object.spec.schedule_ref.as_ref().map(|r| r.name.clone()),
        slot: object.spec.slot.clone(),
        triggered_by: object.spec.triggered_by.clone(),
        deadline_seconds: object.spec.deadline_seconds,
        backup_id: status.and_then(|s| s.backup_id.clone()),
        records: status.and_then(|s| s.records),
        window_covered: status
            .and_then(|s| s.window_covered)
            .map(|w| WindowCoveredView {
                from_ms: w.from_ms,
                to_ms: w.to_ms,
            }),
        manifest_key: status.and_then(|s| s.manifest_key.clone()),
        observed_auth: status
            .and_then(|s| s.auth.as_ref())
            .map(|a| ObservedAuthView {
                mode: a.mode.clone(),
                username: a.username.clone(),
            }),
        operation: summary(&operation),
    }
}

/// A `Restore`. `with_plan_bytes` is true on the single-object read only.
#[must_use]
pub fn restore(object: &RestoreCr, with_plan_bytes: bool) -> Restore {
    let status = object.status.as_ref();
    let operation = restore_operation(object);
    Restore {
        name: object.name_any(),
        namespace: object.namespace().unwrap_or_default(),
        uid: object.uid().unwrap_or_default(),
        resource_version: object.resource_version().unwrap_or_default(),
        created_at: created_at(object),
        plan_hash: logweir_core::ids::sha256_prefixed(object.spec.plan_bytes.as_bytes()),
        plan_bytes_length: object.spec.plan_bytes.len(),
        plan_bytes: with_plan_bytes.then(|| object.spec.plan_bytes.clone()),
        approval_ref: name_ref(&object.spec.approval_ref),
        source_archive: archive_view(&object.spec.source_archive),
        backup_set_ref: object.spec.backup_set_ref.clone(),
        point_in_time: object.spec.point_in_time,
        target: RestoreTargetView {
            cluster_ref: name_ref(&object.spec.target.cluster_ref),
            mode: match object.spec.target.mode {
                TargetMode::Scratch => RestoreMode::Scratch,
                TargetMode::NewTopic => RestoreMode::NewTopic,
            },
            topic_prefix: object.spec.target.topic_naming.prefix.clone(),
        },
        deadline_seconds: object.spec.deadline_seconds,
        new_topics: status
            .and_then(|s| s.new_topics.clone())
            .unwrap_or_default()
            .into_iter()
            .take(MAX_LIST_ENTRIES)
            .collect(),
        operation: summary(&operation),
    }
}

fn subject_ref(object: &ApprovalCr) -> SubjectRefView {
    SubjectRefView {
        kind: object.spec.subject_ref.kind.as_str().to_string(),
        name: object.spec.subject_ref.name.clone(),
    }
}

/// An `Approval`'s public metadata. No document bytes.
#[must_use]
pub fn approval(object: &ApprovalCr) -> Approval {
    let status = object.status.as_ref();
    Approval {
        name: object.name_any(),
        namespace: object.namespace().unwrap_or_default(),
        uid: object.uid().unwrap_or_default(),
        resource_version: object.resource_version().unwrap_or_default(),
        created_at: created_at(object),
        subject_ref: subject_ref(object),
        plan_hash: object.spec.plan_hash.clone(),
        approval_bytes_length: object.spec.approval_bytes.len(),
        sidecar_bytes_length: object.spec.sidecar_bytes.len(),
        verified: status.and_then(|s| s.verified),
        matched_key_id: status.and_then(|s| s.matched_key_id.clone()),
        approver: status.and_then(|s| s.approver.clone()),
        ticket: status.and_then(|s| s.ticket.clone()),
        self_attested_risk: status.and_then(|s| s.self_attested_risk),
        verified_subject: status
            .and_then(|s| s.verified_subject_ref.as_ref())
            .map(|v| VerifiedSubjectView {
                kind: v.kind.as_str().to_string(),
                name: v.name.clone(),
                namespace: v.namespace.clone(),
                uid: v.uid.clone(),
            }),
        conditions: status
            .and_then(|s| s.conditions.as_ref())
            .map(|cs| cs.iter().take(MAX_CONDITIONS).map(condition_view).collect())
            .unwrap_or_default(),
    }
}

/// An `Approval`'s raw documents, verbatim.
#[must_use]
pub fn approval_packet(object: &ApprovalCr) -> ApprovalPacket {
    ApprovalPacket {
        name: object.name_any(),
        namespace: object.namespace().unwrap_or_default(),
        uid: object.uid().unwrap_or_default(),
        subject_ref: subject_ref(object),
        plan_hash: object.spec.plan_hash.clone(),
        approval_bytes: object.spec.approval_bytes.clone(),
        sidecar_bytes: object.spec.sidecar_bytes.clone(),
    }
}
