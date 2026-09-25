//! Stored objects to product DTOs. Pure functions, no I/O.
//!
//! WHAT A PROJECTION DROPS: every annotation and label (including this
//! service's own idempotency annotations), managed fields, owner references,
//! finalizers, the approval and sidecar documents outside the packet route,
//! and URL userinfo. What it keeps is named in `crate::contract`.

use kube::ResourceExt;
use weirkeeper::crds::approval::Approval as ApprovalCr;
use weirkeeper::crds::backup::{Backup as BackupCr, TriggerKind as CrdTriggerKind};
use weirkeeper::crds::backup_schedule::{
    BackupSchedule, CatchUpPolicy as CrdCatchUp, ConcurrencyPolicy as CrdConcurrency,
};
use weirkeeper::crds::kafka_cluster::{AuthMode, KafkaCluster};
use weirkeeper::crds::restore::{Restore as RestoreCr, TargetMode};
use weirkeeper::crds::selection::IncompleteDiscovery as CrdIncompleteDiscovery;
use weirkeeper::crds::{ArchiveRef, LocalRef};

use crate::contract::{
    ActiveRunView, AllUserTopics, Approval, ApprovalPacket, ArchiveView, Backup,
    BackupDestinationRefView, CadenceAdjustment, CadencePreset, CatchUpPolicy, ConcurrencyPolicy,
    Connection, ConnectionAuthMode, ConnectionAuthView, IncompleteDiscoveryPolicy, LastSlotView,
    LastTestView, MissedSlotView, MissedSlotsView, NameRef, NextRunView, ObservedAuthView,
    ReachabilityState, ReachabilityView, RemovableSetView, Restore, RestoreMode, RestoreTargetView,
    RetentionReportView, RetentionView, RetryPolicy, Schedule, SchedulePolicyView, ScheduleRefView,
    ScheduleStatusView, SubjectRefView, TopicExclusions, TriggerKind, TriggerView,
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
pub fn connection(cluster: &KafkaCluster, last_test: Option<LastTestView>) -> Connection {
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
        last_test,
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
        generation: object.metadata.generation,
        created_at: created_at(object),
        schedule: object.spec.schedule.clone(),
        preset: weirkeeper::cadence::presets::match_preset(&object.spec.schedule).map(preset_view),
        time_zone: object.spec.time_zone.clone(),
        source_ref: name_ref(&object.spec.source_ref),
        topics: object.spec.topics.clone(),
        all_user_topics: object.spec.all_user_topics.as_ref().map(all_user_topics),
        archive: archive_view(&object.spec.archive),
        destination_ref: object.spec.destination_ref.as_ref().map(name_ref),
        concurrency_policy: match object.spec.concurrency_policy {
            CrdConcurrency::Forbid => ConcurrencyPolicy::Forbid,
            CrdConcurrency::Allow => ConcurrencyPolicy::Allow,
        },
        starting_deadline_seconds: object.spec.starting_deadline_seconds,
        catch_up_policy: object.spec.catch_up_policy.map(|c| match c {
            CrdCatchUp::None => CatchUpPolicy::None,
            CrdCatchUp::Latest => CatchUpPolicy::Latest,
        }),
        retry: object.spec.retry.as_ref().map(|r| RetryPolicy {
            max_retries: r.max_retries,
            delay_seconds: r.delay_seconds,
        }),
        active_deadline_seconds: object.spec.active_deadline_seconds,
        retention: object.spec.retention.as_ref().map(|r| RetentionView {
            keep_last: r.keep_last,
            keep_days: r.keep_days,
        }),
        suspended: object.spec.suspend,
        status: ScheduleStatusView {
            observed_generation: status.and_then(|s| s.observed_generation),
            policy: status
                .and_then(|s| s.policy.as_ref())
                .map(|p| SchedulePolicyView {
                    generation: p.generation,
                    run_policy_sha256: p.run_policy_sha256.clone(),
                    time_zone: p.time_zone.clone(),
                    tzdb: p.tzdb.clone(),
                    effective_since: p.effective_since,
                    evaluated_at: p.evaluated_at,
                }),
            next_runs: status.and_then(|s| s.next_runs.as_ref()).map(|runs| {
                runs.iter()
                    .take(MAX_LIST_ENTRIES)
                    .map(|r| NextRunView {
                        at: r.at,
                        local_time: r.local_time.clone(),
                        // An adjustment word this build does not know is
                        // DROPPED, not echoed: an unrecognised marker is not a
                        // fact about a time zone.
                        adjustment: r.adjustment.as_deref().and_then(CadenceAdjustment::parse),
                    })
                    .collect()
            }),
            active_runs: status
                .and_then(|s| s.active_runs.as_ref())
                .map(|runs| runs.iter().take(MAX_LIST_ENTRIES).map(active_run).collect()),
            last_fire_time: status.and_then(|s| s.last_fire_time),
            next_fire_time: status.and_then(|s| s.next_fire_time),
            active_backup: status
                .and_then(|s| s.active_backup_ref.as_ref())
                .map(|r| r.name.clone()),
            pending_backup: status
                .and_then(|s| s.pending_backup_ref.as_ref())
                .map(|r| r.name.clone()),
            last_missed_slot: status.and_then(|s| s.last_missed_slot.clone()),
            last_slot: status
                .and_then(|s| s.last_slot.as_ref())
                .map(|l| LastSlotView {
                    slot: l.slot.clone(),
                    due_at: l.due_at,
                    attempt: l.attempt,
                    disposition: l.disposition.clone(),
                    backup_ref: l.backup_ref.as_ref().map(name_ref),
                    reason: l.reason.clone(),
                    decided_at: l.decided_at,
                }),
            missed_slots: status
                .and_then(|s| s.missed_slots.as_ref())
                .map(|m| MissedSlotsView {
                    count: m.count,
                    count_capped: m.count_capped,
                    last_evaluated_slot: m.last_evaluated_slot.clone(),
                    recent: m.recent.as_ref().map(|recent| {
                        recent
                            .iter()
                            .take(MAX_LIST_ENTRIES)
                            .map(|r| MissedSlotView {
                                slot: r.slot.clone(),
                                reason: r.reason.clone(),
                                recorded_at: r.recorded_at,
                            })
                            .collect()
                    }),
                }),
            ready: status
                .and_then(|s| s.conditions.as_ref())
                .and_then(|cs| cs.iter().find(|c| c.r#type == "Ready"))
                .map(condition_view),
            retention_report: report,
        },
    }
}

/// One non-terminal schedule-created run.
#[must_use]
pub fn active_run(run: &weirkeeper::crds::backup_schedule::ActiveRun) -> ActiveRunView {
    ActiveRunView {
        name: run.name.clone(),
        kind: run.kind.clone(),
        attempt: run.attempt,
    }
}

/// The cadence engine's preset as the contract spells it. The two enums are
/// the same five shapes; `contract` carries the `JsonSchema` derive the
/// OpenAPI document needs, and `tests/cadence_previews.rs` asserts the JSON is
/// byte-identical so they cannot drift.
#[must_use]
pub fn preset_view(preset: weirkeeper::cadence::presets::Preset) -> CadencePreset {
    use weirkeeper::cadence::presets::Preset;
    match preset {
        Preset::Hourly { minute } => CadencePreset::Hourly { minute },
        Preset::EveryNHours { n, minute } => CadencePreset::EveryNHours { n, minute },
        Preset::Daily { hour, minute } => CadencePreset::Daily { hour, minute },
        Preset::Weekly {
            day_of_week,
            hour,
            minute,
        } => CadencePreset::Weekly {
            day_of_week,
            hour,
            minute,
        },
        Preset::Monthly {
            day_of_month,
            hour,
            minute,
        } => CadencePreset::Monthly {
            day_of_month,
            hour,
            minute,
        },
    }
}

/// Dynamic selection as the contract spells it.
#[must_use]
pub fn all_user_topics(spec: &weirkeeper::crds::selection::AllUserTopics) -> AllUserTopics {
    AllUserTopics {
        exclude: spec.exclude.as_ref().map(|e| TopicExclusions {
            topics: e.topics.clone(),
            prefixes: e.prefixes.clone(),
        }),
        incomplete_discovery: match spec.incomplete_discovery {
            CrdIncompleteDiscovery::Refuse => IncompleteDiscoveryPolicy::Refuse,
            CrdIncompleteDiscovery::BackUpVisibleTopics => {
                IncompleteDiscoveryPolicy::BackUpVisibleTopics
            }
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

/// WHERE A RECOVERY POINT IS, published from the two fields that record it and
/// from nothing else (BACKUP-PROJECTION-NO-DESTINATION).
///
/// `spec.destinationRef` is what the run ASKED for and `status.destination` is
/// what the controller FROZE. Both are published, and neither is invented from
/// the other:
///
/// * a run with neither is a legacy inline-archive run — `None`, and the
///   console reads `archive` as it always did;
/// * a run with `spec.destinationRef` whose freeze has not happened yet (or
///   whose controller predates the block) publishes the NAME with no `uid` and
///   no `locationDigest`: it names a destination and has no frozen location;
/// * a run with both publishes the name, the frozen uid and the frozen digest.
///
/// THE UID IS ONLY PUBLISHED FOR THE NAME IT BELONGS TO. The freeze writes the
/// same name the spec asks for, so the two agree in every object this product
/// writes; if they ever disagree the spec's name is published WITHOUT the
/// frozen uid, because a uid attached to another object's name is worse than no
/// uid at all.
///
/// A `status.destination` with no `spec.destinationRef` is not a shape this
/// controller writes, but it is published rather than dropped: the freeze is
/// the record of where the archive went, and dropping it would hide a location
/// the object states.
fn backup_destination(object: &BackupCr) -> (Option<BackupDestinationRefView>, Option<String>) {
    let frozen = object.status.as_ref().and_then(|s| s.destination.as_ref());
    let name = object
        .spec
        .destination_ref
        .as_ref()
        .map(|r| r.name.clone())
        .or_else(|| frozen.map(|d| d.name.clone()));
    let Some(name) = name else {
        return (None, None);
    };
    let frozen = frozen.filter(|d| d.name == name);
    (
        Some(BackupDestinationRefView {
            name,
            uid: frozen.map(|d| d.uid.clone()),
        }),
        frozen.map(|d| d.location_digest.clone()),
    )
}

/// A `Backup`.
#[must_use]
pub fn backup(object: &BackupCr) -> Backup {
    let status = object.status.as_ref();
    let operation = backup_operation(object);
    let (destination_ref, location_digest) = backup_destination(object);
    Backup {
        name: object.name_any(),
        namespace: object.namespace().unwrap_or_default(),
        uid: object.uid().unwrap_or_default(),
        resource_version: object.resource_version().unwrap_or_default(),
        created_at: created_at(object),
        source_ref: name_ref(&object.spec.source_ref),
        topics: object.spec.topics.clone(),
        archive: archive_view(&object.spec.archive),
        destination_ref,
        location_digest,
        schedule: object.spec.schedule_ref.as_ref().map(|r| r.name.clone()),
        slot: object.spec.slot.clone(),
        triggered_by: object.spec.triggered_by.clone(),
        trigger: object.spec.trigger.as_ref().map(|t| TriggerView {
            kind: match t.kind {
                CrdTriggerKind::Scheduled => TriggerKind::Scheduled,
                CrdTriggerKind::CatchUp => TriggerKind::CatchUp,
                CrdTriggerKind::Retry => TriggerKind::Retry,
                CrdTriggerKind::Manual => TriggerKind::Manual,
            },
            attempt: t.attempt,
            retry_of: t.retry_of.as_ref().map(name_ref),
            time_zone: t.time_zone.clone(),
        }),
        schedule_ref: object.spec.schedule_ref.as_ref().map(|r| ScheduleRefView {
            name: r.name.clone(),
            uid: r.uid.clone(),
            generation: r.generation,
            run_policy_sha256: r.run_policy_sha256.clone(),
        }),
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
        queue: queue_view(operation.state, status.and_then(|s| s.queue.as_ref())),
        operation: summary(&operation),
    }
}

/// P10's `queue` block, published ONLY while the run is queued: a block the
/// controller has not cleared yet beside a run that already left the queue is
/// not a statement about that run any more.
fn queue_view(
    state: crate::contract::OperationState,
    queue: Option<&weirkeeper::crds::RunQueue>,
) -> Option<crate::contract::RunQueueView> {
    if state != crate::contract::OperationState::Queued {
        return None;
    }
    queue.map(|q| crate::contract::RunQueueView {
        limit: q.limit,
        authorization_expires_at: q.authorization_expires_at,
    })
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
        // A standing-authorised Restore names no per-run Approval; the DTO
        // keeps the field required and carries the empty name, which is what
        // every consumer of this view already treats as "no approval".
        approval_ref: NameRef {
            name: object.spec.approval_ref_name().to_string(),
        },
        source_archive: archive_view(&object.spec.source_archive),
        source_destination_ref: object.spec.source_destination_ref.as_ref().map(name_ref),
        evidence_destination_ref: object.spec.evidence_destination_ref.as_ref().map(name_ref),
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
        queue: queue_view(operation.state, status.and_then(|s| s.queue.as_ref())),
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
        authorization: status.and_then(|s| s.authorization.as_ref()).map(|a| {
            crate::contract::ApprovalProvenanceView {
                mode: a.mode.clone(),
                policy_name: a.policy_name.clone(),
                policy_digest: a.policy_digest.clone(),
                requester: a.requester.clone(),
                confirmation_key_id: a.confirmation_key_id.clone(),
            }
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
