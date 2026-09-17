//! `restorePreflight` — D2 §6.7's archive and target checks.
//!
//! # The plan bytes are hashed BEFORE anything else
//!
//! D2 §4.2's startup order already proved the CHECK PLAN's digest. This kind
//! carries a second document — the verbatim restore spec at
//! `/check/plan.yaml` — and `RestorePreflightRequest::plan_sha256` pins it.
//! Those bytes are read and hashed before a socket is opened, because every
//! check below is a claim ABOUT them: the topics, the recovery point, the
//! mapped names and the replication factor all come from that document, and a
//! preflight run against different bytes would be a green preview of a plan
//! nobody approved. A mismatch is `plan.parse notReady PlanHashMismatch` and
//! the check stops: no broker is dialled and no bucket is read.
//!
//! # No writes, and no topics
//!
//! D2 §6.7: "No topic is created, altered or deleted by preflight." The
//! collision answer is built from two read-only probes — targeted metadata per
//! mapped name, and a `CreateTopics` with `validate_only = true` — and never
//! from creating a probe topic, which is what the execution path's
//! `LogAppendTime` guard does and what G12 forbids here.
//!
//! # Which rows are the runner's
//!
//! `plan.parse`, `archive.*`, `target.authenticated`, `target.scratchMarker`,
//! `target.mappedTopics`, `target.topicCreate`, `target.timestampBound` and
//! `target.logAppendTime`. `plan.bindings`, `plan.names`,
//! `recoveryPoint.state`, `approval.*`, `target.clusterIdentity` and
//! `signer.rostered` are the controller's: each needs a Kubernetes object a
//! credential-holding Job is not given read on.

use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use logweir_core::check_contract::{
    CheckCode, CheckId, CheckOutcome, CheckPlanKind, CheckResult, CheckState, Gating,
    RestorePreflightRequest, Stream,
};
use logweir_core::destination::DestinationRole;
use logweir_core::spec::{target_topic_prefix, DrillSpec, TargetMode};
use logweir_kafka::inventory::{InventoryProbe, TopicPresence};
use logweir_kafka::reader::{NewTopicSpec, TARGET_TOPIC_CONFIGS};

use super::readiness::{authenticated, detail, DETAIL_SAMPLE};
use super::{
    execution_only, from_broker_failure, from_store_failure, ready, remedy_for, runner_contract,
    state_for, Wiring,
};
use crate::check::archive::{self, ManifestError};
use crate::check::store::{self, ObjectAccess};
use crate::check::{catalogue, Deadline, Emission};

/// `log.message.timestamp.type` — the broker key `target.logAppendTime` is
/// about.
///
/// The three constants below are the ones
/// `crate::drill::phase0_admit` reads at execution. They are private there, so
/// they are re-declared here and
/// `the_broker_config_keys_match_the_execution_guard` in
/// `crates/logweir/tests/check_cli.rs` reads that file and asserts every
/// literal still appears in it — a preflight that checked a key the run does
/// not read would be a green preview of a bound nobody enforces.
pub const BROKER_TIMESTAMP_TYPE: &str = "log.message.timestamp.type";
/// `log.message.timestamp.before.max.ms` — Kafka >= 3.6.
pub const BROKER_TIMESTAMP_BEFORE_MAX_MS: &str = "log.message.timestamp.before.max.ms";
/// `log.message.timestamp.difference.max.ms` — the pre-3.6 spelling.
pub const BROKER_TIMESTAMP_DIFFERENCE_MAX_MS: &str = "log.message.timestamp.difference.max.ms";
/// The timestamp type that overwrites every restored record's timestamp.
pub const LOG_APPEND_TIME: &str = "LogAppendTime";

/// How many segment keys a preflight will list before it gives up.
///
/// D2 §6.3's `SegmentListTooLarge` threshold. Above it the row is `unknown`
/// and says so: a preflight that held a million keys in memory to answer a
/// preview would be a preflight nobody could afford to run.
pub const SEGMENT_LIST_LIMIT: usize = 200_000;

/// The `details` stream's cap — D2 §6.7's "≤ 768 KiB, truncated with count".
pub const DETAILS_MAX_BYTES: usize = 768 * 1024;

/// Run one `restorePreflight`.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn run(req: &RestorePreflightRequest, wiring: &dyn Wiring, deadline: Deadline) -> Emission {
    let now = wiring.now();
    let wanted: BTreeSet<CheckId> = req.checks.iter().copied().collect();
    let skipped: BTreeSet<CheckId> = req.skip_checks.iter().copied().collect();
    let want = |id: CheckId| (wanted.is_empty() || wanted.contains(&id)) && !skipped.contains(&id);

    let mut checks: Vec<CheckOutcome> = Vec::new();
    let mut details: Vec<String> = Vec::new();
    checks.push(runner_contract(now));

    // 1 — THE PLAN BYTES, BEFORE ANY SOCKET.
    let spec = match plan_spec(req, wiring) {
        Ok(spec) => {
            if want(CheckId::PlanParse) {
                checks.push(
                    ready(CheckId::PlanParse, CheckCode::PlanParsed, now).with_message(
                        "the mounted restore plan hashes to the digest this check was pinned to \
                         and parses as a Logweir restore spec",
                    ),
                );
            }
            spec
        }
        Err((code, message)) => {
            checks.push(
                catalogue::outcome(CheckId::PlanParse, CheckState::NotReady, code, now)
                    .with_message(&message)
                    .with_remedy(remedy_for(code)),
            );
            // EVERY LATER CHECK IS A CLAIM ABOUT THESE BYTES, so none of them
            // runs. Nothing was dialled and nothing was read.
            let mut result = CheckResult::new(CheckPlanKind::RestorePreflight);
            result.checks = checks;
            return Emission::of(result);
        }
    };

    // 2 — the archive, through the source destination's `archiveRead` grant.
    archive_checks(
        req,
        &spec,
        wiring,
        deadline,
        now,
        &want,
        &mut checks,
        &mut details,
    );

    // 3 — the target broker.
    target_checks(
        req,
        &spec,
        wiring,
        deadline,
        now,
        &want,
        &mut checks,
        &mut details,
    );

    // 4 — the evidence destination.
    //
    // D2 §6.3 lists `destination.evidenceWritable` for a Restore, but
    // `RestorePreflightRequest` (the landed W1 contract) carries no
    // `writeProbe` flag for it, and a check may not write unasked. The row is
    // therefore execution-only with `WriteNotProbed`, which is the same answer
    // a Backup readiness plan gives when its destination configures no probe.
    if let Some(evidence) = req.evidence_destination.as_ref() {
        if want(CheckId::DestinationEvidenceWritable) {
            checks.push(
                catalogue::outcome_gated(
                    CheckId::DestinationEvidenceWritable,
                    CheckState::Unknown,
                    CheckCode::WriteNotProbed,
                    Gating::ExecutionOnly,
                    now,
                )
                .with_message(
                    "a restore preflight plan carries no create-only write probe, so the \
                     evidence-write grant is verified when the restore executes",
                )
                .with_remedy(remedy_for(CheckCode::WriteNotProbed))
                .with_scope(catalogue::scope(
                    "BackupDestination",
                    &evidence.name,
                    Some(&evidence.uid),
                )),
            );
        }
    }

    let mut result = CheckResult::new(CheckPlanKind::RestorePreflight);
    result.checks = checks;
    let mut emission = Emission::of(result);
    if !details.is_empty() {
        emission
            .extra
            .push((Stream::Details, details_stream(&details)));
    }
    emission
}

/// The verbatim plan bytes, hashed and parsed.
///
/// # Errors
/// `(code, message)` for the `plan.parse` row.
fn plan_spec(
    req: &RestorePreflightRequest,
    wiring: &dyn Wiring,
) -> Result<DrillSpec, (CheckCode, String)> {
    let bytes = wiring.read_bytes(&req.plan_file).map_err(|e| {
        (
            CheckCode::PlanUnparseable,
            format!(
                "the restore plan at `{}` could not be read ({})",
                req.plan_file,
                e.kind()
            ),
        )
    })?;
    let got = logweir_core::ids::sha256_prefixed(&bytes);
    if got != req.plan_sha256 {
        return Err((
            CheckCode::PlanHashMismatch,
            format!(
                "the restore plan at `{}` hashes to {got}, not to the {} this check is bound \
                 to; no broker was dialled and no archive was read",
                req.plan_file, req.plan_sha256
            ),
        ));
    }
    serde_yaml::from_slice::<DrillSpec>(&bytes).map_err(|_| {
        (
            CheckCode::PlanUnparseable,
            format!(
                "the restore plan at `{}` hashes correctly but is not a Logweir restore spec",
                req.plan_file
            ),
        )
    })
}

/// The recovery point this plan asks for: `restore.point_in_time` when the
/// spec states one, else `sample.window_end`.
///
/// The SAME pairing `crate::drill::build_plan_with_floor` makes for
/// `time_window.1` and `phase0_admit::target_topic_preflight` makes for the
/// broker's timestamp bound. Reading `sample.window_end` unconditionally would
/// check a bound against an instant this restore never requests.
#[must_use]
pub fn recovery_point(spec: &DrillSpec) -> DateTime<Utc> {
    spec.restore.point_in_time.unwrap_or(spec.sample.window_end)
}

#[allow(clippy::too_many_arguments)]
fn archive_checks(
    req: &RestorePreflightRequest,
    spec: &DrillSpec,
    wiring: &dyn Wiring,
    deadline: Deadline,
    now: DateTime<Utc>,
    want: &dyn Fn(CheckId) -> bool,
    checks: &mut Vec<CheckOutcome>,
    details: &mut Vec<String>,
) {
    let dest = &req.source_destination;
    let scope = catalogue::scope("BackupDestination", &dest.name, Some(&dest.uid));
    let budget = deadline.slice(2);
    let access = match wiring.objects(dest, DestinationRole::ArchiveRead, budget) {
        Ok(a) => a,
        Err(f) => {
            if want(CheckId::ArchiveBackupSet) {
                checks
                    .push(from_store_failure(CheckId::ArchiveBackupSet, &f, now).with_scope(scope));
            }
            block_rest(
                &[CheckId::ArchiveCoverage, CheckId::ArchiveSegments],
                want,
                now,
                checks,
            );
            return;
        }
    };

    let bytes = match access.get(&req.manifest_key) {
        Ok(b) => b,
        Err(e) => {
            let class = store::classify(&e);
            // A manifest that is NOT THERE is the backup set not being there,
            // which is the fact an operator acts on; every other refusal keeps
            // its store code so the remedy points at the bucket policy rather
            // than at the recovery point.
            let code = if class == CheckCode::ObjectNotFound {
                CheckCode::BackupSetNotFound
            } else {
                class
            };
            if want(CheckId::ArchiveBackupSet) {
                checks.push(
                    catalogue::outcome(CheckId::ArchiveBackupSet, state_for(code), code, now)
                        .with_message(&format!(
                            "the backup manifest `{}` on destination `{}` could not be read: \
                             {class}",
                            req.manifest_key, dest.name
                        ))
                        .with_remedy(remedy_for(code))
                        .with_scope(scope),
                );
            }
            block_rest(
                &[CheckId::ArchiveCoverage, CheckId::ArchiveSegments],
                want,
                now,
                checks,
            );
            return;
        }
    };

    let manifest = match archive::parse(&bytes) {
        Ok(v) => v,
        Err(ManifestError::NotAManifest | ManifestError::NoSegments) => {
            if want(CheckId::ArchiveBackupSet) {
                checks.push(
                    catalogue::outcome(
                        CheckId::ArchiveBackupSet,
                        CheckState::NotReady,
                        CheckCode::ManifestUnreadable,
                        now,
                    )
                    .with_message(&format!(
                        "the object at `{}` is not a backup manifest",
                        req.manifest_key
                    ))
                    .with_remedy(remedy_for(CheckCode::ManifestUnreadable))
                    .with_scope(scope),
                );
            }
            block_rest(
                &[CheckId::ArchiveCoverage, CheckId::ArchiveSegments],
                want,
                now,
                checks,
            );
            return;
        }
    };

    if want(CheckId::ArchiveBackupSet) {
        checks.push(
            ready(CheckId::ArchiveBackupSet, CheckCode::ManifestReadable, now)
                .with_message(&format!(
                    "the backup manifest for set `{}` is readable",
                    req.backup_id
                ))
                .with_scope(scope.clone()),
        );
    }

    // ---- coverage ------------------------------------------------------
    let window = archive::window(&manifest);
    if want(CheckId::ArchiveCoverage) {
        checks.push(coverage_row(req, spec, &manifest, window, now));
    }

    // ---- segments ------------------------------------------------------
    if want(CheckId::ArchiveSegments) {
        checks.push(segments_row(
            req,
            spec,
            &manifest,
            window,
            access.as_ref(),
            now,
            details,
        ));
    }
}

/// `archive.coverage` — the point in time against the set's window, and the
/// selected topics against the set's topic list.
fn coverage_row(
    req: &RestorePreflightRequest,
    spec: &DrillSpec,
    manifest: &serde_json::Value,
    window: Result<archive::ManifestWindow, ManifestError>,
    now: DateTime<Utc>,
) -> CheckOutcome {
    let missing: Vec<String> = {
        let have = archive::topics(manifest);
        spec.source
            .topics
            .iter()
            .filter(|t| !have.contains(t.as_str()))
            .cloned()
            .collect()
    };
    if !missing.is_empty() {
        return catalogue::outcome(
            CheckId::ArchiveCoverage,
            CheckState::NotReady,
            CheckCode::TopicNotInBackupSet,
            now,
        )
        .with_message(&format!(
            "{} of {} selected topic(s) are not in backup set `{}`",
            missing.len(),
            spec.source.topics.len(),
            req.backup_id
        ))
        .with_remedy(remedy_for(CheckCode::TopicNotInBackupSet))
        .with_detail(detail(&missing));
    }
    let Ok(w) = window else {
        // Manifest-shaped and naming no segment: it bounds no window, so no
        // point in time is covered by it.
        return catalogue::outcome(
            CheckId::ArchiveCoverage,
            CheckState::NotReady,
            CheckCode::PointInTimeAfterCoverage,
            now,
        )
        .with_message(&format!(
            "backup set `{}` declares no segment, so it covers no instant",
            req.backup_id
        ))
        .with_remedy(remedy_for(CheckCode::PointInTimeAfterCoverage));
    };
    let pit = recovery_point(spec).timestamp_millis();
    // `<=`, NOT `<`, and the same rule as the execution guard: the restore
    // window is `[archive floor, recovery point]`, so a recovery point AT the
    // floor describes one instant and restores whatever shares that exact
    // millisecond — almost always nothing. An empty restore reported as a pass
    // is guard G-WIN's silent loss.
    if pit <= w.oldest_ms {
        return catalogue::outcome(
            CheckId::ArchiveCoverage,
            CheckState::NotReady,
            CheckCode::PointInTimeBeforeCoverage,
            now,
        )
        .with_message(&format!(
            "the requested recovery point is epoch-ms {pit}, at or before backup set `{}`'s \
             earliest covered timestamp of epoch-ms {}, so the restore window holds no instant",
            req.backup_id, w.oldest_ms
        ))
        .with_remedy(remedy_for(CheckCode::PointInTimeBeforeCoverage));
    }
    if pit > w.newest_ms {
        return catalogue::outcome(
            CheckId::ArchiveCoverage,
            CheckState::NotReady,
            CheckCode::PointInTimeAfterCoverage,
            now,
        )
        .with_message(&format!(
            "the requested recovery point is epoch-ms {pit}, after backup set `{}`'s newest \
             covered timestamp of epoch-ms {}",
            req.backup_id, w.newest_ms
        ))
        .with_remedy(remedy_for(CheckCode::PointInTimeAfterCoverage));
    }
    ready(CheckId::ArchiveCoverage, CheckCode::PointInTimeCovered, now).with_message(&format!(
        "backup set `{}` covers epoch-ms {} to {}, which includes the requested recovery point \
         epoch-ms {pit}",
        req.backup_id, w.oldest_ms, w.newest_ms
    ))
}

/// `archive.segments` — every segment the manifest names for the restore
/// window is present under the set's prefix.
#[allow(clippy::too_many_arguments)]
fn segments_row(
    req: &RestorePreflightRequest,
    spec: &DrillSpec,
    manifest: &serde_json::Value,
    window: Result<archive::ManifestWindow, ManifestError>,
    access: &dyn ObjectAccess,
    now: DateTime<Utc>,
    details: &mut Vec<String>,
) -> CheckOutcome {
    let Ok(w) = window else {
        return ready(CheckId::ArchiveSegments, CheckCode::SegmentsPresent, now)
            .with_message("the backup set names no segment, so none can be missing");
    };
    // The window the restore will actually ask for: the archive's own floor to
    // the recovery point (guard G-WIN — the start is the archive's, never the
    // spec's).
    let want = (w.oldest_ms, recovery_point(spec).timestamp_millis());
    let expected = archive::segment_keys_for_topics(manifest, &spec.source.topics, want, &|k| {
        access.qualify(k)
    });
    // `<prefix>/<backupId>` — the manifest key's own directory, so the listing
    // and the manifest cannot disagree about which set is being checked.
    let set_prefix = req
        .manifest_key
        .trim_end_matches("/manifest.json")
        .to_string();
    let listed = match access.list_bounded(&set_prefix, SEGMENT_LIST_LIMIT + 1) {
        Ok(k) => k,
        Err(e) => {
            let code = store::classify(&e);
            return catalogue::outcome(CheckId::ArchiveSegments, state_for(code), code, now)
                .with_message(&format!(
                    "listing backup set `{set_prefix}` was refused: {code}"
                ))
                .with_remedy(remedy_for(code));
        }
    };
    if listed.len() > SEGMENT_LIST_LIMIT {
        return catalogue::outcome(
            CheckId::ArchiveSegments,
            CheckState::Unknown,
            CheckCode::SegmentListTooLarge,
            now,
        )
        .with_message(&format!(
            "backup set `{}` holds more than {SEGMENT_LIST_LIMIT} keys, which a preflight does \
             not list",
            req.backup_id
        ))
        .with_remedy(remedy_for(CheckCode::SegmentListTooLarge));
    }
    let present: BTreeSet<&str> = listed.iter().map(String::as_str).collect();
    let missing: Vec<String> = expected
        .iter()
        .filter(|k| !present.contains(k.as_str()))
        .cloned()
        .collect();
    if missing.is_empty() {
        return ready(CheckId::ArchiveSegments, CheckCode::SegmentsPresent, now).with_message(
            &format!(
                "all {} segment(s) backup set `{}` names for this window are present",
                expected.len(),
                req.backup_id
            ),
        );
    }
    for key in &missing {
        details.push(
            serde_json::json!({"check": CheckId::ArchiveSegments.as_str(), "missingSegment": key})
                .to_string(),
        );
    }
    catalogue::outcome(
        CheckId::ArchiveSegments,
        CheckState::NotReady,
        CheckCode::SegmentMissing,
        now,
    )
    .with_message(&format!(
        "{} of {} segment(s) backup set `{}` names for this window are not in the archive",
        missing.len(),
        expected.len(),
        req.backup_id
    ))
    .with_remedy(remedy_for(CheckCode::SegmentMissing))
    .with_detail(detail(&missing))
}

#[allow(clippy::too_many_arguments)]
fn target_checks(
    req: &RestorePreflightRequest,
    spec: &DrillSpec,
    wiring: &dyn Wiring,
    deadline: Deadline,
    now: DateTime<Utc>,
    want: &dyn Fn(CheckId) -> bool,
    checks: &mut Vec<CheckOutcome>,
    details: &mut Vec<String>,
) {
    let scope = catalogue::scope("RestoreTarget", &req.target.principal, None);
    let budget = deadline.slice(2);
    let probe = match wiring.broker(&req.target, budget) {
        Ok(p) => p,
        Err(f) => {
            if want(CheckId::TargetAuthenticated) {
                checks.push(
                    from_broker_failure(CheckId::TargetAuthenticated, &f, now).with_scope(scope),
                );
            }
            block_rest(
                &[
                    CheckId::TargetScratchMarker,
                    CheckId::TargetMappedTopics,
                    CheckId::TargetTopicCreate,
                    CheckId::TargetTimestampBound,
                ],
                want,
                now,
                checks,
            );
            return;
        }
    };

    let (row, _) = authenticated(
        CheckId::TargetAuthenticated,
        probe.as_ref(),
        Some(scope.clone()),
        now,
    );
    let authenticated_ok = row.state == CheckState::Ready;
    if want(CheckId::TargetAuthenticated) {
        checks.push(row);
    }
    if !authenticated_ok {
        block_rest(
            &[
                CheckId::TargetScratchMarker,
                CheckId::TargetMappedTopics,
                CheckId::TargetTopicCreate,
                CheckId::TargetTimestampBound,
            ],
            want,
            now,
            checks,
        );
        return;
    }

    // ---- the scratch segregation proof ---------------------------------
    if spec.target.mode == TargetMode::Scratch && want(CheckId::TargetScratchMarker) {
        checks.push(
            marker_row(probe.as_ref(), &spec.target.marker_topic, now).with_scope(scope.clone()),
        );
    }

    // ---- the mapped names ----------------------------------------------
    let prefix = target_topic_prefix(spec);
    let mapped: Vec<String> = spec
        .source
        .topics
        .iter()
        .map(|t| format!("{prefix}{t}"))
        .collect();

    if want(CheckId::TargetMappedTopics) {
        checks.push(
            mapped_topics_row(probe.as_ref(), &mapped, deadline, now, details)
                .with_scope(scope.clone()),
        );
    }
    if want(CheckId::TargetTopicCreate) {
        checks.push(topic_create_row(probe.as_ref(), spec, &mapped, now).with_scope(scope.clone()));
    }
    if want(CheckId::TargetTimestampBound) {
        checks.push(timestamp_bound_row(probe.as_ref(), spec, now).with_scope(scope.clone()));
    }
    if want(CheckId::TargetLogAppendTime) {
        checks.push(
            execution_only(
                CheckId::TargetLogAppendTime,
                CheckCode::LogAppendTimeOverrideVerifiedOnlyAtExecution,
                now,
            )
            .with_message(
                "establishing whether a per-topic CreateTime override is honoured needs a topic \
                 to exist, and a preflight creates none",
            )
            .with_scope(scope),
        );
    }
}

/// `target.scratchMarker` — the v0.1 segregation proof, read-only.
fn marker_row(probe: &dyn InventoryProbe, marker: &str, now: DateTime<Utc>) -> CheckOutcome {
    match probe.describe_topic(marker) {
        Ok(TopicPresence::Present { .. }) => {
            ready(CheckId::TargetScratchMarker, CheckCode::MarkerHealthy, now)
                .with_message(&format!("the scratch marker topic `{marker}` is present"))
        }
        Ok(TopicPresence::NotFound) => catalogue::outcome(
            CheckId::TargetScratchMarker,
            CheckState::NotReady,
            CheckCode::MarkerTopicMissing,
            now,
        )
        .with_message(&format!(
            "the scratch marker topic `{marker}` is not on the target"
        ))
        .with_remedy(remedy_for(CheckCode::MarkerTopicMissing)),
        Ok(TopicPresence::NotAuthorized | TopicPresence::Unknown) | Err(_) => catalogue::outcome(
            CheckId::TargetScratchMarker,
            CheckState::NotReady,
            CheckCode::MarkerTopicErrored,
            now,
        )
        .with_message(&format!(
            "the scratch marker topic `{marker}` could not be described on the target"
        ))
        .with_remedy(remedy_for(CheckCode::MarkerTopicErrored)),
    }
}

/// `target.mappedTopics` — D2 §6.7(a): targeted metadata per mapped name.
fn mapped_topics_row(
    probe: &dyn InventoryProbe,
    mapped: &[String],
    deadline: Deadline,
    now: DateTime<Utc>,
    details: &mut Vec<String>,
) -> CheckOutcome {
    let mut exists: Vec<String> = Vec::new();
    let mut unknown: Vec<String> = Vec::new();
    for name in mapped {
        if !deadline.has_room() {
            unknown.push(name.clone());
            continue;
        }
        match probe.describe_topic(name) {
            // ABSENT is the pass: the restore creates it.
            Ok(TopicPresence::NotFound) => {}
            Ok(TopicPresence::Present { .. }) => exists.push(name.clone()),
            // `TOPIC_AUTHORIZATION_FAILED` says nothing about existence.
            Ok(TopicPresence::NotAuthorized | TopicPresence::Unknown) | Err(_) => {
                unknown.push(name.clone());
            }
        }
    }
    if !exists.is_empty() {
        for name in &exists {
            details.push(
                serde_json::json!({
                    "check": CheckId::TargetMappedTopics.as_str(),
                    "mappedTopicExists": name
                })
                .to_string(),
            );
        }
        return catalogue::outcome(
            CheckId::TargetMappedTopics,
            CheckState::NotReady,
            CheckCode::MappedTopicExists,
            now,
        )
        .with_message(&format!(
            "{} of {} mapped target topic(s) already exist",
            exists.len(),
            mapped.len()
        ))
        .with_remedy(remedy_for(CheckCode::MappedTopicExists))
        .with_detail(detail(&exists));
    }
    if !unknown.is_empty() {
        return catalogue::outcome(
            CheckId::TargetMappedTopics,
            CheckState::Unknown,
            CheckCode::MappedTopicVisibilityUnknown,
            now,
        )
        .with_message(&format!(
            "{} of {} mapped target topic(s) could not be described",
            unknown.len(),
            mapped.len()
        ))
        .with_remedy(remedy_for(CheckCode::MappedTopicVisibilityUnknown))
        .with_detail(detail(&unknown));
    }
    ready(
        CheckId::TargetMappedTopics,
        CheckCode::MappedTopicsAbsent,
        now,
    )
    .with_message(&format!(
        "none of the {} mapped target topic(s) exists on the target",
        mapped.len()
    ))
}

/// `target.topicCreate` — D2 §6.7(b): a `CreateTopics` with
/// `validate_only = true`.
///
/// It creates nothing. It is the second half of the collision answer, and it
/// reaches a case metadata cannot: a topic hidden from `DESCRIBE` answers
/// `TOPIC_ALREADY_EXISTS` here when `CREATE` is authorized.
///
/// The partition count is **1** and is not a claim about the restore: the real
/// count is the manifest's and is chosen at execution. A validate-only request
/// validates the CONFIGURATION and the REPLICATION FACTOR against the
/// cluster's brokers, which is what this row is about.
fn topic_create_row(
    probe: &dyn InventoryProbe,
    spec: &DrillSpec,
    mapped: &[String],
    now: DateTime<Utc>,
) -> CheckOutcome {
    if mapped.is_empty() {
        return ready(
            CheckId::TargetTopicCreate,
            CheckCode::TopicCreateValidated,
            now,
        )
        .with_message("the plan maps no target topic");
    }
    let specs: Vec<NewTopicSpec> = mapped
        .iter()
        .map(|name| NewTopicSpec {
            name: name.clone(),
            num_partitions: 1,
            replication_factor: i32::from(spec.target.default_replication_factor),
            configs: TARGET_TOPIC_CONFIGS
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect(),
        })
        .collect();
    let outcomes = match probe.validate_create_topics(&specs) {
        Ok(o) => o,
        Err(f) => {
            // The REQUEST failed, not a topic: the target did not answer a
            // validate-only CreateTopics at all.
            return catalogue::outcome(
                CheckId::TargetTopicCreate,
                CheckState::Unknown,
                CheckCode::TopicCreateValidationUnsupported,
                now,
            )
            .with_message(&f.message)
            .with_remedy(remedy_for(CheckCode::TopicCreateValidationUnsupported));
        }
    };
    let refused: Vec<&logweir_kafka::inventory::TopicCreateOutcome> = outcomes
        .iter()
        .filter(|o| o.code != CheckCode::TopicCreateValidated)
        .collect();
    let Some(first) = refused.first() else {
        return ready(
            CheckId::TargetTopicCreate,
            CheckCode::TopicCreateValidated,
            now,
        )
        .with_message(&format!(
            "the target validated creation of all {} mapped topic(s) at replication factor {}",
            mapped.len(),
            spec.target.default_replication_factor
        ));
    };
    let code = first.code;
    let names: Vec<String> = refused.iter().map(|o| o.name.clone()).collect();
    catalogue::outcome(CheckId::TargetTopicCreate, state_for(code), code, now)
        .with_message(&format!(
            "{} of {} mapped topic(s) were refused by a validate-only CreateTopics: {code}",
            refused.len(),
            mapped.len()
        ))
        .with_remedy(remedy_for(code))
        .with_detail(detail(&names))
}

/// `target.timestampBound` — the same arithmetic the execution guard runs
/// (`phase0_admit.rs`'s `target_topic_preflight`, step 2).
fn timestamp_bound_row(
    probe: &dyn InventoryProbe,
    spec: &DrillSpec,
    now: DateTime<Utc>,
) -> CheckOutcome {
    let broker = match probe.broker_configs() {
        Ok(b) => b,
        Err(_) => {
            return catalogue::outcome(
                CheckId::TargetTimestampBound,
                CheckState::Unknown,
                CheckCode::BrokerConfigsNotReadable,
                now,
            )
            .with_message("the target's broker configuration could not be read")
            .with_remedy(remedy_for(CheckCode::BrokerConfigsNotReadable));
        }
    };
    // `before.max.ms` first: on Kafka >= 3.6 BOTH keys are reported and
    // `difference.max.ms` is the deprecated one.
    let bound = broker
        .get(BROKER_TIMESTAMP_BEFORE_MAX_MS)
        .or_else(|| broker.get(BROKER_TIMESTAMP_DIFFERENCE_MAX_MS))
        .and_then(|v| v.trim().parse::<i64>().ok());
    let Some(bound) = bound else {
        return ready(
            CheckId::TargetTimestampBound,
            CheckCode::TimestampWithinBound,
            now,
        )
        .with_message("the target declares no record-timestamp bound");
    };
    let end_ms = recovery_point(spec).timestamp_millis();
    // `saturating_sub` is load-bearing: the Apache default for both keys is
    // i64::MAX, so a plain `-` overflows and wraps to a floor in the FUTURE,
    // which would refuse every window on every default broker.
    let oldest_accepted = now.timestamp_millis().saturating_sub(bound);
    if end_ms < oldest_accepted {
        return catalogue::outcome(
            CheckId::TargetTimestampBound,
            CheckState::NotReady,
            CheckCode::TimestampBoundExceeded,
            now,
        )
        .with_message(&format!(
            "the target bounds record timestamps at {bound} ms, so nothing older than epoch-ms \
             {oldest_accepted} is accepted, and this plan's recovery point is epoch-ms {end_ms}"
        ))
        .with_remedy(remedy_for(CheckCode::TimestampBoundExceeded));
    }
    let mut row = ready(
        CheckId::TargetTimestampBound,
        CheckCode::TimestampWithinBound,
        now,
    )
    .with_message(&format!(
        "this plan's recovery point epoch-ms {end_ms} is inside the target's {bound} ms \
         record-timestamp bound"
    ));
    if let Some(t) = broker.get(BROKER_TIMESTAMP_TYPE) {
        row = row.with_fact("brokerTimestampType", t);
    }
    row
}

/// Rows that could not run because a prerequisite did not pass.
fn block_rest(
    ids: &[CheckId],
    want: &dyn Fn(CheckId) -> bool,
    now: DateTime<Utc>,
    checks: &mut Vec<CheckOutcome>,
) {
    for id in ids {
        if want(*id) {
            checks.push(
                catalogue::outcome(
                    *id,
                    CheckState::Unknown,
                    CheckCode::BlockedByPrerequisite,
                    now,
                )
                .with_message("a prerequisite of this check did not pass, so it did not run")
                .with_remedy(remedy_for(CheckCode::BlockedByPrerequisite)),
            );
        }
    }
}

/// The `details` stream: JSON lines, **redacted**, capped, with a final count
/// line when the cap bit.
///
/// The cap is enforced HERE and not by the caller, so every producer of a
/// detail line is bounded by one rule. A truncated stream says so rather than
/// simply ending: a consumer that could not tell would render "3 missing
/// segments" for a set missing three thousand.
///
/// # The redaction, and why it is at this line and not at the push sites
///
/// A details line carries adopter identifiers — a segment key, a mapped topic
/// name — and the controller writes the stream VERBATIM into an immutable
/// `<job>-details` `ConfigMap` (D2 §6.7). `CheckOutcome`'s own builders redact
/// `message`, `remedy` and `facts`, and `catalogue::scope` and
/// `readiness::detail` were made to redact for the same reason; this stream was
/// the third exception to a rule the code, `docs/stability.md` and the report
/// all state has exactly one. It has one now.
///
/// It is applied HERE because this function is the ONE place the stream is
/// assembled, so a new producer of a detail line cannot forget — which is the
/// property a per-push-site redaction would not have. `redact` is idempotent
/// and fires only on credential shapes, so an ordinary key or topic name is
/// unchanged, and it never emits a quote, so a JSON line stays a JSON line.
#[must_use]
pub fn details_stream(lines: &[String]) -> Vec<u8> {
    let mut out = String::new();
    let mut kept = 0usize;
    for line in lines {
        let line = crate::check::redact_path(line);
        if out.len() + line.len() + 1 > DETAILS_MAX_BYTES {
            break;
        }
        out.push_str(&line);
        out.push('\n');
        kept += 1;
    }
    if kept < lines.len() {
        let note = serde_json::json!({
            "truncated": true,
            "kept": kept,
            "total": lines.len(),
            "sampleLimit": DETAIL_SAMPLE,
        })
        .to_string();
        out.push_str(&note);
        out.push('\n');
    }
    out.into_bytes()
}
