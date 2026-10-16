//! **PROD-07.2: an interrupted restore, reconciled** (design
//! `docs/to-do/decisions/OD-23-core.md` §3).
//!
//! A restore run that created its target topics writes the **targets record**
//! (`logweir/drills/<run_id>.targets.json`, create-only, internal) before its
//! engine starts. When that run ends without a signed scorecard (a cancel, a
//! deleted pod, an engine failure), `logweir restore reconcile --run-id <id>`,
//! with the run's own flags, tells what is on the target:
//!
//! 1. the run's own scorecard exists after all (it signed before it was
//!    stopped): its keys are relayed and the scorecard decides (`relayed`);
//! 2. no targets record, or none of its topics is still the one the run
//!    created (same cluster, same topic ID): `reconcile-none=<why>`, exit 0;
//! 3. otherwise the kept topics' end offsets must hold still for ten seconds
//!    (at most five minutes, else exit 1 `TargetStillGrowing`), and the run's
//!    plan is verified against them with the complete lane, nothing restored:
//!    an ordinary scorecard under the reconcile's own run id, `pass` when
//!    every partition is exact (exit 0), `fail-integrity` with the counts
//!    otherwise (exit 2), its `target.creation` the kept topics with their
//!    recorded IDs, which `logweir restore cleanup` then removes.
//!
//! Nothing here writes to the target.

use super::{DrillError, RestoreOutcome};
use crate::exit::ExitCode;
use logweir_core::guard::GuardRefusal;
use logweir_core::outcome::Outcome;
use logweir_core::scorecard::Scorecard;
use logweir_core::target_creation::{cleanup_plan, CurrentId, TargetCreation};
use logweir_engine_oso::storage::{caps, Store, StoreError};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

/// The line a reconcile that found nothing to verify prints, with the reason.
pub const NONE_PREFIX: &str = "reconcile-none=";
/// The line a run that created its targets prints at an exit without a
/// scorecard: the targets record's key.
pub const TARGETS_RECORD_KEY_PREFIX: &str = "targets-record-key=";
/// The note the reconcile's scorecard opens `sample.coverage_note` with.
pub const NOTE_PREFIX: &str = "reconciliation of interrupted run";
/// How long the targets must hold still.
pub const STILL_FOR: Duration = Duration::from_secs(10);
/// How long the reconcile waits for them to.
pub const STILL_WITHIN: Duration = Duration::from_secs(300);

/// `logweir/drills/<run_id>.targets.json`.
#[must_use]
pub fn targets_record_key(run_id: &str) -> String {
    format!("logweir/drills/{run_id}.targets.json")
}

/// **The targets record**: what a run created, written before its engine
/// starts. Internal working state, read only by the reconcile; the signed
/// scorecard is the evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TargetsRecord {
    /// The run that created the topics.
    pub run_id: String,
    /// `sha256:<hex>` of the plan bytes it ran.
    pub plan_hash: String,
    /// The target cluster it created them on.
    pub cluster_id: String,
    /// Each created topic with its partition count and its ID at creation.
    pub topics: Vec<logweir_core::target_creation::CreatedTopic>,
}

/// Put `record` create-only at its key; the key on success.
///
/// # Errors
///
/// [`DrillError::Operational`] when it cannot be serialised or put: the run
/// then starts no engine.
pub fn write(store: &Store, record: &TargetsRecord) -> Result<String, DrillError> {
    let key = targets_record_key(&record.run_id);
    let bytes = serde_json::to_vec(record)
        .map_err(|e| DrillError::Operational(format!("targets record: {e}")))?;
    store.put_create_only(&key, &bytes).map_err(|e| {
        DrillError::Operational(format!(
            "the targets record {key} could not be written ({e}); no engine was started"
        ))
    })?;
    Ok(key)
}

/// The topics a reconcile verifies: the recorded ones that are still the run's.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Kept {
    /// The interrupted run.
    pub run_id: String,
    /// The cluster the run created them on, which the target still is.
    pub cluster_id: String,
    /// Source topic → kept target topic.
    pub mapping: BTreeMap<String, String>,
    /// The kept topics, with the IDs recorded at creation.
    pub creation: TargetCreation,
    /// Recorded topics left out, and why.
    pub left_out: Vec<(String, String)>,
}

impl Kept {
    /// Phase 0's answer for a reconcile: the kept mapping on the recorded
    /// cluster. Nothing is created and no probe is written.
    #[must_use]
    pub fn admitted(&self, spec: &logweir_core::spec::DrillSpec) -> super::phase0_admit::Admitted {
        super::phase0_admit::Admitted {
            target_cluster_id: self.cluster_id.clone(),
            topic_mapping: self.mapping.clone(),
            topic_mapping_prefix: logweir_core::spec::target_topic_prefix(spec),
            topic_preflight: Self::empty_preflight(),
            original_name: None,
        }
    }

    /// The note the scorecard carries.
    #[must_use]
    pub fn note(&self) -> String {
        let mut note = format!("{NOTE_PREFIX} {}; nothing was resumed", self.run_id);
        if !self.left_out.is_empty() {
            note.push_str(&format!(
                "; left out: {}",
                self.left_out
                    .iter()
                    .map(|(t, why)| format!("{t} ({why})"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        note
    }
}

/// Which recorded topics are still the run's: the cleanup's ID rule
/// ([`cleanup_plan`]) on the recorded cluster. `Ok(None)` when none is, with
/// the reason in the `Err` of the inner result.
///
/// # Errors
///
/// [`DrillError::Operational`] when an ID could not be read: a later attempt
/// may read it.
pub fn kept(
    spec: &logweir_core::spec::DrillSpec,
    record: &TargetsRecord,
    current_cluster: &str,
    current: &BTreeMap<String, CurrentId>,
) -> Result<Result<Kept, String>, DrillError> {
    if current_cluster != record.cluster_id {
        return Ok(Err(format!(
            "the target answers as cluster {current_cluster} and run {} created its topics on {}; \
             nothing was verified",
            record.run_id, record.cluster_id
        )));
    }
    let recorded = TargetCreation {
        topics: record.topics.clone(),
        lineage_headers: logweir_core::target_creation::LINEAGE_HEADERS_KEPT.to_string(),
        teardown_after_seconds: None,
    };
    let plan = cleanup_plan(&recorded, current);
    if let Some((topic, why)) = plan.unreadable.first() {
        return Err(DrillError::Operational(format!(
            "{topic}: {why}; the reconcile is tried again"
        )));
    }
    let mut left_out: Vec<(String, String)> = plan.skipped.clone();
    left_out.extend(
        plan.already_absent
            .iter()
            .map(|t| (t.clone(), "no longer exists".to_string())),
    );
    if plan.delete.is_empty() {
        return Ok(Err(format!(
            "none of the {} topics run {} created is still the one it created ({}); nothing was \
             verified",
            record.topics.len(),
            record.run_id,
            left_out
                .iter()
                .map(|(t, why)| format!("{t}: {why}"))
                .collect::<Vec<_>>()
                .join("; ")
        )));
    }
    let prefix = logweir_core::spec::target_topic_prefix(spec);
    let mapping: BTreeMap<String, String> = spec
        .source
        .topics
        .iter()
        .map(|s| (s.clone(), format!("{prefix}{s}")))
        .filter(|(_, t)| plan.delete.contains(t))
        .collect();
    Ok(Ok(Kept {
        run_id: record.run_id.clone(),
        cluster_id: record.cluster_id.clone(),
        mapping,
        creation: TargetCreation {
            topics: recorded
                .topics
                .into_iter()
                .filter(|t| plan.delete.contains(&t.name))
                .collect(),
            lineage_headers: recorded.lineage_headers,
            teardown_after_seconds: None,
        },
        left_out,
    }))
}

/// Wait until the kept topics' end offsets are unchanged over [`STILL_FOR`],
/// for at most [`STILL_WITHIN`].
///
/// # Errors
///
/// [`DrillError::Operational`] `TargetStillGrowing` at the bound, or a read
/// failure.
pub fn wait_until_still(
    reader: &dyn logweir_kafka::reader::ClusterReader,
    topics: &[String],
    still_for: Duration,
    within: Duration,
) -> Result<(), DrillError> {
    let read = || -> Result<BTreeMap<String, Vec<(i32, i64)>>, DrillError> {
        topics
            .iter()
            .map(|t| Ok((t.clone(), reader.end_offsets(t)?)))
            .collect()
    };
    let started = Instant::now();
    let mut last = read()?;
    let mut since = Instant::now();
    loop {
        if since.elapsed() >= still_for {
            return Ok(());
        }
        if started.elapsed() >= within {
            return Err(DrillError::Operational(format!(
                "TargetStillGrowing: the end offsets of {} still moved after {}s, so something \
                 is still writing to them; nothing was verified",
                topics.join(", "),
                within.as_secs()
            )));
        }
        std::thread::sleep(Duration::from_millis(500).min(still_for));
        let now = read()?;
        if now != last {
            last = now;
            since = Instant::now();
        }
    }
}

/// What the reconcile found.
#[derive(Debug)]
pub enum Reconciled {
    /// Nothing to verify, and why.
    Nothing(String),
    /// The run's own scorecard, and its keys: the scorecard decides.
    Relayed(Box<Scorecard>, super::EvidenceKeys),
    /// The reconcile's own signed scorecard (or the error that stopped it).
    Signed(Result<RestoreOutcome, DrillError>),
}

/// A read of `key` through `store`: `Ok(None)` when it is absent.
fn read_optional(store: &Store, key: &str, cap: u64) -> Result<Option<Vec<u8>>, DrillError> {
    match store.get_capped(key, cap) {
        Ok((bytes, _)) => Ok(Some(bytes)),
        Err(StoreError::NotFound(_)) => Ok(None),
        Err(e) => Err(DrillError::Operational(format!(
            "EvidenceUnreadable: {key} could not be read ({e}); the reconcile reads the evidence \
             location with the run's archive grant, which must be allowed to read it"
        ))),
    }
}

/// **The reconcile**, after `prepare` built the run's context.
///
/// # Errors
///
/// [`DrillError`] by the module doc.
pub(super) fn execute(
    args: &super::RunArgs,
    run_id: &str,
    mut prepared: super::Prepared,
) -> Result<Reconciled, DrillError> {
    let interrupted = args.run_id.clone().ok_or_else(|| {
        GuardRefusal("restore reconcile needs --run-id, the interrupted run".to_string())
    })?;
    let read =
        crate::evidence_read::read_store(&prepared.ctx.spec.evidence, prepared.store_contract)
            .map_err(DrillError::Operational)?;
    let plan_hash = logweir_core::ids::sha256_prefixed(prepared.ctx.spec_text.as_bytes());
    // 1. The run signed after all: its scorecard decides.
    let key = format!("logweir/drills/{interrupted}.json");
    if let Some(bytes) = read_optional(&read, &key, caps::SIGNED_DOCUMENT)? {
        let sidecar_key = format!("logweir/drills/{interrupted}.sig");
        let sidecar = read_optional(&read, &sidecar_key, caps::SIDECAR)?.ok_or_else(|| {
            GuardRefusal(format!(
                "{key} exists without its sidecar {sidecar_key}; nothing was verified"
            ))
        })?;
        let sidecar: logweir_evidence::Sidecar = serde_json::from_slice(&sidecar)
            .map_err(|e| GuardRefusal(format!("{sidecar_key} is not a DSSE sidecar ({e})")))?;
        logweir_evidence::verify::verify_detached(
            &prepared.signer.verifying_key(),
            logweir_evidence::PAYLOAD_TYPE_SCORECARD,
            &bytes,
            &sidecar,
        )
        .map_err(|e| GuardRefusal(format!("{key}'s signature does not verify ({e})")))?;
        let sc: Scorecard = serde_json::from_slice(&bytes)
            .map_err(|e| GuardRefusal(format!("{key} is not a scorecard ({e})")))?;
        if sc.run_id != interrupted || sc.approval.plan_hash != plan_hash {
            return Err(GuardRefusal(format!(
                "{key} is not run {interrupted}'s scorecard over this plan"
            ))
            .into());
        }
        let keys = super::EvidenceKeys {
            scorecard_key: key,
            sidecar_key,
            offset_report_key: sc.evidence.offset_report_key.clone(),
        };
        return Ok(Reconciled::Relayed(Box::new(sc), keys));
    }
    // 2. The targets record.
    let record_key = targets_record_key(&interrupted);
    let Some(bytes) = read_optional(&read, &record_key, caps::SIGNED_DOCUMENT)? else {
        return Ok(Reconciled::Nothing(format!(
            "run {interrupted} wrote no targets record ({record_key}), so it created no topic"
        )));
    };
    let record: TargetsRecord = serde_json::from_slice(&bytes)
        .map_err(|e| GuardRefusal(format!("{record_key} does not parse ({e})")))?;
    if record.run_id != interrupted || record.plan_hash != plan_hash {
        return Err(GuardRefusal(format!(
            "{record_key} records run {} over plan {}, not run {interrupted} over this plan \
             ({plan_hash}); nothing was verified",
            record.run_id, record.plan_hash
        ))
        .into());
    }
    let reader = prepared.ctx.client.as_reader();
    let cluster = reader.cluster_id()?;
    let names: Vec<String> = record.topics.iter().map(|t| t.name.clone()).collect();
    let current: BTreeMap<String, CurrentId> = reader
        .topic_ids(&names)?
        .into_iter()
        .map(|(name, read)| {
            let now = match read {
                logweir_kafka::topic_ids::TopicIdRead::Id(id) => CurrentId::Id(id),
                logweir_kafka::topic_ids::TopicIdRead::NotFound => CurrentId::NotFound,
                other => CurrentId::Unread(
                    other
                        .to_id_read()
                        .reason()
                        .unwrap_or(logweir_core::topic_identity::READ_FAILED)
                        .to_string(),
                ),
            };
            (name, now)
        })
        .collect();
    let kept = match kept(&prepared.ctx.spec, &record, &cluster, &current)? {
        Ok(kept) => kept,
        Err(why) => return Ok(Reconciled::Nothing(why)),
    };
    // 3. Still, then verified, complete, nothing restored.
    let targets: Vec<String> = kept.mapping.values().cloned().collect();
    wait_until_still(reader, &targets, STILL_FOR, STILL_WITHIN)?;
    prepared.ctx.spec.sample.coverage = logweir_core::spec::Coverage::Complete;
    Ok(Reconciled::Signed(super::execute_reconcile(
        args,
        run_id,
        &prepared.ctx,
        &prepared.signer,
        prepared.approved,
        &kept,
    )))
}

/// `logweir restore reconcile`: print what [`execute`] found and exit by the
/// module doc.
pub fn run(args: super::RunArgs) -> ExitCode {
    let run_id = crate::ids::new_run_id();
    super::init_logging(&run_id);
    let _span = tracing::info_span!("drill", run_id = %run_id).entered();
    let found = super::prepare(&args).and_then(|p| execute(&args, &run_id, p));
    let outcome = match found {
        Ok(Reconciled::Nothing(why)) => {
            tracing::warn!(run_id = %run_id, "{why}");
            let mut last = crate::exit::FinalLines::default();
            last.push(format!("{NONE_PREFIX}{}", crate::exit::one_line(&why)));
            last.print(ExitCode::Ok);
            return ExitCode::Ok;
        }
        Ok(Reconciled::Relayed(sc, keys)) => {
            if sc.outcome == Outcome::Pass {
                Ok(RestoreOutcome {
                    scorecard: *sc,
                    topic_preflight: Kept::empty_preflight(),
                    evidence: keys,
                })
            } else {
                Err(DrillError::NotPass(sc, Some(keys)))
            }
        }
        Ok(Reconciled::Signed(outcome)) => outcome,
        Err(e) => Err(e),
    };
    super::report(&args, &run_id, outcome)
}

impl Kept {
    /// The preflight line of a run that read no target configuration.
    #[must_use]
    pub fn empty_preflight() -> super::phase0_admit::TopicPreflight {
        super::phase0_admit::TopicPreflight {
            timestamp_type: None,
            retention_ms: String::new(),
            timestamp_bound_ms: None,
            configs_set: Vec::new(),
            topics_created: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logweir_core::target_creation::CreatedTopic;

    fn spec(prefix: &str) -> logweir_core::spec::DrillSpec {
        serde_yaml::from_str(&format!(
            "source:\n  storage: {{backend: s3, bucket: b, prefix: p, region: us-east-1}}\n  \
             backup: latestCompleted\n  topics: [orders, payments]\n\
             target:\n  bootstrap_servers: [\"k:9092\"]\n  mode: newTopic\n  \
             topic_mapping_prefix: \"drill-\"\n  \
             topic_naming: {{prefix: \"{prefix}\"}}\n\
             sample:\n  window_start: \"2026-10-13T00:00:00Z\"\n  \
             window_end: \"2026-10-13T01:00:00Z\"\n\
             objectives: {{}}\n\
             evidence: {{backend: s3, bucket: e, prefix: logweir/, region: us-east-1}}\n"
        ))
        .expect("a plan")
    }

    fn record() -> TargetsRecord {
        TargetsRecord {
            run_id: "R1".into(),
            plan_hash: "sha256:x".into(),
            cluster_id: "C".into(),
            topics: ["r-orders", "r-payments"]
                .iter()
                .map(|n| CreatedTopic {
                    name: (*n).to_string(),
                    partitions: 3,
                    topic_id: Some(format!("id-{n}")),
                    topic_id_reason: None,
                })
                .collect(),
        }
    }

    /// A recorded topic whose ID is still the recorded one is kept; one
    /// deleted and created again under the same name is left out and named;
    /// the mapping holds only the kept one. KILLS: keeping by name alone.
    #[test]
    fn only_topics_with_the_recorded_id_are_kept() {
        let current: BTreeMap<String, CurrentId> = [
            ("r-orders".to_string(), CurrentId::Id("id-r-orders".into())),
            (
                "r-payments".to_string(),
                CurrentId::Id("someone-else".into()),
            ),
        ]
        .into_iter()
        .collect();
        let kept = kept(&spec("r-"), &record(), "C", &current)
            .unwrap()
            .expect("one is kept");
        assert_eq!(
            kept.mapping,
            [("orders".to_string(), "r-orders".to_string())]
                .into_iter()
                .collect()
        );
        assert_eq!(kept.creation.topics.len(), 1);
        assert_eq!(
            kept.creation.topics[0].topic_id.as_deref(),
            Some("id-r-orders")
        );
        assert_eq!(kept.left_out.len(), 1);
        assert_eq!(kept.left_out[0].0, "r-payments");
        assert!(
            kept.note().contains("left out: r-payments"),
            "{}",
            kept.note()
        );
    }

    /// Another cluster, or nothing kept, is nothing to verify; an ID that
    /// could not be read is an error a later attempt retries.
    #[test]
    fn another_cluster_or_nothing_kept_verifies_nothing() {
        let same: BTreeMap<String, CurrentId> = [
            ("r-orders".to_string(), CurrentId::Id("id-r-orders".into())),
            (
                "r-payments".to_string(),
                CurrentId::Id("id-r-payments".into()),
            ),
        ]
        .into_iter()
        .collect();
        assert!(kept(&spec("r-"), &record(), "OTHER", &same)
            .unwrap()
            .is_err());
        let gone: BTreeMap<String, CurrentId> = [
            ("r-orders".to_string(), CurrentId::NotFound),
            ("r-payments".to_string(), CurrentId::NotFound),
        ]
        .into_iter()
        .collect();
        let why = kept(&spec("r-"), &record(), "C", &gone)
            .unwrap()
            .unwrap_err();
        assert!(why.contains("no longer exists"), "{why}");
        let unread: BTreeMap<String, CurrentId> = [
            ("r-orders".to_string(), CurrentId::Unread("x".into())),
            ("r-payments".to_string(), CurrentId::NotFound),
        ]
        .into_iter()
        .collect();
        assert!(kept(&spec("r-"), &record(), "C", &unread).is_err());
    }

    /// The record round-trips, and its key is the run's.
    #[test]
    fn the_record_round_trips_under_the_runs_key() {
        let r = record();
        let back: TargetsRecord = serde_json::from_slice(&serde_json::to_vec(&r).unwrap()).unwrap();
        assert_eq!(back, r);
        assert_eq!(targets_record_key("R1"), "logweir/drills/R1.targets.json");
    }
}
