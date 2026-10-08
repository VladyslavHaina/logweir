//! **FX-8** — which clock a restore's TIME SELECTION reads, per source topic,
//! and the refusal of a selection by producer time the plan did not accept.
//!
//! # The defect
//!
//! The pinned engine archives each record's PRODUCER timestamp: it reads a
//! batch's first timestamp plus the record's delta and discards the batch's
//! max timestamp, which is the only place a broker on `LogAppendTime` writes
//! its append time (PROD-01.1 S3). Every selection by time — the restore
//! window's end — therefore reads producer time. For a `CreateTime` topic that
//! is the topic's own clock. For a `LogAppendTime` topic it is not, and phase 7
//! cannot notice, because it compares the target with the archive and both
//! carry producer time: PROD-01.1 restored six records at a point in 2001 that
//! the broker had appended in 2026, signed `pass` 6/6
//! (`docs/to-do/decisions/PROD-01.1-record-semantics.md` §2.3).
//!
//! # The rule ([`decide`])
//!
//! For each source topic the plan names:
//!
//! 1. **Its recorded timestamp type** ([`recorded_timestamp_type`]) comes from
//!    two records and nothing else: the archive manifest's
//!    `configurations["message.timestamp.type"]` (a TOPIC OVERRIDE — the engine
//!    keeps explicit overrides only), and the effective value a VERIFIED backup
//!    receipt recorded at backup time (FX-4's `config_coverage[topic].
//!    timestamp_type`, which is how a broker-wide default reaches a restore).
//!    `LogAppendTime` from either wins; else `CreateTime` from either; else the
//!    type is NOT RECORDED. A live read of the source cluster is never taken:
//!    what the archive holds was decided when it was written.
//! 2. **Whether the restore selects it by time** ([`selects_by_time`]): always
//!    when the plan states `restore.point_in_time`; otherwise the window's end
//!    is `sample.window_end`, and it selects by time when that end is earlier
//!    than the newest timestamp the manifest records for the topic (a record
//!    the archive certainly holds falls outside the window).
//! 3. **The verdict.** A `LogAppendTime` topic selected by time is REFUSED with
//!    [`crate::guard::TERMINAL_STATE_POINT_IN_TIME_BY_PRODUCER_TIME`] unless the
//!    plan states `restore.time_basis: producerTime`; with it the topic is
//!    listed in the signed label's `producer_time`. A topic selected by time
//!    whose type is NOT RECORDED runs, and is listed in `not_recorded` (the
//!    unknown-type decision, argued at [`crate::scorecard::TimeBasisLabel`]).
//!    A `CreateTime` topic, and a topic not selected by time, is listed nowhere.
//!
//! # Global Constraint 1
//!
//! No I/O, no clock, no network: every input is a value the caller read.

use crate::backup_receipt::{SourceConfigCoverage, TopicConfigCoverage};
use crate::engine::{BackupSetFacts, TopicFacts};
use crate::guard::TERMINAL_STATE_POINT_IN_TIME_BY_PRODUCER_TIME;
use crate::scorecard::TimeBasisLabel;
use crate::spec::{DrillSpec, TimeBasis};
use std::collections::BTreeMap;

/// The topic configuration key the engine captures as an override and FX-4's
/// read records as an effective value.
pub const TIMESTAMP_TYPE_KEY: &str = "message.timestamp.type";
/// Kafka's two values for [`TIMESTAMP_TYPE_KEY`].
pub const CREATE_TIME: &str = "CreateTime";
/// See [`CREATE_TIME`].
pub const LOG_APPEND_TIME: &str = "LogAppendTime";

/// A source topic's timestamp type as the archive and its receipt RECORD it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordedTimestampType {
    CreateTime,
    LogAppendTime,
    /// Neither record says: no manifest override, and no effective value in a
    /// verified receipt (a plan bound to no receipt, a receipt from before
    /// FX-4's format 1.1.0, or a backup whose configuration read failed).
    NotRecorded,
}

/// [`RecordedTimestampType`] with the record it came from, for the refusal
/// message an operator acts on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicTimestampType {
    pub value: RecordedTimestampType,
    /// Where the value came from, in words; empty for
    /// [`RecordedTimestampType::NotRecorded`].
    pub evidence: String,
}

/// One source topic's recorded timestamp type: the manifest's topic override
/// and the verified receipt's effective value, `LogAppendTime` from EITHER
/// winning.
///
/// Either record saying `LogAppendTime` is enough, because the two are
/// written by two reads at two moments (FX-4: Logweir's own DescribeConfigs
/// immediately before the engine starts, the engine's capture after it) and a
/// disagreement between them is a configuration that changed in between —
/// the archive may then hold records appended under either type. A value
/// outside Kafka's two is not a record of either and is ignored.
#[must_use]
pub fn recorded_timestamp_type(
    manifest_configurations: &BTreeMap<String, String>,
    receipt_entry: Option<&TopicConfigCoverage>,
) -> TopicTimestampType {
    let manifest = manifest_configurations
        .get(TIMESTAMP_TYPE_KEY)
        .map(String::as_str);
    let receipt = receipt_entry.and_then(|e| e.timestamp_type.as_ref());
    let from_manifest =
        |value: &str| format!("the archive manifest's topic override {TIMESTAMP_TYPE_KEY}={value}");
    let from_receipt = |value: &str, source: &str| {
        format!("the bound backup receipt's effective {TIMESTAMP_TYPE_KEY} {value} from {source}")
    };
    if manifest == Some(LOG_APPEND_TIME) {
        return TopicTimestampType {
            value: RecordedTimestampType::LogAppendTime,
            evidence: from_manifest(LOG_APPEND_TIME),
        };
    }
    if let Some(r) = receipt.filter(|r| r.value == LOG_APPEND_TIME) {
        return TopicTimestampType {
            value: RecordedTimestampType::LogAppendTime,
            evidence: from_receipt(LOG_APPEND_TIME, &r.source),
        };
    }
    if manifest == Some(CREATE_TIME) {
        return TopicTimestampType {
            value: RecordedTimestampType::CreateTime,
            evidence: from_manifest(CREATE_TIME),
        };
    }
    if let Some(r) = receipt.filter(|r| r.value == CREATE_TIME) {
        return TopicTimestampType {
            value: RecordedTimestampType::CreateTime,
            evidence: from_receipt(CREATE_TIME, &r.source),
        };
    }
    TopicTimestampType {
        value: RecordedTimestampType::NotRecorded,
        evidence: String::new(),
    }
}

/// Whether this plan's restore window selects `topic`'s records BY TIME.
///
/// * A stated `restore.point_in_time` always does: the engine filters every
///   record by its own timestamp at the point, and nothing the manifest holds
///   proves the point leaves every record in (a segment records its first and
///   last timestamps, not its maximum — PROD-01.1 S6).
/// * A stated `restore.window_start` (PROD-11.1) always does, for the same
///   reason at the other end of the window.
/// * With no point stated the window's end is `sample.window_end`, and it
///   selects by time when that end is EARLIER than the newest timestamp the
///   manifest records for the topic: a record the archive certainly holds is
///   then outside the window. At or after it, the restore takes the archive
///   as the manifest records it (a full restore, which FX-6 discloses) — as
///   far as the segment bounds show: with out-of-order timestamps inside a
///   segment a record later than both ends can still fall outside the window,
///   which is PROD-01.1b's (review L-1).
///
/// `None` for the topic's facts (the manifest does not describe it) counts as
/// "no recorded timestamp", so only a stated point selects it by time.
#[must_use]
pub fn selects_by_time(spec: &DrillSpec, topic: Option<&TopicFacts>) -> bool {
    // PROD-11.1: a stated window START is a selection by time exactly as a
    // stated point is — it excludes every archived record older than it.
    if spec.restore.point_in_time.is_some() || spec.restore.window_start.is_some() {
        return true;
    }
    let end = spec.sample.window_end.timestamp_millis();
    topic
        .and_then(TopicFacts::newest_recorded_timestamp_ms)
        .is_some_and(|newest| end < newest)
}

/// The time-basis decision for one restore: the signed label, or the
/// `PointInTimeByProducerTime` refusal message.
///
/// `topics` are the plan's source topics (the admitted mapping's keys). The
/// message opens with the terminal state, so `refusal-reason=` names it
/// (`crate::guard::terminal_state`), and names every refused topic with the
/// record that made it `LogAppendTime`.
///
/// # Errors
///
/// The refusal message, when any topic selected by time is recorded as
/// `LogAppendTime` and the plan does not state `restore.time_basis:
/// producerTime`.
pub fn decide(
    spec: &DrillSpec,
    facts: &BackupSetFacts,
    coverage: &SourceConfigCoverage,
    topics: &[String],
) -> Result<TimeBasisLabel, String> {
    let accepted = spec.restore.time_basis == Some(TimeBasis::ProducerTime);
    let empty = BTreeMap::new();
    let mut topics: Vec<&String> = topics.iter().collect();
    topics.sort();
    topics.dedup();
    let mut label = TimeBasisLabel {
        plan: spec.restore.time_basis.map(|t| t.as_str().to_string()),
        producer_time: Vec::new(),
        not_recorded: Vec::new(),
    };
    let mut refused: Vec<String> = Vec::new();
    for topic in topics {
        let found = facts.topics.iter().find(|t| &t.name == topic);
        if !selects_by_time(spec, found) {
            continue;
        }
        let recorded = recorded_timestamp_type(
            found.map_or(&empty, |t| &t.configurations),
            coverage.entry(topic),
        );
        match recorded.value {
            RecordedTimestampType::CreateTime => {}
            RecordedTimestampType::NotRecorded => label.not_recorded.push(topic.clone()),
            RecordedTimestampType::LogAppendTime if accepted => {
                label.producer_time.push(topic.clone());
            }
            RecordedTimestampType::LogAppendTime => {
                refused.push(format!("`{topic}` ({})", recorded.evidence));
            }
        }
    }
    if refused.is_empty() {
        return Ok(label);
    }
    // PROD-11.1 (review L2): a stated window start is a time selection too,
    // and is written inside `point_in_time`, so the refusal names the whole
    // interval when there is one.
    let selection = match (spec.restore.window_start, spec.restore.point_in_time) {
        (Some(start), Some(point)) => format!(
            "restore.point_in_time {}/{}, a window with a stated start",
            start.to_rfc3339(),
            point.to_rfc3339()
        ),
        (_, Some(point)) => format!("restore.point_in_time {}", point.to_rfc3339()),
        (_, None) => format!(
            "sample.window_end {}, which is earlier than the newest timestamp the archive \
             manifest records for the topic",
            spec.sample.window_end.to_rfc3339()
        ),
    };
    Err(format!(
        "{TERMINAL_STATE_POINT_IN_TIME_BY_PRODUCER_TIME}: this plan selects records by time \
         ({selection}) over source topic(s) recorded as {LOG_APPEND_TIME}: {}. The archive holds \
         each record's PRODUCER timestamp, not the broker's append time, so the selection would \
         read the producers' clocks: a record the broker appended after the point can be \
         restored and one it appended before can be left out, and the drill compares the target \
         with the archive, so it would pass. The run stopped before the restore's target topics \
         were created. To restore by producer time knowingly, state `restore.time_basis: producerTime` in the plan and have \
         the changed plan approved (the field is inside plan_hash); the signed scorecard then \
         labels the selection. A restore that states no point in time and whose \
         sample.window_end is at or after the archive's newest record is not a selection by \
         time.",
        refused.join(", ")
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backup_receipt::EffectiveConfigValue;

    fn configs(kv: &[(&str, &str)]) -> BTreeMap<String, String> {
        kv.iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn entry(value: &str, source: &str) -> TopicConfigCoverage {
        TopicConfigCoverage {
            coverage: "captured".into(),
            reason: None,
            timestamp_type: Some(EffectiveConfigValue {
                value: value.into(),
                source: source.into(),
            }),
        }
    }

    /// Both records, in every combination that decides the answer:
    /// `LogAppendTime` from either wins, `CreateTime` needs one of them, and
    /// silence — or a value outside Kafka's two — is NOT RECORDED, never
    /// `CreateTime`.
    ///
    /// KILLS: reading only the manifest (the broker-default row reads
    /// `NotRecorded`); reading only the receipt (the override row reads
    /// `NotRecorded`); defaulting silence to `CreateTime`; letting a
    /// `CreateTime` record outvote a `LogAppendTime` one.
    #[test]
    fn the_recorded_type_reads_both_records_and_never_assumes_create_time() {
        use RecordedTimestampType::*;
        let override_lat = configs(&[(TIMESTAMP_TYPE_KEY, LOG_APPEND_TIME)]);
        let override_ct = configs(&[(TIMESTAMP_TYPE_KEY, CREATE_TIME)]);
        let none = BTreeMap::new();
        let bd_lat = entry(LOG_APPEND_TIME, "dynamicDefaultBrokerConfig");
        let bd_ct = entry(CREATE_TIME, "defaultConfig");
        let cases: Vec<(&BTreeMap<String, String>, Option<&TopicConfigCoverage>, _)> = vec![
            (&override_lat, None, LogAppendTime),
            (&none, Some(&bd_lat), LogAppendTime),
            (&override_ct, Some(&bd_lat), LogAppendTime),
            (&override_lat, Some(&bd_ct), LogAppendTime),
            (&override_ct, None, CreateTime),
            (&none, Some(&bd_ct), CreateTime),
            (&none, None, NotRecorded),
        ];
        for (manifest, receipt, want) in cases {
            assert_eq!(
                recorded_timestamp_type(manifest, receipt).value,
                want,
                "manifest {manifest:?}, receipt {receipt:?}"
            );
        }
        // A receipt entry whose read failed carries no type: not recorded.
        let denied = TopicConfigCoverage {
            coverage: "captureDenied".into(),
            reason: None,
            timestamp_type: None,
        };
        assert_eq!(
            recorded_timestamp_type(&none, Some(&denied)).value,
            NotRecorded
        );
        // A value outside Kafka's two is a record of neither.
        let odd = configs(&[(TIMESTAMP_TYPE_KEY, "logappendtime")]);
        assert_eq!(recorded_timestamp_type(&odd, None).value, NotRecorded);
        // The evidence names the record, for the operator.
        assert_eq!(
            recorded_timestamp_type(&none, Some(&bd_lat)).evidence,
            "the bound backup receipt's effective message.timestamp.type LogAppendTime from \
             dynamicDefaultBrokerConfig"
        );
        assert_eq!(
            recorded_timestamp_type(&override_lat, None).evidence,
            "the archive manifest's topic override message.timestamp.type=LogAppendTime"
        );
    }

    // ------------------------------------------------------------ decide

    /// 2026-09-06T00:00:00Z and one hour later: the one segment every topic
    /// below holds, first record at T0 and last at T1.
    const T0: i64 = 1_788_652_800_000;
    const T1: i64 = T0 + 3_600_000;

    fn rfc3339(ms: i64) -> String {
        chrono::DateTime::from_timestamp_millis(ms)
            .unwrap()
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
    }

    /// A plan over `topics`, with an optional point in time, an optional
    /// `restore.time_basis`, and a sample window ending at `window_end`.
    fn plan(
        topics: &[&str],
        point: Option<i64>,
        basis: Option<&str>,
        window_end: i64,
    ) -> DrillSpec {
        let mut restore = String::new();
        if point.is_some() || basis.is_some() {
            restore.push_str("restore:\n");
            if let Some(p) = point {
                restore.push_str(&format!("  point_in_time: \"{}\"\n", rfc3339(p)));
            }
            if let Some(b) = basis {
                restore.push_str(&format!("  time_basis: {b}\n"));
            }
        }
        serde_yaml::from_str(&format!(
            "source:\n  storage: {{backend: filesystem, path: /a}}\n  topics: [{}]\n\
             target:\n  bootstrap_servers: [b:9092]\n  topic_mapping_prefix: drill-\n\
             {restore}\
             sample:\n  window_start: \"{}\"\n  window_end: \"{}\"\n\
             objectives: {{}}\n\
             evidence: {{backend: filesystem, path: /e}}\n",
            topics.join(", "),
            rfc3339(T0),
            rfc3339(window_end),
        ))
        .expect("a valid plan")
    }

    fn topic(name: &str, overrides: &[(&str, &str)]) -> TopicFacts {
        TopicFacts {
            name: name.into(),
            original_partition_count: Some(1),
            source_replication_factor: Some(1),
            configurations: configs(overrides),
            partitions: vec![crate::engine::PartitionFacts {
                partition_id: 0,
                segments: vec![crate::engine::SegmentFacts {
                    key: format!("{name}/0/0.kbak"),
                    start_offset: 0,
                    end_offset: 9,
                    start_timestamp: T0,
                    end_timestamp: T1,
                    record_count: 10,
                    sha256: String::new(),
                    uploaded_at: T1,
                }],
                gaps: vec![],
                pruned: vec![],
            }],
        }
    }

    fn facts(topics: Vec<TopicFacts>) -> BackupSetFacts {
        BackupSetFacts {
            backup_id: "set".into(),
            created_at: chrono::DateTime::from_timestamp_millis(T1).unwrap(),
            source_cluster_id: None,
            manifest_sha256: "sha256:0".into(),
            manifest_version_id: None,
            consumer_group_snapshot_sha256: None,
            topics,
        }
    }

    /// A verified receipt's coverage recording `lat` as `LogAppendTime` by the
    /// broker's DYNAMIC DEFAULT — the broker-default arm, which no manifest
    /// carries.
    fn broker_default(topic: &str, value: &str) -> SourceConfigCoverage {
        let receipt: crate::backup_receipt::BackupReceipt =
            serde_json::from_value(serde_json::json!({
                "format_version": "1.1.0",
                "run_id": "r", "backup_id": "set",
                "requested_at": "2026-08-29T00:00:00Z",
                "started_at": "2026-08-29T00:00:00Z",
                "finished_at": "2026-08-29T00:00:01Z",
                "exit_code": 0, "triggered_by": "t",
                "source": {"cluster_id": "c", "bootstrap_servers": ["b"],
                           "auth": {"mode": "plaintext", "username": null}, "topics": [topic]},
                "engine": {"id": "oso-cli", "version": "v0.21.0", "digest": "sha256:0"},
                "archive": {"manifest_key": "m", "manifest_sha256": "sha256:0", "prefix": "p"},
                "records": {topic: 10},
                "covered": {"from_ms": T0, "to_ms": T1 + 1},
                "config_coverage": {topic: {"coverage": "captured",
                    "timestamp_type": {"value": value, "source": "dynamicDefaultBrokerConfig"}}}
            }))
            .expect("a receipt");
        SourceConfigCoverage::from_receipt(&receipt)
    }

    fn names(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    /// **The topic-override arm.** A point in time over a topic whose MANIFEST
    /// records the override `LogAppendTime` is refused, naming the topic, the
    /// record and the remedy; the same plan's `CreateTime` neighbour is not
    /// what is refused.
    #[test]
    fn a_point_in_time_over_a_log_append_time_override_is_refused() {
        let spec = plan(&["lat", "orders"], Some(T0 + 1_500), None, T1);
        let f = facts(vec![
            topic("lat", &[(TIMESTAMP_TYPE_KEY, LOG_APPEND_TIME)]),
            topic("orders", &[(TIMESTAMP_TYPE_KEY, CREATE_TIME)]),
        ]);
        let err = decide(
            &spec,
            &f,
            &SourceConfigCoverage::unknown(),
            &names(&["lat", "orders"]),
        )
        .expect_err("a LogAppendTime override selected at a point is refused");
        assert!(err.starts_with("PointInTimeByProducerTime: "), "{err}");
        assert_eq!(
            crate::guard::terminal_state(&err),
            "PointInTimeByProducerTime",
            "the refusal names its terminal state, so refusal-reason= does"
        );
        assert!(
            err.contains("`lat` (the archive manifest's topic override message.timestamp.type=LogAppendTime)"),
            "{err}"
        );
        assert!(
            !err.contains("`orders`"),
            "a CreateTime topic is not refused: {err}"
        );
        assert!(err.contains("restore.time_basis: producerTime"), "{err}");
        let point = chrono::DateTime::from_timestamp_millis(T0 + 1_500).unwrap();
        assert!(
            err.contains(&format!("restore.point_in_time {}", point.to_rfc3339())),
            "{err}"
        );
    }

    /// **PROD-11.1 (review L2).** A stated window start — the interval form
    /// of `restore.point_in_time` — is a selection by time, and the refusal
    /// names the whole interval, never `sample.window_end`. KILLS: the start
    /// not counted as a time selection; the refusal naming only the end.
    #[test]
    fn a_window_start_over_a_log_append_time_topic_is_refused_and_named() {
        let mut spec = plan(&["lat"], Some(T0 + 1_500), None, T1);
        spec.restore.window_start = chrono::DateTime::from_timestamp_millis(T0 + 500);
        let f = facts(vec![topic("lat", &[(TIMESTAMP_TYPE_KEY, LOG_APPEND_TIME)])]);
        let err = decide(
            &spec,
            &f,
            &SourceConfigCoverage::unknown(),
            &names(&["lat"]),
        )
        .expect_err("a window start over a LogAppendTime topic is refused");
        let start = chrono::DateTime::from_timestamp_millis(T0 + 500).unwrap();
        let point = chrono::DateTime::from_timestamp_millis(T0 + 1_500).unwrap();
        assert!(
            err.contains(&format!(
                "restore.point_in_time {}/{}, a window with a stated start",
                start.to_rfc3339(),
                point.to_rfc3339()
            )),
            "{err}"
        );
        assert!(!err.contains("sample.window_end 2"), "{err}");
    }

    /// **The broker-default arm.** No override in the manifest; the verified
    /// receipt recorded the topic's EFFECTIVE type `LogAppendTime` from the
    /// broker's dynamic default. Refused the same way, naming that record.
    #[test]
    fn a_point_in_time_over_a_broker_default_log_append_time_is_refused() {
        let spec = plan(&["bd"], Some(T0 + 1_500), None, T1);
        let f = facts(vec![topic("bd", &[])]);
        let err = decide(
            &spec,
            &f,
            &broker_default("bd", LOG_APPEND_TIME),
            &names(&["bd"]),
        )
        .expect_err("a broker-default LogAppendTime is refused once it is recorded");
        assert!(
            err.contains("`bd` (the bound backup receipt's effective message.timestamp.type LogAppendTime from dynamicDefaultBrokerConfig)"),
            "{err}"
        );
        // ...and BEFORE it was recorded (no verified receipt: an unbound plan
        // or a receipt older than FX-4) the same archive reads NOT RECORDED:
        // the restore runs and the label says so.
        let label = decide(&spec, &f, &SourceConfigCoverage::unknown(), &names(&["bd"]))
            .expect("an unrecorded type is labelled, not refused");
        assert_eq!(label.not_recorded, names(&["bd"]));
        assert!(label.producer_time.is_empty());
    }

    /// **The opt-in.** The same two refused plans with `restore.time_basis:
    /// producerTime` run, and the label lists both topics under
    /// `producer_time` and copies the plan's value.
    #[test]
    fn the_producer_time_opt_in_runs_and_labels_both_arms() {
        let spec = plan(&["bd", "lat"], Some(T0 + 1_500), Some("producerTime"), T1);
        let f = facts(vec![
            topic("lat", &[(TIMESTAMP_TYPE_KEY, LOG_APPEND_TIME)]),
            topic("bd", &[]),
        ]);
        let label = decide(
            &spec,
            &f,
            &broker_default("bd", LOG_APPEND_TIME),
            &names(&["lat", "bd"]),
        )
        .expect("the opt-in accepts the selection");
        assert_eq!(
            label,
            TimeBasisLabel {
                plan: Some("producerTime".into()),
                producer_time: names(&["bd", "lat"]),
                not_recorded: vec![],
            }
        );
    }

    /// **The unknown case**, and the `CreateTime` control. A point in time
    /// over a topic with no recorded type runs and is labelled
    /// `not_recorded`; over a `CreateTime` topic (override or receipt) it runs
    /// and is listed nowhere — selection by producer time IS its own clock.
    #[test]
    fn an_unrecorded_type_is_labelled_and_create_time_is_not_refused() {
        let spec = plan(&["ct", "ctr", "old"], Some(T0 + 1_500), None, T1);
        let f = facts(vec![
            topic("ct", &[(TIMESTAMP_TYPE_KEY, CREATE_TIME)]),
            topic("ctr", &[]),
            topic("old", &[]),
        ]);
        let label = decide(
            &spec,
            &f,
            &broker_default("ctr", CREATE_TIME),
            &names(&["old", "ct", "ctr"]),
        )
        .expect("nothing here is refused");
        assert_eq!(
            label,
            TimeBasisLabel {
                plan: None,
                producer_time: vec![],
                not_recorded: names(&["old"]),
            }
        );
    }

    /// **A full restore still runs.** No point in time and a sample window
    /// ending at the archive's newest recorded timestamp: not a selection by
    /// time, so a `LogAppendTime` topic is neither refused nor listed. The
    /// SAME plan with a window end one millisecond earlier certainly leaves an
    /// archived record out, and is refused.
    #[test]
    fn a_full_restore_runs_and_a_window_end_that_cuts_the_archive_is_a_selection() {
        let f = facts(vec![topic("lat", &[(TIMESTAMP_TYPE_KEY, LOG_APPEND_TIME)])]);
        let full = plan(&["lat"], None, None, T1);
        assert_eq!(
            decide(
                &full,
                &f,
                &SourceConfigCoverage::unknown(),
                &names(&["lat"])
            ),
            Ok(TimeBasisLabel::default())
        );
        let cut = plan(&["lat"], None, None, T1 - 1);
        let err = decide(&cut, &f, &SourceConfigCoverage::unknown(), &names(&["lat"]))
            .expect_err("a sample.window_end inside the archive selects by time");
        assert!(err.contains("sample.window_end"), "{err}");
        assert!(selects_by_time(&cut, f.topics.first()));
        assert!(!selects_by_time(&full, f.topics.first()));
        // A stated point ALWAYS selects by time, even at the archive's end:
        // the manifest cannot prove no record lies beyond it (S6).
        assert!(selects_by_time(
            &plan(&["lat"], Some(T1), None, T1),
            f.topics.first()
        ));
    }

    /// **Review L-2 (mutant X4).** A segment whose FIRST record is later than
    /// its LAST (out-of-order timestamps, PROD-01.1 S6): the newest timestamp
    /// the manifest records is the first record's, so a `sample.window_end`
    /// between the two ends cuts the archive — a selection by time, refused
    /// for a `LogAppendTime` topic. A reader of the last timestamp alone sees
    /// the window end past it, calls the restore full, and signs nothing.
    #[test]
    fn a_segment_whose_first_record_is_the_later_one_still_bounds_the_window() {
        let mut lat = topic("lat", &[(TIMESTAMP_TYPE_KEY, LOG_APPEND_TIME)]);
        let seg = &mut lat.partitions[0].segments[0];
        seg.start_timestamp = T1;
        seg.end_timestamp = T0;
        assert_eq!(lat.newest_recorded_timestamp_ms(), Some(T1));
        let f = facts(vec![lat]);
        let between = plan(&["lat"], None, None, T1 - 1);
        assert!(selects_by_time(&between, f.topics.first()));
        let err = decide(
            &between,
            &f,
            &SourceConfigCoverage::unknown(),
            &names(&["lat"]),
        )
        .expect_err("a window end below the segment's first record cuts the archive");
        assert!(err.starts_with("PointInTimeByProducerTime: "), "{err}");
        assert!(err.contains("sample.window_end"), "{err}");
        // The control: at the first record's timestamp, the manifest shows
        // nothing beyond the window.
        let at = plan(&["lat"], None, None, T1);
        assert_eq!(
            decide(&at, &f, &SourceConfigCoverage::unknown(), &names(&["lat"])),
            Ok(TimeBasisLabel::default())
        );
    }

    /// The plan grammar: `producerTime` and nothing else parses, and an
    /// absent field is no opt-in.
    #[test]
    fn restore_time_basis_parses_one_value_and_absent_is_no_opt_in() {
        assert_eq!(
            plan(&["a"], None, Some("producerTime"), T1)
                .restore
                .time_basis,
            Some(TimeBasis::ProducerTime)
        );
        assert_eq!(
            plan(&["a"], Some(T0 + 1), None, T1).restore.time_basis,
            None
        );
        for bad in ["appendTime", "ProducerTime", "producer_time", "\"\""] {
            let yaml = format!(
                "source:\n  storage: {{backend: filesystem, path: /a}}\n  topics: [a]\n\
                 target:\n  bootstrap_servers: [b:9092]\n  topic_mapping_prefix: drill-\n\
                 restore:\n  time_basis: {bad}\n\
                 sample:\n  window_start: \"2026-08-29T00:00:00Z\"\n  window_end: \"2026-08-29T01:00:00Z\"\n\
                 objectives: {{}}\nevidence: {{backend: filesystem, path: /e}}\n"
            );
            assert!(
                serde_yaml::from_str::<DrillSpec>(&yaml).is_err(),
                "`{bad}` must not parse as a time basis"
            );
        }
        // Serialised, an absent value stays absent: a plan rendered from the
        // type (a rehearsal slot's) is byte-identical to before FX-8.
        let text = serde_yaml::to_string(&plan(&["a"], Some(T0 + 1), None, T1).restore).unwrap();
        assert!(!text.contains("time_basis"), "{text}");
        let text =
            serde_yaml::to_string(&plan(&["a"], None, Some("producerTime"), T1).restore).unwrap();
        assert!(text.contains("time_basis: producerTime"), "{text}");
    }
}
