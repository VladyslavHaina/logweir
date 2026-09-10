use crate::drill::DrillError;
use logweir_core::guard::{
    check_topic_mapping_coverage, scan_forbidden_keys, GuardRefusal,
    TERMINAL_STATE_TARGET_TOPIC_CONFIG_REFUSED,
};
use logweir_core::spec::{AllowedClusters, Anchor, DrillSpec};
use logweir_kafka::reader::{
    ClusterReader, NewTopicSpec, TopicCreator, TopicDeleter, TARGET_TOPIC_CONFIGS,
};
use std::collections::BTreeMap;

#[derive(Debug)]
pub struct Admitted {
    pub target_cluster_id: String,
    pub topic_mapping: BTreeMap<String, String>,
    /// **Guard G-TS.** What phase 0 read off the broker, and the config set it
    /// will apply to every target topic. `topics_created` is filled in by
    /// `create_target_topics` — see that function for why the creation itself
    /// cannot happen inside this phase.
    pub topic_preflight: TopicPreflight,
}

/// What phase 0 found out about the target topics before anything was written,
/// and what Logweir set on them.
///
/// **This is NOT a scorecard field** (Global Constraint 12 as amended): the
/// scorecard is frozen at 21 top-level properties and 17 required ones, and a
/// new top-level block would break `the_scorecard_top_level_shape_is_unchanged`
/// and re-open GC12 in a direction the plan's preamble forbids. It is returned
/// by phase 0 in `crate::drill::RestoreOutcome` and written to
/// `Restore.status.topicPreflight` (spec §10 G-TS) by the operator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicPreflight {
    /// The broker's effective `log.message.timestamp.type`.
    pub timestamp_type: String,
    /// The broker's `log.retention.ms`, as reported — a STRING, not an `i64`,
    /// because that is what DescribeConfigs returns and because a value this
    /// build cannot parse is a fact worth carrying verbatim rather than
    /// silently dropping to `None`.
    pub retention_ms: String,
    /// `log.message.timestamp.before.max.ms` (Kafka >= 3.6), else
    /// `log.message.timestamp.difference.max.ms` (< 3.6), when either is
    /// present and parses.
    pub timestamp_bound_ms: Option<i64>,
    /// Exactly `TARGET_TOPIC_CONFIGS`, as applied — and it IS applied to every
    /// name in `topics_created`, because `create_target_topics` pushes a name
    /// only when the broker confirmed the creation carrying this set, and
    /// `run` refuses at phase 0 rather than reusing a target that already
    /// exists. A reused topic keeps its own `retention.ms` and
    /// `message.timestamp.type`, so reporting this pair over one would be a
    /// false claim in `Restore.status.topicPreflight`.
    pub configs_set: Vec<(String, String)>,
    /// The mapped target topics THIS RUN created, in the order the broker
    /// confirmed them. Never a topic that already existed: that plan is
    /// refused at phase 0.
    pub topics_created: Vec<String>,
}

impl TopicPreflight {
    /// `TARGET_TOPIC_CONFIGS`, owned and in order. The ONE place the constant
    /// becomes a `Vec`, so no caller can reorder it on the way in.
    fn pinned_configs() -> Vec<(String, String)> {
        TARGET_TOPIC_CONFIGS
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }
}

/// The literal `message.timestamp.type` is the PER-TOPIC key; the broker-wide
/// one is `log.message.timestamp.type`. Both spellings appear below and mixing
/// them up is the whole hazard, so each is a named constant.
const BROKER_TIMESTAMP_TYPE: &str = "log.message.timestamp.type";
const BROKER_RETENTION_MS: &str = "log.retention.ms";
const BROKER_TIMESTAMP_BEFORE_MAX_MS: &str = "log.message.timestamp.before.max.ms";
const BROKER_TIMESTAMP_DIFFERENCE_MAX_MS: &str = "log.message.timestamp.difference.max.ms";
const TOPIC_TIMESTAMP_TYPE: &str = "message.timestamp.type";
const LOG_APPEND_TIME: &str = "LogAppendTime";
const CREATE_TIME: &str = "CreateTime";

/// Opens every G-TS refusal message, so `logweir_core::guard::terminal_state`
/// — which matches a PREFIX and the `": "` separator with it — classifies the
/// run as `TargetTopicConfigRefused` and the runner's final stdout line reads
/// `refusal-reason=TargetTopicConfigRefused` (interface **I9**, exit 3).
fn target_topic_refusal(detail: String) -> DrillError {
    GuardRefusal(format!(
        "{TERMINAL_STATE_TARGET_TOPIC_CONFIG_REFUSED}: {detail}"
    ))
    .into()
}

/// Every check here is OBSERVED. The `spec_text` argument is the raw file
/// bytes, not the parsed struct, so a forbidden key survives round-tripping.
///
/// Returns `DrillError`, not a bare `GuardRefusal`: a reader that cannot be
/// reached or cannot answer (`KafkaError`, via `DrillError::Kafka`) is an
/// OPERATIONAL failure — the plan itself may be fine and the correct action
/// is to retry — and must map to exit 1, never to exit 3's "the plan is
/// refused." Only `GuardRefusal` (via `DrillError::Guard`) means the guard
/// looked at something it could read and refused to proceed.
pub fn run(
    spec: &DrillSpec,
    spec_text: &str,
    allowed: &AllowedClusters,
    reader: &dyn ClusterReader,
    creator: &dyn TopicCreator,
    deleter: &dyn TopicDeleter,
) -> Result<Admitted, DrillError> {
    // `?` on purpose: the scan FAILS CLOSED. A spec text this scanner cannot
    // parse is a spec it did not scan, and that is a `GuardRefusal` (exit 3),
    // never an empty result silently treated as clean.
    let bad = scan_forbidden_keys(spec_text)?;
    if !bad.is_empty() {
        return Err(GuardRefusal(format!(
            "forbidden key(s) present in the drill spec, at any value: {}. \
             purge_topics is irreversible, absent from the engine's dry run, has no \
             confirmation gate and truncates EVERY partition of each target topic \
             regardless of partition or time-window filters; dry_run would make the \
             restore a no-op and the measured RTO meaningless; header_preflight_external \
             would silently disable the header scan the drill depends on.",
            bad.join(", ")
        ))
        .into());
    }

    // The two PURELY LOCAL checks run first, before any network round trip. A
    // local refusal should not need a reachable broker, and putting them first
    // is what lets `guard_cli.rs` distinguish "refused by the mapping guard"
    // from "refused because the broker was down".
    let topic_mapping: BTreeMap<String, String> = spec
        .source
        .topics
        .iter()
        .map(|t| {
            (
                t.clone(),
                format!("{}{t}", spec.target.topic_mapping_prefix),
            )
        })
        .collect();
    // `check_topic_mapping_coverage` returns `Result<(), GuardRefusal>`; `?`
    // converts it into `DrillError::Guard` via the `#[from]` impl.
    check_topic_mapping_coverage(&spec.source.topics, &topic_mapping)?;

    // **G-GLOB and G-EXP, at phase 0.** Both are also enforced by
    // `render_restore::render`, and that is NOT where a plan gets refused.
    //
    // This arm is Task 2's review finding F2. Before it, a wildcard such as
    // `orders*` in `source.topics` was ADMITTED here and refused only in
    // `preflight` — at phase 5, as `EngineError::Operational`, which is exit
    // **1** with no `refusal-reason=` line. Ruling R-E is right that a
    // renderer refusal reached at phase 5 is exit 1 ("by phase 6 phases 0-5
    // have run", `logweir-engine-oso/src/engine.rs:243-250`), and that ruling
    // is not reopened here. The defect was the missing arm, not the mapping:
    // Global Constraint 11 reserves exit 3 for a plan refused BEFORE anything
    // runs, and a glob in a spec is exactly that — an adopter-written string
    // this drill will never accept, knowable with no broker, no bucket and no
    // engine. So it is refused here, where the exit code is 3 and the
    // `refusal-reason=GuardRefused` line is emitted, and phase 5 keeps its
    // fail-closed backstop for a value that reached a renderer by some other
    // route.
    //
    // BOTH SIDES, exactly as the renderer checks both: the keys become
    // `target.topics.include` entries and the values become
    // `restore.topic_mapping` targets, which the engine also treats as topic
    // selectors. Checking only the keys would leave the half an operator is
    // more likely to template — and the values are the half that carries
    // `target.topic_mapping_prefix`, so a prefix of `drill-*` is refused here
    // and nowhere else in phase 0.
    //
    // **G-EXP is checked BEFORE G-GLOB**, in the same order and for the same
    // reason as the renderers (`logweir_engine_oso::yaml::reject_dollar_brace`):
    // `${` contains `{` and `}`, two of `GLOB_METACHARACTERS`, so a
    // glob-first order reports `orders${X}` as a pattern and sends the
    // operator to escape a brace instead of to stop an expansion.
    let sources: Vec<String> = topic_mapping.keys().cloned().collect();
    let targets: Vec<String> = topic_mapping.values().cloned().collect();
    for (side, entries) in [("source topic", &sources), ("mapped target", &targets)] {
        // **G-EXP** at the spec layer. The renderer refuses a `${` at every
        // interpolation site (`logweir_engine_oso::yaml::yaml_scalar_checked`)
        // and sweeps the finished document, but that is a phase-5 refusal and
        // exit 1. The hazard is not only an attacker: the engine expands
        // `${NAME}` over the whole config as raw text before parsing and
        // replaces an UNSET name with the EMPTY STRING behind a warning
        // [U:crates/kafka-backup-cli/src/commands/config.rs:1-34], so a topic
        // named `orders${X}` becomes `orders` and the drill restores a
        // different topic than the plan named — while every hash still
        // matches, because the bytes logweir hashed are a template and the
        // bytes the engine executed are its expansion.
        if let Some(entry) = entries.iter().find(|e| e.contains("${")) {
            return Err(GuardRefusal(format!(
                "{side} `{entry}` contains `${{`, which the engine expands textually BEFORE the \
                 config is parsed [U:crates/kafka-backup-cli/src/commands/config.rs:1-34], \
                 replacing an unset name with the empty string behind nothing but a warning. The \
                 document logweir hashes would be a TEMPLATE and the document the engine executes \
                 its expansion, so the approved plan_hash would not cover what runs."
            ))
            .into());
        }
        if let Err(entry) = logweir_core::guard::reject_glob_metacharacters(entries) {
            return Err(GuardRefusal(format!(
                "{side} `{entry}` contains a glob metacharacter (one of {}); the engine's \
                 TopicSelection treats include entries as GLOB PATTERNS \
                 [U/kafka-backup/crates/kafka-backup-core/src/config.rs:334-343], so one named \
                 entry would silently widen to a set and the drill would restore topics the \
                 approved plan never named. GC18(c) rail 1 is a mandatory named-topic allowlist \
                 with no wildcard; if the topic is genuinely called that, this build cannot \
                 restore it.",
                logweir_core::guard::GLOB_METACHARACTERS
                    .iter()
                    .collect::<String>()
            ))
            .into());
        }
    }

    // v0.1 implements `head` and refuses the other two, HERE, before anything
    // runs. `logweir_core::spec::Anchor`'s doc comment carries the full
    // reasoning and the measurement; the short version is that phase 4 honours
    // the anchor when choosing which ARCHIVE records to fingerprint while
    // `phase7_verify::verdict_for_selection` reads the TARGET's first
    // `records_per_partition` records, so `tail` and `random` reconcile two
    // different sets of records and report a healthy backup as broken.
    //
    // A REFUSAL, not a downgrade to `head`: silently running a different
    // sample than the approved plan named is the same class of dishonesty as
    // silently restoring a different backup set (`pick_backup_set` refuses
    // that too), and the scorecard would then record an anchor the drill did
    // not apply.
    if spec.sample.anchor != Anchor::Head {
        return Err(GuardRefusal(format!(
            "sample.anchor `{}` is not supported in v0.1; only `head` is. Phase 7 reconciles \
             the restored topic by reading its FIRST sample.records_per_partition records, \
             while `{}` selects archive records from elsewhere in the window — the two would \
             compare different records and report a healthy backup as a failure. Set \
             `sample.anchor: head`, or omit the field (that is now its default). Refusing \
             rather than silently sampling `head` under a plan that asked for `{}`.",
            spec.sample.anchor, spec.sample.anchor, spec.sample.anchor
        ))
        .into());
    }

    // FROM HERE ON every failure reaches the network. A `KafkaError` here
    // means the guard could not observe the fact it needed — it is NOT a
    // refusal, and `?` converts it into `DrillError::Kafka` (exit 1), not
    // `DrillError::Guard` (exit 3).
    let target_cluster_id = reader.cluster_id()?;
    if !allowed.allowed_cluster_ids.contains(&target_cluster_id) {
        return Err(GuardRefusal(format!(
            "target cluster id {target_cluster_id} is not in allowedClusterIds"
        ))
        .into());
    }
    if allowed.source_cluster_id.as_deref() == Some(target_cluster_id.as_str()) {
        return Err(GuardRefusal(format!(
            "target cluster id {target_cluster_id} equals the source cluster id"
        ))
        .into());
    }

    let topics = reader.list_topics()?;
    // The marker topic must be CONFIRMED HEALTHY, not merely named in the
    // list: `logweir_kafka::reader::TopicMeta`'s own doc comment assigns this
    // caller the job of checking `error.is_none()` rather than trusting bare
    // presence — a topic mid-leader-election or one this principal cannot
    // describe still appears in `list_topics`'s output (by that same
    // contract) carrying `partitions: 0` and an `error`, and admitting on
    // name alone would let that meaningless metadata reach a later phase.
    match topics.iter().find(|t| t.name == spec.target.marker_topic) {
        Some(t) if t.error.is_none() => {}
        Some(t) => {
            return Err(GuardRefusal(format!(
                "marker topic `{}` exists on cluster {target_cluster_id} but its metadata \
                 carried an error, so its presence cannot be confirmed healthy: {}. \
                 Its existence is the v0.1 segregation proof — recreate it healthy on the \
                 SCRATCH cluster only.",
                spec.target.marker_topic,
                t.error.as_deref().unwrap_or("<no detail>")
            ))
            .into());
        }
        None => {
            return Err(GuardRefusal(format!(
                "marker topic `{}` does not exist on cluster {target_cluster_id}. \
                 Create it on the SCRATCH cluster only — its existence is the v0.1 \
                 segregation proof.",
                spec.target.marker_topic
            ))
            .into());
        }
    }

    // **Spec §6.1: "A `Restore` refuses if any mapped target topic already
    // exists."** Twice stated there — once as the reason `broker_configs` and
    // not `topic_configs` is the preflight's instrument ("the mapped target
    // topics do not exist at phase 0 (a `Restore` refuses if any of them
    // does)"), once as the rule itself under **New-topic naming**, with the
    // measurement: appending into a half-populated topic is the false-pass
    // class measured at `e2e-seed.sh:81-91`, where a second run left the
    // manifest describing 2048 records while the broker held 6000.
    //
    // **A refusal, not a reuse.** Reusing the topic was the behaviour this
    // task shipped first, and it made `TopicPreflight.configs_set` a false
    // claim: the pinned pair is reported "as applied" while a reused topic
    // keeps whatever `retention.ms` and `message.timestamp.type` it had —
    // `Restore.status.topicPreflight` would then say `retention.ms=-1` over a
    // target on `604800000`, exactly the silent-failure class G-TS exists to
    // prevent. There is no way to make the claim true after the fact either:
    // `TopicCreator` creates, it does not alter, and `alter_configs` is
    // surface `logweir-kafka` deliberately does not have.
    //
    // **HERE, and not at the creation step**, because this is where exit 3
    // still means what Global Constraint 11 says it means: refused before
    // anything ran, before phase 1 consumes the approval, before the archive
    // is opened, with nothing created and no client write of any kind. The
    // topic list is the one `list_topics()` read above — no extra round trip,
    // and the same metadata the marker check just used.
    //
    // The refusal message does NOT open with a terminal state, so
    // `logweir_core::guard::terminal_state` classifies it as the general
    // `GuardRefused` and the runner's last stdout line reads
    // `refusal-reason=GuardRefused`. `TargetTopicConfigRefused` is this task's
    // state for a target CONFIGURATION this build refuses (the two arms in
    // `target_topic_preflight`); a target that merely exists is not a
    // configuration finding, spec §3.2 names no state of its own for it, and
    // widening the declared state to cover it would make the terminal state a
    // worse signal for the operator, not a better one.
    let already_present: Vec<&String> = topic_mapping
        .values()
        .filter(|t| topics.iter().any(|m| &m.name == *t))
        .collect();
    if !already_present.is_empty() {
        return Err(GuardRefusal(format!(
            "mapped target topic(s) already exist on cluster {target_cluster_id}: {}. A restore \
             appends into them, so the drill would reconcile the archive against records it did \
             not write, and their message.timestamp.type and retention.ms are whatever they were \
             created with — this build sets the pinned pair by CREATING each target and cannot \
             alter an existing one. Delete them on the SCRATCH cluster, or restore under a \
             target.topic_mapping_prefix nothing has used yet.",
            already_present
                .iter()
                .map(|t| format!("`{t}`"))
                .collect::<Vec<_>>()
                .join(", ")
        ))
        .into());
    }

    // **Guard G-TS.** The target-topic preflight, after the cluster-identity,
    // marker and target-absence checks and before anything else. The absence
    // check comes FIRST on purpose: the `LogAppendTime` arm below creates the
    // first mapped target name as its probe, and it may only do that to a name
    // this phase has just proved absent.
    let topic_preflight = target_topic_preflight(spec, &topic_mapping, reader, creator, deleter)?;

    Ok(Admitted {
        target_cluster_id,
        topic_mapping,
        topic_preflight,
    })
}

/// **Guard G-TS**, the refusing half.
///
/// Three target-topic settings destroy a restore silently, and none of them is
/// observed by anything that ships today:
///
/// 1. `retention.ms` on cluster defaults (typically `604800000`) — restoring an
///    older point writes segments already past the deletion threshold, removed
///    on the next retention check, possibly AFTER phase 7 signed a `pass`.
/// 2. `message.timestamp.difference.max.ms` (Kafka < 3.6) /
///    `message.timestamp.before.max.ms` (>= 3.6) — rejects a CreateTime too far
///    in the past. Unbounded by Apache default, commonly tightened, and set by
///    MSK cluster configurations.
/// 3. `message.timestamp.type = LogAppendTime` — overwrites every restored
///    timestamp with the restore's wall clock, voiding any timestamp lookup.
///
/// The obvious instrument is the WRONG one. `ClusterReader::topic_configs`
/// describes a topic that does not exist until Logweir creates it — the mapped
/// target topics are absent at phase 0 by construction — so a fixture-driven
/// test over it would guard nothing. That is why spec §10 marks G-TS
/// `[EDIT-DERIVED]`, and why this reads `broker_configs` instead.
///
/// # A `KafkaError` here is exit 1, not exit 3
///
/// `broker_configs()?` maps into `DrillError::Kafka`. A broker that cannot
/// answer DescribeConfigs has told us nothing about the plan; the plan may be
/// perfectly fine and the correct action is to retry. Only a value we could
/// READ and refuse is a `GuardRefusal`.
///
/// # The one write phase 0 performs, and why it is undone
///
/// The `LogAppendTime` arm cannot be answered by reading: whether a broker on
/// `LogAppendTime` honours a per-topic `message.timestamp.type` override is
/// exactly spec §17 residual 3, and the only way to find out is to ask this
/// broker. So that arm — and ONLY that arm, i.e. only when the broker actually
/// reports `LogAppendTime` — creates the first mapped target topic with
/// `TARGET_TOPIC_CONFIGS` and reads it back.
///
/// The probe topic is then deleted on BOTH branches, not only on the refusing
/// one: at phase 0 the manifest's partition count is not known (see
/// `create_target_topics`), so a probe topic left behind would be a
/// one-partition target for an N-partition source — precisely the silent
/// restore failure this guard exists to prevent. Deleting it means phase 0
/// leaves the target exactly as it found it, which is what exit 3's "refused
/// before anything ran" claims.
fn target_topic_preflight(
    spec: &DrillSpec,
    topic_mapping: &BTreeMap<String, String>,
    reader: &dyn ClusterReader,
    creator: &dyn TopicCreator,
    deleter: &dyn TopicDeleter,
) -> Result<TopicPreflight, DrillError> {
    // 1 — read the BROKER's defaults.
    let broker = reader.broker_configs()?;
    let timestamp_type = broker
        .get(BROKER_TIMESTAMP_TYPE)
        .cloned()
        // The Apache default [VERIFIED kafka 3.7 server.properties reference].
        // An absent key is not a positive observation of a hostile setting, and
        // the per-topic `message.timestamp.type = CreateTime` this task pins on
        // every created topic is what actually decides the topic's behaviour;
        // refusing on absence would refuse every broker that does not surface
        // the key while protecting nothing extra.
        .unwrap_or_else(|| CREATE_TIME.to_string());
    let retention_ms = broker.get(BROKER_RETENTION_MS).cloned().unwrap_or_default();
    // `before.max.ms` first: on Kafka >= 3.6 BOTH keys are reported and
    // `difference.max.ms` is the deprecated one.
    let timestamp_bound_ms = broker
        .get(BROKER_TIMESTAMP_BEFORE_MAX_MS)
        .or_else(|| broker.get(BROKER_TIMESTAMP_DIFFERENCE_MAX_MS))
        .and_then(|v| v.trim().parse::<i64>().ok());

    let preflight = TopicPreflight {
        timestamp_type: timestamp_type.clone(),
        retention_ms,
        timestamp_bound_ms,
        configs_set: TopicPreflight::pinned_configs(),
        topics_created: Vec::new(),
    };

    // 2 — the timestamp bound. A PURE comparison, so it runs before the arm
    // that writes: creating a probe topic on a target for a plan that is about
    // to be refused anyway would contradict exit 3's own meaning.
    //
    // `saturating_sub` is load-bearing, not defensive: the Apache default for
    // both bound keys is `9223372036854775807`, so a plain `-` overflows and
    // panics in debug and wraps to a floor in the FUTURE in release — which
    // would refuse every window on every default broker.
    if let Some(bound) = timestamp_bound_ms {
        // The window END this plan will actually ask the engine for, which is
        // `spec.restore.point_in_time` when the spec states one and
        // `spec.sample.window_end` otherwise — the same choice
        // `crate::drill::build_plan_with_floor` makes for `time_window.1`.
        // Reading `sample.window_end` unconditionally would check a bound
        // against a timestamp this restore never requests.
        //
        // The window START is deliberately NOT checked here: since Task 9 it
        // is the ARCHIVE's floor and not a spec field (guard **G-WIN**), and a
        // broker's `message.timestamp.before.max.ms` refusal is about how far
        // in the past a record may be — a window whose end is inside the bound
        // but whose start is not is a partial refusal the engine reports per
        // record, not a plan this guard can adjudicate before anything runs.
        let (window_end_field, window_end) = match spec.restore.point_in_time {
            Some(t) => ("restore.point_in_time", t),
            None => ("sample.window_end", spec.sample.window_end),
        };
        let window_end_ms = window_end.timestamp_millis();
        let oldest_accepted_ms = chrono::Utc::now().timestamp_millis().saturating_sub(bound);
        if window_end_ms < oldest_accepted_ms {
            return Err(target_topic_refusal(format!(
                "the target broker bounds how far in the past a CreateTime record may be \
                 ({bound} ms, so nothing older than epoch-ms {oldest_accepted_ms} is accepted), \
                 and this plan's {window_end_field} is epoch-ms {window_end_ms}. The engine \
                 produces every restored record with its ORIGINAL timestamp \
                 [U:crates/kafka-backup-core/src/kafka/produce.rs:101-104], so the broker would \
                 reject the whole window. Raise message.timestamp.before.max.ms (or, before \
                 Kafka 3.6, message.timestamp.difference.max.ms) on the target, or restore a \
                 more recent window."
            )));
        }
    }

    // 3 — `LogAppendTime`, and the per-topic override.
    if timestamp_type != LOG_APPEND_TIME {
        return Ok(preflight);
    }
    let Some(probe) = topic_mapping.values().next().cloned() else {
        // No mapped target topic at all. `check_topic_mapping_coverage` above
        // has already refused an unmapped SELECTED topic, so this is reachable
        // only from a spec that selects nothing — there is nothing to probe and
        // nothing to restore either.
        return Ok(preflight);
    };
    let spec_probe = NewTopicSpec {
        name: probe.clone(),
        // ONE partition. The real count is the manifest's and is not known
        // here; this topic exists for one DescribeConfigs read and is deleted
        // immediately below, so the count is not a fact about the restore.
        num_partitions: 1,
        replication_factor: i32::from(spec.target.default_replication_factor),
        configs: TopicPreflight::pinned_configs(),
    };
    let created = creator.create_topics(std::slice::from_ref(&spec_probe))?;
    if let Some((name, Err(e))) = created.iter().find(|(_, r)| r.is_err()) {
        return Err(target_topic_refusal(format!(
            "the target broker reports {BROKER_TIMESTAMP_TYPE}={LOG_APPEND_TIME}, which \
             overwrites every restored record's timestamp with the restore's wall clock, and the \
             per-topic override could not be tested because creating `{name}` failed: {e}"
        )));
    }
    let readback = reader.topic_configs(&probe);
    let delete = deleter.delete_topics(std::slice::from_ref(&probe));
    // The probe topic is gone before any verdict is returned, so no path out of
    // here leaves it behind. A deletion that was REFUSED (a degenerate
    // `target.topic_mapping_prefix` disables `RdKafkaReader::delete_topics`
    // entirely) is itself a refusal, naming the topic an operator now has to
    // remove by hand.
    match &delete {
        Ok(results) => {
            if let Some((name, Err(e))) = results.iter().find(|(_, r)| r.is_err()) {
                return Err(target_topic_refusal(format!(
                    "the {LOG_APPEND_TIME} override probe created target topic `{name}` and could \
                     not delete it again: {e}. Delete `{name}` on the target before re-running: \
                     it was created with one partition for the probe and is NOT the topic this \
                     restore needs."
                )));
            }
        }
        Err(e) => {
            return Err(target_topic_refusal(format!(
                "the {LOG_APPEND_TIME} override probe created target topic `{probe}` and the \
                 delete call itself failed: {e}. Delete `{probe}` on the target before \
                 re-running: it was created with one partition for the probe and is NOT the \
                 topic this restore needs."
            )));
        }
    }
    let effective = readback?
        .get(TOPIC_TIMESTAMP_TYPE)
        .cloned()
        .unwrap_or_else(|| timestamp_type.clone());
    if effective == LOG_APPEND_TIME {
        return Err(target_topic_refusal(format!(
            "the target broker reports {BROKER_TIMESTAMP_TYPE}={LOG_APPEND_TIME} and REFUSED a \
             per-topic {TOPIC_TIMESTAMP_TYPE}={CREATE_TIME} override: `{probe}` read back \
             {TOPIC_TIMESTAMP_TYPE}={effective}. Every restored record's timestamp would be \
             replaced by the restore's wall clock, so the drill could not verify a single \
             timestamp and any point-in-time claim over the result would be false. Restore into a \
             cluster whose {BROKER_TIMESTAMP_TYPE} is {CREATE_TIME}."
        )));
    }
    Ok(preflight)
}

/// **Guard G-TS**, the creating half: every mapped target topic, created by
/// LOGWEIR, with exactly `TARGET_TOPIC_CONFIGS`, the source topic's partition
/// count and `spec.target.default_replication_factor`.
///
/// The rendered `restore.yaml` says `create_topics: false`, so this is the only
/// thing that creates them. The engine's own creation path carries no
/// configuration at all —
/// `TopicToCreate { name, num_partitions, replication_factor }`
/// [U:crates/kafka-backup-core/src/restore/engine.rs:1447-1455] — which is
/// exactly why it cannot be trusted with the two settings that decide whether
/// the restored records survive being written.
///
/// # Why this is not inside `run`
///
/// The brief for this task places the creation step inside phase 0. It cannot
/// be there, for three independent reasons, each of which has a test in this
/// tree today:
///
/// 1. **Phase 1 verifies the approval.** Creating every mapped target topic at
///    phase 0 would let an UNAPPROVED plan write to the target cluster, and
///    exit 3 promises the opposite ("refused before anything ran").
/// 2. **Phases 2 and 3 read the target's PRE-EXISTING state.**
///    `phase3_diff::run` reports an already-present target topic as a
///    `Collision` in the signed scorecard; creating first would turn every
///    target topic into a self-inflicted collision.
/// 3. **The partition count is a fact of the archive MANIFEST**
///    (`BackupSetFacts::topics[].partitions`, via
///    `phase3_diff::restore_partition_count`), and the archive is opened in
///    `execute_with` AFTER phases 0 and 1 by design — `Ctx`'s own doc comment
///    says so and `crates/logweir/tests/guard_cli.rs` pins the consequence,
///    because a refused plan must never open the bucket. Creating at phase 0
///    would mean creating at a count nobody had read yet, and a one-partition
///    target for a three-partition source loses two thirds of the restore
///    silently.
///
/// So `run` keeps the OBSERVATION and both REFUSALS — the guard, at phase 0,
/// exit 3, before anything runs — and this runs from `execute_with` after the
/// phase-5 verdict and immediately before phase 6.
///
/// **After phase 5, not before it**, for a fourth reason found by execution:
/// phase 5's `Verdict::Block` branch returns without a teardown on the stated
/// ground that a blocked preflight means the restore never ran and the drill
/// created nothing on the target, and
/// `e2e/tests/full_drill.rs`'s
/// `a_corrupted_segment_yields_exit_2_and_a_signed_preflight_failed_scorecard`
/// asserts exactly that (`!topic_exists("drill-orders")`). It failed against a
/// version of this call placed before phase 5. The engine's preflight is
/// `validate-restore`, which force-sets `dry_run = true` and writes nothing, so
/// it needs no target topic; `restore` does.
///
/// # Every per-topic failure here is `Operational`, including "already exists"
///
/// `run` has already REFUSED, at phase 0 and with exit 3, any plan whose
/// mapped target topics existed (spec §6.1). So a broker answering
/// `TopicAlreadyExists` at this point means the target changed under the run
/// after admission — someone else created it, or a previous drill's teardown
/// completed late — and the topic's `message.timestamp.type` and
/// `retention.ms` are then NOT the pinned pair, whatever `configs_set` says.
/// Continuing would append into it and make that claim false, which is the
/// silent-failure class this guard exists to prevent.
///
/// It is `DrillError::Operational` (exit 1, no artifact) rather than exit 3
/// because by here phases 0-5 have run: Global Constraint 11 reserves exit 3
/// for a plan refused BEFORE anything runs, and the recorded ruling in
/// `docs/stability.md` ("a phase-5 / phase-6 divergence is exit 1, not exit
/// 3") is the same boundary. Nothing about the archive was established and the
/// fix is on the cluster.
///
/// Consequently `TopicPreflight.topics_created` names ONLY topics this run
/// created, and `configs_set` describes what was applied to every one of them
/// —  the property `topics_created_names_only_the_topics_this_run_created`
/// asserts.
pub fn create_target_topics(
    creator: &dyn TopicCreator,
    topic_mapping: &BTreeMap<String, String>,
    facts: &logweir_core::engine::BackupSetFacts,
    default_replication_factor: i16,
    preflight: &mut TopicPreflight,
) -> Result<(), DrillError> {
    let mut specs: Vec<NewTopicSpec> = Vec::new();
    for t in &facts.topics {
        let Some(dst) = topic_mapping.get(&t.name) else {
            continue;
        };
        specs.push(NewTopicSpec {
            name: dst.clone(),
            num_partitions: super::phase3_diff::restore_partition_count(t),
            replication_factor: i32::from(default_replication_factor),
            // `TARGET_TOPIC_CONFIGS`, in order, on EVERY topic. Not a default
            // set, not a broker default, and not conditional on what the
            // broker reported: `configs_set` in the preflight is the claim
            // that this is what was applied.
            configs: TopicPreflight::pinned_configs(),
        });
    }
    if specs.is_empty() {
        return Ok(());
    }
    // The slice outlives the `NewTopic`s built from it inside the impl — see
    // `RdKafkaReader::create_topics`, which cannot compile otherwise.
    let results = creator.create_topics(&specs)?;
    for (name, r) in results {
        match r {
            Ok(()) => preflight.topics_created.push(name),
            Err(e) => {
                return Err(DrillError::Operational(format!(
                    "target topic `{name}` could not be created with the pinned configuration \
                     ({}): {e}",
                    TARGET_TOPIC_CONFIGS
                        .iter()
                        .map(|(k, v)| format!("{k}={v}"))
                        .collect::<Vec<_>>()
                        .join(", ")
                )))
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use logweir_core::engine::StorageUrl;
    use logweir_core::spec::{Notifications, ObjectivesSpec, SampleSpec, SourceSpec, TargetSpec};
    use logweir_kafka::reader::{ConsumedRecord, KafkaError, TopicMeta};

    fn ts(s: &str) -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::parse_from_rfc3339(s)
            .unwrap()
            .with_timezone(&chrono::Utc)
    }

    fn spec_with(topics: &[&str], prefix: &str) -> DrillSpec {
        DrillSpec {
            name: None,
            source: SourceSpec {
                storage: StorageUrl::Filesystem {
                    path: "/tmp/src".into(),
                },
                backup: "latestCompleted".into(),
                topics: topics.iter().map(|t| t.to_string()).collect(),
            },
            target: TargetSpec {
                bootstrap_servers: vec!["localhost:9092".into()],
                auth: logweir_core::spec::AuthSpec::Plaintext,
                marker_topic: "logweir.scratch".into(),
                topic_mapping_prefix: prefix.into(),
                default_replication_factor: 1,
                teardown: "delete".into(),
            },
            sample: SampleSpec {
                window_start: ts("2026-08-29T00:00:00Z"),
                window_end: ts("2026-08-30T00:00:00Z"),
                records_per_partition: 25,
                anchor: Anchor::Head,
                max_partitions: None,
            },
            restore: logweir_core::spec::RestoreSpecBlock::default(),
            objectives: ObjectivesSpec {
                rto_seconds: None,
                rpo_seconds: None,
                pass_rate: None,
            },
            evidence: StorageUrl::Filesystem {
                path: "/tmp/evidence".into(),
            },
            engine_overrides: Default::default(),
            notifications: Notifications::default(),
        }
    }

    fn allowed(ids: &[&str], source: Option<&str>) -> AllowedClusters {
        AllowedClusters {
            allowed_cluster_ids: ids.iter().map(|s| s.to_string()).collect(),
            source_cluster_id: source.map(str::to_string),
        }
    }

    /// A `ClusterReader` double whose every response is configured directly
    /// by the test, scoped to this file only. The SHARED `FakeReader` double
    /// in `crates/logweir/tests/fixtures/mod.rs` is deferred to Task 16
    /// (behind `TargetState`, per addendum A4); this stub carries no such
    /// dependency, so it can prove phase 0's cluster-identity checks now
    /// rather than waiting six tasks for a shared double to exist.
    struct StubReader {
        cluster_id: Result<String, KafkaError>,
        topics: Result<Vec<TopicMeta>, KafkaError>,
    }

    impl ClusterReader for StubReader {
        fn cluster_id(&self) -> Result<String, KafkaError> {
            self.cluster_id.clone()
        }
        fn list_topics(&self) -> Result<Vec<TopicMeta>, KafkaError> {
            self.topics.clone()
        }
        fn end_offsets(&self, _topic: &str) -> Result<Vec<(i32, i64)>, KafkaError> {
            Ok(vec![])
        }
        fn topic_configs(&self, _topic: &str) -> Result<BTreeMap<String, String>, KafkaError> {
            Ok(BTreeMap::new())
        }
        /// An empty broker-config map: `target_topic_preflight` then reads no
        /// `log.message.timestamp.type` (so it treats the broker as the Apache
        /// default, `CreateTime`) and no timestamp bound, which is exactly what
        /// the identity, marker and anchor tests in this module want — they
        /// prove the checks that run BEFORE the preflight, and would be worse
        /// tests if a hostile broker value could interfere. Guard **G-TS**'s
        /// own four arms live in `crates/logweir/tests/topic_preflight.rs`,
        /// over doubles built for it.
        fn broker_configs(&self) -> Result<BTreeMap<String, String>, KafkaError> {
            Ok(BTreeMap::new())
        }
        fn consume_range(
            &self,
            _topic: &str,
            _partition: i32,
            _from: i64,
            _max: usize,
        ) -> Result<Vec<ConsumedRecord>, KafkaError> {
            Ok(vec![])
        }
    }

    /// Task 8, guard **G-TS**. Phase 0 now takes a `TopicCreator` and a
    /// `TopicDeleter` because its target-topic preflight needs them for the one
    /// case that cannot be answered by reading — a broker on `LogAppendTime`.
    /// Every test in THIS module runs against a broker that reports nothing, so
    /// neither of these may be reached; both panic rather than returning
    /// something plausible, so a preflight that started writing on the ordinary
    /// path would fail here instead of passing quietly.
    struct NoopCreator;
    impl logweir_kafka::reader::TopicCreator for NoopCreator {
        fn create_topics(
            &self,
            topics: &[logweir_kafka::reader::NewTopicSpec],
        ) -> Result<Vec<(String, Result<(), String>)>, KafkaError> {
            panic!(
                "phase 0 must create nothing on a broker that is not LogAppendTime; asked for \
                 {topics:?}"
            )
        }
    }

    struct NoopDeleter;
    impl logweir_kafka::reader::TopicDeleter for NoopDeleter {
        fn delete_topics(
            &self,
            names: &[String],
        ) -> Result<Vec<(String, Result<(), String>)>, KafkaError> {
            panic!("phase 0 must delete nothing here; asked for {names:?}")
        }
    }

    fn healthy_reader(cluster_id: &str, marker_topic: &str) -> StubReader {
        StubReader {
            cluster_id: Ok(cluster_id.to_string()),
            topics: Ok(vec![TopicMeta::new(marker_topic, 1)]),
        }
    }

    #[test]
    fn a_healthy_target_is_admitted() {
        let spec = spec_with(&["orders"], "drill-");
        let reader = healthy_reader("ALLOWED0000000000000000", &spec.target.marker_topic);
        let admitted = run(
            &spec,
            "restore: {}\n",
            &allowed(&["ALLOWED0000000000000000"], None),
            &reader,
            &NoopCreator,
            &NoopDeleter,
        )
        .unwrap();
        assert_eq!(admitted.target_cluster_id, "ALLOWED0000000000000000");
        assert_eq!(
            admitted.topic_mapping.get("orders"),
            Some(&"drill-orders".to_string())
        );
    }

    #[test]
    fn a_target_cluster_not_in_allowed_cluster_ids_is_a_guard_refusal() {
        let spec = spec_with(&["orders"], "drill-");
        let reader = healthy_reader("WRONG0000000000000000000", &spec.target.marker_topic);
        let err = run(
            &spec,
            "restore: {}\n",
            &allowed(&["ALLOWED0000000000000000"], None),
            &reader,
            &NoopCreator,
            &NoopDeleter,
        )
        .unwrap_err();
        match err {
            DrillError::Guard(GuardRefusal(msg)) => {
                assert!(msg.contains("not in allowedClusterIds"), "{msg}")
            }
            other => panic!("expected a guard refusal (exit 3), got {other:?}"),
        }
    }

    #[test]
    fn a_target_cluster_equal_to_the_source_cluster_is_a_guard_refusal() {
        let spec = spec_with(&["orders"], "drill-");
        let reader = healthy_reader("SAME0000000000000000000A", &spec.target.marker_topic);
        let err = run(
            &spec,
            "restore: {}\n",
            &allowed(
                &["SAME0000000000000000000A"],
                Some("SAME0000000000000000000A"),
            ),
            &reader,
            &NoopCreator,
            &NoopDeleter,
        )
        .unwrap_err();
        match err {
            DrillError::Guard(GuardRefusal(msg)) => {
                assert!(msg.contains("equals the source cluster id"), "{msg}")
            }
            other => panic!("expected a guard refusal (exit 3), got {other:?}"),
        }
    }

    #[test]
    fn a_missing_marker_topic_is_a_guard_refusal_naming_it() {
        let spec = spec_with(&["orders"], "drill-");
        let reader = StubReader {
            cluster_id: Ok("ALLOWED0000000000000000".into()),
            topics: Ok(vec![]),
        };
        let err = run(
            &spec,
            "restore: {}\n",
            &allowed(&["ALLOWED0000000000000000"], None),
            &reader,
            &NoopCreator,
            &NoopDeleter,
        )
        .unwrap_err();
        match err {
            DrillError::Guard(GuardRefusal(msg)) => {
                assert!(msg.contains(&spec.target.marker_topic), "{msg}");
                assert!(msg.contains("does not exist"), "{msg}");
            }
            other => panic!("expected a guard refusal (exit 3), got {other:?}"),
        }
    }

    /// The marker topic's NAME is present but its metadata carried an error
    /// (leader election, an authorization gap, etc.) — `error.is_none()`
    /// must be checked, not just the name, and the error text must be named
    /// in the refusal.
    #[test]
    fn a_marker_topic_present_but_errored_is_a_guard_refusal_naming_the_error() {
        let spec = spec_with(&["orders"], "drill-");
        let reader = StubReader {
            cluster_id: Ok("ALLOWED0000000000000000".into()),
            topics: Ok(vec![TopicMeta::errored(
                spec.target.marker_topic.clone(),
                "leader election in progress",
            )]),
        };
        let err = run(
            &spec,
            "restore: {}\n",
            &allowed(&["ALLOWED0000000000000000"], None),
            &reader,
            &NoopCreator,
            &NoopDeleter,
        )
        .unwrap_err();
        match err {
            DrillError::Guard(GuardRefusal(msg)) => {
                assert!(msg.contains("leader election in progress"), "{msg}");
            }
            other => panic!("expected a guard refusal (exit 3), got {other:?}"),
        }
    }

    /// `tail` and `random` select archive records phase 7's leading-range
    /// read cannot reach, so they are REFUSED at phase 0 rather than
    /// downgraded. Refusing is exit 3 — the plan is not one this version can
    /// honour, and nothing has run.
    #[test]
    fn a_sample_anchor_other_than_head_is_a_guard_refusal_naming_the_limitation() {
        for anchor in [Anchor::Tail, Anchor::Random] {
            let mut spec = spec_with(&["orders"], "drill-");
            spec.sample.anchor = anchor;
            let reader = healthy_reader("ALLOWED0000000000000000", &spec.target.marker_topic);
            let err = run(
                &spec,
                "restore: {}\n",
                &allowed(&["ALLOWED0000000000000000"], None),
                &reader,
                &NoopCreator,
                &NoopDeleter,
            )
            .unwrap_err();
            match err {
                DrillError::Guard(GuardRefusal(msg)) => {
                    assert!(msg.contains("sample.anchor"), "{msg}");
                    assert!(msg.contains(anchor.as_str()), "{msg}");
                    assert!(msg.contains("head"), "{msg}");
                }
                other => panic!("expected a guard refusal (exit 3) for {anchor}, got {other:?}"),
            }
        }
    }

    /// The refusal message is the ENTIRE explanation a refused operator gets,
    /// and it is the shape of thing that breaks silently: a multi-line Rust
    /// string literal without a trailing `\` bakes the source indentation into
    /// the message, so it reaches the terminal as
    /// "Phase 7 reconciles              the restored topic". That exact defect
    /// shipped here, and shipped in `phase4_sample.rs` earlier in the build, so
    /// it gets an assertion rather than a promise.
    #[test]
    fn the_anchor_refusal_message_is_single_spaced_prose_not_source_indentation() {
        let mut spec = spec_with(&["orders"], "drill-");
        spec.sample.anchor = Anchor::Random;
        let reader = healthy_reader("ALLOWED0000000000000000", &spec.target.marker_topic);
        let msg = match run(
            &spec,
            "restore: {}\n",
            &allowed(&["ALLOWED0000000000000000"], None),
            &reader,
            &NoopCreator,
            &NoopDeleter,
        )
        .unwrap_err()
        {
            DrillError::Guard(GuardRefusal(m)) => m,
            other => panic!("expected a guard refusal, got {other:?}"),
        };
        assert!(
            !msg.contains("  "),
            "the refusal message carries a run of spaces from the source \
             indentation of its own string literal:\n{msg}"
        );
        assert!(!msg.contains('\n'), "it is one line: {msg}");
        // …and it still says the things an operator needs: what was refused,
        // what to do instead, and why.
        for needle in [
            "sample.anchor `random`",
            "only `head` is",
            "`sample.anchor: head`",
            "report a healthy backup as a failure",
        ] {
            assert!(msg.contains(needle), "missing {needle:?} in:\n{msg}");
        }
    }

    /// The refusal is LOCAL: it must not need a reachable broker, or a plan
    /// this version cannot honour would report exit 1 ("retry me") on a host
    /// whose target is down. Same property the forbidden-key and mapping
    /// guards have.
    #[test]
    fn the_anchor_refusal_does_not_need_a_reachable_broker() {
        let mut spec = spec_with(&["orders"], "drill-");
        spec.sample.anchor = Anchor::Random;
        let reader = StubReader {
            cluster_id: Err(KafkaError::Unreachable("no broker answered".into())),
            topics: Err(KafkaError::Unreachable("no broker answered".into())),
        };
        match run(
            &spec,
            "restore: {}\n",
            &allowed(&["ALLOWED0000000000000000"], None),
            &reader,
            &NoopCreator,
            &NoopDeleter,
        )
        .unwrap_err()
        {
            DrillError::Guard(GuardRefusal(msg)) => assert!(msg.contains("sample.anchor"), "{msg}"),
            other => panic!("expected a guard refusal (exit 3), got {other:?}"),
        }
    }

    /// A broker that cannot answer `cluster_id` is an OPERATIONAL failure
    /// (exit 1) — the plan may be perfectly fine — never a guard refusal
    /// (exit 3).
    #[test]
    fn a_cluster_id_read_failure_is_operational_not_a_guard_refusal() {
        let spec = spec_with(&["orders"], "drill-");
        let reader = StubReader {
            cluster_id: Err(KafkaError::Unreachable("no broker answered".into())),
            topics: Ok(vec![]),
        };
        let err = run(
            &spec,
            "restore: {}\n",
            &allowed(&["ALLOWED0000000000000000"], None),
            &reader,
            &NoopCreator,
            &NoopDeleter,
        )
        .unwrap_err();
        match err {
            DrillError::Kafka(_) => {}
            other => panic!("expected DrillError::Kafka (exit 1), got {other:?}"),
        }
    }

    /// Same as above for `list_topics`: unreachable is operational, not a
    /// refusal.
    #[test]
    fn a_list_topics_read_failure_is_operational_not_a_guard_refusal() {
        let spec = spec_with(&["orders"], "drill-");
        let reader = StubReader {
            cluster_id: Ok("ALLOWED0000000000000000".into()),
            topics: Err(KafkaError::Unreachable("no broker answered".into())),
        };
        let err = run(
            &spec,
            "restore: {}\n",
            &allowed(&["ALLOWED0000000000000000"], None),
            &reader,
            &NoopCreator,
            &NoopDeleter,
        )
        .unwrap_err();
        match err {
            DrillError::Kafka(_) => {}
            other => panic!("expected DrillError::Kafka (exit 1), got {other:?}"),
        }
    }
}
