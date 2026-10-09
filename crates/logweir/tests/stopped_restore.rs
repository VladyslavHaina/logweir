//! **FX-23 — a restore the engine stopped early must never be signed `pass`.**
//!
//! A SIGTERM reaches the pinned engine between topics: it finishes the topic
//! it is on, exits 0, and the topics after it in manifest order are never
//! restored (their targets exist, empty, because Logweir creates them in phase
//! 0). PROD-07.1's review proved, against the shipped phases 4, 6 and 7, that
//! the default SAMPLED lane signed `pass` over that restore in two shapes:
//!
//! - **hole A** — `max_partitions` truncated the sample to the topics that
//!   finished, and the missing topic's segments straddle the point in time,
//!   so they added nothing to the aggregate count bound's `lower`;
//! - **hole B** — the same truncation, the missing topic's segments wholly
//!   inside the window, and the restored topics' in-window straddle records
//!   making up its count in the one aggregate sum.
//!
//! Each row below drives the real `phase4_sample::run`,
//! `phase6_restore::assert_post_condition` and `phase7_verify` over a
//! two-topic archive in manifest order `payments` (restored), `orders` (the
//! topic the engine never started). The holes are now `Fail`; the review's two
//! controls stay `Fail`. Then one row per fix shows that fix deciding alone:
//!
//! - **(a)** the per-partition count bound (holes A and B, with no engine
//!   report and `orders` unsampled);
//! - **(b)** round-robin `max_partitions` (an `orders` segment that proves no
//!   in-window record, so (a) and (c) are silent, and the old first-N
//!   selection over the same restore is the control that passes);
//! - **(c)** the engine's offset report (a HEALTHY target whose report lacks
//!   `orders`: the report is the only finding).
mod fixtures;

use logweir::drill::phase4_sample;
use logweir::drill::phase6_restore::assert_post_condition;
use logweir::drill::phase7_verify::{run_after_restore, VerifyOutcome};
use logweir_core::engine::{
    BackupSetFacts, BackupSetRef, DataEngine, EngineError, EngineId, EngineReport, EngineRun,
    PartitionFacts, PhaseObserver, PreflightReport, RecordFingerprint, RestoreFacts, RestorePlan,
    SampleSelection, SegmentFacts, StorageUrl, TopicFacts, WindowFloorSource,
};
use logweir_core::outcome::IntegrityResult;
use logweir_core::spec::{Anchor, Coverage, SampleSpec};
use logweir_kafka::reader::{ClusterReader, ConsumedRecord, KafkaError, TopicMeta};
use std::collections::{BTreeMap, BTreeSet};

/// `2026-08-29T00:00:00Z` and `2026-08-30T02:00:00Z`, in epoch milliseconds.
const WINDOW: (i64, i64) = (1_787_961_600_000, 1_788_055_200_000);
/// The point in time: one millisecond before the window's end, inside the
/// segments that straddle it.
const PIT: i64 = WINDOW.1 - 1;

#[derive(Default, Clone)]
struct TopicData {
    end_offsets: Vec<(i32, i64)>,
    records: Vec<ConsumedRecord>,
}

/// A target cluster keyed strictly by topic name.
struct MapReader {
    topics: BTreeMap<String, TopicData>,
}

impl ClusterReader for MapReader {
    fn cluster_id(&self) -> Result<String, KafkaError> {
        Ok("TARGET0000000000000000".into())
    }
    fn list_topics(&self) -> Result<Vec<TopicMeta>, KafkaError> {
        Ok(self
            .topics
            .iter()
            .map(|(n, d)| TopicMeta::new(n.clone(), d.end_offsets.len() as i32))
            .collect())
    }
    fn end_offsets(&self, topic: &str) -> Result<Vec<(i32, i64)>, KafkaError> {
        self.topics
            .get(topic)
            .map(|d| d.end_offsets.clone())
            .ok_or_else(|| KafkaError::TopicNotFound(topic.to_string()))
    }
    fn topic_configs(&self, topic: &str) -> Result<BTreeMap<String, String>, KafkaError> {
        self.topics
            .get(topic)
            .map(|_| BTreeMap::new())
            .ok_or_else(|| KafkaError::TopicNotFound(topic.to_string()))
    }
    fn broker_configs(&self) -> Result<BTreeMap<String, String>, KafkaError> {
        Ok(BTreeMap::new())
    }
    fn consume_range(
        &self,
        topic: &str,
        partition: i32,
        from: i64,
        max: usize,
    ) -> Result<Vec<ConsumedRecord>, KafkaError> {
        let d = self
            .topics
            .get(topic)
            .ok_or_else(|| KafkaError::TopicNotFound(topic.to_string()))?;
        Ok(d.records
            .iter()
            .filter(|r| r.partition == partition && r.offset >= from)
            .take(max)
            .cloned()
            .collect())
    }
}

/// An engine double answering `fingerprints` per `(topic, partition)`.
struct ArchiveEngine {
    facts: BackupSetFacts,
    archive: BTreeMap<(String, i32), Vec<RecordFingerprint>>,
}

impl DataEngine for ArchiveEngine {
    fn id(&self) -> EngineId {
        EngineId {
            id: "fake".into(),
            version: "v0.0.0".into(),
            digest: format!("sha256:{}", "0".repeat(64)),
        }
    }
    fn list_backup_sets(&self, _: &StorageUrl) -> Result<Vec<BackupSetRef>, EngineError> {
        Ok(vec![])
    }
    fn describe(&self, _: &BackupSetRef) -> Result<BackupSetFacts, EngineError> {
        Ok(self.facts.clone())
    }
    fn preflight(&self, _: &RestorePlan) -> Result<PreflightReport, EngineError> {
        Err(EngineError::Operational("unused".into()))
    }
    fn restore(
        &self,
        _: &RestorePlan,
        _: &mut dyn PhaseObserver,
    ) -> Result<RestoreFacts, EngineError> {
        Err(EngineError::Operational("unused".into()))
    }
    fn fingerprints(&self, s: &SampleSelection) -> Result<Vec<RecordFingerprint>, EngineError> {
        Ok(self
            .archive
            .get(&(s.topic.clone(), s.partition))
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .take(s.count)
            .collect())
    }
    fn validation_run(&self, _: &RestorePlan) -> Result<EngineRun, EngineError> {
        Ok(EngineRun { exit_code: 0 })
    }
}

/// `n` records of `topic/partition` at offsets `0..n` and timestamps from
/// the window's start: the archive's fingerprints and the target's copies,
/// each carrying its `x-original-offset`.
fn records(topic: &str, partition: i32, n: usize) -> (Vec<RecordFingerprint>, Vec<ConsumedRecord>) {
    let mut arch = Vec::with_capacity(n);
    let mut cons = Vec::with_capacity(n);
    for i in 0..n {
        let off = i as i64;
        let headers = vec![(
            "x-original-offset".to_string(),
            Some(fixtures::le_offset(off)),
        )];
        let key = format!("{topic}-{partition}-k{i}").into_bytes();
        let value = format!("{topic}-{partition}-v{i}").into_bytes();
        let ts = WINDOW.0 + off;
        arch.push(RecordFingerprint {
            topic: topic.into(),
            partition,
            offset: off,
            sha256: logweir_kafka::fingerprint::record_fingerprint(
                Some(&key),
                Some(&value),
                &headers,
                ts,
            ),
        });
        cons.push(ConsumedRecord {
            partition,
            offset: off,
            timestamp_ms: ts,
            key: Some(key),
            value: Some(value),
            headers,
        });
    }
    (arch, cons)
}

/// How `orders`' one segment sits against the restore window.
#[derive(Clone, Copy, Debug)]
enum Orders {
    /// `[window start, past the point in time]`, 25 records: the normal
    /// point-in-time shape (hole A). Its first record is inside the window.
    Straddles,
    /// Wholly inside the window, `n` records (hole B and control 2).
    Inside(i64),
    /// `[before the window, past the point in time]`, 25 records: neither
    /// end inside the window, so the manifest proves no record of it there.
    /// Needs a plan whose floor is not the manifest's (`InheritedFromSpec`).
    Around,
}

struct Case {
    /// How many partitions `payments` has; each is restored.
    payments_partitions: i32,
    orders: Orders,
    /// `true`: `orders` was restored too (a healthy target). `false`: the
    /// engine stopped before it — its target exists, empty.
    orders_restored: bool,
    max_partitions: Option<u32>,
    report: EngineReport,
}

impl Case {
    fn stopped(orders: Orders, max_partitions: Option<u32>) -> Case {
        Case {
            payments_partitions: 1,
            orders,
            orders_restored: false,
            max_partitions,
            report: EngineReport::Absent,
        }
    }
}

struct Run {
    selected: Vec<String>,
    unsampled: Vec<String>,
    out: VerifyOutcome,
}

fn reason(r: &Run) -> String {
    r.out.integrity.partial_reason.clone().unwrap_or_default()
}

/// Facts, store, target and plan for `c`. `payments` is FIRST in manifest
/// order (the engine finished it) and `orders` SECOND.
fn build(
    c: &Case,
) -> (
    BackupSetFacts,
    logweir_engine_oso::storage::Store,
    MapReader,
    ArchiveEngine,
    RestorePlan,
    BTreeMap<String, String>,
) {
    let store = logweir_engine_oso::storage::Store::in_memory("logweir");
    let put = |key: &str| -> String {
        let bytes = format!("segment {key}").into_bytes();
        store.put_create_only(key, &bytes).unwrap();
        logweir_core::ids::sha256_prefixed(&bytes)
    };
    let seg = |key: String, sha: String, s0: i64, s1: i64, o0: i64, n: i64| SegmentFacts {
        key,
        start_offset: o0,
        end_offset: o0 + n - 1,
        start_timestamp: s0,
        end_timestamp: s1,
        record_count: n,
        sha256: sha,
        uploaded_at: WINDOW.1,
    };
    let mut archive = BTreeMap::new();
    let mut topics = BTreeMap::new();

    // payments/p: 25 records wholly inside, then a 100-record segment that
    // straddles the point in time, 25 of whose records are in the window. Its
    // target holds those 50.
    let mut payments = Vec::new();
    let mut pay_ends = Vec::new();
    let mut pay_records = Vec::new();
    for p in 0..c.payments_partitions {
        let k0 = format!("logweir/pay-{p}-0.kbak");
        let k1 = format!("logweir/pay-{p}-1.kbak");
        let (s0, s1) = (put(&k0), put(&k1));
        payments.push(PartitionFacts {
            partition_id: p,
            segments: vec![
                seg(k0, s0, WINDOW.0, WINDOW.0 + 24, 0, 25),
                seg(k1, s1, WINDOW.0 + 25, WINDOW.1 + 10_000, 25, 100),
            ],
            gaps: vec![],
            pruned: vec![],
        });
        let (arch, cons) = records("payments", p, 50);
        archive.insert(("payments".to_string(), p), arch);
        pay_ends.push((p, 50));
        pay_records.extend(cons);
    }
    topics.insert(
        "drill-payments".to_string(),
        TopicData {
            end_offsets: pay_ends,
            records: pay_records,
        },
    );

    let (start, end, n) = match c.orders {
        Orders::Straddles => (WINDOW.0, WINDOW.1 + 10_000, 25),
        Orders::Inside(n) => (WINDOW.0, WINDOW.0 + n - 1, n),
        Orders::Around => (WINDOW.0 - 1_000, WINDOW.1 + 10_000, 25),
    };
    let k = "logweir/ord-0.kbak".to_string();
    let sha = put(&k);
    let orders = PartitionFacts {
        partition_id: 0,
        segments: vec![seg(k, sha, start, end, 0, n)],
        gaps: vec![],
        pruned: vec![],
    };
    let in_window = match c.orders {
        Orders::Inside(n) => n as usize,
        _ => 25,
    };
    let (arch, cons) = records("orders", 0, in_window);
    archive.insert(("orders".to_string(), 0), arch);
    topics.insert(
        "drill-orders".to_string(),
        if c.orders_restored {
            TopicData {
                end_offsets: vec![(0, in_window as i64)],
                records: cons,
            }
        } else {
            TopicData {
                end_offsets: vec![(0, 0)],
                records: vec![],
            }
        },
    );

    let topic = |name: &str, partitions: Vec<PartitionFacts>| TopicFacts {
        name: name.into(),
        original_partition_count: Some(partitions.len() as i32),
        source_replication_factor: Some(1),
        configurations: BTreeMap::new(),
        partitions,
    };
    let facts = BackupSetFacts {
        backup_id: "fx23".into(),
        created_at: fixtures::ts("2026-09-03T09:00:00Z"),
        source_cluster_id: Some("SRC0000000000000000000".into()),
        manifest_sha256: "sha256:0".into(),
        manifest_version_id: None,
        consumer_group_snapshot_sha256: None,
        topics: vec![topic("payments", payments), topic("orders", vec![orders])],
    };
    let mut mapping = BTreeMap::new();
    mapping.insert("payments".to_string(), "drill-payments".to_string());
    mapping.insert("orders".to_string(), "drill-orders".to_string());
    let plan = RestorePlan {
        set: BackupSetRef {
            backup_id: "fx23".into(),
            manifest_key: "fx23/manifest.json".into(),
        },
        storage: StorageUrl::Filesystem {
            path: "/tmp".into(),
        },
        target_bootstrap: vec!["broker:9092".into()],
        target_auth: logweir_core::engine::AuthRender::Plaintext,
        topic_mapping: mapping.clone(),
        time_window: (
            chrono::DateTime::from_timestamp_millis(WINDOW.0).unwrap(),
            chrono::DateTime::from_timestamp_millis(PIT).unwrap(),
        ),
        window_floor_source: match c.orders {
            Orders::Around => WindowFloorSource::InheritedFromSpec,
            _ => WindowFloorSource::ArchiveManifest,
        },
        source_partitions: Default::default(),
        default_replication_factor: 1,
        checkpoint_state: "/tmp/fx23/checkpoint.json".into(),
        checkpoint_interval_secs: 30,
        offset_report: "/tmp/fx23/offsets.json".into(),
    };
    let engine = ArchiveEngine {
        facts: facts.clone(),
        archive,
    };
    (facts, store, MapReader { topics }, engine, plan, mapping)
}

/// Phase 6's post-condition, phase 4's selection and phase 7's sampled lane
/// over `c`. `selection` overrides phase 4 (the control that replays the old
/// first-N truncation).
fn drive(c: &Case, selection: Option<&[(&str, i32)]>) -> Run {
    drive_with(c, selection, |_| {})
}

/// [`drive`], with the target cluster moved by `tweak` before any phase reads
/// it.
fn drive_with(
    c: &Case,
    selection: Option<&[(&str, i32)]>,
    tweak: impl FnOnce(&mut MapReader),
) -> Run {
    let (facts, store, mut reader, engine, plan, mapping) = build(c);
    tweak(&mut reader);

    // Phase 6 accepts this target: one partition is above 0.
    let mut ends = BTreeMap::new();
    for dst in mapping.values() {
        ends.insert(dst.clone(), reader.end_offsets(dst).unwrap());
    }
    assert_post_condition(&ends).expect("phase 6 accepts a target with an empty topic");

    let spec = SampleSpec {
        window_start: chrono::DateTime::from_timestamp_millis(WINDOW.0).unwrap(),
        window_end: chrono::DateTime::from_timestamp_millis(PIT).unwrap(),
        records_per_partition: 25,
        anchor: Anchor::Head,
        max_partitions: c.max_partitions,
        coverage: Coverage::Sampled,
        complete_max_records: None,
    };
    let src: Vec<String> = mapping.keys().cloned().collect();
    let mut sel = phase4_sample::run(&facts, &spec, &src).unwrap();
    if let Some(chosen) = selection {
        sel.per_partition
            .retain(|s| chosen.contains(&(s.topic.as_str(), s.partition)));
        // The old truncation kept partitions phase 4 now drops; rebuild them.
        for (t, p) in chosen {
            if !sel
                .per_partition
                .iter()
                .any(|s| s.topic == *t && s.partition == *p)
            {
                let mut s = sel.per_partition[0].clone();
                s.topic = (*t).into();
                s.partition = *p;
                sel.per_partition.push(s);
            }
        }
    }
    sel.bind_backup_set(&plan.set);
    let selected = sel
        .per_partition
        .iter()
        .map(|s| format!("{}/{}", s.topic, s.partition))
        .collect();
    let out = run_after_restore(
        &engine,
        &reader,
        &store,
        &facts,
        &sel.per_partition,
        &mapping,
        &plan,
        &logweir_core::backup_receipt::SourceConfigCoverage::unknown(),
        logweir_core::spec::TargetMode::Scratch,
        Coverage::Sampled,
        None,
        &c.report,
    )
    .expect("a stopped restore is a DRILL RESULT, never an operational failure");
    Run {
        selected,
        unsampled: sel.unsampled_topics,
        out,
    }
}

fn read(entries: &[(&str, i32)]) -> EngineReport {
    EngineReport::Read(
        entries
            .iter()
            .map(|(t, p)| ((*t).to_string(), *p))
            .collect::<BTreeSet<_>>(),
    )
}

// ------------------------------------------------- the review's four rows

/// **Hole A, now `Fail`.** `max_partitions: 1` keeps `payments/0` alone,
/// `orders` is named unsampled, and its one segment straddles the point in
/// time — the aggregate bound is `[25, 150]` and holds 50. The per-partition
/// bound proves one `orders` record in the window and finds none.
#[test]
fn hole_a_a_missing_topic_whose_segment_straddles_the_point_in_time_is_fail() {
    let r = drive(&Case::stopped(Orders::Straddles, Some(1)), None);
    assert_eq!(r.selected, vec!["payments/0"]);
    assert_eq!(r.unsampled, vec!["orders"]);
    assert_eq!(
        r.out.integrity.result,
        IntegrityResult::Fail,
        "{:?}",
        r.out.integrity
    );
    assert_eq!(
        reason(&r),
        format!(
            "orders/0: drill-orders/0 holds no record but the manifest proves at least 1 in \
             the window [{}, {PIT}]",
            WINDOW.0
        )
    );
    // The sample it did take still reconciled: the count decided.
    assert_eq!(r.out.integrity.records_sampled_matching, 25);
}

/// **Hole B, now `Fail`.** `orders` wholly inside with 25 records; the
/// aggregate bound is `[50, 150]` and `payments`' 25 in-window straddle
/// records made the sum 50. Per partition, `orders/0` proves 25 and holds 0.
#[test]
fn hole_b_a_missing_topic_covered_by_its_siblings_straddle_records_is_fail() {
    let r = drive(&Case::stopped(Orders::Inside(25), Some(1)), None);
    assert_eq!(r.selected, vec!["payments/0"]);
    assert_eq!(r.out.integrity.result, IntegrityResult::Fail);
    assert_eq!(
        reason(&r),
        format!(
            "orders/0: drill-orders/0 holds no record but the manifest proves at least 25 in \
             the window [{}, {PIT}]",
            WINDOW.0
        )
    );
}

/// **Control 1, still `Fail`.** No truncation: `orders/0` is sampled, and its
/// 25 sampled records did not reconcile.
#[test]
fn control_1_an_untruncated_sample_still_fails_the_missing_topic() {
    let r = drive(&Case::stopped(Orders::Straddles, None), None);
    assert_eq!(r.selected, vec!["payments/0", "orders/0"]);
    assert!(r.unsampled.is_empty());
    assert_eq!(r.out.integrity.result, IntegrityResult::Fail);
    assert!(reason(&r).contains("orders/0: "), "{}", reason(&r));
}

/// **Control 2, still `Fail`, in the aggregate's own words.** 26 records
/// wholly inside: the sum is below its `lower` — the text the aggregate
/// bound always signed — and the partition is named after it.
#[test]
fn control_2_a_sum_below_its_bound_still_fails_in_its_own_words() {
    let r = drive(&Case::stopped(Orders::Inside(26), Some(1)), None);
    assert_eq!(r.out.integrity.result, IntegrityResult::Fail);
    let why = reason(&r);
    assert!(
        why.starts_with(&format!(
            "restored 50 records but the manifest bounds the window [{}, {PIT}] at [51, 151]; \
             orders/0: drill-orders/0 holds no record",
            WINDOW.0
        )),
        "{why}"
    );
}

// ------------------------------------------------ each fix deciding alone

/// **(c) alone.** A HEALTHY target — both topics restored, every selection
/// reconciles, every count inside its bound — whose engine report names
/// `payments` only. The report is the one finding, and the whole reason. A
/// complete report, no report, or an unreadable one checks nothing and the
/// drill passes. KILLS: `check_engine_report` returning `None`, keying the
/// lookup by the SOURCE topic (`orders/0` is not an entry), and checking a
/// partition the manifest proves nothing in.
#[test]
fn c_a_report_that_lacks_a_mapped_topic_fails_a_target_that_otherwise_passes() {
    let healthy = |report: EngineReport| Case {
        payments_partitions: 1,
        orders: Orders::Inside(25),
        orders_restored: true,
        max_partitions: None,
        report,
    };
    let finished = read(&[("drill-payments", 0), ("drill-orders", 0)]);
    for report in [
        finished,
        EngineReport::Absent,
        EngineReport::Unreadable("x".into()),
    ] {
        let r = drive(&healthy(report.clone()), None);
        assert_eq!(
            r.out.integrity.result,
            IntegrityResult::Pass,
            "{report:?}: {:?}",
            r.out.integrity
        );
    }
    let r = drive(&healthy(read(&[("drill-payments", 0)])), None);
    assert_eq!(r.out.integrity.result, IntegrityResult::Fail);
    assert_eq!(
        reason(&r),
        format!(
            "the engine's offset report has no entry for 1 mapped partition(s) the manifest \
             proves hold records in the window [{}, {PIT}]: orders/0 -> drill-orders/0; an \
             engine that stopped early (it honours a SIGTERM between topics and exits 0) \
             reports only the topics it finished",
            WINDOW.0
        )
    );
    // Keyed by the TARGET topic, as the engine writes it.
    let r = drive(
        &healthy(read(&[("drill-payments", 0), ("orders", 0)])),
        None,
    );
    assert_eq!(r.out.integrity.result, IntegrityResult::Fail);
    // A partition the manifest proves nothing in is not required.
    let around = Case {
        orders: Orders::Around,
        ..healthy(read(&[("drill-payments", 0)]))
    };
    assert_eq!(
        drive(&around, None).out.integrity.result,
        IntegrityResult::Pass
    );
}

/// **(c) beside (a) on the trigger itself.** Hole A with the report the
/// stopped engine really writes (`payments` only): both whole-drill findings,
/// the count first.
#[test]
fn c_the_stopped_engines_own_report_is_named_after_the_count() {
    let mut c = Case::stopped(Orders::Straddles, Some(1));
    c.report = read(&[("drill-payments", 0)]);
    let why = reason(&drive(&c, None));
    let count = why
        .find("orders/0: drill-orders/0 holds no record")
        .unwrap();
    let report = why.find("the engine's offset report has no entry").unwrap();
    assert!(count < report, "{why}");
}

/// **(b) alone.** `payments` has two partitions, `orders` one, and
/// `max_partitions: 2`. The `orders` segment covers the window from both
/// sides, so the manifest proves no record of it there: the count bound and
/// the report are silent BY CONSTRUCTION. Round-robin samples `orders/0` and
/// its sample does not reconcile. The CONTROL replays the old first-N
/// truncation over the same restore (`payments/0`, `payments/1`) and passes —
/// which is the hole (b) closes. KILLS: truncating to the first N again.
#[test]
fn b_round_robin_samples_the_missing_topic_where_first_n_did_not() {
    let c = Case {
        payments_partitions: 2,
        orders: Orders::Around,
        orders_restored: false,
        max_partitions: Some(2),
        report: EngineReport::Absent,
    };
    let r = drive(&c, None);
    assert_eq!(r.selected, vec!["payments/0", "orders/0"]);
    assert!(r.unsampled.is_empty());
    assert_ne!(
        r.out.integrity.result,
        IntegrityResult::Pass,
        "{:?}",
        r.out.integrity
    );
    assert!(
        reason(&r).contains("orders/0: "),
        "the sample names the topic: {}",
        reason(&r)
    );
    assert!(
        !reason(&r).contains("holds no record") && !reason(&r).contains("offset report"),
        "(a) and (c) must be silent here, or this row does not show (b) alone: {}",
        reason(&r)
    );

    let old = drive(&c, Some(&[("payments", 0), ("payments", 1)]));
    assert_eq!(old.selected, vec!["payments/0", "payments/1"]);
    assert_eq!(
        old.out.integrity.result,
        IntegrityResult::Pass,
        "the control: the old first-N sample of the same restore passes: {:?}",
        old.out.integrity
    );
}

/// **(a), the rest of the per-partition bound.** A healthy restore whose
/// first `payments` partition holds MORE than its own segments can account for
/// (150 against an upper of 125) while its sibling holds fewer: the sum is
/// inside the aggregate bound and only the partition's own bound sees it. And
/// a record in a target partition the manifest lists no partition for.
/// KILLS: checking only each partition's lower edge; dropping the
/// unlisted-partition arm.
#[test]
fn a_a_partition_outside_its_own_bound_fails_while_the_sum_is_inside_its() {
    let healthy = Case {
        payments_partitions: 2,
        orders: Orders::Inside(25),
        orders_restored: true,
        max_partitions: None,
        report: EngineReport::Absent,
    };
    assert_eq!(
        drive(&healthy, None).out.integrity.result,
        IntegrityResult::Pass,
        "the control: the healthy restore passes"
    );
    let r = drive_with(&healthy, None, |reader| {
        let pay = reader.topics.get_mut("drill-payments").unwrap();
        pay.end_offsets = vec![(0, 150), (1, 30)];
    });
    assert_eq!(r.out.integrity.result, IntegrityResult::Fail);
    assert_eq!(
        reason(&r),
        format!(
            "payments/0: drill-payments/0 holds 150 records but the manifest bounds this \
             partition's window [{}, {PIT}] at [26, 125]",
            WINDOW.0
        )
    );
    let r = drive_with(&healthy, None, |reader| {
        let ord = reader.topics.get_mut("drill-orders").unwrap();
        ord.end_offsets.push((1, 5));
    });
    assert_eq!(r.out.integrity.result, IntegrityResult::Fail);
    assert_eq!(
        reason(&r),
        "drill-orders/1 holds 5 records but the manifest lists no partition 1 of orders"
    );
}

// ---------------------------------------------------------- phase 4 alone

/// Round-robin, as phase 4 alone: three topics in manifest order —
/// `payments` (3 partitions), `orders` (2), `audit` (1). Every cap keeps one
/// partition of each topic before a second of any, the kept partitions come
/// back in manifest order, and the topics a cap could not reach are named,
/// sorted. KILLS: first-N truncation (`max 3` would keep three `payments`
/// partitions), an unsorted or missing `unsampled_topics`.
#[test]
fn max_partitions_keeps_a_partition_of_every_topic_first() {
    let seg = |t: &str, p: i32| SegmentFacts {
        key: format!("{t}-{p}"),
        start_offset: 0,
        end_offset: 9,
        start_timestamp: WINDOW.0,
        end_timestamp: WINDOW.0 + 9,
        record_count: 10,
        sha256: String::new(),
        uploaded_at: WINDOW.1,
    };
    let topic = |t: &str, n: i32| TopicFacts {
        name: t.into(),
        original_partition_count: Some(n),
        source_replication_factor: Some(1),
        configurations: BTreeMap::new(),
        partitions: (0..n)
            .map(|p| PartitionFacts {
                partition_id: p,
                segments: vec![seg(t, p)],
                gaps: vec![],
                pruned: vec![],
            })
            .collect(),
    };
    let facts = BackupSetFacts {
        backup_id: "rr".into(),
        created_at: fixtures::ts("2026-09-03T09:00:00Z"),
        source_cluster_id: None,
        manifest_sha256: "sha256:0".into(),
        manifest_version_id: None,
        consumer_group_snapshot_sha256: None,
        topics: vec![topic("payments", 3), topic("orders", 2), topic("audit", 1)],
    };
    let topics: Vec<String> = ["payments", "orders", "audit"].map(String::from).to_vec();
    let pick = |max: Option<u32>| {
        let spec = SampleSpec {
            window_start: chrono::DateTime::from_timestamp_millis(WINDOW.0).unwrap(),
            window_end: chrono::DateTime::from_timestamp_millis(WINDOW.1).unwrap(),
            records_per_partition: 5,
            anchor: Anchor::Head,
            max_partitions: max,
            coverage: Coverage::Sampled,
            complete_max_records: None,
        };
        let s = phase4_sample::run(&facts, &spec, &topics).unwrap();
        let ids: Vec<String> = s
            .per_partition
            .iter()
            .map(|p| format!("{}/{}", p.topic, p.partition))
            .collect();
        (ids, s.unsampled_topics, s.topics, s.records_expected)
    };
    assert_eq!(
        pick(Some(1)),
        (
            vec!["payments/0".into()],
            vec!["audit".into(), "orders".into()],
            1,
            10
        )
    );
    assert_eq!(
        pick(Some(2)),
        (
            vec!["payments/0".into(), "orders/0".into()],
            vec!["audit".into()],
            2,
            20
        )
    );
    assert_eq!(
        pick(Some(3)),
        (
            vec!["payments/0".into(), "orders/0".into(), "audit/0".into()],
            vec![],
            3,
            30
        )
    );
    assert_eq!(
        pick(Some(4)).0,
        vec!["payments/0", "payments/1", "orders/0", "audit/0"]
    );
    assert_eq!(
        pick(Some(5)).0,
        vec![
            "payments/0",
            "payments/1",
            "orders/0",
            "orders/1",
            "audit/0"
        ]
    );
    let all = pick(None);
    assert_eq!(all.0.len(), 6);
    assert!(all.1.is_empty());
}
