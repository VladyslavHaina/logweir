//! **PROD-01.4 — topic identity and generations: the reusable oracle.**
//!
//! A Kafka topic that is deleted and created again under the same name is a
//! NEW topic. The broker gives it a new topic ID (KIP-516) and restarts every
//! partition's offsets at zero, so an offset that meant one record before the
//! recreation means a different record after it. Logweir cannot read topic IDs
//! today: rdkafka 0.36.2's safe API has no DescribeTopics, `logweir-kafka`
//! forbids `unsafe`, and the pinned engine asks for Metadata v9, which carries
//! none. `docs/to-do/decisions/PROD-01.4-topic-identity.md` therefore adopts a
//! heuristic until a real ID route lands, and this file defines and measures it.
//!
//! # Two parts
//!
//! * **The reference rule** (`classify`, `intra_run`, `lineage`, `arm10`) and
//!   its pure tests compile in every feature set and open no socket:
//!   `cargo test --locked -p e2e --test topic_identity` runs them.
//! * **The live rows** sit in `mod live`, behind the `e2e` feature, against the
//!   compose broker (`just e2e-up`):
//!   `cargo test --locked -p e2e --features e2e --test topic_identity -- --include-ignored --test-threads=1`.
//!
//! # What a live row does
//!
//! * It builds one situation on the broker: a recreated topic (same, fewer and
//!   more partitions), added partitions, DeleteRecords, compaction, retention
//!   expiry, an open transaction, non-monotonic timestamps, a byte-identical
//!   replay, an original-name restore, a `LogAppendTime` topic and a repeated
//!   header key.
//! * The GROUND TRUTH is the broker's own topic ID, read with
//!   `kafka-topics.sh --describe` before and after each capture. Each row
//!   asserts it before any verdict, so a fixture that did not do what it
//!   claims fails instead of making its verdict vacuous.
//! * It takes two captures with the PINNED ENGINE (`engine_bin()`) and applies
//!   the decision's comparison: the first capture's archived tail against the
//!   SAME offsets in the second capture's archive, both decoded by Logweir's
//!   `.kbak` decoder with the engine's two appended headers stripped
//!   (`archived_fingerprints`). Two captures of an unchanged offset are equal
//!   whatever the engine loses at capture (a `LogAppendTime` batch's append
//!   time, a repeated header key). A live read is not (rows c17 and c18).
//! * It records four verdicts side by side:
//!   - the rule (archive against archive);
//!   - offsets only;
//!   - a source read (the variant the decision rejects);
//!   - the ID path (the broker's IDs standing in for PROD-01.4a's
//!     DescribeTopics).
//!
//! Known false negatives (c13, c14) and the known false positive (c19) are
//! asserted as such. A change that closes one fails here, and so has to update
//! the decision record deliberately.
//!
//! Set `LOGWEIR_TOPIC_IDENTITY_EVIDENCE=<file>` to append one JSON line per
//! live row. Row c10 waits for the broker's five-minute retention check and is
//! `#[ignore]`d. Every broker address the live rows use comes from ONE
//! function, `live::broker_address`, and no S3 endpoint is used: the engine
//! archives to a filesystem path under `engine_mount()`.
use std::collections::BTreeMap;

// ===========================================================================
// The reference rule. Pure: no broker, no clock, no I/O. Every feature set.
// ===========================================================================

/// Log start offset and high watermark of one partition, read
/// READ_UNCOMMITTED. The engine captures READ_UNCOMMITTED, so the marks it is
/// compared with must be read the same way (row c11).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Marks {
    log_start: i64,
    high_watermark: i64,
}

/// One archived record near the end of a partition, with the fingerprint of
/// the SOURCE record it was archived from.
#[derive(Debug, Clone, PartialEq, Eq)]
struct TailRecord {
    offset: i64,
    /// `record_fingerprint` over the archived record, minus the two headers
    /// the engine appended.
    fingerprint: String,
    /// The same over the archived bytes verbatim. Only the live evidence reads
    /// it, to show why the strip is required.
    #[cfg_attr(not(feature = "e2e"), allow(dead_code))]
    raw_fingerprint: String,
}

/// What one capture archived for one partition.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Archived {
    first_offset: i64,
    last_offset: i64,
    #[cfg_attr(not(feature = "e2e"), allow(dead_code))]
    first_timestamp_ms: i64,
    #[cfg_attr(not(feature = "e2e"), allow(dead_code))]
    last_timestamp_ms: i64,
    #[cfg_attr(not(feature = "e2e"), allow(dead_code))]
    records: usize,
    /// Up to three records, newest first.
    tail: Vec<TailRecord>,
}

/// One partition as one capture observed it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PartitionObs {
    partition: i32,
    before: Marks,
    after: Marks,
    /// `None` when the engine archived no record for the partition.
    archived: Option<Archived>,
}

/// One capture of one topic: the receipt's observation block.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Capture {
    cluster_id: String,
    /// The topic ID read at phase −1: the receipt's `topic_id`.
    topic_id: Option<String>,
    /// The topic ID read after the engine exits: the receipt's
    /// `topic_id_after`. The within-run check compares the two.
    topic_id_after: Option<String>,
    partition_count: i32,
    partitions: Vec<PartitionObs>,
}

/// What run *k* compares with its predecessor: its pre-run marks and topic ID,
/// and what its own capture says about itself.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Current {
    cluster_id: String,
    topic_id: Option<String>,
    partition_count: i32,
    marks: BTreeMap<i32, Marks>,
    /// `intra_run` of run *k*'s capture; empty for a bare read with no
    /// capture behind it.
    within_run: Vec<Signal>,
}

impl Current {
    fn of(c: &Capture) -> Current {
        Current {
            cluster_id: c.cluster_id.clone(),
            topic_id: c.topic_id.clone(),
            partition_count: c.partition_count,
            marks: c
                .partitions
                .iter()
                .map(|p| (p.partition, p.before))
                .collect(),
            within_run: intra_run(c),
        }
    }
}

/// What a read at exactly one offset returned.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Probe {
    /// A record at exactly the requested offset, with its fingerprint.
    At(String),
    /// No record at that offset: the next record's offset, or `None` when
    /// nothing follows (compacted away, or a marker a consumer never sees).
    Absent(Option<i64>),
    /// Below the log start offset.
    OutOfRange,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Signal {
    TopicIdChanged {
        previous: String,
        current: String,
    },
    PartitionCountDecreased {
        previous: i32,
        current: i32,
    },
    PartitionCountIncreased {
        previous: i32,
        current: i32,
    },
    LogStartRegressed {
        partition: i32,
        previous: i64,
        current: i64,
    },
    EndRegressed {
        partition: i32,
        previous: i64,
        current: i64,
    },
    BoundaryRecordChanged {
        partition: i32,
        offset: i64,
    },
    BoundaryRecordVerified {
        partition: i32,
        offset: i64,
    },
    BoundaryRecordAbsent {
        partition: i32,
        offset: i64,
        returned: Option<i64>,
    },
    BoundaryDeleted {
        partition: i32,
        offset: i64,
        log_start: i64,
    },
    CaptureGap {
        partition: i32,
        from: i64,
        to: i64,
    },
    /// `partition: None` for a topic-level change (the topic ID).
    ChangedDuringCapture {
        partition: Option<i32>,
        reason: &'static str,
    },
}

impl Signal {
    /// A signal that cannot occur within one continuous offset history.
    fn is_break(&self) -> bool {
        matches!(
            self,
            Signal::TopicIdChanged { .. }
                | Signal::PartitionCountDecreased { .. }
                | Signal::LogStartRegressed { .. }
                | Signal::EndRegressed { .. }
                | Signal::BoundaryRecordChanged { .. }
                | Signal::ChangedDuringCapture { .. }
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Verdict {
    /// No break signal; the same history, verified by ID or by content.
    Continuous,
    /// No break signal, and nothing left to verify content against.
    Unverified,
    /// No break signal, but partitions were added, nothing was verified and
    /// no ID proves the topic: CreatePartitions and a recreation with more
    /// partitions look alike.
    Suspected,
    /// A signal that cannot occur within one continuous offset history.
    Break,
    /// No comparison was made (no predecessor with generation data, or
    /// another cluster).
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Basis {
    TopicId,
    Content,
    Watermarks,
    None,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Outcome {
    verdict: Verdict,
    basis: Basis,
    signals: Vec<Signal>,
}

/// The reference rule: compare run *k* with the previous capture of the same
/// (cluster, topic). §4.2 of the decision, R1–R6.
///
/// `probe` is `None` for the offsets-only variant, which the evidence reports
/// beside the rule to show what the boundary comparison adds.
fn classify(
    prev: &Capture,
    cur: &Current,
    mut probe: Option<&mut dyn FnMut(i32, i64) -> Probe>,
) -> Outcome {
    if prev.cluster_id != cur.cluster_id {
        return Outcome {
            verdict: Verdict::Unknown,
            basis: Basis::None,
            signals: Vec::new(),
        };
    }
    let mut s = Vec::new();
    // R1. Two IDs decide the generation. Equal IDs still leave R2–R6 to run:
    // they report coverage gaps and a truncation under the same ID.
    let ids_equal = match (&prev.topic_id, &cur.topic_id) {
        (Some(a), Some(b)) if a != b => {
            s.push(Signal::TopicIdChanged {
                previous: a.clone(),
                current: b.clone(),
            });
            s.extend(cur.within_run.iter().cloned());
            return Outcome {
                verdict: Verdict::Break,
                basis: Basis::None,
                signals: s,
            };
        }
        (Some(_), Some(_)) => true,
        _ => false,
    };
    // R2.
    if cur.partition_count < prev.partition_count {
        s.push(Signal::PartitionCountDecreased {
            previous: prev.partition_count,
            current: cur.partition_count,
        });
    } else if cur.partition_count > prev.partition_count {
        s.push(Signal::PartitionCountIncreased {
            previous: prev.partition_count,
            current: cur.partition_count,
        });
    }
    for p in &prev.partitions {
        // A partition that no longer exists is PartitionCountDecreased above.
        let Some(m) = cur.marks.get(&p.partition) else {
            continue;
        };
        let prev_start = p.before.log_start.max(p.after.log_start);
        let mut prev_end = p.before.high_watermark.max(p.after.high_watermark);
        if let Some(a) = &p.archived {
            prev_end = prev_end.max(a.last_offset + 1);
        }
        // R3, R4.
        if m.log_start < prev_start {
            s.push(Signal::LogStartRegressed {
                partition: p.partition,
                previous: prev_start,
                current: m.log_start,
            });
        }
        if m.high_watermark < prev_end {
            s.push(Signal::EndRegressed {
                partition: p.partition,
                previous: prev_end,
                current: m.high_watermark,
            });
        }
        let Some(a) = &p.archived else {
            continue;
        };
        // R5.
        if m.log_start > a.last_offset + 1 {
            s.push(Signal::CaptureGap {
                partition: p.partition,
                from: a.last_offset + 1,
                to: m.log_start,
            });
        }
        // R6.
        let Some(probe) = probe.as_deref_mut() else {
            continue;
        };
        for t in &a.tail {
            if t.offset < m.log_start {
                s.push(Signal::BoundaryDeleted {
                    partition: p.partition,
                    offset: t.offset,
                    log_start: m.log_start,
                });
                break;
            }
            if t.offset >= m.high_watermark {
                // Beyond the current end: EndRegressed already says so.
                continue;
            }
            match probe(p.partition, t.offset) {
                Probe::At(fp) if fp == t.fingerprint => {
                    s.push(Signal::BoundaryRecordVerified {
                        partition: p.partition,
                        offset: t.offset,
                    });
                    break;
                }
                Probe::At(_) => {
                    s.push(Signal::BoundaryRecordChanged {
                        partition: p.partition,
                        offset: t.offset,
                    });
                    break;
                }
                Probe::Absent(returned) => {
                    // Compacted away, or a marker: try the next older one.
                    s.push(Signal::BoundaryRecordAbsent {
                        partition: p.partition,
                        offset: t.offset,
                        returned,
                    });
                }
                Probe::OutOfRange => {
                    s.push(Signal::BoundaryDeleted {
                        partition: p.partition,
                        offset: t.offset,
                        log_start: m.log_start,
                    });
                    break;
                }
            }
        }
    }
    // The within-run check of run k (§4.4).
    s.extend(cur.within_run.iter().cloned());
    let verified = s
        .iter()
        .any(|x| matches!(x, Signal::BoundaryRecordVerified { .. }));
    let added = s
        .iter()
        .any(|x| matches!(x, Signal::PartitionCountIncreased { .. }));
    let (verdict, basis) = if s.iter().any(Signal::is_break) {
        (Verdict::Break, Basis::None)
    } else if ids_equal {
        (Verdict::Continuous, Basis::TopicId)
    } else if added && !verified {
        (Verdict::Suspected, Basis::None)
    } else if verified {
        (Verdict::Continuous, Basis::Content)
    } else {
        (Verdict::Unverified, Basis::Watermarks)
    };
    Outcome {
        verdict,
        basis,
        signals: s,
    }
}

/// The within-run check: what one capture says about itself (§4.4).
fn intra_run(c: &Capture) -> Vec<Signal> {
    let mut s = Vec::new();
    if let (Some(a), Some(b)) = (&c.topic_id, &c.topic_id_after) {
        if a != b {
            s.push(Signal::ChangedDuringCapture {
                partition: None,
                reason: "the topic ID changed during the capture",
            });
        }
    }
    for p in &c.partitions {
        let mut flag = |reason| {
            s.push(Signal::ChangedDuringCapture {
                partition: Some(p.partition),
                reason,
            })
        };
        if p.after.log_start < p.before.log_start {
            flag("the log start offset regressed during the capture");
        }
        if p.after.high_watermark < p.before.high_watermark {
            flag("the end offset regressed during the capture");
        }
        if let Some(a) = &p.archived {
            if a.first_offset < p.before.log_start {
                flag("the capture archived an offset below the pre-run log start");
            }
            if a.last_offset >= p.after.high_watermark {
                flag("the capture archived an offset at or beyond the post-run end");
            }
        }
    }
    s
}

/// A comparison read from run *k*'s own archive: exactly `offset`, among the
/// records the engine archived (offset → source-equivalent fingerprint).
/// Both sides are then the same engine's decoding of the broker's bytes.
fn probe_in_archive(archived: Option<&BTreeMap<i64, String>>, offset: i64) -> Probe {
    let Some(records) = archived else {
        return Probe::Absent(None);
    };
    match records.get(&offset) {
        Some(fp) => Probe::At(fp.clone()),
        None => Probe::Absent(records.range(offset..).next().map(|(o, _)| *o)),
    }
}

/// What run *k* knows about its predecessor (§3.3 arm 10).
#[derive(Debug, Clone)]
enum Predecessor<'a> {
    /// No committed point with this lineage key.
    None,
    /// A point without generation data: every receipt before 1.1.0.
    NotRecorded { point_id: &'a str },
    Recorded {
        point_id: &'a str,
        capture: &'a Capture,
    },
}

/// The receipt's `lineage` block.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Lineage {
    previous_point_id: Option<String>,
    /// Present exactly when no comparison was made.
    reason: Option<&'static str>,
    outcome: Outcome,
}

fn lineage(
    prev: &Predecessor<'_>,
    cur: &Current,
    probe: Option<&mut dyn FnMut(i32, i64) -> Probe>,
) -> Lineage {
    let no_comparison = |id: Option<&str>, reason: &'static str| {
        let signals = cur.within_run.clone();
        let verdict = if signals.iter().any(Signal::is_break) {
            Verdict::Break
        } else {
            Verdict::Unknown
        };
        Lineage {
            previous_point_id: id.map(str::to_string),
            reason: Some(reason),
            outcome: Outcome {
                verdict,
                basis: Basis::None,
                signals,
            },
        }
    };
    match prev {
        Predecessor::None => no_comparison(None, "noPredecessor"),
        Predecessor::NotRecorded { point_id } => {
            no_comparison(Some(point_id), "previousNotRecorded")
        }
        Predecessor::Recorded { point_id, capture } => Lineage {
            previous_point_id: Some(point_id.to_string()),
            reason: None,
            outcome: classify(capture, cur, probe),
        },
    }
}

/// Arm 10 over one receipt's lineage block and its own observation, as both
/// readers implement it. It checks the document against itself only: it
/// cannot see the predecessor.
fn arm10(l: &Lineage, own: &Capture) -> Result<(), &'static str> {
    let brk = l.outcome.signals.iter().any(Signal::is_break);
    if brk != (l.outcome.verdict == Verdict::Break) {
        return Err("a break-class signal is present if and only if the verdict is break");
    }
    if intra_run(own)
        .iter()
        .any(|x| !l.outcome.signals.contains(x))
    {
        return Err("the document's own marks derive a ChangedDuringCapture its lineage omits");
    }
    if (l.outcome.verdict == Verdict::Unknown) != (l.reason.is_some() && !brk) {
        return Err(
            "the verdict is unknown if and only if no comparison was made and nothing broke \
             during the capture",
        );
    }
    match (l.reason, l.previous_point_id.is_some()) {
        (Some("noPredecessor"), true) => Err("noPredecessor names no previous point"),
        (Some("previousNotRecorded"), false) => Err("previousNotRecorded names its previous point"),
        (None, false) => Err("a comparison names the point it compared with"),
        _ => Ok(()),
    }
}

// ===========================================================================
// The rule on its own: no broker. Each test kills one way the rule can drift.
// ===========================================================================

fn obs(partition: i32, ls: i64, hw: i64, tail: &[(i64, &str)]) -> PartitionObs {
    PartitionObs {
        partition,
        before: Marks {
            log_start: ls,
            high_watermark: hw,
        },
        after: Marks {
            log_start: ls,
            high_watermark: hw,
        },
        archived: (!tail.is_empty()).then(|| Archived {
            first_offset: ls,
            last_offset: tail[0].0,
            first_timestamp_ms: 0,
            last_timestamp_ms: 0,
            records: tail.len(),
            tail: tail
                .iter()
                .map(|(o, fp)| TailRecord {
                    offset: *o,
                    fingerprint: fp.to_string(),
                    raw_fingerprint: format!("raw-{fp}"),
                })
                .collect(),
        }),
    }
}

fn prev1(ls: i64, hw: i64, tail: &[(i64, &str)]) -> Capture {
    Capture {
        cluster_id: "c".into(),
        topic_id: None,
        topic_id_after: None,
        partition_count: 1,
        partitions: vec![obs(0, ls, hw, tail)],
    }
}

fn cur(count: i32, marks: &[(i64, i64)]) -> Current {
    Current {
        cluster_id: "c".into(),
        topic_id: None,
        partition_count: count,
        marks: marks
            .iter()
            .enumerate()
            .map(|(p, (ls, hw))| {
                (
                    p as i32,
                    Marks {
                        log_start: *ls,
                        high_watermark: *hw,
                    },
                )
            })
            .collect(),
        within_run: Vec::new(),
    }
}

fn with_ids(mut c: Capture, id: &str) -> Capture {
    c.topic_id = Some(id.into());
    c.topic_id_after = Some(id.into());
    c
}

fn cur_id(mut c: Current, id: &str) -> Current {
    c.topic_id = Some(id.into());
    c
}

fn run_rule(prev: &Capture, now: &Current, answers: &[(i64, Probe)]) -> (Verdict, Vec<Signal>) {
    let o = run_rule_full(prev, now, answers);
    (o.verdict, o.signals)
}

fn run_rule_full(prev: &Capture, now: &Current, answers: &[(i64, Probe)]) -> Outcome {
    let mut f = |_p: i32, o: i64| {
        answers
            .iter()
            .find(|(x, _)| *x == o)
            .map(|(_, r)| r.clone())
            .unwrap_or_else(|| panic!("unexpected probe at {o}"))
    };
    classify(prev, now, Some(&mut f))
}

#[test]
fn rule_a_matching_boundary_is_continuous() {
    let o = run_rule_full(
        &prev1(0, 10, &[(9, "a")]),
        &cur(1, &[(0, 12)]),
        &[(9, Probe::At("a".into()))],
    );
    assert_eq!((o.verdict, o.basis), (Verdict::Continuous, Basis::Content));
}

#[test]
fn rule_a_different_record_at_the_boundary_is_a_break() {
    let (v, s) = run_rule(
        &prev1(0, 10, &[(9, "a")]),
        &cur(1, &[(0, 12)]),
        &[(9, Probe::At("b".into()))],
    );
    assert_eq!(v, Verdict::Break);
    assert_eq!(
        s,
        vec![Signal::BoundaryRecordChanged {
            partition: 0,
            offset: 9
        }]
    );
}

#[test]
fn rule_an_end_regression_is_a_break_without_probing() {
    let (v, s) = run_rule(&prev1(0, 10, &[(9, "a")]), &cur(1, &[(0, 4)]), &[]);
    assert_eq!(v, Verdict::Break);
    assert!(s.contains(&Signal::EndRegressed {
        partition: 0,
        previous: 10,
        current: 4
    }));
}

#[test]
fn rule_a_log_start_regression_is_a_break() {
    let (v, s) = run_rule(
        &prev1(5, 10, &[(9, "a")]),
        &cur(1, &[(0, 12)]),
        &[(9, Probe::At("a".into()))],
    );
    assert_eq!(v, Verdict::Break);
    assert!(s.contains(&Signal::LogStartRegressed {
        partition: 0,
        previous: 5,
        current: 0
    }));
}

#[test]
fn rule_a_partition_count_decrease_is_a_break() {
    let prev = Capture {
        partition_count: 2,
        partitions: vec![obs(0, 0, 10, &[(9, "a")]), obs(1, 0, 10, &[(9, "b")])],
        ..prev1(0, 10, &[(9, "a")])
    };
    let (v, _) = run_rule(&prev, &cur(1, &[(0, 12)]), &[(9, Probe::At("a".into()))]);
    assert_eq!(v, Verdict::Break);
}

#[test]
fn rule_an_absent_boundary_tries_the_next_candidate() {
    let (v, s) = run_rule(
        &prev1(0, 10, &[(9, "a"), (8, "b")]),
        &cur(1, &[(0, 12)]),
        &[(9, Probe::Absent(Some(10))), (8, Probe::At("b".into()))],
    );
    assert_eq!(v, Verdict::Continuous, "{s:?}");
}

#[test]
fn rule_an_absent_boundary_is_never_a_break() {
    let (v, _) = run_rule(
        &prev1(0, 10, &[(9, "a")]),
        &cur(1, &[(0, 12)]),
        &[(9, Probe::Absent(Some(10)))],
    );
    assert_eq!(v, Verdict::Unverified);
}

#[test]
fn rule_an_out_of_range_boundary_is_deleted_and_never_a_break() {
    // The log start moved between the marks and the read: the record is
    // gone, which a continuous history allows.
    let o = run_rule_full(
        &prev1(0, 10, &[(9, "a"), (8, "b")]),
        &cur(1, &[(0, 12)]),
        &[(9, Probe::OutOfRange)],
    );
    assert_eq!(
        (o.verdict, o.basis),
        (Verdict::Unverified, Basis::Watermarks),
        "{o:?}"
    );
    assert_eq!(
        o.signals,
        vec![Signal::BoundaryDeleted {
            partition: 0,
            offset: 9,
            log_start: 0
        }]
    );
}

#[test]
fn rule_a_log_start_past_the_tail_is_a_gap_and_unverified() {
    let (v, s) = run_rule(&prev1(0, 10, &[(9, "a")]), &cur(1, &[(15, 15)]), &[]);
    assert_eq!(v, Verdict::Unverified);
    assert!(s.contains(&Signal::CaptureGap {
        partition: 0,
        from: 10,
        to: 15
    }));
}

#[test]
fn rule_a_log_start_just_past_the_tail_is_no_gap() {
    let (v, s) = run_rule(&prev1(0, 10, &[(9, "a")]), &cur(1, &[(10, 10)]), &[]);
    assert_eq!(v, Verdict::Unverified);
    assert!(
        !s.iter().any(|x| matches!(x, Signal::CaptureGap { .. })),
        "{s:?}"
    );
}

#[test]
fn rule_no_new_records_is_not_a_regression() {
    let (v, s) = run_rule(
        &prev1(0, 10, &[(9, "a")]),
        &cur(1, &[(0, 10)]),
        &[(9, Probe::At("a".into()))],
    );
    assert_eq!(v, Verdict::Continuous, "{s:?}");
}

#[test]
fn rule_added_partitions_without_verification_are_suspected() {
    let (v, _) = run_rule(
        &prev1(0, 10, &[(9, "a")]),
        &cur(2, &[(12, 20), (0, 3)]),
        &[],
    );
    assert_eq!(v, Verdict::Suspected);
}

#[test]
fn rule_added_partitions_with_verification_are_continuous() {
    let (v, _) = run_rule(
        &prev1(0, 10, &[(9, "a")]),
        &cur(2, &[(0, 20), (0, 3)]),
        &[(9, Probe::At("a".into()))],
    );
    assert_eq!(v, Verdict::Continuous);
}

#[test]
fn rule_another_cluster_is_unknown() {
    let mut now = cur(1, &[(0, 12)]);
    now.cluster_id = "other".into();
    let (v, _) = run_rule(&prev1(0, 10, &[(9, "a")]), &now, &[]);
    assert_eq!(v, Verdict::Unknown);
}

#[test]
fn rule_offsets_only_never_probes() {
    let o = classify(&prev1(0, 10, &[(9, "a")]), &cur(1, &[(0, 12)]), None);
    assert_eq!(o.verdict, Verdict::Unverified);
}

#[test]
fn rule_a_change_within_run_k_is_a_break() {
    let mut k = prev1(0, 12, &[(11, "z")]);
    k.partitions[0].after.high_watermark = 11;
    let (v, s) = run_rule(
        &prev1(0, 10, &[(9, "a")]),
        &Current::of(&k),
        &[(9, Probe::At("a".into()))],
    );
    assert_eq!(v, Verdict::Break, "{s:?}");
    assert!(s
        .iter()
        .any(|x| matches!(x, Signal::ChangedDuringCapture { .. })));
}

// R1: the ID path (review M4).

#[test]
fn ids_that_differ_are_a_break_whatever_the_offsets_say() {
    let o = run_rule_full(
        &with_ids(prev1(0, 10, &[(9, "a")]), "id-1"),
        &cur_id(cur(1, &[(0, 12)]), "id-2"),
        &[],
    );
    assert_eq!(o.verdict, Verdict::Break);
    assert_eq!(
        o.signals,
        vec![Signal::TopicIdChanged {
            previous: "id-1".into(),
            current: "id-2".into()
        }]
    );
}

#[test]
fn equal_ids_still_report_a_capture_gap() {
    let o = run_rule_full(
        &with_ids(prev1(0, 10, &[(9, "a")]), "id-1"),
        &cur_id(cur(1, &[(15, 20)]), "id-1"),
        &[],
    );
    assert_eq!((o.verdict, o.basis), (Verdict::Continuous, Basis::TopicId));
    assert!(o.signals.contains(&Signal::CaptureGap {
        partition: 0,
        from: 10,
        to: 15
    }));
}

#[test]
fn equal_ids_still_catch_a_refilled_truncation() {
    // Truncated below the old end under the same ID (unclean election), then
    // refilled past it: no offset regresses, but offset 9 is another record.
    let o = run_rule_full(
        &with_ids(prev1(0, 10, &[(9, "a")]), "id-1"),
        &cur_id(cur(1, &[(0, 14)]), "id-1"),
        &[(9, Probe::At("b".into()))],
    );
    assert_eq!(o.verdict, Verdict::Break, "{o:?}");
    assert!(o.signals.contains(&Signal::BoundaryRecordChanged {
        partition: 0,
        offset: 9
    }));
}

#[test]
fn equal_ids_make_added_partitions_continuous() {
    let o = run_rule_full(
        &with_ids(prev1(0, 10, &[(9, "a")]), "id-1"),
        &cur_id(cur(2, &[(12, 20), (0, 3)]), "id-1"),
        &[],
    );
    assert_eq!((o.verdict, o.basis), (Verdict::Continuous, Basis::TopicId));
}

#[test]
fn one_id_alone_decides_nothing() {
    let o = run_rule_full(
        &prev1(0, 10, &[(9, "a")]),
        &cur_id(cur(1, &[(0, 12)]), "id-2"),
        &[(9, Probe::At("a".into()))],
    );
    assert_eq!((o.verdict, o.basis), (Verdict::Continuous, Basis::Content));
}

// The within-run check: one test per condition (review M2).

#[test]
fn intra_run_flags_an_archive_beyond_the_post_run_end() {
    let mut c = prev1(0, 10, &[(9, "a")]);
    c.partitions[0].after.high_watermark = 5;
    let s = intra_run(&c);
    assert!(s.iter().any(Signal::is_break), "{s:?}");
    assert!(intra_run(&prev1(0, 10, &[(9, "a")])).is_empty());
}

#[test]
fn intra_run_flags_an_archive_that_ends_at_the_post_run_end() {
    // Offset 9 archived, and the partition now ends at 9: offset 9 no longer
    // exists, which one continuous history cannot produce.
    let mut c = prev1(0, 10, &[(9, "a")]);
    c.partitions[0].before.high_watermark = 9;
    c.partitions[0].after.high_watermark = 9;
    let s = intra_run(&c);
    assert_eq!(s.len(), 1, "{s:?}");
}

#[test]
fn intra_run_flags_a_log_start_that_regressed_during_the_capture() {
    let mut c = prev1(5, 10, &[(9, "a")]);
    c.partitions[0].after.log_start = 0;
    assert_eq!(
        intra_run(&c),
        vec![Signal::ChangedDuringCapture {
            partition: Some(0),
            reason: "the log start offset regressed during the capture"
        }]
    );
}

#[test]
fn intra_run_flags_an_end_that_regressed_during_the_capture() {
    // The end fell from 20 to 12 while the archive stopped at 9: only this
    // condition holds.
    let mut c = prev1(0, 20, &[(9, "a")]);
    c.partitions[0].after.high_watermark = 12;
    assert_eq!(
        intra_run(&c),
        vec![Signal::ChangedDuringCapture {
            partition: Some(0),
            reason: "the end offset regressed during the capture"
        }]
    );
}

#[test]
fn intra_run_flags_an_archived_offset_below_the_pre_run_log_start() {
    // The engine archived offset 0 of a partition whose log started at 5.
    let mut c = prev1(5, 10, &[(9, "a")]);
    c.partitions[0]
        .archived
        .as_mut()
        .expect("archived")
        .first_offset = 0;
    assert_eq!(
        intra_run(&c),
        vec![Signal::ChangedDuringCapture {
            partition: Some(0),
            reason: "the capture archived an offset below the pre-run log start"
        }]
    );
}

#[test]
fn intra_run_flags_a_topic_id_that_changed_during_the_capture() {
    let mut c = with_ids(prev1(0, 10, &[(9, "a")]), "id-1");
    c.topic_id_after = Some("id-2".into());
    assert_eq!(
        intra_run(&c),
        vec![Signal::ChangedDuringCapture {
            partition: None,
            reason: "the topic ID changed during the capture"
        }]
    );
    c.topic_id_after = None;
    assert!(intra_run(&c).is_empty(), "one ID alone proves nothing");
}

// The archive-side comparison read (review H1).

#[test]
fn an_archive_read_is_exact() {
    let archived: BTreeMap<i64, String> = [(7, "x".to_string()), (9, "y".to_string())].into();
    assert_eq!(probe_in_archive(Some(&archived), 9), Probe::At("y".into()));
    assert_eq!(probe_in_archive(Some(&archived), 8), Probe::Absent(Some(9)));
    assert_eq!(probe_in_archive(Some(&archived), 10), Probe::Absent(None));
    assert_eq!(probe_in_archive(None, 9), Probe::Absent(None));
}

// The predecessor rule and arm 10 (review M3).

#[test]
fn the_first_receipt_after_the_upgrade_names_its_predecessor_and_verifies() {
    let own = prev1(0, 10, &[(9, "a")]);
    let l = lineage(
        &Predecessor::NotRecorded {
            point_id: "lwp1-old",
        },
        &Current::of(&own),
        None,
    );
    assert_eq!(l.previous_point_id.as_deref(), Some("lwp1-old"));
    assert_eq!(
        (l.outcome.verdict, l.reason),
        (Verdict::Unknown, Some("previousNotRecorded"))
    );
    assert_eq!(arm10(&l, &own), Ok(()));
}

#[test]
fn no_predecessor_names_none_and_verifies() {
    let own = prev1(0, 10, &[(9, "a")]);
    let l = lineage(&Predecessor::None, &Current::of(&own), None);
    assert_eq!(
        (l.previous_point_id.as_deref(), l.reason),
        (None, Some("noPredecessor"))
    );
    assert_eq!(l.outcome.verdict, Verdict::Unknown);
    assert_eq!(arm10(&l, &own), Ok(()));
}

#[test]
fn a_change_within_the_first_run_is_a_break_even_without_a_predecessor() {
    let mut own = prev1(0, 10, &[(9, "a")]);
    own.partitions[0].after.high_watermark = 5;
    let l = lineage(&Predecessor::None, &Current::of(&own), None);
    assert_eq!(l.outcome.verdict, Verdict::Break);
    assert_eq!(arm10(&l, &own), Ok(()));
}

#[test]
fn a_recorded_predecessor_is_compared() {
    let prev = prev1(0, 10, &[(9, "a")]);
    let own = prev1(0, 12, &[(11, "c")]);
    let mut f = |_p: i32, _o: i64| Probe::At("a".into());
    let l = lineage(
        &Predecessor::Recorded {
            point_id: "lwp1-prev",
            capture: &prev,
        },
        &Current::of(&own),
        Some(&mut f),
    );
    assert_eq!((l.reason, l.outcome.verdict), (None, Verdict::Continuous));
    assert_eq!(arm10(&l, &own), Ok(()));
}

#[test]
fn arm10_refuses_each_self_contradiction() {
    let own = prev1(0, 10, &[(9, "a")]);
    let good = lineage(
        &Predecessor::NotRecorded {
            point_id: "lwp1-old",
        },
        &Current::of(&own),
        None,
    );
    let mut l = good.clone();
    l.reason = Some("noPredecessor");
    assert!(
        arm10(&l, &own).is_err(),
        "noPredecessor beside a named point"
    );
    let mut l = good.clone();
    l.previous_point_id = None;
    assert!(
        arm10(&l, &own).is_err(),
        "previousNotRecorded without a point"
    );
    let mut l = good.clone();
    l.reason = None;
    assert!(arm10(&l, &own).is_err(), "unknown after a comparison");
    let mut l = good.clone();
    l.outcome.verdict = Verdict::Continuous;
    l.outcome.signals = vec![Signal::EndRegressed {
        partition: 0,
        previous: 10,
        current: 4,
    }];
    assert!(arm10(&l, &own).is_err(), "continuous beside a break signal");
    let mut hid = own.clone();
    hid.partitions[0].after.high_watermark = 5;
    assert!(
        arm10(&good, &hid).is_err(),
        "a ChangedDuringCapture the document's own marks derive, left out"
    );
}

// ===========================================================================
// The live rows: broker, engine, archive. `e2e` feature only.
// ===========================================================================

// `harness/mod.rs` gates itself with `#![cfg(feature = "e2e")]`.
mod harness;

#[cfg(feature = "e2e")]
mod live {
    use super::*;
    use crate::harness::*;

    use logweir_engine_oso::kbak::{decode_segment, ArchivedRecord};
    use logweir_kafka::fingerprint::record_fingerprint;
    use logweir_kafka::rdkafka_reader::RdKafkaReader;
    use logweir_kafka::reader::{
        AuthConfig, ClusterReader, NewTopicSpec, TopicCreator, TopicDeleter,
    };
    use rdkafka::config::ClientConfig;
    use rdkafka::consumer::{BaseConsumer, Consumer};
    use rdkafka::error::{KafkaError, RDKafkaErrorCode};
    use rdkafka::message::{BorrowedMessage, Header, Headers, OwnedHeaders};
    use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
    use rdkafka::{Message, Offset, Timestamp, TopicPartitionList};
    use serde_json::{json, Value};
    use std::path::{Path, PathBuf};
    use std::process::{Command, Output, Stdio};
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    /// Every client call's own budget.
    const T: Duration = Duration::from_secs(20);

    /// The topic prefix every row uses. `RdKafkaReader::delete_topics` is
    /// scoped to it, so this file can delete nothing else.
    const PREFIX: &str = "ti-";

    /// Tail candidates, newest first. More than one because compaction can
    /// remove the newest, and a consumer never returns a transaction marker.
    const TAIL: usize = 3;

    /// Which listener an address is for.
    #[derive(Debug, Clone, Copy)]
    enum Side {
        /// A client on the host: this process, and the engine (the docker
        /// shim maps `localhost` to the host).
        Host,
        /// A Kafka tool inside the broker's own container.
        InNetwork,
        /// A fake broker this process listens with on the host's loopback.
        Loopback(u16),
    }

    /// EVERY broker address this file dials, in ONE place (review M1).
    /// `Host` is the harness's `bootstrap()` (PROD-01.5): the stack this
    /// process addresses, default or slot, checked coherent first.
    /// `InNetwork` is the listener tools inside the broker container use, the
    /// same on every slot.
    fn broker_address(side: Side) -> String {
        match side {
            Side::Host => bootstrap(),
            Side::InNetwork => "kafka-broker-1:9094".to_string(),
            Side::Loopback(port) => format!("localhost:{port}"),
        }
    }

    impl Signal {
        fn to_json(&self) -> Value {
            match self {
                Signal::TopicIdChanged { previous, current } => {
                    json!({"signal": "TopicIdChanged", "previous": previous, "current": current})
                }
                Signal::PartitionCountDecreased { previous, current } => {
                    json!({"signal": "PartitionCountDecreased", "previous": previous, "current": current})
                }
                Signal::PartitionCountIncreased { previous, current } => {
                    json!({"signal": "PartitionCountIncreased", "previous": previous, "current": current})
                }
                Signal::LogStartRegressed {
                    partition,
                    previous,
                    current,
                } => json!({"signal": "LogStartRegressed", "partition": partition,
                            "previous": previous, "current": current}),
                Signal::EndRegressed {
                    partition,
                    previous,
                    current,
                } => json!({"signal": "EndRegressed", "partition": partition,
                            "previous": previous, "current": current}),
                Signal::BoundaryRecordChanged { partition, offset } => {
                    json!({"signal": "BoundaryRecordChanged", "partition": partition, "offset": offset})
                }
                Signal::BoundaryRecordVerified { partition, offset } => {
                    json!({"signal": "BoundaryRecordVerified", "partition": partition, "offset": offset})
                }
                Signal::BoundaryRecordAbsent {
                    partition,
                    offset,
                    returned,
                } => json!({"signal": "BoundaryRecordAbsent", "partition": partition,
                            "offset": offset, "returned": returned}),
                Signal::BoundaryDeleted {
                    partition,
                    offset,
                    log_start,
                } => json!({"signal": "BoundaryDeleted", "partition": partition,
                            "offset": offset, "log_start": log_start}),
                Signal::CaptureGap {
                    partition,
                    from,
                    to,
                } => {
                    json!({"signal": "CaptureGap", "partition": partition, "from": from, "to": to})
                }
                Signal::ChangedDuringCapture { partition, reason } => {
                    json!({"signal": "ChangedDuringCapture", "partition": partition, "reason": reason})
                }
            }
        }
    }

    impl Verdict {
        fn as_str(self) -> &'static str {
            match self {
                Verdict::Continuous => "continuous",
                Verdict::Unverified => "unverified",
                Verdict::Suspected => "suspected",
                Verdict::Break => "break",
                Verdict::Unknown => "unknown",
            }
        }
    }

    impl Basis {
        fn as_str(self) -> &'static str {
            match self {
                Basis::TopicId => "topicId",
                Basis::Content => "content",
                Basis::Watermarks => "watermarks",
                Basis::None => "none",
            }
        }
    }

    fn outcome_json(o: &Outcome, probes: Vec<Value>) -> Value {
        json!({
            "verdict": o.verdict.as_str(),
            "basis": o.basis.as_str(),
            "signals": o.signals.iter().map(Signal::to_json).collect::<Vec<_>>(),
            "probes": probes,
        })
    }

    // -----------------------------------------------------------------------
    // Subprocesses: every one has a deadline, and so does the engine itself.
    // -----------------------------------------------------------------------

    /// `cmd` to completion, or killed after `secs`. On a timeout `on_timeout`
    /// runs after the child is killed: for the engine's docker route, killing
    /// the `docker` client leaves the container running, so the caller kills
    /// the container too (review L9).
    fn try_run_bounded(
        mut cmd: Command,
        secs: u64,
        on_timeout: &dyn Fn(),
    ) -> Result<Output, String> {
        use std::io::Read;
        let mut child = cmd
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| format!("could not start: {e}"))?;
        let mut out = child.stdout.take().expect("piped stdout");
        let mut err = child.stderr.take().expect("piped stderr");
        let t_out = std::thread::spawn(move || {
            let mut b = Vec::new();
            let _ = out.read_to_end(&mut b);
            b
        });
        let t_err = std::thread::spawn(move || {
            let mut b = Vec::new();
            let _ = err.read_to_end(&mut b);
            b
        });
        let deadline = Instant::now() + Duration::from_secs(secs);
        let status = loop {
            if let Some(status) = child.try_wait().map_err(|e| e.to_string())? {
                break status;
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                on_timeout();
                return Err(format!("still running after {secs}s, killed"));
            }
            std::thread::sleep(Duration::from_millis(100));
        };
        Ok(Output {
            status,
            stdout: t_out.join().expect("stdout reader"),
            stderr: t_err.join().expect("stderr reader"),
        })
    }

    fn run_bounded(cmd: Command, secs: u64, what: &str) -> Output {
        try_run_bounded(cmd, secs, &|| {}).unwrap_or_else(|e| panic!("{what}: {e}"))
    }

    fn docker(args: &[&str], what: &str) -> Output {
        let mut c = Command::new("docker");
        c.args(args);
        run_bounded(c, 30, what)
    }

    /// Running containers whose arguments name `cfg`: the engine's docker
    /// route passes the config path through, and every config path here is
    /// unique to one capture.
    fn engine_containers_for(cfg: &Path) -> Vec<String> {
        let ps = docker(&["ps", "-q", "--no-trunc"], "docker ps");
        let ids: Vec<String> = ps
            .stdout_utf8()
            .split_whitespace()
            .map(String::from)
            .collect();
        if ids.is_empty() {
            return Vec::new();
        }
        let mut args = vec!["inspect", "-f", "{{.Id}} {{json .Args}}"];
        args.extend(ids.iter().map(String::as_str));
        let needle = cfg.display().to_string();
        // A container that exits between `ps` and `inspect` fails the call;
        // its line is simply absent, which is the answer.
        docker(&args, "docker inspect")
            .stdout_utf8()
            .lines()
            .filter(|l| l.contains(&needle))
            .filter_map(|l| l.split_whitespace().next().map(String::from))
            .collect()
    }

    fn kill_engine_containers(cfg: &Path) {
        for id in engine_containers_for(cfg) {
            let _ = docker(&["kill", &id], "docker kill");
        }
    }

    /// A Kafka command-line tool inside the RUNNING broker container. Killing
    /// the `docker` client on a timeout leaves the tool running inside the
    /// broker; `just e2e-down` ends it with the container.
    fn broker_cli(args: &[&str], what: &str) -> String {
        // The project is COMPOSE_PROJECT_NAME's: refuse an environment that
        // is not one coherent stack before reaching one (PROD-01.5).
        stack::ensure_coherent();
        let mut c = Command::new("docker");
        c.args([
            "compose",
            "-f",
            "e2e/compose/docker-compose.yml",
            "exec",
            "-T",
            "kafka-broker-1",
        ]);
        c.args(args);
        c.current_dir(root());
        let o = run_bounded(c, 120, what);
        assert!(
            o.status.success(),
            "{what} failed (exit {:?}) — is the stack up? run `just e2e-up`\n{}\n{}",
            o.status.code(),
            o.stdout_utf8(),
            o.stderr_utf8()
        );
        o.stdout_utf8()
    }

    /// The broker's own topic ID: the ground truth every row is judged
    /// against.
    fn topic_id(topic: &str) -> String {
        let inside = broker_address(Side::InNetwork);
        let out = broker_cli(
            &[
                "/opt/kafka/bin/kafka-topics.sh",
                "--bootstrap-server",
                &inside,
                "--describe",
                "--topic",
                topic,
            ],
            "kafka-topics --describe",
        );
        let id = out
            .split_whitespace()
            .skip_while(|w| *w != "TopicId:")
            .nth(1)
            .unwrap_or_else(|| panic!("no TopicId in the describe output for {topic}:\n{out}"));
        assert_eq!(
            id.len(),
            22,
            "a Kafka topic ID prints as 22 base64url characters: {id:?}"
        );
        id.to_string()
    }

    fn kafka_version() -> String {
        static V: std::sync::OnceLock<String> = std::sync::OnceLock::new();
        V.get_or_init(|| {
            broker_cli(
                &["/opt/kafka/bin/kafka-topics.sh", "--version"],
                "kafka-topics --version",
            )
            .split_whitespace()
            .next()
            .unwrap_or("unknown")
            .to_string()
        })
        .clone()
    }

    fn delete_records(topic: &str, to: &[(i32, i64)]) {
        let parts: Vec<String> = to
            .iter()
            .map(|(p, o)| format!(r#"{{"topic":"{topic}","partition":{p},"offset":{o}}}"#))
            .collect();
        let json = format!(r#"{{"partitions":[{}],"version":1}}"#, parts.join(","));
        let file = format!("/tmp/{topic}-delete-records.json");
        let inside = broker_address(Side::InNetwork);
        let script = format!(
            "printf '%s' '{json}' > {file} && /opt/kafka/bin/kafka-delete-records.sh \
             --bootstrap-server {inside} --offset-json-file {file}"
        );
        broker_cli(&["sh", "-c", &script], "kafka-delete-records");
    }

    fn alter_partitions(topic: &str, partitions: i32) {
        let inside = broker_address(Side::InNetwork);
        broker_cli(
            &[
                "/opt/kafka/bin/kafka-topics.sh",
                "--bootstrap-server",
                &inside,
                "--alter",
                "--topic",
                topic,
                "--partitions",
                &partitions.to_string(),
            ],
            "kafka-topics --alter --partitions",
        );
        wait_for(
            60,
            &format!("{topic} to report {partitions} partitions"),
            || partition_count(topic) == Some(partitions),
        );
        // FX-18: the ADDED partitions are created partitions, with the same
        // window between "listed" and "served" as a created topic.
        await_created_on(&broker_address(Side::Host), topic, partitions);
    }

    fn alter_config(topic: &str, entry: &str) {
        let inside = broker_address(Side::InNetwork);
        broker_cli(
            &[
                "/opt/kafka/bin/kafka-configs.sh",
                "--bootstrap-server",
                &inside,
                "--alter",
                "--entity-type",
                "topics",
                "--entity-name",
                topic,
                "--add-config",
                entry,
            ],
            "kafka-configs --alter",
        );
    }

    fn wait_for(secs: u64, what: &str, mut ready: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(secs);
        while Instant::now() < deadline {
            if ready() {
                return;
            }
            std::thread::sleep(Duration::from_millis(500));
        }
        panic!("timed out after {secs}s waiting for {what}");
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Isolation {
        ReadUncommitted,
        ReadCommitted,
    }

    impl Isolation {
        fn as_str(self) -> &'static str {
            match self {
                Isolation::ReadUncommitted => "read_uncommitted",
                Isolation::ReadCommitted => "read_committed",
            }
        }
    }

    fn consumer(isolation: Isolation) -> BaseConsumer {
        ClientConfig::new()
            .set("bootstrap.servers", broker_address(Side::Host))
            .set("group.id", "logweir-topic-identity-oracle")
            .set("enable.auto.commit", "false")
            .set("enable.partition.eof", "true")
            .set("auto.offset.reset", "error")
            .set("allow.auto.create.topics", "false")
            .set("isolation.level", isolation.as_str())
            .create()
            .expect("a consumer for the compose broker")
    }

    /// Healthy partition count, or `None` while the topic is absent or
    /// electing.
    fn partition_count(topic: &str) -> Option<i32> {
        let c = consumer(Isolation::ReadUncommitted);
        let md = c.fetch_metadata(Some(topic), T).ok()?;
        let t = md.topics().first()?;
        if t.error().is_some() || t.partitions().is_empty() {
            return None;
        }
        if t.partitions().iter().any(|p| p.leader() < 0) {
            return None;
        }
        Some(t.partitions().len() as i32)
    }

    fn marks(topic: &str, isolation: Isolation) -> BTreeMap<i32, Marks> {
        let c = consumer(isolation);
        let n = partition_count(topic).unwrap_or_else(|| panic!("{topic} has no healthy metadata"));
        (0..n)
            .map(|p| {
                let (lo, hi) = c
                    .fetch_watermarks(topic, p, T)
                    .unwrap_or_else(|e| panic!("watermarks {topic}/{p}: {e}"));
                (
                    p,
                    Marks {
                        log_start: lo,
                        high_watermark: hi,
                    },
                )
            })
            .collect()
    }

    fn message_headers(m: &BorrowedMessage<'_>) -> Vec<(String, Option<Vec<u8>>)> {
        m.headers()
            .map(|hs| {
                (0..hs.count())
                    .map(|i| {
                        let h = hs.get(i);
                        (h.key.to_string(), h.value.map(|v| v.to_vec()))
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    fn message_fingerprint(m: &BorrowedMessage<'_>) -> String {
        record_fingerprint(
            m.key(),
            m.payload(),
            &message_headers(m),
            m.timestamp().to_millis().unwrap_or(-1),
        )
    }

    /// A SOURCE read at EXACTLY `offset`, READ_UNCOMMITTED: the variant the
    /// decision rejects, kept to measure its false positives (c17, c18). The
    /// first record returned counts only if it carries the requested offset.
    fn source_probe(topic: &str, partition: i32, offset: i64) -> Probe {
        let c = consumer(Isolation::ReadUncommitted);
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(topic, partition, Offset::Offset(offset))
            .expect("add partition");
        c.assign(&tpl).expect("assign");
        let deadline = Instant::now() + T;
        while Instant::now() < deadline {
            match c.poll(Duration::from_millis(500)) {
                None => continue,
                Some(Err(KafkaError::PartitionEOF(_))) => return Probe::Absent(None),
                Some(Err(KafkaError::MessageConsumption(RDKafkaErrorCode::AutoOffsetReset))) => {
                    return Probe::OutOfRange
                }
                Some(Err(e)) => panic!("probe {topic}/{partition}@{offset}: {e}"),
                Some(Ok(m)) if m.offset() == offset => return Probe::At(message_fingerprint(&m)),
                Some(Ok(m)) => return Probe::Absent(Some(m.offset())),
            }
        }
        panic!("probe {topic}/{partition}@{offset}: no answer within {T:?}");
    }

    /// What a source read returns at one offset, for the evidence: the
    /// timestamp and its type, and the header count.
    fn source_record_summary(topic: &str, partition: i32, offset: i64) -> Value {
        let c = consumer(Isolation::ReadUncommitted);
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(topic, partition, Offset::Offset(offset))
            .expect("add partition");
        c.assign(&tpl).expect("assign");
        let deadline = Instant::now() + T;
        while Instant::now() < deadline {
            match c.poll(Duration::from_millis(500)) {
                Some(Ok(m)) if m.offset() == offset => {
                    let (kind, ms) = match m.timestamp() {
                        Timestamp::CreateTime(t) => ("CreateTime", t),
                        Timestamp::LogAppendTime(t) => ("LogAppendTime", t),
                        Timestamp::NotAvailable => ("NotAvailable", -1),
                    };
                    let headers = message_headers(&m);
                    return json!({"offset": offset, "timestamp_type": kind, "timestamp_ms": ms,
                                  "header_keys": headers.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>()});
                }
                Some(Ok(m)) => return json!({"offset": offset, "absent": true, "next": m.offset()}),
                Some(Err(e)) => return json!({"offset": offset, "error": e.to_string()}),
                None => continue,
            }
        }
        json!({"offset": offset, "error": "no answer"})
    }

    /// The fingerprint of the SOURCE record an archived record came from, and
    /// of the archived bytes verbatim.
    ///
    /// Logweir renders `include_offset_headers: true`, so the engine appends
    /// `x-original-offset` and `x-original-timestamp` (little-endian `i64`)
    /// AFTER the record's own headers. Only those two TRAILING headers are
    /// removed, and only when their values are this record's own offset and
    /// timestamp: a record that already carried such headers at the source
    /// keeps its own inner pair.
    fn archived_fingerprints(r: &ArchivedRecord) -> (String, String) {
        let raw = record_fingerprint(
            r.key.as_deref(),
            r.value.as_deref(),
            &r.headers,
            r.timestamp,
        );
        let n = r.headers.len();
        assert!(
            n >= 2,
            "archived record at {} carries {n} headers; the engine appends two",
            r.offset
        );
        let (own, appended) = r.headers.split_at(n - 2);
        assert_eq!(appended[0].0, "x-original-offset", "at {}", r.offset);
        assert_eq!(
            appended[0].1.as_deref(),
            Some(&r.offset.to_le_bytes()[..]),
            "x-original-offset at {} is not the record's own offset",
            r.offset
        );
        assert_eq!(appended[1].0, "x-original-timestamp", "at {}", r.offset);
        assert_eq!(
            appended[1].1.as_deref(),
            Some(&r.timestamp.to_le_bytes()[..]),
            "x-original-timestamp at {} is not the record's own timestamp",
            r.offset
        );
        let source = record_fingerprint(r.key.as_deref(), r.value.as_deref(), own, r.timestamp);
        (source, raw)
    }

    /// One partition of one capture's archive, read back.
    struct ArchivedPartition {
        summary: Archived,
        /// Every decoded record, by offset.
        records: Vec<ArchivedRecord>,
        /// Offset → source-equivalent fingerprint, for the comparison read.
        by_offset: BTreeMap<i64, String>,
    }

    /// One capture's archive, read back.
    struct Archive {
        partition_count: Option<i64>,
        partitions: BTreeMap<i32, ArchivedPartition>,
        gaps: Value,
    }

    fn engine_config(backup_id: &str, bootstrap: &str, topic: &str, archive: &Path) -> String {
        format!(
            "mode: backup\nbackup_id: \"{backup_id}\"\nsource:\n  bootstrap_servers:\n    - {bootstrap}\n  \
             topics:\n    include:\n      - \"{topic}\"\nstorage:\n  backend: filesystem\n  path: \"{}\"\n\
             backup:\n  compression: zstd\n  continuous: false\n  segment_max_records: 1000\n  \
             include_offset_headers: true\n",
            archive.display()
        )
    }

    fn engine_command(cfg: &Path) -> Command {
        let mut c = Command::new(engine_bin());
        c.env("LOGWEIR_E2E_ENGINE_MOUNT", engine_mount())
            .env("RUST_LOG", "warn")
            .arg("backup")
            .arg("--config")
            .arg(cfg)
            .current_dir(root());
        c
    }

    /// The pinned engine backs `topic` up into a filesystem archive under
    /// `engine_mount()`, the one host directory its container route can
    /// write. A timeout kills the engine's container as well as the client.
    fn engine_backup(dir: &Path, backup_id: &str, topic: &str) -> Archive {
        let archive = dir.join("archive");
        std::fs::create_dir_all(&archive).expect("archive dir");
        let cfg = dir.join(format!("{backup_id}.yaml"));
        std::fs::write(
            &cfg,
            engine_config(backup_id, &broker_address(Side::Host), topic, &archive),
        )
        .expect("engine config");
        let o = try_run_bounded(engine_command(&cfg), 300, &|| kill_engine_containers(&cfg))
            .unwrap_or_else(|e| panic!("engine backup of {topic}: {e}"));
        assert!(
            o.status.success(),
            "engine backup of {topic} exited {:?}\n{}\n{}",
            o.status.code(),
            o.stdout_utf8(),
            o.stderr_utf8()
        );
        let manifest_path = archive.join(backup_id).join("manifest.json");
        let manifest: Value = serde_json::from_slice(
            &std::fs::read(&manifest_path)
                .unwrap_or_else(|e| panic!("{}: {e}", manifest_path.display())),
        )
        .expect("manifest json");
        let entry = manifest["topics"]
            .as_array()
            .and_then(|ts| ts.iter().find(|t| t["name"] == topic))
            .unwrap_or_else(|| panic!("manifest names no topic {topic}: {manifest}"))
            .clone();
        let mut partitions = BTreeMap::new();
        let mut gaps = serde_json::Map::new();
        for p in entry["partitions"].as_array().cloned().unwrap_or_default() {
            let pid = p["partition_id"].as_i64().expect("partition_id") as i32;
            let segments = p["segments"].as_array().cloned().unwrap_or_default();
            if segments.is_empty() {
                continue;
            }
            if let Some(g) = p.get("gaps") {
                gaps.insert(pid.to_string(), g.clone());
            }
            let mut records = Vec::new();
            let (mut first_ts, mut last_ts) = (None, None);
            let (mut lo, mut hi) = (i64::MAX, i64::MIN);
            for s in &segments {
                let key = s["key"].as_str().expect("segment key");
                let bytes =
                    std::fs::read(archive.join(key)).unwrap_or_else(|e| panic!("{key}: {e}"));
                records.extend(decode_segment(&bytes).unwrap_or_else(|e| panic!("{key}: {e}")));
                let start = s["start_offset"].as_i64().expect("start_offset");
                let end = s["end_offset"].as_i64().expect("end_offset");
                if start < lo {
                    lo = start;
                    first_ts = s["start_timestamp"].as_i64();
                }
                if end > hi {
                    hi = end;
                    last_ts = s["end_timestamp"].as_i64();
                }
            }
            records.sort_by_key(|r| r.offset);
            let first = records.first().expect("a segment decodes to records");
            let last = records.last().expect("a segment decodes to records");
            assert_eq!(
                (first.offset, last.offset),
                (lo, hi),
                "{topic}/{pid}: the manifest's offset bounds disagree with the decoded records"
            );
            let by_offset: BTreeMap<i64, String> = records
                .iter()
                .map(|r| (r.offset, archived_fingerprints(r).0))
                .collect();
            let tail = records
                .iter()
                .rev()
                .take(TAIL)
                .map(|r| {
                    let (fingerprint, raw_fingerprint) = archived_fingerprints(r);
                    TailRecord {
                        offset: r.offset,
                        fingerprint,
                        raw_fingerprint,
                    }
                })
                .collect();
            let archived = Archived {
                first_offset: lo,
                last_offset: hi,
                first_timestamp_ms: first_ts.expect("start_timestamp"),
                last_timestamp_ms: last_ts.expect("end_timestamp"),
                records: records.len(),
                tail,
            };
            partitions.insert(
                pid,
                ArchivedPartition {
                    summary: archived,
                    records,
                    by_offset,
                },
            );
        }
        Archive {
            partition_count: entry["original_partition_count"].as_i64(),
            partitions,
            gaps: Value::Object(gaps),
        }
    }

    /// One capture: the topic ID and marks before (both isolation levels),
    /// the engine, then the marks and the ID after.
    struct CaptureRun {
        capture: Capture,
        /// The broker's IDs before and after the engine: ground truth, and
        /// the ID path's input.
        ids: (String, String),
        rc_before: BTreeMap<i32, Marks>,
        rc_after: BTreeMap<i32, Marks>,
        archive: Archive,
    }

    impl CaptureRun {
        /// This capture as the ID path sees it: PROD-01.4a's DescribeTopics
        /// would read the same IDs the broker's CLI prints.
        fn with_broker_ids(&self) -> Capture {
            Capture {
                topic_id: Some(self.ids.0.clone()),
                topic_id_after: Some(self.ids.1.clone()),
                ..self.capture.clone()
            }
        }
    }

    fn capture(topic: &str, dir: &Path, backup_id: &str) -> CaptureRun {
        let cluster = cluster_id();
        let count = partition_count(topic).expect("healthy topic before the capture");
        let id_before = topic_id(topic);
        let before = marks(topic, Isolation::ReadUncommitted);
        let rc_before = marks(topic, Isolation::ReadCommitted);
        let archive = engine_backup(dir, backup_id, topic);
        let after = marks(topic, Isolation::ReadUncommitted);
        let rc_after = marks(topic, Isolation::ReadCommitted);
        let id_after = topic_id(topic);
        let partitions = (0..count)
            .map(|p| PartitionObs {
                partition: p,
                before: before[&p],
                after: after[&p],
                archived: archive.partitions.get(&p).map(|a| a.summary.clone()),
            })
            .collect();
        CaptureRun {
            capture: Capture {
                cluster_id: cluster,
                // The heuristic path has no ID: Logweir cannot read one yet.
                topic_id: None,
                topic_id_after: None,
                partition_count: count,
                partitions,
            },
            ids: (id_before, id_after),
            rc_before,
            rc_after,
            archive,
        }
    }

    fn bare_current(topic: &str, isolation: Isolation) -> Current {
        Current {
            cluster_id: cluster_id(),
            topic_id: None,
            partition_count: partition_count(topic).expect("healthy topic"),
            marks: marks(topic, isolation),
            within_run: Vec::new(),
        }
    }

    /// One record to produce, fully specified.
    #[derive(Debug, Clone)]
    struct Rec {
        partition: i32,
        key: Option<Vec<u8>>,
        value: Option<Vec<u8>>,
        headers: Vec<(String, Option<Vec<u8>>)>,
        ts: i64,
    }

    fn now_ms() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after 1970")
            .as_millis() as i64
    }

    /// `per_partition` records on each partition, each with one header of its
    /// own so the strip in `archived_fingerprints` has something to keep.
    fn recs(partitions: i32, per_partition: usize, tag: &str, ts0: i64) -> Vec<Rec> {
        let mut out = Vec::new();
        for i in 0..per_partition {
            for p in 0..partitions {
                out.push(Rec {
                    partition: p,
                    key: Some(format!("{tag}-k{p}-{i}").into_bytes()),
                    value: Some(format!("{tag}-v{p}-{i}").into_bytes()),
                    headers: vec![("lw-fixture".to_string(), Some(tag.as_bytes().to_vec()))],
                    ts: ts0 + (i as i64) * 10 + i64::from(p),
                });
            }
        }
        out
    }

    fn send(producer: &BaseProducer, topic: &str, r: &Rec) {
        let mut headers = OwnedHeaders::new_with_capacity(r.headers.len());
        for (k, v) in &r.headers {
            headers = headers.insert(Header {
                key: k.as_str(),
                value: v.as_deref(),
            });
        }
        let mut record: BaseRecord<'_, [u8], [u8]> = BaseRecord::to(topic)
            .partition(r.partition)
            .timestamp(r.ts)
            .headers(headers);
        if let Some(k) = &r.key {
            record = record.key(&k[..]);
        }
        if let Some(v) = &r.value {
            record = record.payload(&v[..]);
        }
        producer
            .send(record)
            .unwrap_or_else(|(e, _)| panic!("enqueue into {topic}/{}: {e}", r.partition));
    }

    fn producer(extra: &[(&str, &str)]) -> BaseProducer {
        let mut cfg = ClientConfig::new();
        cfg.set("bootstrap.servers", broker_address(Side::Host))
            .set("acks", "all")
            .set("enable.idempotence", "true")
            .set("message.timeout.ms", "10000");
        for (k, v) in extra {
            cfg.set(*k, *v);
        }
        cfg.create().expect("a producer for the compose broker")
    }

    /// Produces `records` in order and returns once every partition's
    /// READ_UNCOMMITTED end offset has advanced by what was sent to it.
    fn produce(topic: &str, records: &[Rec]) {
        let before = marks(topic, Isolation::ReadUncommitted);
        let p = producer(&[]);
        for r in records {
            send(&p, topic, r);
        }
        p.flush(Duration::from_secs(15)).expect("flush");
        let mut want = BTreeMap::new();
        for r in records {
            *want.entry(r.partition).or_insert(0i64) += 1;
        }
        wait_for(20, &format!("{topic}'s end offsets to advance"), || {
            let now = marks(topic, Isolation::ReadUncommitted);
            want.iter().all(|(p, n)| {
                now.get(p).map(|m| m.high_watermark).unwrap_or(0)
                    >= before.get(p).map(|m| m.high_watermark).unwrap_or(0) + n
            })
        });
    }

    fn create_topic_exact(topic: &str, partitions: i32, configs: &[(&str, &str)]) {
        let spec = NewTopicSpec {
            name: topic.to_string(),
            num_partitions: partitions,
            replication_factor: 1,
            configs: configs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        };
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let res = TopicCreator::create_topics(&reader(), std::slice::from_ref(&spec))
                .expect("create_topics");
            match &res[0].1 {
                Ok(()) => break,
                // A name whose deletion the controller is still processing.
                Err(e) if Instant::now() < deadline => {
                    eprintln!("[topic-identity] create {topic}: {e}; retrying");
                    std::thread::sleep(Duration::from_millis(500));
                }
                Err(e) => panic!("create {topic}: {e}"),
            }
        }
        wait_for(
            60,
            &format!("{topic} to report {partitions} partitions"),
            || partition_count(topic) == Some(partitions),
        );
        // FX-18: listed with a leader is not yet SERVED. c02 read watermarks
        // here, after recreating its topic, and main CI run 37753000930 failed
        // with `NotLeaderForPartition`: the broker named as leader had not yet
        // become it.
        await_created_on(&broker_address(Side::Host), topic, partitions);
    }

    fn delete_topic(topic: &str) {
        assert!(
            topic.starts_with(PREFIX),
            "{topic} is not a topic this file owns"
        );
        let r = reader()
            .with_scratch_prefix(PREFIX)
            .expect("the prefix is long enough");
        let _ = TopicDeleter::delete_topics(&r, &[topic.to_string()]);
        wait_for(60, &format!("{topic} to disappear"), || {
            !ClusterReader::list_topics(&reader())
                .map(|ts| ts.iter().any(|t| t.name == topic))
                .unwrap_or(true)
        });
    }

    fn marks_json(m: &BTreeMap<i32, Marks>) -> Value {
        Value::Object(
            m.iter()
                .map(|(p, x)| {
                    (
                        p.to_string(),
                        json!({"log_start": x.log_start, "high_watermark": x.high_watermark}),
                    )
                })
                .collect(),
        )
    }

    fn run_json(r: &CaptureRun) -> Value {
        let parts: Vec<Value> = r
            .capture
            .partitions
            .iter()
            .map(|p| {
                json!({
                    "partition": p.partition,
                    "before": {"log_start": p.before.log_start, "high_watermark": p.before.high_watermark},
                    "after": {"log_start": p.after.log_start, "high_watermark": p.after.high_watermark},
                    "archived": p.archived.as_ref().map(|a| json!({
                        "first_offset": a.first_offset,
                        "last_offset": a.last_offset,
                        "first_timestamp_ms": a.first_timestamp_ms,
                        "last_timestamp_ms": a.last_timestamp_ms,
                        "records": a.records,
                        "tail": a.tail.iter().map(|t| json!({
                            "offset": t.offset,
                            "fingerprint": t.fingerprint,
                            "raw_fingerprint": t.raw_fingerprint,
                        })).collect::<Vec<_>>(),
                    })),
                })
            })
            .collect();
        json!({
            "cluster_id": r.capture.cluster_id,
            "topic_id_before": r.ids.0,
            "topic_id_after": r.ids.1,
            "partition_count": r.capture.partition_count,
            "engine_partition_count": r.archive.partition_count,
            "engine_gaps": r.archive.gaps,
            "partitions": parts,
            "read_committed_before": marks_json(&r.rc_before),
            "read_committed_after": marks_json(&r.rc_after),
            "intra_run": intra_run(&r.capture).iter().map(Signal::to_json).collect::<Vec<_>>(),
        })
    }

    /// The four readings of one row, side by side.
    struct Modes {
        /// The decision's rule: archive against archive.
        archive: Outcome,
        offsets_only: Outcome,
        /// A source read READ_UNCOMMITTED: the rejected variant.
        source: Outcome,
        /// The ID path, with the broker's IDs.
        ids: Outcome,
    }

    /// One live row: its topic, its scratch directory and its evidence.
    struct Case {
        name: &'static str,
        topic: String,
        dir: PathBuf,
        runs: u32,
        evidence: serde_json::Map<String, Value>,
    }

    impl Case {
        fn start(name: &'static str) -> Case {
            let nonce = format!("{:x}", now_ms());
            let topic = format!("{PREFIX}{name}-{nonce}");
            let dir = engine_mount().join("topic-identity").join(&topic);
            std::fs::create_dir_all(&dir).expect("case dir");
            eprintln!("[topic-identity] {name}: topic {topic}");
            let mut evidence = serde_json::Map::new();
            evidence.insert("case".into(), json!(name));
            evidence.insert("topic".into(), json!(topic));
            evidence.insert("kafka_version".into(), json!(kafka_version()));
            evidence.insert("engine_digest".into(), json!(engine_digest()));
            evidence.insert(
                "engine_route".into(),
                json!(engine_bin().display().to_string()),
            );
            Case {
                name,
                topic,
                dir,
                runs: 0,
                evidence,
            }
        }

        fn create(&self, partitions: i32, configs: &[(&str, &str)]) {
            create_topic_exact(&self.topic, partitions, configs);
        }

        fn recreate(&self, partitions: i32, configs: &[(&str, &str)]) {
            delete_topic(&self.topic);
            create_topic_exact(&self.topic, partitions, configs);
        }

        fn capture(&mut self) -> CaptureRun {
            self.runs += 1;
            let id = format!("{}-r{}", self.topic, self.runs);
            let run = capture(&self.topic, &self.dir, &id);
            self.evidence
                .insert(format!("run{}", self.runs), run_json(&run));
            run
        }

        fn note(&mut self, key: &str, v: Value) {
            self.evidence.insert(key.to_string(), v);
        }

        /// Classifies run `k` against run `j` four ways and records them.
        fn classify(&mut self, j: &CaptureRun, k: &CaptureRun) -> Modes {
            let archive_of_k = |p: i32, o: i64| {
                probe_in_archive(k.archive.partitions.get(&p).map(|a| &a.by_offset), o)
            };
            let mut archive_probes = Vec::new();
            let mut by_archive = |p: i32, o: i64| {
                let r = archive_of_k(p, o);
                archive_probes
                    .push(json!({"partition": p, "offset": o, "result": format!("{r:?}")}));
                r
            };
            let archive = classify(&j.capture, &Current::of(&k.capture), Some(&mut by_archive));
            let offsets_only = classify(&j.capture, &Current::of(&k.capture), None);
            let mut id_probes = Vec::new();
            let mut by_archive_ids = |p: i32, o: i64| {
                let r = archive_of_k(p, o);
                id_probes.push(json!({"partition": p, "offset": o, "result": format!("{r:?}")}));
                r
            };
            let ids = classify(
                &j.with_broker_ids(),
                &Current::of(&k.with_broker_ids()),
                Some(&mut by_archive_ids),
            );
            let (source, source_probes) = self.source_variant(j, Isolation::ReadUncommitted);
            self.evidence.insert(
                "modes".into(),
                json!({
                    "archive": outcome_json(&archive, archive_probes),
                    "offsets_only": outcome_json(&offsets_only, Vec::new()),
                    "source_read_uncommitted": outcome_json(&source, source_probes),
                    "topic_ids": outcome_json(&ids, id_probes),
                }),
            );
            Modes {
                archive,
                offsets_only,
                source,
                ids,
            }
        }

        /// The rejected variant: marks read now at `isolation`, and a source
        /// read at exactly each tail offset.
        fn source_variant(&self, j: &CaptureRun, isolation: Isolation) -> (Outcome, Vec<Value>) {
            let topic = self.topic.clone();
            let mut probes = Vec::new();
            let mut by_source = |p: i32, o: i64| {
                let r = source_probe(&topic, p, o);
                probes.push(json!({"partition": p, "offset": o, "result": format!("{r:?}")}));
                r
            };
            let o = classify(
                &j.capture,
                &bare_current(&self.topic, isolation),
                Some(&mut by_source),
            );
            (o, probes)
        }

        /// Writes the evidence line: the ground truth beside the rule's
        /// verdict.
        fn finish(&mut self, j: &CaptureRun, k: &CaptureRun, m: &Modes, expected: Verdict) {
            let same = j.ids.0 == k.ids.1;
            let v = m.archive.verdict;
            let class = match (same, v) {
                (false, Verdict::Break) | (false, Verdict::Suspected) => "true_positive",
                (false, _) => "false_negative",
                (true, Verdict::Break) | (true, Verdict::Suspected) => "false_positive",
                (true, _) => "true_negative",
            };
            self.evidence.insert(
                "ground_truth".into(),
                json!({
                    "run1": {"topic_id_before": j.ids.0, "topic_id_after": j.ids.1},
                    "run2": {"topic_id_before": k.ids.0, "topic_id_after": k.ids.1},
                    "same_generation": same,
                }),
            );
            self.evidence.insert("verdict".into(), json!(v.as_str()));
            self.evidence
                .insert("expected".into(), json!(expected.as_str()));
            self.evidence.insert("classification".into(), json!(class));
            if let Ok(path) = std::env::var("LOGWEIR_TOPIC_IDENTITY_EVIDENCE") {
                use std::io::Write;
                let mut f = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&path)
                    .unwrap_or_else(|e| panic!("{path}: {e}"));
                writeln!(f, "{}", Value::Object(self.evidence.clone())).expect("evidence line");
            }
            eprintln!(
                "[topic-identity] {}: rule {} (expected {}), offsets only {}, source read {}, \
                 IDs {}; same generation {same}: {class}",
                self.name,
                v.as_str(),
                expected.as_str(),
                m.offsets_only.verdict.as_str(),
                m.source.verdict.as_str(),
                m.ids.verdict.as_str(),
            );
        }
    }

    impl Drop for Case {
        /// Best effort, also on a failed assertion: the row's topic and
        /// archive. Never panics, so a failing row cannot turn into a double
        /// panic.
        fn drop(&mut self) {
            let r = RdKafkaReader::connect(&[broker_address(Side::Host)], AuthConfig::Plaintext)
                .and_then(|r| r.with_scratch_prefix(PREFIX));
            if let Ok(r) = r {
                let _ = TopicDeleter::delete_topics(&r, std::slice::from_ref(&self.topic));
            }
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    /// Topic settings every row starts from: no retention, record timestamps
    /// kept.
    const PLAIN: &[(&str, &str)] = &[
        ("retention.ms", "-1"),
        ("message.timestamp.type", "CreateTime"),
    ];

    /// The assertions every live row shares: the rule's verdict, the source
    /// read agreeing with it (except where a row pins their difference), the
    /// ID path, and the within-run checks of both captures.
    fn assert_modes(m: &Modes, expected: Verdict, ids: Verdict, j: &CaptureRun, k: &CaptureRun) {
        assert_eq!(m.archive.verdict, expected, "rule: {:?}", m.archive.signals);
        assert_eq!(m.ids.verdict, ids, "ID path: {:?}", m.ids.signals);
        assert!(
            intra_run(&j.capture).is_empty() && intra_run(&k.capture).is_empty(),
            "no fixture changes a topic during a capture"
        );
    }

    fn assert_source_agrees(m: &Modes) {
        assert_eq!(
            m.source.verdict, m.archive.verdict,
            "source read: {:?}",
            m.source.signals
        );
    }

    // =======================================================================
    // Live rows. Ground truth first, then the verdicts.
    // =======================================================================

    /// c01: recreated with the same partition count and fewer records. The
    /// end offsets regress: the offsets-only rule already sees it.
    #[test]
    fn c01_recreate_same_partition_count_shorter() {
        let mut c = Case::start("c01");
        c.create(3, PLAIN);
        produce(&c.topic, &recs(3, 10, "g1", now_ms() - 60_000));
        let run1 = c.capture();
        c.recreate(3, PLAIN);
        produce(&c.topic, &recs(3, 4, "g2", now_ms() - 30_000));
        let run2 = c.capture();
        assert_ne!(
            run1.ids.0, run2.ids.0,
            "the fixture must have made a new topic"
        );
        let m = c.classify(&run1, &run2);
        assert!(
            m.archive
                .signals
                .iter()
                .any(|x| matches!(x, Signal::EndRegressed { .. })),
            "{:?}",
            m.archive.signals
        );
        assert_eq!(m.offsets_only.verdict, Verdict::Break);
        assert_source_agrees(&m);
        c.finish(&run1, &run2, &m, Verdict::Break);
        assert_modes(&m, Verdict::Break, Verdict::Break, &run1, &run2);
    }

    /// c02: recreated with the same partition count and refilled past the old
    /// end. No offset regresses; only the boundary comparison sees the new
    /// topic.
    #[test]
    fn c02_recreate_same_partition_count_refilled_past_the_old_end() {
        let mut c = Case::start("c02");
        c.create(3, PLAIN);
        produce(&c.topic, &recs(3, 10, "g1", now_ms() - 60_000));
        let run1 = c.capture();
        c.recreate(3, PLAIN);
        produce(&c.topic, &recs(3, 15, "g2", now_ms() - 30_000));
        let run2 = c.capture();
        assert_ne!(
            run1.ids.0, run2.ids.0,
            "the fixture must have made a new topic"
        );
        let m = c.classify(&run1, &run2);
        assert_ne!(
            m.offsets_only.verdict,
            Verdict::Break,
            "the offsets-only rule misses this row by construction"
        );
        assert!(
            m.archive
                .signals
                .iter()
                .any(|x| matches!(x, Signal::BoundaryRecordChanged { .. })),
            "{:?}",
            m.archive.signals
        );
        assert_source_agrees(&m);
        c.finish(&run1, &run2, &m, Verdict::Break);
        assert_modes(&m, Verdict::Break, Verdict::Break, &run1, &run2);
    }

    /// c03: recreated with fewer partitions. Kafka never removes a partition
    /// from a live topic, so the count alone is proof.
    #[test]
    fn c03_recreate_with_fewer_partitions() {
        let mut c = Case::start("c03");
        c.create(3, PLAIN);
        produce(&c.topic, &recs(3, 10, "g1", now_ms() - 60_000));
        let run1 = c.capture();
        c.recreate(1, PLAIN);
        produce(&c.topic, &recs(1, 30, "g2", now_ms() - 30_000));
        let run2 = c.capture();
        assert_ne!(
            run1.ids.0, run2.ids.0,
            "the fixture must have made a new topic"
        );
        let m = c.classify(&run1, &run2);
        assert!(
            m.archive
                .signals
                .iter()
                .any(|x| matches!(x, Signal::PartitionCountDecreased { .. })),
            "{:?}",
            m.archive.signals
        );
        assert_source_agrees(&m);
        c.finish(&run1, &run2, &m, Verdict::Break);
        assert_modes(&m, Verdict::Break, Verdict::Break, &run1, &run2);
    }

    /// c04: recreated with more partitions and refilled. The comparison on
    /// the old partitions finds different records.
    #[test]
    fn c04_recreate_with_more_partitions() {
        let mut c = Case::start("c04");
        c.create(3, PLAIN);
        produce(&c.topic, &recs(3, 10, "g1", now_ms() - 60_000));
        let run1 = c.capture();
        c.recreate(5, PLAIN);
        produce(&c.topic, &recs(5, 15, "g2", now_ms() - 30_000));
        let run2 = c.capture();
        assert_ne!(
            run1.ids.0, run2.ids.0,
            "the fixture must have made a new topic"
        );
        let m = c.classify(&run1, &run2);
        assert!(
            m.archive
                .signals
                .iter()
                .any(|x| matches!(x, Signal::BoundaryRecordChanged { .. })),
            "{:?}",
            m.archive.signals
        );
        assert_source_agrees(&m);
        c.finish(&run1, &run2, &m, Verdict::Break);
        assert_modes(&m, Verdict::Break, Verdict::Break, &run1, &run2);
    }

    /// c05 (negative control): partitions ADDED to the same topic. Not a
    /// break: the old partitions verify by content.
    #[test]
    fn c05_create_partitions_keeps_the_generation() {
        let mut c = Case::start("c05");
        c.create(3, PLAIN);
        produce(&c.topic, &recs(3, 10, "g1", now_ms() - 60_000));
        let run1 = c.capture();
        alter_partitions(&c.topic, 5);
        produce(&c.topic, &recs(5, 5, "g1b", now_ms() - 30_000));
        let run2 = c.capture();
        assert_eq!(
            run1.ids.0, run2.ids.0,
            "CreatePartitions keeps the topic ID"
        );
        let m = c.classify(&run1, &run2);
        assert!(
            m.archive
                .signals
                .iter()
                .any(|x| matches!(x, Signal::PartitionCountIncreased { .. })),
            "{:?}",
            m.archive.signals
        );
        // The strip keeps a tail fingerprint a fact about the SOURCE record:
        // the archived bytes verbatim carry the engine's two headers, so they
        // never equal it (and a live read could never match them).
        for p in &run1.capture.partitions {
            let a = p.archived.as_ref().expect("archived");
            assert!(a.tail.iter().all(|t| t.raw_fingerprint != t.fingerprint));
        }
        assert_source_agrees(&m);
        c.finish(&run1, &run2, &m, Verdict::Continuous);
        assert_modes(&m, Verdict::Continuous, Verdict::Continuous, &run1, &run2);
    }

    /// c06 (negative control): DeleteRecords inside the archived range, and
    /// on one partition exactly past it. The log start advances; nothing
    /// breaks.
    #[test]
    fn c06_delete_records_inside_the_archived_range() {
        let mut c = Case::start("c06");
        c.create(3, PLAIN);
        produce(&c.topic, &recs(3, 10, "g1", now_ms() - 60_000));
        let run1 = c.capture();
        delete_records(&c.topic, &[(0, 5), (1, 9), (2, 10)]);
        let m0 = marks(&c.topic, Isolation::ReadUncommitted);
        assert_eq!(
            (m0[&0].log_start, m0[&1].log_start, m0[&2].log_start),
            (5, 9, 10),
            "the fixture must have advanced the log starts"
        );
        let run2 = c.capture();
        assert_eq!(run1.ids.0, run2.ids.0, "DeleteRecords keeps the topic ID");
        let m = c.classify(&run1, &run2);
        assert!(
            !m.archive
                .signals
                .iter()
                .any(|x| matches!(x, Signal::CaptureGap { .. })),
            "{:?}",
            m.archive.signals
        );
        assert_source_agrees(&m);
        c.finish(&run1, &run2, &m, Verdict::Continuous);
        assert_modes(&m, Verdict::Continuous, Verdict::Continuous, &run1, &run2);
    }

    /// c07 (negative control): records produced after the capture and
    /// deleted before the next one. The same topic, and a capture gap the
    /// next point must report, with or without IDs.
    #[test]
    fn c07_delete_records_past_the_archived_end_is_a_gap_not_a_break() {
        let mut c = Case::start("c07");
        c.create(3, PLAIN);
        produce(&c.topic, &recs(3, 10, "g1", now_ms() - 60_000));
        let run1 = c.capture();
        produce(&c.topic, &recs(3, 10, "g1b", now_ms() - 30_000));
        delete_records(&c.topic, &[(0, 15), (1, 15), (2, 15)]);
        let run2 = c.capture();
        assert_eq!(run1.ids.0, run2.ids.0, "DeleteRecords keeps the topic ID");
        let m = c.classify(&run1, &run2);
        for p in 0..3 {
            let gap = Signal::CaptureGap {
                partition: p,
                from: 10,
                to: 15,
            };
            assert!(
                m.archive.signals.contains(&gap),
                "partition {p}: {:?}",
                m.archive.signals
            );
            assert!(
                m.ids.signals.contains(&gap),
                "ID path, partition {p}: {:?}",
                m.ids.signals
            );
        }
        assert_source_agrees(&m);
        c.finish(&run1, &run2, &m, Verdict::Unverified);
        assert_modes(&m, Verdict::Unverified, Verdict::Continuous, &run1, &run2);
    }

    /// c08 (negative control): every record deleted (log start = end).
    #[test]
    fn c08_delete_all_records() {
        let mut c = Case::start("c08");
        c.create(3, PLAIN);
        produce(&c.topic, &recs(3, 10, "g1", now_ms() - 60_000));
        let run1 = c.capture();
        delete_records(&c.topic, &[(0, 10), (1, 10), (2, 10)]);
        let run2 = c.capture();
        assert_eq!(run1.ids.0, run2.ids.0, "DeleteRecords keeps the topic ID");
        let m = c.classify(&run1, &run2);
        assert_source_agrees(&m);
        c.finish(&run1, &run2, &m, Verdict::Unverified);
        assert_modes(&m, Verdict::Unverified, Verdict::Continuous, &run1, &run2);
    }

    /// c09 (negative control): compaction removes the archived tail. The log
    /// start does not move and the end only grows; the tail records are
    /// absent, and the comparison must not read the next surviving record as
    /// a mismatch.
    #[test]
    fn c09_compaction_removes_the_archived_tail() {
        let mut c = Case::start("c09");
        let compact: &[(&str, &str)] = &[
            ("cleanup.policy", "compact"),
            ("min.cleanable.dirty.ratio", "0.01"),
            ("min.compaction.lag.ms", "0"),
            ("segment.ms", "100"),
            ("message.timestamp.type", "CreateTime"),
        ];
        c.create(1, compact);
        let ts0 = now_ms() - 60_000;
        // Five keys, four rounds: offsets 0..19; the tail is round 3 of k2..k4.
        let round = |r: usize, tag: &str, ts: i64| -> Vec<Rec> {
            (0..5)
                .map(|k| Rec {
                    partition: 0,
                    key: Some(format!("k{k}").into_bytes()),
                    value: Some(format!("{tag}-r{r}-k{k}").into_bytes()),
                    headers: vec![("lw-fixture".into(), Some(tag.as_bytes().to_vec()))],
                    ts: ts + (r as i64) * 10 + k,
                })
                .collect()
        };
        for r in 0..4 {
            produce(&c.topic, &round(r, "g1", ts0));
        }
        let run1 = c.capture();
        let tail: Vec<i64> = run1.capture.partitions[0]
            .archived
            .as_ref()
            .expect("archived")
            .tail
            .iter()
            .map(|t| t.offset)
            .collect();
        assert_eq!(tail, vec![19, 18, 17]);
        // Overwrite every key, then roll the segment holding the overwrites.
        produce(&c.topic, &round(4, "g1b", ts0 + 1_000));
        for i in 0..2 {
            std::thread::sleep(Duration::from_millis(300));
            produce(
                &c.topic,
                &[Rec {
                    partition: 0,
                    key: Some(format!("roll-{i}").into_bytes()),
                    value: Some(b"roll".to_vec()),
                    headers: vec![],
                    ts: ts0 + 2_000 + i,
                }],
            );
        }
        let topic = c.topic.clone();
        wait_for(180, "the log cleaner to remove offsets 17..19", || {
            [17, 18, 19]
                .iter()
                .all(|o| matches!(source_probe(&topic, 0, *o), Probe::Absent(Some(_))))
        });
        // What an inexact read would have compared: the first record at or
        // after the old tail is a different record, so "first record from L"
        // reads compaction as a new topic.
        let next = source_probe(&c.topic, 0, 19);
        c.note(
            "inexact_probe_first_record_after_19",
            json!(format!("{next:?}")),
        );
        let run2 = c.capture();
        assert_eq!(run1.ids.0, run2.ids.0, "compaction keeps the topic ID");
        let m = c.classify(&run1, &run2);
        assert_eq!(
            m.archive
                .signals
                .iter()
                .filter(|x| matches!(x, Signal::BoundaryRecordAbsent { .. }))
                .count(),
            3,
            "{:?}",
            m.archive.signals
        );
        assert_source_agrees(&m);
        c.finish(&run1, &run2, &m, Verdict::Unverified);
        assert_modes(&m, Verdict::Unverified, Verdict::Continuous, &run1, &run2);
    }

    /// c10 (negative control): retention expiry. Waits for the broker's
    /// retention check, which runs every five minutes by default.
    #[test]
    #[ignore = "waits up to seven minutes for the broker's five-minute retention check; run with --ignored"]
    fn c10_retention_expiry_advances_the_log_start() {
        let mut c = Case::start("c10");
        c.create(1, PLAIN);
        produce(&c.topic, &recs(1, 10, "g1", now_ms() - 60_000));
        let run1 = c.capture();
        produce(&c.topic, &recs(1, 5, "g1b", now_ms() - 30_000));
        alter_config(&c.topic, "retention.ms=1000");
        let started = Instant::now();
        let topic = c.topic.clone();
        wait_for(420, "retention to delete every segment", || {
            marks(&topic, Isolation::ReadUncommitted)[&0].log_start >= 15
        });
        c.note("retention_wait_seconds", json!(started.elapsed().as_secs()));
        let run2 = c.capture();
        assert_eq!(run1.ids.0, run2.ids.0, "retention keeps the topic ID");
        let m = c.classify(&run1, &run2);
        assert!(
            m.archive.signals.contains(&Signal::CaptureGap {
                partition: 0,
                from: 10,
                to: 15
            }),
            "{:?}",
            m.archive.signals
        );
        assert_source_agrees(&m);
        c.finish(&run1, &run2, &m, Verdict::Unverified);
        assert_modes(&m, Verdict::Unverified, Verdict::Continuous, &run1, &run2);
    }

    /// c11 (negative control): an open transaction during and after both
    /// captures. READ_UNCOMMITTED marks agree with what the engine archived;
    /// librdkafka's default READ_COMMITTED marks stop at the last stable
    /// offset and read as a regression, which is why the rule requires the
    /// former.
    #[test]
    fn c11_an_open_transaction_needs_read_uncommitted_marks() {
        let mut c = Case::start("c11");
        c.create(1, PLAIN);
        let ts0 = now_ms() - 60_000;
        produce(&c.topic, &recs(1, 5, "g1", ts0));
        let txid = format!("{}-txn", c.topic);
        let txp = producer(&[("transactional.id", txid.as_str())]);
        txp.init_transactions(Duration::from_secs(30))
            .expect("init_transactions");
        txp.begin_transaction().expect("begin_transaction");
        for r in recs(1, 3, "txn", ts0 + 1_000) {
            send(&txp, &c.topic, &r);
        }
        txp.flush(Duration::from_secs(15)).expect("flush");
        let topic = c.topic.clone();
        wait_for(20, "the open transaction's records to be appended", || {
            marks(&topic, Isolation::ReadUncommitted)[&0].high_watermark == 8
        });
        let run1 = c.capture();
        let a = run1.capture.partitions[0]
            .archived
            .clone()
            .expect("archived");
        assert_eq!(
            a.last_offset, 7,
            "the engine archives the open transaction's records"
        );
        assert_eq!(
            run1.rc_after[&0].high_watermark, 5,
            "READ_COMMITTED stops at the LSO"
        );
        // Within the run: READ_COMMITTED marks make the capture contradict
        // itself.
        let mut rc_capture = run1.capture.clone();
        rc_capture.partitions[0].before = run1.rc_before[&0];
        rc_capture.partitions[0].after = run1.rc_after[&0];
        let rc_intra = intra_run(&rc_capture);
        assert!(
            !rc_intra.is_empty(),
            "READ_COMMITTED marks flag the capture"
        );
        c.note(
            "intra_run_read_committed",
            json!(rc_intra.iter().map(Signal::to_json).collect::<Vec<_>>()),
        );
        let run2 = c.capture();
        assert_eq!(run1.ids.0, run2.ids.0);
        let m = c.classify(&run1, &run2);
        let (rc, rc_probes) = c.source_variant(&run1, Isolation::ReadCommitted);
        c.note("source_read_committed", outcome_json(&rc, rc_probes));
        txp.abort_transaction(Duration::from_secs(30))
            .expect("abort_transaction");
        assert_eq!(
            rc.verdict,
            Verdict::Break,
            "READ_COMMITTED marks: a false break: {:?}",
            rc.signals
        );
        assert_source_agrees(&m);
        c.finish(&run1, &run2, &m, Verdict::Continuous);
        assert_modes(&m, Verdict::Continuous, Verdict::Continuous, &run1, &run2);
    }

    /// c12 (negative control): records produced after the capture carry
    /// OLDER timestamps than the archived tail. Timestamps are not a signal.
    #[test]
    fn c12_non_monotonic_timestamps_are_not_a_signal() {
        let mut c = Case::start("c12");
        c.create(1, PLAIN);
        let ts0 = now_ms() - 60_000;
        produce(&c.topic, &recs(1, 10, "g1", ts0));
        let run1 = c.capture();
        produce(&c.topic, &recs(1, 5, "g1b", ts0 - 3_600_000));
        let run2 = c.capture();
        assert_eq!(run1.ids.0, run2.ids.0);
        let last_ts = run1.capture.partitions[0]
            .archived
            .as_ref()
            .expect("archived")
            .last_timestamp_ms;
        assert!(
            ts0 - 3_600_000 < last_ts,
            "the new records are older than the tail"
        );
        let m = c.classify(&run1, &run2);
        assert_source_agrees(&m);
        c.finish(&run1, &run2, &m, Verdict::Continuous);
        assert_modes(&m, Verdict::Continuous, Verdict::Continuous, &run1, &run2);
    }

    /// c13 (KNOWN FALSE NEGATIVE): recreated, refilled past the old end, and
    /// the old boundary offsets deleted before the next capture. Nothing is
    /// left to compare and no offset regresses. The ID path closes it.
    #[test]
    fn c13_known_miss_recreated_then_trimmed_past_the_boundary() {
        let mut c = Case::start("c13");
        c.create(1, PLAIN);
        produce(&c.topic, &recs(1, 10, "g1", now_ms() - 60_000));
        let run1 = c.capture();
        c.recreate(1, PLAIN);
        produce(&c.topic, &recs(1, 20, "g2", now_ms() - 30_000));
        delete_records(&c.topic, &[(0, 12)]);
        let run2 = c.capture();
        assert_ne!(
            run1.ids.0, run2.ids.0,
            "the fixture must have made a new topic"
        );
        let m = c.classify(&run1, &run2);
        assert_source_agrees(&m);
        c.finish(&run1, &run2, &m, Verdict::Unverified);
        assert_modes(&m, Verdict::Unverified, Verdict::Break, &run1, &run2);
    }

    /// c14 (KNOWN FALSE NEGATIVE): recreated and refilled with byte-identical
    /// records (same keys, values, headers and timestamps, same order). The
    /// boundary records match. A restore that strips Logweir's offset headers
    /// produces exactly this. The ID path closes it.
    #[test]
    fn c14_known_miss_byte_identical_replay() {
        let mut c = Case::start("c14");
        c.create(1, PLAIN);
        let gen1 = recs(1, 10, "g1", now_ms() - 60_000);
        produce(&c.topic, &gen1);
        let run1 = c.capture();
        c.recreate(1, PLAIN);
        produce(&c.topic, &gen1);
        produce(&c.topic, &recs(1, 5, "g2", now_ms() - 30_000));
        let run2 = c.capture();
        assert_ne!(
            run1.ids.0, run2.ids.0,
            "the fixture must have made a new topic"
        );
        let m = c.classify(&run1, &run2);
        assert_source_agrees(&m);
        c.finish(&run1, &run2, &m, Verdict::Continuous);
        assert_modes(&m, Verdict::Continuous, Verdict::Break, &run1, &run2);
    }

    /// c15: the original-name restore PROD-15.1 will perform, emulated: the
    /// archived records produced back verbatim, WITH the offset headers the
    /// engine added (`strip_offset_headers: false`, which Logweir renders).
    /// The comparison sees the extra headers, so the next capture reports a
    /// break.
    #[test]
    fn c15_an_original_name_restore_is_a_new_generation() {
        let mut c = Case::start("c15");
        c.create(1, PLAIN);
        produce(&c.topic, &recs(1, 10, "g1", now_ms() - 60_000));
        let run1 = c.capture();
        let archived: Vec<Rec> = run1.archive.partitions[&0]
            .records
            .iter()
            .map(|r| Rec {
                partition: 0,
                key: r.key.clone(),
                value: r.value.clone(),
                headers: r.headers.clone(),
                ts: r.timestamp,
            })
            .collect();
        c.recreate(1, PLAIN);
        produce(&c.topic, &archived);
        let run2 = c.capture();
        assert_ne!(
            run1.ids.0, run2.ids.0,
            "the fixture must have made a new topic"
        );
        let m = c.classify(&run1, &run2);
        assert!(
            m.archive
                .signals
                .iter()
                .any(|x| matches!(x, Signal::BoundaryRecordChanged { .. })),
            "{:?}",
            m.archive.signals
        );
        assert_source_agrees(&m);
        c.finish(&run1, &run2, &m, Verdict::Break);
        assert_modes(&m, Verdict::Break, Verdict::Break, &run1, &run2);
    }

    /// c16: recreated with more partitions, refilled, and the old boundary
    /// offsets deleted. Nothing verifies and partitions were added:
    /// suspected, not continuous.
    #[test]
    fn c16_more_partitions_with_nothing_to_verify_is_suspected() {
        let mut c = Case::start("c16");
        c.create(3, PLAIN);
        produce(&c.topic, &recs(3, 10, "g1", now_ms() - 60_000));
        let run1 = c.capture();
        c.recreate(5, PLAIN);
        produce(&c.topic, &recs(5, 20, "g2", now_ms() - 30_000));
        delete_records(&c.topic, &[(0, 12), (1, 12), (2, 12)]);
        let run2 = c.capture();
        assert_ne!(
            run1.ids.0, run2.ids.0,
            "the fixture must have made a new topic"
        );
        let m = c.classify(&run1, &run2);
        assert_source_agrees(&m);
        c.finish(&run1, &run2, &m, Verdict::Suspected);
        assert_modes(&m, Verdict::Suspected, Verdict::Break, &run1, &run2);
    }

    /// c17 (review H1): an unchanged `LogAppendTime` topic. The engine
    /// archives each record's timestamp as the batch's first timestamp plus
    /// its delta (the producer's CreateTime); librdkafka reports the batch's
    /// MaxTimestamp (the append time). Archive against archive is
    /// `continuous`; a SOURCE read is a false `break`, pinned here.
    #[test]
    fn c17_log_append_time_breaks_a_source_read_but_not_the_rule() {
        let mut c = Case::start("c17");
        c.create(
            1,
            &[
                ("retention.ms", "-1"),
                ("message.timestamp.type", "LogAppendTime"),
            ],
        );
        produce(&c.topic, &recs(1, 10, "g1", now_ms() - 3_600_000));
        let run1 = c.capture();
        let run2 = c.capture();
        assert_eq!(run1.ids.0, run2.ids.0, "nothing touched the topic");
        let at9 = source_record_summary(&c.topic, 0, 9);
        assert_eq!(
            at9["timestamp_type"], "LogAppendTime",
            "the fixture must be a LogAppendTime topic: {at9}"
        );
        let archived_ts = run1.archive.partitions[&0].records[9].timestamp;
        assert_ne!(
            at9["timestamp_ms"].as_i64(),
            Some(archived_ts),
            "the archive keeps the producer's CreateTime, the source reports the append time"
        );
        c.note("source_at_9", at9);
        c.note("archived_timestamp_at_9", json!(archived_ts));
        let m = c.classify(&run1, &run2);
        assert_eq!(
            m.source.verdict,
            Verdict::Break,
            "the rejected source read reports a false break here: {:?}",
            m.source.signals
        );
        c.finish(&run1, &run2, &m, Verdict::Continuous);
        assert_modes(&m, Verdict::Continuous, Verdict::Continuous, &run1, &run2);
    }

    /// c18 (review H1): an unchanged topic whose tail records repeat a header
    /// key. The engine's decoder keeps one entry per key (first position,
    /// last value); librdkafka returns every header. Archive against archive
    /// is `continuous`; a SOURCE read is a false `break`, pinned here.
    #[test]
    fn c18_a_repeated_header_key_breaks_a_source_read_but_not_the_rule() {
        let mut c = Case::start("c18");
        c.create(1, PLAIN);
        let mut records = recs(1, 10, "g1", now_ms() - 60_000);
        for r in records.iter_mut().skip(7) {
            r.headers = vec![
                ("h".into(), Some(b"a".to_vec())),
                ("x".into(), Some(b"1".to_vec())),
                ("h".into(), Some(b"b".to_vec())),
            ];
        }
        produce(&c.topic, &records);
        let run1 = c.capture();
        let run2 = c.capture();
        assert_eq!(run1.ids.0, run2.ids.0, "nothing touched the topic");
        let at9 = source_record_summary(&c.topic, 0, 9);
        assert_eq!(
            at9["header_keys"],
            json!(["h", "x", "h"]),
            "the source keeps every header: {at9}"
        );
        let own: Vec<&str> = {
            let r = &run1.archive.partitions[&0].records[9];
            r.headers[..r.headers.len() - 2]
                .iter()
                .map(|(k, _)| k.as_str())
                .collect()
        };
        assert_eq!(own, vec!["h", "x"], "the archive keeps one entry per key");
        c.note("source_at_9", at9);
        c.note("archived_own_header_keys_at_9", json!(own));
        let m = c.classify(&run1, &run2);
        assert_eq!(
            m.source.verdict,
            Verdict::Break,
            "the rejected source read reports a false break here: {:?}",
            m.source.signals
        );
        c.finish(&run1, &run2, &m, Verdict::Continuous);
        assert_modes(&m, Verdict::Continuous, Verdict::Continuous, &run1, &run2);
    }

    /// c19 (KNOWN FALSE POSITIVE, review M5): partitions ADDED to the same
    /// topic, then every old tail deleted before the next capture. The rule
    /// cannot tell this from c16's recreation and says `suspected`, which
    /// every consumer treats as a break. The ID path closes it.
    #[test]
    fn c19_known_false_positive_added_partitions_with_nothing_to_verify() {
        let mut c = Case::start("c19");
        c.create(3, PLAIN);
        produce(&c.topic, &recs(3, 10, "g1", now_ms() - 60_000));
        let run1 = c.capture();
        alter_partitions(&c.topic, 5);
        produce(&c.topic, &recs(5, 20, "g1b", now_ms() - 30_000));
        delete_records(&c.topic, &[(0, 12), (1, 12), (2, 12)]);
        let run2 = c.capture();
        assert_eq!(
            run1.ids.0, run2.ids.0,
            "CreatePartitions keeps the topic ID"
        );
        let m = c.classify(&run1, &run2);
        assert_source_agrees(&m);
        c.finish(&run1, &run2, &m, Verdict::Suspected);
        assert_modes(&m, Verdict::Suspected, Verdict::Continuous, &run1, &run2);
    }

    /// Review L9: a deadline bounds the engine ITSELF, not only the client.
    /// The engine dials a fake broker that accepts and never answers; at the
    /// deadline the client is killed and so is the engine's container (on the
    /// docker route) or process (native).
    #[test]
    fn z_an_engine_deadline_kills_the_engine_itself() {
        use std::net::TcpListener;
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        use std::sync::Arc;
        let listener = TcpListener::bind("127.0.0.1:0").expect("loopback listener");
        let port = listener.local_addr().expect("local addr").port();
        listener.set_nonblocking(true).expect("nonblocking");
        let accepted = Arc::new(AtomicUsize::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let (seen, stopping) = (accepted.clone(), stop.clone());
        // Accepts every connection and never writes a byte, for at most 90 s.
        let holder = std::thread::spawn(move || {
            let mut held = Vec::new();
            let deadline = Instant::now() + Duration::from_secs(90);
            while Instant::now() < deadline && !stopping.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((s, _)) => {
                        seen.fetch_add(1, Ordering::SeqCst);
                        held.push(s);
                    }
                    Err(_) => std::thread::sleep(Duration::from_millis(100)),
                }
            }
            drop(held);
        });
        let dir = engine_mount()
            .join("topic-identity")
            .join(format!("{PREFIX}hang-{:x}", now_ms()));
        std::fs::create_dir_all(dir.join("archive")).expect("dir");
        let cfg = dir.join("hang.yaml");
        std::fs::write(
            &cfg,
            engine_config(
                "ti-hang",
                &broker_address(Side::Loopback(port)),
                "ti-hang-topic",
                &dir.join("archive"),
            ),
        )
        .expect("config");
        let r = try_run_bounded(engine_command(&cfg), 15, &|| kill_engine_containers(&cfg));
        let dialled = accepted.load(Ordering::SeqCst);
        let left = {
            let mut left = Vec::new();
            for _ in 0..30 {
                left = engine_containers_for(&cfg);
                if left.is_empty() {
                    break;
                }
                std::thread::sleep(Duration::from_secs(1));
            }
            left
        };
        stop.store(true, Ordering::SeqCst);
        let _ = holder.join();
        let _ = std::fs::remove_dir_all(&dir);
        assert!(
            r.is_err(),
            "premise: the engine must still be waiting at the deadline, got {:?}",
            r.map(|o| (o.status.code(), o.stderr_utf8()))
        );
        assert!(dialled > 0, "premise: the engine dialled the fake broker");
        assert!(
            left.is_empty(),
            "the engine's container outlived its deadline: {left:?}"
        );
    }
}
