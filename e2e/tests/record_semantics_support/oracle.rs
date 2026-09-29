//! PROD-01.1's record oracle: a pure comparison between the records a model
//! says a restore should produce and the records a reader actually observed.
//!
//! No I/O lives here. The live rows in `e2e/tests/record_semantics.rs` read a
//! source topic, a `.kbak` archive and a restored topic, turn all three into
//! [`Rec`]s and hand them to [`compare`]; the always-on negative controls in
//! `e2e/tests/record_semantics_oracle.rs` hand it synthetic divergences and
//! require each to be reported. So a divergence this module can name is a
//! divergence the live rows can see, and nothing the live rows assert rests
//! on a comparison that was never shown to fail.
//!
//! # Record identity is the source offset, never the payload
//!
//! A restore gives every record a new offset. The only link back to the
//! source is the `x-original-offset` header the pinned engine stamps on every
//! archived record (`include_offset_headers: true`, rendered by
//! `crates/logweir-engine-oso/src/render_backup.rs`), so an output record is
//! matched to its model record by `(partition, x-original-offset)`. An archive
//! record carries its source offset in the segment itself, which is what
//! [`LineageFrom::RecordOffset`] reads.
//!
//! # The expected transformation is stated, not tolerated
//!
//! A restored record is not byte-identical to its source: the backup appends
//! `x-original-offset` (8 bytes, little-endian `i64`) and
//! `x-original-timestamp` (the archived timestamp, same encoding) after the
//! record's own headers, and Logweir renders `strip_offset_headers: false`
//! (`crates/logweir-engine-oso/src/render_restore.rs`), so they survive into
//! the target. [`HeaderModel::AppendLineage`] is that transformation and
//! nothing else: every other header difference is a divergence.
#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};

/// The header the backup stamps with the record's source offset.
pub const X_ORIGINAL_OFFSET: &str = "x-original-offset";
/// The header the backup stamps with the record's ARCHIVED timestamp.
pub const X_ORIGINAL_TIMESTAMP: &str = "x-original-timestamp";

/// Ordered headers. A `Vec` and not a map on purpose: Kafka allows the same
/// key twice, and whether a copy survives is one of the things measured.
pub type Headers = Vec<(String, Option<Vec<u8>>)>;

/// Which clock a timestamp came from, as the reader reported it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TsType {
    CreateTime,
    LogAppendTime,
    NotAvailable,
    /// The `.kbak` format stores a timestamp and no timestamp type, so an
    /// archive record's type is unknowable. Never compared.
    Unrecorded,
}

impl TsType {
    pub fn as_str(self) -> &'static str {
        match self {
            TsType::CreateTime => "CreateTime",
            TsType::LogAppendTime => "LogAppendTime",
            TsType::NotAvailable => "NotAvailable",
            TsType::Unrecorded => "unrecorded",
        }
    }
}

/// One record as a consumer saw it, or as the archive holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rec {
    pub partition: i32,
    /// The offset in the log it was READ FROM: a source offset for a source
    /// or archive record, a target offset for a restored one.
    pub offset: i64,
    pub timestamp: i64,
    pub ts_type: TsType,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Headers,
}

impl Rec {
    /// The LAST `x-original-offset` header whose value is an 8-byte
    /// little-endian `i64`. The last, because a source record that already
    /// carried the header (a topic that was itself restored) is archived with
    /// two, and the engine's own is appended after the record's.
    pub fn lineage_header(&self) -> Option<i64> {
        self.headers
            .iter()
            .rev()
            .filter(|(k, _)| k == X_ORIGINAL_OFFSET)
            .find_map(|(_, v)| decode_le_i64(v.as_deref()))
    }
}

pub fn decode_le_i64(v: Option<&[u8]>) -> Option<i64> {
    let b: [u8; 8] = v?.try_into().ok()?;
    Some(i64::from_le_bytes(b))
}

pub fn le_i64(v: i64) -> Option<Vec<u8>> {
    Some(v.to_le_bytes().to_vec())
}

/// `indexmap::IndexMap::insert` semantics over an ordered header list: a
/// repeated key keeps the position of its FIRST occurrence and takes the
/// value of its LAST. The pinned engine decodes (`kafka-protocol` 0.18.0
/// `records.rs:896-919`) and encodes (`kafka/produce.rs:84-90`) headers
/// through an `IndexMap`, so this is the exact shape of the loss it predicts.
pub fn indexmap_collapse(h: &Headers) -> Headers {
    let mut out: Headers = Vec::with_capacity(h.len());
    for (k, v) in h {
        if let Some(slot) = out.iter_mut().find(|(ek, _)| ek == k) {
            slot.1 = v.clone();
        } else {
            out.push((k.clone(), v.clone()));
        }
    }
    out
}

/// A Kafka transaction control record, recognised by shape: key
/// `ControlRecordType` v0 (`00 00` version, `00 00` abort or `00 01`
/// commit), value `EndTransactionMarker` v0 (`00 00` version and a 4-byte
/// coordinator epoch), and no header but the two lineage headers the backup
/// appends. A consumer never returns one; a restore that writes one as data is
/// what makes it visible.
///
/// **A shape, not a proof.** A user record can have it (an integer key 0 or 1
/// with a 6-byte value starting `00 00`), and a marker of another encoding
/// version does not. Inside `compare` it only classifies an extra that is in
/// NEITHER source view, which is where a control record lives; a product scan
/// must confirm it against the capture-time offset gap (decision record §6.2).
pub fn is_control_shaped(r: &Rec) -> bool {
    let key_ok = matches!(r.key.as_deref(), Some([0, 0, 0, 0]) | Some([0, 0, 0, 1]));
    let value_ok = matches!(r.value.as_deref(), Some(v) if v.len() == 6 && v[0] == 0 && v[1] == 0);
    let headers_ok = r
        .headers
        .iter()
        .all(|(k, _)| k == X_ORIGINAL_OFFSET || k == X_ORIGINAL_TIMESTAMP);
    key_ok && value_ok && headers_ok
}

/// `Some(true)` for a commit marker, `Some(false)` for an abort marker.
pub fn control_is_commit(r: &Rec) -> Option<bool> {
    if !is_control_shaped(r) {
        return None;
    }
    Some(r.key.as_deref() == Some(&[0, 0, 0, 1][..]))
}

/// How the observed headers are expected to relate to the model's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeaderModel {
    /// Source -> archive and source -> target: the model's headers, then
    /// `x-original-offset` (the model record's offset) and
    /// `x-original-timestamp` (the OBSERVED record's timestamp, which is what
    /// the engine stamps; a timestamp difference is reported once, as
    /// [`Divergence::TimestampChanged`], not twice).
    AppendLineage,
    /// Archive -> target: the archive already carries the lineage headers.
    Identity,
}

/// Where an observed record's source offset comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LineageFrom {
    /// A restored record: its `x-original-offset` header.
    Header,
    /// An archive record: the source offset the segment stores.
    RecordOffset,
}

/// Why an observed record has no model counterpart.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ExtraKind {
    /// Its source offset holds a COMMITTED source record the model excluded:
    /// for a point-in-time model, a record outside the requested window.
    OutsideModel,
    /// Its source offset holds a record the raw (`read_uncommitted`) source
    /// view returns and the committed view does not: aborted, or open when
    /// captured.
    Uncommitted,
    /// Its source offset is in neither source view and it has the shape of a
    /// transaction control record.
    ControlMarker,
    /// No readable `x-original-offset`.
    NoLineage,
    /// None of the above.
    Unexplained,
}

/// One difference between the model and the observation. Offsets named
/// `source_offset` are source offsets; `target_offset` is an offset in the
/// observed log.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Divergence {
    Missing {
        partition: i32,
        source_offset: i64,
    },
    Extra {
        partition: i32,
        target_offset: i64,
        lineage: Option<i64>,
        kind: ExtraKind,
    },
    Duplicate {
        partition: i32,
        source_offset: i64,
        copies: usize,
    },
    OutOfOrder {
        partition: i32,
        source_offset: i64,
        after: i64,
    },
    KeyChanged {
        partition: i32,
        source_offset: i64,
    },
    ValueChanged {
        partition: i32,
        source_offset: i64,
    },
    TimestampChanged {
        partition: i32,
        source_offset: i64,
        expected: i64,
        observed: i64,
    },
    TimestampTypeChanged {
        partition: i32,
        source_offset: i64,
        expected: TsType,
        observed: TsType,
    },
    /// The observed headers are exactly [`indexmap_collapse`] of the expected
    /// ones: a repeated key lost its earlier copies.
    HeadersCollapsed {
        partition: i32,
        source_offset: i64,
    },
    HeadersChanged {
        partition: i32,
        source_offset: i64,
        expected: Headers,
        observed: Headers,
    },
}

impl Divergence {
    /// The class name the outcome tables use.
    pub fn class(&self) -> &'static str {
        match self {
            Divergence::Missing { .. } => "missing",
            Divergence::Extra { kind, .. } => match kind {
                ExtraKind::OutsideModel => "extra:outside-model",
                ExtraKind::Uncommitted => "extra:uncommitted",
                ExtraKind::ControlMarker => "extra:control-marker",
                ExtraKind::NoLineage => "extra:no-lineage",
                ExtraKind::Unexplained => "extra:unexplained",
            },
            Divergence::Duplicate { .. } => "duplicate",
            Divergence::OutOfOrder { .. } => "out-of-order",
            Divergence::KeyChanged { .. } => "key-changed",
            Divergence::ValueChanged { .. } => "value-changed",
            Divergence::TimestampChanged { .. } => "timestamp-changed",
            Divergence::TimestampTypeChanged { .. } => "timestamp-type-changed",
            Divergence::HeadersCollapsed { .. } => "headers-collapsed",
            Divergence::HeadersChanged { .. } => "headers-changed",
        }
    }
}

/// One comparison's inputs.
pub struct Comparison<'a> {
    /// The model: what the output SHOULD hold, offsets are source offsets.
    pub expected: &'a [Rec],
    /// Every committed source record, before any window was applied; used
    /// only to classify extras. The same slice as `expected` when the model
    /// is unfiltered.
    pub committed: &'a [Rec],
    /// The raw source (`read_uncommitted`), used only to classify extras. May
    /// be the same slice as `committed`.
    pub raw_source: &'a [Rec],
    /// What was read back, in any order; it is sorted by `(partition,
    /// offset)` before anything is checked.
    pub observed: &'a [Rec],
    pub headers: HeaderModel,
    pub lineage: LineageFrom,
}

/// Every divergence between `c.expected` and `c.observed`, sorted.
pub fn compare(c: &Comparison<'_>) -> Vec<Divergence> {
    let mut out: Vec<Divergence> = Vec::new();

    let expected: BTreeMap<(i32, i64), &Rec> = c
        .expected
        .iter()
        .map(|r| ((r.partition, r.offset), r))
        .collect();
    let committed: BTreeSet<(i32, i64)> = c
        .committed
        .iter()
        .map(|r| (r.partition, r.offset))
        .collect();
    let raw: BTreeSet<(i32, i64)> = c
        .raw_source
        .iter()
        .map(|r| (r.partition, r.offset))
        .collect();

    let mut observed: Vec<&Rec> = c.observed.iter().collect();
    observed.sort_by_key(|r| (r.partition, r.offset));

    // Copies per (partition, lineage), counted before any matching so a
    // duplicate is reported once with its multiplicity.
    let mut copies: BTreeMap<(i32, i64), usize> = BTreeMap::new();
    for r in &observed {
        if let Some(l) = lineage_of(r, c.lineage) {
            *copies.entry((r.partition, l)).or_default() += 1;
        }
    }
    for ((partition, source_offset), n) in &copies {
        if *n > 1 {
            out.push(Divergence::Duplicate {
                partition: *partition,
                source_offset: *source_offset,
                copies: *n,
            });
        }
    }

    let mut matched: BTreeSet<(i32, i64)> = BTreeSet::new();
    let mut seen: BTreeSet<(i32, i64)> = BTreeSet::new();
    let mut high_water: BTreeMap<i32, i64> = BTreeMap::new();

    for r in &observed {
        let Some(l) = lineage_of(r, c.lineage) else {
            out.push(Divergence::Extra {
                partition: r.partition,
                target_offset: r.offset,
                lineage: None,
                kind: ExtraKind::NoLineage,
            });
            continue;
        };
        let id = (r.partition, l);
        // A later copy of a lineage already seen is the duplicate reported
        // above. It is not judged for order or re-classified, but its FIELDS
        // are checked, so a tampered copy is not mistaken for an exact one.
        if !seen.insert(id) {
            if let Some(want) = expected.get(&id) {
                check_fields(want, r, c.headers, &mut out);
            }
            continue;
        }
        let hw = high_water.entry(r.partition).or_insert(i64::MIN);
        if l < *hw {
            out.push(Divergence::OutOfOrder {
                partition: r.partition,
                source_offset: l,
                after: *hw,
            });
        } else {
            *hw = l;
        }

        match expected.get(&id) {
            Some(want) => {
                matched.insert(id);
                check_fields(want, r, c.headers, &mut out);
            }
            None => {
                let kind = if committed.contains(&id) {
                    ExtraKind::OutsideModel
                } else if raw.contains(&id) {
                    ExtraKind::Uncommitted
                } else if is_control_shaped(r) {
                    ExtraKind::ControlMarker
                } else {
                    ExtraKind::Unexplained
                };
                out.push(Divergence::Extra {
                    partition: r.partition,
                    target_offset: r.offset,
                    lineage: Some(l),
                    kind,
                });
            }
        }
    }

    for id in expected.keys() {
        if !matched.contains(id) {
            out.push(Divergence::Missing {
                partition: id.0,
                source_offset: id.1,
            });
        }
    }

    out.sort();
    out.dedup();
    out
}

fn lineage_of(r: &Rec, from: LineageFrom) -> Option<i64> {
    match from {
        LineageFrom::Header => r.lineage_header(),
        LineageFrom::RecordOffset => Some(r.offset),
    }
}

fn check_fields(want: &Rec, got: &Rec, model: HeaderModel, out: &mut Vec<Divergence>) {
    let (partition, source_offset) = (want.partition, want.offset);
    if want.key != got.key {
        out.push(Divergence::KeyChanged {
            partition,
            source_offset,
        });
    }
    if want.value != got.value {
        out.push(Divergence::ValueChanged {
            partition,
            source_offset,
        });
    }
    if want.timestamp != got.timestamp {
        out.push(Divergence::TimestampChanged {
            partition,
            source_offset,
            expected: want.timestamp,
            observed: got.timestamp,
        });
    }
    let comparable = |t: TsType| t != TsType::Unrecorded;
    if comparable(want.ts_type) && comparable(got.ts_type) && want.ts_type != got.ts_type {
        out.push(Divergence::TimestampTypeChanged {
            partition,
            source_offset,
            expected: want.ts_type,
            observed: got.ts_type,
        });
    }
    let expected_headers = expected_headers(want, got, model);
    if got.headers != expected_headers {
        if got.headers == indexmap_collapse(&expected_headers) {
            out.push(Divergence::HeadersCollapsed {
                partition,
                source_offset,
            });
        } else {
            out.push(Divergence::HeadersChanged {
                partition,
                source_offset,
                expected: expected_headers,
                observed: got.headers.clone(),
            });
        }
    }
}

/// The headers `got` should carry under `model`.
pub fn expected_headers(want: &Rec, got: &Rec, model: HeaderModel) -> Headers {
    match model {
        HeaderModel::Identity => want.headers.clone(),
        HeaderModel::AppendLineage => {
            let mut h = want.headers.clone();
            h.push((X_ORIGINAL_OFFSET.to_string(), le_i64(want.offset)));
            h.push((X_ORIGINAL_TIMESTAMP.to_string(), le_i64(got.timestamp)));
            h
        }
    }
}

/// Class name -> count, for the outcome table.
pub fn summarize(d: &[Divergence]) -> BTreeMap<&'static str, usize> {
    let mut m = BTreeMap::new();
    for x in d {
        *m.entry(x.class()).or_insert(0) += 1;
    }
    m
}

/// A one-line rendering, stable enough to diff between runs.
pub fn describe(d: &Divergence) -> String {
    match d {
        Divergence::Missing {
            partition,
            source_offset,
        } => format!("missing p{partition}@{source_offset}"),
        Divergence::Extra {
            partition,
            target_offset,
            lineage,
            kind,
        } => format!(
            "extra p{partition} target@{target_offset} lineage={} ({kind:?})",
            lineage.map_or_else(|| "none".to_string(), |l| l.to_string())
        ),
        Divergence::Duplicate {
            partition,
            source_offset,
            copies,
        } => format!("duplicate p{partition}@{source_offset} x{copies}"),
        Divergence::OutOfOrder {
            partition,
            source_offset,
            after,
        } => format!("out-of-order p{partition}@{source_offset} after @{after}"),
        Divergence::KeyChanged {
            partition,
            source_offset,
        } => format!("key-changed p{partition}@{source_offset}"),
        Divergence::ValueChanged {
            partition,
            source_offset,
        } => format!("value-changed p{partition}@{source_offset}"),
        Divergence::TimestampChanged {
            partition,
            source_offset,
            expected,
            observed,
        } => format!("timestamp-changed p{partition}@{source_offset} {expected} -> {observed}"),
        Divergence::TimestampTypeChanged {
            partition,
            source_offset,
            expected,
            observed,
        } => format!(
            "timestamp-type-changed p{partition}@{source_offset} {} -> {}",
            expected.as_str(),
            observed.as_str()
        ),
        Divergence::HeadersCollapsed {
            partition,
            source_offset,
        } => format!("headers-collapsed p{partition}@{source_offset}"),
        Divergence::HeadersChanged {
            partition,
            source_offset,
            expected,
            observed,
        } => format!(
            "headers-changed p{partition}@{source_offset} expected {} observed {}",
            render_headers(expected),
            render_headers(observed)
        ),
    }
}

pub fn render_headers(h: &Headers) -> String {
    let parts: Vec<String> = h
        .iter()
        .map(|(k, v)| match v {
            None => format!("{k}=null"),
            Some(b) => format!("{k}={}", render_bytes(b)),
        })
        .collect();
    format!("[{}]", parts.join(", "))
}

/// Printable ASCII as-is, anything else as `0x…`.
pub fn render_bytes(b: &[u8]) -> String {
    if !b.is_empty() && b.iter().all(|c| c.is_ascii_graphic() || *c == b' ') {
        format!("{:?}", String::from_utf8_lossy(b))
    } else {
        let hex: String = b.iter().map(|c| format!("{c:02x}")).collect();
        format!("0x{hex}")
    }
}
