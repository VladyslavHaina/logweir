//! PROD-01.1: the record oracle's negative controls. **Not** `e2e`-gated: no
//! broker, no bucket and no engine, so they run in the default test set on
//! every CI job.
//!
//! "A guard without a mutant is not a guard." `e2e/tests/record_semantics.rs`
//! asserts that a restore's output differs from its committed input in
//! exactly the ways PROD-01.1's decision record lists, and that assertion is
//! only worth something if [`oracle::compare`] reports every other kind of
//! difference. Each row below injects one divergence into an otherwise
//! identical pair and requires the exact class it should produce; the first
//! row requires the identical pair to produce nothing, so a comparator that
//! reported everything could not pass either.
#[path = "record_semantics_support/oracle.rs"]
mod oracle;

use oracle::*;

/// A source record with the given identity and payload.
fn src(partition: i32, offset: i64, ts: i64, key: Option<&[u8]>, value: Option<&[u8]>) -> Rec {
    Rec {
        partition,
        offset,
        timestamp: ts,
        ts_type: TsType::CreateTime,
        key: key.map(<[u8]>::to_vec),
        value: value.map(<[u8]>::to_vec),
        headers: Vec::new(),
    }
}

/// What a correct restore writes for `s` at `target_offset`: the same record,
/// plus the two lineage headers the backup appends.
fn restored(s: &Rec, target_offset: i64) -> Rec {
    let mut r = s.clone();
    r.offset = target_offset;
    r.ts_type = TsType::CreateTime;
    r.headers
        .push((X_ORIGINAL_OFFSET.to_string(), le_i64(s.offset)));
    r.headers
        .push((X_ORIGINAL_TIMESTAMP.to_string(), le_i64(s.timestamp)));
    r
}

/// Three records on each of two partitions, source offsets 0..3.
fn fixture() -> Vec<Rec> {
    let mut v = Vec::new();
    for p in 0..2 {
        for o in 0..3 {
            let k = format!("k{p}{o}");
            let val = format!("v{p}{o}");
            v.push(src(
                p,
                o,
                1_000 + o,
                Some(k.as_bytes()),
                Some(val.as_bytes()),
            ));
        }
    }
    v
}

fn restore_of(s: &[Rec]) -> Vec<Rec> {
    let mut next = std::collections::BTreeMap::<i32, i64>::new();
    s.iter()
        .map(|r| {
            let t = next.entry(r.partition).or_insert(0);
            let out = restored(r, *t);
            *t += 1;
            out
        })
        .collect()
}

fn end_to_end(expected: &[Rec], raw: &[Rec], observed: &[Rec]) -> Vec<Divergence> {
    compare(&Comparison {
        expected,
        raw_source: raw,
        observed,
        headers: HeaderModel::AppendLineage,
        lineage: LineageFrom::Header,
    })
}

// ------------------------------------------------------------ the baseline

#[test]
fn an_identical_restore_has_no_divergence() {
    let s = fixture();
    let t = restore_of(&s);
    assert_eq!(end_to_end(&s, &s, &t), vec![]);
}

#[test]
fn an_identical_archive_has_no_divergence_under_either_lineage_source() {
    let s = fixture();
    // An archive record: source offset in the record, lineage headers already
    // appended, no timestamp type.
    let archive: Vec<Rec> = s
        .iter()
        .map(|r| {
            let mut a = restored(r, r.offset);
            a.ts_type = TsType::Unrecorded;
            a
        })
        .collect();
    let capture = compare(&Comparison {
        expected: &s,
        raw_source: &s,
        observed: &archive,
        headers: HeaderModel::AppendLineage,
        lineage: LineageFrom::RecordOffset,
    });
    assert_eq!(capture, vec![]);
    let replay = compare(&Comparison {
        expected: &archive,
        raw_source: &archive,
        observed: &restore_of(&s),
        headers: HeaderModel::Identity,
        lineage: LineageFrom::Header,
    });
    assert_eq!(replay, vec![]);
}

// ------------------------------------------------- record-set divergences

#[test]
fn a_dropped_record_is_missing() {
    let s = fixture();
    let mut t = restore_of(&s);
    t.retain(|r| !(r.partition == 1 && r.lineage_header() == Some(1)));
    assert_eq!(
        end_to_end(&s, &s, &t),
        vec![Divergence::Missing {
            partition: 1,
            source_offset: 1
        }]
    );
}

#[test]
fn an_aborted_record_restored_as_data_is_an_uncommitted_extra() {
    // The raw view holds offset 3 on p0; the committed model does not.
    let raw = {
        let mut r = fixture();
        r.push(src(0, 3, 1_003, Some(b"aborted"), Some(b"x")));
        r
    };
    let committed = fixture();
    let t = restore_of(&raw);
    assert_eq!(
        end_to_end(&committed, &raw, &t),
        vec![Divergence::Extra {
            partition: 0,
            target_offset: 3,
            lineage: Some(3),
            kind: ExtraKind::Uncommitted
        }]
    );
}

#[test]
fn a_restored_commit_marker_is_a_control_marker_extra() {
    let s = fixture();
    let mut t = restore_of(&s);
    // Offset 3 is in neither view (a consumer never returns a control
    // record), and the record has ControlRecordType/EndTransactionMarker v0.
    let mut marker = src(0, 3, 1_010, Some(&[0, 0, 0, 1]), Some(&[0, 0, 0, 0, 0, 7]));
    marker = restored(&marker, 3);
    t.push(marker);
    assert_eq!(
        end_to_end(&s, &s, &t),
        vec![Divergence::Extra {
            partition: 0,
            target_offset: 3,
            lineage: Some(3),
            kind: ExtraKind::ControlMarker
        }]
    );
}

#[test]
fn an_extra_that_is_neither_uncommitted_nor_control_shaped_is_unexplained() {
    let s = fixture();
    let mut t = restore_of(&s);
    // Control-shaped key but a 5-byte value: not a marker.
    t.push(restored(
        &src(0, 3, 1, Some(&[0, 0, 0, 1]), Some(&[0, 0, 0, 0, 7])),
        3,
    ));
    assert_eq!(
        end_to_end(&s, &s, &t),
        vec![Divergence::Extra {
            partition: 0,
            target_offset: 3,
            lineage: Some(3),
            kind: ExtraKind::Unexplained
        }]
    );
}

#[test]
fn a_record_without_lineage_is_reported_and_its_source_is_missing() {
    let s = fixture();
    let mut t = restore_of(&s);
    t[0].headers.clear();
    assert_eq!(
        end_to_end(&s, &s, &t),
        vec![
            Divergence::Missing {
                partition: 0,
                source_offset: 0
            },
            Divergence::Extra {
                partition: 0,
                target_offset: 0,
                lineage: None,
                kind: ExtraKind::NoLineage
            },
        ]
    );
}

#[test]
fn a_retried_batch_is_reported_as_duplicates() {
    // p0 written 0,1,2 and then 0,1 again: the shape of a produce retried
    // after its acknowledgement was lost.
    let s = fixture();
    let mut t = restore_of(&s);
    let again: Vec<Rec> = s
        .iter()
        .filter(|r| r.partition == 0 && r.offset < 2)
        .enumerate()
        .map(|(i, r)| restored(r, 3 + i as i64))
        .collect();
    t.extend(again);
    assert_eq!(
        end_to_end(&s, &s, &t),
        vec![
            Divergence::Duplicate {
                partition: 0,
                source_offset: 0,
                copies: 2
            },
            Divergence::Duplicate {
                partition: 0,
                source_offset: 1,
                copies: 2
            },
        ],
        "a later copy is judged only as a duplicate; the ORDER check applies to first copies"
    );
}

#[test]
fn a_reordered_partition_is_out_of_order() {
    let s = fixture();
    let mut t = restore_of(&s);
    // Swap the target offsets of p1's source offsets 1 and 2.
    for r in t.iter_mut().filter(|r| r.partition == 1) {
        r.offset = match r.offset {
            1 => 2,
            2 => 1,
            o => o,
        };
    }
    assert_eq!(
        end_to_end(&s, &s, &t),
        vec![Divergence::OutOfOrder {
            partition: 1,
            source_offset: 1,
            after: 2
        }]
    );
}

// ------------------------------------------------------ field divergences

#[test]
fn null_and_empty_keys_and_values_are_different_values() {
    let mut s = fixture();
    s[0].key = None; // null key in the source
    s[1].value = Some(Vec::new()); // empty value in the source
    let mut t = restore_of(&s);
    t[0].key = Some(Vec::new()); // restored as empty
    t[1].value = None; // restored as null
    assert_eq!(
        end_to_end(&s, &s, &t),
        vec![
            Divergence::KeyChanged {
                partition: 0,
                source_offset: 0
            },
            Divergence::ValueChanged {
                partition: 0,
                source_offset: 1
            },
        ]
    );
}

#[test]
fn a_changed_timestamp_and_timestamp_type_are_reported_separately() {
    let mut s = fixture();
    s[2].ts_type = TsType::LogAppendTime;
    let mut t = restore_of(&s);
    t[2].ts_type = TsType::CreateTime;
    t[2].timestamp = 42;
    // The engine stamps x-original-timestamp with the timestamp it restored,
    // so the header model follows the observed timestamp and the difference
    // is reported once, as a timestamp.
    t[2].headers[1].1 = le_i64(42);
    assert_eq!(
        end_to_end(&s, &s, &t),
        vec![
            Divergence::TimestampChanged {
                partition: 0,
                source_offset: 2,
                expected: 1_002,
                observed: 42
            },
            Divergence::TimestampTypeChanged {
                partition: 0,
                source_offset: 2,
                expected: TsType::LogAppendTime,
                observed: TsType::CreateTime
            },
        ]
    );
}

#[test]
fn an_unrecorded_timestamp_type_is_never_compared() {
    let mut s = fixture();
    s[0].ts_type = TsType::LogAppendTime;
    let mut t = restore_of(&s);
    t[0].ts_type = TsType::Unrecorded;
    assert_eq!(end_to_end(&s, &s, &t), vec![]);
}

#[test]
fn collapsed_duplicate_headers_are_named_as_such() {
    let mut s = fixture();
    s[0].headers = vec![
        ("h".into(), Some(b"a".to_vec())),
        ("x".into(), Some(b"1".to_vec())),
        ("h".into(), Some(b"b".to_vec())),
        ("h".into(), Some(b"c".to_vec())),
    ];
    let mut t = restore_of(&s);
    t[0].headers = indexmap_collapse(&t[0].headers);
    assert_eq!(
        t[0].headers[..2],
        [
            ("h".to_string(), Some(b"c".to_vec())),
            ("x".to_string(), Some(b"1".to_vec()))
        ],
        "IndexMap::insert keeps the first position and the last value"
    );
    assert_eq!(
        end_to_end(&s, &s, &t),
        vec![Divergence::HeadersCollapsed {
            partition: 0,
            source_offset: 0
        }]
    );
}

/// One named header mutation.
type Injection = (&'static str, fn(&mut Headers));

#[test]
fn any_other_header_difference_is_headers_changed() {
    // Three independent injections, each alone.
    let base = {
        let mut s = fixture();
        s[0].headers = vec![
            ("a".into(), Some(b"1".to_vec())),
            ("n".into(), None),
            ("e".into(), Some(Vec::new())),
        ];
        s
    };
    let injections: [Injection; 3] = [
        ("dropped", |h| {
            h.remove(0);
        }),
        ("reordered", |h| h.swap(0, 1)),
        ("null became empty", |h| h[1].1 = Some(Vec::new())),
    ];
    for (what, inject) in injections {
        let mut t = restore_of(&base);
        inject(&mut t[0].headers);
        let d = end_to_end(&base, &base, &t);
        assert_eq!(d.len(), 1, "{what}: {d:?}");
        assert!(
            matches!(
                d[0],
                Divergence::HeadersChanged {
                    partition: 0,
                    source_offset: 0,
                    ..
                }
            ),
            "{what}: {d:?}"
        );
    }
}

#[test]
fn a_missing_engine_lineage_header_is_not_tolerated_under_append_lineage() {
    // Lineage read from the record offset (an archive), so the record still
    // matches, and the absent x-original-timestamp is a header divergence.
    let s = fixture();
    let mut archive: Vec<Rec> = s.iter().map(|r| restored(r, r.offset)).collect();
    archive[0].headers.pop();
    let d = compare(&Comparison {
        expected: &s,
        raw_source: &s,
        observed: &archive,
        headers: HeaderModel::AppendLineage,
        lineage: LineageFrom::RecordOffset,
    });
    assert_eq!(d.len(), 1, "{d:?}");
    assert!(matches!(d[0], Divergence::HeadersChanged { .. }), "{d:?}");
}

// ------------------------------------------------------- the helpers

#[test]
fn the_last_eight_byte_original_offset_is_the_lineage() {
    let mut r = src(0, 9, 0, None, None);
    r.headers = vec![
        (X_ORIGINAL_OFFSET.into(), le_i64(777)),
        (X_ORIGINAL_OFFSET.into(), Some(b"short".to_vec())),
        (X_ORIGINAL_OFFSET.into(), le_i64(9)),
        (X_ORIGINAL_OFFSET.into(), None),
    ];
    assert_eq!(r.lineage_header(), Some(9));
    r.headers.truncate(1);
    assert_eq!(r.lineage_header(), Some(777));
    r.headers.clear();
    assert_eq!(r.lineage_header(), None);
}

#[test]
fn control_record_shapes() {
    let commit = src(0, 0, 0, Some(&[0, 0, 0, 1]), Some(&[0, 0, 0, 0, 0, 3]));
    let abort = src(0, 0, 0, Some(&[0, 0, 0, 0]), Some(&[0, 0, 0, 0, 0, 3]));
    assert_eq!(control_is_commit(&commit), Some(true));
    assert_eq!(control_is_commit(&abort), Some(false));
    for (what, k, v) in [
        (
            "type 2",
            Some(&[0u8, 0, 0, 2][..]),
            Some(&[0u8, 0, 0, 0, 0, 3][..]),
        ),
        (
            "version 1 key",
            Some(&[0, 1, 0, 1][..]),
            Some(&[0, 0, 0, 0, 0, 3][..]),
        ),
        ("null value", Some(&[0, 0, 0, 1][..]), None),
        ("short value", Some(&[0, 0, 0, 1][..]), Some(&[0, 0, 0][..])),
        ("null key", None, Some(&[0, 0, 0, 0, 0, 3][..])),
    ] {
        assert!(!is_control_shaped(&src(0, 0, 0, k, v)), "{what}");
    }
}

#[test]
fn summaries_count_by_class() {
    let d = vec![
        Divergence::Missing {
            partition: 0,
            source_offset: 1,
        },
        Divergence::Missing {
            partition: 1,
            source_offset: 1,
        },
        Divergence::HeadersCollapsed {
            partition: 0,
            source_offset: 4,
        },
    ];
    let s = summarize(&d);
    assert_eq!(s.get("missing"), Some(&2));
    assert_eq!(s.get("headers-collapsed"), Some(&1));
    assert_eq!(s.len(), 2);
}
