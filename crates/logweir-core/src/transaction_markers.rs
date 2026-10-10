//! **PROD-01.1a.** A restore of an archive that holds Kafka transaction
//! control records is refused unless its plan allows them.
//!
//! The pinned engine captures `READ_UNCOMMITTED` and archives every commit
//! and abort marker as an ordinary record, and the restore writes each one,
//! with the aborted records before it, as data every consumer reads
//! (`docs/to-do/decisions/PROD-01.1-record-semantics.md` §2.1). Before phase 2
//! the runner counts the records with a marker's shape ([`is_control_shaped`])
//! in the segments the restore reads; a count is refused by name
//! ([`refusal`]) unless the approved plan states
//! `restore.allow_transaction_markers: true`, and a run under that override
//! says so in its scorecard ([`OVERRIDE_NOTE`]).
//!
//! The shape alone is the signal: the source is not read, so the decision
//! record's offset-gap confirmation (§6.2) is not made, and a user record of
//! the same shape is counted too. Not seen: a transaction open at capture
//! whose marker is not in the archive, one opened after the backup began, one
//! whose marker is in a segment the restore's window does not reach, and a
//! marker of another encoding version.

use std::collections::BTreeMap;

use crate::guard::TERMINAL_STATE_TRANSACTION_MARKERS_IN_ARCHIVE;

/// The two headers the backup appends to every archived record.
pub const LINEAGE_HEADERS: [&[u8]; 2] = [b"x-original-offset", b"x-original-timestamp"];

/// How many bytes of a key, a value and a header key a caller must keep for
/// [`is_control_shaped`] to judge them: more than the longest lineage header
/// name, so a longer key cut to this length never equals one.
pub const SHAPE_PREFIX: usize = 32;

/// A transaction control record by shape: key `00 00 00 00` (abort) or
/// `00 00 00 01` (commit), a 6-byte value starting `00 00`
/// (`EndTransactionMarker` v0), and no header but the lineage headers. The
/// same predicate as the e2e oracle's `is_control_shaped`. `key`, `value` and
/// each header key may be cut to [`SHAPE_PREFIX`] bytes.
#[must_use]
pub fn is_control_shaped(
    key: Option<&[u8]>,
    value: Option<&[u8]>,
    header_keys: &[Vec<u8>],
) -> bool {
    matches!(key, Some([0, 0, 0, 0] | [0, 0, 0, 1]))
        && matches!(value, Some([0, 0, _, _, _, _]))
        && header_keys
            .iter()
            .all(|k| LINEAGE_HEADERS.contains(&k.as_slice()))
}

/// Marker-shaped records per source topic and partition.
pub type MarkerCounts = BTreeMap<String, BTreeMap<i32, u64>>;

/// What a run under `restore.allow_transaction_markers: true` adds to its
/// scorecard's `sample.coverage_note`. The check is not run under the
/// override, so the note says what the plan allowed, not what was found.
pub const OVERRIDE_NOTE: &str = "restore.allow_transaction_markers: true: the archive was not \
     checked for transaction markers; any it holds, and the records of aborted transactions \
     beside them, were restored as ordinary data that every consumer reads";

/// The refusal for `counts`, or `None` when it holds no record. The message
/// opens with [`TERMINAL_STATE_TRANSACTION_MARKERS_IN_ARCHIVE`], so the
/// runner's `refusal-reason=` line names it.
#[must_use]
pub fn refusal(counts: &MarkerCounts) -> Option<String> {
    let total: u64 = counts.values().flat_map(BTreeMap::values).sum();
    if total == 0 {
        return None;
    }
    let found: Vec<String> = counts
        .iter()
        .filter(|(_, parts)| parts.values().any(|n| *n > 0))
        .map(|(topic, parts)| {
            let n: u64 = parts.values().sum();
            let list: Vec<String> = parts
                .iter()
                .filter(|(_, n)| **n > 0)
                .map(|(p, _)| p.to_string())
                .collect();
            format!("{n} in topic `{topic}` (partitions {})", list.join(", "))
        })
        .collect();
    Some(format!(
        "{TERMINAL_STATE_TRANSACTION_MARKERS_IN_ARCHIVE}: the archive holds {total} record(s) \
         shaped like Kafka transaction markers (commit or abort control records): {}. Restoring \
         them would write each marker, and the records of aborted transactions beside it, as \
         ordinary data that every consumer of the restored topic reads, read_committed consumers \
         included. The run stopped before any target topic was created. The check reads the \
         shape only, so a record of your own with an integer key 0 or 1 and a 6-byte value \
         starting 00 00 is counted too. To restore the archive as it is, state \
         `restore.allow_transaction_markers: true` in the plan and have the changed plan approved \
         (the field is inside plan_hash); the signed scorecard then says so.",
        found.join("; ")
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    const LINEAGE: [&[u8]; 2] = LINEAGE_HEADERS;

    fn lineage() -> Vec<Vec<u8>> {
        LINEAGE.iter().map(|k| k.to_vec()).collect()
    }

    /// R3: a marker of each type is caught, archived with its lineage headers
    /// or with none.
    #[test]
    fn a_commit_and_an_abort_marker_are_caught() {
        let value = [0u8, 0, 0, 0, 0, 3];
        for key in [[0u8, 0, 0, 1], [0, 0, 0, 0]] {
            assert!(is_control_shaped(Some(&key), Some(&value), &lineage()));
            assert!(is_control_shaped(Some(&key), Some(&value), &[]));
        }
    }

    /// R3: a user record of the same shape is counted too (the stated false
    /// positive the override gets past).
    #[test]
    fn a_user_record_of_the_same_shape_is_caught_too() {
        let integer_one = 1u32.to_be_bytes();
        let framed_one_byte = [0u8, 0, 0, 0, 0, 42];
        assert!(is_control_shaped(
            Some(&integer_one),
            Some(&framed_one_byte),
            &lineage()
        ));
    }

    /// R3's controls: a 7-byte value, a non-zero key prefix, and the other
    /// near misses of the shape are not caught.
    #[test]
    fn near_misses_of_the_shape_are_not_caught() {
        let key = [0u8, 0, 0, 1];
        let value = [0u8, 0, 0, 0, 0, 3];
        let own = vec![b"trace".to_vec()];
        let longer = vec![b"x-original-offset-and-more".to_vec()];
        for (what, k, v, h) in [
            (
                "7-byte value",
                &key[..],
                &[0u8, 0, 0, 0, 0, 3, 0][..],
                lineage(),
            ),
            ("5-byte value", &key[..], &[0u8, 0, 0, 0, 0][..], lineage()),
            (
                "value version 00 01",
                &key[..],
                &[0u8, 1, 0, 0, 0, 3][..],
                lineage(),
            ),
            ("key prefix 01", &[1u8, 0, 0, 1][..], &value[..], lineage()),
            (
                "key version 00 01",
                &[0u8, 1, 0, 1][..],
                &value[..],
                lineage(),
            ),
            ("type 2", &[0u8, 0, 0, 2][..], &value[..], lineage()),
            ("5-byte key", &[0u8, 0, 0, 0, 1][..], &value[..], lineage()),
            ("an own header", &key[..], &value[..], own),
            ("a longer header key", &key[..], &value[..], longer),
        ] {
            assert!(!is_control_shaped(Some(k), Some(v), &h), "{what}");
        }
        assert!(!is_control_shaped(None, Some(&value), &[]), "null key");
        assert!(!is_control_shaped(Some(&key), None, &[]), "null value");
    }

    #[test]
    fn the_refusal_names_the_state_the_topics_partitions_and_count() {
        assert_eq!(refusal(&MarkerCounts::new()), None);
        let counts: MarkerCounts = [
            ("orders".to_string(), [(0, 3u64), (2, 4)].into()),
            ("payments".to_string(), [(1, 1u64)].into()),
        ]
        .into();
        let text = refusal(&counts).expect("a count refuses");
        assert!(text.starts_with("TransactionMarkersInArchive: "), "{text}");
        for owed in [
            "holds 8 record(s)",
            "7 in topic `orders` (partitions 0, 2)",
            "1 in topic `payments` (partitions 1)",
            "aborted transactions",
            "every consumer",
            "restore.allow_transaction_markers: true",
        ] {
            assert!(text.contains(owed), "`{owed}` missing: {text}");
        }
    }
}
