use sha2::{Digest, Sha256};

/// sha256(key ‖ value ‖ headers ‖ timestamp).
///
/// Every component is length-prefixed as an 8-byte big-endian count, and a
/// null is encoded as the count `u64::MAX`, so a null value and an empty value
/// cannot collide. Headers are sorted by (key, value) before hashing because
/// Kafka does not guarantee header order across a produce/consume round trip.
pub fn record_fingerprint(
    key: Option<&[u8]>,
    value: Option<&[u8]>,
    headers: &[(String, Option<Vec<u8>>)],
    timestamp_ms: i64,
) -> String {
    fn feed(h: &mut Sha256, part: Option<&[u8]>) {
        match part {
            None => h.update(u64::MAX.to_be_bytes()),
            Some(b) => {
                h.update((b.len() as u64).to_be_bytes());
                h.update(b);
            }
        }
    }
    let mut h = Sha256::new();
    feed(&mut h, key);
    feed(&mut h, value);
    let mut hs: Vec<(&str, Option<&[u8]>)> = headers
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_deref()))
        .collect();
    hs.sort();
    h.update((hs.len() as u64).to_be_bytes());
    for (k, v) in hs {
        feed(&mut h, Some(k.as_bytes()));
        feed(&mut h, v);
    }
    h.update(timestamp_ms.to_be_bytes());
    hex::encode(h.finalize())
}

/// **PROD-08.1, complete verification.** sha256(key ‖ value ‖ headers ‖
/// timestamp) with the headers in their RECORDED ORDER, every occurrence
/// kept — the record exactly as the archive holds it and as a consumer reads
/// it back.
///
/// The byte layout is [`record_fingerprint`]'s with one difference: the
/// headers are NOT sorted. Each component is length-prefixed as an 8-byte
/// big-endian count, a null is the count `u64::MAX`, then the header count,
/// each header's key and value in order, and the 8-byte big-endian
/// timestamp.
///
/// # Why a second digest, and why ordered
///
/// [`record_fingerprint`] sorts the headers, so a restore that reordered a
/// record's headers reconciles as a match: PROD-01.1's acceptance row 08-6
/// ("header comparison is ordered and multiplicity-aware in complete mode, or
/// the evidence states that header order is not verified"). Kafka keeps a
/// record's headers in order — they are an array in the record format, and
/// PROD-01.1 measured the pinned engine keeping header order through capture
/// and replay (its contract row C5) — so an ordered comparison holds a correct
/// restore to nothing it cannot meet. Sampled verification keeps the sorted
/// fingerprint, and its evidence says header order is not verified.
///
/// Returns the raw 32 bytes: complete verification holds one digest per
/// archived record of a partition in memory, so the hex form would double it.
pub fn record_digest_ordered(
    key: Option<&[u8]>,
    value: Option<&[u8]>,
    headers: &[(String, Option<Vec<u8>>)],
    timestamp_ms: i64,
) -> [u8; 32] {
    fn feed(h: &mut Sha256, part: Option<&[u8]>) {
        match part {
            None => h.update(u64::MAX.to_be_bytes()),
            Some(b) => {
                h.update((b.len() as u64).to_be_bytes());
                h.update(b);
            }
        }
    }
    let mut h = Sha256::new();
    feed(&mut h, key);
    feed(&mut h, value);
    h.update((headers.len() as u64).to_be_bytes());
    for (k, v) in headers {
        feed(&mut h, Some(k.as_bytes()));
        feed(&mut h, v.as_deref());
    }
    h.update(timestamp_ms.to_be_bytes());
    h.finalize().into()
}

#[cfg(test)]
mod tests {
    use super::{record_digest_ordered, record_fingerprint};

    /// The ordered digest of a record with no headers is the sorted
    /// fingerprint's bytes: sorting zero headers changes nothing, so the two
    /// layouts must agree exactly (the hand-computed literal of
    /// `hand_computed_digest_for_a_simple_key_value_pair`).
    #[test]
    fn the_ordered_digest_without_headers_is_the_fingerprints_layout() {
        assert_eq!(
            hex::encode(record_digest_ordered(Some(b"k"), Some(b"v"), &[], 1)),
            "a212be3a0bca153705386373c72d35d273ba266db8cdf05cfb3230131a4bf177"
        );
    }

    /// Hand-computed OUTSIDE this implementation (Python hashlib over the
    /// documented layout): key "k", value "v", headers in the order
    /// `b=2, a=1`, timestamp 42 — the order the sorted fingerprint would
    /// reverse.
    #[test]
    fn hand_computed_ordered_digest_keeps_the_header_order() {
        // sha256( be64(1)||"k" || be64(1)||"v" || be64(2) ||
        //         be64(1)||"b" || be64(1)||"2" || be64(1)||"a" || be64(1)||"1" ||
        //         be64_i(42) )
        let ordered = [
            ("b".to_string(), Some(b"2".to_vec())),
            ("a".to_string(), Some(b"1".to_vec())),
        ];
        assert_eq!(
            hex::encode(record_digest_ordered(Some(b"k"), Some(b"v"), &ordered, 42)),
            "8b0ff8ddfd9af550df9f30e17b0fc511a358ed454c531b33cca21b1b1e74e3c7"
        );
    }

    /// The property complete verification relies on, and the one the sorted
    /// fingerprint cannot give: two records that differ ONLY in header order
    /// digest differently; so do one header and the same header twice.
    #[test]
    fn header_order_and_multiplicity_change_the_ordered_digest_only() {
        let ab = [
            ("a".to_string(), Some(b"1".to_vec())),
            ("b".to_string(), Some(b"2".to_vec())),
        ];
        let ba = [ab[1].clone(), ab[0].clone()];
        assert_ne!(
            record_digest_ordered(Some(b"k"), Some(b"v"), &ab, 7),
            record_digest_ordered(Some(b"k"), Some(b"v"), &ba, 7)
        );
        // The sorted fingerprint cannot tell them apart: the gap 08-6 names.
        assert_eq!(
            record_fingerprint(Some(b"k"), Some(b"v"), &ab, 7),
            record_fingerprint(Some(b"k"), Some(b"v"), &ba, 7)
        );
        let once = [ab[0].clone()];
        let twice = [ab[0].clone(), ab[0].clone()];
        assert_ne!(
            record_digest_ordered(Some(b"k"), Some(b"v"), &once, 7),
            record_digest_ordered(Some(b"k"), Some(b"v"), &twice, 7)
        );
        // Null versus empty header value, key and value stay distinct.
        assert_ne!(
            record_digest_ordered(Some(b"k"), None, &[], 7),
            record_digest_ordered(Some(b"k"), Some(b""), &[], 7)
        );
        assert_ne!(
            record_digest_ordered(None, Some(b"v"), &[], 7),
            record_digest_ordered(Some(b""), Some(b"v"), &[], 7)
        );
        assert_ne!(
            record_digest_ordered(Some(b"k"), Some(b"v"), &[("h".into(), None)], 7),
            record_digest_ordered(Some(b"k"), Some(b"v"), &[("h".into(), Some(Vec::new()))], 7)
        );
    }

    // Each expected digest below was computed OUTSIDE this implementation (a
    // standalone Python script doing the same byte assembly by hand: an
    // 8-byte big-endian length or `u64::MAX` per component, concatenated key,
    // value, header count, sorted headers, then an 8-byte big-endian
    // timestamp) and then hashed with hashlib.sha256. A round-trip test can
    // pass under two different constructions that happen to agree with each
    // other; a hand-computed literal can only pass if this function matches
    // the documented byte layout exactly — field order, big-endian length
    // prefixes, and the `u64::MAX` null marker included.

    #[test]
    fn hand_computed_digest_for_a_simple_key_value_pair() {
        // sha256( be64(1) || "k" || be64(1) || "v" || be64(0) || be64(1) )
        assert_eq!(
            record_fingerprint(Some(b"k"), Some(b"v"), &[], 1),
            "a212be3a0bca153705386373c72d35d273ba266db8cdf05cfb3230131a4bf177"
        );
    }

    #[test]
    fn hand_computed_digest_for_a_null_value() {
        // sha256( be64(1) || "k" || be64(u64::MAX) || be64(0) || be64(1) )
        assert_eq!(
            record_fingerprint(Some(b"k"), None, &[], 1),
            "34bb8d681621430ea68f3060cd9155e5239c9d2838b784580f4b219ac2d0f43c"
        );
    }

    #[test]
    fn hand_computed_digest_for_an_empty_value() {
        // sha256( be64(1) || "k" || be64(0) || be64(0) || be64(1) )
        assert_eq!(
            record_fingerprint(Some(b"k"), Some(b""), &[], 1),
            "823e279467bfb49080f1749effb24d7d7f5a195e3dc5050a845ba329aef2de4a"
        );
    }

    #[test]
    fn hand_computed_digest_with_a_header_present() {
        // sha256( be64(1)||"k" || be64(1)||"v" || be64(1) || be64(1)||"a" ||
        //         be64(1)||"1" || be64_i(42) )
        assert_eq!(
            record_fingerprint(
                Some(b"k"),
                Some(b"v"),
                &[("a".to_string(), Some(b"1".to_vec()))],
                42
            ),
            "242c78f417cc240704228a3ca3241b52dc20bae8efb721d000c5041de0764dcd"
        );
    }
}
