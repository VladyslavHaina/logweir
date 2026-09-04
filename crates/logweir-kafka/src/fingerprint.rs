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

#[cfg(test)]
mod tests {
    use super::record_fingerprint;

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
