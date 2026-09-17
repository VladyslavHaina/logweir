//! Reading a backup manifest, PURELY — the half of D2 §6.7's archive checks
//! that is not I/O.
//!
//! # Why this exists beside `logweir_store`
//!
//! `Store::manifest_facts` and `Store::segment_keys_for_set` read exactly
//! these field paths, and D2 §6.7 names the second one. Both take a `&Store`,
//! and a `Store` can only be built over a real backend — which is the one
//! thing a check's denial paths (`AccessDenied`, `InvalidCredentials`,
//! `RegionMismatch`) need to be fakeable without. So the restore preflight
//! reads the manifest through [`super::store::ObjectAccess`], a four-method
//! seam, and interprets the bytes here.
//!
//! **That is a second implementation of one JSON shape, and a second
//! implementation is how two readers come to disagree.** It is held to the
//! first by `the_pure_segment_walk_agrees_with_the_store` in
//! `crates/logweir/tests/check_cli.rs`, which drives BOTH over the same
//! filesystem archive fixture and asserts the same keys in the same order —
//! including the `qualify` rule, which is the one a review already found
//! wrong once (`segment_keys_for` used to return manifest-relative keys, so
//! every present segment 404'd on a prefixed archive).
//!
//! # The field paths
//!
//! `topics[].name`, `.partitions[].partition_id`,
//! `.segments[].{key,start_timestamp,end_timestamp}` — the three upstream
//! writes and the three `logweir_store` reads.

use std::collections::BTreeSet;

use serde_json::Value;

/// The window a whole manifest bounds: the smallest `start_timestamp` and the
/// largest `end_timestamp` over every segment it names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ManifestWindow {
    pub oldest_ms: i64,
    pub newest_ms: i64,
}

/// Why a manifest could not be interpreted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManifestError {
    /// The bytes are not JSON, or declare no `topics` array. This is a
    /// CONFIGURATION fact — the key points at something that is not a
    /// manifest — and `logweir_store::StoreError::NotAManifest` makes the same
    /// distinction for the same reason.
    NotAManifest,
    /// Manifest-shaped, but it names no segment, so it bounds no window. A
    /// fact about a real backup set, not a mistake.
    NoSegments,
}

/// Parse the bytes, or say which of the two failures it was.
///
/// # Errors
/// [`ManifestError::NotAManifest`].
pub fn parse(bytes: &[u8]) -> Result<Value, ManifestError> {
    let v: Value = serde_json::from_slice(bytes).map_err(|_| ManifestError::NotAManifest)?;
    match v.get("topics").and_then(Value::as_array) {
        Some(_) => Ok(v),
        None => Err(ManifestError::NotAManifest),
    }
}

fn topic_entries(v: &Value) -> &[Value] {
    v.get("topics")
        .and_then(Value::as_array)
        .map_or(&[][..], |a| a.as_slice())
}

fn partition_entries(t: &Value) -> &[Value] {
    t.get("partitions")
        .and_then(Value::as_array)
        .map_or(&[][..], |a| a.as_slice())
}

fn segment_entries(p: &Value) -> &[Value] {
    p.get("segments")
        .and_then(Value::as_array)
        .map_or(&[][..], |a| a.as_slice())
}

/// Every topic name the manifest declares.
#[must_use]
pub fn topics(v: &Value) -> BTreeSet<String> {
    topic_entries(v)
        .iter()
        .filter_map(|t| t.get("name").and_then(Value::as_str))
        .map(ToString::to_string)
        .collect()
}

/// The window the whole set covers.
///
/// # Errors
/// [`ManifestError::NoSegments`] when it names none.
pub fn window(v: &Value) -> Result<ManifestWindow, ManifestError> {
    let mut oldest: Option<i64> = None;
    let mut newest: Option<i64> = None;
    for t in topic_entries(v) {
        for p in partition_entries(t) {
            for s in segment_entries(p) {
                let (Some(t0), Some(t1)) = (
                    s.get("start_timestamp").and_then(Value::as_i64),
                    s.get("end_timestamp").and_then(Value::as_i64),
                ) else {
                    continue;
                };
                oldest = Some(oldest.map_or(t0, |o: i64| o.min(t0)));
                newest = Some(newest.map_or(t1, |n: i64| n.max(t1)));
            }
        }
    }
    match (oldest, newest) {
        (Some(oldest_ms), Some(newest_ms)) => Ok(ManifestWindow {
            oldest_ms,
            newest_ms,
        }),
        _ => Err(ManifestError::NoSegments),
    }
}

/// The qualified segment keys one topic/partition contributes to `window`.
///
/// The filter is `start <= window.1 && end >= window.0` — the CLOSED-interval
/// overlap `Store::segment_keys_from` applies, and not a containment test: a
/// segment that straddles the window's edge holds in-window records.
///
/// `qualify` turns a manifest-relative key into the key space `get` operates
/// in. It is a parameter rather than a rule because it is the handle's
/// property (`Store::qualify` is `prefix` plus `/`), and the review that found
/// it missing is why it is not optional here.
#[must_use]
pub fn segment_keys_for(
    v: &Value,
    topic: &str,
    partition: i64,
    window: (i64, i64),
    qualify: &dyn Fn(&str) -> String,
) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for t in topic_entries(v) {
        if t.get("name").and_then(Value::as_str) != Some(topic) {
            continue;
        }
        for p in partition_entries(t) {
            if p.get("partition_id").and_then(Value::as_i64) != Some(partition) {
                continue;
            }
            for s in segment_entries(p) {
                let (Some(k), Some(t0), Some(t1)) = (
                    s.get("key").and_then(Value::as_str),
                    s.get("start_timestamp").and_then(Value::as_i64),
                    s.get("end_timestamp").and_then(Value::as_i64),
                ) else {
                    continue;
                };
                if t0 <= window.1 && t1 >= window.0 {
                    out.push(qualify(k));
                }
            }
        }
    }
    out.sort();
    out.dedup();
    out
}

/// Every qualified segment key the named topics contribute to `window`,
/// across every partition the manifest declares.
///
/// An EMPTY `topics` means "every topic in the manifest", which is what a
/// restore that names no subset asks for.
#[must_use]
pub fn segment_keys_for_topics(
    v: &Value,
    topics_wanted: &[String],
    window: (i64, i64),
    qualify: &dyn Fn(&str) -> String,
) -> Vec<String> {
    let wanted: BTreeSet<&str> = topics_wanted.iter().map(String::as_str).collect();
    let mut out: Vec<String> = Vec::new();
    for t in topic_entries(v) {
        let Some(name) = t.get("name").and_then(Value::as_str) else {
            continue;
        };
        if !wanted.is_empty() && !wanted.contains(name) {
            continue;
        }
        for p in partition_entries(t) {
            let Some(partition) = p.get("partition_id").and_then(Value::as_i64) else {
                continue;
            };
            out.extend(segment_keys_for(v, name, partition, window, qualify));
        }
    }
    out.sort();
    out.dedup();
    out
}
