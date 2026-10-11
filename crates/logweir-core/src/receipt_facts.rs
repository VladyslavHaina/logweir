//! **FX-33 — the facts a reader takes from a backup receipt WITHOUT building
//! a tree of it.**
//!
//! The shared controller verifies a receipt of up to
//! [`crate::topic_budget::MAX_RECEIPT_BYTES`] and then needs five things from
//! it: the `backup_id` it names, the capture's two instants (`started_at`,
//! `finished_at` — the second is also the signing time the trust check
//! reads), the `covered` window, and the sum of `records`. It used to parse
//! the whole document into a `serde_json::Value`, which costs up to 37 times
//! the document's size; at the receipt bound that is 190 MB in a process that
//! serves every namespace. [`ReceiptFacts::fold`] reads the same five things
//! as the bytes stream past and keeps nothing per topic.
//!
//! # It answers exactly what the `Value` walk answered
//!
//! The functions this replaces read a `Value`; their rules are kept bit for
//! bit, because a verdict hangs on them:
//!
//! - **Bytes that are not JSON are not a document**: [`ReceiptFacts::fold`]
//!   is `None` exactly when `serde_json::from_slice::<Value>` fails — invalid
//!   UTF-8 or a lone surrogate in ANY string, a number out of range, nesting
//!   past the parser's depth, trailing bytes. Every value the fold does not
//!   need goes through `deserialize_any`, never `serde::de::IgnoredAny`,
//!   whose skip path checks none of those.
//! - **A top level that is not an object has no field.**
//! - **A repeated key: the LAST occurrence wins**, at every level, as in a
//!   `serde_json::Map`.
//! - **`covered`** is both `from_ms` and `to_ms` as `i64`, or nothing.
//! - **`records`** is the sum of every value as `u64`, each fitting `i64`,
//!   the total fitting `i64`, or nothing.
//!
//! One stated difference, on the safer side: a `records` object naming more
//! than [`MAX_RECORDS_ENTRIES`] distinct topics yields no sum (the status
//! column stays blank, which says "not read", never a number). The `Value`
//! walk had no such bound and held every key; no receipt this product can
//! write under its read cap comes near it.
//!
//! `crates/logweir-core/tests/receipt_facts.rs` holds the fold to the `Value`
//! walk over a corpus of bodies, and measures that it keeps nothing.
//!
//! Pure: no I/O, no clock (Global Constraint 1).

use chrono::{DateTime, Utc};
use serde::de::{DeserializeSeed, Deserializer, MapAccess, SeqAccess, Visitor};
use std::collections::BTreeMap;
use std::fmt;

use crate::trust::ClaimAbsence;

/// The most distinct `records` keys the fold sums. One past it and the sum is
/// `None`: the fold holds the keys it has seen (a repeated key must replace,
/// not add), and this is what bounds that.
pub const MAX_RECORDS_ENTRIES: usize = 16_384;

/// One top-level field, as its LAST occurrence read.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum Field {
    /// The document carries no such key (or is not an object).
    #[default]
    Absent,
    /// The key's value is this string.
    Text(String),
    /// The key is present and its value is not a string.
    Other,
}

impl Field {
    /// The string, when the field is one — `Value::as_str` of `get(key)`.
    #[must_use]
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::Text(text) => Some(text),
            Self::Absent | Self::Other => None,
        }
    }
}

/// What [`ReceiptFacts::fold`] read.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReceiptFacts {
    /// `backup_id`.
    pub backup_id: Field,
    /// `started_at`.
    pub started_at: Field,
    /// `finished_at` — the receipt's claimed signing time.
    pub finished_at: Field,
    /// `covered{from_ms, to_ms}`, both or neither.
    pub covered: Option<(i64, i64)>,
    /// The sum of `records`, or `None` when the block is absent, is not an
    /// object, holds a value that is not a `u64` fitting `i64`, sums past
    /// `i64`, or names more than [`MAX_RECORDS_ENTRIES`] topics.
    pub records: Option<i64>,
}

impl ReceiptFacts {
    /// Read the facts out of `bytes`. `None` when the bytes are not JSON.
    #[must_use]
    pub fn fold(bytes: &[u8]) -> Option<Self> {
        let mut de = serde_json::Deserializer::from_slice(bytes);
        let facts = de.deserialize_any(TopVisitor).ok()?;
        de.end().ok()?;
        Some(facts)
    }

    /// `status.capture`: both instants, or neither — the rule
    /// `capture_from_receipt` states for a half-read window.
    #[must_use]
    pub fn capture(&self) -> Option<(DateTime<Utc>, DateTime<Utc>)> {
        let at = |field: &Field| field.as_str()?.parse::<DateTime<Utc>>().ok();
        Some((at(&self.started_at)?, at(&self.finished_at)?))
    }

    /// The receipt's claimed signing time, with its absence named —
    /// [`crate::trust::read_claimed_signing_time`] for a backup receipt.
    ///
    /// # Errors
    ///
    /// [`ClaimAbsence::FieldAbsent`] for a document with no `finished_at`,
    /// [`ClaimAbsence::Unparseable`] for one whose value is not an RFC 3339
    /// string.
    pub fn signing_time(&self) -> Result<DateTime<Utc>, ClaimAbsence> {
        match &self.finished_at {
            Field::Absent => Err(ClaimAbsence::FieldAbsent),
            Field::Other => Err(ClaimAbsence::Unparseable),
            Field::Text(text) => DateTime::parse_from_rfc3339(text)
                .map(|t| t.with_timezone(&Utc))
                .map_err(|_| ClaimAbsence::Unparseable),
        }
    }
}

// ---------------------------------------------------------------------------
// The visitors. Each consumes its value and keeps only what a fact needs.
// ---------------------------------------------------------------------------

/// A value the fold does not need, consumed, VALIDATED as a `Value` parse
/// validates it, and dropped. See the module doc for why this is not
/// `serde::de::IgnoredAny`.
struct Skip;

impl<'de> DeserializeSeed<'de> for Skip {
    type Value = ();
    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<(), D::Error> {
        d.deserialize_any(SkipVisitor)
    }
}

struct SkipVisitor;

impl<'de> Visitor<'de> for SkipVisitor {
    type Value = ();
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("any JSON value")
    }
    fn visit_bool<E>(self, _: bool) -> Result<(), E> {
        Ok(())
    }
    fn visit_i64<E>(self, _: i64) -> Result<(), E> {
        Ok(())
    }
    fn visit_u64<E>(self, _: u64) -> Result<(), E> {
        Ok(())
    }
    fn visit_f64<E>(self, _: f64) -> Result<(), E> {
        Ok(())
    }
    fn visit_str<E>(self, _: &str) -> Result<(), E> {
        Ok(())
    }
    fn visit_unit<E>(self) -> Result<(), E> {
        Ok(())
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<(), A::Error> {
        while seq.next_element_seed(Skip)?.is_some() {}
        Ok(())
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<(), A::Error> {
        while map.next_key_seed(Skip)?.is_some() {
            map.next_value_seed(Skip)?;
        }
        Ok(())
    }
}

/// A map key, kept: the top level and `covered` compare it, `records` holds
/// it so a repeated topic replaces its earlier count.
struct KeySeed;

impl<'de> DeserializeSeed<'de> for KeySeed {
    type Value = String;
    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<String, D::Error> {
        d.deserialize_str(KeyVisitor)
    }
}

struct KeyVisitor;

impl Visitor<'_> for KeyVisitor {
    type Value = String;
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a key")
    }
    fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<String, E> {
        Ok(v.to_string())
    }
}

/// The top level: an object's fields, or nothing for any other value.
struct TopVisitor;

impl<'de> Visitor<'de> for TopVisitor {
    type Value = ReceiptFacts;
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a JSON document")
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<ReceiptFacts, A::Error> {
        let mut facts = ReceiptFacts::default();
        while let Some(key) = map.next_key_seed(KeySeed)? {
            match key.as_str() {
                "backup_id" => facts.backup_id = map.next_value_seed(FieldSeed)?,
                "started_at" => facts.started_at = map.next_value_seed(FieldSeed)?,
                "finished_at" => facts.finished_at = map.next_value_seed(FieldSeed)?,
                "covered" => facts.covered = map.next_value_seed(CoveredSeed)?,
                "records" => facts.records = map.next_value_seed(RecordsSeed)?,
                _ => map.next_value_seed(Skip)?,
            }
        }
        Ok(facts)
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<ReceiptFacts, A::Error> {
        while seq.next_element_seed(Skip)?.is_some() {}
        Ok(ReceiptFacts::default())
    }
    fn visit_bool<E>(self, _: bool) -> Result<ReceiptFacts, E> {
        Ok(ReceiptFacts::default())
    }
    fn visit_i64<E>(self, _: i64) -> Result<ReceiptFacts, E> {
        Ok(ReceiptFacts::default())
    }
    fn visit_u64<E>(self, _: u64) -> Result<ReceiptFacts, E> {
        Ok(ReceiptFacts::default())
    }
    fn visit_f64<E>(self, _: f64) -> Result<ReceiptFacts, E> {
        Ok(ReceiptFacts::default())
    }
    fn visit_str<E>(self, _: &str) -> Result<ReceiptFacts, E> {
        Ok(ReceiptFacts::default())
    }
    fn visit_unit<E>(self) -> Result<ReceiptFacts, E> {
        Ok(ReceiptFacts::default())
    }
}

/// One top-level field: its string, or that it is something else.
struct FieldSeed;

impl<'de> DeserializeSeed<'de> for FieldSeed {
    type Value = Field;
    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<Field, D::Error> {
        d.deserialize_any(FieldVisitor)
    }
}

struct FieldVisitor;

impl<'de> Visitor<'de> for FieldVisitor {
    type Value = Field;
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("any JSON value")
    }
    fn visit_str<E>(self, v: &str) -> Result<Field, E> {
        Ok(Field::Text(v.to_string()))
    }
    fn visit_bool<E>(self, _: bool) -> Result<Field, E> {
        Ok(Field::Other)
    }
    fn visit_i64<E>(self, _: i64) -> Result<Field, E> {
        Ok(Field::Other)
    }
    fn visit_u64<E>(self, _: u64) -> Result<Field, E> {
        Ok(Field::Other)
    }
    fn visit_f64<E>(self, _: f64) -> Result<Field, E> {
        Ok(Field::Other)
    }
    fn visit_unit<E>(self) -> Result<Field, E> {
        Ok(Field::Other)
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Field, A::Error> {
        while seq.next_element_seed(Skip)?.is_some() {}
        Ok(Field::Other)
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Field, A::Error> {
        while map.next_key_seed(Skip)?.is_some() {
            map.next_value_seed(Skip)?;
        }
        Ok(Field::Other)
    }
}

/// A number as an `i128`, or `None` for a float and for every value that is
/// not a number. One visitor serves both readings below.
struct RawNumber;

impl<'de> DeserializeSeed<'de> for RawNumber {
    type Value = Option<i128>;
    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<Option<i128>, D::Error> {
        d.deserialize_any(NumberVisitor)
    }
}

/// `Value::as_i64`: an integer that fits `i64`.
fn as_i64(n: Option<i128>) -> Option<i64> {
    n.and_then(|n| i64::try_from(n).ok())
}

/// `Value::as_u64`: a non-negative integer.
fn as_u64(n: Option<i128>) -> Option<u64> {
    n.and_then(|n| u64::try_from(n).ok())
}

struct NumberVisitor;

impl<'de> Visitor<'de> for NumberVisitor {
    type Value = Option<i128>;
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("any JSON value")
    }
    fn visit_i64<E>(self, v: i64) -> Result<Option<i128>, E> {
        Ok(Some(i128::from(v)))
    }
    fn visit_u64<E>(self, v: u64) -> Result<Option<i128>, E> {
        Ok(Some(i128::from(v)))
    }
    fn visit_f64<E>(self, _: f64) -> Result<Option<i128>, E> {
        Ok(None)
    }
    fn visit_bool<E>(self, _: bool) -> Result<Option<i128>, E> {
        Ok(None)
    }
    fn visit_str<E>(self, _: &str) -> Result<Option<i128>, E> {
        Ok(None)
    }
    fn visit_unit<E>(self) -> Result<Option<i128>, E> {
        Ok(None)
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Option<i128>, A::Error> {
        while seq.next_element_seed(Skip)?.is_some() {}
        Ok(None)
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Option<i128>, A::Error> {
        while map.next_key_seed(Skip)?.is_some() {
            map.next_value_seed(Skip)?;
        }
        Ok(None)
    }
}

/// `covered`: an object's last `from_ms` and last `to_ms`, both `i64`.
struct CoveredSeed;

impl<'de> DeserializeSeed<'de> for CoveredSeed {
    type Value = Option<(i64, i64)>;
    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
        d.deserialize_any(CoveredVisitor)
    }
}

struct CoveredVisitor;

impl<'de> Visitor<'de> for CoveredVisitor {
    type Value = Option<(i64, i64)>;
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("any JSON value")
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        // `Some(None)` is a key that is present and is not an `i64`: the
        // last occurrence decides, so a later non-number unsets the window.
        let mut from: Option<Option<i64>> = None;
        let mut to: Option<Option<i64>> = None;
        while let Some(key) = map.next_key_seed(KeySeed)? {
            match key.as_str() {
                "from_ms" => from = Some(as_i64(map.next_value_seed(RawNumber)?)),
                "to_ms" => to = Some(as_i64(map.next_value_seed(RawNumber)?)),
                _ => map.next_value_seed(Skip)?,
            }
        }
        Ok(match (from.flatten(), to.flatten()) {
            (Some(from), Some(to)) => Some((from, to)),
            _ => None,
        })
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        while seq.next_element_seed(Skip)?.is_some() {}
        Ok(None)
    }
    fn visit_bool<E>(self, _: bool) -> Result<Self::Value, E> {
        Ok(None)
    }
    fn visit_i64<E>(self, _: i64) -> Result<Self::Value, E> {
        Ok(None)
    }
    fn visit_u64<E>(self, _: u64) -> Result<Self::Value, E> {
        Ok(None)
    }
    fn visit_f64<E>(self, _: f64) -> Result<Self::Value, E> {
        Ok(None)
    }
    fn visit_str<E>(self, _: &str) -> Result<Self::Value, E> {
        Ok(None)
    }
    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(None)
    }
}

/// `records`: the sum of an object's values. See [`ReceiptFacts::records`].
struct RecordsSeed;

impl<'de> DeserializeSeed<'de> for RecordsSeed {
    type Value = Option<i64>;
    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<Option<i64>, D::Error> {
        d.deserialize_any(RecordsVisitor)
    }
}

struct RecordsVisitor;

impl<'de> Visitor<'de> for RecordsVisitor {
    type Value = Option<i64>;
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("any JSON value")
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Option<i64>, A::Error> {
        // Keyed, because a repeated topic REPLACES its earlier count (a
        // `serde_json::Map` keeps the last), and bounded: one key past
        // `MAX_RECORDS_ENTRIES` and there is no sum.
        let mut counts: BTreeMap<String, Option<u64>> = BTreeMap::new();
        let mut over = false;
        while let Some(key) = map.next_key_seed(KeySeed)? {
            let value = as_u64(map.next_value_seed(RawNumber)?);
            if over {
                continue;
            }
            if counts.len() >= MAX_RECORDS_ENTRIES && !counts.contains_key(&key) {
                over = true;
                counts.clear();
                continue;
            }
            counts.insert(key, value);
        }
        if over {
            return Ok(None);
        }
        let mut total: i64 = 0;
        for value in counts.values() {
            let Some(count) = value.and_then(|v| i64::try_from(v).ok()) else {
                return Ok(None);
            };
            let Some(sum) = total.checked_add(count) else {
                return Ok(None);
            };
            total = sum;
        }
        Ok(Some(total))
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Option<i64>, A::Error> {
        while seq.next_element_seed(Skip)?.is_some() {}
        Ok(None)
    }
    fn visit_bool<E>(self, _: bool) -> Result<Option<i64>, E> {
        Ok(None)
    }
    fn visit_i64<E>(self, _: i64) -> Result<Option<i64>, E> {
        Ok(None)
    }
    fn visit_u64<E>(self, _: u64) -> Result<Option<i64>, E> {
        Ok(None)
    }
    fn visit_f64<E>(self, _: f64) -> Result<Option<i64>, E> {
        Ok(None)
    }
    fn visit_str<E>(self, _: &str) -> Result<Option<i64>, E> {
        Ok(None)
    }
    fn visit_unit<E>(self) -> Result<Option<i64>, E> {
        Ok(None)
    }
}
