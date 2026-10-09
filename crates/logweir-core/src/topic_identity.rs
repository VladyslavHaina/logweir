//! **PROD-01.4a: the topic ID's canonical text, and the generation rule that
//! reads it** (`docs/to-do/decisions/PROD-01.4-topic-identity.md` §2–§4).
//!
//! A Kafka topic that is deleted and created again under the same name is a
//! NEW topic: the broker gives it a new topic ID (KIP-516) and restarts every
//! partition's offsets at zero, so an offset that meant one record before the
//! recreation means a different record after it. From format 1.6.0 a backup
//! receipt records, per named topic, the ID Logweir's own DescribeTopics read
//! returned immediately before the engine (`topic_id`) and immediately after
//! it (`topic_id_after`), or why there is none
//! ([`crate::backup_receipt::TopicIdentity`]). This module holds the pure
//! half of that: the text form, its closed sets, and the rule two points'
//! IDs are read by.
//!
//! # The text form (decision §3.1)
//!
//! 22 characters: URL-safe base64 with no padding over the ID's 16 bytes,
//! most significant half first, big-endian. That is exactly what
//! `kafka-topics.sh --describe` prints. It is derived from the two 64-bit
//! halves ([`topic_id_text`]) and NEVER from librdkafka's
//! `rd_kafka_Uuid_base64str`, which uses the standard alphabet: the same ID
//! reads `Cf6zT/mcTNCoxuPmv1Ztxw` there and `Cf6zT_mcTNCoxuPmv1Ztxw` in Kafka
//! (PROD-01.4 §1.3 C4, measured). Kafka RESERVES two IDs that no topic is
//! ever given (`org.apache.kafka.common.Uuid.RESERVED`, which `randomUuid`
//! never returns): the all-zero ID, Kafka's "no ID" (a broker below
//! inter-broker protocol 2.8 answers with it), and `(0, 1)`,
//! `AAAAAAAAAAAAAAAAAAAAAQ`, Kafka's `ONE_UUID` / `METADATA_TOPIC_ID`
//! (librdkafka's `RD_KAFKA_UUID_METADATA_TOPIC_ID`). Neither is ever written
//! or accepted as an identity: zero becomes `null` with the reason
//! [`NO_TOPIC_ID`], the sentinel `null` with [`RESERVED_TOPIC_ID`], and a
//! recorded one is refused (receipt arm 38) — so two captures can never read
//! as the same generation through a sentinel.
//!
//! # The generation rule (decision §4.2 R1 and §4.4)
//!
//! [`between`] compares a point with the previous point of the same lineage
//! key (source cluster ID, topic name):
//!
//! | the two points' IDs | verdict |
//! |---|---|
//! | both recorded before their captures, and different | [`Generation::New`]: the topic was recreated between them (`TopicIdChanged`), never a continuation |
//! | the current point's own two reads differ | [`Generation::ChangedDuringCapture`]: recreated while the engine ran |
//! | both pre-capture IDs recorded and equal, and the current capture's own two reads recorded and equal | [`Generation::Same`] (basis `topicId`) |
//! | anything else | [`Generation::NotEstablished`], with the reason |
//!
//! `NotEstablished` is TODAY'S FALLBACK, stated and never guessed: a point
//! whose ID is unknown — every receipt before 1.6.0, a broker below 2.8, a
//! refused or failed read — is never the same generation as another point.
//! The offset rule of decision §4 (PROD-02.1) is what will decide those
//! links; until it lands, they are UNKNOWN.
//!
//! What equal IDs do NOT say: that the offsets between the two points are
//! continuous. A same-ID truncation (an unclean leader election) reuses
//! offsets without a new ID (decision §2, FP1), so offset-dependent consumers
//! still need decision §4's offset checks over equal IDs (R2–R6). [`Same`]
//! is "the same topic incarnation", nothing more.
//!
//! [`Same`]: Generation::Same
//!
//! Global Constraint 1: no I/O, no clock.

use crate::backup_receipt::{BackupReceipt, TopicIdentity};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;

/// The length of a topic ID's text form.
pub const TOPIC_ID_TEXT_LEN: usize = 22;

/// `topic_id_source` for an ID Logweir's own DescribeTopics read returned
/// (`logweir-rdkafka-ffi`, OD-6 (a2)).
pub const DESCRIBE_TOPICS: &str = "describeTopics";

/// `topic_id_source` for an ID the engine's manifest carried — the engine
/// route of decision §6.3 (PROD-00.3). No writer in this build produces it;
/// the format defines it so that route needs no version of its own.
pub const ENGINE_MANIFEST: &str = "engineManifest";

/// `topic_id_source`'s closed set (receipt arm 40), in the order the refusal
/// names them.
pub const TOPIC_ID_SOURCES: [&str; 2] = [DESCRIBE_TOPICS, ENGINE_MANIFEST];

/// The broker answered with the all-zero ID: it has none to give (a cluster
/// below inter-broker protocol 2.8).
pub const NO_TOPIC_ID: &str = "noTopicId";

/// The broker refused the read (`TOPIC_AUTHORIZATION_FAILED`): the principal
/// may not Describe the topic. Named, never read as "absent".
pub const NOT_AUTHORIZED: &str = "notAuthorized";

/// The broker does not hold the topic (`UNKNOWN_TOPIC_OR_PARTITION`) — after
/// the engine, a topic deleted while it ran.
pub const TOPIC_NOT_FOUND: &str = "topicNotFound";

/// The read failed for any other reason: no broker answered in time, the
/// answer omitted the topic, or the broker sent another error. Nothing about
/// the topic is known.
pub const READ_FAILED: &str = "readFailed";

/// The reader that took the backup does not read topic IDs at all.
pub const NOT_READ: &str = "notRead";

/// The broker answered with one of Kafka's reserved IDs that no topic is ever
/// given — `(0, 1)`, `AAAAAAAAAAAAAAAAAAAAAQ` (`Uuid.ONE_UUID`,
/// `METADATA_TOPIC_ID`). A sentinel is never an identity.
pub const RESERVED_TOPIC_ID: &str = "reservedTopicId";

/// The text of Kafka's reserved `(0, 1)` ID, which is never a topic's.
pub const RESERVED_ID_TEXT: &str = "AAAAAAAAAAAAAAAAAAAAAQ";

/// `topic_id_reason`'s and `topic_id_after_reason`'s closed set (receipt arm
/// 25), in the order the refusal names them.
pub const TOPIC_ID_REASONS: [&str; 6] = [
    NO_TOPIC_ID,
    NOT_AUTHORIZED,
    TOPIC_NOT_FOUND,
    READ_FAILED,
    NOT_READ,
    RESERVED_TOPIC_ID,
];

/// Whether the two halves are one of Kafka's reserved IDs, which no topic is
/// ever given: zero ("no ID") and `(0, 1)` (`ONE_UUID`, `METADATA_TOPIC_ID`).
#[must_use]
pub const fn is_reserved(most_significant_bits: i64, least_significant_bits: i64) -> bool {
    most_significant_bits == 0 && (least_significant_bits == 0 || least_significant_bits == 1)
}

/// **The canonical text of a topic ID**, from its two halves; `None` for
/// Kafka's reserved IDs ([`is_reserved`]: zero and `(0, 1)`), which are never
/// an identity.
#[must_use]
pub fn topic_id_text(most_significant_bits: i64, least_significant_bits: i64) -> Option<String> {
    if is_reserved(most_significant_bits, least_significant_bits) {
        return None;
    }
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&most_significant_bits.to_be_bytes());
    bytes[8..].copy_from_slice(&least_significant_bits.to_be_bytes());
    Some(URL_SAFE_NO_PAD.encode(bytes))
}

/// The two halves a canonical text names, or `None` when it is not one.
#[must_use]
pub fn topic_id_halves(text: &str) -> Option<(i64, i64)> {
    if text.len() != TOPIC_ID_TEXT_LEN
        || !text
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        return None;
    }
    let bytes = URL_SAFE_NO_PAD.decode(text).ok()?;
    let bytes: [u8; 16] = bytes.try_into().ok()?;
    let mut most = [0u8; 8];
    let mut least = [0u8; 8];
    most.copy_from_slice(&bytes[..8]);
    least.copy_from_slice(&bytes[8..]);
    let halves = (i64::from_be_bytes(most), i64::from_be_bytes(least));
    // Canonical means it re-encodes to ITSELF (no stray trailing bits) and is
    // not one of Kafka's reserved IDs.
    (topic_id_text(halves.0, halves.1).as_deref() == Some(text)).then_some(halves)
}

/// Whether `text` is a topic ID in this format's text form (receipt arm 38):
/// 22 URL-safe base64 characters over 16 bytes that re-encode to themselves,
/// and not one of Kafka's reserved IDs (`AAAAAAAAAAAAAAAAAAAAAA`,
/// `AAAAAAAAAAAAAAAAAAAAAQ`).
#[must_use]
pub fn is_canonical(text: &str) -> bool {
    topic_id_halves(text).is_some()
}

/// **The one check both verifiers make of a catalog point record** (review
/// M1): every topic ID the record copies from its receipt
/// (`topics[].identity.topic_id`, `.topic_id_after`) is a real topic ID in
/// Kafka's text — never one of Kafka's reserved IDs, never another alphabet.
/// A record's copied facts are otherwise checked against the verified receipt
/// it names (D3 §5.2 rule 3), which no verifier fetches; this check needs
/// nothing but the record, and keeps a sentinel from ever reading as an
/// identity in a catalog. `docs/verify_scorecard.py::_catalog_point_problem`
/// returns the same text.
///
/// # Errors
///
/// The refusal, naming the topic, the field and the value.
pub fn refuse_copied_topic_ids(record: &serde_json::Value) -> Result<(), String> {
    let Some(topics) = record.get("topics").and_then(serde_json::Value::as_array) else {
        return Ok(());
    };
    for topic in topics {
        let Some(identity) = topic.get("identity").filter(|i| !i.is_null()) else {
            continue;
        };
        let name = topic
            .get("name")
            .and_then(serde_json::Value::as_str)
            .map_or_else(|| "?".to_string(), |n| format!("{n:?}"));
        for field in ["topic_id", "topic_id_after"] {
            match identity.get(field) {
                None | Some(serde_json::Value::Null) => {}
                Some(serde_json::Value::String(id)) if is_canonical(id) => {}
                Some(serde_json::Value::String(id)) => {
                    return Err(format!(
                        "topics[{name}].identity.{field} {id:?} is not a topic ID this format \
                         defines: 22 characters of URL-safe base64 without padding over the ID's \
                         16 bytes, and never one of Kafka's reserved IDs \
                         (AAAAAAAAAAAAAAAAAAAAAA, AAAAAAAAAAAAAAAAAAAAAQ)"
                    ))
                }
                Some(_) => return Err(format!("topics[{name}].identity.{field} is not a string")),
            }
        }
    }
    Ok(())
}

/// One read of one topic's ID: the canonical text, or why there is none.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdRead {
    /// The ID, in canonical text.
    Id(String),
    /// No ID; the reason is one of [`TOPIC_ID_REASONS`].
    Unread(&'static str),
}

impl IdRead {
    /// What the broker's two halves mean: the ID, [`NO_TOPIC_ID`] for zero,
    /// or [`RESERVED_TOPIC_ID`] for Kafka's reserved `(0, 1)`.
    #[must_use]
    pub fn of_halves(most_significant_bits: i64, least_significant_bits: i64) -> IdRead {
        match topic_id_text(most_significant_bits, least_significant_bits) {
            Some(text) => IdRead::Id(text),
            None if least_significant_bits == 0 => IdRead::Unread(NO_TOPIC_ID),
            None => IdRead::Unread(RESERVED_TOPIC_ID),
        }
    }

    /// The ID, when there is one.
    #[must_use]
    pub fn id(&self) -> Option<&str> {
        match self {
            IdRead::Id(id) => Some(id),
            IdRead::Unread(_) => None,
        }
    }

    /// Why there is no ID, when there is none.
    #[must_use]
    pub fn reason(&self) -> Option<&'static str> {
        match self {
            IdRead::Id(_) => None,
            IdRead::Unread(reason) => Some(reason),
        }
    }
}

/// **The receipt entry for one topic**, from Logweir's DescribeTopics read
/// before the engine and the one after it: each ID or its reason, and the
/// source exactly when an ID was recorded (arms 39 and 40 hold by
/// construction).
#[must_use]
pub fn observed(before: &IdRead, after: &IdRead) -> TopicIdentity {
    TopicIdentity {
        topic_id: before.id().map(str::to_string),
        topic_id_after: after.id().map(str::to_string),
        topic_id_source: (before.id().is_some() || after.id().is_some())
            .then(|| DESCRIBE_TOPICS.to_string()),
        topic_id_reason: before.reason().map(str::to_string),
        topic_id_after_reason: after.reason().map(str::to_string),
    }
}

/// What ONE capture's own two reads say about it (decision §4.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WithinCapture<'a> {
    /// Both reads recorded the same ID: the capture saw one generation.
    Unchanged {
        /// The ID.
        topic_id: &'a str,
    },
    /// Both reads recorded an ID, and they differ: the topic was deleted and
    /// recreated while the engine ran, so the point mixes two generations
    /// (`ChangedDuringCapture`, a break).
    Changed {
        /// The ID before the engine.
        before: &'a str,
        /// The ID after it.
        after: &'a str,
    },
    /// One read or both recorded no ID: whether the topic changed during the
    /// capture is not established by ID.
    NotEstablished,
}

/// An entry's ID as the rule reads it: only a canonical, non-reserved text
/// counts as an ID. A verified receipt never carries another (arm 38); this
/// keeps the rule from reading a sentinel as an identity even when it is
/// handed a document nobody verified.
fn real(id: Option<&str>) -> Option<&str> {
    id.filter(|t| is_canonical(t))
}

/// [`WithinCapture`] of one receipt entry.
#[must_use]
pub fn within_capture(entry: &TopicIdentity) -> WithinCapture<'_> {
    match (
        real(entry.topic_id.as_deref()),
        real(entry.topic_id_after.as_deref()),
    ) {
        (Some(before), Some(after)) if before == after => {
            WithinCapture::Unchanged { topic_id: before }
        }
        (Some(before), Some(after)) => WithinCapture::Changed { before, after },
        _ => WithinCapture::NotEstablished,
    }
}

/// How one point relates to the previous point of the same lineage key, by
/// topic ID (decision §4.2 R1, §4.4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Generation {
    /// Both points recorded the topic's ID before their captures, and the IDs
    /// differ: the topic was deleted and recreated between them. A NEW
    /// generation, never a continuation; offsets of one point mean nothing
    /// in the other (`TopicIdChanged`).
    New {
        /// The previous point's ID.
        previous: String,
        /// This point's ID.
        current: String,
    },
    /// This point's own two reads differ: recreated while the engine ran
    /// (`ChangedDuringCapture`). A break, whatever the previous point says.
    ChangedDuringCapture {
        /// The ID before the engine.
        before: String,
        /// The ID after it.
        after: String,
    },
    /// Both points recorded the same ID before their captures, AND this
    /// capture's own two reads were recorded and are equal (it saw no change):
    /// the same topic incarnation (basis `topicId`). A capture whose read after
    /// the engine recorded no ID is never `Same` — the topic may have been
    /// recreated while it ran (decision §4.7 FN3). Not, by itself, a claim that
    /// the offsets between the two points are continuous (a same-ID truncation
    /// keeps the ID; decision §2, FP1).
    Same {
        /// The ID.
        topic_id: String,
    },
    /// Not established by topic ID: today's fallback, UNKNOWN and never the
    /// same generation as another point.
    NotEstablished(Unestablished),
}

/// Why [`Generation::NotEstablished`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Unestablished {
    /// There is no previous point of this lineage key.
    NoPredecessor,
    /// The previous point is from another source cluster: not a predecessor.
    OtherCluster,
    /// The previous point records no topic IDs for this topic (a receipt
    /// before 1.6.0, or one that did not name the topic).
    PreviousNotRecorded,
    /// The previous point's read before its capture recorded no ID, for this
    /// reason ([`TOPIC_ID_REASONS`], or `absent`).
    PreviousUnread(String),
    /// This point records no topic IDs for this topic.
    CurrentNotRecorded,
    /// This point's read before its capture recorded no ID, for this reason.
    CurrentUnread(String),
    /// This point's read AFTER its capture recorded no ID, for this reason:
    /// whether the topic was recreated while the engine ran is not known
    /// (decision §4.7 FN3), so the link is never `Same`.
    CurrentAfterUnread(String),
}

impl Unestablished {
    /// A stable word for the reason.
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Unestablished::NoPredecessor => "noPredecessor",
            Unestablished::OtherCluster => "otherCluster",
            Unestablished::PreviousNotRecorded => "previousNotRecorded",
            Unestablished::PreviousUnread(_) => "previousUnread",
            Unestablished::CurrentNotRecorded => "currentNotRecorded",
            Unestablished::CurrentUnread(_) => "currentUnread",
            Unestablished::CurrentAfterUnread(_) => "currentAfterUnread",
        }
    }
}

impl std::fmt::Display for Unestablished {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Unestablished::NoPredecessor => write!(f, "there is no previous point"),
            Unestablished::OtherCluster => {
                write!(f, "the previous point is from another source cluster")
            }
            Unestablished::PreviousNotRecorded => {
                write!(f, "the previous point records no topic ID for the topic")
            }
            Unestablished::PreviousUnread(why) => {
                write!(f, "the previous point recorded no topic ID ({why})")
            }
            Unestablished::CurrentNotRecorded => {
                write!(f, "this point records no topic ID for the topic")
            }
            Unestablished::CurrentUnread(why) => {
                write!(f, "this point recorded no topic ID ({why})")
            }
            Unestablished::CurrentAfterUnread(why) => write!(
                f,
                "this point recorded no topic ID after its capture ({why}), so whether the \
                 topic changed while it ran is unknown"
            ),
        }
    }
}

impl std::fmt::Display for Generation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Generation::New { previous, current } => write!(
                f,
                "a NEW generation: the topic ID changed from {previous} to {current} between \
                 the two points (the topic was deleted and recreated)"
            ),
            Generation::ChangedDuringCapture { before, after } => write!(
                f,
                "the topic ID changed DURING this capture ({before} before, {after} after): \
                 the point mixes two generations"
            ),
            Generation::Same { topic_id } => {
                write!(f, "the same generation: topic ID {topic_id} at both points")
            }
            Generation::NotEstablished(why) => write!(
                f,
                "generation NOT ESTABLISHED by topic ID ({why}): unknown, never the same \
                 generation as another point"
            ),
        }
    }
}

/// **R1 and the within-run check over two entries**: `previous` is the
/// previous point's entry for the topic (`None` when it has none), `current`
/// this point's. The caller has established that the two points share the
/// lineage key; [`between`] does that from two receipts.
///
/// Precedence follows the oracle (`e2e/tests/topic_identity.rs::classify`):
/// two different pre-capture IDs decide first, then this capture's own
/// change, then equality.
#[must_use]
pub fn by_topic_id(previous: Option<&TopicIdentity>, current: &TopicIdentity) -> Generation {
    let prev_id = previous.and_then(|p| real(p.topic_id.as_deref()));
    let cur_id = real(current.topic_id.as_deref());
    if let (Some(a), Some(b)) = (prev_id, cur_id) {
        if a != b {
            return Generation::New {
                previous: a.to_string(),
                current: b.to_string(),
            };
        }
    }
    let within = within_capture(current);
    if let WithinCapture::Changed { before, after } = within {
        return Generation::ChangedDuringCapture {
            before: before.to_string(),
            after: after.to_string(),
        };
    }
    // Why a side has no ID: its recorded reason, or — for a document nobody
    // verified — that what it carries is not a topic ID.
    let reason_of = |id: &Option<String>, reason: &Option<String>| match id {
        Some(text) if !is_canonical(text) => format!("{text:?} is not a topic ID"),
        _ => reason.clone().unwrap_or_else(|| "absent".to_string()),
    };
    let Some(previous) = previous else {
        return Generation::NotEstablished(Unestablished::PreviousNotRecorded);
    };
    match (prev_id, cur_id) {
        (None, _) => Generation::NotEstablished(Unestablished::PreviousUnread(reason_of(
            &previous.topic_id,
            &previous.topic_id_reason,
        ))),
        (Some(_), None) => Generation::NotEstablished(Unestablished::CurrentUnread(reason_of(
            &current.topic_id,
            &current.topic_id_reason,
        ))),
        // Equal pre-capture IDs are `Same` only when this capture's own two
        // reads were recorded and agree: an after-read that recorded no ID
        // leaves a recreation DURING the capture open (FN3).
        (Some(a), Some(_)) => match within {
            WithinCapture::Unchanged { .. } => Generation::Same {
                topic_id: a.to_string(),
            },
            WithinCapture::Changed { .. } | WithinCapture::NotEstablished => {
                Generation::NotEstablished(Unestablished::CurrentAfterUnread(reason_of(
                    &current.topic_id_after,
                    &current.topic_id_after_reason,
                )))
            }
        },
    }
}

/// **How `current`'s `topic` relates to the same topic in `previous`**, the
/// newest earlier point of the same source cluster. The receipts must be
/// VERIFIED by the caller: this trusts what it is handed.
#[must_use]
pub fn between(
    previous: Option<&BackupReceipt>,
    current: &BackupReceipt,
    topic: &str,
) -> Generation {
    let Some(entry) = current.generations.as_ref().and_then(|g| g.get(topic)) else {
        return Generation::NotEstablished(Unestablished::CurrentNotRecorded);
    };
    let Some(previous) = previous else {
        // No predecessor: only this capture's own reads can say anything.
        return match within_capture(entry) {
            WithinCapture::Changed { before, after } => Generation::ChangedDuringCapture {
                before: before.to_string(),
                after: after.to_string(),
            },
            _ => Generation::NotEstablished(Unestablished::NoPredecessor),
        };
    };
    if previous.source.cluster_id != current.source.cluster_id {
        return Generation::NotEstablished(Unestablished::OtherCluster);
    }
    by_topic_id(
        previous.generations.as_ref().and_then(|g| g.get(topic)),
        entry,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The four IDs PROD-01.4 measured (`artifacts/prod-01-4/ffi-route-evidence.txt`):
    /// the broker CLI's text and librdkafka's two halves for the same topic.
    const MEASURED: [(&str, i64, i64); 4] = [
        (
            "gtOq2VXiTCK1QM2UtERijA",
            -9_019_677_778_267_452_382,
            -5_386_079_115_771_878_772,
        ),
        (
            "tpWwuKExQo2lN9NziDMpYg",
            -5_290_127_880_251_948_403,
            -6_541_527_440_572_602_014,
        ),
        (
            "NSSUDfCtRqWqyhqP5Vttsw",
            3_829_348_370_765_137_573,
            -6_140_065_936_635_630_157,
        ),
        (
            "Cf6zT_mcTNCoxuPmv1Ztxw",
            720_210_146_497_416_400,
            -6_285_085_649_756_852_793,
        ),
    ];

    #[test]
    fn the_text_is_the_brokers_from_the_two_halves() {
        for (cli, most, least) in MEASURED {
            assert_eq!(topic_id_text(most, least).as_deref(), Some(cli));
            assert_eq!(topic_id_halves(cli), Some((most, least)));
            assert!(is_canonical(cli));
        }
        // C4: librdkafka's own helper printed the standard alphabet for the
        // fourth; that text is NOT a topic ID in this format.
        assert!(!is_canonical("Cf6zT/mcTNCoxuPmv1Ztxw"));
    }

    #[test]
    fn kafkas_reserved_ids_are_never_text_and_never_an_identity() {
        // Zero, Kafka's "no ID".
        assert_eq!(topic_id_text(0, 0), None);
        assert!(!is_canonical("AAAAAAAAAAAAAAAAAAAAAA"));
        assert_eq!(IdRead::of_halves(0, 0), IdRead::Unread(NO_TOPIC_ID));
        // (0, 1), Kafka's ONE_UUID / METADATA_TOPIC_ID (review M1): never a
        // topic's, so never text, never canonical, and its own reason.
        assert!(is_reserved(0, 1));
        assert_eq!(topic_id_text(0, 1), None);
        assert!(!is_canonical(RESERVED_ID_TEXT));
        assert_eq!(IdRead::of_halves(0, 1), IdRead::Unread(RESERVED_TOPIC_ID));
        assert_eq!(
            URL_SAFE_NO_PAD.encode([0u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]),
            RESERVED_ID_TEXT
        );
        // Their neighbours are IDs.
        assert!(!is_reserved(1, 0) && !is_reserved(0, 2) && !is_reserved(0, -1));
        assert!(topic_id_text(1, 0).is_some());
        assert!(topic_id_text(0, 2).is_some());
        assert!(is_canonical(&topic_id_text(0, 2).unwrap()));
    }

    /// Review M1's failure, closed: a recreated topic can never read as the
    /// same generation through the sentinel. Two points whose reads carried
    /// it record no ID (`reservedTopicId`) and are not established; even
    /// handed unverified entries carrying the text, the rule does not read it
    /// as an ID. The control: two real equal IDs are `Same`.
    #[test]
    fn a_sentinel_id_never_makes_two_points_the_same_generation() {
        let sentinel_read = IdRead::of_halves(0, 1);
        let point = observed(&sentinel_read, &sentinel_read);
        assert_eq!(point.topic_id, None);
        assert_eq!(point.topic_id_reason.as_deref(), Some(RESERVED_TOPIC_ID));
        assert_eq!(point.topic_id_source, None);
        assert!(matches!(
            by_topic_id(Some(&point), &point),
            Generation::NotEstablished(Unestablished::PreviousUnread(r)) if r == RESERVED_TOPIC_ID
        ));
        let forged = TopicIdentity {
            topic_id: Some(RESERVED_ID_TEXT.into()),
            topic_id_after: Some(RESERVED_ID_TEXT.into()),
            topic_id_source: Some(DESCRIBE_TOPICS.into()),
            topic_id_reason: None,
            topic_id_after_reason: None,
        };
        assert_eq!(within_capture(&forged), WithinCapture::NotEstablished);
        assert!(matches!(
            by_topic_id(Some(&forged), &forged),
            Generation::NotEstablished(Unestablished::PreviousUnread(r))
                if r.contains("is not a topic ID")
        ));
        // The control.
        assert_eq!(
            by_topic_id(Some(&entry(Some(A), Some(A))), &entry(Some(A), Some(A))),
            Generation::Same { topic_id: A.into() }
        );
    }

    #[test]
    fn only_the_canonical_text_is_canonical() {
        let good = "gtOq2VXiTCK1QM2UtERijA";
        for bad in [
            "",
            "gtOq2VXiTCK1QM2UtERij",    // 21
            "gtOq2VXiTCK1QM2UtERijAA",  // 23
            "gtOq2VXiTCK1QM2UtERijA==", // padded
            "gtOq2VXiTCK1QM2UtERij+",   // standard alphabet
            "gtOq2VXiTCK1QM2UtERij.",   // not base64
            "gtOq2VXiTCK1QM2UtERijB",   // stray trailing bits: re-encodes as ...ijA
            " tOq2VXiTCK1QM2UtERijA",   // whitespace
            "gtOq2VXiTCK1QM2UtERijÀ",   // non-ASCII
        ] {
            assert!(!is_canonical(bad), "{bad:?}");
        }
        assert!(is_canonical(good));
    }

    #[test]
    fn an_id_with_a_dash_or_an_underscore_keeps_it() {
        // Bytes chosen so the text carries both URL-safe-only characters.
        let most = i64::from_be_bytes([0xfb, 0xff, 0xbf, 0xfb, 0xff, 0xbf, 0xfb, 0xff]);
        let least = i64::from_be_bytes([0xbf, 0xfb, 0xff, 0xbf, 0xfb, 0xff, 0xbf, 0x00]);
        let text = topic_id_text(most, least).expect("not zero");
        assert!(text.contains('-') && text.contains('_'), "{text}");
        assert!(is_canonical(&text));
        assert_eq!(topic_id_halves(&text), Some((most, least)));
    }

    fn entry(before: Option<&str>, after: Option<&str>) -> TopicIdentity {
        let read = |id: Option<&str>| match id {
            Some(id) => IdRead::Id(id.to_string()),
            None => IdRead::Unread(NO_TOPIC_ID),
        };
        observed(&read(before), &read(after))
    }

    #[test]
    fn the_observation_records_each_id_or_its_reason_and_the_source_with_an_id() {
        let e = observed(
            &IdRead::Id("gtOq2VXiTCK1QM2UtERijA".into()),
            &IdRead::Unread(TOPIC_NOT_FOUND),
        );
        assert_eq!(e.topic_id.as_deref(), Some("gtOq2VXiTCK1QM2UtERijA"));
        assert_eq!(e.topic_id_after, None);
        assert_eq!(e.topic_id_source.as_deref(), Some(DESCRIBE_TOPICS));
        assert_eq!(e.topic_id_reason, None);
        assert_eq!(e.topic_id_after_reason.as_deref(), Some(TOPIC_NOT_FOUND));
        let none = observed(
            &IdRead::Unread(NOT_AUTHORIZED),
            &IdRead::Unread(READ_FAILED),
        );
        assert_eq!(none.topic_id_source, None);
        assert_eq!(none.topic_id_reason.as_deref(), Some(NOT_AUTHORIZED));
        assert_eq!(none.topic_id_after_reason.as_deref(), Some(READ_FAILED));
    }

    const A: &str = "gtOq2VXiTCK1QM2UtERijA";
    const B: &str = "tpWwuKExQo2lN9NziDMpYg";

    #[test]
    fn a_recreated_topic_is_a_new_generation_and_never_a_continuation() {
        assert_eq!(
            by_topic_id(Some(&entry(Some(A), Some(A))), &entry(Some(B), Some(B))),
            Generation::New {
                previous: A.into(),
                current: B.into()
            }
        );
        // Even when this capture also changed: the earlier break wins, and it
        // is still never `Same`.
        assert!(matches!(
            by_topic_id(Some(&entry(Some(A), Some(A))), &entry(Some(B), Some(A))),
            Generation::New { .. }
        ));
    }

    #[test]
    fn the_same_topic_is_the_same_generation() {
        assert_eq!(
            by_topic_id(Some(&entry(Some(A), Some(A))), &entry(Some(A), Some(A))),
            Generation::Same { topic_id: A.into() }
        );
        // The previous point's AFTER read does not decide R1; its BEFORE read
        // does (the oracle's `classify`).
        assert_eq!(
            by_topic_id(Some(&entry(Some(A), None)), &entry(Some(A), Some(A))),
            Generation::Same { topic_id: A.into() }
        );
    }

    /// Review M2: equal pre-capture IDs are `Same` only when this capture's
    /// read AFTER the engine recorded the same ID. Whatever the after-read's
    /// reason, the link is not established (the topic may have been recreated
    /// while the engine ran, decision §4.7 FN3) — never `Same`.
    #[test]
    fn an_after_read_that_recorded_no_id_is_never_the_same_generation() {
        for reason in TOPIC_ID_REASONS {
            let current = observed(&IdRead::Id(A.into()), &IdRead::Unread(reason));
            assert_eq!(
                by_topic_id(Some(&entry(Some(A), Some(A))), &current),
                Generation::NotEstablished(Unestablished::CurrentAfterUnread(reason.into())),
                "{reason}"
            );
        }
        // The control: the same pre-capture ID, both reads recorded.
        assert_eq!(
            by_topic_id(Some(&entry(Some(A), Some(A))), &entry(Some(A), Some(A))),
            Generation::Same { topic_id: A.into() }
        );
        assert_eq!(
            Unestablished::CurrentAfterUnread("readFailed".into()).code(),
            "currentAfterUnread"
        );
    }

    /// Review M1, the catalog half: a record copying a reserved or
    /// other-alphabet ID is refused; real IDs, nulls and records without the
    /// block pass.
    #[test]
    fn a_catalog_record_copying_a_reserved_id_is_refused() {
        let record = |id: serde_json::Value| {
            serde_json::json!({"topics": [
                {"name": "orders", "identity": {"topic_id": A, "topic_id_after": id}},
            ]})
        };
        let tail = " is not a topic ID this format defines: 22 characters of URL-safe base64 \
                    without padding over the ID's 16 bytes, and never one of Kafka's reserved \
                    IDs (AAAAAAAAAAAAAAAAAAAAAA, AAAAAAAAAAAAAAAAAAAAAQ)";
        for bad in [
            RESERVED_ID_TEXT,
            "AAAAAAAAAAAAAAAAAAAAAA",
            "Cf6zT/mcTNCoxuPmv1Ztxw",
        ] {
            assert_eq!(
                refuse_copied_topic_ids(&record(serde_json::json!(bad))),
                Err(format!(
                    "topics[\"orders\"].identity.topic_id_after \"{bad}\"{tail}"
                ))
            );
        }
        assert_eq!(
            refuse_copied_topic_ids(&record(serde_json::json!(7))),
            Err("topics[\"orders\"].identity.topic_id_after is not a string".to_string())
        );
        for ok in [serde_json::json!(B), serde_json::Value::Null] {
            assert_eq!(refuse_copied_topic_ids(&record(ok)), Ok(()));
        }
        assert_eq!(
            refuse_copied_topic_ids(&serde_json::json!({"topics": [{"name": "orders"}]})),
            Ok(())
        );
        assert_eq!(refuse_copied_topic_ids(&serde_json::json!({})), Ok(()));
    }

    /// Review L2: a previous point that itself changed during its capture
    /// (A before, B after) is followed by a point that reads B: R1 compares the
    /// PRE-capture IDs, A and B, so the link is a new generation — never `Same`
    /// through the previous point's after-read.
    #[test]
    fn a_previous_point_that_changed_during_its_capture_is_a_new_generation_after_it() {
        assert_eq!(
            by_topic_id(Some(&entry(Some(A), Some(B))), &entry(Some(B), Some(B))),
            Generation::New {
                previous: A.into(),
                current: B.into()
            }
        );
    }

    #[test]
    fn a_change_during_the_capture_is_a_break_whatever_came_before() {
        assert_eq!(
            by_topic_id(Some(&entry(Some(A), Some(A))), &entry(Some(A), Some(B))),
            Generation::ChangedDuringCapture {
                before: A.into(),
                after: B.into()
            }
        );
        assert_eq!(
            by_topic_id(None, &entry(Some(A), Some(B))),
            Generation::ChangedDuringCapture {
                before: A.into(),
                after: B.into()
            }
        );
        assert_eq!(
            within_capture(&entry(Some(A), Some(B))),
            WithinCapture::Changed {
                before: A,
                after: B
            }
        );
        assert_eq!(
            within_capture(&entry(Some(A), Some(A))),
            WithinCapture::Unchanged { topic_id: A }
        );
        assert_eq!(
            within_capture(&entry(Some(A), None)),
            WithinCapture::NotEstablished
        );
    }

    /// The fallback: an unknown ID on either side is NEVER `Same`, and says
    /// which side and why.
    #[test]
    fn an_absent_id_is_not_established_and_never_the_same_generation() {
        let unread = |reason| TopicIdentity {
            topic_id: None,
            topic_id_after: None,
            topic_id_source: None,
            topic_id_reason: Some(String::from(reason)),
            topic_id_after_reason: Some(String::from(reason)),
        };
        assert_eq!(
            by_topic_id(Some(&unread(NO_TOPIC_ID)), &entry(Some(A), Some(A))),
            Generation::NotEstablished(Unestablished::PreviousUnread(NO_TOPIC_ID.into()))
        );
        assert_eq!(
            by_topic_id(Some(&entry(Some(A), Some(A))), &unread(NOT_AUTHORIZED)),
            Generation::NotEstablished(Unestablished::CurrentUnread(NOT_AUTHORIZED.into()))
        );
        assert_eq!(
            by_topic_id(None, &entry(Some(A), Some(A))),
            Generation::NotEstablished(Unestablished::PreviousNotRecorded)
        );
        // Both unknown, even with the same reason: unknown, not "same".
        assert!(matches!(
            by_topic_id(Some(&unread(NO_TOPIC_ID)), &unread(NO_TOPIC_ID)),
            Generation::NotEstablished(_)
        ));
    }

    #[test]
    fn the_sentences_name_the_verdict() {
        let s = Generation::New {
            previous: A.into(),
            current: B.into(),
        }
        .to_string();
        assert!(s.starts_with("a NEW generation"), "{s}");
        let s = Generation::NotEstablished(Unestablished::CurrentUnread(NO_TOPIC_ID.into()))
            .to_string();
        assert!(s.contains("never the same generation"), "{s}");
        assert!(s.contains("(noTopicId)"), "{s}");
        assert_eq!(Unestablished::OtherCluster.code(), "otherCluster");
    }
}
