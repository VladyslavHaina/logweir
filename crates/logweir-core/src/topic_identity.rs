//! **PROD-01.4a: the topic ID's canonical text, and the generation rule that
//! reads it** (`docs/to-do/decisions/PROD-01.4-topic-identity.md` §2–§4).
//!
//! A Kafka topic that is deleted and created again under the same name is a
//! NEW topic: the broker gives it a new topic ID (KIP-516) and restarts every
//! partition's offsets at zero, so an offset that meant one record before the
//! recreation means a different record after it. From format 1.5.0 a backup
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
//! (PROD-01.4 §1.3 C4, measured). The all-zero ID is Kafka's "no ID" (a broker
//! below inter-broker protocol 2.8 answers with it) and is never written: it
//! becomes `null` with the reason [`NO_TOPIC_ID`].
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
//! | both recorded and equal, and the current capture saw no change | [`Generation::Same`] (basis `topicId`) |
//! | anything else | [`Generation::NotEstablished`], with the reason |
//!
//! `NotEstablished` is TODAY'S FALLBACK, stated and never guessed: a point
//! whose ID is unknown — every receipt before 1.5.0, a broker below 2.8, a
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

/// `topic_id_source`'s closed set (receipt arm 26), in the order the refusal
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

/// `topic_id_reason`'s and `topic_id_after_reason`'s closed set (receipt arm
/// 25), in the order the refusal names them.
pub const TOPIC_ID_REASONS: [&str; 5] = [
    NO_TOPIC_ID,
    NOT_AUTHORIZED,
    TOPIC_NOT_FOUND,
    READ_FAILED,
    NOT_READ,
];

/// **The canonical text of a topic ID**, from its two halves; `None` for the
/// all-zero ID, which is Kafka's "no ID" and never an identity.
#[must_use]
pub fn topic_id_text(most_significant_bits: i64, least_significant_bits: i64) -> Option<String> {
    if most_significant_bits == 0 && least_significant_bits == 0 {
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
    // not the all-zero "no ID".
    (topic_id_text(halves.0, halves.1).as_deref() == Some(text)).then_some(halves)
}

/// Whether `text` is a topic ID in this format's text form (receipt arm 24):
/// 22 URL-safe base64 characters over 16 bytes that re-encode to themselves,
/// and not the all-zero ID.
#[must_use]
pub fn is_canonical(text: &str) -> bool {
    topic_id_halves(text).is_some()
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
    /// What the broker's two halves mean: the ID, or [`NO_TOPIC_ID`] for zero.
    #[must_use]
    pub fn of_halves(most_significant_bits: i64, least_significant_bits: i64) -> IdRead {
        topic_id_text(most_significant_bits, least_significant_bits)
            .map_or(IdRead::Unread(NO_TOPIC_ID), IdRead::Id)
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
/// source exactly when an ID was recorded (arms 25 and 26 hold by
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

/// [`WithinCapture`] of one receipt entry.
#[must_use]
pub fn within_capture(entry: &TopicIdentity) -> WithinCapture<'_> {
    match (entry.topic_id.as_deref(), entry.topic_id_after.as_deref()) {
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
    /// Both points recorded the same ID and this capture saw no change: the
    /// same topic incarnation (basis `topicId`). Not, by itself, a claim that
    /// the offsets between them are continuous (a same-ID truncation keeps
    /// the ID; decision §2, FP1).
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
    /// before 1.5.0, or one that did not name the topic).
    PreviousNotRecorded,
    /// The previous point's read before its capture recorded no ID, for this
    /// reason ([`TOPIC_ID_REASONS`], or `absent`).
    PreviousUnread(String),
    /// This point records no topic IDs for this topic.
    CurrentNotRecorded,
    /// This point's read before its capture recorded no ID, for this reason.
    CurrentUnread(String),
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
    let prev_id = previous.and_then(|p| p.topic_id.as_deref());
    if let (Some(a), Some(b)) = (prev_id, current.topic_id.as_deref()) {
        if a != b {
            return Generation::New {
                previous: a.to_string(),
                current: b.to_string(),
            };
        }
    }
    match within_capture(current) {
        WithinCapture::Changed { before, after } => {
            return Generation::ChangedDuringCapture {
                before: before.to_string(),
                after: after.to_string(),
            }
        }
        WithinCapture::Unchanged { .. } | WithinCapture::NotEstablished => {}
    }
    let reason_of = |r: &Option<String>| r.clone().unwrap_or_else(|| "absent".to_string());
    let Some(previous) = previous else {
        return Generation::NotEstablished(Unestablished::PreviousNotRecorded);
    };
    match (previous.topic_id.as_deref(), current.topic_id.as_deref()) {
        (Some(a), Some(_)) => Generation::Same {
            topic_id: a.to_string(),
        },
        (None, _) => Generation::NotEstablished(Unestablished::PreviousUnread(reason_of(
            &previous.topic_id_reason,
        ))),
        (Some(_), None) => Generation::NotEstablished(Unestablished::CurrentUnread(reason_of(
            &current.topic_id_reason,
        ))),
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
    fn zero_is_no_id_and_never_text() {
        assert_eq!(topic_id_text(0, 0), None);
        assert!(!is_canonical("AAAAAAAAAAAAAAAAAAAAAA"));
        assert_eq!(IdRead::of_halves(0, 0), IdRead::Unread(NO_TOPIC_ID));
        // One half zero is an ID.
        assert!(topic_id_text(0, 1).is_some());
        assert!(topic_id_text(1, 0).is_some());
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
            by_topic_id(Some(&entry(Some(A), None)), &entry(Some(A), None)),
            Generation::Same { topic_id: A.into() }
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
