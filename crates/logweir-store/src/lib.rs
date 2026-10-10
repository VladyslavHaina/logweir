//! The only place Logweir touches object storage. object_store 0.14 with
//! features ["aws","azure","gcp","http"] — the same crate, version and feature
//! set OSO uses (Global Constraint 9), so a bucket OSO can read, we can read.
//!
//! CREDENTIALS: `AmazonS3Builder::from_env()` applies object_store's OWN chain
//! (static keys, then web identity / IRSA, ECS, EKS Pod Identity, IMDS). That is
//! NOT the AWS SDK chain: `~/.aws/credentials` profiles, `AWS_PROFILE` and SSO
//! are unsupported. docs/stability.md states this in one sentence, because an
//! adopter discovering it at drill time is a support ticket.
#![forbid(unsafe_code)]
use logweir_core::engine::{BackupSetRef, EngineError, StorageUrl};
use object_store::path::Path as OPath;
use object_store::{ObjectStore, ObjectStoreExt as _, PutMode, PutOptions};
use std::sync::Arc;

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("object already exists at {0} — refusing to overwrite evidence")]
    AlreadyExists(String),
    /// Distinguished from `Io` so a caller can tell "definitely absent" from
    /// "could not tell" (permission denied, a timeout, a truncated read, a
    /// transient network error). Task 12 fix: `describe()`'s sibling
    /// consumer-groups-snapshot read used to collapse every `get` failure
    /// into `None` ("no snapshot"), which reported a 403 or a dropped
    /// connection identically to genuine absence — a positive claim
    /// (`consumer_group_snapshot_sha256: None`) the store never actually
    /// established, which then flows into the signed scorecard.
    #[error("object not found at {0}")]
    NotFound(String),
    #[error("storage: {0}")]
    Io(String),
    #[error("unsupported storage backend `{0}`")]
    Backend(String),
    /// Controller amendment: the handle returned by `read_only_from_url`
    /// physically cannot put. Every put method checks this before it checks
    /// anything else — including before the `LOGWEIR_ROOT` assertion — so a
    /// read-only handle constructed over the OSO archive prefix (which is
    /// exactly the case `read_only_from_url` exists to allow) can never reach
    /// a codepath that writes.
    #[error("store is read-only — refusing to put {0}")]
    ReadOnly(String),
    /// The object at this key parsed as JSON but is not a backup manifest:
    /// it declares no `topics` array at all.
    ///
    /// TASK 13 REVIEW CARRY, DISCHARGED HERE. `manifest_facts` used to answer
    /// `{"hello":"world"}` and `{"topics":5}` with
    /// `Backend("… declares no segment, so it bounds no window")` — the same
    /// error a REAL manifest describing an empty backup set gets. Those two
    /// facts are not the same fact, and collapsing them is the
    /// `NotFound`-versus-`Io` defect this enum's own doc comments already
    /// argue about: "this is not a manifest" is a configuration error (the
    /// prefix points at the wrong thing, or a sibling JSON object was picked
    /// up by the `/manifest.json` filter), while "this manifest bounds no
    /// window" is a fact about a backup set that really exists. A caller —
    /// and a reader of a controller log — needs to be able to tell them
    /// apart, so `manifest_facts` now returns THIS for the first and keeps
    /// `Backend` for the second.
    ///
    /// The second field carries what was wrong, so the message names the key
    /// AND the reason rather than only one of them.
    #[error("{0} is not a backup manifest: {1}")]
    NotAManifest(String, String),
    /// **FX-31.** The object is larger than the cap its reader set, so it was
    /// not read: [`Store::get_capped`] refused it on the size the store
    /// reported before any body byte was taken, or stopped at the first chunk
    /// that went past the cap when a store reported a size within it and then
    /// streamed more.
    ///
    /// A fact about the OBJECT, not a failure of the store: the read was
    /// answered, and the answer is "too big for this reader". It is never
    /// `NotFound` (the object is there) and never `Io` (nothing is wrong with
    /// the connection, and a retry reads the same object). The message names
    /// the key, the cap and what was observed, so the sentence an operator
    /// reads says which limit to look up.
    #[error(
        "{key} is larger than the {cap}-byte read cap ({observed}); nothing past the cap was read"
    )]
    TooLarge {
        key: String,
        cap: u64,
        observed: OverCap,
    },
}

/// What [`StoreError::TooLarge`] saw of an object over its reader's cap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverCap {
    /// The store's own size for the object — the `Content-Length` /
    /// `Content-Range` of the answer, or a filesystem's metadata — checked
    /// BEFORE any body byte was read.
    Reported(u64),
    /// The store reported `reported` bytes, within the cap, and its stream
    /// carried `read` bytes by the chunk that went past the cap. Reading
    /// stopped there: the chunk that crossed it was dropped, not kept.
    Streamed { reported: u64, read: u64 },
}

impl std::fmt::Display for OverCap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Reported(size) => write!(f, "the store reports {size} bytes"),
            Self::Streamed { reported, read } => write!(
                f,
                "the store reported {reported} bytes and streamed at least {read} before reading \
                 stopped"
            ),
        }
    }
}

/// What a `HEAD` of one object says — [`Store::head`], for an existence test
/// that must not read the body (FX-31).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectHead {
    /// The object's size, as the store reports it.
    pub size: u64,
    /// The version id the store reports, `None` on a store that keeps no
    /// versions.
    pub version: Option<String>,
}

/// **FX-31 — the read cap of every document Logweir reads from an object
/// store, in one table.**
///
/// [`Store::get_capped`] takes a cap and nothing reads without one: the
/// uncapped `Store::get` is gone, so a new read site has to name which of
/// these it is (or argue for a new row here). The caps are per DOCUMENT and
/// per READER: a runner, CLI or check Job reads in its own pod under its own
/// memory limit, while `weirkeeper` is one process for every namespace, so
/// the controller's caps are the smaller `CONTROLLER_*` rows and an object a
/// tenant planted cannot take memory the other namespaces' reconciles need.
///
/// | cap | bytes | read by | measured |
/// |---|---|---|---|
/// | [`SIDECAR`] | 64 KiB | everyone | one DSSE signature is about 312 bytes; equal to the evidence relay's sidecar cap |
/// | [`SIGNED_DOCUMENT`] | 64 MiB | runner, CLI, check Jobs | a 1.5.0 receipt is about 3.4 KB per topic (two-space pretty JSON, 14 semantic configuration entries and a schema-dependency block each), so a 5,000-topic run (`MAX_RESOLVED_TOPICS`) is about 16.4 MiB, 20 MiB with five overrides per topic |
/// | [`CONTROLLER_DOCUMENT`] | 1 MiB | `weirkeeper` | equal to the evidence relay's payload cap, so a document is verifiable by the controller exactly when it is verifiable through a relay; about 300 topics of receipt |
/// | [`MANIFEST`] | 256 MiB | runner, CLI, check Jobs | about 540 bytes per segment entry, so about 500,000 segments |
/// | [`CONTROLLER_MANIFEST`] | 64 MiB | `weirkeeper`'s retention report | about 124,000 segments; parsed as a stream, so memory is the bytes and no more |
/// | [`SEGMENT`] | 1 GiB | runner, CLI | eight times the engine's default `segment_max_bytes` (128 MiB); Logweir's default is 10 MiB. FX-30 owns the decode cap |
/// | [`ENGINE_DOCUMENT`] | 64 MiB | runner, CLI | the engine's consumer-groups snapshot and validation report |
/// | [`PROBE`] | 0 | check Jobs, `backup run` | a readiness probe of a key nobody wrote, and the backup set check's "is the manifest there": the answer is the GET's status, and any body is refused unread |
///
/// PROD-03.0's schema-dependency detection reads archived segments through
/// [`Store::get_bounded`] under its own 64 MiB stored cap (a `HEAD`, then a
/// ranged GET of exactly the reported size), so it is bounded the same way
/// and is not a row here.
pub mod caps {
    use logweir_core::check_contract::{MAX_EVIDENCE_PAYLOAD_BYTES, MAX_EVIDENCE_SIDECAR_BYTES};

    /// A detached DSSE sidecar, whoever reads it: the evidence relay's own
    /// sidecar cap.
    pub const SIDECAR: u64 = MAX_EVIDENCE_SIDECAR_BYTES;
    /// A signed evidence document (receipt, scorecard, catalog point record)
    /// read in a runner, CLI or check-Job process.
    pub const SIGNED_DOCUMENT: u64 = 64 << 20;
    /// A signed evidence document read by the SHARED controller: the evidence
    /// relay's payload cap, so the controller's own handle and a relay agree
    /// on which documents can be verified at all. The controller parses two
    /// such documents into a `serde_json::Value` before any digest check (the
    /// receipt's window, the scorecard's outcome), and a document of tiny
    /// values parses into about 37 times its size, so this cap is also what
    /// bounds that parse: about 40 MB at worst.
    pub const CONTROLLER_DOCUMENT: u64 = MAX_EVIDENCE_PAYLOAD_BYTES;
    /// An engine manifest read in a runner, CLI or check-Job process.
    pub const MANIFEST: u64 = 256 << 20;
    /// An engine manifest read by the controller's retention report.
    pub const CONTROLLER_MANIFEST: u64 = 64 << 20;
    /// One archived segment.
    pub const SEGMENT: u64 = 1 << 30;
    /// An engine-written document beside a set or a run: the consumer-groups
    /// snapshot, the engine's validation report.
    pub const ENGINE_DOCUMENT: u64 = 64 << 20;
    /// A readiness probe's GET of a key nobody wrote.
    pub const PROBE: u64 = 0;
}

/// The `backup_id` a manifest key belongs to: the key's parent directory.
///
/// TASK 13 REVIEW CARRY, DISCHARGED HERE. This derivation was copy-pasted in
/// two places — `Store::list_manifests` and `Store::manifest_facts` — and the
/// second one's doc comment promised it was "derived exactly as
/// `list_manifests` derives it, so the two can never disagree about which
/// backup set a manifest belongs to". A promise kept by two copies of four
/// chained iterator calls is a promise one edit breaks silently, and
/// `manifest_facts`'s `backup_id` is the string the retention report's
/// rendered `aws s3 rm` command names. It is one function now, both callers
/// go through it, and `the_backup_id_derivation_is_one_function` asserts the
/// two agree over every shape that reaches either.
///
/// `pub` because `weirkeeper`'s retention reconciler lists manifest KEYS
/// (`list_manifest_keys`, the same call `list_manifests` is written on top of)
/// and must name the sets by the same rule.
#[must_use]
pub fn backup_id_from_manifest_key(key: &str) -> String {
    key.trim_end_matches("/manifest.json")
        .rsplit('/')
        .next()
        .unwrap_or("")
        .to_string()
}

/// **FX-31 — the covered window ONE manifest body declares, folded while it
/// is parsed**: `(oldest start_timestamp, newest end_timestamp)` over every
/// segment of every partition of every topic.
///
/// It replaces a walk over a `serde_json::Value` of the whole body and keeps
/// that walk's answers exactly, because [`Store::manifest_facts`]'s callers
/// and tests were written against them:
///
/// | body | answer |
/// |---|---|
/// | not JSON (anywhere, trailing bytes included) | `Io("<key>: <serde_json's error>")` |
/// | not an object, or an object with no `topics` | `NotAManifest(key, "it declares no `topics` key")` |
/// | `topics` that is not an array | `NotAManifest(key, "`topics` is <a number / an object / …>, not an array")` |
/// | a topic that is not an object, or whose `partitions` is absent or not an array (likewise a partition and its `segments`) | that branch contributes nothing |
/// | a segment that is not an object, or whose `start_timestamp` / `end_timestamp` is absent or not an `i64` | `Backend("<key>: segment entry missing start_timestamp/end_timestamp")` |
/// | no segment at all | `Backend("<key>: manifest declares no segment, so it bounds no window")` |
///
/// **A duplicate key: the LAST occurrence wins**, at every level, as it does
/// in a `serde_json::Map`. Each level keeps the result of its last occurrence
/// and drops the earlier one; nothing about an earlier `topics` survives a
/// later one.
///
/// Nothing of the body is kept: keys are compared as they stream past, and
/// every value this does not need is skipped without being kept.
fn manifest_window(key: &str, bytes: &[u8]) -> Result<(i64, i64), StoreError> {
    let mut de = serde_json::Deserializer::from_slice(bytes);
    let top = serde::Deserializer::deserialize_any(&mut de, window::TopVisitor)
        .and_then(|top| de.end().map(|()| top))
        .map_err(|e| StoreError::Io(format!("{key}: {e}")))?;
    let topics = match top {
        None => {
            return Err(StoreError::NotAManifest(
                key.to_string(),
                "it declares no `topics` key".to_string(),
            ))
        }
        Some(Err(kind)) => {
            return Err(StoreError::NotAManifest(
                key.to_string(),
                format!("`topics` is {kind}, not an array"),
            ))
        }
        Some(Ok(fold)) => fold,
    };
    if topics.bad_segment {
        return Err(StoreError::Backend(format!(
            "{key}: segment entry missing start_timestamp/end_timestamp"
        )));
    }
    match topics.window {
        Some(window) => Ok(window),
        None => Err(StoreError::Backend(format!(
            "{key}: manifest declares no segment, so it bounds no window"
        ))),
    }
}

/// The visitors behind [`manifest_window`]. Each level answers a [`window::Fold`]
/// and holds nothing of the body.
mod window {
    use serde::de::{DeserializeSeed, Deserializer, MapAccess, SeqAccess, Visitor};
    use std::fmt;

    /// What one subtree contributes: the window of the segments it holds, and
    /// whether any of them lacked a timestamp.
    #[derive(Clone, Copy, Default)]
    pub(super) struct Fold {
        pub(super) window: Option<(i64, i64)>,
        pub(super) bad_segment: bool,
    }

    impl Fold {
        fn merge(&mut self, other: Fold) {
            self.bad_segment |= other.bad_segment;
            self.window = match (self.window, other.window) {
                (Some((a0, a1)), Some((b0, b1))) => Some((a0.min(b0), a1.max(b1))),
                (a, b) => a.or(b),
            };
        }
    }

    /// A value this fold does not need, consumed and dropped — and VALIDATED
    /// exactly as a `serde_json::Value` parse would validate it.
    ///
    /// Not `serde::de::IgnoredAny`: serde_json's ignore path does not check a
    /// string's UTF-8 or its `\u` escapes (`read.rs` `ignore_str` /
    /// `ignore_escape` in 1.0.151), so a body the `Value` walk refused as `Io`
    /// would have folded to a window here. `deserialize_any` parses every
    /// string and number the way `Value` does — with no allocation for a
    /// string without escapes, and serde_json's one scratch buffer for one
    /// with them — and nothing is kept.
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
            skip_map(&mut map)
        }
    }

    /// Every remaining entry of `map`, keys and values alike, through [`Skip`].
    fn skip_map<'de, A: MapAccess<'de>>(map: &mut A) -> Result<(), A::Error> {
        while map.next_key_seed(Skip)?.is_some() {
            map.next_value_seed(Skip)?;
        }
        Ok(())
    }

    /// A map key, compared and dropped.
    enum Key {
        Topics,
        Partitions,
        Segments,
        Start,
        End,
        Other,
    }

    struct KeyVisitor;

    impl<'de> Visitor<'de> for KeyVisitor {
        type Value = Key;
        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("a key")
        }
        fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Key, E> {
            Ok(match v {
                "topics" => Key::Topics,
                "partitions" => Key::Partitions,
                "segments" => Key::Segments,
                "start_timestamp" => Key::Start,
                "end_timestamp" => Key::End,
                _ => Key::Other,
            })
        }
    }

    impl<'de> DeserializeSeed<'de> for KeyVisitor {
        type Value = Key;
        fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<Key, D::Error> {
            d.deserialize_str(self)
        }
    }

    /// Which level of the manifest a value sits at, which decides what an
    /// array or an object there means. (`topics` itself is [`TopicsVisitor`]'s,
    /// because it alone names the kind of a value that is not an array.)
    #[derive(Clone, Copy)]
    enum Level {
        /// One element of `topics`: an object whose `partitions` counts.
        Topic,
        /// `partitions`: an array of partitions.
        Partitions,
        /// One element of `partitions`: an object whose `segments` counts.
        Partition,
        /// `segments`: an array of segments.
        Segments,
        /// One element of `segments`: an object with two timestamps.
        Segment,
    }

    /// The type name `Value`'s walk reported for a `topics` that is not an
    /// array (`kind_of`, which this replaces).
    pub(super) type NotAnArray = &'static str;

    /// The top level: `Some(Ok(fold))` for an object whose LAST `topics` is
    /// an array, `Some(Err(kind))` when that `topics` is something else, and
    /// `None` for a non-object or an object with no `topics`.
    pub(super) struct TopVisitor;

    impl<'de> Visitor<'de> for TopVisitor {
        type Value = Option<Result<Fold, NotAnArray>>;
        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("a JSON document")
        }
        fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
            let mut topics = None;
            while let Some(key) = map.next_key_seed(KeyVisitor)? {
                if matches!(key, Key::Topics) {
                    topics = Some(map.next_value_seed(TopicsSeed)?);
                } else {
                    map.next_value_seed(Skip)?;
                }
            }
            Ok(topics)
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

    /// The value of `topics`: an array folds, anything else names its kind.
    struct TopicsSeed;

    impl<'de> DeserializeSeed<'de> for TopicsSeed {
        type Value = Result<Fold, NotAnArray>;
        fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
            d.deserialize_any(TopicsVisitor)
        }
    }

    struct TopicsVisitor;

    impl<'de> Visitor<'de> for TopicsVisitor {
        type Value = Result<Fold, NotAnArray>;
        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("any JSON value")
        }
        fn visit_seq<A: SeqAccess<'de>>(self, seq: A) -> Result<Self::Value, A::Error> {
            fold_seq(seq, Level::Topic).map(Ok)
        }
        fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
            skip_map(&mut map)?;
            Ok(Err("an object"))
        }
        fn visit_bool<E>(self, _: bool) -> Result<Self::Value, E> {
            Ok(Err("a boolean"))
        }
        fn visit_i64<E>(self, _: i64) -> Result<Self::Value, E> {
            Ok(Err("a number"))
        }
        fn visit_u64<E>(self, _: u64) -> Result<Self::Value, E> {
            Ok(Err("a number"))
        }
        fn visit_f64<E>(self, _: f64) -> Result<Self::Value, E> {
            Ok(Err("a number"))
        }
        fn visit_str<E>(self, _: &str) -> Result<Self::Value, E> {
            Ok(Err("a string"))
        }
        fn visit_unit<E>(self) -> Result<Self::Value, E> {
            Ok(Err("null"))
        }
    }

    /// Fold every element of an array whose elements sit at `element`.
    fn fold_seq<'de, A: SeqAccess<'de>>(mut seq: A, element: Level) -> Result<Fold, A::Error> {
        let mut fold = Fold::default();
        while let Some(one) = seq.next_element_seed(LevelSeed(element))? {
            fold.merge(one);
        }
        Ok(fold)
    }

    /// One value at a known level.
    struct LevelSeed(Level);

    impl<'de> DeserializeSeed<'de> for LevelSeed {
        type Value = Fold;
        fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<Fold, D::Error> {
            d.deserialize_any(LevelVisitor(self.0))
        }
    }

    struct LevelVisitor(Level);

    impl LevelVisitor {
        /// What a value that is neither the array nor the object this level
        /// expects contributes: nothing, except at a SEGMENT, where a value
        /// that is not an object carries no timestamp and is a bad segment.
        fn scalar(&self) -> Fold {
            Fold {
                window: None,
                bad_segment: matches!(self.0, Level::Segment),
            }
        }
    }

    impl<'de> Visitor<'de> for LevelVisitor {
        type Value = Fold;
        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("any JSON value")
        }
        fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Fold, A::Error> {
            let element = match self.0 {
                Level::Partitions => Level::Partition,
                Level::Segments => Level::Segment,
                // An array where an OBJECT belongs: a topic, partition or
                // segment that is not an object. It carries no key, so it
                // contributes what any other non-object there does.
                Level::Topic | Level::Partition | Level::Segment => {
                    while seq.next_element_seed(Skip)?.is_some() {}
                    return Ok(self.scalar());
                }
            };
            fold_seq(seq, element)
        }
        fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Fold, A::Error> {
            match self.0 {
                Level::Topic | Level::Partition => {
                    // The LAST `partitions` (of a topic) or `segments` (of a
                    // partition) is the one that counts; an earlier one is
                    // folded and then replaced, never merged.
                    let (wanted, inner) = match self.0 {
                        Level::Topic => (Key::Partitions, Level::Partitions),
                        _ => (Key::Segments, Level::Segments),
                    };
                    let mut last = Fold::default();
                    while let Some(key) = map.next_key_seed(KeyVisitor)? {
                        if std::mem::discriminant(&key) == std::mem::discriminant(&wanted) {
                            last = map.next_value_seed(LevelSeed(inner))?;
                        } else {
                            map.next_value_seed(Skip)?;
                        }
                    }
                    Ok(last)
                }
                Level::Segment => {
                    let (mut start, mut end) = (None, None);
                    while let Some(key) = map.next_key_seed(KeyVisitor)? {
                        match key {
                            Key::Start => start = map.next_value_seed(I64Seed)?,
                            Key::End => end = map.next_value_seed(I64Seed)?,
                            _ => {
                                map.next_value_seed(Skip)?;
                            }
                        }
                    }
                    Ok(match (start, end) {
                        (Some(t0), Some(t1)) => Fold {
                            window: Some((t0, t1)),
                            bad_segment: false,
                        },
                        _ => Fold {
                            window: None,
                            bad_segment: true,
                        },
                    })
                }
                // An object where an ARRAY belongs: `partitions` or `segments`
                // that is not an array contributes nothing.
                Level::Partitions | Level::Segments => {
                    skip_map(&mut map)?;
                    Ok(self.scalar())
                }
            }
        }
        fn visit_bool<E>(self, _: bool) -> Result<Fold, E> {
            Ok(self.scalar())
        }
        fn visit_i64<E>(self, _: i64) -> Result<Fold, E> {
            Ok(self.scalar())
        }
        fn visit_u64<E>(self, _: u64) -> Result<Fold, E> {
            Ok(self.scalar())
        }
        fn visit_f64<E>(self, _: f64) -> Result<Fold, E> {
            Ok(self.scalar())
        }
        fn visit_str<E>(self, _: &str) -> Result<Fold, E> {
            Ok(self.scalar())
        }
        fn visit_unit<E>(self) -> Result<Fold, E> {
            Ok(self.scalar())
        }
    }

    /// A timestamp, as `serde_json::Value::as_i64` reads one: an integer that
    /// fits an `i64`, and `None` for anything else (a float, a string, an
    /// integer above `i64::MAX`, null, an array, an object).
    struct I64Seed;

    impl<'de> DeserializeSeed<'de> for I64Seed {
        type Value = Option<i64>;
        fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<Option<i64>, D::Error> {
            d.deserialize_any(I64Visitor)
        }
    }

    struct I64Visitor;

    impl<'de> Visitor<'de> for I64Visitor {
        type Value = Option<i64>;
        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("any JSON value")
        }
        fn visit_i64<E>(self, v: i64) -> Result<Option<i64>, E> {
            Ok(Some(v))
        }
        fn visit_u64<E>(self, v: u64) -> Result<Option<i64>, E> {
            Ok(i64::try_from(v).ok())
        }
        fn visit_f64<E>(self, _: f64) -> Result<Option<i64>, E> {
            Ok(None)
        }
        fn visit_bool<E>(self, _: bool) -> Result<Option<i64>, E> {
            Ok(None)
        }
        fn visit_str<E>(self, _: &str) -> Result<Option<i64>, E> {
            Ok(None)
        }
        fn visit_unit<E>(self) -> Result<Option<i64>, E> {
            Ok(None)
        }
        fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Option<i64>, A::Error> {
            while seq.next_element_seed(Skip)?.is_some() {}
            Ok(None)
        }
        fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Option<i64>, A::Error> {
            skip_map(&mut map)?;
            Ok(None)
        }
    }
}

impl From<StoreError> for EngineError {
    fn from(e: StoreError) -> Self {
        EngineError::Operational(e.to_string())
    }
}

#[derive(Debug, Clone)]
pub struct PutOutcome {
    pub version_id: Option<String>,
    /// false when the backend answered `Unsupported` to `PutMode::Create` and we
    /// fell back to HEAD-then-PUT. Recorded honestly into
    /// `evidence.create_only_enforced`; never assumed true.
    pub create_only_enforced: bool,
}

/// What a provider actually reported about an object's WORM retention. Only
/// ever constructed from a readback — never inferred from bucket settings, and
/// never defaulted — so `evidence.immutable` in a signed scorecard can be
/// `true` only when a provider said so (spec §6 C3).
#[derive(Debug, Clone)]
pub struct LockInfo {
    pub immutable: bool,
    pub retain_until: Option<chrono::DateTime<chrono::Utc>>,
}

/// The covered window one manifest declares, plus the backup set it belongs
/// to. Interface **I12**, the only new type in the `logweir-store`
/// extraction.
///
/// It exists because `list_manifests` returns `Vec<BackupSetRef>` and
/// `BackupSetRef` is `{ backup_id, manifest_key }` — derived from the key
/// string alone, with no object read, so **there is no timestamp anywhere in
/// the returned type**. The only structure carrying one is `BackupSetFacts`,
/// produced by `OsoCliEngine::describe`, in the crate this extraction exists
/// to keep out of the control plane. A retention reconciler needs a window
/// and must not link that crate to get one.
#[derive(Debug, Clone)]
pub struct ManifestFacts {
    pub backup_id: String,
    pub newest_record_ms: i64,
    pub oldest_record_ms: i64,
}

/// The ONLY key root Logweir may write under (Global Constraint 6). Fixed in
/// code, never taken from a spec, so `put_create_only`'s guard cannot be
/// widened by an adopter's configuration.
pub const LOGWEIR_ROOT: &str = "logweir/";

pub struct Store {
    inner: Arc<dyn ObjectStore>,
    prefix: String,
    /// Set false by `in_memory_without_conditional_put` and by the runtime
    /// fallback, so the flag the scorecard publishes is observed, not declared.
    conditional_put: bool,
    /// Built once per store and reused. See `new_rt` for why per-call runtimes
    /// break connection reuse on every network backend.
    rt: Arc<tokio::runtime::Runtime>,
    /// Controller amendment: true only for handles returned by
    /// `read_only_from_url`. A handle that physically cannot put is a
    /// stronger guarantee than a prefix check, and it keeps the write path's
    /// `LOGWEIR_ROOT` guard (in `from_url` and in `put_create_only`) intact —
    /// this flag never widens or bypasses that guard, it only ever adds an
    /// earlier, unconditional refusal in front of it.
    read_only: bool,
    /// TEST DOUBLE ONLY — true for [`Store::in_memory_ignoring_conditional_put`]
    /// and NEVER set by a production constructor. It models an S3-compatible
    /// store that accepts `If-None-Match: *` and overwrites anyway: a
    /// create-only put over an existing key SUCCEEDS and reports
    /// `create_only_enforced: true`, because that is what such a store tells
    /// its client. `object_store` has no way to see it, and the backup
    /// runner's execution claim (RECEIPT-DUP) must fail closed on it, so the
    /// runner's rows need a store that lies the way that one does.
    ignores_create_mode: bool,
    /// TEST DOUBLE ONLY — true for [`Store::in_memory_erroring_on_existing_key`]
    /// and never set by a production constructor. A create-only put over an
    /// existing key answers `StoreError::Io` instead of `AlreadyExists`: a
    /// store that errors where it should have refused. The backup runner's
    /// exclusivity probe must not read that error as proof (RECEIPT-DUP).
    errors_on_existing_key: bool,
    /// TEST DOUBLE ONLY — `Some` for [`Store::in_memory_versioned`] and `None`
    /// for every production constructor, which read version ids from the
    /// backend itself. `object_store`'s `InMemory` reports no version id and
    /// IGNORES a request for one, so a versioned bucket (FX-7: the backup
    /// runner pins the manifest's version id, and readers read by it) has to
    /// be modelled beside it. See [`VersionedBucket`].
    versions: Option<Arc<VersionLog>>,
    /// TEST DOUBLE ONLY — `Some` for [`Store::in_memory_misreporting_size`]
    /// and `None` for every production constructor (FX-31). Every GET and HEAD
    /// answer is then rewritten to REPORT a size other than the object's, and
    /// every body byte a reader takes is counted. Modelled here, beside
    /// `versions`, rather than as a wrapping `ObjectStore`: that trait cannot
    /// be implemented without naming its delete, which this crate never does
    /// (G-RET, `scripts/check-no-archive-write.sh`).
    misreport: Option<Arc<Misreport>>,
}

/// What [`Store::in_memory_misreporting_size`] reports, and its meter.
struct Misreport {
    reported: u64,
    streamed: Arc<std::sync::atomic::AtomicU64>,
}

impl Store {
    /// One arm per `StorageUrl` variant, matching upstream's own
    /// `StorageBackendConfig` shape [VERIFIED
    /// U/kafka-backup/crates/kafka-backup-core/src/storage/config.rs:14-105].
    /// The client is built INSIDE `rt.block_on`, so the client and its
    /// connection pool belong to the runtime that will later drive them.
    /// Shared by `from_url` and `read_only_from_url` — the two constructors
    /// differ only in whether the `LOGWEIR_ROOT` guard runs, never in how the
    /// backend itself is built.
    fn build_backend(
        u: &StorageUrl,
        rt: &tokio::runtime::Runtime,
    ) -> Result<Arc<dyn ObjectStore>, StoreError> {
        rt.block_on(async {
            let r: Result<Arc<dyn ObjectStore>, StoreError> = match u {
                StorageUrl::S3 {
                    bucket,
                    region,
                    endpoint,
                    path_style,
                    allow_http,
                    ..
                } => {
                    // FX-20 fix round (review F1): `from_env()` reads
                    // `AWS_REGION`/`AWS_DEFAULT_REGION` when the location names
                    // none, and the region becomes the host on an endpoint-less
                    // location, so the environment's spelling is held to the
                    // same rule as the location's.
                    refuse_invalid_region(region.as_deref())?;
                    if region.is_none() {
                        for var in ["AWS_REGION", "AWS_DEFAULT_REGION"] {
                            refuse_invalid_region(env_value(var).as_deref())?;
                        }
                    }
                    let mut b = object_store::aws::AmazonS3Builder::from_env()
                        .with_bucket_name(bucket)
                        .with_virtual_hosted_style_request(!*path_style)
                        .with_allow_http(*allow_http);
                    if let Some(r) = region {
                        b = b.with_region(r);
                    }
                    if let Some(e) = endpoint {
                        b = b.with_endpoint(e);
                    }
                    Ok(Arc::new(
                        b.build().map_err(|e| StoreError::Io(e.to_string()))?,
                    ))
                }
                StorageUrl::Azure {
                    account_name,
                    container_name,
                    ..
                } => Ok(Arc::new(
                    object_store::azure::MicrosoftAzureBuilder::from_env()
                        .with_account(account_name)
                        .with_container_name(container_name)
                        .build()
                        .map_err(|e| StoreError::Io(e.to_string()))?,
                )),
                StorageUrl::Gcs { bucket, .. } => Ok(Arc::new(
                    object_store::gcp::GoogleCloudStorageBuilder::from_env()
                        .with_bucket_name(bucket)
                        .build()
                        .map_err(|e| StoreError::Io(e.to_string()))?,
                )),
                StorageUrl::Filesystem { path } => Ok(Arc::new(
                    object_store::local::LocalFileSystem::new_with_prefix(path)
                        .map_err(|e| StoreError::Io(e.to_string()))?,
                )),
            };
            r
        })
    }

    /// The write-path constructor. Enforces Global Constraint 6 at
    /// construction rather than only at the put: a spec naming a prefix
    /// outside `logweir/` is a phase-0 refusal, not a panic in the middle of
    /// a signed upload.
    ///
    /// THE PREFIX MUST BE EXACTLY `logweir/`, not merely start with it, and
    /// that is a fix. `starts_with` admitted `logweir/prod/` — a legal-looking
    /// prefix that `docs/quickstart.md` and `examples/drill.yaml` both already
    /// described as refused — while every key builder in this crate is
    /// hard-coded to `logweir/drills/…`. The result was `assert!(key
    /// .starts_with(&self.prefix))` firing in `put_create_only`, AFTER the
    /// restore had already run: a panic, exit 101, outside the five-code exit
    /// contract entirely, on the one path where the drill had already touched
    /// the operator's cluster. Refusing at construction puts it back inside
    /// the contract, and this constructor's own doc comment above already
    /// promised exactly that.
    ///
    /// `Filesystem` is exempt because it HAS no prefix — `StorageUrl::prefix`
    /// returns `""` for it, since upstream's variant carries only `path` — and
    /// its keys still go through the `LOGWEIR_ROOT` assertion in
    /// `put_create_only`, so objects still land under `<path>/logweir/`. The
    /// exemption is about a field that does not exist, not about the rule.
    pub fn from_url(u: &StorageUrl) -> Result<Self, StoreError> {
        let rt = Self::new_rt();
        let prefix = u.prefix().to_string();
        Self::assert_evidence_prefix(u, &prefix)?;
        let inner = Self::build_backend(u, &rt)?;
        Ok(Self {
            inner,
            prefix,
            conditional_put: true,
            rt,
            read_only: false,
            ignores_create_mode: false,
            errors_on_existing_key: false,
            versions: None,
            misreport: None,
        })
    }

    /// Controller amendment (added after the addenda pass): the read-path
    /// constructor for reading the OSO archive. Skips the `LOGWEIR_ROOT`
    /// prefix guard — the archive prefix (e.g. `kafka-backups/daily`) is never
    /// under `logweir/`, and `from_url` would refuse to construct a `Store`
    /// over it, which would make `list_backup_sets`/`describe`/
    /// `segment_keys_for` unable to run at all.
    ///
    /// The handle this returns physically cannot put: `read_only` is set
    /// `true` here and `put_create_only` checks it FIRST, before the
    /// `LOGWEIR_ROOT` assertion, so this constructor never becomes a second
    /// way to write outside `logweir/`. `from_url`'s guard is unchanged and
    /// stays the only way to build a *writable* store.
    pub fn read_only_from_url(u: &StorageUrl) -> Result<Self, StoreError> {
        let rt = Self::new_rt();
        let prefix = u.prefix().to_string();
        let inner = Self::build_backend(u, &rt)?;
        Ok(Self {
            inner,
            prefix,
            conditional_put: true,
            rt,
            read_only: true,
            ignores_create_mode: false,
            errors_on_existing_key: false,
            versions: None,
            misreport: None,
        })
    }

    /// The EXPLICIT write-path constructor (D2 W2). Same `LOGWEIR_ROOT`
    /// guard as [`Store::from_url`], and the same handle; what differs is
    /// that every addressing, transport and credential decision comes from
    /// the arguments rather than from the process environment.
    pub fn from_url_with(u: &StorageUrl, opts: &StoreOptions) -> Result<Self, StoreError> {
        let rt = Self::new_rt();
        let prefix = u.prefix().to_string();
        Self::assert_evidence_prefix(u, &prefix)?;
        let inner = Self::build_backend_with(u, opts, &rt)?;
        Ok(Self {
            inner,
            prefix,
            conditional_put: true,
            rt,
            read_only: false,
            ignores_create_mode: false,
            errors_on_existing_key: false,
            versions: None,
            misreport: None,
        })
    }

    /// The EXPLICIT read-path constructor (D2 W2), the sibling of
    /// [`Store::read_only_from_url`]. The handle it returns physically cannot
    /// put, for the same reason and by the same flag.
    ///
    /// This is what the controller's allowlisted `ControllerIdentity`
    /// evidence cache builds (D2 §3.10) and what a check Job's archive and
    /// evidence reads use.
    pub fn read_only_with(u: &StorageUrl, opts: &StoreOptions) -> Result<Self, StoreError> {
        let rt = Self::new_rt();
        let prefix = u.prefix().to_string();
        let inner = Self::build_backend_with(u, opts, &rt)?;
        Ok(Self {
            inner,
            prefix,
            conditional_put: true,
            rt,
            read_only: true,
            ignores_create_mode: false,
            errors_on_existing_key: false,
            versions: None,
            misreport: None,
        })
    }

    /// ONE builder, driven by [`s3_effective`]'s decision.
    ///
    /// Every value below is READ OFF `eff`, never recomputed here, so the
    /// configuration a test inspects with [`s3_effective`] is the
    /// configuration the client gets. `AmazonS3Builder::from_env()` is called
    /// ONLY for [`CredentialSource::Ambient`]; every other source starts from
    /// `AmazonS3Builder::new()`, so no `AWS_*` variable can reach the client
    /// except the named ones `s3_effective` itself resolved.
    fn build_backend_with(
        u: &StorageUrl,
        opts: &StoreOptions,
        rt: &tokio::runtime::Runtime,
    ) -> Result<Arc<dyn ObjectStore>, StoreError> {
        let b = rt.block_on(async { Self::s3_builder(u, opts) })?;
        let s3: Arc<dyn ObjectStore> =
            Arc::new(b.build().map_err(|e| StoreError::Io(e.to_string()))?);
        Ok(s3)
    }

    /// THE builder, configured and not yet built.
    ///
    /// Split out of [`Store::build_backend_with`] so [`s3_builder_config`] can
    /// read back what a store WILL dial without dialling it. That readback is
    /// taken off this very builder, not off a second model of it, which is the
    /// property `s3_effective`'s own doc claims and which the first version of
    /// this module did not actually have.
    fn s3_builder(
        u: &StorageUrl,
        opts: &StoreOptions,
    ) -> Result<object_store::aws::AmazonS3Builder, StoreError> {
        let eff = s3_effective(u, opts)?;
        let mut client = object_store::ClientOptions::default().with_allow_http(eff.allow_http);
        for pem in &opts.root_certificates {
            for cert in object_store::Certificate::from_pem_bundle(pem)
                .map_err(|e| StoreError::Backend(format!("root certificate: {e}")))?
            {
                client = client.with_root_certificate(cert);
            }
        }
        if let Some(t) = eff.request_timeout {
            client = client.with_timeout(t).with_connect_timeout(t);
        }
        {
            // `AmazonS3Builder::new()`, NEVER `from_env()`, for EVERY source.
            // `from_env()` sweeps every `AWS_*` variable, including
            // `AWS_ENDPOINT_URL` and `AWS_REGION`, which would silently
            // relocate a destination that names neither.
            let mut b = object_store::aws::AmazonS3Builder::new()
                .with_bucket_name(&eff.bucket)
                // From the `StorageUrl` and from nothing else.
                .with_virtual_hosted_style_request(eff.virtual_hosted_style)
                // THE SINGLE TRANSPORT OVERRIDE. `AmazonS3Builder::with_allow_http`
                // writes into the builder's own `client_options`, which this
                // call then REPLACES wholesale — so calling both would leave
                // one of them dead, and a dead override is how a guard comes
                // to be deleted as redundant while the live one is deleted as
                // "already covered". `client` was built above from
                // `eff.allow_http`, which came from the plan's transport and
                // from nothing else (defect SEC-ENVHTTP, D-SEAMS S5).
                .with_client_options(client);
            if let Some(r) = &eff.region {
                b = b.with_region(r);
            }
            if let Some(e) = &eff.endpoint {
                b = b.with_endpoint(e);
            }
            if let Some(m) = &eff.metadata_endpoint {
                b = b.with_metadata_endpoint(m);
            }
            if let Some(n) = eff.max_retries {
                b = b.with_retry(object_store::RetryConfig {
                    max_retries: n,
                    // object_store's own default window, unless the caller
                    // asked for another. Setting `max_retries` alone does not
                    // shorten it.
                    retry_timeout: eff
                        .retry_timeout
                        .unwrap_or_else(|| std::time::Duration::from_secs(180)),
                    ..Default::default()
                });
            }

            // The credential, projected variable by NAMED variable.
            // `eff.environment_variables_read` is the complete list, and this
            // block reads nothing outside it.
            let mut static_keys: Option<(String, String, Option<String>)> = None;
            match &opts.credentials {
                CredentialSource::Static {
                    access_key_id,
                    secret_access_key,
                    session_token,
                } => {
                    static_keys = Some((
                        access_key_id.clone(),
                        secret_access_key.clone(),
                        session_token.clone(),
                    ));
                }
                CredentialSource::StaticFromEnv => {
                    // `s3_effective` already established that both are set.
                    static_keys = Some((
                        std::env::var("AWS_ACCESS_KEY_ID").unwrap_or_default(),
                        std::env::var("AWS_SECRET_ACCESS_KEY").unwrap_or_default(),
                        env_value("AWS_SESSION_TOKEN"),
                    ));
                }
                CredentialSource::WorkloadIdentity => {}
                CredentialSource::Ambient => {
                    // The chain `s3_effective` resolved, re-read from the same
                    // named variables. `CredentialKind::Ambient` means it
                    // resolved to instance metadata, which needs no projection.
                    if eff.credentials == CredentialKind::Static {
                        static_keys = Some((
                            std::env::var("AWS_ACCESS_KEY_ID").unwrap_or_default(),
                            std::env::var("AWS_SECRET_ACCESS_KEY").unwrap_or_default(),
                            env_value("AWS_SESSION_TOKEN"),
                        ));
                    }
                }
            }
            if let Some((key_id, secret, token)) = static_keys {
                b = b.with_access_key_id(key_id).with_secret_access_key(secret);
                if let Some(t) = token {
                    b = b.with_token(t);
                }
            }
            if let Some(id) = eff.workload_identity.clone() {
                b = Self::with_workload_identity(b, id);
            }
            Ok(b)
        }
    }

    /// The injected identity, variable by named variable. Split out so the
    /// three shapes are readable and so `build_backend_with` stays one screen.
    fn with_workload_identity(
        b: object_store::aws::AmazonS3Builder,
        id: WorkloadIdentity,
    ) -> object_store::aws::AmazonS3Builder {
        use object_store::aws::AmazonS3ConfigKey as K;
        match id {
            WorkloadIdentity::WebIdentity {
                token_file,
                role_arn,
                session_name,
                sts_endpoint,
            } => {
                let mut b = b
                    .with_config(K::WebIdentityTokenFile, token_file)
                    .with_config(K::RoleArn, role_arn);
                if let Some(n) = session_name {
                    b = b.with_config(K::RoleSessionName, n);
                }
                if let Some(e) = sts_endpoint {
                    b = b.with_config(K::StsEndpoint, e);
                }
                b
            }
            WorkloadIdentity::ContainerFullUri { uri, token_file } => b
                .with_config(K::ContainerCredentialsFullUri, uri)
                .with_config(K::ContainerAuthorizationTokenFile, token_file),
            WorkloadIdentity::ContainerRelativeUri { uri } => {
                b.with_config(K::ContainerCredentialsRelativeUri, uri)
            }
        }
    }

    /// Global Constraint 6's construction-time guard, extracted so
    /// [`Store::from_url`] and [`Store::from_url_with`] enforce it by calling
    /// ONE function rather than by carrying two copies of the same message.
    fn assert_evidence_prefix(u: &StorageUrl, prefix: &str) -> Result<(), StoreError> {
        if prefix != LOGWEIR_ROOT && !matches!(u, StorageUrl::Filesystem { .. }) {
            return Err(StoreError::Backend(format!(
                "evidence prefix `{prefix}` must be exactly `{LOGWEIR_ROOT}` (Global \
                 Constraint 6). Logweir builds every evidence key as \
                 `{LOGWEIR_ROOT}drills/<run_id>…`, so a deeper prefix such as \
                 `{LOGWEIR_ROOT}prod/` names a location nothing would ever be written \
                 to; put the environment in the BUCKET, not in the prefix."
            )));
        }
        Ok(())
    }

    pub fn in_memory(prefix: &str) -> Self {
        Self {
            inner: Arc::new(object_store::memory::InMemory::new()),
            prefix: prefix.to_string(),
            conditional_put: true,
            rt: Self::new_rt(),
            read_only: false,
            ignores_create_mode: false,
            errors_on_existing_key: false,
            versions: None,
            misreport: None,
        }
    }

    pub fn in_memory_without_conditional_put(prefix: &str) -> Self {
        Self {
            conditional_put: false,
            ..Self::in_memory(prefix)
        }
    }

    /// A TEST DOUBLE of an S3-compatible store that ACCEPTS `If-None-Match: *`
    /// and overwrites anyway (RECEIPT-DUP): every create-only put succeeds,
    /// replaces what is there, and reports `create_only_enforced: true` —
    /// exactly what such a store's answer looks like to `object_store`. No
    /// production path builds it.
    #[doc(hidden)]
    pub fn in_memory_ignoring_conditional_put(prefix: &str) -> Self {
        Self {
            ignores_create_mode: true,
            ..Self::in_memory(prefix)
        }
    }

    /// A TEST DOUBLE of a store that ERRORS (`StoreError::Io`) on a
    /// create-only put over an existing key, where an enforcing store answers
    /// `AlreadyExists` (RECEIPT-DUP). No production path builds it.
    #[doc(hidden)]
    pub fn in_memory_erroring_on_existing_key(prefix: &str) -> Self {
        Self {
            errors_on_existing_key: true,
            ..Self::in_memory(prefix)
        }
    }

    /// A TEST DOUBLE of a store that MISREPORTS an object's size (FX-31): every
    /// GET (and HEAD) answers with `reported` as the object's size and the
    /// range it covers, and then streams the object's real bytes — a
    /// misbehaving proxy or endpoint, which is what [`Store::get_capped`]'s
    /// running cap exists for. Writes and lists are the in-memory backend's.
    ///
    /// The [`StreamMeter`] counts every body byte a reader actually took from
    /// a GET's stream, so a row can show that a refusal on the reported size,
    /// or a [`Store::head`], took none. No production path builds it.
    #[doc(hidden)]
    pub fn in_memory_misreporting_size(prefix: &str, reported: u64) -> (Self, StreamMeter) {
        let meter = StreamMeter::default();
        let store = Self {
            misreport: Some(Arc::new(Misreport {
                reported,
                streamed: Arc::clone(&meter.0),
            })),
            ..Self::in_memory(prefix)
        };
        (store, meter)
    }

    /// The test double's rewrite of one GET answer (see `misreport`); every
    /// production store answers `r` unchanged.
    fn as_answered(&self, mut r: object_store::GetResult) -> object_store::GetResult {
        use futures::StreamExt as _;
        use object_store::GetResultPayload;
        let Some(m) = &self.misreport else {
            return r;
        };
        // THE LIE: the headers say `reported`, the body is whole.
        r.meta.size = m.reported;
        r.range = 0..m.reported;
        // THE METER: every chunk a reader polls out of the body. The double is
        // built over the in-memory backend, whose body is always a stream.
        let placeholder = GetResultPayload::Stream(futures::stream::empty().boxed());
        if let GetResultPayload::Stream(body) = std::mem::replace(&mut r.payload, placeholder) {
            let streamed = Arc::clone(&m.streamed);
            r.payload = GetResultPayload::Stream(
                body.inspect(move |chunk| {
                    if let Ok(bytes) = chunk {
                        streamed.fetch_add(
                            u64::try_from(bytes.len()).unwrap_or(u64::MAX),
                            std::sync::atomic::Ordering::SeqCst,
                        );
                    }
                })
                .boxed(),
            );
        }
        r
    }

    /// A TEST DOUBLE of a VERSIONED bucket (FX-7), and the handle a test uses
    /// to act on it the way a writer OTHER than this store would: overwrite a
    /// key unconditionally, as the engine's own manifest put does. No
    /// production path builds it, and it offers no delete (G-RET: this crate
    /// names no object-store delete).
    ///
    /// Every successful put through the STORE, and every write through the
    /// handle, becomes a new version with a fresh id; [`Store::get_capped`] reports
    /// the current one, and [`Store::get_version_capped`] reads any retained one —
    /// which is what S3, MinIO and SeaweedFS do for a bucket with versioning
    /// enabled.
    #[doc(hidden)]
    pub fn in_memory_versioned(prefix: &str) -> (Self, VersionedBucket) {
        let backend = Arc::new(object_store::memory::InMemory::new());
        let log = Arc::new(VersionLog::default());
        let rt = Self::new_rt();
        let store = Self {
            inner: backend.clone(),
            prefix: prefix.to_string(),
            conditional_put: true,
            rt: rt.clone(),
            read_only: false,
            ignores_create_mode: false,
            errors_on_existing_key: false,
            versions: Some(log.clone()),
            misreport: None,
        };
        (store, VersionedBucket { backend, log, rt })
    }

    /// ONE runtime for the life of the store, built in every constructor and
    /// held in `self.rt`. `object_store` 0.14's AmazonS3/MicrosoftAzure/
    /// GoogleCloudStorage backends hold an HTTP client whose connection pool and
    /// hyper background tasks are bound to the runtime that drove them; building
    /// and DROPPING a runtime per call orphans those pooled connections, so
    /// every later call pays a fresh TCP+TLS handshake and can observe
    /// `connection closed before message completed` on a reused pool entry.
    /// The InMemory suite cannot surface this — it does no I/O — which is why
    /// the assertion lives in Task 20 step 0's MinIO leg as well.
    fn new_rt() -> Arc<tokio::runtime::Runtime> {
        Arc::new(
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("current-thread runtime"),
        )
    }

    /// **FX-31 — read one object whole, and never more than `max_bytes` of
    /// it.** Returns the bytes and the version id the store answered with.
    ///
    /// # The two fences, in order
    ///
    /// 1. **The size the store reports**, from the GET's own answer (its
    ///    `Content-Length` / `Content-Range`, or a filesystem's metadata),
    ///    is checked BEFORE any body byte is taken. An object over the cap is
    ///    refused there, with [`OverCap::Reported`], and its body is dropped
    ///    unread.
    /// 2. **A running cap over the stream.** A store that reports a size
    ///    within the cap and then streams more — a misbehaving proxy, a
    ///    compromised endpoint, an object replaced between the headers and
    ///    the body — is cut off at the first chunk that would take the total
    ///    past the cap, with [`OverCap::Streamed`]. That chunk is not kept,
    ///    and the buffer grows by doubling but never past the cap, so the
    ///    most this read ever holds is the cap plus the one chunk in hand.
    ///
    /// So the memory a read can take is the caller's decision, named at the
    /// call site from [`caps`], and not the size of whatever a bucket holds.
    /// A shared controller that read whole objects could be OOM-killed by one
    /// tenant's multi-gigabyte object at a receipt key, for every namespace,
    /// on every restart.
    ///
    /// `StoreError::NotFound` specifically when the object genuinely does not
    /// exist, distinct from every other failure mode (`Io`) — see
    /// `StoreError::NotFound`'s doc comment for why the distinction exists.
    /// `EngineError: From<StoreError>` keeps every `?`-based caller a plain
    /// operational failure, whose message names the cap.
    pub fn get_capped(
        &self,
        key: &str,
        max_bytes: u64,
    ) -> Result<(Vec<u8>, Option<String>), StoreError> {
        let rt = &self.rt;
        rt.block_on(async {
            // The one whole-body GET outside a version read (clippy.toml
            // forbids it everywhere else): its body is taken through
            // `read_within`'s two fences, never collected whole.
            #[allow(clippy::disallowed_methods)]
            let r = self
                .inner
                .get(&OPath::from(key))
                .await
                .map_err(|e| not_found_or_io(key, e))?;
            let r = self.as_answered(r);
            let vid = match &self.versions {
                // The test double's version log: see `versions`.
                Some(log) => log.current(key),
                None => r.meta.version.clone(),
            };
            let bytes = read_within(key, key, r, max_bytes).await?;
            Ok((bytes, vid))
        })
    }

    /// **FX-31 — what the store says about one object, without its body.**
    ///
    /// For an existence test: "is the sidecar there" needs no byte of it, and
    /// reading a whole object to throw it away is exactly the unbounded read
    /// [`Store::get_capped`] exists to end. `NotFound` exactly as
    /// [`Store::get_capped`] answers it.
    ///
    /// A `HEAD` carries no response body, so an S3 denial arrives without its
    /// XML `<Code>`: a caller that must CLASSIFY a refusal (the readiness
    /// probe) keeps a GET with [`caps::PROBE`] instead.
    pub fn head(&self, key: &str) -> Result<ObjectHead, StoreError> {
        let rt = &self.rt;
        rt.block_on(async {
            let meta = self
                .inner
                .head(&OPath::from(key))
                .await
                .map_err(|e| not_found_or_io(key, e))?;
            let version = match &self.versions {
                // The test double's version log: see `versions`.
                Some(log) => log.current(key),
                None => meta.version.clone(),
            };
            Ok(ObjectHead {
                // The test double's size claim: see `misreport`.
                size: self.misreport.as_ref().map_or(meta.size, |m| m.reported),
                version,
            })
        })
    }

    /// **PROD-03.0 — read an object only when it is at most `max_bytes`
    /// long, and only within `within`.** `Ok(None)` when the store reports it
    /// larger: nothing is fetched. Otherwise the bytes of a ranged read of
    /// exactly the size the store reported, so an object that grows between
    /// the two requests is still read to that bound and no further (a
    /// truncated read is the caller's to refuse). Both requests together end
    /// within `within` when it is given — the store's own retries included —
    /// or fail with [`StoreError::Io`] naming the timeout. For a reader that
    /// must never hold an object the adopter's configuration could make
    /// arbitrarily large, nor wait on a degraded store past its own budget —
    /// schema dependency detection at backup time.
    pub fn get_bounded(
        &self,
        key: &str,
        max_bytes: u64,
        within: Option<std::time::Duration>,
    ) -> Result<Option<Vec<u8>>, StoreError> {
        let rt = &self.rt;
        rt.block_on(async {
            let path = OPath::from(key);
            let not_found_or_io = |e: object_store::Error| not_found_or_io(key, e);
            let read = async {
                let size = self.inner.head(&path).await.map_err(not_found_or_io)?.size;
                if size > max_bytes {
                    return Ok(None);
                }
                if size == 0 {
                    return Ok(Some(Vec::new()));
                }
                // PROD-03.0's ranged read, to exactly the size the HEAD
                // reported and only when it is under the caller's cap.
                #[allow(clippy::disallowed_methods)]
                let b = self
                    .inner
                    .get_range(&path, 0..size)
                    .await
                    .map_err(not_found_or_io)?;
                // Moved, not copied, where the buffer is the read's own: the
                // caller holds the stored bytes once, not twice.
                Ok(Some(Vec::<u8>::from(b)))
            };
            match within {
                None => read.await,
                // No budget left: no request is started.
                Some(limit) if limit.is_zero() => Err(StoreError::Io(format!(
                    "{key}: not read: no time is left of the reader's remaining budget"
                ))),
                Some(limit) => tokio::time::timeout(limit, read).await.unwrap_or_else(|_| {
                    Err(StoreError::Io(format!(
                        "{key}: not read within {limit:?}, the reader's remaining budget"
                    )))
                }),
            }
        })
    }

    /// **FX-7 — read ONE VERSION of an object**, by the version id a signed
    /// document pinned, and (FX-31) never more than `max_bytes` of it, by the
    /// same two fences as [`Store::get_capped`]. Returns the bytes and the
    /// version id the store answered with.
    ///
    /// `NotFound` when the store holds no such version of the key: never
    /// written, that version expired, or an id this bucket never issued at all
    /// — which is what a pin taken in ANOTHER bucket is, and the reason the
    /// last case is `NotFound` and not `Io` (see [`version_read_error`]).
    ///
    /// **A store that does not read by version is an ERROR here, never an
    /// answer**: `object_store` 0.14 sends
    /// `?versionId=` to S3 and S3-compatible stores, but its in-memory and
    /// local-filesystem backends ignore the option and return the CURRENT
    /// object — so a reply whose version id is not the one asked for is
    /// refused as [`StoreError::Backend`] rather than handed back as the
    /// pinned bytes. A reader that took the current object for the pinned one
    /// would verify exactly the rewrite the pin exists to detect.
    pub fn get_version_capped(
        &self,
        key: &str,
        version: &str,
        max_bytes: u64,
    ) -> Result<(Vec<u8>, Option<String>), StoreError> {
        if let Some(log) = &self.versions {
            // The test double's version log: see `versions`.
            if let Some(fault) = log.version_read_fault() {
                return Err(StoreError::Io(format!(
                    "{key}?versionId={version}: {fault}"
                )));
            }
            let bytes = log
                .read(key, version)
                .ok_or_else(|| StoreError::NotFound(format!("{key}?versionId={version}")))?;
            let size = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
            if size > max_bytes {
                return Err(StoreError::TooLarge {
                    key: format!("{key}?versionId={version}"),
                    cap: max_bytes,
                    observed: OverCap::Reported(size),
                });
            }
            return Ok((bytes, Some(version.to_string())));
        }
        let rt = &self.rt;
        rt.block_on(async {
            let options = object_store::GetOptions {
                version: Some(version.to_string()),
                ..Default::default()
            };
            // A version read's GET: its body goes through `read_within` too.
            #[allow(clippy::disallowed_methods)]
            let r = self
                .inner
                .get_opts(&OPath::from(key), options)
                .await
                .map_err(|e| version_read_error(key, version, e))?;
            let r = self.as_answered(r);
            let answered = r.meta.version.clone();
            if answered.as_deref() != Some(version) {
                return Err(StoreError::Backend(format!(
                    "a read of version {version} of {key} was answered with version {}; this \
                     store does not read objects by version",
                    answered.as_deref().unwrap_or("none")
                )));
            }
            let at = format!("{key}?versionId={version}");
            let bytes = read_within(key, &at, r, max_bytes).await?;
            Ok((bytes, answered))
        })
    }

    /// Every key under `prefix`, sorted, with no filter of any kind. Added for
    /// Task 20 phase 8, which reads the engine's own validation report out of
    /// the per-run prefix Logweir itself set in `validation.yaml`:
    /// `list_manifest_keys` cannot serve that read because its
    /// `/manifest.json` filter would return nothing there.
    ///
    /// Sorted so a caller taking `keys[0]` gets a deterministic answer rather
    /// than whatever order the backend happened to stream.
    ///
    /// The sort is DEFENSIVE and, honestly, unproven: `object_store` 0.14
    /// contracts no list ordering across backends, but both in-process
    /// backends this workspace can build (`InMemory`, which is a `BTreeMap`,
    /// and `LocalFileSystem`) happen to return keys already ordered. So no
    /// test here can distinguish "sorted by this line" from "sorted by the
    /// backend" — deleting `out.sort()` leaves the suite green (Task 20 fix
    /// round 1, mutant Z4). It is kept because phase 8 takes `keys[0]` and a
    /// backend that streamed in arbitrary order would otherwise make WHICH
    /// engine report is retained non-deterministic; it is documented as
    /// unproven rather than asserted as a tested guarantee.
    pub fn list_keys(&self, prefix: &str) -> Result<Vec<String>, EngineError> {
        use futures::StreamExt as _;
        let rt = &self.rt;
        rt.block_on(async {
            let mut out = Vec::new();
            let mut st = self.inner.list(Some(&OPath::from(prefix)));
            while let Some(m) = st.next().await {
                let m = m.map_err(|e| EngineError::Operational(e.to_string()))?;
                out.push(m.location.to_string());
            }
            out.sort();
            Ok(out)
        })
    }

    /// ONE BOUNDED PAGE of keys under `prefix`, in ascending key order,
    /// starting strictly AFTER `start_after`, together with the cursor that
    /// resumes the walk — `None` when this page exhausted the prefix.
    ///
    /// Decision D3 §5.2: this is the ONE new store capability the recovery
    /// catalog needs, and it deliberately adds no write and no delete. It
    /// exists because [`Store::list_keys`] is an UNBOUNDED, in-memory, sorted
    /// list: a catalog of a hundred thousand points cannot be read by a
    /// controller materialising a bounded Kubernetes view, and a CLI printing
    /// fifty rows must not first hold every key in the bucket.
    ///
    /// # What `max` bounds, and what it does not
    ///
    /// It bounds the RESULT and this call's MEMORY: at most `max` keys are
    /// retained at any moment, whatever the prefix holds.
    ///
    /// It does NOT bound how many objects the backend streams. `object_store`
    /// 0.14.1 contracts no list ordering — "Note: the order of returned
    /// `ObjectMeta` is not guaranteed" sits above `list_with_offset` itself
    /// ([VERIFIED object_store-0.14.1/src/lib.rs:1238-1252]) — so a page that
    /// took the first `max` keys off the stream and called them "the smallest
    /// `max`" would be non-deterministic across backends, and its `next`
    /// cursor could skip keys a later page would then never return. A catalog
    /// that silently loses points is worse than one that lists slowly. The
    /// walk is therefore complete and the SELECTION is bounded, and the design
    /// keeps `n` small by day-sharding the log (D3 §5.2) rather than by
    /// trusting an ordering the crate does not promise.
    ///
    /// `prefix` is evaluated on a PATH SEGMENT basis, which is
    /// `ObjectStore::list`'s own rule and not this method's: `foo/bar` is a
    /// prefix of `foo/bar/x` and **not** of `foo/bar_baz/x` ([VERIFIED
    /// object_store-0.14.1/src/lib.rs:1233-1236]). A caller that wants
    /// "everything whose key starts with this STRING" must list the containing
    /// segment and filter; a partial segment matches nothing at all rather
    /// than matching loosely, which is the safer of the two failure modes and
    /// is why it is written down here.
    ///
    /// `start_after` is EXCLUSIVE, which is `list_with_offset`'s own contract
    /// ("objects at exactly `offset` will not be included", ibid.), so feeding
    /// the returned cursor straight back in returns the next page and never
    /// repeats its last row.
    ///
    /// `max == 0` returns an empty page and no cursor: a caller asking for
    /// nothing is answered with nothing rather than with everything.
    pub fn list_page(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        max: usize,
    ) -> Result<(Vec<String>, Option<String>), EngineError> {
        use futures::StreamExt as _;
        if max == 0 {
            return Ok((Vec::new(), None));
        }
        let rt = &self.rt;
        rt.block_on(async {
            let p = OPath::from(prefix);
            // `list_with_offset` when the caller gave a cursor, `list`
            // otherwise. NOT `list` plus a filter: on S3 and GCS the offset is
            // pushed down into the request, which is the whole reason D3 names
            // this method rather than a slice of `list_keys`.
            let mut st = match start_after {
                Some(after) => self.inner.list_with_offset(Some(&p), &OPath::from(after)),
                None => self.inner.list(Some(&p)),
            };
            // The bounded selection: `page` holds at most `max` keys, sorted
            // ascending, and a key that is not smaller than the largest one
            // held is discarded on arrival once the buffer is full. That is
            // the whole memory bound.
            let mut page: Vec<String> = Vec::with_capacity(max);
            let mut more = false;
            while let Some(m) = st.next().await {
                let m = m.map_err(|e| EngineError::Operational(e.to_string()))?;
                let key = m.location.to_string();
                // `list_with_offset`'s default implementation filters on
                // `location > offset`, but a backend may push the offset down
                // itself; re-asserting it here means the exclusivity is this
                // method's own property on every backend rather than a
                // behaviour inherited from whichever one is configured.
                if start_after.is_some_and(|after| key.as_str() <= after) {
                    continue;
                }
                if page.len() == max {
                    // SAFETY of the index: `max >= 1` and the buffer is full.
                    if key >= page[max - 1] {
                        more = true;
                        continue;
                    }
                    page.pop();
                    more = true;
                }
                let at = page.partition_point(|k| k.as_str() < key.as_str());
                page.insert(at, key);
            }
            // The cursor is the page's LAST key and not the largest key seen:
            // resuming from anything else would skip the keys between them.
            let next = if more { page.last().cloned() } else { None };
            Ok((page, next))
        })
    }

    /// Every key under `prefix` ending `/manifest.json` — exactly what the CLI's
    /// `list` scans (GT-10). Expressed as `list_keys` plus the filter, so the
    /// two can never disagree about what "under this prefix" means.
    pub fn list_manifest_keys(&self, prefix: &str) -> Result<Vec<String>, EngineError> {
        Ok(self
            .list_keys(prefix)?
            .into_iter()
            .filter(|k| k.ends_with("/manifest.json"))
            .collect())
    }

    /// The provider's Object Lock state for `key`, or `None` when the backend
    /// exposes no such readback.
    ///
    /// `object_store` 0.14 — the crate, version and feature set Global
    /// Constraint 9 fixes — models no Object Lock / WORM retention API at all,
    /// on any of its `aws`/`azure`/`gcp`/`http` backends. So this returns
    /// `None` on every backend Logweir can currently build, and phase 8
    /// therefore publishes `evidence.immutable: false`. That is the point:
    /// spec §6 C3 allows `immutable: true` ONLY after a provider readback, so
    /// the absence of a readback must produce an honest `false` rather than an
    /// optimistic guess derived from the bucket's configuration or the
    /// adopter's say-so. If a future object_store exposes the readback, this
    /// method is the one place that changes.
    pub fn object_lock_readback(&self, key: &str) -> Option<LockInfo> {
        let _ = key;
        None
    }

    pub fn list_manifests(&self, loc: &StorageUrl) -> Result<Vec<BackupSetRef>, EngineError> {
        Ok(self
            .list_manifest_keys(loc.prefix())?
            .into_iter()
            .map(|k| BackupSetRef {
                backup_id: backup_id_from_manifest_key(&k),
                manifest_key: k,
            })
            .collect())
    }

    /// The covered window ONE manifest declares, read out of the manifest
    /// BODY. Interface **I12**, for the retention reconciler (spec §5, guard
    /// G-RET), which runs in `weirkeeper` and therefore cannot call
    /// `OsoCliEngine::describe`.
    ///
    /// An untyped `serde_json::Value` read of the manifest body's
    /// covered-window fields. Links NO vendored upstream struct — the same
    /// untyped read, for the same reason, that `segments_in_manifest` states
    /// in its own doc comment further down this file. The field paths are
    /// `topics[].partitions[].segments[].{start_timestamp,end_timestamp}`,
    /// two of the three that method already reads.
    ///
    /// `newest_record_ms` is the MAXIMUM `end_timestamp` and `oldest_record_ms`
    /// the MINIMUM `start_timestamp` over every segment of every partition of
    /// every topic — the whole manifest, unfiltered, because a backup set's
    /// window is the union of its segments' windows and not any one topic's.
    /// Both come from the BODY: the key carries no timestamp, so deriving
    /// either from the key string would report `0`.
    ///
    /// `backup_id` is the manifest key's parent directory, derived by the ONE
    /// function [`backup_id_from_manifest_key`] that `list_manifests` also
    /// calls, so the two cannot disagree about which backup set a manifest
    /// belongs to. (Task 13 review carry: it used to be two copies of the same
    /// four chained calls, under a doc comment promising they agreed.)
    ///
    /// TWO DIFFERENT FAILURES, TWO DIFFERENT ERRORS (Task 13 review carry).
    /// A body that carries no `topics` array — `{"hello":"world"}`, or
    /// `{"topics":5}` where the key exists but is not an array — is not a
    /// manifest at all, and answers [`StoreError::NotAManifest`]. A body that
    /// IS manifest-shaped but whose segments bound no window answers
    /// `Backend`, because "this backup set declares no segment" is a fact
    /// about a real set and not a configuration mistake. Collapsing the two,
    /// which is what this method did, reported a prefix pointing at the wrong
    /// objects identically to an empty backup set.
    ///
    /// A manifest with no segment bounds no window, and saying so is the only
    /// honest answer: an empty min/max would be published as a real window.
    ///
    /// # FX-31: read under the caller's cap, and parsed as a STREAM
    ///
    /// `max_bytes` is the reader's cap ([`caps::CONTROLLER_MANIFEST`] in the
    /// controller's retention report); a manifest over it is
    /// [`StoreError::TooLarge`], which the report lists under `skipped` —
    /// neither kept nor removable. The window is folded while the body is
    /// parsed ([`manifest_window`]) and no `serde_json::Value` of the manifest
    /// is ever built, so the memory this takes is the capped bytes and no
    /// more: a document of tiny values (`[0,0,0,…]`) costs a `Value` tree
    /// about 37 times its own size (16 MiB of JSON held 621 MB, measured by
    /// `crates/weirkeeper/tests/read_caps.rs`), in the one process every
    /// namespace shares. The answers are the ones the earlier `Value` walk
    /// gave, byte for byte, including which duplicate key wins (the last).
    pub fn manifest_facts(&self, key: &str, max_bytes: u64) -> Result<ManifestFacts, StoreError> {
        let (bytes, _) = self.get_capped(key, max_bytes)?;
        let (oldest_record_ms, newest_record_ms) = manifest_window(key, &bytes)?;
        Ok(ManifestFacts {
            backup_id: backup_id_from_manifest_key(key),
            newest_record_ms,
            oldest_record_ms,
        })
    }

    /// Pure window filter, extracted so it is testable with no backend.
    pub fn segment_keys_from(&self, segs: &[(String, i64, i64)], w: (i64, i64)) -> Vec<String> {
        segs.iter()
            .filter(|(_, t0, t1)| *t0 <= w.1 && *t1 >= w.0)
            .map(|(k, _, _)| k.clone())
            .collect()
    }

    /// Qualifies a manifest-relative key into the fully-qualified key
    /// `get`/`list_manifest_keys` operate on.
    ///
    /// Upstream stores every key a manifest carries — `manifest_key` itself
    /// (`{backup_id}/manifest.json` [VERIFIED
    /// U/kafka-backup/crates/kafka-backup-core/src/backup/engine.rs:1599]) and
    /// every segment key (`{backup_id}/topics/{topic}/partition={n}/
    /// segment-{offset:020}.bin{ext}` [VERIFIED .../backup/engine.rs:1436-1442])
    /// — RELATIVE to the configured prefix, prepending that prefix only at the
    /// storage boundary (`S3Backend::full_path` [VERIFIED
    /// .../storage/s3.rs:101-104]: `format!("{}/{}", prefix.trim_end_matches('/'),
    /// key)`). `list_manifest_keys` and `get` in THIS file already operate in
    /// the fully-qualified space — the `prefix` handed to `object_store::list`
    /// IS the search root — so a key read out of a manifest BODY must be
    /// qualified the same way before `get` can resolve it. Fixed after a
    /// review found `segment_keys_for` passing the manifest's relative key
    /// straight through: a real archive at `s3://bucket` with a non-empty
    /// `prefix` would have every returned segment key 404 in `get`, reporting
    /// a present backup's segments as missing.
    ///
    /// `pub` since Task 21c: `OsoCliEngine::describe` must qualify the segment
    /// keys it lifts out of a manifest body for exactly the same reason
    /// `segments_in_manifest` does, and it lives in a different module.
    pub fn qualify(&self, relative_key: &str) -> String {
        if self.prefix.is_empty() {
            relative_key.to_string()
        } else {
            format!("{}/{}", self.prefix.trim_end_matches('/'), relative_key)
        }
    }

    /// **FX-16 (review M-1).** The bucket-absolute key the ENGINE reads backup
    /// set `backup_id`'s manifest at, for a plan whose storage is this
    /// store's: `<prefix>/<backup_id>/manifest.json`, normalised as an
    /// `object_store` path, in the form [`Store::list_keys`] returns keys.
    ///
    /// That is exactly what the pinned engine resolves: its restore loads
    /// `format!("{}/manifest.json", backup_id)` (kafka-backup 0.23.3
    /// `restore/engine.rs:1086`) through `S3Backend::full_path`, which is
    /// `Path::from(format!("{}/{}", prefix.trim_end_matches('/'), key))`
    /// (`storage/s3.rs:121-126`), or `base_path.join(key)` on a filesystem.
    /// The engine is never told any other key, so a listing's choice of a
    /// same-id manifest elsewhere under the prefix is not what it restores.
    #[must_use]
    pub fn engine_manifest_key(&self, backup_id: &str) -> String {
        OPath::from(self.qualify(&format!("{backup_id}/manifest.json"))).to_string()
    }

    /// Resolves `topic`/`partition` against ONE manifest's body and returns
    /// its (qualified key, start_timestamp, end_timestamp) triples,
    /// unfiltered by any time window. Shared by `segment_keys_for` (scans
    /// every manifest under the prefix — kept for callers that genuinely want
    /// every backup set, none as of this crate) and `segment_keys_for_set`
    /// (reads exactly the one manifest a caller names — what
    /// `OsoCliEngine::fingerprints` uses, per the Task 12 fix that closed the
    /// cross-set merging hole: two backup sets sharing a topic/partition with
    /// an overlapping window used to merge silently here).
    ///
    /// Parsed as `serde_json::Value` rather than `crate::vendored::manifest::
    /// BackupManifest`: Task 12b executes BEFORE Task 12, so the vendored types
    /// do not exist yet. The three field paths read here — topics[].name,
    /// .partitions[].partition_id, .segments[].{key,start_timestamp,
    /// end_timestamp} — are the same ones Task 12's `describe()` maps. Each
    /// `key` is manifest-relative (see `qualify`) and is qualified into this
    /// store's key space before being returned, so the caller can `get` it
    /// directly.
    fn segments_in_manifest(
        &self,
        manifest_key: &str,
        topic: &str,
        partition: i32,
    ) -> Result<Vec<(String, i64, i64)>, EngineError> {
        let mut segs: Vec<(String, i64, i64)> = Vec::new();
        // FX-31: a runner-side read, under the manifest cap.
        let (bytes, _) = self.get_capped(manifest_key, caps::MANIFEST)?;
        let v: serde_json::Value = serde_json::from_slice(&bytes)
            .map_err(|e| EngineError::Operational(format!("{manifest_key}: {e}")))?;
        let topics = v
            .get("topics")
            .and_then(|t| t.as_array())
            .map(|a| a.as_slice())
            .unwrap_or(&[]);
        for t in topics {
            if t.get("name").and_then(|n| n.as_str()) != Some(topic) {
                continue;
            }
            let parts = t
                .get("partitions")
                .and_then(|p| p.as_array())
                .map(|a| a.as_slice())
                .unwrap_or(&[]);
            for p in parts {
                if p.get("partition_id").and_then(|i| i.as_i64()) != Some(partition as i64) {
                    continue;
                }
                let ss = p
                    .get("segments")
                    .and_then(|s| s.as_array())
                    .map(|a| a.as_slice())
                    .unwrap_or(&[]);
                for s in ss {
                    let (Some(k), Some(t0), Some(t1)) = (
                        s.get("key").and_then(|k| k.as_str()),
                        s.get("start_timestamp").and_then(|x| x.as_i64()),
                        s.get("end_timestamp").and_then(|x| x.as_i64()),
                    ) else {
                        return Err(EngineError::Operational(format!(
                            "{manifest_key}: segment entry missing key/start_timestamp/end_timestamp"
                        )));
                    };
                    segs.push((self.qualify(k), t0, t1));
                }
            }
        }
        Ok(segs)
    }

    /// The Interfaces-block method from Task 12b's brief. Resolves
    /// `topic`/`partition` against EVERY manifest under this store's prefix.
    /// No caller in this workspace uses this anymore as of the Task 12 fix
    /// (see `segment_keys_for_set`) — kept because it is part of Task 12b's
    /// committed, tested Interfaces contract, and a future caller that
    /// genuinely wants a cross-set view (e.g. an archive-wide audit) has a
    /// real use for it. `OsoCliEngine::fingerprints` MUST NOT call this one.
    pub fn segment_keys_for(
        &self,
        topic: &str,
        partition: i32,
        window: (i64, i64),
    ) -> Result<Vec<String>, EngineError> {
        let mut segs: Vec<(String, i64, i64)> = Vec::new();
        for mk in self.list_manifest_keys(&self.prefix)? {
            segs.extend(self.segments_in_manifest(&mk, topic, partition)?);
        }
        let mut out = self.segment_keys_from(&segs, window);
        out.sort();
        out.dedup();
        Ok(out)
    }

    /// Task 12 fix (post-review): the set-scoped sibling of `segment_keys_for`.
    /// Reads exactly the ONE manifest named by `manifest_key` — never lists or
    /// touches any other manifest under this store's prefix — so two backup
    /// sets sharing a topic/partition with an overlapping window can no
    /// longer merge: `fingerprints()` calls this, passing
    /// `sel.set.manifest_key`, instead of `segment_keys_for`.
    pub fn segment_keys_for_set(
        &self,
        manifest_key: &str,
        topic: &str,
        partition: i32,
        window: (i64, i64),
    ) -> Result<Vec<String>, EngineError> {
        let segs = self.segments_in_manifest(manifest_key, topic, partition)?;
        let mut out = self.segment_keys_from(&segs, window);
        out.sort();
        out.dedup();
        Ok(out)
    }

    /// Global Constraint 6: `PutMode::Create` everywhere, under `logweir/` only.
    pub fn put_create_only(&self, key: &str, bytes: &[u8]) -> Result<PutOutcome, StoreError> {
        // Controller amendment: checked before anything else, including the
        // LOGWEIR_ROOT assertion below. A handle from `read_only_from_url` is
        // built over the OSO archive prefix precisely so it CAN read that
        // prefix, so it must never reach an assertion that would panic on
        // that same prefix — it must simply refuse to put, every time.
        if self.read_only {
            return Err(StoreError::ReadOnly(key.to_string()));
        }
        // NOT `key.starts_with(&self.prefix)` as the only test: `self.prefix`
        // is caller-supplied from the evidence StorageUrl, so that disjunct is
        // satisfied by ANY prefix a spec names — including the OSO archive
        // prefix, which is the one outcome Global Constraint 6 exists to
        // prevent. The sanctioned root is fixed in code and checked FIRST.
        assert!(
            key.starts_with(LOGWEIR_ROOT),
            "Global Constraint 6: logweir writes only under `{LOGWEIR_ROOT}`, got `{key}`"
        );
        assert!(
            key.starts_with(&self.prefix),
            "key `{key}` escapes this store's configured prefix `{}`",
            self.prefix
        );
        let rt = &self.rt;
        rt.block_on(async {
            let p = OPath::from(key);
            let payload = object_store::PutPayload::from(bytes.to_vec());
            if self.ignores_create_mode {
                // The test double's lie: see `ignores_create_mode`.
                let r = self
                    .inner
                    .put(&p, payload)
                    .await
                    .map_err(|e| StoreError::Io(e.to_string()))?;
                return Ok(PutOutcome {
                    version_id: r.version,
                    create_only_enforced: true,
                });
            }
            if self.conditional_put {
                let opts = PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                };
                match self.inner.put_opts(&p, payload.clone(), opts).await {
                    Ok(r) => {
                        let version_id = match &self.versions {
                            // The test double's version log: see `versions`.
                            Some(log) => Some(log.record(key, bytes)),
                            None => r.version,
                        };
                        return Ok(PutOutcome {
                            version_id,
                            create_only_enforced: true,
                        });
                    }
                    Err(object_store::Error::AlreadyExists { .. }) => {
                        if self.errors_on_existing_key {
                            // The test double's fault: see `errors_on_existing_key`.
                            return Err(StoreError::Io(format!(
                                "{key}: injected transport error on an existing key"
                            )));
                        }
                        return Err(StoreError::AlreadyExists(key.to_string()));
                    }
                    // The backend does not implement conditional put. Fall
                    // through to HEAD-then-PUT and RECORD that we did.
                    //
                    // Two distinct object_store variants mean this, not one:
                    // `NotSupported` is what a backend that never implements
                    // conditional put at all would return (object_store 0.14.1
                    // only actually produces it for copy-if-not-exists
                    // [VERIFIED object_store-0.14.1/src/aws/mod.rs:399]).
                    // `NotImplemented` is what `AmazonS3` ACTUALLY returns for
                    // `PutMode::Create` when `AWS_CONDITIONAL_PUT=disabled` (or
                    // the equivalent builder config) — reachable through
                    // `AmazonS3Builder::from_env()` on a real S3-compatible
                    // endpoint [VERIFIED
                    // object_store-0.14.1/src/aws/mod.rs:186-192]. Without this
                    // arm the put fails closed with `StoreError::Io` instead of
                    // falling back — safe, but it means
                    // `create_only_enforced: false` could only ever be
                    // observed against the in-memory test double, never
                    // against the real backend this fallback exists for.
                    Err(object_store::Error::NotSupported { .. })
                    | Err(object_store::Error::NotImplemented { .. }) => {}
                    Err(e) => return Err(StoreError::Io(e.to_string())),
                }
            }
            if self.inner.head(&p).await.is_ok() {
                return Err(StoreError::AlreadyExists(key.to_string()));
            }
            let r = self
                .inner
                .put(&p, payload)
                .await
                .map_err(|e| StoreError::Io(e.to_string()))?;
            Ok(PutOutcome {
                version_id: r.version,
                create_only_enforced: false,
            })
        })
    }
}

/// One key's history in a [`VersionLog`]: `(version id, bytes)`, oldest first.
type VersionHistory = Vec<(String, Vec<u8>)>;

/// TEST DOUBLE ONLY (FX-7): the version history of [`Store::in_memory_versioned`].
///
/// Every write appends `(version id, bytes)` to its key's history, and the
/// current version is the last entry — S3's model of a versioned bucket, less
/// the delete marker, which this crate does not model because it names no
/// object-store delete at all (G-RET, `scripts/check-no-archive-write.sh`).
#[doc(hidden)]
#[derive(Debug, Default)]
pub struct VersionLog {
    /// Every key's history.
    state: std::sync::Mutex<std::collections::BTreeMap<String, VersionHistory>>,
    /// When set, every read BY VERSION fails with this text as `Io` — a
    /// principal without `s3:GetObjectVersion`, or a transport failure on the
    /// one extra read a reader makes (FX-7 fix round). Never set by a
    /// production path.
    version_read_fault: std::sync::Mutex<Option<String>>,
}

/// The id counter EVERY [`VersionLog`] in this process draws from.
///
/// **A version id is a property of one object in ONE bucket** (FX-7 fix round,
/// review H-1), and a real store never issues an id another bucket issued. A
/// per-bucket counter would: two doubles written in the same order hand out
/// the same ids, and a byte-for-byte COPY of a pinned point would then find
/// its pin among the copy's own versions by coincidence — a test of "the pin
/// is not this bucket's" that could not fail.
static NEXT_VERSION_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

impl VersionLog {
    fn record(&self, key: &str, bytes: &[u8]) -> String {
        let n = NEXT_VERSION_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
        let id = format!("fx7v{n:06}");
        self.state
            .lock()
            .expect("the version log is never poisoned")
            .entry(key.to_string())
            .or_default()
            .push((id.clone(), bytes.to_vec()));
        id
    }

    fn version_read_fault(&self) -> Option<String> {
        self.version_read_fault
            .lock()
            .expect("the version log is never poisoned")
            .clone()
    }

    fn current(&self, key: &str) -> Option<String> {
        self.state
            .lock()
            .expect("the version log is never poisoned")
            .get(key)
            .and_then(|h| h.last())
            .map(|(id, _)| id.clone())
    }

    fn read(&self, key: &str, version: &str) -> Option<Vec<u8>> {
        self.state
            .lock()
            .expect("the version log is never poisoned")
            .get(key)?
            .iter()
            .find(|(id, _)| id == version)
            .map(|(_, bytes)| bytes.clone())
    }

    fn history(&self, key: &str) -> Vec<String> {
        self.state
            .lock()
            .expect("the version log is never poisoned")
            .get(key)
            .map(|h| h.iter().map(|(id, _)| id.clone()).collect())
            .unwrap_or_default()
    }
}

/// TEST DOUBLE ONLY (FX-7): a second writer on a [`Store::in_memory_versioned`]
/// bucket — the engine's unconditional manifest put — which the `Store`
/// itself, being create-only, cannot be.
#[doc(hidden)]
pub struct VersionedBucket {
    backend: Arc<object_store::memory::InMemory>,
    log: Arc<VersionLog>,
    rt: Arc<tokio::runtime::Runtime>,
}

impl VersionedBucket {
    /// An unconditional put: a NEW current version of `key`. Returns its id.
    ///
    /// Held to Global Constraint 6's root like every write this crate makes:
    /// a key outside `logweir/` panics here, as it does in `put_create_only`,
    /// so the double cannot become a way to model writes anywhere else.
    pub fn overwrite(&self, key: &str, bytes: &[u8]) -> String {
        assert!(
            key.starts_with(LOGWEIR_ROOT),
            "Global Constraint 6: the versioned double writes only under `{LOGWEIR_ROOT}`, got \
             `{key}`"
        );
        self.rt
            .block_on(self.backend.put(
                &OPath::from(key),
                object_store::PutPayload::from(bytes.to_vec()),
            ))
            .expect("the in-memory backend accepts every put");
        self.log.record(key, bytes)
    }

    /// Every version id `key` has had, oldest first.
    #[must_use]
    pub fn versions(&self, key: &str) -> Vec<String> {
        self.log.history(key)
    }

    /// From now on every read BY VERSION through the store fails as
    /// `StoreError::Io` carrying `text` (a real `object_store` message shape):
    /// the "could not tell" answer a reader must not take for "this bucket
    /// does not hold the version". Plain reads are untouched.
    pub fn fail_version_reads(&self, text: &str) {
        *self
            .log
            .version_read_fault
            .lock()
            .expect("the version log is never poisoned") = Some(text.to_string());
    }
}

/// TEST DOUBLE ONLY (FX-31): how many body bytes readers took from a
/// [`Store::in_memory_misreporting_size`] store's GET streams, in total.
#[doc(hidden)]
#[derive(Clone, Debug, Default)]
pub struct StreamMeter(Arc<std::sync::atomic::AtomicU64>);

impl StreamMeter {
    /// The body bytes taken so far.
    #[must_use]
    pub fn streamed(&self) -> u64 {
        self.0.load(std::sync::atomic::Ordering::SeqCst)
    }
}

// ===========================================================================
// EXPLICIT STORE CONSTRUCTION (decision D2 W2)
// ===========================================================================
// Explicit store construction (decision D2 W2, §3.5, §3.10, §4.2).
//
// # Why this module exists
//
// `AmazonS3Builder::from_env()` evaluates EVERY `AWS_*` variable in the
// process environment (object_store 0.14.1 `aws/builder.rs:606-617`). That is
// the right default for a single-destination installation and the wrong one
// for anything else, and it is the mechanism behind tracker defect
// **SEC-ENVHTTP**: a forwarded `AWS_ALLOW_HTTP=true` enables plaintext
// transport even when the approved plan says `allow_http: false`. A global
// setting must never override approved execution inputs (D-SEAMS S5).
//
// So this module adds a SECOND way to build a store, beside the existing
// `from_url` / `read_only_from_url` (which are unchanged, still read the
// environment, and still serve legacy inline objects):
//
// * [`CredentialSource`] says exactly where the credential comes from —
//   explicit static values, static values read from three NAMED variables and
//   nothing else, an injected workload identity ONLY, or today's ambient
//   chain.
// * every addressing and transport value comes from the [`StorageUrl`] the
//   caller passes, and overrides whatever the environment says.
// * [`StoreOptions::pin_instance_metadata`] points the instance-metadata
//   endpoint at a dead loopback address, so a missing workload identity can
//   never fall back to the node's instance role (D2 G16).
// * [`StoreOptions::root_certificates`] carries a destination's private CA.
//
// # How the guarantee is testable without a socket
//
// [`s3_effective`] is the ONE function that decides what a store will be
// built with, and [`Store::from_url_with`] builds the client by
// consuming its output. A test can therefore read the decision directly
// rather than modelling it a second time — a parallel model is exactly how a
// test comes to assert something the production path does not do.
//
// The claim holds for EVERY credential source, including
// [`CredentialSource::Ambient`], and that is a fix: the first version of this
// module built `Ambient` from `AmazonS3Builder::from_env()`, which reads every
// `AWS_*` variable, so a destination naming no endpoint inherited
// `AWS_ENDPOINT_URL` from the controller's own environment while
// `s3_effective` reported `endpoint: None`. D2 §3.10 builds the
// `ControllerIdentity` evidence cache with exactly that source, so a
// `BackupDestination` on plain AWS S3 would have had its evidence read from
// the controller's MinIO bucket — G2's "global configuration leakage", the
// defect PLAT-08.1 exists to close, arriving through the fix for it.
//
// No source calls `from_env()` now. Every source starts from
// `AmazonS3Builder::new()` and takes its location and route from the
// `StorageUrl` alone; what differs between them is WHICH NAMED credential
// variables they are allowed to read, and `S3Effective::environment_variables_read`
// lists exactly those. `from_url` and `read_only_from_url` are untouched and
// still read the whole environment — they are the legacy inline path.

use std::time::Duration;

/// The message prefix a refusal carries when a `WorkloadIdentity` store finds
/// no injected identity. D2 §3.5: the runner fails CLOSED rather than falling
/// through to a node role.
pub const WORKLOAD_IDENTITY_NOT_INJECTED: &str = "WorkloadIdentityNotInjected";

/// A dead loopback address. Port 1 is `tcpmux`, which nothing in a runner or
/// controller image listens on, so a credential chain that reaches instance
/// metadata gets an immediate connection refusal instead of a node role.
pub const DEAD_METADATA_ENDPOINT: &str = "http://127.0.0.1:1";

/// Where the object-store credential comes from.
///
/// `Debug` is HAND-WRITTEN below: the `Static` variant holds a secret access
/// key and a session token, and `StoreOptions` derives `Debug`, so one
/// `tracing` `?opts` or one `expect(&format!("{opts:?}"))` in W4, W7 or W10
/// would put an AWS secret into a runner or controller log. The struct went
/// out of its way to keep the secret out of [`S3Effective`]; this is the other
/// door.
#[derive(Clone, Default, PartialEq, Eq)]
pub enum CredentialSource {
    /// object_store's own credential ORDER — static keys, then web identity,
    /// then container credentials, then instance metadata — over the NAMED
    /// credential variables ([`STATIC_CREDENTIAL_VARS`],
    /// [`WORKLOAD_IDENTITY_VARS`], [`AMBIENT_METADATA_VAR`]) and over nothing
    /// else. What the controller's allowlisted `ControllerIdentity` evidence
    /// reads use (D2 §3.10).
    ///
    /// It is a CREDENTIAL source and not a configuration source: location,
    /// region, addressing and transport come from the `StorageUrl` even here.
    /// `AWS_ENDPOINT_URL` and `AWS_REGION` are read by neither this source nor
    /// any other — `from_url` and `read_only_from_url` are the legacy path
    /// that still honours them.
    #[default]
    Ambient,
    /// Explicit values the caller already holds — the shape the restore
    /// runner's evidence store uses when its grant differs from the archive
    /// grant (`LOGWEIR_EVIDENCE_AWS_*`, D2 §3.5).
    Static {
        access_key_id: String,
        secret_access_key: String,
        session_token: Option<String>,
    },
    /// The three NAMED variables `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
    /// and `AWS_SESSION_TOKEN`, and nothing else. This is the destination
    /// -backed Job's archive credential: the kubelet projects those three from
    /// the grant's Secret, and no other `AWS_*` variable may influence the
    /// store.
    StaticFromEnv,
    /// An injected workload identity ONLY: `AWS_WEB_IDENTITY_TOKEN_FILE` plus
    /// `AWS_ROLE_ARN`, or `AWS_CONTAINER_CREDENTIALS_FULL_URI` /
    /// `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI` plus its token file. Static
    /// keys present in the environment are IGNORED — otherwise they would take
    /// precedence over the identity the operator asked for (object_store's
    /// chain puts static first) — and an absent injection is a refusal.
    WorkloadIdentity,
}

impl std::fmt::Debug for CredentialSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ambient => f.write_str("Ambient"),
            Self::Static {
                access_key_id,
                session_token,
                ..
            } => f
                .debug_struct("Static")
                // The access key id is the PUBLIC half — it travels in every
                // signed request and naming it is how an operator tells which
                // principal was used. The secret and the session token are
                // never rendered, not even truncated.
                .field("access_key_id", access_key_id)
                .field("secret_access_key", &"[redacted]")
                .field(
                    "session_token",
                    &if session_token.is_some() {
                        "[redacted]"
                    } else {
                        "absent"
                    },
                )
                .finish(),
            Self::StaticFromEnv => f.write_str("StaticFromEnv"),
            Self::WorkloadIdentity => f.write_str("WorkloadIdentity"),
        }
    }
}

/// Which credential provider a built store will use. The observable half of
/// [`CredentialSource`], with the secret removed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CredentialKind {
    Ambient,
    Static,
    WorkloadIdentity,
}

/// Everything about a store that is NOT its location.
#[derive(Debug, Clone)]
pub struct StoreOptions {
    pub credentials: CredentialSource,
    /// PEM bundles to trust IN ADDITION to the platform store. Each is parsed
    /// with `object_store::Certificate::from_pem_bundle`, so a file holding a
    /// chain works.
    pub root_certificates: Vec<Vec<u8>>,
    /// Point the instance-metadata endpoint at [`DEAD_METADATA_ENDPOINT`].
    /// Defaults to `true` for every explicit credential source and `false` for
    /// [`CredentialSource::Ambient`], which is the one case where an instance
    /// role may legitimately be what the operator configured.
    pub pin_instance_metadata: Option<bool>,
    /// An overall request timeout, applied as both `ClientOptions::with_timeout`
    /// and `with_connect_timeout`. A check has a budget; without one,
    /// object_store's default retry window is three minutes.
    pub request_timeout: Option<Duration>,
    /// `Some(0)` disables retries. `None` keeps object_store's default of 10.
    ///
    /// Setting this does NOT shorten the retry window on its own: the window
    /// is [`StoreOptions::retry_timeout`], which defaults to object_store's
    /// own 180 s. The first version of this struct silently overwrote the
    /// window with `request_timeout` (or 30 s) whenever `max_retries` was set,
    /// which is a coupling a caller reading the field name could not have
    /// guessed.
    pub max_retries: Option<usize>,
    /// The maximum time from the initial request after which no further retry
    /// is attempted. `None` keeps object_store's default of 180 s. Only read
    /// when [`StoreOptions::max_retries`] is set, because that is the only
    /// case in which this crate builds a `RetryConfig` at all.
    pub retry_timeout: Option<Duration>,
}

impl Default for StoreOptions {
    fn default() -> Self {
        Self {
            credentials: CredentialSource::Ambient,
            root_certificates: Vec::new(),
            pin_instance_metadata: None,
            request_timeout: None,
            max_retries: None,
            retry_timeout: None,
        }
    }
}

impl StoreOptions {
    /// Today's behaviour, named: the ambient chain over the whole `AWS_*`
    /// environment.
    #[must_use]
    pub fn ambient() -> Self {
        Self::default()
    }

    /// The three projected variables and nothing else.
    #[must_use]
    pub fn static_from_env() -> Self {
        Self {
            credentials: CredentialSource::StaticFromEnv,
            ..Self::default()
        }
    }

    /// An injected workload identity only.
    #[must_use]
    pub fn workload_identity() -> Self {
        Self {
            credentials: CredentialSource::WorkloadIdentity,
            ..Self::default()
        }
    }

    /// Explicit values.
    #[must_use]
    pub fn static_keys(
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
        session_token: Option<String>,
    ) -> Self {
        Self {
            credentials: CredentialSource::Static {
                access_key_id: access_key_id.into(),
                secret_access_key: secret_access_key.into(),
                session_token,
            },
            ..Self::default()
        }
    }

    #[must_use]
    pub fn with_root_certificate(mut self, pem: Vec<u8>) -> Self {
        self.root_certificates.push(pem);
        self
    }

    #[must_use]
    pub fn with_request_timeout(mut self, d: Duration) -> Self {
        self.request_timeout = Some(d);
        self
    }

    #[must_use]
    pub fn with_max_retries(mut self, n: usize) -> Self {
        self.max_retries = Some(n);
        self
    }

    /// See [`StoreOptions::retry_timeout`]. Independent of
    /// [`StoreOptions::with_request_timeout`] on purpose.
    #[must_use]
    pub fn with_retry_timeout(mut self, d: Duration) -> Self {
        self.retry_timeout = Some(d);
        self
    }

    /// Whether the instance-metadata endpoint is pinned, after defaulting.
    #[must_use]
    pub fn pins_instance_metadata(&self) -> bool {
        self.pin_instance_metadata
            .unwrap_or(!matches!(self.credentials, CredentialSource::Ambient))
    }

    /// Whether building a store with these options reads the process
    /// environment at all. EVERY source reads a NAMED list and nothing else
    /// (see [`S3Effective::environment_variables_read`] for the exact one);
    /// [`CredentialSource::Static`] reads no variable at all.
    #[must_use]
    pub fn reads_environment(&self) -> bool {
        !matches!(self.credentials, CredentialSource::Static { .. })
    }
}

/// An injected workload identity, as found in the environment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkloadIdentity {
    /// IRSA: `AWS_WEB_IDENTITY_TOKEN_FILE` + `AWS_ROLE_ARN`.
    WebIdentity {
        token_file: String,
        role_arn: String,
        session_name: Option<String>,
        sts_endpoint: Option<String>,
    },
    /// EKS Pod Identity: `AWS_CONTAINER_CREDENTIALS_FULL_URI` +
    /// `AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE`.
    ContainerFullUri { uri: String, token_file: String },
    /// ECS task role: `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`.
    ContainerRelativeUri { uri: String },
}

/// EXACTLY what a store will be built with. Read by a test, and consumed by
/// [`Store::from_url_with`] — one decision, two readers, so a test
/// cannot assert something the production path does not do.
///
/// The SECRET is deliberately absent: `access_key_id` is the public half of a
/// static credential (it appears in every signed request), and nothing here
/// carries the secret access key or a session token value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3Effective {
    pub bucket: String,
    pub prefix: String,
    pub region: Option<String>,
    pub endpoint: Option<String>,
    /// `true` renders `AWS_VIRTUAL_HOSTED_STYLE_REQUEST=true`; it is the
    /// NEGATION of the `StorageUrl`'s `path_style`, and it comes from there
    /// and from nowhere else.
    pub virtual_hosted_style: bool,
    /// Comes from the `StorageUrl`'s `allow_http`, which a destination derives
    /// from `transport.security` alone. NEVER from `AWS_ALLOW_HTTP` and never
    /// from the addressing style.
    pub allow_http: bool,
    pub credentials: CredentialKind,
    /// The public half of a static credential, for a log line that says WHICH
    /// principal was used. `None` for every other source.
    pub access_key_id: Option<String>,
    pub session_token_present: bool,
    pub workload_identity: Option<WorkloadIdentity>,
    pub metadata_endpoint: Option<String>,
    pub root_certificate_count: usize,
    pub request_timeout: Option<Duration>,
    pub max_retries: Option<usize>,
    pub retry_timeout: Option<Duration>,
    /// `true` when construction consults the process environment.
    pub reads_environment: bool,
    /// EXACTLY which environment variables construction may read, sorted.
    ///
    /// This is the observable form of "no unnamed `AWS_*` variable reaches an
    /// explicit store": the list is finite, it is a field a test can assert
    /// on, and no code path outside [`s3_effective`] and the matching arm of
    /// `Store::build_backend_with` reads anything else. Empty for
    /// [`CredentialSource::Static`], which consults nothing.
    pub environment_variables_read: Vec<&'static str>,
}

/// The three variables a projected static credential occupies (D2 §3.5).
pub const STATIC_CREDENTIAL_VARS: [&str; 3] = [
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
];

/// The variables an injected workload identity occupies. object_store's own
/// chain reads these; this crate copies them ONE BY ONE rather than letting
/// `from_env()` sweep the environment, so a variable that is not on this list
/// cannot reach the client.
pub const WORKLOAD_IDENTITY_VARS: [&str; 7] = [
    "AWS_WEB_IDENTITY_TOKEN_FILE",
    "AWS_ROLE_ARN",
    "AWS_ROLE_SESSION_NAME",
    "AWS_ENDPOINT_URL_STS",
    "AWS_CONTAINER_CREDENTIALS_FULL_URI",
    "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
    "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
];

/// The instance-metadata endpoint, read only by [`CredentialSource::Ambient`]
/// and only when the caller did not pin it. `AWS_ENDPOINT_URL`, `AWS_REGION`,
/// `AWS_ALLOW_HTTP` and `AWS_VIRTUAL_HOSTED_STYLE_REQUEST` are deliberately
/// NOT here and are on no list: location and route come from the `StorageUrl`
/// and from nothing else.
pub const AMBIENT_METADATA_VAR: &str = "AWS_METADATA_ENDPOINT";

fn env_value(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

/// The injected identity, or `None`. The NAMED list is the whole list: a
/// variable not on it cannot influence a `WorkloadIdentity` store.
#[must_use]
pub fn workload_identity_from_env() -> Option<WorkloadIdentity> {
    if let (Some(token_file), Some(role_arn)) = (
        env_value("AWS_WEB_IDENTITY_TOKEN_FILE"),
        env_value("AWS_ROLE_ARN"),
    ) {
        return Some(WorkloadIdentity::WebIdentity {
            token_file,
            role_arn,
            session_name: env_value("AWS_ROLE_SESSION_NAME"),
            sts_endpoint: env_value("AWS_ENDPOINT_URL_STS"),
        });
    }
    if let (Some(uri), Some(token_file)) = (
        env_value("AWS_CONTAINER_CREDENTIALS_FULL_URI"),
        env_value("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE"),
    ) {
        return Some(WorkloadIdentity::ContainerFullUri { uri, token_file });
    }
    env_value("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI")
        .map(|uri| WorkloadIdentity::ContainerRelativeUri { uri })
}

/// Resolves a `StorageUrl` plus [`StoreOptions`] into the exact configuration
/// a store will carry.
///
/// Refuses a non-S3 `StorageUrl` with an explicit credential source: a
/// `BackupDestination` is S3-only (D2 §3.1 `provider: S3`), and silently
/// ignoring an explicit credential on an Azure or filesystem URL would build a
/// store with a credential nobody asked for.
pub fn s3_effective(u: &StorageUrl, opts: &StoreOptions) -> Result<S3Effective, StoreError> {
    let StorageUrl::S3 {
        bucket,
        prefix,
        region,
        endpoint,
        path_style,
        allow_http,
    } = u
    else {
        return Err(StoreError::Backend(format!(
            "explicit store options apply to `s3://` locations only; a destination is \
             `provider: S3` (got {})",
            backend_name(u)
        )));
    };
    // FX-20 fix round (review F1): the builder below takes the region as it
    // is, and with no endpoint the region is the host.
    refuse_invalid_region(region.as_deref())?;

    // Which NAMED variables this source may read, and what the credential
    // chain resolves to. Nothing here looks at `AWS_ENDPOINT_URL`,
    // `AWS_REGION`, `AWS_ALLOW_HTTP` or `AWS_VIRTUAL_HOSTED_STYLE_REQUEST`:
    // location and route come from the `StorageUrl` alone.
    let mut read: Vec<&'static str> = Vec::new();
    let (credentials, access_key_id, session_token_present, workload_identity) =
        match &opts.credentials {
            CredentialSource::Ambient => {
                // object_store's own order, resolved HERE so the answer is a
                // value a test can read instead of a behaviour it must dial to
                // observe: static keys, then web identity, then container
                // credentials, then instance metadata.
                read.extend(STATIC_CREDENTIAL_VARS);
                read.extend(WORKLOAD_IDENTITY_VARS);
                if !opts.pins_instance_metadata() {
                    read.push(AMBIENT_METADATA_VAR);
                }
                match (
                    env_value("AWS_ACCESS_KEY_ID"),
                    env_value("AWS_SECRET_ACCESS_KEY"),
                ) {
                    (Some(key_id), Some(_)) => (
                        CredentialKind::Static,
                        Some(key_id),
                        env_value("AWS_SESSION_TOKEN").is_some(),
                        None,
                    ),
                    _ => match workload_identity_from_env() {
                        Some(id) => (CredentialKind::WorkloadIdentity, None, false, Some(id)),
                        // Nothing projected: object_store falls through to the
                        // instance-metadata provider. That is what `Ambient`
                        // means and it is reported as such.
                        None => (CredentialKind::Ambient, None, false, None),
                    },
                }
            }
            CredentialSource::Static {
                access_key_id,
                session_token,
                ..
            } => (
                CredentialKind::Static,
                Some(access_key_id.clone()),
                session_token.is_some(),
                None,
            ),
            CredentialSource::StaticFromEnv => {
                read.extend(STATIC_CREDENTIAL_VARS);
                let Some(key_id) = env_value("AWS_ACCESS_KEY_ID") else {
                    return Err(StoreError::Backend(
                        "credential mode `static` needs AWS_ACCESS_KEY_ID and \
                         AWS_SECRET_ACCESS_KEY projected into this process; neither is set"
                            .to_string(),
                    ));
                };
                if env_value("AWS_SECRET_ACCESS_KEY").is_none() {
                    return Err(StoreError::Backend(
                        "credential mode `static` has AWS_ACCESS_KEY_ID but no \
                         AWS_SECRET_ACCESS_KEY; check the Secret key names on the grant"
                            .to_string(),
                    ));
                }
                (
                    CredentialKind::Static,
                    Some(key_id),
                    env_value("AWS_SESSION_TOKEN").is_some(),
                    None,
                )
            }
            CredentialSource::WorkloadIdentity => {
                read.extend(WORKLOAD_IDENTITY_VARS);
                let Some(id) = workload_identity_from_env() else {
                    return Err(StoreError::Backend(format!(
                        "{WORKLOAD_IDENTITY_NOT_INJECTED}: credential mode \
                         `workloadIdentity` found neither AWS_WEB_IDENTITY_TOKEN_FILE plus \
                         AWS_ROLE_ARN nor AWS_CONTAINER_CREDENTIALS_FULL_URI plus its token \
                         file. Refusing rather than falling back to a node instance role, \
                         which is not a supported destination mode"
                    )));
                };
                (CredentialKind::WorkloadIdentity, None, false, Some(id))
            }
        };
    read.sort_unstable();
    read.dedup();

    // The metadata endpoint: the dead-loopback pin when asked for, otherwise
    // the ambient source's own `AWS_METADATA_ENDPOINT` if it is set, otherwise
    // object_store's default.
    let metadata_endpoint = if opts.pins_instance_metadata() {
        Some(DEAD_METADATA_ENDPOINT.to_string())
    } else if matches!(opts.credentials, CredentialSource::Ambient) {
        env_value(AMBIENT_METADATA_VAR)
    } else {
        None
    };

    Ok(S3Effective {
        bucket: bucket.clone(),
        prefix: prefix.clone(),
        region: region.clone(),
        endpoint: endpoint.clone(),
        virtual_hosted_style: !*path_style,
        allow_http: *allow_http,
        credentials,
        access_key_id,
        session_token_present,
        workload_identity,
        metadata_endpoint,
        root_certificate_count: opts.root_certificates.len(),
        request_timeout: opts.request_timeout,
        max_retries: opts.max_retries,
        retry_timeout: opts.retry_timeout,
        reads_environment: !read.is_empty(),
        environment_variables_read: read,
    })
}

/// **FX-20 fix round (review F1), the store's backstop.** A region that is not
/// a region name ([`logweir_core::engine::S3_REGION_PATTERN`]) builds no
/// client: on an endpoint-less S3 location `object_store` interpolates it
/// into the host (`s3.<region>.amazonaws.com`), so `x@attacker/` would carry
/// the credential's signed requests to another host. Every runner refuses the
/// same location earlier, by name, before this is reached; this is the rule
/// held once more where the client is built, for every caller. Never echoes
/// the value.
fn refuse_invalid_region(region: Option<&str>) -> Result<(), StoreError> {
    match region {
        Some(region) if !logweir_core::engine::is_valid_s3_region(region) => {
            Err(StoreError::Backend(format!(
                "{}: the S3 region is not a region name (it must match {}); no client is \
                 built, because without an endpoint the region is part of the host a request \
                 is sent to",
                logweir_core::guard::STORAGE_REGION_INVALID,
                logweir_core::engine::S3_REGION_PATTERN
            )))
        }
        _ => Ok(()),
    }
}

fn backend_name(u: &StorageUrl) -> &'static str {
    match u {
        StorageUrl::S3 { .. } => "s3",
        StorageUrl::Azure { .. } => "azure",
        StorageUrl::Gcs { .. } => "gcs",
        StorageUrl::Filesystem { .. } => "filesystem",
    }
}

/// **FX-31 — the two fences of a capped read**, over one GET's answer:
/// [`Store::get_capped`] and [`Store::get_version_capped`] both end here.
///
/// `key` is what a `TooLarge` names; `at` is how an I/O error names the read
/// (a version read says which version).
///
/// The size the answer reports is the LARGER of the object's size and the
/// length of the range the answer covers: for a whole-object GET they are
/// equal, and a store that disagrees with itself is held to the bigger claim.
async fn read_within(
    key: &str,
    at: &str,
    r: object_store::GetResult,
    max_bytes: u64,
) -> Result<Vec<u8>, StoreError> {
    use futures::StreamExt as _;
    let reported = r.meta.size.max(r.range.end.saturating_sub(r.range.start));
    // FENCE 1: the reported size, before a single body byte is taken. The
    // answer is dropped here with its body unread.
    if reported > max_bytes {
        return Err(StoreError::TooLarge {
            key: key.to_string(),
            cap: max_bytes,
            observed: OverCap::Reported(reported),
        });
    }
    // `reported <= max_bytes` here, so the reservation is the caller's cap at
    // most, never a size the store chose.
    let mut out: Vec<u8> = Vec::with_capacity(usize::try_from(reported).unwrap_or(0));
    let mut stream = r.into_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| StoreError::Io(format!("{at}: {e}")))?;
        // FENCE 2: a running cap, so a store that reported a small size and
        // streams more is cut off at the cap. The chunk that crosses it is
        // dropped, not appended.
        let read = u64::try_from(out.len())
            .unwrap_or(u64::MAX)
            .saturating_add(u64::try_from(chunk.len()).unwrap_or(u64::MAX));
        if read > max_bytes {
            return Err(StoreError::TooLarge {
                key: key.to_string(),
                cap: max_bytes,
                observed: OverCap::Streamed { reported, read },
            });
        }
        // THE BUFFER NEVER GROWS PAST THE CAP (review F10). A store that
        // reported less than it streams outgrows the reservation made from
        // its report, and `Vec`'s own doubling could then take up to twice the
        // cap. So it grows the way `Vec` would, doubling, but never past the
        // cap: `read <= max_bytes` here, so the target always holds the chunk.
        let needed = out.len() + chunk.len();
        if out.capacity() < needed {
            let target = needed
                .max(out.capacity().saturating_mul(2))
                .min(usize::try_from(max_bytes).unwrap_or(usize::MAX));
            out.reserve_exact(target - out.len());
        }
        out.extend_from_slice(&chunk);
    }
    Ok(out)
}

/// **FX-7 — what a failed read of ONE VERSION means.** [`Store::get_version_capped`]'s
/// error mapping, kept beside the classifier because it reads the same text.
///
/// A store says "I hold no such version" in TWO ways, and both are measured on
/// the e2e stack (`artifacts/fx-7/fix-round/live/foreign-id-probe/answers.txt`
/// in the FX-7 fix round):
///
/// * `404 NoSuchVersion` for an id it could have issued — SeaweedFS for every
///   foreign id, MinIO for a UUID-shaped one. `object_store` already answers
///   it as `Error::NotFound`.
/// * `400 InvalidArgument` ("Invalid version id specified") for an id whose
///   SHAPE it could never have issued — MinIO for anything that is not a UUID,
///   and AWS S3 for an id that is not in its own format. `object_store` 0.14
///   maps a 400 to `Error::Generic`, with the status and the S3 XML body in
///   its text.
///
/// Both are [`StoreError::NotFound`] here. A reader holding a pin taken in
/// ANOTHER bucket — the receipt of an archive copied byte for byte, the design's
/// "one point in two places" — meets the second as often as the first, and
/// reporting it as `Io` ("could not tell") would refuse every such copy. The
/// request carries no argument but the version id (no range, no conditional),
/// so an `InvalidArgument` on it can only be about that id. Every other failure
/// stays `Io`: a 403 is a grant to fix, not an absence.
fn version_read_error(key: &str, version: &str, error: object_store::Error) -> StoreError {
    let at = format!("{key}?versionId={version}");
    match error {
        // C6: a 404 that is about the credential is not an absence.
        object_store::Error::NotFound { .. } if refuses_the_credential(&error) => {
            StoreError::Io(format!("{at}: {error}"))
        }
        object_store::Error::NotFound { .. } => StoreError::NotFound(at),
        object_store::Error::Generic { .. } if names_a_version_never_issued(&error) => {
            StoreError::NotFound(at)
        }
        other => StoreError::Io(format!("{at}: {other}")),
    }
}

/// **C6: whether a 404's own answer says the CREDENTIAL was refused**, not
/// that the object is absent.
///
/// `object_store` maps every HTTP 404 to `Error::NotFound`, and S3 answers an
/// unknown access key id with 403 `InvalidAccessKeyId`, so on S3, MinIO,
/// SeaweedFS and RustFS a 404 is always about the object. versitygw answers
/// an unknown key id with `404 XAdminUserNotFound` (measured on v1.8.0,
/// PROD-01.5 §3.1). Read as an absence, that turned "this credential is
/// wrong" into "this receipt, manifest or segment does not exist": a missing
/// backup set, an empty catalog, a point reported gone.
///
/// **Decided from the answer's own `<Code>`, and from nothing else**
/// ([`Answer::of`]). PROD-01.2's review, M1: the first version of this
/// function scanned the error's whole text for credential words, and that
/// text echoes the request: MinIO's `404 NoSuchKey` body carries `<Key>`,
/// `<BucketName>` and `<Resource>`, and `object_store` prints the path and
/// the URL in front of it. A backup id, a prefix or a bucket containing
/// `expiredtoken` or `invalidsecurity` therefore turned every "this object is
/// not there yet" into a refused credential, and every backup to it exited 4.
/// The codes are [`code_class`]'s, so a credential code the table learns is
/// honoured here with no second list.
fn refuses_the_credential(error: &object_store::Error) -> bool {
    Answer::of(error).and_then(|a| a.code_class()) == Some(StoreErrorClass::InvalidCredentials)
}

/// A read's failure as [`StoreError`]: `NotFound` for a 404 about the object,
/// `Io` for everything else, **including a 404 about the credential** (C6,
/// [`refuses_the_credential`]). `Io` keeps the store's text, so
/// [`StoreErrorClass::classify`] names it `InvalidCredentials`.
///
/// Every read that turns `object_store`'s `NotFound` into ours ends here: a
/// read, a bounded read, a `HEAD`. A `HEAD`'s answer has no body and so no
/// code: on a store that refuses a credential with 404 it reads as an
/// absence, which is why a caller that must classify a refusal keeps a GET
/// ([`Store::head`]).
fn not_found_or_io(key: &str, error: object_store::Error) -> StoreError {
    match error {
        object_store::Error::NotFound { .. } if refuses_the_credential(&error) => {
            StoreError::Io(format!("{key}: {error}"))
        }
        object_store::Error::NotFound { .. } => StoreError::NotFound(key.to_string()),
        other => StoreError::Io(format!("{key}: {other}")),
    }
}

/// The `400 InvalidArgument` half of [`version_read_error`]: the answer's own
/// status and code ([`Answer::of`]), never a search of text that carries the
/// key and the version id asked for.
fn names_a_version_never_issued(error: &object_store::Error) -> bool {
    Answer::of(error).is_some_and(|a| {
        a.status == Some(400)
            && a.code
                .as_deref()
                .is_some_and(|c| c.eq_ignore_ascii_case("InvalidArgument"))
    })
}

// ------------------------------------------------- the backend's own answer

/// `object_store`'s words for an answer whose status is not 2xx
/// (`RequestError::Status`'s `Display`, object_store 0.14.1
/// `src/client/retry.rs:116`): the status, a colon, and the response body
/// verbatim to the end of the text.
const STATUS_LINE: &str = "Server returned non-2xx status code: ";

/// `object_store`'s words for an error document sent with a 2xx status
/// (`RequestError::Response`, `retry.rs:122`): the body to the end of the
/// text.
const RESPONSE_LINE: &str = "Server returned error response: ";

/// The words `object_store` opens its account of one HTTP request with
/// (`RetryError`'s `Display`, `retry.rs:50-67`):
/// `Error performing GET <url> in 2.5ms - `, with
/// `, after 2 retries, max_retries: 2, retry_timeout: 5s ` before the dash
/// when it retried. What the request was ANSWERED with follows the dash.
const REQUEST_LINE: &str = "Error performing ";

/// **What a backend said about a request, apart from everything that echoes
/// the request** (PROD-01.2 review, M1).
///
/// An object store's error reaches this crate as text that is mostly an echo.
/// `object_store` prints the path it asked for and the URL it sent; an S3
/// error document repeats the key in `<Key>`, the bucket in `<BucketName>`
/// and both in `<Resource>`; this crate puts the key in front of all of it.
/// An operator chooses every one of those names. So a word found SOMEWHERE in
/// the text says nothing about what the store answered, and the two facts
/// that do are read from where the store put them and from nowhere else:
///
/// * the HTTP **status**, from `object_store`'s own status line;
/// * the error **code**, the `<Code>` element that is a direct child of the
///   error document's root ([`error_code`]).
#[derive(Debug, Clone, PartialEq, Eq)]
struct Answer {
    /// The HTTP status, when `object_store` printed one.
    status: Option<u16>,
    /// The error document's own code, when the body is one error document
    /// with exactly one.
    code: Option<String>,
}

impl Answer {
    /// The answer a raw `object_store::Error` carries, read off the error's
    /// SOURCE CHAIN and never off its own `Display`.
    ///
    /// The variant's `Display` begins with the path (`Object at location
    /// <path> not found: …`), which is the caller's key. Its sources do not:
    /// the retry error begins with [`REQUEST_LINE`] and the request error
    /// inside it with [`STATUS_LINE`], so each link is read only where it
    /// STARTS with words `object_store` wrote, and what follows those words
    /// is the status and the body. No link is searched.
    ///
    /// `None` when no link is an HTTP answer: a transport failure, a local
    /// backend, a builder error.
    fn of(error: &object_store::Error) -> Option<Self> {
        let mut link = std::error::Error::source(error);
        while let Some(e) = link {
            if let Some(answer) = Self::opening(&e.to_string()) {
                return Some(answer);
            }
            link = e.source();
        }
        None
    }

    /// The answer `text` OPENS with: `text` is a status or response line, or
    /// a request line and then one. Anchored at the first byte; nothing
    /// before it is skipped.
    fn opening(text: &str) -> Option<Self> {
        Self::at_start(text).or_else(|| {
            let answered = request_line_end(text)?;
            Self::at_start(&text[answered..])
        })
    }

    /// The answer `text` starts with, where `text` begins with
    /// [`STATUS_LINE`] or [`RESPONSE_LINE`].
    fn at_start(text: &str) -> Option<Self> {
        if let Some(body) = text.strip_prefix(RESPONSE_LINE) {
            return Some(Self {
                status: None,
                code: error_code(body).map(str::to_string),
            });
        }
        let rest = text.strip_prefix(STATUS_LINE)?;
        // "<three digits> <reason>: <body>". `http::StatusCode` prints the
        // number and its canonical reason, and no reason holds a colon.
        let digits = rest.get(..3)?;
        if !digits.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        let status: u16 = digits.parse().ok()?;
        let body = rest.split_once(':').map_or("", |(_, body)| body);
        Some(Self {
            status: Some(status),
            code: error_code(body).map(str::to_string),
        })
    }

    /// The answer in a FLATTENED message: one that has already been through
    /// `Display` ([`StoreError::Io`]'s text, an `EngineError`'s), so its
    /// source chain is gone.
    ///
    /// Such a text opens with this crate's key and `object_store`'s path, and
    /// the answer follows `object_store`'s request line, so that is where it
    /// is read: at each of the text's [`own_accounts`], and only as what an
    /// account OPENS with.
    ///
    /// # What an echo can do to a flattened text, and the one limit
    ///
    /// A flattened text cannot say where the echo ends: a key may itself spell
    /// a request line and a status line. So EVERY request line in the text is
    /// read, and they must agree. The real one is always among them, so on a
    /// store that speaks HTTP an echo can at most make the text read as "no
    /// one answer" (`None` here, unclassified in [`classify_text`]); it can
    /// never put its own status or code in the real one's place.
    ///
    /// The limit: a backend that speaks no HTTP (a directory archive, the
    /// in-memory double) prints no request line of its own, so a key that
    /// spells one whole (`Error performing GET x in 1ms - Server returned
    /// non-2xx status code: 403 Forbidden: `) is then the only one and is
    /// read. It needs spaces and a colon, which no `BackupDestination` admits
    /// in a bucket or a prefix (R5, R6) and no Logweir key layout adds. The
    /// sites that DECIDE something on a raw error ([`not_found_or_io`],
    /// [`version_read_error`], [`StoreErrorClass::classify_object_store`])
    /// use [`Answer::of`], which reads no text a key is printed in.
    fn in_flattened(text: &str) -> Option<Self> {
        let accounts = own_accounts(text);
        let (first, rest) = accounts.split_first()?;
        let answer = Self::at_start(first)?;
        rest.iter()
            .all(|other| Self::at_start(other).as_ref() == Some(&answer))
            .then_some(answer)
    }

    /// The class the answer's CODE names, `None` for no code or one the table
    /// does not hold.
    fn code_class(&self) -> Option<StoreErrorClass> {
        self.code.as_deref().and_then(code_class)
    }

    /// The class of the answer: its code's, or else its status's. `None` for
    /// an answer that is neither (a 500, a 400 with a code nobody has a row
    /// for): "the store answered, and not in a way this table names".
    fn class(&self) -> Option<StoreErrorClass> {
        self.code_class().or(match self.status {
            Some(401) => Some(StoreErrorClass::InvalidCredentials),
            Some(403) => Some(StoreErrorClass::AccessDenied),
            Some(404) => Some(StoreErrorClass::ObjectNotFound),
            _ => None,
        })
    }
}

/// Where the text after `object_store`'s request line starts, when `text`
/// OPENS with one: `Error performing <METHOD> <url> in <elapsed>[, after …
/// retries, …] - `. `None` for any other text.
///
/// Each part is checked for its shape, so words that merely begin
/// `Error performing` (the list client's "Error performing list request: ")
/// are not a request line: the method is upper-case letters, the URL is one
/// run without a space (`object_store` percent-encodes it, or prints
/// `REDACTED`), and nothing but the retry clause stands before the dash.
fn request_line_end(text: &str) -> Option<usize> {
    let rest = text.strip_prefix(REQUEST_LINE)?;
    let (method, rest) = rest.split_once(' ')?;
    if method.is_empty() || !method.bytes().all(|b| b.is_ascii_uppercase()) {
        return None;
    }
    let (url, rest) = rest.split_once(' ')?;
    if url.is_empty() {
        return None;
    }
    let rest = rest.strip_prefix("in ")?;
    let (before_dash, answered) = rest.split_once(" - ")?;
    // The elapsed time and, after a retry, the retry clause: digits, units
    // and the clause's own lower-case words. `check::store::strip_retry_noise`
    // may already have removed the clause's numbers, which leaves its commas.
    let plain = |c: char| {
        c.is_ascii_lowercase()
            || c.is_ascii_digit()
            || matches!(c, ' ' | ',' | '.' | ':' | '_' | 'µ')
    };
    if before_dash.len() > 160 || !before_dash.chars().all(plain) {
        return None;
    }
    Some(text.len() - answered.len())
}

/// **`object_store`'s own accounts of a failure inside a flattened text**:
/// what follows each request line it printed, in order. Empty when the text
/// holds none.
///
/// For a failed HTTP request an account is the status line and the body, or
/// the HTTP client's own words (`HTTP error: error sending request`). What
/// stands BEFORE a request line is this crate's key, `object_store`'s path
/// and the URL, all three the operator's names, and none of it is returned.
///
/// A real failure has one request line. More than one means the text quotes
/// one, and [`classify_text`] and [`Answer::in_flattened`] then require them
/// to agree.
fn own_accounts(text: &str) -> Vec<&str> {
    let mut accounts = Vec::new();
    let mut from = 0;
    while let Some(at) = text[from..].find(REQUEST_LINE) {
        let start = from + at;
        if let Some(end) = request_line_end(&text[start..]) {
            accounts.push(&text[start + end..]);
        }
        from = start + REQUEST_LINE.len();
    }
    accounts
}

/// **The error document's own code**: the text of the `<Code>` element that
/// is a DIRECT CHILD of the root, when `body` is one `<Error>` document and
/// nothing else, and the root has exactly one such child.
///
/// Read as a document, not searched as text, because the document echoes the
/// request. MinIO's answer for an absent object:
///
/// ```text
/// <Error><Code>NoSuchKey</Code><Message>The specified key does not
/// exist.</Message><Key>expiredtoken-2026/manifest.json</Key><BucketName>…
/// ```
///
/// So: a `<Code>` anywhere deeper (inside a `<Key>` a store did not escape)
/// is not the code; two codes at the root are no code, because a document
/// that says two things about itself says nothing reliable; and anything that
/// is not a well-formed run of elements (a comment, a CDATA section, a
/// mismatched tag, text after the root closes) is `None`. `None` is "this
/// body names no code", and the caller falls back to the HTTP status. It is
/// never a guess.
fn error_code(body: &str) -> Option<&str> {
    let mut rest = body.trim();
    if rest.starts_with("<?") {
        rest = rest[rest.find("?>")? + 2..].trim_start();
    }
    let mut open: Vec<&str> = Vec::new();
    let mut code: Option<&str> = None;
    let mut codes = 0usize;
    // The start of the text of a root-level `<Code>` whose close is awaited.
    let mut code_text_from: Option<usize> = None;
    let mut at = 0usize;
    loop {
        let lt = at + rest[at..].find('<')?;
        if open.is_empty() && !rest[at..lt].trim().is_empty() {
            return None;
        }
        let gt = lt + rest[lt..].find('>')?;
        let tag = &rest[lt + 1..gt];
        at = gt + 1;
        if tag.starts_with(['!', '?']) {
            return None;
        }
        if let Some(name) = tag.strip_prefix('/') {
            if open.pop() != Some(name.trim()) {
                return None;
            }
            if let Some(from) = code_text_from.take() {
                // Closed by the very next tag, so its content is text alone.
                code = Some(rest[from..lt].trim());
                codes += 1;
            }
            if open.is_empty() {
                break;
            }
            continue;
        }
        // An element opening inside an awaited `<Code>`: not a plain code.
        if code_text_from.take().is_some() {
            codes += 1;
            code = None;
        }
        let name = tag.split_whitespace().next()?;
        if tag.ends_with('/') {
            if open.is_empty() {
                return None;
            }
            continue;
        }
        if open.is_empty() && name != "Error" {
            return None;
        }
        if open.len() == 1 && name == "Code" {
            code_text_from = Some(at);
        }
        open.push(name);
    }
    if !rest[at..].trim().is_empty() || codes != 1 {
        return None;
    }
    code.filter(|c| !c.is_empty())
}

/// **The code table**: what an error document's own code means, compared
/// whole and without regard to case. Never matched as a substring of
/// anything.
///
/// Credential codes come before `AccessDenied` in meaning as well as in this
/// list: `SignatureDoesNotMatch` arrives with a 403, and a wrong key is not
/// the same problem as a missing grant.
fn code_class(code: &str) -> Option<StoreErrorClass> {
    use StoreErrorClass as C;
    const CODES: [(&str, StoreErrorClass); 15] = [
        ("InvalidAccessKeyId", C::InvalidCredentials),
        ("SignatureDoesNotMatch", C::InvalidCredentials),
        ("ExpiredToken", C::InvalidCredentials),
        ("TokenRefreshRequired", C::InvalidCredentials),
        ("InvalidSecurity", C::InvalidCredentials),
        // C6 (PROD-01.5, closed by PROD-01.2): versitygw's code for an access
        // key id it does not know, answered with **404** (measured on v1.8.0).
        ("XAdminUserNotFound", C::InvalidCredentials),
        // A wrong region answers 301 `PermanentRedirect` (path-style) or 400
        // `AuthorizationHeaderMalformed` naming the expected region.
        ("PermanentRedirect", C::RegionMismatch),
        ("AuthorizationHeaderMalformed", C::RegionMismatch),
        ("IllegalLocationConstraintException", C::RegionMismatch),
        ("NoSuchBucket", C::BucketNotFound),
        ("AccessDenied", C::AccessDenied),
        ("AllAccessDisabled", C::AccessDenied),
        ("NoSuchKey", C::ObjectNotFound),
        ("NoSuchVersion", C::ObjectNotFound),
        ("RequestTimeout", C::Timeout),
    ];
    CODES
        .iter()
        .find(|(known, _)| known.eq_ignore_ascii_case(code))
        .map(|(_, class)| *class)
}

/// **The HTTP status a store answered with**, when `e` is a failure that
/// carries one: `Some(503)` for a throttled read, `None` for a transport
/// failure, a structural variant, or text with no answer in it.
///
/// For a caller that must tell a 5xx or a 429 from every other unclassified
/// failure without searching text that carries the key
/// (`logweir::backup::phase_run`'s retry hint). Read as
/// [`Answer::in_flattened`] reads it, with that function's stated limit.
#[must_use]
pub fn answered_status(e: &StoreError) -> Option<u16> {
    match e {
        StoreError::Backend(m) | StoreError::Io(m) => Answer::in_flattened(m)?.status,
        _ => None,
    }
}

// ------------------------------------------------------- error classification

/// The CLOSED store-side code vocabulary of D2 §4.2, so a caller can map a
/// failure to a check code without ever printing the raw error.
///
/// The spellings are the ones `logweir_core::check_contract::CheckCode` uses;
/// `the_class_names_are_check_codes` in `tests/options.rs` asserts the two
/// tables agree, so a rename in either is a failing test rather than a check
/// result with a reason string no UI has text for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum StoreErrorClass {
    /// The principal is authenticated and not authorized.
    AccessDenied,
    /// The credential itself is wrong, expired or not signed correctly:
    /// `InvalidAccessKeyId`, `SignatureDoesNotMatch`, `ExpiredToken`.
    InvalidCredentials,
    BucketNotFound,
    ObjectNotFound,
    EndpointUnreachable,
    TlsTrustFailed,
    RegionMismatch,
    Timeout,
    /// Deliberately NOT a fallback that pretends to know: "I could not
    /// classify this" is a different fact from any of the above, and a caller
    /// that showed a wrong remedy would send an operator after the wrong
    /// problem.
    StoreErrorUnclassified,
}

impl StoreErrorClass {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AccessDenied => "AccessDenied",
            Self::InvalidCredentials => "InvalidCredentials",
            Self::BucketNotFound => "BucketNotFound",
            Self::ObjectNotFound => "ObjectNotFound",
            Self::EndpointUnreachable => "EndpointUnreachable",
            Self::TlsTrustFailed => "TlsTrustFailed",
            Self::RegionMismatch => "RegionMismatch",
            Self::Timeout => "Timeout",
            Self::StoreErrorUnclassified => "StoreErrorUnclassified",
        }
    }

    pub const ALL: [Self; 9] = [
        Self::AccessDenied,
        Self::InvalidCredentials,
        Self::BucketNotFound,
        Self::ObjectNotFound,
        Self::EndpointUnreachable,
        Self::TlsTrustFailed,
        Self::RegionMismatch,
        Self::Timeout,
        Self::StoreErrorUnclassified,
    ];

    /// Classifies a [`StoreError`].
    ///
    /// # What this is, honestly
    ///
    /// The variant carries the reliable half: [`StoreError::NotFound`] is a
    /// genuine absence, established by the backend, and never a denial (that
    /// distinction is what `StoreError::NotFound`'s own doc comment exists
    /// for). Everything else arrives as [`StoreError::Io`], whose message is
    /// `object_store::Error`'s `Display` behind this crate's key: a flattened
    /// text, read as `classify_text` reads one.
    ///
    /// **A store's answer is read from its status and its code, never found
    /// by searching** (PROD-01.2 review, M1): the text echoes the key, the
    /// bucket and the URL, so a word somewhere in it says nothing. Only a
    /// failure with no answer in it at all (a transport error, a TLS failure,
    /// `object_store`'s own wording for a local backend) is matched against
    /// the wording table, and then only `object_store`'s own account of it.
    /// Every row of both tables is pinned by a message in
    /// `tests/options.rs::the_classifier_table`, and the unmatched case is its
    /// own answer rather than a guess.
    #[must_use]
    pub fn classify(e: &StoreError) -> Self {
        match e {
            StoreError::NotFound(_) => Self::ObjectNotFound,
            StoreError::AlreadyExists(_) | StoreError::ReadOnly(_) => Self::StoreErrorUnclassified,
            StoreError::NotAManifest(_, _) => Self::StoreErrorUnclassified,
            // FX-31: an object over its reader's cap is a fact about the
            // object, and the closed vocabulary has no code for it. It is
            // STRUCTURAL, so its text — which names a key an adopter chose —
            // is never token-scanned into a code it does not mean.
            StoreError::TooLarge { .. } => Self::StoreErrorUnclassified,
            StoreError::Backend(m) | StoreError::Io(m) => classify_text(m),
        }
    }

    /// Classifies a raw `object_store::Error`: its STRUCTURED variant, and the
    /// answer its source chain carries ([`Answer::of`]). Callers that still
    /// hold the raw error should prefer this: it reads no text that names the
    /// path or the URL.
    #[must_use]
    pub fn classify_object_store(e: &object_store::Error) -> Self {
        let answer = Answer::of(e);
        let named = answer.as_ref().and_then(Answer::code_class);
        match e {
            // A `NoSuchBucket` arrives as NotFound too, and "the bucket is
            // not there" is a different remedy from "the key is not there".
            // So does a credential refusal on a store that answers one with
            // 404 (C6, `refuses_the_credential`).
            object_store::Error::NotFound { .. } => named.unwrap_or(Self::ObjectNotFound),
            // A wrong key is not a missing grant: `SignatureDoesNotMatch`
            // arrives as a 403.
            object_store::Error::PermissionDenied { .. } => named.unwrap_or(Self::AccessDenied),
            object_store::Error::Unauthenticated { .. } => {
                named.unwrap_or(Self::InvalidCredentials)
            }
            other => match answer {
                Some(answer) => answer.class().unwrap_or(Self::StoreErrorUnclassified),
                None => classify_text(&other.to_string()),
            },
        }
    }
}

/// **A flattened failure text, classified.**
///
/// 1. `object_store`'s own account of the failure is taken
///    ([`own_accounts`]): what follows its request line. The key, the path
///    and the URL in front of it are not read. A text with no request line
///    (a builder error, a backend that speaks no HTTP, a message some caller
///    composed) is its own account.
/// 2. If the account is a store's ANSWER, the class is the answer's
///    ([`Answer::class`]): its code, or else its status. The body is not
///    searched, and an answer the tables do not name is unclassified.
/// 3. Otherwise nothing answered, and the account is the HTTP client's or
///    `object_store`'s own wording, matched against [`wording_class`].
/// 4. A text with more than one request line quotes one (a key can spell
///    one). They must all give the same class, or the text is unclassified:
///    the real one is among them, so an echo can turn a class into "could
///    not classify" and never into another class
///    ([`Answer::in_flattened`] states the one limit).
fn classify_text(text: &str) -> StoreErrorClass {
    let class_of = |own: &str| match Answer::at_start(own) {
        Some(answer) => answer
            .class()
            .unwrap_or(StoreErrorClass::StoreErrorUnclassified),
        None => wording_class(own),
    };
    let accounts = own_accounts(text);
    let Some((first, rest)) = accounts.split_first() else {
        return class_of(text);
    };
    let class = class_of(first);
    if rest.iter().all(|other| class_of(other) == class) {
        class
    } else {
        StoreErrorClass::StoreErrorUnclassified
    }
}

/// **The wording table**, for a failure that carries no answer: the words of
/// the HTTP client, the TLS library and `object_store` itself. No S3 error
/// code is here: a code is read from an error document
/// ([`code_class`]) and from nowhere else.
///
/// Order matters: TLS before anything that can also say "connect", a timeout
/// before a transport symptom.
///
/// What is scanned is one of [`own_accounts`] with any `for url (…)` clause
/// removed. When `object_store` printed a request line that excludes every
/// echo of the request. When it printed none (a builder error, a local backend, a
/// message some caller composed) the text may still name a path, and these
/// words would be found in one; none of them is a code, and a
/// `BackupDestination`'s bucket and prefix (R5, R6: no space) can spell only
/// the bare word `timeout`, which is therefore matched as a word of its own.
fn wording_class(own: &str) -> StoreErrorClass {
    let lower = without_urls(own).to_ascii_lowercase();
    let says = |words: &[&str]| words.iter().any(|w| lower.contains(w));

    // A private CA that the process does not trust. Checked first: a TLS
    // failure can also carry the word "connect", and "add the CA bundle" is a
    // very different remedy from "open the port".
    if says(&[
        "invalid peer certificate",
        "certificate verify failed",
        "certificate: unknownissuer",
        "self-signed certificate",
        "self signed certificate",
        "certificate is not trusted",
    ]) {
        return StoreErrorClass::TlsTrustFailed;
    }
    // `object_store`'s wording for its `Unauthenticated` variant (a 401).
    if says(&["lacked valid authentication credentials"]) {
        return StoreErrorClass::InvalidCredentials;
    }
    // Its wording for `PermissionDenied` (a 403).
    if says(&["lacked the necessary privileges"]) {
        return StoreErrorClass::AccessDenied;
    }
    // Its wording for `NotFound`, from a backend that speaks no HTTP.
    if says(&["not found: "]) {
        return StoreErrorClass::ObjectNotFound;
    }
    if says(&["timed out", "operation timed out", "deadline has elapsed"])
        || lower
            .split(|c: char| !(c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.' | '/')))
            .any(|word| word == "timeout")
    {
        return StoreErrorClass::Timeout;
    }
    if says(&[
        "connection refused",
        "dns error",
        "failed to lookup address",
        "no route to host",
        "network is unreachable",
        "error sending request",
        "connection reset",
        "url scheme is not allowed",
    ]) {
        return StoreErrorClass::EndpointUnreachable;
    }
    StoreErrorClass::StoreErrorUnclassified
}

/// `text` with every `for url (…)` clause the HTTP client prints replaced by
/// `for url`: the URL names the bucket and the key.
fn without_urls(text: &str) -> String {
    const OPEN: &str = "for url (";
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(at) = rest.find(OPEN) {
        let after = &rest[at + OPEN.len()..];
        let Some(close) = after.find(')') else { break };
        out.push_str(&rest[..at]);
        out.push_str("for url");
        rest = &after[close + 1..];
    }
    out.push_str(rest);
    out
}

/// What the S3 client a store would build is ACTUALLY configured with, read
/// back off `AmazonS3Builder` itself.
///
/// [`s3_effective`] states the decision; this states what the object_store
/// builder ended up holding. They are two different claims, and the gap
/// between them is precisely where F2 lived: `s3_effective` reported
/// `endpoint: None` while `from_env()` had already put `AWS_ENDPOINT_URL` into
/// the builder. A test that reads only the decision cannot see that; this
/// function makes the second claim checkable with no socket.
///
/// Returns `(key, value)` pairs for the location and route keys, sorted. The
/// secret access key is NEVER included — `AccessKeyId` is the public half.
pub fn s3_builder_config(
    u: &StorageUrl,
    opts: &StoreOptions,
) -> Result<Vec<(&'static str, String)>, StoreError> {
    use object_store::aws::AmazonS3ConfigKey as K;
    let b = Store::s3_builder(u, opts)?;
    let keys: [(&'static str, K); 7] = [
        ("bucket", K::Bucket),
        ("region", K::Region),
        ("endpoint", K::Endpoint),
        ("virtual_hosted_style_request", K::VirtualHostedStyleRequest),
        ("metadata_endpoint", K::MetadataEndpoint),
        ("access_key_id", K::AccessKeyId),
        ("web_identity_token_file", K::WebIdentityTokenFile),
    ];
    let mut out: Vec<(&'static str, String)> = keys
        .into_iter()
        .filter_map(|(name, k)| b.get_config_value(&k).map(|v| (name, v)))
        .filter(|(_, v)| !v.is_empty())
        .collect();
    out.sort_unstable();
    Ok(out)
}

/// `true` when this refusal is D2 §3.5's fail-closed "no injected identity".
#[must_use]
pub fn is_workload_identity_not_injected(e: &StoreError) -> bool {
    matches!(e, StoreError::Backend(m) if m.starts_with(WORKLOAD_IDENTITY_NOT_INJECTED))
}

#[cfg(test)]
mod version_read_tests {
    //! FX-7 fix round: [`version_read_error`] over the answers the e2e stack
    //! MEASURED (`artifacts/fx-7/fix-round/live/foreign-id-probe/answers.txt`).
    //! The text is the shape `object_store` 0.14.1 prints for a non-2xx answer:
    //! `RetryError`'s "Error performing GET … - " and then
    //! `RequestError::Status`'s "Server returned non-2xx status code: …".
    use super::*;

    const KEY: &str = "fx7/set-1/manifest.json";

    fn answered(status_and_body: &str) -> object_store::Error {
        object_store::Error::Generic {
            store: "S3",
            source: format!(
                "Error performing GET http://minio:9000/kafka-backups/{KEY}?versionId=x in \
                 3.1ms - Server returned non-2xx status code: {status_and_body}"
            )
            .into(),
        }
    }

    /// MinIO's answer to an id that is not a UUID, verbatim from the probe.
    const MINIO_400: &str = "400 Bad Request: <?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
        <Error><Code>InvalidArgument</Code><Message>Invalid version id specified</Message>\
        <Key>fx7/set-1/manifest.json</Key><BucketName>kafka-backups</BucketName></Error>";

    #[test]
    fn an_id_the_bucket_could_never_have_issued_is_not_found() {
        assert!(
            matches!(
                version_read_error(KEY, "fx7v000001", answered(MINIO_400)),
                StoreError::NotFound(_)
            ),
            "MinIO's 400 InvalidArgument for a foreign id says the bucket holds no such \
             version, exactly as a 404 NoSuchVersion does"
        );
    }

    #[test]
    fn a_404_is_not_found() {
        let e = object_store::Error::NotFound {
            path: KEY.to_string(),
            source: "Server returned non-2xx status code: 404 Not Found: <Error><Code>\
                     NoSuchVersion</Code></Error>"
                .into(),
        };
        assert!(matches!(
            version_read_error(KEY, "v", e),
            StoreError::NotFound(_)
        ));
    }

    /// versitygw v1.8.0's answer to an unknown access key id, as `object_store`
    /// carries it (the text is PROD-01.5's recorded run, request id removed).
    const VERSITYGW_UNKNOWN_KEY: &str = "Server returned non-2xx status code: 404 Not Found: \
        <?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>XAdminUserNotFound</Code>\
        <Message>No user exists with the provided access key ID.</Message></Error>";

    fn not_found(source: &str) -> object_store::Error {
        object_store::Error::NotFound {
            path: KEY.to_string(),
            source: source.to_string().into(),
        }
    }

    /// **C6: a 404 about the credential is never an absence**, at each place a
    /// read turns `object_store`'s `NotFound` into ours: a plain read, a
    /// bounded read and a `HEAD` (all three through `not_found_or_io`) and a
    /// read by version id.
    ///
    /// CONTROL, in the same test: a genuine `404 NoSuchKey` and a genuine
    /// `404 NoSuchVersion` stay `NotFound`, so the rule is about the code and
    /// not about every 404.
    ///
    /// Mutant: drop `XAdminUserNotFound` from the code table and all three
    /// credential arms answer `NotFound`.
    #[test]
    fn a_404_that_refuses_the_credential_is_never_not_found() {
        for (site, e) in [
            (
                "a read",
                not_found_or_io(KEY, not_found(VERSITYGW_UNKNOWN_KEY)),
            ),
            (
                "a read by version id",
                version_read_error(KEY, "v", not_found(VERSITYGW_UNKNOWN_KEY)),
            ),
        ] {
            match &e {
                StoreError::Io(message) => {
                    assert!(message.contains("XAdminUserNotFound"), "{site}: {message}");
                }
                other => panic!("{site}: a refused credential must be Io, got {other:?}"),
            }
        }
        assert_eq!(
            StoreErrorClass::classify_object_store(&not_found(VERSITYGW_UNKNOWN_KEY)),
            StoreErrorClass::InvalidCredentials
        );

        // CONTROL: a 404 about the object.
        let no_such_key = "Server returned non-2xx status code: 404 Not Found: <Error><Code>\
                           NoSuchKey</Code><Message>The specified key does not exist.</Message>\
                           </Error>";
        assert!(matches!(
            not_found_or_io(KEY, not_found(no_such_key)),
            StoreError::NotFound(_)
        ));
        assert_eq!(
            StoreErrorClass::classify_object_store(&not_found(no_such_key)),
            StoreErrorClass::ObjectNotFound
        );
        assert_eq!(
            StoreErrorClass::classify_object_store(&not_found(
                "Server returned non-2xx status code: 404 Not Found: <Error><Code>NoSuchBucket\
                 </Code></Error>"
            )),
            StoreErrorClass::BucketNotFound
        );
        assert!(matches!(
            version_read_error(
                KEY,
                "v",
                not_found(
                    "Server returned non-2xx status code: 404 Not Found: <Error><Code>\
                     NoSuchVersion</Code></Error>"
                )
            ),
            StoreError::NotFound(_)
        ));
    }

    /// The seven words the first classifier matched as substrings: six S3
    /// credential codes or phrases and versitygw's. Lower case, as it matched
    /// them, and as a bucket name must be.
    const CREDENTIAL_WORDS: [&str; 7] = [
        "invalidaccesskeyid",
        "signaturedoesnotmatch",
        "expiredtoken",
        "tokenrefreshrequired",
        "invalidsecurity",
        "lacked valid authentication credentials",
        "xadminusernotfound",
    ];

    /// MinIO's `404 NoSuchKey` for `key` in `bucket`, in the words
    /// `object_store` 0.14.1 hands over: the retry error's request line with
    /// the URL, the status line, and MinIO's body with its `<Key>`,
    /// `<BucketName>` and `<Resource>` (captured on the compose stack by
    /// PROD-01.2's review; the ids are placeholders).
    fn minio_no_such_key(bucket: &str, key: &str) -> object_store::Error {
        let url_key = key.replace(' ', "%20");
        object_store::Error::NotFound {
            path: key.to_string(),
            source: format!(
                "Error performing GET http://minio:9000/{bucket}/{url_key} in 2.523042ms - \
                 Server returned non-2xx status code: 404 Not Found: <?xml version=\"1.0\" \
                 encoding=\"UTF-8\"?>\n<Error><Code>NoSuchKey</Code><Message>The specified key \
                 does not exist.</Message><Key>{key}</Key><BucketName>{bucket}</BucketName>\
                 <Resource>/{bucket}/{key}</Resource><RequestId>18DD1B6AA58ED4B2</RequestId>\
                 <HostId>dd9025ba</HostId></Error>"
            )
            .into(),
        }
    }

    /// **PROD-01.2 review, M1: an absent object is absent, whatever it is
    /// called.** A `404 NoSuchKey` whose text echoes a key, a prefix and a
    /// bucket spelling EACH of the seven credential words is `NotFound` at
    /// every read site, and its class is `ObjectNotFound`.
    ///
    /// The first classifier matched those words anywhere in the lowercased
    /// text, so a backup id containing `expiredtoken` made the "is this set
    /// new?" read answer `Io`, and every backup to it exited 4
    /// (`ExecutionClaimUnproven`), with a remedy about IAM grants.
    ///
    /// CONTROL: the same shape with versitygw's own code still refuses, under
    /// the same echoing names, so the row is not passing because nothing is
    /// a credential refusal any more.
    ///
    /// Mutants: the substring match restored (`refuses_the_credential`
    /// answering from the whole text) fails every one of the 21 arms.
    #[test]
    fn an_absent_object_is_not_found_whatever_its_key_prefix_or_bucket_spell() {
        for word in CREDENTIAL_WORDS {
            // A bucket cannot hold a space: the one phrase is tried in the key
            // and the prefix, and as a hyphenated bucket.
            let as_bucket = word.replace(' ', "-");
            let cases = [
                (
                    "the key",
                    "kafka-backups".to_string(),
                    format!("team-a/set-1/{word}.json"),
                ),
                (
                    "the prefix",
                    "kafka-backups".to_string(),
                    format!("{word}-2026/set-1/manifest.json"),
                ),
                (
                    "the bucket",
                    format!("{as_bucket}-archive"),
                    "team-a/set-1/manifest.json".to_string(),
                ),
            ];
            for (where_, bucket, key) in cases {
                let what = format!("`{word}` in {where_}");
                let error = || minio_no_such_key(&bucket, &key);
                assert!(
                    !refuses_the_credential(&error()),
                    "{what}: a NoSuchKey is not a refused credential"
                );
                assert!(
                    matches!(not_found_or_io(&key, error()), StoreError::NotFound(k) if k == key),
                    "{what}: a read"
                );
                assert!(
                    matches!(
                        version_read_error(&key, "v1", error()),
                        StoreError::NotFound(_)
                    ),
                    "{what}: a read by version id"
                );
                assert_eq!(
                    StoreErrorClass::classify_object_store(&error()),
                    StoreErrorClass::ObjectNotFound,
                    "{what}: the class of the raw error"
                );
                // The flattened text, as a caller that kept only `Display`
                // would hold it: still the object's absence.
                assert_eq!(
                    StoreErrorClass::classify(&StoreError::Io(format!("{key}: {}", error()))),
                    StoreErrorClass::ObjectNotFound,
                    "{what}: the class of the flattened text"
                );

                // CONTROL: the same names, and the store really refuses the
                // credential.
                let refused = object_store::Error::NotFound {
                    path: key.clone(),
                    source: format!(
                        "Error performing GET http://gw:7070/{bucket}/{} in 1.7745ms - \
                         {VERSITYGW_UNKNOWN_KEY}",
                        key.replace(' ', "%20")
                    )
                    .into(),
                };
                assert!(refuses_the_credential(&refused), "{what}: the control");
                let e = not_found_or_io(&key, refused);
                assert!(matches!(e, StoreError::Io(_)), "{what}: the control: {e:?}");
                assert_eq!(
                    StoreErrorClass::classify(&e),
                    StoreErrorClass::InvalidCredentials,
                    "{what}: the control's flattened class"
                );
            }
        }
    }

    /// **The code is the error document's own, not a `<Code>` found in the
    /// text.** Three shapes of one absence, each `NotFound`:
    ///
    /// 1. a key literally named `<Code>ExpiredToken</Code>`, echoed by a store
    ///    that escapes it (MinIO, S3) and by one that does not, where it then
    ///    sits INSIDE `<Key>`, before or after the real code;
    /// 2. a body with no `<Code>` at all (a proxy's page, an empty body);
    /// 3. a body with two codes at its root, which names none.
    ///
    /// Mutants: a code taken from the first `<Code>` anywhere in the text, or
    /// from any `<Code>` anywhere, each fail an arm of (1).
    #[test]
    fn the_code_is_the_error_documents_own_and_never_one_found_in_the_text() {
        let key = "team-a/<Code>ExpiredToken</Code>";
        let status = "Error performing GET http://minio:9000/kafka-backups/team-a/%3CCode%3E\
                      ExpiredToken%3C/Code%3E in 1.2ms - Server returned non-2xx status code: \
                      404 Not Found: ";
        let bodies = [
            (
                "escaped, as MinIO and S3 answer",
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>NoSuchKey</Code>\
                 <Message>The specified key does not exist.</Message><Key>team-a/&lt;Code&gt;\
                 ExpiredToken&lt;/Code&gt;</Key><BucketName>kafka-backups</BucketName></Error>"
                    .to_string(),
            ),
            (
                "not escaped, the key after the code",
                format!("<Error><Code>NoSuchKey</Code><Key>{key}</Key></Error>"),
            ),
            (
                "not escaped, the key BEFORE the code",
                format!("<Error><Key>{key}</Key><Code>NoSuchKey</Code></Error>"),
            ),
            (
                "not escaped, and the only code in the body",
                format!("<Error><Key>{key}</Key><Message>absent</Message></Error>"),
            ),
            ("no code at all: an empty body", String::new()),
            (
                "no code at all: a proxy's page",
                "<html><body><h1>404 Not Found</h1>ExpiredToken</body></html>".to_string(),
            ),
            (
                "no code at all: an error document without one",
                "<Error><Message>ExpiredToken</Message></Error>".to_string(),
            ),
            (
                "two codes at the root",
                "<Error><Code>NoSuchKey</Code><Code>ExpiredToken</Code></Error>".to_string(),
            ),
            (
                "a code after the document closed",
                "<Error><Message>absent</Message></Error><Code>ExpiredToken</Code>".to_string(),
            ),
        ];
        for (what, body) in bodies {
            let error = || object_store::Error::NotFound {
                path: key.to_string(),
                source: format!("{status}{body}").into(),
            };
            assert!(!refuses_the_credential(&error()), "{what}");
            assert!(
                matches!(not_found_or_io(key, error()), StoreError::NotFound(_)),
                "{what}: a read"
            );
            assert!(
                matches!(
                    version_read_error(key, "v1", error()),
                    StoreError::NotFound(_)
                ),
                "{what}: a read by version id"
            );
            assert_eq!(
                StoreErrorClass::classify_object_store(&error()),
                StoreErrorClass::ObjectNotFound,
                "{what}"
            );
            // Flattened, with this crate's own key in front: the key's
            // `<Code>` is the FIRST one in the text.
            assert_eq!(
                StoreErrorClass::classify(&StoreError::Io(format!("{key}: {}", error()))),
                StoreErrorClass::ObjectNotFound,
                "{what}: flattened"
            );
        }

        // CONTROL: the document's own code, under the same key, refuses.
        let refused = object_store::Error::NotFound {
            path: key.to_string(),
            source: format!(
                "{status}<Error><Key>{key}</Key><Code>XAdminUserNotFound</Code></Error>"
            )
            .into(),
        };
        assert!(refuses_the_credential(&refused));
        assert!(matches!(not_found_or_io(key, refused), StoreError::Io(_)));
    }

    /// [`error_code`] reads a document and nothing looser.
    #[test]
    fn an_error_code_is_the_one_root_level_code_of_a_whole_document() {
        for (body, want) in [
            ("<Error><Code>NoSuchKey</Code></Error>", Some("NoSuchKey")),
            (
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error>\n  <Code> AccessDenied \
                 </Code>\n  <Message>m</Message>\n</Error>\n",
                Some("AccessDenied"),
            ),
            (
                "<Error xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Message>a > b\
                 </Message><Code>SlowDown</Code><HostId/></Error>",
                Some("SlowDown"),
            ),
            ("", None),
            ("NoSuchKey", None),
            ("<Code>NoSuchKey</Code>", None),
            ("<Error><Code></Code></Error>", None),
            ("<Error><Code/></Error>", None),
            ("<Error><Code><b>NoSuchKey</b></Code></Error>", None),
            ("<Error><Code>NoSuchKey</Code>", None),
            ("<Error><Code>NoSuchKey</Message></Error>", None),
            ("<Error><Code>NoSuchKey</Code></Error> and more", None),
            ("words <Error><Code>NoSuchKey</Code></Error>", None),
            ("<Error><!-- c --><Code>NoSuchKey</Code></Error>", None),
            (
                "<Error><Message><![CDATA[<Code>X</Code>]]></Message></Error>",
                None,
            ),
            ("<Errors><Code>NoSuchKey</Code></Errors>", None),
        ] {
            assert_eq!(error_code(body), want, "{body:?}");
        }
    }

    /// Where `object_store`'s request line ends, and what is not one.
    #[test]
    fn a_request_line_is_read_only_in_its_own_shape() {
        for line in [
            "Error performing GET http://minio:9000/b/k in 2.523042ms - ",
            "Error performing HEAD http://minio:9000/b/k in 942.667µs - ",
            "Error performing PUT REDACTED in 3s - ",
            "Error performing GET http://minio:9000/b/k in 6.2s, after 2 retries, max_retries: \
             2, retry_timeout: 5s  - ",
            // What `check::store::strip_retry_noise` leaves of the clause.
            "Error performing GET http://minio:9000/b/k in 6.2s, , ,   - ",
        ] {
            let text = format!("{line}HTTP error: error sending request");
            assert_eq!(request_line_end(&text), Some(line.len()), "{line:?}");
            assert_eq!(
                own_accounts(&format!("k: Generic S3 error: {text}")),
                ["HTTP error: error sending request"],
                "{line:?}"
            );
        }
        for not_one in [
            "Error performing list request: Error performing GET http://m/b in 1ms - x",
            "Error performing get http://minio:9000/b/k in 1ms - x",
            "Error performing GET http://minio:9000/b/k within 1ms - x",
            "Error performing GET http://minio:9000/b/k in 1ms; <Code>X</Code> - x",
            "Error performing GET",
            "error performing GET http://minio:9000/b/k in 1ms - x",
        ] {
            assert_eq!(request_line_end(not_one), None, "{not_one:?}");
        }
        // The list client's lead-in is skipped and the request line after it
        // is found.
        assert_eq!(
            own_accounts(
                "Generic S3 error: Error performing list request: Error performing GET \
                 http://m/b?list-type=2 in 1ms - Server returned non-2xx status code: 403 \
                 Forbidden: "
            ),
            ["Server returned non-2xx status code: 403 Forbidden: "]
        );
        // No request line: no account, and the classifier reads the text.
        assert!(own_accounts("Generic S3 error: builder error").is_empty());
    }

    /// A key that spells a whole request line and status line of its own, the
    /// one echo a flattened text cannot tell from `object_store`'s words.
    const SPELLS_A_REFUSAL: &str = "Error performing GET x in 1ms - Server returned non-2xx \
        status code: 403 Forbidden: <Error><Code>ExpiredToken</Code></Error>";

    /// **What a key that spells `object_store`'s own request line can do: on
    /// a store that speaks HTTP, make a flattened text unclassified, and
    /// nothing else.** Every request line in the text is read and they must
    /// agree; the real one is among them.
    ///
    /// The raw error is not moved at all: its answer is read off the source
    /// chain, where no key is printed.
    ///
    /// THE LIMIT, pinned so it is a stated fact and not a surprise: with no
    /// request line of `object_store`'s own (a backend that speaks no HTTP),
    /// the key's is the only one and is read.
    #[test]
    fn a_key_that_spells_a_request_line_can_only_make_a_flattened_text_unclassified() {
        let key = SPELLS_A_REFUSAL;
        // An absent object under that key: the raw error is an absence.
        let absent = || minio_no_such_key("kafka-backups", key);
        assert!(!refuses_the_credential(&absent()));
        assert!(matches!(
            not_found_or_io(key, absent()),
            StoreError::NotFound(_)
        ));
        assert_eq!(
            StoreErrorClass::classify_object_store(&absent()),
            StoreErrorClass::ObjectNotFound
        );
        // Flattened, the key is printed before the real request line (this
        // crate's prefix, object_store's path) and after it (the body's
        // `<Key>` and `<Resource>`): the lines disagree, so no class and no
        // status is taken from any of them.
        let flattened = StoreError::Io(format!("{key}: {}", absent()));
        assert_eq!(
            StoreErrorClass::classify(&flattened),
            StoreErrorClass::StoreErrorUnclassified
        );
        assert_eq!(answered_status(&flattened), None);

        // A transport failure under that key: the same.
        let refused_connection = StoreError::Io(format!(
            "{key}: Generic S3 error: Error performing GET http://minio:9000/kafka-backups/k in \
             2ms - HTTP error: error sending request"
        ));
        assert_eq!(
            StoreErrorClass::classify(&refused_connection),
            StoreErrorClass::StoreErrorUnclassified
        );
        assert_eq!(answered_status(&refused_connection), None);

        // CONTROL: the same two failures under an ordinary key classify.
        assert_eq!(
            StoreErrorClass::classify(&StoreError::Io(format!(
                "k: {}",
                minio_no_such_key("kafka-backups", "k")
            ))),
            StoreErrorClass::ObjectNotFound
        );
        assert_eq!(
            StoreErrorClass::classify(&StoreError::Io(
                "k: Generic S3 error: Error performing GET http://minio:9000/kafka-backups/k in \
                 2ms - HTTP error: error sending request"
                    .to_string()
            )),
            StoreErrorClass::EndpointUnreachable
        );

        // THE LIMIT: no request line but the key's.
        let directory = StoreError::Io(format!(
            "{key}: Generic LocalFileSystem error: Unable to open file: Permission denied (os \
             error 13)"
        ));
        assert_eq!(
            StoreErrorClass::classify(&directory),
            StoreErrorClass::AccessDenied,
            "read from the key's own words: the stated limit of a flattened text"
        );
    }

    /// The status a caller may act on is the answer's, not a number in a key.
    #[test]
    fn the_answered_status_is_the_status_lines() {
        let io = |text: &str| StoreError::Io(text.to_string());
        assert_eq!(
            answered_status(&io(
                "logs-503/k: Generic S3 error: Error performing GET http://m/b/logs-503/k in \
                 1ms - Server returned non-2xx status code: 404 Not Found: "
            )),
            Some(404)
        );
        assert_eq!(
            answered_status(&io(
                "k: Generic S3 error: Error performing GET http://m/b/k in 6s, after 2 retries, \
                 max_retries: 2, retry_timeout: 5s  - Server returned non-2xx status code: 503 \
                 Service Unavailable: <Error><Code>SlowDown</Code></Error>"
            )),
            Some(503)
        );
        assert_eq!(
            answered_status(&io(
                "non-2xx status code: 503/k: Generic S3 error: Error performing GET \
                 http://m/b/k in 1ms - HTTP error: error sending request"
            )),
            None,
            "a transport failure answered with no status, whatever the key spells"
        );
        assert_eq!(answered_status(&StoreError::NotFound("k".into())), None);
    }

    /// Every other failure is "could not tell", never "not here".
    #[test]
    fn a_denial_an_outage_or_another_bad_request_is_io() {
        let denied = object_store::Error::PermissionDenied {
            path: KEY.to_string(),
            source: "Server returned non-2xx status code: 403 Forbidden: <Error><Code>\
                     AccessDenied</Code></Error>"
                .into(),
        };
        for (what, error) in [
            ("a 403 (no s3:GetObjectVersion)", denied),
            (
                "a 503",
                answered("503 Service Unavailable: <Error><Code>SlowDown</Code></Error>"),
            ),
            (
                "a 400 with another code",
                answered("400 Bad Request: <Error><Code>InvalidRequest</Code></Error>"),
            ),
            (
                "InvalidArgument on a status that is not 400",
                answered("500 Internal Server Error: <Error><Code>InvalidArgument</Code></Error>"),
            ),
        ] {
            match version_read_error(KEY, "v", error) {
                StoreError::Io(message) => {
                    assert!(message.contains("?versionId=v"), "{what}: {message}");
                }
                other => panic!("{what} must be Io, got {other:?}"),
            }
        }
    }
}
