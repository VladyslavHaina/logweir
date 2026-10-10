//! The request versions the pinned engine SENDS — the table a capability
//! check holds an endpoint's own ApiVersions answer against (PROD-01.2).
//!
//! SOURCE: U/kafka-backup/crates/kafka-backup-core/src/kafka/client.rs @ tag
//! v0.23.3 (`third_party/kafka-backup-v0.23.3.tar.gz`), `get_api_version` at
//! `:625-648`; byte-identical to v0.21.0's `:588-611` (PROD-00.1 §3.8,
//! PROD-00.3f §12.3). Ported, not linked (Global Constraint 2), and NOT a
//! serde shape, so the xtask gate pairs it as a VERSION TABLE
//! (`VERSION_CHECKS` in `xtask/src/main.rs`): the pairs below must be exactly
//! the `ApiKey::<Name> => <version>` arms of the pinned tarball's function,
//! and the fallback exactly its `_ =>` arm, on every `cargo test --workspace`.
//!
//! # Why Logweir needs the engine's versions at all
//!
//! **The engine never negotiates.** It does not send ApiVersions; every
//! request goes out at the version this table names, and any key the table
//! does not list goes out at [`ENGINE_FALLBACK_VERSION`]. An endpoint that
//! does not serve one of those versions closes the connection, and the engine
//! reports a transport error with no version in it. Measured on Redpanda
//! v26.2.4, which serves Produce v0-v7: a restore fails after five retries
//! with "Connection error during read response length (UnexpectedEof): early
//! eof" (`docs/to-do/decisions/PROD-01.2-compatibility-contract.md` §4).
//! Logweir's own client DOES negotiate, so it learns each endpoint's ranges on
//! the connection it already has, and can say before an operation starts
//! which request the engine would be refused.
//!
//! # Which requests an operation sends
//!
//! [`CAPTURE_REQUESTS`] and [`REPLAY_REQUESTS`] are a reading of the engine's
//! call sites, not text the gate can compare; a pin bump re-reads them:
//!
//! | API | call site (v0.23.3) | capture | replay |
//! |---|---|---|---|
//! | Metadata | `kafka/metadata.rs:62`, `kafka/partition_router.rs:158` | yes | yes |
//! | ListOffsets | `kafka/fetch.rs:267`, `:341` | yes | no |
//! | Fetch | `kafka/fetch.rs:56` | yes | no |
//! | DescribeConfigs | `kafka/admin.rs:472` (`capture_topic_configs`, on by default) | yes | no |
//! | Produce | `kafka/produce.rs:181` | no | yes |
//! | SaslHandshake, SaslAuthenticate | `kafka/client.rs:333-471` | on a SASL connection | on a SASL connection |
//!
//! CreateTopics (`kafka/admin.rs:205`) is NOT a replay request: Logweir
//! renders `create_topics: false` and creates the target topics itself
//! (`render_restore.rs`, guard G-TS). The consumer-group requests
//! (`kafka/consumer_groups.rs`) are not sent either: Logweir renders
//! `consumer_group_snapshot` off and `reset_consumer_offsets: false`.

/// The pinned engine's `get_api_version`, verbatim and in its order: the
/// `ApiKey` variant's name and the version the engine sends it at.
pub const ENGINE_REQUEST_VERSIONS: [(&str, i16); 18] = [
    ("Metadata", 9),
    ("Fetch", 11),
    ("Produce", 8),
    ("SaslHandshake", 1),
    ("SaslAuthenticate", 2),
    ("ApiVersions", 3),
    ("ListOffsets", 5),
    ("CreateTopics", 5),
    ("FindCoordinator", 2),
    ("OffsetFetch", 5),
    ("OffsetCommit", 5),
    ("ListGroups", 2),
    ("DeleteRecords", 1),
    ("DescribeConfigs", 1),
    ("IncrementalAlterConfigs", 1),
    ("DescribeAcls", 1),
    ("CreateAcls", 1),
    ("DeleteAcls", 1),
];

/// The function's `_ =>` arm: the version of every key the table omits.
pub const ENGINE_FALLBACK_VERSION: i16 = 0;

/// One request the engine sends: the Kafka API's name, its protocol key and
/// the version the engine sends it at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EngineRequest {
    /// The API's name, as the engine's `ApiKey` and Kafka's protocol guide
    /// spell it.
    pub api: &'static str,
    /// Kafka's protocol API key.
    pub key: i16,
    /// The version the engine sends.
    pub version: i16,
}

/// Kafka's protocol API key for each API an operation sends. A fact of the
/// Kafka protocol, not of the engine.
const API_KEYS: [(&str, i16); 7] = [
    ("Produce", 0),
    ("Fetch", 1),
    ("ListOffsets", 2),
    ("Metadata", 3),
    ("SaslHandshake", 17),
    ("DescribeConfigs", 32),
    ("SaslAuthenticate", 36),
];

/// What a CAPTURE (`kafka-backup backup`) sends, by API name — see the module
/// table.
pub const CAPTURE_REQUESTS: [&str; 4] = ["Metadata", "ListOffsets", "Fetch", "DescribeConfigs"];

/// What a REPLAY (`kafka-backup restore`) sends, by API name.
pub const REPLAY_REQUESTS: [&str; 2] = ["Metadata", "Produce"];

/// What the engine adds on a SASL connection, whichever operation it runs.
pub const SASL_REQUESTS: [&str; 2] = ["SaslHandshake", "SaslAuthenticate"];

/// The version the engine sends `api` at: its table's, or the fallback for a
/// name the table does not list.
#[must_use]
pub fn engine_version(api: &str) -> i16 {
    ENGINE_REQUEST_VERSIONS
        .iter()
        .find(|(name, _)| *name == api)
        .map_or(ENGINE_FALLBACK_VERSION, |(_, v)| *v)
}

fn requests(names: &[&'static str], sasl: bool) -> Vec<EngineRequest> {
    names
        .iter()
        .chain(SASL_REQUESTS.iter().filter(|_| sasl))
        .map(|api| EngineRequest {
            api,
            key: API_KEYS
                .iter()
                .find(|(name, _)| name == api)
                .map(|(_, key)| *key)
                .expect("every request name an operation sends has its API key above"),
            version: engine_version(api),
        })
        .collect()
}

/// Every request a capture sends, on a SASL connection or not.
#[must_use]
pub fn capture_requests(sasl: bool) -> Vec<EngineRequest> {
    requests(&CAPTURE_REQUESTS, sasl)
}

/// Every request a replay sends, on a SASL connection or not.
#[must_use]
pub fn replay_requests(sasl: bool) -> Vec<EngineRequest> {
    requests(&REPLAY_REQUESTS, sasl)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The table as data, so an edit is a visible diff beside the drift gate.
    #[test]
    fn the_table_is_the_pinned_engines_eighteen_arms() {
        assert_eq!(ENGINE_REQUEST_VERSIONS.len(), 18);
        assert_eq!(engine_version("Produce"), 8);
        assert_eq!(engine_version("Fetch"), 11);
        assert_eq!(engine_version("Metadata"), 9);
        assert_eq!(engine_version("ListOffsets"), 5);
        assert_eq!(engine_version("DescribeConfigs"), 1);
        // A key the table omits goes out at the fallback: DescribeGroups is
        // the one the engine has a call site for.
        assert_eq!(engine_version("DescribeGroups"), ENGINE_FALLBACK_VERSION);
    }

    #[test]
    fn a_capture_and_a_replay_send_what_the_module_table_says() {
        let capture: Vec<(&str, i16, i16)> = capture_requests(false)
            .iter()
            .map(|r| (r.api, r.key, r.version))
            .collect();
        assert_eq!(
            capture,
            vec![
                ("Metadata", 3, 9),
                ("ListOffsets", 2, 5),
                ("Fetch", 1, 11),
                ("DescribeConfigs", 32, 1),
            ]
        );
        let replay: Vec<(&str, i16, i16)> = replay_requests(true)
            .iter()
            .map(|r| (r.api, r.key, r.version))
            .collect();
        assert_eq!(
            replay,
            vec![
                ("Metadata", 3, 9),
                ("Produce", 0, 8),
                ("SaslHandshake", 17, 1),
                ("SaslAuthenticate", 36, 2),
            ]
        );
        // No SASL request on a connection without SASL.
        assert!(replay_requests(false)
            .iter()
            .all(|r| !r.api.starts_with("Sasl")));
    }
}
