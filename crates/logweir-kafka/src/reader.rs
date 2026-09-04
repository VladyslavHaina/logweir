use crate::fingerprint::record_fingerprint;
use std::collections::BTreeMap;

#[derive(Debug, thiserror::Error)]
pub enum KafkaError {
    #[error("kafka: {0}")]
    Client(String),
    #[error("timeout after {0:?}")]
    Timeout(std::time::Duration),
    /// The named topic does not exist on the target cluster (broker code
    /// `UnknownTopicOrPartition`, verified against rdkafka-sys
    /// 4.10.0+2.12.1's error table — see `rdkafka_reader.rs`'s
    /// `classify_topic_error`).
    #[error("topic not found: {0}")]
    TopicNotFound(String),
    /// The authenticated principal may not describe/read the named topic
    /// (broker code `TopicAuthorizationFailed`), distinguished from
    /// `TopicNotFound` so a caller does not send an operator chasing a
    /// permissions problem down the "topic is missing" path or vice versa.
    #[error("not authorized: {0}")]
    NotAuthorized(String),
    /// No broker answered within the call's own budget. Distinct from
    /// `TopicNotFound`/`NotAuthorized`: nothing about the topic's existence
    /// or the principal's rights is known either way — the call simply
    /// never got an answer.
    #[error("unreachable: {0}")]
    Unreachable(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicMeta {
    pub name: String,
    pub partitions: i32,
}

#[derive(Debug, Clone)]
pub struct ConsumedRecord {
    pub partition: i32,
    pub offset: i64,
    pub timestamp_ms: i64,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Vec<(String, Option<Vec<u8>>)>,
}

impl ConsumedRecord {
    pub fn fingerprint(&self) -> String {
        record_fingerprint(
            self.key.as_deref(),
            self.value.as_deref(),
            &self.headers,
            self.timestamp_ms,
        )
    }
}

/// v0.1 ships PLAINTEXT and SASL/SCRAM over TLS. OAUTHBEARER and MSK IAM
/// arrive in SP4 through `crate::token::TokenProvider`; there is no AWS
/// dependency in this crate in v0.1 (Global Constraint 1).
#[derive(Clone)]
pub enum AuthConfig {
    Plaintext,
    ScramSha512 {
        username: String,
        password: String,
        tls: bool,
    },
    /// SP4. Constructing this in v0.1 returns KafkaError::Client.
    Token(std::sync::Arc<dyn crate::token::TokenProvider>),
}

// Manual `Debug`, not `#[derive]`: a derived impl would print `password`
// verbatim, and `AuthConfig` reaches `{:?}` far too easily to trust — a
// tracing field, an error context, a config dump — for a derive to be safe
// here. Every other field is left exactly as a derive would render it.
impl std::fmt::Debug for AuthConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuthConfig::Plaintext => write!(f, "Plaintext"),
            AuthConfig::ScramSha512 {
                username,
                password: _,
                tls,
            } => f
                .debug_struct("ScramSha512")
                .field("username", username)
                .field("password", &"***")
                .field("tls", tls)
                .finish(),
            AuthConfig::Token(provider) => f.debug_tuple("Token").field(provider).finish(),
        }
    }
}

/// Phase 9's teardown seam, kept in `logweir-kafka` so `crates/logweir` never
/// takes an rdkafka dependency and the layering rule ("logweir-kafka is the only
/// crate that dials a broker") stays literally true. It is deliberately a
/// SECOND, narrow trait rather than a method on `ClusterReader`: a reader is
/// read-only, and the only write the drill ever performs is deleting the exact
/// scratch topics it created.
pub trait TopicDeleter: Send + Sync {
    /// Deletes exactly the named topics. Never a pattern, never a prefix.
    /// Returns one result per name so a partial failure is reportable.
    ///
    /// The nested `Result` in the return type is the brief's and addendum's
    /// exact, binding signature (one outer `Result` for the call itself, one
    /// inner `Result` per topic name so a partial failure is reportable) —
    /// `#[allow]` rather than a type alias keeps it exactly as specified.
    #[allow(clippy::type_complexity)]
    fn delete_topics(
        &self,
        names: &[String],
    ) -> Result<Vec<(String, Result<(), String>)>, KafkaError>;
}

pub trait ClusterReader: Send + Sync {
    fn cluster_id(&self) -> Result<String, KafkaError>;
    fn list_topics(&self) -> Result<Vec<TopicMeta>, KafkaError>;
    /// (partition, high watermark) — the ListOffsets read used by the phase-3
    /// diff, the phase-6 post-condition and the phase-7 restored-count check.
    fn end_offsets(&self, topic: &str) -> Result<Vec<(i32, i64)>, KafkaError>;
    /// DescribeConfigs for ConfigResource TOPIC only.
    fn topic_configs(&self, topic: &str) -> Result<BTreeMap<String, String>, KafkaError>;
    fn consume_range(
        &self,
        topic: &str,
        partition: i32,
        from: i64,
        max: usize,
    ) -> Result<Vec<ConsumedRecord>, KafkaError>;
}
