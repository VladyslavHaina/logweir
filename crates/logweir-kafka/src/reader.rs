use crate::fingerprint::record_fingerprint;
use std::collections::BTreeMap;

#[derive(Debug, Clone, thiserror::Error)]
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
    /// `Some(<message>)` when the broker's metadata for this specific topic
    /// carried an error (absent, not authorized, or a transient state such
    /// as leader election just after `create_topics`) — `partitions` is `0`
    /// and not meaningful in that case.
    ///
    /// `list_topics` never drops an entry for this reason: a topic mid-
    /// election disappearing from the list would make a later completeness
    /// check ("this scratch cluster holds nothing but my drill topics")
    /// wrongly conclude the cluster is emptier than it actually is. A caller
    /// that specifically wants "confirmed healthy and present" — the
    /// phase-0 marker-topic guard, for one — checks `error.is_none()`
    /// itself rather than relying on absence from this list to mean that.
    pub error: Option<String>,
}

impl TopicMeta {
    /// A topic whose metadata was read successfully.
    pub fn new(name: impl Into<String>, partitions: i32) -> Self {
        Self {
            name: name.into(),
            partitions,
            error: None,
        }
    }

    /// A topic whose metadata carried an error — still reported, never
    /// dropped. `partitions` is `0` and not meaningful.
    pub fn errored(name: impl Into<String>, error: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            partitions: 0,
            error: Some(error.into()),
        }
    }
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

impl AuthConfig {
    /// Interface **I1**: the ONE construction site every caller uses.
    ///
    /// Three call sites in the workspace build a client — `drill::context`,
    /// `doctor::check_target` and `backup::run` — and all three went through
    /// this function in Task 6. Before it, two of them hard-coded
    /// `AuthConfig::Plaintext` with a comment explaining that `TargetSpec`
    /// carried no auth block to render one from, so SASL was unreachable from
    /// any shipped spec. `crates/logweir/tests/auth_binding.rs::
    /// no_construction_site_hardcodes_plaintext` reads those three sources and
    /// asserts each names this function and none names a bare
    /// `AuthConfig::Plaintext`, so the next editor cannot quietly re-pin one.
    ///
    /// # `password` is `Option<String>`, and why the absent case is exit 1
    ///
    /// The secret is NEVER in a spec, a plan, a rendered document or a
    /// receipt: it is projected into the process environment and read there
    /// (`LOGWEIR_SOURCE_PASSWORD` / `LOGWEIR_TARGET_PASSWORD`). This function
    /// takes what the caller read, so it can be unit-tested with no
    /// environment at all.
    ///
    /// `ScramSha512` with `None` is `KafkaError::Client` — **operational,
    /// exit 1, NOT a guard refusal**. Nothing was refused: the plan is
    /// probably fine and the fix is to project the Secret, which is exactly
    /// what "retry" means to a reconciler. A guard refusal (exit 3) is
    /// reserved for a plan this build will never accept, and would tell
    /// Task 18's cron reconciler to stop retrying a condition an operator is
    /// about to fix. The unrenderable-VALUE case is the refusal, and it is
    /// raised by the caller before this function is reached
    /// (`logweir_core::guard::credential_is_renderable`, interface **I11**).
    ///
    /// The message cannot name WHICH of the two variables the caller read —
    /// this crate never saw it, and the signature above is interface I1's
    /// pinned shape — so `crates/logweir`'s
    /// `drill::naming_the_password_var` re-states it with the variable that
    /// call site actually reads.
    ///
    /// `Plaintext` ignores `password` rather than refusing a present one: a
    /// Secret left projected after a spec was switched back to plaintext is a
    /// tidiness problem, not a reason to fail a backup, and the runner's own
    /// `check_projected_credentials` has already validated whatever is there.
    pub fn from_spec(
        auth: &logweir_core::spec::AuthSpec,
        password: Option<String>,
    ) -> Result<AuthConfig, KafkaError> {
        match auth {
            logweir_core::spec::AuthSpec::Plaintext => Ok(AuthConfig::Plaintext),
            logweir_core::spec::AuthSpec::ScramSha512 { username, tls } => match password {
                Some(password) => Ok(AuthConfig::ScramSha512 {
                    username: username.clone(),
                    password,
                    tls: *tls,
                }),
                None => Err(KafkaError::Client(
                    "auth.mode is scramSha512 but no SASL password was projected into this \
                     process; set $LOGWEIR_SOURCE_PASSWORD for a source cluster or \
                     $LOGWEIR_TARGET_PASSWORD for a target cluster. Nothing was refused: this \
                     is operational (exit 1), not a guard refusal (exit 3)"
                        .to_string(),
                )),
            },
        }
    }
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
    /// DescribeConfigs for `ResourceSpecifier::Topic(&str)` only — a TOPIC
    /// resource and nothing else. `ResourceSpecifier` is rdkafka 0.36's
    /// DescribeConfigs INPUT type; the Java client's name for that role is a
    /// different thing and is not it (see `broker_configs`).
    fn topic_configs(&self, topic: &str) -> Result<BTreeMap<String, String>, KafkaError>;
    /// DescribeConfigs for ResourceSpecifier::Broker(i32). NOT topic_configs:
    /// the mapped target topics do not exist at phase 0 (a Restore refuses if
    /// any of them does), so a TOPIC-resource read would describe nothing.
    fn broker_configs(&self) -> Result<BTreeMap<String, String>, KafkaError>;
    fn consume_range(
        &self,
        topic: &str,
        partition: i32,
        from: i64,
        max: usize,
    ) -> Result<Vec<ConsumedRecord>, KafkaError>;
}

/// The SECOND write seam this crate exposes (the first is TopicDeleter,
/// reader.rs:135-148).
///
/// **Guard G-TS.** Logweir creates the restore's target topics itself, with
/// the config entries that decide whether the restore survives at all, because
/// the engine's own creation path carries no configuration whatsoever
/// (`TopicToCreate { name, num_partitions, replication_factor }`,
/// `U:crates/kafka-backup-core/src/restore/engine.rs:1447-1455`) — so on a
/// topic left on cluster defaults the restored segments are written already
/// past a `retention.ms` deletion threshold, or every restored timestamp is
/// overwritten by `message.timestamp.type = LogAppendTime`, and the drill signs
/// a `pass` over records the broker is about to delete or has already
/// re-stamped.
///
/// Unlike `TopicDeleter`, this needs no prefix scope: creation is not
/// destruction, and a `Restore` already refuses if any mapped target topic
/// exists.
pub trait TopicCreator: Send + Sync {
    /// Creates exactly the named topics with exactly the given config entries.
    /// Never a pattern, never a default set. One result per name.
    #[allow(clippy::type_complexity)]
    fn create_topics(
        &self,
        topics: &[NewTopicSpec],
    ) -> Result<Vec<(String, Result<(), String>)>, KafkaError>;
}

/// One topic to create, fully specified. Nothing here is defaulted by the
/// broker and nothing is inferred: the name, the partition count, the
/// replication factor and the ordered config entries are all decided by the
/// caller, so the whole set is assertable by a golden.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewTopicSpec {
    pub name: String,
    pub num_partitions: i32,
    pub replication_factor: i32,
    /// Explicit, ordered, and asserted by a golden.
    pub configs: Vec<(String, String)>,
}

/// The config entries **every** target topic Logweir creates carries, in this
/// order. Ordered because the order is what `rdkafka::admin::NewTopic::set` is
/// called in, and a test asserts the recorded vector, not a set.
///
/// - `message.timestamp.type = CreateTime` — the engine builds its batches with
///   `timestamp_type: TimestampType::Creation` and the record's ORIGINAL
///   timestamp (`U:crates/kafka-backup-core/src/kafka/produce.rs:101-104`).
///   `LogAppendTime` on the target would overwrite every one of them with the
///   restore's wall clock, voiding any timestamp lookup and any point-in-time
///   claim.
/// - `retention.ms = -1` — infinite. A topic on cluster defaults typically
///   carries `retention.ms = 604800000`, so restoring an older point writes
///   segments already past the deletion threshold, removed on the next
///   retention check — possibly AFTER phase 7 signed a `pass`.
pub const TARGET_TOPIC_CONFIGS: &[(&str, &str)] = &[
    ("message.timestamp.type", "CreateTime"),
    ("retention.ms", "-1"),
];
