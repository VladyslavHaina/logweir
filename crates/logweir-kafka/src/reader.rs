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

/// v0.1 shipped PLAINTEXT and SASL/SCRAM-SHA-512 (optionally over TLS);
/// PROD-01.3 adds SASL/SCRAM-SHA-256, SASL/PLAIN over TLS and TLS client
/// certificates (`mtls`). OAUTHBEARER and MSK IAM stay deferred (OD-3) behind
/// `crate::token::TokenProvider`; there is no AWS dependency in this crate
/// (Global Constraint 1).
///
/// `tls_ca_file` (PLAT-07.1) is the pod-local path of a projected private CA
/// certificate: when set, librdkafka's `ssl.ca.location` points at it and the
/// image's default trust store is not consulted for this connection. It is
/// attached with [`AuthConfig::with_tls_ca_file`], which refuses it for a
/// transport that is not TLS.
///
/// # The shapes that cannot be built
///
/// `Plain` has NO `tls` field: it is always `SASL_SSL`, because
/// [`AuthConfig::from_spec`] refuses a `plain` spec without TLS
/// ([`logweir_core::connection::PlainWithoutTls`]) and no other constructor
/// exists — so a client that sends a PLAIN password in the clear is not a
/// value of this type. `Mtls` is always `SSL` for the same reason
/// ([`logweir_core::connection::MtlsWithoutTls`]); its certificate pair is
/// attached by [`AuthConfig::with_client_certificate`], and a client config
/// built without one is refused.
#[derive(Clone)]
pub enum AuthConfig {
    Plaintext,
    ScramSha512 {
        username: String,
        password: String,
        tls: bool,
        tls_ca_file: Option<String>,
    },
    /// PROD-01.3. SASL/SCRAM-SHA-256, over TLS or not.
    ScramSha256 {
        username: String,
        password: String,
        tls: bool,
        tls_ca_file: Option<String>,
    },
    /// PROD-01.3. SASL/PLAIN, always over TLS (`SASL_SSL`).
    Plain {
        username: String,
        password: String,
        tls_ca_file: Option<String>,
    },
    /// PROD-01.3. A TLS client certificate, no SASL (`SSL`).
    Mtls {
        tls_ca_file: Option<String>,
        client_certificate: Option<logweir_core::connection::ClientCertificateFiles>,
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
    /// A SASL mode (`scramSha512`, `scramSha256`, `plain`) with `None` is
    /// `KafkaError::Client` — **operational, exit 1, NOT a guard refusal**.
    /// Nothing was refused: the plan is probably fine and the fix is to
    /// project the Secret, which is exactly what "retry" means to a
    /// reconciler. A guard refusal (exit 3) is reserved for a plan this build
    /// will never accept, and would tell Task 18's cron reconciler to stop
    /// retrying a condition an operator is about to fix. The unrenderable-VALUE
    /// case is the refusal, and it is raised by the caller before this
    /// function is reached (`logweir_core::guard::credential_is_renderable`,
    /// interface **I11**).
    ///
    /// The message cannot name WHICH of the two variables the caller read —
    /// this crate never saw it, and the signature above is interface I1's
    /// pinned shape — so `crates/logweir`'s
    /// `drill::naming_the_password_var` re-states it with the variable that
    /// call site actually reads.
    ///
    /// `Plaintext` and `Mtls` ignore `password` rather than refusing a present
    /// one: a Secret left projected after a spec was switched to a mode with
    /// no SASL is a tidiness problem, not a reason to fail a backup, and the
    /// runner's own `check_projected_credentials` has already validated
    /// whatever is there.
    ///
    /// # The transport refusals (PROD-01.3)
    ///
    /// `plain` and `mtls` without `tls: true` are `KafkaError::Client`
    /// carrying [`logweir_core::spec::AuthSpec::transport_refusal`]'s text.
    /// The runner refuses the same spec at exit 3 before this function is
    /// reached (`refusal-reason=PlainWithoutTls`); this is the backstop that
    /// makes such a client unconstructible.
    pub fn from_spec(
        auth: &logweir_core::spec::AuthSpec,
        password: Option<String>,
    ) -> Result<AuthConfig, KafkaError> {
        use logweir_core::spec::AuthSpec;
        if let Some(refusal) = auth.transport_refusal() {
            return Err(KafkaError::Client(refusal));
        }
        let missing = |mode: &str| {
            KafkaError::Client(format!(
                "auth.mode is {mode} but no SASL password was projected into this process; set \
                 $LOGWEIR_SOURCE_PASSWORD for a source cluster or $LOGWEIR_TARGET_PASSWORD for \
                 a target cluster. Nothing was refused: this is operational (exit 1), not a \
                 guard refusal (exit 3)"
            ))
        };
        match auth {
            AuthSpec::Plaintext => Ok(AuthConfig::Plaintext),
            AuthSpec::ScramSha512 { username, tls } => match password {
                Some(password) => Ok(AuthConfig::ScramSha512 {
                    username: username.clone(),
                    password,
                    tls: *tls,
                    // A spec names no trust anchor; see `with_tls_ca_file`.
                    tls_ca_file: None,
                }),
                None => Err(missing(auth.mode_str())),
            },
            AuthSpec::ScramSha256 { username, tls } => match password {
                Some(password) => Ok(AuthConfig::ScramSha256 {
                    username: username.clone(),
                    password,
                    tls: *tls,
                    tls_ca_file: None,
                }),
                None => Err(missing(auth.mode_str())),
            },
            // `transport_refusal` above has already refused `tls: false`.
            AuthSpec::Plain { username, .. } => match password {
                Some(password) => Ok(AuthConfig::Plain {
                    username: username.clone(),
                    password,
                    tls_ca_file: None,
                }),
                None => Err(missing(auth.mode_str())),
            },
            AuthSpec::Mtls { .. } => Ok(AuthConfig::Mtls {
                tls_ca_file: None,
                client_certificate: None,
            }),
        }
    }

    /// The mode as the documents spell it.
    pub fn mode_str(&self) -> &'static str {
        use logweir_core::connection as c;
        match self {
            AuthConfig::Plaintext => c::AUTH_MODE_PLAINTEXT,
            AuthConfig::ScramSha512 { .. } => c::AUTH_MODE_SCRAM_SHA_512,
            AuthConfig::ScramSha256 { .. } => c::AUTH_MODE_SCRAM_SHA_256,
            AuthConfig::Plain { .. } => c::AUTH_MODE_PLAIN,
            AuthConfig::Mtls { .. } => c::AUTH_MODE_MTLS,
            AuthConfig::Token(_) => "token",
        }
    }
}

impl AuthConfig {
    /// Attach a projected private CA file (PLAT-07.1), or refuse because the
    /// transport is not TLS.
    ///
    /// `None` returns `self` unchanged, so a connection that names no CA builds
    /// exactly the client it built before the field existed. The path is the
    /// one the runner read from `LOGWEIR_{SOURCE,TARGET}_TLS_CA_FILE`; it is not
    /// opened here — librdkafka opens it while creating the client and reports
    /// a missing or unparsable file as a client-creation error naming
    /// `ssl.ca.location`.
    ///
    /// # Errors
    ///
    /// `KafkaError::Client` when `ca_file` is `Some` and the transport is not
    /// TLS — the message is `logweir_core::connection::TlsCaWithoutTls`'s.
    pub fn with_tls_ca_file(self, ca_file: Option<String>) -> Result<AuthConfig, KafkaError> {
        let Some(ca_file) = ca_file else {
            return Ok(self);
        };
        let refuse = |mode: &'static str| {
            Err(KafkaError::Client(
                logweir_core::connection::TlsCaWithoutTls { mode }.to_string(),
            ))
        };
        match self {
            AuthConfig::ScramSha512 {
                username,
                password,
                tls: true,
                ..
            } => Ok(AuthConfig::ScramSha512 {
                username,
                password,
                tls: true,
                tls_ca_file: Some(ca_file),
            }),
            AuthConfig::ScramSha256 {
                username,
                password,
                tls: true,
                ..
            } => Ok(AuthConfig::ScramSha256 {
                username,
                password,
                tls: true,
                tls_ca_file: Some(ca_file),
            }),
            AuthConfig::Plain {
                username, password, ..
            } => Ok(AuthConfig::Plain {
                username,
                password,
                tls_ca_file: Some(ca_file),
            }),
            AuthConfig::Mtls {
                client_certificate, ..
            } => Ok(AuthConfig::Mtls {
                tls_ca_file: Some(ca_file),
                client_certificate,
            }),
            AuthConfig::Token(_) => Err(KafkaError::Client(
                "token auth (OAUTHBEARER / MSK IAM) is deferred (OD-3)".to_string(),
            )),
            other => refuse(other.mode_str()),
        }
    }

    /// Attach the projected client-certificate pair of an `mtls` connection
    /// (PROD-01.3), or refuse.
    ///
    /// `None` returns `self` unchanged for every mode but `Mtls`, whose client
    /// config is then refused when it is built: an mTLS dial with no
    /// certificate has no identity to present.
    ///
    /// # Errors
    ///
    /// `KafkaError::Client` with
    /// [`logweir_core::connection::ClientCertificateRefusal::NotMtls`]'s text
    /// when files are supplied for any other mode.
    pub fn with_client_certificate(
        self,
        files: Option<logweir_core::connection::ClientCertificateFiles>,
    ) -> Result<AuthConfig, KafkaError> {
        match (self, files) {
            (AuthConfig::Mtls { tls_ca_file, .. }, Some(files)) => Ok(AuthConfig::Mtls {
                tls_ca_file,
                client_certificate: Some(files),
            }),
            (other, Some(_)) => Err(KafkaError::Client(
                logweir_core::connection::ClientCertificateRefusal::NotMtls {
                    mode: other.mode_str(),
                }
                .to_string(),
            )),
            (other, None) => Ok(other),
        }
    }
}

// Manual `Debug`, not `#[derive]`: a derived impl would print `password`
// verbatim, and `AuthConfig` reaches `{:?}` far too easily to trust — a
// tracing field, an error context, a config dump — for a derive to be safe
// here. Every other field is left exactly as a derive would render it. The
// mTLS arm prints the two PATHS (where a volume is mounted), never key bytes,
// which no Logweir process holds.
impl std::fmt::Debug for AuthConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuthConfig::Plaintext => write!(f, "Plaintext"),
            AuthConfig::ScramSha512 {
                username,
                password: _,
                tls,
                tls_ca_file,
            } => f
                .debug_struct("ScramSha512")
                .field("username", username)
                .field("password", &"***")
                .field("tls", tls)
                // A path, not certificate text and not a credential.
                .field("tls_ca_file", tls_ca_file)
                .finish(),
            AuthConfig::ScramSha256 {
                username,
                password: _,
                tls,
                tls_ca_file,
            } => f
                .debug_struct("ScramSha256")
                .field("username", username)
                .field("password", &"***")
                .field("tls", tls)
                .field("tls_ca_file", tls_ca_file)
                .finish(),
            AuthConfig::Plain {
                username,
                password: _,
                tls_ca_file,
            } => f
                .debug_struct("Plain")
                .field("username", username)
                .field("password", &"***")
                .field("tls_ca_file", tls_ca_file)
                .finish(),
            AuthConfig::Mtls {
                tls_ca_file,
                client_certificate,
            } => f
                .debug_struct("Mtls")
                .field("tls_ca_file", tls_ca_file)
                .field("client_certificate", client_certificate)
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

/// Where one value in a DescribeConfigs answer came from — Kafka's
/// `ConfigSource`, as librdkafka reports it per entry (FX-4).
///
/// `DynamicTopicConfig` is a TOPIC OVERRIDE; the next four are the broker's
/// (a per-broker dynamic value, the cluster-wide dynamic default, the broker's
/// static `server.properties`, and the built-in default). `Unknown` is what a
/// broker before Kafka 1.1 reports, and what this build reports for a source
/// it does not map.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigSourceKind {
    DynamicTopicConfig,
    DynamicBrokerConfig,
    DynamicDefaultBrokerConfig,
    StaticBrokerConfig,
    DefaultConfig,
    Unknown,
}

impl ConfigSourceKind {
    /// The spelling the backup receipt carries — Kafka's own names,
    /// camel-cased (`logweir_core::backup_receipt::CONFIG_SOURCES`).
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::DynamicTopicConfig => "dynamicTopicConfig",
            Self::DynamicBrokerConfig => "dynamicBrokerConfig",
            Self::DynamicDefaultBrokerConfig => "dynamicDefaultBrokerConfig",
            Self::StaticBrokerConfig => "staticBrokerConfig",
            Self::DefaultConfig => "defaultConfig",
            Self::Unknown => "unknown",
        }
    }
}

/// One entry of a topic's DescribeConfigs answer, with the flags a capture
/// decision needs — what [`ClusterReader::topic_configs`]'s flat map throws
/// away (FX-4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigEntryObservation {
    pub name: String,
    /// `None` for a sensitive value the broker withholds.
    pub value: Option<String>,
    pub source: ConfigSourceKind,
    pub read_only: bool,
    pub sensitive: bool,
}

/// One topic's DescribeConfigs answer: every entry the broker reported, or
/// why there is none. `Ok` is NEVER empty — see [`empty_topic_config_answer`].
pub type TopicConfigRead = Result<Vec<ConfigEntryObservation>, KafkaError>;

/// What the same principal's METADATA says about a topic, read to explain an
/// empty DescribeConfigs answer ([`empty_topic_config_answer`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TopicVisibility {
    /// Metadata answered for the topic with no error.
    Visible,
    /// `TOPIC_AUTHORIZATION_FAILED`, which Kafka returns whether or not the
    /// topic exists.
    NotAuthorized,
    /// `UNKNOWN_TOPIC_OR_PARTITION`.
    NotFound,
    /// The metadata read failed, or answered with another error.
    Unread(String),
}

/// **T13, the rule this crate reads DescribeConfigs by: an EMPTY answer is a
/// failed read, never "no overrides".** Pure, so every reader shares one
/// decision and a test can pin it without a broker.
///
/// rust-rdkafka 0.36.2's `DescribeConfigsFuture` returns `Ok` for EVERY
/// resource and never reads `rd_kafka_ConfigResource_error`
/// (`rdkafka-0.36.2/src/admin.rs:1121-1159`; unchanged through 0.39.0,
/// PROD-04.0 §6 T13), so a resource the broker refused comes back as `Ok` with
/// no entries. The per-resource error code is not readable without FFI, which
/// is owner choice AP-OC1 and not taken here.
///
/// A broker never answers a successful describe that way. Kafka 4.3.1 answers
/// an authorised topic that exists with every `LogConfig` entry, and fills an
/// EMPTY list only beside a per-resource error: `TOPIC_AUTHORIZATION_FAILED`
/// for a principal without `DESCRIBE_CONFIGS` on the topic,
/// `UNKNOWN_TOPIC_OR_PARTITION` for a topic its metadata cache does not hold,
/// or the code of an exception (`core/src/main/scala/kafka/server/
/// ConfigHelper.scala:54-79`, `:88-98`, `:144-158` at tag 4.3.1, commit
/// `26b251a4`; the fetched copy's sha256 is in the FX-4 report).
///
/// So the failure is named from what the safe client CAN read, the topic's
/// metadata under the same principal:
///
/// | metadata | answer |
/// |---|---|
/// | visible | [`KafkaError::NotAuthorized`] — the topic exists and is visible, so of the errors above only the authorizer's remains, short of an internal broker error |
/// | `TOPIC_AUTHORIZATION_FAILED` | [`KafkaError::NotAuthorized`] |
/// | `UNKNOWN_TOPIC_OR_PARTITION` | [`KafkaError::TopicNotFound`] |
/// | unread | [`KafkaError::Client`], naming both reads |
#[must_use]
pub fn empty_topic_config_answer(topic: &str, visibility: &TopicVisibility) -> KafkaError {
    match visibility {
        TopicVisibility::Visible => KafkaError::NotAuthorized(format!(
            "{topic} (DescribeConfigs answered this visible topic with no configuration, which \
             Kafka does only beside TOPIC_AUTHORIZATION_FAILED: the principal lacks \
             DescribeConfigs on it)"
        )),
        TopicVisibility::NotAuthorized => KafkaError::NotAuthorized(format!(
            "{topic} (DescribeConfigs answered with no configuration, and metadata refused the \
             topic too)"
        )),
        TopicVisibility::NotFound => KafkaError::TopicNotFound(topic.to_string()),
        TopicVisibility::Unread(why) => KafkaError::Client(format!(
            "{topic}: DescribeConfigs answered with no configuration and the topic's metadata \
             could not be read to say why ({why})"
        )),
    }
}

/// T13 for a BROKER resource: an empty answer is a refused read.
///
/// Kafka answers a broker resource with no configuration only beside
/// `CLUSTER_AUTHORIZATION_FAILED`, or an `INVALID_REQUEST` for a broker id
/// that is not its own (`ConfigHelper.scala:54-79`, `:100-109`).
/// Every caller in this workspace takes the id from the same broker's
/// metadata, so the refusal is the authorizer's. MEASURED by PROD-04.0 (§3.8):
/// a principal refused on the cluster read `authorizer.class.name` as MISSING,
/// "no entries and no error".
#[must_use]
pub fn empty_broker_config_answer(broker_id: i32) -> KafkaError {
    KafkaError::NotAuthorized(format!(
        "broker {broker_id} configuration (DescribeConfigs answered with no configuration, \
         which Kafka does only beside CLUSTER_AUTHORIZATION_FAILED: the principal lacks \
         DescribeConfigs on the cluster)"
    ))
}

/// **FX-18.** How long a caller that has just CREATED a topic waits for the
/// cluster to serve it before reading it.
///
/// A successful `CreateTopics` means the controller has committed the topic,
/// not that every broker has applied it. Until they have, a read of the new
/// topic gets the answers [`Settling::NotYet`] names: `UnknownTopicOrPartition`
/// from a broker whose metadata does not hold it yet, `LeaderNotAvailable`
/// before a leader is elected, `NotLeaderForPartition` from a broker that has
/// not yet become the leader it is listed as. On an idle single broker that
/// window is milliseconds; main CI run 37753000930 (`e2e/tests/topic_identity.rs`
/// c02) and PROD-00.3f's matrix row (`e2e/tests/guards.rs`) each hit it once.
/// Thirty seconds is a bound for a wedged cluster, not an expected wait: a
/// served topic ends the wait at once.
pub const CREATED_TOPIC_SETTLE: std::time::Duration = std::time::Duration::from_secs(30);

/// The longest pause between two attempts in [`settle`].
const SETTLE_MAX_PAUSE: std::time::Duration = std::time::Duration::from_secs(1);

/// **FX-18.** One attempt at a read that may be racing a topic's creation.
#[derive(Debug)]
pub enum Settling<T> {
    /// The read answered.
    Done(T),
    /// An answer a topic gives while its creation is still propagating. It is
    /// retried until the deadline and then returned as it is, so a caller that
    /// classifies the error (not found, not authorized) still can.
    NotYet(KafkaError),
    /// Any other answer. Returned at once: waiting cannot change it.
    Failed(KafkaError),
}

/// **FX-18.** Run `attempt` until it is [`Settling::Done`] or
/// [`Settling::Failed`], retrying [`Settling::NotYet`] with a doubling pause
/// (50 ms up to one second) until `within` has passed, then returning the last
/// `NotYet` error.
///
/// A bounded poll on the condition, never a fixed sleep: a topic that is
/// served on the first attempt costs no wait at all, and one that never is
/// costs `within` and the answer it last gave.
///
/// # Errors
///
/// The `Failed` error at once, or the last `NotYet` error at the deadline.
pub fn settle<T>(
    within: std::time::Duration,
    mut attempt: impl FnMut() -> Settling<T>,
) -> Result<T, KafkaError> {
    let deadline = std::time::Instant::now() + within;
    let mut pause = std::time::Duration::from_millis(50);
    loop {
        match attempt() {
            Settling::Done(value) => return Ok(value),
            Settling::Failed(error) => return Err(error),
            Settling::NotYet(error) => {
                let now = std::time::Instant::now();
                if now >= deadline {
                    return Err(error);
                }
                std::thread::sleep(pause.min(deadline - now));
                pause = (pause * 2).min(SETTLE_MAX_PAUSE);
            }
        }
    }
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
    ///
    /// **`Ok` is never an empty map standing for a refused read** (FX-4, T13):
    /// a denied topic is [`KafkaError::NotAuthorized`] and an unknown one
    /// [`KafkaError::TopicNotFound`] — see [`empty_topic_config_answer`].
    fn topic_configs(&self, topic: &str) -> Result<BTreeMap<String, String>, KafkaError>;
    /// DescribeConfigs for ResourceSpecifier::Broker(i32). NOT topic_configs:
    /// the mapped target topics do not exist at phase 0 (a Restore refuses if
    /// any of them does), so a TOPIC-resource read would describe nothing.
    ///
    /// **`Ok` is never an empty map standing for a refused read** (FX-4, T13):
    /// a refused cluster read is [`KafkaError::NotAuthorized`] — see
    /// [`empty_broker_config_answer`].
    fn broker_configs(&self) -> Result<BTreeMap<String, String>, KafkaError>;
    /// FX-4: DescribeConfigs for every named TOPIC in ONE request, keeping
    /// each entry's SOURCE and flags — what a backup needs to record capture
    /// coverage and the effective `message.timestamp.type`.
    ///
    /// One `(topic, answer)` per name, in the order given. The outer `Err` is
    /// the call itself failing (no broker answered); an inner one is that
    /// topic's own failure, with an empty answer named by
    /// [`empty_topic_config_answer`] — so an inner `Ok` always holds entries.
    ///
    /// The DEFAULT reports that this reader cannot answer, so a reader that
    /// does not implement it — every test double written before FX-4 — can
    /// only make a coverage record WEAKER (`notCaptured`), never `captured`.
    fn describe_topic_configs(
        &self,
        topics: &[String],
    ) -> Result<Vec<(String, TopicConfigRead)>, KafkaError> {
        let _ = topics;
        Err(KafkaError::Client(
            "this ClusterReader does not report per-entry configuration sources".to_string(),
        ))
    }
    /// **PROD-05.1.** Per named topic, its replication factor as the
    /// cluster's metadata states it: the SMALLEST replica count of its
    /// partitions (a partition mid-reassignment lists the adding replicas too,
    /// so the smallest never overstates), from one metadata request. A topic
    /// the answer does not name, or names with an error or no partition, is
    /// ABSENT — not recorded, never `0`.
    ///
    /// The DEFAULT knows none, so a reader that does not implement it — every
    /// test double — can only leave the factor unrecorded.
    fn replication_factors(&self, topics: &[String]) -> Result<BTreeMap<String, u32>, KafkaError> {
        let _ = topics;
        Ok(BTreeMap::new())
    }
    /// **FX-18.** Wait, at most `within`, until `topic` — which the caller has
    /// just CREATED with `partitions` partitions — is SERVED: its metadata
    /// lists every partition with a leader, and every leader answers a
    /// ListOffsets read. Only the answers of a creation still propagating are
    /// waited out ([`Settling::NotYet`]); any other answer is returned at once.
    ///
    /// Call it between a create and the first read or write that needs the
    /// topic (a watermark read, a DescribeConfigs, a restore). A producer
    /// retries these answers on its own; a one-shot read does not.
    ///
    /// The DEFAULT is `Ok(())` at once: a reader with no cluster behind it —
    /// every test double — has no propagation to wait for.
    ///
    /// # Errors
    ///
    /// The last answer the topic gave, when it is still not served at
    /// `within`; any other failure at once.
    fn await_served(
        &self,
        topic: &str,
        partitions: i32,
        within: std::time::Duration,
    ) -> Result<(), KafkaError> {
        let _ = (topic, partitions, within);
        Ok(())
    }
    /// **FX-18.** [`Self::topic_configs`] for a topic the caller has just
    /// CREATED: [`Self::await_served`], then the read, retrying
    /// [`KafkaError::TopicNotFound`] (a broker whose metadata does not hold the
    /// topic yet) until `within` from the start.
    ///
    /// The DEFAULT returns any other answer — [`KafkaError::NotAuthorized`]
    /// included — at once. `RdKafkaReader` also waits out `NotAuthorized`,
    /// because there that error is T13's inference from an EMPTY answer
    /// ([`empty_topic_config_answer`]), which a just-created topic gives before
    /// the answering broker holds it and which metadata read a moment later
    /// then calls "visible".
    ///
    /// # Errors
    ///
    /// As [`Self::await_served`], then as [`Self::topic_configs`].
    fn created_topic_configs(
        &self,
        topic: &str,
        partitions: i32,
        within: std::time::Duration,
    ) -> Result<BTreeMap<String, String>, KafkaError> {
        let started = std::time::Instant::now();
        self.await_served(topic, partitions, within)?;
        settle(within.saturating_sub(started.elapsed()), || {
            match self.topic_configs(topic) {
                Ok(configs) => Settling::Done(configs),
                Err(e @ KafkaError::TopicNotFound(_)) => Settling::NotYet(e),
                Err(e) => Settling::Failed(e),
            }
        })
    }
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
