use crate::positions::{
    commit_request, fetch_request, valid_bound, CommitError, CommittedPosition, GroupListing,
    GroupPositions, PositionsError, TopicPartition, DEFAULT_POSITION_BOUND,
};
use crate::rdkafka_positions::GroupHandle;
use crate::reader::{
    empty_broker_config_answer, empty_topic_config_answer, settle, AuthConfig, ClusterReader,
    ConfigEntryObservation, ConfigSourceKind, ConsumedRecord, KafkaError, NewTopicSpec, Settling,
    TopicConfigRead, TopicCreator, TopicDeleter, TopicMeta, TopicVisibility,
};
use rdkafka::admin::AdminClient;
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
// `BorrowedHeaders::count`/`::get` (used in `consume_range` below) are
// provided by the `Headers` trait, not the type itself.
use rdkafka::message::Headers;
use rdkafka::{Message, Offset, TopicPartitionList};
use std::collections::BTreeMap;
use std::time::Duration;

const T: Duration = Duration::from_secs(20);

/// **FX-43, channel (a): a client's own log lines, on one line each.**
///
/// rust-rdkafka 0.36.2 installs no log callback, so librdkafka's default
/// logger prints a client's lines RAW on stderr, where a bootstrap address or
/// a broker's message holding a line break starts a line of the pod log a
/// controller reads by key. Every client this crate builds for a runner is
/// created with `log.queue=true` ([`RdKafkaReader::client_config`],
/// `inventory`'s twin) and wrapped by [`pumped`], so its lines reach
/// [`print_client_log`] instead, escaped.
///
/// # Errors
///
/// The text of `logweir_rdkafka_ffi`'s refusal (a handle built without
/// `log.queue=true`, a thread that could not start).
pub(crate) fn pumped<C: logweir_rdkafka_ffi::logs::OwnsClient>(
    client: C,
) -> Result<logweir_rdkafka_ffi::logs::LogPumped<C>, String> {
    logweir_rdkafka_ffi::logs::LogPumped::new(client, print_client_log)
        .map_err(|e| format!("the client's log could not be routed: {e:?}"))
}

/// One client log line as this process prints it: `librdkafka:
/// %<level>|<facility>| <message>`, on ONE line whatever it holds
/// (`logweir_core::runner_line::one_line`). Pure.
#[must_use]
pub fn client_log_line(line: &logweir_rdkafka_ffi::logs::LogLine) -> String {
    let text = |t: &logweir_rdkafka_ffi::CText| match t {
        logweir_rdkafka_ffi::CText::Utf8(s) => s.clone(),
        logweir_rdkafka_ffi::CText::NotUtf8(b) => String::from_utf8_lossy(b).into_owned(),
        logweir_rdkafka_ffi::CText::Null => String::new(),
    };
    logweir_core::runner_line::one_line(&format!(
        "librdkafka: %{}|{}| {}",
        line.level,
        text(&line.facility),
        text(&line.message)
    ))
}

/// The sink every runner client's log lines go to: stderr, one line each.
fn print_client_log(line: &logweir_rdkafka_ffi::logs::LogLine) {
    eprintln!("{}", client_log_line(line));
}

/// **How long the BROKER may take over `CreateTopics` before it answers, set
/// explicitly BELOW the client's request timeout [`T`].**
///
/// librdkafka's operation timeout defaults to the admin request timeout of
/// the client configuration (`rdkafka_admin.c`, `socket.timeout.ms`, 60 s),
/// which is LONGER than the 20 s this client waits. So a creation the
/// controller finished between 20 s and 60 s was applied on the cluster while
/// the call here failed as a whole, with no answer for any name.
///
/// WHAT THIS BUYS: a broker that is reachable and answering returns within
/// this bound with ONE ANSWER PER NAME — `REQUEST_TIMED_OUT` for a topic it
/// has not finished creating — and the remaining [`T`] minus this is the
/// round trip's. The creation step can then say which names it cannot
/// account for.
///
/// WHAT IT DOES NOT GUARANTEE: an answer at all (a lost connection, a broker
/// that stops responding, or a controller lookup that eats the margin still
/// fails the whole call), or that a name answered `REQUEST_TIMED_OUT` is not
/// created afterwards — Kafka goes on creating it. Both are what
/// `logweir::drill::phase0_admit::Unconfirmed::look` is for.
pub const CREATE_OPERATION_TIMEOUT: Duration = Duration::from_secs(15);

const _: () = assert!(
    CREATE_OPERATION_TIMEOUT.as_secs() < T.as_secs(),
    "the broker's operation timeout must be below the client's request timeout"
);

pub struct RdKafkaReader {
    consumer: logweir_rdkafka_ffi::logs::LogPumped<BaseConsumer>,
    admin: logweir_rdkafka_ffi::logs::LogPumped<AdminClient<DefaultClientContext>>,
    /// The connection `connect` dialled with ([`RdKafkaReader::client_config`]),
    /// kept so [`RdKafkaReader::committed_positions`] and
    /// [`RdKafkaReader::commit_positions`] can open a per-group handle on the
    /// same cluster as the same principal. It holds the SASL password, as the
    /// two clients above already do inside librdkafka; this type has no
    /// `Debug` and never prints it.
    base: ClientConfig,
    /// The bound of one positions fetch or commit wait.
    position_bound: Duration,
    /// The bound of one admin call through `logweir-rdkafka-ffi`
    /// (`crate::rdkafka_admin`).
    admin_bound: Duration,
    /// The drill's own scratch-topic namespace. `TopicDeleter::delete_topics`
    /// refuses any name that does not start with this prefix, and refuses
    /// EVERYTHING until it is set via `with_scratch_prefix` — `connect`
    /// alone never enables deletion. This is the only code path in the whole
    /// product that destroys data, and short of this guard its only
    /// protection was "the caller passed the right names" (see
    /// `delete_topics`'s doc comment for the fuller rationale).
    scratch_prefix: Option<String>,
    /// Names `delete_topics` refuses WHATEVER the prefix: the
    /// source topics' own names, set by the drill via
    /// [`RdKafkaReader::with_protected_names`]. An original topic name may
    /// start with the scratch prefix (a source topic called `drill-orders`
    /// under `topic_mapping_prefix: drill-`), and teardown must never delete
    /// an original-name topic.
    protected_names: std::collections::BTreeSet<String>,
    /// PROD-07.2: when set, `delete_topics` deletes only these exact names
    /// (a cleanup's mapped target names) instead of the scratch prefix's
    /// namespace. [`RdKafkaReader::with_deletable_names`].
    deletable_names: Option<std::collections::BTreeSet<String>>,
}

/// The two TLS controls every TLS arm of [`RdKafkaReader::client_config`] sets.
///
/// Hostname verification is pinned explicitly rather than left to the default,
/// for the reason `allow.auto.create.topics` is: it is librdkafka 2.x's
/// default (`https`) and this pins it, so an upgrade cannot quietly turn it
/// off. There is deliberately no override — the engine's rustls client
/// verifies the broker hostname with no way to turn that off, and the two
/// clients must agree. [VERIFIED rdkafka-sys 4.10.0+2.12.1's
/// vendored librdkafka/CONFIGURATION.md: ssl.endpoint.identification.algorithm
/// `none, https`, default `https`.]
///
/// With a CA file, `SSL_CTX_load_verify_locations` on that file ONLY: with
/// `ssl.ca.location` set, librdkafka skips the default verify paths, so the
/// connection trusts exactly the projected CA — the same set the engine's
/// `ssl_ca_location` builds [VERIFIED vendored librdkafka/src/rdkafka_ssl.c:
/// the `ca_location` branch clears `ca_probe`].
fn pin_tls(base: &mut ClientConfig, tls_ca_file: Option<&str>) {
    base.set("ssl.endpoint.identification.algorithm", "https");
    if let Some(ca) = tls_ca_file {
        base.set("ssl.ca.location", ca);
    }
}

impl RdKafkaReader {
    /// The exact `ClientConfig` [`RdKafkaReader::connect`] dials with, before
    /// the consumer-only keys are added.
    ///
    /// **EXTRACTED SO THE SECURITY-CRITICAL SETTINGS ARE ASSERTABLE WITHOUT A
    /// BROKER.** Two of the keys below are controls, not tuning:
    /// `ssl.endpoint.identification.algorithm` (hostname verification) and
    /// `ssl.ca.location` (which trust anchor). A `ClientConfig` is a map until
    /// `create()` is called, so
    /// `tests::the_tls_client_pins_hostname_verification_and_the_projected_ca`
    /// asserts them with `ClientConfig::get` and dials nothing.
    ///
    /// # Errors
    ///
    /// [`KafkaError::Client`] for a CA supplied without TLS (the silent
    /// downgrade `AuthConfig::with_tls_ca_file` also refuses), and for
    /// [`AuthConfig::Token`], which SP4 introduces.
    pub fn client_config(
        bootstrap: &[String],
        auth: &AuthConfig,
    ) -> Result<ClientConfig, KafkaError> {
        // Shared by both clients `connect` constructs.
        let mut base = ClientConfig::new();
        base.set("bootstrap.servers", bootstrap.join(","))
            .set("client.id", "logweir-drill")
            // FX-43: the client QUEUES its log lines, which [`pumped`] prints
            // escaped; librdkafka's default logger would print them raw.
            .set("log.queue", "true")
            // Explicit rather than relying on the default: librdkafka's own
            // docs note the consumer default (false) already differs from
            // the producer default (true) and from the Java consumer
            // (true), and that split has moved across versions — pin it so
            // a librdkafka upgrade can never silently create a topic on a
            // cluster an operator cares about via a metadata lookup for a
            // name that does not exist
            // [VERIFIED rdkafka-sys librdkafka/CONFIGURATION.md:63].
            .set("allow.auto.create.topics", "false");
        // One SASL arm for the three SASL mechanisms: the keys are
        // the same, only `sasl.mechanism` differs — librdkafka's spellings,
        // two hyphens for SCRAM, where the engine writes one.
        let sasl = |base: &mut ClientConfig,
                    mechanism: &str,
                    username: &str,
                    password: &str,
                    tls: bool,
                    tls_ca_file: Option<&str>,
                    mode: &'static str|
         -> Result<(), KafkaError> {
            base.set(
                "security.protocol",
                if tls { "SASL_SSL" } else { "SASL_PLAINTEXT" },
            )
            .set("sasl.mechanism", mechanism)
            .set("sasl.username", username)
            .set("sasl.password", password);
            if tls {
                pin_tls(base, tls_ca_file);
            } else if tls_ca_file.is_some() {
                return Err(KafkaError::Client(
                    logweir_core::connection::TlsCaWithoutTls { mode }.to_string(),
                ));
            }
            Ok(())
        };
        match auth {
            AuthConfig::Plaintext => {
                base.set("security.protocol", "PLAINTEXT");
            }
            AuthConfig::ScramSha512 {
                username,
                password,
                tls,
                tls_ca_file,
            } => sasl(
                &mut base,
                "SCRAM-SHA-512",
                username,
                password,
                *tls,
                tls_ca_file.as_deref(),
                logweir_core::connection::AUTH_MODE_SCRAM_SHA_512,
            )?,
            AuthConfig::ScramSha256 {
                username,
                password,
                tls,
                tls_ca_file,
            } => sasl(
                &mut base,
                "SCRAM-SHA-256",
                username,
                password,
                *tls,
                tls_ca_file.as_deref(),
                logweir_core::connection::AUTH_MODE_SCRAM_SHA_256,
            )?,
            // ALWAYS TLS: `AuthConfig::Plain` has no `tls` field, because
            // `from_spec` refuses a `plain` spec without TLS (PlainWithoutTls).
            AuthConfig::Plain {
                username,
                password,
                tls_ca_file,
            } => sasl(
                &mut base,
                "PLAIN",
                username,
                password,
                true,
                tls_ca_file.as_deref(),
                logweir_core::connection::AUTH_MODE_PLAIN,
            )?,
            // `SSL` with the client certificate and key as FILE PATHS —
            // librdkafka's `ssl.certificate.location` / `ssl.key.location`,
            // the same two files the engine's `ssl_certificate_location` /
            // `ssl_key_location` load. No `ssl.key.password`: the projected
            // key is unencrypted PEM, the one form the engine's loader reads.
            AuthConfig::Mtls {
                tls_ca_file,
                client_certificate,
            } => {
                let Some(files) = client_certificate else {
                    return Err(KafkaError::Client(
                        logweir_core::connection::ClientCertificateRefusal::Missing.to_string(),
                    ));
                };
                base.set("security.protocol", "SSL")
                    .set("ssl.certificate.location", files.cert_file.as_str())
                    .set("ssl.key.location", files.key_file.as_str());
                pin_tls(&mut base, tls_ca_file.as_deref());
            }
            AuthConfig::Token(_) => {
                return Err(KafkaError::Client(
                    "token auth (OAUTHBEARER / MSK IAM) is deferred (OD-3)".into(),
                ));
            }
        }
        Ok(base)
    }

    pub fn connect(bootstrap: &[String], auth: AuthConfig) -> Result<Self, KafkaError> {
        let base = Self::client_config(bootstrap, &auth)?;
        // These four properties are meaningful only to a CONSUMER. Set on a
        // clone, so the admin client's `create()` never sees them and
        // librdkafka logs no `CONFWARN` for each one on every connection.
        let mut consumer_cfg = base.clone();
        consumer_cfg
            .set("group.id", "logweir-canary-do-not-commit")
            .set("enable.auto.commit", "false") // never commit on any cluster
            .set("enable.partition.eof", "true")
            // A `from` outside the log's retained range must be a loud,
            // distinguishable error, not a silent reseek to a DIFFERENT
            // offset whose records get fingerprinted as if they were the
            // ones asked for [VERIFIED rdkafka-sys 4.10.0+2.12.1's vendored
            // librdkafka/CONFIGURATION.md:203 — default is `largest`
            // ("latest"); 'error' is the documented alternative that
            // "trigger[s] an error (ERR__AUTO_OFFSET_RESET) which is
            // retrieved by consuming messages"]. See `consume_range`'s
            // handling of `KafkaError::MessageConsumption(AutoOffsetReset)`.
            .set("auto.offset.reset", "error");
        let consumer: BaseConsumer = consumer_cfg
            .create()
            .map_err(|e| KafkaError::Client(e.to_string()))?;
        let consumer = pumped(consumer).map_err(KafkaError::Client)?;
        let admin: AdminClient<DefaultClientContext> = base
            .create()
            .map_err(|e| KafkaError::Client(e.to_string()))?;
        let admin = pumped(admin).map_err(KafkaError::Client)?;
        Ok(Self {
            consumer,
            admin,
            base,
            position_bound: DEFAULT_POSITION_BOUND,
            admin_bound: crate::groups::DEFAULT_ADMIN_BOUND,
            scratch_prefix: None,
            protected_names: std::collections::BTreeSet::new(),
            deletable_names: None,
        })
    }

    /// Names this reader's `delete_topics` never deletes,
    /// whatever the scratch prefix: a drill passes its source topics, so no
    /// teardown, probe clean-up or later caller can delete a topic under its
    /// ORIGINAL name — the one name a restore under the original names writes.
    #[must_use]
    pub fn with_protected_names(mut self, names: impl IntoIterator<Item = String>) -> Self {
        self.protected_names.extend(names);
        self
    }

    /// **PROD-07.2: scope `delete_topics` to exactly `names`**, a restore
    /// plan's mapped target names, in place of the scratch prefix: what
    /// `logweir restore cleanup` deletes from, so an interrupted restore under
    /// any prefix can be cleaned (protected names still win). The cleanup
    /// still deletes only topics its signed record names, while each one's ID
    /// is the recorded one.
    #[must_use]
    pub fn with_deletable_names(mut self, names: impl IntoIterator<Item = String>) -> Self {
        self.deletable_names = Some(names.into_iter().collect());
        self
    }

    /// The admin client, for `crate::rdkafka_admin`'s calls through
    /// `logweir-rdkafka-ffi`, which borrow its `Client` for each call.
    pub(crate) fn admin_client(&self) -> &AdminClient<DefaultClientContext> {
        &self.admin
    }

    /// The connection the reader dialled with, for `crate::rdkafka_capture`'s
    /// READ_UNCOMMITTED marks handle. Holds the password; never
    /// printed.
    pub(crate) fn base_config(&self) -> &ClientConfig {
        &self.base
    }

    /// The reader's consumer, for `crate::rdkafka_capture`'s metadata reads.
    pub(crate) fn metadata_consumer(&self) -> &BaseConsumer {
        &self.consumer
    }

    /// The bound of one admin call.
    pub(crate) fn admin_bound(&self) -> Duration {
        self.admin_bound
    }

    /// Sets it; `crate::rdkafka_admin`'s `with_admin_bound` validates first.
    pub(crate) fn set_admin_bound(&mut self, bound: Duration) {
        self.admin_bound = bound;
    }

    /// Sets the bound of every later [`RdKafkaReader::committed_positions`]
    /// fetch and [`RdKafkaReader::commit_positions`] wait (default
    /// [`DEFAULT_POSITION_BOUND`], 15 s).
    ///
    /// # Errors
    ///
    /// [`KafkaError::Client`] outside 2 s..=300 s
    /// ([`crate::positions::valid_bound`]).
    pub fn with_position_bound(mut self, bound: Duration) -> Result<Self, KafkaError> {
        self.position_bound = valid_bound(bound).map_err(KafkaError::Client)?;
        Ok(self)
    }

    /// The committed positions of `group` on exactly
    /// `partitions`, read with ONE RequireStable OffsetFetch from a fresh
    /// handle that carries the group's id and can never subscribe, join or
    /// commit implicitly (`rdkafka_positions`), dropped before this returns.
    ///
    /// - One entry per requested partition, in request order. A partition
    ///   with no committed offset is [`PartitionPosition::NoCommittedPosition`],
    ///   never offset 0; so is every partition of an ABSENT group and of a
    ///   SHARE group, so classify the group before reading meaning into
    ///   it.
    /// - A pending transactional offset commit makes the whole bounded fetch
    ///   time out: [`PositionsError::PositionsUnstable`] for a group the
    ///   caller's listing shows, [`PositionsError::NotVisibleOrUnreachable`]
    ///   for one it does not ([`GroupListing`]). The pre-transaction position
    ///   is never returned.
    /// - A refusal of this principal on the group is
    ///   [`PositionsError::NotAuthorized`] (listed) or
    ///   [`PositionsError::NotVisibleToPrincipal`] (not listed), when the
    ///   broker said so, either as the answer or as the coordinator lookup's
    ///   refusal on the handle's queue.
    /// - Leader epochs are not exposed by this route: every
    ///   [`CommittedPosition::leader_epoch`] is `None`.
    ///
    /// Takes at most about the bound, plus the handle's own teardown.
    ///
    /// [`PartitionPosition::NoCommittedPosition`]: crate::positions::PartitionPosition::NoCommittedPosition
    pub fn committed_positions(
        &self,
        group: &str,
        partitions: &[TopicPartition],
        listing: GroupListing,
    ) -> Result<GroupPositions, PositionsError> {
        fetch_request(partitions)?;
        GroupHandle::open(&self.base, group, self.position_bound)
            .map_err(PositionsError::Client)?
            .committed_positions(partitions, listing)
    }

    /// Commits `positions` for `group` in ONE synchronous
    /// OffsetCommit from a non-member (generation −1, empty member id), from a
    /// fresh handle that can never subscribe or join, dropped before this
    /// returns.
    ///
    /// - The broker refuses the WHOLE request while the group has members:
    ///   [`CommitError::GroupActive`], nothing changed. A share group
    ///   is [`CommitError::NotAConsumerGroup`]; a principal without Read on
    ///   the group [`CommitError::NotAuthorized`]; no coordinator within the
    ///   bound [`CommitError::NotVisibleOrUnreachable`], nothing sent.
    /// - An ABSENT group id is created by the commit as a simple classic
    ///   group: classify the target first.
    /// - Only each position's offset is committed, with leader epoch −1 and
    ///   [`crate::positions::COMMIT_METADATA_MARKER`] as its metadata.
    /// - Any other failure is [`CommitError::Failed`] with its integer code;
    ///   [`CommitError::may_have_applied`] says whether to read back first.
    pub fn commit_positions(
        &self,
        group: &str,
        positions: &[(TopicPartition, CommittedPosition)],
    ) -> Result<(), CommitError> {
        commit_request(positions)?;
        GroupHandle::open(&self.base, group, self.position_bound)
            .map_err(CommitError::Client)?
            .commit_positions(positions)
    }

    /// Scopes this reader's `delete_topics` to names beginning with
    /// `prefix`. Read paths (`ClusterReader`) are never restricted by this —
    /// only deletion, the sole destructive path in the whole product. A
    /// reader that never calls this can still read everything; it can
    /// delete nothing.
    ///
    /// `prefix` need not end in a separator: `with_scratch_prefix("logweir")`
    /// permits `logweir-prod-orders` exactly as readily as any of a drill's
    /// own scratch topics. This builder cannot know the operator's naming
    /// scheme, so pass the FULL rendered prefix including its trailing
    /// delimiter (e.g. `"drill-20260903-"`, not `"drill"`).
    ///
    /// Rejects an empty, whitespace-only, or shorter-than-3-character
    /// prefix. `""` would make `str::starts_with` unconditionally true —
    /// every name, including a production topic, would pass the namespace
    /// check while the reader still reports as properly scoped, which
    /// defeats this guard's entire purpose. There is no Kafka-side floor for
    /// what counts as "long enough"; three characters is this crate's own,
    /// deliberately conservative minimum, chosen only to refuse the
    /// degenerate cases (`""`, `" "`, a stray single character) rather than
    /// to certify any particular prefix as well-chosen — the caller's own
    /// per-run rendered value is what actually authors the truth.
    pub fn with_scratch_prefix(mut self, prefix: impl Into<String>) -> Result<Self, KafkaError> {
        let prefix = prefix.into();
        if prefix.trim().chars().count() < 3 {
            return Err(KafkaError::Client(format!(
                "with_scratch_prefix: {prefix:?} is too short to serve as a scratch-topic namespace (must be at least 3 non-whitespace characters) — an empty or near-empty prefix would make str::starts_with match everything, deleting anything a caller asks for"
            )));
        }
        self.scratch_prefix = Some(prefix);
        Ok(self)
    }

    /// Maps a per-topic Kafka error code to the distinction a caller needs —
    /// topic-not-found vs. not-authorised vs. anything else — instead of one
    /// generic `Client` bucket regardless of cause. Verified against the vendored rdkafka-sys 4.10.0+2.12.1 error
    /// code table (`src/types.rs`): `UnknownTopicOrPartition = 3`,
    /// `TopicAuthorizationFailed = 29`.
    fn classify_topic_error(topic: &str, code: rdkafka::error::RDKafkaErrorCode) -> KafkaError {
        use rdkafka::error::RDKafkaErrorCode as Code;
        match code {
            Code::UnknownTopicOrPartition => KafkaError::TopicNotFound(topic.to_string()),
            Code::TopicAuthorizationFailed => KafkaError::NotAuthorized(topic.to_string()),
            other => KafkaError::Client(format!("{topic}: {other}")),
        }
    }

    /// What this principal's METADATA says about one topic — the evidence
    /// [`empty_topic_config_answer`] names an empty DescribeConfigs answer
    /// by. Read only for a topic whose answer came back empty.
    fn topic_visibility(&self, topic: &str) -> TopicVisibility {
        use rdkafka::error::RDKafkaErrorCode as Code;
        let md = match self.consumer.fetch_metadata(Some(topic), T) {
            Ok(md) => md,
            Err(e) => return TopicVisibility::Unread(e.to_string()),
        };
        let Some(t) = md.topics().first() else {
            return TopicVisibility::Unread("metadata named no topic".to_string());
        };
        match t.error().map(Code::from) {
            None => TopicVisibility::Visible,
            Some(Code::TopicAuthorizationFailed) => TopicVisibility::NotAuthorized,
            Some(Code::UnknownTopicOrPartition) => TopicVisibility::NotFound,
            Some(other) => TopicVisibility::Unread(other.to_string()),
        }
    }

    /// One BROKER resource's answer under the empty-answer rule, flattened: an
    /// empty entry list is the cluster authorizer's refusal, never an empty
    /// configuration.
    fn broker_answer(
        broker_id: i32,
        cfg: rdkafka::admin::ConfigResource,
    ) -> Result<BTreeMap<String, String>, KafkaError> {
        if cfg.entries.is_empty() {
            return Err(empty_broker_config_answer(broker_id));
        }
        Ok(cfg
            .entries
            .into_iter()
            .filter_map(|e| e.value.map(|v| (e.name, v)))
            .collect())
    }

    /// rdkafka's per-entry source, in this crate's vocabulary.
    fn source_kind(source: &rdkafka::admin::ConfigSource) -> ConfigSourceKind {
        use rdkafka::admin::ConfigSource as S;
        match source {
            S::DynamicTopic => ConfigSourceKind::DynamicTopicConfig,
            S::DynamicBroker => ConfigSourceKind::DynamicBrokerConfig,
            S::DynamicDefaultBroker => ConfigSourceKind::DynamicDefaultBrokerConfig,
            S::StaticBroker => ConfigSourceKind::StaticBrokerConfig,
            S::Default => ConfigSourceKind::DefaultConfig,
            S::Unknown => ConfigSourceKind::Unknown,
        }
    }

    /// One topic resource's answer under the empty-answer rule: an empty entry
    /// list is a FAILED read, named from the topic's metadata; never an empty
    /// set of overrides. Shared by [`ClusterReader::topic_configs`] and
    /// [`ClusterReader::describe_topic_configs`], so the two cannot disagree.
    fn topic_answer(
        &self,
        topic: &str,
        result: rdkafka::admin::ConfigResourceResult,
    ) -> TopicConfigRead {
        Self::topic_answer_with(topic, result, || self.topic_visibility(topic))
    }

    /// [`Self::topic_answer`] with the metadata read INJECTED, so the
    /// empty-answer decision is testable over a constructed rdkafka result with
    /// no broker
    /// (`tests::an_empty_topic_answer_is_a_refusal_never_an_empty_override_set`).
    /// `visibility` is called only for an empty answer.
    fn topic_answer_with(
        topic: &str,
        result: rdkafka::admin::ConfigResourceResult,
        visibility: impl FnOnce() -> TopicVisibility,
    ) -> TopicConfigRead {
        // The `Err` arm is what the TYPE promises and rdkafka 0.36.2 never
        // produces: kept, because a later rdkafka that reads the
        // per-resource error would hand it here, and it classifies exactly.
        let cfg = result.map_err(|code| Self::classify_topic_error(topic, code))?;
        if cfg.entries.is_empty() {
            return Err(empty_topic_config_answer(topic, &visibility()));
        }
        Ok(cfg
            .entries
            .into_iter()
            .map(|e| ConfigEntryObservation {
                source: Self::source_kind(&e.source),
                name: e.name,
                value: e.value,
                read_only: e.is_read_only,
                sensitive: e.is_sensitive,
            })
            .collect())
    }
}

/// The broker and client codes a topic answers with while its creation is still
/// propagating, and nothing else: `UnknownTopicOrPartition` (the answering
/// broker's metadata does not hold it yet) and its two client-side spellings,
/// `LeaderNotAvailable` (no leader elected yet) and `NotLeaderForPartition`
/// (the listed leader has not yet become one). An authorization failure, a
/// timeout or a transport error is not on the list: waiting cannot change it.
fn creation_in_progress(code: rdkafka::error::RDKafkaErrorCode) -> bool {
    use rdkafka::error::RDKafkaErrorCode as Code;
    matches!(
        code,
        Code::UnknownTopicOrPartition
            | Code::UnknownTopic
            | Code::UnknownPartition
            | Code::LeaderNotAvailable
            | Code::NotLeaderForPartition
    )
}

/// How [`RdKafkaReader::created_topic_configs`] reads one answer of
/// the configuration read it repeats: the configuration is done;
/// `TopicNotFound` and `NotAuthorized` are a creation still propagating
/// (`NotAuthorized` here is an inference from an EMPTY DescribeConfigs
/// answer, never a code the broker sent); anything else (unreachable, a
/// timeout, a client error) is final. A pure function so a unit row pins the
/// exact set: dropping `NotAuthorized` brings back the empty-answer
/// failure, and widening it makes a transport failure or a refused read
/// cost `within` before the same exit.
fn created_topic_config_settling(
    answer: Result<BTreeMap<String, String>, KafkaError>,
) -> Settling<BTreeMap<String, String>> {
    match answer {
        Ok(configs) => Settling::Done(configs),
        Err(e @ (KafkaError::TopicNotFound(_) | KafkaError::NotAuthorized(_))) => {
            Settling::NotYet(e)
        }
        Err(e) => Settling::Failed(e),
    }
}

/// [`Settling::NotYet`] for a code [`creation_in_progress`] names,
/// [`Settling::Failed`] for any other, with the same error either way.
fn settling_on<T>(code: rdkafka::error::RDKafkaErrorCode, error: KafkaError) -> Settling<T> {
    if creation_in_progress(code) {
        Settling::NotYet(error)
    } else {
        Settling::Failed(error)
    }
}

impl ClusterReader for RdKafkaReader {
    /// `crate::rdkafka_capture`.
    fn observe_consumer_groups(
        &self,
        selected: &[String],
        topics: &[String],
    ) -> crate::capture::GroupsObservation {
        self.capture_consumer_groups(selected, topics)
    }

    /// `crate::rdkafka_capture`.
    fn partition_marks(&self, topics: &[String]) -> BTreeMap<String, crate::capture::TopicMarks> {
        self.read_partition_marks(topics)
    }

    fn cluster_id(&self) -> Result<String, KafkaError> {
        self.consumer.client().fetch_cluster_id(T).ok_or_else(|| {
            // librdkafka's own doc for `rd_kafka_clusterid`: "returns a newly
            // allocated string containing the ClusterId, or NULL if no
            // ClusterId could be retrieved in the ALLOTTED TIMESPAN"
            // [VERIFIED rdkafka-sys 4.10.0+2.12.1's vendored
            // librdkafka/src/rdkafka.h, doc comment directly above
            // `rd_kafka_clusterid`]. NULL is what this call returns when
            // nothing answered within `T`, not a generic client failure.
            KafkaError::Unreachable(format!("no broker returned a ClusterId within {T:?}"))
        })
    }

    fn list_topics(&self) -> Result<Vec<TopicMeta>, KafkaError> {
        let md = self
            .consumer
            .fetch_metadata(None, T)
            .map_err(|e| KafkaError::Unreachable(e.to_string()))?;
        // The contract, made to agree with `end_offsets` below rather than
        // silently disagree with it: a per-topic metadata error is never
        // dropped. `end_offsets` targets ONE named topic and its return
        // shape (`Vec<(i32, i64)>`) has no room for a status, so it
        // surfaces the error as `Err`. `list_topics` targets ALL topics at
        // once and dropping an errored entry here would be worse than
        // `end_offsets` failing: `LeaderNotAvailable`/`ReplicaNotAvailable`
        // are ordinary TRANSIENT states during topic creation or leader
        // election, so silently excluding such a topic would make a later
        // completeness check ("this scratch cluster holds nothing but my
        // drill topics") wrongly conclude the cluster is emptier than it
        // is. So every topic is always present in this list; a caller that
        // specifically wants "confirmed healthy and present" — the phase-0
        // marker-topic guard, for one — checks `TopicMeta::error.is_none()`
        // itself rather than relying on the entry's absence to mean that.
        Ok(md
            .topics()
            .iter()
            .map(|t| match t.error() {
                None => TopicMeta::new(t.name(), t.partitions().len() as i32),
                Some(err) => TopicMeta::errored(
                    t.name(),
                    Self::classify_topic_error(t.name(), err.into()).to_string(),
                ),
            })
            .collect())
    }

    fn topic_ids(
        &self,
        topics: &[String],
    ) -> Result<Vec<(String, crate::topic_ids::TopicIdRead)>, KafkaError> {
        self.read_topic_ids(topics)
    }

    fn topic_read_access(
        &self,
        topics: &[String],
    ) -> Result<Vec<(String, crate::access::TopicRead)>, KafkaError> {
        self.read_topic_access(topics)
    }

    /// The check's own observation ([`crate::inventory::observe_broker_view`]),
    /// over the connection this reader dialled: the same transport, trust
    /// anchor and credential, on a handle of its own that is closed when it
    /// returns.
    fn broker_view(
        &self,
        within: Duration,
    ) -> Result<crate::api_versions::ApiVersions, crate::api_versions::NotObserved> {
        crate::inventory::observe_broker_view(self.base_config(), within)
    }

    fn replication_factors(&self, topics: &[String]) -> Result<BTreeMap<String, u32>, KafkaError> {
        if topics.is_empty() {
            return Ok(BTreeMap::new());
        }
        let md = self
            .consumer
            .fetch_metadata(None, T)
            .map_err(|e| KafkaError::Unreachable(e.to_string()))?;
        Ok(md
            .topics()
            .iter()
            .filter(|t| t.error().is_none() && topics.iter().any(|n| n == t.name()))
            .filter_map(|t| {
                let factor =
                    replication_factor_of(t.partitions().iter().map(|p| p.replicas().len()))?;
                Some((t.name().to_string(), factor))
            })
            .collect())
    }

    fn end_offsets(&self, topic: &str) -> Result<Vec<(i32, i64)>, KafkaError> {
        let md = self
            .consumer
            .fetch_metadata(Some(topic), T)
            .map_err(|e| KafkaError::Unreachable(e.to_string()))?;
        let t = md
            .topics()
            .first()
            .ok_or_else(|| KafkaError::TopicNotFound(topic.to_string()))?;
        // A topic that is absent, or that the principal may not describe,
        // returns a metadata entry with zero partitions rather than an
        // outright request failure — so without this check, `Ok(vec![])`
        // reads identically to "a healthy, empty topic" (impossible in
        // Kafka; every existing topic has at least one partition) when the
        // truth is "not found" or "not authorized".
        if let Some(err) = t.error() {
            return Err(Self::classify_topic_error(topic, err.into()));
        }
        let mut out = Vec::new();
        for p in t.partitions() {
            let (_lo, hi) = self
                .consumer
                .fetch_watermarks(topic, p.id(), T)
                .map_err(|e| KafkaError::Client(e.to_string()))?;
            out.push((p.id(), hi));
        }
        out.sort();
        Ok(out)
    }

    /// Metadata first (the topic, every partition, every leader), then
    /// one ListOffsets per partition through the leader, which can fail while
    /// the topic propagates. One attempt is two kinds of request;
    /// [`settle`] repeats it while the answers are those of a creation in
    /// progress.
    fn await_served(
        &self,
        topic: &str,
        partitions: i32,
        within: Duration,
    ) -> Result<(), KafkaError> {
        use rdkafka::error::{KafkaError as RdKafkaError, RDKafkaErrorCode as Code};
        settle(within, || {
            let md = match self.consumer.fetch_metadata(Some(topic), T) {
                Ok(md) => md,
                Err(e) => return Settling::Failed(KafkaError::Unreachable(e.to_string())),
            };
            let Some(t) = md.topics().first() else {
                return Settling::NotYet(KafkaError::TopicNotFound(topic.to_string()));
            };
            if let Some(err) = t.error() {
                let code = Code::from(err);
                return settling_on(code, Self::classify_topic_error(topic, code));
            }
            let listed = t.partitions().len();
            if i32::try_from(listed).ok() != Some(partitions) {
                return Settling::NotYet(KafkaError::Client(format!(
                    "{topic}: metadata lists {listed} partition(s); it was created with \
                     {partitions}"
                )));
            }
            for p in t.partitions() {
                if let Some(err) = p.error() {
                    let code = Code::from(err);
                    return settling_on(
                        code,
                        KafkaError::Client(format!("{topic}/{}: metadata: {code}", p.id())),
                    );
                }
                if p.leader() < 0 {
                    return Settling::NotYet(KafkaError::Client(format!(
                        "{topic}/{}: no leader yet",
                        p.id()
                    )));
                }
            }
            for p in t.partitions() {
                match self.consumer.fetch_watermarks(topic, p.id(), T) {
                    Ok(_) => {}
                    Err(RdKafkaError::MetadataFetch(code)) => {
                        return settling_on(
                            code,
                            KafkaError::Client(format!("{topic}/{}: ListOffsets: {code}", p.id())),
                        )
                    }
                    Err(e) => {
                        return Settling::Failed(KafkaError::Client(format!(
                            "{topic}/{}: ListOffsets: {e}",
                            p.id()
                        )))
                    }
                }
            }
            Settling::Done(())
        })
    }

    /// As the default, and ALSO waits out [`KafkaError::NotAuthorized`],
    /// because here that error is an inference from an EMPTY DescribeConfigs
    /// answer ([`empty_topic_config_answer`]), never a code the broker sent
    /// (rdkafka 0.36.2 does not read the per-resource error). Kafka answers a
    /// topic its metadata does not hold yet with exactly that empty list, and
    /// the metadata read that names it a moment later finds the topic
    /// "visible". A principal that really lacks DescribeConfigs therefore costs
    /// `within` before the same refusal is returned.
    fn created_topic_configs(
        &self,
        topic: &str,
        partitions: i32,
        within: Duration,
    ) -> Result<BTreeMap<String, String>, KafkaError> {
        let started = std::time::Instant::now();
        self.await_served(topic, partitions, within)?;
        settle(within.saturating_sub(started.elapsed()), || {
            created_topic_config_settling(self.topic_configs(topic))
        })
    }

    fn topic_configs(&self, topic: &str) -> Result<BTreeMap<String, String>, KafkaError> {
        use rdkafka::admin::{AdminOptions, ResourceSpecifier};
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| KafkaError::Client(e.to_string()))?;
        let res = rt
            .block_on(self.admin.describe_configs(
                &[ResourceSpecifier::Topic(topic)],
                &AdminOptions::new().request_timeout(Some(T)),
            ))
            .map_err(|e| KafkaError::Unreachable(e.to_string()))?;
        let mut out = BTreeMap::new();
        for r in res {
            // `describe_configs` yields one `Result<_, RDKafkaErrorCode>` per
            // resource [VERIFIED rdkafka-0.36.2/src/admin.rs:948, the alias
            // whose Ok side is the resource-plus-entries struct at :1019]: a
            // BARE code, not `TopicResult`'s `(String, RDKafkaErrorCode)`
            // tuple. The alias is named only by file:line: `tests/reader.rs`'s
            // `the_broker_resource_is_spelled_the_rdkafka_way` keeps its name
            // out of this file.
            //
            // **AND THAT `Err` NEVER ARRIVES.** rdkafka 0.36.2's future pushes
            // `Ok` for every resource and never reads the per-resource error
            // (`src/admin.rs:1121-1159`), so a DENIED topic comes back as an
            // empty entry list; `topic_answer` turns it into the refusal it
            // stands for, never `Ok({})`.
            for e in self.topic_answer(topic, r)? {
                if let Some(v) = e.value {
                    out.insert(e.name, v);
                }
            }
        }
        Ok(out)
    }

    /// One DescribeConfigs request for every named topic.
    ///
    /// librdkafka returns the resources IN REQUEST ORDER — "As a convenience to
    /// the application we insert result in the same order as they were
    /// requested. The broker does not maintain ordering"
    /// (`rdkafka-sys-4.10.0+2.12.1/librdkafka/src/rdkafka_admin.c:3832-3856`) —
    /// which is what makes the zip below sound: rdkafka 0.36.2's per-resource
    /// `Err` would carry no name. A count that disagrees is refused rather
    /// than zipped short.
    fn describe_topic_configs(
        &self,
        topics: &[String],
    ) -> Result<Vec<(String, TopicConfigRead)>, KafkaError> {
        use rdkafka::admin::{AdminOptions, ResourceSpecifier};
        if topics.is_empty() {
            return Ok(Vec::new());
        }
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| KafkaError::Client(e.to_string()))?;
        let specs: Vec<ResourceSpecifier<'_>> = topics
            .iter()
            .map(|t| ResourceSpecifier::Topic(t.as_str()))
            .collect();
        let res = rt
            .block_on(
                self.admin
                    .describe_configs(&specs, &AdminOptions::new().request_timeout(Some(T))),
            )
            .map_err(|e| KafkaError::Unreachable(e.to_string()))?;
        if res.len() != topics.len() {
            return Err(KafkaError::Client(format!(
                "DescribeConfigs for {} topic(s) returned {} result(s); a short or long answer \
                 cannot be matched to the topics it describes",
                topics.len(),
                res.len()
            )));
        }
        Ok(topics
            .iter()
            .zip(res)
            .map(|(topic, r)| (topic.clone(), self.topic_answer(topic, r)))
            .collect())
    }

    /// **Guard G-TS.** DescribeConfigs on `ResourceSpecifier::Broker(<the
    /// broker id from metadata>)`, returned flat.
    ///
    /// The DescribeConfigs INPUT is `ResourceSpecifier::Broker(i32)`
    /// (`rdkafka-0.36.2/src/admin.rs:962-968`) and the result carries
    /// `OwnedResourceSpecifier::Broker(i32)` (`:973-979`). The Java client's
    /// name for the input role is not one rdkafka 0.36 offers, and writing it
    /// is how this read gets aimed at the wrong type; `tests/reader.rs`'s
    /// `the_broker_resource_is_spelled_the_rdkafka_way` keeps it out of both
    /// source files.
    ///
    /// The broker id comes from METADATA rather than from a config or a
    /// literal: a `Broker(0)` sent to a cluster whose only node is `1001` —
    /// which is exactly what the compose stack runs — describes nothing, and
    /// the preflight built on top of this would then read an empty map and
    /// conclude the broker is harmless.
    fn broker_configs(&self) -> Result<BTreeMap<String, String>, KafkaError> {
        use rdkafka::admin::{AdminOptions, ResourceSpecifier};
        let md = self
            .consumer
            .fetch_metadata(None, T)
            .map_err(|e| KafkaError::Unreachable(e.to_string()))?;
        let broker_id = md.brokers().first().map(|b| b.id()).ok_or_else(|| {
            KafkaError::Unreachable(
                "cluster metadata listed no broker, so there is no broker id to describe configs \
                 for"
                .to_string(),
            )
        })?;
        // Same per-call current-thread runtime as `topic_configs` and
        // `delete_topics`, for the reason ADR 0004 records: rdkafka's admin
        // futures need a driven tokio runtime.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| KafkaError::Client(e.to_string()))?;
        let res = rt
            .block_on(self.admin.describe_configs(
                &[ResourceSpecifier::Broker(broker_id)],
                &AdminOptions::new().request_timeout(Some(T)),
            ))
            .map_err(|e| KafkaError::Unreachable(e.to_string()))?;
        let mut out = BTreeMap::new();
        for r in res {
            // One `Result<_, RDKafkaErrorCode>` per resource — a BARE code,
            // as in `topic_configs`, not `TopicResult`'s
            // `(String, RDKafkaErrorCode)` tuple. There is no topic name to
            // classify against here, so a failure is reported as-is rather
            // than through `classify_topic_error`.
            let cfg = r.map_err(|e| {
                KafkaError::Client(format!("DescribeConfigs on broker {broker_id}: {e}"))
            })?;
            // An EMPTY answer is the refusal rdkafka 0.36.2 does not report
            // (measured for a principal refused on the cluster). Read as
            // `Ok({})` it would make phase 0's G-TS assume the Apache default
            // `CreateTime` and skip the timestamp bound.
            out.extend(Self::broker_answer(broker_id, cfg)?);
        }
        Ok(out)
    }

    /// [`Self::broker_configs`]'s read, for EVERY broker id the metadata lists,
    /// in ONE DescribeConfigs request; each broker's answer is held to the
    /// empty-answer rule exactly as the one-broker read is (an empty answer is
    /// a refused read, an error), so `None` means only "this broker answered
    /// and did not name the key".
    fn broker_count(&self) -> Result<Option<u32>, KafkaError> {
        let md = self
            .consumer
            .fetch_metadata(None, T)
            .map_err(|e| KafkaError::Unreachable(e.to_string()))?;
        Ok(u32::try_from(md.brokers().len()).ok().filter(|n| *n >= 1))
    }

    fn broker_config_value_all(&self, key: &str) -> Result<Vec<(i32, Option<String>)>, KafkaError> {
        use rdkafka::admin::{AdminOptions, OwnedResourceSpecifier, ResourceSpecifier};
        let md = self
            .consumer
            .fetch_metadata(None, T)
            .map_err(|e| KafkaError::Unreachable(e.to_string()))?;
        let ids: Vec<i32> = md.brokers().iter().map(|b| b.id()).collect();
        if ids.is_empty() {
            return Err(KafkaError::Unreachable(
                "cluster metadata listed no broker, so there is no broker to describe configs for"
                    .to_string(),
            ));
        }
        let resources: Vec<ResourceSpecifier<'_>> = ids
            .iter()
            .map(|id| ResourceSpecifier::Broker(*id))
            .collect();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| KafkaError::Client(e.to_string()))?;
        let res = rt
            .block_on(
                self.admin
                    .describe_configs(&resources, &AdminOptions::new().request_timeout(Some(T))),
            )
            .map_err(|e| KafkaError::Unreachable(e.to_string()))?;
        if res.len() != ids.len() {
            return Err(KafkaError::Client(format!(
                "DescribeConfigs for {} broker(s) returned {} result(s); a short or long answer \
                 cannot be matched to the brokers it describes",
                ids.len(),
                res.len()
            )));
        }
        // Matched by the resource EACH ANSWER NAMES, never by position: a
        // broker id the answers do not name is an error, not an absent value.
        let mut by_broker: BTreeMap<i32, Option<String>> = BTreeMap::new();
        for r in res {
            let cfg =
                r.map_err(|e| KafkaError::Client(format!("DescribeConfigs on a broker: {e}")))?;
            let OwnedResourceSpecifier::Broker(id) = cfg.specifier else {
                return Err(KafkaError::Client(format!(
                    "DescribeConfigs for brokers answered for {:?}",
                    cfg.specifier
                )));
            };
            let answer = Self::broker_answer(id, cfg)?;
            by_broker.insert(id, answer.get(key).cloned());
        }
        ids.iter()
            .map(|id| {
                by_broker
                    .remove(id)
                    .map(|value| (*id, value))
                    .ok_or_else(|| {
                        KafkaError::Client(format!(
                            "DescribeConfigs returned no answer for broker {id}"
                        ))
                    })
            })
            .collect()
    }

    fn consume_range(
        &self,
        topic: &str,
        partition: i32,
        from: i64,
        max: usize,
    ) -> Result<Vec<ConsumedRecord>, KafkaError> {
        // Bound the read by the partition's high watermark rather than by an
        // end-of-partition ERROR: rdkafka 0.36.2 routes
        // `RD_KAFKA_RESP_ERR__PARTITION_EOF` to its own variant,
        // `KafkaError::PartitionEOF(i32)` (`base_consumer.rs`'s
        // `handle_error_event`), whose `Display` is `"Partition EOF: {n}"`, so
        // a short read (`max` bigger than what remains) must not reach the
        // generic error arm, which discards every record collected. Upstream's
        // fetch path keys off the same `high_watermark`. The `PartitionEOF` arm
        // below is a fallback for a watermark that shifts during the loop.
        //
        // ONE `fetch_watermarks(topic, partition, ..)`, not
        // `end_offsets(topic)`, which does a metadata fetch plus one
        // ListOffsets per partition of the whole topic.
        //
        // Its error cannot tell "no such topic" from "no such partition": Kafka
        // returns `UNKNOWN_TOPIC_OR_PARTITION` for both, so it is a `Client`
        // error naming both coordinates, not `TopicNotFound`.
        let (_lo, hi) = self
            .consumer
            .fetch_watermarks(topic, partition, T)
            .map_err(|e| KafkaError::Client(format!("{topic}:{partition}: {e}")))?;
        if from >= hi {
            return Ok(Vec::new());
        }
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(topic, partition, Offset::Offset(from))
            .map_err(|e| KafkaError::Client(e.to_string()))?;
        self.consumer
            .assign(&tpl)
            .map_err(|e| KafkaError::Client(e.to_string()))?;
        let deadline = std::time::Instant::now() + T;
        let mut out = Vec::with_capacity(max.min((hi - from) as usize));
        let mut next = from;
        while out.len() < max && next < hi {
            if std::time::Instant::now() > deadline {
                return Err(KafkaError::Timeout(T));
            }
            match self.consumer.poll(Duration::from_millis(500)) {
                None => continue,
                // Defensive fallback — see the doc comment above. Bounding
                // by `hi` means the loop should already have stopped before
                // this ever fires, but a shifted watermark makes it
                // reachable.
                Some(Err(rdkafka::error::KafkaError::PartitionEOF(_))) => break,
                // `auto.offset.reset=error` (set in `connect`) turns a `from`
                // outside the log's retained range into this error instead of
                // librdkafka silently reseeking and returning the WRONG
                // records. It is `KafkaError::MessageConsumption` carrying this
                // code, not a variant of its own [VERIFIED
                // rdkafka-0.36.2/src/consumer/base_consumer.rs's
                // `handle_error_event`].
                Some(Err(rdkafka::error::KafkaError::MessageConsumption(
                    rdkafka::error::RDKafkaErrorCode::AutoOffsetReset,
                ))) => {
                    return Err(KafkaError::Client(format!(
                        "consume_range: offset {from} is out of range for {topic}:{partition} \
                         (auto.offset.reset=error tripped rather than silently reseeking)"
                    )));
                }
                Some(Err(e)) => return Err(KafkaError::Client(e.to_string())),
                Some(Ok(m)) => {
                    // Belt-and-suspenders for the same silent-reseek
                    // concern: even with auto.offset.reset=error
                    // configured, assert every record actually starts at or
                    // after what was asked for, rather than trusting the
                    // config alone.
                    if m.offset() < from {
                        return Err(KafkaError::Client(format!(
                            "consume_range: record for {topic}:{partition} is at offset {} \
                             but {from} was requested — the broker returned a record before \
                             the requested start",
                            m.offset()
                        )));
                    }
                    next = m.offset() + 1;
                    let headers = m
                        .headers()
                        .map(|hs| {
                            (0..hs.count())
                                .map(|i| {
                                    let h = hs.get(i);
                                    (h.key.to_string(), h.value.map(|v| v.to_vec()))
                                })
                                .collect()
                        })
                        .unwrap_or_default();
                    out.push(ConsumedRecord {
                        partition: m.partition(),
                        offset: m.offset(),
                        timestamp_ms: m.timestamp().to_millis().unwrap_or(-1),
                        key: m.key().map(|k| k.to_vec()),
                        value: m.payload().map(|v| v.to_vec()),
                        headers,
                    });
                }
            }
        }
        self.consumer
            .unassign()
            .map_err(|e| KafkaError::Client(e.to_string()))?;
        Ok(out)
    }
}

/// The `NewTopicSpec` -> `rdkafka::admin::NewTopic` conversion, guard **G-TS**.
///
/// Split out of `TopicCreator::create_topics` so it is assertable with no
/// broker and no admin client: `crates/logweir-kafka/tests/reader.rs`'s
/// `create_topics_sets_the_config_entries_it_was_given` reads `NewTopic`'s own
/// public fields back off the result, so the assertion is over the rdkafka
/// structs that are actually sent and not over a restatement of them.
///
/// **The lifetime is the contract.** `NewTopic<'a>` borrows its name and both
/// halves of every config entry (`NewTopic::set(key: &'a str, value: &'a str)`,
/// `rdkafka-0.36.2/src/admin.rs:658`), and the returned `Vec` is tied to
/// `specs` by the elided `'_`, so the caller's slice must outlive every
/// `NewTopic` built from it. Building one from a `String` created inside this
/// function would not compile.
pub fn new_topics_for(specs: &[NewTopicSpec]) -> Vec<rdkafka::admin::NewTopic<'_>> {
    use rdkafka::admin::{NewTopic, TopicReplication};
    specs
        .iter()
        .map(|t| {
            let mut nt = NewTopic::new(
                t.name.as_str(),
                t.num_partitions,
                TopicReplication::Fixed(t.replication_factor),
            );
            // IN ORDER, one `.set` per entry. `NewTopic::set` pushes onto
            // `config`, so the vector it builds is the order the CreateTopics
            // request carries.
            for (k, v) in &t.configs {
                nt = nt.set(k.as_str(), v.as_str());
            }
            nt
        })
        .collect()
}

/// Whether a per-topic `create_topics` failure is the
/// broker's `TOPIC_ALREADY_EXISTS` — the exclusive create losing to a name
/// that appeared after phase 0 proved it absent.
///
/// The creator reports a failure as the error code's text (`(name, code)`
/// rendered by `RDKafkaErrorCode`'s `Display`), so this compares against that
/// one code's own rendering rather than a hand-written string.
#[must_use]
pub fn is_topic_already_exists(error: &str) -> bool {
    error == rdkafka::types::RDKafkaErrorCode::TopicAlreadyExists.to_string()
}

/// Whether a per-topic `delete_topics` failure is the broker's
/// `TOPIC_AUTHORIZATION_FAILED`: the principal lacks `Delete` on that topic.
/// Compared against the code's own rendering, as [`is_topic_already_exists`].
#[must_use]
pub fn is_topic_authorization_failed(error: &str) -> bool {
    error == rdkafka::types::RDKafkaErrorCode::TopicAuthorizationFailed.to_string()
}

impl TopicCreator for RdKafkaReader {
    /// **Guard G-TS.** One `rdkafka::admin::NewTopic` per `NewTopicSpec`, with
    /// `.set(k, v)` called for each entry of `configs` IN ORDER.
    ///
    /// # Why the `topics` slice must outlive the `NewTopic`s
    ///
    /// `NewTopic<'a>` borrows: `NewTopic::new(name: &'a str, …)` and
    /// `NewTopic::set(key: &'a str, value: &'a str)`
    /// (`rdkafka-0.36.2/src/admin.rs:658`) store `&'a str`, and
    /// `AdminClient::create_topics` takes `I: IntoIterator<Item = &'a NewTopic<'a>>`.
    /// So every string these borrow — the name and both halves of every config
    /// entry — must live at least as long as the `create_topics` call. Here
    /// they live in the caller's `&[NewTopicSpec]`, which by the signature
    /// outlives this whole function body; nothing is built from a temporary.
    /// Formatting a name or a value into a local `String` inside the loop
    /// would not compile, and that is deliberate.
    ///
    /// Unlike `delete_topics` there is no `scratch_prefix` check: creation is
    /// not destruction. The names come from `topic_mapping`, which phase 0's
    /// mapping guard has already refused to let equal any source topic, and
    /// phase 3 reports every target name that already exists.
    ///
    /// **THIS IMPLEMENTATION NEVER DELETES**, not what its own request
    /// created and not after a partial failure: `logweir-kafka`'s one topic
    /// delete is `impl TopicDeleter`'s, behind the scratch prefix and the
    /// protected names (`crates/logweir/tests/no_topic_delete_inventory.rs`
    /// reads this file). Beside the create it validates (`validate_only`,
    /// which writes nothing) and, for PROD-05.2's phase 9, alters the
    /// configuration of a topic the restore created.
    fn create_topics(
        &self,
        topics: &[NewTopicSpec],
    ) -> Result<Vec<(String, Result<(), String>)>, KafkaError> {
        use rdkafka::admin::AdminOptions;
        if topics.is_empty() {
            return Ok(Vec::new());
        }
        let new_topics = new_topics_for(topics);
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| KafkaError::Client(e.to_string()))?;
        // The broker's bound is BELOW the client's, so its answer is per name
        // wherever it can be (`CREATE_OPERATION_TIMEOUT`).
        let options = AdminOptions::new()
            .request_timeout(Some(T))
            .operation_timeout(Some(CREATE_OPERATION_TIMEOUT));
        let res = rt
            .block_on(self.admin.create_topics(&new_topics, &options))
            .map_err(|e| KafkaError::Client(e.to_string()))?;
        // `TopicResult = Result<String, (String, RDKafkaErrorCode)>` — the
        // error here IS the `(name, code)` tuple, as in `delete_topics`.
        Ok(res
            .into_iter()
            .map(|r| match r {
                Ok(name) => (name, Ok(())),
                Err((name, code)) => (name, Err(code.to_string())),
            })
            .collect())
    }

    /// The same request with `validate_only(true)`: the broker judges each
    /// spec and creates nothing.
    fn validate_create_topics(
        &self,
        topics: &[NewTopicSpec],
    ) -> Result<Vec<(String, Result<(), String>)>, KafkaError> {
        use rdkafka::admin::AdminOptions;
        if topics.is_empty() {
            return Ok(Vec::new());
        }
        let new_topics = new_topics_for(topics);
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| KafkaError::Client(e.to_string()))?;
        let options = AdminOptions::new()
            .request_timeout(Some(T))
            .operation_timeout(Some(CREATE_OPERATION_TIMEOUT))
            .validate_only(true);
        let res = rt
            .block_on(self.admin.create_topics(&new_topics, &options))
            .map_err(|e| KafkaError::Client(e.to_string()))?;
        Ok(res
            .into_iter()
            .map(|r| match r {
                Ok(name) => (name, Ok(())),
                Err((name, code)) => (name, Err(code.to_string())),
            })
            .collect())
    }

    /// One legacy `AlterConfigs` for `topic`, its whole set. The answer is
    /// not proof (the trait's doc); a whole-request failure is an error.
    fn alter_topic_configs(
        &self,
        topic: &str,
        configs: &BTreeMap<String, String>,
    ) -> Result<(), KafkaError> {
        use rdkafka::admin::{AdminOptions, AlterConfig, ResourceSpecifier};
        let mut alter = AlterConfig::new(ResourceSpecifier::Topic(topic));
        for (key, value) in configs {
            alter = alter.set(key.as_str(), value.as_str());
        }
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| KafkaError::Client(e.to_string()))?;
        let res = rt
            .block_on(self.admin.alter_configs(
                std::iter::once(&alter),
                &AdminOptions::new().request_timeout(Some(T)),
            ))
            .map_err(|e| KafkaError::Client(e.to_string()))?;
        for r in res {
            if let Err((_, code)) = r {
                return Err(KafkaError::Client(code.to_string()));
            }
        }
        Ok(())
    }
}

impl TopicDeleter for RdKafkaReader {
    /// Exactly the named topics. rdkafka 0.36's `delete_topics` takes
    /// `&[&str]` and returns `Vec<TopicResult>` where
    /// `TopicResult = Result<String, (String, RDKafkaErrorCode)>` — note the
    /// error here IS the `(name, code)` tuple, unlike the DescribeConfigs
    /// element type used by `topic_configs` and `broker_configs`
    /// (rdkafka-0.36.2/src/admin.rs:948), whose error is a bare code.
    ///
    /// This is the only code path in the entire product that destroys data. A
    /// caller names exact, full names (the trait's contract: never a pattern,
    /// never a prefix), and this impl additionally refuses:
    ///
    /// 1. Every name must start with the prefix set via
    ///    `RdKafkaReader::with_scratch_prefix`. Without that guard, this
    ///    method's only protection is "the caller passed the right names" —
    ///    `delete_topics(&["orders".into()])` would delete a production
    ///    topic exactly as readily as a scratch one, and there is no path in
    ///    this crate to undo it.
    /// 2. If `with_scratch_prefix` was never called, EVERY name is refused.
    ///    `connect` alone never grants delete power over anything.
    fn delete_topics(
        &self,
        names: &[String],
    ) -> Result<Vec<(String, Result<(), String>)>, KafkaError> {
        use rdkafka::admin::AdminOptions;
        if names.is_empty() {
            return Ok(Vec::new());
        }
        let in_scope = |n: &str| -> bool {
            match (&self.deletable_names, self.scratch_prefix.as_deref()) {
                (Some(set), _) => set.contains(n),
                (None, Some(prefix)) => n.starts_with(prefix),
                (None, None) => false,
            }
        };
        let prefix = self.scratch_prefix.as_deref().unwrap_or_default();
        if self.deletable_names.is_none() && self.scratch_prefix.is_none() {
            return Ok(names
                .iter()
                .map(|n| {
                    (
                        n.clone(),
                        Err(
                            "delete_topics refused: no scratch-topic namespace configured — \
                             call RdKafkaReader::with_scratch_prefix before deleting anything"
                                .to_string(),
                        ),
                    )
                })
                .collect());
        }
        // Names outside the configured namespace are reported as refused and
        // never reach the broker call; only in-namespace names are sent to
        // `AdminClient::delete_topics`. Refused entries are reported first,
        // then the broker's results in the order rdkafka returned them.
        let mut allowed: Vec<&str> = Vec::new();
        let mut out: Vec<(String, Result<(), String>)> = Vec::new();
        for n in names {
            if self.protected_names.contains(n) {
                out.push((
                    n.clone(),
                    Err(format!(
                        "delete_topics refused: {n:?} is a source topic's own name, and Logweir \
                         never deletes a topic under its original name"
                    )),
                ));
            } else if in_scope(n) {
                allowed.push(n.as_str());
            } else if self.deletable_names.is_some() {
                out.push((
                    n.clone(),
                    Err(format!(
                        "delete_topics refused: {n:?} is not one of the plan's mapped target \
                         names"
                    )),
                ));
            } else {
                out.push((
                    n.clone(),
                    Err(format!(
                        "delete_topics refused: {n:?} is outside the configured scratch-topic \
                         namespace {prefix:?}"
                    )),
                ));
            }
        }
        if allowed.is_empty() {
            return Ok(out);
        }
        // Same per-call current-thread runtime as `topic_configs`, for the
        // reason ADR 0004 records: rdkafka's admin futures need a driven
        // tokio runtime, not NaiveRuntime's thread::sleep timers.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| KafkaError::Client(e.to_string()))?;
        let res = rt
            .block_on(
                self.admin
                    .delete_topics(&allowed, &AdminOptions::new().request_timeout(Some(T))),
            )
            .map_err(|e| KafkaError::Client(e.to_string()))?;
        out.extend(res.into_iter().map(|r| match r {
            Ok(name) => (name, Ok(())),
            Err((name, code)) => (name, Err(code.to_string())),
        }));
        Ok(out)
    }
}

/// A topic's replication factor from its partitions' replica
/// counts: the SMALLEST. A partition mid-reassignment lists the replicas being
/// added beside the ones being removed, so the largest count can exceed the
/// topic's factor; the smallest never does. `None` for a topic with no
/// partitions (metadata the principal could not read) or a count of zero —
/// NOT RECORDED, never `0`.
fn replication_factor_of(replica_counts: impl Iterator<Item = usize>) -> Option<u32> {
    let smallest = replica_counts.min()?;
    u32::try_from(smallest).ok().filter(|n| *n >= 1)
}

#[cfg(test)]
mod tests {
    //! These need the `client` feature but no broker: they pin the rdkafka
    //! facts `consume_range` depends on, so an upgrade that changed one fails
    //! CI instead of making every short read discard the records already
    //! collected. A full short-read scenario needs a live broker
    //! (`BorrowedMessage` has no public constructor).

    /// **librdkafka's settings for the three newer modes**, with no
    /// socket. Each row is the property its mutant breaks: SCRAM-SHA-256's
    /// two-hyphen spelling (the ENGINE writes one), PLAIN only as `SASL_SSL`
    /// with hostname verification pinned, and mTLS as `SSL` with the two FILE
    /// paths and no SASL key — and a client config without the certificate,
    /// which has no identity to present, refused.
    #[test]
    fn the_prod_01_3_modes_configure_librdkafka_as_the_engine_is_configured() {
        use super::RdKafkaReader;
        use crate::reader::AuthConfig;
        use logweir_core::connection::ClientCertificateFiles;
        let bootstrap = vec!["b0:9093".to_string()];

        for (tls, protocol) in [(false, "SASL_PLAINTEXT"), (true, "SASL_SSL")] {
            let cfg = RdKafkaReader::client_config(
                &bootstrap,
                &AuthConfig::ScramSha256 {
                    username: "u".into(),
                    password: "pw".into(),
                    tls,
                    tls_ca_file: None,
                },
            )
            .expect("scram-256 configures");
            assert_eq!(cfg.get("security.protocol"), Some(protocol));
            assert_eq!(cfg.get("sasl.mechanism"), Some("SCRAM-SHA-256"));
            assert_eq!(cfg.get("sasl.username"), Some("u"));
            assert_eq!(
                cfg.get("ssl.endpoint.identification.algorithm"),
                tls.then_some("https")
            );
        }

        let plain = RdKafkaReader::client_config(
            &bootstrap,
            &AuthConfig::Plain {
                username: "u".into(),
                password: "pw".into(),
                tls_ca_file: Some("/ca.crt".into()),
            },
        )
        .expect("plain configures");
        assert_eq!(
            plain.get("security.protocol"),
            Some("SASL_SSL"),
            "never SASL_PLAINTEXT"
        );
        assert_eq!(plain.get("sasl.mechanism"), Some("PLAIN"));
        assert_eq!(
            plain.get("ssl.endpoint.identification.algorithm"),
            Some("https")
        );
        assert_eq!(plain.get("ssl.ca.location"), Some("/ca.crt"));

        let mtls = RdKafkaReader::client_config(
            &bootstrap,
            &AuthConfig::Mtls {
                tls_ca_file: None,
                client_certificate: Some(ClientCertificateFiles {
                    cert_file: "/c/tls.crt".into(),
                    key_file: "/c/tls.key".into(),
                }),
            },
        )
        .expect("mtls configures");
        assert_eq!(mtls.get("security.protocol"), Some("SSL"));
        assert_eq!(mtls.get("ssl.certificate.location"), Some("/c/tls.crt"));
        assert_eq!(mtls.get("ssl.key.location"), Some("/c/tls.key"));
        assert_eq!(
            mtls.get("ssl.endpoint.identification.algorithm"),
            Some("https")
        );
        assert_eq!(mtls.get("sasl.mechanism"), None);
        assert!(RdKafkaReader::client_config(
            &bootstrap,
            &AuthConfig::Mtls {
                tls_ca_file: None,
                client_certificate: None,
            },
        )
        .is_err());
    }

    /// **The transport refusals at the ONE construction site, and
    /// no password in `Debug` for either new SASL arm.**
    #[test]
    fn from_spec_refuses_plain_and_mtls_without_tls_and_debug_redacts() {
        use crate::reader::AuthConfig;
        use logweir_core::spec::AuthSpec;
        let err = AuthConfig::from_spec(
            &AuthSpec::Plain {
                username: "u".into(),
                tls: false,
            },
            Some("pw-SEEDED-7".into()),
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("PlainWithoutTls"), "{err}");
        assert!(!err.contains("pw-SEEDED-7"), "{err}");
        assert!(AuthConfig::from_spec(&AuthSpec::Mtls { tls: false }, None).is_err());
        assert!(AuthConfig::from_spec(&AuthSpec::Mtls { tls: true }, None).is_ok());
        for spec in [
            AuthSpec::Plain {
                username: "u".into(),
                tls: true,
            },
            AuthSpec::ScramSha256 {
                username: "u".into(),
                tls: false,
            },
        ] {
            let auth = AuthConfig::from_spec(&spec, Some("pw-SEEDED-7".into())).expect("builds");
            assert!(!format!("{auth:?}").contains("pw-SEEDED-7"), "{auth:?}");
            // A missing password is operational and names the mode.
            let missing = AuthConfig::from_spec(&spec, None).unwrap_err().to_string();
            assert!(missing.contains(spec.mode_str()), "{missing}");
        }
    }

    /// The SMALLEST replica count is the factor: a partition mid-reassignment
    /// (four replicas listed while the topic's factor is three) must not raise
    /// it, and an RF-1 partition beside RF-3 ones is what a restore can count
    /// on. No partitions, or a zero, is NOT RECORDED.
    #[test]
    fn the_replication_factor_is_the_smallest_replica_count() {
        use super::replication_factor_of;
        assert_eq!(replication_factor_of([3, 4, 3].into_iter()), Some(3));
        assert_eq!(replication_factor_of([3, 1, 3].into_iter()), Some(1));
        assert_eq!(replication_factor_of([2].into_iter()), Some(2));
        assert_eq!(replication_factor_of(std::iter::empty()), None);
        assert_eq!(replication_factor_of([0, 3].into_iter()), None);
    }

    /// **THE TLS CLIENT PINS HOSTNAME VERIFICATION AND TRUSTS THE PROJECTED
    /// CA.** Both are CONTROLS, and a librdkafka upgrade, a merge resolution or
    /// a refactor could turn either off with no other test failing.
    ///
    /// `https` is librdkafka 2.x's own default, so this pin is not a change of
    /// behaviour — it is the statement that a future default change cannot
    /// quietly weaken us, and the engine's rustls client verifies the hostname
    /// with no way to turn that off, so the two clients must agree.
    ///
    /// **FX-43, channel (a).** Every client this reader builds queues its log
    /// lines (`log.queue=true`, which [`super::pumped`] requires), and a
    /// queued line is printed on ONE line whatever the client logged: a
    /// bootstrap name or a broker's message holding a line break cannot start
    /// a line of the pod log. KILLS: the key dropped (librdkafka's default
    /// logger prints raw); the sink printing the message unescaped.
    #[test]
    fn a_clients_log_lines_are_queued_and_printed_on_one_line() {
        let cfg = super::RdKafkaReader::client_config(
            &["b0:9092".to_string()],
            &super::AuthConfig::Plaintext,
        )
        .expect("a plaintext config");
        assert_eq!(cfg.get("log.queue"), Some("true"));
        let line = logweir_rdkafka_ffi::logs::LogLine {
            level: 3,
            facility: logweir_rdkafka_ffi::CText::Utf8("FAIL".to_string()),
            message: logweir_rdkafka_ffi::CText::Utf8(
                "b0\nfailure-reason=ExecutionAlreadyClaimed:9092/bootstrap: Failed to resolve"
                    .to_string(),
            ),
        };
        let printed = super::client_log_line(&line);
        assert_eq!(printed.lines().count(), 1, "{printed}");
        assert_eq!(
            printed,
            "librdkafka: %3|FAIL| b0\\nfailure-reason=ExecutionAlreadyClaimed:9092/bootstrap: \
             Failed to resolve"
        );
    }

    /// NO SOCKET. `ClientConfig` is a `HashMap` until `create()` is called,
    /// and `create()` is never called here.
    #[test]
    fn the_tls_client_pins_hostname_verification_and_the_projected_ca() {
        use super::RdKafkaReader;
        use crate::reader::AuthConfig;
        const CA: &str = "/connection/source-ca/ca.crt";
        let bootstrap = vec!["b0.orders:9093".to_string(), "b1.orders:9093".to_string()];
        let scram = |tls: bool, ca: Option<&str>| AuthConfig::ScramSha512 {
            username: "logweir".to_string(),
            password: "pw".to_string(),
            tls,
            tls_ca_file: ca.map(str::to_string),
        };

        // TLS, with and without a private CA: hostname verification is
        // `https` in BOTH, because a public-root connection needs it just as
        // much as a private-CA one.
        for ca in [None, Some(CA)] {
            let cfg = RdKafkaReader::client_config(&bootstrap, &scram(true, ca))
                .expect("a TLS SCRAM connection configures");
            assert_eq!(cfg.get("security.protocol"), Some("SASL_SSL"));
            assert_eq!(
                cfg.get("ssl.endpoint.identification.algorithm"),
                Some("https"),
                "broker hostnames are verified; `none` would accept any certificate that chains \
                 to a trusted root, for ANY host"
            );
        }

        // The CA the controller projected reaches librdkafka, as the
        // path and nothing derived from it. With `ssl.ca.location` set,
        // librdkafka skips the default verify paths, so the connection trusts
        // exactly this file.
        let with_ca =
            RdKafkaReader::client_config(&bootstrap, &scram(true, Some(CA))).expect("configures");
        assert_eq!(
            with_ca.get("ssl.ca.location"),
            Some(CA),
            "the projected CA reaches THIS client too, not the engine's alone (Global \
             Constraint 29)"
        );
        // …and no CA means no key at all — never an empty string, which
        // librdkafka would read as a path.
        let no_ca =
            RdKafkaReader::client_config(&bootstrap, &scram(true, None)).expect("configures");
        assert_eq!(no_ca.get("ssl.ca.location"), None);

        // The addresses are the ones handed in, comma-joined, and the
        // password is in the config and never in a bootstrap string.
        assert_eq!(
            with_ca.get("bootstrap.servers"),
            Some("b0.orders:9093,b1.orders:9093")
        );

        // A non-TLS transport carries neither key, and a CA on one is refused
        // rather than dropped — dropping it would dial in the clear a
        // connection whose author configured it to verify.
        for auth in [AuthConfig::Plaintext, scram(false, None)] {
            let cfg = RdKafkaReader::client_config(&bootstrap, &auth).expect("configures");
            assert_eq!(cfg.get("ssl.endpoint.identification.algorithm"), None);
            assert_eq!(cfg.get("ssl.ca.location"), None);
        }
        let err = RdKafkaReader::client_config(&bootstrap, &scram(false, Some(CA)))
            .expect_err("a CA without TLS is refused");
        assert!(
            matches!(err, crate::reader::KafkaError::Client(_)),
            "{err:?}"
        );
        assert!(
            !err.to_string().contains("pw"),
            "and the refusal names no credential: {err}"
        );
    }

    #[test]
    fn partition_eof_is_a_dedicated_variant_the_old_string_match_never_caught() {
        use rdkafka::error::KafkaError as RdErr;
        let eof = RdErr::PartitionEOF(0);
        // The fact `consume_range`'s
        // `Some(Err(rdkafka::error::KafkaError::PartitionEOF(_)))` arm
        // depends on: this is a real, matchable variant, not folded into
        // `MessageConsumption`.
        assert!(matches!(eof, RdErr::PartitionEOF(_)));
        // `.to_string()` calls `Display`, whose text for this variant is
        // "Partition EOF: {n}": a guard that searched it for "Broker: No more
        // messages" never matched.
        assert_eq!(eof.to_string(), "Partition EOF: 0");
        assert!(!eof.to_string().contains("Broker: No more messages"));
        assert!(!format!("{eof:?}").contains("Broker: No more messages"));
    }

    #[test]
    fn auto_offset_reset_surfaces_as_message_consumption_with_a_specific_code() {
        use rdkafka::error::{KafkaError as RdErr, RDKafkaErrorCode as Code};
        // The fact `consume_range`'s `auto.offset.reset=error` handling
        // depends on: this condition is NOT a dedicated variant (unlike
        // `PartitionEOF`) — it is `MessageConsumption` carrying this one
        // specific code, so the guard must match on the code, not the
        // variant alone.
        let reset = RdErr::MessageConsumption(Code::AutoOffsetReset);
        assert!(matches!(reset, RdErr::MessageConsumption(c) if c == Code::AutoOffsetReset));
    }

    /// Exactly the answers of a creation in progress are waited
    /// out by `await_served`; an authorization failure, a timeout and a
    /// transport failure are not, because waiting cannot change them and a
    /// wait on them would only delay the refusal by `CREATED_TOPIC_SETTLE`.
    #[test]
    fn only_the_answers_of_a_creation_in_progress_are_waited_out() {
        use rdkafka::error::RDKafkaErrorCode as Code;
        for code in [
            Code::UnknownTopicOrPartition,
            Code::UnknownTopic,
            Code::UnknownPartition,
            Code::LeaderNotAvailable,
            Code::NotLeaderForPartition,
        ] {
            assert!(super::creation_in_progress(code), "{code:?} is propagation");
        }
        for code in [
            Code::TopicAuthorizationFailed,
            Code::ClusterAuthorizationFailed,
            Code::OperationTimedOut,
            Code::RequestTimedOut,
            Code::BrokerTransportFailure,
            Code::AllBrokersDown,
            Code::InvalidTopic,
        ] {
            assert!(!super::creation_in_progress(code), "{code:?} is final");
        }
    }

    /// The configuration read after a create waits out
    /// exactly `TopicNotFound` and `NotAuthorized` (an empty answer while the
    /// topic propagates), and returns every other error at once.
    #[test]
    fn the_created_topic_configuration_read_waits_out_only_propagation() {
        use crate::reader::{KafkaError, Settling};
        let done = super::created_topic_config_settling(Ok(Default::default()));
        assert!(matches!(done, Settling::Done(_)), "{done:?}");
        for waits in [
            KafkaError::TopicNotFound("t".into()),
            KafkaError::NotAuthorized("t".into()),
        ] {
            let s = super::created_topic_config_settling(Err(waits));
            assert!(matches!(s, Settling::NotYet(_)), "{s:?} is propagation");
        }
        for fails in [
            KafkaError::Unreachable("down".into()),
            KafkaError::Client("bad".into()),
            KafkaError::Timeout(std::time::Duration::from_secs(1)),
        ] {
            let s = super::created_topic_config_settling(Err(fails));
            assert!(matches!(s, Settling::Failed(_)), "{s:?} is final");
        }
    }

    #[test]
    fn unknown_topic_and_authorization_failed_classify_distinctly() {
        // The fact `classify_topic_error` depends on: these are the two
        // codes it special-cases, verified against rdkafka-sys
        // 4.10.0+2.12.1's error table (UnknownTopicOrPartition = 3,
        // TopicAuthorizationFailed = 29).
        use crate::reader::KafkaError;
        use rdkafka::error::RDKafkaErrorCode as Code;
        assert!(matches!(
            super::RdKafkaReader::classify_topic_error("t", Code::UnknownTopicOrPartition),
            KafkaError::TopicNotFound(t) if t == "t"
        ));
        assert!(matches!(
            super::RdKafkaReader::classify_topic_error("t", Code::TopicAuthorizationFailed),
            KafkaError::NotAuthorized(t) if t == "t"
        ));
        assert!(matches!(
            super::RdKafkaReader::classify_topic_error("t", Code::UnknownMemberId),
            KafkaError::Client(_)
        ));
    }

    #[test]
    fn with_scratch_prefix_rejects_an_empty_prefix_at_construction_not_at_delete_time() {
        // `connect` does not dial anything synchronously, so this needs no
        // broker to prove `with_scratch_prefix("")` is refused at THIS call:
        // `"anything".starts_with("")` is true, so an empty prefix would pass
        // every name, a production topic included.
        //
        // `with_scratch_prefix` consumes `self`, so each case below needs its
        // own freshly connected reader.
        fn reader() -> super::RdKafkaReader {
            super::RdKafkaReader::connect(
                &["127.0.0.1:1".to_string()],
                crate::reader::AuthConfig::Plaintext,
            )
            .expect("connect() only builds local client state; it does not dial the broker")
        }
        assert!(reader().with_scratch_prefix("").is_err());
        assert!(reader().with_scratch_prefix("  ").is_err());
        assert!(
            reader().with_scratch_prefix("dr").is_err(),
            "2 chars is below the 3-char floor"
        );
    }

    /// A source topic's own name is never handed to the
    /// broker's delete, even inside the scratch namespace (a source named
    /// `drill-orders` under the scratch prefix `drill-`): refused per name,
    /// before any broker call — `127.0.0.1:1` is never dialled, so a name
    /// that reached `AdminClient::delete_topics` would come back as a
    /// transport error, not this refusal. KILLS: dropping the protected-name
    /// arm, or checking it after the namespace arm.
    #[test]
    fn a_protected_source_name_is_refused_before_the_broker_even_inside_the_namespace() {
        use crate::reader::TopicDeleter;
        let reader = super::RdKafkaReader::connect(
            &["127.0.0.1:1".to_string()],
            crate::reader::AuthConfig::Plaintext,
        )
        .unwrap()
        .with_scratch_prefix("drill-")
        .unwrap()
        .with_protected_names(["drill-orders".to_string()]);
        let out = reader
            .delete_topics(&["drill-orders".to_string()])
            .expect("a per-name answer");
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].0, "drill-orders");
        let why = out[0].1.as_ref().expect_err("refused");
        assert!(
            why.contains("a source topic's own name") && why.contains("original name"),
            "{why}"
        );
    }

    #[test]
    fn with_scratch_prefix_accepts_a_realistic_operator_rendered_prefix() {
        let reader = super::RdKafkaReader::connect(
            &["127.0.0.1:1".to_string()],
            crate::reader::AuthConfig::Plaintext,
        )
        .unwrap();
        assert!(reader.with_scratch_prefix("drill-20260903-").is_ok());
    }

    // An empty DescribeConfigs answer is a REFUSAL, never "no overrides".
    // Constructed rdkafka results, no broker: the result structs
    // `rdkafka::admin::ConfigResource` and `ConfigEntry` have public fields
    // (rdkafka-0.36.2/src/admin.rs:1000-1024).

    fn entry(
        name: &str,
        value: Option<&str>,
        source: rdkafka::admin::ConfigSource,
    ) -> rdkafka::admin::ConfigEntry {
        rdkafka::admin::ConfigEntry {
            name: name.to_string(),
            value: value.map(str::to_string),
            source,
            is_read_only: false,
            is_default: false,
            is_sensitive: false,
        }
    }

    fn topic_resource(
        name: &str,
        entries: Vec<rdkafka::admin::ConfigEntry>,
    ) -> rdkafka::admin::ConfigResourceResult {
        Ok(rdkafka::admin::ConfigResource {
            specifier: rdkafka::admin::OwnedResourceSpecifier::Topic(name.to_string()),
            entries,
        })
    }

    /// **A denied resource is never read back as "no overrides".** rdkafka
    /// 0.36.2 hands a refused topic back as `Ok` with NO entries; the reader
    /// must turn that into the refusal it stands for, whatever the metadata
    /// says, and never into `Ok`.
    #[test]
    fn an_empty_topic_answer_is_a_refusal_never_an_empty_override_set() {
        use crate::reader::{KafkaError, TopicVisibility};
        for (visibility, want) in [
            (TopicVisibility::Visible, "NotAuthorized"),
            (TopicVisibility::NotAuthorized, "NotAuthorized"),
            (TopicVisibility::NotFound, "TopicNotFound"),
            (TopicVisibility::Unread("timed out".into()), "Client"),
        ] {
            let got = super::RdKafkaReader::topic_answer_with(
                "orders",
                topic_resource("orders", vec![]),
                || visibility.clone(),
            );
            let kind = match &got {
                Ok(entries) => panic!(
                    "an EMPTY DescribeConfigs answer (metadata {visibility:?}) came back Ok \
                     with {} entr(y/ies) — that is the defect: a refused read as no overrides",
                    entries.len()
                ),
                Err(KafkaError::NotAuthorized(_)) => "NotAuthorized",
                Err(KafkaError::TopicNotFound(_)) => "TopicNotFound",
                Err(KafkaError::Client(_)) => "Client",
                Err(other) => panic!("unexpected {other:?}"),
            };
            assert_eq!(kind, want, "metadata {visibility:?}");
        }
    }

    /// The metadata read is spent ONLY on an empty answer, and a real answer
    /// keeps every entry with its source — the flags the capture filter reads.
    #[test]
    fn a_non_empty_topic_answer_keeps_every_entry_and_its_source() {
        use crate::reader::ConfigSourceKind;
        use rdkafka::admin::ConfigSource as S;
        let got = super::RdKafkaReader::topic_answer_with(
            "orders",
            topic_resource(
                "orders",
                vec![
                    entry("retention.ms", Some("3600000"), S::DynamicTopic),
                    entry(
                        "message.timestamp.type",
                        Some("LogAppendTime"),
                        S::DynamicDefaultBroker,
                    ),
                    entry("cleanup.policy", Some("delete"), S::Default),
                    entry("min.insync.replicas", Some("1"), S::StaticBroker),
                    entry("segment.ms", Some("1"), S::DynamicBroker),
                    entry("ssl.secret", None, S::Unknown),
                ],
            ),
            || panic!("metadata must not be read for an answer that has entries"),
        )
        .expect("a non-empty answer is a successful read");
        let sources: Vec<(String, ConfigSourceKind)> =
            got.iter().map(|e| (e.name.clone(), e.source)).collect();
        assert_eq!(
            sources,
            vec![
                ("retention.ms".into(), ConfigSourceKind::DynamicTopicConfig),
                (
                    "message.timestamp.type".into(),
                    ConfigSourceKind::DynamicDefaultBrokerConfig
                ),
                ("cleanup.policy".into(), ConfigSourceKind::DefaultConfig),
                (
                    "min.insync.replicas".into(),
                    ConfigSourceKind::StaticBrokerConfig
                ),
                ("segment.ms".into(), ConfigSourceKind::DynamicBrokerConfig),
                ("ssl.secret".into(), ConfigSourceKind::Unknown),
            ],
            "the source of every entry is kept: FX-8's broker-default arm reads it"
        );
        assert_eq!(got[5].value, None, "a withheld value stays withheld");
    }

    /// **The no-misfire direction.** Most topics
    /// carry NO override: an authorised describe of one answers every entry
    /// from the broker or the built-in defaults, and none from the topic (31
    /// entries on 3.7.1, 33 on 4.3.1, measured). Such an answer is a
    /// successful read with every entry kept — a refusal keyed on "no
    /// topic-override entry" instead of "no entry at all" would sign
    /// `captureDenied` for the common case while every other test passed.
    #[test]
    fn an_answer_with_no_topic_override_is_a_successful_read_with_every_entry() {
        use crate::reader::ConfigSourceKind;
        use rdkafka::admin::ConfigSource as S;
        let got = super::RdKafkaReader::topic_answer_with(
            "plain",
            topic_resource(
                "plain",
                vec![
                    entry("cleanup.policy", Some("delete"), S::Default),
                    entry("message.timestamp.type", Some("CreateTime"), S::Default),
                    entry("min.insync.replicas", Some("1"), S::StaticBroker),
                    entry("retention.ms", Some("604800000"), S::DynamicDefaultBroker),
                    entry("segment.bytes", Some("1073741824"), S::DynamicBroker),
                ],
            ),
            || panic!("metadata must not be read for an answer that has entries"),
        )
        .expect("an answer with entries and no override is a successful read");
        assert_eq!(got.len(), 5, "every entry is kept: {got:?}");
        assert!(
            got.iter()
                .all(|e| e.source != ConfigSourceKind::DynamicTopicConfig),
            "the fixture really carries no topic override: {got:?}"
        );
        assert_eq!(
            got.iter()
                .find(|e| e.name == "message.timestamp.type")
                .map(|e| (e.value.as_deref(), e.source)),
            Some((Some("CreateTime"), ConfigSourceKind::DefaultConfig)),
            "the effective timestamp type and its source survive"
        );
    }

    /// A per-resource error code, should a later rdkafka ever report one, is
    /// classified exactly as `classify_topic_error` classifies it.
    #[test]
    fn a_reported_per_resource_code_is_classified() {
        use crate::reader::KafkaError;
        use rdkafka::error::RDKafkaErrorCode as Code;
        let got = super::RdKafkaReader::topic_answer_with(
            "orders",
            Err(Code::TopicAuthorizationFailed),
            || panic!("a reported code needs no metadata"),
        );
        assert!(matches!(got, Err(KafkaError::NotAuthorized(_))), "{got:?}");
    }

    /// The broker twin: an empty broker answer is the cluster authorizer's
    /// refusal (measured), never an empty configuration —
    /// which phase 0's G-TS read as "the Apache default, CreateTime".
    #[test]
    fn an_empty_broker_answer_is_a_refusal_never_an_empty_configuration() {
        use crate::reader::KafkaError;
        let empty = rdkafka::admin::ConfigResource {
            specifier: rdkafka::admin::OwnedResourceSpecifier::Broker(1001),
            entries: vec![],
        };
        let got = super::RdKafkaReader::broker_answer(1001, empty);
        assert!(
            matches!(got, Err(KafkaError::NotAuthorized(ref m)) if m.contains("broker 1001")),
            "{got:?}"
        );
        let full = rdkafka::admin::ConfigResource {
            specifier: rdkafka::admin::OwnedResourceSpecifier::Broker(1001),
            entries: vec![entry(
                "log.message.timestamp.type",
                Some("LogAppendTime"),
                rdkafka::admin::ConfigSource::DynamicDefaultBroker,
            )],
        };
        assert_eq!(
            super::RdKafkaReader::broker_answer(1001, full)
                .unwrap()
                .get("log.message.timestamp.type")
                .map(String::as_str),
            Some("LogAppendTime")
        );
    }

    /// The per-topic answer a broker gives a delete the principal
    /// may not make is named as such, and no other answer is.
    #[test]
    fn a_topic_authorization_refusal_is_told_from_other_delete_failures() {
        use rdkafka::types::RDKafkaErrorCode as Code;
        assert_eq!(
            Code::TopicAuthorizationFailed.to_string(),
            "TopicAuthorizationFailed (Broker: Topic authorization failed)"
        );
        assert!(super::is_topic_authorization_failed(
            &Code::TopicAuthorizationFailed.to_string()
        ));
        for other in [
            Code::TopicDeletionDisabled,
            Code::UnknownTopicOrPartition,
            Code::ClusterAuthorizationFailed,
            Code::RequestTimedOut,
        ] {
            assert!(!super::is_topic_authorization_failed(&other.to_string()));
        }
    }
}
