use crate::positions::{
    commit_request, fetch_request, valid_bound, CommitError, CommittedPosition, GroupListing,
    GroupPositions, PositionsError, TopicPartition, DEFAULT_POSITION_BOUND,
};
use crate::rdkafka_positions::GroupHandle;
use crate::reader::{
    empty_broker_config_answer, empty_topic_config_answer, AuthConfig, ClusterReader,
    ConfigEntryObservation, ConfigSourceKind, ConsumedRecord, KafkaError, NewTopicSpec,
    TopicConfigRead, TopicCreator, TopicDeleter, TopicMeta, TopicVisibility,
};
use rdkafka::admin::AdminClient;
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
// `BorrowedHeaders::count`/`::get` (used in `consume_range` below) are
// provided by the `Headers` trait, not the type itself — this import isn't
// in the brief's Step 5 listing but the code doesn't compile without it.
use rdkafka::message::Headers;
use rdkafka::{Message, Offset, TopicPartitionList};
use std::collections::BTreeMap;
use std::time::Duration;

const T: Duration = Duration::from_secs(20);

pub struct RdKafkaReader {
    consumer: BaseConsumer,
    admin: AdminClient<DefaultClientContext>,
    /// The connection `connect` dialled with ([`RdKafkaReader::client_config`]),
    /// kept so [`RdKafkaReader::committed_positions`] and
    /// [`RdKafkaReader::commit_positions`] can open a per-group handle on the
    /// same cluster as the same principal. It holds the SASL password, as the
    /// two clients above already do inside librdkafka; this type has no
    /// `Debug` and never prints it.
    base: ClientConfig,
    /// The bound of one positions fetch or commit wait (PROD-04.0a).
    position_bound: Duration,
    /// The drill's own scratch-topic namespace. `TopicDeleter::delete_topics`
    /// refuses any name that does not start with this prefix, and refuses
    /// EVERYTHING until it is set via `with_scratch_prefix` — `connect`
    /// alone never enables deletion. This is the only code path in the whole
    /// product that destroys data, and short of this guard its only
    /// protection was "the caller passed the right names" (see
    /// `delete_topics`'s doc comment for the fuller rationale).
    scratch_prefix: Option<String>,
}

impl RdKafkaReader {
    /// The exact `ClientConfig` [`RdKafkaReader::connect`] dials with, before
    /// the consumer-only keys are added.
    ///
    /// **EXTRACTED SO THE SECURITY-CRITICAL SETTINGS ARE ASSERTABLE WITHOUT A
    /// BROKER.** Two of the keys below are controls, not tuning:
    /// `ssl.endpoint.identification.algorithm` (hostname verification) and
    /// `ssl.ca.location` (which trust anchor). While they were built inline
    /// inside `connect`, no test could read either without opening a socket —
    /// and a review mutant that turned hostname verification off, and one that
    /// denied this reader the projected CA, both survived the whole suite. A
    /// `ClientConfig` is a map until `create()` is called, so
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
            // Explicit rather than relying on the default: librdkafka's own
            // docs note the consumer default (false) already differs from
            // the producer default (true) and from the Java consumer
            // (true), and that split has moved across versions — pin it so
            // a librdkafka upgrade can never silently create a topic on a
            // cluster an operator cares about via a metadata lookup for a
            // name that does not exist
            // [VERIFIED rdkafka-sys librdkafka/CONFIGURATION.md:63].
            .set("allow.auto.create.topics", "false");
        match auth {
            AuthConfig::Plaintext => {
                base.set("security.protocol", "PLAINTEXT");
            }
            AuthConfig::ScramSha512 {
                username,
                password,
                tls,
                tls_ca_file,
            } => {
                let tls = *tls;
                base.set(
                    "security.protocol",
                    if tls { "SASL_SSL" } else { "SASL_PLAINTEXT" },
                )
                .set("sasl.mechanism", "SCRAM-SHA-512")
                .set("sasl.username", username.as_str())
                .set("sasl.password", password.as_str());
                if tls {
                    // Explicit rather than relying on the default, for the
                    // reason `allow.auto.create.topics` is above: hostname
                    // verification is librdkafka 2.x's default (`https`) and
                    // this pins it, so an upgrade cannot quietly turn it off.
                    // There is deliberately no override — the engine's rustls
                    // client verifies the broker hostname with no way to turn
                    // that off, and the two clients must agree (PLAT-07.1).
                    // [VERIFIED rdkafka-sys 4.10.0+2.12.1's vendored
                    // librdkafka/CONFIGURATION.md: ssl.endpoint.identification.algorithm
                    // `none, https`, default `https`.]
                    base.set("ssl.endpoint.identification.algorithm", "https");
                    if let Some(ca) = tls_ca_file.as_deref() {
                        // `SSL_CTX_load_verify_locations` on this file ONLY:
                        // with `ssl.ca.location` set, librdkafka skips the
                        // default verify paths, so the connection trusts
                        // exactly the projected CA — the same set the engine's
                        // `ssl_ca_location` builds [VERIFIED vendored
                        // librdkafka/src/rdkafka_ssl.c: the `ca_location` branch
                        // clears `ca_probe`].
                        base.set("ssl.ca.location", ca);
                    }
                } else if tls_ca_file.is_some() {
                    return Err(KafkaError::Client(
                        logweir_core::connection::TlsCaWithoutTls {
                            mode: "scramSha512",
                        }
                        .to_string(),
                    ));
                }
            }
            AuthConfig::Token(_) => {
                return Err(KafkaError::Client(
                    "token auth (OAUTHBEARER / MSK IAM) is introduced by SP4".into(),
                ));
            }
        }
        Ok(base)
    }

    pub fn connect(bootstrap: &[String], auth: AuthConfig) -> Result<Self, KafkaError> {
        let base = Self::client_config(bootstrap, &auth)?;
        // Fix round 2, nit M7: these four properties are meaningful only to
        // a CONSUMER. Setting them on `base` and sharing `base` with the
        // admin client's `create()` (the previous shape) made librdkafka log
        // a `CONFWARN` for every one of them when the admin client's
        // underlying (producer-shaped) handle was built — four lines of
        // noise interleaved into `doctor`'s check list, and into every
        // `drill run`'s log, on every single connection. Cloning `base` here
        // means the admin client's `create()` below never sees these keys at
        // all, so librdkafka has nothing to warn about — this removes the
        // cause rather than filtering the symptom.
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
        let admin: AdminClient<DefaultClientContext> = base
            .create()
            .map_err(|e| KafkaError::Client(e.to_string()))?;
        Ok(Self {
            consumer,
            admin,
            base,
            position_bound: DEFAULT_POSITION_BOUND,
            scratch_prefix: None,
        })
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

    /// **PROD-04.0a.** The committed positions of `group` on exactly
    /// `partitions`, read with ONE RequireStable OffsetFetch from a fresh
    /// handle that carries the group's id and can never subscribe, join or
    /// commit implicitly (`rdkafka_positions`), dropped before this returns.
    ///
    /// - One entry per requested partition, in request order. A partition
    ///   with no committed offset is [`PartitionPosition::NoCommittedPosition`],
    ///   never offset 0; so is every partition of an ABSENT group and of a
    ///   SHARE group (K7), so classify the group before reading meaning into
    ///   it (PROD-04.0 §5).
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

    /// **PROD-04.0a.** Commits `positions` for `group` in ONE synchronous
    /// OffsetCommit from a non-member (generation −1, empty member id), from a
    /// fresh handle that can never subscribe or join, dropped before this
    /// returns.
    ///
    /// - The broker refuses the WHOLE request while the group has members:
    ///   [`CommitError::GroupActive`], nothing changed (§3.3). A share group
    ///   is [`CommitError::NotAConsumerGroup`]; a principal without Read on
    ///   the group [`CommitError::NotAuthorized`]; no coordinator within the
    ///   bound [`CommitError::NotVisibleOrUnreachable`], nothing sent.
    /// - An ABSENT group id is created by the commit as a simple classic
    ///   group (K3): classify the target first (PROD-04.0 §4.3, §5).
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
    /// topic-not-found vs. not-authorised vs. anything else — instead of the
    /// generic `Client` bucket every code used to fall into regardless of
    /// cause. Verified against the vendored rdkafka-sys 4.10.0+2.12.1 error
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
    /// by (T13). Read only for a topic whose answer came back empty.
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

    /// One BROKER resource's answer under T13, flattened: an empty entry list
    /// is the cluster authorizer's refusal, never an empty configuration.
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

    /// One topic resource's answer under T13: an empty entry list is a
    /// FAILED read, named from the topic's metadata; never an empty set of
    /// overrides. Shared by [`ClusterReader::topic_configs`] and
    /// [`ClusterReader::describe_topic_configs`], so the two cannot disagree.
    fn topic_answer(
        &self,
        topic: &str,
        result: rdkafka::admin::ConfigResourceResult,
    ) -> TopicConfigRead {
        Self::topic_answer_with(topic, result, || self.topic_visibility(topic))
    }

    /// [`Self::topic_answer`] with the metadata read INJECTED, so the T13
    /// decision is testable over a constructed rdkafka result with no broker
    /// (`tests::an_empty_topic_answer_is_a_refusal_never_an_empty_override_set`).
    /// `visibility` is called only for an empty answer.
    fn topic_answer_with(
        topic: &str,
        result: rdkafka::admin::ConfigResourceResult,
        visibility: impl FnOnce() -> TopicVisibility,
    ) -> TopicConfigRead {
        // The `Err` arm is what the TYPE promises and rdkafka 0.36.2 never
        // produces (T13): kept, because a later rdkafka that reads the
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

impl ClusterReader for RdKafkaReader {
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
            // `describe_configs` yields one `Result<_, RDKafkaErrorCode>`
            // per resource [VERIFIED rdkafka-0.36.2/src/admin.rs:948, the
            // alias whose Ok side is the resource-plus-entries struct at
            // :1019]. The error is a BARE code, not the
            // `(String, RDKafkaErrorCode)` tuple that belongs to
            // `TopicResult`; the tuple pattern does not match and the crate
            // does not compile. (The alias's own NAME is spelled out in
            // `tests/reader.rs`'s
            // `the_broker_resource_is_spelled_the_rdkafka_way`, which is why
            // it is a file:line here.)
            //
            // **AND THAT `Err` NEVER ARRIVES (FX-4, T13).** rdkafka 0.36.2's
            // future pushes `Ok` for every resource and never reads the
            // per-resource error (`src/admin.rs:1121-1159`), so a DENIED topic
            // used to come back here as an empty entry list and leave this
            // function as `Ok({})` — "no overrides". `topic_answer` turns the
            // empty list into the refusal it stands for.
            for e in self.topic_answer(topic, r)? {
                if let Some(v) = e.value {
                    out.insert(e.name, v);
                }
            }
        }
        Ok(out)
    }

    /// FX-4. One DescribeConfigs request for every named topic.
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
            // FX-4, T13: an EMPTY answer is the refusal rdkafka 0.36.2 does
            // not report (PROD-04.0 §3.8 measured it for a principal refused
            // on the cluster). Read as `Ok({})` it made phase 0's G-TS assume
            // the Apache default `CreateTime` and skip the timestamp bound.
            out.extend(Self::broker_answer(broker_id, cfg)?);
        }
        Ok(out)
    }

    fn consume_range(
        &self,
        topic: &str,
        partition: i32,
        from: i64,
        max: usize,
    ) -> Result<Vec<ConsumedRecord>, KafkaError> {
        // Bound the read by the partition's high watermark rather than
        // relying on an end-of-partition ERROR to know when to stop — the
        // previous version matched `e.to_string().contains("Broker: No more
        // messages")` against whatever `poll()` returned on EOF, but rdkafka
        // 0.36.2 routes `RD_KAFKA_RESP_ERR__PARTITION_EOF` to a DEDICATED
        // variant, `KafkaError::PartitionEOF(i32)`, before it ever reaches
        // the generic error path
        // [VERIFIED rdkafka-0.36.2/src/consumer/base_consumer.rs's
        // `handle_error_event`: `if rdkafka_err ==
        // RD_KAFKA_RESP_ERR__PARTITION_EOF { ...
        // Some(KafkaError::PartitionEOF(partition)) } else if ... else {
        // Some(KafkaError::MessageConsumption(rdkafka_err.into())) }` — EOF
        // is matched and returned before the generic
        // `MessageConsumption` arm is ever reached]. That variant's
        // `Display` is `"Partition EOF: {n}"`
        // [VERIFIED rdkafka-0.36.2/src/error.rs:283, the `impl fmt::Display
        // for KafkaError` block — NOT line 236's `impl fmt::Debug`, which
        // renders the same variant as `"KafkaError (Partition EOF: {n})"`;
        // `.to_string()` calls `Display`], a string the old guard never
        // matched — `"Broker: No more messages"` does not appear anywhere in
        // either impl. So every short read (the common case: `max` bigger
        // than what remains in the partition) fell through to the generic
        // arm below and turned an ordinary end-of-partition into a hard
        // error that discarded every record already collected.
        //
        // Bounding by the watermark up front — the same value upstream's own
        // fetch path keys off (`kafka-backup-core/src/kafka/fetch.rs`'s
        // `high_watermark`, read from the raw Fetch response rather than
        // inferred from an error) — means the common short-read case never
        // needs the error path at all; the `PartitionEOF` match below is now
        // a defensive fallback for the rarer case where the watermark shifts
        // between this call and the poll loop (e.g. concurrent retention).
        //
        // A single `fetch_watermarks(topic, partition, ..)` call, not
        // `end_offsets(topic)`: `end_offsets` does one `fetch_metadata` PLUS
        // one blocking `fetch_watermarks` per partition of the whole topic,
        // then this call would have discarded every result but the one
        // partition it asked for. A drill phase reading a P-partition topic
        // partition-by-partition would have driven P metadata fetches and
        // P² ListOffsets round trips for information a single call already
        // provides directly.
        //
        // The error from this single-partition call cannot distinguish "the
        // topic does not exist" from "the topic exists but this partition
        // index does not" — Kafka's own wire protocol returns the identical
        // `UNKNOWN_TOPIC_OR_PARTITION` code for both, so there is no basis
        // to call this `KafkaError::TopicNotFound` (whose payload is a bare
        // topic name everywhere else it's constructed, and whose own doc
        // says the TOPIC does not exist — not "this specific
        // topic:partition combination could not be resolved, cause
        // unknown"). A generic `Client` error naming both coordinates is the
        // honest option here, not a new variant whose semantics would need
        // the same qualification.
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
                // `auto.offset.reset=error` (set in `connect`) turns a
                // `from` outside the log's retained range into this
                // specific error instead of librdkafka silently reseeking
                // to the log start or end and returning the WRONG records
                // with no signal that it did so. It surfaces as
                // `KafkaError::MessageConsumption` carrying this specific
                // code, not a dedicated variant of its own
                // [VERIFIED rdkafka-0.36.2/src/consumer/base_consumer.rs's
                // `handle_error_event`: PARTITION_EOF and fatal errors get
                // their own arms, everything else — including
                // AUTO_OFFSET_RESET — falls into
                // `KafkaError::MessageConsumption(code)`].
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
        let res = rt
            .block_on(
                self.admin
                    .create_topics(&new_topics, &AdminOptions::new().request_timeout(Some(T))),
            )
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
}

impl TopicDeleter for RdKafkaReader {
    /// Exactly the named topics. rdkafka 0.36's `delete_topics` takes
    /// `&[&str]` and returns `Vec<TopicResult>` where
    /// `TopicResult = Result<String, (String, RDKafkaErrorCode)>` — note the
    /// error here IS the `(name, code)` tuple, unlike the DescribeConfigs
    /// element type used by `topic_configs` and `broker_configs`
    /// (rdkafka-0.36.2/src/admin.rs:948), whose error is a bare code.
    ///
    /// This is the only code path in the entire product that destroys data.
    /// The trait's contract ("never a pattern, never a prefix") describes
    /// how a caller SPECIFIES what to delete — always exact, full names,
    /// never a wildcard — and says nothing about what this concrete impl
    /// may additionally refuse. It refuses two things beyond that contract,
    /// neither of which turns deletion into a pattern-based operation:
    /// every name must still be passed out in full, exactly as given.
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
        let Some(prefix) = self.scratch_prefix.as_deref() else {
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
        };
        // Names outside the configured namespace are reported as refused
        // and never reach the broker call at all; only in-namespace names
        // are sent to `AdminClient::delete_topics`. (Ordering note: refused
        // entries are reported first, followed by the broker's results for
        // the in-namespace names in the order rdkafka returned them — this
        // impl has no unit test asserting input-order preservation, unlike
        // `FakeDeleter` in `tests/reader.rs`, per addendum ruling A1.)
        let mut allowed: Vec<&str> = Vec::new();
        let mut out: Vec<(String, Result<(), String>)> = Vec::new();
        for n in names {
            if n.starts_with(prefix) {
                allowed.push(n.as_str());
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

#[cfg(test)]
mod tests {
    //! These need the `client` feature (this whole file is gated on it) but
    //! no broker — they pin the exact rdkafka facts `consume_range`'s FIX 1
    //! (see its doc comment) depends on, so a future rdkafka upgrade that
    //! silently changed either fact would fail CI instead of silently
    //! reintroducing the original bug: every short read (`max` larger than
    //! what remains in the partition) discarding every record already
    //! collected. A full end-to-end short-read scenario needs a live broker
    //! (`BorrowedMessage` has no public constructor) and is not attempted
    //! here — see the Task 10 fix report's "unproven without a live broker"
    //! list.

    /// **THE TLS CLIENT PINS HOSTNAME VERIFICATION AND TRUSTS THE PROJECTED
    /// CA** — PLAT-07.1 review findings H1 and H2.
    ///
    /// Both are CONTROLS and both were unguarded: a review planted
    /// `ssl.endpoint.identification.algorithm = "none"` and a mutant that
    /// denied this reader the projected CA, and each survived all 725 rows of
    /// `-p logweir -p logweir-kafka`. A librdkafka upgrade, a merge resolution
    /// or a refactor could turn either off and CI would say nothing; the only
    /// signal would be a live TLS run nobody does per commit.
    ///
    /// `https` is librdkafka 2.x's own default, so this pin is not a change of
    /// behaviour — it is the statement that a future default change cannot
    /// quietly weaken us, and the engine's rustls client verifies the hostname
    /// with no way to turn that off, so the two clients must agree.
    ///
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

        // H1 — TLS, with and without a private CA: hostname verification is
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

        // H2 — the CA the controller projected reaches librdkafka, as the
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
        // The fact that made the OLD guard dead code: `.to_string()` (what
        // `e.to_string().contains(...)` read) calls `Display`, and
        // `Display`'s text for this variant is "Partition EOF: {n}" — the
        // string "Broker: No more messages" the old code searched for never
        // appears here, in `Debug`'s "KafkaError (Partition EOF: {n})", or
        // anywhere else in rdkafka 0.36.2's error text for this variant.
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
        // `connect` does not dial anything synchronously — librdkafka's
        // client creation validates config and starts background IO
        // threads, it does not block waiting for a broker — so this needs
        // no broker to prove `with_scratch_prefix("")` is refused at THIS
        // call, before any `delete_topics` call could ever see it. This is
        // the regression test for the hole the previous fix round left
        // open: `with_scratch_prefix("")` used to store `Some("")`, and
        // `"anything".starts_with("")` is unconditionally true, so every
        // name — including a production topic — would have passed the
        // namespace check while the reader still reported as properly
        // scoped.
        //
        // `with_scratch_prefix` consumes `self`, so each case below needs
        // its own freshly connected reader rather than reusing one across
        // assertions (connecting is local and broker-free either way, per
        // the doc comment above).
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

    #[test]
    fn with_scratch_prefix_accepts_a_realistic_operator_rendered_prefix() {
        let reader = super::RdKafkaReader::connect(
            &["127.0.0.1:1".to_string()],
            crate::reader::AuthConfig::Plaintext,
        )
        .unwrap();
        assert!(reader.with_scratch_prefix("drill-20260903-").is_ok());
    }

    // -----------------------------------------------------------------------
    // FX-4 / T13: an empty DescribeConfigs answer is a REFUSAL, never "no
    // overrides". Constructed rdkafka results, no broker: the result structs
    // `rdkafka::admin::ConfigResource` and `ConfigEntry` have public fields
    // (rdkafka-0.36.2/src/admin.rs:1000-1024).
    // -----------------------------------------------------------------------

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

    /// **The mutant the orchestrator named for FX-4: "a denied resource read
    /// back as 'no overrides'".** rdkafka 0.36.2 hands a refused topic back
    /// as `Ok` with NO entries; the reader must turn that into the refusal it
    /// stands for, whatever the metadata says, and never into `Ok`.
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

    /// **The no-misfire direction (FX-4 review M4, mutant R1).** Most topics
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
    /// refusal (measured by PROD-04.0 §3.8), never an empty configuration —
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
}
