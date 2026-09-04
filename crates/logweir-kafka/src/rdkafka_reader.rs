use crate::reader::{
    AuthConfig, ClusterReader, ConsumedRecord, KafkaError, TopicDeleter, TopicMeta,
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
    pub fn connect(bootstrap: &[String], auth: AuthConfig) -> Result<Self, KafkaError> {
        let mut c = ClientConfig::new();
        c.set("bootstrap.servers", bootstrap.join(","))
            .set("client.id", "logweir-drill")
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
            .set("auto.offset.reset", "error")
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
                c.set("security.protocol", "PLAINTEXT");
            }
            AuthConfig::ScramSha512 {
                username,
                password,
                tls,
            } => {
                c.set(
                    "security.protocol",
                    if tls { "SASL_SSL" } else { "SASL_PLAINTEXT" },
                )
                .set("sasl.mechanism", "SCRAM-SHA-512")
                .set("sasl.username", username)
                .set("sasl.password", password);
            }
            AuthConfig::Token(_) => {
                return Err(KafkaError::Client(
                    "token auth (OAUTHBEARER / MSK IAM) is introduced by SP4".into(),
                ));
            }
        }
        let consumer: BaseConsumer = c.create().map_err(|e| KafkaError::Client(e.to_string()))?;
        let admin: AdminClient<DefaultClientContext> =
            c.create().map_err(|e| KafkaError::Client(e.to_string()))?;
        Ok(Self {
            consumer,
            admin,
            scratch_prefix: None,
        })
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
            // rdkafka 0.36's element type is
            // `ConfigResourceResult = Result<ConfigResource, RDKafkaErrorCode>`
            // [VERIFIED https://docs.rs/rdkafka/0.36.2/rdkafka/admin/type.ConfigResourceResult.html].
            // The error is a BARE code, not the `(String, RDKafkaErrorCode)`
            // tuple that belongs to `TopicResult`; the tuple pattern does not
            // match and the crate does not compile.
            let cfg = r.map_err(|e| Self::classify_topic_error(topic, e))?;
            for e in cfg.entries {
                if let Some(v) = e.value {
                    out.insert(e.name, v);
                }
            }
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

impl TopicDeleter for RdKafkaReader {
    /// Exactly the named topics. rdkafka 0.36's `delete_topics` takes
    /// `&[&str]` and returns `Vec<TopicResult>` where
    /// `TopicResult = Result<String, (String, RDKafkaErrorCode)>` — note the
    /// error here IS the `(name, code)` tuple, unlike `ConfigResourceResult`
    /// in `topic_configs`, whose error is a bare code.
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
}
