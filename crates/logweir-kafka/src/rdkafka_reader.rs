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
}

impl RdKafkaReader {
    pub fn connect(bootstrap: &[String], auth: AuthConfig) -> Result<Self, KafkaError> {
        let mut c = ClientConfig::new();
        c.set("bootstrap.servers", bootstrap.join(","))
            .set("client.id", "logweir-drill")
            .set("group.id", "logweir-canary-do-not-commit")
            .set("enable.auto.commit", "false") // never commit on any cluster
            .set("enable.partition.eof", "true");
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
        Ok(Self { consumer, admin })
    }
}

impl ClusterReader for RdKafkaReader {
    fn cluster_id(&self) -> Result<String, KafkaError> {
        self.consumer
            .client()
            .fetch_cluster_id(T)
            .ok_or_else(|| KafkaError::Client("broker returned no cluster id".into()))
    }

    fn list_topics(&self) -> Result<Vec<TopicMeta>, KafkaError> {
        let md = self
            .consumer
            .fetch_metadata(None, T)
            .map_err(|e| KafkaError::Client(e.to_string()))?;
        Ok(md
            .topics()
            .iter()
            .map(|t| TopicMeta {
                name: t.name().to_string(),
                partitions: t.partitions().len() as i32,
            })
            .collect())
    }

    fn end_offsets(&self, topic: &str) -> Result<Vec<(i32, i64)>, KafkaError> {
        let md = self
            .consumer
            .fetch_metadata(Some(topic), T)
            .map_err(|e| KafkaError::Client(e.to_string()))?;
        let t = md
            .topics()
            .first()
            .ok_or_else(|| KafkaError::Client(format!("topic {topic} not found")))?;
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
            .map_err(|e| KafkaError::Client(e.to_string()))?;
        let mut out = BTreeMap::new();
        for r in res {
            // rdkafka 0.36's element type is
            // `ConfigResourceResult = Result<ConfigResource, RDKafkaErrorCode>`
            // [VERIFIED https://docs.rs/rdkafka/0.36.2/rdkafka/admin/type.ConfigResourceResult.html].
            // The error is a BARE code, not the `(String, RDKafkaErrorCode)`
            // tuple that belongs to `TopicResult`; the tuple pattern does not
            // match and the crate does not compile.
            let cfg = r.map_err(|e| KafkaError::Client(e.to_string()))?;
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
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(topic, partition, Offset::Offset(from))
            .map_err(|e| KafkaError::Client(e.to_string()))?;
        self.consumer
            .assign(&tpl)
            .map_err(|e| KafkaError::Client(e.to_string()))?;
        let deadline = std::time::Instant::now() + T;
        let mut out = Vec::with_capacity(max);
        while out.len() < max {
            if std::time::Instant::now() > deadline {
                return Err(KafkaError::Timeout(T));
            }
            match self.consumer.poll(Duration::from_millis(500)) {
                None => continue,
                Some(Err(e)) if e.to_string().contains("Broker: No more messages") => break,
                Some(Err(e)) => return Err(KafkaError::Client(e.to_string())),
                Some(Ok(m)) => {
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
    fn delete_topics(
        &self,
        names: &[String],
    ) -> Result<Vec<(String, Result<(), String>)>, KafkaError> {
        use rdkafka::admin::AdminOptions;
        if names.is_empty() {
            return Ok(Vec::new());
        }
        let refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
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
                    .delete_topics(&refs, &AdminOptions::new().request_timeout(Some(T))),
            )
            .map_err(|e| KafkaError::Client(e.to_string()))?;
        Ok(res
            .into_iter()
            .map(|r| match r {
                Ok(name) => (name, Ok(())),
                Err((name, code)) => (name, Err(code.to_string())),
            })
            .collect())
    }
}
