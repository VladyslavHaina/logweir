//! One configurable double per runner trait: [`Reader`]
//! (`ClusterReader`), [`Engine`] (`DataEngine`), [`Creator`] (`TopicCreator`)
//! and [`Deleter`] (`TopicDeleter`). A test sets only what it reads; every
//! other method answers the variant's default. A method the trait gives a
//! default (`describe_topic_configs`, `backup`, ...) keeps the trait's own
//! answer unless a test sets it.

use std::collections::BTreeMap;
use std::sync::Mutex;

use logweir_core::engine::{
    BackupFacts, BackupPlan, BackupSetFacts, BackupSetRef, DataEngine, EngineError, EngineId,
    PhaseObserver, PreflightReport, RecordFingerprint, RestoreFacts, RestorePlan, SampleSelection,
    StorageUrl,
};
use logweir_kafka::reader::{
    ClusterReader, ConsumedRecord, KafkaError, NewTopicSpec, TopicConfigRead, TopicCreator,
    TopicDeleter, TopicMeta,
};

type Answer<T> = Result<T, KafkaError>;
type Configs = BTreeMap<String, String>;

// ------------------------------------------------------------------ Reader

/// One topic as [`Reader`] holds it.
#[derive(Default, Clone)]
pub struct TopicData {
    pub end_offsets: Vec<(i32, i64)>,
    pub configs: Configs,
    pub records: Vec<ConsumedRecord>,
}

type OnTopic<T> = Box<dyn Fn(&str) -> Answer<T> + Send + Sync>;
type OnConsume = Box<dyn Fn(&str, i32, i64, usize) -> Answer<Vec<ConsumedRecord>> + Send + Sync>;
type OnDescribe = Box<dyn Fn(&[String]) -> Answer<Vec<(String, TopicConfigRead)>> + Send + Sync>;

/// A `ClusterReader` answering from `topics`. A name not in `topics` reads as
/// `TopicNotFound` when the reader is strict ([`Reader::target`]) and as empty
/// otherwise ([`Reader::new`], [`Reader::lenient`]). `list_topics` names every
/// topic with one partition per `end_offsets` entry, unless [`Reader::listing`]
/// or [`Reader::lists`] says otherwise. The broker configuration is empty.
/// Each `on_*` hook replaces one method's answer.
pub struct Reader {
    cluster_id: Answer<String>,
    pub topics: BTreeMap<String, TopicData>,
    strict: bool,
    listing: Option<Vec<TopicMeta>>,
    on_end_offsets: Option<OnTopic<Vec<(i32, i64)>>>,
    on_topic_configs: Option<OnTopic<Configs>>,
    on_broker_configs: Option<Box<dyn Fn() -> Answer<Configs> + Send + Sync>>,
    on_consume_range: Option<OnConsume>,
    on_describe_topic_configs: Option<OnDescribe>,
}

impl Reader {
    /// A cluster with this id and no topics; every read answers empty.
    pub fn new(cluster_id: &str) -> Self {
        Self::answering(Ok(cluster_id.to_string()))
    }

    /// [`Reader::new`] whose `cluster_id` answers `id`, which may be an error.
    pub fn answering(id: Answer<String>) -> Self {
        Self {
            cluster_id: id,
            topics: BTreeMap::new(),
            strict: false,
            listing: None,
            on_end_offsets: None,
            on_topic_configs: None,
            on_broker_configs: None,
            on_consume_range: None,
            on_describe_topic_configs: None,
        }
    }

    /// A restore target keyed strictly by topic name: a name it was not built
    /// with is `TopicNotFound`, never silently empty.
    pub fn target(topics: BTreeMap<String, TopicData>) -> Self {
        Self {
            topics,
            strict: true,
            ..Self::new("TARGET0000000000000000")
        }
    }

    /// A cluster answering from `topics`, and empty for any other name.
    pub fn lenient(cluster_id: &str, topics: BTreeMap<String, TopicData>) -> Self {
        Self {
            topics,
            ..Self::new(cluster_id)
        }
    }

    /// What `list_topics` answers, whatever `topics` holds.
    pub fn listing(mut self, topics: Vec<TopicMeta>) -> Self {
        self.listing = Some(topics);
        self
    }

    /// [`Reader::listing`] plus one healthy topic.
    pub fn lists(mut self, topic: &str, partitions: i32) -> Self {
        self.listing
            .get_or_insert_with(Vec::new)
            .push(TopicMeta::new(topic, partitions));
        self
    }

    pub fn on_end_offsets(
        mut self,
        f: impl Fn(&str) -> Answer<Vec<(i32, i64)>> + Send + Sync + 'static,
    ) -> Self {
        self.on_end_offsets = Some(Box::new(f));
        self
    }

    pub fn on_topic_configs(
        mut self,
        f: impl Fn(&str) -> Answer<Configs> + Send + Sync + 'static,
    ) -> Self {
        self.on_topic_configs = Some(Box::new(f));
        self
    }

    pub fn on_broker_configs(
        mut self,
        f: impl Fn() -> Answer<Configs> + Send + Sync + 'static,
    ) -> Self {
        self.on_broker_configs = Some(Box::new(f));
        self
    }

    pub fn on_consume_range(
        mut self,
        f: impl Fn(&str, i32, i64, usize) -> Answer<Vec<ConsumedRecord>> + Send + Sync + 'static,
    ) -> Self {
        self.on_consume_range = Some(Box::new(f));
        self
    }

    pub fn on_describe_topic_configs(
        mut self,
        f: impl Fn(&[String]) -> Answer<Vec<(String, TopicConfigRead)>> + Send + Sync + 'static,
    ) -> Self {
        self.on_describe_topic_configs = Some(Box::new(f));
        self
    }

    fn topic(&self, topic: &str) -> Answer<Option<&TopicData>> {
        match self.topics.get(topic) {
            Some(d) => Ok(Some(d)),
            None if self.strict => Err(KafkaError::TopicNotFound(topic.to_string())),
            None => Ok(None),
        }
    }
}

/// The trait's own answers, for the methods a [`Reader`] leaves unset.
struct TraitDefaults;

impl ClusterReader for TraitDefaults {
    fn cluster_id(&self) -> Answer<String> {
        unreachable!()
    }
    fn list_topics(&self) -> Answer<Vec<TopicMeta>> {
        unreachable!()
    }
    fn end_offsets(&self, _: &str) -> Answer<Vec<(i32, i64)>> {
        unreachable!()
    }
    fn topic_configs(&self, _: &str) -> Answer<Configs> {
        unreachable!()
    }
    fn broker_configs(&self) -> Answer<Configs> {
        unreachable!()
    }
    fn consume_range(&self, _: &str, _: i32, _: i64, _: usize) -> Answer<Vec<ConsumedRecord>> {
        unreachable!()
    }
}

impl ClusterReader for Reader {
    fn cluster_id(&self) -> Answer<String> {
        self.cluster_id.clone()
    }
    fn list_topics(&self) -> Answer<Vec<TopicMeta>> {
        Ok(match &self.listing {
            Some(listing) => listing.clone(),
            None => self
                .topics
                .iter()
                .map(|(n, d)| TopicMeta::new(n.clone(), d.end_offsets.len() as i32))
                .collect(),
        })
    }
    fn end_offsets(&self, topic: &str) -> Answer<Vec<(i32, i64)>> {
        match &self.on_end_offsets {
            Some(f) => f(topic),
            None => Ok(self
                .topic(topic)?
                .map(|d| d.end_offsets.clone())
                .unwrap_or_default()),
        }
    }
    fn topic_configs(&self, topic: &str) -> Answer<Configs> {
        match &self.on_topic_configs {
            Some(f) => f(topic),
            None => Ok(self
                .topic(topic)?
                .map(|d| d.configs.clone())
                .unwrap_or_default()),
        }
    }
    fn broker_configs(&self) -> Answer<Configs> {
        match &self.on_broker_configs {
            Some(f) => f(),
            None => Ok(BTreeMap::new()),
        }
    }
    fn consume_range(
        &self,
        topic: &str,
        partition: i32,
        from: i64,
        max: usize,
    ) -> Answer<Vec<ConsumedRecord>> {
        if let Some(f) = &self.on_consume_range {
            return f(topic, partition, from, max);
        }
        Ok(self
            .topic(topic)?
            .map(|d| {
                d.records
                    .iter()
                    .filter(|r| r.partition == partition && r.offset >= from)
                    .take(max)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default())
    }
    fn describe_topic_configs(&self, topics: &[String]) -> Answer<Vec<(String, TopicConfigRead)>> {
        match &self.on_describe_topic_configs {
            Some(f) => f(topics),
            None => TraitDefaults.describe_topic_configs(topics),
        }
    }
}

// ------------------------------------------------------------------ Engine

type EngineAnswer<T> = Result<T, EngineError>;
type OnList<'a> = Box<dyn Fn(&StorageUrl) -> EngineAnswer<Vec<BackupSetRef>> + 'a>;
type OnDescribeSet<'a> = Box<dyn Fn(&BackupSetRef) -> EngineAnswer<BackupSetFacts> + 'a>;
type OnPreflight<'a> = Box<dyn Fn(&RestorePlan) -> EngineAnswer<PreflightReport> + 'a>;
type OnRestore<'a> =
    Box<dyn Fn(&RestorePlan, &mut dyn PhaseObserver) -> EngineAnswer<RestoreFacts> + 'a>;
type OnFingerprints<'a> =
    Box<dyn Fn(&SampleSelection) -> EngineAnswer<Vec<RecordFingerprint>> + 'a>;
type OnBackup<'a> =
    Box<dyn Fn(&BackupPlan, &mut dyn PhaseObserver) -> EngineAnswer<BackupFacts> + 'a>;

/// `sha256:` and 64 zeros, the digest the engine doubles report.
pub fn zero_digest() -> String {
    format!("sha256:{}", "0".repeat(64))
}

enum Identity {
    Is(EngineId),
    Panics(&'static str),
    Unreachable,
}

/// A `DataEngine` built from one hook per method. `backup` and
/// `describe_with_notices` keep the trait's defaults unless set. A hook may
/// borrow for `'a` (a test's store, say).
pub struct Engine<'a> {
    id: Identity,
    list: OnList<'a>,
    describe: OnDescribeSet<'a>,
    preflight: OnPreflight<'a>,
    restore: OnRestore<'a>,
    fingerprints: OnFingerprints<'a>,
    backup: Option<OnBackup<'a>>,
}

impl<'a> Engine<'a> {
    /// The restore-side double: engine `fake` `v0.0.0`. It lists no sets,
    /// reads no fingerprints, and answers `describe`, `preflight` and
    /// `restore` with `Operational("not used by this fixture")`.
    pub fn new() -> Self {
        Self {
            id: Identity::Is(EngineId {
                id: "fake".into(),
                version: "v0.0.0".into(),
                digest: zero_digest(),
            }),
            list: Box::new(|_| Ok(vec![])),
            describe: Box::new(|_| Err(not_used("not used by this fixture"))),
            preflight: Box::new(|_| Err(not_used("not used by this fixture"))),
            restore: Box::new(|_, _| Err(not_used("not used by this fixture"))),
            fingerprints: Box::new(|_| Ok(vec![])),
            backup: None,
        }
    }

    /// The backup-side double: engine `double` `v0.21.0`, whose restore-side
    /// methods are never reached by the backup path. Set `on_backup`, and
    /// `on_describe` when the run reads its manifest back.
    pub fn for_backup() -> Self {
        Self {
            id: Identity::Is(EngineId {
                id: "double".into(),
                version: "v0.21.0".into(),
                digest: "sha256:0".into(),
            }),
            list: Box::new(|_| {
                unimplemented!("the backup path lists through the Store handle, not the engine")
            }),
            describe: Box::new(|_| unimplemented!()),
            preflight: Box::new(|_| unimplemented!()),
            restore: Box::new(|_, _| unimplemented!()),
            fingerprints: Box::new(|_| unimplemented!()),
            backup: None,
        }
    }

    /// An engine every required method of which panics with
    /// `"{why} DataEngine::{method}"`.
    pub fn never_called(why: &'static str) -> Self {
        Self {
            id: Identity::Panics(why),
            list: Box::new(move |_| panic!("{why} DataEngine::list_backup_sets")),
            describe: Box::new(move |_| panic!("{why} DataEngine::describe")),
            preflight: Box::new(move |_| panic!("{why} DataEngine::preflight")),
            restore: Box::new(move |_, _| panic!("{why} DataEngine::restore")),
            fingerprints: Box::new(move |_| panic!("{why} DataEngine::fingerprints")),
            backup: None,
        }
    }

    /// An engine no required method of which is ever reached.
    pub fn unreachable() -> Self {
        Self {
            id: Identity::Unreachable,
            list: Box::new(|_| unreachable!()),
            describe: Box::new(|_| unreachable!()),
            preflight: Box::new(|_| unreachable!()),
            restore: Box::new(|_, _| unreachable!()),
            fingerprints: Box::new(|_| unreachable!()),
            backup: None,
        }
    }

    pub fn identified(mut self, id: &str, version: &str, digest: &str) -> Self {
        self.id = Identity::Is(EngineId {
            id: id.into(),
            version: version.into(),
            digest: digest.into(),
        });
        self
    }

    /// `describe` answers these facts for every set.
    pub fn describing(self, facts: BackupSetFacts) -> Self {
        self.on_describe(move |_| Ok(facts.clone()))
    }

    /// `fingerprints` answers this list for every selection.
    pub fn fingerprinting(self, fingerprints: Vec<RecordFingerprint>) -> Self {
        self.on_fingerprints(move |_| Ok(fingerprints.clone()))
    }

    /// `fingerprints` answers `Unsupported(why)` for every selection.
    pub fn unsupported(self, why: &str) -> Self {
        let why = why.to_string();
        self.on_fingerprints(move |_| Err(EngineError::Unsupported(why.clone())))
    }

    pub fn on_list(
        mut self,
        f: impl Fn(&StorageUrl) -> EngineAnswer<Vec<BackupSetRef>> + 'a,
    ) -> Self {
        self.list = Box::new(f);
        self
    }

    pub fn on_describe(
        mut self,
        f: impl Fn(&BackupSetRef) -> EngineAnswer<BackupSetFacts> + 'a,
    ) -> Self {
        self.describe = Box::new(f);
        self
    }

    pub fn on_preflight(
        mut self,
        f: impl Fn(&RestorePlan) -> EngineAnswer<PreflightReport> + 'a,
    ) -> Self {
        self.preflight = Box::new(f);
        self
    }

    pub fn on_restore(
        mut self,
        f: impl Fn(&RestorePlan, &mut dyn PhaseObserver) -> EngineAnswer<RestoreFacts> + 'a,
    ) -> Self {
        self.restore = Box::new(f);
        self
    }

    pub fn on_fingerprints(
        mut self,
        f: impl Fn(&SampleSelection) -> EngineAnswer<Vec<RecordFingerprint>> + 'a,
    ) -> Self {
        self.fingerprints = Box::new(f);
        self
    }

    pub fn on_backup(
        mut self,
        f: impl Fn(&BackupPlan, &mut dyn PhaseObserver) -> EngineAnswer<BackupFacts> + 'a,
    ) -> Self {
        self.backup = Some(Box::new(f));
        self
    }
}

impl Default for Engine<'_> {
    fn default() -> Self {
        Self::new()
    }
}

fn not_used(why: &str) -> EngineError {
    EngineError::Operational(why.into())
}

/// The trait's own `backup`, for an [`Engine`] that sets none.
struct NoBackup;

impl DataEngine for NoBackup {
    fn id(&self) -> EngineId {
        unreachable!()
    }
    fn list_backup_sets(&self, _: &StorageUrl) -> EngineAnswer<Vec<BackupSetRef>> {
        unreachable!()
    }
    fn describe(&self, _: &BackupSetRef) -> EngineAnswer<BackupSetFacts> {
        unreachable!()
    }
    fn preflight(&self, _: &RestorePlan) -> EngineAnswer<PreflightReport> {
        unreachable!()
    }
    fn restore(&self, _: &RestorePlan, _: &mut dyn PhaseObserver) -> EngineAnswer<RestoreFacts> {
        unreachable!()
    }
    fn fingerprints(&self, _: &SampleSelection) -> EngineAnswer<Vec<RecordFingerprint>> {
        unreachable!()
    }
}

impl DataEngine for Engine<'_> {
    fn id(&self) -> EngineId {
        match &self.id {
            Identity::Is(id) => id.clone(),
            Identity::Panics(why) => panic!("{why} DataEngine::id"),
            Identity::Unreachable => unreachable!(),
        }
    }
    fn list_backup_sets(&self, loc: &StorageUrl) -> EngineAnswer<Vec<BackupSetRef>> {
        (self.list)(loc)
    }
    fn describe(&self, set: &BackupSetRef) -> EngineAnswer<BackupSetFacts> {
        (self.describe)(set)
    }
    fn preflight(&self, plan: &RestorePlan) -> EngineAnswer<PreflightReport> {
        (self.preflight)(plan)
    }
    fn restore(
        &self,
        plan: &RestorePlan,
        obs: &mut dyn PhaseObserver,
    ) -> EngineAnswer<RestoreFacts> {
        (self.restore)(plan, obs)
    }
    fn fingerprints(&self, sel: &SampleSelection) -> EngineAnswer<Vec<RecordFingerprint>> {
        (self.fingerprints)(sel)
    }
    fn backup(&self, plan: &BackupPlan, obs: &mut dyn PhaseObserver) -> EngineAnswer<BackupFacts> {
        match &self.backup {
            Some(f) => f(plan, obs),
            None => NoBackup.backup(plan, obs),
        }
    }
}

// ----------------------------------------------------- Creator and Deleter

/// A `TopicCreator` that records every call in `calls` and creates every
/// topic, except the ones `refusing` names, which it answers with that error.
#[derive(Default)]
pub struct Creator {
    pub calls: Mutex<Vec<NewTopicSpec>>,
    refused: BTreeMap<String, String>,
}

impl Creator {
    pub fn refusing(mut self, topic: &str, why: &str) -> Self {
        self.refused.insert(topic.to_string(), why.to_string());
        self
    }
}

impl TopicCreator for Creator {
    fn create_topics(&self, topics: &[NewTopicSpec]) -> Answer<Vec<(String, Result<(), String>)>> {
        self.calls.lock().unwrap().extend_from_slice(topics);
        Ok(topics
            .iter()
            .map(|t| match self.refused.get(&t.name) {
                Some(why) => (t.name.clone(), Err(why.clone())),
                None => (t.name.clone(), Ok(())),
            })
            .collect())
    }
}

/// A `TopicDeleter` that records every call in `calls` and deletes every
/// topic, except the ones `refusing` names (or every one, `refusing_all`),
/// which it answers with that error. `failing` makes the whole call fail.
#[derive(Default)]
pub struct Deleter {
    pub calls: Mutex<Vec<String>>,
    refused: BTreeMap<String, String>,
    refuse_all: Option<String>,
    fails: Option<KafkaError>,
}

impl Deleter {
    pub fn refusing(mut self, topic: &str, why: &str) -> Self {
        self.refused.insert(topic.to_string(), why.to_string());
        self
    }

    pub fn refusing_all(mut self, why: &str) -> Self {
        self.refuse_all = Some(why.to_string());
        self
    }

    pub fn failing(mut self, error: KafkaError) -> Self {
        self.fails = Some(error);
        self
    }
}

impl TopicDeleter for Deleter {
    fn delete_topics(&self, names: &[String]) -> Answer<Vec<(String, Result<(), String>)>> {
        if let Some(e) = &self.fails {
            return Err(e.clone());
        }
        self.calls.lock().unwrap().extend_from_slice(names);
        Ok(names
            .iter()
            .map(|n| match self.refuse_all.as_ref().or(self.refused.get(n)) {
                Some(why) => (n.clone(), Err(why.clone())),
                None => (n.clone(), Ok(())),
            })
            .collect())
    }
}
