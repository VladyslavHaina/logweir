//! D2 §4.2's check runner, `logweir check run` — the contract, the frames, the
//! exit codes and the six mutants.
//!
//! # What this file is, and why it is shaped this way
//!
//! The runner's whole output is a byte stream a CONTROLLER parses. So the
//! strongest assertion available is not "the result struct has this field" but
//! "these exact bytes are accepted by `check_contract::frames::Decoder` — the
//! decoder `weirkeeper::check::relay` calls — and carry this". Every kind's
//! row therefore runs the real emission path (`check::execute_with`) into a
//! buffer and decodes it, so a change that breaks the relay fails here rather
//! than in a controller integration test nobody runs on a laptop.
//!
//! # Nothing here dials, except the e2e-gated rows at the end
//!
//! Every default row drives the runner through [`FakeWiring`], whose broker is
//! a `&dyn InventoryProbe` over a map and whose object store is a
//! `&dyn ObjectAccess` over a `BTreeMap`. One row builds a REAL `Store` over a
//! tempdir filesystem URL, because the parity claim between this crate's pure
//! manifest walk and `logweir_store`'s own cannot be made against a double.
//! The `e2e`-gated rows at the end are the only ones that open a socket, and
//! `crates/logweir/tests/no_network_in_unit_tests.rs` carries this file with
//! that reason.
//!
//! # The mutants
//!
//! | # | mutant | test that kills it |
//! |---|---|---|
//! | M1 | a client is built before the plan verifies | `the_startup_path_builds_no_client` + `a_refused_plan_prints_one_line_and_no_frame` |
//! | M2 | the plan digest is not checked | `a_wrong_plan_digest_is_refused_with_no_frames`, `a_restore_preflight_with_a_bad_plan_hash_opens_nothing` |
//! | M3 | exit 0 without an end line | `a_writer_that_refuses_bytes_is_operational_not_ok` |
//! | M4 | a credential reaches a message | `a_credential_planted_in_every_error_path_is_redacted` |
//! | M5 | the marker is not create-only | `the_only_write_in_the_check_runner_is_create_only`, `a_marker_that_is_already_present_is_write_authorised` |
//! | M6 | the relay budget is ignored | `the_writer_stops_at_the_relay_budget` |

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use chrono::{DateTime, TimeZone, Utc};
use logweir::check::frames::FrameWriter;
use logweir::check::kinds::{self, Wiring};
use logweir::check::store::{ObjectAccess, StoreFailure};
use logweir::check::{self, CheckRunArgs, Loaded};
use logweir::exit::ExitCode;
use logweir_core::backup_receipt::BackupReceipt;
use logweir_core::check_contract::{
    frames::Decoder, CheckCode, CheckId, CheckPlan, CheckRelay, CheckRequest, CheckResult,
    CheckState, ConnectionPlan, CredentialMode, DestinationAccessRequest, DestinationPlan,
    EvidenceFetchRequest, EvidenceObjectRequest, FrameExpectations, Gating, GrantCredentials,
    GrantRef, OperationReadinessRequest, RestorePreflightRequest, Stream, TopicEntry,
    TopicInventoryRequest, CHECK_CONTRACT_VERSION, CHECK_PLAN_CONTRACT,
};
use logweir_core::destination::{
    Addressing, DestinationLocation, DestinationRole, StorageProvider, TransportSecurity,
};
use logweir_engine_oso::storage::{PutOutcome, StoreError};
use logweir_kafka::api_versions::ApiVersions;
use logweir_kafka::inventory::{
    CheckFailure, InventoryProbe, ListedTopic, Listing, TopicCreateOutcome, TopicPresence,
};
use logweir_kafka::reader::NewTopicSpec;

// ===========================================================================
// Fixtures
// ===========================================================================

/// The subject this suite's plans are about.
const SUBJECT_UID: &str = "11111111-2222-3333-4444-555555555555";
/// The destination UID the marker and the absent-probe key are built from.
const DEST_UID: &str = "99999999-8888-7777-6666-555555555555";

/// Obviously fake, obviously a credential, and the shape
/// `check_contract::redact`'s AWS rule matches.
const PLANTED_KEY_ID: &str = "AKIAFAKEFAKEFAKEFAKE";
/// A secret-shaped value in the `secret_access_key=<value>` form.
const PLANTED_SECRET: &str = "aws_secret_access_key=ZZfakefakefakefakefakefakefakefake01";
/// A URL whose userinfo is the credential.
const PLANTED_USERINFO: &str = "https://root:hunter2@minio.example:9000";

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .canonicalize()
        .expect("the repository root resolves from CARGO_MANIFEST_DIR")
}

fn now() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 9, 16, 12, 0, 0).unwrap()
}

fn connection() -> ConnectionPlan {
    ConnectionPlan {
        bootstrap_servers: vec!["broker.example:9092".to_string()],
        auth_mode: "plaintext".to_string(),
        username: None,
        password_env: None,
        tls: Some(false),
        ca_file: None,
        client_cert_file: None,
        client_key_file: None,
        principal: "User:ANONYMOUS".to_string(),
    }
}

fn location() -> DestinationLocation {
    DestinationLocation {
        provider: StorageProvider::S3,
        bucket: "lw-archive".to_string(),
        prefix: "kafka-backups".to_string(),
        region: Some("us-east-1".to_string()),
        endpoint: Some("https://minio.example:9000".to_string()),
        addressing: Addressing::PathStyle,
        transport: TransportSecurity::Tls,
    }
}

fn destination() -> DestinationPlan {
    let loc = location();
    DestinationPlan {
        location_digest: loc.location_digest(),
        location: loc,
        name: "prod-archive".to_string(),
        uid: DEST_UID.to_string(),
        ca_file: None,
        credentials: CredentialMode::Static,
        grant_bindings: Vec::new(),
    }
}

fn plan_of(request: CheckRequest) -> CheckPlan {
    CheckPlan {
        contract: CHECK_PLAN_CONTRACT.to_string(),
        contract_version: CHECK_CONTRACT_VERSION,
        subject_uid: SUBJECT_UID.to_string(),
        timeout_seconds: 120,
        policy_digest: None,
        request,
    }
}

fn inventory_plan(max_topics: u32, relay_budget_bytes: u64) -> CheckPlan {
    plan_of(CheckRequest::TopicInventory(TopicInventoryRequest {
        connection: connection(),
        include_internal: false,
        expected_topics: Vec::new(),
        max_topics,
        relay_budget_bytes,
    }))
}

/// A plan, its canonical bytes and its digest — the three values the runner's
/// environment pins.
struct Mounted {
    bytes: Vec<u8>,
    sha256: String,
    loaded: Loaded,
}

fn mount(plan: &CheckPlan) -> Mounted {
    let bytes = serde_json::to_vec(plan).expect("a plan serialises");
    let sha256 = logweir_core::ids::sha256_prefixed(&bytes);
    Mounted {
        bytes,
        sha256: sha256.clone(),
        loaded: Loaded {
            plan: plan.clone(),
            plan_sha256: sha256,
            subject_uid: SUBJECT_UID.to_string(),
        },
    }
}

// ===========================================================================
// Doubles
// ===========================================================================

/// What a fake object store answers instead of an object.
#[derive(Clone, Debug)]
enum Fault {
    NotFound,
    /// Anything the classifier reaches through the token scan: the text is a
    /// REAL `object_store` message shape.
    Io(String),
    AlreadyExists,
}

impl Fault {
    fn to_error(&self, key: &str) -> StoreError {
        match self {
            Self::NotFound => StoreError::NotFound(key.to_string()),
            Self::Io(m) => StoreError::Io(format!("{key}: {m}")),
            Self::AlreadyExists => StoreError::AlreadyExists(key.to_string()),
        }
    }
}

#[derive(Default)]
struct ObjectState {
    objects: BTreeMap<String, Vec<u8>>,
    get_fault: Option<Fault>,
    /// Faults scoped to ONE key. A whole-store `get_fault` cannot tell a
    /// receipt read from a record read — which is how review finding F4's
    /// guard gap survived: the record read failed first, so the receipt
    /// branch was never reached with a failure at all.
    key_faults: BTreeMap<String, Fault>,
    /// The same, for a listing: one day shard that will not list is a
    /// different fact from a whole store that will not.
    list_prefix_faults: BTreeMap<String, Fault>,
    list_fault: Option<Fault>,
    put_fault: Option<Fault>,
    /// The backend answered `NotSupported`/`NotImplemented` to
    /// `PutMode::Create` and `put_create_only` fell back to HEAD-then-PUT, so
    /// the write happened WITHOUT the precondition (reviewer question Q1).
    unconditional_put: bool,
    puts: Vec<(String, Vec<u8>)>,
    /// Every operation this handle was asked for, as `get <key>`,
    /// `list <prefix>` or `put <key>` — so a least-privilege claim ("this
    /// principal was only ever asked to create one key") is an assertion and
    /// not a reading of the code.
    calls: Vec<String>,
    /// FX-31: every read's key and the cap it was made with, in call order —
    /// so "this read is bounded by THAT cap" is an assertion too.
    read_caps: Vec<(String, u64)>,
    /// FX-31: sizes the store reports for keys, over their real bytes.
    reported_sizes: BTreeMap<String, u64>,
    prefix: String,
    /// FX-7: a key's VERSION history, oldest first; the last entry is the
    /// current version, and its bytes are also the key's entry in `objects`.
    /// A key with no history is an object on an unversioned bucket.
    versions: BTreeMap<String, Vec<(String, Vec<u8>)>>,
}

#[derive(Clone, Default)]
struct FakeObjects {
    state: Arc<Mutex<ObjectState>>,
}

impl FakeObjects {
    fn new() -> Self {
        Self::default()
    }

    fn with_prefix(self, prefix: &str) -> Self {
        self.state.lock().unwrap().prefix = prefix.to_string();
        self
    }

    fn with_object(self, key: &str, bytes: &[u8]) -> Self {
        self.state
            .lock()
            .unwrap()
            .objects
            .insert(key.to_string(), bytes.to_vec());
        self
    }

    /// FX-31: the store REPORTS `size` for `key`, whatever its bytes are —
    /// so a row can hold a multi-gigabyte object without allocating one.
    fn reporting_size(self, key: &str, size: u64) -> Self {
        self.state
            .lock()
            .unwrap()
            .reported_sizes
            .insert(key.to_string(), size);
        self
    }

    fn failing_get(self, f: Fault) -> Self {
        self.state.lock().unwrap().get_fault = Some(f);
        self
    }

    /// FX-7: make `key` an object on a VERSIONED bucket with this history,
    /// oldest first. Its current bytes become the last version's.
    fn with_versions(self, key: &str, history: &[(&str, &[u8])]) -> Self {
        let mut s = self.state.lock().unwrap();
        let (_, current) = history.last().expect("a history has a current version");
        s.objects.insert(key.to_string(), current.to_vec());
        s.versions.insert(
            key.to_string(),
            history
                .iter()
                .map(|(id, bytes)| ((*id).to_string(), bytes.to_vec()))
                .collect(),
        );
        drop(s);
        self
    }

    /// Fail the `list_page` of ONE prefix, and only that prefix.
    fn failing_list_prefix(self, prefix: &str, f: Fault) -> Self {
        self.state
            .lock()
            .unwrap()
            .list_prefix_faults
            .insert(prefix.to_string(), f);
        self
    }

    /// Fail the `get` of ONE key, and only that key.
    fn failing_key(self, key: &str, f: Fault) -> Self {
        self.state
            .lock()
            .unwrap()
            .key_faults
            .insert(key.to_string(), f);
        self
    }

    fn failing_list(self, f: Fault) -> Self {
        self.state.lock().unwrap().list_fault = Some(f);
        self
    }

    fn failing_put(self, f: Fault) -> Self {
        self.state.lock().unwrap().put_fault = Some(f);
        self
    }

    /// A backend with conditional put disabled: the object is written, and
    /// `create_only_enforced` comes back `false`.
    fn unconditional_put(self) -> Self {
        self.state.lock().unwrap().unconditional_put = true;
        self
    }

    fn puts(&self) -> Vec<(String, Vec<u8>)> {
        self.state.lock().unwrap().puts.clone()
    }

    fn calls(&self) -> Vec<String> {
        self.state.lock().unwrap().calls.clone()
    }

    fn read_caps(&self) -> Vec<(String, u64)> {
        self.state.lock().unwrap().read_caps.clone()
    }
}

/// `Store::get_capped`'s first fence, in memory (FX-31): an object over the
/// caller's cap is `TooLarge` and its bytes are not handed back.
fn within_cap(key: &str, bytes: Vec<u8>, max_bytes: u64) -> Result<Vec<u8>, StoreError> {
    let size = bytes.len() as u64;
    if size > max_bytes {
        return Err(StoreError::TooLarge {
            key: key.to_string(),
            cap: max_bytes,
            observed: logweir_engine_oso::storage::OverCap::Reported(size),
        });
    }
    Ok(bytes)
}

impl ObjectAccess for FakeObjects {
    fn get(&self, key: &str, max_bytes: u64) -> Result<Vec<u8>, StoreError> {
        let mut s = self.state.lock().unwrap();
        s.calls.push(format!("get {key}"));
        s.read_caps.push((key.to_string(), max_bytes));
        if let Some(f) = s.key_faults.get(key) {
            return Err(f.to_error(key));
        }
        if let Some(f) = &s.get_fault {
            return Err(f.to_error(key));
        }
        let bytes = s
            .objects
            .get(key)
            .cloned()
            .ok_or_else(|| StoreError::NotFound(key.to_string()))?;
        if let Some(&size) = s.reported_sizes.get(key) {
            if size > max_bytes {
                return Err(StoreError::TooLarge {
                    key: key.to_string(),
                    cap: max_bytes,
                    observed: logweir_engine_oso::storage::OverCap::Reported(size),
                });
            }
        }
        within_cap(key, bytes, max_bytes)
    }

    /// `Store::list_page`'s contract, in memory: ascending keys under
    /// `prefix`, strictly after `start_after`, at most `max` of them.
    ///
    /// The exclusivity and the ordering are asserted here rather than assumed
    /// because a `catalogSync`'s `Full` rescan resumes from the cursor this
    /// returns, and a fake that repeated its last row would hide a walk that
    /// never advances.
    fn list_page(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        max: usize,
    ) -> Result<Vec<String>, StoreError> {
        let mut s = self.state.lock().unwrap();
        s.calls.push(format!("list {prefix}"));
        if let Some(f) = s.list_prefix_faults.get(prefix) {
            return Err(f.to_error(prefix));
        }
        if let Some(f) = &s.list_fault {
            return Err(f.to_error(prefix));
        }
        Ok(s.objects
            .keys()
            .filter(|k| k.starts_with(prefix))
            .filter(|k| start_after.is_none_or(|after| k.as_str() > after))
            .take(max)
            .cloned()
            .collect())
    }

    fn put_create_only(&self, key: &str, bytes: &[u8]) -> Result<PutOutcome, StoreError> {
        let mut s = self.state.lock().unwrap();
        s.calls.push(format!("put {key}"));
        if let Some(f) = &s.put_fault {
            return Err(f.to_error(key));
        }
        if s.objects.contains_key(key) {
            return Err(StoreError::AlreadyExists(key.to_string()));
        }
        s.objects.insert(key.to_string(), bytes.to_vec());
        s.puts.push((key.to_string(), bytes.to_vec()));
        let create_only_enforced = !s.unconditional_put;
        Ok(PutOutcome {
            version_id: None,
            create_only_enforced,
        })
    }

    fn qualify(&self, relative_key: &str) -> String {
        let s = self.state.lock().unwrap();
        if s.prefix.is_empty() {
            relative_key.to_string()
        } else {
            format!("{}/{}", s.prefix.trim_end_matches('/'), relative_key)
        }
    }

    /// FX-7: the current version, when the key has a history.
    fn get_with_version(
        &self,
        key: &str,
        max_bytes: u64,
    ) -> Result<(Vec<u8>, Option<String>), StoreError> {
        let bytes = self.get(key, max_bytes)?;
        let s = self.state.lock().unwrap();
        let current = s
            .versions
            .get(key)
            .and_then(|h| h.last())
            .map(|(id, _)| id.clone());
        Ok((bytes, current))
    }

    /// FX-7: one retained version by id; an unversioned key cannot be read by
    /// version at all, which is an error and never the current bytes.
    fn get_version(&self, key: &str, version: &str, max_bytes: u64) -> Result<Vec<u8>, StoreError> {
        let mut s = self.state.lock().unwrap();
        let at = format!("{key}?versionId={version}");
        s.calls.push(format!("get {at}"));
        s.read_caps.push((at.clone(), max_bytes));
        // A fault scoped to ONE version read (FX-7 fix round): `failing_key`
        // with the `<key>?versionId=<id>` spelling.
        if let Some(f) = s.key_faults.get(&at) {
            return Err(f.to_error(&at));
        }
        match s.versions.get(key) {
            None => Err(StoreError::Backend(format!(
                "{key} is not versioned; it cannot be read by version"
            ))),
            Some(history) => history
                .iter()
                .find(|(id, _)| id == version)
                .map(|(_, bytes)| bytes.clone())
                .ok_or_else(|| StoreError::NotFound(format!("{key}?versionId={version}")))
                .and_then(|bytes| {
                    // FX-31: a size the store REPORTS for one version
                    // (`reporting_size` with the `<key>?versionId=<id>`
                    // spelling), refused before its bytes as a current read is.
                    match s.reported_sizes.get(&at) {
                        Some(&size) if size > max_bytes => Err(StoreError::TooLarge {
                            key: at.clone(),
                            cap: max_bytes,
                            observed: logweir_engine_oso::storage::OverCap::Reported(size),
                        }),
                        _ => within_cap(&at, bytes, max_bytes),
                    }
                }),
        }
    }
}

/// FX-7: a REAL `Store` behind the check's object trait, shared across the
/// handles a wiring hands out — so a row can hold the live implementation's
/// `get_with_version` override in place. Every method delegates to `Store`'s
/// OWN trait implementation, including the two FX-7 methods: were `Store` to
/// lose its override, it would answer the trait's default (no version) here
/// too, and the row reading it would fail.
struct SharedStore(Arc<logweir_engine_oso::storage::Store>);

impl ObjectAccess for SharedStore {
    fn get(&self, key: &str, max_bytes: u64) -> Result<Vec<u8>, StoreError> {
        ObjectAccess::get(&*self.0, key, max_bytes)
    }
    fn list_page(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        max: usize,
    ) -> Result<Vec<String>, StoreError> {
        ObjectAccess::list_page(&*self.0, prefix, start_after, max)
    }
    fn put_create_only(&self, key: &str, bytes: &[u8]) -> Result<PutOutcome, StoreError> {
        ObjectAccess::put_create_only(&*self.0, key, bytes)
    }
    fn qualify(&self, relative_key: &str) -> String {
        ObjectAccess::qualify(&*self.0, relative_key)
    }
    fn get_with_version(
        &self,
        key: &str,
        max_bytes: u64,
    ) -> Result<(Vec<u8>, Option<String>), StoreError> {
        ObjectAccess::get_with_version(&*self.0, key, max_bytes)
    }
    fn get_version(&self, key: &str, version: &str, max_bytes: u64) -> Result<Vec<u8>, StoreError> {
        ObjectAccess::get_version(&*self.0, key, version, max_bytes)
    }
}

#[derive(Default)]
struct ProbeState {
    cluster_id: Option<String>,
    cluster_id_fault: Option<(CheckCode, String)>,
    listing: Listing,
    listing_fault: Option<(CheckCode, String)>,
    presence: BTreeMap<String, TopicPresence>,
    default_presence: Option<TopicPresence>,
    broker_configs: BTreeMap<String, String>,
    broker_configs_fault: Option<(CheckCode, String)>,
    create_outcomes: Option<Vec<TopicCreateOutcome>>,
    create_fault: Option<(CheckCode, String)>,
    create_calls: Vec<Vec<NewTopicSpec>>,
    describe_calls: Vec<String>,
    /// PROD-01.2: what the endpoint answered ApiVersions. `None` is the
    /// trait's own default: not observed.
    api_versions: Option<Result<ApiVersions, (CheckCode, String)>>,
    api_versions_calls: usize,
    /// PROD-01.2: topics whose DescribeConfigs fails, and how.
    topic_config_faults: BTreeMap<String, (CheckCode, String)>,
    topic_config_calls: Vec<String>,
}

#[derive(Clone, Default)]
struct FakeProbe {
    state: Arc<Mutex<ProbeState>>,
}

impl FakeProbe {
    /// A healthy Apache Kafka broker: it names a cluster id, and its
    /// configuration reports the record-timestamp bound, as every Apache
    /// Kafka broker does (unbounded is the value `i64::MAX`). PROD-01.2: a
    /// broker answer WITHOUT the key is "not reported", which is its own row
    /// (`without_broker_config`).
    fn new() -> Self {
        let p = Self::default();
        p.state.lock().unwrap().cluster_id = Some("M29I2S7FQPyHBEX12Vx7XA".to_string());
        p.state.lock().unwrap().listing.broker_count = 3;
        p.with_broker_config(
            logweir::check::kinds::restore::BROKER_TIMESTAMP_BEFORE_MAX_MS,
            &i64::MAX.to_string(),
        )
    }

    fn without_broker_config(self, k: &str) -> Self {
        self.state.lock().unwrap().broker_configs.remove(k);
        self
    }

    /// PROD-01.2: the endpoint's ApiVersions answer, as `(key, min, max)`.
    fn with_api_versions(self, ranges: &[(i16, i16, i16)]) -> Self {
        self.state.lock().unwrap().api_versions = Some(Ok(ApiVersions::of(ranges)));
        self
    }

    /// The whole view of a cluster of `brokers` brokers: its weakest answer,
    /// and whether the brokers' answers differed.
    fn with_cluster_api_versions(
        self,
        ranges: &[(i16, i16, i16)],
        brokers: usize,
        differ: bool,
    ) -> Self {
        self.state.lock().unwrap().api_versions =
            Some(Ok(ApiVersions::of_cluster(ranges, brokers, differ)));
        self
    }

    fn failing_api_versions(self, message: &str) -> Self {
        self.state.lock().unwrap().api_versions = Some(Err((
            CheckCode::ApiVersionsNotObserved,
            message.to_string(),
        )));
        self
    }

    fn api_versions_calls(&self) -> usize {
        self.state.lock().unwrap().api_versions_calls
    }

    fn failing_topic_configs(self, topic: &str, code: CheckCode, message: &str) -> Self {
        self.state
            .lock()
            .unwrap()
            .topic_config_faults
            .insert(topic.to_string(), (code, message.to_string()));
        self
    }

    fn topic_config_calls(&self) -> Vec<String> {
        self.state.lock().unwrap().topic_config_calls.clone()
    }

    fn with_topics(self, topics: &[(&str, u32)]) -> Self {
        let mut s = self.state.lock().unwrap();
        for (name, partitions) in topics {
            s.listing.topics.push(ListedTopic {
                name: (*name).to_string(),
                partitions: *partitions,
                error: None,
            });
        }
        drop(s);
        self
    }

    fn with_presence(self, name: &str, p: TopicPresence) -> Self {
        self.state
            .lock()
            .unwrap()
            .presence
            .insert(name.to_string(), p);
        self
    }

    fn default_presence(self, p: TopicPresence) -> Self {
        self.state.lock().unwrap().default_presence = Some(p);
        self
    }

    fn with_broker_config(self, k: &str, v: &str) -> Self {
        self.state
            .lock()
            .unwrap()
            .broker_configs
            .insert(k.to_string(), v.to_string());
        self
    }

    /// FX-4 / T13: the broker configuration read is REFUSED — what
    /// `KafkaInventory::broker_configs` now returns for the empty answer
    /// rdkafka 0.36.2 hands back when the principal lacks DescribeConfigs on
    /// the cluster (`ClusterAuthorizationFailed`), instead of an empty map.
    fn failing_broker_configs(self, code: CheckCode, message: &str) -> Self {
        self.state.lock().unwrap().broker_configs_fault = Some((code, message.to_string()));
        self
    }

    fn failing_listing(self, code: CheckCode, message: &str) -> Self {
        self.state.lock().unwrap().listing_fault = Some((code, message.to_string()));
        self
    }

    fn failing_cluster_id(self, code: CheckCode, message: &str) -> Self {
        self.state.lock().unwrap().cluster_id_fault = Some((code, message.to_string()));
        self
    }

    fn with_create_outcomes(self, outcomes: Vec<TopicCreateOutcome>) -> Self {
        self.state.lock().unwrap().create_outcomes = Some(outcomes);
        self
    }

    fn create_calls(&self) -> Vec<Vec<NewTopicSpec>> {
        self.state.lock().unwrap().create_calls.clone()
    }

    fn describe_calls(&self) -> Vec<String> {
        self.state.lock().unwrap().describe_calls.clone()
    }
}

impl InventoryProbe for FakeProbe {
    fn cluster_id(&self) -> Result<Option<String>, CheckFailure> {
        let s = self.state.lock().unwrap();
        match &s.cluster_id_fault {
            Some((c, m)) => Err(CheckFailure::new(*c, m)),
            None => Ok(s.cluster_id.clone()),
        }
    }

    fn list_topics(&self) -> Result<Listing, CheckFailure> {
        let s = self.state.lock().unwrap();
        match &s.listing_fault {
            Some((c, m)) => Err(CheckFailure::new(*c, m)),
            None => Ok(s.listing.clone()),
        }
    }

    fn describe_topic(&self, name: &str) -> Result<TopicPresence, CheckFailure> {
        let mut s = self.state.lock().unwrap();
        s.describe_calls.push(name.to_string());
        let answer = s
            .presence
            .get(name)
            .copied()
            .or(s.default_presence)
            .unwrap_or(TopicPresence::NotFound);
        Ok(answer)
    }

    fn topic_configs(&self, name: &str) -> Result<BTreeMap<String, String>, CheckFailure> {
        let mut s = self.state.lock().unwrap();
        s.topic_config_calls.push(name.to_string());
        match s.topic_config_faults.get(name) {
            Some((c, m)) => Err(CheckFailure::new(*c, m)),
            None => Ok(BTreeMap::from([(
                "cleanup.policy".to_string(),
                "delete".to_string(),
            )])),
        }
    }

    fn broker_configs(&self) -> Result<BTreeMap<String, String>, CheckFailure> {
        let s = self.state.lock().unwrap();
        match &s.broker_configs_fault {
            Some((c, m)) => Err(CheckFailure::new(*c, m)),
            None => Ok(s.broker_configs.clone()),
        }
    }

    fn api_versions(&self, _within: std::time::Duration) -> Result<ApiVersions, CheckFailure> {
        let mut s = self.state.lock().unwrap();
        s.api_versions_calls += 1;
        match &s.api_versions {
            Some(Ok(v)) => Ok(v.clone()),
            Some(Err((c, m))) => Err(CheckFailure::new(*c, m)),
            // The trait's own default: this probe observed nothing.
            None => Err(CheckFailure::new(
                CheckCode::ApiVersionsNotObserved,
                "this probe does not observe the endpoint's ApiVersions answer",
            )),
        }
    }

    fn validate_create_topics(
        &self,
        specs: &[NewTopicSpec],
    ) -> Result<Vec<TopicCreateOutcome>, CheckFailure> {
        let mut s = self.state.lock().unwrap();
        s.create_calls.push(specs.to_vec());
        if let Some((c, m)) = &s.create_fault {
            return Err(CheckFailure::new(*c, m));
        }
        Ok(s.create_outcomes.clone().unwrap_or_else(|| {
            specs
                .iter()
                .map(|n| TopicCreateOutcome {
                    name: n.name.clone(),
                    code: CheckCode::TopicCreateValidated,
                    message: String::new(),
                })
                .collect()
        }))
    }
}

#[derive(Default)]
struct FakeWiring {
    probe: Option<FakeProbe>,
    broker_fault: Option<(CheckCode, String)>,
    role_objects: BTreeMap<&'static str, FakeObjects>,
    role_faults: BTreeMap<&'static str, (CheckCode, String)>,
    writer: Option<FakeObjects>,
    writer_fault: Option<(CheckCode, String)>,
    /// The marker handle a SEPARATE `evidenceWrite` grant opens — a second
    /// principal, which can be denied where the destination grant is not.
    evidence_principal: Option<FakeObjects>,
    evidence_principal_fault: Option<(CheckCode, String)>,
    /// Every grant `evidence_writer` was handed, in order.
    writer_grants: Arc<Mutex<Vec<Option<GrantRef>>>>,
    /// The read handle a SEPARATE `evidenceRead` grant opens.
    evidence_reader: Option<FakeObjects>,
    evidence_reader_fault: Option<(CheckCode, String)>,
    /// Every grant `evidence_reader` was handed, in order.
    reader_grants: Arc<Mutex<Vec<Option<GrantRef>>>>,
    signer: Option<Result<String, String>>,
    files: BTreeMap<String, Vec<u8>>,
    /// FX-7: when set, EVERY `objects` handle is this real `Store` (see
    /// [`SharedStore`]) instead of a `FakeObjects`.
    shared_store: Option<Arc<logweir_engine_oso::storage::Store>>,
}

fn role_key(role: DestinationRole) -> &'static str {
    role.as_str()
}

impl FakeWiring {
    fn with_probe(mut self, p: FakeProbe) -> Self {
        self.probe = Some(p);
        self
    }

    fn broker_fails(mut self, code: CheckCode, message: &str) -> Self {
        self.broker_fault = Some((code, message.to_string()));
        self
    }

    fn with_role(mut self, role: DestinationRole, o: FakeObjects) -> Self {
        self.role_objects.insert(role_key(role), o);
        self
    }

    fn role_fails(mut self, role: DestinationRole, code: CheckCode, message: &str) -> Self {
        self.role_faults
            .insert(role_key(role), (code, message.to_string()));
        self
    }

    fn with_writer(mut self, o: FakeObjects) -> Self {
        self.writer = Some(o);
        self
    }

    /// The handle the separate `evidenceWrite` principal gets.
    fn with_evidence_principal(mut self, o: FakeObjects) -> Self {
        self.evidence_principal = Some(o);
        self
    }

    fn evidence_principal_fails(mut self, code: CheckCode, message: &str) -> Self {
        self.evidence_principal_fault = Some((code, message.to_string()));
        self
    }

    fn writer_grants(&self) -> Vec<Option<GrantRef>> {
        self.writer_grants.lock().unwrap().clone()
    }

    /// The handle the separate `evidenceRead` principal gets.
    fn with_evidence_reader(mut self, o: FakeObjects) -> Self {
        self.evidence_reader = Some(o);
        self
    }

    fn evidence_reader_fails(mut self, code: CheckCode, message: &str) -> Self {
        self.evidence_reader_fault = Some((code, message.to_string()));
        self
    }

    fn reader_grants(&self) -> Vec<Option<GrantRef>> {
        self.reader_grants.lock().unwrap().clone()
    }

    fn with_signer(mut self, r: Result<String, String>) -> Self {
        self.signer = Some(r);
        self
    }

    fn with_file(mut self, path: &str, bytes: &[u8]) -> Self {
        self.files.insert(path.to_string(), bytes.to_vec());
        self
    }
}

impl Wiring for FakeWiring {
    fn broker(
        &self,
        _plan: &ConnectionPlan,
        _budget: std::time::Duration,
    ) -> Result<Box<dyn InventoryProbe>, CheckFailure> {
        if let Some((c, m)) = &self.broker_fault {
            return Err(CheckFailure::new(*c, m));
        }
        Ok(Box::new(
            self.probe.clone().expect("this row wires a broker probe"),
        ))
    }

    fn objects(
        &self,
        _plan: &DestinationPlan,
        role: DestinationRole,
        _budget: std::time::Duration,
    ) -> Result<Box<dyn ObjectAccess>, StoreFailure> {
        if let Some((c, m)) = self.role_faults.get(role_key(role)) {
            return Err(StoreFailure::new(*c, m.clone()));
        }
        if let Some(store) = &self.shared_store {
            return Ok(Box::new(SharedStore(store.clone())));
        }
        Ok(Box::new(
            self.role_objects
                .get(role_key(role))
                .cloned()
                .unwrap_or_default(),
        ))
    }

    fn evidence_writer(
        &self,
        _plan: &DestinationPlan,
        grant: Option<&GrantRef>,
        _budget: std::time::Duration,
    ) -> Result<Box<dyn ObjectAccess>, StoreFailure> {
        self.writer_grants.lock().unwrap().push(grant.cloned());
        // TWO PRINCIPALS, TWO HANDLES. A wiring that ignored `grant` would
        // hand the destination grant's handle to a separated destination —
        // which is the defect these fakes exist to catch.
        if grant.is_some() {
            if let Some((c, m)) = &self.evidence_principal_fault {
                return Err(StoreFailure::new(*c, m.clone()));
            }
            return Ok(Box::new(
                self.evidence_principal.clone().unwrap_or_default(),
            ));
        }
        if let Some((c, m)) = &self.writer_fault {
            return Err(StoreFailure::new(*c, m.clone()));
        }
        Ok(Box::new(self.writer.clone().unwrap_or_default()))
    }

    fn evidence_reader(
        &self,
        plan: &DestinationPlan,
        grant: Option<&GrantRef>,
        budget: std::time::Duration,
    ) -> Result<Box<dyn ObjectAccess>, StoreFailure> {
        self.reader_grants.lock().unwrap().push(grant.cloned());
        // No separate grant: the destination grant's read handle, exactly the
        // one `objects` hands every other read.
        if grant.is_none() {
            return self.objects(plan, DestinationRole::EvidenceRead, budget);
        }
        if let Some((c, m)) = &self.evidence_reader_fault {
            return Err(StoreFailure::new(*c, m.clone()));
        }
        Ok(Box::new(self.evidence_reader.clone().unwrap_or_default()))
    }

    fn signer_key_id(&self, path: &str) -> Result<String, String> {
        self.signer
            .clone()
            .unwrap_or_else(|| Err(format!("no signing key at `{path}`")))
    }

    fn read_bytes(&self, path: &str) -> std::io::Result<Vec<u8>> {
        self.files.get(path).cloned().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotFound, "no such projected file")
        })
    }

    fn now(&self) -> DateTime<Utc> {
        now()
    }
}

// ===========================================================================
// Harness
// ===========================================================================

/// Run the whole emission path into a buffer and DECODE it with the
/// controller's own decoder.
struct Run {
    code: ExitCode,
    stdout: String,
    relay: Result<CheckRelay, logweir_core::check_contract::FrameError>,
}

impl Run {
    fn result(&self) -> CheckResult {
        self.relay
            .as_ref()
            .expect("the relay decodes")
            .result()
            .expect("an emission always carries a result stream")
            .expect("the result document parses")
    }

    fn row(&self, id: CheckId) -> logweir_core::check_contract::CheckOutcome {
        self.result()
            .checks
            .iter()
            .find(|c| c.id == id)
            .unwrap_or_else(|| panic!("no `{id}` row in {:?}", ids(&self.result())))
            .clone()
    }

    fn has(&self, id: CheckId) -> bool {
        self.result().checks.iter().any(|c| c.id == id)
    }

    /// The raw stdout PLUS every decoded stream's bytes.
    ///
    /// A disclosure assertion over `stdout` alone is not one: every stream
    /// travels as base64 part frames, so a credential inside the result
    /// document is invisible to a plaintext scan of the log. The mutant round
    /// for M4 found exactly that — the planted secret was redacted, the
    /// bypass was planted, and the test still passed, because it was reading
    /// the wrapper and not the payload.
    ///
    /// The decoded bytes here are the RUNNER's, before
    /// `CheckRelay::result()`'s `sanitise` — which is the point: the claim is
    /// that the runner did not print a credential, not that the controller
    /// would have scrubbed one.
    fn everything(&self) -> String {
        let mut all = self.stdout.clone();
        if let Ok(relay) = &self.relay {
            for bytes in relay.streams.values() {
                all.push('\n');
                all.push_str(&String::from_utf8_lossy(bytes));
            }
        }
        all
    }

    fn topic_frame_count(&self) -> usize {
        self.stdout
            .lines()
            .filter(|l| l.starts_with("logweir-check-topic="))
            .count()
    }
}

fn ids(result: &CheckResult) -> Vec<&'static str> {
    result.checks.iter().map(|c| c.id.as_str()).collect()
}

fn drive(mounted: &Mounted, wiring: &dyn Wiring) -> Run {
    let mut buf: Vec<u8> = Vec::new();
    let code = check::execute_with(&mounted.loaded, &mut buf, wiring);
    let stdout = String::from_utf8(buf).expect("frames are UTF-8");
    let mut decoder = Decoder::new();
    let mut error = None;
    for line in stdout.lines() {
        if let Err(e) = decoder.push_line(line) {
            error = Some(e);
            break;
        }
    }
    let relay = match error {
        Some(e) => Err(e),
        None => decoder.finish(&FrameExpectations {
            plan_sha256: mounted.sha256.clone(),
            subject_uid: SUBJECT_UID.to_string(),
        }),
    };
    Run {
        code,
        stdout,
        relay,
    }
}

/// `load` over an explicit environment and a plan written to a tempdir — the
/// startup order with no process state.
fn load_with(bytes: &[u8], env: &[(&str, &str)], version: u32) -> Result<Loaded, check::Refusal> {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("check-plan.json");
    std::fs::write(&path, bytes).expect("write the plan");
    let map: BTreeMap<String, String> = env
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect();
    check::load(
        &CheckRunArgs {
            plan: path,
            check_contract_version: version,
        },
        &|k| map.get(k).cloned(),
    )
}

fn good_env(sha: &str) -> Vec<(&'static str, String)> {
    vec![
        (check::CONTRACT_VERSION_ENV, "1".to_string()),
        (check::PLAN_SHA256_ENV, sha.to_string()),
        (check::SUBJECT_UID_ENV, SUBJECT_UID.to_string()),
    ]
}

fn as_pairs<'a>(v: &'a [(&'static str, String)]) -> Vec<(&'static str, &'a str)> {
    v.iter().map(|(k, x)| (*k, x.as_str())).collect()
}

// ===========================================================================
// 1. The startup order (D2 §4.2 steps 1-4): exit 3, no frames, no network
// ===========================================================================

/// **M2.** The digest the Job pinned decides which bytes may run.
#[test]
fn a_wrong_plan_digest_is_refused_with_no_frames() {
    let m = mount(&inventory_plan(100, 1 << 20));
    let env = good_env(&logweir_core::ids::sha256_prefixed(b"some other plan"));
    let err = load_with(&m.bytes, &as_pairs(&env), 1).expect_err("a wrong digest is refused");
    assert!(
        err.detail.contains("sha256"),
        "the refusal names the digest: {}",
        err.detail
    );
    assert_eq!(err.code(), CheckCode::CheckContractMismatch);
}

#[test]
fn a_wrong_contract_version_in_argv_is_refused() {
    let m = mount(&inventory_plan(100, 1 << 20));
    let env = good_env(&m.sha256);
    let err = load_with(&m.bytes, &as_pairs(&env), 2).expect_err("version 2 is refused");
    assert!(
        err.detail.contains("--check-contract-version 2"),
        "{}",
        err.detail
    );
}

#[test]
fn a_wrong_contract_version_in_the_environment_is_refused() {
    let m = mount(&inventory_plan(100, 1 << 20));
    let mut env = good_env(&m.sha256);
    env[0].1 = "7".to_string();
    let err =
        load_with(&m.bytes, &as_pairs(&env), 1).expect_err("a half-upgraded rollout is refused");
    assert!(
        err.detail.contains(check::CONTRACT_VERSION_ENV),
        "{}",
        err.detail
    );
}

#[test]
fn a_missing_contract_version_env_is_refused() {
    let m = mount(&inventory_plan(100, 1 << 20));
    let env = good_env(&m.sha256);
    let without: Vec<(&str, &str)> = as_pairs(&env).into_iter().skip(1).collect();
    let err = load_with(&m.bytes, &without, 1).expect_err("an absent version variable is refused");
    assert!(err.detail.contains("is not set"), "{}", err.detail);
}

#[test]
fn a_subject_uid_mismatch_is_refused() {
    let m = mount(&inventory_plan(100, 1 << 20));
    let mut env = good_env(&m.sha256);
    env[2].1 = "00000000-0000-0000-0000-000000000000".to_string();
    let err = load_with(&m.bytes, &as_pairs(&env), 1).expect_err("a swapped subject is refused");
    assert!(err.detail.contains("subject uid"), "{}", err.detail);
}

#[test]
fn a_missing_subject_uid_env_is_refused() {
    let m = mount(&inventory_plan(100, 1 << 20));
    let mut env = good_env(&m.sha256);
    env[2].1 = String::new();
    let err = load_with(&m.bytes, &as_pairs(&env), 1).expect_err("an empty subject is refused");
    assert!(
        err.detail.contains(check::SUBJECT_UID_ENV),
        "{}",
        err.detail
    );
}

#[test]
fn a_malformed_plan_sha_env_is_refused() {
    let m = mount(&inventory_plan(100, 1 << 20));
    let mut env = good_env(&m.sha256);
    env[1].1 = "deadbeef".to_string();
    let err = load_with(&m.bytes, &as_pairs(&env), 1).expect_err("a bare hex digest is refused");
    assert!(err.detail.contains("sha256:"), "{}", err.detail);
}

/// An unknown kind is a PARSE error, which D2 §4.2 step 4 wants: the request
/// enum is externally tagged, so a tag this build does not know names a variant
/// serde cannot construct.
///
/// **The example used to be `{"catalogSync": …}`**, which has landed as the
/// sixth kind. It is now `retentionSweep` — D3 §6's, and not this build's — so
/// the row still asserts what it was written to assert. The second half of the
/// row is the one that would have gone quiet otherwise: a KNOWN tag carrying
/// the WRONG body is refused too, because every variant keeps its own
/// `deny_unknown_fields`.
#[test]
fn an_unknown_plan_kind_is_refused() {
    let m = mount(&inventory_plan(100, 1 << 20));
    let mut doc: serde_json::Value = serde_json::from_slice(&m.bytes).unwrap();
    let inner = doc["request"]["topicInventory"].take();
    doc["request"] = serde_json::json!({ "retentionSweep": inner.clone() });
    let bytes = serde_json::to_vec(&doc).unwrap();
    let sha = logweir_core::ids::sha256_prefixed(&bytes);
    let env = good_env(&sha);
    let err = load_with(&bytes, &as_pairs(&env), 1).expect_err("an unknown kind is refused");
    assert!(err.detail.contains("does not parse"), "{}", err.detail);

    // A known tag with a body that is not its own is refused just as hard.
    doc["request"] = serde_json::json!({ "catalogSync": inner });
    let bytes = serde_json::to_vec(&doc).unwrap();
    let sha = logweir_core::ids::sha256_prefixed(&bytes);
    let env = good_env(&sha);
    let err = load_with(&bytes, &as_pairs(&env), 1).expect_err("a wrong body is refused");
    assert!(err.detail.contains("does not parse"), "{}", err.detail);
}

#[test]
fn an_unreadable_plan_file_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("nope.json");
    let env: BTreeMap<String, String> = good_env("sha256:{}")
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();
    let mut env = env;
    env.insert(
        check::PLAN_SHA256_ENV.to_string(),
        logweir_core::ids::sha256_prefixed(b""),
    );
    let err = check::load(
        &CheckRunArgs {
            plan: missing,
            check_contract_version: 1,
        },
        &|k| env.get(k).cloned(),
    )
    .expect_err("a plan that is not mounted is refused");
    assert!(err.detail.contains("could not be read"), "{}", err.detail);
}

/// A runner-side bound the pure contract does not state: one stream carries one
/// ordered run of parts, so two objects may not share one.
#[test]
fn two_evidence_objects_on_one_stream_are_refused() {
    let plan = plan_of(CheckRequest::EvidenceFetch(EvidenceFetchRequest {
        destination: destination(),
        objects: vec![
            EvidenceObjectRequest {
                role: DestinationRole::EvidenceRead,
                key: "logweir/backups/a/receipt.json".to_string(),
                max_bytes: 1024,
                stream: Stream::EvidencePayload,
            },
            EvidenceObjectRequest {
                role: DestinationRole::EvidenceRead,
                key: "logweir/backups/b/receipt.json".to_string(),
                max_bytes: 1024,
                stream: Stream::EvidencePayload,
            },
        ],
    }));
    let m = mount(&plan);
    let env = good_env(&m.sha256);
    let err = load_with(&m.bytes, &as_pairs(&env), 1).expect_err("a duplicate stream is refused");
    assert!(err.detail.contains("evidence.payload"), "{}", err.detail);
}

/// The refusal line is EXACT BYTES, and it is the key
/// `weirkeeper::check::relay::refusal_reason` reads.
#[test]
fn the_refusal_line_is_the_exact_bytes() {
    let mut out: Vec<u8> = Vec::new();
    check::print_refusal_to(&mut out).unwrap();
    assert_eq!(
        String::from_utf8(out).unwrap(),
        "refusal-reason=CheckContractMismatch\n"
    );
}

/// The prefix this runner prints and the prefix the controller matches on are
/// ONE string. The controller crate is not a dependency of this test binary, so
/// the assertion reads its source.
#[test]
fn the_refusal_key_is_the_controllers_key() {
    let src =
        std::fs::read_to_string(repo_root().join("crates/weirkeeper/src/controllers/backup.rs"))
            .expect("the controller source is readable");
    let wanted = format!(
        "pub const REFUSAL_REASON_PREFIX: &str = \"{}\";",
        check::REFUSAL_REASON_PREFIX
    );
    assert!(
        src.contains(&wanted),
        "the controller no longer spells the refusal key as `{}`",
        check::REFUSAL_REASON_PREFIX
    );
}

/// **M1 and MR-B, the structural half.** Nothing D2 §4.2's startup order can
/// reach builds a client.
///
/// # Why this is a MODULE rule and not a two-function rule
///
/// The first version brace-counted the bodies of `load` and `runner_bounds`
/// and forbade the constructors plus this crate's wrappers around them. The
/// reviewer's mutant **MR-B** walked straight past it: a new helper
/// `fn prewarm(bytes: &[u8])` in `check/mod.rs` that dials, called from `load`
/// before `parse_and_verify`, is invisible to a scan of two bodies — and
/// `check/mod.rs` is not a `no_network_in_unit_tests` construction site
/// either, because it names `kafka::dial(` and not `KafkaInventory::connect(`.
/// A plan `ConfigMap` swapped under a running Job would have reached a broker
/// with the projected SASL password before the digest refused it, which is the
/// one property the startup order exists for.
///
/// That was the same defect one level up as the one the M1 round already fixed
/// once: a rule about NAMES where a rule about REACHABILITY was needed. So the
/// rule is now about the module:
///
/// * **every function in `check/mod.rs` except [`EXECUTION_FUNCTIONS`] is
///   forbidden to name a dialling or handle-building token** — so a helper
///   cannot be added at all, wherever it is called from;
/// * **`load` may call only functions on a named allowlist**, so a future
///   helper is a deliberate, reviewable addition to that list rather than an
///   edit nobody sees;
/// * and `run` still calls `load` before `execute`, which still takes a
///   `&Loaded` that only `load` produces.
///
/// The two execution functions are exempt because reaching a kind IS their job,
/// and they are reachable only from a verified plan: `execute_with` takes a
/// `&Loaded`, and the third clause is what keeps that true.
#[test]
fn the_startup_path_builds_no_client() {
    let src = std::fs::read_to_string(repo_root().join("crates/logweir/src/check/mod.rs"))
        .expect("the startup module is readable");
    let code: String = src
        .lines()
        .filter(|l| {
            let t = l.trim_start();
            !(t.starts_with("//") || t.starts_with("///") || t.starts_with("//!"))
        })
        .collect::<Vec<_>>()
        .join("\n");

    let bodies = fn_bodies(&code);
    for wanted in ["pub fn load(", "pub fn runner_bounds(", "pub fn run("] {
        assert!(
            bodies.iter().any(|(sig, _)| sig.starts_with(wanted)),
            "`{wanted}` is no longer in `check/mod.rs`; this guard is scanning nothing"
        );
    }

    // (a) NO FUNCTION IN THE MODULE dials or builds a handle, except the two
    //     whose job is to run a kind from a verified plan.
    for (sig, body) in &bodies {
        if EXECUTION_FUNCTIONS.iter().any(|f| sig.starts_with(f)) {
            continue;
        }
        for token in CLIENT_TOKENS {
            assert!(
                !body.contains(token),
                "`{sig}` is in `check/mod.rs`, which holds D2 §4.2's startup order, and names \
                 `{token}`. No client may be built and no kind may run outside {EXECUTION_FUNCTIONS:?} \
                 — a helper that dials is reachable from `load` whatever it is called from \
                 (reviewer mutant MR-B)"
            );
        }
    }

    // (b) EVERY FUNCTION REACHABLE BEFORE STEP 5 CALLS ONLY WHAT IT IS ALLOWED
    //     TO. Clause (a) stops a dialling helper being added to this module;
    //     this stops one being reached in another module.
    //
    //     It is a CLOSURE and not a check of `load` alone, which is reviewer
    //     finding R-F1: `runner_bounds` is step 4 too and is itself on the
    //     allowlist, so a `kafka::prewarm(` reached from THERE satisfied
    //     clause (a) — the token is not a client constructor — and clause (b)
    //     never looked at it. Mutant MR-B3 dialled before the digest check
    //     with the whole suite green.
    //
    //     The walk starts at the three startup roots and follows every callee
    //     that is DEFINED in this module, so a new startup helper is pulled in
    //     automatically. It stops at the execution functions, which are the
    //     step-5 boundary by definition: `execute_with` takes a `&Loaded`,
    //     which only `load` produces.
    let defined: BTreeSet<String> = bodies.iter().filter_map(|(sig, _)| fn_name(sig)).collect();
    let execution: BTreeSet<String> = bodies
        .iter()
        .filter(|(sig, _)| EXECUTION_FUNCTIONS.iter().any(|f| sig.starts_with(f)))
        .filter_map(|(sig, _)| fn_name(sig))
        .collect();
    let mut seen: BTreeSet<String> = BTreeSet::new();
    let mut queue: Vec<String> = STARTUP_ROOTS
        .iter()
        .map(|r| {
            let n = fn_name(r).unwrap_or_else(|| panic!("`{r}` is not a signature"));
            assert!(
                defined.contains(&n),
                "`{r}` is no longer in `check/mod.rs`; this guard is scanning nothing"
            );
            n
        })
        .collect();
    while let Some(name) = queue.pop() {
        if !seen.insert(name.clone()) {
            continue;
        }
        // EVERY body with that name, not the first: `new`, `code` and `of` are
        // each defined more than once in this module, and following all of
        // them is the conservative reading — a guard that picked one could
        // walk past the other.
        for (sig, body) in bodies
            .iter()
            .filter(|(s, _)| fn_name(s).as_deref() == Some(&name))
        {
            for call in calls_in(body) {
                // A call to a function DEFINED here is followed; clause (a)
                // already forbids it a client token.
                let leaf = call.rsplit("::").next().unwrap_or(&call).to_string();
                if execution.contains(&leaf) {
                    continue;
                }
                if defined.contains(&leaf) {
                    queue.push(leaf);
                    continue;
                }
                assert!(
                STARTUP_MAY_CALL.contains(&call.as_str())
                    || STARTUP_HELPERS_MAY_CALL.contains(&call.as_str()),
                "`{sig}` is reachable before D2 §4.2 step 5 and calls `{call}`, which is not on \
                 the startup allowlist. Steps 1-4 open nothing, so a new callee there is a \
                 decision: add it to STARTUP_MAY_CALL with a reason, or move the call after \
                 the plan verifies. Allowed: {STARTUP_MAY_CALL:?} plus \
                 {STARTUP_HELPERS_MAY_CALL:?}"
            );
            }
        }
    }
    // The closure really walked past its roots — a walk that stopped at `load`
    // would assert nothing about `runner_bounds`, which is R-F1 exactly.
    assert!(
        seen.contains("runner_bounds"),
        "the startup closure did not reach `runner_bounds`: {seen:?}"
    );

    // (c) `run` reaches a kind only after `load` returned Ok, through a
    //     `&Loaded` that only `load` produces.
    let (_, run_body) = bodies
        .iter()
        .find(|(sig, _)| sig.starts_with("pub fn run("))
        .unwrap();
    let load_at = run_body.find("load(").expect("`run` calls `load`");
    let exec_at = run_body.find("execute(").expect("`run` calls `execute`");
    assert!(
        load_at < exec_at,
        "`run` must verify the plan before it executes a kind"
    );
    assert!(
        code.contains("pub fn execute_with<W: Write>(loaded: &Loaded"),
        "a kind must only be reachable from a `&Loaded`, which only `load` produces"
    );
}

/// The two functions in `check/mod.rs` whose job is to run a kind. Everything
/// else in that module is startup, and startup opens nothing.
const EXECUTION_FUNCTIONS: [&str; 2] = ["pub fn execute<", "pub fn execute_with<"];

/// Every token that means "a client or a store handle is built or reached
/// here" — the constructors AND this crate's own wrappers around them, because
/// a wrapper is what mutant MR-B used.
const CLIENT_TOKENS: [&str; 13] = [
    "KafkaInventory::connect(",
    "RdKafkaReader::connect(",
    "Store::read_only_with(",
    "Store::from_url_with(",
    "Store::from_url(",
    "Store::read_only_from_url(",
    "AuthConfig::from_spec",
    "kafka::dial(",
    "store::open_read(",
    "store::open_evidence_write(",
    "kinds::Live",
    "run_kind_with(",
    "Wiring",
];

/// The three functions D2 §4.2's startup order is made of. The closure starts
/// here and follows every callee defined in the same module.
///
/// `run` is a root because it is what calls `load`, and the clause that it
/// calls `load` BEFORE `execute` is (c) below.
const STARTUP_ROOTS: [&str; 3] = ["pub fn run(", "pub fn load(", "pub fn runner_bounds("];

/// Everything a function reachable before step 5 may call.
///
/// A LIST, so a new callee there is a reviewable decision rather than an edit
/// nobody sees. That is the half of the fix for reviewer mutants MR-B / MR-B2 /
/// MR-B3 that clause (a) cannot make on its own: (a) stops a dialling helper
/// being ADDED to `check/mod.rs`, and this stops one being REACHED in any other
/// module.
///
/// Only callees DEFINED OUTSIDE this module appear here — one defined inside it
/// is followed by the walk instead, and clause (a) has already forbidden it a
/// client token. `format!` does not appear because a macro's `!` ends the
/// identifier run, which is a property of the tokeniser and not an exemption.
/// `every_startup_allowlist_entry_is_still_earned` fails on an entry nothing
/// calls, so the list cannot rot into a comment.
const STARTUP_MAY_CALL: [&str; 25] = [
    // -- control flow: Result/Option constructors and combinators ---------
    "Ok",
    "Err",
    "Some",
    "map_err",
    "ok",
    "into",
    // -- the injected environment reader, and the shipped one it stands for
    "env",
    "std::env::var",
    // -- the strings that shape a refusal message --------------------------
    "to_string",
    "trim",
    "unwrap_or_default",
    "is_empty",
    "display", // `Path::display`, for the unreadable-plan message
    "kind",    // `io::Error::kind` — the KIND, never adopter bytes
    // -- the pure contract: step 3's digest SHAPE, and steps 3+4 as ONE call
    "logweir_core::check_contract::is_sha256_prefixed",
    "CheckPlan::parse_and_verify",
    // -- step 2: the bytes, once ------------------------------------------
    "std::fs::read",
    // -- `runner_bounds`' walk over the request ----------------------------
    "logweir_core::check_contract::CheckRequest::EvidenceFetch",
    "std::collections::BTreeSet::new",
    "iter",
    "enumerate",
    "contains",
    "insert",
    "as_str",
    // -- the redactor for `run`'s single stderr warn line ------------------
    "logweir_core::check_contract::redact",
];

/// Callees the walk reaches through a function DEFINED in `check/mod.rs` that
/// is itself pure — the stderr subscriber, the deadline's clock and the stdout
/// handle a refusal is printed through.
///
/// SEPARATE from [`STARTUP_MAY_CALL`] because they are a different claim: those
/// are what STARTUP calls, these are what startup's own helpers call. Splitting
/// them keeps the first list readable as "what steps 1-4 do".
///
/// None of them opens a socket or a bucket. `std::io::stdout` is the refusal
/// line's writer, and D2 §4.2 requires that line on exit 3.
const STARTUP_HELPERS_MAY_CALL: [&str; 9] = [
    "Instant::now",
    "Duration::from_secs",
    "std::io::stdout",
    "lock",
    "tracing_subscriber::fmt",
    "json",
    "with_writer",
    "with_env_filter",
    "try_init",
];

/// A signature line's function NAME.
fn fn_name(sig: &str) -> Option<String> {
    let after = sig.split_once("fn ")?.1;
    let name: String = after
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .collect();
    (!name.is_empty()).then_some(name)
}

/// Every `name(` called in a function body, as a bare identifier path.
///
/// A TOKENISER, not a parser: it takes the identifier run immediately before
/// each `(` that is not a definition, which is exactly what "which functions
/// does this body call" needs and is why the allowlist above can be a list of
/// names. Method calls arrive as their final segment (`foo.trim()` is `trim`),
/// which is what makes the list readable.
fn calls_in(body: &str) -> Vec<String> {
    let bytes: Vec<char> = body.chars().collect();
    let mut out: Vec<String> = Vec::new();
    for (i, c) in bytes.iter().enumerate() {
        if *c != '(' {
            continue;
        }
        let mut j = i;
        while j > 0 {
            let p = bytes[j - 1];
            if p.is_alphanumeric() || p == '_' || p == ':' {
                j -= 1;
            } else {
                break;
            }
        }
        if j == i {
            continue;
        }
        let name: String = bytes[j..i].iter().collect();
        let name = name.trim_start_matches(':').to_string();
        if name.is_empty() || name.chars().next().is_some_and(char::is_numeric) {
            continue;
        }
        // Keywords that take a parenthesised expression, and the macros that
        // are not calls in the sense this asks about.
        if matches!(name.as_str(), "if" | "match" | "while" | "for" | "return") {
            continue;
        }
        if !out.contains(&name) {
            out.push(name);
        }
    }
    out
}

/// Neither startup allowlist may hold an entry nothing calls.
///
/// The repository's own idiom (`no_network_in_unit_tests::
/// every_allow_list_entry_is_still_earned`): a list that outlives the call it
/// was written for stops describing the code and starts hiding it, and the
/// next reader cannot tell which entries are load-bearing.
#[test]
fn every_startup_allowlist_entry_is_still_earned() {
    let src = std::fs::read_to_string(repo_root().join("crates/logweir/src/check/mod.rs"))
        .expect("the startup module is readable");
    let code: String = src
        .lines()
        .filter(|l| {
            let t = l.trim_start();
            !(t.starts_with("//") || t.starts_with("///") || t.starts_with("//!"))
        })
        .collect::<Vec<_>>()
        .join("\n");
    let called: BTreeSet<String> = fn_bodies(&code)
        .iter()
        .flat_map(|(_, b)| calls_in(b))
        .collect();
    let mut stale: Vec<&str> = Vec::new();
    for entry in STARTUP_MAY_CALL
        .iter()
        .chain(STARTUP_HELPERS_MAY_CALL.iter())
    {
        if !called.contains(*entry) {
            stale.push(entry);
        }
    }
    assert!(
        stale.is_empty(),
        "these startup-allowlist entries name nothing `check/mod.rs` calls any more; remove \
         them in the commit that removed the call: {stale:?}"
    );
}

/// The bodies of every `fn` in a source file, keyed by its signature line.
///
/// Brace-counted, and consumed in shape from
/// `crates/logweir/tests/no_network_in_unit_tests.rs::fn_bodies`, whose own
/// header records why a line-based scan cannot answer a question about a body.
fn fn_bodies(src: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mut line_start = 0usize;
    for line in src.lines() {
        let start = line_start;
        line_start += line.len() + 1;
        let t = line.trim_start();
        if !(t.starts_with("fn ") || t.starts_with("pub fn ") || t.starts_with("pub(crate) fn ")) {
            continue;
        }
        let Some(open) = src[start..].find('{').map(|i| start + i) else {
            continue;
        };
        let mut depth = 0usize;
        let mut end = open;
        for (i, c) in src[open..].char_indices() {
            match c {
                '{' => depth += 1,
                '}' => {
                    depth -= 1;
                    if depth == 0 {
                        end = open + i;
                        break;
                    }
                }
                _ => {}
            }
        }
        out.push((t.to_string(), src[open..=end].to_string()));
    }
    out
}

// ===========================================================================
// 2. The frame writer
// ===========================================================================

fn entries(n: usize) -> Vec<TopicEntry> {
    (0..n)
        .map(|i| TopicEntry::new(&format!("orders-{i:04}"), 6))
        .collect()
}

/// **M6.** The relay budget is enforced by the WRITER, independently of what
/// `logweir_kafka::inventory::assemble` handed it.
#[test]
fn the_writer_stops_at_the_relay_budget() {
    let all = entries(50);
    let one = logweir::check::frames::relay_cost(&all[0]);
    let mut buf: Vec<u8> = Vec::new();
    let mut w = FrameWriter::new(&mut buf, one * 3);
    let written = w.write_topics(&all).expect("writing a topic line succeeds");
    assert_eq!(written, 3, "the budget carries exactly three lines");
    let end = w
        .finish("sha256:x", SUBJECT_UID)
        .expect("the end line is written");
    let lines = end
        .topic_lines
        .expect("an inventory declares its topic lines");
    assert_eq!(lines.count, 3);
    assert_eq!(
        lines.sha256,
        logweir_core::check_contract::topic_tsv_sha256(&all[..3]),
        "the end frame declares the digest of the lines that were PRINTED"
    );
    assert_eq!(
        String::from_utf8(buf)
            .unwrap()
            .lines()
            .filter(|l| l.starts_with("logweir-check-topic="))
            .count(),
        3
    );
}

/// The two relay-cost implementations — this crate's writer and
/// `logweir_kafka::inventory`'s assembler — cost a line identically, so the two
/// bounds cannot disagree about when the budget is spent.
#[test]
fn the_two_relay_costs_agree() {
    let mut cases = entries(3);
    let mut internal = TopicEntry::new("__consumer_offsets", 50);
    internal.internal = true;
    cases.push(internal);
    let mut errored = TopicEntry::new("payments", 12);
    errored.expected = true;
    errored.error = Some(CheckCode::TopicAuthorizationFailed);
    cases.push(errored);
    cases.push(TopicEntry::new(&"a".repeat(249), 999_999));
    for e in &cases {
        assert_eq!(
            logweir::check::frames::relay_cost(e),
            logweir_kafka::inventory::relay_cost(e),
            "the writer and the assembler disagree about `{}`",
            e.name
        );
    }
}

/// An inventory that never ran declares NO `topicLines` block; an empty
/// cluster declares one with `count: 0`. PLAT-09.1 requires a user to be able
/// to tell "empty" from "failed".
#[test]
fn an_inventory_that_did_not_run_has_no_topic_lines_block() {
    let m = mount(&inventory_plan(100, 1 << 20));
    let run = drive(
        &m,
        &FakeWiring::default().broker_fails(CheckCode::BrokerUnreachable, "no broker answered"),
    );
    assert_eq!(run.code, ExitCode::Ok, "a failed inventory still ends");
    let relay = run.relay.as_ref().expect("the relay decodes");
    assert!(
        relay.end.topic_lines.is_none(),
        "an inventory that did not run must not declare a topic-line count"
    );
    assert!(run.result().inventory.is_none());
    assert_eq!(
        run.row(CheckId::ConnectionAuthenticated).code,
        CheckCode::BrokerUnreachable
    );
}

#[test]
fn an_empty_cluster_has_a_topic_lines_block_with_count_zero() {
    let m = mount(&inventory_plan(100, 1 << 20));
    let run = drive(&m, &FakeWiring::default().with_probe(FakeProbe::new()));
    let relay = run.relay.as_ref().expect("the relay decodes");
    let lines = relay
        .end
        .topic_lines
        .as_ref()
        .expect("an inventory that ran declares its count");
    assert_eq!(lines.count, 0);
    let inv = run.result().inventory.expect("an inventory block");
    assert_eq!(inv.counts.returned, 0);
    assert_eq!(inv.counts.listed, 0);
}

/// **M3.** A writer that will not take bytes is exit 1 with no end line — not
/// exit 0.
#[test]
fn a_writer_that_refuses_bytes_is_operational_not_ok() {
    struct Broken;
    impl std::io::Write for Broken {
        fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
            Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "the pod's stdout went away",
            ))
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let m = mount(&inventory_plan(100, 1 << 20));
    let code = check::execute_with(
        &m.loaded,
        Broken,
        &FakeWiring::default().with_probe(FakeProbe::new()),
    );
    assert_eq!(
        code,
        ExitCode::Operational,
        "no end line was printed, so this is an operational failure and never a result"
    );
}

/// Every frame the runner writes is inside D2 §4.1's 4,096-byte bound,
/// newline included, so no line can be split by the CRI's partial-line rule.
#[test]
fn every_frame_line_is_within_the_bound() {
    let m = mount(&inventory_plan(5_000, 1 << 22));
    let long = "t".repeat(249);
    let run = drive(
        &m,
        &FakeWiring::default()
            .with_probe(FakeProbe::new().with_topics(&[(long.as_str(), 999_999), ("orders", 6)])),
    );
    for line in run.stdout.lines() {
        assert!(
            line.len() < logweir_core::check_contract::FRAME_MAX_BYTES,
            "a frame line is {} bytes including its newline",
            line.len() + 1
        );
    }
    assert!(run.relay.is_ok());
}

// ===========================================================================
// 3. `topicInventory`
// ===========================================================================

#[test]
fn a_topic_inventory_relays_the_listing_and_its_digest() {
    let m = mount(&inventory_plan(1_000, 1 << 20));
    let probe = FakeProbe::new().with_topics(&[
        ("payments", 12),
        ("orders", 6),
        ("__consumer_offsets", 50),
    ]);
    let run = drive(&m, &FakeWiring::default().with_probe(probe));
    assert_eq!(run.code, ExitCode::Ok);
    let relay = run.relay.as_ref().expect("the relay decodes");
    // Byte-order sorted, internal excluded by default.
    assert_eq!(
        relay
            .topics
            .iter()
            .map(|t| t.name.as_str())
            .collect::<Vec<_>>(),
        vec!["orders", "payments"]
    );
    let inv = run.result().inventory.expect("an inventory block");
    assert_eq!(inv.counts.listed, 3);
    assert_eq!(inv.counts.returned, 2);
    assert_eq!(inv.counts.internal_excluded, 1);
    assert_eq!(inv.cluster_id.as_deref(), Some("M29I2S7FQPyHBEX12Vx7XA"));
    assert_eq!(inv.broker_count, Some(3));
    assert!(!inv.truncated);
    // The digest in the document and the digest in the end frame are the same
    // number over the same lines — which is what the controller checks.
    assert_eq!(
        inv.topics_sha256,
        relay.end.topic_lines.as_ref().unwrap().sha256
    );
    assert_eq!(run.topic_frame_count(), 2);
}

/// The relay bound biting is reported as `truncated: RelayLimit`, and the
/// declared count is the count that was PRINTED.
#[test]
fn a_relay_budget_truncation_is_declared_in_the_end_frame() {
    let probe = FakeProbe::new();
    {
        let mut s = probe.state.lock().unwrap();
        for i in 0..200 {
            s.listing.topics.push(ListedTopic {
                name: format!("orders-{i:04}"),
                partitions: 6,
                error: None,
            });
        }
    }
    let one = logweir::check::frames::relay_cost(&TopicEntry::new("orders-0000", 6));
    let m = mount(&inventory_plan(1_000, one * 5));
    let run = drive(&m, &FakeWiring::default().with_probe(probe));
    let relay = run.relay.as_ref().expect("the relay decodes");
    let inv = run.result().inventory.expect("an inventory block");
    assert!(inv.truncated);
    assert_eq!(
        inv.truncation_reason,
        Some(logweir_core::check_contract::TruncationReason::RelayLimit)
    );
    assert_eq!(inv.counts.returned, 5);
    assert_eq!(relay.end.topic_lines.as_ref().unwrap().count, 5);
    assert_eq!(run.topic_frame_count(), 5);
    assert_eq!(
        inv.topics_sha256,
        relay.end.topic_lines.as_ref().unwrap().sha256
    );
}

/// A listing this principal is only partly authorized for carries the D2 §5.4
/// signal, and the runner NEVER decides `limited` itself.
#[test]
fn an_authorization_signal_is_relayed_and_no_verdict_is() {
    let probe = FakeProbe::new();
    probe
        .state
        .lock()
        .unwrap()
        .listing
        .topics
        .push(ListedTopic {
            name: "secrets".to_string(),
            partitions: 0,
            error: Some(CheckCode::TopicAuthorizationFailed),
        });
    let m = mount(&inventory_plan(1_000, 1 << 20));
    let run = drive(&m, &FakeWiring::default().with_probe(probe));
    let inv = run.result().inventory.expect("an inventory block");
    assert!(inv.topic_authorization_error_in_listing);
    assert_eq!(inv.counts.errored, 1);
    assert!(
        !run.stdout.contains("attestedComplete") && !run.stdout.contains("\"limited\""),
        "a visibility VERDICT is the controller's (D-SEAMS S3); the runner relays signals"
    );
}

/// D2 §5.2 step 5: an expected topic the listing did not show gets a targeted
/// request, and `TOPIC_AUTHORIZATION_FAILED` is reported as a VISIBILITY fact,
/// never as absence.
#[test]
fn an_expected_topic_that_is_hidden_is_not_authorized_and_not_absent() {
    let mut plan = inventory_plan(1_000, 1 << 20);
    if let CheckRequest::TopicInventory(r) = &mut plan.request {
        r.expected_topics = vec!["hidden".to_string(), "gone".to_string()];
    }
    let m = mount(&plan);
    let probe = FakeProbe::new()
        .with_presence("hidden", TopicPresence::NotAuthorized)
        .with_presence("gone", TopicPresence::NotFound);
    let run = drive(&m, &FakeWiring::default().with_probe(probe));
    let inv = run.result().inventory.expect("an inventory block");
    assert_eq!(inv.expected.requested, 2);
    assert_eq!(inv.expected.not_authorized, 1);
    assert_eq!(inv.expected.not_found, 1);
    assert!(inv.expected_results.iter().any(|r| r.name == "hidden"
        && r.state == logweir_core::check_contract::ExpectedTopicState::NotAuthorized));
}

// ===========================================================================
// 4. `destinationAccess`
// ===========================================================================

/// A store's refusal in the words `object_store` 0.14.1 hands over: its
/// request line, its status line, and the store's error document (captured
/// with the real client; `crates/logweir-store/tests/options.rs` holds the
/// shape to the client itself).
///
/// The classifier reads an answer's own status and code and searches no text
/// (PROD-01.2 review, M1: the text echoes the bucket and the key), so a row
/// offers a refusal in the shape the product meets. The rows here used to
/// pass on strings no store sends, with the code anywhere in them.
fn s3_refusal(status: &str, code: &str) -> String {
    format!(
        "Generic S3 error: Error performing GET https://s3.example.com/lw-archive/k in 3ms - \
         Server returned non-2xx status code: {status}: <Error><Code>{code}</Code></Error>"
    )
}

fn access_plan(roles: Vec<DestinationRole>, write_probe: bool) -> CheckPlan {
    plan_of(CheckRequest::DestinationAccess(DestinationAccessRequest {
        destination: destination(),
        roles,
        write_probe,
        evidence_write: None,
        evidence_read: None,
    }))
}

/// FX-20c: a `destinationAccess` plan that lists a Secret-backed grant emits
/// `destination.credentialBound` beside its role rows. The `want` literal is
/// what `weirkeeper`'s `job_rows` mirror is held against
/// (`the_expected_rows_include_the_binding_row_when_a_grant_is_listed`).
#[test]
fn a_destination_access_check_with_listed_grants_reports_every_row_it_owns() {
    let mut plan = access_plan(
        vec![DestinationRole::ArchiveRead, DestinationRole::ArchiveWrite],
        false,
    );
    let CheckRequest::DestinationAccess(r) = &mut plan.request else {
        unreachable!()
    };
    r.destination.grant_bindings = vec![logweir_core::check_contract::GrantBindingRef {
        role: DestinationRole::ArchiveWrite,
        secret_name: "lwd-primary-archive-write".to_string(),
    }];
    let m = mount(&plan);
    let run = drive(
        &m,
        &FakeWiring::default().with_role(DestinationRole::ArchiveRead, FakeObjects::new()),
    );
    let got: BTreeSet<&str> = ids(&run.result()).into_iter().collect();
    let want: BTreeSet<&str> = [
        "runner.contract",
        "destination.credentialBound",
        "destination.archiveListable",
        "destination.archivePrefixWritable",
    ]
    .into_iter()
    .collect();
    assert_eq!(got, want);
}

/// D2 §4.2's `destinationAccess` row: a denial and a not-found are different
/// answers, and the not-found one is the PASS.
#[test]
fn a_destination_access_check_tells_denial_from_not_found() {
    // Not-found: the backend evaluated the request. That IS the grant.
    let m = mount(&access_plan(vec![DestinationRole::EvidenceRead], false));
    let run = drive(
        &m,
        &FakeWiring::default().with_role(DestinationRole::EvidenceRead, FakeObjects::new()),
    );
    let row = run.row(CheckId::DestinationEvidenceReadable);
    assert_eq!(row.state, CheckState::Ready);
    assert_eq!(row.code, CheckCode::EvidenceReadable);
    assert_eq!(row.gating, Gating::Advisory);

    // Denial: the backend refused before it looked.
    let denied =
        FakeObjects::new().failing_get(Fault::Io(s3_refusal("403 Forbidden", "AccessDenied")));
    let run = drive(
        &m,
        &FakeWiring::default().with_role(DestinationRole::EvidenceRead, denied),
    );
    let row = run.row(CheckId::DestinationEvidenceReadable);
    assert_eq!(row.state, CheckState::NotReady);
    assert_eq!(row.code, CheckCode::AccessDenied);
    assert!(
        !row.remedy.is_empty(),
        "PLAT-03.1: every failure names a remedy"
    );
}

#[test]
fn an_archive_list_denial_is_blocking_and_a_wrong_key_is_invalid_credentials() {
    let m = mount(&access_plan(vec![DestinationRole::ArchiveRead], false));
    let run = drive(
        &m,
        &FakeWiring::default().with_role(
            DestinationRole::ArchiveRead,
            FakeObjects::new().failing_list(Fault::Io(s3_refusal(
                "403 Forbidden",
                "SignatureDoesNotMatch",
            ))),
        ),
    );
    let row = run.row(CheckId::DestinationArchiveListable);
    assert_eq!(row.code, CheckCode::InvalidCredentials);
    assert_eq!(row.gating, Gating::Blocking);
    assert_eq!(
        logweir_core::check_contract::aggregate(&run.result().checks),
        logweir_core::check_contract::OverallState::NotReady
    );
}

/// A destination whose prefix simply holds nothing is LISTABLE. Reading
/// emptiness as a denial is the `NotFound`-versus-`Io` defect.
#[test]
fn an_empty_archive_prefix_is_listable() {
    let m = mount(&access_plan(vec![DestinationRole::ArchiveRead], false));
    let run = drive(
        &m,
        &FakeWiring::default().with_role(DestinationRole::ArchiveRead, FakeObjects::new()),
    );
    assert_eq!(
        run.row(CheckId::DestinationArchiveListable).code,
        CheckCode::ArchiveListable
    );
}

/// The archive-WRITE grant is never probed: Global Constraint 6 gives a check
/// one writable key and it is under the evidence root.
#[test]
fn an_archive_write_role_is_execution_only() {
    let m = mount(&access_plan(vec![DestinationRole::ArchiveWrite], true));
    let objects = FakeObjects::new();
    let run = drive(
        &m,
        &FakeWiring::default()
            .with_role(DestinationRole::ArchiveWrite, objects.clone())
            .with_writer(objects.clone()),
    );
    let row = run.row(CheckId::DestinationArchivePrefixWritable);
    assert_eq!(row.gating, Gating::ExecutionOnly);
    assert_eq!(row.state, CheckState::Unknown);
    assert!(objects.puts().is_empty(), "a check wrote into the archive");
}

/// **M5.** The marker is create-only, and "already there" is the grant.
#[test]
fn a_marker_that_is_already_present_is_write_authorised() {
    let m = mount(&access_plan(vec![DestinationRole::EvidenceWrite], true));
    let key = format!("logweir/readiness/{DEST_UID}.json");

    let fresh = FakeObjects::new();
    let run = drive(&m, &FakeWiring::default().with_writer(fresh.clone()));
    let row = run.row(CheckId::DestinationEvidenceWritable);
    assert_eq!(row.state, CheckState::Ready);
    assert_eq!(row.code, CheckCode::MarkerWritten);
    assert_eq!(
        fresh
            .puts()
            .iter()
            .map(|(k, _)| k.clone())
            .collect::<Vec<_>>(),
        vec![key.clone()],
        "the ONLY key a check may write is the readiness marker"
    );

    let present = FakeObjects::new().with_object(&key, b"{}");
    let run = drive(&m, &FakeWiring::default().with_writer(present.clone()));
    let row = run.row(CheckId::DestinationEvidenceWritable);
    assert_eq!(
        row.state,
        CheckState::Ready,
        "S3 and MinIO authorise a PUT before they evaluate the precondition, so a 412 proves \
         the grant (D2 §4.2 [VERIFY U7])"
    );
    assert_eq!(row.code, CheckCode::MarkerAlreadyPresent);
    assert!(present.puts().is_empty(), "the object was not overwritten");
}

/// Without a configured probe the write grant is not guessed at.
#[test]
fn an_evidence_write_role_without_a_probe_is_execution_only() {
    let m = mount(&access_plan(vec![DestinationRole::EvidenceWrite], false));
    let writer = FakeObjects::new();
    let run = drive(&m, &FakeWiring::default().with_writer(writer.clone()));
    let row = run.row(CheckId::DestinationEvidenceWritable);
    assert_eq!(row.code, CheckCode::WriteNotProbed);
    assert_eq!(row.gating, Gating::ExecutionOnly);
    assert_eq!(row.state, CheckState::Unknown);
    assert!(writer.puts().is_empty());
}

#[test]
fn a_denied_marker_put_is_reported_with_the_store_code() {
    let m = mount(&access_plan(vec![DestinationRole::EvidenceWrite], true));
    let run = drive(
        &m,
        &FakeWiring::default().with_writer(
            FakeObjects::new().failing_put(Fault::Io(s3_refusal("403 Forbidden", "AccessDenied"))),
        ),
    );
    let row = run.row(CheckId::DestinationEvidenceWritable);
    assert_eq!(row.code, CheckCode::AccessDenied);
    assert_eq!(row.state, CheckState::NotReady);
}

// ===========================================================================
// 4-bis. One row, one principal (PREFLIGHT-EVIDENCEWRITABLE-WRONG-PRINCIPAL)
// ===========================================================================

/// The Secret a separated `evidenceWrite` grant names in these rows.
const EVIDENCE_SECRET: &str = "evidence-writer";

fn separated_access_plan(grant: Option<GrantRef>) -> CheckPlan {
    plan_of(CheckRequest::DestinationAccess(DestinationAccessRequest {
        destination: destination(),
        roles: vec![DestinationRole::ArchiveRead, DestinationRole::EvidenceWrite],
        write_probe: true,
        evidence_write: grant,
        evidence_read: None,
    }))
}

fn access_denied() -> Fault {
    Fault::Io(s3_refusal("403 Forbidden", "AccessDenied"))
}

/// **The defect's own shape.** A destination whose ARCHIVE principal may write
/// under `logweir/` and whose EVIDENCE-WRITE principal may not: the row must be
/// red, it must be red about the evidence-write principal, and the archive
/// principal must not have written anything.
///
/// MUTANT RP-1 (`access.rs`): hand `evidence_writer` `None` instead of
/// `probe.evidence_write`. The destination grant's handle then writes the
/// marker, the row reads `MarkerWritten`, and this test fails on the first
/// assertion.
#[test]
fn a_separated_evidence_write_principal_that_cannot_write_is_red_for_that_principal() {
    let grant = GrantRef::static_secret(EVIDENCE_SECRET);
    let m = mount(&separated_access_plan(Some(grant.clone())));
    let archive_principal = FakeObjects::new();
    let evidence_principal = FakeObjects::new().failing_put(access_denied());
    let wiring = FakeWiring::default()
        .with_role(DestinationRole::ArchiveRead, FakeObjects::new())
        .with_writer(archive_principal.clone())
        .with_evidence_principal(evidence_principal.clone());
    let run = drive(&m, &wiring);

    let row = run.row(CheckId::DestinationEvidenceWritable);
    assert_eq!(
        (row.state, row.code),
        (CheckState::NotReady, CheckCode::AccessDenied),
        "the evidence-write principal cannot write, so the row is red: {row:?}"
    );
    assert_eq!(row.gating, Gating::Blocking);
    assert_eq!(
        row.facts.get("grant").map(String::as_str),
        Some("evidenceWrite"),
        "the row says WHICH principal it is about"
    );
    assert!(
        row.message.contains(EVIDENCE_SECRET),
        "and names it by reference: {}",
        row.message
    );
    assert!(!row.remedy.is_empty());
    assert_eq!(
        wiring.writer_grants(),
        vec![Some(grant)],
        "the marker handle was built for the evidence-write grant, once"
    );
    assert!(
        archive_principal.calls().is_empty(),
        "the archive principal was asked for nothing: {:?}",
        archive_principal.calls()
    );
    // The archive row is about the archive principal and stays green: one row
    // per principal, and neither answers for the other.
    assert_eq!(
        run.row(CheckId::DestinationArchiveListable).state,
        CheckState::Ready
    );
    assert_eq!(
        logweir_core::check_contract::aggregate(&run.result().checks),
        logweir_core::check_contract::OverallState::NotReady
    );
}

/// The mirror image: the archive principal is the one that cannot write under
/// `logweir/`, the evidence-write principal can. The row is green — about the
/// evidence-write principal — and the marker was created by it alone.
#[test]
fn a_separated_evidence_write_principal_that_can_write_is_green_whatever_the_archive_grant_may_do()
{
    let grant = GrantRef::static_secret(EVIDENCE_SECRET);
    let m = mount(&separated_access_plan(Some(grant)));
    let archive_principal = FakeObjects::new().failing_put(access_denied());
    let evidence_principal = FakeObjects::new();
    let run = drive(
        &m,
        &FakeWiring::default()
            .with_role(DestinationRole::ArchiveRead, FakeObjects::new())
            .with_writer(archive_principal.clone())
            .with_evidence_principal(evidence_principal.clone()),
    );
    let row = run.row(CheckId::DestinationEvidenceWritable);
    assert_eq!(
        (row.state, row.code),
        (CheckState::Ready, CheckCode::MarkerWritten)
    );
    assert_eq!(
        row.facts.get("grant").map(String::as_str),
        Some("evidenceWrite")
    );
    assert_eq!(
        evidence_principal
            .puts()
            .iter()
            .map(|(k, _)| k.clone())
            .collect::<Vec<_>>(),
        vec![format!("logweir/readiness/{DEST_UID}.json")]
    );
    assert!(archive_principal.puts().is_empty());
}

/// How many create-only puts of the marker key one probe makes: TWO — the
/// marker, then the conditional-create proof (RECEIPT-DUP) re-puts the same key
/// and must observe `AlreadyExists`. Both are the `evidenceWrite` principal's.
const MARKER_PUTS_PER_PROBE: usize = 2;

/// **Least privilege, asserted.** The evidence-write principal's handle is
/// asked for EXACTLY [`MARKER_PUTS_PER_PROBE`] create-only puts of the marker
/// key: no read, no list,
/// no delete (the `ObjectAccess` seam has no delete at all, and
/// `the_only_write_in_the_check_runner_is_create_only` pins that). D2 §3.11
/// grants `evidenceWrite` `s3:PutObject` (conditional create) and
/// `s3:GetObject` on `logweir/*`; the probe needs the first on
/// `logweir/readiness/*` and nothing else.
///
/// MUTANT RP-2 (`access.rs`): make the evidence-READ arm open its handle with
/// `evidence_writer(dest, probe.evidence_write, …)` — a "reuse the writable
/// handle" shortcut. The evidence principal is then asked for a `get`, and
/// this test fails.
#[test]
fn the_evidence_write_principal_is_asked_for_one_create_only_put_and_nothing_else() {
    let grant = GrantRef::static_secret(EVIDENCE_SECRET);
    let m = mount(&plan_of(CheckRequest::DestinationAccess(
        DestinationAccessRequest {
            destination: destination(),
            roles: vec![
                DestinationRole::ArchiveWrite,
                DestinationRole::ArchiveRead,
                DestinationRole::EvidenceWrite,
                DestinationRole::EvidenceRead,
            ],
            write_probe: true,
            evidence_write: Some(grant),
            evidence_read: None,
        },
    )));
    let evidence_principal = FakeObjects::new();
    let run = drive(
        &m,
        &FakeWiring::default()
            .with_role(DestinationRole::ArchiveRead, FakeObjects::new())
            .with_role(DestinationRole::EvidenceRead, FakeObjects::new())
            .with_writer(FakeObjects::new())
            .with_evidence_principal(evidence_principal.clone()),
    );
    assert_eq!(run.code, ExitCode::Ok);
    assert_eq!(
        evidence_principal.calls(),
        vec![format!("put logweir/readiness/{DEST_UID}.json"); MARKER_PUTS_PER_PROBE],
        "the evidence-write principal is used for create-only puts of the one marker key and \
         for nothing else"
    );
}

/// **Unchanged when the grants are one grant.** No `evidenceWrite` in the
/// plan: the destination grant writes the marker, exactly as every earlier
/// build did, and the row says the principal was the destination's.
#[test]
fn without_a_separate_grant_the_destination_grant_writes_the_marker_as_before() {
    let m = mount(&separated_access_plan(None));
    let destination_principal = FakeObjects::new();
    let wiring = FakeWiring::default()
        .with_role(DestinationRole::ArchiveRead, FakeObjects::new())
        .with_writer(destination_principal.clone())
        .evidence_principal_fails(CheckCode::AccessDenied, "never asked for");
    let run = drive(&m, &wiring);
    let row = run.row(CheckId::DestinationEvidenceWritable);
    assert_eq!(
        (row.state, row.code),
        (CheckState::Ready, CheckCode::MarkerWritten)
    );
    assert_eq!(
        row.facts.get("grant").map(String::as_str),
        Some("destination")
    );
    assert_eq!(wiring.writer_grants(), vec![None]);
    assert_eq!(destination_principal.puts().len(), 1);
}

/// A backup readiness plan answers the row for the same principal as a
/// destination-access plan does: the one the plan names.
#[test]
fn a_backup_readiness_plan_answers_evidence_writable_for_the_evidence_write_principal() {
    let grant = GrantRef::workload_identity("evidence-writer-sa");
    let mut plan = readiness_plan(vec!["orders"], true, Some("/signing/key.pem"));
    if let CheckRequest::OperationReadiness(r) = &mut plan.request {
        r.evidence_write = Some(grant.clone());
    }
    let m = mount(&plan);
    let wiring = FakeWiring::default()
        .with_probe(
            FakeProbe::new().with_presence("orders", TopicPresence::Present { partitions: 6 }),
        )
        .with_role(DestinationRole::ArchiveRead, FakeObjects::new())
        .with_role(DestinationRole::EvidenceRead, FakeObjects::new())
        .with_writer(FakeObjects::new())
        .evidence_principal_fails(
            CheckCode::WorkloadIdentityNotInjected,
            "no web identity token in the pod",
        )
        .with_signer(Ok("abc123".to_string()));
    let run = drive(&m, &wiring);
    let row = run.row(CheckId::DestinationEvidenceWritable);
    assert_eq!(
        (row.state, row.code),
        (CheckState::NotReady, CheckCode::WorkloadIdentityNotInjected),
        "{row:?}"
    );
    assert!(
        row.message.contains("evidence-writer-sa"),
        "{}",
        row.message
    );
    assert_eq!(wiring.writer_grants(), vec![Some(grant)]);
}

/// The principal selection itself, over an injected environment — the unit
/// under both rows above, driven with no process-environment mutation.
///
/// MUTANT RP-3 (`store.rs`): make the `static` arm return
/// `StoreOptions::static_from_env()` (the destination grant's `AWS_*`). The
/// first assertion fails: the options would name `StaticFromEnv`, not the
/// evidence keys.
#[test]
fn the_evidence_write_options_are_the_evidence_grants_and_never_fall_back() {
    use logweir::check::store::{
        evidence_write_options, EVIDENCE_ACCESS_KEY_ID_ENV, EVIDENCE_SECRET_ACCESS_KEY_ENV,
        EVIDENCE_SESSION_TOKEN_ENV,
    };
    use logweir_engine_oso::storage::CredentialSource;
    let plan = destination();
    let budget = std::time::Duration::from_secs(10);
    // Placeholder values, deliberately not credential-shaped.
    let evidence_env = |k: &str| match k {
        k if k == EVIDENCE_ACCESS_KEY_ID_ENV => Some("evidence-key-id-fixture".to_string()),
        k if k == EVIDENCE_SECRET_ACCESS_KEY_ENV => Some("evidence-secret-fixture".to_string()),
        "AWS_ACCESS_KEY_ID" => Some("destination-key-id-fixture".to_string()),
        "AWS_SECRET_ACCESS_KEY" => Some("destination-secret-fixture".to_string()),
        // The DESTINATION grant's session token is right there too (review
        // L5): it must not be paired with the evidence keys.
        "AWS_SESSION_TOKEN" => Some("destination-token-fixture".to_string()),
        _ => None,
    };

    let grant = GrantRef::static_secret(EVIDENCE_SECRET);
    let opts = evidence_write_options(&plan, Some(&grant), budget, &evidence_env)
        .expect("the evidence keys are projected");
    assert_eq!(
        opts.credentials,
        CredentialSource::Static {
            access_key_id: "evidence-key-id-fixture".to_string(),
            secret_access_key: "evidence-secret-fixture".to_string(),
            session_token: None,
        },
        "a `static` evidence-write grant is the LOGWEIR_EVIDENCE_AWS_* keys, and the \
         destination grant's session token is not borrowed"
    );
    // …and its OWN session token, when the grant configures one.
    let with_token = |k: &str| match k {
        k if k == EVIDENCE_SESSION_TOKEN_ENV => Some("evidence-token-fixture".to_string()),
        other => evidence_env(other),
    };
    let CredentialSource::Static { session_token, .. } =
        evidence_write_options(&plan, Some(&grant), budget, &with_token)
            .expect("projected")
            .credentials
    else {
        panic!("a static source")
    };
    assert_eq!(session_token.as_deref(), Some("evidence-token-fixture"));
    assert_eq!(opts.request_timeout, Some(budget));
    assert_eq!(opts.max_retries, Some(logweir::check::store::RETRIES));

    // A projected variable that is missing is a REFUSAL — never the
    // destination grant's `AWS_*`, which are right there in the environment.
    let only_destination = |k: &str| match k {
        "AWS_ACCESS_KEY_ID" => Some("destination-key-id-fixture".to_string()),
        "AWS_SECRET_ACCESS_KEY" => Some("destination-secret-fixture".to_string()),
        _ => None,
    };
    let refused = evidence_write_options(&plan, Some(&grant), budget, &only_destination)
        .expect_err("no evidence keys, no marker");
    assert_eq!(refused.code, CheckCode::CredentialSecretKeyMissing);
    assert!(
        refused.message.contains(EVIDENCE_ACCESS_KEY_ID_ENV)
            && refused.message.contains(EVIDENCE_SECRET),
        "the refusal names the variable and the Secret: {}",
        refused.message
    );
    assert!(
        !refused.message.contains("fixture"),
        "and never a value: {}",
        refused.message
    );
    assert!(
        !refused.message.contains("  "),
        "and it is one sentence, not a source-indented one: {}",
        refused.message
    );

    let wi = GrantRef::workload_identity("evidence-writer-sa");
    assert_eq!(
        evidence_write_options(&plan, Some(&wi), budget, &evidence_env)
            .expect("workload identity needs no variable here")
            .credentials,
        CredentialSource::WorkloadIdentity,
        "the identity only; the static keys beside it are not this principal's"
    );

    // No separate grant: the destination grant, exactly as `options_for`
    // builds it.
    assert_eq!(
        evidence_write_options(&plan, None, budget, &evidence_env)
            .expect("the destination grant")
            .credentials,
        CredentialSource::StaticFromEnv
    );
}

/// The Secret a separated `evidenceRead` grant names in these rows.
const EVIDENCE_READ_SECRET: &str = "evidence-reader";

fn read_plan(grant: Option<GrantRef>) -> CheckPlan {
    plan_of(CheckRequest::DestinationAccess(DestinationAccessRequest {
        destination: destination(),
        roles: vec![DestinationRole::ArchiveRead, DestinationRole::EvidenceRead],
        write_probe: false,
        evidence_write: None,
        evidence_read: grant,
    }))
}

/// **The class sweep's own shape.** The destination grant may read the
/// evidence root and the `evidenceRead` principal may not: the row is red,
/// about the evidence-read principal, and the destination grant's handle was
/// asked for nothing under the evidence root.
///
/// MUTANT RP-10 (`access.rs`): hand `evidence_reader` `None` instead of
/// `probe.evidence_read`. The destination grant's handle answers, the row reads
/// `EvidenceReadable`, and this test fails on its first assertion.
#[test]
fn a_separated_evidence_read_principal_that_cannot_read_is_red_for_that_principal() {
    let grant = GrantRef::static_secret(EVIDENCE_READ_SECRET);
    let m = mount(&read_plan(Some(grant.clone())));
    let destination_principal = FakeObjects::new();
    let denied = FakeObjects::new().failing_get(access_denied());
    let wiring = FakeWiring::default()
        .with_role(DestinationRole::ArchiveRead, FakeObjects::new())
        .with_role(DestinationRole::EvidenceRead, destination_principal.clone())
        .with_evidence_reader(denied.clone());
    let run = drive(&m, &wiring);
    let row = run.row(CheckId::DestinationEvidenceReadable);
    assert_eq!(
        (row.state, row.code),
        (CheckState::NotReady, CheckCode::AccessDenied),
        "{row:?}"
    );
    assert_eq!(
        row.facts.get("grant").map(String::as_str),
        Some("evidenceRead")
    );
    assert!(
        row.message.contains(EVIDENCE_READ_SECRET),
        "{}",
        row.message
    );
    assert_eq!(wiring.reader_grants(), vec![Some(grant)]);
    assert!(
        destination_principal.calls().is_empty(),
        "the destination grant read nothing: {:?}",
        destination_principal.calls()
    );
    assert_eq!(
        denied.calls(),
        vec![format!("get logweir/readiness/{DEST_UID}.absent-probe")],
        "the evidence-read principal is asked for one get of an absent key and nothing else"
    );
}

/// The mirror image: the evidence-read principal can read, the destination
/// grant cannot. Green, about the evidence-read principal.
#[test]
fn a_separated_evidence_read_principal_that_can_read_is_green_whatever_the_destination_grant_may_do(
) {
    let m = mount(&read_plan(Some(GrantRef::static_secret(
        EVIDENCE_READ_SECRET,
    ))));
    let run = drive(
        &m,
        &FakeWiring::default()
            .with_role(DestinationRole::ArchiveRead, FakeObjects::new())
            .with_role(
                DestinationRole::EvidenceRead,
                FakeObjects::new().failing_get(access_denied()),
            )
            .with_evidence_reader(FakeObjects::new()),
    );
    let row = run.row(CheckId::DestinationEvidenceReadable);
    assert_eq!(
        (row.state, row.code),
        (CheckState::Ready, CheckCode::EvidenceReadable)
    );
    assert_eq!(
        row.facts.get("grant").map(String::as_str),
        Some("evidenceRead")
    );
}

/// **FX-31: the readiness probe reads no body.** It GETs a key nobody wrote
/// under `caps::PROBE` (0 bytes); when an object IS there, the store's answer
/// (refused unread, over the cap) still proves the grant, and is green.
///
/// KILLS: "the probe reads whatever is at the key" (the cap is not 0);
/// "TooLarge is a refusal" (the row turns `notReady`/unclassified).
#[test]
fn the_evidence_probe_reads_no_body_and_a_planted_object_still_proves_the_grant() {
    let m = mount(&read_plan(Some(GrantRef::static_secret(
        EVIDENCE_READ_SECRET,
    ))));
    let probe_key = logweir::check::store::absent_probe_key(DEST_UID);
    let reader = FakeObjects::new().with_object(&probe_key, &vec![b'x'; 1 << 20]);
    let run = drive(
        &m,
        &FakeWiring::default()
            .with_role(DestinationRole::ArchiveRead, FakeObjects::new())
            .with_role(DestinationRole::EvidenceRead, FakeObjects::new())
            .with_evidence_reader(reader.clone()),
    );
    let row = run.row(CheckId::DestinationEvidenceReadable);
    assert_eq!(
        (row.state, row.code),
        (CheckState::Ready, CheckCode::EvidenceReadable),
        "{row:?}"
    );
    assert_eq!(
        reader.read_caps(),
        vec![(probe_key, logweir_engine_oso::storage::caps::PROBE)],
        "the probe's one GET reads no body"
    );
}

/// A grant no check pod holds — the controller's identity, or none at all —
/// is answered `unknown` with NO request by anybody.
///
/// MUTANT RP-11 (`access.rs`): delete the `!exercisable_in_pod()` early
/// answer. The row is then built through `evidence_reader` with a grant no pod
/// holds; the fake's handle answers, the row reads `EvidenceReadable`, and
/// this test fails.
#[test]
fn an_evidence_read_grant_no_pod_holds_is_unknown_and_reads_nothing() {
    for credentials in [
        GrantCredentials::ControllerIdentity,
        GrantCredentials::NotConfigured,
    ] {
        let m = mount(&read_plan(Some(GrantRef::without_pod_credential(
            credentials,
        ))));
        let destination_principal = FakeObjects::new();
        let wiring = FakeWiring::default()
            .with_role(DestinationRole::ArchiveRead, FakeObjects::new())
            .with_role(DestinationRole::EvidenceRead, destination_principal.clone())
            .with_evidence_reader(FakeObjects::new());
        let run = drive(&m, &wiring);
        let row = run.row(CheckId::DestinationEvidenceReadable);
        assert_eq!(
            (row.state, row.code, row.gating),
            (
                CheckState::Unknown,
                CheckCode::EvidenceReadNotConfigured,
                Gating::Advisory
            ),
            "{credentials:?}: {row:?}"
        );
        assert!(!row.remedy.is_empty());
        assert!(
            wiring.reader_grants().is_empty() && destination_principal.calls().is_empty(),
            "{credentials:?}: nobody read anything"
        );
    }
}

/// Unchanged when the grants are one grant: the destination grant reads, and
/// the row says so.
#[test]
fn without_a_separate_read_grant_the_destination_grant_reads_as_before() {
    let m = mount(&read_plan(None));
    let destination_principal = FakeObjects::new();
    let wiring = FakeWiring::default()
        .with_role(DestinationRole::ArchiveRead, FakeObjects::new())
        .with_role(DestinationRole::EvidenceRead, destination_principal.clone())
        .evidence_reader_fails(CheckCode::AccessDenied, "never asked for");
    let run = drive(&m, &wiring);
    let row = run.row(CheckId::DestinationEvidenceReadable);
    assert_eq!(row.code, CheckCode::EvidenceReadable);
    assert_eq!(
        row.facts.get("grant").map(String::as_str),
        Some("destination")
    );
    assert_eq!(wiring.reader_grants(), vec![None]);
    assert_eq!(destination_principal.calls().len(), 1);
}

/// **Decision 1 (orchestrator, 2026-09-23): the archive-write grant is a
/// documented limit, and its row can NEVER read green** — not with the marker
/// probe on, not with a writer that would accept anything, not with a
/// separate evidence-write grant. It says why, naming the archive prefix, and
/// nothing is written anywhere but the marker key.
///
/// MUTANT RP-12 (`access.rs`): answer the `ArchiveWrite` arm with
/// `ready(..., MarkerWritten)` when `write_probe` is set (the "reuse the
/// marker for the archive" shortcut). The first assertion fails.
#[test]
fn the_archive_write_row_is_never_green_and_names_the_archive_prefix() {
    for grant in [None, Some(GrantRef::static_secret(EVIDENCE_SECRET))] {
        let m = mount(&plan_of(CheckRequest::DestinationAccess(
            DestinationAccessRequest {
                destination: destination(),
                roles: vec![
                    DestinationRole::ArchiveWrite,
                    DestinationRole::EvidenceWrite,
                ],
                write_probe: true,
                evidence_write: grant.clone(),
                evidence_read: None,
            },
        )));
        let writer = FakeObjects::new();
        let evidence_principal = FakeObjects::new();
        let run = drive(
            &m,
            &FakeWiring::default()
                .with_role(DestinationRole::ArchiveWrite, writer.clone())
                .with_writer(writer.clone())
                .with_evidence_principal(evidence_principal.clone()),
        );
        let row = run.row(CheckId::DestinationArchivePrefixWritable);
        assert_ne!(row.state, CheckState::Ready, "{grant:?}: {row:?}");
        assert_eq!(
            (row.state, row.gating, row.code),
            (
                CheckState::Unknown,
                Gating::ExecutionOnly,
                CheckCode::ArchivePrefixWriteVerifiedOnlyAtExecution
            )
        );
        assert!(
            row.message.contains("`kafka-backups`") && row.message.contains("not write-probed"),
            "the row names the archive prefix and says it was not probed: {}",
            row.message
        );
        assert!(!row.remedy.is_empty());
        // The marker row may be green; it is a DIFFERENT row about a
        // different key.
        assert_eq!(
            run.row(CheckId::DestinationEvidenceWritable).state,
            CheckState::Ready
        );
        let puts: Vec<String> = writer
            .puts()
            .into_iter()
            .chain(evidence_principal.puts())
            .map(|(k, _)| k)
            .collect();
        assert_eq!(
            puts,
            vec![format!("logweir/readiness/{DEST_UID}.json")],
            "nothing is written under the archive prefix"
        );
        assert!(
            logweir_core::check_contract::aggregate(&run.result().checks)
                != logweir_core::check_contract::OverallState::NotReady
        );
    }
}

/// The evidence-read principal's options: its OWN three variables, never the
/// destination's `AWS_*` and never the evidence-write grant's.
///
/// MUTANT RP-13 (`store.rs`): map `EvidenceRead` to `EVIDENCE_WRITE_ENV` in
/// `grant_env`. The options then carry the evidence-WRITE keys, and the first
/// assertion fails.
#[test]
fn the_evidence_read_options_are_the_evidence_read_grants_and_never_fall_back() {
    use logweir::check::store::evidence_read_options;
    use logweir_core::check_contract::{EVIDENCE_READ_ENV, EVIDENCE_WRITE_ENV};
    use logweir_engine_oso::storage::CredentialSource;
    let plan = destination();
    let budget = std::time::Duration::from_secs(10);
    let env = |k: &str| match k {
        k if k == EVIDENCE_READ_ENV.access_key_id => Some("read-key-id-fixture".to_string()),
        k if k == EVIDENCE_READ_ENV.secret_access_key => Some("read-secret-fixture".to_string()),
        k if k == EVIDENCE_WRITE_ENV.access_key_id => Some("write-key-id-fixture".to_string()),
        k if k == EVIDENCE_WRITE_ENV.secret_access_key => Some("write-secret-fixture".to_string()),
        k if k == EVIDENCE_WRITE_ENV.session_token => Some("write-token-fixture".to_string()),
        "AWS_ACCESS_KEY_ID" => Some("destination-key-id-fixture".to_string()),
        "AWS_SECRET_ACCESS_KEY" => Some("destination-secret-fixture".to_string()),
        "AWS_SESSION_TOKEN" => Some("destination-token-fixture".to_string()),
        _ => None,
    };
    let grant = GrantRef::static_secret(EVIDENCE_READ_SECRET);
    assert_eq!(
        evidence_read_options(&plan, Some(&grant), budget, &env)
            .expect("projected")
            .credentials,
        CredentialSource::Static {
            access_key_id: "read-key-id-fixture".to_string(),
            secret_access_key: "read-secret-fixture".to_string(),
            session_token: None,
        },
        "no other principal's session token is borrowed (review L5)"
    );
    let with_token = |k: &str| match k {
        k if k == EVIDENCE_READ_ENV.session_token => Some("read-token-fixture".to_string()),
        other => env(other),
    };
    let CredentialSource::Static { session_token, .. } =
        evidence_read_options(&plan, Some(&grant), budget, &with_token)
            .expect("projected")
            .credentials
    else {
        panic!("a static source")
    };
    assert_eq!(session_token.as_deref(), Some("read-token-fixture"));
    let only_others = |k: &str| match k {
        k if k == EVIDENCE_WRITE_ENV.access_key_id => Some("write-key-id-fixture".to_string()),
        k if k == EVIDENCE_WRITE_ENV.secret_access_key => Some("write-secret-fixture".to_string()),
        "AWS_ACCESS_KEY_ID" => Some("destination-key-id-fixture".to_string()),
        _ => None,
    };
    let refused = evidence_read_options(&plan, Some(&grant), budget, &only_others)
        .expect_err("no read keys, no read");
    assert_eq!(refused.code, CheckCode::CredentialSecretKeyMissing);
    assert!(refused.message.contains(EVIDENCE_READ_ENV.access_key_id));
    assert!(!refused.message.contains("fixture"));
    for c in [
        GrantCredentials::ControllerIdentity,
        GrantCredentials::NotConfigured,
    ] {
        assert_eq!(
            evidence_read_options(
                &plan,
                Some(&GrantRef::without_pod_credential(c)),
                budget,
                &env
            )
            .expect_err("no pod holds it")
            .code,
            CheckCode::EvidenceReadNotConfigured
        );
    }
}

/// One set of evidence-WRITE variable names across the check runner and the
/// Restore runner's store contract.
#[test]
fn the_check_and_the_restore_runner_read_one_set_of_evidence_variables() {
    use logweir::check::store::{
        EVIDENCE_ACCESS_KEY_ID_ENV, EVIDENCE_SECRET_ACCESS_KEY_ENV, EVIDENCE_SESSION_TOKEN_ENV,
    };
    let w = logweir_core::check_contract::EVIDENCE_WRITE_ENV;
    assert_eq!(
        (w.access_key_id, w.secret_access_key, w.session_token),
        (
            EVIDENCE_ACCESS_KEY_ID_ENV,
            EVIDENCE_SECRET_ACCESS_KEY_ENV,
            EVIDENCE_SESSION_TOKEN_ENV
        )
    );
    let r = logweir_core::check_contract::EVIDENCE_READ_ENV;
    for name in [r.access_key_id, r.secret_access_key, r.session_token] {
        assert!(
            ![w.access_key_id, w.secret_access_key, w.session_token].contains(&name)
                && !name.starts_with("AWS_"),
            "`{name}` shadows another principal's variable"
        );
    }
}

/// The runner's two handles must never be built from ONE grant when the plan
/// names two: a source-level guard that `open_evidence_write` goes through
/// `evidence_write_options` and nowhere else.
#[test]
fn the_marker_handle_is_built_only_through_the_principal_selection() {
    let (_, store) = check_sources()
        .into_iter()
        .find(|(p, _)| p.ends_with("check/store.rs"))
        .expect("store.rs");
    let body = store
        .split("pub fn open_evidence_write(")
        .nth(1)
        .expect("open_evidence_write exists")
        .split("\npub fn ")
        .next()
        .unwrap_or_default();
    assert!(
        body.contains("evidence_write_options(plan, grant,"),
        "open_evidence_write builds its options from the evidence-write grant: {body}"
    );
    assert!(
        !body.contains("options_for("),
        "and never from the destination grant's options directly: {body}"
    );
}

// ===========================================================================
// 4a. `sourceConnection` (D2-SOURCECHECK): PLAT-03.1's source-connectivity kind
// ===========================================================================

fn source_connection_plan() -> CheckPlan {
    plan_of(CheckRequest::SourceConnection(
        logweir_core::check_contract::SourceConnectionRequest {
            connection: connection(),
        },
    ))
}

#[test]
fn a_source_connection_check_reports_every_row_it_owns() {
    let m = mount(&source_connection_plan());
    let run = drive(
        &m,
        &FakeWiring::default().with_probe(FakeProbe::new().with_topics(&[("orders", 6)])),
    );
    assert_eq!(run.code, ExitCode::Ok);
    let got: BTreeSet<&str> = ids(&run.result()).into_iter().collect();
    let want: BTreeSet<&str> = ["runner.contract", "connection.authenticated"]
        .into_iter()
        .collect();
    assert_eq!(
        got, want,
        "a connectivity check emits the two rows its request can ask about, and no other"
    );
    assert_eq!(
        logweir_core::check_contract::aggregate(&run.result().checks),
        logweir_core::check_contract::OverallState::Ready
    );

    // THE TWO FACTS D2 §6.3 puts on this row, and the `clusterIdentity`
    // VERDICT that stays the controller's: a Job holding a credential is given
    // no Kubernetes read, so it can publish what the broker said and not what
    // `KafkaCluster.status.clusterId` says.
    let auth = run.row(CheckId::ConnectionAuthenticated);
    assert_eq!(auth.state, CheckState::Ready);
    assert_eq!(auth.code, CheckCode::Authenticated);
    assert_eq!(
        auth.facts.get("clusterId").map(String::as_str),
        Some("M29I2S7FQPyHBEX12Vx7XA")
    );
    assert_eq!(auth.facts.get("brokerCount").map(String::as_str), Some("3"));
    assert!(!run.has(CheckId::ConnectionClusterIdentity));

    // THE SCOPE IS ON THE ROW, because D2 §6.3's acceptance sentence is "check
    // time AND scope" and a status row with `scope: null` is a finding about
    // nothing an operator can match against an ACL export.
    let scope = auth.scope.clone().expect("the row carries its scope");
    assert_eq!(scope.kind, "KafkaCluster");
    assert_eq!(scope.name, connection().principal);
    assert!(
        auth.observed_at.is_some(),
        "and the instant it was observed"
    );
    assert!(auth.expires_at.is_some(), "and when it stops counting");
}

/// The row this kind must NOT publish.
///
/// MUTANT: emit `connection.topicsDescribable` from
/// `kinds::source_connection::run` — for instance by delegating to
/// `readiness::topics_describable` over the empty slice, which returns a READY
/// `TopicsDescribable` row. This test fails, and so does
/// `the_expected_rows_are_the_rows_the_runner_emits` in `weirkeeper`, because
/// the controller's mirror does not list it.
///
/// The claim is not stylistic. `TopicNotFound` and `TopicNotAuthorized` are
/// facts about a topic the requester NAMED; over an empty selection `ready`
/// would be a green verdict about the empty set and `unknown` on a blocking
/// row would pin every connection test at `unknown` for ever.
#[test]
fn a_source_connection_check_makes_no_claim_about_topics() {
    let m = mount(&source_connection_plan());
    let run = drive(
        &m,
        &FakeWiring::default().with_probe(
            FakeProbe::new()
                .with_topics(&[("orders", 6)])
                .default_presence(TopicPresence::Present { partitions: 6 }),
        ),
    );
    assert!(
        !run.has(CheckId::ConnectionTopicsDescribable),
        "this request names no topic, so no topic row has an honest answer: {:?}",
        ids(&run.result())
    );
    assert!(!run.has(CheckId::ConnectionTopicsReadable));
    for row in &run.result().checks {
        assert_ne!(
            row.code,
            CheckCode::TopicsDescribable,
            "`{}` published a topic verdict",
            row.id
        );
    }
    // AND NO TOPIC LINE REACHES THE RELAY. The inventory frames are a
    // `topicInventory` fact; a connectivity check that relayed a topic list
    // would be a discovery nobody asked for, over a bound nobody set.
    assert_eq!(run.topic_frame_count(), 0);
}

/// A broker that does not answer is `connection.authenticated notReady`, with
/// the broker's own classified code, a remedy, and the scope still on it.
///
/// MUTANT: drop `.with_scope(scope)` from the `Err` arm of
/// `kinds::source_connection::run`. The row an operator actually reads is the
/// failing one, and it is the one that used to reach a status with
/// `scope: null` (the same defect `readiness::authenticated` records).
#[test]
fn a_source_connection_check_reports_the_dial_that_failed() {
    let m = mount(&source_connection_plan());
    let run = drive(
        &m,
        &FakeWiring::default().broker_fails(CheckCode::AuthenticationFailed, "SASL rejected"),
    );
    assert_eq!(
        run.code,
        ExitCode::Ok,
        "a verdict is not an operational failure"
    );
    let row = run.row(CheckId::ConnectionAuthenticated);
    assert_eq!(row.state, CheckState::NotReady);
    assert_eq!(row.code, CheckCode::AuthenticationFailed);
    assert!(
        row.remedy.contains("SASL"),
        "every code the runner emits carries its remedy: {:?}",
        row.remedy
    );
    assert_eq!(
        row.scope.as_ref().map(|s| s.name.clone()),
        Some(connection().principal),
        "the row an operator reads is the failing one, and it carries the scope too"
    );
    assert_eq!(
        logweir_core::check_contract::aggregate(&run.result().checks),
        logweir_core::check_contract::OverallState::NotReady
    );
}

/// A metadata timeout is `unknown`, never `notReady`: "I could not tell" and
/// "it said no" are different facts and D2 §6.3 gives them different columns.
#[test]
fn a_source_connection_timeout_is_unknown_and_not_a_refusal() {
    let m = mount(&source_connection_plan());
    let run = drive(
        &m,
        &FakeWiring::default().with_probe(
            FakeProbe::new().failing_cluster_id(CheckCode::MetadataTimeout, "no metadata"),
        ),
    );
    let row = run.row(CheckId::ConnectionAuthenticated);
    assert_eq!(row.state, CheckState::Unknown);
    assert_eq!(row.code, CheckCode::MetadataTimeout);
    assert_eq!(
        logweir_core::check_contract::aggregate(&run.result().checks),
        logweir_core::check_contract::OverallState::Unknown
    );
}

/// `runner.contract` is Ready by construction: the plan parsed at this exact
/// contract version, which is the proof.
#[test]
fn every_kind_reports_the_runner_contract() {
    let m = mount(&access_plan(vec![DestinationRole::ArchiveRead], false));
    let run = drive(
        &m,
        &FakeWiring::default().with_role(DestinationRole::ArchiveRead, FakeObjects::new()),
    );
    let row = run.row(CheckId::RunnerContract);
    assert_eq!(row.code, CheckCode::ContractSupported);
    assert_eq!(row.state, CheckState::Ready);
}

// ===========================================================================
// 5. `operationReadiness`
// ===========================================================================

fn readiness_plan(topics: Vec<&str>, write_probe: bool, signer: Option<&str>) -> CheckPlan {
    plan_of(CheckRequest::OperationReadiness(Box::new(
        OperationReadinessRequest {
            operation: logweir_core::check_contract::CheckOperation::Backup,
            connection: connection(),
            destination: Some(destination()),
            roles: vec![
                DestinationRole::ArchiveRead,
                DestinationRole::EvidenceWrite,
                DestinationRole::EvidenceRead,
            ],
            topics: topics.into_iter().map(ToString::to_string).collect(),
            signer_path: signer.map(ToString::to_string),
            write_probe,
            evidence_write: None,
            evidence_read: None,
            skip_checks: Vec::new(),
            capability_checks: Vec::new(),
        },
    )))
}

#[test]
fn a_readiness_check_reports_every_row_it_owns() {
    let m = mount(&readiness_plan(
        vec!["orders"],
        true,
        Some("/signing/key.pem"),
    ));
    let run = drive(
        &m,
        &FakeWiring::default()
            .with_probe(
                FakeProbe::new().with_presence("orders", TopicPresence::Present { partitions: 6 }),
            )
            .with_role(DestinationRole::ArchiveRead, FakeObjects::new())
            .with_role(DestinationRole::EvidenceRead, FakeObjects::new())
            .with_writer(FakeObjects::new())
            .with_signer(Ok("abc123".to_string())),
    );
    assert_eq!(run.code, ExitCode::Ok);
    let got: BTreeSet<&str> = ids(&run.result()).into_iter().collect();
    let want: BTreeSet<&str> = [
        "runner.contract",
        "connection.authenticated",
        "connection.topicsDescribable",
        "connection.topicsReadable",
        "destination.archiveListable",
        "destination.evidenceWritable",
        "destination.evidenceReadable",
        "signer.privateKeyUsable",
    ]
    .into_iter()
    .collect();
    assert_eq!(
        got, want,
        "the runner emits exactly the D2 §6.3 rows it owns"
    );
    assert_eq!(
        logweir_core::check_contract::aggregate(&run.result().checks),
        logweir_core::check_contract::OverallState::Ready
    );
    // D2 §6.3: the observed cluster id is a FACT of `connection.authenticated`
    // and NOT a `connection.clusterIdentity` verdict — the controller owns the
    // comparison, and this runner never emits that row.
    let auth = run.row(CheckId::ConnectionAuthenticated);
    assert_eq!(
        auth.facts.get("clusterId").map(String::as_str),
        Some("M29I2S7FQPyHBEX12Vx7XA")
    );
    assert_eq!(auth.facts.get("brokerCount").map(String::as_str), Some("3"));
    assert!(!run.has(CheckId::ConnectionClusterIdentity));
    assert_eq!(
        run.row(CheckId::SignerPrivateKeyUsable)
            .facts
            .get("signerKeyId")
            .map(String::as_str),
        Some("abc123")
    );
    // The catalogue's expiries, not the call site's.
    let d = run.row(CheckId::ConnectionTopicsDescribable);
    assert_eq!(
        d.expires_at.unwrap() - d.observed_at.unwrap(),
        chrono::Duration::minutes(10)
    );
    assert_eq!(
        auth.expires_at.unwrap() - auth.observed_at.unwrap(),
        chrono::Duration::minutes(15)
    );
}

// ---------------------------------------------------------------------------
// 5a. PROD-01.2 — the capability rows. For each: the capability PRESENT (no
//     finding), ABSENT (the finding, with its fallback text), and NOT
//     OBSERVED (unknown, never ready).
// ---------------------------------------------------------------------------

/// Apache Kafka 4.3.1's ranges for the APIs these rows read
/// (`docs/support-matrix.md`, measured).
const KAFKA_4_3: [(i16, i16, i16); 8] = [
    (0, 0, 13), // Produce
    (1, 4, 18), // Fetch
    (2, 1, 11), // ListOffsets
    (3, 0, 13), // Metadata
    (16, 0, 5), // ListGroups
    (17, 0, 1), // SaslHandshake
    (32, 1, 4), // DescribeConfigs
    (36, 0, 2), // SaslAuthenticate
];

/// Redpanda v26.2.4's, measured: Produce stops at v7 and ListGroups at v4.
const REDPANDA_26_2: [(i16, i16, i16); 8] = [
    (0, 0, 7),
    (1, 4, 13),
    (2, 0, 6),
    (3, 0, 12),
    (16, 0, 4),
    (17, 0, 1),
    (32, 0, 4),
    (36, 0, 2),
];

fn backup_capabilities() -> Vec<CheckId> {
    logweir_core::check_contract::capability_checks_for(
        logweir_core::check_contract::CheckOperation::Backup,
    )
    .to_vec()
}

/// `readiness_plan`, listing the capability rows `listed`.
fn capability_plan(topics: Vec<&str>, listed: Vec<CheckId>) -> CheckPlan {
    let mut plan = readiness_plan(topics, false, None);
    let CheckRequest::OperationReadiness(r) = &mut plan.request else {
        unreachable!("readiness_plan builds an operationReadiness request")
    };
    r.capability_checks = listed;
    plan
}

fn capability_wiring(probe: FakeProbe) -> FakeWiring {
    FakeWiring::default()
        .with_probe(probe)
        .with_role(DestinationRole::ArchiveRead, FakeObjects::new())
        .with_role(DestinationRole::EvidenceRead, FakeObjects::new())
}

/// **Listed by the plan, or not emitted.** A plan WITHOUT `capabilityChecks`
/// (every plan an older controller renders) gets no capability row and no
/// ApiVersions observation, so that controller never receives an id it cannot
/// read; the same plan listing them gets exactly those rows, pinned for the
/// controller's mirror (`weirkeeper::controllers::preflight::job_rows`).
#[test]
fn a_readiness_check_with_capability_checks_reports_every_row_it_owns() {
    let probe = || {
        FakeProbe::new()
            .with_presence("orders", TopicPresence::Present { partitions: 6 })
            .with_api_versions(&KAFKA_4_3)
    };
    // Not listed: nothing emitted, nothing observed.
    let unlisted = probe();
    let run = drive(
        &mount(&capability_plan(vec!["orders"], Vec::new())),
        &capability_wiring(unlisted.clone()),
    );
    for id in logweir_core::check_contract::CAPABILITY_CHECKS {
        assert!(
            !run.has(id),
            "`{id}` was emitted for a plan that does not list it"
        );
    }
    assert_eq!(unlisted.api_versions_calls(), 0);
    assert!(unlisted.topic_config_calls().is_empty());

    // Listed.
    let listed = probe();
    let m = mount(&{
        let mut plan = readiness_plan(vec!["orders"], true, Some("/signing/key.pem"));
        let CheckRequest::OperationReadiness(r) = &mut plan.request else {
            unreachable!()
        };
        r.capability_checks = backup_capabilities();
        plan
    });
    let run = drive(
        &m,
        &capability_wiring(listed.clone())
            .with_writer(FakeObjects::new())
            .with_signer(Ok("abc123".to_string())),
    );
    assert_eq!(run.code, ExitCode::Ok);
    let got: BTreeSet<&str> = ids(&run.result()).into_iter().collect();
    let want: BTreeSet<&str> = [
        "runner.contract",
        "connection.authenticated",
        "connection.topicsDescribable",
        "connection.topicsReadable",
        "connection.engineProtocol",
        "connection.topicConfigsReadable",
        "connection.groupTypes",
        "destination.archiveListable",
        "destination.evidenceWritable",
        "destination.evidenceReadable",
        "signer.privateKeyUsable",
    ]
    .into_iter()
    .collect();
    assert_eq!(got, want);
    assert_eq!(
        logweir_core::check_contract::aggregate(&run.result().checks),
        logweir_core::check_contract::OverallState::Ready
    );
    // ONE observation serves both rows that read it.
    assert_eq!(listed.api_versions_calls(), 1);
    assert_eq!(listed.topic_config_calls(), vec!["orders".to_string()]);
    // The catalogue's gating: the engine row blocks, the other two advise.
    assert_eq!(
        run.row(CheckId::ConnectionEngineProtocol).gating,
        Gating::Blocking
    );
    assert_eq!(
        run.row(CheckId::ConnectionTopicConfigsReadable).gating,
        Gating::Advisory
    );
    assert_eq!(
        run.row(CheckId::ConnectionGroupTypes).gating,
        Gating::Advisory
    );
}

/// `connection.engineProtocol`. PRESENT on Apache Kafka's ranges and on
/// Redpanda's (a capture sends Metadata v9, ListOffsets v5, Fetch v11 and
/// DescribeConfigs v1, all inside both). ABSENT when the endpoint does not
/// serve one of them: the row names the request, both versions and the
/// fallback, and blocks.
#[test]
fn connection_engine_protocol_names_the_request_an_endpoint_does_not_serve() {
    let plan = || {
        mount(&capability_plan(
            vec![],
            vec![CheckId::ConnectionEngineProtocol],
        ))
    };
    for present in [&KAFKA_4_3, &REDPANDA_26_2] {
        let run = drive(
            &plan(),
            &capability_wiring(FakeProbe::new().with_api_versions(present)),
        );
        let row = run.row(CheckId::ConnectionEngineProtocol);
        assert_eq!(
            (row.state, row.code),
            (CheckState::Ready, CheckCode::EngineProtocolSupported),
            "{row:?}"
        );
        assert_eq!(
            row.facts.get("engineRequests").map(String::as_str),
            Some("Metadata v9, ListOffsets v5, Fetch v11, DescribeConfigs v1")
        );
        assert!(row.remedy.is_empty(), "a ready row has nothing to fix");
    }

    // ABSENT: an endpoint whose Fetch stops at v10 (one below what the engine
    // sends) and that does not serve DescribeConfigs at all.
    let mut lacking: Vec<(i16, i16, i16)> = KAFKA_4_3
        .iter()
        .copied()
        .filter(|(key, _, _)| *key != 32)
        .collect();
    lacking.iter_mut().find(|(k, _, _)| *k == 1).unwrap().2 = 10;
    let run = drive(
        &plan(),
        &capability_wiring(FakeProbe::new().with_api_versions(&lacking)),
    );
    let row = run.row(CheckId::ConnectionEngineProtocol);
    assert_eq!(
        (row.state, row.code, row.gating),
        (
            CheckState::NotReady,
            CheckCode::EngineProtocolUnsupported,
            Gating::Blocking
        ),
        "{row:?}"
    );
    assert!(
        row.message
            .contains("Fetch v11 and this endpoint serves Fetch v4-v10")
            && row
                .message
                .contains("DescribeConfigs v1 and this endpoint serves DescribeConfigs not served")
            && row.message.contains("cannot back up from this endpoint"),
        "{}",
        row.message
    );
    assert!(
        row.remedy.contains("Use an endpoint that serves")
            && row.remedy.contains("docs/support-matrix.md"),
        "the fallback says what to use and where the measured endpoints are: {}",
        row.remedy
    );
    assert!(!row.remedy.ends_with('…'), "the remedy fits its cap whole");
    assert_eq!(
        row.detail.as_ref().unwrap()["sample"],
        serde_json::json!(["Fetch v11", "DescribeConfigs v1"])
    );
    assert_eq!(
        logweir_core::check_contract::aggregate(&run.result().checks),
        logweir_core::check_contract::OverallState::NotReady
    );

    // NOT OBSERVED: unknown, blocking, never ready.
    for probe in [
        FakeProbe::new(),
        FakeProbe::new().failing_api_versions("no broker answered ApiVersions"),
    ] {
        let run = drive(&plan(), &capability_wiring(probe));
        let row = run.row(CheckId::ConnectionEngineProtocol);
        assert_eq!(
            (row.state, row.code),
            (CheckState::Unknown, CheckCode::ApiVersionsNotObserved),
            "{row:?}"
        );
        assert!(row.remedy.contains("Re-run the check"), "{}", row.remedy);
        assert_ne!(
            logweir_core::check_contract::aggregate(&run.result().checks),
            logweir_core::check_contract::OverallState::Ready
        );
    }
}

/// On a SASL connection the engine also sends SaslHandshake v1 and
/// SaslAuthenticate v2, and an endpoint that does not serve them is refused
/// for those; on a plaintext connection they are not asked for.
#[test]
fn connection_engine_protocol_asks_for_the_sasl_requests_only_on_a_sasl_connection() {
    let no_sasl_v2: Vec<(i16, i16, i16)> = KAFKA_4_3
        .iter()
        .map(|(k, a, b)| if *k == 36 { (*k, *a, 1) } else { (*k, *a, *b) })
        .collect();
    let plan = |mode: &str| {
        let mut plan = capability_plan(vec![], vec![CheckId::ConnectionEngineProtocol]);
        let CheckRequest::OperationReadiness(r) = &mut plan.request else {
            unreachable!()
        };
        r.connection.auth_mode = mode.to_string();
        mount(&plan)
    };
    let run = drive(
        &plan("plaintext"),
        &capability_wiring(FakeProbe::new().with_api_versions(&no_sasl_v2)),
    );
    assert_eq!(
        run.row(CheckId::ConnectionEngineProtocol).code,
        CheckCode::EngineProtocolSupported
    );
    let run = drive(
        &plan("scramSha512"),
        &capability_wiring(FakeProbe::new().with_api_versions(&no_sasl_v2)),
    );
    let row = run.row(CheckId::ConnectionEngineProtocol);
    assert_eq!(row.code, CheckCode::EngineProtocolUnsupported, "{row:?}");
    assert!(
        row.message
            .contains("SaslAuthenticate v2 and this endpoint serves SaslAuthenticate v0-v1"),
        "{}",
        row.message
    );
}

/// **PROD-01.2 review, M3: a capability row is about EVERY broker of the
/// cluster, or it is `unknown`.** The first version said `ready` from
/// whichever brokers the observing client had dialled.
///
/// * All three brokers answered and agree: `ready`, and both the message and
///   the fact say three, as DISTINCT BROKERS of how many.
/// * They answered and do not agree (a rolling upgrade): the row is judged on
///   what every one of them serves, here Produce v3-v7, so the restore row is
///   `notReady` although one broker serves v8, and the message says they
///   differ.
/// * Two of three answered: `unknown`, never `ready`, with the count and the
///   silent broker in the message. The reason is the observation's own
///   (`logweir_kafka::api_versions::full_view`), built here from the lines a
///   two-of-three log holds.
#[test]
fn a_capability_row_is_about_every_broker_or_it_is_unknown() {
    use logweir_kafka::api_versions::{answers, full_view, Broker};
    let plan = || {
        mount(&capability_plan(
            vec![],
            vec![
                CheckId::ConnectionEngineProtocol,
                CheckId::ConnectionGroupTypes,
            ],
        ))
    };

    // All three, agreeing.
    let run = drive(
        &plan(),
        &capability_wiring(FakeProbe::new().with_cluster_api_versions(&KAFKA_4_3, 3, false)),
    );
    for id in [
        CheckId::ConnectionEngineProtocol,
        CheckId::ConnectionGroupTypes,
    ] {
        let row = run.row(id);
        assert_eq!(row.state, CheckState::Ready, "{row:?}");
        assert!(
            row.message
                .starts_with("all 3 brokers of this endpoint serve"),
            "{}",
            row.message
        );
        assert!(!row.message.contains("do not all serve"), "{}", row.message);
        assert_eq!(
            row.facts.get("brokersAnswered").map(String::as_str),
            Some("3 of 3"),
            "distinct brokers of how many, not connections: {row:?}"
        );
    }

    // All three, DIFFERING: the weakest answer, ListGroups v0-v4 and Fetch
    // v4-v10 (one broker has not been upgraded).
    let mut weakest = KAFKA_4_3.to_vec();
    weakest.iter_mut().find(|(k, _, _)| *k == 16).unwrap().2 = 4;
    weakest.iter_mut().find(|(k, _, _)| *k == 1).unwrap().2 = 10;
    let run = drive(
        &plan(),
        &capability_wiring(FakeProbe::new().with_cluster_api_versions(&weakest, 3, true)),
    );
    let row = run.row(CheckId::ConnectionEngineProtocol);
    assert_eq!(
        (row.state, row.code),
        (CheckState::NotReady, CheckCode::EngineProtocolUnsupported),
        "one broker that cannot be read from is enough: {row:?}"
    );
    assert!(
        row.message
            .contains("Fetch v11 and all 3 brokers of this endpoint serve Fetch v4-v10")
            && row
                .message
                .contains("The brokers do not all serve the same versions"),
        "{}",
        row.message
    );
    let row = run.row(CheckId::ConnectionGroupTypes);
    assert_eq!(row.code, CheckCode::GroupTypesNotListed, "{row:?}");
    assert!(
        row.message
            .contains("The brokers do not all serve the same versions"),
        "{}",
        row.message
    );

    // TWO OF THREE: the reason the observation itself gives.
    let listed: Vec<Broker> = (1..=3)
        .map(|id| Broker::new(id, &format!("b{id}.example"), 9092))
        .collect();
    let lines: Vec<(&str, String)> = [1, 3]
        .iter()
        .flat_map(|id| {
            let at = format!("[thrd:b{id}.example:9092/{id}]: b{id}.example:9092/{id}: ");
            vec![
                ("APIVERSION", format!("{at}Broker API support:")),
                (
                    "APIVERSION",
                    format!("{at}  ApiKey Produce (0) Versions 0..13"),
                ),
            ]
        })
        .collect();
    let partial = full_view(
        &answers(lines.iter().map(|(f, m)| (*f, m.as_str()))),
        true,
        &listed,
        &[],
    )
    .expect_err("broker 2 did not answer");
    let probe = FakeProbe::new().failing_api_versions(&partial.to_string());
    let run = drive(&plan(), &capability_wiring(probe));
    for id in [
        CheckId::ConnectionEngineProtocol,
        CheckId::ConnectionGroupTypes,
    ] {
        let row = run.row(id);
        assert_eq!(
            (row.state, row.code),
            (CheckState::Unknown, CheckCode::ApiVersionsNotObserved),
            "a partial view is never ready: {row:?}"
        );
        assert!(
            row.message.contains("2 of 3 broker(s)")
                && row.message.contains("broker 2 (b2.example:9092)"),
            "{}",
            row.message
        );
        assert!(!row.facts.contains_key("brokersAnswered"), "{row:?}");
    }
    assert_ne!(
        logweir_core::check_contract::aggregate(&run.result().checks),
        logweir_core::check_contract::OverallState::Ready
    );
}

/// **Review L9: the observation is not started with no budget left.** A row
/// that reads the ApiVersions answer is `unknown` and says the endpoint was
/// not asked; the probe is never called, so nothing is dialled after the
/// check's deadline.
///
/// CONTROL: with budget left the same probe is called once and the rows are
/// `ready`.
#[test]
fn the_api_versions_observation_does_not_start_with_no_budget_left() {
    use logweir::check::kinds::readiness::capability_rows;
    let plan = capability_plan(vec![], backup_capabilities());
    let CheckRequest::OperationReadiness(r) = &plan.request else {
        unreachable!()
    };
    let listed = [
        CheckId::ConnectionEngineProtocol,
        CheckId::ConnectionGroupTypes,
    ];
    let now = chrono::Utc::now();

    let probe = FakeProbe::new().with_api_versions(&KAFKA_4_3);
    let rows = capability_rows(
        &listed,
        &r.connection,
        &probe,
        &[],
        (true, true),
        logweir::check::Deadline::new(0),
        now,
    );
    assert_eq!(rows.len(), 2);
    for row in &rows {
        assert_eq!(
            (row.state, row.code),
            (CheckState::Unknown, CheckCode::ApiVersionsNotObserved),
            "{row:?}"
        );
        assert!(
            row.message.contains("was not asked"),
            "the row says it did not ask: {}",
            row.message
        );
    }
    assert_eq!(
        probe.api_versions_calls(),
        0,
        "no dial after the check's deadline"
    );

    let rows = capability_rows(
        &listed,
        &r.connection,
        &probe,
        &[],
        (true, true),
        logweir::check::Deadline::new(60),
        now,
    );
    assert!(
        rows.iter().all(|row| row.state == CheckState::Ready),
        "{rows:?}"
    );
    assert_eq!(probe.api_versions_calls(), 1);
}

/// `connection.groupTypes`. PRESENT from ListGroups v5 (Apache Kafka 3.9 and
/// 4.x). ABSENT below it (Redpanda v26.2.4 and Apache Kafka 3.7.1, measured):
/// the row says what a backup will record for a selected group, and the
/// fallback; it ADVISES, so the verdict stays `ready`.
#[test]
fn connection_group_types_says_when_a_group_cannot_be_typed() {
    let plan = || {
        mount(&capability_plan(
            vec![],
            vec![CheckId::ConnectionGroupTypes],
        ))
    };
    let run = drive(
        &plan(),
        &capability_wiring(FakeProbe::new().with_api_versions(&KAFKA_4_3)),
    );
    let row = run.row(CheckId::ConnectionGroupTypes);
    assert_eq!(
        (row.state, row.code),
        (CheckState::Ready, CheckCode::GroupTypesListed),
        "{row:?}"
    );
    assert!(row.message.contains("ListGroups v0-v5"), "{}", row.message);

    let run = drive(
        &plan(),
        &capability_wiring(FakeProbe::new().with_api_versions(&REDPANDA_26_2)),
    );
    let row = run.row(CheckId::ConnectionGroupTypes);
    assert_eq!(
        (row.state, row.code, row.gating),
        (
            CheckState::NotReady,
            CheckCode::GroupTypesNotListed,
            Gating::Advisory
        ),
        "{row:?}"
    );
    assert!(
        row.message.contains("ListGroups v0-v4") && row.message.contains("GroupTypeNotCaptured"),
        "{}",
        row.message
    );
    assert!(
        row.remedy.contains("ListGroups v5") && row.remedy.contains("never as offset 0"),
        "{}",
        row.remedy
    );
    // Advisory: a warning beside a verdict it does not change.
    assert_eq!(
        logweir_core::check_contract::aggregate(&run.result().checks),
        logweir_core::check_contract::OverallState::Ready
    );
    assert_eq!(
        logweir_core::check_contract::advisory_warnings(&run.result().checks)
            .iter()
            .map(|c| c.id)
            .collect::<Vec<_>>(),
        vec![CheckId::ConnectionGroupTypes]
    );

    let run = drive(&plan(), &capability_wiring(FakeProbe::new()));
    let row = run.row(CheckId::ConnectionGroupTypes);
    assert_eq!(
        (row.state, row.code),
        (CheckState::Unknown, CheckCode::ApiVersionsNotObserved),
        "{row:?}"
    );
}

/// `connection.topicConfigsReadable`. PRESENT when DescribeConfigs answers
/// for every selected topic. ABSENT when one is refused (FX-4's `acl` fixture,
/// measured): the row names the topic, says the backup records it as
/// `captureDenied`, and gives the grant and the alternative. A read that did
/// not answer is unknown; topics the principal cannot even describe leave the
/// row blocked on that one.
#[test]
fn connection_topic_configs_readable_names_the_topic_whose_read_is_refused() {
    let listed = vec![CheckId::ConnectionTopicConfigsReadable];
    let both_present = || {
        FakeProbe::new()
            .with_presence("orders", TopicPresence::Present { partitions: 6 })
            .with_presence("payments", TopicPresence::Present { partitions: 3 })
    };
    let m = mount(&capability_plan(vec!["orders", "payments"], listed.clone()));

    let run = drive(&m, &capability_wiring(both_present()));
    let row = run.row(CheckId::ConnectionTopicConfigsReadable);
    assert_eq!(
        (row.state, row.code),
        (CheckState::Ready, CheckCode::TopicConfigsReadable),
        "{row:?}"
    );

    let run = drive(
        &m,
        &capability_wiring(both_present().failing_topic_configs(
            "payments",
            CheckCode::TopicAuthorizationFailed,
            "DescribeConfigs on topic payments answered with no configuration",
        )),
    );
    let row = run.row(CheckId::ConnectionTopicConfigsReadable);
    assert_eq!(
        (row.state, row.code, row.gating),
        (
            CheckState::NotReady,
            CheckCode::TopicConfigsNotReadable,
            Gating::Advisory
        ),
        "{row:?}"
    );
    assert!(
        row.message.contains("1 of 2") && row.message.contains("captureDenied"),
        "{}",
        row.message
    );
    assert_eq!(
        row.detail.as_ref().unwrap()["sample"],
        serde_json::json!(["payments"])
    );
    assert!(
        row.remedy.contains("Grant this principal DescribeConfigs")
            && row.remedy.contains("not recorded"),
        "{}",
        row.remedy
    );
    assert_eq!(
        logweir_core::check_contract::aggregate(&run.result().checks),
        logweir_core::check_contract::OverallState::Ready,
        "the backup still runs: the row advises"
    );

    // NO TOPIC SELECTED: nothing was read, so the row is not `ready` (review
    // L12: it was, with the words "the operation selects no topic by name").
    // It is `unknown` and says no configuration was read. Advisory, so the
    // verdict is not moved.
    let none = mount(&capability_plan(vec![], listed.clone()));
    let probe = both_present();
    let run = drive(&none, &capability_wiring(probe));
    let row = run.row(CheckId::ConnectionTopicConfigsReadable);
    assert_eq!(
        (row.state, row.code, row.gating),
        (
            CheckState::Unknown,
            CheckCode::BlockedByPrerequisite,
            Gating::Advisory
        ),
        "a row that read nothing does not answer `ready`: {row:?}"
    );
    assert!(
        row.message.contains("no topic's configuration was read"),
        "{}",
        row.message
    );
    assert!(row.remedy.contains("Name the topics"), "{}", row.remedy);

    // A read that timed out is "could not tell", never "readable" or "refused".
    let run = drive(
        &m,
        &capability_wiring(both_present().failing_topic_configs(
            "orders",
            CheckCode::MetadataTimeout,
            "no answer",
        )),
    );
    let row = run.row(CheckId::ConnectionTopicConfigsReadable);
    assert_eq!(
        (row.state, row.code),
        (CheckState::Unknown, CheckCode::MetadataTimeout),
        "{row:?}"
    );

    // A selected topic that is not describable: no configuration read is
    // attempted, and the row points at the row to fix first.
    let hidden = FakeProbe::new().default_presence(TopicPresence::NotAuthorized);
    let run = drive(&m, &capability_wiring(hidden.clone()));
    let row = run.row(CheckId::ConnectionTopicConfigsReadable);
    assert_eq!(
        (row.state, row.code),
        (CheckState::Unknown, CheckCode::BlockedByPrerequisite),
        "{row:?}"
    );
    assert!(hidden.topic_config_calls().is_empty());
}

/// **PROD-01.2: a bootstrap that answers and advertised brokers that do not**
/// (measured on the `confluent` profile's OFFNET listener, which advertises
/// `127.0.0.1:1`). The cluster id is read off the bootstrap connection and the
/// listing then cannot reach a broker: the row stays `BrokerUnreachable` and
/// blocking, and says it is the ADVERTISED address, with a remedy about
/// `advertised.listeners` instead of the generic one about the bootstrap
/// address, which is the one thing that works.
///
/// CONTROLS: a connection whose cluster id read fails too keeps the generic
/// remedy; and a listing that fails for another reason (authorization) keeps
/// its own.
#[test]
fn an_unreachable_advertised_address_is_named_as_one() {
    let m = mount(&readiness_plan(vec!["orders"], false, None));
    let advertised_away = FakeProbe::new().failing_listing(
        CheckCode::BrokerUnreachable,
        "all-topics metadata reported BrokerUnreachable",
    );
    let run = drive(&m, &capability_wiring(advertised_away));
    let row = run.row(CheckId::ConnectionAuthenticated);
    assert_eq!(
        (row.state, row.code, row.gating),
        (
            CheckState::NotReady,
            CheckCode::BrokerUnreachable,
            Gating::Blocking
        ),
        "{row:?}"
    );
    assert!(
        row.message.contains("named cluster M29I2S7FQPyHBEX12Vx7XA")
            && row
                .message
                .contains("advertised listeners are not reachable"),
        "{}",
        row.message
    );
    assert!(
        row.remedy.contains("advertised.listeners") && !row.remedy.ends_with('…'),
        "{}",
        row.remedy
    );
    assert_eq!(
        row.facts.get("clusterId").map(String::as_str),
        Some("M29I2S7FQPyHBEX12Vx7XA")
    );
    assert_eq!(
        run.row(CheckId::ConnectionTopicsDescribable).code,
        CheckCode::BlockedByPrerequisite
    );

    // CONTROL: nothing answered at all. The generic remedy, about the
    // bootstrap address, is the right one.
    let dead = FakeProbe::new().failing_cluster_id(CheckCode::BrokerUnreachable, "no route");
    let row = drive(&m, &capability_wiring(dead)).row(CheckId::ConnectionAuthenticated);
    assert_eq!(row.code, CheckCode::BrokerUnreachable);
    assert!(
        row.remedy.contains("bootstrap addresses") && !row.remedy.contains("advertised.listeners"),
        "{}",
        row.remedy
    );
    // CONTROL: the listing failed for another reason.
    let denied = FakeProbe::new().failing_listing(
        CheckCode::ClusterAuthorizationFailed,
        "all-topics metadata reported ClusterAuthorizationFailed",
    );
    let row = drive(&m, &capability_wiring(denied)).row(CheckId::ConnectionAuthenticated);
    assert_eq!(row.code, CheckCode::ClusterAuthorizationFailed);
    assert!(
        !row.remedy.contains("advertised.listeners"),
        "{}",
        row.remedy
    );
}

/// A connection that does not authenticate leaves every listed capability row
/// blocked on that, and observes nothing.
#[test]
fn a_connection_that_does_not_authenticate_blocks_every_capability_row() {
    let m = mount(&capability_plan(vec!["orders"], backup_capabilities()));
    let run = drive(
        &m,
        &FakeWiring::default().broker_fails(CheckCode::AuthenticationFailed, "SASL refused"),
    );
    for id in backup_capabilities() {
        let row = run.row(id);
        assert_eq!(
            (row.state, row.code),
            (CheckState::Unknown, CheckCode::BlockedByPrerequisite),
            "{row:?}"
        );
    }
    let probe = FakeProbe::new().failing_cluster_id(CheckCode::AuthenticationFailed, "refused");
    let run = drive(&m, &capability_wiring(probe.clone()));
    for id in backup_capabilities() {
        assert_eq!(run.row(id).code, CheckCode::BlockedByPrerequisite);
    }
    assert_eq!(probe.api_versions_calls(), 0);
}

#[test]
fn a_selected_topic_that_is_absent_is_not_ready_and_one_that_is_hidden_is_unknown() {
    let m = mount(&readiness_plan(vec!["gone"], false, None));
    let run = drive(
        &m,
        &FakeWiring::default()
            .with_probe(FakeProbe::new().default_presence(TopicPresence::NotFound)),
    );
    let row = run.row(CheckId::ConnectionTopicsDescribable);
    assert_eq!(row.code, CheckCode::TopicNotFound);
    assert_eq!(row.state, CheckState::NotReady);
    assert_eq!(row.detail.as_ref().unwrap()["sample"][0], "gone");

    let run = drive(
        &m,
        &FakeWiring::default()
            .with_probe(FakeProbe::new().default_presence(TopicPresence::NotAuthorized)),
    );
    let row = run.row(CheckId::ConnectionTopicsDescribable);
    assert_eq!(row.code, CheckCode::TopicNotAuthorized);
    assert_eq!(
        row.state,
        CheckState::NotReady,
        "a principal that cannot DESCRIBE a selected topic cannot back it up"
    );
}

#[test]
fn a_connection_that_does_not_authenticate_blocks_the_topic_row() {
    let m = mount(&readiness_plan(vec!["orders"], false, None));
    let run = drive(
        &m,
        &FakeWiring::default().with_probe(FakeProbe::new().failing_cluster_id(
            CheckCode::AuthenticationFailed,
            "the broker refused the SASL handshake",
        )),
    );
    assert_eq!(
        run.row(CheckId::ConnectionAuthenticated).code,
        CheckCode::AuthenticationFailed
    );
    let blocked = run.row(CheckId::ConnectionTopicsDescribable);
    assert_eq!(blocked.code, CheckCode::BlockedByPrerequisite);
    assert_eq!(blocked.state, CheckState::Unknown);
}

/// **D2 §6.3's acceptance sentence names "check time AND scope".** The row an
/// operator reads is the one that FAILED, and both of `authenticated`'s early
/// returns used to drop the scope on the floor — so `connection.authenticated
/// notReady` reached the live status with `scope: null` while the blocked
/// `topicsDescribable` beside it carried one
/// (`objects/s14/notready-rows.json`).
///
/// Both probes, because the two returns are separate lines and a fix to one is
/// not a fix to the other.
#[test]
fn a_connection_row_that_fails_still_names_its_principal() {
    for probe in [
        FakeProbe::new().failing_cluster_id(CheckCode::BrokerUnreachable, "no broker answered"),
        FakeProbe::new().failing_listing(CheckCode::AuthenticationFailed, "the broker refused"),
    ] {
        let m = mount(&readiness_plan(vec!["orders"], false, None));
        let run = drive(&m, &FakeWiring::default().with_probe(probe));
        let row = run.row(CheckId::ConnectionAuthenticated);
        assert_eq!(row.state, CheckState::NotReady, "{:?}", row.code);
        let scope = row
            .scope
            .as_ref()
            .unwrap_or_else(|| panic!("`{}` carries no scope", row.code));
        assert_eq!(scope.kind, "KafkaCluster");
        assert_eq!(
            scope.name,
            connection().principal,
            "the scope names the identity this check actually exercised"
        );
    }
}

#[test]
fn a_metadata_timeout_is_unknown_and_not_not_ready() {
    let m = mount(&readiness_plan(vec![], false, None));
    let run = drive(
        &m,
        &FakeWiring::default().with_probe(
            FakeProbe::new().failing_listing(CheckCode::MetadataTimeout, "no metadata in time"),
        ),
    );
    let row = run.row(CheckId::ConnectionAuthenticated);
    assert_eq!(row.code, CheckCode::MetadataTimeout);
    assert_eq!(
        row.state,
        CheckState::Unknown,
        "`I could not tell` is not `it is broken`"
    );
    assert_eq!(
        logweir_core::check_contract::aggregate(&run.result().checks),
        logweir_core::check_contract::OverallState::Unknown
    );
}

#[test]
fn a_missing_signing_key_and_an_unusable_one_are_different_codes() {
    let m = mount(&readiness_plan(vec![], false, Some("/signing/absent.pem")));
    let run = drive(
        &m,
        &FakeWiring::default()
            .with_probe(FakeProbe::new())
            .with_signer(Err(
                "signing prerequisite `/signing/absent.pem` is not ready".to_string(),
            )),
    );
    assert_eq!(
        run.row(CheckId::SignerPrivateKeyUsable).code,
        CheckCode::SigningKeyMissing
    );

    let dir = tempfile::tempdir().unwrap();
    let present = dir.path().join("key.pem");
    std::fs::write(&present, b"not a pem").unwrap();
    let mut plan = readiness_plan(vec![], false, None);
    if let CheckRequest::OperationReadiness(r) = &mut plan.request {
        r.signer_path = Some(present.to_string_lossy().to_string());
    }
    let m = mount(&plan);
    let run = drive(
        &m,
        &FakeWiring::default()
            .with_probe(FakeProbe::new())
            .with_signer(Err("the key could not be parsed".to_string())),
    );
    assert_eq!(
        run.row(CheckId::SignerPrivateKeyUsable).code,
        CheckCode::SigningKeyInvalid
    );
}

/// The shipped wiring runs the SAME readiness probe the execution path does —
/// parse, sign, verify — over the checked-in fixture key.
#[test]
fn the_live_signer_probe_publishes_the_public_key_id() {
    let key = repo_root().join("e2e/fixtures/signed/signing.pem");
    assert!(
        key.exists(),
        "the checked-in signing fixture moved; this row must not SKIP, because a probe that \
         skips proves nothing about the one path that really loads a key"
    );
    let id = kinds::Live
        .signer_key_id(&key.to_string_lossy())
        .expect("the checked-in fixture key is usable");
    assert_eq!(
        id.len(),
        64,
        "a key id is a hex sha256 of the SPKI DER: {id}"
    );
    assert!(id.chars().all(|c| c.is_ascii_hexdigit()));
}

#[test]
fn a_skipped_check_is_not_relayed() {
    let mut plan = readiness_plan(vec!["orders"], false, None);
    if let CheckRequest::OperationReadiness(r) = &mut plan.request {
        r.skip_checks = vec![CheckId::ConnectionTopicsDescribable];
    }
    let m = mount(&plan);
    let run = drive(
        &m,
        &FakeWiring::default()
            .with_probe(FakeProbe::new())
            .with_role(DestinationRole::ArchiveRead, FakeObjects::new())
            .with_role(DestinationRole::EvidenceRead, FakeObjects::new()),
    );
    assert!(!run.has(CheckId::ConnectionTopicsDescribable));
    assert!(run.has(CheckId::ConnectionAuthenticated));
}

// ===========================================================================
// 6. `evidenceFetch`
// ===========================================================================

fn fetch_plan(key: &str, max_bytes: u64) -> CheckPlan {
    plan_of(CheckRequest::EvidenceFetch(EvidenceFetchRequest {
        destination: destination(),
        objects: vec![EvidenceObjectRequest {
            role: DestinationRole::EvidenceRead,
            key: key.to_string(),
            max_bytes,
            stream: Stream::EvidencePayload,
        }],
    }))
}

#[test]
fn an_evidence_fetch_relays_the_object_and_its_digest() {
    let key = "logweir/backups/20260916/receipt.json";
    let body = br#"{"contract":"logweir.dev/backup-receipt/v1"}"#;
    let m = mount(&fetch_plan(key, 1 << 20));
    let run = drive(
        &m,
        &FakeWiring::default().with_role(
            DestinationRole::EvidenceRead,
            FakeObjects::new().with_object(key, body),
        ),
    );
    assert_eq!(run.code, ExitCode::Ok);
    let relay = run.relay.as_ref().expect("the relay decodes");
    assert_eq!(
        relay
            .stream(Stream::EvidencePayload)
            .expect("a payload stream"),
        body
    );
    let e = &run.result().evidence[0];
    assert!(e.present);
    assert!(!e.truncated);
    assert_eq!(e.bytes, Some(body.len() as u64));
    assert_eq!(
        e.sha256.as_deref(),
        Some(logweir_core::ids::sha256_prefixed(body).as_str())
    );
}

/// `present: false` requires a `NotFound`; a denial is `present: false` WITH a
/// code and is never a claim of absence.
#[test]
fn an_evidence_fetch_tells_absence_from_denial() {
    let key = "logweir/backups/20260916/receipt.json";
    let m = mount(&fetch_plan(key, 1 << 20));

    let run = drive(
        &m,
        &FakeWiring::default().with_role(DestinationRole::EvidenceRead, FakeObjects::new()),
    );
    let e = &run.result().evidence[0];
    assert!(!e.present);
    assert_eq!(e.code, Some(CheckCode::ObjectNotFound));

    let run = drive(
        &m,
        &FakeWiring::default().with_role(
            DestinationRole::EvidenceRead,
            FakeObjects::new().failing_get(Fault::Io(s3_refusal("403 Forbidden", "AccessDenied"))),
        ),
    );
    let e = &run.result().evidence[0];
    assert!(!e.present);
    assert_eq!(e.code, Some(CheckCode::AccessDenied));
}

/// **FX-31: an object over `maxBytes` is never read past the cap.** It is
/// reported present and `truncated`, with NO bytes relayed and no digest: the
/// controller refuses a truncated object whatever was relayed, so a prefix was
/// never worth relaying, and the read itself is made UNDER the plan's cap (the
/// fake records it), where the pre-FX-31 Job read the whole object and then cut
/// it. An object exactly at the cap is relayed whole.
///
/// KILLS: "read whole, then truncate" (the read's cap is not `maxBytes`);
/// "relay a prefix" (a stream and a digest appear).
#[test]
fn an_evidence_fetch_over_max_bytes_is_truncated_relays_nothing_and_reads_under_the_cap() {
    let key = "logweir/backups/20260916/receipt.json";
    let body = vec![b'x'; 4096];
    let m = mount(&fetch_plan(key, 100));
    let objects = FakeObjects::new().with_object(key, &body);
    let run = drive(
        &m,
        &FakeWiring::default().with_role(DestinationRole::EvidenceRead, objects.clone()),
    );
    let e = &run.result().evidence[0];
    assert!(e.present, "the object is there");
    assert!(e.truncated, "and over the plan's cap");
    assert_eq!(e.bytes, None, "nothing was relayed for it");
    assert_eq!(e.sha256, None, "so there is no digest to declare");
    assert_eq!(e.code, None);
    assert!(
        run.relay
            .as_ref()
            .unwrap()
            .stream(Stream::EvidencePayload)
            .is_none(),
        "no prefix is relayed"
    );
    assert_eq!(
        objects.read_caps(),
        vec![(key.to_string(), 100)],
        "the read itself is bounded by the plan's maxBytes"
    );

    // AT the cap: relayed whole.
    let m = mount(&fetch_plan(key, 4096));
    let run = drive(
        &m,
        &FakeWiring::default().with_role(
            DestinationRole::EvidenceRead,
            FakeObjects::new().with_object(key, &body),
        ),
    );
    let e = &run.result().evidence[0];
    assert!(!e.truncated);
    assert_eq!(e.bytes, Some(4096));
}

/// The same refusal through a REAL `Store` that reports a small size and
/// streams more: the running cap cuts it off, and the relay is the same
/// truncated, byte-less answer.
#[test]
fn an_evidence_fetch_from_a_store_that_misreports_its_size_relays_nothing() {
    let key = "logweir/backups/20260916/receipt.json";
    let (store, _meter) =
        logweir_engine_oso::storage::Store::in_memory_misreporting_size("logweir/", 10);
    store
        .put_create_only(key, &vec![b'x'; 4096])
        .expect("the fixture is written");
    let m = mount(&fetch_plan(key, 100));
    let wiring = FakeWiring {
        shared_store: Some(Arc::new(store)),
        ..FakeWiring::default()
    };
    let run = drive(&m, &wiring);
    let e = &run.result().evidence[0];
    assert!(e.present && e.truncated, "{e:?}");
    assert_eq!((e.bytes, e.sha256.as_deref()), (None, None));
}

/// A handle that will not build fails every object with one code, and no
/// stream is relayed.
#[test]
fn an_evidence_fetch_whose_handle_fails_relays_nothing() {
    let m = mount(&fetch_plan("logweir/x", 1024));
    let run = drive(
        &m,
        &FakeWiring::default().role_fails(
            DestinationRole::EvidenceRead,
            CheckCode::WorkloadIdentityNotInjected,
            "no web identity was injected",
        ),
    );
    let e = &run.result().evidence[0];
    assert_eq!(e.code, Some(CheckCode::WorkloadIdentityNotInjected));
    assert!(run
        .relay
        .as_ref()
        .unwrap()
        .stream(Stream::EvidencePayload)
        .is_none());
    assert_eq!(run.code, ExitCode::Ok, "an end line was printed");
}

// ===========================================================================
// 7. `restorePreflight`
// ===========================================================================

const PLAN_FILE: &str = "/check/plan.yaml";
/// The destination's `storage.prefix`, as `location()` declares it.
const ARCHIVE_PREFIX: &str = "kafka-backups";
/// The manifest key the CONTROLLER renders into a check plan: `<backupId>/
/// manifest.json`, the archive's own convention, RELATIVE to the prefix
/// (`build_job_shape`, and `Store::qualify`'s header). The runner joins the
/// prefix — this fixture used to carry it already, which is exactly how
/// D2-PREFLIGHT-PREFIX went unnoticed by every test in this file.
const MANIFEST_KEY: &str = "20260915T030000Z/manifest.json";
/// Where that manifest actually IS in the bucket.
const MANIFEST_OBJECT_KEY: &str = "kafka-backups/20260915T030000Z/manifest.json";
const BACKUP_ID: &str = "20260915T030000Z";

/// A restore spec whose recovery point sits inside the fixture manifest's
/// window.
fn restore_yaml(point_in_time: &str, topics: &[&str], mode: &str) -> String {
    let list = topics
        .iter()
        .map(|t| format!("    - {t}\n"))
        .collect::<String>();
    let mut y = String::new();
    y.push_str("source:\n");
    y.push_str("  storage:\n");
    y.push_str("    backend: s3\n");
    y.push_str("    bucket: lw-archive\n");
    y.push_str("    prefix: kafka-backups\n");
    y.push_str("    region: us-east-1\n");
    y.push_str("    endpoint: https://minio.example:9000\n");
    y.push_str("    path_style: true\n");
    y.push_str("    allow_http: false\n");
    y.push_str(&format!("  backup: {BACKUP_ID}\n"));
    y.push_str("  topics:\n");
    y.push_str(&list);
    y.push_str("target:\n");
    y.push_str("  bootstrap_servers:\n");
    y.push_str("    - target.example:9092\n");
    y.push_str(&format!("  mode: {mode}\n"));
    y.push_str("  marker_topic: logweir.scratch\n");
    y.push_str("  topic_mapping_prefix: 'restore-'\n");
    y.push_str("  default_replication_factor: 1\n");
    y.push_str("sample:\n");
    y.push_str("  window_start: 2026-09-15T00:00:00Z\n");
    y.push_str("  window_end: 2026-09-15T06:00:00Z\n");
    y.push_str("restore:\n");
    y.push_str(&format!("  point_in_time: {point_in_time}\n"));
    y.push_str("objectives:\n");
    y.push_str("  rpo_seconds: 3600\n");
    y.push_str("  rto_seconds: 3600\n");
    y.push_str("evidence:\n");
    y.push_str("  backend: s3\n");
    y.push_str("  bucket: lw-archive\n");
    y.push_str("  prefix: logweir/\n");
    y.push_str("  region: us-east-1\n");
    y.push_str("  endpoint: https://minio.example:9000\n");
    y.push_str("  path_style: true\n");
    y.push_str("  allow_http: false\n");
    y
}

/// The manifest fixture: one topic, one partition, two segments covering
/// 2026-09-15T01:00Z .. 04:00Z.
fn manifest_json() -> serde_json::Value {
    serde_json::json!({
        "topics": [{
            "name": "orders",
            "partitions": [{
                "partition_id": 0,
                "segments": [
                    {"key": "20260915T030000Z/topics/orders/partition=0/segment-0.bin",
                     "start_timestamp": 1_757_898_000_000i64,
                     "end_timestamp":   1_757_901_600_000i64},
                    {"key": "20260915T030000Z/topics/orders/partition=0/segment-1.bin",
                     "start_timestamp": 1_757_901_600_001i64,
                     "end_timestamp":   1_757_908_800_000i64}
                ]
            }]
        }]
    })
}

/// epoch-ms inside the fixture window.
const INSIDE_MS: i64 = 1_757_905_000_000;

fn ms_to_rfc3339(ms: i64) -> String {
    Utc.timestamp_millis_opt(ms)
        .unwrap()
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

fn restore_plan(yaml: &str, sha_override: Option<&str>) -> CheckPlan {
    plan_of(CheckRequest::RestorePreflight(Box::new(
        RestorePreflightRequest {
            plan_file: PLAN_FILE.to_string(),
            plan_sha256: sha_override
                .map(ToString::to_string)
                .unwrap_or_else(|| logweir_core::ids::sha256_prefixed(yaml.as_bytes())),
            target: connection(),
            source_destination: destination(),
            evidence_destination: None,
            backup_id: BACKUP_ID.to_string(),
            manifest_key: MANIFEST_KEY.to_string(),
            checks: Vec::new(),
            skip_checks: Vec::new(),
            capability_checks: Vec::new(),
        },
    )))
}

/// The archive handle a healthy restore preflight sees: the manifest plus both
/// segments, under the destination's prefix.
fn archive_objects(manifest: &serde_json::Value) -> FakeObjects {
    let mut o = FakeObjects::new()
        .with_prefix(ARCHIVE_PREFIX)
        .with_object(MANIFEST_OBJECT_KEY, &serde_json::to_vec(manifest).unwrap());
    for i in 0..2 {
        o = o.with_object(
            &format!("kafka-backups/{BACKUP_ID}/topics/orders/partition=0/segment-{i}.bin"),
            b"segment",
        );
    }
    o
}

fn restore_wiring(yaml: &str, manifest: &serde_json::Value, probe: FakeProbe) -> FakeWiring {
    FakeWiring::default()
        .with_file(PLAN_FILE, yaml.as_bytes())
        .with_probe(probe)
        .with_role(DestinationRole::ArchiveRead, archive_objects(manifest))
}

/// **M2, the restore half.** The plan bytes are hashed before anything else,
/// and a mismatch dials nothing and reads nothing.
#[test]
fn a_restore_preflight_with_a_bad_plan_hash_opens_nothing() {
    let yaml = restore_yaml(&ms_to_rfc3339(INSIDE_MS), &["orders"], "scratch");
    let plan = restore_plan(
        &yaml,
        Some(&logweir_core::ids::sha256_prefixed(b"other bytes")),
    );
    let m = mount(&plan);
    let probe = FakeProbe::new();
    let archive = archive_objects(&manifest_json());
    let run = drive(
        &m,
        &FakeWiring::default()
            .with_file(PLAN_FILE, yaml.as_bytes())
            .with_probe(probe.clone())
            .with_role(DestinationRole::ArchiveRead, archive.clone()),
    );
    let row = run.row(CheckId::PlanParse);
    assert_eq!(row.code, CheckCode::PlanHashMismatch);
    assert_eq!(row.state, CheckState::NotReady);
    assert_eq!(
        ids(&run.result()),
        vec!["runner.contract", "plan.parse"],
        "every later row is a claim about these bytes, so none of them ran"
    );
    assert!(probe.describe_calls().is_empty(), "the target was dialled");
    assert!(probe.create_calls().is_empty());
    assert_eq!(run.code, ExitCode::Ok);
}

#[test]
fn a_restore_plan_that_is_not_a_spec_is_plan_unparseable() {
    let yaml = "this: is: not: a: spec\n";
    let m = mount(&restore_plan(yaml, None));
    let run = drive(
        &m,
        &restore_wiring(yaml, &manifest_json(), FakeProbe::new()),
    );
    assert_eq!(run.row(CheckId::PlanParse).code, CheckCode::PlanUnparseable);
}

#[test]
fn a_healthy_restore_preflight_reports_every_row_it_owns() {
    let yaml = restore_yaml(&ms_to_rfc3339(INSIDE_MS), &["orders"], "scratch");
    let m = mount(&restore_plan(&yaml, None));
    let probe = FakeProbe::new()
        .with_presence("logweir.scratch", TopicPresence::Present { partitions: 1 })
        .default_presence(TopicPresence::NotFound);
    let run = drive(&m, &restore_wiring(&yaml, &manifest_json(), probe.clone()));
    assert_eq!(run.code, ExitCode::Ok);
    let got: BTreeSet<&str> = ids(&run.result()).into_iter().collect();
    let want: BTreeSet<&str> = [
        "runner.contract",
        "plan.parse",
        "archive.backupSet",
        "archive.coverage",
        "archive.segments",
        "target.authenticated",
        "target.scratchMarker",
        "target.mappedTopics",
        "target.topicCreate",
        "target.timestampBound",
        "target.logAppendTime",
    ]
    .into_iter()
    .collect();
    assert_eq!(got, want);
    assert_eq!(
        logweir_core::check_contract::aggregate(&run.result().checks),
        logweir_core::check_contract::OverallState::Ready
    );
    // The controller's rows are NOT here: a credential-holding Job cannot read
    // a TrustRoster, an Approval or a recovery point.
    for absent in [
        CheckId::PlanBindings,
        CheckId::PlanNames,
        CheckId::RecoveryPointState,
        CheckId::ApprovalState,
        CheckId::TargetClusterIdentity,
        CheckId::SignerRostered,
    ] {
        assert!(!run.has(absent), "`{absent}` is the controller's row");
    }
    // D2 §6.3's five-minute expiry on the two collision rows.
    let c = run.row(CheckId::TargetMappedTopics);
    assert_eq!(
        c.expires_at.unwrap() - c.observed_at.unwrap(),
        chrono::Duration::minutes(5)
    );
    // ...and thirty minutes on the archive rows, which read immutable objects.
    let a = run.row(CheckId::ArchiveBackupSet);
    assert_eq!(
        a.expires_at.unwrap() - a.observed_at.unwrap(),
        chrono::Duration::minutes(30)
    );
}

/// **PROD-01.2: `target.engineProtocol`, the row Redpanda v26.2.4 is missing.**
/// A replay sends Metadata v9 and Produce v8. PRESENT on Apache Kafka's
/// ranges: ready, and the verdict is `ready`. ABSENT on Redpanda's (Produce
/// v0-v7, measured: the restore then fails with "early eof" after five
/// retries): the row names Produce v8 against v0-v7 and the fallback, and the
/// verdict is `notReady` BEFORE a restore starts. Not listed: not emitted.
#[test]
fn target_engine_protocol_refuses_an_endpoint_that_does_not_serve_the_engines_produce() {
    let yaml = restore_yaml(&ms_to_rfc3339(INSIDE_MS), &["orders"], "scratch");
    let listing = |listed: Vec<CheckId>| {
        let mut plan = restore_plan(&yaml, None);
        let CheckRequest::RestorePreflight(r) = &mut plan.request else {
            unreachable!("restore_plan builds a restorePreflight request")
        };
        r.capability_checks = listed;
        mount(&plan)
    };
    let probe = |ranges: &[(i16, i16, i16)]| {
        FakeProbe::new()
            .with_presence("logweir.scratch", TopicPresence::Present { partitions: 1 })
            .default_presence(TopicPresence::NotFound)
            .with_api_versions(ranges)
    };

    // Not listed: an older controller's plan. No row, no observation.
    let unlisted = probe(&REDPANDA_26_2);
    let run = drive(
        &listing(Vec::new()),
        &restore_wiring(&yaml, &manifest_json(), unlisted.clone()),
    );
    assert!(!run.has(CheckId::TargetEngineProtocol));
    assert_eq!(unlisted.api_versions_calls(), 0);

    let m = listing(vec![CheckId::TargetEngineProtocol]);

    // PRESENT.
    let run = drive(
        &m,
        &restore_wiring(&yaml, &manifest_json(), probe(&KAFKA_4_3)),
    );
    let row = run.row(CheckId::TargetEngineProtocol);
    assert_eq!(
        (row.state, row.code, row.gating),
        (
            CheckState::Ready,
            CheckCode::EngineProtocolSupported,
            Gating::Blocking
        ),
        "{row:?}"
    );
    assert_eq!(
        row.facts.get("engineRequests").map(String::as_str),
        Some("Metadata v9, Produce v8")
    );
    assert!(
        row.message.contains("Produce v8 (served v0-v13)"),
        "{}",
        row.message
    );
    assert_eq!(
        logweir_core::check_contract::aggregate(&run.result().checks),
        logweir_core::check_contract::OverallState::Ready
    );
    let got: BTreeSet<&str> = ids(&run.result()).into_iter().collect();
    assert!(
        got.contains("target.engineProtocol") && got.len() == 12,
        "{got:?}"
    );

    // ABSENT: Redpanda v26.2.4.
    let run = drive(
        &m,
        &restore_wiring(&yaml, &manifest_json(), probe(&REDPANDA_26_2)),
    );
    let row = run.row(CheckId::TargetEngineProtocol);
    assert_eq!(
        (row.state, row.code),
        (CheckState::NotReady, CheckCode::EngineProtocolUnsupported),
        "{row:?}"
    );
    assert!(
        row.message
            .contains("Produce v8 and this endpoint serves Produce v0-v7")
            && row.message.contains("never negotiates")
            && row.message.contains("cannot restore into this endpoint"),
        "{}",
        row.message
    );
    assert!(
        row.remedy.contains("can still be a backup source")
            && row
                .remedy
                .contains("restore its archive into a cluster that serves them"),
        "the fallback is actionable: {}",
        row.remedy
    );
    assert_eq!(
        row.detail.as_ref().unwrap()["sample"],
        serde_json::json!(["Produce v8"])
    );
    assert_eq!(
        logweir_core::check_contract::aggregate(&run.result().checks),
        logweir_core::check_contract::OverallState::NotReady,
        "a restore that cannot run is refused by the verdict before it starts"
    );

    // NOT OBSERVED: unknown, and the verdict is not `ready`.
    let unobserved = FakeProbe::new()
        .with_presence("logweir.scratch", TopicPresence::Present { partitions: 1 })
        .default_presence(TopicPresence::NotFound);
    let run = drive(&m, &restore_wiring(&yaml, &manifest_json(), unobserved));
    let row = run.row(CheckId::TargetEngineProtocol);
    assert_eq!(
        (row.state, row.code),
        (CheckState::Unknown, CheckCode::ApiVersionsNotObserved),
        "{row:?}"
    );
    assert_eq!(
        logweir_core::check_contract::aggregate(&run.result().checks),
        logweir_core::check_contract::OverallState::Unknown
    );

    // A target that does not dial: the row is blocked on that.
    let run = drive(
        &m,
        &restore_wiring(&yaml, &manifest_json(), FakeProbe::new())
            .broker_fails(CheckCode::BrokerUnreachable, "no route"),
    );
    assert_eq!(
        run.row(CheckId::TargetEngineProtocol).code,
        CheckCode::BlockedByPrerequisite
    );
}

/// D2 §6.7: **no topic is created**. The collision answer is two read-only
/// probes, and the `CreateTopics` carries the pinned configs.
#[test]
fn a_restore_preflight_validates_creation_and_creates_nothing() {
    let yaml = restore_yaml(&ms_to_rfc3339(INSIDE_MS), &["orders"], "scratch");
    let m = mount(&restore_plan(&yaml, None));
    let probe = FakeProbe::new()
        .with_presence("logweir.scratch", TopicPresence::Present { partitions: 1 })
        .default_presence(TopicPresence::NotFound);
    let run = drive(&m, &restore_wiring(&yaml, &manifest_json(), probe.clone()));
    assert_eq!(
        run.row(CheckId::TargetTopicCreate).code,
        CheckCode::TopicCreateValidated
    );
    let calls = probe.create_calls();
    assert_eq!(calls.len(), 1, "exactly one validate-only request");
    assert_eq!(calls[0][0].name, "restore-orders");
    assert_eq!(calls[0][0].replication_factor, 1);
    assert_eq!(
        calls[0][0].configs,
        logweir_kafka::reader::TARGET_TOPIC_CONFIGS
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect::<Vec<_>>(),
        "the validate-only request carries the configs the run would pin"
    );
    // The mapped name was probed by metadata too — D2 §6.7(a) and (b).
    assert!(probe
        .describe_calls()
        .contains(&"restore-orders".to_string()));
}

#[test]
fn a_mapped_topic_that_exists_blocks_and_one_that_is_hidden_is_unknown() {
    let yaml = restore_yaml(&ms_to_rfc3339(INSIDE_MS), &["orders"], "scratch");
    let m = mount(&restore_plan(&yaml, None));

    let probe = FakeProbe::new()
        .with_presence("logweir.scratch", TopicPresence::Present { partitions: 1 })
        .with_presence("restore-orders", TopicPresence::Present { partitions: 6 });
    let run = drive(&m, &restore_wiring(&yaml, &manifest_json(), probe));
    let row = run.row(CheckId::TargetMappedTopics);
    assert_eq!(row.code, CheckCode::MappedTopicExists);
    assert_eq!(row.state, CheckState::NotReady);
    assert_eq!(row.detail.as_ref().unwrap()["sample"][0], "restore-orders");
    let details = run.relay.as_ref().unwrap().stream(Stream::Details).unwrap();
    assert!(String::from_utf8_lossy(details).contains("restore-orders"));

    let probe = FakeProbe::new()
        .with_presence("logweir.scratch", TopicPresence::Present { partitions: 1 })
        .with_presence("restore-orders", TopicPresence::NotAuthorized);
    let run = drive(&m, &restore_wiring(&yaml, &manifest_json(), probe));
    let row = run.row(CheckId::TargetMappedTopics);
    assert_eq!(row.code, CheckCode::MappedTopicVisibilityUnknown);
    assert_eq!(row.state, CheckState::Unknown);
}

#[test]
fn a_missing_scratch_marker_blocks_a_scratch_restore_and_newtopic_skips_it() {
    let yaml = restore_yaml(&ms_to_rfc3339(INSIDE_MS), &["orders"], "scratch");
    let m = mount(&restore_plan(&yaml, None));
    let run = drive(
        &m,
        &restore_wiring(
            &yaml,
            &manifest_json(),
            FakeProbe::new().default_presence(TopicPresence::NotFound),
        ),
    );
    assert_eq!(
        run.row(CheckId::TargetScratchMarker).code,
        CheckCode::MarkerTopicMissing
    );

    let yaml = restore_yaml(&ms_to_rfc3339(INSIDE_MS), &["orders"], "newTopic");
    let m = mount(&restore_plan(&yaml, None));
    let run = drive(
        &m,
        &restore_wiring(
            &yaml,
            &manifest_json(),
            FakeProbe::new().default_presence(TopicPresence::NotFound),
        ),
    );
    assert!(
        !run.has(CheckId::TargetScratchMarker),
        "the marker proves nothing about a restore into a new topic on a real cluster"
    );
}

#[test]
fn the_recovery_point_is_checked_against_the_sets_window_inclusively() {
    let manifest = manifest_json();
    // AT the floor: the window `[t, t]` holds one instant, which is G-WIN's
    // silent loss — the same `>=` the execution guard applies.
    for (pit_ms, want) in [
        (1_757_898_000_000i64, CheckCode::PointInTimeBeforeCoverage),
        (1_757_897_000_000, CheckCode::PointInTimeBeforeCoverage),
        (1_757_908_800_001, CheckCode::PointInTimeAfterCoverage),
        (1_757_908_800_000, CheckCode::PointInTimeCovered),
        (INSIDE_MS, CheckCode::PointInTimeCovered),
    ] {
        let yaml = restore_yaml(&ms_to_rfc3339(pit_ms), &["orders"], "scratch");
        let m = mount(&restore_plan(&yaml, None));
        let run = drive(&m, &restore_wiring(&yaml, &manifest, FakeProbe::new()));
        assert_eq!(
            run.row(CheckId::ArchiveCoverage).code,
            want,
            "epoch-ms {pit_ms} against [{}, {}]",
            1_757_898_000_000i64,
            1_757_908_800_000i64
        );
    }
}

#[test]
fn a_topic_that_is_not_in_the_backup_set_is_reported_by_name() {
    let yaml = restore_yaml(
        &ms_to_rfc3339(INSIDE_MS),
        &["orders", "payments"],
        "scratch",
    );
    let m = mount(&restore_plan(&yaml, None));
    let run = drive(
        &m,
        &restore_wiring(&yaml, &manifest_json(), FakeProbe::new()),
    );
    let row = run.row(CheckId::ArchiveCoverage);
    assert_eq!(row.code, CheckCode::TopicNotInBackupSet);
    assert_eq!(row.detail.as_ref().unwrap()["sample"][0], "payments");
}

#[test]
fn a_manifest_that_is_absent_is_backup_set_not_found_and_a_stranger_is_unreadable() {
    let yaml = restore_yaml(&ms_to_rfc3339(INSIDE_MS), &["orders"], "scratch");
    let m = mount(&restore_plan(&yaml, None));

    let run = drive(
        &m,
        &FakeWiring::default()
            .with_file(PLAN_FILE, yaml.as_bytes())
            .with_probe(FakeProbe::new())
            .with_role(DestinationRole::ArchiveRead, FakeObjects::new()),
    );
    assert_eq!(
        run.row(CheckId::ArchiveBackupSet).code,
        CheckCode::BackupSetNotFound
    );
    assert_eq!(
        run.row(CheckId::ArchiveCoverage).code,
        CheckCode::BlockedByPrerequisite
    );

    let run = drive(
        &m,
        &FakeWiring::default()
            .with_file(PLAN_FILE, yaml.as_bytes())
            .with_probe(FakeProbe::new())
            .with_role(
                DestinationRole::ArchiveRead,
                FakeObjects::new().with_object(MANIFEST_KEY, br#"{"hello":"world"}"#),
            ),
    );
    assert_eq!(
        run.row(CheckId::ArchiveBackupSet).code,
        CheckCode::ManifestUnreadable
    );
}

#[test]
fn a_segment_the_manifest_names_and_the_archive_lacks_is_reported_with_details() {
    let yaml = restore_yaml(&ms_to_rfc3339(INSIDE_MS), &["orders"], "scratch");
    let m = mount(&restore_plan(&yaml, None));
    let manifest = manifest_json();
    let partial = FakeObjects::new()
        .with_prefix(ARCHIVE_PREFIX)
        .with_object(MANIFEST_OBJECT_KEY, &serde_json::to_vec(&manifest).unwrap())
        .with_object(
            &format!("kafka-backups/{BACKUP_ID}/topics/orders/partition=0/segment-0.bin"),
            b"segment",
        );
    let run = drive(
        &m,
        &FakeWiring::default()
            .with_file(PLAN_FILE, yaml.as_bytes())
            .with_probe(FakeProbe::new())
            .with_role(DestinationRole::ArchiveRead, partial),
    );
    let row = run.row(CheckId::ArchiveSegments);
    assert_eq!(row.code, CheckCode::SegmentMissing);
    assert_eq!(row.detail.as_ref().unwrap()["count"], 1);
    let details = String::from_utf8(
        run.relay
            .as_ref()
            .unwrap()
            .stream(Stream::Details)
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(details.contains("segment-1.bin"), "{details}");
    // D2-REDACT-OVERBROAD: the details document exists to carry the key an
    // operator has to go and recover, and it used to read `[redacted].bin`.
    assert!(
        details.contains(&format!(
            "kafka-backups/{BACKUP_ID}/topics/orders/partition=0/segment-1.bin"
        )),
        "the missing segment's whole key must reach the details stream: {details}"
    );
}

/// **D2-PREFLIGHT-PREFIX.** A destination with a `storage.prefix` — which is
/// every destination the live run used — gets the same answers as a
/// prefix-less one.
///
/// The live finding (`d2w14.result.md` §5.2, `objects/s16/preflight-pf-flat.
/// json`): `dest-a` with `prefix: team/prod` answered `archive.backupSet
/// notReady/AccessDenied` for a manifest `mc` reads as the same principal at
/// `lw-a/team/prod/<id>/manifest.json`, while `dest-c` with no prefix answered
/// `ready/ManifestReadable` for the same set. No restore preflight could be
/// green for any prefixed destination, so a green preview was reachable only
/// prefix-less.
///
/// The fixture places every object where the backup engine writes it —
/// `<prefix>/<backupId>/…`, with the manifest naming its segments RELATIVE —
/// and hands the request the relative `manifest_key` the controller renders.
#[test]
fn a_prefixed_destination_reads_its_manifest_and_its_segments() {
    let yaml = restore_yaml(&ms_to_rfc3339(INSIDE_MS), &["orders"], "scratch");
    let m = mount(&restore_plan(&yaml, None));
    let probe = FakeProbe::new()
        .with_presence("logweir.scratch", TopicPresence::Present { partitions: 1 })
        .default_presence(TopicPresence::NotFound);

    // A DEEP, MULTI-SEGMENT PREFIX, because a one-component one would pass a
    // `trim`-shaped fix by accident.
    let prefix = "team/prod/archives";
    let manifest = manifest_json();
    let mut objects = FakeObjects::new().with_prefix(prefix).with_object(
        &format!("{prefix}/{MANIFEST_KEY}"),
        &serde_json::to_vec(&manifest).unwrap(),
    );
    for i in 0..2 {
        objects = objects.with_object(
            &format!("{prefix}/{BACKUP_ID}/topics/orders/partition=0/segment-{i}.bin"),
            b"segment",
        );
    }
    let run = drive(
        &m,
        &FakeWiring::default()
            .with_file(PLAN_FILE, yaml.as_bytes())
            .with_probe(probe)
            .with_role(DestinationRole::ArchiveRead, objects),
    );

    let set = run.row(CheckId::ArchiveBackupSet);
    assert_eq!(
        set.code,
        CheckCode::ManifestReadable,
        "a prefixed destination could not read its own manifest: {}",
        set.message
    );
    assert_eq!(set.state, CheckState::Ready);
    assert_eq!(
        run.row(CheckId::ArchiveSegments).code,
        CheckCode::SegmentsPresent,
        "`expected` and `listed` were compared in two different key spaces"
    );
    assert_eq!(run.row(CheckId::ArchiveCoverage).state, CheckState::Ready);

    // THE NEGATIVE HALF, in the same key space: the manifest is there and one
    // segment is not, so the row is `SegmentMissing` and names the PREFIXED
    // key. A fix that simply stopped listing would pass the row above.
    let mut partial = FakeObjects::new().with_prefix(prefix).with_object(
        &format!("{prefix}/{MANIFEST_KEY}"),
        &serde_json::to_vec(&manifest).unwrap(),
    );
    partial = partial.with_object(
        &format!("{prefix}/{BACKUP_ID}/topics/orders/partition=0/segment-0.bin"),
        b"segment",
    );
    let run = drive(
        &m,
        &FakeWiring::default()
            .with_file(PLAN_FILE, yaml.as_bytes())
            .with_probe(FakeProbe::new())
            .with_role(DestinationRole::ArchiveRead, partial),
    );
    let row = run.row(CheckId::ArchiveSegments);
    assert_eq!(row.code, CheckCode::SegmentMissing);
    assert_eq!(row.detail.as_ref().unwrap()["count"], 1);
    let details = String::from_utf8(
        run.relay
            .as_ref()
            .unwrap()
            .stream(Stream::Details)
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(
        details.contains(&format!(
            "{prefix}/{BACKUP_ID}/topics/orders/partition=0/segment-1.bin"
        )),
        "the missing key is the one an operator must go and look for: {details}"
    );
}

/// The manifest key a refusal NAMES is the qualified one, so an operator can
/// paste it into `mc` — which is exactly how the live run established that the
/// object was readable and the check was wrong.
#[test]
fn an_unreadable_manifest_names_the_prefixed_key() {
    let yaml = restore_yaml(&ms_to_rfc3339(INSIDE_MS), &["orders"], "scratch");
    let m = mount(&restore_plan(&yaml, None));
    let run = drive(
        &m,
        &FakeWiring::default()
            .with_file(PLAN_FILE, yaml.as_bytes())
            .with_probe(FakeProbe::new())
            .with_role(
                DestinationRole::ArchiveRead,
                FakeObjects::new().with_prefix(ARCHIVE_PREFIX),
            ),
    );
    let row = run.row(CheckId::ArchiveBackupSet);
    assert_eq!(row.code, CheckCode::BackupSetNotFound);
    assert!(
        row.message.contains(MANIFEST_OBJECT_KEY),
        "the message names `{}` rather than the key that was read: {}",
        MANIFEST_OBJECT_KEY,
        row.message
    );
}

/// **FX-31: a manifest over the read cap is refused unread, and the row
/// NAMES the cap** — Logweir's own sentence, never the backend's text.
#[test]
fn a_manifest_over_the_read_cap_is_refused_naming_the_cap() {
    use logweir_engine_oso::storage::caps;
    let yaml = restore_yaml(&ms_to_rfc3339(INSIDE_MS), &["orders"], "scratch");
    let m = mount(&restore_plan(&yaml, None));
    let objects = FakeObjects::new()
        .with_prefix(ARCHIVE_PREFIX)
        .with_object(MANIFEST_OBJECT_KEY, b"{}")
        .reporting_size(MANIFEST_OBJECT_KEY, 5 << 30);
    let run = drive(
        &m,
        &FakeWiring::default()
            .with_file(PLAN_FILE, yaml.as_bytes())
            .with_probe(FakeProbe::new())
            .with_role(DestinationRole::ArchiveRead, objects.clone()),
    );
    let row = run.row(CheckId::ArchiveBackupSet);
    assert_ne!(row.state, CheckState::Ready, "{row:?}");
    assert!(
        row.message.contains(&format!(
            "larger than the {}-byte read cap for a manifest",
            caps::MANIFEST
        )),
        "the row names the cap: {}",
        row.message
    );
    assert_eq!(
        objects.read_caps(),
        vec![(MANIFEST_OBJECT_KEY.to_string(), caps::MANIFEST)],
        "one read, under the manifest cap"
    );
    // REVIEW F7: nothing refused, so the remedy does not blame the store.
    assert!(
        row.remedy.contains("larger than this build reads")
            && !row.remedy.contains("does not classify"),
        "{row:?}"
    );
}

/// The bound arithmetic is the execution guard's, including the
/// `saturating_sub` that keeps the Apache default from wrapping into the
/// future.
#[test]
fn the_timestamp_bound_is_the_execution_guards_arithmetic() {
    let yaml = restore_yaml(&ms_to_rfc3339(INSIDE_MS), &["orders"], "scratch");
    let m = mount(&restore_plan(&yaml, None));

    // The Apache default. A plain subtraction here overflows.
    let run = drive(
        &m,
        &restore_wiring(
            &yaml,
            &manifest_json(),
            FakeProbe::new()
                .with_broker_config("log.message.timestamp.before.max.ms", &i64::MAX.to_string()),
        ),
    );
    assert_eq!(
        run.row(CheckId::TargetTimestampBound).code,
        CheckCode::TimestampWithinBound
    );

    // A one-hour bound against a recovery point from 2026-09-15.
    let run = drive(
        &m,
        &restore_wiring(
            &yaml,
            &manifest_json(),
            FakeProbe::new().with_broker_config("log.message.timestamp.before.max.ms", "3600000"),
        ),
    );
    assert_eq!(
        run.row(CheckId::TargetTimestampBound).code,
        CheckCode::TimestampBoundExceeded
    );

    // The pre-3.6 spelling is read when the new one is absent.
    let run = drive(
        &m,
        &restore_wiring(
            &yaml,
            &manifest_json(),
            FakeProbe::new()
                .without_broker_config("log.message.timestamp.before.max.ms")
                .with_broker_config("log.message.timestamp.difference.max.ms", "3600000"),
        ),
    );
    assert_eq!(
        run.row(CheckId::TargetTimestampBound).code,
        CheckCode::TimestampBoundExceeded
    );
}

/// **PROD-01.2: an answer WITHOUT the bound is "not reported", never "no
/// bound".** Every Apache Kafka broker reports at least one of the two keys;
/// Redpanda v26.2.4's broker resource answers nine keys and neither (it keeps
/// the bound per topic). This row answered `ready`, "the target declares no
/// record-timestamp bound", for it: an empty answer recorded as a fact.
///
/// CONTROL, in the same test: the SAME broker answer plus the key is `ready`,
/// so the unknown is about the missing key and nothing else.
///
/// Negative control (mutant): restore the `ready(TimestampWithinBound)` arm
/// for a missing key and the first assertion fails with `TimestampWithinBound`.
///
/// **The code is one the plan's controller can read.** The code vocabulary is
/// closed on the reading side, and this row is answered for EVERY restore
/// plan. A plan that lists capability rows came from a controller that knows
/// `TimestampBoundNotReported`; a plan that lists none came from an older
/// one, which would refuse the whole result over the new code, so it gets the
/// same `unknown`, message and remedy under `BrokerConfigsNotReadable`.
/// Mutant: answer the new code for both and the older-plan assertion fails.
#[test]
fn a_broker_answer_without_the_timestamp_bound_is_unknown_never_no_bound() {
    let yaml = restore_yaml(&ms_to_rfc3339(INSIDE_MS), &["orders"], "scratch");
    let mut plan = restore_plan(&yaml, None);
    let CheckRequest::RestorePreflight(r) = &mut plan.request else {
        unreachable!("restore_plan builds a restorePreflight request")
    };
    r.capability_checks = vec![CheckId::TargetEngineProtocol];
    let m = mount(&plan);
    // Redpanda's nine broker keys, as measured (values abridged).
    let redpanda_like = || {
        let mut p = FakeProbe::new()
            .with_presence("logweir.scratch", TopicPresence::Present { partitions: 1 })
            .default_presence(TopicPresence::NotFound)
            .without_broker_config("log.message.timestamp.before.max.ms");
        for (k, v) in [
            ("advertised.listeners", "internal://redpanda:9094"),
            ("auto.create.topics.enable", "false"),
            ("default.replication.factor", "1"),
            ("listeners", "internal://0.0.0.0:9094"),
            ("log.dirs", "/var/lib/redpanda/data"),
            ("log.retention.bytes", "18446744073709551615"),
            ("log.retention.ms", "604800000"),
            ("log.segment.bytes", "134217728"),
            ("num.partitions", "1"),
        ] {
            p = p.with_broker_config(k, v);
        }
        p
    };
    let run = drive(
        &m,
        &restore_wiring(&yaml, &manifest_json(), redpanda_like()),
    );
    let row = run.row(CheckId::TargetTimestampBound);
    assert_eq!(
        (row.state, row.code),
        (CheckState::Unknown, CheckCode::TimestampBoundNotReported),
        "{row:?}"
    );
    assert!(row.message.contains("9 key(s)"), "{}", row.message);
    assert!(
        row.remedy.contains("has not declared that it has none")
            && row.remedy.contains("message.timestamp.before.max.ms"),
        "the remedy names the fallback: {}",
        row.remedy
    );
    // A blocking row with no answer keeps the verdict from being `ready`.
    assert_eq!(
        logweir_core::check_contract::aggregate(&run.result().checks),
        logweir_core::check_contract::OverallState::Unknown
    );

    // AN OLDER CONTROLLER'S PLAN (no capability rows listed): the same
    // finding under the code that controller already reads.
    let older = mount(&restore_plan(&yaml, None));
    let older_run = drive(
        &older,
        &restore_wiring(&yaml, &manifest_json(), redpanda_like()),
    );
    let older_row = older_run.row(CheckId::TargetTimestampBound);
    assert_eq!(
        (older_row.state, older_row.code),
        (CheckState::Unknown, CheckCode::BrokerConfigsNotReadable),
        "{older_row:?}"
    );
    assert_eq!(
        (&older_row.message, &older_row.remedy),
        (&row.message, &row.remedy),
        "the same message and remedy under either code"
    );
    assert!(
        older_run
            .result()
            .checks
            .iter()
            .all(|c| !c.id.is_capability()),
        "and no capability row it did not ask for"
    );

    // CONTROL: the same answer WITH the key.
    let run = drive(
        &m,
        &restore_wiring(
            &yaml,
            &manifest_json(),
            redpanda_like()
                .with_broker_config("log.message.timestamp.before.max.ms", "9223372036854"),
        ),
    );
    let row = run.row(CheckId::TargetTimestampBound);
    assert_eq!(
        (row.state, row.code),
        (CheckState::Ready, CheckCode::TimestampWithinBound),
        "{row:?}"
    );
}

/// **FX-4 / T13, consumer 1 — the false-green readiness row.** A principal
/// without DescribeConfigs on the cluster used to get an EMPTY broker
/// configuration back (rdkafka 0.36.2 never reads the per-resource error), and
/// this row read "no bound declared" as READY. The inventory now returns the
/// refusal; this row must take its `Err` arm and say UNKNOWN.
///
/// Negative control: make the row read a refused configuration as an empty
/// map (`probe.broker_configs().unwrap_or_default()`) and this test fails with
/// `TimestampWithinBound` — the pre-fix behaviour.
#[test]
fn a_refused_broker_configuration_read_is_unknown_never_within_bound() {
    let yaml = restore_yaml(&ms_to_rfc3339(INSIDE_MS), &["orders"], "scratch");
    let m = mount(&restore_plan(&yaml, None));
    let run = drive(
        &m,
        &restore_wiring(
            &yaml,
            &manifest_json(),
            FakeProbe::new().failing_broker_configs(
                CheckCode::ClusterAuthorizationFailed,
                "DescribeConfigs on broker 1001 answered with no configuration",
            ),
        ),
    );
    let row = run.row(CheckId::TargetTimestampBound);
    assert_eq!(
        (row.state, row.code),
        (CheckState::Unknown, CheckCode::BrokerConfigsNotReadable),
        "a refused broker configuration is UNKNOWN, never a green \"no bound declared\": {row:?}"
    );
}

/// The three broker keys this preflight reads are the three the execution
/// guard reads. A preview of a bound nobody enforces is worse than no preview.
#[test]
fn the_broker_config_keys_match_the_execution_guard() {
    let src = std::fs::read_to_string(repo_root().join("crates/logweir/src/drill/phase0_admit.rs"))
        .expect("the execution guard is readable");
    for key in [
        logweir::check::kinds::restore::BROKER_TIMESTAMP_TYPE,
        logweir::check::kinds::restore::BROKER_TIMESTAMP_BEFORE_MAX_MS,
        logweir::check::kinds::restore::BROKER_TIMESTAMP_DIFFERENCE_MAX_MS,
        logweir::check::kinds::restore::LOG_APPEND_TIME,
    ] {
        assert!(
            src.contains(&format!("\"{key}\"")),
            "`phase0_admit.rs` no longer names `{key}`, so the preflight is checking a bound \
             the run does not enforce"
        );
    }
}

#[test]
fn the_details_stream_is_capped_and_says_it_was_truncated() {
    let many: Vec<String> = (0..50_000)
        .map(|i| serde_json::json!({"missingSegment": format!("k{i:08}")}).to_string())
        .collect();
    let bytes = logweir::check::kinds::restore::details_stream(&many);
    assert!(bytes.len() <= logweir::check::kinds::restore::DETAILS_MAX_BYTES + 256);
    let text = String::from_utf8(bytes).unwrap();
    let last = text.lines().last().unwrap();
    let note: serde_json::Value = serde_json::from_str(last).unwrap();
    assert_eq!(note["truncated"], true);
    assert_eq!(note["total"], 50_000);
}

/// The `AlreadyExists` arm of the put itself, reached without seeding the
/// object: some backends answer the precondition rather than the key.
#[test]
fn a_put_that_answers_already_exists_is_write_authorised() {
    let m = mount(&access_plan(vec![DestinationRole::EvidenceWrite], true));
    let run = drive(
        &m,
        &FakeWiring::default().with_writer(FakeObjects::new().failing_put(Fault::AlreadyExists)),
    );
    let row = run.row(CheckId::DestinationEvidenceWritable);
    assert_eq!(row.state, CheckState::Ready);
    assert_eq!(row.code, CheckCode::MarkerAlreadyPresent);
}

/// A backend that answers `NotFound` to a LIST is answering, not denying — an
/// empty prefix on a filesystem-shaped backend takes this arm.
#[test]
fn a_list_that_answers_not_found_is_still_a_refusal_with_its_own_code() {
    let m = mount(&access_plan(vec![DestinationRole::ArchiveRead], false));
    let run = drive(
        &m,
        &FakeWiring::default().with_role(
            DestinationRole::ArchiveRead,
            FakeObjects::new().failing_list(Fault::NotFound),
        ),
    );
    let row = run.row(CheckId::DestinationArchiveListable);
    assert_eq!(
        row.code,
        CheckCode::ObjectNotFound,
        "the code names what the backend said; it is never upgraded to a denial"
    );
}

/// A validate-only `CreateTopics` that the target refuses per topic carries
/// the broker's own classified code into the row.
#[test]
fn a_refused_validate_only_create_reports_the_brokers_code() {
    let yaml = restore_yaml(&ms_to_rfc3339(INSIDE_MS), &["orders"], "scratch");
    let m = mount(&restore_plan(&yaml, None));
    let probe = FakeProbe::new()
        .with_presence("logweir.scratch", TopicPresence::Present { partitions: 1 })
        .default_presence(TopicPresence::NotFound)
        .with_create_outcomes(vec![TopicCreateOutcome {
            name: "restore-orders".to_string(),
            code: CheckCode::ReplicationFactorExceedsBrokers,
            message: "the cluster has 1 broker".to_string(),
        }]);
    let run = drive(&m, &restore_wiring(&yaml, &manifest_json(), probe));
    let row = run.row(CheckId::TargetTopicCreate);
    assert_eq!(row.code, CheckCode::ReplicationFactorExceedsBrokers);
    assert_eq!(row.state, CheckState::NotReady);
    assert_eq!(row.detail.as_ref().unwrap()["sample"][0], "restore-orders");
    assert!(!row.remedy.is_empty());
}

// ---------------------------------------------------------------------------
// FX-14: a plan BOUND TO A RECOVERY POINT (`source.point`). The preflight
// judges the manifest it read with the point's pin, exactly as the runner's
// binding (`drill::binding::verify_point_binding`) will, so it never passes a
// point the run then refuses.
// ---------------------------------------------------------------------------

/// The bound fixture's archive prefix. `Store::in_memory*` holds only keys
/// under `logweir/` (Global Constraint 6), so the archive sits there — the
/// same concession `drill::binding`'s own rows make.
const BOUND_PREFIX: &str = "logweir";
/// The bound point's backup set.
const BOUND_SET: &str = "nightly-7";
/// Where the engine reads that set's manifest under [`BOUND_PREFIX`]
/// (`Store::engine_manifest_key`), and so where the receipt attests it.
const BOUND_MANIFEST_KEY: &str = "logweir/nightly-7/manifest.json";

/// [`manifest_json`], for [`BOUND_SET`]: the same window, its own segment keys.
fn bound_manifest() -> Vec<u8> {
    bound_manifest_of(BOUND_SET)
}

/// [`manifest_json`], for set `set`: the same window, its own segment keys.
/// The id is spliced in as JSON string content, so it may hold any character.
fn bound_manifest_of(set: &str) -> Vec<u8> {
    let quoted = serde_json::to_string(set).expect("a string");
    serde_json::to_vec(
        &serde_json::from_str::<serde_json::Value>(
            &manifest_json()
                .to_string()
                .replace(BACKUP_ID, &quoted[1..quoted.len() - 1]),
        )
        .expect("the fixture stays JSON"),
    )
    .expect("serialises")
}

/// Where the engine reads set `set`'s manifest under [`BOUND_PREFIX`].
fn bound_manifest_key_of(set: &str) -> String {
    format!("{BOUND_PREFIX}/{set}/manifest.json")
}

/// One SIGNED recovery point in a real in-memory `Store`, the keyring that
/// trusts its signer, and — on a versioned bucket — the handle that rewrites
/// its manifest the way the engine's own unconditional put does.
struct BoundPoint {
    store: logweir_engine_oso::storage::Store,
    bucket: Option<logweir_engine_oso::storage::VersionedBucket>,
    binding: logweir_core::execution_contract::PointBinding,
    keys: Vec<u8>,
}

/// A point whose receipt pins its manifest's version (`pin`, a versioned
/// bucket), or one written by a build that pinned nothing.
fn bound_point(pin: bool) -> BoundPoint {
    bound_point_of(BOUND_SET, "run-1", pin)
}

/// [`bound_point`], for backup set `set` and run `run`: every key the WRITER
/// would derive from those two ids, and nothing spelled by hand.
fn bound_point_of(set: &str, run: &str, pin: bool) -> BoundPoint {
    use logweir_evidence::keys::SigningKey;
    let (store, bucket) = logweir_engine_oso::storage::Store::in_memory_versioned(BOUND_PREFIX);
    let manifest = bound_manifest_of(set);
    let manifest_key = bound_manifest_key_of(set);
    let version = store
        .put_create_only(&manifest_key, &manifest)
        .expect("the manifest is written")
        .version_id
        .expect("a versioned bucket names the version");
    for i in 0..2 {
        store
            .put_create_only(
                &format!("{BOUND_PREFIX}/{set}/topics/orders/partition=0/segment-{i}.bin"),
                b"segment",
            )
            .expect("a segment is written");
    }
    // Instants relative to NOW, so the signer's validity window holds on any
    // date the suite runs (`just test-shifted-clock`).
    let now = Utc::now();
    let mut receipt = catalog_receipt(set, run, "2026-09-16T03:00:00Z");
    receipt.requested_at = now - chrono::Duration::minutes(5);
    receipt.started_at = now - chrono::Duration::minutes(4);
    receipt.finished_at = now - chrono::Duration::minutes(1);
    receipt.archive.manifest_key = manifest_key;
    receipt.archive.manifest_sha256 = logweir_core::ids::sha256_prefixed(&manifest);
    if pin {
        receipt.format_version =
            logweir_core::backup_receipt::FORMAT_VERSION_WITH_MANIFEST_VERSION.to_string();
        receipt.archive.manifest_version_id = Some(version);
    }
    let receipt_bytes =
        logweir_core::det_json::to_deterministic_json(&receipt).expect("the receipt serialises");
    let keys = logweir::backup::phase_run::receipt_keys(set, run);
    let signer = SigningKey::generate_ed25519();
    let sidecar = logweir_evidence::sign::sign_detached(
        &signer,
        logweir_evidence::PAYLOAD_TYPE_BACKUP_RECEIPT,
        &receipt_bytes,
    )
    .expect("signs");
    store
        .put_create_only(&keys.receipt_key, &receipt_bytes)
        .expect("the receipt is written");
    store
        .put_create_only(
            &keys.sidecar_key,
            &serde_json::to_vec(&sidecar).expect("serialises"),
        )
        .expect("the sidecar is written");
    let keyring = logweir_core::execution_contract::EvidenceKeyring {
        format_version: logweir_core::execution_contract::EVIDENCE_KEYRING_FORMAT_VERSION
            .to_string(),
        keys: vec![logweir_core::execution_contract::EvidenceKey {
            public_key_pem: signer.verifying_key().to_public_key_pem().expect("pem"),
            trust: logweir_core::trust::TrustedKey {
                key_id: signer.key_id(),
                principal_id: format!("install:{}", signer.key_id()),
                usages: vec![logweir_core::trust::KeyUsage::EvidenceSigning],
                not_before: now - chrono::Duration::days(1),
                not_after: now + chrono::Duration::days(1),
                state: logweir_core::trust::KeyState::Active,
                retired_at: None,
                revoked_at: None,
                revocation_reason: None,
                revocation_effective_from: None,
            },
        }],
    };
    BoundPoint {
        binding: logweir_core::execution_contract::PointBinding {
            point_id: logweir::catalog::record::point_id(&receipt_bytes),
            receipt_key: keys.receipt_key,
            receipt_sha256: logweir_core::ids::sha256_prefixed(&receipt_bytes),
            manifest_sha256: logweir_core::ids::sha256_prefixed(&manifest),
        },
        store,
        bucket: Some(bucket),
        keys: serde_json::to_vec(&keyring).expect("the keyring serialises"),
    }
}

/// The read cap of one object of a [`BoundPoint`]'s archive, by what the key
/// names (FX-31: every read names its class from `logweir_store::caps`). A
/// whole-archive copy reads four kinds of object, so the class is the
/// document's own and not one cap for all four.
fn archive_object_cap(key: &str) -> u64 {
    use logweir_engine_oso::storage::caps;
    if key.ends_with("/manifest.json") {
        caps::MANIFEST
    } else if key.ends_with(".receipt.json") {
        caps::SIGNED_DOCUMENT
    } else if key.ends_with(".receipt.sig") {
        caps::SIDECAR
    } else {
        assert!(
            key.contains("/topics/"),
            "{key}: an object of a kind this fixture does not write"
        );
        caps::SEGMENT
    }
}

impl BoundPoint {
    /// Every object, byte for byte, into a fresh UNVERSIONED bucket — `aws s3
    /// sync`, `mc mirror`: the same signed point in another place, carrying
    /// the pin and not the pinned version.
    fn copied(&self) -> Self {
        let copy = logweir_engine_oso::storage::Store::in_memory(BOUND_PREFIX);
        for key in self.store.list_keys("").expect("the source lists") {
            let (bytes, _) = self
                .store
                .get_capped(&key, archive_object_cap(&key))
                .expect("the source reads");
            copy.put_create_only(&key, &bytes)
                .expect("the copy is written");
        }
        Self {
            store: copy,
            bucket: None,
            binding: self.binding.clone(),
            keys: self.keys.clone(),
        }
    }

    fn rewrite_manifest(&self, bytes: &[u8]) {
        self.bucket
            .as_ref()
            .expect("a versioned bucket")
            .overwrite(BOUND_MANIFEST_KEY, bytes);
    }
}

/// The restore plan, bound to `point` (or to none) and naming its set.
fn bound_yaml(point: Option<&logweir_core::execution_contract::PointBinding>) -> String {
    let yaml = restore_yaml(&ms_to_rfc3339(INSIDE_MS), &["orders"], "scratch");
    let mut source = format!("  backup: {BOUND_SET}\n");
    if let Some(p) = point {
        source.push_str(&format!(
            "  point:\n    point_id: {}\n    receipt_key: {}\n    receipt_sha256: \"{}\"\n    \
             manifest_sha256: \"{}\"\n",
            p.point_id, p.receipt_key, p.receipt_sha256, p.manifest_sha256
        ));
    }
    let bound = yaml.replace(&format!("  backup: {BACKUP_ID}\n"), &source);
    assert_ne!(bound, yaml, "the fixture names its set");
    bound
}

/// The restore preflight over `store`, for a plan bound to `point`.
fn bound_preflight(
    store: logweir_engine_oso::storage::Store,
    point: Option<&logweir_core::execution_contract::PointBinding>,
) -> Run {
    bound_preflight_of(BOUND_SET, &bound_yaml(point), store)
}

/// The restore preflight over `store` for the plan `yaml`, whose request
/// reads set `set` (the controller renders `backupId` and the manifest key
/// from the same `backupSetRef` the plan's `source.backup` was written from).
fn bound_preflight_of(set: &str, yaml: &str, store: logweir_engine_oso::storage::Store) -> Run {
    let mut plan = restore_plan(yaml, None);
    if let CheckRequest::RestorePreflight(req) = &mut plan.request {
        req.backup_id = set.to_string();
        req.manifest_key = format!("{set}/manifest.json");
    }
    let probe = FakeProbe::new()
        .with_presence("logweir.scratch", TopicPresence::Present { partitions: 1 })
        .default_presence(TopicPresence::NotFound);
    let wiring = FakeWiring {
        shared_store: Some(Arc::new(store)),
        ..FakeWiring::default()
    }
    .with_file(PLAN_FILE, yaml.as_bytes())
    .with_probe(probe);
    drive(&mount(&plan), &wiring)
}

/// What the RUNNER's binding says about the same plan and the same archive.
fn bound_binding(
    p: &BoundPoint,
) -> Result<Option<logweir::drill::binding::VerifiedPoint>, logweir::drill::DrillError> {
    bound_binding_of(&bound_yaml(Some(&p.binding)), p)
}

/// What the RUNNER's binding says about the plan `yaml` over `p`'s archive.
fn bound_binding_of(
    yaml: &str,
    p: &BoundPoint,
) -> Result<Option<logweir::drill::binding::VerifiedPoint>, logweir::drill::DrillError> {
    let plan: logweir_core::spec::DrillSpec = serde_yaml::from_str(yaml).expect("the plan parses");
    logweir::drill::binding::verify_point_binding(&plan, &p.store, Some(&p.keys), Utc::now())
}

/// The restore plan naming set `set` and bound to `point`, with the set id and
/// the receipt key written as JSON strings — each a valid YAML double-quoted
/// scalar — so an id or a key may hold a space, a tab or a character that is
/// not ASCII and reach the reader unchanged.
fn quoted_point_yaml(set: &str, point: &logweir_core::execution_contract::PointBinding) -> String {
    let yaml = restore_yaml(&ms_to_rfc3339(INSIDE_MS), &["orders"], "scratch");
    let bound = yaml.replace(
        &format!("  backup: {BACKUP_ID}\n"),
        &format!(
            "  backup: {}\n  point:\n    point_id: {}\n    receipt_key: {}\n    \
             receipt_sha256: \"{}\"\n    manifest_sha256: \"{}\"\n",
            serde_json::to_string(set).expect("a string"),
            point.point_id,
            serde_json::to_string(&point.receipt_key).expect("a string"),
            point.receipt_sha256,
            point.manifest_sha256
        ),
    );
    assert_ne!(bound, yaml, "the fixture names its set");
    bound
}

/// The two archive rows a refused `archive.backupSet` holds back.
fn assert_archive_rest_blocked(run: &Run) {
    for id in [CheckId::ArchiveCoverage, CheckId::ArchiveSegments] {
        let row = run.row(id);
        assert_eq!(row.state, CheckState::Unknown, "{id}: {row:?}");
        assert_eq!(row.code, CheckCode::BlockedByPrerequisite, "{id}: {row:?}");
    }
}

/// **FX-14 item 1, the row the ledger asks for.** A pinned point whose set was
/// written again after it was signed — engine 0.21.0 rewrites a set in place
/// and can put a manifest with IDENTICAL bytes, which only the version shows —
/// is refused by the preflight (`ManifestSuperseded`, the superseded remedy),
/// as the runner's binding refuses it (exit 3), and the coverage and segment
/// rows, which would describe a manifest the run will not accept, do not run.
///
/// CONTROLS: the same point with its manifest UNCHANGED passes every archive
/// row and names the point; and the same rewrite under a plan bound to NO
/// point is the v1 shape and passes, as the runner admits it.
#[test]
fn a_point_bound_preflight_refuses_a_manifest_written_again_after_the_point() {
    let p = bound_point(true);
    p.rewrite_manifest(&bound_manifest());
    let versions = p
        .bucket
        .as_ref()
        .expect("versioned")
        .versions(BOUND_MANIFEST_KEY);
    assert_eq!(versions.len(), 2, "the pinned version and the rewrite");
    assert_eq!(
        bound_binding(&p)
            .expect_err("the runner refuses a superseded pin")
            .exit_code(),
        ExitCode::GuardRefused
    );
    let run = bound_preflight(p.store, Some(&p.binding));
    let row = run.row(CheckId::ArchiveBackupSet);
    assert_eq!(row.state, CheckState::NotReady, "{row:?}");
    assert_eq!(row.code, CheckCode::ManifestSuperseded, "{row:?}");
    assert_eq!(row.remedy, logweir::catalog::pin::SUPERSEDED_REMEDY);
    assert!(row.message.contains(&p.binding.point_id), "{}", row.message);
    for version in &versions {
        assert!(
            !row.message.contains(version.as_str()),
            "a version id is the bucket's, and no answer repeats it: {}",
            row.message
        );
    }
    assert_archive_rest_blocked(&run);
    assert_ne!(
        logweir_core::check_contract::aggregate(&run.result().checks),
        logweir_core::check_contract::OverallState::Ready
    );

    // CONTROL: unchanged, every archive row passes and the point is named.
    let unchanged = bound_point(true);
    assert!(bound_binding(&unchanged)
        .expect("the runner proves an unchanged point")
        .is_some_and(|v| v.pin_note.is_none()));
    let run = bound_preflight(unchanged.store, Some(&unchanged.binding));
    let row = run.row(CheckId::ArchiveBackupSet);
    assert_eq!(
        (row.state, row.code),
        (CheckState::Ready, CheckCode::ManifestReadable),
        "{row:?}"
    );
    assert!(
        row.message.contains(&unchanged.binding.point_id),
        "{}",
        row.message
    );
    for id in [CheckId::ArchiveCoverage, CheckId::ArchiveSegments] {
        assert_eq!(
            run.row(id).state,
            CheckState::Ready,
            "{id}: {:?}",
            run.row(id)
        );
    }
    assert_eq!(
        logweir_core::check_contract::aggregate(&run.result().checks),
        logweir_core::check_contract::OverallState::Ready
    );

    // CONTROL: a plan bound to no point reads the current manifest, as before.
    let unbound = bound_point(true);
    unbound.rewrite_manifest(&bound_manifest());
    let run = bound_preflight(unbound.store, None);
    let row = run.row(CheckId::ArchiveBackupSet);
    assert_eq!(
        (row.state, row.code),
        (CheckState::Ready, CheckCode::ManifestReadable),
        "{row:?}"
    );
}

/// **FX-14 — the preflight and the runner's binding agree, case by case**,
/// over ONE archive each: the real `Store` both read (the versioned in-memory
/// double, and an unversioned copy of it). "Agree" is the binding's outcome
/// read as a preview: proven → `ready`; proven with the note → `ready` with the
/// note and `UNCHECKED_NOTE` as the remedy; refused (exit 3) → `notReady`;
/// could not tell (exit 1) → not `ready`, with the remedy naming
/// `s3:GetObjectVersion`.
#[test]
fn the_point_bound_preflight_and_the_runners_binding_agree_on_every_pin_case() {
    use logweir::catalog::pin::{UNCHECKED_NOTE, UNREADABLE_REMEDY};
    use logweir::drill::binding::POINT_PIN_UNCHECKED;

    // (a) A byte-identical COPY in an unversioned bucket: the pin cannot be
    //     checked there, the digest decides, the note is said — never refused.
    let copy = bound_point(true).copied();
    let verified = bound_binding(&copy)
        .expect("the runner proves a copy of a signed point")
        .expect("the plan is bound");
    assert!(verified.pin_note.is_some(), "{verified:?}");
    let run = bound_preflight(copy.store, Some(&copy.binding));
    let row = run.row(CheckId::ArchiveBackupSet);
    assert_eq!(
        (row.state, row.code),
        (CheckState::Ready, CheckCode::ManifestReadable),
        "{row:?}"
    );
    assert!(row.message.contains(POINT_PIN_UNCHECKED), "{}", row.message);
    assert_eq!(row.remedy, UNCHECKED_NOTE);
    assert_eq!(run.row(CheckId::ArchiveCoverage).state, CheckState::Ready);

    // (b) A copy whose manifest is NOT the attested one: the digest refuses
    //     it, with the note — exit 3 at the runner.
    let p = bound_point(true);
    p.rewrite_manifest(br#"{"topics":[]}"#);
    let changed = p.copied();
    assert_eq!(
        bound_binding(&changed)
            .expect_err("a copy of another manifest is not the point")
            .exit_code(),
        ExitCode::GuardRefused
    );
    let run = bound_preflight(changed.store, Some(&changed.binding));
    let row = run.row(CheckId::ArchiveBackupSet);
    assert_eq!(
        (row.state, row.code),
        (CheckState::NotReady, CheckCode::PointBindingMismatch),
        "{row:?}"
    );
    assert!(row.message.contains(POINT_PIN_UNCHECKED), "{}", row.message);
    assert_archive_rest_blocked(&run);

    // (c) The pinned version cannot be READ (a 403: no s3:GetObjectVersion):
    //     "could not tell" — exit 1 at the runner, never ready here.
    let p = bound_point(true);
    p.rewrite_manifest(&bound_manifest());
    p.bucket
        .as_ref()
        .expect("versioned")
        .fail_version_reads(&s3_refusal("403 Forbidden", "AccessDenied"));
    assert_eq!(
        bound_binding(&p)
            .expect_err("the runner cannot tell")
            .exit_code(),
        ExitCode::Operational
    );
    let run = bound_preflight(p.store, Some(&p.binding));
    let row = run.row(CheckId::ArchiveBackupSet);
    assert_ne!(row.state, CheckState::Ready, "{row:?}");
    assert_eq!(row.code, CheckCode::AccessDenied, "{row:?}");
    assert_eq!(row.remedy, UNREADABLE_REMEDY);
    assert_archive_rest_blocked(&run);

    // (d) A point that pins nothing, over the same identical rewrite: the
    //     digest is the whole check for both, and both pass — what the pin adds.
    let unpinned = bound_point(false);
    unpinned.rewrite_manifest(&bound_manifest());
    assert!(bound_binding(&unpinned)
        .expect("without a pin the runner proves it")
        .is_some_and(|v| v.pin_note.is_none()));
    let run = bound_preflight(unpinned.store, Some(&unpinned.binding));
    assert_eq!(
        run.row(CheckId::ArchiveBackupSet).state,
        CheckState::Ready,
        "{:?}",
        run.row(CheckId::ArchiveBackupSet)
    );
}

/// **FX-14 — the receipt half of the binding, in the preview.** The pin is
/// read from the RECEIPT, so the preflight must be holding the receipt the
/// plan binds before it can judge it: each way the runner refuses that
/// receipt (exit 3, or exit 1 when it is not there) is refused here, and none
/// is ever `ready`.
///
/// **One answer for "absent" and "not these bytes"** (the orchestrator's
/// security note): a receipt that is not there and one whose bytes are not
/// the bound digest get the SAME code and the SAME message, so the answer
/// does not say whether an object exists at a key the plan's author chose —
/// and no answer repeats the stored receipt's own digest.
#[test]
fn a_point_bound_preflight_refuses_every_receipt_the_runner_refuses() {
    let p = bound_point(true);
    let receipt_key = p.binding.receipt_key.clone();
    let stored_digest = p.binding.receipt_sha256.clone();

    // A receipt that is not in this archive (a confined key: this set's own
    // receipt namespace, another run).
    let mut missing = p.binding.clone();
    missing.receipt_key = "logweir/backups/nightly-7/run-0.receipt.json".to_string();
    // A receipt whose bytes are not the bound ones.
    let mut digest = p.binding.clone();
    digest.receipt_sha256 = format!("sha256:{}", "0".repeat(64));
    // A point id the receipt does not derive.
    let mut id = p.binding.clone();
    id.point_id = format!("lwp1-{}", "a".repeat(32));
    // A manifest digest the receipt does not attest.
    let mut attested = p.binding.clone();
    attested.manifest_sha256 = format!("sha256:{}", "1".repeat(64));
    // Malformed on its face: answered before the archive is read.
    let mut malformed = p.binding.clone();
    malformed.receipt_sha256 = "sha256:short".to_string();

    let mut messages = BTreeMap::new();
    for (case, binding) in [
        ("missing", missing),
        ("digest", digest),
        ("point id", id),
        ("attested", attested),
        ("malformed", malformed),
    ] {
        let point = BoundPoint {
            store: p.copied().store,
            bucket: None,
            binding,
            keys: p.keys.clone(),
        };
        assert!(
            bound_binding(&point).is_err(),
            "{case}: the runner refuses this binding"
        );
        let run = bound_preflight(point.store, Some(&point.binding));
        let row = run.row(CheckId::ArchiveBackupSet);
        assert_eq!(
            (row.state, row.code),
            (CheckState::NotReady, CheckCode::PointBindingMismatch),
            "{case}: {row:?}"
        );
        assert!(!row.remedy.is_empty(), "{case}: a refusal names its remedy");
        assert_archive_rest_blocked(&run);
        messages.insert(case, row.message.clone());
    }
    // ABSENT and NOT-THESE-BYTES are one answer, apart from the key the plan
    // itself named.
    assert_eq!(
        messages["missing"].replace("run-0.receipt.json", "run-1.receipt.json"),
        messages["digest"],
        "the answer tells an absent receipt from a different one"
    );
    assert!(messages["digest"].contains(&receipt_key));
    // The stored receipt's digest is the archive's, and is never repeated
    // where the plan did not already state it.
    assert!(
        !messages["digest"].contains(&stored_digest),
        "{}",
        messages["digest"]
    );
}

/// **FX-14 — the set the receipt describes is the set this restore reads**
/// (FX-16's `PointBindingSetMismatch`). A receipt of set `nightly-7` copied
/// into set `nightly-8`'s receipt namespace, under a plan that restores
/// `nightly-8`: the key is confined, the bytes are the bound ones, and the
/// receipt still describes another set and another manifest — refused, and
/// the pin is never judged against an object the point does not describe.
#[test]
fn a_point_bound_preflight_refuses_a_set_the_point_does_not_describe() {
    let p = bound_point(true);
    let (receipt_bytes, _) = p
        .store
        .get_capped(
            &p.binding.receipt_key,
            logweir_engine_oso::storage::caps::SIGNED_DOCUMENT,
        )
        .expect("the receipt reads");
    p.store
        .put_create_only("logweir/nightly-8/manifest.json", &bound_manifest())
        .expect("another set's manifest");
    let foreign_key = "logweir/backups/nightly-8/run-1.receipt.json";
    p.store
        .put_create_only(foreign_key, &receipt_bytes)
        .expect("the receipt, copied into another set's namespace");
    let mut binding = p.binding.clone();
    binding.receipt_key = foreign_key.to_string();
    let yaml = bound_yaml(Some(&binding)).replace("backup: nightly-7", "backup: nightly-8");
    let mut plan = restore_plan(&yaml, None);
    if let CheckRequest::RestorePreflight(req) = &mut plan.request {
        req.backup_id = "nightly-8".to_string();
        req.manifest_key = "nightly-8/manifest.json".to_string();
    }
    let wiring = FakeWiring {
        shared_store: Some(Arc::new(p.store)),
        ..FakeWiring::default()
    }
    .with_file(PLAN_FILE, yaml.as_bytes())
    .with_probe(FakeProbe::new().default_presence(TopicPresence::NotFound));
    let run = drive(&mount(&plan), &wiring);
    let row = run.row(CheckId::ArchiveBackupSet);
    assert_eq!(
        (row.state, row.code),
        (CheckState::NotReady, CheckCode::PointBindingMismatch),
        "{row:?}"
    );
    assert!(
        row.message
            .starts_with(logweir::drill::binding::POINT_BINDING_SET_MISMATCH),
        "{}",
        row.message
    );
    assert!(
        !row.message.contains("nightly-7"),
        "the receipt's own set is the archive's, and the answer does not repeat it: {}",
        row.message
    );
}

/// `p`'s receipt with one field changed by `edit`, written as run `run` of
/// the SAME set — so its key is confined — and the point bound to those bytes
/// over the same archive. The manifest digest the plan binds is unchanged.
fn rebound(p: BoundPoint, run: &str, edit: impl FnOnce(&mut BackupReceipt)) -> BoundPoint {
    let (bytes, _) = p
        .store
        .get_capped(
            &p.binding.receipt_key,
            logweir_engine_oso::storage::caps::SIGNED_DOCUMENT,
        )
        .expect("the receipt reads");
    let mut receipt: BackupReceipt = serde_json::from_slice(&bytes).expect("a receipt");
    edit(&mut receipt);
    let bytes =
        logweir_core::det_json::to_deterministic_json(&receipt).expect("the receipt serialises");
    let key = logweir::backup::phase_run::receipt_keys(BOUND_SET, run).receipt_key;
    p.store
        .put_create_only(&key, &bytes)
        .expect("the edited receipt is written");
    BoundPoint {
        binding: logweir_core::execution_contract::PointBinding {
            point_id: logweir::catalog::record::point_id(&bytes),
            receipt_key: key,
            receipt_sha256: logweir_core::ids::sha256_prefixed(&bytes),
            manifest_sha256: p.binding.manifest_sha256,
        },
        store: p.store,
        bucket: None,
        keys: p.keys,
    }
}

/// What both readers must say of a receipt that describes another set or
/// another manifest key than the one this restore reads: the runner's binding
/// refuses the plan (exit 3, `PointBindingSetMismatch`), and the preflight is
/// `notReady PointBindingMismatch` opening with the same token, with the two
/// later archive rows held back.
fn assert_both_refuse_the_set(point: BoundPoint) -> String {
    let refused = bound_binding(&point).expect_err("the runner's binding refuses");
    assert_eq!(refused.exit_code(), ExitCode::GuardRefused, "{refused}");
    assert!(
        refused
            .to_string()
            .contains(logweir::drill::binding::POINT_BINDING_SET_MISMATCH),
        "{refused}"
    );
    let run = bound_preflight(point.store, Some(&point.binding));
    let row = run.row(CheckId::ArchiveBackupSet);
    assert_eq!(
        (row.state, row.code),
        (CheckState::NotReady, CheckCode::PointBindingMismatch),
        "{row:?}"
    );
    assert!(
        row.message
            .starts_with(logweir::drill::binding::POINT_BINDING_SET_MISMATCH),
        "{}",
        row.message
    );
    assert_archive_rest_blocked(&run);
    row.message
}

/// **FX-14 review M2 (O4) — the manifest-KEY half of the set comparison, on
/// its own.** The receipt names THIS set and attests its manifest at ANOTHER
/// key than the one this restore reads: the archive copied under another
/// prefix of the same bucket, byte for byte, with a destination at the copy.
/// Receipts are bucket-absolute and shared by both copies, so the set id
/// agrees and the digest agrees; only the key tells the two apart. The
/// runner's binding refuses it (FX-16 review M-1, the engine's key), and the
/// preflight must too — `a_point_bound_preflight_refuses_a_set_the_point_does_
/// not_describe` changes the set id and the key together, so it cannot see
/// this half dropped.
///
/// KILLS: the preflight's set comparison without `same_object_key` (it is
/// then `ready` for a point the run refuses).
#[test]
fn a_receipt_attesting_its_manifest_at_another_key_is_refused_by_the_binding_and_the_preflight() {
    const ELSEWHERE: &str = "logweir/elsewhere/nightly-7/manifest.json";
    let p = bound_point(false);
    p.store
        .put_create_only(ELSEWHERE, &bound_manifest())
        .expect("the copy of the manifest");
    let point = rebound(p, "run-2", |receipt| {
        receipt.archive.manifest_key = ELSEWHERE.to_string();
    });
    let message = assert_both_refuse_the_set(point);
    assert!(
        !message.contains("elsewhere"),
        "the key the receipt names is the archive's, and the answer does not repeat it: {message}"
    );
}

/// **FX-14 review M2 (O5) — the set-ID half, on its own.** The receipt
/// attests the manifest at exactly the key this restore reads, with its
/// digest, and says it is a receipt of ANOTHER set. No writer produces such a
/// receipt; a reader is still held to what the receipt says, because every
/// decision the receipt informs is about the set it names. The runner's
/// binding refuses it (FX-16), and so does the preflight.
///
/// KILLS: the preflight's set comparison without the `backup_id` (it is then
/// `ready`: the key, the digest and the pin all agree).
#[test]
fn a_receipt_of_another_set_naming_this_sets_manifest_is_refused_by_the_binding_and_the_preflight()
{
    let point = rebound(bound_point(false), "run-2", |receipt| {
        assert_eq!(receipt.archive.manifest_key, BOUND_MANIFEST_KEY);
        receipt.backup_id = "nightly-8".to_string();
    });
    let message = assert_both_refuse_the_set(point);
    assert!(
        !message.contains("nightly-8"),
        "the receipt's own set is the archive's, and the answer does not repeat it: {message}"
    );
}

/// **FX-14 review M1 — a set id and a run id that hold a SPACE.** A backup
/// set id is free text (`BackupSpec::backup_id`; `Restore.spec.backupSetRef`
/// carries no pattern), a space is addressed by the store exactly as written,
/// and the runner's binding restores such a set. So the preflight judges it
/// too, and judges it the same: it never refuses a point the binding accepts.
///
/// One signed point of set `nightly 7`, run `run 1`, in a real `Store` both
/// readers read: the binding proves it and the preflight is `ready` on every
/// archive row. AND THE OTHER WAY, over the same ids: the set written again
/// after the point was signed is refused by both — so the spaced manifest key
/// reaches the read by VERSION as written as well, and "they agree" is not
/// "both say yes to anything".
///
/// KILLS: a rule that refuses the space (the preflight then answers
/// `PointBindingMismatch`, "it was not read", for the writer's own key).
#[test]
fn a_set_id_and_a_run_id_with_a_space_are_judged_as_the_runners_binding_judges_them() {
    const SET: &str = "nightly 7";
    const RUN: &str = "run 1";
    let p = bound_point_of(SET, RUN, true);
    assert_eq!(
        p.binding.receipt_key, "logweir/backups/nightly 7/run 1.receipt.json",
        "the writer's own derivation, both spaces as written"
    );
    let yaml = quoted_point_yaml(SET, &p.binding);
    let plan: logweir_core::spec::DrillSpec = serde_yaml::from_str(&yaml).expect("the plan parses");
    assert_eq!(plan.source.backup, SET);
    assert_eq!(
        plan.source.point.as_ref().expect("bound").receipt_key,
        p.binding.receipt_key
    );

    let verified = bound_binding_of(&yaml, &p)
        .expect("the runner's binding accepts a set id with a space")
        .expect("the plan is bound");
    assert_eq!(verified.point_id, p.binding.point_id);
    assert_eq!(verified.set.backup_id, SET);
    assert!(verified.pin_note.is_none(), "{verified:?}");
    let run = bound_preflight_of(SET, &yaml, p.store);
    let row = run.row(CheckId::ArchiveBackupSet);
    assert_eq!(
        (row.state, row.code),
        (CheckState::Ready, CheckCode::ManifestReadable),
        "the binding accepts this point and the preflight refuses it: {row:?}"
    );
    assert!(row.message.contains(&p.binding.point_id), "{}", row.message);
    for id in [CheckId::ArchiveCoverage, CheckId::ArchiveSegments] {
        assert_eq!(
            run.row(id).state,
            CheckState::Ready,
            "{id}: {:?}",
            run.row(id)
        );
    }

    // The other way: the same set written again after the point was signed.
    let again = bound_point_of(SET, RUN, true);
    let bucket = again.bucket.as_ref().expect("versioned");
    bucket.overwrite(&bound_manifest_key_of(SET), &bound_manifest_of(SET));
    assert_eq!(bucket.versions(&bound_manifest_key_of(SET)).len(), 2);
    let yaml = quoted_point_yaml(SET, &again.binding);
    assert_eq!(
        bound_binding_of(&yaml, &again)
            .expect_err("the runner refuses a superseded pin")
            .exit_code(),
        ExitCode::GuardRefused
    );
    let run = bound_preflight_of(SET, &yaml, again.store);
    let row = run.row(CheckId::ArchiveBackupSet);
    assert_eq!(
        (row.state, row.code),
        (CheckState::NotReady, CheckCode::ManifestSuperseded),
        "{row:?}"
    );
    assert_archive_rest_blocked(&run);
}

// ---------------------------------------------------------------------------
// FX-14, the orchestrator's security note: a preflight runs BEFORE any
// approval, with the namespace's archive-read credential, so a plan's receipt
// key is CONFINED before anything is read. These rows use the read-counting
// double: `FakeObjects::calls` records every `get` and every `list`.
// ---------------------------------------------------------------------------

/// The confined fixture: set [`BACKUP_ID`] under [`ARCHIVE_PREFIX`], its
/// receipt at the key `backup run` writes, and the SAME receipt bytes at every
/// key in `elsewhere` outside the archive's own objects — so a read of any of
/// them would succeed and would hash to the plan's digest. Returns the objects
/// and the binding to the real key.
fn confined_fixture(
    elsewhere: &[&str],
) -> (FakeObjects, logweir_core::execution_contract::PointBinding) {
    confined_fixture_of(BACKUP_ID, "run-1", elsewhere)
}

/// [`confined_fixture`], for set `set` and run `run`: the set's manifest and
/// segments under [`ARCHIVE_PREFIX`], and its receipt at the key the WRITER
/// derives from those two ids.
fn confined_fixture_of(
    set: &str,
    run: &str,
    elsewhere: &[&str],
) -> (FakeObjects, logweir_core::execution_contract::PointBinding) {
    let manifest = bound_manifest_of(set);
    let manifest_key = format!("{ARCHIVE_PREFIX}/{set}/manifest.json");
    let mut receipt = catalog_receipt(set, run, "2026-09-15T05:00:00Z");
    receipt.archive.manifest_key = manifest_key.clone();
    receipt.archive.manifest_sha256 = logweir_core::ids::sha256_prefixed(&manifest);
    let receipt_bytes =
        logweir_core::det_json::to_deterministic_json(&receipt).expect("the receipt serialises");
    let key = logweir::backup::phase_run::receipt_keys(set, run).receipt_key;
    let mut objects = FakeObjects::new()
        .with_prefix(ARCHIVE_PREFIX)
        .with_object(&manifest_key, &manifest)
        .with_object(&key, &receipt_bytes);
    for i in 0..2 {
        objects = objects.with_object(
            &format!("{ARCHIVE_PREFIX}/{set}/topics/orders/partition=0/segment-{i}.bin"),
            b"segment",
        );
    }
    for other in elsewhere {
        // The archive's own objects keep their bytes: they are in the list as
        // keys a plan might name, not as places to plant a receipt.
        if !other.starts_with(&format!("{ARCHIVE_PREFIX}/")) {
            objects = objects.with_object(other, &receipt_bytes);
        }
    }
    let binding = logweir_core::execution_contract::PointBinding {
        point_id: logweir::catalog::record::point_id(&receipt_bytes),
        receipt_key: key,
        receipt_sha256: logweir_core::ids::sha256_prefixed(&receipt_bytes),
        manifest_sha256: logweir_core::ids::sha256_prefixed(&manifest),
    };
    (objects, binding)
}

/// The restore plan over [`BACKUP_ID`], bound to `point`, naming `backup`.
fn confined_yaml(point: &logweir_core::execution_contract::PointBinding, backup: &str) -> String {
    restore_yaml(&ms_to_rfc3339(INSIDE_MS), &["orders"], "scratch").replace(
        &format!("  backup: {BACKUP_ID}\n"),
        &format!(
            "  backup: {backup}\n  point:\n    point_id: {}\n    receipt_key: \"{}\"\n    \
             receipt_sha256: \"{}\"\n    manifest_sha256: \"{}\"\n",
            point.point_id, point.receipt_key, point.receipt_sha256, point.manifest_sha256
        ),
    )
}

/// The preflight over the confined fixture; the handle is returned so a row
/// can count what was read.
fn confined_preflight(objects: &FakeObjects, yaml: &str) -> Run {
    confined_preflight_of(BACKUP_ID, objects, yaml)
}

/// [`confined_preflight`], with the request reading set `set`.
fn confined_preflight_of(set: &str, objects: &FakeObjects, yaml: &str) -> Run {
    let mut plan = restore_plan(yaml, None);
    if let CheckRequest::RestorePreflight(req) = &mut plan.request {
        req.backup_id = set.to_string();
        req.manifest_key = format!("{set}/manifest.json");
    }
    let probe = FakeProbe::new()
        .with_presence("logweir.scratch", TopicPresence::Present { partitions: 1 })
        .default_presence(TopicPresence::NotFound);
    drive(
        &mount(&plan),
        &FakeWiring::default()
            .with_file(PLAN_FILE, yaml.as_bytes())
            .with_probe(probe)
            .with_role(DestinationRole::ArchiveRead, objects.clone()),
    )
}

/// **The confinement, and NO store read.** A plan may name any text as its
/// receipt key. Every key that is not the key `backup run` writes THIS plan's
/// set's receipt at — another tenant's prefix, another set, a relative or
/// nested path, an object that is not a receipt — is refused by name with the
/// archive untouched: not the receipt, not the manifest, no listing. Each of
/// those keys HOLDS the bound receipt's bytes in this fixture, so a build
/// that read them would find the digest it was told to expect and go on.
///
/// CONTROL: the writer's own key, in the same fixture, is read and the row is
/// `ready`.
///
/// KILLS: the confinement removed (`receipt_run_id` not consulted — every
/// key below is then read, and most read `ready`); the confinement moved
/// after the manifest read (the read count is no longer zero).
#[test]
fn a_receipt_key_outside_the_plans_own_receipts_is_refused_with_no_store_read() {
    let outside = [
        // another tenant's prefix, in a bucket shared by prefix
        "tenant-b/logweir/backups/20260915T030000Z/run-1.receipt.json",
        "tenant-b/kafka-backups/20260915T030000Z/manifest.json",
        // an absolute spelling, and another set's receipt
        "/logweir/backups/20260915T030000Z/run-1.receipt.json",
        "logweir/backups/20260916T030000Z/run-1.receipt.json",
        // relative and nested paths
        "logweir/backups/20260915T030000Z/../20260916T030000Z/run-1.receipt.json",
        "logweir/backups/20260915T030000Z/../../../tenant-b/private.receipt.json",
        "logweir/backups/20260915T030000Z/nested/run-1.receipt.json",
        // objects that are not a receipt
        "logweir/backups/20260915T030000Z/run-1.receipt.sig",
        "logweir/backups/20260915T030000Z/execution.claim.json",
        "logweir/catalog/v1/points/lwp1-00000000000000000000000000000000/record.json",
        "logweir/drills/run-1.receipt.json",
        "kafka-backups/20260915T030000Z/manifest.json",
        "kafka-backups/20260915T030000Z/topics/orders/partition=0/segment-0.bin",
    ];
    let (objects, real) = confined_fixture(&outside);
    for key in outside {
        let before = objects.calls().len();
        let mut binding = real.clone();
        binding.receipt_key = key.to_string();
        let run = confined_preflight(&objects, &confined_yaml(&binding, BACKUP_ID));
        let row = run.row(CheckId::ArchiveBackupSet);
        assert_eq!(
            (row.state, row.code),
            (CheckState::NotReady, CheckCode::PointBindingMismatch),
            "{key}: {row:?}"
        );
        assert!(
            row.message.contains("it was not read"),
            "{key}: the refusal says so: {}",
            row.message
        );
        assert_archive_rest_blocked(&run);
        assert_eq!(
            objects.calls()[before..],
            Vec::<String>::new()[..],
            "{key}: the archive was touched for a key outside the plan's own receipts"
        );
    }

    // CONTROL: the writer's key is read, once, and the point is judged.
    let before = objects.calls().len();
    let run = confined_preflight(&objects, &confined_yaml(&real, BACKUP_ID));
    let row = run.row(CheckId::ArchiveBackupSet);
    assert_eq!(
        (row.state, row.code),
        (CheckState::Ready, CheckCode::ManifestReadable),
        "{row:?}"
    );
    let calls = objects.calls()[before..].to_vec();
    assert_eq!(
        calls
            .iter()
            .filter(|c| **c == format!("get {}", real.receipt_key))
            .count(),
        1,
        "{calls:?}"
    );
    assert_eq!(
        calls.iter().filter(|c| c.starts_with("get ")).count(),
        2,
        "the manifest and the one confined receipt, and no other object: {calls:?}"
    );
}

/// **The other two plan-only refusals read nothing either.** A binding that
/// is malformed on its face, and a bound plan whose `source.backup` is not the
/// set this check reads (`latestCompleted` included — FX-16: a bound plan
/// names its point's own set), are answered from the plan alone.
///
/// KILLS: the shape check skipped (a malformed digest then reaches the read,
/// and the read count is no longer zero); the set comparison dropped.
#[test]
fn a_malformed_or_foreign_set_binding_is_refused_with_no_store_read() {
    let (objects, real) = confined_fixture(&[]);

    let mut short = real.clone();
    short.receipt_sha256 = "sha256:short".to_string();
    let mut empty_key = real.clone();
    empty_key.receipt_key = "  ".to_string();
    let mut bad_id = real.clone();
    bad_id.point_id = "lwp1-NOT-A-POINT".to_string();

    for (case, binding, backup, opens) in [
        ("digest shape", short, BACKUP_ID, "PointBindingMismatch. "),
        ("empty key", empty_key, BACKUP_ID, "PointBindingMismatch. "),
        (
            "point id shape",
            bad_id,
            BACKUP_ID,
            "PointBindingMismatch. ",
        ),
        (
            "latestCompleted",
            real.clone(),
            "latestCompleted",
            "PointBindingSetMismatch. ",
        ),
        (
            "another set",
            real.clone(),
            "20260916T030000Z",
            "PointBindingSetMismatch. ",
        ),
    ] {
        let before = objects.calls().len();
        let run = confined_preflight(&objects, &confined_yaml(&binding, backup));
        let row = run.row(CheckId::ArchiveBackupSet);
        assert_eq!(
            (row.state, row.code),
            (CheckState::NotReady, CheckCode::PointBindingMismatch),
            "{case}: {row:?}"
        );
        assert!(row.message.starts_with(opens), "{case}: {}", row.message);
        assert_eq!(
            objects.calls()[before..],
            Vec::<String>::new()[..],
            "{case}: answered from the plan alone"
        );
        assert_archive_rest_blocked(&run);
    }
}

/// **A confined key whose set has no manifest under this destination is never
/// read.** The receipt read comes AFTER the set's manifest was read under the
/// destination's own prefix, so a plan cannot name a set this archive does
/// not hold and learn about that set's receipts: the answer is the manifest's
/// (`BackupSetNotFound`), and the receipt key is not fetched.
#[test]
fn a_bound_receipt_is_read_only_after_its_sets_manifest_was() {
    let (objects, real) = confined_fixture(&[]);
    let other_set = "20260916T030000Z";
    let key = logweir::backup::phase_run::receipt_keys(other_set, "run-1").receipt_key;
    let receipt_bytes = objects
        .get(
            &real.receipt_key,
            logweir_engine_oso::storage::caps::SIGNED_DOCUMENT,
        )
        .expect("the receipt reads");
    let objects = objects.with_object(&key, &receipt_bytes);
    let mut binding = real.clone();
    binding.receipt_key = key.clone();
    let yaml = confined_yaml(&binding, other_set);
    let mut plan = restore_plan(&yaml, None);
    if let CheckRequest::RestorePreflight(req) = &mut plan.request {
        req.backup_id = other_set.to_string();
        req.manifest_key = format!("{other_set}/manifest.json");
    }
    let before = objects.calls().len();
    let run = drive(
        &mount(&plan),
        &FakeWiring::default()
            .with_file(PLAN_FILE, yaml.as_bytes())
            .with_probe(FakeProbe::new().default_presence(TopicPresence::NotFound))
            .with_role(DestinationRole::ArchiveRead, objects.clone()),
    );
    let row = run.row(CheckId::ArchiveBackupSet);
    assert_eq!(row.code, CheckCode::BackupSetNotFound, "{row:?}");
    let calls = objects.calls()[before..].to_vec();
    assert_eq!(
        calls,
        vec![format!("get {ARCHIVE_PREFIX}/{other_set}/manifest.json")],
        "only this destination's own manifest key was asked for"
    );
}

/// **FX-14 review M1 — the space is allowed, and the confinement is still of
/// BYTES.** For a plan that restores set `nightly 7`, the one receipt key
/// that may be read is `logweir/backups/nightly 7/<run id>.receipt.json`,
/// byte for byte. Everything that only LOOKS like it is refused with the
/// archive untouched: the set segment with a space after it, or before it; a
/// tab or a no-break space where the space is; the space spelled `%20` or
/// `+`; and the same in the run segment and around the whole key. Each of
/// those keys holds the bound receipt's bytes in this fixture, so a build
/// that read one would find the digest it was told to expect.
///
/// A look-alike as the plan's OWN set id is no better: a tab, a no-break
/// space and `%20` are not plain, so such a set is answered from the plan
/// alone, though its manifest and its receipt are both there to be read.
///
/// CONTROL: the writer's own key for `nightly 7` / `run 1` is read, once, and
/// the row is `ready`.
///
/// KILLS: a rule that accepts a tab (the `run<TAB>1` key and the
/// `nightly<TAB>7` set are then read, and are `ready`).
#[test]
fn a_look_alike_of_a_set_id_with_a_space_is_refused_with_no_store_read() {
    const SET: &str = "nightly 7";
    const RUN: &str = "run 1";
    let look_alikes = [
        // the set segment: the plan's id and a space after it, or before it
        "logweir/backups/nightly 7 /run 1.receipt.json",
        "logweir/backups/ nightly 7/run 1.receipt.json",
        // a tab, and a no-break space, in the space's place
        "logweir/backups/nightly\t7/run 1.receipt.json",
        "logweir/backups/nightly\u{a0}7/run 1.receipt.json",
        // the space as a URL would spell it
        "logweir/backups/nightly%207/run 1.receipt.json",
        "logweir/backups/nightly+7/run 1.receipt.json",
        // two spaces, none, and the usual separator
        "logweir/backups/nightly  7/run 1.receipt.json",
        "logweir/backups/nightly7/run 1.receipt.json",
        "logweir/backups/nightly-7/run 1.receipt.json",
        // the run segment
        "logweir/backups/nightly 7/run\t1.receipt.json",
        "logweir/backups/nightly 7/run\u{a0}1.receipt.json",
        "logweir/backups/nightly 7/run%201.receipt.json",
        // the key as a whole
        " logweir/backups/nightly 7/run 1.receipt.json",
        "logweir/backups/nightly 7/run 1.receipt.json ",
    ];
    let (objects, real) = confined_fixture_of(SET, RUN, &look_alikes);
    assert_eq!(
        real.receipt_key,
        "logweir/backups/nightly 7/run 1.receipt.json"
    );
    let assert_refused_unread = |what: &str, run: &Run, calls: &[String]| {
        let row = run.row(CheckId::ArchiveBackupSet);
        assert_eq!(
            (row.state, row.code),
            (CheckState::NotReady, CheckCode::PointBindingMismatch),
            "{what:?}: {row:?}"
        );
        assert!(
            row.message.contains("it was not read"),
            "{what:?}: the refusal says so: {}",
            row.message
        );
        assert_archive_rest_blocked(run);
        assert_eq!(
            calls,
            &Vec::<String>::new()[..],
            "{what:?}: the archive was touched for a key that is not the plan's own set's receipt"
        );
    };
    for key in look_alikes {
        assert!(
            objects
                .get(key, logweir_engine_oso::storage::caps::SIGNED_DOCUMENT)
                .is_ok(),
            "{key:?} holds the receipt's bytes"
        );
        let before = objects.calls().len();
        let mut binding = real.clone();
        binding.receipt_key = key.to_string();
        let run = confined_preflight_of(SET, &objects, &quoted_point_yaml(SET, &binding));
        assert_refused_unread(key, &run, &objects.calls()[before..]);
    }

    // A look-alike as the plan's OWN set id, with that set's manifest and
    // receipt in place at the keys the writer would derive for it.
    for set in ["nightly\t7", "nightly\u{a0}7", "nightly%207"] {
        let (objects, binding) = confined_fixture_of(set, RUN, &[]);
        let before = objects.calls().len();
        let run = confined_preflight_of(set, &objects, &quoted_point_yaml(set, &binding));
        assert_refused_unread(set, &run, &objects.calls()[before..]);
    }

    // CONTROL: the writer's key is read, once, and the point is judged.
    let before = objects.calls().len();
    let run = confined_preflight_of(SET, &objects, &quoted_point_yaml(SET, &real));
    let row = run.row(CheckId::ArchiveBackupSet);
    assert_eq!(
        (row.state, row.code),
        (CheckState::Ready, CheckCode::ManifestReadable),
        "{row:?}"
    );
    let gets: Vec<String> = objects.calls()[before..]
        .iter()
        .filter(|c| c.starts_with("get "))
        .cloned()
        .collect();
    assert_eq!(
        gets,
        vec![
            format!("get {ARCHIVE_PREFIX}/{SET}/manifest.json"),
            format!("get {}", real.receipt_key),
        ],
        "the set's manifest and its one confined receipt, as written, and no other object"
    );
}

/// **FX-14 review M2 (O3) — a store FAILURE is a classified code, never the
/// store's own text.** A preflight's answer is read by whoever can create
/// one, before any approval, and a backend's error text carries what the
/// backend chose to say: request ids, host ids, bucket and endpoint names.
/// So a failed read of the bound receipt, and a failed read of the manifest
/// at the version the receipt pins, answer with the classified code and a
/// message made of the plan's own values — and the text planted in each
/// failure is nowhere in the output, in any stream.
///
/// KILLS: the store's error interpolated into the message in place of its
/// code (`PointRefusal::store`).
#[test]
fn a_failed_read_under_a_bound_plan_answers_a_classified_code_and_never_the_stores_text() {
    const PLANTED: [&str; 2] = ["PLANTED-REQUEST-7Q2", "PLANTED-HOST-9Z4"];
    let tail = format!("RequestId={} HostId={}", PLANTED[0], PLANTED[1]);
    let assert_nothing_planted = |case: &str, run: &Run| {
        let all = run.everything();
        for planted in PLANTED {
            assert!(
                !all.contains(planted),
                "{case}: the store's own text is in the output"
            );
        }
    };

    // The receipt read.
    let (objects, real) = confined_fixture(&[]);
    let yaml = confined_yaml(&real, BACKUP_ID);
    for (case, text, state, code) in [
        (
            "403",
            format!("{} {tail}", s3_refusal("403 Forbidden", "AccessDenied")),
            CheckState::NotReady,
            CheckCode::AccessDenied,
        ),
        (
            "timeout",
            format!("operation timed out {tail}"),
            CheckState::Unknown,
            CheckCode::Timeout,
        ),
        (
            "500",
            format!("Server error 500 InternalError {tail}"),
            CheckState::NotReady,
            CheckCode::StoreErrorUnclassified,
        ),
        (
            "unclassified",
            format!("something nobody classified {tail}"),
            CheckState::NotReady,
            CheckCode::StoreErrorUnclassified,
        ),
    ] {
        let objects = objects
            .clone()
            .failing_key(&real.receipt_key, Fault::Io(text));
        let run = confined_preflight(&objects, &yaml);
        let row = run.row(CheckId::ArchiveBackupSet);
        assert_eq!((row.state, row.code), (state, code), "{case}: {row:?}");
        assert_eq!(
            row.message,
            format!(
                "the receipt `{}` of recovery point {} could not be read: {code}",
                real.receipt_key, real.point_id
            ),
            "{case}: the plan's own values and the classified code, nothing else"
        );
        assert!(!row.remedy.is_empty(), "{case}: a refusal names its remedy");
        assert_nothing_planted(case, &run);
        assert_archive_rest_blocked(&run);
    }

    // The read of the manifest at the version the receipt pins.
    let p = bound_point(true);
    p.rewrite_manifest(&bound_manifest());
    p.bucket
        .as_ref()
        .expect("versioned")
        .fail_version_reads(&format!(
            "{} {tail}",
            s3_refusal("403 Forbidden", "AccessDenied")
        ));
    let run = bound_preflight(p.store, Some(&p.binding));
    let row = run.row(CheckId::ArchiveBackupSet);
    assert_eq!(
        (row.state, row.code),
        (CheckState::NotReady, CheckCode::AccessDenied),
        "{row:?}"
    );
    assert!(
        row.message.ends_with(": AccessDenied"),
        "the classified code closes the message: {}",
        row.message
    );
    assert_nothing_planted("the pinned version", &run);
    assert_archive_rest_blocked(&run);
}

/// **FX-14 under FX-31 — the bound receipt is read under the receipt cap, and
/// an object over it gets the answer of an absent one.** FX-14 read the
/// receipt whole (its review's L2); FX-31 gives every read a cap, and the
/// preflight names the class the runner's binding reads the same document
/// under, `caps::SIGNED_DOCUMENT`, so the two readers agree on which objects
/// can be read at all.
///
/// An object over the cap is THERE, and saying so would bring back what the
/// confinement closed: a preflight runs before any approval, and its author
/// chose the key. So absent, other bytes and over the cap are ONE answer —
/// the same state, code, message and remedy, the same rows elsewhere in the
/// result, the same exit code — at the same two store calls in the same
/// order under the same caps: the set's manifest, then one `get` of the
/// receipt. The size the store reports is nowhere in the output.
///
/// The over-cap object HOLDS the bound receipt's bytes in this fixture, so a
/// build that read past the cap would find the digest it was told to expect
/// and answer `ready`.
///
/// CONTROL: the same bytes at the same key, reported at EXACTLY the cap, are
/// read and judged `ready`: the cap is the receipt class's, not a tighter one.
///
/// **The manifest at the pinned version** is read under `caps::MANIFEST`, the
/// class the binding and the catalog's deep check read it under. That read is
/// made only once the receipt's bytes are proven the bound ones, and a
/// version over the cap is the shared `pin::judge`'s "could not tell": the
/// classified code, and neither the size nor the version id.
///
/// KILLS: the receipt read under a larger cap (`caps::MANIFEST`, `u64::MAX`:
/// the over-cap object is read and the row is `ready`) or a smaller one
/// (`caps::SIDECAR`, the catalog walk's 256 KiB: the control is refused);
/// `TooLarge` left to the store-failure arm (its own code, so present is told
/// from absent); the pinned version read under another cap.
#[test]
fn a_bound_receipt_over_the_read_cap_is_answered_as_an_absent_one_at_the_same_store_calls() {
    use logweir::catalog::pin::UNREADABLE_REMEDY;
    use logweir_engine_oso::storage::caps;
    const CAP: u64 = caps::SIGNED_DOCUMENT;
    const FAR_OVER: u64 = 5 << 30;
    let manifest_key = format!("{ARCHIVE_PREFIX}/{BACKUP_ID}/manifest.json");
    // One fixture per case: a double's clones share their state.
    let (_, real) = confined_fixture(&[]);
    let yaml = confined_yaml(&real, BACKUP_ID);
    let the_two_reads = vec![
        (manifest_key.clone(), caps::MANIFEST),
        (real.receipt_key.clone(), CAP),
    ];

    // CONTROL: reported at exactly the cap, the bound receipt is read and
    // judged.
    let (objects, binding) = confined_fixture(&[]);
    assert_eq!(binding, real, "the fixture is the same archive every time");
    let objects = objects.reporting_size(&real.receipt_key, CAP);
    let run = confined_preflight(&objects, &yaml);
    let row = run.row(CheckId::ArchiveBackupSet);
    assert_eq!(
        (row.state, row.code),
        (CheckState::Ready, CheckCode::ManifestReadable),
        "at the cap: {row:?}"
    );
    assert_eq!(
        objects.read_caps(),
        the_two_reads,
        "the manifest under its cap, then the receipt under the signed-document cap"
    );

    // Absent, other bytes, and over the cap (by one byte, and by far).
    type Arrange = fn(FakeObjects, &str) -> FakeObjects;
    let cases: [(&str, Arrange); 4] = [
        ("absent", |o, key| o.failing_key(key, Fault::NotFound)),
        ("other bytes", |o, key| {
            o.with_object(key, b"not the bound receipt")
        }),
        ("one byte over the cap", |o, key| {
            o.reporting_size(key, CAP + 1)
        }),
        ("far over the cap", |o, key| o.reporting_size(key, FAR_OVER)),
    ];
    let mut answers = BTreeMap::new();
    for (case, arrange) in cases {
        let (objects, binding) = confined_fixture(&[]);
        assert_eq!(binding, real, "{case}: the same archive");
        let objects = arrange(objects, &real.receipt_key);
        let run = confined_preflight(&objects, &yaml);
        let row = run.row(CheckId::ArchiveBackupSet);
        assert_eq!(
            (row.state, row.code),
            (CheckState::NotReady, CheckCode::PointBindingMismatch),
            "{case}: {row:?}"
        );
        assert_archive_rest_blocked(&run);
        assert_eq!(
            objects.calls(),
            vec![
                format!("get {manifest_key}"),
                format!("get {}", real.receipt_key),
            ],
            "{case}: the set's manifest, then ONE read of the receipt, and nothing else"
        );
        assert_eq!(
            objects.read_caps(),
            the_two_reads,
            "{case}: each under its cap"
        );
        let all = run.everything();
        for size in [CAP + 1, FAR_OVER] {
            assert!(
                !all.contains(&size.to_string()),
                "{case}: the size the store reports is in the output"
            );
        }
        // Everything an operator, or the plan's author, can read off the run.
        let rows: Vec<_> = run
            .result()
            .checks
            .iter()
            .map(|c| {
                (
                    c.id.as_str(),
                    c.state,
                    c.code,
                    c.message.clone(),
                    c.remedy.clone(),
                )
            })
            .collect();
        answers.insert(case, (run.code, rows));
    }
    for case in ["other bytes", "one byte over the cap", "far over the cap"] {
        assert_eq!(
            answers[case], answers["absent"],
            "{case}: the answer tells this receipt from an absent one"
        );
    }

    // The manifest at the version the receipt pins, over the manifest cap.
    const PIN: &str = "PINNED-VERSION-3F9A";
    let pinned_fixture = || {
        let manifest = bound_manifest_of(BACKUP_ID);
        let mut receipt = catalog_receipt(BACKUP_ID, "run-1", "2026-09-15T05:00:00Z");
        receipt.format_version =
            logweir_core::backup_receipt::FORMAT_VERSION_WITH_MANIFEST_VERSION.to_string();
        receipt.archive.manifest_key = manifest_key.clone();
        receipt.archive.manifest_sha256 = logweir_core::ids::sha256_prefixed(&manifest);
        receipt.archive.manifest_version_id = Some(PIN.to_string());
        let receipt_bytes = logweir_core::det_json::to_deterministic_json(&receipt)
            .expect("the receipt serialises");
        let key = logweir::backup::phase_run::receipt_keys(BACKUP_ID, "run-1").receipt_key;
        // Written again with identical bytes: the pin is no longer current.
        let objects = FakeObjects::new()
            .with_prefix(ARCHIVE_PREFIX)
            .with_object(&key, &receipt_bytes)
            .with_versions(&manifest_key, &[(PIN, &manifest), ("v2", &manifest)]);
        let binding = logweir_core::execution_contract::PointBinding {
            point_id: logweir::catalog::record::point_id(&receipt_bytes),
            receipt_key: key,
            receipt_sha256: logweir_core::ids::sha256_prefixed(&receipt_bytes),
            manifest_sha256: logweir_core::ids::sha256_prefixed(&manifest),
        };
        (objects, binding)
    };
    let pinned_at = format!("{manifest_key}?versionId={PIN}");
    let the_three_reads = |binding: &logweir_core::execution_contract::PointBinding| {
        vec![
            (manifest_key.clone(), caps::MANIFEST),
            (binding.receipt_key.clone(), CAP),
            (pinned_at.clone(), caps::MANIFEST),
        ]
    };

    // CONTROL: the pinned version within the cap is read, and the set was
    // written again.
    let (objects, binding) = pinned_fixture();
    let run = confined_preflight(&objects, &confined_yaml(&binding, BACKUP_ID));
    let row = run.row(CheckId::ArchiveBackupSet);
    assert_eq!(
        (row.state, row.code),
        (CheckState::NotReady, CheckCode::ManifestSuperseded),
        "{row:?}"
    );
    assert_eq!(objects.read_caps(), the_three_reads(&binding));

    let (objects, binding) = pinned_fixture();
    let objects = objects.reporting_size(&pinned_at, caps::MANIFEST + 1);
    let run = confined_preflight(&objects, &confined_yaml(&binding, BACKUP_ID));
    let row = run.row(CheckId::ArchiveBackupSet);
    assert_eq!(
        (row.state, row.code),
        (CheckState::NotReady, CheckCode::StoreErrorUnclassified),
        "a pinned version over the cap is \"could not tell\": {row:?}"
    );
    assert!(
        row.message.ends_with(": StoreErrorUnclassified"),
        "the classified code closes the message: {}",
        row.message
    );
    assert_eq!(row.remedy, UNREADABLE_REMEDY);
    assert_eq!(objects.read_caps(), the_three_reads(&binding));
    let all = run.everything();
    assert!(
        !all.contains(&(caps::MANIFEST + 1).to_string()) && !all.contains(PIN),
        "neither the size the store reports nor the version id is in the output"
    );
    assert_archive_rest_blocked(&run);
}

// ===========================================================================
// 8. Parity with `logweir_store`'s own manifest walk
// ===========================================================================

/// D2 §6.7 names `Store::segment_keys_for_set`. The runner reads the manifest
/// through a four-method seam instead, so the denial paths are fakeable — and
/// that makes [`logweir::check::archive`] a SECOND implementation of one JSON
/// shape. This row holds the two to the same answer over the same archive,
/// including the `qualify` rule a review already found wrong once.
///
/// It builds a REAL `Store` over a tempdir filesystem URL: no endpoint, no
/// network, and the same shape `crates/weirkeeper/tests/retention.rs` uses.
#[test]
fn the_pure_segment_walk_agrees_with_the_store() {
    use logweir_engine_oso::storage::Store;
    let dir = tempfile::tempdir().unwrap();
    let prefix = "kafka-backups";
    let set_dir = dir.path().join(prefix).join(BACKUP_ID);
    std::fs::create_dir_all(&set_dir).unwrap();
    let manifest = manifest_json();
    std::fs::write(
        set_dir.join("manifest.json"),
        serde_json::to_vec(&manifest).unwrap(),
    )
    .unwrap();

    let store = Store::read_only_from_url(&logweir_core::engine::StorageUrl::Filesystem {
        path: dir.path().to_path_buf(),
    })
    .expect("a filesystem store over a tempdir");

    for window in [
        (1_757_898_000_000i64, 1_757_908_800_000i64),
        (1_757_898_000_000, 1_757_901_600_000),
        (1_757_901_600_001, 1_757_908_800_000),
        (0, 1),
    ] {
        let theirs = store
            .segment_keys_for_set(
                &format!("{prefix}/{BACKUP_ID}/manifest.json"),
                "orders",
                0,
                window,
            )
            .expect("the store walks its own manifest");
        let ours =
            logweir::check::archive::segment_keys_for(&manifest, "orders", 0, window, &|k| {
                ObjectAccess::qualify(&store, k)
            });
        assert_eq!(
            ours, theirs,
            "the runner's pure walk and `Store::segment_keys_for_set` disagree for {window:?}"
        );
    }
}

/// The two window computations agree as well: `logweir_store::manifest_facts`
/// is what the retention path reads, and `check::archive::window` is what the
/// coverage row reads.
#[test]
fn the_pure_manifest_window_agrees_with_the_store() {
    use logweir_engine_oso::storage::Store;
    let dir = tempfile::tempdir().unwrap();
    let set_dir = dir.path().join("kafka-backups").join(BACKUP_ID);
    std::fs::create_dir_all(&set_dir).unwrap();
    let manifest = manifest_json();
    std::fs::write(
        set_dir.join("manifest.json"),
        serde_json::to_vec(&manifest).unwrap(),
    )
    .unwrap();
    let store = Store::read_only_from_url(&logweir_core::engine::StorageUrl::Filesystem {
        path: dir.path().to_path_buf(),
    })
    .unwrap();
    let facts = store
        .manifest_facts(
            &format!("kafka-backups/{BACKUP_ID}/manifest.json"),
            logweir_engine_oso::storage::caps::CONTROLLER_MANIFEST,
        )
        .expect("the store reads its own manifest");
    let ours = logweir::check::archive::window(&manifest).expect("a bounded window");
    assert_eq!(ours.oldest_ms, facts.oldest_record_ms);
    assert_eq!(ours.newest_ms, facts.newest_record_ms);
}

// ===========================================================================
// 9. Redaction — D2 §4.2 "never prints a credential"
// ===========================================================================

/// **M4.** A credential planted in EVERY input that can reach a message never
/// reaches a frame.
///
/// The plants are in the places adopter-controlled text really flows into a
/// check's output: the destination name, the bucket, the prefix, the endpoint
/// (with userinfo), an object key, a topic name, the signer refusal text, the
/// plan file path and the backend's own error string. `redact` is the
/// chokepoint, and it is reached only because every message goes through
/// `CheckOutcome::with_message` / `with_remedy` / `with_fact`.
#[test]
fn a_credential_planted_in_every_error_path_is_redacted() {
    let secrets = [
        PLANTED_KEY_ID,
        "hunter2",
        "ZZfakefakefakefakefakefakefakefake01",
    ];

    // (a) readiness: a poisoned destination, a poisoned topic, a poisoned
    //     signer refusal, and a backend error carrying an S3 XML body.
    let mut dest = destination();
    dest.name = format!("prod-{PLANTED_KEY_ID}");
    dest.location.bucket = format!("bucket-{PLANTED_KEY_ID}");
    dest.location.prefix = PLANTED_SECRET.replace('=', "-");
    dest.location.endpoint = Some(PLANTED_USERINFO.to_string());
    dest.location_digest = dest.location.location_digest();
    let mut plan = readiness_plan(vec![], true, Some("/signing/key.pem"));
    if let CheckRequest::OperationReadiness(r) = &mut plan.request {
        r.destination = Some(dest);
        r.topics = vec![format!("orders-{PLANTED_KEY_ID}")];
        r.connection.principal = format!("User:{PLANTED_KEY_ID}");
    }
    let m = mount(&plan);
    let run = drive(
        &m,
        &FakeWiring::default()
            .with_probe(FakeProbe::new().default_presence(TopicPresence::NotFound))
            .with_role(
                DestinationRole::ArchiveRead,
                FakeObjects::new().failing_list(Fault::Io(format!(
                    "Generic S3 error: <Error><Code>AccessDenied</Code><Message>{PLANTED_SECRET}\
                     </Message></Error> for user {PLANTED_KEY_ID}"
                ))),
            )
            .with_role(
                DestinationRole::EvidenceRead,
                FakeObjects::new().failing_get(Fault::Io(format!(
                    "Generic S3 error: connecting to {PLANTED_USERINFO} failed"
                ))),
            )
            .with_writer(FakeObjects::new().failing_put(Fault::Io(format!(
                "Generic S3 error: <Error><Code>AccessDenied</Code></Error> {PLANTED_SECRET}"
            ))))
            .with_signer(Err(format!(
                "signing prerequisite is not ready: the PEM began {PLANTED_SECRET}"
            ))),
    );
    let seen = run.everything();
    for s in secrets {
        assert!(
            !seen.contains(s),
            "`{s}` reached the readiness relay:\n{seen}"
        );
    }

    // (b) evidence fetch: a poisoned backend error. The KEY is deliberately
    //     clean here — see `the_evidence_result_echoes_the_requested_key`,
    //     which owns the one documented exception.
    let key = "logweir/backups/20260916/receipt.json";
    let m = mount(&fetch_plan(key, 1 << 20));
    let run = drive(
        &m,
        &FakeWiring::default().with_role(
            DestinationRole::EvidenceRead,
            FakeObjects::new().failing_get(Fault::Io(format!(
                "Generic S3 error: <Error><Code>InvalidAccessKeyId</Code></Error> {PLANTED_SECRET}"
            ))),
        ),
    );
    let seen = run.everything();
    for s in secrets {
        assert!(
            !seen.contains(s),
            "`{s}` reached the evidence relay:\n{seen}"
        );
    }

    // (c) restore preflight: a poisoned plan path and a poisoned mapped name.
    let yaml = restore_yaml(
        &ms_to_rfc3339(INSIDE_MS),
        &[&format!("o-{PLANTED_KEY_ID}")],
        "scratch",
    );
    let mut plan = restore_plan(&yaml, None);
    if let CheckRequest::RestorePreflight(r) = &mut plan.request {
        r.plan_file = format!("/check/{PLANTED_KEY_ID}.yaml");
        r.plan_sha256 = logweir_core::ids::sha256_prefixed(b"other");
    }
    let m = mount(&plan);
    let run = drive(
        &m,
        &FakeWiring::default()
            .with_probe(FakeProbe::new())
            .with_file(&format!("/check/{PLANTED_KEY_ID}.yaml"), yaml.as_bytes()),
    );
    let seen = run.everything();
    for s in secrets {
        assert!(
            !seen.contains(s),
            "`{s}` reached the preflight relay:\n{seen}"
        );
    }
    // ...and the code still says what happened.
    assert_eq!(
        run.row(CheckId::PlanParse).code,
        CheckCode::PlanHashMismatch
    );
}

/// The ONE field a check echoes verbatim, and why.
///
/// `EvidenceObjectResult::key` is the plan's own key returned unchanged: it is
/// the correlation between a three-object request and its three answers, and a
/// redacted key would match nothing the controller holds. Everything else the
/// runner writes — message, remedy, fact, scope, detail sample — goes through
/// `check_contract::redact`. This row pins the exception so that widening it
/// is a decision and not an edit.
#[test]
fn the_evidence_result_echoes_the_requested_key() {
    let key = "logweir/backups/20260916/AKIAFAKEFAKEFAKEFAKE.receipt.json";
    let m = mount(&fetch_plan(key, 1 << 20));
    let run = drive(
        &m,
        &FakeWiring::default().with_role(DestinationRole::EvidenceRead, FakeObjects::new()),
    );
    let e = &run.result().evidence[0];
    assert_eq!(
        e.key, key,
        "the key is the correlation and is returned unchanged"
    );
    // ...and nothing ELSE in the document carries it.
    let doc = String::from_utf8(
        run.relay
            .as_ref()
            .unwrap()
            .stream(Stream::Result)
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert_eq!(
        doc.matches("AKIAFAKEFAKEFAKEFAKE").count(),
        1,
        "the requested key is the only place a plan-supplied identifier is echoed:\n{doc}"
    );
}

/// A scope and a detail sample are redacted although both are references: the
/// chokepoint is worth more than the two exceptions.
#[test]
fn a_scope_and_a_detail_sample_are_redacted() {
    let scope = logweir::check::catalogue::scope(
        "BackupDestination",
        &format!("prod-{PLANTED_KEY_ID}"),
        Some(DEST_UID),
    );
    assert!(!scope.name.contains(PLANTED_KEY_ID), "{scope:?}");
    assert_eq!(
        scope.uid.as_deref(),
        Some(DEST_UID),
        "a UUID is not credential-shaped and survives"
    );
    let d = kinds::readiness::detail(&["orders".to_string(), format!("orders-{PLANTED_KEY_ID}")]);
    assert_eq!(d["sample"][0], "orders", "an ordinary name is untouched");
    assert!(!d["sample"][1].as_str().unwrap().contains(PLANTED_KEY_ID));
}

/// The marker body is deterministic and carries no credential, no subject and
/// no clock.
#[test]
fn the_marker_body_names_only_the_destination() {
    let body = logweir::check::store::marker_body(DEST_UID);
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["contract"], logweir::check::store::MARKER_CONTRACT);
    assert_eq!(v["destinationUid"], DEST_UID);
    assert_eq!(
        v.as_object().unwrap().len(),
        2,
        "a marker that varied per check is a marker nobody can predict"
    );
    assert_eq!(body, logweir::check::store::marker_body(DEST_UID));
}

// ===========================================================================
// 10. Structural guards
// ===========================================================================

fn check_sources() -> Vec<(String, String)> {
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
        for e in std::fs::read_dir(dir).unwrap() {
            let p = e.unwrap().path();
            if p.is_dir() {
                walk(&p, out);
            } else if p.extension().is_some_and(|x| x == "rs") {
                out.push(p);
            }
        }
    }
    let root = repo_root();
    let mut files = Vec::new();
    walk(&root.join("crates/logweir/src/check"), &mut files);
    files.sort();
    assert!(files.len() >= 8, "the walk found {} files", files.len());
    files
        .into_iter()
        .map(|p| {
            let rel = p
                .strip_prefix(&root)
                .unwrap()
                .to_string_lossy()
                .replace('\\', "/");
            let body = std::fs::read_to_string(&p).unwrap();
            let code = body
                .lines()
                .filter(|l| {
                    let t = l.trim_start();
                    !(t.starts_with("//") || t.starts_with("///") || t.starts_with("//!"))
                })
                .collect::<Vec<_>>()
                .join("\n");
            (rel, code)
        })
        .collect()
}

/// D2 §4.2: "Never invokes the engine binary, so `scripts/check-no-oso.sh`
/// passes unchanged."
#[test]
fn the_check_runner_never_invokes_the_engine() {
    for (path, code) in check_sources() {
        for token in [
            "run_engine(",
            "Command::new(",
            "OsoCliEngine",
            "engine_bin::",
            "DataEngine",
        ] {
            assert!(
                !code.contains(token),
                "{path} names `{token}`; a check must never invoke the engine (D2 §4.2)"
            );
        }
    }
}

/// **M5, the structural half.** The ONLY write is `put_create_only`, and the
/// only key it is handed is the readiness marker.
#[test]
fn the_only_write_in_the_check_runner_is_create_only() {
    let mut writers: Vec<String> = Vec::new();
    for (path, code) in check_sources() {
        for token in [".put(", ".put_opts(", "PutMode", "from_url(", "delete("] {
            assert!(
                !code.contains(token),
                "{path} names `{token}`; Global Constraint 6 gives a check exactly one write \
                 and it is `put_create_only` of the readiness marker"
            );
        }
        if code.contains("put_create_only(") {
            writers.push(path);
        }
    }
    assert_eq!(
        writers,
        vec!["crates/logweir/src/check/store.rs".to_string()],
        "exactly one module may name the one write; found {writers:?}"
    );
    // ...and the key it writes is built by one function.
    let (_, store) = check_sources()
        .into_iter()
        .find(|(p, _)| p.ends_with("check/store.rs"))
        .unwrap();
    assert!(store.contains("MARKER_PREFIX: &str = \"logweir/readiness/\""));
    assert!(
        store.matches("fn put_create_only").count() == 2,
        "the trait method and its `Store` impl, and nothing else"
    );
}

/// Every row the runner can emit has a catalogue entry — so `expiresAt` and
/// `gating` are properties of the ROW and not of the call site.
#[test]
fn every_relayed_row_has_a_catalogue_entry() {
    let mut named: BTreeSet<CheckId> = BTreeSet::new();
    for (_, code) in check_sources() {
        for id in CheckId::ALL {
            let variant = format!("CheckId::{:?}", id);
            if code.contains(&variant) {
                named.insert(*id);
            }
        }
    }
    assert!(!named.is_empty(), "the scan found no CheckId at all");
    for id in &named {
        assert!(
            logweir::check::catalogue::entry(*id).is_some(),
            "`{id}` is emitted by the runner and has no D2 §6.3 catalogue entry"
        );
    }
    // And no catalogue entry is dead: an entry standing for a row nothing
    // emits is an expiry nobody reads.
    for (id, _, _) in logweir::check::catalogue::RUNNER_ROWS {
        assert!(
            named.contains(id),
            "`{id}` is in the catalogue and no check emits it"
        );
    }
}

/// PLAT-03.1's acceptance: every failure names a remedy.
#[test]
fn every_failing_code_the_runner_emits_has_a_remedy() {
    let mut named: BTreeSet<CheckCode> = BTreeSet::new();
    for (_, code) in check_sources() {
        for c in CheckCode::ALL {
            if code.contains(&format!("CheckCode::{:?}", c)) {
                named.insert(*c);
            }
        }
    }
    // Ready codes carry no remedy: there is nothing to fix.
    let ready: BTreeSet<CheckCode> = [
        CheckCode::Authenticated,
        CheckCode::TopicsDescribable,
        CheckCode::ArchiveListable,
        CheckCode::MarkerWritten,
        CheckCode::MarkerAlreadyPresent,
        CheckCode::EvidenceReadable,
        CheckCode::SignerUsable,
        CheckCode::ContractSupported,
        CheckCode::PlanParsed,
        CheckCode::ManifestReadable,
        CheckCode::PointInTimeCovered,
        CheckCode::SegmentsPresent,
        CheckCode::MarkerHealthy,
        CheckCode::MappedTopicsAbsent,
        CheckCode::TopicCreateValidated,
        CheckCode::TimestampWithinBound,
        // FX-20c: `destination.credentialBound`'s ready code.
        CheckCode::CredentialBound,
        // PROD-01.2: the capability rows' ready codes.
        CheckCode::EngineProtocolSupported,
        CheckCode::TopicConfigsReadable,
        CheckCode::GroupTypesListed,
        CheckCode::CheckContractMismatch,
        CheckCode::ResultUnreadable,
        CheckCode::StoreErrorUnclassified,
    ]
    .into_iter()
    .collect();
    let mut missing: Vec<String> = Vec::new();
    for c in &named {
        if ready.contains(c) {
            continue;
        }
        if kinds::remedy_for(*c).is_empty() {
            missing.push(c.to_string());
        }
    }
    assert!(
        missing.is_empty(),
        "these codes reach a UI with no remedy text: {missing:?}"
    );
    // `StoreErrorUnclassified` is the one non-ready code with a remedy anyway,
    // because "I could not classify this" still needs an action.
    assert!(!kinds::remedy_for(CheckCode::StoreErrorUnclassified).is_empty());
}

/// The catalogue's execution-only rows really are forced to `unknown`.
#[test]
fn an_execution_only_row_can_never_claim_a_verdict() {
    for (id, gating, _) in logweir::check::catalogue::RUNNER_ROWS {
        if *gating != Gating::ExecutionOnly {
            continue;
        }
        let row =
            logweir::check::catalogue::outcome(*id, CheckState::Ready, CheckCode::Valid, now());
        assert_eq!(
            row.state,
            CheckState::Unknown,
            "`{id}` is execution-only and was allowed to claim `ready`"
        );
    }
}

// ===========================================================================
// 11. The shipped binary: the exit-code table
// ===========================================================================

/// Run the shipped binary with a DEADLINE, killing and REAPING it if it
/// overruns.
///
/// Every subprocess a test spawns must have a timeout: a hung child blocks the
/// whole worker. Consumed in shape from
/// `crates/logweir/tests/notify_deliver.rs::bounded_output`, whose header
/// records the 2026-09-15 failure this exists for.
struct BoundedRun {
    code: Option<i32>,
    stdout: String,
    stderr: String,
}

fn bounded_output(cmd: &mut std::process::Command, deadline: std::time::Duration) -> BoundedRun {
    let mut child = cmd
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("the shipped binary is spawnable");
    let started = std::time::Instant::now();
    let code = loop {
        match child.try_wait().expect("try_wait on the child") {
            Some(status) => break status.code(),
            None => {
                if started.elapsed() > deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    panic!("`logweir check run` did not exit within {deadline:?}");
                }
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
        }
    };
    let (mut out, mut err) = child
        .stdout
        .take()
        .zip(child.stderr.take())
        .expect("piped stdout and stderr");
    let mut stdout = String::new();
    let mut stderr = String::new();
    use std::io::Read as _;
    let _ = out.read_to_string(&mut stdout);
    let _ = err.read_to_string(&mut stderr);
    BoundedRun {
        code,
        stdout,
        stderr,
    }
}

/// Write a plan to a tempdir and invoke the shipped binary over it.
fn shipped(
    plan: &CheckPlan,
    env: &[(&str, &str)],
    version: &str,
) -> (BoundedRun, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("check-plan.json");
    std::fs::write(&path, serde_json::to_vec(plan).unwrap()).unwrap();
    let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_logweir"));
    cmd.args([
        "check",
        "run",
        "--plan",
        &path.to_string_lossy(),
        "--check-contract-version",
        version,
    ]);
    // A CLEAN environment: the runner must take every value from the Job
    // template, never from whatever the developer's shell holds.
    cmd.env_clear();
    cmd.env("PATH", std::env::var("PATH").unwrap_or_default());
    for (k, v) in env {
        cmd.env(k, v);
    }
    let run = bounded_output(&mut cmd, std::time::Duration::from_secs(30));
    (run, dir)
}

/// **M1, the behavioural half, and the exit-3 row of the table.** A refused
/// plan prints ONE stdout line and no frame — although the plan names a
/// destination and a broker, so a runner that had built clients first would
/// have printed frames or hung.
#[test]
fn a_refused_plan_prints_one_line_and_no_frame() {
    let plan = readiness_plan(vec!["orders"], true, Some("/nonexistent/key.pem"));
    let env = [
        (check::CONTRACT_VERSION_ENV, "1"),
        (
            check::PLAN_SHA256_ENV,
            "sha256:0000000000000000000000000000000000000000000000000000000000000000",
        ),
        (check::SUBJECT_UID_ENV, SUBJECT_UID),
    ];
    let (run, _dir) = shipped(&plan, &env, "1");
    assert_eq!(run.code, Some(3), "stderr: {}", run.stderr);
    assert_eq!(
        run.stdout, "refusal-reason=CheckContractMismatch\n",
        "exit 3 prints the refusal key and NOTHING else"
    );
    assert!(
        !run.stdout.contains("logweir-check-"),
        "a refusal prints no frame"
    );
}

/// The exit-0 row: an end line was printed, whatever the per-check states.
/// This plan's destination is a dead loopback address, so every destination row
/// is `notReady` — and the process still exits 0.
#[test]
fn a_check_that_found_problems_still_exits_zero() {
    let mut dest = destination();
    dest.location.endpoint = Some("http://127.0.0.1:1".to_string());
    dest.location.transport = TransportSecurity::InsecureHttp;
    dest.location_digest = dest.location.location_digest();
    let plan = plan_of(CheckRequest::DestinationAccess(DestinationAccessRequest {
        destination: dest,
        roles: vec![DestinationRole::ArchiveRead],
        write_probe: false,
        evidence_write: None,
        evidence_read: None,
    }));
    let bytes = serde_json::to_vec(&plan).unwrap();
    let sha = logweir_core::ids::sha256_prefixed(&bytes);
    let env = [
        (check::CONTRACT_VERSION_ENV, "1"),
        (check::PLAN_SHA256_ENV, sha.as_str()),
        (check::SUBJECT_UID_ENV, SUBJECT_UID),
        // Static credentials, so nothing reaches an instance-metadata endpoint.
        ("AWS_ACCESS_KEY_ID", "AKIAEXAMPLEEXAMPLE00"),
        (
            "AWS_SECRET_ACCESS_KEY",
            "not-a-real-secret-0000000000000000000000",
        ),
        ("AWS_REGION", "us-east-1"),
    ];
    let (run, _dir) = shipped(&plan, &env, "1");
    assert_eq!(run.code, Some(0), "stderr: {}", run.stderr);
    let end: Vec<&str> = run
        .stdout
        .lines()
        .filter(|l| l.starts_with("logweir-check-end="))
        .collect();
    assert_eq!(end.len(), 1, "exactly one end line: {}", run.stdout);
    assert_eq!(
        run.stdout.lines().last(),
        end.first().copied(),
        "the end line is the LAST stdout line"
    );
    // The frames verify against the digest the environment pinned.
    let mut decoder = Decoder::new();
    for line in run.stdout.lines() {
        decoder
            .push_line(line)
            .expect("every line is a frame or is ignored");
    }
    let relay = decoder
        .finish(&FrameExpectations {
            plan_sha256: sha,
            subject_uid: SUBJECT_UID.to_string(),
        })
        .expect("the shipped binary's frames verify");
    let result = relay.result().unwrap().unwrap();
    let row = result
        .checks
        .iter()
        .find(|c| c.id == CheckId::DestinationArchiveListable)
        .expect("an archive row");
    assert_ne!(
        row.state,
        CheckState::Ready,
        "nothing listens on 127.0.0.1:1, so the archive cannot be listable: {row:?}"
    );
    assert_eq!(
        row.code,
        CheckCode::EndpointUnreachable,
        "a closed port is EndpointUnreachable. It came back `Timeout` until the retry preamble \
         was stripped (reviewer finding F3), and this row accepted three codes, so nothing \
         noticed: {row:?}"
    );
    assert_eq!(
        row.state,
        CheckState::NotReady,
        "a closed port BLOCKS; `Timeout` would have made it a non-blocking `unknown`"
    );
    assert!(
        !run.stdout.contains("not-a-real-secret"),
        "a projected credential reached stdout"
    );
    assert!(
        !run.stderr.contains("not-a-real-secret"),
        "a projected credential reached stderr"
    );
}

/// 2 and 4 are never returned by this subcommand — a contract, not an accident
/// (Global Constraint 11 reserves both for a drill result).
#[test]
fn the_check_runner_never_returns_two_or_four() {
    let (_, code) = check_sources()
        .into_iter()
        .find(|(p, _)| p.ends_with("check/mod.rs"))
        .unwrap();
    for forbidden in ["ExitCode::DrillNotPass", "ExitCode::SigningOrLock"] {
        assert!(
            !code.contains(forbidden),
            "the check runner names `{forbidden}`; 2 and 4 belong to a drill result"
        );
    }
    for (path, code) in check_sources() {
        assert!(
            !code.contains("ExitCode::DrillNotPass") && !code.contains("ExitCode::SigningOrLock"),
            "{path} names a drill exit code"
        );
    }
}

/// `check run --help` is release-smokeable and exits 0.
#[test]
fn check_run_help_exits_ok() {
    let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_logweir"));
    cmd.args(["check", "run", "--help"]);
    let run = bounded_output(&mut cmd, std::time::Duration::from_secs(30));
    assert_eq!(run.code, Some(0));
    assert!(run.stdout.contains("--check-contract-version"));
}

/// A usage error is exit 1, never exit 2 — `main.rs`'s clap arm, over the new
/// subcommand.
#[test]
fn a_check_usage_error_is_operational() {
    let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_logweir"));
    cmd.args(["check", "run"]);
    let run = bounded_output(&mut cmd, std::time::Duration::from_secs(30));
    assert_eq!(
        run.code,
        Some(1),
        "missing required flags is a usage error, not a drill result"
    );
}

// ===========================================================================
// 12. The controller's invocation and this runner's are ONE contract
// ===========================================================================

/// `weirkeeper::check::job` writes the argv and the environment; this runner
/// reads them. The controller crate is not a dependency of this test binary,
/// so the assertion reads its source — which is the point: a rename on either
/// side fails here rather than at 03:00 in a cluster.
#[test]
fn the_controller_and_the_runner_agree_on_the_invocation() {
    let src = std::fs::read_to_string(repo_root().join("crates/weirkeeper/src/check/job.rs"))
        .expect("the controller's check job module is readable");
    for (name, value) in [
        ("CONTRACT_VERSION_ENV", check::CONTRACT_VERSION_ENV),
        ("PLAN_SHA256_ENV", check::PLAN_SHA256_ENV),
        ("SUBJECT_UID_ENV", check::SUBJECT_UID_ENV),
    ] {
        assert!(
            src.contains(&format!("pub const {name}: &str = \"{value}\";")),
            "the controller no longer spells {name} as `{value}`"
        );
    }
    // The argv, word for word.
    for word in [
        "\"check\".to_string()",
        "\"run\".to_string()",
        "\"--plan\".to_string()",
        "\"--check-contract-version\".to_string()",
        "CHECK_MOUNT_PATH}/{CHECK_PLAN_KEY}",
    ] {
        assert!(
            src.contains(word),
            "the controller's `runner_argv` no longer writes `{word}`"
        );
    }
    assert!(
        src.contains("pub const CHECK_MOUNT_PATH: &str = \"/check\";")
            && src.contains("pub const CHECK_PLAN_KEY: &str = \"check-plan.json\";"),
        "the plan mount moved; `--plan /check/check-plan.json` is D2 §4.2's argv"
    );
    // And the contract version the controller writes is the one this build
    // implements.
    assert!(
        src.contains("logweir_core::check_contract::CHECK_CONTRACT_VERSION.to_string()"),
        "the controller must write the shared contract constant, not a literal"
    );
    assert_eq!(CHECK_CONTRACT_VERSION, 1);
}

/// The relay budget the controller reads and the one this runner writes are
/// the same number.
#[test]
fn the_relay_bounds_are_the_controllers() {
    let src = std::fs::read_to_string(repo_root().join("crates/weirkeeper/src/check/relay.rs"))
        .expect("the controller's relay module is readable");
    assert!(
        src.contains("pub const RELAY_LIMIT_BYTES: i64 = 8 * 1024 * 1024;"),
        "the controller's log read is no longer 8 MiB"
    );
    let budget = logweir_core::check_contract::DEFAULT_RELAY_BUDGET_BYTES;
    let read_limit: usize = 8 * 1024 * 1024;
    assert!(
        budget < read_limit,
        "the relay budget ({budget}) must stay under the controller's log read ({read_limit})"
    );
}

// ===========================================================================
// 13. Live rows, against the compose stack (`just e2e-up`)
// ===========================================================================
//
// `#[cfg(feature = "e2e")]` per row, this repository's gating convention: under
// the default feature set the module compiles to nothing, so
// `cargo test -p logweir` skips it cleanly when the stack is absent, and
// `crates/logweir/tests/no_network_in_unit_tests.rs` carries this file with
// that reason.
//
//     just e2e-up
//     cargo test --locked -p logweir --features e2e --test check_cli -- --test-threads=1
//     just e2e-down
#[cfg(feature = "e2e")]
mod live {
    use super::*;

    /// The compose stack's SASL listener, published on the host.
    /// `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP` makes `SASLEXT` SASL_PLAINTEXT,
    /// so this is SCRAM over a CLEAR transport — `tls: false` — which is the
    /// combination `AuthConfig::with_tls_ca_file` refuses a CA for, and the
    /// one the e2e stack really serves.
    fn sasl_bootstrap() -> String {
        std::env::var("LOGWEIR_TEST_SASL_BOOTSTRAP").unwrap_or_else(|_| "localhost:9097".into())
    }

    fn plain_bootstrap() -> String {
        std::env::var("LOGWEIR_TEST_BOOTSTRAP").unwrap_or_else(|_| "localhost:9092".into())
    }

    fn s3_endpoint() -> String {
        std::env::var("LOGWEIR_TEST_S3_ENDPOINT").unwrap_or_else(|_| "http://localhost:9000".into())
    }

    const SCRAM_USER: &str = "logweir";
    /// The compose stack's own fixture credential
    /// (`e2e/compose/docker-compose.yml`'s `scram-setup`). It is a FIXTURE and
    /// is published in that file; it authenticates nothing outside this
    /// stack.
    const SCRAM_PASSWORD_VAR: &str = "LOGWEIR_CHECK_E2E_SASL_PASSWORD";
    const MINIO_USER: &str = "minioadmin";
    const MINIO_PASSWORD_VAR: &str = "AWS_SECRET_ACCESS_KEY";

    fn sasl_connection() -> ConnectionPlan {
        ConnectionPlan {
            bootstrap_servers: vec![sasl_bootstrap()],
            auth_mode: "scramSha512".to_string(),
            username: Some(SCRAM_USER.to_string()),
            password_env: Some(SCRAM_PASSWORD_VAR.to_string()),
            tls: Some(false),
            ca_file: None,
            client_cert_file: None,
            client_key_file: None,
            principal: format!("User:{SCRAM_USER}"),
        }
    }

    fn minio_destination(prefix: &str) -> DestinationPlan {
        let loc = DestinationLocation {
            provider: StorageProvider::S3,
            bucket: "kafka-backups".to_string(),
            prefix: prefix.to_string(),
            region: Some("us-east-1".to_string()),
            endpoint: Some(s3_endpoint()),
            addressing: Addressing::PathStyle,
            // The compose MinIO serves plaintext on 9000, and D2 R3 makes that
            // reachable ONLY through an explicit `InsecureHTTP` transport —
            // never derived from the endpoint scheme (D-SEAMS S5).
            transport: TransportSecurity::InsecureHttp,
        };
        DestinationPlan {
            location_digest: loc.location_digest(),
            location: loc,
            name: "compose-minio".to_string(),
            uid: DEST_UID.to_string(),
            ca_file: None,
            credentials: CredentialMode::Static,
            grant_bindings: Vec::new(),
        }
    }

    /// Drive the SHIPPED wiring — a real broker client, a real object store —
    /// through the real emission path, and decode with the controller's
    /// decoder.
    fn drive_live(m: &Mounted) -> Run {
        let mut buf: Vec<u8> = Vec::new();
        let code = check::execute_with(&m.loaded, &mut buf, &kinds::Live);
        let stdout = String::from_utf8(buf).expect("frames are UTF-8");
        let mut decoder = Decoder::new();
        let mut error = None;
        for line in stdout.lines() {
            if let Err(e) = decoder.push_line(line) {
                error = Some(e);
                break;
            }
        }
        let relay = match error {
            Some(e) => Err(e),
            None => decoder.finish(&FrameExpectations {
                plan_sha256: m.sha256.clone(),
                subject_uid: SUBJECT_UID.to_string(),
            }),
        };
        Run {
            code,
            stdout,
            relay,
        }
    }

    /// A real `topicInventory` against the compose broker's SASL listener.
    #[test]
    fn a_topic_inventory_over_sasl_lists_the_compose_broker() {
        std::env::set_var(SCRAM_PASSWORD_VAR, "logweir-e2e-not-a-secret");
        let mut plan = inventory_plan(5_000, 6 * 1024 * 1024);
        if let CheckRequest::TopicInventory(r) = &mut plan.request {
            r.connection = sasl_connection();
            r.expected_topics = vec![
                "logweir.scratch".to_string(),
                "not-a-real-topic".to_string(),
            ];
        }
        let m = mount(&plan);
        let run = drive_live(&m);
        assert_eq!(run.code, ExitCode::Ok, "stdout:\n{}", run.stdout);
        let inv = run.result().inventory.expect("an inventory block");
        assert!(
            inv.cluster_id.as_deref().is_some_and(|c| !c.is_empty()),
            "a real broker names a cluster id: {inv:?}"
        );
        assert!(inv.broker_count.unwrap_or(0) >= 1);
        let relay = run.relay.as_ref().expect("the relay decodes");
        assert!(
            relay.topics.iter().any(|t| t.name == "logweir.scratch"),
            "the compose stack creates `logweir.scratch`: {:?}",
            relay.topics.iter().map(|t| &t.name).collect::<Vec<_>>()
        );
        assert!(
            !relay.topics.iter().any(|t| t.internal),
            "internal topics are excluded by default"
        );
        assert_eq!(inv.expected.requested, 2);
        assert_eq!(
            inv.expected.visible, 1,
            "`logweir.scratch` is in the listing"
        );
        assert_eq!(
            inv.expected.not_found, 1,
            "a targeted request for a name that is not there answers UNKNOWN_TOPIC_OR_PARTITION"
        );
        assert!(
            !run.everything().contains("logweir-e2e-not-a-secret"),
            "the SASL password reached a frame"
        );
    }

    /// A refused SASL credential is an AUTHENTICATION answer, not a timeout —
    /// D2 §4.2's `[VERIFY U5]`, from the runner's side.
    #[test]
    fn a_refused_sasl_credential_is_authentication_failed() {
        std::env::set_var(SCRAM_PASSWORD_VAR, "this-password-is-wrong");
        let mut plan = inventory_plan(100, 1 << 20);
        if let CheckRequest::TopicInventory(r) = &mut plan.request {
            r.connection = sasl_connection();
        }
        // A short budget, so a mis-classification as a timeout is visible.
        plan.timeout_seconds = 20;
        let m = mount(&plan);
        let run = drive_live(&m);
        std::env::set_var(SCRAM_PASSWORD_VAR, "logweir-e2e-not-a-secret");
        assert_eq!(run.code, ExitCode::Ok);
        let row = run.row(CheckId::ConnectionAuthenticated);
        assert_eq!(
            row.code,
            CheckCode::AuthenticationFailed,
            "a wrong SCRAM password must not be reported as an unreachable broker: {row:?}"
        );
        assert!(
            !run.everything().contains("this-password-is-wrong"),
            "the refused password reached a frame"
        );
    }

    /// `destinationAccess` against real MinIO: a denial and a not-found are
    /// different answers.
    #[test]
    fn a_destination_access_against_minio_tells_denial_from_not_found() {
        // (a) NOT-FOUND, with the credentials MinIO knows. The probe key does
        //     not exist, the backend says so, and that IS the grant.
        std::env::set_var("AWS_ACCESS_KEY_ID", MINIO_USER);
        std::env::set_var(MINIO_PASSWORD_VAR, "minioadmin");
        std::env::set_var("AWS_REGION", "us-east-1");
        let plan = plan_of(CheckRequest::DestinationAccess(DestinationAccessRequest {
            destination: minio_destination("mvp-demo"),
            roles: vec![
                DestinationRole::ArchiveRead,
                DestinationRole::EvidenceRead,
                DestinationRole::EvidenceWrite,
            ],
            write_probe: true,
            evidence_write: None,
            evidence_read: None,
        }));
        let m = mount(&plan);
        let run = drive_live(&m);
        assert_eq!(run.code, ExitCode::Ok, "stdout:\n{}", run.stdout);
        assert_eq!(
            run.row(CheckId::DestinationEvidenceReadable).code,
            CheckCode::EvidenceReadable,
            "a `get` of an absent key that ANSWERS is the grant"
        );
        assert_eq!(
            run.row(CheckId::DestinationArchiveListable).code,
            CheckCode::ArchiveListable
        );
        let marker = run.row(CheckId::DestinationEvidenceWritable);
        assert!(
            matches!(
                marker.code,
                CheckCode::MarkerWritten | CheckCode::MarkerAlreadyPresent
            ),
            "the create-only marker is write-authorised on a fresh stack and on a re-run: \
             {marker:?}"
        );
        assert_eq!(marker.state, CheckState::Ready);

        // (b) DENIAL, with a credential MinIO does not know. A wrong key is
        //     classified as a CREDENTIAL problem, never as a missing grant and
        //     never as absence.
        std::env::set_var("AWS_ACCESS_KEY_ID", "AKIANOTAREALKEYAAAAA");
        std::env::set_var(MINIO_PASSWORD_VAR, "not-the-minio-password");
        let run = drive_live(&m);
        std::env::set_var("AWS_ACCESS_KEY_ID", MINIO_USER);
        std::env::set_var(MINIO_PASSWORD_VAR, "minioadmin");
        let row = run.row(CheckId::DestinationEvidenceReadable);
        assert_ne!(
            row.code,
            CheckCode::ObjectNotFound,
            "a refused credential must never be reported as absence: {row:?}"
        );
        assert!(
            matches!(
                row.code,
                CheckCode::InvalidCredentials | CheckCode::AccessDenied
            ),
            "a wrong key is a credential answer: {row:?}"
        );
        assert_eq!(row.state, CheckState::NotReady);
        assert!(
            !run.everything().contains("not-the-minio-password"),
            "the refused secret reached a frame"
        );
    }

    /// `catalogSync` against REAL MinIO.
    ///
    /// # What a server shows that a `BTreeMap` cannot
    ///
    /// The in-memory fake answers `list_page` with `starts_with` over a sorted
    /// map. A real backend does neither: `ObjectStore::list` matches a prefix
    /// on a PATH SEGMENT basis and contracts **no ordering at all**, and
    /// `list_with_offset` pushes the cursor down into the request. Three of
    /// this kind's claims rest on exactly those behaviours — the day-shard
    /// listing, the `Full` rescan's resume, and `NotFound` being a different
    /// answer from every other storage error — and none of them is worth
    /// making against a double.
    ///
    /// It also proves the one thing a unit test cannot: the walk runs through
    /// `kinds::Live`, i.e. `Store::read_only_with`, a handle that physically
    /// cannot put.
    #[test]
    fn a_catalog_sync_walks_a_real_archive_and_writes_nothing() {
        std::env::set_var("AWS_ACCESS_KEY_ID", MINIO_USER);
        std::env::set_var(MINIO_PASSWORD_VAR, "minioadmin");
        std::env::set_var("AWS_REGION", "us-east-1");

        // A FRESH id per run. Every key under `logweir/` is create-only and
        // the compose volume persists, so a fixed id would pass once and
        // `AlreadyExists` forever after.
        let stamp = Utc::now().timestamp_millis();
        let backup_id = format!("e2e-cs-{stamp}");
        let mut receipt = catalog_receipt(&backup_id, "run-a", &Utc::now().to_rfc3339());
        // The manifest lives under `logweir/` for this fixture, because the ONE
        // writable handle Logweir can build is rooted there (Global Constraint
        // 6) and the runner reads whatever key the signed receipt names.
        receipt.archive.manifest_key = format!("logweir/e2e-catalog/{backup_id}/manifest.json");
        let f = catalog_fixture(
            &receipt,
            "s3://kafka-backups/mvp-demo",
            &claimed_sidecar(CATALOG_CLAIMED_KEY_ID),
            CATALOG_CLAIMED_KEY_ID,
        );

        // Seed through a WRITABLE handle this test builds for itself. The
        // runner holds none.
        let url = logweir_core::engine::StorageUrl::S3 {
            bucket: "kafka-backups".to_string(),
            prefix: "logweir/".to_string(),
            region: Some("us-east-1".to_string()),
            endpoint: Some(s3_endpoint()),
            path_style: true,
            allow_http: true,
        };
        let opts = logweir_engine_oso::storage::StoreOptions::static_from_env()
            .with_request_timeout(std::time::Duration::from_secs(20));
        let writer = logweir_engine_oso::storage::Store::from_url_with(&url, &opts)
            .expect("a writable evidence handle over the compose MinIO");
        for (key, bytes) in [
            (&f.log_key, &f.log_bytes),
            (&f.record_key, &f.record_bytes),
            (&f.receipt_key, &f.receipt_bytes),
            (&f.sidecar_key, &f.sidecar_bytes),
            (&f.manifest_key, &CATALOG_MANIFEST.to_vec()),
        ] {
            writer
                .put_create_only(key, bytes)
                .unwrap_or_else(|e| panic!("seeding `{key}`: {e}"));
        }

        let before = writer
            .list_page(
                &format!("logweir/catalog/v1/points/{}/", f.point.point_id),
                None,
                10,
            )
            .expect("the seeded point lists")
            .0;
        assert_eq!(
            before,
            vec![f.record_key.clone()],
            "the record, and nothing else under the point prefix: this fixture writes no \
             `record.sig`, because the runner verifies the RECEIPT and a record signature is \
             not the verification root (D3 §5.2 rule 3)"
        );

        let plan = CheckPlan {
            timeout_seconds: 300,
            ..plan_of(CheckRequest::CatalogSync(Box::new(
                logweir_core::check_contract::CatalogSyncRequest {
                    destination: minio_destination("mvp-demo"),
                    ..sync_request()
                },
            )))
        };
        let m = mount(&plan);
        let run = drive_live(&m);
        assert_eq!(run.code, ExitCode::Ok, "stdout:\n{}", run.stdout);
        let body = body_of(&run);
        let entries = entries_of(&body);
        let mine = entries
            .iter()
            .find(|e| e["pointId"] == f.point.point_id.as_str())
            .unwrap_or_else(|| panic!("the seeded point is not in the body:\n{body}"));
        assert_eq!(
            mine["availability"], "Available",
            "receipt, sidecar and manifest all read, and the manifest digest is the receipt's"
        );
        assert_eq!(
            mine["signature"], "notAttempted",
            "no trust bundle is mounted"
        );
        assert_eq!(mine["receiptSha256"], f.point.receipt.sha256);
        assert_eq!(
            run.row(CheckId::DestinationArchiveListable).code,
            CheckCode::ArchiveListable
        );

        // A `Full` rescan over the same archive, with the cursor pushed down
        // into the request rather than filtered after the fact.
        let full = CheckPlan {
            timeout_seconds: 300,
            ..plan_of(CheckRequest::CatalogSync(Box::new(
                logweir_core::check_contract::CatalogSyncRequest {
                    destination: minio_destination("mvp-demo"),
                    mode: logweir_core::check_contract::CatalogSyncMode::Full,
                    ..sync_request()
                },
            )))
        };
        let rescan = drive_live(&mount(&full));
        let rescan_body = body_of(&rescan);
        assert!(
            entries_of(&rescan_body)
                .iter()
                .any(|e| e["pointId"] == f.point.point_id.as_str()),
            "a Full rescan finds the same point:\n{rescan_body}"
        );

        // AND IT WROTE NOTHING. The point prefix holds exactly what this test
        // put there.
        let after = writer
            .list_page(
                &format!("logweir/catalog/v1/points/{}/", f.point.point_id),
                None,
                10,
            )
            .expect("the point still lists")
            .0;
        assert_eq!(
            after, before,
            "a catalog sync writes nothing at all — not even the create-only readiness marker a \
             destinationAccess may write"
        );
    }

    /// An `evidenceFetch` of a receipt a REAL `logweir backup run` wrote.
    #[test]
    fn an_evidence_fetch_relays_a_receipt_a_real_backup_wrote() {
        std::env::set_var("AWS_ACCESS_KEY_ID", MINIO_USER);
        std::env::set_var(MINIO_PASSWORD_VAR, "minioadmin");
        std::env::set_var("AWS_REGION", "us-east-1");
        let dir = tempfile::tempdir().unwrap();
        // A TOPIC OF THIS ROW'S OWN, with records in it.
        //
        // `topic-setup` creates `test-topic` and produces nothing into it, and
        // `logweir backup run` refuses a backup set that "declares no segment
        // for any of the named topics" — correctly: an archive of an empty
        // topic bounds no window, and a receipt naming one would attest to
        // nothing. So this row seeds its own topic rather than depending on
        // `scripts/e2e-seed.sh`, and it does not touch `test-topic`, which
        // other e2e suites assert the shape of.
        let source_topic = "check-e2e-src";
        let seed = std::process::Command::new("docker")
            .args([
                "compose",
                "-f",
                &repo_root()
                    .join("e2e/compose/docker-compose.yml")
                    .to_string_lossy(),
                "exec",
                "-T",
                "kafka-broker-1",
                "bash",
                "-c",
            ])
            .arg(format!(
                "/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-broker-1:9094 \
                 --create --if-not-exists --topic {source_topic} --partitions 1 \
                 --replication-factor 1 && seq 1 200 | \
                 /opt/kafka/bin/kafka-console-producer.sh \
                 --bootstrap-server kafka-broker-1:9094 --topic {source_topic}"
            ))
            .output()
            .expect("the compose broker is reachable through `docker compose exec`");
        assert!(
            seed.status.success(),
            "seeding `{source_topic}` failed:\n{}\n{}",
            String::from_utf8_lossy(&seed.stdout),
            String::from_utf8_lossy(&seed.stderr)
        );

        let backup_id = format!(
            "check-e2e-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
        );
        let spec = format!(
            concat!(
                "backup_id: {id}\n",
                "source:\n",
                "  bootstrap_servers: [{bootstrap}]\n",
                "  topics: [{topic}]\n",
                "storage:\n",
                "  backend: s3\n",
                "  bucket: kafka-backups\n",
                "  prefix: {id}\n",
                "  region: us-east-1\n",
                "  endpoint: {endpoint}\n",
                "  path_style: true\n",
                "  allow_http: true\n",
            ),
            id = backup_id,
            topic = source_topic,
            bootstrap = plain_bootstrap(),
            endpoint = s3_endpoint()
        );
        std::fs::write(dir.path().join("backup.yaml"), spec).unwrap();
        std::fs::write(
            dir.path().join("allowed.json"),
            br#"{"allowed_cluster_ids":[],"source_cluster_id":null}"#,
        )
        .unwrap();

        // THE ENGINE ROUTE, resolved the way `scripts/demo.sh` resolves it and
        // never hardcoded: upstream publishes `osodevops/kafka-backup` for
        // linux/amd64 only, so on darwin/arm64 `.engine/kafka-backup` cannot be
        // exec'd at all (ENOEXEC) and `e2e/fixtures/engine-docker.sh` is the
        // stand-in. The version in the SIGNED receipt has to describe the
        // binary that really ran, which is why it is read off `--version`
        // rather than typed here (global ruling GR8 permits `--version`).
        let native = repo_root().join(".engine/kafka-backup");
        let shim = repo_root().join("e2e/fixtures/engine-docker.sh");
        let engine_bin = if std::process::Command::new(&native)
            .arg("--version")
            .output()
            .is_ok_and(|o| o.status.success())
        {
            native
        } else {
            shim
        };
        let version_out = std::process::Command::new(&engine_bin)
            .arg("--version")
            .output()
            .expect("the engine answers --version");
        assert!(
            version_out.status.success(),
            "neither the native engine nor the container shim answered --version;              `just engine` extracts the first and the second needs the pinned image: {}",
            String::from_utf8_lossy(&version_out.stderr)
        );
        let engine_version = String::from_utf8_lossy(&version_out.stdout)
            .split_whitespace()
            .last()
            .expect("--version prints a version")
            .to_string();
        let engine_digest =
            std::fs::read_to_string(repo_root().join("third_party/kafka-backup-binary.digest"))
                .expect("the pinned digest is readable")
                .trim()
                .to_string();
        // The ONE host directory the engine reads and writes, mounted at the
        // SAME path inside the container so every absolute path Logweir
        // rendered is valid there too (`e2e/fixtures/engine-docker.sh`).
        let engine_mount = repo_root().join(".e2e/tmp");
        std::fs::create_dir_all(&engine_mount).unwrap();

        let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_logweir"));
        cmd.args([
            "backup",
            "run",
            "--spec",
            &dir.path().join("backup.yaml").to_string_lossy(),
            "--allowed-clusters",
            &dir.path().join("allowed.json").to_string_lossy(),
            "--signing-key",
            &repo_root()
                .join("e2e/fixtures/signed/signing.pem")
                .to_string_lossy(),
        ]);
        cmd.env("LOGWEIR_ENGINE_BIN", &engine_bin)
            .env("LOGWEIR_ENGINE_VERSION", &engine_version)
            .env("LOGWEIR_ENGINE_DIGEST", &engine_digest)
            .env("LOGWEIR_E2E_ENGINE_MOUNT", &engine_mount)
            .env("TMPDIR", &engine_mount)
            .env("AWS_ACCESS_KEY_ID", MINIO_USER)
            .env("AWS_SECRET_ACCESS_KEY", "minioadmin")
            .env("AWS_REGION", "us-east-1");
        let backup = bounded_output(&mut cmd, std::time::Duration::from_secs(300));
        assert_eq!(
            backup.code,
            Some(0),
            "`logweir backup run` did not succeed.\nstdout:\n{}\nstderr:\n{}",
            backup.stdout,
            backup.stderr
        );
        let receipt_key = backup
            .stdout
            .lines()
            .rev()
            .find_map(|l| l.strip_prefix("receipt-key="))
            .expect("`backup run` prints `receipt-key=` on success")
            .to_string();

        let plan = plan_of(CheckRequest::EvidenceFetch(EvidenceFetchRequest {
            destination: minio_destination(&backup_id),
            objects: vec![
                EvidenceObjectRequest {
                    role: DestinationRole::EvidenceRead,
                    key: receipt_key.clone(),
                    max_bytes: 1024 * 1024,
                    stream: Stream::EvidencePayload,
                },
                EvidenceObjectRequest {
                    role: DestinationRole::EvidenceRead,
                    key: receipt_key.replace(".receipt.json", ".receipt.sig"),
                    max_bytes: 64 * 1024,
                    stream: Stream::EvidenceSidecar,
                },
            ],
        }));
        let m = mount(&plan);
        let run = drive_live(&m);
        assert_eq!(run.code, ExitCode::Ok, "stdout:\n{}", run.stdout);
        let relay = run.relay.as_ref().expect("the relay decodes");
        let payload = relay
            .stream(Stream::EvidencePayload)
            .expect("the receipt was relayed");
        let receipt: logweir_core::backup_receipt::BackupReceipt =
            serde_json::from_slice(payload).expect("the relayed bytes are the receipt");
        assert_eq!(receipt.backup_id, backup_id);
        assert!(
            relay.stream(Stream::EvidenceSidecar).is_some(),
            "the DSSE sidecar was relayed on its own stream"
        );
        for e in &run.result().evidence {
            assert!(e.present, "{e:?}");
            assert!(!e.truncated);
        }
        // The relayed digest is the object's.
        assert_eq!(
            run.result().evidence[0].sha256.as_deref(),
            Some(logweir_core::ids::sha256_prefixed(payload).as_str())
        );
        assert!(
            !run.everything().contains("minioadmin"),
            "the MinIO credential reached a frame"
        );
    }

    /// **FX-31, live on the compose MinIO.** An evidence fetch of an object
    /// over the plan's `maxBytes` reports it present and `truncated`, relays
    /// NO bytes for it and never reads it; a normal object in the same fetch
    /// is relayed whole.
    ///
    /// The oversized object is seeded here at 4 MiB. For a memory reading,
    /// where the object must not live in this process, seed it out of process
    /// and name it in `FX31_LIVE_OVERSIZED_KEY` (`claude/fx-31.result.md` §5
    /// seeds 768 MiB with `curl --aws-sigv4` and reads this process's peak RSS
    /// with `/usr/bin/time`).
    #[test]
    fn an_evidence_fetch_of_an_oversized_receipt_relays_nothing() {
        std::env::set_var("AWS_ACCESS_KEY_ID", MINIO_USER);
        std::env::set_var(MINIO_PASSWORD_VAR, "minioadmin");
        let run_id = format!(
            "fx31-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let url = logweir_core::engine::StorageUrl::S3 {
            bucket: "kafka-backups".to_string(),
            prefix: "logweir/".to_string(),
            region: Some("us-east-1".to_string()),
            endpoint: Some(s3_endpoint()),
            path_style: true,
            allow_http: true,
        };
        let opts = logweir_engine_oso::storage::StoreOptions::static_from_env()
            .with_request_timeout(std::time::Duration::from_secs(20));
        let writer = logweir_engine_oso::storage::Store::from_url_with(&url, &opts)
            .expect("a writable evidence handle over the compose MinIO");
        let normal_key = format!("logweir/backups/{run_id}/normal.receipt.json");
        let normal = br#"{"fx31":"a receipt well under the cap"}"#;
        writer
            .put_create_only(&normal_key, normal)
            .expect("the normal object is seeded");
        let oversized_key = match std::env::var("FX31_LIVE_OVERSIZED_KEY") {
            Ok(key) => key,
            Err(_) => {
                let key = format!("logweir/backups/{run_id}/oversized.receipt.json");
                writer
                    .put_create_only(&key, &vec![b' '; 4 << 20])
                    .expect("the oversized object is seeded");
                key
            }
        };

        let plan = plan_of(CheckRequest::EvidenceFetch(EvidenceFetchRequest {
            destination: minio_destination("fx31"),
            objects: vec![
                EvidenceObjectRequest {
                    role: DestinationRole::EvidenceRead,
                    key: oversized_key.clone(),
                    max_bytes: 1024 * 1024,
                    stream: Stream::EvidencePayload,
                },
                EvidenceObjectRequest {
                    role: DestinationRole::EvidenceRead,
                    key: normal_key.clone(),
                    max_bytes: 64 * 1024,
                    stream: Stream::EvidenceSidecar,
                },
            ],
        }));
        let m = mount(&plan);
        let run = drive_live(&m);
        assert_eq!(run.code, ExitCode::Ok, "stdout:\n{}", run.stdout);
        let evidence = &run.result().evidence;
        eprintln!("[fx31-live] evidence-fetch result: {evidence:?}");
        let big = evidence
            .iter()
            .find(|e| e.key == oversized_key)
            .expect("an answer for the oversized object");
        assert!(big.present && big.truncated, "{big:?}");
        assert_eq!((big.bytes, big.sha256.as_deref()), (None, None), "{big:?}");
        let relay = run.relay.as_ref().expect("the relay decodes");
        assert!(
            relay.stream(Stream::EvidencePayload).is_none(),
            "no byte of the oversized object was relayed"
        );
        let small = evidence
            .iter()
            .find(|e| e.key == normal_key)
            .expect("an answer for the normal object");
        assert!(small.present && !small.truncated, "{small:?}");
        assert_eq!(
            relay.stream(Stream::EvidenceSidecar),
            Some(&normal[..]),
            "the normal object is relayed whole"
        );
    }
}

// ===========================================================================
// 14. Review fixes (2026-09-17)
// ===========================================================================

/// **F1 / reviewer probe R3.** TLS without SASL is REFUSED, not downgraded.
///
/// `AuthSpec::Plaintext` has no TLS field, so a mapping that answered it for
/// `authMode: plaintext` would DISCARD `ConnectionPlan::tls` and dial a
/// listener the operator believes is encrypted in the clear. `probe.rs` refuses
/// exactly this shape ("this is the runner's half") and
/// `weirkeeper::connection` refuses it before any Job exists; the check runner
/// is the fifth dialling site and must refuse it too (D-SEAMS S5).
#[test]
fn plaintext_with_tls_is_refused_and_never_dialled_in_the_clear() {
    let mut plan = connection();
    plan.tls = Some(true);
    let err = logweir::check::kafka::auth_spec(&plan)
        .expect_err("TLS without SASL is refused, not downgraded");
    assert_eq!(err.code, CheckCode::AuthenticationFailed);
    assert!(
        err.message.contains("tls: true") && err.message.contains("refused"),
        "the refusal must name the shape it refused: {}",
        err.message
    );
    // `auth_config` refuses at the same point, so no `AuthConfig` is ever
    // built for the shape.
    assert!(logweir::check::kafka::auth_config(&plan).is_err());

    // And the WHOLE runner reports it as a connection row rather than dialling.
    let mut inv = inventory_plan(100, 1 << 20);
    if let CheckRequest::TopicInventory(r) = &mut inv.request {
        r.connection = plan.clone();
    }
    let m = mount(&inv);
    let probe = FakeProbe::new();
    // A wiring whose broker WOULD succeed: the refusal has to come from the
    // mapping, not from the fake.
    let run = drive(&m, &FakeWiring::default().with_probe(probe));
    assert_eq!(run.code, ExitCode::Ok);
    // The fake `Wiring` does not call `auth_spec`, so this row asserts the
    // SHIPPED wiring's own refusal instead.
    let live_err = kinds::Live
        .broker(&plan, std::time::Duration::from_secs(5))
        .err()
        .expect("the shipped wiring refuses the shape before it builds a client");
    assert_eq!(live_err.code, CheckCode::AuthenticationFailed);

    // The three legal shapes still map.
    assert!(logweir::check::kafka::auth_spec(&connection()).is_ok());
    let mut clear = connection();
    clear.tls = None;
    assert!(logweir::check::kafka::auth_spec(&clear).is_ok());
    let scram = ConnectionPlan {
        auth_mode: "scramSha512".to_string(),
        username: Some("backup".to_string()),
        tls: Some(true),
        ..connection()
    };
    assert!(logweir::check::kafka::auth_spec(&scram).is_ok());
}

/// **F1, the other half.** The check client carries NO private rdkafka
/// configuration: `security.protocol`, `sasl.*`, the hostname pin and the
/// trust anchor all come from `RdKafkaReader::client_config`, the ONE
/// implementation the drill reader and the check share.
///
/// Asserted over `logweir-kafka`'s source, because a private copy there would
/// be invisible from this crate and is exactly how the two came to disagree
/// once already (a check against a private-CA broker failing to verify the CA
/// while a drill against the same broker succeeded).
#[test]
fn the_check_client_shares_the_readers_client_config() {
    let src = std::fs::read_to_string(repo_root().join("crates/logweir-kafka/src/inventory.rs"))
        .expect("the inventory module is readable");
    assert!(
        src.contains("crate::rdkafka_reader::RdKafkaReader::client_config("),
        "`KafkaInventory` no longer derives its client configuration from the reader's; a \
         second copy is a second place the hostname pin and the trust anchor can drift"
    );
    // The ONE thing overridden afterwards, and it is not a control.
    assert!(src.contains("cfg.set(\"client.id\", CHECK_CLIENT_ID);"));
    // ...and this crate builds no `ClientConfig` of its own.
    for (path, code) in check_sources() {
        assert!(
            !code.contains("ClientConfig"),
            "{path} builds an rdkafka configuration; the check client must go through \
             `RdKafkaReader::client_config`"
        );
    }
}

/// **F2 / reviewer probes R1 and R2.** The `details` stream is redacted like
/// every other stream.
///
/// The M4 row could not reach this: its restore case forces a
/// `PlanHashMismatch`, so `archive_checks` and `target_checks` never run. These
/// two cases reach `archive.segments` and `target.mappedTopics` with a planted
/// key in a segment key and in a mapped topic name, and assert the DECODED
/// details stream — which the controller writes verbatim into an immutable
/// `<job>-details` ConfigMap — carries neither.
#[test]
fn the_details_stream_is_redacted_like_every_other_stream() {
    // R1: a mapped target topic that exists, whose name carries the key.
    let poisoned = format!("o-{PLANTED_KEY_ID}");
    let yaml = restore_yaml(&ms_to_rfc3339(INSIDE_MS), &[poisoned.as_str()], "scratch");
    let m = mount(&restore_plan(&yaml, None));
    let mut manifest = manifest_json();
    manifest["topics"][0]["name"] = serde_json::Value::String(poisoned.clone());
    let probe = FakeProbe::new()
        .with_presence("logweir.scratch", TopicPresence::Present { partitions: 1 })
        .with_presence(
            &format!("restore-{poisoned}"),
            TopicPresence::Present { partitions: 6 },
        );
    let run = drive(&m, &restore_wiring(&yaml, &manifest, probe));
    assert_eq!(
        run.row(CheckId::TargetMappedTopics).code,
        CheckCode::MappedTopicExists,
        "the row this case exists to reach did not run"
    );
    let details = String::from_utf8(
        run.relay
            .as_ref()
            .unwrap()
            .stream(Stream::Details)
            .expect("a details stream")
            .to_vec(),
    )
    .unwrap();
    assert!(
        details.contains("mappedTopicExists"),
        "the details line was not written at all: {details}"
    );
    assert!(
        !run.everything().contains(PLANTED_KEY_ID),
        "`{PLANTED_KEY_ID}` reached the details stream:\n{details}"
    );

    // R2: a MISSING segment whose key carries the key.
    let yaml = restore_yaml(&ms_to_rfc3339(INSIDE_MS), &["orders"], "scratch");
    let m = mount(&restore_plan(&yaml, None));
    let mut manifest = manifest_json();
    manifest["topics"][0]["partitions"][0]["segments"][1]["key"] = serde_json::Value::String(
        format!("{BACKUP_ID}/topics/orders/partition=0/segment-{PLANTED_KEY_ID}.bin"),
    );
    let partial = FakeObjects::new()
        .with_prefix(ARCHIVE_PREFIX)
        .with_object(MANIFEST_OBJECT_KEY, &serde_json::to_vec(&manifest).unwrap())
        .with_object(
            &format!("kafka-backups/{BACKUP_ID}/topics/orders/partition=0/segment-0.bin"),
            b"segment",
        );
    let run = drive(
        &m,
        &FakeWiring::default()
            .with_file(PLAN_FILE, yaml.as_bytes())
            .with_probe(FakeProbe::new())
            .with_role(DestinationRole::ArchiveRead, partial),
    );
    assert_eq!(
        run.row(CheckId::ArchiveSegments).code,
        CheckCode::SegmentMissing,
        "the row this case exists to reach did not run"
    );
    let details = String::from_utf8(
        run.relay
            .as_ref()
            .unwrap()
            .stream(Stream::Details)
            .expect("a details stream")
            .to_vec(),
    )
    .unwrap();
    assert!(details.contains("missingSegment"), "{details}");
    assert!(
        !run.everything().contains(PLANTED_KEY_ID),
        "`{PLANTED_KEY_ID}` reached the details stream:\n{details}"
    );
}

/// The one verbatim field is still ONE. The code, `docs/stability.md` and the
/// report all say so; this asserts it over every stream the runner writes.
#[test]
fn only_the_evidence_key_is_written_verbatim() {
    let ordinary = [
        ("logweir/backups/20260916/receipt.json", true),
        ("restore-orders", false),
    ];
    for (value, _) in ordinary {
        assert_eq!(
            logweir_core::check_contract::redact(value),
            value,
            "`{value}` is not credential-shaped and must survive redaction"
        );
    }
    // Every producer of a details line goes through the ONE assembly point.
    let (_, restore) = check_sources()
        .into_iter()
        .find(|(p, _)| p.ends_with("kinds/restore.rs"))
        .unwrap();
    assert_eq!(
        restore.matches("details_stream(").count(),
        2,
        "the details stream has exactly one assembly point and one call site"
    );
    let bytes = kinds::restore::details_stream(&[serde_json::json!({
        "missingSegment": format!("k/{PLANTED_KEY_ID}")
    })
    .to_string()]);
    assert!(!String::from_utf8_lossy(&bytes).contains(PLANTED_KEY_ID));
}

/// The path-aware redactor keeps every SHAPE rule and loses only the long-run
/// one at a `/` boundary — the trade-off its header states.
#[test]
fn a_planted_key_in_a_key_path_is_still_redacted() {
    let cases = [
        format!("kafka-backups/{PLANTED_KEY_ID}/manifest.json"),
        format!("kafka-backups/x/segment-{PLANTED_KEY_ID}.bin"),
        format!("topics/{PLANTED_SECRET}"),
        PLANTED_USERINFO.to_string(),
    ];
    for value in &cases {
        let out = logweir::check::redact_path(value);
        for secret in [
            PLANTED_KEY_ID,
            "hunter2",
            "ZZfakefakefakefakefakefakefakefake01",
        ] {
            assert!(
                !out.contains(secret),
                "`{secret}` survived `redact_path` in `{value}` -> `{out}`"
            );
        }
    }
    // ...and an ORDINARY archive key survives whole.
    let ordinary = "kafka-backups/20260915T030000Z/topics/orders/partition=0/segment-1.bin";
    assert_eq!(logweir::check::redact_path(ordinary), ordinary);
    // Since review finding **F5** the whole-string form keeps it too — a run
    // carrying `topics`, `partition=<n>` or `manifest` is an object key
    // whatever the adopter called their backup set, and blanking this exact
    // message is what D2-REDACT-OVERBROAD is about.
    assert_eq!(
        logweir_core::check_contract::redact(ordinary),
        ordinary,
        "`archive.backupSet`'s refusal must name the key it could not read"
    );

    // WHAT STILL SEPARATES THE TWO, so this test keeps saying something. A
    // run that is key-SHAPED but carries no anchor at all — no UUID, no
    // digest, none of this product's own archive components — is an object key
    // to `redact_path`, whose values are already known to be keys, and is not
    // one to `redact`, which is applied to prose that may say anything.
    let unanchored = "team-alpha/prod-region-one/MyBackupSet01";
    assert!(unanchored.len() >= 40, "the probe must reach the threshold");
    assert_eq!(logweir::check::redact_path(unanchored), unanchored);
    assert_eq!(
        logweir_core::check_contract::redact(unanchored),
        "[redacted]",
        "`redact` must not read an unanchored run as a key: {}",
        logweir_core::check_contract::redact(unanchored)
    );
}

/// **The `redact_path` half of review finding F1.** A credential whose own `/`
/// characters split it is not an object key, and this function used to say it
/// was.
///
/// `redact_path` split the value on `/` and ran the long-run rule over each
/// piece. `/` is base64's 64th character, so the canonical AWS secret access key
/// `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY` — 40 characters, split into 13, 7
/// and 18 — had no piece the rule could see, and came through untouched while
/// `redact` replaced it. Roughly 45% of real keys carry at least one `/`. The
/// header excused the gap by asserting what this function is applied to; nothing
/// checked the assertion, which is the shape of mistake the whole finding is
/// about.
///
/// The DIE half is every form of the key, alone and inside the values
/// `redact_path` really receives: a `missingSegment` JSON line and a catalog
/// `receiptKey`. The KEEP half is the reason the function exists at all, so
/// both are here — a fix that redacted the keys would re-open F7.
#[test]
fn a_slash_bearing_credential_is_not_an_object_key() {
    const AWS: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
    const AWS_ONE_SLASH: &str = concat!("wJalrXUtnFEMIK7MDENGb/", "PxRfiCYEXAMPLEKEY0");
    const AWS_NO_SLASH: &str = concat!("wJalrXUtnFEMIK7MDENGb", "PxRfiCYEXAMPLEKEY01");
    const SET: &str = "3f0ada8f-1a2b-4c3d-9e8f-0123456789ab";
    const DIGEST: &str = "6feecc8c16c5551d9feb3eb5f77e2da773bf68bd9ef9c52927ceb2c86e56892b";

    // --- DIE: no shape of the key survives, in any wrapper ------------------
    let mut leaked: Vec<String> = Vec::new();
    for key in [AWS, AWS_ONE_SLASH, AWS_NO_SLASH] {
        for (shape, value) in [
            ("bare", key.to_string()),
            ("under a prefix", format!("kafka-backups/{key}")),
            ("anchored by a set id", format!("kafka-backups/{SET}/{key}")),
            ("anchored by a digest", format!("{DIGEST}/{key}")),
            (
                "inside a details line",
                serde_json::json!({"check": "archive.segments", "missingSegment": key}).to_string(),
            ),
            (
                "inside a catalog receiptKey",
                serde_json::json!({"receiptKey": format!("logweir/backups/{SET}/{key}")})
                    .to_string(),
            ),
        ] {
            let out = logweir::check::redact_path(&value);
            if out.contains(key) {
                leaked.push(format!("{shape}: {out}"));
            }
        }
    }
    // A base64 component carrying `+` — base64's 63rd character, which no
    // object key contains — inside an otherwise key-shaped path, and short
    // enough that the free-component cap alone would let it through. This is
    // what the `[A-Za-z0-9._=-]` alphabet clause is for; without a row for it,
    // mutant M29 deleted the clause and survived.
    for value in [
        "team/prod/n4bQgYhMfWWaL+qgxVrQFaO/manifest.json".to_string(),
        serde_json::json!({"missingSegment": "team/prod/n4bQgYhMfWWaL+qgxVrQFaO/manifest.json"})
            .to_string(),
    ] {
        assert!(value.len() >= 40);
        let out = logweir::check::redact_path(&value);
        if out.contains("n4bQgYhMfWWaL+qgxVrQFaO") {
            leaked.push(format!("a `+`-bearing component: {out}"));
        }
    }
    assert!(
        leaked.is_empty(),
        "`redact_path` returned a credential whole ({} of 20):\n  {}",
        leaked.len(),
        leaked.join("\n  ")
    );

    // --- KEEP: a real object path still survives, whole ---------------------
    // The two anchors an archive key actually carries. If a fix closes the leak
    // by redacting these, it has re-opened F7 — the `detailsRef` ConfigMap
    // saying "3 missing segments: [redacted], [redacted], [redacted]".
    for keep in [
        format!("team/prod/{SET}/topics/payments/partition=2/segment-00000000000000000000.bin.zst"),
        format!(
            "team/prod/{SET}/topics/payments-EU/partition=2/segment-00000000000000000000.bin.zst"
        ),
        format!("logweir/backups/{SET}/run-a.receipt.json"),
        // The same key as `logweir backup run` really writes it: the run id is
        // a 26-character ULID, two over the free-component budget
        // (CATALOG-RECEIPTKEY-REDACTED).
        format!("logweir/backups/{SET}/01M2VKCST7EF12EW5T2Y7SJ86Q.receipt.json"),
        format!("logweir/blobs/{DIGEST}/manifest.json"),
        format!("kafka-backups/{DIGEST}"),
        "kafka-backups/20260915T030000Z/topics/orders/partition=0/segment-1.bin".to_string(),
    ] {
        assert_eq!(
            logweir::check::redact_path(&keep),
            keep,
            "an object key an operator has to act on was redacted"
        );
        // …and inside the JSON line `details_stream` really applies it to,
        // which is where the per-RUN decision matters: the first `/`-piece of
        // that line is `{"check":"archive.segments","missingSegment":"team`.
        let line =
            serde_json::json!({"check": "archive.segments", "missingSegment": keep}).to_string();
        let out = logweir::check::redact_path(&line);
        assert!(out.contains(&keep), "the details line lost its key: {out}");
        let parsed: serde_json::Value =
            serde_json::from_str(&out).expect("a details line stays JSON");
        assert_eq!(parsed["missingSegment"], keep);
    }
}

/// **CATALOG-RECEIPTKEY-REDACTED, at the function the catalog really calls.**
/// A 26-character ULID run id is an identity; a component merely its length is
/// not.
///
/// REGRESSION REASON. `.` is not a run character, so the run `redact_path`
/// weighs in a receipt key is `<prefix>/<backup_id>/<run_id>` and the run id is
/// its one free component. At 26 characters it was over the free-component
/// budget of 24, so the run went and the catalog published
/// `receiptKey: "[redacted].receipt.json"` — the half of a restore's plan
/// binding that the console cannot reconstruct from anything else.
///
/// The DIE half is what pins the fix as a SHAPE exemption rather than a bigger
/// budget: a 27-character component still goes, and so does a 26-character one
/// that is not a ULID. A cap raised to 26 passes the KEEP half and fails all of
/// these.
#[test]
fn a_ulid_run_id_is_an_object_key_and_a_component_its_length_is_not() {
    const SET: &str = "3f0ada8f-1a2b-4c3d-9e8f-0123456789ab";
    const RUN: &str = "01M2VKCST7EF12EW5T2Y7SJ86Q";
    // The live lab's own prefix, set id and run id, from the D3 W14 capture in
    // `crates/logweir-api/tests/fixtures/catalog-page-entries.jsonl` — whose
    // `receiptKey` column held the defect verbatim
    // (`"[redacted].receipt.json"`) until this fix.
    const LAB: &str = "archive/e1c4ff19-62fb-4d76-9a3f-2177d6fd9d4a/01M2VKCST7EF12EW5T2Y7SJ86Q";

    // --- KEEP: the plan binding survives, bare and in the line it travels in -
    for keep in [
        format!("logweir/backups/{SET}/{RUN}.receipt.json"),
        format!("logweir/backups/{SET}/{RUN}.receipt.sig"),
        format!("logweir/drills/{RUN}.receipt.json"),
        format!("{LAB}.receipt.json"),
    ] {
        assert!(keep.contains(RUN), "the fixture must carry the run id");
        assert!(
            keep.len() >= 40,
            "and must reach the long-run threshold, or it proves nothing: {keep}"
        );
        assert_eq!(
            logweir::check::redact_path(&keep),
            keep,
            "a restore's plan binding was redacted"
        );
        let line = serde_json::json!({"receiptKey": keep}).to_string();
        let out = logweir::check::redact_path(&line);
        let parsed: serde_json::Value = serde_json::from_str(&out).expect("the line stays JSON");
        assert_eq!(parsed["receiptKey"], keep, "inside an entry line: {out}");
    }

    // --- DIE: only the ULID shape is exempt, and only from the LENGTH -------
    let refused: Vec<(&str, String)> = vec![
        ("27 characters", format!("{RUN}X1")),
        ("26, timestamp overflow", format!("Z{}", &RUN[1..])),
        ("26, not the Crockford alphabet", RUN.replacen('T', "U", 1)),
        ("26, mixed case", RUN.replacen('T', "t", 1)),
        ("26 of base64", "wJalrXUtnFEMIK7MDENGbPxRfi".to_string()),
    ];
    let mut survived: Vec<String> = Vec::new();
    for (name, one) in &refused {
        for value in [
            format!("logweir/backups/{SET}/{one}.receipt.json"),
            serde_json::json!({"receiptKey": format!("logweir/backups/{SET}/{one}.receipt.json")})
                .to_string(),
        ] {
            let out = logweir::check::redact_path(&value);
            if out.contains(one.as_str()) {
                survived.push(format!("{name}: {out}"));
            }
        }
    }
    // REVIEW MED-1: and the exemption needs the ANCHOR. `redact_path` reads a
    // run as a key on its SHAPE alone — no UUID, no digest, none of this
    // product's own archive components required — and `locationId` is
    // `s3://<adopter bucket>/<adopter prefix>`, so the position is reachable.
    // The alphabet that mints 26-character tokens is base32: an unpadded RFC
    // 4648 encoding of a 128-bit seed (a TOTP secret, a recovery seed) is
    // exactly 26 characters and is ULID-shaped 6.7e-3 of the time — 1 in 155,
    // where before the exemption it was 0 in 400,000.
    const BASE32_SECRET: &str = "2BSWY3DPEHPK3PXPJBSWY3DPEH";
    for value in [
        format!("my-archive-bucket-name/{BASE32_SECRET}"),
        format!("s3://my-archive-bucket-name/{BASE32_SECRET}"),
        serde_json::json!({"missingSegment": format!("tenant-prod/exports/{BASE32_SECRET}")})
            .to_string(),
    ] {
        let out = logweir::check::redact_path(&value);
        if out.contains(BASE32_SECRET) {
            survived.push(format!("un-anchored, on shape alone: {out}"));
        }
    }

    // And the count is never relaxed: a ULID is exempt from the budget, not a
    // licence to carry a second free component beside it.
    for beside in ["MyBackupSet01", "payments-EU"] {
        let value = format!("logweir/backups/{SET}/{beside}/{RUN}.receipt.json");
        let out = logweir::check::redact_path(&value);
        if out.contains(beside) {
            survived.push(format!("a second free component ({beside}): {out}"));
        }
    }
    assert!(
        survived.is_empty(),
        "`redact_path` widened its budget instead of exempting the ULID shape \
         ({} of {}):\n  {}",
        survived.len(),
        refused.len() * 2 + 5,
        survived.join("\n  ")
    );
}

/// `redact_path` splits exactly ONE rule out of the set, by name, and the name
/// still matches exactly one rule.
///
/// A rename in `logweir-core` would otherwise silently make the per-segment
/// clause apply nothing (every rule filtered into the "shape" half) — which
/// would be SAFE but would restore the F7 destruction — or apply everything,
/// which would restore the userinfo hole.
#[test]
fn the_long_run_rule_is_the_only_one_applied_per_segment() {
    let named: Vec<&str> = logweir_core::check_contract::redaction_rules()
        .iter()
        .filter(|r| r.name == logweir::check::LONG_RUN_RULE)
        .map(|r| r.name)
        .collect();
    assert_eq!(
        named,
        vec![logweir::check::LONG_RUN_RULE],
        "`{}` no longer names exactly one redaction rule; the rules are {:?}",
        logweir::check::LONG_RUN_RULE,
        logweir_core::check_contract::redaction_rules()
            .iter()
            .map(|r| r.name)
            .collect::<Vec<_>>()
    );
    // And the shape half really is every other rule.
    assert_eq!(
        logweir_core::check_contract::redaction_rules().len(),
        6,
        "a rule was added or removed; decide which half it belongs in"
    );
}

/// **F3 / reviewer probe R5.** A closed port is `EndpointUnreachable`, not
/// `Timeout`.
///
/// `options_for` sets `retry_timeout`, object_store renders it into the error
/// text, and the shared classifier tests its TIMEOUT tokens before its
/// UNREACHABLE ones — so every transport failure under a check classified as
/// `Timeout`, which is `unknown` rather than blocking `notReady` and carries
/// the remedy "raise the check timeout" instead of "check the URL and port".
#[test]
fn a_transport_failure_under_a_retrying_check_is_unreachable_not_a_timeout() {
    // The EXACT text object_store produced in the reviewer's live probe.
    let real = "Generic S3 error: Error performing GET https://minio.example:9000/lw-archive \
                in 1.0s, after 2 retries, max_retries: 2, retry_timeout: 5s - HTTP error: error \
                sending request for url (https://minio.example:9000/lw-archive)";
    assert_eq!(
        logweir::check::store::classify(&StoreError::Io(real.to_string())),
        CheckCode::EndpointUnreachable,
        "the retry preamble must not shadow the transport symptom"
    );
    // ...and the whole runner says so, with the blocking state and the remedy
    // that sends an operator to the port rather than to the clock.
    let m = mount(&access_plan(vec![DestinationRole::ArchiveRead], false));
    let run = drive(
        &m,
        &FakeWiring::default().with_role(
            DestinationRole::ArchiveRead,
            FakeObjects::new().failing_list(Fault::Io(real.to_string())),
        ),
    );
    let row = run.row(CheckId::DestinationArchiveListable);
    assert_eq!(row.code, CheckCode::EndpointUnreachable);
    assert_eq!(row.state, CheckState::NotReady, "a closed port blocks");
    assert!(row.remedy.contains("egress"), "{}", row.remedy);
}

/// A GENUINE timeout survives the strip. The three patterns removed are
/// object_store's retry bookkeeping and nothing else.
#[test]
fn a_genuine_timeout_survives_the_strip() {
    for text in [
        "Generic S3 error: after 2 retries, max_retries: 2, retry_timeout: 5s - operation timed \
         out",
        "Generic S3 error: request timed out after 2 retries, max_retries: 2",
        "Generic S3 error: the deadline has elapsed, max_retries: 2, retry_timeout: 30s",
    ] {
        assert_eq!(
            logweir::check::store::classify(&StoreError::Io(text.to_string())),
            CheckCode::Timeout,
            "a real timeout must survive: {text}"
        );
    }
    // A sentence that merely begins "after " is left alone, and a message with
    // no retry bookkeeping is unchanged.
    let plain = format!(
        "{} after 3 attempts",
        s3_refusal("403 Forbidden", "AccessDenied")
    );
    assert_eq!(logweir::check::store::strip_retry_noise(&plain), plain);
    assert_eq!(
        logweir::check::store::classify(&StoreError::Io(plain.clone())),
        CheckCode::AccessDenied
    );
}

/// **F5 / reviewer probe R4.** An `evidenceFetch` object may name only an
/// evidence stream.
///
/// An object claiming `result` or `details` had its bytes printed to that
/// stream and then the runner's own document printed to it again; the decoder
/// answered `DuplicatePart` and the controller reported `ResultUnreadable`
/// with no way to say why — the outcome the duplicate-stream refusal was
/// written to prevent, reached from the other side.
#[test]
fn an_evidence_object_may_not_name_a_stream_the_runner_writes() {
    for stream in [Stream::Result, Stream::Details] {
        let plan = plan_of(CheckRequest::EvidenceFetch(EvidenceFetchRequest {
            destination: destination(),
            objects: vec![EvidenceObjectRequest {
                role: DestinationRole::EvidenceRead,
                key: "logweir/backups/20260916/receipt.json".to_string(),
                max_bytes: 1024,
                stream,
            }],
        }));
        let m = mount(&plan);
        let env = good_env(&m.sha256);
        let err = load_with(&m.bytes, &as_pairs(&env), 1).expect_err(
            "an evidenceFetch object naming a stream the runner writes must be refused",
        );
        assert!(
            err.detail.contains(stream.as_str()) && err.detail.contains("runner writes itself"),
            "the refusal must name the stream and why: {}",
            err.detail
        );
    }
    // The two legal streams still pass.
    for stream in [Stream::EvidencePayload, Stream::EvidenceSidecar] {
        let plan = plan_of(CheckRequest::EvidenceFetch(EvidenceFetchRequest {
            destination: destination(),
            objects: vec![EvidenceObjectRequest {
                role: DestinationRole::EvidenceRead,
                key: "logweir/backups/20260916/receipt.json".to_string(),
                max_bytes: 1024,
                stream,
            }],
        }));
        let m = mount(&plan);
        let env = good_env(&m.sha256);
        assert!(load_with(&m.bytes, &as_pairs(&env), 1).is_ok());
    }
}

/// **Q1.** A marker put reports whether `PutMode::Create` was really enforced.
///
/// `put_create_only` falls back to HEAD-then-PUT when a backend answers
/// `NotSupported` / `NotImplemented` to a conditional put — a real,
/// non-conditional write with a TOCTOU window. An enforced put is
/// `MarkerWritten` with `createOnlyEnforced=true`; since RECEIPT-DUP the
/// fallback is `notReady / ConditionalCreateUnsupported` (the backup runner's
/// execution claim needs the enforcement).
#[test]
fn a_marker_row_says_whether_create_only_was_enforced() {
    let m = mount(&access_plan(vec![DestinationRole::EvidenceWrite], true));

    let run = drive(&m, &FakeWiring::default().with_writer(FakeObjects::new()));
    let row = run.row(CheckId::DestinationEvidenceWritable);
    assert_eq!(row.code, CheckCode::MarkerWritten);
    assert_eq!(
        row.facts.get("createOnlyEnforced").map(String::as_str),
        Some("true")
    );

    let run = drive(
        &m,
        &FakeWiring::default().with_writer(FakeObjects::new().unconditional_put()),
    );
    let row = run.row(CheckId::DestinationEvidenceWritable);
    // CHANGED BY RECEIPT-DUP (review F3). The grant is proved, but a store
    // that falls back to HEAD-then-PUT cannot hold the backup runner's
    // execution claim, so every backup to it would exit 4
    // `ExecutionClaimUnproven`. The row says so BEFORE the first backup:
    // notReady, not a written marker with a fact.
    assert_eq!(row.code, CheckCode::ConditionalCreateUnsupported);
    assert_eq!(row.state, CheckState::NotReady);
}

/// **F7.** The two readiness-marker messages name the key FAMILY rather than
/// the whole key — and, since D2-REDACT-OVERBROAD, the whole key survives the
/// redactor too.
#[test]
fn the_marker_messages_name_a_family_that_survives_redaction() {
    let family = logweir::check::store::MARKER_PREFIX;
    assert_eq!(
        logweir_core::check_contract::redact(family),
        family,
        "the key family must survive redaction or the message says nothing"
    );
    // F7's ORIGINAL finding was that the whole key did not survive: `/`, `-`
    // and a UUID's hex are all in the base64 alphabet, so the long-run rule ate
    // `logweir/readiness/<uid>` entire. That is the same rule that blanked the
    // missing segment's path and the `signerKeyId` in the D2 live run, and it
    // now exempts a run whose `/` components are public identifiers. The
    // message still names the family — an operator wants the family, not one
    // probe's UID — but redaction is no longer the reason.
    let whole = logweir::check::store::absent_probe_key(DEST_UID);
    assert_eq!(
        logweir_core::check_contract::redact(&whole),
        whole,
        "an object key built from a UUID is a public identifier"
    );

    let m = mount(&access_plan(
        vec![
            DestinationRole::EvidenceRead,
            DestinationRole::EvidenceWrite,
        ],
        true,
    ));
    let run = drive(
        &m,
        &FakeWiring::default()
            .with_role(
                DestinationRole::EvidenceRead,
                FakeObjects::new()
                    .failing_get(Fault::Io(s3_refusal("403 Forbidden", "AccessDenied"))),
            )
            .with_writer(FakeObjects::new()),
    );
    for id in [
        CheckId::DestinationEvidenceReadable,
        CheckId::DestinationEvidenceWritable,
    ] {
        let row = run.row(id);
        assert!(
            row.message.contains(family) && !row.message.contains("[redacted]"),
            "`{id}`'s message lost its diagnostic to redaction: {}",
            row.message
        );
    }
}

// ===========================================================================
// 15. Re-verification fixes (2026-09-17)
// ===========================================================================

/// **R-F2.** The transport comes from `plan.tls` and from NOTHING else
/// (D-SEAMS S5: transport security is never derived).
///
/// The shipped line was already right, and nothing asserted it: the reviewer's
/// mutant **F1b** re-derived it as `tls: plan.ca_file.is_some()` and survived
/// the whole suite, because the default fixture is `tls: Some(false)` with no
/// `ca_file`, so the `true` branch was never exercised. Under that mutant a
/// connection with `tls: true` and a publicly-trusted CA — no projected bundle,
/// which is the ordinary case for a managed broker — dials `SASL_PLAINTEXT`,
/// and `with_tls_ca_file` cannot notice because the mutant keeps the CA and the
/// transport consistent with each other.
///
/// So the assertion is over all four `(tls, ca_file)` combinations: the answer
/// must track `tls` alone, in both directions, with and without a CA.
#[test]
fn the_transport_comes_from_plan_tls_and_from_nothing_else() {
    for tls in [None, Some(false), Some(true)] {
        for ca in [None, Some("/check/source-ca.pem".to_string())] {
            let plan = ConnectionPlan {
                auth_mode: "scramSha512".to_string(),
                username: Some("backup".to_string()),
                tls,
                ca_file: ca.clone(),
                ..connection()
            };
            let spec = logweir::check::kafka::auth_spec(&plan).expect("scramSha512 maps");
            let want = tls == Some(true);
            match spec {
                logweir_core::spec::AuthSpec::ScramSha512 { tls: got, .. } => assert_eq!(
                    got, want,
                    "tls={tls:?} ca_file={ca:?} mapped to tls: {got}; the transport is the \
                     plan's `tls` and is NEVER derived from whether a trust anchor was \
                     projected (D-SEAMS S5)"
                ),
                other => panic!("scramSha512 must map to ScramSha512, got {other:?}"),
            }
        }
    }
    // ...and the two halves really are independent: a CA on a connection that
    // is not TLS is refused rather than upgrading the transport, which is the
    // same rule from the other side.
    let clear_with_ca = ConnectionPlan {
        auth_mode: "scramSha512".to_string(),
        username: Some("backup".to_string()),
        tls: Some(false),
        ca_file: Some("/check/source-ca.pem".to_string()),
        ..connection()
    };
    let err = logweir::check::kafka::auth_config(&clear_with_ca)
        .expect_err("a CA on a clear connection is refused, never an upgrade");
    assert_eq!(err.code, CheckCode::AuthenticationFailed);
}

/// **R-F3 / reviewer probe R7.** A details line stays parseable JSON however
/// long the key is.
///
/// `redact_path` used to end with `redact`'s 512-CHARACTER cap, and
/// `details_stream` applies it per LINE — so a line carrying a long S3 key with
/// no 40-character run (nothing to redact) was cut mid-string and the stream,
/// which D2 §6.7 documents as JSON lines, emitted an unparseable one. The cap
/// now lives where each bound belongs: on the message (`with_message`), on the
/// sample VALUE (`readiness::detail`, re-serialised by serde_json), and on the
/// stream in BYTES (`details_stream`, which drops whole lines).
#[test]
fn a_long_key_leaves_the_details_stream_parseable() {
    // R7's shape: 120 short `/`-separated segments, 888 characters, and no run
    // anywhere near 40 — so redaction has nothing to do and only a cap could
    // damage it.
    let long_key: String = (0..120)
        .map(|i| format!("seg{i:03}"))
        .collect::<Vec<_>>()
        .join("/");
    assert!(long_key.len() > 512, "the probe needs a line over the cap");
    assert_eq!(
        logweir::check::redact_path(&long_key),
        long_key,
        "nothing in this key is credential-shaped, so nothing may change"
    );

    let line =
        serde_json::json!({"check": "archive.segments", "missingSegment": long_key}).to_string();
    let stream = kinds::restore::details_stream(&[line]);
    for l in String::from_utf8(stream).unwrap().lines() {
        let parsed: serde_json::Value = serde_json::from_str(l)
            .unwrap_or_else(|e| panic!("a details line must be JSON ({e}): {l}"));
        assert_eq!(parsed["missingSegment"], long_key);
    }

    // The bounded `detail` sample is capped as a VALUE, so its JSON survives
    // too — and it says it was cut rather than handing a reader a prefix.
    let d = kinds::readiness::detail(std::slice::from_ref(&long_key));
    let sample = d["sample"][0].as_str().unwrap();
    assert!(
        sample.chars().count() <= kinds::readiness::DETAIL_VALUE_MAX_CHARS,
        "a sample is {} characters",
        sample.chars().count()
    );
    assert!(sample.ends_with('…'), "a cut sample says so: {sample}");
    let round_trip: serde_json::Value = serde_json::from_str(&d.to_string()).unwrap();
    assert_eq!(round_trip["count"], 1);

    // A Kafka topic name is at most 249 characters, so the VALUE cap never
    // cuts one. (Redaction may still fire on a name that is itself one long
    // run of the base64 alphabet — that is F7's documented trade-off for a
    // value with no `/` to split on, and it is a redaction, not a cut.)
    let longest_topic: String = std::iter::repeat("orders.eu-west-1.")
        .flat_map(|s| s.chars())
        .take(249)
        .collect();
    assert_eq!(longest_topic.chars().count(), 249);
    let sample = kinds::readiness::detail(std::slice::from_ref(&longest_topic));
    assert_eq!(sample["sample"][0], longest_topic);
    assert!(!sample["sample"][0].as_str().unwrap().ends_with('…'));
}

/// **D2 W8 seam.** A connection's `caFile` is an OPAQUE IN-POD PATH: the
/// runner hands it to the client and never opens it.
///
/// W8's `TopicDiscovery` reconciler does not render `source-ca.pem` into the
/// plan `ConfigMap` — a `KafkaCluster` may name its CA in a Secret and that
/// controller holds no verb on `secrets` — so it projects the same mount an
/// execution Job gets and names `/connection/source-ca/ca.crt` in the plan.
/// A runner that assumed `/check/source-ca.pem`, or that read the file to
/// inline it, would refuse every discovery against a private-CA broker.
///
/// The DESTINATION side is deliberately not this: `DestinationPlan::ca_file`
/// is read, because `StoreOptions::with_root_certificate` takes PEM bytes.
#[test]
fn a_connection_ca_is_an_opaque_in_pod_path() {
    // A uniquely-named projected variable, so the row does not race another
    // test over the process environment.
    const PASSWORD_VAR: &str = "LOGWEIR_CHECK_TEST_CA_ROW_PASSWORD";
    std::env::set_var(PASSWORD_VAR, "not-a-real-password");
    for path in [
        // W8's projected mount…
        "/connection/source-ca/ca.crt",
        // …and D2 §4.3's plan-ConfigMap key, which other kinds may still use.
        "/check/source-ca.pem",
        // A path that does not exist at all: the runner must not stat it.
        "/connection/nothing-is-here/ca.crt",
    ] {
        assert!(
            !Path::new(path).exists(),
            "the fixture path {path} must not exist, or this row proves nothing"
        );
        let plan = ConnectionPlan {
            auth_mode: "scramSha512".to_string(),
            username: Some("backup".to_string()),
            tls: Some(true),
            ca_file: Some(path.to_string()),
            password_env: Some(PASSWORD_VAR.to_string()),
            ..connection()
        };
        // The mapping does not touch the file...
        let spec = logweir::check::kafka::auth_spec(&plan).expect("scramSha512 maps");
        match spec {
            logweir_core::spec::AuthSpec::ScramSha512 { tls, .. } => {
                assert!(tls, "the transport is still the plan's `tls`");
            }
            other => panic!("unexpected {other:?}"),
        }
        // ...and neither does building the client's auth: the path reaches
        // `ssl.ca.location` unchanged, whether or not anything is there.
        let auth = logweir::check::kafka::auth_config(&plan)
            .expect("a projected CA path is carried, never opened");
        match auth {
            logweir_kafka::reader::AuthConfig::ScramSha512 { tls_ca_file, .. } => assert_eq!(
                tls_ca_file.as_deref(),
                Some(path),
                "the plan's `caFile` must reach the client verbatim"
            ),
            other => panic!("unexpected {other:?}"),
        }
    }

    // And it still cannot decide the transport: a CA on a clear connection is
    // refused rather than upgrading it (D-SEAMS S5).
    let clear = ConnectionPlan {
        auth_mode: "scramSha512".to_string(),
        username: Some("backup".to_string()),
        tls: Some(false),
        ca_file: Some("/connection/source-ca/ca.crt".to_string()),
        password_env: Some(PASSWORD_VAR.to_string()),
        ..connection()
    };
    assert_eq!(
        logweir::check::kafka::auth_config(&clear)
            .expect_err("a CA on a clear connection is refused")
            .code,
        CheckCode::AuthenticationFailed
    );
}

// ===========================================================================
// 12. `catalogSync` — D3 §5.3's bounded walk, and the body §7d specifies
// ===========================================================================
//
// THE CONTROLLER'S PARSER IS THE ORACLE HERE, and it lives in a crate this one
// does not link. So the pinning runs in both directions:
//
//   * `the_grammar_this_runner_writes_is_the_grammar_the_controller_parses`
//     reads `crates/weirkeeper/src/catalog_view.rs` and asserts every prefix
//     and every cap this runner writes equals the controller's constant;
//   * [`PINNED_SYNC_BODY`] is the EXACT body the emitter produces for a fixed
//     fixture, and `crates/weirkeeper/tests/catalog_controller.rs`'s
//     `the_runners_pinned_body_is_one_this_parser_reads` extracts it from THIS
//     file and runs `view::parse_body` over it.
//
// Either side moving breaks a test, and neither crate gained a dependency
// edge — which is the pattern D2 W9 used for the expected-row set, for the
// same reason.

/// The signer key id the pinned fixture's sidecar CLAIMS.
///
/// A claim and nothing more: the pinned body is produced with NO trust bundle,
/// so the runner reports `notAttempted` and echoes the id the sidecar names.
/// That is what puts a stranger's key into `catalog-signers` — and therefore
/// into `status.counts.untrustedSigner` — without any entry claiming a verdict
/// nobody could reach.
const CATALOG_CLAIMED_KEY_ID: &str =
    "0f1e2d3c4b5a69788796a5b4c3d2e1f00f1e2d3c4b5a69788796a5b4c3d2e1f0";

/// The manifest body every catalog fixture writes.
const CATALOG_MANIFEST: &[u8] = br#"{"topics":[]}"#;

fn catalog_ts(s: &str) -> DateTime<Utc> {
    s.parse().expect("an RFC 3339 instant")
}

/// A receipt as `logweir backup run` writes one, with a manifest digest that
/// matches [`CATALOG_MANIFEST`].
fn catalog_receipt(backup_id: &str, run_id: &str, started: &str) -> BackupReceipt {
    let started_at = catalog_ts(started);
    BackupReceipt {
        format_version: "1.0.0".to_string(),
        run_id: run_id.to_string(),
        backup_id: backup_id.to_string(),
        requested_at: started_at - chrono::Duration::minutes(1),
        started_at,
        finished_at: started_at + chrono::Duration::minutes(4),
        exit_code: 0,
        triggered_by: "schedule".to_string(),
        source: logweir_core::backup_receipt::ReceiptSource {
            cluster_id: "SOURCE-CLUSTER-000001".to_string(),
            bootstrap_servers: vec!["kafka-source:9092".to_string()],
            auth: logweir_core::backup_receipt::ReceiptAuth {
                mode: "scramSha512".to_string(),
                username: Some("logweir".to_string()),
            },
            topics: vec!["orders".to_string()],
        },
        engine: logweir_core::backup_receipt::ReceiptEngine {
            id: "oso".to_string(),
            version: "0.21.0".to_string(),
            digest: format!("sha256:{}", "d".repeat(64)),
        },
        archive: logweir_core::backup_receipt::ReceiptArchive {
            manifest_key: format!("kafka-backups/{backup_id}/manifest.json"),
            manifest_sha256: logweir_core::ids::sha256_prefixed(CATALOG_MANIFEST),
            manifest_version_id: None,
            prefix: "kafka-backups".to_string(),
        },
        records: BTreeMap::from([("orders".to_string(), 1234u64)]),
        covered: logweir_core::backup_receipt::ReceiptCovered {
            from_ms: started_at.timestamp_millis() - 3_600_000,
            to_ms: started_at.timestamp_millis(),
        },
        config_coverage: None,
        topic_configuration: None,
        owner_detection: None,
        consumer_positions: None,
        schema_dependency: None,
        generations: None,
    }
}

/// One point's four objects, ready to place in a [`FakeObjects`].
struct CatalogFixture {
    point: logweir::catalog::CatalogPoint,
    record_key: String,
    record_bytes: Vec<u8>,
    log_key: String,
    log_bytes: Vec<u8>,
    receipt_key: String,
    receipt_bytes: Vec<u8>,
    sidecar_key: String,
    sidecar_bytes: Vec<u8>,
    manifest_key: String,
}

/// Build one point from a receipt, its location and the sidecar to publish
/// beside it.
fn catalog_fixture(
    receipt: &BackupReceipt,
    location_id: &str,
    sidecar: &logweir_evidence::Sidecar,
    installation_key_id: &str,
) -> CatalogFixture {
    let receipt_bytes =
        logweir_core::det_json::to_deterministic_json(receipt).expect("the receipt serialises");
    let keys = logweir::backup::phase_run::receipt_keys(&receipt.backup_id, &receipt.run_id);
    let inputs = logweir::catalog::writer::RecordInputs {
        receipt_key: keys.receipt_key.clone(),
        sidecar_key: keys.sidecar_key.clone(),
        location_id: location_id.to_string(),
        recorded_at: catalog_ts("2026-09-16T06:00:00Z"),
        signing: logweir::catalog::RecordSigning {
            key_id: installation_key_id.to_string(),
            algorithm: "ecdsa-p256-sha256".to_string(),
        },
        installation: Some(logweir::catalog::RecordInstallation {
            key_id: installation_key_id.to_string(),
        }),
        execution: None,
    };
    let point = logweir::catalog::writer::from_receipt(receipt, &receipt_bytes, &inputs)
        .expect("the record derives from the receipt");
    let log_entry = logweir::catalog::CatalogLogEntry::of(&point);
    CatalogFixture {
        record_key: logweir::catalog::record::record_key(&point.point_id),
        record_bytes: point.canonical_bytes().expect("the record serialises"),
        log_key: point.log_key(),
        log_bytes: log_entry
            .canonical_bytes()
            .expect("the index entry serialises"),
        receipt_key: keys.receipt_key,
        receipt_bytes,
        sidecar_key: keys.sidecar_key,
        sidecar_bytes: serde_json::to_vec(sidecar).expect("the sidecar serialises"),
        manifest_key: point.archive.manifest_key.clone(),
        point,
    }
}

/// A sidecar that names `key_id` and carries a signature nothing can verify.
///
/// Used only where the fixture has NO trust material: with no key to try, the
/// runner reports `notAttempted` and the bytes are never examined, so a real
/// signature would prove nothing the deterministic fixture needs.
fn claimed_sidecar(key_id: &str) -> logweir_evidence::Sidecar {
    logweir_evidence::Sidecar {
        payload_type: logweir_evidence::PAYLOAD_TYPE_BACKUP_RECEIPT.to_string(),
        signatures: vec![logweir_evidence::Signature {
            keyid: key_id.to_string(),
            sig: "AA==".to_string(),
        }],
    }
}

/// Place one fixture's four objects.
fn place(objects: FakeObjects, f: &CatalogFixture) -> FakeObjects {
    objects
        .with_object(&f.log_key, &f.log_bytes)
        .with_object(&f.record_key, &f.record_bytes)
        .with_object(&f.receipt_key, &f.receipt_bytes)
        .with_object(&f.sidecar_key, &f.sidecar_bytes)
        .with_object(&f.manifest_key, CATALOG_MANIFEST)
}

fn catalog_plan(request: logweir_core::check_contract::CatalogSyncRequest) -> CheckPlan {
    CheckPlan {
        timeout_seconds: 900,
        ..plan_of(CheckRequest::CatalogSync(Box::new(request)))
    }
}

fn sync_request() -> logweir_core::check_contract::CatalogSyncRequest {
    logweir_core::check_contract::CatalogSyncRequest {
        destination: destination(),
        mode: logweir_core::check_contract::CatalogSyncMode::Index,
        deep_check: logweir_core::check_contract::CatalogDeepCheck::ManifestDigest,
        max_objects_per_run: 100_000,
        view_limit: 2000,
        index_shard: None,
        rescan_start_after: None,
        trust_bundle_file: None,
    }
}

/// The `details` body a run relayed, as text.
fn body_of(run: &Run) -> String {
    let relay = run.relay.as_ref().expect("the relay decodes");
    String::from_utf8(
        relay
            .stream(Stream::Details)
            .expect("a catalog sync relays a details body")
            .to_vec(),
    )
    .expect("the body is UTF-8")
}

/// Every `catalog-entry=` line's parsed JSON, in body order.
fn entries_of(body: &str) -> Vec<serde_json::Value> {
    body.lines()
        .filter_map(|l| l.strip_prefix("catalog-entry="))
        .map(|l| serde_json::from_str(l).expect("an entry line is JSON"))
        .collect()
}

fn summary_of(body: &str, prefix: &str) -> serde_json::Value {
    let mut found = body.lines().filter_map(|l| l.strip_prefix(prefix));
    let first = found.next().unwrap_or_else(|| panic!("no `{prefix}` line"));
    assert!(
        found.next().is_none(),
        "`{prefix}` was written twice; a summary that could be overwritten is a summary nobody \
         can attribute"
    );
    serde_json::from_str(first).expect("a summary line is JSON")
}

/// The two-point fixture every row below starts from: one point on the day the
/// clock says, one the day before.
fn two_point_objects() -> (FakeObjects, CatalogFixture, CatalogFixture) {
    let sidecar = claimed_sidecar(CATALOG_CLAIMED_KEY_ID);
    let newer = catalog_fixture(
        &catalog_receipt("set-a", "run-a", "2026-09-16T03:00:00Z"),
        "s3://lw-archive/kafka-backups",
        &sidecar,
        CATALOG_CLAIMED_KEY_ID,
    );
    let older = catalog_fixture(
        &catalog_receipt("set-b", "run-b", "2026-09-15T03:00:00Z"),
        "s3://lw-archive/kafka-backups",
        &sidecar,
        CATALOG_CLAIMED_KEY_ID,
    );
    let objects = place(place(FakeObjects::new(), &newer), &older);
    (objects, newer, older)
}

fn drive_sync(
    request: logweir_core::check_contract::CatalogSyncRequest,
    wiring: &FakeWiring,
) -> Run {
    drive(&mount(&catalog_plan(request)), wiring)
}

// ---------------------------------------------------------------------------
// FX-7: points pinned to a manifest VERSION
// ---------------------------------------------------------------------------

/// A receipt taken on a VERSIONED bucket: the pin's format, pinning `version`.
fn pinned_catalog_receipt(version: &str) -> BackupReceipt {
    let mut r = catalog_receipt("set-a", "run-a", "2026-09-16T03:00:00Z");
    r.format_version =
        logweir_core::backup_receipt::FORMAT_VERSION_WITH_MANIFEST_VERSION.to_string();
    r.archive.manifest_version_id = Some(version.to_string());
    r
}

/// One pinned point whose manifest has `history` (oldest first).
fn versioned_objects(
    receipt: &BackupReceipt,
    history: &[(&str, &[u8])],
) -> (FakeObjects, CatalogFixture) {
    let f = catalog_fixture(
        receipt,
        "s3://lw-archive/kafka-backups",
        &claimed_sidecar(CATALOG_CLAIMED_KEY_ID),
        CATALOG_CLAIMED_KEY_ID,
    );
    let objects = place(FakeObjects::new(), &f).with_versions(&f.manifest_key, history);
    (objects, f)
}

fn only_entry(objects: FakeObjects) -> serde_json::Value {
    let run = drive_sync(
        sync_request(),
        &FakeWiring::default().with_role(DestinationRole::ArchiveRead, objects),
    );
    let entries = entries_of(&body_of(&run));
    assert_eq!(entries.len(), 1, "{entries:?}");
    entries[0].clone()
}

// ---------------------------------------------------------------------------
// PROD-05.1: the view lists a point's topics with their recorded layout
// ---------------------------------------------------------------------------

/// A 1.3.0 receipt over `topics`, each captured with the given replication
/// factor and partition count, `orders` owned by a Strimzi `KafkaTopic`.
/// A 1.3.0 receipt over `(topic, replication factor, partitions)`; a `0`
/// count is NOT RECORDED (arm 19 forbids a recorded zero), the shape engine
/// 0.23.3's manifest leaves for every topic after the first (FX-21). `orders`
/// is owned by a Strimzi `KafkaTopic`, so the run read the resources.
fn modelled_catalog_receipt(topics: &[(&str, u32, u32)]) -> BackupReceipt {
    use logweir_core::backup_receipt::{TopicConfigCoverage, TopicConfiguration, TopicOwner};
    let mut r = catalog_receipt("set-a", "run-a", "2026-09-16T03:00:00Z");
    r.format_version =
        logweir_core::backup_receipt::FORMAT_VERSION_WITH_TOPIC_CONFIGURATION.to_string();
    r.source.topics = topics.iter().map(|(t, ..)| (*t).to_string()).collect();
    r.records = topics
        .iter()
        .map(|(t, ..)| ((*t).to_string(), 1u64))
        .collect();
    r.config_coverage = Some(
        topics
            .iter()
            .map(|(t, ..)| {
                (
                    (*t).to_string(),
                    TopicConfigCoverage {
                        coverage: "captured".into(),
                        reason: None,
                        timestamp_type: None,
                    },
                )
            })
            .collect(),
    );
    r.topic_configuration = Some(
        topics
            .iter()
            .map(|(t, rf, partitions)| {
                (
                    (*t).to_string(),
                    TopicConfiguration {
                        partitions: (*partitions > 0).then_some(*partitions),
                        replication_factor: (*rf > 0).then_some(*rf),
                        entries: Some(BTreeMap::new()),
                        owner: (*t == "orders").then(|| TopicOwner {
                            kind: "strimzi".into(),
                            basis: "kafkaTopicResource".into(),
                            reference: "kafka/orders".into(),
                        }),
                    },
                )
            })
            .collect(),
    );
    r.owner_detection = Some(vec!["kafkaTopicResources".into()]);
    assert_eq!(r.validate_invariants(), Ok(()));
    r
}

/// **M3 (fix round): an absent count stays absent.** A topic whose replication
/// factor was not recorded — engine 0.23.3's manifest keeps it for the first
/// topic only (FX-21) — and one whose partition count was not, are listed
/// WITHOUT the key: never `1`, never `0`, never another topic's.
#[test]
fn a_topic_whose_factor_or_count_was_not_recorded_is_listed_without_it() {
    let receipt = modelled_catalog_receipt(&[("orders", 3, 6), ("ledger", 0, 4), ("audit", 2, 0)]);
    let (objects, _) = versioned_objects(&receipt, &[("v1", CATALOG_MANIFEST)]);
    let entry = only_entry(objects);
    assert_eq!(entry["availability"], "Available", "{entry}");
    assert_eq!(
        entry["topics"],
        serde_json::json!([
            {"name": "audit", "replicationFactor": 2, "configCoverage": "captured"},
            {"name": "ledger", "partitions": 4, "configCoverage": "captured"},
            {"name": "orders", "partitions": 6, "replicationFactor": 3, "configCoverage": "captured",
             "owner": "strimzi"},
        ]),
        "{entry}"
    );
    // Where the run looked for owners travels with the topics (M2).
    assert_eq!(
        entry["ownerDetection"],
        serde_json::json!(["kafkaTopicResources"]),
        "{entry}"
    );
}

/// **M2 (fix round).** A run that looked for owners nowhere publishes an EMPTY
/// `ownerDetection` beside its topics — not an absent one, which is NOT
/// PUBLISHED — so a reader can say "owner not checked" for every topic.
#[test]
fn a_point_whose_owners_were_never_looked_for_publishes_an_empty_detection() {
    let mut receipt = modelled_catalog_receipt(&[("audit", 1, 1)]);
    receipt.owner_detection = Some(Vec::new());
    assert_eq!(receipt.validate_invariants(), Ok(()));
    let (objects, _) = versioned_objects(&receipt, &[("v1", CATALOG_MANIFEST)]);
    let entry = only_entry(objects);
    assert_eq!(entry["ownerDetection"], serde_json::json!([]), "{entry}");
    assert!(entry["topics"][0].get("owner").is_none(), "{entry}");
}

/// **The console's source.** An `Available` 1.3.0 point lists every topic with
/// its recorded partition count, replication factor, coverage and owner kind;
/// the control — a 1.0.0 point — lists none (NOT PUBLISHED).
#[test]
fn an_available_1_3_0_point_lists_its_topics_with_their_recorded_layout() {
    let receipt = modelled_catalog_receipt(&[("orders", 3, 6), ("audit", 1, 1)]);
    let (objects, _) = versioned_objects(&receipt, &[("v1", CATALOG_MANIFEST)]);
    let entry = only_entry(objects);
    assert_eq!(entry["availability"], "Available", "{entry}");
    assert_eq!(
        entry["topics"],
        serde_json::json!([
            {"name": "audit", "partitions": 1, "replicationFactor": 1, "configCoverage": "captured"},
            {"name": "orders", "partitions": 6, "replicationFactor": 3, "configCoverage": "captured",
             "owner": "strimzi"},
        ]),
        "{entry}"
    );
    assert!(entry.get("topicsOmitted").is_none(), "{entry}");
    let (control, _) = versioned_objects(
        &catalog_receipt("set-a", "run-a", "2026-09-16T03:00:00Z"),
        &[("v1", CATALOG_MANIFEST)],
    );
    let control = only_entry(control);
    assert!(
        control.get("topics").is_none(),
        "a pre-1.3.0 point lists no topics: {control}"
    );
}

/// A point the sync cannot stand behind lists no topics: a `Conflict` (here a
/// superseded pin) is a record whose facts are not the point's.
#[test]
fn a_point_that_is_not_available_lists_no_topics() {
    let mut receipt = modelled_catalog_receipt(&[("orders", 3, 6)]);
    receipt.archive.manifest_version_id = Some("v1".into());
    let history: &[(&str, &[u8])] = &[("v1", CATALOG_MANIFEST), ("v2", CATALOG_MANIFEST)];
    let (objects, _) = versioned_objects(&receipt, history);
    let entry = only_entry(objects);
    assert_eq!(entry["availability"], "Conflict", "{entry}");
    assert!(entry.get("topics").is_none(), "{entry}");
}

/// More topics than the cap: none listed, the count said — a partial list would
/// read as the point's topic set.
#[test]
fn a_point_with_more_topics_than_the_cap_lists_none_and_counts_them() {
    use logweir::check::kinds::catalog_sync::MAX_ENTRY_TOPICS;
    let names: Vec<String> = (0..=MAX_ENTRY_TOPICS).map(|i| format!("t{i:03}")).collect();
    let topics: Vec<(&str, u32, u32)> = names.iter().map(|n| (n.as_str(), 1, 1)).collect();
    let (objects, _) = versioned_objects(
        &modelled_catalog_receipt(&topics),
        &[("v1", CATALOG_MANIFEST)],
    );
    let entry = only_entry(objects);
    assert!(entry.get("topics").is_none(), "{entry}");
    assert_eq!(entry["topicsOmitted"], MAX_ENTRY_TOPICS + 1, "{entry}");
    // Nothing for `ownerDetection` to qualify, so it is not published either.
    assert!(entry.get("ownerDetection").is_none(), "{entry}");
}

/// **PROD-04.1.** A 1.7.0 receipt over `orders` (one partition) selecting
/// `ids`: the first captured at a position the archive holds, every other
/// excluded GroupNotFound — built by the RUNNER's own builder, so the block is
/// the one `backup run` signs.
fn positioned_catalog_receipt(ids: &[String]) -> BackupReceipt {
    let mut r = modelled_catalog_receipt(&[("orders", 3, 1)]);
    let built = built_positions(&r, ids, &[("orders", 1)], |i| i == 0);
    r.format_version =
        logweir_core::backup_receipt::FORMAT_VERSION_WITH_CONSUMER_POSITIONS.to_string();
    r.consumer_positions = Some(built.block);
    assert_eq!(r.validate_invariants(), Ok(()));
    r
}

/// A capturable classic group, from a real classification (only `classify`
/// makes one).
fn capturable_group(id: &str) -> logweir_kafka::groups::CapturableGroup {
    use logweir_kafka::groups::{code, GroupListings, GroupVerdict, NameEntry, TypedEntry};
    let listings = GroupListings {
        typed: vec![TypedEntry {
            group_id: id.into(),
            is_simple: false,
            state: code::STATE_STABLE,
            group_type: code::TYPE_CLASSIC,
        }],
        typed_errors: vec![],
        names: vec![NameEntry {
            group_id: id.into(),
            error: 0,
        }],
        names_incomplete: None,
        access: logweir_kafka::access::ClusterAccess::Reported(vec![8]),
        unreadable_ids: 0,
    };
    match listings
        .classify(&[id.to_string()], &BTreeMap::new())
        .remove(0)
        .1
    {
        GroupVerdict::Capture(c) => c,
        other => panic!("{other:?}"),
    }
}

/// **`backup run`'s own builder** over `ids`, the `(topic, partitions)` named,
/// every partition's marks `[0, 10]` read before and after the engine and
/// `[0, 9]` archived. A group `captured(i)` selects is a live classic group
/// committed at 5 on EVERY partition of every topic — the review's worst
/// case, the one that grows the positions — and every other id is absent.
fn built_positions(
    receipt: &BackupReceipt,
    ids: &[String],
    topics: &[(&str, i32)],
    captured: impl Fn(usize) -> bool,
) -> logweir::backup::consumer_positions::Built {
    use logweir_kafka::capture::{GroupsObservation, Marks, ObservedGroup, TopicMarks};
    use logweir_kafka::groups::{
        Absence, Excluded, GroupDescription, GroupState, GroupType, GroupVerdict,
        ListingCompleteness, MemberDescription,
    };
    use logweir_kafka::positions::{
        CommittedPosition, GroupPositions, PartitionPosition, TopicPartition,
    };
    let marks = |n: i32| -> TopicMarks {
        Ok((0..n)
            .map(|p| {
                (
                    p,
                    Ok(Marks {
                        log_start: 0,
                        high_watermark: 10,
                    }),
                )
            })
            .collect())
    };
    let every: Vec<TopicPartition> = topics
        .iter()
        .flat_map(|(t, n)| (0..*n).map(move |p| TopicPartition::new(*t, p)))
        .collect();
    let groups = ids
        .iter()
        .enumerate()
        .map(|(i, id)| {
            if captured(i) {
                ObservedGroup {
                    group_id: id.clone(),
                    verdict: GroupVerdict::Capture(capturable_group(id)),
                    description: Some(Ok(GroupDescription {
                        group_id: id.clone(),
                        group_type: GroupType::Classic,
                        state: GroupState::Stable,
                        is_simple: false,
                        partition_assignor: None,
                        coordinator: Some(1),
                        members: vec![MemberDescription {
                            client_id: None,
                            consumer_id: None,
                            group_instance_id: None,
                            host: None,
                            assignment: vec![],
                            target_assignment: None,
                        }],
                    })),
                    positions: Some(Ok(GroupPositions {
                        group: id.clone(),
                        partitions: every
                            .iter()
                            .map(|tp| {
                                (
                                    tp.clone(),
                                    PartitionPosition::Committed(CommittedPosition {
                                        offset: 5,
                                        leader_epoch: None,
                                        metadata: Some(String::new()),
                                    }),
                                )
                            })
                            .collect(),
                    })),
                }
            } else {
                ObservedGroup {
                    group_id: id.clone(),
                    verdict: GroupVerdict::Excluded(Excluded::GroupNotFound {
                        evidence: Absence::CompleteListing,
                    }),
                    description: None,
                    positions: None,
                }
            }
        })
        .collect();
    let observation = GroupsObservation {
        completeness: Some(ListingCompleteness::Complete),
        groups,
        topics: topics
            .iter()
            .map(|(t, n)| ((*t).to_string(), marks(*n)))
            .collect(),
        unavailable: None,
    };
    let after: BTreeMap<String, TopicMarks> = topics
        .iter()
        .map(|(t, n)| ((*t).to_string(), marks(*n)))
        .collect();
    let archived: logweir::backup::consumer_positions::ArchivedRanges = topics
        .iter()
        .map(|(t, n)| ((*t).to_string(), (0..*n).map(|p| (p, (0, 9))).collect()))
        .collect();
    let names: Vec<String> = topics.iter().map(|(t, _)| (*t).to_string()).collect();
    let started = catalog_ts("2026-09-16T02:59:58Z");
    logweir::backup::consumer_positions::build(&logweir::backup::consumer_positions::Capture {
        backup_id: &receipt.backup_id,
        run_id: &receipt.run_id,
        selected: ids,
        topics: &names,
        observed_from: started,
        observed_to: started + chrono::Duration::seconds(1),
        observation: &observation,
        after: &after,
        archived: &archived,
    })
    .expect("the builder builds")
}

/// **PROD-04.1 review H1: the receipt stays small however many partitions the
/// selected groups hold, and the point stays `Available`.** The review's two
/// sizes, through the runner's own builder with every group committed on every
/// partition: 10 groups over 20 topics of 12 partitions, and 100 groups — the
/// most a backup may select — over 10 topics of 11. Each receipt is far under
/// the 256 KiB the catalog read a document under when PROD-04.1 landed (and
/// the evidence fetch's cap), the catalog reads the point `Available` with its
/// summary, and the receipt's block alone is under its enforced cap. NEGATIVE
/// CONTROL: the positions themselves — the document the receipt binds — are
/// over that 256 KiB at both sizes, which is what the receipt carried inline
/// before the fix.
///
/// FX-33 replaced that cap with the topic budget's (`caps::CATALOG_RECEIPT`),
/// so the number this row holds the receipt under is written down here: the
/// row's claim is that positions do not grow the receipt, whatever a reader's
/// cap is.
#[test]
fn the_reviews_two_sizes_keep_the_receipt_small_and_the_point_available() {
    use logweir::check::kinds::catalog_sync::MAX_ENTRY_GROUPS;
    use logweir_core::consumer_positions::{MAX_BLOCK_BYTES, MAX_SELECTED_GROUPS};
    /// The catalog walk's document cap when this row was written.
    const MAX_CATALOG_DOCUMENT_BYTES: usize = 256 * 1024;
    for (groups, topics, partitions) in [(10usize, 20usize, 12i32), (MAX_SELECTED_GROUPS, 10, 11)] {
        let names: Vec<String> = (0..topics).map(|t| format!("topic-{t:02}")).collect();
        let layout: Vec<(&str, u32, u32)> = names
            .iter()
            .map(|n| (n.as_str(), 3, partitions as u32))
            .collect();
        let mut r = modelled_catalog_receipt(&layout);
        let ids: Vec<String> = (0..groups)
            .map(|i| format!("consumer-group-{i:03}"))
            .collect();
        let named: Vec<(&str, i32)> = names.iter().map(|n| (n.as_str(), partitions)).collect();
        let built = built_positions(&r, &ids, &named, |_| true);
        r.format_version =
            logweir_core::backup_receipt::FORMAT_VERSION_WITH_CONSUMER_POSITIONS.to_string();
        r.consumer_positions = Some(built.block.clone());
        assert_eq!(r.validate_invariants(), Ok(()));
        assert_eq!(
            r.validate_consumer_positions_document(&built.document_bytes, &built.document),
            Ok(())
        );
        let receipt_bytes = logweir_core::det_json::to_deterministic_json(&r).unwrap();
        let block_bytes = logweir_core::det_json::to_deterministic_json(&built.block).unwrap();
        let size = format!(
            "{groups} groups over {topics}x{partitions}: receipt {} bytes, block {} bytes, \
             document {} bytes",
            receipt_bytes.len(),
            block_bytes.len(),
            built.document_bytes.len()
        );
        eprintln!("[h1] {size}");
        assert!(
            receipt_bytes.len() * 4 < MAX_CATALOG_DOCUMENT_BYTES,
            "the receipt is not well under the catalog's cap: {size}"
        );
        assert!(
            (receipt_bytes.len() as u64) < logweir_core::check_contract::MAX_EVIDENCE_PAYLOAD_BYTES,
            "{size}"
        );
        assert!(block_bytes.len() < MAX_BLOCK_BYTES, "{size}");
        assert!(
            built.document_bytes.len() > MAX_CATALOG_DOCUMENT_BYTES,
            "NEGATIVE CONTROL: inline, these positions would be over the cap: {size}"
        );
        // Every partition of every group is accounted for in the document.
        for g in built.document.groups.values() {
            assert_eq!(g.positions.len(), topics * partitions as usize);
        }
        let (objects, _) = versioned_objects(&r, &[("v1", CATALOG_MANIFEST)]);
        let entry = only_entry(objects);
        assert_eq!(entry["availability"], "Available", "{size}: {entry}");
        let cp = &entry["consumerPositions"];
        assert_eq!(cp["listing"], "complete", "{entry}");
        if groups > MAX_ENTRY_GROUPS {
            assert_eq!(cp["groupsOmitted"], groups, "{entry}");
        } else {
            assert_eq!(cp["groups"].as_array().unwrap().len(), groups, "{entry}");
            assert_eq!(
                cp["groups"][0]["positions"]["related"],
                topics * partitions as usize,
                "{entry}"
            );
        }
    }
}

/// **PROD-04.1: the catalog view shows the snapshot's freshness and whether
/// each group's positions relate to archived data.** An `Available` 1.7.0
/// point publishes when its positions were observed and, per group, its
/// outcome and counts; the control — a 1.3.0 point — publishes nothing (NOT
/// PUBLISHED).
#[test]
fn an_available_1_7_0_point_publishes_its_consumer_position_summary() {
    let receipt = positioned_catalog_receipt(&["billing".into(), "gone".into()]);
    let (objects, _) = versioned_objects(&receipt, &[("v1", CATALOG_MANIFEST)]);
    let entry = only_entry(objects);
    assert_eq!(entry["availability"], "Available", "{entry}");
    let cp = &entry["consumerPositions"];
    assert_eq!(
        cp["observedToMs"],
        catalog_ts("2026-09-16T02:59:59Z").timestamp_millis(),
        "{entry}"
    );
    // Freshness: one second before the recovery point.
    assert_eq!(
        entry["recoveryPointAtMs"].as_i64().unwrap() - cp["observedToMs"].as_i64().unwrap(),
        1000,
        "{entry}"
    );
    assert_eq!(cp["listing"], "complete", "{entry}");
    assert_eq!(
        cp["groups"],
        serde_json::json!([
            {"groupId": "billing", "outcome": "captured", "groupType": "classic", "active": true,
             "positions": {"related": 1, "notRelated": 0, "neverCommitted": 0, "beyondEnd": 0,
                           "failed": 0, "notObserved": 0}},
            {"groupId": "gone", "outcome": "excluded", "reason": "GroupNotFound"},
        ]),
        "{entry}"
    );
    let (control, _) = versioned_objects(
        &modelled_catalog_receipt(&[("orders", 3, 1)]),
        &[("v1", CATALOG_MANIFEST)],
    );
    let control = only_entry(control);
    assert!(control.get("consumerPositions").is_none(), "{control}");
}

/// **The review's A7: a summary the receipt does not back is never shown.**
/// A record whose consumer position summary was changed after it was written
/// — a failed group made captured — is a `Conflict` (rule 3), and the entry
/// publishes NO `consumerPositions`: the view never shows a summary the sync
/// could not stand behind. The control — the record as written — is
/// `Available` and publishes it.
#[test]
fn a_record_whose_position_summary_the_receipt_does_not_back_publishes_none() {
    let receipt = positioned_catalog_receipt(&["billing".into(), "gone".into()]);
    let (_, f) = versioned_objects(&receipt, &[("v1", CATALOG_MANIFEST)]);
    let mut record = f.point.clone();
    let cp = record
        .consumer_positions
        .as_mut()
        .expect("the summary travels");
    let gone = cp
        .groups
        .iter_mut()
        .find(|g| g.group_id == "gone")
        .expect("gone");
    gone.outcome = "captured".into();
    gone.reason = None;
    let objects = FakeObjects::new()
        .with_object(&f.log_key, &f.log_bytes)
        .with_object(&f.record_key, &record.canonical_bytes().unwrap())
        .with_object(&f.receipt_key, &f.receipt_bytes)
        .with_object(&f.sidecar_key, &f.sidecar_bytes)
        .with_versions(&f.manifest_key, &[("v1", CATALOG_MANIFEST)]);
    let entry = only_entry(objects);
    assert_eq!(entry["availability"], "Conflict", "{entry}");
    assert!(
        entry.get("consumerPositions").is_none(),
        "a summary the receipt does not back is not published: {entry}"
    );
    let (control, _) = versioned_objects(&receipt, &[("v1", CATALOG_MANIFEST)]);
    let control = only_entry(control);
    assert_eq!(control["availability"], "Available", "{control}");
    assert!(control.get("consumerPositions").is_some(), "{control}");
}

/// More groups than the cap: none listed, the count said; the freshness stays.
#[test]
fn a_point_with_more_groups_than_the_cap_lists_none_and_counts_them() {
    use logweir::check::kinds::catalog_sync::MAX_ENTRY_GROUPS;
    let ids: Vec<String> = (0..=MAX_ENTRY_GROUPS).map(|i| format!("g{i:03}")).collect();
    let (objects, _) = versioned_objects(
        &positioned_catalog_receipt(&ids),
        &[("v1", CATALOG_MANIFEST)],
    );
    let entry = only_entry(objects);
    let cp = &entry["consumerPositions"];
    assert!(cp.get("groups").is_none(), "{entry}");
    assert_eq!(cp["groupsOmitted"], MAX_ENTRY_GROUPS + 1, "{entry}");
    assert_eq!(cp["listing"], "complete", "{entry}");
}

/// A pinned point whose pinned version IS the current one is `Available`,
/// with the record's own (pinned) format reported.
#[test]
fn a_pinned_point_whose_version_is_current_is_available() {
    let (objects, _) =
        versioned_objects(&pinned_catalog_receipt("v1"), &[("v1", CATALOG_MANIFEST)]);
    let entry = only_entry(objects);
    assert_eq!(entry["availability"], "Available", "{entry}");
    assert_eq!(
        entry["formatVersion"],
        logweir::catalog::record::FORMAT_VERSION_WITH_MANIFEST_VERSION,
        "{entry}"
    );
}

/// **The pin's whole point, in the catalog.** Engine 0.21.0 re-running over a
/// set can put a manifest whose bytes are IDENTICAL while it rewrites the
/// segments under it (measured, FX-7). The digest cannot see that; the version
/// can: the point is `Conflict`, not selectable, with a remedy that says the
/// set was written again. The control is the same history under a point that
/// pins nothing — `Available`, which is exactly the blindness the pin closes.
#[test]
fn a_pinned_point_whose_manifest_was_written_again_is_a_conflict_that_says_so() {
    let history: &[(&str, &[u8])] = &[("v1", CATALOG_MANIFEST), ("v2", CATALOG_MANIFEST)];
    let (objects, _) = versioned_objects(&pinned_catalog_receipt("v1"), history);
    let entry = only_entry(objects);
    assert_eq!(entry["availability"], "Conflict", "{entry}");
    assert_eq!(
        entry["remedy"],
        logweir::catalog::pin::SUPERSEDED_REMEDY,
        "{entry}"
    );

    let (control, _) = versioned_objects(
        &catalog_receipt("set-a", "run-a", "2026-09-16T03:00:00Z"),
        history,
    );
    assert_eq!(
        only_entry(control)["availability"],
        "Available",
        "a point that pins nothing cannot see an identical rewrite — the control"
    );
}

/// **The receipt is the authority.** A record an OLDER writer produced carries
/// no pin (absent means unknown) while its receipt does: the deep check takes
/// the pin from the RECEIPT, so the rewrite is still seen.
#[test]
fn the_deep_check_takes_the_pin_from_the_receipt_not_from_the_record() {
    let receipt = pinned_catalog_receipt("v1");
    let (_, pinned) = versioned_objects(&receipt, &[("v1", CATALOG_MANIFEST)]);
    // The record an older writer would have written: the same point, no pin.
    let mut record = pinned.point.clone();
    record.archive.manifest_version_id = None;
    record.format_version = "1.0.0".to_string();
    let record_bytes = record.canonical_bytes().expect("the record serialises");
    let objects = FakeObjects::new()
        .with_object(&pinned.log_key, &pinned.log_bytes)
        .with_object(&pinned.record_key, &record_bytes)
        .with_object(&pinned.receipt_key, &pinned.receipt_bytes)
        .with_object(&pinned.sidecar_key, &pinned.sidecar_bytes)
        .with_versions(
            &pinned.manifest_key,
            &[("v1", CATALOG_MANIFEST), ("v2", CATALOG_MANIFEST)],
        );
    let entry = only_entry(objects);
    assert_eq!(
        entry["availability"], "Conflict",
        "the pin was read from the record, which an older writer left out: {entry}"
    );
}

/// The LIVE object handle — a real `Store`, here the versioned in-memory
/// double — reports the version it read: a pinned point whose version is
/// current is `Available` through it, and the same point after a rewrite is
/// `Conflict`. Were `Store`'s `ObjectAccess` implementation to fall back to the
/// trait's default (no version), the first half would read `Conflict`.
#[test]
fn catalog_sync_reads_the_version_the_live_store_reports() {
    for rewritten in [false, true] {
        let (store, bucket) = logweir_engine_oso::storage::Store::in_memory_versioned("");
        let manifest_key = "logweir/archive/set-a/manifest.json";
        let version = store
            .put_create_only(manifest_key, CATALOG_MANIFEST)
            .unwrap()
            .version_id
            .unwrap();
        let mut receipt = pinned_catalog_receipt(&version);
        receipt.archive.manifest_key = manifest_key.to_string();
        let f = catalog_fixture(
            &receipt,
            "s3://lw-archive/kafka-backups",
            &claimed_sidecar(CATALOG_CLAIMED_KEY_ID),
            CATALOG_CLAIMED_KEY_ID,
        );
        for (key, bytes) in [
            (&f.log_key, &f.log_bytes),
            (&f.record_key, &f.record_bytes),
            (&f.receipt_key, &f.receipt_bytes),
            (&f.sidecar_key, &f.sidecar_bytes),
        ] {
            store.put_create_only(key, bytes).unwrap();
        }
        if rewritten {
            bucket.overwrite(manifest_key, CATALOG_MANIFEST);
        }
        let wiring = FakeWiring {
            shared_store: Some(Arc::new(store)),
            ..FakeWiring::default()
        };
        let run = drive_sync(sync_request(), &wiring);
        let entries = entries_of(&body_of(&run));
        assert_eq!(entries.len(), 1, "{entries:?}");
        assert_eq!(
            entries[0]["availability"],
            if rewritten { "Conflict" } else { "Available" },
            "rewritten={rewritten}: {}",
            entries[0]
        );
    }
}

// ---------------------------------------------------------------------------
// FX-7 fix round (review H-1): a byte-identical COPY of a pinned point is the
// same point in another place — never "written again"
// ---------------------------------------------------------------------------

/// The remedy of an `Available` point whose pin could not be checked here:
/// the table's own remedy (these fixtures trust no key, so `notAttempted`'s)
/// with the note after it, and never the superseded remedy.
fn assert_noted(entry: &serde_json::Value) {
    let remedy = entry["remedy"].as_str().expect("a remedy");
    assert!(
        remedy.contains(logweir::catalog::pin::UNCHECKED_NOTE),
        "the pin could not be checked in this bucket, and the entry says so: {entry}"
    );
    assert!(
        !remedy.contains(logweir::catalog::pin::SUPERSEDED_CAUSE),
        "never the superseded remedy: {entry}"
    );
}

/// **H-1, the unversioned copy.** A pinned point copied byte for byte into an
/// UNVERSIONED bucket: the copy answers no version id and cannot be read by
/// one. It is the same signed point (one point in two places), `Available` by
/// its digest, with the note. Before the fix it was `Conflict`.
#[test]
fn a_byte_identical_copy_of_a_pinned_point_in_an_unversioned_bucket_is_available() {
    let f = catalog_fixture(
        &pinned_catalog_receipt("v1"),
        "s3://lw-copy/kafka-backups",
        &claimed_sidecar(CATALOG_CLAIMED_KEY_ID),
        CATALOG_CLAIMED_KEY_ID,
    );
    let objects = place(FakeObjects::new(), &f);
    let entry = only_entry(objects.clone());
    assert_eq!(entry["availability"], "Available", "{entry}");
    assert_noted(&entry);
    assert!(
        objects
            .calls()
            .contains(&format!("get {}?versionId=v1", f.manifest_key)),
        "the pinned version was asked for BY ID before the point was judged: {:?}",
        objects.calls()
    );
}

/// **H-1, the versioned copy.** The same copy into a VERSIONED bucket that
/// issued its OWN ids: the pinned id is not in its history (`NotFound`).
#[test]
fn a_byte_identical_copy_in_a_versioned_bucket_with_its_own_ids_is_available() {
    let (objects, _) = versioned_objects(
        &pinned_catalog_receipt("v1"),
        &[("copy-7f3a", CATALOG_MANIFEST)],
    );
    let entry = only_entry(objects);
    assert_eq!(entry["availability"], "Available", "{entry}");
    assert_noted(&entry);
}

/// **Review L-1, K4: a store that answers NO version is not, by itself, a
/// copy.** Versioning SUSPENDED and the key written again: S3 then answers the
/// current read with the literal `null` (no pinnable version), while the
/// version the receipt pins is still in the bucket's history. The read by id
/// finds it, so this is a rewrite HERE — `Conflict`, the superseded remedy —
/// and not the unversioned copy's note. Only the by-id read tells the two
/// apart.
#[test]
fn a_rewrite_after_versioning_was_suspended_is_a_conflict_not_a_copy() {
    let (objects, _) = versioned_objects(
        &pinned_catalog_receipt("v1"),
        &[("v1", CATALOG_MANIFEST), ("null", CATALOG_MANIFEST)],
    );
    let entry = only_entry(objects);
    assert_eq!(entry["availability"], "Conflict", "{entry}");
    assert_eq!(
        entry["remedy"],
        logweir::catalog::pin::SUPERSEDED_REMEDY,
        "{entry}"
    );
}

/// **The control.** The same copy of a point that pins NOTHING: `Available`
/// with exactly the table's remedy — the note is about a pin, not about copies
/// — and no read by version at all.
#[test]
fn a_copy_of_an_unpinned_point_carries_no_note() {
    use logweir::check::kinds::catalog_sync::{Availability, SignatureVerdict};
    let f = catalog_fixture(
        &catalog_receipt("set-a", "run-a", "2026-09-16T03:00:00Z"),
        "s3://lw-copy/kafka-backups",
        &claimed_sidecar(CATALOG_CLAIMED_KEY_ID),
        CATALOG_CLAIMED_KEY_ID,
    );
    let objects = place(FakeObjects::new(), &f);
    let entry = only_entry(objects.clone());
    assert_eq!(entry["availability"], "Available", "{entry}");
    assert_eq!(
        entry["remedy"],
        logweir::check::kinds::catalog_sync::remedy_for(
            Availability::Available,
            SignatureVerdict::NotAttempted
        )
        .expect("notAttempted has a remedy"),
        "{entry}"
    );
    assert!(
        !objects.calls().iter().any(|c| c.contains("?versionId=")),
        "{:?}",
        objects.calls()
    );
}

/// The fallback IS the digest check: a copy whose manifest bytes differ is
/// `Conflict` by the digest, with the generic remedy and the note — the pin
/// was not what judged it.
#[test]
fn a_copy_whose_manifest_differs_is_a_conflict_by_the_digest() {
    use logweir::check::kinds::catalog_sync::{Availability, SignatureVerdict};
    let (objects, _) = versioned_objects(
        &pinned_catalog_receipt("v1"),
        &[("copy-7f3a", br#"{"topics":["swapped"]}"#)],
    );
    let entry = only_entry(objects);
    assert_eq!(entry["availability"], "Conflict", "{entry}");
    let remedy = entry["remedy"].as_str().expect("a remedy");
    assert!(
        remedy.starts_with(
            logweir::check::kinds::catalog_sync::remedy_for(
                Availability::Conflict,
                SignatureVerdict::NotAttempted
            )
            .expect("Conflict has a remedy")
        ),
        "{entry}"
    );
    assert_noted(&entry);
}

/// A read of the pinned version that fails for any OTHER reason (a principal
/// without `s3:GetObjectVersion`, an outage) is `Unreadable` — "could not
/// tell" — and never the note, which would pass a rewrite nobody ruled out.
#[test]
fn a_pinned_version_that_cannot_be_read_is_unreadable() {
    let (objects, f) = versioned_objects(
        &pinned_catalog_receipt("v1"),
        &[("v1", CATALOG_MANIFEST), ("v2", CATALOG_MANIFEST)],
    );
    let objects = objects.failing_key(
        &format!("{}?versionId=v1", f.manifest_key),
        Fault::Io(
            "Generic S3 error: Error performing GET http://s3/k?versionId=v1 in 2ms - Server \
             returned non-2xx status code: 403 Forbidden: <Error><Code>AccessDenied</Code>"
                .into(),
        ),
    );
    let entry = only_entry(objects);
    assert_eq!(entry["availability"], "Unreadable", "{entry}");
    assert!(
        !entry["remedy"]
            .as_str()
            .unwrap_or_default()
            .contains(logweir::catalog::pin::UNCHECKED_NOTE),
        "{entry}"
    );
    // FX-7 re-check, nit 1: the remedy names the grant a by-id read needs,
    // which the generic `Unreadable` remedy does not.
    assert_eq!(
        entry["remedy"],
        logweir::catalog::pin::UNREADABLE_REMEDY,
        "{entry}"
    );
}

/// The live `Store` behind the trait, the original and both copy shapes: an
/// unversioned in-memory store (it does not read by version: `Backend`) and a
/// versioned double with its own ids (`NotFound`). The original is read as
/// current through `Store`'s own `get_with_version`; the copies' answers come
/// from `Store::get_version` itself, not from a fake's model of it. (That a
/// RETAINED pinned version still reaches `Conflict` through the live store —
/// which needs `Store`'s `get_version` override — is
/// `catalog_sync_reads_the_version_the_live_store_reports`.)
#[test]
fn catalog_sync_over_a_live_store_copy_notes_the_pin() {
    let (original, _bucket) = logweir_engine_oso::storage::Store::in_memory_versioned("");
    let manifest_key = "logweir/archive/set-a/manifest.json";
    let version = original
        .put_create_only(manifest_key, CATALOG_MANIFEST)
        .unwrap()
        .version_id
        .unwrap();
    let mut receipt = pinned_catalog_receipt(&version);
    receipt.archive.manifest_key = manifest_key.to_string();
    let f = catalog_fixture(
        &receipt,
        "s3://lw-archive/kafka-backups",
        &claimed_sidecar(CATALOG_CLAIMED_KEY_ID),
        CATALOG_CLAIMED_KEY_ID,
    );
    let objects: [(&str, &[u8]); 5] = [
        (&f.log_key, &f.log_bytes),
        (&f.record_key, &f.record_bytes),
        (&f.receipt_key, &f.receipt_bytes),
        (&f.sidecar_key, &f.sidecar_bytes),
        (manifest_key, CATALOG_MANIFEST),
    ];
    for (key, bytes) in &objects[..4] {
        original.put_create_only(key, bytes).unwrap();
    }
    let unversioned = logweir_engine_oso::storage::Store::in_memory("");
    let (versioned, _copy_bucket) = logweir_engine_oso::storage::Store::in_memory_versioned("");
    for (key, bytes) in objects {
        unversioned.put_create_only(key, bytes).unwrap();
        versioned.put_create_only(key, bytes).unwrap();
    }
    for (what, store, noted) in [
        ("the original", original, false),
        ("an unversioned copy", unversioned, true),
        ("a versioned copy with its own ids", versioned, true),
    ] {
        let wiring = FakeWiring {
            shared_store: Some(Arc::new(store)),
            ..FakeWiring::default()
        };
        let run = drive_sync(sync_request(), &wiring);
        let entries = entries_of(&body_of(&run));
        assert_eq!(entries.len(), 1, "{what}: {entries:?}");
        assert_eq!(
            entries[0]["availability"], "Available",
            "{what}: {}",
            entries[0]
        );
        assert_eq!(
            entries[0]["remedy"]
                .as_str()
                .unwrap_or_default()
                .contains(logweir::catalog::pin::UNCHECKED_NOTE),
            noted,
            "{what}: {}",
            entries[0]
        );
    }
}

/// **The fifth object is paid for before the point is begun.** A pinned copy
/// costs five objects (record, receipt, sidecar, manifest, the pinned version
/// by id), and [`OBJECTS_PER_POINT`] reserves five: with seven objects (the
/// floor listing, one shard listing, one point) it is examined whole; with six
/// it is not begun. A reservation of four would begin it at six and spend
/// seven — the overspend `a_budget_stop_is_the_walks_outcome_and_never_a_points`
/// forbids for unpinned points.
#[test]
fn a_pinned_copy_is_reserved_its_fifth_object() {
    let f = catalog_fixture(
        &pinned_catalog_receipt("v1"),
        "s3://lw-copy/kafka-backups",
        &claimed_sidecar(CATALOG_CLAIMED_KEY_ID),
        CATALOG_CLAIMED_KEY_ID,
    );
    let objects = place(FakeObjects::new(), &f);
    for (budget, examined) in [(7_i64, 1_i64), (6, 0)] {
        let run = drive_sync(
            logweir_core::check_contract::CatalogSyncRequest {
                max_objects_per_run: budget,
                ..sync_request()
            },
            &FakeWiring::default().with_role(DestinationRole::ArchiveRead, objects.clone()),
        );
        let body = body_of(&run);
        let spent: i64 = run.row(CheckId::DestinationArchiveListable).facts["catalogObjectsRead"]
            .parse()
            .expect("an object count");
        assert!(
            spent <= budget,
            "A WALK NEVER SPENDS MORE OBJECTS THAN ITS BUDGET: {spent} of {budget}. {body}"
        );
        assert_eq!(
            summary_of(&body, "catalog-counts=")["total"],
            examined,
            "budget {budget}: {body}"
        );
        // And the read by id is COUNTED: two listings and five objects, not
        // two listings and four plus an uncounted fifth.
        assert_eq!(
            spent,
            2 + examined * 5,
            "budget {budget}: catalogObjectsRead counts every read. {body}"
        );
    }
}

/// The happy path: the grammar, in order, with the fence last.
#[test]
fn a_catalog_sync_relays_the_body_the_grammar_specifies() {
    let (objects, newer, older) = two_point_objects();
    let run = drive_sync(
        sync_request(),
        &FakeWiring::default().with_role(DestinationRole::ArchiveRead, objects.clone()),
    );
    assert_eq!(run.code, ExitCode::Ok);
    let body = body_of(&run);
    let lines: Vec<&str> = body.lines().collect();

    assert_eq!(
        lines[0], "catalog-format=1",
        "the version line is first and required: {body}"
    );
    assert!(
        lines[1].starts_with("catalog-page=1/1 count=2 sha256="),
        "one page of two entries: {body}"
    );
    assert!(
        lines.last().expect("a body").starts_with("catalog-cursor="),
        "THE FENCE IS LAST, so a truncated read cannot end with a cursor claiming a walk that \
         did not happen: {body}"
    );

    // Newest recovery point first.
    let entries = entries_of(&body);
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0]["pointId"], newer.point.point_id);
    assert_eq!(entries[1]["pointId"], older.point.point_id);

    // Both axes, and the binding.
    assert_eq!(entries[0]["availability"], "Available");
    assert_eq!(entries[0]["signature"], "notAttempted");
    assert_eq!(entries[0]["signerKeyId"], CATALOG_CLAIMED_KEY_ID);
    assert_eq!(entries[0]["receiptSha256"], newer.point.receipt.sha256);
    assert_eq!(
        entries[0]["manifestSha256"],
        newer.point.archive.manifest_sha256
    );
    assert_eq!(entries[0]["recordedAt"], "2026-09-16T06:00:00Z");

    // The page digest covers the page's own entry lines and nothing else.
    let raw: Vec<&str> = body
        .lines()
        .filter_map(|l| l.strip_prefix("catalog-entry="))
        .collect();
    assert!(
        lines[1].ends_with(&logweir::check::kinds::catalog_sync::page_digest(&raw)),
        "the header's digest is over the raw entry lines: {body}"
    );

    let counts = summary_of(&body, "catalog-counts=");
    assert_eq!(counts["total"], 2);
    assert_eq!(counts["available"], 2);
    assert_eq!(counts["signature"]["notAttempted"], 2);
    assert_eq!(
        counts["byDay"],
        serde_json::json!([
            {"day": "2026-09-16", "points": 1},
            {"day": "2026-09-15", "points": 1}
        ]),
        "the histogram is newest day first"
    );

    let signers = summary_of(&body, "catalog-signers=");
    assert_eq!(signers[0]["keyId"], CATALOG_CLAIMED_KEY_ID);
    assert_eq!(signers[0]["points"], 2);
    assert!(
        signers[0].get("principalHint").is_none(),
        "a record carries no principal, so no hint is invented: {signers}"
    );

    let cursor = summary_of(&body, "catalog-cursor=");
    assert_eq!(cursor["complete"], true);
    assert_eq!(
        cursor["indexShard"], "2026-09-15",
        "the reported floor is the ARCHIVE's own oldest day — one listing establishes it — and \
         not a lookback measured from today: {body}"
    );

    // And the check row says what the walk did, with no credential in it.
    let row = run.row(CheckId::DestinationArchiveListable);
    assert_eq!(row.state, CheckState::Ready);
    assert_eq!(row.code, CheckCode::ArchiveListable);
    assert_eq!(row.facts["catalogPoints"], "2");
    assert_eq!(row.facts["catalogEntries"], "2");
    assert_eq!(row.facts["catalogTrustKeys"], "0");
    assert_eq!(row.facts["catalogTrustNote"], "noTrustMaterial");
    assert!(run.has(CheckId::RunnerContract));

    // NOTHING WAS WRITTEN. `put_create_only` is the only write a check may make
    // and a catalog sync may not make even that one.
    assert!(
        objects.puts().is_empty(),
        "a catalog sync writes nothing at all: {:?}",
        objects.puts()
    );
}

/// An entry names ONE location — the record's own — with that location's own
/// verdict, so the controller's best-of merge has a verdict to merge.
#[test]
fn an_entry_carries_one_location_with_its_own_verdict() {
    let (objects, newer, older) = two_point_objects();
    let run = drive_sync(
        sync_request(),
        &FakeWiring::default().with_role(DestinationRole::ArchiveRead, objects),
    );
    let entries = entries_of(&body_of(&run));
    assert_eq!(
        entries[0]["locations"],
        serde_json::json!([{
            "locationId": newer.point.archive.location_id,
            "availability": "Available"
        }]),
        "one sync reads one destination, and the location carries its OWN verdict rather than \
         making the reader re-apply an inheritance rule"
    );

    // And a DEGRADED observation says so at the location too: a location row
    // that always read `Available` would make the controller's best-of merge
    // return `Available` for a point neither copy holds.
    let degraded = place(FakeObjects::new(), &older)
        .with_object(&newer.log_key, &newer.log_bytes)
        .with_object(&newer.record_key, &newer.record_bytes)
        .with_object(&newer.receipt_key, &newer.receipt_bytes)
        .with_object(&newer.sidecar_key, &newer.sidecar_bytes);
    let run = drive_sync(
        sync_request(),
        &FakeWiring::default().with_role(DestinationRole::ArchiveRead, degraded),
    );
    let entries = entries_of(&body_of(&run));
    assert_eq!(entries[0]["availability"], "Missing");
    assert_eq!(
        entries[0]["locations"][0]["availability"], "Missing",
        "the location carries the observation's verdict, not a constant: {}",
        entries[0]
    );
}

/// A manifest with no receipt sidecar beside it is `noEvidence` — a fact about
/// the ARCHIVE — and never `notAttempted`, which is a fact about this run.
#[test]
fn a_point_with_no_sidecar_is_no_evidence_and_not_merely_unattempted() {
    let sidecar = claimed_sidecar(CATALOG_CLAIMED_KEY_ID);
    let f = catalog_fixture(
        &catalog_receipt("set-a", "run-a", "2026-09-16T03:00:00Z"),
        "s3://lw-archive/kafka-backups",
        &sidecar,
        CATALOG_CLAIMED_KEY_ID,
    );
    // Everything but the sidecar.
    let objects = FakeObjects::new()
        .with_object(&f.log_key, &f.log_bytes)
        .with_object(&f.record_key, &f.record_bytes)
        .with_object(&f.receipt_key, &f.receipt_bytes)
        .with_object(&f.manifest_key, CATALOG_MANIFEST);
    let run = drive_sync(
        sync_request(),
        &FakeWiring::default().with_role(DestinationRole::ArchiveRead, objects),
    );
    let body = body_of(&run);
    let entries = entries_of(&body);
    assert_eq!(entries[0]["signature"], "noEvidence");
    assert!(
        entries[0].get("signerKeyId").is_none(),
        "there is no key to name: {}",
        entries[0]
    );
    assert_eq!(
        summary_of(&body, "catalog-counts=")["signature"]["noEvidence"],
        1
    );
    assert_eq!(
        summary_of(&body, "catalog-signers="),
        serde_json::json!([]),
        "an unsigned point contributes no signer row: {body}"
    );
    assert_eq!(
        entries[0]["availability"], "Available",
        "the BYTES are all there; it is the evidence that is not, and the two axes never merge"
    );
    assert!(entries[0]["remedy"]
        .as_str()
        .expect("a noEvidence point names a remedy")
        .contains("no signed receipt"));
}

/// A tampered receipt: a key this installation HOLDS signed the sidecar and the
/// signature does not verify over the bytes in the bucket.
#[test]
fn a_tampered_receipt_is_invalid_and_never_merely_unverified() {
    let key = logweir_evidence::keys::SigningKey::generate_p256();
    let key_id = key.verifying_key().key_id();
    let pem = key
        .verifying_key()
        .to_public_key_pem()
        .expect("a public key renders");

    let receipt = catalog_receipt("set-a", "run-a", "2026-09-16T03:00:00Z");
    let receipt_bytes =
        logweir_core::det_json::to_deterministic_json(&receipt).expect("the receipt serialises");
    let sidecar = logweir_evidence::sign::sign_detached(
        &key,
        logweir_evidence::PAYLOAD_TYPE_BACKUP_RECEIPT,
        &receipt_bytes,
    )
    .expect("the sidecar signs");
    let f = catalog_fixture(&receipt, "s3://lw-archive/kafka-backups", &sidecar, &key_id);

    // The CONTROL: untouched bytes verify under the mounted key.
    let good = place(FakeObjects::new(), &f);
    let wiring = FakeWiring::default()
        .with_role(DestinationRole::ArchiveRead, good)
        .with_file("/check/trust/trust-bundle.pem", pem.as_bytes());
    let request = logweir_core::check_contract::CatalogSyncRequest {
        trust_bundle_file: Some("/check/trust/trust-bundle.pem".to_string()),
        ..sync_request()
    };
    let entries = entries_of(&body_of(&drive_sync(request.clone(), &wiring)));
    assert_eq!(entries[0]["signature"], "verified");
    assert_eq!(entries[0]["signerKeyId"], key_id);
    assert!(
        entries[0].get("remedy").is_none(),
        "a selectable point needs no remedy: {}",
        entries[0]
    );

    // THE MUTATION: the receipt is edited after it was signed, in a way that
    // keeps it a receipt. A mangled BYTE would make it unparseable, which is
    // `Unreadable` and a different fact; this one still deserialises, so both
    // axes get to answer.
    let tampered = String::from_utf8(f.receipt_bytes.clone())
        .expect("the receipt is UTF-8")
        .replace("\"schedule\"", "\"scheduIe\"");
    assert_ne!(tampered.as_bytes(), f.receipt_bytes.as_slice());
    let bad = place(FakeObjects::new(), &f).with_object(&f.receipt_key, tampered.as_bytes());
    let wiring = FakeWiring::default()
        .with_role(DestinationRole::ArchiveRead, bad)
        .with_file("/check/trust/trust-bundle.pem", pem.as_bytes());
    let entries = entries_of(&body_of(&drive_sync(request, &wiring)));
    assert_eq!(
        entries[0]["signature"], "invalid",
        "bytes that do not match their signature are a definite negative, not a missing verdict"
    );
    assert_eq!(entries[0]["signerKeyId"], key_id);
    assert_eq!(
        entries[0]["availability"], "Conflict",
        "the record names a receipt whose digest is not the one it claims, so the two documents \
         are about different bytes"
    );
    assert!(entries[0]["remedy"]
        .as_str()
        .expect("a Conflict names a remedy")
        .contains("contradicts the signed receipt"));
}

/// A sidecar naming a key this pod does not hold is `notAttempted` WITH the
/// claimed id — never `invalid`, and never `verified`.
#[test]
fn a_signature_by_a_key_this_pod_does_not_hold_is_not_attempted() {
    let ours = logweir_evidence::keys::SigningKey::generate_p256();
    let pem = ours
        .verifying_key()
        .to_public_key_pem()
        .expect("a public key renders");
    let (objects, _, _) = two_point_objects();
    let request = logweir_core::check_contract::CatalogSyncRequest {
        trust_bundle_file: Some("/check/trust/trust-bundle.pem".to_string()),
        ..sync_request()
    };
    let run = drive_sync(
        request,
        &FakeWiring::default()
            .with_role(DestinationRole::ArchiveRead, objects)
            .with_file("/check/trust/trust-bundle.pem", pem.as_bytes()),
    );
    let body = body_of(&run);
    let entries = entries_of(&body);
    assert_eq!(entries[0]["signature"], "notAttempted");
    assert_eq!(
        entries[0]["signerKeyId"], CATALOG_CLAIMED_KEY_ID,
        "the stranger's key id is reported so `status.counts.untrustedSigner` can be exact"
    );
    assert_eq!(
        summary_of(&body, "catalog-signers=")[0]["keyId"],
        CATALOG_CLAIMED_KEY_ID
    );
    assert_eq!(
        run.row(CheckId::DestinationArchiveListable).facts["catalogTrustKeys"],
        "1"
    );
}

/// No trust bundle at all: nothing is verified and nothing is disproved.
#[test]
fn no_trust_material_verifies_nothing_and_says_so() {
    let key = logweir_evidence::keys::SigningKey::generate_p256();
    let receipt = catalog_receipt("set-a", "run-a", "2026-09-16T03:00:00Z");
    let receipt_bytes =
        logweir_core::det_json::to_deterministic_json(&receipt).expect("the receipt serialises");
    let sidecar = logweir_evidence::sign::sign_detached(
        &key,
        logweir_evidence::PAYLOAD_TYPE_BACKUP_RECEIPT,
        &receipt_bytes,
    )
    .expect("the sidecar signs");
    let f = catalog_fixture(
        &receipt,
        "s3://lw-archive/kafka-backups",
        &sidecar,
        &key.verifying_key().key_id(),
    );
    let run = drive_sync(
        sync_request(),
        &FakeWiring::default()
            .with_role(DestinationRole::ArchiveRead, place(FakeObjects::new(), &f)),
    );
    let body = body_of(&run);
    let entries = entries_of(&body);
    assert_eq!(
        entries[0]["signature"], "notAttempted",
        "a genuinely valid signature is still NOT verified when this installation holds no key"
    );
    assert_eq!(
        summary_of(&body, "catalog-counts=")["signature"]["notAttempted"],
        1
    );
    assert_eq!(
        run.row(CheckId::DestinationArchiveListable).facts["catalogTrustNote"],
        "noTrustMaterial"
    );
}

/// A manifest the archive does not hold is `Missing`; an object that could not
/// be read is `Unreadable`. **They are different answers.**
#[test]
fn a_missing_manifest_is_missing_and_an_unreadable_one_is_not() {
    let (objects, newer, older) = two_point_objects();

    // MISSING: every object but the manifest.
    let missing = place(FakeObjects::new(), &older)
        .with_object(&newer.log_key, &newer.log_bytes)
        .with_object(&newer.record_key, &newer.record_bytes)
        .with_object(&newer.receipt_key, &newer.receipt_bytes)
        .with_object(&newer.sidecar_key, &newer.sidecar_bytes);
    let run = drive_sync(
        sync_request(),
        &FakeWiring::default().with_role(DestinationRole::ArchiveRead, missing),
    );
    let body = body_of(&run);
    let entries = entries_of(&body);
    assert_eq!(entries[0]["availability"], "Missing");
    assert!(
        entries[0]["remedy"]
            .as_str()
            .expect("a Missing point names a remedy")
            .contains("does not hold"),
        "{}",
        entries[0]
    );
    assert_eq!(summary_of(&body, "catalog-counts=")["missing"], 1);

    // UNREADABLE: the same shape of failure, from a denial rather than an
    // absence. A 403 that reported `Missing` is how an operator comes to
    // believe an outage deleted their backups.
    let denied = objects
        .clone()
        .failing_get(Fault::Io(s3_refusal("403 Forbidden", "AccessDenied")));
    let run = drive_sync(
        sync_request(),
        &FakeWiring::default().with_role(DestinationRole::ArchiveRead, denied),
    );
    let body = body_of(&run);
    let counts = summary_of(&body, "catalog-counts=");
    assert_eq!(counts["unreadable"], 2, "{body}");
    assert_eq!(
        counts["missing"], 0,
        "`Unreadable` is never `Missing`: {body}"
    );
    assert_eq!(
        counts["total"], 2,
        "the walk still SAW both points — the shard listing is what establishes that"
    );
    // FX-33: AND BOTH ARE LISTED, by the point id each key carries. They
    // used to be counted and listed by no entry. A read that did not answer
    // is the one cause whose remedy names the grant.
    let entries = entries_of(&body);
    assert_eq!(entries.len(), 2, "both points are listed: {body}");
    for (entry, f) in entries.iter().zip([&newer, &older]) {
        assert_eq!(entry["pointId"], f.point.point_id.as_str(), "{entry}");
        assert_eq!(entry["availability"], "Unreadable", "{entry}");
        assert_eq!(entry["receiptKey"], "", "nothing was read: {entry}");
        assert!(
            entry["remedy"]
                .as_str()
                .is_some_and(|r| r.contains("archiveRead grant")),
            "{entry}"
        );
    }
}

/// A record that contradicts the receipt it names is a `Conflict`, and so is a
/// manifest whose bytes are not the ones the signed receipt describes.
#[test]
fn a_record_that_contradicts_its_receipt_is_a_conflict() {
    let sidecar = claimed_sidecar(CATALOG_CLAIMED_KEY_ID);
    let f = catalog_fixture(
        &catalog_receipt("set-a", "run-a", "2026-09-16T03:00:00Z"),
        "s3://lw-archive/kafka-backups",
        &sidecar,
        CATALOG_CLAIMED_KEY_ID,
    );

    // The record says one window; the receipt it names says another.
    let mut doc: serde_json::Value =
        serde_json::from_slice(&f.record_bytes).expect("the record is JSON");
    doc["covered"]["to_ms"] = serde_json::json!(1i64);
    let contradicting = serde_json::to_vec(&doc).expect("JSON");
    let objects = place(FakeObjects::new(), &f).with_object(&f.record_key, &contradicting);
    let run = drive_sync(
        sync_request(),
        &FakeWiring::default().with_role(DestinationRole::ArchiveRead, objects),
    );
    let entries = entries_of(&body_of(&run));
    assert_eq!(entries[0]["availability"], "Conflict");

    // And the archive contradicting the signed receipt is the same verdict:
    // the receipt is the authority and the bytes in the bucket are not it.
    let objects = place(FakeObjects::new(), &f).with_object(&f.manifest_key, b"{\"topics\":[1]}");
    let run = drive_sync(
        sync_request(),
        &FakeWiring::default().with_role(DestinationRole::ArchiveRead, objects),
    );
    let body = body_of(&run);
    assert_eq!(entries_of(&body)[0]["availability"], "Conflict");
    assert_eq!(summary_of(&body, "catalog-counts=")["conflict"], 1);
}

/// A record written by a newer Logweir is `UnsupportedFormat` — per entry, and
/// never fatal for the walk (D3 §5.2 rule 1).
#[test]
fn a_record_from_a_future_major_is_unsupported_and_not_fatal() {
    let (objects, newer, _) = two_point_objects();
    let mut doc: serde_json::Value =
        serde_json::from_slice(&newer.record_bytes).expect("the record is JSON");
    doc["format_version"] = serde_json::json!("2.0.0");
    let objects = objects.with_object(&newer.record_key, &serde_json::to_vec(&doc).expect("JSON"));
    let run = drive_sync(
        sync_request(),
        &FakeWiring::default().with_role(DestinationRole::ArchiveRead, objects),
    );
    let body = body_of(&run);
    let counts = summary_of(&body, "catalog-counts=");
    assert_eq!(counts["unsupportedFormat"], 1);
    assert_eq!(
        counts["available"], 1,
        "the other point still lists: {body}"
    );
    // FX-33: and the point this build cannot read is listed too, as what it
    // is, with the version it declares.
    let entries = entries_of(&body);
    assert_eq!(entries.len(), 2, "{body}");
    let unsupported = &entries[0];
    assert_eq!(unsupported["pointId"], newer.point.point_id.as_str());
    assert_eq!(
        unsupported["availability"], "UnsupportedFormat",
        "{unsupported}"
    );
    assert_eq!(unsupported["formatVersion"], "2.0.0", "{unsupported}");
    assert_eq!(unsupported["backupId"], "", "{unsupported}");
    assert!(
        unsupported["remedy"]
            .as_str()
            .is_some_and(|r| r.contains("newer Logweir")),
        "{unsupported}"
    );
    assert_eq!(entries[1]["availability"], "Available", "{body}");
}

/// The window is the plan's `viewLimit`; everything beyond it is COUNTED.
#[test]
fn the_body_is_bounded_by_the_view_limit_and_counts_the_rest() {
    let sidecar = claimed_sidecar(CATALOG_CLAIMED_KEY_ID);
    let mut objects = FakeObjects::new();
    for i in 0..12 {
        let f = catalog_fixture(
            &catalog_receipt(
                &format!("set-{i:02}"),
                "run-a",
                &format!("2026-09-16T{i:02}:00:00Z"),
            ),
            "s3://lw-archive/kafka-backups",
            &sidecar,
            CATALOG_CLAIMED_KEY_ID,
        );
        objects = place(objects, &f);
    }
    // FIVE, driven straight at the emitter. `CheckPlan::validate` refuses a
    // `viewLimit` below 100 in a real plan (asserted below), and the emitter
    // honours whatever bound it is handed rather than carrying a second one.
    let request = logweir_core::check_contract::CatalogSyncRequest {
        view_limit: 5,
        ..sync_request()
    };
    let run = drive_sync(
        request,
        &FakeWiring::default().with_role(DestinationRole::ArchiveRead, objects.clone()),
    );
    let body = body_of(&run);
    assert_eq!(
        entries_of(&body).len(),
        5,
        "the window is the viewLimit: {body}"
    );
    let counts = summary_of(&body, "catalog-counts=");
    assert_eq!(
        counts["total"], 12,
        "EVERY point the walk saw is counted, which is what makes `truncated` a fact about the \
         archive and not about the page set: {body}"
    );
    assert_eq!(
        counts["available"], 12,
        "EVERY counted point is examined; `viewLimit` bounds the LINES and nothing else \
         (review finding F1). The buckets summing to less than `total` on a completed walk is \
         what finding F2 called a silent bucket scope: {body}"
    );
    assert_eq!(
        counts["byDay"][0]["points"], 12,
        "the histogram covers the whole walk: {body}"
    );

    // And the object budget is the other bound. One object buys one shard
    // listing and nothing else.
    let request = logweir_core::check_contract::CatalogSyncRequest {
        max_objects_per_run: 1,
        ..sync_request()
    };
    let run = drive_sync(
        request,
        &FakeWiring::default().with_role(DestinationRole::ArchiveRead, objects),
    );
    let body = body_of(&run);
    assert!(entries_of(&body).is_empty(), "{body}");
    let cursor = summary_of(&body, "catalog-cursor=");
    assert_eq!(
        cursor["complete"], false,
        "a budgeted walk that continues is not a failure, and the cursor is what says so: {body}"
    );
}

/// The plan's own bounds, refused on the READ side: a declared bound is not a
/// bound.
#[test]
fn a_catalog_plan_outside_its_bounds_is_refused_before_any_client() {
    for (view_limit, max_objects, timeout, field) in [
        (99i64, 100_000i64, 900u32, "viewLimit"),
        (5_001, 100_000, 900, "viewLimit"),
        (2_000, 999, 900, "maxObjectsPerRun"),
        (2_000, 1_000_001, 900, "maxObjectsPerRun"),
        (2_000, 100_000, 1_801, "timeoutSeconds"),
        (2_000, 100_000, 0, "timeoutSeconds"),
    ] {
        let plan = CheckPlan {
            timeout_seconds: timeout,
            ..plan_of(CheckRequest::CatalogSync(Box::new(
                logweir_core::check_contract::CatalogSyncRequest {
                    view_limit,
                    max_objects_per_run: max_objects,
                    ..sync_request()
                },
            )))
        };
        let err = plan
            .validate()
            .expect_err("`{field}` outside its range is refused");
        let text = err.to_string();
        assert!(text.contains(field), "expected `{field}` in `{text}`");
        // THE MESSAGE IS WHAT AN OPERATOR READS in `refusal-detail`, so it is
        // asserted as prose and not only as a field name: it names the range
        // it refused against, and it carries no run of stray whitespace.
        assert!(
            text.contains("is outside") && text.contains(".."),
            "a refusal names the range it refused against: {text}"
        );
        assert!(
            !text.contains("  "),
            "a doubled space in an operator-facing refusal: {text:?}"
        );
    }

    // An index cursor that is not a UTC day would silently become a key prefix
    // that lists nothing, and a sync that walked nothing would publish an
    // empty view of a full archive.
    let plan = catalog_plan(logweir_core::check_contract::CatalogSyncRequest {
        index_shard: Some("2026-9-16".to_string()),
        ..sync_request()
    });
    assert!(plan
        .validate()
        .expect_err("a malformed index cursor is refused")
        .to_string()
        .contains("indexShard"));

    // And the one that a single 600-second ceiling would have refused: the
    // controller's own `SYNC_TIMEOUT_SECONDS` is 900.
    catalog_plan(sync_request())
        .validate()
        .expect("a 900-second catalog sync is inside the catalog ceiling");
}

/// A page is as full as the ceiling allows — splitting buys nothing and costs
/// a header and a digest each time.
#[test]
fn the_page_count_never_exceeds_eight_at_any_window_size() {
    use logweir::check::kinds::catalog_sync as cs;
    for entries in [1usize, 2, 20, 999, 1_000, 1_001, 5_000, 40_000] {
        let per_page = cs::entries_per_page(entries);
        let pages = entries.div_ceil(per_page);
        assert!(
            pages <= cs::MAX_BODY_PAGES,
            "{entries} entries would need {pages} pages and the parser refuses above {}",
            cs::MAX_BODY_PAGES
        );
    }
    assert_eq!(
        cs::entries_per_page(2).min(2),
        2,
        "two entries are ONE page: a body that split them would declare two headers and two \
         digests for no reason"
    );
    assert_eq!(cs::entries_per_page(5_000), 1_000);
}

/// The pages are `1..=n`, in order, `n <= 8`, and each declares its own digest.
#[test]
fn the_body_never_declares_more_than_eight_pages() {
    let sidecar = claimed_sidecar(CATALOG_CLAIMED_KEY_ID);
    let mut objects = FakeObjects::new();
    for i in 0..20 {
        let f = catalog_fixture(
            &catalog_receipt(
                &format!("set-{i:02}"),
                "run-a",
                &format!("2026-09-16T{:02}:{:02}:00Z", i / 4, (i % 4) * 15),
            ),
            "s3://lw-archive/kafka-backups",
            &sidecar,
            CATALOG_CLAIMED_KEY_ID,
        );
        objects = place(objects, &f);
    }
    let run = drive_sync(
        sync_request(),
        &FakeWiring::default().with_role(DestinationRole::ArchiveRead, objects),
    );
    let body = body_of(&run);
    let headers: Vec<&str> = body
        .lines()
        .filter_map(|l| l.strip_prefix("catalog-page="))
        .collect();
    assert!(headers.len() <= 8, "{} pages: {body}", headers.len());
    assert_eq!(headers.len(), 1, "twenty entries are one page: {body}");
    let mut counted = 0usize;
    for (i, h) in headers.iter().enumerate() {
        let want = format!("{}/{} count=", i + 1, headers.len());
        assert!(h.starts_with(&want), "header {i} is `{h}`, not `{want}…`");
        let count: usize = h
            .split_whitespace()
            .nth(1)
            .and_then(|c| c.strip_prefix("count="))
            .and_then(|c| c.parse().ok())
            .expect("a count");
        counted += count;
    }
    assert_eq!(counted, 20, "every entry is on exactly one page: {body}");
}

/// A destination that will not open, and a listing that is denied, both emit
/// NO body — and say which code it was.
#[test]
fn a_walk_that_never_started_emits_no_body() {
    // The handle will not build.
    let run = drive_sync(
        sync_request(),
        &FakeWiring::default().role_fails(
            DestinationRole::ArchiveRead,
            CheckCode::InvalidCredentials,
            "the archiveRead credential is not valid",
        ),
    );
    assert_eq!(run.code, ExitCode::Ok);
    let relay = run.relay.as_ref().expect("the relay decodes");
    assert!(
        relay.stream(Stream::Details).is_none(),
        "a body that parses is a body the controller PUBLISHES, and there is nothing here to \
         publish"
    );
    let row = run.row(CheckId::DestinationArchiveListable);
    assert_eq!(row.state, CheckState::NotReady);
    assert_eq!(row.code, CheckCode::InvalidCredentials);
    assert!(!row.remedy.is_empty());

    // The FIRST listing is denied.
    let denied =
        FakeObjects::new().failing_list(Fault::Io(s3_refusal("403 Forbidden", "AccessDenied")));
    let run = drive_sync(
        sync_request(),
        &FakeWiring::default().with_role(DestinationRole::ArchiveRead, denied),
    );
    assert!(run
        .relay
        .as_ref()
        .expect("the relay decodes")
        .stream(Stream::Details)
        .is_none());
    assert_eq!(
        run.row(CheckId::DestinationArchiveListable).code,
        CheckCode::AccessDenied
    );
}

/// A `Full` rescan resumes strictly after its cursor and reports where it got
/// to.
#[test]
fn a_full_rescan_resumes_after_its_cursor() {
    let sidecar = claimed_sidecar(CATALOG_CLAIMED_KEY_ID);
    let mut objects = FakeObjects::new();
    let mut record_keys: Vec<String> = Vec::new();
    for i in 0..4 {
        let f = catalog_fixture(
            &catalog_receipt(
                &format!("set-{i}"),
                "run-a",
                &format!("2026-09-16T0{i}:00:00Z"),
            ),
            "s3://lw-archive/kafka-backups",
            &sidecar,
            CATALOG_CLAIMED_KEY_ID,
        );
        record_keys.push(f.record_key.clone());
        objects = place(objects, &f);
    }
    record_keys.sort();

    let full = logweir_core::check_contract::CatalogSyncRequest {
        mode: logweir_core::check_contract::CatalogSyncMode::Full,
        ..sync_request()
    };
    let run = drive_sync(
        full.clone(),
        &FakeWiring::default().with_role(DestinationRole::ArchiveRead, objects.clone()),
    );
    let body = body_of(&run);
    let counts = summary_of(&body, "catalog-counts=");
    assert_eq!(counts["total"], 4);
    assert_eq!(
        counts["byDay"],
        serde_json::json!([{"day": "2026-09-16", "points": 4}]),
        "A RESCAN'S KEYS CARRY NO INSTANT, so its histogram is built from the records it read \
         rather than from the key names an Index walk dates for free: {body}"
    );
    let cursor = summary_of(&body, "catalog-cursor=");
    assert_eq!(cursor["complete"], true);
    assert!(
        cursor.get("rescanStartAfter").is_none(),
        "A COMPLETE RESCAN REPORTS NO CURSOR: resuming past the whole archive would publish an \
         empty view of a full one. {cursor}"
    );

    // Resumed after the second record, only the tail is walked.
    let resumed = logweir_core::check_contract::CatalogSyncRequest {
        rescan_start_after: Some(record_keys[1].clone()),
        ..full
    };
    let run = drive_sync(
        resumed,
        &FakeWiring::default().with_role(DestinationRole::ArchiveRead, objects),
    );
    let body = body_of(&run);
    assert_eq!(
        summary_of(&body, "catalog-counts=")["total"],
        2,
        "the cursor is EXCLUSIVE: {body}"
    );
}

/// The plan's `indexShard` never moves the window: the walk starts at today
/// and ends at the archive's own oldest day, whatever the cursor says.
///
/// It used to be CONSUMED as a floor derived from the previous reach, which is
/// review finding F3's ratchet. It is reported and not consumed now, and this
/// row is what says so.
#[test]
fn the_index_cursor_bounds_the_walk_and_never_moves_the_window() {
    let (objects, newer, older) = two_point_objects();
    let request = logweir_core::check_contract::CatalogSyncRequest {
        index_shard: Some("2026-09-16".to_string()),
        ..sync_request()
    };
    let run = drive_sync(
        request,
        &FakeWiring::default().with_role(DestinationRole::ArchiveRead, objects),
    );
    let body = body_of(&run);
    let entries = entries_of(&body);
    assert_eq!(
        entries[0]["pointId"], newer.point.point_id,
        "the newest point is still first: {body}"
    );
    assert_eq!(
        entries.len(),
        2,
        "yesterday's point is still seen, because the floor is the archive's oldest day and \
         not the day the cursor names: {body}"
    );
    assert_eq!(entries[1]["pointId"], older.point.point_id);
    let cursor = summary_of(&body, "catalog-cursor=");
    assert_eq!(
        cursor["indexShard"], "2026-09-15",
        "a cursor naming TODAY does not shrink the range: the floor is the archive's, not the \
         plan's: {body}"
    );
    assert_eq!(cursor["complete"], true);
}

/// A credential planted in every adopter-controlled field of a record is
/// redacted, and the three fields the controller binds on survive intact.
#[test]
fn a_credential_planted_in_a_record_is_redacted_and_the_binding_survives() {
    let (objects, newer, _) = two_point_objects();
    let mut doc: serde_json::Value =
        serde_json::from_slice(&newer.record_bytes).expect("the record is JSON");
    doc["archive"]["location_id"] = serde_json::json!(PLANTED_USERINFO);
    doc["run_id"] = serde_json::json!(PLANTED_KEY_ID);
    doc["backup_id"] = serde_json::json!(PLANTED_SECRET);
    let objects = objects.with_object(&newer.record_key, &serde_json::to_vec(&doc).expect("JSON"));
    let run = drive_sync(
        sync_request(),
        &FakeWiring::default().with_role(DestinationRole::ArchiveRead, objects),
    );
    let everything = run.everything();
    for planted in [
        PLANTED_KEY_ID,
        "hunter2",
        "ZZfakefakefakefakefakefakefakefake01",
    ] {
        assert!(
            !everything.contains(planted),
            "`{planted}` survived into the relay: {everything}"
        );
    }
    // ...and the long-run rule did NOT eat the binding.
    let entries = entries_of(&body_of(&run));
    let bound = entries
        .iter()
        .find(|e| e["receiptSha256"] == newer.point.receipt.sha256.as_str())
        .expect("the receipt digest is emitted verbatim");
    assert_eq!(bound["signerKeyId"], CATALOG_CLAIMED_KEY_ID);
}

/// The one redaction rule an archive reference must not pass is named once.
#[test]
fn the_long_run_rule_is_named_once() {
    assert_eq!(
        logweir::check::kinds::catalog_sync::LONG_RUN_RULE,
        check::LONG_RUN_RULE,
        "two names for one rule is how one of them stops being applied"
    );
    assert_eq!(
        logweir_core::check_contract::redaction_rules()
            .iter()
            .filter(|r| r.name == check::LONG_RUN_RULE)
            .count(),
        1
    );
}

// ---------------------------------------------------------------------------
// Fix round 1 — the review's F1..F5
// ---------------------------------------------------------------------------

/// Seed `n` points on `n` consecutive days ending today, newest first.
fn many_points(n: usize) -> (FakeObjects, Vec<String>) {
    let sidecar = claimed_sidecar(CATALOG_CLAIMED_KEY_ID);
    let mut objects = FakeObjects::new();
    let mut ids = Vec::new();
    for i in 0..n {
        let day = now().date_naive() - chrono::Duration::days(i as i64);
        let f = catalog_fixture(
            &catalog_receipt(
                &format!("set-{i:03}"),
                "run-a",
                &format!("{}T03:00:00Z", day.format("%Y-%m-%d")),
            ),
            "s3://lw-archive/kafka-backups",
            &sidecar,
            CATALOG_CLAIMED_KEY_ID,
        );
        ids.push(f.point.point_id.clone());
        objects = place(objects, &f);
    }
    (objects, ids)
}

/// **F1.** A `Full` rescan EXAMINES everything it counts. `viewLimit` bounds the
/// lines the body carries and nothing else.
///
/// It used to bound examination: the walk stopped calling `examine` at
/// `viewLimit` while it kept counting, then reported `complete: true` with no
/// cursor. On a 20 000-point archive with the default `viewLimit: 2000` the
/// other 18 000 points were never manifest-checked, and never would be on any
/// cadence, because a completed walk leaves nothing to resume from.
#[test]
fn a_full_rescan_examines_every_point_it_counts() {
    let (objects, _) = many_points(9);
    let request = logweir_core::check_contract::CatalogSyncRequest {
        mode: logweir_core::check_contract::CatalogSyncMode::Full,
        view_limit: 3,
        ..sync_request()
    };
    let run = drive_sync(
        request,
        &FakeWiring::default().with_role(DestinationRole::ArchiveRead, objects),
    );
    let body = body_of(&run);
    assert_eq!(entries_of(&body).len(), 3, "the LINES are bounded: {body}");
    let counts = summary_of(&body, "catalog-counts=");
    assert_eq!(counts["total"], 9);
    assert_eq!(
        counts["available"], 9,
        "EVERY counted point was manifest-checked, not just the three the view \
         carries: {body}"
    );
    let cursor = summary_of(&body, "catalog-cursor=");
    assert_eq!(cursor["complete"], true);
    assert!(cursor.get("rescanStartAfter").is_none());
    assert_eq!(
        run.row(CheckId::DestinationArchiveListable).facts["catalogExamined"],
        "9"
    );
}

/// **F2.** The availability buckets sum to `total` on EVERY walk — over a
/// fixture larger than one page and larger than the view limit, in both modes.
///
/// §7d states the invariant; it used to be false, because a walk that reached
/// its floor with more than `viewLimit` points reported `complete: true` with
/// buckets covering only `viewLimit`. An operator read "no conflicts in 50 000
/// points" from a walk that examined 2 000.
#[test]
fn the_buckets_sum_to_total_on_every_walk() {
    // Eleven points over eleven days, one of them broken, with a view limit of
    // two and a page target far below the set — so neither the window nor the
    // page can be what the buckets are measuring.
    let (objects, ids) = many_points(11);
    let broken = logweir::catalog::record::record_key(&ids[5]);
    let objects = objects.failing_key(&broken, Fault::NotFound);

    for mode in [
        logweir_core::check_contract::CatalogSyncMode::Index,
        logweir_core::check_contract::CatalogSyncMode::Full,
    ] {
        let request = logweir_core::check_contract::CatalogSyncRequest {
            mode,
            view_limit: 2,
            ..sync_request()
        };
        let run = drive_sync(
            request,
            &FakeWiring::default().with_role(DestinationRole::ArchiveRead, objects.clone()),
        );
        let body = body_of(&run);
        let counts = summary_of(&body, "catalog-counts=");
        let total = counts["total"].as_i64().expect("a total");
        let summed: i64 = [
            "available",
            "missing",
            "unreadable",
            "deleted",
            "conflict",
            "unsupportedFormat",
            "partial",
        ]
        .iter()
        .map(|k| counts[*k].as_i64().unwrap_or(0))
        .sum();
        assert_eq!(total, 11, "{mode:?}: {body}");
        assert_eq!(
            summed, total,
            "{mode:?}: the buckets must sum to `total` BY CONSTRUCTION — nothing is counted \
             that was not examined: {body}"
        );
        assert_eq!(counts["missing"], 1, "{mode:?}: {body}");
        assert_eq!(entries_of(&body).len(), 2, "{mode:?}: the window is two");
        assert_eq!(summary_of(&body, "catalog-cursor=")["complete"], true);
    }
}

/// **F3.** The `Index` floor is the ARCHIVE's own oldest day, so the reported
/// cursor is stable and `complete: true` is reachable at any archive age.
///
/// The floor used to be the previous walk's reported cursor minus a day, so it
/// receded one day per sync for ever; past the shard cap the loop could never
/// reach it, `complete` was never true again, and every sync published
/// `ScanIncomplete` — "the object budget ran out" — on a healthy archive whose
/// budget was never touched.
#[test]
fn the_index_floor_is_the_archives_own_oldest_day_and_does_not_ratchet() {
    // An archive whose oldest point is a THOUSAND days old — past the cap the
    // old walk could ever reach.
    let sidecar = claimed_sidecar(CATALOG_CLAIMED_KEY_ID);
    let mut objects = FakeObjects::new();
    let mut days = Vec::new();
    for back in [0i64, 500, 1_000] {
        let day = now().date_naive() - chrono::Duration::days(back);
        days.push(day.format("%Y-%m-%d").to_string());
        let f = catalog_fixture(
            &catalog_receipt(
                &format!("set-{back}"),
                "run-a",
                &format!("{}T03:00:00Z", day.format("%Y-%m-%d")),
            ),
            "s3://lw-archive/kafka-backups",
            &sidecar,
            CATALOG_CLAIMED_KEY_ID,
        );
        objects = place(objects, &f);
    }
    let oldest = days.last().expect("three days").clone();

    // ONE sync reaches the oldest day and completes.
    let first = drive_sync(
        sync_request(),
        &FakeWiring::default().with_role(DestinationRole::ArchiveRead, objects.clone()),
    );
    let first_body = body_of(&first);
    let first_cursor = summary_of(&first_body, "catalog-cursor=");
    assert_eq!(
        first_cursor["complete"], true,
        "a 1000-day-old archive completes in ONE sync: {first_body}"
    );
    assert_eq!(first_cursor["indexShard"], oldest);
    assert_eq!(summary_of(&first_body, "catalog-counts=")["total"], 3);

    // AND THE NEXT SYNC, fed that cursor, reports the SAME day. No ratchet.
    let second = drive_sync(
        logweir_core::check_contract::CatalogSyncRequest {
            index_shard: Some(first_cursor["indexShard"].as_str().unwrap().to_string()),
            ..sync_request()
        },
        &FakeWiring::default().with_role(DestinationRole::ArchiveRead, objects.clone()),
    );
    let second_body = body_of(&second);
    let second_cursor = summary_of(&second_body, "catalog-cursor=");
    assert_eq!(
        second_cursor["indexShard"], first_cursor["indexShard"],
        "the reported floor is a fact about the ARCHIVE, not a token fed back — a cursor \
         derived from the previous reach recedes a day per sync for ever: {second_body}"
    );
    assert_eq!(second_cursor["complete"], true);
    assert_eq!(summary_of(&second_body, "catalog-counts=")["total"], 3);

    // An EMPTY index is a complete walk of nothing, not a failure.
    let empty = drive_sync(
        sync_request(),
        &FakeWiring::default().with_role(DestinationRole::ArchiveRead, FakeObjects::new()),
    );
    let empty_body = body_of(&empty);
    assert_eq!(summary_of(&empty_body, "catalog-counts=")["total"], 0);
    assert_eq!(summary_of(&empty_body, "catalog-cursor=")["complete"], true);
}

/// **F4.** The receipt's three outcomes are three different facts, and the
/// record's and the sidecar's too. A key-scoped fault is what makes each one
/// reachable on its own.
///
/// The reviewer's mutant folded the receipt `get`'s `Err(_) => Unreadable` into
/// `Missing` and the whole suite stayed green: the one row that installed a
/// fault failed EVERY `get`, so the record read failed first and the receipt
/// branch was never reached with a failure at all.
#[test]
fn a_receipt_that_cannot_be_read_is_never_missing() {
    let sidecar = claimed_sidecar(CATALOG_CLAIMED_KEY_ID);
    let f = catalog_fixture(
        &catalog_receipt("set-a", "run-a", "2026-09-16T03:00:00Z"),
        "s3://lw-archive/kafka-backups",
        &sidecar,
        CATALOG_CLAIMED_KEY_ID,
    );
    let denied = Fault::Io(s3_refusal("403 Forbidden", "AccessDenied"));

    // The last column is the cause (FX-33): the document the examination
    // stopped at and why, or `None` for a point that is `Available`. Only a
    // read that did not answer has a remedy naming the grant.
    type Cause<'a> = Option<(&'a str, &'a str)>;
    type Case<'a> = (&'a str, String, Fault, &'a str, &'a str, Cause<'a>);
    let cases: Vec<Case<'_>> = vec![
        (
            "the record is absent",
            f.record_key.clone(),
            Fault::NotFound,
            "Missing",
            "notAttempted",
            Some(("record", "notFound")),
        ),
        (
            "the record is denied",
            f.record_key.clone(),
            denied.clone(),
            "Unreadable",
            "notAttempted",
            Some(("record", "readFailed")),
        ),
        (
            "the receipt is absent",
            f.receipt_key.clone(),
            Fault::NotFound,
            "Missing",
            "notAttempted",
            Some(("receipt", "notFound")),
        ),
        (
            "the receipt is denied",
            f.receipt_key.clone(),
            denied.clone(),
            "Unreadable",
            "notAttempted",
            Some(("receipt", "readFailed")),
        ),
        (
            "the sidecar is absent",
            f.sidecar_key.clone(),
            Fault::NotFound,
            "Available",
            "noEvidence",
            None,
        ),
        (
            "the sidecar is denied",
            f.sidecar_key.clone(),
            denied.clone(),
            "Available",
            "notAttempted",
            None,
        ),
        (
            "the manifest is absent",
            f.manifest_key.clone(),
            Fault::NotFound,
            "Missing",
            "notAttempted",
            Some(("manifest", "notFound")),
        ),
        (
            "the manifest is denied",
            f.manifest_key.clone(),
            denied,
            "Unreadable",
            "notAttempted",
            Some(("manifest", "readFailed")),
        ),
    ];
    for (what, key, fault, availability, signature, cause) in cases {
        let objects = place(FakeObjects::new(), &f).failing_key(&key, fault);
        let run = drive_sync(
            sync_request(),
            &FakeWiring::default().with_role(DestinationRole::ArchiveRead, objects),
        );
        let body = body_of(&run);
        let counts = summary_of(&body, "catalog-counts=");
        assert_eq!(counts["total"], 1, "{what}: {body}");
        // FX-33: EVERY counted point is listed — a point whose RECORD could
        // not be read too, which used to be counted and listed by no entry.
        let entries = entries_of(&body);
        assert_eq!(entries.len(), 1, "{what}: the point is not listed: {body}");
        let entry = &entries[0];
        assert_eq!(
            entry["pointId"],
            f.point.point_id.as_str(),
            "{what}: {entry}"
        );
        assert_eq!(entry["availability"], availability, "{what}: {entry}");
        assert_eq!(entry["signature"], signature, "{what}: {entry}");
        if let Some((_, reason)) = cause {
            assert_eq!(
                entry["remedy"]
                    .as_str()
                    .is_some_and(|r| r.contains("archiveRead grant")),
                reason == "readFailed",
                "{what}: {entry}"
            );
        }
        let bucket = match availability {
            "Missing" => "missing",
            "Unreadable" => "unreadable",
            _ => "available",
        };
        assert_eq!(counts[bucket], 1, "{what}: {body}");
        if availability == "Unreadable" {
            assert_eq!(
                counts["missing"], 0,
                "{what}: a denial is NEVER reported as absence — it is the distinction the \
                 seven availability states exist for: {body}"
            );
        }
    }
}

/// **F4, the size half (question F12), and FX-33's.** A record larger than
/// the bound a record is read under is `Unreadable`, is not parsed — and is
/// LISTED, with its size against the bound.
///
/// Before FX-33 the outcome was the count alone: the point had no entry.
///
/// KILLS: the record read uncapped or under a larger cap; `build_entry`
/// dropping a point with no parsed record; the size reported with the
/// grant remedy.
#[test]
fn an_oversized_catalog_document_is_unreadable_and_is_not_parsed() {
    use logweir_engine_oso::storage::caps;
    let sidecar = claimed_sidecar(CATALOG_CLAIMED_KEY_ID);
    let f = catalog_fixture(
        &catalog_receipt("set-a", "run-a", "2026-09-16T03:00:00Z"),
        "s3://lw-archive/kafka-backups",
        &sidecar,
        CATALOG_CLAIMED_KEY_ID,
    );
    // A RECORD THAT WOULD OTHERWISE PARSE. Unknown fields are ignored inside
    // major 1 (D3 §5.2 rule 2), so padding one with a long string leaves a
    // document `read_record` accepts — which is what makes the ceiling, and not
    // the parser, the thing under test. A block of spaces would fail to parse
    // either way and the mutant that deletes the ceiling would survive.
    let cap = usize::try_from(caps::CATALOG_RECORD).expect("fits");
    let mut doc: serde_json::Value =
        serde_json::from_slice(&f.record_bytes).expect("the record is JSON");
    doc["e2e_padding"] = serde_json::json!("x".repeat(cap));
    let huge = serde_json::to_vec(&doc).expect("JSON");
    assert!(huge.len() > cap);
    // The CONTROL: the same document under the ceiling reads normally.
    let control = place(FakeObjects::new(), &f);
    let run = drive_sync(
        sync_request(),
        &FakeWiring::default().with_role(DestinationRole::ArchiveRead, control),
    );
    assert_eq!(
        summary_of(&body_of(&run), "catalog-counts=")["available"],
        1,
        "the control must read, or the row proves nothing"
    );
    let objects = place(FakeObjects::new(), &f).with_object(&f.record_key, &huge);
    let run = drive_sync(
        sync_request(),
        &FakeWiring::default().with_role(DestinationRole::ArchiveRead, objects),
    );
    let body = body_of(&run);
    let counts = summary_of(&body, "catalog-counts=");
    assert_eq!(counts["unreadable"], 1, "{body}");
    assert_eq!(counts["unreadableOverReadCap"], 1, "{body}");
    let entries = entries_of(&body);
    assert_eq!(
        entries.len(),
        1,
        "the oversized point is not listed: {body}"
    );
    let entry = &entries[0];
    assert_eq!(entry["pointId"], f.point.point_id.as_str(), "{entry}");
    assert_eq!(entry["availability"], "Unreadable", "{entry}");
    let remedy = entry["remedy"].as_str().expect("a remedy");
    assert!(
        remedy.contains(&format!("{} bytes", huge.len()))
            && remedy.contains(&format!("{}-byte bound", caps::CATALOG_RECORD)),
        "the remedy states the size against the bound: {remedy}"
    );
    assert!(!remedy.contains("grant"), "a size names no grant: {remedy}");
}

/// **FX-31: the catalog walk READS under its caps**, not only measures after:
/// the record under `caps::CATALOG_RECORD`, the receipt under
/// `caps::CATALOG_RECEIPT` (FX-33: the topic budget's two bounds, the second
/// the controller's own), the sidecar under `caps::SIDECAR`, the manifest
/// under `caps::MANIFEST`. The row above holds the outcome (`Unreadable`);
/// this one holds the read.
///
/// KILLS: "the walk reads uncapped" (any of the four caps); a cap read as
/// `u64::MAX`; the receipt read under a cap other than the controller's.
#[test]
fn a_catalog_walk_reads_every_document_under_its_cap() {
    use logweir_engine_oso::storage::caps;
    let sidecar = claimed_sidecar(CATALOG_CLAIMED_KEY_ID);
    let f = catalog_fixture(
        &catalog_receipt("set-a", "run-a", "2026-09-16T03:00:00Z"),
        "s3://lw-archive/kafka-backups",
        &sidecar,
        CATALOG_CLAIMED_KEY_ID,
    );
    let objects = place(FakeObjects::new(), &f);
    let run = drive_sync(
        sync_request(),
        &FakeWiring::default().with_role(DestinationRole::ArchiveRead, objects.clone()),
    );
    assert_eq!(
        summary_of(&body_of(&run), "catalog-counts=")["available"],
        1,
        "the point reads"
    );
    let caps_read = objects.read_caps();
    let cap_of = |key: &str| {
        caps_read
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, c)| *c)
            .unwrap_or_else(|| panic!("{key} was not read: {caps_read:?}"))
    };
    assert_eq!(cap_of(&f.record_key), caps::CATALOG_RECORD);
    assert_eq!(cap_of(&f.receipt_key), caps::CATALOG_RECEIPT);
    assert_eq!(cap_of(&f.receipt_key), caps::CONTROLLER_RECEIPT);
    assert_eq!(cap_of(&f.sidecar_key), caps::SIDECAR);
    assert_eq!(cap_of(&f.manifest_key), caps::MANIFEST);
    // FX-33: each of the five kinds has ITS cap, and no two reads of one walk
    // share a number by accident — a record is never read under the
    // manifest's 256 MiB, nor a sidecar under a receipt's 5 MB.
    const _: () = assert!(
        caps::SIDECAR < caps::CATALOG_RECEIPT
            && caps::CATALOG_RECEIPT < caps::CATALOG_RECORD
            && caps::CATALOG_RECORD < caps::MANIFEST
    );
    for (key, cap) in &caps_read {
        assert!(
            *cap <= caps::MANIFEST,
            "every catalog read is bounded: {key} at {cap}"
        );
    }
}

/// **F5.** A walk stopped by the object budget says so on the WALK, and no
/// point is reported `Unreadable` for it.
///
/// A point abandoned between its record and its receipt used to be
/// `Unreadable`, whose fixed remedy names the `archiveRead` grant and the
/// network — and which the controller takes BEFORE the `!complete` branch and
/// publishes as `PartialScan`: "a permission or transport failure". A
/// budget-bounded walk is the designed, normal state the cursor exists for.
#[test]
fn a_budget_stop_is_the_walks_outcome_and_never_a_points() {
    // FOUR POINTS ON ONE DAY, so the budget runs out BETWEEN POINTS inside a
    // shard rather than between shards. A point-per-day fixture only ever
    // exercises the shard-level guard, which is how two mutants on the
    // point-level one survived a campaign.
    let sidecar = claimed_sidecar(CATALOG_CLAIMED_KEY_ID);
    let mut objects = FakeObjects::new();
    for i in 0..4 {
        let f = catalog_fixture(
            &catalog_receipt(
                &format!("set-{i}"),
                "run-a",
                &format!("2026-09-16T0{i}:00:00Z"),
            ),
            "s3://lw-archive/kafka-backups",
            &sidecar,
            CATALOG_CLAIMED_KEY_ID,
        );
        objects = place(objects, &f);
    }
    // NINE OBJECTS: the floor listing, one shard listing and exactly ONE whole
    // point, with three left over — fewer than a point costs. The leftover is
    // the point of the number: a guard that asked `objects < budget` instead of
    // `objects + OBJECTS_PER_POINT <= budget` would begin a second point it
    // cannot pay for, overspend the budget and count it. A budget that happens
    // to be a whole number of points cannot tell the two apart, and that is how
    // this mutant survived its first campaign.
    let run = drive_sync(
        logweir_core::check_contract::CatalogSyncRequest {
            max_objects_per_run: 9,
            ..sync_request()
        },
        &FakeWiring::default().with_role(DestinationRole::ArchiveRead, objects.clone()),
    );
    let body = body_of(&run);
    let counts = summary_of(&body, "catalog-counts=");
    let spent: i64 = run.row(CheckId::DestinationArchiveListable).facts["catalogObjectsRead"]
        .parse()
        .expect("an object count");
    assert!(
        spent <= 9,
        "A WALK NEVER SPENDS MORE OBJECTS THAN ITS BUDGET: {spent} of 9. {body}"
    );
    assert_eq!(
        counts["unreadable"], 0,
        "a point the budget never reached is UNEXAMINED, not unreadable: {body}"
    );
    assert_eq!(
        counts["missing"], 0,
        "and it is certainly not absent: {body}"
    );
    assert_eq!(
        counts["total"], 1,
        "ONE whole point fits nine objects and the second was not begun: {body}"
    );
    assert_eq!(
        counts["available"], 1,
        "everything counted was examined — the invariant holds on a stopped walk too: {body}"
    );
    let cursor = summary_of(&body, "catalog-cursor=");
    assert_eq!(cursor["complete"], false);
    assert!(
        cursor["indexShard"].is_string(),
        "and it says where it stopped"
    );
    assert_eq!(
        run.row(CheckId::DestinationArchiveListable).facts["catalogStoppedFor"],
        "objectBudget",
        "the reason is named on the row, because `complete: false` alone is what the \
         controller renders as an object-budget message whatever stopped the walk"
    );

    // THE SAME, IN `Full`: a rescan that stops short says so and carries its
    // cursor. `complete` is derived in ONE place for both modes.
    // TEN: one page listing and two whole points, with one object left over.
    let run = drive_sync(
        logweir_core::check_contract::CatalogSyncRequest {
            mode: logweir_core::check_contract::CatalogSyncMode::Full,
            max_objects_per_run: 10,
            ..sync_request()
        },
        &FakeWiring::default().with_role(DestinationRole::ArchiveRead, objects.clone()),
    );
    let body = body_of(&run);
    let counts = summary_of(&body, "catalog-counts=");
    let spent: i64 = run.row(CheckId::DestinationArchiveListable).facts["catalogObjectsRead"]
        .parse()
        .expect("an object count");
    assert!(spent <= 10, "{spent} of 10 objects. {body}");
    assert_eq!(counts["unreadable"], 0, "{body}");
    assert_eq!(
        counts["total"], 2,
        "two whole points fit ten objects and the third was not begun: {body}"
    );
    let cursor = summary_of(&body, "catalog-cursor=");
    assert_eq!(cursor["complete"], false, "{body}");
    assert!(
        cursor["rescanStartAfter"].is_string(),
        "AN INCOMPLETE RESCAN ALWAYS CARRIES ITS CURSOR, whatever stopped it: {body}"
    );
    assert_eq!(
        run.row(CheckId::DestinationArchiveListable).facts["catalogStoppedFor"],
        "objectBudget"
    );

    // AND A SHARD THAT WILL NOT LIST IS NOT A BUDGET. It gets its own reason,
    // because the controller renders `complete: false` as an object-budget
    // message whatever stopped the walk.
    let older = catalog_fixture(
        &catalog_receipt("set-old", "run-a", "2026-09-14T03:00:00Z"),
        "s3://lw-archive/kafka-backups",
        &sidecar,
        CATALOG_CLAIMED_KEY_ID,
    );
    let shard = format!("{}2026/09/15/", logweir::catalog::record::LOG_PREFIX);
    let objects = place(objects, &older).failing_list_prefix(&shard, Fault::NotFound);
    let run = drive_sync(
        sync_request(),
        &FakeWiring::default().with_role(DestinationRole::ArchiveRead, objects),
    );
    let body = body_of(&run);
    assert_eq!(
        summary_of(&body, "catalog-cursor=")["complete"],
        false,
        "{body}"
    );
    assert_eq!(
        run.row(CheckId::DestinationArchiveListable).facts["catalogStoppedFor"],
        "shardUnreadable",
        "a day this walk could not see is not the object budget: {body}"
    );
}

/// **F11.** A credential planted in a field that is not a digest is redacted by
/// the WHOLE rule set, and the three digests still come through.
#[test]
fn a_long_credential_in_a_non_digest_field_is_redacted() {
    let sidecar = claimed_sidecar(CATALOG_CLAIMED_KEY_ID);
    let f = catalog_fixture(
        &catalog_receipt("set-a", "run-a", "2026-09-16T03:00:00Z"),
        "s3://lw-archive/kafka-backups",
        &sidecar,
        CATALOG_CLAIMED_KEY_ID,
    );
    // Forty-four characters of base64 — over the long-run rule's threshold, and
    // exactly the shape the exemption used to let through.
    const PLANTED_LONG: &str = "c2VjcmV0LWNyZWRlbnRpYWwtbm9ib2R5LXNob3VsZC1zZWU";
    assert!(PLANTED_LONG.len() >= 40);
    let mut doc: serde_json::Value =
        serde_json::from_slice(&f.record_bytes).expect("the record is JSON");
    doc["backup_id"] = serde_json::json!(PLANTED_LONG);
    let objects = place(FakeObjects::new(), &f)
        .with_object(&f.record_key, &serde_json::to_vec(&doc).unwrap());
    let run = drive_sync(
        sync_request(),
        &FakeWiring::default().with_role(DestinationRole::ArchiveRead, objects),
    );
    let everything = run.everything();
    assert!(
        !everything.contains(PLANTED_LONG),
        "a 44-character run in `backupId` reached the relay verbatim: {everything}"
    );
    let entries = entries_of(&body_of(&run));
    assert_eq!(entries[0]["backupId"], "[redacted]");
    assert_eq!(
        entries[0]["receiptSha256"], f.point.receipt.sha256,
        "and the binding is untouched, which is the whole reason the exemption exists"
    );
    assert_eq!(entries[0]["signerKeyId"], CATALOG_CLAIMED_KEY_ID);
    assert_eq!(
        entries[0]["receiptKey"], f.point.receipt.key,
        "an object key survives, because its long-run clause is per path segment"
    );

    // AND THE KEY THAT PROVES THE SEGMENT RULE IS LOAD-BEARING. Every segment
    // of this one is short; the whole string is a 57-character run, because
    // `/` and `-` are both in the base64 alphabet. The WHOLE `redact` would
    // return `[redacted]` and leave the controller a key nobody can fetch.
    let long_key = catalog_fixture(
        &catalog_receipt(
            "set-abcdefghijklmnop",
            "run-abcdefghijklmnop",
            "2026-09-16T04:00:00Z",
        ),
        "s3://lw-archive/kafka-backups",
        &sidecar,
        CATALOG_CLAIMED_KEY_ID,
    );
    let run_of = |k: &str| {
        k.split(|c: char| !(c.is_ascii_alphanumeric() || "+/=_-".contains(c)))
            .map(str::len)
            .max()
            .unwrap_or(0)
    };
    assert!(
        run_of(&long_key.point.receipt.key) >= 40,
        "the fixture key must reach the long-run threshold or this proves nothing: {}",
        long_key.point.receipt.key
    );
    assert!(
        long_key
            .point
            .receipt
            .key
            .split('/')
            .all(|seg| run_of(seg) < 40),
        "and every SEGMENT must stay under it"
    );
    let run = drive_sync(
        sync_request(),
        &FakeWiring::default().with_role(
            DestinationRole::ArchiveRead,
            place(FakeObjects::new(), &long_key),
        ),
    );
    let entries = entries_of(&body_of(&run));
    assert_eq!(
        entries[0]["receiptKey"], long_key.point.receipt.key,
        "a 57-character key run survives because the clause is per segment"
    );
    assert_eq!(
        entries[0]["manifestKey"],
        long_key.point.archive.manifest_key
    );
}

/// **CATALOG-RECEIPTKEY-REDACTED, end to end.** The `receiptKey` the sync
/// publishes is the one `logweir backup run` wrote, character for character,
/// for a run id as the product really mints it.
///
/// REGRESSION REASON. Every fixture above spells the run id `run-a` — a
/// lower-case public NAME, which never reached the free-component budget — so
/// the whole suite was green while every LIVE point published
/// `receiptKey: "[redacted].receipt.json"`. A real run id is a 26-character
/// ULID (`logweir_core::ids::format_run_id`), two over the budget of 24, and
/// it is the ONE free component of the key. D3 §5.5 step 4 builds
/// `source.point {point_id, receipt_key, receipt_sha256, manifest_sha256}`
/// from this line, so the key that arrived redacted was a plan binding the
/// runner refuses with exit 3 `PointBindingMismatch`.
///
/// The second half is the negative control the first half needs: a run id one
/// character LONGER is not a ULID, is over the budget, and is still redacted.
/// A fix that raised the budget instead of exempting the shape passes the
/// first half and fails the second.
#[test]
fn the_receipt_key_a_restore_binds_to_survives_a_sync_for_a_real_run_id() {
    const SET: &str = "3f0ada8f-1a2b-4c3d-9e8f-0123456789ab";
    const RUN: &str = "01M2VKCST7EF12EW5T2Y7SJ86Q";
    let sidecar = claimed_sidecar(CATALOG_CLAIMED_KEY_ID);

    let entry_for = |run_id: &str| -> serde_json::Value {
        let f = catalog_fixture(
            &catalog_receipt(SET, run_id, "2026-09-16T03:00:00Z"),
            "s3://lw-archive/kafka-backups",
            &sidecar,
            CATALOG_CLAIMED_KEY_ID,
        );
        assert_eq!(
            f.point.receipt.key,
            format!("logweir/backups/{SET}/{run_id}.receipt.json"),
            "the fixture is not the key `backup run` writes"
        );
        let run = drive_sync(
            sync_request(),
            &FakeWiring::default()
                .with_role(DestinationRole::ArchiveRead, place(FakeObjects::new(), &f)),
        );
        let mut entry = entries_of(&body_of(&run)).remove(0);
        entry["__key"] = serde_json::json!(f.point.receipt.key);
        entry
    };

    // --- the run id the minter really produces -----------------------------
    assert_eq!(
        logweir_core::ids::format_run_id(1_789_780_191_192, 0x0123_4567_89ab_cdef_0123).len(),
        26,
        "a run id is 26 characters, which is what makes this row the live one"
    );
    let entry = entry_for(RUN);
    assert_eq!(
        entry["receiptKey"], entry["__key"],
        "the plan binding was not published whole"
    );
    assert!(
        entry["receiptKey"]
            .as_str()
            .is_some_and(|k| k.contains(RUN) && k.ends_with(".receipt.json")),
        "the published key must carry the run id: {}",
        entry["receiptKey"]
    );
    assert!(
        !entry["receiptKey"]
            .as_str()
            .unwrap()
            .contains(logweir_core::check_contract::REDACTED),
        "the live symptom is back: {}",
        entry["receiptKey"]
    );
    assert_eq!(entry["runId"], RUN, "and the run id itself travels");
    assert_eq!(
        entry["manifestKey"],
        format!("kafka-backups/{SET}/manifest.json")
    );

    // --- the negative control: one character more is not a ULID ------------
    let over = format!("{RUN}X");
    assert_eq!(over.len(), 27);
    let entry = entry_for(&over);
    assert!(
        !entry["receiptKey"]
            .as_str()
            .unwrap_or_default()
            .contains(over.as_str()),
        "a 27-character free component rode out on the ULID exemption: {}",
        entry["receiptKey"]
    );
    assert_eq!(
        entry["receiptKey"],
        format!("{}.receipt.json", logweir_core::check_contract::REDACTED),
        "…and what it is replaced with is the symptom this defect was reported as"
    );
}

/// **FX-17's class sweep: the resume cursor is a value the NEXT sync decides
/// on**, and it passes `redact_path` (`catalog_sync.rs` `cursor_document`).
/// A redacted cursor would be a listing start-after no key equals. It is a
/// point's RECORD key, `logweir/catalog/v1/points/<pointId>/record.json`,
/// whose every component is a public name (`lwp1-` + 32 hex is 37 characters),
/// so it survives by construction; this row pins that construction for the
/// id range the writer mints, so a longer point id or a new path component
/// fails here and not as a sync that re-walks from the start for ever.
#[test]
fn the_catalog_resume_cursor_is_a_record_key_the_redactor_keeps() {
    for receipt in [b"a".as_slice(), b"b", b"the receipt bytes", &[0xff; 64]] {
        let point_id = logweir::catalog::record::point_id(receipt);
        let key = logweir::catalog::record::record_key(&point_id);
        assert!(key.len() >= 40, "the probe must reach the threshold: {key}");
        assert_eq!(
            logweir::check::redact_path(&key),
            key,
            "the resume cursor was redacted"
        );
    }
    // CONTROL: the same position with a component the redactor withholds is
    // withheld, so the row above is not passing on a redactor that keeps all.
    let forged = "logweir/catalog/v1/points/wJalrXUtnFEMIK7MDENGbPxRfiCYEXAMPLEKEY0/record.json";
    assert!(
        logweir::check::redact_path(forged).contains(logweir_core::check_contract::REDACTED),
        "{}",
        logweir::check::redact_path(forged)
    );
}

/// **FX-17, end to end.** A SCHEDULED run's point is published with its set
/// id, its receipt key and its manifest key whole.
///
/// REGRESSION REASON. The row above, and every catalog fixture in this file,
/// spells the backup set id as a UUID — a manual run's. A scheduled run's set
/// id is `<schedule uid>-<yyyymmdd>-<hhmmss>` (`weirkeeper::slot::
/// backup_id_for_attempt`), 52 characters, which the redactor read as neither a
/// UUID nor a public name. So on the PoC every nightly point reached the
/// console as `backupId: "[redacted]"`, `receiptKey: "[redacted].receipt.json"`
/// and `manifestKey: "[redacted].json"` — 84 of 370 points, the whole first
/// page of the Catalog view — and the console offered none of them, because a
/// plan binding cannot be built from the redactor's output.
///
/// The second half is the negative control: a set id with the same shape but
/// a slot that is not a real instant was never minted, and it is still
/// withheld from all three fields.
#[test]
fn a_scheduled_runs_point_is_published_with_its_set_id_and_keys_whole() {
    /// The PoC's own nightly schedule UID and one of its run ids.
    const SCHEDULE_UID: &str = "89b585c5-5498-48dc-ae32-090809457ec8";
    const RUN: &str = "01M4CKTADX268PREVHJAAYXEMZ";
    let sidecar = claimed_sidecar(CATALOG_CLAIMED_KEY_ID);
    let entry_for = |set: &str| -> (serde_json::Value, CatalogFixture) {
        let f = catalog_fixture(
            // The day every other catalog row here is captured on: the
            // fixture's index walk reads that shard.
            &catalog_receipt(set, RUN, "2026-09-16T03:00:00Z"),
            "s3://kafka-backups/poc",
            &sidecar,
            CATALOG_CLAIMED_KEY_ID,
        );
        let run = drive_sync(
            sync_request(),
            &FakeWiring::default()
                .with_role(DestinationRole::ArchiveRead, place(FakeObjects::new(), &f)),
        );
        let mut entries = entries_of(&body_of(&run));
        assert_eq!(entries.len(), 1, "one receipt, one point");
        (entries.remove(0), f)
    };

    // --- the set ids the schedule controller mints: attempt 0 and a retry ---
    for set in [
        format!("{SCHEDULE_UID}-20260916-030000"),
        format!("{SCHEDULE_UID}-20260916-030000-r2"),
    ] {
        let (entry, f) = entry_for(&set);
        assert_eq!(
            f.point.receipt.key,
            format!("logweir/backups/{set}/{RUN}.receipt.json"),
            "the fixture is not the key `backup run` writes"
        );
        assert_eq!(entry["backupId"], set, "the set id was withheld");
        assert_eq!(
            entry["receiptKey"], f.point.receipt.key,
            "the plan binding was not published whole"
        );
        assert_eq!(
            entry["manifestKey"], f.point.archive.manifest_key,
            "the manifest key was withheld"
        );
        assert_eq!(entry["runId"], RUN);
        let line = serde_json::to_string(&entry).expect("the entry serialises");
        assert!(
            !line.contains(logweir_core::check_contract::REDACTED),
            "the PoC symptom is back: {line}"
        );
    }

    // --- the negative control: the shape, but not a slot anything minted ---
    let forged = format!("{SCHEDULE_UID}-20261309-031700");
    let (entry, _) = entry_for(&forged);
    let line = serde_json::to_string(&entry).expect("the entry serialises");
    assert!(
        !line.contains(&forged),
        "a set id no schedule mints rode out on the scheduled shape: {line}"
    );
    assert_eq!(entry["backupId"], logweir_core::check_contract::REDACTED);
    assert_eq!(
        entry["receiptKey"],
        format!("{}.receipt.json", logweir_core::check_contract::REDACTED)
    );
}

/// **THE CROSS-CRATE GUARD, half one.** Every prefix and every cap this runner
/// writes is the controller's own constant, read out of its source.
#[test]
fn the_grammar_this_runner_writes_is_the_grammar_the_controller_parses() {
    use logweir::check::kinds::catalog_sync as cs;
    let path = repo_root().join("crates/weirkeeper/src/catalog_view.rs");
    let source = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("could not read {}: {e}", path.display()));

    let string_const = |name: &str| -> String {
        let at = source
            .find(&format!("pub const {name}: &str = "))
            .unwrap_or_else(|| panic!("`{name}` is gone from {}", path.display()));
        let rest = &source[at..];
        let open = rest.find('"').expect("an opening quote");
        let close = rest[open + 1..].find('"').expect("a closing quote");
        rest[open + 1..open + 1 + close].to_string()
    };
    let usize_const = |name: &str| -> usize {
        let at = source
            .find(&format!("pub const {name}: usize = "))
            .unwrap_or_else(|| panic!("`{name}` is gone from {}", path.display()));
        let rest = &source[at..];
        let expr = &rest[rest.find('=').expect("an =") + 1..rest.find(';').expect("a ;")];
        // `5 * 1024 * 1024` and `64` are the two shapes the file uses.
        expr.split('*')
            .map(|t| t.trim().parse::<usize>().expect("a literal factor"))
            .product()
    };

    let pairs: [(&str, &str, &str); 6] = [
        (
            "FORMAT_LINE_PREFIX",
            cs::FORMAT_LINE_PREFIX,
            "the version line",
        ),
        ("PAGE_LINE_PREFIX", cs::PAGE_LINE_PREFIX, "a page header"),
        ("ENTRY_LINE_PREFIX", cs::ENTRY_LINE_PREFIX, "an entry"),
        ("COUNTS_LINE_PREFIX", cs::COUNTS_LINE_PREFIX, "the counts"),
        ("CURSOR_LINE_PREFIX", cs::CURSOR_LINE_PREFIX, "the cursor"),
        (
            "SIGNERS_LINE_PREFIX",
            cs::SIGNERS_LINE_PREFIX,
            "the signers",
        ),
    ];
    for (name, ours, what) in pairs {
        assert_eq!(
            string_const(name),
            ours,
            "{what}: the controller parses `{}` and this runner writes `{ours}`",
            string_const(name)
        );
    }
    assert_eq!(usize_const("MAX_BODY_BYTES"), cs::MAX_BODY_BYTES);
    assert_eq!(usize_const("MAX_BODY_SIGNERS"), cs::MAX_BODY_SIGNERS);
    assert_eq!(usize_const("MAX_ENTRY_LOCATIONS"), cs::MAX_ENTRY_LOCATIONS);
    assert_eq!(usize_const("MAX_ENTRY_TOPICS"), cs::MAX_ENTRY_TOPICS);
    assert_eq!(usize_const("MAX_ENTRY_GROUPS"), cs::MAX_ENTRY_GROUPS);
    // PROD-03.0: the controller skips an entry listing more schema ids than
    // the receipt format lists, so the runner never writes more.
    assert_eq!(
        usize_const("MAX_ENTRY_SCHEMA_IDS"),
        logweir_core::schema_dependency::SCHEMA_IDS_LISTED
    );
    assert_eq!(usize_const("MAX_BODY_PAGES"), cs::MAX_BODY_PAGES);
    assert_eq!(usize_const("MAX_HISTOGRAM_DAYS"), cs::MAX_HISTOGRAM_DAYS);
    assert!(
        source.contains("pub const BODY_FORMAT_VERSION: u32 = 1;"),
        "the controller reads grammar version 1 and this runner writes {}",
        cs::BODY_FORMAT_VERSION
    );
}

/// **THE CROSS-CRATE GUARD, half two.** The EXACT body the emitter produces for
/// the fixture below.
///
/// `the_runners_pinned_body_is_one_this_parser_reads` in
/// `crates/weirkeeper/tests/catalog_controller.rs` extracts this literal from
/// this file and runs the controller's own `parse_body` over it. The runner
/// test pins runner ⟷ literal; the controller test pins literal ⟷ parser; and
/// neither crate had to grow a dependency on the other.
const PINNED_SYNC_BODY: &str = r#"catalog-format=1
catalog-page=1/1 count=1 sha256=95250c908736a87d14285e68202b9ab0a1326896a0a4fbaa30bc3f79ad31353d
catalog-entry={"pointId":"lwp1-0e02dc33bf63349ec262a62043d9bd04","backupId":"set-a","runId":"run-a","recoveryPointAtMs":1789527600000,"coveredFromMs":1789524000000,"coveredToMs":1789527600000,"locations":[{"locationId":"s3://lw-archive/kafka-backups","availability":"Available"}],"receiptKey":"logweir/backups/set-a/run-a.receipt.json","receiptSha256":"sha256:0e02dc33bf63349ec262a62043d9bd0441fb867c72aa2d5b4ce8a085b05469db","manifestKey":"kafka-backups/set-a/manifest.json","manifestSha256":"sha256:d5eea23a2f7ca3f36d2a5dbf3ab2532a3de3a797ded388afb816068c2863a152","recordedAt":"2026-09-16T06:00:00Z","formatVersion":"1.1.0","availability":"Available","signature":"notAttempted","signerKeyId":"0f1e2d3c4b5a69788796a5b4c3d2e1f00f1e2d3c4b5a69788796a5b4c3d2e1f0","remedy":"No signature verdict was reached: this installation holds no key that signed this point. Add the signing key to the trust source if you accept evidence from it."}
catalog-counts={"total":1,"available":1,"missing":0,"unreadable":0,"deleted":0,"conflict":0,"unsupportedFormat":0,"partial":0,"signature":{"verified":0,"invalid":0,"noEvidence":0,"notAttempted":1},"byDay":[{"day":"2026-09-16","points":1}]}
catalog-signers=[{"keyId":"0f1e2d3c4b5a69788796a5b4c3d2e1f00f1e2d3c4b5a69788796a5b4c3d2e1f0","points":1}]
catalog-cursor={"indexShard":"2026-09-16","complete":true}
"#;

/// The emitter produces [`PINNED_SYNC_BODY`], byte for byte.
#[test]
fn the_catalog_sync_body_is_pinned_for_the_controllers_parser() {
    let sidecar = claimed_sidecar(CATALOG_CLAIMED_KEY_ID);
    let f = catalog_fixture(
        &catalog_receipt("set-a", "run-a", "2026-09-16T03:00:00Z"),
        "s3://lw-archive/kafka-backups",
        &sidecar,
        CATALOG_CLAIMED_KEY_ID,
    );
    let run = drive_sync(
        sync_request(),
        &FakeWiring::default()
            .with_role(DestinationRole::ArchiveRead, place(FakeObjects::new(), &f)),
    );
    assert_eq!(
        body_of(&run),
        PINNED_SYNC_BODY,
        "the emitted body moved. If the change is intended, paste the left-hand side into \
         `PINNED_SYNC_BODY` AND check that `crates/weirkeeper/tests/catalog_controller.rs`'s \
         `the_runners_pinned_body_is_one_this_parser_reads` still passes — the two are one \
         contract."
    );
}

/// **THE CROSS-CRATE GUARD, for a point whose record gave no facts (FX-33).**
/// The EXACT body the emitter produces for the fixture below: one point whose
/// record reads, one whose record is over its bound, and one whose record is
/// not a record. The second and third are listed by their point ids alone.
///
/// `the_runners_pinned_recordless_body_is_one_this_parser_reads` in
/// `crates/weirkeeper/tests/catalog_controller.rs` extracts this literal and
/// runs the controller's own `parse_body` and `materialise` over it.
const PINNED_RECORDLESS_SYNC_BODY: &str = r#"catalog-format=1
catalog-page=1/1 count=3 sha256=be12d1d5c6d06e64ac350b4a6be14b79d4e556aa9216d8d2afb775a5d68dd6d3
catalog-entry={"pointId":"lwp1-0e02dc33bf63349ec262a62043d9bd04","backupId":"set-a","runId":"run-a","recoveryPointAtMs":1789527600000,"coveredFromMs":1789524000000,"coveredToMs":1789527600000,"locations":[{"locationId":"s3://lw-archive/kafka-backups","availability":"Available"}],"receiptKey":"logweir/backups/set-a/run-a.receipt.json","receiptSha256":"sha256:0e02dc33bf63349ec262a62043d9bd0441fb867c72aa2d5b4ce8a085b05469db","manifestKey":"kafka-backups/set-a/manifest.json","manifestSha256":"sha256:d5eea23a2f7ca3f36d2a5dbf3ab2532a3de3a797ded388afb816068c2863a152","recordedAt":"2026-09-16T06:00:00Z","formatVersion":"1.1.0","availability":"Available","signature":"notAttempted","signerKeyId":"0f1e2d3c4b5a69788796a5b4c3d2e1f00f1e2d3c4b5a69788796a5b4c3d2e1f0","remedy":"No signature verdict was reached: this installation holds no key that signed this point. Add the signing key to the trust source if you accept evidence from it."}
catalog-entry={"pointId":"lwp1-08cf9d03519c5c31f573595a8b368b15","backupId":"","runId":"","recoveryPointAtMs":0,"coveredFromMs":0,"coveredToMs":0,"receiptKey":"","receiptSha256":"","availability":"Unreadable","signature":"notAttempted","remedy":"The catalog record of this recovery point is larger than the 6131072-byte bound Logweir reads for one (it is 6131073 bytes): its backup named more topics, or recorded more configuration, than one backup may (1000 topics). This is the document's size; no permission or network change lists the point. The archive is intact and can be restored from the command line."}
catalog-entry={"pointId":"lwp1-f207be56309520fa5ac69057a3c104e4","backupId":"","runId":"","recoveryPointAtMs":0,"coveredFromMs":0,"coveredToMs":0,"receiptKey":"","receiptSha256":"","availability":"Unreadable","signature":"notAttempted","remedy":"The object at this recovery point's catalog record key is not a catalog point record: it is not JSON, or it is another document. This is the object's content; no permission or network change lists the point. Nothing under logweir/ is rewritten, so find which writer produced the object before relying on this point."}
catalog-counts={"total":3,"available":1,"missing":0,"unreadable":2,"deleted":0,"conflict":0,"unsupportedFormat":0,"partial":0,"signature":{"verified":0,"invalid":0,"noEvidence":0,"notAttempted":3},"byDay":[{"day":"2026-09-16","points":1},{"day":"2026-09-15","points":1},{"day":"2026-09-14","points":1}],"unreadableOverReadCap":1,"unreadableMalformed":1}
catalog-signers=[{"keyId":"0f1e2d3c4b5a69788796a5b4c3d2e1f00f1e2d3c4b5a69788796a5b4c3d2e1f0","points":1}]
catalog-cursor={"indexShard":"2026-09-14","complete":true}
"#;

/// The emitter produces [`PINNED_RECORDLESS_SYNC_BODY`], byte for byte.
#[test]
fn the_recordless_catalog_sync_body_is_pinned_for_the_controllers_parser() {
    use logweir_engine_oso::storage::caps;
    let sidecar = claimed_sidecar(CATALOG_CLAIMED_KEY_ID);
    let fixture = |set: &str, run: &str, at: &str| {
        catalog_fixture(
            &catalog_receipt(set, run, at),
            "s3://lw-archive/kafka-backups",
            &sidecar,
            CATALOG_CLAIMED_KEY_ID,
        )
    };
    let reads = fixture("set-a", "run-a", "2026-09-16T03:00:00Z");
    let oversized = fixture("set-b", "run-b", "2026-09-15T03:00:00Z");
    let broken = fixture("set-c", "run-c", "2026-09-14T03:00:00Z");
    let objects = place(
        place(place(FakeObjects::new(), &reads), &oversized),
        &broken,
    )
    .reporting_size(&oversized.record_key, caps::CATALOG_RECORD + 1)
    .with_object(&broken.record_key, b"not a record");
    assert_eq!(
        body_of(&fx33_sync(&objects)),
        PINNED_RECORDLESS_SYNC_BODY,
        "the emitted body moved. If the change is intended, paste the left-hand side into \
         `PINNED_RECORDLESS_SYNC_BODY` AND check that `crates/weirkeeper/tests/\
         catalog_controller.rs`'s `the_runners_pinned_recordless_body_is_one_this_parser_reads` \
         still passes — the two are one contract."
    );
}

// ===========================================================================
// PROD-11.1 — the replay selection's PREVIEW, through the shared function
// ===========================================================================

/// `restore_yaml` whose `restore:` block states `point_in_time` as the
/// interval `"<start>/<end>"` when `start_ms` is given, plus `extra` keys.
fn selecting_yaml(start_ms: Option<i64>, point_in_time_ms: i64, extra: &str) -> String {
    let pit = ms_to_rfc3339(point_in_time_ms);
    let written = match start_ms {
        Some(start) => format!("\"{}/{pit}\"", ms_to_rfc3339(start)),
        None => pit.clone(),
    };
    restore_yaml(&pit, &["orders"], "scratch").replace(
        &format!("  point_in_time: {pit}\n"),
        &format!("  point_in_time: {written}\n{extra}"),
    )
}

/// The fixture manifest plus a partition 1 whose one segment is early
/// (epoch-ms 1_757_898_000_000 .. 1_757_898_100_000).
fn two_partition_manifest() -> serde_json::Value {
    let mut m = manifest_json();
    m["topics"][0]["partitions"]
        .as_array_mut()
        .unwrap()
        .push(serde_json::json!({
            "partition_id": 1,
            "segments": [
                {"key": "20260915T030000Z/topics/orders/partition=1/segment-0.bin",
                 "start_timestamp": 1_757_898_000_000i64,
                 "end_timestamp":   1_757_898_100_000i64}
            ]
        }));
    m
}

/// One partition whose two segments leave a gap: `[t0, t0 + 1 h]` and
/// `[t0 + 2 h, t0 + 3 h]` (t0 = epoch-ms 1_757_898_000_000).
fn gapped_manifest() -> serde_json::Value {
    serde_json::json!({
        "topics": [{"name": "orders", "partitions": [{"partition_id": 0, "segments": [
            {"key": "20260915T030000Z/topics/orders/partition=0/segment-0.bin",
             "start_timestamp": 1_757_898_000_000i64, "end_timestamp": 1_757_901_600_000i64},
            {"key": "20260915T030000Z/topics/orders/partition=0/segment-1.bin",
             "start_timestamp": 1_757_905_200_000i64, "end_timestamp": 1_757_908_800_000i64}
        ]}]}]
    })
}

/// **A stated start before the archive's coverage is previewed as refused**
/// (`WindowStartBeforeCoverage`), and one AT the floor is covered — the
/// execution guard's rule, from the same function. KILLS: `<=` for `<`, a
/// preview that clamps the start.
#[test]
fn the_preview_refuses_a_window_start_before_coverage() {
    for (start_ms, want) in [
        (1_757_897_999_999i64, CheckCode::WindowStartBeforeCoverage),
        (1_757_898_000_000, CheckCode::PointInTimeCovered),
    ] {
        let yaml = selecting_yaml(Some(start_ms), INSIDE_MS, "");
        let m = mount(&restore_plan(&yaml, None));
        let run = drive(
            &m,
            &restore_wiring(&yaml, &manifest_json(), FakeProbe::new()),
        );
        let row = run.row(CheckId::ArchiveCoverage);
        assert_eq!(row.code, want, "start {start_ms}");
        if want == CheckCode::PointInTimeCovered {
            assert!(
                row.message
                    .contains("from epoch-ms 1757898000000 (the plan's stated start)"),
                "{:?}",
                row.message
            );
        }
    }
}

/// `selecting_yaml` with a partition subset, written beside the interval
/// form a subset needs (PROD-11.1b): `"<start>/<pit>"`, or `"../<pit>"` from
/// the archive's floor.
fn subset_yaml(start_ms: Option<i64>, point_in_time_ms: i64, partitions: &str) -> String {
    let pit = ms_to_rfc3339(point_in_time_ms);
    let written = match start_ms {
        Some(start) => format!("\"{}/{pit}\"", ms_to_rfc3339(start)),
        None => format!("\"../{pit}\""),
    };
    restore_yaml(&pit, &["orders"], "scratch").replace(
        &format!("  point_in_time: {pit}\n"),
        &format!("  point_in_time: {written}\n  partitions:\n    {partitions}\n"),
    )
}

/// **A partition subset is previewed through the shared function**
/// (PROD-11.1b, OD-9 (a)): a subset the archive satisfies is `ready` at
/// `plan.parse` and covered at `archive.coverage`, which names how many
/// partitions it selects, from where and in how many engine runs; a subset
/// naming a partition the archive does not list is `PartitionNotInBackupSet`;
/// an empty subset is `SelectionInvalid` at `plan.parse` and nothing after it
/// runs; a subset beside a plain instant (which an older runner would widen)
/// does not parse. A window no segment overlaps is `SelectionEmpty`. KILLS:
/// the old by-name refusal; a preview that widens a subset to the topic.
#[test]
fn the_preview_resolves_a_partition_subset_and_names_what_it_cannot_satisfy() {
    let run_of = |yaml: &str, manifest: &serde_json::Value| {
        let m = mount(&restore_plan(yaml, None));
        drive(&m, &restore_wiring(yaml, manifest, FakeProbe::new()))
    };
    let run = run_of(
        &subset_yaml(None, INSIDE_MS, "orders: [1]"),
        &two_partition_manifest(),
    );
    assert_eq!(run.row(CheckId::PlanParse).code, CheckCode::PlanParsed);
    let row = run.row(CheckId::ArchiveCoverage);
    assert_eq!(row.code, CheckCode::PointInTimeCovered, "{:?}", row.message);
    assert!(
        row.message.contains(
            "the plan selects 1 partition(s) of 1 topic(s) (1 with archived segments in the \
             window) from epoch-ms 1757898000000 (the archive's floor)"
        ) && row.message.contains("restored by 1 engine run(s)"),
        "{:?}",
        row.message
    );

    let run = run_of(
        &subset_yaml(None, INSIDE_MS, "orders: [0, 3]"),
        &manifest_json(),
    );
    let row = run.row(CheckId::ArchiveCoverage);
    assert_eq!(row.code, CheckCode::PartitionNotInBackupSet);
    assert!(
        row.message
            .contains("restore.partitions.orders names partition 3, which the archive set's"),
        "{:?}",
        row.message
    );

    let run = run_of(
        &subset_yaml(None, INSIDE_MS, "orders: []"),
        &manifest_json(),
    );
    let row = run.row(CheckId::PlanParse);
    assert_eq!(row.code, CheckCode::SelectionInvalid);
    assert_eq!(row.state, CheckState::NotReady);
    assert!(
        row.message
            .starts_with("restore.partitions.orders is empty"),
        "{}",
        row.message
    );
    assert!(
        !run.has(CheckId::ArchiveCoverage),
        "nothing after plan.parse runs"
    );

    let beside_an_instant = selecting_yaml(None, INSIDE_MS, "  partitions:\n    orders: [0]\n");
    let run = run_of(&beside_an_instant, &manifest_json());
    assert_eq!(run.row(CheckId::PlanParse).code, CheckCode::PlanUnparseable);
    assert!(!run.has(CheckId::ArchiveCoverage));

    // The gap between the two segments: a window inside it selects nothing.
    let yaml = selecting_yaml(Some(1_757_902_000_000), 1_757_904_000_000, "");
    let m = mount(&restore_plan(&yaml, None));
    let run = drive(
        &m,
        &restore_wiring(&yaml, &gapped_manifest(), FakeProbe::new()),
    );
    assert_eq!(
        run.row(CheckId::ArchiveCoverage).code,
        CheckCode::SelectionEmpty
    );
}

/// **The segments row checks the SELECTION's segments.** With a start after
/// segment 0 and after partition 1's only segment, a missing segment 0 and a
/// missing partition-1 segment are not the selection's and the row is ready;
/// the same archive previewed with no start (the control) reports them
/// missing. KILLS: the segments row reading the archive's floor when the plan
/// states a start.
#[test]
fn the_preview_checks_only_the_segments_the_selection_reads() {
    let manifest = two_partition_manifest();
    let archive = |drop: &[&str]| {
        let mut o = FakeObjects::new()
            .with_prefix(ARCHIVE_PREFIX)
            .with_object(MANIFEST_OBJECT_KEY, &serde_json::to_vec(&manifest).unwrap());
        for key in [
            "topics/orders/partition=0/segment-0.bin",
            "topics/orders/partition=0/segment-1.bin",
            "topics/orders/partition=1/segment-0.bin",
        ] {
            if !drop.contains(&key) {
                o = o.with_object(&format!("kafka-backups/{BACKUP_ID}/{key}"), b"segment");
            }
        }
        o
    };
    let wiring = |yaml: &str, objects: FakeObjects| {
        FakeWiring::default()
            .with_file(PLAN_FILE, yaml.as_bytes())
            .with_probe(FakeProbe::new())
            .with_role(DestinationRole::ArchiveRead, objects)
    };
    let missing = archive(&[
        "topics/orders/partition=0/segment-0.bin",
        "topics/orders/partition=1/segment-0.bin",
    ]);

    let selecting = selecting_yaml(Some(1_757_901_600_001), INSIDE_MS, "");
    let run = drive(
        &mount(&restore_plan(&selecting, None)),
        &wiring(&selecting, missing.clone()),
    );
    let row = run.row(CheckId::ArchiveSegments);
    assert_eq!(row.code, CheckCode::SegmentsPresent, "{:?}", row.message);
    assert!(
        row.message.starts_with("all 1 segment(s)"),
        "{}",
        row.message
    );

    let control = selecting_yaml(None, INSIDE_MS, "");
    let run = drive(
        &mount(&restore_plan(&control, None)),
        &wiring(&control, missing),
    );
    assert_eq!(
        run.row(CheckId::ArchiveSegments).code,
        CheckCode::SegmentMissing,
        "the full selection reads segment 0 and partition 1"
    );
}

/// **Preview and execution select the same records: one function, one
/// answer.** The restore preflight's projection of the manifest JSON
/// (`check::archive::topic_facts`) and execution's `OsoCliEngine::describe`
/// of the SAME archive go through `ReplaySelection::resolve` and name the
/// same segments, partitions, bounds and engine runs, for three window starts
/// (inside the first segment, just after it, and inside a later partition's
/// only segment).
#[test]
fn the_preview_and_execution_resolve_the_same_selection() {
    use logweir_core::engine::{BackupSetRef, DataEngine};
    use logweir_engine_oso::storage::Store;
    let dir = tempfile::tempdir().unwrap();
    let set_dir = dir.path().join("kafka-backups").join(BACKUP_ID);
    std::fs::create_dir_all(&set_dir).unwrap();
    let seg = |p: i64, i: i64, t0: i64, t1: i64, o0: i64| {
        serde_json::json!({
            "key": format!("{BACKUP_ID}/topics/orders/partition={p}/segment-{i}.bin"),
            "start_offset": o0, "end_offset": o0 + 9, "record_count": 10,
            "start_timestamp": t0, "end_timestamp": t1
        })
    };
    let manifest = serde_json::json!({
        "backup_id": BACKUP_ID,
        "created_at": 1_757_908_800_000i64,
        "topics": [
            {"name": "orders", "partitions": [
                {"partition_id": 0, "segments": [
                    seg(0, 0, 1_757_898_000_000, 1_757_901_600_000, 0),
                    seg(0, 1, 1_757_901_600_001, 1_757_908_800_000, 10)]},
                {"partition_id": 1, "segments": [
                    seg(1, 0, 1_757_899_000_000, 1_757_905_000_000, 0)]},
                {"partition_id": 2, "segments": [
                    seg(2, 0, 1_757_906_000_000, 1_757_907_000_000, 0)]}
            ]},
            {"name": "payments", "partitions": [
                {"partition_id": 0, "segments": [
                    seg(0, 0, 1_757_897_000_000, 1_757_899_000_000, 0)]}
            ]}
        ]
    });
    std::fs::write(
        set_dir.join("manifest.json"),
        serde_json::to_vec(&manifest).unwrap(),
    )
    .unwrap();
    let store = Store::read_only_from_url(&logweir_core::engine::StorageUrl::Filesystem {
        path: dir.path().to_path_buf(),
    })
    .unwrap();
    let preview_store = Store::read_only_from_url(&logweir_core::engine::StorageUrl::Filesystem {
        path: dir.path().to_path_buf(),
    })
    .unwrap();
    let engine = logweir_engine_oso::engine::OsoCliEngine::new(
        "/nonexistent/kafka-backup".into(),
        "v0".into(),
        "sha256:0".into(),
        dir.path().to_path_buf(),
        store,
    );
    let facts = engine
        .describe(&BackupSetRef {
            backup_id: BACKUP_ID.into(),
            manifest_key: format!("kafka-backups/{BACKUP_ID}/manifest.json"),
        })
        .expect("execution reads the manifest");
    let mapping: BTreeMap<String, String> = [("orders".to_string(), "restore-orders".to_string())]
        .into_iter()
        .collect();
    // Three window starts, and (PROD-11.1b) two partition subsets: one from
    // the archive's floor and one from a start.
    let plans: Vec<(String, String)> = [1_757_900_000_000i64, 1_757_901_600_001, 1_757_906_500_000]
        .iter()
        .map(|start| {
            (
                format!("start {start}"),
                selecting_yaml(Some(*start), 1_757_908_000_000, ""),
            )
        })
        .chain([
            (
                "subset [0, 2] from the floor".to_string(),
                subset_yaml(None, 1_757_908_000_000, "orders: [2, 0]"),
            ),
            (
                "subset [1] from a start".to_string(),
                subset_yaml(Some(1_757_900_000_000), 1_757_908_000_000, "orders: [1]"),
            ),
        ])
        .collect();
    for (start, yaml) in plans {
        let spec: logweir_core::spec::DrillSpec = serde_yaml::from_str(&yaml).unwrap();
        let execution = logweir::drill::resolve_selection(&spec, &mapping, &facts)
            .expect("execution resolves")
            .expect("a selection");
        let preview = logweir_core::replay_selection::ReplaySelection::from_spec(&spec)
            .unwrap()
            .resolve(&logweir::check::archive::topic_facts(&manifest))
            .expect("the preview resolves");
        let qualified: Vec<String> = preview
            .segment_keys()
            .iter()
            .map(|k| ObjectAccess::qualify(&preview_store, k))
            .collect();
        assert_eq!(qualified, execution.segment_keys(), "start {start}");
        let shape = |r: &logweir_core::replay_selection::ResolvedSelection| {
            (
                r.floor_ms,
                r.start_ms,
                r.start_source,
                r.end_ms,
                r.runs.clone(),
                r.partitions
                    .iter()
                    .map(|p| {
                        (
                            p.topic.clone(),
                            p.partition,
                            p.records_lower,
                            p.records_upper,
                        )
                    })
                    .collect::<Vec<_>>(),
            )
        };
        assert_eq!(shape(&preview), shape(&execution), "start {start}");
    }
}

/// **PROD-03.0: the sync lists each topic's schema dependency as the shared
/// fixture says.** `ui/tests/fixtures/console/catalog-point-schema-dependency.json`
/// holds a point record's `topics[].schema_dependency` (`recordTopics`) and the
/// entry the view must list for each (`entryTopics`): the verdict, the basis or
/// reason, the DEPENDENT sides only, their ids merged (never a not-dependent
/// side's id), and `schemaIdsOmitted` when more were seen than listed. The API
/// row (`d3_reads.rs`) and the console spec read the same file.
#[test]
fn the_sync_lists_each_topics_schema_dependency_as_the_fixture_says() {
    use logweir::check::kinds::catalog_sync::EntrySchemaDependency;
    let path = repo_root().join("ui/tests/fixtures/console/catalog-point-schema-dependency.json");
    let fixture: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
    let records = fixture["recordTopics"].as_array().unwrap();
    let entries = fixture["entryTopics"].as_array().unwrap();
    assert_eq!(records.len(), entries.len());
    assert!(
        records.len() >= 4,
        "the chain covers every verdict and the cap"
    );
    for (record, entry) in records.iter().zip(entries) {
        assert_eq!(record["name"], entry["name"]);
        let d: logweir_core::backup_receipt::TopicSchemaDependency =
            serde_json::from_value(record["schema_dependency"].clone()).unwrap();
        let listed = serde_json::to_value(EntrySchemaDependency::of(&d)).unwrap();
        assert_eq!(listed, entry["schemaDependency"], "{}", record["name"]);
    }
}

/// **PROD-15.1 review L6.** The readiness check's `plan.parse` refuses an
/// original-name block in a shape phase 0 refuses — in scratch mode, beside a
/// prefix, in a SAMPLED plan (an original-name restore requires complete
/// verification), or beside a PARTITION SUBSET (it restores whole topics) —
/// in the runner's own words, and nothing after it runs; the opted-in plan
/// that asks for complete coverage parses. KILLS: a preview that says ready
/// for a plan the runner refuses at phase 0.
#[test]
fn the_preview_refuses_an_original_name_block_in_a_shape_phase_0_refuses() {
    let point = ms_to_rfc3339(INSIDE_MS);
    let base = restore_yaml(&point, &["orders"], "newTopic");
    for (label, yaml, token) in [
        (
            "in scratch mode",
            restore_yaml(&point, &["orders"], "scratch").replace(
                "  topic_mapping_prefix: 'restore-'\n",
                "  topic_mapping_prefix: 'restore-'\n  topic_naming:\n    prefix: ''\n    original_name: {owners: []}\n",
            ),
            "OriginalNameNotNewTopic",
        ),
        (
            "beside a prefix",
            base.replace(
                "  topic_mapping_prefix: 'restore-'\n",
                "  topic_mapping_prefix: 'restore-'\n  topic_naming:\n    prefix: 'x-'\n    original_name: {owners: []}\n",
            ),
            "OriginalNamePrefixNotEmpty",
        ),
        (
            "with sampled coverage",
            base.replace(
                "  topic_mapping_prefix: 'restore-'\n",
                "  topic_mapping_prefix: 'restore-'\n  topic_naming:\n    prefix: ''\n    original_name: {owners: []}\n",
            ),
            "OriginalNameNeedsCompleteCoverage",
        ),
        (
            // PROD-15.1 after PROD-11.1b: an original-name restore restores
            // whole topics. Complete coverage, so the subset is what refuses.
            "with a partition subset",
            base.replace(
                "  topic_mapping_prefix: 'restore-'\n",
                "  topic_mapping_prefix: 'restore-'\n  topic_naming:\n    prefix: ''\n    original_name: {owners: []}\n",
            )
            .replace(
                "  window_end: 2026-09-15T06:00:00Z\n",
                "  window_end: 2026-09-15T06:00:00Z\n  coverage: complete\n",
            )
            .replace(
                &format!("  point_in_time: {point}\n"),
                &format!("  point_in_time: \"../{point}\"\n  partitions:\n    orders: [0]\n"),
            ),
            "OriginalNameNeedsWholeTopics",
        ),
    ] {
        assert!(yaml.contains("original_name"), "{label}: the fixture carries the block");
        assert_eq!(
            label == "with a partition subset",
            yaml.contains("partitions:\n    orders: [0]"),
            "{label}: only the subset case states a subset"
        );
        let m = mount(&restore_plan(&yaml, None));
        let run = drive(
            &m,
            &restore_wiring(&yaml, &manifest_json(), FakeProbe::new()),
        );
        let row = run.row(CheckId::PlanParse);
        assert_eq!(row.state, CheckState::NotReady, "{label}");
        assert_eq!(row.code, CheckCode::TopicMappingIdentity, "{label}");
        assert!(row.message.starts_with(token), "{label}: {}", row.message);
        assert!(!run.has(CheckId::ArchiveCoverage), "{label}: nothing after plan.parse runs");
    }
    let opted_in = base
        .replace(
            "  topic_mapping_prefix: 'restore-'\n",
            "  topic_mapping_prefix: 'restore-'\n  topic_naming:\n    prefix: ''\n    original_name: {owners: []}\n",
        )
        .replace(
            "  window_end: 2026-09-15T06:00:00Z\n",
            "  window_end: 2026-09-15T06:00:00Z\n  coverage: complete\n",
        );
    assert!(opted_in.contains("coverage: complete"));
    let m = mount(&restore_plan(&opted_in, None));
    let run = drive(
        &m,
        &restore_wiring(&opted_in, &manifest_json(), FakeProbe::new()),
    );
    assert_eq!(run.row(CheckId::PlanParse).state, CheckState::Ready);
}

// ===========================================================================
// FX-33 — a backup of many topics stays listed; a counted point is never
// dropped
// ===========================================================================

/// The budget's reference receipt of `topics` topics — each with the 13
/// recorded entries, a `generations` entry, a `schema_dependency` entry, and
/// PROD-04.1's largest consumer position summary beside them
/// (`logweir_core::topic_budget::reference_receipt`, the `FULL` shape) — as a
/// run on `started` wrote it into set `backup_id`, over [`CATALOG_MANIFEST`].
fn fx33_receipt(topics: usize, backup_id: &str, started: &str) -> BackupReceipt {
    use logweir_core::topic_budget::{reference_receipt, ReferenceShape};
    let mut r = reference_receipt(topics, &ReferenceShape::FULL);
    let shift = catalog_ts(started) - r.started_at;
    r.requested_at += shift;
    r.started_at += shift;
    r.finished_at += shift;
    r.covered.from_ms += shift.num_milliseconds();
    r.covered.to_ms += shift.num_milliseconds();
    r.backup_id = backup_id.to_string();
    r.archive.manifest_key = format!("kafka-backups/{backup_id}/manifest.json");
    r.archive.manifest_sha256 = logweir_core::ids::sha256_prefixed(CATALOG_MANIFEST);
    if let Some(positions) = r.consumer_positions.as_mut() {
        positions.observed_from += shift;
        positions.observed_to += shift;
        positions.document.key =
            logweir_core::consumer_positions::document_key(backup_id, &r.run_id);
    }
    assert_eq!(r.validate_invariants(), Ok(()), "the reference is valid");
    r
}

fn fx33_sync(objects: &FakeObjects) -> Run {
    drive_sync(
        sync_request(),
        &FakeWiring::default().with_role(DestinationRole::ArchiveRead, objects.clone()),
    )
}

/// An entry for a point whose record gave no facts: listed, not `Available`,
/// and carrying nothing a restore binds to or a reader joins on.
fn fx33_assert_recordless(entry: &serde_json::Value) {
    assert_ne!(entry["availability"], "Available", "{entry}");
    assert_eq!(entry["signature"], "notAttempted", "{entry}");
    for (field, empty) in [
        ("backupId", serde_json::json!("")),
        ("runId", serde_json::json!("")),
        ("recoveryPointAtMs", serde_json::json!(0)),
        ("coveredFromMs", serde_json::json!(0)),
        ("coveredToMs", serde_json::json!(0)),
        ("receiptKey", serde_json::json!("")),
        ("receiptSha256", serde_json::json!("")),
    ] {
        assert_eq!(entry[field], empty, "`{field}`: {entry}");
    }
    for absent in [
        "locations",
        "manifestKey",
        "manifestSha256",
        "signerKeyId",
        "topics",
    ] {
        assert!(entry.get(absent).is_none(), "`{absent}`: {entry}");
    }
}

/// **FX-33's acceptance, at the catalog: a backup of 70, 105, 113, 300, 500 or
/// 1,000 topics with full recorded configuration is LISTED, `Available`, and
/// its signature VERIFIED** — every one of them a record that the walk's old
/// 256 KiB cap dropped from the catalog without a line.
///
/// KILLS: the walk's record or receipt cap put back to one a backup
/// outgrows (the point is `Unreadable`, not `Available`); the entry dropped.
#[test]
fn a_backup_of_many_topics_is_listed_available_and_verified() {
    use logweir_engine_oso::storage::caps;
    /// The catalog walk's one document cap before FX-33.
    const OLD_WALK_CAP: usize = 256 * 1024;
    let key = logweir_evidence::keys::SigningKey::generate_p256();
    let key_id = key.verifying_key().key_id();
    let pem = key
        .verifying_key()
        .to_public_key_pem()
        .expect("a public key renders");
    let request = logweir_core::check_contract::CatalogSyncRequest {
        trust_bundle_file: Some("/check/trust/trust-bundle.pem".to_string()),
        ..sync_request()
    };
    for topics in [70usize, 105, 113, 300, 500, 1_000] {
        let receipt = fx33_receipt(topics, "set-a", "2026-09-16T03:00:00Z");
        let receipt_bytes =
            logweir_core::det_json::to_deterministic_json(&receipt).expect("serialises");
        let sidecar = logweir_evidence::sign::sign_detached(
            &key,
            logweir_evidence::PAYLOAD_TYPE_BACKUP_RECEIPT,
            &receipt_bytes,
        )
        .expect("the sidecar signs");
        let f = catalog_fixture(&receipt, "s3://lw-archive/kafka-backups", &sidecar, &key_id);
        let sizes = format!(
            "{topics} topics: receipt {} bytes, record {} bytes",
            f.receipt_bytes.len(),
            f.record_bytes.len()
        );
        eprintln!("[fx-33] {sizes}");
        assert!(
            f.record_bytes.len() > OLD_WALK_CAP,
            "NEGATIVE CONTROL: under the old cap this record was dropped: {sizes}"
        );
        assert!(
            f.receipt_bytes.len() as u64 <= caps::CATALOG_RECEIPT
                && f.record_bytes.len() as u64 <= caps::CATALOG_RECORD,
            "{sizes}"
        );
        let wiring = FakeWiring::default()
            .with_role(DestinationRole::ArchiveRead, place(FakeObjects::new(), &f))
            .with_file("/check/trust/trust-bundle.pem", pem.as_bytes());
        let body = body_of(&drive_sync(request.clone(), &wiring));
        let entries = entries_of(&body);
        assert_eq!(entries.len(), 1, "{sizes}: the point is not listed");
        let entry = &entries[0];
        assert_eq!(entry["pointId"], f.point.point_id.as_str(), "{sizes}");
        assert_eq!(entry["availability"], "Available", "{sizes}: {entry}");
        assert_eq!(entry["signature"], "verified", "{sizes}: {entry}");
        assert_eq!(entry["signerKeyId"], key_id, "{sizes}");
        assert_eq!(entry["receiptKey"], f.receipt_key.as_str(), "{sizes}");
        assert_eq!(
            entry["receiptSha256"],
            logweir_core::ids::sha256_prefixed(&f.receipt_bytes),
            "{sizes}"
        );
        assert!(entry.get("remedy").is_none(), "{sizes}: {entry}");
        // More topics than an entry lists: counted, and the signed record
        // names each (PROD-05.1).
        assert_eq!(entry["topicsOmitted"], topics, "{sizes}: {entry}");
        assert_eq!(
            entry["consumerPositions"]["groupsOmitted"],
            logweir_core::consumer_positions::MAX_SELECTED_GROUPS,
            "{sizes}: {entry}"
        );
        let counts = summary_of(&body, "catalog-counts=");
        assert_eq!(counts["total"], 1, "{sizes}");
        assert_eq!(counts["available"], 1, "{sizes}");
        assert_eq!(counts["signature"]["verified"], 1, "{sizes}");
        assert!(counts.get("unreadableOverReadCap").is_none(), "{counts}");
    }
}

/// **A backup over the maximum — one an older runner wrote, 5,000 topics — is
/// LISTED with its size against the bound, and never dropped.** Nothing but
/// its point id and the reason is published; nothing else is read for it;
/// and the remedy names no grant.
///
/// KILLS: `build_entry` drops again (no entry); the remedy names the grant;
/// the record read under no cap (a 15 MB record parsed, the point
/// `Available`).
#[test]
fn a_backup_over_the_maximum_is_listed_with_its_size_and_never_dropped() {
    use logweir_engine_oso::storage::caps;
    let receipt = fx33_receipt(5_000, "set-big", "2026-09-16T03:00:00Z");
    let f = catalog_fixture(
        &receipt,
        "s3://lw-archive/kafka-backups",
        &claimed_sidecar(CATALOG_CLAIMED_KEY_ID),
        CATALOG_CLAIMED_KEY_ID,
    );
    let record_len = f.record_bytes.len() as u64;
    assert!(
        record_len > caps::CATALOG_RECORD && f.receipt_bytes.len() as u64 > caps::CATALOG_RECEIPT,
        "5,000 topics are over both bounds: record {record_len}, receipt {}",
        f.receipt_bytes.len()
    );
    let objects = place(FakeObjects::new(), &f);
    let body = body_of(&fx33_sync(&objects));
    let counts = summary_of(&body, "catalog-counts=");
    assert_eq!(counts["total"], 1, "{counts}");
    assert_eq!(counts["unreadable"], 1, "{counts}");
    assert_eq!(counts["unreadableOverReadCap"], 1, "{counts}");
    assert_eq!(counts["available"], 0, "{counts}");
    let entries = entries_of(&body);
    assert_eq!(entries.len(), 1, "a counted point is listed: {counts}");
    let entry = &entries[0];
    fx33_assert_recordless(entry);
    assert_eq!(entry["pointId"], f.point.point_id.as_str());
    assert_eq!(entry["availability"], "Unreadable");
    let remedy = entry["remedy"].as_str().expect("a remedy");
    assert!(
        remedy.contains(&format!("{record_len} bytes"))
            && remedy.contains(&format!("{}-byte bound", caps::CATALOG_RECORD))
            && remedy.contains("1000 topics")
            && remedy.contains("restored from the command line"),
        "{remedy}"
    );
    for word in ["grant", "archiveRead", "endpoint", "credential"] {
        assert!(!remedy.contains(word), "a size names no `{word}`: {remedy}");
    }
    assert!(remedy.len() <= 512, "{} characters", remedy.len());
    // THE READS: the record under its own cap, and nothing else of this
    // point — not its index row, not its 15 MB receipt.
    let reads = objects.read_caps();
    assert_eq!(
        reads,
        vec![(f.record_key.clone(), caps::CATALOG_RECORD)],
        "nothing else is read of a point whose record gave no facts"
    );
}

/// **A counted point is always listed, and each way it can fail has its own
/// reason.** Nine faults, each on one document of one point: the entry is
/// there, it is not `Available`, its remedy fits the fault, and the
/// sub-count that fits the reason — and only that one — moves. A point whose
/// RECORD gave no facts carries its point id and nothing else; a point whose
/// record read keeps its record's facts.
///
/// KILLS: `build_entry` drops again, for any one cause; a size or a content
/// fault counted as a permission or transport failure; a cause's remedy
/// naming the grant.
#[test]
fn a_counted_point_is_always_listed_with_its_own_reason() {
    use logweir_engine_oso::storage::caps;
    let sidecar = claimed_sidecar(CATALOG_CLAIMED_KEY_ID);
    let fixture = || {
        catalog_fixture(
            &catalog_receipt("set-a", "run-a", "2026-09-16T03:00:00Z"),
            "s3://lw-archive/kafka-backups",
            &sidecar,
            CATALOG_CLAIMED_KEY_ID,
        )
    };
    let f = fixture();
    let io = || Fault::Io("connection reset by peer".to_string());
    let other_major = {
        let mut doc: serde_json::Value = serde_json::from_slice(&f.record_bytes).expect("JSON");
        doc["format_version"] = serde_json::json!("2.0.0");
        serde_json::to_vec(&doc).expect("JSON")
    };
    let placed = || place(FakeObjects::new(), &f);
    // (what, the store, availability, document, reason, over-cap, malformed,
    //  the record gave facts, the remedy names the grant)
    type Case = (
        &'static str,
        FakeObjects,
        &'static str,
        &'static str,
        &'static str,
        i64,
        i64,
        bool,
        bool,
    );
    let cases: Vec<Case> = vec![
        (
            "the record's read fails",
            placed().failing_key(&f.record_key, io()),
            "Unreadable",
            "record",
            "readFailed",
            0,
            0,
            false,
            true,
        ),
        (
            "the record is absent",
            placed().failing_key(&f.record_key, Fault::NotFound),
            "Missing",
            "record",
            "notFound",
            0,
            0,
            false,
            false,
        ),
        (
            "the record is over its bound",
            placed().reporting_size(&f.record_key, caps::CATALOG_RECORD + 1),
            "Unreadable",
            "record",
            "overReadCap",
            1,
            0,
            false,
            false,
        ),
        (
            "the record is not a record",
            placed().with_object(&f.record_key, b"not a record"),
            "Unreadable",
            "record",
            "malformed",
            0,
            1,
            false,
            false,
        ),
        (
            "the record is of another major",
            placed().with_object(&f.record_key, &other_major),
            "UnsupportedFormat",
            "record",
            "unsupportedFormat",
            0,
            0,
            false,
            false,
        ),
        (
            "the receipt's read fails",
            placed().failing_key(&f.receipt_key, io()),
            "Unreadable",
            "receipt",
            "readFailed",
            0,
            0,
            true,
            true,
        ),
        (
            "the receipt is over its bound",
            placed().reporting_size(&f.receipt_key, caps::CATALOG_RECEIPT + 1),
            "Unreadable",
            "receipt",
            "overReadCap",
            1,
            0,
            true,
            false,
        ),
        (
            "the receipt is not a receipt",
            placed().with_object(&f.receipt_key, b"{\"not\":\"a receipt\"}"),
            "Unreadable",
            "receipt",
            "malformed",
            0,
            1,
            true,
            false,
        ),
        (
            "the manifest is over its bound",
            placed().reporting_size(&f.manifest_key, caps::MANIFEST + 1),
            "Unreadable",
            "manifest",
            "overReadCap",
            1,
            0,
            true,
            false,
        ),
    ];
    for (what, objects, availability, document, reason, over, malformed, backed, grant) in cases {
        let body = body_of(&fx33_sync(&objects));
        let counts = summary_of(&body, "catalog-counts=");
        let entries = entries_of(&body);
        assert_eq!(counts["total"], 1, "{what}: {counts}");
        assert_eq!(entries.len(), 1, "{what}: a counted point is not listed");
        let entry = &entries[0];
        assert_eq!(entry["pointId"], f.point.point_id.as_str(), "{what}");
        assert_eq!(entry["availability"], availability, "{what}: {entry}");
        assert_eq!(
            counts
                .get("unreadableOverReadCap")
                .and_then(serde_json::Value::as_i64)
                .unwrap_or(0),
            over,
            "{what}: {counts}"
        );
        assert_eq!(
            counts
                .get("unreadableMalformed")
                .and_then(serde_json::Value::as_i64)
                .unwrap_or(0),
            malformed,
            "{what}: {counts}"
        );
        if backed {
            assert_eq!(entry["backupId"], "set-a", "{what}: the record's facts");
            assert_eq!(entry["receiptKey"], f.receipt_key.as_str(), "{what}");
        } else {
            fx33_assert_recordless(entry);
        }
        let remedy = entry["remedy"].as_str().unwrap_or_default();
        assert!(!remedy.is_empty(), "{what}: a point not offered says why");
        assert_eq!(
            remedy.contains("grant"),
            grant,
            "{what}: only a read that did not answer names the grant: {remedy}"
        );
        if reason == "overReadCap" {
            assert!(
                remedy.contains("-byte bound") && remedy.contains(document),
                "{what}: a size names its document and its bound: {remedy}"
            );
        }
    }
}

/// **Points that only failed to be read never push a readable point out of
/// the window.** An archive can hold many points nobody can take — an older
/// build's oversized points, reads that failed — and the window is
/// `viewLimit` entries in walk order. Here five such points are NEWER than
/// three readable ones, so the walk meets them first, and the window holds
/// three: it lists the three readable points, and counts all eight (so the
/// controller's view says it is a window). CONTROL: with room for all eight,
/// the five are listed too.
///
/// KILLS: entries kept in walk order alone (the window would hold three
/// unreadable points and no readable one).
#[test]
fn unreadable_points_never_push_a_readable_point_out_of_the_window() {
    use logweir_engine_oso::storage::caps;
    let sidecar = claimed_sidecar(CATALOG_CLAIMED_KEY_ID);
    let mut objects = FakeObjects::new();
    let mut readable = Vec::new();
    for day in 1..=8 {
        let f = catalog_fixture(
            &catalog_receipt(
                &format!("set-{day}"),
                "run-a",
                &format!("2026-09-{day:02}T03:00:00Z"),
            ),
            "s3://lw-archive/kafka-backups",
            &sidecar,
            CATALOG_CLAIMED_KEY_ID,
        );
        objects = place(objects, &f);
        if day <= 3 {
            readable.push(f.point.point_id.clone());
        } else if day % 2 == 0 {
            // An older build's oversized point.
            objects = objects.reporting_size(&f.record_key, caps::CATALOG_RECORD + 1);
        } else {
            // A read that failed.
            objects = objects.failing_key(
                &f.record_key,
                Fault::Io("connection reset by peer".to_string()),
            );
        }
    }
    let window = |view_limit: i64| {
        let body = body_of(&drive_sync(
            logweir_core::check_contract::CatalogSyncRequest {
                view_limit,
                ..sync_request()
            },
            &FakeWiring::default().with_role(DestinationRole::ArchiveRead, objects.clone()),
        ));
        let counts = summary_of(&body, "catalog-counts=");
        assert_eq!(counts["total"], 8, "every point is counted: {counts}");
        assert_eq!(counts["unreadable"], 5, "{counts}");
        entries_of(&body)
    };
    let listed = window(3);
    let mut ids: Vec<&str> = listed
        .iter()
        .map(|e| e["pointId"].as_str().expect("an id"))
        .collect();
    ids.sort_unstable();
    let mut want: Vec<&str> = readable.iter().map(String::as_str).collect();
    want.sort_unstable();
    assert_eq!(
        ids, want,
        "the window holds the readable points: {listed:?}"
    );
    assert!(listed.iter().all(|e| e["availability"] == "Available"));
    // CONTROL: room for every point, and the unreadable ones are listed.
    let all = window(8);
    assert_eq!(all.len(), 8);
    assert_eq!(
        all.iter()
            .filter(|e| e["availability"] == "Unreadable")
            .count(),
        5
    );
}

/// **A readable point never pushes EVIDENCE out of the window.** Entries
/// that say something about the archive's integrity — a `Conflict` (the
/// archive contradicts the signed receipt), a record whose bytes are not a
/// record — are never the ones an `Available` point displaces. Here three
/// such points are newer than three readable ones, the window holds three,
/// and it keeps the three pieces of evidence; the readable points are
/// counted, and the view says it is a window.
///
/// KILLS: an `Available` point displacing any entry that is not `Available`
/// (a tampered point pushed out of the listing by points written after it).
#[test]
fn a_readable_point_never_pushes_integrity_evidence_out_of_the_window() {
    let sidecar = claimed_sidecar(CATALOG_CLAIMED_KEY_ID);
    let mut objects = FakeObjects::new();
    let mut evidence = Vec::new();
    for day in 1..=6 {
        let f = catalog_fixture(
            &catalog_receipt(
                &format!("set-{day}"),
                "run-a",
                &format!("2026-09-{day:02}T03:00:00Z"),
            ),
            "s3://lw-archive/kafka-backups",
            &sidecar,
            CATALOG_CLAIMED_KEY_ID,
        );
        objects = place(objects, &f);
        match day {
            // The archive's manifest is not the one the receipt signed.
            4 => objects = objects.with_object(&f.manifest_key, b"{\"topics\":[1]}"),
            // The record contradicts the receipt it names.
            5 => {
                let mut doc: serde_json::Value =
                    serde_json::from_slice(&f.record_bytes).expect("JSON");
                doc["covered"]["to_ms"] = serde_json::json!(1i64);
                objects =
                    objects.with_object(&f.record_key, &serde_json::to_vec(&doc).expect("JSON"));
            }
            // Bytes at the record's key that are not a record.
            6 => objects = objects.with_object(&f.record_key, b"{\"format_version\":\"1."),
            _ => continue,
        }
        evidence.push(f.point.point_id.clone());
    }
    let body = body_of(&drive_sync(
        logweir_core::check_contract::CatalogSyncRequest {
            view_limit: 3,
            ..sync_request()
        },
        &FakeWiring::default().with_role(DestinationRole::ArchiveRead, objects),
    ));
    let counts = summary_of(&body, "catalog-counts=");
    assert_eq!(
        (&counts["total"], &counts["available"], &counts["conflict"]),
        (
            &serde_json::json!(6),
            &serde_json::json!(3),
            &serde_json::json!(2)
        ),
        "{counts}"
    );
    let listed = entries_of(&body);
    let mut ids: Vec<&str> = listed
        .iter()
        .map(|e| e["pointId"].as_str().expect("an id"))
        .collect();
    ids.sort_unstable();
    evidence.sort_unstable();
    assert_eq!(ids, evidence, "the evidence stays listed: {listed:?}");
}

/// The H1 fixture: set `set-x` written twice (a re-created Job), its newer
/// receipt's read failing in this sync, then `others` older `Available`
/// points, one a day further back. Returns the store and the newer point's id.
fn fx33_shared_set(others: usize) -> (FakeObjects, String) {
    let sidecar = claimed_sidecar(CATALOG_CLAIMED_KEY_ID);
    let fixture = |set: &str, run: &str, day: usize| {
        catalog_fixture(
            &catalog_receipt(set, run, &format!("2026-09-{day:02}T03:00:00Z")),
            "s3://lw-archive/kafka-backups",
            &sidecar,
            CATALOG_CLAIMED_KEY_ID,
        )
    };
    let newer = fixture("set-x", "run-b", 16);
    let older = fixture("set-x", "run-a", 15);
    let mut objects = place(place(FakeObjects::new(), &newer), &older).failing_key(
        &newer.receipt_key,
        Fault::Io("connection reset by peer".to_string()),
    );
    for i in 0..others {
        objects = place(objects, &fixture(&format!("set-o{i}"), "run-a", 14 - i));
    }
    (objects, newer.point.point_id.clone())
}

/// The ids and availabilities a walk at `view_limit` lists.
fn fx33_listed(objects: &FakeObjects, view_limit: i64) -> Vec<(String, String, String)> {
    let body = body_of(&drive_sync(
        logweir_core::check_contract::CatalogSyncRequest {
            view_limit,
            ..sync_request()
        },
        &FakeWiring::default().with_role(DestinationRole::ArchiveRead, objects.clone()),
    ));
    entries_of(&body)
        .iter()
        .map(|e| {
            (
                e["pointId"].as_str().unwrap_or_default().to_string(),
                e["availability"].as_str().unwrap_or_default().to_string(),
                e["backupId"].as_str().unwrap_or_default().to_string(),
            )
        })
        .collect()
}

/// **Review finding H1, the viewLimit-2 case: a skipped entry that names a
/// set keeps its place.** Set `set-x` has two receipts; the newer one's read
/// fails in this sync, so it is `Unreadable` but keeps its record, and with
/// it the set it names, which retention protects for it. An older
/// `Available` point arriving at a full window must not push it out (it did:
/// the window then held the older receipt of `set-x` and an unrelated point,
/// and retention could plan `set-x` away). CONTROL: with room for all three,
/// all three are listed.
///
/// KILLS: an entry with a record treated as displaceable.
#[test]
fn a_skipped_point_that_names_a_set_keeps_its_place_at_view_limit_two() {
    let (objects, newer) = fx33_shared_set(1);
    let listed = fx33_listed(&objects, 2);
    assert!(
        listed
            .iter()
            .any(|(id, a, set)| *id == newer && a == "Unreadable" && set == "set-x"),
        "the newer receipt of set-x stays listed, naming its set: {listed:?}"
    );
    assert_eq!(listed.len(), 2);
    assert_eq!(fx33_listed(&objects, 3).len(), 3, "CONTROL");
}

/// **Review finding H1, the viewLimit-3 case: the pair inside the window, and
/// an unrelated older point arriving.** The window holds the newer and older
/// receipts of `set-x` and one more point; a fourth, older, `Available` point
/// does not take the newer receipt's place. CONTROL: with room for all four,
/// all four are listed.
///
/// KILLS: an entry with a record treated as displaceable.
#[test]
fn a_skipped_point_that_names_a_set_keeps_its_place_against_an_unrelated_point() {
    let (objects, newer) = fx33_shared_set(2);
    let listed = fx33_listed(&objects, 3);
    assert!(
        listed
            .iter()
            .any(|(id, a, set)| *id == newer && a == "Unreadable" && set == "set-x"),
        "{listed:?}"
    );
    assert_eq!(listed.len(), 3);
    assert_eq!(fx33_listed(&objects, 4).len(), 4, "CONTROL");
}

/// **Review finding M1: a record read that did not answer leaves the walk
/// incomplete.** Such a point is listed by its id only, with no set, so a
/// view with it in is not whole until a sync reads the record; the cursor
/// says `complete: false` and the row counts it (`catalogRecordsUnread`).
/// CONTROL: persistent causes — a record over its bound, bytes that are not
/// a record — leave the walk complete.
///
/// KILLS: the walk reported complete over a record it could not read.
#[test]
fn a_record_read_that_did_not_answer_leaves_the_walk_incomplete() {
    use logweir_engine_oso::storage::caps;
    let (objects, newer, _) = two_point_objects();
    let walk = |objects: FakeObjects| {
        let run = drive_sync(
            sync_request(),
            &FakeWiring::default().with_role(DestinationRole::ArchiveRead, objects),
        );
        let body = body_of(&run);
        let complete = summary_of(&body, "catalog-cursor=")["complete"].clone();
        let unread = run
            .row(CheckId::DestinationArchiveListable)
            .facts
            .get("catalogRecordsUnread")
            .cloned();
        (complete, unread)
    };
    let (complete, unread) =
        walk(objects.failing_key(&newer.record_key, Fault::Io("timed out".to_string())));
    assert_eq!(complete, false);
    assert_eq!(unread.as_deref(), Some("1"));
    // CONTROL: a size and a content fault are facts that will not change.
    // (A fresh store: a fake's clones share their faults.)
    let (objects, newer, older) = two_point_objects();
    let (complete, unread) = walk(
        objects
            .reporting_size(&newer.record_key, caps::CATALOG_RECORD + 1)
            .with_object(&older.record_key, b"not a record"),
    );
    assert_eq!(complete, true);
    assert!(unread.is_none());
}

/// **A day shard of more than one page is read to its end (review finding
/// D4).** Per-minute backups write 1,440 points a day; the walk used to read
/// a day's first page of keys and report the view complete. Here one day
/// holds 1,005 points (their records absent, so each costs one read): every
/// one is counted and listed, and the walk is complete. CONTROL: a budget
/// that runs out after the shard's first page ends the walk incomplete,
/// naming the object budget.
///
/// KILLS: the shard read to its first page only (1,000 counted, complete).
#[test]
fn a_day_shard_of_more_than_one_page_is_listed_whole() {
    use logweir::check::kinds::catalog_sync::SHARD_PAGE_KEYS;
    let points = SHARD_PAGE_KEYS + 5;
    let mut objects = FakeObjects::new();
    let start = catalog_ts("2026-09-16T00:00:00Z");
    for i in 0..points {
        let at = start + chrono::Duration::seconds(i64::try_from(i).expect("fits") * 60);
        let point_id = format!("lwp1-{i:032x}");
        objects = objects.with_object(&logweir::catalog::record::log_key(at, &point_id), b"{}");
    }
    let run = drive_sync(
        sync_request(),
        &FakeWiring::default().with_role(DestinationRole::ArchiveRead, objects.clone()),
    );
    let body = body_of(&run);
    let counts = summary_of(&body, "catalog-counts=");
    assert_eq!(counts["total"], points, "every key of the day is counted");
    assert_eq!(counts["missing"], points);
    assert_eq!(entries_of(&body).len(), points, "and listed");
    assert_eq!(summary_of(&body, "catalog-cursor=")["complete"], true);

    // CONTROL: two objects — the floor listing and the shard's first page.
    let run = drive_sync(
        logweir_core::check_contract::CatalogSyncRequest {
            max_objects_per_run: 2,
            ..sync_request()
        },
        &FakeWiring::default().with_role(DestinationRole::ArchiveRead, objects),
    );
    let body = body_of(&run);
    assert_eq!(
        summary_of(&body, "catalog-cursor=")["complete"],
        false,
        "{body}"
    );
    assert_eq!(
        run.row(CheckId::DestinationArchiveListable).facts["catalogStoppedFor"],
        "objectBudget"
    );
    let spent: i64 = run.row(CheckId::DestinationArchiveListable).facts["catalogObjectsRead"]
        .parse()
        .expect("an object count");
    assert!(
        spent <= 2,
        "the shard's pages are paid from the budget: {spent} of 2"
    );
}

/// **A `Full` rescan lists a point whose record gave no facts, by the point id
/// its key carries, and an object under the points prefix whose key names no
/// point id is not a point.**
///
/// KILLS: a rescan dropping a record it cannot read; a key's text published
/// as a point id.
#[test]
fn a_full_rescan_lists_a_point_without_a_record_from_its_key_alone() {
    let (objects, newer, older) = two_point_objects();
    let full = || logweir_core::check_contract::CatalogSyncRequest {
        mode: logweir_core::check_contract::CatalogSyncMode::Full,
        ..sync_request()
    };
    let objects = objects
        .with_object(&newer.record_key, b"not a record")
        .with_object(
            "logweir/catalog/v1/points/<script>alert(1)<\u{2f}script>/record.json",
            &older.record_bytes,
        )
        .with_object(
            "logweir/catalog/v1/points/not-a-point/record.json",
            &older.record_bytes,
        );
    let run = drive_sync(
        full(),
        &FakeWiring::default().with_role(DestinationRole::ArchiveRead, objects),
    );
    let body = body_of(&run);
    let counts = summary_of(&body, "catalog-counts=");
    assert_eq!(
        counts["total"], 2,
        "two points; two objects that are not: {counts}"
    );
    assert_eq!(counts["available"], 1, "{counts}");
    assert_eq!(counts["unreadableMalformed"], 1, "{counts}");
    let entries = entries_of(&body);
    assert_eq!(entries.len(), 2, "{body}");
    let broken = entries
        .iter()
        .find(|e| e["pointId"] == newer.point.point_id.as_str())
        .expect("the point whose record is not a record is listed");
    fx33_assert_recordless(broken);
    assert!(
        !run.everything().contains("script"),
        "a key's text is not shown"
    );
}

// FX-33: the walk's peak memory, measured in child processes
// ---------------------------------------------------------------------------

const FX33_WALK_TEST: &str = "the_walks_peak_memory_is_one_points";
const FX33_WALK_CHILD_ENV: &str = "FX33_WALK_CHILD";
const FX33_WALK_ROOT_ENV: &str = "FX33_WALK_ROOT";
const FX33_WALK_PEAK_LINE: &str = "FX33_WALK_PEAK_RSS=";
const FX33_WALK_FACT_LINE: &str = "FX33_WALK_AVAILABLE=";
/// How many maximum-size points the "many" tree holds.
const FX33_WALK_POINTS: usize = 4;
/// Where the child mounts the trust bundle.
const FX33_WALK_BUNDLE: &str = "/check/trust/trust-bundle.pem";

/// This process's own peak resident set, in bytes.
fn fx33_self_peak_rss() -> u64 {
    let usage = nix::sys::resource::getrusage(nix::sys::resource::UsageWho::RUSAGE_SELF)
        .expect("getrusage(RUSAGE_SELF)");
    let max = u64::try_from(usage.max_rss()).unwrap_or(0);
    if cfg!(target_os = "macos") {
        max
    } else {
        max * 1024
    }
}

/// Write `points` catalog points under `root`, one a day back from the
/// fixture clock: the largest receipt Logweir writes
/// (`ReferenceShape::LONGEST_NAMES`, within one percent of its bound) when
/// `large`, the 1 KB fixture receipt otherwise. Each is signed under `key`.
/// Returns the largest receipt and record written.
fn fx33_plant_walk(
    root: &Path,
    points: usize,
    large: bool,
    key: &logweir_evidence::keys::SigningKey,
) -> (usize, usize) {
    use logweir_core::topic_budget::{reference_receipt, ReferenceShape, REFERENCE_AT_THE_BOUND};
    let key_id = key.verifying_key().key_id();
    let write = |object_key: &str, bytes: &[u8]| {
        let path = root.join(object_key);
        std::fs::create_dir_all(path.parent().expect("a key has a parent")).expect("mkdir");
        std::fs::write(&path, bytes).expect("the object is written");
    };
    let mut largest = (0, 0);
    for i in 0..points {
        let day = now().date_naive() - chrono::Duration::days(i as i64);
        let started = format!("{}T03:00:00Z", day.format("%Y-%m-%d"));
        let set = format!("set-{i:03}");
        let receipt = if large {
            let mut r = reference_receipt(REFERENCE_AT_THE_BOUND, &ReferenceShape::LONGEST_NAMES);
            let shift = catalog_ts(&started) - r.started_at;
            r.requested_at += shift;
            r.started_at += shift;
            r.finished_at += shift;
            r.covered.from_ms += shift.num_milliseconds();
            r.covered.to_ms += shift.num_milliseconds();
            r.backup_id.clone_from(&set);
            r.archive.manifest_key = format!("kafka-backups/{set}/manifest.json");
            r.archive.manifest_sha256 = logweir_core::ids::sha256_prefixed(CATALOG_MANIFEST);
            if let Some(positions) = r.consumer_positions.as_mut() {
                positions.observed_from += shift;
                positions.observed_to += shift;
                positions.document.key =
                    logweir_core::consumer_positions::document_key(&set, &r.run_id);
            }
            assert_eq!(r.validate_invariants(), Ok(()));
            r
        } else {
            catalog_receipt(&set, "run-a", &started)
        };
        let receipt_bytes =
            logweir_core::det_json::to_deterministic_json(&receipt).expect("serialises");
        let sidecar = logweir_evidence::sign::sign_detached(
            key,
            logweir_evidence::PAYLOAD_TYPE_BACKUP_RECEIPT,
            &receipt_bytes,
        )
        .expect("the sidecar signs");
        let f = catalog_fixture(&receipt, "s3://lw-archive/kafka-backups", &sidecar, &key_id);
        largest = (
            largest.0.max(f.receipt_bytes.len()),
            largest.1.max(f.record_bytes.len()),
        );
        write(&f.log_key, &f.log_bytes);
        write(&f.record_key, &f.record_bytes);
        write(&f.receipt_key, &f.receipt_bytes);
        write(&f.sidecar_key, &f.sidecar_bytes);
        write(&f.manifest_key, CATALOG_MANIFEST);
    }
    let pem = key
        .verifying_key()
        .to_public_key_pem()
        .expect("a public key renders");
    std::fs::write(root.join("trust.pem"), pem).expect("the bundle is written");
    largest
}

fn fx33_walk_store(root: &Path) -> Arc<logweir_engine_oso::storage::Store> {
    Arc::new(
        logweir_engine_oso::storage::Store::read_only_from_url(
            &logweir_core::engine::StorageUrl::Filesystem {
                path: root.to_path_buf(),
            },
        )
        .expect("a filesystem handle over an existing directory builds"),
    )
}

/// The child's work: one `catalogSync` over the tree, as the check Job runs
/// one — or (the control) every point's record and receipt read and KEPT.
fn fx33_walk_child(mode: &str, root: &Path) {
    let store = fx33_walk_store(root);
    match mode {
        "walk" => {
            let pem = std::fs::read(root.join("trust.pem")).expect("the bundle");
            let wiring = FakeWiring {
                shared_store: Some(store),
                ..FakeWiring::default()
            }
            .with_file(FX33_WALK_BUNDLE, &pem);
            let request = logweir_core::check_contract::CatalogSyncRequest {
                trust_bundle_file: Some(FX33_WALK_BUNDLE.to_string()),
                ..sync_request()
            };
            let body = body_of(&drive_sync(request, &wiring));
            let counts = summary_of(&body, "catalog-counts=");
            assert_eq!(
                counts["signature"]["verified"], counts["available"],
                "every available point's signature was verified: {counts}"
            );
            assert_eq!(
                entries_of(&body).len() as u64,
                counts["total"].as_u64().expect("a total"),
                "every counted point is listed"
            );
            println!("{FX33_WALK_FACT_LINE}{}", counts["available"]);
        }
        // THE CONTROL: what a walk that accumulated its documents would hold.
        "hold" => {
            let records =
                ObjectAccess::list_page(&*store, "logweir/catalog/v1/points/", None, 1000)
                    .expect("the points list");
            let mut held = Vec::new();
            for key in records.iter().filter(|k| k.ends_with("/record.json")) {
                let record = ObjectAccess::get(&*store, key, u64::MAX).expect("a record");
                let logweir::catalog::reader::RecordVerdict::Point(point) =
                    logweir::catalog::reader::read_record(&record)
                else {
                    panic!("{key} is a record");
                };
                let receipt =
                    ObjectAccess::get(&*store, &point.receipt.key, u64::MAX).expect("a receipt");
                let typed: BackupReceipt = serde_json::from_slice(&receipt).expect("a receipt");
                held.push((record, point, receipt, typed));
            }
            println!("{FX33_WALK_FACT_LINE}{}", held.len());
        }
        other => panic!("unknown {FX33_WALK_CHILD_ENV} mode {other}"),
    }
}

/// Runs this test again in a child, in `mode`, over `root`: the child's own
/// peak resident set and the one number it reported.
fn fx33_walk_peak(mode: &str, root: &Path) -> (u64, u64) {
    let out = std::process::Command::new(std::env::current_exe().expect("this binary"))
        .args(["--exact", FX33_WALK_TEST, "--nocapture", "--test-threads=1"])
        .env(FX33_WALK_CHILD_ENV, mode)
        .env(FX33_WALK_ROOT_ENV, root)
        // The meter measures live memory, not an allocator's cache of freed
        // blocks (`crates/weirkeeper/tests/read_caps.rs`, review F2).
        .env("MallocLargeCache", "0")
        .env("MALLOC_MMAP_THRESHOLD_", "131072")
        .output()
        .expect("the child test process starts");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success(),
        "the {mode} child failed: {}\n{stdout}\n{}",
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );
    let number = |prefix: &str| -> u64 {
        stdout
            .lines()
            .find_map(|l| l.find(prefix).map(|at| &l[at + prefix.len()..]))
            .and_then(|v| {
                v.trim()
                    .split(|c: char| !c.is_ascii_digit())
                    .next()?
                    .parse()
                    .ok()
            })
            .unwrap_or_else(|| panic!("the {mode} child printed no {prefix} line:\n{stdout}"))
    };
    (number(FX33_WALK_PEAK_LINE), number(FX33_WALK_FACT_LINE))
}

/// **The catalog walk's peak memory is ONE point's, however many it walks.**
///
/// What a walk holds at once is bounded by construction (`examine`): the
/// parsed record of the point it is on, and beside it one of — the receipt's
/// bytes with their parse; the receipt's bytes with the signature's copy of
/// them; the manifest's bytes. Each is released before the next is read, and
/// the point's entry (under 1 KB without its topic list) is all that is kept
/// when the walk moves on.
///
/// Measured in child processes over points whose receipt is within one
/// percent of the largest Logweir writes, each signature VERIFIED:
///
/// - a walk of [`FX33_WALK_POINTS`] such points adds no more than a walk of
///   one, plus a fixed slack;
/// - a walk of one adds less than [`FX33_WALK_BOUND`];
/// - the CONTROL — the same documents read and kept — adds at least two
///   points' documents more than the walk of many does, so the meter sees an
///   accumulating walk.
///
/// Check Jobs state no memory limit in the chart (`check/job.rs`,
/// `resources: None`), so a namespace `LimitRange` is what applies; the
/// figure printed here is what a `catalogSync` Job needs above its baseline.
///
/// KILLS: a walk that keeps each point's record or receipt until the body is
/// rendered. (A read cap removed is
/// `a_counted_point_is_always_listed_with_its_own_reason`'s and
/// `a_catalog_walk_reads_every_document_under_its_cap`'s.)
#[test]
fn the_walks_peak_memory_is_one_points() {
    if let Ok(mode) = std::env::var(FX33_WALK_CHILD_ENV) {
        let root = PathBuf::from(std::env::var(FX33_WALK_ROOT_ENV).expect(FX33_WALK_ROOT_ENV));
        fx33_walk_child(&mode, &root);
        println!("{FX33_WALK_PEAK_LINE}{}", fx33_self_peak_rss());
        return;
    }
    let scratch = tempfile::tempdir().expect("a scratch directory");
    let key = logweir_evidence::keys::SigningKey::generate_p256();
    let (small, one, many) = (
        scratch.path().join("small"),
        scratch.path().join("one"),
        scratch.path().join("many"),
    );
    fx33_plant_walk(&small, FX33_WALK_POINTS, false, &key);
    let (receipt_len, record_len) = fx33_plant_walk(&one, 1, true, &key);
    fx33_plant_walk(&many, FX33_WALK_POINTS, true, &key);

    let (base, listed) = fx33_walk_peak("walk", &small);
    assert_eq!(
        listed, FX33_WALK_POINTS as u64,
        "the baseline walk lists its points"
    );
    let (peak_one, listed_one) = fx33_walk_peak("walk", &one);
    let (peak_many, listed_many) = fx33_walk_peak("walk", &many);
    let (peak_held, held) = fx33_walk_peak("hold", &many);
    assert_eq!(listed_one, 1, "the one large point is Available");
    assert_eq!(
        listed_many, FX33_WALK_POINTS as u64,
        "every large point is Available"
    );
    assert_eq!(held, FX33_WALK_POINTS as u64);
    let over = |peak: u64| peak.saturating_sub(base);
    let (one, many, held) = (over(peak_one), over(peak_many), over(peak_held));
    eprintln!(
        "[fx33-walk] points of a {receipt_len} B receipt and a {record_len} B record, each \
         signature verified. Baseline walk {base} B. A walk of one adds {one} B; a walk of \
         {FX33_WALK_POINTS} adds {many} B; the same {FX33_WALK_POINTS} read and kept add {held} B \
         (bound for one point {FX33_WALK_BOUND} B)"
    );
    // Allocator noise between two child processes, measured at up to 7 MB.
    let slack: u64 = 16 << 20;
    assert!(
        many < one + slack,
        "a walk of {FX33_WALK_POINTS} maximum-size points added {many} bytes and a walk of one \
         {one}; the walk may hold one point's documents at a time, never more"
    );
    assert!(
        one < FX33_WALK_BOUND,
        "a walk of one maximum-size point added {one} bytes (bound {FX33_WALK_BOUND})"
    );
    let documents = u64::try_from(receipt_len + record_len).expect("fits");
    assert!(
        held > many + 2 * documents,
        "the control added only {held} bytes against the walk's {many}; the meter cannot tell \
         a walk that accumulates from one that does not"
    );
}

/// The most resident memory the walk of ONE maximum-size point may add to a
/// baseline walk: 64 MiB.
///
/// What it is made of, at the caps: the record's bytes (at most 6,131,072),
/// the `serde_json::Value` `read_record` reads its version from, and its
/// typed parse, held while the point is examined; and beside them the larger
/// of the receipt's bytes with their typed parse, and the receipt's bytes
/// with the signature's pre-authentication copy (at most 5,131,072 each).
/// Measured with a 5.09 MB receipt and a 4.2 MB record: about 41 MB.
///
/// The MANIFEST is not in this number. The walk reads a point's manifest
/// whole to hash it, one at a time, under the runner's 256 MiB manifest cap
/// (FX-31); a real manifest is about 540 bytes a segment, and this row's is
/// 13 bytes.
const FX33_WALK_BOUND: u64 = 64 << 20;
