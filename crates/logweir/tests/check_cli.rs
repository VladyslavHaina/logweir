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
    EvidenceFetchRequest, EvidenceObjectRequest, FrameExpectations, Gating,
    OperationReadinessRequest, RestorePreflightRequest, Stream, TopicEntry, TopicInventoryRequest,
    CHECK_CONTRACT_VERSION, CHECK_PLAN_CONTRACT,
};
use logweir_core::destination::{
    Addressing, DestinationLocation, DestinationRole, StorageProvider, TransportSecurity,
};
use logweir_engine_oso::storage::{PutOutcome, StoreError};
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
    prefix: String,
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

    fn failing_get(self, f: Fault) -> Self {
        self.state.lock().unwrap().get_fault = Some(f);
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
}

impl ObjectAccess for FakeObjects {
    fn get(&self, key: &str) -> Result<Vec<u8>, StoreError> {
        let s = self.state.lock().unwrap();
        if let Some(f) = s.key_faults.get(key) {
            return Err(f.to_error(key));
        }
        if let Some(f) = &s.get_fault {
            return Err(f.to_error(key));
        }
        s.objects
            .get(key)
            .cloned()
            .ok_or_else(|| StoreError::NotFound(key.to_string()))
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
        let s = self.state.lock().unwrap();
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
}

#[derive(Clone, Default)]
struct FakeProbe {
    state: Arc<Mutex<ProbeState>>,
}

impl FakeProbe {
    fn new() -> Self {
        let p = Self::default();
        p.state.lock().unwrap().cluster_id = Some("M29I2S7FQPyHBEX12Vx7XA".to_string());
        p.state.lock().unwrap().listing.broker_count = 3;
        p
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

    fn topic_configs(&self, _name: &str) -> Result<BTreeMap<String, String>, CheckFailure> {
        Ok(BTreeMap::new())
    }

    fn broker_configs(&self) -> Result<BTreeMap<String, String>, CheckFailure> {
        let s = self.state.lock().unwrap();
        match &s.broker_configs_fault {
            Some((c, m)) => Err(CheckFailure::new(*c, m)),
            None => Ok(s.broker_configs.clone()),
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
    signer: Option<Result<String, String>>,
    files: BTreeMap<String, Vec<u8>>,
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
        _budget: std::time::Duration,
    ) -> Result<Box<dyn ObjectAccess>, StoreFailure> {
        if let Some((c, m)) = &self.writer_fault {
            return Err(StoreFailure::new(*c, m.clone()));
        }
        Ok(Box::new(self.writer.clone().unwrap_or_default()))
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

fn access_plan(roles: Vec<DestinationRole>, write_probe: bool) -> CheckPlan {
    plan_of(CheckRequest::DestinationAccess(DestinationAccessRequest {
        destination: destination(),
        roles,
        write_probe,
    }))
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
    let denied = FakeObjects::new().failing_get(Fault::Io(
        "Generic S3 error: Error performing GET: response error \"<Error><Code>AccessDenied</Code>\
         </Error>\", after 0 retries: HTTP status client error (403 Forbidden)"
            .to_string(),
    ));
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
            FakeObjects::new().failing_list(Fault::Io(
                "Generic S3 error: <Error><Code>SignatureDoesNotMatch</Code></Error>".to_string(),
            )),
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
            FakeObjects::new().failing_put(Fault::Io(
                "Generic S3 error: <Error><Code>AccessDenied</Code></Error> (403 Forbidden)"
                    .to_string(),
            )),
        ),
    );
    let row = run.row(CheckId::DestinationEvidenceWritable);
    assert_eq!(row.code, CheckCode::AccessDenied);
    assert_eq!(row.state, CheckState::NotReady);
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
            skip_checks: Vec::new(),
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
            FakeObjects::new().failing_get(Fault::Io(
                "Generic S3 error: <Error><Code>AccessDenied</Code></Error>".to_string(),
            )),
        ),
    );
    let e = &run.result().evidence[0];
    assert!(!e.present);
    assert_eq!(e.code, Some(CheckCode::AccessDenied));
}

#[test]
fn an_evidence_fetch_truncates_at_max_bytes_and_says_so() {
    let key = "logweir/backups/20260916/receipt.json";
    let body = vec![b'x'; 4096];
    let m = mount(&fetch_plan(key, 100));
    let run = drive(
        &m,
        &FakeWiring::default().with_role(
            DestinationRole::EvidenceRead,
            FakeObjects::new().with_object(key, &body),
        ),
    );
    let e = &run.result().evidence[0];
    assert!(e.truncated, "a prefix is not the object");
    assert_eq!(e.bytes, Some(100));
    assert_eq!(
        run.relay
            .as_ref()
            .unwrap()
            .stream(Stream::EvidencePayload)
            .unwrap()
            .len(),
        100
    );
    assert_eq!(
        e.sha256.as_deref(),
        Some(logweir_core::ids::sha256_prefixed(&body[..100]).as_str()),
        "the declared digest is the digest of what was RELAYED"
    );
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
                .with_broker_config("log.message.timestamp.difference.max.ms", "3600000"),
        ),
    );
    assert_eq!(
        run.row(CheckId::TargetTimestampBound).code,
        CheckCode::TimestampBoundExceeded
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
        .manifest_facts(&format!("kafka-backups/{BACKUP_ID}/manifest.json"))
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
    let plain = "Generic S3 error: <Error><Code>AccessDenied</Code></Error> after 3 attempts";
    assert_eq!(logweir::check::store::strip_retry_noise(plain), plain);
    assert_eq!(
        logweir::check::store::classify(&StoreError::Io(plain.to_string())),
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
                FakeObjects::new().failing_get(Fault::Io(
                    "Generic S3 error: <Error><Code>AccessDenied</Code></Error>".to_string(),
                )),
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
            prefix: "kafka-backups".to_string(),
        },
        records: BTreeMap::from([("orders".to_string(), 1234u64)]),
        covered: logweir_core::backup_receipt::ReceiptCovered {
            from_ms: started_at.timestamp_millis() - 3_600_000,
            to_ms: started_at.timestamp_millis(),
        },
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
    let denied = objects.clone().failing_get(Fault::Io(
        "Generic S3 error: Error performing GET: response error \"<Error><Code>AccessDenied\
         </Code></Error>\", status: 403 Forbidden"
            .to_string(),
    ));
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
    assert!(
        entries_of(&body).is_empty(),
        "a point whose record could not be read has no receipt-derived facts to publish, so it \
         is counted and not listed: {body}"
    );
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
    assert_eq!(entries_of(&body).len(), 1);
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
    let denied = FakeObjects::new().failing_list(Fault::Io(
        "Generic S3 error: Error performing LIST: response error \"<Error><Code>AccessDenied\
         </Code></Error>\", status: 403 Forbidden"
            .to_string(),
    ));
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
    let denied = Fault::Io(
        "Generic S3 error: Error performing GET: response error \"<Error><Code>AccessDenied\
         </Code></Error>\", status: 403 Forbidden"
            .to_string(),
    );

    let cases: Vec<(&str, String, Fault, &str, &str)> = vec![
        (
            "the record is absent",
            f.record_key.clone(),
            Fault::NotFound,
            "Missing",
            "notAttempted",
        ),
        (
            "the record is denied",
            f.record_key.clone(),
            denied.clone(),
            "Unreadable",
            "notAttempted",
        ),
        (
            "the receipt is absent",
            f.receipt_key.clone(),
            Fault::NotFound,
            "Missing",
            "notAttempted",
        ),
        (
            "the receipt is denied",
            f.receipt_key.clone(),
            denied.clone(),
            "Unreadable",
            "notAttempted",
        ),
        (
            "the sidecar is absent",
            f.sidecar_key.clone(),
            Fault::NotFound,
            "Available",
            "noEvidence",
        ),
        (
            "the sidecar is denied",
            f.sidecar_key.clone(),
            denied.clone(),
            "Available",
            "notAttempted",
        ),
        (
            "the manifest is absent",
            f.manifest_key.clone(),
            Fault::NotFound,
            "Missing",
            "notAttempted",
        ),
        (
            "the manifest is denied",
            f.manifest_key.clone(),
            denied,
            "Unreadable",
            "notAttempted",
        ),
    ];
    for (what, key, fault, availability, signature) in cases {
        let objects = place(FakeObjects::new(), &f).failing_key(&key, fault);
        let run = drive_sync(
            sync_request(),
            &FakeWiring::default().with_role(DestinationRole::ArchiveRead, objects),
        );
        let body = body_of(&run);
        let counts = summary_of(&body, "catalog-counts=");
        assert_eq!(counts["total"], 1, "{what}: {body}");
        // A point whose RECORD could not be read has no receipt-derived facts
        // to publish, so it is counted and not listed.
        if let Some(entry) = entries_of(&body).first() {
            assert_eq!(entry["availability"], availability, "{what}: {entry}");
            assert_eq!(entry["signature"], signature, "{what}: {entry}");
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

/// **F4, the size half (question F12).** A document larger than a record could
/// ever be is `Unreadable`, not parsed.
#[test]
fn an_oversized_catalog_document_is_unreadable_and_is_not_parsed() {
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
    let mut doc: serde_json::Value =
        serde_json::from_slice(&f.record_bytes).expect("the record is JSON");
    doc["e2e_padding"] = serde_json::json!(
        "x".repeat(logweir::check::kinds::catalog_sync::MAX_CATALOG_DOCUMENT_BYTES)
    );
    let huge = serde_json::to_vec(&doc).expect("JSON");
    assert!(huge.len() > logweir::check::kinds::catalog_sync::MAX_CATALOG_DOCUMENT_BYTES);
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
    assert_eq!(
        summary_of(&body, "catalog-counts=")["unreadable"],
        1,
        "{body}"
    );
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
catalog-page=1/1 count=1 sha256=9198b1b1f3c4a77fd1788aa8ec661b8c1b2fbf585405d84c80210b484eb52376
catalog-entry={"pointId":"lwp1-0e02dc33bf63349ec262a62043d9bd04","backupId":"set-a","runId":"run-a","recoveryPointAtMs":1789527600000,"coveredFromMs":1789524000000,"coveredToMs":1789527600000,"locations":[{"locationId":"s3://lw-archive/kafka-backups","availability":"Available"}],"receiptKey":"logweir/backups/set-a/run-a.receipt.json","receiptSha256":"sha256:0e02dc33bf63349ec262a62043d9bd0441fb867c72aa2d5b4ce8a085b05469db","manifestKey":"kafka-backups/set-a/manifest.json","manifestSha256":"sha256:d5eea23a2f7ca3f36d2a5dbf3ab2532a3de3a797ded388afb816068c2863a152","recordedAt":"2026-09-16T06:00:00Z","formatVersion":"1.0.0","availability":"Available","signature":"notAttempted","signerKeyId":"0f1e2d3c4b5a69788796a5b4c3d2e1f00f1e2d3c4b5a69788796a5b4c3d2e1f0","remedy":"No signature verdict was reached: this installation holds no key that signed this point. Add the signing key to the trust source if you accept evidence from it."}
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
