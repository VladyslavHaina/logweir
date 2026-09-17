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
    list_fault: Option<Fault>,
    put_fault: Option<Fault>,
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

    fn failing_list(self, f: Fault) -> Self {
        self.state.lock().unwrap().list_fault = Some(f);
        self
    }

    fn failing_put(self, f: Fault) -> Self {
        self.state.lock().unwrap().put_fault = Some(f);
        self
    }

    fn puts(&self) -> Vec<(String, Vec<u8>)> {
        self.state.lock().unwrap().puts.clone()
    }
}

impl ObjectAccess for FakeObjects {
    fn get(&self, key: &str) -> Result<Vec<u8>, StoreError> {
        let s = self.state.lock().unwrap();
        if let Some(f) = &s.get_fault {
            return Err(f.to_error(key));
        }
        s.objects
            .get(key)
            .cloned()
            .ok_or_else(|| StoreError::NotFound(key.to_string()))
    }

    fn list_bounded(&self, prefix: &str, max: usize) -> Result<Vec<String>, StoreError> {
        let s = self.state.lock().unwrap();
        if let Some(f) = &s.list_fault {
            return Err(f.to_error(prefix));
        }
        Ok(s.objects
            .keys()
            .filter(|k| k.starts_with(prefix))
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
        Ok(PutOutcome {
            version_id: None,
            create_only_enforced: true,
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
/// enum is externally tagged, so `{"catalogSync": …}` names a variant serde
/// does not know.
#[test]
fn an_unknown_plan_kind_is_refused() {
    let m = mount(&inventory_plan(100, 1 << 20));
    let mut doc: serde_json::Value = serde_json::from_slice(&m.bytes).unwrap();
    let inner = doc["request"]["topicInventory"].take();
    doc["request"] = serde_json::json!({ "catalogSync": inner });
    let bytes = serde_json::to_vec(&doc).unwrap();
    let sha = logweir_core::ids::sha256_prefixed(&bytes);
    let env = good_env(&sha);
    let err = load_with(&bytes, &as_pairs(&env), 1).expect_err("an unknown kind is refused");
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

/// **M1, the structural half.** Steps 1-4 build nothing, and `run` reaches a
/// kind only through a verified plan.
///
/// The scan is over the BODIES of `load` and `runner_bounds` — brace-counted,
/// because "this function opens nothing" is a claim about a body and a
/// line-based scan cannot tell which function a line is inside. It forbids the
/// constructors AND this crate's own wrappers around them, which is the
/// difference between a guard and a tripwire: the first version listed only
/// `KafkaInventory::connect(` and a planted `kafka::dial(` walked straight
/// past it.
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

    // (a) The two functions that ARE steps 1-4 open nothing, name no
    //     constructor, and reach no wrapper around one.
    for (sig, body) in &bodies {
        if !(sig.starts_with("pub fn load(") || sig.starts_with("pub fn runner_bounds(")) {
            continue;
        }
        for token in [
            "KafkaInventory::connect(",
            "RdKafkaReader::connect(",
            "Store::read_only_with(",
            "Store::from_url_with(",
            "AuthConfig::from_spec",
            "kafka::dial(",
            "store::open_read(",
            "store::open_evidence_write(",
            "kinds::Live",
            "kinds::run_kind",
            "run_kind_with(",
            "execute_with(",
            "Wiring",
        ] {
            assert!(
                !body.contains(token),
                "`{sig}` holds D2 §4.2's startup order and names `{token}`; no client may be \
                 built, and no kind may run, before the plan verifies"
            );
        }
    }

    // (b) `run` reaches a kind only after `load` returned Ok, and it does so
    //     through a `&Loaded` that only `load` produces.
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
const MANIFEST_KEY: &str = "kafka-backups/20260915T030000Z/manifest.json";
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
        .with_prefix("kafka-backups")
        .with_object(MANIFEST_KEY, &serde_json::to_vec(manifest).unwrap());
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
        .with_prefix("kafka-backups")
        .with_object(MANIFEST_KEY, &serde_json::to_vec(&manifest).unwrap())
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
    assert!(
        matches!(
            row.code,
            CheckCode::EndpointUnreachable | CheckCode::Timeout | CheckCode::StoreErrorUnclassified
        ),
        "a closed port is a transport answer, not a permissions one: {row:?}"
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

    /// An `evidenceFetch` of a receipt a REAL `logweir backup run` wrote.
    #[test]
    fn an_evidence_fetch_relays_a_receipt_a_real_backup_wrote() {
        std::env::set_var("AWS_ACCESS_KEY_ID", MINIO_USER);
        std::env::set_var(MINIO_PASSWORD_VAR, "minioadmin");
        std::env::set_var("AWS_REGION", "us-east-1");
        let dir = tempfile::tempdir().unwrap();
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
                "  topics: [test-topic]\n",
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
            bootstrap = plain_bootstrap(),
            endpoint = s3_endpoint()
        );
        std::fs::write(dir.path().join("backup.yaml"), spec).unwrap();
        std::fs::write(
            dir.path().join("allowed.json"),
            br#"{"allowed_cluster_ids":[],"refuse_if_source":true}"#,
        )
        .unwrap();

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
        cmd.env("LOGWEIR_ENGINE", repo_root().join(".engine/kafka-backup"));
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
