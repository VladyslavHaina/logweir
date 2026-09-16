//! Bounded, read-only Kafka probes for the check contract — D2 **W3**.
//!
//! # What this module is, and what it deliberately is not
//!
//! [`crate::reader::ClusterReader`] is the DRILL's view of a cluster: one
//! twenty-second timeout ([`crate::rdkafka_reader`]'s `const T`) shared by
//! every call, errors flattened into [`crate::reader::KafkaError`]'s four
//! strings, and a `list_topics` that returns everything the broker named. That
//! is the right shape for a backup or a restore, which either work or fail.
//!
//! A CHECK is a different question. It has a wall-clock budget it must not
//! overrun (D2 §4.2's `timeoutSeconds`), it has to say WHY a call failed in the
//! closed [`CheckCode`] vocabulary rather than in prose, and — the part that
//! decides whether an operator is told the truth — it has to distinguish "the
//! broker refused this principal" from "nothing answered in time". librdkafka
//! reports a SASL or TLS failure through the client's ERROR CALLBACK and then
//! lets the metadata request time out, so a reader that looks only at the
//! call's return value reports `MetadataTimeout` for a wrong password
//! (D2 §4.2 **[VERIFY U5]**). [`FaultLog`] is this module's answer: the
//! callback records what it saw, and [`escalate`] lets the observed fault win
//! over the call's own generic outcome.
//!
//! # Nothing here writes
//!
//! Every call is metadata, `DescribeConfigs`, or `CreateTopics` with
//! [`rdkafka::admin::AdminOptions::validate_only`] set. No topic is created,
//! altered or deleted, nothing is produced, and no consumer group is joined —
//! [`KafkaInventory`] builds a consumer whose only use is metadata. D2 §6.7
//! G12: preflight never creates a topic.
//!
//! # The layering, and the seam that is still open
//!
//! The types and the assembly ([`Listing`], [`assemble`], [`collect`],
//! [`FaultLog`]) carry no rdkafka and compile with `--no-default-features`, so
//! every rule below is unit-testable with no broker. The rdkafka half is behind
//! the `client` feature and is [`KafkaInventory`].
//!
//! **TODO (D2 §13.2, W3's deferred edit).** [`KafkaInventory::connect`] builds
//! its own consumer and admin client. Once PLAT-07.1 has landed its TLS changes
//! in [`crate::rdkafka_reader`], that file gains a `pub(crate)` accessor for its
//! own two clients and this constructor becomes a borrow of them, so a check
//! that runs beside a drill opens one connection instead of two. The accessor
//! is not written here because `rdkafka_reader.rs` is PLAT-07.1's file for the
//! duration of that rebase. What must move with it: the `security.protocol` /
//! `sasl.*` block in [`client_config`], which is a second copy of the mapping
//! `RdKafkaReader::connect` already performs.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use logweir_core::check_contract::{
    redact, CheckCode, ExpectedSummary, ExpectedTopicResult, ExpectedTopicState, InventoryCounts,
    InventoryResult, TopicEntry, TruncationReason, TOPIC_FRAME_PREFIX, TOPIC_INVENTORY_FORMAT,
};

// ---------------------------------------------------------------------------
// Failure
// ---------------------------------------------------------------------------

/// A check-shaped failure: a closed code and a message that has already been
/// through [`redact`].
///
/// A RAW BROKER STRING NEVER LEAVES THIS MODULE. D2 §4.2: "Raw errors are never
/// printed, only codes plus a redacted message". librdkafka's error reasons
/// carry the broker list, and a SASL failure's reason has been observed to
/// carry the mechanism and the principal; [`CheckFailure::new`] is the ONE
/// constructor, and it redacts, so a caller cannot build one that has not been
/// through the rules.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckFailure {
    /// The closed code a check outcome carries.
    pub code: CheckCode,
    /// A redacted, length-capped explanation. Never a raw broker message.
    pub message: String,
}

impl CheckFailure {
    /// The one constructor. `message` is redacted and capped here, not by the
    /// caller.
    #[must_use]
    pub fn new(code: CheckCode, message: impl AsRef<str>) -> Self {
        Self {
            code,
            message: redact(message.as_ref()),
        }
    }
}

impl std::fmt::Display for CheckFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for CheckFailure {}

// ---------------------------------------------------------------------------
// The fault log — D2 §4.2 [VERIFY U5]
// ---------------------------------------------------------------------------

/// How strongly an observed fault outranks the outcome of the call it happened
/// during. Higher wins.
///
/// THE ORDER IS THE WHOLE POINT. A metadata request against a broker that
/// refused the credential returns `MetadataTimeout`, because librdkafka keeps
/// retrying until the caller's deadline; the ONLY place the refusal appears is
/// the error callback. So an authentication fault must outrank a timeout, a
/// TLS trust failure must outrank a transport failure (a certificate the client
/// will not accept looks exactly like a broker that hung up), and an
/// authorization failure must outrank both of the generic transport codes.
///
/// `0` is "not a fault this table ranks", which means it never displaces
/// anything.
#[must_use]
pub fn fault_rank(code: CheckCode) -> u8 {
    match code {
        CheckCode::AuthenticationFailed => 6,
        CheckCode::TlsTrustFailed => 5,
        CheckCode::TlsHandshakeFailed => 4,
        CheckCode::ClusterAuthorizationFailed | CheckCode::TopicAuthorizationFailed => 3,
        CheckCode::BrokerUnreachable => 2,
        CheckCode::MetadataTimeout => 1,
        _ => 0,
    }
}

/// The code a call reports, given its own outcome and the strongest fault the
/// error callback observed while it ran.
///
/// The observed fault wins only when it outranks the call's own code — so a
/// call that failed with `TopicAuthorizationFailed` is not downgraded to
/// `BrokerUnreachable` by a stale transport blip, and a call that "merely"
/// timed out while the callback reported a SASL refusal is reported as
/// `AuthenticationFailed`.
#[must_use]
pub fn escalate(base: CheckCode, observed: Option<CheckCode>) -> CheckCode {
    match observed {
        Some(o) if fault_rank(o) > fault_rank(base) => o,
        _ => base,
    }
}

/// Everything librdkafka's error callback said, classified as it arrived.
///
/// # ONLY THE CONSUMER EVER FILLS THIS, AND THAT IS AN rdkafka 0.36 FACT
///
/// The log is shared by both handles of one [`KafkaInventory`] because a SASL
/// refusal is a fact about the same connection whichever handle saw it. But in
/// rdkafka 0.36.2 the **admin client's context is inert**:
/// `ClientContext::error` is invoked only from `Client::poll_event`
/// (`src/client.rs:281`, `:292`, `:334`), which is reached only from
/// `BaseConsumer::poll_queue` (`src/consumer/base_consumer.rs:131`) and
/// `BaseProducer::poll` (`src/producer/base_producer.rs:365`);
/// `AdminClient::from_config_and_context` (`src/admin.rs:344-365`) registers no
/// events on its context at all, and its polling thread routes the raw
/// `NativeQueue` by event opaque without ever consulting it. rdkafka registers
/// no native `error_cb` anywhere, so there is no third path.
///
/// So nothing an ADMIN call does can ever fill this log directly. What saves
/// an admin-first failure — `topic_configs` or `validate_create_topics` as the
/// FIRST call on a fresh connection, which the [`InventoryProbe`] seam permits
/// — is that librdkafka connects the CONSUMER handle to its bootstrap brokers
/// eagerly at `rd_kafka_new`, so the consumer's callback observes the same
/// refusal and [`KafkaInventory::fail`]'s drain serves it. That is measured,
/// not assumed: see `KafkaInventory::dial_consumer_if_silent`, which removes
/// the dependence on that timing and records what each half is worth.
///
/// It holds at most [`FaultLog::MAX_ENTRIES`] entries: the callback fires per
/// retry, and an unbounded log would grow for the whole budget of a check
/// against a broker that is down.
#[derive(Debug, Default)]
pub struct FaultLog {
    inner: std::sync::Mutex<Vec<(CheckCode, String)>>,
}

impl FaultLog {
    /// The cap. A librdkafka error callback fires once per broker per retry;
    /// sixty-four is far more than enough to see every distinct cause and small
    /// enough that a fifteen-minute check cannot grow it without bound.
    pub const MAX_ENTRIES: usize = 64;

    /// A fresh, empty log.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record one classified fault. `reason` is redacted here.
    ///
    /// Codes [`fault_rank`] does not rank are dropped rather than stored: the
    /// log exists to answer "what outranks this call's own outcome", and an
    /// unrankable entry can never be that.
    pub fn record(&self, code: CheckCode, reason: &str) {
        if fault_rank(code) == 0 {
            return;
        }
        let mut guard = match self.inner.lock() {
            Ok(g) => g,
            // A poisoned mutex means a previous holder panicked. The log is
            // advisory — dropping a fault degrades a code, it never invents
            // one — so this recovers rather than propagating a panic out of a
            // librdkafka callback, where an unwind would cross an FFI frame.
            Err(poisoned) => poisoned.into_inner(),
        };
        if guard.len() >= Self::MAX_ENTRIES {
            return;
        }
        guard.push((code, redact(reason)));
    }

    /// The highest-ranked fault observed so far, with its redacted reason.
    #[must_use]
    pub fn strongest(&self) -> Option<(CheckCode, String)> {
        let guard = match self.inner.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard
            .iter()
            .max_by_key(|(code, _)| fault_rank(*code))
            .cloned()
    }

    /// Every distinct code observed, in rank order (strongest first). For a
    /// report, never for a decision.
    #[must_use]
    pub fn codes(&self) -> Vec<CheckCode> {
        let guard = match self.inner.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        let distinct: BTreeSet<CheckCode> = guard.iter().map(|(c, _)| *c).collect();
        let mut codes: Vec<CheckCode> = distinct.into_iter().collect();
        codes.sort_by_key(|c| std::cmp::Reverse(fault_rank(*c)));
        codes
    }

    /// Whether anything at all has been recorded.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        let guard = match self.inner.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.is_empty()
    }

    /// Apply the log to one failing call: the reported code is
    /// [`escalate`]d, and the message names the observed fault when it was the
    /// one that decided.
    #[must_use]
    pub fn explain(&self, base: CheckCode, what: &str) -> CheckFailure {
        match self.strongest() {
            Some((observed, reason)) if fault_rank(observed) > fault_rank(base) => {
                CheckFailure::new(
                    observed,
                    format!(
                        "{what} reported {base}, but the client's error callback observed \
                     {observed} first: {reason}"
                    ),
                )
            }
            _ => CheckFailure::new(base, format!("{what} reported {base}")),
        }
    }
}

// ---------------------------------------------------------------------------
// The probe seam
// ---------------------------------------------------------------------------

/// What a targeted metadata request said about one name.
///
/// Four answers and no fifth, because the broker gives exactly these: metadata
/// (present), `TOPIC_AUTHORIZATION_FAILED` (which it returns whether or not the
/// topic exists, so it says NOTHING about existence), `UNKNOWN_TOPIC_OR_PARTITION`
/// (absent), and no answer within the budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopicPresence {
    /// The broker returned metadata for the name.
    Present {
        /// Its partition count.
        partitions: u32,
    },
    /// `TOPIC_AUTHORIZATION_FAILED`. Existence is unknown and stays unknown.
    NotAuthorized,
    /// `UNKNOWN_TOPIC_OR_PARTITION`.
    NotFound,
    /// No answer within this name's slice of the budget.
    Unknown,
}

impl TopicPresence {
    /// The answer a per-topic metadata error carries — **pure**, and the only
    /// place that decision is made.
    ///
    /// # Why this is a function and not three arms inside `describe_topic`
    ///
    /// The `TopicAuthorizationFailed` arm is signal (ii) of D2 §5.4: it is the
    /// ONLY thing that turns an ACL-limited principal into visibility
    /// `limited` rather than `unknown`, and "permission-limited" is a named
    /// PLAT-09.1 distinguishable outcome. Inside `describe_topic` it was
    /// reachable only from a real broker with real ACLs, which the compose
    /// stack does not have — an independent review planted
    /// `TopicAuthorizationFailed => Unknown` there and it **survived the whole
    /// suite**. A regression in that direction reports an ACL-restricted
    /// cluster as merely unknown, which is the softer and wrong answer.
    ///
    /// Everything else — a leader election, a transient broker state, a code
    /// this build does not model — is [`TopicPresence::Unknown`] and never
    /// [`TopicPresence::NotFound`]: "I could not tell" must not be rendered as
    /// "it is not there".
    #[must_use]
    pub fn of_error(code: CheckCode) -> Self {
        match code {
            CheckCode::TopicAuthorizationFailed => Self::NotAuthorized,
            CheckCode::UnknownTopicOrPartition => Self::NotFound,
            _ => Self::Unknown,
        }
    }

    /// The contract's spelling of this answer.
    #[must_use]
    pub fn expected_state(self) -> ExpectedTopicState {
        match self {
            Self::Present { .. } => ExpectedTopicState::Visible,
            Self::NotAuthorized => ExpectedTopicState::NotAuthorized,
            Self::NotFound => ExpectedTopicState::NotFound,
            Self::Unknown => ExpectedTopicState::Unknown,
        }
    }
}

/// One entry of an all-topics metadata listing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedTopic {
    /// The topic name, exactly as the broker spelled it.
    pub name: String,
    /// Its partition count. `0` and meaningless when `error` is set.
    pub partitions: u32,
    /// The per-entry metadata error, classified. `None` is a healthy entry.
    ///
    /// AN ERRORED ENTRY IS NEVER DROPPED, for the reason
    /// [`crate::reader::TopicMeta`] records: `LeaderNotAvailable` during a
    /// leader election would silently shrink the listing, and a completeness
    /// claim built on a shrunken listing is worse than no claim.
    pub error: Option<CheckCode>,
}

/// One all-topics metadata read.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Listing {
    /// `metadata.brokers.len()`. D2 §6.3's `connection.authenticated` fact.
    pub broker_count: u32,
    /// Every entry the broker named, in the order it named them.
    pub topics: Vec<ListedTopic>,
}

/// The result of one validate-only `CreateTopics` entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicCreateOutcome {
    /// The name that was validated.
    pub name: String,
    /// [`CheckCode::TopicCreateValidated`] or the refusal.
    pub code: CheckCode,
    /// A redacted note. Empty when the validation passed.
    pub message: String,
}

/// The bounded, read-only broker surface a check needs.
///
/// A TRAIT AND NOT A STRUCT, so every rule in [`collect`] is testable against a
/// fake with no socket (`crates/logweir/tests/no_network_in_unit_tests.rs`), and
/// so the runner (D2 W4) can be written against the seam before PLAT-07.1's
/// `rdkafka_reader.rs` edit lands.
///
/// **Every method is time-bounded by the implementation**, not by the caller:
/// the timeout belongs to the client that issues the request, and a trait whose
/// caller passed one could be implemented by ignoring it. [`ProbeTimeouts`] is
/// what [`KafkaInventory`] is built with.
pub trait InventoryProbe {
    /// `rd_kafka_clusterid`. `None` when the broker named none.
    ///
    /// # Errors
    /// [`CheckFailure`] with a Kafka code.
    fn cluster_id(&self) -> Result<Option<String>, CheckFailure>;

    /// All-topics metadata, plus the broker count off the same response.
    ///
    /// # Errors
    /// [`CheckFailure`] with a Kafka code.
    fn list_topics(&self) -> Result<Listing, CheckFailure>;

    /// Metadata for ONE name — D2 §5.2 step 5, the probe that can distinguish
    /// "not authorized" from "absent".
    ///
    /// # Errors
    /// [`CheckFailure`] only for a failure that is not about this topic; a
    /// per-topic answer is a [`TopicPresence`], including
    /// [`TopicPresence::Unknown`].
    fn describe_topic(&self, name: &str) -> Result<TopicPresence, CheckFailure>;

    /// `DescribeConfigs` on a topic resource.
    ///
    /// # Errors
    /// [`CheckFailure`] with a Kafka code.
    fn topic_configs(&self, name: &str) -> Result<BTreeMap<String, String>, CheckFailure>;

    /// `DescribeConfigs` on the first broker metadata names — D2 §6.3's
    /// `target.timestampBound`, whose unknown code is `BrokerConfigsNotReadable`.
    ///
    /// # Errors
    /// [`CheckFailure`] with a Kafka code.
    fn broker_configs(&self) -> Result<BTreeMap<String, String>, CheckFailure>;

    /// `CreateTopics` with `validate_only = true` — D2 §6.7(b).
    ///
    /// **This never creates a topic.** An implementation that dropped the flag
    /// would create every mapped target topic of a restore during its
    /// preflight, which G12 forbids;
    /// `validate_only_is_set_on_every_create_request` is the guard.
    ///
    /// # Errors
    /// [`CheckFailure`] when the request itself failed; a per-topic refusal is
    /// a [`TopicCreateOutcome`].
    fn validate_create_topics(
        &self,
        specs: &[crate::reader::NewTopicSpec],
    ) -> Result<Vec<TopicCreateOutcome>, CheckFailure>;
}

// ---------------------------------------------------------------------------
// Timeouts
// ---------------------------------------------------------------------------

/// The per-call timeout for a targeted metadata request — D2 §5.2 step 5's
/// "one name at a time with a 2 s timeout".
pub const TARGETED_METADATA_TIMEOUT: Duration = Duration::from_secs(2);

/// The fraction of a check's whole budget the targeted expected-topic probe may
/// spend — D2 §5.2 step 5.
pub const TARGETED_BUDGET_NUMERATOR: u32 = 2;
/// The denominator of [`TARGETED_BUDGET_NUMERATOR`] (40 % = 2/5).
pub const TARGETED_BUDGET_DENOMINATOR: u32 = 5;

/// Every timeout one check's Kafka calls run under.
///
/// EXPLICIT AND PER-CALL, not one shared constant. `crate::rdkafka_reader`'s
/// `const T` is twenty seconds for everything, which is right for a drill and
/// wrong for a check: a `topicInventory` with `timeoutSeconds: 30` that spent
/// twenty of them on a single `DescribeConfigs` would report `MetadataTimeout`
/// for the whole inventory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProbeTimeouts {
    /// `fetch_cluster_id` and the all-topics `fetch_metadata`.
    pub metadata: Duration,
    /// One targeted `fetch_metadata`.
    pub targeted_metadata: Duration,
    /// One `DescribeConfigs` round trip.
    pub describe_configs: Duration,
    /// One validate-only `CreateTopics` round trip.
    pub create_topics: Duration,
}

impl ProbeTimeouts {
    /// The timeouts for a check whose whole budget is `total`.
    ///
    /// Each per-call timeout is at most half the budget and at least one
    /// second, so a very short budget still issues a request rather than
    /// timing out at zero, and a very long one does not let a single hung
    /// request eat the whole thing.
    #[must_use]
    pub fn for_budget(total: Duration) -> Self {
        let half = total / 2;
        let bound = |d: Duration| d.clamp(Duration::from_secs(1), half.max(Duration::from_secs(1)));
        Self {
            metadata: bound(Duration::from_secs(10)),
            targeted_metadata: bound(TARGETED_METADATA_TIMEOUT),
            describe_configs: bound(Duration::from_secs(10)),
            create_topics: bound(Duration::from_secs(15)),
        }
    }

    /// How long the targeted expected-topic probe may run in total — 40 % of
    /// the check's budget (D2 §5.2 step 5).
    #[must_use]
    pub fn targeted_budget(total: Duration) -> Duration {
        total * TARGETED_BUDGET_NUMERATOR / TARGETED_BUDGET_DENOMINATOR
    }
}

impl Default for ProbeTimeouts {
    fn default() -> Self {
        Self::for_budget(Duration::from_secs(120))
    }
}

// ---------------------------------------------------------------------------
// The inventory algorithm (pure)
// ---------------------------------------------------------------------------

/// What [`collect`] was asked for — the Kafka-side projection of
/// `logweir_core::check_contract::TopicInventoryRequest`.
///
/// A SEPARATE TYPE rather than the request itself: the request carries a
/// `ConnectionPlan` with a credential REFERENCE, and nothing in this crate
/// should be able to reach one. This is the part of the request that is about
/// topics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InventoryRequest {
    /// Return internal topics instead of excluding and counting them.
    pub include_internal: bool,
    /// Names the caller expects to exist, each probed individually when it is
    /// absent from the listing.
    pub expected_topics: Vec<String>,
    /// The hard cap on relayed entries.
    pub max_topics: u32,
    /// The cap on relayed topic-frame bytes (line plus frame prefix).
    pub relay_budget_bytes: u64,
}

/// One assembled inventory: the entries to relay, and the result document's
/// inventory block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Inventory {
    /// Byte-order sorted, internal-filtered, truncated. The topic frames are
    /// exactly these, in this order.
    pub entries: Vec<TopicEntry>,
    /// Counts, signals and digests. Carries **no** visibility verdict: that is
    /// the controller's, because only it holds the attestation (D-SEAMS S3).
    pub result: InventoryResult,
}

/// The expected names that the listing did not contain — the ones step 5 probes
/// individually.
///
/// Pure, and separate from [`collect`], so "which names get a targeted request"
/// is assertable without a probe at all. Order is the caller's, de-duplicated,
/// because the budget is spent in this order and a duplicate name would spend
/// it twice.
#[must_use]
pub fn missing_expected(listing: &Listing, expected: &[String]) -> Vec<String> {
    let present: BTreeSet<&str> = listing.topics.iter().map(|t| t.name.as_str()).collect();
    let mut seen: BTreeSet<&str> = BTreeSet::new();
    expected
        .iter()
        .filter(|n| !present.contains(n.as_str()))
        .filter(|n| seen.insert(n.as_str()))
        .cloned()
        .collect()
}

/// The bytes one entry costs against the relay budget: its canonical TSV line
/// plus the topic frame's prefix (D2 §5.5).
#[must_use]
pub fn relay_cost(entry: &TopicEntry) -> u64 {
    (TOPIC_FRAME_PREFIX.len() + entry.tsv_line().len()) as u64
}

/// Build the inventory from a listing plus the answers to the targeted probes.
///
/// PURE, and the whole of D2 §5.2 steps 4, 6 and 7 that is not I/O:
///
/// 1. every listed entry becomes a [`TopicEntry`], carrying its per-entry error
///    as `error:<Code>` and its `internal` flag from the `__` rule;
/// 2. an expected name the listing DID show is flagged `expected`;
/// 3. an expected name the listing did not show, which a targeted request then
///    answered with metadata, is ADDED as an entry — the broker described it,
///    so it exists and the caller asked for it. A name that came back
///    `NotAuthorized`, `NotFound` or `Unknown` is **not** added: the TSV is
///    what the cluster showed, and a line for a topic nobody could describe
///    would be a claim this probe cannot make. It is reported in
///    `expectedResults` and counted in `expected`, which is where a consumer
///    looks;
/// 4. internal topics are excluded unless asked for, and counted;
/// 5. the survivors are sorted BYTEWISE and truncated at `maxTopics` first and
///    the relay budget second, recording which bound bit.
#[must_use]
pub fn assemble(
    listing: &Listing,
    request: &InventoryRequest,
    targeted: &BTreeMap<String, TopicPresence>,
) -> Inventory {
    let expected: BTreeSet<&str> = request.expected_topics.iter().map(String::as_str).collect();

    let mut all: Vec<TopicEntry> = Vec::with_capacity(listing.topics.len() + targeted.len());
    let mut errored: u32 = 0;
    let mut topic_authorization_error_in_listing = false;

    for t in &listing.topics {
        let mut entry = TopicEntry::new(&t.name, t.partitions);
        entry.internal = TopicEntry::name_is_internal(&t.name);
        entry.expected = expected.contains(t.name.as_str());
        entry.error = t.error;
        if let Some(code) = t.error {
            errored += 1;
            if code == CheckCode::TopicAuthorizationFailed {
                topic_authorization_error_in_listing = true;
            }
        }
        all.push(entry);
    }

    for (name, presence) in targeted {
        if let TopicPresence::Present { partitions } = presence {
            let mut entry = TopicEntry::new(name, *partitions);
            entry.internal = TopicEntry::name_is_internal(name);
            entry.expected = true;
            all.push(entry);
        }
    }

    let listed = listing.topics.len() as u32;

    // 4. Internal exclusion, counted.
    let mut internal_excluded: u32 = 0;
    if !request.include_internal {
        all.retain(|e| {
            if e.internal {
                internal_excluded += 1;
                false
            } else {
                true
            }
        });
    }

    // 5. BYTEWISE. `String`'s `Ord` is byte order, which is what the TSV's
    //    stability and `topicsSha256` depend on.
    all.sort_by(|a, b| a.name.as_bytes().cmp(b.name.as_bytes()));
    all.dedup_by(|a, b| a.name == b.name);

    let mut entries: Vec<TopicEntry> = Vec::with_capacity(all.len());
    let mut used: u64 = 0;
    let mut truncation_reason: Option<TruncationReason> = None;
    for entry in all {
        if entries.len() as u32 >= request.max_topics {
            truncation_reason = Some(TruncationReason::MaxTopics);
            break;
        }
        let cost = relay_cost(&entry);
        if used + cost > request.relay_budget_bytes {
            truncation_reason = Some(TruncationReason::RelayLimit);
            break;
        }
        used += cost;
        entries.push(entry);
    }

    // The expected summary and per-name results: the listing decides for a name
    // it showed, the targeted probe decides for one it did not, and a name with
    // neither answer is `unknown` rather than absent from the report.
    let listed_names: BTreeSet<&str> = listing.topics.iter().map(|t| t.name.as_str()).collect();
    let mut expected_results: Vec<ExpectedTopicResult> = Vec::new();
    let mut summary = ExpectedSummary::default();
    let mut seen: BTreeSet<&str> = BTreeSet::new();
    for name in &request.expected_topics {
        if !seen.insert(name.as_str()) {
            continue;
        }
        let state = if listed_names.contains(name.as_str()) {
            ExpectedTopicState::Visible
        } else {
            targeted
                .get(name)
                .copied()
                .unwrap_or(TopicPresence::Unknown)
                .expected_state()
        };
        summary.requested += 1;
        match state {
            ExpectedTopicState::Visible => summary.visible += 1,
            ExpectedTopicState::NotAuthorized => summary.not_authorized += 1,
            ExpectedTopicState::NotFound => summary.not_found += 1,
            ExpectedTopicState::Unknown => summary.unknown += 1,
        }
        expected_results.push(ExpectedTopicResult {
            name: name.clone(),
            state,
        });
    }

    let topics_sha256 = logweir_core::check_contract::topic_tsv_sha256(&entries);
    let result = InventoryResult {
        format: TOPIC_INVENTORY_FORMAT.to_string(),
        cluster_id: None,
        broker_count: Some(listing.broker_count),
        counts: InventoryCounts {
            listed,
            returned: entries.len() as u32,
            internal_excluded,
            errored,
        },
        truncated: truncation_reason.is_some(),
        truncation_reason,
        topic_authorization_error_in_listing,
        expected: summary,
        expected_results,
        topics_sha256,
    };
    Inventory { entries, result }
}

/// Run one `topicInventory` against a probe — D2 §5.2's runner half.
///
/// `targeted_budget` is the WALL-CLOCK ceiling on step 5 as a whole
/// ([`ProbeTimeouts::targeted_budget`]). Once it is spent, every remaining
/// expected name is [`TopicPresence::Unknown`] and the probe stops asking:
/// a check that overran its budget would be killed by the Job's
/// `activeDeadlineSeconds` with no result at all, which is strictly worse than
/// an inventory that says "I did not get to these".
///
/// # Errors
///
/// A [`CheckFailure`] from `cluster_id` or `list_topics`, which are the two
/// calls without which there is no inventory. A targeted probe that fails is
/// recorded as [`TopicPresence::Unknown`] for that name and never fails the
/// whole check.
pub fn collect(
    probe: &dyn InventoryProbe,
    request: &InventoryRequest,
    targeted_budget: Duration,
) -> Result<Inventory, CheckFailure> {
    let cluster_id = probe.cluster_id()?;
    let listing = probe.list_topics()?;

    let mut targeted: BTreeMap<String, TopicPresence> = BTreeMap::new();
    let missing = missing_expected(&listing, &request.expected_topics);
    let started = std::time::Instant::now();
    for name in missing {
        if started.elapsed() >= targeted_budget {
            targeted.insert(name, TopicPresence::Unknown);
            continue;
        }
        let presence = probe
            .describe_topic(&name)
            .unwrap_or(TopicPresence::Unknown);
        targeted.insert(name, presence);
    }

    let mut inventory = assemble(&listing, request, &targeted);
    inventory.result.cluster_id = cluster_id;
    Ok(inventory)
}

// ---------------------------------------------------------------------------
// The rdkafka half
// ---------------------------------------------------------------------------

#[cfg(feature = "client")]
mod client {
    use super::{
        CheckFailure, FaultLog, Inventory, InventoryProbe, InventoryRequest, ListedTopic, Listing,
        ProbeTimeouts, TopicCreateOutcome, TopicPresence,
    };
    use crate::reader::{AuthConfig, NewTopicSpec};
    use logweir_core::check_contract::CheckCode;
    use rdkafka::client::ClientContext;
    use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext};
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::Duration;

    /// rdkafka's own error-code enum, re-exported so a test in
    /// `crates/logweir-kafka/tests/` can drive [`classify_error_code`] against
    /// the REAL table rather than against integers copied out of it.
    pub use rdkafka::error::RDKafkaErrorCode;

    /// rdkafka's configuration map, re-exported for the same reason: a
    /// `ClientConfig` is a map until `create()` is called, so a test can read
    /// the `ssl.ca.location` and `ssl.endpoint.identification.algorithm` a
    /// check would dial with and open no socket. W4 needs the same path to
    /// assert what its runner sends.
    pub use rdkafka::config::ClientConfig;

    /// The `client.id` every check connection announces.
    ///
    /// D2 §5.2 step 1. DISTINCT FROM THE DRILL'S `logweir-drill`, so a broker
    /// operator reading their own logs can tell a read-only check from a run
    /// that moves data.
    pub const CHECK_CLIENT_ID: &str = "logweir-check";

    /// The consumer group id a check announces.
    ///
    /// **A CHECK NEVER JOINS IT.** `BaseConsumer` does not subscribe here and
    /// never commits; librdkafka requires the property to exist before a
    /// consumer handle can be built, so this is a name and not a membership.
    /// Spelled "do-not-commit" for the same reason
    /// `crate::rdkafka_reader`'s is.
    pub const CHECK_GROUP_ID: &str = "logweir-check-do-not-commit";

    /// Map one librdkafka error code to the closed check vocabulary.
    ///
    /// # The rules, and why each one is where it is
    ///
    /// * **Authorization before transport.** `ClusterAuthorizationFailed` and
    ///   `TopicAuthorizationFailed` are answers from a broker that is up and
    ///   talking, so they are never a transport code.
    /// * **`Authentication` and `SaslAuthenticationFailed` are one code.** The
    ///   first is librdkafka's local "the handshake did not complete", the
    ///   second is the broker's own refusal; an operator's next action is the
    ///   same for both and D2 §4.2 gives them one spelling.
    /// * **`SSL` splits on the reason text, and only on a whitelist of
    ///   fragments.** A certificate the client will not trust
    ///   (`TlsTrustFailed`) and a handshake that failed for any other reason
    ///   (`TlsHandshakeFailed`) have different remedies — the first is a CA
    ///   bundle, the second is a protocol or cipher mismatch — and librdkafka
    ///   reports both through the same code. The fragments are matched
    ///   case-insensitively against OpenSSL's own verify messages.
    /// * **Every timeout is `MetadataTimeout`, and that is why [`FaultLog`]
    ///   exists.** On its own this code is honest: nothing answered. It is only
    ///   misleading when the callback saw the real cause, which
    ///   [`super::escalate`] is what fixes.
    /// * **The default arm is a FAILURE, never a success.** An unknown code
    ///   becomes `BrokerUnreachable` — the most conservative Kafka code in the
    ///   vocabulary — rather than a ready code or a silently dropped error.
    #[must_use]
    pub fn classify_error_code(code: RDKafkaErrorCode, reason: &str) -> CheckCode {
        use RDKafkaErrorCode as C;
        match code {
            C::ClusterAuthorizationFailed | C::GroupAuthorizationFailed => {
                CheckCode::ClusterAuthorizationFailed
            }
            C::TopicAuthorizationFailed => CheckCode::TopicAuthorizationFailed,
            C::Authentication | C::SaslAuthenticationFailed => CheckCode::AuthenticationFailed,
            C::SSL => {
                if is_certificate_trust_reason(reason) {
                    CheckCode::TlsTrustFailed
                } else {
                    CheckCode::TlsHandshakeFailed
                }
            }
            C::UnknownTopicOrPartition | C::UnknownTopic | C::UnknownPartition => {
                CheckCode::UnknownTopicOrPartition
            }
            C::OperationTimedOut | C::TimedOutQueue | C::RequestTimedOut | C::MessageTimedOut => {
                CheckCode::MetadataTimeout
            }
            C::BrokerTransportFailure | C::AllBrokersDown | C::Resolve | C::BrokerDestroy => {
                CheckCode::BrokerUnreachable
            }
            // NEVER A READY CODE. D2 §4.2: unknown is the generic failure.
            _ => CheckCode::BrokerUnreachable,
        }
    }

    /// Every [`CheckCode`] [`classify_error_code`] can return, **derived by
    /// sweeping librdkafka's own error-code space** rather than listed.
    ///
    /// `crates/logweir-kafka/tests/inventory.rs` computes D2 §5.5's worst-case
    /// relayed line from this set. A hand-written list would let a ninth,
    /// longer code be added to the classifier without moving the arithmetic
    /// that the relay budget is built on — which is the quiet invalidation the
    /// worst-case test exists to prevent.
    ///
    /// Both reason forms are swept, because `SSL` splits on the reason text and
    /// `TlsTrustFailed` is reachable only through the certificate wording.
    #[must_use]
    pub fn classification_codes() -> std::collections::BTreeSet<CheckCode> {
        use std::convert::TryFrom as _;
        // librdkafka's local codes run from -200 up and its broker codes to
        // about 100; the window is generous on both sides and `try_from`
        // discards everything that is not a real code.
        (-250i32..=250)
            .filter_map(|i| rdkafka::types::RDKafkaRespErr::try_from(i).ok())
            .map(RDKafkaErrorCode::from)
            .flat_map(|code| {
                [
                    classify_error_code(code, ""),
                    classify_error_code(code, "certificate verify failed"),
                ]
            })
            .collect()
    }

    /// Whether an `SSL` error's reason text names a certificate-verification
    /// failure rather than any other handshake problem.
    ///
    /// The fragments are OpenSSL's and librdkafka's own wordings. Matching a
    /// STRING is unavoidable here — librdkafka collapses every TLS failure onto
    /// one code — and the failure mode is bounded: a fragment that stops
    /// matching degrades `TlsTrustFailed` to `TlsHandshakeFailed`, which is
    /// still a TLS failure and still blocking.
    #[must_use]
    pub fn is_certificate_trust_reason(reason: &str) -> bool {
        const FRAGMENTS: [&str; 6] = [
            "certificate verify failed",
            "unable to get local issuer certificate",
            "self signed certificate",
            "self-signed certificate",
            "unable to verify the first certificate",
            "certificate has expired",
        ];
        let lower = reason.to_ascii_lowercase();
        FRAGMENTS.iter().any(|f| lower.contains(f))
    }

    /// The code one entry of a validate-only `CreateTopics` response carries.
    ///
    /// D2 §6.3's `target.topicCreate` row, verbatim. `TopicAlreadyExists` is
    /// `MappedTopicExists` and not a create failure: it is the SECOND answer
    /// D2 §6.7 wants, because a broker that authorises `CREATE` reports it even
    /// for a topic the principal may not `DESCRIBE` (**[VERIFY U2]**), which is
    /// a collision a targeted metadata request alone could not see.
    #[must_use]
    pub fn classify_create_code(code: RDKafkaErrorCode) -> CheckCode {
        use RDKafkaErrorCode as C;
        match code {
            C::TopicAlreadyExists => CheckCode::MappedTopicExists,
            C::TopicAuthorizationFailed | C::ClusterAuthorizationFailed => {
                CheckCode::TopicCreateNotAuthorized
            }
            C::InvalidReplicationFactor => CheckCode::ReplicationFactorExceedsBrokers,
            C::UnsupportedVersion | C::NotImplemented | C::UnsupportedFeature => {
                CheckCode::TopicCreateValidationUnsupported
            }
            // Every other broker refusal is a rejected configuration: it is a
            // NOT-READY code, which is the conservative direction. The raw code
            // name reaches the redacted message, never the verdict.
            _ => CheckCode::TopicConfigRejected,
        }
    }

    /// The librdkafka client context that turns the error callback into facts.
    ///
    /// D2 §4.2 **[VERIFY U5]**. Without it, a wrong SASL password is reported as
    /// `MetadataTimeout` — see this module's header.
    #[derive(Clone)]
    pub struct CapturingContext {
        faults: Arc<FaultLog>,
    }

    impl CapturingContext {
        /// A context writing into `faults`.
        #[must_use]
        pub fn new(faults: Arc<FaultLog>) -> Self {
            Self { faults }
        }
    }

    impl ClientContext for CapturingContext {
        fn error(&self, error: rdkafka::error::KafkaError, reason: &str) {
            let code = error
                .rdkafka_error_code()
                .map_or(CheckCode::BrokerUnreachable, |c| {
                    classify_error_code(c, reason)
                });
            self.faults.record(code, reason);
        }
    }

    impl ConsumerContext for CapturingContext {}

    /// Everything [`KafkaInventory::connect`] needs, with no credential of its
    /// own beyond the [`AuthConfig`] the caller already built through interface
    /// **I1** (`AuthConfig::from_spec`).
    pub struct ConnectionSettings {
        /// The broker list.
        pub bootstrap_servers: Vec<String>,
        /// The auth, built by the caller and never pinned here.
        ///
        /// **THE PRIVATE CA TRAVELS ON THIS, NOT BESIDE IT.** PLAT-07.1 put the
        /// projected trust anchor inside `AuthConfig::ScramSha512`'s
        /// `tls_ca_file`, reached through
        /// [`AuthConfig::with_tls_ca_file`], which REFUSES a CA on a
        /// connection that is not TLS. A second `ca_file` field here — which
        /// this struct used to carry — was a second place that decision could
        /// be made, and it made the wrong one: it would have turned a
        /// `Plaintext` connection into `SSL` because a CA happened to be
        /// present, which is the silent transport change D-SEAMS S5 forbids.
        /// W4 renders `ConnectionPlan::ca_file` by calling
        /// `with_tls_ca_file`, exactly as the drill path does.
        pub auth: AuthConfig,
        /// Per-call timeouts.
        pub timeouts: ProbeTimeouts,
    }

    /// The shared `ClientConfig` both handles are built from.
    ///
    /// # THE DEFERRED SEAM, NOW CLOSED
    ///
    /// D2 §13.2 gave W3 a later edit that would stop this module building its
    /// own rdkafka configuration. PLAT-07.1 has landed and extracted
    /// [`RdKafkaReader::client_config`], so that edit is this function: the
    /// check client and the drill client now derive their
    /// `security.protocol`, `sasl.*`, `ssl.endpoint.identification.algorithm`
    /// and `ssl.ca.location` from ONE implementation.
    ///
    /// **Why sharing it is a security property and not tidiness.** Two of
    /// those keys are controls: hostname verification, and which trust anchor
    /// the connection accepts. A second copy is a second place they can drift,
    /// and the copy this replaced had already drifted — it set
    /// `ssl.ca.location` from a field of its own, with no TLS check and no
    /// `ssl.endpoint.identification.algorithm` at all, so a check against a
    /// private-CA broker would have failed to verify the CA while a drill
    /// against the same broker succeeded, and a `Plaintext` connection carrying
    /// a CA would have been silently upgraded to `SSL`.
    ///
    /// The ONE thing that is overridden afterwards is `client.id`
    /// ([`CHECK_CLIENT_ID`]), so a broker operator reading their own logs can
    /// tell a read-only check from a run that moves data. It is not a control.
    fn client_config(settings: &ConnectionSettings) -> Result<ClientConfig, CheckFailure> {
        let mut cfg = crate::rdkafka_reader::RdKafkaReader::client_config(
            &settings.bootstrap_servers,
            &settings.auth,
        )
        .map_err(|e| {
            // `client_config` refuses a CA without TLS and refuses SP4's token
            // mode. Both are configuration this check cannot dial with, and
            // both are reported as the authentication code rather than as a
            // transport one: nothing was unreachable.
            CheckFailure::new(CheckCode::AuthenticationFailed, e.to_string())
        })?;
        cfg.set("client.id", CHECK_CLIENT_ID);
        Ok(cfg)
    }

    /// The rdkafka implementation of [`InventoryProbe`].
    ///
    /// Holds a metadata-only consumer and an admin client sharing one
    /// [`FaultLog`], because a refusal seen on either handle is a fact about
    /// the same connection.
    pub struct KafkaInventory {
        consumer: BaseConsumer<CapturingContext>,
        admin: rdkafka::admin::AdminClient<CapturingContext>,
        faults: Arc<FaultLog>,
        timeouts: ProbeTimeouts,
    }

    impl KafkaInventory {
        /// Build both handles. **Opens no connection by itself** — librdkafka
        /// dials lazily on the first request, which is why every timeout below
        /// belongs to a call and not to this constructor.
        ///
        /// # Errors
        ///
        /// [`CheckFailure`] with `AuthenticationFailed` for an auth mode this
        /// build does not implement, or `BrokerUnreachable` when librdkafka
        /// refuses the configuration outright.
        pub fn connect(settings: &ConnectionSettings) -> Result<Self, CheckFailure> {
            let faults = Arc::new(FaultLog::new());
            let cfg = client_config(settings)?;
            // THE CONSUMER-ONLY KEYS GO ON A CLONE, for the reason
            // `crate::rdkafka_reader::RdKafkaReader::connect` records: the
            // admin client's underlying handle is producer-shaped, and sharing
            // one config makes librdkafka log a `CONFWARN` for every consumer
            // property on every connection — measured, two lines per check.
            // A check never subscribes and never commits; these exist because
            // librdkafka requires `group.id` before a consumer handle can be
            // built at all.
            let mut consumer_cfg = cfg.clone();
            consumer_cfg
                .set("group.id", CHECK_GROUP_ID)
                .set("enable.auto.commit", "false");
            let consumer: BaseConsumer<CapturingContext> = consumer_cfg
                .create_with_context(CapturingContext::new(Arc::clone(&faults)))
                .map_err(|e| {
                    CheckFailure::new(
                        CheckCode::BrokerUnreachable,
                        format!("the check consumer could not be built: {e}"),
                    )
                })?;
            let admin: rdkafka::admin::AdminClient<CapturingContext> = cfg
                .create_with_context(CapturingContext::new(Arc::clone(&faults)))
                .map_err(|e| {
                    CheckFailure::new(
                        CheckCode::BrokerUnreachable,
                        format!("the check admin client could not be built: {e}"),
                    )
                })?;
            Ok(Self {
                consumer,
                admin,
                faults,
                timeouts: settings.timeouts,
            })
        }

        /// The shared fault log, for a caller that wants to report every code
        /// the callback saw rather than only the deciding one.
        #[must_use]
        pub fn faults(&self) -> &Arc<FaultLog> {
            &self.faults
        }

        /// Run one `topicInventory` against this connection.
        ///
        /// # Errors
        /// As [`super::collect`].
        pub fn inventory(
            &self,
            request: &InventoryRequest,
            targeted_budget: Duration,
        ) -> Result<Inventory, CheckFailure> {
            super::collect(self, request, targeted_budget)
        }

        /// The per-call tokio runtime rdkafka's admin futures need.
        ///
        /// ADR 0004 and `crate::rdkafka_reader`'s own note: without a driven
        /// runtime, `describe_configs` blocks the calling thread for its whole
        /// timeout instead of being polled.
        fn admin_runtime(&self) -> Result<tokio::runtime::Runtime, CheckFailure> {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| {
                    CheckFailure::new(
                        CheckCode::BrokerUnreachable,
                        format!("no tokio runtime for the admin call: {e}"),
                    )
                })
        }

        /// Serve librdkafka's event queue so the error callback actually runs.
        ///
        /// # D2 §4.2's [VERIFY U5] was only half true, and this is the other
        /// half
        ///
        /// Installing a [`CapturingContext`] is NOT enough. In rdkafka 0.36
        /// `ClientContext::error` is invoked from
        /// `Client::poll_event` (`rdkafka-0.36.2/src/client.rs:327-334`), which
        /// is reached only from `BaseConsumer::poll_queue`
        /// (`src/consumer/base_consumer.rs:131`). `fetch_metadata`,
        /// `fetch_cluster_id` and the admin futures never poll that queue, so a
        /// metadata-only workflow — which is exactly what a check is — leaves
        /// every error event sitting on it and the callback never fires.
        ///
        /// MEASURED, on the compose broker's SASL listener with a principal it
        /// does not know: without this drain the call reports
        /// `BrokerUnreachable`, librdkafka prints its
        /// `SASL authentication error … invalid credentials` line on ITS OWN
        /// stderr logger, and nothing structured records the refusal. With it,
        /// the same call reports `AuthenticationFailed`.
        /// `tests/live.rs::a_refused_scram_credential_is_authentication_failed_and_not_a_timeout`
        /// is the regression, and it fails by name if this drain is removed.
        ///
        /// **This consumes nothing.** The consumer has no subscription and no
        /// assignment, so no fetch is in flight and no group is joined; `poll`
        /// here serves the event queue and its return value is deliberately
        /// discarded.
        fn drain_events(&self) {
            for _ in 0..MAX_DRAINED_EVENTS {
                // A zero timeout handles at most one queued event and returns,
                // so the loop bound is the only bound this needs: a broker that
                // is down queues one error per retry, and an unbounded drain
                // would be a second place a check could overrun its budget.
                let _ = self.consumer.poll(Duration::ZERO);
            }
        }

        /// Dial the consumer when nothing has yet, so the error callback has
        /// something to report — the ADMIN half of D2 §4.2's `[VERIFY U5]`.
        ///
        /// [`FaultLog`]'s header has the crate-source citations: the admin
        /// client's `CapturingContext` is never consulted in rdkafka 0.36, so
        /// the only handle that can fill the log is the consumer, and the only
        /// thing that makes the consumer dial is a request. On an admin-first
        /// failure the consumer has never dialled, the log is empty, and
        /// [`FaultLog::explain`] has nothing to escalate with — the exact
        /// defect the drain was added to remove, surviving on the admin half.
        ///
        /// **Bounded, and only on the failure path.** It runs only when the log
        /// is empty (a populated log already has the answer), under
        /// [`ProbeTimeouts::targeted_metadata`] — two seconds by default, the
        /// shortest timeout this struct carries — and its result is discarded:
        /// the metadata is not wanted, the CONNECTION ATTEMPT is.
        ///
        /// # What it is worth, measured
        ///
        /// It is belt-and-braces, and saying so is more useful than implying
        /// otherwise. MEASURED against the compose broker's SASL listener with
        /// `validate_create_topics` as the very first call: removing this
        /// function alone leaves
        /// `tests/live.rs::an_admin_first_refusal_is_authentication_failed_and_not_a_timeout`
        /// GREEN, because librdkafka 2.12 connects a consumer handle to its
        /// bootstrap brokers eagerly at `rd_kafka_new` — so by the time a
        /// ten-second admin call has failed, the consumer's own error callback
        /// has already queued the refusal and [`drain_events`] serves it.
        /// Removing the DRAIN turns the same test red
        /// (`BrokerUnreachable`), with or without this function.
        ///
        /// It stays because that greenness rests on a librdkafka timing
        /// assumption this code does not control: a shorter admin timeout, a
        /// build that connects lazily, or a `metadata.refresh` change would
        /// restore the empty-log case the reviewer identified, and the cost of
        /// covering it is one skipped call on a path that has already failed.
        /// `an_admin_failure_dials_the_consumer_before_it_classifies` is the
        /// always-on guard, and
        /// `an_admin_first_failure_has_nothing_to_escalate_until_the_consumer_has_dialled`
        /// is the unit row for the state it removes.
        ///
        /// [`drain_events`]: KafkaInventory::drain_events
        fn dial_consumer_if_silent(&self) {
            if !self.faults.is_empty() {
                return;
            }
            let _ = self
                .consumer
                .fetch_metadata(None, self.timeouts.targeted_metadata);
        }

        /// A failing call's code, with the error callback's observation applied.
        ///
        /// Dials the consumer if nothing has (so an admin-first failure has a
        /// fault to find), then drains: the events that explain the failure are
        /// queued by the time the call returns, and nothing else would ever
        /// serve them.
        fn fail(&self, base: CheckCode, what: &str) -> CheckFailure {
            self.dial_consumer_if_silent();
            self.drain_events();
            self.faults.explain(base, what)
        }
    }

    /// How many queued librdkafka events one failing call serves.
    ///
    /// Each `poll(ZERO)` handles at most one event, and the interesting ones —
    /// the first authentication or TLS failure per broker — arrive first.
    /// Sixty-four is [`FaultLog::MAX_ENTRIES`], so the drain can fill the log
    /// and no more.
    const MAX_DRAINED_EVENTS: usize = FaultLog::MAX_ENTRIES;

    impl InventoryProbe for KafkaInventory {
        fn cluster_id(&self) -> Result<Option<String>, CheckFailure> {
            let id = self
                .consumer
                .client()
                .fetch_cluster_id(self.timeouts.metadata);
            if id.is_none() {
                // NULL means "no ClusterId in the allotted timespan". Serve the
                // event queue before deciding what that meant: with a recorded
                // fault that is not a timeout, saying `MetadataTimeout` would
                // hide a credential refusal.
                self.drain_events();
                if !self.faults.is_empty() {
                    return Err(self.fail(CheckCode::MetadataTimeout, "fetch_cluster_id"));
                }
            }
            Ok(id.filter(|s| !s.trim().is_empty()))
        }

        fn list_topics(&self) -> Result<Listing, CheckFailure> {
            let md = self
                .consumer
                .fetch_metadata(None, self.timeouts.metadata)
                .map_err(|e| {
                    let base = e
                        .rdkafka_error_code()
                        .map_or(CheckCode::BrokerUnreachable, |c| classify_error_code(c, ""));
                    self.fail(base, "all-topics metadata")
                })?;
            let topics = md
                .topics()
                .iter()
                .map(|t| ListedTopic {
                    name: t.name().to_string(),
                    partitions: match t.error() {
                        Some(_) => 0,
                        None => t.partitions().len() as u32,
                    },
                    error: t
                        .error()
                        .map(|e| classify_error_code(RDKafkaErrorCode::from(e), "")),
                })
                .collect();
            Ok(Listing {
                broker_count: md.brokers().len() as u32,
                topics,
            })
        }

        fn describe_topic(&self, name: &str) -> Result<TopicPresence, CheckFailure> {
            let md = match self
                .consumer
                .fetch_metadata(Some(name), self.timeouts.targeted_metadata)
            {
                Ok(md) => md,
                // A TARGETED PROBE NEVER FAILS THE WHOLE INVENTORY. Its whole
                // job is to produce one of four answers about one name, and
                // "the request did not come back" is one of them.
                Err(_) => return Ok(TopicPresence::Unknown),
            };
            let Some(t) = md.topics().first() else {
                return Ok(TopicPresence::Unknown);
            };
            match t.error() {
                // THE THREE-WAY DECISION IS `TopicPresence::of_error`'s, not
                // this function's: the authorization arm is unreachable from
                // the compose stack (no ACLs), so a mutant in it survived the
                // whole suite until the rule moved to the pure layer.
                Some(err) => Ok(TopicPresence::of_error(classify_error_code(
                    RDKafkaErrorCode::from(err),
                    "",
                ))),
                None => Ok(TopicPresence::Present {
                    partitions: t.partitions().len() as u32,
                }),
            }
        }

        fn topic_configs(&self, name: &str) -> Result<BTreeMap<String, String>, CheckFailure> {
            use rdkafka::admin::{AdminOptions, ResourceSpecifier};
            self.describe(
                &[ResourceSpecifier::Topic(name)],
                &AdminOptions::new().request_timeout(Some(self.timeouts.describe_configs)),
                &format!("DescribeConfigs on topic {name}"),
            )
        }

        fn broker_configs(&self) -> Result<BTreeMap<String, String>, CheckFailure> {
            use rdkafka::admin::{AdminOptions, ResourceSpecifier};
            // THE BROKER ID COMES FROM METADATA, never from a literal: a
            // `Broker(0)` against a cluster whose only node is 1001 describes
            // nothing, and a bound computed from an empty map would pass.
            let md = self
                .consumer
                .fetch_metadata(None, self.timeouts.metadata)
                .map_err(|_| self.fail(CheckCode::BrokerConfigsNotReadable, "broker metadata"))?;
            let broker_id = md
                .brokers()
                .first()
                .map(rdkafka::metadata::MetadataBroker::id)
                .ok_or_else(|| {
                    CheckFailure::new(
                        CheckCode::BrokerConfigsNotReadable,
                        "cluster metadata listed no broker, so there is no broker id to describe",
                    )
                })?;
            self.describe(
                &[ResourceSpecifier::Broker(broker_id)],
                &AdminOptions::new().request_timeout(Some(self.timeouts.describe_configs)),
                &format!("DescribeConfigs on broker {broker_id}"),
            )
        }

        fn validate_create_topics(
            &self,
            specs: &[NewTopicSpec],
        ) -> Result<Vec<TopicCreateOutcome>, CheckFailure> {
            use rdkafka::admin::AdminOptions;
            if specs.is_empty() {
                return Ok(Vec::new());
            }
            let new_topics = crate::rdkafka_reader::new_topics_for(specs);
            // THE FLAG. `validate_only(true)` is what makes this a preflight
            // and not a restore: without it every mapped target topic of the
            // plan is created here, which G12 forbids and which no later phase
            // would undo.
            let options = AdminOptions::new()
                .request_timeout(Some(self.timeouts.create_topics))
                .validate_only(true);
            let rt = self.admin_runtime()?;
            let res = rt
                .block_on(self.admin.create_topics(&new_topics, &options))
                .map_err(|e| {
                    let base = e
                        .rdkafka_error_code()
                        .map_or(CheckCode::BrokerUnreachable, |c| classify_error_code(c, ""));
                    self.fail(base, "validate-only CreateTopics")
                })?;
            Ok(res
                .into_iter()
                .map(|r| match r {
                    Ok(name) => TopicCreateOutcome {
                        name,
                        code: CheckCode::TopicCreateValidated,
                        message: String::new(),
                    },
                    Err((name, code)) => TopicCreateOutcome {
                        name,
                        code: classify_create_code(code),
                        message: logweir_core::check_contract::redact(&format!(
                            "the broker refused the validate-only CreateTopics with {code}"
                        )),
                    },
                })
                .collect())
        }
    }

    impl KafkaInventory {
        /// The shared `DescribeConfigs` body: one resource, flattened, with the
        /// fault log applied to a failure.
        fn describe(
            &self,
            resources: &[rdkafka::admin::ResourceSpecifier<'_>],
            options: &rdkafka::admin::AdminOptions,
            what: &str,
        ) -> Result<BTreeMap<String, String>, CheckFailure> {
            let rt = self.admin_runtime()?;
            let res = rt
                .block_on(self.admin.describe_configs(resources, options))
                .map_err(|e| {
                    let base = e
                        .rdkafka_error_code()
                        .map_or(CheckCode::BrokerUnreachable, |c| classify_error_code(c, ""));
                    self.fail(base, what)
                })?;
            let mut out = BTreeMap::new();
            for r in res {
                let cfg = r.map_err(|code| self.fail(classify_error_code(code, ""), what))?;
                for e in cfg.entries {
                    if let Some(v) = e.value {
                        out.insert(e.name, v);
                    }
                }
            }
            Ok(out)
        }
    }
}

#[cfg(feature = "client")]
pub use client::{
    classification_codes, classify_create_code, classify_error_code, is_certificate_trust_reason,
    CapturingContext, ClientConfig, ConnectionSettings, KafkaInventory, RDKafkaErrorCode,
    CHECK_CLIENT_ID, CHECK_GROUP_ID,
};
