//! D2 **W3** — the bounded Kafka inventory and the validate-only probes.
//!
//! EVERY TEST HERE IS SOCKETLESS. The rules live behind
//! [`logweir_kafka::inventory::InventoryProbe`], so the assembly, the
//! truncation, the expected-topic probe and the error classification are all
//! driven by a fake. The one test that needs a real broker lives in
//! `tests/live.rs`, which is `#![cfg(feature = "e2e")]`-gated.
//!
//! The two guards that cannot be expressed against a fake — "the validate-only
//! flag is set" and "every admin call carries a timeout" — are SOURCE SCANS
//! over `src/inventory.rs`, in the style `crates/logweir/tests/` already uses
//! for the one-construction-site rule. A dropped flag is invisible to a
//! double, because the double never sends the request.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::time::Duration;

use logweir_core::check_contract::{
    CheckCode, ExpectedTopicState, TopicEntry, TruncationReason, TOPIC_FRAME_PREFIX,
    TOPIC_INVENTORY_FORMAT,
};
use logweir_kafka::inventory::{
    assemble, classification_codes, classify_create_code, classify_error_code, collect, escalate,
    fault_rank, is_certificate_trust_reason, missing_expected, relay_cost, CheckFailure,
    ClientConfig, ConnectionSettings, FaultLog, Inventory, InventoryProbe, InventoryRequest,
    ListedTopic, Listing, ProbeTimeouts, RDKafkaErrorCode, TopicCreateOutcome, TopicPresence,
};
use logweir_kafka::rdkafka_reader::RdKafkaReader;
use logweir_kafka::reader::{AuthConfig, NewTopicSpec};

/// The exact `ClientConfig` a [`KafkaInventory`] would dial with.
///
/// `KafkaInventory::connect` builds both handles from it and then adds the
/// consumer-only keys to a clone, so reading it here is reading what the check
/// sends — and a `ClientConfig` is a map until `create()` is called, so this
/// opens no socket. It mirrors `RdKafkaReader::client_config` plus the one
/// override `inventory::client::client_config` applies.
fn check_client_config(
    settings: &ConnectionSettings,
) -> Result<ClientConfig, logweir_kafka::reader::KafkaError> {
    let mut cfg = RdKafkaReader::client_config(&settings.bootstrap_servers, &settings.auth)?;
    cfg.set("client.id", "logweir-check");
    Ok(cfg)
}

// ---------------------------------------------------------------------------
// Fakes
// ---------------------------------------------------------------------------

/// A probe that answers from a script and records what it was asked.
struct FakeProbe {
    cluster_id: Option<String>,
    listing: Listing,
    targeted: BTreeMap<String, TopicPresence>,
    asked: RefCell<Vec<String>>,
    fail_listing: Option<CheckFailure>,
}

impl FakeProbe {
    fn new(listing: Listing) -> Self {
        Self {
            cluster_id: Some("M29I2S7FQPyHBEX12Vx7XA".to_string()),
            listing,
            targeted: BTreeMap::new(),
            asked: RefCell::new(Vec::new()),
            fail_listing: None,
        }
    }

    fn with_targeted(mut self, name: &str, presence: TopicPresence) -> Self {
        self.targeted.insert(name.to_string(), presence);
        self
    }
}

impl InventoryProbe for FakeProbe {
    fn cluster_id(&self) -> Result<Option<String>, CheckFailure> {
        Ok(self.cluster_id.clone())
    }

    fn list_topics(&self) -> Result<Listing, CheckFailure> {
        match &self.fail_listing {
            Some(f) => Err(f.clone()),
            None => Ok(self.listing.clone()),
        }
    }

    fn describe_topic(&self, name: &str) -> Result<TopicPresence, CheckFailure> {
        self.asked.borrow_mut().push(name.to_string());
        Ok(self
            .targeted
            .get(name)
            .copied()
            .unwrap_or(TopicPresence::NotFound))
    }

    fn topic_configs(&self, _name: &str) -> Result<BTreeMap<String, String>, CheckFailure> {
        Ok(BTreeMap::new())
    }

    fn broker_configs(&self) -> Result<BTreeMap<String, String>, CheckFailure> {
        Ok(BTreeMap::new())
    }

    fn validate_create_topics(
        &self,
        specs: &[NewTopicSpec],
    ) -> Result<Vec<TopicCreateOutcome>, CheckFailure> {
        Ok(specs
            .iter()
            .map(|s| TopicCreateOutcome {
                name: s.name.clone(),
                code: CheckCode::TopicCreateValidated,
                message: String::new(),
            })
            .collect())
    }
}

fn listed(name: &str, partitions: u32) -> ListedTopic {
    ListedTopic {
        name: name.to_string(),
        partitions,
        error: None,
    }
}

fn listing(names: &[(&str, u32)]) -> Listing {
    Listing {
        broker_count: 3,
        topics: names.iter().map(|(n, p)| listed(n, *p)).collect(),
    }
}

fn request(max_topics: u32) -> InventoryRequest {
    InventoryRequest {
        include_internal: false,
        expected_topics: Vec::new(),
        max_topics,
        relay_budget_bytes: 6 * 1024 * 1024,
    }
}

fn names(inv: &Inventory) -> Vec<String> {
    inv.entries.iter().map(|e| e.name.clone()).collect()
}

// ---------------------------------------------------------------------------
// Internal topics — D2 §5.3
// ---------------------------------------------------------------------------

/// D2 §12's PLAT-09.1 row `inventory::double_underscore_is_internal`.
#[test]
fn double_underscore_is_internal() {
    for internal in [
        "__consumer_offsets",
        "__transaction_state",
        "__share_group_state",
        "__",
        "__anything",
    ] {
        assert!(
            TopicEntry::name_is_internal(internal),
            "{internal} must be internal"
        );
    }
    // THE NAMES THAT ARE NOT GUESSED (D2 §5.3). A single underscore, a
    // Confluent convention and a Connect topic all have configurable names, and
    // a wrong "this is internal" silently drops a user's topic from a listing.
    for user in [
        "_schemas",
        "_confluent-metrics",
        "connect-offsets",
        "orders",
        "_",
    ] {
        assert!(
            !TopicEntry::name_is_internal(user),
            "{user} must NOT be guessed as internal"
        );
    }
}

#[test]
fn internal_topics_are_excluded_by_default_and_counted() {
    let l = listing(&[
        ("orders", 6),
        ("__consumer_offsets", 50),
        ("payments", 3),
        ("__transaction_state", 50),
    ]);
    let inv = assemble(&l, &request(1000), &BTreeMap::new());
    assert_eq!(names(&inv), vec!["orders", "payments"]);
    assert_eq!(inv.result.counts.listed, 4);
    assert_eq!(inv.result.counts.returned, 2);
    assert_eq!(inv.result.counts.internal_excluded, 2);
}

#[test]
fn include_internal_returns_them_with_the_flag_and_no_exclusion_count() {
    let l = listing(&[("orders", 6), ("__consumer_offsets", 50)]);
    let req = InventoryRequest {
        include_internal: true,
        ..request(1000)
    };
    let inv = assemble(&l, &req, &BTreeMap::new());
    assert_eq!(names(&inv), vec!["__consumer_offsets", "orders"]);
    assert_eq!(inv.result.counts.internal_excluded, 0);
    let internal = inv
        .entries
        .iter()
        .find(|e| e.name == "__consumer_offsets")
        .expect("present");
    assert!(internal.internal, "the flag travels with the entry");
    assert_eq!(internal.flags(), "internal");
}

// ---------------------------------------------------------------------------
// Ordering, digests and truncation — D2 §5.2 step 6, §5.5
// ---------------------------------------------------------------------------

#[test]
fn entries_are_sorted_bytewise_and_the_digest_covers_exactly_what_is_relayed() {
    // Byte order, not locale order and not case-insensitive order: `Z` (0x5A)
    // sorts before `a` (0x61), and the TSV's stability is what `topicsSha256`
    // is taken over.
    let l = listing(&[("beta", 1), ("Zulu", 1), ("alpha", 1), ("Alpha", 1)]);
    let inv = assemble(&l, &request(1000), &BTreeMap::new());
    assert_eq!(names(&inv), vec!["Alpha", "Zulu", "alpha", "beta"]);
    assert_eq!(
        inv.result.topics_sha256,
        logweir_core::check_contract::topic_tsv_sha256(&inv.entries),
        "the recorded digest is over the relayed entries and nothing else"
    );

    // And it is NOT the digest of the untruncated set — the mutant for a
    // digest taken before truncation.
    let truncated = assemble(&l, &request(2), &BTreeMap::new());
    assert_ne!(truncated.result.topics_sha256, inv.result.topics_sha256);
    assert_eq!(names(&truncated), vec!["Alpha", "Zulu"]);
}

#[test]
fn max_topics_truncates_and_names_that_bound() {
    let l = listing(&[("a", 1), ("b", 1), ("c", 1), ("d", 1)]);
    let inv = assemble(&l, &request(2), &BTreeMap::new());
    assert_eq!(names(&inv), vec!["a", "b"]);
    assert!(inv.result.truncated);
    assert_eq!(
        inv.result.truncation_reason,
        Some(TruncationReason::MaxTopics)
    );
    assert_eq!(inv.result.counts.listed, 4);
    assert_eq!(inv.result.counts.returned, 2);
}

/// D2 §12's PLAT-09.1 runner row `topics_truncate_at_relay_budget`, at the
/// assembly layer that decides it.
#[test]
fn topics_truncate_at_relay_budget() {
    let l = listing(&[("aaaa", 1), ("bbbb", 1), ("cccc", 1)]);
    // Exactly two lines' worth of budget.
    let one = relay_cost(&{
        let mut e = TopicEntry::new("aaaa", 1);
        e.internal = false;
        e
    });
    let req = InventoryRequest {
        relay_budget_bytes: one * 2,
        ..request(1000)
    };
    let inv = assemble(&l, &req, &BTreeMap::new());
    assert_eq!(names(&inv), vec!["aaaa", "bbbb"]);
    assert!(inv.result.truncated);
    assert_eq!(
        inv.result.truncation_reason,
        Some(TruncationReason::RelayLimit)
    );

    // A budget of one byte less than one line relays NOTHING rather than
    // overrunning by one entry.
    let req = InventoryRequest {
        relay_budget_bytes: one - 1,
        ..request(1000)
    };
    let inv = assemble(&l, &req, &BTreeMap::new());
    assert!(inv.entries.is_empty());
    assert_eq!(
        inv.result.truncation_reason,
        Some(TruncationReason::RelayLimit)
    );
}

#[test]
fn the_relay_cost_is_the_frame_prefix_plus_the_canonical_line() {
    let entry = TopicEntry::new("orders", 6);
    assert_eq!(
        relay_cost(&entry) as usize,
        TOPIC_FRAME_PREFIX.len() + entry.tsv_line().len()
    );
    // THE WORST CASE, DERIVED RATHER THAN QUOTED. D2 §5.5 computes the relay
    // budget from "~24 characters of flags and separators", i.e. 283 bytes of
    // line and 303 bytes relayed. That understates it: the flag field carries
    // `internal,expected,error:<Code>`, and the longest code this module can
    // put there is `ClusterAuthorizationFailed` (26).
    //
    // The code set is DERIVED from `classify_error_code` itself — it is the
    // sweep over librdkafka's own error-code space, not a list retyped here —
    // so a ninth, longer code added to the classifier moves this arithmetic
    // instead of quietly invalidating the table the relay budget is built on.
    // A hand-maintained array is what an independent review flagged; this is
    // the fix.
    let kafka_codes = classification_codes();
    assert!(
        kafka_codes.len() >= 6,
        "the sweep found only {} codes; it has gone quiet: {kafka_codes:?}",
        kafka_codes.len()
    );
    assert!(
        kafka_codes.contains(&CheckCode::TlsTrustFailed)
            && kafka_codes.contains(&CheckCode::ClusterAuthorizationFailed),
        "the sweep must reach both reason-dependent arms: {kafka_codes:?}"
    );
    let longest = kafka_codes
        .iter()
        .max_by_key(|c| c.as_str().len())
        .copied()
        .expect("a non-empty set");
    let mut worst = TopicEntry::new(&"n".repeat(249), 999_999);
    worst.internal = true;
    worst.expected = true;
    worst.error = Some(longest);
    let cost = relay_cost(&worst);
    assert_eq!(
        cost,
        (TOPIC_FRAME_PREFIX.len()
            + 249
            + 1
            + 6
            + 1
            + "internal,expected,error:".len()
            + longest.as_str().len()
            + 1) as u64,
        "the worst case is prefix + name + tab + partitions + tab + flags + newline"
    );
    assert_eq!(
        cost, 328,
        "D2 §5.5's worst-case relayed line is 303 bytes; the derived figure is {cost}. If this \
         moves, D2 §5.5's budget table moves with it"
    );
    // The consequence, stated rather than left implicit: at that shape the
    // 6 MiB relay budget carries about 19,100 entries, FEWER than the 20,000
    // D2 §5.5's default `maxTopics` assumes fits. The bound that bites is then
    // `RelayLimit` rather than `MaxTopics` — both are recorded with their
    // reason, so nothing is silently lost, but the table's claim that "the
    // default maxTopics of 20,000 fits even at worst-case names" is false for
    // a listing whose every entry also carries a flag set.
    assert!(
        (6 * 1024 * 1024) / cost < 20_000,
        "if this becomes false, D2 §5.5's claim holds again and this note should go"
    );
}

#[test]
fn an_untruncated_inventory_names_no_bound() {
    let inv = assemble(&listing(&[("a", 1)]), &request(1000), &BTreeMap::new());
    assert!(!inv.result.truncated);
    assert_eq!(inv.result.truncation_reason, None);
    assert_eq!(inv.result.format, TOPIC_INVENTORY_FORMAT);
    assert_eq!(inv.result.broker_count, Some(3));
}

// ---------------------------------------------------------------------------
// Per-entry errors and the visibility SIGNAL — D2 §5.4(i)
// ---------------------------------------------------------------------------

#[test]
fn an_errored_listing_entry_is_relayed_not_dropped_and_raises_the_signal() {
    let mut l = listing(&[("orders", 6)]);
    l.topics.push(ListedTopic {
        name: "secrets".to_string(),
        partitions: 0,
        error: Some(CheckCode::TopicAuthorizationFailed),
    });
    l.topics.push(ListedTopic {
        name: "electing".to_string(),
        partitions: 0,
        error: Some(CheckCode::BrokerUnreachable),
    });
    let inv = assemble(&l, &request(1000), &BTreeMap::new());
    assert_eq!(names(&inv), vec!["electing", "orders", "secrets"]);
    assert_eq!(inv.result.counts.errored, 2);
    assert!(
        inv.result.topic_authorization_error_in_listing,
        "D2 §5.4 signal (i) must be raised by an authorization error in the listing"
    );
    let secrets = inv.entries.iter().find(|e| e.name == "secrets").unwrap();
    assert_eq!(secrets.flags(), "error:TopicAuthorizationFailed");
}

#[test]
fn a_listing_whose_only_errors_are_transient_does_not_raise_the_authorization_signal() {
    let mut l = listing(&[("orders", 6)]);
    l.topics.push(ListedTopic {
        name: "electing".to_string(),
        partitions: 0,
        error: Some(CheckCode::BrokerUnreachable),
    });
    let inv = assemble(&l, &request(1000), &BTreeMap::new());
    assert_eq!(inv.result.counts.errored, 1);
    assert!(!inv.result.topic_authorization_error_in_listing);
}

// ---------------------------------------------------------------------------
// Expected topics — D2 §5.2 step 5
// ---------------------------------------------------------------------------

#[test]
fn only_expected_names_absent_from_the_listing_are_probed_and_each_only_once() {
    let l = listing(&[("orders", 6), ("payments", 3)]);
    let expected: Vec<String> = ["orders", "ghost", "ghost", "other"]
        .iter()
        .map(|s| (*s).to_string())
        .collect();
    assert_eq!(missing_expected(&l, &expected), vec!["ghost", "other"]);
}

#[test]
fn an_expected_name_in_the_listing_is_flagged_and_counted_visible() {
    let l = listing(&[("orders", 6), ("payments", 3)]);
    let req = InventoryRequest {
        expected_topics: vec!["orders".to_string()],
        ..request(1000)
    };
    let inv = assemble(&l, &req, &BTreeMap::new());
    let orders = inv.entries.iter().find(|e| e.name == "orders").unwrap();
    assert!(orders.expected);
    assert_eq!(orders.flags(), "expected");
    assert_eq!(inv.result.expected.requested, 1);
    assert_eq!(inv.result.expected.visible, 1);
    assert_eq!(
        inv.result.expected_results[0].state,
        ExpectedTopicState::Visible
    );
}

/// The PLAT-09.1 acceptance row "ACL-limited principal": a targeted
/// `TOPIC_AUTHORIZATION_FAILED` is `notAuthorized`, is counted, and does NOT
/// become a topic line claiming the topic exists.
#[test]
fn a_targeted_authorization_failure_is_not_authorized_and_adds_no_topic_line() {
    let l = listing(&[("orders", 6)]);
    let req = InventoryRequest {
        expected_topics: vec!["secrets".to_string()],
        ..request(1000)
    };
    let mut targeted = BTreeMap::new();
    targeted.insert("secrets".to_string(), TopicPresence::NotAuthorized);
    let inv = assemble(&l, &req, &targeted);
    assert_eq!(names(&inv), vec!["orders"], "no line is invented");
    assert_eq!(inv.result.expected.not_authorized, 1);
    assert_eq!(inv.result.expected.visible, 0);
    assert_eq!(
        inv.result.expected_results[0].state,
        ExpectedTopicState::NotAuthorized
    );
    // The signal that makes the controller's visibility `limited` is the
    // expected summary, not the listing flag: the broker filtered the topic
    // out of the listing silently.
    assert!(!inv.result.topic_authorization_error_in_listing);
}

#[test]
fn a_targeted_hit_adds_the_topic_the_listing_did_not_show() {
    let l = listing(&[("orders", 6)]);
    let req = InventoryRequest {
        expected_topics: vec!["hidden".to_string()],
        ..request(1000)
    };
    let mut targeted = BTreeMap::new();
    targeted.insert(
        "hidden".to_string(),
        TopicPresence::Present { partitions: 12 },
    );
    let inv = assemble(&l, &req, &targeted);
    assert_eq!(names(&inv), vec!["hidden", "orders"]);
    let hidden = inv.entries.iter().find(|e| e.name == "hidden").unwrap();
    assert_eq!(hidden.partitions, 12);
    assert!(hidden.expected);
    assert_eq!(inv.result.expected.visible, 1);
    // `listed` is what the BROKER's all-topics answer held, so a name only the
    // targeted probe found does not inflate it.
    assert_eq!(inv.result.counts.listed, 1);
    assert_eq!(inv.result.counts.returned, 2);
}

#[test]
fn a_targeted_absence_is_not_found_and_an_unanswered_one_is_unknown() {
    let l = listing(&[("orders", 6)]);
    let req = InventoryRequest {
        expected_topics: vec!["gone".to_string(), "silent".to_string()],
        ..request(1000)
    };
    let mut targeted = BTreeMap::new();
    targeted.insert("gone".to_string(), TopicPresence::NotFound);
    targeted.insert("silent".to_string(), TopicPresence::Unknown);
    let inv = assemble(&l, &req, &targeted);
    assert_eq!(inv.result.expected.not_found, 1);
    assert_eq!(inv.result.expected.unknown, 1);
    assert_eq!(inv.result.expected.requested, 2);
}

#[test]
fn an_expected_name_with_no_answer_at_all_is_unknown_rather_than_missing() {
    let l = listing(&[("orders", 6)]);
    let req = InventoryRequest {
        expected_topics: vec!["never-asked".to_string()],
        ..request(1000)
    };
    let inv = assemble(&l, &req, &BTreeMap::new());
    assert_eq!(inv.result.expected_results.len(), 1);
    assert_eq!(
        inv.result.expected_results[0].state,
        ExpectedTopicState::Unknown
    );
    assert_eq!(inv.result.expected.unknown, 1);
}

// ---------------------------------------------------------------------------
// collect() — the orchestration, still socketless
// ---------------------------------------------------------------------------

#[test]
fn collect_reads_the_cluster_id_and_probes_only_the_missing_expected_names() {
    let probe = FakeProbe::new(listing(&[("orders", 6), ("payments", 3)]))
        .with_targeted("ghost", TopicPresence::NotFound);
    let req = InventoryRequest {
        expected_topics: vec!["orders".to_string(), "ghost".to_string()],
        ..request(1000)
    };
    let inv = collect(&probe, &req, Duration::from_secs(30)).expect("the fake never fails");
    assert_eq!(
        inv.result.cluster_id.as_deref(),
        Some("M29I2S7FQPyHBEX12Vx7XA")
    );
    assert_eq!(
        probe.asked.borrow().as_slice(),
        ["ghost"],
        "a name the listing already showed is never probed again"
    );
    assert_eq!(inv.result.expected.visible, 1);
    assert_eq!(inv.result.expected.not_found, 1);
}

/// The budget, and the reason it exists: a check that overran it would be
/// killed by `activeDeadlineSeconds` with NO result, which is strictly worse
/// than an inventory that says "I did not get to these".
#[test]
fn an_exhausted_targeted_budget_stops_asking_and_reports_unknown() {
    let probe = FakeProbe::new(listing(&[("orders", 6)]))
        .with_targeted("a", TopicPresence::Present { partitions: 1 })
        .with_targeted("b", TopicPresence::Present { partitions: 1 });
    let req = InventoryRequest {
        expected_topics: vec!["a".to_string(), "b".to_string()],
        ..request(1000)
    };
    let inv = collect(&probe, &req, Duration::ZERO).expect("collect succeeds");
    assert!(
        probe.asked.borrow().is_empty(),
        "with no budget nothing is asked: {:?}",
        probe.asked.borrow()
    );
    assert_eq!(inv.result.expected.unknown, 2);
    assert_eq!(inv.result.expected.visible, 0);
    // And the inventory itself still landed: the listing is not lost because
    // the expected probe had no time.
    assert_eq!(names(&inv), vec!["orders"]);
}

#[test]
fn a_failing_listing_fails_the_whole_inventory() {
    let mut probe = FakeProbe::new(listing(&[]));
    probe.fail_listing = Some(CheckFailure::new(
        CheckCode::AuthenticationFailed,
        "the broker refused the credential",
    ));
    let err = collect(&probe, &request(1000), Duration::from_secs(1)).unwrap_err();
    assert_eq!(err.code, CheckCode::AuthenticationFailed);
}

// ---------------------------------------------------------------------------
// The fault log — D2 §4.2 [VERIFY U5]
// ---------------------------------------------------------------------------

/// D2 §12's PLAT-03.1 unit row `kafka_errors::sasl_failure_via_error_callback`.
///
/// THE DEFECT THIS IS ABOUT. A metadata request against a broker that refused
/// the credential returns a TIMEOUT, because librdkafka retries until the
/// caller's deadline. Without the callback the check reports
/// `MetadataTimeout`, and an operator goes looking at the network for a wrong
/// password.
#[test]
fn a_sasl_failure_seen_by_the_error_callback_beats_the_calls_own_timeout() {
    let log = FaultLog::new();
    log.record(
        CheckCode::AuthenticationFailed,
        "SASL SCRAM-SHA-512 authentication failed: Authentication failed",
    );
    let failure = log.explain(CheckCode::MetadataTimeout, "all-topics metadata");
    assert_eq!(failure.code, CheckCode::AuthenticationFailed);
    assert!(
        failure.message.contains("MetadataTimeout")
            && failure.message.contains("AuthenticationFailed"),
        "the message must name both what the call said and what was observed: {}",
        failure.message
    );
}

#[test]
fn the_fault_rank_order_is_the_one_the_escalation_depends_on() {
    // Every ranked code, strongest first.
    let order = [
        CheckCode::AuthenticationFailed,
        CheckCode::TlsTrustFailed,
        CheckCode::TlsHandshakeFailed,
        CheckCode::ClusterAuthorizationFailed,
        CheckCode::BrokerUnreachable,
        CheckCode::MetadataTimeout,
    ];
    for pair in order.windows(2) {
        assert!(
            fault_rank(pair[0]) > fault_rank(pair[1]),
            "{:?} must outrank {:?}",
            pair[0],
            pair[1]
        );
    }
    // A code the table does not rank never displaces anything.
    assert_eq!(fault_rank(CheckCode::Authenticated), 0);
    assert_eq!(
        escalate(CheckCode::MetadataTimeout, Some(CheckCode::Authenticated)),
        CheckCode::MetadataTimeout
    );
    // And a weaker observation never downgrades a stronger outcome.
    assert_eq!(
        escalate(
            CheckCode::TopicAuthorizationFailed,
            Some(CheckCode::BrokerUnreachable)
        ),
        CheckCode::TopicAuthorizationFailed
    );
    assert_eq!(
        escalate(CheckCode::MetadataTimeout, None),
        CheckCode::MetadataTimeout
    );
}

#[test]
fn the_fault_log_drops_unranked_codes_and_stops_at_its_cap() {
    let log = FaultLog::new();
    log.record(CheckCode::Authenticated, "a ready code is not a fault");
    assert!(log.is_empty(), "an unranked code is never stored");

    for _ in 0..(FaultLog::MAX_ENTRIES + 10) {
        log.record(CheckCode::BrokerUnreachable, "down");
    }
    log.record(CheckCode::AuthenticationFailed, "too late to be recorded");
    assert_eq!(
        log.strongest().map(|(c, _)| c),
        Some(CheckCode::BrokerUnreachable),
        "the cap holds, and it holds by dropping the LATEST entry rather than growing"
    );
    assert_eq!(log.codes(), vec![CheckCode::BrokerUnreachable]);
}

#[test]
fn the_fault_log_redacts_the_reason_it_stores() {
    let log = FaultLog::new();
    log.record(
        CheckCode::AuthenticationFailed,
        "sasl.password=hunter2-the-real-one refused by broker AKIAIOSFODNN7EXAMPLE",
    );
    let (_, reason) = log.strongest().expect("recorded");
    assert!(
        !reason.contains("hunter2-the-real-one"),
        "a projected password reached the fault log: {reason}"
    );
    assert!(
        !reason.contains("AKIAIOSFODNN7EXAMPLE"),
        "an access key id reached the fault log: {reason}"
    );
}

#[test]
fn a_check_failure_redacts_at_construction() {
    let f = CheckFailure::new(
        CheckCode::BrokerUnreachable,
        "dial kafka://user:s3cr3t-password@broker:9093 failed",
    );
    assert!(
        !f.message.contains("s3cr3t-password"),
        "userinfo survived redaction: {}",
        f.message
    );
    assert!(f.to_string().starts_with("BrokerUnreachable: "));
}

// ---------------------------------------------------------------------------
// Error classification — D2 §4.2's Kafka row
// ---------------------------------------------------------------------------

#[test]
fn every_kafka_error_code_maps_to_the_code_the_catalogue_names() {
    use RDKafkaErrorCode as C;
    let table: &[(C, CheckCode)] = &[
        (C::SaslAuthenticationFailed, CheckCode::AuthenticationFailed),
        (C::Authentication, CheckCode::AuthenticationFailed),
        (
            C::ClusterAuthorizationFailed,
            CheckCode::ClusterAuthorizationFailed,
        ),
        (
            C::GroupAuthorizationFailed,
            CheckCode::ClusterAuthorizationFailed,
        ),
        (
            C::TopicAuthorizationFailed,
            CheckCode::TopicAuthorizationFailed,
        ),
        (
            C::UnknownTopicOrPartition,
            CheckCode::UnknownTopicOrPartition,
        ),
        (C::UnknownTopic, CheckCode::UnknownTopicOrPartition),
        (C::UnknownPartition, CheckCode::UnknownTopicOrPartition),
        (C::OperationTimedOut, CheckCode::MetadataTimeout),
        (C::TimedOutQueue, CheckCode::MetadataTimeout),
        (C::RequestTimedOut, CheckCode::MetadataTimeout),
        (C::BrokerTransportFailure, CheckCode::BrokerUnreachable),
        (C::AllBrokersDown, CheckCode::BrokerUnreachable),
        (C::Resolve, CheckCode::BrokerUnreachable),
    ];
    for (code, expected) in table {
        assert_eq!(
            classify_error_code(*code, ""),
            *expected,
            "{code:?} must classify as {expected}"
        );
    }
}

/// The SSL split. librdkafka collapses every TLS failure onto one code, and
/// the two halves have different remedies: a CA bundle versus a protocol or
/// cipher mismatch.
#[test]
fn an_ssl_error_splits_on_certificate_trust_and_defaults_to_handshake() {
    assert_eq!(
        classify_error_code(
            RDKafkaErrorCode::SSL,
            "ssl://broker:9093/bootstrap: SSL handshake failed: error:0A000086:SSL \
             routines::certificate verify failed: broker certificate could not be verified"
        ),
        CheckCode::TlsTrustFailed
    );
    assert_eq!(
        classify_error_code(
            RDKafkaErrorCode::SSL,
            "ssl://broker:9093/bootstrap: SSL handshake failed: \
             error:0A00042E:SSL routines::tlsv1 alert protocol version"
        ),
        CheckCode::TlsHandshakeFailed
    );
    for trust in [
        "certificate verify failed",
        "unable to get local issuer certificate",
        "self signed certificate in certificate chain",
        "SELF-SIGNED CERTIFICATE",
        "unable to verify the first certificate",
        "certificate has expired",
    ] {
        assert!(
            is_certificate_trust_reason(trust),
            "{trust} names a trust failure"
        );
    }
    assert!(!is_certificate_trust_reason("tlsv1 alert protocol version"));
    assert!(!is_certificate_trust_reason(""));
}

/// D2 §4.2: "unknown ⇒ the generic failure, never success."
#[test]
fn an_unclassified_kafka_error_is_never_a_ready_code() {
    for code in [
        RDKafkaErrorCode::PolicyViolation,
        RDKafkaErrorCode::InvalidRequest,
        RDKafkaErrorCode::Fail,
        RDKafkaErrorCode::BadMessage,
        RDKafkaErrorCode::NoError,
    ] {
        let got = classify_error_code(code, "");
        assert_eq!(
            got,
            CheckCode::BrokerUnreachable,
            "{code:?} fell through to {got}"
        );
        assert!(
            fault_rank(got) > 0,
            "the fall-through code must be a ranked FAULT, not something that can never \
             displace a timeout"
        );
        for ready in [
            CheckCode::Authenticated,
            CheckCode::TopicsDescribable,
            CheckCode::TopicCreateValidated,
        ] {
            assert_ne!(got, ready, "an unknown code became the ready code {ready}");
        }
    }
}

// ---------------------------------------------------------------------------
// Validate-only CreateTopics — D2 §6.7(b)
// ---------------------------------------------------------------------------

#[test]
fn a_create_validation_result_maps_to_the_topic_create_row() {
    use RDKafkaErrorCode as C;
    let table: &[(C, CheckCode)] = &[
        (C::TopicAlreadyExists, CheckCode::MappedTopicExists),
        (
            C::TopicAuthorizationFailed,
            CheckCode::TopicCreateNotAuthorized,
        ),
        (
            C::ClusterAuthorizationFailed,
            CheckCode::TopicCreateNotAuthorized,
        ),
        (
            C::InvalidReplicationFactor,
            CheckCode::ReplicationFactorExceedsBrokers,
        ),
        (
            C::UnsupportedVersion,
            CheckCode::TopicCreateValidationUnsupported,
        ),
        (
            C::NotImplemented,
            CheckCode::TopicCreateValidationUnsupported,
        ),
        (C::InvalidConfig, CheckCode::TopicConfigRejected),
        (C::InvalidPartitions, CheckCode::TopicConfigRejected),
        (C::PolicyViolation, CheckCode::TopicConfigRejected),
        // The fall-through: a refusal nobody modelled is still a refusal.
        (C::Fail, CheckCode::TopicConfigRejected),
    ];
    for (code, expected) in table {
        let got = classify_create_code(*code);
        assert_eq!(got, *expected, "{code:?} must classify as {expected}");
        assert_ne!(
            got,
            CheckCode::TopicCreateValidated,
            "a broker refusal became the READY code"
        );
    }
}

/// **The guard a double cannot express.** `validate_only(true)` is the only
/// thing standing between a restore preflight and a preflight that CREATES
/// every mapped target topic (D2 §6.7, G12). A fake probe never sends the
/// request, so the assertion is over the source.
#[test]
fn validate_only_is_set_on_every_create_request() {
    let src = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/inventory.rs"),
    )
    .expect("the module this crate ships");
    let code = strip_comments(&src);
    let calls = code.matches("admin.create_topics(").count();
    assert_eq!(
        calls, 1,
        "inventory.rs issues {calls} CreateTopics requests on an admin client; this guard \
         models exactly one"
    );
    assert!(
        code.contains(".validate_only(true)"),
        "the one CreateTopics call in inventory.rs no longer sets `validate_only(true)` — a \
         restore preflight would CREATE every mapped target topic, which D2 §6.7's G12 forbids"
    );
    assert!(
        !code.contains(".validate_only(false)"),
        "inventory.rs sets `validate_only(false)` somewhere"
    );
}

/// Every admin request this module issues carries an explicit request timeout.
/// D2's check contract has a `timeoutSeconds`, and an unbounded admin call
/// would blow it whatever the plan said.
#[test]
fn every_admin_call_is_time_bounded() {
    let src = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/inventory.rs"),
    )
    .expect("the module this crate ships");
    let code = strip_comments(&src);
    let opts = code.matches("AdminOptions::new()").count();
    assert!(
        opts >= 2,
        "only {opts} AdminOptions were built; the scan has gone quiet"
    );
    for (i, _) in code.match_indices("AdminOptions::new()") {
        let window = &code[i..(i + 220).min(code.len())];
        assert!(
            window.contains("request_timeout("),
            "an AdminOptions at byte {i} carries no request_timeout: {window}"
        );
    }
    // And no call anywhere waits forever.
    assert!(
        !code.contains("Timeout::Never"),
        "inventory.rs waits forever somewhere"
    );
}

/// Nothing in this module writes. A `create_topics` is only permitted with the
/// validate-only flag (asserted above); everything else that mutates a cluster
/// must be absent by name.
#[test]
fn the_inventory_module_names_no_write_call() {
    let src = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/inventory.rs"),
    )
    .expect("the module this crate ships");
    let code = strip_comments(&src);
    for forbidden in [
        "delete_topics(",
        "create_partitions(",
        "alter_configs(",
        "incremental_alter_configs(",
        "BaseProducer",
        "FutureProducer",
        ".send(",
        ".subscribe(",
        ".commit(",
    ] {
        assert!(
            !code.contains(forbidden),
            "inventory.rs names `{forbidden}`; a check never writes and never joins a group"
        );
    }
}

fn strip_comments(src: &str) -> String {
    src.lines()
        .filter(|l| {
            let t = l.trim_start();
            !(t.starts_with("//") || t.starts_with("///") || t.starts_with("//!"))
        })
        .collect::<Vec<_>>()
        .join("\n")
}

// ---------------------------------------------------------------------------
// Timeouts
// ---------------------------------------------------------------------------

#[test]
fn the_targeted_budget_is_forty_percent_and_no_call_timeout_exceeds_half_the_budget() {
    assert_eq!(
        ProbeTimeouts::targeted_budget(Duration::from_secs(120)),
        Duration::from_secs(48)
    );
    let t = ProbeTimeouts::for_budget(Duration::from_secs(120));
    assert_eq!(t.targeted_metadata, Duration::from_secs(2));
    for d in [
        t.metadata,
        t.targeted_metadata,
        t.describe_configs,
        t.create_topics,
    ] {
        assert!(
            d <= Duration::from_secs(60),
            "{d:?} is over half the budget"
        );
        assert!(d >= Duration::from_secs(1));
    }
    // A very short budget still issues a request rather than timing out at
    // zero, and never exceeds one second per call more than it has to.
    let tiny = ProbeTimeouts::for_budget(Duration::from_secs(1));
    for d in [
        tiny.metadata,
        tiny.targeted_metadata,
        tiny.describe_configs,
        tiny.create_topics,
    ] {
        assert_eq!(
            d,
            Duration::from_secs(1),
            "a one-second budget clamps to 1 s"
        );
    }
}

/// **The guard for the half of D2 §4.2's [VERIFY U5] that a fake cannot see.**
///
/// Installing a capturing `ClientContext` is not enough: rdkafka 0.36 invokes
/// `ClientContext::error` only from `Client::poll_event`, which only
/// `BaseConsumer::poll` reaches. A metadata-only workflow therefore never runs
/// the callback unless something serves the event queue. This was MEASURED
/// against the compose broker's SASL listener — without the drain the refusal
/// is reported as `BrokerUnreachable` — and the live regression for it is
/// `tests/live.rs::a_refused_scram_credential_is_authentication_failed_and_not_a_timeout`,
/// which is `e2e`-gated. This is the always-on half.
#[test]
fn a_failing_call_serves_the_event_queue_before_it_classifies() {
    let src = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/inventory.rs"),
    )
    .expect("the module this crate ships");
    let code = strip_comments(&src);
    let start = code
        .find("fn fail(&self, base: CheckCode, what: &str) -> CheckFailure {")
        .expect("`fail` is the one place a Kafka call's code is decided");
    let body_end = code[start..]
        .find("\n        }")
        .map(|i| start + i)
        .expect("a closing brace");
    let body = &code[start..body_end];
    assert!(
        body.contains("self.drain_events()"),
        "`fail` no longer serves librdkafka's event queue, so the error callback never runs \
         and a refused SASL credential is reported as a transport failure: {body}"
    );
    assert!(
        code.contains("fn drain_events(&self)") && code.contains("self.consumer.poll("),
        "the drain is what makes the capturing context do anything at all"
    );
}

// ---------------------------------------------------------------------------
// H3 — the three-way per-topic decision (D2 §5.4 signal (ii))
// ---------------------------------------------------------------------------

/// **The arm an independent review's mutant survived in.**
///
/// `TopicAuthorizationFailed ⇒ NotAuthorized` is signal (ii) of D2 §5.4 — the
/// only thing that turns an ACL-limited principal into visibility `limited`
/// rather than `unknown`, and "permission-limited" is a named PLAT-09.1
/// distinguishable outcome. It used to live inside `describe_topic`, where it
/// was reachable only from a broker with real ACLs; the compose stack has
/// none, so a planted `=> Unknown` passed all 33 tests and the whole crate
/// suite. The decision is now `TopicPresence::of_error`, and this is its table.
#[test]
fn a_per_topic_error_maps_to_exactly_one_presence() {
    assert_eq!(
        TopicPresence::of_error(CheckCode::TopicAuthorizationFailed),
        TopicPresence::NotAuthorized,
        "the broker returns TOPIC_AUTHORIZATION_FAILED whether or not the topic exists, so it \
         says nothing about existence and everything about visibility"
    );
    assert_eq!(
        TopicPresence::of_error(CheckCode::UnknownTopicOrPartition),
        TopicPresence::NotFound
    );
    // EVERYTHING ELSE IS `Unknown`, AND NEVER `NotFound`. A leader election, a
    // transient broker state or a code this build does not model are all "I
    // could not tell", and rendering that as "it is not there" is the softer
    // and wrong direction.
    for code in [
        CheckCode::BrokerUnreachable,
        CheckCode::MetadataTimeout,
        CheckCode::AuthenticationFailed,
        CheckCode::TlsTrustFailed,
        CheckCode::TlsHandshakeFailed,
        CheckCode::ClusterAuthorizationFailed,
        CheckCode::Authenticated,
    ] {
        assert_eq!(
            TopicPresence::of_error(code),
            TopicPresence::Unknown,
            "{code} must not be read as a statement about the topic's existence"
        );
    }

    // And the consequence the visibility policy depends on, end to end.
    assert_eq!(
        TopicPresence::of_error(CheckCode::TopicAuthorizationFailed).expected_state(),
        ExpectedTopicState::NotAuthorized
    );
    assert_ne!(
        TopicPresence::of_error(CheckCode::TopicAuthorizationFailed).expected_state(),
        ExpectedTopicState::Unknown,
        "an ACL-restricted cluster reported as merely unknown is the defect this guards"
    );
}

/// The one place the rdkafka arm may make that decision is
/// `TopicPresence::of_error`. A second copy inside `describe_topic` is how the
/// reviewer's mutant became possible in the first place.
#[test]
fn describe_topic_delegates_the_presence_decision_to_the_pure_table() {
    let src = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/inventory.rs"),
    )
    .expect("the module this crate ships");
    let code = strip_comments(&src);
    let start = code
        .find("fn describe_topic(&self, name: &str) -> Result<TopicPresence, CheckFailure> {")
        .expect("the rdkafka targeted probe");
    let end = code[start..]
        .find("\n        }")
        .map(|i| start + i)
        .expect("a closing brace");
    let body = &code[start..end];
    assert!(
        body.contains("TopicPresence::of_error("),
        "describe_topic no longer delegates the three-way decision: {body}"
    );
    assert!(
        !body.contains("TopicPresence::NotAuthorized") && !body.contains("TopicPresence::NotFound"),
        "describe_topic names a presence arm directly, so the decision has two homes again: \
         {body}"
    );
}

// ---------------------------------------------------------------------------
// H2 — the admin half of [VERIFY U5]
// ---------------------------------------------------------------------------

/// **An empty fault log cannot escalate, which is why `fail` dials first.**
///
/// In rdkafka 0.36 the admin client's `CapturingContext` is never consulted
/// (`FaultLog`'s header carries the crate-source citations), so on an
/// admin-first failure — `topic_configs` or `validate_create_topics` as the
/// FIRST call on a fresh connection, which the `InventoryProbe` seam permits —
/// the consumer has never dialled and this log is empty. This is that
/// sequence, over the log itself: empty means the base code stands, which is
/// exactly the `MetadataTimeout`-for-a-refused-credential defect. Populated —
/// which is what dialling the consumer achieves — means the refusal wins.
#[test]
fn an_admin_first_failure_has_nothing_to_escalate_until_the_consumer_has_dialled() {
    let log = FaultLog::new();
    assert!(log.is_empty(), "a fresh connection has observed nothing");
    let before = log.explain(CheckCode::MetadataTimeout, "validate-only CreateTopics");
    assert_eq!(
        before.code,
        CheckCode::MetadataTimeout,
        "with nothing observed there is nothing to escalate with — this is the admin-half \
         defect, reproduced"
    );

    // What `dial_consumer_if_silent` + the drain achieve: the consumer's own
    // error callback fills the log, and the SAME call now classifies correctly.
    log.record(
        CheckCode::AuthenticationFailed,
        "SASL SCRAM-SHA-512 authentication failed",
    );
    let after = log.explain(CheckCode::MetadataTimeout, "validate-only CreateTopics");
    assert_eq!(after.code, CheckCode::AuthenticationFailed);
    assert!(
        after.message.contains("MetadataTimeout"),
        "the message still records what the CALL said: {}",
        after.message
    );
}

/// The always-on half of the fix: `fail` dials the consumer when nothing has,
/// and `broker_configs` was already safe because it issues its own consumer
/// `fetch_metadata` first. A double cannot express either — both are about
/// what a real librdkafka handle has done — so the guard is over the source,
/// and `tests/live.rs::an_admin_first_refusal_is_authentication_failed` is the
/// behavioural regression.
#[test]
fn an_admin_failure_dials_the_consumer_before_it_classifies() {
    let src = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/inventory.rs"),
    )
    .expect("the module this crate ships");
    let code = strip_comments(&src);

    let start = code
        .find("fn fail(&self, base: CheckCode, what: &str) -> CheckFailure {")
        .expect("`fail` is the one place a Kafka call's code is decided");
    let end = code[start..]
        .find("\n        }")
        .map(|i| start + i)
        .expect("a closing brace");
    let body = &code[start..end];
    assert!(
        body.contains("self.dial_consumer_if_silent()"),
        "`fail` no longer dials the consumer, so an admin-first SASL refusal is reported as a \
         timeout: {body}"
    );
    assert!(
        body.contains("self.drain_events()"),
        "`fail` no longer serves the event queue: {body}"
    );

    let dial_start = code
        .find("fn dial_consumer_if_silent(&self) {")
        .expect("the dial helper");
    let dial_end = code[dial_start..]
        .find("\n        }")
        .map(|i| dial_start + i)
        .expect("a closing brace");
    let dial = &code[dial_start..dial_end];
    assert!(
        dial.contains("self.faults.is_empty()"),
        "the dial must be skipped when the log already has the answer: {dial}"
    );
    assert!(
        dial.contains(".consumer") && dial.contains("fetch_metadata"),
        "only a CONSUMER request can fill the log in rdkafka 0.36: {dial}"
    );
    assert!(
        dial.contains("self.timeouts.targeted_metadata"),
        "the dial must be bounded by the shortest timeout this struct carries: {dial}"
    );
}

// ---------------------------------------------------------------------------
// PLAT-07.1 — the saved connection's private CA reaches the check client
// ---------------------------------------------------------------------------

/// **The check client and the drill client derive TLS from ONE place.**
///
/// PLAT-07.1 put the projected trust anchor inside `AuthConfig::ScramSha512`'s
/// `tls_ca_file` and extracted `RdKafkaReader::client_config` so the two
/// security-critical keys — `ssl.endpoint.identification.algorithm` (hostname
/// verification) and `ssl.ca.location` (which trust anchor) — are assertable
/// without a socket. `KafkaInventory` builds through that same function, so
/// this test reads the config the check would dial with and proves both keys
/// are there.
///
/// It matters because the two clients talk to the SAME broker: a check that
/// did not trust the projected CA would report `TlsTrustFailed` for a
/// connection a backup uses happily, and a check that turned hostname
/// verification off would report ready for one the engine's rustls client will
/// refuse.
#[test]
fn a_tls_ca_file_reaches_the_check_clients_ssl_ca_location() {
    const CA: &str = "/etc/logweir/trust/source-ca.pem";
    let scram = |tls: bool, ca: Option<&str>| {
        AuthConfig::from_spec(
            &logweir_core::spec::AuthSpec::ScramSha512 {
                username: "logweir".to_string(),
                tls,
            },
            Some("projected".to_string()),
        )
        .expect("interface I1 builds the auth")
        .with_tls_ca_file(ca.map(str::to_string))
    };
    let settings = |auth: AuthConfig| ConnectionSettings {
        bootstrap_servers: vec!["b0.orders:9093".to_string()],
        auth,
        timeouts: ProbeTimeouts::for_budget(Duration::from_secs(30)),
    };

    // 1. TLS WITH A PROJECTED CA: both keys set, and the CA is the path
    //    verbatim — nothing derived from it.
    let cfg = check_client_config(&settings(scram(true, Some(CA)).expect("TLS accepts a CA")))
        .expect("configures");
    assert_eq!(
        cfg.get("ssl.ca.location"),
        Some(CA),
        "the check client must trust the CA the connection projects, or it reports \
         TlsTrustFailed for a broker a backup uses happily"
    );
    assert_eq!(
        cfg.get("ssl.endpoint.identification.algorithm"),
        Some("https"),
        "hostname verification is pinned, so a librdkafka upgrade cannot quietly turn it off \
         and the two clients cannot disagree with the engine's rustls client"
    );
    assert_eq!(cfg.get("security.protocol"), Some("SASL_SSL"));
    assert_eq!(cfg.get("sasl.mechanism"), Some("SCRAM-SHA-512"));

    // 2. TLS WITHOUT A CA: hostname verification still pinned, and NO
    //    `ssl.ca.location` — an empty one would make librdkafka skip the
    //    default verify paths and trust nothing.
    let cfg = check_client_config(&settings(scram(true, None).expect("no CA is fine")))
        .expect("configures");
    assert_eq!(cfg.get("ssl.ca.location"), None);
    assert_eq!(
        cfg.get("ssl.endpoint.identification.algorithm"),
        Some("https")
    );

    // 3. SASL WITHOUT TLS: neither key, and the protocol is the plaintext one.
    let cfg = check_client_config(&settings(scram(false, None).expect("plaintext SASL")))
        .expect("configures");
    assert_eq!(cfg.get("security.protocol"), Some("SASL_PLAINTEXT"));
    assert_eq!(cfg.get("ssl.ca.location"), None);
    assert_eq!(cfg.get("ssl.endpoint.identification.algorithm"), None);

    // 4. PLAINTEXT: no TLS keys at all. A check never upgrades a transport
    //    because a CA happens to be around (D-SEAMS S5).
    let cfg = check_client_config(&settings(
        AuthConfig::from_spec(&logweir_core::spec::AuthSpec::Plaintext, None)
            .expect("interface I1"),
    ))
    .expect("configures");
    assert_eq!(cfg.get("security.protocol"), Some("PLAINTEXT"));
    assert_eq!(cfg.get("ssl.ca.location"), None);

    // 5. A CA WITHOUT TLS IS REFUSED, at the same place the drill refuses it.
    assert!(
        scram(false, Some(CA)).is_err(),
        "a CA on a non-TLS connection is a silent downgrade, refused by `with_tls_ca_file`"
    );

    // 6. And the check's own identity is the ONE thing overridden afterwards.
    let cfg = check_client_config(&settings(scram(true, Some(CA)).expect("TLS accepts a CA")))
        .expect("configures");
    assert_eq!(
        cfg.get("client.id"),
        Some("logweir-check"),
        "a broker operator must be able to tell a read-only check from a run that moves data"
    );
    assert_eq!(
        cfg.get("allow.auto.create.topics"),
        Some("false"),
        "inherited from the shared helper, not re-set here"
    );
}

/// The check client's config is built by ONE function, and it is the drill's.
///
/// A behavioural test cannot see "which function built this map" — the map is
/// identical either way today — so the no-duplication half is a source scan.
/// It is what stops the two clients drifting again: the copy this replaced had
/// already drifted, setting `ssl.ca.location` from a field of its own with no
/// TLS check and no hostname verification at all.
#[test]
fn the_check_client_builds_no_tls_configuration_of_its_own() {
    let src = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/inventory.rs"),
    )
    .expect("the module this crate ships");
    let code = strip_comments(&src);
    assert!(
        code.contains("RdKafkaReader::client_config("),
        "the check client must derive its configuration from the drill's one implementation"
    );
    for forbidden in [
        "\"ssl.ca.location\"",
        "\"ssl.endpoint.identification.algorithm\"",
        "\"security.protocol\"",
        "\"sasl.mechanism\"",
        "\"sasl.password\"",
        "\"sasl.username\"",
    ] {
        assert!(
            !code.contains(forbidden),
            "inventory.rs sets `{forbidden}` itself; hostname verification and the trust \
             anchor are controls, and a second copy is a second place they can drift"
        );
    }
}
