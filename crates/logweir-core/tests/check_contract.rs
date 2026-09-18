//! The pure check contract (decision D2 §4.1): plan, frames, codes, redaction,
//! visibility and the binding digest.
//!
//! The redaction section is written as a MUTANT HARNESS rather than as a list
//! of happy-path assertions: `redaction_rules()` is a list, so each test runs
//! the redactor with one rule removed and asserts the secret survives, then
//! runs it whole and asserts it does not. A redactor whose clause is deleted
//! by a future edit fails here rather than leaking in production — which is
//! the only interesting property a redactor has.

use chrono::{DateTime, Utc};
use logweir_core::check_contract::{
    advisory_warnings, aggregate, aggregate_expires_at, apply_rules, frames, inputs_digest, redact,
    redaction_rules, stale_reasons, topic_tsv, topic_tsv_sha256, visibility, ApprovalRef,
    Attestation, Authority, BindingInputs, CaBundleRef, CheckCode, CheckId, CheckOperation,
    CheckOutcome, CheckPlan, CheckPlanError, CheckPlanKind, CheckRequest, CheckResult,
    CheckResultError, CheckState, ConnectionPlan, CredentialMode, DestinationAccessRequest,
    DestinationPlan, EndFrame, EvidenceFetchRequest, EvidenceObjectRequest, ExpectedSummary,
    FrameExpectations, Gating, OverallState, Referent, RosterRef, StaleReason, Stream, TopicEntry,
    TopicInventoryRequest, TruncationReason, VisibilityBasis, VisibilitySignals, VisibilityState,
    CHECK_CONTRACT_VERSION, CHECK_PLAN_CONTRACT, CHECK_RESULT_CONTRACT, FRAME_MAX_BYTES,
    MAX_CHECK_ENTRIES, MESSAGE_MAX_CHARS, PART_MAX_BASE64_CHARS, REDACTED,
};
use logweir_core::destination::{
    Addressing, DestinationLocation, DestinationRole, StorageProvider, TransportSecurity,
};
use logweir_core::ids::sha256_prefixed;
use std::collections::BTreeMap;

fn t(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
}

// ------------------------------------------------------------- vocabularies

/// Every code must be usable verbatim as a `metav1.Condition.reason`, which
/// Kubernetes validates against `^[A-Za-z]([A-Za-z0-9_,:]*[A-Za-z0-9_])?$`
/// with a 1024-character cap. A code that fails it makes the status PATCH
/// fail at the API server, after the check has already run.
#[test]
fn every_code_is_a_valid_condition_reason() {
    fn is_reason(s: &str) -> bool {
        let mut cs = s.chars();
        let Some(first) = cs.next() else { return false };
        if !first.is_ascii_alphabetic() {
            return false;
        }
        if s.len() > 1024 {
            return false;
        }
        let last = s.chars().next_back().expect("non-empty");
        if !(last.is_ascii_alphanumeric() || last == '_') {
            return false;
        }
        s.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == ',' || c == ':')
    }
    assert!(CheckCode::ALL.len() > 120, "the table shrank unexpectedly");
    for c in CheckCode::ALL {
        assert!(is_reason(c.as_str()), "`{c}` is not a condition reason");
    }
}

/// The table is CLOSED and its spellings are the wire format, so a duplicate
/// or a rename is a contract change. `parse` must also refuse anything not in
/// it — a fallback member would turn "I did not recognise this" into a
/// specific claim.
#[test]
fn the_code_table_is_closed_and_round_trips() {
    let mut seen = std::collections::BTreeSet::new();
    for c in CheckCode::ALL {
        assert!(seen.insert(c.as_str()), "duplicate code `{c}`");
        assert_eq!(CheckCode::parse(c.as_str()), Some(*c));
        let json = serde_json::to_string(c).unwrap();
        assert_eq!(json, format!("\"{}\"", c.as_str()));
        assert_eq!(serde_json::from_str::<CheckCode>(&json).unwrap(), *c);
    }
    assert_eq!(CheckCode::parse("AccessDeniedish"), None);
    assert!(serde_json::from_str::<CheckCode>("\"NotACode\"").is_err());
}

/// The check ids are dotted `category.name` and `category()` is derived from
/// the spelling — one table, not two.
#[test]
fn every_check_id_has_a_category_and_round_trips() {
    let mut seen = std::collections::BTreeSet::new();
    for id in CheckId::ALL {
        assert!(seen.insert(id.as_str()), "duplicate id `{id}`");
        let (head, tail) = id.as_str().split_once('.').expect("ids are dotted");
        assert_eq!(id.category(), head);
        assert!(!tail.is_empty());
        assert_eq!(CheckId::parse(id.as_str()), Some(*id));
    }
    assert!(
        CheckId::ALL.len() <= 64,
        "a result carries at most 64 entries"
    );
    assert_eq!(CheckId::ConnectionResolved.as_str(), "connection.resolved");
    assert_eq!(CheckId::TargetMappedTopics.category(), "target");
    assert_eq!(CheckId::parse("connection.nope"), None);
}

/// D2 §4.3 derives the check Job name from these; two kinds sharing a
/// discriminator would collide two Jobs onto one name.
#[test]
fn plan_kind_discriminators_are_distinct() {
    let mut seen = std::collections::BTreeSet::new();
    for k in CheckPlanKind::ALL {
        assert!(seen.insert(k.job_discriminator()));
        assert_eq!(k.job_discriminator().len(), 2);
    }
    assert_eq!(CheckPlanKind::TopicInventory.job_discriminator(), "td");
    assert_eq!(CheckPlanKind::RestorePreflight.job_discriminator(), "rp");
    assert_eq!(CheckPlanKind::EvidenceFetch.job_discriminator(), "ev");
}

// ---------------------------------------------------------------- the plan

fn connection() -> ConnectionPlan {
    ConnectionPlan {
        bootstrap_servers: vec!["kafka:9092".into()],
        auth_mode: "scramSha512".into(),
        username: Some("backup".into()),
        password_env: Some("LOGWEIR_SOURCE_PASSWORD".into()),
        tls: Some(true),
        ca_file: Some("/check/source-ca.pem".into()),
        principal: "User:backup".into(),
    }
}

fn location() -> DestinationLocation {
    DestinationLocation {
        provider: StorageProvider::S3,
        bucket: "kafka-backups".into(),
        prefix: "team-a".into(),
        region: Some("us-east-1".into()),
        endpoint: Some("https://minio.storage.svc:9000".into()),
        addressing: Addressing::PathStyle,
        transport: TransportSecurity::Tls,
    }
}

fn destination() -> DestinationPlan {
    let loc = location();
    DestinationPlan {
        name: "primary".into(),
        uid: "11111111-2222-3333-4444-555555555555".into(),
        location_digest: loc.location_digest(),
        location: loc,
        ca_file: None,
        credentials: CredentialMode::Static,
    }
}

fn inventory_plan() -> CheckPlan {
    CheckPlan {
        contract: CHECK_PLAN_CONTRACT.into(),
        contract_version: CHECK_CONTRACT_VERSION,
        subject_uid: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee".into(),
        timeout_seconds: 60,
        policy_digest: Some(sha256_prefixed(b"policy")),
        request: CheckRequest::TopicInventory(TopicInventoryRequest {
            connection: connection(),
            include_internal: false,
            expected_topics: vec!["orders".into(), "payments".into()],
            max_topics: 20_000,
            relay_budget_bytes: 6 * 1024 * 1024,
        }),
    }
}

fn plan_bytes(p: &CheckPlan) -> Vec<u8> {
    serde_json::to_vec(p).unwrap()
}

/// D2 §4.2's startup order, steps 3 and 4, as one function: a plan whose
/// bytes, contract, version or subject UID do not match what the Job env pins
/// is refused BEFORE any client is built. The refusal carries
/// `CheckContractMismatch` and nothing else.
#[test]
fn a_plan_is_verified_before_anything_is_built() {
    let plan = inventory_plan();
    let bytes = plan_bytes(&plan);
    let sha = sha256_prefixed(&bytes);
    let uid = plan.subject_uid.clone();

    let got = CheckPlan::parse_and_verify(&bytes, &sha, &uid).unwrap();
    assert_eq!(got.kind(), CheckPlanKind::TopicInventory);

    // The bytes changed under the digest.
    let err = CheckPlan::parse_and_verify(b"{}", &sha, &uid).unwrap_err();
    assert!(matches!(err, CheckPlanError::PlanSha256 { .. }));
    assert_eq!(err.code(), CheckCode::CheckContractMismatch);

    // The subject UID is not this Job's.
    let err = CheckPlan::parse_and_verify(&bytes, &sha, "someone-else").unwrap_err();
    assert!(matches!(err, CheckPlanError::SubjectUid { .. }));

    // A newer contract.
    let mut newer = plan.clone();
    newer.contract_version = 2;
    let b = plan_bytes(&newer);
    let err = CheckPlan::parse_and_verify(&b, &sha256_prefixed(&b), &uid).unwrap_err();
    assert!(matches!(err, CheckPlanError::ContractVersion(2)));

    let mut other = plan.clone();
    other.contract = "logweir.dev/check-plan/v2".into();
    let b = plan_bytes(&other);
    let err = CheckPlan::parse_and_verify(&b, &sha256_prefixed(&b), &uid).unwrap_err();
    assert!(matches!(err, CheckPlanError::Contract(_)));
}

/// `deny_unknown_fields`: a field a newer controller added is a REFUSAL, not
/// a silently ignored instruction. A runner that ignored it would do less than
/// the controller believes it did.
#[test]
fn an_unknown_plan_field_is_refused() {
    let plan = inventory_plan();
    let mut v: serde_json::Value = serde_json::from_slice(&plan_bytes(&plan)).unwrap();
    v["request"]["topicInventory"]["excludeInternalPrefixes"] = serde_json::json!(["_confluent"]);
    let bytes = serde_json::to_vec(&v).unwrap();
    let err = CheckPlan::parse_and_verify(&bytes, &sha256_prefixed(&bytes), &plan.subject_uid)
        .unwrap_err();
    assert!(matches!(err, CheckPlanError::Parse(_)), "{err:?}");

    let mut v: serde_json::Value = serde_json::from_slice(&plan_bytes(&plan)).unwrap();
    v["unexpected"] = serde_json::json!(true);
    let bytes = serde_json::to_vec(&v).unwrap();
    assert!(matches!(
        CheckPlan::parse_and_verify(&bytes, &sha256_prefixed(&bytes), &plan.subject_uid),
        Err(CheckPlanError::Parse(_))
    ));
}

/// The budgets are what make the relay, the etcd footprint and the Kafka call
/// count bounded, so they are checked on the READ side too.
#[test]
fn plan_bounds_are_enforced_on_the_read_side() {
    let mut plan = inventory_plan();
    let CheckRequest::TopicInventory(r) = &mut plan.request else {
        unreachable!()
    };
    r.expected_topics = (0..501).map(|i| format!("t{i}")).collect();
    assert!(plan.validate().is_err());
    let CheckRequest::TopicInventory(r) = &mut plan.request else {
        unreachable!()
    };
    r.expected_topics = vec!["orders".into()];
    r.max_topics = 50_001;
    assert!(plan.validate().is_err());
    let CheckRequest::TopicInventory(r) = &mut plan.request else {
        unreachable!()
    };
    r.max_topics = 50_000;
    assert!(plan.validate().is_ok());

    plan.timeout_seconds = 0;
    assert!(plan.validate().is_err());
    plan.timeout_seconds = 601;
    assert!(plan.validate().is_err());
    plan.timeout_seconds = 600;
    assert!(plan.validate().is_ok());
}

/// An evidence fetch reads with the `evidenceRead` grant and NOTHING else,
/// at most three objects, payload 1 MiB and sidecar 64 KiB. The role check is
/// the one that keeps an evidence-fetch Job from being turned into an
/// archive-read oracle by a controller bug.
#[test]
fn an_evidence_fetch_is_bounded_and_evidence_read_only() {
    let object = |role: DestinationRole, stream: Stream, max_bytes: u64| EvidenceObjectRequest {
        role,
        key: "logweir/drills/x/receipt.json".into(),
        max_bytes,
        stream,
    };
    let mut plan = CheckPlan {
        contract: CHECK_PLAN_CONTRACT.into(),
        contract_version: CHECK_CONTRACT_VERSION,
        subject_uid: "u".into(),
        timeout_seconds: 60,
        policy_digest: None,
        request: CheckRequest::EvidenceFetch(EvidenceFetchRequest {
            destination: destination(),
            objects: vec![
                object(
                    DestinationRole::EvidenceRead,
                    Stream::EvidencePayload,
                    1024 * 1024,
                ),
                object(
                    DestinationRole::EvidenceRead,
                    Stream::EvidenceSidecar,
                    64 * 1024,
                ),
            ],
        }),
    };
    assert!(plan.validate().is_ok());

    let CheckRequest::EvidenceFetch(r) = &mut plan.request else {
        unreachable!()
    };
    r.objects[0].role = DestinationRole::ArchiveRead;
    assert!(
        plan.validate().is_err(),
        "only evidenceRead may fetch evidence"
    );

    let CheckRequest::EvidenceFetch(r) = &mut plan.request else {
        unreachable!()
    };
    r.objects[0].role = DestinationRole::EvidenceRead;
    r.objects[1].max_bytes = 64 * 1024 + 1;
    assert!(plan.validate().is_err(), "the sidecar cap is 64 KiB");

    let CheckRequest::EvidenceFetch(r) = &mut plan.request else {
        unreachable!()
    };
    r.objects[1].max_bytes = 64 * 1024;
    r.objects.push(object(
        DestinationRole::EvidenceRead,
        Stream::EvidencePayload,
        1,
    ));
    r.objects.push(object(
        DestinationRole::EvidenceRead,
        Stream::EvidencePayload,
        1,
    ));
    assert!(plan.validate().is_err(), "at most three objects");
}

#[test]
fn a_destination_access_plan_needs_between_one_and_four_roles() {
    let mut plan = CheckPlan {
        contract: CHECK_PLAN_CONTRACT.into(),
        contract_version: CHECK_CONTRACT_VERSION,
        subject_uid: "u".into(),
        timeout_seconds: 120,
        policy_digest: None,
        request: CheckRequest::DestinationAccess(DestinationAccessRequest {
            destination: destination(),
            roles: vec![DestinationRole::ArchiveRead],
            write_probe: false,
        }),
    };
    assert!(plan.validate().is_ok());
    let CheckRequest::DestinationAccess(r) = &mut plan.request else {
        unreachable!()
    };
    r.roles.clear();
    assert!(plan.validate().is_err());
}

// ------------------------------------------------------------------ frames

fn expectations(plan_sha: &str, uid: &str) -> FrameExpectations {
    FrameExpectations {
        plan_sha256: plan_sha.to_string(),
        subject_uid: uid.to_string(),
    }
}

fn relay_of(
    lines: &[String],
    expect: &FrameExpectations,
) -> Result<logweir_core::check_contract::CheckRelay, logweir_core::check_contract::FrameError> {
    let mut d = frames::Decoder::new();
    for l in lines {
        d.push_line(l)?;
    }
    d.finish(expect)
}

fn write_all(
    topics: &[TopicEntry],
    streams: &[(Stream, Vec<u8>)],
    sha: &str,
    uid: &str,
) -> Vec<String> {
    let mut lines = Vec::new();
    for t in topics {
        lines.push(frames::write_topic_line(t).unwrap());
    }
    let mut summary: BTreeMap<Stream, (Vec<u8>, usize)> = BTreeMap::new();
    for (s, bytes) in streams {
        let parts = frames::write_parts(*s, bytes).unwrap();
        summary.insert(*s, (bytes.clone(), parts.len()));
        lines.extend(parts);
    }
    let end = frames::end_frame(
        sha,
        uid,
        &summary,
        if topics.is_empty() {
            None
        } else {
            Some(topics)
        },
    );
    lines.push(frames::write_end(&end).unwrap());
    lines
}

#[test]
fn frames_round_trip_topics_and_streams() {
    let topics = vec![
        TopicEntry::new("orders", 3),
        TopicEntry {
            expected: true,
            ..TopicEntry::new("payments", 12)
        },
        TopicEntry {
            internal: true,
            ..TopicEntry::new("__consumer_offsets", 50)
        },
        TopicEntry {
            error: Some(CheckCode::TopicAuthorizationFailed),
            ..TopicEntry::new("secret-topic", 0)
        },
    ];
    let result = CheckResult::new(CheckPlanKind::TopicInventory);
    let body = result.to_canonical_json().unwrap();
    let lines = write_all(
        &topics,
        &[(Stream::Result, body.clone())],
        "sha256:plan",
        "uid-1",
    );
    let relay = relay_of(&lines, &expectations("sha256:plan", "uid-1")).unwrap();
    assert_eq!(relay.topics, topics);
    assert_eq!(relay.stream(Stream::Result).unwrap(), body.as_slice());
    assert_eq!(relay.end.contract, CHECK_RESULT_CONTRACT);
    assert_eq!(
        relay.result().unwrap().unwrap().kind,
        CheckPlanKind::TopicInventory
    );
    // The flags rendering is the canonical TSV, which is what the chunks store
    // and what `topicsSha256` is taken over.
    assert_eq!(topics[0].flags(), "-");
    assert_eq!(topics[2].flags(), "internal");
    assert_eq!(topics[3].flags(), "error:TopicAuthorizationFailed");
    assert_eq!(topic_tsv(&topics).lines().next().unwrap(), "orders\t3\t-");
    assert_eq!(
        relay.end.topic_lines.as_ref().unwrap().sha256,
        topic_tsv_sha256(&topics)
    );
}

/// The framework tests D2 §12 names: a missing end line, a wrong plan digest
/// and a part-digest mismatch are all `ResultUnreadable`, WITHOUT log content.
#[test]
fn a_broken_relay_is_result_unreadable() {
    let topics = vec![TopicEntry::new("orders", 3)];
    let body = b"{\"contract\":\"x\"}".to_vec();
    let good = write_all(
        &topics,
        &[(Stream::Result, body.clone())],
        "sha256:plan",
        "uid-1",
    );
    let expect = expectations("sha256:plan", "uid-1");
    assert!(relay_of(&good, &expect).is_ok());

    // (a) no end line.
    let mut no_end = good.clone();
    no_end.pop();
    let err = relay_of(&no_end, &expect).unwrap_err();
    assert_eq!(err.code(), CheckCode::ResultUnreadable);
    assert!(format!("{err}").contains("no end frame"));

    // (b) the end frame names another plan, or another subject.
    assert!(relay_of(&good, &expectations("sha256:other", "uid-1")).is_err());
    assert!(relay_of(&good, &expectations("sha256:plan", "uid-2")).is_err());

    // (c) a part was tampered with, so the stream digest no longer matches.
    let mut tampered = good.clone();
    let i = tampered
        .iter()
        .position(|l| l.starts_with("logweir-check-part="))
        .unwrap();
    tampered[i] = frames::write_parts(Stream::Result, b"different").unwrap()[0].clone();
    let err = relay_of(&tampered, &expect).unwrap_err();
    assert!(format!("{err}").contains("declared digest"), "{err}");

    // (d) a topic line was dropped, so the count and digest no longer match.
    let mut dropped = good.clone();
    dropped.retain(|l| !l.starts_with("logweir-check-topic="));
    assert!(relay_of(&dropped, &expect).is_err());

    // (e) a topic line was ADDED, which keeps the count wrong in the other
    // direction — the mutant for a decoder that only checked the digest of
    // what it happened to read.
    let mut added = good.clone();
    added.insert(
        0,
        frames::write_topic_line(&TopicEntry::new("extra", 1)).unwrap(),
    );
    assert!(relay_of(&added, &expect).is_err());

    // (f) a frame after the end line.
    let mut after = good.clone();
    after.push(frames::write_topic_line(&TopicEntry::new("late", 1)).unwrap());
    assert!(relay_of(&after, &expect).is_err());

    // (g) a part for a stream the end frame does not declare.
    let mut extra_stream = good.clone();
    let end = extra_stream.pop().unwrap();
    extra_stream.extend(frames::write_parts(Stream::Details, b"sneaky").unwrap());
    extra_stream.push(end);
    assert!(relay_of(&extra_stream, &expect).is_err());
}

/// The count and the digest are TWO guards, and each catches a case the other
/// cannot. Asserted separately so neither can be deleted as "redundant": the
/// first mutant round for this file found that disabling the count check alone
/// left every other test green, because every case they shared was also a
/// digest change.
#[test]
fn the_topic_count_and_the_topic_digest_are_separate_guards() {
    let topics = vec![TopicEntry::new("orders", 3)];
    let expect = expectations("sha256:plan", "uid-1");

    // (a) only the COUNT catches this: the digest is the correct digest of the
    // one line that arrived, and the end frame claims five.
    let mut lines = write_all(&topics, &[], "sha256:plan", "uid-1");
    let end = EndFrame {
        contract: CHECK_RESULT_CONTRACT.into(),
        plan_sha256: "sha256:plan".into(),
        subject_uid: "uid-1".into(),
        streams: BTreeMap::new(),
        topic_lines: Some(logweir_core::check_contract::TopicLineSummary {
            count: 5,
            sha256: topic_tsv_sha256(&topics),
        }),
    };
    *lines.last_mut().unwrap() = frames::write_end(&end).unwrap();
    let err = relay_of(&lines, &expect).unwrap_err();
    assert!(format!("{err}").contains("topic lines arrived"), "{err}");

    // (b) only the DIGEST catches this: one line arrived, the end frame claims
    // one, and it is a different line.
    let other = vec![TopicEntry::new("payments", 3)];
    let mut lines = write_all(&topics, &[], "sha256:plan", "uid-1");
    let end = EndFrame {
        contract: CHECK_RESULT_CONTRACT.into(),
        plan_sha256: "sha256:plan".into(),
        subject_uid: "uid-1".into(),
        streams: BTreeMap::new(),
        topic_lines: Some(logweir_core::check_contract::TopicLineSummary {
            count: 1,
            sha256: topic_tsv_sha256(&other),
        }),
    };
    *lines.last_mut().unwrap() = frames::write_end(&end).unwrap();
    let err = relay_of(&lines, &expect).unwrap_err();
    assert!(
        format!("{err}").contains("do not match the digest"),
        "{err}"
    );
}

/// Non-frame lines are IGNORED, so the same decoder serves the `KafkaCluster`
/// probe, which prints its own I14 lines on the same stdout (D2 §4.5).
#[test]
fn non_frame_lines_are_ignored() {
    let topics = vec![TopicEntry::new("orders", 3)];
    let mut lines = write_all(&topics, &[], "sha256:plan", "uid-1");
    lines.insert(0, "logweir-probe-clusterid=M29I2S7FQPyHBEX12Vx7XA".into());
    lines.insert(1, "  something a library logged".into());
    let relay = relay_of(&lines, &expectations("sha256:plan", "uid-1")).unwrap();
    assert_eq!(relay.topics.len(), 1);
}

/// Every frame stays under the CRI partial-line split, and a part carries at
/// most 3,000 base64 characters.
#[test]
fn frames_stay_inside_the_line_bound() {
    let payload: Vec<u8> = (0..40_000u32).map(|i| (i % 251) as u8).collect();
    let parts = frames::write_parts(Stream::EvidencePayload, &payload).unwrap();
    assert!(parts.len() > 1);
    for p in &parts {
        // The writer appends a newline, so the frame occupies len()+1 bytes.
        assert!(p.len() < FRAME_MAX_BYTES, "{} bytes", p.len());
        let b64 = p.rsplit_once(':').unwrap().1;
        assert!(b64.len() <= PART_MAX_BASE64_CHARS);
    }
    // A 249-character topic name (Kafka's own limit) still fits.
    let long = TopicEntry {
        expected: true,
        error: Some(CheckCode::UnknownTopicOrPartition),
        ..TopicEntry::new(&"t".repeat(249), 999_999)
    };
    let line = frames::write_topic_line(&long).unwrap();
    assert!(line.len() < FRAME_MAX_BYTES);

    // An over-long line is refused by the decoder too, not only by the writer.
    let mut d = frames::Decoder::new();
    let huge = format!("logweir-check-topic={}", "x".repeat(FRAME_MAX_BYTES));
    assert!(d.push_line(&huge).is_err());
}

/// An empty payload still produces one part, so "present and empty" stays
/// distinguishable from "absent".
#[test]
fn an_empty_stream_is_not_an_absent_stream() {
    let lines = write_all(
        &[],
        &[(Stream::Details, Vec::new())],
        "sha256:plan",
        "uid-1",
    );
    let relay = relay_of(&lines, &expectations("sha256:plan", "uid-1")).unwrap();
    assert_eq!(relay.stream(Stream::Details), Some(&[][..]));
    assert_eq!(relay.stream(Stream::Result), None);
    assert!(relay.end.topic_lines.is_none());
}

/// PLAT-09.1's "large catalog", at the frame layer: 50,000 entries round-trip
/// and the digest is over the canonical TSV.
#[test]
fn topic_lines_roundtrip_50000() {
    let topics: Vec<TopicEntry> = (0..50_000)
        .map(|i| TopicEntry::new(&format!("bulk-{i:05}"), (i % 24 + 1) as u32))
        .collect();
    let lines = write_all(&topics, &[], "sha256:plan", "uid-1");
    assert_eq!(lines.len(), 50_001);
    let relay = relay_of(&lines, &expectations("sha256:plan", "uid-1")).unwrap();
    assert_eq!(relay.topics.len(), 50_000);
    assert_eq!(relay.topics[49_999].name, "bulk-49999");
    assert_eq!(
        relay.end.topic_lines.as_ref().unwrap().sha256,
        topic_tsv_sha256(&topics)
    );
}

/// The decoder holds a second, independent bound on what a runner can make a
/// controller accumulate.
#[test]
fn the_decoder_has_a_budget() {
    let mut d = frames::Decoder::with_budget(300);
    let line = frames::write_topic_line(&TopicEntry::new(&"x".repeat(100), 1)).unwrap();
    assert!(d.push_line(&line).is_ok());
    assert!(d.push_line(&line).is_ok());
    assert!(d.push_line(&line).is_err());
}

#[test]
fn a_malformed_frame_never_panics() {
    let cases = [
        "logweir-check-topic=",
        "logweir-check-topic=name",
        "logweir-check-topic=name\tnotanumber\t-",
        "logweir-check-topic=name\t1\tbogusflag",
        "logweir-check-topic=\t1\t-",
        "logweir-check-part=result",
        "logweir-check-part=result:1/0:AA",
        "logweir-check-part=result:0/1:AA",
        "logweir-check-part=result:2/1:AA",
        "logweir-check-part=nosuchstream:1/1:AA",
        "logweir-check-part=result:x/1:AA",
        "logweir-check-end=not json",
    ];
    for c in cases {
        let mut d = frames::Decoder::new();
        let pushed = d.push_line(c);
        let finished = pushed.and_then(|()| d.finish(&expectations("s", "u")).map(|_| ()));
        assert!(finished.is_err(), "`{c}` must not be accepted");
    }
    // A duplicated part sequence.
    let mut d = frames::Decoder::new();
    let p = frames::write_parts(Stream::Result, b"x").unwrap()[0].clone();
    d.push_line(&p).unwrap();
    assert!(d.push_line(&p).is_err());
}

#[test]
fn a_part_that_is_not_base64_is_refused() {
    let body = b"abc".to_vec();
    let mut lines = write_all(&[], &[(Stream::Result, body)], "sha256:plan", "uid-1");
    let end = lines.pop().unwrap();
    lines[0] = "logweir-check-part=result:1/1:!!!!".into();
    lines.push(end);
    assert!(relay_of(&lines, &expectations("sha256:plan", "uid-1")).is_err());
}

// ------------------------------------------------------------- internal rule

/// D2 §5.3: `__` and nothing else. `_schemas` and `_confluent-*` have
/// CONFIGURABLE names, and guessing them would silently drop user data from a
/// backup — the single most expensive wrong answer this module can give.
#[test]
fn double_underscore_is_internal() {
    for name in [
        "__consumer_offsets",
        "__transaction_state",
        "__share_group_state",
    ] {
        assert!(TopicEntry::name_is_internal(name), "{name}");
    }
    for name in [
        "_schemas",
        "_confluent-metrics",
        "orders",
        "connect-configs",
        "_",
    ] {
        assert!(!TopicEntry::name_is_internal(name), "{name}");
    }
}

// --------------------------------------------------------------- aggregation

fn outcome(id: CheckId, state: CheckState, gating: Gating) -> CheckOutcome {
    CheckOutcome::new(id, state, gating, Authority::CheckJob, CheckCode::Resolved)
}

#[test]
fn aggregation_follows_the_blocking_rules() {
    let ready = outcome(
        CheckId::ConnectionResolved,
        CheckState::Ready,
        Gating::Blocking,
    );
    let not_ready = outcome(
        CheckId::DestinationArchiveListable,
        CheckState::NotReady,
        Gating::Blocking,
    );
    let unknown = outcome(
        CheckId::ConnectionAuthenticated,
        CheckState::Unknown,
        Gating::Blocking,
    );
    let skipped = outcome(
        CheckId::ArchiveSegments,
        CheckState::Skipped,
        Gating::Blocking,
    );
    let advisory_bad = outcome(
        CheckId::ConfigurationPolicy,
        CheckState::NotReady,
        Gating::Advisory,
    );

    assert_eq!(aggregate(std::slice::from_ref(&ready)), OverallState::Ready);
    assert_eq!(
        aggregate(&[ready.clone(), not_ready.clone(), unknown.clone()]),
        OverallState::NotReady,
        "notReady wins over unknown"
    );
    assert_eq!(
        aggregate(&[ready.clone(), unknown.clone()]),
        OverallState::Unknown
    );
    assert_eq!(
        aggregate(&[ready.clone(), skipped]),
        OverallState::Unknown,
        "a skipped blocking check keeps the overall state unknown"
    );
    assert_eq!(
        aggregate(&[ready.clone(), advisory_bad.clone()]),
        OverallState::Ready,
        "an advisory failure is a warning, not a verdict"
    );
    assert_eq!(advisory_warnings(&[ready.clone(), advisory_bad]).len(), 1);

    // An EMPTY set is unknown. "Nothing was checked" is not "everything
    // passed"; a bug that dropped every check must not report green.
    assert_eq!(aggregate(&[]), OverallState::Unknown);
    assert_eq!(
        aggregate(&[outcome(
            CheckId::ConnectionTopicsReadable,
            CheckState::Ready,
            Gating::ExecutionOnly
        )]),
        OverallState::Unknown
    );
}

/// D2 §6.3: an execution-only check is ALWAYS `unknown`, whatever a caller
/// asks for. The constructor is the enforcement point, so no call site can
/// publish a green execution-only check.
#[test]
fn an_execution_only_check_is_forced_unknown() {
    let c = outcome(
        CheckId::ConnectionTopicsReadable,
        CheckState::Ready,
        Gating::ExecutionOnly,
    );
    assert_eq!(c.state, CheckState::Unknown);
    assert_eq!(c.category, "connection");
}

#[test]
fn expiry_is_the_minimum_over_non_skipped_checks() {
    let a = outcome(
        CheckId::ConnectionResolved,
        CheckState::Ready,
        Gating::Blocking,
    )
    .with_times(t("2026-09-15T00:00:00Z"), t("2026-09-15T00:15:00Z"));
    let b = outcome(
        CheckId::TargetMappedTopics,
        CheckState::Ready,
        Gating::Blocking,
    )
    .with_times(t("2026-09-15T00:00:00Z"), t("2026-09-15T00:05:00Z"));
    let skipped = outcome(
        CheckId::ArchiveSegments,
        CheckState::Skipped,
        Gating::Blocking,
    )
    .with_times(t("2026-09-15T00:00:00Z"), t("2026-09-15T00:00:30Z"));
    assert_eq!(
        aggregate_expires_at(&[a, b, skipped]),
        Some(t("2026-09-15T00:05:00Z"))
    );
    assert_eq!(aggregate_expires_at(&[]), None);
}

/// Messages, remedies and facts go through the redactor at CONSTRUCTION, so a
/// caller cannot forget.
#[test]
fn an_outcome_redacts_its_own_strings() {
    let c = outcome(
        CheckId::DestinationArchiveListable,
        CheckState::NotReady,
        Gating::Blocking,
    )
    .with_message("denied for AKIAIOSFODNN7EXAMPLE")
    .with_remedy("set password=hunter2 on the secret")
    .with_fact("endpoint", "https://alice:hunter2@minio.storage.svc:9000");
    assert!(!c.message.contains("AKIAIOSFODNN7EXAMPLE"));
    assert!(!c.remedy.contains("hunter2"));
    assert!(!c.facts["endpoint"].contains("hunter2"));
    assert!(c.facts["endpoint"].contains("minio.storage.svc"));
}

// ---------------------------------------------------------------- redaction

/// Each rule, with the mutants that deleting it plants. The harness is the
/// point: `apply_rules` with the rule REMOVED must leak, and with every rule
/// present must not. A test that only asserted the second half is satisfied by
/// a redactor that replaces everything with a constant, and a test that only
/// asserted the first is satisfied by a redactor that does nothing.
///
/// Several fixtures per rule, so a rule that still fires on ONE shape but has
/// lost another is caught too — which is how F1 (the JSON form) and F5 (the
/// second `<Error>` block) got past the first version of this harness.
#[test]
fn every_redaction_rule_has_a_mutant_that_leaks() {
    // (rule, [(input, the witness the rule removes)])
    let cases: [(&str, &[(&str, &str)]); 6] = [
        (
            "pem",
            &[(
                "key: -----BEGIN RSA PRIVATE KEY-----\nMIIBOgIBAAJBAK\n-----END RSA PRIVATE KEY-----\n done",
                "BEGIN RSA PRIVATE KEY",
            )],
        ),
        (
            "s3-xml",
            &[
                (
                    "<Error><Code>AccessDenied</Code><Message>arn:aws:iam::12345:user/backup is not authorized</Message><RequestId>17C3</RequestId></Error>",
                    "not authorized",
                ),
                // F5: the SECOND block. A rule that handles only the first
                // still passes the row above.
                (
                    "<Error><Code>AccessDenied</Code></Error><Error><Code>NoSuchKey</Code><Message>and the second body survives</Message></Error>",
                    "the second body survives",
                ),
            ],
        ),
        (
            "url-userinfo",
            &[(
                "connect to https://alice:hunter2@minio.storage.svc:9000/bucket failed",
                "hunter2",
            )],
        ),
        (
            "secret-key-value",
            &[
                ("sasl.password=sw0rdf1sh and the broker refused", "sw0rdf1sh"),
                // F1: the JSON form, with a value short enough that the
                // long-run rule cannot stand in for this one.
                (r#"admission webhook denied: {"password":"sw0rdf1sh"}"#, "sw0rdf1sh"),
                (r#"{"aws_secret_access_key": "sw0rdf1sh"}"#, "sw0rdf1sh"),
            ],
        ),
        (
            "aws-access-key-id",
            &[("principal AKIAIOSFODNN7EXAMPLE is denied", "AKIAIOSFODNN7EXAMPLE")],
        ),
        (
            "long-base64-or-hex-run",
            &[(
                // EXACTLY 40 characters, so the threshold is pinned at its
                // boundary rather than comfortably inside it.
                "signature over wJalrXUtnFEMIK7MDENGbPxRfiCYEXAMPLEKEY01 rejected",
                "wJalrXUtnFEMIK7MDENGbPxRfiCYEXAMPLEKEY01",
            )],
        ),
    ];
    let all = redaction_rules();
    assert_eq!(
        all.len(),
        cases.len(),
        "a rule was added without a mutant case"
    );

    for (name, fixtures) in cases {
        let without: Vec<_> = all.iter().filter(|r| r.name != name).copied().collect();
        assert_eq!(without.len(), all.len() - 1);
        for (input, witness) in fixtures {
            assert!(
                input.contains(witness),
                "the fixture must carry `{witness}`"
            );

            // The whole redactor removes it.
            let whole = redact(input);
            assert!(
                !whole.contains(witness),
                "rule `{name}`: `{witness}` survived the full redactor: {whole}"
            );

            // THE MUTANT: the same input through every rule but this one.
            let leaked = apply_rules(input, &without);
            assert!(
                leaked.contains(witness),
                "deleting rule `{name}` must leak `{witness}`, but the output was already \
                 clean: {leaked}. Either the rule is redundant (say so) or the fixture no \
                 longer exercises it."
            );
        }
    }
}

/// The redactor keeps what a remedy needs: the host, the Secret and key names,
/// the S3 error CODE. A redactor that removed those would be deleted by the
/// next person who had to debug with it.
#[test]
fn redaction_keeps_what_a_remedy_needs() {
    let out = redact(
        "get s3://kafka-backups/team-a/manifest.json through \
         https://alice:hunter2@minio.storage.svc:9000 failed: \
         <Error><Code>AccessDenied</Code><Message>secret</Message></Error> \
         (secret `logweir-s3` key `access-key-id`)",
    );
    assert!(out.contains("minio.storage.svc:9000"), "{out}");
    assert!(out.contains("AccessDenied"), "{out}");
    assert!(out.contains("logweir-s3"), "{out}");
    assert!(out.contains("access-key-id"), "{out}");
    assert!(!out.contains("hunter2"), "{out}");
    assert!(!out.contains("<Message>"), "{out}");
}

#[test]
fn redaction_caps_at_512_characters_without_splitting_a_character() {
    let long = "é".repeat(4000);
    let out = redact(&long);
    assert_eq!(out.chars().count(), MESSAGE_MAX_CHARS);
    assert!(out.ends_with('…'));
    let short = "hello";
    assert_eq!(redact(short), short);
}

/// **F1.** Every value form of D2 §4.1, in every quoting and spacing shape a
/// real error uses. The JSON and quoted-key rows are the finding: the first
/// version of the scanner skipped only spaces and tabs after the keyword and
/// then required a separator, so the `"` between the two abandoned the match
/// and the value was copied verbatim. W5 relays an admission-webhook or
/// API-server body — JSON — through `redact` into a status message.
#[test]
fn the_secret_keyword_forms_are_all_caught() {
    let cases = [
        // separator forms
        "password=hunter2",
        "password: hunter2",
        "password : hunter2",
        "password => hunter2",
        "sasl.password=\"hunter2\"",
        "SASL.PASSWORD=hunter2",
        "aws_secret_access_key=hunter2",
        "secret_access_key: hunter2",
        "secret-access-key=hunter2",
        "secretaccesskey=hunter2",
        // whitespace-only separator (argv)
        "--token hunter2",
        "token=hunter2,region=us",
        // JSON and quoted-key YAML — the F1 rows
        r#"broker config rejected: {"password":"hunter2","user":"alice"}"#,
        r#"{"aws_secret_access_key": "hunter2"}"#,
        r#"{"sasl.password" : "hunter2"}"#,
        r#"{'password':'hunter2'}"#,
        "password: \"hunter2\"",
        r#"{"secret":"hunter2"}"#,
        r#"{"token":"hunter2"}"#,
        r#"{"password":hunter2}"#,
        // the keyword as a SUFFIX of a longer identifier
        "sessionToken=hunter2",
        "mypassword=hunter2",
        r#"{"clientSecret":"hunter2"}"#,
        // a value that would otherwise slip under the 40-character long-run
        // threshold — which is the whole reason this rule exists
        r#"{"password":"short"}"#,
    ];
    for case in cases {
        let out = redact(case);
        if case.contains("short") {
            assert!(!out.contains("short"), "`{case}` leaked: {out}");
        } else {
            assert!(!out.contains("hunter2"), "`{case}` leaked: {out}");
        }
        assert!(out.contains(REDACTED), "`{case}` produced {out}");
    }

    // An ordinary word that merely CONTAINS a keyword is untouched: a keyword
    // that is the PREFIX of a longer identifier is a word, not a key.
    assert_eq!(redact("the tokenizer failed"), "the tokenizer failed");
    assert_eq!(redact("password_file=/etc/x"), "password_file=/etc/x");

    // D2 §6.5: a Secret NAME and a key name are public references and must
    // survive. `secret` is the bare keyword, so this is the case that decides
    // its form must be quoted-key only.
    let remedy = "secret `logweir-s3` key `access-key-id` not found";
    assert_eq!(redact(remedy), remedy);
}

/// **F5.** S3 and MinIO return several `<Error>` elements in one body for
/// `DeleteObjects` and multipart completion, and the first version replaced
/// only the first and re-emitted the rest untouched.
#[test]
fn every_s3_error_block_is_reduced_to_its_code() {
    let body = "<?xml version=\"1.0\"?><Error><Code>AccessDenied</Code>\
                <Message>first message names a key</Message></Error>\
                <Error><Code>NoSuchKey</Code>\
                <Message>second message names another</Message>\
                <RequestId>17C3E1</RequestId></Error> trailing text";
    let out = redact(body);
    assert!(!out.contains("<Message>"), "{out}");
    assert!(!out.contains("first message"), "{out}");
    assert!(!out.contains("second message"), "{out}");
    assert!(!out.contains("17C3E1"), "{out}");
    assert!(out.contains("AccessDenied"), "{out}");
    assert!(out.contains("NoSuchKey"), "{out}");
    assert!(out.contains("trailing text"), "{out}");

    // A block with no usable `<Code>` still loses its body.
    let out = redact("<Error><Message>nothing to see</Message></Error>");
    assert_eq!(out, "<Error><Code>Unknown</Code></Error>");
    // A code is not a smuggling channel.
    let out = redact("<Error><Code>Access Denied for arn:aws:iam::1:user/x</Code></Error>");
    assert_eq!(out, "<Error><Code>Unknown</Code></Error>");
}

#[test]
fn redaction_is_idempotent_and_total() {
    let inputs = [
        "",
        "-----BEGIN PRIVATE KEY-----",
        "<Error><Code></Code></Error>",
        "https://@/",
        "://",
        "password=",
        "AKIA",
        "AKIAIOSFODNN7EXAMPL",
        // D2-REDACT-OVERBROAD, the bracket half. Every published row is
        // redacted at least twice — `with_message` and again when the
        // controller renders the entry — and the value scanner used to stop at
        // the marker's own `]`, rewrite `[redacted` and leave the bracket
        // behind. Live statuses read `key password [redacted]]] Secret`.
        "password=hunter2",
        "sasl.password: hunter2",
        r#"{"password":"hunter2"}"#,
        "--token hunter2",
        "password: [redacted]",
    ];
    for i in inputs {
        let once = redact(i);
        assert_eq!(redact(&once), once, "not idempotent for `{i}`");
        assert_eq!(redact(&redact(&once)), once, "not idempotent for `{i}`");
    }
}

/// **D2-SIGNERID-REDACTED and D2-REDACT-OVERBROAD, the keeping half.**
///
/// Each row is a sentence the live D2 run published with the acting part blank
/// (`d2w14.result.md` §5.3, §5.4; `objects/s14/notready-rows.json`,
/// `objects/s16/details.jsonl`). The witness is what an operator has to read in
/// order to do the thing the remedy asks: roster THIS key, recover THIS object,
/// add THIS key to THAT Secret.
#[test]
fn a_public_identifier_survives_redaction() {
    // A SHA-256 of a SubjectPublicKeyInfo DER — a `signerKeyId`, and what the
    // TrustRoster is written in.
    const KEY_ID: &str = "6feecc8c16c5551d9feb3eb5f77e2da773bf68bd9ef9c52927ceb2c86e56892b";
    const SET: &str = "3f0ada8f-1a2b-4c3d-9e8f-0123456789ab";

    let cases: Vec<(String, String)> = vec![
        (
            format!("the runner holds signing key `{KEY_ID}`, which the TrustRoster does not list"),
            KEY_ID.to_string(),
        ),
        (
            format!("signing key `{KEY_ID}` is on the TrustRoster"),
            KEY_ID.to_string(),
        ),
        (
            format!("the container started [imageID=docker-pullable://weirkeeper/logweir-runner@sha256:{KEY_ID}]"),
            KEY_ID.to_string(),
        ),
        // The manifest key, prefix-joined exactly as the runner reads it.
        (
            format!("the backup manifest `team/prod/{SET}/manifest.json` could not be read: AccessDenied"),
            format!("team/prod/{SET}/manifest.json"),
        ),
        // The segment path, in the `detailsRef` document that exists to carry
        // it. This is the remedy "do not restore until the objects are
        // recovered" becoming actionable.
        (
            format!(r#"{{"check":"archive.segments","missingSegment":"{SET}/topics/payments/partition=2/segment-00000000000000000000.bin.zst"}}"#),
            format!("{SET}/topics/payments/partition=2/segment-00000000000000000000.bin.zst"),
        ),
        // A Kafka topic may carry upper case; the run is anchored by the set
        // id, so the whole key is read as an object key.
        (
            format!(r#"{{"missingSegment":"team/prod/{SET}/topics/payments-EU/partition=2/segment-00000000000000000000.bin.zst"}}"#),
            "payments-EU".to_string(),
        ),
        // S14b: the kubelet named the Secret and the data key, and the product
        // removed both.
        (
            "couldn't find key password in Secret lw-fix-runner-checks-20260918t051200z/kafka-credentials".to_string(),
            "lw-fix-runner-checks-20260918t051200z/kafka-credentials".to_string(),
        ),
        (
            "couldn't find key password in Secret lw-fix-runner-checks-20260918t051200z/kafka-credentials".to_string(),
            "key password in Secret".to_string(),
        ),
        (
            r#"secret "missing-secret" not found"#.to_string(),
            "missing-secret".to_string(),
        ),
        // The shipped CRD carries no `facts` map, so the controller folds them
        // into the message and redacts the whole sentence again. `=` is in the
        // run alphabet, which makes `signerKeyId=<sha256>` ONE 76-character
        // run — the shape the live `[signerKeyId=[redacted]]` came from.
        (
            format!("the projected signing key parsed, signed a probe and verified it [signerKeyId={KEY_ID}]"),
            format!("signerKeyId={KEY_ID}"),
        ),
        (
            format!("the check pod ran [imageID=docker-pullable://weirkeeper/logweir-runner@sha256:{KEY_ID}; podUID={SET}]"),
            format!("podUID={SET}"),
        ),
    ];

    for (input, witness) in cases {
        assert!(
            input.contains(&witness),
            "the fixture must carry `{witness}`"
        );
        let out = redact(&input);
        assert!(
            out.contains(&witness),
            "`{witness}` did not survive the redactor: {out}"
        );
        // And it still survives the second pass the controller makes.
        assert!(redact(&out).contains(&witness), "second pass: {out}");
    }
}

/// **The other direction, on the same rule.** Loosening the long-run rule must
/// not let an unkeyed credential through, so every shape that reaches it
/// without a key name is asserted to die. `every_redaction_rule_has_a_mutant_
/// that_leaks` pins the rule's existence; this pins its reach.
#[test]
fn unkeyed_secret_material_is_still_removed() {
    let cases = [
        // An AWS secret access key: exactly 40 characters, no separator.
        (
            "signature over wJalrXUtnFEMIK7MDENGbPxRfiCYEXAMPLEKEY01 rejected",
            "wJalrXUtnFEMIK7MDENGbPxRfiCYEXAMPLEKEY01",
        ),
        // The same, in the `/`-bearing form ~72% of real keys take. Splitting
        // a run on `/` and passing short components is exactly the mistake
        // this fixture exists to catch.
        (
            "creds wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY refused",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        ),
        // base64 with padding and `+`.
        (
            "body n4bQgYhMfWWaL+qgxVrQFaO/TxsrC4Is0V1sFbDwCgg= refused",
            "n4bQgYhMfWWaL+qgxVrQFaO",
        ),
        // All lower case, all letters, forty characters: a name-shaped run is
        // public only BELOW the threshold.
        (
            "blob qwertyuiopasdfghjklzxcvbnmqwertyuiopasdfg refused",
            "qwertyuiopasdfghjklzxcvbnmqwertyuiopasdfg",
        ),
        // A SHA-1-width hex run is NOT a digest this product prints, and it is
        // the width several hosted services use for bearer tokens.
        (
            "bearer 0f1e2d3c4b5a69788796a5b4c3d2e1f00f1e2d3c refused",
            "0f1e2d3c4b5a69788796a5b4c3d2e1f00f1e2d3c",
        ),
        // A UUID-anchored run may not carry a forty-character component.
        (
            "3f0ada8f-1a2b-4c3d-9e8f-0123456789ab/wJalrXUtnFEMIK7MDENGbPxRfiCYEXAMPLEKEY01",
            "wJalrXUtnFEMIK7MDENGbPxRfiCYEXAMPLEKEY01",
        ),
        // The `<factName>=<identifier>` clause reaches neither a long key half
        // — a padded base64 blob leaves the whole blob on the key side, where
        // the 24-character cap refuses it …
        (
            "relayed dGhpc2lzYXNlY3JldHZhbHVlZm9ydGVzdGluZ29ubHk= refused",
            "dGhpc2lzYXNlY3JldHZhbHVlZm9ydGVzdGluZ29ubHk",
        ),
        // … nor a value half that is not a public form in its own right …
        (
            "relayed sessionkey=wJalrXUtnFEMIK7MDENGbPxRfiCYEXAMPLEKEY01 refused",
            "wJalrXUtnFEMIK7MDENGbPxRfiCYEXAMPLEKEY01",
        ),
        // … nor a key half longer than a fact name. This is the residual shape
        // the 24-character cap is FOR: a blob with a `=` part-way through and a
        // short lower-case tail after it would otherwise read as
        // `<factName>=<name>`.
        (
            "relayed dGhpc2lzYXNlY3JldHZhbHVlZm9ydGVzdA=abcdef refused",
            "dGhpc2lzYXNlY3JldHZhbHVlZm9ydGVzdA",
        ),
    ];
    for (input, witness) in cases {
        assert!(
            input.contains(witness),
            "the fixture must carry `{witness}`"
        );
        let out = redact(input);
        assert!(!out.contains(witness), "`{witness}` leaked: {out}");
        assert!(out.contains(REDACTED), "`{input}` produced {out}");
    }
}

// --------------------------------------------------------------- visibility

fn signals() -> VisibilitySignals {
    VisibilitySignals {
        namespace: "team-a".into(),
        kafka_cluster: "source".into(),
        cluster_id: "M29I2S7FQPyHBEX12Vx7XA".into(),
        principal: "User:backup".into(),
        topic_authorization_error_in_listing: false,
        expected: ExpectedSummary::default(),
        truncated: false,
    }
}

fn attestation() -> Attestation {
    Attestation {
        id: "att-orders-prod".into(),
        namespace: "team-a".into(),
        kafka_cluster: "source".into(),
        cluster_id: "M29I2S7FQPyHBEX12Vx7XA".into(),
        principal: "User:backup".into(),
        attested_by: "platform-admin@example.invalid".into(),
        attested_at: t("2026-09-15T00:00:00Z"),
        expires_at: t("2026-12-15T00:00:00Z"),
        statement: "User:backup has DESCRIBE on literal Topic:* with no DENY".into(),
    }
}

/// D2 §5.4 and PLAT-09.1's acceptance: a SUCCESSFUL listing alone is
/// `unknown`, including for an empty cluster. This is the whole point of the
/// vocabulary — Kafka omits topics the principal cannot describe, so "I saw
/// none" is not "there are none".
#[test]
fn empty_listing_is_unknown() {
    let v = visibility(&signals(), None, t("2026-09-16T00:00:00Z"));
    assert_eq!(v.state, VisibilityState::Unknown);
    assert_eq!(v.basis, vec![VisibilityBasis::ListingOnly]);
    assert!(v.attestation.is_none());
}

/// Both detection paths of D2 §5.4.
#[test]
fn expected_not_authorized_is_limited() {
    let mut s = signals();
    s.expected = ExpectedSummary {
        requested: 2,
        visible: 1,
        not_authorized: 1,
        ..ExpectedSummary::default()
    };
    let v = visibility(&s, None, t("2026-09-16T00:00:00Z"));
    assert_eq!(v.state, VisibilityState::Limited);
    assert!(v
        .basis
        .contains(&VisibilityBasis::ExpectedTopicNotAuthorized));

    let mut s = signals();
    s.topic_authorization_error_in_listing = true;
    let v = visibility(&s, None, t("2026-09-16T00:00:00Z"));
    assert_eq!(v.state, VisibilityState::Limited);
    assert!(v
        .basis
        .contains(&VisibilityBasis::TopicAuthorizationErrorInListing));

    // An attestation NEVER overrides an observed authorization failure: what
    // was measured beats what was asserted.
    let v = visibility(&s, Some(&attestation()), t("2026-09-16T00:00:00Z"));
    assert_eq!(v.state, VisibilityState::Limited);
    assert!(v.attestation.is_none());
}

/// The four ways an attestation fails to apply, each with its own basis entry
/// so an operator can see why their attestation did nothing.
#[test]
fn attestation_match_expiry_principal_cluster() {
    let now = t("2026-09-16T00:00:00Z");
    let s = signals();

    // The match.
    let v = visibility(&s, Some(&attestation()), now);
    assert_eq!(v.state, VisibilityState::AttestedComplete);
    assert!(v.basis.contains(&VisibilityBasis::AdministratorAttestation));
    let a = v.attestation.unwrap();
    assert_eq!(a.id, "att-orders-prod");
    assert_eq!(a.attested_by, "platform-admin@example.invalid");

    // Expired.
    let v = visibility(&s, Some(&attestation()), t("2026-12-16T00:00:00Z"));
    assert_eq!(v.state, VisibilityState::Unknown);
    assert!(v.basis.contains(&VisibilityBasis::AttestationExpired));

    // Another principal: the credential rotated to one nobody attested for.
    let mut other = attestation();
    other.principal = "User:someone-else".into();
    let v = visibility(&s, Some(&other), now);
    assert_eq!(v.state, VisibilityState::Unknown);
    assert!(v
        .basis
        .contains(&VisibilityBasis::AttestationPrincipalMismatch));

    // Another cluster id: the KafkaCluster now points somewhere else.
    let mut other = attestation();
    other.cluster_id = "OTHERCLUSTERID00000000".into();
    let v = visibility(&s, Some(&other), now);
    assert_eq!(v.state, VisibilityState::Unknown);
    assert!(v
        .basis
        .contains(&VisibilityBasis::AttestationClusterIdMismatch));

    // Another namespace or another KafkaCluster name: not a candidate at all,
    // so it leaves no trace. An attestation for team-b must not even be
    // considered for team-a.
    for mutate in [
        (|a: &mut Attestation| a.namespace = "team-b".into()) as fn(&mut Attestation),
        |a: &mut Attestation| a.kafka_cluster = "other".into(),
    ] {
        let mut m = attestation();
        mutate(&mut m);
        let v = visibility(&s, Some(&m), now);
        assert_eq!(v.state, VisibilityState::Unknown);
        assert_eq!(v.basis, vec![VisibilityBasis::ListingOnly]);
    }

    // Truncated: a partial listing can never be complete, attested or not.
    let mut truncated = signals();
    truncated.truncated = true;
    let v = visibility(&truncated, Some(&attestation()), now);
    assert_eq!(v.state, VisibilityState::Unknown);
    assert!(v.basis.contains(&VisibilityBasis::Truncated));
    assert!(v.attestation.is_none());
}

#[test]
fn the_completeness_vocabulary_has_exactly_three_states() {
    assert_eq!(VisibilityState::Unknown.as_str(), "unknown");
    assert_eq!(VisibilityState::Limited.as_str(), "limited");
    assert_eq!(
        VisibilityState::AttestedComplete.as_str(),
        "attestedComplete"
    );
    assert_eq!(
        serde_json::to_string(&VisibilityState::AttestedComplete).unwrap(),
        "\"attestedComplete\""
    );
    assert!(serde_json::from_str::<VisibilityState>("\"complete\"").is_err());
}

#[test]
fn expected_topics_all_visible_is_recorded_but_is_not_completeness() {
    let mut s = signals();
    s.expected = ExpectedSummary {
        requested: 2,
        visible: 2,
        ..ExpectedSummary::default()
    };
    let v = visibility(&s, None, t("2026-09-16T00:00:00Z"));
    assert_eq!(
        v.state,
        VisibilityState::Unknown,
        "seeing every topic somebody NAMED says nothing about topics nobody named"
    );
    assert!(v.basis.contains(&VisibilityBasis::ExpectedTopicsAllVisible));
}

// ----------------------------------------------------------- binding digest

fn referent(kind: &str, name: &str, uid: &str, generation: i64) -> Referent {
    Referent {
        kind: kind.into(),
        namespace: "team-a".into(),
        name: name.into(),
        uid: uid.into(),
        generation: Some(generation),
    }
}

fn binding() -> BindingInputs {
    BindingInputs {
        operation: CheckOperation::Restore,
        plan_hash: Some(sha256_prefixed(b"restore.yaml")),
        topics: None,
        referents: vec![
            referent("KafkaCluster", "target", "uid-target", 1),
            referent("BackupDestination", "primary", "uid-primary", 3),
        ],
        ca_bundles: vec![CaBundleRef {
            destination_uid: "uid-primary".into(),
            sha256: sha256_prefixed(b"ca"),
        }],
        roster: RosterRef {
            uid: "uid-roster".into(),
            generation: 2,
        },
        approval: Some(ApprovalRef {
            uid: "uid-approval".into(),
            resource_version: "12345".into(),
        }),
        policy_digest: sha256_prefixed(b"policy.json"),
    }
}

/// The digest is what makes a stored result go stale, so it must be stable
/// under presentation and sensitive to content.
#[test]
fn the_binding_digest_is_order_independent() {
    let a = binding();
    let mut reordered = binding();
    reordered.referents.reverse();
    assert_eq!(inputs_digest(&a), inputs_digest(&reordered));

    let mut topics_a = binding();
    topics_a.topics = Some(vec!["payments".into(), "orders".into()]);
    let mut topics_b = binding();
    topics_b.topics = Some(vec!["orders".into(), "payments".into()]);
    assert_eq!(inputs_digest(&topics_a), inputs_digest(&topics_b));

    // `None` topics and an EMPTY topic list are different facts.
    let mut empty = binding();
    empty.topics = Some(Vec::new());
    assert_ne!(inputs_digest(&binding()), inputs_digest(&empty));

    assert!(inputs_digest(&a).starts_with("sha256:"));
}

/// **F4.** `inputs_digest` sorts `ca_bundles` and documents that callers must
/// not pre-sort; `stale_reasons` compared them POSITIONALLY, so two bindings
/// with an identical digest reported `caBundleChanged`.
///
/// The case is a two-destination restore preflight: the controller records the
/// CA bundles in the order it resolved source and evidence, the API recomputes
/// them from a map and gets the other order, and every GET of that `Preflight`
/// answers `stale` — so the restore can never be approved from the UI. It
/// fails safe and is undiagnosable, which is the worst combination.
///
/// MUTANT: dropping either `sorted(...)` call in `stale_reasons` turns the
/// second assertion red.
#[test]
fn the_two_ca_bundle_orderings_agree() {
    let mut recorded = binding();
    recorded.ca_bundles = vec![
        CaBundleRef {
            destination_uid: "uid-source".into(),
            sha256: sha256_prefixed(b"ca-source"),
        },
        CaBundleRef {
            destination_uid: "uid-evidence".into(),
            sha256: sha256_prefixed(b"ca-evidence"),
        },
    ];
    let mut recomputed = recorded.clone();
    recomputed.ca_bundles.reverse();

    assert_eq!(
        inputs_digest(&recorded),
        inputs_digest(&recomputed),
        "the digest is already order-independent"
    );
    assert!(
        stale_reasons(
            &recorded,
            Some(t("2026-09-16T01:00:00Z")),
            &recomputed,
            t("2026-09-16T00:00:00Z")
        )
        .is_empty(),
        "a reordered CA-bundle list is the same list"
    );

    // And a genuine rotation is still caught, so the fix is not a rule that
    // never fires.
    let mut rotated = recomputed.clone();
    rotated.ca_bundles[0].sha256 = sha256_prefixed(b"ca-rotated");
    assert_eq!(
        stale_reasons(
            &recorded,
            Some(t("2026-09-16T01:00:00Z")),
            &rotated,
            t("2026-09-16T00:00:00Z")
        ),
        vec![StaleReason::CaBundleChanged]
    );
}

/// Every consequence D2 §6.6 lists, one assertion each.
#[test]
fn every_binding_input_changes_the_digest() {
    let base = inputs_digest(&binding());
    type Mutation = (&'static str, fn(&mut BindingInputs));
    let mutations: Vec<Mutation> = vec![
        ("plan hash", |b| {
            b.plan_hash = Some(sha256_prefixed(b"other"))
        }),
        ("operation", |b| b.operation = CheckOperation::Backup),
        ("referent uid", |b| b.referents[0].uid = "recreated".into()),
        ("referent generation", |b| {
            b.referents[1].generation = Some(4)
        }),
        ("referent added", |b| {
            b.referents
                .push(referent("Backup", "nightly-1", "uid-backup", 1))
        }),
        ("ca bundle", |b| {
            b.ca_bundles[0].sha256 = sha256_prefixed(b"rotated")
        }),
        ("roster generation", |b| b.roster.generation = 3),
        ("approval resource version", |b| {
            b.approval.as_mut().unwrap().resource_version = "12346".into()
        }),
        ("policy digest", |b| {
            b.policy_digest = sha256_prefixed(b"policy-2.json")
        }),
    ];
    for (what, mutate) in mutations {
        let mut m = binding();
        mutate(&mut m);
        assert_ne!(inputs_digest(&m), base, "{what} did not change the digest");
    }
}

/// PLAT-08.2's "destination edit during a draft": the plan bytes are
/// unchanged, so the plan hash is unchanged, and the result is stale anyway
/// because the destination's generation moved.
#[test]
fn destination_generation_change_changes_digest_but_not_plan_hash() {
    let before = binding();
    let mut after = binding();
    after.referents[1].generation = Some(4);
    assert_eq!(before.plan_hash, after.plan_hash);
    assert_ne!(inputs_digest(&before), inputs_digest(&after));

    let reasons = stale_reasons(
        &before,
        Some(t("2026-09-16T01:00:00Z")),
        &after,
        t("2026-09-16T00:00:00Z"),
    );
    assert_eq!(
        reasons,
        vec![StaleReason::ReferentChanged(
            "BackupDestination/primary".into()
        )]
    );
    assert_eq!(
        reasons[0].to_string(),
        "referentChanged:BackupDestination/primary"
    );
}

/// PLAT-03.2's "plan edits": editing the target, recovery point, point in
/// time, topic subset or mapping prefix changes the plan bytes and therefore
/// the hash, and a green preview cannot survive it.
#[test]
fn plan_hash_change_invalidates() {
    let before = binding();
    let mut after = binding();
    after.plan_hash = Some(sha256_prefixed(b"edited restore.yaml"));
    let reasons = stale_reasons(
        &before,
        Some(t("2026-09-16T01:00:00Z")),
        &after,
        t("2026-09-16T00:00:00Z"),
    );
    assert_eq!(reasons, vec![StaleReason::PlanHashChanged]);
}

/// PLAT-09.1's "credential rotation": the connection's generation or the
/// principal moved, so a stored inventory is stale.
#[test]
fn principal_or_generation_change_marks_stale() {
    let before = binding();
    let mut after = binding();
    after.referents[0].generation = Some(2);
    let reasons = stale_reasons(
        &before,
        Some(t("2026-09-16T01:00:00Z")),
        &after,
        t("2026-09-16T00:00:00Z"),
    );
    assert_eq!(
        reasons,
        vec![StaleReason::ReferentChanged("KafkaCluster/target".into())]
    );
}

/// Applicability needs ALL of D2 §6.6's conditions, so expiry alone is enough
/// to make an otherwise identical binding stale, and a matching, unexpired
/// binding is applicable.
#[test]
fn an_unchanged_unexpired_binding_is_applicable() {
    let b = binding();
    assert!(stale_reasons(
        &b,
        Some(t("2026-09-16T01:00:00Z")),
        &b,
        t("2026-09-16T00:00:00Z")
    )
    .is_empty());
    assert_eq!(
        stale_reasons(
            &b,
            Some(t("2026-09-16T00:00:00Z")),
            &b,
            t("2026-09-16T00:00:00Z")
        ),
        vec![StaleReason::Expired],
        "expiry is inclusive: at expiresAt the result is already stale"
    );
    assert_eq!(
        stale_reasons(&b, None, &b, t("2026-09-16T00:00:00Z")),
        vec![StaleReason::Expired],
        "a result with no expiry is never applicable"
    );
}

/// The catch-all exists so a digest difference can never be reported as "not
/// stale". Topics are the field the five named reasons do not cover.
#[test]
fn a_digest_difference_no_named_reason_explains_is_still_reported() {
    let mut before = binding();
    before.topics = Some(vec!["orders".into()]);
    let mut after = before.clone();
    after.topics = Some(vec!["orders".into(), "payments".into()]);
    let reasons = stale_reasons(
        &before,
        Some(t("2026-09-16T01:00:00Z")),
        &after,
        t("2026-09-16T00:00:00Z"),
    );
    assert_eq!(reasons, vec![StaleReason::InputsDigestChanged]);
}

// ---------------------------------------------------------------- documents

/// The `result` stream is canonical JSON, so the digest the end frame declares
/// is reproducible by anyone holding the document.
#[test]
fn a_result_document_round_trips_through_canonical_json() {
    let mut r = CheckResult::new(CheckPlanKind::OperationReadiness);
    r.checks.push(
        outcome(
            CheckId::DestinationArchiveListable,
            CheckState::NotReady,
            Gating::Blocking,
        )
        .with_message("denied")
        .with_remedy("grant s3:ListBucket")
        .with_times(t("2026-09-15T00:00:00Z"), t("2026-09-15T00:15:00Z"))
        .with_fact("bucket", "kafka-backups")
        .with_detail(serde_json::json!({"count": 2})),
    );
    let bytes = r.to_canonical_json().unwrap();
    assert_eq!(bytes, r.to_canonical_json().unwrap(), "not deterministic");
    let back: CheckResult = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(back, r);
    assert_eq!(back.contract, CHECK_RESULT_CONTRACT);
}

#[test]
fn an_inventory_result_carries_signals_and_not_a_verdict() {
    let json = serde_json::json!({
        "contract": CHECK_RESULT_CONTRACT,
        "kind": "topicInventory",
        "inventory": {
            "format": "logweir.dev/topic-inventory/v1",
            "clusterId": "M29I2S7FQPyHBEX12Vx7XA",
            "brokerCount": 1,
            "counts": {"listed": 5004, "returned": 5003, "internalExcluded": 1, "errored": 0},
            "truncated": true,
            "truncationReason": "relayLimit",
            "topicAuthorizationErrorInListing": false,
            "expected": {"requested": 2, "visible": 1, "notAuthorized": 1, "notFound": 0, "unknown": 0},
            "expectedResults": [
                {"name": "orders", "state": "visible"},
                {"name": "payments", "state": "notAuthorized"}
            ],
            "topicsSha256": "sha256:00"
        }
    });
    let r: CheckResult = serde_json::from_value(json).unwrap();
    let inv = r.inventory.unwrap();
    assert_eq!(inv.truncation_reason, Some(TruncationReason::RelayLimit));
    assert_eq!(inv.expected.not_authorized, 1);
    // The result carries NO visibility field: the verdict is the controller's,
    // because only the controller holds the policy attestation.
    assert!(!serde_json::to_string(&inv).unwrap().contains("visibility"));
}

/// **F6.** D2 §6.4's "≤ 64 entries", enforced rather than declared.
/// `MAX_CHECK_ENTRIES` had no reader anywhere in the workspace, so a W4 or W9
/// bug emitting 400 outcomes would have written a half-megabyte status and
/// nothing would have refused it.
#[test]
fn a_result_document_over_the_entry_cap_is_refused() {
    let mut r = CheckResult::new(CheckPlanKind::OperationReadiness);
    for _ in 0..MAX_CHECK_ENTRIES {
        r.checks.push(outcome(
            CheckId::ConnectionResolved,
            CheckState::Ready,
            Gating::Blocking,
        ));
    }
    assert!(r.validate().is_ok(), "exactly the cap is allowed");
    r.checks.push(outcome(
        CheckId::ConnectionResolved,
        CheckState::Ready,
        Gating::Blocking,
    ));
    assert!(matches!(
        r.validate(),
        Err(CheckResultError::TooManyChecks(65))
    ));

    // And it is refused through the relay, as a `ResultUnreadable`, not only
    // by a method nobody calls.
    let bytes = r.to_canonical_json().unwrap();
    let lines = write_all(&[], &[(Stream::Result, bytes)], "sha256:plan", "uid-1");
    let relay = relay_of(&lines, &expectations("sha256:plan", "uid-1")).unwrap();
    let err = relay.result().unwrap().unwrap_err();
    assert_eq!(err.code(), CheckCode::ResultUnreadable);
}

/// **F11.** The decoded document carries whatever the runner wrote:
/// `CheckOutcome`'s fields are public and `Deserialize`d directly, and
/// `redact`/`cap` run only inside the constructors. D2 §4.1 applies redaction
/// to every RELAYED status message too, so `CheckRelay::result` sanitises
/// before it hands the document over — the controller gets one call instead of
/// a hand-rolled walk it could forget.
#[test]
fn a_relayed_result_is_sanitised_before_the_controller_sees_it() {
    // A document built the way a COMPROMISED or buggy runner would: fields
    // assigned directly, bypassing `with_message` and friends.
    let mut r = CheckResult::new(CheckPlanKind::OperationReadiness);
    let mut bad = outcome(
        CheckId::DestinationArchiveListable,
        CheckState::NotReady,
        Gating::Blocking,
    );
    bad.message = r#"denied: {"password":"hunter2"} for AKIAIOSFODNN7EXAMPLE"#.to_string();
    // Long, but broken into short tokens so it is the CAP that shortens it
    // and not the long-run rule.
    bad.remedy = "grant the action and retry. ".repeat(60);
    bad.facts.insert(
        "endpoint".into(),
        "https://alice:hunter2@minio.svc:9000".into(),
    );
    r.checks.push(bad);

    let bytes = r.to_canonical_json().unwrap();
    assert!(
        String::from_utf8_lossy(&bytes).contains("hunter2"),
        "the fixture must really carry the secret before the relay"
    );
    let lines = write_all(&[], &[(Stream::Result, bytes)], "sha256:plan", "uid-1");
    let relay = relay_of(&lines, &expectations("sha256:plan", "uid-1")).unwrap();
    let got = relay.result().unwrap().unwrap();

    assert!(
        !got.checks[0].message.contains("hunter2"),
        "{:?}",
        got.checks[0]
    );
    assert!(!got.checks[0].message.contains("AKIAIOSFODNN7EXAMPLE"));
    assert!(!got.checks[0].facts["endpoint"].contains("hunter2"));
    assert!(got.checks[0].facts["endpoint"].contains("minio.svc"));
    assert_eq!(got.checks[0].remedy.chars().count(), MESSAGE_MAX_CHARS);

    // `sanitise` is idempotent and is also callable on its own, for a
    // controller that assembles a document from several sources.
    let mut again = got.clone();
    again.sanitise();
    assert_eq!(again, got);
}

/// A result document whose contract or JSON is wrong is `ResultUnreadable`
/// too, so the caller has ONE code for the frames and their contents.
#[test]
fn a_bad_result_document_is_result_unreadable() {
    let lines = write_all(
        &[],
        &[(Stream::Result, b"{not json".to_vec())],
        "sha256:plan",
        "uid-1",
    );
    let relay = relay_of(&lines, &expectations("sha256:plan", "uid-1")).unwrap();
    let err = relay.result().unwrap().unwrap_err();
    assert_eq!(err.code(), CheckCode::ResultUnreadable);

    let mut r = CheckResult::new(CheckPlanKind::TopicInventory);
    r.contract = "logweir.dev/check-result/v2".into();
    let lines = write_all(
        &[],
        &[(Stream::Result, r.to_canonical_json().unwrap())],
        "sha256:plan",
        "uid-1",
    );
    let relay = relay_of(&lines, &expectations("sha256:plan", "uid-1")).unwrap();
    assert!(matches!(
        relay.result().unwrap().unwrap_err(),
        logweir_core::check_contract::FrameError::ResultDocument(CheckResultError::Contract(_))
    ));
}

#[test]
fn the_stream_names_are_pinned() {
    assert_eq!(
        Stream::ALL.map(Stream::as_str),
        ["result", "details", "evidence.payload", "evidence.sidecar"]
    );
    for s in Stream::ALL {
        assert_eq!(Stream::parse(s.as_str()), Some(s));
        assert_eq!(
            serde_json::to_string(&s).unwrap(),
            format!("\"{}\"", s.as_str())
        );
    }
    assert_eq!(Stream::parse("evidence"), None);
}

#[test]
fn an_end_frame_is_compact_single_line_json() {
    let end = EndFrame {
        contract: CHECK_RESULT_CONTRACT.into(),
        plan_sha256: sha256_prefixed(b"plan"),
        subject_uid: "uid".into(),
        streams: BTreeMap::new(),
        topic_lines: None,
    };
    let line = frames::write_end(&end).unwrap();
    assert!(!line.contains('\n'));
    assert!(line.starts_with("logweir-check-end={"));
    let decoded: EndFrame =
        serde_json::from_str(line.strip_prefix("logweir-check-end=").unwrap()).unwrap();
    assert_eq!(decoded, end);
}

#[test]
fn a_credential_mode_is_one_of_three() {
    for (m, s) in [
        (CredentialMode::Static, "\"static\""),
        (CredentialMode::WorkloadIdentity, "\"workloadIdentity\""),
        (CredentialMode::Ambient, "\"ambient\""),
    ] {
        assert_eq!(serde_json::to_string(&m).unwrap(), s);
    }
    assert!(serde_json::from_str::<CredentialMode>("\"instanceRole\"").is_err());
}
