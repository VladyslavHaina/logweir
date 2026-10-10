//! **FX-33 — the shared controller reads a backup of many topics, and stays
//! bounded while it does.**
//!
//! A backup's receipt carries blocks for every topic, about 3 KB each. The
//! controller read one under the 1 MiB cap it reads a scorecard under, so a
//! backup of about 300 topics was `NotAttempted` for ever — and it PARSED what
//! it read into a `serde_json::Value`, many times the bytes, in the one
//! process that serves every namespace. Since FX-33:
//!
//! - a backup receipt has its own cap, `caps::CONTROLLER_RECEIPT`, the largest
//!   receipt Logweir writes (`logweir_core::topic_budget::MAX_RECEIPT_BYTES`);
//!   every other document keeps 1 MiB;
//! - **the cap is the consumer's**: decided from the document KIND the
//!   controller is about to hold, and enforced on the bytes where they arrive
//!   — the store read, the relay's frame decoder, the relay reader, the
//!   verifier — never taken from what a plan asked a pod for;
//! - nothing read under the receipt cap is parsed into a tree: its facts are
//!   folded (`logweir_core::receipt_facts`);
//! - every such read, and every evidence RELAY from its pod-log read to its
//!   verdict, reserves its worst case from the controller's one read budget.
//!
//! These rows hold each of those, and measure the memory in child processes
//! (`getrusage`, as `tests/read_caps.rs` does) with a control beside every
//! number, so the meter is shown to see what the bound forbids.
//!
//! No socket: an in-memory or filesystem `Store`, and relay logs framed by
//! `logweir_core::check_contract::frames` exactly as `logweir check run`
//! frames one.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use chrono::{TimeZone as _, Utc};
use logweir_core::check_contract::{
    frames, CheckPlanKind, CheckResult, EvidenceObjectResult, FrameExpectations, Stream,
};
use logweir_core::engine::StorageUrl;
use logweir_core::ids::sha256_prefixed;
use logweir_core::receipt_facts::ReceiptFacts;
use logweir_core::topic_budget::{self, reference_receipt, ReferenceShape, REFERENCE_SET};
use logweir_store::{caps, Store};
use weirkeeper::check::relay::{decode_within, DECODER_BUDGET_BYTES, RELAY_LIMIT_BYTES};
use weirkeeper::controllers::backup::{
    capture_from_receipt, covered_from_receipt, observe_archive, records_from_receipt, EvidenceKeys,
};
use weirkeeper::crds::trust_roster::{KeyEntry, TrustRosterSpec};
use weirkeeper::evidence_fetch::{read_relay, Presence, RelayHold, Relayed, Request};
use weirkeeper::read_budget::{
    self, ReadBudget, CONTROLLER_READ_BUDGET_BYTES, DOCUMENT_READ_COST_BYTES,
    RECEIPT_READ_COST_BYTES, RELAY_READ_COST_BYTES,
};
use weirkeeper::trust::ResolvedTrust;
use weirkeeper::verification::{
    controller_cap_for, not_attempted_class, read_signing_time, verify_evidence,
    verify_evidence_within, verify_fetched, NotAttemptedClass, SigningTime, SigningTimeNeed,
    VerificationVerdict, CONTROLLER_READ_CAP_PHRASE,
};

const RECEIPT: &str = logweir_verify::PAYLOAD_TYPE_BACKUP_RECEIPT;
const SCORECARD: &str = logweir_verify::PAYLOAD_TYPE_SCORECARD;
const PAYLOAD_KEY: &str = "logweir/backups/reference-set/01JABCDEFGHJKMNPQRSTVWXYZ0.receipt.json";
const SIDECAR_KEY: &str = "logweir/backups/reference-set/01JABCDEFGHJKMNPQRSTVWXYZ0.receipt.sig";
/// `VerifyingKey::key_id()` over the fixture's public key.
const FIXTURE_KEY_ID: &str = "917cf9a299872cbf8b2715999ce457464705bb8f48df0a07e9b1e19bb9f383fd";
/// The relay's frame expectations: any plan digest and subject, the same on
/// both sides.
const PLAN: &str = "sha256:7777777777777777777777777777777777777777777777777777777777777777";
const SUBJECT: &str = "3f1c8a5e-0000-4000-8000-0000000000f3";

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crates/weirkeeper sits two levels under the workspace root")
        .to_path_buf()
}

fn fixture(rel: &str) -> Vec<u8> {
    std::fs::read(repo_root().join(rel)).unwrap_or_else(|e| panic!("{rel} is readable: {e}"))
}

/// A roster carrying the fixture's PUBLIC key.
fn trust() -> ResolvedTrust {
    let pem = String::from_utf8(fixture("e2e/fixtures/signed/public.pem")).unwrap();
    weirkeeper::trust::synthesize_legacy(&TrustRosterSpec {
        approver_keys: Vec::new(),
        signing_keys: vec![KeyEntry {
            key_id: FIXTURE_KEY_ID.to_string(),
            spki_pem: pem,
            subject: Some("logweir-runner@example.invalid".to_string()),
            not_after: None,
        }],
        allowed_cluster_ids: Vec::new(),
    })
}

/// One of `topic_budget::REFERENCE_SET`'s receipts — the bytes `backup run`
/// would sign — and the committed sidecar over them
/// (`crates/logweir/tests/topic_budget.rs` holds each sidecar to the signer).
fn reference(label: &str) -> (usize, Vec<u8>, Vec<u8>) {
    let (_, topics, shape) = REFERENCE_SET
        .iter()
        .find(|(l, ..)| *l == label)
        .unwrap_or_else(|| panic!("no reference receipt labelled {label}"));
    let receipt = logweir_core::det_json::to_deterministic_json(&reference_receipt(*topics, shape))
        .expect("the reference serialises");
    let sidecar = fixture(&format!(
        "crates/weirkeeper/tests/fixtures/topic-budget/reference-{label}.sig"
    ));
    (*topics, receipt, sidecar)
}

fn put(s: &Store, key: &str, bytes: &[u8]) {
    s.put_create_only(key, bytes)
        .expect("the fixture object is written");
}

fn stored(receipt: &[u8], sidecar: &[u8]) -> Store {
    let store = Store::in_memory("logweir/");
    put(&store, PAYLOAD_KEY, receipt);
    put(&store, SIDECAR_KEY, sidecar);
    store
}

fn keys() -> EvidenceKeys {
    EvidenceKeys {
        receipt: Some(PAYLOAD_KEY.to_string()),
        sidecar: Some(SIDECAR_KEY.to_string()),
        receipt_sha256: None,
    }
}

/// What every controller refusal says after the cap's byte count, for a cap.
fn names(detail: &str, cap: u64) -> bool {
    detail.contains(&format!("{cap}{CONTROLLER_READ_CAP_PHRASE}"))
}

fn request(payload_type: &'static str) -> Request {
    Request {
        payload_key: PAYLOAD_KEY.to_string(),
        sidecar_key: SIDECAR_KEY.to_string(),
        payload_type,
    }
}

/// A verified relay of `payload` and `sidecar`, framed exactly as `logweir
/// check run` frames one, with an HONEST result document.
fn relay_log(payload: &[u8], sidecar: &[u8]) -> String {
    let mut result = CheckResult::new(CheckPlanKind::EvidenceFetch);
    for (key, stream, bytes) in [
        (PAYLOAD_KEY, Stream::EvidencePayload, payload),
        (SIDECAR_KEY, Stream::EvidenceSidecar, sidecar),
    ] {
        result.evidence.push(EvidenceObjectResult {
            key: key.to_string(),
            stream,
            present: true,
            sha256: Some(sha256_prefixed(bytes)),
            bytes: Some(bytes.len() as u64),
            code: None,
            truncated: false,
        });
    }
    let result = result.to_canonical_json().expect("canonical");
    let mut streams = BTreeMap::new();
    let mut lines = vec!["{\"level\":\"WARN\",\"message\":\"stderr is not stdout\"}".to_string()];
    for (stream, bytes) in [
        (Stream::Result, result.as_slice()),
        (Stream::EvidencePayload, payload),
        (Stream::EvidenceSidecar, sidecar),
    ] {
        let parts = frames::write_parts(stream, bytes).expect("framable");
        streams.insert(stream, (bytes.to_vec(), parts.len()));
        lines.extend(parts);
    }
    lines.push(frames::write_end(&frames::end_frame(PLAN, SUBJECT, &streams, None)).expect("end"));
    lines.join("\n") + "\n"
}

fn expectations() -> FrameExpectations {
    FrameExpectations {
        plan_sha256: PLAN.to_string(),
        subject_uid: SUBJECT.to_string(),
    }
}

// ===========================================================================
// 1. The cap is one of two rows, chosen by what the document IS
// ===========================================================================

/// **The cap the controller reads a signed document under is one of two rows
/// of the caps table, and which one is decided by the document's kind.** A
/// backup receipt — any version of its media type — is read under
/// `caps::CONTROLLER_RECEIPT`; everything else, including a type that only
/// looks like a receipt's and a type nobody defined, under
/// `caps::CONTROLLER_DOCUMENT`. The store's source-scan guard
/// (`every_production_read_names_a_cap_from_the_table`) admits
/// `controller_cap_for(` as a cap on the strength of this row.
///
/// It also holds the arithmetic the read budget's table states: what each
/// read reserves, and that the budget is a quarter of the chart's controller
/// memory limit.
///
/// KILLS: `controller_cap_for` answering a third number, or the receipt cap
/// for a document that is not a receipt (a scorecard read and parsed at 5 MB).
#[test]
fn the_controllers_document_cap_is_one_of_its_two_rows() {
    for receipt_type in [
        RECEIPT,
        "application/vnd.logweir.backup-receipt+json",
        "application/vnd.logweir.backup-receipt+json;version=1.7.0",
        "application/vnd.logweir.backup-receipt+json;version=9.9.9",
    ] {
        assert_eq!(
            controller_cap_for(receipt_type),
            caps::CONTROLLER_RECEIPT,
            "{receipt_type}"
        );
        assert_eq!(
            read_budget::document_cost_for(receipt_type),
            RECEIPT_READ_COST_BYTES,
            "{receipt_type}"
        );
    }
    for other in [
        SCORECARD,
        logweir_verify::PAYLOAD_TYPE_TEARDOWN,
        logweir_verify::PAYLOAD_TYPE_PUT_RECEIPT,
        logweir_verify::PAYLOAD_TYPE_CATALOG_POINT,
        "application/vnd.logweir.backup-receipt+jsonx",
        "application/vnd.logweir.backup-receipts+json;version=1.0.0",
        "x-application/vnd.logweir.backup-receipt+json",
        "text/plain",
        "",
    ] {
        assert_eq!(
            controller_cap_for(other),
            caps::CONTROLLER_DOCUMENT,
            "{other:?} is not a backup receipt"
        );
        assert_eq!(
            read_budget::document_cost_for(other),
            DOCUMENT_READ_COST_BYTES,
            "{other:?}"
        );
    }
    // The two rows, and where each number comes from.
    assert_eq!(caps::CONTROLLER_RECEIPT, topic_budget::MAX_RECEIPT_BYTES);
    assert_eq!(caps::CONTROLLER_RECEIPT, caps::CATALOG_RECEIPT);
    assert_eq!(caps::CONTROLLER_DOCUMENT, 1 << 20);
    assert!(caps::CONTROLLER_RECEIPT > caps::CONTROLLER_DOCUMENT);
    // What each read reserves is at least what it can hold.
    assert_eq!(
        RECEIPT_READ_COST_BYTES,
        2 * caps::CONTROLLER_RECEIPT + (2 << 20)
    );
    assert!(RELAY_READ_COST_BYTES >= u64::try_from(RELAY_LIMIT_BYTES).unwrap() * 3);
    for cost in [
        DOCUMENT_READ_COST_BYTES,
        RECEIPT_READ_COST_BYTES,
        RELAY_READ_COST_BYTES,
    ] {
        assert!(
            cost <= CONTROLLER_READ_BUDGET_BYTES,
            "one read fits the budget"
        );
    }
    // The budget is a quarter of the chart's controller memory limit.
    let values = String::from_utf8(fixture("charts/logweir/values.yaml")).unwrap();
    let controller = &values[values.find("\ncontroller:").expect("a controller block")..];
    let limit = controller
        .lines()
        .find(|l| l.trim_start().starts_with("limits:"))
        .expect("the controller states a limit");
    assert!(
        limit.contains("memory: 512Mi"),
        "the chart's controller limit moved: {limit}"
    );
    assert_eq!(CONTROLLER_READ_BUDGET_BYTES * 4, 512 << 20);
}

// ===========================================================================
// 2. A backup of many topics is verified
// ===========================================================================

/// **FX-33's acceptance, at the controller: the receipt of a backup of 70,
/// 105, 113, 300, 500 or 1,000 topics with full recorded configuration — and
/// the largest receipt Logweir writes — is VERIFIED `Valid`**, by the same
/// `verify_evidence` every `Backup` goes through, and `observe_archive` and
/// the signing-time re-read take their facts from it.
///
/// Before FX-33 every one of these over 1 MiB (300 topics and up, here) was
/// `NotAttempted` for ever. The row says which are over the old cap so that
/// is visible, and its controls hold the verdict to the bytes: one changed
/// byte is `Invalid`.
///
/// KILLS: the receipt read under the document cap again (`NotAttempted`
/// from 300 topics); the fold disagreeing with the document (a wrong record
/// count or window); a verdict reached without the signature.
#[test]
fn a_backup_of_many_topics_is_verified_by_the_controller() {
    let trust = trust();
    let mut over_the_old_cap = Vec::new();
    for (label, _, shape) in REFERENCE_SET {
        let (topics, receipt, sidecar) = reference(label);
        let len = receipt.len() as u64;
        assert!(len <= caps::CONTROLLER_RECEIPT, "{label}: {len} bytes");
        if len > caps::CONTROLLER_DOCUMENT {
            over_the_old_cap.push(label);
        }
        let store = stored(&receipt, &sidecar);
        let r = verify_evidence(
            Some(&store),
            &trust,
            PAYLOAD_KEY,
            &sha256_prefixed(&receipt),
            SIDECAR_KEY,
            RECEIPT,
        );
        assert_eq!(
            r.result,
            VerificationVerdict::Valid,
            "{label} ({topics} topics, {len} bytes): {:?}",
            r.detail
        );
        assert_eq!(r.matched_key_id.as_deref(), Some(FIXTURE_KEY_ID), "{label}");

        // The facts a `Backup`'s status takes from its verified receipt.
        let document = reference_receipt(topics, &shape);
        let o = observe_archive(&store, &keys()).expect("two keys: an observation");
        assert!(o.presence.payload && o.presence.sidecar, "{label}");
        assert_eq!(o.receipt_sha256, Some(sha256_prefixed(&receipt)), "{label}");
        assert_eq!(
            o.covered,
            Some((document.covered.from_ms, document.covered.to_ms)),
            "{label}"
        );
        assert_eq!(
            o.records,
            Some(i64::try_from(topics).unwrap() * 123_456),
            "{label}: the sum over every topic"
        );
        assert_eq!(
            o.capture,
            Some((document.started_at, document.finished_at)),
            "{label}"
        );
        // The signing time a trust change re-reads.
        let need = SigningTimeNeed {
            payload_key: PAYLOAD_KEY.to_string(),
            payload_sha256: sha256_prefixed(&receipt),
            payload_type: RECEIPT.to_string(),
        };
        match read_signing_time(Some(&store), &need) {
            SigningTime::Recovered(at) => assert_eq!(at, document.finished_at, "{label}"),
            other => panic!("{label}: the receipt's own signing time, got {other:?}"),
        }

        // NEGATIVE CONTROL: one byte of the document changed after signing.
        let mut tampered = receipt.clone();
        let at = tampered
            .windows(8)
            .position(|w| w == b"schedule")
            .expect("the receipt says how it was triggered");
        tampered[at] = b'S';
        let store = stored(&tampered, &sidecar);
        let r = verify_evidence(
            Some(&store),
            &trust,
            PAYLOAD_KEY,
            &sha256_prefixed(&tampered),
            SIDECAR_KEY,
            RECEIPT,
        );
        assert_eq!(r.result, VerificationVerdict::Invalid, "{label}: {r:?}");
    }
    assert_eq!(
        over_the_old_cap,
        vec!["500", "1000", "at-the-bound"],
        "which reference receipts the 1 MiB cap refused before FX-33"
    );
}

/// **A receipt over the bound — one an older runner wrote, 5,000 topics — is
/// `NotAttempted` NAMING THE RECEIPT CAP, final, and is not read.** It is
/// present, and nothing is taken from it.
///
/// KILLS: a cap far too large (15 MB read into the shared controller); the
/// refusal naming the 1 MiB document cap for a receipt.
#[test]
fn a_receipt_over_the_bound_is_not_attempted_naming_the_receipt_cap() {
    let receipt = logweir_core::det_json::to_deterministic_json(&reference_receipt(
        5_000,
        &ReferenceShape::FULL,
    ))
    .expect("serialises");
    assert!(receipt.len() as u64 > caps::CONTROLLER_RECEIPT);
    let store = stored(&receipt, b"{}");
    let r = verify_evidence(
        Some(&store),
        &trust(),
        PAYLOAD_KEY,
        &sha256_prefixed(&receipt),
        SIDECAR_KEY,
        RECEIPT,
    );
    assert_eq!(r.result, VerificationVerdict::NotAttempted, "{r:?}");
    let detail = r.detail.clone().expect("a refusal carries its sentence");
    assert!(
        names(&detail, caps::CONTROLLER_RECEIPT) && detail.contains(PAYLOAD_KEY),
        "the detail names the key and the receipt cap: {detail}"
    );
    assert!(
        detail.contains(&format!("the store reports {} bytes", receipt.len())),
        "and the size: {detail}"
    );
    assert_eq!(not_attempted_class(&detail), NotAttemptedClass::Final);
    let (_, retry) =
        weirkeeper::evidence_fetch::scheduled(r, 1, Utc.timestamp_opt(1_790_000_000, 0).unwrap());
    assert_eq!(retry, None, "the object will not shrink: no retry");
    let o = observe_archive(&store, &keys()).expect("an observation");
    assert!(o.presence.payload, "it is THERE");
    assert_eq!(
        (o.receipt_sha256, o.covered, o.records, o.capture),
        (None, None, None, None),
        "and nothing of it was read"
    );
}

// ===========================================================================
// 3. The cap is the consumer's, never the request's
// ===========================================================================

/// **A document is capped by what it IS, where its bytes are consumed.** The
/// same 1 MiB + 1 bytes are refused as a scorecard and read as a receipt, at
/// the store read and at the verifier that a relay hands bytes to directly;
/// a receipt one byte over its own cap, and a sidecar one byte over its, are
/// refused by the verifier itself, whoever passed them.
///
/// KILLS: the kind-specific cap removed at the controller (one cap for both
/// kinds: a 5 MB scorecard is read and parsed); the verifier relying on its
/// callers' caps.
#[test]
fn a_document_is_capped_by_what_it_is_where_it_is_consumed() {
    let trust = trust();
    let over_a_scorecard = vec![b' '; usize::try_from(caps::CONTROLLER_DOCUMENT).unwrap() + 1];
    let digest = sha256_prefixed(&over_a_scorecard);
    let store = stored(&over_a_scorecard, b"{}");

    // -- the store read ------------------------------------------------------
    let as_scorecard = verify_evidence(
        Some(&store),
        &trust,
        PAYLOAD_KEY,
        &digest,
        SIDECAR_KEY,
        SCORECARD,
    );
    assert_eq!(as_scorecard.result, VerificationVerdict::NotAttempted);
    let detail = as_scorecard.detail.expect("a sentence");
    assert!(names(&detail, caps::CONTROLLER_DOCUMENT), "{detail}");
    assert_eq!(not_attempted_class(&detail), NotAttemptedClass::Final);
    let as_receipt = verify_evidence(
        Some(&store),
        &trust,
        PAYLOAD_KEY,
        &digest,
        SIDECAR_KEY,
        RECEIPT,
    );
    assert_eq!(
        as_receipt.result,
        VerificationVerdict::Invalid,
        "the same bytes are within a receipt's cap, so they are READ and judged: {as_receipt:?}"
    );

    // -- the verifier, handed bytes directly (the relay's path) ----------------
    let as_scorecard = verify_fetched(
        &over_a_scorecard,
        b"{}",
        &trust,
        PAYLOAD_KEY,
        &digest,
        SIDECAR_KEY,
        SCORECARD,
    );
    assert_eq!(as_scorecard.result, VerificationVerdict::NotAttempted);
    let detail = as_scorecard.detail.expect("a sentence");
    assert!(
        names(&detail, caps::CONTROLLER_DOCUMENT) && detail.contains(PAYLOAD_KEY),
        "{detail}"
    );
    assert_eq!(not_attempted_class(&detail), NotAttemptedClass::Final);

    let over_a_receipt = vec![b' '; usize::try_from(caps::CONTROLLER_RECEIPT).unwrap() + 1];
    let r = verify_fetched(
        &over_a_receipt,
        b"{}",
        &trust,
        PAYLOAD_KEY,
        // A digest that does NOT match: over the cap, nothing is hashed, so
        // the verdict is the size and never `Invalid`.
        "sha256:0000000000000000000000000000000000000000000000000000000000000000",
        SIDECAR_KEY,
        RECEIPT,
    );
    assert_eq!(r.result, VerificationVerdict::NotAttempted, "{r:?}");
    assert!(names(
        &r.detail.expect("a sentence"),
        caps::CONTROLLER_RECEIPT
    ));

    let (_, receipt, _) = reference("70");
    let fat_sidecar = vec![b' '; usize::try_from(caps::SIDECAR).unwrap() + 1];
    let r = verify_fetched(
        &receipt,
        &fat_sidecar,
        &trust,
        PAYLOAD_KEY,
        &sha256_prefixed(&receipt),
        SIDECAR_KEY,
        RECEIPT,
    );
    assert_eq!(r.result, VerificationVerdict::NotAttempted, "{r:?}");
    let detail = r.detail.expect("a sentence");
    assert!(
        names(&detail, caps::SIDECAR) && detail.contains(SIDECAR_KEY),
        "{detail}"
    );

    // -- the signing-time re-read --------------------------------------------
    let need = |payload_type: &str| SigningTimeNeed {
        payload_key: PAYLOAD_KEY.to_string(),
        payload_sha256: digest.clone(),
        payload_type: payload_type.to_string(),
    };
    match read_signing_time(Some(&store), &need(SCORECARD)) {
        SigningTime::OverCap(detail) => {
            assert!(names(&detail, caps::CONTROLLER_DOCUMENT), "{detail}")
        }
        other => panic!("a scorecard over its cap yields no signing time, got {other:?}"),
    }
    assert!(
        !matches!(
            read_signing_time(Some(&store), &need(RECEIPT)),
            SigningTime::OverCap(_)
        ),
        "the same bytes are within a receipt's cap"
    );
}

/// **A relay carrying 5 MiB where a scorecard is expected is REFUSED — by the
/// frame decoder at the first part past 1 MiB, and again by the relay's
/// reader — whatever the plan asked the pod for and whatever the pod's own
/// result document says.** The same relay where a receipt is expected is
/// read. The refusal is the stream and the cap, so the caller reports the
/// object as too large (final) and not as a relay that failed (retried).
///
/// KILLS: the controller trusting the requested size (a pod that was asked
/// for 1 MiB and sends 5 is read); the kind-specific cap removed at the
/// decoder or at the reader.
#[test]
fn a_relay_carrying_five_mebibytes_where_a_scorecard_is_expected_is_refused() {
    let scorecard = request(SCORECARD);
    let receipt = request(RECEIPT);
    assert_eq!(scorecard.payload_cap(), caps::CONTROLLER_DOCUMENT);
    assert_eq!(receipt.payload_cap(), caps::CONTROLLER_RECEIPT);

    // 1. The decoder, under the reader's caps for the document it expects.
    //    5 MiB is over a receipt's cap too (4.89 MiB), and each reader's
    //    refusal names ITS cap.
    let five = relay_log(&vec![b'{'; 5 << 20], b"{}");
    for (request, cap) in [
        (&scorecard, caps::CONTROLLER_DOCUMENT),
        (&receipt, caps::CONTROLLER_RECEIPT),
    ] {
        let refused = decode_within(&five, &expectations(), &request.stream_caps())
            .expect_err("5 MiB is over both caps");
        assert_eq!(
            refused.over_cap,
            Some((Stream::EvidencePayload, cap)),
            "{refused}"
        );
    }
    drop(five);
    // A payload as large as a receipt may be: refused where a scorecard is
    // expected, and — the CONTROL — decoded whole where a receipt is.
    let payload_len = usize::try_from(caps::CONTROLLER_RECEIPT).unwrap();
    let log = relay_log(&vec![b'{'; payload_len], b"{}");
    let refused = decode_within(&log, &expectations(), &scorecard.stream_caps())
        .expect_err("4.89 MiB is not a scorecard");
    assert_eq!(
        refused.over_cap,
        Some((Stream::EvidencePayload, caps::CONTROLLER_DOCUMENT)),
        "{refused}"
    );
    let relay = decode_within(&log, &expectations(), &receipt.stream_caps())
        .expect("a payload at a receipt's cap is within it");
    assert_eq!(
        relay.stream(Stream::EvidencePayload).map(<[u8]>::len),
        Some(payload_len)
    );

    // 2. THE READER, whatever decoded the relay. Here it was decoded with NO
    //    stream cap at all — as if the decoder's were removed — and the pod's
    //    result document honestly declares its size, not truncated. The
    //    reader still measures what arrived against the cap of the kind.
    let uncapped = decode_within(&log, &expectations(), &[]).expect("within the relay budget");
    let (presence, relayed) = read_relay(&uncapped, &scorecard);
    assert_eq!(presence, Presence::Unknown);
    match relayed {
        Relayed::Unread { detail } => {
            assert!(
                detail.starts_with(PAYLOAD_KEY)
                    && detail.contains(&format!("{}-byte cap", caps::CONTROLLER_DOCUMENT))
                    && detail.contains(&format!("{payload_len} bytes relayed")),
                "{detail}"
            );
            assert_eq!(not_attempted_class(&detail), NotAttemptedClass::Final);
        }
        Relayed::Both { .. } => panic!("4.89 MiB was handed on as a scorecard"),
    }
    // CONTROL: for a receipt the same relay is both objects.
    assert!(matches!(
        read_relay(&uncapped, &receipt),
        (Presence::Complete, Relayed::Both { .. })
    ));

    // 3. THE PLAN ASKS FOR THE KIND'S CAP, and that is not what is relied on:
    //    the two numbers above are the request's own, read where bytes arrive.
    assert_eq!(
        scorecard.stream_caps()[0],
        (Stream::EvidencePayload, caps::CONTROLLER_DOCUMENT)
    );
    assert_eq!(
        receipt.stream_caps()[1],
        (Stream::EvidenceSidecar, caps::SIDECAR)
    );
}

/// **The largest receipt Logweir writes fits the relay, with room.** A
/// payload of exactly `MAX_RECEIPT_BYTES`, a sidecar at its cap and the
/// result document frame into a log under the controller's pod-log read, and
/// decode under its decoder budget; one byte more of payload is refused by
/// the receipt's own stream cap, not by the relay giving out.
///
/// This is the constraint that binds the budget: a relay goes through ONE
/// pod log, read under `RELAY_LIMIT_BYTES` (8 MiB, below the kubelet's 10 MiB
/// rotation), and base64 in 4 KiB lines costs about 1.36 bytes of log a byte
/// of document.
///
/// KILLS: a receipt cap raised past what a relay carries (the log is cut and
/// the relay never verifies).
#[test]
fn the_largest_receipt_fits_the_relay() {
    let largest = vec![b'r'; usize::try_from(topic_budget::MAX_RECEIPT_BYTES).unwrap()];
    let sidecar = vec![b's'; usize::try_from(caps::SIDECAR).unwrap()];
    let log = relay_log(&largest, &sidecar);
    let read_limit = usize::try_from(RELAY_LIMIT_BYTES).unwrap();
    eprintln!(
        "[fx-33] a relay of a {}-byte receipt and a {}-byte sidecar is {} bytes of pod log \
         ({:.3} log bytes a document byte); the controller reads {read_limit}, so {} bytes are \
         left for stderr and the result document",
        largest.len(),
        sidecar.len(),
        log.len(),
        log.len() as f64 / (largest.len() + sidecar.len()) as f64,
        read_limit.saturating_sub(log.len())
    );
    assert!(
        log.len() + (512 << 10) <= read_limit,
        "the largest relay is {} bytes of log; it must leave half a mebibyte under the {} the \
         controller reads",
        log.len(),
        read_limit
    );
    let request = request(RECEIPT);
    let relay = decode_within(&log, &expectations(), &request.stream_caps())
        .expect("the largest receipt decodes under the decoder budget");
    assert_eq!(
        relay.stream(Stream::EvidencePayload),
        Some(largest.as_slice())
    );
    assert!(matches!(
        read_relay(&relay, &request),
        (Presence::Complete, Relayed::Both { .. })
    ));
    assert!(DECODER_BUDGET_BYTES <= read_limit);

    // One byte more is refused BY THE RECEIPT'S CAP.
    let mut over = largest;
    over.push(b'r');
    let refused = decode_within(
        &relay_log(&over, &sidecar),
        &expectations(),
        &request.stream_caps(),
    )
    .expect_err("one byte over the receipt cap");
    assert_eq!(
        refused.over_cap,
        Some((Stream::EvidencePayload, caps::CONTROLLER_RECEIPT))
    );
}

// ===========================================================================
// 4. The fold answers what the parse answered
// ===========================================================================

/// **The receipt fold answers what the three rule functions answer over a
/// parsed document** — `records_from_receipt`, `covered_from_receipt` and
/// `capture_from_receipt`, which were the controller's readers until FX-33
/// and remain as the statement of the rule. Over every reference receipt, the
/// signed fixture, and bodies that break each rule one way.
///
/// (`logweir-core/tests/receipt_facts.rs` holds the fold to a `Value` walk
/// over 59 bodies; this row holds it to the CONTROLLER's own three functions.)
///
/// KILLS: the fold summing differently, reading half a window, or taking the
/// first of a duplicated key where `serde_json` takes the last.
#[test]
fn the_receipt_fold_answers_what_these_functions_answer() {
    let mut bodies: Vec<(String, Vec<u8>)> = vec![(
        "the signed fixture".to_string(),
        fixture("e2e/fixtures/signed/backup-receipt.json"),
    )];
    for (label, ..) in REFERENCE_SET {
        bodies.push((format!("reference {label}"), reference(label).1));
    }
    for (what, text) in [
        ("no records block", r#"{"covered":{"from_ms":1,"to_ms":2}}"#),
        ("records not an object", r#"{"records":[1,2]}"#),
        ("a negative count", r#"{"records":{"a":-1}}"#),
        ("a fractional count", r#"{"records":{"a":1.5}}"#),
        (
            "a count past i64",
            r#"{"records":{"a":18446744073709551615}}"#,
        ),
        (
            "a sum past i64",
            r#"{"records":{"a":9223372036854775807,"b":1}}"#,
        ),
        ("an empty records block", r#"{"records":{}}"#),
        ("half a window", r#"{"covered":{"from_ms":1}}"#),
        (
            "a window of strings",
            r#"{"covered":{"from_ms":"1","to_ms":"2"}}"#,
        ),
        ("one instant", r#"{"started_at":"2026-09-09T11:02:19Z"}"#),
        (
            "an instant that is not one",
            r#"{"started_at":"x","finished_at":"2026-09-09T11:02:19Z"}"#,
        ),
        (
            "an instant that is a number",
            r#"{"started_at":1,"finished_at":2}"#,
        ),
        (
            "a duplicated key",
            r#"{"records":{"a":1},"records":{"a":7,"b":1}}"#,
        ),
        ("a duplicated topic", r#"{"records":{"a":1,"a":5}}"#),
        (
            "a nested look-alike",
            r#"{"archive":{"records":{"a":9},"covered":{"from_ms":3,"to_ms":4}}}"#,
        ),
        ("not an object", r#"[{"records":{"a":1}}]"#),
        ("a scalar", "7"),
    ] {
        bodies.push((what.to_string(), text.as_bytes().to_vec()));
    }
    for (what, bytes) in &bodies {
        let value: serde_json::Value = serde_json::from_slice(bytes).expect("each body is JSON");
        let facts = ReceiptFacts::fold(bytes).unwrap_or_else(|| panic!("{what}: JSON folds"));
        assert_eq!(
            facts.records,
            records_from_receipt(&value),
            "{what}: records"
        );
        assert_eq!(
            facts.covered,
            covered_from_receipt(&value),
            "{what}: covered"
        );
        assert_eq!(
            facts.capture(),
            capture_from_receipt(&value),
            "{what}: capture"
        );
    }
    // Bytes that are not JSON fold to nothing, as they parse to nothing.
    for junk in [
        &b"{"[..],
        b"",
        b"{\"records\":{\"a\":1}} trailing",
        b"\xff\xfe",
    ] {
        assert!(ReceiptFacts::fold(junk).is_none());
        assert!(serde_json::from_slice::<serde_json::Value>(junk).is_err());
    }
}

/// **No production path of the controller parses bytes into a tree, except
/// the sites listed here, and none of them can hold a receipt.**
///
/// A `serde_json::Value` of a document is many times its bytes (37 times for
/// a document of tiny values), and this process serves every namespace. So
/// every place `crates/weirkeeper/src` builds a `Value` out of bytes or text
/// is on the list below with what the bytes are and what bounds them. A new
/// one fails this row until it is classified — which is the question "can a
/// backup receipt reach this parse?" asked of whoever adds it.
///
/// The scan is of the sources' production code: a file is read up to its
/// first `#[cfg(test)]`, and `testing.rs` (test support) is not read. The
/// tokens are assembled so this file does not match itself.
///
/// KILLS: the receipt fold replaced by a whole parse anywhere in the
/// controller — in `claim_of`, in `observe_archive`, in the relayed-receipt
/// path, or in a new error or logging path.
#[test]
fn the_controller_builds_no_tree_of_a_receipt_on_any_path() {
    let value = "Value";
    let patterns = [
        format!("from_slice::<{value}>"),
        format!("from_slice::<serde_json::{value}>"),
        format!("from_str::<{value}>"),
        format!("from_str::<serde_json::{value}>"),
        format!(": {value} = serde_json::from_slice"),
        format!(": serde_json::{value} = serde_json::from_slice"),
        format!(": {value} = serde_json::from_str"),
        format!(": serde_json::{value} = serde_json::from_str"),
        format!("from_reader::<_, {value}>"),
    ];
    // (file, a fragment of the line, what the bytes are and what bounds them)
    let allowed: [(&str, &str, &str); 7] = [
        (
            "verification.rs",
            "match serde_json::from_slice::<Value>(payload) {",
            "claim_of: a document that is NOT a backup receipt, and only within the 1 MiB \
             document cap (the two lines above it)",
        ),
        (
            "controllers/restore.rs",
            "let doc: Value = serde_json::from_slice(bytes).ok()?;",
            "scorecard_observation: a scorecard, after within_scorecard_cap",
        ),
        (
            "controllers/restore.rs",
            ".and_then(|bytes| serde_json::from_slice::<Value>(bytes).ok())",
            "the relayed scorecard's run id, after within_scorecard_cap",
        ),
        (
            "controllers/restore.rs",
            "serde_json::from_str::<Value>(&approval.spec.approval_bytes)",
            "an Approval's own spec text, bounded by the API server's object size",
        ),
        (
            "controllers/restore.rs",
            "serde_json::from_str::<Value>(&approval.spec.sidecar_bytes)",
            "an Approval's own spec text",
        ),
        (
            "controllers/restore.rs",
            "let doc: Value = serde_json::from_str(raw?).ok()?;",
            "a status annotation of the Restore itself",
        ),
        (
            "backup_execution.rs",
            "let version = serde_json::from_str::<serde_json::Value>(stored)",
            "the frozen inputs of a plan ConfigMap, at most 1 MiB",
        ),
    ];
    let src = repo_root().join("crates/weirkeeper/src");
    let mut files = Vec::new();
    let mut stack = vec![src.clone()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).expect("a source directory") {
            let path = entry.expect("readable").path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|e| e == "rs") {
                files.push(path);
            }
        }
    }
    assert!(files.len() > 40, "the scan reads the controller's sources");
    let mut used = vec![false; allowed.len()];
    let mut scanned = 0usize;
    for path in &files {
        let rel = path
            .strip_prefix(&src)
            .expect("under src")
            .to_string_lossy()
            .replace('\\', "/");
        if rel == "testing.rs" {
            continue;
        }
        let text = std::fs::read_to_string(path).expect("a source file");
        let production = text.split("\n#[cfg(test)]").next().unwrap_or(&text);
        for (number, line) in production.lines().enumerate() {
            let trimmed = line.trim_start();
            if trimmed.starts_with("//") || !patterns.iter().any(|p| line.contains(p.as_str())) {
                continue;
            }
            scanned += 1;
            let known = allowed
                .iter()
                .position(|(file, fragment, _)| rel == *file && line.contains(fragment));
            match known {
                Some(at) => used[at] = true,
                None => panic!(
                    "{rel}:{} builds a serde_json::Value out of bytes or text:\n    {}\nThe \
                     controller builds no tree of a backup receipt (FX-33): fold what you need \
                     (logweir_core::receipt_facts), or, if these bytes can never be a receipt, \
                     add the site to this row's list with what bounds it.",
                    number + 1,
                    line.trim()
                ),
            }
        }
    }
    assert!(scanned >= allowed.len(), "the patterns match the sources");
    for ((file, fragment, why), used) in allowed.iter().zip(&used) {
        assert!(
            used,
            "{file} no longer contains `{fragment}` ({why}); remove it from this row's list so \
             the list stays the whole truth"
        );
    }
    // AND THE THREE SITES A RECEIPT REACHES FOLD IT. Named, so replacing one
    // with a typed or untyped parse is a change to this row.
    let read = |rel: &str| std::fs::read_to_string(src.join(rel)).expect("a source file");
    let fold = "receipt_facts::ReceiptFacts::fold(";
    assert!(
        read("verification.rs").contains(fold),
        "claim_of folds a receipt"
    );
    assert_eq!(
        read("controllers/backup.rs").matches(fold).count()
            + read("controllers/backup.rs")
                .matches("and_then(logweir_core::receipt_facts::ReceiptFacts::fold)")
                .count(),
        2,
        "observe_archive and the relayed-receipt path each fold the receipt once"
    );
}

// ===========================================================================
// 5. A relay waits for the read budget
// ===========================================================================

/// **An evidence relay reserves its worst case from the read budget before
/// its pod log is read, and waits when it does not fit.** With all but one
/// byte of a relay's cost held, `RelayHold::reserve` does not come back;
/// released, it does, holding exactly `RELAY_READ_COST_BYTES`; dropped, the
/// budget is whole again.
///
/// KILLS: `RelayHold::reserve` reserving nothing (the relay path under no
/// budget, as it was before FX-33).
#[tokio::test]
async fn a_relay_waits_for_the_read_budget() {
    static BUDGET: ReadBudget = ReadBudget::new(RELAY_READ_COST_BYTES);
    let held = BUDGET.reserve(1);
    let waiting = tokio::spawn(async {
        let mut hold = RelayHold::on(&BUDGET);
        assert!(!hold.is_held());
        hold.reserve().await;
        hold
    });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert!(
        !waiting.is_finished(),
        "a relay that does not fit the budget waits for it"
    );
    assert_eq!(BUDGET.in_use(), 1);
    drop(held);
    let mut hold = tokio::time::timeout(std::time::Duration::from_secs(10), waiting)
        .await
        .expect("the relay is admitted once the budget is released")
        .expect("the task ends");
    assert!(hold.is_held());
    assert_eq!(BUDGET.in_use(), RELAY_READ_COST_BYTES);
    // Idempotent within a pass: a second reserve takes nothing more.
    hold.reserve().await;
    assert_eq!(BUDGET.in_use(), RELAY_READ_COST_BYTES);
    drop(hold);
    assert_eq!(BUDGET.in_use(), 0, "the reservation is the hold's");
}

// ===========================================================================
// 6. Peak resident memory, measured in child processes
// ===========================================================================

const TEST_NAME: &str = "a_receipt_read_and_a_receipt_relay_stay_inside_what_they_reserve";
const CHILD_ENV: &str = "FX33_MEM_CHILD";
const ROOT_ENV: &str = "FX33_MEM_ROOT";
const PEAK_LINE: &str = "FX33_PEAK_RSS=";

/// How many receipt verifications, and how many relays, start at once.
const CONC_RECEIPTS: usize = 32;
const CONC_RELAYS: usize = 12;

/// This process's own peak resident set, in bytes.
fn self_peak_rss() -> u64 {
    let usage = nix::sys::resource::getrusage(nix::sys::resource::UsageWho::RUSAGE_SELF)
        .expect("getrusage(RUSAGE_SELF)");
    let max = u64::try_from(usage.max_rss()).unwrap_or(0);
    if cfg!(target_os = "macos") {
        max
    } else {
        max * 1024
    }
}

fn handle(root: &Path) -> Store {
    Store::read_only_from_url(&StorageUrl::Filesystem {
        path: root.to_path_buf(),
    })
    .expect("a filesystem handle over an existing directory builds")
}

/// A scratch tree holding one receipt and its sidecar at the two evidence
/// keys, `links` hard links to each for the concurrency rows, and the relay
/// log of the pair.
fn plant(root: &Path, receipt: &[u8], sidecar: &[u8], links: usize) {
    for (key, bytes) in [(PAYLOAD_KEY, receipt), (SIDECAR_KEY, sidecar)] {
        let path = root.join(key);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, bytes).unwrap();
    }
    for i in 0..links {
        for (key, suffix) in [(PAYLOAD_KEY, "json"), (SIDECAR_KEY, "sig")] {
            let at = root.join(format!("logweir/conc/r{i}.{suffix}"));
            std::fs::create_dir_all(at.parent().unwrap()).unwrap();
            std::fs::hard_link(root.join(key), &at).unwrap();
        }
    }
    std::fs::write(root.join("relay.log"), relay_log(receipt, sidecar)).unwrap();
    std::fs::write(root.join("digest"), sha256_prefixed(receipt)).unwrap();
}

/// One relay, as `evidence_fetch` and the `Backup` reconciler hold it: the
/// pod log read into a string, decoded under the reader's caps; the relay
/// read into the two documents; the receipt verified and its facts folded.
/// With `as_value`, the receipt is ALSO parsed into a `serde_json::Value` —
/// what the relay path did before FX-33 — and held beside the rest.
fn one_relay(root: &Path, kind: &'static str, as_value: bool) -> bool {
    let request = request(kind);
    let digest = std::fs::read_to_string(root.join("digest")).unwrap();
    let relay = {
        let log = std::fs::read_to_string(root.join("relay.log")).expect("the pod log");
        match decode_within(&log, &expectations(), &request.stream_caps()) {
            Ok(relay) => relay,
            Err(refused) => {
                assert!(refused.over_cap.is_some(), "{refused}");
                return false;
            }
        }
    };
    let (presence, relayed) = read_relay(&relay, &request);
    assert_eq!(presence, Presence::Complete);
    let Relayed::Both { payload, sidecar } = relayed else {
        panic!("both objects were relayed");
    };
    let tree = as_value.then(|| {
        serde_json::from_slice::<serde_json::Value>(&payload).expect("the receipt is JSON")
    });
    let r = verify_fetched(
        &payload,
        &sidecar,
        &trust(),
        PAYLOAD_KEY,
        &digest,
        SIDECAR_KEY,
        kind,
    );
    assert_eq!(r.result, VerificationVerdict::Valid, "{r:?}");
    let facts = ReceiptFacts::fold(&payload).expect("the receipt folds");
    assert!(facts.records.is_some() && facts.covered.is_some());
    drop(tree);
    drop(relay);
    true
}

fn run_child(mode: &str, root: &Path) {
    let digest = std::fs::read_to_string(root.join("digest")).unwrap_or_default();
    match mode {
        // The controller's three store reads of one receipt.
        "read" | "read-value" => {
            let store = handle(root);
            let r = verify_evidence(
                Some(&store),
                &trust(),
                PAYLOAD_KEY,
                &digest,
                SIDECAR_KEY,
                RECEIPT,
            );
            assert_eq!(r.result, VerificationVerdict::Valid, "{r:?}");
            let o = observe_archive(&store, &keys()).expect("an observation");
            assert!(o.records.is_some() && o.covered.is_some() && o.capture.is_some());
            let need = SigningTimeNeed {
                payload_key: PAYLOAD_KEY.to_string(),
                payload_sha256: digest.clone(),
                payload_type: RECEIPT.to_string(),
            };
            assert!(matches!(
                read_signing_time(Some(&store), &need),
                SigningTime::Recovered(_)
            ));
            if mode == "read-value" {
                // The control: the same bytes as a tree, which is what the
                // controller built of every receipt before FX-33.
                let (bytes, _) = store
                    .get_capped(PAYLOAD_KEY, caps::CONTROLLER_RECEIPT)
                    .expect("within the cap");
                let v: serde_json::Value = serde_json::from_slice(&bytes).expect("valid JSON");
                assert!(v["records"].is_object());
            }
        }
        "relay" => assert!(one_relay(root, RECEIPT, false)),
        "relay-value" => assert!(one_relay(root, RECEIPT, true)),
        // The same log where a scorecard is expected: refused at the decoder.
        "relay-as-scorecard" => assert!(!one_relay(root, SCORECARD, false)),
        "conc-receipts" | "conc-receipts-free" => {
            let free = ReadBudget::new(u64::MAX);
            let budget = if mode.ends_with("-free") {
                &free
            } else {
                ReadBudget::controller()
            };
            let store = handle(root);
            let trust = trust();
            let start = std::sync::Barrier::new(CONC_RECEIPTS);
            std::thread::scope(|scope| {
                for i in 0..CONC_RECEIPTS {
                    let (store, trust, start, digest) = (&store, &trust, &start, &digest);
                    scope.spawn(move || {
                        start.wait();
                        let r = verify_evidence_within(
                            budget,
                            Some(store),
                            trust,
                            &format!("logweir/conc/r{i}.json"),
                            digest,
                            &format!("logweir/conc/r{i}.sig"),
                            RECEIPT,
                        );
                        assert_eq!(r.result, VerificationVerdict::Valid, "{r:?}");
                    });
                }
            });
        }
        "conc-relays" | "conc-relays-free" => {
            let budget: &'static ReadBudget = if mode.ends_with("-free") {
                Box::leak(Box::new(ReadBudget::new(u64::MAX)))
            } else {
                ReadBudget::controller()
            };
            // One reconciler thread, as many blocking threads as there are
            // relays waiting and relays working: the concurrency under test
            // is the blocking pool's, where every archive read runs.
            let runtime = tokio::runtime::Builder::new_current_thread()
                .max_blocking_threads(CONC_RELAYS * 2 + 2)
                .enable_all()
                .build()
                .expect("a runtime");
            runtime.block_on(async {
                let mut tasks = Vec::new();
                for _ in 0..CONC_RELAYS {
                    let root = root.to_path_buf();
                    tasks.push(tokio::spawn(async move {
                        // The product's own hold, taken before the log is
                        // read and kept until the verdict is reached.
                        let mut hold = RelayHold::on(budget);
                        hold.reserve().await;
                        let done =
                            tokio::task::spawn_blocking(move || one_relay(&root, RECEIPT, false))
                                .await
                                .expect("the relay finishes");
                        assert!(done);
                        drop(hold);
                    }));
                }
                for task in tasks {
                    task.await.expect("a relay task");
                }
            });
        }
        other => panic!("unknown {CHILD_ENV} mode {other}"),
    }
}

/// Runs this test again in a child, in `mode`, over `root`, and returns the
/// child's own peak resident set.
fn child_peak(mode: &str, root: &Path) -> u64 {
    let out = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", TEST_NAME, "--nocapture", "--test-threads=1"])
        .env(CHILD_ENV, mode)
        .env(ROOT_ENV, root)
        // The meter measures live memory, not an allocator's cache of freed
        // blocks (`tests/read_caps.rs`, review F2).
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
    stdout
        .lines()
        .find_map(|l| l.find(PEAK_LINE).map(|at| &l[at + PEAK_LINE.len()..]))
        .and_then(|v| {
            v.trim()
                .split(|c: char| !c.is_ascii_digit())
                .next()?
                .parse()
                .ok()
        })
        .unwrap_or_else(|| panic!("the {mode} child printed no {PEAK_LINE} line:\n{stdout}"))
}

fn scratch(what: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "logweir-fx33-{what}-{}-{}",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    ))
}

/// **One receipt read, and one receipt relay, stay inside what they reserve
/// from the read budget — with a receipt at the bound.**
///
/// In child processes, each against a baseline child doing the same work
/// over the 1 KB signed fixture:
///
/// - the controller's three store reads of the receipt at the bound
///   (`verify_evidence`, `observe_archive`, `read_signing_time`) add less
///   than `RECEIPT_READ_COST_BYTES`: the bytes and the signature's
///   pre-authentication copy of them, and nothing per topic;
/// - one RELAY of it — the pod log read into a string, decoded, read,
///   verified, folded — adds less than `RELAY_READ_COST_BYTES`;
/// - the same relay log where a SCORECARD is expected is refused at the
///   decoder and adds little more than the log it was given.
///
/// The controls parse the receipt into a `serde_json::Value` beside the same
/// work — what the controller did before FX-33 — and must add at least twice
/// the document on top, which shows the meter sees a tree.
///
/// KILLS: the fold replaced by a whole parse in `claim_of`, `observe_archive`
/// or the relay path (the read or the relay child holds the control's
/// memory); a relay stream cap removed (the scorecard child holds 5 MB).
#[test]
fn a_receipt_read_and_a_receipt_relay_stay_inside_what_they_reserve() {
    if let Ok(mode) = std::env::var(CHILD_ENV) {
        let root = PathBuf::from(std::env::var(ROOT_ENV).expect(ROOT_ENV));
        run_child(&mode, &root);
        println!("{PEAK_LINE}{}", self_peak_rss());
        return;
    }
    let dir = scratch("rss");
    let (small, big) = (dir.join("small"), dir.join("big"));
    plant(
        &small,
        &fixture("e2e/fixtures/signed/backup-receipt.json"),
        &fixture("e2e/fixtures/signed/backup-receipt.sig"),
        0,
    );
    let (topics, receipt, sidecar) = reference("at-the-bound");
    plant(&big, &receipt, &sidecar, 0);
    let len = receipt.len() as u64;
    let log_len = std::fs::metadata(big.join("relay.log")).unwrap().len();

    let read_base = child_peak("read", &small);
    let read = child_peak("read", &big).saturating_sub(read_base);
    let read_value = child_peak("read-value", &big).saturating_sub(read_base);
    let relay_base = child_peak("relay", &small);
    let relay = child_peak("relay", &big).saturating_sub(relay_base);
    let relay_value = child_peak("relay-value", &big).saturating_sub(relay_base);
    let as_scorecard = child_peak("relay-as-scorecard", &big).saturating_sub(relay_base);
    let _ = std::fs::remove_dir_all(&dir);
    eprintln!(
        "[fx33-mem] a receipt of {topics} topics, {len} B (cap {} B). Three store reads add \
         {read} B (reserves {RECEIPT_READ_COST_BYTES} B); with the receipt also parsed into a \
         Value, {read_value} B. One relay of it, a {log_len} B pod log, adds {relay} B (reserves \
         {RELAY_READ_COST_BYTES} B); with the Value, {relay_value} B. The same log where a \
         scorecard is expected adds {as_scorecard} B.",
        caps::CONTROLLER_RECEIPT
    );
    assert!(
        read < RECEIPT_READ_COST_BYTES,
        "three store reads of a {len}-byte receipt added {read} bytes; a read reserves \
         {RECEIPT_READ_COST_BYTES}"
    );
    assert!(
        relay < RELAY_READ_COST_BYTES,
        "one relay of a {len}-byte receipt added {relay} bytes; a relay reserves \
         {RELAY_READ_COST_BYTES}"
    );
    assert!(
        as_scorecard < log_len + (4 << 20),
        "a relay refused at the decoder added {as_scorecard} bytes; it may hold the {log_len}-byte \
         log it was given, the 1 MiB it accepted, and little else"
    );
    assert!(
        read_value > read + 2 * len,
        "the Value control added {read_value} bytes against {read}; the meter cannot show what \
         the fold saves"
    );
    assert!(
        relay_value > relay + 2 * len,
        "the Value control added {relay_value} bytes against {relay}; the meter cannot show \
         what the fold saves"
    );
}

/// **Simultaneous large receipts stay inside the read budget, by the read
/// path and by the relay path.** In child processes, with a receipt at the
/// bound:
///
/// - [`CONC_RECEIPTS`] verifications start together. Under the controller's
///   budget at most ten hold a receipt at once, and the peak stays within
///   the budget plus a fixed slack;
/// - [`CONC_RELAYS`] relays start together, each behind the product's own
///   `RelayHold`. Under the controller's budget at most three run at once.
///
/// The controls run the same work under a budget that admits everything and
/// must exceed that same bound, which shows the meter sees the concurrency
/// the budget forbids.
///
/// KILLS: no reservation in `verify_evidence` for a receipt, or one far too
/// small; `RelayHold::reserve` reserving nothing.
#[test]
fn simultaneous_large_receipts_stay_inside_the_read_budget() {
    if std::env::var(CHILD_ENV).is_ok() {
        // The child's work is the other row's dispatch (`TEST_NAME`).
        return;
    }
    let dir = scratch("conc");
    let (small, big) = (dir.join("small"), dir.join("big"));
    plant(
        &small,
        &fixture("e2e/fixtures/signed/backup-receipt.json"),
        &fixture("e2e/fixtures/signed/backup-receipt.sig"),
        CONC_RECEIPTS,
    );
    let (_, receipt, sidecar) = reference("at-the-bound");
    plant(&big, &receipt, &sidecar, CONC_RECEIPTS);
    let slack: u64 = 32 << 20;
    let bound = CONTROLLER_READ_BUDGET_BYTES + slack;

    let r_base = child_peak("conc-receipts", &small);
    let r_budget = child_peak("conc-receipts", &big).saturating_sub(r_base);
    let r_free = child_peak("conc-receipts-free", &big).saturating_sub(r_base);
    let l_base = child_peak("conc-relays", &small);
    let l_budget = child_peak("conc-relays", &big).saturating_sub(l_base);
    let l_free = child_peak("conc-relays-free", &big).saturating_sub(l_base);
    let _ = std::fs::remove_dir_all(&dir);
    eprintln!(
        "[fx33-conc] {CONC_RECEIPTS} verifications of a {} B receipt: under the budget add \
         {r_budget} B, with no budget {r_free} B; {CONC_RELAYS} relays of it: under the budget \
         {l_budget} B, with no budget {l_free} B (budget {CONTROLLER_READ_BUDGET_BYTES} B, slack \
         {slack} B)",
        receipt.len()
    );
    assert!(
        r_budget < bound,
        "{CONC_RECEIPTS} concurrent receipt verifications added {r_budget} bytes; the budget \
         holds them to {bound}"
    );
    assert!(
        l_budget < bound,
        "{CONC_RELAYS} concurrent relays added {l_budget} bytes; the budget holds them to {bound}"
    );
    assert!(
        r_free > bound,
        "the control's verifications added only {r_free} bytes; the meter cannot see the \
         concurrency the budget forbids"
    );
    assert!(
        l_free > bound,
        "the control's relays added only {l_free} bytes; the meter cannot see the concurrency \
         the budget forbids"
    );
}
