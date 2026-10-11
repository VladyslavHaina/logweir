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
//! - the cap is chosen by the document KIND the controller expects: asked
//!   for in an evidence fetch's plan, and measured by the store read, the
//!   relay reader and the verifier;
//! - nothing read under the receipt cap is parsed into a tree: its facts are
//!   folded (`logweir_core::receipt_facts`).
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
use weirkeeper::check::relay::{decode, DECODER_BUDGET_BYTES, RELAY_LIMIT_BYTES};
use weirkeeper::controllers::backup::{
    capture_from_receipt, covered_from_receipt, observe_archive, records_from_receipt, EvidenceKeys,
};
use weirkeeper::crds::trust_roster::{KeyEntry, TrustRosterSpec};
use weirkeeper::evidence_fetch::{read_relay, Presence, Relayed, Request};
use weirkeeper::read_budget::DOCUMENT_READ_COST_BYTES;
use weirkeeper::trust::ResolvedTrust;
use weirkeeper::verification::{
    controller_cap_for, not_attempted_class, read_signing_time, verify_evidence, verify_fetched,
    NotAttemptedClass, SigningTime, SigningTimeNeed, VerificationVerdict,
    CONTROLLER_READ_CAP_PHRASE,
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
    }
    // The two rows, and where each number comes from.
    assert_eq!(caps::CONTROLLER_RECEIPT, topic_budget::MAX_RECEIPT_BYTES);
    assert_eq!(caps::CONTROLLER_RECEIPT, caps::CATALOG_RECEIPT);
    assert_eq!(caps::CONTROLLER_DOCUMENT, 1 << 20);
    const _: () = assert!(caps::CONTROLLER_RECEIPT > caps::CONTROLLER_DOCUMENT);
    // And an evidence fetch asks for, and measures against, the same two.
    assert_eq!(request(RECEIPT).payload_cap(), caps::CONTROLLER_RECEIPT);
    assert_eq!(request(SCORECARD).payload_cap(), caps::CONTROLLER_DOCUMENT);
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

/// **The receipt a REAL backup of 500 topics signed gets a real verdict from
/// the controller's own verification, on both of its paths.** The document is
/// the one the live row `e2e/tests/topic_budget.rs` leaves in its scratch
/// directory (`topic-budget/receipt.json` and `receipt.sig`): a real engine,
/// a real broker, signed with the fixture key this file's roster carries.
///
/// `#[ignore]`: it needs those two files, named by `FX33_LIVE_RECEIPT_DIR`.
/// It is a row about a REAL document; the rows above hold the same code to
/// generated ones at every size on every run.
///
/// NEGATIVE CONTROLS: the same bytes under the 1 MiB document cap, which is
/// how the controller read a receipt before FX-33, are `NotAttempted`; and one
/// changed byte is `Invalid`.
#[test]
#[ignore = "live: needs FX33_LIVE_RECEIPT_DIR, where e2e/tests/topic_budget.rs wrote receipt.json and receipt.sig"]
fn the_receipt_a_real_backup_of_many_topics_signed_is_verified_by_the_controller() {
    let dir = PathBuf::from(
        std::env::var("FX33_LIVE_RECEIPT_DIR").expect("FX33_LIVE_RECEIPT_DIR names the directory"),
    );
    let receipt = std::fs::read(dir.join("receipt.json")).expect("receipt.json");
    let sidecar = std::fs::read(dir.join("receipt.sig")).expect("receipt.sig");
    let document: logweir_core::backup_receipt::BackupReceipt =
        serde_json::from_slice(&receipt).expect("a backup receipt");
    let topics = document.source.topics.len();
    let len = receipt.len() as u64;
    let digest = sha256_prefixed(&receipt);
    let trust = trust();

    // 1. The controller's own handle.
    let store = stored(&receipt, &sidecar);
    let r = verify_evidence(
        Some(&store),
        &trust,
        PAYLOAD_KEY,
        &digest,
        SIDECAR_KEY,
        RECEIPT,
    );
    assert_eq!(r.result, VerificationVerdict::Valid, "{:?}", r.detail);
    assert_eq!(r.matched_key_id.as_deref(), Some(FIXTURE_KEY_ID));
    let o = observe_archive(&store, &keys()).expect("two keys: an observation");
    assert_eq!(o.receipt_sha256, Some(digest.clone()));
    assert_eq!(
        o.covered,
        Some((document.covered.from_ms, document.covered.to_ms))
    );
    let records: u64 = document.records.values().sum();
    assert_eq!(o.records, Some(i64::try_from(records).unwrap()));

    // 2. The relay, framed as `logweir check run` frames it.
    let log = relay_log(&receipt, &sidecar);
    let request = request(RECEIPT);
    let relay = decode(&log, &expectations()).expect("the receipt's relay decodes");
    let (
        Presence::Complete,
        Relayed::Both {
            payload,
            sidecar: relayed_sidecar,
        },
    ) = read_relay(&relay, &request)
    else {
        panic!("the relay carries both objects whole");
    };
    let via_relay = verify_fetched(
        &payload,
        &relayed_sidecar,
        &trust,
        PAYLOAD_KEY,
        &digest,
        SIDECAR_KEY,
        RECEIPT,
    );
    assert_eq!(
        via_relay.result,
        VerificationVerdict::Valid,
        "{:?}",
        via_relay.detail
    );

    eprintln!(
        "[fx33-live] a real receipt of {topics} topics, {len} bytes ({} a topic), {records} \
         records: Valid by the controller's handle and by the relay ({} bytes of log); key {}",
        len / topics.max(1) as u64,
        log.len(),
        FIXTURE_KEY_ID
    );

    // CONTROL: what the controller answered for these bytes before FX-33.
    if len > caps::CONTROLLER_DOCUMENT {
        let old = verify_fetched(
            &receipt,
            &sidecar,
            &trust,
            PAYLOAD_KEY,
            &digest,
            SIDECAR_KEY,
            SCORECARD,
        );
        assert_eq!(old.result, VerificationVerdict::NotAttempted, "{old:?}");
        assert!(
            names(
                old.detail.as_deref().unwrap_or_default(),
                caps::CONTROLLER_DOCUMENT
            ),
            "{old:?}"
        );
        eprintln!("[fx33-live] under the 1 MiB document cap the same bytes are NotAttempted");
    }
    // CONTROL: one byte changed after signing.
    let mut tampered = receipt.clone();
    let marker = b"\"backup_id\": \"";
    let at = tampered
        .windows(marker.len())
        .position(|w| w == marker)
        .expect("the receipt names its set")
        + marker.len();
    assert!(tampered[at].is_ascii_alphabetic());
    tampered[at] ^= 0x20;
    let r = verify_fetched(
        &tampered,
        &sidecar,
        &trust,
        PAYLOAD_KEY,
        &sha256_prefixed(&tampered),
        SIDECAR_KEY,
        RECEIPT,
    );
    assert_eq!(r.result, VerificationVerdict::Invalid, "{r:?}");
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
    let relay = decode(&log, &expectations()).expect("the largest receipt decodes");
    assert_eq!(
        relay.stream(Stream::EvidencePayload),
        Some(largest.as_slice())
    );
    assert!(matches!(
        read_relay(&relay, &request),
        (Presence::Complete, Relayed::Both { .. })
    ));
    assert!(DECODER_BUDGET_BYTES <= read_limit);

    // One byte more is refused by the relay reader, BY THE RECEIPT'S CAP.
    let mut over = largest;
    over.push(b'r');
    let relay = decode(&relay_log(&over, &sidecar), &expectations()).expect("it decodes");
    match read_relay(&relay, &request).1 {
        Relayed::Unread { detail } => assert!(
            detail.contains(&format!("{}-byte cap", caps::CONTROLLER_RECEIPT)),
            "{detail}"
        ),
        other => panic!("one byte over the receipt cap is not read: {other:?}"),
    }
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
// 5. Peak resident memory, measured in child processes
// ===========================================================================

const TEST_NAME: &str = "a_receipt_read_and_a_receipt_relay_stay_bounded";
const CHILD_ENV: &str = "FX33_MEM_CHILD";
const ROOT_ENV: &str = "FX33_MEM_ROOT";
const PEAK_LINE: &str = "FX33_PEAK_RSS=";

/// The most one relay of the largest receipt may add, from its pod-log read
/// to its verdict: the log (under 8 MiB), its decoded payload, the
/// verifier's copy and the signature's, measured at about 27 MB.
const RELAY_BOUND_BYTES: u64 = 40 << 20;

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
/// keys, and the relay log of the pair.
fn plant(root: &Path, receipt: &[u8], sidecar: &[u8]) {
    for (key, bytes) in [(PAYLOAD_KEY, receipt), (SIDECAR_KEY, sidecar)] {
        let path = root.join(key);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, bytes).unwrap();
    }
    std::fs::write(root.join("relay.log"), relay_log(receipt, sidecar)).unwrap();
    std::fs::write(root.join("digest"), sha256_prefixed(receipt)).unwrap();
}

/// One relay, as `evidence_fetch` and the `Backup` reconciler hold it: the
/// pod log read into a string and decoded; the relay read into the two
/// documents; the receipt verified and its facts folded. With `as_value`, the
/// receipt is ALSO parsed into a `serde_json::Value` — what the relay path
/// did before FX-33 — and held beside the rest.
fn one_relay(root: &Path, kind: &'static str, as_value: bool) {
    let request = request(kind);
    let digest = std::fs::read_to_string(root.join("digest")).unwrap();
    let relay = {
        let log = std::fs::read_to_string(root.join("relay.log")).expect("the pod log");
        decode(&log, &expectations()).expect("the relay decodes")
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
        "relay" => one_relay(root, RECEIPT, false),
        "relay-value" => one_relay(root, RECEIPT, true),
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

/// **One receipt read, and one receipt relay, stay bounded — with a receipt
/// at the bound.**
///
/// In child processes, each against a baseline child doing the same work
/// over the 1 KB signed fixture:
///
/// - the controller's three store reads of the receipt at the bound
///   (`verify_evidence`, `observe_archive`, `read_signing_time`) add less
///   than the `DOCUMENT_READ_COST_BYTES` each reserves: the bytes and the
///   signature's pre-authentication copy of them, and nothing per topic;
/// - one RELAY of it — the pod log read into a string, decoded, read,
///   verified, folded — adds less than [`RELAY_BOUND_BYTES`].
///
/// The control parses the receipt into a `serde_json::Value` beside the
/// three reads — what the controller did before FX-33 — and must add at least
/// twice the document on top, which shows the meter sees a tree. (The relay's
/// own `Value` figure is printed beside it.)
///
/// KILLS: the fold replaced by a whole parse in `claim_of`, `observe_archive`
/// or the relay path (the read or the relay child holds the control's
/// memory).
#[test]
fn a_receipt_read_and_a_receipt_relay_stay_bounded() {
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
    );
    let (topics, receipt, sidecar) = reference("at-the-bound");
    plant(&big, &receipt, &sidecar);
    let len = receipt.len() as u64;
    let log_len = std::fs::metadata(big.join("relay.log")).unwrap().len();

    let read_base = child_peak("read", &small);
    let read = child_peak("read", &big).saturating_sub(read_base);
    let read_value = child_peak("read-value", &big).saturating_sub(read_base);
    let relay_base = child_peak("relay", &small);
    let relay = child_peak("relay", &big).saturating_sub(relay_base);
    let relay_value = child_peak("relay-value", &big).saturating_sub(relay_base);
    let _ = std::fs::remove_dir_all(&dir);
    eprintln!(
        "[fx33-mem] a receipt of {topics} topics, {len} B (cap {} B). Three store reads add \
         {read} B (each reserves {DOCUMENT_READ_COST_BYTES} B); with the receipt also parsed \
         into a Value, {read_value} B. One relay of it, a {log_len} B pod log, adds {relay} B \
         (bound {RELAY_BOUND_BYTES} B); with the Value, {relay_value} B.",
        caps::CONTROLLER_RECEIPT
    );
    assert!(
        read < DOCUMENT_READ_COST_BYTES,
        "three store reads of a {len}-byte receipt added {read} bytes; a read reserves \
         {DOCUMENT_READ_COST_BYTES}"
    );
    assert!(
        relay < RELAY_BOUND_BYTES,
        "one relay of a {len}-byte receipt added {relay} bytes; the bound is {RELAY_BOUND_BYTES}"
    );
    assert!(
        read_value > read + 2 * len,
        "the Value control added {read_value} bytes against {read}; the meter cannot show what \
         the fold saves"
    );
}
