//! The five checks, in order, and the two reconcilers around them.
//!
//! ONE FILE, BECAUSE THE PLAN NAMES ONE. Every pure-`evaluate` test in Task 16
//! lives here beside the reconciler tests, so the order the checks run in and
//! the status the cluster ends up with are read in one place.
//!
//! # Where the signed material comes from, and why it is `const` and not a file
//!
//! These tests need a signature that genuinely verifies, and this crate
//! **cannot make one**. `scripts/check-one-signer.sh`'s check 3 greps
//! `crates/*/tests` for the signing API and `weirkeeper` is on neither
//! `ALLOWED_LINK` nor `ALLOWED_SOURCE` (Global Constraint 27): a test here that
//! minted a key pair and signed with it would put the controller crate's test
//! target on the signing side of the one boundary this crate exists to respect.
//!
//! So the material below was produced OUT OF TREE, once, by a throwaway
//! Ed25519 key pair generated in a scratch process. **Only the public halves,
//! the key ids and the base64 signatures were ever written down** — the private
//! halves existed only in that process's memory and were never persisted
//! anywhere, in this repository or outside it. Ed25519 rather than P-256
//! because its signatures are deterministic and fixed-length, so a checked-in
//! signature is reproducible rather than one sample of a randomised scheme.
//!
//! They are `const` strings in this file rather than files under a `fixtures/`
//! directory for one reason: Task 16's Files block names the files this task
//! creates, and a test fixture tree is not one of them. Nothing is lost — the
//! bytes are as fixed either way — and a reader gets the document, the key and
//! the signature over it in one screen.
//!
//! # What each test is allowed to prove
//!
//! The pure tests call [`evaluate`] directly, with no client and no route
//! table, because the property is an ORDER over bytes. The reconciler tests go
//! through `weirkeeper::testing::mock_client_recording`, whose double panics on
//! a request it was not given a route for — that panic is what makes "the
//! reconciler asked for nothing else" an assertion rather than a hope. None of
//! them dials a socket, waits on a Job, or comes near STANDING RULE 22's 15 s
//! per-test bound.
//!
//! [`evaluate`]: weirkeeper::controllers::approval::evaluate

use std::path::Path;

use chrono::{DateTime, Duration, Utc};
use kube::api::ObjectMeta;
use logweir_core::ids::sha256_prefixed;
use weirkeeper::conditions::apply_merge_patch;
use weirkeeper::controllers::approval::{
    self, evaluate, ApprovalOutcome, ApprovalRefusal, ReferentProblem, PAYLOAD_TYPE_APPROVAL,
    ROSTER_NAME, ROSTER_NOT_FOUND_MESSAGE,
};
use weirkeeper::controllers::trust_roster;
use weirkeeper::crds::approval::ApprovalStatus;
use weirkeeper::crds::approval::{
    Approval, ApprovalSpec, SubjectKind, SubjectRef, VerifiedSubjectRef,
};
use weirkeeper::crds::trust_roster::TrustRosterStatus;
use weirkeeper::crds::trust_roster::{KeyEntry, TrustRoster, TrustRosterSpec};
use weirkeeper::testing::{
    mock_client_recording, mock_client_recording_bodies, Recorder, Route, SeenBody, SeenRequest,
};

// ---------------------------------------------------------------------------
// The out-of-tree signed material. See the module header.
// ---------------------------------------------------------------------------

/// The referent's `spec.planBytes`, byte-for-byte what was hashed into
/// [`APPROVAL_DOC`]'s `plan_hash`.
const PLAN_BYTES: &str = "apiVersion: logweir.dev/v1alpha1\nkind: RestorePlan\ntopics:\n  - orders\nwindow_start: 2026-09-01T00:00:00Z\nwindow_end: 2026-09-02T00:00:00Z\n";

/// `sha256_prefixed(PLAN_BYTES)`. Asserted against the live function by
/// [`the_fixture_plan_hash_is_what_sha256_prefixed_computes`], so a change to
/// either side of the hash is a red test and not a silently-passing fixture.
const PLAN_HASH: &str = "sha256:742778e4f9dc02eced0b9d0b9dc35f3a3ab5dbef5a559375b3aec76e73006091";

/// The approval document, as the UTF-8 text an approver's tool wrote. The
/// bytes [`APPROVER_SIG`] was made over.
const APPROVAL_DOC: &str = r#"{"approver":"ops@example.com","ticket":"CHG-4711","plan_hash":"sha256:742778e4f9dc02eced0b9d0b9dc35f3a3ab5dbef5a559375b3aec76e73006091","approved_at":"2026-09-09T12:00:00Z","subject_kind":"Restore"}"#;

/// The same document, base64. Signed by nobody: it exists so
/// [`approval_bytes_are_the_document_text_not_base64`] can show the two paths
/// are distinguishable.
const APPROVAL_DOC_BASE64: &str = "eyJhcHByb3ZlciI6Im9wc0BleGFtcGxlLmNvbSIsInRpY2tldCI6IkNIRy00NzExIiwicGxhbl9oYXNoIjoic2hhMjU2Ojc0Mjc3OGU0ZjlkYzAyZWNlZDBiOWQwYjlkYzM1ZjNhM2FiNWRiZWY1YTU1OTM3NWIzYWVjNzZlNzMwMDYwOTEiLCJhcHByb3ZlZF9hdCI6IjIwMjYtMDktMDlUMTI6MDA6MDBaIiwic3ViamVjdF9raW5kIjoiUmVzdG9yZSJ9";

/// The rostered approver's **public** key, SubjectPublicKeyInfo PEM.
const APPROVER_PEM: &str = "-----BEGIN PUBLIC KEY-----\nMCowBQYDK2VwAyEApFpEU8uY5S8Lv43HL4DcXKKyM8WHurCPZIxvq8ZBfpY=\n-----END PUBLIC KEY-----\n";
/// `sha256(APPROVER_PEM's SPKI DER)`, lowercase hex.
const APPROVER_KEY_ID: &str = "f27c7f51aad0700db76887b306d413a039156b44ee147c1d82c5e4dc339558f6";
/// A genuine Ed25519 signature over `PAE(PAYLOAD_TYPE_APPROVAL, APPROVAL_DOC)`.
const APPROVER_SIG: &str =
    "afLmiRCAVRJGg0IfHJTDWHWQQE+PXZWqryC5ATTC2GUHcvriC4RRyy+4ZzhONWM1V5HmBeSO52eMH+6I75AMBQ==";

/// A key that is NOT on the roster — the attacker's, in the two-signature
/// case. Its **public** half only.
const OUTSIDER_PEM: &str = "-----BEGIN PUBLIC KEY-----\nMCowBQYDK2VwAyEAKTrTSpPTt1d9M1kMim3Imkt2s1OjRm48GfqVh+fjvyk=\n-----END PUBLIC KEY-----\n";
/// `sha256(OUTSIDER_PEM's SPKI DER)`, lowercase hex.
const OUTSIDER_KEY_ID: &str = "067bf4d360d3c0658620a75a225d4d3e2e038cdb12cdf38ef6611241b9d380d2";
/// A genuine signature over the same document, by the unrostered key. It is a
/// VALID signature — just not by anyone authorised — which is the whole point
/// of [`approval_refuses_a_key_outside_the_roster`].
const OUTSIDER_SIG: &str =
    "i9KK+ZDF4eieuRUlr8IXbaHEmemdluCxDn51A8+qwbB51rpqjgYYFy5Zk9M3ejNW9x3RQt/Jh+VRydzXpm+7CQ==";

/// PEM armour around bytes that are not a key at all.
const UNPARSEABLE_PEM: &str =
    "-----BEGIN PUBLIC KEY-----\nbm90IGEgcHVibGljIGtleQ==\n-----END PUBLIC KEY-----\n";
/// The `keyId` the unparseable entry declares. Every refusal about it must
/// name this string.
const BAD_KEY_ID: &str = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef";

/// The DSSE payload type of a SCORECARD — a genuinely-signed sidecar for a
/// different kind of document.
const PAYLOAD_TYPE_SCORECARD: &str = logweir_verify::PAYLOAD_TYPE_SCORECARD;

/// The namespace every namespaced route below is written against — STANDING
/// RULE 13, `logweir-t<N>` for task 16.
const NS: &str = "logweir-t16";

// ---------------------------------------------------------------------------
// Builders
// ---------------------------------------------------------------------------

/// A fixed clock. Every expiry comparison in this file is relative to it, so
/// no test in here can start failing because a wall clock passed a literal.
fn now() -> DateTime<Utc> {
    DateTime::parse_from_rfc3339("2026-09-09T13:00:00Z")
        .expect("a literal RFC 3339 timestamp")
        .with_timezone(&Utc)
}

fn key(key_id: &str, pem: &str, not_after: Option<DateTime<Utc>>) -> KeyEntry {
    KeyEntry {
        key_id: key_id.to_string(),
        spki_pem: pem.to_string(),
        subject: None,
        not_after,
    }
}

/// The good approver key, unexpired.
fn approver() -> KeyEntry {
    key(APPROVER_KEY_ID, APPROVER_PEM, None)
}

fn roster_spec(approver_keys: Vec<KeyEntry>, signing_keys: Vec<KeyEntry>) -> TrustRosterSpec {
    TrustRosterSpec {
        approver_keys,
        signing_keys,
        allowed_cluster_ids: vec!["scratch-cluster-id".to_string()],
    }
}

/// The ordinary roster: one good approver key, no signing keys.
fn one_good_key() -> TrustRosterSpec {
    roster_spec(vec![approver()], Vec::new())
}

/// A sidecar document, as the UTF-8 text `spec.sidecarBytes` carries.
fn sidecar(payload_type: &str, signatures: &[(&str, &str)]) -> String {
    let sigs = signatures
        .iter()
        .map(|(keyid, sig)| format!(r#"{{"keyid":"{keyid}","sig":"{sig}"}}"#))
        .collect::<Vec<_>>()
        .join(",");
    format!(r#"{{"payloadType":"{payload_type}","signatures":[{sigs}]}}"#)
}

/// The sidecar the good key made, over [`APPROVAL_DOC`].
fn good_sidecar() -> String {
    sidecar(PAYLOAD_TYPE_APPROVAL, &[(APPROVER_KEY_ID, APPROVER_SIG)])
}

/// An `Approval` as it would arrive from a watch.
fn approval_object(approval_bytes: &str, sidecar_bytes: &str, kind: SubjectKind) -> Approval {
    Approval {
        metadata: ObjectMeta {
            name: Some("a1".to_string()),
            namespace: Some(NS.to_string()),
            generation: Some(1),
            ..ObjectMeta::default()
        },
        spec: ApprovalSpec {
            subject_ref: SubjectRef {
                kind,
                name: match kind {
                    SubjectKind::Restore => "r1".to_string(),
                    SubjectKind::Backup => "b1".to_string(),
                },
            },
            plan_hash: PLAN_HASH.to_string(),
            approval_bytes: approval_bytes.to_string(),
            sidecar_bytes: sidecar_bytes.to_string(),
        },
        status: None,
    }
}

/// A `TrustRoster` body the double answers `GET …/trustrosters/default` with.
fn roster_body(approver_keys: &str, signing_keys: &str) -> String {
    format!(
        r#"{{"apiVersion":"logweir.dev/v1alpha1","kind":"TrustRoster",
             "metadata":{{"name":"{ROSTER_NAME}"}},
             "spec":{{"approverKeys":[{approver_keys}],"signingKeys":[{signing_keys}],
                      "allowedClusterIds":["scratch-cluster-id"]}}}}"#
    )
}

/// The good approver key, as a JSON `KeyEntry` for [`roster_body`].
fn approver_entry_json() -> String {
    let pem = APPROVER_PEM.replace('\n', "\\n");
    format!(r#"{{"keyId":"{APPROVER_KEY_ID}","spkiPem":"{pem}"}}"#)
}

/// A `Restore` body the double answers `GET …/restores/r1` with.
///
/// `status` CARRIES A `planHash` THAT IS NOT A FIELD OF `RestoreStatus`. That
/// is deliberate and is the second arm of
/// [`approval_recomputes_the_plan_hash_from_the_referent_bytes`]: serde drops
/// unknown fields, so a status that names the *correct* hash cannot reach the
/// controller at all, let alone rescue an approval. Both halves are asserted —
/// the type has no such field, and the refusal still happens.
fn restore_body(plan_bytes: &str, status_plan_hash: &str) -> String {
    let plan = plan_bytes.replace('\n', "\\n");
    format!(
        r#"{{"apiVersion":"logweir.dev/v1alpha1","kind":"Restore",
             "metadata":{{"name":"r1","namespace":"{NS}","uid":"restore-uid-1"}},
             "spec":{{"planBytes":"{plan}","approvalRef":{{"name":"a1"}},
                      "sourceArchive":{{"url":"s3://archive/logweir"}},
                      "backupSetRef":"bk-1","pointInTime":"2026-09-02T00:00:00Z",
                      "target":{{"clusterRef":{{"name":"scratch"}},"mode":"scratch",
                                 "topicNaming":{{"prefix":"restored-"}}}},
                      "deadlineSeconds":900}},
             "status":{{"phase":"Pending","planHash":"{status_plan_hash}"}}}}"#
    )
}

/// A `Backup` body the double answers `GET …/backups/b1` with.
fn backup_body() -> String {
    format!(
        r#"{{"apiVersion":"logweir.dev/v1alpha1","kind":"Backup",
             "metadata":{{"name":"b1","namespace":"{NS}"}},
             "spec":{{"sourceRef":{{"name":"prod"}},"topics":["orders"],
                      "archive":{{"url":"s3://archive/logweir"}},
                      "triggeredBy":"manual","deadlineSeconds":900}}}}"#
    )
}

/// What the API server answers a `PATCH …/approvals/a1/status` with.
fn patched_approval_body() -> String {
    let doc = APPROVAL_DOC.replace('"', "\\\"");
    let side = good_sidecar().replace('"', "\\\"");
    format!(
        r#"{{"apiVersion":"logweir.dev/v1alpha1","kind":"Approval",
             "metadata":{{"name":"a1","namespace":"{NS}"}},
             "spec":{{"subjectRef":{{"kind":"Restore","name":"r1"}},"planHash":"{PLAN_HASH}",
                      "approvalBytes":"{doc}","sidecarBytes":"{side}"}},
             "status":{{"verified":true}}}}"#
    )
}

/// A Kubernetes `Status` with `code: 404`, which is what an API server answers
/// for an absent object.
fn not_found_body(message: &str) -> String {
    format!(
        r#"{{"kind":"Status","apiVersion":"v1","status":"Failure","message":"{message}",
             "reason":"NotFound","code":404}}"#
    )
}

/// Every request the double saw, method and path, query string dropped.
fn seen(recorder: &Recorder) -> Vec<(String, String)> {
    recorder
        .lock()
        .expect("the recorder mutex")
        .iter()
        .map(|SeenRequest { method, uri }| {
            (
                method.clone(),
                uri.split('?').next().unwrap_or(uri).to_string(),
            )
        })
        .collect()
}

// ---------------------------------------------------------------------------
// The fixtures are what they claim to be
// ---------------------------------------------------------------------------

/// The checked-in `PLAN_HASH` is what the shared hash function computes.
///
/// WITHOUT THIS, EVERY PLAN-HASH TEST BELOW COULD PASS VACUOUSLY. Check 7
/// compares the document's `plan_hash` against `sha256_prefixed` of the
/// referent's bytes; if the fixture's hash were wrong, the happy-path test
/// would fail — but the MISMATCH tests would still pass, for the wrong reason.
/// This pins the fixture to the live function.
#[test]
fn the_fixture_plan_hash_is_what_sha256_prefixed_computes() {
    assert_eq!(sha256_prefixed(PLAN_BYTES.as_bytes()), PLAN_HASH);
    assert!(
        APPROVAL_DOC.contains(PLAN_HASH),
        "the signed document must name the fixture plan hash"
    );
}

/// The payload type is byte-equal to the CLI's.
///
/// READ OUT OF THE OTHER FILE, NOT RESTATED. The same approval document is
/// verified by `logweir drill approve`'s phase 1 and by this controller before
/// a Job exists; two literals that are equal today and maintained separately
/// are two literals that will differ.
#[test]
fn the_payload_type_is_byte_equal_to_the_cli() {
    let src = Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crates/weirkeeper sits two levels under the workspace root")
        .join("crates/logweir/src/drill/phase1_approval.rs");
    let text = std::fs::read_to_string(&src).unwrap_or_else(|e| panic!("read {src:?}: {e}"));
    let wanted = format!("pub const PAYLOAD_TYPE_APPROVAL: &str = \"{PAYLOAD_TYPE_APPROVAL}\";");
    assert!(
        text.contains(&wanted),
        "{} must declare exactly {wanted}",
        src.display()
    );
}

/// `ROSTER_NAME` is `default`, and `weirkeeper::ROSTER_NAME` is the same
/// constant — interface **I16**.
#[test]
fn the_roster_name_is_default_and_is_re_exported_from_the_crate_root() {
    assert_eq!(ROSTER_NAME, "default");
    assert_eq!(weirkeeper::ROSTER_NAME, ROSTER_NAME);
}

/// The missing-roster message names the roster it could not find.
#[test]
fn the_missing_roster_message_names_the_roster() {
    assert_eq!(
        ROSTER_NOT_FOUND_MESSAGE,
        "no cluster-scoped TrustRoster named 'default'; see docs/kubernetes.md install step 1"
    );
    assert!(
        ROSTER_NOT_FOUND_MESSAGE.contains(ROSTER_NAME),
        "the message must name ROSTER_NAME so the two cannot drift"
    );
    assert_eq!(
        ApprovalRefusal::RosterNotFound.to_string(),
        ROSTER_NOT_FOUND_MESSAGE
    );
    assert_eq!(ApprovalRefusal::RosterNotFound.reason(), "RosterNotFound");
}

/// `SubjectKind::as_str` returns the wire spellings, not a second vocabulary.
#[test]
fn the_subject_kind_strings_are_the_wire_spellings() {
    for kind in [SubjectKind::Restore, SubjectKind::Backup] {
        let wire = serde_json::to_string(&kind).expect("a SubjectKind serialises");
        assert_eq!(
            wire,
            format!("\"{}\"", kind.as_str()),
            "as_str must be the serde spelling of {kind:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// Check 0 and check 1 — the sidecar, before any key
// ---------------------------------------------------------------------------

/// An unparseable sidecar is `SignatureInvalid`, naming the parse error.
#[test]
fn an_unparseable_sidecar_is_signature_invalid() {
    let got = evaluate(
        APPROVAL_DOC.as_bytes(),
        b"not a sidecar",
        &one_good_key(),
        now(),
        "Restore",
        PLAN_BYTES.as_bytes(),
    );
    match got {
        Err(ApprovalRefusal::SignatureInvalid(why)) => assert!(
            why.contains("spec.sidecarBytes is not a DSSE sidecar document"),
            "the refusal must name the parse failure; got {why}"
        ),
        other => panic!("expected SignatureInvalid, got {other:?}"),
    }
}

/// **Acceptance.** A sidecar for a different kind of document is refused as a
/// payload-type mismatch, with both strings in full, **before any key is
/// tried**.
///
/// THE ROSTER PROVES THE ORDER, NOT A COMMENT. Its single entry carries an
/// spkiPem that cannot parse, so an implementation that parsed the roster
/// first — check 2 — would return `SignatureInvalid` instead. That is the
/// mutant this test kills: *try the keys before comparing the payload type*.
#[test]
fn approval_refuses_a_wrong_payload_type() {
    let deliberately_unparseable =
        roster_spec(vec![key(BAD_KEY_ID, UNPARSEABLE_PEM, None)], vec![]);
    let got = evaluate(
        APPROVAL_DOC.as_bytes(),
        sidecar(PAYLOAD_TYPE_SCORECARD, &[(APPROVER_KEY_ID, APPROVER_SIG)]).as_bytes(),
        &deliberately_unparseable,
        now(),
        "Restore",
        PLAN_BYTES.as_bytes(),
    );
    assert_eq!(
        got,
        Err(ApprovalRefusal::PayloadTypeMismatch {
            got: PAYLOAD_TYPE_SCORECARD.to_string(),
            want: PAYLOAD_TYPE_APPROVAL.to_string(),
        }),
        "a payload-type mismatch must be reported with both strings in full, and must be \
         reached before the roster's keys are parsed — reaching check 2 with this roster would \
         have returned SignatureInvalid"
    );
}

// ---------------------------------------------------------------------------
// Check 2 and check 3 — the roster, before any signature
// ---------------------------------------------------------------------------

/// **Acceptance.** A roster with one unparseable key refuses **every**
/// approval, including one the other entry would have verified.
///
/// Kills the mutant *skip an unparseable roster entry and continue*: with that
/// change the good first entry verifies and the verdict becomes `Ok`.
#[test]
fn a_roster_with_one_unparseable_key_refuses_every_approval() {
    let partial = roster_spec(
        vec![approver(), key(BAD_KEY_ID, UNPARSEABLE_PEM, None)],
        vec![],
    );
    let got = evaluate(
        APPROVAL_DOC.as_bytes(),
        good_sidecar().as_bytes(),
        &partial,
        now(),
        "Restore",
        PLAN_BYTES.as_bytes(),
    );
    match &got {
        Err(ApprovalRefusal::SignatureInvalid(why)) => {
            assert!(
                why.contains(BAD_KEY_ID),
                "the refusal must name the unparseable entry's keyId; got {why}"
            );
            assert!(
                why.contains("a partially loaded roster is not a roster"),
                "the refusal must say why one bad entry disqualifies the whole roster; got {why}"
            );
        }
        other => panic!(
            "a roster with one unparseable entry must refuse an approval the FIRST entry would \
             have verified; got {other:?}"
        ),
    }
    // The same sidecar and the same first key, on a roster without the bad
    // entry, DOES verify — so the refusal above is about the roster and not
    // about the signature.
    assert!(
        evaluate(
            APPROVAL_DOC.as_bytes(),
            good_sidecar().as_bytes(),
            &one_good_key(),
            now(),
            "Restore",
            PLAN_BYTES.as_bytes(),
        )
        .is_ok(),
        "the control arm must verify, or this test proves nothing about the bad entry"
    );
}

/// A roster entry whose declared `keyId` is not the hash of its own key
/// material is `KeyIdNotInRoster`, naming both, before any signature is
/// checked — check 3.
#[test]
fn a_roster_entry_that_disagrees_with_its_own_key_material_is_key_id_not_in_roster() {
    let lying = roster_spec(vec![key(BAD_KEY_ID, APPROVER_PEM, None)], vec![]);
    let got = evaluate(
        APPROVAL_DOC.as_bytes(),
        good_sidecar().as_bytes(),
        &lying,
        now(),
        "Restore",
        PLAN_BYTES.as_bytes(),
    );
    match got {
        Err(ApprovalRefusal::KeyIdNotInRoster { key_id }) => {
            assert!(
                key_id.contains(BAD_KEY_ID) && key_id.contains(APPROVER_KEY_ID),
                "the refusal must name BOTH the declared id and the id its own spkiPem hashes \
                 to; got {key_id}"
            );
        }
        other => panic!("expected KeyIdNotInRoster, got {other:?}"),
    }
}

/// **Acceptance.** A key outside the roster is `KeyIdNotInRoster`, naming the
/// sidecar's key ids — **not** `SignatureInvalid`.
///
/// Kills the mutant *return `SignatureInvalid` for a key outside the roster
/// instead of reaching step 4*. The signature in this sidecar is a genuine,
/// verifying Ed25519 signature; the only thing wrong with it is who made it,
/// and an operator has to be able to see that.
#[test]
fn approval_refuses_a_key_outside_the_roster() {
    let got = evaluate(
        APPROVAL_DOC.as_bytes(),
        sidecar(PAYLOAD_TYPE_APPROVAL, &[(OUTSIDER_KEY_ID, OUTSIDER_SIG)]).as_bytes(),
        &one_good_key(),
        now(),
        "Restore",
        PLAN_BYTES.as_bytes(),
    );
    match got {
        Err(ApprovalRefusal::KeyIdNotInRoster { key_id }) => {
            assert!(
                key_id.contains(OUTSIDER_KEY_ID),
                "the refusal must name the sidecar's key ids; got {key_id}"
            );
            assert!(
                key_id.contains(APPROVER_KEY_ID),
                "the refusal must also name the roster's approverKeys, so the operator can see \
                 what was compared; got {key_id}"
            );
        }
        other => panic!(
            "a key outside the roster must be KeyIdNotInRoster and never SignatureInvalid: \
             the signature verified, the signer is not authorised. Got {other:?}"
        ),
    }
}

// ---------------------------------------------------------------------------
// Check 5 — the matched key id, and nothing else
// ---------------------------------------------------------------------------

/// **Acceptance.** With two signatures — index 0 by an unrostered key, index 1
/// by a rostered one — `evaluate` reports the **matched** key id.
///
/// Kills the mutant *return `sidecar.signatures[0].keyid` instead of the
/// matched one*. `verify_detached` returns the matched keyid
/// (`crates/logweir-verify/src/verify.rs:60`) precisely so this is possible;
/// a caller that ignores the return value and reports `signatures[0]` credits
/// the attacker's key with an approval the honest key made.
#[test]
fn approval_evaluate_returns_the_matched_key_id_not_signatures_zero() {
    let two = sidecar(
        PAYLOAD_TYPE_APPROVAL,
        &[
            (OUTSIDER_KEY_ID, OUTSIDER_SIG),
            (APPROVER_KEY_ID, APPROVER_SIG),
        ],
    );
    // The premise: index 0 really is the unrostered key.
    let parsed: serde_json::Value = serde_json::from_str(&two).expect("the sidecar is JSON");
    assert_eq!(parsed["signatures"][0]["keyid"], OUTSIDER_KEY_ID);
    assert_eq!(parsed["signatures"][1]["keyid"], APPROVER_KEY_ID);

    let verified = evaluate(
        APPROVAL_DOC.as_bytes(),
        two.as_bytes(),
        &one_good_key(),
        now(),
        "Restore",
        PLAN_BYTES.as_bytes(),
    )
    .expect("the rostered signature verifies");
    assert_eq!(
        verified.matched_key_id, APPROVER_KEY_ID,
        "the reported key id must be the one that VERIFIED, never signatures[0]"
    );
    assert_ne!(verified.matched_key_id, OUTSIDER_KEY_ID);
    assert_eq!(verified.approver, "ops@example.com");
    assert_eq!(verified.ticket, "CHG-4711");
}

/// A sidecar whose signature is by a rostered key but does not verify over the
/// bytes is `SignatureInvalid` — the variant that means what it says.
#[test]
fn a_signature_that_does_not_verify_over_the_bytes_is_signature_invalid() {
    // The rostered key id, with the OUTSIDER's signature bytes under it: the
    // key id matches the roster (so check 4 passes) and the crypto does not.
    let forged = sidecar(PAYLOAD_TYPE_APPROVAL, &[(APPROVER_KEY_ID, OUTSIDER_SIG)]);
    match evaluate(
        APPROVAL_DOC.as_bytes(),
        forged.as_bytes(),
        &one_good_key(),
        now(),
        "Restore",
        PLAN_BYTES.as_bytes(),
    ) {
        Err(ApprovalRefusal::SignatureInvalid(_)) => {}
        other => panic!("expected SignatureInvalid, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Check 6 — notAfter
// ---------------------------------------------------------------------------

/// **Acceptance.** A matched key one second past its `notAfter` is
/// `KeyIdExpired`.
#[test]
fn approval_refuses_an_expired_key() {
    let expired_at = now() - Duration::seconds(1);
    let expired = roster_spec(
        vec![key(APPROVER_KEY_ID, APPROVER_PEM, Some(expired_at))],
        vec![],
    );
    assert_eq!(
        evaluate(
            APPROVAL_DOC.as_bytes(),
            good_sidecar().as_bytes(),
            &expired,
            now(),
            "Restore",
            PLAN_BYTES.as_bytes(),
        ),
        Err(ApprovalRefusal::KeyIdExpired {
            key_id: APPROVER_KEY_ID.to_string(),
            not_after: expired_at.to_rfc3339(),
        })
    );
    // One second the other way verifies, so the boundary is the clock and not
    // the presence of the field.
    let fresh = roster_spec(
        vec![key(
            APPROVER_KEY_ID,
            APPROVER_PEM,
            Some(now() + Duration::seconds(1)),
        )],
        vec![],
    );
    assert!(evaluate(
        APPROVAL_DOC.as_bytes(),
        good_sidecar().as_bytes(),
        &fresh,
        now(),
        "Restore",
        PLAN_BYTES.as_bytes(),
    )
    .is_ok());
}

// ---------------------------------------------------------------------------
// Check 7 — the plan hash, recomputed
// ---------------------------------------------------------------------------

/// **Acceptance, first arm.** The referent's `spec.planBytes` mutated by one
/// byte after signing is `PlanHashMismatch`.
#[test]
fn approval_recomputes_the_plan_hash_from_the_referent_bytes() {
    let mutated = PLAN_BYTES.replacen("orders", "ordera", 1);
    assert_eq!(
        mutated.len(),
        PLAN_BYTES.len(),
        "the mutation must be one byte, not a length change"
    );
    assert_eq!(
        evaluate(
            APPROVAL_DOC.as_bytes(),
            good_sidecar().as_bytes(),
            &one_good_key(),
            now(),
            "Restore",
            mutated.as_bytes(),
        ),
        Err(ApprovalRefusal::PlanHashMismatch {
            got: PLAN_HASH.to_string(),
            want: sha256_prefixed(mutated.as_bytes()),
        }),
        "check 7 must hash the referent's OWN bytes; one changed byte is a different plan"
    );
}

/// **Acceptance, second arm.** A referent whose `status` names the *correct*
/// plan hash does not rescue an approval whose `spec.planBytes` were mutated.
///
/// TWO HALVES, AND THE FIRST IS THE STRONGER ONE. `RestoreStatus` carries no
/// `planHash` field at all, so the JSON below cannot reach the controller —
/// serde drops it. That is asserted directly, and then the reconcile is run to
/// show the refusal happens anyway. Kills the mutant *read `plan_hash` from
/// the referent's `status.planHash`*: with that change there is no field to
/// read, so the mutant cannot even be written without first adding one — and
/// if it is added, this test's first half fails.
#[tokio::test]
async fn a_correct_status_plan_hash_does_not_rescue_a_mutated_plan() {
    let mutated = PLAN_BYTES.replacen("orders", "ordera", 1);
    let body = restore_body(&mutated, PLAN_HASH);

    let restore: weirkeeper::crds::restore::Restore =
        serde_json::from_str(&body).expect("the fixture is a Restore");
    let round_tripped = serde_json::to_value(&restore.status).expect("a RestoreStatus serialises");
    assert!(
        round_tripped.get("planHash").is_none(),
        "RestoreStatus must carry no planHash field for check 7 to read; got {round_tripped}"
    );

    let (client, recorder) = mock_client_recording(vec![
        Route {
            method: "GET",
            path_suffix: "/trustrosters/default",
            status: 200,
            body: roster_body(&approver_entry_json(), ""),
        },
        Route {
            method: "GET",
            path_suffix: "/restores/r1",
            status: 200,
            body,
        },
        Route {
            method: "PATCH",
            path_suffix: "/approvals/a1/status",
            status: 200,
            body: patched_approval_body(),
        },
    ]);
    let approval = approval_object(APPROVAL_DOC, &good_sidecar(), SubjectKind::Restore);
    let outcome = approval::reconcile_approval(&approval, &client)
        .await
        .expect("the reconcile completes");
    match outcome {
        ApprovalOutcome::Refused(ApprovalRefusal::PlanHashMismatch { got, want }) => {
            assert_eq!(got, PLAN_HASH);
            assert_eq!(want, sha256_prefixed(mutated.as_bytes()));
        }
        other => panic!("a status field must not rescue a mutated plan; got {other:?}"),
    }
    assert!(
        seen(&recorder)
            .iter()
            .any(|(m, p)| m == "PATCH" && p.ends_with("/approvals/a1/status")),
        "the refusal is still recorded on /status"
    );
}

/// **The unsigned `spec.planHash` must name the plan that was signed.**
///
/// `spec.planHash` is a plain CRD field beside the two documents: nothing in
/// checks 1-8 reads it, because authorisation rests on the hash INSIDE the
/// signed bytes. But it is the value `kubectl get approval -o yaml`, the
/// `SUBJECT` view and the UI all SHOW, and the CRD has always said a wrong one
/// is a refusal. Without the comparison in `decide`, an `Approval` could be
/// `Verified=True` while displaying a plan hash that is not the plan it
/// authorises — so the one thing this field exists for, letting a reader
/// compare, would be the one thing it could not be trusted for.
///
/// Kills the mutant *drop the `spec.planHash` comparison*. The POSITIVE
/// CONTROL runs first, over the same fixtures and the same routes, so this row
/// can only pass because the comparison exists — never because the fixture
/// stopped verifying at all.
#[tokio::test]
async fn a_forged_spec_plan_hash_is_refused_even_when_the_signed_document_matches() {
    let control = approval_object(APPROVAL_DOC, &good_sidecar(), SubjectKind::Restore);
    assert_eq!(
        control.spec.plan_hash, PLAN_HASH,
        "the control names the signed plan"
    );
    let (client, _) =
        mock_client_recording(approval_routes((200, restore_body(PLAN_BYTES, PLAN_HASH))));
    match approval::reconcile_approval(&control, &client)
        .await
        .expect("the reconcile completes")
    {
        ApprovalOutcome::Verified(_) => {}
        other => panic!("the control must verify, or this test proves nothing; got {other:?}"),
    }

    // The ONE change: the unsigned field beside the documents names another
    // plan. The signature still verifies and the document's own `plan_hash`
    // still equals the referent's bytes.
    let mut forged = approval_object(APPROVAL_DOC, &good_sidecar(), SubjectKind::Restore);
    let claimed = sha256_prefixed(b"a plan nobody signed");
    forged.spec.plan_hash.clone_from(&claimed);
    let (forged_client, recorder) =
        mock_client_recording(approval_routes((200, restore_body(PLAN_BYTES, PLAN_HASH))));
    let outcome = approval::reconcile_approval(&forged, &forged_client)
        .await
        .expect("the reconcile completes");
    match &outcome {
        ApprovalOutcome::Refused(ApprovalRefusal::PlanHashMismatch { got, want }) => {
            assert_eq!(
                got, &claimed,
                "the refusal names the value the object DISPLAYS"
            );
            assert_eq!(want, PLAN_HASH, "and the hash of the referent's own bytes");
        }
        other => panic!("a forged spec.planHash must not verify; got {other:?}"),
    }
    assert_eq!(
        outcome.reason(),
        "PlanHashMismatch",
        "the fact is check 7's — this approval names a plan the referent does not carry — so the \
         closed set of reasons does not grow a tenth member"
    );
    assert!(
        outcome.message().contains(&claimed) && outcome.message().contains(PLAN_HASH),
        "the message names both hashes: {}",
        outcome.message()
    );
    let status = approval::status_for(&forged, &outcome, now());
    assert_eq!(status.verified, Some(false));
    assert!(
        status.matched_key_id.is_none(),
        "a refused approval reports no key id"
    );
    assert!(
        status.verified_subject_ref.is_none(),
        "and binds no subject: a refusal is not provenance"
    );
    assert!(
        seen(&recorder)
            .iter()
            .any(|(m, p)| m == "PATCH" && p.ends_with("/approvals/a1/status")),
        "the refusal is recorded on /status, where every reader of this object sees it"
    );
}

// ---------------------------------------------------------------------------
// Check 8 — the subject kind, from inside the signed bytes
// ---------------------------------------------------------------------------

/// **Acceptance.** An approval that binds a `Restore` is refused for a
/// `Backup`.
///
/// Kills the mutant *delete check 8*. Without it the second approval
/// degenerates: "a valid signature by a rostered key exists in this namespace"
/// is a property any approved restore already produced, and an `Approval`
/// whose `planHash` matched a `Restore` would authorise a tag-2 `Switchover`.
#[test]
fn an_approval_for_a_restore_is_refused_for_another_kind() {
    assert!(
        APPROVAL_DOC.contains(r#""subject_kind":"Restore""#),
        "the subject kind must be INSIDE the signed bytes, or check 8 binds nothing"
    );
    assert_eq!(
        evaluate(
            APPROVAL_DOC.as_bytes(),
            good_sidecar().as_bytes(),
            &one_good_key(),
            now(),
            "Backup",
            PLAN_BYTES.as_bytes(),
        ),
        Err(ApprovalRefusal::SubjectKindMismatch {
            approval_says: "Restore".to_string(),
            referent_is: "Backup".to_string(),
        })
    );
}

// ---------------------------------------------------------------------------
// The bytes are the document text
// ---------------------------------------------------------------------------

/// **Acceptance.** `approvalBytes` is the raw document text; a base64 of the
/// same document is refused.
///
/// THE TWO PATHS ARE DISTINGUISHABLE, WHICH IS THE POINT. Kills the mutant
/// *base64-decode `approvalBytes` before hashing*: under that change the
/// raw-text arm stops verifying, because a decode of document text is not the
/// document.
#[test]
fn approval_bytes_are_the_document_text_not_base64() {
    let verified = evaluate(
        APPROVAL_DOC.as_bytes(),
        good_sidecar().as_bytes(),
        &one_good_key(),
        now(),
        "Restore",
        PLAN_BYTES.as_bytes(),
    )
    .expect("the raw document text verifies");
    assert_eq!(verified.matched_key_id, APPROVER_KEY_ID);

    match evaluate(
        APPROVAL_DOC_BASE64.as_bytes(),
        good_sidecar().as_bytes(),
        &one_good_key(),
        now(),
        "Restore",
        PLAN_BYTES.as_bytes(),
    ) {
        Err(ApprovalRefusal::SignatureInvalid(_)) => {}
        other => panic!(
            "a base64 of the same document must NOT verify — otherwise a future decode step \
             could be added silently. Got {other:?}"
        ),
    }
}

// ---------------------------------------------------------------------------
// selfAttestedRisk — labelled, never refused
// ---------------------------------------------------------------------------

/// An approver key that is also a signing key is `selfAttestedRisk: true`, and
/// is still verified.
///
/// `false` MEANS ONLY "TWO DIFFERENT KEY IDS". One operator holding both keys
/// satisfies it (`design-operator.md:169-181`), which is why this is a label
/// on the status and not an eighth refusal.
#[test]
fn a_matched_approver_key_that_is_also_a_signing_key_is_self_attested_risk() {
    let both = roster_spec(vec![approver()], vec![approver()]);
    let verified = evaluate(
        APPROVAL_DOC.as_bytes(),
        good_sidecar().as_bytes(),
        &both,
        now(),
        "Restore",
        PLAN_BYTES.as_bytes(),
    )
    .expect("the same key on both lists still verifies");
    assert!(verified.self_attested_risk);

    let separate = roster_spec(
        vec![approver()],
        vec![key(OUTSIDER_KEY_ID, OUTSIDER_PEM, None)],
    );
    let verified = evaluate(
        APPROVAL_DOC.as_bytes(),
        good_sidecar().as_bytes(),
        &separate,
        now(),
        "Restore",
        PLAN_BYTES.as_bytes(),
    )
    .expect("two different keys verify");
    assert!(!verified.self_attested_risk);
}

// ---------------------------------------------------------------------------
// The reconciler
// ---------------------------------------------------------------------------

/// **Acceptance.** A missing roster names itself, and nothing else is touched.
///
/// Kills the mutant *resolve a roster by any name the `Approval` supplies, or
/// refuse silently when it is absent*. The route table answers 404 for
/// `…/trustrosters/default` and holds no other GET, so a reconciler that
/// resolved a name from the object would panic the double instead of reaching
/// the 404.
#[tokio::test]
async fn a_missing_roster_names_itself() {
    let (client, recorder) = mock_client_recording(vec![
        Route {
            method: "GET",
            path_suffix: "/trustrosters/default",
            status: 404,
            body: not_found_body("trustrosters.logweir.dev \\\"default\\\" not found"),
        },
        Route {
            method: "PATCH",
            path_suffix: "/approvals/a1/status",
            status: 200,
            body: patched_approval_body(),
        },
    ]);
    let approval = approval_object(APPROVAL_DOC, &good_sidecar(), SubjectKind::Restore);
    let outcome = approval::reconcile_approval(&approval, &client)
        .await
        .expect("a 404 on the roster is a verdict, not an error");
    assert_eq!(
        outcome,
        ApprovalOutcome::Refused(ApprovalRefusal::RosterNotFound)
    );

    // The condition the reconciler patched. `status_for` is the function that
    // BUILDS the patch body, so asserting over it asserts the bytes that were
    // sent — the double records methods and paths, not bodies.
    let status = approval::status_for(&approval, &outcome, now());
    assert_eq!(status.verified, Some(false));
    let conditions = status.conditions.expect("a condition is written");
    assert_eq!(conditions.len(), 1);
    assert_eq!(conditions[0].r#type, "Verified");
    assert_eq!(conditions[0].status, "False");
    assert_eq!(conditions[0].reason.as_deref(), Some("RosterNotFound"));
    assert_eq!(
        conditions[0].message.as_deref(),
        Some(ROSTER_NOT_FOUND_MESSAGE),
        "the message is the exact sentence interface I16 fixes"
    );

    let calls = seen(&recorder);
    assert_eq!(
        calls,
        vec![
            (
                "GET".to_string(),
                format!("/apis/logweir.dev/v1alpha1/trustrosters/{ROSTER_NAME}")
            ),
            (
                "PATCH".to_string(),
                format!("/apis/logweir.dev/v1alpha1/namespaces/{NS}/approvals/a1/status")
            ),
        ],
        "a missing roster short-circuits: the roster is asked for by its FIXED name, the \
         verdict is patched, and nothing else is called"
    );
    assert!(
        !calls
            .iter()
            .any(|(m, p)| m == "POST" && p.contains("/jobs")),
        "zero POST …/jobs: a refused approval starts nothing"
    );
}

/// **Acceptance.** Exactly one `PATCH …/approvals/<name>/status`, and zero
/// requests to `…/approvals/<name>` without the `/status` suffix.
///
/// Kills the mutant *patch the `Approval`'s `spec`*: the double holds no route
/// for the bare object, so such a patch panics it — and the recorder assertion
/// below names the property even if a future route table is more generous.
#[tokio::test]
async fn approval_reconcile_patches_only_status() {
    let (client, recorder) = mock_client_recording(vec![
        Route {
            method: "GET",
            path_suffix: "/trustrosters/default",
            status: 200,
            body: roster_body(&approver_entry_json(), ""),
        },
        Route {
            method: "GET",
            path_suffix: "/restores/r1",
            status: 200,
            body: restore_body(PLAN_BYTES, PLAN_HASH),
        },
        Route {
            method: "PATCH",
            path_suffix: "/approvals/a1/status",
            status: 200,
            body: patched_approval_body(),
        },
    ]);
    let approval = approval_object(APPROVAL_DOC, &good_sidecar(), SubjectKind::Restore);
    let outcome = approval::reconcile_approval(&approval, &client)
        .await
        .expect("the reconcile completes");
    match &outcome {
        ApprovalOutcome::Verified(v) => {
            assert_eq!(v.matched_key_id, APPROVER_KEY_ID);
            assert_eq!(v.approver, "ops@example.com");
            assert_eq!(v.ticket, "CHG-4711");
            assert!(!v.self_attested_risk);
        }
        other => panic!("the happy path must verify; got {other:?}"),
    }

    let status = approval::status_for(&approval, &outcome, now());
    assert_eq!(status.verified, Some(true));
    assert_eq!(status.matched_key_id.as_deref(), Some(APPROVER_KEY_ID));
    assert_eq!(status.approver.as_deref(), Some("ops@example.com"));
    assert_eq!(status.ticket.as_deref(), Some("CHG-4711"));
    assert_eq!(status.self_attested_risk, Some(false));
    let subject = status
        .verified_subject_ref
        .as_ref()
        .expect("successful verification records exact subject provenance");
    assert_eq!(subject.name, "r1");
    assert_eq!(subject.namespace, NS);
    assert_eq!(subject.uid, "restore-uid-1");

    let calls = seen(&recorder);
    let patches: Vec<_> = calls.iter().filter(|(m, _)| m == "PATCH").collect();
    assert_eq!(
        patches.len(),
        1,
        "exactly one PATCH per reconcile; got {patches:?}"
    );
    assert!(
        patches[0].1.ends_with("/approvals/a1/status"),
        "the one PATCH must be to /status; got {}",
        patches[0].1
    );
    let bare = format!("/namespaces/{NS}/approvals/a1");
    assert!(
        !calls.iter().any(|(_, p)| p.ends_with(&bare)),
        "zero requests to the Approval without the /status suffix — spec is CEL-sealed and a \
         controller that patched it would be widening an approval. Got {calls:?}"
    );
}

#[tokio::test]
async fn a_recreated_subject_uid_does_not_rebind_an_existing_approval() {
    let (client, _recorder) = mock_client_recording(vec![
        Route {
            method: "GET",
            path_suffix: "/trustrosters/default",
            status: 200,
            body: roster_body(&approver_entry_json(), ""),
        },
        Route {
            method: "GET",
            path_suffix: "/restores/r1",
            status: 200,
            body: restore_body(PLAN_BYTES, PLAN_HASH),
        },
        Route {
            method: "GET",
            path_suffix: "/trustrosters/default",
            status: 200,
            body: roster_body(&approver_entry_json(), ""),
        },
        Route {
            method: "GET",
            path_suffix: "/restores/r1",
            status: 200,
            body: restore_body(PLAN_BYTES, PLAN_HASH),
        },
    ]);
    let mut approval = approval_object(APPROVAL_DOC, &good_sidecar(), SubjectKind::Restore);
    approval.status = Some(ApprovalStatus {
        verified: Some(true),
        matched_key_id: Some(APPROVER_KEY_ID.to_string()),
        approver: Some("ops@example.com".to_string()),
        ticket: Some("CHG-4711".to_string()),
        self_attested_risk: Some(false),
        verified_subject_ref: Some(VerifiedSubjectRef {
            api_version: "logweir.dev/v1alpha1".to_string(),
            kind: SubjectKind::Restore,
            name: "r1".to_string(),
            namespace: NS.to_string(),
            uid: "deleted-restore-uid".to_string(),
        }),
        conditions: None,
    });

    let outcome = approval::decide(&approval, &client).await.unwrap();
    assert!(matches!(
        outcome,
        ApprovalOutcome::Referent(ReferentProblem::ReferentUidChanged {
            ref verified_uid,
            ref current_uid,
            ..
        }) if verified_uid == "deleted-restore-uid" && current_uid == "restore-uid-1"
    ));
    let status = approval::status_for(&approval, &outcome, now());
    assert_eq!(status.verified, Some(false));
    assert_eq!(
        status
            .verified_subject_ref
            .as_ref()
            .expect("the old identity remains a permanent replay fence")
            .uid,
        "deleted-restore-uid"
    );

    approval.status = Some(status);
    let second = approval::decide(&approval, &client).await.unwrap();
    assert!(
        matches!(
            second,
            ApprovalOutcome::Referent(ReferentProblem::ReferentUidChanged { .. })
        ),
        "a second reconcile must not erase the refusal and rebind the Approval: {second:?}"
    );
}

/// **Acceptance.** A refused `Approval` is not deleted.
///
/// Kills the mutant *delete a refused one*. A refused approval STAYS in the
/// cluster: it is the audit trail of a rejected attempt, and an operator
/// answering "who tried to approve this, and why was it turned down?" needs
/// the object, not a controller log line that has rotated away.
#[tokio::test]
async fn a_refused_approval_is_not_deleted() {
    let (client, recorder) = mock_client_recording(vec![
        Route {
            method: "GET",
            path_suffix: "/trustrosters/default",
            status: 200,
            body: roster_body(&approver_entry_json(), ""),
        },
        Route {
            method: "GET",
            path_suffix: "/restores/r1",
            status: 200,
            body: restore_body(PLAN_BYTES, PLAN_HASH),
        },
        Route {
            method: "PATCH",
            path_suffix: "/approvals/a1/status",
            status: 200,
            body: patched_approval_body(),
        },
    ]);
    // An unrostered signer: a refusal that reaches every step of the flow.
    let approval = approval_object(
        APPROVAL_DOC,
        &sidecar(PAYLOAD_TYPE_APPROVAL, &[(OUTSIDER_KEY_ID, OUTSIDER_SIG)]),
        SubjectKind::Restore,
    );
    let outcome = approval::reconcile_approval(&approval, &client)
        .await
        .expect("the reconcile completes");
    assert_eq!(outcome.reason(), "KeyIdNotInRoster");

    let calls = seen(&recorder);
    assert!(
        !calls.iter().any(|(m, _)| m == "DELETE"),
        "zero DELETE requests in the recorded route table; got {calls:?}"
    );
    // And the refusal reports no approver and no key id: a name lifted out of
    // bytes whose signature nobody authorised is an attacker-controlled string
    // on a field the UI renders.
    let status = approval::status_for(&approval, &outcome, now());
    assert_eq!(status.verified, Some(false));
    assert_eq!(status.matched_key_id, None);
    assert_eq!(status.approver, None);
}

/// An `Approval` whose referent does not exist is a named referent problem,
/// not a signature verdict.
#[tokio::test]
async fn an_approval_whose_referent_is_absent_says_so() {
    let (client, _recorder) = mock_client_recording(vec![
        Route {
            method: "GET",
            path_suffix: "/trustrosters/default",
            status: 200,
            body: roster_body(&approver_entry_json(), ""),
        },
        Route {
            method: "GET",
            path_suffix: "/restores/r1",
            status: 404,
            body: not_found_body("restores.logweir.dev \\\"r1\\\" not found"),
        },
        Route {
            method: "PATCH",
            path_suffix: "/approvals/a1/status",
            status: 200,
            body: patched_approval_body(),
        },
    ]);
    let approval = approval_object(APPROVAL_DOC, &good_sidecar(), SubjectKind::Restore);
    let outcome = approval::reconcile_approval(&approval, &client)
        .await
        .expect("the reconcile completes");
    assert_eq!(
        outcome,
        ApprovalOutcome::Referent(ReferentProblem::ReferentNotFound {
            kind: "Restore".to_string(),
            name: "r1".to_string(),
        })
    );
    assert_eq!(outcome.reason(), "ReferentNotFound");
}

/// In tag 1 a `Backup` referent carries no `spec.planBytes`, and the refusal
/// says which field is missing.
///
/// NOT A DEGRADED CHECK 7. `Backup.spec` has no `planBytes`
/// (`crates/weirkeeper/src/crds/backup.rs`) and check 7 recomputes the hash
/// from the referent's own bytes, so there is nothing to hash. Refusing with a
/// message that names the field is the alternative to hashing something nobody
/// signed. `Backup` stays in `SubjectKind` because check 8 must be able to
/// tell the two kinds apart.
#[tokio::test]
async fn a_backup_referent_carries_no_plan_bytes_in_tag_one() {
    let (client, _recorder) = mock_client_recording(vec![
        Route {
            method: "GET",
            path_suffix: "/trustrosters/default",
            status: 200,
            body: roster_body(&approver_entry_json(), ""),
        },
        Route {
            method: "GET",
            path_suffix: "/backups/b1",
            status: 200,
            body: backup_body(),
        },
        Route {
            method: "PATCH",
            path_suffix: "/approvals/a1/status",
            status: 200,
            body: patched_approval_body(),
        },
    ]);
    let approval = approval_object(APPROVAL_DOC, &good_sidecar(), SubjectKind::Backup);
    let outcome = approval::reconcile_approval(&approval, &client)
        .await
        .expect("the reconcile completes");
    assert_eq!(
        outcome,
        ApprovalOutcome::Referent(ReferentProblem::ReferentHasNoPlanBytes {
            kind: "Backup".to_string(),
            name: "b1".to_string(),
        })
    );
    assert!(
        outcome.message().contains("spec.planBytes"),
        "the message must name the missing field; got {}",
        outcome.message()
    );
}

// ---------------------------------------------------------------------------
// The TrustRoster reconciler
// ---------------------------------------------------------------------------

/// **Acceptance.** `status.expiredKeyIds` lists expired entries from
/// **both** lists.
///
/// Kills a walk over `approverKeys` alone, which would look complete: the
/// roster carries key material for both lists (interface **I17**), and a
/// runner's lapsed signing key is exactly as invisible to an operator as a
/// lapsed approver key.
#[test]
fn the_roster_reconciler_expires_keys_from_both_lists() {
    let past = now() - Duration::seconds(1);
    let future = now() + Duration::days(30);
    let spec = roster_spec(
        vec![
            key(APPROVER_KEY_ID, APPROVER_PEM, Some(past)),
            key(OUTSIDER_KEY_ID, OUTSIDER_PEM, Some(future)),
        ],
        vec![key(OUTSIDER_KEY_ID, OUTSIDER_PEM, Some(past))],
    );
    let verdict = trust_roster::evaluate(&spec, now());
    assert!(verdict.loaded, "every PEM here parses");
    assert_eq!(
        verdict.expired_key_ids,
        vec![APPROVER_KEY_ID.to_string(), OUTSIDER_KEY_ID.to_string()],
        "both the expired approverKeys entry and the expired signingKeys entry must appear, in \
         approverKeys-then-signingKeys order; the unexpired entry must not"
    );
    assert_eq!(verdict.reason, "Loaded");
}

/// A roster with an unparseable PEM is `Loaded=False`, naming the `keyId`.
#[test]
fn a_roster_with_an_unparseable_pem_is_not_loaded_and_names_the_key_id() {
    let spec = roster_spec(
        vec![approver()],
        vec![key(BAD_KEY_ID, UNPARSEABLE_PEM, None)],
    );
    let verdict = trust_roster::evaluate(&spec, now());
    assert!(!verdict.loaded);
    assert_eq!(verdict.reason, "UnparseableKey");
    assert!(
        verdict.message.contains(BAD_KEY_ID) && verdict.message.contains("signingKeys"),
        "the message must name the keyId and which list it came from; got {}",
        verdict.message
    );
    // And the same rule from the other side: `evaluate` refuses every approval
    // against a roster with one bad entry, which is what makes `Loaded=False`
    // more than a cosmetic column.
    assert!(matches!(
        evaluate(
            APPROVAL_DOC.as_bytes(),
            good_sidecar().as_bytes(),
            &roster_spec(
                vec![approver(), key(BAD_KEY_ID, UNPARSEABLE_PEM, None)],
                vec![]
            ),
            now(),
            "Restore",
            PLAN_BYTES.as_bytes(),
        ),
        Err(ApprovalRefusal::SignatureInvalid(_))
    ));
}

/// The roster reconciler patches only `/status`, on the cluster-scoped path.
#[tokio::test]
async fn the_roster_reconciler_patches_only_status_at_cluster_scope() {
    let roster: TrustRoster = serde_json::from_str(&roster_body(&approver_entry_json(), ""))
        .expect("the fixture is a TrustRoster");
    let (client, recorder) = mock_client_recording(vec![Route {
        method: "PATCH",
        path_suffix: "/trustrosters/default/status",
        status: 200,
        body: roster_body(&approver_entry_json(), ""),
    }]);
    let verdict = trust_roster::reconcile_roster(&roster, &client)
        .await
        .expect("the reconcile completes");
    assert!(verdict.loaded);
    assert_eq!(
        seen(&recorder),
        vec![(
            "PATCH".to_string(),
            format!("/apis/logweir.dev/v1alpha1/trustrosters/{ROSTER_NAME}/status")
        )],
        "cluster-scoped, no namespace segment, and /status only"
    );
}

// ===========================================================================
// TASK 16's REVIEW CARRIES, ROUTED TO TASK 20 AND WRITTEN HERE
// ===========================================================================
//
// Both live BESIDE Task 16's tests rather than in
// `tests/restore_controller.rs`, and the reason is the key material: the
// `notAfter` boundary is reached only AFTER check 5's crypto verifies, so a
// test of it needs a real signature by a real key over the real document —
// `APPROVER_PEM`, `APPROVER_SIG` and `APPROVAL_DOC` are here and are the only
// place in this crate they exist. Copying them into a second test file to keep
// the tests beside their consumer would be two fixtures for one signature,
// which is how one of them comes to be regenerated and the other not.
// `load_roster`'s three states are here for the same reason: they are the
// function's own contract, not the `Restore` reconciler's use of it.

/// **Task 16 review carry, routed to Task 20.** `load_roster`'s THREE states,
/// each asserted, with the transport error kept distinguishable from the 404.
///
/// # Why three and not two
///
/// `RosterLoad` exists because a 404 and a connection reset must not become
/// the same thing. A 404 is a VERDICT — the install skipped step 1 — and lands
/// on `status.conditions` as `RosterNotFound`; a transport error is not a
/// verdict about anybody's approval and must REQUEUE instead of stamping a
/// refusal onto an object that may well be fine. A `Result<TrustRoster,
/// ApprovalRefusal>` shape could not express the difference, and every
/// consumer in chain O — `approval::decide` and `restore`'s argv builder —
/// routes on it.
#[tokio::test]
async fn load_roster_has_three_states_and_a_transport_error_is_not_a_verdict() {
    // ---- FOUND ---------------------------------------------------------
    let (client, recorder) = mock_client_recording(vec![Route {
        method: "GET",
        path_suffix: "/trustrosters/default",
        status: 200,
        body: roster_body(&approver_entry_json(), ""),
    }]);
    match approval::load_roster(&client)
        .await
        .expect("a 200 is not an error")
    {
        approval::RosterLoad::Found(roster) => {
            assert_eq!(roster.spec.approver_keys.len(), 1);
            assert_eq!(roster.spec.approver_keys[0].key_id, APPROVER_KEY_ID);
        }
        approval::RosterLoad::NotFound => panic!("a 200 is Found"),
    }
    assert_eq!(
        seen(&recorder),
        vec![(
            "GET".to_string(),
            format!("/apis/logweir.dev/v1alpha1/trustrosters/{ROSTER_NAME}")
        )],
        "CLUSTER-SCOPED, and by the ONE name: `trustrosters/default`, with no namespace segment. \
         A roster whose name the subject supplies is a roster the subject can choose"
    );

    // ---- NOT FOUND: a verdict, not an error ----------------------------
    let (client, _rec) = mock_client_recording(vec![Route {
        method: "GET",
        path_suffix: "/trustrosters/default",
        status: 404,
        body: not_found_body("trustrosters.logweir.dev \\\"default\\\" not found"),
    }]);
    assert!(
        matches!(
            approval::load_roster(&client)
                .await
                .expect("a 404 is a VERDICT and never an Err — the install skipped step 1"),
            approval::RosterLoad::NotFound
        ),
        "a 404 is NotFound"
    );

    // ---- A TRANSPORT / SERVER ERROR: an Err, so the caller requeues -----
    for status in [500u16, 503, 403] {
        let (client, _rec) = mock_client_recording(vec![Route {
            method: "GET",
            path_suffix: "/trustrosters/default",
            status,
            body: format!(
                r#"{{"kind":"Status","apiVersion":"v1","status":"Failure",
                     "message":"the API server is unavailable","code":{status}}}"#
            ),
        }]);
        let err = approval::load_roster(&client).await.expect_err(
            "anything that is not a 404 must be an Err, so the caller REQUEUES rather than \
             stamping RosterNotFound onto an approval that may well be fine",
        );
        // The error is the API server's, verbatim, so a caller can log which.
        assert!(
            format!("{err}").contains(&status.to_string())
                || format!("{err:?}").contains(&status.to_string()),
            "the error names the status the API server returned: {err}"
        );
    }

    // ---- AND `decide` ROUTES THE THREE APART ---------------------------
    // A 404 reaches the object as a REFUSAL and the referent is never fetched.
    let approval = approval_object(APPROVAL_DOC, &good_sidecar(), SubjectKind::Restore);
    let (client, recorder) = mock_client_recording(vec![Route {
        method: "GET",
        path_suffix: "/trustrosters/default",
        status: 404,
        body: not_found_body("not found"),
    }]);
    assert_eq!(
        approval::decide(&approval, &client)
            .await
            .expect("a missing roster is a verdict"),
        ApprovalOutcome::Refused(ApprovalRefusal::RosterNotFound)
    );
    assert_eq!(
        seen(&recorder).len(),
        1,
        "…and the referent is NEVER fetched: without a roster there is no set of keys any \
         signature could be checked against, so fetching it would be work performed to reach a \
         conclusion already known. Saw: {:?}",
        seen(&recorder)
    );
    // A transport error reaches `decide` as an Err — nothing is written.
    let (client, _rec) = mock_client_recording(vec![Route {
        method: "GET",
        path_suffix: "/trustrosters/default",
        status: 500,
        body: r#"{"kind":"Status","apiVersion":"v1","status":"Failure","code":500}"#.to_string(),
    }]);
    assert!(
        approval::decide(&approval, &client).await.is_err(),
        "a transport error is not a verdict, so `decide` returns an Err and the reconcile writes \
         nothing at all"
    );
}

/// **Task 16 review carry, routed to Task 20.** A key expiring EXACTLY at
/// `now` does not authorise anything.
///
/// # The boundary, and why this arm was owed
///
/// Check 6 is `if *not_after <= now`, so the interval a key authorises over is
/// **closed at the start and OPEN at the end**: `notAfter` is the first
/// instant the key is dead, not the last instant it is alive.
/// `approval_refuses_an_expired_key` asserts one second either side of the
/// boundary and therefore passes under BOTH comparisons — `<=` and `<` — which
/// leaves the boundary itself untested and the `<` mutant alive. This arm is
/// the boundary: with `notAfter == now` the verdict must be `KeyIdExpired`.
///
/// KILLS: `*not_after < now` in check 6.
///
/// A KEY THAT EXPIRES AT MIDNIGHT IS NOT VALID AT MIDNIGHT. That is the
/// reading an operator writing `notAfter: 2027-01-01T00:00:00Z` expects, and
/// it is the safe direction: a one-instant window in which a retired key still
/// authorises a restore is a window nobody can reason about.
#[test]
fn a_key_expiring_exactly_at_now_authorises_nothing() {
    let boundary = now();
    let at_boundary = roster_spec(
        vec![key(APPROVER_KEY_ID, APPROVER_PEM, Some(boundary))],
        vec![],
    );
    assert_eq!(
        evaluate(
            APPROVAL_DOC.as_bytes(),
            good_sidecar().as_bytes(),
            &at_boundary,
            boundary,
            "Restore",
            PLAN_BYTES.as_bytes(),
        ),
        Err(ApprovalRefusal::KeyIdExpired {
            key_id: APPROVER_KEY_ID.to_string(),
            not_after: boundary.to_rfc3339(),
        }),
        "`notAfter` is the first instant the key is DEAD, not the last instant it is alive: \
         check 6 is `<=`, and a `<` would leave a one-instant window in which a retired key \
         still authorises a restore"
    );

    // ONE MILLISECOND LATER STILL REFUSES, and one millisecond EARLIER
    // verifies — so the boundary is a single instant and not a rounding.
    let just_after = roster_spec(
        vec![key(
            APPROVER_KEY_ID,
            APPROVER_PEM,
            Some(boundary - Duration::milliseconds(1)),
        )],
        vec![],
    );
    assert!(matches!(
        evaluate(
            APPROVAL_DOC.as_bytes(),
            good_sidecar().as_bytes(),
            &just_after,
            boundary,
            "Restore",
            PLAN_BYTES.as_bytes(),
        ),
        Err(ApprovalRefusal::KeyIdExpired { .. })
    ));
    let just_before = roster_spec(
        vec![key(
            APPROVER_KEY_ID,
            APPROVER_PEM,
            Some(boundary + Duration::milliseconds(1)),
        )],
        vec![],
    );
    assert!(
        evaluate(
            APPROVAL_DOC.as_bytes(),
            good_sidecar().as_bytes(),
            &just_before,
            boundary,
            "Restore",
            PLAN_BYTES.as_bytes(),
        )
        .is_ok(),
        "one millisecond of remaining life is still life"
    );

    // AND AN ABSENT `notAfter` IS NOT AN EXPIRY. A roster entry with no
    // `notAfter` never expires, which is what `Option` means here — and a
    // check that treated `None` as "expired at the epoch" would refuse every
    // approval on a roster written without the field.
    let never = roster_spec(vec![key(APPROVER_KEY_ID, APPROVER_PEM, None)], vec![]);
    assert!(
        evaluate(
            APPROVAL_DOC.as_bytes(),
            good_sidecar().as_bytes(),
            &never,
            boundary,
            "Restore",
            PLAN_BYTES.as_bytes(),
        )
        .is_ok(),
        "an absent notAfter is `no expiry`, never `expired`"
    );
}

// ===========================================================================
// TASK 16b — THE STEADY-OBJECT ROWS, plan erratum E11(d)
// ===========================================================================
//
// Task 15c's review measured these two reconcilers live on a cluster nobody
// was touching: `trust_roster` 12,107 reconciles and 12,270 own
// `resourceVersion` bumps in 91.2 s, `approval` 7,114 in 90.4 s. Neither owns
// a child object. The engine of both was `last_transition_time: Some(now)`
// written unconditionally in `status_for`: the patch changed, the object's own
// watch fired, and the next pass wrote another changing patch.
//
// The rows below are route-table counts and not byte comparisons, because the
// property is "NO REQUEST WAS MADE". Each second object is the first pass's own
// patch applied to the first object exactly as the API server would apply it
// (`conditions::apply_merge_patch`, RFC 7386), so no arm can pass by asserting
// over a status the reconciler would never have produced.

/// Every `/status` `PATCH` the double was asked for, as its `status` object.
fn status_patches(bodies: &[SeenBody]) -> Vec<serde_json::Value> {
    bodies
        .iter()
        .filter(|b| {
            b.method == "PATCH"
                && b.uri
                    .split('?')
                    .next()
                    .unwrap_or(&b.uri)
                    .ends_with("/status")
        })
        .map(|b| {
            serde_json::from_str::<serde_json::Value>(&b.body).expect("a status patch is JSON")
                ["status"]
                .clone()
        })
        .collect()
}

/// `lastTransitionTime` off the single condition of a patched status.
fn transition_time(status: &serde_json::Value) -> String {
    status["conditions"][0]["lastTransitionTime"]
        .as_str()
        .expect("the condition carries a lastTransitionTime")
        .to_string()
}

/// The roster's `Loaded` condition, as the API server would hold it after
/// `patch`.
fn roster_carrying(patch: &serde_json::Value, spec_json: &str) -> TrustRoster {
    let mut roster: TrustRoster =
        serde_json::from_str(spec_json).expect("the fixture is a TrustRoster");
    let mut stored = serde_json::Value::Null;
    apply_merge_patch(&mut stored, patch);
    roster.status = Some(
        serde_json::from_value::<TrustRosterStatus>(stored)
            .expect("the patched status is a TrustRosterStatus"),
    );
    roster
}

/// The one route a `TrustRoster` reconcile can take.
fn roster_patch_route() -> Vec<Route> {
    vec![Route {
        method: "PATCH",
        path_suffix: "/trustrosters/default/status",
        status: 200,
        body: roster_body(&approver_entry_json(), ""),
    }]
}

/// The good approver key with a `notAfter`, as a JSON `KeyEntry`.
fn approver_entry_json_expiring(not_after: &str) -> String {
    let pem = APPROVER_PEM.replace('\n', "\\n");
    format!(r#"{{"keyId":"{APPROVER_KEY_ID}","spkiPem":"{pem}","notAfter":"{not_after}"}}"#)
}

/// **Task 16b.** A steady `TrustRoster` is patched ONCE and then never again —
/// finding **H-1**, the worst measured rate in the family.
#[tokio::test]
async fn a_steady_trust_roster_issues_no_second_status_patch() {
    let body = roster_body(&approver_entry_json(), "");
    let roster: TrustRoster = serde_json::from_str(&body).expect("the fixture is a TrustRoster");

    let (client, _calls, bodies) = mock_client_recording_bodies(roster_patch_route());
    trust_roster::reconcile_roster(&roster, &client)
        .await
        .expect("the first reconcile completes");
    let first = bodies.lock().expect("the recorder is readable").clone();
    let patches = status_patches(&first);
    assert_eq!(
        patches.len(),
        1,
        "the first pass writes the verdict: {first:?}"
    );

    let steady = roster_carrying(&patches[0], &body);
    let (client, calls) = mock_client_recording(roster_patch_route());
    let verdict = trust_roster::reconcile_roster(&steady, &client)
        .await
        .expect("the second reconcile completes");
    assert!(verdict.loaded, "the verdict is still computed and returned");
    assert_eq!(
        seen(&calls),
        Vec::<(String, String)>::new(),
        "the second pass over an unchanged roster makes NO CALL AT ALL — this reconciler's only \
         call is the patch it no longer sends. Measured live before the fix: 12,107 reconciles \
         and 12,270 resourceVersion bumps in 91.2 s on one steady object"
    );
}

/// **Task 16b.** A roster that really changes is patched exactly once, and the
/// transition time obeys the `metav1.Condition` contract in both directions.
///
/// # Two arms, because "a real state change" is two different things here
///
/// ARM 1 — **a key expires.** `status.expiredKeyIds` and the message change, so
/// exactly one patch is sent; but the condition's `status` is still `True` and
/// its `reason` is still `Loaded`, so `lastTransitionTime` is KEPT. That is the
/// contract, not an omission: the field names when the CONDITION last changed,
/// and this condition did not. Moving it here is the same lie the hot loop was
/// telling, once a day instead of 133 times a second.
///
/// ARM 2 — **a key stops parsing.** `Loaded=True/Loaded` becomes
/// `Loaded=False/UnparseableKey`: a transition, and the timestamp moves.
#[tokio::test]
async fn a_roster_that_changes_is_patched_once_with_the_right_transition_time() {
    let body = roster_body(&approver_entry_json(), "");
    let roster: TrustRoster = serde_json::from_str(&body).expect("the fixture is a TrustRoster");
    let (client, _calls, bodies) = mock_client_recording_bodies(roster_patch_route());
    trust_roster::reconcile_roster(&roster, &client)
        .await
        .expect("the first reconcile completes");
    let first = status_patches(&bodies.lock().expect("readable")).remove(0);
    let stamped = transition_time(&first);

    // ---- ARM 1: the key expires ------------------------------------------
    let expired_body = roster_body(&approver_entry_json_expiring("2020-01-01T00:00:00Z"), "");
    let expired = roster_carrying(&first, &expired_body);
    let (client, _calls, bodies) = mock_client_recording_bodies(roster_patch_route());
    let verdict = trust_roster::reconcile_roster(&expired, &client)
        .await
        .expect("the reconcile completes");
    assert_eq!(
        verdict.expired_key_ids,
        vec![APPROVER_KEY_ID.to_string()],
        "the key is past its notAfter"
    );
    let after = status_patches(&bodies.lock().expect("readable"));
    assert_eq!(
        after.len(),
        1,
        "a real change is written exactly once: {after:?}"
    );
    assert_eq!(
        after[0]["expiredKeyIds"],
        serde_json::json!([APPROVER_KEY_ID]),
        "and the change is the expiry: {}",
        after[0]
    );
    assert_eq!(
        transition_time(&after[0]),
        stamped,
        "the CONDITION did not transition — still Loaded=True/Loaded — so its timestamp is the \
         one it already carried. `metav1.Condition`'s contract, and ruling 1's own rule"
    );

    // ---- ARM 2: the key stops parsing -------------------------------------
    let broken_pem = UNPARSEABLE_PEM.replace('\n', "\\n");
    let broken_body = roster_body(
        &format!(r#"{{"keyId":"{BAD_KEY_ID}","spkiPem":"{broken_pem}"}}"#),
        "",
    );
    let broken = roster_carrying(&first, &broken_body);
    let (client, _calls, bodies) = mock_client_recording_bodies(roster_patch_route());
    let verdict = trust_roster::reconcile_roster(&broken, &client)
        .await
        .expect("the reconcile completes");
    assert!(!verdict.loaded, "an unparseable entry is Loaded=False");
    let after = status_patches(&bodies.lock().expect("readable"));
    assert_eq!(after.len(), 1, "written exactly once: {after:?}");
    assert_ne!(
        transition_time(&after[0]),
        stamped,
        "Loaded=True/Loaded became Loaded=False/UnparseableKey, which IS a transition, so the \
         timestamp moves. A comparison that never moved the field would be as wrong as one that \
         always did"
    );
}

/// The three routes an `Approval` reconcile takes when its referent exists.
fn approval_routes(referent: (u16, String)) -> Vec<Route> {
    vec![
        Route {
            method: "GET",
            path_suffix: "/trustrosters/default",
            status: 200,
            body: roster_body(&approver_entry_json(), ""),
        },
        Route {
            method: "GET",
            path_suffix: "/restores/r1",
            status: referent.0,
            body: referent.1,
        },
        Route {
            method: "PATCH",
            path_suffix: "/approvals/a1/status",
            status: 200,
            body: patched_approval_body(),
        },
    ]
}

/// **Task 16b.** A steady `Approval` is patched ONCE and then never again —
/// finding **H-2**.
#[tokio::test]
async fn a_steady_approval_issues_no_second_status_patch() {
    let approval = approval_object(APPROVAL_DOC, &good_sidecar(), SubjectKind::Restore);
    let (client, _calls, bodies) =
        mock_client_recording_bodies(approval_routes((200, restore_body(PLAN_BYTES, PLAN_HASH))));
    approval::reconcile_approval(&approval, &client)
        .await
        .expect("the first reconcile completes");
    let first = status_patches(&bodies.lock().expect("readable"));
    assert_eq!(first.len(), 1, "the first pass writes the verdict");

    let mut stored = serde_json::Value::Null;
    apply_merge_patch(&mut stored, &first[0]);
    let mut steady = approval_object(APPROVAL_DOC, &good_sidecar(), SubjectKind::Restore);
    steady.status = Some(
        serde_json::from_value::<ApprovalStatus>(stored)
            .expect("the patched status is an ApprovalStatus"),
    );

    let (client, calls) =
        mock_client_recording(approval_routes((200, restore_body(PLAN_BYTES, PLAN_HASH))));
    let outcome = approval::reconcile_approval(&steady, &client)
        .await
        .expect("the second reconcile completes");
    assert!(outcome.is_verified(), "the verdict is still computed");
    assert_eq!(
        seen(&calls),
        vec![
            (
                "GET".to_string(),
                format!("/apis/logweir.dev/v1alpha1/trustrosters/{ROSTER_NAME}")
            ),
            (
                "GET".to_string(),
                format!("/apis/logweir.dev/v1alpha1/namespaces/{NS}/restores/r1")
            ),
        ],
        "the second pass STILL READS the roster and the referent — the verdict is recomputed \
         from scratch every time, which is the whole point of a controller — and writes NOTHING. \
         Measured live before the fix: 7,114 reconciles in 90.4 s on one steady Approval"
    );
}

/// **Task 16b.** An `Approval` whose REFERENT changes is patched exactly once,
/// with a new transition time.
///
/// The referent is the only thing that can change here: `Approval.spec` is
/// sealed by an object-level CEL rule (`spec is immutable; create a new object
/// instead`), so a `Verified=False` approval becomes `Verified=True` when the
/// `Restore` it names appears — and never by an edit to itself.
#[tokio::test]
async fn an_approval_whose_referent_appears_is_patched_once_with_a_new_transition_time() {
    let approval = approval_object(APPROVAL_DOC, &good_sidecar(), SubjectKind::Restore);
    let (client, _calls, bodies) = mock_client_recording_bodies(approval_routes((
        404,
        not_found_body("restores.logweir.dev \"r1\" not found"),
    )));
    let outcome = approval::reconcile_approval(&approval, &client)
        .await
        .expect("the first reconcile completes");
    assert!(
        !outcome.is_verified(),
        "no referent, no verdict: {outcome:?}"
    );
    let refused = status_patches(&bodies.lock().expect("readable")).remove(0);
    let stamped = transition_time(&refused);
    assert_eq!(refused["verified"], serde_json::json!(false));

    let mut stored = serde_json::Value::Null;
    apply_merge_patch(&mut stored, &refused);
    let mut carrying = approval_object(APPROVAL_DOC, &good_sidecar(), SubjectKind::Restore);
    carrying.status = Some(
        serde_json::from_value::<ApprovalStatus>(stored)
            .expect("the patched status is an ApprovalStatus"),
    );

    // The referent is created. NOTHING about the Approval changed.
    let (client, _calls, bodies) =
        mock_client_recording_bodies(approval_routes((200, restore_body(PLAN_BYTES, PLAN_HASH))));
    let outcome = approval::reconcile_approval(&carrying, &client)
        .await
        .expect("the second reconcile completes");
    assert!(
        outcome.is_verified(),
        "the referent is there now: {outcome:?}"
    );
    let after = status_patches(&bodies.lock().expect("readable"));
    assert_eq!(after.len(), 1, "written exactly once: {after:?}");
    assert_eq!(after[0]["verified"], serde_json::json!(true));
    assert_ne!(
        transition_time(&after[0]),
        stamped,
        "Verified=False/ReferentNotFound became Verified=True/Verified, which IS a transition"
    );
}
