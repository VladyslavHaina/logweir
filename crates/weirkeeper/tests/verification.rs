//! **Task 24.** `verify_evidence`, the two green-badge rules, the second
//! status patch, and the Phase B demo's own shape.
//!
//! # No socket, no cluster, no daemon
//!
//! `Store::read_only_from_url(` over a **filesystem** URL — a scratch
//! directory under `std::env::temp_dir()` holding the checked-in
//! `e2e/fixtures/signed/` documents — and one `kube` test over a `tower`
//! closure. STANDING RULE 18 requires the allow-list entry that names this
//! file and that reason, and
//! `crates/logweir/tests/no_network_in_unit_tests.rs` carries it.
//!
//! `tempfile` IS NOT ADDED TO THIS CRATE'S MANIFEST — Global Constraint 38
//! closes the workspace graph, and `std::env::temp_dir()` plus a removal guard
//! is the same test. The same decision `tests/retention.rs` records.
//!
//! # The fixtures are REAL signed documents, and that is the point
//!
//! `e2e/fixtures/signed/scorecard.json` + `.sig` and `backup-receipt.json` +
//! `.sig` were signed by the stage-1 throwaway key whose PUBLIC half is
//! `e2e/fixtures/signed/public.pem` (plan erratum **E10(e)**). A verification
//! test over a hand-built sidecar would assert the shape of a struct; these
//! assert that `logweir-verify` said yes about bytes a `logweir` binary
//! actually signed, which is what makes `an_empty_signing_key_list_names_itself`'s
//! second arm the proof that Phase B's exit criterion is REACHABLE.
//!
//! **No private key is read by anything here.** `signing.pem` sits beside
//! those files and this test never opens it.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use chrono::{TimeZone as _, Utc};
use http::{Request, Response};
use http_body_util::BodyExt as _;
use kube::client::Body;
use serde_json::{json, Value};
use tower::util::service_fn;

use logweir_core::engine::StorageUrl;
use logweir_core::ids::sha256_prefixed;
use logweir_store::Store;
use weirkeeper::conditions::{
    CONDITION_VERIFIED, REASON_EXIT_CODE_NOT_ZERO, REASON_OUTCOME_NOT_PASS,
    REASON_VERIFICATION_INVALID, REASON_VERIFICATION_NOT_ATTEMPTED, REASON_VERIFIED,
};
use weirkeeper::controllers::backup::{
    reconcile_backup, ArchiveObservation, EvidencePresence, RUNNER_SERVICE_ACCOUNT,
};
use weirkeeper::crds::backup::Backup;
use weirkeeper::crds::trust_roster::{KeyEntry, TrustRosterSpec};
use weirkeeper::verification::{
    backup_badge, restore_badge, verify_evidence, EvidenceRef, VerificationResult,
    VerificationVerdict, NO_CREDENTIAL_DETAIL, NO_SIGNING_KEYS_DETAIL, UNVERIFIED,
};

// ===========================================================================
// Paths, fixtures and a scratch evidence tree
// ===========================================================================

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crates/weirkeeper sits two levels under the workspace root")
        .to_path_buf()
}

fn read(rel: &str) -> String {
    let p = repo_root().join(rel);
    std::fs::read_to_string(&p).unwrap_or_else(|e| {
        panic!(
            "{} must be readable for this test to mean anything: {e}",
            p.display()
        )
    })
}

/// The runner's PUBLIC key — the half a roster carries. Never `signing.pem`.
fn runner_public_key() -> String {
    read("e2e/fixtures/signed/public.pem")
}

/// The key id the fixture sidecars name, which is
/// `VerifyingKey::key_id()` over that public key's SPKI DER.
const FIXTURE_KEY_ID: &str = "917cf9a299872cbf8b2715999ce457464705bb8f48df0a07e9b1e19bb9f383fd";

const PAYLOAD_KEY: &str = "logweir/drills/r1.json";
const SIDECAR_KEY: &str = "logweir/drills/r1.sig";

/// A scratch evidence tree, removed when the test ends.
struct EvidenceTree {
    root: PathBuf,
}

impl EvidenceTree {
    /// A tree holding `payload` at [`PAYLOAD_KEY`] and `sidecar` at
    /// [`SIDECAR_KEY`].
    fn new(label: &str, payload: &[u8], sidecar: &[u8]) -> Self {
        let root = std::env::temp_dir().join(format!(
            "logweir-t24-{label}-{}-{}",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let dir = root.join("logweir/drills");
        std::fs::create_dir_all(&dir).expect("a scratch directory is creatable");
        std::fs::write(root.join(PAYLOAD_KEY), payload).expect("the payload is writable");
        std::fs::write(root.join(SIDECAR_KEY), sidecar).expect("the sidecar is writable");
        Self { root }
    }

    /// `read_only_from_url` AND NEVER `from_url`: the archive is never under
    /// `logweir/` in production, and the write-path constructor's
    /// `LOGWEIR_ROOT` guard exists to refuse exactly that (Global Constraint
    /// 6). The read-only handle physically cannot put.
    fn handle(&self) -> Store {
        Store::read_only_from_url(&StorageUrl::Filesystem {
            path: self.root.clone(),
        })
        .expect("a filesystem handle over an existing directory builds")
    }

    fn remove(&self, rel: &str) {
        std::fs::remove_file(self.root.join(rel)).expect("the fixture object is removable");
    }

    /// An object's mode. `0o000` is how this test reaches `StoreError::Io`
    /// rather than `StoreError::NotFound` — see
    /// [`a_storage_failure_is_not_invalid`]'s second arm for why a directory
    /// path does not.
    #[cfg(unix)]
    fn chmod(&self, rel: &str, mode: u32) {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(self.root.join(rel), std::fs::Permissions::from_mode(mode))
            .expect("the fixture object's mode is settable");
    }
}

impl Drop for EvidenceTree {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

/// A roster carrying `keys` as its `signingKeys`.
fn roster(keys: Vec<KeyEntry>) -> TrustRosterSpec {
    TrustRosterSpec {
        approver_keys: Vec::new(),
        signing_keys: keys,
        allowed_cluster_ids: Vec::new(),
    }
}

/// One signing key entry.
fn entry(key_id: &str, spki_pem: &str) -> KeyEntry {
    KeyEntry {
        key_id: key_id.to_string(),
        spki_pem: spki_pem.to_string(),
        subject: Some("logweir-runner@example.invalid".to_string()),
        not_after: None,
    }
}

/// A `status` block shaped the way a UI renders one.
fn renderable(verification: Value, extra: Value) -> Value {
    let mut status = json!({ "evidence": { "verification": verification } });
    for (k, v) in extra.as_object().expect("extra is an object") {
        status[k] = v.clone();
    }
    status
}

/// A `verification` block with `result`, and the two fields a green badge's
/// label is rendered from.
fn verification(result: &str) -> Value {
    json!({
        "result": result,
        "matchedKeyId": FIXTURE_KEY_ID,
        "payloadType": logweir_verify::PAYLOAD_TYPE_SCORECARD,
        "verifiedAt": "2026-09-11T03:20:00Z",
    })
}

// ===========================================================================
// Interface I21 — the two badge rules, ONE PER KIND
// ===========================================================================

/// **Interface I21, the `Backup` half.** Green ⟺ `Valid` AND `exitCode == 0`.
///
/// KILLS THREE MUTANTS:
///  * "render a green `Backup` badge on `Valid` + `exitCode: 2`" — fixture 2;
///  * "return `Invalid` for an absent signing-key list" reaching the badge as
///    `Invalid` on an exit-0 run — fixture 3 vs fixture 4 carry DIFFERENT
///    reasons, so collapsing the two is a failure here;
///  * **"use one badge rule for both kinds by reading `status.outcome` on a
///    `Backup`"** — fixture 1's status carries `exitCode` and no `outcome`,
///    because `BackupStatus` HAS none (spec §3.2, C95), so a rule that read
///    `outcome` would render the green case ungreen.
#[test]
fn a_backup_is_green_only_on_valid_and_exit_code_zero() {
    let green = renderable(verification("Valid"), json!({ "exitCode": 0 }));
    let b = backup_badge(&green);
    assert!(
        b.green,
        "a Backup whose receipt verified and whose runner exited 0 is the ONE green case; got \
         {b:?}"
    );
    assert_eq!(b.reason, REASON_VERIFIED);
    assert_eq!(
        b.label,
        format!("verified by weirkeeper at 2026-09-11T03:20:00Z against key {FIXTURE_KEY_ID}"),
        "the label names the instant and the key — never \"verified in your browser\", which is \
         a claim tag 1's cut WASM verifier cannot support"
    );

    for (status, want_reason, why) in [
        (
            renderable(verification("Valid"), json!({ "exitCode": 2 })),
            REASON_EXIT_CODE_NOT_ZERO,
            "exit 2 is a signed document saying the run did not pass; the signature being good \
             is not the badge",
        ),
        (
            renderable(verification("Invalid"), json!({ "exitCode": 0 })),
            REASON_VERIFICATION_INVALID,
            "a claim about the document",
        ),
        (
            renderable(verification("NotAttempted"), json!({ "exitCode": 0 })),
            REASON_VERIFICATION_NOT_ATTEMPTED,
            "a claim about the controller, and DISTINCT from Invalid",
        ),
    ] {
        let b = backup_badge(&status);
        assert!(!b.green, "{why}: {status} must not be green; got {b:?}");
        assert_eq!(b.reason, want_reason, "{why}; got {b:?}");
        assert_eq!(
            b.label, UNVERIFIED,
            "anything that is not green renders the literal word `unverified`"
        );
    }

    // AND NONE OF THE FOUR EVER RENDERS THE WORD `pass`
    // (`design-operator.md:135-139`). `pass` is a `Restore`'s own outcome
    // value and means something; borrowing it for a badge would make the happy
    // word appear over a document nobody checked.
    for status in [
        renderable(verification("Valid"), json!({ "exitCode": 0 })),
        renderable(verification("Valid"), json!({ "exitCode": 2 })),
        renderable(verification("Invalid"), json!({ "exitCode": 0 })),
        renderable(verification("NotAttempted"), json!({ "exitCode": 0 })),
    ] {
        let b = backup_badge(&status);
        assert!(
            !b.label.contains("pass"),
            "no Backup badge renders the word `pass`; got {b:?}"
        );
    }

    // THE FIELD THAT IS NOT THERE. A `Backup` fixture carrying an `outcome`
    // would still be green on `exitCode: 0` and NOT green on `exitCode: 2`,
    // because the Backup rule never reads it.
    let with_outcome = renderable(
        verification("Valid"),
        json!({ "exitCode": 2, "outcome": "pass" }),
    );
    assert!(
        !backup_badge(&with_outcome).green,
        "the Backup rule reads exitCode and NEVER outcome — a Backup carries none, and a rule \
         that read one would be the Restore rule wearing the wrong kind's name"
    );
}

/// **Interface I21, the `Restore` half.** Green ⟺ `Valid` AND
/// `outcome == pass`.
///
/// KILLS: "render a green `Restore` badge on `Valid` + `fail-integrity`" —
/// a `fail-integrity` run produced a perfectly valid SIGNED document saying
/// the restore did not reconcile, which is the most valuable thing the tool
/// reports and the last thing a green badge should sit on.
#[test]
fn a_restore_is_green_only_on_valid_and_a_pass_outcome() {
    let green = renderable(verification("Valid"), json!({ "outcome": "pass" }));
    let b = restore_badge(&green);
    assert!(b.green, "Valid + pass is the ONE green case; got {b:?}");
    assert_eq!(b.reason, REASON_VERIFIED);
    assert_eq!(
        b.label,
        format!("verified by weirkeeper at 2026-09-11T03:20:00Z against key {FIXTURE_KEY_ID}")
    );

    for (status, want_reason) in [
        (
            renderable(
                verification("Valid"),
                json!({ "outcome": "fail-integrity" }),
            ),
            REASON_OUTCOME_NOT_PASS,
        ),
        (
            renderable(verification("Invalid"), json!({ "outcome": "pass" })),
            REASON_VERIFICATION_INVALID,
        ),
        (
            renderable(verification("NotAttempted"), json!({ "outcome": "pass" })),
            REASON_VERIFICATION_NOT_ATTEMPTED,
        ),
    ] {
        let b = restore_badge(&status);
        assert!(!b.green, "{status} must not be green; got {b:?}");
        assert_eq!(b.reason, want_reason, "got {b:?}");
        assert_eq!(b.label, UNVERIFIED);
        assert!(
            !b.label.contains("pass"),
            "never the word `pass` on a badge; got {b:?}"
        );
    }

    // THE TWO RULES ARE NOT ONE RULE, and this is the pair that says so: the
    // SAME status block is green under one and not under the other.
    let restore_shaped = renderable(verification("Valid"), json!({ "outcome": "pass" }));
    assert!(restore_badge(&restore_shaped).green);
    assert!(
        !backup_badge(&restore_shaped).green,
        "a Restore-shaped status has no exitCode, so the Backup rule refuses it — which is what \
         makes a single shared rule impossible rather than merely inadvisable"
    );
}

// ===========================================================================
// `verify_evidence` — the four steps
// ===========================================================================

/// The roster must carry KEY MATERIAL, and both arms of that are here.
///
/// KILLS: "resolve signing keys from `signingKeyIds` with no key material"
/// (second arm — nothing would ever be `Valid` and Phase B's exit criterion
/// would be unreachable) and "return `Invalid` for an absent signing-key list"
/// (first arm).
#[test]
fn an_empty_signing_key_list_names_itself() {
    let payload = read("e2e/fixtures/signed/scorecard.json");
    let sidecar = read("e2e/fixtures/signed/scorecard.sig");
    let tree = EvidenceTree::new("empty-roster", payload.as_bytes(), sidecar.as_bytes());
    let digest = sha256_prefixed(payload.as_bytes());
    let store = tree.handle();

    // FIRST ARM — no key material at all.
    let r = verify_evidence(
        Some(&store),
        &roster(Vec::new()),
        PAYLOAD_KEY,
        &digest,
        SIDECAR_KEY,
        logweir_verify::PAYLOAD_TYPE_SCORECARD,
    );
    assert_eq!(
        r.result,
        VerificationVerdict::NotAttempted,
        "an empty signingKeys list is NotAttempted, never Invalid: there is nothing wrong with \
         the document, and blaming it would send an operator to read a scorecard when the fix is \
         one line in a cluster-scoped object. Got {r:?}"
    );
    assert_eq!(
        r.detail.as_deref(),
        Some(NO_SIGNING_KEYS_DETAIL),
        "the detail NAMES ITSELF and names the field to edit"
    );
    assert_eq!(r.matched_key_id, None);

    // SECOND ARM — the runner's public key, and the exit criterion is
    // reachable.
    let r = verify_evidence(
        Some(&store),
        &roster(vec![entry(FIXTURE_KEY_ID, &runner_public_key())]),
        PAYLOAD_KEY,
        &digest,
        SIDECAR_KEY,
        logweir_verify::PAYLOAD_TYPE_SCORECARD,
    );
    assert_eq!(
        r.result,
        VerificationVerdict::Valid,
        "a roster carrying the runner's PUBLIC key verifies a document that runner signed — \
         this is the pair that makes `status.evidence.verification.result: Valid` reachable at \
         all, and with a `signingKeyIds: [string]` shape it could not be. Got {r:?}"
    );
    assert_eq!(
        r.matched_key_id.as_deref(),
        Some(FIXTURE_KEY_ID),
        "matchedKeyId is the ROSTER ENTRY's own keyId — the string an operator can grep for in \
         the object they edit"
    );
    assert_eq!(r.detail, None, "a Valid has nothing to explain");
    assert_eq!(r.payload_type, logweir_verify::PAYLOAD_TYPE_SCORECARD);

    // …AND THE ROSTER'S ID IS THE ONE REPORTED, not the sidecar's. The two
    // agree whenever a roster declares its ids correctly; when they do not,
    // the id an operator can act on is the one in the object they wrote.
    let r = verify_evidence(
        Some(&store),
        &roster(vec![entry("nightly-runner-2026", &runner_public_key())]),
        PAYLOAD_KEY,
        &digest,
        SIDECAR_KEY,
        logweir_verify::PAYLOAD_TYPE_SCORECARD,
    );
    assert_eq!(r.result, VerificationVerdict::Valid);
    assert_eq!(r.matched_key_id.as_deref(), Some("nightly-runner-2026"));

    // A ROSTER WHOSE ONLY KEY IS THE WRONG ONE IS `Invalid`, not
    // NotAttempted: key material WAS resolved and it said no.
    let other = read("e2e/fixtures/signed/public.pem").replace("D39", "D40");
    let r = verify_evidence(
        Some(&store),
        &roster(vec![entry("wrong", &other)]),
        PAYLOAD_KEY,
        &digest,
        SIDECAR_KEY,
        logweir_verify::PAYLOAD_TYPE_SCORECARD,
    );
    assert_eq!(
        r.result,
        VerificationVerdict::Invalid,
        "a roster with key material that does not verify is a claim about the document; got {r:?}"
    );
    assert!(
        r.detail.is_some_and(|d| d.contains("wrong")),
        "the detail carries the last error, naming the key it came from"
    );
}

/// A storage failure is NEVER `Invalid` — `Invalid` is a claim about the
/// document, and an object that could not be fetched made no claim.
///
/// KILLS: "map a `StoreError` other than `NotFound` to `Invalid`".
#[test]
fn a_storage_failure_is_not_invalid() {
    let payload = read("e2e/fixtures/signed/scorecard.json");
    let sidecar = read("e2e/fixtures/signed/scorecard.sig");
    let digest = sha256_prefixed(payload.as_bytes());
    let keys = vec![entry(FIXTURE_KEY_ID, &runner_public_key())];

    // FIRST ARM — the payload object is REMOVED: `StoreError::NotFound`.
    let tree = EvidenceTree::new("not-found", payload.as_bytes(), sidecar.as_bytes());
    let store = tree.handle();
    tree.remove(PAYLOAD_KEY);
    let r = verify_evidence(
        Some(&store),
        &roster(keys.clone()),
        PAYLOAD_KEY,
        &digest,
        SIDECAR_KEY,
        logweir_verify::PAYLOAD_TYPE_SCORECARD,
    );
    assert_eq!(
        r.result,
        VerificationVerdict::NotAttempted,
        "an absent object is NotAttempted. A controller that called it Invalid would report \
         tampering for a run whose bucket the controller simply cannot see. Got {r:?}"
    );
    assert!(
        r.detail.as_deref().is_some_and(|d| d.contains(PAYLOAD_KEY)),
        "the detail names the object; got {:?}",
        r.detail
    );

    // SECOND ARM — AN UNREADABLE OBJECT, which is `StoreError::Io` and NOT
    // `NotFound`. This arm is the one that kills "map a StoreError other than
    // NotFound to Invalid", and it took two attempts to write: a key naming a
    // DIRECTORY comes back from `LocalFileSystem` as `NotFound` too, so the
    // first version of this arm exercised the same branch as the first and the
    // mutant SURVIVED at 14 passed / 0 failed. A file whose mode is `000` is
    // the shape that actually reaches the other branch.
    let tree = EvidenceTree::new("unreadable", payload.as_bytes(), sidecar.as_bytes());
    let store = tree.handle();
    tree.chmod(PAYLOAD_KEY, 0o000);
    let r = verify_evidence(
        Some(&store),
        &roster(keys.clone()),
        PAYLOAD_KEY,
        &digest,
        SIDECAR_KEY,
        logweir_verify::PAYLOAD_TYPE_SCORECARD,
    );
    tree.chmod(PAYLOAD_KEY, 0o600);
    assert_eq!(
        r.result,
        VerificationVerdict::NotAttempted,
        "EVERY StoreError is NotAttempted, not only NotFound — a bucket that will not answer has \
         made no claim about the document. Got {r:?}"
    );
    let detail = r
        .detail
        .expect("a NotAttempted from storage carries the error");
    assert!(
        detail.contains("could not be read"),
        "an unreadable object is DISTINGUISHED from an absent one, because they are different \
         things for an operator to act on; got {detail}"
    );

    // THIRD ARM — no credential at all. The documented switch (spec §9): an
    // adopter who declines to give the control plane bucket access runs with
    // verification display OFF and uses the printed CLI command.
    let r = verify_evidence(
        None,
        &roster(keys),
        PAYLOAD_KEY,
        &digest,
        SIDECAR_KEY,
        logweir_verify::PAYLOAD_TYPE_SCORECARD,
    );
    assert_eq!(r.result, VerificationVerdict::NotAttempted);
    assert_eq!(r.detail.as_deref(), Some(NO_CREDENTIAL_DETAIL));
}

/// The digest the status recorded, against the bytes in the bucket — and a
/// mismatch IS `Invalid`.
///
/// KILLS: "skip the digest comparison and verify the signature alone". With
/// the comparison gone this returns `Valid`, because the sidecar in the tree
/// genuinely signs the payload in the tree: the property the digest defends is
/// **substitution of one genuinely-signed document for another**, which no
/// signature check can see.
#[test]
fn a_digest_mismatch_is_invalid() {
    let payload = read("e2e/fixtures/signed/scorecard.json");
    let sidecar = read("e2e/fixtures/signed/scorecard.sig");
    let tree = EvidenceTree::new("digest", payload.as_bytes(), sidecar.as_bytes());
    let store = tree.handle();

    // The recorded digest differs from the fetched bytes by ONE BYTE.
    let mut bytes = payload.clone().into_bytes();
    let last = bytes.len() - 1;
    bytes[last] ^= 0x01;
    let recorded = sha256_prefixed(&bytes);
    let actual = sha256_prefixed(payload.as_bytes());
    assert_ne!(
        recorded, actual,
        "the two digests must differ for this test"
    );

    let r = verify_evidence(
        Some(&store),
        &roster(vec![entry(FIXTURE_KEY_ID, &runner_public_key())]),
        PAYLOAD_KEY,
        &recorded,
        SIDECAR_KEY,
        logweir_verify::PAYLOAD_TYPE_SCORECARD,
    );
    assert_eq!(
        r.result,
        VerificationVerdict::Invalid,
        "the bytes in the bucket are not the bytes the run reported writing. That IS a claim \
         about the document. Got {r:?}"
    );
    let detail = r.detail.expect("an Invalid carries its reason");
    assert!(
        detail.contains(&recorded) && detail.contains(&actual),
        "the detail names BOTH digests, so an operator can tell which one moved; got {detail}"
    );
}

/// A `NotAttempted` and an `Invalid` are different values, and the
/// `VerificationResult` shape is the brief's.
#[test]
fn not_attempted_is_a_verdict_distinct_from_invalid() {
    assert_ne!(
        VerificationVerdict::NotAttempted,
        VerificationVerdict::Invalid
    );
    assert_eq!(VerificationVerdict::Valid.as_str(), "Valid");
    assert_eq!(VerificationVerdict::Invalid.as_str(), "Invalid");
    assert_eq!(VerificationVerdict::NotAttempted.as_str(), "NotAttempted");

    // THE TIMESTAMP RULE (plan erratum E11(d)): a verdict whose SUBSTANCE has
    // not changed keeps the instant it was reached at, so the second status
    // patch is a no-op that is never sent and the reconciler stays quiet.
    let r = VerificationResult {
        result: VerificationVerdict::Valid,
        matched_key_id: Some(FIXTURE_KEY_ID.to_string()),
        payload_type: logweir_verify::PAYLOAD_TYPE_SCORECARD.to_string(),
        verified_at: Utc.with_ymd_and_hms(2026, 9, 11, 9, 0, 0).unwrap(),
        detail: None,
    };
    let first = r.to_status_value(None);
    assert_eq!(first["verifiedAt"], json!("2026-09-11T09:00:00Z"));
    let later = VerificationResult {
        verified_at: Utc.with_ymd_and_hms(2026, 9, 11, 18, 0, 0).unwrap(),
        ..r.clone()
    };
    assert_eq!(
        later.to_status_value(Some(&first))["verifiedAt"],
        json!("2026-09-11T09:00:00Z"),
        "an unchanged verdict keeps its stored instant — otherwise every reconcile is a write, \
         every write is a wake-up, and E11(d)'s 12,000-reconciles-in-90-seconds loop is back"
    );
    let changed = VerificationResult {
        result: VerificationVerdict::Invalid,
        matched_key_id: None,
        detail: Some("x".into()),
        ..later
    };
    assert_eq!(
        changed.to_status_value(Some(&first))["verifiedAt"],
        json!("2026-09-11T18:00:00Z"),
        "a CHANGED verdict takes the new instant: that is when it was reached"
    );
}

// ===========================================================================
// The second patch, over `mock_client`
// ===========================================================================

const NS: &str = "logweir-t24";
const NAME: &str = "logweir-backup-nightly-20261109-031700";
const POD: &str = "logweir-backup-nightly-20261109-031700-abcde";
const UID: &str = "3f1c8a5e-0000-4000-8000-0000000000a1";
const RECEIPT_KEY: &str = "logweir/backups/b1/r1.receipt.json";
const RECEIPT_SIDECAR_KEY: &str = "logweir/backups/b1/r1.receipt.sig";
const RECEIPT_DIGEST: &str =
    "sha256:0000000000000000000000000000000000000000000000000000000000000001";

fn backup_json() -> String {
    let argv = serde_json::to_string(&[
        "backup",
        "run",
        "--spec",
        "/plan/backup.yaml",
        "--signing-key",
        "/signing/key.pem",
        "--receipt-out",
        "/work/receipt.json",
        "--triggered-by",
        "manual",
    ])
    .expect("the argv serialises");
    let argv = serde_json::to_string(&argv).expect("the annotation value is a JSON string");
    format!(
        r#"{{
  "apiVersion": "logweir.dev/v1alpha1", "kind": "Backup",
  "metadata": {{
    "name": "{NAME}", "namespace": "{NS}", "uid": "{UID}", "generation": 3,
    "annotations": {{ "logweir.dev/runner-argv": {argv} }}
  }},
  "spec": {{
    "sourceRef": {{ "name": "prod" }},
    "topics": ["orders"],
    "archive": {{ "url": "s3://kafka-backups/k8s-demo", "secretRef": {{ "name": "logweir-s3" }} }},
    "triggeredBy": "manual",
    "deadlineSeconds": 3600
  }}
}}"#
    )
}

fn backup() -> Backup {
    serde_json::from_str(&backup_json()).expect("the fixture is a Backup")
}

fn job_body() -> String {
    format!(
        r#"{{"apiVersion":"batch/v1","kind":"Job",
  "metadata":{{"name":"{NAME}","namespace":"{NS}","uid":"bbbbbbbb-0000-4000-8000-0000000000b1"}},
  "spec":{{"template":{{"spec":{{"containers":[],"restartPolicy":"Never","serviceAccountName":"{RUNNER_SERVICE_ACCOUNT}"}}}}}},
  "status":{{"conditions":[{{"type":"Complete","status":"True",
     "lastProbeTime":"2026-11-09T03:20:00Z","lastTransitionTime":"2026-11-09T03:20:00Z"}}]}}}}"#
    )
}

fn pod_list() -> String {
    format!(
        r#"{{"apiVersion":"v1","kind":"PodList","metadata":{{}},"items":[
  {{"apiVersion":"v1","kind":"Pod",
    "metadata":{{"name":"{POD}","namespace":"{NS}",
      "labels":{{"batch.kubernetes.io/job-name":"{NAME}","job-name":"{NAME}"}}}},
    "spec":{{"containers":[]}},
    "status":{{"phase":"Succeeded","containerStatuses":[
      {{"name":"log-shipper","ready":false,"restartCount":0,"image":"x","imageID":"x",
        "state":{{"terminated":{{"exitCode":0,"finishedAt":"2026-11-09T03:19:00Z"}}}}}},
      {{"name":"runner","ready":false,"restartCount":0,"image":"x","imageID":"x",
        "state":{{"terminated":{{"exitCode":0,"finishedAt":"2026-11-09T03:19:00Z"}}}}}}
    ]}}}}]}}"#
    )
}

fn log_body() -> String {
    format!(
        "{{\"level\":\"INFO\"}}\nreceipt-key={RECEIPT_KEY}\nsidecar-key={RECEIPT_SIDECAR_KEY}\n"
    )
}

/// The archive oracle a finished, verifiable run gets.
fn observed_archive(
    _keys: weirkeeper::controllers::backup::EvidenceKeys,
) -> futures::future::BoxFuture<'static, Option<ArchiveObservation>> {
    Box::pin(async {
        Some(ArchiveObservation {
            presence: EvidencePresence {
                payload: true,
                sidecar: true,
            },
            covered: Some((1_760_000_000_000, 1_760_000_060_000)),
            receipt_sha256: Some(RECEIPT_DIGEST.to_string()),
        })
    })
}

/// What one request the double answered looked like.
#[derive(Clone, Debug)]
struct Seen {
    method: String,
    path: String,
    body: String,
}

/// A `kube::Client` whose Nth `PATCH …/status` answers `status_codes[N]`.
///
/// WRITTEN HERE RATHER THAN TAKEN FROM `weirkeeper::testing`, because this
/// test's whole property is about the SECOND patch: a static route table
/// answers both of them the same way, so "the second one 500s and the first
/// one's values survive" is unassertable through it.
fn sequenced_client(status_codes: Vec<u16>) -> (kube::Client, Arc<Mutex<Vec<Seen>>>) {
    let seen: Arc<Mutex<Vec<Seen>>> = Arc::new(Mutex::new(Vec::new()));
    let patches = Arc::new(AtomicUsize::new(0));
    let codes = Arc::new(status_codes);
    let svc = {
        let seen = Arc::clone(&seen);
        service_fn(move |req: Request<Body>| {
            let seen = Arc::clone(&seen);
            let patches = Arc::clone(&patches);
            let codes = Arc::clone(&codes);
            async move {
                let method = req.method().to_string();
                let uri = req.uri().to_string();
                let path = uri.split('?').next().unwrap_or(&uri).to_string();
                let body = req
                    .into_body()
                    .collect()
                    .await
                    .map(|c| String::from_utf8_lossy(&c.to_bytes()).to_string())
                    .unwrap_or_default();
                seen.lock().expect("readable").push(Seen {
                    method: method.clone(),
                    path: path.clone(),
                    body,
                });
                let (status, payload) = if method == "PATCH" && path.ends_with("/status") {
                    let n = patches.fetch_add(1, Ordering::SeqCst);
                    let code = codes.get(n).copied().unwrap_or(200);
                    if code == 200 {
                        (200, backup_json())
                    } else {
                        (
                            code,
                            r#"{"kind":"Status","apiVersion":"v1","status":"Failure",
                                "message":"the status subresource is unavailable","code":500}"#
                                .to_string(),
                        )
                    }
                } else if method == "PATCH" {
                    (200, job_body())
                } else if path.ends_with("/log") {
                    (200, log_body())
                } else if path.ends_with("/pods") {
                    (200, pod_list())
                } else {
                    (200, job_body())
                };
                Ok::<_, std::convert::Infallible>(
                    Response::builder()
                        .status(status)
                        .header("content-type", "application/json")
                        .body(Body::from(payload.into_bytes()))
                        .expect("a response builds"),
                )
            }
        })
    };
    (kube::Client::new(svc, "default"), seen)
}

/// A verify oracle that answers `Valid` without touching a bucket.
fn valid_oracle(r: EvidenceRef) -> futures::future::BoxFuture<'static, VerificationResult> {
    valid_oracle_at(r, Utc.with_ymd_and_hms(2026, 9, 11, 3, 20, 0).unwrap())
}

/// The same answer, reached at `at`.
///
/// SEPARATE BECAUSE THE INSTANT IS THE PROPERTY IN ONE TEST AND NOISE IN THE
/// REST. `verify_evidence` reads its own clock (the brief's signature carries
/// no `now`), so a re-verification on a later pass genuinely produces a later
/// `verifiedAt` — and the rule that keeps the controller quiet is that a
/// verdict whose SUBSTANCE has not changed keeps the instant it was reached
/// at. A steady-object test whose oracle returns a CONSTANT instant cannot see
/// that rule at all: measured, the mutant "verifiedAt is always now" left
/// `a_verified_object_reconciles_without_a_patch` green at 13 passed / 1
/// failed, killed only by `not_attempted_is_a_verdict_distinct_from_invalid`.
fn valid_oracle_at(
    r: EvidenceRef,
    at: chrono::DateTime<Utc>,
) -> futures::future::BoxFuture<'static, VerificationResult> {
    Box::pin(async move {
        VerificationResult {
            result: VerificationVerdict::Valid,
            matched_key_id: Some(FIXTURE_KEY_ID.to_string()),
            payload_type: r.payload_type.to_string(),
            verified_at: at,
            detail: None,
        }
    })
}

/// The verification is a SECOND patch, after the one carrying the exit code.
///
/// KILLS: "write the verification into the same patch as the exit code" — the
/// second arm's 500 then loses the exit code too, which is exactly the failure
/// mode the split exists to prevent.
#[tokio::test]
async fn verification_is_a_second_patch_after_the_status_patch() {
    // ---- ARM 1: the ORDER ----
    let (client, seen) = sequenced_client(vec![200, 200]);
    reconcile_backup(
        &backup(),
        &client,
        &observed_archive,
        &valid_oracle,
        Utc.with_ymd_and_hms(2026, 11, 9, 3, 20, 0).unwrap(),
    )
    .await
    .expect("the reconcile succeeds");

    let log = seen.lock().expect("readable").clone();
    let status_patches: Vec<&Seen> = log
        .iter()
        .filter(|s| s.method == "PATCH" && s.path.ends_with("/status"))
        .collect();
    assert_eq!(
        status_patches.len(),
        2,
        "exactly two `/status` patches: the terminal one and the verification one. Got {:?}",
        log.iter().map(|s| (&s.method, &s.path)).collect::<Vec<_>>()
    );

    let first: Value = serde_json::from_str(&status_patches[0].body).expect("the first is JSON");
    let second: Value = serde_json::from_str(&status_patches[1].body).expect("the second is JSON");
    assert_eq!(
        first.pointer("/status/exitCode"),
        Some(&json!(0)),
        "the FIRST patch carries the exit code; got {first}"
    );
    assert!(
        first.pointer("/status/evidence/verification").is_none(),
        "the first patch carries NO verification — a verification failure must never be able to \
         prevent the exit code from being recorded; got {first}"
    );
    assert_eq!(
        second.pointer("/status/evidence/verification/result"),
        Some(&json!("Valid")),
        "the SECOND patch carries the verification; got {second}"
    );
    assert_eq!(
        second.pointer("/status/evidence/verification/matchedKeyId"),
        Some(&json!(FIXTURE_KEY_ID))
    );
    assert!(
        second.pointer("/status/exitCode").is_none(),
        "the second patch is about the verification and nothing else; got {second}"
    );

    // THE SECOND PATCH CARRIES THE WHOLE CONDITION LIST. A JSON merge patch
    // REPLACES arrays, so a second patch carrying only `[Verified]` would
    // DELETE the terminal `Complete` condition the first one just wrote.
    let types: Vec<String> = second
        .pointer("/status/conditions")
        .and_then(Value::as_array)
        .expect("the second patch carries conditions")
        .iter()
        .map(|c| c["type"].as_str().unwrap_or_default().to_string())
        .collect();
    assert!(
        types.contains(&"Complete".to_string()) && types.contains(&CONDITION_VERIFIED.to_string()),
        "the second patch re-sends every condition the first wrote, plus `Verified`; got {types:?}"
    );
    assert_eq!(
        types.iter().filter(|t| *t == CONDITION_VERIFIED).count(),
        1,
        "a condition array is a map keyed by `type`; got {types:?}"
    );
    let verified = second
        .pointer("/status/conditions")
        .and_then(Value::as_array)
        .expect("conditions")
        .iter()
        .find(|c| c["type"] == json!(CONDITION_VERIFIED))
        .expect("the Verified condition is there");
    assert_eq!(verified["status"], json!("True"));
    assert_eq!(verified["reason"], json!(REASON_VERIFIED));
    assert!(
        verified["message"]
            .as_str()
            .is_some_and(|m| m.starts_with("verified by weirkeeper at")),
        "the condition's message IS the badge label; got {verified}"
    );

    // ---- ARM 2: a 500 on the SECOND patch leaves the first's values ----
    let (client, seen) = sequenced_client(vec![200, 500]);
    let outcome = reconcile_backup(
        &backup(),
        &client,
        &observed_archive,
        &valid_oracle,
        Utc.with_ymd_and_hms(2026, 11, 9, 3, 20, 0).unwrap(),
    )
    .await;
    assert!(
        outcome.is_err(),
        "a 500 on the verification patch is an error the reconciler requeues on — it is not \
         swallowed, because a verification that never lands must be retried"
    );
    let log = seen.lock().expect("readable").clone();
    let status_patches: Vec<&Seen> = log
        .iter()
        .filter(|s| s.method == "PATCH" && s.path.ends_with("/status"))
        .collect();
    assert_eq!(status_patches.len(), 2, "both patches were attempted");
    let first: Value = serde_json::from_str(&status_patches[0].body).expect("JSON");
    assert_eq!(
        first.pointer("/status/exitCode"),
        Some(&json!(0)),
        "THE WHOLE POINT: the exit code was recorded by a patch that returned 200 BEFORE the one \
         that failed. Folded into one patch, this 500 would have lost it. Got {first}"
    );
    assert_eq!(
        first.pointer("/status/evidence/receiptSha256"),
        Some(&json!(RECEIPT_DIGEST)),
        "and so was the digest the later verification is checked against"
    );
    assert!(
        log.iter()
            .any(|s| s.method == "PATCH" && s.path.ends_with(&format!("/jobs/{NAME}"))),
        "the TTL patch happened between them, so a failed verification cannot leave a Job \
         without one; got {:?}",
        log.iter().map(|s| (&s.method, &s.path)).collect::<Vec<_>>()
    );
}

/// **A VERIFIED OBJECT RECONCILES AGAIN AND SENDS NOTHING.**
///
/// Plan erratum **E11(d)**, and this one was MEASURED ON A LIVE CLUSTER during
/// the Phase B run that otherwise passed: **20 `Backup` reconciles and 20
/// `Restore` reconciles per second**, each a real write. A JSON merge patch
/// REPLACES arrays, so the terminal patch's `conditions: [Complete,
/// EvidenceRecorded]` DELETED the `Verified` condition the second patch had
/// just added; the second patch re-added it; the write woke the reconciler;
/// forever. Every verdict on every pass was correct, and the object never
/// stopped being rewritten.
///
/// KILLS: removing `verification::carry_verified` from the terminal patch
/// builder. The count goes from 0 to 2 and the `Verified` condition is absent
/// from the terminal patch, which is the loop.
#[tokio::test]
async fn a_verified_object_reconciles_without_a_patch() {
    // PASS ONE, against an object with no status at all: two patches.
    let (client, seen) = sequenced_client(vec![200, 200]);
    reconcile_backup(
        &backup(),
        &client,
        &observed_archive,
        &valid_oracle,
        Utc.with_ymd_and_hms(2026, 11, 9, 3, 20, 0).unwrap(),
    )
    .await
    .expect("the first reconcile succeeds");
    let first = seen.lock().expect("readable").clone();
    let patches: Vec<&Seen> = first
        .iter()
        .filter(|s| s.method == "PATCH" && s.path.ends_with("/status"))
        .collect();
    assert_eq!(patches.len(), 2, "the first pass writes both patches");

    // THE OBJECT AS THE API SERVER NOW HOLDS IT: the two patches applied, in
    // order, exactly as `conditions::apply_merge_patch` says the server does.
    let mut status = json!({});
    for p in &patches {
        let body: Value = serde_json::from_str(&p.body).expect("the patch is JSON");
        weirkeeper::conditions::apply_merge_patch(
            &mut status,
            body.get("status").expect("the patch carries a status"),
        );
    }
    let types: Vec<&str> = status["conditions"]
        .as_array()
        .expect("conditions")
        .iter()
        .filter_map(|c| c["type"].as_str())
        .collect();
    assert!(
        types.contains(&CONDITION_VERIFIED),
        "after both patches the object carries a Verified condition; got {types:?}"
    );

    // PASS TWO, over that object. NOTHING IS SENT.
    let mut settled: Value = serde_json::from_str(&backup_json()).expect("the fixture is JSON");
    settled["status"] = status;
    let settled: Backup = serde_json::from_value(settled).expect("the settled object is a Backup");

    // …AND THE ORACLE'S OWN CLOCK MOVES TOO. `verify_evidence` reads the clock
    // itself (the signature carries no `now`), so a second verification really
    // does reach a later instant — and the rule that keeps this quiet is that
    // an UNCHANGED verdict keeps the instant it was reached at. An oracle
    // returning a constant would make this test blind to that rule; measured,
    // the mutant "verifiedAt is always now" survived here until this line moved.
    let later =
        |r: EvidenceRef| valid_oracle_at(r, Utc.with_ymd_and_hms(2026, 9, 11, 18, 45, 0).unwrap());
    let (client, seen) = sequenced_client(vec![200, 200]);
    reconcile_backup(
        &settled,
        &client,
        &observed_archive,
        &later,
        Utc.with_ymd_and_hms(2026, 11, 9, 4, 0, 0).unwrap(),
    )
    .await
    .expect("the second reconcile succeeds");
    let second = seen.lock().expect("readable").clone();
    let patches: Vec<String> = second
        .iter()
        .filter(|s| s.method == "PATCH" && s.path.ends_with("/status"))
        .map(|s| s.body.clone())
        .collect();
    assert_eq!(
        patches.len(),
        0,
        "A STEADY OBJECT ISSUES ZERO `/status` PATCHES. A reconciler's own status write is what \
         wakes it, so one patch per pass is a loop and not an inefficiency — measured at 20 \
         reconciles per second before `carry_verified` existed. Sent:\n  {}",
        patches.join("\n  ")
    );

    // …AND BOTH CLOCKS MOVED BETWEEN THE TWO PASSES — the reconcile's
    // (03:20 -> 04:00) and the verification's own (03:20 -> 18:45). This is
    // not passing because nothing changed: it is passing because `verifiedAt`
    // and every `lastTransitionTime` are WHEN THE FACT CHANGED, not when it
    // was last re-confirmed.
}

/// With no evidence credential the controller constructs `None` and every
/// verification is `NotAttempted` — the controller STARTS, and says so.
///
/// KILLS: "make the evidence Secret mandatory" (the controller refuses to
/// start, and `optional: true` is what stops that).
#[tokio::test]
async fn no_evidence_credential_starts_and_reports_not_attempted() {
    let r = weirkeeper::verification::unverified_evidence(EvidenceRef {
        payload_key: RECEIPT_KEY.to_string(),
        payload_sha256: RECEIPT_DIGEST.to_string(),
        sidecar_key: RECEIPT_SIDECAR_KEY.to_string(),
        payload_type: logweir_verify::PAYLOAD_TYPE_BACKUP_RECEIPT,
    })
    .await;
    assert_eq!(r.result, VerificationVerdict::NotAttempted);
    assert_eq!(r.detail.as_deref(), Some(NO_CREDENTIAL_DETAIL));

    // …and the reconcile still records the exit code and writes the block.
    let (client, seen) = sequenced_client(vec![200, 200]);
    reconcile_backup(
        &backup(),
        &client,
        &observed_archive,
        &weirkeeper::verification::unverified_evidence,
        Utc.with_ymd_and_hms(2026, 11, 9, 3, 20, 0).unwrap(),
    )
    .await
    .expect("a controller with no evidence credential reconciles normally");
    let log = seen.lock().expect("readable").clone();
    let second = log
        .iter()
        .filter(|s| s.method == "PATCH" && s.path.ends_with("/status"))
        .nth(1)
        .expect("the second patch happened");
    let v: Value = serde_json::from_str(&second.body).expect("JSON");
    assert_eq!(
        v.pointer("/status/evidence/verification/result"),
        Some(&json!("NotAttempted")),
        "not `Invalid`, and not absent: the UI renders `unverified` beside the printed CLI \
         command, which is strictly more honest than a green badge or a blank. Got {v}"
    );
    assert_eq!(
        v.pointer("/status/evidence/verification/detail"),
        Some(&json!(NO_CREDENTIAL_DETAIL))
    );
    let verified = v
        .pointer("/status/conditions")
        .and_then(Value::as_array)
        .expect("conditions")
        .iter()
        .find(|c| c["type"] == json!(CONDITION_VERIFIED))
        .expect("a Verified condition");
    assert_eq!(verified["status"], json!("False"));
    assert_eq!(verified["reason"], json!(REASON_VERIFICATION_NOT_ATTEMPTED));
    assert_eq!(verified["message"], json!(UNVERIFIED));
}

/// The shipped Deployment projects `logweir-evidence-ro` with
/// `optional: true`, so a cluster without that Secret STARTS.
///
/// KILLS: "make the evidence Secret mandatory" — the same mutant, checked on
/// the manifest rather than on the code, because that is where it would
/// actually be made.
#[test]
fn the_deployment_marks_the_evidence_secret_optional() {
    let text = read("config/manager/deployment.yaml");
    let doc: Value = {
        // The file leads with a comment block and one `---`.
        let body = text.split_once("\n---\n").map_or(text.as_str(), |(_, b)| b);
        serde_yaml::from_str(body).expect("the Deployment is YAML")
    };
    let env = doc
        .pointer("/spec/template/spec/containers/0/env")
        .and_then(Value::as_array)
        .expect("the container carries an env block");

    let ro: Vec<&Value> = env
        .iter()
        .filter(|e| {
            e.pointer("/valueFrom/secretKeyRef/name") == Some(&json!("logweir-evidence-ro"))
        })
        .collect();
    assert_eq!(
        ro.len(),
        2,
        "the evidence credential is TWO variables — an access key id and a secret access key — \
         both from `logweir-evidence-ro`; got {env:?}"
    );
    for e in &ro {
        assert_eq!(
            e.pointer("/valueFrom/secretKeyRef/optional"),
            Some(&json!(true)),
            "`optional: true` is what makes a cluster WITHOUT this Secret start at all. Mandatory, \
             the pod sits in CreateContainerConfigError and every one of the six reconcilers is \
             down for want of a display feature. Got {e}"
        );
    }
    // AND NO LAPTOP-SPECIFIC ENDPOINT IN THE SHIPPED DOCUMENT. The demo's S3
    // literals belong to the demo's own patch (Global Constraint 37): a
    // stranger must be able to apply `logweir.yaml` unedited.
    //
    // CHECKED OVER THE PARSED DOCUMENT, NOT THE FILE TEXT. The file's comment
    // block explains WHERE those literals live and therefore names the host —
    // which is the sentence an operator needs and not a value the API server
    // ever sees. A text grep here would forbid the explanation along with the
    // thing it explains.
    let shipped = serde_yaml::to_string(&doc).expect("the parsed Deployment re-serialises");
    assert!(
        !shipped.contains("host.docker.internal"),
        "the SHIPPED Deployment names no host-specific endpoint in any VALUE; the demo patches \
         its own. Got:\n{shipped}"
    );

    // …AND `logweir.yaml` — the one file a stranger applies — carries the same
    // env block, because `scripts/render-install.sh` is its only producer and
    // a Deployment edit that did not reach it would ship the old one.
    let install = read("logweir.yaml");
    assert!(
        install.contains("logweir-evidence-ro"),
        "the rendered install file carries the evidence credential's env block; re-run \
         `./scripts/render-install.sh`"
    );
    assert_eq!(
        install.matches("optional: true").count(),
        2,
        "both projections in the rendered install file are optional"
    );
}

// ===========================================================================
// The Phase B demo — its addressing, its ordering, and its transcript
// ===========================================================================

fn justfile() -> String {
    read("justfile")
}

/// The `k8s-demo` recipe's body **and the script it names**, concatenated.
///
/// ONE STRING, TWO FILES, ON PURPOSE. `just k8s-demo` is the two-line recipe
/// `cargo build -p logweir` + `./scripts/k8s-demo.sh`, exactly as `just
/// mvp-demo` is; everything a demo actually does is in the script, and an
/// assertion about "the demo" that read only the justfile would assert nothing
/// about any of it. The ORDER matters for
/// [`the_demo_runs_lint_before_the_stack_is_up`], and it is preserved: the
/// recipe comes first because that is the order they execute in.
///
/// The recipe body runs from its recipe line to the end of the file — it is
/// the LAST recipe there by STANDING RULE 17 (chain J, slot 17, appended at
/// the end), so that is exact rather than convenient.
fn k8s_demo_recipe() -> String {
    let text = justfile();
    let start = text
        .find("\nk8s-demo:")
        .expect("`just k8s-demo` is a recipe in the justfile");
    format!("{}\n{}", &text[start + 1..], read("scripts/k8s-demo.sh"))
}

fn phase_b_demo() -> String {
    read("e2e/k8s/phase-b-demo.md")
}

/// The demo addresses the compose stack's MinIO over `host.docker.internal`,
/// with the three `AWS_*` variables a pod needs to reach it.
///
/// KILLS: "point the demo's archive at `s3://kafka-backups/k8s-demo` with no
/// endpoint override" — no pod could reach the bucket, and nothing else in the
/// tree says where the object store is (critique B **H20**).
#[test]
fn the_demo_addresses_minio_over_host_docker_internal() {
    let recipe = k8s_demo_recipe();
    let doc = phase_b_demo();
    for needle in [
        "http://host.docker.internal:9000",
        "AWS_ALLOW_HTTP",
        "true",
        "AWS_REGION",
        "us-east-1",
        "s3://kafka-backups/k8s-demo",
    ] {
        assert!(
            recipe.contains(needle),
            "`just k8s-demo` must name `{needle}` — the compose stack's MinIO is reachable from \
             a docker-desktop pod ONLY as host.docker.internal:9000 and ONLY with an S3 endpoint \
             override"
        );
        assert!(
            doc.contains(needle),
            "`e2e/k8s/phase-b-demo.md` must name `{needle}`, so the transcript records the \
             addressing it ran with"
        );
    }

    // THE BUCKET IS CREATED BEFORE ANY CUSTOM RESOURCE. `just e2e-down` runs
    // `down -v` and EMPTIES the MinIO volume (plan erratum E12(g)), so the
    // demo makes its own bucket after bringing the stack up — and it has to do
    // that before a `Backup` exists to write into it.
    let mc = recipe
        .find(" mb ")
        .expect("the recipe creates the bucket with `mc mb`");
    for kind in ["kafkacluster", "backup.yaml", "restore.yaml"] {
        if let Some(at) = recipe.find(kind) {
            assert!(
                mc < at,
                "the bucket is created BEFORE any custom resource is applied; `mc mb` is at \
                 {mc} and `{kind}` at {at}"
            );
        }
    }
}

/// `just lint` runs BEFORE the stack comes up, and never while it is up.
///
/// KILLS: "run `just lint` while the stack is up" —
/// `scripts/time-unit-suite.sh` refuses, exit 1, while 9092 or 9000 answers
/// (Global Constraint 22), so the recipe would fail by design.
#[test]
fn the_demo_runs_lint_before_the_stack_is_up() {
    let recipe = k8s_demo_recipe();
    let lint = recipe
        .find("just lint")
        .expect("the recipe runs `just lint`");
    let up = recipe
        .find("just e2e-up")
        .expect("the recipe brings the compose stack up");
    assert!(
        lint < up,
        "`just lint` is at {lint} and `just e2e-up` at {up}: lint must run with the stack DOWN. \
         `scripts/time-unit-suite.sh` refuses to run, exit 1, while 9092 or 9000 answers (Global \
         Constraint 22), so the other order is not a style preference — it is a red recipe."
    );
    let after = &recipe[up..];
    assert!(
        !after.contains("just lint"),
        "no `lint` invocation follows the stack coming up; the tail of the recipe is:\n{after}"
    );

    // The transcript records BOTH orderings with their own rc lines.
    let doc = phase_b_demo();
    let dlint = doc
        .find("just lint")
        .expect("the transcript records the lint");
    let dup = doc
        .find("just e2e-up")
        .expect("the transcript records the stack coming up");
    assert!(
        dlint < dup,
        "the transcript shows lint first, at {dlint}, and the stack at {dup}"
    );
}

/// The transcript is a section per step, each with output and a stated
/// verdict.
#[test]
fn k8s_demo_transcript_is_present() {
    let doc = phase_b_demo();
    let sections: Vec<&str> = doc.split("\n## ").skip(1).collect();
    assert!(
        sections.len() >= 8,
        "the transcript carries a section per step of the demo; got {} sections",
        sections.len()
    );
    let mut with_verdict = 0;
    for s in &sections {
        let has_output = s.contains("```");
        let has_verdict = s.contains("**Verdict:");
        assert!(
            has_output,
            "every step section carries its transcript; this one does not:\n{}",
            &s[..s.len().min(400)]
        );
        if has_verdict {
            with_verdict += 1;
        }
    }
    assert_eq!(
        with_verdict,
        sections.len(),
        "every step section states a verdict; {} of {} did",
        with_verdict,
        sections.len()
    );

    // THE FOUR FIELDS THE EXIT CRITERION NAMES, on the object each belongs to.
    for needle in [
        "phase: Succeeded",
        "exitCode: 0",
        "outcome: pass",
        "result: Valid",
        "matchedKeyId",
        "reachable: true",
    ] {
        assert!(
            doc.contains(needle),
            "Phase B's exit criterion requires `{needle}` in the recorded transcript"
        );
    }

    // EVERY FIELD READ WITH `-o jsonpath`, NEVER THROUGH A PIPE INTO ANOTHER
    // EXIT CODE (STANDING RULE 20).
    assert!(
        doc.matches("-o jsonpath").count() >= 8,
        "every status field in the transcript is read with `kubectl -o jsonpath`; found {}",
        doc.matches("-o jsonpath").count()
    );
    assert!(
        doc.matches("rc=").count() >= 12,
        "every command's exit code is read on its own line; found {} `rc=` readings",
        doc.matches("rc=").count()
    );
}

/// The demo is a RECIPE, and it is author-only.
///
/// Global Constraint 37: a locally built or locally tagged image is author-only
/// and never satisfies spec §16 clause 1. The recipe and the transcript both
/// have to say so where somebody would read it.
#[test]
fn the_demo_says_it_is_author_only() {
    let recipe = k8s_demo_recipe();
    assert!(
        recipe.contains("author-only"),
        "the recipe's own header says it is author-only (Global Constraint 37)"
    );
    let doc = phase_b_demo();
    assert!(
        doc.contains("author-only") && doc.contains("docker tag"),
        "the transcript records the author-only `docker tag` step by name — the kubelet keys on \
         the WHOLE reference, so the shipped `ghcr.io/logweir/…@sha256:…` references start on \
         this node only after the local images are tagged with them (plan erratum E19(b))"
    );
}
