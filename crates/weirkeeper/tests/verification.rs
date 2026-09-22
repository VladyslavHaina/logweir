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
use kube::api::ObjectMeta;
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
use weirkeeper::crds::trust_policy::{
    KeyAlgorithm, KeyPrincipal, KeyState, KeyUsage as SpecUsage, RevocationReason, TrustPolicy,
    TrustPolicySpec, TrustedKey as SpecKey,
};
use weirkeeper::crds::trust_roster::{KeyEntry, TrustRosterSpec};
use weirkeeper::trust::{Resolution, ResolvedTrust};
use weirkeeper::verification::{
    backup_badge, restore_badge, verify_evidence, EvidenceRef, VerificationResult,
    VerificationVerdict, NO_CREDENTIAL_DETAIL, NO_POLICY_KEYS_DETAIL, NO_SIGNING_KEYS_DETAIL,
    UNVERIFIED,
};

// ===========================================================================
// Paths, fixtures and a scratch evidence tree
// ===========================================================================
/// The `metadata.resourceVersion` every fixture object carries — the thing a
/// watch always delivers and a hand-built fixture used not to.
///
/// D-SEAMS **S7**: every `/status` write is a merge PATCH preconditioned on
/// this value, so a fixture without one is not an object this controller could
/// ever have been handed. Defect STATUS-PATCH-NO-RV is what its absence hid.
const FIXTURE_RESOURCE_VERSION: &str = "4071";

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

/// The trust a ROSTER-ONLY cluster resolves to, given `keys` as its
/// `signingKeys`.
///
/// PLAT-19.1 moved `verify_evidence` from a `TrustRosterSpec` to the
/// namespace's resolved trust, and `weirkeeper::trust::synthesize_legacy` is
/// exactly what the controller reaches for a cluster with no `TrustPolicy`
/// (D3 §7.5). Every assertion below was written against the roster walk and now
/// runs through the resolution layer unchanged — which is what makes them
/// evidence for §7.5's byte-for-byte claim rather than tests that merely still
/// compile.
fn roster(keys: Vec<KeyEntry>) -> ResolvedTrust {
    weirkeeper::trust::synthesize_legacy(&roster_spec(keys))
}

/// The raw `TrustRosterSpec` behind [`roster`].
fn roster_spec(keys: Vec<KeyEntry>) -> TrustRosterSpec {
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

/// One signing key entry with a `notAfter` — the ONLY roster shape PLAT-19.1
/// changes the behaviour of, and the one [`entry`] cannot express.
fn entry_expiring(key_id: &str, spki_pem: &str, not_after: &str) -> KeyEntry {
    KeyEntry {
        not_after: Some(
            chrono::DateTime::parse_from_rfc3339(not_after)
                .expect("a fixture instant")
                .with_timezone(&Utc),
        ),
        ..entry(key_id, spki_pem)
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
        trust: None,
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

/// The inputs digest the fixture Backup's `status.execution` records and the
/// fixture Job carries: a Backup whose Job this controller created from frozen
/// inputs (PLAT-06.1). A label, not a real digest — the Job-observing path
/// compares the two strings.
const FIXTURE_INPUTS_SHA256: &str =
    "sha256:f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1";

/// A manual `Backup` whose inputs this controller froze and whose Job it
/// created: typed spec, server UID, `status.execution`, and no annotation.
fn backup_json() -> String {
    format!(
        r#"{{
  "apiVersion": "logweir.dev/v1alpha1", "kind": "Backup",
  "metadata": {{
    "name": "{NAME}", "namespace": "{NS}", "uid": "{UID}", "generation": 3,
    "resourceVersion": "{FIXTURE_RESOURCE_VERSION}"
  }},
  "spec": {{
    "sourceRef": {{ "name": "prod" }},
    "topics": ["orders"],
    "archive": {{ "url": "s3://kafka-backups/k8s-demo", "secretRef": {{ "name": "logweir-s3" }} }},
    "triggeredBy": "manual",
    "deadlineSeconds": 3600
  }},
  "status": {{
    "execution": {{
      "id": "{UID}",
      "inputsRef": {{ "name": "{NAME}-plan" }},
      "inputsSha256": "{FIXTURE_INPUTS_SHA256}"
    }}
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
  "metadata":{{"name":"{NAME}","namespace":"{NS}","uid":"bbbbbbbb-0000-4000-8000-0000000000b1",
    "ownerReferences":[{{"apiVersion":"logweir.dev/v1alpha1","kind":"Backup","name":"{NAME}","uid":"{UID}","controller":true,"blockOwnerDeletion":true}}],
    "annotations":{{"logweir.dev/execution-inputs-sha256":"{FIXTURE_INPUTS_SHA256}"}}}},
  "spec":{{"template":{{"spec":{{"containers":[],"restartPolicy":"Never","serviceAccountName":"{RUNNER_SERVICE_ACCOUNT}"}}}}}},
  "status":{{"conditions":[{{"type":"Complete","status":"True",
     "lastProbeTime":"2026-11-09T03:20:00Z","lastTransitionTime":"2026-11-09T03:20:00Z"}}]}}}}"#
    )
}

fn pod_list() -> String {
    // THE OWNER REFERENCE IS NOT DECORATION (D-SEAMS **S6**, defect
    // `SEC-PODLOG`). The reconciler reads a pod's exit code and stdout only
    // when the pod's CONTROLLER owner reference is the Job it is holding, so a
    // fixture without one is a fixture whose pod is never read and whose every
    // verification assertion would be vacuous.
    format!(
        r#"{{"apiVersion":"v1","kind":"PodList","metadata":{{}},"items":[
  {{"apiVersion":"v1","kind":"Pod",
    "metadata":{{"name":"{POD}","namespace":"{NS}",
      "ownerReferences":[{{"apiVersion":"batch/v1","kind":"Job","name":"{NAME}",
        "uid":"bbbbbbbb-0000-4000-8000-0000000000b1","controller":true,
        "blockOwnerDeletion":true}}],
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
        "{{\"level\":\"INFO\"}}\nreceipt-sha256={RECEIPT_DIGEST}\nreceipt-key={RECEIPT_KEY}\nsidecar-key={RECEIPT_SIDECAR_KEY}\n"
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
            // D3 W2 (PLAT-14.1, defect STATUS-RECORDS): two additive fields
            // on the observation. `None` here on purpose — this fixture is
            // about the VERIFICATION path, and a `Valid` verdict with nothing
            // observed must write neither `status.records` nor
            // `status.capture`. `tests/backup_controller.rs` owns the rows
            // where they ARE observed.
            records: None,
            capture: None,
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
    sequenced_client_with_pods(status_codes, pod_list())
}

/// An EMPTY pod list — the runner pod GC'd, evicted or deleted out from under
/// a finished Job, which the Job outlives by seven days
/// (`TTL_SECONDS_AFTER_FINISHED`).
fn no_pods() -> String {
    r#"{"apiVersion":"v1","kind":"PodList","metadata":{},"items":[]}"#.to_string()
}

/// [`sequenced_client`] with the `/pods` answer as a parameter.
///
/// THE POD LIST IS THE VARIABLE IN THE CRASHED-PATH ROW and a constant
/// everywhere else, so it is a parameter of this function and not a second
/// copy of the double.
fn sequenced_client_with_pods(
    status_codes: Vec<u16>,
    pods: String,
) -> (kube::Client, Arc<Mutex<Vec<Seen>>>) {
    let pods = Arc::new(pods);
    let seen: Arc<Mutex<Vec<Seen>>> = Arc::new(Mutex::new(Vec::new()));
    let patches = Arc::new(AtomicUsize::new(0));
    let codes = Arc::new(status_codes);
    let svc = {
        let seen = Arc::clone(&seen);
        service_fn(move |req: Request<Body>| {
            let seen = Arc::clone(&seen);
            let patches = Arc::clone(&patches);
            let codes = Arc::clone(&codes);
            let pods = Arc::clone(&pods);
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
                    (200, pods.as_ref().clone())
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
            trust: None,
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

    // AND THE BUILDER ITSELF, REACHED DIRECTLY — Task 24 fix round 1.
    //
    // The zero above is now defended by TWO independent guards: `carry_verified`
    // in this builder, and `reconcile_backup`'s already-terminal return (step
    // 2b), which stops a settled object from reaching the builder at all. The
    // second one masks the first, so the count alone can no longer tell whether
    // `carry_verified` is still there. This arm asks the builder directly, and
    // it is the arm that dies when `carry_verified` is removed.
    let rebuilt = weirkeeper::controllers::backup::finished_status_patch(
        &settled,
        0,
        &weirkeeper::controllers::backup::evidence_keys(&log_body()),
        None,
        None,
        None,
        Some(RECEIPT_DIGEST),
        Utc.with_ymd_and_hms(2026, 11, 9, 4, 0, 0).unwrap(),
    );
    let rebuilt_types = condition_types(rebuilt.pointer("/status").expect("a status"));
    assert!(
        rebuilt_types.iter().any(|t| t == CONDITION_VERIFIED),
        "`finished_status_patch` carries the object's existing `Verified` condition forward — a \
         merge patch REPLACES arrays, so a builder that dropped it would delete the condition \
         the second patch adds, which would re-add it, which would wake the reconciler. That is \
         the loop, measured at 20 reconciles per second. Got {rebuilt_types:?} from {rebuilt}"
    );
}

/// The settled object of [`a_verified_object_reconciles_without_a_patch`]'s
/// first pass: terminal, `exitCode: 0`, `Valid`, and carrying `Complete`,
/// `EvidenceRecorded` and `Verified`.
///
/// Built by RUNNING the first pass and applying its two patches the way the
/// API server would (`conditions::apply_merge_patch`), never by writing a
/// status literal: a hand-written fixture would pin what this test's author
/// believes a verified `Backup` looks like, and the defect below is about what
/// the controller does to the one it actually produced.
async fn settled_verified_backup() -> Backup {
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
    // Onto the status the object already had (`status.execution`), exactly as
    // the API server merges the two patches.
    let mut status = serde_json::from_str::<Value>(&backup_json()).expect("the fixture is JSON")
        ["status"]
        .clone();
    for p in seen
        .lock()
        .expect("readable")
        .iter()
        .filter(|s| s.method == "PATCH" && s.path.ends_with("/status"))
    {
        let body: Value = serde_json::from_str(&p.body).expect("the patch is JSON");
        weirkeeper::conditions::apply_merge_patch(
            &mut status,
            body.get("status").expect("the patch carries a status"),
        );
    }
    let mut settled: Value = serde_json::from_str(&backup_json()).expect("the fixture is JSON");
    settled["status"] = status;
    serde_json::from_value(settled).expect("the settled object is a Backup")
}

/// The condition `type`s on a status value, in order.
fn condition_types(status: &Value) -> Vec<String> {
    status["conditions"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|c| c["type"].as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

/// **A TERMINAL, VERIFIED `Backup` WHOSE RUNNER POD IS GONE IS LEFT ALONE.**
///
/// Task 24 review, Milestone 5 — **proven on a live cluster**, not reasoned
/// about. A `Backup` at `phase: Succeeded`, `exitCode: 0`, a signed receipt in
/// the bucket and `verification.result: Valid` was turned into
///
/// | field | before | after |
/// |---|---|---|
/// | `status.phase` | `Succeeded` | `Failed` |
/// | `status.exitReason` | *(absent)* | `operational` |
/// | `status.conditions` | `[Complete, EvidenceRecorded, Verified]` | `[Failed/NoExitCode]` |
///
/// by nothing more than `kubectl delete pod` on its already-finished runner.
/// The Job survives its pod by seven days (`TTL_SECONDS_AFTER_FINISHED`), and
/// in that window pod GC, an eviction or a node restart does the same thing;
/// the `Restore` twin needs only a controller restart. The exit code was not
/// re-read — it was RE-DERIVED from a pod that no longer exists, and a run that
/// exited 0 and verified `Valid` was relabelled a failure that contradicts its
/// own `exitCode`.
///
/// KILLS: removing `reconcile_backup`'s already-terminal guard (step 2b,
/// before `find_pod`). Without it this pass finds no pod, takes the crashed
/// branch and PATCHES — the count goes from 0, and the patch's `conditions`
/// array is what replaces the three above.
#[tokio::test]
async fn a_terminal_verified_backup_whose_pod_is_gone_is_not_re_patched() {
    let settled = settled_verified_backup().await;
    let before = serde_json::to_value(settled.status.as_ref().expect("the settled object has one"))
        .expect("the status serialises");
    let types = condition_types(&before);
    assert!(
        types.iter().any(|t| t == CONDITION_VERIFIED) && types.iter().any(|t| t == "Complete"),
        "the fixture this row is about carries the conditions the defect deletes; got {types:?}"
    );
    assert_eq!(
        before["phase"],
        json!("Succeeded"),
        "…and it is terminal and green"
    );

    // THE POD IS GONE AND THE JOB IS NOT. This is the cluster state the review
    // produced with one `kubectl delete pod`.
    let (client, seen) = sequenced_client_with_pods(vec![200, 200], no_pods());
    reconcile_backup(
        &settled,
        &client,
        &observed_archive,
        &valid_oracle,
        Utc.with_ymd_and_hms(2026, 11, 9, 4, 0, 0).unwrap(),
    )
    .await
    .expect("the reconcile completes");

    let log = seen.lock().expect("readable").clone();
    let patches: Vec<String> = log
        .iter()
        .filter(|s| s.method == "PATCH" && s.path.ends_with("/status"))
        .map(|s| {
            let body: Value = serde_json::from_str(&s.body).unwrap_or(Value::Null);
            format!(
                "phase={} exitReason={} conditions={:?}",
                body.pointer("/status/phase").unwrap_or(&Value::Null),
                body.pointer("/status/exitReason").unwrap_or(&Value::Null),
                condition_types(body.pointer("/status").unwrap_or(&Value::Null))
            )
        })
        .collect();
    assert_eq!(
        patches.len(),
        0,
        "A FINISHED RUN WHOSE POD HAS BEEN GARBAGE-COLLECTED IS NOT RE-JUDGED. A merge patch \
         REPLACES arrays, so every patch listed here deletes `Complete` and `EvidenceRecorded` \
         off an object that earned them, and relabels a run that exited 0 with a signed, `Valid` \
         receipt as `Failed`/`operational`. Sent:\n  {}",
        patches.join("\n  ")
    );
    assert!(
        !log.iter().any(|s| s.path.ends_with("/pods")),
        "…and the pod list is not even READ: the guard is before `find_pod`, so a terminal \
         object costs one `GET /jobs` and nothing else. Saw: {:?}",
        log.iter().map(|s| (&s.method, &s.path)).collect::<Vec<_>>()
    );

    // BELT AND BRACES, AND IT IS A SEPARATE CLAIM. Even reached directly —
    // by a future edit that moves the guard, or by a caller this file does not
    // know about — the crashed-path builder no longer OWNS the whole condition
    // array: it carries the `Verified` condition it did not write.
    let crashed = weirkeeper::controllers::backup::crashed_status_patch(
        &settled,
        "NoExitCode",
        NAME,
        Utc.with_ymd_and_hms(2026, 11, 9, 4, 0, 0).unwrap(),
    );
    assert!(
        condition_types(crashed.pointer("/status").expect("the patch has a status"))
            .iter()
            .any(|t| t == CONDITION_VERIFIED),
        "`crashed_status_patch` goes through `verification::carry_verified`, so the controller's \
         own verdict about the run survives a patch that is not about it. Got {crashed}"
    );
}

/// **`status.backupId` SURVIVES THE VERIFICATION PASS.** Task 28a, defect 1,
/// third row.
///
/// The finished patch writes the id (`controllers::backup::finished_status_patch`);
/// the verification patch — the SECOND write of the same pass — follows it
/// milliseconds later. A merge patch leaves a key it does not mention alone,
/// so the id must still be on the object the API server holds afterwards, and
/// this asserts that against the object BUILT BY RUNNING BOTH PATCHES rather
/// than against a hand-written status literal.
///
/// It matters because the restore wizard reads `status.backupId` off a
/// `Succeeded` `Backup`, which is by definition an object that has been
/// through both patches. A second patch that nulled the field would put the
/// page back in the error box Task 28 found it in, with every unit test on the
/// first patch still green.
///
/// KILLS: dropping the `backupId` line from `finished_status_patch`; a second
/// patch that replaced the whole `status` object instead of merging into it.
#[tokio::test]
async fn the_backup_id_survives_the_second_status_patch() {
    let settled = settled_verified_backup().await;
    let status = settled.status.as_ref().expect("the settled object has one");
    assert_eq!(
        status.backup_id.as_deref(),
        Some(weirkeeper::controllers::backup::plan_backup_id(&backup()).as_str()),
        "after BOTH of the first pass's patches, applied the way the API server applies them, \
         the object still carries the archive's backup id. Got {:?}",
        status.backup_id
    );
    assert_eq!(
        status.backup_id.as_deref(),
        Some(UID),
        "…and for this fixture — no controller owner — that is the object's own UID"
    );
    assert_eq!(
        status.phase.as_deref(),
        Some("Succeeded"),
        "on the object shape the restore wizard actually reads: a terminal, verified `Backup`"
    );
}

/// With no evidence credential the controller constructs `None` and every
/// verification is `NotAttempted` — the controller STARTS, and says so.
///
/// KILLS: "make the evidence Secret mandatory" (the controller refuses to
/// start, and `optional: true` is what stops that).
#[tokio::test]
async fn no_evidence_credential_starts_and_reports_not_attempted() {
    let r = weirkeeper::verification::unverified_evidence(EvidenceRef {
        namespace: NS.to_string(),
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

/// **THE SHIPPED CONTROLLER CONTAINER PINS A LOG LEVEL** — Task 24 defect 3,
/// and the row review finding 2 says was missing.
///
/// # The measurement
///
/// During Task 24's Phase B run, `kubectl -n logweir-system logs
/// deploy/weirkeeper` returned **exit 0 and ZERO BYTES** from a controller
/// that had just reconciled six kinds, created two Jobs and verified two
/// signed documents. `main.rs` builds its subscriber with
/// `EnvFilter::from_default_env()`, and with `RUST_LOG` unset that filter
/// enables NOTHING — including every `error!` on the startup path. A control
/// plane whose diagnostics are invisible by default is one nobody can debug,
/// and it is how E19(e)'s wrong ERROR line survived undetected: nothing was
/// printing it.
///
/// PARSED, NEVER GREPPED. The file's comment block explains why the value is
/// what it is and names `RUST_LOG` four times in prose; a text search would
/// pass on the explanation alone, which is the defect
/// `the_demo_addresses_minio_over_host_docker_internal` was faulted for.
///
/// KILLS: deleting the `RUST_LOG` env entry from the controller container;
/// setting it to a level below `info`, at which the per-object lines — the
/// exit code, the two evidence keys, the verification verdict and its matched
/// key id — are lost; and editing `deployment.yaml` without re-running
/// `scripts/render-install.sh`, which is what a stranger actually applies.
#[test]
fn the_deployment_sets_a_log_level() {
    let text = read("config/manager/deployment.yaml");
    let doc: Value = {
        // The file leads with a comment block and one `---`.
        let body = text.split_once("\n---\n").map_or(text.as_str(), |(_, b)| b);
        serde_yaml::from_str(body).expect("the Deployment is YAML")
    };
    let containers = doc
        .pointer("/spec/template/spec/containers")
        .and_then(Value::as_array)
        .expect("the PodSpec carries containers");
    assert_eq!(
        containers.len(),
        1,
        "one container, so `containers/0` below is THE controller and not whichever one sorts \
         first"
    );
    let env = containers[0]
        .pointer("/env")
        .and_then(Value::as_array)
        .expect("the controller container carries an env block");
    let level = env
        .iter()
        .find(|e| e.pointer("/name") == Some(&json!("RUST_LOG")))
        .and_then(|e| e.pointer("/value"))
        .and_then(Value::as_str)
        .unwrap_or_else(|| {
            panic!(
                "the controller container must pin `RUST_LOG`. Without it the shipped install \
                 logs NOTHING — measured, exit 0 and zero bytes from `kubectl logs \
                 deploy/weirkeeper`. Got: {env:?}"
            )
        });
    assert_eq!(
        level, "info",
        "`info` is the FLOOR and not a preference: below it the exit code, the two evidence \
         keys, the verification verdict and its matched key id are all dropped, and those are \
         what correlate an object with the archive it read. Raise it at run time with `kubectl \
         set env deploy/weirkeeper RUST_LOG=debug`."
    );

    // …AND IN `logweir.yaml`, WHICH IS THE FILE A STRANGER APPLIES.
    // `scripts/render-install.sh` is its only producer, so a Deployment edit
    // that did not reach it would ship the silent controller anyway.
    let install = read("logweir.yaml");
    let rendered: Vec<&str> = install
        .split("\n---")
        .filter(|d| d.contains("kind: Deployment"))
        .collect();
    assert_eq!(rendered.len(), 1, "one Deployment in the rendered install");
    let deployment: Value =
        serde_yaml::from_str(rendered[0].trim_start_matches("\n-")).expect("it is YAML");
    let rendered_level = deployment
        .pointer("/spec/template/spec/containers/0/env")
        .and_then(Value::as_array)
        .and_then(|env| {
            env.iter()
                .find(|e| e.pointer("/name") == Some(&json!("RUST_LOG")))
        })
        .and_then(|e| e.pointer("/value"))
        .cloned();
    assert_eq!(
        rendered_level,
        Some(json!("info")),
        "the rendered install carries the same level; re-run `./scripts/render-install.sh`"
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
/// The recipe body runs from its recipe line to the first unindented,
/// non-blank line after it — the comment block above Task 25's `ui` recipe,
/// which follows `k8s-demo` since Task 25 landed. Reading to the end of the
/// file, as this helper did while `k8s-demo` was the last recipe, would hand
/// every row below `ui`'s 2,500-byte block too, and a token planted in that
/// block would satisfy an assertion about the demo (Task 24's re-review).
fn k8s_demo_recipe() -> String {
    let text = justfile();
    let start = text
        .find("\nk8s-demo:")
        .expect("`just k8s-demo` is a recipe in the justfile");
    let mut body = String::new();
    for (i, line) in text[start + 1..].lines().enumerate() {
        let indented = line.starts_with(' ') || line.starts_with('\t');
        if i > 0 && !indented && !line.is_empty() {
            break;
        }
        body.push_str(line);
        body.push('\n');
    }
    format!("{body}\n{}", read("scripts/k8s-demo.sh"))
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
         the WHOLE reference, so the shipped `docker.io/vladyslavhaina/…@sha256:…` references start on \
         this node only after the local images are tagged with them (plan erratum E19(b))"
    );
}

/// **THE DEMO READS THE `Restore`'s VERDICT BY ITS REAL FIELD PATHS, AND
/// CAPTURES ONLY THE VALUE** — Task 24 defect 5, and the row review finding 2
/// says was a comment rather than a test.
///
/// # Two measurements, one helper
///
/// 1. **The capture.** `read_field` printed its human-readable transcript line
///    on STDOUT and then printed the value, so `v=$(read_field …)` captured
///    BOTH: the exit-criterion assertion compared
///    `"    rc=0  status.exitCode: 0\n0"` against `"0"` and refused a run that
///    had in fact passed. The fix is not a quieter helper — the log still
///    carries both streams — it is that the transcript line goes to STDERR and
///    the value is the only thing on stdout.
/// 2. **The fields.** A demo that reads the right object with the wrong
///    jsonpath prints `<absent>` and passes: `kubectl get -o jsonpath` exits 0
///    for a path that matches nothing. Every field the exit criterion depends
///    on is therefore named here, as the exact expression the script must use.
///
/// The recipe itself is NOT changed by this row — the transcript in
/// `e2e/k8s/phase-b-demo.md` records what the shipped text did, and a demo
/// edited after the fact is a demo nobody ran.
///
/// KILLS: reverting any of the five expressions to a field that does not exist
/// or to the wrong one (`.status.result` for `.status.outcome`, a
/// `verification.result` read off `.status` rather than `.status.evidence`);
/// and moving `read_field`'s transcript line back onto stdout, which puts the
/// line inside the value again.
#[test]
fn the_demo_reads_the_restore_verdict_by_field_and_captures_only_the_value() {
    let recipe = k8s_demo_recipe();

    // ---- 1. THE FIVE EXPRESSIONS THE EXIT CRITERION RESTS ON ------------
    //
    // `.status.phase` and `.status.exitCode` are the run; `.status.outcome` is
    // interface I21's `Restore` rule — the `Backup` rule reads `exitCode` and
    // the two are NOT one rule; the two `verification` fields are this task's
    // whole increment.
    for jsonpath in [
        "{.status.phase}",
        "{.status.exitCode}",
        "{.status.outcome}",
        "{.status.evidence.verification.result}",
        "{.status.evidence.verification.matchedKeyId}",
    ] {
        let at = recipe.find(jsonpath).unwrap_or_else(|| {
            panic!(
                "`just k8s-demo` must read the Restore's `{jsonpath}` — a jsonpath that matches \
                 nothing exits 0 and prints the empty string, so a demo reading the wrong field \
                 PASSES while proving nothing"
            )
        });
        // …and it is read ABOUT THE RESTORE. A `Backup`-only reading of
        // `verification.result` would satisfy a bare substring search while
        // leaving the `Restore` half — the approved, signed, scored one —
        // unchecked.
        let restore_reads: Vec<&str> = recipe
            .lines()
            .filter(|l| l.contains(jsonpath) && l.contains("restore demo-restore"))
            .collect();
        assert!(
            !restore_reads.is_empty(),
            "`{jsonpath}` appears at {at} but never on a line that reads `restore demo-restore`"
        );
    }

    // ---- 2. THE CAPTURE SHAPE -------------------------------------------
    let helper = recipe
        .split_once("read_field() {")
        .map(|(_, rest)| rest.split_once("\n}").map_or(rest, |(body, _)| body))
        .expect("the demo reads its fields through one `read_field` helper");
    assert!(
        helper.contains(">&2"),
        "THE MEASURED DEFECT: `read_field`'s transcript line must go to STDERR. On stdout, \
         `v=$(read_field …)` captures the line INSIDE the value — measured, the exit-criterion \
         check compared `\"    rc=0  status.exitCode: 0\\n0\"` against `\"0\"` and refused a run \
         that had passed. Body:\n{helper}"
    );
    let value_writes: Vec<&str> = helper
        .lines()
        .map(str::trim)
        .filter(|l| (l.starts_with("echo") || l.starts_with("printf")) && !l.contains(">&2"))
        .collect();
    assert_eq!(
        value_writes.len(),
        1,
        "EXACTLY ONE thing reaches stdout from this helper, and it is the value. Everything a \
         human reads is on stderr. Got: {value_writes:?}"
    );
    assert!(
        value_writes[0].starts_with("printf"),
        "…and it is written with `printf`, not `echo`: `echo` appends a newline, which a `$(…)` \
         strips but a `[ \"$v\" = \"0\" ]` against a multi-line value would not. Got: {}",
        value_writes[0]
    );

    // ---- 3. EVERY OTHER CAPTURE IS A BARE `kubectl`, AND THAT IS THE RULE
    //
    // The polling loops capture `$(kubectl … -o jsonpath=…)` directly, which is
    // SAFE — a bare `kubectl` writes the value and nothing else. What is not
    // safe is capturing anything that also says something to a human, which is
    // precisely what `read_field` was before the fix. So: a captured command
    // is either `kubectl` itself or `read_field`, and nothing in between.
    let captures: Vec<&str> = recipe
        .lines()
        .map(str::trim)
        .filter(|l| l.contains("=$(") && l.contains("demo-restore"))
        .collect();
    assert!(
        !captures.is_empty(),
        "the demo captures Restore fields at all"
    );
    for line in &captures {
        let inner = line.split_once("=$(").map(|(_, r)| r).unwrap_or_default();
        assert!(
            inner.starts_with("kubectl") || inner.starts_with("read_field"),
            "a captured command is either `kubectl` — which writes the value and nothing else — \
             or `read_field`, whose stream split is the fix. Anything in between is a human-\
             readable line ending up INSIDE the value, which is the measured defect. Got: {line}"
        );
    }
}

// ===========================================================================
// PLAT-19.1 — the trust lifecycle wired into evidence verification (D3 §7.4)
//
// The rows D3 §13 names for PLAT-19.1, minus the ones W1 already proved. W1's
// `crates/logweir-core/tests/trust_core.rs` covers §7.4's table over the PURE
// `decide`, `may_sign_new`, `claimed_signing_time` and the `unknown` rule, and
// `crates/weirkeeper/tests/trust_policy_controller.rs` covers resolution order,
// the conflict refusal, `boundNamespaces` and the legacy synthesis's SHAPE.
// What is below is what only this layer can answer: the same table reached
// THROUGH a real signature over real signed fixtures, the re-trust pass in both
// directions, the usage-separation refusal at the point a key is offered to a
// verifier, and the legacy verdicts compared against the pre-change ones.
// ===========================================================================

/// A `TrustPolicy` naming `namespaces`, carrying `keys`.
fn policy(name: &str, namespaces: &[&str], keys: Vec<SpecKey>) -> TrustPolicy {
    TrustPolicy {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            uid: Some(format!("uid-{name}")),
            generation: Some(4),
            resource_version: Some("17".to_string()),
            ..ObjectMeta::default()
        },
        spec: TrustPolicySpec {
            default: namespaces.is_empty(),
            namespaces: (!namespaces.is_empty())
                .then(|| namespaces.iter().map(|n| (*n).to_string()).collect()),
            allowed_target_cluster_ids: None,
            keys,
        },
        status: None,
    }
}

/// One policy key over the fixture runner's PUBLIC half, `Active` and open
/// until 2099 — so "is this key current" does not become "what year is it".
fn policy_key(key_id: &str, usages: Vec<SpecUsage>) -> SpecKey {
    SpecKey {
        key_id: key_id.to_string(),
        spki_pem: runner_public_key(),
        algorithm: KeyAlgorithm::Ed25519,
        usages,
        principal: KeyPrincipal {
            id: format!("install:{key_id}"),
            display: None,
        },
        not_before: at("2026-01-01T00:00:00Z"),
        not_after: at("2099-01-01T00:00:00Z"),
        state: KeyState::Active,
        retired_at: None,
        revoked_at: None,
        revocation_reason: None,
        revocation_effective_from: None,
    }
}

/// The fixture signing key, declared for evidence.
fn evidence_key() -> SpecKey {
    policy_key(FIXTURE_KEY_ID, vec![SpecUsage::EvidenceSigning])
}

fn at(s: &str) -> chrono::DateTime<Utc> {
    chrono::DateTime::parse_from_rfc3339(s)
        .expect("a fixture instant")
        .with_timezone(&Utc)
}

/// One edit to a policy key, as a table row carries it — a boxed closure so a
/// table can hold rows that change different fields.
type KeyMutation = Box<dyn Fn(&mut SpecKey)>;

/// The resolved trust one policy produces.
fn resolved(policy: &TrustPolicy) -> ResolvedTrust {
    weirkeeper::trust::from_policy(policy)
}

/// The scorecard fixture, its digest, and a tree holding it — the three things
/// every verification below needs.
struct Signed {
    tree: EvidenceTree,
    digest: String,
}

impl Signed {
    fn scorecard(label: &str) -> Self {
        let payload = read("e2e/fixtures/signed/scorecard.json");
        let sidecar = read("e2e/fixtures/signed/scorecard.sig");
        let digest = sha256_prefixed(payload.as_bytes());
        Self {
            tree: EvidenceTree::new(label, payload.as_bytes(), sidecar.as_bytes()),
            digest,
        }
    }

    /// `verify_evidence` over this tree, against `trust`.
    fn verify(&self, trust: &ResolvedTrust) -> VerificationResult {
        let store = self.tree.handle();
        verify_evidence(
            Some(&store),
            trust,
            PAYLOAD_KEY,
            &self.digest,
            SIDECAR_KEY,
            logweir_verify::PAYLOAD_TYPE_SCORECARD,
        )
    }
}

/// The claim the scorecard fixture makes about when it was signed:
/// `phases[last].at`. Asserted against the live reader rather than pasted, so
/// this constant cannot drift away from the file.
const SCORECARD_SIGNED_AT: &str = "2026-09-03T09:00:00Z";

/// The `detail` the pre-change roster walk produced for a `signingKeys` entry
/// whose PEM does not parse at all — the LAST error's `Display`, prefixed with
/// the entry's declared `keyId`.
const WRONG_KEY_DETAIL: &str = "wrong: key error: not a P-256 or Ed25519 public key: unknown/unsupported algorithm OID: 1.3.101.112";

/// The fixture's claimed signing time is what `logweir_core` reads out of it.
///
/// A FIXTURE ASSERTION, and the reason every row below can be written against
/// one instant. If the checked-in scorecard ever changes its last phase, this
/// is the test that says so — rather than eight window comparisons quietly
/// moving to the other side of a boundary.
#[test]
fn the_fixture_scorecards_claimed_signing_time_is_its_last_phase() {
    let json: Value = serde_json::from_str(&read("e2e/fixtures/signed/scorecard.json"))
        .expect("the fixture scorecard is JSON");
    assert_eq!(
        logweir_core::trust::claimed_signing_time(logweir_verify::PAYLOAD_TYPE_SCORECARD, &json),
        Some(at(SCORECARD_SIGNED_AT)),
        "D3 §7.4 reads a scorecard's signing time from `phases[last].at`"
    );
}

/// **Overlap (§13 row 1, §7.6 step 1).** Two `Active` `EvidenceSigning` keys
/// both verify, and the one that MATCHED is the one reported.
///
/// KILLS: "stop at the first key in the list" — the outsider is declared first,
/// so a verifier that gave up after one would report `Invalid` for a document
/// the second key genuinely signed, and a rotation window would be a trust gap
/// instead of an overlap.
#[test]
fn a_rotation_overlap_verifies_under_either_active_key() {
    let signed = Signed::scorecard("overlap");
    let outsider = read("e2e/fixtures/signed/public.pem").replace("D39", "D40");
    let mut first = policy_key("outsider", vec![SpecUsage::EvidenceSigning]);
    first.spki_pem = outsider;

    for keys in [
        vec![first.clone(), evidence_key()],
        vec![evidence_key(), first],
    ] {
        let trust = resolved(&policy("org-default", &[], keys));
        let r = signed.verify(&trust);
        assert_eq!(
            r.result,
            VerificationVerdict::Valid,
            "both keys are Active and one of them signed this; got {r:?}"
        );
        assert_eq!(
            r.matched_key_id.as_deref(),
            Some(FIXTURE_KEY_ID),
            "the key that MATCHED is reported, never the first one tried"
        );
        let block = r.to_status_value(None);
        assert_eq!(block["trust"]["basis"], json!("Current"));
        assert_eq!(block["trust"]["keyState"], json!("Active"));
        assert_eq!(block["trust"]["policy"]["name"], json!("org-default"));
        assert_eq!(block["trust"]["policy"]["uid"], json!("uid-org-default"));
        assert_eq!(
            block["trust"]["policy"]["generation"],
            json!(4),
            "the REVISION that answered, so a later generation is recognisably a different answer"
        );
        assert_eq!(block["signedAt"], json!(SCORECARD_SIGNED_AT));
    }
}

/// **Retirement and revocation (§13 rows 2 and 3), through a real signature.**
///
/// D3 §7.4's table, every row this layer can reach, asserted on the STATUS
/// BLOCK a console reads rather than on the pure verdict — because the pure
/// verdict is W1's and what was missing was the projection.
///
/// KILLS, one per row:
///  * "treat `Retired` like `Active`" — row 2 would render `Current`;
///  * "compare the claim against `notAfter` and ignore `retiredAt`" — row 3
///    would render `Historical` for a document signed AFTER the retirement,
///    which is the whole point of `accepted_through()` taking the earliest
///    bound;
///  * "treat every revocation as a retirement" — row 6 would render
///    `Historical` for a compromised key;
///  * "read the DOCUMENT's claim on the compromise rows" — row 6's claim is
///    inside every window, so a verifier that consulted it would render green
///    over the one key nobody should trust.
#[test]
fn retirement_verifies_historically_and_compromise_never_does() {
    let signed = Signed::scorecard("retire-revoke");
    let before = at("2026-08-01T00:00:00Z"); // before the fixture's claim
    let after = at("2026-09-10T00:00:00Z"); // after it

    // (label, mutation, expected result, expected basis, expected keyState)
    let rows: Vec<(&str, KeyMutation, &str, &str, &str)> = vec![
        (
            "active",
            Box::new(|_: &mut SpecKey| {}),
            "Valid",
            "Current",
            "Active",
        ),
        (
            "retired after it signed",
            Box::new(move |k: &mut SpecKey| {
                k.state = KeyState::Retired;
                k.retired_at = Some(after);
            }),
            "Valid",
            "Historical",
            "Retired",
        ),
        (
            "retired BEFORE it signed",
            Box::new(move |k: &mut SpecKey| {
                k.state = KeyState::Retired;
                k.retired_at = Some(before);
            }),
            "Untrusted",
            "None",
            "Retired",
        ),
        (
            "expired after it signed",
            Box::new(move |k: &mut SpecKey| k.not_after = after),
            "Valid",
            "Historical",
            "Expired",
        ),
        (
            "expired BEFORE it signed",
            Box::new(move |k: &mut SpecKey| k.not_after = before),
            "Untrusted",
            "None",
            "Expired",
        ),
        (
            "revoked as Superseded, effective after it signed",
            Box::new(move |k: &mut SpecKey| {
                k.state = KeyState::Revoked;
                k.revoked_at = Some(after);
                k.revocation_reason = Some(RevocationReason::Superseded);
                k.revocation_effective_from = Some(after);
            }),
            "Valid",
            "Historical",
            "Revoked",
        ),
        (
            "revoked for compromise, no independent observation",
            Box::new(move |k: &mut SpecKey| {
                k.state = KeyState::Revoked;
                k.revoked_at = Some(after);
                k.revocation_reason = Some(RevocationReason::KeyCompromise);
                k.revocation_effective_from = Some(after);
            }),
            "Untrusted",
            "None",
            "Revoked",
        ),
        (
            "staged for a rotation that has not started",
            Box::new(|k: &mut SpecKey| k.not_before = at("2099-01-01T00:00:00Z")),
            "Untrusted",
            "None",
            "Active",
        ),
    ];

    for (label, mutate, want_result, want_basis, want_key_state) in rows {
        let mut key = evidence_key();
        mutate(&mut key);
        let trust = resolved(&policy("org-default", &[], vec![key]));
        let block = signed.verify(&trust).to_status_value(None);
        assert_eq!(
            block["result"],
            json!(want_result),
            "{label}: D3 §7.4's row says {want_result}; got {block}"
        );
        assert_eq!(block["trust"]["basis"], json!(want_basis), "{label}");
        assert_eq!(block["trust"]["keyState"], json!(want_key_state), "{label}");
        // A SIGNATURE ALWAYS HAPPENED. Every row above verified cryptographically
        // — the difference between them is entirely the key's lifecycle — so
        // none of them may be `Invalid` and all of them name the key.
        assert_ne!(block["result"], json!("Invalid"), "{label}");
        assert_eq!(block["matchedKeyId"], json!(FIXTURE_KEY_ID), "{label}");
        if want_result == "Untrusted" {
            let detail = block["detail"].as_str().unwrap_or_default();
            assert!(
                detail.contains(FIXTURE_KEY_ID),
                "{label}: an Untrusted names the key an operator must act on; got {detail}"
            );
        } else {
            assert!(
                block["detail"].is_null(),
                "{label}: a Valid explains nothing"
            );
        }
    }
}

/// **The compromise row that DOES read an observation**, and the two rules
/// that make it safe.
///
/// D3 §7.4: a compromise-revoked key with a controller-written observation from
/// before the revocation renders `RecordedBeforeRevocation` — `Untrusted`, with
/// the instant shown, never green. The observation is
/// `status.evidence.verification.verifiedAt` from an EARLIER reconcile and
/// nothing else.
///
/// KILLS: "pass the document's own claimed signing time as the observation" —
/// the fixture's claim (2026-09-03) is before the revocation, so that mutation
/// would render `RecordedBeforeRevocation` for an archive this installation
/// never saw, which is exactly the attacker-controlled input §7.4 refuses.
#[test]
fn only_a_controller_written_observation_reaches_recorded_before_revocation() {
    let signed = Signed::scorecard("compromise-observed");
    let effective = at("2026-09-10T00:00:00Z");
    let mut key = evidence_key();
    key.state = KeyState::Revoked;
    key.revoked_at = Some(effective);
    key.revocation_reason = Some(RevocationReason::KeyCompromise);
    key.revocation_effective_from = Some(effective);
    let trust = resolved(&policy("org-default", &[], vec![key]));
    let result = signed.verify(&trust);

    // NO HISTORY — an imported archive. Fails closed (D3 §16).
    let fresh = result.to_status_value(None);
    assert_eq!(fresh["result"], json!("Untrusted"));
    assert_eq!(fresh["trust"]["basis"], json!("None"));
    assert!(
        fresh["detail"]
            .as_str()
            .is_some_and(|d| d.contains("no record of having seen")),
        "an imported archive with a compromised signer names the missing history: {fresh}"
    );

    // THIS INSTALLATION SAW IT, BEFORE THE REVOCATION.
    let observed = json!({
        "result": "Valid",
        "matchedKeyId": FIXTURE_KEY_ID,
        "payloadType": logweir_verify::PAYLOAD_TYPE_SCORECARD,
        "verifiedAt": "2026-09-04T00:00:00Z",
    });
    let recorded = result.to_status_value(Some(&observed));
    assert_eq!(
        recorded["result"],
        json!("Untrusted"),
        "RecordedBeforeRevocation is a kind of Untrusted and is NEVER green: {recorded}"
    );
    assert_eq!(
        recorded["trust"]["basis"],
        json!("RecordedBeforeRevocation"),
        "the basis is what the console renders the recorded instant beside"
    );
    assert!(!backup_badge(&renderable(recorded.clone(), json!({"exitCode": 0}))).green);

    // …AND AN OBSERVATION FROM AFTER THE REVOCATION IS NOT CORROBORATION.
    let late = json!({
        "result": "Valid",
        "matchedKeyId": FIXTURE_KEY_ID,
        "payloadType": logweir_verify::PAYLOAD_TYPE_SCORECARD,
        "verifiedAt": "2026-09-11T00:00:00Z",
    });
    assert_eq!(
        result.to_status_value(Some(&late))["trust"]["basis"],
        json!("None"),
        "an observation made after the revocation took effect says nothing about the document's \
         standing before it"
    );
}

/// **Usage separation (§7.3).** An evidence key is never accepted for a
/// console confirmation and a confirmation key is never accepted for evidence.
///
/// THE FILTER IS AT THE POINT THE KEY IS OFFERED. The material below is the
/// same public key in all three rows — only the declared `usages` differ — so
/// nothing but the usage decides, and a verifier that ignored it would return
/// `Valid` for every one of them.
///
/// KILLS: "offer every resolved key to `verify_detached`" (rows 2 and 3 would
/// verify) and "check the usage only inside `decide`" (the verdict would be
/// `Untrusted/KeyUsageMismatch` instead of `NotAttempted`, which is a different
/// and wronger sentence: no key was offered, so no verdict about a signer was
/// reached).
#[test]
fn an_evidence_key_is_never_accepted_for_a_confirmation_and_the_reverse() {
    let signed = Signed::scorecard("usage-separation");
    for (usage, verifies) in [
        (SpecUsage::EvidenceSigning, true),
        (SpecUsage::ConsoleConfirmation, false),
        (SpecUsage::GovernedApproval, false),
    ] {
        let trust = resolved(&policy(
            "org-default",
            &[],
            vec![policy_key(FIXTURE_KEY_ID, vec![usage])],
        ));
        let r = signed.verify(&trust);
        if verifies {
            assert_eq!(r.result, VerificationVerdict::Valid, "{usage:?}");
        } else {
            assert_eq!(
                r.result,
                VerificationVerdict::NotAttempted,
                "{usage:?} is not an evidence usage, so that key is never offered to the verifier \
                 at all — and the same key material declared EvidenceSigning verifies, which is \
                 what proves the usage is the only difference. Got {r:?}"
            );
            assert_eq!(r.detail.as_deref(), Some(NO_POLICY_KEYS_DETAIL));
            assert_eq!(r.matched_key_id, None);
        }
    }
}

/// **Upgrade from the default roster (§13 row 8, §7.5).** The synthesised
/// `legacy-roster-v1` reaches the same verdict, the same `matchedKeyId` and the
/// same badge as the pre-change roster walk did.
///
/// # What "byte-for-byte" means here, exactly
///
/// D3 §7.4's status block is **additive**: `signedAt` and `trust` are new keys
/// beside the old ones. So the claim under test is that every field that
/// existed before this change carries the same value it carried before, and
/// that the badge is identical — not that the block has the same key set,
/// which §7.4 explicitly changes.
///
/// The expected values below are the pre-change output, written out: `result`,
/// `matchedKeyId`, `payloadType` and `detail` as `verify_evidence` produced them
/// against a `TrustRosterSpec`.
///
/// KILLS: "synthesise `notBefore` as now" (the `Valid` row becomes
/// `Untrusted/SignedOutsideValidity`, because the fixture signed in the past);
/// "synthesise `state: Retired`" (the basis becomes `Historical`); "report the
/// policy name for the legacy source" (`trustSource` stops being
/// `legacy-roster-v1` and L10 has nothing to assert).
#[test]
fn the_legacy_synthesis_reaches_the_pre_change_verdicts() {
    let signed = Signed::scorecard("legacy-equivalence");
    let good = entry(FIXTURE_KEY_ID, &runner_public_key());
    let wrong = entry(
        "wrong",
        &read("e2e/fixtures/signed/public.pem").replace("D39", "D40"),
    );
    let unparseable = entry(
        "unparseable",
        "-----BEGIN PUBLIC KEY-----\nbm90IGEga2V5\n-----END PUBLIC KEY-----\n",
    );

    // (label, signingKeys, the pre-change block, minus verifiedAt)
    let rows: Vec<(&str, Vec<KeyEntry>, Value)> = vec![
        (
            "no key material at all",
            vec![],
            json!({
                "result": "NotAttempted",
                "payloadType": logweir_verify::PAYLOAD_TYPE_SCORECARD,
                "detail": NO_SIGNING_KEYS_DETAIL,
            }),
        ),
        (
            "the runner's public key",
            vec![good.clone()],
            json!({
                "result": "Valid",
                "matchedKeyId": FIXTURE_KEY_ID,
                "payloadType": logweir_verify::PAYLOAD_TYPE_SCORECARD,
            }),
        ),
        (
            "a declared id that is not the material's own hash",
            vec![entry("nightly-runner-2026", &runner_public_key())],
            json!({
                "result": "Valid",
                "matchedKeyId": "nightly-runner-2026",
                "payloadType": logweir_verify::PAYLOAD_TYPE_SCORECARD,
            }),
        ),
        (
            "one unparseable entry, SKIPPED, and a good one after it",
            vec![unparseable.clone(), good],
            json!({
                "result": "Valid",
                "matchedKeyId": FIXTURE_KEY_ID,
                "payloadType": logweir_verify::PAYLOAD_TYPE_SCORECARD,
            }),
        ),
        (
            "key material that does not verify",
            vec![wrong],
            json!({
                "result": "Invalid",
                "payloadType": logweir_verify::PAYLOAD_TYPE_SCORECARD,
                "detail": WRONG_KEY_DETAIL,
            }),
        ),
        // ---- THE ONE ROSTER THIS CHANGE ALTERS (review finding F3) --------
        //
        // Every row above uses an entry with NO `notAfter`, which synthesises
        // to 9999 — so none of them can see the tightening the report records:
        // `verify_evidence` never consulted a signing key's `notAfter`, and
        // under §7.4 it now does. A roster that SETS one is therefore both the
        // only roster whose verdict moves and the only roster the byte-for-byte
        // fixture could not observe, which is the definition of an uncompared
        // behaviour change.
        //
        // Signed BEFORE the expiry: still a pass, on a `Historical` basis.
        (
            "a signing key whose notAfter is after the signing time",
            vec![entry_expiring(
                FIXTURE_KEY_ID,
                &runner_public_key(),
                "2026-09-10T00:00:00Z",
            )],
            json!({
                "result": "Valid",
                "matchedKeyId": FIXTURE_KEY_ID,
                "payloadType": logweir_verify::PAYLOAD_TYPE_SCORECARD,
            }),
        ),
        // Signed AFTER it: the deliberate tightening, and it is `Untrusted`
        // rather than `Invalid` because the bytes are exactly what they claim.
        (
            "a signing key whose notAfter is BEFORE the signing time",
            vec![entry_expiring(
                FIXTURE_KEY_ID,
                &runner_public_key(),
                "2026-08-01T00:00:00Z",
            )],
            json!({
                "result": "Untrusted",
                "matchedKeyId": FIXTURE_KEY_ID,
                "payloadType": logweir_verify::PAYLOAD_TYPE_SCORECARD,
            }),
        ),
    ];

    for (label, keys, want) in rows {
        let block = signed.verify(&roster(keys)).to_status_value(None);
        for (field, value) in want.as_object().expect("the expectation is an object") {
            assert_eq!(
                &block[field], value,
                "{label}: `{field}` must carry exactly what the roster walk produced; got {block}"
            );
        }
        for field in ["result", "matchedKeyId", "payloadType", "detail"] {
            if want.get(field).is_none() && block["result"] != json!("Untrusted") {
                assert!(
                    block[field].is_null(),
                    "{label}: `{field}` was absent before this change and must stay absent; got \
                     {block}"
                );
            }
        }
        // …AND THE SOURCE NAMES ITSELF, which is what D3 §15's L10 reads.
        if block["result"] == json!("Valid") {
            assert_eq!(
                block["trust"]["policy"]["name"],
                json!("legacy-roster-v1"),
                "{label}: an unmigrated cluster says so on every verdict"
            );
            assert!(
                block["trust"]["policy"]["uid"].is_null(),
                "{label}: the synthesised policy is not an object, so it has no uid to name"
            );
        }
        // THE BADGE, AND NOT ONLY THE BLOCK. The doc comment above claims the
        // badge is identical; until finding F3 no badge was computed here at
        // all, so the claim rested on nothing.
        let badge = backup_badge(&renderable(block.clone(), json!({"exitCode": 0})));
        assert_eq!(
            badge.green,
            block["result"] == json!("Valid"),
            "{label}: green is `Valid` and the kind's own field, and nothing else; got {badge:?}"
        );
        match label {
            "a signing key whose notAfter is after the signing time" => {
                assert_eq!(
                    block["trust"]["basis"],
                    json!("Historical"),
                    "signed while the key was valid and the window has since closed — a pass"
                );
                assert!(
                    badge.label.contains("signed before that key was retired"),
                    "a Historical badge carries its qualifier; got {}",
                    badge.label
                );
            }
            "a signing key whose notAfter is BEFORE the signing time" => {
                assert_eq!(block["trust"]["basis"], json!("None"));
                assert_eq!(badge.reason, "VerificationUntrusted");
                let detail = block["detail"].as_str().unwrap_or_default();
                assert!(
                    detail.contains("SignedOutsideValidity"),
                    "THE ONE RECORDED DEVIATION FROM §7.5's byte-for-byte claim: today's \
                     `verify_evidence` never consulted a signing key's notAfter, so this \
                     document used to render Valid and now renders Untrusted. It is the \
                     intended §7.4 behaviour and it is the only row where 'byte-for-byte' is \
                     not literally true. Got {detail}"
                );
            }
            // Every other row either verified under an unexpired key (Current)
            // or never reached a key at all (no `trust` block, which is what an
            // `Invalid` and a `NotAttempted` carry).
            _ if block["result"] == json!("Valid") => assert_eq!(
                block["trust"]["basis"],
                json!("Current"),
                "{label}: an unexpired roster key is Current"
            ),
            _ => assert!(
                block["trust"].is_null(),
                "{label}: no signature matched, so there is no signer to have an opinion about; \
                 got {block}"
            ),
        }
    }
}

/// A namespace two policies claim verifies NOTHING — and says which policies.
///
/// KILLS: "pick the first policy" — the fixture's key is on neither side of the
/// conflict, so picking one would still have to produce a verdict, and this
/// asserts that no verdict is produced at all.
#[test]
fn a_contested_namespace_verifies_nothing() {
    let signed = Signed::scorecard("conflict");
    let store = signed.tree.handle();
    let r = weirkeeper::verification::verify_resolved(
        Some(&store),
        &Resolution::Conflict {
            namespace: "team-a".to_string(),
            policies: vec!["org-default".to_string(), "team-a-local".to_string()],
        },
        PAYLOAD_KEY,
        &signed.digest,
        SIDECAR_KEY,
        logweir_verify::PAYLOAD_TYPE_SCORECARD,
    );
    assert_eq!(
        r.result,
        VerificationVerdict::NotAttempted,
        "a disagreement between administrators is not a verdict about a signer; got {r:?}"
    );
    let detail = r.detail.expect("a NotAttempted carries its reason");
    assert!(detail.contains("TrustPolicyConflict"), "{detail}");
    assert!(detail.contains("team-a"), "{detail}");
    assert!(
        detail.contains("org-default") && detail.contains("team-a-local"),
        "{detail}"
    );

    // AND A CLUSTER WITH NEITHER A POLICY NOR A ROSTER READS EXACTLY WHAT IT
    // READS TODAY — `controllers::roster_spec` flattened a 404 into an empty
    // roster, and that sentence must not change under an unmigrated install.
    let r = weirkeeper::verification::verify_resolved(
        Some(&store),
        &Resolution::Unconfigured,
        PAYLOAD_KEY,
        &signed.digest,
        SIDECAR_KEY,
        logweir_verify::PAYLOAD_TYPE_SCORECARD,
    );
    assert_eq!(r.result, VerificationVerdict::NotAttempted);
    assert_eq!(r.detail.as_deref(), Some(NO_SIGNING_KEYS_DETAIL));
}

/// The observation refines the BASIS and never the RESULT.
///
/// The claim [`VerificationResult::signed`]'s doc comment makes, asserted over
/// the whole §7.4 table rather than trusted: `verify_evidence` decides `result`
/// with no observation, and adding one later may only move the two compromise
/// rows between each other.
///
/// KILLS: "read the observation on the retirement rows too" — a stored
/// `verifiedAt` inside a retired key's window would then flip a
/// `SignedOutsideValidity` into a pass.
#[test]
fn the_observation_only_moves_the_basis() {
    let signed = Signed::scorecard("observation-invariant");
    let after = at("2026-09-10T00:00:00Z");
    let mutations: Vec<KeyMutation> = vec![
        Box::new(|_: &mut SpecKey| {}),
        Box::new(move |k: &mut SpecKey| {
            k.state = KeyState::Retired;
            k.retired_at = Some(at("2026-08-01T00:00:00Z"));
        }),
        Box::new(move |k: &mut SpecKey| {
            k.state = KeyState::Retired;
            k.retired_at = Some(after);
        }),
        Box::new(move |k: &mut SpecKey| {
            k.state = KeyState::Revoked;
            k.revoked_at = Some(after);
            k.revocation_reason = Some(RevocationReason::KeyCompromise);
            k.revocation_effective_from = Some(after);
        }),
        Box::new(move |k: &mut SpecKey| {
            k.state = KeyState::Revoked;
            k.revoked_at = Some(after);
            k.revocation_reason = Some(RevocationReason::Superseded);
            k.revocation_effective_from = Some(after);
        }),
    ];
    let observed = json!({
        "result": "Valid",
        "matchedKeyId": FIXTURE_KEY_ID,
        "payloadType": logweir_verify::PAYLOAD_TYPE_SCORECARD,
        "verifiedAt": "2026-09-04T00:00:00Z",
    });
    for (i, mutate) in mutations.into_iter().enumerate() {
        let mut key = evidence_key();
        mutate(&mut key);
        let trust = resolved(&policy("org-default", &[], vec![key]));
        let r = signed.verify(&trust);
        let without = r.to_status_value(None);
        let with = r.to_status_value(Some(&observed));
        assert_eq!(
            without["result"], with["result"],
            "row {i}: an observation may refine the basis and must never change the result — \
             both compromise rows are Untrusted, so nothing it can say makes a document trusted"
        );
        assert_eq!(
            json!(r.result.as_str()),
            without["result"],
            "row {i}: the result `verify_evidence` reports is the one that lands, so the log line \
             and the status cannot disagree"
        );
    }
}

// ---------------------------------------------------------------------------
// The re-trust pass — D3 §7.4, "re-evaluation without re-fetching"
// ---------------------------------------------------------------------------

/// A terminal `Backup` status carrying a `Valid` verdict reached at
/// `verifiedAt`, signed at [`SCORECARD_SIGNED_AT`].
fn verified_status(verified_at: &str) -> Value {
    json!({
        "phase": "Succeeded",
        "exitCode": 0,
        "evidence": {
            "verification": {
                "result": "Valid",
                "matchedKeyId": FIXTURE_KEY_ID,
                "payloadType": logweir_verify::PAYLOAD_TYPE_SCORECARD,
                "signedAt": SCORECARD_SIGNED_AT,
                "verifiedAt": verified_at,
                "trust": {
                    "basis": "Current",
                    "keyState": "Active",
                    "policy": {"name": "org-default", "uid": "uid-org-default", "generation": 4},
                },
            }
        },
    })
}

/// **A key revoked AFTER an approval was granted.** The stored verdict is
/// re-derived from the status alone — no storage read, no signature check — and
/// the object's own facts are untouched.
///
/// KILLS, and each is a separate assertion below:
///  * "re-fetch the document" — there is no `Store` in this test at all;
///  * "rewrite `verifiedAt` with the new clock" — the next pass would then read
///    its own write as the observation and flip `RecordedBeforeRevocation` to
///    `Revoked` forever, which the last arm proves does not happen;
///  * "patch the whole status" — `phase` and `exitCode` are asserted unchanged;
///  * "leave the `Verified` condition alone" — a green condition beside an
///    `Untrusted` result is the silent-revocation failure this task exists to
///    prevent.
#[test]
fn a_revocation_after_approval_downgrades_the_verdict_and_nothing_else() {
    let status = verified_status("2026-09-04T00:00:00Z");
    let effective = at("2026-09-10T00:00:00Z");
    let mut key = evidence_key();
    key.state = KeyState::Revoked;
    key.revoked_at = Some(effective);
    key.revocation_reason = Some(RevocationReason::KeyCompromise);
    key.revocation_effective_from = Some(effective);
    let resolution = Resolution::Trust(Box::new(resolved(&policy("org-default", &[], vec![key]))));
    let existing = vec![
        weirkeeper::crds::Condition {
            r#type: "Complete".to_string(),
            status: "True".to_string(),
            observed_generation: Some(1),
            last_transition_time: Some(at("2026-09-04T00:00:00Z")),
            reason: Some("Succeeded".to_string()),
            message: Some("the run exited 0".to_string()),
        },
        weirkeeper::crds::Condition {
            r#type: CONDITION_VERIFIED.to_string(),
            status: "True".to_string(),
            observed_generation: Some(1),
            last_transition_time: Some(at("2026-09-04T00:00:00Z")),
            reason: Some(REASON_VERIFIED.to_string()),
            message: Some("verified by weirkeeper".to_string()),
        },
    ];
    let now = at("2026-09-12T08:00:00Z");
    let r = weirkeeper::verification::retrust(
        &status,
        &resolution,
        backup_badge,
        Some(&existing),
        Some(1),
        now,
    )
    .expect("a revocation changes the verdict, so a patch is owed");

    assert_eq!(r.from, "Valid");
    assert_eq!(r.to, "Untrusted");
    assert_eq!(r.verification["result"], json!("Untrusted"));
    assert_eq!(
        r.verification["trust"]["basis"],
        json!("RecordedBeforeRevocation"),
        "this installation saw the document on 2026-09-04, before the revocation took effect"
    );
    assert_eq!(r.verification["trust"]["keyState"], json!("Revoked"));

    // WHAT DOES NOT MOVE.
    let stored = status.pointer("/evidence/verification").unwrap();
    for field in ["matchedKeyId", "payloadType", "signedAt", "verifiedAt"] {
        assert_eq!(
            r.verification[field], stored[field],
            "`{field}` is carried verbatim: the re-trust pass re-derives a verdict, it does not \
             re-observe anything"
        );
    }

    // THE PATCH TOUCHES NO RUN FACT.
    let patch = r.patch(&existing);
    let touched: Vec<&String> = patch["status"]
        .as_object()
        .expect("the patch writes a status object")
        .keys()
        .collect();
    assert_eq!(
        touched,
        vec!["evidence", "conditions"],
        "D3 §7.4: the patch carries the verification block and the condition that says the same \
         thing, and never `phase`, `exitCode` or `outcome`. Got {patch}"
    );

    // THE CONDITION MOVES WITH THE VERDICT, AND CARRIES A REASON.
    assert_eq!(r.verified.status, "False");
    assert_eq!(
        r.verified.reason.as_deref(),
        Some("VerificationUntrusted"),
        "a withdrawn verdict is recorded as a withdrawal, not as a corrupt document"
    );
    assert_eq!(
        r.verified.last_transition_time,
        Some(now),
        "the condition CHANGED, so it takes the instant it changed at"
    );
    let types: Vec<&str> = patch["status"]["conditions"]
        .as_array()
        .expect("the whole condition list is carried")
        .iter()
        .filter_map(|c| c["type"].as_str())
        .collect();
    assert!(
        types.contains(&"Complete"),
        "a merge patch REPLACES arrays, so a patch carrying only [Verified] would delete the \
         terminal condition; got {types:?}"
    );

    // …AND A SECOND PASS OVER THE PATCHED OBJECT IS A NO-OP, which is only true
    // because `verifiedAt` did not move. The object it runs against is the one
    // the patch produced: the new block AND the new condition, because the
    // comparison is over both (review finding F6).
    let mut patched = status.clone();
    patched["evidence"]["verification"] = r.verification.clone();
    let patched_conditions: Vec<weirkeeper::crds::Condition> = existing
        .iter()
        .filter(|c| c.r#type != CONDITION_VERIFIED)
        .cloned()
        .chain(std::iter::once(r.verified.clone()))
        .collect();
    assert!(
        weirkeeper::verification::retrust(
            &patched,
            &resolution,
            backup_badge,
            Some(&patched_conditions),
            Some(1),
            at("2026-09-13T08:00:00Z"),
        )
        .is_none(),
        "the verdict is now what the policy says, so nothing is written — and the observation it \
         rests on is still 2026-09-04 rather than the instant of the last write"
    );
}

/// **A key added after an `UntrustedSigner` verdict** — the other direction,
/// which is D3 §15's L7 in miniature.
///
/// An imported archive verified under a key this installation did not carry;
/// an administrator adds that key as `Retired/EvidenceSigning`; the point
/// becomes trusted again on a HISTORICAL basis, with a recorded reason.
///
/// KILLS: "treat an unknown `matchedKeyId` as `Valid`" (the first arm would be
/// green) and "re-grant with no condition change" (the second arm's condition
/// would still say `VerificationUntrusted`).
#[test]
fn a_key_added_after_an_untrusted_verdict_is_re_granted_with_a_reason() {
    // ARM 1 — the namespace resolves to a policy that does not carry the key.
    let status = verified_status("2026-09-04T00:00:00Z");
    let stranger = resolved(&policy(
        "team-b",
        &["team-b"],
        vec![policy_key(
            "some-other-key",
            vec![SpecUsage::EvidenceSigning],
        )],
    ));
    let untrusted = weirkeeper::verification::retrust(
        &status,
        &Resolution::Trust(Box::new(stranger)),
        backup_badge,
        None,
        Some(1),
        at("2026-09-12T08:00:00Z"),
    )
    .expect("a policy that does not carry the signer changes the verdict");
    assert_eq!(untrusted.to, "Untrusted");
    assert_eq!(
        untrusted.verification["trust"]["keyState"],
        json!("Unknown")
    );
    assert!(
        untrusted.verification["detail"]
            .as_str()
            .is_some_and(|d| d.contains("UntrustedSigner")),
        "the reason names the row: {}",
        untrusted.verification["detail"]
    );
    let catalog = weirkeeper::verification::catalog_verification(
        VerificationVerdict::Untrusted,
        Some(&logweir_core::trust::decide(
            None,
            logweir_core::trust::KeyUsage::EvidenceSigning,
            &logweir_core::trust::EvidenceClaim::at(at(SCORECARD_SIGNED_AT)),
            &logweir_core::trust::IndependentObservation::none(),
            at("2026-09-12T08:00:00Z"),
        )),
    );
    assert_eq!(
        catalog.state, "UntrustedSigner",
        "D3 §5.4's word for this row, so the catalog and the badge cannot disagree"
    );
    assert_eq!(
        catalog.reason,
        Some(logweir_core::trust::UntrustReason::UntrustedSigner),
        "…and the ROW travels with the word, so L7's remedy sentence can be the right one"
    );

    // ARM 2 — the administrator adds the key, Retired, with a window that
    // covers the signature.
    let mut downgraded = status.clone();
    downgraded["evidence"]["verification"] = untrusted.verification.clone();
    let mut added = evidence_key();
    added.state = KeyState::Retired;
    added.retired_at = Some(at("2026-09-10T00:00:00Z"));
    let now = at("2026-09-12T09:00:00Z");
    let regranted = weirkeeper::verification::retrust(
        &downgraded,
        &Resolution::Trust(Box::new(resolved(&policy(
            "team-b",
            &["team-b"],
            vec![added],
        )))),
        backup_badge,
        None,
        Some(1),
        now,
    )
    .expect("adding the signer's public half changes the verdict back");
    assert_eq!(regranted.from, "Untrusted");
    assert_eq!(regranted.to, "Valid");
    assert_eq!(
        regranted.verification["trust"]["basis"],
        json!("Historical"),
        "signed while the key was valid, and the key has since been retired — which is what \
         rotation is supposed to look like"
    );
    assert_eq!(regranted.verified.status, "True");
    assert_eq!(regranted.verified.reason.as_deref(), Some(REASON_VERIFIED));
    assert!(
        regranted
            .verified
            .message
            .as_deref()
            .is_some_and(|m| m.contains("signed before that key was retired")),
        "D3 §7.4: a Historical badge carries the qualifier, so nobody has to guess why a green \
         badge names a retired key. Got {:?}",
        regranted.verified.message
    );
    assert_eq!(
        regranted.verification["verifiedAt"],
        json!("2026-09-04T00:00:00Z"),
        "the re-grant does not re-observe either"
    );
}

/// A verification that matched NO key is left alone by the re-trust pass.
///
/// KILLS: "re-derive every stored block" — an `Invalid` from a digest mismatch
/// has no `matchedKeyId`, so a pass that re-derived it would invent a trust
/// verdict about a key that never existed and overwrite a real finding about a
/// real document.
#[test]
fn the_retrust_pass_declines_what_was_never_a_trust_verdict() {
    let policy = Resolution::Trust(Box::new(resolved(&policy(
        "org-default",
        &[],
        vec![evidence_key()],
    ))));
    for stored in [
        json!({"result": "Invalid", "payloadType": "x", "detail": "digest mismatch",
               "verifiedAt": "2026-09-04T00:00:00Z"}),
        json!({"result": "NotAttempted", "payloadType": "x", "detail": NO_CREDENTIAL_DETAIL,
               "verifiedAt": "2026-09-04T00:00:00Z"}),
    ] {
        let status = json!({"phase": "Succeeded", "exitCode": 0,
                            "evidence": {"verification": stored}});
        assert!(
            weirkeeper::verification::retrust(
                &status,
                &policy,
                backup_badge,
                None,
                Some(1),
                at("2026-09-12T08:00:00Z"),
            )
            .is_none(),
            "no key was ever selected, so a policy change has nothing to re-decide: {status}"
        );
    }
    // …and an object with no verification block at all is not a candidate.
    assert!(weirkeeper::verification::retrust(
        &json!({"phase": "Succeeded"}),
        &policy,
        backup_badge,
        None,
        Some(1),
        at("2026-09-12T08:00:00Z"),
    )
    .is_none());
}

/// A policy edit that does not touch this object's signer writes NOTHING.
///
/// Plan erratum **E11(d)**: the comparison is over the rendered block, not over
/// a "did the generation change" flag. One `kubectl apply` on a 64-key policy
/// must not wake every terminal object in the cluster.
///
/// KILLS: "return `Some` whenever the resolved generation differs from the
/// stored one" — the generation below moves from 4 to 9 and the verdict does
/// not, so that mutation would send a patch per object per edit.
#[test]
fn a_policy_edit_that_changes_no_verdict_sends_nothing() {
    let status = verified_status("2026-09-04T00:00:00Z");
    let mut later = policy(
        "org-default",
        &[],
        vec![
            evidence_key(),
            policy_key("added", vec![SpecUsage::EvidenceSigning]),
        ],
    );
    later.metadata.generation = Some(9);
    let resolution = Resolution::Trust(Box::new(resolved(&later)));
    let r = weirkeeper::verification::retrust(
        &status,
        &resolution,
        backup_badge,
        None,
        Some(1),
        at("2026-09-12T08:00:00Z"),
    );
    // The POLICY REF changed (generation 4 -> 9), so a patch IS owed — but it
    // carries the same verdict, and that is the distinction this asserts.
    let r = r.expect("the recorded policy revision moved, which is itself worth recording");
    assert_eq!(r.from, "Valid");
    assert_eq!(r.to, "Valid");
    assert_eq!(r.verification["trust"]["policy"]["generation"], json!(9));
    assert_eq!(r.verification["verifiedAt"], json!("2026-09-04T00:00:00Z"));

    // AND RE-RUNNING AGAINST THE SAME POLICY IS A TRUE NO-OP — over the block
    // AND the condition the first pass wrote (review finding F6).
    let mut patched = status.clone();
    patched["evidence"]["verification"] = r.verification.clone();
    let patched_conditions = vec![r.verified.clone()];
    assert!(
        weirkeeper::verification::retrust(
            &patched,
            &resolution,
            backup_badge,
            Some(&patched_conditions),
            Some(1),
            at("2026-09-30T08:00:00Z"),
        )
        .is_none(),
        "nothing changed, so nothing is sent — a later clock is not a new verdict"
    );
}

/// A trust downgrade does not move `verifiedAt` on the FRESH path either.
///
/// The same rule as the re-trust pass, enforced where the verdict is first
/// projected: the stored instant is the observation, and a verdict that
/// re-confirms the SAME SIGNATURE keeps it whatever the trust layer decides.
///
/// KILLS: "compare `result` and `detail` when deciding whether `verifiedAt`
/// moves" — the pre-PLAT-19.1 rule. Under it, the arm below refreshes the
/// instant to 2026-09-12 and the compromise row's own observation is destroyed.
#[test]
fn a_trust_downgrade_keeps_the_stored_observation() {
    let signed = Signed::scorecard("stable-observation");
    let effective = at("2026-09-10T00:00:00Z");
    let mut key = evidence_key();
    key.state = KeyState::Revoked;
    key.revoked_at = Some(effective);
    key.revocation_reason = Some(RevocationReason::KeyCompromise);
    key.revocation_effective_from = Some(effective);
    let trust = resolved(&policy("org-default", &[], vec![key]));

    let stored = json!({
        "result": "Valid",
        "matchedKeyId": FIXTURE_KEY_ID,
        "payloadType": logweir_verify::PAYLOAD_TYPE_SCORECARD,
        "verifiedAt": "2026-09-04T00:00:00Z",
    });
    let block = signed.verify(&trust).to_status_value(Some(&stored));
    assert_eq!(block["result"], json!("Untrusted"));
    assert_eq!(
        block["verifiedAt"],
        json!("2026-09-04T00:00:00Z"),
        "the same signature by the same key over the same media type is the same observation; \
         only the TRUST answer changed, and recording that must not erase the evidence it rests \
         on. Got {block}"
    );
    assert_eq!(block["trust"]["basis"], json!("RecordedBeforeRevocation"));

    // A DIFFERENT SIGNATURE IS A DIFFERENT OBSERVATION, and takes the new
    // instant — otherwise the rule above would freeze `verifiedAt` forever.
    let other = json!({
        "result": "Valid",
        "matchedKeyId": "a-different-key",
        "payloadType": logweir_verify::PAYLOAD_TYPE_SCORECARD,
        "verifiedAt": "2026-09-04T00:00:00Z",
    });
    assert_ne!(
        signed.verify(&trust).to_status_value(Some(&other))["verifiedAt"],
        json!("2026-09-04T00:00:00Z"),
        "a verdict about a different key is a new observation"
    );
}

/// The catalog's six-word vocabulary (D3 §5.4) and this module's badge cannot
/// disagree about the same key.
///
/// KILLS: "map every `Untrusted` to `UntrustedSigner`" — the revocation rows
/// would stop being distinguishable in the catalog view, which is the one place
/// D3 §15's L8 reads them.
#[test]
fn the_catalog_vocabulary_is_the_same_verdict_in_other_words() {
    use logweir_core::trust::{TrustBasis, TrustKeyState, TrustResult, UntrustReason, Verdict};
    let valid = |basis| Verdict {
        result: TrustResult::Valid,
        basis,
        key_state: TrustKeyState::Active,
        reason: None,
    };
    let untrusted = |reason| Verdict {
        result: TrustResult::Untrusted,
        basis: TrustBasis::None,
        key_state: TrustKeyState::Unknown,
        reason: Some(reason),
    };
    let cases: Vec<(VerificationVerdict, Option<Verdict>, &str)> = vec![
        (
            VerificationVerdict::Valid,
            Some(valid(TrustBasis::Current)),
            "Verified",
        ),
        (
            VerificationVerdict::Valid,
            Some(valid(TrustBasis::Historical)),
            "VerifiedHistorical",
        ),
        (
            VerificationVerdict::Untrusted,
            Some(untrusted(UntrustReason::UntrustedSigner)),
            "UntrustedSigner",
        ),
        (
            VerificationVerdict::Untrusted,
            Some(untrusted(UntrustReason::KeyUsageMismatch)),
            "UntrustedSigner",
        ),
        (
            VerificationVerdict::Untrusted,
            Some(untrusted(UntrustReason::SignedOutsideValidity)),
            "UntrustedSigner",
        ),
        (
            VerificationVerdict::Untrusted,
            Some(untrusted(UntrustReason::Revoked)),
            "Revoked",
        ),
        (
            VerificationVerdict::Untrusted,
            Some(untrusted(UntrustReason::RecordedBeforeRevocation)),
            "Revoked",
        ),
        (VerificationVerdict::Invalid, None, "Invalid"),
        (VerificationVerdict::NotAttempted, None, "NotAttempted"),
    ];
    for (result, verdict, want) in cases {
        let catalog = weirkeeper::verification::catalog_verification(result, verdict.as_ref());
        assert_eq!(catalog.state, want, "{result:?} / {verdict:?}");
        // A GREEN BADGE AND A CATALOG `Verified` ARE THE SAME ANSWER.
        let green = verdict.as_ref().is_some_and(Verdict::may_render_green);
        assert_eq!(
            green,
            want.starts_with("Verified"),
            "{result:?} / {verdict:?}: the badge rule and §5.4's vocabulary must not diverge"
        );
        // …AND THE ROW SURVIVES THE FLATTENING (review finding F4). Three rows
        // become `UntrustedSigner` and two become `Revoked`; §5.4's definitions
        // are true of only one of each, so the remedy an operator is handed can
        // only be right if the reason travels beside the word.
        assert_eq!(
            catalog.reason,
            verdict.as_ref().and_then(|v| v.reason),
            "{result:?} / {verdict:?}: the row that decided is carried verbatim"
        );
        assert_eq!(catalog.basis, verdict.as_ref().map(|v| v.basis));
    }

    // THE TWO LOSSY PAIRS, NAMED. A consumer that renders §5.4's own remedy
    // sentence from `state` alone tells an operator to add a key that is
    // already there, or that nothing was observed when something was.
    let lossy = [
        (UntrustReason::UntrustedSigner, "UntrustedSigner"),
        (UntrustReason::KeyUsageMismatch, "UntrustedSigner"),
        (UntrustReason::SignedOutsideValidity, "UntrustedSigner"),
        (UntrustReason::Revoked, "Revoked"),
        (UntrustReason::RecordedBeforeRevocation, "Revoked"),
    ];
    let mut by_word: std::collections::BTreeMap<&str, usize> = std::collections::BTreeMap::new();
    for (reason, word) in lossy {
        let c = weirkeeper::verification::catalog_verification(
            VerificationVerdict::Untrusted,
            Some(&untrusted(reason)),
        );
        assert_eq!(c.state, word);
        assert_eq!(c.reason, Some(reason));
        *by_word.entry(word).or_default() += 1;
    }
    assert_eq!(
        by_word.get("UntrustedSigner").copied(),
        Some(3),
        "three distinct rows flatten into one word, which is why the word is not enough"
    );
    assert_eq!(by_word.get("Revoked").copied(), Some(2));
}

/// `carry_conditions` carries every type it is given, in order, and never
/// overwrites one the builder already owns.
///
/// KILLS: "carry only the first type" (the second assertion), "carry a type the
/// builder already wrote" (the third — the stored `Verified` would replace the
/// fresh verdict and a changed badge would never land).
#[test]
fn carry_conditions_carries_each_type_it_is_given() {
    let stored = vec![
        weirkeeper::crds::Condition {
            r#type: CONDITION_VERIFIED.to_string(),
            status: "True".to_string(),
            observed_generation: Some(1),
            last_transition_time: Some(at("2026-09-04T00:00:00Z")),
            reason: Some("Stored".to_string()),
            message: Some("stored".to_string()),
        },
        weirkeeper::crds::Condition {
            r#type: "RunnerReady".to_string(),
            status: "True".to_string(),
            observed_generation: Some(1),
            last_transition_time: Some(at("2026-09-04T00:00:00Z")),
            reason: Some("RunnerStarted".to_string()),
            message: Some("started".to_string()),
        },
    ];
    let own = vec![json!({"type": "Complete", "status": "True"})];
    let carried = weirkeeper::verification::carry_conditions(
        Some(&stored),
        own.clone(),
        &[CONDITION_VERIFIED, "RunnerReady"],
    );
    let types: Vec<&str> = carried.iter().filter_map(|c| c["type"].as_str()).collect();
    assert_eq!(
        types,
        vec!["Complete", CONDITION_VERIFIED, "RunnerReady"],
        "the builder's own first, then each carried type in the order given — a steady object has \
         to compute the same array on every pass or `status_unchanged` cannot skip the patch"
    );

    // A TYPE THE BUILDER OWNS IS NOT OVERWRITTEN.
    let fresh = vec![json!({"type": CONDITION_VERIFIED, "status": "False", "reason": "Fresh"})];
    let carried = weirkeeper::verification::carry_conditions(
        Some(&stored),
        fresh,
        &[CONDITION_VERIFIED, "RunnerReady"],
    );
    assert_eq!(
        carried
            .iter()
            .filter(|c| c["type"] == json!(CONDITION_VERIFIED))
            .count(),
        1,
        "one Verified, and it is the builder's"
    );
    assert_eq!(carried[0]["reason"], json!("Fresh"));

    // …AND `carry_verified` IS THIS FUNCTION WITH ONE TYPE.
    assert_eq!(
        weirkeeper::verification::carry_verified(Some(&stored), own.clone()),
        weirkeeper::verification::carry_conditions(Some(&stored), own, &[CONDITION_VERIFIED]),
    );
}

/// An `Untrusted` verdict gets its OWN condition reason, and no badge.
///
/// KILLS: "fold `Untrusted` into the `NotAttempted` arm of `valid_verification`"
/// — the badge would be ungreen either way, so only the reason distinguishes
/// "we did not check" from "we checked and will not accept the signer", and an
/// operator routes on that reason.
#[test]
fn an_untrusted_verdict_is_not_a_verification_that_did_not_happen() {
    let untrusted = json!({
        "result": "Untrusted",
        "matchedKeyId": FIXTURE_KEY_ID,
        "payloadType": logweir_verify::PAYLOAD_TYPE_SCORECARD,
        "verifiedAt": "2026-09-11T03:20:00Z",
        "trust": {"basis": "None", "keyState": "Revoked"},
    });
    for badge in [
        backup_badge(&renderable(untrusted.clone(), json!({"exitCode": 0}))),
        restore_badge(&renderable(untrusted.clone(), json!({"outcome": "pass"}))),
    ] {
        assert!(!badge.green, "a refused signer is never green");
        assert_eq!(badge.reason, "VerificationUntrusted");
        assert_eq!(badge.label, UNVERIFIED);
    }

    // A `Valid` ON A BASIS NOBODY WRITES IS ALSO NOT GREEN — defence in depth
    // for the second half of §7.4's rule, `Valid ∧ (Current|Historical)`.
    let odd = json!({
        "result": "Valid",
        "matchedKeyId": FIXTURE_KEY_ID,
        "payloadType": logweir_verify::PAYLOAD_TYPE_SCORECARD,
        "verifiedAt": "2026-09-11T03:20:00Z",
        "trust": {"basis": "RecordedBeforeRevocation", "keyState": "Revoked"},
    });
    let badge = backup_badge(&renderable(odd, json!({"exitCode": 0})));
    assert!(
        !badge.green,
        "D3 §7.4 spells the green rule as `Valid AND basis Current|Historical`, and the second \
         clause is read as well as the first"
    );

    // AN OBJECT WRITTEN BY A CONTROLLER THAT PREDATES `trust` STAYS GREEN —
    // the block is additive and an absent `trust` is not a downgrade.
    let old = json!({
        "result": "Valid",
        "matchedKeyId": FIXTURE_KEY_ID,
        "payloadType": logweir_verify::PAYLOAD_TYPE_SCORECARD,
        "verifiedAt": "2026-09-11T03:20:00Z",
    });
    assert!(backup_badge(&renderable(old, json!({"exitCode": 0}))).green);
}

// ===========================================================================
// Fix round 1 — the review's findings, each with the row that would have
// caught it
// ===========================================================================

/// **F5.** A stored `verifiedAt` is an observation only when a SIGNATURE
/// produced it.
///
/// Every verdict carries a `verifiedAt`, including the ones that never read the
/// document — a `NotAttempted` written because no credential was configured,
/// because the object could not be fetched, or because two policies contest the
/// namespace; an `Invalid` written because the digest did not match. D3 §7.4's
/// compromise rule rests on "this installation recorded having SEEN this
/// document", and none of those did.
///
/// KILLS: "accept any stored `verifiedAt`" — the pre-fix rule. Under it the
/// first three rows below render `RecordedBeforeRevocation` and put a false
/// provenance sentence in front of an operator about a compromised key.
#[test]
fn a_stored_instant_is_an_observation_only_if_a_signature_produced_it() {
    let signed = Signed::scorecard("observation-provenance");
    let effective = at("2026-09-10T00:00:00Z");
    let mut key = evidence_key();
    key.state = KeyState::Revoked;
    key.revoked_at = Some(effective);
    key.revocation_reason = Some(RevocationReason::KeyCompromise);
    key.revocation_effective_from = Some(effective);
    let result = signed.verify(&resolved(&policy("org-default", &[], vec![key])));

    // An early instant on a block that never read the document.
    let early = "2026-09-04T00:00:00Z";
    let not_observations = [
        json!({"result": "NotAttempted", "payloadType": "p", "detail": NO_CREDENTIAL_DETAIL,
               "verifiedAt": early}),
        json!({"result": "Invalid", "payloadType": "p", "detail": "digest mismatch",
               "verifiedAt": early}),
        // `Valid` with NO matched key is not a shape this controller writes —
        // and it is the shape a hand-edited status could carry, so it is
        // refused rather than trusted.
        json!({"result": "Valid", "payloadType": "p", "verifiedAt": early}),
        json!({"result": "Valid", "matchedKeyId": "", "payloadType": "p", "verifiedAt": early}),
    ];
    for stored in not_observations {
        let block = result.to_status_value(Some(&stored));
        assert_eq!(
            block["result"],
            json!("Untrusted"),
            "a compromise-revoked signer is never trusted: {block}"
        );
        assert_eq!(
            block["trust"]["basis"],
            json!("None"),
            "{stored}: this block is not a record of having seen the document, so it corroborates \
             nothing — the verdict is the fail-closed `Revoked`, not `RecordedBeforeRevocation`"
        );
        assert_eq!(
            weirkeeper::verification::VerificationResult::observation(Some(&stored)),
            logweir_core::trust::IndependentObservation::none(),
            "{stored}: and the seam itself says so, not only the verdict"
        );
    }

    // …AND THE ONE THAT IS. Same instant, same key, a block a signature wrote.
    let observation = json!({
        "result": "Valid",
        "matchedKeyId": FIXTURE_KEY_ID,
        "payloadType": logweir_verify::PAYLOAD_TYPE_SCORECARD,
        "verifiedAt": early,
    });
    assert_eq!(
        result.to_status_value(Some(&observation))["trust"]["basis"],
        json!("RecordedBeforeRevocation"),
        "the ONLY difference between this and the rows above is that a signature produced it"
    );
}

/// **F6.** A `Verified` condition inconsistent with an already-correct
/// verification block is repaired.
///
/// The header always claimed the comparison was over the rendered block AND the
/// rendered condition; the code compared the block alone. A condition clobbered
/// by another builder writing the array, or left behind by a partially applied
/// patch, was therefore never repaired — `retrust` saw an unchanged block and
/// sent nothing.
///
/// KILLS: `if &verification == stored { return None; }` (the pre-fix form).
#[test]
fn the_retrust_pass_repairs_a_condition_that_contradicts_its_own_block() {
    // The block is ALREADY what the policy says: an `Untrusted` verdict with
    // the right basis. Only the condition is wrong.
    let mut status = verified_status("2026-09-04T00:00:00Z");
    let effective = at("2026-09-10T00:00:00Z");
    let mut key = evidence_key();
    key.state = KeyState::Revoked;
    key.revoked_at = Some(effective);
    key.revocation_reason = Some(RevocationReason::KeyCompromise);
    key.revocation_effective_from = Some(effective);
    let resolution = Resolution::Trust(Box::new(resolved(&policy("org-default", &[], vec![key]))));
    let now = at("2026-09-12T08:00:00Z");

    // One pass to reach the settled block.
    let settled =
        weirkeeper::verification::retrust(&status, &resolution, backup_badge, None, Some(1), now)
            .expect("the revocation changes the verdict");
    status["evidence"]["verification"] = settled.verification.clone();

    // With the condition the pass itself produced, a second pass is a no-op.
    let matching = vec![settled.verified.clone()];
    assert!(
        weirkeeper::verification::retrust(
            &status,
            &resolution,
            backup_badge,
            Some(&matching),
            Some(1),
            at("2026-09-13T08:00:00Z"),
        )
        .is_none(),
        "block and condition both agree with the policy, so nothing is sent (E11(d))"
    );

    // With a STALE condition beside the same correct block, it is repaired.
    let stale = vec![weirkeeper::crds::Condition {
        r#type: CONDITION_VERIFIED.to_string(),
        status: "True".to_string(),
        observed_generation: Some(1),
        last_transition_time: Some(at("2026-09-04T00:00:00Z")),
        reason: Some(REASON_VERIFIED.to_string()),
        message: Some("verified by weirkeeper".to_string()),
    }];
    let repair = weirkeeper::verification::retrust(
        &status,
        &resolution,
        backup_badge,
        Some(&stale),
        Some(1),
        at("2026-09-13T08:00:00Z"),
    )
    .expect(
        "a green condition beside an Untrusted block is the silent-revocation failure this whole \
         task exists to prevent, and leaving it is not an option",
    );
    assert_eq!(repair.verified.status, "False");
    assert_eq!(
        repair.verified.reason.as_deref(),
        Some("VerificationUntrusted")
    );
    assert_eq!(
        repair.verification, settled.verification,
        "…and the block is untouched, because it was already right"
    );
    assert_eq!(
        repair.from, repair.to,
        "the RESULT did not move; the condition did"
    );
}

/// **F7.** A `trust` block this build cannot read is not green.
///
/// Two states were folded together: "no `trust` key at all", which is an object
/// written before PLAT-19.1 and is correctly green, and "a `trust` key with no
/// readable `basis`", which is malformed. This module argues at length for
/// reading the basis clause BECAUSE one-field badge rules rot; the arm that
/// keeps the old rule must not swallow a shape the old rule never had.
///
/// KILLS: `Some(TRUST_BASIS_CURRENT) | None => false` (the pre-fix form).
#[test]
fn a_trust_block_this_build_cannot_read_is_not_green() {
    let base = |trust: Option<Value>| {
        let mut block = json!({
            "result": "Valid",
            "matchedKeyId": FIXTURE_KEY_ID,
            "payloadType": logweir_verify::PAYLOAD_TYPE_SCORECARD,
            "verifiedAt": "2026-09-11T03:20:00Z",
        });
        if let Some(t) = trust {
            block["trust"] = t;
        }
        renderable(block, json!({"exitCode": 0}))
    };

    // ADDITIVE COMPATIBILITY — an older controller wrote this. Still green.
    assert!(
        backup_badge(&base(None)).green,
        "an object that predates the trust block is not downgraded by its absence"
    );

    // MALFORMED — present and unreadable, in four shapes.
    for malformed in [
        json!({"keyState": "Active"}),
        json!({"basis": null, "keyState": "Active"}),
        json!({"basis": 7}),
        json!(null),
    ] {
        let badge = backup_badge(&base(Some(malformed.clone())));
        assert!(
            !badge.green,
            "{malformed}: a trust block this build cannot read is not a licence to render the \
             old rule; got {badge:?}"
        );
        assert_eq!(badge.reason, "VerificationUntrusted");
    }

    // …AND THE TWO IT CAN READ still decide as they did.
    assert!(backup_badge(&base(Some(json!({"basis": "Current"})))).green);
    let historical = backup_badge(&base(Some(json!({"basis": "Historical"}))));
    assert!(historical.green);
    assert!(historical
        .label
        .contains("signed before that key was retired"));
}

/// **F2.** The re-trust trigger maps a policy event to the objects that policy
/// could govern, out of the controller's OWN store — no LIST at all.
///
/// KILLS: "return every object whatever the policy names" (row 2 would enqueue
/// the unbound object) and "return nothing for a `default: true` policy" (row 3
/// would leave a revoked key green in every namespace no policy names, which is
/// most of them).
#[test]
fn a_policy_event_enqueues_the_objects_it_could_govern_and_no_others() {
    use weirkeeper::trust::may_govern;

    let explicit = policy("team-a", &["team-a", "team-b"], vec![evidence_key()]);
    assert!(may_govern(&explicit, "team-a"));
    assert!(may_govern(&explicit, "team-b"));
    assert!(
        !may_govern(&explicit, "team-c"),
        "a policy that names its namespaces claims no others, so an event on it touches nothing \
         in team-c"
    );

    // A `default: true` policy claims every namespace HERE, although resolution
    // would hand an explicitly-named one to its own policy. Over-approximating
    // costs a re-derivation that writes nothing; under-approximating leaves a
    // revoked key green until something else happens to reconcile.
    let fallback = policy("org-default", &[], vec![evidence_key()]);
    assert!(
        fallback.spec.default,
        "the builder makes an unnamed policy the default"
    );
    for namespace in ["team-a", "team-c", "anything-at-all"] {
        assert!(
            may_govern(&fallback, namespace),
            "the default is the fallback for every namespace no policy names, and the trigger \
             cannot tell which those are from one object"
        );
    }

    // AND THE GUARD IN FRONT OF THE EXPENSIVE HALF. An object with no verdict
    // has nothing to re-decide, and must not pay for an API read to find out.
    use weirkeeper::verification::has_trust_verdict;
    assert!(!has_trust_verdict(None));
    assert!(!has_trust_verdict(Some(&json!({"phase": "Running"}))));
    assert!(!has_trust_verdict(Some(&json!({
        "evidence": {"verification": {"result": "NotAttempted", "payloadType": "p"}}
    }))));
    assert!(has_trust_verdict(Some(&verified_status(
        "2026-09-04T00:00:00Z"
    ))));
}

// ===========================================================================
// Fix round 2 — the wiring itself (review findings G1 and G2)
// ===========================================================================

/// A `Backup` in `namespace`, named `name`, with a `resourceVersion`.
fn backup_in(namespace: &str, name: &str, resource_version: &str) -> Backup {
    serde_json::from_value(json!({
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "Backup",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "uid": format!("uid-{name}"),
            "generation": 1,
            "resourceVersion": resource_version,
        },
        "spec": {
            "sourceRef": {"name": "prod"},
            "topics": ["orders"],
            "archive": {"url": "s3://kafka-backups/k8s-demo",
                        "secretRef": {"name": "logweir-s3"}},
            "triggeredBy": "manual",
            "deadlineSeconds": 3600,
        },
    }))
    .expect("the fixture is a Backup")
}

/// **G1.** An edit that NARROWS wakes the namespace it stopped governing.
///
/// # The fail-open this closes
///
/// A `watches` mapper is handed only the NEW object. Removing `team-a` from
/// `spec.namespaces` — or clearing `spec.default` — makes the new object's own
/// scope false for `team-a` and would enqueue nothing there, although that edit
/// is precisely what changed `team-a`'s resolution. A terminal `Backup` heals
/// on its next requeue; a terminal `Restore` parks on `Action::await_change()`
/// and **nothing** wakes it, so if the namespace's new fallback does not carry
/// the signing key the correct verdict is `UntrustedSigner` and the object
/// keeps a green badge indefinitely.
///
/// KILLS: "enqueue only what the new object names" — rows 2 and 4 below.
#[test]
fn a_policy_edit_that_narrows_still_wakes_what_it_stopped_governing() {
    use weirkeeper::trust::{PolicyScope, PolicyScopeMemory};

    let scopes = PolicyScopeMemory::default();
    let wide = policy("team", &["team-a", "team-b"], vec![evidence_key()]);
    let narrow = policy("team", &["team-a"], vec![evidence_key()]);

    // 1. FIRST SIGHTING — no history, so the scope is what it declares.
    assert_eq!(
        scopes.observe(&wide),
        PolicyScope::Namespaces(["team-a".to_string(), "team-b".to_string()].into()),
    );

    // 2. NARROWING — `team-b` is gone from the object and MUST still be woken.
    assert_eq!(
        scopes.observe(&narrow),
        PolicyScope::Namespaces(["team-a".to_string(), "team-b".to_string()].into()),
        "the namespace this edit stopped governing is the one whose resolution certainly \
         changed, and the new object is the one place its name no longer appears"
    );

    // 3. THE MEMORY MOVED WITH IT — a second identical event is not still
    //    dragging `team-b` along for ever.
    assert_eq!(
        scopes.observe(&narrow),
        PolicyScope::Namespaces(["team-a".to_string()].into()),
        "the union is with the PREVIOUS event, not with everything ever seen"
    );

    // 4. CLEARING `default` IS THE SAME EDIT ONE LEVEL UP.
    let scopes = PolicyScopeMemory::default();
    let fallback = policy("org-default", &[], vec![evidence_key()]);
    assert_eq!(scopes.observe(&fallback), PolicyScope::Everything);
    assert_eq!(
        scopes.observe(&policy("org-default", &["team-a"], vec![evidence_key()])),
        PolicyScope::Everything,
        "a policy that stops being the cluster default could have changed EVERY namespace that \
         was falling through to it"
    );

    // 5. WIDENING still works, and `Everything` absorbs from either side.
    let scopes = PolicyScopeMemory::default();
    assert_eq!(
        scopes.observe(&policy("p", &["team-a"], vec![evidence_key()])),
        PolicyScope::Namespaces(["team-a".to_string()].into()),
    );
    assert_eq!(
        scopes.observe(&policy("p", &[], vec![evidence_key()])),
        PolicyScope::Everything
    );
}

/// **G2.** The mapper returns the objects in scope and no others.
///
/// KILLS: "return the whole store whatever the scope" (the unbound object) and
/// "return nothing for `Everything`" (the third arm).
#[test]
fn the_trigger_maps_a_scope_to_the_objects_in_it() {
    use weirkeeper::trust::PolicyScope;
    use weirkeeper::verification::targets_in_scope;

    let store: Vec<std::sync::Arc<Backup>> = vec![
        std::sync::Arc::new(backup_in("team-a", "nightly", "11")),
        std::sync::Arc::new(backup_in("team-b", "hourly", "12")),
        std::sync::Arc::new(backup_in("team-c", "weekly", "13")),
    ];
    let names = |refs: Vec<kube::runtime::reflector::ObjectRef<Backup>>| {
        refs.into_iter()
            .map(|r| format!("{}/{}", r.namespace.unwrap_or_default(), r.name))
            .collect::<std::collections::BTreeSet<String>>()
    };

    let scoped = names(targets_in_scope(
        store.clone(),
        &PolicyScope::Namespaces(["team-a".to_string(), "team-b".to_string()].into()),
    ));
    assert_eq!(
        scoped,
        ["team-a/nightly".to_string(), "team-b/hourly".to_string()].into(),
        "a policy that names its namespaces wakes those and nothing else — `team-c` is \
         untouched, which is what makes the trigger cheap on a cluster with many tenants"
    );

    assert_eq!(
        names(targets_in_scope(store.clone(), &PolicyScope::Everything)).len(),
        3,
        "a policy that is, or was, the cluster default could have changed any of them"
    );
    assert!(
        names(targets_in_scope(
            store,
            &PolicyScope::Namespaces(Default::default())
        ))
        .is_empty(),
        "an empty scope enqueues nothing at all"
    );
}

/// **G2.** `apply_retrust` sends ONE preconditioned `/status` PATCH, and
/// nothing when the verdict has not moved.
///
/// This is the only function that writes the re-trust verdict and the only
/// place seam **S7**'s `metadata.resourceVersion` precondition is applied, and
/// until this row it had no test at all: a refactor that dropped the
/// precondition, or that wrote a run fact, would have passed the whole suite.
///
/// KILLS: "send the patch without `metadata.resourceVersion`"; "patch the whole
/// status"; "send a patch even when `retrust` returned `None`".
#[tokio::test]
async fn apply_retrust_sends_one_preconditioned_status_patch_or_nothing() {
    let sent: Arc<Mutex<Vec<(String, String, Value)>>> = Arc::new(Mutex::new(Vec::new()));
    let client = {
        let sent = Arc::clone(&sent);
        kube::Client::new(
            service_fn(move |req: Request<Body>| {
                let sent = Arc::clone(&sent);
                async move {
                    let method = req.method().to_string();
                    let uri = req.uri().to_string();
                    let bytes = req.into_body().collect().await.expect("a body").to_bytes();
                    let body: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
                    sent.lock().expect("the recorder").push((method, uri, body));
                    Ok::<_, std::convert::Infallible>(
                        Response::builder()
                            .status(200)
                            .body(Body::from(
                                serde_json::to_vec(&json!({
                                    "apiVersion": "logweir.dev/v1alpha1", "kind": "Backup",
                                    "metadata": {"name": "nightly", "namespace": "team-a",
                                                 "uid": "uid-nightly", "resourceVersion": "42"},
                                    "spec": {"sourceRef": {"name": "prod"},
                                             "topics": ["orders"],
                                             "archive": {"url": "s3://b",
                                                         "secretRef": {"name": "s"}},
                                             "triggeredBy": "manual",
                                             "deadlineSeconds": 3600},
                                }))
                                .expect("serialises"),
                            ))
                            .expect("a response"),
                    )
                }
            }),
            "default",
        )
    };

    // A terminal Backup carrying a `Valid` verdict, and a policy that revoked
    // the key that produced it.
    let mut object = backup_in("team-a", "nightly", "17");
    object.status = serde_json::from_value(verified_status("2026-09-04T00:00:00Z"))
        .expect("the fixture status is a BackupStatus");
    let effective = at("2026-09-10T00:00:00Z");
    let mut key = evidence_key();
    key.state = KeyState::Revoked;
    key.revoked_at = Some(effective);
    key.revocation_reason = Some(RevocationReason::KeyCompromise);
    key.revocation_effective_from = Some(effective);
    let revoked = Resolution::Trust(Box::new(resolved(&policy("org-default", &[], vec![key]))));
    let api: kube::Api<Backup> = kube::Api::namespaced(client.clone(), "team-a");

    let outcome = weirkeeper::verification::apply_retrust(
        &api,
        &object,
        &revoked,
        backup_badge,
        at("2026-09-12T08:00:00Z"),
        &weirkeeper::verification::SigningTime::NotNeeded,
    )
    .await
    .expect("the double answers 200")
    .expect("a revocation changes the verdict, so a patch is owed");
    assert_eq!(
        (outcome.from.as_str(), outcome.to.as_str()),
        ("Valid", "Untrusted")
    );

    let calls = sent.lock().expect("the recorder").clone();
    assert_eq!(calls.len(), 1, "exactly one write; got {calls:?}");
    let (method, uri, body) = &calls[0];
    assert_eq!(method, "PATCH");
    assert!(
        uri.split('?')
            .next()
            .is_some_and(|p| p.ends_with("/namespaces/team-a/backups/nightly/status")),
        "the STATUS subresource and nothing else; got {uri}"
    );
    // ---- SEAM S7 ----
    assert_eq!(
        body["metadata"]["resourceVersion"],
        json!("17"),
        "every status write this branch adds carries `metadata.resourceVersion` as a \
         PRECONDITION, so an object that changed between the read and this write answers 409 and \
         the next reconcile reasons from what the object now says — which matters more here than \
         anywhere else, because this pass reasons entirely from stored fields. Got {body}"
    );
    let touched: Vec<&String> = body["status"]
        .as_object()
        .expect("a status object")
        .keys()
        .collect();
    assert_eq!(
        touched,
        vec!["evidence", "conditions"],
        "D3 §7.4: the verification block and the condition that says the same thing, and never \
         `phase`, `exitCode` or `outcome`. Got {body}"
    );
    assert_eq!(
        body["status"]["evidence"]["verification"]["result"],
        json!("Untrusted")
    );

    // ---- AND A SECOND PASS OVER THE PATCHED OBJECT SENDS NOTHING ----
    sent.lock().expect("the recorder").clear();
    let patched = body["status"].clone();
    let mut settled = backup_in("team-a", "nightly", "18");
    settled.status = serde_json::from_value(patched).expect("the patched status round-trips");
    assert!(
        weirkeeper::verification::apply_retrust(
            &api,
            &settled,
            &revoked,
            backup_badge,
            at("2026-09-13T08:00:00Z"),
            &weirkeeper::verification::SigningTime::NotNeeded,
        )
        .await
        .expect("no request is made at all")
        .is_none(),
        "the verdict is already what the policy says (erratum E11(d))"
    );
    assert!(
        sent.lock().expect("the recorder").is_empty(),
        "…and nothing was sent: one `kubectl apply` on a policy must not wake every terminal \
         object in the cluster into a write"
    );

    // ---- NO PRECONDITION, NO PATCH ----
    sent.lock().expect("the recorder").clear();
    let mut unversioned = object.clone();
    unversioned.metadata.resource_version = None;
    assert!(
        weirkeeper::verification::apply_retrust(
            &api,
            &unversioned,
            &revoked,
            backup_badge,
            at("2026-09-12T08:00:00Z"),
            &weirkeeper::verification::SigningTime::NotNeeded,
        )
        .await
        .expect("this is not an API failure")
        .is_none(),
        "an object with nothing to precondition on is not one this pass may write over blind"
    );
    assert!(sent.lock().expect("the recorder").is_empty());
}

/// **G3.** The roster is the FALLBACK, so a namespace a policy governs costs
/// **no** API call at all.
///
/// # Why this counts requests instead of asserting a verdict
///
/// Because the optimisation is invisible in the verdict: resolving with the
/// roster fetched and resolving without it produce the same `Resolution` for
/// every namespace a policy answers — that is precisely why it is safe. The
/// only observable is the request that does not happen, and the re-trust pass
/// runs on every reconcile of every verdict-carrying object, so "one
/// `GET trustrosters/default` per object per requeue" is the cost this row
/// exists to hold down.
///
/// KILLS: "always fetch the roster before resolving" — the first two arms then
/// make one request each.
#[tokio::test]
async fn a_namespace_a_policy_governs_reads_no_roster() {
    let reads = Arc::new(AtomicUsize::new(0));
    let client = {
        let reads = Arc::clone(&reads);
        kube::Client::new(
            service_fn(move |req: Request<Body>| {
                let reads = Arc::clone(&reads);
                async move {
                    assert!(
                        req.uri().path().ends_with("/trustrosters/default"),
                        "the only call this seam may make is the roster fallback; got {}",
                        req.uri()
                    );
                    reads.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, std::convert::Infallible>(
                        Response::builder()
                            .status(404)
                            .body(Body::from(
                                br#"{"kind":"Status","apiVersion":"v1","status":"Failure",
                                     "reason":"NotFound","code":404}"#
                                    .to_vec(),
                            ))
                            .expect("a response"),
                    )
                }
            }),
            "default",
        )
    };

    let explicit = policy("team-a", &["team-a"], vec![evidence_key()]);
    let fallback = policy("org-default", &[], vec![evidence_key()]);
    let contested = vec![
        policy("one", &["team-b"], vec![evidence_key()]),
        policy("two", &["team-b"], vec![evidence_key()]),
    ];

    for (label, policies, namespace) in [
        (
            "an explicit spec.namespaces match",
            vec![explicit.clone()],
            "team-a",
        ),
        ("the single default policy", vec![fallback], "anything"),
        ("a contested namespace", contested, "team-b"),
    ] {
        let before = reads.load(Ordering::SeqCst);
        weirkeeper::trust::resolve_with(&policies, &client, namespace)
            .await
            .expect("resolution completes");
        assert_eq!(
            reads.load(Ordering::SeqCst),
            before,
            "{label}: the roster is reached only after an explicit match AND the default have \
             both missed, so this answer costs nothing"
        );
    }

    // …AND A NAMESPACE NO POLICY ANSWERS STILL FALLS THROUGH TO IT, exactly
    // once. The optimisation removes a read; it must not remove the fallback.
    let before = reads.load(Ordering::SeqCst);
    let resolution = weirkeeper::trust::resolve_with(&[explicit], &client, "team-z")
        .await
        .expect("resolution completes");
    assert_eq!(
        reads.load(Ordering::SeqCst),
        before + 1,
        "a namespace no policy names is what the roster is FOR"
    );
    assert_eq!(
        resolution,
        Resolution::Unconfigured,
        "and with no roster either, that is today's RosterNotFound"
    );
}

// ===========================================================================
// Fix round 2, R4 — the re-trust patch clears what the new verdict does not hold
// ===========================================================================

/// **AN `Untrusted -> Valid` RE-DERIVATION TAKES ITS REFUSAL SENTENCE WITH IT**
/// — review finding **R4**.
///
/// # The staleness, and why THIS writer is the one that matters
///
/// `retrust` builds a fresh block and OMITS `detail`, `signedAt` and `trust`
/// when the re-derived verdict has none — and `untrusted_detail` returns `None`
/// for a trust verdict that is `Valid`. A JSON merge patch (RFC 7386) leaves an
/// omitted key in place, so an administrator who adds the signer's public half
/// used to get `result: Valid` with the previous pass's *"the trust policy …
/// does not accept it"* sentence still sitting under it: a green verdict
/// carrying its own refusal.
///
/// The second patch has the same shape and no producer — `status_is_terminal`
/// short-circuits every pass after the terminal write, so it runs at most once
/// per object. **`apply_retrust` has one.** It runs on TERMINAL objects,
/// repeatedly, every time a `TrustRoster` or an installation policy changes,
/// which is exactly when a verdict moves. That is why R4 is reachable where R3
/// was hypothetical.
///
/// # What is asserted
///
/// The scenario is `a_key_added_after_an_untrusted_verdict_is_re_granted_with_a_reason`'s
/// two arms — the same fixtures, because the property is about what the SECOND
/// pass's patch does to what the FIRST pass wrote. Then: the patch carries
/// `detail: null`, applying it as the API server would REMOVES the sentence,
/// the fields the new verdict DOES hold are untouched, and the re-grant is
/// still green.
///
/// KILLS: dropping `verification_patch_value` from `Retrust::patch`; turning
/// its `or_insert` into an `insert`, which would null the `trust` block this
/// verdict legitimately carries.
#[test]
fn a_re_granted_verdict_does_not_keep_the_sentence_that_refused_it() {
    // PASS 1 — the policy does not carry the signer: `Untrusted`, with a reason.
    let status = verified_status("2026-09-04T00:00:00Z");
    let untrusted = weirkeeper::verification::retrust(
        &status,
        &Resolution::Trust(Box::new(resolved(&policy(
            "team-b",
            &["team-b"],
            vec![policy_key(
                "some-other-key",
                vec![SpecUsage::EvidenceSigning],
            )],
        )))),
        backup_badge,
        None,
        Some(1),
        at("2026-09-12T08:00:00Z"),
    )
    .expect("a policy that does not carry the signer changes the verdict");
    let refusal = untrusted.verification["detail"]
        .as_str()
        .expect("the Untrusted verdict carries its reason")
        .to_string();

    // The object as it stands after pass 1, built the way the API server would.
    let mut stored = status.clone();
    let first = untrusted.patch(&[]);
    weirkeeper::conditions::apply_merge_patch(&mut stored, &first["status"]);
    assert_eq!(
        stored["evidence"]["verification"]["detail"].as_str(),
        Some(refusal.as_str()),
        "pass 1 really did put the sentence on the object: {stored}"
    );

    // PASS 2 — the administrator adds the key, Retired, over a window that
    // covers the signature. The verdict goes back to `Valid`.
    let mut added = evidence_key();
    added.state = KeyState::Retired;
    added.retired_at = Some(at("2026-09-10T00:00:00Z"));
    let regranted = weirkeeper::verification::retrust(
        &stored,
        &Resolution::Trust(Box::new(resolved(&policy(
            "team-b",
            &["team-b"],
            vec![added],
        )))),
        backup_badge,
        None,
        Some(1),
        at("2026-09-12T09:00:00Z"),
    )
    .expect("adding the signer's public half changes the verdict back");
    assert_eq!(
        (regranted.from.as_str(), regranted.to.as_str()),
        ("Untrusted", "Valid")
    );

    // ---- 1. THE PATCH NULLS THE SENTENCE -----------------------------
    let patch = regranted.patch(&[]);
    let block = &patch["status"]["evidence"]["verification"];
    assert_eq!(
        block.get("detail"),
        Some(&Value::Null),
        "R4: an OMITTED `detail` leaves the refusal in place — a merge patch only deletes what \
         it sends as null. Got: {block}"
    );

    // ---- 2. AND THE SENTENCE IS ACTUALLY GONE ------------------------
    let mut after = stored.clone();
    weirkeeper::conditions::apply_merge_patch(&mut after, &patch["status"]);
    let settled = &after["evidence"]["verification"];
    assert_eq!(settled["result"], json!("Valid"));
    assert_eq!(
        settled.get("detail"),
        None,
        "a green verdict must not carry the sentence that refused it — an operator reading \
         `Valid` beside \"does not accept it\" cannot tell which is current: {settled}"
    );

    // ---- 3. THE FIELDS THE NEW VERDICT HOLDS ARE UNTOUCHED -----------
    //
    // `or_insert` writes only into a VACANT entry, so a present value is never
    // overwritten. This re-grant legitimately carries a `trust` block and a
    // `matchedKeyId`; nulling either would erase the re-derivation itself.
    assert_eq!(
        settled["trust"]["basis"],
        json!("Historical"),
        "signed while the key was valid, and the key has since been retired: {settled}"
    );
    assert!(
        settled["matchedKeyId"]
            .as_str()
            .is_some_and(|k| !k.is_empty()),
        "the key the signature matched survives: {settled}"
    );
    assert_eq!(
        settled["verifiedAt"],
        json!("2026-09-04T00:00:00Z"),
        "and the re-grant still does not re-observe"
    );

    // ---- 4. THE BADGE IS THE UN-NULLED BLOCK'S -----------------------
    //
    // `retrust` computes the condition over the rendered block, BEFORE
    // `Retrust::patch` wraps it — `valid_verification` reads `trust` as
    // `None => compatible` but `Some(unreadable) => Untrusted`, so a badge
    // computed over the nulled form would refuse a legacy `Valid`.
    assert_eq!(
        regranted.verified.status, "True",
        "the re-grant is green: {:?}",
        regranted.verified
    );
}

// ===========================================================================
// TRUST-UPGRADE-SIGNEDAT — the pre-`signedAt` status, repaired by ONE read
// ===========================================================================

/// The receipt fixture's own claimed signing time, read out of `finished_at`.
const RECEIPT_FINISHED_AT: &str = "2026-09-09T11:04:46Z";

/// The `status.evidence` key a `Backup` records its receipt under, in the
/// shape the lab objects carry.
const LAB_RECEIPT_KEY: &str = "logweir/backups/b1/01M2TAEMHP5V3XPZXJWA5AD5PA.receipt.json";
/// …and its sidecar.
const LAB_SIDECAR_KEY: &str = "logweir/backups/b1/01M2TAEMHP5V3XPZXJWA5AD5PA.receipt.sig";

/// The backup-receipt media type EXACTLY as the lab objects spell it, with the
/// `;version=` parameter `logweir_core` matches on the base type of.
const LAB_PAYLOAD_TYPE: &str = "application/vnd.logweir.backup-receipt+json;version=1.0.0";

fn receipt_bytes() -> String {
    read("e2e/fixtures/signed/backup-receipt.json")
}

/// **THE LAB'S EXACT STATUS SHAPE**, from `lab-refresh-2.result.md` §8 and the
/// D1 fence run's `artifacts/d1-live/20260918t1209z/objects/L-05.1-3/before.json`:
/// a `Valid` verification block carrying `matchedKeyId`, `payloadType`,
/// `result` and `verifiedAt` — and **no `signedAt` and no `trust`**, the two
/// fields the controller that wrote it did not have.
///
/// The key id is the fixture's rather than the lab's `2c76e22f…` so the policy
/// helpers in this file resolve it; every other field is the recorded shape.
fn pre_signedat_status() -> Value {
    json!({
        "phase": "Succeeded",
        "exitCode": 0,
        "backupId": "7db2c737-a722-43e9-8ff9-fa0e7a356338-20260918-131400",
        "evidence": {
            "receiptKey": LAB_RECEIPT_KEY,
            "receiptSha256": sha256_prefixed(receipt_bytes().as_bytes()),
            "sidecarKey": LAB_SIDECAR_KEY,
            "verification": {
                "matchedKeyId": FIXTURE_KEY_ID,
                "payloadType": LAB_PAYLOAD_TYPE,
                "result": "Valid",
                "verifiedAt": "2026-09-18T13:14:23Z",
            }
        }
    })
}

/// The lab's own shape, verbatim, with `basis` replaced.
///
/// `None` is `lab-refresh-3.result.md` §9's five objects; `Current` and
/// `Historical` are the two bases that can only have been reached from a real
/// claim and therefore must NOT be re-read.
fn basis_status(basis: &str, result: &str) -> Value {
    let mut status = pre_signedat_status();
    status["evidence"]["verification"]["result"] = json!(result);
    status["evidence"]["verification"]["trust"] = json!({
        "basis": basis,
        "keyState": "Active",
        "policy": {"name": "org-default", "uid": "uid-org-default", "generation": 4},
    });
    status
}

fn org_default() -> Resolution {
    Resolution::Trust(Box::new(resolved(&policy(
        "org-default",
        &[],
        vec![evidence_key()],
    ))))
}

fn retrust_row(status: &Value, signing_time: &weirkeeper::verification::SigningTime) -> Value {
    let outcome = weirkeeper::verification::retrust_with(
        status,
        &org_default(),
        backup_badge,
        None,
        Some(1),
        at("2026-09-18T13:14:39Z"),
        signing_time,
    )
    .expect("this row changes the stored block");
    json!({
        "verification": outcome.verification,
        "from": outcome.from,
        "to": outcome.to,
        "conditionStatus": outcome.verified.status,
        "conditionReason": outcome.verified.reason,
    })
}

/// **ROW 1 — the pre-`signedAt` object is repaired to `Valid` after ONE read.**
///
/// The claim comes out of the REAL signed receipt fixture, through the same
/// `read_claimed_signing_time` a fresh run uses, and the repaired block carries
/// the `signedAt` a fresh run would have written.
///
/// KILLS: "carry the absence forward and flip to `Untrusted`" — the shipped
/// behaviour, which this row's `to` and `basis` both contradict; and "write the
/// recovered instant somewhere other than `signedAt`", which the badge assertion
/// catches because the green rule reads the basis the claim produced.
#[test]
fn a_pre_signedat_object_is_repaired_to_valid_by_one_bounded_read() {
    // THE WHOLE CHAIN, NOT ONLY ITS LAST LINK. Feeding `Recovered` straight in
    // proves what the pass does with a claim; it proves nothing about the pass
    // ever ASKING for one, and a discriminator that never fires makes the rest
    // of this row unreachable in production.
    assert!(
        weirkeeper::verification::signing_time_owed(Some(&pre_signedat_status())).is_some(),
        "a block with no `trust` at all compared nothing, so the read has to be asked for"
    );
    let outcome = weirkeeper::verification::retrust_with(
        &pre_signedat_status(),
        &org_default(),
        backup_badge,
        None,
        Some(1),
        at("2026-09-18T13:14:39Z"),
        &weirkeeper::verification::SigningTime::Recovered(at(RECEIPT_FINISHED_AT)),
    )
    .expect("the repair changes the stored block");
    let row = json!({
        "verification": outcome.verification.clone(),
        "from": outcome.from.clone(),
        "to": outcome.to.clone(),
        "conditionStatus": outcome.verified.status.clone(),
        "conditionReason": outcome.verified.reason.clone(),
    });
    assert_eq!(row["from"], json!("Valid"));
    assert_eq!(row["to"], json!("Valid"));
    assert_eq!(
        row["verification"]["signedAt"],
        json!(RECEIPT_FINISHED_AT),
        "the document's OWN `finished_at`, written in the same spelling the fresh path uses"
    );
    assert_eq!(row["verification"]["trust"]["basis"], json!("Current"));
    assert_eq!(row["verification"]["trust"]["keyState"], json!("Active"));
    assert_eq!(
        row["verification"].get("detail"),
        None,
        "there is nothing to explain about an answer that came out yes"
    );
    assert_eq!(row["conditionStatus"], json!("True"));
    assert_eq!(row["conditionReason"], json!(REASON_VERIFIED));
    assert_eq!(
        row["verification"]["verifiedAt"],
        json!("2026-09-18T13:14:23Z"),
        "the repair does not re-observe: `verifiedAt` is the one independent observation D3 §7.4 \
         accepts and a pass that moved it would destroy the evidence it reasons from"
    );

    // …AND THE NEXT PASS SENDS NOTHING (erratum E11(d)). The repaired block
    // carries a `signedAt`, so it is no longer `NotRecorded` and needs no read.
    let mut repaired = pre_signedat_status();
    repaired["evidence"]["verification"] = row["verification"].clone();
    assert!(
        weirkeeper::verification::signing_time_owed(Some(&repaired)).is_none(),
        "a repaired object must not ask for the archive again on every policy event"
    );
    assert!(
        weirkeeper::verification::retrust(
            &repaired,
            &org_default(),
            backup_badge,
            Some(&vec![outcome.verified.clone()]),
            Some(1),
            at("2026-09-18T14:00:00Z"),
        )
        .is_none(),
        "one repair, then silence: the block and the condition are both already what the policy \
         says, so erratum E11(d) sends nothing"
    );
}

/// **ROWS 2 and 3 — an archive that does not answer, and a grant only a pod may
/// hold.** Neither withdraws the verdict and neither presents one: the block
/// becomes `NotAttempted` on an `Unverified` basis, saying which one it is.
///
/// KILLS: "flip to `Untrusted` when the read fails" — the failure this whole
/// task exists to prevent, now reachable through a temporarily unreachable
/// bucket instead of through an upgrade; and "carry the stored `Valid` across"
/// — review finding **F1**, which put a green *"verified by weirkeeper"* badge
/// on the console and `VerificationState::Valid` on the API for a document
/// nobody had re-verified.
#[test]
fn an_unread_archive_neither_withdraws_nor_presents_a_verdict() {
    let rows = [
        (
            "unreachable archive",
            weirkeeper::verification::SigningTime::Unreadable(
                "the evidence object logweir/backups/b1/r1.receipt.json is not in the archive; \
                 nothing was verified"
                    .to_string(),
            ),
            "is not in the archive",
        ),
        (
            "a grant only a pod may hold",
            weirkeeper::verification::SigningTime::NotAttempted(
                "BackupDestination team-a/warm reads evidence with a grant only a pod may hold \
                 (D2 §3.9's evidence-fetch Job), and this build does not create that Job"
                    .to_string(),
            ),
            "only a pod may hold",
        ),
        (
            "no read was performed at all",
            weirkeeper::verification::SigningTime::NotNeeded,
            "has not been re-read yet",
        ),
    ];
    for (label, signing_time, expected) in rows {
        let row = retrust_row(&pre_signedat_status(), &signing_time);
        assert_eq!(row["from"], json!("Valid"), "{label}");
        assert_eq!(
            row["to"],
            json!("NotAttempted"),
            "{label}: F1 — the string every consumer that reads `result` alone already fails \
             closed on. NOT the stored `Valid`, which the console, the API projection and the \
             SIGNED printer column all render as verified"
        );
        assert_eq!(
            row["verification"]["trust"]["basis"],
            json!("Unverified"),
            "{label}: and it is rendered honestly, not as a basis nobody established"
        );
        assert_eq!(
            row["verification"].get("signedAt"),
            None,
            "{label}: nothing was read, so nothing is invented"
        );
        let detail = row["verification"]["detail"]
            .as_str()
            .expect("an unverified block always says why")
            .to_string();
        assert!(
            detail.contains(expected),
            "{label}: the reason travels with the verdict. Got {detail}"
        );
        assert!(
            detail.contains(FIXTURE_KEY_ID) && detail.contains("org-default"),
            "{label}: every detail in this module names the key and the policy. Got {detail}"
        );
        assert!(
            !detail.contains("the document carries no signing-time field"),
            "{label}: nobody read the document, so nothing may be claimed about its fields. Got \
             {detail}"
        );

        // ---- NOT GREEN, AND NOT CALLED UNTRUSTED EITHER ------------------
        assert_eq!(row["conditionStatus"], json!("False"), "{label}");
        assert_eq!(
            row["conditionReason"],
            json!(REASON_VERIFICATION_NOT_ATTEMPTED),
            "{label}: `VerificationUntrusted` over a verdict nobody refused is the same \
             dishonesty as a green badge over one nobody reached"
        );
        let mut projected = pre_signedat_status();
        projected["evidence"] = json!({"verification": row["verification"].clone()});
        let badge = backup_badge(&projected);
        assert!(!badge.green, "{label}");
        assert_eq!(badge.label, UNVERIFIED, "{label}");
    }
}

/// **ROW 4 — a document that GENUINELY claims no signing time is still
/// `Untrusted`.** Both when the stored block already says so, and when the one
/// bounded read comes back and the bytes carry no `finished_at`.
///
/// KILLS: "treat every missing `signedAt` as a pre-upgrade status" — which
/// would make a document whose timestamp field a future format renames verify
/// green against a retired key, the exact fail-open `decide`'s rule exists for.
#[test]
fn a_document_that_claims_no_signing_time_is_still_untrusted() {
    // ---- A BASIS THAT COULD ONLY HAVE COME FROM A REAL CLAIM -------------
    //
    // `Current` and `Historical` are reached only through `decide`'s window
    // rows, and both of those rows are unreachable without `claim.signed_at`.
    // A block carrying one has already compared a signing time to the key's
    // window, so there is nothing an archive read could add — it stays
    // `FieldAbsent` and fails closed, with no `get`.
    for basis in ["Current", "Historical"] {
        let compared = basis_status(basis, "Untrusted");
        assert!(
            weirkeeper::verification::signing_time_owed(Some(&compared)).is_none(),
            "{basis}: this block is evidence that a claim WAS compared, so re-reading it would \
             be a `get` that can change nothing"
        );
        let refused = retrust_row(&compared, &weirkeeper::verification::SigningTime::NotNeeded);
        assert_eq!(refused["to"], json!("Untrusted"), "{basis}");
        assert!(
            refused["verification"]["detail"]
                .as_str()
                .is_some_and(|d| d.contains("the document carries no signing-time field")),
            "{basis}: the absence is still reported as the document's. Got {}",
            refused["verification"]["detail"]
        );
    }
    let stored = basis_status("Current", "Untrusted");
    let row = retrust_row(&stored, &weirkeeper::verification::SigningTime::NotNeeded);
    assert_eq!(row["to"], json!("Untrusted"));
    assert_eq!(row["verification"]["trust"]["basis"], json!("None"));
    let detail = row["verification"]["detail"].as_str().expect("a reason");
    assert!(
        detail.contains("SignedOutsideValidity")
            && detail.contains("the document carries no signing-time field"),
        "unchanged behaviour, and the sentence is true of the document. Got {detail}"
    );

    // ---- the read happened and the bytes carry no claim -------------------
    let read_back = retrust_row(
        &pre_signedat_status(),
        &weirkeeper::verification::SigningTime::Absent(
            logweir_core::trust::ClaimAbsence::FieldAbsent,
        ),
    );
    assert_eq!(
        read_back["to"],
        json!("Untrusted"),
        "the absence is the DOCUMENT's now, and a document's absence has always failed closed"
    );
    assert_eq!(read_back["verification"]["trust"]["basis"], json!("None"));
    assert_eq!(read_back["verification"].get("signedAt"), None);
}

/// **ROW 5 — the ratchet, through the controller's own pass.** A
/// `KeyCompromise` revocation flips a pre-`signedAt` object immediately, with
/// no read and on `SigningTime::NotNeeded`.
///
/// KILLS: "park every pre-`signedAt` object on `Unverified` until a read
/// succeeds" — which would leave a stolen key's signatures accepted for as long
/// as a bucket stayed unreachable.
#[test]
fn a_compromise_revocation_does_not_wait_for_the_re_read() {
    let effective = at("2026-09-10T00:00:00Z");
    let mut key = evidence_key();
    key.state = KeyState::Revoked;
    key.revoked_at = Some(effective);
    key.revocation_reason = Some(RevocationReason::KeyCompromise);
    key.revocation_effective_from = Some(effective);
    let outcome = weirkeeper::verification::retrust_with(
        &pre_signedat_status(),
        &Resolution::Trust(Box::new(resolved(&policy("org-default", &[], vec![key])))),
        backup_badge,
        None,
        Some(1),
        at("2026-09-18T13:14:39Z"),
        &weirkeeper::verification::SigningTime::NotNeeded,
    )
    .expect("a compromise revocation always changes a Valid verdict");
    assert_eq!(
        (outcome.from.as_str(), outcome.to.as_str()),
        ("Valid", "Untrusted")
    );
    assert_ne!(
        outcome.verification["trust"]["basis"],
        json!("Unverified"),
        "a stolen private half is not a question about when the document was signed"
    );
    assert_eq!(outcome.verified.status, "False");
}

/// `signing_time_need` asks for a read only when all three facts hold, and it
/// knows both kinds' evidence field names.
///
/// KILLS: "read the archive on every re-trust pass" — the `None` rows below are
/// what keep one `kubectl apply` on a policy from turning into one bucket `get`
/// per terminal object in the cluster; and "repair without checking the digest"
/// — the `receiptSha256`-less row, which has nothing safe to read.
#[test]
fn a_re_read_is_asked_for_only_when_it_can_be_done_safely() {
    let need = weirkeeper::verification::signing_time_owed(Some(&pre_signedat_status()))
        .expect("the lab's own shape is exactly the case this repairs");
    assert_eq!(need.payload_key, LAB_RECEIPT_KEY);
    assert_eq!(need.payload_type, LAB_PAYLOAD_TYPE);
    assert_eq!(
        need.payload_sha256,
        sha256_prefixed(receipt_bytes().as_bytes())
    );

    // ---- a Restore names its scorecard, and the same rule applies ---------
    let mut restore = pre_signedat_status();
    restore["evidence"] = json!({
        "scorecardKey": "logweir/drills/d1/scorecard.json",
        "scorecardSha256": "sha256:abc",
        "verification": pre_signedat_status()["evidence"]["verification"].clone(),
    });
    let scorecard = weirkeeper::verification::signing_time_owed(Some(&restore))
        .expect("a Restore's scorecard is the same repair");
    assert_eq!(scorecard.payload_key, "logweir/drills/d1/scorecard.json");

    // ---- and every row that must NOT ask ---------------------------------
    let mut with_signed_at = pre_signedat_status();
    with_signed_at["evidence"]["verification"]["signedAt"] = json!(RECEIPT_FINISHED_AT);
    let mut no_digest = pre_signedat_status();
    no_digest["evidence"]["receiptSha256"] = json!(null);
    let mut no_key = pre_signedat_status();
    no_key["evidence"]["receiptKey"] = json!("");
    let mut no_match = pre_signedat_status();
    no_match["evidence"]["verification"]["matchedKeyId"] = json!("");
    let mut null_signed_at = pre_signedat_status();
    null_signed_at["evidence"]["verification"]["signedAt"] = json!(null);
    null_signed_at["evidence"]["verification"]["trust"] = json!({"basis": "Current"});
    for (label, status) in [
        ("the block already carries a signing time", with_signed_at),
        ("no digest, so nothing safe to read", no_digest),
        ("no document key", no_key),
        ("no signature was ever matched", no_match),
        (
            "an explicit null `signedAt` beside a basis that compared a claim",
            null_signed_at,
        ),
        (
            "a basis reached from a real claim",
            basis_status("Current", "Untrusted"),
        ),
        ("the other such basis", basis_status("Historical", "Valid")),
        ("no status at all", json!({})),
    ] {
        assert!(
            weirkeeper::verification::signing_time_owed(Some(&status)).is_none(),
            "{label}: {status}"
        );
    }
}

/// The bytes are checked against the digest the run recorded BEFORE any claim
/// is read out of them, and the four outcomes are distinguishable.
///
/// KILLS: "take the signing time from whatever is in the bucket" — the
/// substituted-document row below, which would let anyone who can write the
/// archive choose the instant a retired key's signature is compared against.
#[test]
fn a_signing_time_is_only_taken_from_the_bytes_the_run_recorded() {
    let payload = receipt_bytes();
    let need = weirkeeper::verification::SigningTimeNeed {
        payload_key: LAB_RECEIPT_KEY.to_string(),
        payload_sha256: sha256_prefixed(payload.as_bytes()),
        payload_type: LAB_PAYLOAD_TYPE.to_string(),
    };
    assert_eq!(
        weirkeeper::verification::signing_time_in(payload.as_bytes(), &need),
        weirkeeper::verification::SigningTime::Recovered(at(RECEIPT_FINISHED_AT)),
        "the document's own `finished_at`, through the same reader the fresh path uses"
    );

    // ---- SUBSTITUTED BYTES, genuinely signed, with a later claim ----------
    let substituted = json!({"finished_at": "2099-01-01T00:00:00Z"}).to_string();
    match weirkeeper::verification::signing_time_in(substituted.as_bytes(), &need) {
        weirkeeper::verification::SigningTime::Unreadable(detail) => assert!(
            detail.contains("digest mismatch") && detail.contains(&need.payload_sha256),
            "the refusal names both digests, like every other digest refusal here: {detail}"
        ),
        other => panic!("bytes that are not the recorded bytes supply no claim, got {other:?}"),
    }

    // ---- the recorded bytes, carrying no claim ---------------------------
    let empty = json!({}).to_string();
    let empty_need = weirkeeper::verification::SigningTimeNeed {
        payload_sha256: sha256_prefixed(empty.as_bytes()),
        ..need.clone()
    };
    assert_eq!(
        weirkeeper::verification::signing_time_in(empty.as_bytes(), &empty_need),
        weirkeeper::verification::SigningTime::Absent(
            logweir_core::trust::ClaimAbsence::FieldAbsent
        )
    );

    // ---- the recorded bytes, which are not JSON --------------------------
    let junk = b"not json at all";
    let junk_need = weirkeeper::verification::SigningTimeNeed {
        payload_sha256: sha256_prefixed(junk),
        ..need.clone()
    };
    assert_eq!(
        weirkeeper::verification::signing_time_in(junk, &junk_need),
        weirkeeper::verification::SigningTime::Absent(
            logweir_core::trust::ClaimAbsence::Unparseable
        )
    );
}

/// The read itself, through a real read-only [`Store`] over the checked-in
/// signed receipt — and a controller with no credential attempts nothing.
#[test]
fn the_bounded_read_goes_through_the_read_only_handle() {
    let payload = receipt_bytes();
    let tree = EvidenceTree::new("signedat", payload.as_bytes(), b"unused");
    let store = tree.handle();
    let need = weirkeeper::verification::SigningTimeNeed {
        payload_key: PAYLOAD_KEY.to_string(),
        payload_sha256: sha256_prefixed(payload.as_bytes()),
        payload_type: LAB_PAYLOAD_TYPE.to_string(),
    };
    assert_eq!(
        weirkeeper::verification::read_signing_time(Some(&store), &need),
        weirkeeper::verification::SigningTime::Recovered(at(RECEIPT_FINISHED_AT))
    );

    // ---- NO CREDENTIAL IS NOT A BAD DOCUMENT -----------------------------
    assert_eq!(
        weirkeeper::verification::read_signing_time(None, &need),
        weirkeeper::verification::SigningTime::NotAttempted(NO_CREDENTIAL_DETAIL.to_string())
    );

    // ---- AND A MISSING OBJECT IS `Unreadable`, NOT A VERDICT -------------
    tree.remove(PAYLOAD_KEY);
    match weirkeeper::verification::read_signing_time(Some(&store), &need) {
        weirkeeper::verification::SigningTime::Unreadable(detail) => assert!(
            detail.contains("is not in the archive"),
            "the storage error's own sentence: {detail}"
        ),
        other => panic!("a storage failure is never a claim about the document, got {other:?}"),
    }
}

/// **The implication the whole discriminator rests on.** On the fresh path, a
/// block with a `matchedKeyId` ALWAYS carries a `trust` object — so
/// "`matchedKeyId` and no `trust`" can only have come from a controller that
/// predates PLAT-19.1.
///
/// KILLS: "write `trust` only when the verdict is interesting" — any such edit
/// would make a block this build wrote indistinguishable from a pre-upgrade
/// one, and the repair would start re-reading archives for documents whose
/// absence is their own.
#[test]
fn a_matched_key_is_always_written_beside_a_trust_block() {
    let signed = Signed::scorecard("implication");
    let rows: [(&str, Vec<SpecKey>); 3] = [
        ("current", vec![evidence_key()]),
        (
            "retired",
            vec![SpecKey {
                state: KeyState::Retired,
                retired_at: Some(at("2026-09-10T00:00:00Z")),
                ..evidence_key()
            }],
        ),
        (
            "wrong usage",
            vec![policy_key(
                FIXTURE_KEY_ID,
                vec![SpecUsage::GovernedApproval, SpecUsage::EvidenceSigning],
            )],
        ),
    ];
    for (label, keys) in rows {
        let result = signed.verify(&resolved(&policy("org-default", &[], keys)));
        let block = result.to_status_value(None);
        assert_eq!(
            block["matchedKeyId"],
            json!(FIXTURE_KEY_ID),
            "{label}: the fixture signature matches"
        );
        assert!(
            block.get("trust").is_some(),
            "{label}: every verdict that matched a key carries the basis it reached. Got {block}"
        );
    }
}

/// **Review finding F1, the guard.** The block the undecided arm writes is not
/// green on any surface that reads `result` — the CRD's own `SIGNED` printer
/// column included, read out of the shipped CRD so the JSONPath cannot drift
/// away from this assertion.
///
/// KILLS: "carry the stored `Valid` across while the read is outstanding" —
/// `crds::TrustBasis`'s invariant is *"every old reader treats anything that is
/// not `Valid` as unverified"*, and this is the first verdict that had to obey
/// it without being a refusal. With `Valid` restored, the printer column below
/// prints `Valid` for a document nobody re-verified.
#[test]
fn the_signed_printer_column_never_prints_valid_for_an_unread_verdict() {
    let row = retrust_row(
        &pre_signedat_status(),
        &weirkeeper::verification::SigningTime::Unreadable("the bucket did not answer".to_string()),
    );
    let block = row["verification"].clone();
    assert_eq!(block["result"], json!("NotAttempted"));

    let object = json!({"status": {"evidence": {"verification": block}}});
    for crd in ["config/crd/backups.yaml", "config/crd/restores.yaml"] {
        let doc: Value = serde_yaml::from_str(&read(crd)).expect("the shipped CRD parses");
        let columns = doc["spec"]["versions"][0]["additionalPrinterColumns"]
            .as_array()
            .expect("the version declares printer columns");
        let signed = columns
            .iter()
            .find(|c| c["name"] == json!("SIGNED"))
            .unwrap_or_else(|| panic!("{crd} declares a SIGNED column"));
        let json_path = signed["jsonPath"].as_str().expect("a JSONPath");
        // `.status.evidence.verification.result` -> `/status/evidence/…`
        let pointer = json_path.replace('.', "/");
        let printed = object.pointer(&pointer);
        assert_ne!(
            printed,
            Some(&json!("Valid")),
            "{crd}: `kubectl get` prints this column verbatim and an operator reads it as the \
             verdict. Path {json_path}, object {object}"
        );
        assert_eq!(printed, Some(&json!("NotAttempted")), "{crd}");
    }
}

/// **Defence in depth for the same finding.** Even if a block shaped
/// `result: Valid` + `basis: Unverified` ever reaches a badge — a hand-edited
/// status, a rollback that half-wrote, a future builder — the Rust rule refuses
/// it, and refuses it as *not attempted* rather than as *untrusted*.
///
/// KILLS: "drop the `Unverified` arm from `valid_verification`" — the block
/// below would fall through to the catch-all and render
/// `VerificationUntrusted`, accusing a document nobody examined; and any edit
/// that made the basis clause permissive would make it green.
#[test]
fn a_valid_result_on_an_unverified_basis_is_still_not_green() {
    let mut status = pre_signedat_status();
    status["evidence"]["verification"] = json!({
        "result": "Valid",
        "matchedKeyId": FIXTURE_KEY_ID,
        "payloadType": LAB_PAYLOAD_TYPE,
        "verifiedAt": "2026-09-18T13:14:23Z",
        "trust": {"basis": "Unverified", "keyState": "Active"},
    });
    for (label, badge) in [
        ("backup", backup_badge(&status)),
        (
            "restore",
            restore_badge(&json!({
                "outcome": "pass",
                "evidence": status["evidence"].clone(),
            })),
        ),
    ] {
        assert!(
            !badge.green,
            "{label}: an unestablished basis is never green"
        );
        assert_eq!(badge.label, UNVERIFIED, "{label}");
        assert_eq!(
            badge.reason, REASON_VERIFICATION_NOT_ATTEMPTED,
            "{label}: nothing was refused, so `VerificationUntrusted` would be an accusation \
             about a document this controller never read"
        );
    }
}

/// **Review finding F2, the guard.** The pass's own output is still recognised
/// as a status that predates `signedAt`, so an archive that was unreachable on
/// the first policy event is re-read on the second — and the object ends
/// `Valid` with the document's own `signedAt`.
///
/// KILLS: "read the discriminator off the presence of `trust` alone" — the
/// shipped behaviour, under which the undecided write destroyed its own
/// evidence: `signing_time_need` returned `None` on the second event and the
/// object was written `Untrusted` / *"the document carries no signing-time
/// field"*, permanently, from one transient blip during an upgrade.
#[test]
fn a_second_policy_event_re_reads_what_the_first_one_could_not() {
    // ---- EVENT 1: the bucket does not answer ----------------------------
    let first = weirkeeper::verification::retrust_with(
        &pre_signedat_status(),
        &org_default(),
        backup_badge,
        None,
        Some(1),
        at("2026-09-18T13:14:39Z"),
        &weirkeeper::verification::SigningTime::Unreadable(
            "the evidence object is not in the archive; nothing was verified".to_string(),
        ),
    )
    .expect("an undecided verdict is a change from the stored Valid");
    assert_eq!(first.to, "NotAttempted");
    assert_eq!(first.verification["trust"]["basis"], json!("Unverified"));

    // The status the controller now holds is the one it just patched.
    let mut stored = pre_signedat_status();
    stored["evidence"]["verification"] = first.verification.clone();

    // ---- EVENT 2: the pass still knows this status predates `signedAt` ---
    let need = weirkeeper::verification::signing_time_owed(Some(&stored)).expect(
        "an `Unverified` basis is this pass's own mark for `I have not read the document yet`, \
         and reading it back as a document's absence is how one blip became permanent",
    );
    assert_eq!(need.payload_key, LAB_RECEIPT_KEY);

    // …and with the bucket answering, the object is repaired.
    let second = weirkeeper::verification::retrust_with(
        &stored,
        &org_default(),
        backup_badge,
        Some(&vec![first.verified.clone()]),
        Some(1),
        at("2026-09-18T14:20:00Z"),
        &weirkeeper::verification::SigningTime::Recovered(at(RECEIPT_FINISHED_AT)),
    )
    .expect("the read succeeded, so the verdict changes");
    assert_eq!(second.from, "NotAttempted");
    assert_eq!(second.to, "Valid");
    assert_eq!(second.verification["signedAt"], json!(RECEIPT_FINISHED_AT));
    assert_eq!(second.verification["trust"]["basis"], json!("Current"));
    assert_eq!(second.verified.status, "True");
    assert_eq!(
        second.verification["verifiedAt"],
        json!("2026-09-18T13:14:23Z"),
        "two passes and still no re-observation"
    );

    // ---- AND A REPEAT INSIDE THE BACKOFF WRITES NOTHING (E11(d)) --------
    //
    // The controller's next reconcile is 15 s later, `trust.retryAfter` has not
    // passed, so the plan is `Deferred`: no `get`, the stored sentence and the
    // stored backoff carried forward, and the rendered block identical.
    assert!(
        weirkeeper::verification::retrust_with(
            &stored,
            &org_default(),
            backup_badge,
            Some(&vec![first.verified.clone()]),
            Some(1),
            at("2026-09-18T13:15:00Z"),
            &weirkeeper::verification::SigningTime::Deferred,
        )
        .is_none(),
        "a deferred pass renders the identical block, so an archive that is still down costs \
         neither a `get` nor a write"
    );
}

/// **Review finding F6, the guard.** Neither reconcile hook aborts on a kube
/// API failure while resolving the evidence path: the error becomes the reason
/// no read was attempted, and the verdict is still re-derived on that pass.
///
/// A SOURCE SCAN, because reaching the error arm needs a `kube::Client` that
/// fails only on the destination read, and the property is structural: this
/// hook must not own a `?`. The window scanned is exactly the hunk between the
/// need and the read, so an unrelated `?` elsewhere in either file is invisible
/// to it.
///
/// KILLS: "`…evidence_source(…).await.map_err(BackupError::Api)?`" — the shipped
/// shape, under which a transient API blip skipped the whole re-trust pass and
/// delayed a `KeyCompromise` revocation on exactly the objects this hook exists
/// for.
#[test]
fn an_evidence_path_api_failure_does_not_abort_the_re_trust_pass() {
    for relative in [
        "crates/weirkeeper/src/controllers/backup.rs",
        "crates/weirkeeper/src/controllers/restore.rs",
    ] {
        let src = read(relative);
        let start = src
            .find("signing_time_need(")
            .unwrap_or_else(|| panic!("{relative} invokes the re-derivation"));
        let end = src[start..]
            .find("recover_signing_time(")
            .map(|i| start + i)
            .unwrap_or_else(|| panic!("{relative} performs the bounded read"));
        let hunk = &src[start..end];
        assert!(
            hunk.contains("evidence_path_unreadable"),
            "{relative}: the evidence path's own failure has to become a `NotAttempted` reason, \
             not the reconcile's error. Hunk:\n{hunk}"
        );
        assert!(
            !hunk.contains("map_err"),
            "{relative}: a `?` here skips `apply_retrust` entirely, so a revocation waits for a \
             healthy API on the one class of object that cannot afford to. Hunk:\n{hunk}"
        );
    }
    // …and the sentence it produces says both things an operator needs.
    let detail = weirkeeper::verification::evidence_path_unreadable(
        &kube::Error::LinesCodecMaxLineLengthExceeded,
    );
    assert!(detail.contains("no re-read was attempted"), "{detail}");
    assert!(detail.contains("next policy event"), "{detail}");
}

// ===========================================================================
// TRUST-UPGRADE-SIGNEDAT, the THIRD shape — lab-refresh-3 §9
// ===========================================================================

/// The key id the lab's five 2026-09-14 objects name, as
/// `artifacts/lab-refresh-3/trust-selfheal/*.json` record it.
const LAB_MATCHED_KEY_ID: &str = "2c76e22ff89969dc0337e64756c85f18edb3e51ae2950ea18d81021d7176d7fe";

/// The `detail` the intermediate `e7d0e79` build stamped on all five, verbatim
/// — the sentence this defect exists to stop being told about sound archives.
fn lab_detail(payload_type: &str) -> String {
    format!(
        "the signature over this document verified under key {LAB_MATCHED_KEY_ID}, and the trust \
         policy legacy-roster-v1 does not accept it (SignedOutsideValidity): the document \
         carries no signing-time field for its payload type, so the key's validity window could \
         not be checked ({payload_type})"
    )
}

/// **`Backup/rotated-secret-backup`, verbatim** from
/// `artifacts/lab-refresh-3/trust-selfheal/backup-rotated-secret-backup.json`:
/// `result: Untrusted`, a `trust` block whose `basis` is the literal string
/// `"None"`, and **no `signedAt`**. The digest is the checked-in receipt
/// fixture's so the repaired path can be exercised end to end; every other
/// field is the lab's own.
fn lab_backup_status() -> Value {
    json!({
        "phase": "Succeeded",
        "exitCode": 0,
        "exitReason": "Ok",
        "backupId": "6424f937-7554-4976-9c1f-b02410e3c22a",
        "evidence": {
            "receiptKey": "logweir/backups/6424f937-7554-4976-9c1f-b02410e3c22a/01M2H5KH9G29EM0RANHYD4F4H9.receipt.json",
            "receiptSha256": sha256_prefixed(receipt_bytes().as_bytes()),
            "sidecarKey": "logweir/backups/6424f937-7554-4976-9c1f-b02410e3c22a/01M2H5KH9G29EM0RANHYD4F4H9.receipt.sig",
            "verification": {
                "detail": lab_detail("application/vnd.logweir.backup-receipt+json;version=1.0.0"),
                "matchedKeyId": FIXTURE_KEY_ID,
                "payloadType": "application/vnd.logweir.backup-receipt+json;version=1.0.0",
                "result": "Untrusted",
                "trust": {
                    "basis": "None",
                    "keyState": "Active",
                    "policy": {"name": "legacy-roster-v1"},
                },
                "verifiedAt": "2026-09-14T23:56:31Z",
            }
        }
    })
}

/// **`Restore/scram-record-restore`, verbatim** from the same directory — the
/// same third shape on the other kind, whose evidence keys are
/// `scorecardKey`/`scorecardSha256`.
fn lab_restore_status() -> Value {
    json!({
        "phase": "Succeeded",
        "outcome": "pass",
        "exitCode": 0,
        "exitReason": "Ok",
        "evidence": {
            "offsetReportKey": "logweir/drills/01M2H5MMSKY5JR6RW108EGN954.offsets.json",
            "scorecardKey": "logweir/drills/01M2H5MMSKY5JR6RW108EGN954.json",
            "scorecardSha256": sha256_prefixed(read("e2e/fixtures/signed/scorecard.json").as_bytes()),
            "sidecarKey": "logweir/drills/01M2H5MMSKY5JR6RW108EGN954.sig",
            "verification": {
                "detail": lab_detail("application/vnd.logweir.drill-scorecard+json;version=1.0.0"),
                "matchedKeyId": FIXTURE_KEY_ID,
                "payloadType": "application/vnd.logweir.drill-scorecard+json;version=1.0.0",
                "result": "Untrusted",
                "trust": {
                    "basis": "None",
                    "keyState": "Active",
                    "policy": {"name": "legacy-roster-v1"},
                },
                "verifiedAt": "2026-09-14T23:57:10Z",
            }
        }
    })
}

/// **THE ROW THREE LAB REFRESHES FAILED.** The five objects
/// `lab-refresh-3.result.md` §9 measured carry a `trust` block whose `basis` is
/// the literal `"None"` — stamped by the intermediate `e7d0e79` build's own
/// re-derivation, not by a document that claims nothing — and they end `Valid`
/// with the document's own `signedAt` after one bounded re-read, on both kinds.
///
/// KILLS: "a `trust` block means a build that knows `signedAt` wrote this" (the
/// first shipped discriminator) and "only the literal `Unverified` means
/// undecided" (the second). Under either, `signing_time_need` below returns
/// `None`, no `get` is ever issued, and these five objects stay `Untrusted`
/// forever — which is exactly what three consecutive lab runs reported.
#[test]
fn the_labs_intermediate_build_shape_is_repaired_by_one_read() {
    /// One lab object: its name, its verbatim status, its kind's badge rule,
    /// the document it owns and that document's own claimed signing time.
    type LabRow = (
        &'static str,
        Value,
        fn(&Value) -> weirkeeper::verification::Badge,
        &'static str,
        &'static str,
    );
    let rows: [LabRow; 2] = [
        (
            "Backup/rotated-secret-backup",
            lab_backup_status(),
            backup_badge,
            "logweir/backups/6424f937-7554-4976-9c1f-b02410e3c22a/01M2H5KH9G29EM0RANHYD4F4H9.receipt.json",
            RECEIPT_FINISHED_AT,
        ),
        (
            "Restore/scram-record-restore",
            lab_restore_status(),
            restore_badge,
            "logweir/drills/01M2H5MMSKY5JR6RW108EGN954.json",
            SCORECARD_SIGNED_AT,
        ),
    ];
    for (label, stored, badge, key, signed_at) in rows {
        // ---- EVENT 1: the archive does not answer -----------------------
        let need =
            weirkeeper::verification::signing_time_owed(Some(&stored)).unwrap_or_else(|| {
                panic!(
                    "{label}: `basis: None` with no `signedAt` is the intermediate build's own \
                 re-derivation, not a document that claims nothing — and reading it as the \
                 latter is what left five sound archives Untrusted across three refreshes"
                )
            });
        assert_eq!(need.payload_key, key, "{label}: its OWN document");

        let first = weirkeeper::verification::retrust_with(
            &stored,
            &org_default(),
            badge,
            None,
            Some(1),
            at("2026-09-19T09:00:00Z"),
            &weirkeeper::verification::SigningTime::Unreadable(
                "the evidence object is not in the archive; nothing was verified".to_string(),
            ),
        )
        .unwrap_or_else(|| panic!("{label}: an undecided verdict differs from the stored one"));
        assert_eq!(first.from, "Untrusted", "{label}");
        assert_eq!(
            first.to, "NotAttempted",
            "{label}: nothing was read, so nothing is claimed and nothing is refused"
        );
        assert_eq!(
            first.verification["trust"]["basis"],
            json!("Unverified"),
            "{label}"
        );
        assert!(
            !first.verification["detail"]
                .as_str()
                .is_some_and(|d| d.contains("the document carries no signing-time field")),
            "{label}: the sentence that was never true of these receipts is gone. Got {}",
            first.verification["detail"]
        );

        // ---- EVENT 2: the archive answers, and the object is repaired ----
        let mut after_first = stored.clone();
        after_first["evidence"]["verification"] = first.verification.clone();
        assert!(
            weirkeeper::verification::signing_time_owed(Some(&after_first)).is_some(),
            "{label}: and the retry survives its own output (review F2)"
        );
        let second = weirkeeper::verification::retrust_with(
            &after_first,
            &org_default(),
            badge,
            Some(&vec![first.verified.clone()]),
            Some(1),
            at("2026-09-19T09:30:00Z"),
            &weirkeeper::verification::SigningTime::Recovered(at(signed_at)),
        )
        .unwrap_or_else(|| panic!("{label}: the read succeeded, so the verdict changes"));
        assert_eq!(second.to, "Valid", "{label}: repaired");
        assert_eq!(second.verification["signedAt"], json!(signed_at), "{label}");
        assert_eq!(
            second.verification["trust"]["basis"],
            json!("Current"),
            "{label}"
        );
        assert_eq!(
            second.verified.status, "True",
            "{label}: and it renders green"
        );
        assert_eq!(
            second.verification["verifiedAt"], stored["evidence"]["verification"]["verifiedAt"],
            "{label}: two events and still no re-observation"
        );

        // ---- AND IT SETTLES: no third read, no third write ---------------
        let mut repaired = stored.clone();
        repaired["evidence"]["verification"] = second.verification.clone();
        assert!(
            weirkeeper::verification::signing_time_owed(Some(&repaired)).is_none(),
            "{label}: a repaired object never asks for the archive again"
        );
    }
}

/// A document that genuinely claims no signing time is written by a CURRENT
/// build as `basis: None` with no `signedAt` — the same bytes as the lab's
/// legacy re-stamp — so it is re-read once. **And exactly once:** the completed
/// read records `trust.signingTimeRead: absent`, which says the absence is the
/// DOCUMENT's, and the object never asks the archive again.
///
/// KILLS: "leave the block unchanged after a fruitless read" — review finding
/// **G2**. A terminal object reconciles every `REQUEUE_SECS`, so without the
/// marker below this document cost one `Store::get` every 15 seconds, for ever,
/// for a verdict that provably cannot change.
#[test]
fn a_fruitless_re_read_settles_the_object_for_good() {
    let absent = || {
        weirkeeper::verification::SigningTime::Absent(
            logweir_core::trust::ClaimAbsence::FieldAbsent,
        )
    };
    // PASS 1 — the read comes back with the document's own absence. The block
    // settles on `Untrusted` / basis `None`, which is where it already was in
    // substance; this pass only rewrites the policy's own name into the
    // sentence.
    let first = weirkeeper::verification::retrust_with(
        &lab_backup_status(),
        &org_default(),
        backup_badge,
        None,
        Some(1),
        at("2026-09-19T09:00:00Z"),
        &absent(),
    )
    .expect("the fixture's stored detail names a different policy, so one patch settles it");
    assert_eq!(first.to, "Untrusted");
    assert_eq!(first.verification["trust"]["basis"], json!("None"));

    // PASS 2 — the SAME fruitless read over the settled block renders
    // identical bytes, so nothing is sent. The cost of not being able to tell
    // the two shapes apart on the status is one `get`, never a write.
    let mut settled = lab_backup_status();
    settled["evidence"]["verification"] = first.verification.clone();
    let second = weirkeeper::verification::retrust_with(
        &settled,
        &org_default(),
        backup_badge,
        Some(&vec![first.verified.clone()]),
        Some(1),
        at("2026-09-19T10:00:00Z"),
        &absent(),
    );
    assert!(
        second.is_none(),
        "the re-read confirmed what the block already said, so erratum E11(d) sends no patch. \
         Got {second:?}"
    );
    // ---- AND THE QUESTION IS CLOSED -----------------------------------
    assert_eq!(
        first.verification["trust"]["signingTimeRead"],
        json!("absent"),
        "the completed read records its ANSWER, so the question is not asked again"
    );
    assert!(
        weirkeeper::verification::signing_time_owed(Some(&settled)).is_none(),
        "one read, then silence: a terminal object reconciles every REQUEUE_SECS, so an object \
         that kept asking would cost one archive `get` every 15 seconds for ever"
    );
    assert_eq!(
        weirkeeper::verification::signing_time_need(Some(&settled), at("2027-01-01T00:00:00Z")),
        weirkeeper::verification::ReadPlan::None,
        "…at any later instant, because this is an answer and not a backoff"
    );
}

/// **Review finding G2, the guard.** Three reconciles with an unreachable
/// archive issue **exactly one** `Store::get`.
///
/// # Why this counts real reads and not a plan enum
///
/// `ReadPlan::Read` is the ONLY route to `read_signing_time`, which is the only
/// thing in this module that calls `Store::get` — so the loop below performs a
/// real read through a real read-only handle whenever the plan says to, over an
/// evidence tree the object is missing from, and counts the reads it made. A
/// deferred pass never reaches the handle at all, which is also why it costs no
/// `evidence_source` resolution in the controller.
///
/// KILLS: "re-read on every pass" — the shipped behaviour. A terminal object
/// reconciles every `REQUEUE_SECS` (15 s), so the three passes below are 30
/// seconds of one object's life; at the shipped rate a namespace holding a
/// thousand such objects issued roughly 67 archive GETs a second, for ever, for
/// verdicts that provably cannot change.
#[test]
fn three_reconciles_with_an_unreachable_archive_issue_one_get() {
    let tree = EvidenceTree::new("g2", b"unused", b"unused");
    let store = tree.handle();
    tree.remove(PAYLOAD_KEY);

    let mut status = pre_signedat_status();
    // The object this run owns is the one the tree does not have.
    status["evidence"]["receiptKey"] = json!(PAYLOAD_KEY);

    let mut reads = 0usize;
    let mut conditions: Vec<weirkeeper::crds::Condition> = Vec::new();
    let mut patches = 0usize;
    // Three reconciles, `REQUEUE_SECS` apart, exactly as the controller requeues.
    for minute in [0, 15, 30] {
        let now = at("2026-09-19T12:00:00Z") + chrono::Duration::seconds(minute);
        let signing_time = match weirkeeper::verification::signing_time_need(Some(&status), now) {
            weirkeeper::verification::ReadPlan::None => {
                panic!("a pre-`signedAt` block always owes a read at {now}")
            }
            weirkeeper::verification::ReadPlan::Deferred => {
                weirkeeper::verification::SigningTime::Deferred
            }
            weirkeeper::verification::ReadPlan::Read(need) => {
                reads += 1;
                weirkeeper::verification::read_signing_time(Some(&store), &need)
            }
        };
        if let Some(outcome) = weirkeeper::verification::retrust_with(
            &status,
            &org_default(),
            backup_badge,
            Some(&conditions),
            Some(1),
            now,
            &signing_time,
        ) {
            patches += 1;
            status["evidence"]["verification"] = outcome.verification.clone();
            conditions = vec![outcome.verified.clone()];
        }
    }
    assert_eq!(
        reads, 1,
        "ONE bounded re-read, and the bound is `trust.retryAfter` on the object rather than a \
         hope that reconciles are rare"
    );
    assert_eq!(
        patches, 1,
        "and one patch: the first pass records the backoff, and the two deferred passes render \
         the identical block"
    );
    let retry_after = status["evidence"]["verification"]["trust"]["retryAfter"]
        .as_str()
        .expect("a fruitless attempt records when the next one is due");
    assert_eq!(
        retry_after, "2026-09-19T12:15:00Z",
        "fifteen minutes is sixty reconciles, so the steady cost is one `get` per object per \
         quarter hour instead of one every fifteen seconds"
    );

    // ---- AND THE ARCHIVE COMING BACK IS NOTICED WITHIN ONE WINDOW -------
    assert_eq!(
        weirkeeper::verification::signing_time_need(Some(&status), at("2026-09-19T12:14:59Z")),
        weirkeeper::verification::ReadPlan::Deferred,
        "one second before the instant, still barred"
    );
    assert!(
        matches!(
            weirkeeper::verification::signing_time_need(Some(&status), at("2026-09-19T12:15:00Z")),
            weirkeeper::verification::ReadPlan::Read(_)
        ),
        "and due at it — a bucket that comes back is picked up, not waited on for ever"
    );
}

/// A backoff must never outlive the thing it is bounding: a read that SUCCEEDS
/// clears it, and a `KeyCompromise` revocation is not delayed by one.
///
/// KILLS: "carry `retryAfter` forward unconditionally" — a repaired object
/// would keep a stale instant nobody reads; and "defer before the key rows" —
/// the ratchet has to survive the backoff, because a stolen key may not wait
/// fifteen minutes.
#[test]
fn a_backoff_does_not_outlive_its_purpose() {
    let mut deferred_status = pre_signedat_status();
    deferred_status["evidence"]["verification"]["trust"] =
        json!({"basis": "Unverified", "keyState": "Active", "retryAfter": "2099-01-01T00:00:00Z"});
    deferred_status["evidence"]["verification"]["result"] = json!("NotAttempted");

    // ---- A SUCCESSFUL READ CLEARS IT --------------------------------------
    let repaired = weirkeeper::verification::retrust_with(
        &deferred_status,
        &org_default(),
        backup_badge,
        None,
        Some(1),
        at("2026-09-19T12:00:00Z"),
        &weirkeeper::verification::SigningTime::Recovered(at(RECEIPT_FINISHED_AT)),
    )
    .expect("the read succeeded, so the verdict changes");
    assert_eq!(repaired.to, "Valid");
    assert_eq!(
        repaired.verification["trust"].get("retryAfter"),
        None,
        "there is nothing left to retry, so the instant goes with the question"
    );
    assert_eq!(repaired.verification["trust"].get("signingTimeRead"), None);

    // ---- AND THE RATCHET IGNORES IT ---------------------------------------
    let effective = at("2026-09-10T00:00:00Z");
    let mut key = evidence_key();
    key.state = KeyState::Revoked;
    key.revoked_at = Some(effective);
    key.revocation_reason = Some(RevocationReason::KeyCompromise);
    key.revocation_effective_from = Some(effective);
    let revoked = weirkeeper::verification::retrust_with(
        &deferred_status,
        &Resolution::Trust(Box::new(resolved(&policy("org-default", &[], vec![key])))),
        backup_badge,
        None,
        Some(1),
        at("2026-09-19T12:00:00Z"),
        // Deferred: no read happened, and none is due for another 73 years.
        &weirkeeper::verification::SigningTime::Deferred,
    )
    .expect("a compromise revocation always changes a NotAttempted verdict");
    assert_eq!(
        revoked.to, "Untrusted",
        "a stolen private half does not wait for a backoff: `decide`'s compromise rows run \
         before the row that defers, and they never consult the claim"
    );
    assert_ne!(revoked.verification["trust"]["basis"], json!("Unverified"));
}

/// **The re-verification's hardening nit.** A stored `trust.retryAfter` can
/// only ever mean "at most `RETRY_AFTER_SECS` from now", so a hand-written
/// far-future instant defers one window and is then rewritten to a real one.
///
/// A forged value was already fail-closed — a deferred pass still re-derives
/// the verdict, so it can neither hold a `Valid`, nor render green, nor delay a
/// `KeyCompromise` revocation (both asserted below). What the clamp removes is
/// the remaining denial: "this object can never be repaired".
///
/// KILLS: "believe the stored instant" — the plan below would stay `Deferred`
/// at every instant this century, and the block would carry `2099` for ever.
#[test]
fn a_stored_retry_after_is_clamped_to_one_window() {
    let now = at("2026-09-19T12:00:00Z");
    let mut forged = pre_signedat_status();
    forged["evidence"]["verification"]["result"] = json!("NotAttempted");
    forged["evidence"]["verification"]["detail"] = json!("the bucket did not answer");
    forged["evidence"]["verification"]["trust"] = json!({
        "basis": "Unverified",
        "keyState": "Active",
        "policy": {"name": "org-default", "uid": "uid-org-default", "generation": 4},
        "retryAfter": "2099-01-01T00:00:00Z",
    });

    // ---- IT DEFERS NOTHING. A `min()` would have deferred for ever in
    // ---- fifteen-minute steps, because the ceiling moves with `now`.
    assert!(
        matches!(
            weirkeeper::verification::signing_time_need(Some(&forged), now),
            weirkeeper::verification::ReadPlan::Read(_)
        ),
        "a value above the ceiling is not one this controller wrote, so it is honoured as no \
         backoff at all and the read happens on this very pass — `2099` is not a denial this \
         field may grant"
    );
    assert!(
        matches!(
            weirkeeper::verification::signing_time_need(
                Some(&forged),
                now + chrono::Duration::seconds(900)
            ),
            weirkeeper::verification::ReadPlan::Read(_)
        ),
        "…and at no later instant either"
    );

    // ---- and the attempt it does not defer writes a real instant -----------
    let attempted = weirkeeper::verification::retrust_with(
        &forged,
        &org_default(),
        backup_badge,
        None,
        Some(1),
        now,
        &weirkeeper::verification::SigningTime::Unreadable("the bucket did not answer".to_string()),
    )
    .expect("the forged instant differs from a real one, so it is rewritten");
    assert_eq!(
        attempted.verification["trust"]["retryAfter"],
        json!("2026-09-19T12:15:00Z"),
        "the object self-heals to the instant this controller would itself have written"
    );
    assert_eq!(
        attempted.to, "NotAttempted",
        "a forged backoff was fail-closed to begin with: the pass still re-derives"
    );

    // ---- a legitimate value is carried unchanged, so the pass stays silent --
    let mut honest = forged.clone();
    honest["evidence"]["verification"]["trust"]["retryAfter"] = json!("2026-09-19T12:10:00Z");
    let first = weirkeeper::verification::retrust_with(
        &honest,
        &org_default(),
        backup_badge,
        None,
        Some(1),
        now,
        &weirkeeper::verification::SigningTime::Deferred,
    )
    .expect("the stored detail names a different policy, so one patch settles it");
    assert_eq!(
        first.verification["trust"]["retryAfter"],
        json!("2026-09-19T12:10:00Z"),
        "below the ceiling, so carried verbatim"
    );
    let mut settled = honest.clone();
    settled["evidence"]["verification"] = first.verification.clone();
    assert!(
        weirkeeper::verification::retrust_with(
            &settled,
            &org_default(),
            backup_badge,
            Some(&vec![first.verified.clone()]),
            Some(1),
            now + chrono::Duration::seconds(15),
            &weirkeeper::verification::SigningTime::Deferred,
        )
        .is_none(),
        "…and the next deferred reconcile renders the identical block and writes nothing"
    );

    // ---- the ratchet ignores it entirely -----------------------------------
    let effective = at("2026-09-10T00:00:00Z");
    let mut key = evidence_key();
    key.state = KeyState::Revoked;
    key.revoked_at = Some(effective);
    key.revocation_reason = Some(RevocationReason::KeyCompromise);
    key.revocation_effective_from = Some(effective);
    let revoked = weirkeeper::verification::retrust_with(
        &forged,
        &Resolution::Trust(Box::new(resolved(&policy("org-default", &[], vec![key])))),
        backup_badge,
        None,
        Some(1),
        now,
        &weirkeeper::verification::SigningTime::Deferred,
    )
    .expect("a compromise revocation always changes a NotAttempted verdict");
    assert_eq!(revoked.to, "Untrusted");
}
