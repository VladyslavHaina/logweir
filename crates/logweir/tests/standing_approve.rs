//! PLAT-14.3b fix round 1, prerequisite **P0** — `logweir approve --standing`.
//!
//! Nothing in the product minted a signed `StandingRehearsalAuthorization`
//! before this. `logweir approve` signed only `PAYLOAD_TYPE_APPROVAL`, while
//! the `Approval` controller REQUIRES
//! `PAYLOAD_TYPE_STANDING_AUTHORIZATION` for a `RehearsalSchedule` referent
//! and the runner refuses anything else — so the only producers were Rust test
//! fixtures, and no operator could authorise a rehearsal at all.
//!
//! These rows mint through the shipped code path and then drive the RUNNER's
//! own verification over the produced bytes. The CONTROLLER's half of the same
//! bytes is asserted in `crates/weirkeeper/tests/standing_restore.rs` against
//! the shared fixture this file also pins
//! (`crates/logweir-core/tests/fixtures/standing-authorization.json`) —
//! `weirkeeper` cannot depend on `logweir` (that is what keeps the signer out
//! of the controller), so the two sides meet on bytes rather than on a call.

use logweir::approve::{mint_standing, ApproveArgs, StandingArgs};
use logweir_core::execution_contract as wire;
use logweir_evidence::keys::SigningKey;
use std::path::{Path, PathBuf};

/// The shared fixture both crates read. Public data only — the envelope, with
/// no signature and no key material — so it can live in the tree.
const SHARED_FIXTURE: &str = "../logweir-core/tests/fixtures/standing-authorization.json";

const NS: &str = "logweir-d3-w7";
const SCHEDULE: &str = "weekly-orders";
const SCHEDULE_UID: &str = "3f2a91c7-1111-4222-8333-444444444444";
const TARGET_CLUSTER_ID: &str = "TARGET00000000000000000";
const PREFIX: &str = "rehearsal-3f2a91c7-";

/// The instant the committed fixture was minted at. Passed explicitly so the
/// bytes are reproducible; every clock-relative check takes `now` as an
/// argument on both sides (Global Constraint 1), so a fixed instant here costs
/// nothing and buys a byte-comparable artifact.
fn issued_at() -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::parse_from_rfc3339("2026-09-01T00:00:00Z")
        .expect("a fixture instant")
        .with_timezone(&chrono::Utc)
}

fn scope_json() -> String {
    serde_json::to_string_pretty(&serde_json::json!({
        "templateDigest": "sha256:aa",
        "targetClusterId": TARGET_CLUSTER_ID,
        "topicPrefix": PREFIX,
        "topics": ["orders"],
        "maxPartitions": 200,
        "recordsPerPartition": 25,
        "deadlineSeconds": 3600,
        "modes": ["scratch"],
    }))
    .expect("the fixture scope serialises")
}

struct Minted {
    _dir: tempfile::TempDir,
    envelope: Vec<u8>,
    sidecar: Vec<u8>,
    keys: Vec<u8>,
    key_id: String,
}

fn args_for(dir: &Path, scope: &Path, key: &Path, valid_days: i64) -> ApproveArgs {
    ApproveArgs {
        spec: None,
        key: key.to_path_buf(),
        approver: String::new(),
        ticket: String::new(),
        out: dir.join("standing-authorization.json"),
        subject_kind: "RehearsalSchedule".to_string(),
        standing: Some(StandingArgs {
            schedule_namespace: NS.to_string(),
            schedule_name: SCHEDULE.to_string(),
            schedule_uid: SCHEDULE_UID.to_string(),
            scope: scope.to_path_buf(),
            valid_days,
            issued_at: Some(issued_at()),
        }),
    }
}

/// Mint with the shipped code path, and build the keyring the bundle would
/// carry for the key that signed it.
fn mint(valid_days: i64) -> Minted {
    let dir = tempfile::tempdir().expect("a temp dir");
    let scope = dir.path().join("scope.json");
    std::fs::write(&scope, scope_json()).expect("the scope is written");
    let signer = SigningKey::generate_ed25519();
    let key = dir.path().join("approver.pem");
    std::fs::write(&key, signer.to_pkcs8_pem().expect("a private PEM")).expect("written");

    let args = args_for(dir.path(), &scope, &key, valid_days);
    mint_standing(&args).expect("the authorization mints");

    let out = dir.path().join("standing-authorization.json");
    let keys = serde_json::to_vec(&serde_json::json!({
        "formatVersion": "1.0.0",
        "keys": [{
            "keyId": signer.key_id(),
            "publicKeyPem": signer.verifying_key().to_public_key_pem().expect("a public PEM"),
            "usages": ["GovernedApproval"],
        }],
    }))
    .expect("the keyring serialises");

    Minted {
        envelope: std::fs::read(&out).expect("the envelope was written"),
        sidecar: std::fs::read(out.with_extension("sig")).expect("the sidecar was written"),
        keys,
        key_id: signer.key_id(),
        _dir: dir,
    }
}

fn plan() -> logweir_core::spec::DrillSpec {
    serde_yaml::from_str(&format!(
        "source:\n  storage:\n    backend: s3\n    bucket: archives\n    prefix: logweir/\n\
         \n  backup: latestCompleted\n  topics: [orders]\ntarget:\n  bootstrap_servers: \
         [kafka-target:9092]\n  mode: scratch\n  topic_mapping_prefix: \"{PREFIX}\"\n  \
         marker_topic: logweir.scratch\n  default_replication_factor: 1\n  teardown: \
         delete\nsample:\n  window_start: \"2026-09-20T00:00:00Z\"\n  window_end: \
         \"2026-09-20T02:00:00Z\"\n  records_per_partition: 25\n  max_partitions: 200\n  \
         anchor: head\nobjectives: {{}}\nevidence:\n  backend: s3\n  bucket: archives\n  \
         prefix: logweir/\n"
    ))
    .expect("the fixture plan parses")
}

fn allowed() -> logweir_core::spec::AllowedClusters {
    logweir_core::spec::AllowedClusters {
        allowed_cluster_ids: vec![TARGET_CLUSTER_ID.to_string()],
        source_cluster_id: None,
    }
}

/// **THE P0 ROW.** Bytes this command mints are bytes the RUNNER accepts —
/// signature, key usage, kind, subject UID, window and `plan ∈ scope`, through
/// the runner's own entry point and not a paraphrase of it.
#[test]
fn a_minted_standing_authorization_is_accepted_by_the_runner() {
    let minted = mint(30);
    let verified = logweir::drill::binding::verify_standing_authorization(
        &plan(),
        &allowed(),
        &minted.envelope,
        &minted.sidecar,
        &minted.keys,
        Some(SCHEDULE_UID),
        // Inside the window the fixture was minted for.
        issued_at() + chrono::Duration::days(1),
    )
    .expect("the runner admits a document this product minted");

    assert_eq!(
        verified.key_id, minted.key_id,
        "the key that VERIFIED is the one that signed"
    );
    assert_eq!(verified.document.subject_ref.uid, SCHEDULE_UID);
    assert_eq!(verified.document.subject_ref.name, SCHEDULE);
    assert_eq!(verified.document.scope.target_cluster_id, TARGET_CLUSTER_ID);
    assert_eq!(verified.document.scope.deadline_seconds, 3600);
}

/// The payload type is the whole point of the second signer path: a standing
/// document is NOT verifiable as a per-run approval, and an approval is not
/// replayable as a standing authorization.
#[test]
fn the_standing_document_is_signed_under_its_own_payload_type() {
    let minted = mint(30);
    let sidecar: logweir_evidence::Sidecar =
        serde_json::from_slice(&minted.sidecar).expect("the sidecar parses");
    let key: logweir_evidence::keys::VerifyingKey = {
        let keyring: wire::AuthorizationKeyring =
            serde_json::from_slice(&minted.keys).expect("the keyring parses");
        logweir_evidence::keys::VerifyingKey::from_pem_str(&keyring.keys[0].public_key_pem)
            .expect("the public key parses")
    };
    logweir_evidence::verify::verify_detached(
        &key,
        wire::PAYLOAD_TYPE_STANDING_AUTHORIZATION,
        &minted.envelope,
        &sidecar,
    )
    .expect("it verifies under the STANDING payload type");
    let replayed = logweir_evidence::verify::verify_detached(
        &key,
        logweir::drill::phase1_approval::PAYLOAD_TYPE_APPROVAL,
        &minted.envelope,
        &sidecar,
    );
    assert!(
        replayed.is_err(),
        "a standing document must not verify as a per-run approval — that mismatch is what \
         stops one being replayed as the other"
    );
}

/// **The shared byte fixture.** `weirkeeper` cannot depend on `logweir`, so the
/// controller's rows read these exact bytes from the tree. This row re-mints
/// them and requires byte equality, which is what stops the fixture drifting
/// away from what the product actually emits.
#[test]
fn the_committed_fixture_is_what_this_command_mints_today() {
    let minted = mint(30);
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(SHARED_FIXTURE);
    let committed = std::fs::read(&path).unwrap_or_else(|e| {
        panic!(
            "the shared standing-authorization fixture is missing at {}: {e}",
            path.display()
        )
    });
    assert_eq!(
        String::from_utf8_lossy(&minted.envelope),
        String::from_utf8_lossy(&committed),
        "the committed fixture and `logweir approve --standing` have diverged; re-mint it \
         (the controller's rows in weirkeeper read these exact bytes)"
    );
    assert!(
        !committed.windows(7).any(|w| w == b"PRIVATE"),
        "the shared fixture is PUBLIC data: no key material, ever"
    );
}

/// Everything the command refuses BEFORE signing, because each is something
/// the cluster would refuse afterwards — and re-signing needs a key the
/// operator may not have twice.
#[test]
fn a_document_the_cluster_would_refuse_is_refused_before_it_is_signed() {
    let dir = tempfile::tempdir().expect("a temp dir");
    let scope = dir.path().join("scope.json");
    std::fs::write(&scope, scope_json()).expect("written");
    let signer = SigningKey::generate_ed25519();
    let key = dir.path().join("approver.pem");
    std::fs::write(&key, signer.to_pkcs8_pem().expect("a PEM")).expect("written");
    let base = |days: i64| args_for(dir.path(), &scope, &key, days);

    // D3 §4.3's ninety-day cap, and its floor.
    for (days, why) in [(91, "90"), (0, "minimum 1")] {
        let error = mint_standing(&base(days)).expect_err("a window outside the cap is refused");
        assert!(error.contains(why), "{days}: {error}");
    }

    // A blank UID is not a UID: it is what binds the document to ONE object.
    let mut blank = base(30);
    blank.standing.as_mut().expect("standing").schedule_uid = "  ".to_string();
    let error = mint_standing(&blank).expect_err("a blank subject UID is refused");
    assert!(error.contains("--schedule-uid is required"), "{error}");

    // A per-run approval's inputs, which this document does not bind.
    let mut with_spec = base(30);
    with_spec.spec = Some(PathBuf::from("drill.yaml"));
    assert!(mint_standing(&with_spec)
        .expect_err("--spec is refused")
        .contains("--spec is not used with --standing"));
    let mut with_ticket = base(30);
    with_ticket.ticket = "CHG-1".to_string();
    assert!(mint_standing(&with_ticket)
        .expect_err("--ticket is refused")
        .contains("would NOT be signed"));

    // The per-run approval's FILENAME, which is how a standing document ends
    // up pasted into the slot that makes it look substituted.
    let mut wrong_name = base(30);
    wrong_name.out = dir.path().join("approval.json");
    assert!(mint_standing(&wrong_name)
        .expect_err("approval.json is refused")
        .contains("standing-authorization.json"));

    // Scope bounds whose absence the runner treats as a MISMATCH.
    for (edit, why) in [
        (r#""modes": ["newTopic"]"#, "and nothing else"),
        (r#""maxPartitions": 0"#, "maxPartitions"),
        (r#""topics": []"#, "authorises the restore of nothing"),
        (r#""targetClusterId": """#, "targetClusterId"),
    ] {
        let field = edit.split(':').next().expect("a field").trim_matches('"');
        let mut value: serde_json::Value =
            serde_json::from_str(&scope_json()).expect("the scope parses");
        let edited: serde_json::Value =
            serde_json::from_str(&format!("{{{edit}}}")).expect("the edit parses");
        value[field] = edited[field].clone();
        let bad = dir.path().join(format!("scope-{field}.json"));
        std::fs::write(&bad, value.to_string()).expect("written");
        let mut args = args_for(dir.path(), &bad, &key, 30);
        args.out = dir.path().join(format!("sa-{field}.json"));
        let error = mint_standing(&args).unwrap_or_else(|e| e);
        assert!(error.contains(why), "{field}: {error}");
    }

    // And nothing was written for any of them.
    assert!(
        !dir.path().join("standing-authorization.json").exists(),
        "a refused mint writes no document"
    );
}
