//! PLAT-14.3b fix round 1, prerequisite **P0** — `logweir drill approve --standing`.
//!
//! Nothing in the product minted a signed `StandingRehearsalAuthorization`
//! before this. `logweir drill approve` signed only `PAYLOAD_TYPE_APPROVAL`, while
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
///
/// **FX-9.** Until 2026-10-05 the MINT side broke that rule: `mint_standing`
/// read the wall clock for its already-expired refusal, so these rows passed
/// only while the wall clock was inside this fixture's thirty days, and from
/// 2026-10-01 three of them failed. The mint now takes [`now`] like the
/// runner and the controller do.
fn issued_at() -> chrono::DateTime<chrono::Utc> {
    at("2026-09-01T00:00:00Z")
}

/// The caller's clock these rows hand `mint_standing`: one day into the
/// fixture's window, the same instant the runner row judges it at. Fixed, so
/// no row here depends on the date it runs.
fn now() -> chrono::DateTime<chrono::Utc> {
    issued_at() + chrono::Duration::days(1)
}

fn at(rfc3339: &str) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::parse_from_rfc3339(rfc3339)
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
        approval_subject: None,
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
    mint_scope(valid_days, &scope_json())
}

/// [`mint`] over any scope file text.
fn mint_scope(valid_days: i64, scope_text: &str) -> Minted {
    let dir = tempfile::tempdir().expect("a temp dir");
    let scope = dir.path().join("scope.json");
    std::fs::write(&scope, scope_text).expect("the scope is written");
    let signer = SigningKey::generate_ed25519();
    let key = dir.path().join("approver.pem");
    std::fs::write(&key, signer.to_pkcs8_pem().expect("a private PEM")).expect("written");

    let args = args_for(dir.path(), &scope, &key, valid_days);
    mint_standing(&args, now()).expect("the authorization mints");

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
///
/// **What it does NOT prove:** the keyring here is hand-built from the key that
/// signed, not rendered by `rehearsal_schedule::keyring`. This row is about the
/// ENVELOPE; the controller's rendering of `authorization-keys.json` is proved
/// separately by `rehearsal_controller.rs::the_rendered_bundle_is_what_the_runner_loads`.
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
        // Inside the window the fixture was minted for: the instant it was
        // minted at.
        now(),
    )
    .expect("the runner admits a document this product minted");

    assert_eq!(
        verified.key_id, minted.key_id,
        "the key that VERIFIED is the one that signed"
    );
    // **EVERY field of the document, read back from the bytes whose signature
    // just verified** — not from the struct the command built. A scope field
    // dropped or defaulted on the way through the signer would be a scope a
    // human did not sign, and the runner would enforce the wrong bound.
    let doc = &verified.document;
    assert_eq!(doc.format_version, "1.0.0");
    assert_eq!(doc.kind, "StandingRehearsalAuthorization");
    assert_eq!(doc.subject_ref.api_version, "logweir.dev/v1alpha1");
    assert_eq!(doc.subject_ref.kind, "RehearsalSchedule");
    assert_eq!(doc.subject_ref.namespace, NS);
    assert_eq!(doc.subject_ref.name, SCHEDULE);
    assert_eq!(doc.subject_ref.uid, SCHEDULE_UID);
    assert_eq!(doc.issued_at, issued_at());
    assert_eq!(doc.expires_at, issued_at() + chrono::Duration::days(30));

    // All eight scope fields, against the file the operator passed.
    let scope: serde_json::Value =
        serde_json::from_str(&scope_json()).expect("the fixture scope parses");
    assert_eq!(doc.scope.template_digest, scope["templateDigest"]);
    assert_eq!(doc.scope.target_cluster_id, scope["targetClusterId"]);
    assert_eq!(doc.scope.topic_prefix, scope["topicPrefix"]);
    assert_eq!(
        doc.scope.topics,
        vec!["orders".to_string()],
        "the topic list is carried verbatim"
    );
    assert_eq!(u64::from(doc.scope.max_partitions), scope["maxPartitions"]);
    assert_eq!(
        u64::from(doc.scope.records_per_partition),
        scope["recordsPerPartition"]
    );
    assert_eq!(
        u64::from(doc.scope.deadline_seconds),
        scope["deadlineSeconds"]
    );
    assert_eq!(doc.scope.modes, vec!["scratch".to_string()]);
}

/// **PROD-08.1a: a scope that signs complete coverage mints format 1.1.0, and
/// the runner admits a complete plan under it.** The scope file states
/// `coverage: complete` and `completeMaxRecords`; the document is minted at
/// 1.1.0 carrying both, read back from the verified bytes; a complete plan
/// inside the bound is admitted by the runner's own entry point and one past
/// it is refused. A scope stating `completeMaxRecords` without complete
/// coverage is refused BEFORE anything is signed.
///
/// KILLS: the minter writing 1.0.0 over a scope that carries the fields (the
/// runner then refuses the document); either field dropped on the way
/// through the signer; the record bound not enforced.
#[test]
fn a_scope_that_signs_complete_coverage_mints_1_1_and_the_runner_admits_its_plan() {
    let mut scope: serde_json::Value = serde_json::from_str(&scope_json()).expect("parses");
    scope["coverage"] = serde_json::json!("complete");
    scope["maxPartitions"] = serde_json::json!(0);
    scope["completeMaxRecords"] = serde_json::json!(5000);
    let minted = mint_scope(30, &scope.to_string());
    let mut complete = plan();
    complete.sample.max_partitions = None;
    complete.sample.coverage = logweir_core::spec::Coverage::Complete;
    complete.sample.complete_max_records = Some(5000);
    let verified = logweir::drill::binding::verify_standing_authorization(
        &complete,
        &allowed(),
        &minted.envelope,
        &minted.sidecar,
        &minted.keys,
        Some(SCHEDULE_UID),
        now(),
    )
    .expect("a complete plan inside the signed complete scope");
    assert_eq!(verified.document.format_version, "1.1.0");
    assert_eq!(
        verified.document.scope.coverage,
        Some(logweir_core::spec::Coverage::Complete)
    );
    assert_eq!(verified.document.scope.complete_max_records, Some(5000));

    complete.sample.complete_max_records = Some(5001);
    let refused = logweir::drill::binding::verify_standing_authorization(
        &complete,
        &allowed(),
        &minted.envelope,
        &minted.sidecar,
        &minted.keys,
        Some(SCHEDULE_UID),
        now(),
    )
    .expect_err("past the signed record bound");
    assert!(
        format!("{refused:?}").contains("may decode 5001 records"),
        "{refused:?}"
    );

    // A sampled plan (the 1.0.0 fixture's) is not the coverage this scope signed.
    let refused = logweir::drill::binding::verify_standing_authorization(
        &plan(),
        &allowed(),
        &minted.envelope,
        &minted.sidecar,
        &minted.keys,
        Some(SCHEDULE_UID),
        now(),
    )
    .expect_err("a weaker check than signed");
    assert!(
        format!("{refused:?}").contains("authorises `complete`"),
        "{refused:?}"
    );

    // A bound on a coverage the scope does not authorise is refused before signing.
    let dir = tempfile::tempdir().expect("a temp dir");
    let path = dir.path().join("scope.json");
    let mut incoherent: serde_json::Value = serde_json::from_str(&scope_json()).expect("parses");
    incoherent["completeMaxRecords"] = serde_json::json!(5000);
    std::fs::write(&path, incoherent.to_string()).expect("written");
    let signer = SigningKey::generate_ed25519();
    let key = dir.path().join("approver.pem");
    std::fs::write(&key, signer.to_pkcs8_pem().expect("a private PEM")).expect("written");
    let refused = mint_standing(&args_for(dir.path(), &path, &key, 30), now())
        .expect_err("an incoherent scope is not signed");
    assert!(refused.contains("does not authorise"), "{refused}");
    assert!(
        !dir.path().join("standing-authorization.json").exists(),
        "nothing was written"
    );
}

/// **PROD-08.1a review M1: the minter signs a complete scope only with
/// `"maxPartitions": 0`.** A complete scope with a positive bound is refused
/// before anything is written, naming the value to write — an older runner or
/// controller would read it as a sampled scope it could run sampled plans
/// under. With 0 it mints at 1.1.0 and the document carries 0. A SAMPLED scope
/// with 0 is still refused as a bound that admits nothing (the control: the
/// zero is allowed only beside complete).
///
/// KILLS: the minter's complete check removed (the wide scope is minted — the
/// shared admission would still refuse it, but with a reader's wording and
/// after the scope file was accepted); the zero allowed on a sampled scope.
#[test]
fn a_complete_scope_is_minted_only_with_a_partition_bound_of_zero() {
    let refused_with = |scope: serde_json::Value| -> String {
        let dir = tempfile::tempdir().expect("a temp dir");
        let path = dir.path().join("scope.json");
        std::fs::write(&path, scope.to_string()).expect("written");
        let signer = SigningKey::generate_ed25519();
        let key = dir.path().join("approver.pem");
        std::fs::write(&key, signer.to_pkcs8_pem().expect("a private PEM")).expect("written");
        let refused = mint_standing(&args_for(dir.path(), &path, &key, 30), now())
            .expect_err("refused before signing");
        assert!(
            !dir.path().join("standing-authorization.json").exists(),
            "nothing was written: {refused}"
        );
        refused
    };
    let mut wide: serde_json::Value = serde_json::from_str(&scope_json()).expect("parses");
    wide["coverage"] = serde_json::json!("complete");
    let refused = refused_with(wide.clone());
    assert!(
        refused.contains("`\"maxPartitions\": 200`")
            && refused.contains("states `\"maxPartitions\": 0`"),
        "{refused}"
    );

    let mut sampled_zero: serde_json::Value = serde_json::from_str(&scope_json()).expect("parses");
    sampled_zero["maxPartitions"] = serde_json::json!(0);
    let refused = refused_with(sampled_zero);
    assert!(refused.contains("`maxPartitions` is 0"), "{refused}");

    wide["maxPartitions"] = serde_json::json!(0);
    let minted = mint_scope(30, &wide.to_string());
    let doc: serde_json::Value = serde_json::from_slice(&minted.envelope).expect("JSON");
    assert_eq!(doc["formatVersion"], "1.1.0", "{doc}");
    assert_eq!(doc["scope"]["maxPartitions"], 0, "{doc}");
    assert_eq!(doc["scope"]["coverage"], "complete", "{doc}");
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
        "the committed fixture and `logweir drill approve --standing` have diverged; re-mint it \
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
        let error =
            mint_standing(&base(days), now()).expect_err("a window outside the cap is refused");
        assert!(error.contains(why), "{days}: {error}");
    }

    // **AN ALREADY-EXPIRED DOCUMENT IS NEVER SIGNED.** `--issued-at` exists so
    // a test can mint the same bytes twice, but it can also move the window
    // out from under the document — and `admit_standing_authorization` is
    // handed `issued_at` as its clock, so it answers "self-consistent", not
    // "valid now". Signing this would spend a key on bytes every reader
    // refuses, which is the one thing this command promises not to do.
    let mut long_past = base(30);
    long_past.standing.as_mut().expect("standing").issued_at = Some(at("2020-01-01T00:00:00Z"));
    let out_past = dir.path().join("sa-past.json");
    long_past.out = out_past.clone();
    let error =
        mint_standing(&long_past, now()).expect_err("an already-expired document is refused");
    assert!(
        error.contains("already in the past"),
        "the refusal says what is wrong: {error}"
    );
    assert!(!out_past.exists(), "and nothing was signed");

    // A blank UID is not a UID: it is what binds the document to ONE object.
    let mut blank = base(30);
    blank.standing.as_mut().expect("standing").schedule_uid = "  ".to_string();
    let error = mint_standing(&blank, now()).expect_err("a blank subject UID is refused");
    assert!(error.contains("--schedule-uid is required"), "{error}");

    // A per-run approval's inputs, which this document does not bind.
    let mut with_spec = base(30);
    with_spec.spec = Some(PathBuf::from("drill.yaml"));
    assert!(mint_standing(&with_spec, now())
        .expect_err("--spec is refused")
        .contains("--spec is not used with --standing"));
    let mut with_ticket = base(30);
    with_ticket.ticket = "CHG-1".to_string();
    assert!(mint_standing(&with_ticket, now())
        .expect_err("--ticket is refused")
        .contains("would NOT be signed"));

    // The per-run approval's FILENAME, which is how a standing document ends
    // up pasted into the slot that makes it look substituted.
    let mut wrong_name = base(30);
    wrong_name.out = dir.path().join("approval.json");
    assert!(mint_standing(&wrong_name, now())
        .expect_err("approval.json is refused")
        .contains("standing-authorization.json"));

    // **EVERY scope field, and the assertion is on the `Err` ARM.**
    //
    // An earlier revision used `unwrap_or_else(|e| e)`, which turns a SUCCESS
    // summary into `error` — and that summary prints `scope      <path>`, so a
    // `why` of `"maxPartitions"` was satisfied by the filename
    // `scope-maxPartitions.json` whether the check ran or not. Two planted
    // mutants deleting the zero-bound and the blank checks SURVIVED it. The
    // row now requires a refusal, and every `why` is a phrase from the message
    // that cannot appear in a path.
    for (edit, why) in [
        (r#""modes": ["newTopic"]"#, "and nothing else"),
        (r#""topics": []"#, "authorises the restore of nothing"),
        (r#""maxPartitions": 0"#, "is a BOUND"),
        (r#""recordsPerPartition": 0"#, "is a BOUND"),
        (r#""deadlineSeconds": 0"#, "is a BOUND"),
        (r#""targetClusterId": """#, "is blank"),
        (r#""topicPrefix": """#, "is blank"),
        (r#""templateDigest": """#, "is blank"),
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
        let out = dir.path().join(format!("sa-{field}.json"));
        args.out = out.clone();
        let error = mint_standing(&args, now()).expect_err(
            "a scope this build cannot act on must be refused BEFORE anything is signed",
        );
        assert!(
            error.contains(why),
            "{field}: the refusal names what it refused: {error}"
        );
        assert!(
            !out.exists() && !out.with_extension("sig").exists(),
            "{field}: a refused mint signs nothing and writes nothing"
        );
    }

    // And nothing was written for any of them.
    assert!(
        !dir.path().join("standing-authorization.json").exists(),
        "a refused mint writes no document"
    );
}

// ---------------------------------------------------------------------------
// FX-9: the mint judges the CALLER's `now` and reads no clock of its own
// ---------------------------------------------------------------------------

/// One temp dir, scope and key, and `ApproveArgs` over them with the window
/// each FX-9 row names.
struct Bench {
    dir: tempfile::TempDir,
    scope: PathBuf,
    key: PathBuf,
}

impl Bench {
    fn new() -> Self {
        let dir = tempfile::tempdir().expect("a temp dir");
        let scope = dir.path().join("scope.json");
        std::fs::write(&scope, scope_json()).expect("the scope is written");
        let key = dir.path().join("approver.pem");
        let signer = SigningKey::generate_ed25519();
        std::fs::write(&key, signer.to_pkcs8_pem().expect("a private PEM")).expect("written");
        Self { dir, scope, key }
    }

    /// `issued_at: None` is the operator's ordinary command line, with no
    /// `--issued-at`.
    fn args(
        &self,
        out: &str,
        issued_at: Option<chrono::DateTime<chrono::Utc>>,
        valid_days: i64,
    ) -> ApproveArgs {
        let mut args = args_for(self.dir.path(), &self.scope, &self.key, valid_days);
        args.out = self.dir.path().join(out);
        args.standing.as_mut().expect("standing").issued_at = issued_at;
        args
    }
}

fn read_doc(path: &Path) -> wire::StandingAuthorization {
    serde_json::from_slice(&std::fs::read(path).expect("the envelope was written"))
        .expect("the envelope parses")
}

/// **FX-9's negative control.** The committed fixture's own window (issued
/// 2026-09-01, thirty days), judged at the caller's `now`. It is refused AT
/// `expiresAt` and after it, and it mints one second before it. The refusal
/// names the caller's `now`, not a clock of its own, and writes nothing.
///
/// The boundary row is what `<=` is for: `expiresAt` is the first instant the
/// document authorises nothing (`admit_standing_authorization` refuses
/// `expires_at <= now`), so a mint that admitted `now == expiresAt` would
/// spend a key on bytes every reader refuses.
#[test]
fn the_fixture_window_is_refused_once_the_callers_now_reaches_its_expiry() {
    let bench = Bench::new();
    let expires = issued_at() + chrono::Duration::days(30);

    for (label, judged_at) in [
        ("at-expiry", expires),
        ("a-day-after", expires + chrono::Duration::days(1)),
    ] {
        let args = bench.args(&format!("sa-{label}.json"), Some(issued_at()), 30);
        let error = mint_standing(&args, judged_at)
            .expect_err("a window that has closed at the caller's now is refused");
        assert!(error.contains("already in the past"), "{label}: {error}");
        assert!(
            error.contains(&format!("it is now {}", judged_at.to_rfc3339())),
            "{label}: the refusal judged the CALLER's now, not a clock of its own: {error}"
        );
        assert!(
            !args.out.exists() && !args.out.with_extension("sig").exists(),
            "{label}: a refused mint signs nothing and writes nothing"
        );
    }

    // The positive control at the same boundary. One second earlier the same
    // document mints, so the refusals above are the expiry check firing at
    // `expiresAt`, and not something that refuses this window at any instant.
    let args = bench.args("sa-inside.json", Some(issued_at()), 30);
    mint_standing(&args, expires - chrono::Duration::seconds(1))
        .expect("one second before expiresAt the document still mints");
    assert_eq!(read_doc(&args.out).expires_at, expires);
}

/// **THE GUARD (FX-9): the mint reads no clock of its own.** Every instant
/// here is centuries or decades from any date this suite runs on, in both
/// directions. A build that reads the wall clock anywhere in `mint_standing`
/// therefore fails a row today and on every later date:
///
/// 1. `now` in 2001, no `--issued-at`: the document mints, stamped with
///    exactly that `now`. A wall-clock `issuedAt` is not 2001, and a
///    wall-clock expiry check sees a window that closed in 2001 and refuses.
///    This catches the expiry check on every date after 2001-01-31.
/// 2. `now` in 2999, no `--issued-at`: stamped with exactly that `now`.
/// 3. `now` in 2999, a window that closed a month earlier: refused, naming
///    that `now`. A wall-clock expiry check sees a window open until 2999
///    and signs it.
#[test]
fn the_mint_judges_and_stamps_the_callers_now_and_reads_no_clock() {
    let bench = Bench::new();

    for now in [at("2001-01-01T00:00:00Z"), at("2999-06-01T00:00:00Z")] {
        let args = bench.args(&format!("sa-{}.json", now.timestamp()), None, 30);
        mint_standing(&args, now).unwrap_or_else(|e| {
            panic!(
                "{now}: a window open at the caller's now mints, whatever the wall clock says: {e}"
            )
        });
        let doc = read_doc(&args.out);
        assert_eq!(
            doc.issued_at, now,
            "issuedAt defaults to the caller's now, never to the wall clock"
        );
        assert_eq!(doc.expires_at, now + chrono::Duration::days(30));
    }

    let now = at("2999-06-01T00:00:00Z");
    let args = bench.args("sa-closed-2999.json", Some(at("2999-04-01T00:00:00Z")), 30);
    let error = mint_standing(&args, now).expect_err(
        "a window that closed before the caller's now is refused, however far ahead of the wall \
         clock that now is",
    );
    assert!(error.contains("already in the past"), "{error}");
    assert!(
        error.contains(&format!("it is now {}", now.to_rfc3339())),
        "{error}"
    );
    assert!(
        !args.out.exists() && !args.out.with_extension("sig").exists(),
        "nothing was signed"
    );
}
