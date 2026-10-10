//! PLAT-19.2 at the runner: an authorization document v2 is re-verified from
//! the mounted bundle before any data-plane client exists — the console's
//! signature always, a DISTINCT governed approver's under `Governed`, and the
//! document's binding to this Restore, this plan and the frozen policy
//! snapshot.
//!
//! The pure rows call `phase1_approval::verify_authorization_v2_bytes`; the
//! real-binary rows run `logweir restore run` with a complete v2 execution
//! contract in the environment, so the flag names, the digest pins and the
//! verifier choice are proved together.

use chrono::{Duration, Utc};
use logweir::drill::phase1_approval::{self, ContractSubject};
use logweir::drill::DrillError;
use logweir_core::approval_policy::{
    ApprovalMode, ApprovalPolicySet, AuthorizedSubject, PolicyRef, Requester, RestoreAuthorization,
    PAYLOAD_TYPE_RESTORE_AUTHORIZATION, RESTORE_AUTHORIZATION_FORMAT_VERSION,
    RESTORE_AUTHORIZATION_KIND,
};
use logweir_core::execution_contract as wire;
use logweir_core::ids::sha256_prefixed;
use logweir_evidence::keys::SigningKey;
use logweir_evidence::sign::sign_detached;
use logweir_evidence::Sidecar;
use std::collections::BTreeMap;
use std::process::Command;

const PLAN: &str = r#"
name: p192
source:
  storage: {backend: filesystem, path: /tmp/logweir-p192-archive}
  backup: latestCompleted
  topics: [orders]
target:
  bootstrap_servers: ["127.0.0.1:19099"]
  mode: scratch
  topic_mapping_prefix: "drill-"
  marker_topic: logweir.scratch
sample:
  window_start: 2026-01-01T00:00:00Z
  window_end: 2026-01-02T00:00:00Z
  records_per_partition: 25
objectives: {rto_seconds: 1800, pass_rate: 1.0}
evidence: {backend: filesystem, path: /tmp/logweir-p192-evidence}
"#;

const NS: &str = "team-a";
const NAME: &str = "rst-1";
const UID: &str = "uid-1";

fn policies() -> ApprovalPolicySet {
    ApprovalPolicySet::parse(
        "allowOrdinaryConfirmation: true\npolicies:\n  - name: team-ordinary\n    mode: Ordinary\n  - name: prod-governed\n    mode: Governed\nnamespaces:\n  team-a: team-ordinary\n  prod: prod-governed\n",
    )
    .expect("valid")
}

fn policy(mode: ApprovalMode) -> logweir_core::approval_policy::ApprovalPolicy {
    policies()
        .resolve(match mode {
            ApprovalMode::Ordinary => "team-a",
            ApprovalMode::Governed => "prod",
        })
        .bound()
        .cloned()
        .expect("bound")
}

fn document(mode: ApprovalMode) -> RestoreAuthorization {
    let p = policy(mode);
    RestoreAuthorization {
        format_version: RESTORE_AUTHORIZATION_FORMAT_VERSION.into(),
        kind: RESTORE_AUTHORIZATION_KIND.into(),
        authorization_mode: mode,
        subject: AuthorizedSubject {
            api_version: "logweir.dev/v1alpha1".into(),
            kind: "Restore".into(),
            namespace: NS.into(),
            name: NAME.into(),
            uid: UID.into(),
        },
        plan_hash: sha256_prefixed(PLAN.as_bytes()),
        requester: Requester {
            issuer: "urn:logweir:local-admin".into(),
            subject: "admin".into(),
        },
        policy: PolicyRef {
            name: p.name.clone(),
            digest: p.digest(),
        },
        issued_at: Utc::now() - Duration::minutes(1),
        expires_at: Utc::now() + Duration::minutes(5),
        ticket: Some("CHG-1".into()),
        approval_subject: None,
        original_name_confirmation: None,
        approver: None,
        approved_at: None,
    }
}

struct Keys {
    console: SigningKey,
    approver: SigningKey,
    signing: SigningKey,
}

fn keys() -> Keys {
    Keys {
        console: SigningKey::generate_ed25519(),
        approver: SigningKey::generate_p256(),
        signing: SigningKey::generate_ed25519(),
    }
}

fn pem(key: &SigningKey) -> Vec<u8> {
    key.verifying_key()
        .to_public_key_pem()
        .expect("spki")
        .into_bytes()
}

/// The sidecar: the console's signature, and the approver's when `countersign`.
fn sidecar(doc: &[u8], k: &Keys, countersign: bool) -> Vec<u8> {
    let mut sidecar =
        sign_detached(&k.console, PAYLOAD_TYPE_RESTORE_AUTHORIZATION, doc).expect("console sig");
    if countersign {
        let second = sign_detached(&k.approver, PAYLOAD_TYPE_RESTORE_AUTHORIZATION, doc)
            .expect("approver sig");
        sidecar.signatures.extend(second.signatures);
    }
    serde_json::to_vec(&sidecar).expect("sidecar json")
}

fn subject() -> ContractSubject {
    ContractSubject {
        namespace: NS.into(),
        name: NAME.into(),
        uid: UID.into(),
    }
}

#[allow(clippy::too_many_arguments)]
fn verify(
    plan: &str,
    doc: &[u8],
    sidecar: &[u8],
    approver_pem: &[u8],
    console_pem: &[u8],
    snapshot: &[u8],
    subject: &ContractSubject,
    k: &Keys,
) -> Result<phase1_approval::Approved, DrillError> {
    phase1_approval::verify_authorization_v2_bytes(
        plan,
        doc,
        sidecar,
        approver_pem,
        console_pem,
        snapshot,
        subject,
        &k.signing.verifying_key(),
    )
}

fn is_guard(result: &Result<phase1_approval::Approved, DrillError>) -> bool {
    matches!(result, Err(DrillError::Guard(_)))
}

fn message(result: &Result<phase1_approval::Approved, DrillError>) -> String {
    match result {
        Err(e) => e.to_string(),
        Ok(_) => String::new(),
    }
}

#[test]
fn an_ordinary_bundle_verifies_on_the_consoles_signature_alone() {
    let k = keys();
    let doc = document(ApprovalMode::Ordinary).to_bytes();
    let snapshot = policy(ApprovalMode::Ordinary).snapshot_bytes();
    let approved = verify(
        PLAN,
        &doc,
        &sidecar(&doc, &k, false),
        &pem(&k.console),
        &pem(&k.console),
        &snapshot,
        &subject(),
        &k,
    )
    .expect("ordinary verifies");
    assert_eq!(approved.approval.approver, "urn:logweir:local-admin#admin");
    assert_eq!(approved.approval.key_id, k.console.key_id());
    assert_eq!(approved.approval.ticket, "CHG-1");
    assert!(!approved.approval.self_attested);
}

/// Under Ordinary the console key IS the authoriser: a bundle naming any other
/// approver key is not an ordinary run.
#[test]
fn an_ordinary_bundle_naming_another_authoriser_is_refused() {
    let k = keys();
    let doc = document(ApprovalMode::Ordinary).to_bytes();
    let result = verify(
        PLAN,
        &doc,
        &sidecar(&doc, &k, true),
        &pem(&k.approver),
        &pem(&k.console),
        &policy(ApprovalMode::Ordinary).snapshot_bytes(),
        &subject(),
        &k,
    );
    assert!(is_guard(&result), "{}", message(&result));
    assert!(
        message(&result).contains("Ordinary"),
        "{}",
        message(&result)
    );
}

#[test]
fn a_governed_bundle_needs_a_distinct_approvers_countersignature() {
    let k = keys();
    let doc = document(ApprovalMode::Governed).to_bytes();
    let snapshot = policy(ApprovalMode::Governed).snapshot_bytes();
    let approved = verify(
        PLAN,
        &doc,
        &sidecar(&doc, &k, true),
        &pem(&k.approver),
        &pem(&k.console),
        &snapshot,
        &subject(),
        &k,
    )
    .expect("governed with a countersignature verifies");
    assert_eq!(approved.approval.key_id, k.approver.key_id());

    // Console only: refused.
    let result = verify(
        PLAN,
        &doc,
        &sidecar(&doc, &k, false),
        &pem(&k.approver),
        &pem(&k.console),
        &snapshot,
        &subject(),
        &k,
    );
    assert!(is_guard(&result), "{}", message(&result));
    // The console posing as the approver: refused.
    let result = verify(
        PLAN,
        &doc,
        &sidecar(&doc, &k, false),
        &pem(&k.console),
        &pem(&k.console),
        &snapshot,
        &subject(),
        &k,
    );
    assert!(is_guard(&result), "{}", message(&result));
    assert!(
        message(&result).contains("Governed"),
        "{}",
        message(&result)
    );
}

/// The approver's signature never stands in for the console's.
#[test]
fn a_bundle_without_the_consoles_signature_is_refused() {
    let k = keys();
    let doc = document(ApprovalMode::Governed).to_bytes();
    let only_approver = serde_json::to_vec(
        &sign_detached(&k.approver, PAYLOAD_TYPE_RESTORE_AUTHORIZATION, &doc).expect("sig"),
    )
    .expect("json");
    let result = verify(
        PLAN,
        &doc,
        &only_approver,
        &pem(&k.approver),
        &pem(&k.console),
        &policy(ApprovalMode::Governed).snapshot_bytes(),
        &subject(),
        &k,
    );
    assert!(is_guard(&result), "{}", message(&result));
    assert!(
        message(&result).contains("console confirmation"),
        "{}",
        message(&result)
    );
}

/// The snapshot the controller froze must be the policy the document names:
/// the governed snapshot under an ordinary document (a downgrade by bundle
/// substitution), and a non-canonical rendering of the right policy, are both
/// refused.
#[test]
fn the_snapshot_must_be_the_policy_the_document_names() {
    let k = keys();
    let doc = document(ApprovalMode::Ordinary).to_bytes();
    let result = verify(
        PLAN,
        &doc,
        &sidecar(&doc, &k, false),
        &pem(&k.console),
        &pem(&k.console),
        &policy(ApprovalMode::Governed).snapshot_bytes(),
        &subject(),
        &k,
    );
    assert!(is_guard(&result), "{}", message(&result));
    // Refused BY THE POLICY CHECK, not incidentally by the Governed arm's
    // "approver key == console key" rule (review M1).
    assert!(
        message(&result).contains(POLICY_MISMATCH),
        "{}",
        message(&result)
    );
    let spaced = String::from_utf8(policy(ApprovalMode::Ordinary).snapshot_bytes())
        .expect("utf-8")
        .replace(',', ", ");
    let result = verify(
        PLAN,
        &doc,
        &sidecar(&doc, &k, false),
        &pem(&k.console),
        &pem(&k.console),
        spaced.as_bytes(),
        &subject(),
        &k,
    );
    assert!(is_guard(&result), "{}", message(&result));
}

/// The runner's own policy-digest check, and nothing else (review M1).
const POLICY_MISMATCH: &str = "a policy change requires a new confirmation";

/// An EDITED policy with the same name and the same mode -- `maxAgeSeconds`
/// 600 instead of 900 -- frozen into an otherwise valid ordinary bundle. Only
/// the digest differs, so the ONLY check that can refuse it is the runner's
/// comparison of the document's `policy.digest` with the mounted snapshot's;
/// the window (6 min) fits both policies. The control is the same bundle with
/// the unedited snapshot, which verifies.
#[test]
fn an_edited_snapshot_of_the_same_policy_is_refused_by_the_digest_alone() {
    let k = keys();
    let doc = document(ApprovalMode::Ordinary).to_bytes();
    let side = sidecar(&doc, &k, false);
    let unedited = policy(ApprovalMode::Ordinary);
    let edited = logweir_core::approval_policy::ApprovalPolicy {
        max_age_seconds: 600,
        ..unedited.clone()
    };
    assert_eq!(edited.name, unedited.name);
    assert_eq!(edited.mode, unedited.mode);
    assert_ne!(edited.digest(), unedited.digest());
    let run = |snapshot: &[u8]| {
        verify(
            PLAN,
            &doc,
            &side,
            &pem(&k.console),
            &pem(&k.console),
            snapshot,
            &subject(),
            &k,
        )
    };
    run(&unedited.snapshot_bytes()).expect("the control: the unedited snapshot verifies");
    let refused = run(&edited.snapshot_bytes());
    assert!(is_guard(&refused), "{}", message(&refused));
    assert!(
        message(&refused).contains(POLICY_MISMATCH),
        "{}",
        message(&refused)
    );

    // And the REAL runner, exit 3, before any client is constructed.
    let m = mount(&k, ApprovalMode::Ordinary, false);
    std::fs::write(&m.snapshot, edited.snapshot_bytes()).expect("edited snapshot");
    let (code, transcript) = invoke(&m, &contract_env(&m), true);
    assert_eq!(code, 3, "{transcript}");
    assert!(transcript.contains(POLICY_MISMATCH), "{transcript}");
    assert!(
        !transcript.contains("19099"),
        "no broker was dialled:\n{transcript}"
    );
}

#[test]
fn the_document_must_bind_this_restore_and_this_plan() {
    let k = keys();
    let doc = document(ApprovalMode::Ordinary).to_bytes();
    let side = sidecar(&doc, &k, false);
    let snapshot = policy(ApprovalMode::Ordinary).snapshot_bytes();
    let mut recreated = subject();
    recreated.uid = "uid-2".into();
    let result = verify(
        PLAN,
        &doc,
        &side,
        &pem(&k.console),
        &pem(&k.console),
        &snapshot,
        &recreated,
        &k,
    );
    assert!(is_guard(&result), "{}", message(&result));
    let changed = format!("{PLAN}# edited\n");
    let result = verify(
        &changed,
        &doc,
        &side,
        &pem(&k.console),
        &pem(&k.console),
        &snapshot,
        &subject(),
        &k,
    );
    assert!(is_guard(&result), "{}", message(&result));
    assert!(
        message(&result).contains("plan hash"),
        "{}",
        message(&result)
    );
}

/// The v1 verifier refuses a v2 document outright (its payload type), so a
/// bundle that lost its v2 members cannot fall back to "v1, and fine".
#[test]
fn the_v1_verifier_refuses_a_v2_document() {
    let k = keys();
    let doc = document(ApprovalMode::Ordinary).to_bytes();
    let result = phase1_approval::verify_bytes(
        PLAN,
        &doc,
        &sidecar(&doc, &k, false),
        &pem(&k.console),
        &k.signing.verifying_key(),
    );
    assert!(matches!(result, Err(DrillError::Guard(_))));
}

// ---------------------------------------------------------------------------
// The real binary
// ---------------------------------------------------------------------------

struct Mounted {
    _dir: tempfile::TempDir,
    plan: std::path::PathBuf,
    approval: std::path::PathBuf,
    approver_key: std::path::PathBuf,
    confirmation_key: std::path::PathBuf,
    snapshot: std::path::PathBuf,
    allowed: std::path::PathBuf,
    signing: std::path::PathBuf,
}

fn mount(k: &Keys, mode: ApprovalMode, countersign: bool) -> Mounted {
    mount_with(k, mode, countersign, PLAN, &document(mode).to_bytes())
}

/// [`mount`], over a given plan and a given signed document.
fn mount_with(k: &Keys, mode: ApprovalMode, countersign: bool, plan: &str, doc: &[u8]) -> Mounted {
    let dir = tempfile::tempdir().expect("tempdir");
    let doc = doc.to_vec();
    let m = Mounted {
        plan: dir.path().join("restore.yaml"),
        approval: dir.path().join("approval.json"),
        approver_key: dir.path().join("approver.pub.pem"),
        confirmation_key: dir.path().join("confirmation.pub.pem"),
        snapshot: dir.path().join("approval-policy.json"),
        allowed: dir.path().join("allowed-clusters.json"),
        signing: dir.path().join("signing.pem"),
        _dir: dir,
    };
    std::fs::write(&m.plan, plan).expect("plan");
    std::fs::write(&m.approval, &doc).expect("doc");
    std::fs::write(
        m.approval.with_extension("sig"),
        sidecar(&doc, k, countersign),
    )
    .expect("sig");
    let authoriser = match mode {
        ApprovalMode::Ordinary => pem(&k.console),
        ApprovalMode::Governed => pem(&k.approver),
    };
    std::fs::write(&m.approver_key, authoriser).expect("approver");
    std::fs::write(&m.confirmation_key, pem(&k.console)).expect("console");
    std::fs::write(&m.snapshot, policy(mode).snapshot_bytes()).expect("snapshot");
    std::fs::write(
        &m.allowed,
        r#"{"allowed_cluster_ids":["TARGET00000000000000000"]}"#,
    )
    .expect("allowed");
    std::fs::write(&m.signing, k.signing.to_pkcs8_pem().expect("pkcs8")).expect("signing");
    m
}

fn contract_env(m: &Mounted) -> BTreeMap<String, String> {
    let digest = |p: &std::path::Path| sha256_prefixed(&std::fs::read(p).expect("mounted"));
    [
        (wire::VERSION_ENV, wire::VERSION.to_string()),
        (wire::SUBJECT_API_VERSION_ENV, "logweir.dev/v1alpha1".into()),
        (wire::SUBJECT_KIND_ENV, "Restore".into()),
        (wire::SUBJECT_NAME_ENV, NAME.into()),
        (wire::SUBJECT_NAMESPACE_ENV, NS.into()),
        (wire::SUBJECT_UID_ENV, UID.into()),
        (wire::APPROVAL_NAME_ENV, "a1".into()),
        (wire::APPROVAL_UID_ENV, "approval-uid".into()),
        (wire::PLAN_SHA256_ENV, digest(&m.plan)),
        (wire::APPROVAL_SHA256_ENV, digest(&m.approval)),
        (
            wire::APPROVAL_SIDECAR_SHA256_ENV,
            digest(&m.approval.with_extension("sig")),
        ),
        (wire::APPROVER_KEY_SHA256_ENV, digest(&m.approver_key)),
        (wire::ALLOWED_CLUSTERS_SHA256_ENV, digest(&m.allowed)),
        (wire::POLICY_SNAPSHOT_SHA256_ENV, digest(&m.snapshot)),
        (
            wire::CONFIRMATION_KEY_SHA256_ENV,
            digest(&m.confirmation_key),
        ),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v))
    .collect()
}

fn invoke(m: &Mounted, env: &BTreeMap<String, String>, v2_flags: bool) -> (i32, String) {
    invoke_binary(
        std::path::Path::new(env!("CARGO_BIN_EXE_logweir")),
        m,
        env,
        v2_flags,
    )
}

/// [`invoke`], with the runner binary named: this build's, or an older one.
fn invoke_binary(
    binary: &std::path::Path,
    m: &Mounted,
    env: &BTreeMap<String, String>,
    v2_flags: bool,
) -> (i32, String) {
    let mut command = Command::new(binary);
    command
        .args(["restore", "run", "--spec"])
        .arg(&m.plan)
        .arg("--approval")
        .arg(&m.approval)
        .arg("--approver-key")
        .arg(&m.approver_key)
        .arg("--allowed-clusters")
        .arg(&m.allowed)
        .arg("--signing-key")
        .arg(&m.signing)
        .args(["--triggered-by", "approval/a1"])
        .args([wire::VERSION_ARG, wire::VERSION]);
    if v2_flags {
        command
            .arg("--policy-snapshot")
            .arg(&m.snapshot)
            .arg("--confirmation-key")
            .arg(&m.confirmation_key);
    }
    for name in wire::ALL_ENV_ANY {
        command.env_remove(name);
    }
    for (name, value) in env {
        command.env(name, value);
    }
    let output = command.output().expect("the runner runs");
    (
        output.status.code().unwrap_or(-1),
        format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ),
    )
}

const AUTHORIZATION_REFUSALS: [&str; 4] = [
    "console confirmation signature",
    "governed approver signature",
    "is Governed",
    "is Ordinary",
];

/// The control: a complete ordinary v2 bundle gets past every authorization
/// check and fails later, for a reason that is not the authorization — no
/// broker listens on the plan's address.
#[test]
fn an_ordinary_bundle_passes_the_real_runners_authorization_checks() {
    let k = keys();
    let m = mount(&k, ApprovalMode::Ordinary, false);
    let (code, transcript) = invoke(&m, &contract_env(&m), true);
    for refusal in AUTHORIZATION_REFUSALS {
        assert!(
            !transcript.contains(refusal),
            "{refusal} (exit {code}):\n{transcript}"
        );
    }
    assert!(
        !transcript.contains("payload_type mismatch"),
        "{transcript}"
    );
    assert!(
        !transcript.contains("no data operation was started"),
        "every startup guard passed; the run must fail LATER (exit {code}):\n{transcript}"
    );
    assert_ne!(code, 0, "no broker is running");
}

/// A governed bundle that carries only the console's confirmation is refused
/// by the real binary with exit 3, before any client is constructed.
#[test]
fn a_governed_bundle_without_the_approver_is_refused_by_the_real_runner() {
    let k = keys();
    let m = mount(&k, ApprovalMode::Governed, false);
    let (code, transcript) = invoke(&m, &contract_env(&m), true);
    assert_eq!(code, 3, "{transcript}");
    assert!(
        transcript.contains("governed approver signature"),
        "{transcript}"
    );
    assert!(
        !transcript.contains("19099"),
        "no broker was dialled:\n{transcript}"
    );
}

/// Dropping the two v2 flags from a v2 Job is refused (the contract pins
/// members nothing mounted), and so is half a v2 bundle.
#[test]
fn a_v2_contract_without_its_members_is_refused() {
    let k = keys();
    let m = mount(&k, ApprovalMode::Ordinary, false);
    let (code, transcript) = invoke(&m, &contract_env(&m), false);
    assert_eq!(code, 3, "{transcript}");
    assert!(
        transcript.contains("no such bundle member is mounted"),
        "{transcript}"
    );

    let mut env = contract_env(&m);
    env.remove(wire::CONFIRMATION_KEY_SHA256_ENV);
    let mut command_env = env.clone();
    command_env.remove(wire::CONFIRMATION_KEY_SHA256_ENV);
    let (code, transcript) = invoke(&m, &command_env, true);
    assert_eq!(code, 3, "{transcript}");
}

/// The sidecar the controller copies verbatim is a DSSE document the verifier
/// reads — asserted so a serialisation change on either side is a red row.
#[test]
fn the_multi_signature_sidecar_round_trips() {
    let k = keys();
    let doc = document(ApprovalMode::Governed).to_bytes();
    let parsed: Sidecar = serde_json::from_slice(&sidecar(&doc, &k, true)).expect("parses");
    assert_eq!(parsed.signatures.len(), 2);
    assert_eq!(parsed.payload_type, PAYLOAD_TYPE_RESTORE_AUTHORIZATION);
}

// ---------------------------------------------------------------------------
// `logweir drill countersign` — the governed approver's half
// ---------------------------------------------------------------------------

fn countersign(
    dir: &std::path::Path,
    doc: &[u8],
    confirmation: &[u8],
    key: &SigningKey,
) -> (i32, String) {
    let document = dir.join("approval.json");
    let conf = dir.join("confirmation.sig");
    let key_path = dir.join("approver.pem");
    std::fs::write(&document, doc).expect("doc");
    std::fs::write(&conf, confirmation).expect("conf");
    std::fs::write(&key_path, key.to_pkcs8_pem().expect("pkcs8")).expect("key");
    let output = Command::new(env!("CARGO_BIN_EXE_logweir"))
        .args(["drill", "countersign", "--document"])
        .arg(&document)
        .arg("--confirmation")
        .arg(&conf)
        .arg("--key")
        .arg(&key_path)
        .arg("--out")
        .arg(dir.join("approval.sig"))
        .output()
        .expect("the CLI runs");
    (
        output.status.code().unwrap_or(-1),
        format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ),
    )
}

/// The countersigned sidecar is exactly what the runner's governed verifier
/// admits, and the summary names the requester the approver is approving for.
#[test]
fn countersign_produces_the_sidecar_the_runner_admits() {
    let k = keys();
    let dir = tempfile::tempdir().expect("tempdir");
    let doc = document(ApprovalMode::Governed).to_bytes();
    let (code, out) = countersign(dir.path(), &doc, &sidecar(&doc, &k, false), &k.approver);
    assert_eq!(code, 0, "{out}");
    assert!(
        out.contains("urn:logweir:local-admin#admin"),
        "the requester is shown: {out}"
    );
    let merged = std::fs::read(dir.path().join("approval.sig")).expect("written");
    let parsed: Sidecar = serde_json::from_slice(&merged).expect("a sidecar");
    assert_eq!(parsed.signatures.len(), 2);
    let approved = verify(
        PLAN,
        &doc,
        &merged,
        &pem(&k.approver),
        &pem(&k.console),
        &policy(ApprovalMode::Governed).snapshot_bytes(),
        &subject(),
        &k,
    );
    assert!(approved.is_ok(), "{}", message(&approved));
}

#[test]
fn countersign_refuses_what_it_cannot_make_valid() {
    let k = keys();
    let dir = tempfile::tempdir().expect("tempdir");
    // An ordinary document needs no approver.
    let ordinary = document(ApprovalMode::Ordinary).to_bytes();
    let (code, out) = countersign(
        dir.path(),
        &ordinary,
        &sidecar(&ordinary, &k, false),
        &k.approver,
    );
    assert_eq!(code, 1, "{out}");
    assert!(out.contains("Ordinary"), "{out}");
    // The console key cannot countersign its own confirmation.
    let governed = document(ApprovalMode::Governed).to_bytes();
    let (code, out) = countersign(
        dir.path(),
        &governed,
        &sidecar(&governed, &k, false),
        &k.console,
    );
    assert_eq!(code, 1, "{out}");
    assert!(out.contains("already signed"), "{out}");
    // An expired request.
    let mut expired = document(ApprovalMode::Governed);
    expired.issued_at = Utc::now() - Duration::hours(2);
    expired.expires_at = Utc::now() - Duration::hours(1);
    let bytes = expired.to_bytes();
    let (code, out) = countersign(dir.path(), &bytes, &sidecar(&bytes, &k, false), &k.approver);
    assert_eq!(code, 1, "{out}");
    assert!(out.contains("expired"), "{out}");
    // No confirmation at all.
    let empty =
        format!(r#"{{"payloadType":"{PAYLOAD_TYPE_RESTORE_AUTHORIZATION}","signatures":[]}}"#);
    let (code, out) = countersign(dir.path(), &governed, empty.as_bytes(), &k.approver);
    assert_eq!(code, 1, "{out}");
    assert!(out.contains("no console confirmation"), "{out}");
}

/// **FX-9: `countersign` judges the CALLER's `now` and reads no clock.**
///
/// `run_countersign` hands it the wall clock, which the two binary rows above
/// pin: a request five minutes from expiry is countersigned, and one that
/// expired an hour ago is refused. These rows call the library with instants
/// far from any date this suite runs on, in both directions, so a build that
/// reads the wall clock fails on every date:
///
/// - a request open in 2001 is countersigned at a `now` in 2001, where a
///   wall-clock check sees it long expired;
/// - a request that expired in 2999 is refused at a `now` after that, where a
///   wall-clock check sees it open;
/// - at `now == expiresAt` it is refused, because `expiresAt` is the first
///   instant the request authorises nothing; one minute earlier it is
///   countersigned.
#[test]
fn countersign_judges_the_callers_now_and_reads_no_clock() {
    let k = keys();
    let dir = tempfile::tempdir().expect("tempdir");
    let at = |s: &str| {
        chrono::DateTime::parse_from_rfc3339(s)
            .expect("an instant")
            .with_timezone(&Utc)
    };
    let request = |issued: &str, expires: &str| {
        let mut doc = document(ApprovalMode::Governed);
        doc.issued_at = at(issued);
        doc.expires_at = at(expires);
        doc.to_bytes()
    };
    let countersign_at = |name: &str, bytes: &[u8], now| {
        let document = dir.path().join(format!("{name}.json"));
        let confirmation = dir.path().join(format!("{name}.confirmation.sig"));
        let key = dir.path().join(format!("{name}.approver.pem"));
        let out = dir.path().join(format!("{name}.sig"));
        std::fs::write(&document, bytes).expect("doc");
        std::fs::write(&confirmation, sidecar(bytes, &k, false)).expect("conf");
        std::fs::write(&key, k.approver.to_pkcs8_pem().expect("pkcs8")).expect("key");
        let result = logweir::approve::countersign(
            &logweir::approve::CountersignArgs {
                document,
                confirmation,
                key,
                out: out.clone(),
            },
            now,
        );
        (result, out)
    };

    // Open in 2001, judged in 2001.
    let open_2001 = request("2001-01-01T00:00:00Z", "2001-01-01T00:05:00Z");
    let (result, out) = countersign_at("open-2001", &open_2001, at("2001-01-01T00:01:00Z"));
    result.expect(
        "a request open at the caller's now is countersigned, whatever the wall clock says",
    );
    let merged: Sidecar =
        serde_json::from_slice(&std::fs::read(&out).expect("written")).expect("a sidecar");
    assert_eq!(
        merged.signatures.len(),
        2,
        "the console's and the approver's"
    );

    // Expired in 2999: refused at its expiry and after it, naming that now.
    let closed_2999 = request("2999-01-01T00:00:00Z", "2999-01-01T00:05:00Z");
    for (label, now) in [
        ("at-expiry", at("2999-01-01T00:05:00Z")),
        ("an-hour-after", at("2999-01-01T01:05:00Z")),
    ] {
        let (result, out) = countersign_at(label, &closed_2999, now);
        let error = result.expect_err("an expired request is refused at the caller's now");
        assert!(error.contains("expired"), "{label}: {error}");
        assert!(
            error.contains(&format!("it is now {}", now.to_rfc3339())),
            "{label}: the refusal judged the CALLER's now: {error}"
        );
        assert!(
            !out.exists(),
            "{label}: a refused countersign writes nothing"
        );
    }

    // The positive control at the same boundary.
    let (result, _) = countersign_at("inside-2999", &closed_2999, at("2999-01-01T00:04:00Z"));
    result.expect("one minute before expiresAt the request is countersigned");
}

/// PROD-16.1: a fresh install's one-click confirmation reaches the runner as
/// an ordinary bundle under `default-confirm-v1`, attested to the local
/// administrator, and verifies exactly as an explicit Ordinary binding's — no
/// approver key file anywhere. NEGATIVE CONTROL: the same document beside the
/// snapshot of another policy is refused before any client exists.
#[test]
fn a_fresh_install_default_confirm_bundle_verifies_at_the_runner() {
    let k = keys();
    let policy = logweir_core::approval_policy::default_confirm_policy();
    let mut doc = document(ApprovalMode::Ordinary);
    doc.policy = PolicyRef {
        name: policy.name.clone(),
        digest: policy.digest(),
    };
    doc.ticket = None;
    let bytes = doc.to_bytes();
    let approved = verify(
        PLAN,
        &bytes,
        &sidecar(&bytes, &k, false),
        &pem(&k.console),
        &pem(&k.console),
        &policy.snapshot_bytes(),
        &subject(),
        &k,
    )
    .expect("a default-confirm bundle verifies");
    assert_eq!(approved.approval.approver, "urn:logweir:local-admin#admin");
    let other = verify(
        PLAN,
        &bytes,
        &sidecar(&bytes, &k, false),
        &pem(&k.console),
        &pem(&k.console),
        &policy_of(ApprovalMode::Ordinary).snapshot_bytes(),
        &subject(),
        &k,
    );
    assert!(is_guard(&other), "{}", message(&other));
}

fn policy_of(mode: ApprovalMode) -> logweir_core::approval_policy::ApprovalPolicy {
    policy(mode)
}

// ---------------------------------------------------------------------------
// PROD-15.1: the approval subject and the typed names are document format 2.1.0
// ---------------------------------------------------------------------------

/// A plan restored under the ORIGINAL topic names, in the shape the runner
/// accepts: `newTopic`, the empty prefix, the block, complete verification.
const ORIGINAL_PLAN: &str = r#"
name: p151
source:
  storage: {backend: filesystem, path: /tmp/logweir-p151-archive}
  backup: latestCompleted
  topics: [orders]
target:
  bootstrap_servers: ["127.0.0.1:19099"]
  mode: newTopic
  topic_mapping_prefix: "drill-"
  topic_naming: {prefix: "", original_name: {owners: []}}
sample:
  window_start: 2026-01-01T00:00:00Z
  window_end: 2026-01-02T00:00:00Z
  records_per_partition: 25
  coverage: complete
objectives: {rto_seconds: 1800, pass_rate: 1.0}
evidence: {backend: filesystem, path: /tmp/logweir-p151-evidence}
"#;

/// A one-person confirmation of `plan`, carrying the `originalName` subject
/// and the typed topic names, declaring `format_version`.
fn subject_document(plan: &str, format_version: &str) -> RestoreAuthorization {
    let mut doc = document(ApprovalMode::Ordinary);
    doc.plan_hash = sha256_prefixed(plan.as_bytes());
    doc.format_version = format_version.into();
    doc.approval_subject = Some("originalName".into());
    doc.original_name_confirmation = Some(logweir_core::original_name::OriginalNameConfirmation {
        typed_topics: vec!["orders".into()],
    });
    doc
}

/// **The real runner reads format 2.1.0, and refuses the subject under
/// 2.0.0.** The same one-person confirmation of an original-name plan —
/// `approvalSubject`, the typed names, the console's signature over the exact
/// bytes — at 2.1.0 gets past every authorization check (and fails later: no
/// broker listens); declared as 2.0.0 it is refused, exit 3, naming the
/// version the fields are defined from, before any client is constructed.
/// KILLS: a runner that reads the subject out of a document older than the
/// subject; a runner that refuses 2.1.0.
#[test]
fn the_real_runner_reads_format_2_1_0_and_refuses_the_subject_under_2_0_0() {
    let k = keys();
    // 2.1.0: admitted.
    let doc = subject_document(ORIGINAL_PLAN, "2.1.0");
    assert_eq!(
        logweir_core::approval_policy::restore_authorization_format_version_for(
            doc.approval_subject.as_deref(),
            doc.original_name_confirmation.as_ref()
        ),
        "2.1.0",
        "the version the writer gives this document"
    );
    let m = mount_with(
        &k,
        ApprovalMode::Ordinary,
        false,
        ORIGINAL_PLAN,
        &doc.to_bytes(),
    );
    let (code, transcript) = invoke(&m, &contract_env(&m), true);
    for refusal in AUTHORIZATION_REFUSALS {
        assert!(
            !transcript.contains(refusal),
            "{refusal} (exit {code}):\n{transcript}"
        );
    }
    assert!(
        !transcript.contains("no data operation was started"),
        "every startup guard passed; the run must fail LATER (exit {code}):\n{transcript}"
    );
    assert!(
        !transcript.contains("defined from formatVersion"),
        "{transcript}"
    );
    assert_ne!(code, 0, "no broker is running");

    // 2.0.0: refused, by name, before anything is dialled.
    let old = subject_document(ORIGINAL_PLAN, "2.0.0");
    let m = mount_with(
        &k,
        ApprovalMode::Ordinary,
        false,
        ORIGINAL_PLAN,
        &old.to_bytes(),
    );
    let (code, transcript) = invoke(&m, &contract_env(&m), true);
    assert_eq!(code, 3, "{transcript}");
    assert!(
        transcript.contains("defined from formatVersion 2.1.0"),
        "{transcript}"
    );
    assert!(
        transcript.contains("no data operation was started"),
        "{transcript}"
    );
    assert!(
        !transcript.contains("19099"),
        "no broker was dialled:\n{transcript}"
    );
}

/// **A runner built BEFORE the subject refuses every 2.1.0 document.** Run
/// with `LOGWEIR_OLDER_RUNNER_BIN=<a logweir built from main>`; without it
/// the row measures nothing and says so (CI has no older binary).
///
/// - A 2.1.0 document — which always carries `approvalSubject` or the typed
///   names — over an ORDINARY plan the older runner would otherwise run: exit
///   3, `unknown field`, before any client exists. It never reads the
///   document as an ordinary authorization.
/// - The same over an original-name plan: refused too.
/// - CONTROL: the 2.0.0 document this build writes for an ordinary restore is
///   still admitted by the older runner (it fails later, on the broker).
#[test]
fn an_older_runner_refuses_every_2_1_0_document() {
    let Some(older) = std::env::var_os("LOGWEIR_OLDER_RUNNER_BIN") else {
        eprintln!(
            "an_older_runner_refuses_every_2_1_0_document: LOGWEIR_OLDER_RUNNER_BIN is not set, \
             so no older runner was measured"
        );
        return;
    };
    let older = std::path::PathBuf::from(older);
    assert!(older.is_file(), "{} is not a file", older.display());
    let k = keys();
    for (label, plan) in [
        ("an ordinary plan", PLAN),
        ("an original-name plan", ORIGINAL_PLAN),
    ] {
        let doc = subject_document(plan, "2.1.0");
        let m = mount_with(&k, ApprovalMode::Ordinary, false, plan, &doc.to_bytes());
        let (code, transcript) = invoke_binary(&older, &m, &contract_env(&m), true);
        println!("older runner, a 2.1.0 document over {label}: exit {code}\n{transcript}");
        assert_eq!(code, 3, "{label}: {transcript}");
        assert!(
            !transcript.contains("19099"),
            "{label}: no broker was dialled:\n{transcript}"
        );
        if plan == PLAN {
            assert!(
                transcript.contains("unknown field `approvalSubject`"),
                "{label}: {transcript}"
            );
            assert!(
                transcript.contains("no data operation was started"),
                "{label}: {transcript}"
            );
        }
    }
    // The typed names alone (a document this build never writes) are an
    // unknown field to the older reader too.
    let mut typed_only = subject_document(PLAN, "2.1.0");
    typed_only.approval_subject = None;
    let m = mount_with(
        &k,
        ApprovalMode::Ordinary,
        false,
        PLAN,
        &typed_only.to_bytes(),
    );
    let (code, transcript) = invoke_binary(&older, &m, &contract_env(&m), true);
    println!("older runner, typed names alone: exit {code}\n{transcript}");
    assert_eq!(code, 3, "{transcript}");
    assert!(
        transcript.contains("unknown field `originalNameConfirmation`"),
        "{transcript}"
    );

    // CONTROL: this build's 2.0.0 document for an ordinary restore.
    let ordinary = document(ApprovalMode::Ordinary);
    assert_eq!(ordinary.format_version, "2.0.0");
    let m = mount_with(
        &k,
        ApprovalMode::Ordinary,
        false,
        PLAN,
        &ordinary.to_bytes(),
    );
    let (code, transcript) = invoke_binary(&older, &m, &contract_env(&m), true);
    println!("older runner, a 2.0.0 ordinary document: exit {code}\n{transcript}");
    assert!(
        !transcript.contains("no data operation was started"),
        "the older runner admits a 2.0.0 document (exit {code}):\n{transcript}"
    );
    assert_ne!(code, 0, "no broker is running");
}

// ---------------------------------------------------------------------------
// PROD-16.2: two-person approval in the console (`approverSignature: Console`)
// ---------------------------------------------------------------------------

const IDP: &str = "https://idp.example";

/// The installation document of the PROD-16.2 rows: `team-a` is bound to a
/// two-person policy, and `prod` to the SAME policy with a personal key.
const PAIR_POLICIES: &str = "policies:\n  - name: prod-pair\n    mode: two-person\n    maxAgeSeconds: 3600\n  - name: prod-strict\n    mode: strict\n    maxAgeSeconds: 3600\nnamespaces:\n  team-a: prod-pair\n  prod: prod-strict\n";

fn pair_policy() -> logweir_core::approval_policy::ApprovalPolicy {
    ApprovalPolicySet::parse(PAIR_POLICIES)
        .expect("valid")
        .resolve("team-a")
        .bound()
        .cloned()
        .expect("bound")
}

fn strict_policy() -> logweir_core::approval_policy::ApprovalPolicy {
    ApprovalPolicySet::parse(PAIR_POLICIES)
        .expect("valid")
        .resolve("prod")
        .bound()
        .cloned()
        .expect("bound")
}

/// A two-person REQUEST over `plan`: what the console signs for `alice`.
fn pair_request(plan: &str) -> RestoreAuthorization {
    let p = pair_policy();
    let mut doc = document(ApprovalMode::Governed);
    doc.plan_hash = sha256_prefixed(plan.as_bytes());
    doc.requester = Requester {
        issuer: IDP.into(),
        subject: "alice".into(),
    };
    doc.policy = PolicyRef {
        name: p.name.clone(),
        digest: p.digest(),
    };
    doc
}

/// The APPROVAL of [`pair_request`]: `bob` clicked thirty seconds after the
/// request was made. 2.2.0, as the one writer gives it.
fn pair_approved(plan: &str) -> RestoreAuthorization {
    let mut doc = pair_request(plan);
    doc.approver = Some(logweir_core::approval_policy::Approver {
        issuer: IDP.into(),
        subject: "bob".into(),
    });
    doc.approved_at = Some(doc.issued_at + Duration::seconds(30));
    doc.format_version = logweir_core::approval_policy::restore_authorization_format_version(
        doc.approval_subject.as_deref(),
        doc.original_name_confirmation.as_ref(),
        true,
    )
    .into();
    doc
}

/// Verify a console-approved bundle as the controller mounts it: the console
/// key is BOTH the confirmation key and the approver key, and the sidecar
/// carries the console's signature alone.
fn verify_console(
    plan: &str,
    doc: &RestoreAuthorization,
    snapshot: &[u8],
    k: &Keys,
) -> Result<phase1_approval::Approved, DrillError> {
    let bytes = doc.to_bytes();
    verify(
        plan,
        &bytes,
        &sidecar(&bytes, k, false),
        &pem(&k.console),
        &pem(&k.console),
        snapshot,
        &subject(),
        k,
    )
}

/// **A console-approved bundle verifies at the runner on the console's
/// signature, and names the approver's principal** — the value the scorecard
/// signs as `approval.approver`.
///
/// NEGATIVE CONTROLS, each refused before any client exists: the REQUEST the
/// approver was shown (no approver), offered as the approval; and the same
/// approved document beside the snapshot of the same policy with a personal
/// key (another digest).
#[test]
fn a_console_approved_bundle_verifies_and_names_the_approvers_principal() {
    let k = keys();
    let snapshot = pair_policy().snapshot_bytes();
    let doc = pair_approved(PLAN);
    assert_eq!(doc.format_version, "2.2.0");
    let approved = verify_console(PLAN, &doc, &snapshot, &k).expect("a console approval verifies");
    assert_eq!(approved.approval.approver, "https://idp.example#bob");
    assert_eq!(approved.approval.key_id, k.console.key_id());
    assert_eq!(approved.approval.approved_at, doc.approved_at.expect("set"));
    assert_eq!(approved.approval.ticket, "CHG-1");
    assert!(!approved.approval.self_attested);
    assert_eq!(
        approved.approval_mode,
        phase1_approval::APPROVAL_MODE_CONSOLE
    );
    assert!(approved.original_name_confirmation.is_none());

    // The request is not an approval.
    let pending = verify_console(PLAN, &pair_request(PLAN), &snapshot, &k);
    assert!(is_guard(&pending), "{}", message(&pending));
    assert!(
        message(&pending).contains("nobody has approved"),
        "{}",
        message(&pending)
    );
    // A request made under one setting is not approved under the other.
    let mut personal = pair_policy();
    personal.approver_signature = logweir_core::approval_policy::ApproverSignature::PersonalKey;
    let other = verify_console(PLAN, &doc, &personal.snapshot_bytes(), &k);
    assert!(is_guard(&other), "{}", message(&other));
    assert!(
        message(&other).contains(POLICY_MISMATCH),
        "{}",
        message(&other)
    );
}

/// **THE RELAXATION IS THE SNAPSHOT'S, AND ONLY THE SNAPSHOT'S.** Under a
/// `Governed` policy the runner refuses a bundle that names the console key as
/// the approver — unless the snapshot says `approverSignature: Console`.
///
/// Three bundles that all name the console key as the approver:
/// 1. a personal-key policy, its own request, the console's signature alone:
///    refused, "a governed run needs a separate approver signature";
/// 2. the same policy, with `approver` and `approvedAt` written into the
///    document (and its digest named, so the digest cannot be what refuses
///    it): refused — a document cannot grant itself the relaxation;
/// 3. the two-person policy: admitted (the control).
///
/// And the other direction: under the two-person snapshot a bundle that names
/// a PERSONAL key as the approver, countersigned by it, is refused.
#[test]
fn the_console_key_is_the_approver_only_when_the_snapshot_says_console() {
    let k = keys();
    let strict = strict_policy();
    assert_eq!(
        strict.approver_signature,
        logweir_core::approval_policy::ApproverSignature::PersonalKey
    );
    // 1. A strict request, the console posing as the approver.
    let mut request = pair_request(PLAN);
    request.policy = PolicyRef {
        name: strict.name.clone(),
        digest: strict.digest(),
    };
    let refused = verify_console(PLAN, &request, &strict.snapshot_bytes(), &k);
    assert!(is_guard(&refused), "{}", message(&refused));
    assert!(
        message(&refused).contains("is Governed")
            && message(&refused).contains("separate approver"),
        "{}",
        message(&refused)
    );
    // 2. The same, with a console approver written into the document.
    let mut forged = pair_approved(PLAN);
    forged.policy = PolicyRef {
        name: strict.name.clone(),
        digest: strict.digest(),
    };
    let refused = verify_console(PLAN, &forged, &strict.snapshot_bytes(), &k);
    assert!(is_guard(&refused), "{}", message(&refused));
    assert!(
        message(&refused).contains("approverSignature is Console"),
        "{}",
        message(&refused)
    );
    // 3. THE CONTROL.
    assert!(verify_console(
        PLAN,
        &pair_approved(PLAN),
        &pair_policy().snapshot_bytes(),
        &k
    )
    .is_ok());

    // A two-person namespace offered a personal-key document: the request,
    // countersigned by a personal key the bundle names as the approver.
    let bytes = pair_request(PLAN).to_bytes();
    let personal = verify(
        PLAN,
        &bytes,
        &sidecar(&bytes, &k, true),
        &pem(&k.approver),
        &pem(&k.console),
        &pair_policy().snapshot_bytes(),
        &subject(),
        &k,
    );
    assert!(is_guard(&personal), "{}", message(&personal));
    // ... and an APPROVED document mounted beside a personal approver key.
    let bytes = pair_approved(PLAN).to_bytes();
    let wrong_key = verify(
        PLAN,
        &bytes,
        &sidecar(&bytes, &k, true),
        &pem(&k.approver),
        &pem(&k.console),
        &pair_policy().snapshot_bytes(),
        &subject(),
        &k,
    );
    assert!(is_guard(&wrong_key), "{}", message(&wrong_key));
    assert!(
        message(&wrong_key).contains("personal-key countersignature is not accepted"),
        "{}",
        message(&wrong_key)
    );
}

/// **The runner re-checks the second person itself**, from the bytes the
/// console signed: every way the approver might not be one is refused with
/// exit-3 routing before any client exists, and the control beside it runs.
#[test]
fn the_runner_refuses_an_approver_who_is_not_a_second_person() {
    let k = keys();
    let snapshot = pair_policy().snapshot_bytes();
    let who = |issuer: &str, subject: &str| logweir_core::approval_policy::Approver {
        issuer: issuer.into(),
        subject: subject.into(),
    };
    let asked = |issuer: &str, subject: &str| Requester {
        issuer: issuer.into(),
        subject: subject.into(),
    };
    type Case = (
        &'static str,
        Requester,
        logweir_core::approval_policy::Approver,
        &'static str,
    );
    let cases: Vec<Case> = vec![
        (
            "the requester",
            asked(IDP, "alice"),
            who(IDP, "alice"),
            "is the requester",
        ),
        (
            "another case",
            asked(IDP, "alice"),
            who(IDP, "ALICE"),
            "is the requester",
        ),
        (
            "a trailing slash on the issuer",
            asked(IDP, "alice"),
            who("https://idp.example/", "alice"),
            "is the requester",
        ),
        (
            "the same subject from another issuer",
            asked(IDP, "alice"),
            who("https://other.example", "alice"),
            "two issuers",
        ),
        (
            "trailing whitespace",
            asked(IDP, "alice"),
            who(IDP, "alice "),
            "not in a form that can be compared",
        ),
        (
            "a decomposed letter",
            asked(IDP, "jos\u{e9}"),
            who(IDP, "jose\u{301}"),
            "not in a form that can be compared",
        ),
        (
            "the local administrator as the approver",
            asked(IDP, "alice"),
            who("urn:logweir:local-admin", "admin"),
            "administrator console",
        ),
        (
            "the local administrator as the requester",
            asked("urn:logweir:local-admin", "admin"),
            who("urn:logweir:local-admin", "bob"),
            "administrator console",
        ),
        (
            "a service account as the requester",
            asked(IDP, "system:serviceaccount:team-a:deployer"),
            who(IDP, "bob"),
            "Kubernetes system identity",
        ),
    ];
    for (label, requester, approver, needle) in cases {
        let mut doc = pair_approved(PLAN);
        doc.requester = requester;
        doc.approver = Some(approver);
        let result = verify_console(PLAN, &doc, &snapshot, &k);
        assert!(is_guard(&result), "{label}: {}", message(&result));
        assert!(
            message(&result).contains(needle)
                && message(&result).contains("no data operation was started"),
            "{label}: {}",
            message(&result)
        );
    }
    // THE CONTROL: another subject of the same issuer, also when the issuer
    // is spelled with a trailing slash.
    let mut doc = pair_approved(PLAN);
    doc.approver = Some(who("https://idp.example/", "bob"));
    assert!(verify_console(PLAN, &doc, &snapshot, &k).is_ok());
}

/// **`approvedAt` lies inside the request's own window, and the runner reads
/// no clock.** Before the request, at its expiry and after it: refused. An
/// instant inside the window is admitted even when it is ahead of this
/// machine's clock — the controller, which has a clock, judged that.
#[test]
fn the_runner_holds_approved_at_to_the_requests_window_without_a_clock() {
    let k = keys();
    let snapshot = pair_policy().snapshot_bytes();
    let base = pair_approved(PLAN);
    let at = |instant| {
        let mut doc = base.clone();
        doc.approved_at = Some(instant);
        verify_console(PLAN, &doc, &snapshot, &k)
    };
    for (label, instant) in [
        ("before the request", base.issued_at - Duration::seconds(1)),
        ("at the expiry", base.expires_at),
        ("after the expiry", base.expires_at + Duration::seconds(1)),
    ] {
        let result = at(instant);
        assert!(is_guard(&result), "{label}: {}", message(&result));
        assert!(
            message(&result).contains("outside the request's own window"),
            "{label}: {}",
            message(&result)
        );
    }
    assert!(at(base.issued_at).is_ok(), "at issuedAt");
    // Two minutes ahead of now, still inside the window (it closes in five).
    assert!(at(Utc::now() + Duration::minutes(2)).is_ok());
    // One field without the other is not an approval.
    let mut half = base.clone();
    half.approved_at = None;
    let result = verify_console(PLAN, &half, &snapshot, &k);
    assert!(is_guard(&result), "{}", message(&result));
    assert!(
        message(&result).contains("without the other"),
        "{}",
        message(&result)
    );
}

/// **The fields are format 2.2.0 at the runner too.** The same approved
/// document declared as 2.1.0 or 2.0.0 is refused by name.
#[test]
fn the_runner_refuses_the_approver_fields_under_an_older_version() {
    let k = keys();
    let snapshot = pair_policy().snapshot_bytes();
    for version in ["2.0.0", "2.1.0"] {
        let mut doc = pair_approved(PLAN);
        doc.format_version = version.into();
        let result = verify_console(PLAN, &doc, &snapshot, &k);
        assert!(is_guard(&result), "{version}: {}", message(&result));
        assert!(
            message(&result).contains("defined from formatVersion 2.2.0"),
            "{version}: {}",
            message(&result)
        );
    }
    assert!(verify_console(PLAN, &pair_approved(PLAN), &snapshot, &k).is_ok());
}

/// **THE COMBINATION WITH PROD-15.1.** An original-name restore approved by a
/// second person: the bundle carries `approvalSubject: originalName` and the
/// approver, at 2.2.0; the runner reads the subject, reads no typed names
/// (they are a one-person confirmation's), and reports the approval mode a
/// scorecard signs as `consoleApproval`. Typed names beside it are refused.
#[test]
fn an_original_name_restore_approved_in_the_console_carries_the_subject_and_no_typed_names() {
    let k = keys();
    let snapshot = pair_policy().snapshot_bytes();
    let mut doc = pair_request(ORIGINAL_PLAN);
    doc.approval_subject = Some("originalName".into());
    doc.approver = Some(logweir_core::approval_policy::Approver {
        issuer: IDP.into(),
        subject: "bob".into(),
    });
    doc.approved_at = Some(doc.issued_at + Duration::seconds(30));
    doc.format_version = logweir_core::approval_policy::restore_authorization_format_version(
        doc.approval_subject.as_deref(),
        None,
        true,
    )
    .into();
    assert_eq!(doc.format_version, "2.2.0");
    let approved =
        verify_console(ORIGINAL_PLAN, &doc, &snapshot, &k).expect("the combination verifies");
    assert_eq!(
        approved.approval_subject,
        logweir_core::original_name::ApprovalSubject::OriginalName
    );
    assert_eq!(
        approved.approval_mode,
        phase1_approval::APPROVAL_MODE_CONSOLE
    );
    assert!(approved.original_name_confirmation.is_none());
    assert_eq!(approved.approval.approver, "https://idp.example#bob");
    // The subject's own version is not enough for the approver.
    let mut older = doc.clone();
    older.format_version = "2.1.0".into();
    let result = verify_console(ORIGINAL_PLAN, &older, &snapshot, &k);
    assert!(is_guard(&result), "{}", message(&result));
    // Typed names belong to a one-person confirmation, never to this.
    let mut typed = doc.clone();
    typed.original_name_confirmation =
        Some(logweir_core::original_name::OriginalNameConfirmation {
            typed_topics: vec!["orders".into()],
        });
    let result = verify_console(ORIGINAL_PLAN, &typed, &snapshot, &k);
    assert!(is_guard(&result), "{}", message(&result));
    assert!(
        message(&result)
            .contains(logweir_core::original_name::ORIGINAL_NAME_CONFIRMATION_NOT_ACCEPTED),
        "{}",
        message(&result)
    );
}

/// Mount a console-approved bundle as the controller mounts it: the console
/// key as both the approver key and the confirmation key.
fn mount_console(k: &Keys, plan: &str, doc: &RestoreAuthorization, snapshot: &[u8]) -> Mounted {
    let dir = tempfile::tempdir().expect("tempdir");
    let bytes = doc.to_bytes();
    let m = Mounted {
        plan: dir.path().join("restore.yaml"),
        approval: dir.path().join("approval.json"),
        approver_key: dir.path().join("approver.pub.pem"),
        confirmation_key: dir.path().join("confirmation.pub.pem"),
        snapshot: dir.path().join("approval-policy.json"),
        allowed: dir.path().join("allowed-clusters.json"),
        signing: dir.path().join("signing.pem"),
        _dir: dir,
    };
    std::fs::write(&m.plan, plan).expect("plan");
    std::fs::write(&m.approval, &bytes).expect("doc");
    std::fs::write(m.approval.with_extension("sig"), sidecar(&bytes, k, false)).expect("sig");
    std::fs::write(&m.approver_key, pem(&k.console)).expect("approver");
    std::fs::write(&m.confirmation_key, pem(&k.console)).expect("console");
    std::fs::write(&m.snapshot, snapshot).expect("snapshot");
    std::fs::write(
        &m.allowed,
        r#"{"allowed_cluster_ids":["TARGET00000000000000000"]}"#,
    )
    .expect("allowed");
    std::fs::write(&m.signing, k.signing.to_pkcs8_pem().expect("pkcs8")).expect("signing");
    m
}

/// **The real runner admits a console-approved bundle and refuses the
/// requester's own approval**, exit 3, before any client is constructed.
#[test]
fn the_real_runner_admits_a_console_approval_and_refuses_the_requesters_own() {
    let k = keys();
    let snapshot = pair_policy().snapshot_bytes();
    let m = mount_console(&k, PLAN, &pair_approved(PLAN), &snapshot);
    let (code, transcript) = invoke(&m, &contract_env(&m), true);
    for refusal in AUTHORIZATION_REFUSALS {
        assert!(
            !transcript.contains(refusal),
            "{refusal} (exit {code}):\n{transcript}"
        );
    }
    assert!(
        !transcript.contains("no data operation was started"),
        "every startup guard passed; the run must fail LATER (exit {code}):\n{transcript}"
    );
    assert_ne!(code, 0, "no broker is running");

    let mut own = pair_approved(PLAN);
    own.approver = Some(logweir_core::approval_policy::Approver {
        issuer: IDP.into(),
        subject: "alice".into(),
    });
    let m = mount_console(&k, PLAN, &own, &snapshot);
    let (code, transcript) = invoke(&m, &contract_env(&m), true);
    assert_eq!(code, 3, "{transcript}");
    assert!(transcript.contains("is the requester"), "{transcript}");
    assert!(
        transcript.contains("no data operation was started"),
        "{transcript}"
    );
    assert!(
        !transcript.contains("19099"),
        "no broker was dialled:\n{transcript}"
    );

    // The request alone, mounted as the approval: exit 3 too.
    let m = mount_console(&k, PLAN, &pair_request(PLAN), &snapshot);
    let (code, transcript) = invoke(&m, &contract_env(&m), true);
    assert_eq!(code, 3, "{transcript}");
    assert!(transcript.contains("nobody has approved"), "{transcript}");
}

/// **A runner built BEFORE two-person approval refuses a two-person bundle.**
/// Run with `LOGWEIR_OLDER_RUNNER_BIN=<a logweir built from an older
/// commit>`; without it the row measures nothing and says so (CI has no
/// older binary).
///
/// - The bundle as this build's controller mounts it (the console key as the
///   approver, the `Console` snapshot, the 2.2.0 document): exit 3, before
///   any client exists. The older runner never runs a two-person policy as a
///   personal-key one.
/// - CONTROL: this build's 2.0.0 one-person bundle is still admitted by it.
#[test]
fn an_older_runner_refuses_a_console_approved_bundle() {
    let Some(older) = std::env::var_os("LOGWEIR_OLDER_RUNNER_BIN") else {
        eprintln!(
            "an_older_runner_refuses_a_console_approved_bundle: LOGWEIR_OLDER_RUNNER_BIN is not \
             set, so no older runner was measured"
        );
        return;
    };
    let older = std::path::PathBuf::from(older);
    assert!(older.is_file(), "{} is not a file", older.display());
    let k = keys();
    let snapshot = pair_policy().snapshot_bytes();
    for (label, doc) in [
        ("the approved document", pair_approved(PLAN)),
        ("the request alone", pair_request(PLAN)),
    ] {
        let m = mount_console(&k, PLAN, &doc, &snapshot);
        let (code, transcript) = invoke_binary(&older, &m, &contract_env(&m), true);
        println!("older runner, a two-person bundle ({label}): exit {code}\n{transcript}");
        assert_eq!(code, 3, "{label}: {transcript}");
        assert!(
            transcript.contains("no data operation was started"),
            "{label}: {transcript}"
        );
        assert!(
            !transcript.contains("19099"),
            "{label}: no broker was dialled:\n{transcript}"
        );
        assert!(
            transcript.contains("unknown field `approverSignature`"),
            "{label}: the older runner stops at the snapshot: {transcript}"
        );
    }
    // The 2.2.0 document beside a snapshot the older runner CAN read (the
    // same policy with a personal key, which it would otherwise run): it
    // refuses the document itself.
    let mut personal = pair_policy();
    personal.approver_signature = logweir_core::approval_policy::ApproverSignature::PersonalKey;
    let mut doc = pair_approved(PLAN);
    doc.policy = PolicyRef {
        name: personal.name.clone(),
        digest: personal.digest(),
    };
    let m = mount_console(&k, PLAN, &doc, &personal.snapshot_bytes());
    let (code, transcript) = invoke_binary(&older, &m, &contract_env(&m), true);
    println!(
        "older runner, a 2.2.0 document beside a personal-key snapshot: exit {code}\n{transcript}"
    );
    assert_eq!(code, 3, "{transcript}");
    assert!(
        transcript.contains("unknown field `approver`"),
        "{transcript}"
    );

    // CONTROL: this build's 2.0.0 one-person bundle.
    let m = mount(&k, ApprovalMode::Ordinary, false);
    let (code, transcript) = invoke_binary(&older, &m, &contract_env(&m), true);
    println!("older runner, a 2.0.0 one-person bundle: exit {code}\n{transcript}");
    assert!(
        !transcript.contains("no data operation was started"),
        "the older runner admits a 2.0.0 document (exit {code}):\n{transcript}"
    );
    assert_ne!(code, 0, "no broker is running");
}

// ---------------------------------------------------------------------------
// PROD-16.2: the fixture the controller's rows read
// ---------------------------------------------------------------------------

/// Where the controller's console-approval fixture lives. `weirkeeper` cannot
/// sign (`scripts/check-one-signer.sh`), so its rows read documents and
/// signatures this crate produced; this crate's own row below reads the same
/// file, so both sides are held to it.
const CONSOLE_APPROVAL_FIXTURE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../weirkeeper/tests/fixtures/console-approval.json"
);

/// The fixture's fixed instants: the request, the click, its expiry, and the
/// controller's clock.
const FIXTURE_ISSUED_AT: &str = "2026-10-10T12:00:00Z";
const FIXTURE_APPROVED_AT: &str = "2026-10-10T12:04:00Z";
const FIXTURE_EXPIRES_AT: &str = "2026-10-10T13:00:00Z";
const FIXTURE_NOW: &str = "2026-10-10T12:05:00Z";
const FIXTURE_NS: &str = "logweir-t16";
const FIXTURE_RESTORE: &str = "r1";
const FIXTURE_UID: &str = "restore-uid-1";

fn fixture_instant(text: &str) -> chrono::DateTime<Utc> {
    chrono::DateTime::parse_from_rfc3339(text)
        .expect("a fixture instant")
        .with_timezone(&Utc)
}

/// **THE GENERATOR** of `crates/weirkeeper/tests/fixtures/console-approval.json`.
/// Run it with `LOGWEIR_WRITE_CONSOLE_APPROVAL_FIXTURE=1`; without the
/// variable it does nothing.
///
/// Every document is built by this build's own writer and signed by two
/// THROWAWAY keys that exist only in this process: a console key and a
/// personal approver key. Only their public halves, their key ids, the
/// documents and the signatures are written. Regenerating changes every key
/// id and signature, and nothing else.
#[test]
fn write_the_console_approval_fixture_for_the_controllers_rows() {
    if std::env::var_os("LOGWEIR_WRITE_CONSOLE_APPROVAL_FIXTURE").is_none() {
        return;
    }
    use logweir_core::approval_policy::Approver;
    let k = keys();
    let foreign = SigningKey::generate_ed25519();
    let pair = pair_policy();
    let strict = strict_policy();
    let base = |plan: &str| {
        let mut doc = pair_request(plan);
        doc.subject.namespace = FIXTURE_NS.into();
        doc.subject.name = FIXTURE_RESTORE.into();
        doc.subject.uid = FIXTURE_UID.into();
        doc.issued_at = fixture_instant(FIXTURE_ISSUED_AT);
        doc.expires_at = fixture_instant(FIXTURE_EXPIRES_AT);
        doc.ticket = Some("CHG-4711".into());
        doc
    };
    let approve = |mut doc: RestoreAuthorization, issuer: &str, subject: &str| {
        doc.approver = Some(Approver {
            issuer: issuer.into(),
            subject: subject.into(),
        });
        doc.approved_at = Some(fixture_instant(FIXTURE_APPROVED_AT));
        doc.format_version = logweir_core::approval_policy::restore_authorization_format_version(
            doc.approval_subject.as_deref(),
            doc.original_name_confirmation.as_ref(),
            true,
        )
        .into();
        doc
    };
    let approved = || approve(base(PLAN), IDP, "bob");
    let under_strict = |mut doc: RestoreAuthorization| {
        doc.policy = PolicyRef {
            name: strict.name.clone(),
            digest: strict.digest(),
        };
        doc
    };
    let asked_by = |mut doc: RestoreAuthorization, issuer: &str, subject: &str| {
        doc.requester = Requester {
            issuer: issuer.into(),
            subject: subject.into(),
        };
        doc
    };
    let at = |mut doc: RestoreAuthorization, instant: chrono::DateTime<Utc>| {
        doc.approved_at = Some(instant);
        doc
    };
    let versioned = |mut doc: RestoreAuthorization, version: &str| {
        doc.format_version = version.into();
        doc
    };
    let original = || {
        let mut doc = base(ORIGINAL_PLAN);
        doc.approval_subject = Some("originalName".into());
        doc.format_version =
            logweir_core::approval_policy::restore_authorization_format_version_for(
                doc.approval_subject.as_deref(),
                None,
            )
            .into();
        doc
    };
    // (name, document, countersigned by the personal approver key)
    let mut cases: Vec<(&str, RestoreAuthorization, bool)> = vec![
        ("request", base(PLAN), false),
        ("approved", approved(), false),
        (
            "approved-by-requester",
            approve(base(PLAN), IDP, "alice"),
            false,
        ),
        (
            "approved-by-requester-in-another-case",
            approve(base(PLAN), IDP, "ALICE"),
            false,
        ),
        (
            "approved-by-requester-behind-a-trailing-slash",
            approve(base(PLAN), "https://idp.example/", "alice"),
            false,
        ),
        (
            "approved-by-a-second-person-behind-a-trailing-slash",
            approve(base(PLAN), "https://idp.example/", "bob"),
            false,
        ),
        (
            "approved-by-the-same-subject-of-another-issuer",
            approve(base(PLAN), "https://other.example", "alice"),
            false,
        ),
        (
            "approved-by-a-subject-with-a-trailing-space",
            approve(base(PLAN), IDP, "alice "),
            false,
        ),
        (
            "approved-by-a-decomposed-spelling",
            approve(asked_by(base(PLAN), IDP, "jos\u{e9}"), IDP, "jose\u{301}"),
            false,
        ),
        (
            "approved-by-the-local-admin",
            approve(base(PLAN), "urn:logweir:local-admin", "admin"),
            false,
        ),
        (
            "requested-by-the-local-admin",
            approve(
                asked_by(base(PLAN), "urn:logweir:local-admin", "admin"),
                IDP,
                "bob",
            ),
            false,
        ),
        (
            "requested-by-a-service-account",
            approve(
                asked_by(
                    base(PLAN),
                    IDP,
                    "system:serviceaccount:logweir-t16:deployer",
                ),
                IDP,
                "bob",
            ),
            false,
        ),
        (
            "approved-before-the-request",
            at(
                approved(),
                fixture_instant(FIXTURE_ISSUED_AT) - Duration::seconds(1),
            ),
            false,
        ),
        (
            "approved-at-the-expiry",
            at(approved(), fixture_instant(FIXTURE_EXPIRES_AT)),
            false,
        ),
        (
            "approved-ahead-of-the-controllers-clock",
            at(
                approved(),
                fixture_instant(FIXTURE_NOW) + Duration::seconds(61),
            ),
            false,
        ),
        (
            "approver-under-2-1-0",
            versioned(approved(), "2.1.0"),
            false,
        ),
        (
            "approver-under-2-0-0",
            versioned(approved(), "2.0.0"),
            false,
        ),
        ("strict-request", under_strict(base(PLAN)), false),
        (
            "strict-request-countersigned",
            under_strict(base(PLAN)),
            true,
        ),
        (
            "strict-with-a-console-approver",
            under_strict(approved()),
            true,
        ),
        ("request-countersigned-by-a-personal-key", base(PLAN), true),
        ("original-name-request", original(), false),
        (
            "original-name-approved",
            approve(original(), IDP, "bob"),
            false,
        ),
    ];
    let mut half = approved();
    half.approved_at = None;
    cases.push(("approver-without-the-instant", half, false));
    let mut other_uid = approved();
    other_uid.subject.uid = "restore-uid-2".into();
    cases.push(("approved-for-another-uid", other_uid, false));
    let mut other_plan = approved();
    other_plan.plan_hash = sha256_prefixed(b"another plan");
    cases.push(("approved-for-another-plan", other_plan, false));
    let mut other_ns = approved();
    other_ns.subject.namespace = "elsewhere".into();
    cases.push(("approved-for-another-namespace", other_ns, false));
    let mut other_name = approved();
    other_name.subject.name = "r2".into();
    cases.push(("approved-for-another-restore", other_name, false));
    let mut typed = approve(original(), IDP, "bob");
    typed.original_name_confirmation =
        Some(logweir_core::original_name::OriginalNameConfirmation {
            typed_topics: vec!["orders".into()],
        });
    cases.push(("original-name-approved-with-typed-names", typed, false));

    let mut out = serde_json::Map::new();
    for (name, doc, countersign) in cases {
        let bytes = doc.to_bytes();
        out.insert(
            name.to_string(),
            serde_json::json!({
                "document": String::from_utf8(bytes.clone()).expect("utf-8"),
                "sidecar": String::from_utf8(sidecar(&bytes, &k, countersign)).expect("utf-8"),
            }),
        );
    }
    // The approved document, signed by a key nobody trusts instead of the
    // console's.
    let bytes = approved().to_bytes();
    let foreign_sidecar =
        sign_detached(&foreign, PAYLOAD_TYPE_RESTORE_AUTHORIZATION, &bytes).expect("sig");
    out.insert(
        "approved-signed-by-another-key".to_string(),
        serde_json::json!({
            "document": String::from_utf8(bytes).expect("utf-8"),
            "sidecar": serde_json::to_string(&foreign_sidecar).expect("json"),
        }),
    );
    let fixture = serde_json::json!({
        "comment": "PROD-16.2. Generated by crates/logweir/tests/authorization_v2.rs \
                    (write_the_console_approval_fixture_for_the_controllers_rows, \
                    LOGWEIR_WRITE_CONSOLE_APPROVAL_FIXTURE=1) with throwaway keys that existed \
                    only in that process. It holds public halves, key ids, documents and \
                    signatures: no private key.",
        "namespace": FIXTURE_NS,
        "restore": FIXTURE_RESTORE,
        "restoreUid": FIXTURE_UID,
        "planBytes": PLAN,
        "originalPlanBytes": ORIGINAL_PLAN,
        "policyDocument": PAIR_POLICIES.split("namespaces:").next().expect("policies"),
        "twoPersonPolicy": pair.name,
        "strictPolicy": strict.name,
        "issuedAt": FIXTURE_ISSUED_AT,
        "approvedAt": FIXTURE_APPROVED_AT,
        "expiresAt": FIXTURE_EXPIRES_AT,
        "now": FIXTURE_NOW,
        "consoleKeyId": k.console.key_id(),
        "consolePublicPem": String::from_utf8(pem(&k.console)).expect("pem"),
        "approverKeyId": k.approver.key_id(),
        "approverPublicPem": String::from_utf8(pem(&k.approver)).expect("pem"),
        "cases": out,
    });
    let mut text = serde_json::to_string_pretty(&fixture).expect("json");
    text.push('\n');
    std::fs::write(CONSOLE_APPROVAL_FIXTURE, text).expect("the fixture is written");
    eprintln!("wrote {CONSOLE_APPROVAL_FIXTURE}");
}

/// **The checked-in fixture is what THIS runner admits and refuses**, read
/// with the public halves it carries: the approved case verifies and names
/// the approver, and the request, the requester's own approval and the
/// version cases are refused. So the controller's rows and the runner's judge
/// one file.
#[test]
fn the_checked_in_console_approval_fixture_is_judged_the_same_by_the_runner() {
    let text = std::fs::read_to_string(CONSOLE_APPROVAL_FIXTURE).expect("the fixture exists");
    let fixture: serde_json::Value = serde_json::from_str(&text).expect("json");
    assert!(
        !text.contains("PRIVATE"),
        "the fixture carries public halves only"
    );
    let field = |name: &str| fixture[name].as_str().expect("a string field").to_string();
    let policies = ApprovalPolicySet::parse(&format!(
        "{}namespaces:\n  {}: {}\n",
        field("policyDocument"),
        field("namespace"),
        field("twoPersonPolicy")
    ))
    .expect("the fixture's policy document validates");
    let policy = policies
        .resolve(&field("namespace"))
        .bound()
        .cloned()
        .expect("bound");
    let subject = ContractSubject {
        namespace: field("namespace"),
        name: field("restore"),
        uid: field("restoreUid"),
    };
    let signing = SigningKey::generate_ed25519();
    let judge = |case: &str, plan: &str| {
        let entry = &fixture["cases"][case];
        phase1_approval::verify_authorization_v2_bytes(
            plan,
            entry["document"].as_str().expect("document").as_bytes(),
            entry["sidecar"].as_str().expect("sidecar").as_bytes(),
            field("consolePublicPem").as_bytes(),
            field("consolePublicPem").as_bytes(),
            &policy.snapshot_bytes(),
            &subject,
            &signing.verifying_key(),
        )
    };
    let plan = field("planBytes");
    let approved = judge("approved", &plan).expect("the approved case verifies");
    assert_eq!(approved.approval.approver, "https://idp.example#bob");
    assert_eq!(approved.approval.key_id, field("consoleKeyId"));
    assert_eq!(
        approved.approval_mode,
        phase1_approval::APPROVAL_MODE_CONSOLE
    );
    assert!(judge("approved-by-a-second-person-behind-a-trailing-slash", &plan).is_ok());
    // The runner reads no clock: an approval the controller's clock refuses
    // as too far ahead is inside the request's window, which is all it reads.
    assert!(judge("approved-ahead-of-the-controllers-clock", &plan).is_ok());
    let original = judge("original-name-approved", &field("originalPlanBytes"))
        .expect("the combination verifies");
    assert_eq!(
        original.approval_subject,
        logweir_core::original_name::ApprovalSubject::OriginalName
    );
    for (case, needle) in [
        ("request", "nobody has approved"),
        ("approved-by-requester", "is the requester"),
        ("approved-by-requester-in-another-case", "is the requester"),
        (
            "approved-by-requester-behind-a-trailing-slash",
            "is the requester",
        ),
        (
            "approved-by-the-same-subject-of-another-issuer",
            "two issuers",
        ),
        (
            "approved-by-a-subject-with-a-trailing-space",
            "not in a form that can be compared",
        ),
        (
            "approved-by-a-decomposed-spelling",
            "not in a form that can be compared",
        ),
        ("approved-by-the-local-admin", "administrator console"),
        ("requested-by-the-local-admin", "administrator console"),
        (
            "requested-by-a-service-account",
            "Kubernetes system identity",
        ),
        (
            "approved-before-the-request",
            "outside the request's own window",
        ),
        ("approved-at-the-expiry", "outside the request's own window"),
        ("approver-under-2-1-0", "defined from formatVersion 2.2.0"),
        ("approver-under-2-0-0", "defined from formatVersion 2.2.0"),
        ("approver-without-the-instant", "without the other"),
        ("approved-for-another-uid", "uid restore-uid-2"),
        ("approved-for-another-plan", "plan hash"),
        ("approved-for-another-namespace", "namespace elsewhere"),
        ("approved-for-another-restore", "name r2"),
        ("strict-with-a-console-approver", POLICY_MISMATCH),
        (
            "request-countersigned-by-a-personal-key",
            "nobody has approved",
        ),
        ("approved-signed-by-another-key", "console confirmation"),
    ] {
        let result = judge(case, &plan);
        assert!(is_guard(&result), "{case}: {}", message(&result));
        assert!(
            message(&result).contains(needle),
            "{case}: {}",
            message(&result)
        );
    }
}
