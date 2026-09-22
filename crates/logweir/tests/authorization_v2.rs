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
    let dir = tempfile::tempdir().expect("tempdir");
    let doc = document(mode).to_bytes();
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
    std::fs::write(&m.plan, PLAN).expect("plan");
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
    let mut command = Command::new(env!("CARGO_BIN_EXE_logweir"));
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
