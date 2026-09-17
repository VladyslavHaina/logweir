//! **Execution contract v2 (decision D3 §8, Amendment I) and D0's bundle
//! contract v2, at the runner.**
//!
//! `crates/logweir/tests/approval_bundle_startup.rs` owns PLAT-01.2's rows and
//! they are unchanged; this file owns what v2 ADDS, and in particular the one
//! rule that decides whether the transition is a transition or a hole:
//!
//! > v1 stays accepted for already-created legacy governed Restores — and a v1
//! > invocation may therefore carry no v2 material at all.
//!
//! A runner holds no cluster credential, so it cannot read a `Restore`'s
//! creation timestamp and cannot ask anybody whether this object is "legacy".
//! What it CAN see is whether the invocation carries a point binding, a
//! standing authorization, a policy snapshot or a confirmation key — all of
//! which only a post-rollout Restore has. That is the rule, and
//! `a_v1_contract_carrying_*` is the mutant for each piece of it.
//!
//! | D3 / D0 clause | rows |
//! |---|---|
//! | `VERSION` is `"2"`, argv handshake unchanged | `the_controllers_stamp_and_the_runners_expectation_are_one_value` |
//! | v1 still accepted for a legacy Restore | `a_v1_contract_with_no_v2_material_is_still_accepted` |
//! | **v1 refused for a new Restore** | `a_v1_contract_carrying_v2_material_is_refused_by_name` |
//! | v2 blocks are all-or-nothing | `a_standing_authorization_without_its_scope_is_refused`, `a_scope_without_the_standing_kind_is_refused` |
//! | bundle v2 members are digest-pinned both ways | `every_optional_bundle_member_is_pinned_in_both_directions` |
//! | unpinned material is never acted on | `bundle_v2_material_without_a_contract_is_refused_by_the_real_binary` |
//! | the scope check is the runner's, before any client | `a_plan_outside_the_signed_scope_is_refused_by_the_real_binary` |

use chrono::Utc;
use logweir::drill::phase1_approval;
use logweir::drill::{
    execution_contract_for_invocation, execution_contract_from, validate_execution_contract,
    ApprovalBundleBytes, DrillError, ExecutionContract,
};
use logweir_core::execution_contract as wire;
use logweir_core::ids::sha256_prefixed;
use logweir_core::spec::ApprovalDoc;
use logweir_evidence::keys::SigningKey;
use logweir_evidence::sign::sign_detached;
use std::collections::BTreeMap;
use std::process::Command;

// ===========================================================================
// Fixtures — local to this file on purpose: it must be able to build a
// contract this repository's shared helpers do not yet produce.
// ===========================================================================

/// A restore plan that PARSES, targets a scratch cluster and maps through a
/// rehearsal prefix. `approval_bundle_startup.rs`'s fixture plan is deliberately
/// unparseable (its rows refuse before the parse); the scope check runs AFTER
/// the parse, so this file needs a real one.
const REHEARSAL_PLAN: &str = r#"
name: weekly-orders
source:
  storage: {backend: filesystem, path: /tmp/logweir-v2-archive}
  backup: latestCompleted
  topics: [orders]
target:
  bootstrap_servers: ["127.0.0.1:19099"]
  mode: scratch
  topic_mapping_prefix: "rehearsal-3f2a91c7-"
  marker_topic: logweir.scratch
sample:
  window_start: 2026-01-01T00:00:00Z
  window_end: 2026-01-02T00:00:00Z
  records_per_partition: 25
objectives: {rto_seconds: 1800, pass_rate: 1.0}
evidence: {backend: filesystem, path: /tmp/logweir-v2-evidence}
"#;

struct Fixture {
    bundle: ApprovalBundleBytes,
    approver: SigningKey,
    signing: SigningKey,
}

fn fixture_with_plan(plan: Vec<u8>) -> Fixture {
    let approver = SigningKey::generate_ed25519();
    let signing = SigningKey::generate_ed25519();
    let doc = ApprovalDoc {
        approver: "operator@example.com".to_string(),
        ticket: "CHG-D3-W5".to_string(),
        plan_hash: sha256_prefixed(&plan),
        approved_at: Utc::now(),
        subject_kind: "Restore".to_string(),
    };
    let approval = serde_json::to_vec(&doc).unwrap();
    let sidecar =
        sign_detached(&approver, phase1_approval::PAYLOAD_TYPE_APPROVAL, &approval).unwrap();
    Fixture {
        bundle: ApprovalBundleBytes {
            plan,
            approval,
            approval_sidecar: serde_json::to_vec(&sidecar).unwrap(),
            approver_key: approver
                .verifying_key()
                .to_public_key_pem()
                .unwrap()
                .into_bytes(),
            allowed_clusters: br#"{"allowed_cluster_ids":["TARGET00000000000000000"]}"#.to_vec(),
            ..Default::default()
        },
        approver,
        signing,
    }
}

fn fixture() -> Fixture {
    fixture_with_plan(b"name: contract-v2\n".to_vec())
}

fn scope_bytes(prefix: &str, cluster: &str) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "templateDigest": "sha256:aa",
        "targetClusterId": cluster,
        "topicPrefix": prefix,
        "topics": ["orders"],
        "maxPartitions": 200,
        "recordsPerPartition": 25,
        "deadlineSeconds": 3600,
        "modes": ["scratch"],
    }))
    .unwrap()
}

fn contract_for(bundle: &ApprovalBundleBytes, version: wire::ContractVersion) -> ExecutionContract {
    ExecutionContract {
        version,
        subject_api_version: "logweir.dev/v1alpha1".to_string(),
        subject_kind: "Restore".to_string(),
        subject_name: "rehearsal-weekly-orders-1".to_string(),
        subject_namespace: "team-a".to_string(),
        subject_uid: "restore-uid-a".to_string(),
        approval_name: "approval-a".to_string(),
        approval_uid: "approval-uid-a".to_string(),
        plan_sha256: sha256_prefixed(&bundle.plan),
        approval_sha256: sha256_prefixed(&bundle.approval),
        approval_sidecar_sha256: sha256_prefixed(&bundle.approval_sidecar),
        approver_key_sha256: sha256_prefixed(&bundle.approver_key),
        allowed_clusters_sha256: sha256_prefixed(&bundle.allowed_clusters),
        authorization_kind: wire::AuthorizationKind::Approval,
        scope_sha256: None,
        rehearsal_schedule_uid: None,
        policy_snapshot_sha256: None,
        confirmation_key_sha256: None,
    }
}

/// The thirteen mandatory variables, plus whichever v2 ones the contract sets.
fn env_map(contract: &ExecutionContract) -> BTreeMap<String, String> {
    let mut map: BTreeMap<String, String> = [
        (wire::VERSION_ENV, contract.version.as_str().to_string()),
        (
            wire::SUBJECT_API_VERSION_ENV,
            contract.subject_api_version.clone(),
        ),
        (wire::SUBJECT_KIND_ENV, contract.subject_kind.clone()),
        (wire::SUBJECT_NAME_ENV, contract.subject_name.clone()),
        (
            wire::SUBJECT_NAMESPACE_ENV,
            contract.subject_namespace.clone(),
        ),
        (wire::SUBJECT_UID_ENV, contract.subject_uid.clone()),
        (wire::APPROVAL_NAME_ENV, contract.approval_name.clone()),
        (wire::APPROVAL_UID_ENV, contract.approval_uid.clone()),
        (wire::PLAN_SHA256_ENV, contract.plan_sha256.clone()),
        (wire::APPROVAL_SHA256_ENV, contract.approval_sha256.clone()),
        (
            wire::APPROVAL_SIDECAR_SHA256_ENV,
            contract.approval_sidecar_sha256.clone(),
        ),
        (
            wire::APPROVER_KEY_SHA256_ENV,
            contract.approver_key_sha256.clone(),
        ),
        (
            wire::ALLOWED_CLUSTERS_SHA256_ENV,
            contract.allowed_clusters_sha256.clone(),
        ),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v))
    .collect();
    if contract.authorization_kind != wire::AuthorizationKind::Approval {
        map.insert(
            wire::AUTHORIZATION_KIND_ENV.to_string(),
            contract.authorization_kind.as_str().to_string(),
        );
    }
    for (name, value) in [
        (wire::SCOPE_SHA256_ENV, &contract.scope_sha256),
        (
            wire::REHEARSAL_SCHEDULE_UID_ENV,
            &contract.rehearsal_schedule_uid,
        ),
        (
            wire::POLICY_SNAPSHOT_SHA256_ENV,
            &contract.policy_snapshot_sha256,
        ),
        (
            wire::CONFIRMATION_KEY_SHA256_ENV,
            &contract.confirmation_key_sha256,
        ),
    ] {
        if let Some(value) = value {
            map.insert(name.to_string(), value.clone());
        }
    }
    map
}

// ===========================================================================
// The version itself
// ===========================================================================

#[test]
fn the_controllers_stamp_and_the_runners_expectation_are_one_value() {
    assert_eq!(wire::VERSION, wire::VERSION_V2);
    assert_eq!(
        wire::ContractVersion::parse(wire::VERSION),
        Some(wire::ContractVersion::V2)
    );
    // A version NEWER than this build is refused, not downgraded.
    assert_eq!(wire::ContractVersion::parse("3"), None);
}

/// The transition D0 and D3 §8 document: an already-created legacy governed
/// Restore still runs, and it runs with the v1 checks and no others.
#[test]
fn a_v1_contract_with_no_v2_material_is_still_accepted() {
    let fixture = fixture();
    let map = env_map(&contract_for(&fixture.bundle, wire::ContractVersion::V1));
    let parsed = execution_contract_for_invocation(Some(wire::VERSION_V1), |n| map.get(n).cloned())
        .expect("a legacy v1 invocation is accepted")
        .expect("and it carries a contract");
    assert_eq!(parsed.version, wire::ContractVersion::V1);
    assert_eq!(parsed.authorization_kind, wire::AuthorizationKind::Approval);
    assert_eq!(parsed.scope_sha256, None);
    assert_eq!(parsed.policy_snapshot_sha256, None);
    assert_eq!(parsed.confirmation_key_sha256, None);
}

/// **THE MUTANT: v1 accepted for a new Restore.**
///
/// Each piece of v2 material, one at a time, under a v1 version. Every one of
/// them must be refused, and the refusal must NAME the material — an operator
/// reading "unsupported version" would go and change the version, which is the
/// one repair that makes the situation worse.
#[test]
fn a_v1_contract_carrying_v2_material_is_refused_by_name() {
    let fixture = fixture();
    let base = contract_for(&fixture.bundle, wire::ContractVersion::V1);
    /// One piece of v2 material: what to call it, how to put it in the
    /// environment, and the words the refusal must use.
    type V2Material = (
        &'static str,
        fn(&mut BTreeMap<String, String>),
        &'static str,
    );
    let cases: [V2Material; 4] = [
        (
            "a standing rehearsal authorization",
            |m: &mut BTreeMap<String, String>| {
                m.insert(
                    wire::AUTHORIZATION_KIND_ENV.to_string(),
                    wire::AUTHORIZATION_KIND_STANDING.to_string(),
                );
            },
            "a standing rehearsal authorization",
        ),
        (
            "a scope digest",
            |m: &mut BTreeMap<String, String>| {
                m.insert(
                    wire::SCOPE_SHA256_ENV.to_string(),
                    format!("sha256:{}", "a".repeat(64)),
                );
            },
            "a signed rehearsal scope",
        ),
        (
            "a policy snapshot",
            |m: &mut BTreeMap<String, String>| {
                m.insert(
                    wire::POLICY_SNAPSHOT_SHA256_ENV.to_string(),
                    format!("sha256:{}", "b".repeat(64)),
                );
            },
            "an approval-policy snapshot",
        ),
        (
            "a confirmation key",
            |m: &mut BTreeMap<String, String>| {
                m.insert(
                    wire::CONFIRMATION_KEY_SHA256_ENV.to_string(),
                    format!("sha256:{}", "c".repeat(64)),
                );
            },
            "a confirmation-issuer public key",
        ),
    ];
    for (label, mutate, expected) in cases {
        let mut map = env_map(&base);
        mutate(&mut map);
        let error = execution_contract_from(|n| map.get(n).cloned())
            .unwrap_err_or_panic(&format!("v1 carrying {label} must be refused"));
        let rendered = error.to_string();
        assert!(
            matches!(error, DrillError::Guard(_)),
            "{label}: a new Restore under an old version is a REFUSAL, got {rendered}"
        );
        assert!(rendered.contains(expected), "{label}: {rendered}");
        assert!(
            rendered.contains("no data operation was started"),
            "{label}: {rendered}"
        );
    }
}

/// The same material under v2 is accepted — otherwise the row above would pass
/// for a build that simply refused everything.
#[test]
fn the_same_material_under_v2_is_accepted() {
    let fixture = fixture();
    let mut contract = contract_for(&fixture.bundle, wire::ContractVersion::V2);
    contract.authorization_kind = wire::AuthorizationKind::Standing;
    contract.scope_sha256 = Some(format!("sha256:{}", "a".repeat(64)));
    contract.rehearsal_schedule_uid = Some("schedule-uid-1".to_string());
    contract.policy_snapshot_sha256 = Some(format!("sha256:{}", "b".repeat(64)));
    contract.confirmation_key_sha256 = Some(format!("sha256:{}", "c".repeat(64)));
    let map = env_map(&contract);
    let parsed = execution_contract_from(|n| map.get(n).cloned())
        .expect("v2 carries v2 material")
        .expect("and there is a contract");
    assert_eq!(parsed, contract);
}

// ===========================================================================
// Each v2 block is all-or-nothing
// ===========================================================================

#[test]
fn a_standing_authorization_without_its_scope_is_refused() {
    let fixture = fixture();
    let mut contract = contract_for(&fixture.bundle, wire::ContractVersion::V2);
    contract.authorization_kind = wire::AuthorizationKind::Standing;
    let map = env_map(&contract);
    let error = execution_contract_from(|n| map.get(n).cloned())
        .unwrap_err_or_panic("a standing authorization with no pinned scope is refused");
    assert!(
        error.to_string().contains(wire::SCOPE_SHA256_ENV),
        "{error}"
    );
}

#[test]
fn a_standing_authorization_without_its_subject_uid_is_refused() {
    let fixture = fixture();
    let mut contract = contract_for(&fixture.bundle, wire::ContractVersion::V2);
    contract.authorization_kind = wire::AuthorizationKind::Standing;
    contract.scope_sha256 = Some(format!("sha256:{}", "a".repeat(64)));
    let map = env_map(&contract);
    let error = execution_contract_from(|n| map.get(n).cloned())
        .unwrap_err_or_panic("a standing authorization with no subject uid is refused");
    assert!(
        error.to_string().contains(wire::REHEARSAL_SCHEDULE_UID_ENV),
        "{error}"
    );
}

/// The other direction: a scope pinned under the ORDINARY authorization kind.
/// Accepting it would mean a scope was mounted and never checked, which is the
/// exact shape "the scope check was skipped" takes in production.
#[test]
fn a_scope_without_the_standing_kind_is_refused() {
    let fixture = fixture();
    let mut contract = contract_for(&fixture.bundle, wire::ContractVersion::V2);
    contract.scope_sha256 = Some(format!("sha256:{}", "a".repeat(64)));
    contract.rehearsal_schedule_uid = Some("schedule-uid-1".to_string());
    let map = env_map(&contract);
    let error = execution_contract_from(|n| map.get(n).cloned())
        .unwrap_err_or_panic("a scope under the ordinary kind is refused");
    assert!(
        error.to_string().contains("meaningful only under"),
        "{error}"
    );
}

#[test]
fn an_unknown_authorization_kind_is_refused_rather_than_defaulted() {
    let fixture = fixture();
    let mut map = env_map(&contract_for(&fixture.bundle, wire::ContractVersion::V2));
    map.insert(
        wire::AUTHORIZATION_KIND_ENV.to_string(),
        "Standing".to_string(),
    );
    let error = execution_contract_from(|n| map.get(n).cloned())
        .unwrap_err_or_panic("a mis-spelled kind is refused");
    assert!(error.to_string().contains("authorization kind"), "{error}");
}

/// A Job that set only v2 variables is an INCOMPLETE contract, not a
/// standalone invocation. Under the old `ALL_ENV` presence test it would have
/// looked like "no contract at all" and run with every contract check off.
#[test]
fn a_job_carrying_only_v2_variables_is_incomplete_and_not_standalone() {
    let error = execution_contract_from(|n| {
        (n == wire::POLICY_SNAPSHOT_SHA256_ENV).then(|| format!("sha256:{}", "b".repeat(64)))
    })
    .unwrap_err_or_panic("a v2-only environment is an incomplete contract");
    assert!(error.to_string().contains(wire::VERSION_ENV), "{error}");
}

// ===========================================================================
// Bundle contract v2's three optional members
// ===========================================================================

/// D0: "document, sidecar, both public keys, policy snapshot/digest, subject
/// UID". Each optional member is checked in BOTH directions — a pinned digest
/// with nothing mounted is a lost bundle member (PLAT-01.2's case), and a
/// mounted member with no pinned digest is material the controller never
/// committed to.
#[test]
fn every_optional_bundle_member_is_pinned_in_both_directions() {
    let fixture = fixture();
    let scope = scope_bytes("rehearsal-3f2a91c7-", "TARGET00000000000000000");
    /// One optional bundle member: its label in the refusal, how to mount it,
    /// and how to pin (or unpin) its digest on the contract.
    type OptionalMember = (
        &'static str,
        fn(&mut ApprovalBundleBytes, Vec<u8>),
        fn(&mut ExecutionContract, Option<String>),
    );
    let members: [OptionalMember; 3] = [
        (
            "rehearsal scope",
            |b, v| b.scope = Some(v),
            |c, d| {
                c.scope_sha256 = d;
                if c.scope_sha256.is_some() {
                    c.authorization_kind = wire::AuthorizationKind::Standing;
                    c.rehearsal_schedule_uid = Some("schedule-uid-1".to_string());
                }
            },
        ),
        (
            "approval-policy snapshot",
            |b, v| b.policy_snapshot = Some(v),
            |c, d| c.policy_snapshot_sha256 = d,
        ),
        (
            "confirmation-issuer public key",
            |b, v| b.confirmation_key = Some(v),
            |c, d| c.confirmation_key_sha256 = d,
        ),
    ];
    for (label, put_member, pin_digest) in members {
        // (a) Both present and agreeing: accepted.
        let mut bundle = fixture.bundle.clone();
        put_member(&mut bundle, scope.clone());
        let mut contract = contract_for(&fixture.bundle, wire::ContractVersion::V2);
        pin_digest(&mut contract, Some(sha256_prefixed(&scope)));
        validate_execution_contract(&contract, Some("approval/approval-a"), &bundle)
            .unwrap_or_else(|e| panic!("{label}: an agreeing member must be accepted: {e}"));

        // (b) Bytes that differ.
        let mut tampered = fixture.bundle.clone();
        let mut altered = scope.clone();
        altered.push(b'!');
        put_member(&mut tampered, altered);
        let error = validate_execution_contract(&contract, Some("approval/approval-a"), &tampered)
            .unwrap_err_or_panic(&format!("{label}: substituted bytes must be refused"));
        assert!(error.to_string().contains(label), "{label}: {error}");

        // (c) Pinned, nothing mounted.
        let error = validate_execution_contract(
            &contract,
            Some("approval/approval-a"),
            &fixture.bundle.clone(),
        )
        .unwrap_err_or_panic(&format!("{label}: a lost member must be refused"));
        assert!(
            error
                .to_string()
                .contains("no such bundle member is mounted"),
            "{label}: {error}"
        );

        // (d) Mounted, nothing pinned.
        let mut unpinned = contract_for(&fixture.bundle, wire::ContractVersion::V2);
        pin_digest(&mut unpinned, None);
        let error = validate_execution_contract(&unpinned, Some("approval/approval-a"), &bundle)
            .unwrap_err_or_panic(&format!("{label}: unpinned material must be refused"));
        assert!(
            error.to_string().contains("pins no digest for it"),
            "{label}: {error}"
        );
    }
}

/// The five mandatory members are unchanged by v2 — PLAT-01.2's regression,
/// restated here so a v2 change that loosened one fails in this file too.
#[test]
fn the_five_mandatory_members_are_still_hash_bound() {
    let fixture = fixture();
    let contract = contract_for(&fixture.bundle, wire::ContractVersion::V2);
    for member in 0..5 {
        let mut changed = fixture.bundle.clone();
        match member {
            0 => changed.plan.push(b'!'),
            1 => changed.approval.push(b'!'),
            2 => changed.approval_sidecar.push(b'!'),
            3 => changed.approver_key.push(b'!'),
            4 => changed.allowed_clusters.push(b'!'),
            _ => unreachable!(),
        }
        let error = validate_execution_contract(&contract, Some("approval/approval-a"), &changed)
            .unwrap_err_or_panic(&format!("member {member} must be hash-bound"));
        assert!(matches!(error, DrillError::Guard(_)), "{member}: {error}");
        assert!(!error.to_string().contains("PRIVATE KEY"));
    }
}

// ===========================================================================
// The real binary
// ===========================================================================

struct Mounted {
    _dir: tempfile::TempDir,
    plan: std::path::PathBuf,
    approval: std::path::PathBuf,
    approver_key: std::path::PathBuf,
    allowed: std::path::PathBuf,
    signing: std::path::PathBuf,
    scope: std::path::PathBuf,
}

fn mount(fixture: &Fixture, scope: &[u8]) -> Mounted {
    let dir = tempfile::tempdir().unwrap();
    let m = Mounted {
        plan: dir.path().join("restore.yaml"),
        approval: dir.path().join("approval.json"),
        approver_key: dir.path().join("approver.pub.pem"),
        allowed: dir.path().join("allowed-clusters.json"),
        signing: dir.path().join("signing.pem"),
        scope: dir.path().join("rehearsal-scope.json"),
        _dir: dir,
    };
    std::fs::write(&m.plan, &fixture.bundle.plan).unwrap();
    std::fs::write(&m.approval, &fixture.bundle.approval).unwrap();
    std::fs::write(
        m.approval.with_extension("sig"),
        &fixture.bundle.approval_sidecar,
    )
    .unwrap();
    std::fs::write(&m.approver_key, &fixture.bundle.approver_key).unwrap();
    std::fs::write(&m.allowed, &fixture.bundle.allowed_clusters).unwrap();
    std::fs::write(&m.signing, fixture.signing.to_pkcs8_pem().unwrap()).unwrap();
    std::fs::write(&m.scope, scope).unwrap();
    m
}

fn invoke(
    m: &Mounted,
    fixture: &Fixture,
    env: &BTreeMap<String, String>,
    extra: &[&str],
) -> (i32, String) {
    let mut command = Command::new(env!("CARGO_BIN_EXE_logweir"));
    command
        .args(["restore", "run", "--spec"])
        .arg(&m.plan)
        .arg("--approval")
        .arg(&m.approval)
        .arg("--approver-key")
        .arg(&m.approver_key)
        .arg("--approver-key-ids")
        .arg(fixture.approver.key_id())
        .arg("--allowed-clusters")
        .arg(&m.allowed)
        .arg("--signing-key")
        .arg(&m.signing)
        .args(["--triggered-by", "approval/approval-a"])
        .args(extra);
    // The runner must see ONLY the variables this case names: the parent test
    // process inherits whatever the developer's shell has, and a stray
    // `LOGWEIR_EXECUTION_*` would silently change which contract is under test.
    for name in wire::ALL_ENV_ANY {
        command.env_remove(name);
    }
    for (name, value) in env {
        command.env(name, value);
    }
    let output = command.output().unwrap();
    let transcript = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    (output.status.code().unwrap_or(-1), transcript)
}

/// Unpinned bundle-v2 material is never acted on. A standalone invocation has
/// no controller to pin anything, so a `--rehearsal-scope` handed to one is
/// refused rather than silently enforced — which would let an operator believe
/// a scope was checked when nothing bound it to this run.
#[test]
fn bundle_v2_material_without_a_contract_is_refused_by_the_real_binary() {
    let fixture = fixture();
    let m = mount(
        &fixture,
        &scope_bytes("rehearsal-3f2a91c7-", "TARGET00000000000000000"),
    );
    let (code, transcript) = invoke(
        &m,
        &fixture,
        &BTreeMap::new(),
        &["--rehearsal-scope", m.scope.to_str().unwrap()],
    );
    assert_eq!(code, 3, "{transcript}");
    assert!(
        transcript.contains("without a Restore execution contract"),
        "{transcript}"
    );
    assert!(
        !transcript.contains("BrokerTransportFailure"),
        "{transcript}"
    );
    assert!(!transcript.contains("Connection refused"), "{transcript}");
}

/// **The runner's half of D3 §4.3, end to end.** A rendered plan outside the
/// signed scope is refused with exit 3, the refusal names every mismatch, and
/// the bootstrap in the plan is never dialled.
#[test]
fn a_plan_outside_the_signed_scope_is_refused_by_the_real_binary() {
    let fixture = fixture_with_plan(REHEARSAL_PLAN.as_bytes().to_vec());
    let scope = scope_bytes("rehearsal-deadbeef-", "TARGET00000000000000000");
    let m = mount(&fixture, &scope);
    let mut contract = contract_for(&fixture.bundle, wire::ContractVersion::V2);
    contract.authorization_kind = wire::AuthorizationKind::Standing;
    contract.scope_sha256 = Some(sha256_prefixed(&scope));
    contract.rehearsal_schedule_uid = Some("schedule-uid-1".to_string());
    let (code, transcript) = invoke(
        &m,
        &fixture,
        &env_map(&contract),
        &[
            wire::VERSION_ARG,
            wire::VERSION,
            "--rehearsal-scope",
            m.scope.to_str().unwrap(),
        ],
    );
    assert_eq!(code, 3, "{transcript}");
    assert!(
        transcript.contains("RehearsalScopeViolation"),
        "{transcript}"
    );
    assert!(transcript.contains("rehearsal-deadbeef-"), "{transcript}");
    assert!(transcript.contains("schedule-uid-1"), "{transcript}");
    assert!(
        transcript.contains("refusal-reason="),
        "every guard refusal prints interface I9's line: {transcript}"
    );
    // The plan's bootstrap is 127.0.0.1:19099 and nothing listens there; a run
    // that had constructed the reader would say so on the transcript.
    assert!(
        !transcript.contains("BrokerTransportFailure"),
        "{transcript}"
    );
    assert!(!transcript.contains("Connection refused"), "{transcript}");
    assert!(!transcript.contains("19099"), "{transcript}");
}

/// The control: the SAME plan under a scope that covers it gets past the scope
/// check and fails later, for a reason that is not the scope. Without this row
/// the one above would pass for a build that refused every standing run.
#[test]
fn a_plan_inside_the_signed_scope_passes_the_scope_check() {
    let fixture = fixture_with_plan(REHEARSAL_PLAN.as_bytes().to_vec());
    let scope = scope_bytes("rehearsal-3f2a91c7-", "TARGET00000000000000000");
    let m = mount(&fixture, &scope);
    let mut contract = contract_for(&fixture.bundle, wire::ContractVersion::V2);
    contract.authorization_kind = wire::AuthorizationKind::Standing;
    contract.scope_sha256 = Some(sha256_prefixed(&scope));
    contract.rehearsal_schedule_uid = Some("schedule-uid-1".to_string());
    let (code, transcript) = invoke(
        &m,
        &fixture,
        &env_map(&contract),
        &[
            wire::VERSION_ARG,
            wire::VERSION,
            "--rehearsal-scope",
            m.scope.to_str().unwrap(),
        ],
    );
    assert!(
        !transcript.contains("RehearsalScopeViolation"),
        "the plan is inside the scope; the run must fail for some LATER reason, not this one \
         (exit {code}):\n{transcript}"
    );
    assert_ne!(code, 0, "no broker is running, so it cannot succeed");
}

// ===========================================================================
// A tiny helper so every row above reads the same way.
// ===========================================================================

trait UnwrapErrOrPanic<T, E> {
    fn unwrap_err_or_panic(self, why: &str) -> E;
}

impl<T, E> UnwrapErrOrPanic<T, E> for Result<T, E> {
    fn unwrap_err_or_panic(self, why: &str) -> E {
        match self {
            Ok(_) => panic!("{why}"),
            Err(e) => e,
        }
    }
}
