//! PROD-16.1 at the product API: on a FRESH install every namespace without a
//! binding is one-person confirmation in the console (`confirm`), so the first
//! restore needs no key; an UPGRADED install keeps `legacy-governed-v1` until
//! an administrator opts in; an explicit binding always wins.
//!
//! "Fresh" is the claim the identity hook writes, in the run that generated
//! the installation identity, on the public identity ConfigMap
//! (`logweir.dev/approval-default: confirm;policy=…;uid=…;signing=…;console=…`),
//! beside the default `TrustPolicy` the same run created. These rows seed both
//! objects in the fake cluster exactly as the hook leaves them, and every
//! stored confirmation is verified here with the same `verify_detached` the
//! controller and the runner use.
//!
//! THE SECURITY REVIEW'S ROWS: on an install that existed before PROD-16.1, a
//! marker patched into the ConfigMap — bare, or as a claim naming a policy
//! that is missing, replaced, not hook-made, or that trusts no console key —
//! changes nothing: legacy, nothing signed.

mod support;

use std::sync::Arc;

use logweir_api::approval::{ApprovalSettings, ConfirmationKey, InstallationIdentityRef};
use logweir_core::approval_policy::{
    ApprovalPolicySet, MarkerClaim, RestoreAuthorization, DEFAULT_CONFIRM_POLICY_NAME,
    PAYLOAD_TYPE_RESTORE_AUTHORIZATION,
};
use logweir_evidence::keys::{SigningKey, VerifyingKey};
use logweir_evidence::Sidecar;
use serde_json::{json, Value};
use support::{FakeKube, Options, TestApp, LOCAL_ADMIN_ACTOR, NS_A, NS_B};

const RELEASE_NS: &str = "logweir-system";
const IDENTITY_CM: &str = "logweir-signing-trust";
const POLICY: &str = "logweir-installation";
const POLICY_UID: &str = "00000000-0000-4000-8000-0000000016a1";

fn signing_id() -> String {
    "a".repeat(64)
}

/// The claim the hook writes for a console key id.
fn claim(console_key_id: &str) -> String {
    MarkerClaim {
        policy_name: POLICY.into(),
        policy_uid: POLICY_UID.into(),
        signing_key_id: signing_id(),
        console_key_id: console_key_id.into(),
    }
    .to_annotation()
}

/// The public identity ConfigMap as the hook leaves it; `marker` is the value
/// of `logweir.dev/approval-default`, or none.
fn identity_configmap(marker: Option<&str>) -> Value {
    let mut annotations = json!({"logweir.dev/identity-state": "established"});
    if let Some(marker) = marker {
        annotations["logweir.dev/approval-default"] = json!(marker);
    }
    json!({
        "metadata": {"name": IDENTITY_CM, "annotations": annotations},
        "data": {"key-id": signing_id(), "algorithm": "ecdsa-p256-sha256"}
    })
}

/// The installation TrustPolicy as the hook creates it, trusting
/// `console_key_id`.
fn hook_policy(console_key_id: &str) -> Value {
    json!({
        "metadata": {
            "name": POLICY,
            "uid": POLICY_UID,
            "annotations": {
                "logweir.dev/created-by": "identity-bootstrap",
                "logweir.dev/approval-default": "confirm"
            }
        },
        "spec": {
            "default": true,
            "keys": [
                {"keyId": signing_id(), "algorithm": "p256", "usages": ["EvidenceSigning"],
                 "state": "Active", "notBefore": "2026-10-07T09:55:00Z",
                 "notAfter": "9999-12-31T23:59:59Z",
                 "principal": {"id": format!("install:{RELEASE_NS}/logweir-signing-key")},
                 "spkiPem": "-----BEGIN PUBLIC KEY-----\nx\n-----END PUBLIC KEY-----\n"},
                {"keyId": console_key_id, "algorithm": "ed25519",
                 "usages": ["ConsoleConfirmation"], "state": "Active",
                 "notBefore": "2026-10-07T09:55:00Z", "notAfter": "9999-12-31T23:59:59Z",
                 "principal": {"id": format!("console:{RELEASE_NS}/logweir-console-confirmation")},
                 "spkiPem": "-----BEGIN PUBLIC KEY-----\ny\n-----END PUBLIC KEY-----\n"}
            ]
        }
    })
}

/// What the cluster holds of the installation's identity and trust.
enum Cluster {
    /// The fresh install, exactly as the hook leaves it.
    Fresh,
    /// The identity ConfigMap with this annotation (or none), and this
    /// policy (or none) — an upgraded install, or an attack on one.
    Seeded(Option<String>, Option<Value>),
    /// No identity ConfigMap at all.
    Nothing,
}

struct Install {
    app: TestApp,
    public: VerifyingKey,
}

/// A console over `document` (empty: no approval policy configured at all),
/// the managed key, and the marker location, over `cluster`.
fn install(document: &str, cluster: Cluster) -> Install {
    let key = SigningKey::generate_ed25519();
    let public = key.verifying_key();
    let console_id = key.key_id();
    let fake = FakeKube::new();
    let seeded = match cluster {
        Cluster::Fresh => Some((Some(claim(&console_id)), Some(hook_policy(&console_id)))),
        Cluster::Seeded(marker, policy) => Some((marker, policy)),
        Cluster::Nothing => None,
    };
    if let Some((marker, policy)) = seeded {
        fake.seed(
            "configmaps",
            RELEASE_NS,
            identity_configmap(marker.as_deref()),
        );
        if let Some(policy) = policy {
            fake.seed_cluster("trustpolicies", policy);
        }
    }
    let settings = ApprovalSettings {
        policies: ApprovalPolicySet::parse(document).expect("valid"),
        confirmation: Some(ConfirmationKey::from_key(key).expect("key")),
        installation: Some(InstallationIdentityRef {
            namespace: RELEASE_NS.into(),
            config_map: IDENTITY_CM.into(),
        }),
        ..ApprovalSettings::default()
    };
    Install {
        app: TestApp::with(
            fake,
            Options {
                approval: Arc::new(settings),
                ..Options::default()
            },
        ),
        public,
    }
}

async fn create(app: &TestApp, ns: &str, key: &str) -> support::TestResponse {
    app.post(
        &format!("/api/v1/namespaces/{ns}/restores"),
        Some(key),
        &support::restore_body(&support::golden_plan()).to_string(),
    )
    .await
}

fn approvals_posted(fake: &FakeKube) -> Vec<Value> {
    fake.requests()
        .into_iter()
        .filter(|r| r.method == "POST" && r.path.ends_with("/approvals"))
        .map(|r| serde_json::from_str(&r.body).expect("json"))
        .collect()
}

fn posted(fake: &FakeKube) -> usize {
    fake.requests()
        .iter()
        .filter(|r| r.method == "POST")
        .count()
}

fn verify(document: &str, sidecar: &str, key: &VerifyingKey) -> bool {
    let sidecar: Sidecar = serde_json::from_str(sidecar).expect("a sidecar");
    logweir_evidence::verify::verify_detached(
        key,
        PAYLOAD_TYPE_RESTORE_AUTHORIZATION,
        document.as_bytes(),
        &sidecar,
    )
    .is_ok()
}

async fn policy(app: &TestApp, ns: &str) -> Value {
    app.get(&format!("/api/v1/namespaces/{ns}/approval-policy"))
        .await
        .json()["item"]
        .clone()
}

/// **The fresh install's first restore needs no key.** No approval policy is
/// configured at all; the marker is there; the restore in an unbound namespace
/// is confirmed by the console in the same request, under `default-confirm-v1`,
/// and the stored document verifies against the console key.
#[tokio::test]
async fn a_fresh_install_confirms_an_unbound_namespace_in_one_request() {
    let install = install("", Cluster::Fresh);
    let view = policy(&install.app, NS_B).await;
    assert_eq!(view["name"], DEFAULT_CONFIRM_POLICY_NAME);
    assert_eq!(view["operatorMode"], "confirm");
    assert_eq!(view["basis"], "freshInstall");
    assert_eq!(view["legacy"], false);
    assert_eq!(view["ordinaryConfirmationAvailable"], true);

    let created = create(&install.app, NS_B, "fresh-restore-01").await;
    assert_eq!(
        created.status,
        201,
        "{}",
        String::from_utf8_lossy(&created.body)
    );
    let body = created.json();
    assert_eq!(body["authorization"]["state"], "confirmed");
    assert_eq!(body["authorization"]["operatorMode"], "confirm");
    assert_eq!(body["authorization"]["policy"], DEFAULT_CONFIRM_POLICY_NAME);
    assert_eq!(body["authorization"]["requester"], LOCAL_ADMIN_ACTOR);
    let stored = approvals_posted(&install.app.fake)
        .pop()
        .expect("the console stored its confirmation");
    let document = stored["spec"]["approvalBytes"].as_str().expect("bytes");
    let sidecar = stored["spec"]["sidecarBytes"].as_str().expect("sidecar");
    assert!(verify(document, sidecar, &install.public));
    let doc = RestoreAuthorization::from_bytes(document.as_bytes()).expect("v2");
    assert_eq!(doc.policy.name, DEFAULT_CONFIRM_POLICY_NAME);
    assert_eq!(
        doc.policy.digest,
        logweir_core::approval_policy::default_confirm_policy().digest()
    );
    assert_eq!(doc.subject.uid, body["item"]["uid"].as_str().expect("uid"));
    install.app.fake.assert_strict();
}

/// **An upgrade never weakens approval, and a patched marker changes
/// nothing** (the PROD-16.1 security review). An upgraded install: an identity
/// ConfigMap with no marker, or none at all. Then the ATTACKS on it, one
/// object edit each: a bare `confirm`; a claim naming a policy that does not
/// exist; a claim beside a policy of that name an administrator wrote (no
/// hook provenance) or re-created (another UID); a claim naming a policy that
/// trusts no console key. Every one reads `legacy-governed-v1` and signs
/// nothing. NEGATIVE CONTROL for "the marker honoured whenever present": a
/// console that honoured any of these fails here.
#[tokio::test]
async fn an_upgraded_install_keeps_legacy_and_a_patched_marker_changes_nothing() {
    let console_id = "b".repeat(64);
    let mut hand_made = hook_policy(&console_id);
    hand_made["metadata"]["annotations"] = json!({});
    let mut recreated = hook_policy(&console_id);
    recreated["metadata"]["uid"] = json!("00000000-0000-4000-8000-000000000bad");
    let mut no_console = hook_policy(&console_id);
    no_console["spec"]["keys"] = json!([no_console["spec"]["keys"][0].clone()]);
    for (what, cluster) in [
        ("upgraded, unmarked", Cluster::Seeded(None, None)),
        ("no identity object", Cluster::Nothing),
        (
            "bare confirm patched in",
            Cluster::Seeded(Some("confirm".into()), None),
        ),
        (
            "claim, no policy",
            Cluster::Seeded(Some(claim(&console_id)), None),
        ),
        (
            "claim, hand-made policy",
            Cluster::Seeded(Some(claim(&console_id)), Some(hand_made.clone())),
        ),
        (
            "claim, re-created policy",
            Cluster::Seeded(Some(claim(&console_id)), Some(recreated.clone())),
        ),
        (
            "claim, no console key",
            Cluster::Seeded(Some(claim(&console_id)), Some(no_console.clone())),
        ),
    ] {
        let install = install("", cluster);
        let view = policy(&install.app, NS_B).await;
        assert_eq!(view["name"], "legacy-governed-v1", "{what}");
        assert_eq!(view["operatorMode"], "strict", "{what}");
        assert_eq!(view["basis"], "legacy", "{what}");
        let created = create(&install.app, NS_B, "upgraded-restore-01").await;
        assert_eq!(created.status, 201, "{what}");
        assert_eq!(
            created.json()["authorization"]["state"],
            "awaitingApproval",
            "{what}"
        );
        assert_eq!(created.json()["authorization"]["legacy"], true, "{what}");
        assert!(
            approvals_posted(&install.app.fake).is_empty(),
            "{what}: nothing signed"
        );
    }
    // THE DOCUMENTED OPT-IN of an older install: an explicit
    // `defaultMode: confirm` with D0's floor (an approval-policy rollout),
    // beside a trust administrator adding the console key to the namespace's
    // TrustPolicy (which the controller checks; not modelled here).
    let opted = install(
        "allowOrdinaryConfirmation: true\ndefaultMode: confirm\n",
        Cluster::Seeded(None, None),
    );
    let view = policy(&opted.app, NS_B).await;
    assert_eq!(view["basis"], "configured");
    assert_eq!(view["operatorMode"], "confirm");
    let created = create(&opted.app, NS_B, "opted-restore-01").await;
    assert_eq!(created.json()["authorization"]["state"], "confirmed");
    // And an explicit `strict` beats a genuine fresh-install marker.
    let strict = install("defaultMode: strict\n", Cluster::Fresh);
    assert_eq!(
        policy(&strict.app, NS_B).await["name"],
        "legacy-governed-v1"
    );
}

/// **An explicit binding wins.** On a marked install, a namespace bound
/// Governed stays strict (awaits an independent approver, with its ticket),
/// while the unbound one beside it confirms.
#[tokio::test]
async fn an_explicit_binding_wins_over_the_fresh_install_default() {
    let install = install(
        &format!(
            "policies:\n  - name: prod-governed\n    mode: strict\nnamespaces:\n  {NS_A}: prod-governed\n"
        ),
        Cluster::Fresh,
    );
    let bound = policy(&install.app, NS_A).await;
    assert_eq!(bound["name"], "prod-governed");
    assert_eq!(bound["operatorMode"], "strict");
    assert_eq!(bound["basis"], "binding");
    assert_eq!(bound["ticketRequired"], true);
    let unbound = policy(&install.app, NS_B).await;
    assert_eq!(unbound["operatorMode"], "confirm");
    // Same installation digest for both: one document, one marker.
    assert_eq!(bound["installationDigest"], unbound["installationDigest"]);
}

/// **An unread marker creates nothing.** The identity object cannot be read
/// (a 500 here): the request is refused before the Restore exists, rather than
/// guessing — a guess of "legacy" would park a fresh install's Restore where
/// nothing could ever authorise it.
#[tokio::test]
async fn an_unreadable_marker_refuses_before_anything_is_created() {
    let install = install("", Cluster::Fresh);
    install.app.fake.inject(support::Fault {
        method: "GET",
        path_contains: format!("/namespaces/{RELEASE_NS}/configmaps/{IDENTITY_CM}"),
        status: 500,
        reason: "InternalError",
        delay: None,
        remaining: 1,
    });
    let refused = create(&install.app, NS_B, "unread-restore-01").await;
    assert!(refused.status.as_u16() >= 500, "{}", refused.status);
    assert_eq!(posted(&install.app.fake), 0, "no Restore, no Approval");
    // The next request reads it.
    let created = create(&install.app, NS_B, "unread-restore-01").await;
    assert_eq!(created.json()["authorization"]["state"], "confirmed");
}

/// **The managed key arrives after the console starts** (the identity hook is
/// a post-install hook). Until the file exists every confirm request is
/// refused before anything is created; once it is written the next request
/// confirms with it — no restart, no person handling the key.
#[tokio::test]
async fn the_managed_key_is_read_when_the_identity_hook_has_written_it() {
    let dir = std::env::temp_dir().join(format!(
        "logweir-prod161-key-{}-{}",
        std::process::id(),
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    ));
    std::fs::create_dir_all(&dir).expect("dir");
    let key_file = dir.join("confirmation.key");
    // THE FIX ROUND (review L5): only the MANAGED key file may be missing at
    // start. An operator-named file that is missing is a refusal to start,
    // naming it — a mistyped path is not "the hook has not run yet".
    let refused = ApprovalSettings::load(
        None,
        Some(&key_file),
        false,
        &[NS_A.to_string(), NS_B.to_string()],
    )
    .expect_err("an operator-named key file that does not exist refuses to start");
    assert!(refused.contains("confirmation.key"), "{refused}");
    let settings = ApprovalSettings::load(
        None,
        Some(&key_file),
        true,
        &[NS_A.to_string(), NS_B.to_string()],
    )
    .expect("the managed key file that does not exist yet is not a startup refusal");
    assert!(settings.confirmation.is_none());
    let settings = ApprovalSettings {
        installation: Some(InstallationIdentityRef {
            namespace: RELEASE_NS.into(),
            config_map: IDENTITY_CM.into(),
        }),
        ..settings
    };
    // The key the hook WILL write; the claim and the policy already name it.
    let key = SigningKey::generate_ed25519();
    let fake = FakeKube::new();
    fake.seed(
        "configmaps",
        RELEASE_NS,
        identity_configmap(Some(&claim(&key.key_id()))),
    );
    fake.seed_cluster("trustpolicies", hook_policy(&key.key_id()));
    let app = TestApp::with(
        fake,
        Options {
            approval: Arc::new(settings),
            ..Options::default()
        },
    );
    assert_eq!(
        policy(&app, NS_B).await["ordinaryConfirmationAvailable"],
        false
    );
    let refused = create(&app, NS_B, "pending-restore-01").await;
    assert_eq!(
        refused.status,
        409,
        "{}",
        String::from_utf8_lossy(&refused.body)
    );
    assert_eq!(refused.code(), "policy_mismatch");
    assert!(String::from_utf8_lossy(&refused.body).contains("not there yet"));
    assert_eq!(posted(&app.fake), 0, "nothing was created");

    // THE HOOK WRITES IT (the kubelet projects the Secret into the volume).
    std::fs::write(&key_file, key.to_pkcs8_pem().expect("pem")).expect("write");
    assert_eq!(policy(&app, NS_B).await["confirmationKeyId"], key.key_id());
    let created = create(&app, NS_B, "pending-restore-01").await;
    assert_eq!(
        created.status,
        201,
        "{}",
        String::from_utf8_lossy(&created.body)
    );
    assert_eq!(created.json()["authorization"]["state"], "confirmed");
    let stored = approvals_posted(&app.fake).pop().expect("confirmation");
    assert!(verify(
        stored["spec"]["approvalBytes"].as_str().expect("bytes"),
        stored["spec"]["sidecarBytes"].as_str().expect("sidecar"),
        &key.verifying_key()
    ));
    let _ = std::fs::remove_dir_all(&dir);

    // A file that EXISTS and is not a key is still a refusal to start.
    let bad = std::env::temp_dir().join(format!("logweir-prod161-bad-{}", std::process::id()));
    std::fs::write(&bad, "not a key").expect("write");
    for managed in [false, true] {
        assert!(ApprovalSettings::load(None, Some(&bad), managed, &[NS_A.to_string()]).is_err());
    }
    let _ = std::fs::remove_file(&bad);
}
