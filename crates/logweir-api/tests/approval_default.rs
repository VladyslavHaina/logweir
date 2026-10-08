//! PROD-16.1 at the product API: on a FRESH install every namespace without a
//! binding is one-person confirmation in the console (`confirm`), so the first
//! restore needs no key; an UPGRADED install keeps `legacy-governed-v1` until
//! an administrator opts in; an explicit binding always wins.
//!
//! "Fresh" is the marker the identity hook writes, once, on the installation's
//! public identity ConfigMap (`logweir.dev/approval-default: confirm`). These
//! rows seed that object in the fake cluster exactly as the hook leaves it, and
//! every stored confirmation is verified here with the same `verify_detached`
//! the controller and the runner use.

mod support;

use std::sync::Arc;

use logweir_api::approval::{ApprovalSettings, ConfirmationKey, InstallationIdentityRef};
use logweir_core::approval_policy::{
    ApprovalPolicySet, RestoreAuthorization, DEFAULT_CONFIRM_POLICY_NAME,
    PAYLOAD_TYPE_RESTORE_AUTHORIZATION,
};
use logweir_evidence::keys::{SigningKey, VerifyingKey};
use logweir_evidence::Sidecar;
use serde_json::{json, Value};
use support::{FakeKube, Options, TestApp, LOCAL_ADMIN_ACTOR, NS_A, NS_B};

const RELEASE_NS: &str = "logweir-system";
const IDENTITY_CM: &str = "logweir-signing-trust";

/// The public identity ConfigMap as the hook leaves it; `marker` is the value
/// of `logweir.dev/approval-default`, or none.
fn identity_configmap(marker: Option<&str>) -> Value {
    let mut annotations = json!({"logweir.dev/identity-state": "established"});
    if let Some(marker) = marker {
        annotations["logweir.dev/approval-default"] = json!(marker);
    }
    json!({
        "metadata": {"name": IDENTITY_CM, "annotations": annotations},
        "data": {"key-id": "0".repeat(64), "algorithm": "ecdsa-p256-sha256"}
    })
}

struct Install {
    app: TestApp,
    public: VerifyingKey,
}

/// A console over `document` (empty: no approval policy configured at all),
/// the managed key, and the marker location; `marker` seeds the identity
/// ConfigMap (`None` seeds none).
fn install(document: &str, marker: Option<Option<&str>>) -> Install {
    let key = SigningKey::generate_ed25519();
    let public = key.verifying_key();
    let fake = FakeKube::new();
    if let Some(marker) = marker {
        fake.seed("configmaps", RELEASE_NS, identity_configmap(marker));
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
    let install = install("", Some(Some("confirm")));
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

/// **An upgrade never weakens approval.** The same console over an install
/// whose identity carries NO marker — the upgraded install — and over one
/// with no identity object at all: the unbound namespace keeps
/// `legacy-governed-v1`, nothing is signed, the Restore awaits today's v1
/// approval. NEGATIVE CONTROL for "the upgrade marker ignored": a console
/// that treated every install as fresh fails both halves.
#[tokio::test]
async fn an_upgraded_install_keeps_legacy_until_an_administrator_opts_in() {
    for marker in [
        Some(None),
        None,
        Some(Some("Confirm")),
        Some(Some("strict")),
    ] {
        let install = install("", marker);
        let view = policy(&install.app, NS_B).await;
        assert_eq!(view["name"], "legacy-governed-v1", "{marker:?}");
        assert_eq!(view["operatorMode"], "strict");
        assert_eq!(view["basis"], "legacy");
        let created = create(&install.app, NS_B, "upgraded-restore-01").await;
        assert_eq!(created.status, 201);
        assert_eq!(created.json()["authorization"]["state"], "awaitingApproval");
        assert_eq!(created.json()["authorization"]["legacy"], true);
        assert!(
            approvals_posted(&install.app.fake).is_empty(),
            "nothing signed"
        );
    }
    // THE OPT-IN: an explicit `defaultMode: confirm`, no marker.
    let opted = install("defaultMode: confirm\n", Some(None));
    let view = policy(&opted.app, NS_B).await;
    assert_eq!(view["basis"], "configured");
    assert_eq!(view["operatorMode"], "confirm");
    let created = create(&opted.app, NS_B, "opted-restore-01").await;
    assert_eq!(created.json()["authorization"]["state"], "confirmed");
    // And an explicit `strict` beats a marker.
    let strict = install("defaultMode: strict\n", Some(Some("confirm")));
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
        Some(Some("confirm")),
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
    let install = install("", Some(Some("confirm")));
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
    let settings =
        ApprovalSettings::load(None, Some(&key_file), &[NS_A.to_string(), NS_B.to_string()])
            .expect("a configured key file that does not exist yet is not a startup refusal");
    assert!(settings.confirmation.is_none());
    let settings = ApprovalSettings {
        installation: Some(InstallationIdentityRef {
            namespace: RELEASE_NS.into(),
            config_map: IDENTITY_CM.into(),
        }),
        ..settings
    };
    let fake = FakeKube::new();
    fake.seed(
        "configmaps",
        RELEASE_NS,
        identity_configmap(Some("confirm")),
    );
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
    let key = SigningKey::generate_ed25519();
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
    assert!(ApprovalSettings::load(None, Some(&bad), &[NS_A.to_string()]).is_err());
    let _ = std::fs::remove_file(&bad);
}
