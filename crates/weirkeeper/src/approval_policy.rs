//! The installation's approval-policy document, as this controller reads it —
//! PLAT-19.2.
//!
//! The document itself, its validation and every check made over it are
//! `logweir_core::approval_policy`'s. This module is only the READ: one
//! environment variable naming one mounted file, read once at startup, the
//! same arrangement `main` uses for the archive URL and the runner image — the
//! read is in `main`, the decision is a pure function here that a test can
//! hand an absent variable, an empty one, an unreadable file or a bad
//! document without touching process state.
//!
//! # Absent is legacy, and bad is a refusal to start
//!
//! No variable (or an empty one) is an installation that configured no
//! policy: every namespace resolves to `legacy-governed-v1`, which is exactly
//! the pre-PLAT-19.2 behaviour — D0's "existing installations retain their
//! approval requirement until explicitly changed".
//!
//! A variable that names a file this process cannot read, or a document that
//! does not validate, is NOT that: the operator asked for a policy and would
//! get none. The controller therefore refuses to start, naming the file and
//! the field, rather than silently running every namespace as legacy governed
//! — which would be fail-closed for an Ordinary binding but would also quietly
//! accept v1 approvals in a namespace whose operator bound it Governed
//! precisely to require separation of duties.
//!
//! # Why a restart and not a watch
//!
//! D0: "Selecting a different policy is an explicit installation-admin rollout
//! and audit event, not a namespace operator edit." The chart renders the
//! document into an immutable, content-addressed ConfigMap whose name the
//! Deployment references, so a changed policy is a new ReplicaSet — a rollout,
//! visible in `kubectl rollout history` — and a running controller is always
//! running the document its pod template names.

use std::path::Path;
use std::sync::{Arc, RwLock};

use k8s_openapi::api::core::v1::ConfigMap;
use kube::{Api, ResourceExt as _};
use logweir_core::approval_policy::{
    verify_marker, ApprovalPolicySet, InstallationMarker, MarkerClaim, MarkerRefusal,
    TrustKeyFacts, TrustPolicyFacts, APPROVAL_DEFAULT_ANNOTATION, CREATED_BY_ANNOTATION,
};

use crate::crds::trust_policy::{KeyUsage, TrustPolicy};

/// The environment variable naming the mounted approval-policy document.
pub const APPROVAL_POLICY_FILE_ENV: &str = "LOGWEIR_APPROVAL_POLICY_FILE";

/// PROD-16.1: the environment variable naming the installation's PUBLIC
/// identity ConfigMap (`identity.publicConfigMapName`) in the release
/// namespace ([`crate::check::policy::INSTALLATION_NAMESPACE_ENV`]), on which
/// the identity hook writes the fresh-install marker. Absent: this controller
/// never reads a marker, and every unbound namespace without a `defaultMode`
/// stays `legacy-governed-v1`.
pub const IDENTITY_PUBLIC_CONFIGMAP_ENV: &str = "LOGWEIR_IDENTITY_PUBLIC_CONFIGMAP";

/// How often the marker is read again. It is written once, at install, by a
/// hook Helm runs AFTER this pod starts; this is how long a fresh install's
/// first minute can read `legacy-governed-v1` — the strict side — before the
/// controller agrees with the console.
pub const MARKER_POLL_SECONDS: u64 = 15;

/// PROD-16.1 — the fresh-install marker as this process last read it. Starts
/// [`InstallationMarker::Unmarked`] (strict); a failed read keeps the last
/// value read, because the marker is create-once and an API-server blip is
/// not an uninstall.
#[derive(Clone, Debug, Default)]
pub struct MarkerHandle(Arc<RwLock<InstallationMarker>>);

impl MarkerHandle {
    /// The marker as last read.
    #[must_use]
    pub fn get(&self) -> InstallationMarker {
        self.0.read().map_or(InstallationMarker::Unmarked, |m| *m)
    }

    /// Record a read.
    pub fn set(&self, marker: InstallationMarker) {
        if let Ok(mut current) = self.0.write() {
            *current = marker;
        }
    }
}

/// What a reader extracts from a `TrustPolicy` for
/// [`logweir_core::approval_policy::verify_marker`].
#[must_use]
pub fn trust_policy_facts(policy: &TrustPolicy) -> TrustPolicyFacts {
    let annotation = |key: &str| policy.annotations().get(key).cloned();
    TrustPolicyFacts {
        uid: policy.metadata.uid.clone().unwrap_or_default(),
        default: policy.spec.default,
        created_by: annotation(CREATED_BY_ANNOTATION),
        approval_default: annotation(APPROVAL_DEFAULT_ANNOTATION),
        keys: policy
            .spec
            .keys
            .iter()
            .map(|k| TrustKeyFacts {
                key_id: k.key_id.clone(),
                usages: k
                    .usages
                    .iter()
                    .map(|u| {
                        match u {
                            KeyUsage::EvidenceSigning => "EvidenceSigning",
                            KeyUsage::GovernedApproval => "GovernedApproval",
                            KeyUsage::ConsoleConfirmation => "ConsoleConfirmation",
                        }
                        .to_string()
                    })
                    .collect(),
                principal_id: k.principal.id.clone(),
            })
            .collect(),
    }
}

/// THE ONE VERDICT the controller and the console both reach over the
/// fresh-install marker (PROD-16.1 security review): the annotation and
/// `key-id` of the public identity ConfigMap in `namespace`, and the
/// `TrustPolicy` the annotation's claim names as the reader found it.
///
/// # Errors
///
/// A [`MarkerRefusal`] — the caller treats it as unmarked and logs a WARN.
pub fn marker_verdict(
    annotation: Option<&str>,
    identity_key_id: Option<&str>,
    namespace: &str,
    policy: Option<&TrustPolicy>,
) -> Result<InstallationMarker, MarkerRefusal> {
    let facts = policy.map(trust_policy_facts);
    verify_marker(annotation, identity_key_id, namespace, facts.as_ref())
}

/// The name of the `TrustPolicy` a marker annotation claims, when it is a
/// claim at all — what a reader fetches before [`marker_verdict`].
#[must_use]
pub fn claimed_policy(annotation: Option<&str>) -> Option<String> {
    annotation
        .and_then(MarkerClaim::parse)
        .map(|claim| claim.policy_name)
}

/// One read of the marker: the identity ConfigMap, then the policy it claims.
async fn read_marker(
    client: &kube::Client,
    namespace: &str,
    name: &str,
) -> Result<Result<InstallationMarker, MarkerRefusal>, kube::Error> {
    let configmaps: Api<ConfigMap> = Api::namespaced(client.clone(), namespace);
    let Some(config_map) = configmaps.get_opt(name).await? else {
        return Ok(Ok(InstallationMarker::Unmarked));
    };
    let annotation = config_map
        .metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get(APPROVAL_DEFAULT_ANNOTATION))
        .map(String::as_str);
    let identity_key_id = config_map
        .data
        .as_ref()
        .and_then(|d| d.get("key-id"))
        .map(String::as_str);
    // A `list` by name, not a `get`: this controller's grant on the kind is
    // `list`/`watch`/`patch` (its reflector and the compromise finalizer),
    // and the marker is no reason to widen it.
    let policy = match claimed_policy(annotation) {
        Some(policy) => Api::<TrustPolicy>::all(client.clone())
            .list(&kube::api::ListParams::default().fields(&format!("metadata.name={policy}")))
            .await?
            .items
            .into_iter()
            .find(|p| p.name_any() == policy),
        None => None,
    };
    Ok(marker_verdict(
        annotation,
        identity_key_id,
        namespace,
        policy.as_ref(),
    ))
}

/// What one read did to the marker handle, for the log.
#[derive(Debug, PartialEq, Eq)]
pub enum MarkerRead<E> {
    /// The verdict differs from the last one (or is the first).
    Changed(Result<InstallationMarker, MarkerRefusal>),
    /// The same verdict as the last read.
    Unchanged,
    /// The read failed; the handle keeps what it held.
    Failed(E),
}

/// THE CONTROLLER'S LAST MILE (PROD-16.1 fix round, review M1): one read's
/// effect on the handle. An honoured marker is `FreshInstallConfirm`; a
/// REFUSED marker is `Unmarked` — never anything else — and a failed read
/// keeps the last verdict (the marker is create-once, so an API-server blip
/// must not flip an honoured install back to strict, and a handle that was
/// never set is `Unmarked`).
pub fn apply_marker_read<E>(
    handle: &MarkerHandle,
    last: &mut Option<Result<InstallationMarker, MarkerRefusal>>,
    read: Result<Result<InstallationMarker, MarkerRefusal>, E>,
) -> MarkerRead<E> {
    match read {
        Ok(verdict) => {
            handle.set(verdict.unwrap_or(InstallationMarker::Unmarked));
            if *last == Some(verdict) {
                MarkerRead::Unchanged
            } else {
                *last = Some(verdict);
                MarkerRead::Changed(verdict)
            }
        }
        Err(error) => MarkerRead::Failed(error),
    }
}

/// Read the marker every [`MARKER_POLL_SECONDS`] until the process ends. A
/// marker that is not honoured is unmarked, logged as a WARN whenever the
/// verdict changes; a failed read keeps the last verdict
/// ([`apply_marker_read`]).
pub async fn poll_marker(
    client: kube::Client,
    namespace: String,
    name: String,
    handle: MarkerHandle,
) {
    let mut last: Option<Result<InstallationMarker, MarkerRefusal>> = None;
    loop {
        let read = read_marker(&client, &namespace, &name).await;
        match apply_marker_read(&handle, &mut last, read) {
            MarkerRead::Changed(Ok(marker)) => tracing::info!(
                configmap = %format!("{namespace}/{name}"),
                marker = ?marker,
                "the installation's fresh-install marker; an unbound namespace without \
                 a defaultMode resolves to default-confirm-v1 only when it is \
                 FreshInstallConfirm"
            ),
            MarkerRead::Changed(Err(refusal)) => tracing::warn!(
                configmap = %format!("{namespace}/{name}"),
                refusal = refusal.as_str(),
                "a fresh-install marker is present and NOT honoured: unbound \
                 namespaces stay legacy-governed-v1"
            ),
            MarkerRead::Unchanged => {}
            MarkerRead::Failed(error) => tracing::warn!(
                configmap = %format!("{namespace}/{name}"),
                %error,
                marker = ?handle.get(),
                "the fresh-install marker could not be read; keeping the last verdict"
            ),
        }
        tokio::time::sleep(std::time::Duration::from_secs(MARKER_POLL_SECONDS)).await;
    }
}

/// PROD-16.1 — what the Approval and Restore reconcilers resolve against: the
/// installation document read at startup, and the marker as last read.
#[derive(Clone, Debug, Default)]
pub struct PolicySource {
    base: Arc<ApprovalPolicySet>,
    marker: MarkerHandle,
}

impl PolicySource {
    /// A source over `base` and a marker handle.
    #[must_use]
    pub fn new(base: Arc<ApprovalPolicySet>, marker: MarkerHandle) -> Self {
        Self { base, marker }
    }

    /// A source that never reads a marker (rows, and an install with no
    /// managed identity).
    #[must_use]
    pub fn fixed(base: Arc<ApprovalPolicySet>) -> Self {
        Self::new(base, MarkerHandle::default())
    }

    /// The document as this reconcile sees it, the marker applied.
    #[must_use]
    pub fn effective(&self) -> ApprovalPolicySet {
        (*self.base).clone().with_installation(self.marker.get())
    }
}

/// The installation's approval policies, from the value `main` read out of
/// [`APPROVAL_POLICY_FILE_ENV`] and a reader for the file it names.
///
/// # Errors
///
/// A message naming the file and what is wrong with it; `main` refuses to
/// start on it.
pub fn configured_policy(
    variable: Result<String, std::env::VarError>,
    read: impl FnOnce(&Path) -> std::io::Result<String>,
) -> Result<ApprovalPolicySet, String> {
    let Ok(path) = variable else {
        return Ok(ApprovalPolicySet::default());
    };
    let path = path.trim();
    if path.is_empty() {
        return Ok(ApprovalPolicySet::default());
    }
    let text = read(Path::new(path)).map_err(|e| {
        format!("{APPROVAL_POLICY_FILE_ENV} names {path}, which could not be read: {e}")
    })?;
    ApprovalPolicySet::parse(&text).map_err(|e| format!("{path}: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use logweir_core::approval_policy::{ApprovalMode, EffectivePolicy};

    #[test]
    fn absent_and_empty_are_the_legacy_installation() {
        for variable in [
            Err(std::env::VarError::NotPresent),
            Ok(String::new()),
            Ok("  ".into()),
        ] {
            let set = configured_policy(variable, |_| unreachable!("nothing is read"))
                .unwrap_or_else(|e| panic!("{e}"));
            assert_eq!(set.resolve("any"), EffectivePolicy::Legacy);
        }
    }

    #[test]
    fn an_unreadable_file_or_a_bad_document_is_a_refusal_naming_the_file() {
        let unreadable = configured_policy(Ok("/nope/policy.yaml".into()), |_| {
            Err(std::io::Error::new(std::io::ErrorKind::NotFound, "gone"))
        });
        assert!(unreadable
            .err()
            .is_some_and(|e| e.contains("/nope/policy.yaml") && e.contains("gone")));
        let bad = configured_policy(Ok("/p.yaml".into()), |_| Ok("bogus: 1\n".into()));
        assert!(bad.err().is_some_and(|e| e.contains("/p.yaml")));
    }

    /// The installation TrustPolicy exactly as the identity hook writes it,
    /// parsed as the controller's reflector would hold it.
    fn hook_policy() -> TrustPolicy {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "logweir.dev/v1alpha1",
            "kind": "TrustPolicy",
            "metadata": {
                "name": "logweir-installation",
                "uid": "uid-7",
                "annotations": {
                    "logweir.dev/created-by": "identity-bootstrap",
                    "logweir.dev/approval-default": "confirm"
                }
            },
            "spec": {
                "default": true,
                "keys": [
                    {"keyId": "a".repeat(64), "algorithm": "p256", "usages": ["EvidenceSigning"],
                     "state": "Active", "notBefore": "2026-10-07T09:55:00Z",
                     "notAfter": "9999-12-31T23:59:59Z",
                     "principal": {"id": "install:logweir-system/logweir-signing-key"},
                     "spkiPem": "x"},
                    {"keyId": "b".repeat(64), "algorithm": "ed25519",
                     "usages": ["ConsoleConfirmation"], "state": "Active",
                     "notBefore": "2026-10-07T09:55:00Z", "notAfter": "9999-12-31T23:59:59Z",
                     "principal": {"id": "console:logweir-system/logweir-console-confirmation"},
                     "spkiPem": "y"}
                ]
            }
        }))
        .expect("a TrustPolicy")
    }

    fn claim() -> String {
        MarkerClaim {
            policy_name: "logweir-installation".into(),
            policy_uid: "uid-7".into(),
            signing_key_id: "a".repeat(64),
            console_key_id: "b".repeat(64),
        }
        .to_annotation()
    }

    /// The controller's half of the bound marker, over CRD objects: the same
    /// verdict the console reaches (both call `marker_verdict`).
    #[test]
    fn the_controller_honours_the_marker_only_beside_the_hook_made_policy() {
        let id = "a".repeat(64);
        let ns = "logweir-system";
        let claim = claim();
        assert_eq!(
            claimed_policy(Some(&claim)).as_deref(),
            Some("logweir-installation")
        );
        assert_eq!(claimed_policy(Some("confirm")), None);
        assert_eq!(
            marker_verdict(Some(&claim), Some(&id), ns, Some(&hook_policy())),
            Ok(InstallationMarker::FreshInstallConfirm)
        );
        // NEGATIVE CONTROLS, one binding broken each.
        assert_eq!(
            marker_verdict(Some("confirm"), Some(&id), ns, Some(&hook_policy())),
            Err(MarkerRefusal::NotAClaim)
        );
        assert_eq!(
            marker_verdict(Some(&claim), Some(&id), ns, None),
            Err(MarkerRefusal::PolicyMissing),
            "an upgraded install has no hook-made policy"
        );
        let mut hand_made = hook_policy();
        hand_made.metadata.annotations = None;
        assert_eq!(
            marker_verdict(Some(&claim), Some(&id), ns, Some(&hand_made)),
            Err(MarkerRefusal::NotHookMade)
        );
        let mut recreated = hook_policy();
        recreated.metadata.uid = Some("uid-8".into());
        assert_eq!(
            marker_verdict(Some(&claim), Some(&id), ns, Some(&recreated)),
            Err(MarkerRefusal::PolicyReplaced)
        );
        let mut governed = hook_policy();
        governed.spec.keys[1].usages = vec![KeyUsage::GovernedApproval];
        assert_eq!(
            marker_verdict(Some(&claim), Some(&id), ns, Some(&governed)),
            Err(MarkerRefusal::KeysDiffer)
        );
        assert_eq!(
            marker_verdict(
                Some(&claim),
                Some(&"c".repeat(64)),
                ns,
                Some(&hook_policy())
            ),
            Err(MarkerRefusal::OtherIdentity)
        );
    }

    #[test]
    fn the_source_applies_the_marker_it_last_read_and_starts_strict() {
        let source = PolicySource::fixed(Arc::new(ApprovalPolicySet::default()));
        assert_eq!(source.effective().resolve("any"), EffectivePolicy::Legacy);
        let marker = MarkerHandle::default();
        let source = PolicySource::new(Arc::new(ApprovalPolicySet::default()), marker.clone());
        assert_eq!(
            source.effective().resolve("any"),
            EffectivePolicy::Legacy,
            "starts strict"
        );
        marker.set(InstallationMarker::FreshInstallConfirm);
        assert_eq!(
            source.effective().resolve("any").name(),
            logweir_core::approval_policy::DEFAULT_CONFIRM_POLICY_NAME
        );
    }

    #[test]
    fn a_good_document_binds_its_namespaces() {
        let set = configured_policy(Ok("/p.yaml".into()), |_| {
            Ok("allowOrdinaryConfirmation: true\npolicies:\n  - name: o\n    mode: Ordinary\nnamespaces:\n  team-a: o\n".into())
        })
        .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(set.resolve("team-a").mode(), ApprovalMode::Ordinary);
    }

    /// THE CONTROLLER'S LAST MILE (PROD-16.1 fix round, review M1, whose
    /// mutant R3 — `unwrap_or(FreshInstallConfirm)` — survived every suite).
    #[test]
    fn a_refused_marker_is_unmarked_and_a_failed_read_keeps_the_last_verdict() {
        let handle = MarkerHandle::default();
        let mut last = None;
        // Never read: unmarked.
        assert_eq!(handle.get(), InstallationMarker::Unmarked);
        // A read that fails before any verdict keeps unmarked.
        assert_eq!(
            apply_marker_read(&handle, &mut last, Err::<Result<_, _>, _>("503")),
            MarkerRead::Failed("503")
        );
        assert_eq!(handle.get(), InstallationMarker::Unmarked);
        // An honoured marker.
        assert_eq!(
            apply_marker_read(
                &handle,
                &mut last,
                Ok::<_, &str>(Ok(InstallationMarker::FreshInstallConfirm))
            ),
            MarkerRead::Changed(Ok(InstallationMarker::FreshInstallConfirm))
        );
        assert_eq!(handle.get(), InstallationMarker::FreshInstallConfirm);
        // The same verdict again is not logged again.
        assert_eq!(
            apply_marker_read(
                &handle,
                &mut last,
                Ok::<_, &str>(Ok(InstallationMarker::FreshInstallConfirm))
            ),
            MarkerRead::Unchanged
        );
        // A failed read after it keeps it (an API-server blip).
        assert_eq!(
            apply_marker_read(&handle, &mut last, Err::<Result<_, _>, _>("timeout")),
            MarkerRead::Failed("timeout")
        );
        assert_eq!(handle.get(), InstallationMarker::FreshInstallConfirm);
        // EVERY refusal is unmarked — a forged or replaced marker turns the
        // controller strict at once.
        for refusal in [
            MarkerRefusal::NotAClaim,
            MarkerRefusal::OtherIdentity,
            MarkerRefusal::PolicyMissing,
            MarkerRefusal::PolicyReplaced,
            MarkerRefusal::NotHookMade,
            MarkerRefusal::NotDefault,
            MarkerRefusal::KeysDiffer,
        ] {
            handle.set(InstallationMarker::FreshInstallConfirm);
            let mut fresh_last = Some(Ok(InstallationMarker::FreshInstallConfirm));
            assert_eq!(
                apply_marker_read(&handle, &mut fresh_last, Ok::<_, &str>(Err(refusal))),
                MarkerRead::Changed(Err(refusal))
            );
            assert_eq!(handle.get(), InstallationMarker::Unmarked, "{refusal:?}");
            // And the reconcilers see legacy for an unbound namespace.
            let source = PolicySource::new(Arc::new(ApprovalPolicySet::default()), handle.clone());
            assert_eq!(
                source.effective().resolve("unbound"),
                EffectivePolicy::Legacy
            );
        }
    }
}
