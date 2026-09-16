//! The check plan `ConfigMap` — D2 §4.3's `plan.rs` row.
//!
//! # Immutable, owned, and digest-pinned
//!
//! The runner mounts this at [`super::job::CHECK_MOUNT_PATH`] and refuses any
//! plan whose SHA-256 is not the one pinned in its environment (D2 §4.2's
//! startup step 3). Three properties make that refusal meaningful:
//!
//! * **`immutable: true`.** A `ConfigMap` a second pass could rewrite is a
//!   document a running pod already mounted and would then see change under it.
//! * **An owner reference with `controller: true`.** The object goes away with
//!   its check through garbage collection, so nothing here deletes it — the
//!   rule this whole directory keeps, and the reason the `weirkeeper`
//!   `ClusterRole` grants `delete` on nothing.
//! * **The digest on an annotation.** It is what makes the 409 rule below a
//!   comparison rather than a guess.
//!
//! # The 409 rule, and why a conflict is TERMINAL
//!
//! The Job's name — and therefore this object's — is a pure function of the
//! subject's UID and the check kind ([`super::job::check_job_name`]), so a
//! duplicate reconcile computes the same name and gets 409 `AlreadyExists`.
//! That is the normal, healthy case and it is ACCEPTED, but only when the
//! object already there is the same document owned by the same subject and
//! still immutable. Anything else means two different plans want one name:
//! retrying cannot fix it, and running the OTHER one would execute a check
//! against inputs this pass never rendered. So it is
//! [`CheckCode::CheckPlanConflict`], terminal, and the caller writes it to the
//! status rather than requeueing.

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::ConfigMap;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kube::api::{Api, ObjectMeta, PostParams};
use kube::ResourceExt as _;

use logweir_core::check_contract::CheckCode;

use crate::job::RunnerOwner;

/// The suffix a check Job's plan `ConfigMap` takes.
pub const PLAN_SUFFIX: &str = "-plan";

/// The annotation carrying `sha256:<hex>` of the plan document.
///
/// The SAME value the Job pins in [`super::job::PLAN_SHA256_ENV`]. One
/// computation, two places, so the 409 comparison and the runner's own refusal
/// cannot disagree about which document this is.
pub const DIGEST_ANNOTATION: &str = "logweir.dev/check-plan-sha256";

/// The trust-bundle keys D2 §4.3 names, in the order they are written.
pub const SOURCE_CA_KEY: &str = "source-ca.pem";
/// See [`SOURCE_CA_KEY`].
pub const TARGET_CA_KEY: &str = "target-ca.pem";
/// See [`SOURCE_CA_KEY`].
pub const ARCHIVE_CA_KEY: &str = "archive-ca.pem";
/// See [`SOURCE_CA_KEY`].
pub const EVIDENCE_CA_KEY: &str = "evidence-ca.pem";

/// The verbatim restore plan bytes a `restorePreflight` reads (D2 §4.3).
pub const RESTORE_PLAN_KEY: &str = "plan.yaml";

/// The plan `ConfigMap`'s name for a check Job: `<job>-plan`.
#[must_use]
pub fn plan_config_map_name(job_name: &str) -> String {
    format!("{job_name}{PLAN_SUFFIX}")
}

/// Everything one check's plan `ConfigMap` carries.
///
/// The CA bundles and the restore plan are `Option` because the kinds differ:
/// a `topicInventory` has one connection and no destination, a
/// `restorePreflight` has a target, two destinations and the plan bytes. A key
/// that is `None` is ABSENT from `data` rather than empty — an empty
/// `source-ca.pem` would make the runner configure a trust store with no
/// certificates in it, which fails differently from having no custom CA at all.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PlanDocuments {
    /// The check plan itself — D2 §4.1's `CheckPlan`, serialised.
    pub check_plan: Vec<u8>,
    /// The source connection's CA bundle.
    pub source_ca: Option<Vec<u8>>,
    /// The target connection's CA bundle.
    pub target_ca: Option<Vec<u8>>,
    /// The archive destination's CA bundle.
    pub archive_ca: Option<Vec<u8>>,
    /// The evidence destination's CA bundle.
    pub evidence_ca: Option<Vec<u8>>,
    /// The restore plan's bytes, VERBATIM. Never re-serialised: the digest the
    /// preflight checks is over what the user submitted.
    pub restore_plan: Option<Vec<u8>>,
}

impl PlanDocuments {
    /// `sha256:<hex>` of the check plan document — the value the Job pins and
    /// the annotation records.
    #[must_use]
    pub fn check_plan_sha256(&self) -> String {
        logweir_core::ids::sha256_prefixed(&self.check_plan)
    }

    /// The `data` map, in key order.
    ///
    /// `data` and not `binaryData`: every one of these is UTF-8 by
    /// construction — JSON, PEM, YAML — and a `binaryData` entry would be
    /// base64 in the object, which an operator reading
    /// `kubectl get configmap -o yaml` could not check against the digest.
    ///
    /// # Errors
    ///
    /// [`PlanError::NotUtf8`] naming the key, rather than a silent lossy
    /// conversion: a CA bundle that is not UTF-8 is a CA bundle that was read
    /// wrong, and mounting the mangled form would fail inside the pod with a
    /// TLS error that names nothing.
    pub fn data(&self) -> Result<BTreeMap<String, String>, PlanError> {
        let mut out = BTreeMap::new();
        let mut put = |key: &str, bytes: &[u8]| -> Result<(), PlanError> {
            let text =
                std::str::from_utf8(bytes).map_err(|_| PlanError::NotUtf8(key.to_string()))?;
            out.insert(key.to_string(), text.to_string());
            Ok(())
        };
        put(super::job::CHECK_PLAN_KEY, &self.check_plan)?;
        for (key, bytes) in [
            (SOURCE_CA_KEY, self.source_ca.as_ref()),
            (TARGET_CA_KEY, self.target_ca.as_ref()),
            (ARCHIVE_CA_KEY, self.archive_ca.as_ref()),
            (EVIDENCE_CA_KEY, self.evidence_ca.as_ref()),
            (RESTORE_PLAN_KEY, self.restore_plan.as_ref()),
        ] {
            if let Some(bytes) = bytes {
                put(key, bytes)?;
            }
        }
        Ok(out)
    }
}

/// Why a plan `ConfigMap` could not be rendered or adopted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PlanError {
    /// A document is not UTF-8, naming the key.
    NotUtf8(String),
    /// An object with this name exists and is not this check's plan.
    Conflict(String),
}

impl PlanError {
    /// The closed code a status carries for this.
    #[must_use]
    pub fn code(&self) -> CheckCode {
        CheckCode::CheckPlanConflict
    }
}

impl std::fmt::Display for PlanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotUtf8(key) => write!(f, "the plan document `{key}` is not UTF-8"),
            Self::Conflict(why) => write!(f, "{why}"),
        }
    }
}

impl std::error::Error for PlanError {}

/// The immutable, owned plan `ConfigMap`.
///
/// # Errors
///
/// [`PlanError::NotUtf8`] from [`PlanDocuments::data`].
pub fn build(
    job_name: &str,
    namespace: &str,
    owner: &RunnerOwner,
    documents: &PlanDocuments,
) -> Result<ConfigMap, PlanError> {
    Ok(ConfigMap {
        metadata: ObjectMeta {
            name: Some(plan_config_map_name(job_name)),
            namespace: Some(namespace.to_string()),
            annotations: Some(BTreeMap::from([(
                DIGEST_ANNOTATION.to_string(),
                documents.check_plan_sha256(),
            )])),
            owner_references: Some(vec![OwnerReference {
                api_version: owner.api_version.clone(),
                kind: owner.kind.clone(),
                name: owner.name.clone(),
                uid: owner.uid.clone(),
                // THE CASCADE IS THE DELETE. Nothing in this crate calls
                // `Api::delete`, and the `ClusterRole` grants it on nothing.
                controller: Some(true),
                block_owner_deletion: Some(true),
            }]),
            ..ObjectMeta::default()
        },
        // A RUNNING POD HAS THIS MOUNTED. Immutable is what stops a second
        // reconcile rewriting a document the kubelet is already projecting.
        immutable: Some(true),
        data: Some(documents.data()?),
        binary_data: None,
    })
}

/// Whether an object already at this name IS this check's plan — D2 §4.3's
/// 409 rule.
///
/// All three, and each one matters:
///
/// * the **owner UID** — a name is not an identity, and two subjects whose
///   UIDs collided in twenty hex characters would otherwise share a plan;
/// * the **digest** — the same subject asking for a different document means
///   the inputs changed under a check that is already running;
/// * **`immutable: true`** — an object that can still be rewritten cannot be
///   adopted, whatever it says right now.
///
/// # Errors
///
/// [`PlanError::Conflict`] naming which of the three failed.
pub fn accepts_existing(
    existing: &ConfigMap,
    owner_uid: &str,
    digest: &str,
) -> Result<(), PlanError> {
    let owned = existing
        .metadata
        .owner_references
        .as_deref()
        .unwrap_or_default()
        .iter()
        .any(|o| o.uid == owner_uid && o.controller == Some(true));
    if !owned {
        return Err(PlanError::Conflict(format!(
            "ConfigMap {} exists and is not controlled by this check's subject; a check plan is \
             never adopted across owners",
            existing.name_any()
        )));
    }
    let found = existing
        .metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get(DIGEST_ANNOTATION))
        .map(String::as_str);
    if found != Some(digest) {
        return Err(PlanError::Conflict(format!(
            "ConfigMap {} exists with plan digest {} and this pass rendered {digest}; the check's \
             inputs changed under a plan that is already mounted",
            existing.name_any(),
            found.unwrap_or("<none>")
        )));
    }
    if existing.immutable != Some(true) {
        return Err(PlanError::Conflict(format!(
            "ConfigMap {} exists without `immutable: true`, so its content could change under a \
             running pod; it is not adopted",
            existing.name_any()
        )));
    }
    Ok(())
}

/// What [`ensure`] did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PlanOutcome {
    /// This pass created it.
    Created,
    /// It was already there and is the same document, same owner, immutable.
    Adopted,
}

/// Create the plan `ConfigMap`, accepting an identical one that is already
/// there.
///
/// # Errors
///
/// [`EnsureError::Api`] for a transport failure (requeue) and
/// [`EnsureError::Plan`] for a terminal [`CheckCode::CheckPlanConflict`].
pub async fn ensure(
    client: &kube::Client,
    namespace: &str,
    config_map: &ConfigMap,
    owner_uid: &str,
    digest: &str,
) -> Result<PlanOutcome, EnsureError> {
    let maps: Api<ConfigMap> = Api::namespaced(client.clone(), namespace);
    match maps.create(&PostParams::default(), config_map).await {
        Ok(_) => Ok(PlanOutcome::Created),
        Err(kube::Error::Api(response)) if response.code == 409 => {
            // THE ONLY BRANCH THAT READS THE EXISTING OBJECT. A 409 is the
            // healthy duplicate-reconcile case, and the `get` is what turns it
            // into either an adoption or a named refusal.
            let existing = maps
                .get_opt(&config_map.name_any())
                .await
                .map_err(EnsureError::Api)?
                .ok_or_else(|| {
                    // Created and deleted between the two calls. Requeue: the
                    // next pass creates it.
                    EnsureError::Api(kube::Error::Api(response.clone()))
                })?;
            accepts_existing(&existing, owner_uid, digest).map_err(EnsureError::Plan)?;
            Ok(PlanOutcome::Adopted)
        }
        Err(e) => Err(EnsureError::Api(e)),
    }
}

/// Why [`ensure`] did not produce an outcome.
#[derive(Debug)]
pub enum EnsureError {
    /// The API server could not be talked to. **Requeue.**
    Api(kube::Error),
    /// A terminal refusal. The caller writes it to the status.
    Plan(PlanError),
}

impl std::fmt::Display for EnsureError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Api(e) => write!(f, "the Kubernetes API returned an error: {e}"),
            Self::Plan(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for EnsureError {}
