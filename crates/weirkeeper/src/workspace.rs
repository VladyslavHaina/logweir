//! Workspaces: the groups inside the one namespace an install lives in
//! (PROD-19.1).
//!
//! An install is one Helm release in one namespace. A workspace (`dev`, `prd`,
//! …) is a name in the chart's `workspaces` list, which this process gets as
//! [`WORKSPACES_ENV`], and the label [`LABEL`] on every Logweir object. The
//! label is required and has no default: a label selector cannot match
//! "absent or `dev`", so an unlabelled object would run while no console
//! showed it.
//!
//! Two refusals, both terminal for the object that carries them:
//! * [`REASON_UNKNOWN`]: the label is absent, or names a workspace that is not
//!   in the list ([`Workspaces::of`]);
//! * [`REASON_MISMATCH`]: a spec reference names an object of another
//!   workspace ([`same`]).
//!
//! [`gate`] applies both at the top of every namespaced reconcile, to the
//! object and to every object and **Secret** it names (directly, or through
//! its `KafkaCluster`s and `BackupDestination`s). The Secret rule is what keeps
//! the install's keys out of runner Jobs now that they share the namespace:
//! the signing key, the console's keys and the OIDC client secret carry no
//! workspace label, so no object can name them, and a credential Secret must
//! carry its referrer's label (the API writes it on every credential it
//! creates; a `kubectl` writer must set it).

use std::collections::HashSet;
use std::fmt;
use std::sync::OnceLock;

use chrono::Utc;
use k8s_openapi::api::core::v1::Secret;
use k8s_openapi::NamespaceResourceScope;
use kube::api::{Patch, PatchParams};
use kube::runtime::controller::Action;
use kube::{Api, Client, Resource, ResourceExt};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::json;

use crate::crds::approval::{Approval, SubjectKind};
use crate::crds::backup::Backup;
use crate::crds::backup_destination::{BackupDestination, S3SecretKeysRef};
use crate::crds::backup_schedule::BackupSchedule;
use crate::crds::kafka_cluster::KafkaCluster;
use crate::crds::protection_policy::ProtectionPolicy;
use crate::crds::recovery_catalog::RecoveryCatalog;
use crate::crds::restore::Restore;
use crate::crds::retention_policy::RetentionPolicy;
use crate::crds::switchover::Switchover;
use crate::crds::{ArchiveRef, LocalRef};

/// The label every Logweir object carries.
pub const LABEL: &str = "logweir.dev/workspace";

/// The environment variable that lists the install's workspaces.
pub const WORKSPACES_ENV: &str = "LOGWEIR_WORKSPACES";

/// The condition reason for an object whose label is absent or unlisted.
pub const REASON_UNKNOWN: &str = "WorkspaceUnknown";

/// The condition reason for a reference into another workspace.
pub const REASON_MISMATCH: &str = "WorkspaceMismatch";

/// The most workspaces one install may list (the chart's bound).
pub const MAX_WORKSPACES: usize = 256;

/// A workspace name: a DNS label.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Workspace(String);

impl Workspace {
    /// A workspace name, or why it is not one.
    ///
    /// # Errors
    ///
    /// A sentence naming the value.
    pub fn new(value: &str) -> Result<Self, String> {
        if is_dns_label(value) {
            Ok(Self(value.to_string()))
        } else {
            Err(format!(
                "`{value}` is not a workspace name (a DNS label: lower-case letters, digits and \
                 `-`, at most 63 characters)"
            ))
        }
    }

    /// The name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Workspace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// The install's workspaces, as [`WORKSPACES_ENV`] lists them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Workspaces(Vec<Workspace>);

impl Workspaces {
    /// Parse [`WORKSPACES_ENV`]: a comma list of 1 to [`MAX_WORKSPACES`]
    /// unique DNS labels. Pure, so a test can hand it any value.
    ///
    /// # Errors
    ///
    /// Unset, empty, an entry that is not a DNS label, a duplicate, or too
    /// many: a sentence naming the variable. The process refuses to start.
    pub fn configured(raw: Result<String, std::env::VarError>) -> Result<Self, String> {
        let raw = raw.unwrap_or_default();
        let mut list: Vec<Workspace> = Vec::new();
        for entry in raw.split(',').map(str::trim) {
            if entry.is_empty() {
                continue;
            }
            let workspace =
                Workspace::new(entry).map_err(|why| format!("{WORKSPACES_ENV}: {why}"))?;
            if list.contains(&workspace) {
                return Err(format!("{WORKSPACES_ENV} lists `{entry}` twice"));
            }
            list.push(workspace);
        }
        if list.is_empty() {
            return Err(format!("{WORKSPACES_ENV} lists no workspace"));
        }
        if list.len() > MAX_WORKSPACES {
            return Err(format!(
                "{WORKSPACES_ENV} lists {} workspaces; at most {MAX_WORKSPACES}",
                list.len()
            ));
        }
        Ok(Self(list))
    }

    /// The workspaces, in the configured order.
    #[must_use]
    pub fn list(&self) -> &[Workspace] {
        &self.0
    }

    /// The workspace an object belongs to.
    ///
    /// # Errors
    ///
    /// [`Refusal::Unknown`] when the label is absent or not in the list.
    pub fn of<K: Resource>(&self, object: &K) -> Result<Workspace, Refusal> {
        let label = label_of(object);
        match label {
            Some(value) if self.0.iter().any(|w| w.as_str() == value) => {
                Ok(Workspace(value.to_string()))
            }
            _ => Err(Refusal::Unknown {
                label: label.map(str::to_string),
            }),
        }
    }
}

/// Why an object is refused for its workspace.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Refusal {
    /// The label is absent (`None`) or names an unlisted workspace.
    Unknown {
        /// The label's value, when there is one.
        label: Option<String>,
    },
    /// A reference reaches into another workspace.
    Mismatch {
        /// The referent's kind.
        referent_kind: String,
        /// The referent's name.
        referent_name: String,
        /// The referent's workspace, when it has a label.
        referent_workspace: Option<String>,
        /// The referrer's kind.
        referrer_kind: String,
        /// The referrer's workspace, when it has a label.
        referrer_workspace: Option<String>,
    },
}

impl Refusal {
    /// The condition reason.
    #[must_use]
    pub const fn reason(&self) -> &'static str {
        match self {
            Refusal::Unknown { .. } => REASON_UNKNOWN,
            Refusal::Mismatch { .. } => REASON_MISMATCH,
        }
    }
}

impl fmt::Display for Refusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let named = |w: &Option<String>| match w {
            Some(w) => format!("workspace `{w}`"),
            None => "no workspace".to_string(),
        };
        match self {
            Refusal::Unknown { label: None } => write!(
                f,
                "the object has no `{LABEL}` label; label it with one of this install's workspaces"
            ),
            Refusal::Unknown { label: Some(label) } => write!(
                f,
                "workspace `{label}` is not one of this install's workspaces ({WORKSPACES_ENV})"
            ),
            Refusal::Mismatch {
                referent_kind,
                referent_name,
                referent_workspace,
                referrer_kind,
                referrer_workspace,
            } => write!(
                f,
                "{referent_kind} `{referent_name}` is in {}; this {referrer_kind} is in {}",
                named(referent_workspace),
                named(referrer_workspace),
            ),
        }
    }
}

/// Whether `referent` may be named by `referrer`: both carry the same
/// workspace label.
///
/// # Errors
///
/// [`Refusal::Mismatch`], naming both workspaces. Two objects with no label
/// are not in the same workspace: [`Workspaces::of`] refuses each of them
/// first, and this does not treat an absence as a match.
pub fn same<A, B>(referrer: &A, referent: &B) -> Result<(), Refusal>
where
    A: Resource<DynamicType = ()>,
    B: Resource<DynamicType = ()>,
{
    let mine = label_of(referrer);
    let theirs = label_of(referent);
    if mine.is_some() && mine == theirs {
        return Ok(());
    }
    Err(Refusal::Mismatch {
        referent_kind: B::kind(&()).into_owned(),
        referent_name: referent.meta().name.clone().unwrap_or_default(),
        referent_workspace: theirs.map(str::to_string),
        referrer_kind: A::kind(&()).into_owned(),
        referrer_workspace: mine.map(str::to_string),
    })
}

static INSTALL: OnceLock<(String, Workspaces)> = OnceLock::new();

/// Fix this process's install: the one namespace every watch runs in, and
/// the workspaces. `main` calls it once, before any reconciler starts; a
/// second call is ignored and returns `false`.
pub fn install(namespace: String, workspaces: Workspaces) -> bool {
    INSTALL.set((namespace, workspaces)).is_ok()
}

/// The install's workspaces. Before [`install`] the list is empty, so every
/// object is refused [`REASON_UNKNOWN`]: the closed side.
#[must_use]
pub fn installed() -> &'static Workspaces {
    static NONE: Workspaces = Workspaces(Vec::new());
    INSTALL.get().map_or(&NONE, |(_, workspaces)| workspaces)
}

/// The install's namespace (`LOGWEIR_INSTALLATION_NAMESPACE`), where every
/// namespaced watch runs.
#[must_use]
pub fn namespace() -> &'static str {
    INSTALL
        .get()
        .map_or("", |(namespace, _)| namespace.as_str())
}

/// One object a Logweir object names, whose workspace label must be the
/// referrer's.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Referent {
    /// A `KafkaCluster`; its own Secrets are checked against the referrer too.
    Cluster(String),
    /// A `BackupDestination`; its own Secrets are checked too.
    Destination(String),
    /// A `RecoveryCatalog`.
    Catalog(String),
    /// A `BackupSchedule`.
    Schedule(String),
    /// A `Backup`.
    Backup(String),
    /// A `Restore`.
    Restore(String),
    /// A `Switchover`.
    Switchover(String),
    /// An `Approval`.
    Approval(String),
    /// A Secret the object names: a runner Job would mount it, so a Secret
    /// without the referrer's label (the signing key, the console's keys, the
    /// OIDC client secret, another workspace's credential) is refused.
    Secret(String),
}

/// A Logweir kind the workspace gate admits.
pub trait Referrer:
    Resource<DynamicType = (), Scope = NamespaceResourceScope>
    + Clone
    + DeserializeOwned
    + Serialize
    + fmt::Debug
{
    /// Every object its spec names (PROD-19.1 §4.1).
    fn referents(&self) -> Vec<Referent>;
}

/// A reference's name.
#[must_use]
pub fn local(r: &LocalRef) -> String {
    r.name.clone()
}

/// The Secret an archive reference names, as a referent.
#[must_use]
pub fn archive_secret(archive: Option<&ArchiveRef>) -> Option<Referent> {
    archive
        .and_then(|a| a.secret_ref.as_ref())
        .map(|s| Referent::Secret(s.name.clone()))
}

impl Referrer for KafkaCluster {
    fn referents(&self) -> Vec<Referent> {
        let auth = &self.spec.auth;
        [
            auth.secret_ref.as_ref().map(|s| s.name.clone()),
            auth.client_certificate.as_ref().map(|s| s.name.clone()),
            auth.tls_ca
                .as_ref()
                .and_then(|ca| ca.secret_key_ref.as_ref())
                .map(|s| s.name.clone()),
            // PROD-03.1: the registry's basic-auth password.
            self.spec
                .schema_registry
                .as_ref()
                .and_then(|r| r.secret_ref.as_ref())
                .map(|s| s.name.clone()),
        ]
        .into_iter()
        .flatten()
        .map(Referent::Secret)
        .collect()
    }
}

impl Referrer for BackupDestination {
    fn referents(&self) -> Vec<Referent> {
        let access = &self.spec.access;
        let named = |s: Option<&S3SecretKeysRef>| s.map(|s| Referent::Secret(s.name.clone()));
        [
            named(access.archive_write.secret.as_ref()),
            named(access.archive_read.as_ref().and_then(|g| g.secret.as_ref())),
            named(
                access
                    .evidence_write
                    .as_ref()
                    .and_then(|g| g.secret.as_ref()),
            ),
            named(
                access
                    .evidence_read
                    .as_ref()
                    .and_then(|g| g.secret.as_ref()),
            ),
        ]
        .into_iter()
        .flatten()
        .collect()
    }
}

impl Referrer for Backup {
    fn referents(&self) -> Vec<Referent> {
        let spec = &self.spec;
        let mut out = vec![Referent::Cluster(local(&spec.source_ref))];
        out.extend(
            spec.destination_ref
                .as_ref()
                .map(|r| Referent::Destination(local(r))),
        );
        out.extend(
            spec.trigger
                .as_ref()
                .and_then(|t| t.retry_of.as_ref())
                .map(|r| Referent::Backup(local(r))),
        );
        out.extend(
            spec.schedule_ref
                .as_ref()
                .map(|r| Referent::Schedule(r.name.clone())),
        );
        out.extend(archive_secret(Some(&spec.archive)));
        out
    }
}

impl Referrer for BackupSchedule {
    fn referents(&self) -> Vec<Referent> {
        let spec = &self.spec;
        let mut out = vec![Referent::Cluster(local(&spec.source_ref))];
        out.extend(
            spec.destination_ref
                .as_ref()
                .map(|r| Referent::Destination(local(r))),
        );
        out.extend(archive_secret(Some(&spec.archive)));
        out
    }
}

impl Referrer for Restore {
    fn referents(&self) -> Vec<Referent> {
        let spec = &self.spec;
        let mut out = vec![Referent::Cluster(local(&spec.target.cluster_ref))];
        out.extend(
            spec.approval_ref
                .as_ref()
                .map(|r| Referent::Approval(local(r))),
        );
        out.extend(
            [&spec.source_destination_ref, &spec.evidence_destination_ref]
                .into_iter()
                .flatten()
                .map(|r| Referent::Destination(local(r))),
        );
        out.extend(archive_secret(Some(&spec.source_archive)));
        out
    }
}

impl Referrer for Approval {
    fn referents(&self) -> Vec<Referent> {
        let name = self.spec.subject_ref.name.clone();
        match self.spec.subject_ref.kind {
            SubjectKind::Restore => vec![Referent::Restore(name)],
            SubjectKind::Switchover => vec![Referent::Switchover(name)],
            SubjectKind::Backup => vec![Referent::Backup(name)],
            SubjectKind::RehearsalSchedule => Vec::new(),
        }
    }
}

impl Referrer for Switchover {
    fn referents(&self) -> Vec<Referent> {
        vec![
            Referent::Approval(local(&self.spec.approval_ref)),
            Referent::Restore(self.spec.restore_ref.name.clone()),
        ]
    }
}

impl Referrer for RecoveryCatalog {
    fn referents(&self) -> Vec<Referent> {
        let spec = &self.spec;
        let mut out: Vec<Referent> = spec
            .destination_ref
            .as_ref()
            .map(|r| Referent::Destination(local(r)))
            .into_iter()
            .collect();
        out.extend(archive_secret(spec.legacy_archive.as_ref()));
        out
    }
}

impl Referrer for RetentionPolicy {
    fn referents(&self) -> Vec<Referent> {
        let spec = &self.spec;
        let mut out = vec![
            Referent::Destination(local(&spec.destination_ref)),
            Referent::Catalog(local(&spec.catalog_ref)),
        ];
        out.extend(
            spec.enforcement
                .as_ref()
                .map(|e| Referent::Secret(local(&e.credential_secret_ref))),
        );
        out
    }
}

impl Referrer for ProtectionPolicy {
    fn referents(&self) -> Vec<Referent> {
        let p = &self.spec.protects;
        let mut out = vec![Referent::Cluster(local(&p.source_ref))];
        out.extend(
            p.schedule_refs
                .iter()
                .flatten()
                .map(|r| Referent::Schedule(local(r))),
        );
        out.extend(
            p.destination_ref
                .as_ref()
                .map(|r| Referent::Destination(local(r))),
        );
        out.extend(p.catalog_ref.as_ref().map(|r| Referent::Catalog(local(r))));
        out.extend(archive_secret(p.legacy_archive.as_ref()));
        out
    }
}

/// **The workspace gate**, at the top of every namespaced reconcile: the
/// object's own label must be one of the install's workspaces
/// ([`REASON_UNKNOWN`]), and everything it names must carry the same label
/// ([`REASON_MISMATCH`]): every object and Secret in its spec, and, followed
/// to the end, everything those objects name in turn. A reconcile reads
/// through its referents (a Preflight's Restore's target connection, a
/// Switchover's Restore's destinations), so a same-workspace referent that
/// itself names another workspace's object or Secret is refused here, at the
/// referrer, and not only at its own reconcile.
///
/// A referent that does not exist is left to the reconcile, which already
/// says so. An object being deleted is held to the same rule: a clone's
/// clean-up re-reads its destinations, so a refused clone keeps its finalizer
/// and its status says why.
///
/// # Errors
///
/// A [`kube::Error`] reading a referent: a requeue, never an admission.
pub async fn admit<K: Referrer>(
    client: &Client,
    object: &K,
) -> Result<Option<Refusal>, kube::Error> {
    if let Err(refusal) = installed().of(object) {
        return Ok(Some(refusal));
    }
    let namespace = object.namespace().unwrap_or_default();
    let mut pending = object.referents();
    let mut seen: HashSet<Referent> = HashSet::new();
    while let Some(referent) = pending.pop() {
        if !seen.insert(referent.clone()) {
            continue;
        }
        let (c, ns, p) = (client, namespace.as_str(), &mut pending);
        let verdict = match referent {
            Referent::Cluster(name) => follow::<KafkaCluster, _>(c, ns, object, &name, p).await?,
            Referent::Destination(name) => {
                follow::<BackupDestination, _>(c, ns, object, &name, p).await?
            }
            Referent::Catalog(name) => {
                follow::<RecoveryCatalog, _>(c, ns, object, &name, p).await?
            }
            Referent::Schedule(name) => {
                follow::<BackupSchedule, _>(c, ns, object, &name, p).await?
            }
            Referent::Backup(name) => follow::<Backup, _>(c, ns, object, &name, p).await?,
            Referent::Restore(name) => follow::<Restore, _>(c, ns, object, &name, p).await?,
            Referent::Switchover(name) => follow::<Switchover, _>(c, ns, object, &name, p).await?,
            Referent::Approval(name) => follow::<Approval, _>(c, ns, object, &name, p).await?,
            Referent::Secret(name) => {
                let api: Api<Secret> = Api::namespaced(client.clone(), ns);
                labelled_like(object, api.get_metadata_opt(&name).await?)
            }
        };
        if let Err(refusal) = verdict {
            return Ok(Some(refusal));
        }
    }
    Ok(None)
}

/// Read one referent of kind `R`, queue everything it names, and hold it to
/// the referrer's workspace. Absent: admitted (the reconcile says so).
async fn follow<R: Referrer, K: Referrer>(
    client: &Client,
    namespace: &str,
    object: &K,
    name: &str,
    pending: &mut Vec<Referent>,
) -> Result<Result<(), Refusal>, kube::Error> {
    let api = Api::<R>::namespaced(client.clone(), namespace);
    Ok(match api.get_opt(name).await? {
        Some(found) => {
            pending.extend(found.referents());
            same(object, &found)
        }
        None => Ok(()),
    })
}

fn labelled_like<A, B>(referrer: &A, referent: Option<B>) -> Result<(), Refusal>
where
    A: Resource<DynamicType = ()>,
    B: Resource<DynamicType = ()>,
{
    referent.map_or(Ok(()), |referent| same(referrer, &referent))
}

/// [`admit`], and on a refusal the terminal status: `phase: Failed` (a kind
/// with no phase prunes it) and one `Failed=True` condition with the reason
/// and the sentence, written once. `Some(action)` when the object was refused
/// and the reconcile must stop with it; nothing else is created for it.
///
/// # Errors
///
/// A [`kube::Error`] reading a referent or writing the status.
pub async fn gate<K: Referrer>(client: &Client, object: &K) -> Result<Option<Action>, kube::Error> {
    let Some(refusal) = admit(client, object).await? else {
        return Ok(None);
    };
    let reason = refusal.reason();
    let message = refusal.to_string();
    let written = serde_json::to_value(object)
        .ok()
        .and_then(|v| v.pointer("/status/conditions").cloned())
        .and_then(|c| c.as_array().cloned())
        .unwrap_or_default()
        .iter()
        .any(|c| c["reason"] == reason && c["message"] == message.as_str());
    if !written {
        let api = Api::<K>::namespaced(client.clone(), &object.namespace().unwrap_or_default());
        let patch = json!({
            "status": {
                "phase": "Failed",
                "conditions": [{
                    "type": "Failed",
                    "status": "True",
                    "reason": reason,
                    "message": message,
                    "observedGeneration": object.meta().generation,
                    "lastTransitionTime": Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                }],
            }
        });
        api.patch_status(
            &object.name_any(),
            &PatchParams::default(),
            &Patch::Merge(&patch),
        )
        .await?;
    }
    Ok(Some(Action::await_change()))
}

/// Copy `parent`'s workspace label onto an object the controller creates for
/// it (a Backup from a schedule, a runner Job), so lists, caps and
/// per-workspace views see it.
pub fn copy_label<P: Resource, C: Resource>(parent: &P, child: &mut C) {
    if let Some(workspace) = label_of(parent) {
        child
            .meta_mut()
            .labels
            .get_or_insert_with(Default::default)
            .insert(LABEL.to_string(), workspace.to_string());
    }
}

/// The object's workspace label, as written.
#[must_use]
pub fn label_of<K: Resource>(object: &K) -> Option<&str> {
    object
        .meta()
        .labels
        .as_ref()?
        .get(LABEL)
        .map(String::as_str)
}

fn is_dns_label(value: &str) -> bool {
    let bytes = value.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 63
        && bytes
            .iter()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-')
        && bytes[0] != b'-'
        && bytes[bytes.len() - 1] != b'-'
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::batch::v1::Job;
    use k8s_openapi::api::core::v1::Secret;

    fn listed(raw: &str) -> Workspaces {
        Workspaces::configured(Ok(raw.to_string())).unwrap()
    }

    /// A kind-agnostic helper, so two core kinds with `Default` stand in for
    /// the CRDs: a Job (the referrer) and a Secret (the referent).
    fn labelled<K: Resource + Default>(name: &str, workspace: Option<&str>) -> K {
        let mut object = K::default();
        object.meta_mut().name = Some(name.to_string());
        if let Some(workspace) = workspace {
            object.meta_mut().labels = Some([(LABEL.to_string(), workspace.to_string())].into());
        }
        object
    }

    fn job(workspace: Option<&str>) -> Job {
        labelled("nightly", workspace)
    }

    fn secret(workspace: Option<&str>) -> Secret {
        labelled("source", workspace)
    }

    #[test]
    fn the_list_is_ordered_trimmed_and_exact() {
        let w = listed(" dev, tst,stg ,prd");
        let names: Vec<&str> = w.list().iter().map(Workspace::as_str).collect();
        assert_eq!(names, ["dev", "tst", "stg", "prd"]);
        assert_eq!(listed("default").list().len(), 1);
    }

    #[test]
    fn a_malformed_list_refuses_to_start() {
        let unset = Workspaces::configured(Err(std::env::VarError::NotPresent)).unwrap_err();
        assert!(unset.contains(WORKSPACES_ENV), "{unset}");
        for bad in [
            "",
            " , ",
            "dev,Dev",
            "dev,dev",
            "dev,team_a",
            "-a",
            "a-",
            "dev,*",
            &"a".repeat(64),
        ] {
            let error = Workspaces::configured(Ok(bad.to_string()))
                .expect_err(&format!("`{bad}` must refuse"));
            assert!(error.contains(WORKSPACES_ENV), "{error}");
        }
        let too_many: Vec<String> = (0..=MAX_WORKSPACES).map(|i| format!("w{i}")).collect();
        assert!(Workspaces::configured(Ok(too_many.join(","))).is_err());
        let most: Vec<String> = (0..MAX_WORKSPACES).map(|i| format!("w{i}")).collect();
        assert!(Workspaces::configured(Ok(most.join(","))).is_ok());
    }

    #[test]
    fn of_admits_a_listed_label() {
        let w = listed("dev,prd");
        assert_eq!(w.of(&job(Some("prd"))).unwrap().as_str(), "prd");
    }

    #[test]
    fn of_refuses_an_absent_or_unlisted_label() {
        let w = listed("dev,prd");
        let absent = w.of(&job(None)).unwrap_err();
        assert_eq!(absent, Refusal::Unknown { label: None });
        assert_eq!(absent.reason(), "WorkspaceUnknown");
        assert!(absent.to_string().contains(LABEL), "{absent}");

        let unlisted = w.of(&job(Some("qa"))).unwrap_err();
        assert_eq!(unlisted.reason(), "WorkspaceUnknown");
        assert!(unlisted.to_string().contains("`qa`"), "{unlisted}");
        // Exact: neither a prefix nor a case variant is the workspace.
        assert!(w.of(&job(Some("pr"))).is_err());
        assert!(w.of(&job(Some("PRD"))).is_err());
    }

    #[test]
    fn same_admits_a_reference_in_the_same_workspace() {
        assert_eq!(same(&job(Some("dev")), &secret(Some("dev"))), Ok(()));
    }

    #[test]
    fn same_refuses_a_reference_into_another_workspace_naming_both() {
        let refusal = same(&job(Some("prd")), &secret(Some("dev"))).unwrap_err();
        assert_eq!(refusal.reason(), "WorkspaceMismatch");
        assert_eq!(
            refusal.to_string(),
            "Secret `source` is in workspace `dev`; this Job is in workspace `prd`"
        );
    }

    #[test]
    fn same_never_matches_an_absent_label() {
        for (mine, theirs) in [(None, None), (Some("dev"), None), (None, Some("dev"))] {
            let refusal = same(&job(mine), &secret(theirs)).unwrap_err();
            assert_eq!(refusal.reason(), "WorkspaceMismatch", "{mine:?} {theirs:?}");
        }
        let refusal = same(&job(None), &secret(Some("dev"))).unwrap_err();
        assert!(refusal.to_string().ends_with("this Job is in no workspace"));
    }
}
