//! THE ONLY KUBERNETES ADAPTER.
//!
//! Every Kubernetes call this service makes is a method on [`KubeAdapter`],
//! and every method is typed over the closed [`ProductResource`] set —
//! `KafkaCluster`, `BackupSchedule`, `Backup`, `Restore`, `Approval`,
//! `BackupDestination`, `TopicDiscovery` and `Preflight`. There is no method
//! that takes a group, a version, a plural or a path, so no request can name a
//! Pod, a log, an exec stream, a Job or a core Namespace; there is no delete.
//!
//! TWO CORE OBJECTS ARE REACHED, EACH THROUGH ONE VERB AND ONE HAND-WRITTEN
//! TYPE. [`ResultDocument`] is a `configmaps` GET and nothing else: it is a
//! read-only projection with no writable `data` path, used only for the chunk
//! and detail documents a check owns, and every read is verified by owner UID,
//! immutability and digest by the route that asked for it (D2 §5.6).
//! [`WriteOnlyCredential`] is a `secrets` POST and nothing else: its `data`
//! field is `skip_deserializing`, so the API server's create response CANNOT
//! carry a credential value back into this process even in principle, and
//! there is no method on this adapter that reads, lists, updates or deletes a
//! Secret. `k8s_openapi`'s own `Secret` and `ConfigMap` types are deliberately
//! NOT imported: a type that can hold a Secret's data is a type that can leak
//! one.
//!
//! THE UPDATES ARE THREE MERGE PATCHES, EACH BUILT HERE FROM TYPED
//! ARGUMENTS: [`KubeAdapter::set_schedule_suspension`],
//! [`KubeAdapter::set_destination_access`] and
//! [`KubeAdapter::request_check_cancel`]. Each carries
//! `metadata.resourceVersion`, so each is a conditional write the API server
//! refuses on a stale read, and none of them accepts a caller-supplied path or
//! patch document.
//!
//! EVERY CALL HAS A 10-SECOND DEADLINE ([`KUBE_DEADLINE`]) enforced around the
//! whole call, and the client's own connect/read/write timeouts are set to the
//! same bound. A timeout is [`KubeFailure::Timeout`] and maps to 504.
//!
//! NO KUBERNETES MESSAGE LEAVES THIS MODULE UNREDACTED. [`KubeFailure`] carries
//! the status code and reason class only; the message is logged through
//! [`redact`] and then dropped.
//!
//! NO INBOUND HEADER IS FORWARDED. Requests are built by `kube::Api` from typed
//! parameters; nothing from the HTTP request reaches them but validated names.

use std::path::Path;
use std::time::Duration;

use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference};
use k8s_openapi::NamespaceResourceScope;
use kube::api::{Api, ListParams, ObjectList, Patch, PatchParams, PostParams};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use weirkeeper::crds::approval::Approval;
use weirkeeper::crds::backup::Backup;
use weirkeeper::crds::backup_destination::BackupDestination;
use weirkeeper::crds::backup_schedule::BackupSchedule;
use weirkeeper::crds::kafka_cluster::KafkaCluster;
use weirkeeper::crds::preflight::Preflight;
use weirkeeper::crds::restore::Restore;
use weirkeeper::crds::topic_discovery::TopicDiscovery;

use crate::config::KubeSource;
use crate::problem::{ApiError, ProblemCode};

/// The deadline on every Kubernetes call.
pub const KUBE_DEADLINE: Duration = Duration::from_secs(10);

/// The field manager recorded on every write.
pub const FIELD_MANAGER: &str = "logweir-api";

mod sealed {
    pub trait Sealed {}
    impl Sealed for weirkeeper::crds::kafka_cluster::KafkaCluster {}
    impl Sealed for weirkeeper::crds::backup_schedule::BackupSchedule {}
    impl Sealed for weirkeeper::crds::backup::Backup {}
    impl Sealed for weirkeeper::crds::restore::Restore {}
    impl Sealed for weirkeeper::crds::approval::Approval {}
    impl Sealed for weirkeeper::crds::backup_destination::BackupDestination {}
    impl Sealed for weirkeeper::crds::topic_discovery::TopicDiscovery {}
    impl Sealed for weirkeeper::crds::preflight::Preflight {}
}

/// The closed set of CUSTOM resources this service may touch. Sealed: no
/// other crate, and no other module, can add a ninth.
pub trait ProductResource:
    kube::Resource<DynamicType = (), Scope = NamespaceResourceScope>
    + Clone
    + DeserializeOwned
    + Serialize
    + std::fmt::Debug
    + Send
    + Sync
    + 'static
    + sealed::Sealed
{
}

impl ProductResource for KafkaCluster {}
impl ProductResource for BackupSchedule {}
impl ProductResource for Backup {}
impl ProductResource for Restore {}
impl ProductResource for Approval {}
impl ProductResource for BackupDestination {}
impl ProductResource for TopicDiscovery {}
impl ProductResource for Preflight {}

/// The two kinds whose `spec.cancelRequested` may be raised.
///
/// A SECOND SEAL INSIDE THE FIRST. `Backup` and `Restore` have no cancel in
/// v1 (D0: "external side effects and cleanup semantics are not yet
/// defined"), and a generic "patch any product resource" method would have
/// given them one by accident. Only a transient check — a discovery or a
/// preflight, both of which own nothing but a Job — implements this.
pub trait CancellableCheck: ProductResource {}

impl CancellableCheck for TopicDiscovery {}
impl CancellableCheck for Preflight {}

// ======================================================================
// The two core objects, each with one verb and no way to do more
// ======================================================================

/// A `ConfigMap` this service may GET, projected down to what a check result
/// needs.
///
/// READ-ONLY BY CONSTRUCTION. There is no create, patch or delete method for
/// it on [`KubeAdapter`], and `binary_data` is not a field here at all: a
/// check result is UTF-8 TSV or JSON lines, and a document that put its bytes
/// somewhere this type cannot see would fail its digest check rather than be
/// served half-read.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ResultDocument {
    /// Name, UID, annotations and owner references.
    #[serde(default)]
    pub metadata: ObjectMeta,
    /// Whether the API server has sealed the object. A check result that is
    /// not immutable is not a check result.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub immutable: Option<bool>,
    /// The string data. One key for a chunk, one for a details document.
    #[serde(default)]
    pub data: std::collections::BTreeMap<String, String>,
}

impl kube::Resource for ResultDocument {
    type DynamicType = ();
    type Scope = NamespaceResourceScope;

    fn kind(_: &()) -> std::borrow::Cow<'_, str> {
        "ConfigMap".into()
    }
    fn group(_: &()) -> std::borrow::Cow<'_, str> {
        "".into()
    }
    fn version(_: &()) -> std::borrow::Cow<'_, str> {
        "v1".into()
    }
    fn plural(_: &()) -> std::borrow::Cow<'_, str> {
        "configmaps".into()
    }
    fn meta(&self) -> &ObjectMeta {
        &self.metadata
    }
    fn meta_mut(&mut self) -> &mut ObjectMeta {
        &mut self.metadata
    }
}

impl ResultDocument {
    /// The UID of the controlling owner, when there is exactly one owner and
    /// it is the controller.
    #[must_use]
    pub fn controller_owner_uid(&self) -> Option<&str> {
        let owners = self.metadata.owner_references.as_deref()?;
        let [owner] = owners else { return None };
        (owner.controller == Some(true)).then_some(owner.uid.as_str())
    }

    /// One annotation's value.
    #[must_use]
    pub fn annotation(&self, key: &str) -> Option<&str> {
        self.metadata
            .annotations
            .as_ref()?
            .get(key)
            .map(String::as_str)
    }
}

/// A `Secret` this service may POST, and nothing else.
///
/// THE RESPONSE CANNOT CARRY THE VALUE BACK. `data` is
/// `#[serde(skip_deserializing)]`, so the API server's create response — which
/// echoes `data` — is parsed into a value whose `data` is empty. The bytes
/// exist in this process only between the request DTO and the outgoing body,
/// and [`CreatedCredential`] is all a route ever sees of the answer.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WriteOnlyCredential {
    /// Name, namespace, labels, annotations and the owner reference.
    #[serde(default)]
    pub metadata: ObjectMeta,
    /// The Secret `type`. A distinct type is what a ValidatingAdmissionPolicy
    /// scoped to this ServiceAccount can require, so the create permission
    /// cannot be spent on a ServiceAccount-token Secret (PLAT-07.1's rule).
    #[serde(rename = "type", default, skip_serializing_if = "Option::is_none")]
    pub type_: Option<String>,
    /// Base64 data keys. Written, never read.
    #[serde(default, skip_deserializing)]
    pub data: std::collections::BTreeMap<String, String>,
}

impl kube::Resource for WriteOnlyCredential {
    type DynamicType = ();
    type Scope = NamespaceResourceScope;

    fn kind(_: &()) -> std::borrow::Cow<'_, str> {
        "Secret".into()
    }
    fn group(_: &()) -> std::borrow::Cow<'_, str> {
        "".into()
    }
    fn version(_: &()) -> std::borrow::Cow<'_, str> {
        "v1".into()
    }
    fn plural(_: &()) -> std::borrow::Cow<'_, str> {
        "secrets".into()
    }
    fn meta(&self) -> &ObjectMeta {
        &self.metadata
    }
    fn meta_mut(&mut self) -> &mut ObjectMeta {
        &mut self.metadata
    }
}

/// The non-secret facts a route keeps from a credential create.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreatedCredential {
    /// `metadata.name` — the reference every response carries instead of a
    /// value.
    pub name: String,
    /// `metadata.uid`.
    pub uid: String,
}

/// An owner reference to an object this service created, for the Secrets a
/// destination owns.
#[must_use]
pub fn owner_reference(api_version: &str, kind: &str, name: &str, uid: &str) -> OwnerReference {
    OwnerReference {
        api_version: api_version.to_string(),
        kind: kind.to_string(),
        name: name.to_string(),
        uid: uid.to_string(),
        controller: Some(true),
        block_owner_deletion: Some(true),
    }
}

/// Why a Kubernetes call did not return the object.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KubeFailure {
    /// 404.
    NotFound,
    /// 409 with reason `AlreadyExists`.
    AlreadyExists,
    /// 409 for any other reason (a resourceVersion precondition).
    Conflict,
    /// 410 — an expired continue token.
    Gone,
    /// 422 — the object failed server-side validation.
    Invalid,
    /// 400 from the API server.
    BadRequest,
    /// 401 or 403 — this service's own identity was refused.
    Refused(u16),
    /// 429.
    TooManyRequests,
    /// The call exceeded [`KUBE_DEADLINE`].
    Timeout,
    /// A transport failure, an unparseable response or a 5xx.
    Unavailable,
}

impl KubeFailure {
    /// The problem for a failure that the calling route does not handle
    /// itself.
    #[must_use]
    pub fn into_api_error(self) -> ApiError {
        match self {
            KubeFailure::NotFound => ApiError::not_found(),
            KubeFailure::AlreadyExists | KubeFailure::Conflict => ApiError::new(
                ProblemCode::StateConflict,
                "The object changed concurrently; read it again and retry.",
            ),
            KubeFailure::Gone => ApiError::new(
                ProblemCode::CursorExpired,
                "The list snapshot expired; restart the list without a cursor.",
            ),
            KubeFailure::Invalid | KubeFailure::BadRequest => ApiError::new(
                ProblemCode::ValidationFailed,
                "Kubernetes rejected the object. The rejection details are recorded in the \
                 service log under this request ID.",
            ),
            KubeFailure::Refused(status) => ApiError::new(
                ProblemCode::KubernetesUnavailable,
                format!(
                    "Kubernetes refused this service's own identity (HTTP {status}); check the \
                     service's Kubernetes permissions."
                ),
            ),
            KubeFailure::TooManyRequests => {
                let mut e = ApiError::new(
                    ProblemCode::RateLimited,
                    "Kubernetes is throttling requests; retry after the indicated delay.",
                );
                e.retry_after_seconds = Some(1);
                e
            }
            KubeFailure::Timeout => ApiError::new(
                ProblemCode::UpstreamTimeout,
                "The Kubernetes call exceeded its 10-second deadline.",
            ),
            KubeFailure::Unavailable => ApiError::new(
                ProblemCode::KubernetesUnavailable,
                "Kubernetes could not be reached.",
            ),
        }
    }
}

/// One page request against a native Kubernetes list.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PageRequest {
    /// The page size.
    pub limit: u32,
    /// The Kubernetes continue token from the previous page.
    pub continue_token: Option<String>,
    /// An equality-only label selector, already validated.
    pub label_selector: Option<String>,
}

/// The adapter.
#[derive(Clone)]
pub struct KubeAdapter {
    client: kube::Client,
    deadline: Duration,
}

impl KubeAdapter {
    /// An adapter over a built client with the standard deadline.
    #[must_use]
    pub fn new(client: kube::Client) -> Self {
        Self {
            client,
            deadline: KUBE_DEADLINE,
        }
    }

    /// An adapter with a different deadline. Tests only shorten it.
    #[must_use]
    pub fn with_deadline(client: kube::Client, deadline: Duration) -> Self {
        Self { client, deadline }
    }

    /// One page of a namespaced list.
    ///
    /// # Errors
    ///
    /// [`KubeFailure`].
    pub async fn list<K: ProductResource>(
        &self,
        namespace: &str,
        page: &PageRequest,
    ) -> Result<ObjectList<K>, KubeFailure> {
        let api: Api<K> = Api::namespaced(self.client.clone(), namespace);
        let mut params = ListParams::default().limit(page.limit);
        if let Some(token) = &page.continue_token {
            params = params.continue_token(token);
        }
        if let Some(selector) = &page.label_selector {
            params = params.labels(selector);
        }
        // The verb label is `list_page` and not the bare word: that word is a
        // denied engine subcommand token, and `scripts/check-no-oso.sh`'s
        // secondary scan greps every `crates/**/*.rs` for it as a quoted
        // literal. Nothing in this crate invokes the engine; the label is a log
        // field, and spelling it this way keeps the gate meaningful instead of
        // adding an escape comment to it.
        self.bounded("list_page", K::plural(&()).as_ref(), api.list(&params))
            .await
    }

    /// One object by name.
    ///
    /// # Errors
    ///
    /// [`KubeFailure`], `NotFound` included.
    pub async fn get<K: ProductResource>(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<K, KubeFailure> {
        let api: Api<K> = Api::namespaced(self.client.clone(), namespace);
        self.bounded("get", K::plural(&()).as_ref(), api.get(name))
            .await
    }

    /// Create one object.
    ///
    /// # Errors
    ///
    /// [`KubeFailure`], `AlreadyExists` included.
    pub async fn create<K: ProductResource>(
        &self,
        namespace: &str,
        object: &K,
    ) -> Result<K, KubeFailure> {
        let api: Api<K> = Api::namespaced(self.client.clone(), namespace);
        let params = PostParams {
            dry_run: false,
            field_manager: Some(FIELD_MANAGER.to_string()),
        };
        self.bounded(
            "create",
            K::plural(&()).as_ref(),
            api.create(&params, object),
        )
        .await
    }

    /// The one permitted update: `BackupSchedule.spec.suspend`, under an
    /// optimistic-concurrency precondition.
    ///
    /// The merge patch carries `metadata.resourceVersion`, which makes the API
    /// server refuse it with 409 `Conflict` unless the stored object is still
    /// at that version. Its only other key is `spec.suspend`, and the CRD's
    /// object-level CEL seal refuses any other spec change regardless.
    ///
    /// # Errors
    ///
    /// [`KubeFailure`], `Conflict` for a stale version.
    pub async fn set_schedule_suspension(
        &self,
        namespace: &str,
        name: &str,
        suspend: bool,
        expected_resource_version: &str,
    ) -> Result<BackupSchedule, KubeFailure> {
        let api: Api<BackupSchedule> = Api::namespaced(self.client.clone(), namespace);
        let patch = serde_json::json!({
            "metadata": { "resourceVersion": expected_resource_version },
            "spec": { "suspend": suspend },
        });
        let params = PatchParams {
            field_manager: Some(FIELD_MANAGER.to_string()),
            ..PatchParams::default()
        };
        self.bounded(
            "patch",
            "backupschedules",
            api.patch(name, &params, &Patch::Merge(&patch)),
        )
        .await
    }

    /// Rotate a destination's credential references and CA bundle — the ONLY
    /// mutable half of a `BackupDestination` (D2 §3.1).
    ///
    /// `access` is the complete four-grant object, built by the route from a
    /// validated DTO, so a merge patch that omits a grant REMOVES it rather
    /// than leaving a stale one behind. `ca_bundle` is `Some(value)` to set,
    /// `Some(Value::Null)` to clear and `None` to leave alone. Nothing in the
    /// patch can name `spec.storage` or `spec.transport.security`: this method
    /// cannot build those keys, and the CRD's R1/R2 refuse them anyway.
    ///
    /// # Errors
    ///
    /// [`KubeFailure`], `Conflict` for a stale `resourceVersion`.
    pub async fn set_destination_access(
        &self,
        namespace: &str,
        name: &str,
        access: &serde_json::Value,
        ca_bundle: Option<serde_json::Value>,
        expected_resource_version: &str,
    ) -> Result<BackupDestination, KubeFailure> {
        let api: Api<BackupDestination> = Api::namespaced(self.client.clone(), namespace);
        let mut spec = serde_json::Map::new();
        spec.insert("access".to_string(), access.clone());
        if let Some(bundle) = ca_bundle {
            spec.insert(
                "transport".to_string(),
                serde_json::json!({ "caBundle": bundle }),
            );
        }
        let patch = serde_json::json!({
            "metadata": { "resourceVersion": expected_resource_version },
            "spec": serde_json::Value::Object(spec),
        });
        let params = PatchParams {
            field_manager: Some(FIELD_MANAGER.to_string()),
            ..PatchParams::default()
        };
        self.bounded(
            "patch",
            "backupdestinations",
            api.patch(name, &params, &Patch::Merge(&patch)),
        )
        .await
    }

    /// Ask one transient check to stop: `spec.cancelRequested: false -> true`,
    /// the single transition D0 permits, under a resourceVersion
    /// precondition.
    ///
    /// THE CONTROLLER STOPS THE JOB, NOT THIS SERVICE. This writes a wish on
    /// the object the caller already read and authorized; the controller
    /// verifies the exact owned Job and UID before stopping anything, and no
    /// archive byte, Kafka topic, durable run or signed evidence is touched
    /// either way.
    ///
    /// # Errors
    ///
    /// [`KubeFailure`], `Conflict` for a stale `resourceVersion`.
    pub async fn request_check_cancel<K: CancellableCheck>(
        &self,
        namespace: &str,
        name: &str,
        expected_resource_version: &str,
    ) -> Result<K, KubeFailure> {
        let api: Api<K> = Api::namespaced(self.client.clone(), namespace);
        let patch = serde_json::json!({
            "metadata": { "resourceVersion": expected_resource_version },
            "spec": { "cancelRequested": true },
        });
        let params = PatchParams {
            field_manager: Some(FIELD_MANAGER.to_string()),
            ..PatchParams::default()
        };
        self.bounded(
            "patch",
            K::plural(&()).as_ref(),
            api.patch(name, &params, &Patch::Merge(&patch)),
        )
        .await
    }

    /// Read one stored check result document by name.
    ///
    /// NAMED, NEVER LISTED. The name comes from `status.result.chunks[i].name`
    /// or `status.result.detailsRef.name` of an object the caller already
    /// authorized, so this can only reach documents a check wrote; D2 §5.6's
    /// "never list all `ConfigMap`s" is a property of there being no list
    /// method for this type at all.
    ///
    /// # Errors
    ///
    /// [`KubeFailure`], `NotFound` when the owner was collected.
    pub async fn get_result_document(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<ResultDocument, KubeFailure> {
        let api: Api<ResultDocument> = Api::namespaced(self.client.clone(), namespace);
        self.bounded("get", "configmaps", api.get(name)).await
    }

    /// Create one write-only credential Secret and keep only its identity.
    ///
    /// THE ANSWER IS TWO STRINGS. The API server echoes `data` on a create;
    /// [`WriteOnlyCredential`]'s `data` is `skip_deserializing`, so the echo is
    /// dropped by the parser, and this method narrows what is left to a name
    /// and a UID before any caller sees it.
    ///
    /// # Errors
    ///
    /// [`KubeFailure`], `AlreadyExists` when the name is taken — which is
    /// never resolved by reading the existing Secret, because this service has
    /// no verb that could.
    pub async fn create_credential(
        &self,
        namespace: &str,
        secret: &WriteOnlyCredential,
    ) -> Result<CreatedCredential, KubeFailure> {
        let api: Api<WriteOnlyCredential> = Api::namespaced(self.client.clone(), namespace);
        let params = PostParams {
            dry_run: false,
            field_manager: Some(FIELD_MANAGER.to_string()),
        };
        let created = self
            .bounded("create", "secrets", api.create(&params, secret))
            .await?;
        Ok(CreatedCredential {
            name: created.metadata.name.unwrap_or_default(),
            uid: created.metadata.uid.unwrap_or_default(),
        })
    }

    /// Readiness: the API server answers and `kafkaclusters` can be listed in
    /// `namespace` with this service's identity.
    ///
    /// # Errors
    ///
    /// [`KubeFailure`].
    pub async fn probe(&self, namespace: &str) -> Result<(), KubeFailure> {
        self.bounded("version", "version", self.client.apiserver_version())
            .await?;
        let page = PageRequest {
            limit: 1,
            ..PageRequest::default()
        };
        self.list::<KafkaCluster>(namespace, &page)
            .await
            .map(|_| ())
    }

    async fn bounded<T>(
        &self,
        verb: &'static str,
        resource: &str,
        call: impl std::future::Future<Output = Result<T, kube::Error>>,
    ) -> Result<T, KubeFailure> {
        match tokio::time::timeout(self.deadline, call).await {
            Err(_) => {
                tracing::warn!(verb, resource, "kubernetes call exceeded its deadline");
                Err(KubeFailure::Timeout)
            }
            Ok(Ok(value)) => Ok(value),
            Ok(Err(error)) => Err(classify(verb, resource, &error)),
        }
    }
}

fn classify(verb: &'static str, resource: &str, error: &kube::Error) -> KubeFailure {
    match error {
        kube::Error::Api(response) => {
            let failure = match (response.code, response.reason.as_str()) {
                (404, _) => KubeFailure::NotFound,
                (409, "AlreadyExists") => KubeFailure::AlreadyExists,
                (409, _) => KubeFailure::Conflict,
                (410, _) => KubeFailure::Gone,
                (422, _) => KubeFailure::Invalid,
                (400, _) => KubeFailure::BadRequest,
                (401 | 403, _) => KubeFailure::Refused(response.code),
                (429, _) => KubeFailure::TooManyRequests,
                _ => KubeFailure::Unavailable,
            };
            if !matches!(failure, KubeFailure::NotFound | KubeFailure::AlreadyExists) {
                tracing::warn!(
                    verb,
                    resource,
                    code = response.code,
                    reason = %redact(&response.reason),
                    message = %redact(&response.message),
                    "kubernetes refused the call"
                );
            }
            failure
        }
        other => {
            tracing::warn!(
                verb,
                resource,
                error = %redact(&other.to_string()),
                "kubernetes call failed"
            );
            KubeFailure::Unavailable
        }
    }
}

/// Redact a Kubernetes message for the log: bearer tokens, URL userinfo and
/// anything past 512 bytes are removed.
#[must_use]
pub fn redact(message: &str) -> String {
    let mut out = String::with_capacity(message.len().min(600));
    for word in message.split_inclusive(char::is_whitespace) {
        let trimmed = word.trim_end();
        let lower = trimmed.to_ascii_lowercase();
        if lower.starts_with("eyj") && trimmed.len() > 24 {
            // A JWT-shaped token.
            out.push_str("[redacted-token]");
            out.push_str(&word[trimmed.len()..]);
        } else if trimmed.contains("://") && trimmed.contains('@') {
            out.push_str(&crate::validate::redact_url_userinfo(trimmed));
            out.push_str(&word[trimmed.len()..]);
        } else {
            out.push_str(word);
        }
    }
    let lower = out.to_ascii_lowercase();
    if let Some(at) = lower.find("bearer ") {
        out.truncate(at);
        out.push_str("bearer [redacted]");
    }
    crate::validate::bounded(&out, 512)
}

/// Build a Kubernetes client from a validated source, with the deadline
/// applied to connect, read and write, and refuse a kubeconfig user that
/// impersonates.
///
/// No request is sent: a client is not a connection.
///
/// # Errors
///
/// A human-readable reason naming the source.
pub async fn build_client(source: &KubeSource) -> Result<kube::Client, String> {
    // BEFORE any client: the workspace graph enables two rustls providers, and
    // with two installed-by-feature providers rustls picks none and panics.
    // weirkeeper's installer is the one the controller binary uses.
    let _ = weirkeeper::install_default_crypto_provider();
    let mut config = match source {
        KubeSource::InCluster => kube::Config::incluster()
            .map_err(|e| format!("the in-cluster Kubernetes environment is not usable: {e}"))?,
        KubeSource::Kubeconfig { path, context } => {
            let kubeconfig = match path {
                Some(p) => read_kubeconfig(p)?,
                None => kube::config::Kubeconfig::read()
                    .map_err(|e| format!("cannot read the kubeconfig: {e}"))?,
            };
            refuse_impersonation(&kubeconfig, context)?;
            let options = kube::config::KubeConfigOptions {
                context: Some(context.clone()),
                cluster: None,
                user: None,
            };
            kube::Config::from_custom_kubeconfig(kubeconfig, &options)
                .await
                .map_err(|e| format!("cannot load kubeconfig context `{context}`: {e}"))?
        }
    };
    config.connect_timeout = Some(KUBE_DEADLINE);
    config.read_timeout = Some(KUBE_DEADLINE);
    config.write_timeout = Some(KUBE_DEADLINE);
    config.headers.clear();
    kube::Client::try_from(config).map_err(|e| format!("cannot build the Kubernetes client: {e}"))
}

fn read_kubeconfig(path: &Path) -> Result<kube::config::Kubeconfig, String> {
    kube::config::Kubeconfig::read_from(path)
        .map_err(|e| format!("cannot read the kubeconfig {}: {e}", path.display()))
}

/// Refuse a context whose user entry impersonates another identity: this
/// service sends no `Impersonate-*` header under any configuration.
///
/// # Errors
///
/// A reason naming the context.
pub fn refuse_impersonation(
    kubeconfig: &kube::config::Kubeconfig,
    context: &str,
) -> Result<(), String> {
    let Some(named) = kubeconfig.contexts.iter().find(|c| c.name == context) else {
        return Err(format!("the kubeconfig has no context named `{context}`"));
    };
    let Some(user) = named.context.as_ref().and_then(|c| c.user.as_ref()) else {
        return Ok(());
    };
    let impersonates = kubeconfig
        .auth_infos
        .iter()
        .filter(|a| &a.name == user)
        .filter_map(|a| a.auth_info.as_ref())
        .any(|a| {
            a.impersonate.is_some()
                || a.impersonate_groups
                    .as_ref()
                    .is_some_and(|groups| !groups.is_empty())
        });
    if impersonates {
        return Err(format!(
            "kubeconfig context `{context}` uses a user that impersonates another identity; \
             logweir-api never sends Impersonate-* headers"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redaction_removes_tokens_and_userinfo_and_bounds_length() {
        let jwt = format!("eyJ{}", "a".repeat(40));
        let r = redact(&format!("denied {jwt} at s3://k:s@bucket/x Bearer abc"));
        assert!(!r.contains(&jwt));
        assert!(!r.contains("k:s@"));
        assert!(!r.contains("abc"));
        assert!(redact(&"x".repeat(2000)).len() <= 520);
    }
}
