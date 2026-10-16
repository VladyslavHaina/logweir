//! Connections: `KafkaCluster` projections, and a typed create whose
//! credential is entered once, as a value.
//!
//! No route here reads a Secret; a credential is entered as a value once and
//! written to a Secret this service creates (see [`create`]).

use std::collections::BTreeMap;

use axum::body::Body;
use axum::extract::State;
use axum::response::Response;
use http::{HeaderMap, StatusCode, Uri};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::{Resource as _, ResourceExt as _};
use weirkeeper::crds::kafka_cluster::{
    AuthBlock, AuthMode, ClientCertificateRef, CredentialSecretRef, KafkaCluster, KafkaClusterSpec,
    ObjectKeyRef, RegistrySecretRef, SchemaRegistryBlock, TlsCaSource, UnrecognizedFields,
};
use weirkeeper::crds::preflight::Preflight as PreflightCr;

use super::{
    authorize, authorize_also, create_named_idempotent, get_object, is_own_replay, json, list_page,
    list_query, ApiPath, MAX_LIMIT,
};
use crate::app::AppState;
use crate::auth::Actor;
use crate::authz::Action;
use crate::contract::{
    ConnectionAuthMode, ConnectionList, ConnectionResponse, ConnectionRole,
    CreateConnectionRequest, LastTestView,
};
use crate::http::{read_json, RequestId, MAX_JSON_BODY};
use crate::idempotency::IdempotencyKey;
use crate::kube::{KubeFailure, PageRequest, WriteOnlyCredential};
use crate::problem::{ApiError, FieldError, ProblemCode};
use crate::projection;
use crate::validate;

/// The list route identifier (cursor scope).
pub const ROUTE_LIST: &str = "GET /api/v1/workspaces/{ws}/connections";
/// The create route identifier (idempotency scope).
pub const ROUTE_CREATE: &str = "POST /api/v1/workspaces/{ws}/connections";
/// The deterministic name prefix. With 26 hash characters the name is 31
/// characters, inside the 49 the probe Job name leaves a KafkaCluster.
pub const NAME_PREFIX: &str = "conn-";

/// The label a `SourceConnection` preflight carries, naming the connection it
/// dialled.
///
/// ITS OWN LABEL, FOR `DESTINATION_TEST_LABEL`'s REASON. A connectivity check
/// is the only object that carries it, so `last_test` below selects on it
/// alone and can never pick up a Backup readiness check that happens to name
/// the same source. The value is the connection's NAME, because that is what
/// the requester named and what a label selector can be built from without a
/// second read at create time; the UID check that makes it exact is done here,
/// against the object this route already holds.
pub const CONNECTION_TEST_LABEL: &str = "logweir.dev/connection-test";

/// How many pages of preflights [`last_test`] will read before it gives up and
/// says so. The same bound, for the same reason, as the destinations route's.
pub const MAX_PAGES: usize = 8;

/// `GET .../connections`.
pub async fn list(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    actor: Actor,
    ApiPath(ws): ApiPath<String>,
    uri: Uri,
) -> Result<Response, ApiError> {
    authorize(&state, &actor, &ws, Action::ReadConnections)?;
    let query = list_query(uri.query())?;
    let (items, page) = list_page::<KafkaCluster>(&state, &actor, &ws, ROUTE_LIST, &query).await?;
    Ok(json(
        StatusCode::OK,
        &ConnectionList {
            request_id,
            // NO `lastTest` ON A LIST, and `destinations` makes the same
            // choice: one detail read is one bounded label scan, and a page of
            // N connections would be N of them.
            items: items
                .iter()
                .map(|c| projection::connection(c, None))
                .collect(),
            page,
        },
    ))
}

/// The newest `SourceConnection` preflight for one connection.
///
/// ONE SELECTOR, PAGED TO EXHAUSTION, BOUNDED — `destinations::last_test`'s
/// shape, because it answers the same question about the other half of a run.
/// [`CONNECTION_TEST_LABEL`] is set only by a connectivity check, the loop
/// follows the continue token so a workspace with many checks cannot hide the
/// newest one behind a first page in name order, and [`MAX_PAGES`] caps the
/// work with the cap REPORTED rather than swallowed.
///
/// # A NAME IS NOT AN IDENTITY, AND THIS IS WHERE THAT IS ENFORCED
///
/// The label carries the connection's NAME, so a `KafkaCluster` deleted and
/// recreated under that name would inherit its predecessor's checks — the
/// exact substitution the console refuses everywhere else (a recreated cluster
/// is a different set of brokers reached with a different credential). Every
/// candidate is therefore matched against the UID of the object this route
/// already read: a `Preflight` records every referent it resolved, with its
/// UID, in `status.binding.referents`, so the comparison needs no extra read
/// and no create-time resolution. A check whose binding names another UID —
/// or names none, because it never got far enough to resolve one — is not this
/// connection's last test and is skipped.
async fn last_test(
    state: &AppState,
    workspace: &str,
    name: &str,
    cluster_uid: &str,
) -> Result<Option<LastTestView>, ApiError> {
    if cluster_uid.is_empty() {
        return Ok(None);
    }
    let selector = format!("{CONNECTION_TEST_LABEL}={name}");
    let mut continue_token = None;
    let mut newest: Option<PreflightCr> = None;
    let mut truncated = true;
    for _ in 0..MAX_PAGES {
        let page = PageRequest {
            limit: MAX_LIMIT,
            continue_token: continue_token.clone(),
            label_selector: Some(selector.clone()),
        };
        let list = state
            .kube()
            .list::<PreflightCr>(workspace, &page)
            .await
            .map_err(KubeFailure::into_api_error)?;
        for item in list.items {
            if !binds_cluster(&item, cluster_uid) {
                continue;
            }
            let newer = newest.as_ref().is_none_or(|current| {
                item.meta().creation_timestamp > current.meta().creation_timestamp
            });
            if newer {
                newest = Some(item);
            }
        }
        continue_token = list.metadata.continue_.filter(|token| !token.is_empty());
        if continue_token.is_none() {
            truncated = false;
            break;
        }
    }
    let now = state.now();
    let Some(newest) = newest else {
        return Ok(None);
    };
    let live = super::preflights::read_live_binding(state, workspace, &newest).await;
    let projected = super::preflights::project(&newest, now, None, Some(&live));
    Ok(Some(LastTestView {
        preflight_id: projected.id.clone(),
        state: projected.state,
        observed_at: projected.observed_at,
        stale: projected.stale,
        truncated,
    }))
}

/// Whether a preflight's recorded binding names THIS `KafkaCluster` by UID.
fn binds_cluster(preflight: &PreflightCr, cluster_uid: &str) -> bool {
    preflight
        .status
        .as_ref()
        .and_then(|s| s.binding.as_ref())
        .and_then(|b| b.referents.as_ref())
        .is_some_and(|referents| {
            referents
                .iter()
                .any(|r| r.kind == "KafkaCluster" && r.uid.as_deref() == Some(cluster_uid))
        })
}

/// `GET .../connections/{name}`.
pub async fn get_one(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    actor: Actor,
    ApiPath((ws, name)): ApiPath<(String, String)>,
    uri: Uri,
) -> Result<Response, ApiError> {
    authorize(&state, &actor, &ws, Action::ReadConnections)?;
    crate::http::parse_query(uri.query(), &[])?;
    let object = get_object::<KafkaCluster>(&state, &actor, &ws, &name).await?;
    let test = last_test(&state, &ws, &name, &object.uid().unwrap_or_default()).await?;
    Ok(json(
        StatusCode::OK,
        &ConnectionResponse {
            request_id,
            replayed: None,
            item: projection::connection(&object, test),
        },
    ))
}

/// Validate a create request.
///
/// # Errors
///
/// `validation_failed` naming every invalid field.
pub fn validate_create(request: &CreateConnectionRequest) -> Result<(), ApiError> {
    let mut errors = Vec::new();
    if request.bootstrap_servers.is_empty() || request.bootstrap_servers.len() > 16 {
        errors.push(FieldError::new(
            "bootstrapServers",
            "count_out_of_range",
            "between 1 and 16 bootstrap addresses are required",
        ));
    }
    let mut seen = std::collections::BTreeSet::new();
    for (i, server) in request.bootstrap_servers.iter().enumerate() {
        if let Err(code) = validate::check_bootstrap_server(server) {
            errors.push(FieldError::new(
                format!("bootstrapServers[{i}]"),
                code,
                "must be host:port with no scheme or userinfo",
            ));
        } else if !seen.insert(server.as_str()) {
            errors.push(FieldError::new(
                format!("bootstrapServers[{i}]"),
                "duplicate",
                "each address may appear once",
            ));
        }
    }
    validate_auth(&request.auth, &mut errors);
    if let Some(registry) = &request.schema_registry {
        validate_registry(registry, &mut errors);
    }
    if let Some(topic) = &request.marker_topic {
        if !validate::is_topic_name(topic) {
            errors.push(FieldError::new(
                "markerTopic",
                "invalid_topic",
                "must be a Kafka topic name",
            ));
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(ApiError::validation(errors))
    }
}

/// The registry block's rules (PROD-03.1): a registry URL, and a username
/// exactly when a password is entered.
fn validate_registry(
    registry: &crate::contract::SchemaRegistryRequest,
    errors: &mut Vec<FieldError>,
) {
    if !weirkeeper::connection::is_registry_url(&registry.url) {
        errors.push(FieldError::new(
            "schemaRegistry.url",
            "invalid_url",
            "must be an http:// or https:// URL with no userinfo, query or fragment",
        ));
    }
    match (&registry.username, &registry.password) {
        (Some(username), Some(password)) => {
            if let Err(code) = validate::check_single_line(username, 256) {
                errors.push(FieldError::new(
                    "schemaRegistry.username",
                    code,
                    "must be one printable line",
                ));
            }
            if let Err(e) = weirkeeper::connection::credential::check_password(password) {
                errors.push(FieldError::new(
                    "schemaRegistry.password",
                    "invalid_credential",
                    e.to_string(),
                ));
            }
        }
        (None, None) => {}
        _ => errors.push(FieldError::new(
            "schemaRegistry",
            "username_and_password",
            "a registry username and password are entered together (HTTP basic auth) or not at \
             all",
        )),
    }
}

/// Whether the request carries a registry password VALUE.
fn registry_password(request: &CreateConnectionRequest) -> Option<&String> {
    request
        .schema_registry
        .as_ref()
        .and_then(|r| r.password.as_ref())
}

/// The auth block's rules, every mode.
///
/// The SAME shapes the controller's resolver and the CRD's admission rules
/// refuse, answered here as field errors so the console can name the field
/// before anything is created: a SASL mode needs a username and a password,
/// `plain` and `mtls` need `tls: true` (PLAIN without TLS is the named reason
/// `PlainWithoutTls`), `mtls` needs a client certificate and key, and a CA
/// needs TLS.
///
/// **THE CREDENTIAL IS TYPED, NEVER NAMED.**
/// `auth.credentialRef` is refused for every mode: a connection that could name
/// an existing Secret could make the runner present a credential its author
/// could not read to a broker its author chose. Every refusal below names a
/// field and a rule, and never a value.
fn validate_auth(auth: &crate::contract::ConnectionAuthRequest, errors: &mut Vec<FieldError>) {
    use weirkeeper::connection::credential;
    let mode = match auth.mode {
        ConnectionAuthMode::Plaintext => "plaintext",
        ConnectionAuthMode::ScramSha512 => "scramSha512",
        ConnectionAuthMode::ScramSha256 => "scramSha256",
        ConnectionAuthMode::Plain => "plain",
        ConnectionAuthMode::Mtls => "mtls",
    };
    if auth.credential_ref.is_some() {
        errors.push(FieldError::new(
            "auth.credentialRef",
            "existing_credential_refused",
            "a connection no longer names an existing Secret: a connection that could name any \
             Secret could make Logweir present a credential its author cannot read to brokers \
             its author chose. Enter the credential once in auth.credential; it becomes a Secret \
             owned by and bound to this connection",
        ));
    }
    let sasl = matches!(
        auth.mode,
        ConnectionAuthMode::ScramSha512
            | ConnectionAuthMode::ScramSha256
            | ConnectionAuthMode::Plain
    );
    let credential = auth.credential.as_ref();
    if sasl {
        match &auth.username {
            None => errors.push(FieldError::new(
                "auth.username",
                "required",
                format!("{mode} requires the SASL username"),
            )),
            Some(username) => {
                if let Err(code) = validate::check_single_line(username, 256) {
                    errors.push(FieldError::new(
                        "auth.username",
                        code,
                        "must be one printable line",
                    ));
                }
            }
        }
        match credential.and_then(|c| c.password.as_deref()) {
            None => errors.push(FieldError::new(
                "auth.credential.password",
                "required",
                format!("{mode} requires the SASL password, entered once"),
            )),
            Some(password) => {
                if let Err(e) = credential::check_password(password) {
                    errors.push(FieldError::new(
                        "auth.credential.password",
                        "invalid_credential",
                        e.to_string(),
                    ));
                }
            }
        }
        if credential.is_some_and(|c| c.certificate_pem.is_some() || c.private_key_pem.is_some()) {
            errors.push(FieldError::new(
                "auth.credential",
                "not_allowed",
                format!("{mode} takes a password, not a client certificate"),
            ));
        }
    } else {
        let takes = match auth.mode {
            ConnectionAuthMode::Mtls => "mtls authenticates with a client certificate and takes",
            _ => "plaintext authentication takes",
        };
        if auth.username.is_some() {
            errors.push(FieldError::new(
                "auth.username",
                "not_allowed",
                format!("{takes} no username"),
            ));
        }
        if credential.is_some_and(|c| c.password.is_some()) {
            errors.push(FieldError::new(
                "auth.credential.password",
                "not_allowed",
                format!("{takes} no password"),
            ));
        }
    }
    // SASL/PLAIN only over TLS -- the named reason.
    if auth.mode == ConnectionAuthMode::Plain && !auth.tls {
        errors.push(FieldError::new(
            "auth.tls",
            "plain_requires_tls",
            "PlainWithoutTls: SASL/PLAIN sends the password itself, so it is accepted only with \
             tls: true",
        ));
    }
    if auth.mode == ConnectionAuthMode::Mtls {
        if !auth.tls {
            errors.push(FieldError::new(
                "auth.tls",
                "mtls_requires_tls",
                "mtls presents a TLS client certificate, so it requires tls: true",
            ));
        }
        match credential.map(|c| (c.certificate_pem.as_deref(), c.private_key_pem.as_deref())) {
            Some((Some(cert), Some(key))) => {
                if let Err(e) = credential::check_client_certificate(cert, key) {
                    errors.push(FieldError::new(
                        "auth.credential",
                        "invalid_credential",
                        e.to_string(),
                    ));
                }
            }
            _ => errors.push(FieldError::new(
                "auth.credential",
                "required",
                "mtls requires the client certificate and its unencrypted private key, entered \
                 once (certificatePem, privateKeyPem)",
            )),
        }
    } else if !sasl
        && credential.is_some_and(|c| c.certificate_pem.is_some() || c.private_key_pem.is_some())
    {
        errors.push(FieldError::new(
            "auth.credential",
            "not_allowed",
            "only mtls presents a client certificate",
        ));
    }
    if let Some(ca) = &auth.tls_ca {
        if !auth.tls {
            errors.push(FieldError::new(
                "auth.tlsCa",
                "requires_tls",
                "a CA verifies a TLS transport, so it requires tls: true",
            ));
        }
        let r = &ca.config_map_key_ref;
        if !validate::is_dns_subdomain(&r.name) {
            errors.push(FieldError::new(
                "auth.tlsCa.configMapKeyRef.name",
                "invalid_name",
                "must be a Kubernetes object name",
            ));
        }
        if !weirkeeper::connection::is_data_key(&r.key) {
            errors.push(FieldError::new(
                "auth.tlsCa.configMapKeyRef.key",
                "invalid_key",
                "must be a data key ([-._a-zA-Z0-9]+)",
            ));
        }
    }
}

/// The deterministic name of the Secret a connection's entered credential
/// becomes: the connection's own name and a fixed suffix, so the
/// `KafkaCluster` (whose spec is immutable) can name it before it exists, and
/// the name says whose it is.
#[must_use]
pub fn credential_secret_name(connection: &str) -> String {
    format!("{connection}-credential")
}

/// Build the stored object. A credential is NAMED here by the deterministic
/// Secret name this route creates right after the object; it is never a
/// Secret the request chose.
#[must_use]
pub fn build(
    name: String,
    annotations: BTreeMap<String, String>,
    request: &CreateConnectionRequest,
) -> KafkaCluster {
    let secret = credential_secret_name(&name);
    let sasl = matches!(
        request.auth.mode,
        ConnectionAuthMode::ScramSha512
            | ConnectionAuthMode::ScramSha256
            | ConnectionAuthMode::Plain
    );
    KafkaCluster {
        metadata: ObjectMeta {
            name: Some(name),
            annotations: Some(annotations),
            ..ObjectMeta::default()
        },
        spec: KafkaClusterSpec {
            bootstrap_servers: request.bootstrap_servers.clone(),
            auth: AuthBlock {
                mode: match request.auth.mode {
                    ConnectionAuthMode::Plaintext => AuthMode::Plaintext,
                    ConnectionAuthMode::ScramSha512 => AuthMode::ScramSha512,
                    ConnectionAuthMode::ScramSha256 => AuthMode::ScramSha256,
                    ConnectionAuthMode::Plain => AuthMode::Plain,
                    ConnectionAuthMode::Mtls => AuthMode::Mtls,
                },
                username: request.auth.username.clone(),
                // THE CREDENTIAL THIS ROUTE CREATES, BY ITS DETERMINISTIC NAME,
                // under the default `password` key (`passwordKey` is not offered: absent is
                // `password`).
                secret_ref: sasl.then(|| CredentialSecretRef {
                    name: secret.clone(),
                    password_key: None,
                    unrecognized_fields: UnrecognizedFields::default(),
                }),
                tls: request.auth.tls,
                tls_ca: request.auth.tls_ca.as_ref().map(|ca| TlsCaSource {
                    secret_key_ref: None,
                    config_map_key_ref: Some(ObjectKeyRef {
                        name: ca.config_map_key_ref.name.clone(),
                        key: ca.config_map_key_ref.key.clone(),
                        unrecognized_fields: UnrecognizedFields::default(),
                    }),
                    unrecognized_fields: UnrecognizedFields::default(),
                }),
                // The `mtls` Secret this route creates, at the
                // `kubernetes.io/tls` default keys (`tls.crt`, `tls.key`).
                client_certificate: (request.auth.mode == ConnectionAuthMode::Mtls).then(|| {
                    ClientCertificateRef {
                        name: secret.clone(),
                        certificate_key: None,
                        private_key_key: None,
                        unrecognized_fields: UnrecognizedFields::default(),
                    }
                }),
                unrecognized_fields: UnrecognizedFields::default(),
            },
            role: match request.role {
                ConnectionRole::Source => "source".to_string(),
                ConnectionRole::Target => "target".to_string(),
            },
            marker_topic: request.marker_topic.clone(),
            // PROD-05.1a's Strimzi reference is set on the object, never by
            // this route.
            strimzi: None,
            // The registry, its password in THIS route's Secret, by name.
            schema_registry: request
                .schema_registry
                .as_ref()
                .map(|r| SchemaRegistryBlock {
                    url: r.url.clone(),
                    username: r.username.clone(),
                    secret_ref: r.password.is_some().then(|| RegistrySecretRef {
                        name: secret.clone(),
                        password_key: None,
                        unrecognized_fields: UnrecognizedFields::default(),
                    }),
                    unrecognized_fields: UnrecognizedFields::default(),
                }),
            unrecognized_fields: UnrecognizedFields::default(),
        },
        status: None,
    }
}

/// The credential Secret for a created connection: built by the ONE
/// write-only contract (`weirkeeper::connection::credential`) with the
/// connection's BINDING, owned by the connection, and converted to the
/// create-only shape this service writes.
///
/// # Errors
///
/// `internal_error` when the stored object does not resolve (it was just
/// built from a validated request, so that is a defect) or the contract
/// refuses a value validation already accepted.
fn credential_secret(
    created: &KafkaCluster,
    credential: Option<&crate::contract::NewConnectionCredentialRequest>,
    registry_password: Option<&String>,
    request_id: &str,
) -> Result<WriteOnlyCredential, ApiError> {
    use weirkeeper::connection::credential;
    let internal = |what: &str| {
        ApiError::new(
            ProblemCode::InternalError,
            format!("The credential could not be prepared: {what}. Nothing was written to it."),
        )
    };
    let resolved =
        weirkeeper::connection::resolve(created, weirkeeper::connection::ConnectionUse::Probe)
            .map_err(|r| internal(&r.to_string()))?;
    let binding = resolved
        .credential_binding()
        .ok_or_else(|| internal("the connection has no UID to bind the credential to"))?;
    let name = created.metadata.name.clone().unwrap_or_default();
    let uid = created.metadata.uid.clone().unwrap_or_default();
    let namespace = created.metadata.namespace.as_deref().unwrap_or_default();
    let secret_name = credential_secret_name(&name);
    let request_id = (!request_id.is_empty() && request_id.len() <= 128).then_some(request_id);
    let built = match (
        credential.and_then(|c| c.password.as_ref()),
        credential.and_then(|c| c.certificate_pem.as_ref()),
        credential.and_then(|c| c.private_key_pem.as_ref()),
    ) {
        // PROD-03.1: a connection whose only credential is its registry's.
        (None, None, None) => match registry_password {
            Some(password) => {
                credential::build_registry_credential_secret(credential::NewKafkaCredential {
                    namespace,
                    secret_name: &secret_name,
                    connection_name: &name,
                    password: credential::WriteOnlyPassword::new(password.clone()),
                    request_id,
                    binding: Some(&binding),
                    owner_uid: Some(&uid),
                })
            }
            None => return Err(internal("the request carries no credential")),
        },
        (Some(password), None, None) => {
            credential::build_kafka_credential_secret(credential::NewKafkaCredential {
                namespace,
                secret_name: &secret_name,
                connection_name: &name,
                password: credential::WriteOnlyPassword::new(password.clone()),
                request_id,
                binding: Some(&binding),
                owner_uid: Some(&uid),
            })
        }
        (None, Some(cert), Some(key)) => credential::build_kafka_client_certificate_secret(
            credential::NewKafkaClientCertificate {
                namespace,
                secret_name: &secret_name,
                connection_name: &name,
                certificate: credential::WriteOnlyClientCertificate::new(cert.clone(), key.clone()),
                request_id,
                binding: Some(&binding),
                owner_uid: Some(&uid),
            },
        ),
        _ => return Err(internal("the request carries no single credential")),
    }
    .and_then(|built| match registry_password {
        // The registry password joins the Kafka credential in the
        // connection's one Secret.
        Some(password) if credential.is_some() => {
            built.with_registry_password(credential::WriteOnlyPassword::new(password.clone()))
        }
        _ => Ok(built),
    })
    .map_err(|e| internal(&e.to_string()))?;
    Ok(WriteOnlyCredential::from_parts(built.into_parts()))
}

/// The name-only dry-run probe for the credential Secret.
fn probe_secret(name: &str, type_: &str) -> WriteOnlyCredential {
    WriteOnlyCredential {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            labels: Some(BTreeMap::from([(
                weirkeeper::connection::credential::MANAGED_BY_LABEL.to_string(),
                weirkeeper::connection::credential::MANAGED_BY_VALUE.to_string(),
            )])),
            ..ObjectMeta::default()
        },
        type_: Some(type_.to_string()),
        data: BTreeMap::new(),
    }
}

/// `POST .../connections`.
///
/// # The credential flow
///
/// A request that carries a credential VALUE needs `credential.write` too, and
/// is written in the order the destinations route established: the Secret's
/// deterministic name is checked FREE before anything is written (a dry-run
/// `create`, no read verb); the `KafkaCluster` is created naming it; then the
/// Secret is created with the connection's binding and an owner reference to
/// it. A retry of the same request (this service's own idempotency record,
/// never the Secret's existence) adopts a Secret its earlier attempt wrote; any
/// other existing Secret under the name is `state_conflict` and nothing is
/// written.
pub async fn create(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    actor: Actor,
    ApiPath(ws): ApiPath<String>,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, ApiError> {
    authorize(&state, &actor, &ws, Action::CreateConnection)?;
    crate::http::parse_query(uri.query(), &[])?;
    let key = IdempotencyKey::from_headers(&headers)?;
    let request: CreateConnectionRequest = read_json(body, MAX_JSON_BODY).await?;
    validate_create(&request)?;
    let credential = request
        .auth
        .credential
        .as_ref()
        .filter(|_| request.auth.carries_a_value());
    let registry_password = registry_password(&request);
    let writes_secret = credential.is_some() || registry_password.is_some();
    if writes_secret {
        authorize_also(&state, &actor, &ws, Action::WriteCredential)?;
    }
    // The name the object will have, decided before anything is written, so
    // the credential's name is known and can be checked free first.
    let canonical = serde_json::to_vec(&request).map_err(|_| {
        ApiError::new(
            ProblemCode::InternalError,
            "The request could not be hashed.",
        )
    })?;
    let name =
        crate::idempotency::identity(&actor, &ws, ROUTE_CREATE, NAME_PREFIX, &key, &canonical).name;
    let replaying =
        is_own_replay::<KafkaCluster, _>(&state, &actor, &ws, ROUTE_CREATE, &name, &key, &request)
            .await?;
    let secret_type = if request.auth.mode == ConnectionAuthMode::Mtls {
        weirkeeper::connection::credential::CLIENT_CERTIFICATE_SECRET_TYPE
    } else {
        weirkeeper::connection::credential::CREDENTIAL_SECRET_TYPE
    };
    let secret_name = credential_secret_name(&name);
    if writes_secret && !replaying {
        let probe = probe_secret(&secret_name, secret_type);
        if state
            .kube()
            .credential_name_is_taken(&probe)
            .await
            .map_err(KubeFailure::into_api_error)?
        {
            return Err(ApiError::new(
                ProblemCode::StateConflict,
                format!(
                    "`{secret_name}` already exists, so the credential you entered was NOT \
                     written and nothing was created. This service holds `create` on Secrets and \
                     nothing else: it cannot read or overwrite an existing Secret. Retry with a \
                     new Idempotency-Key."
                ),
            ));
        }
    }
    let created = create_named_idempotent(
        &state,
        &actor,
        &ws,
        ROUTE_CREATE,
        &name,
        &key,
        &request_id,
        &request,
        |name, annotations| build(name, annotations, &request),
    )
    .await?;
    if writes_secret {
        let secret =
            credential_secret(&created.object, credential, registry_password, &request_id)?;
        match state.kube().create_credential(&ws, &secret).await {
            Ok(reference) => {
                // THE LOG LINE IS A NAME — the only non-secret fact about a
                // credential is which object holds it.
                tracing::info!(
                    workspace = %ws,
                    secret = %reference.name,
                    connection = %name,
                    "connection credential secret created"
                );
                actor.audit.note("credential", &reference.name);
            }
            Err(KubeFailure::AlreadyExists) if replaying || created.replayed => {
                actor.audit.note("credential", &secret_name);
            }
            Err(KubeFailure::AlreadyExists) => {
                return Err(ApiError::new(
                    ProblemCode::StateConflict,
                    format!(
                        "`{secret_name}` was taken between this request's check and its write, \
                         so the credential you entered was NOT written. The connection `{name}` \
                         WAS created and carries no usable credential: delete it and retry with \
                         a new Idempotency-Key."
                    ),
                ))
            }
            Err(other) => return Err(other.into_api_error()),
        }
    }
    Ok(json(
        created.status(),
        &ConnectionResponse {
            request_id,
            replayed: Some(created.replayed),
            // A CONNECTION THAT HAS JUST BEEN CREATED HAS NO TEST.
            item: projection::connection(&created.object, None),
        },
    ))
}
