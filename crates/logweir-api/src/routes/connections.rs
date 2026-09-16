//! Connections: `KafkaCluster` projections, and a typed create that
//! references an existing credential Secret by name.
//!
//! No route here reads a Secret, and the create accepts no credential
//! material: write-only credential input is PLAT-07.1 and advertised `false`.

use std::collections::BTreeMap;

use axum::body::Body;
use axum::extract::State;
use axum::response::Response;
use http::{HeaderMap, StatusCode, Uri};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use weirkeeper::crds::kafka_cluster::{
    AuthBlock, AuthMode, CredentialSecretRef, KafkaCluster, KafkaClusterSpec, UnrecognizedFields,
};

use super::{authorize, create_idempotent, get_object, json, list_page, list_query, ApiPath};
use crate::app::AppState;
use crate::auth::Actor;
use crate::authz::Action;
use crate::contract::{
    ConnectionAuthMode, ConnectionList, ConnectionResponse, ConnectionRole, CreateConnectionRequest,
};
use crate::http::{read_json, RequestId, MAX_JSON_BODY};
use crate::idempotency::IdempotencyKey;
use crate::problem::{ApiError, FieldError};
use crate::projection;
use crate::validate;

/// The list route identifier (cursor scope).
pub const ROUTE_LIST: &str = "GET /api/v1/namespaces/{ns}/connections";
/// The create route identifier (idempotency scope).
pub const ROUTE_CREATE: &str = "POST /api/v1/namespaces/{ns}/connections";
/// The deterministic name prefix. With 26 hash characters the name is 31
/// characters, inside the 49 the probe Job name leaves a KafkaCluster.
pub const NAME_PREFIX: &str = "conn-";

/// `GET .../connections`.
pub async fn list(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    actor: Actor,
    ApiPath(ns): ApiPath<String>,
    uri: Uri,
) -> Result<Response, ApiError> {
    authorize(&state, &actor, &ns, Action::ReadConnections)?;
    let query = list_query(uri.query())?;
    let (items, page) = list_page::<KafkaCluster>(&state, &actor, &ns, ROUTE_LIST, &query).await?;
    Ok(json(
        StatusCode::OK,
        &ConnectionList {
            request_id,
            items: items.iter().map(projection::connection).collect(),
            page,
        },
    ))
}

/// `GET .../connections/{name}`.
pub async fn get_one(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    actor: Actor,
    ApiPath((ns, name)): ApiPath<(String, String)>,
    uri: Uri,
) -> Result<Response, ApiError> {
    authorize(&state, &actor, &ns, Action::ReadConnections)?;
    crate::http::parse_query(uri.query(), &[])?;
    let object = get_object::<KafkaCluster>(&state, &actor, &ns, &name).await?;
    Ok(json(
        StatusCode::OK,
        &ConnectionResponse {
            request_id,
            replayed: None,
            item: projection::connection(&object),
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
    match request.auth.mode {
        ConnectionAuthMode::ScramSha512 => {
            match &request.auth.username {
                None => errors.push(FieldError::new(
                    "auth.username",
                    "required",
                    "scramSha512 requires the SASL username",
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
            match &request.auth.credential_ref {
                None => errors.push(FieldError::new(
                    "auth.credentialRef",
                    "required",
                    "scramSha512 requires the name of an existing credential Secret",
                )),
                Some(r) if !validate::is_dns_subdomain(&r.name) => errors.push(FieldError::new(
                    "auth.credentialRef.name",
                    "invalid_name",
                    "must be a Kubernetes object name",
                )),
                Some(_) => {}
            }
        }
        ConnectionAuthMode::Plaintext => {
            if request.auth.username.is_some() {
                errors.push(FieldError::new(
                    "auth.username",
                    "not_allowed",
                    "plaintext authentication takes no username",
                ));
            }
            if request.auth.credential_ref.is_some() {
                errors.push(FieldError::new(
                    "auth.credentialRef",
                    "not_allowed",
                    "plaintext authentication takes no credential",
                ));
            }
        }
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

/// Build the stored object.
#[must_use]
pub fn build(
    namespace: &str,
    name: String,
    annotations: BTreeMap<String, String>,
    request: &CreateConnectionRequest,
) -> KafkaCluster {
    KafkaCluster {
        metadata: ObjectMeta {
            name: Some(name),
            namespace: Some(namespace.to_string()),
            annotations: Some(annotations),
            ..ObjectMeta::default()
        },
        spec: KafkaClusterSpec {
            bootstrap_servers: request.bootstrap_servers.clone(),
            auth: AuthBlock {
                mode: match request.auth.mode {
                    ConnectionAuthMode::Plaintext => AuthMode::Plaintext,
                    ConnectionAuthMode::ScramSha512 => AuthMode::ScramSha512,
                },
                username: request.auth.username.clone(),
                // PLAT-07.1's contract v1 added `passwordKey` and `tlsCa`.
                // The API does not accept either yet, and ABSENT is the
                // contract's documented legacy behaviour — `password`, and the
                // runner image's own trust store — so an object this route
                // creates is exactly the object it created before the fields
                // existed. Surfacing them is PLAT-17.2's own decision.
                secret_ref: request
                    .auth
                    .credential_ref
                    .as_ref()
                    .map(|r| CredentialSecretRef {
                        name: r.name.clone(),
                        password_key: None,
                        unrecognized_fields: UnrecognizedFields::default(),
                    }),
                tls: request.auth.tls,
                tls_ca: None,
                unrecognized_fields: UnrecognizedFields::default(),
            },
            role: match request.role {
                ConnectionRole::Source => "source".to_string(),
                ConnectionRole::Target => "target".to_string(),
            },
            marker_topic: request.marker_topic.clone(),
            unrecognized_fields: UnrecognizedFields::default(),
        },
        status: None,
    }
}

/// `POST .../connections`.
pub async fn create(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    actor: Actor,
    ApiPath(ns): ApiPath<String>,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, ApiError> {
    authorize(&state, &actor, &ns, Action::CreateConnection)?;
    crate::http::parse_query(uri.query(), &[])?;
    let key = IdempotencyKey::from_headers(&headers)?;
    let request: CreateConnectionRequest = read_json(body, MAX_JSON_BODY).await?;
    validate_create(&request)?;
    let created = create_idempotent(
        &state,
        &actor,
        &ns,
        ROUTE_CREATE,
        NAME_PREFIX,
        &key,
        &request_id,
        &request,
        |name, annotations| build(&ns, name, annotations, &request),
    )
    .await?;
    Ok(json(
        created.status(),
        &ConnectionResponse {
            request_id,
            replayed: Some(created.replayed),
            item: projection::connection(&created.object),
        },
    ))
}
