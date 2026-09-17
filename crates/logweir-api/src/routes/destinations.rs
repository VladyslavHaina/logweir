//! Destinations: `BackupDestination` projections, a typed create, an access
//! rotation, an explicit access test and a legacy adoption.
//!
//! # No credential value ever leaves here
//!
//! A grant may be entered as a VALUE exactly once, in `access.*.secret.new`.
//! That value becomes a Secret through [`crate::kube::KubeAdapter::create_credential`]
//! and is never read back: this service has no Secret read verb, the create
//! response's `data` is dropped by the parser, and every projection below
//! carries a Secret NAME and the KEY NAMES inside it — both public references —
//! and nothing else. [`crate::contract::NewCredentialRequest`] has a
//! hand-written `Debug` and no `Display`, so a stray log field cannot print
//! one either.
//!
//! # The name is the contract
//!
//! Unlike a schedule or a restore, a destination is referenced BY NAME by
//! every object that uses it, so the operator's chosen name is the Kubernetes
//! object name (see [`super::create_named_idempotent`]).
//!
//! # Addressing is never transport
//!
//! `storage.addressing` and `transport.security` are independent fields, and
//! the only rule that couples anything is the endpoint SCHEME. This module
//! never derives one from the other in either direction: that derivation is
//! defect G5/UI-HTTPDOWNGRADE, and it does not exist here.

use std::collections::BTreeMap;

use axum::body::Body;
use axum::extract::State;
use axum::response::Response;
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use http::{HeaderMap, StatusCode, Uri};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::Resource as _;
use logweir_core::destination as core_destination;
use weirkeeper::crds::backup::Backup as BackupCr;
use weirkeeper::crds::backup_destination::{
    AccessBlock, AccessGrant, AccessMode, BackupDestination, BackupDestinationSpec, CaBundleRef,
    EvidenceReadAccessGrant, EvidenceReadMode, ReadinessBlock, S3SecretKeysRef, StorageLocation,
    TransportBlock, WorkloadIdentityRef, WriteProbe, DEFAULT_ACCESS_KEY_ID_KEY, DEFAULT_CA_KEY,
    DEFAULT_RUNNER_SERVICE_ACCOUNT, DEFAULT_SECRET_ACCESS_KEY_KEY,
};
use weirkeeper::crds::backup_schedule::BackupSchedule;
use weirkeeper::crds::preflight::Preflight as PreflightCr;

use super::{
    authorize, authorize_also, check_name, create_named_idempotent, get_object, is_own_replay,
    json, list_page, list_query, ApiPath, MAX_LIMIT,
};
use crate::app::AppState;
use crate::auth::Actor;
use crate::authz::Action;
use crate::contract::{
    AccessGrantRequest, AccessGrantView, AccessModeDto, AccessRequest, AccessView, AddressingDto,
    CaBundleRequest, CaBundleView, CreateDestinationRequest, Destination,
    DestinationFromLegacyRequest, DestinationList, DestinationResponse, DestinationRoleDto,
    DestinationStatusView, DestinationSummary, DestinationUsageResponse, DestinationUseView,
    LastTestView, PreflightResponse, StorageProviderDto, StorageRequest, StorageView,
    TestDestinationRequest, TransportRequest, TransportSecurityDto, TransportView,
    UpdateDestinationAccessRequest, WriteProbeDto,
};
use crate::http::{read_json, RequestId, MAX_JSON_BODY};
use crate::idempotency::IdempotencyKey;
use crate::kube::{KubeFailure, PageRequest, WriteOnlyCredential};
use crate::problem::{ApiError, FieldError, ProblemCode};
use crate::status::{condition_view, MAX_CONDITIONS};
use crate::validate;

/// The list route identifier (cursor scope).
pub const ROUTE_LIST: &str = "GET /api/v1/namespaces/{ns}/destinations";
/// The create route identifier (idempotency scope).
pub const ROUTE_CREATE: &str = "POST /api/v1/namespaces/{ns}/destinations";
/// The access-rotation route identifier.
pub const ROUTE_UPDATE_ACCESS: &str =
    "POST /api/v1/namespaces/{ns}/destinations/{name}:update-access";
/// The test route identifier (idempotency scope).
pub const ROUTE_TEST: &str = "POST /api/v1/namespaces/{ns}/destinations/{name}:test";
/// The legacy-adoption route identifier (idempotency scope).
pub const ROUTE_FROM_LEGACY: &str = "POST /api/v1/namespaces/{ns}/destinations:from-legacy";

/// The `:update-access` command suffix.
pub const UPDATE_ACCESS: &str = ":update-access";
/// The `:test` command suffix.
pub const TEST: &str = ":test";
/// The collection-level `:from-legacy` command name.
pub const FROM_LEGACY: &str = "destinations:from-legacy";

/// The annotation that marks the namespace default. At most one destination
/// per namespace may hold it; the create route refuses a second.
pub const DEFAULT_ANNOTATION: &str = "logweir.dev/default-destination";

/// The label every object the API creates for a destination carries, so
/// `usage` is a label read and never an unbounded scan.
pub const DESTINATION_LABEL: &str = "logweir.dev/destination";

/// The label ONLY `:test` sets, and the one `lastTest` selects on.
///
/// A SECOND LABEL, BECAUSE THE FIRST ONE IS SHARED. A Backup readiness check
/// against this destination also carries [`DESTINATION_LABEL`], so selecting on
/// it alone returns a mixture and the newest access test can sit behind any
/// number of backup checks — in NAME order, which for hashed names is
/// arbitrary. Selecting on a label only the access test carries keeps the
/// query to one selector and the answer to the objects it is about.
pub const DESTINATION_TEST_LABEL: &str = "logweir.dev/destination-test";

/// The label the API's own default destination carries, so the at-most-one
/// check is a bounded selector read rather than a scan of every destination in
/// the namespace.
///
/// The ANNOTATION D2 §3.1 names is written too, and [`is_default`] honours
/// either, so a default set by hand with `kubectl` still reads as one.
pub const DEFAULT_LABEL: &str = "logweir.dev/default-destination";

/// The most list pages any bounded read here follows before it reports
/// `truncated` instead of continuing.
pub const MAX_PAGES: usize = 8;

/// The label on a Secret this service created for a destination role.
pub const CREDENTIAL_FOR_LABEL: &str = "logweir.dev/credential-for";
/// The role a credential Secret serves.
pub const CREDENTIAL_ROLE_LABEL: &str = "logweir.dev/credential-role";
/// `app.kubernetes.io/managed-by`.
pub const MANAGED_BY_LABEL: &str = "app.kubernetes.io/managed-by";
/// Its value.
pub const MANAGED_BY_VALUE: &str = "logweir";
/// States the write-only rule on the object itself.
pub const WRITE_ONLY_ANNOTATION: &str = "logweir.dev/write-only";

/// The Secret `type` of an object-store credential this service creates.
///
/// A DISTINCT TYPE, DELIBERATELY. `create` on Secrets is broad — in a
/// namespace it can mint a `kubernetes.io/service-account-token` for any
/// ServiceAccount there. A distinct, immutable-after-create type is what a
/// ValidatingAdmissionPolicy scoped to the console ServiceAccount can require,
/// so this service's create permission cannot be spent on anything else. The
/// policy is stage 7's to ship (D2 §7.3); the type it needs is here.
pub const CREDENTIAL_SECRET_TYPE: &str = "logweir.dev/object-store-credential";

/// The session-token data key, when one is entered.
pub const SESSION_TOKEN_KEY: &str = "session-token";

/// The longest credential component accepted, in bytes.
pub const MAX_CREDENTIAL_BYTES: usize = 1024;

/// The most schedules or backups `usage` reports.
pub const MAX_USAGE_ENTRIES: usize = 100;

// ======================================================================
// Projection
// ======================================================================

fn addressing_dto(value: core_destination::Addressing) -> AddressingDto {
    match value {
        core_destination::Addressing::PathStyle => AddressingDto::PathStyle,
        core_destination::Addressing::VirtualHosted => AddressingDto::VirtualHosted,
    }
}

fn transport_dto(value: core_destination::TransportSecurity) -> TransportSecurityDto {
    match value {
        core_destination::TransportSecurity::Tls => TransportSecurityDto::Tls,
        core_destination::TransportSecurity::InsecureHttp => TransportSecurityDto::InsecureHttp,
    }
}

fn grant_view(grant: &AccessGrant) -> AccessGrantView {
    match grant.mode {
        AccessMode::SecretKeys => secret_keys_view(grant.secret.as_ref()),
        AccessMode::WorkloadIdentity => AccessGrantView {
            mode: AccessModeDto::WorkloadIdentity,
            secret_name: None,
            keys: Vec::new(),
            service_account_name: Some(grant.workload_identity.as_ref().map_or_else(
                || DEFAULT_RUNNER_SERVICE_ACCOUNT.to_string(),
                |w| w.service_account_name.clone(),
            )),
        },
    }
}

fn secret_keys_view(secret: Option<&S3SecretKeysRef>) -> AccessGrantView {
    let mut keys = Vec::new();
    let mut name = None;
    if let Some(secret) = secret {
        name = Some(secret.name.clone());
        keys.push(secret.access_key_id_key.clone());
        keys.push(secret.secret_access_key_key.clone());
        if let Some(token) = &secret.session_token_key {
            keys.push(token.clone());
        }
        keys.sort();
    }
    AccessGrantView {
        mode: AccessModeDto::SecretKeys,
        secret_name: name,
        keys,
        service_account_name: None,
    }
}

fn evidence_read_view(grant: Option<&EvidenceReadAccessGrant>) -> AccessGrantView {
    let Some(grant) = grant else {
        // ABSENT IS A STATED ANSWER. `evidenceRead` absent means verification
        // is NotAttempted, and a response that left the field out would leave
        // a console to guess it was an oversight.
        return AccessGrantView {
            mode: AccessModeDto::NotConfigured,
            secret_name: None,
            keys: Vec::new(),
            service_account_name: None,
        };
    };
    match grant.mode {
        EvidenceReadMode::SecretKeys => secret_keys_view(grant.secret.as_ref()),
        EvidenceReadMode::WorkloadIdentity => AccessGrantView {
            mode: AccessModeDto::WorkloadIdentity,
            secret_name: None,
            keys: Vec::new(),
            service_account_name: Some(grant.workload_identity.as_ref().map_or_else(
                || DEFAULT_RUNNER_SERVICE_ACCOUNT.to_string(),
                |w| w.service_account_name.clone(),
            )),
        },
        EvidenceReadMode::ControllerIdentity => AccessGrantView {
            mode: AccessModeDto::ControllerIdentity,
            secret_name: None,
            keys: Vec::new(),
            service_account_name: None,
        },
        EvidenceReadMode::ArchiveReadGrant => AccessGrantView {
            mode: AccessModeDto::ArchiveReadGrant,
            secret_name: None,
            keys: Vec::new(),
            service_account_name: None,
        },
    }
}

fn inherits() -> AccessGrantView {
    AccessGrantView {
        mode: AccessModeDto::InheritsArchiveWrite,
        secret_name: None,
        keys: Vec::new(),
        service_account_name: None,
    }
}

fn access_view(access: &AccessBlock) -> AccessView {
    AccessView {
        archive_write: grant_view(&access.archive_write),
        archive_read: access
            .archive_read
            .as_ref()
            .map_or_else(inherits, grant_view),
        evidence_write: access
            .evidence_write
            .as_ref()
            .map_or_else(inherits, grant_view),
        evidence_read: evidence_read_view(access.evidence_read.as_ref()),
    }
}

fn location_of(spec: &BackupDestinationSpec) -> core_destination::DestinationLocation {
    core_destination::DestinationLocation {
        provider: spec.storage.provider,
        bucket: spec.storage.bucket.clone(),
        prefix: spec.storage.prefix.clone(),
        region: spec.storage.region.clone(),
        endpoint: spec.storage.endpoint.clone(),
        addressing: spec.storage.addressing,
        transport: spec.transport.security,
    }
}

fn status_view(object: &BackupDestination) -> DestinationStatusView {
    let status = object.status.as_ref();
    let valid = status
        .and_then(|s| s.conditions.as_ref())
        .and_then(|c| c.iter().find(|c| c.r#type == "Valid"))
        .map(|c| c.status == "True");
    DestinationStatusView {
        valid,
        reason: status.and_then(|s| s.reason.clone()),
        message: status
            .and_then(|s| s.conditions.as_ref())
            .and_then(|c| c.iter().find(|c| c.r#type == "Valid"))
            .and_then(|c| c.message.clone())
            .map(|m| validate::bounded(&m, 1024)),
        observed_generation: status.and_then(|s| s.observed_generation),
        observed_at: status.and_then(|s| s.observed_at.as_ref().copied()),
    }
}

/// EITHER SPELLING IS HONOURED. The API writes both the annotation D2 §3.1
/// names (what a human reads in `kubectl get -o yaml`) and the label the
/// at-most-one check selects on; an object marked by hand with only the
/// annotation still reads as the default here.
fn is_default(object: &BackupDestination) -> bool {
    let meta = object.meta();
    meta.annotations
        .as_ref()
        .and_then(|a| a.get(DEFAULT_ANNOTATION))
        .is_some_and(|v| v == "true")
        || meta
            .labels
            .as_ref()
            .and_then(|l| l.get(DEFAULT_LABEL))
            .is_some_and(|v| v == "true")
}

fn storage_view(spec: &BackupDestinationSpec) -> StorageView {
    StorageView {
        provider: StorageProviderDto::S3,
        bucket: spec.storage.bucket.clone(),
        prefix: spec.storage.prefix.clone(),
        region: spec.storage.region.clone(),
        endpoint: spec.storage.endpoint.clone(),
        addressing: addressing_dto(spec.storage.addressing),
    }
}

/// A `BackupDestination` as the product DTO. References only.
#[must_use]
pub fn project(object: &BackupDestination, last_test: Option<LastTestView>) -> Destination {
    let meta = object.meta();
    let location = location_of(&object.spec);
    Destination {
        name: meta.name.clone().unwrap_or_default(),
        namespace: meta.namespace.clone().unwrap_or_default(),
        uid: meta.uid.clone().unwrap_or_default(),
        resource_version: meta.resource_version.clone().unwrap_or_default(),
        generation: meta.generation.unwrap_or_default(),
        created_at: meta.creation_timestamp.as_ref().map(|t| t.0),
        description: object.spec.description.clone(),
        storage: storage_view(&object.spec),
        transport: TransportView {
            security: transport_dto(object.spec.transport.security),
            ca_bundle: object
                .spec
                .transport
                .ca_bundle
                .as_ref()
                .map(|c| CaBundleView {
                    config_map_name: c.config_map_name.clone(),
                    key: c.key.clone(),
                    sha256: object
                        .status
                        .as_ref()
                        .and_then(|s| s.ca_bundle_sha256.clone()),
                }),
        },
        access: access_view(&object.spec.access),
        write_probe: match object
            .spec
            .readiness
            .as_ref()
            .map_or(WriteProbe::Disabled, |r| r.write_probe)
        {
            WriteProbe::Disabled => WriteProbeDto::Disabled,
            WriteProbe::CreateOnlyMarker => WriteProbeDto::CreateOnlyMarker,
        },
        // THE CONTROLLER'S URL WHEN IT HAS ONE, THE PURE FUNCTION OTHERWISE.
        // Both are `logweir_core::destination`'s own spelling, so the two can
        // only agree; computing it here means a destination reads correctly in
        // the seconds before its first reconcile.
        canonical_url: object
            .status
            .as_ref()
            .and_then(|s| s.canonical_url.clone())
            .unwrap_or_else(|| location.canonical_url()),
        location_digest: object
            .status
            .as_ref()
            .and_then(|s| s.location_digest.clone()),
        status: status_view(object),
        default: is_default(object),
        last_test,
    }
}

/// A `BackupDestination` as a list row.
#[must_use]
pub fn summary(object: &BackupDestination) -> DestinationSummary {
    let meta = object.meta();
    let location = location_of(&object.spec);
    DestinationSummary {
        name: meta.name.clone().unwrap_or_default(),
        uid: meta.uid.clone().unwrap_or_default(),
        generation: meta.generation.unwrap_or_default(),
        description: object.spec.description.clone(),
        canonical_url: object
            .status
            .as_ref()
            .and_then(|s| s.canonical_url.clone())
            .unwrap_or_else(|| location.canonical_url()),
        endpoint: object.spec.storage.endpoint.clone(),
        transport: transport_dto(object.spec.transport.security),
        addressing: addressing_dto(object.spec.storage.addressing),
        status: status_view(object),
        default: is_default(object),
    }
}

// ======================================================================
// Validation
// ======================================================================

/// Strip the `spec.` the pure layer's field paths carry: a 422 names the field
/// the CLIENT sent, not the field the CRD stores it in.
fn request_field(core_field: &str) -> String {
    core_field
        .strip_prefix("spec.")
        .unwrap_or(core_field)
        .to_string()
}

fn code_for(core: &core_destination::FieldError, transport: TransportSecurityDto) -> &'static str {
    match core.rule {
        "bucket" => "bucket_invalid",
        "region" => "region_invalid",
        "R5" => "endpoint_not_origin",
        "R3" => match transport {
            TransportSecurityDto::InsecureHttp => "insecure_http_requires_http_endpoint",
            TransportSecurityDto::Tls => "transport_scheme_mismatch",
        },
        "R6" => {
            if core.message.contains("reserved evidence root") {
                "prefix_reserved"
            } else {
                "prefix_invalid"
            }
        }
        "R4" => "ca_bundle_requires_tls",
        _ => "invalid",
    }
}

fn location_from_request(
    storage: &StorageRequest,
    transport: TransportSecurityDto,
) -> core_destination::DestinationLocation {
    core_destination::DestinationLocation {
        provider: core_destination::StorageProvider::S3,
        bucket: storage.bucket.clone(),
        prefix: storage.prefix.clone().unwrap_or_default(),
        region: storage.region.clone(),
        endpoint: storage.endpoint.clone(),
        addressing: match storage.addressing {
            AddressingDto::PathStyle => core_destination::Addressing::PathStyle,
            AddressingDto::VirtualHosted => core_destination::Addressing::VirtualHosted,
        },
        transport: match transport {
            TransportSecurityDto::Tls => core_destination::TransportSecurity::Tls,
            TransportSecurityDto::InsecureHttp => core_destination::TransportSecurity::InsecureHttp,
        },
    }
}

fn check_secret_name(field: &str, name: &str, errors: &mut Vec<FieldError>) {
    if !validate::is_dns_subdomain(name) {
        errors.push(FieldError::new(
            field,
            "invalid_name",
            "must be a Kubernetes object name",
        ));
    }
}

fn check_data_key(field: &str, key: &str, errors: &mut Vec<FieldError>) {
    let ok = !key.is_empty()
        && key.len() <= 253
        && key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_'));
    if !ok {
        errors.push(FieldError::new(
            field,
            "invalid_key",
            "a Secret data key matches ^[-._a-zA-Z0-9]{1,253}$",
        ));
    }
}

/// A credential component: non-empty, bounded, no control characters and no
/// leading or trailing whitespace. The message names a CLASS and never the
/// value.
fn check_credential(field: &str, value: &str, errors: &mut Vec<FieldError>) {
    if value.is_empty() {
        errors.push(FieldError::new(field, "required", "the value is empty"));
        return;
    }
    if value.len() > MAX_CREDENTIAL_BYTES {
        errors.push(FieldError::new(
            field,
            "too_long",
            format!("the value is longer than {MAX_CREDENTIAL_BYTES} bytes"),
        ));
        return;
    }
    if value.chars().any(char::is_control) {
        errors.push(FieldError::new(
            field,
            "invalid_character",
            "the value contains a control character, which no run can render",
        ));
        return;
    }
    if value.trim() != value {
        errors.push(FieldError::new(
            field,
            "edge_whitespace",
            "the value starts or ends with whitespace, which configuration substitution strips",
        ));
    }
}

/// Which modes a given grant field may carry.
#[derive(Clone, Copy, PartialEq, Eq)]
enum GrantSlot {
    /// `archiveWrite`, `archiveRead`, `evidenceWrite`: R7's two modes.
    Standard,
    /// `evidenceRead`: R8's four.
    EvidenceRead,
}

fn validate_grant(
    field: &str,
    slot: GrantSlot,
    grant: &AccessGrantRequest,
    errors: &mut Vec<FieldError>,
) {
    let allowed = match slot {
        GrantSlot::Standard => matches!(
            grant.mode,
            AccessModeDto::SecretKeys | AccessModeDto::WorkloadIdentity
        ),
        GrantSlot::EvidenceRead => matches!(
            grant.mode,
            AccessModeDto::SecretKeys
                | AccessModeDto::WorkloadIdentity
                | AccessModeDto::ControllerIdentity
                | AccessModeDto::ArchiveReadGrant
        ),
    };
    if !allowed {
        errors.push(FieldError::new(
            format!("{field}.mode"),
            "grant_mode_fields",
            match slot {
                GrantSlot::Standard => {
                    "this grant takes secretKeys or workloadIdentity; inheritsArchiveWrite and \
                     notConfigured are response-only spellings of an ABSENT grant"
                }
                GrantSlot::EvidenceRead => {
                    "evidenceRead takes secretKeys, workloadIdentity, controllerIdentity or \
                     archiveReadGrant; notConfigured is the response-only spelling of an ABSENT \
                     grant"
                }
            },
        ));
        return;
    }
    // R7 and R8: the reference shape must match the mode.
    if grant.mode == AccessModeDto::SecretKeys {
        if grant.workload_identity.is_some() {
            errors.push(FieldError::new(
                format!("{field}.workloadIdentity"),
                "grant_mode_fields",
                "secretKeys takes a secret and no workloadIdentity",
            ));
        }
        match &grant.secret {
            None => errors.push(FieldError::new(
                format!("{field}.secret"),
                "grant_mode_fields",
                "secretKeys needs a secret: either an existing name or a new value",
            )),
            Some(source) => match (&source.existing, &source.new) {
                (Some(existing), None) => {
                    check_secret_name(
                        &format!("{field}.secret.existing.name"),
                        &existing.name,
                        errors,
                    );
                    for (key_field, key) in [
                        ("accessKeyIdKey", &existing.access_key_id_key),
                        ("secretAccessKeyKey", &existing.secret_access_key_key),
                        ("sessionTokenKey", &existing.session_token_key),
                    ] {
                        if let Some(key) = key {
                            check_data_key(
                                &format!("{field}.secret.existing.{key_field}"),
                                key,
                                errors,
                            );
                        }
                    }
                }
                (None, Some(new)) => {
                    check_credential(
                        &format!("{field}.secret.new.accessKeyId"),
                        &new.access_key_id,
                        errors,
                    );
                    check_credential(
                        &format!("{field}.secret.new.secretAccessKey"),
                        &new.secret_access_key,
                        errors,
                    );
                    if let Some(token) = &new.session_token {
                        check_credential(
                            &format!("{field}.secret.new.sessionToken"),
                            token,
                            errors,
                        );
                    }
                }
                _ => errors.push(FieldError::new(
                    format!("{field}.secret"),
                    "grant_mode_fields",
                    "set exactly one of secret.existing or secret.new",
                )),
            },
        }
    } else {
        if grant.secret.is_some() {
            errors.push(FieldError::new(
                format!("{field}.secret"),
                "grant_mode_fields",
                "only secretKeys takes a secret",
            ));
        }
        if grant.mode != AccessModeDto::WorkloadIdentity && grant.workload_identity.is_some() {
            errors.push(FieldError::new(
                format!("{field}.workloadIdentity"),
                "grant_mode_fields",
                "only workloadIdentity takes a ServiceAccount",
            ));
        }
        if let Some(w) = &grant.workload_identity {
            if let Some(name) = &w.service_account_name {
                check_secret_name(
                    &format!("{field}.workloadIdentity.serviceAccountName"),
                    name,
                    errors,
                );
            }
        }
    }
}

/// Validate the four grants, including R9.
pub(crate) fn validate_access(access: &AccessRequest, errors: &mut Vec<FieldError>) {
    validate_grant(
        "access.archiveWrite",
        GrantSlot::Standard,
        &access.archive_write,
        errors,
    );
    for (field, grant) in [
        ("access.archiveRead", &access.archive_read),
        ("access.evidenceWrite", &access.evidence_write),
    ] {
        if let Some(grant) = grant {
            validate_grant(field, GrantSlot::Standard, grant, errors);
        }
    }
    if let Some(grant) = &access.evidence_read {
        validate_grant(
            "access.evidenceRead",
            GrantSlot::EvidenceRead,
            grant,
            errors,
        );
        // R9 — a write grant is never reused to read evidence.
        if grant.mode == AccessModeDto::ArchiveReadGrant && access.archive_read.is_none() {
            errors.push(FieldError::new(
                "access.evidenceRead.mode",
                "grant_mode_fields",
                "evidenceRead archiveReadGrant requires an explicit read-only archiveRead \
                 grant; a write grant is never reused for verification",
            ));
        }
    }
}

/// Validate a create request against D2 §3.2's rules, every failure at once.
///
/// # Errors
///
/// `destination_invalid` naming every field and the rule it breaks.
pub fn validate_create(request: &CreateDestinationRequest) -> Result<(), ApiError> {
    let mut errors = Vec::new();
    if !validate::is_dns_subdomain(&request.name) || request.name.len() > 63 {
        errors.push(FieldError::new(
            "name",
            "invalid_name",
            "a destination name is a Kubernetes object name of at most 63 characters",
        ));
    }
    if let Some(description) = &request.description {
        if description.len() > 256 || description.chars().any(char::is_control) {
            errors.push(FieldError::new(
                "description",
                "invalid_description",
                "at most 256 bytes on one line",
            ));
        }
    }
    validate_location(&request.storage, &request.transport, &mut errors);
    validate_access(&request.access, &mut errors);
    if errors.is_empty() {
        Ok(())
    } else {
        Err(ApiError::destination_invalid(errors))
    }
}

pub(crate) fn validate_location(
    storage: &StorageRequest,
    transport: &TransportRequest,
    errors: &mut Vec<FieldError>,
) {
    let location = location_from_request(storage, transport.security);
    if let Err(core_errors) = core_destination::validate(&location) {
        for core in core_errors {
            errors.push(FieldError::new(
                request_field(&core.field),
                code_for(&core, transport.security),
                core.message.clone(),
            ));
        }
    }
    if let Err(core) =
        core_destination::validate_ca_bundle(location.transport, transport.ca_bundle.is_some())
    {
        errors.push(FieldError::new(
            request_field(&core.field),
            code_for(&core, transport.security),
            core.message.clone(),
        ));
    }
    if let Some(bundle) = &transport.ca_bundle {
        check_secret_name(
            "transport.caBundle.configMapName",
            &bundle.config_map_name,
            errors,
        );
        if let Some(key) = &bundle.key {
            check_data_key("transport.caBundle.key", key, errors);
        }
    }
    // G4/ENGINE-PATHSTYLE: a setting the pinned engine cannot honour is
    // refused, never advertised.
    if let Err(message) = core_destination::engine_compatible(&location) {
        errors.push(FieldError::new(
            "storage.addressing",
            "addressing_unsupported_by_engine",
            message,
        ));
    }
}

// ======================================================================
// Building the stored object
// ======================================================================

fn secret_ref_for(
    destination: &str,
    role: DestinationRoleDto,
    grant: &AccessGrantRequest,
) -> Option<S3SecretKeysRef> {
    let source = grant.secret.as_ref()?;
    if let Some(existing) = &source.existing {
        return Some(S3SecretKeysRef {
            name: existing.name.clone(),
            access_key_id_key: existing
                .access_key_id_key
                .clone()
                .unwrap_or_else(|| DEFAULT_ACCESS_KEY_ID_KEY.to_string()),
            secret_access_key_key: existing
                .secret_access_key_key
                .clone()
                .unwrap_or_else(|| DEFAULT_SECRET_ACCESS_KEY_KEY.to_string()),
            session_token_key: existing.session_token_key.clone(),
        });
    }
    let new = source.new.as_ref()?;
    Some(S3SecretKeysRef {
        name: credential_secret_name(destination, role),
        access_key_id_key: DEFAULT_ACCESS_KEY_ID_KEY.to_string(),
        secret_access_key_key: DEFAULT_SECRET_ACCESS_KEY_KEY.to_string(),
        session_token_key: new
            .session_token
            .as_ref()
            .map(|_| SESSION_TOKEN_KEY.to_string()),
    })
}

/// `lwd-<destination>-<role>` (D2 §8.1).
#[must_use]
pub fn credential_secret_name(destination: &str, role: DestinationRoleDto) -> String {
    format!("lwd-{destination}-{}", role_slug(role))
}

const fn role_slug(role: DestinationRoleDto) -> &'static str {
    match role {
        DestinationRoleDto::ArchiveWrite => "archive-write",
        DestinationRoleDto::ArchiveRead => "archive-read",
        DestinationRoleDto::EvidenceWrite => "evidence-write",
        DestinationRoleDto::EvidenceRead => "evidence-read",
    }
}

fn workload_ref(grant: &AccessGrantRequest) -> Option<WorkloadIdentityRef> {
    Some(WorkloadIdentityRef {
        service_account_name: grant
            .workload_identity
            .as_ref()
            .and_then(|w| w.service_account_name.clone())
            .unwrap_or_else(|| DEFAULT_RUNNER_SERVICE_ACCOUNT.to_string()),
    })
}

fn build_grant(
    destination: &str,
    role: DestinationRoleDto,
    grant: &AccessGrantRequest,
) -> AccessGrant {
    match grant.mode {
        AccessModeDto::SecretKeys => AccessGrant {
            mode: AccessMode::SecretKeys,
            secret: secret_ref_for(destination, role, grant),
            workload_identity: None,
        },
        _ => AccessGrant {
            mode: AccessMode::WorkloadIdentity,
            secret: None,
            workload_identity: workload_ref(grant),
        },
    }
}

fn build_evidence_read(destination: &str, grant: &AccessGrantRequest) -> EvidenceReadAccessGrant {
    match grant.mode {
        AccessModeDto::SecretKeys => EvidenceReadAccessGrant {
            mode: EvidenceReadMode::SecretKeys,
            secret: secret_ref_for(destination, DestinationRoleDto::EvidenceRead, grant),
            workload_identity: None,
        },
        AccessModeDto::WorkloadIdentity => EvidenceReadAccessGrant {
            mode: EvidenceReadMode::WorkloadIdentity,
            secret: None,
            workload_identity: workload_ref(grant),
        },
        AccessModeDto::ControllerIdentity => EvidenceReadAccessGrant {
            mode: EvidenceReadMode::ControllerIdentity,
            secret: None,
            workload_identity: None,
        },
        _ => EvidenceReadAccessGrant {
            mode: EvidenceReadMode::ArchiveReadGrant,
            secret: None,
            workload_identity: None,
        },
    }
}

/// The stored `spec.access` for a validated request.
#[must_use]
pub fn build_access(destination: &str, access: &AccessRequest) -> AccessBlock {
    AccessBlock {
        archive_write: build_grant(
            destination,
            DestinationRoleDto::ArchiveWrite,
            &access.archive_write,
        ),
        archive_read: access
            .archive_read
            .as_ref()
            .map(|g| build_grant(destination, DestinationRoleDto::ArchiveRead, g)),
        evidence_write: access
            .evidence_write
            .as_ref()
            .map(|g| build_grant(destination, DestinationRoleDto::EvidenceWrite, g)),
        evidence_read: access
            .evidence_read
            .as_ref()
            .map(|g| build_evidence_read(destination, g)),
    }
}

fn build_storage(storage: &StorageRequest) -> StorageLocation {
    StorageLocation {
        provider: core_destination::StorageProvider::S3,
        bucket: storage.bucket.clone(),
        prefix: storage.prefix.clone().unwrap_or_default(),
        region: storage.region.clone(),
        endpoint: storage.endpoint.clone(),
        addressing: match storage.addressing {
            AddressingDto::PathStyle => core_destination::Addressing::PathStyle,
            AddressingDto::VirtualHosted => core_destination::Addressing::VirtualHosted,
        },
    }
}

fn build_transport(transport: &TransportRequest) -> TransportBlock {
    TransportBlock {
        security: match transport.security {
            TransportSecurityDto::Tls => core_destination::TransportSecurity::Tls,
            TransportSecurityDto::InsecureHttp => core_destination::TransportSecurity::InsecureHttp,
        },
        ca_bundle: transport.ca_bundle.as_ref().map(build_ca_bundle),
    }
}

fn build_ca_bundle(bundle: &CaBundleRequest) -> CaBundleRef {
    CaBundleRef {
        config_map_name: bundle.config_map_name.clone(),
        key: bundle
            .key
            .clone()
            .unwrap_or_else(|| DEFAULT_CA_KEY.to_string()),
    }
}

/// Build the stored object for a validated create request.
#[must_use]
pub fn build(
    namespace: &str,
    name: String,
    mut annotations: BTreeMap<String, String>,
    request: &CreateDestinationRequest,
) -> BackupDestination {
    let mut labels = BTreeMap::new();
    if request.default == Some(true) {
        annotations.insert(DEFAULT_ANNOTATION.to_string(), "true".to_string());
        labels.insert(DEFAULT_LABEL.to_string(), "true".to_string());
    }
    BackupDestination {
        metadata: ObjectMeta {
            name: Some(name),
            namespace: Some(namespace.to_string()),
            annotations: Some(annotations),
            labels: Some(labels),
            ..ObjectMeta::default()
        },
        spec: BackupDestinationSpec {
            description: request.description.clone(),
            storage: build_storage(&request.storage),
            transport: build_transport(&request.transport),
            access: build_access(&request.name, &request.access),
            readiness: Some(ReadinessBlock {
                // THE API'S DEFAULT DIFFERS FROM THE CRD'S, AND SAYS SO.
                // A form that discloses the choice may default to the useful
                // answer; an object created by `kubectl` with no opinion may
                // not, because nothing disclosed it there (D2 §8.1).
                write_probe: match request
                    .readiness
                    .as_ref()
                    .and_then(|r| r.write_probe)
                    .unwrap_or(WriteProbeDto::CreateOnlyMarker)
                {
                    WriteProbeDto::Disabled => WriteProbe::Disabled,
                    WriteProbeDto::CreateOnlyMarker => WriteProbe::CreateOnlyMarker,
                },
            }),
        },
        status: None,
    }
}

// ======================================================================
// Write-only credential creation
// ======================================================================

/// The Secret one `secret.new` entry becomes.
///
/// OWNED BY THE DESTINATION, so deleting the destination collects it; the API
/// never deletes anything itself.
fn build_credential_secret(
    namespace: &str,
    destination: &BackupDestination,
    role: DestinationRoleDto,
    new: &crate::contract::NewCredentialRequest,
    request_id: &str,
) -> WriteOnlyCredential {
    let name = destination.meta().name.clone().unwrap_or_default();
    let uid = destination.meta().uid.clone().unwrap_or_default();
    let mut data = BTreeMap::from([
        (
            DEFAULT_ACCESS_KEY_ID_KEY.to_string(),
            BASE64.encode(new.access_key_id.as_bytes()),
        ),
        (
            DEFAULT_SECRET_ACCESS_KEY_KEY.to_string(),
            BASE64.encode(new.secret_access_key.as_bytes()),
        ),
    ]);
    if let Some(token) = &new.session_token {
        data.insert(
            SESSION_TOKEN_KEY.to_string(),
            BASE64.encode(token.as_bytes()),
        );
    }
    WriteOnlyCredential {
        metadata: ObjectMeta {
            name: Some(credential_secret_name(&name, role)),
            namespace: Some(namespace.to_string()),
            labels: Some(BTreeMap::from([
                (MANAGED_BY_LABEL.to_string(), MANAGED_BY_VALUE.to_string()),
                (CREDENTIAL_FOR_LABEL.to_string(), name.clone()),
                (
                    CREDENTIAL_ROLE_LABEL.to_string(),
                    role_slug(role).to_string(),
                ),
            ])),
            annotations: Some(BTreeMap::from([
                (WRITE_ONLY_ANNOTATION.to_string(), "true".to_string()),
                (
                    crate::idempotency::ANNOTATION_REQUEST_ID.to_string(),
                    request_id.to_string(),
                ),
            ])),
            owner_references: Some(vec![crate::kube::owner_reference(
                "logweir.dev/v1alpha1",
                "BackupDestination",
                &name,
                &uid,
            )]),
            ..ObjectMeta::default()
        },
        type_: Some(CREDENTIAL_SECRET_TYPE.to_string()),
        data,
    }
}

/// The roles whose grants carry a credential VALUE, in a fixed order.
///
/// ONE PLACE THAT KNOWS WHICH NAMES A REQUEST WOULD WRITE, so the pre-flight
/// below and the writes that follow it cannot disagree about the set.
fn planned_credentials(
    access: &AccessRequest,
) -> Vec<(DestinationRoleDto, &crate::contract::NewCredentialRequest)> {
    let grants: [(DestinationRoleDto, Option<&AccessGrantRequest>); 4] = [
        (
            DestinationRoleDto::ArchiveWrite,
            Some(&access.archive_write),
        ),
        (
            DestinationRoleDto::ArchiveRead,
            access.archive_read.as_ref(),
        ),
        (
            DestinationRoleDto::EvidenceWrite,
            access.evidence_write.as_ref(),
        ),
        (
            DestinationRoleDto::EvidenceRead,
            access.evidence_read.as_ref(),
        ),
    ];
    grants
        .into_iter()
        .filter_map(|(role, grant)| {
            let new = grant?.secret.as_ref()?.new.as_ref()?;
            Some((role, new))
        })
        .collect()
}

/// **CHECK EVERY NAME BEFORE WRITING ANYTHING.**
///
/// Two defects share one cause, and this is the fix for both. Writing the
/// destination first and the credentials second meant a refusal had already
/// created the destination — so the retry the documentation prescribes came
/// back as a replay and ADOPTED the foreign Secret the first attempt had
/// refused. And writing the credentials role by role meant an earlier role's
/// value was already live when a later role refused, under a message that said
/// nothing had changed.
///
/// Establishing absence needs no read verb: a `dryRun: All` create is still
/// the `create` verb (see
/// [`crate::kube::KubeAdapter::credential_name_is_taken`]), and it answers
/// `AlreadyExists` for a taken name without revealing anything about the
/// object holding it. The probe carries an EMPTY `data`, so the entered value
/// does not travel anywhere before the decision to write it is made.
///
/// EVERY taken name is reported, not the first, because an operator fixing one
/// conflict should not discover the next one on the retry.
///
/// # Errors
///
/// `state_conflict` naming every name that already exists, with nothing
/// written; or the adapter's failure.
async fn refuse_taken_credential_names(
    state: &AppState,
    namespace: &str,
    destination: &str,
    access: &AccessRequest,
) -> Result<(), ApiError> {
    let mut taken = Vec::new();
    for (role, _) in planned_credentials(access) {
        let name = credential_secret_name(destination, role);
        let probe = probe_secret(namespace, &name);
        if state
            .kube()
            .credential_name_is_taken(namespace, &probe)
            .await
            .map_err(KubeFailure::into_api_error)?
        {
            taken.push(name);
        }
    }
    if taken.is_empty() {
        return Ok(());
    }
    tracing::warn!(
        namespace,
        destination,
        secrets = %taken.join(","),
        "credential secret names already exist; nothing was written"
    );
    Err(ApiError::new(
        ProblemCode::StateConflict,
        format!(
            "{} already exist{}, so the credential{} you entered {} NOT written and nothing at \
             all was created — not the destination, and not any other credential in this \
             request. This service holds `create` on Secrets and nothing else: it cannot read \
             the existing value to compare it, and it cannot overwrite it. Rotate the content \
             out of band (kubectl, or your secret manager), or point the grant at a different \
             existing Secret with `secret.existing`.",
            taken
                .iter()
                .map(|n| format!("`{n}`"))
                .collect::<Vec<_>>()
                .join(" and "),
            if taken.len() == 1 { "s" } else { "" },
            if taken.len() == 1 { "" } else { "s" },
            if taken.len() == 1 { "was" } else { "were" },
        ),
    ))
}

/// A name-only Secret for the dry-run probe. NO `data`, and no owner
/// reference: the destination may not exist yet, and a name conflict is
/// decided by the name.
fn probe_secret(namespace: &str, name: &str) -> WriteOnlyCredential {
    WriteOnlyCredential {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some(namespace.to_string()),
            labels: Some(BTreeMap::from([(
                MANAGED_BY_LABEL.to_string(),
                MANAGED_BY_VALUE.to_string(),
            )])),
            ..ObjectMeta::default()
        },
        type_: Some(CREDENTIAL_SECRET_TYPE.to_string()),
        data: BTreeMap::new(),
    }
}

/// What an existing Secret under the deterministic name means for this call.
///
/// THE NAME IS A FUNCTION OF THE DESTINATION AND THE ROLE, so "it is already
/// there" is ambiguous in exactly one direction: on a CREATE it may be the
/// object an earlier attempt of THIS request wrote, and on a rotation it can
/// only be an object holding some OTHER value. This service cannot read a
/// Secret, so it cannot tell the two apart by looking — the caller states
/// which case it is in, and the ambiguous one fails closed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ExistingCredential {
    /// This request scope created the destination on an earlier attempt, so a
    /// Secret under the deterministic name is the one that attempt wrote.
    /// Accept it, and report it as replayed rather than as created.
    AcceptAsReplay,
    /// Anything else. The value in the request was NOT written, and saying
    /// otherwise would let an operator record a rotation that did not happen.
    Refuse,
}

/// What a credential write actually did.
///
/// TWO LISTS, NOT ONE. A name that was already there is not a name this call
/// wrote, and the previous shape put both into one `created` vector — which is
/// how a rotation that rotated nothing came to answer 200.
#[derive(Debug, Default)]
struct CredentialOutcome {
    /// Secrets this call created.
    created: Vec<String>,
    /// Secrets an earlier attempt of this same request had already created.
    replayed: Vec<String>,
}

/// Create every `secret.new` value the request entered, after the destination
/// exists so the Secrets can be owned by it.
///
/// # Errors
///
/// `state_conflict` when a Secret under the deterministic name already exists
/// and `on_existing` is [`ExistingCredential::Refuse`], or the adapter's
/// failure.
async fn create_new_credentials(
    state: &AppState,
    namespace: &str,
    destination: &BackupDestination,
    access: &AccessRequest,
    request_id: &str,
    on_existing: ExistingCredential,
) -> Result<CredentialOutcome, ApiError> {
    let mut outcome = CredentialOutcome::default();
    let destination_name = destination.meta().name.clone().unwrap_or_default();
    let grants: [(DestinationRoleDto, Option<&AccessGrantRequest>); 4] = [
        (
            DestinationRoleDto::ArchiveWrite,
            Some(&access.archive_write),
        ),
        (
            DestinationRoleDto::ArchiveRead,
            access.archive_read.as_ref(),
        ),
        (
            DestinationRoleDto::EvidenceWrite,
            access.evidence_write.as_ref(),
        ),
        (
            DestinationRoleDto::EvidenceRead,
            access.evidence_read.as_ref(),
        ),
    ];
    for (role, grant) in grants {
        let Some(grant) = grant else { continue };
        let Some(new) = grant.secret.as_ref().and_then(|s| s.new.as_ref()) else {
            continue;
        };
        let secret = build_credential_secret(namespace, destination, role, new, request_id);
        match state.kube().create_credential(namespace, &secret).await {
            Ok(reference) => {
                // THE LOG LINE IS A NAME. Not a key, not a length, not a
                // prefix: the only non-secret fact about a credential is which
                // object holds it.
                tracing::info!(
                    namespace,
                    secret = %reference.name,
                    role = role_slug(role),
                    "credential secret created"
                );
                outcome.created.push(reference.name);
            }
            Err(KubeFailure::AlreadyExists) => {
                let name = credential_secret_name(&destination_name, role);
                match on_existing {
                    ExistingCredential::AcceptAsReplay => outcome.replayed.push(name),
                    ExistingCredential::Refuse => {
                        tracing::warn!(
                            namespace,
                            secret = %name,
                            role = role_slug(role),
                            "credential secret already exists; the entered value was NOT written"
                        );
                        // THE MESSAGE NAMES WHAT WAS ALREADY WRITTEN. The
                        // pre-flight above makes this arm reachable only when
                        // the name was taken BETWEEN the probe and the write —
                        // a race, not the ordinary case — and in that window an
                        // earlier role may already be live. Saying "nothing was
                        // changed" there would be false, and it is what the
                        // operator needs to clean up before retrying.
                        let already = if outcome.created.is_empty() {
                            " Nothing was changed.".to_string()
                        } else {
                            format!(
                                " It was taken between this request's check and its write, and \
                                 these Secrets WERE created by this request and must be deleted \
                                 before a retry: {}.",
                                outcome.created.join(", ")
                            )
                        };
                        return Err(ApiError::new(
                            ProblemCode::StateConflict,
                            format!(
                                "The Secret `{name}` already exists, so the credential you \
                                 entered was NOT written.{already} This service holds `create` \
                                 on Secrets and nothing else: it cannot read the existing value \
                                 to compare it, and it cannot overwrite it. Rotate `{name}`'s \
                                 content out of band (kubectl, or your secret manager), or point \
                                 this grant at a different existing Secret with \
                                 `secret.existing`."
                            ),
                        ));
                    }
                }
            }
            Err(other) => return Err(other.into_api_error()),
        }
    }
    Ok(outcome)
}

/// Record on the audit line which Secrets a call wrote, and which it found.
fn note_credentials(actor: &Actor, outcome: &CredentialOutcome) {
    if !outcome.created.is_empty() {
        actor
            .audit
            .note("credentialSecretsCreated", &outcome.created.join(","));
    }
    if !outcome.replayed.is_empty() {
        actor
            .audit
            .note("credentialSecretsReplayed", &outcome.replayed.join(","));
    }
}

// ======================================================================
// Routes
// ======================================================================

/// `GET .../destinations`.
pub async fn list(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    actor: Actor,
    ApiPath(ns): ApiPath<String>,
    uri: Uri,
) -> Result<Response, ApiError> {
    authorize(&state, &actor, &ns, Action::ReadDestinations)?;
    let query = list_query(uri.query())?;
    let (items, page) =
        list_page::<BackupDestination>(&state, &actor, &ns, ROUTE_LIST, &query).await?;
    Ok(json(
        StatusCode::OK,
        &DestinationList {
            request_id,
            items: items.iter().map(summary).collect(),
            page,
        },
    ))
}

/// The newest `DestinationAccess` preflight for one destination.
///
/// ONE SELECTOR, PAGED TO EXHAUSTION, BOUNDED. The selector is
/// [`DESTINATION_TEST_LABEL`], which only `:test` sets, so every object the
/// pages return is an access test for this destination; the loop follows the
/// continue token so a namespace with many tests cannot hide the newest one
/// behind a first page in name order; and [`MAX_PAGES`] caps the work, with
/// the cap REPORTED rather than swallowed — a `lastTest` that might not be the
/// last is worse than none, so the caller is told.
async fn last_test(
    state: &AppState,
    namespace: &str,
    name: &str,
) -> Result<Option<LastTestView>, ApiError> {
    let selector = format!("{DESTINATION_TEST_LABEL}={name}");
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
            .list::<PreflightCr>(namespace, &page)
            .await
            .map_err(KubeFailure::into_api_error)?;
        for item in list.items {
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
    Ok(newest.map(|p| {
        let projected = super::preflights::project(&p, now, None);
        LastTestView {
            preflight_id: projected.id.clone(),
            state: projected.state,
            observed_at: projected.observed_at,
            stale: projected.stale,
            truncated,
        }
    }))
}

/// `GET .../destinations/{name}`.
pub async fn get_one(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    actor: Actor,
    ApiPath((ns, name)): ApiPath<(String, String)>,
    uri: Uri,
) -> Result<Response, ApiError> {
    authorize(&state, &actor, &ns, Action::ReadDestinations)?;
    crate::http::parse_query(uri.query(), &[])?;
    let object = get_object::<BackupDestination>(&state, &actor, &ns, &name).await?;
    let test = last_test(&state, &ns, &name).await?;
    Ok(json(
        StatusCode::OK,
        &DestinationResponse {
            request_id,
            replayed: None,
            item: project(&object, test),
        },
    ))
}

/// `GET .../destinations/{name}/usage`.
pub async fn usage(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    actor: Actor,
    ApiPath((ns, name)): ApiPath<(String, String)>,
    uri: Uri,
) -> Result<Response, ApiError> {
    authorize(&state, &actor, &ns, Action::ReadDestinations)?;
    crate::http::parse_query(uri.query(), &[])?;
    check_name(&name)?;
    let page = PageRequest {
        limit: MAX_USAGE_ENTRIES as u32 + 1,
        continue_token: None,
        label_selector: Some(format!("{DESTINATION_LABEL}={name}")),
    };
    let schedules = state
        .kube()
        .list::<BackupSchedule>(&ns, &page)
        .await
        .map_err(KubeFailure::into_api_error)?;
    let backups = state
        .kube()
        .list::<BackupCr>(&ns, &page)
        .await
        .map_err(KubeFailure::into_api_error)?;
    let truncated =
        schedules.items.len() > MAX_USAGE_ENTRIES || backups.items.len() > MAX_USAGE_ENTRIES;
    let rows = |kind: &str, items: &[ObjectMeta]| -> Vec<DestinationUseView> {
        items
            .iter()
            .take(MAX_USAGE_ENTRIES)
            .map(|m| DestinationUseView {
                kind: kind.to_string(),
                name: m.name.clone().unwrap_or_default(),
                created_at: m.creation_timestamp.as_ref().map(|t| t.0),
            })
            .collect()
    };
    let schedule_meta: Vec<ObjectMeta> =
        schedules.items.iter().map(|s| s.metadata.clone()).collect();
    let backup_meta: Vec<ObjectMeta> = backups.items.iter().map(|b| b.metadata.clone()).collect();
    Ok(json(
        StatusCode::OK,
        &DestinationUsageResponse {
            request_id,
            name,
            schedules: rows("BackupSchedule", &schedule_meta),
            backups: rows("Backup", &backup_meta),
            truncated,
            // THE BASIS IS PART OF THE ANSWER. An empty list means "no object
            // carries this label", which is not the same as "nothing uses this
            // destination": an object created outside this API, or before the
            // label existed, is not listed. Saying so is the difference
            // between a bounded answer and a wrong one.
            basis: format!(
                "objects in this namespace labelled {DESTINATION_LABEL}=<name>, which the API \
                 sets on the objects it creates. Objects created outside the API are not listed."
            ),
        },
    ))
}

/// `POST .../destinations`.
pub async fn create(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    actor: Actor,
    ApiPath(ns): ApiPath<String>,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, ApiError> {
    authorize(&state, &actor, &ns, Action::ManageDestinations)?;
    crate::http::parse_query(uri.query(), &[])?;
    let key = IdempotencyKey::from_headers(&headers)?;
    let mut request: CreateDestinationRequest = read_json(body, MAX_JSON_BODY).await?;
    validate_create(&request)?;
    if request.access_carries_a_value() {
        authorize_also(&state, &actor, &ns, Action::WriteCredential)?;
    }
    // Canonical form: an omitted probe and an omitted default are the values
    // the API applies, so both spellings hash identically and replay as one.
    request.readiness = Some(crate::contract::ReadinessRequest {
        write_probe: Some(
            request
                .readiness
                .as_ref()
                .and_then(|r| r.write_probe)
                .unwrap_or(WriteProbeDto::CreateOnlyMarker),
        ),
    });
    request.default = Some(request.default.unwrap_or(false));
    if request.default == Some(true) {
        refuse_second_default(&state, &ns, &request.name).await?;
    }
    // THE REPLAY QUESTION IS ASKED FIRST, AND ONLY OF THIS SERVICE'S OWN
    // RECORD. `created.replayed` cannot answer it here: it says a destination
    // already existed under this scope, which — with the destination written
    // before the credentials — is exactly what the FIRST attempt leaves behind
    // when it refuses a foreign Secret. Deciding from it meant the retry this
    // API's own documentation prescribes adopted the value that attempt had
    // just refused.
    let replaying = is_own_replay::<BackupDestination, _>(
        &state,
        &actor,
        &ns,
        ROUTE_CREATE,
        &request.name,
        &key,
        &request,
    )
    .await?;
    // AND NOTHING IS WRITTEN UNTIL EVERY NAME IS KNOWN TO BE FREE. Not the
    // destination, and not the first of four Secrets.
    if !replaying {
        refuse_taken_credential_names(&state, &ns, &request.name, &request.access).await?;
    }
    let created = create_named_idempotent(
        &state,
        &actor,
        &ns,
        ROUTE_CREATE,
        &request.name,
        &key,
        &request_id,
        &request,
        |name, annotations| build(&ns, name, annotations, &request),
    )
    .await?;
    let outcome = create_new_credentials(
        &state,
        &ns,
        &created.object,
        &request.access,
        &request_id,
        if replaying {
            ExistingCredential::AcceptAsReplay
        } else {
            ExistingCredential::Refuse
        },
    )
    .await?;
    note_credentials(&actor, &outcome);
    Ok(json(
        created.status(),
        &DestinationResponse {
            request_id,
            replayed: Some(created.replayed),
            item: project(&created.object, None),
        },
    ))
}

/// At most one destination per namespace is the default.
///
/// ONE BOUNDED SELECTOR READ. The previous shape listed a single 200-object
/// page with no continuation and searched it for an annotation, so a namespace
/// with more than 200 destinations could accept a second default — precisely
/// the state the refusal below calls impossible. Selecting on
/// [`DEFAULT_LABEL`] returns only the holders, of which there is at most one
/// in a namespace this API has managed alone.
///
/// THE RESIDUAL, STATED: a default set by hand with only the annotation D2
/// §3.1 names carries no label, so this check does not see it. It still reads
/// as the default in every projection ([`is_default`] honours either
/// spelling), and `docs/api.md` says so.
///
/// # Errors
///
/// `state_conflict` naming the destination that already holds it.
async fn refuse_second_default(
    state: &AppState,
    namespace: &str,
    name: &str,
) -> Result<(), ApiError> {
    let page = PageRequest {
        limit: MAX_LIMIT,
        continue_token: None,
        label_selector: Some(format!("{DEFAULT_LABEL}=true")),
    };
    let list = state
        .kube()
        .list::<BackupDestination>(namespace, &page)
        .await
        .map_err(KubeFailure::into_api_error)?;
    let holder = list
        .items
        .iter()
        .find(|d| d.meta().name.as_deref() != Some(name));
    if let Some(holder) = holder {
        return Err(ApiError::new(
            ProblemCode::StateConflict,
            format!(
                "`{}` is already this namespace's default destination. Clear it there before \
                 naming another: a namespace with two defaults has none.",
                holder.meta().name.clone().unwrap_or_default()
            ),
        ));
    }
    Ok(())
}

impl CreateDestinationRequest {
    fn access_carries_a_value(&self) -> bool {
        access_carries_a_value(&self.access)
    }
}

pub(crate) fn access_carries_a_value(access: &AccessRequest) -> bool {
    [
        Some(&access.archive_write),
        access.archive_read.as_ref(),
        access.evidence_write.as_ref(),
        access.evidence_read.as_ref(),
    ]
    .into_iter()
    .flatten()
    .any(|g| g.secret.as_ref().is_some_and(|s| s.new.is_some()))
}

/// `POST .../destinations/{name}:update-access`.
#[allow(clippy::too_many_arguments)]
pub async fn update_access(
    state: AppState,
    request_id: String,
    actor: Actor,
    ns: String,
    name: String,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, ApiError> {
    authorize(&state, &actor, &ns, Action::ManageDestinations)?;
    check_name(&name)?;
    crate::http::parse_query(uri.query(), &[])?;
    IdempotencyKey::refuse_on(&headers, ROUTE_UPDATE_ACCESS, Some("expectedGeneration"))?;
    let request: UpdateDestinationAccessRequest = read_json(body, MAX_JSON_BODY).await?;
    let mut errors = Vec::new();
    validate_access(&request.access, &mut errors);
    if let Some(transport) = &request.transport {
        if let Some(bundle) = &transport.ca_bundle {
            check_secret_name(
                "transport.caBundle.configMapName",
                &bundle.config_map_name,
                &mut errors,
            );
            if let Some(key) = &bundle.key {
                check_data_key("transport.caBundle.key", key, &mut errors);
            }
        }
    }
    if !errors.is_empty() {
        return Err(ApiError::destination_invalid(errors));
    }
    if access_carries_a_value(&request.access) {
        authorize_also(&state, &actor, &ns, Action::WriteCredential)?;
    }
    let object = get_object::<BackupDestination>(&state, &actor, &ns, &name).await?;
    let generation = object.meta().generation.unwrap_or_default();
    if generation != request.expected_generation {
        return Err(ApiError::new(
            ProblemCode::PreconditionFailed,
            format!(
                "expectedGeneration {} is not the destination's current generation {generation}; \
                 read it again.",
                request.expected_generation
            ),
        ));
    }
    // A CA BUNDLE ON A PLAINTEXT DESTINATION IS REFUSED HERE TOO (R4). The
    // stored transport is the only one there will ever be, so the rule is
    // evaluated against it and not against anything the request claims.
    if request
        .transport
        .as_ref()
        .is_some_and(|t| t.ca_bundle.is_some())
        && object.spec.transport.security != core_destination::TransportSecurity::Tls
    {
        return Err(ApiError::new(
            ProblemCode::TransportDowngradeForbidden,
            "transport.caBundle requires transport.security TLS, and transport.security is \
             immutable: a plaintext destination cannot be given trust material.",
        ));
    }
    // EVERY NAME FIRST, THEN THE WRITES, THEN THE PATCH. A rotation that
    // cannot write one of its values must change nothing at all — not the
    // destination, and not the credential of some other role that happened to
    // come earlier in the loop.
    //
    // `:update-access` carries no `Idempotency-Key`, so there is no record
    // that could make an existing name this request's own: it is always a
    // refusal.
    refuse_taken_credential_names(&state, &ns, &name, &request.access).await?;
    let outcome = create_new_credentials(
        &state,
        &ns,
        &object,
        &request.access,
        &request_id,
        ExistingCredential::Refuse,
    )
    .await?;
    let access = build_access(&name, &request.access);
    let access_json = serde_json::json!({
        "archiveWrite": access.archive_write,
        "archiveRead": access.archive_read,
        "evidenceWrite": access.evidence_write,
        "evidenceRead": access.evidence_read,
    });
    let ca_bundle = request.transport.as_ref().map(|t| match &t.ca_bundle {
        None => serde_json::Value::Null,
        Some(bundle) => {
            serde_json::to_value(build_ca_bundle(bundle)).unwrap_or(serde_json::Value::Null)
        }
    });
    let version = object.meta().resource_version.clone().unwrap_or_default();
    let updated = state
        .kube()
        .set_destination_access(&ns, &name, &access_json, ca_bundle, &version)
        .await
        .map_err(|failure| match failure {
            KubeFailure::Conflict => ApiError::new(
                ProblemCode::PreconditionFailed,
                "The destination changed between the read and the rotation; read it again.",
            ),
            KubeFailure::Invalid | KubeFailure::BadRequest => ApiError::new(
                ProblemCode::DestinationLocationImmutable,
                "Kubernetes refused the rotation. spec.storage and spec.transport.security are \
                 immutable: a different location or transport is a different destination.",
            ),
            other => other.into_api_error(),
        })?;
    note_credentials(&actor, &outcome);
    tracing::info!(
        namespace = %ns,
        name = %name,
        actor = %actor.id(),
        "destination access rotated"
    );
    Ok(json(
        StatusCode::OK,
        &DestinationResponse {
            request_id,
            replayed: None,
            item: project(&updated, None),
        },
    ))
}

/// `POST .../destinations/{name}:test`.
#[allow(clippy::too_many_arguments)]
pub async fn test(
    state: AppState,
    request_id: String,
    actor: Actor,
    ns: String,
    name: String,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, ApiError> {
    authorize(&state, &actor, &ns, Action::ManageDestinations)?;
    check_name(&name)?;
    crate::http::parse_query(uri.query(), &[])?;
    let key = IdempotencyKey::from_headers(&headers)?;
    let request: TestDestinationRequest = read_json(body, MAX_JSON_BODY).await?;
    let object = get_object::<BackupDestination>(&state, &actor, &ns, &name).await?;
    let roles = match &request.roles {
        Some(roles) if roles.is_empty() => {
            return Err(ApiError::validation(vec![FieldError::new(
                "roles",
                "count_out_of_range",
                "name between 1 and 4 roles, or omit the field for every configured role",
            )]))
        }
        Some(roles) => {
            let mut roles = roles.clone();
            roles.sort();
            roles.dedup();
            roles
        }
        None => configured_roles(&object),
    };
    let created = super::preflights::start_destination_access(
        &state,
        &actor,
        &ns,
        &name,
        &roles,
        &key,
        &request_id,
    )
    .await?;
    Ok(json(
        StatusCode::ACCEPTED,
        &PreflightResponse {
            request_id,
            replayed: Some(created.replayed),
            item: super::preflights::project(&created.object, state.now(), None),
        },
    ))
}

fn configured_roles(object: &BackupDestination) -> Vec<DestinationRoleDto> {
    let mut roles = vec![DestinationRoleDto::ArchiveWrite];
    if object.spec.access.archive_read.is_some() {
        roles.push(DestinationRoleDto::ArchiveRead);
    }
    if object.spec.access.evidence_write.is_some() {
        roles.push(DestinationRoleDto::EvidenceWrite);
    }
    if object.spec.access.evidence_read.is_some() {
        roles.push(DestinationRoleDto::EvidenceRead);
    }
    roles
}

// ======================================================================
// Legacy adoption (D2 §3.12)
// ======================================================================

/// D2 §3.12 STEP 2 HAS THREE BRANCHES, AND THIS ROUTE IS ON (c).
///
/// (a) is the PLAT-06.1 frozen `execution-inputs.json` of the newest succeeded
/// `Backup` — the one place that records the exact bucket, prefix, endpoint,
/// region, `path_style` and `allow_http` a runner actually used. (b) is the
/// `archive.url` plus the installation's legacy addressing, published in the
/// policy `ConfigMap` W11 renders, and it requires the operator to confirm
/// because a global setting is not a fact about this archive.
///
/// NEITHER IS READABLE HERE. Both live in `ConfigMap`s, and the sealed adapter
/// reads a `ConfigMap` only when a CHECK owns it (owner UID, immutability and
/// digest all verified); an execution input and a policy document are owned by
/// neither. So this route takes (c) and refuses.
///
/// WHY REFUSING BEATS DERIVING. A legacy `archive.url` carries the bucket and
/// the prefix and nothing else. An installation whose runs used MinIO
/// (`endpoint: https://minio…:9000`, path-style) and one that used AWS S3
/// produce the SAME url, so any endpoint, region, addressing or transport this
/// route filled in would be a guess — and `locationDigest`, which restore
/// selection and catalog indexing compare, is computed over exactly those
/// fields. A destination adopted at the wrong location is worse than no
/// destination: it is a location that looks derived from facts.
///
/// [`SOURCE_FROZEN_EXECUTION`] and [`SOURCE_INSTALLATION_CONFIG`] are the two
/// provenance values `addressingSource` will carry when (a) and (b) land. No
/// route emits either today, and a provenance label naming a source that was
/// not read is the defect this replaced.
pub const SOURCE_FROZEN_EXECUTION: &str = "frozenExecution";
/// See [`SOURCE_FROZEN_EXECUTION`].
pub const SOURCE_INSTALLATION_CONFIG: &str = "installationConfig";

/// The bucket and the prefix — the only two facts a legacy `archive.url`
/// carries — checked so that the refusal below is about the missing SOURCE and
/// not about an unreadable URL.
///
/// # Errors
///
/// `legacy_location_unknown` for a scheme this adoption cannot describe or a
/// URL that names no bucket.
fn legacy_location_facts(url: &str) -> Result<(String, String), ApiError> {
    let Some(rest) = url.strip_prefix("s3://") else {
        return Err(ApiError::new(
            ProblemCode::LegacyLocationUnknown,
            "Only an s3:// archive can become a BackupDestination; this one names another \
             scheme.",
        ));
    };
    let mut parts = rest.splitn(2, '/');
    let bucket = parts.next().unwrap_or_default().to_string();
    let prefix = parts
        .next()
        .unwrap_or_default()
        .trim_matches('/')
        .to_string();
    if bucket.is_empty() {
        return Err(ApiError::new(
            ProblemCode::LegacyLocationUnknown,
            "The legacy archive URL names no bucket.",
        ));
    }
    Ok((bucket, prefix))
}

/// The refusal, with the bucket and prefix that WERE recovered named in it, so
/// an operator can paste them straight into `POST .../destinations`.
fn refuse_until_a_source_is_readable(bucket: &str, prefix: &str) -> ApiError {
    let location = if prefix.is_empty() {
        format!("s3://{bucket}")
    } else {
        format!("s3://{bucket}/{prefix}")
    };
    ApiError::new(
        ProblemCode::LegacyLocationUnknown,
        format!(
            "The legacy archive at `{location}` gives its bucket and prefix and nothing else. A \
             BackupDestination also needs the endpoint, the region, the addressing and the \
             transport, and this build can read neither source that records them: the frozen \
             execution inputs of a succeeded Backup (PLAT-06.1) nor the installation's legacy \
             addressing in the policy ConfigMap (D2 W11). Those are facts about how the runs \
             actually reached the store, and guessing them would move the location. Create the \
             destination explicitly with `POST /api/v1/namespaces/{{ns}}/destinations`, naming \
             this bucket and prefix and the endpoint your runs used; adoption from the \
             installation config arrives with W11."
        ),
    )
}

/// `POST .../destinations:from-legacy`.
pub async fn from_legacy(
    State(state): State<AppState>,
    RequestId(_request_id): RequestId,
    actor: Actor,
    ApiPath(ns): ApiPath<String>,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, ApiError> {
    authorize(&state, &actor, &ns, Action::ManageDestinations)?;
    crate::http::parse_query(uri.query(), &[])?;
    // THE KEY IS STILL REQUIRED AND STILL VALIDATED. This is a durable POST by
    // contract; a client that forgets the header learns that here rather than
    // discovering it after W11 makes the route create something.
    let _key = IdempotencyKey::from_headers(&headers)?;
    let request: DestinationFromLegacyRequest = read_json(body, MAX_JSON_BODY).await?;
    let mut errors = Vec::new();
    if !validate::is_dns_subdomain(&request.name) || request.name.len() > 63 {
        errors.push(FieldError::new(
            "name",
            "invalid_name",
            "a destination name is a Kubernetes object name of at most 63 characters",
        ));
    }
    match (&request.source_schedule, &request.source_backup) {
        (Some(_), Some(_)) | (None, None) => errors.push(FieldError::new(
            "sourceSchedule",
            "grant_mode_fields",
            "name exactly one of sourceSchedule or sourceBackup",
        )),
        _ => {}
    }
    validate_access(&request.access, &mut errors);
    if !errors.is_empty() {
        return Err(ApiError::destination_invalid(errors));
    }
    if access_carries_a_value(&request.access) {
        authorize_also(&state, &actor, &ns, Action::WriteCredential)?;
    }
    let legacy_url = if let Some(schedule) = &request.source_schedule {
        let object = get_object::<BackupSchedule>(&state, &actor, &ns, schedule).await?;
        object.spec.archive.url.clone()
    } else {
        let name = request.source_backup.clone().unwrap_or_default();
        let object = get_object::<BackupCr>(&state, &actor, &ns, &name).await?;
        object.spec.archive.url.clone()
    };
    let (bucket, prefix) = legacy_location_facts(&legacy_url)?;
    // NOTHING IS CREATED. The idempotency key was validated, the legacy object
    // was read and the grants were checked — so the operator learns the
    // request itself is well formed — and then the route refuses, because the
    // one thing it would have to invent is the one thing that decides where
    // the archive is.
    Err(refuse_until_a_source_is_readable(&bucket, &prefix))
}

/// The command dispatcher for `POST .../destinations/{target}`, where `target`
/// is `<name>:update-access`, `<name>:test` or the collection command
/// `destinations:from-legacy` (which arrives on the collection path).
#[allow(clippy::too_many_arguments)]
pub async fn command(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    actor: Actor,
    ApiPath((ns, target)): ApiPath<(String, String)>,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, ApiError> {
    if let Some(name) = target.strip_suffix(UPDATE_ACCESS) {
        return update_access(
            state,
            request_id,
            actor,
            ns,
            name.to_string(),
            uri,
            headers,
            body,
        )
        .await;
    }
    if let Some(name) = target.strip_suffix(TEST) {
        return test(
            state,
            request_id,
            actor,
            ns,
            name.to_string(),
            uri,
            headers,
            body,
        )
        .await;
    }
    Err(ApiError::new(ProblemCode::NotFound, "No such command."))
}

/// The conditions of a destination, bounded.
#[must_use]
pub fn conditions(object: &BackupDestination) -> Vec<crate::contract::ConditionView> {
    object
        .status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .map(|c| c.iter().take(MAX_CONDITIONS).map(condition_view).collect())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_request_field_path_names_the_request_and_not_the_crd() {
        assert_eq!(request_field("spec.storage.bucket"), "storage.bucket");
        assert_eq!(request_field("storage.bucket"), "storage.bucket");
    }

    #[test]
    fn credential_secret_names_follow_the_decision() {
        assert_eq!(
            credential_secret_name("primary", DestinationRoleDto::ArchiveRead),
            "lwd-primary-archive-read"
        );
        for role in [
            DestinationRoleDto::ArchiveWrite,
            DestinationRoleDto::ArchiveRead,
            DestinationRoleDto::EvidenceWrite,
            DestinationRoleDto::EvidenceRead,
        ] {
            let name = credential_secret_name(&"d".repeat(63), role);
            assert!(validate::is_dns_subdomain(&name), "{name}");
        }
    }
}
