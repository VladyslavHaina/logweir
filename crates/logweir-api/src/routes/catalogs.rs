//! `GET`/`POST /api/v1/namespaces/{ns}/catalogs`, plus the point view and the
//! signer panel — PLAT-15.1's read half and PLAT-15.2's "connect an existing
//! archive".
//!
//! # Availability and verification are two columns, never one
//!
//! D3 §5.4 keeps "can these bytes be read" and "does the evidence verify under
//! a key this installation accepts" apart, and this route publishes both, plus
//! the conjunction the controller already materialised (`selectable`). A
//! single "ok" column would have to choose which of `Missing` and
//! `UntrustedSigner` to call the same thing as the other, and both choices are
//! wrong: one is an outage and one is a stranger's signature.
//!
//! # The untrusted-signer panel offers no button
//!
//! `signers[]` publishes the key id — the SHA-256 of the DER SPKI, the number
//! `openssl` prints — its point count and whether the bound policy trusts it.
//! There is no "trust this key" route here and there is not going to be one in
//! v1: `docs/keys.md`'s rule is that a key found beside an archive is never
//! trusted by proximity, and one-click trust is proximity with a confirmation
//! dialog on it. An administrator adds the key with `kubectl apply` after
//! comparing the fingerprint out of band.
//!
//! # The view is a WINDOW, and the response says so
//!
//! The durable truth is in object storage. Kubernetes holds the newest
//! `sync.viewLimit` points in immutable `ConfigMap` pages owned by the sync
//! Job; when the Job's TTL collects it, the pages go with it and the catalog
//! reports `Stale` and then an expired view. So every point page carries
//! `truncated`, `viewExpiresAt` and the generation it was read from, and a
//! cursor is bound to that generation: a view that was replaced under a
//! paging client is `cursor_expired` with "restart the list", never a
//! silently different page.

use axum::body::Body;
use axum::extract::State;
use axum::response::Response;
use chrono::{DateTime, Utc};
use http::{HeaderMap, StatusCode, Uri};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::ResourceExt;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use weirkeeper::catalog_view::{ViewEntry, PAGE_DATA_KEY, PAGE_DIGEST_ANNOTATION};
use weirkeeper::crds::recovery_catalog::{
    DeepCheck, RecoveryCatalog, RecoveryCatalogSpec, SyncMode, SyncSettings,
};
use weirkeeper::crds::{ArchiveRef, LocalRef};

use super::{
    authorize, check_name, create_named_idempotent, get_object, json, list_page, list_query,
    ApiPath,
};
use crate::app::AppState;
use crate::auth::Actor;
use crate::authz::Action;
use crate::contract::{ArchiveRequest, ConditionView, NameRef, Page};
use crate::cursor::{self, CursorError, CursorScope};
use crate::http::{read_json, RequestId, MAX_JSON_BODY};
use crate::idempotency::IdempotencyKey;
use crate::kube::{KubeFailure, ResultDocument};
use crate::problem::{ApiError, FieldError, ProblemCode};
use crate::status::condition_view;
use crate::validate::{self, bounded};

/// The list route identifier (cursor scope).
pub const ROUTE_LIST: &str = "GET /api/v1/namespaces/{ns}/catalogs";
/// The create route identifier (idempotency scope).
pub const ROUTE_CREATE: &str = "POST /api/v1/namespaces/{ns}/catalogs";
/// The point-page route identifier (cursor scope).
pub const ROUTE_POINTS: &str = "GET /api/v1/namespaces/{ns}/catalogs/{name}/points";

/// The largest point page.
pub const MAX_POINT_PAGE: u32 = 200;
/// The most page `ConfigMap`s one request will read. D3 §10's bound.
pub const MAX_PAGES_PER_REQUEST: usize = 8;
/// The most signers a response carries.
pub const MAX_SIGNERS: usize = 16;
/// The most histogram days a response carries.
pub const MAX_HISTOGRAM_DAYS: usize = 400;

// ======================================================================
// The catalog projection
// ======================================================================

/// What the last sync found, by verdict. **Ten counts, not two**: "how many
/// points are there" and "how many can I restore from" are different numbers.
#[derive(Clone, Copy, Debug, Default, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CatalogCountsView {
    /// Every point the walk saw.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total: Option<i64>,
    /// Readable, digest-matching, selectable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub available: Option<i64>,
    /// A definite `NotFound`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub missing: Option<i64>,
    /// Present but unreadable — NOT the same as absent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unreadable: Option<i64>,
    /// No signature verdict was reached.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unverified: Option<i64>,
    /// Signed by a key the bound policy does not trust.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub untrusted_signer: Option<i64>,
    /// The signature did not verify.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub invalid: Option<i64>,
    /// Two records disagree about one point.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conflict: Option<i64>,
    /// Recorded as deleted by an attributable retention run.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deleted: Option<i64>,
    /// Written by a format this build does not understand.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unsupported_format: Option<i64>,
}

/// How many points a day holds.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct HistogramBucketView {
    /// `YYYY-MM-DD`.
    pub day: String,
    /// How many points.
    pub points: i64,
}

/// Who signed the points in this catalog — the untrusted-signer panel's data.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SignerView {
    /// The key id: the SHA-256 of the DER SPKI, lowercase hex. Compare it out
    /// of band; there is no one-click trust.
    pub key_id: String,
    /// A hint at the principal, taken from the record. **Never authority.**
    #[serde(skip_serializing_if = "Option::is_none")]
    pub principal_hint: Option<String>,
    /// How many points it signed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub points: Option<i64>,
    /// Whether the bound `TrustPolicy` accepts it. Absent means the catalog
    /// could not tell, which is not `false` and is certainly not `true`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trusted: Option<bool>,
}

/// The sync Job that produced this view.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct LastSyncView {
    /// When it started.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<DateTime<Utc>>,
    /// When it finished.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<DateTime<Utc>>,
    /// Its exit code.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    /// Its refusal reason, for an exit 3.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refusal_reason: Option<String>,
}

/// Where the last walk got to.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SyncCursorView {
    /// The day shard the next `Index` sync resumes from.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_shard: Option<String>,
    /// Whether the walk finished. `false` is a budgeted walk that will
    /// continue, not a failure.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub complete: Option<bool>,
}

/// A `RecoveryCatalog`, projected.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CatalogView {
    /// The object name.
    pub name: String,
    /// The namespace.
    pub namespace: String,
    /// The Kubernetes UID.
    pub uid: String,
    /// The resourceVersion this projection was read at.
    pub resource_version: String,
    /// `metadata.generation`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub generation: Option<i64>,
    /// When the object was created.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<DateTime<Utc>>,
    /// The saved destination it indexes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub destination_ref: Option<NameRef>,
    /// The inline archive, with any userinfo redacted and the credential as a
    /// Secret NAME.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub legacy_archive: Option<crate::contract::ArchiveView>,
    /// How often it syncs; `0` is manual only.
    pub interval_seconds: i32,
    /// `Index` or `Full`.
    pub mode: String,
    /// `None`, `ManifestDigest` or `SegmentSample`.
    pub deep_check: String,
    /// How many newest points are materialised into Kubernetes.
    pub view_limit: i32,
    /// The generation the view was built from.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    /// The `syncRequest` token the controller has acted on. A request whose
    /// token is not this one has not been served yet.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_sync_request: Option<String>,
    /// When the view was last refreshed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub synced_at: Option<DateTime<Utc>>,
    /// When the pages age out with their Job. After it the view is gone and
    /// this object says so rather than reporting an empty archive.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub view_expires_at: Option<DateTime<Utc>>,
    /// Whether the view has already expired, by this server's clock.
    pub view_expired: bool,
    /// Where the walk got to.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<SyncCursorView>,
    /// What it found.
    pub counts: CatalogCountsView,
    /// Whether the archive holds more points than the view carries.
    pub truncated: bool,
    /// How many points the materialised view actually holds.
    pub view_points: i64,
    /// Points per day.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub histogram: Vec<HistogramBucketView>,
    /// Who signed them.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub signers: Vec<SignerView>,
    /// The last sync Job.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_sync: Option<LastSyncView>,
    /// `Ready`, `Synced`, `Stale` and `TrustAvailable`.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<ConditionView>,
}

/// Project one catalog at `now`.
#[must_use]
pub fn view(catalog: &RecoveryCatalog, now: DateTime<Utc>) -> CatalogView {
    let spec = &catalog.spec;
    let status = catalog.status.as_ref();
    let counts = status.and_then(|s| s.counts.as_ref()).copied();
    CatalogView {
        name: catalog.name_any(),
        namespace: catalog.namespace().unwrap_or_default(),
        uid: catalog.uid().unwrap_or_default(),
        resource_version: catalog.resource_version().unwrap_or_default(),
        generation: catalog.metadata.generation,
        created_at: catalog.metadata.creation_timestamp.as_ref().map(|t| t.0),
        destination_ref: spec.destination_ref.as_ref().map(|r| NameRef {
            name: r.name.clone(),
        }),
        legacy_archive: spec
            .legacy_archive
            .as_ref()
            .map(|a| crate::contract::ArchiveView {
                url: validate::redact_url_userinfo(&a.url),
                credential_ref: a.secret_ref.as_ref().map(|r| NameRef {
                    name: r.name.clone(),
                }),
            }),
        interval_seconds: spec.sync.interval_seconds,
        mode: crate::status::wire_name(&spec.sync.mode),
        deep_check: crate::status::wire_name(&spec.sync.deep_check),
        view_limit: spec.sync.view_limit,
        observed_generation: status.and_then(|s| s.observed_generation),
        observed_sync_request: status
            .and_then(|s| s.observed_sync_request.as_deref())
            .map(|t| bounded(t, 253)),
        synced_at: status.and_then(|s| s.synced_at),
        view_expires_at: status.and_then(|s| s.view_expires_at),
        // AN EXPIRED VIEW IS SAID OUT LOUD. The pages are collected with their
        // Job, so a catalog past this instant answers an EMPTY point list —
        // and an empty list that means "the window aged out" must never read
        // as "the archive holds nothing".
        view_expired: status
            .and_then(|s| s.view_expires_at)
            .is_some_and(|at| now >= at),
        cursor: status
            .and_then(|s| s.cursor.as_ref())
            .map(|c| SyncCursorView {
                index_shard: c.index_shard.clone(),
                complete: c.complete,
            }),
        counts: counts.map_or_else(CatalogCountsView::default, |c| CatalogCountsView {
            total: c.total,
            available: c.available,
            missing: c.missing,
            unreadable: c.unreadable,
            unverified: c.unverified,
            untrusted_signer: c.untrusted_signer,
            invalid: c.invalid,
            conflict: c.conflict,
            deleted: c.deleted,
            unsupported_format: c.unsupported_format,
        }),
        truncated: status.and_then(|s| s.truncated).unwrap_or(false),
        view_points: status
            .and_then(|s| s.pages.as_ref())
            .into_iter()
            .flatten()
            .map(|p| p.count)
            .sum(),
        histogram: status
            .and_then(|s| s.histogram.as_ref())
            .into_iter()
            .flatten()
            .take(MAX_HISTOGRAM_DAYS)
            .map(|b| HistogramBucketView {
                day: bounded(&b.day, 10),
                points: b.points,
            })
            .collect(),
        signers: status
            .and_then(|s| s.signers.as_ref())
            .into_iter()
            .flatten()
            .take(MAX_SIGNERS)
            .map(|s| SignerView {
                key_id: bounded(&s.key_id, 64),
                principal_hint: s.principal_hint.as_deref().map(|p| bounded(p, 253)),
                points: s.points,
                trusted: s.trusted,
            })
            .collect(),
        last_sync: status
            .and_then(|s| s.last_sync_job.as_ref())
            .map(|j| LastSyncView {
                started_at: j.started_at,
                finished_at: j.finished_at,
                exit_code: j.exit_code,
                refusal_reason: j.refusal_reason.as_deref().map(|r| bounded(r, 128)),
            }),
        conditions: status
            .and_then(|s| s.conditions.as_ref())
            .into_iter()
            .flatten()
            .take(crate::status::MAX_CONDITIONS)
            .map(condition_view)
            .collect(),
    }
}

// ======================================================================
// The point view
// ======================================================================

/// One location a point was seen at.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PointLocationView {
    /// `s3://<bucket>/<prefix>` — bucket and prefix only. No endpoint, no
    /// region, no credential.
    pub location_id: String,
    /// What was found THERE.
    pub availability: String,
}

/// One recovery point, with its two verdicts kept apart.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PointView {
    /// The content-derived point identity.
    pub point_id: String,
    /// The backup set it belongs to.
    pub backup_id: String,
    /// The run that produced it.
    pub run_id: String,
    /// Capture start, as an instant.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recovery_point_at: Option<DateTime<Utc>>,
    /// The start of the window it covers.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub covered_from: Option<DateTime<Utc>>,
    /// The end of the window it covers. The restore window's end is
    /// inclusive, so a plan restores to `coveredTo` minus one millisecond.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub covered_to: Option<DateTime<Utc>>,
    /// `Available`, `Missing`, `Unreadable`, `Deleted`, `Conflict`,
    /// `UnsupportedFormat` or `Partial` — the BEST of its locations.
    pub availability: String,
    /// `Verified`, `VerifiedHistorical`, `UntrustedSigner`, `Revoked`,
    /// `Invalid`, `NoEvidence` or `NotAttempted` — the WORST of its locations.
    pub verification: String,
    /// The conjunction of the two, materialised by the controller so no
    /// surface writes D3 §5.4's selection rule a second time.
    pub selectable: bool,
    /// The key that signed it, when one was identified.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signer_key_id: Option<String>,
    /// Every location it was seen at, each with its own availability.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub locations: Vec<PointLocationView>,
    /// What to do about a degraded verdict, from the catalog's own vocabulary.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remedy: Option<String>,
    /// The record's format version.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format_version: Option<String>,
}

fn instant(ms: i64) -> Option<DateTime<Utc>> {
    DateTime::from_timestamp_millis(ms)
}

fn point_view(entry: &ViewEntry) -> PointView {
    PointView {
        point_id: bounded(&entry.point_id, 128),
        backup_id: bounded(&entry.backup_id, 128),
        run_id: bounded(&entry.run_id, 64),
        recovery_point_at: instant(entry.recovery_point_at_ms),
        covered_from: instant(entry.covered_from_ms),
        covered_to: instant(entry.covered_to_ms),
        availability: entry.availability.as_str().to_string(),
        verification: entry.verification.as_str().to_string(),
        selectable: entry.selectable,
        signer_key_id: entry.signer_key_id.as_deref().map(|k| bounded(k, 64)),
        locations: entry
            .locations
            .iter()
            .take(16)
            .map(|l| PointLocationView {
                location_id: bounded(&l.location_id, 512),
                availability: l.availability.as_str().to_string(),
            })
            .collect(),
        remedy: entry.remedy.as_deref().map(|r| bounded(r, 512)),
        format_version: entry.format_version.as_deref().map(|v| bounded(v, 32)),
    }
}

/// One page of the materialised point view.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PointPageResponse {
    /// The request ID.
    pub request_id: String,
    /// The points, newest first, in the order the view materialised them.
    pub items: Vec<PointView>,
    /// Paging. `snapshot` is the view generation this page came from.
    pub page: Page,
    /// Whether the archive holds more points than the view carries.
    pub truncated: bool,
    /// Whether the view has aged out. An empty `items` with this `true` means
    /// "the window is gone", never "the archive is empty".
    pub view_expired: bool,
    /// When the view ages out.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub view_expires_at: Option<DateTime<Utc>>,
}

/// The signer panel.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SignerPageResponse {
    /// The request ID.
    pub request_id: String,
    /// The signers.
    pub items: Vec<SignerView>,
    /// How many points carry a signature the bound policy does not accept.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub untrusted_points: Option<i64>,
    /// The out-of-band comparison an administrator runs before adding a key.
    /// A command, not an action this service offers.
    pub fingerprint_command: String,
}

/// The documented way to check a key id out of band (`docs/keys.md`).
pub const FINGERPRINT_COMMAND: &str =
    "openssl pkey -pubin -in <key>.pem -outform DER | openssl dgst -sha256";

// ======================================================================
// Envelopes and the create request
// ======================================================================

/// A page of catalogs.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CatalogList {
    /// The request ID.
    pub request_id: String,
    /// The items on this page.
    pub items: Vec<CatalogView>,
    /// Paging.
    pub page: Page,
}

/// One catalog.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CatalogResponse {
    /// The request ID.
    pub request_id: String,
    /// Whether this replays an earlier identical request (HTTP 200) rather
    /// than creating (HTTP 201).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replayed: Option<bool>,
    /// The item.
    pub item: CatalogView,
}

/// `POST .../catalogs` — connect an archive.
///
/// THE NAME IS THE CALLER'S, like a destination's and for the same reason: a
/// `ProtectionPolicy`, a `RehearsalSchedule` and a `RetentionPolicy` all
/// reference a catalog by name, and a hashed name would make every one of
/// those references unreadable.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ConnectArchiveRequest {
    /// The catalog's name, a DNS-1123 subdomain.
    pub name: String,
    /// The saved destination to index.
    #[serde(default)]
    pub destination_ref: Option<NameRef>,
    /// An archive that predates saved destinations. Exactly one of this and
    /// `destinationRef`.
    #[serde(default)]
    pub legacy_archive: Option<ArchiveRequest>,
    /// `index` or `full`. Connecting an existing archive wants `full`: an
    /// index walk only reads shards a Logweir installation wrote.
    pub sync_mode: ConnectSyncMode,
    /// Seconds between syncs; `0` is manual only. Absent takes the CRD's
    /// default.
    #[serde(default)]
    pub interval_seconds: Option<i32>,
    /// How hard to check each point. Absent takes the CRD's default.
    #[serde(default)]
    pub deep_check: Option<ConnectDeepCheck>,
    /// How many newest points are materialised. Absent takes the CRD's
    /// default.
    #[serde(default)]
    pub view_limit: Option<i32>,
}

/// The request spelling of [`SyncMode`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum ConnectSyncMode {
    /// Day shards at or after the recorded cursor.
    Index,
    /// A full, resumable rescan of receipts and manifests.
    Full,
}

/// The request spelling of [`DeepCheck`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum ConnectDeepCheck {
    /// Existence only.
    None,
    /// The manifest digest equals the receipt's.
    ManifestDigest,
    /// Additionally sample segment bytes.
    SegmentSample,
}

/// The CRD's floor on a non-manual cadence (rule J3).
pub const MIN_INTERVAL_SECONDS: i32 = 300;
/// The CRD's ceiling.
pub const MAX_INTERVAL_SECONDS: i32 = 86400;
/// The CRD's view-limit range.
pub const MIN_VIEW_LIMIT: i32 = 100;
/// The CRD's view-limit ceiling.
pub const MAX_VIEW_LIMIT: i32 = 5000;

/// Validate a connect request against the shape the CRD will accept.
///
/// THE FIELD-LEVEL RANGES ONLY, NEVER A SECOND COPY OF A CEL RULE. D1 §5.2's
/// rule holds here: the API does not pre-empt a transition rule the API server
/// owns. What it does check is the two things a 422 from the API server could
/// not explain usefully — a request naming both locations or neither, and a
/// number outside the range the schema publishes — so the caller gets a field
/// error rather than a generic rejection.
///
/// # Errors
///
/// `validation_failed` naming the field.
pub fn validate_connect(request: &ConnectArchiveRequest) -> Result<(), ApiError> {
    let mut errors = Vec::new();
    if !validate::is_dns_subdomain(&request.name) {
        errors.push(FieldError::new(
            "name",
            "invalid_value",
            "a catalog name is a DNS-1123 subdomain",
        ));
    }
    match (&request.destination_ref, &request.legacy_archive) {
        (Some(_), Some(_)) | (None, None) => errors.push(FieldError::new(
            "destinationRef",
            "invalid_value",
            "set exactly one of destinationRef or legacyArchive",
        )),
        (Some(reference), None) => {
            if !validate::is_dns_subdomain(&reference.name) {
                errors.push(FieldError::new(
                    "destinationRef.name",
                    "invalid_value",
                    "a destination name is a DNS-1123 subdomain",
                ));
            }
        }
        (None, Some(archive)) => {
            if let Err(why) = validate::check_archive_url(&archive.url) {
                errors.push(FieldError::new("legacyArchive.url", "invalid_value", why));
            }
            if let Some(reference) = &archive.credential_ref {
                if !validate::is_dns_subdomain(&reference.name) {
                    errors.push(FieldError::new(
                        "legacyArchive.credentialRef.name",
                        "invalid_value",
                        "a Secret name is a DNS-1123 subdomain",
                    ));
                }
            }
        }
    }
    if let Some(interval) = request.interval_seconds {
        let ok = interval == 0 || (MIN_INTERVAL_SECONDS..=MAX_INTERVAL_SECONDS).contains(&interval);
        if !ok {
            errors.push(FieldError::new(
                "intervalSeconds",
                "out_of_range",
                format!(
                    "intervalSeconds is 0 (manual only) or {MIN_INTERVAL_SECONDS} to \
                     {MAX_INTERVAL_SECONDS}"
                ),
            ));
        }
    }
    if let Some(limit) = request.view_limit {
        if !(MIN_VIEW_LIMIT..=MAX_VIEW_LIMIT).contains(&limit) {
            errors.push(FieldError::new(
                "viewLimit",
                "out_of_range",
                format!("viewLimit is {MIN_VIEW_LIMIT} to {MAX_VIEW_LIMIT}"),
            ));
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(ApiError::validation(errors))
    }
}

fn build(
    namespace: &str,
    name: String,
    annotations: std::collections::BTreeMap<String, String>,
    request: &ConnectArchiveRequest,
) -> RecoveryCatalog {
    let sync = SyncSettings {
        interval_seconds: request.interval_seconds.unwrap_or(3600),
        mode: match request.sync_mode {
            ConnectSyncMode::Index => SyncMode::Index,
            ConnectSyncMode::Full => SyncMode::Full,
        },
        max_objects_per_run: 100_000,
        deep_check: match request.deep_check {
            None | Some(ConnectDeepCheck::ManifestDigest) => DeepCheck::ManifestDigest,
            Some(ConnectDeepCheck::None) => DeepCheck::None,
            Some(ConnectDeepCheck::SegmentSample) => DeepCheck::SegmentSample,
        },
        view_limit: request.view_limit.unwrap_or(2000),
    };
    RecoveryCatalog {
        metadata: ObjectMeta {
            name: Some(name),
            namespace: Some(namespace.to_string()),
            annotations: Some(annotations),
            ..ObjectMeta::default()
        },
        spec: RecoveryCatalogSpec {
            destination_ref: request.destination_ref.as_ref().map(|r| LocalRef {
                name: r.name.clone(),
            }),
            legacy_archive: request.legacy_archive.as_ref().map(|a| ArchiveRef {
                url: a.url.clone(),
                secret_ref: a.credential_ref.as_ref().map(|r| LocalRef {
                    name: r.name.clone(),
                }),
            }),
            sync,
            // NOT SET BY A CREATE. `syncRequest` is the one mutable field and
            // its command route is a later task; a create that pre-filled it
            // would make the first reconcile look like a re-sync request.
            sync_request: None,
        },
        status: None,
    }
}

// ======================================================================
// Routes
// ======================================================================

/// `GET .../catalogs`.
pub async fn list(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    actor: Actor,
    ApiPath(ns): ApiPath<String>,
    uri: Uri,
) -> Result<Response, ApiError> {
    authorize(&state, &actor, &ns, Action::ReadCatalogs)?;
    let query = list_query(uri.query())?;
    let (items, page) =
        list_page::<RecoveryCatalog>(&state, &actor, &ns, ROUTE_LIST, &query).await?;
    let now = state.now();
    Ok(json(
        StatusCode::OK,
        &CatalogList {
            request_id,
            items: items.iter().map(|c| view(c, now)).collect(),
            page,
        },
    ))
}

/// `POST .../catalogs` — connect an existing archive.
pub async fn create(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    actor: Actor,
    ApiPath(ns): ApiPath<String>,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, ApiError> {
    authorize(&state, &actor, &ns, Action::ConnectCatalog)?;
    crate::http::parse_query(uri.query(), &[])?;
    let key = IdempotencyKey::from_headers(&headers)?;
    let request: ConnectArchiveRequest = read_json(body, MAX_JSON_BODY).await?;
    validate_connect(&request)?;
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
    let now = state.now();
    Ok(json(
        created.status(),
        &CatalogResponse {
            request_id,
            replayed: Some(created.replayed),
            item: view(&created.object, now),
        },
    ))
}

/// `GET .../catalogs/{name}`.
pub async fn get_one(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    actor: Actor,
    ApiPath((ns, name)): ApiPath<(String, String)>,
    uri: Uri,
) -> Result<Response, ApiError> {
    crate::http::parse_query(uri.query(), &[])?;
    authorize(&state, &actor, &ns, Action::ReadCatalogs)?;
    let object = get_object::<RecoveryCatalog>(&state, &actor, &ns, &name).await?;
    Ok(json(
        StatusCode::OK,
        &CatalogResponse {
            request_id,
            replayed: None,
            item: view(&object, state.now()),
        },
    ))
}

/// `GET .../catalogs/{name}/signers` — the untrusted-signer panel.
pub async fn signers(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    actor: Actor,
    ApiPath((ns, name)): ApiPath<(String, String)>,
    uri: Uri,
) -> Result<Response, ApiError> {
    crate::http::parse_query(uri.query(), &[])?;
    authorize(&state, &actor, &ns, Action::ReadCatalogs)?;
    let object = get_object::<RecoveryCatalog>(&state, &actor, &ns, &name).await?;
    let projected = view(&object, state.now());
    Ok(json(
        StatusCode::OK,
        &SignerPageResponse {
            request_id,
            untrusted_points: projected.counts.untrusted_signer,
            items: projected.signers,
            fingerprint_command: FINGERPRINT_COMMAND.to_string(),
        },
    ))
}

/// The cursor a point page carries: the view generation and the offset into
/// the concatenated pages.
fn point_scope(actor: &Actor, ns: &str, name: &str, generation: &str) -> CursorScope {
    CursorScope {
        actor_id: actor.id(),
        route: ROUTE_POINTS.to_string(),
        namespace: ns.to_string(),
        // THE GENERATION IS PART OF THE SCOPE, WHICH IS WHAT MAKES A REPLACED
        // VIEW AN ERROR RATHER THAN A DIFFERENT PAGE. A sync publishes a new
        // set of pages under a new `syncedAt`; a cursor minted against the old
        // one no longer matches this string and is `cursor_invalid`, so a
        // paging client restarts instead of silently reading half of one view
        // and half of another.
        filters: format!("name={name}&view={generation}"),
    }
}

/// The string that identifies one materialisation of the view.
fn view_generation(catalog: &RecoveryCatalog) -> String {
    let status = catalog.status.as_ref();
    let synced = status
        .and_then(|s| s.synced_at)
        .map(|t| t.to_rfc3339())
        .unwrap_or_default();
    let names: Vec<&str> = status
        .and_then(|s| s.pages.as_ref())
        .into_iter()
        .flatten()
        .map(|p| p.config_map_name.as_str())
        .collect();
    format!("{synced}|{}", names.join(","))
}

/// `GET .../catalogs/{name}/points`.
///
/// THE PAGE NAMES COME FROM THE OBJECT, NEVER FROM THE CALLER. Each
/// `ConfigMap` read here is named in `status.pages[].configMapName` of an
/// object this request has already authorized, so this route cannot be pointed
/// at another `ConfigMap`; there is no list verb for the type at all.
///
/// AND EACH ONE IS CHECKED BEFORE A ROW IS SERVED. A page must be
/// `immutable: true` and its content digest must equal the `sha256` the status
/// recorded. The owner is the sync JOB rather than the catalog — that is how
/// the view is garbage-collected without any `delete` permission — so owner
/// identity cannot be the check here, and the digest is: it is transport
/// integrity over bytes a Job produced, which is exactly what D3 §5.3 says it
/// is, and it is not authorization.
pub async fn points(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    actor: Actor,
    ApiPath((ns, name)): ApiPath<(String, String)>,
    uri: Uri,
) -> Result<Response, ApiError> {
    authorize(&state, &actor, &ns, Action::ReadCatalogs)?;
    let query = crate::http::parse_query(uri.query(), &["limit", "cursor", "selectable"])?;
    let limit = match query.get("limit") {
        None => super::DEFAULT_LIMIT,
        Some(text) => match text.parse::<u32>() {
            Ok(n) if (1..=MAX_POINT_PAGE).contains(&n) => n,
            _ => {
                return Err(ApiError::validation(vec![FieldError::new(
                    "limit",
                    "out_of_range",
                    format!("limit must be an integer from 1 to {MAX_POINT_PAGE}"),
                )]))
            }
        },
    };
    let selectable_only = match query.get("selectable").map(String::as_str) {
        None => false,
        Some("true") => true,
        Some("false") => false,
        Some(_) => {
            return Err(ApiError::validation(vec![FieldError::new(
                "selectable",
                "invalid_value",
                "selectable is true or false",
            )]))
        }
    };
    check_name(&name)?;
    let catalog = get_object::<RecoveryCatalog>(&state, &actor, &ns, &name).await?;
    let projected = view(&catalog, state.now());
    let generation = view_generation(&catalog);
    let scope = point_scope(&actor, &ns, &name, &generation);
    let offset = match query.get("cursor") {
        None => 0usize,
        Some(cursor) => match cursor::open(state.cursor_key(), &scope, cursor, state.now()) {
            Ok(token) => token.parse::<usize>().map_err(|_| {
                ApiError::new(
                    ProblemCode::CursorInvalid,
                    "The cursor is not valid for this list; restart the list without a cursor.",
                )
            })?,
            Err(CursorError::Invalid) => {
                return Err(ApiError::new(
                    ProblemCode::CursorInvalid,
                    "The catalog view was replaced or the cursor is not valid for this list; \
                     restart the list without a cursor.",
                ))
            }
            Err(CursorError::Expired) => {
                return Err(ApiError::new(
                    ProblemCode::CursorExpired,
                    "The cursor expired; restart the list without a cursor.",
                ))
            }
        },
    };

    let pages: Vec<(String, Option<String>, i64)> = catalog
        .status
        .as_ref()
        .and_then(|s| s.pages.as_ref())
        .into_iter()
        .flatten()
        .take(MAX_PAGES_PER_REQUEST)
        .map(|p| (p.config_map_name.clone(), p.sha256.clone(), p.count))
        .collect();

    let mut items: Vec<PointView> = Vec::new();
    let mut scanned = 0usize;
    let mut next_cursor = None;
    'pages: for (page_name, digest, _count) in &pages {
        let document = match state.kube().get_result_document(&ns, page_name).await {
            Ok(document) => document,
            // A PAGE THAT IS GONE IS A VIEW THAT AGED OUT, NOT AN EMPTY
            // ARCHIVE. The `viewExpired` flag on the response is what says so;
            // answering 404 here would tell a console the catalog does not
            // exist, and answering a short list without the flag would tell it
            // the archive is smaller than it is.
            Err(KubeFailure::NotFound) => break 'pages,
            Err(other) => return Err(other.into_api_error()),
        };
        verify_page(&document, digest.as_deref(), page_name)?;
        let body = document
            .data
            .get(PAGE_DATA_KEY)
            .cloned()
            .unwrap_or_default();
        for line in body.lines() {
            if line.trim().is_empty() {
                continue;
            }
            // A LINE THAT IS NOT A VIEW ENTRY IS SKIPPED, NOT GUESSED AT. The
            // producer writes one JSON entry per line; a half-written line
            // rendered as a point would be a recovery point nobody recorded.
            let Ok(entry) = serde_json::from_str::<ViewEntry>(line) else {
                continue;
            };
            if selectable_only && !entry.selectable {
                continue;
            }
            scanned += 1;
            if scanned <= offset {
                continue;
            }
            items.push(point_view(&entry));
            if items.len() as u32 >= limit {
                next_cursor = Some(cursor::seal(
                    state.cursor_key(),
                    &scope,
                    &scanned.to_string(),
                    state.now(),
                ));
                break 'pages;
            }
        }
    }

    Ok(json(
        StatusCode::OK,
        &PointPageResponse {
            request_id,
            items,
            page: Page {
                limit,
                next_cursor,
                snapshot: Some(generation),
            },
            truncated: projected.truncated,
            view_expired: projected.view_expired,
            view_expires_at: projected.view_expires_at,
        },
    ))
}

/// A page is immutable and its bytes are the ones the status recorded.
///
/// # Errors
///
/// `result_integrity_failed` naming neither the bytes nor the digest that did
/// not match — a digest comparison that printed both sides would be a digest
/// comparison that teaches an attacker what to aim at.
fn verify_page(
    document: &ResultDocument,
    recorded: Option<&str>,
    page_name: &str,
) -> Result<(), ApiError> {
    let integrity = |detail: &'static str| {
        tracing::warn!(page = %page_name, detail, "catalog page refused");
        ApiError::new(
            ProblemCode::ResultIntegrityFailed,
            "The catalog view page did not match what the catalog recorded; re-sync the catalog.",
        )
    };
    if document.immutable != Some(true) {
        return Err(integrity("the page is not immutable"));
    }
    let Some(recorded) = recorded else {
        return Err(integrity("the status recorded no digest for this page"));
    };
    let body = document.data.get(PAGE_DATA_KEY).map(String::as_str);
    let Some(body) = body else {
        return Err(integrity("the page carries no entries key"));
    };
    // The producer digests the entry LINES. `page_digest` is weirkeeper's own
    // function, so this comparison cannot drift from the one that wrote it.
    let lines: Vec<&str> = body.lines().filter(|l| !l.trim().is_empty()).collect();
    let computed = weirkeeper::catalog_view::page_digest(&lines);
    // The published spelling carries a `sha256:` prefix and the function
    // returns bare hex; accept either and refuse anything else, which is the
    // same normalisation RET-DIGEST-PREFIX landed on the retention side.
    let recorded_bare = recorded.strip_prefix("sha256:").unwrap_or(recorded);
    let computed_bare = computed.strip_prefix("sha256:").unwrap_or(&computed);
    if recorded_bare.len() != 64
        || !recorded_bare
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
    {
        return Err(integrity("the recorded digest is not lowercase sha256 hex"));
    }
    if recorded_bare != computed_bare {
        return Err(integrity("the page digest does not match the recorded one"));
    }
    // The annotation is the producer's own copy of the same number; when it is
    // present it must agree, so a page whose status row was rewritten without
    // its bytes is refused too.
    if let Some(annotated) = document.annotation(PAGE_DIGEST_ANNOTATION) {
        let annotated = annotated.strip_prefix("sha256:").unwrap_or(annotated);
        if annotated != computed_bare {
            return Err(integrity("the page annotation disagrees with its bytes"));
        }
    }
    Ok(())
}
