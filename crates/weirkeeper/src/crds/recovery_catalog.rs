//! `RecoveryCatalog` — the durable list of what can actually be recovered,
//! read from object storage rather than from Kubernetes.
//!
//! # Why it is a kind and not a field on the destination
//!
//! A destination is storage configuration and is nearly immutable. A catalog
//! has a mutable sync trigger, its own bounded view, its own Jobs and its own
//! failure modes — and it must exist for archives that PREDATE this
//! installation, which is the whole point of PLAT-15.2's "connect an existing
//! archive" (ADR 0008 Amendment G).
//!
//! # `syncRequest` is the one mutable field, and it is a token
//!
//! Everything else is sealed. `syncRequest` is an opaque token the API's
//! command route writes; the controller records the one it has acted on in
//! `status.observedSyncRequest`, so a repeated request is idempotent and a
//! genuinely new one is distinguishable from a retry. A boolean would not be:
//! "sync now" set twice is one sync or two, and nobody could tell which.
//!
//! # The view is bounded and expires, on purpose
//!
//! The durable truth is in object storage. What lives in Kubernetes is a
//! bounded, newest-first window materialised into immutable `ConfigMap` pages
//! owned by the sync Job. **No `delete` verb is added anywhere**: the sync Job
//! carries a TTL, Kubernetes garbage-collects the Job, and its pages go with
//! it. If syncing stops for longer than the TTL the view disappears and this
//! object reports `Stale` and then `ViewExpired` — honest, and the archive is
//! untouched.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{ArchiveRef, Condition, LocalRef, SpecRule, Time};

/// J1 — everything but `syncRequest` is sealed.
///
/// The clause order is the field order of [`RecoveryCatalogSpec`], minus
/// `syncRequest`. Object-level for the reason
/// [`super::backup_schedule::SUSPEND_ONLY_RULE`] is.
pub const SYNC_REQUEST_ONLY_RULE: &str = "has(self.destinationRef) == has(oldSelf.destinationRef) && (!has(self.destinationRef) || self.destinationRef == oldSelf.destinationRef) && has(self.legacyArchive) == has(oldSelf.legacyArchive) && (!has(self.legacyArchive) || self.legacyArchive == oldSelf.legacyArchive) && has(self.sync) == has(oldSelf.sync) && (!has(self.sync) || self.sync == oldSelf.sync)";

/// The message [`SYNC_REQUEST_ONLY_RULE`] travels with.
pub const SYNC_REQUEST_ONLY_MESSAGE: &str =
    "only spec.syncRequest is mutable; a catalog of a different location is a different catalog";

/// J2 — a catalog indexes a saved destination or an inline archive, not both
/// and not neither.
pub const J2_DESTINATION_XOR_RULE: &str = "has(self.destinationRef) != has(self.legacyArchive)";
/// J2's message.
pub const J2_DESTINATION_XOR_MESSAGE: &str =
    "set exactly one of spec.destinationRef or spec.legacyArchive";

/// The rules on `.spec`.
pub const SPEC_RULES: [SpecRule; 2] = [
    SpecRule::new(SYNC_REQUEST_ONLY_RULE, SYNC_REQUEST_ONLY_MESSAGE),
    SpecRule::new(J2_DESTINATION_XOR_RULE, J2_DESTINATION_XOR_MESSAGE),
];

fn default_interval_seconds() -> i32 {
    3600
}
fn default_max_objects_per_run() -> i32 {
    100_000
}
fn default_view_limit() -> i32 {
    2000
}

/// How much of the archive a sync walks.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, Default, JsonSchema, PartialEq, Eq)]
pub enum SyncMode {
    /// Day shards at or after the recorded cursor, minus one day of overlap.
    #[default]
    Index,
    /// A full rescan of receipts and manifests, resumable through
    /// `status.cursor.rescanStartAfter`.
    Full,
}

/// How hard a sync checks each point it finds.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, Default, JsonSchema, PartialEq, Eq)]
pub enum DeepCheck {
    /// Existence only.
    None,
    /// The manifest digest equals the receipt's — the default, because it is
    /// the check that distinguishes "the object is there" from "the object is
    /// the one this receipt describes".
    #[default]
    ManifestDigest,
    /// Additionally sample segment bytes.
    SegmentSample,
}

/// How and how often to sync.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SyncSettings {
    /// Seconds between syncs. `0` means manual only, through `syncRequest`.
    #[serde(default = "default_interval_seconds")]
    #[schemars(range(min = 0, max = 86400))]
    pub interval_seconds: i32,
    /// How much to walk.
    #[serde(default)]
    pub mode: SyncMode,
    /// The object budget for one Job. A walk that does not finish persists its
    /// cursor and continues next time, rather than restarting forever.
    #[serde(default = "default_max_objects_per_run")]
    #[schemars(range(min = 1000, max = 1000000))]
    pub max_objects_per_run: i32,
    /// How hard to check each point.
    #[serde(default)]
    pub deep_check: DeepCheck,
    /// How many newest points are materialised into Kubernetes. Points beyond
    /// it are counted and histogrammed here and reachable with `logweir
    /// catalog list` on an operator workstation — never silently dropped.
    #[serde(default = "default_view_limit")]
    #[schemars(range(min = 100, max = 5000))]
    pub view_limit: i32,
}

/// `RecoveryCatalog.spec`.
#[derive(kube::CustomResource, Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "logweir.dev",
    version = "v1alpha1",
    kind = "RecoveryCatalog",
    doc = "The durable list of recovery points in one destination, read from object storage by a short-lived check Job (ADR 0008 Amendment G). `spec.syncRequest` is the only mutable field. The Kubernetes view is a bounded, newest-first window in immutable ConfigMap pages owned by the sync Job; it expires with that Job's TTL and the archive is never touched. This kind adds NO delete permission anywhere.",
    plural = "recoverycatalogs",
    singular = "recoverycatalog",
    namespaced,
    status = "RecoveryCatalogStatus",
    printcolumn = r#"{"name":"DESTINATION","type":"string","jsonPath":".spec.destinationRef.name"}"#,
    printcolumn = r#"{"name":"POINTS","type":"integer","jsonPath":".status.counts.total"}"#,
    printcolumn = r#"{"name":"AVAILABLE","type":"integer","jsonPath":".status.counts.available"}"#,
    printcolumn = r#"{"name":"SYNCED","type":"date","jsonPath":".status.syncedAt"}"#,
    printcolumn = r#"{"name":"TRUNCATED","type":"boolean","jsonPath":".status.truncated"}"#,
    printcolumn = r#"{"name":"AGE","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct RecoveryCatalogSpec {
    /// The saved destination to index. Immutable (J1).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination_ref: Option<LocalRef>,
    /// An inline archive, for an archive that predates saved destinations.
    /// Immutable (J1).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub legacy_archive: Option<ArchiveRef>,
    /// How and how often to sync. Immutable (J1).
    pub sync: SyncSettings,
    /// An opaque token: change it to ask for a sync now. **The only mutable
    /// field.** The controller records the token it acted on, so a retried
    /// request is idempotent and a new one is not mistaken for a retry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 253))]
    pub sync_request: Option<String>,
}

/// Where the last walk got to.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SyncCursor {
    /// The day shard the next `Index` sync resumes from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_shard: Option<String>,
    /// The key the next `Full` rescan continues after.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rescan_start_after: Option<String>,
    /// Whether the walk finished. `false` is not a failure: it is a budgeted
    /// walk that will continue.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub complete: Option<bool>,
}

/// What the last sync found, by verdict.
///
/// TEN COUNTS, NOT TWO. "How many points are there" and "how many can I
/// actually restore from" are different numbers, and collapsing them is how a
/// console comes to promise recovery from a point whose manifest does not
/// parse.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, Default, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CatalogCounts {
    /// Every point the walk saw.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total: Option<i64>,
    /// Readable, digest-matching, selectable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub available: Option<i64>,
    /// A definite `NotFound`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub missing: Option<i64>,
    /// Present but unreadable — a permission or transport failure, which is
    /// NOT the same as absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unreadable: Option<i64>,
    /// No signature verdict was reached.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unverified: Option<i64>,
    /// Signed by a key the bound `TrustPolicy` does not trust.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub untrusted_signer: Option<i64>,
    /// The signature did not verify.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub invalid: Option<i64>,
    /// Two records disagree about the same point.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conflict: Option<i64>,
    /// Recorded as deleted by an attributable retention run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deleted: Option<i64>,
    /// Written by a format this build does not understand.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unsupported_format: Option<i64>,
}

/// How many points a day holds.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct HistogramBucket {
    /// The day, `YYYY-MM-DD`.
    pub day: String,
    /// How many points.
    pub points: i64,
}

/// Who signed the points in this catalog.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SignerSummary {
    /// The key id.
    pub key_id: String,
    /// A hint at the principal, from the record. Never authority.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub principal_hint: Option<String>,
    /// How many points it signed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub points: Option<i64>,
    /// Whether the bound `TrustPolicy` trusts it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trusted: Option<bool>,
}

/// One immutable page of the materialised view.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CatalogPage {
    /// The `ConfigMap`, owned by the sync Job so it is collected with it.
    pub config_map_name: String,
    /// Its index in the page sequence.
    pub index: i64,
    /// How many entries it carries.
    pub count: i64,
    /// The first point id in it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_point_id: Option<String>,
    /// The last point id in it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_point_id: Option<String>,
    /// `sha256:<lowercase hex>` over its entry lines — transport integrity,
    /// never authorization.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
}

/// The sync Job that produced this view.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LastSyncJob {
    /// Its name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// When it started.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<Time>,
    /// When it finished.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<Time>,
    /// Its exit code.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    /// Its `refusal-reason=`, for an exit 3.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refusal_reason: Option<String>,
}

/// `RecoveryCatalog.status`.
#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RecoveryCatalogStatus {
    /// The `metadata.generation` this view was built from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    /// The `spec.syncRequest` token the controller has acted on.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_sync_request: Option<String>,
    /// When the view was last refreshed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub synced_at: Option<Time>,
    /// When the pages age out with their Job. After it, the view is gone and
    /// this object says `ViewExpired` rather than reporting an empty archive.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub view_expires_at: Option<Time>,
    /// Where the walk got to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<SyncCursor>,
    /// What it found.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub counts: Option<CatalogCounts>,
    /// Whether `counts.total` exceeds `sync.viewLimit`, so the Kubernetes view
    /// is a window and not the whole archive.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub truncated: Option<bool>,
    /// Points per day.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 400))]
    pub histogram: Option<Vec<HistogramBucket>>,
    /// Who signed them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 16))]
    pub signers: Option<Vec<SignerSummary>>,
    /// The materialised pages.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 8))]
    pub pages: Option<Vec<CatalogPage>>,
    /// The fence-pointer `ConfigMap`: point-id ranges to page index.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_config_map: Option<String>,
    /// The Job that built the view.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_sync_job: Option<LastSyncJob>,
    /// The condition set: `Ready`, `Synced`, `Stale`, `TrustAvailable`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<Condition>>,
}
