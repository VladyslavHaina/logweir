//! Preflights: operation readiness and restore preflight (PLAT-03).
//!
//! # A result is a result ABOUT something
//!
//! Every preflight carries a `binding` — the plan hash and the digest over the
//! objects it resolved — and [`project`] recomputes `applicable` and `stale`
//! on EVERY read against the caller's current draft. Edit the plan and the
//! hash changes; choose another target, destination or recovery point and a
//! referent UID changes; let it sit and it expires. A cached verdict rendered
//! as current readiness is the defect PLAT-03 exists to remove, so this module
//! never serves one without saying so.
//!
//! # Ready authorizes nothing
//!
//! The aggregate is advisory. Execution-time guards remain the authority, and
//! `executionOnly` names in the response the checks that cannot be answered
//! before the run, so "ready" is never read as "every permission is verified".
//!
//! # Plan bytes are forwarded verbatim
//!
//! `planBytes` is opaque: this service checks `planHash` against the SHA-256 of
//! exactly those bytes and stores them unchanged. It never parses, reformats
//! or re-emits a plan document.

use axum::body::Body;
use axum::extract::State;
use axum::response::Response;
use chrono::{DateTime, Utc};
use http::{HeaderMap, StatusCode, Uri};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::Resource as _;
use logweir_core::check_contract::{
    stale_reasons, BindingInputs, CheckOperation as CoreCheckOperation, Referent as CoreReferent,
    RosterRef, StaleReason,
};
use logweir_core::destination::DestinationRole;
use std::collections::BTreeMap;
use weirkeeper::crds::approval::Approval as ApprovalCr;
use weirkeeper::crds::backup::Backup as BackupCr;
use weirkeeper::crds::backup_destination::BackupDestination;
use weirkeeper::crds::backup_schedule::BackupSchedule;
use weirkeeper::crds::kafka_cluster::KafkaCluster;
use weirkeeper::crds::preflight::Referent;
use weirkeeper::crds::preflight::{
    BackupPreflightRequest as CrdBackupRequest, CatalogPointRef, DestinationAccessRequest,
    Preflight as PreflightCr, PreflightOperation, PreflightRequest, PreflightSpec,
    RestorePreflightRequest as CrdRestoreRequest,
    SourceConnectionPreflightRequest as CrdSourceConnectionRequest, UidRef,
};
use weirkeeper::crds::recovery_catalog::RecoveryCatalog;
use weirkeeper::crds::restore::Restore as RestoreCr;
use weirkeeper::crds::trust_policy::TrustPolicy;
use weirkeeper::crds::{ArchiveRef, LocalRef};

use super::{
    authorize, cancel_check, check_create_rate, check_name, create_idempotent, get_object, json,
    ApiPath, Created, DEFAULT_LIMIT, MAX_LIMIT, PREFLIGHT_CREATES_PER_MINUTE,
};
use crate::app::AppState;
use crate::auth::Actor;
use crate::authz::{Action, Role};
use crate::contract::{
    CancelResponse, CheckEntryView, CheckGating, CheckOperation, CheckOperationKind,
    CheckOperationResponse, CheckScopeView, CheckVerdict, CreatePreflightRequest,
    DestinationRoleDto, DetailEntryView, DetailPageResponse, ExecutionOnlyView, Page, Preflight,
    PreflightBindingView, PreflightOperationDto, PreflightResponse, PreflightState, ReferentView,
    StaleReasonKind, StaleReasonView,
};
use crate::cursor::{self, CursorError, CursorScope};
use crate::http::{read_json, RequestId, MAX_JSON_BODY};
use crate::idempotency::IdempotencyKey;
use crate::kube::KubeFailure;
use crate::problem::{ApiError, FieldError, ProblemCode};
use crate::status::{condition_view, MAX_CONDITIONS};
use crate::validate;

/// The create route identifier (idempotency scope).
pub const ROUTE_CREATE: &str = "POST /api/v1/namespaces/{ns}/preflights";
/// The details route identifier (cursor scope).
pub const ROUTE_DETAILS: &str = "GET /api/v1/namespaces/{ns}/preflights/{id}/details";
/// The cancel route identifier.
pub const ROUTE_CANCEL: &str = "POST /api/v1/namespaces/{ns}/preflights/{id}:cancel";
/// The destination-test route's idempotency scope.
pub const ROUTE_DESTINATION_TEST: &str = "POST /api/v1/namespaces/{ns}/destinations/{name}:test";
/// The deterministic name prefix (D2 §6.2).
pub const NAME_PREFIX: &str = "pf-";
/// The `:cancel` command suffix.
pub const CANCEL: &str = ":cancel";

/// The largest plan document accepted, in bytes (D2 §6.2).
pub const MAX_PLAN_BYTES: usize = 256 * 1024;
/// The most detail entries one page carries.
pub const MAX_DETAIL_PAGE: u32 = MAX_LIMIT;
/// The one data key inside a details document.
pub const DETAILS_KEY: &str = "details.jsonl";
/// The annotation carrying a result document's own digest.
pub const SHA256_ANNOTATION: &str = "logweir.dev/result-sha256";

// ======================================================================
// Projection
// ======================================================================

fn operation_dto(operation: PreflightOperation) -> PreflightOperationDto {
    match operation {
        PreflightOperation::Backup => PreflightOperationDto::Backup,
        PreflightOperation::Restore => PreflightOperationDto::Restore,
        PreflightOperation::DestinationAccess => PreflightOperationDto::DestinationAccess,
        PreflightOperation::SourceConnection => PreflightOperationDto::SourceConnection,
    }
}

fn verdict(state: &str) -> CheckVerdict {
    match state {
        "ready" => CheckVerdict::Ready,
        "notReady" => CheckVerdict::NotReady,
        "skipped" => CheckVerdict::Skipped,
        _ => CheckVerdict::Unknown,
    }
}

fn gating(value: Option<&String>) -> Option<CheckGating> {
    match value.map(String::as_str) {
        Some("blocking") => Some(CheckGating::Blocking),
        Some("advisory") => Some(CheckGating::Advisory),
        Some("executionOnly") => Some(CheckGating::ExecutionOnly),
        _ => None,
    }
}

fn entry_view(entry: &weirkeeper::crds::preflight::CheckEntry) -> CheckEntryView {
    CheckEntryView {
        id: entry.id.clone(),
        category: entry.category.clone(),
        scope: entry.scope.as_ref().map(|s| CheckScopeView {
            kind: s.kind.clone(),
            name: s.name.clone(),
            uid: s.uid.clone(),
        }),
        state: verdict(&entry.state),
        gating: gating(entry.gating.as_ref()),
        authority: entry.authority.clone(),
        code: entry.code.clone(),
        message: entry.message.as_ref().map(|m| validate::bounded(m, 512)),
        remedy: entry.remedy.as_ref().map(|m| validate::bounded(m, 512)),
        observed_at: entry.observed_at.as_ref().copied(),
        expires_at: entry.expires_at.as_ref().copied(),
    }
}

/// The aggregate: the controller's phase, and — once complete — the result
/// state it recorded.
fn state_of(object: &PreflightCr) -> PreflightState {
    let status = object.status.as_ref();
    let phase = status.and_then(|s| s.phase.as_deref()).unwrap_or("Pending");
    match phase {
        "Pending" => PreflightState::Pending,
        "Queued" => PreflightState::Queued,
        "Running" => PreflightState::Running,
        "Cancelled" => PreflightState::Cancelled,
        "Failed" => PreflightState::Failed,
        "Completed" => match status
            .and_then(|s| s.result.as_ref())
            .map(|r| r.state.as_str())
        {
            Some("ready") => PreflightState::Ready,
            Some("notReady") => PreflightState::NotReady,
            // COMPLETED WITH NO RECORDED AGGREGATE IS `unknown`, NOT `ready`.
            _ => PreflightState::Unknown,
        },
        // A phase this build does not recognise is never optimistic.
        _ => PreflightState::Unknown,
    }
}

const fn is_terminal(state: PreflightState) -> bool {
    matches!(
        state,
        PreflightState::Ready
            | PreflightState::NotReady
            | PreflightState::Unknown
            | PreflightState::Failed
            | PreflightState::Cancelled
    )
}

/// The kinds a `Preflight`'s `status.binding.referents[]` can name, and
/// whether this service can read one.
///
/// ALL NINE ARE READ. The reconciler records `KafkaCluster`,
/// `BackupDestination`, `Backup`, `Restore`, `Approval`, `BackupSchedule` and
/// `RecoveryCatalog` (a catalog point's catalog, PLAT-15.2) — namespaced
/// `ProductResource`s this API already reads — plus two
/// CLUSTER-SCOPED trust referents: `TrustRoster/default` whenever it exists
/// (since `4c4d2ed`), and the namespace's governing `TrustPolicy`. Both trust
/// kinds are compared by uid and generation like every other referent
/// (PREFLIGHT-TRUSTROSTER-STALE): the roster through
/// [`crate::kube::KubeAdapter::get_trust_roster`], which reads `default` and
/// nothing else, and the policy through the existing read-only
/// `ClusterResource` seal. A kind this build does not know — anything a later
/// controller adds — is still reported [`StaleReasonKind::Unverifiable`]
/// rather than quietly skipped, and so is a read that fails: a roster or
/// policy edit is exactly the kind of change that invalidates a signer check,
/// and not being able to see one must never read as "unchanged".
pub const REFERENT_KIND_KAFKA_CLUSTER: &str = "KafkaCluster";
/// See [`REFERENT_KIND_KAFKA_CLUSTER`].
pub const REFERENT_KIND_DESTINATION: &str = "BackupDestination";
/// See [`REFERENT_KIND_KAFKA_CLUSTER`].
pub const REFERENT_KIND_BACKUP: &str = "Backup";
/// See [`REFERENT_KIND_KAFKA_CLUSTER`].
pub const REFERENT_KIND_RESTORE: &str = "Restore";
/// See [`REFERENT_KIND_KAFKA_CLUSTER`].
pub const REFERENT_KIND_APPROVAL: &str = "Approval";
/// See [`REFERENT_KIND_KAFKA_CLUSTER`].
pub const REFERENT_KIND_SCHEDULE: &str = "BackupSchedule";
/// See [`REFERENT_KIND_KAFKA_CLUSTER`]. The catalog a catalog-point restore
/// check names (weirkeeper `controllers/preflight.rs`, "a catalog point"),
/// recorded by uid alone — see [`UID_BOUND_KINDS`].
pub const REFERENT_KIND_RECOVERY_CATALOG: &str = "RecoveryCatalog";

/// The kinds the controller records WITHOUT a generation, and so the only
/// kinds this service compares by uid alone.
///
/// `logweir_core::check_contract::Referent::generation` is `None` "for a kind
/// whose generation is not meaningful", and weirkeeper's reconciler records
/// exactly these three that way: the recovery-point `Backup` and the `Approval`
/// (both spec-immutable by CEL, so their generation never moves), and a catalog
/// point's `RecoveryCatalog` (PLAT-15.2). The controller's own
/// `stale_against_status` compares all three by uid alone too — its current
/// side records them with no generation either — so the API and the
/// controller agree, and a catalog recreated under the same name still
/// differs by uid.
///
/// A CLOSED LIST, ON PURPOSE (PLAT-08.2 review L4). Any OTHER kind recorded
/// without a generation is reported `unverifiable` with
/// [`BASIS_GENERATION_NOT_RECORDED`], never compared by uid alone: a mutable
/// kind (`BackupDestination`, `KafkaCluster`, a `TrustPolicy`) whose edits are
/// silently ignored would read as "unchanged" when nobody compared its
/// revision.
pub const UID_BOUND_KINDS: [&str; 3] = [
    REFERENT_KIND_BACKUP,
    REFERENT_KIND_APPROVAL,
    REFERENT_KIND_RECOVERY_CATALOG,
];
/// See [`REFERENT_KIND_KAFKA_CLUSTER`]. Cluster-scoped; only `default` is read.
pub const REFERENT_KIND_TRUST_ROSTER: &str = "TrustRoster";
/// See [`REFERENT_KIND_KAFKA_CLUSTER`]. Cluster-scoped.
pub const REFERENT_KIND_TRUST_POLICY: &str = "TrustPolicy";

/// `basis` for a referent whose kind this service has no verb for.
pub const BASIS_KIND_NOT_READABLE: &str =
    "this service has no verb for this kind, so its revision cannot be compared";
/// `basis` for a `TrustRoster` referent that names any roster but `default`.
///
/// The controller records only `weirkeeper::ROSTER_NAME`, and this service's
/// grant is `get` on that one name, so another name is not something it can
/// compare — and it is reported, never skipped.
pub const BASIS_ROSTER_NOT_DEFAULT: &str =
    "this service reads only the TrustRoster named `default`, so this roster's revision cannot be compared";
/// `basis` for a referent of a generation-bearing kind that was recorded
/// without one. See [`UID_BOUND_KINDS`].
pub const BASIS_GENERATION_NOT_RECORDED: &str =
    "the check recorded no generation for this kind, so its revision cannot be compared";
/// `basis` for a referent whose read failed.
pub const BASIS_READ_FAILED: &str =
    "the object could not be read, so its revision cannot be compared";
/// `basis` when a projection did not recompute staleness at all.
pub const BASIS_NOT_RECOMPUTED: &str =
    "this response did not recompute staleness; read the preflight itself for a current verdict";
/// `basis` when the check recorded no binding at all.
pub const BASIS_NO_BINDING: &str =
    "the check recorded no binding, so there is nothing to compare the current objects against";
/// `staleBasis` entry for the installation policy: compared, but by the
/// controller rather than here.
///
/// WHY THIS IS A BASIS LINE AND NOT AN `unverifiable` REASON. The other gaps
/// are per-request — a read that failed this time, a binding that is missing
/// from this object — and they mean nobody checked. The policy is different:
/// the reconciler compares `binding.policyDigest` against
/// `CheckPolicy::digest()` on every pass and downgrades `result.state` to
/// `unknown` when it moves, so it IS checked, continuously, by the component
/// that can see it. Reporting `unverifiable` for it on every response would
/// make every verdict permanently inapplicable while adding no information a
/// console could act on — and a field that always says the same thing is a
/// field that gets ignored, including on the day it means something.
///
/// The digest itself is `CheckPolicy::digest()` over the PARSED
/// `LOGWEIR_POLICY_CONFIGMAP` document (default `weirkeeper-policy`, key
/// `policy.json`) in the installation namespace. This service reads a
/// `ConfigMap` only when a check owns it — owner UID, immutability and digest
/// all verified — and that document is owned by nothing, is mutable, and lives
/// outside the actor's granted namespaces, so reaching it is a new grant and a
/// cross-namespace read that D2 W11 owns.
pub const COVERED_POLICY_BY_CONTROLLER: &str =
    "policyDigest:byController(the reconciler compares it each pass and downgrades the result)";

/// What the comparison covered, for `staleBasis`.
pub const COVERED_EXPIRY: &str = "expiry";
/// See [`COVERED_EXPIRY`].
pub const COVERED_PLAN_HASH: &str = "planHash";
/// See [`COVERED_EXPIRY`].
pub const COVERED_REFERENTS: &str = "referents";
/// See [`COVERED_EXPIRY`].
pub const COVERED_POLICY_DIGEST: &str = "policyDigest";

/// One recorded referent, read as it is NOW.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReferentReading {
    /// The object exists; this is its revision now.
    Live {
        /// Its UID now. A recreated object has a different one.
        uid: String,
        /// Its `metadata.generation` now.
        generation: Option<i64>,
    },
    /// The object is gone.
    Absent,
    /// It could not be read at all, with the reason for `basis`.
    Unreadable(&'static str),
}

/// The live half of the comparison: every recorded referent, read back.
#[derive(Clone, Debug, Default)]
pub struct LiveBinding {
    /// One reading per recorded referent, in the recorded order.
    pub readings: Vec<(Referent, ReferentReading)>,
    /// The installation policy digest as it is now, when this service can see
    /// it. `None` means it could not be compared, NOT that it is unchanged.
    pub policy_digest: Option<String>,
}

/// Read every referent a check's binding recorded, as it is now.
///
/// THIS IS THE RECOMPUTATION THE CONTROLLER'S CONTRACT ASSIGNS TO THE API.
/// `status.binding` is written once, before the Job is created; `weirkeeper`'s
/// preflight reconciler documents that "W12 recomputes that from live objects
/// on every GET and answers `applicable: false` with named `staleReasons` when
/// it differs", and `check_contract::inputs_digest` says the same. Every input
/// is already structured — `referents[]` carries `kind`, `name`, `uid` and
/// `generation` — so nothing here parses anything.
///
/// A read that fails is recorded as [`ReferentReading::Unreadable`] and never
/// as "unchanged": the whole point of this pass is that not knowing must not
/// look like knowing.
///
/// # Errors
///
/// Never. Every per-object failure becomes an `Unreadable` reading, because a
/// staleness answer that 503s is worse than one that says which referent it
/// could not check.
pub async fn read_live_binding(
    state: &AppState,
    namespace: &str,
    object: &PreflightCr,
) -> LiveBinding {
    let recorded = object
        .status
        .as_ref()
        .and_then(|s| s.binding.as_ref())
        .and_then(|b| b.referents.clone())
        .unwrap_or_default();
    let mut readings = Vec::with_capacity(recorded.len());
    for referent in recorded {
        let reading = read_one_referent(state, namespace, &referent).await;
        readings.push((referent, reading));
    }
    LiveBinding {
        readings,
        // THE POLICY DIGEST IS NOT READABLE HERE, AND THAT IS REPORTED RATHER
        // THAN ASSUMED. The controller takes it from `CheckPolicy::digest()`
        // over the parsed `LOGWEIR_POLICY_CONFIGMAP` document (default
        // `weirkeeper-policy`, key `policy.json`) in the INSTALLATION
        // namespace. This service reads a `ConfigMap` only when a check owns
        // it — owner UID, immutability and digest all verified — and the
        // policy document is owned by nothing, is mutable, and lives outside
        // the actor's granted namespaces. Reaching it is a new grant and a new
        // cross-namespace read, which is D2 W11's to make; until then
        // `policyChanged` is answered `unverifiable`, never "unchanged".
        policy_digest: None,
    }
}

async fn read_one_referent(
    state: &AppState,
    namespace: &str,
    referent: &Referent,
) -> ReferentReading {
    // A name that cannot exist is `Absent` without a Kubernetes call.
    if !crate::validate::is_dns_subdomain(&referent.name) {
        return ReferentReading::Absent;
    }
    match referent.kind.as_str() {
        REFERENT_KIND_KAFKA_CLUSTER => {
            revision::<KafkaCluster>(state, namespace, &referent.name).await
        }
        REFERENT_KIND_DESTINATION => {
            revision::<BackupDestination>(state, namespace, &referent.name).await
        }
        REFERENT_KIND_BACKUP => revision::<BackupCr>(state, namespace, &referent.name).await,
        REFERENT_KIND_RESTORE => revision::<RestoreCr>(state, namespace, &referent.name).await,
        REFERENT_KIND_APPROVAL => revision::<ApprovalCr>(state, namespace, &referent.name).await,
        REFERENT_KIND_SCHEDULE => {
            revision::<BackupSchedule>(state, namespace, &referent.name).await
        }
        REFERENT_KIND_RECOVERY_CATALOG => {
            revision::<RecoveryCatalog>(state, namespace, &referent.name).await
        }
        // THE TWO CLUSTER-SCOPED TRUST REFERENTS. The check's namespace does
        // not enter either read: a cluster-scoped object has none.
        REFERENT_KIND_TRUST_ROSTER if referent.name == weirkeeper::ROSTER_NAME => {
            reading_of(state.kube().get_trust_roster().await)
        }
        REFERENT_KIND_TRUST_ROSTER => ReferentReading::Unreadable(BASIS_ROSTER_NOT_DEFAULT),
        REFERENT_KIND_TRUST_POLICY => reading_of(
            state
                .kube()
                .get_cluster::<TrustPolicy>(&referent.name)
                .await,
        ),
        // Anything a later controller adds.
        _ => ReferentReading::Unreadable(BASIS_KIND_NOT_READABLE),
    }
}

async fn revision<K: crate::kube::ProductResource>(
    state: &AppState,
    namespace: &str,
    name: &str,
) -> ReferentReading {
    reading_of(state.kube().get::<K>(namespace, name).await)
}

/// One read's outcome as a [`ReferentReading`]. A refused, timed-out or
/// otherwise failed read is `Unreadable` — NEVER `Live` and never `Absent` —
/// which is what keeps the comparison fail-closed.
fn reading_of<K: kube::Resource>(read: Result<K, KubeFailure>) -> ReferentReading {
    match read {
        Ok(object) => ReferentReading::Live {
            uid: object.meta().uid.clone().unwrap_or_default(),
            generation: object.meta().generation,
        },
        Err(KubeFailure::NotFound) => ReferentReading::Absent,
        Err(_) => ReferentReading::Unreadable(BASIS_READ_FAILED),
    }
}

/// The neutral value for every `BindingInputs` member the CRD does not record.
///
/// IDENTICAL ON BOTH SIDES, ON PURPOSE. `PreflightBinding` stores the
/// operation, the plan hash, the referents, the policy digest and one digest
/// over everything; it does not store the CA bundle list, the roster, the
/// approval's resourceVersion or a backup's topic set. Feeding core the same
/// placeholder for those on both sides means it compares exactly what was
/// recorded and reports nothing about what was not — which is honest, because
/// a difference invented by a narrower recomputation is not a change in the
/// world. What is NOT compared is reported instead, as `unverifiable`.
fn neutral_roster() -> RosterRef {
    RosterRef {
        uid: String::new(),
        generation: 0,
    }
}

fn binding_inputs(
    operation: CoreCheckOperation,
    plan_hash: Option<String>,
    referents: Vec<CoreReferent>,
    policy_digest: String,
) -> BindingInputs {
    BindingInputs {
        operation,
        plan_hash,
        topics: None,
        referents,
        ca_bundles: Vec::new(),
        roster: neutral_roster(),
        approval: None,
        policy_digest,
    }
}

/// A core referent from a CRD one.
///
/// THE NAMESPACE IS THE CHECK'S OWN. `PreflightBinding.referents[]` has no
/// namespace field, because a `LocalRef` cannot leave its namespace — so both
/// sides of the comparison carry the same one and it contributes nothing,
/// while core's `(kind, namespace, name)` identity still works.
fn core_referent(
    kind: &str,
    namespace: &str,
    name: &str,
    uid: &str,
    generation: Option<i64>,
) -> CoreReferent {
    CoreReferent {
        kind: kind.to_string(),
        namespace: namespace.to_string(),
        name: name.to_string(),
        uid: uid.to_string(),
        generation,
    }
}

fn core_operation(operation: PreflightOperation) -> CoreCheckOperation {
    match operation {
        PreflightOperation::Backup => CoreCheckOperation::Backup,
        PreflightOperation::Restore => CoreCheckOperation::Restore,
        PreflightOperation::DestinationAccess => CoreCheckOperation::DestinationAccess,
        PreflightOperation::SourceConnection => CoreCheckOperation::SourceConnection,
    }
}

fn view_of(reason: &StaleReason) -> StaleReasonView {
    match reason {
        StaleReason::Expired => StaleReasonView::plain(StaleReasonKind::Expired),
        StaleReason::PlanHashChanged => StaleReasonView::plain(StaleReasonKind::PlanHashChanged),
        StaleReason::ReferentChanged(subject) => {
            // `<Kind>/<name>`, as core renders it. A Kubernetes name cannot
            // contain `/`, so the first one splits it.
            let (kind, name) = match subject.split_once('/') {
                Some((kind, name)) => (kind.to_string(), Some(name.to_string())),
                None => (subject.clone(), None),
            };
            StaleReasonView {
                reason: StaleReasonKind::ReferentChanged,
                kind: Some(crate::validate::bounded(&kind, 128)),
                name: name.map(|n| crate::validate::bounded(&n, 253)),
                basis: None,
            }
        }
        StaleReason::CaBundleChanged => StaleReasonView::plain(StaleReasonKind::CaBundleChanged),
        StaleReason::PolicyChanged => StaleReasonView::plain(StaleReasonKind::PolicyChanged),
        StaleReason::InputsDigestChanged => {
            StaleReasonView::plain(StaleReasonKind::InputsDigestChanged)
        }
    }
}

/// D2 §6.6's applicability, recomputed per read — from STRUCTURED FIELDS.
///
/// `draft_plan_hash` is the `?planHash=` the caller sent: the hash of the plan
/// they are looking at RIGHT NOW. `live` is every recorded referent read back
/// through the sealed adapter.
///
/// The comparison itself is `check_contract::stale_reasons`, so the API and
/// the controller cannot each implement half of the rule. What this function
/// adds is the part core has no vocabulary for: everything it could NOT
/// compare becomes [`StaleReasonKind::Unverifiable`] with a cause, and never
/// an absence of reasons.
fn staleness(
    object: &PreflightCr,
    now: DateTime<Utc>,
    draft_plan_hash: Option<&str>,
    live: Option<&LiveBinding>,
) -> (bool, Vec<StaleReasonView>, Vec<String>) {
    let namespace = object.meta().namespace.clone().unwrap_or_default();
    let namespace = namespace.as_str();
    let status = object.status.as_ref();
    let Some(result) = status.and_then(|s| s.result.as_ref()) else {
        // NOTHING HAS BEEN DECIDED YET. A check with no result has no verdict
        // to be stale; `applicable` is false because it has not completed, and
        // saying "stale" here would name a problem that does not exist.
        return (false, Vec::new(), Vec::new());
    };
    let Some(binding) = status.and_then(|s| s.binding.as_ref()) else {
        return (
            true,
            vec![StaleReasonView::unverifiable(None, None, BASIS_NO_BINDING)],
            Vec::new(),
        );
    };
    // A CALLER THAT DID NOT READ THE LIVE OBJECTS SAYS SO. Handing this
    // function an empty `LiveBinding` would compare nothing and report
    // nothing, which is `applicable: true` for a verdict nobody re-checked —
    // so "I did not recompute" is a distinct, explicit input.
    //
    // EXCEPT THE ONE THING THAT NEEDS NO READ (P14, poc-upgrade-2): whether
    // the check's own validity has passed. That is the recorded `expiresAt`
    // against the clock, and a replay that left it out answered an expired
    // check as merely "not recomputed" -- so a client asking the same question
    // again after the expiry was handed the old check with nothing in the
    // answer saying it had lapsed. It is core's rule, not a copy of it: the
    // same `stale_reasons` over the recorded inputs compared with themselves,
    // which can only ever report `expired`.
    let Some(live) = live else {
        let operation = core_operation(object.spec.request.operation);
        let recorded = binding_inputs(
            operation,
            binding.plan_hash.clone(),
            Vec::new(),
            binding.policy_digest.clone().unwrap_or_default(),
        );
        let mut reasons: Vec<StaleReasonView> =
            stale_reasons(&recorded, result.expires_at, &recorded, now)
                .iter()
                .map(view_of)
                .collect();
        reasons.push(StaleReasonView::unverifiable(
            None,
            None,
            BASIS_NOT_RECOMPUTED,
        ));
        return (true, reasons, vec![COVERED_EXPIRY.to_string()]);
    };

    let mut covered = vec![COVERED_EXPIRY.to_string()];
    let mut unverifiable = Vec::new();
    let mut recorded_referents = Vec::new();
    let mut current_referents = Vec::new();
    for (referent, reading) in &live.readings {
        match reading {
            ReferentReading::Unreadable(basis) => unverifiable.push(StaleReasonView::unverifiable(
                Some(crate::validate::bounded(&referent.kind, 128)),
                Some(crate::validate::bounded(&referent.name, 253)),
                basis,
            )),
            ReferentReading::Absent => {
                // Recorded and gone: core renders it `referentChanged` because
                // only one side has it.
                recorded_referents.push(core_referent(
                    &referent.kind,
                    namespace,
                    &referent.name,
                    referent.uid.as_deref().unwrap_or_default(),
                    referent.generation,
                ));
            }
            ReferentReading::Live { uid, generation } => {
                // A REFERENT RECORDED WITHOUT A GENERATION IS BOUND BY UID
                // ALONE — BUT ONLY FOR THE KINDS THE CONTROLLER RECORDS THAT
                // WAY. `logweir_core::check_contract::Referent::generation`
                // is `None` "for a kind whose generation is not meaningful" —
                // the controller records the recovery-point `Backup`, the
                // `Approval` and a catalog point's `RecoveryCatalog` that way
                // — so the live side is compared on the same terms. Comparing
                // a live `Some(n)` against a recorded `None` reported
                // `referentChanged` for EVERY restore check that names a
                // recovery point, on every re-read (found by PLAT-08.2's live
                // journey); a recreated object still differs by uid and is
                // still reported. Any other kind without a generation is
                // `unverifiable` (see [`UID_BOUND_KINDS`]).
                let current_generation = match referent.generation {
                    Some(_) => *generation,
                    None if UID_BOUND_KINDS.contains(&referent.kind.as_str()) => None,
                    None => {
                        unverifiable.push(StaleReasonView::unverifiable(
                            Some(crate::validate::bounded(&referent.kind, 128)),
                            Some(crate::validate::bounded(&referent.name, 253)),
                            BASIS_GENERATION_NOT_RECORDED,
                        ));
                        continue;
                    }
                };
                recorded_referents.push(core_referent(
                    &referent.kind,
                    namespace,
                    &referent.name,
                    referent.uid.as_deref().unwrap_or_default(),
                    referent.generation,
                ));
                current_referents.push(core_referent(
                    &referent.kind,
                    namespace,
                    &referent.name,
                    uid,
                    current_generation,
                ));
            }
        }
    }
    if !live.readings.is_empty() {
        covered.push(format!("{COVERED_REFERENTS}:{}", live.readings.len()));
    }

    // The policy digest, when it can be seen at all. See
    // [`COVERED_POLICY_BY_CONTROLLER`] for why an unreadable policy is a basis
    // line rather than a refusal to answer.
    let recorded_policy = binding.policy_digest.clone().unwrap_or_default();
    let current_policy = match &live.policy_digest {
        Some(digest) => {
            covered.push(COVERED_POLICY_DIGEST.to_string());
            digest.clone()
        }
        None => {
            covered.push(COVERED_POLICY_BY_CONTROLLER.to_string());
            recorded_policy.clone()
        }
    };

    let operation = core_operation(object.spec.request.operation);
    let recorded_inputs = binding_inputs(
        operation,
        binding.plan_hash.clone(),
        recorded_referents,
        recorded_policy,
    );
    let current_inputs = binding_inputs(
        operation,
        // A caller who sent no draft hash is not claiming to hold one, so the
        // recorded hash is compared with itself and contributes nothing.
        draft_plan_hash
            .map(str::to_string)
            .or_else(|| binding.plan_hash.clone()),
        current_referents,
        current_policy,
    );
    if draft_plan_hash.is_some() {
        covered.push(COVERED_PLAN_HASH.to_string());
    }

    let mut reasons: Vec<StaleReasonView> =
        stale_reasons(&recorded_inputs, result.expires_at, &current_inputs, now)
            .iter()
            .map(view_of)
            .collect();
    for reason in unverifiable {
        if !reasons.contains(&reason) {
            reasons.push(reason);
        }
    }
    let stale = !reasons.is_empty();
    (stale, reasons, covered)
}

/// A `Preflight` as the product DTO.
#[must_use]
pub fn project(
    object: &PreflightCr,
    now: DateTime<Utc>,
    draft_plan_hash: Option<&str>,
    live: Option<&LiveBinding>,
) -> Preflight {
    let meta = object.meta();
    let status = object.status.as_ref();
    let result = status.and_then(|s| s.result.as_ref());
    let state = state_of(object);
    let (stale, stale_reasons, stale_basis) = staleness(object, now, draft_plan_hash, live);
    let entries: Vec<CheckEntryView> = result
        .and_then(|r| r.checks.as_ref())
        .map(|c| c.iter().map(entry_view).collect())
        .unwrap_or_default();
    let (warnings, checks): (Vec<CheckEntryView>, Vec<CheckEntryView>) =
        entries.iter().cloned().partition(|e| {
            e.gating == Some(CheckGating::Advisory) && e.state == CheckVerdict::NotReady
        });
    let execution_only: Vec<ExecutionOnlyView> = entries
        .iter()
        .filter(|e| e.gating == Some(CheckGating::ExecutionOnly))
        .map(|e| ExecutionOnlyView {
            id: e.id.clone(),
            note: e.message.clone().unwrap_or_else(|| {
                "This permission is verified only when the run executes.".to_string()
            }),
        })
        .collect();
    let completed = matches!(
        state,
        PreflightState::Ready | PreflightState::NotReady | PreflightState::Unknown
    );
    Preflight {
        id: meta.name.clone().unwrap_or_default(),
        namespace: meta.namespace.clone().unwrap_or_default(),
        uid: meta.uid.clone().unwrap_or_default(),
        resource_version: meta.resource_version.clone().unwrap_or_default(),
        created_at: meta.creation_timestamp.as_ref().map(|t| t.0),
        operation: operation_dto(object.spec.request.operation),
        state,
        reason: status.and_then(|s| s.reason.clone()),
        terminal: is_terminal(state),
        binding: PreflightBindingView {
            plan_hash: status
                .and_then(|s| s.binding.as_ref())
                .and_then(|b| b.plan_hash.clone()),
            inputs_digest: status
                .and_then(|s| s.binding.as_ref())
                .and_then(|b| b.inputs_digest.clone()),
            referents: status
                .and_then(|s| s.binding.as_ref())
                .and_then(|b| b.referents.as_ref())
                .map(|r| {
                    r.iter()
                        .take(16)
                        .map(|r| ReferentView {
                            kind: r.kind.clone(),
                            name: r.name.clone(),
                            uid: r.uid.clone(),
                            generation: r.generation,
                        })
                        .collect()
                })
                .unwrap_or_default(),
        },
        applicable: completed && !stale,
        stale,
        stale_reasons,
        stale_basis,
        observed_at: status.and_then(|s| s.observed_at.as_ref().copied()),
        expires_at: result.and_then(|r| r.expires_at.as_ref().copied()),
        checks,
        warnings,
        execution_only,
        details_available: result.and_then(|r| r.details_ref.as_ref()).is_some(),
        conditions: status
            .and_then(|s| s.conditions.as_ref())
            .map(|c| c.iter().take(MAX_CONDITIONS).map(condition_view).collect())
            .unwrap_or_default(),
    }
}

// ======================================================================
// Approver narrowing
// ======================================================================

/// D0: an approver reads readiness "only when needed for the approval packet".
///
/// An actor whose ONLY binding in this namespace is Approver may read a
/// Restore preflight — the readiness of the thing it is being asked to
/// authorize — and nothing else. It answers `not_found` rather than
/// `forbidden`, for the same enumeration reason an ungranted namespace does:
/// an approver must not be able to map which backup checks exist.
fn narrow_for_approver(
    state: &AppState,
    actor: &Actor,
    namespace: &str,
    object: &PreflightCr,
) -> Result<(), ApiError> {
    let roles = state.authorizer().roles(actor, namespace);
    let approver_only = roles.contains(&Role::Approver)
        && !roles
            .iter()
            .any(|r| matches!(r, Role::Viewer | Role::Operator | Role::Administrator));
    if approver_only && object.spec.request.operation != PreflightOperation::Restore {
        actor.audit.set_failure("forbidden");
        return Err(ApiError::not_found());
    }
    Ok(())
}

// ======================================================================
// Validation
// ======================================================================

fn check_reference(field: &str, name: &str, errors: &mut Vec<FieldError>) {
    if !validate::is_dns_subdomain(name) {
        errors.push(FieldError::new(
            field,
            "invalid_name",
            "must be a Kubernetes object name",
        ));
    }
}

/// Validate a create request.
///
/// # Errors
///
/// `validation_failed` naming every invalid field.
pub fn validate_create(request: &CreatePreflightRequest) -> Result<(), ApiError> {
    let mut errors = Vec::new();
    let blocks = [
        ("backup", request.backup.is_some()),
        ("restore", request.restore.is_some()),
        ("destinationAccess", request.destination_access.is_some()),
        ("sourceConnection", request.source_connection.is_some()),
    ];
    let expected = match request.operation {
        PreflightOperationDto::Backup => "backup",
        PreflightOperationDto::Restore => "restore",
        PreflightOperationDto::DestinationAccess => "destinationAccess",
        PreflightOperationDto::SourceConnection => "sourceConnection",
    };
    for (name, present) in blocks {
        if present && name != expected {
            errors.push(FieldError::new(
                name,
                "block_mismatch",
                format!("operation `{expected}` takes the `{expected}` block and no other"),
            ));
        }
    }
    if !blocks
        .iter()
        .any(|(name, present)| *name == expected && *present)
    {
        errors.push(FieldError::new(
            expected,
            "required",
            format!("operation `{expected}` needs the `{expected}` block"),
        ));
    }
    if let Some(backup) = &request.backup {
        check_reference(
            "backup.sourceConnection",
            &backup.source_connection,
            &mut errors,
        );
        match (&backup.destination, &backup.legacy_archive) {
            (Some(destination), None) => {
                check_reference("backup.destination", destination, &mut errors);
            }
            (None, Some(archive)) => {
                super::schedules::validate_archive("backup.legacyArchive", archive, &mut errors);
            }
            _ => errors.push(FieldError::new(
                "backup.destination",
                "exactly_one",
                "set exactly one of destination or legacyArchive",
            )),
        }
        if backup.topics.is_empty() || backup.topics.len() > 1000 {
            errors.push(FieldError::new(
                "backup.topics",
                "count_out_of_range",
                "between 1 and 1000 named topics are required",
            ));
        }
        for (i, topic) in backup.topics.iter().enumerate() {
            if !validate::is_topic_name(topic) {
                errors.push(FieldError::new(
                    format!("backup.topics[{i}]"),
                    "invalid_topic",
                    "must be a Kafka topic name; patterns are refused",
                ));
            }
        }
        if let Some(schedule) = &backup.schedule {
            check_reference("backup.schedule", schedule, &mut errors);
        }
    }
    if let Some(restore) = &request.restore {
        match (&restore.plan_bytes, &restore.restore_name) {
            (Some(bytes), None) => {
                if bytes.is_empty() || bytes.len() > MAX_PLAN_BYTES {
                    errors.push(FieldError::new(
                        "restore.planBytes",
                        "out_of_range",
                        format!("the plan is 1 to {MAX_PLAN_BYTES} bytes"),
                    ));
                }
                match &restore.plan_hash {
                    None => errors.push(FieldError::new(
                        "restore.planHash",
                        "required",
                        "a draft needs the sha256 of exactly these bytes",
                    )),
                    Some(hash) => {
                        let computed = logweir_core::ids::sha256_prefixed(bytes.as_bytes());
                        if hash != &computed {
                            // THE HASH IS COMPARED, NOT TRUSTED, and the
                            // MESSAGE DOES NOT ECHO EITHER VALUE: a plan hash
                            // in an error body is a fingerprint of a document
                            // the reader may not be authorized to see.
                            errors.push(FieldError::new(
                                "restore.planHash",
                                "hash_mismatch",
                                "planHash is not the sha256 of planBytes",
                            ));
                        }
                    }
                }
                match &restore.target {
                    None => errors.push(FieldError::new(
                        "restore.target",
                        "required",
                        "a draft needs the target connection",
                    )),
                    Some(target) => check_reference("restore.target", target, &mut errors),
                }
            }
            (None, Some(name)) => check_reference("restore.restoreName", name, &mut errors),
            _ => errors.push(FieldError::new(
                "restore.planBytes",
                "exactly_one",
                "set exactly one of planBytes (a draft) or restoreName (an existing Restore)",
            )),
        }
        match (
            &restore.source_destination,
            &restore.evidence_destination,
            &restore.legacy_source_archive,
        ) {
            (Some(source), Some(evidence), None) => {
                check_reference("restore.sourceDestination", source, &mut errors);
                check_reference("restore.evidenceDestination", evidence, &mut errors);
            }
            (None, None, Some(archive)) => {
                super::schedules::validate_archive(
                    "restore.legacySourceArchive",
                    archive,
                    &mut errors,
                );
            }
            (None, None, None) => {}
            _ => errors.push(FieldError::new(
                "restore.sourceDestination",
                "exactly_one",
                "source and evidence destinations are set together, and never beside a \
                 legacySourceArchive",
            )),
        }
        if let Some(point) = &restore.recovery_point {
            check_reference(
                "restore.recoveryPoint.backupName",
                &point.backup_name,
                &mut errors,
            );
        }
        // PLAT-15.2: a catalog point, named by its catalog and its
        // content-derived id — and never beside a `Backup` point, because one
        // check answers `recoveryPoint.state` about ONE point (CRD rule P10).
        if let Some(point) = &restore.catalog_point {
            if restore.recovery_point.is_some() {
                errors.push(FieldError::new(
                    "restore.catalogPoint",
                    "exactly_one",
                    "name the recovery point once: a Backup (recoveryPoint) or a catalog point \
                     (catalogPoint), not both",
                ));
            }
            check_reference("restore.catalogPoint.catalog", &point.catalog, &mut errors);
            if !is_point_id(&point.point_id) {
                errors.push(FieldError::new(
                    "restore.catalogPoint.pointId",
                    "invalid_point_id",
                    "a point id is `lwp1-` followed by 32 lowercase hexadecimal characters",
                ));
            }
        }
    }
    if let Some(access) = &request.destination_access {
        check_reference(
            "destinationAccess.destination",
            &access.destination,
            &mut errors,
        );
        if access.roles.is_empty() || access.roles.len() > 4 {
            errors.push(FieldError::new(
                "destinationAccess.roles",
                "count_out_of_range",
                "between 1 and 4 roles are required",
            ));
        }
    }
    if let Some(connection) = &request.source_connection {
        // THE ONE FIELD, AND IT IS A REFERENCE. An absent `connectionRef` is
        // refused by `deny_unknown_fields`' sibling rule — a missing required
        // field is `Category::Data`, which `read_json` turns into a 422 naming
        // `connectionRef` — and a present one that is not a Kubernetes name is
        // refused here, before an object with an unresolvable reference is
        // created for a controller to report `ConnectionNotFound` about.
        check_reference(
            "sourceConnection.connectionRef",
            &connection.connection_ref,
            &mut errors,
        );
    }
    if let Some(skip) = &request.skip_checks {
        if skip.len() > 32 {
            errors.push(FieldError::new(
                "skipChecks",
                "count_out_of_range",
                "at most 32 checks may be skipped",
            ));
        }
    }
    if let Some(timeout) = request.timeout_seconds {
        if !(30..=600).contains(&timeout) {
            errors.push(FieldError::new(
                "timeoutSeconds",
                "out_of_range",
                "the check budget is 30 to 600 seconds",
            ));
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(ApiError::validation(errors))
    }
}

/// `lwp1-` plus 32 lowercase hex — D3 §5.1's point identity, and the CRD's
/// `POINT_ID_PATTERN`.
fn is_point_id(value: &str) -> bool {
    value.strip_prefix("lwp1-").is_some_and(|hex| {
        hex.len() == 32
            && hex
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    })
}

/// Refuse the legacy marker when the named recovery point records a saved
/// destination.
///
/// `legacySourceArchive` is a compatibility shape for a point whose archive
/// location is inline in `Backup.spec.archive`. Once the point carries
/// `spec.destinationRef`, accepting the marker only creates a `Preflight` the
/// controller must fail terminally as `ArchiveUrlUnreadable`. Refuse that
/// mismatch on the request instead, naming the saved destination the caller
/// must use.
async fn validate_legacy_source_for_point(
    state: &AppState,
    namespace: &str,
    request: &CreatePreflightRequest,
) -> Result<(), ApiError> {
    let Some(restore) = request.restore.as_ref() else {
        return Ok(());
    };
    if restore.legacy_source_archive.is_none() {
        return Ok(());
    }
    let Some(point) = restore.recovery_point.as_ref() else {
        return Ok(());
    };

    match state
        .kube()
        .get::<BackupCr>(namespace, &point.backup_name)
        .await
    {
        Ok(backup) => {
            // The name is only a lookup key. When the caller froze a UID, a
            // same-name replacement is not this recovery point and its
            // destination must not be used to diagnose or advise this
            // request. Let the stored preflight's ordinary binding check
            // report the disappeared/recreated referent instead.
            if let Some(expected_uid) = point.backup_uid.as_deref() {
                if backup.metadata.uid.as_deref() != Some(expected_uid) {
                    return Ok(());
                }
            }
            let Some(destination) = backup.spec.destination_ref.as_ref() else {
                return Ok(());
            };
            Err(ApiError::validation(vec![FieldError::new(
                "restore.legacySourceArchive",
                "destination_ref_required",
                format!(
                    "recovery point `{}` carries destinationRef `{}`; send that saved destination as sourceDestination and evidenceDestination instead of legacySourceArchive",
                    point.backup_name, destination.name
                ),
            )]))
        }
        // A missing point is still a valid subject for a readiness result:
        // `recoveryPoint.state` reports `RecoveryPointNotFound`. This guard is
        // narrower — it refuses only the contradictory shape it can prove.
        Err(KubeFailure::NotFound) => Ok(()),
        Err(other) => Err(other.into_api_error()),
    }
}

// ======================================================================
// Building the stored object
// ======================================================================

fn local(name: &str) -> LocalRef {
    LocalRef {
        name: name.to_string(),
    }
}

fn crd_role(role: DestinationRoleDto) -> DestinationRole {
    match role {
        DestinationRoleDto::ArchiveWrite => DestinationRole::ArchiveWrite,
        DestinationRoleDto::ArchiveRead => DestinationRole::ArchiveRead,
        DestinationRoleDto::EvidenceWrite => DestinationRole::EvidenceWrite,
        DestinationRoleDto::EvidenceRead => DestinationRole::EvidenceRead,
    }
}

fn archive_ref(archive: &crate::contract::ArchiveRequest) -> ArchiveRef {
    ArchiveRef {
        url: archive.url.clone(),
        secret_ref: archive.credential_ref.as_ref().map(|r| local(&r.name)),
    }
}

/// Build the stored object for a validated request.
#[must_use]
pub fn build(
    namespace: &str,
    name: String,
    annotations: BTreeMap<String, String>,
    labels: BTreeMap<String, String>,
    request: &CreatePreflightRequest,
) -> PreflightCr {
    PreflightCr {
        metadata: ObjectMeta {
            name: Some(name),
            namespace: Some(namespace.to_string()),
            annotations: Some(annotations),
            labels: Some(labels),
            ..ObjectMeta::default()
        },
        spec: PreflightSpec {
            request: PreflightRequest {
                operation: match request.operation {
                    PreflightOperationDto::Backup => PreflightOperation::Backup,
                    PreflightOperationDto::Restore => PreflightOperation::Restore,
                    PreflightOperationDto::DestinationAccess => {
                        PreflightOperation::DestinationAccess
                    }
                    PreflightOperationDto::SourceConnection => PreflightOperation::SourceConnection,
                },
                backup: request.backup.as_ref().map(|b| CrdBackupRequest {
                    source_ref: local(&b.source_connection),
                    destination_ref: b.destination.as_deref().map(local),
                    legacy_archive: b.legacy_archive.as_ref().map(archive_ref),
                    topics: b.topics.clone(),
                    schedule_ref: b.schedule.as_deref().map(local),
                }),
                restore: request.restore.as_ref().map(|r| CrdRestoreRequest {
                    // VERBATIM. The bytes go in as they arrived; nothing here
                    // parses or re-serializes a plan document.
                    plan_bytes: r.plan_bytes.clone(),
                    plan_hash: r.plan_hash.clone(),
                    restore_ref: r.restore_name.as_ref().map(|name| UidRef {
                        name: name.clone(),
                        uid: None,
                    }),
                    target_ref: r.target.as_deref().map(local),
                    source_destination_ref: r.source_destination.as_deref().map(local),
                    evidence_destination_ref: r.evidence_destination.as_deref().map(local),
                    legacy_source_archive: r.legacy_source_archive.as_ref().map(archive_ref),
                    recovery_point_ref: r.recovery_point.as_ref().map(|p| UidRef {
                        name: p.backup_name.clone(),
                        uid: p.backup_uid.clone(),
                    }),
                    catalog_point_ref: r.catalog_point.as_ref().map(|p| CatalogPointRef {
                        catalog_ref: local(&p.catalog),
                        point_id: p.point_id.clone(),
                    }),
                }),
                destination_access: request.destination_access.as_ref().map(|d| {
                    DestinationAccessRequest {
                        destination_ref: local(&d.destination),
                        roles: d.roles.iter().copied().map(crd_role).collect(),
                    }
                }),
                source_connection: request.source_connection.as_ref().map(|c| {
                    CrdSourceConnectionRequest {
                        connection_ref: local(&c.connection_ref),
                    }
                }),
                skip_checks: request.skip_checks.clone(),
                timeout_seconds: request
                    .timeout_seconds
                    .unwrap_or(weirkeeper::crds::preflight::DEFAULT_TIMEOUT_SECONDS),
            },
            cancel_requested: false,
        },
        status: None,
    }
}

fn labels_for(request: &CreatePreflightRequest) -> BTreeMap<String, String> {
    let mut labels = BTreeMap::new();
    if let Some(access) = &request.destination_access {
        labels.insert(
            super::destinations::DESTINATION_LABEL.to_string(),
            access.destination.clone(),
        );
        // A SECOND LABEL ONLY AN ACCESS TEST CARRIES, so `lastTest` selects on
        // it alone and never competes with the Backup readiness checks that
        // share the first one.
        labels.insert(
            super::destinations::DESTINATION_TEST_LABEL.to_string(),
            access.destination.clone(),
        );
    }
    if let Some(backup) = &request.backup {
        if let Some(destination) = &backup.destination {
            labels.insert(
                super::destinations::DESTINATION_LABEL.to_string(),
                destination.clone(),
            );
        }
    }
    if let Some(connection) = &request.source_connection {
        // ONE LABEL, NOT TWO. `destinationAccess` sets a general
        // `logweir.dev/destination` beside its test label because Backup
        // readiness checks share the first one and the "what uses this
        // destination" view reads it. Nothing reads a general connection label
        // on a `Preflight`, and a label nothing reads is a label that drifts,
        // so a connectivity check carries exactly the one that finds it again:
        // `connections::get_one`'s `lastTest`.
        labels.insert(
            super::connections::CONNECTION_TEST_LABEL.to_string(),
            connection.connection_ref.clone(),
        );
    }
    labels
}

// ======================================================================
// Routes
// ======================================================================

/// `POST .../preflights`.
pub async fn create(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    actor: Actor,
    ApiPath(ns): ApiPath<String>,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, ApiError> {
    authorize(&state, &actor, &ns, Action::RunPreflight)?;
    crate::http::parse_query(uri.query(), &[])?;
    let key = IdempotencyKey::from_headers(&headers)?;
    let mut request: CreatePreflightRequest = read_json(body, MAX_JSON_BODY).await?;
    validate_create(&request)?;
    check_create_rate(
        &state,
        &actor,
        &ns,
        ROUTE_CREATE,
        PREFLIGHT_CREATES_PER_MINUTE,
    )?;
    // The limiter is the last synchronous gate: structurally invalid input is
    // still refused precisely, but an over-limit request performs no
    // recovery-point or other Kubernetes read.
    validate_legacy_source_for_point(&state, &ns, &request).await?;
    // Canonical form: an omitted budget IS the default, so both spellings hash
    // identically and replay as one request.
    request.timeout_seconds = Some(
        request
            .timeout_seconds
            .unwrap_or(weirkeeper::crds::preflight::DEFAULT_TIMEOUT_SECONDS),
    );
    let labels = labels_for(&request);
    let created = create_idempotent(
        &state,
        &actor,
        &ns,
        ROUTE_CREATE,
        NAME_PREFIX,
        &key,
        &request_id,
        &request,
        |name, annotations| build(&ns, name, annotations, labels.clone(), &request),
    )
    .await?;
    Ok(json(
        if created.replayed {
            StatusCode::OK
        } else {
            StatusCode::ACCEPTED
        },
        &PreflightResponse {
            request_id,
            replayed: Some(created.replayed),
            // A BRAND-NEW CHECK HAS NO VERDICT TO BE STALE, so there is
            // nothing to recompute against.
            item: project(&created.object, state.now(), None, None),
        },
    ))
}

/// Start a `DestinationAccess` preflight for `POST .../destinations/{name}:test`.
///
/// # Errors
///
/// The create's own failures.
pub async fn start_destination_access(
    state: &AppState,
    actor: &Actor,
    namespace: &str,
    destination: &str,
    roles: &[DestinationRoleDto],
    key: &IdempotencyKey,
    request_id: &str,
) -> Result<Created<PreflightCr>, ApiError> {
    check_create_rate(
        state,
        actor,
        namespace,
        ROUTE_DESTINATION_TEST,
        PREFLIGHT_CREATES_PER_MINUTE,
    )?;
    let request = CreatePreflightRequest {
        operation: PreflightOperationDto::DestinationAccess,
        backup: None,
        restore: None,
        destination_access: Some(crate::contract::DestinationAccessPreflightRequest {
            destination: destination.to_string(),
            roles: roles.to_vec(),
        }),
        source_connection: None,
        skip_checks: None,
        timeout_seconds: Some(weirkeeper::crds::preflight::DEFAULT_TIMEOUT_SECONDS),
    };
    validate_create(&request)?;
    let labels = labels_for(&request);
    create_idempotent(
        state,
        actor,
        namespace,
        ROUTE_DESTINATION_TEST,
        NAME_PREFIX,
        key,
        request_id,
        &request,
        |name, annotations| build(namespace, name, annotations, labels.clone(), &request),
    )
    .await
}

/// `GET .../preflights/{id}`.
pub async fn get_one(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    actor: Actor,
    ApiPath((ns, id)): ApiPath<(String, String)>,
    uri: Uri,
) -> Result<Response, ApiError> {
    authorize(&state, &actor, &ns, Action::ReadPreflights)?;
    let query = crate::http::parse_query(uri.query(), &["planHash"])?;
    if let Some(hash) = query.get("planHash") {
        if !is_sha256(hash) {
            return Err(ApiError::validation(vec![FieldError::new(
                "planHash",
                "invalid_hash",
                "planHash is sha256:<64 lowercase hex>",
            )]));
        }
    }
    let object = get_object::<PreflightCr>(&state, &actor, &ns, &id).await?;
    narrow_for_approver(&state, &actor, &ns, &object)?;
    // THE RECOMPUTATION THE CONTROLLER'S CONTRACT ASSIGNS TO THIS SERVICE.
    // Every recorded referent is read back before the verdict is projected;
    // this is the only route that answers a readiness verdict, so it is the
    // only one that owes the comparison.
    let live = read_live_binding(&state, &ns, &object).await;
    Ok(json(
        StatusCode::OK,
        &PreflightResponse {
            request_id,
            replayed: None,
            item: project(
                &object,
                state.now(),
                query.get("planHash").map(String::as_str),
                Some(&live),
            ),
        },
    ))
}

fn is_sha256(value: &str) -> bool {
    value.strip_prefix("sha256:").is_some_and(|hex| {
        hex.len() == 64
            && hex
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
    })
}

/// `GET .../preflights/{id}/details`.
pub async fn details(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    actor: Actor,
    ApiPath((ns, id)): ApiPath<(String, String)>,
    uri: Uri,
) -> Result<Response, ApiError> {
    authorize(&state, &actor, &ns, Action::ReadPreflights)?;
    let query = crate::http::parse_query(uri.query(), &["check", "limit", "cursor"])?;
    let limit = match query.get("limit") {
        None => DEFAULT_LIMIT,
        Some(text) => match text.parse::<u32>() {
            Ok(n) if (1..=MAX_DETAIL_PAGE).contains(&n) => n,
            _ => {
                return Err(ApiError::validation(vec![FieldError::new(
                    "limit",
                    "out_of_range",
                    format!("limit must be an integer from 1 to {MAX_DETAIL_PAGE}"),
                )]))
            }
        },
    };
    let check = query.get("check").cloned().unwrap_or_default();
    if check.len() > 128 || check.chars().any(char::is_control) {
        return Err(ApiError::validation(vec![FieldError::new(
            "check",
            "invalid_value",
            "a check id is one bounded printable line",
        )]));
    }
    let object = get_object::<PreflightCr>(&state, &actor, &ns, &id).await?;
    narrow_for_approver(&state, &actor, &ns, &object)?;
    let Some(reference) = object
        .status
        .as_ref()
        .and_then(|s| s.result.as_ref())
        .and_then(|r| r.details_ref.as_ref())
    else {
        return Ok(json(
            StatusCode::OK,
            &DetailPageResponse {
                request_id,
                items: Vec::new(),
                page: Page {
                    limit,
                    next_cursor: None,
                    snapshot: None,
                },
            },
        ));
    };
    let scope = CursorScope {
        actor_id: actor.id(),
        route: ROUTE_DETAILS.to_string(),
        namespace: ns.clone(),
        filters: format!("id={id}&check={check}"),
    };
    let offset = match query.get("cursor") {
        None => 0usize,
        Some(cursor) => open_offset(&state, &scope, cursor)?,
    };
    let document = state
        .kube()
        .get_result_document(&ns, &reference.name)
        .await
        .map_err(KubeFailureExt::details_failure)?;
    verify_document(
        &document,
        object.meta().uid.as_deref().unwrap_or_default(),
        reference.sha256.as_deref(),
    )?;
    let body = document.data.get(DETAILS_KEY).cloned().unwrap_or_default();
    let mut items = Vec::new();
    let mut scanned;
    let mut next = None;
    for (index, line) in body.lines().enumerate() {
        if index < offset {
            continue;
        }
        scanned = index + 1;
        let Ok(entry) = serde_json::from_str::<serde_json::Value>(line) else {
            // A LINE THAT IS NOT JSON IS SKIPPED, NOT GUESSED AT. The producer
            // writes JSON lines; anything else is a truncated write, and a
            // half-line rendered as prose would be a detail nobody wrote.
            continue;
        };
        let owner = entry
            .get("check")
            .or_else(|| entry.get("id"))
            .and_then(serde_json::Value::as_str)
            .map(str::to_string);
        if !check.is_empty() && owner.as_deref() != Some(check.as_str()) {
            continue;
        }
        items.push(DetailEntryView {
            check: owner,
            entry,
        });
        if items.len() as u32 >= limit {
            next = Some(cursor::seal(
                state.cursor_key(),
                &scope,
                &scanned.to_string(),
                state.now(),
            ));
            break;
        }
    }
    Ok(json(
        StatusCode::OK,
        &DetailPageResponse {
            request_id,
            items,
            page: Page {
                limit,
                next_cursor: next,
                snapshot: reference.sha256.clone(),
            },
        },
    ))
}

fn open_offset(state: &AppState, scope: &CursorScope, cursor: &str) -> Result<usize, ApiError> {
    match cursor::open(state.cursor_key(), scope, cursor, state.now()) {
        Ok(token) => token.parse::<usize>().map_err(|_| {
            ApiError::new(
                ProblemCode::CursorInvalid,
                "The cursor is not valid for this list; restart the list without a cursor.",
            )
        }),
        Err(CursorError::Invalid) => Err(ApiError::new(
            ProblemCode::CursorInvalid,
            "The cursor is not valid for this list; restart the list without a cursor.",
        )),
        Err(CursorError::Expired) => Err(ApiError::new(
            ProblemCode::CursorExpired,
            "The cursor expired; restart the list without a cursor.",
        )),
    }
}

/// D2 §5.6's integrity rule, applied to every stored result this API serves.
pub(crate) fn verify_document(
    document: &crate::kube::ResultDocument,
    owner_uid: &str,
    expected_sha256: Option<&str>,
) -> Result<(), ApiError> {
    let integrity = |why: &str| {
        ApiError::new(
            ProblemCode::ResultIntegrityFailed,
            format!(
                "A stored result document failed its integrity check ({why}). The page is \
                 refused rather than served from bytes whose provenance did not hold."
            ),
        )
    };
    if document.controller_owner_uid() != Some(owner_uid) {
        return Err(integrity("it is not owned by this check"));
    }
    if document.immutable != Some(true) {
        return Err(integrity("it is not immutable"));
    }
    let Some(expected) = expected_sha256 else {
        return Err(integrity("the status records no digest for it"));
    };
    if document.annotation(SHA256_ANNOTATION) != Some(expected) {
        return Err(integrity(
            "its recorded digest is not the one the status indexes",
        ));
    }
    let body: String = document.data.values().cloned().collect();
    if logweir_core::ids::sha256_prefixed(body.as_bytes()) != expected {
        return Err(integrity("its bytes do not hash to its recorded digest"));
    }
    Ok(())
}

trait KubeFailureExt {
    fn details_failure(self) -> ApiError;
}

impl KubeFailureExt for crate::kube::KubeFailure {
    fn details_failure(self) -> ApiError {
        match self {
            crate::kube::KubeFailure::NotFound => ApiError::new(
                ProblemCode::NotFound,
                "The stored result was collected with its check.",
            ),
            other => other.into_api_error(),
        }
    }
}

/// `POST .../preflights/{id}:cancel`.
pub async fn cancel(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    actor: Actor,
    ApiPath((ns, target)): ApiPath<(String, String)>,
    uri: Uri,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    authorize(&state, &actor, &ns, Action::CancelPreflight)?;
    let Some(id) = target.strip_suffix(CANCEL) else {
        return Err(ApiError::new(ProblemCode::NotFound, "No such command."));
    };
    check_name(id)?;
    crate::http::parse_query(uri.query(), &[])?;
    IdempotencyKey::refuse_on(&headers, ROUTE_CANCEL, None)?;
    let (object, already_terminal) = cancel_check::<PreflightCr>(
        &state,
        &actor,
        &ns,
        id,
        |o| is_terminal(state_of(o)),
        |o| o.spec.cancel_requested,
    )
    .await?;
    let projected = project(&object, state.now(), None, None);
    Ok(json(
        StatusCode::OK,
        &CancelResponse {
            request_id,
            id: projected.id,
            state: serde_json::to_value(projected.state)
                .ok()
                .and_then(|v| v.as_str().map(str::to_string))
                .unwrap_or_default(),
            already_terminal,
        },
    ))
}

/// `GET .../operations/preflight/{name}`.
pub async fn operation(
    state: &AppState,
    request_id: String,
    actor: &Actor,
    namespace: &str,
    name: &str,
) -> Result<Response, ApiError> {
    authorize(state, actor, namespace, Action::ReadPreflights)?;
    let object = get_object::<PreflightCr>(state, actor, namespace, name).await?;
    narrow_for_approver(state, actor, namespace, &object)?;
    let projected = project(&object, state.now(), None, None);
    Ok(json(
        StatusCode::OK,
        &CheckOperationResponse {
            request_id,
            item: CheckOperation {
                kind: CheckOperationKind::Preflight,
                name: projected.id,
                namespace: projected.namespace,
                uid: projected.uid,
                resource_version: projected.resource_version,
                created_at: projected.created_at,
                state: lifecycle(projected.state),
                state_reason: projected.reason,
                message: object
                    .status
                    .as_ref()
                    .and_then(|s| s.message.clone())
                    .map(|m| validate::bounded(&m, 1024)),
                terminal: projected.terminal,
                cancellable: !projected.terminal && !object.spec.cancel_requested,
                observed_at: projected.observed_at,
                conditions: projected.conditions,
            },
        },
    ))
}

/// A preflight's aggregate, as the lifecycle a normalized operation reports.
///
/// `ready`, `notReady` and `unknown` are all "the check produced a result" —
/// the VERDICT is `Preflight.state`, and folding it into a lifecycle here
/// would say "failed" about a check that worked perfectly and found a problem.
const fn lifecycle(state: PreflightState) -> crate::contract::CheckLifecycle {
    use crate::contract::CheckLifecycle as L;
    match state {
        PreflightState::Pending => L::Pending,
        PreflightState::Queued => L::Queued,
        PreflightState::Running => L::Running,
        PreflightState::Ready | PreflightState::NotReady | PreflightState::Unknown => L::Succeeded,
        PreflightState::Failed => L::Failed,
        PreflightState::Cancelled => L::Cancelled,
    }
}

/// A destination exists before it is tested: the test route reads it first, so
/// a test against a name that does not exist is 404 rather than a Preflight
/// nobody can explain.
#[allow(dead_code)]
fn destination_exists(_: &BackupDestination) {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_plan_hash_query_is_the_repository_s_own_spelling() {
        assert!(is_sha256(&format!("sha256:{}", "a".repeat(64))));
        assert!(!is_sha256(&format!("sha256:{}", "A".repeat(64))));
        assert!(!is_sha256(&format!("sha256:{}", "a".repeat(63))));
        assert!(!is_sha256("deadbeef"));
    }

    #[test]
    fn an_unrecognised_phase_is_never_optimistic() {
        let mut object = PreflightCr::new(
            "pf-x",
            PreflightSpec {
                request: PreflightRequest {
                    operation: PreflightOperation::Backup,
                    backup: None,
                    restore: None,
                    destination_access: None,
                    source_connection: None,
                    skip_checks: None,
                    timeout_seconds: 120,
                },
                cancel_requested: false,
            },
        );
        object.status = Some(weirkeeper::crds::preflight::PreflightStatus {
            phase: Some("Ascendant".to_string()),
            ..Default::default()
        });
        assert_eq!(state_of(&object), PreflightState::Unknown);
    }
}
