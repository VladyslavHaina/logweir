//! `GET /api/v1/trust-policies[/{name}]` — PLAT-19.1's read half: which keys
//! this installation accepts, for what, and whether anybody has checked
//! lately.
//!
//! # `unknown` is not `valid` (D3 §7.7)
//!
//! The page this replaces printed `valid` for any key not listed in
//! `status.expiredKeyIds`, *including when the object carried no status at
//! all*. So the evaluation freshness is decided HERE, on the server, and a key
//! whose evaluation is not fresh is published with `effectiveState: unknown`
//! and no usability verdict — not with the last verdict anybody happened to
//! write. An evaluation is fresh only when the object has a status, that
//! status was computed from the CURRENT generation, and `evaluatedAt` is
//! younger than [`FRESH_EVALUATION_SECONDS`].
//!
//! THE CLOCK IS THE SERVER'S. D3 §7.7 says freshness is measured against a
//! clock the cluster saw, never the browser's, so the comparison happens in
//! this process and the response carries `serverTime` beside `evaluatedAt` —
//! the two numbers the verdict was computed from, so a reader can check the
//! arithmetic instead of redoing it against a clock nobody trusts.
//!
//! # No public key bytes, and no write
//!
//! `spec.keys[].spkiPem` is public material and is still not published here:
//! `docs/keys.md`'s rule is that a key arriving beside an archive is never
//! trusted by proximity, and a console that could display a PEM is a console
//! somebody will copy one out of. What is published is the key ID — the
//! SHA-256 of the DER SPKI, which is the number `openssl` prints — so an
//! operator compares fingerprints out of band, which is the supported path.
//! There is no write route: D3 §10 keeps trust administration on
//! `kubectl apply` and the `logweir trust` helpers, and
//! `capabilities.trustAdministration` is advertised `false` for everybody.

use axum::extract::State;
use axum::response::Response;
use chrono::{DateTime, Utc};
use http::{StatusCode, Uri};
use kube::ResourceExt;
use schemars::JsonSchema;
use serde::Serialize;
use weirkeeper::crds::trust_policy::TrustPolicy;

use super::{check_name, json, list_query, ApiPath};
use crate::app::AppState;
use crate::auth::Actor;
use crate::authz::{Action, Role};
use crate::contract::Page;
use crate::cursor::{self, CursorError, CursorScope};
use crate::http::RequestId;
use crate::kube::{KubeFailure, PageRequest};
use crate::problem::{ApiError, ProblemCode};
use crate::status::condition_view;
use crate::validate::bounded;

/// The cursor scope's route identifier.
pub const ROUTE_LIST: &str = "GET /api/v1/trust-policies";

/// How old an evaluation may be and still be believed. D3 §7.7's fifteen
/// minutes.
pub const FRESH_EVALUATION_SECONDS: i64 = 15 * 60;

/// The most keys and bound namespaces a response carries.
pub const MAX_ROWS: usize = 64;

/// The word a key's state gets when nothing fresh has been evaluated.
pub const UNKNOWN: &str = "unknown";

/// Why an evaluation is or is not believed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum EvaluationState {
    /// The controller evaluated the current generation recently.
    Fresh,
    /// Nothing fresh: no status, a status computed from an older generation,
    /// or an `evaluatedAt` older than [`FRESH_EVALUATION_SECONDS`]. **Not
    /// `valid`.**
    Unknown,
}

/// Why the evaluation is not fresh, when it is not.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum EvaluationReason {
    /// The controller evaluated this generation recently.
    Evaluated,
    /// The object carries no status at all.
    NotEvaluated,
    /// The status was computed from an older `metadata.generation`.
    GenerationBehind,
    /// The status has no `evaluatedAt`.
    NoEvaluationTime,
    /// `evaluatedAt` is older than the freshness window.
    Stale,
}

/// How fresh the controller's verdict is, and what it was computed from.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct EvaluationView {
    /// `fresh` or `unknown`.
    pub state: EvaluationState,
    /// Which of the four ways it is not fresh, or `evaluated`.
    pub reason: EvaluationReason,
    /// When the controller evaluated.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub evaluated_at: Option<DateTime<Utc>>,
    /// How old that is, by this server's clock.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub age_seconds: Option<i64>,
    /// The clock the comparison used. Published so a reader can check the
    /// arithmetic rather than redo it against the browser's.
    pub server_time: DateTime<Utc>,
    /// The freshness window.
    pub fresh_within_seconds: i64,
}

/// Who holds a key.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct KeyPrincipalView {
    /// The stable identity.
    pub id: String,
    /// A label. Never authority.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display: Option<String>,
}

/// One key, as the policy declares it and as the controller evaluated it.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TrustKeyView {
    /// The key ID: the SHA-256 of the DER SPKI, lowercase hex — the number
    /// `openssl` prints, and what an out-of-band comparison compares.
    pub key_id: String,
    /// `p256` or `ed25519`.
    pub algorithm: String,
    /// Exactly one of `EvidenceSigning`, `GovernedApproval` or
    /// `ConsoleConfirmation`. One key, one usage.
    pub usages: Vec<String>,
    /// Who holds it.
    pub principal: KeyPrincipalView,
    /// The start of its validity window.
    pub not_before: DateTime<Utc>,
    /// The end of it. It may only ever move earlier.
    pub not_after: DateTime<Utc>,
    /// The DECLARED lifecycle state: `Active`, `Retired` or `Revoked`.
    pub state: String,
    /// When it was retired.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retired_at: Option<DateTime<Utc>>,
    /// When it was revoked.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub revoked_at: Option<DateTime<Utc>>,
    /// `KeyCompromise`, `Superseded` or `Unspecified`. The distinction decides
    /// what happens to evidence signed before the revocation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub revocation_reason: Option<String>,
    /// From when the revocation bites.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub revocation_effective_from: Option<DateTime<Utc>>,
    /// The EVALUATED state — `Active`, `NotYetValid`, `Expired`, `Retired`,
    /// `Revoked`, `Unparseable` — or `unknown` when the evaluation is not
    /// fresh. **`valid` and `expired` are rendered only for a fresh
    /// evaluation.**
    pub effective_state: String,
    /// Whether it may sign new material. Absent when the evaluation is not
    /// fresh: an unevaluated key is not a usable one, and it is not an
    /// unusable one either.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usable_for_new_signatures: Option<bool>,
    /// `Full`, `Historical` or `None`. Absent when the evaluation is not
    /// fresh.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usable_for_verification: Option<String>,
}

/// A namespace two policies both claim. It resolves to NOTHING, and every
/// approval and verification in it is refused.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct NamespaceConflictView {
    /// The namespace.
    pub namespace: String,
    /// The policies claiming it.
    pub policies: Vec<String>,
}

/// A `TrustPolicy`, projected.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TrustPolicyView {
    /// The object name. Cluster-scoped: there is no namespace.
    pub name: String,
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
    /// Whether this is the installation default.
    pub default: bool,
    /// The namespaces it governs by name.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub namespaces: Vec<String>,
    /// Whether the namespace list was cut short.
    pub namespaces_truncated: bool,
    /// How many target cluster ids a rehearsal or restore may write to.
    pub allowed_target_cluster_ids: i64,
    /// How many keys the policy carries in total.
    pub key_count: i64,
    /// The keys, at most [`MAX_ROWS`].
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub keys: Vec<TrustKeyView>,
    /// Whether the key list was cut short.
    pub keys_truncated: bool,
    /// How fresh the verdicts above are.
    pub evaluation: EvaluationView,
    /// The generation the status was computed from.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    /// Whether the controller could load the policy at all.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub loaded: Option<bool>,
    /// The namespaces the controller resolved to this policy.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub bound_namespaces: Vec<String>,
    /// Namespaces two policies claim.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub conflicts: Vec<NamespaceConflictView>,
    /// `Loaded`, `Bound` and `ExpiringSoon`.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<crate::contract::ConditionView>,
}

/// D3 §7.7's freshness rule, in one function so no surface writes its own.
#[must_use]
pub fn evaluation(policy: &TrustPolicy, now: DateTime<Utc>) -> EvaluationView {
    let status = policy.status.as_ref();
    let evaluated_at = status.and_then(|s| s.evaluated_at);
    let age_seconds = evaluated_at.map(|at| (now - at).num_seconds());
    let reason = if status.is_none() {
        EvaluationReason::NotEvaluated
    } else if status.and_then(|s| s.observed_generation) != policy.metadata.generation {
        EvaluationReason::GenerationBehind
    } else {
        match age_seconds {
            None => EvaluationReason::NoEvaluationTime,
            // A NEGATIVE AGE IS NOT FRESH EITHER. It means the controller's
            // clock is ahead of this one, and "the future" is not a time this
            // service can say a verdict was reached at.
            Some(age) if !(0..=FRESH_EVALUATION_SECONDS).contains(&age) => EvaluationReason::Stale,
            Some(_) => EvaluationReason::Evaluated,
        }
    };
    EvaluationView {
        state: if reason == EvaluationReason::Evaluated {
            EvaluationState::Fresh
        } else {
            EvaluationState::Unknown
        },
        reason,
        evaluated_at,
        age_seconds,
        server_time: now,
        fresh_within_seconds: FRESH_EVALUATION_SECONDS,
    }
}

/// Project one trust policy at `now`.
#[must_use]
pub fn view(policy: &TrustPolicy, now: DateTime<Utc>) -> TrustPolicyView {
    let spec = &policy.spec;
    let status = policy.status.as_ref();
    let evaluation = evaluation(policy, now);
    let fresh = evaluation.state == EvaluationState::Fresh;
    let verdicts = status.and_then(|s| s.keys.as_ref());
    let keys = spec
        .keys
        .iter()
        .take(MAX_ROWS)
        .map(|key| {
            let verdict = verdicts
                .into_iter()
                .flatten()
                .find(|v| v.key_id == key.key_id)
                .filter(|_| fresh);
            TrustKeyView {
                key_id: bounded(&key.key_id, 64),
                algorithm: format!("{:?}", key.algorithm),
                usages: key.usages.iter().map(|u| format!("{u:?}")).collect(),
                principal: KeyPrincipalView {
                    id: bounded(&key.principal.id, 253),
                    display: key.principal.display.as_deref().map(|d| bounded(d, 253)),
                },
                not_before: key.not_before,
                not_after: key.not_after,
                state: format!("{:?}", key.state),
                retired_at: key.retired_at,
                revoked_at: key.revoked_at,
                revocation_reason: key.revocation_reason.map(|r| format!("{r:?}")),
                revocation_effective_from: key.revocation_effective_from,
                effective_state: verdict
                    .map_or_else(|| UNKNOWN.to_string(), |v| bounded(&v.effective_state, 32)),
                usable_for_new_signatures: verdict.and_then(|v| v.usable_for_new_signatures),
                usable_for_verification: verdict
                    .and_then(|v| v.usable_for_verification.as_deref().map(|s| bounded(s, 32))),
            }
        })
        .collect();
    let namespaces = spec.namespaces.as_ref();
    TrustPolicyView {
        name: policy.name_any(),
        uid: policy.uid().unwrap_or_default(),
        resource_version: policy.resource_version().unwrap_or_default(),
        generation: policy.metadata.generation,
        created_at: policy.metadata.creation_timestamp.as_ref().map(|t| t.0),
        default: spec.default,
        namespaces: namespaces
            .into_iter()
            .flatten()
            .take(MAX_ROWS)
            .map(|n| bounded(n, 63))
            .collect(),
        namespaces_truncated: namespaces.is_some_and(|n| n.len() > MAX_ROWS),
        allowed_target_cluster_ids: spec.allowed_target_cluster_ids.as_ref().map_or(0, Vec::len)
            as i64,
        key_count: spec.keys.len() as i64,
        keys,
        keys_truncated: spec.keys.len() > MAX_ROWS,
        evaluation,
        observed_generation: status.and_then(|s| s.observed_generation),
        loaded: status.and_then(|s| s.loaded),
        bound_namespaces: status
            .and_then(|s| s.bound_namespaces.as_ref())
            .into_iter()
            .flatten()
            .take(MAX_ROWS)
            .map(|n| bounded(n, 63))
            .collect(),
        conflicts: status
            .and_then(|s| s.conflicts.as_ref())
            .into_iter()
            .flatten()
            .take(MAX_ROWS)
            .map(|c| NamespaceConflictView {
                namespace: bounded(&c.namespace, 63),
                policies: c
                    .policies
                    .iter()
                    .take(16)
                    .map(|p| bounded(p, 253))
                    .collect(),
            })
            .collect(),
        conditions: status
            .and_then(|s| s.conditions.as_ref())
            .into_iter()
            .flatten()
            .take(crate::status::MAX_CONDITIONS)
            .map(condition_view)
            .collect(),
    }
}

/// A page of trust policies.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TrustPolicyList {
    /// The request ID.
    pub request_id: String,
    /// The items on this page.
    pub items: Vec<TrustPolicyView>,
    /// Paging.
    pub page: Page,
}

/// One trust policy.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TrustPolicyResponse {
    /// The request ID.
    pub request_id: String,
    /// The item.
    pub item: TrustPolicyView,
}

/// The pseudo-namespace a cluster-scoped decision is recorded under in the
/// audit record. It is not a Kubernetes namespace and no grant can name it: a
/// namespace name is a DNS-1123 label, and `*` is not one.
pub const CLUSTER_SCOPE: &str = "*";

/// Authorize a cluster-scoped read.
///
/// "INSTALLATION ADMINISTRATOR" IS A ROLE THIS MODEL EXPRESSES AS "ADMINISTERS
/// SOMETHING". D0's bindings are per namespace and there is no cluster-wide
/// grant to check, so the nearest honest rule is: the actor must hold
/// `trustPolicy.read` — which only [`Role::Administrator`] holds — in at least
/// one namespace bound to them. An actor with no administrator binding
/// anywhere is `forbidden`, and the audit line records the decision under
/// [`CLUSTER_SCOPE`] with the namespace that carried it.
///
/// IT IS DELIBERATELY NOT "ANY VIEWER". A `TrustPolicy` lists the namespaces
/// it governs, so serving it to an actor bound in one namespace would publish
/// the shape of every other one; that is the enumeration the shared-mode 404
/// rule exists to prevent, on a cluster-scoped object where there is no 404 to
/// hide behind.
///
/// # Errors
///
/// `forbidden`.
pub fn authorize_cluster(state: &AppState, actor: &Actor) -> Result<(), ApiError> {
    let authorizer = state.authorizer();
    let granted = authorizer.namespaces(actor);
    let carrier = granted
        .iter()
        .find(|ns| authorizer.allows(actor, ns, Action::ReadTrustPolicies));
    let roles: Vec<String> = carrier
        .map(|ns| authorizer.roles(actor, ns))
        .unwrap_or_default()
        .iter()
        .map(|role| role.as_str().to_string())
        .collect();
    actor.audit.set_decision(
        CLUSTER_SCOPE,
        Action::ReadTrustPolicies.name(),
        &roles,
        &authorizer.binding_revision(),
        if carrier.is_some() {
            crate::audit::Decision::Allow
        } else {
            crate::audit::Decision::Deny
        },
    );
    match carrier {
        Some(namespace) => {
            actor.audit.note("clusterScopeCarrier", namespace);
            Ok(())
        }
        None => {
            actor.audit.set_failure("forbidden");
            Err(ApiError::new(
                ProblemCode::Forbidden,
                "Reading the installation's trust policies requires the administrator role in at \
                 least one bound namespace.",
            ))
        }
    }
}

/// Whether any role in this build may read a trust policy. A compile-time
/// assertion in function form: if the role table ever opened this action to a
/// viewer, [`authorize_cluster`]'s narrowing sentence would stop being true.
#[must_use]
pub fn roles_that_may_read() -> Vec<Role> {
    Role::ALL
        .into_iter()
        .filter(|role| role.allows(Action::ReadTrustPolicies))
        .collect()
}

/// `GET /api/v1/trust-policies`.
pub async fn list(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    actor: Actor,
    uri: Uri,
) -> Result<Response, ApiError> {
    authorize_cluster(&state, &actor)?;
    let query = list_query(uri.query())?;
    let scope = CursorScope {
        actor_id: actor.id(),
        route: ROUTE_LIST.to_string(),
        namespace: CLUSTER_SCOPE.to_string(),
        filters: query
            .label_selector
            .as_ref()
            .map(|s| format!("labelSelector={s}"))
            .unwrap_or_default(),
    };
    let continue_token = match &query.cursor {
        None => None,
        Some(cursor) => match cursor::open(state.cursor_key(), &scope, cursor, state.now()) {
            Ok(token) => Some(token),
            Err(CursorError::Invalid) => {
                return Err(ApiError::new(
                    ProblemCode::CursorInvalid,
                    "The cursor is not valid for this list; restart the list without a cursor.",
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
    let page = PageRequest {
        limit: query.limit,
        continue_token,
        label_selector: query.label_selector.clone(),
    };
    let list = state
        .kube()
        .list_cluster::<TrustPolicy>(&page)
        .await
        .map_err(KubeFailure::into_api_error)?;
    let next_cursor = list
        .metadata
        .continue_
        .as_deref()
        .filter(|token| !token.is_empty())
        .map(|token| cursor::seal(state.cursor_key(), &scope, token, state.now()));
    let now = state.now();
    Ok(json(
        StatusCode::OK,
        &TrustPolicyList {
            request_id,
            items: list.items.iter().map(|p| view(p, now)).collect(),
            page: Page {
                limit: query.limit,
                next_cursor,
                snapshot: list.metadata.resource_version.clone(),
            },
        },
    ))
}

/// `GET /api/v1/trust-policies/{name}`.
pub async fn get_one(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    actor: Actor,
    ApiPath(name): ApiPath<String>,
    uri: Uri,
) -> Result<Response, ApiError> {
    crate::http::parse_query(uri.query(), &[])?;
    authorize_cluster(&state, &actor)?;
    check_name(&name)?;
    let policy = state
        .kube()
        .get_cluster::<TrustPolicy>(&name)
        .await
        .map_err(KubeFailure::into_api_error)?;
    actor.audit.set_object(
        "TrustPolicy",
        &policy.name_any(),
        policy.uid().unwrap_or_default().as_str(),
        policy.resource_version().unwrap_or_default().as_str(),
    );
    Ok(json(
        StatusCode::OK,
        &TrustPolicyResponse {
            request_id,
            item: view(&policy, state.now()),
        },
    ))
}
