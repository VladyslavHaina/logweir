//! Route access declarations: the ONE table that says, for every route this
//! service answers, who may reach it — and the layer that enforces it before
//! any handler runs.
//!
//! WHY A TABLE AND A LAYER, WHEN EVERY HANDLER ALREADY AUTHORIZES. Until
//! PLAT-17.2's completion every route body called `routes::authorize` as its
//! first line, and that was correct for every route that existed. It was a
//! convention, not a boundary: a route added by another stage that forgot the
//! line would have answered an authenticated viewer — or, for a route that
//! took no `Actor` at all, anybody. D0 §"Application role and namespace
//! matrix" says the namespace is checked BEFORE resource lookup on every
//! route; this module makes that a property of the router rather than of each
//! author's memory.
//!
//! * [`ROUTES`] declares one [`Access`] per `(method, path)` the router serves,
//!   public routes included, so "this route is deliberately public" is written
//!   down rather than inferred from an absence.
//! * [`enforce`] is applied ONCE over the whole of `crate::app::router`,
//!   after the last route. It looks the matched path up; a route with no
//!   declaration FAILS CLOSED (`500 internal_error`, audit code
//!   `route_access_undeclared`) and its handler never runs, and a method the
//!   table does not declare for a declared path is `405` from here, so a
//!   handler registered under an undeclared method is unreachable too.
//! * For everything but [`Access::Public`] it authenticates the actor (the
//!   `Actor` extractor, CSRF included for unsafe methods), decides the declared
//!   action(s) with the same `authz::decide` table every handler uses, records
//!   the decision in the audit record, and hands the authenticated actor to the
//!   handler so the session is not decoded twice.
//! * `tests/route_access.rs` holds the table to the router from both sides:
//!   every `.route(` in `src/app.rs` must have an entry here, every entry must
//!   be routed, and every non-public entry must answer 401 without an identity
//!   and refuse a viewer where the declared action is not a viewer's.
//!
//! THE HANDLERS KEEP THEIR OWN CHECKS. A handler that decides the same action
//! again is defence in depth and costs one table lookup; a handler that needs
//! MORE than the declared floor (a destination create that also writes a
//! credential, an approver reading only restore preflights, an operator
//! cancelling only its own check) still refines it. The layer is the floor no
//! route can fall below, not a replacement for object-level rules.

use std::sync::Arc;

use axum::extract::{FromRequestParts, MatchedPath, RawPathParams, Request, State};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use http::Method;

use crate::app::AppState;
use crate::auth::Actor;
use crate::authz::Action;
use crate::problem::{ApiError, ProblemCode};

/// Who may reach one route.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Access {
    /// No identity is consulted: the probes, the static page and its
    /// redirects, and the two sign-in steps an unauthenticated browser must be
    /// able to reach. None of them reads a namespace or a Kubernetes object.
    Public,
    /// Any authenticated actor. The route reads only the actor's own session
    /// and grants (`/session`, `/namespaces`, logout).
    Authenticated,
    /// Every listed action, in the namespace named by the `{ns}` path
    /// parameter, decided namespace first.
    Namespaced(&'static [Action]),
    /// A `POST …/{target}` command route whose action is chosen by the
    /// target's `:verb` suffix. A target that names no declared verb FAILS
    /// CLOSED — `404 not_found`, audit code `unknown_command` — before any
    /// handler runs, so a verb a handler gains without a declaration here is
    /// unreachable rather than admitted under some other verb's action.
    Command {
        /// `(suffix, action)` pairs, matched against the target's end.
        verbs: &'static [(&'static str, Action)],
    },
    /// The action in AT LEAST ONE granted namespace. For the two routes that
    /// carry no `{ns}`: the cadence preview (reads nothing) and the
    /// cluster-scoped trust-policy reads (which the handler then narrows to
    /// the namespaces the actor administers).
    AnyNamespace(Action),
}

impl Access {
    /// The declaration's kind, as the audit note `routeAccess` records it.
    #[must_use]
    pub const fn kind(self) -> &'static str {
        match self {
            Access::Public => "public",
            Access::Authenticated => "authenticated",
            Access::Namespaced(_) => "namespaced",
            Access::Command { .. } => "command",
            Access::AnyNamespace(_) => "anyNamespace",
        }
    }

    /// Every action this declaration can decide, for the guard tests.
    #[must_use]
    pub fn actions(self) -> Vec<Action> {
        match self {
            Access::Public | Access::Authenticated => Vec::new(),
            Access::Namespaced(actions) => actions.to_vec(),
            Access::Command { verbs } => verbs.iter().map(|(_, action)| *action).collect(),
            Access::AnyNamespace(action) => vec![action],
        }
    }
}

/// One declared route.
#[derive(Clone, Copy, Debug)]
pub struct RouteAccess {
    /// The HTTP method, upper case. `GET` also covers `HEAD`.
    pub method: &'static str,
    /// The route path exactly as `crate::app::router` registers it.
    pub path: &'static str,
    /// Who may reach it.
    pub access: Access,
}

const fn route(method: &'static str, path: &'static str, access: Access) -> RouteAccess {
    RouteAccess {
        method,
        path,
        access,
    }
}

const fn ns(actions: &'static [Action]) -> Access {
    Access::Namespaced(actions)
}

/// The destination commands `routes::destinations::command` dispatches.
const DESTINATION_VERBS: &[(&str, Action)] = &[
    (
        crate::routes::destinations::UPDATE_ACCESS,
        Action::ManageDestinations,
    ),
    (
        crate::routes::destinations::TEST,
        Action::ManageDestinations,
    ),
];

/// THE TABLE. One entry per `(method, path)` in `crate::app::router`.
///
/// Read it as the route half of D0's role matrix: the action named here is
/// the one `authz::Role::allows` decides, so what a viewer, operator, approver
/// or administrator may reach is this table joined with that one.
pub const ROUTES: &[RouteAccess] = &[
    // ------------------------------------------------ public by declaration
    route("GET", "/healthz", Access::Public),
    route("GET", "/readyz", Access::Public),
    route("GET", "/", Access::Public),
    route("GET", "/ui", Access::Public),
    route("GET", "/ui/", Access::Public),
    route("GET", "/ui/{*path}", Access::Public),
    route("GET", crate::auth::login::LOGIN_PATH, Access::Public),
    route("GET", crate::auth::login::CALLBACK_PATH, Access::Public),
    // ------------------------------------------ the actor's own session
    route("GET", "/api/v1/session", Access::Authenticated),
    route("POST", "/api/v1/session/logout", Access::Authenticated),
    route("GET", "/api/v1/namespaces", Access::Authenticated),
    route(
        "GET",
        "/api/v1/cadence-previews",
        Access::AnyNamespace(Action::ReadSchedules),
    ),
    // ------------------------------------------------------ connections
    route(
        "GET",
        "/api/v1/namespaces/{ns}/connections",
        ns(&[Action::ReadConnections]),
    ),
    route(
        "POST",
        "/api/v1/namespaces/{ns}/connections",
        ns(&[Action::CreateConnection]),
    ),
    route(
        "GET",
        "/api/v1/namespaces/{ns}/connections/{name}",
        ns(&[Action::ReadConnections]),
    ),
    route(
        "GET",
        "/api/v1/namespaces/{ns}/connections/{name}/topic-discoveries",
        ns(&[Action::ReadTopicDiscoveries]),
    ),
    route(
        "POST",
        "/api/v1/namespaces/{ns}/connections/{name}/topic-discoveries",
        ns(&[Action::DiscoverTopics]),
    ),
    // ----------------------------------------------------- destinations
    route(
        "GET",
        "/api/v1/namespaces/{ns}/destinations",
        ns(&[Action::ReadDestinations]),
    ),
    route(
        "POST",
        "/api/v1/namespaces/{ns}/destinations",
        ns(&[Action::ManageDestinations]),
    ),
    route(
        "POST",
        "/api/v1/namespaces/{ns}/destinations:from-legacy",
        ns(&[Action::ManageDestinations]),
    ),
    route(
        "GET",
        "/api/v1/namespaces/{ns}/destinations/{name}",
        ns(&[Action::ReadDestinations]),
    ),
    route(
        "POST",
        "/api/v1/namespaces/{ns}/destinations/{name}",
        Access::Command {
            verbs: DESTINATION_VERBS,
        },
    ),
    route(
        "GET",
        "/api/v1/namespaces/{ns}/destinations/{name}/usage",
        ns(&[Action::ReadDestinations]),
    ),
    // ------------------------------------------------ transient checks
    route(
        "GET",
        "/api/v1/namespaces/{ns}/topic-discoveries/{id}",
        ns(&[Action::ReadTopicDiscoveries]),
    ),
    route(
        "POST",
        "/api/v1/namespaces/{ns}/topic-discoveries/{id}",
        Access::Command {
            verbs: &[(
                crate::routes::topic_discoveries::CANCEL,
                Action::CancelTopicDiscovery,
            )],
        },
    ),
    route(
        "GET",
        "/api/v1/namespaces/{ns}/topic-discoveries/{id}/topics",
        ns(&[Action::ReadTopicDiscoveries]),
    ),
    route(
        "POST",
        "/api/v1/namespaces/{ns}/preflights",
        ns(&[Action::RunPreflight]),
    ),
    route(
        "GET",
        "/api/v1/namespaces/{ns}/preflights/{id}",
        ns(&[Action::ReadPreflights]),
    ),
    route(
        "POST",
        "/api/v1/namespaces/{ns}/preflights/{id}",
        Access::Command {
            verbs: &[(crate::routes::preflights::CANCEL, Action::CancelPreflight)],
        },
    ),
    route(
        "GET",
        "/api/v1/namespaces/{ns}/preflights/{id}/details",
        ns(&[Action::ReadPreflights]),
    ),
    // -------------------------------------------------------- schedules
    route(
        "GET",
        "/api/v1/namespaces/{ns}/schedules",
        ns(&[Action::ReadSchedules]),
    ),
    route(
        "POST",
        "/api/v1/namespaces/{ns}/schedules",
        ns(&[Action::CreateSchedule]),
    ),
    route(
        "GET",
        "/api/v1/namespaces/{ns}/schedules/{name}",
        ns(&[Action::ReadSchedules]),
    ),
    route(
        "PUT",
        "/api/v1/namespaces/{ns}/schedules/{name}",
        ns(&[Action::EditSchedulePolicy]),
    ),
    route(
        "POST",
        "/api/v1/namespaces/{ns}/schedules/{name}",
        Access::Command {
            verbs: &[(
                crate::routes::schedules::SET_SUSPENSION,
                Action::SetScheduleSuspension,
            )],
        },
    ),
    // ------------------------------------------ backups and restores
    route(
        "GET",
        "/api/v1/namespaces/{ns}/backups",
        ns(&[Action::ReadBackups]),
    ),
    route(
        "POST",
        "/api/v1/namespaces/{ns}/backups",
        ns(&[Action::CreateManualBackup]),
    ),
    route(
        "GET",
        "/api/v1/namespaces/{ns}/backups/{name}",
        ns(&[Action::ReadBackups]),
    ),
    route(
        "GET",
        "/api/v1/namespaces/{ns}/restores",
        ns(&[Action::ReadRestores]),
    ),
    route(
        "POST",
        "/api/v1/namespaces/{ns}/restores",
        ns(&[Action::CreateRestore]),
    ),
    route(
        "GET",
        "/api/v1/namespaces/{ns}/restores/{name}",
        ns(&[Action::ReadRestores]),
    ),
    // PLAT-19.2: a governed approver's countersignature. The handler keeps
    // its own role check (it decides by the frozen policy's mode as well).
    route(
        "POST",
        "/api/v1/namespaces/{ns}/restores/{name}/approval",
        ns(&[Action::SubmitApproval]),
    ),
    // -------------------------------------------------------- approvals
    // PLAT-19.2: the namespace's approval-policy binding, as the console
    // routes a submission by it. The handler keeps its own role check.
    route(
        "GET",
        "/api/v1/namespaces/{ns}/approval-policy",
        ns(&[Action::ReadApprovals]),
    ),
    route(
        "GET",
        "/api/v1/namespaces/{ns}/approvals",
        ns(&[Action::ReadApprovals]),
    ),
    route(
        "GET",
        "/api/v1/namespaces/{ns}/approvals/{name}",
        ns(&[Action::ReadApprovals]),
    ),
    route(
        "GET",
        "/api/v1/namespaces/{ns}/approvals/{name}/packet",
        ns(&[Action::ReadApprovalPacket]),
    ),
    // ------------------------------------ operations and the event stream
    route(
        "GET",
        "/api/v1/namespaces/{ns}/operations/{kind}/{name}",
        ns(&[Action::ReadOperations]),
    ),
    // THE STREAM NEEDS BOTH. Opening it is a read of the operation AND the
    // stream action, decided here before any slot is taken or any watch opens.
    route(
        "GET",
        "/api/v1/namespaces/{ns}/operations/{kind}/{name}/events",
        ns(&[Action::ReadOperations, Action::StreamOperationEvents]),
    ),
    // ------------------------------------------------ D3 read families
    route(
        "GET",
        "/api/v1/namespaces/{ns}/protection-policies",
        ns(&[Action::ReadProtectionPolicies]),
    ),
    route(
        "GET",
        "/api/v1/namespaces/{ns}/protection-policies/{name}",
        ns(&[Action::ReadProtectionPolicies]),
    ),
    route(
        "GET",
        "/api/v1/namespaces/{ns}/rehearsal-schedules",
        ns(&[Action::ReadRehearsalSchedules]),
    ),
    route(
        "GET",
        "/api/v1/namespaces/{ns}/rehearsal-schedules/{name}",
        ns(&[Action::ReadRehearsalSchedules]),
    ),
    route(
        "GET",
        "/api/v1/namespaces/{ns}/catalogs",
        ns(&[Action::ReadCatalogs]),
    ),
    route(
        "POST",
        "/api/v1/namespaces/{ns}/catalogs",
        ns(&[Action::ConnectCatalog]),
    ),
    route(
        "GET",
        "/api/v1/namespaces/{ns}/catalogs/{name}",
        ns(&[Action::ReadCatalogs]),
    ),
    route(
        "GET",
        "/api/v1/namespaces/{ns}/catalogs/{name}/points",
        ns(&[Action::ReadCatalogs]),
    ),
    route(
        "GET",
        "/api/v1/namespaces/{ns}/catalogs/{name}/signers",
        ns(&[Action::ReadCatalogs]),
    ),
    route(
        "GET",
        "/api/v1/namespaces/{ns}/retention-policies",
        ns(&[Action::ReadRetentionPolicies]),
    ),
    route(
        "GET",
        "/api/v1/namespaces/{ns}/retention-policies/{name}",
        ns(&[Action::ReadRetentionPolicies]),
    ),
    route(
        "GET",
        "/api/v1/trust-policies",
        Access::AnyNamespace(Action::ReadTrustPolicies),
    ),
    route(
        "GET",
        "/api/v1/trust-policies/{name}",
        Access::AnyNamespace(Action::ReadTrustPolicies),
    ),
];

/// The declaration for `(method, path)`. `HEAD` is looked up as `GET`, which
/// is how the router serves it.
#[must_use]
pub fn lookup(method: &Method, path: &str) -> Option<Access> {
    let method = if method == Method::HEAD {
        "GET"
    } else {
        method.as_str()
    };
    ROUTES
        .iter()
        .find(|r| r.path == path && r.method == method)
        .map(|r| r.access)
}

/// The methods declared for a path, in table order, `HEAD` after `GET`.
#[must_use]
pub fn declared_methods(path: &str) -> Vec<&'static str> {
    let mut out = Vec::new();
    for entry in ROUTES.iter().filter(|r| r.path == path) {
        out.push(entry.method);
        if entry.method == "GET" {
            out.push("HEAD");
        }
    }
    out
}

/// The actor this layer authenticated, handed to the handler's `Actor`
/// extractor so the session is decoded — and its CSRF token compared — once.
///
/// It can only be inserted by [`enforce`]: extensions are not reachable from
/// the wire.
#[derive(Clone, Debug)]
pub struct AuthenticatedActor(pub Actor);

/// The route layer. See the module documentation.
pub async fn enforce(State(state): State<AppState>, req: Request, next: Next) -> Response {
    // NO MATCHED PATH IS THE FALLBACK AND NOTHING ELSE. This layer is applied
    // over the whole router, so it also wraps `fallback`, which answers 404 and
    // reads nothing; every ROUTED request carries its `MatchedPath`.
    let Some(matched) = req
        .extensions()
        .get::<MatchedPath>()
        .map(|m| m.as_str().to_string())
    else {
        return next.run(req).await;
    };
    let access = match lookup(req.method(), &matched) {
        Some(access) => access,
        None if !declared_methods(&matched).is_empty() => {
            // A declared path, an undeclared method. The router would answer
            // this 405 itself unless a handler was registered for the method
            // without a declaration — and that handler must stay unreachable.
            let mut response = ApiError::new(
                ProblemCode::MethodNotAllowed,
                "The route does not accept this method.",
            )
            .into_response();
            if let Ok(allow) = http::HeaderValue::from_str(&declared_methods(&matched).join(",")) {
                response.headers_mut().insert(http::header::ALLOW, allow);
            }
            return response;
        }
        None => return refuse(&req, undeclared()),
    };
    // THE LAYER SIGNS WHAT IT DECIDED. The audit record of every routed
    // request names the declaration the layer applied, so a route that ever
    // runs outside it is visible in the log and in `tests/route_access.rs`.
    if let Some(audit) = req.extensions().get::<Arc<crate::audit::AuditContext>>() {
        audit.note("routeAccess", access.kind());
    }
    if access == Access::Public {
        return next.run(req).await;
    }

    let (mut parts, body) = req.into_parts();
    let actor = match Actor::from_request_parts(&mut parts, &state).await {
        Ok(actor) => actor,
        Err(error) => return error.into_response(),
    };
    let decision = match access {
        Access::Public | Access::Authenticated => Ok(()),
        Access::Namespaced(actions) => match namespace_of(&mut parts, &state).await {
            Ok(namespace) => decide_all(&state, &actor, &namespace, actions),
            Err(error) => Err(error),
        },
        Access::Command { verbs } => match params(&mut parts, &state).await {
            Ok((namespace, target)) => {
                match verbs.iter().find(|(suffix, _)| target.ends_with(suffix)) {
                    Some((_, action)) => {
                        crate::routes::authorize(&state, &actor, &namespace, *action)
                    }
                    None => {
                        actor.audit.set_failure("unknown_command");
                        Err(ApiError::new(ProblemCode::NotFound, "No such command."))
                    }
                }
            }
            Err(error) => Err(error),
        },
        Access::AnyNamespace(action) => decide_anywhere(&state, &actor, action),
    };
    if let Err(error) = decision {
        return error.into_response();
    }
    parts.extensions.insert(AuthenticatedActor(actor));
    next.run(Request::from_parts(parts, body)).await
}

fn undeclared() -> ApiError {
    ApiError::new(
        ProblemCode::InternalError,
        "This route has no access declaration, so it is not served.",
    )
}

fn refuse(req: &Request, error: ApiError) -> Response {
    if let Some(audit) = req.extensions().get::<Arc<crate::audit::AuditContext>>() {
        audit.set_failure("route_access_undeclared");
    }
    tracing::error!(
        path = %crate::validate::bounded(req.uri().path(), 256),
        "a routed path has no access declaration in crate::access::ROUTES; refused"
    );
    error.into_response()
}

async fn path_params(
    parts: &mut http::request::Parts,
    state: &AppState,
) -> Result<Vec<(String, String)>, ApiError> {
    RawPathParams::from_request_parts(parts, state)
        .await
        .map(|params| {
            params
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect()
        })
        .map_err(|_| ApiError::not_found())
}

async fn namespace_of(
    parts: &mut http::request::Parts,
    state: &AppState,
) -> Result<String, ApiError> {
    path_params(parts, state)
        .await?
        .into_iter()
        .find(|(k, _)| k == "ns")
        .map(|(_, v)| v)
        // A namespaced declaration on a path with no `{ns}` is a table error,
        // and it fails closed.
        .ok_or_else(undeclared)
}

async fn params(
    parts: &mut http::request::Parts,
    state: &AppState,
) -> Result<(String, String), ApiError> {
    let all = path_params(parts, state).await?;
    let namespace = all
        .iter()
        .find(|(k, _)| k == "ns")
        .map(|(_, v)| v.clone())
        .ok_or_else(undeclared)?;
    let target = all
        .iter()
        .rev()
        .find(|(k, _)| k != "ns")
        .map(|(_, v)| v.clone())
        .ok_or_else(undeclared)?;
    Ok((namespace, target))
}

/// The first action is the audit record's action; the rest are recorded as
/// `alsoRequired`, exactly as a handler calling `authorize_also` would.
fn decide_all(
    state: &AppState,
    actor: &Actor,
    namespace: &str,
    actions: &[Action],
) -> Result<(), ApiError> {
    let Some((first, rest)) = actions.split_first() else {
        return Err(undeclared());
    };
    crate::routes::authorize(state, actor, namespace, *first)?;
    for action in rest {
        crate::routes::authorize_also(state, actor, namespace, *action)?;
    }
    Ok(())
}

fn decide_anywhere(state: &AppState, actor: &Actor, action: Action) -> Result<(), ApiError> {
    let authorizer = state.authorizer();
    let allowed = authorizer
        .namespaces(actor)
        .iter()
        .any(|ns| authorizer.allows(actor, ns, action));
    if allowed {
        actor
            .audit
            .set_action(action.name(), crate::audit::Decision::Allow);
        return Ok(());
    }
    actor
        .audit
        .set_action(action.name(), crate::audit::Decision::Deny);
    actor.audit.set_failure("forbidden");
    Err(ApiError::new(
        ProblemCode::Forbidden,
        "This route requires the action in at least one granted namespace.",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_entry_is_unique_and_names_a_real_method() {
        let mut seen = std::collections::BTreeSet::new();
        for entry in ROUTES {
            assert!(
                matches!(entry.method, "GET" | "POST" | "PUT"),
                "{} {}",
                entry.method,
                entry.path
            );
            assert!(
                seen.insert((entry.method, entry.path)),
                "declared twice: {} {}",
                entry.method,
                entry.path
            );
        }
    }

    #[test]
    fn head_is_looked_up_as_get_and_nothing_else_is_aliased() {
        assert_eq!(
            lookup(&Method::HEAD, "/api/v1/session"),
            Some(Access::Authenticated)
        );
        assert_eq!(lookup(&Method::DELETE, "/api/v1/session"), None);
        assert_eq!(lookup(&Method::GET, "/api/v1/nowhere"), None);
    }

    #[test]
    fn every_namespaced_route_has_a_namespace_parameter_and_nothing_else_does() {
        for entry in ROUTES {
            let has_ns = entry.path.contains("{ns}");
            match entry.access {
                Access::Namespaced(actions) => {
                    assert!(has_ns, "{}", entry.path);
                    assert!(!actions.is_empty(), "{}", entry.path);
                }
                Access::Command { .. } => assert!(has_ns, "{}", entry.path),
                Access::Public | Access::Authenticated | Access::AnyNamespace(_) => {
                    assert!(
                        !has_ns,
                        "{} carries {{ns}} but is not namespaced",
                        entry.path
                    );
                }
            }
        }
    }

    #[test]
    fn only_implemented_actions_are_declared() {
        for entry in ROUTES {
            for action in entry.access.actions() {
                assert!(action.implemented(), "{} {:?}", entry.path, action);
            }
        }
    }
}
