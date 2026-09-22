//! The structured audit record: who asked for what, in which namespace, and
//! whether it was allowed.
//!
//! ONE RECORD PER REQUEST, EMITTED ONCE, AT THE END. The audit middleware puts
//! an [`AuditContext`] in the request extensions; the authenticator, the
//! authorizer and the create path fill in what only they know; the middleware
//! emits the finished record on the `logweir_api::audit` tracing target once
//! the status and the latency exist. A handler cannot forget to emit one,
//! because emitting is not the handler's job.
//!
//! IT IS AN ATTRIBUTION RECORD, NOT A PROOF. The same sentence is in D0 and it
//! matters: annotations on a created object and lines on stdout are
//! correlation. The tamper-evident requester/approver record is the DSSE
//! document PLAT-19 owns, and the complementary fact that the `logweir-api`
//! ServiceAccount made the Kubernetes call comes from Kubernetes audit. Nothing
//! here claims more than "this process believed this".
//!
//! WHAT NEVER ENTERS IT. Cookies, bearer/authorization-code/refresh tokens,
//! CSRF tokens, the raw idempotency key, Secret values, kubeconfig, approval or
//! sidecar bodies, plan bytes, raw pod logs, Kafka records, and object-store
//! URLs carrying userinfo. Identity-shaped request headers are recorded BY NAME
//! ONLY, so an operator can see that a client sent `X-Remote-User` without the
//! log becoming the place that stores what it claimed. [`redact`] is applied to
//! every free-text field that did not originate in this crate, and
//! `tests/audit.rs` plants a mutant per class.

use std::collections::BTreeMap;
use std::sync::Mutex;

use serde::Serialize;

/// The tracing target every audit line carries. A deployment filters on it.
pub const TARGET: &str = "logweir_api::audit";

/// Whether the request was allowed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Decision {
    /// The actor was authenticated and authorized for the action.
    Allow,
    /// The request was refused. `failureCode` says by which check.
    Deny,
}

impl Decision {
    /// The wire spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Decision::Allow => "allow",
            Decision::Deny => "deny",
        }
    }
}

/// The record, as one JSON object.
#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AuditRecord {
    /// The request/audit ID, also returned as `X-Request-ID`.
    pub audit_id: String,
    /// The HTTP method.
    pub method: String,
    /// The request path, bounded. Never the query string.
    pub path: String,
    /// How the actor was authenticated: `localAdmin` or `oidc`.
    pub authentication_mode: String,
    /// The stable actor ID, `<issuer>#<subject>`. Empty when unauthenticated.
    pub actor_id: String,
    /// The display claim, separately, because it is never an authorization
    /// input and must not be mistaken for one.
    pub display_claim: String,
    /// The SHA-256 of the session ID. The session ID itself is never logged.
    pub session_id_hash: String,
    /// The revision of the role-binding configuration this decision used.
    pub binding_revision: String,
    /// The roles the actor held in `namespace`, sorted.
    pub roles: Vec<String>,
    /// The namespace, when the route has one.
    pub namespace: String,
    /// The product action.
    pub action: String,
    /// The product resource, `<kind>/<name>` when known.
    pub resource: String,
    /// `allow` or `deny`.
    pub decision: String,
    /// The approval policy identity/digest, when a decision consulted one:
    /// `<policy name>@<snapshot digest>`, or `legacy-governed-v1` for an
    /// unbound namespace (PLAT-19.2).
    pub policy_digest: String,
    /// The SHA-256 of the idempotency scope. The raw key never appears.
    pub idempotency_key_hash: String,
    /// The SHA-256 of the canonical validated request.
    pub request_hash: String,
    /// The plan hash, when the request carried one. Never the plan bytes.
    pub plan_hash: String,
    /// The recovery point a restore-shaped request selected — backup set,
    /// point in time and the saved destination (or the credential-free
    /// archive location) it is read from — as the one-line form
    /// [`recovery_point_of`] builds. Empty for every other request.
    pub recovery_point: String,
    /// The Kubernetes identity this process writes as: the `user.username`
    /// Kubernetes audit records for the calls this request made. Configured
    /// (`kubernetes.principal`, which the chart renders from the
    /// ServiceAccount it runs the pod as), never taken from a request.
    pub kubernetes_principal: String,
    /// The Kubernetes object's name, when one was created or read.
    pub object_name: String,
    /// Its UID.
    pub object_uid: String,
    /// Its resourceVersion.
    pub object_resource_version: String,
    /// The HTTP status.
    pub http_status: u16,
    /// The latency in milliseconds.
    pub latency_ms: u64,
    /// The stable failure code, sanitized. Empty on success.
    pub failure_code: String,
    /// The immediate peer's address, when it is known.
    pub peer: String,
    /// The forwarded client address, present ONLY when the immediate peer is
    /// inside a configured trusted-proxy CIDR. Transport logging only: no
    /// decision anywhere reads it.
    pub forwarded_for: String,
    /// Identity-shaped headers the request carried, BY NAME. Their values are
    /// never read and never recorded; the names are here so an operator can
    /// see a client trying.
    pub ignored_identity_headers: Vec<String>,
}

#[derive(Default)]
struct Fields {
    record: AuditRecord,
    extra: BTreeMap<String, String>,
}

/// The per-request record under construction.
#[derive(Default)]
pub struct AuditContext(Mutex<Fields>);

impl std::fmt::Debug for AuditContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AuditContext(..)")
    }
}

impl AuditContext {
    /// A context for one request.
    #[must_use]
    pub fn new(audit_id: &str, method: &str, path: &str) -> Self {
        let mut record = AuditRecord {
            audit_id: audit_id.to_string(),
            method: method.to_string(),
            path: crate::validate::bounded(path, 256),
            decision: Decision::Deny.as_str().to_string(),
            ..AuditRecord::default()
        };
        // A request that reaches no decision point is a DENY, not a blank: the
        // record's default must be the conservative one.
        record.action = "unknown".to_string();
        Self(Mutex::new(Fields {
            record,
            extra: BTreeMap::new(),
        }))
    }

    fn with<R>(&self, f: impl FnOnce(&mut Fields) -> R) -> R {
        let mut fields = self.0.lock().expect("the audit lock is never poisoned");
        f(&mut fields)
    }

    /// Record the authenticated actor.
    pub fn set_actor(&self, mode: &str, actor_id: &str, display: &str, session_id: Option<&str>) {
        self.with(|f| {
            f.record.authentication_mode = mode.to_string();
            f.record.actor_id = crate::validate::bounded(actor_id, 320);
            f.record.display_claim = redact(&crate::validate::bounded(display, 128));
            if let Some(session_id) = session_id {
                f.record.session_id_hash = crate::auth::keys::session_id_hash(session_id);
            }
        });
    }

    /// Record which namespace and action the route asked about, and which
    /// roles and binding revision decided it.
    pub fn set_decision(
        &self,
        namespace: &str,
        action: &str,
        roles: &[String],
        binding_revision: &str,
        decision: Decision,
    ) {
        self.with(|f| {
            f.record.namespace = crate::validate::bounded(namespace, 253);
            f.record.action = action.to_string();
            f.record.roles = roles.to_vec();
            f.record.binding_revision = crate::validate::bounded(binding_revision, 128);
            f.record.decision = decision.as_str().to_string();
        });
    }

    /// Record the action alone, for routes with no namespace.
    pub fn set_action(&self, action: &str, decision: Decision) {
        self.with(|f| {
            f.record.action = action.to_string();
            f.record.decision = decision.as_str().to_string();
        });
    }

    /// Record the object a create or a read touched.
    pub fn set_object(&self, kind: &str, name: &str, uid: &str, resource_version: &str) {
        self.with(|f| {
            f.record.resource = format!("{kind}/{}", crate::validate::bounded(name, 253));
            f.record.object_name = crate::validate::bounded(name, 253);
            f.record.object_uid = crate::validate::bounded(uid, 64);
            f.record.object_resource_version = crate::validate::bounded(resource_version, 64);
        });
    }

    /// Record the two hashes of a durable create. The raw idempotency key and
    /// the request body never appear.
    pub fn set_create_hashes(&self, scope_hash: &str, request_hash: &str) {
        self.with(|f| {
            f.record.idempotency_key_hash = scope_hash.to_string();
            f.record.request_hash = request_hash.to_string();
        });
    }

    /// Record the approval policy a decision consulted (PLAT-19.2).
    pub fn set_policy_digest(&self, policy: &str) {
        self.with(|f| f.record.policy_digest = crate::validate::bounded(policy, 160));
    }

    /// Record a plan hash. The plan bytes are never recorded.
    pub fn set_plan_hash(&self, plan_hash: &str) {
        self.with(|f| f.record.plan_hash = crate::validate::bounded(plan_hash, 80));
    }

    /// Record the recovery point a request selected. The caller builds the
    /// value with [`recovery_point_of`], which never carries a credential.
    pub fn set_recovery_point(&self, recovery_point: &str) {
        self.with(|f| {
            f.record.recovery_point = redact(&crate::validate::bounded(recovery_point, 512));
        });
    }

    /// Record the Kubernetes identity this process writes as.
    pub fn set_kubernetes_principal(&self, principal: &str) {
        self.with(|f| {
            f.record.kubernetes_principal = crate::validate::bounded(principal, 253);
        });
    }

    /// The attribution every durable object this request creates carries.
    ///
    /// ONE PLACE, CALLED BY THE ADAPTER. `crate::kube::KubeAdapter::create` and
    /// `create_credential` merge this into the object's annotations on the way
    /// out, so a route another stage adds attributes its objects without
    /// knowing this function exists. The values are the record's own: the
    /// actor the authenticator produced, the action and binding revision the
    /// authorizer decided under, the request ID, the Kubernetes principal the
    /// write is made as, and the selected recovery point when there is one.
    ///
    /// # Errors
    ///
    /// When the request reached no authenticated actor or no decided action.
    /// The adapter then refuses the write: a durable object with no author is
    /// exactly what attribution exists to prevent, so it is not created.
    pub fn object_annotations(&self) -> Result<BTreeMap<String, String>, &'static str> {
        self.with(|f| {
            let r = &f.record;
            if r.actor_id.is_empty() || r.authentication_mode.is_empty() {
                return Err("no authenticated actor");
            }
            if r.action.is_empty() || r.action == "unknown" {
                return Err("no decided action");
            }
            let mut out = BTreeMap::from([
                (
                    crate::idempotency::ANNOTATION_ACTOR.to_string(),
                    r.actor_id.clone(),
                ),
                (
                    crate::idempotency::ANNOTATION_REQUEST_ID.to_string(),
                    r.audit_id.clone(),
                ),
                (
                    ANNOTATION_AUTHENTICATION_MODE.to_string(),
                    r.authentication_mode.clone(),
                ),
                (ANNOTATION_ACTION.to_string(), r.action.clone()),
                (
                    ANNOTATION_KUBERNETES_PRINCIPAL.to_string(),
                    if r.kubernetes_principal.is_empty() {
                        UNDECLARED_PRINCIPAL.to_string()
                    } else {
                        r.kubernetes_principal.clone()
                    },
                ),
            ]);
            if !r.binding_revision.is_empty() {
                out.insert(
                    ANNOTATION_BINDING_REVISION.to_string(),
                    r.binding_revision.clone(),
                );
            }
            if !r.recovery_point.is_empty() {
                out.insert(
                    ANNOTATION_RECOVERY_POINT.to_string(),
                    r.recovery_point.clone(),
                );
            }
            Ok(out)
        })
    }

    /// Record the transport facts.
    pub fn set_transport(
        &self,
        peer: &str,
        forwarded_for: Option<&str>,
        ignored_headers: &[String],
    ) {
        self.with(|f| {
            f.record.peer = crate::validate::bounded(peer, 64);
            f.record.forwarded_for = forwarded_for
                .map(|v| crate::validate::bounded(v, 128))
                .unwrap_or_default();
            f.record.ignored_identity_headers = ignored_headers.to_vec();
        });
    }

    /// Record the sanitized failure code, and make the decision a deny.
    pub fn set_failure(&self, code: &str) {
        self.with(|f| {
            f.record.failure_code = crate::validate::bounded(code, 64);
            f.record.decision = Decision::Deny.as_str().to_string();
        });
    }

    /// Record one extra sanitized key/value. Free text goes through
    /// [`redact`].
    pub fn note(&self, key: &str, value: &str) {
        self.with(|f| {
            f.extra.insert(
                key.to_string(),
                redact(&crate::validate::bounded(value, 200)),
            );
        });
    }

    /// The finished record.
    #[must_use]
    pub fn finish(&self, http_status: u16, latency_ms: u64) -> AuditRecord {
        self.with(|f| {
            f.record.http_status = http_status;
            f.record.latency_ms = latency_ms;
            if http_status >= 400 && f.record.failure_code.is_empty() {
                f.record.failure_code = format!("http_{http_status}");
            }
            if http_status < 400 && f.record.decision == Decision::Deny.as_str() {
                // A 2xx/3xx that reached no explicit decision point — the
                // health probes, the static assets, a login redirect — is an
                // allow, and saying "deny, 200" would be a lie in the log.
                f.record.decision = Decision::Allow.as_str().to_string();
            }
            f.record.clone()
        })
    }

    /// The extra notes, in key order.
    #[must_use]
    pub fn notes(&self) -> BTreeMap<String, String> {
        self.with(|f| f.extra.clone())
    }
}

/// How the creating actor was authenticated: `localAdmin` or `oidc`.
pub const ANNOTATION_AUTHENTICATION_MODE: &str = "api.logweir.dev/authentication-mode";
/// The product action the create was authorized as, e.g. `restore.create`.
pub const ANNOTATION_ACTION: &str = "api.logweir.dev/action";
/// The role-binding revision that authorized it (shared mode).
pub const ANNOTATION_BINDING_REVISION: &str = "api.logweir.dev/binding-revision";
/// The Kubernetes identity the object was written as.
pub const ANNOTATION_KUBERNETES_PRINCIPAL: &str = "api.logweir.dev/kubernetes-principal";
/// The recovery point a restore-shaped create selected.
pub const ANNOTATION_RECOVERY_POINT: &str = "api.logweir.dev/recovery-point";

/// What [`AuditContext::object_annotations`] records when no principal is
/// configured — a laptop run whose kubeconfig identity this process cannot
/// name. Kubernetes audit still has the real one.
pub const UNDECLARED_PRINCIPAL: &str = "undeclared";

tokio::task_local! {
    static CURRENT: std::sync::Arc<AuditContext>;
}

/// Run `future` with `context` as the current request's audit record.
///
/// `crate::http::request_context` is the one caller: every handler, and every
/// adapter call a handler makes, runs inside it.
pub async fn scope<F: std::future::Future>(
    context: std::sync::Arc<AuditContext>,
    future: F,
) -> F::Output {
    CURRENT.scope(context, future).await
}

/// The current request's audit record, when called inside [`scope`].
#[must_use]
pub fn current() -> Option<std::sync::Arc<AuditContext>> {
    CURRENT.try_with(std::sync::Arc::clone).ok()
}

/// The recovery point a validated request selected, read from its canonical
/// JSON — or `None` when the request selects none.
///
/// READ FROM THE REQUEST, NOT PASSED BY EACH ROUTE, for the reason the plan
/// hash is: a route that submits a restore-shaped request cannot forget to
/// attribute what it restores. The fields are the ones every restore-shaped
/// DTO in this crate uses (`backupSetRef`, `pointInTime`, and the source as
/// `sourceDestinationRef.name` or `sourceArchive.url`), plus a catalog
/// `recoveryPointRef` when a request carries one.
///
/// NO CREDENTIAL CAN ENTER IT. A destination is recorded by NAME; a legacy
/// archive URL has any userinfo replaced before it is recorded; nothing else
/// is copied.
#[must_use]
pub fn recovery_point_of(canonical: &serde_json::Value) -> Option<String> {
    let text = |pointer: &str| {
        canonical
            .pointer(pointer)
            .and_then(serde_json::Value::as_str)
            .filter(|v| !v.is_empty())
            .map(str::to_string)
    };
    let backup_set = text("/backupSetRef");
    let point_in_time = text("/pointInTime");
    let catalog_point = text("/recoveryPointRef/name").or_else(|| text("/recoveryPointRef"));
    if backup_set.is_none() && point_in_time.is_none() && catalog_point.is_none() {
        return None;
    }
    let source = text("/sourceDestinationRef/name")
        .map(|name| format!("destination/{name}"))
        .or_else(|| {
            // The LOCATION only: userinfo replaced, and no query or fragment,
            // which is where a pre-signed parameter would ride.
            text("/sourceArchive/url").map(|url| {
                let location = url.split(['?', '#']).next().unwrap_or_default();
                crate::validate::redact_url_userinfo(location)
            })
        });
    let mut parts = Vec::new();
    if let Some(v) = catalog_point {
        parts.push(format!("recoveryPoint={v}"));
    }
    if let Some(v) = backup_set {
        parts.push(format!("backupSet={v}"));
    }
    if let Some(v) = point_in_time {
        parts.push(format!("pointInTime={v}"));
    }
    if let Some(v) = source {
        parts.push(format!("source={v}"));
    }
    Some(crate::validate::bounded(&parts.join(" "), 512))
}

/// Dependency log targets pinned below DEBUG, whatever `RUST_LOG` says.
///
/// THIS IS A LEAK PATH, NOT TIDINESS. `kube_client` logs the upstream
/// `ErrorResponse` VERBATIM at DEBUG — body, message and all — which is exactly
/// the text `crate::kube::redact` exists to keep out of this service's logs. An
/// operator who turns on `RUST_LOG=debug` to debug a 503 would otherwise print
/// the unredacted Kubernetes message next to the redacted one. The same is true
/// in a smaller way of the transport crates, which log request lines.
///
/// A directive added after the environment's wins for its own target, so this
/// holds even against an explicit `RUST_LOG=kube_client=trace`. Raising one of
/// these is a deliberate edit here, not an environment variable.
pub const SILENCED_TARGETS: [&str; 7] = [
    "kube_client",
    "kube",
    "hyper",
    "hyper_util",
    "rustls",
    "tower",
    "h2",
];

/// The log filter this service runs with: the environment's `RUST_LOG`, or
/// `info`, with [`SILENCED_TARGETS`] pinned at `warn`.
#[must_use]
pub fn log_filter() -> tracing_subscriber::EnvFilter {
    log_filter_from(&std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string()))
}

/// [`log_filter`] over an explicit specification, so a test can prove the
/// pinning survives `debug`.
#[must_use]
pub fn log_filter_from(spec: &str) -> tracing_subscriber::EnvFilter {
    let mut filter = tracing_subscriber::EnvFilter::new(spec);
    for target in SILENCED_TARGETS {
        filter = filter.add_directive(
            format!("{target}=warn")
                .parse()
                .expect("a constant directive parses"),
        );
    }
    filter
}

/// The audit context of the current request.
///
/// The middleware inserts one before routing, so every extractor and handler
/// sees the same record. A context built outside a request — a unit test — gets
/// a fresh one rather than a panic, because a missing audit context must never
/// be the thing that fails a request.
#[must_use]
pub fn context_of(parts: &http::request::Parts) -> std::sync::Arc<AuditContext> {
    parts
        .extensions
        .get::<std::sync::Arc<AuditContext>>()
        .map_or_else(
            || std::sync::Arc::new(AuditContext::default()),
            std::sync::Arc::clone,
        )
}

/// Emit one finished record.
pub fn emit(record: &AuditRecord, notes: &BTreeMap<String, String>) {
    let json = serde_json::to_string(record)
        .unwrap_or_else(|_| r#"{"auditId":"","decision":"deny"}"#.to_string());
    let notes = if notes.is_empty() {
        String::new()
    } else {
        serde_json::to_string(notes).unwrap_or_default()
    };
    tracing::info!(target: TARGET, audit = %json, notes = %notes, "audit");
}

/// Remove credential-shaped substrings from free text destined for a log.
///
/// It is the same discipline `crate::kube::redact` applies to Kubernetes
/// messages — JWT-shaped words, `Bearer`, URL userinfo — plus the two shapes
/// this module can meet that the adapter cannot: a `key=value` pair whose key
/// names a credential, and a bare `code=`/`token=` query fragment.
#[must_use]
pub fn redact(text: &str) -> String {
    const SENSITIVE_KEYS: [&str; 12] = [
        "password",
        "passwd",
        "secret",
        "client_secret",
        "token",
        "id_token",
        "access_token",
        "refresh_token",
        "code",
        "code_verifier",
        "authorization",
        "cookie",
    ];
    let mut out = String::with_capacity(text.len());
    for word in text.split_inclusive(char::is_whitespace) {
        let trimmed = word.trim_end();
        let tail = &word[trimmed.len()..];
        let lower = trimmed.to_ascii_lowercase();
        let redacted = if let Some((key, _)) = trimmed.split_once('=') {
            let key_lower = key.trim().to_ascii_lowercase();
            let bare = key_lower
                .rsplit(['&', '?', '.', '"', '\''])
                .next()
                .unwrap_or("");
            if SENSITIVE_KEYS.contains(&bare) {
                Some(format!("{key}=[redacted]"))
            } else {
                None
            }
        } else {
            None
        };
        match redacted {
            Some(value) => {
                out.push_str(&value);
                out.push_str(tail);
            }
            None if lower.starts_with("eyj") && trimmed.len() > 24 => {
                out.push_str("[redacted-token]");
                out.push_str(tail);
            }
            None if trimmed.contains("://") && trimmed.contains('@') => {
                out.push_str(&crate::validate::redact_url_userinfo(trimmed));
                out.push_str(tail);
            }
            None => out.push_str(word),
        }
    }
    let lower = out.to_ascii_lowercase();
    if let Some(at) = lower.find("bearer ") {
        out.truncate(at);
        out.push_str("bearer [redacted]");
    }
    crate::validate::bounded(&out, 512)
}

/// The request headers that claim an identity and are ignored everywhere.
///
/// They are refused as inputs, stripped before routing, and recorded by name
/// only. `Impersonate-*` is NOT in this list: `crate::http::boundary_guard`
/// answers those 400 outright rather than ignoring them, because a client
/// sending one is asking this service to do something it will never do.
pub const IGNORED_IDENTITY_HEADERS: [&str; 10] = [
    "x-remote-user",
    "x-remote-group",
    "x-remote-groups",
    "x-remote-extra",
    "x-forwarded-user",
    "x-forwarded-email",
    "x-forwarded-groups",
    "x-forwarded-preferred-username",
    "x-auth-request-user",
    "x-auth-request-email",
];

/// Whether a header name claims an identity this service ignores.
#[must_use]
pub fn is_ignored_identity_header(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    IGNORED_IDENTITY_HEADERS.contains(&lower.as_str())
        || lower.starts_with("x-auth-request-")
        || lower.starts_with("x-remote-extra-")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redaction_removes_every_class_it_claims_to() {
        let jwt = "eyJhbGciOiJSUzI1NiIsImtpZCI6ImsxIn0.eyJzdWIiOiJ1In0.sig";
        let text = format!(
            "denied {jwt} password=hunter2 client_secret=abc code=xyz at \
             s3://user:pw@bucket/key Bearer abcdef"
        );
        let out = redact(&text);
        assert!(!out.contains("eyJhbGci"), "{out}");
        assert!(!out.contains("hunter2"), "{out}");
        assert!(!out.contains("abc "), "{out}");
        assert!(!out.contains("xyz"), "{out}");
        assert!(!out.contains("user:pw@"), "{out}");
        assert!(out.contains("redacted@bucket/key"), "{out}");
        assert!(out.ends_with("bearer [redacted]"), "{out}");
        assert!(redact(&"x".repeat(2000)).len() <= 520);
        // A value that merely CONTAINS a sensitive word is not mangled.
        assert_eq!(redact("namespace=team-a"), "namespace=team-a");
        assert_eq!(redact("decoded=ok"), "decoded=ok");
    }

    #[test]
    fn identity_headers_are_recognised_including_the_prefixed_families() {
        for name in [
            "X-Remote-User",
            "x-forwarded-user",
            "X-Auth-Request-User",
            "x-auth-request-anything",
            "x-remote-extra-scopes",
            "X-Forwarded-Email",
        ] {
            assert!(is_ignored_identity_header(name), "{name}");
        }
        for name in [
            "authorization",
            "cookie",
            "host",
            "x-forwarded-for",
            "origin",
        ] {
            assert!(!is_ignored_identity_header(name), "{name}");
        }
    }

    #[test]
    fn a_record_defaults_to_deny_and_a_failure_cannot_be_an_allow() {
        let ctx = AuditContext::new("audit-1", "POST", "/api/v1/namespaces/team-a/restores");
        let blank = ctx.finish(500, 3);
        assert_eq!(blank.decision, "deny");
        assert_eq!(blank.failure_code, "http_500");

        ctx.set_decision(
            "team-a",
            "restore.create",
            &["operator".into()],
            "r1",
            Decision::Allow,
        );
        assert_eq!(ctx.finish(201, 4).decision, "allow");
        ctx.set_failure("namespace_forbidden");
        let denied = ctx.finish(404, 5);
        assert_eq!(denied.decision, "deny");
        assert_eq!(denied.failure_code, "namespace_forbidden");
    }

    #[test]
    fn the_session_id_is_hashed_and_the_display_claim_is_kept_separate() {
        let ctx = AuditContext::new("audit-2", "GET", "/api/v1/session");
        ctx.set_actor(
            "oidc",
            "https://idp#u-1",
            "Ada Lovelace",
            Some("sid-secret"),
        );
        let record = ctx.finish(200, 1);
        assert_eq!(record.actor_id, "https://idp#u-1");
        assert_eq!(record.display_claim, "Ada Lovelace");
        assert!(record.session_id_hash.starts_with("sha256:"));
        let json = serde_json::to_string(&record).unwrap();
        assert!(
            !json.contains("sid-secret"),
            "the session id leaked: {json}"
        );
    }
}
