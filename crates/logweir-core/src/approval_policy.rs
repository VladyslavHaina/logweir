//! PLAT-19.2 — ordinary confirmation and governed approval: the pure contract.
//!
//! # What lives here
//!
//! Everything about the two approval modes that is decidable without a clock
//! read, a key, a cluster or a file: the installation's policy set and how a
//! namespace resolves to one policy; the policy SNAPSHOT (the exact bytes a
//! run freezes, and the digest every signed document names); the
//! authorization document v2 that both modes produce (decision D0,
//! "Authorization document v2"); and the binding checks every boundary makes
//! over it — the Approval controller, Restore admission and the runner. The
//! signatures are checked by the crates that link a verifier; this crate
//! links no crypto (`scripts/check-pure-core.sh`).
//!
//! # Where the policy lives, and why it is not a namespaced object
//!
//! D0 names an immutable `ApprovalPolicy` "or the exact PLAT-19.1 policy
//! resource", bound to a namespace by INSTALLATION configuration that also
//! carries the `allowOrdinaryConfirmation` floor, and says "selecting a
//! different policy is an explicit installation-admin rollout and audit event,
//! not a namespace operator edit". This build keeps the policies AND the
//! binding in that one installation document, which the chart renders once and
//! mounts into both the controller and `logweir-api`, so the two consume the
//! same bytes and therefore the same digest (D0: "API and controller consume
//! the same content hash"). A namespaced policy object would be writable by
//! whoever holds namespace RBAC — the "a namespace names its own authority"
//! shape D3 §7.1 forbids for trust — and a new cluster-scoped kind would be a
//! second writable authority beside `TrustPolicy`. Immutability comes from the
//! content address: a signed document names the policy's `digest`, so ANY edit
//! to a policy is a different policy to every outstanding document, which is
//! D0's "binding/policy mismatch requires re-confirmation/re-approval".
//!
//! # The two modes, and the one that is never synthesised
//!
//! * [`ApprovalMode::Governed`] — the console's confirmation signature attests
//!   the requester, AND a human approver countersigns the same bytes with a
//!   `GovernedApproval` key whose principal differs from the requester.
//! * [`ApprovalMode::Ordinary`] — the console's confirmation signature alone:
//!   an authorised operator confirming their own operation.
//!
//! A namespace with no binding resolves to [`EffectivePolicy::Legacy`]
//! (`legacy-governed-v1`), which is TODAY'S behaviour byte for byte: a v1
//! approval document signed by a `GovernedApproval` key. Nothing synthesises
//! `Ordinary`; it exists only where the installation both binds it and sets
//! `allowOrdinaryConfirmation: true`.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::ids::sha256_prefixed;
use crate::trust::KeyUsage;

/// The name the synthesised policy of an unbound namespace carries.
pub const LEGACY_GOVERNED_POLICY_NAME: &str = "legacy-governed-v1";

/// The policy snapshot's own format version.
pub const POLICY_SNAPSHOT_FORMAT_VERSION: &str = "1";

/// The snapshot's `kind`, so a snapshot can never be confused with another
/// small JSON document mounted beside it.
pub const POLICY_SNAPSHOT_KIND: &str = "ApprovalPolicySnapshot";

/// The shortest document lifetime a policy may declare.
pub const MIN_MAX_AGE_SECONDS: i64 = 60;

/// The installation maximum D0 requires ("`maxAgeSeconds` with a bounded
/// installation maximum"): seven days.
pub const MAX_MAX_AGE_SECONDS: i64 = 7 * 86_400;

/// The default lifetime of an ordinary confirmation. Short on purpose: an
/// ordinary confirmation is admitted within seconds of being signed, and a long
/// window is only a longer replay window for an unadmitted request.
pub const DEFAULT_ORDINARY_MAX_AGE_SECONDS: i64 = 900;

/// The default lifetime of a governed request: a day for a human to approve.
pub const DEFAULT_GOVERNED_MAX_AGE_SECONDS: i64 = 86_400;

/// How far a document's `issuedAt` may lie ahead of the verifier's clock. The
/// console and the controller read two different clocks; without a bound here
/// a skew of one millisecond would refuse a fresh confirmation until the
/// Approval controller's next heartbeat.
pub const MAX_ISSUED_AT_SKEW_SECONDS: i64 = 60;

/// The longest change ticket a document carries.
pub const MAX_TICKET_LEN: usize = 128;

/// The most policies one installation document may declare.
pub const MAX_POLICIES: usize = 64;

/// The most namespace bindings one installation document may declare.
pub const MAX_BINDINGS: usize = 256;

/// The DSSE `payloadType` of an authorization document v2.
///
/// ITS OWN PAYLOAD TYPE, and that is the version boundary: a v1 approval
/// signature cannot be replayed as a v2 document or the reverse, because the
/// payload type is inside what the signature covers (DSSE PAE). An old
/// controller reached by rollback refuses every v2 document as a
/// `PayloadTypeMismatch` — fail closed, which is D0's rollback rule.
pub const PAYLOAD_TYPE_RESTORE_AUTHORIZATION: &str =
    "application/vnd.logweir.restore-authorization+json;version=2.0.0";

/// The document's `formatVersion`.
pub const RESTORE_AUTHORIZATION_FORMAT_VERSION: &str = "2.0.0";

/// The document's `kind`.
pub const RESTORE_AUTHORIZATION_KIND: &str = "RestoreAuthorization";

/// The API version every Logweir subject carries.
pub const SUBJECT_API_VERSION: &str = "logweir.dev/v1alpha1";

/// The one subject kind a v2 document may authorise in this build.
pub const SUBJECT_KIND_RESTORE: &str = "Restore";

/// The two approval modes.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ApprovalMode {
    /// A console-attested requester plus an independent human approver.
    Governed,
    /// A console-attested requester confirming their own operation.
    Ordinary,
}

impl ApprovalMode {
    /// The wire spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Governed => "Governed",
            Self::Ordinary => "Ordinary",
        }
    }

    /// The key usages whose signatures a v2 document in this mode requires,
    /// in the order they are checked.
    ///
    /// The console's `ConsoleConfirmation` signature is required in BOTH modes
    /// (D0: "the console signature attests the verified requester in both
    /// modes"); `Governed` adds the approver's. A `GovernedApproval`
    /// signature is never sufficient on its own for a v2 document, because
    /// without the console's attestation nothing says who the requester was
    /// and separation of duties is not checkable.
    #[must_use]
    pub const fn required_usages(self) -> &'static [KeyUsage] {
        match self {
            Self::Governed => &[KeyUsage::ConsoleConfirmation, KeyUsage::GovernedApproval],
            Self::Ordinary => &[KeyUsage::ConsoleConfirmation],
        }
    }
}

impl std::fmt::Display for ApprovalMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One named, content-addressed approval policy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApprovalPolicy {
    /// The installation-unique name.
    pub name: String,
    /// Which of the two modes.
    pub mode: ApprovalMode,
    /// The longest `expiresAt - issuedAt` a document under this policy may
    /// declare.
    pub max_age_seconds: i64,
    /// Whether the governed approver's principal must differ from the
    /// requester's. Always `true` for `Governed` (D0: "must be true for
    /// Governed in the supported baseline"); meaningless and `false` for
    /// `Ordinary`, which has no approver.
    pub require_distinct_principal: bool,
}

/// The snapshot as serialised: fixed field order, so the bytes — and
/// therefore the digest — are a function of the policy alone.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct Snapshot {
    format_version: String,
    kind: String,
    name: String,
    mode: ApprovalMode,
    max_age_seconds: i64,
    require_distinct_principal: bool,
}

impl ApprovalPolicy {
    /// The exact snapshot bytes a run freezes into its bundle.
    #[must_use]
    pub fn snapshot_bytes(&self) -> Vec<u8> {
        let snapshot = Snapshot {
            format_version: POLICY_SNAPSHOT_FORMAT_VERSION.to_string(),
            kind: POLICY_SNAPSHOT_KIND.to_string(),
            name: self.name.clone(),
            mode: self.mode,
            max_age_seconds: self.max_age_seconds,
            require_distinct_principal: self.require_distinct_principal,
        };
        // A struct of strings, an enum, an integer and a bool cannot fail to
        // serialise; an empty vector would hash to a digest no document names.
        serde_json::to_vec(&snapshot).unwrap_or_default()
    }

    /// `sha256:<hex>` over [`Self::snapshot_bytes`] — the value a signed
    /// document names as `policy.digest`.
    #[must_use]
    pub fn digest(&self) -> String {
        sha256_prefixed(&self.snapshot_bytes())
    }

    /// Read a snapshot back — the runner's direction. The snapshot must be the
    /// CANONICAL rendering of what it parses to, so a reader cannot be handed
    /// equivalent-but-different bytes that hash to another digest.
    ///
    /// # Errors
    ///
    /// A message naming what is wrong with the bytes.
    pub fn from_snapshot_bytes(bytes: &[u8]) -> Result<Self, String> {
        let snapshot: Snapshot = serde_json::from_slice(bytes)
            .map_err(|e| format!("the approval-policy snapshot does not parse: {e}"))?;
        if snapshot.format_version != POLICY_SNAPSHOT_FORMAT_VERSION
            || snapshot.kind != POLICY_SNAPSHOT_KIND
        {
            return Err(format!(
                "the approval-policy snapshot declares formatVersion {:?} and kind {:?}; this \
                 build reads {POLICY_SNAPSHOT_FORMAT_VERSION:?} and {POLICY_SNAPSHOT_KIND:?}",
                snapshot.format_version, snapshot.kind
            ));
        }
        let policy = Self {
            name: snapshot.name,
            mode: snapshot.mode,
            max_age_seconds: snapshot.max_age_seconds,
            require_distinct_principal: snapshot.require_distinct_principal,
        };
        if policy.snapshot_bytes() != bytes {
            return Err(
                "the approval-policy snapshot is not in canonical form; the bytes a run \
                 freezes are exactly the bytes the controller rendered"
                    .to_string(),
            );
        }
        Ok(policy)
    }
}

/// What a namespace resolves to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EffectivePolicy {
    /// No binding: `legacy-governed-v1`, today's behaviour. Accepts v1
    /// approval documents signed by a `GovernedApproval` key and nothing else.
    Legacy,
    /// An explicit installation binding. Accepts authorization document v2
    /// naming exactly this policy, and nothing else.
    Bound(ApprovalPolicy),
}

impl EffectivePolicy {
    /// The policy name — [`LEGACY_GOVERNED_POLICY_NAME`] for the synthesis.
    #[must_use]
    pub fn name(&self) -> &str {
        match self {
            Self::Legacy => LEGACY_GOVERNED_POLICY_NAME,
            Self::Bound(policy) => &policy.name,
        }
    }

    /// The mode. The synthesis is governed; it is never ordinary.
    #[must_use]
    pub fn mode(&self) -> ApprovalMode {
        match self {
            Self::Legacy => ApprovalMode::Governed,
            Self::Bound(policy) => policy.mode,
        }
    }

    /// The explicit policy, when there is one.
    #[must_use]
    pub fn bound(&self) -> Option<&ApprovalPolicy> {
        match self {
            Self::Legacy => None,
            Self::Bound(policy) => Some(policy),
        }
    }

    /// Whether this is the synthesised legacy policy.
    #[must_use]
    pub fn is_legacy(&self) -> bool {
        matches!(self, Self::Legacy)
    }

    /// The digest a reader can compare — `None` for the synthesis, which has
    /// no snapshot because no v2 document may name it.
    #[must_use]
    pub fn digest(&self) -> Option<String> {
        self.bound().map(ApprovalPolicy::digest)
    }
}

/// A policy as the installation document writes it.
#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct PolicyEntry {
    name: String,
    mode: ApprovalMode,
    #[serde(default)]
    max_age_seconds: Option<i64>,
    #[serde(default)]
    require_distinct_principal: Option<bool>,
}

/// The installation document as written.
#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct PolicySetDocument {
    #[serde(default)]
    allow_ordinary_confirmation: bool,
    #[serde(default)]
    policies: Vec<PolicyEntry>,
    #[serde(default)]
    namespaces: BTreeMap<String, String>,
}

/// The installation's approval policies and namespace bindings.
///
/// `Default` is the installation that configured nothing: every namespace
/// resolves to [`EffectivePolicy::Legacy`], which is what an upgrade without
/// a policy document must mean (D0: "existing installations retain their
/// approval requirement until explicitly changed").
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ApprovalPolicySet {
    allow_ordinary_confirmation: bool,
    policies: BTreeMap<String, ApprovalPolicy>,
    bindings: BTreeMap<String, String>,
}

/// Why an installation document was refused. Every refusal names the field.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PolicyConfigError(pub String);

impl std::fmt::Display for PolicyConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "approval policy configuration: {}", self.0)
    }
}

impl std::error::Error for PolicyConfigError {}

fn is_dns_label(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 63
        && value
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        && !value.starts_with('-')
        && !value.ends_with('-')
}

impl ApprovalPolicySet {
    /// Parse and validate an installation document (YAML or JSON).
    ///
    /// # The refusals
    ///
    /// * an unknown field anywhere (`deny_unknown_fields`);
    /// * a policy name that is not a DNS label, repeated, or the reserved
    ///   [`LEGACY_GOVERNED_POLICY_NAME`];
    /// * an `Ordinary` policy while `allowOrdinaryConfirmation` is not `true` —
    ///   D0's installation floor, refused rather than silently demoted, because
    ///   an operator who bound `Ordinary` and got `Governed` would be told
    ///   nothing;
    /// * `requireDistinctPrincipal: false` on a `Governed` policy (D0's
    ///   supported baseline) and `requireDistinctPrincipal: true` on an
    ///   `Ordinary` one, which has no approver to compare;
    /// * `maxAgeSeconds` outside [`MIN_MAX_AGE_SECONDS`]..=[`MAX_MAX_AGE_SECONDS`];
    /// * a binding naming an invalid namespace or an undeclared policy;
    /// * more than [`MAX_POLICIES`] policies or [`MAX_BINDINGS`] bindings.
    ///
    /// # Errors
    ///
    /// [`PolicyConfigError`] naming the field.
    pub fn parse(text: &str) -> Result<Self, PolicyConfigError> {
        if text.trim().is_empty() {
            return Ok(Self::default());
        }
        let document: PolicySetDocument = serde_yaml::from_str(text)
            .map_err(|e| PolicyConfigError(format!("the document does not parse: {e}")))?;
        if document.policies.len() > MAX_POLICIES {
            return Err(PolicyConfigError(format!(
                "`policies` declares {} policies; at most {MAX_POLICIES}",
                document.policies.len()
            )));
        }
        if document.namespaces.len() > MAX_BINDINGS {
            return Err(PolicyConfigError(format!(
                "`namespaces` declares {} bindings; at most {MAX_BINDINGS}",
                document.namespaces.len()
            )));
        }
        let mut policies = BTreeMap::new();
        for (index, entry) in document.policies.into_iter().enumerate() {
            let field = format!("policies[{index}]");
            if !is_dns_label(&entry.name) {
                return Err(PolicyConfigError(format!(
                    "{field}.name {:?} must be a DNS label: lowercase letters, digits and '-'",
                    entry.name
                )));
            }
            if entry.name == LEGACY_GOVERNED_POLICY_NAME {
                return Err(PolicyConfigError(format!(
                    "{field}.name {LEGACY_GOVERNED_POLICY_NAME:?} is reserved for the policy an \
                     unbound namespace resolves to; leave the namespace unbound instead"
                )));
            }
            if entry.mode == ApprovalMode::Ordinary && !document.allow_ordinary_confirmation {
                return Err(PolicyConfigError(format!(
                    "{field} ({}) is Ordinary but allowOrdinaryConfirmation is not true; ordinary \
                     confirmation is an explicit installation decision (D0) and is never enabled \
                     by declaring a policy alone",
                    entry.name
                )));
            }
            let require_distinct_principal = match (entry.mode, entry.require_distinct_principal) {
                (ApprovalMode::Governed, None | Some(true)) => true,
                (ApprovalMode::Governed, Some(false)) => {
                    return Err(PolicyConfigError(format!(
                        "{field}.requireDistinctPrincipal is false on a Governed policy; the \
                         supported baseline requires the approver's principal to differ from \
                         the requester's"
                    )))
                }
                (ApprovalMode::Ordinary, None | Some(false)) => false,
                (ApprovalMode::Ordinary, Some(true)) => {
                    return Err(PolicyConfigError(format!(
                        "{field}.requireDistinctPrincipal is true on an Ordinary policy, which has \
                         no approver to compare; use a Governed policy"
                    )))
                }
            };
            let max_age_seconds = entry.max_age_seconds.unwrap_or(match entry.mode {
                ApprovalMode::Governed => DEFAULT_GOVERNED_MAX_AGE_SECONDS,
                ApprovalMode::Ordinary => DEFAULT_ORDINARY_MAX_AGE_SECONDS,
            });
            if !(MIN_MAX_AGE_SECONDS..=MAX_MAX_AGE_SECONDS).contains(&max_age_seconds) {
                return Err(PolicyConfigError(format!(
                    "{field}.maxAgeSeconds is {max_age_seconds}; it must be from \
                     {MIN_MAX_AGE_SECONDS} to {MAX_MAX_AGE_SECONDS}"
                )));
            }
            let policy = ApprovalPolicy {
                name: entry.name.clone(),
                mode: entry.mode,
                max_age_seconds,
                require_distinct_principal,
            };
            if policies.insert(entry.name.clone(), policy).is_some() {
                return Err(PolicyConfigError(format!(
                    "{field}.name {:?} is declared twice",
                    entry.name
                )));
            }
        }
        for (namespace, policy) in &document.namespaces {
            if !is_dns_label(namespace) {
                return Err(PolicyConfigError(format!(
                    "namespaces.{namespace:?} is not a namespace name"
                )));
            }
            if !policies.contains_key(policy) {
                return Err(PolicyConfigError(format!(
                    "namespaces.{namespace} names policy {policy:?}, which `policies` does not \
                     declare"
                )));
            }
        }
        Ok(Self {
            allow_ordinary_confirmation: document.allow_ordinary_confirmation,
            policies,
            bindings: document.namespaces,
        })
    }

    /// What `namespace` resolves to.
    #[must_use]
    pub fn resolve(&self, namespace: &str) -> EffectivePolicy {
        self.bindings
            .get(namespace)
            .and_then(|name| self.policies.get(name))
            .map_or(EffectivePolicy::Legacy, |policy| {
                EffectivePolicy::Bound(policy.clone())
            })
    }

    /// Whether anything is configured at all.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.policies.is_empty() && self.bindings.is_empty()
    }

    /// The installation floor.
    #[must_use]
    pub fn allows_ordinary_confirmation(&self) -> bool {
        self.allow_ordinary_confirmation
    }

    /// The bound namespaces, sorted.
    #[must_use]
    pub fn bound_namespaces(&self) -> Vec<String> {
        self.bindings.keys().cloned().collect()
    }

    /// `sha256:<hex>` over the canonical form of the whole document — the
    /// readiness value D0 asks both processes to expose, so an operator can
    /// see that the console and the controller run the same configuration.
    #[must_use]
    pub fn digest(&self) -> String {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Canonical<'a> {
            allow_ordinary_confirmation: bool,
            policies: Vec<String>,
            namespaces: &'a BTreeMap<String, String>,
        }
        let canonical = Canonical {
            allow_ordinary_confirmation: self.allow_ordinary_confirmation,
            policies: self.policies.values().map(ApprovalPolicy::digest).collect(),
            namespaces: &self.bindings,
        };
        sha256_prefixed(&serde_json::to_vec(&canonical).unwrap_or_default())
    }
}

// ---------------------------------------------------------------------------
// Authorization document v2 (D0)
// ---------------------------------------------------------------------------

/// The exact Kubernetes object a v2 document authorises.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct AuthorizedSubject {
    /// `logweir.dev/v1alpha1`.
    pub api_version: String,
    /// `Restore`.
    pub kind: String,
    /// The subject's namespace.
    pub namespace: String,
    /// The subject's name.
    pub name: String,
    /// The subject's immutable UID — what stops a document from following a
    /// delete/recreate of a same-named object.
    pub uid: String,
}

/// The authenticated requester the console attests.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Requester {
    /// The identity issuer (an OIDC issuer, or `urn:logweir:local-admin`).
    pub issuer: String,
    /// The subject within that issuer.
    pub subject: String,
}

impl Requester {
    /// The stable principal id, `<issuer>#<subject>` — the same string
    /// `logweir-api`'s `Actor::id` produces and the form a `TrustPolicy`
    /// governed-approver key's `principal.id` must use for separation of duties
    /// to compare like with like.
    #[must_use]
    pub fn principal_id(&self) -> String {
        format!("{}#{}", self.issuer, self.subject)
    }
}

/// Which policy a document was issued under.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PolicyRef {
    /// The policy name.
    pub name: String,
    /// [`ApprovalPolicy::digest`].
    pub digest: String,
}

/// **Authorization document v2** — what both modes sign.
///
/// `deny_unknown_fields`: a field this build does not know is a field whose
/// meaning it cannot enforce, and an authorization is the last place to ignore
/// one. A future field is a new `formatVersion` major.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct RestoreAuthorization {
    /// [`RESTORE_AUTHORIZATION_FORMAT_VERSION`].
    pub format_version: String,
    /// [`RESTORE_AUTHORIZATION_KIND`].
    pub kind: String,
    /// The mode it was issued under. Must equal the bound policy's.
    pub authorization_mode: ApprovalMode,
    /// The exact subject.
    pub subject: AuthorizedSubject,
    /// `sha256:<hex>` of the subject's `spec.planBytes`.
    pub plan_hash: String,
    /// Who asked, as the console authenticated them.
    pub requester: Requester,
    /// The policy it was issued under.
    pub policy: PolicyRef,
    /// When the console signed it.
    pub issued_at: DateTime<Utc>,
    /// After this instant it authorises nothing new.
    pub expires_at: DateTime<Utc>,
    /// A change ticket: REQUIRED under `Governed`, optional under `Ordinary`
    /// (D0), and in both at most [`MAX_TICKET_LEN`] printable characters —
    /// [`check_ticket`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ticket: Option<String>,
}

impl RestoreAuthorization {
    /// The exact bytes the console signs and an `Approval` carries.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }

    /// Parse signed bytes.
    ///
    /// # Errors
    ///
    /// [`AuthorizationRefusal::DocumentInvalid`] naming the parse error.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, AuthorizationRefusal> {
        serde_json::from_slice(bytes).map_err(|e| {
            AuthorizationRefusal::DocumentInvalid(format!(
                "the bytes are not an authorization document v2: {e}"
            ))
        })
    }
}

/// What a boundary knows about the subject independently of the document.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExpectedSubject {
    /// The subject's namespace.
    pub namespace: String,
    /// The subject's name.
    pub name: String,
    /// The subject's UID.
    pub uid: String,
    /// `sha256:<hex>` recomputed from the subject's own plan bytes.
    pub plan_hash: String,
}

/// Why a v2 document does not authorise this subject under this policy.
///
/// The variants are refusal CLASSES; each boundary maps them onto its own
/// closed reason set, and [`Self::reason`] is the shared spelling.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AuthorizationRefusal {
    /// Not a v2 document, the wrong kind, or a blank requester.
    DocumentInvalid(String),
    /// The document names another object.
    SubjectMismatch(String),
    /// The document names another plan.
    PlanHashMismatch {
        /// The hash inside the signed document.
        got: String,
        /// The hash recomputed from the subject.
        want: String,
    },
    /// The document names another policy, another digest, or another mode
    /// than the namespace is bound to — including a v2 document in a namespace
    /// with no binding.
    PolicyMismatch(String),
    /// The validity window is not well formed or exceeds the policy.
    WindowInvalid(String),
    /// The window has closed.
    Expired(String),
}

impl AuthorizationRefusal {
    /// The condition reason every boundary writes for this class.
    #[must_use]
    pub const fn reason(&self) -> &'static str {
        match self {
            Self::DocumentInvalid(_) => "AuthorizationDocumentInvalid",
            Self::SubjectMismatch(_) => "AuthorizationSubjectMismatch",
            Self::PlanHashMismatch { .. } => "PlanHashMismatch",
            Self::PolicyMismatch(_) => "ApprovalPolicyMismatch",
            Self::WindowInvalid(_) => "AuthorizationWindowInvalid",
            Self::Expired(_) => "AuthorizationExpired",
        }
    }
}

impl std::fmt::Display for AuthorizationRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DocumentInvalid(d)
            | Self::SubjectMismatch(d)
            | Self::PolicyMismatch(d)
            | Self::WindowInvalid(d)
            | Self::Expired(d) => f.write_str(d),
            Self::PlanHashMismatch { got, want } => write!(
                f,
                "the authorization document names plan hash {got} but the subject's \
                 spec.planBytes hash to {want}; a changed plan needs a new confirmation"
            ),
        }
    }
}

/// The refusal a v2 document earns in a namespace with no binding.
#[must_use]
pub fn unbound_namespace_refusal(namespace: &str) -> AuthorizationRefusal {
    AuthorizationRefusal::PolicyMismatch(format!(
        "namespace {namespace} is bound to no approval policy, so it resolves to \
         {LEGACY_GOVERNED_POLICY_NAME}, which accepts a v1 approval document signed by a \
         GovernedApproval key and no authorization document v2"
    ))
}

/// The refusal a v1 approval document earns under an explicit binding.
#[must_use]
pub fn v1_under_bound_policy_refusal(policy: &ApprovalPolicy) -> AuthorizationRefusal {
    AuthorizationRefusal::PolicyMismatch(format!(
        "this namespace is bound to approval policy {} ({}), which accepts authorization \
         document v2 only; a v1 approval document carries no console-attested requester, so \
         neither the policy nor separation of duties can be checked against it",
        policy.name, policy.mode
    ))
}

/// Everything about a v2 document that does NOT depend on the clock: format,
/// subject, plan and policy. The runner, which admits a run the controller
/// already admitted, calls this and [`check_window_shape`]; the controller
/// calls [`check_restore_authorization`], which adds the clock.
///
/// # Errors
///
/// The first [`AuthorizationRefusal`] in the order: document, subject, plan,
/// policy, requester.
pub fn check_binding(
    doc: &RestoreAuthorization,
    expected: &ExpectedSubject,
    policy: &ApprovalPolicy,
) -> Result<(), AuthorizationRefusal> {
    if doc.format_version.split('.').next() != Some("2") || doc.kind != RESTORE_AUTHORIZATION_KIND {
        return Err(AuthorizationRefusal::DocumentInvalid(format!(
            "the document declares formatVersion {:?} and kind {:?}; this build reads major 2 and \
             kind {RESTORE_AUTHORIZATION_KIND:?}",
            doc.format_version, doc.kind
        )));
    }
    let subject = &doc.subject;
    let mismatch = if subject.api_version != SUBJECT_API_VERSION {
        Some(format!("apiVersion {}", subject.api_version))
    } else if subject.kind != SUBJECT_KIND_RESTORE {
        Some(format!("kind {}", subject.kind))
    } else if subject.namespace != expected.namespace {
        Some(format!("namespace {}", subject.namespace))
    } else if subject.name != expected.name {
        Some(format!("name {}", subject.name))
    } else if subject.uid != expected.uid {
        Some(format!("uid {}", subject.uid))
    } else {
        None
    };
    if let Some(what) = mismatch {
        return Err(AuthorizationRefusal::SubjectMismatch(format!(
            "the authorization document names {what}, but the subject is Restore {}/{} UID {}; a \
             document authorises exactly one object",
            expected.namespace, expected.name, expected.uid
        )));
    }
    if doc.plan_hash != expected.plan_hash {
        return Err(AuthorizationRefusal::PlanHashMismatch {
            got: doc.plan_hash.clone(),
            want: expected.plan_hash.clone(),
        });
    }
    let digest = policy.digest();
    if doc.policy.name != policy.name
        || doc.policy.digest != digest
        || doc.authorization_mode != policy.mode
    {
        return Err(AuthorizationRefusal::PolicyMismatch(format!(
            "the authorization document was issued under policy {} ({}, {}), but this namespace \
             is bound to policy {} ({}, {}); a policy change requires a new confirmation",
            doc.policy.name,
            doc.authorization_mode,
            doc.policy.digest,
            policy.name,
            policy.mode,
            digest
        )));
    }
    if doc.requester.issuer.trim().is_empty() || doc.requester.subject.trim().is_empty() {
        return Err(AuthorizationRefusal::DocumentInvalid(
            "the authorization document names no requester issuer and subject; the console \
             attests who asked, and a document that names nobody attests nothing"
                .to_string(),
        ));
    }
    check_ticket(doc.authorization_mode, doc.ticket.as_deref())
        .map_err(AuthorizationRefusal::DocumentInvalid)
}

/// The change ticket's rule (D0: "ticket (required in Governed, optional in
/// Ordinary)"): under `Governed` a non-blank ticket of at most
/// [`MAX_TICKET_LEN`] characters; under `Ordinary` absent or the same shape.
///
/// # Errors
///
/// A sentence naming what is wrong.
pub fn check_ticket(mode: ApprovalMode, ticket: Option<&str>) -> Result<(), String> {
    match ticket {
        None if mode == ApprovalMode::Governed => Err(
            "a Governed authorization carries a change ticket (D0: required in Governed); \
             this document names none"
                .to_string(),
        ),
        None => Ok(()),
        Some(t) if t.trim().is_empty() || t.trim() != t => Err(format!(
            "the change ticket {t:?} is blank or carries surrounding whitespace"
        )),
        Some(t) if t.chars().count() > MAX_TICKET_LEN || t.chars().any(char::is_control) => Err(
            format!("the change ticket is at most {MAX_TICKET_LEN} printable characters"),
        ),
        Some(_) => Ok(()),
    }
}

/// The window's SHAPE, with no clock: positive and no longer than the policy
/// allows.
///
/// # Errors
///
/// [`AuthorizationRefusal::WindowInvalid`].
pub fn check_window_shape(
    doc: &RestoreAuthorization,
    policy: &ApprovalPolicy,
) -> Result<(), AuthorizationRefusal> {
    let lifetime = (doc.expires_at - doc.issued_at).num_seconds();
    if doc.expires_at <= doc.issued_at || lifetime > policy.max_age_seconds {
        return Err(AuthorizationRefusal::WindowInvalid(format!(
            "the authorization window {}..{} is {lifetime}s; policy {} allows a positive window of \
             at most {}s",
            doc.issued_at.to_rfc3339(),
            doc.expires_at.to_rfc3339(),
            policy.name,
            policy.max_age_seconds
        )));
    }
    Ok(())
}

/// The whole non-cryptographic verdict at `now`: [`check_binding`], then
/// [`check_window_shape`], then the clock.
///
/// # Errors
///
/// The first [`AuthorizationRefusal`].
pub fn check_restore_authorization(
    doc: &RestoreAuthorization,
    expected: &ExpectedSubject,
    policy: &ApprovalPolicy,
    now: DateTime<Utc>,
) -> Result<(), AuthorizationRefusal> {
    check_binding(doc, expected, policy)?;
    check_window_shape(doc, policy)?;
    if doc.issued_at > now + chrono::Duration::seconds(MAX_ISSUED_AT_SKEW_SECONDS) {
        return Err(AuthorizationRefusal::WindowInvalid(format!(
            "the authorization document was issued at {}, more than {MAX_ISSUED_AT_SKEW_SECONDS}s \
             after this verifier's clock ({})",
            doc.issued_at.to_rfc3339(),
            now.to_rfc3339()
        )));
    }
    if doc.expires_at <= now {
        return Err(AuthorizationRefusal::Expired(format!(
            "the authorization expired at {} (it is now {}); an expired confirmation or approval \
             authorises nothing new — create a new Restore",
            doc.expires_at.to_rfc3339(),
            now.to_rfc3339()
        )));
    }
    Ok(())
}

/// Whether a key's `principal.id` is in the `<issuer>#<subject>` form the
/// console attests a requester in — the only form separation of duties can
/// compare (review M4). Anything else — an email, a display name, an
/// `install:` digest, surrounding whitespace — is a principal that could be
/// the requester under another spelling, so it cannot establish separation.
#[must_use]
pub fn is_issuer_subject_principal(principal_id: &str) -> bool {
    principal_id.trim() == principal_id
        && principal_id
            .split_once('#')
            .is_some_and(|(issuer, subject)| !issuer.is_empty() && !subject.is_empty())
}

/// Separation of duties (D0): the governed approver's principal must not be
/// the requester's. Compares the stable `principal_id` strings exactly —
/// never display names, and never key ids — and FAILS CLOSED on an approver
/// principal that is not `<issuer>#<subject>` ([`is_issuer_subject_principal`]):
/// `alice@example.com` differs from `https://idp#alice` as a string and may
/// still be Alice.
#[must_use]
pub fn separation_holds(requester: &Requester, approver_principal_id: &str) -> bool {
    is_issuer_subject_principal(approver_principal_id)
        && approver_principal_id != requester.principal_id()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(text: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(text)
            .map(|t| t.with_timezone(&Utc))
            .unwrap_or_default()
    }

    const DOC: &str = "allowOrdinaryConfirmation: true
policies:
  - name: team-ordinary
    mode: Ordinary
  - name: prod-governed
    mode: Governed
    maxAgeSeconds: 3600
namespaces:
  team-a: team-ordinary
  prod: prod-governed
";

    fn set() -> ApprovalPolicySet {
        ApprovalPolicySet::parse(DOC).unwrap_or_default()
    }

    fn policy(mode: ApprovalMode) -> ApprovalPolicy {
        let name = match mode {
            ApprovalMode::Ordinary => "team-ordinary",
            ApprovalMode::Governed => "prod-governed",
        };
        set()
            .resolve(match mode {
                ApprovalMode::Ordinary => "team-a",
                ApprovalMode::Governed => "prod",
            })
            .bound()
            .cloned()
            .unwrap_or(ApprovalPolicy {
                name: name.into(),
                mode,
                max_age_seconds: 0,
                require_distinct_principal: false,
            })
    }

    fn expected() -> ExpectedSubject {
        ExpectedSubject {
            namespace: "team-a".into(),
            name: "rst-1".into(),
            uid: "uid-1".into(),
            plan_hash: format!("sha256:{}", "a".repeat(64)),
        }
    }

    fn doc(policy: &ApprovalPolicy) -> RestoreAuthorization {
        RestoreAuthorization {
            format_version: RESTORE_AUTHORIZATION_FORMAT_VERSION.into(),
            kind: RESTORE_AUTHORIZATION_KIND.into(),
            authorization_mode: policy.mode,
            subject: AuthorizedSubject {
                api_version: SUBJECT_API_VERSION.into(),
                kind: SUBJECT_KIND_RESTORE.into(),
                namespace: "team-a".into(),
                name: "rst-1".into(),
                uid: "uid-1".into(),
            },
            plan_hash: format!("sha256:{}", "a".repeat(64)),
            requester: Requester {
                issuer: "https://idp.example".into(),
                subject: "alice".into(),
            },
            policy: PolicyRef {
                name: policy.name.clone(),
                digest: policy.digest(),
            },
            issued_at: at("2026-09-22T10:00:00Z"),
            expires_at: at("2026-09-22T10:10:00Z"),
            ticket: (policy.mode == ApprovalMode::Governed).then(|| "CHG-1".to_string()),
        }
    }

    #[test]
    fn an_empty_document_is_the_legacy_installation() {
        let empty = ApprovalPolicySet::parse("").unwrap_or_else(|e| panic!("{e}"));
        assert!(empty.is_empty());
        assert_eq!(empty.resolve("anything"), EffectivePolicy::Legacy);
        assert_eq!(empty, ApprovalPolicySet::default());
    }

    #[test]
    fn a_namespace_resolves_to_its_binding_and_an_unbound_one_to_legacy() {
        let s = set();
        assert_eq!(s.resolve("team-a").mode(), ApprovalMode::Ordinary);
        assert_eq!(s.resolve("prod").mode(), ApprovalMode::Governed);
        assert_eq!(s.resolve("elsewhere"), EffectivePolicy::Legacy);
        assert_eq!(s.resolve("elsewhere").mode(), ApprovalMode::Governed);
        assert_eq!(s.resolve("elsewhere").name(), LEGACY_GOVERNED_POLICY_NAME);
        assert_eq!(s.resolve("elsewhere").digest(), None);
    }

    #[test]
    fn defaults_are_per_mode_and_governed_requires_distinct_principals() {
        let s = set();
        let ordinary = s
            .resolve("team-a")
            .bound()
            .cloned()
            .unwrap_or_else(|| panic!());
        assert_eq!(ordinary.max_age_seconds, DEFAULT_ORDINARY_MAX_AGE_SECONDS);
        assert!(!ordinary.require_distinct_principal);
        let governed = s
            .resolve("prod")
            .bound()
            .cloned()
            .unwrap_or_else(|| panic!());
        assert_eq!(governed.max_age_seconds, 3600);
        assert!(governed.require_distinct_principal);
    }

    #[test]
    fn ordinary_without_the_installation_floor_is_refused_not_demoted() {
        let err = ApprovalPolicySet::parse(&DOC.replace(
            "allowOrdinaryConfirmation: true",
            "allowOrdinaryConfirmation: false",
        ))
        .err()
        .unwrap_or_else(|| panic!("an Ordinary policy without the floor must be refused"));
        assert!(err.0.contains("allowOrdinaryConfirmation"), "{err}");
        let absent =
            ApprovalPolicySet::parse(&DOC.replace("allowOrdinaryConfirmation: true\n", ""));
        assert!(absent.is_err(), "absent floor is false");
    }

    #[test]
    fn every_malformed_document_is_refused_by_field() {
        for (text, needle) in [
            ("bogus: 1\n", "unknown field"),
            (
                "policies:\n  - name: legacy-governed-v1\n    mode: Governed\n",
                "reserved",
            ),
            ("policies:\n  - name: Bad_Name\n    mode: Governed\n", "DNS label"),
            (
                "policies:\n  - name: g\n    mode: Governed\n  - name: g\n    mode: Governed\n",
                "declared twice",
            ),
            (
                "policies:\n  - name: g\n    mode: Governed\n    requireDistinctPrincipal: false\n",
                "requireDistinctPrincipal",
            ),
            (
                "allowOrdinaryConfirmation: true\npolicies:\n  - name: o\n    mode: Ordinary\n    requireDistinctPrincipal: true\n",
                "requireDistinctPrincipal",
            ),
            (
                "policies:\n  - name: g\n    mode: Governed\n    maxAgeSeconds: 59\n",
                "maxAgeSeconds",
            ),
            (
                "policies:\n  - name: g\n    mode: Governed\n    maxAgeSeconds: 604801\n",
                "maxAgeSeconds",
            ),
            ("namespaces:\n  team-a: nothing\n", "does not declare"),
            (
                "policies:\n  - name: g\n    mode: Governed\nnamespaces:\n  Team_A: g\n",
                "not a namespace",
            ),
            (
                "policies:\n  - name: g\n    mode: Governed\n    extra: 1\n",
                "unknown field",
            ),
        ] {
            let err = ApprovalPolicySet::parse(text)
                .err()
                .unwrap_or_else(|| panic!("{text:?} must be refused"));
            assert!(err.0.contains(needle), "{text:?}: {err}");
        }
    }

    #[test]
    fn the_snapshot_is_canonical_and_round_trips() {
        let p = policy(ApprovalMode::Governed);
        let bytes = p.snapshot_bytes();
        assert_eq!(
            String::from_utf8(bytes.clone()).unwrap_or_default(),
            "{\"formatVersion\":\"1\",\"kind\":\"ApprovalPolicySnapshot\",\"name\":\"prod-governed\",\"mode\":\"Governed\",\"maxAgeSeconds\":3600,\"requireDistinctPrincipal\":true}"
        );
        assert_eq!(ApprovalPolicy::from_snapshot_bytes(&bytes), Ok(p.clone()));
        let spaced = String::from_utf8(bytes)
            .unwrap_or_default()
            .replace(',', ", ");
        assert!(ApprovalPolicy::from_snapshot_bytes(spaced.as_bytes()).is_err());
        assert_ne!(
            p.digest(),
            policy(ApprovalMode::Ordinary).digest(),
            "different policies have different digests"
        );
    }

    #[test]
    fn any_policy_edit_changes_the_digest() {
        let p = policy(ApprovalMode::Governed);
        let mut longer = p.clone();
        longer.max_age_seconds += 1;
        let mut renamed = p.clone();
        renamed.name.push('x');
        let mut flipped = p.clone();
        flipped.mode = ApprovalMode::Ordinary;
        for other in [longer, renamed, flipped] {
            assert_ne!(p.digest(), other.digest());
        }
    }

    #[test]
    fn a_matching_document_passes_every_check() {
        let p = policy(ApprovalMode::Ordinary);
        let d = doc(&p);
        assert_eq!(
            check_restore_authorization(&d, &expected(), &p, at("2026-09-22T10:05:00Z")),
            Ok(())
        );
        let bytes = d.to_bytes();
        assert_eq!(RestoreAuthorization::from_bytes(&bytes), Ok(d));
    }

    #[test]
    fn each_binding_field_is_checked() {
        let p = policy(ApprovalMode::Ordinary);
        let now = at("2026-09-22T10:05:00Z");
        let mut cases: Vec<(RestoreAuthorization, &str)> = Vec::new();
        let mut d = doc(&p);
        d.subject.uid = "uid-2".into();
        cases.push((d, "AuthorizationSubjectMismatch"));
        let mut d = doc(&p);
        d.subject.name = "rst-2".into();
        cases.push((d, "AuthorizationSubjectMismatch"));
        let mut d = doc(&p);
        d.subject.namespace = "team-b".into();
        cases.push((d, "AuthorizationSubjectMismatch"));
        let mut d = doc(&p);
        d.subject.kind = "Backup".into();
        cases.push((d, "AuthorizationSubjectMismatch"));
        let mut d = doc(&p);
        d.plan_hash = format!("sha256:{}", "b".repeat(64));
        cases.push((d, "PlanHashMismatch"));
        let mut d = doc(&p);
        d.policy.digest = format!("sha256:{}", "c".repeat(64));
        cases.push((d, "ApprovalPolicyMismatch"));
        let mut d = doc(&p);
        d.policy.name = "prod-governed".into();
        cases.push((d, "ApprovalPolicyMismatch"));
        let mut d = doc(&p);
        d.authorization_mode = ApprovalMode::Governed;
        cases.push((d, "ApprovalPolicyMismatch"));
        let mut d = doc(&p);
        d.requester.subject = " ".into();
        cases.push((d, "AuthorizationDocumentInvalid"));
        let mut d = doc(&p);
        d.kind = "StandingRehearsalAuthorization".into();
        cases.push((d, "AuthorizationDocumentInvalid"));
        let mut d = doc(&p);
        d.format_version = "1.0.0".into();
        cases.push((d, "AuthorizationDocumentInvalid"));
        for (d, reason) in cases {
            let got = check_restore_authorization(&d, &expected(), &p, now)
                .err()
                .unwrap_or_else(|| panic!("{d:?} must be refused"));
            assert_eq!(got.reason(), reason, "{d:?}: {got}");
        }
    }

    #[test]
    fn an_ordinary_document_is_refused_under_a_governed_binding_and_the_reverse() {
        let ordinary = policy(ApprovalMode::Ordinary);
        let governed = policy(ApprovalMode::Governed);
        let now = at("2026-09-22T10:05:00Z");
        let refused = check_restore_authorization(&doc(&ordinary), &expected(), &governed, now);
        assert_eq!(
            refused.map_err(|r| r.reason()),
            Err("ApprovalPolicyMismatch"),
            "the downgrade is refused"
        );
        let refused = check_restore_authorization(&doc(&governed), &expected(), &ordinary, now);
        assert_eq!(
            refused.map_err(|r| r.reason()),
            Err("ApprovalPolicyMismatch")
        );
    }

    #[test]
    fn the_window_is_bounded_by_the_policy_and_the_clock() {
        let p = policy(ApprovalMode::Ordinary);
        let mut d = doc(&p);
        assert_eq!(
            check_restore_authorization(&d, &expected(), &p, at("2026-09-22T10:10:00Z"))
                .map_err(|r| r.reason()),
            Err("AuthorizationExpired"),
            "expiresAt is exclusive"
        );
        assert_eq!(
            check_restore_authorization(&d, &expected(), &p, at("2026-09-22T09:59:30Z")),
            Ok(()),
            "a small skew is tolerated"
        );
        assert_eq!(
            check_restore_authorization(&d, &expected(), &p, at("2026-09-22T09:58:59Z"))
                .map_err(|r| r.reason()),
            Err("AuthorizationWindowInvalid"),
            "an issuedAt beyond the skew bound is refused"
        );
        d.expires_at = d.issued_at + chrono::Duration::seconds(p.max_age_seconds + 1);
        assert_eq!(
            check_window_shape(&d, &p).map_err(|r| r.reason()),
            Err("AuthorizationWindowInvalid")
        );
        d.expires_at = d.issued_at + chrono::Duration::seconds(p.max_age_seconds);
        assert_eq!(check_window_shape(&d, &p), Ok(()));
        d.expires_at = d.issued_at;
        assert_eq!(
            check_window_shape(&d, &p).map_err(|r| r.reason()),
            Err("AuthorizationWindowInvalid")
        );
    }

    #[test]
    fn unknown_document_fields_are_refused() {
        let p = policy(ApprovalMode::Ordinary);
        let mut value = serde_json::to_value(doc(&p)).unwrap_or_default();
        value["approvedBy"] = serde_json::json!("mallory");
        let bytes = serde_json::to_vec(&value).unwrap_or_default();
        assert_eq!(
            RestoreAuthorization::from_bytes(&bytes).map_err(|r| r.reason()),
            Err("AuthorizationDocumentInvalid")
        );
    }

    #[test]
    fn separation_compares_principal_ids_exactly() {
        let requester = Requester {
            issuer: "https://idp.example".into(),
            subject: "alice".into(),
        };
        assert!(!separation_holds(&requester, "https://idp.example#alice"));
        assert!(!separation_holds(&requester, " https://idp.example#alice "));
        assert!(!separation_holds(&requester, ""));
        assert!(separation_holds(&requester, "https://idp.example#bob"));
        // FAILS CLOSED (review M4): a principal not in `<issuer>#<subject>`
        // form cannot be compared with a requester, so it never establishes
        // separation -- `alice@example.com` may be Alice.
        for other_form in [
            "alice",
            "alice@example.com",
            "install:sha256:abc",
            "#alice",
            "https://idp.example#",
            "https://idp.example#bob ",
        ] {
            assert!(
                !separation_holds(&requester, other_form),
                "{other_form:?} establishes nothing"
            );
            assert!(!is_issuer_subject_principal(other_form));
        }
        assert!(is_issuer_subject_principal("https://idp.example#bob"));
    }

    #[test]
    fn a_governed_document_carries_a_ticket_and_an_ordinary_one_may() {
        let governed = policy(ApprovalMode::Governed);
        let mut d = doc(&governed);
        d.ticket = None;
        assert!(matches!(
            check_binding(&d, &expected(), &governed),
            Err(AuthorizationRefusal::DocumentInvalid(_))
        ));
        for bad in ["", " CHG-1", "CHG\n1"] {
            d.ticket = Some(bad.to_string());
            assert!(
                check_binding(&d, &expected(), &governed).is_err(),
                "{bad:?}"
            );
        }
        d.ticket = Some("x".repeat(MAX_TICKET_LEN + 1));
        assert!(check_binding(&d, &expected(), &governed).is_err());
        d.ticket = Some("CHG-4711".to_string());
        assert_eq!(check_binding(&d, &expected(), &governed), Ok(()));

        let ordinary = policy(ApprovalMode::Ordinary);
        let mut d = doc(&ordinary);
        d.ticket = None;
        assert_eq!(check_binding(&d, &expected(), &ordinary), Ok(()));
        d.ticket = Some("CHG-1".to_string());
        assert_eq!(check_binding(&d, &expected(), &ordinary), Ok(()));
    }

    #[test]
    fn the_required_signatures_are_per_mode() {
        assert_eq!(
            ApprovalMode::Ordinary.required_usages(),
            &[KeyUsage::ConsoleConfirmation]
        );
        assert_eq!(
            ApprovalMode::Governed.required_usages(),
            &[KeyUsage::ConsoleConfirmation, KeyUsage::GovernedApproval]
        );
    }

    #[test]
    fn the_set_digest_moves_with_any_binding_or_policy() {
        let base = set().digest();
        let rebound =
            ApprovalPolicySet::parse(&DOC.replace("prod: prod-governed", "prod: team-ordinary"))
                .unwrap_or_default();
        assert_ne!(base, rebound.digest());
        let longer =
            ApprovalPolicySet::parse(&DOC.replace("maxAgeSeconds: 3600", "maxAgeSeconds: 3601"))
                .unwrap_or_default();
        assert_ne!(base, longer.digest());
        assert_eq!(base, set().digest());
    }
}
