//! The installation policy `ConfigMap` — D2 §4.4.
//!
//! # Who may write it, and why that is the whole point
//!
//! `weirkeeper-policy` lives in the RELEASE namespace, not in a tenant's. Only
//! a principal who can write a `ConfigMap` there — a chart or cluster
//! administrator — can change a limit, a retention, or the
//! `visibilityAttestations` list that is the only route to a completeness claim
//! of `attestedComplete` (D-SEAMS **S3**). A namespace operator cannot, which
//! is what makes the attestation an "explicit administrator-governed
//! capability" rather than a self-certification.
//!
//! # Absent is not malformed
//!
//! * **Absent** — no `LOGWEIR_POLICY_CONFIGMAP`, or the `ConfigMap` is not
//!   there — is [`PolicyLoad::Defaulted`]: every value below has a documented
//!   default, an install that renders no policy is a supported install, and the
//!   `configuration.policy` check is READY.
//! * **Malformed** — the key is missing, the JSON does not parse, or a field is
//!   out of range — is [`PolicyLoad::Unreadable`], and it **fails closed**:
//!   the defaults apply to the limits, and the attestation list and the
//!   `ControllerIdentity` allowlist are EMPTY. A policy nobody can read must
//!   never be the reason a topic listing is reported as complete.
//!   `configuration.policy` is then `notReady` with
//!   [`CheckCode::PolicyUnreadable`].
//!
//! # No clock is read here
//!
//! [`PolicyCache`] takes `now`, like everything else in this crate.

use std::collections::BTreeMap;
use std::time::Duration;

use chrono::{DateTime, Utc};
use k8s_openapi::api::core::v1::ConfigMap;
use kube::api::Api;
use serde::{Deserialize, Serialize};

use logweir_core::check_contract::{redact, Attestation, CheckCode};

/// The environment variable naming the policy `ConfigMap` as `<ns>/<name>`.
pub const POLICY_CONFIGMAP_ENV: &str = "LOGWEIR_POLICY_CONFIGMAP";
/// The environment variable carrying the release namespace, via a `fieldRef`.
pub const INSTALLATION_NAMESPACE_ENV: &str = "LOGWEIR_INSTALLATION_NAMESPACE";
/// The `ConfigMap`'s default name.
pub const DEFAULT_POLICY_NAME: &str = "weirkeeper-policy";
/// The one key inside it.
pub const POLICY_KEY: &str = "policy.json";
/// The only `version` this build understands.
pub const POLICY_VERSION: u32 = 1;
/// How long a loaded policy is reused before it is read again — D2 §4.3.
pub const CACHE_TTL: Duration = Duration::from_secs(30);

/// Check concurrency limits — D2 §4.4's `checks` block.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ChecksPolicy {
    /// Active check Jobs in one namespace.
    pub max_active_per_namespace: u32,
    /// Active check Jobs across the installation.
    pub max_active_total: u32,
    /// Active discoveries against one connection UID.
    pub max_active_discoveries_per_connection: u32,
    /// Active evidence fetches per namespace — a SEPARATE pool, so verification
    /// cannot be starved by interactive checks.
    pub max_evidence_fetch_active_per_namespace: u32,
}

impl Default for ChecksPolicy {
    fn default() -> Self {
        Self {
            max_active_per_namespace: 4,
            max_active_total: 20,
            max_active_discoveries_per_connection: 1,
            max_evidence_fetch_active_per_namespace: 4,
        }
    }
}

/// Topic-discovery policy — D2 §4.4's `discovery` block.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DiscoveryPolicy {
    /// How long a discovery result is considered fresh.
    pub fresh_seconds: u32,
    /// How long a terminal discovery is kept.
    pub retention_seconds: u32,
    /// How many terminal discoveries per connection UID are kept.
    pub keep_per_connection: u32,
    /// The `maxTopics` a request that names none gets.
    pub default_max_topics: u32,
    /// The ceiling a request's `maxTopics` is clamped to.
    pub hard_max_topics: u32,
    /// The administrator's completeness attestations (D2 §5.4).
    #[serde(default)]
    pub visibility_attestations: Vec<Attestation>,
}

impl Default for DiscoveryPolicy {
    fn default() -> Self {
        Self {
            fresh_seconds: 900,
            retention_seconds: 86_400,
            keep_per_connection: 5,
            default_max_topics: 20_000,
            hard_max_topics: 50_000,
            // EMPTY BY DEFAULT, and that is the safe direction: with no
            // attestation nothing is ever `attestedComplete`.
            visibility_attestations: Vec::new(),
        }
    }
}

/// Preflight policy — D2 §4.4's `preflight` block.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PreflightPolicy {
    /// The `timeoutSeconds` a request that names none gets.
    pub default_timeout_seconds: u32,
    /// How long a terminal preflight is kept.
    pub retention_seconds: u32,
}

impl Default for PreflightPolicy {
    fn default() -> Self {
        Self {
            default_timeout_seconds: 120,
            retention_seconds: 3600,
        }
    }
}

/// Engine policy — D2 §4.4's `engine` block.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct EnginePolicy {
    /// Whether a destination may carry a custom CA the engine cannot verify.
    /// `false` by default — the closed direction.
    #[serde(default)]
    pub allow_unverified_custom_ca: bool,
}

/// One entry of the `ControllerIdentity` allowlist — D2 §4.4's `evidence`
/// block.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct IdentityLocation {
    /// The object-store endpoint, or empty for AWS.
    #[serde(default)]
    pub endpoint: String,
    /// The region, or empty.
    #[serde(default)]
    pub region: String,
    /// The bucket.
    pub bucket: String,
}

/// Evidence policy — D2 §4.4's `evidence` block.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct EvidencePolicy {
    /// Where the controller's own identity may read evidence from. EMPTY by
    /// default: an unlisted location is refused with
    /// [`CheckCode::ControllerIdentityNotAllowlisted`].
    #[serde(default)]
    pub controller_identity_locations: Vec<IdentityLocation>,
}

/// The legacy inline-archive addressing an upgrade keeps working with — D2
/// §4.4's `legacyArchiveAddressing` block.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct LegacyArchiveAddressing {
    /// The endpoint, or empty.
    #[serde(default)]
    pub endpoint: String,
    /// The region, or empty.
    #[serde(default)]
    pub region: String,
    /// Whether plaintext HTTP is permitted for the legacy path. `false` by
    /// default — D-SEAMS **S5**: transport security is never derived.
    #[serde(default)]
    pub allow_http: bool,
    /// Whether virtual-hosted addressing is used for the legacy path.
    #[serde(default)]
    pub virtual_hosted_style: bool,
}

/// The whole installation policy.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Policy {
    /// Must equal [`POLICY_VERSION`].
    pub version: u32,
    #[serde(default)]
    pub checks: ChecksPolicy,
    #[serde(default)]
    pub discovery: DiscoveryPolicy,
    #[serde(default)]
    pub preflight: PreflightPolicy,
    #[serde(default)]
    pub engine: EnginePolicy,
    #[serde(default)]
    pub evidence: EvidencePolicy,
    #[serde(default)]
    pub legacy_archive_addressing: LegacyArchiveAddressing,
}

impl Policy {
    /// The documented defaults — what an install that renders no policy gets.
    ///
    /// NOT `Default::default()`, which `serde` would give a `version` of 0.
    /// Every consumer reads `defaults()`, so "absent" and "present and empty"
    /// produce the same object.
    #[must_use]
    pub fn defaults() -> Self {
        Self {
            version: POLICY_VERSION,
            checks: ChecksPolicy::default(),
            discovery: DiscoveryPolicy::default(),
            preflight: PreflightPolicy::default(),
            engine: EnginePolicy::default(),
            evidence: EvidencePolicy::default(),
            legacy_archive_addressing: LegacyArchiveAddressing::default(),
        }
    }

    /// This policy with **no attestation and no identity allowlist** — the
    /// two collections that could turn a listing into a completeness claim or
    /// an unlisted bucket into an allowed one.
    ///
    /// A METHOD ON A VALUE, not a constant. [`Policy::fail_closed`] is
    /// `defaults().closed()`, and today the two are observationally equal
    /// because both collections already default to empty — which is exactly
    /// why a reviewer's `fail_closed() -> defaults()` mutant was a no-op and
    /// survived. Taking `self` gives the rule something to do and therefore
    /// something to test: `fail_closed_clears_the_two_collections_whatever_the_defaults_hold`
    /// hands it a policy that carries both and asserts they are gone.
    #[must_use]
    pub fn closed(mut self) -> Self {
        self.discovery.visibility_attestations.clear();
        self.evidence.controller_identity_locations.clear();
        self
    }

    /// The defaults with **no attestation and no identity allowlist** — the
    /// fail-closed object a malformed policy produces.
    #[must_use]
    pub fn fail_closed() -> Self {
        Self::defaults().closed()
    }

    /// `sha256:<hex>` over the policy's canonical JSON — the value every check
    /// binding records as its `policyDigest` (D2 §4.4 "Provenance").
    ///
    /// Over the PARSED object rather than over the `ConfigMap` bytes, so
    /// reformatting the document does not invalidate every recorded binding,
    /// while a changed value does.
    #[must_use]
    pub fn digest(&self) -> String {
        let bytes = serde_json::to_vec(self).unwrap_or_default();
        logweir_core::ids::sha256_prefixed(&bytes)
    }

    /// Range checks the types cannot express.
    ///
    /// # Errors
    /// [`PolicyError::Field`] naming the field and the rule.
    pub fn validate(&self) -> Result<(), PolicyError> {
        if self.version != POLICY_VERSION {
            return Err(PolicyError::Version(self.version));
        }
        let rule = |ok: bool, field: &'static str, rule: &'static str| {
            if ok {
                Ok(())
            } else {
                Err(PolicyError::Field { field, rule })
            }
        };
        rule(
            self.checks.max_active_per_namespace >= 1,
            "checks.maxActivePerNamespace",
            "must be at least 1; a limit of 0 would queue every check forever",
        )?;
        rule(
            self.checks.max_active_total >= self.checks.max_active_per_namespace,
            "checks.maxActiveTotal",
            "must be at least checks.maxActivePerNamespace",
        )?;
        rule(
            self.checks.max_active_discoveries_per_connection >= 1,
            "checks.maxActiveDiscoveriesPerConnection",
            "must be at least 1",
        )?;
        rule(
            self.checks.max_evidence_fetch_active_per_namespace >= 1,
            "checks.maxEvidenceFetchActivePerNamespace",
            "must be at least 1",
        )?;
        rule(
            self.discovery.default_max_topics >= 1
                && self.discovery.default_max_topics <= self.discovery.hard_max_topics,
            "discovery.defaultMaxTopics",
            "must be at least 1 and at most discovery.hardMaxTopics",
        )?;
        rule(
            self.discovery.hard_max_topics >= 1
                && u64::from(self.discovery.hard_max_topics)
                    <= u64::from(logweir_core::check_contract::MAX_TOPICS_CEILING),
            "discovery.hardMaxTopics",
            "must be at least 1 and at most the contract's MAX_TOPICS_CEILING",
        )?;
        rule(
            self.discovery.keep_per_connection >= 1,
            "discovery.keepPerConnection",
            "must be at least 1; keeping none would delete a discovery the moment it finished",
        )?;
        rule(
            self.preflight.default_timeout_seconds >= 1
                && self.preflight.default_timeout_seconds <= 600,
            "preflight.defaultTimeoutSeconds",
            "must be within the contract's 1..=600",
        )?;
        for (i, a) in self.discovery.visibility_attestations.iter().enumerate() {
            rule(
                !a.id.trim().is_empty(),
                "discovery.visibilityAttestations[].id",
                "must be non-empty; an attestation with no id cannot be reported",
            )
            .map_err(|e| match e {
                PolicyError::Field { field, rule } => PolicyError::Attestation {
                    index: i,
                    field,
                    rule,
                },
                other => other,
            })?;
        }
        Ok(())
    }
}

/// Why a policy document was refused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PolicyError {
    /// The `ConfigMap` exists but carries no [`POLICY_KEY`].
    KeyMissing,
    /// The JSON did not parse, or carried an unknown field.
    Parse(String),
    /// `version` is not [`POLICY_VERSION`].
    Version(u32),
    /// A value is out of range.
    Field {
        /// The dotted field name.
        field: &'static str,
        /// What it must satisfy.
        rule: &'static str,
    },
    /// A value inside `visibilityAttestations[i]` is out of range.
    Attestation {
        /// Which entry.
        index: usize,
        /// The dotted field name.
        field: &'static str,
        /// What it must satisfy.
        rule: &'static str,
    },
}

impl PolicyError {
    /// The one code every refusal is reported as — D2 §6.3's
    /// `configuration.policy` row.
    #[must_use]
    pub fn code(&self) -> CheckCode {
        CheckCode::PolicyUnreadable
    }
}

impl std::fmt::Display for PolicyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::KeyMissing => write!(f, "the policy ConfigMap carries no `{POLICY_KEY}` key"),
            Self::Parse(e) => write!(f, "`{POLICY_KEY}` did not parse: {e}"),
            Self::Version(v) => write!(
                f,
                "`version` is {v}; this build understands {POLICY_VERSION}"
            ),
            Self::Field { field, rule } => write!(f, "`{field}` {rule}"),
            Self::Attestation { index, field, rule } => write!(
                f,
                "`discovery.visibilityAttestations[{index}]`: `{field}` {rule}"
            ),
        }
    }
}

impl std::error::Error for PolicyError {}

/// What one load produced.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PolicyLoad {
    /// No policy is configured, or the `ConfigMap` is absent. The documented
    /// defaults apply and `configuration.policy` is READY.
    Defaulted(Policy),
    /// A policy was read and validated.
    Loaded(Policy),
    /// A policy was read and REFUSED. [`Policy::fail_closed`] applies and
    /// `configuration.policy` is `notReady` with [`CheckCode::PolicyUnreadable`].
    Unreadable {
        /// The fail-closed object every consumer uses meanwhile.
        policy: Policy,
        /// The refusal, redacted — a policy document is administrator-authored
        /// and its parse error can quote its content.
        reason: String,
    },
}

impl PolicyLoad {
    /// The policy to use, whatever happened.
    #[must_use]
    pub fn policy(&self) -> &Policy {
        match self {
            Self::Defaulted(p) | Self::Loaded(p) | Self::Unreadable { policy: p, .. } => p,
        }
    }

    /// The `configuration.policy` code: [`CheckCode::PolicyLoaded`] or
    /// [`CheckCode::PolicyUnreadable`].
    ///
    /// **An ABSENT policy is `PolicyLoaded`.** An install that renders none is
    /// supported, and reporting `notReady` for the shipped default would make
    /// the advisory check permanently amber on a correct install.
    #[must_use]
    pub fn code(&self) -> CheckCode {
        match self {
            Self::Defaulted(_) | Self::Loaded(_) => CheckCode::PolicyLoaded,
            Self::Unreadable { .. } => CheckCode::PolicyUnreadable,
        }
    }

    /// Whether this load is usable as authority for a completeness claim.
    #[must_use]
    pub fn is_readable(&self) -> bool {
        !matches!(self, Self::Unreadable { .. })
    }
}

/// Parse and validate a policy document.
///
/// # Errors
/// [`PolicyError`].
pub fn parse(bytes: &[u8]) -> Result<Policy, PolicyError> {
    let policy: Policy =
        serde_json::from_slice(bytes).map_err(|e| PolicyError::Parse(e.to_string()))?;
    policy.validate()?;
    Ok(policy)
}

/// Classify one `ConfigMap`'s `data` — **pure**, so every branch of the absent
/// / present / malformed decision has a test with no API server.
#[must_use]
pub fn from_data(data: Option<&BTreeMap<String, String>>) -> PolicyLoad {
    let Some(raw) = data.and_then(|d| d.get(POLICY_KEY)) else {
        return unreadable(&PolicyError::KeyMissing);
    };
    match parse(raw.as_bytes()) {
        Ok(policy) => PolicyLoad::Loaded(policy),
        Err(e) => unreadable(&e),
    }
}

fn unreadable(error: &PolicyError) -> PolicyLoad {
    PolicyLoad::Unreadable {
        policy: Policy::fail_closed(),
        reason: redact(&error.to_string()),
    }
}

/// `<namespace>/<name>` from [`POLICY_CONFIGMAP_ENV`], or the release
/// namespace's [`DEFAULT_POLICY_NAME`] when only
/// [`INSTALLATION_NAMESPACE_ENV`] is set.
///
/// `None` means "no policy is configured", which is
/// [`PolicyLoad::Defaulted`] and not an error: a chart that renders no policy
/// is a supported install.
#[must_use]
pub fn configured_ref(
    policy_var: Option<&str>,
    installation_namespace: Option<&str>,
) -> Option<(String, String)> {
    if let Some(v) = policy_var.map(str::trim).filter(|v| !v.is_empty()) {
        let (ns, name) = v.split_once('/')?;
        let (ns, name) = (ns.trim(), name.trim());
        if ns.is_empty() || name.is_empty() {
            return None;
        }
        return Some((ns.to_string(), name.to_string()));
    }
    let ns = installation_namespace
        .map(str::trim)
        .filter(|v| !v.is_empty())?;
    Some((ns.to_string(), DEFAULT_POLICY_NAME.to_string()))
}

/// A policy with a TTL, so a reconcile does not `get` the `ConfigMap` on every
/// pass.
///
/// `now` is an argument, as everywhere else in this crate.
#[derive(Debug, Default)]
pub struct PolicyCache {
    entry: std::sync::Mutex<Option<(DateTime<Utc>, PolicyLoad)>>,
}

impl PolicyCache {
    /// A fresh, empty cache.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The cached load, when it is younger than [`CACHE_TTL`].
    #[must_use]
    pub fn get(&self, now: DateTime<Utc>) -> Option<PolicyLoad> {
        let guard = self.entry.lock().ok()?;
        let (at, load) = guard.as_ref()?;
        let age = (now - *at).to_std().ok()?;
        (age < CACHE_TTL).then(|| load.clone())
    }

    /// Record a load.
    ///
    /// **`record` and not `put`.** `scripts/check-no-archive-write.sh` refuses
    /// the token `.put(` anywhere under `crates/weirkeeper/src` — deliberately
    /// UNANCHORED, because an object-store write reached through any receiver
    /// is the thing Global Constraint 6 forbids and a narrower pattern is how
    /// such a gate stops catching anything. A cache setter called `put` is a
    /// false positive, and the right response to a false positive on a gate
    /// this important is to rename the setter, not to widen the gate.
    pub fn record(&self, now: DateTime<Utc>, load: PolicyLoad) {
        if let Ok(mut guard) = self.entry.lock() {
            *guard = Some((now, load));
        }
    }
}

/// Load the policy, through the cache.
///
/// # Errors
///
/// [`kube::Error`] from the `get`. An absent `ConfigMap` is **not** an error:
/// `Api::get_opt` returns `None` and that is [`PolicyLoad::Defaulted`].
pub async fn load(
    client: &kube::Client,
    reference: Option<&(String, String)>,
    cache: &PolicyCache,
    now: DateTime<Utc>,
) -> Result<PolicyLoad, kube::Error> {
    let Some((namespace, name)) = reference else {
        return Ok(PolicyLoad::Defaulted(Policy::defaults()));
    };
    if let Some(cached) = cache.get(now) {
        return Ok(cached);
    }
    let maps: Api<ConfigMap> = Api::namespaced(client.clone(), namespace);
    let load = match maps.get_opt(name).await? {
        Some(cm) => from_data(cm.data.as_ref()),
        None => PolicyLoad::Defaulted(Policy::defaults()),
    };
    // A REFUSED POLICY SAYS SO, ONCE PER CACHE MISS — D2 W11 fix round 1,
    // review finding F1.
    //
    // THE FAILURE THIS CLOSES IS THE QUIET ONE. A document this loader refuses
    // becomes `Policy::fail_closed()`, which discards EVERY
    // `visibilityAttestation` and EVERY `controllerIdentityLocation` the
    // administrator wrote: `attestedComplete` becomes unreachable, the
    // evidence allowlist becomes empty, and the ceilings and retention windows
    // revert to the compiled-in defaults. Until this line the only signal
    // anywhere was one advisory `configuration.policy notReady
    // PolicyUnreadable` row on a `Preflight` — which nobody sees unless they
    // happen to run one and read it. The controller log is where an operator
    // looks when an attestation "did not work", so the reason belongs there,
    // naming the failing rule.
    //
    // ONCE PER CACHE MISS AND NOT PER RECONCILE: this runs only past the
    // 30-second cache, so a permanently bad document costs two lines a minute
    // rather than one per pass of every check in the installation.
    //
    // THE REASON IS ALREADY REDACTED. `unreadable()` runs `redact` over the
    // `PolicyError`'s `Display`, and a policy document is administrator-authored
    // configuration that carries no credential by construction — but a parse
    // error can quote the content it choked on, so the redaction stays on the
    // path to the log as well as to the status.
    if let PolicyLoad::Unreadable { reason, .. } = &load {
        tracing::warn!(
            namespace = %namespace,
            config_map = %name,
            reason = %reason,
            "the installation policy ConfigMap was REFUSED; every completeness attestation and \
             every evidence location in it is being ignored, and the compiled-in defaults apply \
             until it is fixed"
        );
    }
    cache.record(now, load.clone());
    Ok(load)
}
