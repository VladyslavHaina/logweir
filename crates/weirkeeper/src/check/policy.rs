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
    pub max_active_per_workspace: u32,
    /// Active check Jobs across the installation.
    pub max_active_total: u32,
    /// Active discoveries against one connection UID.
    pub max_active_discoveries_per_connection: u32,
    /// Active evidence fetches per namespace — a SEPARATE pool, so verification
    /// cannot be starved by interactive checks.
    pub max_evidence_fetch_active_per_workspace: u32,
}

impl Default for ChecksPolicy {
    fn default() -> Self {
        Self {
            max_active_per_workspace: 4,
            max_active_total: 20,
            max_active_discoveries_per_connection: 1,
            max_evidence_fetch_active_per_workspace: 4,
        }
    }
}

/// Manual-run concurrency limits: the `runs` block.
///
/// A `BackupSchedule` bounds its own runs, and D1 §8.3 keeps manual runs
/// outside that ("Back up now" must work while the nightly run is going), so
/// without this nothing bounds them and a burst of manual requests becomes as
/// many simultaneous runner pods. It caps the runner Jobs a namespace's
/// operators can have active at once, queueing the rest in creation order
/// (`crate::run_pool`). Scheduled, catch-up and retry `Backup`s are neither
/// counted nor queued: their own policy bounds them, and a slot that waited
/// behind a browser click would miss its window.
///
/// A document written before this block parses with [`RunsPolicy::default`],
/// so an upgrade that keeps an older `ConfigMap` still bounds manual runs.
/// Inside the block both fields are required: a half-written block is refused,
/// not guessed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct RunsPolicy {
    /// Manual `Backup` runs ("Back up now") ACTIVE at once in one namespace.
    pub max_manual_backups_active_per_workspace: u32,
    /// Manual `Restore` runs ACTIVE at once in one namespace.
    pub max_manual_restores_active_per_workspace: u32,
}

impl Default for RunsPolicy {
    fn default() -> Self {
        Self {
            // Four, the `checks` number: one namespace rarely needs more ad-hoc
            // captures at once, and four runner pods is what the smallest
            // supported node carries beside the controller and console.
            max_manual_backups_active_per_workspace: 4,
            // Two: a restore writes into a Kafka cluster, and the rest wait
            // with their approval intact.
            max_manual_restores_active_per_workspace: 2,
        }
    }
}

/// The value the withdrawn `discovery.defaultMaxTopics` carries on the wire;
/// see [`DiscoveryPolicy::withdrawn_default_max_topics`].
///
/// `charts/logweir/templates/policy.yaml` renders `min(this, hardMaxTopics)`,
/// and `weirkeeper/tests/chart_policy.rs` reads both, so the two cannot drift.
pub const WITHDRAWN_DEFAULT_MAX_TOPICS: u32 = 20_000;

/// The value the withdrawn `preflight.defaultTimeoutSeconds` carries on the
/// wire; see [`PreflightPolicy::withdrawn_default_timeout_seconds`].
pub const WITHDRAWN_DEFAULT_TIMEOUT_SECONDS: u32 = 120;

fn withdrawn_default_max_topics() -> u32 {
    WITHDRAWN_DEFAULT_MAX_TOPICS
}

fn withdrawn_default_timeout_seconds() -> u32 {
    WITHDRAWN_DEFAULT_TIMEOUT_SECONDS
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
    /// Withdrawn, and read by nothing on purpose.
    ///
    /// No request ever omits `maxTopics`: the `TopicDiscovery` CRD defaults it
    /// to 20 000 at admission, the typed field has the same serde default, and
    /// the console API writes it explicitly. A request's own `maxTopics` and
    /// [`Self::hard_max_topics`] are the two bounds.
    ///
    /// Still parsed, with a default and no rule, so two documents keep working
    /// across an upgrade and a rollback: one an older chart rendered still
    /// carries the key (and `deny_unknown_fields` would fail it closed over a
    /// value that does nothing), and an older controller requires the key, so
    /// the chart keeps rendering [`WITHDRAWN_DEFAULT_MAX_TOPICS`]. Keeping it
    /// at its wire name and position also keeps [`Policy::digest`] of an
    /// unchanged document byte-identical, so retained `Preflight`s are not
    /// downgraded with `PolicyChanged`; an install that had set another value
    /// sees its digest change once.
    ///
    /// No range rule: a value nothing reads must not fail the document closed.
    /// The type still applies (a whole number up to `u32::MAX`), so `null`, a
    /// negative, a fraction or a quoted number fails it like any malformed
    /// field; the chart never renders one.
    #[serde(rename = "defaultMaxTopics", default = "withdrawn_default_max_topics")]
    pub withdrawn_default_max_topics: u32,
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
            withdrawn_default_max_topics: WITHDRAWN_DEFAULT_MAX_TOPICS,
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
    /// Withdrawn, and read by nothing on purpose: the `Preflight` CRD defaults
    /// `spec.request.timeoutSeconds` to 120 at admission and the console API
    /// writes it explicitly, so a request's own `timeoutSeconds` (30..600) is
    /// the budget. Parsed with a default and no rule for the reasons
    /// [`DiscoveryPolicy::withdrawn_default_max_topics`] gives; the chart
    /// renders [`WITHDRAWN_DEFAULT_TIMEOUT_SECONDS`].
    #[serde(
        rename = "defaultTimeoutSeconds",
        default = "withdrawn_default_timeout_seconds"
    )]
    pub withdrawn_default_timeout_seconds: u32,
    /// How long a terminal preflight is kept.
    pub retention_seconds: u32,
}

impl Default for PreflightPolicy {
    fn default() -> Self {
        Self {
            withdrawn_default_timeout_seconds: WITHDRAWN_DEFAULT_TIMEOUT_SECONDS,
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
///
/// `allow_http` and `virtual_hosted_style` are parsed and read by nothing: a
/// restore readiness check takes transport and addressing from the plan, as the
/// restore runner does (seam S5), and a reader of these copies would make the
/// check and the run disagree. The chart keeps publishing them, since dropping
/// them would change every install's policy digest;
/// `weirkeeper/tests/chart_policy.rs` fails, naming the field, on any reader
/// added later.
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
    /// Manual-run ceilings. Absent in a document written before the block
    /// existed, which is [`RunsPolicy::default`].
    #[serde(default)]
    pub runs: RunsPolicy,
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
            runs: RunsPolicy::default(),
        }
    }

    /// This policy with **no attestation and no identity allowlist** — the
    /// two collections that could turn a listing into a completeness claim or
    /// an unlisted bucket into an allowed one.
    ///
    /// A method on a value rather than a constant: today [`Policy::fail_closed`]
    /// equals `defaults()` because both collections default to empty, so taking
    /// `self` is what gives the rule something to do and test
    /// (`fail_closed_clears_the_two_collections_whatever_the_defaults_hold`).
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
            self.checks.max_active_per_workspace >= 1,
            "checks.maxActivePerWorkspace",
            "must be at least 1; a limit of 0 would queue every check forever",
        )?;
        rule(
            self.checks.max_active_total >= self.checks.max_active_per_workspace,
            "checks.maxActiveTotal",
            "must be at least checks.maxActivePerWorkspace",
        )?;
        rule(
            self.checks.max_active_discoveries_per_connection >= 1,
            "checks.maxActiveDiscoveriesPerConnection",
            "must be at least 1",
        )?;
        rule(
            self.checks.max_evidence_fetch_active_per_workspace >= 1,
            "checks.maxEvidenceFetchActivePerWorkspace",
            "must be at least 1",
        )?;
        rule(
            self.runs.max_manual_backups_active_per_workspace >= 1,
            "runs.maxManualBackupsActivePerWorkspace",
            "must be at least 1; a limit of 0 would queue every manual Backup forever",
        )?;
        rule(
            self.runs.max_manual_restores_active_per_workspace >= 1,
            "runs.maxManualRestoresActivePerWorkspace",
            "must be at least 1; a limit of 0 would queue every manual Restore forever",
        )?;
        // No rule for the withdrawn `discovery.defaultMaxTopics` or
        // `preflight.defaultTimeoutSeconds`: a document must not fail closed
        // over a value that changes nothing.
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
    /// `record`, not `put`: `scripts/check-no-archive-write.sh` refuses the
    /// token `.put(` anywhere under `crates/weirkeeper/src`, deliberately
    /// unanchored because an object-store write through any receiver is what
    /// Global Constraint 6 forbids. A false positive there is answered by
    /// renaming the setter, not by widening the gate.
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
    // A refused policy says so in the log, once per cache miss. A refused
    // document becomes `Policy::fail_closed()`, discarding every attestation
    // and evidence location the administrator wrote and reverting the limits
    // to the defaults, and otherwise the only signal is an advisory
    // `configuration.policy` row on a `Preflight` nobody may run. Once per
    // cache miss (30 s), not per reconcile. The reason is already redacted:
    // a parse error can quote the document's content.
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
