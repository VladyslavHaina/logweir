//! The object-store half of a check: how a [`DestinationPlan`] becomes a
//! handle, and what the four probes a check may run against one are.
//!
//! # Nothing here reads the environment for addressing or transport
//!
//! Every handle is built through D2 W2's EXPLICIT constructors
//! (`Store::read_only_with` / `Store::from_url_with`), whose bucket, region,
//! endpoint, addressing style and `allow_http` come from the plan's
//! [`DestinationLocation`] and from nowhere else. That is D-SEAMS **S5** and
//! defect SEC-ENVHTTP: a forwarded `AWS_ALLOW_HTTP=true` must not be able to
//! put an approved TLS destination on plaintext. The only thing the
//! environment ever supplies is the CREDENTIAL, and only through the named
//! variables [`CredentialMode`] selects.
//!
//! # The one key a check may write
//!
//! D2 §4.2: "Never writes, except the optional create-only marker
//! `logweir/readiness/<destinationUid>.json` through `Store::put_create_only`."
//! [`marker_key`] is the only key builder here that a writable handle is ever
//! handed, [`open_evidence_write`] is the only constructor that returns a
//! writable handle, and both name the destination UID so a marker can never
//! land under a run's evidence keys.
//!
//! **`AlreadyExists` counts as write-authorised** (D2 §4.2, `[VERIFY U7]`): S3
//! and MinIO authorise a `PUT` before they evaluate the `If-None-Match`
//! precondition, so a 412 is proof the principal could have written. It is
//! reported as [`CheckCode::MarkerAlreadyPresent`] rather than as
//! `MarkerWritten`, because "I wrote it" and "it was already there" are
//! different facts even though they answer the same question.
//!
//! # The seam
//!
//! [`ObjectAccess`] is four thin methods. It exists so every rule above it is
//! driven by a fake with no socket in the default test suite (Global
//! Constraint 22), and so the denial paths — `AccessDenied`,
//! `InvalidCredentials`, `RegionMismatch` — are reachable at all: a
//! filesystem-backed `Store` can produce `NotFound` and nothing else.

use std::time::Duration;

use logweir_core::check_contract::{CheckCode, CredentialMode, DestinationPlan};
use logweir_core::destination::{DestinationRole, EVIDENCE_PREFIX};
use logweir_core::engine::StorageUrl;
use logweir_engine_oso::storage::{
    is_workload_identity_not_injected, PutOutcome, Store, StoreError, StoreErrorClass, StoreOptions,
};

/// The create-only readiness marker's key root — the ONE key family a check
/// may write (D2 §4.2).
pub const MARKER_PREFIX: &str = "logweir/readiness/";

/// The marker document's media contract. Written so an operator who finds the
/// object can tell what wrote it; it carries no credential and no run
/// identity.
pub const MARKER_CONTRACT: &str = "logweir.dev/readiness-marker/v1";

/// How many keys one `destination.archiveListable` probe asks for.
///
/// ONE. The question is "may this principal list under this prefix", and a
/// single key answers it; a larger page would spend an adopter's request
/// budget to learn nothing more. An EMPTY page is still a pass — an archive
/// with no objects yet is a normal state, and reading emptiness as a denial is
/// the `NotFound`-versus-`Io` confusion `StoreError` exists to prevent.
pub const LIST_PROBE_KEYS: usize = 1;

/// `logweir/readiness/<destinationUid>.json`.
#[must_use]
pub fn marker_key(destination_uid: &str) -> String {
    format!("{MARKER_PREFIX}{destination_uid}.json")
}

/// The key a `destination.evidenceReadable` probe `get`s.
///
/// DELIBERATELY ABSENT, and that is the probe: a `get` of a key nobody wrote
/// separates a denial (`AccessDenied` / `InvalidCredentials`, which the
/// backend answers before it looks for the object) from a genuine absence
/// (`ObjectNotFound`, which is the READ SUCCEEDING). D2 §4.2's
/// `destinationAccess` row: "evidence get of a nonexistent key (classifies
/// denial vs not-found)".
#[must_use]
pub fn absent_probe_key(destination_uid: &str) -> String {
    format!("{MARKER_PREFIX}{destination_uid}.absent-probe")
}

/// The marker body: deterministic JSON naming the contract and the
/// destination, and nothing else.
///
/// It carries NO subject UID and no timestamp on purpose. The put is
/// create-only, so the first check to run writes it and every later one reads
/// `AlreadyExists`; a body that varied per check would be a body nobody could
/// predict, and an operator looking at the object could not tell whether it
/// was Logweir's.
#[must_use]
pub fn marker_body(destination_uid: &str) -> Vec<u8> {
    format!("{{\"contract\":\"{MARKER_CONTRACT}\",\"destinationUid\":\"{destination_uid}\"}}")
        .into_bytes()
}

/// The bounded object-store surface one check needs.
///
/// Four methods, each time-bounded by the implementation (the timeout lives on
/// the handle, in [`StoreOptions::with_request_timeout`], not on the call).
pub trait ObjectAccess {
    /// Read one object whole.
    ///
    /// # Errors
    /// [`StoreError`]; `NotFound` is a genuine absence and never a denial.
    fn get(&self, key: &str) -> Result<Vec<u8>, StoreError>;

    /// At most `max` keys under `prefix`, in ascending key order.
    ///
    /// # Errors
    /// [`StoreError`].
    fn list_bounded(&self, prefix: &str, max: usize) -> Result<Vec<String>, StoreError>;

    /// `PutMode::Create`, under `logweir/` only.
    ///
    /// # Errors
    /// [`StoreError`]; `AlreadyExists` is a write that was AUTHORISED and
    /// refused by the precondition.
    fn put_create_only(&self, key: &str, bytes: &[u8]) -> Result<PutOutcome, StoreError>;

    /// A manifest-relative key, qualified into this handle's key space.
    fn qualify(&self, relative_key: &str) -> String;
}

impl ObjectAccess for Store {
    fn get(&self, key: &str) -> Result<Vec<u8>, StoreError> {
        Store::get(self, key).map(|(bytes, _)| bytes)
    }

    fn list_bounded(&self, prefix: &str, max: usize) -> Result<Vec<String>, StoreError> {
        // `list_page` is the BOUNDED list D3 §5.2 added; `list_keys` is
        // unbounded and in-memory, and a check must not hold an adopter's
        // whole bucket to answer "may I list".
        Store::list_page(self, prefix, None, max)
            .map(|(keys, _)| keys)
            // `EngineError::Operational`'s message is `object_store::Error`'s
            // `Display`, which is exactly what `StoreErrorClass` scans, so the
            // classification is unchanged by the hop.
            .map_err(|e| StoreError::Io(e.to_string()))
    }

    fn put_create_only(&self, key: &str, bytes: &[u8]) -> Result<PutOutcome, StoreError> {
        Store::put_create_only(self, key, bytes)
    }

    fn qualify(&self, relative_key: &str) -> String {
        Store::qualify(self, relative_key)
    }
}

/// Why a handle could not be built or a probe could not run, in the CLOSED
/// vocabulary plus a message this module composed.
///
/// **The message is never the backend's.** D2 §4.2: "Raw errors are never
/// printed, only codes plus a redacted message." Nothing in this module
/// interpolates a `StoreError`'s `Display` into a message that reaches a
/// frame; the code carries the classification and the message names the
/// operation, the destination and the key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreFailure {
    pub code: CheckCode,
    pub message: String,
}

impl StoreFailure {
    #[must_use]
    pub fn new(code: CheckCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

/// Map a [`StoreError`] onto the closed check vocabulary.
///
/// D2 W2's [`StoreErrorClass`] is the classifier and this is the ONE place its
/// answer becomes a [`CheckCode`]; `the_class_names_are_check_codes` in
/// `logweir-store` asserts every class name is a code spelling, so the
/// `unwrap_or` below is unreachable and is written as the honest
/// "unclassified" rather than as a panic.
///
/// The ONE case the classifier cannot see is a missing workload identity:
/// `StoreError::Backend(WorkloadIdentityNotInjected…)` is a refusal this crate
/// raised itself, before any request, and D2 §6.3 gives it its own code so an
/// operator is sent to the ServiceAccount annotation rather than to the
/// bucket policy.
#[must_use]
pub fn classify(e: &StoreError) -> CheckCode {
    if is_workload_identity_not_injected(e) {
        return CheckCode::WorkloadIdentityNotInjected;
    }
    CheckCode::parse(StoreErrorClass::classify(e).as_str())
        .unwrap_or(CheckCode::StoreErrorUnclassified)
}

/// The `StorageUrl` one role reads or writes through.
///
/// The two archive roles address the destination's own prefix; the two
/// evidence roles address Global Constraint 6's `logweir/` root in the same
/// bucket, over the same route. This is
/// [`DestinationLocation::archive_storage_url`] and
/// [`DestinationLocation::evidence_storage_url`] and nothing else — the
/// runner never composes a URL of its own.
#[must_use]
pub fn url_for(plan: &DestinationPlan, role: DestinationRole) -> StorageUrl {
    match role {
        DestinationRole::ArchiveRead | DestinationRole::ArchiveWrite => {
            plan.location.archive_storage_url()
        }
        DestinationRole::EvidenceRead | DestinationRole::EvidenceWrite => {
            plan.location.evidence_storage_url()
        }
    }
}

/// The key prefix a listing probe for `role` searches under.
#[must_use]
pub fn prefix_for(plan: &DestinationPlan, role: DestinationRole) -> String {
    match role {
        DestinationRole::ArchiveRead | DestinationRole::ArchiveWrite => {
            plan.location.prefix.clone()
        }
        DestinationRole::EvidenceRead | DestinationRole::EvidenceWrite => {
            EVIDENCE_PREFIX.to_string()
        }
    }
}

/// The store options for this destination, under this check's budget.
///
/// * the credential comes from [`CredentialMode`] and from no other variable;
/// * the private CA, if the plan projected one, is added to the trust store
///   **in addition to** the platform roots;
/// * the request timeout is the check's own budget, because object_store's
///   default retry window is three minutes and a check with
///   `timeoutSeconds: 120` would be killed by its Job deadline before a single
///   unreachable endpoint gave up;
/// * retries are capped for the same reason.
///
/// # Errors
/// [`StoreFailure`] when the projected CA file cannot be read.
pub fn options_for(plan: &DestinationPlan, budget: Duration) -> Result<StoreOptions, StoreFailure> {
    let mut opts = match plan.credentials {
        CredentialMode::Static => StoreOptions::static_from_env(),
        CredentialMode::WorkloadIdentity => StoreOptions::workload_identity(),
        CredentialMode::Ambient => StoreOptions::ambient(),
    };
    if let Some(path) = plan.ca_file.as_deref() {
        let pem = std::fs::read(path).map_err(|e| {
            // The path is a projection the controller chose and is safe to
            // name; the io::Error's KIND is the diagnostic and carries no
            // adopter bytes.
            StoreFailure::new(
                CheckCode::CaBundleNotFound,
                format!(
                    "the destination's projected trust bundle `{path}` could not be read \
                     ({}); the check cannot verify the endpoint's certificate without it",
                    e.kind()
                ),
            )
        })?;
        opts = opts.with_root_certificate(pem);
    }
    Ok(opts
        .with_request_timeout(budget)
        .with_max_retries(RETRIES)
        .with_retry_timeout(budget))
}

/// How many times a check's store request is retried.
///
/// TWO. Enough to ride out one transient refusal, few enough that three
/// destination roles cannot spend a 120-second budget between them.
pub const RETRIES: usize = 2;

/// A READ-ONLY handle for `role`.
///
/// # Errors
/// [`StoreFailure`].
pub fn open_read(
    plan: &DestinationPlan,
    role: DestinationRole,
    budget: Duration,
) -> Result<Store, StoreFailure> {
    let opts = options_for(plan, budget)?;
    let url = url_for(plan, role);
    Store::read_only_with(&url, &opts).map_err(|e| {
        StoreFailure::new(
            classify(&e),
            format!(
                "a {} handle for destination `{}` could not be built",
                role.as_str(),
                plan.name
            ),
        )
    })
}

/// The ONE WRITABLE handle a check may hold: the evidence root, for the
/// create-only marker.
///
/// It is deliberately built over
/// [`DestinationLocation::evidence_storage_url`], whose prefix is exactly
/// `logweir/` — which is what `Store::from_url_with`'s Global Constraint 6
/// guard requires, and what makes [`marker_key`] the only key this handle can
/// accept.
///
/// # Errors
/// [`StoreFailure`].
pub fn open_evidence_write(
    plan: &DestinationPlan,
    budget: Duration,
) -> Result<Store, StoreFailure> {
    let opts = options_for(plan, budget)?;
    let url = url_for(plan, DestinationRole::EvidenceWrite);
    Store::from_url_with(&url, &opts).map_err(|e| {
        StoreFailure::new(
            classify(&e),
            format!(
                "a writable evidence handle for destination `{}` could not be built",
                plan.name
            ),
        )
    })
}

/// What a marker put proved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarkerOutcome {
    /// The object was created by this check.
    Written,
    /// The object was already there, and the backend authorised the write
    /// before it evaluated the precondition — so the grant is proved.
    AlreadyPresent,
}

impl MarkerOutcome {
    /// [`CheckCode::MarkerWritten`] or [`CheckCode::MarkerAlreadyPresent`].
    #[must_use]
    pub fn code(self) -> CheckCode {
        match self {
            Self::Written => CheckCode::MarkerWritten,
            Self::AlreadyPresent => CheckCode::MarkerAlreadyPresent,
        }
    }
}

/// The create-only marker probe.
///
/// # Errors
/// [`StoreFailure`] for anything that is not a write this principal was
/// allowed to make.
pub fn put_marker(
    access: &dyn ObjectAccess,
    destination_uid: &str,
) -> Result<MarkerOutcome, StoreFailure> {
    let key = marker_key(destination_uid);
    match access.put_create_only(&key, &marker_body(destination_uid)) {
        Ok(_) => Ok(MarkerOutcome::Written),
        // D2 §4.2 `[VERIFY U7]`: AUTHORISED. Turning this arm into a failure
        // is mutant M5's twin — it would report a healthy destination as
        // unwritable on every check after the first.
        Err(StoreError::AlreadyExists(_)) => Ok(MarkerOutcome::AlreadyPresent),
        Err(e) => Err(StoreFailure::new(
            classify(&e),
            format!("the create-only readiness marker `{key}` could not be written"),
        )),
    }
}
