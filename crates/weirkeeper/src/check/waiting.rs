//! Pure classification of pod, Job and event state — D2 §4.3's waiting table.
//!
//! # Why this is a table and not a message scan at the call site
//!
//! Everything below is something the kubelet or the Job controller says about
//! a pod that has not run yet. Each row has a distinct remedy — project the
//! Secret, add the key, load the image, create the ServiceAccount, make room on
//! a node — and reporting them all as "the check timed out" is the defect the
//! whole check framework exists to remove. A pure function over the objects is
//! what lets every row have a test with a real fixture.
//!
//! # Nothing here reads a clock
//!
//! Three rows are "for longer than N seconds". `now` is an argument, as on
//! every other path in this crate, so the boundary is assertable rather than
//! observable only by waiting.

use std::time::Duration;

use chrono::{DateTime, Utc};
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::{Event, Pod};
use kube::ResourceExt as _;

use logweir_core::check_contract::{redact, CheckCode};

/// How long a pod may sit unschedulable before it is reported — D2 §4.3.
pub const UNSCHEDULABLE_GRACE: Duration = Duration::from_secs(60);
/// How long a pod may sit in `ContainerCreating` before a `FailedMount` event
/// is believed — D2 §4.3.
pub const MOUNT_GRACE: Duration = Duration::from_secs(60);
/// How long a Job may exist with no pod before a `FailedCreate` event is
/// believed — D2 §4.3.
pub const POD_CREATE_GRACE: Duration = Duration::from_secs(30);

/// The volume name whose `FailedMount` means the signing key, not just "a
/// volume" — [`crate::controllers::backup::SIGNING_VOLUME`].
pub const SIGNING_VOLUME: &str = crate::controllers::backup::SIGNING_VOLUME;

/// One classified waiting state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Waiting {
    /// The closed code.
    pub code: CheckCode,
    /// The Secret the kubelet named, when the row has one.
    pub secret: Option<String>,
    /// The key within it, when the row has one.
    pub key: Option<String>,
    /// The `ConfigMap` the kubelet named, when the row has one.
    pub config_map: Option<String>,
    /// The volume, when the row has one.
    pub volume: Option<String>,
    /// A redacted explanation. A kubelet message is not a secret, but a
    /// `PodCreateRejected` message carries an admission webhook's prose, which
    /// is arbitrary text from a third party.
    pub message: String,
}

impl Waiting {
    fn of(code: CheckCode, message: impl AsRef<str>) -> Self {
        Self {
            code,
            secret: None,
            key: None,
            config_map: None,
            volume: None,
            message: redact(message.as_ref()),
        }
    }

    /// Whether this state can only get worse by waiting, so the Job should be
    /// cancelled now rather than at its `activeDeadlineSeconds` — D2 §4.3's
    /// "early cancel".
    ///
    /// `PodUnschedulable` is deliberately NOT in this set: a node can join a
    /// cluster, a pod can be preempted, and a cluster autoscaler exists. It is
    /// reported and left to the deadline. Everything else here is a reference
    /// to an object that does not exist, or an image this node will not get,
    /// and no amount of waiting fixes either.
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        matches!(
            self.code,
            CheckCode::CredentialSecretNotFound
                | CheckCode::CredentialSecretKeyMissing
                | CheckCode::TrustBundleNotFound
                | CheckCode::RunnerImageNotPresent
                | CheckCode::RunnerImageInvalid
                | CheckCode::RunnerServiceAccountMissing
                | CheckCode::SigningKeyMissing
                | CheckCode::PodCreateRejected
        )
    }
}

/// One cluster Event, reduced to the four fields the table reads.
///
/// A SEPARATE TYPE, and the reason is RBAC rather than taste: the weirkeeper
/// `ClusterRole` grants no verb on `events` today (D2 §7.1 adds it with W11),
/// and `crates/logweir/tests/manifest_lint.rs`'s `every_call_site_has_a_grant`
/// refuses a call whose grant is missing. Taking the facts as a VALUE keeps
/// this classifier complete and testable now, and leaves the one `Api<Event>`
/// handle to the controller that will have the grant — W8 and W9, with W11's
/// rule. [`EventFact::from_event`] is the converter, and it names the
/// `k8s_openapi` type without ever building an `Api` for it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventFact {
    /// `reason` — `FailedMount`, `FailedCreate`, …
    pub reason: String,
    /// `message`, verbatim as the API server holds it.
    pub message: String,
    /// `involvedObject.kind`.
    pub involved_kind: String,
    /// `involvedObject.name`.
    pub involved_name: String,
}

impl EventFact {
    /// The four fields of a cluster Event. `None` when it carries no reason,
    /// which nothing below could match anyway.
    #[must_use]
    pub fn from_event(event: &Event) -> Option<Self> {
        Some(Self {
            reason: event.reason.clone()?,
            message: event.message.clone().unwrap_or_default(),
            involved_kind: event.involved_object.kind.clone().unwrap_or_default(),
            involved_name: event.involved_object.name.clone().unwrap_or_default(),
        })
    }
}

/// Everything [`classify`] looks at.
#[derive(Clone, Copy, Debug)]
pub struct Observed<'a> {
    /// The check Job. Always present: this is only called for a Job that
    /// exists.
    pub job: &'a Job,
    /// Its owned pod, when [`super::pod::find_owned_pod`] found one.
    pub pod: Option<&'a Pod>,
    /// The events for the Job and the pod, in any order.
    pub events: &'a [EventFact],
    /// The instant this pass runs at. An argument, never a clock read.
    pub now: DateTime<Utc>,
}

/// The waiting state of a check that has not produced a result, or `None` when
/// nothing is wrong yet.
///
/// # The order of the rows IS the classification
///
/// 1. **A disrupted pod first.** `DisruptionTarget=True` can coexist with
///    `Pending` and with a container still `waiting`, and "the node went away"
///    explains all of them.
/// 2. **The container's own `waiting.reason`**, which is the kubelet's most
///    specific statement and the only one that names a Secret or a key.
/// 3. **Unschedulable**, after its grace period.
/// 4. **A `FailedMount` event**, after its grace period and only while the
///    container is still creating — a mount that later succeeded is not a
///    finding.
/// 5. **No pod at all**, after its grace period, explained by a `FailedCreate`
///    event.
/// 6. **The Job's own deadline.**
#[must_use]
pub fn classify(observed: &Observed<'_>) -> Option<Waiting> {
    if let Some(pod) = observed.pod {
        if pod_condition(pod, "DisruptionTarget") == Some("True") {
            return Some(Waiting::of(
                CheckCode::DisruptedMidCheck,
                "the node this check's pod was running on was disrupted; nothing about the \
                 subject was observed",
            ));
        }
        if let Some(w) = from_container_waiting(pod) {
            return Some(w);
        }
        if unschedulable_for(pod, observed.now) >= Some(UNSCHEDULABLE_GRACE) {
            return Some(Waiting::of(
                CheckCode::PodUnschedulable,
                format!(
                    "the check pod has been unschedulable for more than {} seconds",
                    UNSCHEDULABLE_GRACE.as_secs()
                ),
            ));
        }
        if creating_for(pod, observed.now) >= Some(MOUNT_GRACE) {
            if let Some(w) = from_failed_mount(observed.events, &pod.name_any()) {
                return Some(w);
            }
        }
    } else if age(observed.job.creation_timestamp_utc(), observed.now) >= Some(POD_CREATE_GRACE) {
        if let Some(w) = from_failed_create(observed.events) {
            return Some(w);
        }
    }
    if job_deadline_exceeded(observed.job) {
        return Some(Waiting::of(
            CheckCode::DeadlineExceeded,
            "the check Job reached its activeDeadlineSeconds",
        ));
    }
    None
}

// ---------------------------------------------------------------------------
// Rows
// ---------------------------------------------------------------------------

/// The `waiting.reason` rows — D2 §4.3's first six.
fn from_container_waiting(pod: &Pod) -> Option<Waiting> {
    let waiting = pod
        .status
        .as_ref()?
        .container_statuses
        .as_ref()?
        .iter()
        .find(|c| c.name == crate::job::CONTAINER_NAME)?
        .state
        .as_ref()?
        .waiting
        .as_ref()?;
    let reason = waiting.reason.as_deref().unwrap_or_default();
    let message = waiting.message.as_deref().unwrap_or_default();
    match reason {
        "CreateContainerConfigError" => Some(from_config_error(message)),
        "ErrImagePull" | "ImagePullBackOff" => Some(Waiting::of(
            CheckCode::RunnerImagePullFailed,
            format!("the runner image could not be pulled: {message}"),
        )),
        "ErrImageNeverPull" => Some(Waiting::of(
            CheckCode::RunnerImageNotPresent,
            "the runner image is not present on the node and the pull policy is `Never`; load \
             the image for this node's architecture, or set a pull policy that fetches it",
        )),
        "InvalidImageName" => Some(Waiting::of(
            CheckCode::RunnerImageInvalid,
            format!("the runner image reference is not a valid name: {message}"),
        )),
        _ => None,
    }
}

/// `CreateContainerConfigError`'s three sub-cases, told apart by the kubelet's
/// own message forms.
///
/// The forms are the kubelet's, quoted in D2 §4.3:
/// `secret "X" not found`, `couldn't find key K in Secret NS/X`,
/// `configmap "X" not found`. A message that matches none of them is still a
/// `CredentialSecretNotFound`-shaped failure — the container's configuration
/// names something that is not there — but it names nothing, so the generic
/// code is [`CheckCode::PodCreateRejected`] rather than a guess at which
/// reference it was.
fn from_config_error(message: &str) -> Waiting {
    if let Some((secret, key)) = key_missing(message) {
        return Waiting {
            secret: Some(secret),
            key: Some(key),
            ..Waiting::of(CheckCode::CredentialSecretKeyMissing, message)
        };
    }
    if let Some(secret) = quoted_after(message, "secret ") {
        if message.contains("not found") {
            return Waiting {
                secret: Some(secret),
                ..Waiting::of(CheckCode::CredentialSecretNotFound, message)
            };
        }
    }
    if let Some(config_map) = quoted_after(message, "configmap ") {
        if message.contains("not found") {
            return Waiting {
                config_map: Some(config_map),
                ..Waiting::of(CheckCode::TrustBundleNotFound, message)
            };
        }
    }
    Waiting::of(
        CheckCode::PodCreateRejected,
        format!("the kubelet refused the container configuration: {message}"),
    )
}

/// `couldn't find key <key> in Secret <ns>/<name>` -> `(name, key)`.
fn key_missing(message: &str) -> Option<(String, String)> {
    let rest = message.split("find key ").nth(1)?;
    let (key, tail) = rest.split_once(" in Secret ")?;
    let secret = tail
        .split_whitespace()
        .next()?
        .rsplit('/')
        .next()?
        .trim_end_matches(|c: char| !c.is_alphanumeric() && c != '-' && c != '.');
    if key.is_empty() || secret.is_empty() {
        return None;
    }
    Some((secret.to_string(), key.to_string()))
}

/// The quoted name following a lowercase marker: `secret "x" not found`.
fn quoted_after(message: &str, marker: &str) -> Option<String> {
    let lower = message.to_ascii_lowercase();
    let at = lower.find(marker)? + marker.len();
    let rest = &message[at..];
    let rest = rest.strip_prefix('"')?;
    let (name, _) = rest.split_once('"')?;
    (!name.is_empty()).then(|| name.to_string())
}

/// The `FailedMount` row, split on the volume name.
fn from_failed_mount(events: &[EventFact], pod_name: &str) -> Option<Waiting> {
    let e = events.iter().find(|e| {
        e.reason == "FailedMount" && (e.involved_name == pod_name || e.involved_name.is_empty())
    })?;
    let volume = quoted_after(&e.message, "volume ");
    match volume.as_deref() {
        Some(SIGNING_VOLUME) => Some(Waiting {
            volume,
            ..Waiting::of(
                CheckCode::SigningKeyMissing,
                format!("the signing key volume did not mount: {}", e.message),
            )
        }),
        _ => Some(Waiting {
            volume: volume.clone(),
            ..Waiting::of(
                CheckCode::VolumeMountFailed,
                format!(
                    "volume {} did not mount: {}",
                    volume.as_deref().unwrap_or("<unnamed>"),
                    e.message
                ),
            )
        }),
    }
}

/// The `FailedCreate` row, split on whether the ServiceAccount is the cause.
fn from_failed_create(events: &[EventFact]) -> Option<Waiting> {
    let e = events.iter().find(|e| e.reason == "FailedCreate")?;
    if e.message.contains("serviceaccount ") && e.message.contains("not found") {
        return Some(Waiting::of(
            CheckCode::RunnerServiceAccountMissing,
            format!(
                "the Job controller could not create the check pod: {}. Create the \
                 ServiceAccount the check names, or point the check at one that exists",
                e.message
            ),
        ));
    }
    Some(Waiting::of(
        CheckCode::PodCreateRejected,
        format!(
            "the Job controller could not create the check pod: {}",
            e.message
        ),
    ))
}

// ---------------------------------------------------------------------------
// Small readers over the objects
// ---------------------------------------------------------------------------

/// A pod condition's `status`, by type.
#[must_use]
pub fn pod_condition<'a>(pod: &'a Pod, type_: &str) -> Option<&'a str> {
    pod.status
        .as_ref()?
        .conditions
        .as_ref()?
        .iter()
        .find(|c| c.type_ == type_)
        .map(|c| c.status.as_str())
}

/// How long `PodScheduled=False`/`Unschedulable` has held, or `None` when it
/// does not hold at all.
#[must_use]
pub fn unschedulable_for(pod: &Pod, now: DateTime<Utc>) -> Option<Duration> {
    let c = pod
        .status
        .as_ref()?
        .conditions
        .as_ref()?
        .iter()
        .find(|c| c.type_ == "PodScheduled")?;
    if c.status != "False" || c.reason.as_deref() != Some("Unschedulable") {
        return None;
    }
    age(c.last_transition_time.as_ref().map(|t| t.0), now)
}

/// How long the `runner` container has been `ContainerCreating`, or `None`.
#[must_use]
pub fn creating_for(pod: &Pod, now: DateTime<Utc>) -> Option<Duration> {
    let creating = pod
        .status
        .as_ref()?
        .container_statuses
        .as_ref()?
        .iter()
        .find(|c| c.name == crate::job::CONTAINER_NAME)?
        .state
        .as_ref()?
        .waiting
        .as_ref()
        .is_some_and(|w| w.reason.as_deref() == Some("ContainerCreating"));
    if !creating {
        return None;
    }
    age(pod.creation_timestamp_utc(), now)
}

/// Whether the Job failed on its own `activeDeadlineSeconds`.
#[must_use]
pub fn job_deadline_exceeded(job: &Job) -> bool {
    job.status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .is_some_and(|cs| {
            cs.iter().any(|c| {
                c.type_ == "Failed"
                    && c.status == "True"
                    && c.reason.as_deref() == Some("DeadlineExceeded")
            })
        })
}

/// `now - then`, or `None` when `then` is absent or in the future.
fn age(then: Option<DateTime<Utc>>, now: DateTime<Utc>) -> Option<Duration> {
    (now - then?).to_std().ok()
}

/// `metadata.creationTimestamp` as a `chrono` instant.
trait CreationInstant {
    fn creation_timestamp_utc(&self) -> Option<DateTime<Utc>>;
}

impl<K: kube::Resource> CreationInstant for K {
    fn creation_timestamp_utc(&self) -> Option<DateTime<Utc>> {
        self.meta().creation_timestamp.as_ref().map(|t| t.0)
    }
}
