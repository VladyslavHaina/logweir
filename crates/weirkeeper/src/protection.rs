//! Protection freshness, the alert vocabulary and the deduplication ledger —
//! PLAT-14.2, decision D3 §§3.2–3.4.
//!
//! # PURE. There is no `kube::Client` in this module and there must not be one
//!
//! Every verdict here is a function of objects the caller already read and of
//! one `now`. That is not tidiness: "an enabled schedule can have no recent
//! recoverable backup" is a sentence about a five-valued health, a
//! three-valued freshness, an availability rule with four conjuncts and a
//! ledger whose whole property is *how few* messages it sends. A verdict
//! reachable only through a client can only be tested through a route table,
//! and a route table answers whatever it is asked — which is exactly how a
//! dedup ledger comes to send one page per reconcile and nobody notices until
//! an on-call rotation is muted.
//!
//! [`crate::controllers::protection_policy`] is the thin half: it reads, calls
//! [`evaluate`] and [`reconcile_alerts`], and patches `/status`.
//!
//! # THREE THINGS THIS MODULE REFUSES TO CONFLATE
//!
//! 1. **Scheduling health and protection health.** `BackupSchedule`'s `Ready`
//!    condition says the cron is firing. [`Health`] says there is something to
//!    recover from. A green schedule with no recoverable point is the defect
//!    PLAT-14.2 exists for, so the two never collapse into one value and
//!    [`Verdict::schedules`] carries the scheduling facts beside the verdict
//!    rather than inside it.
//! 2. **`Unknown` and healthy.** [`Freshness::Unknown`] is a real answer —
//!    the catalog could not be read, the source is gone, this policy's own
//!    status is stale — and it becomes [`Health::Unknown`], whose `Protected`
//!    condition is `Unknown` and not `True` and not `False`. An evaluation
//!    that could not happen is not a pass and is not a failure.
//! 3. **Capture start and the newest record.** [`AvailablePointFacts::
//!    recovery_point_at`] is the capture START (`Backup.status.capture.
//!    startedAt`, the receipt's `started_at`). `newest_record_at` —
//!    `windowCovered.toMs - 1 ms` — sits beside it under its own name. An
//!    objective measured from the newest record makes an idle topic look
//!    stale forever; one measured from `finishedAt` under-reports the gap by
//!    the length of the run.
//!
//! # No credential value reaches anything this module builds
//!
//! The event document ([`event_document`]) carries a policy name, an alert
//! key, a health, a summary and a point identity. The sink credentials reach
//! the delivery Job by `secretKeyRef` and are never read by this process
//! (`config/rbac/role.yaml` grants no verb on `secrets`), so there is nothing
//! here to redact — which is a stronger statement than "it is redacted", and
//! `a_sink_credential_value_never_reaches_status_or_a_plan` is the test that
//! keeps it true against an API server that echoes one back.

use std::collections::BTreeMap;

use serde_json::{json, Value};

use crate::crds::protection_policy::{
    AlertDelivery, AlertEntry, AlertKind, AvailablePoint, LastAttempt, MissedSummary,
    Notifications, ProtectionPolicySpec, RehearsalSummary, ScheduleSummary,
};
use crate::crds::{LocalRef, Time};

// ===========================================================================
// Bounds
// ===========================================================================

/// `status.alerts` `maxItems` on the CRD — one entry per `(policy, kind)`, and
/// there are five kinds, so this is three times the ledger a correct
/// implementation can fill.
pub const MAX_ALERTS: usize = 16;

/// `status.schedules` `maxItems`, matching `spec.protects.scheduleRefs`'s own.
pub const MAX_SCHEDULES: usize = 16;

/// `status.lastAvailablePoint.topics` `maxItems`. A point covering more sets
/// `topicsTruncated: true` rather than growing the object.
pub const MAX_TOPICS: usize = 64;

/// D3 §3.2: "list at most the newest 50 Backups selected by
/// `logweir.dev/schedule-uid`". The cap is on what is CONSIDERED, so a
/// namespace with ten thousand runs costs one bounded list per schedule.
pub const MAX_BACKUPS_SCANNED: usize = 50;

/// D3 §3.4 point 4: "retries at most 3 times".
pub const MAX_DELIVERY_ATTEMPTS: i64 = 3;

/// D3 §3.4 point 4's backoff, in seconds, indexed by the attempt that just
/// failed.
pub const DELIVERY_BACKOFF_SECONDS: [i64; 3] = [60, 300, 900];

/// The most delivery Jobs one reconcile pass may create for one policy.
///
/// # The arithmetic bound this makes exact
///
/// Exactly one Job exists per `(alertKey, transition)` — the name is a pure
/// function of both ([`delivery_job_name`]), so a duplicate reconcile is a 409
/// and not a second page (guard **G-SLOT**'s shape). `transition` moves only
/// on open, on resolve and on a re-notify no sooner than
/// `renotifyAfterSeconds`. So over a window `W` one policy creates at most
///
/// ```text
/// kinds(5) × (2 + W / renotifyAfterSeconds) × MAX_DELIVERY_ATTEMPTS
/// ```
///
/// Jobs, and this constant bounds the BURST inside one pass on top of that: a
/// policy that opened four alerts at once creates four Jobs and not five, and
/// the deferred one is picked up by the next pass. The deferral is visible —
/// `delivery.state` stays `Pending` — rather than silent.
pub const MAX_DELIVERY_JOBS_PER_PASS: usize = 4;

/// The most `RecoveryCompleted` entries the ledger carries.
///
/// EIGHT, leaving room inside [`MAX_ALERTS`] for the four policy-keyed kinds
/// twice over. A recovery entry keys on a RESTORE UID, so there is no natural
/// ceiling on how many a busy namespace produces; the oldest DELIVERED ones
/// are dropped first, because an entry nobody has heard about yet is the one
/// that still has work to do.
pub const MAX_RECOVERY_ALERTS: usize = 8;

/// `alerts[].delivery.lastError`'s cap, matching the CRD's `maxLength`.
pub const MAX_ERROR_CHARS: usize = 512;

/// How many hex characters of a sha256 go into a derived object name.
///
/// EIGHT, as D3 §3.4 spells it (`sha8`). It is a NAME and not an identity: the
/// full `(policyUID, alertKey, transition)` triple is inside the event
/// document, and the API server's own 409 on the name is what makes a
/// duplicate reconcile idempotent.
pub const NAME_DIGEST_CHARS: usize = 8;

// ===========================================================================
// Vocabulary
// ===========================================================================

macro_rules! wire_enum {
    ($(#[$meta:meta])* $name:ident {
        $($(#[$vmeta:meta])* $variant:ident => $text:literal),+ $(,)?
    }) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub enum $name {
            $($(#[$vmeta])* $variant),+
        }

        impl $name {
            /// The wire spelling — what lands in a status field and in the
            /// event document.
            #[must_use]
            pub fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $text),+
                }
            }

            /// Every member, in declaration order.
            pub const ALL: &'static [Self] = &[$(Self::$variant),+];

            /// The wire spelling back to a member, or `None`.
            ///
            /// Never a fallback member: "this build does not know that value"
            /// and "this value happened" are different facts, and collapsing
            /// them is how a newer writer's state is read as a healthy one.
            #[must_use]
            pub fn parse(s: &str) -> Option<Self> {
                match s {
                    $($text => Some(Self::$variant),)+
                    _ => None,
                }
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(self.as_str())
            }
        }
    };
}

wire_enum! {
    /// Is there a recovery point inside the objective — D3 §3.2, and the
    /// brief's `Fresh | Stale | Unknown`.
    ///
    /// **`Unknown` IS NOT A THIRD SHADE OF BAD.** It means the question was
    /// not answerable: the catalog view expired, the referenced source or
    /// destination is gone, this policy's own last evaluation is older than
    /// the interval it declares. It is reported as itself and never rendered
    /// as healthy and never as failed — `unknown_is_neither_fresh_nor_stale`
    /// and `unknown_health_is_not_protected_and_not_a_failure` are the tests.
    Freshness {
        Fresh => "Fresh",
        Stale => "Stale",
        Unknown => "Unknown",
    }
}

wire_enum! {
    /// Why [`Freshness`] is what it is. A closed set, so `status` and the
    /// conditions carry a machine-readable reason rather than prose a
    /// reconcile arm invented.
    FreshnessReason {
        /// An available point inside `maxRecoveryPointAgeSeconds`.
        WithinObjective => "WithinObjective",
        /// The newest available point is older than the objective.
        PointOlderThanObjective => "PointOlderThanObjective",
        /// Nothing this policy covers is available at all.
        NoAvailablePoint => "NoAvailablePoint",
        /// `catalogRef` is set, `requireCatalogAvailability` is on, and the
        /// catalog's view is expired or was never synced.
        CatalogStale => "CatalogStale",
        /// The `RecoveryCatalog` this policy names does not exist, or its
        /// pages could not be read.
        CatalogUnreadable => "CatalogUnreadable",
        /// `spec.protects.sourceRef` names no `KafkaCluster`.
        SourceMissing => "SourceMissing",
        /// `spec.protects.destinationRef` names no `BackupDestination`.
        DestinationMissing => "DestinationMissing",
        /// `spec.protects.scheduleRefs` names no `BackupSchedule` that exists.
        ScheduleMissing => "ScheduleMissing",
        /// The object carries no namespace or no UID, so nothing was read and
        /// no verdict was computed. Unreachable for an object that came from
        /// the API server; named rather than borrowed from another reason,
        /// because a status that said `SourceMissing` about an object whose
        /// source was never looked at is a lie with a plausible shape.
        NotEvaluated => "NotEvaluated",
    }
}

wire_enum! {
    /// D3 §3.2's five-valued protection health.
    Health {
        Healthy => "Healthy",
        AtRisk => "AtRisk",
        Stale => "Stale",
        Unprotected => "Unprotected",
        Unknown => "Unknown",
    }
}

impl Health {
    /// `Protected`'s `status`: `True` only for [`Health::Healthy`],
    /// **`Unknown`** for [`Health::Unknown`], `False` otherwise.
    ///
    /// THE MIDDLE CASE IS THE WHOLE POINT. A `False` on an evaluation that
    /// could not happen reads, on every surface, as "Logweir checked and you
    /// are not protected" — a claim this controller did not make. `metav1`
    /// has a third status for exactly this and the UI renders it as its own
    /// state.
    #[must_use]
    pub fn condition_status(self) -> &'static str {
        match self {
            Self::Healthy => "True",
            Self::Unknown => "Unknown",
            Self::AtRisk | Self::Stale | Self::Unprotected => "False",
        }
    }
}

wire_enum! {
    /// How availability was decided — printed, because "the point is in
    /// storage" and "a `Backup` object says it succeeded" are different
    /// claims and an incident is the wrong time to learn which one you had.
    AvailabilityBasis {
        /// No `catalogRef`: the verdict comes from `Backup.status` alone.
        KubernetesStatus => "KubernetesStatus",
        /// A fresh catalog view answered.
        Catalog => "Catalog",
        /// A `catalogRef` is set and its view could not answer. Always
        /// [`Freshness::Unknown`] — never `Healthy`.
        CatalogStale => "CatalogStale",
    }
}

wire_enum! {
    /// `alerts[].state`.
    AlertState {
        Open => "Open",
        Resolved => "Resolved",
    }
}

wire_enum! {
    /// `alerts[].delivery.state`.
    DeliveryState {
        /// A Job exists (or is about to) for this transition.
        Pending => "Pending",
        /// The Job exited 0 and every configured sink accepted.
        Delivered => "Delivered",
        /// Three attempts were made and none succeeded.
        Failed => "Failed",
        /// Nothing was sent, on purpose: no route is configured, this kind is
        /// not in `notifications.kinds`, or `sendResolved` is off and this is
        /// a resolve.
        Suppressed => "Suppressed",
    }
}

wire_enum! {
    /// How thoroughly the archive behind an alert was checked.
    ///
    /// **THERE IS NO `Complete`.** Logweir compares a sample. The value
    /// travels into a PagerDuty incident title and a Slack channel, where it
    /// is read by someone deciding during an incident whether an archive can
    /// be trusted; a fourth variant here would be the product's one
    /// unrecoverable lie. `logweir notify deliver` refuses `"complete"` at
    /// parse time (`logweir::notify::VerificationScope`), and this enum is
    /// why the controller can never write it.
    VerificationScope {
        Sampled => "sampled",
        Degraded => "degraded",
        None => "none",
    }
}

wire_enum! {
    /// The evidence verdict for a point, as
    /// `status.lastAvailablePoint.evidence` spells it.
    ///
    /// `ValidHistorical` is the pair (`result: Valid`,
    /// `trust.basis: Historical`) flattened: the key was valid when it signed
    /// and has since been retired, which is what rotation looks like and is a
    /// PASS. `Untrusted` is a signature that verifies under a key this
    /// installation will not accept — a different fact from `Invalid`, and the
    /// one that matters.
    Evidence {
        Valid => "Valid",
        ValidHistorical => "ValidHistorical",
        Untrusted => "Untrusted",
        NotAttempted => "NotAttempted",
    }
}

impl Evidence {
    /// Whether this verdict satisfies `objectives.requireVerifiedEvidence`.
    ///
    /// `Valid` and `ValidHistorical` only. `Untrusted` is deliberately NOT a
    /// pass even though the bytes verify: the installation said it does not
    /// accept that key, and a protection objective that ignored the answer
    /// would make `TrustPolicy` decorative.
    #[must_use]
    pub fn is_verified(self) -> bool {
        matches!(self, Self::Valid | Self::ValidHistorical)
    }

    /// Read a `Backup.status.evidence.verification` pair into this vocabulary.
    ///
    /// `None` result — no verification block at all — is
    /// [`Evidence::NotAttempted`], which is what an unverified point IS. It is
    /// never read as a pass.
    #[must_use]
    pub fn from_verification(result: Option<&str>, trust_basis: Option<&str>) -> Self {
        match (result, trust_basis) {
            (Some("Valid"), Some("Historical")) => Self::ValidHistorical,
            (Some("Valid"), _) => Self::Valid,
            (Some("Untrusted"), _) => Self::Untrusted,
            _ => Self::NotAttempted,
        }
    }
}

/// The four alert kinds keyed on the POLICY UID.
///
/// A separate type from [`AlertKind`] and not a runtime check, mirroring
/// `logweir::notify::PolicyAlertKind`: `RecoveryCompleted` keys on the
/// **Restore** UID, and a builder that could be handed it would key every
/// recovery of a policy onto one incident, so the second restore of the day
/// overwrites the first one's message. Here the mistake does not compile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PolicyAlertKind {
    /// [`AlertKind::BackupFailure`].
    BackupFailure,
    /// [`AlertKind::Staleness`].
    Staleness,
    /// [`AlertKind::ArchiveUnavailable`].
    ArchiveUnavailable,
    /// [`AlertKind::RehearsalFailure`].
    RehearsalFailure,
}

impl PolicyAlertKind {
    /// Every member, in declaration order.
    pub const ALL: [Self; 4] = [
        Self::BackupFailure,
        Self::Staleness,
        Self::ArchiveUnavailable,
        Self::RehearsalFailure,
    ];

    /// The wire spelling, which is also the tail of the dedup key.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::BackupFailure => "BackupFailure",
            Self::Staleness => "Staleness",
            Self::ArchiveUnavailable => "ArchiveUnavailable",
            Self::RehearsalFailure => "RehearsalFailure",
        }
    }

    /// The CRD's five-variant enum, widened.
    #[must_use]
    pub fn widen(self) -> AlertKind {
        match self {
            Self::BackupFailure => AlertKind::BackupFailure,
            Self::Staleness => AlertKind::Staleness,
            Self::ArchiveUnavailable => AlertKind::ArchiveUnavailable,
            Self::RehearsalFailure => AlertKind::RehearsalFailure,
        }
    }

    /// The narrowing, `None` for `RecoveryCompleted`.
    #[must_use]
    pub fn narrow(kind: AlertKind) -> Option<Self> {
        match kind {
            AlertKind::BackupFailure => Some(Self::BackupFailure),
            AlertKind::Staleness => Some(Self::Staleness),
            AlertKind::ArchiveUnavailable => Some(Self::ArchiveUnavailable),
            AlertKind::RehearsalFailure => Some(Self::RehearsalFailure),
            AlertKind::RecoveryCompleted => None,
        }
    }
}

/// The wire spelling of a CRD [`AlertKind`].
#[must_use]
pub fn alert_kind_str(kind: AlertKind) -> &'static str {
    match kind {
        AlertKind::BackupFailure => "BackupFailure",
        AlertKind::Staleness => "Staleness",
        AlertKind::ArchiveUnavailable => "ArchiveUnavailable",
        AlertKind::RehearsalFailure => "RehearsalFailure",
        AlertKind::RecoveryCompleted => "RecoveryCompleted",
    }
}

/// Whether this kind may reach PagerDuty at all.
///
/// `RecoveryCompleted` is webhook/Slack only (D3 §3.3). D3 W4 records the
/// consequence W6 has to honour: a `RecoveryCompleted` event whose routes are
/// PagerDuty-only configures ZERO sinks, so `logweir notify deliver` exits 1
/// with `notify-result=none:unconfigured`. That is a **no-op, not a failure**,
/// and [`classify_delivery`] is where it is turned into
/// [`DeliveryState::Suppressed`] rather than into three wasted attempts and a
/// red `NotificationsDelivered`.
#[must_use]
pub fn kind_pages(kind: AlertKind) -> bool {
    !matches!(kind, AlertKind::RecoveryCompleted)
}

// ===========================================================================
// Dedup keys — the same family `logweir::notify` builds
// ===========================================================================

/// The prefix that keeps the protection family apart from `logweir-drill-…`,
/// so a protection `resolve` can never close a drill's open page.
pub const DEDUP_PREFIX: &str = "logweir-protection-";

/// What a blank identifier collapses to.
///
/// NOT THE EMPTY STRING. `logweir-protection--Staleness` would be ONE incident
/// per kind across every policy in the cluster: one team's `resolve` closes
/// another team's open page, silently. `unknown` is wrong in a way somebody
/// reads.
pub const UNKNOWN_IDENT: &str = "unknown";

/// `logweir-protection-<policyUID>-<kind>` — D3 §3.3.
#[must_use]
pub fn dedup_key(policy_uid: &str, kind: PolicyAlertKind) -> String {
    let uid = ident(policy_uid);
    format!("{DEDUP_PREFIX}{uid}-{}", kind.as_str())
}

/// `logweir-protection-<restoreUID>-RecoveryCompleted` — D3 §3.3's exception.
///
/// Keyed on the RESTORE and not on the policy: a policy's points are restored
/// many times, and each of those is its own completed recovery with its own
/// topic names and counts.
#[must_use]
pub fn recovery_completed_key(restore_uid: &str) -> String {
    let uid = ident(restore_uid);
    format!("{DEDUP_PREFIX}{uid}-RecoveryCompleted")
}

fn ident(raw: &str) -> &str {
    if raw.trim().is_empty() {
        UNKNOWN_IDENT
    } else {
        raw
    }
}

/// The first [`NAME_DIGEST_CHARS`] hex characters of `sha256(parts joined by
/// '|')`.
#[must_use]
pub fn sha8(parts: &[&str]) -> String {
    let joined = parts.join("|");
    logweir_core::ids::sha256_hex(joined.as_bytes())[..NAME_DIGEST_CHARS].to_string()
}

/// `<policy>-ev-<sha8(alertKey|transition)>` — the immutable event ConfigMap.
#[must_use]
pub fn event_config_map_name(policy: &str, alert_key: &str, transition: i64) -> String {
    let digest = sha8(&[alert_key, &transition.to_string()]);
    truncate_name(
        &format!("{policy}-ev-{digest}"),
        policy,
        &format!("ev-{digest}"),
    )
}

/// `<policy>-n-<sha8(policyUID|alertKey|transition)>-<attempt>` — the delivery
/// Job.
///
/// THE NAME IS THE DEDUPLICATION. A duplicate reconcile computes the same
/// string and gets **409 `AlreadyExists`** from the API server rather than
/// creating a second Job that pages a human a second time — the same property
/// guard **G-SLOT** gives a scheduled `Backup`. `attempt` is in the name
/// because a retry is a NEW Job (the previous one has a terminal pod whose
/// exit code is still being read), and `transition` is in it because a
/// re-notify is a new message about the same open alert.
#[must_use]
pub fn delivery_job_name(
    policy: &str,
    policy_uid: &str,
    alert_key: &str,
    transition: i64,
    attempt: i64,
) -> String {
    let digest = sha8(&[ident(policy_uid), alert_key, &transition.to_string()]);
    let suffix = format!("n-{digest}-{attempt}");
    truncate_name(&format!("{policy}-{suffix}"), policy, &suffix)
}

/// Keep a derived name inside Kubernetes' 63-character `metadata.name` budget
/// by trimming the POLICY half, never the digest half.
///
/// Trimming the digest would make two policies' Jobs collide; trimming the
/// prefix only costs readability, and the full policy name is on the owner
/// reference and in the event document either way.
fn truncate_name(full: &str, policy: &str, suffix: &str) -> String {
    const LIMIT: usize = 63;
    if full.len() <= LIMIT {
        return full.to_string();
    }
    let budget = LIMIT.saturating_sub(suffix.len() + 1);
    let head: String = policy.chars().take(budget).collect();
    format!("{}-{suffix}", head.trim_end_matches('-'))
}

// ===========================================================================
// Inputs
// ===========================================================================

/// One candidate recovery point, as the controller read it off a `Backup` and
/// (optionally) a catalog entry.
///
/// A FLAT STRUCT AND NOT A `&Backup`, so [`evaluate`] is testable over a table
/// and so the catalog path and the Kubernetes path reach the same rule. The
/// controller's `candidate_from_backup` is the one place a `Backup` becomes
/// one of these.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PointCandidate {
    /// The `Backup` object's name, while one exists.
    pub backup_name: Option<String>,
    /// D3 §5.1's identity, when it is derivable — see
    /// [`point_id_from_receipt_digest`].
    pub point_id: Option<String>,
    /// **Capture start.** `Backup.status.capture.startedAt`.
    pub recovery_point_at: Option<Time>,
    /// `windowCovered.toMs - 1 ms`, labelled separately and never used for the
    /// objective.
    pub newest_record_at: Option<Time>,
    /// `status.phase`.
    pub phase: Option<String>,
    /// `status.exitCode`.
    pub exit_code: Option<i32>,
    /// The evidence verdict.
    pub evidence: Evidence,
    /// The topics the point covers, in the order the run resolved them.
    pub topics: Vec<String>,
    /// The run selected `allUserTopics`, so its topic set is "whatever the
    /// principal could see" and is NOT enumerated on the object
    /// (`SelectionStatus` carries counts, not names).
    ///
    /// **It satisfies `topics ⊆ point topics` and the honest reading is
    /// recorded here**: a dynamic run's `spec.topics` is `[]`, so a literal
    /// subset test would make every dynamic installation `Unprotected` while
    /// its backups ran perfectly. What Logweir can say is that the run asked
    /// for every user topic; what it cannot say is that the broker showed it
    /// every one (that is D2's `visibility`/`coverage` axis, and
    /// `status.lastAvailablePoint.topics` is empty for such a point rather
    /// than listing a set nobody recorded).
    pub covers_all_topics: bool,
    /// Whether the `Backup`'s archive/destination matches what this policy
    /// protects. Decided by the controller, which holds the resolved
    /// destination; carried as a boolean so the rule stays testable.
    pub destination_matches: bool,
    /// Whether the `Backup`'s `sourceRef` matches `spec.protects.sourceRef`.
    pub source_matches: bool,
}

impl PointCandidate {
    /// Whether the run itself succeeded: `phase: Succeeded` **and**
    /// `exitCode == 0`.
    ///
    /// BOTH, and that is not belt and braces. A crashed Job reaches a terminal
    /// phase with NO exit code at all
    /// (`controllers::backup::crash_terminal_state`), and a point whose run
    /// left no exit code is a point nobody can say completed.
    #[must_use]
    pub fn succeeded(&self) -> bool {
        self.phase.as_deref() == Some("Succeeded") && self.exit_code == Some(0)
    }
}

/// D3 §5.1's point identity, derived from the digest the controller already
/// recorded instead of from a catalog round trip.
///
/// `pointId = "lwp1-" + lowercase_hex(sha256(receipt_bytes))[0..32]`, and
/// `Backup.status.evidence.receiptSha256` IS `sha256:<that hex>` — the
/// controller COMPUTED it over the bytes it fetched (`crds/backup.rs`'s own
/// note), so the two agree by construction and this is the same value the
/// catalog would supply. That matters because
/// `logweir::notify::LastAvailablePoint` requires `point_id`: without this,
/// every event from a `KubernetesStatus`-basis policy would have to omit the
/// whole `last_available_point` block and print "no available recovery point"
/// into an incident about a point that exists.
///
/// `None` for a digest that is not `sha256:` + 64 hex characters. Nothing is
/// invented: a point with no recorded receipt digest has no identity here.
#[must_use]
pub fn point_id_from_receipt_digest(receipt_sha256: &str) -> Option<String> {
    let hex = receipt_sha256.strip_prefix("sha256:")?;
    if hex.len() != 64 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    Some(format!("lwp1-{}", hex[..32].to_ascii_lowercase()))
}

/// One catalog entry, narrowed to the two axes D3 §3.2 reads.
///
/// **DELIBERATELY NOT `catalog_view::ViewEntry`.** The page body is
/// `entries.jsonl`, one compact JSON object per line, and this type reads it
/// with `serde`'s default leniency — unknown fields ignored — so a catalog
/// written by a newer controller still answers "is the point there?" instead
/// of failing to parse. The reverse coupling would make protection health
/// depend on the catalog writer's field set, which is the opposite of what a
/// bounded projection is for.
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CatalogEntry {
    /// D3 §5.1's identity.
    pub point_id: String,
    /// The archive set this point belongs to.
    #[serde(default)]
    pub backup_id: String,
    /// Capture start, in epoch milliseconds.
    #[serde(default)]
    pub recovery_point_at_ms: i64,
    /// D3 §5.4's first axis: `Available`, `Missing`, `Unreadable`, `Deleted`,
    /// `Conflict`, `UnsupportedFormat`, `Partial`.
    #[serde(default)]
    pub availability: String,
    /// D3 §5.4's second axis: `Verified`, `VerifiedHistorical`,
    /// `UntrustedSigner`, `Revoked`, `Invalid`, `NoEvidence`, `NotAttempted`.
    #[serde(default)]
    pub verification: String,
    /// `availability.selectable() && verification.selectable()`, materialised
    /// by the catalog controller so no surface recomputes D3 §5.4's rule.
    #[serde(default)]
    pub selectable: bool,
}

impl CatalogEntry {
    /// Whether the bytes are there — the FIRST axis alone.
    #[must_use]
    pub fn is_available(&self) -> bool {
        self.availability == "Available"
    }

    /// Whether this entry is one of D3 §3.3's `ArchiveUnavailable` triggers:
    /// the bytes are gone or unreadable, or the signature is not one this
    /// installation accepts.
    #[must_use]
    pub fn is_degraded(&self) -> bool {
        matches!(
            self.availability.as_str(),
            "Missing" | "Unreadable" | "Deleted" | "Conflict" | "UnsupportedFormat" | "Partial"
        ) || matches!(
            self.verification.as_str(),
            "UntrustedSigner" | "Revoked" | "Invalid"
        )
    }
}

/// What the catalog could say, if anything.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogAnswer {
    /// No `catalogRef`, or `requireCatalogAvailability` is off. Availability
    /// is judged from `Backup.status` and `status.availabilityBasis` says so.
    NotConsulted,
    /// A fresh view answered, with these entries.
    Fresh(Vec<CatalogEntry>),
    /// A `catalogRef` is set and the view is expired, never synced, or the
    /// object is gone. **Never `Healthy`** — D3 §3.2.
    Stale(FreshnessReason),
}

/// One `BackupSchedule` as this policy sees it, plus the run facts D1 owns.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ScheduleFacts {
    /// `metadata.name`.
    pub name: String,
    /// `spec.suspend`.
    pub suspended: bool,
    /// The `Ready` condition's `status`, or `None` when the schedule carries
    /// none.
    pub ready: Option<String>,
    /// `status.nextFireTime`.
    pub next_fire_time: Option<Time>,
    /// `status.lastMissedSlot`.
    pub last_missed_slot: Option<String>,
    /// `status.lastFireTime`.
    pub last_fire_time: Option<Time>,
    /// D1 W2's `status.missedSlots`, when the schedule carries it. Read
    /// defensively out of the serialized status: it is a CONTRACT this worker
    /// consumes, not a field it can require, and a policy that refused to
    /// evaluate until a neighbouring worker merged would be a protection
    /// verdict held hostage to a rebase.
    pub missed_slots: Option<i64>,
    /// Whether the schedule resolved at all.
    pub exists: bool,
}

/// Which way a run ended, for the consecutive-failure count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotOutcome {
    /// Terminal and successful.
    Succeeded,
    /// Terminal and not successful.
    Failed,
    /// Not terminal yet. Counts as neither, and STOPS the walk — see
    /// [`consecutive_failed_slots`].
    Running,
}

/// One slot of one schedule's history, newest first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotRun {
    /// The slot the run belongs to, or the run's own name for a manual run.
    pub slot: String,
    /// The attempt number: `0` for the first, `n` for the nth retry.
    pub attempt: u32,
    /// How it ended.
    pub outcome: SlotOutcome,
    /// When it ended (or started, for a run with no finish). Newest first is
    /// the caller's ordering; this is carried for the `lastAttempt` block.
    pub at: Option<Time>,
    /// The `Backup` object's name.
    pub backup_name: String,
    /// The terminal reason, if any.
    pub reason: Option<String>,
}

/// The rehearsal side of protection, as PLAT-14.3 will report it.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RehearsalFacts {
    /// When a rehearsal last passed.
    pub last_succeeded_at: Option<Time>,
    /// The `Restore` it ran as.
    pub last_restore: Option<String>,
    /// When one last failed.
    pub last_failed_at: Option<Time>,
    /// Why.
    pub last_reason: Option<String>,
}

/// Everything [`evaluate`] reads.
#[derive(Debug, Clone)]
pub struct Inputs<'a> {
    /// The policy's spec.
    pub spec: &'a ProtectionPolicySpec,
    /// Whether `spec.protects.sourceRef` resolved.
    pub source_exists: bool,
    /// Whether `spec.protects.destinationRef` resolved. `true` when the policy
    /// names a `legacyArchive` instead.
    pub destination_exists: bool,
    /// Every candidate point, in any order.
    pub candidates: &'a [PointCandidate],
    /// What the catalog said.
    pub catalog: &'a CatalogAnswer,
    /// The schedules, in `spec.protects.scheduleRefs` order.
    pub schedules: &'a [ScheduleFacts],
    /// The slot history, newest first, already de-duplicated per slot by the
    /// caller's list order.
    pub slots: &'a [SlotRun],
    /// The rehearsal facts.
    pub rehearsal: &'a RehearsalFacts,
    /// Now.
    pub now: Time,
}

// ===========================================================================
// The verdict
// ===========================================================================

/// The newest available point, as the verdict describes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AvailablePointFacts {
    /// D3 §5.1's identity, when derivable.
    pub point_id: Option<String>,
    /// The `Backup` object, while one exists.
    pub backup_name: Option<String>,
    /// **Capture start.**
    pub recovery_point_at: Option<Time>,
    /// The newest archived record instant, labelled separately.
    pub newest_record_at: Option<Time>,
    /// `now - recovery_point_at`, in seconds.
    pub age_seconds: Option<i64>,
    /// The evidence verdict.
    pub evidence: Evidence,
    /// The topics, capped at [`MAX_TOPICS`].
    pub topics: Vec<String>,
    /// Whether `topics` was cut short.
    pub topics_truncated: bool,
}

/// Everything one evaluation concluded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Verdict {
    /// D3 §3.2's five-valued health.
    pub health: Health,
    /// The brief's three-valued freshness.
    pub freshness: Freshness,
    /// Why.
    pub reason: FreshnessReason,
    /// How availability was decided.
    pub basis: AvailabilityBasis,
    /// The newest available point.
    pub point: Option<AvailablePointFacts>,
    /// The most recent run, whatever became of it.
    pub last_attempt: Option<LastAttempt>,
    /// Consecutive failed **slots** — a retry chain is one.
    pub consecutive_failed_runs: i64,
    /// Missed-slot facts.
    pub missed: MissedSummary,
    /// The schedules, as this policy sees them.
    pub schedules: Vec<ScheduleSummary>,
    /// The rehearsal side.
    pub rehearsal: Option<RehearsalSummary>,
    /// Which alerts D3 §3.3 says should be OPEN right now.
    pub open_kinds: Vec<PolicyAlertKind>,
    /// One sentence for a human. Reaches a PagerDuty incident title verbatim.
    pub summary: String,
}

/// D3 §3.2's availability rule, as one predicate with the four conjuncts
/// named.
///
/// `catalog` is consulted only when the policy set `catalogRef` AND
/// `requireCatalogAvailability`; [`CatalogAnswer::NotConsulted`] is what the
/// caller passes otherwise, and a [`CatalogAnswer::Stale`] makes NOTHING
/// available — which is how `availabilityBasis: CatalogStale` becomes
/// `health: Unknown` and never `Healthy`.
#[must_use]
pub fn is_available(
    candidate: &PointCandidate,
    spec: &ProtectionPolicySpec,
    catalog: &CatalogAnswer,
) -> bool {
    if !candidate.succeeded() {
        return false;
    }
    if spec.objectives.require_verified_evidence && !candidate.evidence.is_verified() {
        return false;
    }
    if !candidate.source_matches || !candidate.destination_matches {
        return false;
    }
    if !candidate.covers_all_topics && !topics_covered(spec, &candidate.topics) {
        return false;
    }
    match catalog {
        CatalogAnswer::NotConsulted => true,
        CatalogAnswer::Stale(_) => false,
        CatalogAnswer::Fresh(entries) => match candidate.point_id.as_deref() {
            // A point the catalog has never heard of is not available when the
            // operator asked for catalog availability: the whole point of the
            // objective is that the bytes were confirmed to be there.
            Some(id) => entries.iter().any(|e| e.point_id == id && e.is_available()),
            None => false,
        },
    }
}

/// `topics ⊆ point topics` — D3 §3.2.
///
/// An ABSENT `spec.protects.topics` is "the topics of the newest matching
/// point", which is every point: the operator declared a source and a schedule
/// set and let the schedule decide the topics, and a policy that then matched
/// nothing would report `Unprotected` for a perfectly protected cluster.
#[must_use]
pub fn topics_covered(spec: &ProtectionPolicySpec, point_topics: &[String]) -> bool {
    match spec.protects.topics.as_deref() {
        None => true,
        Some(required) => required.iter().all(|t| point_topics.iter().any(|p| p == t)),
    }
}

/// D3 §3.2's failure accounting: consecutive failed **SLOTS**, newest first, a
/// retry chain counting once.
///
/// # Why the walk stops at a running slot and not at a successful one only
///
/// `slots` is newest-first. A slot still running has not failed, and counting
/// past it would report a failure streak that a run in flight may be about to
/// end — an alert opened for a condition that is being fixed while it is
/// opened. It stops there and the count is the streak BELOW it, which is the
/// honest one.
///
/// A retry chain (D1's `trigger.kind=Retry`, `attempt`, `retryOf`) is one
/// slot: only the FINAL attempt of each slot — the first one seen in a
/// newest-first list — votes, so `maxConsecutiveFailedRuns` is a count of
/// slots and not of attempts. Three retries of one nightly slot is ONE failed
/// slot, which is what an operator who set the threshold to 2 meant.
#[must_use]
pub fn consecutive_failed_slots(slots: &[SlotRun]) -> i64 {
    let mut seen: Vec<&str> = Vec::new();
    let mut count = 0i64;
    for run in slots {
        if seen.contains(&run.slot.as_str()) {
            // An earlier attempt of a slot already counted. Not a second vote.
            continue;
        }
        seen.push(run.slot.as_str());
        match run.outcome {
            SlotOutcome::Failed => count += 1,
            SlotOutcome::Succeeded | SlotOutcome::Running => break,
        }
    }
    count
}

/// Evaluate one policy — D3 §3.2's whole table, as one pure function.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn evaluate(input: &Inputs<'_>) -> Verdict {
    let spec = input.spec;
    let consult_catalog =
        spec.protects.catalog_ref.is_some() && spec.objectives.require_catalog_availability;
    let catalog = if consult_catalog {
        input.catalog
    } else {
        &CatalogAnswer::NotConsulted
    };

    let basis = match catalog {
        CatalogAnswer::NotConsulted => AvailabilityBasis::KubernetesStatus,
        CatalogAnswer::Fresh(_) => AvailabilityBasis::Catalog,
        CatalogAnswer::Stale(_) => AvailabilityBasis::CatalogStale,
    };

    // The newest available point, by CAPTURE START. A candidate with no
    // capture start cannot be aged and therefore cannot satisfy an objective
    // measured in seconds; it is not available.
    let mut available: Vec<&PointCandidate> = input
        .candidates
        .iter()
        .filter(|c| c.recovery_point_at.is_some() && is_available(c, spec, catalog))
        .collect();
    available.sort_by_key(|c| std::cmp::Reverse(c.recovery_point_at));
    let newest = available.first().copied();

    let point = newest.map(|c| point_facts(c, input.now));
    let age = point.as_ref().and_then(|p| p.age_seconds);

    let schedules: Vec<ScheduleSummary> = input
        .schedules
        .iter()
        .take(MAX_SCHEDULES)
        .map(|s| ScheduleSummary {
            name: s.name.clone(),
            suspended: Some(s.suspended),
            ready: s.ready.clone(),
            next_fire_time: s.next_fire_time,
            last_missed_slot: s.last_missed_slot.clone(),
        })
        .collect();

    let consecutive_failed_runs = consecutive_failed_slots(input.slots);
    let missed = missed_summary(input);
    let last_attempt = input.slots.first().map(|run| LastAttempt {
        backup_ref: Some(LocalRef {
            name: run.backup_name.clone(),
        }),
        phase: Some(
            match run.outcome {
                SlotOutcome::Succeeded => "Succeeded",
                SlotOutcome::Failed => "Failed",
                SlotOutcome::Running => "Running",
            }
            .to_string(),
        ),
        reason: run.reason.clone(),
        at: run.at,
    });

    // ------------------------------------------------------------------
    // Freshness, then health. In that order: health is freshness plus the
    // risk signals, and computing them together is how `Unknown` acquires a
    // `Healthy` arm.
    // ------------------------------------------------------------------
    let unresolvable = if !input.source_exists {
        Some(FreshnessReason::SourceMissing)
    } else if !input.destination_exists {
        Some(FreshnessReason::DestinationMissing)
    } else if spec
        .protects
        .schedule_refs
        .as_ref()
        .is_some_and(|r| !r.is_empty())
        && !input.schedules.is_empty()
        && input.schedules.iter().all(|s| !s.exists)
    {
        Some(FreshnessReason::ScheduleMissing)
    } else if let CatalogAnswer::Stale(reason) = catalog {
        Some(*reason)
    } else {
        None
    };

    let (freshness, reason) = match unresolvable {
        Some(reason) => (Freshness::Unknown, reason),
        None => match (newest, age) {
            (Some(_), Some(age))
                if age <= i64::from(spec.objectives.max_recovery_point_age_seconds) =>
            {
                (Freshness::Fresh, FreshnessReason::WithinObjective)
            }
            (Some(_), Some(_)) => (Freshness::Stale, FreshnessReason::PointOlderThanObjective),
            _ => (Freshness::Stale, FreshnessReason::NoAvailablePoint),
        },
    };

    let at_risk = consecutive_failed_runs >= i64::from(spec.objectives.max_consecutive_failed_runs)
        && spec.objectives.max_consecutive_failed_runs > 0
        || input.schedules.iter().any(|s| s.suspended)
        || input
            .schedules
            .iter()
            .any(|s| s.ready.as_deref() == Some("False"))
        || missed.last_missed_slot.is_some();

    let health = match freshness {
        Freshness::Unknown => Health::Unknown,
        Freshness::Fresh if at_risk => Health::AtRisk,
        Freshness::Fresh => Health::Healthy,
        Freshness::Stale if newest.is_some() => Health::Stale,
        Freshness::Stale => Health::Unprotected,
    };

    let open_kinds = open_alert_kinds(input, health, consecutive_failed_runs, newest, catalog);
    let summary = summarize(
        spec,
        health,
        reason,
        point.as_ref(),
        consecutive_failed_runs,
    );

    Verdict {
        health,
        freshness,
        reason,
        basis,
        point,
        last_attempt,
        consecutive_failed_runs,
        missed,
        schedules,
        rehearsal: rehearsal_summary(input.rehearsal),
        open_kinds,
        summary,
    }
}

fn point_facts(candidate: &PointCandidate, now: Time) -> AvailablePointFacts {
    let age = candidate
        .recovery_point_at
        .map(|at| (now - at).num_seconds().max(0));
    let truncated = candidate.topics.len() > MAX_TOPICS;
    AvailablePointFacts {
        point_id: candidate.point_id.clone(),
        backup_name: candidate.backup_name.clone(),
        recovery_point_at: candidate.recovery_point_at,
        newest_record_at: candidate.newest_record_at,
        age_seconds: age,
        evidence: candidate.evidence,
        topics: candidate.topics.iter().take(MAX_TOPICS).cloned().collect(),
        topics_truncated: truncated,
    }
}

fn missed_summary(input: &Inputs<'_>) -> MissedSummary {
    // The NEWEST missed slot across the schedules: a slot name is
    // `yyyymmdd-hhmmss`, so lexical order IS chronological order.
    let last = input
        .schedules
        .iter()
        .filter_map(|s| s.last_missed_slot.clone())
        .max();
    let since_last_fire = input
        .schedules
        .iter()
        .filter_map(|s| s.last_fire_time)
        .max()
        .map(|at| (input.now - at).num_seconds().max(0));
    MissedSummary {
        last_missed_slot: last,
        since_last_fire,
    }
}

fn rehearsal_summary(facts: &RehearsalFacts) -> Option<RehearsalSummary> {
    if facts.last_succeeded_at.is_none()
        && facts.last_failed_at.is_none()
        && facts.last_restore.is_none()
    {
        return None;
    }
    Some(RehearsalSummary {
        last_succeeded_at: facts.last_succeeded_at,
        last_restore_ref: facts
            .last_restore
            .as_ref()
            .map(|name| LocalRef { name: name.clone() }),
        last_failed_at: facts.last_failed_at,
        last_reason: facts.last_reason.clone(),
    })
}

/// D3 §3.3's "opens when" column, as one function.
fn open_alert_kinds(
    input: &Inputs<'_>,
    health: Health,
    consecutive_failed_runs: i64,
    newest: Option<&PointCandidate>,
    catalog: &CatalogAnswer,
) -> Vec<PolicyAlertKind> {
    let spec = input.spec;
    let mut open = Vec::new();

    if spec.objectives.max_consecutive_failed_runs > 0
        && consecutive_failed_runs >= i64::from(spec.objectives.max_consecutive_failed_runs)
    {
        open.push(PolicyAlertKind::BackupFailure);
    }
    if health == Health::Stale {
        open.push(PolicyAlertKind::Staleness);
    }

    // `ArchiveUnavailable`: the newest OTHERWISE-available point is degraded
    // in the catalog. "Otherwise available" is the run half of the rule
    // without its catalog conjunct — the point Logweir would have chosen if
    // the bytes were there — which is why this is not simply "the chosen point
    // is missing": the chosen point is by construction available, so that
    // reading would make the alert unreachable.
    if let CatalogAnswer::Fresh(entries) = catalog {
        let mut otherwise: Vec<&PointCandidate> = input
            .candidates
            .iter()
            .filter(|c| {
                c.recovery_point_at.is_some() && is_available(c, spec, &CatalogAnswer::NotConsulted)
            })
            .collect();
        otherwise.sort_by_key(|c| std::cmp::Reverse(c.recovery_point_at));
        if let Some(top) = otherwise.first() {
            let degraded = top
                .point_id
                .as_deref()
                .is_some_and(|id| entries.iter().any(|e| e.point_id == id && e.is_degraded()));
            // Only when the degraded point is NOT the one finally chosen —
            // otherwise the chosen point is available and there is nothing
            // unavailable to report.
            let chosen_is_top = newest.is_some_and(|n| n.point_id == top.point_id);
            if degraded && !chosen_is_top {
                open.push(PolicyAlertKind::ArchiveUnavailable);
            }
        }
    }

    if let Some(max_age) = spec.objectives.max_rehearsal_age_seconds {
        let failed_latest = match (
            input.rehearsal.last_failed_at,
            input.rehearsal.last_succeeded_at,
        ) {
            (Some(failed), Some(ok)) => failed > ok,
            (Some(_), None) => true,
            _ => false,
        };
        let stale = input
            .rehearsal
            .last_succeeded_at
            .is_none_or(|at| (input.now - at).num_seconds() > i64::from(max_age));
        if failed_latest || stale {
            open.push(PolicyAlertKind::RehearsalFailure);
        }
    }
    open
}

/// The one controller-authored sentence that reaches an incident title.
///
/// # It says what Logweir DID, and never claims an exhaustive check
///
/// D3 W4's delivery path scans the summary against a fixed phrase list and
/// replaces a match with `[claim removed]` — including the word `exhaustive`
/// inside a DENIAL, because these channels truncate and a sentence surviving
/// as "…an exhaustive comparison" is worse than no sentence. Nothing generated
/// here reaches for that vocabulary; `no_summary_claims_an_exhaustive_check`
/// is the test over every arm.
#[must_use]
pub fn summarize(
    spec: &ProtectionPolicySpec,
    health: Health,
    reason: FreshnessReason,
    point: Option<&AvailablePointFacts>,
    consecutive_failed_runs: i64,
) -> String {
    let objective = i64::from(spec.objectives.max_recovery_point_age_seconds);
    match health {
        Health::Healthy => match point.and_then(|p| p.age_seconds) {
            Some(age) => format!(
                "the newest available recovery point is {} old, inside the objective of {}",
                humanize(age),
                humanize(objective)
            ),
            None => "an available recovery point is inside the objective".to_string(),
        },
        Health::AtRisk => format!(
            "a recovery point is inside the objective, but protection is at risk: \
             {consecutive_failed_runs} consecutive failed slots, a suspended or not-ready \
             schedule, or a missed slot"
        ),
        Health::Stale => match point.and_then(|p| p.age_seconds) {
            Some(age) => format!(
                "the newest available recovery point is {} old, past the objective of {}",
                humanize(age),
                humanize(objective)
            ),
            None => format!(
                "the newest available recovery point is past the objective of {}",
                humanize(objective)
            ),
        },
        Health::Unprotected => {
            "there is no available recovery point for this policy at all".to_string()
        }
        Health::Unknown => format!(
            "protection could not be evaluated ({}); this is not a pass and not a failure",
            reason.as_str()
        ),
    }
}

/// `31h`, `2d 3h`, `45m`, `12s` — bounded, no locale, no prose.
#[must_use]
pub fn humanize(seconds: i64) -> String {
    let s = seconds.max(0);
    let (d, h, m) = (s / 86_400, (s % 86_400) / 3_600, (s % 3_600) / 60);
    if d > 0 {
        format!("{d}d {h}h")
    } else if h > 0 {
        format!("{h}h {m}m")
    } else if m > 0 {
        format!("{m}m")
    } else {
        format!("{s}s")
    }
}

// ===========================================================================
// The ledger
// ===========================================================================

/// What one pass decided about one alert.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LedgerOutcome {
    /// The new ledger, capped at [`MAX_ALERTS`], in a stable order.
    pub alerts: Vec<AlertEntry>,
    /// The entries whose `transition` has no delivery Job yet, in the order
    /// they should be delivered. Bounded by [`MAX_DELIVERY_JOBS_PER_PASS`] at
    /// the CALLER, not here — this reports everything that is due so the
    /// caller can say how many it deferred.
    pub due: Vec<AlertEntry>,
}

/// D3 §3.3's deduplication, as one pure function over the stored ledger.
///
/// # The property, stated as the thing that goes wrong without it
///
/// A condition that stays true produces NO further transitions. Not one per
/// reconcile, not one per minute — none, until it resolves or until
/// `renotifyAfterSeconds` elapses. The failure mode this prevents is an
/// on-call rotation muting Logweir's integration in week two, after which no
/// alert from this product reaches anybody ever again. `transition` is the
/// counter and `notifiedTransition` is the high-water mark a delivery Job was
/// created for; the pair survives a controller restart because both live in
/// the object's status and neither is derived from a clock.
///
/// `desired_open` is [`Verdict::open_kinds`]. Entries for kinds no longer open
/// are RESOLVED, not deleted: the resolve is a transition of its own and the
/// `resolve` PagerDuty needs under the same `dedup_key`.
#[must_use]
pub fn reconcile_alerts(
    existing: &[AlertEntry],
    desired_open: &[PolicyAlertKind],
    policy_uid: &str,
    notifications: Option<&Notifications>,
    now: Time,
) -> LedgerOutcome {
    let renotify = notifications.map_or(0, |n| i64::from(n.renotify_after_seconds));
    let send_resolved = notifications.is_none_or(|n| n.send_resolved);

    let mut by_key: BTreeMap<String, AlertEntry> = existing
        .iter()
        .map(|e| (e.key.clone(), e.clone()))
        .collect();

    for kind in PolicyAlertKind::ALL {
        let key = dedup_key(policy_uid, kind);
        let wants_open = desired_open.contains(&kind);
        let current = by_key.get(&key).cloned();
        let next = match (current, wants_open) {
            // Nothing stored and nothing wanted: no entry at all. A ledger
            // full of never-fired alerts is noise in every `kubectl get -o
            // yaml` an operator runs during an incident.
            (None, false) => None,
            (None, true) => Some(AlertEntry {
                key: key.clone(),
                kind: kind.widen(),
                state: AlertState::Open.as_str().to_string(),
                opened_at: Some(now),
                resolved_at: None,
                transition: Some(1),
                notified_transition: None,
                delivery: None,
            }),
            (Some(entry), true) => {
                let open = entry.state == AlertState::Open.as_str();
                if !open {
                    // Re-opening after a resolve is a transition.
                    Some(AlertEntry {
                        state: AlertState::Open.as_str().to_string(),
                        opened_at: Some(now),
                        resolved_at: None,
                        transition: Some(entry.transition.unwrap_or(0) + 1),
                        ..entry
                    })
                } else if renotify > 0 && renotify_due(&entry, renotify, now) {
                    Some(AlertEntry {
                        transition: Some(entry.transition.unwrap_or(0) + 1),
                        ..entry
                    })
                } else {
                    // THE ARM THAT MATTERS: unchanged, so no transition, so
                    // no delivery, so no page.
                    Some(entry)
                }
            }
            (Some(entry), false) => {
                let open = entry.state == AlertState::Open.as_str();
                if open {
                    Some(AlertEntry {
                        state: AlertState::Resolved.as_str().to_string(),
                        resolved_at: Some(now),
                        transition: Some(entry.transition.unwrap_or(0) + 1),
                        ..entry
                    })
                } else {
                    Some(entry)
                }
            }
        };
        match next {
            Some(entry) => {
                by_key.insert(key, entry);
            }
            None => {
                by_key.remove(&key);
            }
        }
    }

    let mut alerts: Vec<AlertEntry> = by_key.into_values().collect();
    alerts.sort_by(|a, b| a.key.cmp(&b.key));
    alerts.truncate(MAX_ALERTS);

    let due: Vec<AlertEntry> = alerts
        .iter()
        .filter(|e| {
            let transition = e.transition.unwrap_or(0);
            if e.notified_transition.unwrap_or(0) >= transition && !retryable(e) {
                // Already delivered (or being delivered) for this transition,
                // and not a failure with attempts left. THIS IS THE ARM THAT
                // MATTERS: a condition that stays true produces no further
                // messages.
                return false;
            }
            // `sendResolved: false` silences the CLEARING of a page. A
            // `RecoveryCompleted` is not the clearing of anything — it is
            // news, recorded as `Resolved` because it auto-resolves the
            // instant it opens (D3 §3.3) — so reading its state as a resolve
            // would silence the one alert in the set that is good news.
            if e.state == AlertState::Resolved.as_str()
                && !send_resolved
                && e.kind != AlertKind::RecoveryCompleted
            {
                return false;
            }
            true
        })
        .cloned()
        .collect();

    LedgerOutcome { alerts, due }
}

/// One terminal `Restore` that recovered a point this policy protects.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryCompletion {
    /// The `Restore` object's name.
    pub restore_name: String,
    /// Its UID — the whole of the dedup key, because a policy's points are
    /// restored many times and each of those is its own completed recovery.
    pub restore_uid: String,
    /// `Succeeded` or `Failed`.
    pub phase: String,
    /// `pass`, `fail-integrity`, … when the run recorded one.
    pub outcome: Option<String>,
    /// When it finished.
    pub at: Option<Time>,
}

/// Fold terminal recoveries into the ledger — D3 §3.3's fifth kind.
///
/// # It opens and resolves in the same breath, and never pages
///
/// A `RecoveryCompleted` is informational. It is recorded as
/// [`AlertState::Resolved`] with `openedAt == resolvedAt` so nothing stays
/// open, nothing re-notifies, and the PagerDuty half of the delivery is a
/// no-op by construction ([`kind_pages`] is `false`). What it is FOR is the
/// incident-facing surface: an on-call responder wants to know a recovery
/// finished and which one, and D3 §3.5's counts and cutover sentences are
/// rendered by the console from the `Restore` this entry names.
///
/// **An entry that already exists is never rewritten.** The key is the Restore
/// UID, and a Restore reaches a terminal state once; re-opening would deliver
/// a second message about the same recovery on every reconcile.
#[must_use]
pub fn fold_recoveries(
    existing: &[AlertEntry],
    completions: &[RecoveryCompletion],
    now: Time,
) -> Vec<AlertEntry> {
    let mut out: Vec<AlertEntry> = existing.to_vec();
    for completion in completions {
        let key = recovery_completed_key(&completion.restore_uid);
        if out.iter().any(|e| e.key == key) {
            continue;
        }
        out.push(AlertEntry {
            key,
            kind: AlertKind::RecoveryCompleted,
            state: AlertState::Resolved.as_str().to_string(),
            opened_at: completion.at.or(Some(now)),
            resolved_at: completion.at.or(Some(now)),
            transition: Some(1),
            notified_transition: None,
            delivery: None,
        });
    }
    prune_recoveries(out)
}

/// Keep at most [`MAX_RECOVERY_ALERTS`] recovery entries.
///
/// The survivors are the newest by `openedAt`, and an entry with NO delivery
/// yet outranks a delivered one of the same age: a message nobody has heard is
/// the one still owed. Ties break on the key so the answer does not vary
/// between two passes over one ledger.
fn prune_recoveries(entries: Vec<AlertEntry>) -> Vec<AlertEntry> {
    let mut rank: Vec<(bool, Option<Time>, String)> = entries
        .iter()
        .filter(|e| e.kind == AlertKind::RecoveryCompleted)
        .map(|e| (e.notified_transition.is_none(), e.opened_at, e.key.clone()))
        .collect();
    if rank.len() <= MAX_RECOVERY_ALERTS {
        return entries;
    }
    rank.sort();
    rank.reverse();
    rank.truncate(MAX_RECOVERY_ALERTS);
    entries
        .into_iter()
        .filter(|e| {
            e.kind != AlertKind::RecoveryCompleted
                || rank.contains(&(e.notified_transition.is_none(), e.opened_at, e.key.clone()))
        })
        .collect()
}

/// Whether a delivery that FAILED for the current transition may be attempted
/// again.
///
/// The transition has not moved — the condition did not change — so this is a
/// RETRY of one message and not a second message. The caller still applies the
/// backoff ([`next_attempt_at`]); this only says the budget is not spent.
fn retryable(entry: &AlertEntry) -> bool {
    let Some(delivery) = entry.delivery.as_ref() else {
        return false;
    };
    DeliveryState::parse(delivery.state.as_deref().unwrap_or_default())
        == Some(DeliveryState::Failed)
        && delivery.attempts.unwrap_or(0) < MAX_DELIVERY_ATTEMPTS
}

/// Whether an open alert is due a re-notify.
///
/// Measured from the LAST DELIVERY ATTEMPT and not from `openedAt`, because
/// `transition` already moved for every previous re-notify and measuring from
/// the open instant would fire every pass once the first interval elapsed.
fn renotify_due(entry: &AlertEntry, renotify_seconds: i64, now: Time) -> bool {
    let last = entry
        .delivery
        .as_ref()
        .and_then(|d| d.last_attempt_at)
        .or(entry.opened_at);
    last.is_some_and(|at| (now - at).num_seconds() >= renotify_seconds)
}

/// Whether this alert should be delivered at all, given the policy's routes.
///
/// Four suppressions, each of which is a real operator choice and not a
/// failure:
///
/// * no `notifications` block, or no route;
/// * `notifications.kinds` is set and does not list this kind;
/// * a resolve while `sendResolved` is off;
/// * `RecoveryCompleted` with PagerDuty-only routes — D3 W4's recorded no-op,
///   which must NOT burn three delivery attempts and must NOT turn
///   `NotificationsDelivered` red.
#[must_use]
pub fn is_suppressed(
    notifications: Option<&Notifications>,
    kind: AlertKind,
    state: AlertState,
) -> bool {
    let Some(n) = notifications else {
        return true;
    };
    let routes = n.routes.as_deref().unwrap_or_default();
    if routes.is_empty() {
        return true;
    }
    if let Some(kinds) = n.kinds.as_deref() {
        if !kinds
            .iter()
            .any(|k| alert_kind_str(*k) == alert_kind_str(kind))
        {
            return true;
        }
    }
    if state == AlertState::Resolved && !n.send_resolved && kind != AlertKind::RecoveryCompleted {
        return true;
    }
    if !kind_pages(kind)
        && routes
            .iter()
            .all(|r| r.webhook.is_none() && r.slack.is_none())
    {
        return true;
    }
    false
}

/// What the delivery Job's exit code and `notify-result=` lines mean.
///
/// # D3 W4's contract, read exactly
///
/// | exit | lines | verdict |
/// |---|---|---|
/// | 0 | one `…:ok` per configured sink | [`DeliveryState::Delivered`] |
/// | 1 | `notify-result=none:unconfigured` | [`DeliveryState::Suppressed`] — nothing was configured for this kind, which for `RecoveryCompleted` with PagerDuty-only routes is BY DESIGN and is not retried |
/// | 1 | at least one `…:failed` | [`DeliveryState::Failed`], retried |
/// | 3 | none | the document was unreadable and nothing was posted: [`DeliveryState::Failed`], retried (the ConfigMap is immutable, so a retry re-reads the same bytes and fails the same way — three attempts and then a named, permanent `DeliveryFailed`) |
/// | other / absent | — | [`DeliveryState::Failed`] |
#[must_use]
pub fn classify_delivery(exit_code: Option<i32>, log_tail: &[&str]) -> (DeliveryState, String) {
    let results: Vec<&'static str> = log_tail
        .iter()
        .filter_map(|l| l.trim().strip_prefix(NOTIFY_RESULT_PREFIX))
        .filter_map(known_result)
        .collect();
    let unconfigured = results.contains(&"none:unconfigured");
    match exit_code {
        Some(0) => (
            DeliveryState::Delivered,
            format!("every configured sink accepted ({})", sinks(&results)),
        ),
        Some(1) if unconfigured => (
            DeliveryState::Suppressed,
            "no sink was configured for this alert kind; nothing was posted and nothing is \
             retried"
                .to_string(),
        ),
        Some(1) => (
            DeliveryState::Failed,
            format!("a configured sink did not accept ({})", sinks(&results)),
        ),
        Some(3) => (
            DeliveryState::Failed,
            "the delivery Job refused the event document and posted nothing".to_string(),
        ),
        Some(code) => (
            DeliveryState::Failed,
            format!("the delivery Job exited {code}"),
        ),
        None => (
            DeliveryState::Failed,
            "the delivery Job finished with no exit code".to_string(),
        ),
    }
}

fn sinks(results: &[&'static str]) -> String {
    if results.is_empty() {
        return "no notify-result line".to_string();
    }
    results.join(", ")
}

/// D3 W4's key line prefix — matched BY KEY NAME from a bounded tail, never by
/// position (erratum **E4**: a pod log is stdout and stderr merged in
/// nondeterministic order).
pub const NOTIFY_RESULT_PREFIX: &str = "notify-result=";

/// Every `notify-result=` value this build understands.
pub const NOTIFY_RESULTS: [&str; 7] = [
    "pagerduty:ok",
    "pagerduty:failed",
    "webhook:ok",
    "webhook:failed",
    "slack:ok",
    "slack:failed",
    "none:unconfigured",
];

/// A `notify-result=` value mapped to the `'static` spelling this build knows,
/// or `None`.
///
/// # THIS IS A CREDENTIAL BOUNDARY AND NOT A TIDINESS RULE
///
/// The value goes into `alerts[].delivery.lastError`, which lands in the
/// object's status, in the API's response and in the console. A pod log is
/// **adopter-influenced input** — a sink's error body, a runner's stderr, a
/// line a compromised image printed — so echoing the tail of a matched line
/// back into a CR status is how a routing key put on that line by anything at
/// all becomes a permanently stored, API-served secret. Returning a
/// `&'static str` from a closed table makes that unwritable: no byte of the
/// log can reach the status through this path, whatever the log says.
#[must_use]
pub fn known_result(value: &str) -> Option<&'static str> {
    let value = value.trim();
    NOTIFY_RESULTS.iter().copied().find(|k| *k == value)
}

/// Cap an error sentence at [`MAX_ERROR_CHARS`], on a character boundary.
#[must_use]
pub fn cap_error(message: &str) -> String {
    if message.chars().count() <= MAX_ERROR_CHARS {
        return message.to_string();
    }
    message
        .chars()
        .take(MAX_ERROR_CHARS - 1)
        .collect::<String>()
        + "…"
}

/// When the next attempt of a failed delivery may be made.
///
/// `None` once [`MAX_DELIVERY_ATTEMPTS`] have been made: exhaustion sets
/// `NotificationsDelivered=False/DeliveryFailed` **and nothing else** — no
/// Backup is touched, no phase moves, no evidence changes.
#[must_use]
pub fn next_attempt_at(delivery: &AlertDelivery) -> Option<Time> {
    let attempts = delivery.attempts.unwrap_or(0);
    if attempts >= MAX_DELIVERY_ATTEMPTS {
        return None;
    }
    let index = usize::try_from(attempts.max(1) - 1).unwrap_or(0);
    let backoff = DELIVERY_BACKOFF_SECONDS
        .get(index)
        .copied()
        .unwrap_or(DELIVERY_BACKOFF_SECONDS[DELIVERY_BACKOFF_SECONDS.len() - 1]);
    delivery
        .last_attempt_at
        .map(|at| at + chrono::Duration::seconds(backoff))
}

// ===========================================================================
// The event document
// ===========================================================================

/// `application/vnd.logweir.protection-event+json;version=1.0.0`.
pub const EVENT_MEDIA_TYPE: &str = "application/vnd.logweir.protection-event+json;version=1.0.0";

/// The document's `format_version`.
pub const EVENT_FORMAT_VERSION: &str = "1.0.0";

/// The `ConfigMap` key the delivery Job mounts the event at.
pub const EVENT_DATA_KEY: &str = "event.json";

/// Where the event volume is mounted in the delivery pod.
pub const EVENT_MOUNT_PATH: &str = "/event";

/// What one event says.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventFacts<'a> {
    /// The policy's namespace.
    pub namespace: &'a str,
    /// The policy's name.
    pub name: &'a str,
    /// The policy's UID.
    pub uid: &'a str,
    /// The alert key — the controller is the authority on this value.
    pub alert_key: &'a str,
    /// Which of the five kinds.
    pub kind: AlertKind,
    /// `trigger` on Open (and on re-notify), `resolve` on Resolved.
    pub open: bool,
    /// The transition this event is about.
    pub transition: i64,
    /// The health at the moment of the transition.
    pub health: Health,
    /// One sentence for a human.
    pub summary: &'a str,
    /// The newest available point, or `None` — and its absence is the fact.
    pub point: Option<&'a AvailablePointFacts>,
    /// Consecutive failed slots.
    pub consecutive_failed_runs: i64,
    /// Missed schedule slots.
    pub missed_slots: i64,
    /// How thoroughly the archive was checked. Never `complete`.
    pub scope: VerificationScope,
    /// When the controller generated the event.
    pub generated_at: Time,
}

/// `sha256:<hex>` of `policyUID|alertKey|transition` — the document's
/// `event_id`.
#[must_use]
pub fn event_id(policy_uid: &str, alert_key: &str, transition: i64) -> String {
    let joined = format!("{}|{alert_key}|{transition}", ident(policy_uid));
    logweir_core::ids::sha256_prefixed(joined.as_bytes())
}

/// Build D3 §3.4's event document.
///
/// # Why this is a `serde_json::Value` and not `logweir::notify::ProtectionEvent`
///
/// `weirkeeper` does not depend on `crates/logweir`, and it must not: that
/// crate links `logweir-evidence`, the SIGNER, and
/// `scripts/check-one-signer.sh` computes the set of crates that do. A
/// `[dev-dependencies]` edge would put the signer in this crate's test graph
/// and break guard **G-SIGN** for a type import. So the document is built here
/// and the two spellings are held together by a test that parses
/// `docs/formats/protection-event.md`'s own worked example and asserts this
/// builder emits exactly that field set — a rename on either side is a red
/// test, which is what the shared type would have bought.
///
/// The delivery Job parses with `deny_unknown_fields`, so an extra key here is
/// a refusal (exit 3) and not a silently dropped detail.
#[must_use]
pub fn event_document(facts: &EventFacts<'_>) -> Value {
    let mut doc = json!({
        "format_version": EVENT_FORMAT_VERSION,
        "event_id": event_id(facts.uid, facts.alert_key, facts.transition),
        "policy": {
            "namespace": facts.namespace,
            "name": facts.name,
            "uid": facts.uid,
        },
        "alert": {
            "key": facts.alert_key,
            "kind": alert_kind_str(facts.kind),
            "action": if facts.open { "trigger" } else { "resolve" },
            "transition": facts.transition.max(0),
        },
        "health": facts.health.as_str(),
        "summary": facts.summary,
        "consecutive_failed_runs": facts.consecutive_failed_runs.max(0),
        "missed_slots": facts.missed_slots.max(0),
        "verification_scope": facts.scope.as_str(),
        "details_route": details_route(facts.namespace, facts.name),
        "generated_at": rfc3339(facts.generated_at),
    });

    // `last_available_point` is emitted ONLY when every one of its four
    // required fields is present. The delivery Job's type requires all four,
    // so a partial block is a parse failure and a refused delivery; an absent
    // block is a documented, renderable "no available recovery point".
    if let Some(point) = facts.point {
        if let (Some(id), Some(at), Some(age)) = (
            point.point_id.as_deref(),
            point.recovery_point_at,
            point.age_seconds,
        ) {
            doc.as_object_mut()
                .expect("the event document is an object")
                .insert(
                    "last_available_point".to_string(),
                    json!({
                        "point_id": id,
                        "recovery_point_at": rfc3339(at),
                        "age_seconds": age,
                        "evidence": point.evidence.as_str(),
                    }),
                );
        }
    }
    doc
}

/// The UI fragment route an incident responder opens.
///
/// A FRAGMENT AND NOT AN ABSOLUTE URL: the controller does not know the
/// installation's external hostname, and inventing one puts a dead link in an
/// incident.
#[must_use]
pub fn details_route(namespace: &str, name: &str) -> String {
    format!("#/protection?ns={namespace}&name={name}")
}

/// RFC 3339 with a `Z` offset and second precision — what the delivery Job's
/// `chrono::DateTime<Utc>` parses and what every other timestamp in this
/// project's documents looks like.
#[must_use]
pub fn rfc3339(at: Time) -> String {
    at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

/// Every top-level field name [`event_document`] can emit, in order.
///
/// Exposed so the format test can compare this builder against the documented
/// example without reflecting over a `Value` twice.
pub const EVENT_FIELDS: [&str; 12] = [
    "format_version",
    "event_id",
    "policy",
    "alert",
    "health",
    "summary",
    "last_available_point",
    "consecutive_failed_runs",
    "missed_slots",
    "verification_scope",
    "details_route",
    "generated_at",
];

// ===========================================================================
// Status assembly
// ===========================================================================

/// [`AvailablePointFacts`] as the CRD's `status.lastAvailablePoint`.
#[must_use]
pub fn available_point_status(point: &AvailablePointFacts) -> AvailablePoint {
    AvailablePoint {
        point_id: point.point_id.clone(),
        backup_ref: point
            .backup_name
            .as_ref()
            .map(|name| LocalRef { name: name.clone() }),
        recovery_point_at: point.recovery_point_at,
        newest_record_at: point.newest_record_at,
        age_seconds: point.age_seconds,
        evidence: Some(point.evidence.as_str().to_string()),
        topics: if point.topics.is_empty() {
            None
        } else {
            Some(point.topics.clone())
        },
        topics_truncated: point.topics_truncated.then_some(true),
    }
}
