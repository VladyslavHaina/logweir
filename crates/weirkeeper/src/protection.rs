//! Protection freshness, the alert vocabulary and the alert ledger (decision
//! D3 §§3.2–3.3).
//!
//! # Pure: no `kube::Client` here
//!
//! Every verdict is a function of objects the caller already read and of one
//! `now`, so it can be tested without a route table, which answers whatever it
//! is asked. [`crate::controllers::protection_policy`] is the thin half: it
//! reads, calls [`evaluate`] and [`reconcile_alerts`], and patches `/status`.
//!
//! # Three things this module keeps apart
//!
//! 1. **Scheduling health and protection health.** `BackupSchedule`'s `Ready`
//!    says the cron is firing; [`Health`] says there is something to recover
//!    from. A green schedule with no recoverable point is the case this module
//!    exists for, so [`Verdict::schedules`] carries the scheduling facts beside
//!    the verdict, not inside it.
//! 2. **`Unknown` and healthy.** [`Freshness::Unknown`] (the catalog could not
//!    be read, the source is gone, this policy's own status is stale) becomes
//!    [`Health::Unknown`], whose `Protected` condition is `Unknown`: an
//!    evaluation that could not happen is neither a pass nor a failure.
//! 3. **Capture start and the newest record.**
//!    [`AvailablePointFacts::recovery_point_at`] is the capture start
//!    (`Backup.status.capture.startedAt`, the receipt's `started_at`);
//!    `newest_record_at` (`windowCovered.toMs - 1 ms`) sits beside it. An
//!    objective measured from the newest record makes an idle topic look stale
//!    forever; one measured from `finishedAt` under-reports the gap by the
//!    length of the run.

use std::collections::BTreeMap;

use crate::crds::protection_policy::{
    AlertEntry, AlertKind, AvailablePoint, LastAttempt, MissedSummary, ProtectionPolicySpec,
    RehearsalSummary, ScheduleSummary,
};
use crate::crds::{LocalRef, Time, TrustBasis as TrustBlock};
use crate::verification::ValidBasis;

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

/// The most `RecoveryCompleted` entries the ledger carries, leaving room inside
/// [`MAX_ALERTS`] for the four policy-keyed kinds twice over. A recovery entry
/// keys on a restore UID, so a busy namespace has no natural ceiling; the
/// oldest are dropped first.
pub const MAX_RECOVERY_ALERTS: usize = 8;

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
            /// The wire spelling — what lands in a status field.
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
    /// Is there a recovery point inside the objective (D3 §3.2).
    ///
    /// `Unknown` is not a third shade of bad: the question was not answerable
    /// (the catalog view expired, a referenced source or destination is gone,
    /// this policy's last evaluation is older than its interval). It is never
    /// rendered as healthy or as failed.
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
        /// A run this policy covers succeeded, and the controller could not
        /// read the facts that place its recovery point in time.
        ///
        /// `Backup.status.capture` is written only on a `Valid` verification
        /// verdict, and on a destination whose `evidenceRead` grant is
        /// `SecretKeys` or `WorkloadIdentity` the controller reaches no verdict
        /// (`NotAttempted`). The point exists; its receipt is unreadable here.
        /// Reading that as [`FreshnessReason::NoAvailablePoint`] would page as
        /// [`Health::Unprotected`] about an archive whose catalog entry reads
        /// `Available`/`Verified`; "evaluation impossible" is
        /// [`Health::Unknown`].
        PointFactsUnread => "PointFactsUnread",
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
    /// `Protected`'s `status`: `True` only for [`Health::Healthy`], `Unknown`
    /// for [`Health::Unknown`], `False` otherwise. A `False` on an evaluation
    /// that could not happen would read as "Logweir checked and you are not
    /// protected", a claim this controller did not make.
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
    /// The evidence verdict for a point, as `status.lastAvailablePoint.evidence`
    /// spells it.
    ///
    /// `ValidHistorical` is `result: Valid` with `trust.basis: Historical`: the
    /// key was valid when it signed and has since been retired, which is what
    /// rotation looks like and is a pass. `Untrusted` is a signature that
    /// verifies under a key this installation will not accept.
    ///
    /// `Invalid` is its own member, not folded into `NotAttempted`, because
    /// [`evidence_objective_met`] lets `NotAttempted` ("the controller could not
    /// look") defer to the catalog, and an invalid digest is a document the
    /// controller did read and found is not what it claims
    /// ([`crate::verification::VerificationVerdict::Invalid`]). Folded, a
    /// tampered archive could read as `Protected=True` behind an older catalog
    /// view.
    Evidence {
        Valid => "Valid",
        ValidHistorical => "ValidHistorical",
        Invalid => "Invalid",
        Untrusted => "Untrusted",
        NotAttempted => "NotAttempted",
    }
}

impl Evidence {
    /// Whether this verdict satisfies `objectives.requireVerifiedEvidence`:
    /// `Valid` and `ValidHistorical` only. `Untrusted` is not a pass even though
    /// the bytes verify; an objective that ignored the installation's answer
    /// would make `TrustPolicy` decorative.
    #[must_use]
    pub fn is_verified(self) -> bool {
        matches!(self, Self::Valid | Self::ValidHistorical)
    }

    /// Whether a verifier that read the document produced this verdict: every
    /// member but [`Evidence::NotAttempted`], the only one this module may read
    /// as "the controller could not look".
    #[must_use]
    pub fn was_reached(self) -> bool {
        !matches!(self, Self::NotAttempted)
    }

    /// Read a `Backup.status.evidence.verification` result and its `trust`
    /// block into this vocabulary.
    ///
    /// No result is [`Evidence::NotAttempted`], never a pass. A result this
    /// build does not know (possible after a rollback past a build that wrote a
    /// new one) is [`Evidence::Untrusted`]: not a panic, since a controller must
    /// not crash on a newer build's status, and not `NotAttempted`, the one
    /// verdict the catalog may answer for ([`evidence_objective_met`]).
    ///
    /// A `Valid` is read with its basis through [`ValidBasis`], the rule the
    /// controller's badge uses: a pass on `Current`, on `Historical`
    /// ([`Evidence::ValidHistorical`]) and on no `trust` block at all (D3 §12).
    /// `Unverified` ("nothing compared yet") reads as
    /// [`Evidence::NotAttempted`]; every other basis, including a missing or
    /// unknown one, is [`Evidence::Untrusted`].
    #[must_use]
    pub fn from_verification(result: Option<&str>, trust: Option<&TrustBlock>) -> Self {
        match result {
            Some("Valid") => match ValidBasis::of_block(trust) {
                ValidBasis::Absent | ValidBasis::Current => Self::Valid,
                ValidBasis::Historical => Self::ValidHistorical,
                ValidBasis::Unverified => Self::NotAttempted,
                ValidBasis::Refused => Self::Untrusted,
            },
            Some("Invalid") => Self::Invalid,
            Some("Untrusted") => Self::Untrusted,
            // `Pending` is not a verdict (D2 §3.9 step 3): an evidence-fetch
            // Job is still reading the document. As a refusal, every freshly
            // finished destination-backed run would page as `Unprotected` for
            // the seconds its fetch takes.
            None | Some("NotAttempted" | "Pending") => Self::NotAttempted,
            Some(_) => Self::Untrusted,
        }
    }
}

/// The four alert kinds keyed on the policy UID. A separate type from
/// [`AlertKind`] so that `RecoveryCompleted`, keyed on the restore UID, cannot
/// be handed to a policy-keyed builder: the mistake does not compile.
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

/// The prefix of every alert key in the ledger.
pub const DEDUP_PREFIX: &str = "logweir-protection-";

/// What a blank identifier collapses to. Not the empty string:
/// `logweir-protection--Staleness` would be one alert per kind across every
/// policy in the cluster.
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

/// One candidate recovery point, as the controller read it off a `Backup` and
/// (optionally) a catalog entry. A flat struct rather than a `&Backup`, so
/// [`evaluate`] is testable over a table and the catalog and Kubernetes paths
/// reach the same rule; the controller's `candidate_from_backup` builds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PointCandidate {
    /// The `Backup` object's name, while one exists.
    pub backup_name: Option<String>,
    /// D3 §5.1's identity, when it is derivable — see
    /// [`point_id_from_receipt_digest`].
    pub point_id: Option<String>,
    /// The runner-reported full receipt digest. This is the authoritative
    /// catalog join key; `point_id` is only its truncated display identity.
    pub receipt_sha256: Option<String>,
    /// The archive set this run wrote into: `status.execution.id`, or
    /// `status.backupId` for a run frozen before execution inputs existed.
    ///
    /// The second catalog join key: [`Self::point_id`] comes from the receipt
    /// digest, which may be absent, while this id is on `Backup.status` from
    /// the pre-Job patch and `CatalogEntry::backup_id` carries the same value
    /// (the runner is handed it as its backup id override). `None` for a
    /// `Backup` frozen by a controller that wrote neither field.
    pub backup_id: Option<String>,
    /// **Capture start.** `Backup.status.capture.startedAt`.
    pub recovery_point_at: Option<Time>,
    /// `windowCovered.toMs - 1 ms`, labelled separately and never used for the
    /// objective.
    pub newest_record_at: Option<Time>,
    /// `status.phase`.
    pub phase: Option<String>,
    /// `status.exitCode`.
    pub exit_code: Option<i32>,
    pub evidence: Evidence,
    /// The topics the point covers, in the order the run resolved them.
    pub topics: Vec<String>,
    /// The run selected `allUserTopics`, so its topic set is "whatever the
    /// principal could see" and is not enumerated on the object.
    ///
    /// It satisfies `topics ⊆ point topics`: a dynamic run's `spec.topics` is
    /// `[]`, and a literal subset test would make every dynamic installation
    /// `Unprotected`. Logweir can say the run asked for every user topic, not
    /// that the broker showed it every one, so
    /// `status.lastAvailablePoint.topics` is empty for such a point.
    pub covers_all_topics: bool,
    /// Whether the `Backup`'s archive/destination matches what this policy
    /// protects. Decided by the controller, which holds the resolved
    /// destination; carried as a boolean so the rule stays testable.
    pub destination_matches: bool,
    /// Whether the `Backup`'s `sourceRef` matches `spec.protects.sourceRef`.
    pub source_matches: bool,
}

impl PointCandidate {
    /// Whether the run itself succeeded: `phase: Succeeded` and `exitCode == 0`.
    /// A crashed Job reaches a terminal phase with no exit code
    /// (`controllers::backup::crash_terminal_state`), and nobody can say such a
    /// point completed.
    #[must_use]
    pub fn succeeded(&self) -> bool {
        self.phase.as_deref() == Some("Succeeded") && self.exit_code == Some(0)
    }
}

/// D3 §5.1's point identity, derived from the immutable receipt digest the
/// runner reported rather than from a catalog round trip:
/// `pointId = "lwp1-" + lowercase_hex(sha256(receipt_bytes))[0..32]`, and
/// `Backup.status.evidence.receiptSha256` is `sha256:<that hex>`.
///
/// A controller that fetches the receipt hashes those bytes and rejects a
/// mismatch, but never publishes its own hash as the run's claim, so a
/// full-digest catalog join is evidence that both sources name the same point.
/// `None` for a digest that is not `sha256:` + 64 hex characters.
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
/// Deliberately not `catalog_view::ViewEntry`: this type reads `entries.jsonl`
/// with serde's default leniency (unknown fields ignored), so a catalog written
/// by a newer controller still answers "is the point there?", and protection
/// health does not depend on the catalog writer's field set.
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CatalogEntry {
    /// D3 §5.1's identity.
    pub point_id: String,
    /// Full immutable receipt identity, used to disambiguate equal truncated
    /// point ids. Older views omit it and cannot answer a digest-bound join.
    #[serde(default)]
    pub receipt_sha256: String,
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
    ///
    /// An `Option`: `#[serde(default)]` on a `bool` would read a writer that
    /// does not emit the field as `false` and make every point unselectable.
    /// `None` falls back to the two axes; `Some(false)` is a real refusal.
    #[serde(default)]
    pub selectable: Option<bool>,
}

impl CatalogEntry {
    /// The verification verdicts D3 §5.4 calls selectable.
    pub const VERIFIED: [&'static str; 2] = ["Verified", "VerifiedHistorical"];

    /// Whether this point may be counted as protection: both of D3 §5.4's axes,
    /// not availability alone. `Available` but `UntrustedSigner` is bytes this
    /// installation will not accept, and selecting it would report `Healthy`
    /// and suppress `ArchiveUnavailable`. `selectable` decides where present;
    /// otherwise the two axes are read here.
    #[must_use]
    pub fn is_available(&self) -> bool {
        if self.has_blank_axis() {
            // Neither "available" nor "gone": UNANSWERED. The caller turns this
            // into `CatalogUnreadable`; see `blank_axis` on the answer type.
            return false;
        }
        match self.selectable {
            Some(selectable) => selectable,
            None => {
                self.availability == "Available"
                    && Self::VERIFIED.contains(&self.verification.as_str())
            }
        }
    }

    /// Whether this entry's verification axis is one D3 §5.4 calls selectable:
    /// the catalog's answer to `requireVerifiedEvidence`, from the controller
    /// the credential model gives the key to, which read the receipt and checked
    /// it against this installation's trust policy.
    #[must_use]
    pub fn is_verified(&self) -> bool {
        Self::VERIFIED.contains(&self.verification.as_str())
    }

    /// Whether either axis came through empty.
    ///
    /// A blank axis is "could not answer", never "your backups are gone":
    /// `#[serde(default)]` turns a renamed field in `catalog_view::ViewEntry`
    /// into `""` for every entry, and reading that as "not available" would put
    /// every catalog-backed policy into `Unprotected` on a schema change. It is
    /// [`FreshnessReason::CatalogUnreadable`], which is `Unknown`. The two
    /// spellings agree today (both serialise the PascalCase strings compared
    /// here), but no fixture holds them together.
    #[must_use]
    pub fn has_blank_axis(&self) -> bool {
        self.availability.is_empty() || self.verification.is_empty()
    }

    /// Whether this entry is one of D3 §3.3's `ArchiveUnavailable` triggers on
    /// either axis ("`Missing`/`Unreadable`/`Untrusted` in the catalog"): the
    /// bytes are gone or unreadable, or the signature is not one this
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

impl CatalogAnswer {
    /// Whether a fresh view answered with an entry whose axes this build could
    /// not read. The caller turns this into
    /// [`FreshnessReason::CatalogUnreadable`] and [`Health::Unknown`], rather
    /// than report a whole cluster `Unprotected` because a field was renamed.
    #[must_use]
    pub fn blank_axis(&self) -> bool {
        match self {
            Self::Fresh(entries) => entries.iter().any(CatalogEntry::has_blank_axis),
            Self::NotConsulted | Self::Stale(_) => false,
        }
    }
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
    /// `status.missedSlots`, when the schedule carries it, read defensively out
    /// of the serialized status.
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

/// The rehearsal side of protection.
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
    pub spec: &'a ProtectionPolicySpec,
    /// Whether `spec.protects.sourceRef` resolved.
    pub source_exists: bool,
    /// Whether `spec.protects.destinationRef` resolved. `true` when the policy
    /// names a `legacyArchive` instead.
    pub destination_exists: bool,
    /// Every candidate point, in any order.
    pub candidates: &'a [PointCandidate],
    pub catalog: &'a CatalogAnswer,
    /// The schedules, in `spec.protects.scheduleRefs` order.
    pub schedules: &'a [ScheduleFacts],
    /// The slot history, newest first, already de-duplicated per slot by the
    /// caller's list order.
    pub slots: &'a [SlotRun],
    pub rehearsal: &'a RehearsalFacts,
    pub now: Time,
}

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
    pub rehearsal: Option<RehearsalSummary>,
    /// Whether any schedule missed a slot since it last fired: the live signal,
    /// as opposed to `missed.lastMissedSlot`'s sticky audit trail. `AtRisk`
    /// reads it.
    pub missed_since_last_fire: bool,
    /// Which alerts D3 §3.3 says should be OPEN right now.
    pub open_kinds: Vec<PolicyAlertKind>,
    /// One sentence for a human, with the point's concrete age. No surface
    /// carries it since alert delivery was withdrawn; the conditions carry
    /// [`Verdict::condition_summary`].
    pub summary: String,
    /// The same verdict without the clock-derived number: what the `Ready` and
    /// `Protected` condition messages carry. A message embedding `humanize(age)`
    /// changes every minute, so a pass where nothing changed would PATCH, bump
    /// `resourceVersion` and wake the reconciler with its own write.
    pub condition_summary: String,
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
    if !matches_policy(candidate, spec) {
        return false;
    }
    match catalog {
        CatalogAnswer::NotConsulted => evidence_objective_met(candidate, spec, None),
        CatalogAnswer::Stale(_) => false,
        // A point the catalog has never heard of is not available when the
        // operator asked for catalog availability: the whole point of the
        // objective is that the bytes were confirmed to be there.
        CatalogAnswer::Fresh(entries) => entries_for(candidate, entries)
            .any(|e| e.is_available() && evidence_objective_met(candidate, spec, Some(e))),
    }
}

/// `objectives.requireVerifiedEvidence`, asked of whichever component actually
/// read the signature.
///
/// On a destination whose `evidenceRead` grant is `SecretKeys` the controller
/// reaches no verdict ([`Evidence::NotAttempted`]), while the catalog
/// controller, which holds the key, has verified the same receipt. So the
/// catalog row may answer, but only for `NotAttempted`: a verdict the
/// controller did reach still decides ([`Evidence::was_reached`]). Letting a
/// catalog row overrule [`Evidence::Untrusted`] or [`Evidence::Invalid`] would
/// make `TrustPolicy` decorative and a tampered archive `Healthy` for as long
/// as an older catalog view is served (up to an hour).
#[must_use]
fn evidence_objective_met(
    candidate: &PointCandidate,
    spec: &ProtectionPolicySpec,
    entry: Option<&CatalogEntry>,
) -> bool {
    if !spec.objectives.require_verified_evidence || candidate.evidence.is_verified() {
        return true;
    }
    !candidate.evidence.was_reached() && entry.is_some_and(CatalogEntry::is_verified)
}

/// The entries in a fresh view that are about this candidate: by the full
/// receipt digest where the candidate has one (and the point id with it), and
/// by the archive set id only where it has no digest.
///
/// The digest is absent for older runners and for malformed or ambiguous
/// digest output; the archive set id is on the object from the pre-Job patch
/// and on the catalog row under `backupId`. Where the candidate has a digest it
/// decides: a candidate whose identity the view does not list is one the view
/// does not know, not one to look up under a coarser name.
fn entries_for<'e>(
    candidate: &PointCandidate,
    entries: &'e [CatalogEntry],
) -> impl Iterator<Item = &'e CatalogEntry> {
    let point_id = candidate.point_id.clone();
    let receipt_sha256 = candidate.receipt_sha256.clone();
    // An empty `backupId` is not a key: `#[serde(default)]` gives `""` to a
    // view that does not write the field, which would match every candidate.
    let backup_id = candidate.backup_id.clone().filter(|id| !id.is_empty());
    entries.iter().filter(move |e| match &receipt_sha256 {
        // The full digest decides. The point id is checked too when present,
        // but can never substitute for the collision-resistant identity.
        Some(digest) => {
            &e.receipt_sha256 == digest && point_id.as_ref().is_none_or(|id| &e.point_id == id)
        }
        // Compatibility fallback is allowed only when the candidate has no
        // canonical runner digest at all.
        None => backup_id.as_ref().is_some_and(|id| &e.backup_id == id),
    })
}

/// The one entry a fresh view holds for this candidate, for reading facts off;
/// `None` when the view holds none, or more than one.
///
/// Where the join ran on the archive set id, two entries are two points, and
/// reading either's capture time would put a measurement-shaped number on the
/// wrong point. A `Backup` writes one point, so this is a defensive `None`,
/// which the caller reports as [`FreshnessReason::PointFactsUnread`].
/// Availability does not go through here ([`is_available`] asks whether any
/// matching entry is available).
#[must_use]
pub fn catalog_entry_for<'e>(
    candidate: &PointCandidate,
    catalog: &'e CatalogAnswer,
) -> Option<&'e CatalogEntry> {
    let CatalogAnswer::Fresh(entries) = catalog else {
        return None;
    };
    let mut matched = entries_for(candidate, entries);
    let only = matched.next()?;
    matched.next().is_none().then_some(only)
}

/// The run half of D3 §3.2's availability rule: everything decidable from the
/// `Backup` object alone. [`is_available`] adds the catalog conjunct and the
/// evidence objective; [`is_unplaceable`] adds only the verdict test its claim
/// depends on.
#[must_use]
pub fn matches_policy(candidate: &PointCandidate, spec: &ProtectionPolicySpec) -> bool {
    candidate.succeeded()
        && candidate.source_matches
        && candidate.destination_matches
        && (candidate.covers_all_topics || topics_covered(spec, &candidate.topics))
}

/// Whether this candidate is a point the policy covers that the controller
/// could not place in time.
///
/// A point with no capture start cannot be aged, so it cannot satisfy an
/// objective, but it is not evidence that the policy has nothing either (see
/// [`FreshnessReason::PointFactsUnread`]). Only `Evidence::NotAttempted`
/// qualifies: the evidence objective is not applied (the claim is that the
/// receipt could not be read), but the verdict is. `Backup.status.capture` is
/// written only for a `Valid` verdict, so an `Untrusted` or `Invalid` point has
/// no capture time either, and reading it as unplaceable would turn "Logweir
/// refuses this signature" into a silent `Unknown`; D3 §3.2 gives it
/// [`Health::Unprotected`].
#[must_use]
pub fn is_unplaceable(candidate: &PointCandidate, spec: &ProtectionPolicySpec) -> bool {
    candidate.recovery_point_at.is_none()
        && !candidate.evidence.was_reached()
        && matches_policy(candidate, spec)
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

/// D3 §3.2's failure accounting: consecutive failed slots, newest first.
///
/// The walk stops at a running slot: it has not failed, and counting past it
/// would report a streak a run in flight may be about to end. A retry chain
/// (`trigger.kind=Retry`, `attempt`, `retryOf`) is one slot: only its final
/// attempt, the first seen newest-first, votes, so three retries of one nightly
/// slot are one failed slot, which is what a threshold of 2 means.
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

    // Freshness, then health: health is freshness plus the risk signals, and
    // computing them together is how `Unknown` acquires a `Healthy` arm.
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
    } else if catalog.blank_axis() {
        // An entry whose axes this build cannot read is a view that could not
        // answer, not a point that is gone.
        Some(FreshnessReason::CatalogUnreadable)
    } else if newest.is_none() && input.candidates.iter().any(|c| is_unplaceable(c, spec)) {
        // A covered run succeeded and its point could not be placed in time:
        // "evaluation impossible" (`Unknown`), not "no available point", which
        // pages. Last in the chain, because every reason above names an object
        // the operator can look at; gated on `newest.is_none()`, because a
        // policy that found a point has a verdict.
        Some(FreshnessReason::PointFactsUnread)
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

    // D3 §3.2's `Healthy` row says "no missed slot since last fire"; see
    // `missed_since_last_fire`.
    let missed_recently = input.schedules.iter().any(missed_since_last_fire);
    let at_risk = consecutive_failed_runs >= i64::from(spec.objectives.max_consecutive_failed_runs)
        && spec.objectives.max_consecutive_failed_runs > 0
        || input.schedules.iter().any(|s| s.suspended)
        || input
            .schedules
            .iter()
            .any(|s| s.ready.as_deref() == Some("False"))
        || missed_recently;

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
        true,
    );
    let condition_summary = summarize(
        spec,
        health,
        reason,
        point.as_ref(),
        consecutive_failed_runs,
        false,
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
        missed_since_last_fire: missed_recently,
        open_kinds,
        summary,
        condition_summary,
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

/// The UTC instant a `yyyymmdd-hhmmss` slot name denotes, or `None`.
///
/// The same grammar `identity::valid_slot` checks, read rather than validated:
/// a name this build cannot parse is not evidence that a slot was missed
/// recently, so it answers `None` and the caller treats it as "not recent".
#[must_use]
pub fn slot_instant(slot: &str) -> Option<Time> {
    chrono::NaiveDateTime::parse_from_str(slot, "%Y%m%d-%H%M%S")
        .ok()
        .map(|naive| naive.and_utc())
}

/// Whether this schedule missed a slot since it last fired (D3 §3.2's
/// `Healthy` row).
///
/// `BackupSchedule.status.lastMissedSlot` is an audit trail, never cleared, so
/// read as a live signal it would pin a policy to `AtRisk` (and
/// `Protected=False`) for the life of the schedule. A slot name is a UTC
/// instant, so the question is whether it is newer than the last fire. A
/// `status.missedSlots.count` of zero is an explicit "none" and wins; a
/// non-zero count covers the schedule's whole life, so the instant decides.
#[must_use]
pub fn missed_since_last_fire(facts: &ScheduleFacts) -> bool {
    if facts.missed_slots == Some(0) {
        return false;
    }
    let Some(slot) = facts.last_missed_slot.as_deref() else {
        return false;
    };
    let Some(missed_at) = slot_instant(slot) else {
        return false;
    };
    match facts.last_fire_time {
        // Never fired: every recorded miss is still outstanding.
        None => true,
        Some(fired) => missed_at > fired,
    }
}

fn missed_summary(input: &Inputs<'_>) -> MissedSummary {
    // The newest missed slot across the schedules (`yyyymmdd-hhmmss`, so
    // lexical order is chronological), reported whether or not it is recent:
    // it is the audit trail, and only `missed_since_last_fire` decides
    // `AtRisk`.
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
    // `Staleness` covers `Unprotected` too: with nothing to recover from, a
    // policy whose only point was deleted would otherwise page nobody
    // (`BackupFailure` needs failed runs, and a policy with no runs has none).
    // The same kind rather than a sixth, because `Staleness` resolves on
    // `health` back to `Healthy`/`AtRisk`, which already treats `Unprotected`
    // as unresolved; a separate kind would open a second incident for one
    // objective.
    if matches!(health, Health::Stale | Health::Unprotected) {
        open.push(PolicyAlertKind::Staleness);
    }

    // `ArchiveUnavailable`: a point this policy would otherwise count is
    // degraded in the catalog on either axis. Two ways in: the newest
    // otherwise-available point (the one Logweir would choose if the bytes
    // were there), and the point actually chosen, which can still be degraded
    // on an axis selection tolerates (a signer retired or revoked under a
    // point the `Backup` still calls `Valid`).
    if let CatalogAnswer::Fresh(entries) = catalog {
        // The matching rule `is_available` uses (`entries_for`), not one keyed
        // on `point_id` alone, which a point with an unread receipt lacks.
        let degraded = |candidate: &PointCandidate| {
            entries_for(candidate, entries).any(CatalogEntry::is_degraded)
        };
        // The evidence objective asked as it would be answered if the bytes
        // were there: a `NotAttempted` candidate belongs, because a sound row
        // would carry `Verified` (`evidence_objective_met`). An `Untrusted`
        // verdict the controller did reach still excludes the point.
        let mut otherwise: Vec<&PointCandidate> = input
            .candidates
            .iter()
            .filter(|c| {
                c.recovery_point_at.is_some()
                    && matches_policy(c, spec)
                    && (!spec.objectives.require_verified_evidence
                        || c.evidence.is_verified()
                        || !c.evidence.was_reached())
            })
            .collect();
        otherwise.sort_by_key(|c| std::cmp::Reverse(c.recovery_point_at));
        let top_degraded = otherwise.first().is_some_and(|top| degraded(top));
        let chosen_degraded = newest.is_some_and(degraded);
        if top_degraded || chosen_degraded {
            open.push(PolicyAlertKind::ArchiveUnavailable);
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

/// The controller-authored sentence about a verdict. It says what Logweir did
/// and never claims an exhaustive check (`no_summary_claims_an_exhaustive_check`
/// covers every arm).
///
/// `with_age: true` embeds the concrete age; `false` drops it for a condition
/// message, because a message that changes when the minute rolls over is a
/// status patch on a pass where nothing happened. The objective is a spec value
/// and stays in both.
#[must_use]
pub fn summarize(
    spec: &ProtectionPolicySpec,
    health: Health,
    reason: FreshnessReason,
    point: Option<&AvailablePointFacts>,
    consecutive_failed_runs: i64,
    with_age: bool,
) -> String {
    let objective = i64::from(spec.objectives.max_recovery_point_age_seconds);
    let age = point.and_then(|p| p.age_seconds).filter(|_| with_age);
    match health {
        Health::Healthy => match age {
            Some(age) => format!(
                "the newest available recovery point is {} old, inside the objective of {}",
                humanize(age),
                humanize(objective)
            ),
            None => format!(
                "the newest available recovery point is inside the objective of {}",
                humanize(objective)
            ),
        },
        Health::AtRisk => format!(
            "a recovery point is inside the objective, but protection is at risk: \
             {consecutive_failed_runs} consecutive failed slots, a suspended or not-ready \
             schedule, or a slot missed since the last fire"
        ),
        Health::Stale => match age {
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
        // Its own sentence, because the generic one sends an operator looking
        // for a missing object when every object is present. What is missing
        // is a read: no verification verdict, so no capture time to age.
        Health::Unknown if reason == FreshnessReason::PointFactsUnread => format!(
            "a run for this policy succeeded, but its recovery point could not be placed in \
             time: the controller read no verification verdict for it and so holds no capture \
             time, and an age cannot be compared to the objective of {}. This is not a pass \
             and not a failure",
            humanize(objective)
        ),
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

/// D3 §3.3's ledger, as one pure function over the stored one: the new ledger,
/// capped at [`MAX_ALERTS`], in a stable order.
///
/// A condition that stays true produces no further transitions until it
/// resolves. `transition` is the counter, kept in the object's status so it
/// survives a restart. `desired_open` is [`Verdict::open_kinds`]; entries for
/// kinds no longer open are resolved, not deleted.
///
/// A resolve is a claim, and only a health [`resolves_alerts`] accepts may make
/// it: "not in `open_kinds`" also happens when protection got worse (`Stale` →
/// `Unprotected`, the last point gone) or unmeasurable (`Stale` → `Unknown`).
/// Under any other health an open entry is left exactly as it is.
#[must_use]
pub fn reconcile_alerts(
    existing: &[AlertEntry],
    desired_open: &[PolicyAlertKind],
    health: Health,
    policy_uid: &str,
    now: Time,
) -> Vec<AlertEntry> {
    let may_resolve = resolves_alerts(health);

    let mut by_key: BTreeMap<String, AlertEntry> = existing
        .iter()
        .map(|e| (e.key.clone(), e.clone()))
        .collect();

    for kind in PolicyAlertKind::ALL {
        let key = dedup_key(policy_uid, kind);
        let wants_open = desired_open.contains(&kind);
        let current = by_key.get(&key).cloned();
        let next = match (current, wants_open) {
            // Nothing stored and nothing wanted: no entry. A ledger of
            // never-fired alerts is noise during an incident.
            (None, false) => None,
            (None, true) => Some(AlertEntry {
                key: key.clone(),
                kind: kind.widen(),
                state: AlertState::Open.as_str().to_string(),
                opened_at: Some(now),
                resolved_at: None,
                transition: Some(1),
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
                } else {
                    // Unchanged, so no transition.
                    Some(entry)
                }
            }
            (Some(entry), false) => {
                let open = entry.state == AlertState::Open.as_str();
                if open && may_resolve {
                    Some(AlertEntry {
                        state: AlertState::Resolved.as_str().to_string(),
                        resolved_at: Some(now),
                        transition: Some(entry.transition.unwrap_or(0) + 1),
                        ..entry
                    })
                } else {
                    // Open and left alone: protection got worse or became
                    // unmeasurable, which is not the condition clearing.
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
    alerts
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

/// Fold terminal recoveries into the ledger (D3 §3.3's fifth kind).
///
/// A `RecoveryCompleted` is informational: recorded as [`AlertState::Resolved`]
/// with `openedAt == resolvedAt`, so nothing stays open, and an on-call
/// responder can see which recovery finished. An existing entry is never
/// rewritten: the key is the Restore UID, and a Restore ends once.
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
        });
    }
    prune_recoveries(out)
}

/// Keep at most [`MAX_RECOVERY_ALERTS`] recovery entries: the newest by
/// `openedAt`. Ties break on the key so the answer does not vary between two
/// passes over one ledger.
fn prune_recoveries(entries: Vec<AlertEntry>) -> Vec<AlertEntry> {
    let mut rank: Vec<(Option<Time>, String)> = entries
        .iter()
        .filter(|e| e.kind == AlertKind::RecoveryCompleted)
        .map(|e| (e.opened_at, e.key.clone()))
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
            e.kind != AlertKind::RecoveryCompleted || rank.contains(&(e.opened_at, e.key.clone()))
        })
        .collect()
}

/// Whether this health may close an open alert: D3 §3.3's resolve column,
/// "`health` back to `Healthy`/`AtRisk`", and not "anything but `Stale`" (see
/// [`reconcile_alerts`]).
#[must_use]
pub fn resolves_alerts(health: Health) -> bool {
    matches!(health, Health::Healthy | Health::AtRisk)
}

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
