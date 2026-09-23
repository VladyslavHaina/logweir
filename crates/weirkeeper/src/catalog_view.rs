//! The bounded Kubernetes view of a durable recovery catalog — D3 §5.3, §5.4.
//!
//! # What lives here and why it is pure
//!
//! Everything in this module is a function of bytes and values: the grammar of
//! the `catalogSync` result body, the two classification axes, the trust
//! re-evaluation, the page and fence-pointer materialisation, the ten status
//! counters and the names of every object the controller creates. Nothing here
//! reads a clock, a socket or an API server —
//! [`crate::controllers::recovery_catalog`] does that and hands the bytes over.
//!
//! That split is the reason a whole view can be asserted over a table instead
//! of over a route table: "5 001 points page as 2 ConfigMaps, the newest 5 000
//! are materialised and `truncated` is true" is a property of arithmetic, and a
//! test that had to stand up a fake Job to state it would be testing the fake.
//!
//! # The durable truth is in object storage, and this is a window onto it
//!
//! D3 §5.3: the catalog itself is `logweir/catalog/v1/` in the destination.
//! Kubernetes carries the **newest ≤ 5 000 points**, the counts, the histogram
//! and the signer summary, in immutable `ConfigMap` pages **owned by the sync
//! Job**. When the Job's TTL fires, Kubernetes garbage collection takes the
//! pages with it and the catalog reports `ViewExpired` — which is honest, and
//! is why **this decision adds no `delete` verb anywhere**.
//!
//! # Two axes, and nothing merges them
//!
//! [`Availability`] answers *can these bytes be read*, [`Verification`] answers
//! *does the evidence verify under trusted key material*. A point is selectable
//! for an ordinary restore only when it is `Available` **and**
//! (`Verified` | `VerifiedHistorical`); everything else is listed with its exact
//! state. A single "healthy" boolean over the two is precisely how a console
//! comes to offer recovery from an archive whose manifest does not parse.
//!
//! # Who decides what
//!
//! The sync Job reads the archive, so it decides [`Availability`] and the
//! **signature** verdict ([`SignatureVerdict`]) — did these bytes verify under
//! the key material this controller mounted for it. It does NOT decide trust:
//! whether a key is one this installation accepts, whether it is retired or
//! revoked, is a question about the trust source and the controller is what
//! holds that. [`classify_verification`] is the one place the two meet, and
//! [`TrustView`] is the trust bound to the catalog's namespace, projected by
//! [`TrustView::from_resolution`] from [`crate::trust::resolve`] — the same
//! resolution the `Approval` controller, the restore preflight and the runner's
//! keyring are built from, with the synthesised `legacy-roster-v1` as the
//! fallback when no `TrustPolicy` governs the namespace.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, TimeZone as _, Utc};
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::ConfigMap;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kube::api::ObjectMeta;
use kube::ResourceExt as _;
use logweir_core::check_contract::CheckPlanKind;
use serde::{Deserialize, Serialize};

use crate::crds::recovery_catalog::{
    CatalogCounts, CatalogPage, DeepCheck, HistogramBucket, SignerSummary, SyncMode, SyncSettings,
};
use crate::crds::{trust_roster::TrustRosterSpec, Time};
use crate::job::{ConfigMapMount, EnvFromSecret, RunnerJobSpec, RunnerOwner};
use crate::verification::ValidBasis;

// ===========================================================================
// The plan kind, and the one thing this module could not take from D2 yet
// ===========================================================================

/// The plan kind a `RecoveryCatalog` sync runs — D-SEAMS **S1**.
///
/// **THE SEAM IS CLOSED.** This was a hand-written `"catalogSync"` for as long
/// as D2's [`CheckPlanKind`] was a closed FIVE-member vocabulary that did not
/// name it, guarded by a test that failed the day the kind landed. It has
/// landed: the constant is now DERIVED from the enum, so the two cannot drift,
/// and it is kept as a constant only because a `&'static str` in a JSON key
/// position reads better than a method call. Everything else about the Job —
/// the argv, the mount path, the three pinned environment variables, the
/// deadline margin, the labels, `automountServiceAccountToken: false`, the plan
/// `ConfigMap` and its 409 rule — is taken from [`crate::check`] and is not
/// re-decided here.
///
/// [`CheckPlanKind`]: logweir_core::check_contract::CheckPlanKind
pub const PLAN_KIND: &str = CheckPlanKind::CatalogSync.as_str();

/// The two-letter Job-name discriminator for [`PLAN_KIND`], in the shape
/// [`logweir_core::check_contract::CheckPlanKind::job_discriminator`] gives the
/// other five (`td`, `rd`, `rp`, `da`, `ev`).
///
/// `cs`, and DERIVED, for the reason [`PLAN_KIND`] gives.
pub const JOB_DISCRIMINATOR: &str = CheckPlanKind::CatalogSync.job_discriminator();

/// The contract string a check plan carries, re-exported so this module names
/// D2's spelling and never a second one.
pub use logweir_core::check_contract::CHECK_PLAN_CONTRACT;

// ===========================================================================
// The two axes
// ===========================================================================

macro_rules! view_vocabulary {
    ($(#[$meta:meta])* $name:ident { $($variant:ident => $text:literal),+ $(,)? }) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub enum $name {
            $($variant),+
        }

        impl $name {
            /// The wire spelling, as it appears in a page entry and in a status
            /// field.
            #[must_use]
            pub fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $text),+
                }
            }

            /// Every member, in declaration order.
            pub const ALL: &'static [Self] = &[$(Self::$variant),+];

            /// The wire spelling back to a member.
            ///
            /// `None` for anything not in the table, and never a fallback
            /// member: "this build does not know that state" and "this specific
            /// state happened" are different facts, and collapsing them is how
            /// a newer runner's `Partial` would be read as `Available`.
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

        impl Serialize for $name {
            fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                s.serialize_str(self.as_str())
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                let s = String::deserialize(d)?;
                Self::parse(&s).ok_or_else(|| {
                    serde::de::Error::custom(format!(
                        concat!("`{}` is not a ", stringify!($name)),
                        s
                    ))
                })
            }
        }
    };
}

view_vocabulary! {
    /// Can these bytes be read — D3 §5.4's first table.
    ///
    /// `Missing` and `Unreadable` are DIFFERENT ANSWERS and the difference is
    /// the whole reason there are seven of these: a `NotFound` is "the archive
    /// does not hold this", a 403 or a timeout is "this credential could not
    /// tell", and reporting the second as the first is how an operator comes to
    /// believe an outage deleted their backups.
    Availability {
        Available => "Available",
        Missing => "Missing",
        Unreadable => "Unreadable",
        Deleted => "Deleted",
        Conflict => "Conflict",
        UnsupportedFormat => "UnsupportedFormat",
        Partial => "Partial",
    }
}

impl Availability {
    /// Whether a point in this state may be offered for an ordinary restore —
    /// the first half of D3 §5.4's selection rule.
    #[must_use]
    pub fn selectable(self) -> bool {
        self == Self::Available
    }
}

view_vocabulary! {
    /// What the sync Job could say about a signature, and nothing about trust.
    ///
    /// The Job holds the key material this controller mounted and the bytes it
    /// read; it can say "this DSSE verifies under key K" or "it does not". It
    /// cannot say whether K is a key this installation accepts, is retired, or
    /// was revoked for compromise — those are facts about the trust source,
    /// which lives in the cluster and not in the archive.
    SignatureVerdict {
        Verified => "verified",
        Invalid => "invalid",
        NoEvidence => "noEvidence",
        NotAttempted => "notAttempted",
    }
}

view_vocabulary! {
    /// Does the evidence verify under trusted key material — D3 §5.4's second
    /// table. Produced by [`classify_verification`] and by nothing else.
    Verification {
        Verified => "Verified",
        VerifiedHistorical => "VerifiedHistorical",
        UntrustedSigner => "UntrustedSigner",
        Revoked => "Revoked",
        Invalid => "Invalid",
        NoEvidence => "NoEvidence",
        NotAttempted => "NotAttempted",
    }
}

impl Verification {
    /// Whether a point in this state may be offered for an ordinary restore —
    /// the second half of D3 §5.4's selection rule.
    #[must_use]
    pub fn selectable(self) -> bool {
        matches!(self, Self::Verified | Self::VerifiedHistorical)
    }
}

/// D3 §5.4's selection rule, in one function so no surface writes its own.
#[must_use]
pub fn selectable(availability: Availability, verification: Verification) -> bool {
    availability.selectable() && verification.selectable()
}

// ===========================================================================
// The controller's own verdict outranks a view row
// ===========================================================================

/// Whether a `Backup.status.evidence.verification.result` is a verdict the
/// controller REACHED that is not a pass.
///
/// **The catalog decides only where the controller could not look.** `None`
/// (no verdict written), `NotAttempted` and `Pending` (the evidence-fetch Job is
/// still reading the document — `claude/evidence-fetch`; not a verdict at all)
/// are "I have not looked" and defer to the catalog; `Valid` is a pass the
/// catalog may still narrow. Everything else
/// — `Invalid`, `Untrusted`, or a spelling this build does not know (reachable
/// after a rollback past a build that wrote a fifth verdict) — is a refusal no
/// catalog row may overrule. The same rule as
/// `protection::evidence_objective_met` and
/// `controllers::rehearsal_schedule::candidate_from_backup`.
///
/// # `Valid` is a pass only on a basis [`ValidBasis`] admits (TRUST-VALID-BASIS-CLASS)
///
/// `basis` is the verdict's `trust` block, read by the one rule the
/// controller's badge uses. A `Valid` on `Unverified` has compared nothing yet
/// and defers like `NotAttempted`, which is what the badge calls it; a `Valid`
/// on `RecordedBeforeRevocation`, `None`, a missing basis or an unknown word is
/// a verdict this installation does not accept — a refusal. `basis` is not
/// read for any other result.
#[must_use]
pub fn is_reached_refusal(result: Option<&str>, basis: ValidBasis) -> bool {
    match result {
        None | Some("NotAttempted" | "Pending") => false,
        Some("Valid") => basis == ValidBasis::Refused,
        Some(_) => true,
    }
}

/// The word a refusal of a `Valid` on a basis [`ValidBasis`] refuses carries:
/// the verdict is one this installation does not accept, which is what
/// `Untrusted` says. Publishing `Valid` as the reason a point was refused would
/// read as a contradiction.
pub const REFUSED_VALID_VERDICT: &str = "Untrusted";

/// The points whose own `Backup` the controller REFUSED, keyed for a join
/// against view rows.
///
/// # Why a view row is not enough
///
/// A view is served until `viewExpiresAt`, so a row harvested before a receipt
/// was replaced or its signer revoked still says `Available`/`Verified`/
/// `selectable` after the controller fetched the receipt and recorded the
/// `Backup` `Invalid` or `Untrusted`. Every surface that reads a view row as
/// "usable" — retention's keep set, the console's point list — asks this set
/// first.
///
/// # The join
///
/// The FULL receipt digest decides wherever the `Backup` carries one
/// (`status.evidence.receiptSha256`, the digest the view row's `receiptSha256`
/// names). A `Backup` with no digest joins on the archive set id
/// (`status.backupId` = the row's `backupId`), the compatibility key
/// `protection::entries_for` uses for the same case. Both directions are
/// conservative: a match can only take a point OUT of the usable set.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ControllerRefusals {
    by_receipt: BTreeMap<String, String>,
    by_backup_id: BTreeMap<String, String>,
    unattributed: usize,
    incomplete: bool,
}

/// The longest verdict spelling a refusal carries onward. A result is a short
/// enum word; anything longer is truncated rather than copied into a response.
const MAX_REFUSAL_LEN: usize = 32;

/// The word a refusal carries when the `Backup`'s verdict field is PRESENT but
/// is not a verdict this build can read (not a string, or under a non-object
/// parent). "Could not read the verdict" is not "no verdict": an unknown
/// verdict is a refusal, and so is an unreadable one.
pub const UNREADABLE_VERDICT: &str = "Unreadable";

/// What a `Backup`'s `status.evidence.verification.result` field holds, read
/// LENIENTLY.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VerdictField {
    /// No verdict written: the field, or a parent of it, is absent or null.
    Absent,
    /// A verdict word.
    Word(String),
    /// Something is there and it is not a verdict word.
    Unreadable,
}

/// The three facts the refusal rule reads from ONE `Backup` — and nothing else.
///
/// # Why a projection and not the typed CRD
///
/// A typed `Backup` list fails WHOLE when one object does not deserialize: a
/// trigger kind a newer build wrote (`TriggerKind` is closed), a stored object
/// from an older schema, a missing required field. The refusal rule needs three
/// fields, and a bounded projection must not depend on the writer's field set
/// (the reasoning `protection::CatalogEntry` gives for its own lenient type).
/// So one malformed `Backup` refuses at most its OWN point — never the list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackupVerdictFacts {
    /// `status.backupId`, when it is a non-empty string.
    pub backup_id: Option<String>,
    /// `status.evidence.receiptSha256`, when it is a non-empty string.
    pub receipt_sha256: Option<String>,
    /// `status.evidence.verification.result`.
    pub result: VerdictField,
    /// `status.evidence.verification.trust`, read by [`ValidBasis`] — the
    /// half of the refusal rule a `Valid` needs.
    pub basis: ValidBasis,
}

impl BackupVerdictFacts {
    /// Read the three facts from an UNTYPED `Backup` object (its JSON, or the
    /// `data` of a `DynamicObject`, which carries `status` at the top level).
    #[must_use]
    pub fn from_json(object: &serde_json::Value) -> Self {
        let text = |v: Option<&serde_json::Value>| {
            v.and_then(serde_json::Value::as_str)
                .filter(|t| !t.is_empty())
                .map(str::to_string)
        };
        let status = object.get("status");
        let evidence = status.and_then(|s| s.get("evidence"));
        Self {
            backup_id: text(status.and_then(|s| s.get("backupId"))),
            receipt_sha256: text(evidence.and_then(|e| e.get("receiptSha256"))),
            result: verdict_at(object, &["status", "evidence", "verification", "result"]),
            basis: ValidBasis::of_json(
                evidence
                    .and_then(|e| e.get("verification"))
                    .and_then(|v| v.get("trust")),
            ),
        }
    }

    /// The same three facts from a typed `Backup`.
    #[must_use]
    pub fn from_backup(backup: &crate::crds::backup::Backup) -> Self {
        let status = backup.status.as_ref();
        let evidence = status.and_then(|s| s.evidence.as_ref());
        Self {
            backup_id: status
                .and_then(|s| s.backup_id.clone())
                .filter(|t| !t.is_empty()),
            receipt_sha256: evidence
                .and_then(|e| e.receipt_sha256.clone())
                .filter(|t| !t.is_empty()),
            result: evidence
                .and_then(|e| e.verification.as_ref())
                .and_then(|v| v.result.clone())
                .map_or(VerdictField::Absent, VerdictField::Word),
            basis: ValidBasis::of_block(
                evidence
                    .and_then(|e| e.verification.as_ref())
                    .and_then(|v| v.trust.as_ref()),
            ),
        }
    }
}

/// Walk `path`; a null or missing step is [`VerdictField::Absent`], a step that
/// is not an object where one is needed — or a leaf that is not a string — is
/// [`VerdictField::Unreadable`].
fn verdict_at(object: &serde_json::Value, path: &[&str]) -> VerdictField {
    let mut here = object;
    for key in path {
        match here {
            serde_json::Value::Object(map) => match map.get(*key) {
                None | Some(serde_json::Value::Null) => return VerdictField::Absent,
                Some(next) => here = next,
            },
            _ => return VerdictField::Unreadable,
        }
    }
    match here {
        serde_json::Value::String(word) => VerdictField::Word(word.clone()),
        _ => VerdictField::Unreadable,
    }
}

impl ControllerRefusals {
    /// Collect every reached refusal among `backups`, whatever their phase: a
    /// verdict the controller reached about some receipt bytes is a verdict
    /// about those bytes whether or not the run's Job succeeded.
    #[must_use]
    pub fn from_backups<'a, I>(backups: I) -> Self
    where
        I: IntoIterator<Item = &'a crate::crds::backup::Backup>,
    {
        Self::from_facts(backups.into_iter().map(BackupVerdictFacts::from_backup))
    }

    /// The same, over lenient projections.
    ///
    /// A verdict word that [`is_reached_refusal`] refuses, and an
    /// [`VerdictField::Unreadable`] verdict (published as
    /// [`UNREADABLE_VERDICT`]), refuse the point their digest — or, with no
    /// digest, their set id — names. A refusal that names NEITHER cannot be
    /// tied to any row; it is counted in [`Self::unattributed`], so a caller
    /// can say the join is incomplete rather than pretend it was not there.
    #[must_use]
    pub fn from_facts<I>(facts: I) -> Self
    where
        I: IntoIterator<Item = BackupVerdictFacts>,
    {
        let mut out = Self::default();
        for fact in facts {
            let result: String = match &fact.result {
                VerdictField::Absent => continue,
                VerdictField::Word(word) if !is_reached_refusal(Some(word), fact.basis) => continue,
                VerdictField::Word(word) if word == "Valid" => REFUSED_VALID_VERDICT.to_string(),
                VerdictField::Word(word) => word.chars().take(MAX_REFUSAL_LEN).collect(),
                VerdictField::Unreadable => UNREADABLE_VERDICT.to_string(),
            };
            match (fact.receipt_sha256, fact.backup_id) {
                (Some(digest), _) => {
                    out.by_receipt.insert(digest, result);
                }
                (None, Some(id)) => {
                    out.by_backup_id.insert(id, result);
                }
                (None, None) => out.unattributed += 1,
            }
        }
        out
    }

    /// The refused verdict for this view row's point, or `None` when no
    /// `Backup` in the set refused it.
    #[must_use]
    pub fn refusal_for(&self, entry: &ViewEntry) -> Option<&str> {
        self.by_receipt
            .get(&entry.receipt_sha256)
            .or_else(|| {
                Some(entry.backup_id.as_str())
                    .filter(|id| !id.is_empty())
                    .and_then(|id| self.by_backup_id.get(id))
            })
            .map(String::as_str)
    }

    /// How many refusals named no digest and no set id, so could be tied to
    /// no row.
    #[must_use]
    pub fn unattributed(&self) -> usize {
        self.unattributed
    }

    /// This set was built from a listing a bound cut short: a refusal on a
    /// `Backup` it did not reach is not in it.
    #[must_use]
    pub fn incomplete(mut self) -> Self {
        self.incomplete = true;
        self
    }

    /// Whether the listing this set was built from was complete, so "not in
    /// the set" means "no listed `Backup` refused it" for EVERY `Backup`.
    #[must_use]
    pub fn is_complete(&self) -> bool {
        !self.incomplete
    }

    /// Whether the set holds no refusal at all.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_receipt.is_empty() && self.by_backup_id.is_empty() && self.unattributed == 0
    }
}

// ===========================================================================
// The trust seam
// ===========================================================================

/// What a trust source says about one key.
///
/// `Retired` and `Revoked` are the two D3 §7.4 states a `TrustRoster` cannot
/// express, which is exactly why `TrustPolicy` exists.
/// [`TrustView::from_resolved`] produces them from a bound policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrustKeyState {
    /// Accepted for new evidence and for old.
    Active,
    /// Accepted for evidence signed while it was valid, and for nothing new.
    Retired,
    /// Compromised. Nothing it signed verifies without an independent
    /// pre-revocation observation, which this view does not have.
    Revoked,
}

/// One key the trust source lists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrustKey {
    /// `sha256(DER SPKI)` in lowercase hex — the same number `openssl` prints.
    pub key_id: String,
    /// The PUBLIC key, SubjectPublicKeyInfo in PEM. It is carried here so the
    /// bundle the sync Job mounts and the ids this controller classifies
    /// against come from ONE read of the trust source: two reads could observe
    /// two different rosters and mount a bundle the classification does not
    /// describe.
    pub spki_pem: String,
    /// Who the source says it belongs to. Display only, never authority.
    pub subject: Option<String>,
    /// The key's full lifecycle, for a `TrustPolicy` key — and then
    /// [`classify_verification`] judges it with [`logweir_core::trust::decide`]
    /// ITSELF, the judge the restore preflight and the runner apply to the
    /// same key, so the view cannot be the permissive one (a staged key, a
    /// claim in the future, a retirement boundary). `None` for a roster key:
    /// the roster has no lifecycle, and its classification is the one it
    /// always had.
    pub lifecycle: Option<logweir_core::trust::TrustedKey>,
    /// When it stops being accepted for NEW evidence.
    pub not_after: Option<Time>,
    /// Its lifecycle state.
    pub state: TrustKeyState,
}

/// The trust source, projected into the one shape this view needs.
///
/// # Built from the namespace's RESOLVED trust (CATALOG-TRUST-ROSTER-ONLY)
///
/// [`TrustView::from_resolution`] projects what [`crate::trust::resolve`]
/// answers for the catalog's namespace: the `TrustPolicy` that governs it, or
/// the synthesised `legacy-roster-v1` when none does. That is the resolution
/// the `Approval` controller verifies against, the restore preflight re-judges
/// a catalog point's signer against, and the runner's evidence keyring is
/// rendered from — so a point signed under a `TrustPolicy` key is `Verified`
/// here too, and a retired or revoked key is reflected. Builds before this
/// read only the `TrustRoster` ([`TrustView::from_roster`], kept for its
/// tests), so a `TrustPolicy`-signed point was `UntrustedSigner` or
/// `NotAttempted` in the view and never offered.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TrustView {
    /// The keys, in the source's own order.
    pub keys: Vec<TrustKey>,
    /// Which source produced this — for a status message, never for a decision.
    pub source: String,
    /// Why the namespace resolved to NO trust although trust is configured —
    /// two `TrustPolicy` objects claim it ([`crate::trust::Resolution::Conflict`]).
    /// `keys` is then empty, every point is `NotAttempted`, and
    /// `TrustAvailable=False` names the conflict rather than "no key material".
    pub unresolved: Option<String>,
}

impl TrustView {
    /// The `TrustRoster`'s signing keys, every one of them `Active` — the
    /// projection a roster-only namespace had before [`Self::from_resolution`]
    /// was wired in, and the reference `from_resolved` reproduces for the
    /// synthesised `legacy-roster-v1` (`tests/catalog_controller.rs`,
    /// `a_roster_only_namespace_projects_exactly_as_the_roster_did`).
    ///
    /// `signingKeys` AND NOT `approverKeys`: D3 §7.3's usage split says a key
    /// that may AUTHORISE a restore is not thereby a key that may ATTEST to
    /// one. The roster's `approverKeys` are the authorisation half and are not
    /// consulted here.
    ///
    /// Every entry is `Active` because the roster has no lifecycle field to
    /// read — that absence is the defect `TrustPolicy` was designed to fix, and
    /// inventing a state from `notAfter` alone would make a key that simply
    /// expired indistinguishable from one that was retired on purpose.
    /// `notAfter` IS carried, and [`classify_verification`] uses it.
    #[must_use]
    pub fn from_roster(spec: &TrustRosterSpec) -> Self {
        Self {
            keys: spec
                .signing_keys
                .iter()
                .map(|k| TrustKey {
                    key_id: k.key_id.clone(),
                    spki_pem: k.spki_pem.clone(),
                    subject: k.subject.clone(),
                    lifecycle: None,
                    not_after: k.not_after,
                    state: TrustKeyState::Active,
                })
                .collect(),
            source: TRUST_SOURCE_ROSTER.to_string(),
            unresolved: None,
        }
    }

    /// The trust [`crate::trust::resolve`] answered for the catalog's
    /// namespace, projected.
    ///
    /// * [`Resolution::Trust`](crate::trust::Resolution::Trust) →
    ///   [`Self::from_resolved`].
    /// * [`Resolution::Conflict`](crate::trust::Resolution::Conflict) → NO
    ///   keys and [`Self::unresolved`] naming the contesting policies: a
    ///   namespace claimed twice resolves to nothing (the module header of
    ///   [`crate::trust`] says why), and the catalog verifies nothing there
    ///   rather than picking a policy by sort order.
    /// * [`Resolution::Unconfigured`](crate::trust::Resolution::Unconfigured)
    ///   → no keys, under the roster's name — exactly what an absent
    ///   `TrustRoster` produced before `TrustPolicy` resolution was wired in.
    #[must_use]
    pub fn from_resolution(resolution: &crate::trust::Resolution) -> Self {
        match resolution {
            crate::trust::Resolution::Trust(trust) => Self::from_resolved(trust),
            crate::trust::Resolution::Conflict {
                namespace,
                policies,
            } => Self {
                keys: Vec::new(),
                source: String::new(),
                unresolved: Some(format!(
                    "the namespace {namespace} is claimed by more than one TrustPolicy ({}), so \
                     it resolves to no trust and no point here is presented as verified; \
                     remove it from all but one policy",
                    policies.join(", ")
                )),
            },
            crate::trust::Resolution::Unconfigured => Self {
                keys: Vec::new(),
                source: TRUST_SOURCE_ROSTER.to_string(),
                unresolved: None,
            },
        }
    }

    /// One namespace's resolved trust, projected: its `EvidenceSigning` keys
    /// only, each with the state and acceptance bound [`classify_verification`]
    /// reads.
    ///
    /// # The projection, key by key — [`logweir_core::trust::decide`]'s rules
    ///
    /// * `Active` → `Active`, bounded by `notAfter` (a key past it verifies
    ///   what it signed inside it, as `VerifiedHistorical`).
    /// * `Retired` → `Retired`, bounded by `accepted_through()` — the earlier
    ///   of `notAfter` and `retiredAt` — so evidence signed after the
    ///   retirement is not historical.
    /// * `Revoked` for `KeyCompromise` → `Revoked`: nothing it signed is
    ///   accepted, whenever it claims to have signed it.
    /// * `Revoked` for `Superseded`/`Unspecified` → `Retired` at the
    ///   revocation's effective instant (D3 §7.4: "treated as retirement"),
    ///   which is what `decide` answers for the same key.
    ///
    /// # Which keys
    ///
    /// `EvidenceSigning` and nothing else — a key that may AUTHORISE a restore
    /// is not thereby a key that may ATTEST to one (D3 §7.3). From a real
    /// policy, only USABLE keys (the PEM parses and hashes to its declared id):
    /// the runner is handed exactly those (`restore::evidence_keyring_bytes`),
    /// and a key it cannot verify with verifies nothing here either.
    ///
    /// From the synthesised `legacy-roster-v1`, every `signingKeys` entry,
    /// `Active`, with `notAfter` as the roster wrote it — [`Self::from_roster`]'s
    /// projection, so a roster-only installation mounts the same bundle and
    /// classifies every point exactly as before.
    #[must_use]
    pub fn from_resolved(trust: &crate::trust::ResolvedTrust) -> Self {
        use logweir_core::trust::KeyUsage;
        let legacy = trust.source.is_legacy();
        let keys = trust
            .keys
            .iter()
            .filter(|k| k.trust.has_usage(KeyUsage::EvidenceSigning))
            .filter(|k| legacy || k.is_usable())
            .map(|k| {
                if legacy {
                    legacy_trust_key(k)
                } else {
                    policy_trust_key(k)
                }
            })
            .collect();
        Self {
            keys,
            source: if legacy {
                TRUST_SOURCE_ROSTER.to_string()
            } else {
                format!("TrustPolicy/{}", trust.source.name())
            },
            unresolved: None,
        }
    }

    /// The key with this id, if the source lists one.
    #[must_use]
    pub fn key(&self, key_id: &str) -> Option<&TrustKey> {
        self.keys.iter().find(|k| k.key_id == key_id)
    }

    /// Whether the source holds any key material at all.
    ///
    /// `false` is what makes every verdict [`Verification::NotAttempted`] and
    /// `TrustAvailable=False`, and it is a DIFFERENT fact from "the signature
    /// did not verify" — an installation with no trust material has not
    /// disproved anything.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }
}

/// [`TrustView::source`] for the legacy roster.
pub const TRUST_SOURCE_ROSTER: &str = "TrustRoster/default";

/// One synthesised `legacy-roster-v1` key, as [`TrustView::from_roster`]
/// projects the same roster entry: `Active`, and `notAfter` absent when the
/// roster wrote none (the synthesis fills [`crate::trust::legacy_not_after`]).
/// The roster's display-only `subject` is not carried by the synthesis and is
/// read by nothing here.
fn legacy_trust_key(key: &crate::trust::ResolvedKey) -> TrustKey {
    TrustKey {
        key_id: key.trust.key_id.clone(),
        spki_pem: key.spki_pem.clone(),
        subject: None,
        lifecycle: None,
        not_after: (key.trust.not_after != crate::trust::legacy_not_after())
            .then_some(key.trust.not_after),
        state: TrustKeyState::Active,
    }
}

/// One `TrustPolicy` key — see [`TrustView::from_resolved`] for the rules.
fn policy_trust_key(key: &crate::trust::ResolvedKey) -> TrustKey {
    use logweir_core::trust::{KeyState, RevocationReason};
    let lifecycle = &key.trust;
    let (state, not_after) = match lifecycle.state {
        KeyState::Active => (TrustKeyState::Active, Some(lifecycle.not_after)),
        KeyState::Retired => (TrustKeyState::Retired, lifecycle.accepted_through()),
        KeyState::Revoked => match lifecycle.reason() {
            RevocationReason::KeyCompromise => (TrustKeyState::Revoked, Some(lifecycle.not_after)),
            RevocationReason::Superseded | RevocationReason::Unspecified => {
                (TrustKeyState::Retired, lifecycle.accepted_through())
            }
        },
    };
    TrustKey {
        key_id: lifecycle.key_id.clone(),
        spki_pem: key.spki_pem.clone(),
        subject: Some(lifecycle.principal_id.clone()),
        lifecycle: Some(lifecycle.clone()),
        not_after,
        state,
    }
}

/// Turn a signature verdict into a verification state, under a trust source.
///
/// # The order, and why each step is where it is
///
/// 1. **A signature that did not verify is [`Verification::Invalid`] whatever
///    the trust source says.** Trust cannot rescue bytes that do not match
///    their signature, and asking the roster first would let an unlisted key
///    turn a forgery into a mere `UntrustedSigner`.
/// 2. **No evidence and not-attempted pass through.** "There is no receipt" and
///    "nothing could be fetched" are facts about the archive and the run, not
///    about trust.
/// 3. **No trust material at all is `NotAttempted`.** An installation that
///    holds no key has not disproved a signature; reporting `UntrustedSigner`
///    would name the archive as the problem when the cluster is.
/// 4. **A verified signature under a key the source does not list is
///    [`Verification::UntrustedSigner`]** — D3 §5.5's "fresh installation"
///    case. It is never upgraded by proximity: a public key found beside an
///    archive is a claim (`docs/keys.md`).
/// 5. **Revoked wins over everything else the key could be**, because a
///    revocation is a statement that the private half is in someone else's
///    hands.
/// 6. **A `TrustPolicy` key is judged by [`logweir_core::trust::decide`]
///    itself** ([`TrustKey::lifecycle`]): every window row — a claim before
///    `notBefore`, after the accepted bound, in the future, against a key whose
///    window has not opened — is `decide`'s, so the view agrees with the
///    preflight and the runner by construction. Steps 5 and 7 are the roster's.
/// 7. **An expired or retired key still verifies what it signed while it was
///    valid** — [`Verification::VerifiedHistorical`], D3 §7.4. Evidence signed
///    AFTER `notAfter` is [`Verification::Invalid`]: the key was not accepted
///    then either.
///
/// `signed_at` is the point's own recorded instant, and `now` is this pass's.
#[must_use]
pub fn classify_verification(
    signature: SignatureVerdict,
    key_id: Option<&str>,
    signed_at: Option<DateTime<Utc>>,
    trust: &TrustView,
    now: DateTime<Utc>,
) -> Verification {
    match signature {
        SignatureVerdict::Invalid => return Verification::Invalid,
        SignatureVerdict::NoEvidence => return Verification::NoEvidence,
        SignatureVerdict::NotAttempted => return Verification::NotAttempted,
        SignatureVerdict::Verified => {}
    }
    if trust.is_empty() {
        return Verification::NotAttempted;
    }
    let Some(key_id) = key_id.filter(|k| !k.trim().is_empty()) else {
        // A "verified" verdict that names no key cannot be attributed to one,
        // and a verdict nobody can attribute is not a verdict.
        return Verification::NotAttempted;
    };
    let Some(key) = trust.key(key_id) else {
        return Verification::UntrustedSigner;
    };
    if let Some(lifecycle) = key.lifecycle.as_ref() {
        return decided(lifecycle, signed_at, now);
    }
    if key.state == TrustKeyState::Revoked {
        return Verification::Revoked;
    }
    match key.not_after {
        // Still inside its validity, whatever its declared state: a key that is
        // retired but not yet past `notAfter` is accepted for what it signed.
        Some(not_after) if now <= not_after => match key.state {
            TrustKeyState::Retired => Verification::VerifiedHistorical,
            _ => Verification::Verified,
        },
        Some(not_after) => match signed_at {
            Some(signed) if signed <= not_after => Verification::VerifiedHistorical,
            // Signed after the key stopped being accepted, or with no instant to
            // place it by: not historical, because there is nothing to say it
            // was inside the validity window.
            _ => Verification::Invalid,
        },
        None => match key.state {
            TrustKeyState::Retired => Verification::VerifiedHistorical,
            _ => Verification::Verified,
        },
    }
}

/// A `TrustPolicy` key's verdict: [`logweir_core::trust::decide`] for
/// `EvidenceSigning`, at the point's claimed signing time, with no independent
/// observation (the view has none), mapped onto the view's vocabulary.
///
/// | `decide` | view |
/// |---|---|
/// | `Valid`, basis `Current` | `Verified` |
/// | `Valid`, basis `Historical` | `VerifiedHistorical` |
/// | `Valid`, basis `Unverified` | `NotAttempted` |
/// | `Valid`, any other basis | as `Untrusted` below — never selectable |
/// | `Untrusted`, `Revoked` or `RecordedBeforeRevocation` | `Revoked` |
/// | `Untrusted`, `SignedOutsideValidity` — before `notBefore`, after the accepted bound, in the future, against a key whose window has not opened, or no instant at all | `Invalid` |
/// | `Untrusted`, `UntrustedSigner`/`KeyUsageMismatch` | `UntrustedSigner` |
fn decided(
    lifecycle: &logweir_core::trust::TrustedKey,
    signed_at: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
) -> Verification {
    use logweir_core::trust::{ClaimAbsence, EvidenceClaim, IndependentObservation, KeyUsage};
    let claim = signed_at.map_or(
        EvidenceClaim::absent(ClaimAbsence::FieldAbsent),
        EvidenceClaim::at,
    );
    let verdict = logweir_core::trust::decide(
        Some(lifecycle),
        KeyUsage::EvidenceSigning,
        &claim,
        &IndependentObservation::none(),
        now,
    );
    verification_of_verdict(&verdict)
}

/// [`decided`]'s mapping of one [`logweir_core::trust::Verdict`] onto the
/// view's vocabulary — the table above.
///
/// # AN ALLOW-LIST (TRUST-VALID-BASIS-CLASS)
///
/// This used to read `(Valid, Current) => Verified, (Valid, _) =>
/// VerifiedHistorical`: a catch-all that made any `Valid` selectable whatever
/// its basis. `decide` pairs `Valid` with `Current`/`Historical` only, so the
/// wildcard was unreachable — and one edit away from a selectable point on
/// `RecordedBeforeRevocation`. A `Valid` is now read by [`ValidBasis`], the
/// rule the controller's badge uses, and a basis that is not a pass falls to
/// the `Untrusted` rows.
#[must_use]
pub fn verification_of_verdict(verdict: &logweir_core::trust::Verdict) -> Verification {
    use logweir_core::trust::{TrustBasis, TrustResult, UntrustReason};
    if verdict.result == TrustResult::Valid {
        match ValidBasis::of_core(verdict.basis) {
            ValidBasis::Absent | ValidBasis::Current => return Verification::Verified,
            ValidBasis::Historical => return Verification::VerifiedHistorical,
            ValidBasis::Unverified => return Verification::NotAttempted,
            ValidBasis::Refused => {}
        }
    }
    match (verdict.basis, verdict.reason) {
        (_, Some(UntrustReason::Revoked | UntrustReason::RecordedBeforeRevocation))
        | (TrustBasis::RecordedBeforeRevocation, _) => Verification::Revoked,
        (_, Some(UntrustReason::UntrustedSigner | UntrustReason::KeyUsageMismatch)) => {
            Verification::UntrustedSigner
        }
        _ => Verification::Invalid,
    }
}

// ===========================================================================
// The `catalogSync` result body — the grammar
// ===========================================================================

/// `catalog-page=<i>/<n> count=<c> sha256=<hex>`.
pub const PAGE_LINE_PREFIX: &str = "catalog-page=";
/// `catalog-entry=<compact json>`.
pub const ENTRY_LINE_PREFIX: &str = "catalog-entry=";
/// `catalog-counts=<json>`.
pub const COUNTS_LINE_PREFIX: &str = "catalog-counts=";
/// `catalog-cursor=<json>`.
pub const CURSOR_LINE_PREFIX: &str = "catalog-cursor=";
/// `catalog-signers=<json>`.
pub const SIGNERS_LINE_PREFIX: &str = "catalog-signers=";

/// `catalog-format=<n>` — the grammar's own version line, which the parser
/// REQUIRES.
///
/// The record layout is versioned in its key path and in `format_version`; the
/// relay grammar was not, which meant a newer runner could only extend it by
/// hoping this parser ignored what it did not know (review finding F7). A body
/// with no version line, or with a version this build does not know, is refused
/// by name instead.
pub const FORMAT_LINE_PREFIX: &str = "catalog-format=";

/// The only [`FORMAT_LINE_PREFIX`] value this build reads.
pub const BODY_FORMAT_VERSION: u32 = 1;

/// The most bytes of `details` one body may carry — review finding F7.
///
/// FIVE MEGABYTES, AND IT IS A BYTE BUDGET BECAUSE THE TRANSPORT IS. The
/// relay bounds the body twice by bytes and never by lines:
/// [`crate::check::relay::RELAY_LIMIT_BYTES`] is 8 MiB on the `pods/log` read
/// and [`crate::check::relay::DECODER_BUDGET_BYTES`] is 8 MiB counted over
/// BASE64 part-frame lines, i.e. roughly 5.9 MB of raw `details` after the
/// ~1.35x expansion. A line cap says nothing about either: 25 000 entries at a
/// realistic 600 B each is ~15 MB, which blows both budgets and surfaces as
/// `ResultUnreadable` — a transport message for what is really "you sent too
/// much". Five megabytes sits inside the narrower of the two with headroom and
/// is what D2's runner is told to honour.
pub const MAX_BODY_BYTES: usize = 5 * 1024 * 1024;

/// The most `catalog-signers` rows one body may declare — review finding F7.
///
/// SIXTY-FOUR, four times the sixteen [`MAX_SIGNERS`] the status can carry, so
/// an archive written by many installations still reports an exact
/// `untrustedSigner` total (finding F10) without an unbounded list reaching
/// this process.
pub const MAX_BODY_SIGNERS: usize = 64;

/// The most locations one point may be reported at — review finding F12.
///
/// SIXTEEN. `locations[]` is the only unbounded field in an entry, and an
/// entry whose rendered line does not fit a page is an entry that cannot be
/// published; bounding it in the GRAMMAR turns that into a named refusal rather
/// than a `ConfigMap` the API server rejects at CREATE.
pub const MAX_ENTRY_LOCATIONS: usize = 16;

/// The most pages a body may declare. Eight is the CRD's `status.pages`
/// `maxItems`; a body that declares more is refused rather than truncated,
/// because a truncated page set would silently drop points.
pub const MAX_BODY_PAGES: usize = 8;

/// One point, as the sync Job reports it.
///
/// **The receipt-derived facts are the binding** (D3 §5.2 rule 3): the point
/// id, the receipt digest and the manifest digest are what a later restore
/// re-checks. Everything else here is for display and for selection.
///
/// `topics` is deliberately ABSENT: a view entry is ~350 bytes so that 5 000 of
/// them fit in two `ConfigMap`s, and a topic list is unbounded. The topics of a
/// point live in its signed record in object storage, which is where PLAT-15.2's
/// wizard reads them from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunnerEntry {
    /// `lwp1-<32 hex>` — content-derived from the receipt bytes (D3 §5.1).
    pub point_id: String,
    /// The archive SET identifier. Two receipts under one `backupId` are two
    /// points, which is defect **RECEIPT-DUP**'s answer.
    pub backup_id: String,
    /// The run that wrote the receipt.
    pub run_id: String,
    /// The recovery point, in epoch milliseconds — the record's
    /// `capture.started_at`.
    pub recovery_point_at_ms: i64,
    /// The covered window's start, in epoch milliseconds.
    pub covered_from_ms: i64,
    /// The covered window's end, EXCLUSIVE (invariant I22).
    pub covered_to_ms: i64,
    /// Where the bytes could be read from, and **how each place fared** — one
    /// entry per location holding the same receipt. An archive copied to a
    /// second bucket is ONE point in TWO places (D3 §5.1).
    ///
    /// A location whose own `availability` is absent inherits the entry's.
    /// Bounded by [`MAX_ENTRY_LOCATIONS`].
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub locations: Vec<EntryLocation>,
    /// The receipt's key under `logweir/`.
    pub receipt_key: String,
    /// `sha256:<hex>` of the receipt bytes — the binding the short id displays.
    pub receipt_sha256: String,
    /// The manifest's key, when the record names one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manifest_key: Option<String>,
    /// `sha256:<hex>` of the manifest, when the record names one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manifest_sha256: Option<String>,
    /// When the record was written. Absent means unknown, never zero.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recorded_at: Option<Time>,
    /// The record's `format_version`, when it parsed far enough to carry one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format_version: Option<String>,
    /// What the Job could read.
    pub availability: Availability,
    /// What the Job could say about the signature — NOT about trust.
    pub signature: SignatureVerdict,
    /// The key the signature verified under, or the key the record claims.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signer_key_id: Option<String>,
    /// A one-sentence remedy for a state that is not selectable. Never a
    /// credential, a principal or log content.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remedy: Option<String>,
}

impl RunnerEntry {
    /// The receipt-derived facts two records of one identity must agree on —
    /// D3 §5.2 rule 3's set, minus the fields that are informational.
    ///
    /// `locations`, `recordedAt`, `signature`, `signerKeyId` and `remedy` are
    /// deliberately OUT: two records differing only in where they were found
    /// are one point in two places, which is the whole point of a
    /// content-derived identity.
    fn facts(&self) -> (i64, i64, i64, &str, &str, &str, Option<&str>, Option<&str>) {
        (
            self.recovery_point_at_ms,
            self.covered_from_ms,
            self.covered_to_ms,
            &self.backup_id,
            &self.run_id,
            &self.receipt_sha256,
            self.manifest_key.as_deref(),
            self.manifest_sha256.as_deref(),
        )
    }
}

/// One place a point's bytes were looked for, and what was found there.
///
/// PER-LOCATION AVAILABILITY IS WHY A COPY IS AN ASSET AND NOT A LIABILITY
/// (review finding F9, decision recorded at integration as an amendment to D3
/// §5.4). D3 §5.1's whole point is that an archive copied to a second bucket is
/// ONE point in TWO places; a merge that took the WORST availability across
/// those places would hide a fully recoverable point because a second copy went
/// missing — the opposite of what a second copy is for. So availability merges
/// **best-of**, each location keeps its own verdict here, and the degraded ones
/// are named in `remedy` so nobody has to guess which copy to repair.
///
/// The SIGNATURE half keeps worst-of, and that asymmetry is deliberate: bytes
/// that fail verification in one place are evidence about the point, not about
/// the place.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EntryLocation {
    /// `s3://<bucket>/<prefix>` — bucket and prefix only. No endpoint, no
    /// region, no credential, and deliberately NOT part of the identity.
    pub location_id: String,
    /// What was found HERE. Absent inherits the entry's own availability, which
    /// is what a runner reporting one observation at one place writes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub availability: Option<Availability>,
}

impl EntryLocation {
    /// This location's availability, falling back to the observation's.
    #[must_use]
    pub fn availability_or(&self, fallback: Availability) -> Availability {
        self.availability.unwrap_or(fallback)
    }
}

/// One location in a published view entry, with its verdict RESOLVED.
///
/// `availability` is required here and optional on [`EntryLocation`]: a reader
/// of the view must never have to re-apply an inheritance rule to find out
/// whether a copy is readable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResolvedLocation {
    /// See [`EntryLocation::location_id`].
    pub location_id: String,
    /// What was found there.
    pub availability: Availability,
}

/// One point as it is written into a page — the runner's entry with the
/// controller's [`Verification`] in place of the Job's [`SignatureVerdict`].
///
/// **`selectable` is materialised and not derived by the reader.** D3 §5.4's
/// rule is one conjunction, and a UI that recomputed it from two enums it had
/// to parse would be a second implementation of the rule that matters most.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ViewEntry {
    /// See [`RunnerEntry::point_id`].
    pub point_id: String,
    /// See [`RunnerEntry::backup_id`].
    pub backup_id: String,
    /// See [`RunnerEntry::run_id`].
    pub run_id: String,
    /// See [`RunnerEntry::recovery_point_at_ms`].
    pub recovery_point_at_ms: i64,
    /// See [`RunnerEntry::covered_from_ms`].
    pub covered_from_ms: i64,
    /// See [`RunnerEntry::covered_to_ms`].
    pub covered_to_ms: i64,
    /// See [`RunnerEntry::locations`]. Every verdict is resolved.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub locations: Vec<ResolvedLocation>,
    /// See [`RunnerEntry::receipt_key`].
    pub receipt_key: String,
    /// See [`RunnerEntry::receipt_sha256`].
    pub receipt_sha256: String,
    /// See [`RunnerEntry::manifest_key`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manifest_key: Option<String>,
    /// See [`RunnerEntry::manifest_sha256`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manifest_sha256: Option<String>,
    /// See [`RunnerEntry::format_version`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format_version: Option<String>,
    /// D3 §5.4's first axis.
    pub availability: Availability,
    /// D3 §5.4's second axis, after the controller's trust re-evaluation.
    pub verification: Verification,
    /// See [`RunnerEntry::signer_key_id`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signer_key_id: Option<String>,
    /// `availability.selectable() && verification.selectable()`.
    pub selectable: bool,
    /// See [`RunnerEntry::remedy`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remedy: Option<String>,
}

/// What the sync counted over the WHOLE walk, not only over the window.
///
/// The window is the newest `viewLimit` points; these numbers cover everything
/// the walk saw, which is what makes `truncated` meaningful and what lets the
/// histogram describe the archive rather than the page set.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunnerCounts {
    /// Every point the walk saw.
    pub total: i64,
    /// Availability buckets, over the whole walk.
    #[serde(default)]
    pub available: i64,
    /// See [`Availability::Missing`].
    #[serde(default)]
    pub missing: i64,
    /// See [`Availability::Unreadable`].
    #[serde(default)]
    pub unreadable: i64,
    /// See [`Availability::Deleted`].
    #[serde(default)]
    pub deleted: i64,
    /// See [`Availability::Conflict`].
    #[serde(default)]
    pub conflict: i64,
    /// See [`Availability::UnsupportedFormat`].
    #[serde(default)]
    pub unsupported_format: i64,
    /// See [`Availability::Partial`].
    #[serde(default)]
    pub partial: i64,
    /// Signature buckets, over the whole walk.
    #[serde(default)]
    pub signature: SignatureCounts,
    /// Points per day, newest day first. Bounded by [`MAX_HISTOGRAM_DAYS`].
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub by_day: Vec<DayCount>,
}

/// The signature half of [`RunnerCounts`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SignatureCounts {
    /// Verified under the mounted key material.
    #[serde(default)]
    pub verified: i64,
    /// Did not verify.
    #[serde(default)]
    pub invalid: i64,
    /// A manifest with no receipt at all.
    #[serde(default)]
    pub no_evidence: i64,
    /// No verdict was reached.
    #[serde(default)]
    pub not_attempted: i64,
}

/// One day of the histogram, as the runner reports it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DayCount {
    /// `YYYY-MM-DD`, UTC.
    pub day: String,
    /// How many points.
    pub points: i64,
}

/// Who signed the points in this archive, over the whole walk.
///
/// `trusted` is deliberately ABSENT: the Job does not decide trust. The
/// controller adds it in [`signer_summaries`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunnerSigner {
    /// `sha256(DER SPKI)` in lowercase hex.
    pub key_id: String,
    /// What the record said the principal was. A HINT: it is unsigned metadata
    /// and is never authority.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub principal_hint: Option<String>,
    /// How many points it signed.
    #[serde(default)]
    pub points: i64,
}

/// Where the walk got to.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CursorReport {
    /// The day shard the next `Index` sync resumes from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_shard: Option<String>,
    /// The key the next `Full` rescan continues after.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rescan_start_after: Option<String>,
    /// Whether the walk finished. `false` is a budgeted walk that continues,
    /// never a failure.
    #[serde(default)]
    pub complete: bool,
}

/// One page as the runner declared and filled it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedPage {
    /// Its 1-based index within the body.
    pub index: u32,
    /// How many pages the body declares.
    pub of: u32,
    /// How many entries the header declared.
    pub declared_count: u32,
    /// The digest the header declared, lowercase hex, no `sha256:` prefix.
    pub declared_sha256: String,
    /// The entries that parsed.
    pub entries: Vec<RunnerEntry>,
    /// How many entry lines did not parse and were skipped.
    pub skipped: u32,
}

/// Everything one `catalogSync` result body carried.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SyncBody {
    /// The pages, in the order they arrived.
    pub pages: Vec<ParsedPage>,
    /// The whole-walk counts, when the body carried them.
    pub counts: Option<RunnerCounts>,
    /// Where the walk got to, when the body said.
    pub cursor: Option<CursorReport>,
    /// Who signed, over the whole walk.
    pub signers: Vec<RunnerSigner>,
    /// How many entry lines in the whole body did not parse.
    pub skipped_entries: u32,
}

impl SyncBody {
    /// Every entry from every page, in arrival order.
    #[must_use]
    pub fn entries(&self) -> Vec<RunnerEntry> {
        self.pages.iter().flat_map(|p| p.entries.clone()).collect()
    }
}

/// Why a `catalogSync` result body could not be read.
///
/// Every variant is a fact about the BODY and carries no log content: a body
/// that does not decode is exactly the input nobody should paste into a status
/// field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BodyError {
    /// A `catalog-page=` header did not parse.
    MalformedPageHeader,
    /// An entry line arrived before any page header.
    EntryBeforePage,
    /// A page's entry lines do not hash to the digest its header declared.
    PageDigestMismatch {
        /// The page's 1-based index.
        index: u32,
    },
    /// A page carried a different number of entry lines from the one it
    /// declared.
    PageCountMismatch {
        /// The page's 1-based index.
        index: u32,
        /// What the header said.
        declared: u32,
        /// What arrived.
        got: u32,
    },
    /// The body declared more pages than [`MAX_BODY_PAGES`].
    TooManyPages {
        /// What the body declared.
        declared: u32,
    },
    /// The body carried more entry lines than the caller's `viewLimit`.
    TooManyEntries {
        /// What the caller allowed.
        allowed: usize,
    },
    /// The body is larger than [`MAX_BODY_BYTES`].
    TooLarge {
        /// How many bytes arrived.
        got: usize,
    },
    /// The body carries no [`FORMAT_LINE_PREFIX`] line.
    MissingFormat,
    /// The body declares a grammar version this build does not read.
    UnsupportedBodyFormat {
        /// What it declared.
        got: String,
    },
    /// A summary line arrived twice. The field names which.
    RepeatedSummary(&'static str),
    /// Page indices were not `1..=n`, in order, each exactly once.
    PageSequence,
    /// A `catalog-counts=`, `catalog-cursor=` or `catalog-signers=` line did
    /// not parse. The field names which.
    MalformedSummary(&'static str),
    /// The result body was not UTF-8.
    NotUtf8,
}

impl std::fmt::Display for BodyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MalformedPageHeader => write!(
                f,
                "a `{PAGE_LINE_PREFIX}` header is not `<i>/<n> count=<c> sha256=<hex>`"
            ),
            Self::EntryBeforePage => write!(
                f,
                "a `{ENTRY_LINE_PREFIX}` line arrived before any `{PAGE_LINE_PREFIX}` header, so \
                 there is no page to attribute it to"
            ),
            Self::PageDigestMismatch { index } => write!(
                f,
                "page {index}'s entry lines do not hash to the digest its header declared; the \
                 log read is not intact and no page is written from it"
            ),
            Self::PageCountMismatch {
                index,
                declared,
                got,
            } => write!(
                f,
                "page {index} declared {declared} entries and {got} arrived"
            ),
            Self::TooManyPages { declared } => write!(
                f,
                "the body declares {declared} pages and at most {MAX_BODY_PAGES} are readable"
            ),
            Self::TooManyEntries { allowed } => write!(
                f,
                "the body carries more than the {allowed} entry lines this catalog's viewLimit \
                 allows"
            ),
            Self::TooLarge { got } => write!(
                f,
                "the result body is {got} bytes and at most {MAX_BODY_BYTES} are readable; the \
                 relay's own budget is narrower still"
            ),
            Self::MissingFormat => write!(
                f,
                "the body carries no `{FORMAT_LINE_PREFIX}` line, so its grammar version is \
                 unknown and nothing in it is read"
            ),
            Self::UnsupportedBodyFormat { got } => write!(
                f,
                "the body declares grammar version `{got}` and this build reads \
                 {BODY_FORMAT_VERSION}"
            ),
            Self::RepeatedSummary(which) => write!(
                f,
                "the `{which}` line arrived twice; a summary that could be overwritten is a \
                 summary nobody can attribute"
            ),
            Self::PageSequence => {
                write!(f, "page headers are not 1..=n in order, each exactly once")
            }
            Self::MalformedSummary(which) => {
                write!(f, "the `{which}` line is not the JSON document it must be")
            }
            Self::NotUtf8 => write!(f, "the result body is not UTF-8"),
        }
    }
}

impl std::error::Error for BodyError {}

impl BodyError {
    /// The closed D2 code this is reported as.
    ///
    /// ALWAYS [`logweir_core::check_contract::CheckCode::ResultUnreadable`] —
    /// D-SEAMS **S1**: failures use D2's closed error-code vocabulary rather
    /// than new strings, and from a consumer's side "the sync's output did not
    /// read" is one fact whatever the sub-cause. The sub-cause is the
    /// [`Display`] above.
    #[must_use]
    pub fn code(&self) -> logweir_core::check_contract::CheckCode {
        logweir_core::check_contract::CheckCode::ResultUnreadable
    }
}

/// The digest a page header declares, over the page's own entry lines.
///
/// **Over the RAW line bodies, each followed by `\n`** — the JSON after
/// `catalog-entry=`, byte for byte as it arrived, and never a re-serialisation.
/// A digest over reparsed values would verify nothing: the point of the check is
/// that the bytes this controller read are the bytes the Job wrote, which is
/// transport integrity and explicitly NOT authorization (D3 §5.3). The
/// receipt's own signature is the verification root.
#[must_use]
pub fn page_digest(entry_lines: &[&str]) -> String {
    let mut buf = String::new();
    for line in entry_lines {
        buf.push_str(line);
        buf.push('\n');
    }
    logweir_core::ids::sha256_hex(buf.as_bytes())
}

/// Parse a `catalogSync` result body — **pure**.
///
/// # Errors
///
/// [`BodyError`], naming which rule failed and carrying no body content.
pub fn parse_body(text: &str, max_entries: usize) -> Result<SyncBody, BodyError> {
    // THE BYTE BUDGET FIRST, before a single line is scanned. The transport
    // bounds this body by bytes twice over (see [`MAX_BODY_BYTES`]), so a body
    // that is too large is refused as too large rather than as whatever its
    // first malformed line happens to be.
    if text.len() > MAX_BODY_BYTES {
        return Err(BodyError::TooLarge { got: text.len() });
    }
    let mut out = SyncBody::default();
    let mut headers: Vec<(u32, u32, u32, String)> = Vec::new();
    let mut raw_pages: Vec<Vec<&str>> = Vec::new();
    let mut entry_lines = 0usize;
    let mut format: Option<String> = None;

    for line in text.lines() {
        let line = line.trim_end_matches('\r');
        if let Some(rest) = line.strip_prefix(FORMAT_LINE_PREFIX) {
            if format.is_some() {
                return Err(BodyError::RepeatedSummary(FORMAT_LINE_PREFIX));
            }
            format = Some(rest.trim().to_string());
        } else if let Some(rest) = line.strip_prefix(PAGE_LINE_PREFIX) {
            let header = parse_page_header(rest).ok_or(BodyError::MalformedPageHeader)?;
            if header.1 as usize > MAX_BODY_PAGES {
                return Err(BodyError::TooManyPages { declared: header.1 });
            }
            headers.push(header);
            raw_pages.push(Vec::new());
        } else if let Some(rest) = line.strip_prefix(ENTRY_LINE_PREFIX) {
            let page = raw_pages.last_mut().ok_or(BodyError::EntryBeforePage)?;
            entry_lines += 1;
            if entry_lines > max_entries {
                return Err(BodyError::TooManyEntries {
                    allowed: max_entries,
                });
            }
            page.push(rest);
        } else if let Some(rest) = line.strip_prefix(COUNTS_LINE_PREFIX) {
            // A REPEAT IS AN ERROR AND NOT LAST-WINS (review finding F7). Two
            // `catalog-counts=` lines mean the runner disagreed with itself
            // about the whole walk, and silently keeping one of them publishes
            // a number nobody can attribute to an observation.
            if out.counts.is_some() {
                return Err(BodyError::RepeatedSummary(COUNTS_LINE_PREFIX));
            }
            out.counts = Some(
                serde_json::from_str(rest)
                    .map_err(|_| BodyError::MalformedSummary(COUNTS_LINE_PREFIX))?,
            );
        } else if let Some(rest) = line.strip_prefix(CURSOR_LINE_PREFIX) {
            if out.cursor.is_some() {
                return Err(BodyError::RepeatedSummary(CURSOR_LINE_PREFIX));
            }
            out.cursor = Some(
                serde_json::from_str(rest)
                    .map_err(|_| BodyError::MalformedSummary(CURSOR_LINE_PREFIX))?,
            );
        } else if let Some(rest) = line.strip_prefix(SIGNERS_LINE_PREFIX) {
            if !out.signers.is_empty() {
                return Err(BodyError::RepeatedSummary(SIGNERS_LINE_PREFIX));
            }
            let signers: Vec<RunnerSigner> = serde_json::from_str(rest)
                .map_err(|_| BodyError::MalformedSummary(SIGNERS_LINE_PREFIX))?;
            if signers.len() > MAX_BODY_SIGNERS {
                return Err(BodyError::MalformedSummary(SIGNERS_LINE_PREFIX));
            }
            out.signers = signers;
        }
        // Anything else is ignored: a result body shares the stream with
        // whatever else the runner wrote, exactly as D2's frame decoder does.
    }

    // THE VERSION LINE IS REQUIRED. A body with no declared grammar is a body
    // this build cannot promise it read the way the writer meant.
    match format.as_deref() {
        None => return Err(BodyError::MissingFormat),
        Some(v) if v.parse::<u32>().ok() == Some(BODY_FORMAT_VERSION) => {}
        Some(other) => {
            return Err(BodyError::UnsupportedBodyFormat {
                got: other.to_string(),
            })
        }
    }

    if headers.len() > MAX_BODY_PAGES {
        return Err(BodyError::TooManyPages {
            declared: u32::try_from(headers.len()).unwrap_or(u32::MAX),
        });
    }
    // 1..=n, in order, each exactly once — and every header agreeing on `n`.
    let total = u32::try_from(headers.len()).unwrap_or(u32::MAX);
    for (i, (index, of, _, _)) in headers.iter().enumerate() {
        let want = u32::try_from(i + 1).unwrap_or(u32::MAX);
        if *index != want || *of != total {
            return Err(BodyError::PageSequence);
        }
    }

    for ((index, _, declared_count, declared_sha256), raw) in headers.into_iter().zip(raw_pages) {
        let got = u32::try_from(raw.len()).unwrap_or(u32::MAX);
        if got != declared_count {
            return Err(BodyError::PageCountMismatch {
                index,
                declared: declared_count,
                got,
            });
        }
        // THE DIGEST BEFORE THE PARSE. A page whose bytes did not survive the
        // log read is refused as a page; deciding that after parsing would let
        // a truncated read contribute the entries that happened to be whole.
        //
        // IT COVERS THE ENTRY LINES AND NOTHING ELSE. The three summary lines
        // are covered by the frame stream's own digest, which D2's decoder
        // verifies before this function is ever called; a second digest over
        // them would be a second answer to a question already answered.
        if page_digest(&raw) != declared_sha256 {
            return Err(BodyError::PageDigestMismatch { index });
        }
        let mut entries = Vec::with_capacity(raw.len());
        let mut skipped = 0u32;
        for body in &raw {
            match serde_json::from_str::<RunnerEntry>(body) {
                // AN UNBOUNDED `locations[]` IS A MALFORMED ENTRY (review
                // finding F12). It is the only unbounded field an entry has,
                // and an entry whose rendered line cannot fit a page is an
                // entry that cannot be published.
                Ok(entry) if entry.locations.len() > MAX_ENTRY_LOCATIONS => {
                    skipped = skipped.saturating_add(1);
                }
                Ok(entry) => entries.push(entry),
                // SKIPPED AND COUNTED, NEVER FATAL — D3 §5.2's reading rules:
                // a malformed entry is one point this build cannot show, not a
                // sync that failed. The count reaches the status message.
                Err(_) => skipped = skipped.saturating_add(1),
            }
        }
        out.skipped_entries = out.skipped_entries.saturating_add(skipped);
        out.pages.push(ParsedPage {
            index,
            of: total,
            declared_count,
            declared_sha256,
            entries,
            skipped,
        });
    }
    Ok(out)
}

/// `<i>/<n> count=<c> sha256=<hex>`.
fn parse_page_header(rest: &str) -> Option<(u32, u32, u32, String)> {
    let mut parts = rest.split_whitespace();
    let (index, of) = parts.next()?.split_once('/')?;
    let index: u32 = index.parse().ok()?;
    let of: u32 = of.parse().ok()?;
    if index == 0 || of == 0 || index > of {
        return None;
    }
    let count: u32 = parts.next()?.strip_prefix("count=")?.parse().ok()?;
    let sha = parts.next()?.strip_prefix("sha256=")?.to_ascii_lowercase();
    if parts.next().is_some() {
        return None;
    }
    if sha.len() != 64 || !sha.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    Some((index, of, count, sha))
}

// ===========================================================================
// Duplicate identity, ordering, and the window
// ===========================================================================

/// Collapse entries that share one point id — D3 §5.1 and §5.2 rule 4.
///
/// * **The same receipt found in two locations is ONE point with TWO
///   locations.** Identity is `sha256(receipt bytes)`, so a copied archive
///   yields the same id by construction; producing two rows would tell an
///   operator they have two recovery points when they have one, in two places.
/// * **Two records of one identity that disagree about a receipt-derived fact
///   are a [`Availability::Conflict`]**, on the merged row, with the union of
///   their locations. Neither is preferred: `logweir/` objects are never
///   rewritten, so a disagreement means something wrote a second record, and
///   picking one would be picking a side.
/// * Two receipts under one `backupId` have DIFFERENT point ids and are two
///   rows — defect **RECEIPT-DUP**.
///
/// # The two axes merge in OPPOSITE directions, and that asymmetry is the rule
///
/// * **Availability merges BEST-of** (review finding F9; recorded as an
///   amendment to D3 §5.4 at integration). A point present in bucket A and
///   absent from bucket B is still fully recoverable from A, and hiding it from
///   the restore wizard because a second copy went missing is the opposite of
///   what a second copy is for. Each location keeps its own verdict in
///   [`EntryLocation`], and the degraded ones are NAMED in `remedy` so nobody
///   has to guess which copy to repair.
/// * **The signature merges WORST-of**, with the key id following the worst
///   verdict. Bytes that fail verification in one place are evidence about the
///   POINT, not about the place, and `selectable` must never survive it.
/// * **`Conflict` overrides both.** A receipt-derived disagreement is a fact
///   about the records and not about any location, so it is applied last.
///
/// The output is sorted newest recovery point first, with the point id as the
/// tie-break so the order is total and the page set is reproducible.
#[must_use]
pub fn merge_entries(entries: Vec<RunnerEntry>) -> Vec<RunnerEntry> {
    let mut by_id: BTreeMap<String, RunnerEntry> = BTreeMap::new();
    let mut conflicted: BTreeSet<String> = BTreeSet::new();
    for mut entry in entries {
        // Resolve every location's verdict against the observation ONCE, here,
        // so the merge below never has to know which side an entry came from.
        for location in &mut entry.locations {
            location.availability = Some(location.availability_or(entry.availability));
        }
        match by_id.get_mut(&entry.point_id) {
            None => {
                by_id.insert(entry.point_id.clone(), entry);
            }
            Some(kept) => {
                if kept.facts() != entry.facts() {
                    conflicted.insert(entry.point_id.clone());
                }
                for location in entry.locations {
                    match kept
                        .locations
                        .iter_mut()
                        .find(|l| l.location_id == location.location_id)
                    {
                        // ONE PLACE REPORTED TWICE KEEPS THE WORSE VERDICT. Two
                        // observations of the SAME bucket are two attempts at
                        // one thing, and "it worked once" is not a property of
                        // the bucket.
                        Some(existing) => {
                            let a = existing.availability_or(kept.availability);
                            let b = location.availability_or(entry.availability);
                            if worse(b, a) {
                                existing.availability = Some(b);
                            }
                        }
                        None => kept.locations.push(location),
                    }
                }
                kept.locations
                    .sort_by(|a, b| a.location_id.cmp(&b.location_id));
                // THE ENTRY-LEVEL VERDICT IS NOT DECIDED HERE. Every location's
                // own availability was resolved on the way in, so the best-of
                // answer is a `max` over `kept.locations` and is computed once,
                // below, when every observation has been folded in. Doing it
                // pairwise here as well would be a second implementation of the
                // same rule — and a dead one, since the value it wrote would be
                // overwritten by that `max`.
                if worse_signature(entry.signature, kept.signature) {
                    kept.signature = entry.signature;
                    kept.signer_key_id = entry.signer_key_id;
                }
            }
        }
    }
    let mut out: Vec<RunnerEntry> = by_id.into_values().collect();
    for entry in &mut out {
        // The entry-level verdict is the BEST any location reached; with no
        // locations at all it is the observation's own.
        if let Some(best) = entry
            .locations
            .iter()
            .map(|l| l.availability_or(entry.availability))
            .max_by_key(|a| availability_rank(*a))
        {
            entry.availability = best;
        }
        entry.remedy = name_degraded_locations(entry);
        if conflicted.contains(&entry.point_id) {
            entry.availability = Availability::Conflict;
        }
    }
    out.sort_by(|a, b| {
        b.recovery_point_at_ms
            .cmp(&a.recovery_point_at_ms)
            .then_with(|| a.point_id.cmp(&b.point_id))
    });
    out
}

/// The entry's remedy, with every location that is not [`Availability::Available`]
/// named — review finding F9.
///
/// A best-of merge would otherwise DROP the information that a copy is broken:
/// the entry says `Available` and nothing says which bucket to repair. Names
/// only: a location id is a bucket and a prefix, never an endpoint, a region or
/// a credential.
fn name_degraded_locations(entry: &RunnerEntry) -> Option<String> {
    let degraded: Vec<String> = entry
        .locations
        .iter()
        .filter(|l| l.availability_or(entry.availability) != Availability::Available)
        .map(|l| {
            format!(
                "{} is {}",
                l.location_id,
                l.availability_or(entry.availability)
            )
        })
        .collect();
    if degraded.is_empty() {
        return entry.remedy.clone();
    }
    let note = format!(
        "this point is readable, and {} of its {} location(s) is not: {}",
        degraded.len(),
        entry.locations.len(),
        degraded.join("; ")
    );
    Some(match entry.remedy.as_deref() {
        Some(existing) if !existing.trim().is_empty() => format!("{existing}. {note}"),
        _ => note,
    })
}

/// A total order on "how good is this availability", worst first.
fn availability_rank(a: Availability) -> u8 {
    match a {
        Availability::Conflict => 0,
        Availability::Missing => 1,
        Availability::Partial => 2,
        Availability::Unreadable => 3,
        Availability::UnsupportedFormat => 4,
        Availability::Deleted => 5,
        Availability::Available => 6,
    }
}

fn worse(candidate: Availability, kept: Availability) -> bool {
    availability_rank(candidate) < availability_rank(kept)
}

fn signature_rank(s: SignatureVerdict) -> u8 {
    match s {
        SignatureVerdict::Invalid => 0,
        SignatureVerdict::NoEvidence => 1,
        SignatureVerdict::NotAttempted => 2,
        SignatureVerdict::Verified => 3,
    }
}

fn worse_signature(candidate: SignatureVerdict, kept: SignatureVerdict) -> bool {
    signature_rank(candidate) < signature_rank(kept)
}

// ===========================================================================
// Pages and the fence pointer
// ===========================================================================

/// The hard ceiling on `sync.viewLimit`, whatever the spec says — D3 §5.3.
pub const MAX_VIEW_ENTRIES: usize = 5000;

/// Entry bytes one page `ConfigMap` may carry.
///
/// 768 KiB against the API server's 1 MiB object limit: the remainder carries
/// the object's own metadata, its annotations and the second data key. A page
/// that overflowed would be rejected at CREATE with a message about etcd, which
/// is not a message about a catalog.
pub const PAGE_MAX_BYTES: usize = 768 * 1024;

/// The most pages a view may have — the CRD's `status.pages` `maxItems`.
pub const MAX_PAGES: usize = 8;

/// The most days `status.histogram` may carry — the CRD's `maxItems`.
pub const MAX_HISTOGRAM_DAYS: usize = 400;

/// The most signers `status.signers` may carry — the CRD's `maxItems`.
pub const MAX_SIGNERS: usize = 16;

/// The `ConfigMap` key holding one page's entries, one compact JSON per line.
pub const PAGE_DATA_KEY: &str = "entries.jsonl";

/// The `ConfigMap` key holding the fence-pointer document.
pub const INDEX_DATA_KEY: &str = "index.json";

/// `sha256:<hex>` over a page's `entries.jsonl`, on the page object itself.
pub const PAGE_DIGEST_ANNOTATION: &str = "logweir.dev/catalog-page-sha256";

/// The point-id range and time range of one page — the fence pointer.
///
/// # A fence pointer EXCLUDES pages; it never proves one holds anything
///
/// `minPointId`/`maxPointId` bound the ids in a page, so a lookup for an id
/// outside `[min, max]` may skip that page. Inside the range the page may still
/// not hold it: entries are ordered by recovery point and not by id, so the
/// range is not dense. That is the classic LSM fence-pointer contract — no
/// false negatives, false positives allowed — and stating it is what stops a
/// later reader treating a hit as an existence proof.
///
/// `newestMs`/`oldestMs` are the same idea on the axis the view is actually
/// ordered by, and there the bound IS tight.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PageFence {
    /// The page's 0-based index, matching `status.pages[].index`.
    pub index: i64,
    /// The `ConfigMap` holding it.
    pub config_map_name: String,
    /// How many entries it carries.
    pub count: i64,
    /// The first entry's point id, in view order.
    pub first_point_id: String,
    /// The last entry's point id, in view order.
    pub last_point_id: String,
    /// The lexicographically smallest point id in the page.
    pub min_point_id: String,
    /// The lexicographically largest point id in the page.
    pub max_point_id: String,
    /// The newest recovery point in the page, epoch milliseconds.
    pub newest_ms: i64,
    /// The oldest recovery point in the page, epoch milliseconds.
    pub oldest_ms: i64,
    /// `sha256:<hex>` over the page's `entries.jsonl`.
    pub sha256: String,
}

/// The document in the fence-pointer `ConfigMap`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IndexDocument {
    /// The generation token every object of this view shares.
    pub generation: String,
    /// The `viewLimit` this view was materialised under.
    pub view_limit: i64,
    /// Whether the archive holds more points than the view carries.
    pub truncated: bool,
    /// How many entries the view carries in total.
    pub entries: i64,
    /// The pages, in view order.
    pub pages: Vec<PageFence>,
}

/// One page, rendered and ready to become a `ConfigMap`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageDraft {
    /// Its 0-based index.
    pub index: usize,
    /// Its entries, in view order.
    pub entries: Vec<ViewEntry>,
    /// `entries.jsonl` — one compact JSON per line, each line newline
    /// terminated.
    pub body: String,
    /// `sha256:<hex>` over [`PageDraft::body`].
    pub sha256: String,
}

impl PageDraft {
    /// The fence pointer for this page, given its `ConfigMap` name.
    ///
    /// # Panics
    ///
    /// Never: [`materialise`] produces no empty page, and the `expect` names
    /// that invariant rather than hiding it behind a default.
    #[must_use]
    pub fn fence(&self, config_map_name: &str) -> PageFence {
        let first = self.entries.first().expect("a page draft is never empty");
        let last = self.entries.last().expect("a page draft is never empty");
        let mut ids: Vec<&str> = self.entries.iter().map(|e| e.point_id.as_str()).collect();
        ids.sort_unstable();
        PageFence {
            index: i64::try_from(self.index).unwrap_or(i64::MAX),
            config_map_name: config_map_name.to_string(),
            count: i64::try_from(self.entries.len()).unwrap_or(i64::MAX),
            first_point_id: first.point_id.clone(),
            last_point_id: last.point_id.clone(),
            min_point_id: (*ids.first().expect("a page draft is never empty")).to_string(),
            max_point_id: (*ids.last().expect("a page draft is never empty")).to_string(),
            newest_ms: first.recovery_point_at_ms,
            oldest_ms: last.recovery_point_at_ms,
            sha256: self.sha256.clone(),
        }
    }

    /// `status.pages[]`'s row for this page.
    #[must_use]
    pub fn status_row(&self, config_map_name: &str) -> CatalogPage {
        let fence = self.fence(config_map_name);
        CatalogPage {
            config_map_name: fence.config_map_name,
            index: fence.index,
            count: fence.count,
            first_point_id: Some(fence.first_point_id),
            last_point_id: Some(fence.last_point_id),
            sha256: Some(fence.sha256),
        }
    }
}

/// The whole materialised view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct View {
    /// The pages, in view order.
    pub pages: Vec<PageDraft>,
    /// How many entries were materialised.
    pub entries: usize,
    /// Whether the archive holds more points than the view carries.
    pub truncated: bool,
    /// How many entries were dropped because the page budget ran out, as
    /// opposed to because of `viewLimit`. Reported so "the view is a window"
    /// and "this build could not fit the window" stay distinguishable.
    pub dropped_for_space: usize,
    /// How many entries were refused because ONE rendered line does not fit a
    /// page at all — review finding F12. An entry that cannot be published is
    /// counted here rather than producing a `ConfigMap` the API server rejects
    /// at CREATE, which would surface as a requeue loop and not as a verdict.
    pub dropped_oversized: usize,
    /// How many of the materialised entries are `Revoked`, and how many are
    /// `VerifiedHistorical` — the two verification states `status.counts` has
    /// no field for (review finding F4).
    pub window: WindowStates,
}

/// The states of the materialised window that the ten status counters cannot
/// carry — review finding F4.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct WindowStates {
    /// Points whose signing key this installation has REVOKED. Latent until a
    /// trust source can express revocation, and the reason this is counted at
    /// all: a revoked point is `selectable: false` inside the page and would
    /// otherwise appear in no adverse number on the status.
    pub revoked: i64,
    /// Points that verify under a retired or expired key, for evidence signed
    /// while it was valid.
    pub verified_historical: i64,
}

/// The bounds a view is materialised under.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ViewLimits {
    /// `spec.sync.viewLimit`, clamped to [`MAX_VIEW_ENTRIES`].
    pub view_limit: usize,
    /// Entry bytes per page — [`PAGE_MAX_BYTES`] in production, smaller in a
    /// test that wants to reach the second page without 700 KiB of fixture.
    pub page_max_bytes: usize,
    /// The most pages — [`MAX_PAGES`].
    pub max_pages: usize,
}

impl ViewLimits {
    /// The bounds `spec.sync` asks for, clamped to what this build supports.
    #[must_use]
    pub fn from_settings(sync: &SyncSettings) -> Self {
        Self {
            view_limit: usize::try_from(sync.view_limit)
                .unwrap_or(MAX_VIEW_ENTRIES)
                .min(MAX_VIEW_ENTRIES),
            page_max_bytes: PAGE_MAX_BYTES,
            max_pages: MAX_PAGES,
        }
    }
}

/// Turn the walk's entries into pages — **pure**, and the function every
/// "large catalog" property is stated over.
///
/// The entries are merged ([`merge_entries`]), the newest `viewLimit` are
/// kept, each is re-classified against the trust source and rendered, and the
/// renderings are packed into pages by byte budget. `total` is the whole walk's
/// count, which is what decides `truncated`.
#[must_use]
pub fn materialise(
    entries: Vec<RunnerEntry>,
    total: i64,
    trust: &TrustView,
    limits: &ViewLimits,
    now: DateTime<Utc>,
) -> View {
    let merged = merge_entries(entries);
    let merged_len = merged.len();
    let window: Vec<ViewEntry> = merged
        .into_iter()
        .take(limits.view_limit)
        .map(|e| view_entry(e, trust, now))
        .collect();

    let mut pages: Vec<PageDraft> = Vec::new();
    let mut current: Vec<ViewEntry> = Vec::new();
    let mut current_bytes = 0usize;
    let mut placed = 0usize;
    let mut dropped_for_space = 0usize;
    let mut dropped_oversized = 0usize;
    let mut states = WindowStates::default();

    for entry in window {
        let line = serde_json::to_string(&entry).unwrap_or_default();
        let cost = line.len() + 1;
        // ONE LINE THAT CANNOT FIT A PAGE IS REFUSED, NOT PLACED ANYWAY
        // (review finding F12). The old guard only sealed a page when the
        // current one was non-empty, so a single oversized entry went into an
        // over-budget ConfigMap the API server rejects at CREATE.
        if cost > limits.page_max_bytes {
            dropped_oversized += 1;
            continue;
        }
        if !current.is_empty() && current_bytes + cost > limits.page_max_bytes {
            pages.push(seal(std::mem::take(&mut current)));
            current_bytes = 0;
        }
        if pages.len() >= limits.max_pages && current.is_empty() {
            dropped_for_space += 1;
            continue;
        }
        match entry.verification {
            Verification::Revoked => states.revoked += 1,
            Verification::VerifiedHistorical => states.verified_historical += 1,
            _ => {}
        }
        current_bytes += cost;
        current.push(entry);
        placed += 1;
    }
    if !current.is_empty() {
        pages.push(seal(current));
    }

    View {
        pages,
        entries: placed,
        // TRUNCATED IS ABOUT THE ARCHIVE AND NOT ABOUT THE PAGES. `total` is
        // what the walk saw; `merged_len` is what survived de-duplication. The
        // view is a window whenever either exceeds what was placed.
        truncated: total > i64::try_from(placed).unwrap_or(i64::MAX)
            || merged_len > placed
            || dropped_for_space > 0
            || dropped_oversized > 0,
        dropped_for_space,
        dropped_oversized,
        window: states,
    }
}

fn seal(entries: Vec<ViewEntry>) -> PageDraft {
    let mut body = String::new();
    for entry in &entries {
        body.push_str(&serde_json::to_string(entry).unwrap_or_default());
        body.push('\n');
    }
    PageDraft {
        index: 0,
        sha256: logweir_core::ids::sha256_prefixed(body.as_bytes()),
        body,
        entries,
    }
}

/// One runner entry, re-classified against the trust source.
#[must_use]
pub fn view_entry(entry: RunnerEntry, trust: &TrustView, now: DateTime<Utc>) -> ViewEntry {
    let signed_at = entry.recorded_at.or_else(|| {
        Utc.timestamp_millis_opt(entry.recovery_point_at_ms)
            .single()
    });
    let verification = classify_verification(
        entry.signature,
        entry.signer_key_id.as_deref(),
        signed_at,
        trust,
        now,
    );
    let availability = entry.availability;
    ViewEntry {
        selectable: selectable(availability, verification),
        point_id: entry.point_id,
        backup_id: entry.backup_id,
        run_id: entry.run_id,
        recovery_point_at_ms: entry.recovery_point_at_ms,
        covered_from_ms: entry.covered_from_ms,
        covered_to_ms: entry.covered_to_ms,
        locations: entry
            .locations
            .into_iter()
            .map(|l| ResolvedLocation {
                availability: l.availability_or(availability),
                location_id: l.location_id,
            })
            .collect(),
        receipt_key: entry.receipt_key,
        receipt_sha256: entry.receipt_sha256,
        manifest_key: entry.manifest_key,
        manifest_sha256: entry.manifest_sha256,
        format_version: entry.format_version,
        availability: entry.availability,
        verification,
        signer_key_id: entry.signer_key_id,
        remedy: entry.remedy,
    }
}

/// Number the pages and build the fence-pointer document.
#[must_use]
pub fn index_document(
    generation: &str,
    limits: &ViewLimits,
    view: &View,
    names: &[String],
) -> IndexDocument {
    IndexDocument {
        generation: generation.to_string(),
        view_limit: i64::try_from(limits.view_limit).unwrap_or(i64::MAX),
        truncated: view.truncated,
        entries: i64::try_from(view.entries).unwrap_or(i64::MAX),
        pages: view
            .pages
            .iter()
            .enumerate()
            .map(|(i, p)| {
                let mut numbered = p.clone();
                numbered.index = i;
                numbered.fence(names.get(i).map_or("", String::as_str))
            })
            .collect(),
    }
}

// ===========================================================================
// The ten status counters
// ===========================================================================

/// The counters, plus the states the v1alpha1 status has no field for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tally {
    /// What goes on `status.counts`.
    pub counts: CatalogCounts,
    /// `(state, count, scope)` for every state the ten fields cannot carry, in
    /// a stable order. Named in the `Synced` condition message rather than
    /// dropped.
    ///
    /// `scope` is [`SCOPE_WALK`] or [`SCOPE_VIEW`] — some of these numbers can
    /// only be known for the materialised window, and saying which is the
    /// difference between a bounded fact and a wrong one.
    pub unrepresented: Vec<(&'static str, i64, &'static str)>,
}

/// A residue count that covers everything the sync walked.
pub const SCOPE_WALK: &str = "the archive";
/// A residue count that covers only the materialised window — the verification
/// axis is re-decided HERE, so it is only known for the entries this controller
/// re-classified.
pub const SCOPE_VIEW: &str = "the materialised view";

/// Project the walk's counts and signer summary onto the CRD's ten counters.
///
/// # The ten fields are a PROJECTION and not a partition
///
/// D3 §5.4 has seven availability states and seven verification states;
/// `status.counts` has ten fields across both axes. Three consequences, all of
/// them stated rather than smoothed over:
///
/// * **`untrustedSigner` is computed here and not by the Job.** The Job holds
///   the key material this controller mounted; whether a key is one this
///   installation ACCEPTS is a fact about the trust source. Summing
///   `signers[].points` over untrusted keys is what makes the number cover the
///   whole walk rather than only the window.
/// * **`unverified` is `notAttempted` plus `noEvidence`.** Both mean "no
///   signature verdict was reached"; the CRD has one field for them.
/// * **`Partial`, `Revoked` and `VerifiedHistorical` have no field.** They are
///   returned in [`Tally::unrepresented`] so the condition message can name
///   them, and they are the reason the availability fields do not sum to
///   `total`. **NOTE FOR W13:** `status.counts.partial` and
///   `status.counts.verifiedHistorical` are the two fields this would want.
#[must_use]
pub fn tally(counts: &RunnerCounts, signers: &[SignerSummary], window: WindowStates) -> Tally {
    // THE FULL SIGNER LIST, BEFORE [`bounded`] TRUNCATES IT (review finding
    // F10). `untrustedSigner` is documented as covering the whole walk, and
    // summing it over the sixteen rows the status can DISPLAY would silently
    // under-report an archive written by seventeen installations.
    let untrusted: i64 = signers
        .iter()
        .filter(|s| s.trusted == Some(false))
        .map(|s| s.points.unwrap_or(0))
        .sum();
    let mut unrepresented = Vec::new();
    if counts.partial > 0 {
        unrepresented.push((Availability::Partial.as_str(), counts.partial, SCOPE_WALK));
    }
    // `Revoked` AND `VerifiedHistorical` HAVE NO COUNTER EITHER (review
    // finding F4). A revoked point is `selectable: false` inside the page and
    // would otherwise land in NO adverse number on the status — `invalid` comes
    // from the runner's signature verdict, `unverified` from
    // notAttempted + noEvidence, and `untrustedSigner` from unlisted keys.
    if window.revoked > 0 {
        unrepresented.push((Verification::Revoked.as_str(), window.revoked, SCOPE_VIEW));
    }
    if window.verified_historical > 0 {
        unrepresented.push((
            Verification::VerifiedHistorical.as_str(),
            window.verified_historical,
            SCOPE_VIEW,
        ));
    }
    Tally {
        counts: CatalogCounts {
            total: Some(counts.total),
            available: Some(counts.available),
            missing: Some(counts.missing),
            unreadable: Some(counts.unreadable),
            unverified: Some(
                counts
                    .signature
                    .not_attempted
                    .saturating_add(counts.signature.no_evidence),
            ),
            untrusted_signer: Some(untrusted),
            invalid: Some(counts.signature.invalid),
            conflict: Some(counts.conflict),
            deleted: Some(counts.deleted),
            unsupported_format: Some(counts.unsupported_format),
        },
        unrepresented,
    }
}

/// `status.signers[]`, with the trust decision this controller made.
///
/// Bounded to [`MAX_SIGNERS`], **untrusted keys first**: a summary that dropped
/// the unknown signer to stay inside the bound would hide the one row PLAT-15.2
/// asks an administrator to act on. Within each group the order is by point
/// count, descending, then by key id, so the list is stable across passes.
#[must_use]
pub fn signer_summaries(signers: &[RunnerSigner], trust: &TrustView) -> Vec<SignerSummary> {
    let mut rows: Vec<SignerSummary> = signers
        .iter()
        .map(|s| SignerSummary {
            key_id: s.key_id.clone(),
            principal_hint: s.principal_hint.clone(),
            points: Some(s.points),
            // NEVER `None`. An absent `trusted` would read as "not known yet"
            // on a surface whose whole job is to say whether an unknown key
            // signed these points; with no trust material every key is
            // untrusted, and the `TrustAvailable` condition is what says why.
            trusted: Some(accepts(trust, &s.key_id)),
        })
        .collect();
    rows.sort_by(|a, b| {
        a.trusted
            .cmp(&b.trusted)
            .then_with(|| b.points.cmp(&a.points))
            .then_with(|| a.key_id.cmp(&b.key_id))
    });
    rows
}

/// Whether this installation ACCEPTS evidence from a key — review finding F4.
///
/// **Listed is not accepted.** A key the trust source lists with
/// `state: Revoked` is a key whose private half is in someone else's hands, and
/// reporting `trusted: true` for it would tell an operator reading `status`
/// alone that a compromised signer is fine. `Retired` IS accepted, because a
/// retirement is what makes `VerifiedHistorical` meaningful: the evidence it
/// signed while valid still verifies.
#[must_use]
pub fn accepts(trust: &TrustView, key_id: &str) -> bool {
    matches!(
        trust.key(key_id).map(|k| k.state),
        Some(TrustKeyState::Active | TrustKeyState::Retired)
    )
}

/// [`signer_summaries`]'s output, cut to what `status.signers` can carry.
///
/// A SEPARATE STEP so the counters can be summed over the FULL list first
/// (review finding F10). Untrusted keys are already first, so the rows that
/// survive are the ones an administrator has to act on.
#[must_use]
pub fn bounded(mut rows: Vec<SignerSummary>) -> Vec<SignerSummary> {
    rows.truncate(MAX_SIGNERS);
    rows
}

/// `status.histogram`, bounded and newest day first.
#[must_use]
pub fn histogram(counts: &RunnerCounts) -> Vec<HistogramBucket> {
    let mut days: Vec<HistogramBucket> = counts
        .by_day
        .iter()
        .filter(|d| is_iso_day(&d.day))
        .map(|d| HistogramBucket {
            day: d.day.clone(),
            points: d.points,
        })
        .collect();
    days.sort_by(|a, b| b.day.cmp(&a.day));
    // FOLDED WITH `+=`, NEVER DROPPED (review finding F11). A runner that
    // reports one day in two shards must not lose those points from the
    // histogram while `counts.total` still includes them — silently losing
    // points is the one option this module's own philosophy argues against.
    // `dedup_by` sees `(later, earlier)` and keeps the EARLIER element, so the
    // sum is accumulated onto that one.
    days.dedup_by(|later, earlier| {
        if later.day == earlier.day {
            earlier.points = earlier.points.saturating_add(later.points);
            true
        } else {
            false
        }
    });
    days.truncate(MAX_HISTOGRAM_DAYS);
    days
}

fn is_iso_day(day: &str) -> bool {
    day.len() == 10
        && day.as_bytes()[4] == b'-'
        && day.as_bytes()[7] == b'-'
        && day
            .bytes()
            .enumerate()
            .all(|(i, b)| i == 4 || i == 7 || b.is_ascii_digit())
}

// ===========================================================================
// Names, tokens and the TTL
// ===========================================================================

/// How many hex characters of `sha256(catalog uid)` every name below carries.
///
/// Twenty, exactly as [`crate::check::job::OWNER_UID_HEX_CHARS`], and for the
/// same reason: a name derived from the OBJECT's name can be made
/// unschedulable by naming the object badly, and D3 §5.3's illustrative
/// `<catalog>-g<generation>-p<index>` would overflow the 63-character
/// `batch.kubernetes.io/job-name` label at a catalog name of 40 characters.
/// The real names are published in `status.pages[].configMapName` and
/// `status.indexConfigMap`, so no consumer ever computes one.
pub const OWNER_UID_HEX_CHARS: usize = 20;

/// The prefix of every object this view creates.
pub const NAME_PREFIX: &str = "lwc-cs-";

/// Why a sync is running.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncTrigger {
    /// `spec.syncRequest` changed. The token is the spec's, verbatim.
    Requested(String),
    /// The interval elapsed. The number is the slot index.
    Periodic(i64),
}

impl SyncTrigger {
    /// The generation token this trigger produces — short, deterministic and
    /// DNS-label safe.
    ///
    /// Deterministic is the whole property: two reconciles of one trigger
    /// compute one name, so the second gets **409 `AlreadyExists`** from the
    /// API server rather than creating a second sync Job. That is the same
    /// mechanism [`crate::slot`] uses for scheduled backups, and it is what
    /// makes `syncRequest` idempotent without a lock.
    #[must_use]
    pub fn token(&self) -> String {
        match self {
            Self::Requested(token) => {
                let digest = logweir_core::ids::sha256_hex(token.as_bytes());
                format!("r{}", &digest[..8])
            }
            Self::Periodic(slot) => format!("s{:x}", slot.max(&0)),
        }
    }
}

/// Which slot `now` falls in, for an interval in seconds.
///
/// `None` when the interval is zero — `sync.intervalSeconds: 0` is "manual
/// only", and a manual-only catalog has no slots at all.
#[must_use]
pub fn periodic_slot(now: DateTime<Utc>, interval_seconds: i32) -> Option<i64> {
    let interval = i64::from(interval_seconds);
    if interval <= 0 {
        return None;
    }
    Some(now.timestamp().div_euclid(interval))
}

/// The stem every object of one sync shares: `lwc-cs-<20 hex>-<token>`.
#[must_use]
pub fn sync_stem(catalog_uid: &str, token: &str) -> String {
    let digest = logweir_core::ids::sha256_hex(catalog_uid.as_bytes());
    format!("{NAME_PREFIX}{}-{token}", &digest[..OWNER_UID_HEX_CHARS])
}

/// The sync Job's name — [`sync_stem`].
#[must_use]
pub fn sync_job_name(catalog_uid: &str, token: &str) -> String {
    sync_stem(catalog_uid, token)
}

/// Page `i`'s `ConfigMap` name: `<stem>-p<i>`, zero-based.
#[must_use]
pub fn page_config_map_name(stem: &str, index: usize) -> String {
    format!("{stem}-p{index}")
}

/// The fence-pointer `ConfigMap`'s name: `<stem>-index`.
#[must_use]
pub fn index_config_map_name(stem: &str) -> String {
    format!("{stem}-index")
}

/// The trust-material `ConfigMap`'s name: `lwc-cs-<20 hex>-trust-g<gen>`.
///
/// Owned by the **`RecoveryCatalog`** and not by the sync Job, deliberately: it
/// is reused by every sync while the trust source's generation is unchanged, so
/// tying it to a Job's TTL would make each sync render it again under a new
/// owner and get a 409 it could not adopt. It carries only PUBLIC key material
/// — a `ConfigMap` is the right object for that and a Secret would be the wrong
/// one — and it is collected with the catalog, so nothing deletes it.
#[must_use]
pub fn trust_config_map_name(catalog_uid: &str, policy_generation: i64) -> String {
    let digest = logweir_core::ids::sha256_hex(catalog_uid.as_bytes());
    format!(
        "{NAME_PREFIX}{}-trust-g{policy_generation}",
        &digest[..OWNER_UID_HEX_CHARS]
    )
}

/// Where the trust bundle is mounted inside the sync pod.
pub const TRUST_MOUNT_PATH: &str = "/check/trust";
/// The volume name for [`TRUST_MOUNT_PATH`].
pub const TRUST_VOLUME: &str = "catalog-trust";
/// The `ConfigMap` key holding the concatenated PEM bundle.
pub const TRUST_BUNDLE_KEY: &str = "trust-bundle.pem";
/// The `ConfigMap` key holding the key ids, one per line, in bundle order.
pub const TRUST_KEY_IDS_KEY: &str = "key-ids.txt";

/// The floor on a sync Job's `ttlSecondsAfterFinished` — **one hour**.
///
/// IT WAS A DAY, AND A DAY WAS A LEAK (review finding F3). One sync Job lives
/// per slot, so the number of GENERATIONS alive at once is
/// `ttl / intervalSeconds`, not three: at `intervalSeconds: 3600` a day's floor
/// kept 24 of them — about 216 page `ConfigMap`s and, at a realistic 600 B an
/// entry over 5 000 points, some 72 MB of etcd per catalog. At D3 §5.3's own
/// 300 s floor it was 288 generations and near a gigabyte, past etcd's default
/// quota with two or three catalogs.
///
/// An hour bounds it: with the CEL floor of 300 s
/// ([`crate::crds::recovery_catalog::J3_INTERVAL_FLOOR_RULE`]) at most **12**
/// generations coexist, and at an hourly cadence exactly **3** — which is what
/// "three intervals" was always meant to mean. A manual-only catalog keeps its
/// view for an hour after its last sync and republishes on the next request.
pub const TTL_FLOOR_SECONDS: i32 = 3_600;

/// `ttlSecondsAfterFinished` for a sync Job: `max(3 × interval, 3600)`.
///
/// THREE INTERVALS, so a view outlives two missed syncs and the page set does
/// not blink out between them, with an hour's floor so a manual-only catalog
/// (`intervalSeconds: 0`) still has one. The TTL is the ONLY thing that removes
/// a page, a fence pointer or a plan — `delete` is granted on nothing — so it
/// is also the only bound on how many of them exist at once.
#[must_use]
pub fn ttl_seconds(interval_seconds: i32) -> i32 {
    interval_seconds.saturating_mul(3).max(TTL_FLOOR_SECONDS)
}

/// How many view generations can be alive at once at this cadence.
///
/// `ttl / interval`, which is the number D3 §5.3's "the previous one ages out"
/// glosses over. Stated as a function so the documentation and the test read
/// one arithmetic.
#[must_use]
pub fn live_generations(interval_seconds: i32) -> i32 {
    if interval_seconds <= 0 {
        return 1;
    }
    ttl_seconds(interval_seconds) / interval_seconds
}

/// When the view ages out: the Job's finish plus its TTL.
#[must_use]
pub fn view_expires_at(finished_at: DateTime<Utc>, interval_seconds: i32) -> DateTime<Utc> {
    finished_at + chrono::Duration::seconds(i64::from(ttl_seconds(interval_seconds)))
}

/// Whether the view is stale — D3's test matrix: `2 × interval`.
///
/// A manual-only catalog is NEVER stale by the clock: nothing was promised
/// about when it would sync, and reporting `Stale` for a catalog whose owner
/// asked for manual syncs would be reporting the configuration as a fault.
#[must_use]
pub fn is_stale(
    now: DateTime<Utc>,
    synced_at: Option<DateTime<Utc>>,
    interval_seconds: i32,
) -> bool {
    if interval_seconds <= 0 {
        return false;
    }
    let Some(synced_at) = synced_at else {
        return false;
    };
    now > synced_at + chrono::Duration::seconds(i64::from(interval_seconds) * 2)
}

// ===========================================================================
// The objects the controller creates
// ===========================================================================

/// An owner reference that does NOT block deletion — D3 §5.3.
///
/// `blockOwnerDeletion: false` on every page, on the fence pointer and on the
/// trust bundle. `true` asks the API server for `update` on the owner's
/// `finalizers` subresource under the `OwnerReferencesPermissionEnforcement`
/// admission plugin, which this `ClusterRole` grants on nothing; and blocking
/// the deletion of a Job whose TTL has fired is the opposite of what the
/// garbage-collection design wants.
fn owner_reference(owner: &RunnerOwner) -> OwnerReference {
    OwnerReference {
        api_version: owner.api_version.clone(),
        kind: owner.kind.clone(),
        name: owner.name.clone(),
        uid: owner.uid.clone(),
        controller: Some(true),
        block_owner_deletion: Some(false),
    }
}

/// One immutable page `ConfigMap`, owned by the sync Job.
#[must_use]
pub fn page_config_map(
    name: &str,
    namespace: &str,
    job_owner: &RunnerOwner,
    catalog_uid: &str,
    draft: &PageDraft,
    index: usize,
) -> ConfigMap {
    ConfigMap {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some(namespace.to_string()),
            labels: Some(BTreeMap::from([
                (
                    crate::check::job::LABEL_MANAGED_BY.to_string(),
                    crate::check::job::MANAGED_BY.to_string(),
                ),
                (
                    LABEL_COMPONENT.to_string(),
                    COMPONENT_CATALOG_PAGE.to_string(),
                ),
                // THE CATALOG'S UID, NOT THE JOB'S (review finding F8). The label
                // exists so an operator can find every object of ONE catalog with
                // `kubectl get cm -l`; keyed on the Job it changed every slot and
                // matched nothing but that slot.
                (LABEL_CATALOG_UID.to_string(), catalog_uid.to_string()),
                (LABEL_PAGE_INDEX.to_string(), index.to_string()),
            ])),
            annotations: Some(BTreeMap::from([(
                PAGE_DIGEST_ANNOTATION.to_string(),
                draft.sha256.clone(),
            )])),
            owner_references: Some(vec![owner_reference(job_owner)]),
            ..ObjectMeta::default()
        },
        // A VIEW A SECOND PASS COULD REWRITE IS NOT A VIEW. Immutability is
        // what makes `status.pages[].sha256` mean something to a reader that
        // fetched the page a minute later.
        immutable: Some(true),
        data: Some(BTreeMap::from([(
            PAGE_DATA_KEY.to_string(),
            draft.body.clone(),
        )])),
        binary_data: None,
    }
}

/// The immutable fence-pointer `ConfigMap`, owned by the sync Job.
///
/// # Errors
///
/// [`serde_json::Error`] if the document does not serialise, which it always
/// does; named rather than unwrapped.
pub fn index_config_map(
    name: &str,
    namespace: &str,
    job_owner: &RunnerOwner,
    catalog_uid: &str,
    document: &IndexDocument,
) -> Result<ConfigMap, serde_json::Error> {
    let body = serde_json::to_string(document)?;
    Ok(ConfigMap {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some(namespace.to_string()),
            labels: Some(BTreeMap::from([
                (
                    crate::check::job::LABEL_MANAGED_BY.to_string(),
                    crate::check::job::MANAGED_BY.to_string(),
                ),
                (
                    LABEL_COMPONENT.to_string(),
                    COMPONENT_CATALOG_INDEX.to_string(),
                ),
                // See `page_config_map` — the CATALOG's UID (finding F8).
                (LABEL_CATALOG_UID.to_string(), catalog_uid.to_string()),
            ])),
            annotations: Some(BTreeMap::from([(
                PAGE_DIGEST_ANNOTATION.to_string(),
                logweir_core::ids::sha256_prefixed(body.as_bytes()),
            )])),
            owner_references: Some(vec![owner_reference(job_owner)]),
            ..ObjectMeta::default()
        },
        immutable: Some(true),
        data: Some(BTreeMap::from([(INDEX_DATA_KEY.to_string(), body)])),
        binary_data: None,
    })
}

/// The immutable trust-material `ConfigMap`, owned by the `RecoveryCatalog`.
#[must_use]
pub fn trust_config_map(
    name: &str,
    namespace: &str,
    catalog_owner: &RunnerOwner,
    trust: &TrustView,
) -> ConfigMap {
    let bundle = trust
        .keys
        .iter()
        .map(|k| k.spki_pem.trim_end().to_string())
        .collect::<Vec<_>>()
        .join("\n");
    let key_ids = trust
        .keys
        .iter()
        .map(|k| k.key_id.clone())
        .collect::<Vec<_>>()
        .join("\n");
    ConfigMap {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some(namespace.to_string()),
            labels: Some(BTreeMap::from([
                (
                    crate::check::job::LABEL_MANAGED_BY.to_string(),
                    crate::check::job::MANAGED_BY.to_string(),
                ),
                (
                    LABEL_COMPONENT.to_string(),
                    COMPONENT_CATALOG_TRUST.to_string(),
                ),
                (LABEL_CATALOG_UID.to_string(), catalog_owner.uid.clone()),
            ])),
            owner_references: Some(vec![owner_reference(catalog_owner)]),
            ..ObjectMeta::default()
        },
        immutable: Some(true),
        // PUBLIC MATERIAL ONLY. `spkiPem` is a SubjectPublicKeyInfo; nothing
        // private is ever placed in a ConfigMap, and the one-signer gate
        // (`scripts/check-one-signer.sh`) is why this crate cannot even reach a
        // private key type.
        data: Some(BTreeMap::from([
            (TRUST_BUNDLE_KEY.to_string(), bundle),
            (TRUST_KEY_IDS_KEY.to_string(), key_ids),
        ])),
        binary_data: None,
    }
}

/// `app.kubernetes.io/component` on every object this view creates.
pub const LABEL_COMPONENT: &str = "app.kubernetes.io/component";
/// The component value on a page `ConfigMap`.
pub const COMPONENT_CATALOG_PAGE: &str = "catalog-page";
/// The component value on the fence-pointer `ConfigMap`.
pub const COMPONENT_CATALOG_INDEX: &str = "catalog-index";
/// The component value on the trust `ConfigMap`.
pub const COMPONENT_CATALOG_TRUST: &str = "catalog-trust";
/// The `RecoveryCatalog` UID, as a label. **A label and never an identity** —
/// ownership is decided by `ownerReferences`, exactly as D-SEAMS **S6** requires
/// for pods.
pub const LABEL_CATALOG_UID: &str = "logweir.dev/catalog-uid";
/// A page's index, as a label, so an operator can `kubectl get cm -l`.
pub const LABEL_PAGE_INDEX: &str = "logweir.dev/catalog-page-index";

/// Whether an object already at a page's name IS this sync's page.
///
/// The same three questions [`crate::check::plan::accepts_existing`] asks, for
/// the same reasons, and a fourth: the digest annotation must match, so a page
/// left by a sync that computed a different view is refused rather than
/// adopted. **A foreign-owned object is never adopted**, whatever it contains:
/// adopting it would publish somebody else's bytes as this catalog's view.
///
/// # Errors
///
/// [`PageConflict`], naming which of the four failed and naming no content.
pub fn accepts_existing_page(
    existing: &ConfigMap,
    owner_uid: &str,
    digest: &str,
) -> Result<(), PageConflict> {
    let owned = existing
        .metadata
        .owner_references
        .as_deref()
        .unwrap_or_default()
        .iter()
        .any(|o| o.uid == owner_uid && o.controller == Some(true));
    if !owned {
        return Err(PageConflict(format!(
            "ConfigMap {} exists and is not controlled by this sync Job; a catalog page is never \
             adopted across owners",
            existing.name_any()
        )));
    }
    if existing.immutable != Some(true) {
        return Err(PageConflict(format!(
            "ConfigMap {} exists without `immutable: true`, so its content could change under a \
             reader; it is not adopted",
            existing.name_any()
        )));
    }
    let found = existing
        .metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get(PAGE_DIGEST_ANNOTATION))
        .map(String::as_str);
    if found != Some(digest) {
        return Err(PageConflict(format!(
            "ConfigMap {} exists carrying digest {} and this pass rendered {digest}; two \
             different views want one name",
            existing.name_any(),
            found.unwrap_or("<none>")
        )));
    }
    Ok(())
}

/// A page name that is taken by something this sync did not write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageConflict(pub String);

impl std::fmt::Display for PageConflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for PageConflict {}

// ===========================================================================
// The sync Job and its plan
// ===========================================================================

/// The `catalogSync` request, as the plan carries it.
///
/// **IT MOVED, EXACTLY AS THIS COMMENT SAID IT WOULD.** The shape was declared
/// here while `CheckRequest` had no variant for it; now that D2 carries both,
/// the declaration is D2's and this is a re-export, so a field added on one
/// side cannot be missing on the other. The controller still owns the two CRD
/// enums (`spec.sync.mode`, `spec.sync.deepCheck`) because they carry
/// `JsonSchema` and the published CRD is generated from them; the two `From`
/// impls below are the whole of the translation, and
/// `the_crd_and_the_plan_spell_the_two_enums_identically` asserts they spell
/// the same strings.
pub use logweir_core::check_contract::CatalogSyncRequest;

impl From<SyncMode> for logweir_core::check_contract::CatalogSyncMode {
    fn from(mode: SyncMode) -> Self {
        match mode {
            SyncMode::Index => Self::Index,
            SyncMode::Full => Self::Full,
        }
    }
}

impl From<DeepCheck> for logweir_core::check_contract::CatalogDeepCheck {
    fn from(deep: DeepCheck) -> Self {
        match deep {
            DeepCheck::None => Self::None,
            DeepCheck::ManifestDigest => Self::ManifestDigest,
            DeepCheck::SegmentSample => Self::SegmentSample,
        }
    }
}

/// The plan document — D2's own [`CheckPlan`], serialised.
///
/// **IT IS A TYPED `CheckPlan` NOW.** It was a hand-built `serde_json::Value`
/// because `CheckRequest` was a closed enum with no `catalogSync` variant, so a
/// typed value could not be constructed and the field spellings had to be
/// copied. They are no longer copied: this builds the very type the runner
/// deserialises with `deny_unknown_fields`, so a field this controller writes
/// and that runner does not know is a compile error here rather than a
/// `CheckContractMismatch` in a pod.
///
/// [`CheckPlan`]: logweir_core::check_contract::CheckPlan
///
/// # Errors
///
/// [`serde_json::Error`] if the plan does not serialise.
pub fn plan_document(
    subject_uid: &str,
    timeout_seconds: u32,
    policy_digest: Option<&str>,
    request: &CatalogSyncRequest,
) -> Result<Vec<u8>, serde_json::Error> {
    let plan = logweir_core::check_contract::CheckPlan {
        contract: CHECK_PLAN_CONTRACT.to_string(),
        contract_version: logweir_core::check_contract::CHECK_CONTRACT_VERSION,
        subject_uid: subject_uid.to_string(),
        timeout_seconds,
        policy_digest: policy_digest.map(ToString::to_string),
        request: logweir_core::check_contract::CheckRequest::CatalogSync(Box::new(request.clone())),
    };
    serde_json::to_vec(&plan)
}

/// Everything one sync Job needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncJobSpec {
    /// The Job's name — [`sync_job_name`].
    pub name: String,
    /// The namespace: the catalog's, which is the destination's.
    pub namespace: String,
    /// The `RecoveryCatalog` that owns the Job.
    pub owner: RunnerOwner,
    /// The plan `ConfigMap`'s name.
    pub plan_config_map: String,
    /// `sha256:<hex>` of the plan document.
    pub plan_sha256: String,
    /// The catalog's UID, pinned into the environment.
    pub subject_uid: String,
    /// The plan's own budget; the Job's deadline is this plus
    /// [`crate::check::job::DEADLINE_MARGIN_SECONDS`].
    pub timeout_seconds: i64,
    /// `ttlSecondsAfterFinished` — [`ttl_seconds`].
    pub ttl_seconds: i32,
    /// The ServiceAccount: `logweir-runner`.
    pub service_account_name: String,
    /// The trust `ConfigMap` to mount, when there is trust material.
    pub trust_config_map: Option<String>,
    /// The destination's complete, explicit environment.
    pub env_literal: Vec<(String, String)>,
    /// The destination's credential, by `secretKeyRef`.
    pub env_from_secret: Vec<EnvFromSecret>,
    /// The runner image, or `None` for the pin.
    pub image: Option<String>,
    /// The pull policy, or `None` for the compiled-in.
    pub image_pull_policy: Option<String>,
}

/// The sync Job.
///
/// # What is taken from the check framework and what is decided here
///
/// The argv, the `/check` mount, the plan key, the three pinned environment
/// variables, `RUST_LOG=warn`, the deadline margin and every property
/// [`crate::job::build`] holds — container name, `restartPolicy: Never`,
/// `backoffLimit: 0`, `automountServiceAccountToken: false`, the security
/// context — are the framework's and are not re-decided.
///
/// Two things are this function's, and only two:
///
/// 1. The Job name and the `logweir.dev/check-kind` label carry [`PLAN_KIND`],
///    which is now [`CheckPlanKind::CatalogSync`]'s own string rather than a
///    second spelling of it.
///
///    [`CheckPlanKind::CatalogSync`]: logweir_core::check_contract::CheckPlanKind::CatalogSync
/// 2. **`ttlSecondsAfterFinished` IS SET AT CREATION**, unlike every other
///    check Job in this tree. The framework patches a TTL on only after the
///    status commit because a check's relay lives on the pod and the TTL
///    controller removes Jobs and pods together. A sync Job's TTL is at least a
///    DAY ([`TTL_FLOOR_SECONDS`]), so it cannot race a read in the same
///    reconcile — and it is load-bearing in a way the ten-minute one is not:
///    the TTL is the ONLY thing that ever removes a page, so a Job that
///    finished without one would pin its pages forever if the controller died
///    before it could patch. Setting it at creation is what makes the "no
///    `delete` verb" design safe against a crash.
#[must_use]
pub fn build_sync_job(spec: &SyncJobSpec) -> Job {
    let mut config_map_mounts = vec![ConfigMapMount {
        volume: crate::check::job::CHECK_VOLUME.to_string(),
        config_map_name: spec.plan_config_map.clone(),
        mount_path: crate::check::job::CHECK_MOUNT_PATH.to_string(),
        items: Vec::new(),
    }];
    if let Some(trust) = spec.trust_config_map.as_ref() {
        config_map_mounts.push(ConfigMapMount {
            volume: TRUST_VOLUME.to_string(),
            config_map_name: trust.clone(),
            mount_path: TRUST_MOUNT_PATH.to_string(),
            items: Vec::new(),
        });
    }

    let mut env_literal = vec![
        (
            crate::check::job::CONTRACT_VERSION_ENV.to_string(),
            logweir_core::check_contract::CHECK_CONTRACT_VERSION.to_string(),
        ),
        (
            crate::check::job::PLAN_SHA256_ENV.to_string(),
            spec.plan_sha256.clone(),
        ),
        (
            crate::check::job::SUBJECT_UID_ENV.to_string(),
            spec.subject_uid.clone(),
        ),
        ("RUST_LOG".to_string(), "warn".to_string()),
    ];
    env_literal.extend(spec.env_literal.iter().cloned());

    let mut job = crate::job::build(&RunnerJobSpec {
        name: spec.name.clone(),
        namespace: spec.namespace.clone(),
        owner: spec.owner.clone(),
        args: crate::check::job::runner_argv(),
        deadline_seconds: spec.timeout_seconds + crate::check::job::DEADLINE_MARGIN_SECONDS,
        service_account_name: spec.service_account_name.clone(),
        secret_mounts: Vec::new(),
        config_map_mounts,
        env_from_secret: spec.env_from_secret.clone(),
        env_literal,
        plan_config_map: None,
        image: spec.image.clone(),
        image_pull_policy: spec.image_pull_policy.clone(),
    });

    let labels = BTreeMap::from([
        (
            crate::check::job::LABEL_MANAGED_BY.to_string(),
            crate::check::job::MANAGED_BY.to_string(),
        ),
        (
            crate::check::job::LABEL_COMPONENT.to_string(),
            crate::check::job::COMPONENT_CHECK.to_string(),
        ),
        (
            crate::check::job::LABEL_CHECK_KIND.to_string(),
            PLAN_KIND.to_string(),
        ),
        (
            crate::check::job::LABEL_CHECK_OWNER_UID.to_string(),
            spec.owner.uid.clone(),
        ),
    ]);
    job.metadata.labels = Some(labels.clone());
    if let Some(job_spec) = job.spec.as_mut() {
        job_spec.ttl_seconds_after_finished = Some(spec.ttl_seconds);
        let mut meta = job_spec.template.metadata.take().unwrap_or_default();
        meta.labels = Some(labels);
        job_spec.template.metadata = Some(meta);
    }
    job
}
