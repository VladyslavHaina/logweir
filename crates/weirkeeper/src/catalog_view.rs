//! The bounded Kubernetes view of a durable recovery catalog.
//!
//! # Pure
//!
//! Everything here is a function of bytes and values: the grammar of the
//! `catalogSync` result body, the two classification axes, the trust
//! re-evaluation, the page and fence-pointer materialisation, the ten status
//! counters and the names of every object the controller creates. Nothing reads
//! a clock, a socket or an API server; [`crate::controllers::recovery_catalog`]
//! does that and hands the bytes over, so a whole view is asserted over a table
//! ("5 001 points page as 2 ConfigMaps, the newest 5 000 are materialised and
//! `truncated` is true") rather than a fake Job.
//!
//! # The durable truth is in object storage; this is a window onto it
//!
//! The catalog itself is `logweir/catalog/v1/` in the destination. Kubernetes
//! carries the newest ≤ 5 000 points, the counts, the histogram and the signer
//! summary, in immutable `ConfigMap` pages owned by the sync Job. When the
//! Job's TTL fires, garbage collection takes the pages and the catalog reports
//! `ViewExpired`, which is honest, and is why there is no `delete` verb
//! anywhere.
//!
//! # Two axes, and nothing merges them
//!
//! [`Availability`] answers *can these bytes be read*, [`Verification`] *does
//! the evidence verify under trusted key material*. A point is selectable for
//! an ordinary restore only when it is `Available` and (`Verified` |
//! `VerifiedHistorical`); everything else is listed with its exact state. One
//! "healthy" boolean would offer recovery from an archive whose manifest does
//! not parse.
//!
//! # Who decides what
//!
//! The sync Job reads the archive, so it decides [`Availability`] and the
//! signature verdict ([`SignatureVerdict`]) under the key material this
//! controller mounted. It does not decide trust (whether a key is accepted,
//! retired or revoked): the controller holds the trust source.
//! [`classify_verification`] is where the two meet, and [`TrustView`] is the
//! namespace's trust from [`crate::trust::resolve`], the resolution the
//! `Approval` controller, the restore preflight and the runner's keyring use,
//! with the synthesised `legacy-roster-v1` as the fallback.

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

/// The plan kind a `RecoveryCatalog` sync runs (seam **S1**), derived from the
/// enum so the two cannot drift. Everything else about the Job (argv, mount
/// path, pinned environment, deadline margin, labels,
/// `automountServiceAccountToken: false`, the plan `ConfigMap` and its 409
/// rule) is taken from [`crate::check`].
///
/// [`CheckPlanKind`]: logweir_core::check_contract::CheckPlanKind
pub const PLAN_KIND: &str = CheckPlanKind::CatalogSync.as_str();

/// The two-letter Job-name discriminator for [`PLAN_KIND`]
/// ([`logweir_core::check_contract::CheckPlanKind::job_discriminator`]),
/// derived likewise.
pub const JOB_DISCRIMINATOR: &str = CheckPlanKind::CatalogSync.job_discriminator();

/// The contract string a check plan carries, re-exported so this module names
/// D2's spelling and never a second one.
pub use logweir_core::check_contract::CHECK_PLAN_CONTRACT;

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
            /// `None` for anything not in the table, never a fallback member:
            /// "this build does not know that state" is not "this state
            /// happened", and collapsing them would read a newer runner's
            /// `Partial` as `Available`.
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
    /// Can these bytes be read.
    ///
    /// `Missing` and `Unreadable` are different answers: a `NotFound` is "the
    /// archive does not hold this", a 403 or a timeout "this credential could
    /// not tell", and reporting the second as the first makes an operator
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
    /// Whether a point in this state may be offered for an ordinary restore:
    /// the first half of the selection rule.
    #[must_use]
    pub fn selectable(self) -> bool {
        self == Self::Available
    }
}

view_vocabulary! {
    /// What the sync Job could say about a signature, and nothing about trust.
    ///
    /// The Job holds the key material this controller mounted and the bytes it
    /// read; it can say whether a DSSE verifies under key K, not whether K is
    /// accepted, retired or revoked, which are facts about the trust source in
    /// the cluster.
    SignatureVerdict {
        Verified => "verified",
        Invalid => "invalid",
        NoEvidence => "noEvidence",
        NotAttempted => "notAttempted",
    }
}

view_vocabulary! {
    /// Does the evidence verify under trusted key material. Produced by
    /// [`classify_verification`] and nothing else.
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
    /// Whether a point in this state may be offered for an ordinary restore:
    /// the second half of the selection rule.
    #[must_use]
    pub fn selectable(self) -> bool {
        matches!(self, Self::Verified | Self::VerifiedHistorical)
    }
}

/// The selection rule, in one function so no surface writes its own.
#[must_use]
pub fn selectable(availability: Availability, verification: Verification) -> bool {
    availability.selectable() && verification.selectable()
}

/// Whether a `Backup.status.evidence.verification.result` is a verdict the
/// controller reached that is not a pass.
///
/// The catalog decides only where the controller could not look. `None`,
/// `NotAttempted` and `Pending` (an evidence-fetch Job still reading) are "I
/// have not looked" and defer to the catalog; `Valid` is a pass the catalog
/// may still narrow. Everything else (`Invalid`, `Untrusted`, or a spelling
/// this build does not know, reachable after a rollback) is a refusal no
/// catalog row may overrule; the same rule as
/// `protection::evidence_objective_met`.
///
/// `Valid` is a pass only on a basis [`ValidBasis`] admits: on `Unverified` it
/// defers like `NotAttempted`; on `RecordedBeforeRevocation`, `None`, a
/// missing basis or an unknown word it is a refusal. `basis` is read for no
/// other result.
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
/// A view is served until `viewExpiresAt`, so a row harvested before a receipt
/// was replaced or its signer revoked can still say
/// `Available`/`Verified`/`selectable` after the controller recorded the
/// `Backup` `Invalid` or `Untrusted`. Every surface that reads a view row as
/// usable (retention's keep set, the console's point list) asks this set first.
///
/// The full receipt digest decides wherever the `Backup` carries one
/// (`status.evidence.receiptSha256`, which the row's `receiptSha256` names); a
/// `Backup` with no digest joins on the archive set id (`status.backupId`), as
/// `protection::entries_for` does. Both are conservative: a match can only
/// take a point out of the usable set.
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

/// The word a refusal carries when the `Backup`'s verdict field is present but
/// not readable (not a string, or under a non-object parent): an unknown
/// verdict is a refusal, and so is an unreadable one.
pub const UNREADABLE_VERDICT: &str = "Unreadable";

/// What a `Backup`'s `status.evidence.verification.result` field holds, read
/// leniently.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VerdictField {
    /// No verdict written: the field, or a parent of it, is absent or null.
    Absent,
    /// A verdict word.
    Word(String),
    /// Something is there and it is not a verdict word.
    Unreadable,
}

/// The three facts the refusal rule reads from one `Backup`, and nothing else.
///
/// A projection, not the typed CRD: a typed `Backup` list fails whole when one
/// object does not deserialize (a newer trigger kind, an older schema, a
/// missing field), so one malformed `Backup` refuses at most its own point,
/// never the list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackupVerdictFacts {
    /// `status.backupId`, when it is a non-empty string.
    pub backup_id: Option<String>,
    /// `status.evidence.receiptSha256`, when it is a non-empty string.
    pub receipt_sha256: Option<String>,
    /// `status.evidence.verification.result`.
    pub result: VerdictField,
    /// `status.evidence.verification.trust`, read by [`ValidBasis`]: the half
    /// of the refusal rule a `Valid` needs.
    pub basis: ValidBasis,
}

impl BackupVerdictFacts {
    /// Read the three facts from an untyped `Backup` object (its JSON, or the
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

/// Walk `path`; a null or missing step is [`VerdictField::Absent`], a step
/// that is not an object where one is needed, or a leaf that is not a string,
/// is [`VerdictField::Unreadable`].
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
    /// A refused verdict word, and an [`VerdictField::Unreadable`] verdict
    /// (published as [`UNREADABLE_VERDICT`]), refuse the point their digest (or,
    /// without one, their set id) names. A refusal naming neither is counted
    /// in [`Self::unattributed`], so a caller can say the join is incomplete.
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
    /// the set" means "no listed `Backup` refused it" for every `Backup`.
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

/// What a trust source says about one key.
///
/// `Retired` and `Revoked` are states a `TrustRoster` cannot express;
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
    /// `sha256(DER SPKI)` in lowercase hex, the number `openssl` prints.
    pub key_id: String,
    /// The public key, SubjectPublicKeyInfo in PEM, carried so the bundle the
    /// sync Job mounts and the ids this controller classifies against come
    /// from one read of the trust source.
    pub spki_pem: String,
    /// Who the source says it belongs to. Display only, never authority.
    pub subject: Option<String>,
    /// The key's full lifecycle, for a `TrustPolicy` key, so
    /// [`classify_verification`] judges it with [`logweir_core::trust::decide`]
    /// itself, the judge the restore preflight and the runner apply, and the
    /// view cannot be the permissive one. `None` for a roster key, which has no
    /// lifecycle.
    pub lifecycle: Option<logweir_core::trust::TrustedKey>,
    /// When it stops being accepted for NEW evidence.
    pub not_after: Option<Time>,
    /// Its lifecycle state.
    pub state: TrustKeyState,
}

/// The trust source, projected into the one shape this view needs.
///
/// Built from the namespace's resolved trust: [`TrustView::from_resolution`]
/// projects what [`crate::trust::resolve`] answers (the governing
/// `TrustPolicy`, or the synthesised `legacy-roster-v1`), the resolution the
/// `Approval` controller, the restore preflight and the runner's keyring use.
/// So a point signed under a `TrustPolicy` key is `Verified` here too, and a
/// retired or revoked key is reflected.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TrustView {
    /// The keys, in the source's own order.
    pub keys: Vec<TrustKey>,
    /// Which source produced this — for a status message, never for a decision.
    pub source: String,
    /// Why the namespace resolved to no trust although trust is configured:
    /// two `TrustPolicy` objects claim it. `keys` is then empty, every point is
    /// `NotAttempted`, and `TrustAvailable=False` names the conflict.
    pub unresolved: Option<String>,
}

impl TrustView {
    /// The `TrustRoster`'s signing keys, every one `Active`: what
    /// `from_resolved` reproduces for the synthesised `legacy-roster-v1`
    /// (`a_roster_only_namespace_projects_exactly_as_the_roster_did`).
    ///
    /// `signingKeys`, not `approverKeys`: a key that may authorise a restore is
    /// not thereby one that may attest to one.
    ///
    /// Every entry is `Active` because the roster has no lifecycle field, and
    /// inventing one from `notAfter` would make an expired key look retired on
    /// purpose. `notAfter` is carried, and [`classify_verification`] uses it.
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
    /// * [`Resolution::Conflict`](crate::trust::Resolution::Conflict) → no
    ///   keys and [`Self::unresolved`] naming the contesting policies: a
    ///   namespace claimed twice resolves to nothing, and the catalog verifies
    ///   nothing there rather than pick a policy by sort order.
    /// * [`Resolution::Unconfigured`](crate::trust::Resolution::Unconfigured)
    ///   → no keys, under the roster's name.
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
    /// The projection, by [`logweir_core::trust::decide`]'s rules:
    ///
    /// * `Active` → `Active`, bounded by `notAfter` (a key past it verifies
    ///   what it signed inside it, as `VerifiedHistorical`).
    /// * `Retired` → `Retired`, bounded by `accepted_through()` (the earlier of
    ///   `notAfter` and `retiredAt`), so evidence signed after the retirement
    ///   is not historical.
    /// * `Revoked` for `KeyCompromise` → `Revoked`: nothing it signed is
    ///   accepted, whenever it claims to have signed it.
    /// * `Revoked` for `Superseded`/`Unspecified` → `Retired` at the
    ///   revocation's effective instant, as `decide` answers for the same key.
    ///
    /// `EvidenceSigning` keys only: a key that may authorise a restore is not
    /// thereby one that may attest to one. From a real policy, only usable keys
    /// (the PEM parses and hashes to its declared id), exactly those the runner
    /// is handed (`restore::evidence_keyring_bytes`). From the synthesised
    /// `legacy-roster-v1`, every `signingKeys` entry, `Active`, with `notAfter`
    /// as the roster wrote it: [`Self::from_roster`]'s projection.
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
    /// `false` makes every verdict [`Verification::NotAttempted`] and
    /// `TrustAvailable=False`: an installation with no trust material has not
    /// disproved anything, which differs from "the signature did not verify".
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
/// The roster's display-only `subject` is not carried.
///
/// Unless a `TrustPolicy` revoked it for compromise:
/// [`crate::trust::resolve_in`] applies every `KeyCompromise` revocation any
/// policy records to the key with the same id, whatever the source, so a
/// namespace on the roster does not list a compromised key's points
/// `Verified` while every other surface refuses them. `Revoked` wins, as in
/// [`classify_verification`]'s step 5.
fn legacy_trust_key(key: &crate::trust::ResolvedKey) -> TrustKey {
    let compromised = key.trust.state == logweir_core::trust::KeyState::Revoked
        && key.trust.reason() == logweir_core::trust::RevocationReason::KeyCompromise;
    TrustKey {
        key_id: key.trust.key_id.clone(),
        spki_pem: key.spki_pem.clone(),
        subject: None,
        lifecycle: None,
        not_after: (key.trust.not_after != crate::trust::legacy_not_after())
            .then_some(key.trust.not_after),
        state: if compromised {
            TrustKeyState::Revoked
        } else {
            TrustKeyState::Active
        },
    }
}

/// One `TrustPolicy` key; see [`TrustView::from_resolved`] for the rules.
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
/// In order:
///
/// 1. **A signature that did not verify is [`Verification::Invalid`]**
///    whatever the trust source says: trust cannot rescue bytes that do not
///    match their signature, and asking trust first would let an unlisted key
///    turn a forgery into a mere `UntrustedSigner`.
/// 2. **No evidence and not-attempted pass through**: facts about the archive
///    and the run, not trust.
/// 3. **No trust material at all is `NotAttempted`**: an installation with no
///    key has not disproved a signature, and `UntrustedSigner` would blame the
///    archive for the cluster.
/// 4. **A verified signature under a key the source does not list is
///    [`Verification::UntrustedSigner`]**, never upgraded by proximity: a
///    public key found beside an archive is a claim (`docs/keys.md`).
/// 5. **Revoked wins over everything else**: a revocation says the private
///    half is in someone else's hands.
/// 6. **A `TrustPolicy` key is judged by [`logweir_core::trust::decide`]**
///    ([`TrustKey::lifecycle`]): every window row is `decide`'s, so the view
///    agrees with the preflight and the runner by construction. Steps 5 and 7
///    are the roster's.
/// 7. **An expired or retired key still verifies what it signed while valid**:
///    [`Verification::VerifiedHistorical`]. Evidence signed after `notAfter`
///    is [`Verification::Invalid`]: the key was not accepted then either.
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
            // Signed after the key stopped being accepted, or with no instant:
            // nothing says it was inside the validity window.
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
/// | `Untrusted`, `SignedOutsideValidity` (before `notBefore`, after the accepted bound, in the future, before the window opened, or no instant) | `Invalid` |
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
/// view's vocabulary: the table above.
///
/// An allow-list: a `Valid` is read by [`ValidBasis`], the rule the
/// controller's badge uses, and a basis that is not a pass falls to the
/// `Untrusted` rows. A catch-all `(Valid, _)` arm would be one edit away from a
/// selectable point on `RecordedBeforeRevocation`.
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

/// `catalog-format=<n>`: the grammar's own version line, which the parser
/// requires. A body with no version line, or a version this build does not
/// know, is refused by name, so a newer runner cannot extend the grammar by
/// hoping this parser ignores what it does not know.
pub const FORMAT_LINE_PREFIX: &str = "catalog-format=";

/// The only [`FORMAT_LINE_PREFIX`] value this build reads.
pub const BODY_FORMAT_VERSION: u32 = 1;

/// The most bytes of `details` one body may carry.
///
/// A byte budget, because the transport's bounds are bytes:
/// [`crate::check::relay::RELAY_LIMIT_BYTES`] (8 MiB on the `pods/log` read)
/// and [`crate::check::relay::DECODER_BUDGET_BYTES`] (8 MiB over base64 part
/// frames, about 5.9 MB of raw `details`). Too much would otherwise surface as
/// a transport `ResultUnreadable`. Five megabytes sits inside the narrower with
/// headroom, and the runner is told to honour it.
pub const MAX_BODY_BYTES: usize = 5 * 1024 * 1024;

/// The most `catalog-signers` rows one body may declare: four times the
/// [`MAX_SIGNERS`] the status can carry, so an archive written by many
/// installations still reports an exact `untrustedSigner` total without an
/// unbounded list reaching this process.
pub const MAX_BODY_SIGNERS: usize = 64;

/// The most locations one point may be reported at. `locations[]` is the only
/// unbounded field in an entry, and bounding it in the grammar makes an entry
/// too large for a page a named refusal rather than a `ConfigMap` the API
/// server rejects at create.
pub const MAX_ENTRY_LOCATIONS: usize = 16;

/// The most pages a body may declare. Eight is the CRD's `status.pages`
/// `maxItems`; a body that declares more is refused rather than truncated,
/// because a truncated page set would silently drop points.
pub const MAX_BODY_PAGES: usize = 8;

/// The most topics one entry may list: the runner's own cap
/// (`logweir::check::kinds::catalog_sync::MAX_ENTRY_TOPICS`, held equal by
/// `the_grammar_this_runner_writes_is_the_grammar_the_controller_parses`). An
/// entry listing more is malformed, for the reason [`MAX_ENTRY_LOCATIONS`]
/// gives.
pub const MAX_ENTRY_TOPICS: usize = 64;

/// The most words an entry's `ownerDetection` may carry: the sources the
/// receipt format defines
/// (`logweir_core::topic_configuration::OWNER_DETECTION_SOURCES`), each at
/// most once.
pub const MAX_OWNER_DETECTION: usize =
    logweir_core::topic_configuration::OWNER_DETECTION_SOURCES.len();

/// The most consumer groups one entry may list: the runner's own cap
/// (`logweir::check::kinds::catalog_sync::MAX_ENTRY_GROUPS`, held equal by
/// `the_grammar_this_runner_writes_is_the_grammar_the_controller_parses`). An
/// entry listing more is malformed, for [`MAX_ENTRY_TOPICS`]' reason.
pub const MAX_ENTRY_GROUPS: usize = 32;

/// A point's consumer position evidence, as the runner lists it: when the
/// positions were observed, the listing word, and per selected group its
/// outcome and how many positions relate to archived data, never a position.
/// Informational: the runner's rule-3 cross-check tied it to the verified
/// receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EntryConsumerPositions {
    /// When the group capture started, epoch milliseconds.
    pub observed_from_ms: i64,
    /// When it ended, before the engine.
    pub observed_to_ms: i64,
    /// `complete` or `notComplete`.
    pub listing: String,
    /// The groups, bounded by [`MAX_ENTRY_GROUPS`]; empty when left out.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub groups: Vec<EntryGroup>,
    /// How many groups the runner left out of `groups`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub groups_omitted: Option<u32>,
}

/// One selected group of a point.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EntryGroup {
    /// The group id.
    pub group_id: String,
    /// `captured`, `excluded` or `failed`.
    pub outcome: String,
    /// The receipt's reason, when not captured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// `classic`, `consumer` or `other`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_type: Option<String>,
    /// Whether it had members at capture (captured only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active: Option<bool>,
    /// A captured group's positions, counted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub positions: Option<EntryPositionCounts>,
}

/// A captured group's positions, counted by what they say about archived data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EntryPositionCounts {
    /// Related to archived data (within the archive, or at its end).
    pub related: u32,
    /// Captured, not related (before or beyond the archive, before the log
    /// start, or with nothing archived).
    pub not_related: u32,
    /// No committed offset: never offset 0.
    pub never_committed: u32,
    /// Above the partition's end at capture.
    pub beyond_end: u32,
    /// Not read.
    pub failed: u32,
    /// A partition the capture did not observe.
    pub not_observed: u32,
}

/// One topic of a point, as the runner lists it: the record's layout and how
/// its configuration is held, never a configuration value. Informational: two
/// observations of one point are not compared on it (the runner's rule-3
/// cross-check already tied it to the verified receipt).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EntryTopic {
    /// The source topic's name.
    pub name: String,
    /// The source's partition count, when recorded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partitions: Option<u32>,
    /// The source's replication factor, when recorded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replication_factor: Option<u32>,
    /// The capture coverage word, when recorded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_coverage: Option<String>,
    /// The declarative owner's kind, when one manages the topic.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
    /// The topic's schema dependency, when the point's record carries one
    /// (record format 1.5.0). Absent is not assessed, never "not
    /// schema-dependent".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_dependency: Option<EntrySchemaDependency>,
}

/// The most schema ids one topic's entry may list: the receipt format's own
/// cap (`logweir_core::schema_dependency::SCHEMA_IDS_LISTED`, held equal below
/// and by the runner's
/// `the_grammar_this_runner_writes_is_the_grammar_the_controller_parses`). An
/// entry listing more is malformed and skipped.
pub const MAX_ENTRY_SCHEMA_IDS: usize = 16;
const _: () = assert!(MAX_ENTRY_SCHEMA_IDS == logweir_core::schema_dependency::SCHEMA_IDS_LISTED);

/// One topic's schema dependency as the runner lists it: the
/// receipt's verdict and how it was reached, the DEPENDENT sides, and the
/// schema ids they name — what a restore review needs to say "registry not
/// captured" and which ids the applications will ask a registry for. The ids
/// are bytes 1-4 of framed keys or values: a registry's schema id for real
/// Confluent framing, but for the stated residual (big-endian 64-bit integer
/// keys from 2^24 to 2^56) four bytes of the key itself. Nothing else of a
/// payload is listed. Copied from a point record the runner cross-checked
/// against its verified receipt (rule 3).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EntrySchemaDependency {
    /// `schemaDependent`, `notDetected` or `notAssessed`.
    pub verdict: String,
    /// `sampled` or `complete`, for a judged topic.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub basis: Option<String>,
    /// `noRecords`, `segmentUnreadable`, `segmentTooLargeForDetection` or
    /// `detectionTimeBudgetExceeded`, for a `notAssessed` one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// The dependent sides, `key` and/or `value`, in that order.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sides: Vec<String>,
    /// The ids the dependent sides name, merged, ascending, at most
    /// [`MAX_ENTRY_SCHEMA_IDS`].
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub schema_ids: Vec<u32>,
    /// Whether the record names more ids than `schemaIds` lists.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub schema_ids_omitted: bool,
}

/// One point, as the sync Job reports it.
///
/// The receipt-derived facts are the binding: the point id, the receipt digest
/// and the manifest digest are what a later restore re-checks. Everything else
/// is for display and selection.
///
/// `topics` is bounded, not absent: an entry stays small so 5 000 fit in two
/// `ConfigMap`s, so it lists at most [`MAX_ENTRY_TOPICS`] topics (more is
/// malformed and skipped), and the runner drops the list (counting it in
/// `topicsOmitted`) before it would push a point out of the body. The full
/// topic set lives in the point's signed record in object storage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunnerEntry {
    /// `lwp1-<32 hex>`, content-derived from the receipt bytes.
    pub point_id: String,
    /// The archive set identifier. Two receipts under one `backupId` are two
    /// points.
    pub backup_id: String,
    /// The run that wrote the receipt.
    pub run_id: String,
    /// The recovery point, in epoch milliseconds: the record's
    /// `capture.started_at`.
    pub recovery_point_at_ms: i64,
    /// The covered window's start, in epoch milliseconds.
    pub covered_from_ms: i64,
    /// The covered window's end, EXCLUSIVE (invariant I22).
    pub covered_to_ms: i64,
    /// Where the bytes could be read from, and how each place fared: one entry
    /// per location holding the same receipt (an archive copied to a second
    /// bucket is one point in two places). A location with no `availability`
    /// of its own inherits the entry's. Bounded by [`MAX_ENTRY_LOCATIONS`].
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub locations: Vec<EntryLocation>,
    /// The receipt's key under `logweir/`.
    pub receipt_key: String,
    /// `sha256:<hex>` of the receipt bytes: the binding the short id displays.
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
    /// What the Job could say about the signature, not about trust.
    pub signature: SignatureVerdict,
    /// The key the signature verified under, or the key the record claims.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signer_key_id: Option<String>,
    /// A one-sentence remedy for a state that is not selectable. Never a
    /// credential, a principal or log content.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remedy: Option<String>,
    /// The point's topics with their recorded layout, bounded by
    /// [`MAX_ENTRY_TOPICS`]. Empty is not published (an older runner, a record
    /// before 1.3.0, a point that is not `Available`), never "no topics".
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub topics: Vec<EntryTopic>,
    /// How many topics the runner left out of `topics`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topics_omitted: Option<u32>,
    /// Where the run looked for declarative owners (`declared`,
    /// `kafkaTopicResources`), beside `topics` and only there. An empty list
    /// says no owner was looked for, so a topic without an `owner` is "not
    /// checked", never "applied through the admin API"; absent is not
    /// published. At most [`MAX_OWNER_DETECTION`] words; more is malformed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_detection: Option<Vec<String>>,
    /// The point's consumer position evidence. Absent is not published (no
    /// group selected, an older runner or record, a point that is not
    /// `Available`). Its groups are bounded by [`MAX_ENTRY_GROUPS`]; an entry
    /// listing more is malformed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consumer_positions: Option<EntryConsumerPositions>,
}

impl RunnerEntry {
    /// The receipt-derived facts two records of one identity must agree on.
    ///
    /// `locations`, `recordedAt`, `signature`, `signerKeyId` and `remedy` are
    /// out: two records differing only in where they were found are one point
    /// in two places, which is the point of a content-derived identity.
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
/// Availability merges best-of across locations: an archive copied to a second
/// bucket is one point in two places, and taking the worst would hide a fully
/// recoverable point because a second copy went missing. Each location keeps
/// its own verdict, and the degraded ones are named in `remedy`.
///
/// The signature half keeps worst-of, deliberately: bytes that fail
/// verification in one place are evidence about the point, not the place.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EntryLocation {
    /// `s3://<bucket>/<prefix>`: bucket and prefix only. No endpoint, region
    /// or credential, and not part of the identity.
    pub location_id: String,
    /// What was found here. Absent inherits the entry's own availability, as a
    /// runner reporting one observation at one place writes.
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

/// One location in a published view entry, with its verdict resolved, so a
/// reader never has to re-apply the inheritance rule.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResolvedLocation {
    /// See [`EntryLocation::location_id`].
    pub location_id: String,
    /// What was found there.
    pub availability: Availability,
}

/// One point as it is written into a page: the runner's entry with the
/// controller's [`Verification`] in place of the Job's [`SignatureVerdict`].
///
/// `selectable` is materialised, not derived by the reader: a UI recomputing
/// it would be a second implementation of the rule that matters most.
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
    /// The first axis.
    pub availability: Availability,
    /// The second axis, after the controller's trust re-evaluation.
    pub verification: Verification,
    /// See [`RunnerEntry::signer_key_id`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signer_key_id: Option<String>,
    /// `availability.selectable() && verification.selectable()`.
    pub selectable: bool,
    /// See [`RunnerEntry::remedy`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remedy: Option<String>,
    /// See [`RunnerEntry::topics`].
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub topics: Vec<EntryTopic>,
    /// See [`RunnerEntry::topics_omitted`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topics_omitted: Option<u32>,
    /// See [`RunnerEntry::owner_detection`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_detection: Option<Vec<String>>,
    /// See [`RunnerEntry::consumer_positions`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consumer_positions: Option<EntryConsumerPositions>,
}

/// What the sync counted over the whole walk, not only over the window (the
/// newest `viewLimit` points), which makes `truncated` meaningful and lets the
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
    /// How many of `unreadable` are a document over its read bound: a size,
    /// never a permission or transport failure. Absent (zero) from an older
    /// runner, which then says nothing about why.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub unreadable_over_read_cap: i64,
    /// How many of `unreadable` are a document that is not what its key names.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub unreadable_malformed: i64,
}

#[allow(clippy::trivially_copy_pass_by_ref)] // serde's `skip_serializing_if` passes `&T`.
fn is_zero(n: &i64) -> bool {
    *n == 0
}

/// Why points could not be read, in words that fit the cause: the `Synced`
/// condition's message for a walk whose `unreadable` is not zero.
///
/// A document's size and content are named as such (no grant or network change
/// would list an over-cap record); only the rest is called a permission or
/// transport failure. An older runner's body, with no sub-counts, reads as
/// before.
#[must_use]
pub fn unreadable_message(counts: &RunnerCounts) -> String {
    let over = counts.unreadable_over_read_cap.clamp(0, counts.unreadable);
    let malformed = counts
        .unreadable_malformed
        .clamp(0, counts.unreadable - over);
    let rest = counts.unreadable - over - malformed;
    let mut parts: Vec<String> = Vec::new();
    if over > 0 {
        parts.push(format!(
            "{over} because a document is larger than the bound Logweir reads for one (its \
             size: no permission or network change lists it; the point's entry states the size \
             and the bound)"
        ));
    }
    if malformed > 0 {
        parts.push(format!(
            "{malformed} because an object is not the document its key names"
        ));
    }
    if rest > 0 {
        parts.push(format!(
            "{rest} for a permission or transport failure, which is NOT the same as absent"
        ));
    }
    format!(
        "{} of {} points could not be read: {}; those entries say Unreadable and never Missing",
        counts.unreadable,
        counts.total,
        parts.join("; ")
    )
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
/// `trusted` is absent: the Job does not decide trust. The controller adds it
/// in [`signer_summaries`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunnerSigner {
    /// `sha256(DER SPKI)` in lowercase hex.
    pub key_id: String,
    /// What the record said the principal was. A hint: unsigned metadata,
    /// never authority.
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
/// Every variant is a fact about the body and carries no log content: a body
/// that does not decode is exactly the input that must not reach a status.
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
    /// The closed code this is reported as: always
    /// [`logweir_core::check_contract::CheckCode::ResultUnreadable`] (seam
    /// **S1**: the closed vocabulary, no new strings). The sub-cause is the
    /// [`Display`] above.
    #[must_use]
    pub fn code(&self) -> logweir_core::check_contract::CheckCode {
        logweir_core::check_contract::CheckCode::ResultUnreadable
    }
}

/// The digest a page header declares, over the page's own entry lines.
///
/// Over the raw line bodies, each followed by `\n`: the JSON after
/// `catalog-entry=`, byte for byte, never a re-serialisation. The check is
/// that the bytes this controller read are the bytes the Job wrote: transport
/// integrity, not authorization. The receipt's own signature is the
/// verification root.
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
    // The byte budget first, before a line is scanned: a body that is too
    // large is refused as too large, not as its first malformed line.
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
            // A repeat is an error, not last-wins: two `catalog-counts=` lines
            // mean the runner disagreed with itself, and keeping one would
            // publish a number nobody can attribute to an observation.
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
        // whatever else the runner wrote, as the frame decoder does.
    }

    // The version line is required: a body with no declared grammar is one
    // this build cannot promise it read as the writer meant.
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
    // 1..=n, in order, each exactly once, and every header agreeing on `n`.
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
        // The digest before the parse: a page whose bytes did not survive the
        // log read is refused as a page, so a truncated read contributes no
        // entries. It covers the entry lines only; the summary lines are
        // covered by the frame stream's own digest, verified before this
        // function is called.
        if page_digest(&raw) != declared_sha256 {
            return Err(BodyError::PageDigestMismatch { index });
        }
        let mut entries = Vec::with_capacity(raw.len());
        let mut skipped = 0u32;
        for body in &raw {
            match serde_json::from_str::<RunnerEntry>(body) {
                // An unbounded `locations[]` is a malformed entry: an entry
                // whose rendered line cannot fit a page cannot be published.
                Ok(entry) if entry.locations.len() > MAX_ENTRY_LOCATIONS => {
                    skipped = skipped.saturating_add(1);
                }
                // The same rule for `topics[]`.
                Ok(entry) if entry.topics.len() > MAX_ENTRY_TOPICS => {
                    skipped = skipped.saturating_add(1);
                }
                // And for `ownerDetection`.
                Ok(entry)
                    if entry
                        .owner_detection
                        .as_ref()
                        .is_some_and(|d| d.len() > MAX_OWNER_DETECTION) =>
                {
                    skipped = skipped.saturating_add(1);
                }
                // And for a topic's schema ids and sides.
                Ok(entry)
                    if entry.topics.iter().any(|t| {
                        t.schema_dependency.as_ref().is_some_and(|d| {
                            d.schema_ids.len() > MAX_ENTRY_SCHEMA_IDS || d.sides.len() > 2
                        })
                    }) =>
                {
                    skipped = skipped.saturating_add(1);
                }
                // And for the consumer groups.
                Ok(entry)
                    if entry
                        .consumer_positions
                        .as_ref()
                        .is_some_and(|c| c.groups.len() > MAX_ENTRY_GROUPS) =>
                {
                    skipped = skipped.saturating_add(1);
                }
                Ok(entry) => entries.push(entry),
                // Skipped and counted, never fatal: a malformed entry is one
                // point this build cannot show, not a failed sync. The count
                // reaches the status message.
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

/// Collapse entries that share one point id.
///
/// * **The same receipt found in two locations is one point with two
///   locations.** Identity is `sha256(receipt bytes)`, so a copied archive
///   yields the same id; two rows would claim two recovery points.
/// * **Two records of one identity that disagree about a receipt-derived fact
///   are a [`Availability::Conflict`]**, on the merged row, with the union of
///   their locations. Neither is preferred: `logweir/` objects are never
///   rewritten, so a disagreement means something wrote a second record.
/// * Two receipts under one `backupId` have different point ids and are two
///   rows.
///
/// The two axes merge in opposite directions:
///
/// * **Availability merges best-of.** A point present in bucket A and absent
///   from bucket B is still recoverable from A. Each location keeps its own
///   verdict in [`EntryLocation`], and the degraded ones are named in
///   `remedy`.
/// * **The signature merges worst-of**, with the key id following the worst
///   verdict: bytes that fail verification in one place are evidence about the
///   point, and `selectable` must never survive it.
/// * **`Conflict` overrides both**, applied last: it is a fact about the
///   records, not any location.
///
/// The output is sorted newest recovery point first, with the point id as the
/// tie-break, so the order is total and the page set reproducible.
#[must_use]
pub fn merge_entries(entries: Vec<RunnerEntry>) -> Vec<RunnerEntry> {
    let mut by_id: BTreeMap<String, RunnerEntry> = BTreeMap::new();
    let mut conflicted: BTreeSet<String> = BTreeSet::new();
    for mut entry in entries {
        // Resolve every location's verdict against the observation once, here,
        // so the merge never needs to know which side an entry came from.
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
                        // One place reported twice keeps the worse verdict: two
                        // observations of the same bucket are two attempts at
                        // one thing.
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
                // The entry-level verdict is not decided here: it is the `max`
                // over `kept.locations`, computed once below after every
                // observation is folded in.
                if worse_signature(entry.signature, kept.signature) {
                    kept.signature = entry.signature;
                    kept.signer_key_id = entry.signer_key_id;
                }
                // A copy that lists the topics fills a kept entry that does
                // not. Both lists come from records cross-checked against the
                // one verified receipt, so two non-empty lists are the same.
                if kept.topics.is_empty() && !entry.topics.is_empty() {
                    kept.topics = entry.topics;
                    kept.topics_omitted = entry.topics_omitted;
                    // Where the run looked for owners travels with the list it
                    // qualifies.
                    kept.owner_detection = entry.owner_detection;
                } else if kept.topics.is_empty() && kept.topics_omitted.is_none() {
                    kept.topics_omitted = entry.topics_omitted;
                }
                // The same rule for the consumer position summary: a copy that
                // lists the groups fills one that does not, and any summary
                // fills none.
                match (&kept.consumer_positions, &entry.consumer_positions) {
                    (None, Some(_)) => kept.consumer_positions = entry.consumer_positions,
                    (Some(k), Some(e)) if k.groups.is_empty() && !e.groups.is_empty() => {
                        kept.consumer_positions = entry.consumer_positions;
                    }
                    _ => {}
                }
            }
        }
    }
    let mut out: Vec<RunnerEntry> = by_id.into_values().collect();
    for entry in &mut out {
        // The entry-level verdict is the best any location reached; with no
        // locations it is the observation's own.
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

/// The entry's remedy, naming every location that is not
/// [`Availability::Available`]: a best-of merge would otherwise drop which copy
/// is broken. Names only: a location id is a bucket and a prefix, never an
/// endpoint, a region or a credential.
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

/// The hard ceiling on `sync.viewLimit`, whatever the spec says.
pub const MAX_VIEW_ENTRIES: usize = 5000;

/// Entry bytes one page `ConfigMap` may carry.
///
/// 768 KiB against the API server's 1 MiB object limit; the rest carries the
/// object's metadata, annotations and second data key. An overflowing page
/// would be rejected at create with a message about etcd, not the catalog.
pub const PAGE_MAX_BYTES: usize = 768 * 1024;

/// The most pages a view may have: the CRD's `status.pages` `maxItems`.
pub const MAX_PAGES: usize = 8;

/// The most days `status.histogram` may carry: the CRD's `maxItems`.
pub const MAX_HISTOGRAM_DAYS: usize = 400;

/// The most signers `status.signers` may carry: the CRD's `maxItems`.
pub const MAX_SIGNERS: usize = 16;

/// The `ConfigMap` key holding one page's entries, one compact JSON per line.
pub const PAGE_DATA_KEY: &str = "entries.jsonl";

/// The `ConfigMap` key holding the fence-pointer document.
pub const INDEX_DATA_KEY: &str = "index.json";

/// `sha256:<hex>` over a page's `entries.jsonl`, on the page object itself.
pub const PAGE_DIGEST_ANNOTATION: &str = "logweir.dev/catalog-page-sha256";

/// The point-id range and time range of one page: the fence pointer.
///
/// A fence pointer excludes pages and never proves one holds anything:
/// `minPointId`/`maxPointId` bound the ids, so an id outside `[min, max]` may
/// skip the page, but entries are ordered by recovery point, not id, so the
/// range is not dense (no false negatives, false positives allowed).
/// `newestMs`/`oldestMs` are the same idea on the axis the view is ordered by,
/// where the bound is tight.
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
    /// opposed to `viewLimit`, so "the view is a window" and "this build could
    /// not fit the window" stay distinguishable.
    pub dropped_for_space: usize,
    /// How many entries were refused because one rendered line does not fit a
    /// page at all; counted, rather than a `ConfigMap` the API server rejects
    /// at create, which would be a requeue loop and not a verdict.
    pub dropped_oversized: usize,
    /// How many of the materialised entries are `Revoked`, and how many
    /// `VerifiedHistorical`: the two states `status.counts` has no field for.
    pub window: WindowStates,
}

/// The states of the materialised window that the ten status counters cannot
/// carry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct WindowStates {
    /// Points whose signing key this installation has revoked: `selectable:
    /// false` inside the page, and otherwise in no adverse number on the
    /// status.
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
    /// Entry bytes per page: [`PAGE_MAX_BYTES`] in production, smaller in a
    /// test that wants a second page without 700 KiB of fixture.
    pub page_max_bytes: usize,
    /// The most pages: [`MAX_PAGES`].
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

/// Turn the walk's entries into pages. Pure, and the function every "large
/// catalog" property is stated over.
///
/// The entries are merged ([`merge_entries`]), the newest `viewLimit` kept,
/// each re-classified against the trust source and rendered, and the
/// renderings packed into pages by byte budget. `total` is the whole walk's
/// count, which decides `truncated`.
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
        // One line that cannot fit a page is refused, not placed in an
        // over-budget ConfigMap the API server rejects at create.
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
        // `truncated` is about the archive, not the pages: the view is a
        // window whenever the walk's `total` or the de-duplicated count
        // exceeds what was placed.
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
        topics: entry.topics,
        topics_omitted: entry.topics_omitted,
        owner_detection: entry.owner_detection,
        consumer_positions: entry.consumer_positions,
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

/// The counters, plus the states the v1alpha1 status has no field for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tally {
    /// What goes on `status.counts`.
    pub counts: CatalogCounts,
    /// `(state, count, scope)` for every state the ten fields cannot carry, in
    /// a stable order, named in the `Synced` condition message rather than
    /// dropped. `scope` is [`SCOPE_WALK`] or [`SCOPE_VIEW`]: some numbers are
    /// only known for the materialised window, and saying which keeps a bounded
    /// fact from becoming a wrong one.
    pub unrepresented: Vec<(&'static str, i64, &'static str)>,
}

/// A residue count that covers everything the sync walked.
pub const SCOPE_WALK: &str = "the archive";
/// A residue count that covers only the materialised window: the verification
/// axis is re-decided here, so it is only known for the entries this
/// controller re-classified.
pub const SCOPE_VIEW: &str = "the materialised view";

/// Project the walk's counts and signer summary onto the CRD's ten counters.
///
/// The ten fields are a projection, not a partition: seven availability and
/// seven verification states share ten fields across both axes.
///
/// * **`untrustedSigner` is computed here, not by the Job**: whether a key is
///   accepted is a fact about the trust source. Summing `signers[].points`
///   over untrusted keys covers the whole walk, not only the window.
/// * **`unverified` is `notAttempted` plus `noEvidence`**: both mean "no
///   signature verdict was reached".
/// * **`Partial`, `Revoked` and `VerifiedHistorical` have no field**: they are
///   returned in [`Tally::unrepresented`] for the condition message, and are
///   why the availability fields do not sum to `total`.
#[must_use]
pub fn tally(counts: &RunnerCounts, signers: &[SignerSummary], window: WindowStates) -> Tally {
    // The full signer list, before [`bounded`] truncates it: `untrustedSigner`
    // covers the whole walk, and summing over the sixteen displayable rows
    // would under-report an archive written by seventeen installations.
    let untrusted: i64 = signers
        .iter()
        .filter(|s| s.trusted == Some(false))
        .map(|s| s.points.unwrap_or(0))
        .sum();
    let mut unrepresented = Vec::new();
    if counts.partial > 0 {
        unrepresented.push((Availability::Partial.as_str(), counts.partial, SCOPE_WALK));
    }
    // A revoked point is `selectable: false` inside the page and would land in
    // no adverse number otherwise: `invalid` is the runner's signature verdict,
    // `unverified` notAttempted + noEvidence, `untrustedSigner` unlisted keys.
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
/// Bounded to [`MAX_SIGNERS`], untrusted keys first, so the bound never drops
/// the unknown signer an administrator must act on. Within each group, by
/// point count descending, then key id, so the list is stable across passes.
#[must_use]
pub fn signer_summaries(signers: &[RunnerSigner], trust: &TrustView) -> Vec<SignerSummary> {
    let mut rows: Vec<SignerSummary> = signers
        .iter()
        .map(|s| SignerSummary {
            key_id: s.key_id.clone(),
            principal_hint: s.principal_hint.clone(),
            points: Some(s.points),
            // Never `None`, which would read as "not known yet": with no trust
            // material every key is untrusted, and the `TrustAvailable`
            // condition says why.
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

/// Whether this installation accepts evidence from a key.
///
/// Listed is not accepted: a `Revoked` key's private half is in someone else's
/// hands, and `trusted: true` would tell an operator a compromised signer is
/// fine. `Retired` is accepted: the evidence it signed while valid still
/// verifies (`VerifiedHistorical`).
#[must_use]
pub fn accepts(trust: &TrustView, key_id: &str) -> bool {
    matches!(
        trust.key(key_id).map(|k| k.state),
        Some(TrustKeyState::Active | TrustKeyState::Retired)
    )
}

/// [`signer_summaries`]'s output, cut to what `status.signers` can carry.
///
/// A separate step, so the counters are summed over the full list first.
/// Untrusted keys are already first, so the surviving rows are the ones an
/// administrator must act on.
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
    // Folded with `+=`, never dropped: one day reported in two shards must not
    // lose points from the histogram while `counts.total` includes them.
    // `dedup_by` sees `(later, earlier)` and keeps the earlier element, so the
    // sum accumulates onto that one.
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

/// How many hex characters of `sha256(catalog uid)` every name below carries.
///
/// Twenty, as [`crate::check::job::OWNER_UID_HEX_CHARS`], for the same reason:
/// a name derived from the object's name can be made unschedulable by naming
/// the object badly (a `<catalog>-g<generation>-p<index>` name overflows the
/// 63-character `batch.kubernetes.io/job-name` label at a 40-character catalog
/// name). The real names are published in `status.pages[].configMapName` and
/// `status.indexConfigMap`, so no consumer computes one.
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
    /// The generation token this trigger produces: short, deterministic and
    /// DNS-label safe. Deterministic is the property: two reconciles of one
    /// trigger compute one name, so the second gets **409 `AlreadyExists`**
    /// rather than creating a second sync Job, as [`crate::slot`] does for
    /// scheduled backups. That makes `syncRequest` idempotent without a lock.
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
/// `None` when the interval is zero: `sync.intervalSeconds: 0` is "manual
/// only", which has no slots.
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

/// The sync Job's name: [`sync_stem`].
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
/// Owned by the `RecoveryCatalog`, not the sync Job: it is reused by every
/// sync while the trust source's generation is unchanged, so a Job's TTL
/// would make each sync re-render it under a new owner and get a 409 it could
/// not adopt. It carries only public key material, and is collected with the
/// catalog.
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

/// The floor on a sync Job's `ttlSecondsAfterFinished`: one hour.
///
/// One sync Job lives per slot, so `ttl / intervalSeconds` generations are
/// alive at once; a day's floor kept 24 at an hourly cadence (some 72 MB of
/// etcd per catalog at 5 000 points) and 288 at the 300 s floor. An hour
/// bounds it: with the CEL floor of 300 s
/// ([`crate::crds::recovery_catalog::J3_INTERVAL_FLOOR_RULE`]) at most 12
/// generations coexist, and at an hourly cadence exactly 3. A manual-only
/// catalog keeps its view for an hour after its last sync and republishes on
/// the next request.
pub const TTL_FLOOR_SECONDS: i32 = 3_600;

/// `ttlSecondsAfterFinished` for a sync Job: `max(3 × interval, 3600)`.
///
/// Three intervals, so a view outlives two missed syncs, with an hour's floor
/// so a manual-only catalog (`intervalSeconds: 0`) still has one. The TTL is
/// the only thing that removes a page, a fence pointer or a plan (`delete` is
/// granted on nothing), so it is also the only bound on how many exist.
#[must_use]
pub fn ttl_seconds(interval_seconds: i32) -> i32 {
    interval_seconds.saturating_mul(3).max(TTL_FLOOR_SECONDS)
}

/// How many view generations can be alive at once at this cadence.
///
/// `ttl / interval`, stated as a function so the documentation and the test
/// read one arithmetic.
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

/// Whether the view is stale: older than `2 × interval`.
///
/// A manual-only catalog is never stale by the clock: nothing was promised
/// about when it would sync, and `Stale` would report the configuration as a
/// fault.
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

/// An owner reference that does not block deletion.
///
/// `blockOwnerDeletion: false` on every page, the fence pointer and the trust
/// bundle: `true` asks for `update` on the owner's `finalizers` under the
/// `OwnerReferencesPermissionEnforcement` admission plugin, which this
/// `ClusterRole` grants on nothing, and blocking the deletion of a Job whose
/// TTL has fired is the opposite of what the garbage-collection design wants.
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
                // The catalog's UID, not the Job's: the label lets an operator
                // find every object of one catalog with `kubectl get cm -l`.
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
        // Immutable, so `status.pages[].sha256` still means something to a
        // reader that fetched the page a minute later.
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
                // See `page_config_map`: the catalog's UID.
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
        // Public material only: `spkiPem` is a SubjectPublicKeyInfo, and the
        // one-signer gate (`scripts/check-one-signer.sh`) keeps this crate from
        // reaching a private key type.
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
/// The `RecoveryCatalog` UID, as a label. A label, never an identity:
/// ownership is decided by `ownerReferences` (seam **S6**).
pub const LABEL_CATALOG_UID: &str = "logweir.dev/catalog-uid";
/// A page's index, as a label, so an operator can `kubectl get cm -l`.
pub const LABEL_PAGE_INDEX: &str = "logweir.dev/catalog-page-index";

/// Whether an object already at a page's name IS this sync's page.
///
/// The three questions [`crate::check::plan::accepts_existing`] asks, and a
/// fourth: the digest annotation must match, so a page from a sync that
/// computed a different view is refused. A foreign-owned object is never
/// adopted, whatever it contains: that would publish someone else's bytes as
/// this catalog's view.
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

/// The `catalogSync` request, as the plan carries it.
///
/// The declaration is `logweir-core`'s, re-exported, so a field added on one
/// side cannot be missing on the other. The controller owns the two CRD enums
/// (`spec.sync.mode`, `spec.sync.deepCheck`) because they carry `JsonSchema`;
/// the two `From` impls below are the whole translation, and
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

/// The plan document: the core [`CheckPlan`], serialised.
///
/// The very type the runner deserialises with `deny_unknown_fields`, so a
/// field this controller writes that the runner does not know is a compile
/// error here, not a `CheckContractMismatch` in a pod.
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
    /// The Job's name: [`sync_job_name`].
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
    /// `ttlSecondsAfterFinished`: [`ttl_seconds`].
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
/// The argv, the `/check` mount, the plan key, the three pinned environment
/// variables, `RUST_LOG=warn`, the deadline margin and every property
/// [`crate::job::build`] holds (container name, `restartPolicy: Never`,
/// `backoffLimit: 0`, `automountServiceAccountToken: false`, the security
/// context) are the check framework's. Two things are decided here:
///
/// 1. The Job name and the `logweir.dev/check-kind` label carry [`PLAN_KIND`],
///    [`CheckPlanKind::CatalogSync`]'s own string.
///
///    [`CheckPlanKind::CatalogSync`]: logweir_core::check_contract::CheckPlanKind::CatalogSync
/// 2. **`ttlSecondsAfterFinished` is set at creation**, unlike every other
///    check Job, whose TTL is patched after the status commit because its
///    relay lives on the pod. A sync Job's TTL is at least an hour
///    ([`TTL_FLOOR_SECONDS`]), so it cannot race a read in the same reconcile,
///    and it is the only thing that ever removes a page: a Job that finished
///    without one would pin its pages for ever if the controller died before
///    patching. Setting it at creation makes the "no `delete` verb" design safe
///    against a crash.
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
        // No `RecoveryCatalog` field states resources for its sync Job.
        resources: None,
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
