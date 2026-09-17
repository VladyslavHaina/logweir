//! `catalogSync` — D3 §5.3's bounded, read-only walk of the durable recovery
//! catalog, relayed as the body `docs/kubernetes.md` §7d specifies.
//!
//! # What this kind is for
//!
//! A `RecoveryCatalog` indexes one destination's `logweir/catalog/v1/` tree.
//! The controller cannot reach object storage — a destination's credential is
//! projected into runner pods and never into the controller (D2 §3.8) — so it
//! starts one check Job per sync slot and reads what that Job prints. **This
//! module is the printer.** Everything it emits is parsed by
//! `weirkeeper::catalog_view::parse_body`, and the two are held together by
//! `the_catalog_sync_body_is_pinned_for_the_controllers_parser` in
//! `crates/logweir/tests/check_cli.rs` and by
//! `the_runners_pinned_body_is_one_this_parser_reads` in
//! `crates/weirkeeper/tests/catalog_controller.rs`. Neither crate depends on
//! the other; both pin the same bytes.
//!
//! # The four things it may not do
//!
//! 1. **It never writes and never deletes.** Not even the create-only
//!    readiness marker a `destinationAccess` may write: the handle is built
//!    through [`Wiring::objects`], which returns `Store::read_only_with`, and
//!    a read-only `Store` refuses every put before it checks anything else.
//! 2. **It never decides trust.** It reports whether a receipt's DSSE verified
//!    and under which key id. Whether that key is one this installation
//!    accepts, has retired or has revoked is the controller's, against the
//!    trust source, and `weirkeeper::catalog_view::classify_verification` is
//!    where it happens (D3 §5.3, §7.4).
//! 3. **It never verifies against nothing.** With no trust bundle mounted
//!    every point is [`SignatureVerdict::NotAttempted`] — the honest answer
//!    for an installation that holds no key material, and the one that keeps a
//!    forgery from being reported as `verified`.
//! 4. **It never reports "absent" for "could not tell".** A definite
//!    `NotFound` is [`Availability::Missing`]; every other storage failure is
//!    [`Availability::Unreadable`]. That distinction is the reason there are
//!    seven availability states and not two.
//!
//! # The body is emitted, or it is not emitted at all
//!
//! A body that parses is a body the controller PUBLISHES: it replaces
//! `status.pages`, `status.counts` and `status.histogram` wholesale. So a sync
//! that could not open the destination emits **no `details` stream at all**
//! rather than a well-formed body declaring zero points — which the controller
//! would publish as "this archive holds nothing". The controller reads the
//! absence as `ResultUnreadable`, keeps the previous view, and the cause
//! travels in this result's own `destination.archiveListable` row.
//!
//! # What is redacted, and what is deliberately not
//!
//! An entry line is a document of REFERENCES and DIGESTS: a point id, a
//! receipt key, a `sha256:<64 hex>` binding, a `s3://bucket/prefix` location.
//! The controller needs every one of them byte for byte — the digest is what a
//! later restore re-checks — and `check_contract::redact`'s last rule removes
//! any base64-or-hex run of 40 characters or more, so running it over an entry
//! would turn `receiptSha256` into `[redacted]` and make the body useless. It
//! is the same exception `crate::check::kinds::evidence` writes down for
//! `EvidenceObjectResult::key`, for the same reason, and it is bounded the
//! same way:
//!
//! * every string copied out of the archive passes [`redact_reference`], which
//!   is every redaction rule EXCEPT the long-run one — so a `s3://k:secret@b`
//!   location or an `AKIA…` inside a key is still removed;
//! * the two FREE-TEXT fields, an entry's `remedy` and a signer's
//!   `principalHint`, pass the whole `check_contract::redact`, cap included;
//! * nothing else is emitted. The remedies are a fixed table in this file and
//!   carry no adopter bytes at all.
//!
//! # Budgets
//!
//! Bytes and points, never line counts (`docs/kubernetes.md` §7d): at most
//! [`MAX_BODY_BYTES`] of `details`, at most the plan's `viewLimit` entry
//! lines, at most [`MAX_BODY_SIGNERS`] signer rows, at most
//! [`MAX_ENTRY_LOCATIONS`] locations on one point and at most
//! [`MAX_BODY_PAGES`] pages. The WALK is bounded separately, by the plan's
//! `maxObjectsPerRun` and by the check's own deadline, and what it did not
//! reach is recorded in `catalog-cursor=`.

use std::collections::BTreeMap;

use chrono::{DateTime, Datelike, NaiveDate, SecondsFormat, Utc};
use logweir_core::backup_receipt::BackupReceipt;
use logweir_core::check_contract::{
    apply_rules, redact, redaction_rules, CatalogDeepCheck, CatalogSyncMode, CatalogSyncRequest,
    CheckCode, CheckId, CheckPlanKind, CheckResult, CheckState, Stream,
};
use logweir_core::destination::DestinationRole;
use logweir_engine_oso::storage::StoreError;
use logweir_evidence::keys::VerifyingKey;
use logweir_evidence::{Sidecar, PAYLOAD_TYPE_BACKUP_RECEIPT};
use serde::{Serialize, Serializer};

use super::Wiring;
use crate::catalog::reader::{self, CrossCheck, RecordVerdict};
use crate::catalog::record::{self, CatalogPoint};
use crate::check::store::{self as check_store, ObjectAccess};
use crate::check::{catalogue, Deadline, Emission};

// ===========================================================================
// The grammar — the spellings `weirkeeper::catalog_view` parses
// ===========================================================================
//
// THESE ARE A SECOND DECLARATION OF ONE GRAMMAR AND THAT IS A COST, NOT A
// DESIGN. `weirkeeper` and `logweir` share no dependency edge (the controller
// links `logweir-core` and `logweir-verify`, never the runner), and adding one
// so two constants could be one would be a dependency decision made to avoid a
// test. So the constants are declared twice and pinned together twice:
// `the_grammar_this_runner_writes_is_the_grammar_the_controller_parses` reads
// `crates/weirkeeper/src/catalog_view.rs` and asserts every value below equals
// the controller's, and the pinned-body pair asserts the BYTES round-trip.

/// `catalog-format=<n>` — the grammar's own version line, first and required.
pub const FORMAT_LINE_PREFIX: &str = "catalog-format=";
/// The grammar version this build writes.
pub const BODY_FORMAT_VERSION: u32 = 1;
/// `catalog-page=<i>/<n> count=<c> sha256=<64 hex>`.
pub const PAGE_LINE_PREFIX: &str = "catalog-page=";
/// `catalog-entry=<compact json>`.
pub const ENTRY_LINE_PREFIX: &str = "catalog-entry=";
/// `catalog-counts=<json>`, written ONCE.
pub const COUNTS_LINE_PREFIX: &str = "catalog-counts=";
/// `catalog-cursor=<json>`, written ONCE and LAST.
pub const CURSOR_LINE_PREFIX: &str = "catalog-cursor=";
/// `catalog-signers=<json>`, written ONCE.
pub const SIGNERS_LINE_PREFIX: &str = "catalog-signers=";

/// The most bytes of `details` one body may carry — five megabytes.
///
/// A BYTE BUDGET BECAUSE THE TRANSPORT IS ONE. The relay bounds the body twice
/// by bytes and never by lines, and roughly 5.9 MB of raw `details` survives
/// the base64 part-frame expansion; five megabytes sits inside that with
/// headroom.
pub const MAX_BODY_BYTES: usize = 5 * 1024 * 1024;
/// The most `catalog-signers` rows one body may declare.
pub const MAX_BODY_SIGNERS: usize = 64;
/// The most locations one point may be reported at.
pub const MAX_ENTRY_LOCATIONS: usize = 16;
/// The most pages a body may declare — the CRD's `status.pages` `maxItems`.
pub const MAX_BODY_PAGES: usize = 8;
/// The most `byDay` rows a body carries — `status.histogram`'s `maxItems`.
pub const MAX_HISTOGRAM_DAYS: usize = 400;

/// How many entries one page carries before [`entries_per_page`] has to pack
/// them tighter to stay inside [`MAX_BODY_PAGES`].
pub const PAGE_ENTRY_TARGET: usize = 1_000;

// ===========================================================================
// The walk's own bounds
// ===========================================================================

/// How many day shards back an `Index` walk looks when no cursor names a
/// floor.
///
/// FOUR HUNDRED, the same number `status.histogram` can carry, so a first sync
/// of an archive with a year of history publishes a histogram that is not
/// already truncated by the walk.
pub const DEFAULT_LOOKBACK_DAYS: i64 = 400;

/// The hard ceiling on day shards examined in one `Index` walk, whatever the
/// cursor says.
///
/// An empty shard costs one `list` and a catalog whose newest point is five
/// years old would otherwise spend a whole sync listing nothing. The walk
/// stops and says `complete: false`, which is the honest answer and the one
/// that leaves a cursor to resume from.
pub const MAX_LOOKBACK_DAYS: i64 = 1_000;

/// How many keys one day shard's listing asks for.
///
/// The shard holds one UTC day of points; a day with more than this many is a
/// day whose tail is counted through the next page.
pub const SHARD_PAGE_KEYS: usize = 1_000;

/// How many keys one `Full` rescan page asks for.
pub const RESCAN_PAGE_KEYS: usize = 1_000;

// ===========================================================================
// The two axes, as the runner spells them on the wire
// ===========================================================================

macro_rules! wire_vocabulary {
    ($(#[$meta:meta])* $name:ident { $($variant:ident => $text:literal),+ $(,)? }) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub enum $name {
            $($variant),+
        }

        impl $name {
            /// The wire spelling.
            #[must_use]
            pub const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $text),+
                }
            }

            /// Every member, in declaration order.
            pub const ALL: &'static [Self] = &[$(Self::$variant),+];
        }

        impl Serialize for $name {
            fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                s.serialize_str(self.as_str())
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(self.as_str())
            }
        }
    };
}

wire_vocabulary! {
    /// Can these bytes be read — D3 §5.4's first table.
    ///
    /// `Missing` and `Unreadable` are DIFFERENT ANSWERS: a `NotFound` is "the
    /// archive does not hold this", a 403 or a timeout is "this credential
    /// could not tell", and reporting the second as the first is how an
    /// operator comes to believe an outage deleted their backups.
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

wire_vocabulary! {
    /// What the Job could say about a signature — and nothing about trust.
    SignatureVerdict {
        Verified => "verified",
        Invalid => "invalid",
        NoEvidence => "noEvidence",
        NotAttempted => "notAttempted",
    }
}

// ===========================================================================
// The documents
// ===========================================================================

/// One place a point's bytes were looked for, and what was found there.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EntryLocation {
    /// `s3://<bucket>/<prefix>` — bucket and prefix only. No endpoint, no
    /// region, no credential, and deliberately NOT part of the identity.
    pub location_id: String,
    /// What was found HERE.
    pub availability: Availability,
}

/// One point, as this Job reports it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CatalogEntry {
    pub point_id: String,
    pub backup_id: String,
    pub run_id: String,
    pub recovery_point_at_ms: i64,
    pub covered_from_ms: i64,
    pub covered_to_ms: i64,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub locations: Vec<EntryLocation>,
    pub receipt_key: String,
    pub receipt_sha256: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub manifest_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub manifest_sha256: Option<String>,
    /// RFC 3339 to the SECOND and with a `Z`, which is what
    /// `k8s_openapi::apimachinery::pkg::apis::meta::v1::Time` writes and the
    /// controller's `RunnerEntry::recorded_at` deserialises into. A
    /// fractional-second instant is a second spelling of one value.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recorded_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format_version: Option<String>,
    pub availability: Availability,
    pub signature: SignatureVerdict,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signer_key_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remedy: Option<String>,
}

/// The signature half of [`CatalogCounts`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SignatureCounts {
    pub verified: i64,
    pub invalid: i64,
    pub no_evidence: i64,
    pub not_attempted: i64,
}

/// One day of the histogram.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DayCount {
    /// `YYYY-MM-DD`, UTC.
    pub day: String,
    pub points: i64,
}

/// What the walk counted.
///
/// **`total` and the buckets have different scopes, and that is stated rather
/// than smoothed over.** `total` counts every point the walk SAW — one cheap
/// listing per day shard establishes it — while the availability and signature
/// buckets count every point the walk EXAMINED, which costs two to four `get`s
/// each and is therefore bounded by `viewLimit`, by `maxObjectsPerRun` and by
/// the clock. On a complete walk the two are equal; on a budgeted one the
/// buckets sum to less than `total`, `catalog-cursor=` says `complete: false`,
/// and the view reports `truncated: true`. Inventing bucket numbers for points
/// nothing fetched would be the only worse answer.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CatalogCounts {
    pub total: i64,
    pub available: i64,
    pub missing: i64,
    pub unreadable: i64,
    pub deleted: i64,
    pub conflict: i64,
    pub unsupported_format: i64,
    pub partial: i64,
    pub signature: SignatureCounts,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub by_day: Vec<DayCount>,
}

impl CatalogCounts {
    fn count_availability(&mut self, a: Availability) {
        let slot = match a {
            Availability::Available => &mut self.available,
            Availability::Missing => &mut self.missing,
            Availability::Unreadable => &mut self.unreadable,
            Availability::Deleted => &mut self.deleted,
            Availability::Conflict => &mut self.conflict,
            Availability::UnsupportedFormat => &mut self.unsupported_format,
            Availability::Partial => &mut self.partial,
        };
        *slot = slot.saturating_add(1);
    }

    fn count_signature(&mut self, s: SignatureVerdict) {
        let slot = match s {
            SignatureVerdict::Verified => &mut self.signature.verified,
            SignatureVerdict::Invalid => &mut self.signature.invalid,
            SignatureVerdict::NoEvidence => &mut self.signature.no_evidence,
            SignatureVerdict::NotAttempted => &mut self.signature.not_attempted,
        };
        *slot = slot.saturating_add(1);
    }
}

/// Who signed the points in this archive.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CatalogSigner {
    /// `sha256(DER SPKI)` in lowercase hex.
    pub key_id: String,
    /// What the record said the principal was — a HINT, never authority.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub principal_hint: Option<String>,
    pub points: i64,
}

/// Where the walk got to.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CursorReport {
    /// The oldest day shard this `Index` walk examined, `YYYY-MM-DD`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_shard: Option<String>,
    /// The key the next `Full` rescan continues strictly after.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rescan_start_after: Option<String>,
    /// Whether the walk finished. `false` is a budgeted walk that continues,
    /// never a failure.
    pub complete: bool,
}

// ===========================================================================
// Redaction
// ===========================================================================

/// The name of the one redaction rule an archive REFERENCE must not pass.
///
/// Spelled here rather than imported from `crate::check` so this module's own
/// rule is readable in one place; `the_long_run_rule_is_named_once` asserts the
/// two strings are equal.
pub const LONG_RUN_RULE: &str = "long-base64-or-hex-run";

/// Every redaction rule EXCEPT the long-run one, over a value that is a
/// reference or a digest.
///
/// # Why the long-run rule is the one that is dropped
///
/// It removes any base64-or-hex run of 40 characters or more, and a
/// `sha256:<64 hex>` binding, a 64-hex signer key id and a 32-hex point id
/// suffix are exactly that shape. Applying it would delete the three fields a
/// later restore re-checks and leave a body the controller cannot use. Every
/// other rule — PEM blocks, S3 XML bodies, URL userinfo, `secret…=value` forms
/// and `AKIA`/`ASIA` key ids — is applied WHOLE and unweakened, so a location
/// or a key that really does carry a credential shape is still removed.
///
/// # Panics
/// Never in practice: only if [`LONG_RUN_RULE`] names no rule, which
/// `the_long_run_rule_is_named_once` fails on first.
#[must_use]
pub fn redact_reference(value: &str) -> String {
    let shape: Vec<_> = redaction_rules()
        .iter()
        .filter(|r| r.name != LONG_RUN_RULE)
        .copied()
        .collect();
    assert!(
        shape.len() + 1 == redaction_rules().len(),
        "`{LONG_RUN_RULE}` names no redaction rule; the reference clause is dropping the wrong \
         thing"
    );
    apply_rules(value, &shape)
}

// ===========================================================================
// The remedies — a fixed table, so no adopter bytes reach an entry
// ===========================================================================

/// The one-sentence remedy for a state that is not selectable, or `None` for a
/// state that needs none.
///
/// A TABLE and not prose at each site: a `remedy` travels into an immutable
/// page `ConfigMap` and into the restore wizard, and a sentence built from the
/// record would be a place adopter bytes could reach one.
#[must_use]
pub fn remedy_for(availability: Availability, signature: SignatureVerdict) -> Option<&'static str> {
    match availability {
        Availability::Missing => Some(
            "The archive does not hold the objects this recovery point names. Restore from \
             another point, or recover the objects from your own backup of the bucket.",
        ),
        Availability::Unreadable => Some(
            "The objects could not be read — this is \"could not tell\", not \"is not there\". \
             Check the destination's archiveRead grant, the endpoint and the network path, then \
             sync again.",
        ),
        Availability::Deleted => Some(
            "A completed retention tombstone covers this recovery point. It is listed for the \
             record and cannot be restored.",
        ),
        Availability::Conflict => Some(
            "The archive contradicts the signed receipt this point is derived from. Nothing \
             under logweir/ is rewritten, so investigate which writer produced the second \
             document before restoring.",
        ),
        Availability::UnsupportedFormat => Some(
            "This record was written by a newer Logweir than the one that read it. Upgrade the \
             runner image to list this point.",
        ),
        Availability::Partial => Some(
            "Segments the manifest names could not all be found. Do not restore from this point \
             until the objects are recovered.",
        ),
        Availability::Available => match signature {
            SignatureVerdict::Verified => None,
            SignatureVerdict::Invalid => Some(
                "The receipt's signature did not verify over its own bytes. Treat this point as \
                 evidence of tampering or corruption, not as a recovery option.",
            ),
            SignatureVerdict::NoEvidence => Some(
                "There is no signed receipt beside this point, so nothing attests to it. It is \
                 listed and is not offered for an ordinary restore.",
            ),
            SignatureVerdict::NotAttempted => Some(
                "No signature verdict was reached: this installation holds no key that signed \
                 this point. Add the signing key to the trust source if you accept evidence \
                 from it.",
            ),
        },
    }
}

// ===========================================================================
// The walk
// ===========================================================================

/// What one point's examination established.
struct Observation {
    availability: Availability,
    signature: SignatureVerdict,
    signer_key_id: Option<String>,
    point: Option<Box<CatalogPoint>>,
    format_version: Option<String>,
}

impl Observation {
    fn bare(availability: Availability) -> Self {
        Self {
            availability,
            signature: SignatureVerdict::NotAttempted,
            signer_key_id: None,
            point: None,
            format_version: None,
        }
    }
}

/// The state one walk accumulates.
struct Walk {
    entries: Vec<CatalogEntry>,
    counts: CatalogCounts,
    by_day: BTreeMap<String, i64>,
    signers: BTreeMap<String, i64>,
    /// Objects spent — every `get` and every `list`, whatever it answered.
    objects: i64,
    /// Point ids already emitted, so one point seen in two shards costs one
    /// examination.
    seen: std::collections::BTreeSet<String>,
    complete: bool,
    oldest_day: Option<NaiveDate>,
    last_record_key: Option<String>,
    /// How many index rows could not be read at all.
    unreadable_rows: i64,
}

impl Walk {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
            counts: CatalogCounts::default(),
            by_day: BTreeMap::new(),
            signers: BTreeMap::new(),
            objects: 0,
            seen: std::collections::BTreeSet::new(),
            complete: false,
            oldest_day: None,
            last_record_key: None,
            unreadable_rows: 0,
        }
    }

    /// Whether another object may be spent.
    fn affordable(&self, budget: i64, deadline: Deadline) -> bool {
        self.objects < budget && deadline.has_room()
    }
}

/// Run one `catalogSync`.
#[must_use]
pub fn run(req: &CatalogSyncRequest, wiring: &dyn Wiring, deadline: Deadline) -> Emission {
    let now = wiring.now();
    let mut result = CheckResult::new(CheckPlanKind::CatalogSync);
    result.checks.push(super::runner_contract(now));

    // The trust material, BEFORE the handle: a bundle that cannot be read is a
    // fact about this pod, and learning it after a credential was used would
    // put the two failures in the wrong order.
    let (trust, trust_note) = load_trust(req, wiring);

    let access = match wiring.objects(
        &req.destination,
        DestinationRole::ArchiveRead,
        deadline.remaining(),
    ) {
        Ok(a) => a,
        Err(f) => {
            // NO BODY. See the module comment: a body that parses is a body
            // the controller publishes, and there is nothing here to publish.
            result.checks.push(super::from_store_failure(
                CheckId::DestinationArchiveListable,
                &f,
                now,
            ));
            return Emission::of(result);
        }
    };

    let mut walk = Walk::new();
    let listing = match req.mode {
        CatalogSyncMode::Index => {
            walk_index(req, access.as_ref(), &trust, deadline, now, &mut walk)
        }
        CatalogSyncMode::Full => walk_full(req, access.as_ref(), &trust, deadline, &mut walk),
    };
    if let Err(f) = listing {
        // The FIRST listing failed, so nothing was established about the
        // archive at all. Same rule as a handle that would not build.
        result.checks.push(super::from_store_failure(
            CheckId::DestinationArchiveListable,
            &f,
            now,
        ));
        return Emission::of(result);
    }

    let body = render_body(req, &walk);

    let mut row = catalogue::outcome(
        CheckId::DestinationArchiveListable,
        CheckState::Ready,
        CheckCode::ArchiveListable,
        now,
    )
    .with_message(&format!(
        "the durable catalog was walked: {} points seen, {} examined, {} relayed",
        walk.counts.total,
        examined(&walk.counts),
        walk.entries.len()
    ))
    .with_fact("catalogPoints", &walk.counts.total.to_string())
    .with_fact("catalogExamined", &examined(&walk.counts).to_string())
    .with_fact("catalogEntries", &walk.entries.len().to_string())
    .with_fact("catalogPages", &body.pages.to_string())
    .with_fact("catalogObjectsRead", &walk.objects.to_string())
    .with_fact("catalogWalkComplete", &walk.complete.to_string())
    .with_fact("catalogTrustKeys", &trust.len().to_string());
    if walk.unreadable_rows > 0 {
        row = row.with_fact(
            "catalogUnreadableIndexRows",
            &walk.unreadable_rows.to_string(),
        );
    }
    if body.dropped_for_space > 0 {
        row = row.with_fact(
            "catalogDroppedForSpace",
            &body.dropped_for_space.to_string(),
        );
    }
    if let Some(note) = trust_note {
        row = row.with_fact("catalogTrustNote", note);
    }
    if req.deep_check == CatalogDeepCheck::SegmentSample {
        // AN UNIMPLEMENTED DEPTH IS NAMED, NOT SILENTLY HONOURED. A row that
        // claimed a segment sample nobody took would be worse than a bounded
        // answer that says which check ran.
        row = row.with_fact(
            "catalogDeepCheck",
            CatalogDeepCheck::ManifestDigest.as_str(),
        );
        row = row.with_fact("catalogSegmentSample", "notImplemented");
    } else {
        row = row.with_fact("catalogDeepCheck", req.deep_check.as_str());
    }
    result.checks.push(row);

    Emission {
        topics: Vec::new(),
        emit_topics: false,
        result,
        extra: vec![(Stream::Details, body.text.into_bytes())],
    }
}

fn examined(counts: &CatalogCounts) -> i64 {
    counts
        .available
        .saturating_add(counts.missing)
        .saturating_add(counts.unreadable)
        .saturating_add(counts.deleted)
        .saturating_add(counts.conflict)
        .saturating_add(counts.unsupported_format)
        .saturating_add(counts.partial)
}

/// The PUBLIC keys this installation mounted, and a note when there are none.
///
/// A bundle that does not parse is NOT a reason to verify against the keys
/// that did: it is reported as a note and whatever parsed is used, because a
/// sync that refused outright would blank a view over one malformed PEM the
/// operator can fix.
fn load_trust(
    req: &CatalogSyncRequest,
    wiring: &dyn Wiring,
) -> (Vec<VerifyingKey>, Option<&'static str>) {
    let Some(path) = req.trust_bundle_file.as_deref() else {
        return (Vec::new(), Some("noTrustMaterial"));
    };
    let bytes = match wiring.read_bytes(path) {
        Ok(b) => b,
        // The PATH is a projection the controller chose and is safe to name in
        // a log; the fact carries only the shape of the failure.
        Err(_) => return (Vec::new(), Some("trustBundleUnreadable")),
    };
    let Ok(text) = String::from_utf8(bytes) else {
        return (Vec::new(), Some("trustBundleUnreadable"));
    };
    let mut keys = Vec::new();
    let mut rejected = 0usize;
    for block in pem_blocks(&text) {
        match VerifyingKey::from_pem_str(&block) {
            Ok(k) => keys.push(k),
            Err(_) => rejected += 1,
        }
    }
    let note = if keys.is_empty() {
        Some("noTrustMaterial")
    } else if rejected > 0 {
        Some("trustBundlePartlyUnparseable")
    } else {
        None
    };
    (keys, note)
}

/// Split a concatenated PEM bundle into its blocks.
///
/// By the BEGIN/END markers and not by blank lines: the controller joins the
/// roster's `spkiPem` values with a single `\n`, and a splitter that needed a
/// blank line between them would read one key out of a bundle of five.
fn pem_blocks(text: &str) -> Vec<String> {
    const BEGIN: &str = "-----BEGIN ";
    const END: &str = "-----END ";
    let mut out = Vec::new();
    let mut current: Option<String> = None;
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with(BEGIN) {
            current = Some(String::new());
        }
        if let Some(buf) = current.as_mut() {
            buf.push_str(trimmed);
            buf.push('\n');
            if trimmed.starts_with(END) {
                out.push(current.take().unwrap_or_default());
            }
        }
    }
    out
}

/// `Index`: day shards of `logweir/catalog/v1/log/`, newest first.
///
/// # The walk always starts at TODAY, and the cursor is a FLOOR
///
/// The view the controller publishes is the newest `viewLimit` points, and it
/// is rebuilt from this body wholesale on every sync. A walk that resumed at
/// an old cursor would therefore publish a window of old points and call it
/// the catalog. So the start is always today and `indexShard` is read as the
/// oldest day the PREVIOUS walk reached, minus one day of overlap — D3 §5.3's
/// "day shards at or after the recorded cursor, minus one day of overlap",
/// read against a newest-first walk. The floor bounds the cost; it never moves
/// the window.
fn walk_index(
    req: &CatalogSyncRequest,
    access: &dyn ObjectAccess,
    trust: &[VerifyingKey],
    deadline: Deadline,
    now: DateTime<Utc>,
    walk: &mut Walk,
) -> Result<(), check_store::StoreFailure> {
    let today = now.date_naive();
    let floor = req
        .index_shard
        .as_deref()
        .and_then(parse_day)
        .and_then(|d| d.pred_opt())
        .unwrap_or_else(|| {
            today
                .checked_sub_days(chrono::Days::new(
                    u64::try_from(DEFAULT_LOOKBACK_DAYS).unwrap_or(0),
                ))
                .unwrap_or(today)
        });
    let view_limit = usize::try_from(req.view_limit).unwrap_or(0);
    let mut first_listing = true;

    for back in 0..MAX_LOOKBACK_DAYS {
        let Some(day) = today.checked_sub_days(chrono::Days::new(u64::try_from(back).unwrap_or(0)))
        else {
            walk.complete = true;
            break;
        };
        if day < floor {
            walk.complete = true;
            break;
        }
        if !walk.affordable(req.max_objects_per_run, deadline) {
            break;
        }
        let shard = format!(
            "{}{:04}/{:02}/{:02}/",
            record::LOG_PREFIX,
            day.year(),
            day.month(),
            day.day()
        );
        walk.objects = walk.objects.saturating_add(1);
        let keys = match access.list_page(&shard, None, SHARD_PAGE_KEYS) {
            Ok(k) => k,
            Err(e) if first_listing => {
                return Err(listing_failure(&e, &req.destination.name));
            }
            // A LATER SHARD THAT WOULD NOT LIST IS ONE DAY THIS WALK COULD NOT
            // SEE, not a sync that failed: the days already read are true, and
            // the cursor says the walk did not complete.
            Err(_) => {
                walk.unreadable_rows = walk.unreadable_rows.saturating_add(1);
                walk.oldest_day = Some(day);
                continue;
            }
        };
        first_listing = false;
        walk.oldest_day = Some(day);
        // Newest first WITHIN the shard: the log key's fixed-width millisecond
        // makes the largest key the newest one.
        for key in keys.into_iter().rev() {
            // THE POINT ID COMES OUT OF THE KEY AND NOT OUT OF A `get`.
            // `log_key` is `<ms:013>-<pointId>.json`, so counting the archive
            // costs ONE list per day shard and nothing per point — which is
            // what lets `catalog-counts.total` cover the whole walk while only
            // the newest `viewLimit` points are fetched. It is also the safer
            // reading: `read_log_entry` exists partly to refuse a row whose
            // `record_key` is not the one its `point_id` implies (D3 W3's
            // finding F6), and deriving the key from the id cannot be lied to
            // at all.
            let Some(point_id) = point_id_of_log_key(&key) else {
                walk.unreadable_rows = walk.unreadable_rows.saturating_add(1);
                continue;
            };
            if !walk.seen.insert(point_id.clone()) {
                continue;
            }
            walk.counts.total = walk.counts.total.saturating_add(1);
            *walk
                .by_day
                .entry(day.format("%Y-%m-%d").to_string())
                .or_insert(0) += 1;
            if walk.entries.len() >= view_limit {
                // COUNTED AND NOT FETCHED. The window is full; every further
                // point still contributes to `total` and to the histogram,
                // which is what makes `truncated` a fact about the archive.
                continue;
            }
            if !walk.affordable(req.max_objects_per_run, deadline) {
                return Ok(());
            }
            examine_and_push(
                req,
                access,
                trust,
                deadline,
                walk,
                &record::record_key(&point_id),
            );
        }
    }
    Ok(())
}

/// The point id inside a day-shard key, or `None` when the key is not one of
/// ours.
///
/// `logweir/catalog/v1/log/<yyyy>/<mm>/<dd>/<ms:013>-<pointId>.json`, and the
/// id is checked against `lwp1-<32 lowercase hex>` before it is believed: the
/// log prefix is create-only but not append-restricted, so anyone who can write
/// a NEW key under it can choose its name.
#[must_use]
pub fn point_id_of_log_key(key: &str) -> Option<String> {
    let file = key.rsplit('/').next()?;
    let stem = file.strip_suffix(".json")?;
    let (_ms, point_id) = stem.split_once('-')?;
    reader::is_point_id(point_id).then(|| point_id.to_string())
}

/// `Full`: a resumable rescan of `logweir/catalog/v1/points/`.
fn walk_full(
    req: &CatalogSyncRequest,
    access: &dyn ObjectAccess,
    trust: &[VerifyingKey],
    deadline: Deadline,
    walk: &mut Walk,
) -> Result<(), check_store::StoreFailure> {
    let view_limit = usize::try_from(req.view_limit).unwrap_or(0);
    let mut cursor = req.rescan_start_after.clone();
    let mut first_listing = true;
    loop {
        if !walk.affordable(req.max_objects_per_run, deadline) {
            return Ok(());
        }
        walk.objects = walk.objects.saturating_add(1);
        let keys =
            match access.list_page(record::POINTS_PREFIX, cursor.as_deref(), RESCAN_PAGE_KEYS) {
                Ok(k) => k,
                Err(e) if first_listing => return Err(listing_failure(&e, &req.destination.name)),
                Err(_) => return Ok(()),
            };
        first_listing = false;
        if keys.is_empty() {
            walk.complete = true;
            return Ok(());
        }
        // A SHORT PAGE IS THE END OF THE PREFIX. `ObjectAccess::list_page`
        // returns at most `max` keys, so fewer than `max` means the listing
        // exhausted what is there — and the walk is complete once this page is
        // examined, not before.
        let exhausted = keys.len() < RESCAN_PAGE_KEYS;
        cursor = keys.last().cloned();
        for key in keys {
            // `record.sig` sits beside `record.json` under the same point
            // prefix; only the record is a row.
            if !key.ends_with("/record.json") {
                continue;
            }
            if !walk.affordable(req.max_objects_per_run, deadline) {
                return Ok(());
            }
            walk.last_record_key = Some(key.clone());
            if !walk.seen.insert(key.clone()) {
                continue;
            }
            walk.counts.total = walk.counts.total.saturating_add(1);
            if walk.entries.len() >= view_limit {
                continue;
            }
            examine_and_push(req, access, trust, deadline, walk, &key);
        }
        if exhausted {
            walk.complete = true;
            return Ok(());
        }
    }
}

/// `YYYY-MM-DD` -> a UTC day, or `None`.
///
/// `logweir_core::check_contract::is_iso_day` has already refused anything that
/// is not ten characters of the right shape at plan-validation time; this is
/// the calendar half, and a day like `2026-02-30` that passes the shape check
/// fails here and simply means "no floor was named".
fn parse_day(day: &str) -> Option<NaiveDate> {
    NaiveDate::parse_from_str(day, "%Y-%m-%d").ok()
}

fn listing_failure(e: &StoreError, destination: &str) -> check_store::StoreFailure {
    check_store::StoreFailure::new(
        check_store::classify(e),
        format!(
            "the durable recovery catalog under `{}` could not be listed with destination `{}`'s \
             archiveRead grant",
            record::CATALOG_PREFIX,
            destination
        ),
    )
}

/// Examine one point and, if it fits the window, push its entry.
fn examine_and_push(
    req: &CatalogSyncRequest,
    access: &dyn ObjectAccess,
    trust: &[VerifyingKey],
    deadline: Deadline,
    walk: &mut Walk,
    record_key: &str,
) {
    let observation = examine(req, access, deadline, trust, walk, record_key);
    walk.counts.count_availability(observation.availability);
    walk.counts.count_signature(observation.signature);
    if req.mode == CatalogSyncMode::Full {
        // A `Full` rescan's keys carry no instant — `points/<pointId>/
        // record.json` is content-addressed — so the only day it can attribute
        // a point to is the one inside the record it just read. The histogram
        // of a rescan therefore covers the EXAMINED points and not the whole
        // walk, which is the opposite of an `Index` walk, where one shard
        // listing dates every point it names for free. It is said here because
        // the two scopes are genuinely different, and smoothing over it would
        // put a number on `status.histogram` that means one thing on one mode
        // and another on the other.
        if let Some(point) = observation.point.as_ref() {
            *walk
                .by_day
                .entry(point.capture.started_at.format("%Y-%m-%d").to_string())
                .or_insert(0) += 1;
        }
    }
    if let Some(key_id) = observation.signer_key_id.as_deref() {
        *walk.signers.entry(key_id.to_string()).or_insert(0) += 1;
    }
    if let Some(entry) = build_entry(&observation) {
        walk.entries.push(entry);
    }
}

/// One point's two axes.
fn examine(
    req: &CatalogSyncRequest,
    access: &dyn ObjectAccess,
    deadline: Deadline,
    trust: &[VerifyingKey],
    walk: &mut Walk,
    record_key: &str,
) -> Observation {
    if !walk.affordable(req.max_objects_per_run, deadline) {
        return Observation::bare(Availability::Unreadable);
    }
    walk.objects = walk.objects.saturating_add(1);
    let record_bytes = match access.get(record_key) {
        Ok(b) => b,
        Err(StoreError::NotFound(_)) => return Observation::bare(Availability::Missing),
        Err(_) => return Observation::bare(Availability::Unreadable),
    };
    let point = match reader::read_record(&record_bytes) {
        RecordVerdict::Point(p) => p,
        RecordVerdict::UnsupportedFormat { format_version } => {
            let mut o = Observation::bare(Availability::UnsupportedFormat);
            o.format_version = Some(format_version);
            return o;
        }
        RecordVerdict::Unreadable(_) => return Observation::bare(Availability::Unreadable),
    };

    let mut observation = Observation {
        availability: Availability::Available,
        signature: SignatureVerdict::NotAttempted,
        signer_key_id: None,
        format_version: Some(point.format_version.clone()),
        point: Some(point),
    };
    let point = observation
        .point
        .as_ref()
        .expect("the record was just parsed")
        .clone();

    // -- the receipt: the verification root (D3 §5.2 rule 3) ---------------
    if !walk.affordable(req.max_objects_per_run, deadline) {
        observation.availability = Availability::Unreadable;
        return observation;
    }
    walk.objects = walk.objects.saturating_add(1);
    let receipt_bytes = match access.get(&point.receipt.key) {
        Ok(b) => b,
        Err(StoreError::NotFound(_)) => {
            observation.availability = Availability::Missing;
            return observation;
        }
        Err(_) => {
            observation.availability = Availability::Unreadable;
            return observation;
        }
    };
    let Ok(receipt) = serde_json::from_slice::<BackupReceipt>(&receipt_bytes) else {
        observation.availability = Availability::Unreadable;
        return observation;
    };
    match reader::cross_check(&point, &receipt, &receipt_bytes) {
        CrossCheck::Agrees => {}
        // D3 §5.4 as amended: a `RecordMismatch` IS a `Conflict`, and so is a
        // record that names a receipt whose digest is not the one it claims.
        CrossCheck::RecordMismatch(_) | CrossCheck::WrongReceipt { .. } => {
            observation.availability = Availability::Conflict;
        }
    }

    // -- the signature: a verdict and a key id, never a trust decision ------
    if walk.affordable(req.max_objects_per_run, deadline) {
        walk.objects = walk.objects.saturating_add(1);
        let (verdict, key_id) = classify_signature(access, &point, &receipt_bytes, trust);
        observation.signature = verdict;
        observation.signer_key_id = key_id;
    }

    // -- the manifest digest -----------------------------------------------
    if observation.availability == Availability::Available
        && req.deep_check != CatalogDeepCheck::None
        && walk.affordable(req.max_objects_per_run, deadline)
    {
        walk.objects = walk.objects.saturating_add(1);
        match access.get(&point.archive.manifest_key) {
            Ok(bytes) => {
                let got = logweir_core::ids::sha256_prefixed(&bytes);
                if got != point.archive.manifest_sha256 {
                    // The bytes in the bucket are not the bytes the signed
                    // receipt describes. The receipt is the authority, so this
                    // is a contradiction about the point and not a fact about
                    // the record.
                    observation.availability = Availability::Conflict;
                }
            }
            Err(StoreError::NotFound(_)) => observation.availability = Availability::Missing,
            Err(_) => observation.availability = Availability::Unreadable,
        }
    }

    observation
}

/// The signature verdict and the key id it belongs to.
///
/// **Four answers, and `UntrustedSigner` is not one of them**: that state is
/// "the signature verifies under a key this installation does not list", and a
/// Job holding only this installation's keys cannot verify under a key it does
/// not hold. A sidecar naming a key this pod does not have is
/// [`SignatureVerdict::NotAttempted`] with the CLAIMED key id, which is what
/// puts the stranger's key into `catalog-signers` and makes
/// `status.counts.untrustedSigner` exact even though no entry says
/// `UntrustedSigner`. The controller reaches that state when its trust source
/// is narrower than the bundle it mounted.
fn classify_signature(
    access: &dyn ObjectAccess,
    point: &CatalogPoint,
    receipt_bytes: &[u8],
    trust: &[VerifyingKey],
) -> (SignatureVerdict, Option<String>) {
    let sidecar_bytes = match access.get(&point.receipt.sidecar_key) {
        Ok(b) => b,
        // NO SIDECAR IS "no evidence", which is a fact about the archive.
        // Anything else is "could not tell", which is not.
        Err(StoreError::NotFound(_)) => return (SignatureVerdict::NoEvidence, None),
        Err(_) => return (SignatureVerdict::NotAttempted, None),
    };
    let Ok(sidecar) = serde_json::from_slice::<Sidecar>(&sidecar_bytes) else {
        return (SignatureVerdict::NotAttempted, None);
    };
    let claimed = sidecar.signatures.first().map(|s| s.keyid.clone());
    if trust.is_empty() {
        return (SignatureVerdict::NotAttempted, claimed);
    }
    let held: Vec<&VerifyingKey> = trust
        .iter()
        .filter(|k| sidecar.signatures.iter().any(|s| s.keyid == k.key_id()))
        .collect();
    if held.is_empty() {
        return (SignatureVerdict::NotAttempted, claimed);
    }
    for key in &held {
        if logweir_evidence::verify::verify_detached(
            key,
            PAYLOAD_TYPE_BACKUP_RECEIPT,
            receipt_bytes,
            &sidecar,
        )
        .is_ok()
        {
            return (SignatureVerdict::Verified, Some(key.key_id()));
        }
    }
    // A key this installation holds signed this sidecar and the signature does
    // not verify over the receipt's bytes. That is a definite negative.
    (SignatureVerdict::Invalid, Some(held[0].key_id()))
}

/// One observation as an entry line's value.
///
/// `None` for an observation with no record — a `Missing`, an `Unreadable` or
/// a record of a major this build does not implement. **Such a point is
/// COUNTED and not listed**, which is deliberate: an entry line's required
/// fields are the receipt-derived facts, and there is no honest value for any
/// of them when the record could not be read. The counts carry the fact; a row
/// of zeroes would carry a fiction.
fn build_entry(observation: &Observation) -> Option<CatalogEntry> {
    let point = observation.point.as_ref()?;
    let availability = observation.availability;
    let mut entry = CatalogEntry {
        point_id: redact_reference(&point.point_id),
        backup_id: redact_reference(&point.backup_id),
        run_id: redact_reference(&point.run_id),
        recovery_point_at_ms: point.capture.started_at.timestamp_millis(),
        covered_from_ms: point.covered.from_ms,
        covered_to_ms: point.covered.to_ms,
        // ONE LOCATION, because one sync reads one destination. The controller
        // merges the same point id seen elsewhere into one entry with two
        // locations; `MAX_ENTRY_LOCATIONS` is the cap that merge must respect
        // and this side can only ever contribute one row to it.
        locations: vec![EntryLocation {
            location_id: redact_reference(&point.archive.location_id),
            availability,
        }],
        receipt_key: redact_reference(&point.receipt.key),
        receipt_sha256: redact_reference(&point.receipt.sha256),
        manifest_key: Some(redact_reference(&point.archive.manifest_key)),
        manifest_sha256: Some(redact_reference(&point.archive.manifest_sha256)),
        recorded_at: Some(point.recorded_at.to_rfc3339_opts(SecondsFormat::Secs, true)),
        format_version: observation.format_version.as_deref().map(redact_reference),
        availability,
        signature: observation.signature,
        signer_key_id: observation.signer_key_id.as_deref().map(redact_reference),
        remedy: remedy_for(availability, observation.signature)
            .map(redact)
            .filter(|r| !r.is_empty()),
    };
    // One sync contributes one location, so this can only ever be a no-op —
    // and it is written down so the cap is enforced on the side that renders
    // the line rather than assumed on the side that parses it.
    entry.locations.truncate(MAX_ENTRY_LOCATIONS);
    Some(entry)
}

// ===========================================================================
// Rendering
// ===========================================================================

/// The rendered body and what rendering had to leave out.
struct RenderedBody {
    text: String,
    pages: usize,
    dropped_for_space: usize,
}

/// Render one walk as the §7d body.
///
/// The order is fixed and is the grammar's: the version line first, then the
/// pages in `1..=n`, then the three summary lines with `catalog-cursor=` LAST
/// — the fence, so a truncated read cannot end with a cursor that claims a
/// walk reached further than it did.
fn render_body(req: &CatalogSyncRequest, walk: &Walk) -> RenderedBody {
    let mut lines: Vec<String> = Vec::new();
    for entry in &walk.entries {
        lines.push(serde_json::to_string(entry).unwrap_or_default());
    }

    // The summary lines are rendered FIRST so the byte budget is spent on the
    // pages that are left after the numbers that describe them. A body whose
    // counts did not fit would be a body the controller reads as a catalog of
    // nothing.
    let counts_line = format!(
        "{COUNTS_LINE_PREFIX}{}",
        serde_json::to_string(&counts_document(walk)).unwrap_or_default()
    );
    let signers_line = format!(
        "{SIGNERS_LINE_PREFIX}{}",
        serde_json::to_string(&signers_document(walk)).unwrap_or_default()
    );
    let cursor_line = format!(
        "{CURSOR_LINE_PREFIX}{}",
        serde_json::to_string(&cursor_document(req, walk)).unwrap_or_default()
    );
    let format_line = format!("{FORMAT_LINE_PREFIX}{BODY_FORMAT_VERSION}");
    let overhead = format_line.len()
        + counts_line.len()
        + signers_line.len()
        + cursor_line.len()
        + 4
        // Room for up to `MAX_BODY_PAGES` headers, each about 110 bytes.
        + MAX_BODY_PAGES * 128;

    let mut budget = MAX_BODY_BYTES.saturating_sub(overhead);
    let mut kept: Vec<String> = Vec::new();
    let mut dropped_for_space = 0usize;
    for line in lines {
        let cost = ENTRY_LINE_PREFIX.len() + line.len() + 1;
        if cost > budget {
            dropped_for_space += 1;
            continue;
        }
        budget -= cost;
        kept.push(line);
    }

    let pages: Vec<&[String]> = kept.chunks(entries_per_page(kept.len())).collect();
    let total_pages = pages.len();

    let mut text = String::with_capacity(MAX_BODY_BYTES.min(64 * 1024));
    text.push_str(&format_line);
    text.push('\n');
    for (i, page) in pages.iter().enumerate() {
        let refs: Vec<&str> = page.iter().map(String::as_str).collect();
        text.push_str(&format!(
            "{PAGE_LINE_PREFIX}{}/{} count={} sha256={}\n",
            i + 1,
            total_pages,
            page.len(),
            page_digest(&refs)
        ));
        for line in *page {
            text.push_str(ENTRY_LINE_PREFIX);
            text.push_str(line);
            text.push('\n');
        }
    }
    text.push_str(&counts_line);
    text.push('\n');
    text.push_str(&signers_line);
    text.push('\n');
    text.push_str(&cursor_line);
    text.push('\n');

    RenderedBody {
        text,
        pages: total_pages,
        dropped_for_space,
    }
}

/// How many entries go on one page — **as few pages as the ceiling allows**.
///
/// A page is a transport frame and not a unit of meaning: the controller
/// re-packs the entries into its own `ConfigMap` pages by its own byte budget,
/// so splitting here buys nothing and costs a header and a digest each time.
/// The only reason to split at all is [`MAX_BODY_PAGES`], which is the CRD's
/// `status.pages` `maxItems` and the number the parser refuses above.
///
/// So the rule is: [`PAGE_ENTRY_TARGET`] per page, raised to whatever it takes
/// to fit the whole window into eight. At the contract's own 5 000-entry
/// ceiling that is five pages; at anything up to a thousand it is one.
#[must_use]
pub fn entries_per_page(entries: usize) -> usize {
    entries.div_ceil(MAX_BODY_PAGES).max(PAGE_ENTRY_TARGET)
}

/// The digest a page header declares, over the page's own entry lines.
///
/// **Over the RAW line bodies, each followed by `\n`** — the JSON after
/// `catalog-entry=`, byte for byte as it is written. Transport integrity and
/// explicitly NOT authorization (D3 §5.3): the receipt's own signature is the
/// verification root, and a digest over reparsed values would verify nothing.
#[must_use]
pub fn page_digest(entry_lines: &[&str]) -> String {
    let mut buf = String::new();
    for line in entry_lines {
        buf.push_str(line);
        buf.push('\n');
    }
    logweir_core::ids::sha256_hex(buf.as_bytes())
}

fn counts_document(walk: &Walk) -> CatalogCounts {
    let mut days: Vec<DayCount> = walk
        .by_day
        .iter()
        .map(|(day, points)| DayCount {
            day: day.clone(),
            points: *points,
        })
        .collect();
    // Newest day first, and bounded: `status.histogram` carries 400.
    days.sort_by(|a, b| b.day.cmp(&a.day));
    days.truncate(MAX_HISTOGRAM_DAYS);
    let mut counts = walk.counts.clone();
    counts.by_day = days;
    counts
}

fn signers_document(walk: &Walk) -> Vec<CatalogSigner> {
    let mut rows: Vec<CatalogSigner> = walk
        .signers
        .iter()
        .map(|(key_id, points)| CatalogSigner {
            key_id: redact_reference(key_id),
            // The record carries no principal, so there is no hint to give.
            // An invented one would be unsigned metadata presented as a fact.
            principal_hint: None,
            points: *points,
        })
        .collect();
    // Most points first, then by key id, so the bound keeps the signers that
    // describe most of the archive and the order is stable across passes.
    rows.sort_by(|a, b| {
        b.points
            .cmp(&a.points)
            .then_with(|| a.key_id.cmp(&b.key_id))
    });
    rows.truncate(MAX_BODY_SIGNERS);
    rows
}

fn cursor_document(req: &CatalogSyncRequest, walk: &Walk) -> CursorReport {
    match req.mode {
        CatalogSyncMode::Index => CursorReport {
            index_shard: walk.oldest_day.map(|d| d.format("%Y-%m-%d").to_string()),
            rescan_start_after: None,
            complete: walk.complete,
        },
        CatalogSyncMode::Full => CursorReport {
            index_shard: None,
            // A COMPLETE RESCAN REPORTS NO CURSOR. Leaving the last key on a
            // finished walk would make the next one resume past the whole
            // archive and publish an empty view of a full one.
            rescan_start_after: if walk.complete {
                None
            } else {
                walk.last_record_key.clone().map(|k| redact_reference(&k))
            },
            complete: walk.complete,
        },
    }
}
