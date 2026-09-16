//! `logweir catalog sync` and `logweir catalog list` — the OPERATOR's two
//! catalog commands.
//!
//! # What these are, and what they are not (seam S1)
//!
//! `D-SEAMS.md` S1 fixes ONE check runner: a controller that wants a catalog
//! synchronised runs `logweir check run --plan <file>` with D2's plan kind
//! `catalogSync`, in a Job, with a destination-backed credential and D2's
//! result frames. **These two subcommands are not that.** They are what
//! decision D3 §5.3 calls "an operator CLI that reads with the operator's own
//! credentials and creates no Job": a person at a workstation with a bucket
//! they can already read.
//!
//! The consequences are deliberate and are the reason both can exist beside
//! the check runner without being a second execution path:
//!
//! * they take a URL and file paths, never a plan document, and emit no
//!   result frame;
//! * they build the store through `Store::from_url`, the INLINE path that
//!   reads the ambient credential chain — the operator's environment IS the
//!   credential source here, which is exactly what a destination-backed Job
//!   must never do (D-SEAMS S5, defect SEC-ENVHTTP). Transport is still never
//!   DERIVED: `--allow-http` is an explicit flag and no URL scheme or
//!   endpoint shape turns it on;
//! * `sync` writes ONLY under `logweir/catalog/v1/`, create-only, and `list`
//!   writes nothing at all;
//! * neither reads or creates a Kubernetes object.
//!
//! # Exit codes
//!
//! The existing contract (`docs/stability.md`, Global Constraint 11) with no
//! new variant. `0` the command completed; `1` operational — a bad command
//! line, a store that could not be read, a write that failed after other
//! writes had succeeded; `4` signing failed, so nothing was uploaded. `2`
//! (a signed result that is not a pass) and `3` (a plan refused by a guard)
//! are not produced: these commands run no plan and sign no verdict.

use crate::catalog::reader::{self, CrossCheck, RecordVerdict};
use crate::catalog::record::*;
use crate::catalog::writer::{self, RecordInputs};
use crate::exit::ExitCode;
use chrono::Datelike;
use logweir_core::engine::StorageUrl;
use logweir_engine_oso::storage::{Store, StoreError};
use logweir_evidence::keys::VerifyingKey;
use std::path::PathBuf;

/// How many objects one `sync` or `list` run examines unless told otherwise.
///
/// A bound rather than "everything": a catalog walk must be resumable, and a
/// command that ran until the bucket ended would be unusable on the archive
/// this feature exists for.
pub const DEFAULT_MAX: usize = 1000;

/// The page `list` pulls out of the store at a time while it looks for the
/// newest rows. Independent of `--max`, which bounds what is PRINTED.
const LIST_PAGE: usize = 1000;

#[derive(Debug, Clone)]
pub struct Location {
    /// `s3://bucket[/logweir]`, `gs://…`, `az://account/container[/…]`,
    /// `file:///abs/path` or a bare absolute path.
    pub url: String,
    pub region: Option<String>,
    pub endpoint: Option<String>,
    pub path_style: bool,
    /// EXPLICIT, always. Never derived from the endpoint's scheme, from the
    /// addressing style, or from anything else (D-SEAMS S5).
    pub allow_http: bool,
}

#[derive(Debug, Clone)]
pub struct SyncArgs {
    pub location: Location,
    /// The key the point records are signed with.
    pub signing_key: PathBuf,
    /// The public keys a backup receipt must verify under before a record is
    /// written for it. **At least one is required and there is no "trust
    /// whatever is in the bucket" mode**: a public key found beside an
    /// archive is a claim, never a trust anchor (`docs/keys.md`).
    pub public_keys: Vec<PathBuf>,
    /// Resume the receipt walk strictly after this key.
    pub since: Option<String>,
    pub max: usize,
}

#[derive(Debug, Clone)]
pub struct ListArgs {
    pub location: Location,
    /// Only rows whose log key is strictly greater than this. Combined with
    /// newest-first output it is a way to ask "what arrived after the last
    /// thing I saw", and it also stops the backward shard walk at that key's
    /// own day.
    pub since: Option<String>,
    pub max: usize,
    /// How many DAY SHARDS back from today to look. The listing stops as soon
    /// as `--max` rows are held, so this only costs anything on a catalog whose
    /// newest point is old — and the report says how far it looked, so an empty
    /// page is never mistaken for an empty catalog.
    pub days: u32,
}

// ---------------------------------------------------------------------------
// The URL
// ---------------------------------------------------------------------------

/// `--url` -> the EVIDENCE location, with Global Constraint 6's `logweir/`
/// root imposed rather than accepted from the operator.
///
/// # What is refused, and why each refusal exists
///
/// * **userinfo** (`s3://key:secret@bucket/x`) — a credential on a command
///   line is visible in every process listing on the host and in a shell
///   history. It is refused by SHAPE, before anything is parsed out of it, and
///   the offending value is never echoed back: the message names the scheme
///   and the rule, not the string.
/// * **a query or a fragment** — an object-store location has neither, and
///   accepting one would mean quietly ignoring whatever it said.
/// * **a prefix that is neither empty nor `logweir`** — this command reads and
///   writes evidence, and `Store::from_url` refuses any evidence prefix that
///   is not exactly `logweir/` (Global Constraint 6). Refusing here means the
///   operator is told which part of their URL is wrong instead of reading a
///   message about a prefix they never typed.
///
/// `logweir` alone is accepted as the prefix because it is what an operator
/// copies off a receipt key; anything under it (`logweir/backups`) is not,
/// because the root is exactly one segment deep.
pub fn evidence_url(location: &Location) -> Result<StorageUrl, String> {
    let raw = location.url.trim();
    if raw.is_empty() {
        return Err("--url is empty; give the archive's object-store location, \
                    e.g. s3://my-bucket"
            .to_string());
    }
    let (scheme, rest) = match raw.split_once("://") {
        Some((s, r)) => (s.to_ascii_lowercase(), r),
        // A bare path is a filesystem location, which is what the e2e and
        // local-archive paths use.
        None if raw.starts_with('/') => ("file".to_string(), raw),
        None => {
            return Err(format!(
                "--url must be an object-store URL (s3://, gs://, az:// or file:///) or an \
                 absolute filesystem path; `{raw}` is neither"
            ))
        }
    };
    // Checked on the WHOLE remainder, before any splitting, so no arm of the
    // match below can be reached with a credential still in the string.
    if rest
        .split('/')
        .next()
        .is_some_and(|host| host.contains('@'))
    {
        return Err(format!(
            "--url carries userinfo before the host. A credential on an argv is visible in \
             every process listing on this machine and in the shell history; supply it through \
             the environment the object-store client already reads. (The value is not echoed \
             here on purpose.) Scheme: {scheme}"
        ));
    }
    if rest.contains('?') || rest.contains('#') {
        return Err(
            "--url must carry no query string and no fragment: an object-store location has \
             neither, and accepting one would mean ignoring whatever it said"
                .to_string(),
        );
    }
    let mut parts = rest.split('/');
    let first = parts.next().unwrap_or_default().to_string();
    let tail: Vec<&str> = parts.filter(|p| !p.is_empty()).collect();
    match scheme.as_str() {
        "s3" | "gs" => {
            if first.is_empty() {
                return Err(format!("--url names no bucket: `{scheme}://<bucket>`"));
            }
            assert_evidence_prefix(&tail, &scheme)?;
            let prefix = logweir_engine_oso::storage::LOGWEIR_ROOT.to_string();
            Ok(if scheme == "s3" {
                StorageUrl::S3 {
                    bucket: first,
                    prefix,
                    region: location.region.clone(),
                    endpoint: location.endpoint.clone(),
                    path_style: location.path_style,
                    allow_http: location.allow_http,
                }
            } else {
                StorageUrl::Gcs {
                    bucket: first,
                    prefix,
                }
            })
        }
        "az" => {
            let Some(container) = tail.first() else {
                return Err(
                    "--url must name an account and a container: `az://<account>/<container>`"
                        .to_string(),
                );
            };
            if first.is_empty() {
                return Err(
                    "--url names no storage account: `az://<account>/<container>`".to_string(),
                );
            }
            assert_evidence_prefix(&tail[1..], &scheme)?;
            Ok(StorageUrl::Azure {
                account_name: first,
                container_name: (*container).to_string(),
                prefix: logweir_engine_oso::storage::LOGWEIR_ROOT.to_string(),
            })
        }
        "file" => {
            // `file:///abs/path` leaves an empty host and the path in `tail`;
            // a bare `/abs/path` leaves an empty first element and the same
            // tail. Both rebuild to the same absolute path.
            if !first.is_empty() {
                return Err(format!(
                    "--url must be `file:///absolute/path` (three slashes, no host); got a host \
                     segment `{first}`"
                ));
            }
            let path = format!("/{}", tail.join("/"));
            Ok(StorageUrl::Filesystem { path: path.into() })
        }
        other => Err(format!(
            "unsupported scheme `{other}`: object_store is built with the aws/azure/gcp/http \
             feature set (Global Constraint 9), so the schemes are s3://, gs://, az:// and \
             file:///"
        )),
    }
}

/// The key-prefix half of [`evidence_url`]'s contract.
fn assert_evidence_prefix(tail: &[&str], scheme: &str) -> Result<(), String> {
    let root = logweir_engine_oso::storage::LOGWEIR_ROOT.trim_end_matches('/');
    match tail {
        [] => Ok(()),
        [only] if *only == root => Ok(()),
        _ => Err(format!(
            "--url names the key prefix `{}`, but Logweir's evidence root is exactly `{}` \
             (Global Constraint 6). Pass `{scheme}://<bucket>` or \
             `{scheme}://<bucket>/{root}`; put the environment in the BUCKET, not in the \
             prefix.",
            tail.join("/"),
            logweir_engine_oso::storage::LOGWEIR_ROOT
        )),
    }
}

// ---------------------------------------------------------------------------
// sync
// ---------------------------------------------------------------------------

/// What one receipt turned into. Ordered as the walk decides them, so a reader
/// of the source can see which check runs before which.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PointOutcome {
    /// A record was written for a receipt that had none.
    Written,
    /// A record already existed and agrees with the receipt. Repeated import
    /// is idempotent (D3 §5.5 step 2).
    AlreadyPresent,
    /// A record exists and CONTRADICTS the receipt it names, or is about
    /// different bytes entirely. D3 §5.4 availability `Conflict`. Nothing is
    /// rewritten and nothing is deleted.
    Conflict,
    /// A record exists whose `format_version` major this build does not
    /// implement. Per-entry, never fatal (rule 1).
    UnsupportedFormat,
    /// The receipt's DSSE signature verified under NO configured public key.
    /// No record is written: the receipt's signature is the verification root,
    /// and a record derived from bytes nobody could authenticate would assert
    /// facts this command never established.
    UnverifiedSigner,
    /// Could not tell. A read error, a body that is not a receipt, a missing
    /// sidecar. Never collapsed into any of the above — "could not look" is
    /// not "not there".
    Unreadable,
}

impl PointOutcome {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Written => "written",
            Self::AlreadyPresent => "already-present",
            Self::Conflict => "conflict",
            Self::UnsupportedFormat => "unsupported-format",
            Self::UnverifiedSigner => "unverified-signer",
            Self::Unreadable => "unreadable",
        }
    }
}

/// The bounded summary one `sync` establishes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SyncReport {
    pub scanned: usize,
    pub written: usize,
    pub already_present: usize,
    pub conflict: usize,
    pub unsupported_format: usize,
    pub unverified_signer: usize,
    pub unreadable: usize,
    /// The cursor that resumes the receipt walk, present only when this run
    /// stopped at its `--max` bound with more to see.
    pub next: Option<String>,
    /// One row per receipt examined — at most `--max`, which is the operator's
    /// own bound. `(receipt key, point id, outcome)`.
    pub points: Vec<(String, String, PointOutcome)>,
}

impl SyncReport {
    fn record(&mut self, receipt_key: &str, point_id: &str, outcome: PointOutcome) {
        self.scanned += 1;
        match outcome {
            PointOutcome::Written => self.written += 1,
            PointOutcome::AlreadyPresent => self.already_present += 1,
            PointOutcome::Conflict => self.conflict += 1,
            PointOutcome::UnsupportedFormat => self.unsupported_format += 1,
            PointOutcome::UnverifiedSigner => self.unverified_signer += 1,
            PointOutcome::Unreadable => self.unreadable += 1,
        }
        self.points
            .push((receipt_key.to_string(), point_id.to_string(), outcome));
    }
}

/// The failure modes `sync` maps onto the exit-code contract.
#[derive(Debug)]
pub enum SyncError {
    /// Exit 1. The walk could not run, or a write failed after other writes
    /// had already landed.
    Operational(String),
    /// Exit 4. Signing failed, so nothing was uploaded for this point.
    Signing(String),
}

impl From<&SyncError> for ExitCode {
    fn from(e: &SyncError) -> Self {
        match e {
            SyncError::Operational(_) => ExitCode::Operational,
            SyncError::Signing(_) => ExitCode::SigningOrLock,
        }
    }
}

/// **The testable seam.** One `sync` over handles it does not build.
///
/// `evidence` must be the writable `logweir/` handle: every write goes through
/// `Store::put_create_only`, which asserts the root itself, so a mutant
/// pointing this at an archive aborts inside the store rather than writing
/// there.
///
/// # The order, per receipt, and why
///
/// 1. read the receipt bytes and parse them — a body that is not a receipt is
///    `Unreadable`, never a point;
/// 2. derive the identity from those exact bytes (D3 §5.1);
/// 3. ask whether a record already exists — BEFORE verifying, because an
///    existing record has to be cross-checked against the receipt whether or
///    not this run could have written one, and `NotFound` is the only answer
///    that means "write it";
/// 4. verify the receipt's DSSE sidecar against the configured public keys. A
///    receipt nobody can authenticate yields no record;
/// 5. build, sign and put — create-only, three objects.
pub fn sync_with(
    args: &SyncArgs,
    evidence: &Store,
    signer: &crate::signer::ValidatedSigner,
    trust: &[VerifyingKey],
    now: chrono::DateTime<chrono::Utc>,
    location_id: &str,
) -> Result<SyncReport, SyncError> {
    if trust.is_empty() {
        return Err(SyncError::Operational(
            "no --public-key was given, so no backup receipt could be verified and no catalog \
             record would mean anything. A public key found beside an archive is a CLAIM and is \
             never trusted merely by proximity (docs/keys.md)."
                .to_string(),
        ));
    }
    let (keys, next) = evidence
        .list_page(RECEIPTS_PREFIX, args.since.as_deref(), args.max)
        .map_err(|e| SyncError::Operational(format!("cannot list `{RECEIPTS_PREFIX}`: {e}")))?;
    let mut report = SyncReport {
        next,
        ..SyncReport::default()
    };
    for key in keys {
        if !key.ends_with(".receipt.json") {
            continue;
        }
        let Ok((bytes, _version)) = evidence.get(&key) else {
            report.record(&key, "", PointOutcome::Unreadable);
            continue;
        };
        let receipt: logweir_core::backup_receipt::BackupReceipt =
            match serde_json::from_slice(&bytes) {
                Ok(r) => r,
                Err(_) => {
                    report.record(&key, "", PointOutcome::Unreadable);
                    continue;
                }
            };
        let id = point_id(&bytes);
        match existing_record(evidence, &id, &receipt, &bytes) {
            Existing::Absent => {}
            Existing::Outcome(o) => {
                report.record(&key, &id, o);
                continue;
            }
        }
        let sidecar_key = sidecar_key_of(&key);
        let Some(installation) = verified_signer(evidence, &sidecar_key, &bytes, trust) else {
            report.record(&key, &id, PointOutcome::UnverifiedSigner);
            continue;
        };
        let inputs = RecordInputs {
            receipt_key: key.clone(),
            sidecar_key,
            location_id: location_id.to_string(),
            recorded_at: now,
            signing: crate::catalog::signing_of(&signer.verifying_key()),
            installation: Some(installation),
            // UNKNOWN, deliberately. A backfill sees a receipt and a bucket
            // and no Kubernetes object at all, so provenance is exactly what
            // it cannot establish (D3 §5.2 rule 2).
            execution: None,
        };
        let point = match writer::from_receipt(&receipt, &bytes, &inputs) {
            Ok(p) => p,
            Err(_) => {
                report.record(&key, &id, PointOutcome::Unreadable);
                continue;
            }
        };
        let entry = CatalogLogEntry::of(&point);
        // **THE EXIT-CODE DECISION, AND IT IS MADE ON THE ERROR'S KIND, NOT ON
        // HOW FAR THE WALK GOT** (review finding F1).
        //
        // Global Constraint 11's exit 4 says "signing or lock-proof failed —
        // and NOTHING was uploaded". The first version of this arm mapped the
        // FIRST point's failure to 4 whatever caused it, so a denied PUT under
        // `logweir/catalog/*` or a 503 told an operator to rotate signing
        // material over a bucket policy — and, in the case where `record.json`
        // landed and `record.sig` did not, asserted "nothing was uploaded"
        // while an unsigned record sat in the bucket.
        //
        // `PutError::nothing_was_uploaded()` answers the only question exit 4
        // actually asks. A store failure is exit 1 with a message that says
        // which key, what the store said, and what is and is not in the bucket.
        let outcome = writer::put_point(&point, &entry, signer, evidence).map_err(|e| {
            if e.nothing_was_uploaded() {
                return SyncError::Signing(format!(
                    "{e} — no catalog object was written for this point; {} record(s) written \
                     earlier in this run are signed and valid",
                    report.written
                ));
            }
            SyncError::Operational(format!(
                "the object store refused or could not complete a catalog write: {e}. This says \
                 nothing about the signing key — the record was signed before any put was \
                 attempted. Part of this point may already be stored, and nothing under \
                 `logweir/` is ever rewritten, so re-running is safe: {} record(s) written \
                 earlier in this run are valid, and `--since {}` resumes the walk.",
                report.written,
                report.points.last().map_or_else(
                    || args.since.clone().unwrap_or_default(),
                    |(k, _, _)| k.clone()
                )
            ))
        })?;
        report.record(
            &key,
            &id,
            if outcome.created_record() {
                PointOutcome::Written
            } else {
                PointOutcome::AlreadyPresent
            },
        );
    }
    Ok(report)
}

/// `…/<run_id>.receipt.json` -> `…/<run_id>.receipt.sig`, which is
/// `phase_run::receipt_keys`'s pairing read backwards. One derivation, so a
/// backfill looks for the sidecar the runner actually wrote.
fn sidecar_key_of(receipt_key: &str) -> String {
    format!("{}.sig", receipt_key.trim_end_matches(".json"))
}

enum Existing {
    Absent,
    Outcome(PointOutcome),
}

/// Rules 1 and 3 against whatever is already at the record key.
fn existing_record(
    evidence: &Store,
    id: &str,
    receipt: &logweir_core::backup_receipt::BackupReceipt,
    receipt_bytes: &[u8],
) -> Existing {
    match evidence.get(&record_key(id)) {
        Err(StoreError::NotFound(_)) => Existing::Absent,
        // "Could not tell" is not "not there": writing a record here could put
        // a second document beside one this run failed to read.
        Err(_) => Existing::Outcome(PointOutcome::Unreadable),
        Ok((bytes, _)) => match reader::read_record(&bytes) {
            RecordVerdict::UnsupportedFormat { .. } => {
                Existing::Outcome(PointOutcome::UnsupportedFormat)
            }
            RecordVerdict::Unreadable(_) => Existing::Outcome(PointOutcome::Conflict),
            RecordVerdict::Point(p) => match reader::cross_check(&p, receipt, receipt_bytes) {
                CrossCheck::Agrees => Existing::Outcome(PointOutcome::AlreadyPresent),
                _ => Existing::Outcome(PointOutcome::Conflict),
            },
        },
    }
}

/// The receipt's signer, if any configured public key verifies its sidecar
/// over these exact bytes.
///
/// `None` covers every way of not knowing — a missing sidecar, a sidecar that
/// is not JSON, a wrong payload type, a signature under a key nobody
/// configured — because they have one consequence: no record is written. The
/// per-case detail belongs to D3 §5.4's `Invalid` / `UntrustedSigner` /
/// `NotAttempted` vocabulary, which is the catalog VIEW's, and this command
/// does not publish a view.
fn verified_signer(
    evidence: &Store,
    sidecar_key: &str,
    receipt_bytes: &[u8],
    trust: &[VerifyingKey],
) -> Option<RecordInstallation> {
    let (raw, _version) = evidence.get(sidecar_key).ok()?;
    let sidecar: logweir_evidence::Sidecar = serde_json::from_slice(&raw).ok()?;
    for key in trust {
        if logweir_evidence::verify::verify_detached(
            key,
            logweir_evidence::PAYLOAD_TYPE_BACKUP_RECEIPT,
            receipt_bytes,
            &sidecar,
        )
        .is_ok()
        {
            return Some(RecordInstallation {
                key_id: key.key_id(),
            });
        }
    }
    None
}

// ---------------------------------------------------------------------------
// list
// ---------------------------------------------------------------------------

/// What one `list` established: the rows, and what it could NOT read.
///
/// The counts are not decoration. `docs/formats/catalog-point.md` promises
/// "refusal is per entry, not per catalog", and a listing that silently
/// dropped what it skipped would keep that promise while breaking the one
/// underneath it — an operator would read a short page as "these are all the
/// points" (review finding F3).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ListReport {
    /// Newest first.
    pub rows: Vec<CatalogLogEntry>,
    /// Rule 1: index entries whose `format_version` major this build does not
    /// implement.
    pub unsupported_format: usize,
    /// Could not tell: a `get` that failed, bytes that are not JSON, a shape
    /// major 1 cannot hold.
    pub unreadable: usize,
    /// Review finding F6: an entry whose `record_key` is not the one its
    /// `point_id` implies, or whose `point_id` is malformed.
    pub inconsistent: usize,
    /// How many day shards this run looked in.
    pub days_searched: u32,
    /// The oldest day shard searched, `yyyy-mm-dd`, so an EMPTY page says
    /// which window it is empty for rather than "there are no points".
    pub oldest_day_searched: String,
    /// True when `--max` filled before the lookback ran out: there are older
    /// points this page did not reach.
    pub truncated: bool,
}

/// **The testable seam** for `list`: the newest `max` index entries, newest
/// first, read one DAY SHARD at a time.
///
/// # Why it walks shards and not the whole prefix (review finding F5)
///
/// The first version paged forward over the entire `logweir/catalog/v1/log/`
/// prefix keeping a ring of the newest `max` keys. That is correct and it is
/// O(n²/page) object-metadata reads, because `Store::list_page` walks the whole
/// post-offset prefix on every call — 50 000 points cost about 1.25 million
/// streamed entries to print fifty rows. It also made the day shard buy
/// nothing, which is the one structure D3 §5.2 introduced precisely so that
/// "newest first" would be bounded.
///
/// So: walk days BACKWARDS from `today`, list one shard at a time, and stop the
/// moment `max` rows are held. On an archive with a recent backup that is one
/// or two small listings. The lookback is `args.days`, and the report says how
/// far it looked, so an empty page is "nothing in the last N days" and never
/// "there are no points".
///
/// `today` is a parameter: this function reads no clock, so a test can pin the
/// window.
///
/// Reading each entry's BODY happens only for keys that survive the shard
/// selection, so `--max 50` costs fifty `get`s whatever the catalog holds.
pub fn list_with(
    args: &ListArgs,
    store: &Store,
    today: chrono::NaiveDate,
) -> Result<ListReport, String> {
    let mut report = ListReport::default();
    if args.max == 0 {
        report.oldest_day_searched = today.to_string();
        return Ok(report);
    }
    // `--since` is a lower bound on the KEY, so it is also a lower bound on the
    // day: there is nothing to find in a shard that sorts entirely below it.
    let floor_day = args.since.as_deref().and_then(day_of_log_key);
    for back in 0..args.days {
        let Some(day) = today.checked_sub_days(chrono::Days::new(u64::from(back))) else {
            break;
        };
        let shard = format!(
            "{LOG_PREFIX}{:04}/{:02}/{:02}/",
            day.year(),
            day.month(),
            day.day()
        );
        // The floor is checked BEFORE the day is counted: a shard the walk
        // decided not to look in must not appear in `days_searched`, or the
        // number an operator reads as "how far did it look" is one too many.
        if floor_day.is_some_and(|floor| day < floor) {
            break;
        }
        report.days_searched += 1;
        report.oldest_day_searched = day.to_string();
        for key in newest_keys_in(store, &shard, args)? {
            match read_one(store, &key) {
                reader::LogEntryVerdict::Entry(e) => report.rows.push(*e),
                reader::LogEntryVerdict::UnsupportedFormat { .. } => report.unsupported_format += 1,
                reader::LogEntryVerdict::Unreadable(_) => report.unreadable += 1,
                reader::LogEntryVerdict::Inconsistent(_) => report.inconsistent += 1,
            }
            if report.rows.len() == args.max {
                // More days remain unlooked-at, so there are almost certainly
                // older points. Said as `truncated`, not as a resume cursor:
                // a windowed query over older points is an ADVERTISED-ABSENT
                // capability (D3 §5.3), and inventing a `--until` nothing
                // consumes would be the fake stub that section forbids.
                report.truncated = back + 1 < args.days;
                return Ok(report);
            }
        }
    }
    Ok(report)
}

/// The newest keys in ONE day shard, descending, at most `args.max` of them and
/// never fewer than the shard holds.
///
/// Bounded memory: the deque keeps the LAST `max` keys of an ascending walk,
/// which are the largest, and the log key's fixed-width millisecond makes
/// largest mean newest.
fn newest_keys_in(store: &Store, shard: &str, args: &ListArgs) -> Result<Vec<String>, String> {
    let mut newest: std::collections::VecDeque<String> = std::collections::VecDeque::new();
    let mut cursor = args.since.clone();
    loop {
        let (page, next) = store
            .list_page(shard, cursor.as_deref(), LIST_PAGE)
            .map_err(|e| format!("cannot list `{shard}`: {e}"))?;
        for key in page {
            if !key.ends_with(".json") {
                continue;
            }
            newest.push_back(key);
            if newest.len() > args.max {
                newest.pop_front();
            }
        }
        match next {
            Some(n) => cursor = Some(n),
            None => break,
        }
    }
    Ok(newest.into_iter().rev().collect())
}

/// One index entry, with a `get` failure folded into the same per-entry
/// vocabulary as a malformed body: both mean "this row could not be read",
/// and neither is a reason to abandon the listing.
fn read_one(store: &Store, key: &str) -> reader::LogEntryVerdict {
    match store.get(key) {
        Ok((bytes, _)) => reader::read_log_entry(&bytes),
        Err(e) => reader::LogEntryVerdict::Unreadable(format!("{key}: {e}")),
    }
}

/// The UTC day a log key's shard names, or `None` when the key is not one of
/// ours. `logweir/catalog/v1/log/2026/09/15/…` -> 2026-09-15.
fn day_of_log_key(key: &str) -> Option<chrono::NaiveDate> {
    let rest = key.strip_prefix(LOG_PREFIX)?;
    let mut parts = rest.split('/');
    let y: i32 = parts.next()?.parse().ok()?;
    let m: u32 = parts.next()?.parse().ok()?;
    let d: u32 = parts.next()?.parse().ok()?;
    chrono::NaiveDate::from_ymd_opt(y, m, d)
}

// ---------------------------------------------------------------------------
// The two entry points `main.rs` dispatches
// ---------------------------------------------------------------------------

/// `logweir catalog sync`. **The only function in this module that names a
/// store constructor**, which is what keeps every test above socket-free.
pub fn run_sync(args: &SyncArgs) -> ExitCode {
    let url = match evidence_url(&args.location) {
        Ok(u) => u,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::Operational;
        }
    };
    let mut trust = Vec::with_capacity(args.public_keys.len());
    for path in &args.public_keys {
        match VerifyingKey::from_pem_file(path) {
            Ok(k) => trust.push(k),
            Err(e) => {
                eprintln!("{e}");
                return ExitCode::Operational;
            }
        }
    }
    // Signing is a PREREQUISITE, resolved and self-verified before the store
    // is dialled — the same ordering `backup run` uses, for the same reason: a
    // key that cannot sign must cost an operator a message, not a half-written
    // catalog.
    let signer = match crate::backup::phase_run::load_signer(&args.signing_key) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::SigningOrLock;
        }
    };
    let evidence = match Store::from_url(&url) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::Operational;
        }
    };
    let location = location_id(&url);
    match sync_with(
        args,
        &evidence,
        &signer,
        &trust,
        chrono::Utc::now(),
        &location,
    ) {
        Ok(report) => {
            print_sync_report(&report);
            ExitCode::Ok
        }
        Err(e) => {
            match &e {
                SyncError::Operational(m) | SyncError::Signing(m) => eprintln!("{m}"),
            }
            ExitCode::from(&e)
        }
    }
}

/// The summary, as stdout lines a controller or a script can read by prefix.
///
/// Bounded: one `catalog-point=` line per receipt EXAMINED, which `--max`
/// already bounds, then a fixed eight-line block. `catalog-next=` appears only
/// when there is more to walk, so its absence means "the prefix is exhausted"
/// rather than "the cursor was empty".
pub fn print_sync_report(r: &SyncReport) {
    for (receipt, id, outcome) in &r.points {
        println!(
            "catalog-point={} state={} receipt={receipt}",
            if id.is_empty() { "-" } else { id },
            outcome.as_str()
        );
    }
    println!("catalog-scanned={}", r.scanned);
    println!("catalog-written={}", r.written);
    println!("catalog-already-present={}", r.already_present);
    println!("catalog-conflict={}", r.conflict);
    println!("catalog-unsupported-format={}", r.unsupported_format);
    println!("catalog-unverified-signer={}", r.unverified_signer);
    println!("catalog-unreadable={}", r.unreadable);
    if let Some(next) = &r.next {
        println!("catalog-next={next}");
    }
}

/// `logweir catalog list`. Read-only: it constructs the store through
/// `Store::from_url`, which is a WRITABLE handle, and calls no method that
/// writes — the read-only constructor cannot be used here because it skips the
/// `logweir/` guard that keeps this command pointed at the evidence root.
pub fn run_list(args: &ListArgs) -> ExitCode {
    let url = match evidence_url(&args.location) {
        Ok(u) => u,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::Operational;
        }
    };
    let store = match Store::from_url(&url) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::Operational;
        }
    };
    // The ONE clock read on this path, taken here so `list_with` stays pure in
    // the window it searches and a test can pin it.
    match list_with(args, &store, chrono::Utc::now().date_naive()) {
        Ok(report) => {
            print_list(&report);
            ExitCode::Ok
        }
        Err(e) => {
            eprintln!("{e}");
            ExitCode::Operational
        }
    }
}

/// The printed page, newest first, with the sentence that says what it is
/// worth.
///
/// The closing note is not decoration. These rows come from the UNSIGNED
/// day-sharded index, which exists so that listing a page costs one listing
/// rather than one `get` per point; the signed document is `record.json` and
/// the verification root is the backup receipt it names. A listing that did
/// not say so would be a surface presenting unverified metadata as evidence,
/// which is the one thing PLAT-15.1's acceptance forbids.
pub fn print_list(report: &ListReport) {
    for e in &report.rows {
        println!(
            "{}  {}  covered [{}, {})  backup={} run={}  {}",
            e.point_id,
            chrono::DateTime::from_timestamp_millis(e.recovery_point_at_ms)
                .map(|t| t.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
                .unwrap_or_else(|| "<unreadable instant>".to_string()),
            e.covered.from_ms,
            e.covered.to_ms,
            e.backup_id,
            e.run_id,
            e.record_key
        );
    }
    println!("catalog-listed={}", report.rows.len());
    // WHAT WAS SKIPPED, ALWAYS PRINTED (review finding F3). Refusal is per
    // entry and never fatal — and a page that dropped what it could not read
    // without saying so would let a short listing read as "these are all the
    // points".
    println!("catalog-unsupported-format={}", report.unsupported_format);
    println!("catalog-unreadable={}", report.unreadable);
    println!("catalog-inconsistent={}", report.inconsistent);
    // WHICH WINDOW this page is about, so an empty one is "nothing in the last
    // N days" and never "there are no points".
    println!("catalog-searched-days={}", report.days_searched);
    println!("catalog-oldest-day-searched={}", report.oldest_day_searched);
    if report.truncated {
        // NOT a resume cursor. A windowed query over older points is an
        // advertised-absent capability (D3 §5.3), and a `catalog-next=` that
        // nothing consumes would be the fake stub that section forbids. Raise
        // `--max`, or narrow with `--since`.
        println!("catalog-truncated=true");
    }
    println!(
        "note: these rows come from the UNSIGNED day-sharded index. Fetch the record.json named \
         on each row and verify it (`logweir drill verify --payload-type catalog-point`), then \
         verify the backup receipt the record names — the receipt's signature is the \
         verification root, and nothing here is a claim that a point is still available."
    );
}
