//! The run itself, and the read-back that makes its result a measured fact
//! rather than an exit code.
//!
//! `backup` has no `--format` and writes no report file
//! [VERIFIED U:crates/kafka-backup-cli/src/main.rs:35-39,559-561], so its only
//! machine-readable signal is the exit code. Everything else this phase
//! reports is read back out of the ARCHIVE — the manifest key, the exact
//! bytes' digest, the per-topic record counts and the covered window — which
//! is what turns "the engine exited 0" into "these records are in this
//! archive". A backup of empty topics also exits 0, and a run that reports
//! success without reading anything back has measured nothing.
//!
//! Global Constraint 6 is untouched: the archive handle is
//! `Store::read_only_from_url`'s, which physically cannot put.
use crate::backup::BackupError;
use crate::signer::ValidatedSigner;
use logweir_core::backup_receipt::{
    BackupReceipt, ReceiptArchive, ReceiptAuth, ReceiptCovered, ReceiptEngine, ReceiptSource,
};
use logweir_core::engine::{BackupFacts, BackupPlan, DataEngine, PhaseObserver};
use logweir_engine_oso::storage::Store;
use std::collections::BTreeMap;
use std::path::Path;

/// The token a run names when its execution was already
/// claimed by an earlier run — see [`claim_execution`]. Exit 1: a retry under a
/// NEW execution id is the remedy, and D1 §4.6's retry policy takes it.
pub const EXECUTION_ALREADY_CLAIMED: &str =
    logweir_core::guard::TERMINAL_STATE_EXECUTION_ALREADY_CLAIMED;

/// The token a run names when the evidence store could not
/// prove the execution claim is exclusive — it refused the create-only put,
/// answered it without enforcing it, or accepted a second create of the same
/// key. Exit 4 (GC11: "lock-proof failed, nothing uploaded"): nothing about
/// waiting changes a store that does not honour `If-None-Match: *`.
pub const EXECUTION_CLAIM_UNPROVEN: &str =
    logweir_core::guard::TERMINAL_STATE_EXECUTION_CLAIM_UNPROVEN;

/// `logweir/backups/<backup_id>/execution.claim.json` — the ONE
/// execution-scoped object under `logweir/backups/<backup_id>/`. Everything
/// else there is run-scoped (`<run_id>.receipt.json|.sig`).
///
/// Derived in one place, beside [`receipt_keys`], for the same reason.
#[must_use]
pub fn claim_key(backup_id: &str) -> String {
    format!("logweir/backups/{backup_id}/execution.claim.json")
}

/// **One engine run per execution.** Claim `backup_id` with a
/// create-only put, immediately BEFORE the engine starts, and PROVE the claim
/// is exclusive; return the claim's key.
///
/// # Why a claim, and why here
///
/// The engine writes `<prefix>/<backup_id>/manifest.json` with its own,
/// unconditional store client, so a second engine run over the same set
/// REPLACES the manifest an earlier run's receipt attests — and when the topic
/// advanced in between, that earlier signed receipt stops describing what the
/// archive holds (every archive-reading verifier reports it). Logweir cannot
/// make the engine's write conditional. What it can do is make sure no second
/// engine run of the same execution ever starts: a Job re-created from its
/// frozen inputs carries the same `backup_id`, finds the
/// claim, and stops here, before the engine, the manifest read-back and any
/// signature.
///
/// The claim lives under the evidence root because that is the one place this
/// runner may write (GC6), and a create-only put there needs no permission the
/// runner does not already hold (`s3:PutObject` under `logweir/`, the
/// `evidenceWrite` grant). It is NOT read: the runner holds no `s3:GetObject` or
/// `s3:ListBucket` under `logweir/` and this adds none — existence is learned
/// from the conditional put's own answer.
///
/// # The proof, and why it is two puts
///
/// A claim is a lock only on a store that ENFORCES `If-None-Match: *`. Three
/// stores do not, and each fails closed with [`EXECUTION_CLAIM_UNPROVEN`]:
///
/// 1. a store that refuses the put (a missing grant, a transport failure);
/// 2. a backend that reports conditional put unsupported —
///    `Store::put_create_only` then falls back to HEAD-then-PUT and says
///    `create_only_enforced: false`, and a HEAD this runner may not issue under
///    `logweir/` is no lock at all;
/// 3. an S3-compatible store that ACCEPTS the header and overwrites anyway.
///    Nothing in a single successful put distinguishes it, so the claim is
///    put a SECOND time with the same bytes, and only `AlreadyExists` proves
///    the store refused a create over an existing key. On a store that
///    ignored the header, the second put rewrote identical bytes, which
///    harms nothing.
///
/// An `AlreadyExists` on the FIRST put is the case this exists for: an earlier
/// run of this execution reached the engine, and whether it finished, died or
/// is still running, a second engine run could only overwrite its manifest.
/// That is exit 1, [`EXECUTION_ALREADY_CLAIMED`], and D1's rule already says
/// what happens next: a failed attempt's archive is never appended to, and a
/// retry is a new Backup under a new execution id.
pub fn claim_execution(
    backup_id: &str,
    run_id: &str,
    claimed_at: chrono::DateTime<chrono::Utc>,
    evidence: &Store,
) -> Result<String, BackupError> {
    use logweir_engine_oso::storage::StoreError;
    let key = claim_key(backup_id);
    // Unsigned on purpose: the claim is a lock, not evidence. Its only meaning
    // is that it EXISTS; the fields name the run that holds it for an operator
    // who can read the evidence root.
    let bytes = logweir_core::det_json::to_deterministic_json(&serde_json::json!({
        "format_version": "1.0.0",
        "backup_id": backup_id,
        "run_id": run_id,
        "claimed_at": claimed_at.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
    }))
    .map_err(|e| BackupError::Operational(format!("the execution claim: {e}")))?;
    let unproven = |why: String| {
        BackupError::Lock(format!(
            "{EXECUTION_CLAIM_UNPROVEN}: execution `{backup_id}` could not be claimed \
             exclusively at {key}: {why}. A claim the store does not enforce cannot stop a \
             second run of this execution from overwriting the manifest an earlier signed \
             receipt attests, so NO engine run was started and nothing was signed. Use an \
             evidence store that honours conditional create (`If-None-Match: *`) and grant \
             `s3:PutObject` on `logweir/*`."
        ))
    };
    match evidence.put_create_only(&key, &bytes) {
        Ok(put) if put.create_only_enforced => {}
        Ok(_) => {
            return Err(unproven(
                "the store does not implement conditional put, so the create fell back to \
                 HEAD-then-PUT, which is not exclusive"
                    .to_string(),
            ))
        }
        Err(StoreError::AlreadyExists(_)) => {
            return Err(BackupError::ExecutionClaimed(format!(
                "{EXECUTION_ALREADY_CLAIMED}: execution `{backup_id}` was already claimed by an \
                 earlier run ({key} exists). That run reached the engine; a second engine run \
                 would overwrite the manifest its receipt attests, so NO engine run was started \
                 and nothing was signed. Retry under a new execution id (a new Backup)."
            )))
        }
        Err(e) => return Err(unproven(format!("the put was refused: {e}"))),
    }
    match evidence.put_create_only(&key, &bytes) {
        Err(StoreError::AlreadyExists(_)) => Ok(key),
        Ok(_) => Err(unproven(
            "a second create-only put of the same key SUCCEEDED, so this store ignores \
             `If-None-Match: *`"
                .to_string(),
        )),
        Err(e) => Err(unproven(format!(
            "the exclusivity probe (a second create of the same key) failed: {e}"
        ))),
    }
}

/// **One engine run per backup SET, whoever made the first one.** Refuse the
/// engine when the archive already holds what the engine writes for this
/// execution and a signed receipt attests: its manifest,
/// `<prefix>/<backup_id>/manifest.json`, or any segment under
/// `<prefix>/<backup_id>/topics/`.
///
/// # Why the claim alone is not enough
///
/// A set written by an older build carries no claim, so a later run of the
/// same `backup_id` (a Job re-created across the upgrade, or a standalone
/// `backup run` re-using a fixed id) wins a fresh claim. Measured on engine
/// 0.21.0 (`docs/formats/backup-receipt.md`): the engine writes each segment at
/// `<backup_id>/topics/<topic>/partition=<n>/segment-<start offset>…`, so a
/// second run rewrites the first run's segments in place, and its
/// get-merge-put keeps the first run's manifest entries ("existing wins"). The
/// manifest bytes can come out IDENTICAL while the segments hold different
/// records, so the second engine run must never start.
///
/// # Why the segments, and not any object
///
/// The engine writes segments before its final manifest, so a run that is
/// still running or died after its first segment leaves segments and no
/// manifest; both are looked for. Anything else under the set (`offsets.db`,
/// `consumer-groups-snapshot.json`) is not written in the configuration
/// `render_backup` produces, so a run beside it invalidates nothing and is
/// not refused.
///
/// # Where it runs, and what it costs
///
/// After the claim and as the last refusal before the engine: between two runs
/// of this build the claim answers first, and only the topic-configuration
/// read (which writes nothing and is never fatal) runs between these reads and
/// the engine's first write. Two reads through the read-only archive handle (a
/// one-key LIST of `topics/` and a GET of the manifest), under the prefix
/// `run` already reads, so no permission is added. An existing set is exit 1
/// [`EXECUTION_ALREADY_CLAIMED`]: the same fact as an existing claim, with the
/// same remedy (a new execution id).
///
/// # A read that FAILS proves nothing, and which failure decides the code
///
/// No engine run starts and nothing is signed either way:
///
/// * **a transport failure, a timeout, or a 5xx/429 the object-store client
///   already retried** is exit 1 `Operational`, which the controller's retry
///   policy (`weirkeeper::cadence::is_retryable`) retries when the schedule
///   has `spec.retry`. A retry is SAFE: it is a new execution id
///   `<uid>-<slot>-r<k>`, a different set, with its own claim;
/// * **anything else** (a 401/403, a wrong bucket, region or CA, a failure this
///   build cannot classify) is exit 4 [`EXECUTION_CLAIM_UNPROVEN`], a decision
///   no retry changes.
///
/// Either message says the claim is taken, so a MANUAL retry needs a new
/// execution id too. `archive` is the [`ObjectAccess`] seam the check runner
/// reads through (`Store` implements it), so a row can make either read fail.
///
/// [`ObjectAccess`]: crate::check::store::ObjectAccess
pub fn refuse_an_existing_set(
    backup_id: &str,
    storage: &logweir_core::engine::StorageUrl,
    archive: &dyn crate::check::store::ObjectAccess,
) -> Result<(), BackupError> {
    use logweir_engine_oso::storage::StoreError;
    // The directory is taken from the PLAN's storage prefix — the prefix the
    // engine writes under and `run`'s read-back lists (`list_manifests` over
    // `plan.storage`) — so the three agree by construction, whatever prefix
    // the archive handle itself was built with.
    let prefix = storage.prefix().trim_end_matches('/');
    let directory = if prefix.is_empty() {
        format!("{backup_id}/")
    } else {
        format!("{prefix}/{backup_id}/")
    };
    let unproven = |what: &str, e: &StoreError| {
        if a_retry_can_change(e) {
            BackupError::Operational(format!(
                "execution `{backup_id}`: {what} could not be read to prove the backup set is \
                 new: {e}. The failure is transient (a transport error, a timeout or a 5xx), so \
                 NO engine run was started and nothing was signed, and the run ends exit 1: a \
                 schedule with `spec.retry` retries it under a NEW execution id. This \
                 execution's claim is taken, so a manual retry needs a new execution id too (a \
                 new Backup, or a fresh `--backup-id-override`)."
            ))
        } else {
            BackupError::Lock(format!(
                "{EXECUTION_CLAIM_UNPROVEN}: execution `{backup_id}`: {what} could not be read \
                 to prove the backup set is new: {e}. An existing set would be rewritten by the \
                 engine, so NO engine run was started and nothing was signed. Grant \
                 `s3:ListBucket` and `s3:GetObject` on the archive prefix (or fix the bucket, \
                 region or CA the error names), then run again under a new execution id (a new \
                 Backup): this execution's claim is taken."
            ))
        }
    };
    let segments = format!("{directory}topics/");
    let found = match archive.list_page(&segments, None, 1) {
        Ok(keys) => keys.into_iter().next(),
        Err(e) => return Err(unproven(&segments, &e)),
    };
    let found = match found {
        Some(segment) => Some(segment),
        None => {
            let manifest = format!("{directory}manifest.json");
            // An EXISTENCE test, so no byte of the manifest is read:
            // `caps::PROBE` refuses any body unread, and that refusal is the
            // store answering "it is there". A GET and not a HEAD, so a denial
            // keeps the error text `unproven` classifies.
            match archive.get(&manifest, logweir_engine_oso::storage::caps::PROBE) {
                Ok(_) | Err(StoreError::TooLarge { .. }) => Some(manifest),
                Err(StoreError::NotFound(_)) => None,
                Err(e) => return Err(unproven(&manifest, &e)),
            }
        }
    };
    match found {
        None => Ok(()),
        Some(key) => Err(BackupError::ExecutionClaimed(format!(
            "{EXECUTION_ALREADY_CLAIMED}: execution `{backup_id}`'s backup set already exists in \
             the archive ({key} is in it) although it carries no execution claim this run could \
             see — an earlier run of this backup_id, by a build without the claim, wrote it. A \
             second engine run would rewrite that run's segments in place, and its signed \
             receipt would no longer describe the archive, so NO engine run was started and \
             nothing was signed. Retry under a new execution id (a new Backup)."
        ))),
    }
}

/// Whether a failed read of the archive in
/// [`refuse_an_existing_set`] is one a retry under a new execution id can
/// change: a transport failure (`EndpointUnreachable`), a `Timeout`, or a 5xx
/// or 429 the object-store client has already retried — which the classifier
/// leaves unclassified, so the status the store ANSWERED with decides
/// (`answered_status`: read off `object_store`'s status line, never searched
/// for in a text that also carries the archive's key).
/// A closed match with no wildcard: a class added to the vocabulary must be
/// placed here on purpose, and "could not classify" stays a decision.
fn a_retry_can_change(e: &logweir_engine_oso::storage::StoreError) -> bool {
    use logweir_engine_oso::storage::StoreErrorClass as Class;
    match Class::classify(e) {
        Class::EndpointUnreachable | Class::Timeout => true,
        Class::StoreErrorUnclassified => matches!(
            logweir_engine_oso::storage::answered_status(e),
            Some(429 | 500..=599)
        ),
        Class::AccessDenied
        | Class::InvalidCredentials
        | Class::BucketNotFound
        | Class::ObjectNotFound
        | Class::RegionMismatch
        | Class::TlsTrustFailed => false,
    }
}

pub fn load_signer(path: &Path) -> Result<ValidatedSigner, BackupError> {
    ValidatedSigner::load(
        path,
        logweir_evidence::PAYLOAD_TYPE_BACKUP_RECEIPT,
        b"logweir backup signing readiness probe v1",
        "No engine data operation was started",
    )
    .map_err(BackupError::Signing)
}

/// Everything the run and the read-back established.
#[derive(Debug)]
pub struct Ran {
    pub facts: BackupFacts,
    pub manifest_key: String,
    /// `"sha256:<hex>"` over the EXACT bytes read back, via
    /// `logweir_core::ids::sha256_prefixed` (`ids.rs:11-13`) — never over a
    /// re-serialisation of anything.
    pub manifest_sha256: String,
    /// The version id the store answered the SAME read with — the
    /// version those exact bytes are — or `None` on a bucket that keeps no
    /// versions (`logweir_core::backup_receipt::pinnable_version_id`).
    pub manifest_version_id: Option<String>,
    pub records_per_topic: BTreeMap<String, u64>,
    /// The covered window, epoch milliseconds, or `None` when the set holds
    /// no segment for any named topic (an empty capture, when the source's
    /// watermarks say so — [`empty_capture_window`]).
    ///
    /// The start is INCLUSIVE: the oldest `start_timestamp` any segment of
    /// this backup set declares. The end is **EXCLUSIVE** — interface
    /// **I22**, and the one unit conversion in this module: the manifest's
    /// newest `end_timestamp` is INCLUSIVE (a record exists at that
    /// millisecond), so the measured end is that timestamp plus one
    /// millisecond, converted HERE, once, where the window is measured — see
    /// the argument in `run` below.
    pub covered: Option<(i64, i64)>,
    /// The manifest's `configurations` for each NAMED topic the
    /// manifest mentions — read back through the same `describe` as the
    /// counts, so the coverage comparison is against the bytes this run read.
    /// A named topic the manifest does not mention is ABSENT here, which
    /// `config_coverage::classify` reads as `manifestDiffers`, never as "no
    /// overrides".
    pub manifest_configurations: BTreeMap<String, BTreeMap<String, String>>,
    /// The manifest's `original_partition_count` and
    /// `source_replication_factor` for each NAMED topic it mentions — the
    /// counts `topic_configuration` records, from the same read-back.
    pub manifest_layouts: BTreeMap<String, crate::backup::config_coverage::Layout>,
    /// Per NAMED topic, the schema dependency judged from a
    /// bounded sample of the segments the manifest just read back lists —
    /// `crate::backup::schema_dependency::detect`, through the same read-only
    /// archive handle. Never fatal: a segment it cannot judge makes its topic
    /// `notAssessed`.
    pub schema_dependency: BTreeMap<String, logweir_core::backup_receipt::TopicSchemaDependency>,
    /// Per NAMED topic, per partition with at least one
    /// segment, the lowest and highest offsets the manifest records
    /// (inclusive): what a consumer position is judged against. A partition
    /// with no segment is ABSENT: nothing archived, never `[0, 0]`.
    pub manifest_ranges: crate::backup::consumer_positions::ArchivedRanges,
}

pub fn run(
    plan: &BackupPlan,
    engine: &dyn DataEngine,
    store: &Store,
    obs: &mut dyn PhaseObserver,
) -> Result<Ran, BackupError> {
    obs.phase_started(-1, "backup");
    crate::backup::print_progress_step(crate::backup::PROGRESS_STEP_ENGINE);
    let facts = engine.backup(plan, obs);
    obs.phase_finished(
        -1,
        &match &facts {
            Ok(_) => "ok".to_string(),
            Err(e) => format!("failed: {e}"),
        },
    );
    let facts = facts?;

    // D3 §2.4: the engine is done and the archive is about to be read back.
    crate::backup::print_progress_step(crate::backup::PROGRESS_STEP_READBACK);

    // The manifest this run's `backup_id` produced. Listed through the
    // read-only archive handle rather than reconstructed from the prefix and
    // the id: `Store::list_manifests` derives `backup_id` from the key's
    // parent directory, so matching on it is matching the archive's own
    // answer about which set is which, and a set that is not there is a fact
    // worth failing on rather than a path we hope exists.
    let sets = store
        .list_manifests(&plan.storage)
        .map_err(BackupError::Engine)?;
    let set = sets
        .into_iter()
        .find(|s| s.backup_id == plan.backup_id)
        .ok_or_else(|| {
            BackupError::Operational(format!(
                "the engine exited 0 but the archive holds no backup set `{}` at the configured \
                 storage location (prefix `{}`); nothing was read back, so this run establishes \
                 nothing about the source cluster",
                plan.backup_id,
                plan.storage.prefix()
            ))
        })?;

    // The digest is over the bytes THIS RUN READ, not over
    // `BackupSetFacts::manifest_sha256`. The two are the same value today
    // (`OsoCliEngine::describe` hashes the bytes it read the same way), and
    // they are read here anyway: `describe` reaches the archive through the
    // engine's OWN store handle, so a receipt quoting only the engine's
    // number would be attesting bytes this process never saw.
    //
    // **And the version id that same read was answered with.** It is
    // the version of exactly these bytes — one response carries both — so a
    // receipt that pins it names the object version its digest is over. The
    // engine rewrites `<backup_id>/manifest.json` with its own unconditional
    // put, several times in one run; what is pinned is the version this run
    // read back AFTER the engine exited, i.e. the one the receipt attests.
    let (manifest_bytes, answered_version) = store
        .get_capped(
            &set.manifest_key,
            logweir_engine_oso::storage::caps::MANIFEST,
        )
        .map_err(|e| BackupError::Operational(e.to_string()))?;
    let manifest_sha256 = logweir_core::ids::sha256_prefixed(&manifest_bytes);
    let manifest_version_id =
        logweir_core::backup_receipt::pinnable_version_id(answered_version.as_deref());

    // `describe_with_notices`, not `describe`: what the engine found that no
    // signed field carries — today an unreadable consumer-groups snapshot
    // beside this set's manifest — is told to the operator, never dropped. It
    // changes nothing the receipt says.
    let (archive, notices) = engine.describe_with_notices(&set)?;
    crate::drill::surface_archive_notices(&mut std::io::stderr().lock(), &set.backup_id, &notices);

    // **ONE ENTRY PER NAMED TOPIC, AND NO OTHERS** — which is
    // `BackupReceipt::validate_invariants`'s arm 3, and which this loop has to
    // satisfy by construction rather than by luck.
    //
    // `DataEngine::describe` answers about the whole backup SET, not about this
    // run: a set an earlier run appended to holds that run's topics too. So
    // the counted set is seeded from `plan.topics` — the named allowlist,
    // GC18(c) rail 1 — and a topic the archive reports that this plan did not
    // name is SKIPPED, because it is not something this run captured and a
    // receipt that counted it would be describing two runs at once. Without
    // both halves a receipt built from a shared backup set violates its own
    // arm 3 and exits 4 with an archive on disk and no evidence for it.
    //
    // A named topic the archive does not mention keeps its seeded `0`: a
    // backup of an empty topic is a real backup that exits 0, and the receipt
    // says "zero records" rather than omitting the topic and contradicting the
    // list beside it. (A set with NO segment at all is still refused below —
    // that is a different claim: nothing was captured for ANY named topic.)
    let mut records_per_topic: BTreeMap<String, u64> =
        plan.topics.iter().map(|t| (t.clone(), 0u64)).collect();
    let mut manifest_configurations: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
    let mut manifest_layouts: BTreeMap<String, crate::backup::config_coverage::Layout> =
        BTreeMap::new();
    let mut oldest: Option<i64> = None;
    let mut newest: Option<i64> = None;
    let mut manifest_ranges = crate::backup::consumer_positions::ArchivedRanges::new();
    for topic in &archive.topics {
        let Some(entry) = records_per_topic.get_mut(&topic.name) else {
            continue;
        };
        manifest_ranges.insert(
            topic.name.clone(),
            crate::backup::consumer_positions::archived_ranges(&topic.partitions),
        );
        manifest_configurations.insert(topic.name.clone(), topic.configurations.clone());
        manifest_layouts.insert(
            topic.name.clone(),
            (
                topic.original_partition_count,
                topic.source_replication_factor,
            ),
        );
        for partition in &topic.partitions {
            for segment in &partition.segments {
                // `record_count` is `i64` on the wire. A negative count is
                // not a smaller number, it is a manifest this build cannot
                // read as a count, so it saturates at 0 rather than wrapping
                // into a colossal `u64`.
                *entry += segment.record_count.max(0) as u64;
                oldest = Some(oldest.map_or(segment.start_timestamp, |o: i64| {
                    o.min(segment.start_timestamp)
                }));
                newest = Some(
                    newest.map_or(segment.end_timestamp, |n: i64| n.max(segment.end_timestamp)),
                );
            }
        }
    }
    // A set with no segment FOR ANY NAMED TOPIC bounds no window: `[0, 0]`
    // would read as "this archive covers the epoch", so it is `None` and the
    // caller decides ([`empty_capture_window`]): an empty capture when the
    // source's watermarks say it held no record, exit 1 otherwise.
    //
    // The window is half-open, and this is where it becomes so (I22).
    // `segment.end_timestamp` is a record the archive HOLDS, so the range is
    // inclusive; the receipt and `Backup.status.windowCovered` carry an
    // EXCLUSIVE `to_ms`, and `BackupReceipt::validate_invariants`'s arm 4
    // requires `from_ms < to_ms`. A topic whose records share one millisecond
    // becomes `[t, t+1)`, never the empty `[t, t]`. One conversion, in the one
    // place that measures the window; `saturating_add`, because an
    // `end_timestamp` of `i64::MAX` must neither panic nor wrap.
    let covered = oldest
        .zip(newest)
        .map(|(from_ms, newest_inclusive)| (from_ms, newest_inclusive.saturating_add(1)));

    // The named topics' schema dependency, from the archived
    // BYTES this run wrote: a bounded sample of each topic's segments, read
    // back through the read-only archive handle and decoded with Logweir's own
    // `.kbak` decoder. No registry is contacted. Never fatal.
    let schema_dependency = crate::backup::schema_dependency::detect(&archive, &plan.topics, store);

    Ok(Ran {
        facts,
        manifest_key: set.manifest_key,
        manifest_sha256,
        manifest_version_id,
        records_per_topic,
        covered,
        manifest_configurations,
        manifest_layouts,
        schema_dependency,
        manifest_ranges,
    })
}

/// The receipt's `source_watermarks` from ONE marks read taken
/// before the engine (`ClusterReader::partition_marks`), or why there is
/// none. Every named topic must answer with partitions `0..n` (n ≥ 1), each
/// with its marks, and the block must fit
/// `SOURCE_WATERMARKS_BUDGET_BYTES`; anything else — a failed
/// metadata or marks read, a gap in the ids, a block over the budget — leaves
/// the block absent, which a reader takes as coverage NOT RECORDED.
pub fn source_watermarks(
    topics: &[String],
    marks: &BTreeMap<String, logweir_kafka::capture::TopicMarks>,
) -> Result<BTreeMap<String, Vec<[u64; 2]>>, String> {
    let mut block = BTreeMap::new();
    for topic in topics {
        let partitions = match marks.get(topic) {
            None => return Err(format!("{topic}: no marks were read")),
            Some(Err(e)) => return Err(e.clone()),
            Some(Ok(partitions)) => partitions,
        };
        let mut sorted: Vec<&(i32, Result<logweir_kafka::capture::Marks, String>)> =
            partitions.iter().collect();
        sorted.sort_by_key(|(p, _)| *p);
        if sorted.is_empty() {
            return Err(format!("{topic}: the metadata listed no partition"));
        }
        let mut pairs = Vec::with_capacity(sorted.len());
        for (index, (partition, read)) in sorted.into_iter().enumerate() {
            if usize::try_from(*partition).ok() != Some(index) {
                return Err(format!(
                    "{topic}: the metadata's partition ids are not 0..{}",
                    partitions.len()
                ));
            }
            let m = read.as_ref().map_err(Clone::clone)?;
            // `marks_of` admits only `0 <= log_start <= high_watermark`.
            let low =
                u64::try_from(m.log_start).map_err(|e| format!("{topic}:{partition}: {e}"))?;
            let high =
                u64::try_from(m.high_watermark).map_err(|e| format!("{topic}:{partition}: {e}"))?;
            pairs.push([low, high]);
        }
        block.insert(topic.clone(), pairs);
    }
    // The block grows with the partition count, and a receipt over the bound
    // every reader caps it at (`topic_budget::MAX_RECEIPT_BYTES`) reads
    // Unreadable. Over its budget it is NOT recorded, and an empty capture
    // then stays exit 1.
    let budget = logweir_core::backup_receipt::SOURCE_WATERMARKS_BUDGET_BYTES;
    let encoded = logweir_core::backup_receipt::source_watermarks_encoded_len(&block)
        .map_err(|e| format!("the source_watermarks block could not be encoded: {e}"))?;
    if encoded > budget {
        let partitions: usize = block.values().map(Vec::len).sum();
        return Err(format!(
            "the source_watermarks block of {partitions} partition(s) would take {encoded} bytes \
             of the receipt, over its {budget}-byte budget"
        ));
    }
    Ok(block)
}

/// **The EMPTY CAPTURE.** The covered window a run signs when its
/// backup set holds no segment for any named topic: the empty `[t, t)`, `t`
/// the engine's start, which covers no record — when the source's
/// watermarks, read before the engine, say every selected partition held no
/// record (`low == high`). Otherwise the run is refused exit 1,
/// naming why: the source held records the engine did not capture, or its
/// watermarks could not be read, so its emptiness is not known.
pub fn empty_capture_window(
    backup_id: &str,
    facts: &BackupFacts,
    records: &BTreeMap<String, u64>,
    watermarks: Result<&BTreeMap<String, Vec<[u64; 2]>>, &String>,
) -> Result<(i64, i64), BackupError> {
    let why = match watermarks {
        Err(e) => format!(
            "the source's watermarks were not recorded before the engine ({e}), so whether it \
             held any record is not known"
        ),
        Ok(block) => {
            match logweir_core::backup_receipt::empty_capture_contradiction(records, block) {
                None => {
                    let t = facts.started_at.timestamp_millis();
                    return Ok((t, t));
                }
                Some(c) => format!(
                    "the source was not empty before the engine ({c}), so the engine captured \
                     nothing it held"
                ),
            }
        }
    };
    Err(BackupError::Operational(format!(
        "backup set `{backup_id}` declares no segment for any of the named topics, so it bounds \
         no window, and {why}. Nothing was signed; run the backup again."
    )))
}

// The receipt: built, validated, signed, put, and written locally on request.

/// Where the receipt landed. Both keys are printed as `backup run`'s final two
/// stdout lines (**I7**) and both are read by the Backup reconciler.
#[derive(Debug, Clone)]
pub struct Persisted {
    /// `logweir/backups/<backup_id>/<run_id>.receipt.json`
    pub receipt_key: String,
    /// `logweir/backups/<backup_id>/<run_id>.receipt.sig`
    pub sidecar_key: String,
    /// `logweir/backups/<backup_id>/<run_id>.consumer-positions.json`, when
    /// the run selected consumer groups: the positions document
    /// the receipt binds by digest, put BEFORE the receipt so a receipt never
    /// names a document that is not there.
    pub positions_key: Option<String>,
    /// `sha256:<lowercase hex>` over the exact receipt bytes uploaded above.
    /// Public capture metadata; it says nothing about whether a controller has
    /// verified the receipt's signature.
    pub receipt_sha256: String,
    /// `logweir/catalog/v1/points/<pointId>/record.json`, when the catalog
    /// point record was written (D3 §5.2).
    ///
    /// `None` when the catalog write FAILED — which is a warning and nothing
    /// more. See `persist_receipt`'s step 6: the archive and its signed
    /// evidence already exist by then, so a metadata index that could not be
    /// written must not change this run's exit code, its receipt or its two
    /// evidence keys. The `catalog sync` backfill exists precisely to close
    /// that gap later.
    pub catalog_key: Option<String>,
}

/// The evidence keys for one run — Global Constraint 6's `logweir/` root, and
/// the two keys interface I7 prints.
///
/// Derived in ONE place, from the two ids, so the key the runner prints and the
/// key it put cannot differ. `Store::put_create_only` asserts the `logweir/`
/// prefix (`crates/logweir-store/src/lib.rs:626-629`), which is what makes the
/// prefix an assertion rather than a convention: a mutant that puts the
/// receipt anywhere else aborts inside the store rather than writing it.
pub fn receipt_keys(backup_id: &str, run_id: &str) -> Persisted {
    Persisted {
        receipt_key: format!("logweir/backups/{backup_id}/{run_id}.receipt.json"),
        sidecar_key: format!("logweir/backups/{backup_id}/{run_id}.receipt.sig"),
        positions_key: None,
        receipt_sha256: String::new(),
        // Not knowable from the two ids: the point id is derived from the
        // receipt's BYTES (D3 §5.1), which do not exist yet at this call.
        // Filled in by `persist_receipt`'s step 6.
        catalog_key: None,
    }
}

/// `BackupOutcome` -> the document. A pure projection: every field is a value
/// the outcome already carries, and nothing here measures anything.
///
/// `format_version` is decided by
/// `logweir_core::backup_receipt::format_version_for`, the one place, for
/// THIS document type (independent of the scorecard's): at least 1.6.0
/// (`FORMAT_VERSION_WITH_GENERATIONS`, which defines every auth mode),
/// because this build writes `generations` and `schema_dependency` on every
/// receipt. `source.auth` is `BackupOutcome::source_auth` rendered as the two
/// strings `ReceiptAuth` holds — **never a password, and no field that could
/// hold one**.
///
/// `config_coverage` is ALWAYS written: a receipt this build signs never
/// leaves a topic's configuration coverage to be read as unknown by omission
/// when it was measured.
pub fn build_receipt(outcome: &crate::backup::BackupOutcome) -> BackupReceipt {
    let archive = ReceiptArchive {
        manifest_key: outcome.manifest_key.clone(),
        manifest_sha256: outcome.manifest_sha256.clone(),
        manifest_version_id: outcome.manifest_version_id.clone(),
        prefix: outcome.archive_prefix.clone(),
    };
    let auth = receipt_auth(&outcome.source_auth);
    BackupReceipt {
        // Every receipt this build signs carries `schema_dependency` (1.5.0)
        // and `generations` (1.6.0), so every one is at least 1.6.0, which
        // defines every auth mode and the `topic_configuration` block too;
        // 1.7.0, which defines `consumer_positions`, when the run selected
        // consumer groups.
        format_version: logweir_core::backup_receipt::format_version_with_schema_registry(
            logweir_core::backup_receipt::format_version_for(
                &archive,
                true,
                true,
                &auth,
                true,
                outcome.consumer_positions.is_some(),
                outcome.source_watermarks.is_some(),
            ),
            outcome.schema_registry.is_some(),
        )
        .to_string(),
        run_id: outcome.run_id.clone(),
        backup_id: outcome.backup_id.clone(),
        requested_at: outcome.requested_at,
        // Logweir-measured, from the engine subprocess — `backup` has no
        // `--format` and writes no report file, so these two and the exit code
        // are the only facts the process itself yields.
        started_at: outcome.facts.started_at,
        finished_at: outcome.facts.finished_at,
        exit_code: outcome.facts.exit_code,
        triggered_by: outcome.triggered_by.clone(),
        source: ReceiptSource {
            cluster_id: outcome.source_cluster_id.clone(),
            bootstrap_servers: outcome.bootstrap_servers.clone(),
            auth,
            topics: outcome.topics.clone(),
        },
        engine: ReceiptEngine {
            id: outcome.engine.id.clone(),
            version: outcome.engine.version.clone(),
            digest: outcome.engine.digest.clone(),
        },
        archive,
        records: outcome.records_per_topic.clone(),
        covered: ReceiptCovered {
            from_ms: outcome.covered_from_ms,
            // EXCLUSIVE (I22). The conversion happened in `run` above, once.
            to_ms: outcome.covered_to_ms,
        },
        config_coverage: Some(outcome.config_coverage.clone()),
        // ALWAYS written beside `config_coverage`, so a receipt never leaves
        // its topics' configuration model to be read as NOT RECORDED by
        // omission when it was observed.
        topic_configuration: Some(outcome.topic_configuration.clone()),
        // Where the run looked for owners — written beside the
        // model, so a topic without an owner reads "not checked" when it is
        // empty and never "applied through the admin API".
        owner_detection: Some(outcome.owner_detection.clone()),
        // ALWAYS written, so every receipt says, per named topic, whether its
        // records need a schema registry, never left to be read as NOT
        // ASSESSED by omission when it was judged.
        schema_dependency: Some(outcome.schema_dependency.clone()),
        // ALWAYS written, one entry per named topic, so a receipt
        // never leaves a topic's ID to be read as unknown by omission when it
        // was read — and a null ID always says why.
        generations: Some(outcome.generations.clone()),
        // Present exactly when the run selected consumer groups.
        consumer_positions: outcome.consumer_positions.clone(),
        // Present exactly when the marks read before the engine
        // answered for every named partition (1.8.0); absent is coverage NOT
        // RECORDED.
        source_watermarks: outcome.source_watermarks.clone(),
        // Present exactly when the plan named a source registry (1.9.0).
        schema_registry: outcome.schema_registry.clone(),
    }
}

/// The schema dependency block is advisory:
/// it must never be the reason a backup goes unsigned. When the receipt's own
/// arms refuse the block — a manifest this build did not foresee, a detector
/// defect — every topic's entry becomes `notAssessed (segmentUnreadable)`,
/// which the arms always accept, and the run says so at `warn`. Every other
/// refusal is left for step 1 of `persist_receipt` to report.
pub fn signable_schema_dependency(mut receipt: BackupReceipt) -> BackupReceipt {
    let Err(refusal) = receipt.validate_invariants() else {
        return receipt;
    };
    if !refusal.starts_with("schema_dependency") {
        return receipt;
    }
    tracing::warn!(
        run_id = %receipt.run_id,
        refusal = %refusal,
        "the schema dependency this run judged contradicts the receipt; every topic's is          recorded as notAssessed (segmentUnreadable) so the backup is still signed"
    );
    // Keyed by the NAMED topics, so a block whose topic set was the refusal
    // (arm 23) is mended too.
    receipt.schema_dependency = Some(
        receipt
            .source
            .topics
            .iter()
            .map(|topic| {
                (
                    topic.clone(),
                    logweir_core::schema_dependency::not_assessed(
                        logweir_core::schema_dependency::REASON_SEGMENT_UNREADABLE,
                    ),
                )
            })
            .collect(),
    );
    receipt
}

/// `AuthRender` -> `ReceiptAuth`. The wire spellings, and the ONE place they
/// are chosen for this document.
///
/// One spelling per mode in the whole product: `AuthRender::mode_str` is
/// `AuthSpec::mode_str`'s twin, so the receipt carries `scramSha512`,
/// `scramSha256`, `plain`, `mtls` or `plaintext` byte for byte as the spec,
/// the `KafkaCluster` CRD
/// (`crates/weirkeeper/tests/crd_shape.rs::the_crd_auth_mode_enum_and_auth_spec_agree`),
/// `Backup.status.auth.mode` and the scorecard spell them. The set is closed
/// at both readers: `BackupReceipt::validate_invariants`'s arm 5 and
/// `docs/verify_scorecard.py::check_backup_receipt_invariants` refuse any
/// other value, so `logweir backup run` would refuse its own receipt before
/// signing it.
fn receipt_auth(render: &logweir_core::engine::AuthRender) -> ReceiptAuth {
    ReceiptAuth {
        mode: render.mode_str().to_string(),
        // `None`, never `Some("")`: no username is not an empty username, and
        // `ReceiptAuth::username`'s own doc comment says so. `None` under
        // `plaintext` and `mtls`; the SASL principal under the three SASL
        // modes. Never a password: `AuthRender` has no field that holds one.
        username: render.username().map(str::to_string),
    }
}

/// **I6 + I7 + GC6.** Validate, sign, put both objects, and — when the
/// operator asked for it — write the same bytes and the sidecar locally.
///
/// # The order is the contract
///
/// 1. **Validate** the document that is about to be signed, over the exact
///    value step 2 serialises. `phase8_score` does this for the scorecard for
///    the same reason: no signed receipt may carry a self-contradicting claim,
///    and a reader refusing a document Logweir itself wrote is the worst
///    possible way to discover an arithmetic bug. This is also what makes
///    `docs/formats/backup-receipt.md`'s claim that `logweir backup run`
///    refuses a violating receipt true by EXECUTION
///    rather than by assertion.
/// 2. **Serialise** deterministically — the exact bytes that will be signed,
///    stored and (with `--receipt-out`) written to disk. Never a
///    re-serialisation afterwards: a document re-rendered after signing does
///    not verify.
/// 3. **Sign with the already validated execution signer.** Any failure from
///    here to the end of step 4 is Global Constraint 11's exit 4 — "signing or
///    lock-proof failed, nothing uploaded" — and the signing step precedes
///    every put, which is the mechanism rather than a convention.
/// 4. **Put**, create-only, both objects, under `logweir/` (GC6).
/// 5. **Write locally**, last, so a `--receipt-out` path that cannot be
///    written does not leave an operator wondering whether the evidence was
///    uploaded. It was: the two keys are already in the bucket by then.
pub(crate) fn persist_receipt(
    outcome: &crate::backup::BackupOutcome,
    signer: &ValidatedSigner,
    receipt_out: Option<&Path>,
    store: &Store,
) -> Result<Persisted, BackupError> {
    let sig = |e: String| BackupError::Signing(e);
    crate::backup::print_progress_step(crate::backup::PROGRESS_STEP_SIGN);
    let receipt = signable_schema_dependency(build_receipt(outcome));

    // 1. Refuse to sign a self-contradicting document.
    receipt.validate_invariants().map_err(|e| {
        sig(format!(
            "the backup receipt this run measured violates its own invariants and was NOT \
             signed: {e}. The archive may exist; the evidence does not. This is a bug in \
             logweir, not in the spec — please report it with this line."
        ))
    })?;
    // 1b. The positions document the receipt binds holds against
    //     it, through the same arms a reader runs (CP-1 to CP-14) — over the
    //     exact bytes that will be put.
    let positions = match (
        &receipt.consumer_positions,
        &outcome.consumer_positions_document,
    ) {
        (None, None) => None,
        (Some(_), Some(bytes)) => {
            let doc: logweir_core::consumer_positions::PositionsDocument =
                serde_json::from_slice(bytes).map_err(|e| {
                    sig(format!(
                        "the consumer positions document does not parse: {e}"
                    ))
                })?;
            receipt
                .validate_consumer_positions_document(bytes, &doc)
                .map_err(|e| {
                    sig(format!(
                        "the consumer positions document this run measured violates its own \
                         invariants and NOTHING was signed: {e}. The archive may exist; the \
                         evidence does not. This is a bug in logweir — please report it with \
                         this line."
                    ))
                })?;
            Some(bytes.as_slice())
        }
        _ => {
            return Err(sig(
                "the consumer positions block and its document are not both present; nothing \
                 was signed. This is a bug in logweir."
                    .to_string(),
            ))
        }
    };

    // 2. The EXACT bytes.
    let bytes = logweir_core::det_json::to_deterministic_json(&receipt)
        .map_err(|e| sig(format!("the backup receipt could not be serialised: {e}")))?;
    let receipt_sha256 = logweir_core::ids::sha256_prefixed(&bytes);

    // 3. Sign with the signer exercised before engine work.
    //    `logweir-evidence` is the ONE signer (Global Constraint 27): this is
    //    a call into it, exactly as `drill/phase8_score.rs` is, and no signing
    //    primitive lives here.
    let sidecar = signer
        .sign(logweir_evidence::PAYLOAD_TYPE_BACKUP_RECEIPT, &bytes)
        .map_err(BackupError::Signing)?;
    let sidecar_bytes =
        serde_json::to_vec(&sidecar).map_err(|e| sig(format!("DSSE sidecar: {e}")))?;

    // 4. Create-only puts. An object that already exists is REFUSED
    //    (`StoreError::AlreadyExists`), never overwritten, so one run can
    //    never silently replace another's evidence.
    let mut keys = receipt_keys(&outcome.backup_id, &outcome.run_id);
    keys.receipt_sha256 = receipt_sha256;
    crate::backup::print_progress_step(crate::backup::PROGRESS_STEP_UPLOAD);
    // The positions document FIRST, at the key the receipt names:
    // a receipt is never put that names a document that is not there.
    if let (Some(cp), Some(doc)) = (&receipt.consumer_positions, positions) {
        store
            .put_create_only(&cp.document.key, doc)
            .map_err(|e| sig(e.to_string()))?;
        keys.positions_key = Some(cp.document.key.clone());
    }
    store
        .put_create_only(&keys.receipt_key, &bytes)
        .map_err(|e| sig(e.to_string()))?;
    store
        .put_create_only(&keys.sidecar_key, &sidecar_bytes)
        .map_err(|e| sig(e.to_string()))?;

    // 6. **THE CATALOG POINT** (D3 §5.2) — the fifth, sixth and
    //    seventh create-only puts, and the only ones in this function whose
    //    failure is a WARNING.
    //
    //    It is here, after the two receipt puts and before the local copy,
    //    because the point's identity is `sha256` of the receipt bytes and
    //    those bytes are only final at step 2 — and because a record that
    //    named a receipt key nothing had been put to would index evidence
    //    that does not exist.
    //
    //    **A failed catalog write does not change the exit code, the receipt,
    //    or the two evidence keys.** By the time this runs, the archive
    //    exists, the receipt is signed and both objects are in the bucket:
    //    the run's result is established. The catalog is a durable INDEX over
    //    evidence that is already durable, so a bucket that refused a metadata
    //    put must not turn a successful backup into a failure an operator has
    //    to investigate. `logweir catalog sync` backfills exactly this case,
    //    which is why the backfill exists at all (D3 §5.2, "the scanner
    //    backfills").
    //
    //    **An EMPTY CAPTURE is not a recovery point**, so it gets
    //    no catalog point: nothing in it can be restored, and a point would
    //    take a retention `keepLast` rank from a real one.
    if receipt.is_empty_capture() {
        tracing::info!(
            run_id = %outcome.run_id,
            backup_id = %outcome.backup_id,
            "an empty capture holds no record and is not a recovery point; no catalog point \
             is written for it"
        );
    } else {
        keys.catalog_key = write_catalog_point(outcome, &receipt, &bytes, &keys, signer, store);
    }

    // 5. **I6.** The local pair, with the sidecar beside the document under the
    //    extension `.sig` — the pairing `drill run --out` already uses
    //    (`crates/logweir/src/drill/mod.rs`'s `write_scorecard_artifact`), so
    //    an operator learns one convention for both commands.
    //
    //    A failure HERE is exit 1 and not exit 4: the evidence is uploaded and
    //    signed, and what failed is a local copy. Saying "signing failed"
    //    would send an operator looking for a key problem that does not exist.
    if let Some(path) = receipt_out {
        std::fs::write(path, &bytes).map_err(|e| {
            BackupError::Operational(format!(
                "{}: {e} — the receipt and its sidecar ARE in the evidence bucket at {} and \
                 {}; only the local copy could not be written",
                path.display(),
                keys.receipt_key,
                keys.sidecar_key
            ))
        })?;
        let sig_path = path.with_extension("sig");
        std::fs::write(&sig_path, &sidecar_bytes).map_err(|e| {
            BackupError::Operational(format!(
                "{}: {e} — the receipt and its sidecar ARE in the evidence bucket at {} and \
                 {}; only the local sidecar could not be written",
                sig_path.display(),
                keys.receipt_key,
                keys.sidecar_key
            ))
        })?;
        // The positions document beside them, as
        // `<receipt-out stem>.consumer-positions.json`.
        if let Some(doc) = positions {
            let doc_path = positions_out(path);
            std::fs::write(&doc_path, doc).map_err(|e| {
                BackupError::Operational(format!(
                    "{}: {e} — the positions document IS in the evidence bucket at {}; only \
                     the local copy could not be written",
                    doc_path.display(),
                    keys.positions_key.as_deref().unwrap_or_default()
                ))
            })?;
        }
    }

    Ok(keys)
}

/// Where `--receipt-out <path>` writes the positions document: beside the
/// receipt, `<path stem>.consumer-positions.json` (`r.json` →
/// `r.consumer-positions.json`), as the sidecar is `<path stem>.sig`.
#[must_use]
pub fn positions_out(receipt_out: &Path) -> std::path::PathBuf {
    receipt_out.with_extension("consumer-positions.json")
}

/// **D3 §5.2** — the catalog point record for this run, written as
/// three create-only puts under `logweir/catalog/v1/`.
///
/// Returns the record's key when it was written, and `None` on ANY failure —
/// having logged it at `warn`. This function returns no `Result` on purpose:
/// a `Result` here would invite a caller to add a `?` one day, and that `?`
/// would turn a bucket that refused a metadata put into a failed backup whose
/// archive and signed receipt are both sitting in the bucket. The type is the
/// guarantee.
///
/// **It measures nothing.** Every field of the record is a value the receipt
/// already carries plus the location and the signing key, so this write cannot
/// disagree with the receipt it indexes. `recorded_at` is the one clock read,
/// and it is a fact about the record and not about the backup.
///
/// `installation` is this run's own signing key: `ValidatedSigner::load` has
/// already signed and independently VERIFIED a probe with it, and it is the
/// key that just signed the receipt this record names — so "the installation
/// whose key signed the receipt" is established here rather than assumed.
/// `execution` is absent: a Backup Job carries no execution-contract
/// environment on this build, so provenance is UNKNOWN, which is what an
/// absent optional field means (D3 §5.2 rule 2) and is the only honest value.
fn write_catalog_point(
    outcome: &crate::backup::BackupOutcome,
    receipt: &BackupReceipt,
    receipt_bytes: &[u8],
    keys: &Persisted,
    signer: &ValidatedSigner,
    store: &Store,
) -> Option<String> {
    use crate::catalog::writer::{from_receipt, put_point, RecordInputs};
    let inputs = RecordInputs {
        receipt_key: keys.receipt_key.clone(),
        sidecar_key: keys.sidecar_key.clone(),
        location_id: crate::catalog::record::location_id(&outcome.storage),
        recorded_at: chrono::Utc::now(),
        signing: crate::catalog::signing_of(&signer.verifying_key()),
        installation: Some(crate::catalog::RecordInstallation {
            key_id: signer.verifying_key().key_id(),
        }),
        execution: None,
    };
    let warn = |what: &str, detail: String| {
        tracing::warn!(
            run_id = %outcome.run_id,
            backup_id = %outcome.backup_id,
            receipt_key = %keys.receipt_key,
            error = %detail,
            "the recovery-catalog point record could not be {what}; the archive and its signed \
             receipt ARE in the bucket and this run's result is unchanged. Run `logweir catalog \
             sync` against this archive to backfill the record."
        );
        None::<String>
    };
    let point = match from_receipt(receipt, receipt_bytes, &inputs) {
        Ok(p) => p,
        Err(e) => return warn("derived", e),
    };
    let entry = crate::catalog::CatalogLogEntry::of(&point);
    match put_point(&point, &entry, signer, store) {
        Ok(o) => Some(o.keys.record_key),
        Err(e) => warn("written", e.to_string()),
    }
}
