//! `restorePreflight` — D2 §6.7's archive and target checks.
//!
//! # The plan bytes are hashed BEFORE anything else
//!
//! D2 §4.2's startup order already proved the CHECK PLAN's digest. This kind
//! carries a second document — the verbatim restore spec at
//! `/check/plan.yaml` — and `RestorePreflightRequest::plan_sha256` pins it.
//! Those bytes are read and hashed before a socket is opened, because every
//! check below is a claim ABOUT them: the topics, the recovery point, the
//! mapped names and the replication factor all come from that document, and a
//! preflight run against different bytes would be a green preview of a plan
//! nobody approved. A mismatch is `plan.parse notReady PlanHashMismatch` and
//! the check stops: no broker is dialled and no bucket is read.
//!
//! # No writes, and no topics
//!
//! D2 §6.7: "No topic is created, altered or deleted by preflight." The
//! collision answer is built from two read-only probes — targeted metadata per
//! mapped name, and a `CreateTopics` with `validate_only = true` — and never
//! from creating a probe topic, which is what the execution path's
//! `LogAppendTime` guard does and what G12 forbids here.
//!
//! # Which rows are the runner's
//!
//! `plan.parse`, `archive.*`, `target.authenticated`, `target.scratchMarker`,
//! `target.mappedTopics`, `target.topicCreate`, `target.timestampBound` and
//! `target.logAppendTime`. `plan.bindings`, `plan.names`,
//! `recoveryPoint.state`, `approval.*`, `target.clusterIdentity` and
//! `signer.rostered` are the controller's: each needs a Kubernetes object a
//! credential-holding Job is not given read on.

use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use logweir_core::check_contract::{
    CheckCode, CheckId, CheckOutcome, CheckPlanKind, CheckResult, CheckState, Gating,
    RestorePreflightRequest, Stream,
};
use logweir_core::destination::DestinationRole;
use logweir_core::replay_selection::{ReplaySelection, SelectionRefusal};
use logweir_core::spec::{target_topic_prefix, DrillSpec, TargetMode};
use logweir_engine_oso::storage::{caps, StoreError};
use logweir_kafka::inventory::{InventoryProbe, TopicPresence};
use logweir_kafka::reader::{NewTopicSpec, TARGET_TOPIC_CONFIGS};

use super::readiness::{authenticated, detail, DETAIL_SAMPLE};
use super::{
    execution_only, from_broker_failure, from_store_failure, ready, remedy_for, runner_contract,
    state_for, Wiring,
};
use crate::catalog::pin::{self, PinVerdict};
use crate::check::archive::{self, ManifestError};
use crate::check::store::{self, ObjectAccess};
use crate::check::{catalogue, Deadline, Emission};
use crate::drill::binding::{
    self, POINT_BINDING_MISMATCH, POINT_BINDING_SET_MISMATCH, POINT_PIN_UNCHECKED,
};
use logweir_core::backup_receipt::BackupReceipt;
use logweir_core::execution_contract::PointBinding;

/// `log.message.timestamp.type` — the broker key `target.logAppendTime` is
/// about.
///
/// The three constants below are the ones
/// `crate::drill::phase0_admit` reads at execution. They are private there, so
/// they are re-declared here and
/// `the_broker_config_keys_match_the_execution_guard` in
/// `crates/logweir/tests/check_cli.rs` reads that file and asserts every
/// literal still appears in it — a preflight that checked a key the run does
/// not read would be a green preview of a bound nobody enforces.
pub const BROKER_TIMESTAMP_TYPE: &str = "log.message.timestamp.type";
/// `log.message.timestamp.before.max.ms` — Kafka >= 3.6.
pub const BROKER_TIMESTAMP_BEFORE_MAX_MS: &str = "log.message.timestamp.before.max.ms";
/// `log.message.timestamp.difference.max.ms` — the pre-3.6 spelling.
pub const BROKER_TIMESTAMP_DIFFERENCE_MAX_MS: &str = "log.message.timestamp.difference.max.ms";
/// The timestamp type that overwrites every restored record's timestamp.
pub const LOG_APPEND_TIME: &str = "LogAppendTime";

/// How many segment keys a preflight will list before it gives up.
///
/// D2 §6.3's `SegmentListTooLarge` threshold. Above it the row is `unknown`
/// and says so: a preflight that held a million keys in memory to answer a
/// preview would be a preflight nobody could afford to run.
pub const SEGMENT_LIST_LIMIT: usize = 200_000;

/// The `details` stream's cap — D2 §6.7's "≤ 768 KiB, truncated with count".
pub const DETAILS_MAX_BYTES: usize = 768 * 1024;

/// Run one `restorePreflight`.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn run(req: &RestorePreflightRequest, wiring: &dyn Wiring, deadline: Deadline) -> Emission {
    let now = wiring.now();
    let wanted: BTreeSet<CheckId> = req.checks.iter().copied().collect();
    let skipped: BTreeSet<CheckId> = req.skip_checks.iter().copied().collect();
    let want = |id: CheckId| (wanted.is_empty() || wanted.contains(&id)) && !skipped.contains(&id);

    let mut checks: Vec<CheckOutcome> = Vec::new();
    let mut details: Vec<String> = Vec::new();
    checks.push(runner_contract(now));

    // 0 — FX-20c: the binding of every grant the restore presents (the
    // source's `archiveRead`, the evidence destination's `evidenceWrite`),
    // compared with no request and before the plan bytes, because it is not a
    // claim about them: a refused grant is refused whatever the plan says.
    if want(CheckId::DestinationCredentialBound) {
        let mut plans = vec![&req.source_destination];
        plans.extend(req.evidence_destination.as_ref());
        if let Some(row) = super::access::credential_bound_row(&plans, wiring) {
            checks.push(row);
        }
    }

    // 1 — THE PLAN BYTES, BEFORE ANY SOCKET.
    let spec = match plan_spec(req, wiring).and_then(|spec| {
        // PROD-11.1: a replay selection the plan states WRONGLY is a fact
        // about these bytes, like a parse failure: refused here, and nothing
        // later runs. The same shape refusals as execution's phase 0
        // (`ReplaySelection::from_spec`, the shared function).
        match ReplaySelection::from_spec(&spec) {
            Ok(_) => {}
            Err(refusal) => return Err((CheckCode::SelectionInvalid, refusal.to_string())),
        }
        // PROD-15.1 review L6: an original-name block in a shape phase 0
        // refuses (scratch mode, beside a prefix, or in a plan that does not
        // ask for complete verification) is a fact about these bytes too, in
        // the runner's own words.
        match logweir_core::original_name::refuse_shape(&spec) {
            None => Ok(spec),
            Some(why) => Err((CheckCode::TopicMappingIdentity, why)),
        }
    }) {
        Ok(spec) => {
            if want(CheckId::PlanParse) {
                checks.push(
                    ready(CheckId::PlanParse, CheckCode::PlanParsed, now).with_message(
                        "the mounted restore plan hashes to the digest this check was pinned to \
                         and parses as a Logweir restore spec",
                    ),
                );
            }
            spec
        }
        Err((code, message)) => {
            checks.push(
                catalogue::outcome(CheckId::PlanParse, CheckState::NotReady, code, now)
                    .with_message(&message)
                    .with_remedy(remedy_for(code)),
            );
            // EVERY LATER CHECK IS A CLAIM ABOUT THESE BYTES, so none of them
            // runs. Nothing was dialled and nothing was read.
            let mut result = CheckResult::new(CheckPlanKind::RestorePreflight);
            result.checks = checks;
            return Emission::of(result);
        }
    };

    // 2 — the archive, through the source destination's `archiveRead` grant.
    archive_checks(
        req,
        &spec,
        wiring,
        deadline,
        now,
        &want,
        &mut checks,
        &mut details,
    );

    // 3 — the target broker.
    target_checks(
        req,
        &spec,
        wiring,
        deadline,
        now,
        &want,
        &mut checks,
        &mut details,
    );

    // 4 — the evidence destination.
    //
    // D2 §6.3 lists `destination.evidenceWritable` for a Restore, but
    // `RestorePreflightRequest` (the landed W1 contract) carries no
    // `writeProbe` flag for it, and a check may not write unasked. The row is
    // therefore execution-only with `WriteNotProbed`, which is the same answer
    // a Backup readiness plan gives when its destination configures no probe.
    if let Some(evidence) = req.evidence_destination.as_ref() {
        if want(CheckId::DestinationEvidenceWritable) {
            checks.push(
                catalogue::outcome_gated(
                    CheckId::DestinationEvidenceWritable,
                    CheckState::Unknown,
                    CheckCode::WriteNotProbed,
                    Gating::ExecutionOnly,
                    now,
                )
                .with_message(
                    "a restore preflight plan carries no create-only write probe, so the \
                     evidence-write grant is verified when the restore executes",
                )
                .with_remedy(remedy_for(CheckCode::WriteNotProbed))
                .with_scope(catalogue::scope(
                    "BackupDestination",
                    &evidence.name,
                    Some(&evidence.uid),
                )),
            );
        }
    }

    let mut result = CheckResult::new(CheckPlanKind::RestorePreflight);
    result.checks = checks;
    let mut emission = Emission::of(result);
    if !details.is_empty() {
        emission
            .extra
            .push((Stream::Details, details_stream(&details)));
    }
    emission
}

/// The verbatim plan bytes, hashed and parsed.
///
/// # Errors
/// `(code, message)` for the `plan.parse` row.
fn plan_spec(
    req: &RestorePreflightRequest,
    wiring: &dyn Wiring,
) -> Result<DrillSpec, (CheckCode, String)> {
    let bytes = wiring.read_bytes(&req.plan_file).map_err(|e| {
        (
            CheckCode::PlanUnparseable,
            format!(
                "the restore plan at `{}` could not be read ({})",
                req.plan_file,
                e.kind()
            ),
        )
    })?;
    let got = logweir_core::ids::sha256_prefixed(&bytes);
    if got != req.plan_sha256 {
        return Err((
            CheckCode::PlanHashMismatch,
            format!(
                "the restore plan at `{}` hashes to {got}, not to the {} this check is bound \
                 to; no broker was dialled and no archive was read",
                req.plan_file, req.plan_sha256
            ),
        ));
    }
    serde_yaml::from_slice::<DrillSpec>(&bytes).map_err(|_| {
        (
            CheckCode::PlanUnparseable,
            format!(
                "the restore plan at `{}` hashes correctly but is not a Logweir restore spec",
                req.plan_file
            ),
        )
    })
}

/// The recovery point this plan asks for: `restore.point_in_time` when the
/// spec states one, else `sample.window_end`.
///
/// The SAME pairing `crate::drill::build_plan_with_floor` makes for
/// `time_window.1` and `phase0_admit::target_topic_preflight` makes for the
/// broker's timestamp bound. Reading `sample.window_end` unconditionally would
/// check a bound against an instant this restore never requests.
#[must_use]
pub fn recovery_point(spec: &DrillSpec) -> DateTime<Utc> {
    spec.restore.point_in_time.unwrap_or(spec.sample.window_end)
}

/// Why `archive.backupSet` refuses a plan bound to a recovery point: a code,
/// a message this module composed, and the remedy.
///
/// **No message carries anything read from the store** — not an object's
/// digest, not a version id, not a field of the receipt. A preflight runs
/// before any approval, for whoever can create one, so its answer names only
/// what the plan's author wrote (the point id, the receipt key, the bound
/// digests) and the request's own set and manifest key.
#[derive(Debug)]
struct PointRefusal {
    code: CheckCode,
    message: String,
    remedy: &'static str,
}

impl PointRefusal {
    /// A refusal of the PLAN's point: the runner refuses the same with exit 3
    /// and the token the message opens with.
    fn mismatch(message: String) -> Self {
        Self {
            code: CheckCode::PointBindingMismatch,
            message,
            remedy: remedy_for(CheckCode::PointBindingMismatch),
        }
    }

    /// A store failure, classified; never the backend's own text.
    fn store(error: &StoreError, message: String, remedy: Option<&'static str>) -> Self {
        let code = store::classify(error);
        Self {
            code,
            message: format!("{message}: {code}"),
            remedy: remedy.unwrap_or_else(|| remedy_for(code)),
        }
    }
}

/// Whether two object keys name one object: the comparison
/// `Store::engine_manifest_key` makes, without empty path segments (an
/// `object_store` path drops them, so `a//b/` and `a/b` are read as one key).
fn same_object_key(a: &str, b: &str) -> bool {
    a.split('/')
        .filter(|s| !s.is_empty())
        .eq(b.split('/').filter(|s| !s.is_empty()))
}

/// **FX-14 — what a plan's point binding may make this check READ, decided
/// from the plan alone, before any store call.**
///
/// A preflight runs BEFORE any approval, for anyone who can create a
/// `Preflight` or a `Restore` in the namespace, with the namespace's
/// archive-read credential. `source.point.receipt_key` is that person's free
/// text. Read on its word, it would be a probe of the whole bucket — does
/// this object exist, do its bytes hash to my guess — and in a bucket shared
/// by prefix it would reach other tenants' objects. So three things are
/// settled here, and a plan that fails any of them is refused with nothing
/// read:
///
/// 1. the binding is well-formed ([`binding::point_shape_faults`]);
/// 2. the plan restores the set this check reads: `source.backup` is the
///    request's `backupId`. A bound plan names its point's own set (FX-16;
///    the runner refuses any other, `latestCompleted` included);
/// 3. the receipt key is the WRITER's key for that set —
///    `logweir/backups/<backupId>/<run id>.receipt.json`, re-derived by
///    [`crate::catalog::record::receipt_run_id`] and compared whole. Another
///    prefix, another set, a relative or nested path, any object that is not a
///    receipt: none of them is read.
///
/// What remains readable is one object in the receipt namespace of the plan's
/// own set, and [`judge_bound_point`] reads it only after that set's manifest
/// was read under the destination's own prefix.
fn confine_bound_point(
    point: &PointBinding,
    spec: &DrillSpec,
    backup_id: &str,
) -> Result<(), PointRefusal> {
    let id = &point.point_id;
    let faults = binding::point_shape_faults(point);
    if !faults.is_empty() {
        return Err(PointRefusal::mismatch(format!(
            "{POINT_BINDING_MISMATCH}. The plan's recovery point binding is malformed: {}; \
             nothing was read",
            faults.join("; ")
        )));
    }
    if spec.source.backup != backup_id {
        return Err(PointRefusal::mismatch(format!(
            "{POINT_BINDING_SET_MISMATCH}. The plan is bound to recovery point {id} and its \
             source.backup names `{}`, but this restore reads set `{backup_id}`; a bound plan \
             names its point's own set. Nothing was read",
            spec.source.backup
        )));
    }
    if crate::catalog::record::receipt_run_id(&point.receipt_key, backup_id).is_none() {
        return Err(PointRefusal::mismatch(format!(
            "{POINT_BINDING_MISMATCH}. The plan's source.point.receipt_key `{}` is not a receipt \
             of set `{backup_id}` (`{}{backup_id}/<run id>{}`); it was not read",
            point.receipt_key,
            crate::catalog::record::RECEIPTS_PREFIX,
            crate::catalog::record::RECEIPT_SUFFIX
        )));
    }
    Ok(())
}

/// **FX-14 — the manifest this preflight read, judged by the point the plan
/// is bound to, as the runner's binding will judge it**
/// (`drill::binding::verify_point_binding`). `Ok(Some(note))` when the receipt's
/// pin could not be checked in this bucket and the digest decided; `Ok(None)`
/// when nothing needs saying. Called only for a point [`confine_bound_point`]
/// accepted.
///
/// The binding's steps, in its order, with its verdicts:
///
/// 1. the receipt, at its confined key (bucket-absolute, never qualified, as
///    the binding reads it), held to the plan's digest before a byte of it is
///    parsed; the point id it derives; the manifest digest it attests;
/// 2. the set it describes is the plan's, and the manifest it names is the
///    one this preflight read (FX-16; the binding's `PointBindingSetMismatch`);
/// 3. FX-7's pin, through the one shared [`pin::judge`]: `Superseded` refuses
///    (`ManifestSuperseded`), `Unreadable` is "could not tell" with the remedy
///    that names `s3:GetObjectVersion`, `Unchecked` is the note;
/// 4. the current bytes' digest against the bound one.
///
/// **One answer for "absent", "not these bytes" and "over the read cap".** A
/// receipt that is not there, one whose bytes are not the bound digest, and
/// an object larger than this build reads for a receipt are the same refusal
/// with the same message: either way the archive does not hold the point the
/// plan was drafted for, and the difference would tell the plan's author
/// whether an object exists at a key they chose. A store FAILURE keeps its
/// own code — a missing grant is something to fix, and it is a fact about the
/// credential.
///
/// **The reads name their caps (FX-31), in the runner binding's own classes**
/// (`drill::binding::verify_point_binding`), so the two readers agree on
/// which objects can be read at all: the receipt under
/// [`caps::SIGNED_DOCUMENT`], the pinned version of the manifest under
/// [`caps::MANIFEST`]. An object over the receipt cap is refused on the size
/// the store reports, before a body byte is taken, by the one `get` an absent
/// receipt costs; its size is never repeated. `StoreError::TooLarge` is a
/// fact about the OBJECT ("it is there, and it is this big"), so it must not
/// reach the store-failure arm below, which would answer with a code of its
/// own and tell an absent key from a present one.
///
/// What it does NOT repeat is the receipt's SIGNATURE: a check Job is given
/// no evidence keyring, and the controller's `recoveryPoint.state` judges the
/// catalog row's signer against the namespace's current trust. The runner
/// verifies the signature before any data moves.
fn judge_bound_point(
    point: &PointBinding,
    spec: &DrillSpec,
    manifest_key: &str,
    manifest: &[u8],
    answered_version: Option<&str>,
    access: &dyn ObjectAccess,
) -> Result<Option<String>, PointRefusal> {
    let id = &point.point_id;
    let receipt_key = &point.receipt_key;
    let not_held = || {
        PointRefusal::mismatch(format!(
            "{POINT_BINDING_MISMATCH}. This archive does not hold the receipt the plan binds as \
             recovery point {id} at `{receipt_key}`: it is absent, its bytes do not hash to the \
             bound digest, or it is larger than the {}-byte read cap for a receipt",
            caps::SIGNED_DOCUMENT
        ))
    };
    let receipt_bytes = match access.get(receipt_key, caps::SIGNED_DOCUMENT) {
        Ok(bytes) => bytes,
        // ABSENT and OVER THE CAP are the answer of "not these bytes": one
        // refusal, one message, one `get`.
        Err(StoreError::NotFound(_) | StoreError::TooLarge { .. }) => return Err(not_held()),
        Err(error) => {
            return Err(PointRefusal::store(
                &error,
                format!("the receipt `{receipt_key}` of recovery point {id} could not be read"),
                None,
            ))
        }
    };
    if logweir_core::ids::sha256_prefixed(&receipt_bytes) != point.receipt_sha256 {
        return Err(not_held());
    }
    // From here the bytes ARE the ones the plan binds, so its author already
    // holds their digest. Still nothing read out of them is repeated.
    if crate::catalog::record::point_id(&receipt_bytes) != *id {
        return Err(PointRefusal::mismatch(format!(
            "{POINT_BINDING_MISMATCH}. The receipt at `{receipt_key}` does not derive the \
             recovery point id {id} the plan is bound to"
        )));
    }
    let Ok(receipt) = serde_json::from_slice::<BackupReceipt>(&receipt_bytes) else {
        return Err(PointRefusal::mismatch(format!(
            "{POINT_BINDING_MISMATCH}. The bytes bound as recovery point {id} are not a backup \
             receipt"
        )));
    };
    if receipt.archive.manifest_sha256 != point.manifest_sha256 {
        return Err(PointRefusal::mismatch(format!(
            "{POINT_BINDING_MISMATCH}. Recovery point {id}'s receipt does not attest the manifest \
             digest the plan is bound to ({})",
            point.manifest_sha256
        )));
    }
    if receipt.backup_id != spec.source.backup
        || !same_object_key(&receipt.archive.manifest_key, manifest_key)
    {
        return Err(PointRefusal::mismatch(format!(
            "{POINT_BINDING_SET_MISMATCH}. Recovery point {id}'s receipt describes another backup \
             set or manifest key than the one this restore reads (set `{}` at `{manifest_key}`)",
            spec.source.backup
        )));
    }

    let mut note = None;
    match pin::judge(
        receipt.archive.manifest_version_id.as_deref(),
        answered_version,
        &point.manifest_sha256,
        |version| access.get_version(manifest_key, version, caps::MANIFEST),
    ) {
        PinVerdict::Unpinned | PinVerdict::Current => {}
        PinVerdict::Superseded { .. } => {
            return Err(PointRefusal {
                code: CheckCode::ManifestSuperseded,
                message: format!(
                    "{POINT_BINDING_MISMATCH}. Recovery point {id}'s manifest `{manifest_key}` \
                     was written again after the point was signed: the bucket still holds the \
                     version the receipt pins, and it is no longer the current one"
                ),
                remedy: pin::SUPERSEDED_REMEDY,
            })
        }
        PinVerdict::Unchecked { .. } => {
            note = Some(format!(
                "{POINT_PIN_UNCHECKED}: the receipt pins a manifest version this bucket does not \
                 hold, so the manifest was checked by its digest alone"
            ));
        }
        PinVerdict::Unreadable { error, .. } => {
            return Err(PointRefusal::store(
                &error,
                format!(
                    "recovery point {id}'s manifest `{manifest_key}` could not be read at the \
                     version its receipt pins, so whether the set was written again cannot be \
                     told"
                ),
                Some(pin::UNREADABLE_REMEDY),
            ))
        }
    }
    if logweir_core::ids::sha256_prefixed(manifest) != point.manifest_sha256 {
        return Err(PointRefusal::mismatch(format!(
            "{POINT_BINDING_MISMATCH}. Recovery point {id}'s manifest `{manifest_key}` does not \
             hash to the bound {}{}",
            point.manifest_sha256,
            note.as_deref()
                .map(|n| format!("; {n}"))
                .unwrap_or_default()
        )));
    }
    Ok(note)
}

/// The `archive.backupSet` row a [`PointRefusal`] becomes, with the two rows
/// that would describe a manifest the run will not accept held back.
fn refuse_bound_point(
    refusal: &PointRefusal,
    scope: logweir_core::check_contract::CheckScope,
    want: &dyn Fn(CheckId) -> bool,
    now: DateTime<Utc>,
    checks: &mut Vec<CheckOutcome>,
) {
    if want(CheckId::ArchiveBackupSet) {
        checks.push(
            catalogue::outcome(
                CheckId::ArchiveBackupSet,
                state_for(refusal.code),
                refusal.code,
                now,
            )
            .with_message(&refusal.message)
            .with_remedy(refusal.remedy)
            .with_scope(scope),
        );
    }
    block_rest(
        &[CheckId::ArchiveCoverage, CheckId::ArchiveSegments],
        want,
        now,
        checks,
    );
}

#[allow(clippy::too_many_arguments)]
fn archive_checks(
    req: &RestorePreflightRequest,
    spec: &DrillSpec,
    wiring: &dyn Wiring,
    deadline: Deadline,
    now: DateTime<Utc>,
    want: &dyn Fn(CheckId) -> bool,
    checks: &mut Vec<CheckOutcome>,
    details: &mut Vec<String>,
) {
    let dest = &req.source_destination;
    let scope = catalogue::scope("BackupDestination", &dest.name, Some(&dest.uid));
    // **FX-14 — WHAT A BOUND PLAN MAY MAKE THIS CHECK READ IS DECIDED FIRST,
    // FROM THE PLAN ALONE.** No handle is opened and no object is read for a
    // plan whose point binding is malformed, names another set than the one
    // this check reads, or names a receipt key outside that set's own receipt
    // namespace ([`confine_bound_point`]).
    if let Some(point) = spec.source.point.as_ref() {
        if let Err(refusal) = confine_bound_point(point, spec, &req.backup_id) {
            refuse_bound_point(&refusal, scope, want, now, checks);
            return;
        }
    }
    let budget = deadline.slice(2);
    let access = match wiring.objects(dest, DestinationRole::ArchiveRead, budget) {
        Ok(a) => a,
        Err(f) => {
            if want(CheckId::ArchiveBackupSet) {
                checks
                    .push(from_store_failure(CheckId::ArchiveBackupSet, &f, now).with_scope(scope));
            }
            block_rest(
                &[CheckId::ArchiveCoverage, CheckId::ArchiveSegments],
                want,
                now,
                checks,
            );
            return;
        }
    };

    // THE PREFIX IS JOINED HERE, ONCE, AND EVERY LATER KEY IS DERIVED FROM
    // THE RESULT. `RestorePreflightRequest::manifest_key` is the archive's own
    // convention — `<backupId>/manifest.json`, RELATIVE to the destination's
    // `storage.prefix`, which is how the backup engine writes it and how the
    // manifest names its own segments — and `ObjectAccess::get` and
    // `list_page` operate in the fully-qualified key space. Reading it
    // unqualified made every restore preflight against a prefixed destination
    // answer `archive.backupSet notReady AccessDenied` for a manifest the same
    // principal reads with `mc`, because the object is not at the bucket root
    // and listing the root is denied (D2-PREFLIGHT-PREFIX; `d2w14.result.md`
    // §5.2, `objects/s16/preflight-pf-flat.json`). It is the same fix
    // `Store::segment_keys_for` already carries, in the one place that had
    // been left out, and `segment_keys_for_topics` below has always qualified
    // — so `expected` and `listed` were being compared in two different key
    // spaces as well.
    let manifest_key = access.qualify(&req.manifest_key);
    // FX-31: under the manifest cap; an object over it is refused unread, and
    // the message below names the cap.
    // WITH THE VERSION THE STORE ANSWERED (FX-14): a plan bound to a recovery
    // point is judged below against the point's pinned version, exactly as the
    // runner's binding judges it. For a plan bound to nothing the version is
    // read and never used.
    let (bytes, answered_version) = match access.get_with_version(&manifest_key, caps::MANIFEST) {
        Ok(read) => read,
        Err(e) => {
            let class = store::classify(&e);
            // `TooLarge` is Logweir's own sentence about the object (its key
            // and the cap), never the backend's text, so it may be named.
            let why = match &e {
                StoreError::TooLarge { cap, .. } => {
                    format!("{class}: it is larger than the {cap}-byte read cap for a manifest")
                }
                _ => class.to_string(),
            };
            // NOTHING REFUSED, so the store's remedy would send an operator
            // to a bucket policy that is fine (review F7): the object is too
            // big for this build to read.
            let remedy = if matches!(e, StoreError::TooLarge { .. }) {
                OVER_CAP_MANIFEST_REMEDY
            } else {
                remedy_for(if class == CheckCode::ObjectNotFound {
                    CheckCode::BackupSetNotFound
                } else {
                    class
                })
            };
            // A manifest that is NOT THERE is the backup set not being there,
            // which is the fact an operator acts on; every other refusal keeps
            // its store code so the remedy points at the bucket policy rather
            // than at the recovery point.
            let code = if class == CheckCode::ObjectNotFound {
                CheckCode::BackupSetNotFound
            } else {
                class
            };
            if want(CheckId::ArchiveBackupSet) {
                checks.push(
                    catalogue::outcome(CheckId::ArchiveBackupSet, state_for(code), code, now)
                        .with_message(&format!(
                            "the backup manifest `{manifest_key}` on destination `{}` could not \
                             be read: {why}",
                            dest.name
                        ))
                        .with_remedy(remedy)
                        .with_scope(scope),
                );
            }
            block_rest(
                &[CheckId::ArchiveCoverage, CheckId::ArchiveSegments],
                want,
                now,
                checks,
            );
            return;
        }
    };

    // **FX-14 — THE BOUND POINT'S WORD ON THESE BYTES, BEFORE THEY ARE
    // BELIEVED.** A plan bound to a recovery point (`source.point`) is
    // restored only if the runner's binding proves, against this archive, that
    // the manifest is the one the point's signed receipt attests, at the
    // version it pins. A preview that read the current manifest alone would
    // be green over a set that was written again after the point was signed
    // (FX-7: engine 0.21.0 can rewrite a set's segments under a byte-identical
    // manifest, which only the version shows), and the run would then be
    // refused. So the preview applies the binding's own pin and digest
    // judgement here, and every later row describes a manifest the run will
    // accept. The receipt is read only now: at the key the confinement above
    // accepted, and after this set's manifest was read under the
    // destination's own prefix.
    let pin_note = match spec.source.point.as_ref() {
        None => None,
        Some(point) => match judge_bound_point(
            point,
            spec,
            &manifest_key,
            &bytes,
            answered_version.as_deref(),
            access.as_ref(),
        ) {
            Ok(note) => note,
            Err(refusal) => {
                refuse_bound_point(&refusal, scope, want, now, checks);
                return;
            }
        },
    };

    let manifest = match archive::parse(&bytes) {
        Ok(v) => v,
        Err(ManifestError::NotAManifest | ManifestError::NoSegments) => {
            if want(CheckId::ArchiveBackupSet) {
                checks.push(
                    catalogue::outcome(
                        CheckId::ArchiveBackupSet,
                        CheckState::NotReady,
                        CheckCode::ManifestUnreadable,
                        now,
                    )
                    .with_message(&format!(
                        "the object at `{manifest_key}` is not a backup manifest"
                    ))
                    .with_remedy(remedy_for(CheckCode::ManifestUnreadable))
                    .with_scope(scope),
                );
            }
            block_rest(
                &[CheckId::ArchiveCoverage, CheckId::ArchiveSegments],
                want,
                now,
                checks,
            );
            return;
        }
    };

    if want(CheckId::ArchiveBackupSet) {
        // The binding's note travels with the row it qualifies: the pin could
        // not be checked in this bucket, so the digest decided — said, never
        // refused, as the runner logs it.
        let mut message = format!(
            "the backup manifest for set `{}` is readable",
            req.backup_id
        );
        if let Some(point) = spec.source.point.as_ref() {
            message.push_str(&format!(
                " and is the manifest recovery point {} attests",
                point.point_id
            ));
        }
        let mut row = ready(CheckId::ArchiveBackupSet, CheckCode::ManifestReadable, now);
        if let Some(note) = pin_note.as_deref() {
            message.push_str(&format!("; {note}"));
            // The whole note, as the catalog's deep check gives it beside an
            // `Available` point: what the digest alone cannot see.
            row = row.with_remedy(pin::UNCHECKED_NOTE);
        }
        checks.push(row.with_message(&message).with_scope(scope.clone()));
    }

    // ---- coverage ------------------------------------------------------
    let window = archive::window(&manifest);
    if want(CheckId::ArchiveCoverage) {
        checks.push(coverage_row(req, spec, &manifest, window, now));
    }

    // ---- segments ------------------------------------------------------
    if want(CheckId::ArchiveSegments) {
        checks.push(segments_row(
            req,
            spec,
            &manifest,
            window,
            access.as_ref(),
            now,
            details,
        ));
    }
}

/// `archive.coverage` — the point in time against the set's window, and the
/// selected topics against the set's topic list.
fn coverage_row(
    req: &RestorePreflightRequest,
    spec: &DrillSpec,
    manifest: &serde_json::Value,
    window: Result<archive::ManifestWindow, ManifestError>,
    now: DateTime<Utc>,
) -> CheckOutcome {
    let missing: Vec<String> = {
        let have = archive::topics(manifest);
        spec.source
            .topics
            .iter()
            .filter(|t| !have.contains(t.as_str()))
            .cloned()
            .collect()
    };
    if !missing.is_empty() {
        return catalogue::outcome(
            CheckId::ArchiveCoverage,
            CheckState::NotReady,
            CheckCode::TopicNotInBackupSet,
            now,
        )
        .with_message(&format!(
            "{} of {} selected topic(s) are not in backup set `{}`",
            missing.len(),
            spec.source.topics.len(),
            req.backup_id
        ))
        .with_remedy(remedy_for(CheckCode::TopicNotInBackupSet))
        .with_detail(detail(&missing));
    }
    let Ok(w) = window else {
        // Manifest-shaped and naming no segment: it bounds no window, so no
        // point in time is covered by it.
        return catalogue::outcome(
            CheckId::ArchiveCoverage,
            CheckState::NotReady,
            CheckCode::PointInTimeAfterCoverage,
            now,
        )
        .with_message(&format!(
            "backup set `{}` declares no segment, so it covers no instant",
            req.backup_id
        ))
        .with_remedy(remedy_for(CheckCode::PointInTimeAfterCoverage));
    };
    let pit = recovery_point(spec).timestamp_millis();
    // `<=`, NOT `<`, and the same rule as the execution guard: the restore
    // window is `[archive floor, recovery point]`, so a recovery point AT the
    // floor describes one instant and restores whatever shares that exact
    // millisecond — almost always nothing. An empty restore reported as a pass
    // is guard G-WIN's silent loss.
    if pit <= w.oldest_ms {
        return catalogue::outcome(
            CheckId::ArchiveCoverage,
            CheckState::NotReady,
            CheckCode::PointInTimeBeforeCoverage,
            now,
        )
        .with_message(&format!(
            "the requested recovery point is epoch-ms {pit}, at or before backup set `{}`'s \
             earliest covered timestamp of epoch-ms {}, so the restore window holds no instant",
            req.backup_id, w.oldest_ms
        ))
        .with_remedy(remedy_for(CheckCode::PointInTimeBeforeCoverage));
    }
    if pit > w.newest_ms {
        return catalogue::outcome(
            CheckId::ArchiveCoverage,
            CheckState::NotReady,
            CheckCode::PointInTimeAfterCoverage,
            now,
        )
        .with_message(&format!(
            "the requested recovery point is epoch-ms {pit}, after backup set `{}`'s newest \
             covered timestamp of epoch-ms {}",
            req.backup_id, w.newest_ms
        ))
        .with_remedy(remedy_for(CheckCode::PointInTimeAfterCoverage));
    }
    let covered = format!(
        "backup set `{}` covers epoch-ms {} to {}, which includes the requested recovery point \
         epoch-ms {pit}",
        req.backup_id, w.oldest_ms, w.newest_ms
    );
    // PROD-11.1: the plan's replay selection, resolved through the SAME
    // function execution binds and restores it with
    // (`ReplaySelection::resolve`), over the same manifest. A plan that states
    // none is answered exactly as before.
    let selection = match ReplaySelection::from_spec(spec) {
        Ok(s) if !s.is_full() => s,
        _ => {
            return ready(CheckId::ArchiveCoverage, CheckCode::PointInTimeCovered, now)
                .with_message(&covered)
        }
    };
    match selection.resolve(&archive::topic_facts(manifest)) {
        Ok(r) => {
            let topics: BTreeSet<&str> = r.partitions.iter().map(|p| p.topic.as_str()).collect();
            let with_data = r
                .partitions
                .iter()
                .filter(|p| !p.segment_keys.is_empty())
                .count();
            ready(CheckId::ArchiveCoverage, CheckCode::PointInTimeCovered, now).with_message(
                &format!(
                    "{covered}; the plan selects {} partition(s) of {} topic(s) ({with_data} \
                     with archived segments in the window) from epoch-ms {} ({}) to epoch-ms \
                     {}, restored by {} engine run(s)",
                    r.partitions.len(),
                    topics.len(),
                    r.start_ms,
                    if selection.window_start_ms.is_some() {
                        "the plan's stated start"
                    } else {
                        "the archive's floor"
                    },
                    r.end_ms,
                    r.runs.len()
                ),
            )
        }
        Err(refusal) => {
            let code = match &refusal {
                SelectionRefusal::StartBeforeCoverage { .. } => {
                    CheckCode::WindowStartBeforeCoverage
                }
                SelectionRefusal::PartitionNotInArchive { .. } => {
                    CheckCode::PartitionNotInBackupSet
                }
                SelectionRefusal::EmptySelection { .. } => CheckCode::SelectionEmpty,
                SelectionRefusal::NoCoverage { .. } => CheckCode::PointInTimeAfterCoverage,
                _ => CheckCode::SelectionInvalid,
            };
            catalogue::outcome(CheckId::ArchiveCoverage, CheckState::NotReady, code, now)
                .with_message(&format!("backup set `{}`: {refusal}", req.backup_id))
                .with_remedy(remedy_for(code))
        }
    }
}

/// `archive.segments` — every segment the manifest names for the restore
/// window is present under the set's prefix.
#[allow(clippy::too_many_arguments)]
fn segments_row(
    req: &RestorePreflightRequest,
    spec: &DrillSpec,
    manifest: &serde_json::Value,
    window: Result<archive::ManifestWindow, ManifestError>,
    access: &dyn ObjectAccess,
    now: DateTime<Utc>,
    details: &mut Vec<String>,
) -> CheckOutcome {
    if window.is_err() {
        return ready(CheckId::ArchiveSegments, CheckCode::SegmentsPresent, now)
            .with_message("the backup set names no segment, so none can be missing");
    }
    // The segments the restore will actually read: the plan's selection —
    // every partition of every selected topic from the archive's own floor
    // (guard G-WIN) unless the plan states a start or a partition subset
    // (PROD-11.1) — resolved by the ONE function execution restores it with
    // (`ReplaySelection::resolve`), then qualified into this handle's key
    // space. A selection the coverage row refuses names no segment here.
    let mut expected: Vec<String> = ReplaySelection::from_spec(spec)
        .ok()
        .and_then(|selection| selection.resolve(&archive::topic_facts(manifest)).ok())
        .map(|r| r.segment_keys())
        .unwrap_or_default()
        .iter()
        .map(|k| access.qualify(k))
        .collect();
    expected.sort();
    expected.dedup();
    // `<storage.prefix>/<backupId>` — the QUALIFIED manifest key's own
    // directory, so the listing and the manifest cannot disagree about which
    // set is being checked, and neither can the listing and `expected`:
    // `segment_keys_for_topics` above qualifies every key it lifts out of the
    // manifest body, so a set prefix taken from the unqualified request would
    // have compared two different key spaces and reported every segment of a
    // present set as missing (D2-PREFLIGHT-PREFIX).
    let set_prefix = access
        .qualify(&req.manifest_key)
        .trim_end_matches("/manifest.json")
        .to_string();
    let listed = match access.list_bounded(&set_prefix, SEGMENT_LIST_LIMIT + 1) {
        Ok(k) => k,
        Err(e) => {
            let code = store::classify(&e);
            return catalogue::outcome(CheckId::ArchiveSegments, state_for(code), code, now)
                .with_message(&format!(
                    "listing backup set `{set_prefix}` was refused: {code}"
                ))
                .with_remedy(remedy_for(code));
        }
    };
    if listed.len() > SEGMENT_LIST_LIMIT {
        return catalogue::outcome(
            CheckId::ArchiveSegments,
            CheckState::Unknown,
            CheckCode::SegmentListTooLarge,
            now,
        )
        .with_message(&format!(
            "backup set `{}` holds more than {SEGMENT_LIST_LIMIT} keys, which a preflight does \
             not list",
            req.backup_id
        ))
        .with_remedy(remedy_for(CheckCode::SegmentListTooLarge));
    }
    let present: BTreeSet<&str> = listed.iter().map(String::as_str).collect();
    let missing: Vec<String> = expected
        .iter()
        .filter(|k| !present.contains(k.as_str()))
        .cloned()
        .collect();
    if missing.is_empty() {
        return ready(CheckId::ArchiveSegments, CheckCode::SegmentsPresent, now).with_message(
            &format!(
                "all {} segment(s) backup set `{}` names for this window are present",
                expected.len(),
                req.backup_id
            ),
        );
    }
    for key in &missing {
        details.push(
            serde_json::json!({"check": CheckId::ArchiveSegments.as_str(), "missingSegment": key})
                .to_string(),
        );
    }
    catalogue::outcome(
        CheckId::ArchiveSegments,
        CheckState::NotReady,
        CheckCode::SegmentMissing,
        now,
    )
    .with_message(&format!(
        "{} of {} segment(s) backup set `{}` names for this window are not in the archive",
        missing.len(),
        expected.len(),
        req.backup_id
    ))
    .with_remedy(remedy_for(CheckCode::SegmentMissing))
    .with_detail(detail(&missing))
}

#[allow(clippy::too_many_arguments)]
fn target_checks(
    req: &RestorePreflightRequest,
    spec: &DrillSpec,
    wiring: &dyn Wiring,
    deadline: Deadline,
    now: DateTime<Utc>,
    want: &dyn Fn(CheckId) -> bool,
    checks: &mut Vec<CheckOutcome>,
    details: &mut Vec<String>,
) {
    let scope = catalogue::scope("RestoreTarget", &req.target.principal, None);
    let budget = deadline.slice(2);
    // PROD-01.2: the capability rows the plan lists (`capabilityChecks`), and
    // only those: `checks` being empty never includes one. A skipped one is
    // refused by the plan validation, so the list is taken as written.
    let capabilities = &req.capability_checks;
    let probe = match wiring.broker(&req.target, budget) {
        Ok(p) => p,
        Err(f) => {
            if want(CheckId::TargetAuthenticated) {
                checks.push(
                    from_broker_failure(CheckId::TargetAuthenticated, &f, now)
                        .with_scope(scope.clone()),
                );
            }
            for id in capabilities {
                checks.push(
                    super::capability::blocked(
                        *id,
                        "the target did not authenticate, so this check did not run",
                        now,
                    )
                    .with_scope(scope.clone()),
                );
            }
            block_rest(
                &[
                    CheckId::TargetScratchMarker,
                    CheckId::TargetMappedTopics,
                    CheckId::TargetTopicCreate,
                    CheckId::TargetTimestampBound,
                ],
                want,
                now,
                checks,
            );
            return;
        }
    };

    let (row, _) = authenticated(
        CheckId::TargetAuthenticated,
        probe.as_ref(),
        Some(scope.clone()),
        now,
    );
    let authenticated_ok = row.state == CheckState::Ready;
    if want(CheckId::TargetAuthenticated) {
        checks.push(row);
    }
    // PROD-01.2: `target.engineProtocol`, right after the row it depends on
    // and before any row about topics: an endpoint the engine cannot write to
    // is not a restore target whatever its topics look like.
    for row in super::readiness::capability_rows(
        capabilities,
        &req.target,
        probe.as_ref(),
        &[],
        (authenticated_ok, authenticated_ok),
        deadline,
        now,
    ) {
        checks.push(row.with_scope(scope.clone()));
    }
    if !authenticated_ok {
        block_rest(
            &[
                CheckId::TargetScratchMarker,
                CheckId::TargetMappedTopics,
                CheckId::TargetTopicCreate,
                CheckId::TargetTimestampBound,
            ],
            want,
            now,
            checks,
        );
        return;
    }

    // ---- the scratch segregation proof ---------------------------------
    if spec.target.mode == TargetMode::Scratch && want(CheckId::TargetScratchMarker) {
        checks.push(
            marker_row(probe.as_ref(), &spec.target.marker_topic, now).with_scope(scope.clone()),
        );
    }

    // ---- the mapped names ----------------------------------------------
    let prefix = target_topic_prefix(spec);
    let mapped: Vec<String> = spec
        .source
        .topics
        .iter()
        .map(|t| format!("{prefix}{t}"))
        .collect();

    if want(CheckId::TargetMappedTopics) {
        checks.push(
            mapped_topics_row(probe.as_ref(), &mapped, deadline, now, details)
                .with_scope(scope.clone()),
        );
    }
    if want(CheckId::TargetTopicCreate) {
        checks.push(topic_create_row(probe.as_ref(), spec, &mapped, now).with_scope(scope.clone()));
    }
    if want(CheckId::TargetTimestampBound) {
        // A plan that lists capability rows was rendered by a controller of
        // this build, which knows this build's codes (`timestamp_bound_row`).
        let controller_knows_the_code = !capabilities.is_empty();
        checks.push(
            timestamp_bound_row(probe.as_ref(), spec, controller_knows_the_code, now)
                .with_scope(scope.clone()),
        );
    }
    if want(CheckId::TargetLogAppendTime) {
        checks.push(
            execution_only(
                CheckId::TargetLogAppendTime,
                CheckCode::LogAppendTimeOverrideVerifiedOnlyAtExecution,
                now,
            )
            .with_message(
                "establishing whether a per-topic CreateTime override is honoured needs a topic \
                 to exist, and a preflight creates none",
            )
            .with_scope(scope),
        );
    }
}

/// `target.scratchMarker` — the v0.1 segregation proof, read-only.
fn marker_row(probe: &dyn InventoryProbe, marker: &str, now: DateTime<Utc>) -> CheckOutcome {
    match probe.describe_topic(marker) {
        Ok(TopicPresence::Present { .. }) => {
            ready(CheckId::TargetScratchMarker, CheckCode::MarkerHealthy, now)
                .with_message(&format!("the scratch marker topic `{marker}` is present"))
        }
        Ok(TopicPresence::NotFound) => catalogue::outcome(
            CheckId::TargetScratchMarker,
            CheckState::NotReady,
            CheckCode::MarkerTopicMissing,
            now,
        )
        .with_message(&format!(
            "the scratch marker topic `{marker}` is not on the target"
        ))
        .with_remedy(remedy_for(CheckCode::MarkerTopicMissing)),
        Ok(TopicPresence::NotAuthorized | TopicPresence::Unknown) | Err(_) => catalogue::outcome(
            CheckId::TargetScratchMarker,
            CheckState::NotReady,
            CheckCode::MarkerTopicErrored,
            now,
        )
        .with_message(&format!(
            "the scratch marker topic `{marker}` could not be described on the target"
        ))
        .with_remedy(remedy_for(CheckCode::MarkerTopicErrored)),
    }
}

/// `target.mappedTopics` — D2 §6.7(a): targeted metadata per mapped name.
fn mapped_topics_row(
    probe: &dyn InventoryProbe,
    mapped: &[String],
    deadline: Deadline,
    now: DateTime<Utc>,
    details: &mut Vec<String>,
) -> CheckOutcome {
    let mut exists: Vec<String> = Vec::new();
    let mut unknown: Vec<String> = Vec::new();
    for name in mapped {
        if !deadline.has_room() {
            unknown.push(name.clone());
            continue;
        }
        match probe.describe_topic(name) {
            // ABSENT is the pass: the restore creates it.
            Ok(TopicPresence::NotFound) => {}
            Ok(TopicPresence::Present { .. }) => exists.push(name.clone()),
            // `TOPIC_AUTHORIZATION_FAILED` says nothing about existence.
            Ok(TopicPresence::NotAuthorized | TopicPresence::Unknown) | Err(_) => {
                unknown.push(name.clone());
            }
        }
    }
    if !exists.is_empty() {
        for name in &exists {
            details.push(
                serde_json::json!({
                    "check": CheckId::TargetMappedTopics.as_str(),
                    "mappedTopicExists": name
                })
                .to_string(),
            );
        }
        return catalogue::outcome(
            CheckId::TargetMappedTopics,
            CheckState::NotReady,
            CheckCode::MappedTopicExists,
            now,
        )
        .with_message(&format!(
            "{} of {} mapped target topic(s) already exist",
            exists.len(),
            mapped.len()
        ))
        .with_remedy(remedy_for(CheckCode::MappedTopicExists))
        .with_detail(detail(&exists));
    }
    if !unknown.is_empty() {
        return catalogue::outcome(
            CheckId::TargetMappedTopics,
            CheckState::Unknown,
            CheckCode::MappedTopicVisibilityUnknown,
            now,
        )
        .with_message(&format!(
            "{} of {} mapped target topic(s) could not be described",
            unknown.len(),
            mapped.len()
        ))
        .with_remedy(remedy_for(CheckCode::MappedTopicVisibilityUnknown))
        .with_detail(detail(&unknown));
    }
    ready(
        CheckId::TargetMappedTopics,
        CheckCode::MappedTopicsAbsent,
        now,
    )
    .with_message(&format!(
        "none of the {} mapped target topic(s) exists on the target",
        mapped.len()
    ))
}

/// `target.topicCreate` — D2 §6.7(b): a `CreateTopics` with
/// `validate_only = true`.
///
/// It creates nothing. It is the second half of the collision answer, and it
/// reaches a case metadata cannot: a topic hidden from `DESCRIBE` answers
/// `TOPIC_ALREADY_EXISTS` here when `CREATE` is authorized.
///
/// The partition count is **1** and is not a claim about the restore: the real
/// count is the manifest's and is chosen at execution. A validate-only request
/// validates the CONFIGURATION and the REPLICATION FACTOR against the
/// cluster's brokers, which is what this row is about.
fn topic_create_row(
    probe: &dyn InventoryProbe,
    spec: &DrillSpec,
    mapped: &[String],
    now: DateTime<Utc>,
) -> CheckOutcome {
    if mapped.is_empty() {
        return ready(
            CheckId::TargetTopicCreate,
            CheckCode::TopicCreateValidated,
            now,
        )
        .with_message("the plan maps no target topic");
    }
    let specs: Vec<NewTopicSpec> = mapped
        .iter()
        .map(|name| NewTopicSpec {
            name: name.clone(),
            num_partitions: 1,
            replication_factor: i32::from(spec.target.default_replication_factor),
            configs: TARGET_TOPIC_CONFIGS
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect(),
        })
        .collect();
    let outcomes = match probe.validate_create_topics(&specs) {
        Ok(o) => o,
        Err(f) => {
            // The REQUEST failed, not a topic: the target did not answer a
            // validate-only CreateTopics at all.
            return catalogue::outcome(
                CheckId::TargetTopicCreate,
                CheckState::Unknown,
                CheckCode::TopicCreateValidationUnsupported,
                now,
            )
            .with_message(&f.message)
            .with_remedy(remedy_for(CheckCode::TopicCreateValidationUnsupported));
        }
    };
    let refused: Vec<&logweir_kafka::inventory::TopicCreateOutcome> = outcomes
        .iter()
        .filter(|o| o.code != CheckCode::TopicCreateValidated)
        .collect();
    let Some(first) = refused.first() else {
        return ready(
            CheckId::TargetTopicCreate,
            CheckCode::TopicCreateValidated,
            now,
        )
        .with_message(&format!(
            "the target validated creation of all {} mapped topic(s) at replication factor {}",
            mapped.len(),
            spec.target.default_replication_factor
        ));
    };
    let code = first.code;
    let names: Vec<String> = refused.iter().map(|o| o.name.clone()).collect();
    catalogue::outcome(CheckId::TargetTopicCreate, state_for(code), code, now)
        .with_message(&format!(
            "{} of {} mapped topic(s) were refused by a validate-only CreateTopics: {code}",
            refused.len(),
            mapped.len()
        ))
        .with_remedy(remedy_for(code))
        .with_detail(detail(&names))
}

/// `target.timestampBound` — the same arithmetic the execution guard runs
/// (`phase0_admit.rs`'s `target_topic_preflight`, step 2).
///
/// `controller_knows_the_code`: whether the plan's controller can read
/// `TimestampBoundNotReported` (PROD-01.2). The code vocabulary is closed on
/// the READING side, so a controller from before that row refuses a whole
/// result that carries it (`ResultUnreadable`). The row is one this runner
/// answers for every restore plan, so the code cannot be volunteered: a plan
/// from an older controller gets the same state, message and remedy under
/// `BrokerConfigsNotReadable`, the `unknown` code that controller already
/// has for this row.
fn timestamp_bound_row(
    probe: &dyn InventoryProbe,
    spec: &DrillSpec,
    controller_knows_the_code: bool,
    now: DateTime<Utc>,
) -> CheckOutcome {
    let broker = match probe.broker_configs() {
        Ok(b) => b,
        Err(_) => {
            return catalogue::outcome(
                CheckId::TargetTimestampBound,
                CheckState::Unknown,
                CheckCode::BrokerConfigsNotReadable,
                now,
            )
            .with_message("the target's broker configuration could not be read")
            .with_remedy(remedy_for(CheckCode::BrokerConfigsNotReadable));
        }
    };
    // `before.max.ms` first: on Kafka >= 3.6 BOTH keys are reported and
    // `difference.max.ms` is the deprecated one.
    let bound = broker
        .get(BROKER_TIMESTAMP_BEFORE_MAX_MS)
        .or_else(|| broker.get(BROKER_TIMESTAMP_DIFFERENCE_MAX_MS))
        .and_then(|v| v.trim().parse::<i64>().ok());
    let Some(bound) = bound else {
        // PROD-01.2: NEITHER KEY IN THE ANSWER IS "NOT REPORTED", NOT "NO
        // BOUND". Every Apache Kafka broker reports at least one of the two
        // keys (unbounded is the value 9223372036854775807), so an answer
        // without either comes from an endpoint that keeps the setting
        // elsewhere. Measured on Redpanda v26.2.4: nine broker keys, neither
        // of these, while each topic reports
        // `message.timestamp.before.max.ms`. This row used to answer `ready`,
        // "the target declares no record-timestamp bound": an empty answer
        // recorded as a fact.
        let code = if controller_knows_the_code {
            CheckCode::TimestampBoundNotReported
        } else {
            CheckCode::BrokerConfigsNotReadable
        };
        return catalogue::outcome(
            CheckId::TargetTimestampBound,
            CheckState::Unknown,
            code,
            now,
        )
        .with_message(&format!(
            "the target's broker configuration answered {} key(s) and neither \
                 {BROKER_TIMESTAMP_BEFORE_MAX_MS} nor {BROKER_TIMESTAMP_DIFFERENCE_MAX_MS}, so \
                 the record-timestamp bound was not checked",
            broker.len()
        ))
        // The remedy is this finding's under either code:
        // `BrokerConfigsNotReadable`'s own is about a missing grant.
        .with_remedy(remedy_for(CheckCode::TimestampBoundNotReported));
    };
    let end_ms = recovery_point(spec).timestamp_millis();
    // `saturating_sub` is load-bearing: the Apache default for both keys is
    // i64::MAX, so a plain `-` overflows and wraps to a floor in the FUTURE,
    // which would refuse every window on every default broker.
    let oldest_accepted = now.timestamp_millis().saturating_sub(bound);
    if end_ms < oldest_accepted {
        return catalogue::outcome(
            CheckId::TargetTimestampBound,
            CheckState::NotReady,
            CheckCode::TimestampBoundExceeded,
            now,
        )
        .with_message(&format!(
            "the target bounds record timestamps at {bound} ms, so nothing older than epoch-ms \
             {oldest_accepted} is accepted, and this plan's recovery point is epoch-ms {end_ms}"
        ))
        .with_remedy(remedy_for(CheckCode::TimestampBoundExceeded));
    }
    let mut row = ready(
        CheckId::TargetTimestampBound,
        CheckCode::TimestampWithinBound,
        now,
    )
    .with_message(&format!(
        "this plan's recovery point epoch-ms {end_ms} is inside the target's {bound} ms \
         record-timestamp bound"
    ));
    if let Some(t) = broker.get(BROKER_TIMESTAMP_TYPE) {
        row = row.with_fact("brokerTimestampType", t);
    }
    row
}

/// Rows that could not run because a prerequisite did not pass.
fn block_rest(
    ids: &[CheckId],
    want: &dyn Fn(CheckId) -> bool,
    now: DateTime<Utc>,
    checks: &mut Vec<CheckOutcome>,
) {
    for id in ids {
        if want(*id) {
            checks.push(
                catalogue::outcome(
                    *id,
                    CheckState::Unknown,
                    CheckCode::BlockedByPrerequisite,
                    now,
                )
                .with_message("a prerequisite of this check did not pass, so it did not run")
                .with_remedy(remedy_for(CheckCode::BlockedByPrerequisite)),
            );
        }
    }
}

/// The `details` stream: JSON lines, **redacted**, capped, with a final count
/// line when the cap bit.
///
/// The cap is enforced HERE and not by the caller, so every producer of a
/// detail line is bounded by one rule. A truncated stream says so rather than
/// simply ending: a consumer that could not tell would render "3 missing
/// segments" for a set missing three thousand.
///
/// # The redaction, and why it is at this line and not at the push sites
///
/// A details line carries adopter identifiers — a segment key, a mapped topic
/// name — and the controller writes the stream VERBATIM into an immutable
/// `<job>-details` `ConfigMap` (D2 §6.7). `CheckOutcome`'s own builders redact
/// `message`, `remedy` and `facts`, and `catalogue::scope` and
/// `readiness::detail` were made to redact for the same reason; this stream was
/// the third exception to a rule the code, `docs/stability.md` and the report
/// all state has exactly one. It has one now.
///
/// It is applied HERE because this function is the ONE place the stream is
/// assembled, so a new producer of a detail line cannot forget — which is the
/// property a per-push-site redaction would not have. `redact` is idempotent
/// and fires only on credential shapes, so an ordinary key or topic name is
/// unchanged, and it never emits a quote, so a JSON line stays a JSON line.
#[must_use]
pub fn details_stream(lines: &[String]) -> Vec<u8> {
    let mut out = String::new();
    let mut kept = 0usize;
    for line in lines {
        let line = crate::check::redact_path(line);
        if out.len() + line.len() + 1 > DETAILS_MAX_BYTES {
            break;
        }
        out.push_str(&line);
        out.push('\n');
        kept += 1;
    }
    if kept < lines.len() {
        let note = serde_json::json!({
            "truncated": true,
            "kept": kept,
            "total": lines.len(),
            "sampleLimit": DETAIL_SAMPLE,
        })
        .to_string();
        out.push_str(&note);
        out.push('\n');
    }
    out.into_bytes()
}

/// The remedy for a backup manifest over the read cap (FX-31 review F7): the
/// store answered, the object is larger than this build reads, and no grant,
/// bucket policy or network change makes it smaller.
pub const OVER_CAP_MANIFEST_REMEDY: &str =
    "The backup set's manifest is larger than this build reads (docs/kubernetes.md §7b.4). \
     No grant or policy change helps: restore from a smaller set, or a build whose manifest \
     cap holds this one.";
